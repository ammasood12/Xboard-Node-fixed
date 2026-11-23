package singbox

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

// logBuf is a goroutine-safe buffer for capturing node log output.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLog routes node log output to a buffer for the rest of the test.
func captureLog(t *testing.T) *logBuf {
	t.Helper()
	lb := &logBuf{}
	nlog.Init(lb, slog.LevelDebug, false)
	t.Cleanup(func() { nlog.Init(os.Stdout, slog.LevelInfo, false) })
	return lb
}

// deviceTracker returns a tracker for user 1 (uuid-1) with the given limit and
// device_limit settings.
func deviceTracker(limit int, cfg config.DeviceLimitConfig) *ConnTracker {
	tr := NewConnTracker(0)
	tr.SetUserMap(map[string]int{"uuid-1": 1})
	tr.SetDeviceLimitFunc(func(uuid string) (int, bool) { return limit, true })
	tr.SetDeviceConfig(cfg.Runtime())
	return tr
}

// connect opens a connection for user 1 from ip and returns it, wrapped.
func connect(tr *ConnTracker, ip string) (*testConn, *trackedConn) {
	base := &testConn{}
	w := tr.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", ip), nil, nil)
	tc, _ := w.(*trackedConn)
	return base, tc
}

// sample runs one over-limit sample now, regardless of the 30 s spacing.
func sample(tr *ConnTracker) {
	tr.devMu.Lock()
	tr.lastSample = time.Time{}
	tr.devMu.Unlock()
	tr.sampleDevices(time.Now())
}

func TestDefaultConfigIsFixed3RecentConnection(t *testing.T) {
	r := (*config.DeviceLimitConfig)(nil).Runtime()
	if r.CountMethod != config.CountRecentConnection || r.RecentConnection != 180*time.Second {
		t.Fatalf("default method = %s / %v, want recent_connection / 180s", r.CountMethod, r.RecentConnection)
	}
	if r.Block || r.Merge || r.RejectNewImmediately {
		t.Fatalf("defaults must not block, merge or reject: %+v", r)
	}
	if !r.ClearPanelWhenUserIdle || !r.LogOverLimit || r.TCPKeepaliveSeconds != 180 {
		t.Fatalf("unexpected defaults: %+v", r)
	}
}

func TestRecentConnectionCountsNewConnectionAndClientData(t *testing.T) {
	tr := deviceTracker(3, config.DeviceLimitConfig{})
	_, tc := connect(tr, "1.1.1.1")
	us := tr.users[1]

	// A new connection is activity under fixed3.
	if got := us.aliveIPList(tr.deviceRule()); !got["1.1.1.1"] {
		t.Fatalf("new connection not counted: %v", got)
	}

	// 181 s without client data: no longer a device.
	tc.act.lastRead.Store(time.Now().Add(-181 * time.Second).UnixNano())
	if got := us.aliveIPList(tr.deviceRule()); got == nil || got["1.1.1.1"] {
		t.Fatalf("idle IP still counted: %v", got)
	}

	// Bytes sent to the client do not revive it (dead path keeps accepting writes).
	tc.act.addWrite(10 * 1024 * kb)
	if got := us.aliveIPList(tr.deviceRule()); got["1.1.1.1"] {
		t.Fatalf("server-to-client bytes counted as client activity: %v", got)
	}

	// Client data does.
	tc.act.add(1)
	if got := us.aliveIPList(tr.deviceRule()); !got["1.1.1.1"] {
		t.Fatalf("client data not counted: %v", got)
	}
}

func TestClearPanelWhenUserIdleOffOmitsUser(t *testing.T) {
	f := false
	tr := deviceTracker(3, config.DeviceLimitConfig{ClearPanelWhenUserIdle: &f})
	_, tc := connect(tr, "1.1.1.1")
	tc.act.lastRead.Store(time.Now().Add(-time.Hour).UnixNano())
	if got := tr.users[1].aliveIPList(tr.deviceRule()); got != nil {
		t.Fatalf("idle user reported %v, want it omitted", got)
	}
	_, aliveIPs, _ := tr.GetUserTraffic()
	if _, ok := aliveIPs[1]; ok {
		t.Fatal("idle user present in the report")
	}
}

func TestDefaultsNeverBlock(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{CountMethod: config.CountRecentConnection})
	first, _ := connect(tr, "1.1.1.1")
	second, tc2 := connect(tr, "2.2.2.2")
	if tc2 == nil {
		t.Fatal("second device was rejected with block_devices off")
	}
	tr.disconnectOverLimit()
	if first.closed || second.closed {
		t.Fatal("a device was disconnected with block_devices off")
	}
}

func TestOverLimitLogsActionNoneWhenNotBlocking(t *testing.T) {
	lb := captureLog(t)
	tr := deviceTracker(1, config.DeviceLimitConfig{OverLimitKickAfterMinutes: intp(0)})
	connect(tr, "1.1.1.1")
	connect(tr, "2.2.2.2")
	tr.disconnectOverLimit()
	out := lb.String()
	for _, want := range []string{"device over the limit", "ip=2.2.2.2", "limit=1", "action=none", "union=2", "max=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
}

func TestLogOverLimitCanBeSwitchedOff(t *testing.T) {
	lb := captureLog(t)
	f := false
	tr := deviceTracker(1, config.DeviceLimitConfig{OverLimitKickAfterMinutes: intp(0), LogOverLimitDevices: &f})
	connect(tr, "1.1.1.1")
	connect(tr, "2.2.2.2")
	tr.disconnectOverLimit()
	if strings.Contains(lb.String(), "device over the limit") {
		t.Fatalf("logged although log_over_limit_devices is false:\n%s", lb.String())
	}
}

func TestOverLimitLogIsRateLimited(t *testing.T) {
	lb := captureLog(t)
	tr := deviceTracker(1, config.DeviceLimitConfig{OverLimitKickAfterMinutes: intp(0)})
	connect(tr, "1.1.1.1")
	connect(tr, "2.2.2.2")
	for i := 0; i < 5; i++ {
		tr.disconnectOverLimit()
	}
	if n := strings.Count(lb.String(), "device over the limit"); n != 1 {
		t.Fatalf("logged %d times, want 1", n)
	}
}

func TestDelayedKickWaitsForPersistentOverLimit(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true}) // kick after 5 min of 10
	keep, _ := connect(tr, "1.1.1.1")
	over, _ := connect(tr, "2.2.2.2")

	// 9 samples (4.5 min over the limit): nothing happens yet.
	for i := 0; i < 9; i++ {
		sample(tr)
	}
	if keep.closed || over.closed {
		t.Fatal("a device was kicked before the delay passed")
	}
	// The 10th sample reaches 5 minutes: the higher-sorting IP goes.
	sample(tr)
	if keep.closed {
		t.Fatal("the lowest-sorting device must be kept")
	}
	if !over.closed {
		t.Fatal("device over the limit was not kicked after the delay")
	}
}

func TestDelayedKickNeedsNoContinuousOverage(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true})
	keep, _ := connect(tr, "1.1.1.1")
	over, tcOver := connect(tr, "2.2.2.2")

	for i := 0; i < 6; i++ {
		sample(tr)
	}
	// The user drops back to the limit for 2 samples (the second IP idles) ...
	tcOver.act.lastRead.Store(time.Now().Add(-time.Hour).UnixNano())
	sample(tr)
	sample(tr)
	if over.closed {
		t.Fatal("kicked while under the limit")
	}
	// ... and returns: 6 + 4 more over-limit samples inside the 10 minute watch.
	tcOver.act.add(1)
	for i := 0; i < 3; i++ {
		sample(tr)
	}
	if over.closed {
		t.Fatal("kicked before the over-limit total reached 5 minutes")
	}
	sample(tr)
	if !over.closed || keep.closed {
		t.Fatalf("expected the second IP kicked and the first kept (over=%v keep=%v)", over.closed, keep.closed)
	}
}

func TestKickedUserStartsFreshDelay(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true})
	connect(tr, "1.1.1.1")
	connect(tr, "2.2.2.2")
	for i := 0; i < 10; i++ {
		sample(tr)
	}
	tr.devMu.Lock()
	_, kept := tr.overHist[1]
	tr.devMu.Unlock()
	if kept {
		t.Fatal("history kept after the kick")
	}
}

func TestHistoryDroppedOnceUserStaysUnderLimit(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true})
	connect(tr, "1.1.1.1")
	_, tc2 := connect(tr, "2.2.2.2")
	sample(tr)
	sample(tr)
	tc2.act.lastRead.Store(time.Now().Add(-time.Hour).UnixNano())
	for i := 0; i < 20; i++ { // the 10 minute watch slides past the old samples
		sample(tr)
	}
	tr.devMu.Lock()
	_, kept := tr.overHist[1]
	tr.devMu.Unlock()
	if kept {
		t.Fatal("history kept although the user has been under the limit for the whole watch")
	}
}

func TestGateRejectsNewDeviceOnlyWhenPersistentlyOver(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true})
	connect(tr, "1.1.1.1")

	if _, tc := connect(tr, "2.2.2.2"); tc == nil {
		t.Fatal("new device refused although the user was never over the limit for long")
	}
	// Now the user has been over the limit long enough (10 samples = 5 min).
	tr.devMu.Lock()
	tr.overHist[1] = []bool{true, true, true, true, true, true, true, true, true, true}
	tr.devMu.Unlock()
	if base, tc := connect(tr, "3.3.3.3"); tc != nil || !base.closed {
		t.Fatalf("new device admitted at the limit after persistent overage")
	}
}

func TestGateRejectsAtOnceWithRejectNewDeviceImmediately(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true, RejectNewDeviceImmediately: true})
	connect(tr, "1.1.1.1")
	if base, tc := connect(tr, "2.2.2.2"); tc != nil || !base.closed {
		t.Fatal("new device over the limit was not rejected at once")
	}
}

func TestGateDryRunAdmitsButLogsWouldReject(t *testing.T) {
	lb := captureLog(t)
	tr := deviceTracker(1, config.DeviceLimitConfig{RejectNewDeviceImmediately: true})
	connect(tr, "1.1.1.1")
	base, tc := connect(tr, "2.2.2.2")
	if tc == nil || base.closed {
		t.Fatal("dry run rejected a device")
	}
	out := lb.String()
	if !strings.Contains(out, "kind=reject") || !strings.Contains(out, "action=none") {
		t.Fatalf("dry run did not log the would-be rejection:\n%s", out)
	}
}

func TestMergeCountsOnePhoneOnBothFamiliesOnce(t *testing.T) {
	sep := deviceTracker(1, config.DeviceLimitConfig{OverLimitKickAfterMinutes: intp(0)})
	connect(sep, "111.52.73.92")
	connect(sep, "2409:8a0c:872:4ed0:824:af02:ca09:3928")
	sample(sep)
	sep.devMu.Lock()
	sepOver := len(sep.overHist)
	sep.devMu.Unlock()
	if sepOver != 1 {
		t.Fatal("separate counting: IPv4 + IPv6 should be 2 devices, over a limit of 1")
	}

	mrg := deviceTracker(1, config.DeviceLimitConfig{OverLimitKickAfterMinutes: intp(0), IPv4IPv6Counting: config.CountMerge})
	v4, _ := connect(mrg, "111.52.73.92")
	v6, _ := connect(mrg, "2409:8a0c:872:4ed0:824:af02:ca09:3928")
	sample(mrg)
	mrg.devMu.Lock()
	mrgOver := len(mrg.overHist)
	mrg.devMu.Unlock()
	if mrgOver != 0 || v4.closed || v6.closed {
		t.Fatalf("merge counting: one phone on both families is 1 device (hist=%d v4closed=%v v6closed=%v)", mrgOver, v4.closed, v6.closed)
	}
}

func TestMergeStillKicksASecondDeviceOfTheSameFamily(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true, OverLimitKickAfterMinutes: intp(0), IPv4IPv6Counting: config.CountMerge})
	a, _ := connect(tr, "1.1.1.1")
	b, _ := connect(tr, "2.2.2.2")
	v6, _ := connect(tr, "2409:8a0c:872:4ed0::1")
	tr.disconnectOverLimit()
	if a.closed || v6.closed {
		t.Fatal("kept devices were disconnected")
	}
	if !b.closed {
		t.Fatal("second IPv4 device not disconnected")
	}
}

func TestMergeGateAdmitsNewFamilyAtTheLimit(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{BlockDevices: true, RejectNewDeviceImmediately: true, IPv4IPv6Counting: config.CountMerge})
	connect(tr, "1.1.1.1")
	if _, tc := connect(tr, "2409:8a0c:872:4ed0::1"); tc == nil {
		t.Fatal("IPv6 refused although only one IPv4 device is online (the same phone)")
	}
	if base, tc := connect(tr, "2.2.2.2"); tc != nil || !base.closed {
		t.Fatal("second IPv4 device admitted at the limit")
	}
}

func TestIgnoredIPsNeverCountOrGetBlocked(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{
		BlockDevices: true, RejectNewDeviceImmediately: true, OverLimitKickAfterMinutes: intp(0),
		IgnoreIPs: []string{"10.0.0.0/8", "2605:e440:b::302"},
	})
	connect(tr, "1.1.1.1")
	relay, tcRelay := connect(tr, "10.9.9.9")
	relay6, tcRelay6 := connect(tr, "2605:e440:b::302")
	if tcRelay == nil || tcRelay6 == nil {
		t.Fatal("ignored IP was rejected at the gate")
	}
	tr.disconnectOverLimit()
	if relay.closed || relay6.closed {
		t.Fatal("ignored IP was disconnected")
	}
	if got := tr.users[1].aliveIPList(tr.deviceRule()); len(got) != 1 || !got["1.1.1.1"] {
		t.Fatalf("ignored IPs must not be reported as devices, got %v", got)
	}
}

func TestCountMethodOffReportsAllAndNeverBlocks(t *testing.T) {
	tr := deviceTracker(1, config.DeviceLimitConfig{CountMethod: config.CountOff, BlockDevices: true, RejectNewDeviceImmediately: true, OverLimitKickAfterMinutes: intp(0)})
	a, _ := connect(tr, "1.1.1.1")
	b, tcB := connect(tr, "2.2.2.2")
	if tcB == nil {
		t.Fatal("rejected with count_method off")
	}
	tr.disconnectOverLimit()
	sample(tr)
	if a.closed || b.closed {
		t.Fatal("disconnected with count_method off")
	}
	if got := tr.users[1].aliveIPList(tr.deviceRule()); len(got) != 2 {
		t.Fatalf("count_method off must report every connected IP, got %v", got)
	}
}

func TestTrafficMethodUsesConfiguredThresholdAndWindow(t *testing.T) {
	tr := deviceTracker(3, config.DeviceLimitConfig{CountMethod: config.CountTraffic, MinTrafficKB: 500, TrafficCheckSeconds: 30})
	_, tc := connect(tr, "1.1.1.1")
	us := tr.users[1]
	now := bucketStart()
	tc.act.addAt(499*kb, now, true)
	if got := us.activeIPs(now, tr.deviceRule()); len(got) != 0 {
		t.Fatalf("499 KB counted with a 500 KB threshold: %v", got)
	}
	tc.act.addAt(2*kb, now, true)
	if got := us.activeIPs(now, tr.deviceRule()); len(got) != 1 {
		t.Fatal("501 KB not counted")
	}
	// 30 s window: the bytes are gone after 30 s.
	if got := us.activeIPs(now.Add(30*time.Second), tr.deviceRule()); len(got) != 0 {
		t.Fatalf("bytes still counted after the 30 s window: %v", got)
	}
}

func TestSetDeviceConfigAppliesToRunningTracker(t *testing.T) {
	tr := NewConnTracker(0)
	if tr.deviceRule().CountMethod != config.CountRecentConnection {
		t.Fatal("default rule not applied")
	}
	tr.SetDeviceConfig((&config.DeviceLimitConfig{CountMethod: config.CountTraffic}).Runtime())
	if tr.deviceRule().CountMethod != config.CountTraffic {
		t.Fatal("new rule not applied")
	}
}
