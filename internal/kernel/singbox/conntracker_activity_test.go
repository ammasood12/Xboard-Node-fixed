package singbox

import (
	"context"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
)

const kb = 1024

// deviceMinBytes is the default traffic threshold (min_traffic_kb: 100).
const deviceMinBytes = 100 * kb

func intp(v int) *int { return &v }

// trafficRule is the fixed4 rule with the old immediate behaviour: count by
// traffic, reject new devices at once, kick at once, really block.
func trafficRule() config.DeviceRuntime {
	return (&config.DeviceLimitConfig{
		CountMethod:                config.CountTraffic,
		BlockDevices:               true,
		RejectNewDeviceImmediately: true,
		OverLimitKickAfterMinutes:  intp(0),
	}).Runtime()
}

// newTrafficTracker returns a tracker running trafficRule.
func newTrafficTracker() *ConnTracker {
	t := NewConnTracker(0)
	t.SetDeviceConfig(trafficRule())
	return t
}

var rt = trafficRule()

func ruleOf(cfg config.DeviceLimitConfig) config.DeviceRuntime { return cfg.Runtime() }

// bucketStart returns a time aligned to the start of a bucket, so window edges
// in the tests are exact.
func bucketStart() time.Time {
	const dur = int64(deviceBucketDur)
	return time.Unix(0, (time.Now().UnixNano()/dur)*dur)
}

func TestAliveIPListCountsOnlyIPsOverThreshold(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	if got := us.aliveIPList(&rt); got != nil {
		t.Fatalf("no connections: got %v, want nil", got)
	}

	busy := us.addConn("1.1.1.1")
	us.addConn("2.2.2.2") // connected, moved nothing
	busy.add(deviceMinBytes)

	got := us.aliveIPList(&rt)
	if len(got) != 1 || !got["1.1.1.1"] {
		t.Fatalf("got %v, want only 1.1.1.1", got)
	}
}

func TestAliveIPListEmptySetWhenNobodyIsActive(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	got := us.aliveIPList(&rt)
	if got == nil || len(got) != 0 {
		t.Fatalf("connections but no traffic: got %v, want an empty non-nil set", got)
	}
}

func TestNewConnectionAloneIsNotActivity(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	us.addConn("1.1.1.1")
	if got := us.aliveIPList(&rt); got["1.1.1.1"] {
		t.Fatalf("bare connections counted as a device: %v", got)
	}
	if us.ips["1.1.1.1"] != 2 {
		t.Fatalf("refcount = %d, want 2", us.ips["1.1.1.1"])
	}
}

func TestThresholdBoundary(t *testing.T) {
	now := bucketStart()
	us := &userStats{ips: make(map[string]int)}
	below := us.addConn("1.1.1.1")
	exact := us.addConn("2.2.2.2")
	below.addAt(deviceMinBytes-1, now, true)
	exact.addAt(deviceMinBytes, now, true)

	got := us.activeIPs(now, &rt)
	if _, ok := got["1.1.1.1"]; ok {
		t.Fatal("IP just under the threshold counted")
	}
	if _, ok := got["2.2.2.2"]; !ok {
		t.Fatal("IP at the threshold not counted")
	}
}

func TestBytesSpreadAcrossBucketsAdd(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(60*kb, t0, true)
	a.addAt(60*kb, t0.Add(30*time.Second), true)
	if got := a.sumAt(t0.Add(30*time.Second), 6); got != 120*kb {
		t.Fatalf("sum = %d, want %d", got, 120*kb)
	}
}

func TestWindowExpiresAfterSixtySeconds(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(deviceMinBytes, t0, true)

	if got := a.sumAt(t0.Add(59*time.Second), 6); got != deviceMinBytes {
		t.Fatalf("at 59s: sum = %d, want %d", got, deviceMinBytes)
	}
	if got := a.sumAt(t0.Add(60*time.Second), 6); got != 0 {
		t.Fatalf("at 60s: sum = %d, want 0", got)
	}
}

func TestOldBytesDoNotAddToNewOnes(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(60*kb, t0, true)
	a.addAt(60*kb, t0.Add(70*time.Second), true) // first bucket expired
	if got := a.sumAt(t0.Add(70*time.Second), 6); got != 60*kb {
		t.Fatalf("sum = %d, want %d: expired bucket leaked into the window", got, 60*kb)
	}
}

func TestReusedBucketSlotIsReset(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(90*kb, t0, true)
	// deviceBucketMax buckets later the same slot is reused and must start from zero.
	a.addAt(20*kb, t0.Add(deviceBucketMax*deviceBucketDur), true)
	if got := a.sumAt(t0.Add(deviceBucketMax*deviceBucketDur), 6); got != 20*kb {
		t.Fatalf("sum = %d, want %d", got, 20*kb)
	}
}

func TestIPActivityNilAndEmptyAreSafe(t *testing.T) {
	var a *ipActivity
	a.add(1) // must not panic
	b := new(ipActivity)
	b.add(0)
	b.add(-5)
	if got := b.sumAt(time.Now(), 6); got != 0 {
		t.Fatalf("sum = %d, want 0", got)
	}
}

func TestRemoveConnDropsActivity(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	us.addConn("1.1.1.1")
	us.removeConn("1.1.1.1")
	if _, ok := us.act["1.1.1.1"]; !ok {
		t.Fatal("activity dropped while a connection from the IP remains")
	}
	us.removeConn("1.1.1.1")
	if _, ok := us.act["1.1.1.1"]; ok {
		t.Fatal("activity kept after the last connection closed")
	}
}

func TestCheckDeviceGateIgnoresQuietIPs(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	us := tracker.users[1]

	// 1.1.1.1 holds a connection but moves nothing, so it does not use up
	// the single slot.
	us.addConn("1.1.1.1")
	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 1) {
		t.Fatal("new device rejected although the only other IP is quiet")
	}

	// Once 2.2.2.2 moves real traffic, the quiet device coming back is the
	// extra one.
	us.addConn("2.2.2.2").add(deviceMinBytes)
	if !tracker.checkDeviceGate(us, 1, "1.1.1.1", 1) {
		t.Fatal("quiet device returning at the limit should be rejected")
	}
	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 1) {
		t.Fatal("active device must stay admitted")
	}
}

func TestRoutedConnectionCountsReadsAndWrites(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{make([]byte, 60*kb)}}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	us := tracker.users[1]

	if got := us.aliveIPList(&rt); got["1.1.1.1"] {
		t.Fatalf("no traffic yet but IP counted: %v", got)
	}
	if _, err := wrapped.Read(make([]byte, 64*kb)); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := us.aliveIPList(&rt); got["1.1.1.1"] {
		t.Fatalf("60 KB is under the threshold: %v", got)
	}
	if _, err := wrapped.Write(make([]byte, 50*kb)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := us.aliveIPList(&rt); !got["1.1.1.1"] {
		t.Fatalf("60 KB read + 50 KB written should pass 100 KB: %v", got)
	}
}

func TestGetUserTrafficReportsEmptySetForQuietUser(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.SetUserMap(map[string]int{"uuid-1": 1, "uuid-2": 2})
	base := &testConn{}
	tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)

	_, aliveIPs, connCount := tracker.GetUserTraffic()
	if connCount != 1 {
		t.Fatalf("connCount = %d, want 1", connCount)
	}
	set, ok := aliveIPs[1]
	if !ok || set == nil || len(set) != 0 {
		t.Fatalf("quiet user: aliveIPs[1] = %v (present %v), want an empty set", set, ok)
	}
	if _, ok := aliveIPs[2]; ok {
		t.Fatal("user without connections must stay out of the report")
	}
}
