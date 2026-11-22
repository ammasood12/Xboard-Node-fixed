package singbox

import (
	"context"
	"testing"
	"time"
)

const kb = 1024

// bucketStart returns a time aligned to the start of a bucket, so window edges
// in the tests are exact.
func bucketStart() time.Time {
	const dur = int64(deviceBucketDur)
	return time.Unix(0, (time.Now().UnixNano()/dur)*dur)
}

func TestAliveIPListCountsOnlyIPsOverThreshold(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	if got := us.aliveIPList(); got != nil {
		t.Fatalf("no connections: got %v, want nil", got)
	}

	busy := us.addConn("1.1.1.1")
	us.addConn("2.2.2.2") // connected, moved nothing
	busy.add(deviceMinBytes)

	got := us.aliveIPList()
	if len(got) != 1 || !got["1.1.1.1"] {
		t.Fatalf("got %v, want only 1.1.1.1", got)
	}
}

func TestAliveIPListEmptySetWhenNobodyIsActive(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	got := us.aliveIPList()
	if got == nil || len(got) != 0 {
		t.Fatalf("connections but no traffic: got %v, want an empty non-nil set", got)
	}
}

func TestNewConnectionAloneIsNotActivity(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	us.addConn("1.1.1.1")
	if got := us.aliveIPList(); got["1.1.1.1"] {
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
	below.addAt(deviceMinBytes-1, now)
	exact.addAt(deviceMinBytes, now)

	got := us.activeIPs(now)
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
	a.addAt(60*kb, t0)
	a.addAt(60*kb, t0.Add(30*time.Second))
	if got := a.sumAt(t0.Add(30 * time.Second)); got != 120*kb {
		t.Fatalf("sum = %d, want %d", got, 120*kb)
	}
}

func TestWindowExpiresAfterSixtySeconds(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(deviceMinBytes, t0)

	if got := a.sumAt(t0.Add(59 * time.Second)); got != deviceMinBytes {
		t.Fatalf("at 59s: sum = %d, want %d", got, deviceMinBytes)
	}
	if got := a.sumAt(t0.Add(60 * time.Second)); got != 0 {
		t.Fatalf("at 60s: sum = %d, want 0", got)
	}
}

func TestOldBytesDoNotAddToNewOnes(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(60*kb, t0)
	a.addAt(60*kb, t0.Add(70*time.Second)) // first bucket expired
	if got := a.sumAt(t0.Add(70 * time.Second)); got != 60*kb {
		t.Fatalf("sum = %d, want %d: expired bucket leaked into the window", got, 60*kb)
	}
}

func TestReusedBucketSlotIsReset(t *testing.T) {
	t0 := bucketStart()
	a := new(ipActivity)
	a.addAt(90*kb, t0)
	// Six buckets later the same slot is reused and must start from zero.
	a.addAt(20*kb, t0.Add(deviceBucketN*deviceBucketDur))
	if got := a.sumAt(t0.Add(deviceBucketN * deviceBucketDur)); got != 20*kb {
		t.Fatalf("sum = %d, want %d", got, 20*kb)
	}
}

func TestIPActivityNilAndEmptyAreSafe(t *testing.T) {
	var a *ipActivity
	a.add(1) // must not panic
	b := new(ipActivity)
	b.add(0)
	b.add(-5)
	if got := b.sumAt(time.Now()); got != 0 {
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
	tracker := NewConnTracker(0)
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
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{make([]byte, 60*kb)}}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	us := tracker.users[1]

	if got := us.aliveIPList(); got["1.1.1.1"] {
		t.Fatalf("no traffic yet but IP counted: %v", got)
	}
	if _, err := wrapped.Read(make([]byte, 64*kb)); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := us.aliveIPList(); got["1.1.1.1"] {
		t.Fatalf("60 KB is under the threshold: %v", got)
	}
	if _, err := wrapped.Write(make([]byte, 50*kb)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := us.aliveIPList(); !got["1.1.1.1"] {
		t.Fatalf("60 KB read + 50 KB written should pass 100 KB: %v", got)
	}
}

func TestGetUserTrafficReportsEmptySetForQuietUser(t *testing.T) {
	tracker := NewConnTracker(0)
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
