package singbox

import (
	"context"
	"testing"
	"time"
)

// backdate sets ip's activity stamp to d ago.
func backdate(us *userStats, ip string, d time.Duration) {
	us.mu.Lock()
	us.act[ip].Store(time.Now().Add(-d).UnixNano())
	us.mu.Unlock()
}

func TestAliveIPListDropsIdleIPs(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	if got := us.aliveIPList(); got != nil {
		t.Fatalf("no connections: got %v, want nil", got)
	}

	us.addConn("1.1.1.1")
	us.addConn("2.2.2.2")
	backdate(us, "2.2.2.2", deviceActiveWindow+time.Second)

	got := us.aliveIPList()
	if len(got) != 1 || !got["1.1.1.1"] {
		t.Fatalf("one idle IP: got %v, want only 1.1.1.1", got)
	}

	backdate(us, "1.1.1.1", deviceActiveWindow+time.Second)
	got = us.aliveIPList()
	if got == nil || len(got) != 0 {
		t.Fatalf("all IPs idle: got %v, want an empty non-nil set", got)
	}
}

func TestIPInsideWindowStaysActive(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	backdate(us, "1.1.1.1", deviceActiveWindow-10*time.Second)
	if got := us.aliveIPList(); !got["1.1.1.1"] {
		t.Fatalf("IP idle for less than the window dropped: %v", got)
	}
}

func TestTouchActivityRefreshesIdleIP(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	cell := us.addConn("1.1.1.1")
	backdate(us, "1.1.1.1", deviceActiveWindow+time.Second)
	if got := us.aliveIPList(); got["1.1.1.1"] {
		t.Fatalf("IP should be idle, got %v", got)
	}

	touchActivity(cell)
	if got := us.aliveIPList(); !got["1.1.1.1"] {
		t.Fatalf("client data did not revive the IP: %v", got)
	}
}

func TestTouchActivityLeavesFreshStampAlone(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	cell := us.addConn("1.1.1.1")
	fresh := time.Now().Add(-500 * time.Millisecond).UnixNano()
	cell.Store(fresh)
	touchActivity(cell)
	if cell.Load() != fresh {
		t.Fatal("stamp younger than a second was rewritten")
	}
	touchActivity(nil) // must not panic
}

func TestNewConnectionRevivesIdleIP(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	backdate(us, "1.1.1.1", deviceActiveWindow+time.Second)

	us.addConn("1.1.1.1")
	if got := us.aliveIPList(); !got["1.1.1.1"] {
		t.Fatalf("new connection did not revive the IP: %v", got)
	}
	if us.ips["1.1.1.1"] != 2 {
		t.Fatalf("refcount = %d, want 2", us.ips["1.1.1.1"])
	}
}

func TestRemoveConnDropsActivityStamp(t *testing.T) {
	us := &userStats{ips: make(map[string]int)}
	us.addConn("1.1.1.1")
	us.addConn("1.1.1.1")
	us.removeConn("1.1.1.1")
	if _, ok := us.act["1.1.1.1"]; !ok {
		t.Fatal("stamp dropped while a connection from the IP remains")
	}
	us.removeConn("1.1.1.1")
	if _, ok := us.act["1.1.1.1"]; ok {
		t.Fatal("stamp kept after the last connection closed")
	}
}

func TestCheckDeviceGateIgnoresIdleIPs(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	us := tracker.users[1]

	// 1.1.1.1 holds a connection but has been silent past the window, so it
	// does not use up the single slot.
	us.addConn("1.1.1.1")
	backdate(us, "1.1.1.1", deviceActiveWindow+time.Second)
	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 1) {
		t.Fatal("new device rejected although the only other IP is idle")
	}

	// Once 2.2.2.2 is active, the idle device coming back is the extra one.
	us.addConn("2.2.2.2")
	if !tracker.checkDeviceGate(us, 1, "1.1.1.1", 1) {
		t.Fatal("idle device returning at the limit should be rejected")
	}
	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 1) {
		t.Fatal("active device must stay admitted")
	}
}

func TestRoutedConnectionClientDataRefreshesActivity(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{[]byte("hello")}}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)

	us := tracker.users[1]
	backdate(us, "1.1.1.1", deviceActiveWindow+time.Second)
	if got := us.aliveIPList(); got["1.1.1.1"] {
		t.Fatalf("IP should be idle, got %v", got)
	}

	if _, err := wrapped.Read(make([]byte, 16)); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := us.aliveIPList(); !got["1.1.1.1"] {
		t.Fatalf("client data did not revive the IP: %v", got)
	}
}

func TestGetUserTrafficReportsEmptySetForIdleUser(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1, "uuid-2": 2})
	base := &testConn{}
	tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	backdate(tracker.users[1], "1.1.1.1", deviceActiveWindow+time.Second)

	_, aliveIPs, connCount := tracker.GetUserTraffic()
	if connCount != 1 {
		t.Fatalf("connCount = %d, want 1", connCount)
	}
	set, ok := aliveIPs[1]
	if !ok || set == nil || len(set) != 0 {
		t.Fatalf("idle user: aliveIPs[1] = %v (present %v), want an empty set", set, ok)
	}
	if _, ok := aliveIPs[2]; ok {
		t.Fatal("user without connections must stay out of the report")
	}
}
