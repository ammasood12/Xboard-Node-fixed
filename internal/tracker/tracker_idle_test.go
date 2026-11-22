package tracker

import "testing"

func TestProcess_IdleUserReportsEmptyListButNotOnline(t *testing.T) {
	tr := New()
	tr.Process(nil, map[int]map[string]bool{
		1: {},
		2: {"2.2.2.2": true},
	}, 2)

	alive := tr.AliveIPs()
	if ips, ok := alive[1]; !ok || ips == nil || len(ips) != 0 {
		t.Fatalf("idle user must be reported with an empty list, got %v (present %v)", ips, ok)
	}
	if len(alive[2]) != 1 {
		t.Fatalf("active user: got %v, want one IP", alive[2])
	}

	online := tr.CurrentOnline()
	if _, ok := online[1]; ok {
		t.Fatalf("idle user counted online: %v", online)
	}
	if online[2] != 1 {
		t.Fatalf("online[2] = %d, want 1", online[2])
	}
}
