package kernel

import (
	"reflect"
	"sort"
	"testing"
)

func set(ips ...string) map[string]int {
	m := make(map[string]int, len(ips))
	for _, ip := range ips {
		m[ip] = 1
	}
	return m
}

func elsewhere(ips ...string) map[string]bool {
	m := make(map[string]bool, len(ips))
	for _, ip := range ips {
		m[ip] = true
	}
	return m
}

func TestCountDevices(t *testing.T) {
	const v6a, v6b = "2409:8a0c:872:4ed0::1", "2409:8a0c:872:4ed0::2"
	tests := []struct {
		name      string
		local     map[string]int
		elsewhere map[string]bool
		union     int
		merged    int
	}{
		{"none", nil, nil, 0, 0},
		{"one phone on v4 and v6", set("1.1.1.1", v6a), nil, 2, 1},
		{"two v4 one v6", set("1.1.1.1", "2.2.2.2", v6a), nil, 3, 2},
		{"v6 only", set(v6a, v6b), nil, 2, 2},
		{"same IP local and elsewhere counts once", set("1.1.1.1"), elsewhere("1.1.1.1"), 1, 1},
		{"elsewhere adds other family", set("1.1.1.1"), elsewhere(v6a), 2, 1},
		{"elsewhere adds same family", set("1.1.1.1"), elsewhere("2.2.2.2"), 2, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CountDevices(tc.local, tc.elsewhere, false); got != tc.union {
				t.Errorf("union = %d, want %d", got, tc.union)
			}
			if got := CountDevices(tc.local, tc.elsewhere, true); got != tc.merged {
				t.Errorf("merged = %d, want %d", got, tc.merged)
			}
		})
	}
}

func TestAdmitDeviceMerged(t *testing.T) {
	const v6a, v6b = "2409:8a0c:872:4ed0::1", "2409:8a0c:872:4ed0::2"
	// One IPv4 device online, limit 1: a new IPv6 device is the same phone.
	if !AdmitDeviceMerged(set("1.1.1.1"), nil, v6a, 1) {
		t.Error("new IPv6 refused although only one IPv4 device is online")
	}
	// A second IPv4 device is a real second device.
	if AdmitDeviceMerged(set("1.1.1.1"), nil, "2.2.2.2", 1) {
		t.Error("second IPv4 device admitted at the limit")
	}
	// Already online is always kept.
	if !AdmitDeviceMerged(set("1.1.1.1", v6a), nil, v6a, 1) {
		t.Error("online IPv6 device refused")
	}
	// Devices on other nodes count for their own family.
	if AdmitDeviceMerged(nil, elsewhere(v6a), v6b, 1) {
		t.Error("second IPv6 device admitted at the limit")
	}
	if !AdmitDeviceMerged(nil, elsewhere(v6a), "1.1.1.1", 1) {
		t.Error("IPv4 device refused although only an IPv6 device is online elsewhere")
	}
}

func TestOverLimitDevicesMerged(t *testing.T) {
	const v6a, v6b = "2409:8a0c:872:4ed0::1", "2409:8a0c:872:4ed0::2"
	got := OverLimitDevicesMerged(set("1.1.1.1", "2.2.2.2", v6a, v6b), nil, 1)
	sort.Strings(got)
	want := []string{"2.2.2.2", v6b}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := OverLimitDevicesMerged(set("1.1.1.1", v6a), nil, 1); len(got) != 0 {
		t.Errorf("one phone on both families kicked: %v", got)
	}
	// Only local IPs are ever returned, even when elsewhere is over.
	if got := OverLimitDevicesMerged(set("9.9.9.9"), elsewhere("1.1.1.1"), 1); !reflect.DeepEqual(got, []string{"9.9.9.9"}) {
		t.Errorf("got %v, want [9.9.9.9]", got)
	}
}
