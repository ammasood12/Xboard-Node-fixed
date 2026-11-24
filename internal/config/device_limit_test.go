package config

import (
	"strings"
	"testing"
	"time"
)

func TestDeviceLimitDefaults(t *testing.T) {
	r := (*DeviceLimitConfig)(nil).Runtime()
	if r.CountMethod != CountRecentConnection || r.RecentConnection != 180*time.Second {
		t.Errorf("method %s / %v", r.CountMethod, r.RecentConnection)
	}
	if r.MinTrafficBytes != 100*1024 || r.TrafficCheckSeconds != 60 {
		t.Errorf("traffic defaults %d / %d", r.MinTrafficBytes, r.TrafficCheckSeconds)
	}
	if !r.Block || r.Merge || !r.RejectNewImmediately {
		t.Errorf("fixed3 defaults: blocking on (the panel limit decides), merge off, immediate reject on: %+v", r)
	}
	if !r.LogOverLimit || !r.ClearPanelWhenUserIdle {
		t.Errorf("log and clear-idle defaults must be on: %+v", r)
	}
	if r.KickAfter != 0 || r.Watch != 10*time.Minute || r.TCPKeepaliveSeconds != 180 {
		t.Errorf("timing defaults: %v %v %d", r.KickAfter, r.Watch, r.TCPKeepaliveSeconds)
	}
	if !(*SyncConfig)(nil).RESTPolling() {
		t.Error("rest_polling_with_websocket must default to true")
	}
}

func TestDeviceLimitExplicitZeroAndFalseAreKept(t *testing.T) {
	zero, f := 0, false
	c := &DeviceLimitConfig{BlockDevices: &f, OverLimitKickAfterMinutes: &zero, TCPKeepaliveSeconds: &zero, LogOverLimitDevices: &f, ClearPanelWhenUserIdle: &f}
	r := c.Runtime()
	if r.Block || r.KickAfter != 0 || r.TCPKeepaliveSeconds != 0 || r.LogOverLimit || r.ClearPanelWhenUserIdle {
		t.Errorf("explicit zero/false overridden by defaults: %+v", r)
	}
	s := &SyncConfig{RESTPollingWithWebsocket: &f}
	if s.RESTPolling() {
		t.Error("explicit false ignored")
	}
}

func TestDeviceLimitValidate(t *testing.T) {
	five, zero, tooBig := 5, 0, 99
	tests := []struct {
		name string
		cfg  DeviceLimitConfig
		want string // substring of the error, "" = valid
	}{
		{"empty", DeviceLimitConfig{}, ""},
		{"traffic", DeviceLimitConfig{CountMethod: "traffic", MinTrafficKB: 250, TrafficCheckSeconds: 60}, ""},
		{"bad method", DeviceLimitConfig{CountMethod: "bytes"}, "count_method"},
		{"bad counting", DeviceLimitConfig{IPv4IPv6Counting: "union"}, "ipv4_ipv6_counting"},
		{"window not multiple of 10", DeviceLimitConfig{TrafficCheckSeconds: 45}, "traffic_check_seconds"},
		{"window too long", DeviceLimitConfig{TrafficCheckSeconds: 130}, "traffic_check_seconds"},
		{"negative kb", DeviceLimitConfig{MinTrafficKB: -1}, "min_traffic_kb"},
		{"recent too short", DeviceLimitConfig{RecentConnectionSeconds: 5}, "recent_connection_seconds"},
		{"kick too long", DeviceLimitConfig{OverLimitKickAfterMinutes: &tooBig}, "over_limit_kick_after_minutes"},
		{"watch smaller than kick", DeviceLimitConfig{OverLimitKickAfterMinutes: &five, OverLimitWatchMinutes: 3}, "over_limit_watch_minutes"},
		{"watch small with default kick", DeviceLimitConfig{OverLimitWatchMinutes: 3}, ""},
		{"kick off needs no watch", DeviceLimitConfig{OverLimitKickAfterMinutes: &zero, OverLimitWatchMinutes: 3}, ""},
		{"keepalive off", DeviceLimitConfig{TCPKeepaliveSeconds: &zero}, ""},
		{"bad ignore", DeviceLimitConfig{IgnoreIPs: []string{"not-an-ip"}}, "ignore_ips"},
		{"ignore ok", DeviceLimitConfig{IgnoreIPs: []string{"10.0.0.0/8", "2605:e440:b::302"}}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
	if err := (*DeviceLimitConfig)(nil).Validate(); err != nil {
		t.Fatalf("nil config: %v", err)
	}
}

func TestIgnored(t *testing.T) {
	r := (&DeviceLimitConfig{IgnoreIPs: []string{"10.0.0.0/8", "2605:e440:b::302", "192.168.1.5"}}).Runtime()
	for ip, want := range map[string]bool{
		"10.1.2.3":         true,
		"11.1.2.3":         false,
		"2605:e440:b::302": true,
		"2605:e440:b::303": false,
		"192.168.1.5":      true,
		"192.168.1.6":      false,
		"::ffff:10.1.2.3":  true,
		"not-an-ip":        false,
	} {
		if got := r.Ignored(ip); got != want {
			t.Errorf("Ignored(%q) = %v, want %v", ip, got, want)
		}
	}
	if (&DeviceLimitConfig{}).Runtime().Ignored("1.1.1.1") {
		t.Error("empty ignore list ignored an IP")
	}
}

func TestSummary(t *testing.T) {
	r := (&DeviceLimitConfig{CountMethod: "traffic", IPv4IPv6Counting: "merge"}).Runtime()
	s := r.Summary()
	for _, want := range []string{"method=traffic", "min=100KB/60s", "ipv4_ipv6=merge", "block=true", "kick_after=5m/10m"} {
		if !strings.Contains(s, want) {
			t.Errorf("summary %q missing %q", s, want)
		}
	}
}
