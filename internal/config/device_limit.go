package config

import (
	"bytes"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Count methods for device_limit.count_method.
const (
	// CountRecentConnection: an IP is a device while it sent any data within
	// recent_connection_seconds (fixed3).
	CountRecentConnection = "recent_connection"
	// CountTraffic: an IP is a device only while it moved at least
	// min_traffic_kb within traffic_check_seconds (fixed4).
	CountTraffic = "traffic"
	// CountOff: no device counting or blocking; every connected IP is reported.
	CountOff = "off"
)

// IPv4/IPv6 counting modes for device_limit.ipv4_ipv6_counting.
const (
	CountSeparate = "separate"
	CountMerge    = "merge"
)

// SyncConfig tunes how the node syncs with the panel.
//
//	sync:
//	  rest_polling_with_websocket: true
type SyncConfig struct {
	// RESTPollingWithWebsocket keeps the periodic REST pull running while the
	// websocket is connected (fixed2). nil = true.
	RESTPollingWithWebsocket *bool `yaml:"rest_polling_with_websocket,omitempty"`
}

// RESTPolling reports whether REST polling continues while WS is connected.
func (c *SyncConfig) RESTPolling() bool {
	if c == nil || c.RESTPollingWithWebsocket == nil {
		return true
	}
	return *c.RESTPollingWithWebsocket
}

// DeviceLimitConfig selects how devices are counted and what happens to
// devices over a user's limit. It applies to the sing-box kernel; xray keeps
// its own device handling. The limit itself always comes from the panel (per
// user or package); with the zero value the node enforces it with fixed3
// counting, after the delayed over-limit check.
type DeviceLimitConfig struct {
	// CountMethod: recent_connection | traffic | off. Default recent_connection.
	CountMethod string `yaml:"count_method,omitempty"`

	// RecentConnectionSeconds is used when count_method is recent_connection.
	// Default 180.
	RecentConnectionSeconds int `yaml:"recent_connection_seconds,omitempty"`

	// MinTrafficKB and TrafficCheckSeconds are used when count_method is
	// traffic: an IP must move at least MinTrafficKB within the last
	// TrafficCheckSeconds. Defaults 100 and 60 (multiple of 10, 10..120).
	MinTrafficKB        int `yaml:"min_traffic_kb,omitempty"`
	TrafficCheckSeconds int `yaml:"traffic_check_seconds,omitempty"`

	// IPv4IPv6Counting: separate | merge. merge counts a user's devices as
	// max(active IPv4 devices, active IPv6 devices), so one phone that shows
	// up on both address families counts once. Default separate.
	IPv4IPv6Counting string `yaml:"ipv4_ipv6_counting,omitempty"`

	// BlockDevices makes the node really reject and disconnect devices over
	// the limit the panel sets for a user (package). Users without a panel
	// device limit are never affected. false = only count (and log, see
	// LogOverLimitDevices). nil = true: the admin panel decides.
	BlockDevices *bool `yaml:"block_devices,omitempty"`

	// LogOverLimitDevices logs every device found over the limit, including
	// the action taken ("none" when BlockDevices is false). nil = true.
	LogOverLimitDevices *bool `yaml:"log_over_limit_devices,omitempty"`

	// RejectNewDeviceImmediately refuses a new IP at once when the user is
	// already at the limit. false = a new IP is let in and only handled by the
	// delayed over-limit check below (or by the reject check once the user has
	// been over the limit for a while).
	RejectNewDeviceImmediately bool `yaml:"reject_new_device_immediately,omitempty"`

	// OverLimitKickAfterMinutes: kick only if the user was over the limit for
	// at least this long within the last OverLimitWatchMinutes. 0 = kick at
	// once (old behaviour). nil = 5. OverLimitWatchMinutes nil/0 = 10.
	OverLimitKickAfterMinutes *int `yaml:"over_limit_kick_after_minutes,omitempty"`
	OverLimitWatchMinutes     int  `yaml:"over_limit_watch_minutes,omitempty"`

	// TCPKeepaliveSeconds closes dead client sockets (0 = off). nil = 180.
	TCPKeepaliveSeconds *int `yaml:"tcp_keepalive_seconds,omitempty"`

	// ClearPanelWhenUserIdle: a user whose connections are all idle reports an
	// empty device list so the panel drops the stale entries. nil = true.
	ClearPanelWhenUserIdle *bool `yaml:"clear_panel_when_user_idle,omitempty"`

	// IgnoreIPs never count as a device and are never blocked (relays, own
	// servers). Entries are IPs or CIDR prefixes.
	IgnoreIPs []string `yaml:"ignore_ips,omitempty"`
}

// DeviceRuntime is DeviceLimitConfig with every default applied.
type DeviceRuntime struct {
	CountMethod            string
	RecentConnection       time.Duration
	MinTrafficBytes        int64
	TrafficCheckSeconds    int
	Merge                  bool
	Block                  bool
	LogOverLimit           bool
	RejectNewImmediately   bool
	KickAfter              time.Duration
	Watch                  time.Duration
	TCPKeepaliveSeconds    int
	ClearPanelWhenUserIdle bool
	IgnorePrefixes         []netip.Prefix
}

func intOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

func firstInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// Runtime resolves defaults. It is safe on a nil receiver.
func (c *DeviceLimitConfig) Runtime() DeviceRuntime {
	if c == nil {
		c = &DeviceLimitConfig{}
	}
	r := DeviceRuntime{
		CountMethod:            c.CountMethod,
		RecentConnection:       time.Duration(firstInt(c.RecentConnectionSeconds, 180)) * time.Second,
		MinTrafficBytes:        int64(firstInt(c.MinTrafficKB, 100)) * 1024,
		TrafficCheckSeconds:    firstInt(c.TrafficCheckSeconds, 60),
		Merge:                  c.IPv4IPv6Counting == CountMerge,
		Block:                  boolOr(c.BlockDevices, true),
		LogOverLimit:           boolOr(c.LogOverLimitDevices, true),
		RejectNewImmediately:   c.RejectNewDeviceImmediately,
		KickAfter:              time.Duration(intOr(c.OverLimitKickAfterMinutes, 5)) * time.Minute,
		Watch:                  time.Duration(firstInt(c.OverLimitWatchMinutes, 10)) * time.Minute,
		TCPKeepaliveSeconds:    intOr(c.TCPKeepaliveSeconds, 180),
		ClearPanelWhenUserIdle: boolOr(c.ClearPanelWhenUserIdle, true),
	}
	if r.CountMethod == "" {
		r.CountMethod = CountRecentConnection
	}
	for _, s := range c.IgnoreIPs {
		if p, err := parseIPOrPrefix(s); err == nil {
			r.IgnorePrefixes = append(r.IgnorePrefixes, p)
		}
	}
	return r
}

func parseIPOrPrefix(s string) (netip.Prefix, error) {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, err
		}
		return p.Masked(), nil
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	a = a.Unmap()
	return netip.PrefixFrom(a, a.BitLen()), nil
}

// Validate rejects values the node cannot run with.
func (c *DeviceLimitConfig) Validate() error {
	if c == nil {
		return nil
	}
	switch c.CountMethod {
	case "", CountRecentConnection, CountTraffic, CountOff:
	default:
		return fmt.Errorf("device_limit.count_method must be recent_connection, traffic or off, got %q", c.CountMethod)
	}
	switch c.IPv4IPv6Counting {
	case "", CountSeparate, CountMerge:
	default:
		return fmt.Errorf("device_limit.ipv4_ipv6_counting must be separate or merge, got %q", c.IPv4IPv6Counting)
	}
	if v := c.RecentConnectionSeconds; v != 0 && (v < 10 || v > 3600) {
		return fmt.Errorf("device_limit.recent_connection_seconds must be 10..3600, got %d", v)
	}
	if v := c.MinTrafficKB; v != 0 && (v < 1 || v > 102400) {
		return fmt.Errorf("device_limit.min_traffic_kb must be 1..102400, got %d", v)
	}
	if v := c.TrafficCheckSeconds; v != 0 && (v < 10 || v > 120 || v%10 != 0) {
		return fmt.Errorf("device_limit.traffic_check_seconds must be a multiple of 10 between 10 and 120, got %d", v)
	}
	if c.OverLimitKickAfterMinutes != nil {
		if v := *c.OverLimitKickAfterMinutes; v < 0 || v > 60 {
			return fmt.Errorf("device_limit.over_limit_kick_after_minutes must be 0..60, got %d", v)
		}
	}
	if v := c.OverLimitWatchMinutes; v != 0 && (v < 1 || v > 120) {
		return fmt.Errorf("device_limit.over_limit_watch_minutes must be 1..120, got %d", v)
	}
	if c.OverLimitKickAfterMinutes != nil && *c.OverLimitKickAfterMinutes > 0 {
		watch := firstInt(c.OverLimitWatchMinutes, 10)
		if watch < *c.OverLimitKickAfterMinutes {
			return fmt.Errorf("device_limit.over_limit_watch_minutes (%d) must not be smaller than over_limit_kick_after_minutes (%d)",
				watch, *c.OverLimitKickAfterMinutes)
		}
	}
	if c.OverLimitKickAfterMinutes == nil {
		if watch := firstInt(c.OverLimitWatchMinutes, 10); watch < 5 {
			return fmt.Errorf("device_limit.over_limit_watch_minutes (%d) must not be smaller than the default over_limit_kick_after_minutes (5)", watch)
		}
	}
	if c.TCPKeepaliveSeconds != nil {
		if v := *c.TCPKeepaliveSeconds; v != 0 && (v < 10 || v > 3600) {
			return fmt.Errorf("device_limit.tcp_keepalive_seconds must be 0 or 10..3600, got %d", v)
		}
	}
	for _, s := range c.IgnoreIPs {
		if _, err := parseIPOrPrefix(s); err != nil {
			return fmt.Errorf("device_limit.ignore_ips: %q is not an IP or CIDR", s)
		}
	}
	return nil
}

// Ignored reports whether ip matches device_limit.ignore_ips.
func (r DeviceRuntime) Ignored(ip string) bool {
	if len(r.IgnorePrefixes) == 0 {
		return false
	}
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	a = a.Unmap()
	for _, p := range r.IgnorePrefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// Summary is a one-line description of the effective settings.
func (r DeviceRuntime) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "method=%s", r.CountMethod)
	switch r.CountMethod {
	case CountTraffic:
		fmt.Fprintf(&b, " min=%dKB/%ds", r.MinTrafficBytes/1024, r.TrafficCheckSeconds)
	case CountRecentConnection:
		fmt.Fprintf(&b, " recent=%ds", int(r.RecentConnection/time.Second))
	}
	merge := "separate"
	if r.Merge {
		merge = "merge"
	}
	fmt.Fprintf(&b, " ipv4_ipv6=%s block=%t log=%t reject_new=%t kick_after=%dm/%dm keepalive=%ds clear_idle=%t ignore_ips=%d",
		merge, r.Block, r.LogOverLimit, r.RejectNewImmediately,
		int(r.KickAfter/time.Minute), int(r.Watch/time.Minute),
		r.TCPKeepaliveSeconds, r.ClearPanelWhenUserIdle, len(r.IgnorePrefixes))
	return b.String()
}

// CheckDeviceKeys decodes the sync: and device_limit: sections of a config
// file strictly and returns one message per unknown key. yaml ignores typos
// such as "block_device", which would silently leave a node in dry-run mode.
func CheckDeviceKeys(data []byte) []string {
	var doc struct {
		Sync        yaml.Node `yaml:"sync"`
		DeviceLimit yaml.Node `yaml:"device_limit"`
		Instances   []struct {
			Sync        yaml.Node `yaml:"sync"`
			DeviceLimit yaml.Node `yaml:"device_limit"`
		} `yaml:"instances"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil
	}
	var out []string
	check := func(where string, n yaml.Node, dst any) {
		if n.Kind == 0 {
			return
		}
		raw, err := yaml.Marshal(&n)
		if err != nil {
			return
		}
		dec := yaml.NewDecoder(bytes.NewReader(raw))
		dec.KnownFields(true)
		if err := dec.Decode(dst); err != nil {
			out = append(out, fmt.Sprintf("%s: %s", where, strings.ReplaceAll(err.Error(), "\n", "; ")))
		}
	}
	check("sync", doc.Sync, &SyncConfig{})
	check("device_limit", doc.DeviceLimit, &DeviceLimitConfig{})
	for i, inst := range doc.Instances {
		check(fmt.Sprintf("instances[%d].sync", i), inst.Sync, &SyncConfig{})
		check(fmt.Sprintf("instances[%d].device_limit", i), inst.DeviceLimit, &DeviceLimitConfig{})
	}
	return out
}
