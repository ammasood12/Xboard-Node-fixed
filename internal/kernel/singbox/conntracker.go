package singbox

import (
	"context"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	singM "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/time/rate"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

// ipPool caches ipSnapshot maps to reduce allocations.
var ipPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]struct{}, 16)
	},
}

// ─── Per-user statistics ────────────────────────────────────────────────────

// userStats holds per-user traffic counters and alive IP tracking.
// Traffic counters are lock-free atomics (read on every packet).
// IP tracking uses a lightweight mutex (only touched at connect/disconnect).
type userStats struct {
	upload   atomic.Int64
	download atomic.Int64

	mu        sync.RWMutex           // RWMutex for concurrent reads
	ips       map[string]int         // sourceIP → refcount (number of active conns from that IP)
	act       map[string]*ipActivity // sourceIP → bytes it moved in the recent past
	connCount int                    // total active connections
	departed  map[string]time.Time   // sourceIP → when it last went offline here
}

// What counts as an active device is chosen by device_limit.count_method:
//
//   - recent_connection: an IP is a device while its client sent any data (or
//     connected) within recent_connection_seconds (fixed3).
//   - traffic: an IP is a device only while it moved at least min_traffic_kb
//     within traffic_check_seconds (fixed4). Open but quiet sockets (dead paths
//     after a network switch, idle keepalives, the trickle of a carrier-NAT
//     pool IP) move far less and so do not count, and a bare new connection is
//     not activity either. This mirrors V2bX's DeviceOnlineMinTraffic.
//   - off: every connected IP is a device.
//
// It only decides which IPs are devices; traffic accounting is unchanged.
const (
	deviceBucketDur = 10 * time.Second
	// deviceBucketMax is the longest traffic window kept: 12 buckets = 120 s.
	deviceBucketMax = 12
	// deviceSampleEvery is how often every user's device count is sampled for
	// the delayed over-limit check.
	deviceSampleEvery = 30 * time.Second
	// deviceLogEvery rate-limits the over-limit log lines per user.
	deviceLogEvery = 5 * time.Minute
)

// ipActivity records what one source IP has done recently: a sliding byte
// window of deviceBucketDur buckets (for count_method traffic) and the time
// its client last sent data or connected (for count_method recent_connection).
// Lock-free: it is updated on every read and write of every connection from
// the IP.
type ipActivity struct {
	stamp    [deviceBucketMax]atomic.Int64 // bucket index each slot currently holds
	bytes    [deviceBucketMax]atomic.Int64 // bytes moved in that bucket
	lastRead atomic.Int64                  // unix nanos of the last client data or connect
}

// add records n bytes the client sent now. Safe on a nil receiver.
func (a *ipActivity) add(n int64) { a.addAt(n, time.Now(), true) }

// addWrite records n bytes sent to the client now. Safe on a nil receiver.
func (a *ipActivity) addWrite(n int64) { a.addAt(n, time.Now(), false) }

func (a *ipActivity) addAt(n int64, now time.Time, fromClient bool) {
	if a == nil || n <= 0 {
		return
	}
	if fromClient {
		a.lastRead.Store(now.UnixNano())
	}
	idx := now.UnixNano() / int64(deviceBucketDur)
	i := int(idx % deviceBucketMax)
	if old := a.stamp[i].Load(); old != idx && a.stamp[i].CompareAndSwap(old, idx) {
		a.bytes[i].Store(0) // slot held an expired bucket
	}
	a.bytes[i].Add(n)
}

// touch marks the IP as active now without any bytes (a new connection).
func (a *ipActivity) touch(now time.Time) {
	if a != nil {
		a.lastRead.Store(now.UnixNano())
	}
}

// sumAt returns the bytes moved within the last buckets buckets (the current
// partial bucket plus the previous buckets-1).
func (a *ipActivity) sumAt(now time.Time, buckets int) int64 {
	if buckets <= 0 || buckets > deviceBucketMax {
		buckets = deviceBucketMax
	}
	idx := now.UnixNano() / int64(deviceBucketDur)
	var sum int64
	for i := range a.stamp {
		if s := a.stamp[i].Load(); s > idx-int64(buckets) && s <= idx {
			sum += a.bytes[i].Load()
		}
	}
	return sum
}

// idleFor returns how long ago the client last sent data or connected.
func (a *ipActivity) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, a.lastRead.Load()))
}

// addConn registers a new connection from sourceIP and returns the IP's
// activity window. A new connection is not activity by itself.
func (u *userStats) addConn(sourceIP string) *ipActivity {
	u.mu.Lock()
	u.connCount++
	u.ips[sourceIP]++
	if u.act == nil {
		u.act = make(map[string]*ipActivity)
	}
	cell := u.act[sourceIP]
	if cell == nil {
		cell = new(ipActivity)
		u.act[sourceIP] = cell
	}
	cell.touch(time.Now())
	u.mu.Unlock()
	return cell
}

// removeConn unregisters a connection from sourceIP.
func (u *userStats) removeConn(sourceIP string) {
	u.mu.Lock()
	u.connCount--
	u.ips[sourceIP]--
	if u.ips[sourceIP] <= 0 {
		delete(u.ips, sourceIP)
		delete(u.act, sourceIP)
		u.markDeparted(sourceIP, time.Now())
	}
	u.mu.Unlock()
}

// activeIPs returns the IPs, with their connection counts, that count as
// devices under rule r. An IP with connections but no activity record counts
// as active. IPs in device_limit.ignore_ips never count. The caller holds u.mu.
func (u *userStats) activeIPs(now time.Time, r *config.DeviceRuntime) map[string]int {
	active := make(map[string]int, len(u.ips))
	for ip, n := range u.ips {
		if r.Ignored(ip) {
			continue
		}
		cell := u.act[ip]
		switch {
		case r.CountMethod == config.CountOff, cell == nil:
			active[ip] = n
		case r.CountMethod == config.CountTraffic:
			if cell.sumAt(now, r.TrafficCheckSeconds/10) >= r.MinTrafficBytes {
				active[ip] = n
			}
		default: // recent_connection
			if cell.idleFor(now) <= r.RecentConnection {
				active[ip] = n
			}
		}
	}
	return active
}

// markDeparted records that sourceIP just went offline here and forgets
// departures too old to match any global state that is still fresh. The
// caller holds u.mu.
func (u *userStats) markDeparted(sourceIP string, now time.Time) {
	if u.departed == nil {
		u.departed = make(map[string]time.Time)
	}
	cutoff := now.Add(-kernel.DeviceEchoWindow - globalDevicesTTL)
	for ip, left := range u.departed {
		if left.Before(cutoff) {
			delete(u.departed, ip)
		}
	}
	u.departed[sourceIP] = now
}

// distinctIPs returns the number of distinct IPs currently connected.
func (u *userStats) distinctIPs() int {
	u.mu.Lock()
	n := len(u.ips)
	u.mu.Unlock()
	return n
}

// ipSnapshot returns a pooled copy of the current IP set.
func (u *userStats) ipSnapshot() map[string]struct{} {
	cp := ipPool.Get().(map[string]struct{})
	for k := range cp {
		delete(cp, k)
	}
	u.mu.Lock()
	for ip := range u.ips {
		cp[ip] = struct{}{}
	}
	u.mu.Unlock()
	return cp
}

// releaseIPSnapshot returns the snapshot to pool.
// Large maps are discarded to prevent memory bloat.
func releaseIPSnapshot(m map[string]struct{}) {
	// Discard large maps to prevent pool memory bloat
	if len(m) > 64 {
		return
	}
	ipPool.Put(m)
}

// aliveIPList returns the set of active IPs as a boolean map (for reporting).
// It returns nil when the user has no connections. When connections exist but
// none of their IPs is active, it returns an empty, non-nil set (reporting
// that empty set is what makes the panel drop the user's stale entries for
// this node), or nil when device_limit.clear_panel_when_user_idle is off.
func (u *userStats) aliveIPList(r *config.DeviceRuntime) map[string]bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if len(u.ips) == 0 {
		return nil
	}
	active := u.activeIPs(time.Now(), r)
	if len(active) == 0 && !r.ClearPanelWhenUserIdle {
		return nil
	}
	result := make(map[string]bool, len(active))
	for ip := range active {
		result[ip] = true
	}
	return result
}

// ─── ConnTracker ────────────────────────────────────────────────────────────

// ConnTracker provides per-user traffic counting, alive IP tracking, device
// limit gate-keeping, and per-user speed limiting for the sing-box kernel.
//
// Architecture: instead of maintaining a per-connection map (O(connections)),
// it maintains per-user atomic counters and IP refcount sets (O(users)).
// This means:
//   - GetUserTraffic is O(users), not O(connections)
//   - No Snapshot/pending buffer needed
//   - No per-connection map lock contention
//   - Bytes are counted directly into per-user atomics via CountFunc callbacks
//
// The per-connection wrapper (trackedConn) is still needed for:
//   - Hooking into sing's ReadCounter/WriteCounter for zero-copy byte counting
//   - Close callback to decrement IP refcounts
//   - Per-connection rate limit token accumulation (amortized WaitN)
type ConnTracker struct {
	usersMu        sync.RWMutex
	users          map[int]*userStats    // userID → stats
	uuidMap        map[string]int        // UUID → userID (for lookup in RoutedConnection)
	connMap        map[string]trackedRef // connID → live conn (force-close and revocation)
	managedInbound string                // node inbound tag; its conns need a current user

	idCounter atomic.Int64

	// speedLimitFunc resolves a user UUID to their download *rate.Limiter.
	speedLimitFunc atomic.Pointer[func(uuid string) *rate.Limiter]
	// uploadLimitFunc resolves a user UUID to their upload *rate.Limiter.
	uploadLimitFunc atomic.Pointer[func(uuid string) *rate.Limiter]

	// deviceLimitFunc resolves a user UUID to their device limit.
	deviceLimitFunc atomic.Pointer[func(uuid string) (int, bool)]

	// Multi-node device state from panel
	globalDevices    map[int]map[string]bool // userID → IP → exists
	globalMu         sync.RWMutex
	globalLastUpdate time.Time

	// rule is the device counting/blocking configuration (device_limit:).
	rule atomic.Pointer[config.DeviceRuntime]

	// Delayed over-limit check: devMu guards overHist (per user, one entry per
	// deviceSampleEvery sample: was the user over the limit), lastSample and
	// lastLog (rate limit of the over-limit log lines).
	devMu      sync.Mutex
	overHist   map[int][]bool
	lastSample time.Time
	lastLog    map[logKey]time.Time
}

type logKey struct {
	uid  int
	kind string
}

// NewConnTracker creates a tracker.
func NewConnTracker(_ int) *ConnTracker {
	return &ConnTracker{
		users:         make(map[int]*userStats),
		uuidMap:       make(map[string]int),
		connMap:       make(map[string]trackedRef),
		globalDevices: make(map[int]map[string]bool),
		overHist:      make(map[int][]bool),
		lastLog:       make(map[logKey]time.Time),
	}
}

// SetDeviceConfig sets the device counting and blocking rule (device_limit:).
func (t *ConnTracker) SetDeviceConfig(r config.DeviceRuntime) {
	t.rule.Store(&r)
}

// deviceRule returns the current rule; the defaults (fixed3 counting,
// blocking off) when none was set.
func (t *ConnTracker) deviceRule() *config.DeviceRuntime {
	if r := t.rule.Load(); r != nil {
		return r
	}
	r := (*config.DeviceLimitConfig)(nil).Runtime()
	t.rule.CompareAndSwap(nil, &r)
	return t.rule.Load()
}

// trackedRef is a live connection as seen by force-close and revocation.
type trackedRef struct {
	conn     io.Closer
	user     string // metadata.User at admission
	uid      int    // panel user ID, 0 when unknown
	sourceIP string
	managed  bool // admitted on the managed inbound
}

// SetManagedInbound names the node inbound whose connections must belong to a
// user in the current user map.
func (t *ConnTracker) SetManagedInbound(tag string) {
	t.usersMu.Lock()
	t.managedInbound = tag
	t.usersMu.Unlock()
}

// SetSpeedLimitFunc configures the per-user download limiter lookup.
func (t *ConnTracker) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) {
	t.speedLimitFunc.Store(&fn)
}

// SetUploadLimitFunc configures the per-user upload limiter lookup.
func (t *ConnTracker) SetUploadLimitFunc(fn func(uuid string) *rate.Limiter) {
	t.uploadLimitFunc.Store(&fn)
}

// limiters returns uuid's upload and download limiters (nil = unlimited).
func (t *ConnTracker) limiters(uuid string) (up, down *rate.Limiter) {
	if f := t.uploadLimitFunc.Load(); f != nil {
		up = (*f)(uuid)
	}
	if f := t.speedLimitFunc.Load(); f != nil {
		down = (*f)(uuid)
	}
	return up, down
}

// SetDeviceLimitFunc configures the per-user device limit lookup for gate-keeping.
func (t *ConnTracker) SetDeviceLimitFunc(fn func(uuid string) (int, bool)) {
	t.deviceLimitFunc.Store(&fn)
}

// SetUserMap replaces the UUID→userID mapping and ensures per-user stats
// structs exist for all users. Connections on the managed inbound whose user
// is no longer present are closed: removing a user from the inbound only
// stops new handshakes, while an already authenticated session (Hysteria2 or
// TUIC QUIC connection, mux) would keep opening streams. Removed users keep
// their stats until those connections finish closing.
func (t *ConnTracker) SetUserMap(m map[string]int) {
	t.usersMu.Lock()
	t.uuidMap = m
	for _, uid := range m {
		if _, ok := t.users[uid]; !ok {
			t.users[uid] = &userStats{ips: make(map[string]int)}
		}
	}
	var revoked []io.Closer
	for _, ref := range t.connMap {
		if _, ok := m[ref.user]; ref.managed && !ok {
			revoked = append(revoked, ref.conn)
		}
	}
	t.usersMu.Unlock()

	// Close outside the lock: Close removes the conn from connMap.
	for _, conn := range revoked {
		conn.Close()
	}
	if len(revoked) > 0 {
		nlog.Core().Info("singbox: closed connections of removed users", "connections", len(revoked))
	}
}

// UpdateGlobalDevices syncs global device state from panel, then disconnects
// local devices the new state shows to be over a user's limit.
func (t *ConnTracker) UpdateGlobalDevices(users map[int][]string) {
	t.globalMu.Lock()
	t.globalDevices = make(map[int]map[string]bool, len(users))
	for uid, ips := range users {
		m := make(map[string]bool, len(ips))
		for _, ip := range ips {
			m[ip] = true
		}
		t.globalDevices[uid] = m
	}
	t.globalLastUpdate = time.Now()
	t.globalMu.Unlock()
	nlog.Core().Debug("global device state updated", "users", len(users))
	t.disconnectOverLimit()
}

// ClearGlobalDevices resets global device state (on WS disconnect).
func (t *ConnTracker) ClearGlobalDevices() {
	t.globalMu.Lock()
	t.globalDevices = make(map[int]map[string]bool)
	t.globalLastUpdate = time.Time{}
	t.globalMu.Unlock()
	nlog.Core().Debug("global device state cleared")
}

// ─── adapter.ConnectionTracker ──────────────────────────────────────────────

// RoutedConnection wraps a TCP conn to count bytes per-user, track IPs,
// and optionally rate-limit. Gate-keeps device limits at connection time.
func (t *ConnTracker) RoutedConnection(
	ctx context.Context, conn net.Conn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) net.Conn {
	uuid := metadata.User
	sourceIP := metadata.Source.Addr.String()

	t.usersMu.RLock()
	uid := t.uuidMap[uuid]
	us := t.users[uid]
	t.usersMu.RUnlock()

	// Device limit gate-keeping
	if dlf := t.deviceLimitFunc.Load(); dlf != nil {
		if limit, hasLimit := (*dlf)(uuid); hasLimit {
			if t.checkDeviceGate(us, uid, sourceIP, limit) {
				nlog.Core().Info("singbox: device limit gate-keep, rejecting connection",
					"user", uuid, "ip", sourceIP, "limit", limit)
				conn.Close()
				return conn
			}
		}
	}

	// Register connection
	var act *ipActivity
	if us != nil {
		act = us.addConn(sourceIP)
	}

	connID := t.nextID()

	upLim, downLim := t.limiters(uuid)

	tracked := &trackedConn{
		Conn:        conn,
		tracker:     t,
		us:          us,
		act:         act,
		userID:      uid,
		connID:      connID,
		sourceIP:    sourceIP,
		upLimiter:   upLim,
		downLimiter: downLim,
		ctx:         ctx,
	}
	if !t.register(connID, tracked, metadata, uid, sourceIP) {
		nlog.Core().Debug("singbox: rejecting connection from removed user", "user", uuid, "ip", sourceIP)
		tracked.Close()
		return conn
	}
	return tracked
}

// RoutedPacketConnection wraps UDP with per-user counting.
func (t *ConnTracker) RoutedPacketConnection(
	ctx context.Context, conn N.PacketConn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) N.PacketConn {
	uuid := metadata.User
	sourceIP := metadata.Source.Addr.String()

	t.usersMu.RLock()
	uid := t.uuidMap[uuid]
	us := t.users[uid]
	t.usersMu.RUnlock()

	// Device limit gate-keeping
	if dlf := t.deviceLimitFunc.Load(); dlf != nil {
		if limit, hasLimit := (*dlf)(uuid); hasLimit {
			if t.checkDeviceGate(us, uid, sourceIP, limit) {
				nlog.Core().Info("singbox: device limit gate-keep, rejecting UDP connection",
					"user", uuid, "ip", sourceIP, "limit", limit)
				conn.Close()
				return conn
			}
		}
	}

	var act *ipActivity
	if us != nil {
		act = us.addConn(sourceIP)
	}

	connID := t.nextID()

	upLim, downLim := t.limiters(uuid)

	tracked := &trackedPacketConn{
		PacketConn:  conn,
		tracker:     t,
		us:          us,
		act:         act,
		userID:      uid,
		connID:      connID,
		sourceIP:    sourceIP,
		upLimiter:   upLim,
		downLimiter: downLim,
		ctx:         ctx,
	}
	if !t.register(connID, tracked, metadata, uid, sourceIP) {
		nlog.Core().Debug("singbox: rejecting UDP connection from removed user", "user", uuid, "ip", sourceIP)
		tracked.Close()
		return conn
	}
	return tracked
}

// register records a live connection unless it arrived on the managed inbound
// for a user missing from the current user map. It shares usersMu with
// SetUserMap, so every connection is either rejected here or seen by the next
// revocation.
func (t *ConnTracker) register(connID string, conn io.Closer, metadata adapter.InboundContext, uid int, sourceIP string) bool {
	t.usersMu.Lock()
	defer t.usersMu.Unlock()
	managed := t.managedInbound != "" && metadata.Inbound == t.managedInbound
	if _, ok := t.uuidMap[metadata.User]; managed && !ok {
		return false
	}
	t.connMap[connID] = trackedRef{conn: conn, user: metadata.User, uid: uid, sourceIP: sourceIP, managed: managed}
	return true
}

// globalDevicesTTL is how long panel-pushed device state stays usable.
const globalDevicesTTL = 60 * time.Second

// checkDeviceGate reports whether a new connection from sourceIP must be
// rejected. An IP already online here or on another node is always kept. A new
// IP that would exceed the limit is rejected at once with
// reject_new_device_immediately, otherwise only while the user has been over
// the limit for a while (see persistentlyOver). With block_devices off nothing
// is rejected; the would-be rejection is only logged.
func (t *ConnTracker) checkDeviceGate(us *userStats, userID int, sourceIP string, limit int) bool {
	r := t.deviceRule()
	if us == nil || limit <= 0 || r.CountMethod == config.CountOff || r.Ignored(sourceIP) {
		return false
	}

	us.mu.RLock()
	now := time.Now()
	local := us.activeIPs(now, r)
	elsewhere := t.devicesElsewhere(us, userID, r)
	us.mu.RUnlock()

	admit := kernel.AdmitDevice(local, elsewhere, sourceIP, limit)
	if r.Merge {
		admit = kernel.AdmitDeviceMerged(local, elsewhere, sourceIP, limit)
	}
	if admit {
		return false
	}
	if !r.RejectNewImmediately && !t.persistentlyOver(userID, r) {
		return false
	}
	action := "none"
	if r.Block {
		action = "rejected"
	}
	t.logOverLimit(userID, "reject", r, local, elsewhere, limit, sourceIP, action)
	return r.Block
}

// devicesElsewhere returns the user's IPs online on other nodes, from the
// panel's global state while it is fresh. That state includes the devices
// this node reported and trails them, so an IP that left this node less than
// kernel.DeviceEchoWindow before the state arrived is taken for an echo of
// this node's own report and left out. Ignored IPs are left out too. The
// caller holds us.mu.
func (t *ConnTracker) devicesElsewhere(us *userStats, userID int, r *config.DeviceRuntime) map[string]bool {
	t.globalMu.RLock()
	received := t.globalLastUpdate
	global := t.globalDevices[userID]
	t.globalMu.RUnlock()
	if time.Since(received) > globalDevicesTTL {
		return nil
	}
	if len(global) == 0 {
		return global
	}
	if len(us.departed) == 0 && len(r.IgnorePrefixes) == 0 {
		return global
	}
	elsewhere := make(map[string]bool, len(global))
	for ip := range global {
		if r.Ignored(ip) {
			continue
		}
		if left, ok := us.departed[ip]; ok && received.Before(left.Add(kernel.DeviceEchoWindow)) {
			continue
		}
		elsewhere[ip] = true
	}
	return elsewhere
}

// persistentlyOver reports whether the user was over the limit in enough of
// the recent samples to be kicked: samples covering at least
// over_limit_kick_after_minutes within the last over_limit_watch_minutes. With over_limit_kick_after_minutes 0 the delay is
// off and this is always false (the kick check then needs no history).
func (t *ConnTracker) persistentlyOver(uid int, r *config.DeviceRuntime) bool {
	if r.KickAfter <= 0 {
		return false
	}
	need := int(r.KickAfter / deviceSampleEvery)
	t.devMu.Lock()
	defer t.devMu.Unlock()
	hits := 0
	for _, over := range t.overHist[uid] {
		if over {
			hits++
		}
	}
	return hits >= need
}

// sampleDevices records, once per deviceSampleEvery, whether each user is over
// the limit, then applies the over-limit check. It runs from GetUserTraffic,
// which the service calls every track interval. A user's history is the last
// over_limit_watch_minutes of samples and is dropped once it holds no
// over-the-limit sample, or after the user was kicked.
func (t *ConnTracker) sampleDevices(now time.Time) {
	r := t.deviceRule()
	if r.CountMethod == config.CountOff {
		return
	}
	t.devMu.Lock()
	if !t.lastSample.IsZero() && now.Sub(t.lastSample) < deviceSampleEvery {
		t.devMu.Unlock()
		return
	}
	t.lastSample = now
	t.devMu.Unlock()

	dlf := t.deviceLimitFunc.Load()
	if dlf == nil {
		return
	}
	watch := int(r.Watch / deviceSampleEvery)
	if watch < 1 {
		watch = 1
	}
	overNow := make(map[int]bool) // sampled users: over the limit right now?
	t.usersMu.RLock()
	for uuid, uid := range t.uuidMap {
		limit, hasLimit := (*dlf)(uuid)
		us := t.users[uid]
		if !hasLimit || limit <= 0 || us == nil {
			continue
		}
		us.mu.RLock()
		local := us.activeIPs(now, r)
		elsewhere := t.devicesElsewhere(us, uid, r)
		us.mu.RUnlock()
		overNow[uid] = kernel.CountDevices(local, elsewhere, r.Merge) > limit
	}
	t.usersMu.RUnlock()

	t.devMu.Lock()
	for uid := range t.overHist {
		if _, ok := overNow[uid]; !ok {
			delete(t.overHist, uid)
		}
	}
	for uid, over := range overNow {
		h := append(t.overHist[uid], over)
		if len(h) > watch {
			h = h[len(h)-watch:]
		}
		any := false
		for _, v := range h {
			any = any || v
		}
		if any {
			t.overHist[uid] = h
		} else {
			delete(t.overHist, uid)
		}
	}
	t.devMu.Unlock()

	t.disconnectOverLimit()
}

// logOverLimit writes one rate-limited line (per user and kind) describing a
// device found over the limit and what was done about it.
func (t *ConnTracker) logOverLimit(uid int, kind string, r *config.DeviceRuntime,
	local map[string]int, elsewhere map[string]bool, limit int, ips, action string) {
	if !r.LogOverLimit {
		return
	}
	now := time.Now()
	k := logKey{uid, kind}
	t.devMu.Lock()
	if last, ok := t.lastLog[k]; ok && now.Sub(last) < deviceLogEvery {
		t.devMu.Unlock()
		return
	}
	t.lastLog[k] = now
	for key, at := range t.lastLog {
		if now.Sub(at) > 2*deviceLogEvery {
			delete(t.lastLog, key)
		}
	}
	t.devMu.Unlock()
	counting := config.CountSeparate
	if r.Merge {
		counting = config.CountMerge
	}
	nlog.Core().Info("singbox: device over the limit",
		"user", uid, "ip", ips, "limit", limit, "action", action, "kind", kind,
		"union", kernel.CountDevices(local, elsewhere, false),
		"max", kernel.CountDevices(local, elsewhere, true),
		"counting", counting)
}

// disconnectOverLimit closes the connections of local devices that put a
// user over the device limit. Admission cannot prevent this alone: two nodes
// can each admit a new device before the panel tells them about the other.
// With over_limit_kick_after_minutes above 0 a user is only handled once it
// has been over the limit for that long; with block_devices off the devices
// are only logged.
func (t *ConnTracker) disconnectOverLimit() {
	dlf := t.deviceLimitFunc.Load()
	if dlf == nil {
		return
	}
	r := t.deviceRule()
	if r.CountMethod == config.CountOff {
		return
	}
	now := time.Now()
	type found struct {
		uid     int
		ips     []string
		limit   int
		local   map[string]int
		elsewhr map[string]bool
	}
	var hits []found

	t.usersMu.RLock()
	for uuid, uid := range t.uuidMap {
		limit, hasLimit := (*dlf)(uuid)
		us := t.users[uid]
		if !hasLimit || limit <= 0 || us == nil {
			continue
		}
		us.mu.RLock()
		local := us.activeIPs(now, r)
		elsewhere := t.devicesElsewhere(us, uid, r)
		us.mu.RUnlock()
		var ips []string
		if r.Merge {
			ips = kernel.OverLimitDevicesMerged(local, elsewhere, limit)
		} else {
			ips = kernel.OverLimitDevices(local, elsewhere, limit)
		}
		if len(ips) == 0 {
			continue
		}
		if r.KickAfter > 0 && !t.persistentlyOver(uid, r) {
			continue
		}
		hits = append(hits, found{uid, ips, limit, local, elsewhere})
	}
	over := make(map[int]map[string]bool, len(hits))
	for _, h := range hits {
		over[h.uid] = make(map[string]bool, len(h.ips))
		for _, ip := range h.ips {
			over[h.uid][ip] = true
		}
	}
	var revoked []io.Closer
	if r.Block && len(over) > 0 {
		for _, ref := range t.connMap {
			if over[ref.uid][ref.sourceIP] {
				revoked = append(revoked, ref.conn)
			}
		}
	}
	t.usersMu.RUnlock()

	for _, h := range hits {
		sort.Strings(h.ips)
		action := "none"
		if r.Block {
			action = "disconnected"
		}
		t.logOverLimit(h.uid, "kick", r, h.local, h.elsewhr, h.limit, strings.Join(h.ips, ","), action)
	}

	// Close outside the lock: Close removes the conn from connMap.
	for _, conn := range revoked {
		conn.Close()
	}
	if r.Block && len(hits) > 0 {
		// A kicked user starts a fresh delay if it comes back over the limit.
		t.devMu.Lock()
		for _, h := range hits {
			delete(t.overHist, h.uid)
		}
		t.devMu.Unlock()
	}
}

func (t *ConnTracker) nextID() string {
	return "sb-" + formatInt36(t.idCounter.Add(1))
}

func formatInt36(n int64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return "0"
	}
	var buf [13]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%36]
		n /= 36
	}
	return string(buf[i:])
}

// ─── Public API ─────────────────────────────────────────────────────────────

// GetUserTraffic returns per-user cumulative traffic and alive IPs.
// This is O(users), not O(connections).
func (t *ConnTracker) GetUserTraffic() (traffic map[int][2]int64, aliveIPs map[int]map[string]bool, connCount int) {
	r := t.deviceRule()
	t.usersMu.RLock()
	traffic = make(map[int][2]int64, len(t.users))
	aliveIPs = make(map[int]map[string]bool, len(t.users))
	for uid, us := range t.users {
		up := us.upload.Load()
		down := us.download.Load()
		if up > 0 || down > 0 {
			traffic[uid] = [2]int64{up, down}
		}

		ips := us.aliveIPList(r)
		if ips != nil {
			aliveIPs[uid] = ips
		}

		us.mu.Lock()
		connCount += us.connCount
		us.mu.Unlock()
	}
	t.usersMu.RUnlock()
	t.sampleDevices(time.Now())
	return
}

// CloseByID force-closes a connection by its ID.
func (t *ConnTracker) CloseByID(id string) bool {
	t.usersMu.RLock()
	ref, ok := t.connMap[id]
	t.usersMu.RUnlock()
	if !ok {
		return false
	}
	ref.conn.Close()
	return true
}

// CloseByUUID force-closes ALL connections for a given user UUID.
func (t *ConnTracker) CloseByUUID(uuid string) int {
	// This is a no-op for now — sing-box doesn't expose per-user connection
	// kill easily. The kernel's RemoveUsers removes the inbound user which
	// prevents new connections, and existing connections will fail on next I/O.
	return 0
}

// ActiveCount returns the total number of active connections.
func (t *ConnTracker) ActiveCount() int {
	t.usersMu.RLock()
	total := 0
	for _, us := range t.users {
		us.mu.Lock()
		total += us.connCount
		us.mu.Unlock()
	}
	t.usersMu.RUnlock()
	return total
}

// removeConnRef removes the connection reference from connMap on close.
func (t *ConnTracker) removeConnRef(connID string) {
	t.usersMu.Lock()
	delete(t.connMap, connID)
	t.usersMu.Unlock()
}

// ─── trackedConn (TCP) ──────────────────────────────────────────────────────

// RateLimitedWriter wraps a Writer with non-blocking rate limiting.
// Uses AllowN for instant checks and context-aware waiting when needed.
type RateLimitedWriter struct {
	writer  io.Writer
	limiter *rate.Limiter
	ctx     context.Context
}

// Write implements io.Writer with rate limiting.
func (w *RateLimitedWriter) Write(b []byte) (int, error) {
	// Truncate to burst size
	if burst := w.limiter.Burst(); len(b) > burst {
		b = b[:burst]
	}

	// Fast path: check if tokens available (non-blocking)
	if w.limiter.AllowN(time.Now(), len(b)) {
		return w.writer.Write(b)
	}

	// Slow path: wait for tokens with context cancellation support
	resv := w.limiter.ReserveN(time.Now(), len(b))
	if delay := resv.Delay(); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			// Tokens available, proceed
		case <-w.ctx.Done():
			resv.Cancel()
			return 0, w.ctx.Err()
		}
	}

	return w.writer.Write(b)
}

// RateLimitedReadCloser wraps a Reader with non-blocking rate limiting.
type RateLimitedReadCloser struct {
	reader  io.Reader
	limiter *rate.Limiter
	ctx     context.Context
}

// Read implements io.Reader with rate limiting.
func (r *RateLimitedReadCloser) Read(b []byte) (int, error) {
	// Truncate to burst size
	if burst := r.limiter.Burst(); len(b) > burst {
		b = b[:burst]
	}

	n, err := r.reader.Read(b)
	if n > 0 && r.limiter != nil {
		// Fast path: check if tokens available
		if !r.limiter.AllowN(time.Now(), n) {
			// Slow path: wait with context cancellation
			resv := r.limiter.ReserveN(time.Now(), n)
			if delay := resv.Delay(); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					// Tokens available
				case <-r.ctx.Done():
					resv.Cancel()
					return n, r.ctx.Err()
				}
			}
		}
	}
	return n, err
}

type trackedConn struct {
	net.Conn
	tracker  *ConnTracker
	us       *userStats  // per-user stats (upload/download atomics + IP tracking)
	act      *ipActivity // byte window of sourceIP (nil without us)
	userID   int         // user ID for device tracking
	connID   string
	sourceIP string
	// upLimiter throttles reads (user upload), downLimiter writes (download).
	upLimiter   *rate.Limiter
	downLimiter *rate.Limiter
	ctx         context.Context
	closed      atomic.Bool
}

func (c *trackedConn) Read(b []byte) (int, error) {
	if c.upLimiter != nil {
		if burst := c.upLimiter.Burst(); len(b) > burst {
			b = b[:burst]
		}
	}
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.act.add(int64(n))
		if c.us != nil {
			c.us.upload.Add(int64(n)) // 从入站读取 = 用户上传
		}
		if c.upLimiter != nil {
			// Non-blocking rate limiting
			if !c.upLimiter.AllowN(time.Now(), n) {
				resv := c.upLimiter.ReserveN(time.Now(), n)
				if delay := resv.Delay(); delay > 0 {
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-timer.C:
						// Tokens available
					case <-c.ctx.Done():
						resv.Cancel()
						return n, c.ctx.Err()
					}
				}
			}
		}
	}
	return n, err
}

func (c *trackedConn) Write(b []byte) (int, error) {
	// Apply rate limiting before write
	if c.downLimiter != nil {
		if burst := c.downLimiter.Burst(); len(b) > burst {
			b = b[:burst]
		}
		// Non-blocking check first
		if !c.downLimiter.AllowN(time.Now(), len(b)) {
			resv := c.downLimiter.ReserveN(time.Now(), len(b))
			if delay := resv.Delay(); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					// Tokens available
				case <-c.ctx.Done():
					resv.Cancel()
					return 0, c.ctx.Err()
				}
			}
		}
	}

	n, err := c.Conn.Write(b)
	if n > 0 {
		c.act.addWrite(int64(n))
		if c.us != nil {
			c.us.download.Add(int64(n)) // 向入站写入 = 用户下载
		}
	}
	return n, err
}

func (c *trackedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		if c.us != nil {
			c.us.removeConn(c.sourceIP)
		}
		c.tracker.removeConnRef(c.connID)
	}
	return c.Conn.Close()
}

// makeCountFunc builds a CountFunc for zero-copy byte counting via sing's
// ReadCounter/WriteCounter unwrap interfaces, throttled by lim when set.
func (c *trackedConn) makeCountFunc(counter *atomic.Int64, lim *rate.Limiter) N.CountFunc {
	if lim == nil {
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		// Non-blocking rate limiting with context cancellation
		if !lim.AllowN(time.Now(), int(n)) {
			resv := lim.ReserveN(time.Now(), int(n))
			if delay := resv.Delay(); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					// Tokens available
				case <-c.ctx.Done():
					resv.Cancel()
				}
			}
		}
	}
}

func (c *trackedConn) UnwrapReader() (io.Reader, []N.CountFunc) {
	if c.us == nil {
		return c.Conn, nil
	}
	// Zero-copy relaying bypasses Read, so the bytes are also counted here.
	count := c.makeCountFunc(&c.us.upload, c.upLimiter) // 从入站读取 = 用户上传
	return c.Conn, []N.CountFunc{func(n int64) {
		c.act.add(n)
		count(n)
	}}
}

func (c *trackedConn) UnwrapWriter() (io.Writer, []N.CountFunc) {
	if c.us == nil {
		return c.Conn, nil
	}
	// Zero-copy relaying bypasses Write, so the bytes are also counted here.
	count := c.makeCountFunc(&c.us.download, c.downLimiter) // 向入站写入 = 用户下载
	return c.Conn, []N.CountFunc{func(n int64) {
		c.act.addWrite(n)
		count(n)
	}}
}

func (c *trackedConn) Upstream() any           { return c.Conn }
func (c *trackedConn) ReaderReplaceable() bool { return true }
func (c *trackedConn) WriterReplaceable() bool { return true }

// ─── trackedPacketConn (UDP / QUIC) ─────────────────────────────────────────

type trackedPacketConn struct {
	N.PacketConn
	tracker  *ConnTracker
	us       *userStats
	act      *ipActivity // byte window of sourceIP (nil without us)
	userID   int
	connID   string
	sourceIP string
	// upLimiter throttles reads (user upload), downLimiter writes (download).
	upLimiter   *rate.Limiter
	downLimiter *rate.Limiter
	ctx         context.Context
	closed      atomic.Bool
}

func (c *trackedPacketConn) ReadPacket(buffer *buf.Buffer) (singM.Socksaddr, error) {
	dest, err := c.PacketConn.ReadPacket(buffer)
	if err == nil {
		n := int64(buffer.Len())
		c.act.add(n)
		if c.us != nil {
			c.us.upload.Add(n) // 从入站读取 = 用户上传
		}
		if c.upLimiter != nil {
			// Non-blocking rate limiting with context cancellation
			if !c.upLimiter.AllowN(time.Now(), int(n)) {
				resv := c.upLimiter.ReserveN(time.Now(), int(n))
				if delay := resv.Delay(); delay > 0 {
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-timer.C:
						// Tokens available
					case <-c.ctx.Done():
						resv.Cancel()
						return dest, c.ctx.Err()
					}
				}
			}
		}
	}
	return dest, err
}

func (c *trackedPacketConn) WritePacket(buffer *buf.Buffer, dest singM.Socksaddr) error {
	n := int64(buffer.Len())

	// Apply rate limiting before write
	if c.downLimiter != nil {
		if !c.downLimiter.AllowN(time.Now(), int(n)) {
			resv := c.downLimiter.ReserveN(time.Now(), int(n))
			if delay := resv.Delay(); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					// Tokens available
				case <-c.ctx.Done():
					resv.Cancel()
					return c.ctx.Err()
				}
			}
		}
	}

	err := c.PacketConn.WritePacket(buffer, dest)
	if err == nil {
		c.act.addWrite(n)
		if c.us != nil {
			c.us.download.Add(n) // 向入站写入 = 用户下载
		}
	}
	return err
}

func (c *trackedPacketConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		if c.us != nil {
			c.us.removeConn(c.sourceIP)
		}
		c.tracker.removeConnRef(c.connID)
	}
	return c.PacketConn.Close()
}

func (c *trackedPacketConn) makeCountFunc(counter *atomic.Int64, lim *rate.Limiter) N.CountFunc {
	if lim == nil {
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		// Non-blocking rate limiting with context cancellation
		if !lim.AllowN(time.Now(), int(n)) {
			resv := lim.ReserveN(time.Now(), int(n))
			if delay := resv.Delay(); delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					// Tokens available
				case <-c.ctx.Done():
					resv.Cancel()
				}
			}
		}
	}
}

func (c *trackedPacketConn) UnwrapPacketReader() (N.PacketReader, []N.CountFunc) {
	if c.us == nil {
		return c.PacketConn, nil
	}
	// Zero-copy relaying bypasses ReadPacket, so the bytes are also counted
	// here.
	count := c.makeCountFunc(&c.us.download, c.upLimiter)
	return c.PacketConn, []N.CountFunc{func(n int64) {
		c.act.add(n)
		count(n)
	}}
}

func (c *trackedPacketConn) UnwrapPacketWriter() (N.PacketWriter, []N.CountFunc) {
	if c.us == nil {
		return c.PacketConn, nil
	}
	count := c.makeCountFunc(&c.us.upload, c.downLimiter)
	return c.PacketConn, []N.CountFunc{func(n int64) {
		c.act.addWrite(n)
		count(n)
	}}
}

func (c *trackedPacketConn) Upstream() any           { return c.PacketConn }
func (c *trackedPacketConn) ReaderReplaceable() bool { return true }
func (c *trackedPacketConn) WriterReplaceable() bool { return true }
