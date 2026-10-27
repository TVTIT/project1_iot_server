package telemetryws

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Common hub errors.
var (
	// ErrHubClosed indicates operations on an already closed Hub.
	ErrHubClosed = errors.New("telemetryws: hub closed")

	// ErrSessionNotFound indicates the requested session is not registered in the Hub.
	ErrSessionNotFound = errors.New("telemetryws: session not found in hub")
)

// HubConfig configures the limits and bounds for Hub fan-out and session management.
type HubConfig struct {
	// MaxSessions is the upper bound on concurrent sessions registered with the hub (default 32).
	MaxSessions int

	// MaxQueueSlots is the per-session maximum pending event slot capacity (default 64).
	MaxQueueSlots int

	// MaxQueueBytes is the per-session maximum logical pending and in-flight bytes (default 256 KiB).
	MaxQueueBytes int

	// MaxFrameBytes is the upper bound on a single telemetry frame (default DefaultMaxEventBytes = 66,560).
	MaxFrameBytes int
}

// DefaultHubConfig returns the standard production limits for the P7.4 WebSocket Hub.
func DefaultHubConfig() HubConfig {
	return HubConfig{
		MaxSessions:   MaxSessions,
		MaxQueueSlots: DefaultSessionQueueSlots,
		MaxQueueBytes: DefaultSessionQueueBytes,
		MaxFrameBytes: DefaultMaxEventBytes,
	}
}

// Hub manages WebSocket session registration, indexed Gateway/Sensor subscription routing,
// bounded per-session event queues with byte ownership, and revocation invalidation.
//
// CONCURRENCY & ARCHITECTURE:
//  1. Reuses existing SessionRegistry and SessionAccessState as the single source of truth for
//     authorization and token lifecycle. Contradictory authorization caches are strictly avoided.
//  2. Index keys include gateway_id and sensor_id. Incoming events evaluate against exactly two
//     keys: TargetKey{GatewayID, SensorID} and TargetKey{GatewayID, ""}. Lookups are O(1)
//     without scanning all registered viewers.
//  3. Overlapping Gateway and Sensor filters select each session AT MOST ONCE per event.
//  4. TargetAuthorizer queries MUST run outside all hub and state locks.
//  5. Installation via CommitSubscribe atomically verifies the generation counter and synchronizes
//     state and index under the hub lock. Intermediate unsubscribe or close bumps generation,
//     preventing stale authorization results from resurrecting removed filters.
//  6. FanOut snapshots interested session queues under a short read-lock, releasing the lock BEFORE
//     enqueueing frames. Enqueue never blocks and never makes network calls.
//  7. A slow consumer whose queue overflows (slot or byte cap) is isolated immediately:
//     the hub closes that slow session and requests REST catch-up, never blocking other viewers.
//  8. Implements SessionRevocationHandler and SessionProvider to integrate cleanly with
//     PeriodicRecheckWorker and the access state lifecycle.
type Hub struct {
	mu                  sync.RWMutex
	cfg                 HubConfig
	registry            *SessionRegistry
	queues              map[string]*SessionQueue
	targetIndex         map[TargetKey]map[string]struct{}
	sessionTargets      map[string]map[TargetKey]struct{}
	closed              bool
	afterFanOutSnapshot func() // test synchronization seam; nil in production
	metrics             *Metrics
}

// SetMetrics attaches the process-owned, optional observer once during composition.
func (h *Hub) SetMetrics(m *Metrics) {
	if h != nil {
		h.metrics = m
	}
}
func (h *Hub) observer() *Metrics {
	if h == nil {
		return nil
	}
	return h.metrics
}

// SubscriptionCount returns the bounded number of installed session-target pairs.
func (h *Hub) SubscriptionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, targets := range h.sessionTargets {
		total += len(targets)
	}
	return total
}

// NewHub constructs a new Hub initialized with the specified HubConfig.
func NewHub(cfg HubConfig) *Hub {
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = MaxSessions
	}
	if cfg.MaxQueueSlots <= 0 {
		cfg.MaxQueueSlots = DefaultSessionQueueSlots
	}
	if cfg.MaxQueueBytes <= 0 {
		cfg.MaxQueueBytes = DefaultSessionQueueBytes
	}
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = DefaultMaxEventBytes
	}

	return &Hub{
		cfg:            cfg,
		registry:       NewSessionRegistry(cfg.MaxSessions),
		queues:         make(map[string]*SessionQueue),
		targetIndex:    make(map[TargetKey]map[string]struct{}),
		sessionTargets: make(map[string]map[TargetKey]struct{}),
	}
}

// Registry returns the underlying SessionRegistry.
func (h *Hub) Registry() *SessionRegistry {
	return h.registry
}

// GetActiveSessions implements SessionProvider for PeriodicRecheckWorker.
func (h *Hub) GetActiveSessions(limit int) []*SessionAccessState {
	return h.registry.GetActiveSessions(limit)
}

// RegisterSession registers a newly authenticated session with the hub, allocating its bounded queue.
// Returns ErrSessionLimitExceeded if the hub has reached its maximum session capacity (default 32).
func (h *Hub) RegisterSession(state *SessionAccessState) (*SessionQueue, error) {
	if state == nil {
		return nil, errors.New("telemetryws: nil session state")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, ErrHubClosed
	}

	if state.IsClosed() {
		return nil, ErrSessionClosed
	}

	sid := state.SessionID()
	if _, exists := h.queues[sid]; exists {
		return nil, fmt.Errorf("telemetryws: session %q already registered", sid)
	}

	if len(h.queues) >= h.cfg.MaxSessions {
		return nil, ErrSessionLimitExceeded
	}

	if err := h.registry.Register(state); err != nil {
		return nil, err
	}

	q := NewSessionQueue(sid, h.cfg.MaxQueueSlots, h.cfg.MaxQueueBytes, h.cfg.MaxFrameBytes)
	h.queues[sid] = q
	h.sessionTargets[sid] = make(map[TargetKey]struct{})

	return q, nil
}

// UnregisterSession removes a session from the hub cleanly with RevocationReasonClientClose.
func (h *Hub) UnregisterSession(sessionID string) {
	h.CloseSession(sessionID, RevocationReasonClientClose)
}

// GetQueue returns the session queue for the given sessionID, if registered and active.
func (h *Hub) GetQueue(sessionID string) (*SessionQueue, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	q, ok := h.queues[sessionID]
	return q, ok
}

// CommitSubscribe installs authorized targets after TargetAuthorizer finishes outside locks.
// It atomically generation-checks the session access state and installs the targets into
// both the session state and the hub index.
// If an intermediate unsubscribe or close occurred, CommitSubscribe returns ErrStaleAuthorization,
// ensuring removed filters are never resurrected.
func (h *Hub) CommitSubscribe(sessionID string, expectedGen uint64, authorized []AuthorizedTarget, now time.Time) ([]TargetKey, error) {
	h.mu.Lock()

	if h.closed {
		h.mu.Unlock()
		return nil, ErrHubClosed
	}

	state := h.registry.Get(sessionID)
	if state == nil {
		h.mu.Unlock()
		return nil, ErrSessionNotFound
	}

	installed, notify, err := state.commitSubscribeDeferred(expectedGen, authorized, now)
	if err != nil {
		if errors.Is(err, ErrSessionExpired) {
			// State has already failed closed. Remove indexed delivery eligibility
			// before releasing h.mu; the deferred callback remains safe/idempotent.
			h.cleanupSessionInternal(sessionID)
		}
		h.mu.Unlock()
		if notify != nil {
			notify()
		}
		return nil, err
	}

	targetsMap := h.sessionTargets[sessionID]
	if targetsMap == nil {
		targetsMap = make(map[TargetKey]struct{})
		h.sessionTargets[sessionID] = targetsMap
	}

	for _, t := range installed {
		targetsMap[t] = struct{}{}
		sessionsForTarget, exists := h.targetIndex[t]
		if !exists {
			sessionsForTarget = make(map[string]struct{})
			h.targetIndex[t] = sessionsForTarget
		}
		sessionsForTarget[sessionID] = struct{}{}
	}

	h.mu.Unlock()
	return installed, nil
}

// Unsubscribe removes the specified subscription targets for sessionID,
// immediately updating the hub index and purging any unsent queued frames matching the removed targets.
func (h *Hub) Unsubscribe(sessionID string, targets []TargetKey) ([]TargetKey, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, ErrHubClosed
	}

	state := h.registry.Get(sessionID)
	if state == nil {
		return nil, ErrSessionNotFound
	}

	removed, err := state.Unsubscribe(targets)
	if err != nil {
		return nil, err
	}

	targetsMap := h.sessionTargets[sessionID]
	for _, t := range removed {
		if targetsMap != nil {
			delete(targetsMap, t)
		}
		if sessionsForTarget, exists := h.targetIndex[t]; exists {
			delete(sessionsForTarget, sessionID)
			if len(sessionsForTarget) == 0 {
				delete(h.targetIndex, t)
			}
		}
	}

	// Purge only frames no longer authorized by any target still active for this session.
	if len(removed) > 0 {
		if q, ok := h.queues[sessionID]; ok {
			remaining := state.Targets()
			q.PurgeMatching(func(evt *SharedEvent) bool {
				return !MatchesAnyTarget(remaining, evt.GatewayID(), evt.SensorID())
			})
		}
	}

	return removed, nil
}

// CloseSession terminates a session within the hub:
//  1. Cleans up all index entries and reverse index entries under h.mu.
//  2. Closes the session queue, clearing unsent frames and releasing queued byte counters.
//  3. Unregisters the session from the registry.
//  4. Invokes state.Close(reason) outside h.mu to trigger revocation notifications without lock cycles.
func (h *Hub) CloseSession(sessionID string, reason RevocationReason) {
	h.mu.Lock()
	state := h.registry.Get(sessionID)
	h.cleanupSessionInternal(sessionID)
	h.mu.Unlock()

	if state != nil {
		state.Close(reason)
	}
}

// CloseAllSessions fails closed after a global event gap. It is intended for
// background runtime workers only; callers must not invoke it from submission.
func (h *Hub) CloseAllSessions(reason RevocationReason) {
	h.mu.Lock()
	ids := make([]string, 0, len(h.queues))
	for id := range h.queues {
		ids = append(ids, id)
	}
	states := make([]*SessionAccessState, 0, len(ids))
	for _, id := range ids {
		if s := h.registry.Get(id); s != nil {
			states = append(states, s)
		}
		h.cleanupSessionInternal(id)
	}
	h.mu.Unlock()
	for _, s := range states {
		s.Close(reason)
	}
}

// OnRevocation implements SessionRevocationHandler.
// It is invoked by SessionAccessState or PeriodicRecheckWorker outside state locks
// when a session's token expires, membership is revoked, or sweep times out.
// It immediately cleans up hub indexes and clears unsent queues.
func (h *Hub) OnRevocation(sessionID string, reason RevocationReason) {
	if h.metrics != nil {
		switch reason {
		case RevocationReasonExpired:
			h.metrics.Expired()
		case RevocationReasonDenied, RevocationReasonDBError, RevocationReasonSweepTimeout:
			h.metrics.Revoked()
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupSessionInternal(sessionID)
}

// cleanupSessionInternal removes the session from index, queues, and registry.
// MUST be called with h.mu write lock held.
func (h *Hub) cleanupSessionInternal(sessionID string) {
	// Remove from target index
	if targetsMap, ok := h.sessionTargets[sessionID]; ok {
		for t := range targetsMap {
			if sessionsForTarget, exists := h.targetIndex[t]; exists {
				delete(sessionsForTarget, sessionID)
				if len(sessionsForTarget) == 0 {
					delete(h.targetIndex, t)
				}
			}
		}
		delete(h.sessionTargets, sessionID)
	}

	// Close and remove queue
	if q, ok := h.queues[sessionID]; ok {
		_ = q.Close()
		delete(h.queues, sessionID)
	}

	// Unregister from registry
	h.registry.Unregister(sessionID)
}

// FanOut routes one immutable SharedEvent to all interested sessions via index.
// It does NOT scan all registered viewers.
// Overlapping Gateway and Sensor filters select each session AT MOST ONCE.
// If a viewer's queue is full or oversize, that slow session is closed with
// RevocationReasonSlowConsumer and isolated; the publisher and other viewers never block.
// Returns the number of sessions to which the event was successfully queued.
func (h *Hub) FanOut(evt *SharedEvent) (int, error) {
	if evt == nil {
		return 0, errors.New("telemetryws: nil event for fan-out")
	}

	// Step 1: Snapshot interested session queues under read-lock.
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return 0, ErrHubClosed
	}

	// Check exactly two keys: specific sensor filter and gateway-level filter.
	sensorKey := evt.TargetKey()
	gatewayKey := evt.GatewayTargetKey()

	var interestedQueues []*SessionQueue
	seen := make(map[string]struct{})

	for _, key := range [...]TargetKey{sensorKey, gatewayKey} {
		if sessions, exists := h.targetIndex[key]; exists {
			for sid := range sessions {
				if _, alreadySeen := seen[sid]; !alreadySeen {
					seen[sid] = struct{}{}
					if q, ok := h.queues[sid]; ok && !q.IsClosed() {
						interestedQueues = append(interestedQueues, q)
					}
				}
			}
		}
	}
	h.mu.RUnlock()
	if h.afterFanOutSnapshot != nil {
		h.afterFanOutSnapshot()
	}

	if len(interestedQueues) == 0 {
		return 0, nil
	}

	// Step 2: Revalidate that at least one matching target remains installed and
	// enqueue while holding the hub lock. This makes unsubscribe/purge and enqueue
	// linearizable; Enqueue is bounded and never performs network I/O.
	deliveredCount := 0
	var slowSessions []string

	for _, q := range interestedQueues {
		h.mu.Lock()
		if h.closed || h.queues[q.SessionID()] != q || !h.sessionMatchesEventLocked(q.SessionID(), sensorKey, gatewayKey) {
			h.mu.Unlock()
			continue
		}
		err := q.Enqueue(evt)
		h.mu.Unlock()
		if err == nil {
			deliveredCount++
			if h.metrics != nil {
				h.metrics.DeliveryEnqueued(1)
			}
			continue
		}

		if errors.Is(err, ErrQueueFull) || errors.Is(err, ErrQueueClosed) {
			if h.metrics != nil {
				h.metrics.Drop()
			}
			if errors.Is(err, ErrQueueFull) && h.metrics != nil {
				h.metrics.SlowClose()
			}
			slowSessions = append(slowSessions, q.SessionID())
		} else if h.metrics != nil {
			h.metrics.Drop()
		}
	}

	// Step 3: Isolate and close slow sessions outside publisher critical path.
	for _, sid := range slowSessions {
		h.CloseSession(sid, RevocationReasonSlowConsumer)
	}

	return deliveredCount, nil
}

func (h *Hub) sessionMatchesEventLocked(sessionID string, sensorKey, gatewayKey TargetKey) bool {
	if _, ok := h.targetIndex[sensorKey][sessionID]; ok {
		return true
	}
	_, ok := h.targetIndex[gatewayKey][sessionID]
	return ok
}

// Close terminates the hub, closing all registered session queues and states.
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true

	// Gather sessions to close
	sessionIDs := make([]string, 0, len(h.queues))
	for sid := range h.queues {
		sessionIDs = append(sessionIDs, sid)
	}

	states := make([]*SessionAccessState, 0, len(sessionIDs))
	for _, sid := range sessionIDs {
		if s := h.registry.Get(sid); s != nil {
			states = append(states, s)
		}
		h.cleanupSessionInternal(sid)
	}
	h.mu.Unlock()

	for _, s := range states {
		s.Close(RevocationReasonShutdown)
	}

	return nil
}

// SessionCount returns the number of active sessions in the hub.
func (h *Hub) SessionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.queues)
}

// IndexEntryCount returns the total number of distinct target keys currently indexed.
func (h *Hub) IndexEntryCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.targetIndex)
}

// SubscribedSessionsCount returns the number of unique sessions subscribed to the target key.
func (h *Hub) SubscribedSessionsCount(target TargetKey) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if sessions, exists := h.targetIndex[target]; exists {
		return len(sessions)
	}
	return 0
}

// TotalQueuedBytes returns the aggregate logical queued bytes across all active session queues.
func (h *Hub) TotalQueuedBytes() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, q := range h.queues {
		total += q.QueuedBytes()
	}
	return total
}

// TotalInFlightBytes returns the aggregate logical in-flight bytes across all active session queues.
func (h *Hub) TotalInFlightBytes() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, q := range h.queues {
		total += q.InFlightBytes()
	}
	return total
}

// TotalControlBytes returns bytes reserved by queued or in-flight control frames.
func (h *Hub) TotalControlBytes() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, q := range h.queues {
		total += q.ControlBytes()
	}
	return total
}
