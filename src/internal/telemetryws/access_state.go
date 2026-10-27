package telemetryws

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Common lifecycle errors.
var (
	// ErrSessionClosed indicates operations on an already closed WebSocket session.
	ErrSessionClosed = errors.New("telemetryws: session closed")

	// ErrSessionExpired indicates operations on a session whose verified JWT exp has passed.
	ErrSessionExpired = errors.New("telemetryws: session expired")

	// ErrStaleAuthorization indicates an in-flight authorization result is stale because the session
	// generation changed (e.g. via unsubscribe, concurrent subscribe, or close) while the DB check was running.
	ErrStaleAuthorization = errors.New("telemetryws: stale authorization discarded due to generation change")

	// ErrTargetLimitExceeded indicates an attempt to exceed the per-session subscription limit (max 16).
	ErrTargetLimitExceeded = errors.New("telemetryws: target limit per session exceeded")

	// ErrSessionLimitExceeded indicates the registry has reached the maximum allowed concurrent sessions (max 32).
	ErrSessionLimitExceeded = errors.New("telemetryws: maximum session limit exceeded")
)

// RevocationReason classifies why a session's access or subscription was terminated.
type RevocationReason string

const (
	// RevocationReasonDenied indicates membership was revoked, entity was deleted, or unauthorized role.
	RevocationReasonDenied RevocationReason = "MEMBERSHIP_DENIED"

	// RevocationReasonDBError indicates a database query error or transient authorizer failure.
	RevocationReasonDBError RevocationReason = "DATABASE_ERROR"

	// RevocationReasonSweepTimeout indicates the session could not be verified within the published sweep budget.
	RevocationReasonSweepTimeout RevocationReason = "SWEEP_TIMEOUT"

	// RevocationReasonExpired indicates the session reached the verified JWT exp instant (Principal.ExpiresAt).
	RevocationReasonExpired RevocationReason = "TOKEN_EXPIRED"

	// RevocationReasonClientClose indicates clean client disconnect or voluntary session termination.
	RevocationReasonClientClose RevocationReason = "CLIENT_CLOSE"

	// RevocationReasonStaleGeneration indicates an in-flight subscription was invalidated by generation bump.
	RevocationReasonStaleGeneration RevocationReason = "STALE_GENERATION"

	// RevocationReasonSlowConsumer indicates the session's queue overflowed slot or byte bounds.
	RevocationReasonSlowConsumer RevocationReason = "SLOW_CONSUMER"
)

// SessionRevocationHandler is the minimal typed contract requested by the access lifecycle.
// On revocation, denial, DB error, or budget timeout, the session access state requests its owner
// (to be implemented by the P7.4 hub/connection writer) to discard queued unsent data and close the transport socket.
type SessionRevocationHandler interface {
	OnRevocation(sessionID string, reason RevocationReason)
}

// SessionRevocationFunc adapts a function to SessionRevocationHandler.
type SessionRevocationFunc func(sessionID string, reason RevocationReason)

// OnRevocation invokes the underlying function.
func (f SessionRevocationFunc) OnRevocation(sessionID string, reason RevocationReason) {
	if f != nil {
		f(sessionID, reason)
	}
}

// SessionSnapshot captures an immutable point-in-time view of a session's access state.
type SessionSnapshot struct {
	SessionID  string
	UserID     uuid.UUID
	ExpiresAt  time.Time
	Targets    []TargetKey
	Generation uint64
	Closed     bool
}

// SessionAccessState manages bounded per-session subscription targets, generation safety,
// absolute JWT token expiry, and revocation notification.
//
// CONCURRENCY & LIFECYCLE RULES:
//  1. Database queries (TargetAuthorizer) MUST run outside the session mutex.
//  2. Callers capture s.Generation() via PrepareSubscribe prior to invoking TargetAuthorizer.
//  3. CommitSubscribe validates that generation has not changed; any intermediate Unsubscribe or
//     Close bumps generation, causing CommitSubscribe to discard stale results with ErrStaleAuthorization.
//     This guarantees that removed filters are never resurrected after a blocked DB query completes.
//  4. Close is idempotent, bumps generation, clears all targets, and fires the revocation handler
//     at most once outside locks.
//  5. Subscriptions are strictly bounded to MaxSubscriptionsPerSession (16).
type SessionAccessState struct {
	sessionID      string
	userID         uuid.UUID
	expiresAt      time.Time
	handler        SessionRevocationHandler
	mu             sync.Mutex
	targets        map[TargetKey]struct{}
	generation     uint64
	closed         bool
	revoked        bool
	lastVerifiedAt time.Time
}

// NewSessionAccessState initializes a new SessionAccessState.
func NewSessionAccessState(sessionID string, userID uuid.UUID, expiresAt time.Time, handler SessionRevocationHandler) (*SessionAccessState, error) {
	if sessionID == "" {
		return nil, errors.New("telemetryws: empty session id")
	}
	if userID == uuid.Nil {
		return nil, ErrInvalidUserID
	}
	if expiresAt.IsZero() {
		return nil, errors.New("telemetryws: zero expiry time")
	}

	return &SessionAccessState{
		sessionID:  sessionID,
		userID:     userID,
		expiresAt:  expiresAt,
		handler:    handler,
		targets:    make(map[TargetKey]struct{}),
		generation: 1,
		closed:     false,
	}, nil
}

// SessionID returns the unique session identifier.
func (s *SessionAccessState) SessionID() string {
	return s.sessionID
}

// UserID returns the verified human user UUID.
func (s *SessionAccessState) UserID() uuid.UUID {
	return s.userID
}

// ExpiresAt returns the absolute token expiration timestamp in UTC.
func (s *SessionAccessState) ExpiresAt() time.Time {
	return s.expiresAt
}

// Generation returns the current generation counter.
func (s *SessionAccessState) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

// IsClosed reports whether the session has been closed.
func (s *SessionAccessState) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// IsExpired checks if current time is at or after the absolute expiration time without clock skew.
func (s *SessionAccessState) IsExpired(now time.Time) bool {
	if s.expiresAt.IsZero() {
		return false
	}
	return !now.Before(s.expiresAt)
}

// TargetCount returns the number of active subscription targets.
func (s *SessionAccessState) TargetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.targets)
}

// Targets returns a snapshot copy of the currently active subscription targets.
func (s *SessionAccessState) Targets() []TargetKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TargetKey, 0, len(s.targets))
	for t := range s.targets {
		out = append(out, t)
	}
	return out
}

// HasTarget reports whether the session is currently subscribed to target.
func (s *SessionAccessState) HasTarget(target TargetKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.targets[target]
	return ok
}

// Snapshot returns a point-in-time immutable copy of the session access state.
func (s *SessionAccessState) Snapshot() SessionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	targets := make([]TargetKey, 0, len(s.targets))
	for t := range s.targets {
		targets = append(targets, t)
	}

	return SessionSnapshot{
		SessionID:  s.sessionID,
		UserID:     s.userID,
		ExpiresAt:  s.expiresAt,
		Targets:    targets,
		Generation: s.generation,
		Closed:     s.closed,
	}
}

// PrepareSubscribe pre-checks session state, validates target bounds, and returns the current
// generation counter to be held across the asynchronous DB authorization query.
// Must be called prior to invoking TargetAuthorizer outside the lock.
func (s *SessionAccessState) PrepareSubscribe(newTargets []TargetKey, now time.Time) (uint64, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrSessionClosed
	}

	if s.IsExpired(now) {
		notify := !s.revoked
		s.revoked = true
		s.closed = true
		s.generation++
		s.targets = make(map[TargetKey]struct{})
		handler := s.handler
		sid := s.sessionID
		s.mu.Unlock()

		if notify && handler != nil {
			handler.OnRevocation(sid, RevocationReasonExpired)
		}
		return 0, ErrSessionExpired
	}

	// Calculate target count capacity
	deduped := DeduplicateTargets(newTargets)
	newCount := 0
	for _, t := range deduped {
		if _, exists := s.targets[t]; !exists {
			newCount++
		}
	}

	if len(s.targets)+newCount > MaxSubscriptionsPerSession {
		s.mu.Unlock()
		return 0, ErrTargetLimitExceeded
	}

	gen := s.generation
	s.mu.Unlock()
	return gen, nil
}

// CommitSubscribe installs authorized targets after TargetAuthorizer completes outside locks.
// It verifies that session is open, not expired, and generation matches expectedGen.
// Any intervening unsubscribe or close operation bumps generation, causing stale checks to be rejected.
func (s *SessionAccessState) CommitSubscribe(expectedGen uint64, authorized []AuthorizedTarget, now time.Time) ([]TargetKey, error) {
	installed, notify, err := s.commitSubscribeDeferred(expectedGen, authorized, now)
	if notify != nil {
		notify()
	}
	return installed, err
}

// commitSubscribeDeferred is used by Hub while its index lock protects the
// state/index installation. Expiry state changes remain atomic, but the
// revocation callback is returned for invocation after the caller unlocks.
func (s *SessionAccessState) commitSubscribeDeferred(expectedGen uint64, authorized []AuthorizedTarget, now time.Time) ([]TargetKey, func(), error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, ErrSessionClosed
	}

	if s.IsExpired(now) {
		notify := !s.revoked
		s.revoked = true
		s.closed = true
		s.generation++
		s.targets = make(map[TargetKey]struct{})
		handler := s.handler
		sid := s.sessionID
		s.mu.Unlock()

		var callback func()
		if notify && handler != nil {
			callback = func() { handler.OnRevocation(sid, RevocationReasonExpired) }
		}
		return nil, callback, ErrSessionExpired
	}

	if s.generation != expectedGen {
		s.mu.Unlock()
		return nil, nil, ErrStaleAuthorization
	}

	installed := make([]TargetKey, 0, len(authorized))
	for _, auth := range authorized {
		if _, exists := s.targets[auth.Target]; !exists {
			if len(s.targets) >= MaxSubscriptionsPerSession {
				s.mu.Unlock()
				return nil, nil, ErrTargetLimitExceeded
			}
			s.targets[auth.Target] = struct{}{}
			installed = append(installed, auth.Target)
		}
	}

	// Generation increment invalidates other pending in-flight checks using expectedGen
	s.generation++
	s.mu.Unlock()
	return installed, nil, nil
}

// Unsubscribe removes the specified targets from active subscriptions.
// Crucially, it bumps the generation counter, invalidating any in-flight DB checks
// that could otherwise resurrect removed filters.
func (s *SessionAccessState) Unsubscribe(targets []TargetKey) ([]TargetKey, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}

	removed := make([]TargetKey, 0, len(targets))
	for _, t := range targets {
		if _, exists := s.targets[t]; exists {
			delete(s.targets, t)
			removed = append(removed, t)
		}
	}

	// Invalidate any in-flight authorization checks
	s.generation++
	s.mu.Unlock()
	return removed, nil
}

// Close terminates the session access state, invalidates its generation, clears all active targets,
// and invokes the registered SessionRevocationHandler exactly once outside locks.
func (s *SessionAccessState) Close(reason RevocationReason) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	s.closed = true
	s.generation++
	s.targets = make(map[TargetKey]struct{})
	notify := !s.revoked
	s.revoked = true
	handler := s.handler
	sid := s.sessionID
	s.mu.Unlock()

	if notify && handler != nil {
		handler.OnRevocation(sid, reason)
	}
}

// SessionProvider supplies active session access states for periodic recheck sweeps.
type SessionProvider interface {
	GetActiveSessions(limit int) []*SessionAccessState
}

// SessionRegistry provides a bounded container of active SessionAccessState instances (capped at MaxSessions = 32).
// It implements SessionProvider and is intended for use or embedding by the P7.4 hub.
type SessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*SessionAccessState
	max      int
}

// NewSessionRegistry creates a new SessionRegistry with the given maximum session capacity.
// If max <= 0, MaxSessions (32) is used.
func NewSessionRegistry(max int) *SessionRegistry {
	if max <= 0 {
		max = MaxSessions
	}
	return &SessionRegistry{
		sessions: make(map[string]*SessionAccessState),
		max:      max,
	}
}

// Register adds a session to the registry. Returns ErrSessionLimitExceeded if capacity is reached.
func (r *SessionRegistry) Register(s *SessionAccessState) error {
	if s == nil {
		return errors.New("telemetryws: nil session state")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) >= r.max {
		return ErrSessionLimitExceeded
	}
	r.sessions[s.SessionID()] = s
	return nil
}

// Unregister removes a session from the registry by ID.
func (r *SessionRegistry) Unregister(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, sessionID)
}

// Get returns the session access state for the given session ID, or nil if not present.
func (r *SessionRegistry) Get(sessionID string) *SessionAccessState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[sessionID]
}

// GetActiveSessions returns a bounded slice of up to limit active sessions.
func (r *SessionRegistry) GetActiveSessions(limit int) []*SessionAccessState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > r.max {
		limit = r.max
	}
	result := make([]*SessionAccessState, 0, min(limit, len(r.sessions)))
	for _, s := range r.sessions {
		if len(result) >= limit {
			break
		}
		result = append(result, s)
	}
	return result
}

// Count returns the current number of registered sessions.
func (r *SessionRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}
