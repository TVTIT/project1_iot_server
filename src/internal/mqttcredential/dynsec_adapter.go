package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// DynSecController is a trusted local capability, not a public API. A single
// adapter must own the controller in the backend composition root.
type DynSecController interface {
	CloseDrain(context.Context) error
	Describe(context.Context) (LifecycleDescription, error)
	RestartClosed(context.Context) (LifecycleDescription, error)
	OpenVerified(context.Context, VerificationReceipt) error
}

// DynSecAdapterConfig configures dynamic security adapter operations. All callbacks are server-owned, bounded and must use the private TLS tunnel.
// NewClient MUST allocate a new lifetime, never return a cached manager client.
// Login MUST perform a fresh authenticated TLS CONNECT (not network liveness).
type DynSecAdapterConfig struct {
	Controller               DynSecController
	NewClient                func(context.Context) (*DynSecClient, error)
	Observe                  func(context.Context, string) (DynSecClientMetadata, error)
	Login                    func(context.Context, string, string) error
	ProtectedUsernames       []string
	Timeout, RecoveryTimeout time.Duration
}

// DynSecAdapter orchestrates broker dynamic security updates and verification.
type DynSecAdapter struct {
	cfg     DynSecAdapterConfig
	mu      sync.Mutex
	busy    bool
	pending DynSecAdapterResult
	opened  DynSecAdapterResult
}

// DynSecAdapterResult captures adapter execution outcome and verification evidence. Runtime evidence is not DB finalization or power-loss durability. No secret
// is stored or returned. The service owns its transient generation callback.
type DynSecAdapterResult struct {
	OperationID uuid.UUID
	Outcome     ExecutionOutcome
	Evidence    VerificationEvidence
	receipt     VerificationReceipt
}

// NewDynSecAdapter validates configuration and constructs a DynSecAdapter.
func NewDynSecAdapter(c DynSecAdapterConfig) (*DynSecAdapter, error) {
	if dynSecNil(c.Controller) || c.NewClient == nil || c.Observe == nil || c.Login == nil || len(c.ProtectedUsernames) < 2 || c.Timeout <= 0 || c.Timeout > time.Minute || c.RecoveryTimeout <= 0 || c.RecoveryTimeout > time.Minute {
		return nil, ErrInvalidInput
	}
	seen := make(map[string]bool)
	for _, u := range c.ProtectedUsernames {
		if u == "" || seen[u] {
			return nil, ErrInvalidInput
		}
		seen[u] = true
	}
	c.ProtectedUsernames = append([]string(nil), c.ProtectedUsernames...)
	return &DynSecAdapter{cfg: c}, nil
}

func (a *DynSecAdapter) target(o Operation) bool {
	if o.Validate() != nil || o.Status != OperationPending || o.Phase != PhaseIntent || o.Evidence != (VerificationEvidence{}) || o.GatewayID == BackendUsername {
		return false
	}
	for _, u := range a.cfg.ProtectedUsernames {
		if u == o.GatewayID {
			return false
		}
	}
	if o.Action == ActionProvision {
		return o.Previous == nil || o.Previous.Status == CredentialRevoked || o.Previous.Status == CredentialFailed
	}
	return o.Previous != nil && o.Previous.Status == CredentialActive && ((o.Action == ActionRotate && o.CredentialVersion > o.Previous.CredentialVersion) || (o.Action == ActionRevoke && o.CredentialVersion == o.Previous.CredentialVersion))
}

// Execute accepts only a service-confirmed committed intent. It cannot prove a
// commit from an Operation value: service/repository admission is mandatory.
// Global admission is nonblocking (no secret queue) and remains held after a
// verified result until OPEN acknowledgment or explicit maintenance release.
func (a *DynSecAdapter) Execute(ctx context.Context, o Operation, generate func(context.Context) (string, error)) (result DynSecAdapterResult, err error) {
	result = DynSecAdapterResult{OperationID: o.OperationID, Outcome: ExecutionUnchangedFailure}
	if !a.target(o) {
		return result, ErrInvalidInput
	}
	a.mu.Lock()
	if a.busy {
		a.mu.Unlock()
		return result, ErrRuntimeBusy
	}
	a.busy = true
	a.opened = DynSecAdapterResult{}
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.pending = result; a.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	result.Outcome = ExecutionMaintenanceClosed
	if a.cfg.Controller.CloseDrain(ctx) != nil {
		return result, ErrLifecycleUnavailable
	}
	before, e := a.cfg.Controller.Describe(ctx)
	if e != nil || before.Open || !before.Alive || before.Epoch == "" || before.Nonce == "" {
		return result, ErrLifecycleUnavailable
	}
	// Every failure below keeps maintenance CLOSED; recovery uses a detached,
	// separately bounded budget even if the originating HTTP request cancelled.
	mutated := false
	defer func() {
		if err == nil {
			return
		}
		recovery, stop := context.WithTimeout(context.Background(), a.cfg.RecoveryTimeout)
		defer stop()
		_ = a.cfg.Controller.CloseDrain(recovery)
		if mutated {
			result.Outcome = ExecutionRecoveryRequired
			c, e := a.cfg.NewClient(recovery)
			if e == nil && c != nil {
				_ = c.DisableClient(recovery, o.GatewayID)
				c.Close()
			}
		}
	}()
	c, e := a.cfg.NewClient(ctx)
	if e != nil || c == nil {
		return result, ErrVerificationFailed
	}
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	role := "gateway_" + o.GatewayID
	native, e := c.GetClient(ctx, o.GatewayID)
	absent := errors.Is(e, errDynSecClientAbsent)
	if e != nil && !absent {
		return result, ErrVerificationFailed
	}
	if o.Action == ActionProvision {
		if o.Previous == nil && !absent {
			result.Outcome = ExecutionRecoveryRequired
			return result, ErrRecoveryRequired
		}
		if !provisionNativeAdmissible(o.Previous, absent, native, o.GatewayID) {
			result.Outcome = ExecutionRecoveryRequired
			return result, ErrRecoveryRequired
		}
	} else if absent || !adapterClientMatches(native, o.GatewayID, role, false) {
		result.Outcome = ExecutionRecoveryRequired
		return result, ErrRecoveryRequired
	}
	if o.Action != ActionProvision {
		if e = a.ensureRole(ctx, c, o.GatewayID, role, false); e != nil {
			result.Outcome = ExecutionRecoveryRequired
			return result, ErrRecoveryRequired
		}
	}
	var secret string
	if o.Action != ActionRevoke {
		if generate == nil {
			return result, ErrVerificationFailed
		}
		secret, e = generate(ctx)
		if e != nil || !validPassword(secret) || ctx.Err() != nil {
			return result, ErrVerificationFailed
		}
		defer func() { secret = "" }()
	}
	// Mark uncertainty BEFORE publication, including role/client partial failure.
	mutated = true
	if o.Action == ActionProvision {
		if e = a.ensureRole(ctx, c, o.GatewayID, role, true); e != nil {
			return result, ErrRecoveryRequired
		}
		if absent {
			if e = c.CreateClient(ctx, o.GatewayID); e != nil {
				return result, ErrRecoveryRequired
			}
		}
	}
	if e = c.DisableClient(ctx, o.GatewayID); e != nil {
		return result, ErrRecoveryRequired
	}
	disabled := o.Action == ActionRevoke
	if !disabled {
		if e = c.SetClientPassword(ctx, o.GatewayID, secret); e != nil {
			return result, ErrRecoveryRequired
		}
		if o.Action == ActionProvision && absent {
			if e = c.AddClientRole(ctx, o.GatewayID, role); e != nil {
				return result, ErrRecoveryRequired
			}
		}
		if e = c.EnableClient(ctx, o.GatewayID); e != nil {
			return result, ErrRecoveryRequired
		}
	}
	native, e = c.GetClient(ctx, o.GatewayID)
	if e != nil || !adapterClientMatches(native, o.GatewayID, role, disabled) {
		return result, ErrRecoveryRequired
	}
	result.Evidence.RAMApplied = true
	snapshot, e := a.cfg.Observe(ctx, o.GatewayID)
	if e != nil || !adapterClientMatches(snapshot, o.GatewayID, role, disabled) {
		return result, ErrRecoveryRequired
	}
	result.Evidence.SnapshotObserved = true
	if !disabled {
		c.Close()
		c = nil
		fresh, e := a.cfg.Controller.RestartClosed(ctx)
		if e != nil || fresh.Open || !fresh.Alive || fresh.Epoch == before.Epoch || fresh.Epoch == "" || fresh.Nonce == "" {
			return result, ErrRecoveryRequired
		}
		// Evidence collected below belongs only to this cold-loaded lifetime.
		// A later Describe must not re-label it with an unverified new epoch.
		before = fresh
		c, e = a.cfg.NewClient(ctx)
		if e != nil || c == nil {
			return result, ErrRecoveryRequired
		}
		native, e = c.GetClient(ctx, o.GatewayID)
		if e != nil || !adapterClientMatches(native, o.GatewayID, role, false) {
			return result, ErrRecoveryRequired
		}
		if e = a.ensureRole(ctx, c, o.GatewayID, role, false); e != nil {
			return result, ErrRecoveryRequired
		}
		if e = a.cfg.Login(ctx, o.GatewayID, secret); e != nil {
			return result, ErrRecoveryRequired
		}
		result.Evidence.FreshPositiveVerified = true
		snapshot, e = a.cfg.Observe(ctx, o.GatewayID)
		if e != nil || !adapterClientMatches(snapshot, o.GatewayID, role, false) {
			return result, ErrRecoveryRequired
		}
	}
	d, e := a.cfg.Controller.Describe(ctx)
	if e != nil || !d.Alive || d.Open || d.Epoch != before.Epoch || d.Nonce != before.Nonce {
		return result, ErrRecoveryRequired
	}
	result.receipt = VerificationReceipt{Epoch: d.Epoch, Nonce: d.Nonce}
	result.Outcome = ExecutionVerifiedSuccess
	return result, nil
}

func adapterClientMatches(v DynSecClientMetadata, u, role string, disabled bool) bool {
	return v.Username == u && v.Disabled == disabled && len(v.Roles) == 1 && v.Roles[0] == role && len(v.Groups) == 0 && v.ClientID == ""
}

func provisionNativeAdmissible(previous *CredentialSnapshot, absent bool, native DynSecClientMetadata, u string) bool {
	if previous == nil {
		return absent
	}
	// Durable revoked/failed authority still requires a new committed provision
	// intent; absent is safe here, unlike an arbitrary GetClient error.
	return (previous.Status == CredentialRevoked || previous.Status == CredentialFailed) && (absent || adapterClientMatches(native, u, "gateway_"+u, true))
}

// CheckRevoked is a read-only current-state check for authorized new-key no-ops.
// It does not mint an OPEN receipt, mutate native state or reuse historical proof.
func (a *DynSecAdapter) CheckRevoked(ctx context.Context, u string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.busy {
		return ErrRuntimeBusy
	}
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	before, e := a.cfg.Controller.Describe(ctx)
	if e != nil || !before.Alive || !before.Open || !validEpoch(before.Epoch) || before.Nonce != "" {
		return ErrRecoveryRequired
	}
	c, e := a.cfg.NewClient(ctx)
	if e != nil || c == nil {
		return ErrRecoveryRequired
	}
	defer c.Close()
	native, e := c.GetClient(ctx, u)
	if e != nil || !adapterClientMatches(native, u, "gateway_"+u, true) || a.ensureRole(ctx, c, u, "gateway_"+u, false) != nil {
		return ErrRecoveryRequired
	}
	snapshot, e := a.cfg.Observe(ctx, u)
	if e != nil || !adapterClientMatches(snapshot, u, "gateway_"+u, true) {
		return ErrRecoveryRequired
	}
	after, e := a.cfg.Controller.Describe(ctx)
	if e != nil || !after.Alive || !after.Open || after.Epoch != before.Epoch || after.Nonce != before.Nonce {
		return ErrRecoveryRequired
	}
	return nil
}

type adapterACL struct {
	Type     string `json:"acltype"`
	Topic    string `json:"topic"`
	Priority int    `json:"priority"`
	Allow    bool   `json:"allow"`
}

func adapterGatewayACLs(u string) []adapterACL {
	base := "gateways/" + u + "/"
	return []adapterACL{{"publishClientSend", base + "telemetry/#", 0, true}, {"publishClientSend", base + "status", 0, true}, {"publishClientSend", base + "responses/#", 0, true}, {"publishClientReceive", base + "acks/#", 0, true}, {"publishClientReceive", base + "commands/#", 0, true}, {"subscribeLiteral", base + "acks/#", 0, true}, {"subscribeLiteral", base + "commands/#", 0, true}, {"unsubscribeLiteral", base + "acks/#", 0, true}, {"unsubscribeLiteral", base + "commands/#", 0, true}}
}
func (a *DynSecAdapter) ensureRole(ctx context.Context, c *DynSecClient, u, role string, create bool) error {
	if !c.target(u) || !c.cfg.ValidateRole(u, role) {
		return ErrInvalidInput
	}
	wanted := adapterGatewayACLs(u)
	b, e := c.adapterRequest(ctx, "getRole", map[string]any{"rolename": role})
	if errors.Is(e, errDynSecRoleAbsent) {
		if !create {
			return ErrRecoveryRequired
		}
		if _, e = c.adapterRequest(ctx, "createRole", map[string]any{"rolename": role, "acls": wanted}); e != nil {
			return e
		}
		b, e = c.adapterRequest(ctx, "getRole", map[string]any{"rolename": role})
	}
	if e != nil {
		return e
	}
	var data struct {
		Role struct {
			Name string       `json:"rolename"`
			ACLs []adapterACL `json:"acls"`
		} `json:"role"`
	}
	if json.Unmarshal(b, &data) != nil || data.Role.Name != role || len(data.Role.ACLs) != len(wanted) {
		return ErrRecoveryRequired
	}
	// Native output groups ACLs by type; compare an exact set, no extra grants.
	for _, acl := range wanted {
		n := 0
		for _, got := range data.Role.ACLs {
			if reflect.DeepEqual(acl, got) {
				n++
			}
		}
		if n != 1 {
			return ErrRecoveryRequired
		}
	}
	return nil
}

// OpenAfterFinalization is an explicit trusted service seam, not DB proof.
// Service MUST confirm finalization commit (resolve ambiguity on a new DB
// connection), reconcile ALL current authority and acknowledge this exact ID.
// Same-UID capability compromise is outside this unit's trust boundary.
func (a *DynSecAdapter) OpenAfterFinalization(ctx context.Context, r DynSecAdapterResult, finalized uuid.UUID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.busy || r != a.pending || r.Outcome != ExecutionVerifiedSuccess || finalized == uuid.Nil || finalized != r.OperationID {
		return ErrLifecycleUnavailable
	}
	if e := a.cfg.Controller.OpenVerified(ctx, r.receipt); e != nil {
		return ErrLifecycleUnavailable
	}
	a.busy = false
	a.opened = r
	a.pending = DynSecAdapterResult{}
	return nil
}

// ReleaseClosed permits a future recovery/reconciliation pass, never OPEN.
// Caller must durably record failure/recovery first; secrets are never replayed.
func (a *DynSecAdapter) ReleaseClosed(ctx context.Context, id uuid.UUID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.busy || id == uuid.Nil || id != a.pending.OperationID {
		return ErrLifecycleUnavailable
	}
	if e := a.cfg.Controller.CloseDrain(ctx); e != nil {
		return ErrLifecycleUnavailable
	}
	a.busy = false
	a.pending = DynSecAdapterResult{}
	return nil
}

// VerifyRevocations uses the pending operation's private channel without nested
// Execute/admission. Decisions must be freshly scanned current DB authority.
// In particular the operation being reprovisioned cannot revoke itself from an
// obsolete historical decision. Absence needs the same narrow native and
// protected snapshot proof as startup; generic lookup failures fail closed.
func (a *DynSecAdapter) VerifyRevocations(ctx context.Context, r DynSecAdapterResult, decisions []RevocationDecision) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.busy || r != a.pending || r.Outcome != ExecutionVerifiedSuccess {
		return ErrLifecycleUnavailable
	}
	for _, d := range decisions {
		if d.OperationID == uuid.Nil || d.OperationID == r.OperationID || d.CredentialVersion <= 0 {
			return ErrRecoveryRequired
		}
	}
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	d, e := a.cfg.Controller.Describe(ctx)
	if e != nil || d.Open || !d.Alive || d.Epoch != r.receipt.Epoch || d.Nonce != r.receipt.Nonce {
		return ErrLifecycleUnavailable
	}
	if len(decisions) == 0 {
		return nil
	}
	c, e := a.cfg.NewClient(ctx)
	if e != nil || c == nil {
		return ErrRecoveryRequired
	}
	defer c.Close()
	for _, decision := range decisions {
		u := decision.GatewayID
		if gateway.ValidateGatewayID(u) != nil || u == BackendUsername {
			return ErrInvalidInput
		}
		for _, protected := range a.cfg.ProtectedUsernames {
			if u == protected {
				return ErrInvalidInput
			}
		}
		native, e := c.GetClient(ctx, u)
		if errors.Is(e, errDynSecClientAbsent) {
			if !a.snapshotProvesAbsence(ctx, u) {
				return ErrRecoveryRequired
			}
			continue
		}
		if e != nil || !adapterClientMatches(native, u, "gateway_"+u, native.Disabled) {
			return ErrRecoveryRequired
		}
		if e = a.ensureRole(ctx, c, u, "gateway_"+u, false); e != nil {
			return ErrRecoveryRequired
		}
		if e = c.DisableClient(ctx, u); e != nil {
			return ErrRecoveryRequired
		}
		native, e = c.GetClient(ctx, u)
		if e != nil || !adapterClientMatches(native, u, "gateway_"+u, true) {
			return ErrRecoveryRequired
		}
		snapshot, e := a.cfg.Observe(ctx, u)
		if e != nil || !adapterClientMatches(snapshot, u, "gateway_"+u, true) {
			return ErrRecoveryRequired
		}
	}
	d, e = a.cfg.Controller.Describe(ctx)
	if e != nil || d.Open || !d.Alive || d.Epoch != r.receipt.Epoch || d.Nonce != r.receipt.Nonce {
		return ErrLifecycleUnavailable
	}
	return nil
}

// CloseDrain also works after an uncertain OPEN/checkpoint commit. It does not
// claim a completed immutable checkpoint was undone.
func (a *DynSecAdapter) CloseDrain(ctx context.Context) error {
	a.mu.Lock()
	a.opened = DynSecAdapterResult{}
	a.mu.Unlock()
	return a.cfg.Controller.CloseDrain(ctx)
}

// VerifyOpen narrows the last sequential disclosure check to the exact lifetime
// ACK. Controller death immediately afterwards remains a deployment/HA limit.
func (a *DynSecAdapter) VerifyOpen(ctx context.Context, r DynSecAdapterResult) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r != a.opened || r.OperationID == uuid.Nil {
		return ErrLifecycleUnavailable
	}
	d, e := a.cfg.Controller.Describe(ctx)
	// OPEN consumes Nonce. Only this adapter's ACK plus current OPEN lifetime
	// is evidence; comparing the consumed challenge would reject every real ACK.
	if e != nil || !d.Alive || !d.Open || d.Epoch != r.receipt.Epoch || d.Nonce != "" {
		return ErrLifecycleUnavailable
	}
	return nil
}
