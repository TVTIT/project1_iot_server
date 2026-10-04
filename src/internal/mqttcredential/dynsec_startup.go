package mqttcredential

import (
	"context"
	"errors"
	"iot-platform/internal/gateway"
)

// Startup owns a fresh adapter before exposing the service. busy is held until
// OPEN; failed passes can retry only via the startup capability, never Execute.
func (a *DynSecAdapter) BeginStartup(ctx context.Context) (VerificationReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.busy && a.pending.OperationID != [16]byte{} {
		return VerificationReceipt{}, ErrRuntimeBusy
	}
	a.busy = true
	if e := a.cfg.Controller.CloseDrain(ctx); e != nil {
		return VerificationReceipt{}, e
	}
	// A startup pass cold-loads a NEW CLOSED lifetime. This prevents a previous
	// checkpoint's successful epoch from being reused as lost-secret recovery.
	d, e := a.cfg.Controller.RestartClosed(ctx)
	if e != nil || !d.Alive || d.Open || !validEpoch(d.Epoch) || !validEpoch(d.Nonce) {
		return VerificationReceipt{}, ErrLifecycleUnavailable
	}
	return VerificationReceipt{Epoch: d.Epoch, Nonce: d.Nonce}, nil
}

func (a *DynSecAdapter) startupEpoch(ctx context.Context, r VerificationReceipt) error {
	d, e := a.cfg.Controller.Describe(ctx)
	if !a.busy || a.pending.OperationID != [16]byte{} || e != nil || !d.Alive || d.Open || d.Epoch != r.Epoch || d.Nonce != r.Nonce {
		return ErrLifecycleUnavailable
	}
	return nil
}

// Absence is accepted ONLY from both the correlated native getClient response
// and a protected fully validated snapshot. It proves new authentication is
// impossible for this principal, not positive authentication of any password.
// The caller must separately verify its current CLOSED epoch/challenge before
// and after observation. This is current absence, not a stale generation claim,
// a physical connection witness, or a password/power-loss durability proof.
func (a *DynSecAdapter) snapshotProvesAbsence(ctx context.Context, u string) bool {
	_, err := a.cfg.Observe(ctx, u)
	return errors.Is(err, errDynSecSnapshotAbsent)
}

func (a *DynSecAdapter) DisableStartup(ctx context.Context, r VerificationReceipt, u string) (RecoveryDisableEvidence, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	bad := RecoveryDisableEvidence{}
	if gateway.ValidateGatewayID(u) != nil || u == BackendUsername {
		return bad, ErrInvalidInput
	}
	for _, p := range a.cfg.ProtectedUsernames {
		if u == p {
			return bad, ErrInvalidInput
		}
	}
	if e := a.startupEpoch(ctx, r); e != nil {
		return bad, e
	}
	c, e := a.cfg.NewClient(ctx)
	if e != nil || c == nil {
		return bad, ErrRecoveryRequired
	}
	defer c.Close()
	v, e := c.GetClient(ctx, u)
	if errors.Is(e, errDynSecClientAbsent) {
		if !a.snapshotProvesAbsence(ctx, u) {
			return bad, ErrRecoveryRequired
		}
	} else {
		if e != nil || !adapterClientMatches(v, u, "gateway_"+u, v.Disabled) || a.ensureRole(ctx, c, u, "gateway_"+u, false) != nil {
			return bad, ErrRecoveryRequired
		}
		if e = c.DisableClient(ctx, u); e != nil {
			return bad, ErrRecoveryRequired
		}
		v, e = c.GetClient(ctx, u)
		if e != nil || !adapterClientMatches(v, u, "gateway_"+u, true) {
			return bad, ErrRecoveryRequired
		}
		v, e = a.cfg.Observe(ctx, u)
		if e != nil || !adapterClientMatches(v, u, "gateway_"+u, true) {
			return bad, ErrRecoveryRequired
		}
	}
	if e = a.startupEpoch(ctx, r); e != nil {
		return bad, e
	}
	return RecoveryDisableEvidence{RAMDisabled: true, SnapshotObserved: true, BrokerEpoch: r.Epoch}, nil
}

func (a *DynSecAdapter) VerifyStartupInventory(ctx context.Context, r VerificationReceipt, rows []Metadata) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.startupEpoch(ctx, r); e != nil {
		return e
	}
	c, e := a.cfg.NewClient(ctx)
	if e != nil || c == nil {
		return ErrRecoveryRequired
	}
	defer c.Close()
	for _, m := range rows {
		v, e := c.GetClient(ctx, m.GatewayID)
		if m.Status == CredentialRevoked && errors.Is(e, errDynSecClientAbsent) {
			if !a.snapshotProvesAbsence(ctx, m.GatewayID) {
				return ErrRecoveryRequired
			}
			continue
		}
		if m.Status != CredentialActive && m.Status != CredentialRevoked {
			return ErrRecoveryRequired
		}
		disabled := m.Status == CredentialRevoked
		if e != nil || !adapterClientMatches(v, m.GatewayID, "gateway_"+m.GatewayID, disabled) || a.ensureRole(ctx, c, m.GatewayID, "gateway_"+m.GatewayID, false) != nil {
			return ErrRecoveryRequired
		}
		v, e = a.cfg.Observe(ctx, m.GatewayID)
		if e != nil || !adapterClientMatches(v, m.GatewayID, "gateway_"+m.GatewayID, disabled) {
			return ErrRecoveryRequired
		}
	}
	return a.startupEpoch(ctx, r)
}

func (a *DynSecAdapter) OpenStartup(ctx context.Context, r VerificationReceipt) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.startupEpoch(ctx, r); e != nil {
		return e
	}
	if e := a.cfg.Controller.OpenVerified(ctx, r); e != nil {
		return e
	}
	d, e := a.cfg.Controller.Describe(ctx)
	if e != nil || !d.Alive || !d.Open || d.Epoch != r.Epoch || d.Nonce != "" {
		return ErrLifecycleUnavailable
	}
	a.busy = false
	return nil
}
