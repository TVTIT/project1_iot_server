package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// SimplifiedDynSecClient defines the narrow management operations required by
// the simplified DynSec executor. DynSecClient satisfies this interface.
type SimplifiedDynSecClient interface {
	CreateClient(ctx context.Context, username string) error
	DisableClient(ctx context.Context, username string) error
	EnableClient(ctx context.Context, username string) error
	SetClientPassword(ctx context.Context, username, password string) error
	AddClientRole(ctx context.Context, username, role string) error
	GetClient(ctx context.Context, username string) (DynSecClientMetadata, error)
	Close()
	// adapterRequest is package-private to DynSecClient and used for role/ACL management
	adapterRequest(ctx context.Context, command string, fields map[string]any) (json.RawMessage, error)
}

// SimplifiedDynSecConfig configures the controller-free simplified executor.
// Fresh client and probe connections are injected through caller-supplied factories.
type SimplifiedDynSecConfig struct {
	NewClient          func(context.Context) (SimplifiedDynSecClient, error)
	Login              func(ctx context.Context, username, password string) error
	Rejected           func(ctx context.Context, username, password string) error
	ProtectedUsernames []string
	Timeout            time.Duration
}

// SimplifiedExecutionEvidence captures real native observations for an execution.
// RAMApplied: true after RAM modifications and verified native getClient/role readback.
// FreshPositiveVerified: true after fresh TLS login succeeds with the exact new password.
// ProtocolDenialVerified: true for ActionRevoke after fresh TLS probe confirms denial.
// SessionHandlingVerified: true ONLY when backed by trusted executor observation from
// qualified pinned native semantics/probes confirming disconnection and active-session containment.
// Fresh login denial, native disabled readback, Close, mutex, or config flags are NOT sufficient.
// This is evidence of the dependency observation, NOT a request DTO, config, or env flag.
type SimplifiedExecutionEvidence struct {
	RAMApplied              bool `json:"ram_applied"`
	FreshPositiveVerified   bool `json:"fresh_positive_verified"`
	ProtocolDenialVerified  bool `json:"protocol_denial_verified"`
	SessionHandlingVerified bool `json:"session_handling_verified"`
}

// SimplifiedExecutionResult captures the outcome and native evidence of an execution.
type SimplifiedExecutionResult struct {
	OperationID uuid.UUID
	Outcome     ExecutionOutcome
	Evidence    SimplifiedExecutionEvidence
	Mutated     bool // true if a mutation command was sent or may have been received by the broker
}

// SimplifiedDynSecExecutor executes simplified credential operations directly
// against Mosquitto DynSec without an external controller, epoch gate, or watchdog.
type SimplifiedDynSecExecutor struct {
	cfg SimplifiedDynSecConfig
}

// NewSimplifiedDynSecExecutor validates config and instantiates a simplified executor.
func NewSimplifiedDynSecExecutor(cfg SimplifiedDynSecConfig) (*SimplifiedDynSecExecutor, error) {
	if cfg.NewClient == nil || cfg.Login == nil || cfg.Rejected == nil ||
		len(cfg.ProtectedUsernames) < 2 || cfg.Timeout <= 0 || cfg.Timeout > time.Minute {
		return nil, ErrInvalidInput
	}
	seen := make(map[string]bool)
	for _, u := range cfg.ProtectedUsernames {
		if u == "" || seen[u] {
			return nil, ErrInvalidInput
		}
		seen[u] = true
	}
	cfg.ProtectedUsernames = append([]string(nil), cfg.ProtectedUsernames...)
	return &SimplifiedDynSecExecutor{cfg: cfg}, nil
}

// target validates operation eligibility under simplified contract rules.
func (e *SimplifiedDynSecExecutor) target(o Operation) bool {
	if o.ExecutionContract != ContractSimplified {
		return false
	}
	if o.Validate() != nil || o.Status != OperationPending || o.Phase != PhaseIntent ||
		o.Evidence != (VerificationEvidence{}) || o.GatewayID == BackendUsername {
		return false
	}
	for _, u := range e.cfg.ProtectedUsernames {
		if u == o.GatewayID {
			return false
		}
	}

	switch o.Action {
	case ActionProvision:
		// Initial provision: Previous is nil. Reprovision: Previous must be revoked or failed.
		return o.Previous == nil || o.Previous.Status == CredentialRevoked || o.Previous.Status == CredentialFailed
	case ActionRotate:
		// Rotate requires active predecessor with monotonic version
		return o.Previous != nil && o.Previous.Status == CredentialActive && o.CredentialVersion > o.Previous.CredentialVersion
	case ActionRevoke:
		// Revoke requires active predecessor with same version
		return o.Previous != nil && o.Previous.Status == CredentialActive && o.CredentialVersion == o.Previous.CredentialVersion
	case ActionReset:
		// Reset requires validated ResetPredecessorID, ResetReason, and monotonic version
		if o.ResetPredecessorID == nil || *o.ResetPredecessorID == uuid.Nil || o.ResetReason == nil || o.ResetReason.Validate() != nil {
			return false
		}
		if o.Previous != nil && o.CredentialVersion <= o.Previous.CredentialVersion {
			return false
		}
		return true
	default:
		return false
	}
}

// Execute performs the simplified operation lifecycle:
// fresh client -> baseline verification -> mutation -> native state checks -> fresh auth probe -> close client.
func (e *SimplifiedDynSecExecutor) Execute(ctx context.Context, o Operation, generate func(context.Context) (string, error)) (res SimplifiedExecutionResult, err error) {
	res = SimplifiedExecutionResult{
		OperationID: o.OperationID,
		Outcome:     ExecutionUnchangedFailure,
	}

	if !e.target(o) {
		return res, ErrInvalidInput
	}

	// Safety containment (Step 1): ActionRevoke execution is not yet qualified for
	// native session containment or active-session drain in Mosquitto DynSec.
	// Mosquitto DynSec disableClient disconnect/drain behavior under active
	// publish/receive sessions is unverified and not qualified as session-drain proof.
	// Reject early before NewClient allocation, generator invocation, or any broker request,
	// returning ExecutionUnchangedFailure, Mutated=false, zero evidence, and ErrLifecycleUnavailable.
	// Pinned native behavior and actual before/after session drain verification will
	// be qualified in a subsequent stage; this stage ensures zero mutation and zero false evidence.
	if o.Action == ActionRevoke {
		return res, ErrLifecycleUnavailable
	}

	// Bound overall operation by configured timeout
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	// Password generator handling:
	// Provision, Rotate, Reset MUST provide a non-nil generator.
	if generate == nil {
		return res, ErrVerificationFailed
	}
	var secret string
	var genErr error
	secret, genErr = generate(ctx)
	if genErr != nil || !validPassword(secret) || ctx.Err() != nil {
		return res, ErrVerificationFailed
	}
	// Ephemeral ownership: clear secret upon return
	defer func() { secret = "" }()

	// Ensure fresh management client per operation
	c, clientErr := e.cfg.NewClient(ctx)
	if clientErr != nil || c == nil {
		return res, ErrVerificationFailed
	}
	defer c.Close()
	if ctx.Err() != nil {
		return res, ErrVerificationFailed
	}

	role := "gateway_" + o.GatewayID

	// Inspect initial native state
	native, getErr := c.GetClient(ctx, o.GatewayID)
	absent := errors.Is(getErr, errDynSecClientAbsent)
	if getErr != nil && !absent {
		return res, ErrVerificationFailed
	}

	// Baseline checks according to action
	switch o.Action {
	case ActionProvision:
		if o.Previous == nil && !absent {
			// Brand new provision: existing unknown client must NOT be silently adopted
			res.Outcome = ExecutionRecoveryRequired
			return res, ErrRecoveryRequired
		}
		if o.Previous != nil {
			// Reprovision: client may be absent, or disabled matching role
			if !absent && !adapterClientMatches(native, o.GatewayID, role, true) {
				res.Outcome = ExecutionRecoveryRequired
				return res, ErrRecoveryRequired
			}
		}
	case ActionRotate:
		if absent || !adapterClientMatches(native, o.GatewayID, role, false) {
			res.Outcome = ExecutionRecoveryRequired
			return res, ErrRecoveryRequired
		}
		if err = e.ensureRole(ctx, c, o.GatewayID, role, false); err != nil {
			res.Outcome = ExecutionRecoveryRequired
			return res, ErrRecoveryRequired
		}
	case ActionReset:
		// Reset handles known mappings:
		// If lost_secret_recovery: native client must be present, active, and matching
		// If recovery_after_unknown: native client might be absent, disabled, or partially configured
		if *o.ResetReason == ResetReasonLostSecretRecovery {
			if absent || !adapterClientMatches(native, o.GatewayID, role, false) {
				res.Outcome = ExecutionRecoveryRequired
				return res, ErrRecoveryRequired
			}
		} else {
			// RecoveryAfterUnknown:
			// If present, verify it matches our expected target username (not a foreign or unassociated client)
			if !absent {
				if native.Username != o.GatewayID {
					res.Outcome = ExecutionRecoveryRequired
					return res, ErrRecoveryRequired
				}
			}
		}
	}

	// Before sending first mutating command, mark uncertainty / mutation potential
	if ctx.Err() != nil {
		return res, ErrVerificationFailed
	}
	res.Mutated = true

	// Clean up and mark recovery_required on any post-mutation failure.
	// Preserves fail-closed semantics without launching untracked extra broker mutations that can survive into reset.
	defer func() {
		if err != nil && res.Mutated {
			res.Outcome = ExecutionRecoveryRequired
		}
	}()

	// Execute mutations
	switch o.Action {
	case ActionProvision:
		if err = e.ensureRole(ctx, c, o.GatewayID, role, true); err != nil {
			return res, ErrRecoveryRequired
		}
		if absent {
			if err = c.CreateClient(ctx, o.GatewayID); err != nil {
				return res, ErrRecoveryRequired
			}
		}
		if err = c.DisableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}
		if err = c.SetClientPassword(ctx, o.GatewayID, secret); err != nil {
			return res, ErrRecoveryRequired
		}
		if absent {
			if err = c.AddClientRole(ctx, o.GatewayID, role); err != nil {
				return res, ErrRecoveryRequired
			}
		}
		if err = c.EnableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}

	case ActionRotate:
		// Disable -> set password -> enable
		if err = c.DisableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}
		if err = c.SetClientPassword(ctx, o.GatewayID, secret); err != nil {
			return res, ErrRecoveryRequired
		}
		if err = c.EnableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}

	case ActionReset:
		// Ensure role exists
		if err = e.ensureRole(ctx, c, o.GatewayID, role, true); err != nil {
			return res, ErrRecoveryRequired
		}
		if absent {
			if err = c.CreateClient(ctx, o.GatewayID); err != nil {
				return res, ErrRecoveryRequired
			}
		}
		if err = c.DisableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}
		if err = c.SetClientPassword(ctx, o.GatewayID, secret); err != nil {
			return res, ErrRecoveryRequired
		}
		// Ensure role is assigned if client was created or missing role
		if absent || len(native.Roles) == 0 {
			if err = c.AddClientRole(ctx, o.GatewayID, role); err != nil {
				return res, ErrRecoveryRequired
			}
		}
		if err = c.EnableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}
	}

	// Native state checks after mutation: Provision, Rotate, Reset all configure an enabled client
	nativeAfter, getAfterErr := c.GetClient(ctx, o.GatewayID)
	if getAfterErr != nil || !adapterClientMatches(nativeAfter, o.GatewayID, role, false) {
		err = ErrRecoveryRequired
		return res, err
	}
	if err = e.ensureRole(ctx, c, o.GatewayID, role, false); err != nil {
		err = ErrRecoveryRequired
		return res, err
	}
	res.Evidence.RAMApplied = true

	// Fresh connection probe: TLS login must succeed with exact newly configured secret.
	// (Unqualified fixed-invalid-password probe for revoke has been removed; revoke is rejected early).
	if err = e.cfg.Login(ctx, o.GatewayID, secret); err != nil {
		err = ErrRecoveryRequired
		return res, err
	}
	res.Evidence.FreshPositiveVerified = true

	if ctx.Err() != nil {
		err = ErrRecoveryRequired
		return res, err
	}
	res.Outcome = ExecutionVerifiedSuccess
	return res, nil
}

// CheckRevoked verifies that the client is currently disabled with exact gateway role
// in Mosquitto DynSec. It is a read-only check for already-revoked idempotent replay.
//
// NOTE: This is a native read-only disabled-state check for historical replay ONLY.
// It is NOT active-session drain proof and does NOT guarantee quiescence, disconnection,
// or drain of existing TCP/MQTT sessions or in-flight subscriptions. Revoke session-containment
// qualification remains pending.
func (e *SimplifiedDynSecExecutor) CheckRevoked(ctx context.Context, u string) error {
	if gateway.ValidateGatewayID(u) != nil || u == BackendUsername {
		return ErrInvalidInput
	}
	for _, protected := range e.cfg.ProtectedUsernames {
		if protected == u {
			return ErrInvalidInput
		}
	}

	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	c, err := e.cfg.NewClient(ctx)
	if err != nil || c == nil {
		return ErrRecoveryRequired
	}
	defer c.Close()

	role := "gateway_" + u
	native, err := c.GetClient(ctx, u)
	if err != nil || !adapterClientMatches(native, u, role, true) {
		return ErrRecoveryRequired
	}
	if err = e.ensureRole(ctx, c, u, role, false); err != nil {
		return ErrRecoveryRequired
	}
	return nil
}

// ensureRole verifies that the gateway role exists and has exact required ACLs.
// If create is true and role is absent, it creates it.
func (e *SimplifiedDynSecExecutor) ensureRole(ctx context.Context, c SimplifiedDynSecClient, u, role string, create bool) error {
	if gateway.ValidateGatewayID(u) != nil || u == BackendUsername {
		return ErrInvalidInput
	}
	for _, p := range e.cfg.ProtectedUsernames {
		if u == p {
			return ErrInvalidInput
		}
	}
	if role != "gateway_"+u {
		return ErrInvalidInput
	}

	wanted := adapterGatewayACLs(u)
	b, err := c.adapterRequest(ctx, "getRole", map[string]any{"rolename": role})
	if errors.Is(err, errDynSecRoleAbsent) {
		if !create {
			return ErrRecoveryRequired
		}
		if _, err = c.adapterRequest(ctx, "createRole", map[string]any{"rolename": role, "acls": wanted}); err != nil {
			return err
		}
		b, err = c.adapterRequest(ctx, "getRole", map[string]any{"rolename": role})
	}
	if err != nil {
		return err
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
