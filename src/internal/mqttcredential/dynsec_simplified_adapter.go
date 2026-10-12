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

// Qualified Mosquitto 2.0.18 native capability descriptor constants.
// Qualification is a trusted release/composition binding, not a runtime remote broker
// attestation via MQTT banner or runtime evidence parsing.
const (
	QualifiedMosquittoImageRef = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
	QualifiedMosquittoConfigID = "sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
	QualifiedMosquittoPlatform = "linux/amd64"
	QualifiedMosquittoVersion  = "2.0.18"
)

// NativeDisableQualification is a comparable value type representing a verified
// native capability qualification for Mosquitto DynSec disableClient semantics.
// All identity fields are unexported to prevent caller manipulation or synthetic booleans.
type NativeDisableQualification struct {
	imageRef string
	configID string
	platform string
	version  string
}

// NewNativeDisableQualification constructs a validated NativeDisableQualification.
// It accepts only the exact tested and qualified composition identity:
// - imageRef: "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
// - configID: "sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
// - platform: "linux/amd64"
// Config ID must not generically be assumed equal to image digest.
// Any absent, unpinned, mismatched, latest, or unsupported platform is rejected with ErrInvalidInput.
func NewNativeDisableQualification(imageRef, configID, platform string) (NativeDisableQualification, error) {
	if imageRef != QualifiedMosquittoImageRef ||
		configID != QualifiedMosquittoConfigID ||
		platform != QualifiedMosquittoPlatform {
		return NativeDisableQualification{}, ErrInvalidInput
	}
	return NativeDisableQualification{
		imageRef: imageRef,
		configID: configID,
		platform: platform,
		version:  QualifiedMosquittoVersion,
	}, nil
}

// IsZero reports whether the qualification descriptor is the unconfigured zero value.
func (q NativeDisableQualification) IsZero() bool {
	return q == NativeDisableQualification{}
}

// Valid reports whether the qualification descriptor matches the exact qualified identity.
func (q NativeDisableQualification) Valid() bool {
	return q.imageRef == QualifiedMosquittoImageRef &&
		q.configID == QualifiedMosquittoConfigID &&
		q.platform == QualifiedMosquittoPlatform &&
		q.version == QualifiedMosquittoVersion
}

// ImageRef returns the immutable image reference.
func (q NativeDisableQualification) ImageRef() string {
	return q.imageRef
}

// ConfigID returns the immutable config identifier.
func (q NativeDisableQualification) ConfigID() string {
	return q.configID
}

// Platform returns the qualified platform string.
func (q NativeDisableQualification) Platform() string {
	return q.platform
}

// Version returns the qualified Mosquitto version.
func (q NativeDisableQualification) Version() string {
	return q.version
}

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
// Qualification is an optional trusted descriptor of the qualified native broker composition.
// When zero, revoke is not qualified. Nonzero descriptors must be valid or constructor rejects.
type SimplifiedDynSecConfig struct {
	NewClient          func(context.Context) (SimplifiedDynSecClient, error)
	Login              func(ctx context.Context, username, password string) error
	Rejected           func(ctx context.Context, username, password string) error
	ProtectedUsernames []string
	Timeout            time.Duration
	VerifyRevoked      func(ctx context.Context, target string) error
	Qualification      NativeDisableQualification
}

// SimplifiedExecutionEvidence captures real native observations for an execution.
//
// Historical/Legacy fields (retained for backward compatibility):
// - RAMApplied: true after RAM modifications and verified native getClient/role readback.
// - FreshPositiveVerified: true after fresh TLS login succeeds with the exact new password.
// - ProtocolDenialVerified: legacy observation field; must NOT be set true from qualification alone.
// - SessionHandlingVerified: legacy observation field; must NOT be set true from qualification alone.
//
// Per-operation native observations:
//   - NativeDisabledObserved: true ONLY when verified native readback confirms client is disabled in plugin state.
//   - NativePersistedRoleACLObserved: true ONLY when verified native readback confirms role and ACL permissions are in place.
//     Snapshot/persistence observation does NOT mean fsync or exact new-password persistence.
//   - Qualification: reflects the trusted qualification descriptor under which this execution ran.
//
// Qualification is NOT a per-operation EOF or old-password probe.
type SimplifiedExecutionEvidence struct {
	RAMApplied                     bool                       `json:"ram_applied"`
	FreshPositiveVerified          bool                       `json:"fresh_positive_verified"`
	ProtocolDenialVerified         bool                       `json:"protocol_denial_verified"`
	SessionHandlingVerified        bool                       `json:"session_handling_verified"`
	NativeDisabledObserved         bool                       `json:"native_disabled_observed"`
	NativePersistedRoleACLObserved bool                       `json:"native_persisted_role_acl_observed"`
	Qualification                  NativeDisableQualification `json:"qualification,omitempty"`
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
	// If a nonzero qualification is supplied, it must match the exact qualified descriptor.
	if !cfg.Qualification.IsZero() && !cfg.Qualification.Valid() {
		return nil, ErrInvalidInput
	}
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

	// Dispatch ActionRevoke to executeRevoke before any password generator handling or NewClient allocation.
	// Revoke does not require or use a password generator. It executes only when qualification and
	// snapshot verifier prerequisites are present, otherwise failing safe with zero mutation/callbacks.
	if o.Action == ActionRevoke {
		return e.executeRevoke(ctx, o)
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
		// ROT1 ordered sequence:
		// 1. DisableClient
		// 2. Disabled readback (GetClient: disabled=true, expected role/no unexpected groups)
		// 3. Exact ACL readback (ensureRole create=false)
		// 4. SetClientPassword (only after both readbacks succeed)
		// 5. EnableClient
		//
		// Note on persistence guarantee (ROT1 vs deferred ROT2):
		// Current-runtime login (FreshPositiveVerified) proves the new password is accepted
		// by the running Mosquitto broker in memory. Verification of exact persistence of the
		// new password to disk (ROT2) is explicitly deferred; rotate evidence does not claim
		// SnapshotObserved.
		if err = c.DisableClient(ctx, o.GatewayID); err != nil {
			return res, ErrRecoveryRequired
		}
		var disabledMeta DynSecClientMetadata
		disabledMeta, err = c.GetClient(ctx, o.GatewayID)
		if err != nil || !adapterClientMatches(disabledMeta, o.GatewayID, role, true) {
			return res, ErrRecoveryRequired
		}
		if err = e.ensureRole(ctx, c, o.GatewayID, role, false); err != nil {
			return res, ErrRecoveryRequired
		}
		if ctx.Err() != nil {
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

// executeRevoke executes the private revoke operation workflow directly against
// Mosquitto DynSec without an external controller or epoch gate.
//
// The revoke workflow is:
//  1. Validate operation target, qualification descriptor, and persistence verifier.
//  2. Allocate fresh client and verify active baseline state (active, matching role, no extra groups/roles).
//  3. Mark Mutated=true immediately before DisableClient.
//  4. Execute DisableClient (exactly once).
//  5. Read back client metadata to verify disabled status.
//  6. Verify role and ACLs via ensureRole(create=false).
//  7. Verify snapshot persistence via injected VerifyRevoked verifier.
//  8. Check context error.
//  9. Return VerifiedSuccess with exact evidence.
//
// Any failure after DisableClient sets Mutated=true and returns ExecutionRecoveryRequired
// without attempting further mutations or compensations.
func (e *SimplifiedDynSecExecutor) executeRevoke(ctx context.Context, o Operation) (res SimplifiedExecutionResult, err error) {
	res = SimplifiedExecutionResult{
		OperationID: o.OperationID,
		Outcome:     ExecutionUnchangedFailure,
	}

	if o.Action != ActionRevoke || !e.target(o) {
		return res, ErrInvalidInput
	}
	if e.cfg.Qualification.IsZero() || !e.cfg.Qualification.Valid() {
		return res, ErrLifecycleUnavailable
	}
	if e.cfg.VerifyRevoked == nil {
		return res, ErrLifecycleUnavailable
	}

	// Bound overall operation by configured timeout
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	if ctx.Err() != nil {
		return res, ErrVerificationFailed
	}

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

	// Inspect initial native baseline: must be present, active, matching role
	native, getErr := c.GetClient(ctx, o.GatewayID)
	if getErr != nil || !adapterClientMatches(native, o.GatewayID, role, false) {
		res.Outcome = ExecutionRecoveryRequired
		return res, ErrRecoveryRequired
	}
	if err = e.ensureRole(ctx, c, o.GatewayID, role, false); err != nil {
		res.Outcome = ExecutionRecoveryRequired
		return res, ErrRecoveryRequired
	}

	// Mark mutation uncertainty immediately before sending first mutating command
	if ctx.Err() != nil {
		return res, ErrVerificationFailed
	}
	res.Mutated = true

	// Clean up and mark recovery_required on any post-mutation failure.
	defer func() {
		if err != nil && res.Mutated {
			res.Outcome = ExecutionRecoveryRequired
		}
	}()

	// Execute DisableClient
	if err = c.DisableClient(ctx, o.GatewayID); err != nil {
		return res, ErrRecoveryRequired
	}

	// Native state checks after mutation: verify client is disabled
	nativeAfter, getAfterErr := c.GetClient(ctx, o.GatewayID)
	if getAfterErr != nil || !adapterClientMatches(nativeAfter, o.GatewayID, role, true) {
		err = ErrRecoveryRequired
		return res, err
	}
	if err = e.ensureRole(ctx, c, o.GatewayID, role, false); err != nil {
		err = ErrRecoveryRequired
		return res, err
	}
	res.Evidence.RAMApplied = true
	res.Evidence.NativeDisabledObserved = true

	// Verify persistence via injected verifier
	if err = e.cfg.VerifyRevoked(ctx, o.GatewayID); err != nil {
		err = ErrRecoveryRequired
		return res, err
	}
	res.Evidence.NativePersistedRoleACLObserved = true
	res.Evidence.Qualification = e.cfg.Qualification

	if ctx.Err() != nil {
		err = ErrRecoveryRequired
		return res, err
	}

	res.Outcome = ExecutionVerifiedSuccess
	return res, nil
}

// CheckRevoked verifies that the client is currently disabled with exact gateway role
// in Mosquitto DynSec, and validates that persisted snapshot state matches disabled target and ACLs.
// It is a read-only check for already-revoked idempotent replay.
//
// NOTE: This is a native read-only disabled-state and persistence check for historical replay ONLY.
// It is NOT active-session drain proof and does NOT guarantee quiescence, disconnection,
// or drain of existing TCP/MQTT sessions or in-flight subscriptions. Revoke session-containment
// qualification was established in Task 2.8a Q1.
func (e *SimplifiedDynSecExecutor) CheckRevoked(ctx context.Context, u string) error {
	if gateway.ValidateGatewayID(u) != nil || u == BackendUsername {
		return ErrInvalidInput
	}
	for _, protected := range e.cfg.ProtectedUsernames {
		if protected == u {
			return ErrInvalidInput
		}
	}

	if e.cfg.Qualification.IsZero() || !e.cfg.Qualification.Valid() {
		return ErrLifecycleUnavailable
	}
	if e.cfg.VerifyRevoked == nil {
		return ErrLifecycleUnavailable
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

	if err = e.cfg.VerifyRevoked(ctx, u); err != nil {
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
