package telemetryws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// DefaultAuthorizationBudget is the caller-side deadline budget for verifying target authorization.
const DefaultAuthorizationBudget = 2 * time.Second

// ErrTargetUnauthorized is the generic public rejection returned when a target is not found,
// the sensor does not belong to the gateway, or the user lacks viewer/operator/owner membership.
// It deliberately conflates absent entities and missing permissions to avoid leaking existence
// across namespaces.
var ErrTargetUnauthorized = errors.New("subscription target unauthorized")

// ErrInvalidUserID indicates an unauthenticated or nil user UUID.
var ErrInvalidUserID = errors.New("invalid user identity")

// TargetAuthorizerRepository checks membership access and entity existence for subscription targets.
// It accepts primitive types to avoid import cycles between gateway and telemetryws.
type TargetAuthorizerRepository interface {
	AuthorizeSubscriptionTarget(ctx context.Context, userID uuid.UUID, gatewayID string, sensorID string) (gateway.Role, error)
}

// AuthorizedTarget represents a successfully authorized subscription target with its membership role.
type AuthorizedTarget struct {
	Target TargetKey    `json:"target"`
	Role   gateway.Role `json:"role"`
}

// TargetAuthorizationError associates an authorization or database failure with a specific target.
type TargetAuthorizationError struct {
	Target TargetKey
	Err    error
}

func (e *TargetAuthorizationError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("target %s: %v", e.Target.String(), e.Err)
}

func (e *TargetAuthorizationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// TargetAuthorizer performs bounded, DB-backed authorization for subscription targets.
//
// CONCURRENCY & LIFECYCLE CONTRACT:
//  1. Invocation outside locks: Callers (e.g. WebSocket message loop or sweep worker) MUST invoke
//     TargetAuthorizer outside of any hub, session, or subscription mutex. TargetAuthorizer performs DB I/O
//     subject to a bounded budget (up to 2 seconds); holding locks during authorization would stall
//     the hub and event distribution.
//  2. Generation & state checks: Because authorization runs asynchronously and outside locks,
//     callers MUST verify session state and target generation before installing authorized filters.
//     If a session unsubscribed or changed generation while authorization was in-flight, the caller
//     must discard the stale authorization result.
//  3. No goroutine per target/event: AuthorizeTargets processes bounded target slices sequentially
//     on the calling goroutine. No goroutines are created per target or per telemetry event.
//  4. No DB per event/sample: Target authorization occurs only on explicit client subscribe or during
//     bounded periodic recheck sweeps, never during event dispatch or per-sample processing.
//  5. Fail closed: Any database failure, timeout, or context cancellation fails closed immediately
//     without granting subscription rights.
type TargetAuthorizer struct {
	repo   TargetAuthorizerRepository
	budget time.Duration
}

// NewTargetAuthorizer constructs a TargetAuthorizer with the given repository and budget.
// If budget <= 0, DefaultAuthorizationBudget (2s) is used.
func NewTargetAuthorizer(repo TargetAuthorizerRepository, budget time.Duration) (*TargetAuthorizer, error) {
	if repo == nil {
		return nil, fmt.Errorf("target authorizer repository is required")
	}
	if budget <= 0 {
		budget = DefaultAuthorizationBudget
	}
	return &TargetAuthorizer{
		repo:   repo,
		budget: budget,
	}, nil
}

// AuthorizeTarget verifies that userID has an authorized role (viewer, operator, or owner)
// for target, and for sensor targets, that the sensor exists and belongs to that gateway.
func (a *TargetAuthorizer) AuthorizeTarget(ctx context.Context, userID uuid.UUID, target TargetKey) (AuthorizedTarget, error) {
	if userID == uuid.Nil {
		return AuthorizedTarget{}, ErrInvalidUserID
	}
	if target.GatewayID == "" {
		return AuthorizedTarget{}, &TargetAuthorizationError{Target: target, Err: ErrTargetUnauthorized}
	}

	authCtx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()

	return a.authorizeTargetWithContext(authCtx, userID, target)
}

func (a *TargetAuthorizer) authorizeTargetWithContext(ctx context.Context, userID uuid.UUID, target TargetKey) (AuthorizedTarget, error) {
	if userID == uuid.Nil {
		return AuthorizedTarget{}, ErrInvalidUserID
	}
	if target.GatewayID == "" {
		return AuthorizedTarget{}, &TargetAuthorizationError{Target: target, Err: ErrTargetUnauthorized}
	}

	role, err := a.repo.AuthorizeSubscriptionTarget(ctx, userID, target.GatewayID, target.SensorID)
	if err != nil {
		if errors.Is(err, gateway.ErrNotFound) ||
			errors.Is(err, gateway.ErrInvalidGatewayID) ||
			errors.Is(err, gateway.ErrInvalidSensorID) ||
			errors.Is(err, gateway.ErrInvalidUser) {
			return AuthorizedTarget{}, &TargetAuthorizationError{Target: target, Err: ErrTargetUnauthorized}
		}
		// DB errors, context cancellations, timeouts fail closed with internal error
		return AuthorizedTarget{}, &TargetAuthorizationError{Target: target, Err: err}
	}

	switch role {
	case gateway.RoleOwner, gateway.RoleOperator, gateway.RoleViewer:
		return AuthorizedTarget{
			Target: target,
			Role:   role,
		}, nil
	default:
		return AuthorizedTarget{}, &TargetAuthorizationError{Target: target, Err: ErrTargetUnauthorized}
	}
}

// AuthorizeTargets verifies a bounded list of subscription targets (up to MaxTargetsPerMessage)
// under an overall authorization deadline budget. Deduplicates identical targets and executes
// checks sequentially on the calling goroutine without spawning goroutines.
func (a *TargetAuthorizer) AuthorizeTargets(ctx context.Context, userID uuid.UUID, targets []TargetKey) ([]AuthorizedTarget, error) {
	if userID == uuid.Nil {
		return nil, ErrInvalidUserID
	}
	if len(targets) == 0 {
		return nil, &ProtocolError{
			Code:    ErrorCodeInvalidTarget,
			Field:   "targets",
			Message: "targets list must not be empty",
		}
	}
	if len(targets) > MaxTargetsPerMessage {
		return nil, &ProtocolError{
			Code:    ErrorCodeTargetLimitExceeded,
			Field:   "targets",
			Message: "targets list exceeds maximum count",
		}
	}

	deduped := DeduplicateTargets(targets)

	authCtx, cancel := context.WithTimeout(ctx, a.budget)
	defer cancel()

	authorized := make([]AuthorizedTarget, 0, len(deduped))
	for _, target := range deduped {
		res, err := a.authorizeTargetWithContext(authCtx, userID, target)
		if err != nil {
			return nil, err
		}
		authorized = append(authorized, res)
	}

	return authorized, nil
}
