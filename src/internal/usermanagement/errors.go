package usermanagement

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var (
	// ErrMembershipUnavailable means removal could not establish authorization or reach its database.
	ErrMembershipUnavailable = errors.New("membership service unavailable")

	// ErrInvalidInput indicates validation failed on request fields.
	ErrInvalidInput = errors.New("invalid input")

	// ErrSelfDemotionForbidden indicates an actor attempted to revoke their own platform_admin role.
	ErrSelfDemotionForbidden = errors.New("self admin demotion forbidden")

	// ErrOwnerRoleNotAllowed indicates the caller attempted to assign owner via the general role endpoint.
	ErrOwnerRoleNotAllowed = errors.New("owner role requires owner transfer endpoint")

	// ErrInvalidRole indicates an unsupported role was supplied.
	ErrInvalidRole = errors.New("invalid role")

	// ErrUserNotFound indicates the target user does not exist.
	ErrUserNotFound = errors.New("user not found")

	// ErrGatewayNotFound indicates the target gateway does not exist.
	ErrGatewayNotFound = errors.New("gateway not found")

	// ErrUserAlreadyExists indicates an account with the given email already exists.
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrOwnerTransferRequired indicates the target is already an owner and cannot be reassigned via assign role.
	ErrOwnerTransferRequired = errors.New("owner transfer required")

	// ErrGatewayOwnerInconsistent indicates the gateway has zero or multiple owners in database.
	ErrGatewayOwnerInconsistent = errors.New("gateway owner inconsistent")

	// ErrCapacityExceeded indicates user list exceeded configured limit.
	ErrCapacityExceeded = errors.New("user list capacity exceeded")

	// ErrUserListCapacityExceeded is an alias for ErrCapacityExceeded.
	ErrUserListCapacityExceeded = ErrCapacityExceeded

	// ErrAuthUnavailable indicates the upstream Auth service could not be reached or returned an unexpected server error.
	ErrAuthUnavailable = errors.New("auth service unavailable")

	// ErrAuthInvalidResponse indicates the upstream Auth service returned an invalid or malformed response.
	ErrAuthInvalidResponse = errors.New("invalid auth response")

	// ErrProfileVerificationFailed indicates post-creation verification of profile or state failed.
	ErrProfileVerificationFailed = errors.New("profile verification failed")
)

// ProfileVerificationError indicates that the user was created in Auth, but
// post-creation database profile or state verification failed.
// It preserves the created TargetID so callers or operators can reconcile the
// account without leaking secrets or attempting unsafe rollback.
type ProfileVerificationError struct {
	TargetID uuid.UUID
	Cause    error
}

func (e *ProfileVerificationError) Error() string {
	return fmt.Sprintf("profile verification failed for created user %s: %v", e.TargetID, e.Cause)
}

func (e *ProfileVerificationError) Unwrap() error {
	return e.Cause
}

// Is reports whether this error matches target (matching ErrProfileVerificationFailed).
func (e *ProfileVerificationError) Is(target error) bool {
	return target == ErrProfileVerificationFailed
}
