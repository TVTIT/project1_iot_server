package mqttcredential

import (
	"context"

	"github.com/google/uuid"
)

// BeginRequest contains only server identity and the validated transport input.
// OperationID is generated before intent for commit-ambiguity lookup. Generation,
// previous state and timestamps are allocated by the repository under row lock.
type BeginRequest struct {
	ActorUserID uuid.UUID
	OperationID uuid.UUID
	Input       MutationInput
}

// BeginResult permits execution only when Execute is true AND intent commit is
// confirmed. Replay/no-op returns safe metadata only; it never generates a secret.
// Pending/recovery replays return the domain conflict/unavailable codes instead.
type BeginResult struct {
	Operation Operation
	Metadata  Metadata
	Execute   bool
	Replay    *MutationResult
}

// OperationGuard is compare-and-set identity, not an authorization credential.
// Every write locks Gateway then metadata/event and matches LastOperationID,
// event status, and metadata status/version. Stale guards fail without any writes.
type OperationGuard struct {
	GatewayID                 string
	ExpectedOperationID       uuid.UUID
	ExpectedOperationStatus   OperationStatus
	ExpectedCredentialStatus  CredentialStatus
	ExpectedCredentialVersion int64
}

// OperationUpdate holds the safe atomic event + metadata proposal. No runtime
// calls or driver transaction types cross the repository boundary. Error codes
// are server-owned safe constants, never raw runtime/SQL messages.
type OperationUpdate struct {
	Guard      OperationGuard
	Completion Completion
}

// RevocationDecision is CURRENT authoritative desired revoke state, not an audit
// scan of all historical revoke events. An authorized reprovision supersedes this
// decision atomically while retaining immutable history. Unresolved revokes are
// included even before metadata reaches CredentialRevoked.
type RevocationDecision struct {
	GatewayID         string
	OperationID       uuid.UUID
	CredentialVersion int64
}

// Repository is the narrow contract for Task 2.6.3, not a SQL implementation.
// BeginOperation rechecks platform_admins (also on replay/no-op), locks the
// existing Gateway before metadata, enforces actor/key uniqueness and one
// unresolved job, allocates max(metadata, attempted history)+1 ONLY for new
// provision/rotate, and commits intent + current decision together. Revoke and
// revoked no-op never allocate a generation. Unknown Gateway creates no rows.
// Failed attempts remain in history; no audit or decision deletion is allowed.
// Reads used by the API require a fresh service/admin check, not JWT admin claims.
// Internal reconciliation reads are not public authorization bypasses.
// Writes below must enforce CompleteOperation's invariants and identity under
// lock, preserve Previous and immutable identity, and never edit terminal events.
// Each method is a short transaction; no transaction spans runtime network work.
type Repository interface {
	IsPlatformAdmin(context.Context, uuid.UUID) (bool, error)
	BeginOperation(context.Context, BeginRequest) (BeginResult, error)
	ConditionalFinalize(context.Context, OperationUpdate) (Metadata, error)
	FailKnown(context.Context, OperationUpdate) (Metadata, error)
	RequireRecovery(context.Context, OperationUpdate) (Metadata, error)
	GetMetadata(context.Context, string) (Metadata, error)
	// ResolveCommitAmbiguity must query on a new connection by server-generated
	// operation identity. An absent row/error is not permission to rerun runtime;
	// service uses a bounded policy and keeps execution blocked while uncertain.
	ResolveCommitAmbiguity(context.Context, uuid.UUID) (Operation, error)
	// Scans are bounded, cursor is exclusive; empty results are nonnil. Deleted
	// actor rows remain visible to recovery, but never become replayable callers.
	ListUnresolved(context.Context, uuid.UUID, int) ([]Operation, error)
	ListDurableRevocations(context.Context, string, int) ([]RevocationDecision, error)
}
