package mqttcredential

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrLegacyContractBlocked is returned when an unresolved operation has a non-simplified contract (e.g. lifecycle).
	ErrLegacyContractBlocked = errors.New("unresolved non-simplified credential operation blocks simplified startup classification")
	// ErrScanBudgetExhausted is returned when the scan hits max pages before completing the cursor iteration.
	ErrScanBudgetExhausted = errors.New("startup unresolved scan budget exhausted")
	// ErrMalformedPage is returned when operations in a page are out of order or contain invalid/repeated cursors.
	ErrMalformedPage = errors.New("startup unresolved scan encountered malformed page order or repeated cursor")
	// ErrTargetAmbiguity is returned when current metadata or resolved operation does not match the scanned intent.
	ErrTargetAmbiguity = errors.New("startup unresolved scan encountered target state ambiguity or mismatch")
)

// ClassificationStatus distinguishes completed classification from incomplete/unavailable.
type ClassificationStatus string

// ClassificationStatus values indicate whether startup classification completed or halted.
const (
	// ClassificationCompleted indicates all unresolved operations were scanned and classified.
	ClassificationCompleted ClassificationStatus = "completed"
	// ClassificationIncomplete indicates classification halted before all operations could be classified.
	ClassificationIncomplete ClassificationStatus = "incomplete"
)

// SimplifiedStartupRepository is the injected narrow repository interface for the classifier.
type SimplifiedStartupRepository interface {
	HasUnfinishedLifecycleAuthority(ctx context.Context) (bool, error)
	ListUnresolved(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error)
	GetMetadata(ctx context.Context, gatewayID string) (Metadata, error)
	RequireRecovery(ctx context.Context, u OperationUpdate) (Metadata, error)
	ResolveCommitAmbiguity(ctx context.Context, opID uuid.UUID) (Operation, error)
}

// SimplifiedStartupClassifierOptions bounds the startup classifier scan.
type SimplifiedStartupClassifierOptions struct {
	Timeout  time.Duration
	PageSize int
	MaxPages int
}

// RecoveryTarget identifies an unresolved operation that requires recovery.
type RecoveryTarget struct {
	GatewayID         string
	OperationID       uuid.UUID
	CredentialVersion int64
	Action            Action
}

// ClassificationResult reports the outcome of the classification scan.
type ClassificationResult struct {
	Status                  ClassificationStatus
	RecoveryRequiredTargets []RecoveryTarget
}

// SimplifiedStartupClassifier scans unresolved operations at startup and marks
// pending simplified intents as recovery-needed via authoritative guards.
type SimplifiedStartupClassifier struct {
	repo SimplifiedStartupRepository
	opts SimplifiedStartupClassifierOptions
}

// NewSimplifiedStartupClassifier validates dependencies and options and returns a classifier.
func NewSimplifiedStartupClassifier(r SimplifiedStartupRepository, opts SimplifiedStartupClassifierOptions) (*SimplifiedStartupClassifier, error) {
	if r == nil || dynSecNil(r) {
		return nil, ErrInvalidInput
	}
	if opts.Timeout <= 0 || opts.Timeout > 10*time.Minute {
		return nil, ErrInvalidInput
	}
	if opts.PageSize < 1 || opts.PageSize > 1024 {
		return nil, ErrInvalidInput
	}
	if opts.MaxPages < 1 || opts.MaxPages > 1024 {
		return nil, ErrInvalidInput
	}
	return &SimplifiedStartupClassifier{
		repo: r,
		opts: opts,
	}, nil
}

// Classify scans unresolved operations and returns the classification result.
func (c *SimplifiedStartupClassifier) Classify(ctx context.Context) (ClassificationResult, error) {
	fail := func(err error) (ClassificationResult, error) {
		return ClassificationResult{
			Status: ClassificationIncomplete,
		}, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	if ctx.Err() != nil {
		return fail(ctx.Err())
	}

	hasUnfinished, err := c.repo.HasUnfinishedLifecycleAuthority(ctx)
	if err != nil {
		return fail(err)
	}
	if hasUnfinished {
		return fail(ErrLegacyContractBlocked)
	}

	var targets []RecoveryTarget
	cursor := uuid.Nil
	complete := false

	for page := 0; page < c.opts.MaxPages; page++ {
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}

		rows, err := c.repo.ListUnresolved(ctx, cursor, c.opts.PageSize)
		if err != nil {
			return fail(err)
		}
		if len(rows) > c.opts.PageSize {
			return fail(ErrMalformedPage)
		}

		for _, o := range rows {
			if ctx.Err() != nil {
				return fail(ctx.Err())
			}

			// Validate monotonic cursor order and non-nil operation ID
			if o.OperationID == uuid.Nil || o.OperationID.String() <= cursor.String() {
				return fail(ErrMalformedPage)
			}
			cursor = o.OperationID

			// Lifecycle / unfinished non-simplified operations MUST NOT be converted or skipped.
			if o.ExecutionContract != ContractSimplified {
				return fail(ErrLegacyContractBlocked)
			}

			// Validate historical baseline
			if o.ValidateHistorical() != nil {
				return fail(ErrTargetAmbiguity)
			}

			// If already OperationRecoveryNeeded, record target without mutating
			if o.Status == OperationRecoveryNeeded {
				// Read current metadata and resolved operation
				meta, metaErr := c.repo.GetMetadata(ctx, o.GatewayID)
				if metaErr != nil {
					return fail(metaErr)
				}
				currentOp, opErr := c.repo.ResolveCommitAmbiguity(ctx, o.OperationID)
				if opErr != nil {
					return fail(opErr)
				}

				// Validate metadata validity and consistency
				if !validSimplifiedMetadata(meta, o.GatewayID) ||
					meta.GatewayID != o.GatewayID ||
					meta.Username != o.GatewayID ||
					meta.LastOperationID != o.OperationID ||
					meta.Status != CredentialRecoveryNeeded ||
					meta.OperationStatus != OperationRecoveryNeeded {
					return fail(ErrTargetAmbiguity)
				}

				// Validate resolved operation identity, contract, status, action, and version
				if currentOp.OperationID != o.OperationID ||
					currentOp.GatewayID != o.GatewayID ||
					currentOp.ActorUserID != o.ActorUserID ||
					currentOp.IdempotencyKey != o.IdempotencyKey ||
					currentOp.ExecutionContract != ContractSimplified ||
					currentOp.Status != OperationRecoveryNeeded ||
					currentOp.Action != o.Action ||
					currentOp.CredentialVersion != o.CredentialVersion ||
					currentOp.ValidateHistorical() != nil {
					return fail(ErrTargetAmbiguity)
				}

				// Consistency check using CheckResetEligibility
				if err := CheckResetEligibility(meta, currentOp, ResetReasonRecoveryAfterUnknown); err != nil {
					return fail(ErrTargetAmbiguity)
				}

				targets = append(targets, RecoveryTarget{
					GatewayID:         o.GatewayID,
					OperationID:       o.OperationID,
					CredentialVersion: o.CredentialVersion,
					Action:            o.Action,
				})
				continue
			}

			// Must be pending operation
			if o.Status != OperationPending {
				return fail(ErrTargetAmbiguity)
			}

			// Read exact current metadata and ambiguity-resolved operation
			meta, metaErr := c.repo.GetMetadata(ctx, o.GatewayID)
			if metaErr != nil {
				return fail(metaErr)
			}
			currentOp, opErr := c.repo.ResolveCommitAmbiguity(ctx, o.OperationID)
			if opErr != nil {
				return fail(opErr)
			}

			// Validate current metadata matches this pending simplified operation
			if meta.GatewayID != o.GatewayID || meta.LastOperationID != o.OperationID || meta.CredentialVersion != o.CredentialVersion || meta.OperationStatus != OperationPending {
				return fail(ErrTargetAmbiguity)
			}
			// Verify resolved operation contract, status, and identities match scanned intent
			if currentOp.OperationID != o.OperationID || currentOp.GatewayID != o.GatewayID || currentOp.ActorUserID != o.ActorUserID || currentOp.IdempotencyKey != o.IdempotencyKey || currentOp.ExecutionContract != ContractSimplified || currentOp.Status != OperationPending || currentOp.Action != o.Action || currentOp.CredentialVersion != o.CredentialVersion {
				return fail(ErrTargetAmbiguity)
			}

			// Expected credential status for pending operation
			var expectedStatus CredentialStatus
			switch currentOp.Action {
			case ActionProvision:
				expectedStatus = CredentialProvisioning
			case ActionRotate, ActionReset:
				expectedStatus = CredentialRotating
			case ActionRevoke:
				expectedStatus = CredentialRevoking
			default:
				return fail(ErrTargetAmbiguity)
			}
			if meta.Status != expectedStatus {
				return fail(ErrTargetAmbiguity)
			}

			// Validate metadata validity and username match
			if !validSimplifiedMetadata(meta, o.GatewayID) ||
				meta.Username != o.GatewayID {
				return fail(ErrTargetAmbiguity)
			}

			// Validate full resolved operation
			if currentOp.ValidateHistorical() != nil {
				return fail(ErrTargetAmbiguity)
			}

			// Build OperationGuard
			guard := operationGuard(currentOp, meta)

			// Mark recovery-required using real CompleteOperation preserving existing evidence
			now := time.Now().UTC()
			if now.Before(currentOp.UpdatedAt) {
				now = currentOp.UpdatedAt
			}
			completion, compErr := CompleteOperation(currentOp, ExecutionRecoveryRequired, currentOp.Evidence, now)
			if compErr != nil {
				return fail(compErr)
			}

			// Authoritative guarded write
			returnedMeta, recErr := c.repo.RequireRecovery(ctx, OperationUpdate{
				Guard:      guard,
				Completion: completion,
			})
			if recErr != nil {
				return fail(recErr)
			}

			// Verify returned metadata matches proposed Completion credential/status/version/current operation
			if !validSimplifiedMetadata(returnedMeta, o.GatewayID) ||
				returnedMeta.GatewayID != o.GatewayID ||
				returnedMeta.Username != o.GatewayID ||
				returnedMeta.LastOperationID != o.OperationID ||
				returnedMeta.CredentialVersion != completion.Credential.CredentialVersion ||
				returnedMeta.Status != CredentialRecoveryNeeded ||
				returnedMeta.OperationStatus != OperationRecoveryNeeded ||
				!reflect.DeepEqual(canonicalSnapshot(CredentialSnapshot{
					Status:            returnedMeta.Status,
					CredentialVersion: returnedMeta.CredentialVersion,
					ActivatedAt:       returnedMeta.ActivatedAt,
					RevokedAt:         returnedMeta.RevokedAt,
				}), canonicalSnapshot(completion.Credential)) {
				return fail(ErrTargetAmbiguity)
			}

			// Read back exact completed operation and validate sameCompletion
			readbackOp, readbackErr := c.repo.ResolveCommitAmbiguity(ctx, o.OperationID)
			if readbackErr != nil {
				return fail(readbackErr)
			}
			if !sameCompletion(Completion{Operation: readbackOp, Credential: completion.Credential}, completion) {
				return fail(ErrTargetAmbiguity)
			}

			targets = append(targets, RecoveryTarget{
				GatewayID:         o.GatewayID,
				OperationID:       o.OperationID,
				CredentialVersion: o.CredentialVersion,
				Action:            o.Action,
			})
		}

		if len(rows) < c.opts.PageSize {
			complete = true
			break
		}
	}

	if !complete {
		return fail(ErrScanBudgetExhausted)
	}

	return ClassificationResult{
		Status:                  ClassificationCompleted,
		RecoveryRequiredTargets: targets,
	}, nil
}
