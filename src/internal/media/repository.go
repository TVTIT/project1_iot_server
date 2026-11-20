// Package media owns PostgreSQL admission and authorized metadata lookup for
// backend-managed media intents. It deliberately performs no Storage I/O.
package media

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/mediaprotocol"
)

// MaxUploadBytes defines the maximum allowed size in bytes for a media upload.
const MaxUploadBytes int64 = mediaprotocol.MaxUploadBytes

// Common sentinel errors returned by media repository operations.
var (
	ErrInvalidRequest  = errors.New("invalid media admission request")
	ErrForbidden       = errors.New("media admission forbidden")
	ErrNotFound        = errors.New("media intent not found")
	ErrConflict        = errors.New("media upload metadata conflict")
	ErrQuota           = errors.New("media quota exceeded")
	ErrDependency      = errors.New("media repository dependency unavailable")
	ErrStaleOperation  = errors.New("stale media operation")
	ErrGrantUnresolved = errors.New("media grant may still be usable")
)

var uploadIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Admission contains the parameters required to request intent admission for an upload.
type Admission struct {
	GatewayID      string
	UploadID       string
	CapturedAt     time.Time
	ExpectedSize   int64
	DeclaredSHA256 string
}

// Record represents the committed database state for an admitted media object.
type Record struct {
	MediaID                uuid.UUID
	GatewayID              string
	UploadID               string
	StoragePath            string
	MediaType              string
	ExpectedSize           int64
	CapturedAt             time.Time
	DeclaredSHA256         string
	ValidationStatus       string
	TerminalReason         *string
	CreatedAt              time.Time
	IntentExpiresAt        time.Time
	VerificationGeneration int64
	VerificationAttempts   int
	LastVerificationError  string
}

// GrantStatus represents the state of a media upload grant lifecycle.
type GrantStatus string

// Grant status values representing the lifecycle and outcome of an upload request.
const (
	GrantNeedUpload           GrantStatus = "need_upload"
	GrantRetryAfterCompletion GrantStatus = "retry_after_completion"
	GrantPending              GrantStatus = "pending"
	GrantVerifying            GrantStatus = "verifying"
	GrantReady                GrantStatus = "ready"
	GrantRejected             GrantStatus = "rejected"
	GrantExpired              GrantStatus = "expired"
	GrantConflict             GrantStatus = "conflict"
)

// GrantRequest carries the bounded signing operation and the policy TTL/grace.
// ReservedUntil covers all three from reservation time, without storing a URL.
type GrantRequest struct {
	GatewayID, UploadID                     string
	MediaID                                 uuid.UUID
	SigningTimeout, GrantTTL, InFlightGrace time.Duration
}

// GrantReservation represents an in-flight upload grant lease held by a gateway.
type GrantReservation struct {
	GatewayID, UploadID string
	MediaID             uuid.UUID
	GrantGeneration     int64
	ReservedUntil       time.Time
	IntentExpiresAt     time.Time
}

// VerificationGeneration identifies a specific verification lease epoch for an upload.
type VerificationGeneration struct {
	GatewayID, UploadID string
	MediaID             uuid.UUID
	Generation          int64
}

// ClaimDueVerification leases due work. Candidate discovery takes no row locks;
// each claim then follows the ordinary Gateway -> credential -> media lock
// order used by Complete and ReserveGrant. The generation is the durable fence
// and next_verification_at is the lease deadline.
func (r *Repository) ClaimDueVerification(ctx context.Context, limit int, lease time.Duration) ([]Record, error) {
	if limit < 1 || limit > 64 || lease <= 0 || lease > 10*time.Minute {
		return nil, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT m.media_id,m.gateway_id,m.upload_id,m.storage_path,m.media_type,m.expected_size,
 m.captured_at,m.declared_sha256,m.validation_status,m.terminal_reason,m.created_at,m.intent_expires_at,m.verification_generation,
 m.verification_attempts,COALESCE(m.last_verification_error,'') FROM public.media_objects m
 JOIN public.gateway_mqtt_credentials c ON c.gateway_id=m.gateway_id AND c.status='active'
 WHERE m.managed AND m.validation_status='verifying' AND m.intent_expires_at>now()
 AND m.next_verification_at<=now()
 AND NOT EXISTS (
   SELECT 1 FROM public.gateway_mqtt_credential_events e
   WHERE e.gateway_id=m.gateway_id AND e.status IN ('pending','recovery_needed')
     AND e.recovery_id IS NULL AND public.mqtt_operation_effective(e.operation_id)
   UNION ALL SELECT 1 FROM public.mqtt_credential_maintenance x
   WHERE x.gateway_id=m.gateway_id AND x.status<>'completed' AND public.mqtt_operation_effective(x.operation_id)
   UNION ALL SELECT 1 FROM public.mqtt_credential_recovery y
   WHERE y.gateway_id=m.gateway_id AND y.status='pending' AND public.mqtt_operation_effective(y.operation_id)
 )
 ORDER BY m.next_verification_at,m.created_at,m.media_id LIMIT $1`, limit)
	if err != nil {
		return nil, safeDependencyError(err)
	}
	var candidates []Record
	for rows.Next() {
		rec, scanErr := scanRecord(rows)
		if scanErr != nil {
			rows.Close()
			return nil, safeDependencyError(scanErr)
		}
		candidates = append(candidates, rec)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, safeDependencyError(err)
	}
	claimed := make([]Record, 0, len(candidates))
	for _, rec := range candidates {
		tx, beginErr := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if beginErr != nil {
			return nil, safeDependencyError(beginErr)
		}
		if err = authorizeGateway(ctx, tx, rec.GatewayID); err != nil {
			rollback(tx, r.timeout)
			if errors.Is(err, ErrForbidden) {
				continue
			}
			return nil, err
		}
		var out Record
		out, err = scanRecord(tx.QueryRow(ctx, `UPDATE public.media_objects SET
 verification_generation=verification_generation+1,verification_attempts=verification_attempts+1,
 next_verification_at=now()+$2::interval
 WHERE media_id=$1 AND managed AND gateway_id=$3 AND upload_id=$4 AND validation_status='verifying'
 AND verification_generation=$5 AND intent_expires_at>now() AND next_verification_at<=now()
 RETURNING `+recordColumns, rec.MediaID, lease.String(), rec.GatewayID, rec.UploadID, rec.VerificationGeneration))
		if errors.Is(err, pgx.ErrNoRows) {
			rollback(tx, r.timeout)
			continue
		}
		if err != nil {
			rollback(tx, r.timeout)
			return nil, safeDependencyError(err)
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, safeDependencyError(err)
		}
		claimed = append(claimed, out)
	}
	return claimed, nil
}

// VerificationRetry durably reschedules a transient read failure, or rejects
// after the finite configured attempt budget. The claim generation is a fence.
func (r *Repository) VerificationRetry(ctx context.Context, claim VerificationGeneration, code string, maxAttempts int, delay time.Duration) (Record, error) {
	if gateway.ValidateGatewayID(claim.GatewayID) != nil || !uploadIDPattern.MatchString(claim.UploadID) ||
		claim.MediaID == uuid.Nil || claim.Generation < 1 || maxAttempts < 1 || maxAttempts > 20 ||
		delay <= 0 || delay > 24*time.Hour || (code != "STORAGE_UNAVAILABLE" && code != "STORAGE_READ_FAILED") {
		return Record{}, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, claim.GatewayID); err != nil {
		return Record{}, err
	}
	out, err := scanRecord(tx.QueryRow(ctx, `UPDATE public.media_objects SET
 last_verification_at=now(),last_verification_error=$1,
 validation_status=CASE WHEN verification_attempts >= $2 THEN 'rejected' ELSE 'verifying' END,
 terminal_reason=CASE WHEN verification_attempts >= $2 THEN 'verification_exhausted' ELSE terminal_reason END,
 next_verification_at=CASE WHEN verification_attempts >= $2 THEN NULL ELSE now()+$3::interval END,
 cleanup_scheduled_at=CASE WHEN verification_attempts >= $2 THEN GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)) ELSE cleanup_scheduled_at END,
 next_cleanup_at=CASE WHEN verification_attempts >= $2 THEN GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)) ELSE next_cleanup_at END
 WHERE managed AND media_id=$4 AND gateway_id=$5 AND upload_id=$6 AND verification_generation=$7
 AND validation_status='verifying' AND intent_expires_at>now() AND next_verification_at>now()
 RETURNING `+recordColumns,
		code, maxAttempts, delay.String(), claim.MediaID, claim.GatewayID, claim.UploadID, claim.Generation))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrStaleOperation
	}
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, safeDependencyError(err)
	}
	return out, nil
}

// FinishVerification commits verified evidence or a terminal content rejection
// under the current generation lease. It never returns success before COMMIT.
func (r *Repository) FinishVerification(ctx context.Context, claim VerificationGeneration, evidence *VerifiedEvidence, rejectCode string) (Record, error) {
	if claim.Generation < 1 || claim.MediaID == uuid.Nil ||
		(evidence == nil && rejectCode == "") || (evidence != nil && rejectCode != "") {
		return Record{}, ErrInvalidRequest
	}
	if evidence != nil && (evidence.ActualSize < 1 || evidence.ActualSize > MaxUploadBytes ||
		!digestPattern.MatchString(evidence.SHA256) || evidence.MediaType != "video/mp4") {
		return Record{}, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, claim.GatewayID); err != nil {
		return Record{}, err
	}
	var out Record
	if evidence != nil {
		out, err = scanRecord(tx.QueryRow(ctx, `UPDATE public.media_objects SET
 validation_status='ready',actual_size=$1,verified_sha256=$2,actual_media_type=$3,
 terminal_reason=NULL,last_verification_at=now(),last_verification_error=NULL,
 next_verification_at=NULL,next_reconcile_at=NULL
 WHERE managed AND media_id=$4 AND gateway_id=$5 AND upload_id=$6
 AND verification_generation=$7 AND validation_status='verifying'
 AND intent_expires_at>now() AND next_verification_at>now()
 AND expected_size=$1 AND declared_sha256=$2 AND media_type=$3
 RETURNING `+recordColumns, evidence.ActualSize, evidence.SHA256, evidence.MediaType,
			claim.MediaID, claim.GatewayID, claim.UploadID, claim.Generation))
	} else {
		if rejectCode != "SIZE_MISMATCH" && rejectCode != "HASH_MISMATCH" &&
			rejectCode != "CONTENT_TYPE_MISMATCH" && rejectCode != "INVALID_MP4" {
			return Record{}, ErrInvalidRequest
		}
		reason := map[string]string{"SIZE_MISMATCH": "size_mismatch", "HASH_MISMATCH": "hash_mismatch",
			"CONTENT_TYPE_MISMATCH": "content_type_mismatch", "INVALID_MP4": "invalid_media"}[rejectCode]
		out, err = scanRecord(tx.QueryRow(ctx, `UPDATE public.media_objects SET
 validation_status='rejected',terminal_reason=$1,last_verification_at=now(),last_verification_error=$2,
 next_verification_at=NULL,cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
 next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE managed AND media_id=$3 AND gateway_id=$4 AND upload_id=$5
 AND verification_generation=$6 AND validation_status='verifying'
 AND intent_expires_at>now() AND next_verification_at>now()
 RETURNING `+recordColumns, reason, rejectCode, claim.MediaID, claim.GatewayID, claim.UploadID, claim.Generation))
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrStaleOperation
	}
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, safeDependencyError(err)
	}
	return out, nil
}

// VerifiedEvidence contains inspected content properties from storage verification.
type VerifiedEvidence struct {
	ActualSize int64
	SHA256     string
	MediaType  string
}

// GeneratedStoragePath returns the canonical object storage filename for this record.
func (r Record) GeneratedStoragePath() string { return r.MediaID.String() + ".mp4" }

// Repository manages PostgreSQL persistence for media intents, grants, verification, and cleanup.
type Repository struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	limits  config.MediaUploadConfig
}

// CleanupClaim is a short-lived conditional claim. Attempt is the durable fence.
type CleanupClaim struct {
	MediaID     uuid.UUID
	Path        string
	Attempt     int
	FollowupDue bool
}

// ExpireDue terminalizes a small batch without consulting current credentials:
// expiry is system-owned cleanup and must continue after credential revocation.
func (r *Repository) ExpireDue(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 64 {
		return 0, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tag, err := r.pool.Exec(ctx, `WITH due AS (
 SELECT media_id FROM public.media_objects
 WHERE managed AND validation_status IN ('pending','verifying') AND intent_expires_at<=now()
 ORDER BY intent_expires_at,media_id LIMIT $1 FOR UPDATE SKIP LOCKED
)
UPDATE public.media_objects m SET validation_status='expired',terminal_reason='intent_expired',
 next_verification_at=NULL,next_reconcile_at=NULL,
 cleanup_scheduled_at=GREATEST(m.intent_expires_at,COALESCE(m.latest_upload_grant_expires_at,m.intent_expires_at)),
 next_cleanup_at=GREATEST(m.intent_expires_at,COALESCE(m.latest_upload_grant_expires_at,m.intent_expires_at))
FROM due WHERE m.media_id=due.media_id AND m.managed AND m.validation_status IN ('pending','verifying')
RETURNING m.media_id`, limit)
	if err != nil {
		return 0, safeDependencyError(err)
	}
	return int(tag.RowsAffected()), nil
}

// ClaimDueCleanup acquires a batch of expired or rejected media records due for object deletion.
func (r *Repository) ClaimDueCleanup(ctx context.Context, limit int, lease time.Duration) ([]CleanupClaim, error) {
	if limit < 1 || limit > 64 || lease <= 0 || lease > 10*time.Minute {
		return nil, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `WITH due AS (
 SELECT media_id FROM public.media_objects
 WHERE managed AND validation_status IN ('rejected','expired') AND cleanup_confirmed_at IS NULL
 AND next_cleanup_at<=now()
 ORDER BY next_cleanup_at,media_id LIMIT $1 FOR UPDATE SKIP LOCKED
)
UPDATE public.media_objects m SET cleanup_attempts=cleanup_attempts+1,
 next_cleanup_at=now()+$2::interval
FROM due WHERE m.media_id=due.media_id AND m.managed
 AND m.validation_status IN ('rejected','expired') AND m.cleanup_confirmed_at IS NULL
RETURNING m.media_id,m.storage_path,m.cleanup_attempts,(m.cleanup_first_deleted_at IS NOT NULL)`, limit, lease.String())
	if err != nil {
		return nil, safeDependencyError(err)
	}
	defer rows.Close()
	out := make([]CleanupClaim, 0, limit)
	for rows.Next() {
		var c CleanupClaim
		if err := rows.Scan(&c.MediaID, &c.Path, &c.Attempt, &c.FollowupDue); err != nil {
			return nil, safeDependencyError(err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, safeDependencyError(err)
	}
	return out, nil
}

// ScheduleCleanupRetry schedules a retry for a failed object cleanup attempt.
func (r *Repository) ScheduleCleanupRetry(ctx context.Context, c CleanupClaim, delay time.Duration, reason string) error {
	if c.MediaID == uuid.Nil || c.Attempt < 1 || delay <= 0 || delay > 24*time.Hour ||
		(reason != "STORAGE_UNAVAILABLE" && reason != "CLEANUP_EXHAUSTED" && reason != "FOLLOWUP_WINDOW") {
		return ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tag, err := r.pool.Exec(ctx, `UPDATE public.media_objects SET
 next_cleanup_at=CASE WHEN $2='CLEANUP_EXHAUSTED' THEN NULL ELSE now()+$1::interval END,last_cleanup_error=$2,
 cleanup_first_deleted_at=CASE WHEN $2='FOLLOWUP_WINDOW' THEN COALESCE(cleanup_first_deleted_at,now()) ELSE cleanup_first_deleted_at END
 WHERE media_id=$3 AND managed AND storage_path=$4 AND cleanup_attempts=$5
 AND validation_status IN ('rejected','expired') AND cleanup_confirmed_at IS NULL`,
		delay.String(), reason, c.MediaID, c.Path, c.Attempt)
	if err != nil {
		return safeDependencyError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleOperation
	}
	return nil
}

// ConfirmCleanup marks an object cleanup claim as permanently confirmed after verified absence.
func (r *Repository) ConfirmCleanup(ctx context.Context, c CleanupClaim) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tag, err := r.pool.Exec(ctx, `UPDATE public.media_objects SET cleanup_confirmed_at=now(),
 next_cleanup_at=NULL,last_cleanup_error=NULL
 WHERE media_id=$1 AND managed AND storage_path=$2 AND cleanup_attempts=$3
 AND validation_status IN ('rejected','expired') AND cleanup_confirmed_at IS NULL
 AND next_cleanup_at>now() AND cleanup_first_deleted_at IS NOT NULL AND cleanup_first_deleted_at<now()`, c.MediaID, c.Path, c.Attempt)
	if err != nil {
		return safeDependencyError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleOperation
	}
	return nil
}

// NewRepository creates and validates a new media repository with database connection pool and limits.
func NewRepository(pool *pgxpool.Pool, limits config.MediaUploadConfig, timeout time.Duration) (*Repository, error) {
	if pool == nil || timeout <= 0 || limits.MaxPendingPerGateway < 1 ||
		limits.MaxUnsettledIntents < 1 || limits.MaxPendingPerGateway > limits.MaxUnsettledIntents ||
		limits.TemporaryBytes < int64(limits.MaxUnsettledIntents)*MaxUploadBytes ||
		limits.StoredBytes < MaxUploadBytes || limits.IntentTTL <= 0 {
		return nil, ErrInvalidRequest
	}
	return &Repository{pool: pool, timeout: timeout, limits: limits}, nil
}

func validateAdmission(req Admission) (Admission, error) {
	if gateway.ValidateGatewayID(req.GatewayID) != nil || !uploadIDPattern.MatchString(req.UploadID) ||
		req.ExpectedSize < 1 || req.ExpectedSize > MaxUploadBytes ||
		!digestPattern.MatchString(req.DeclaredSHA256) || req.CapturedAt.IsZero() {
		return Admission{}, ErrInvalidRequest
	}
	req.CapturedAt = req.CapturedAt.UTC().Round(time.Microsecond)
	return req, nil
}

func safeDependencyError(error) error { return ErrDependency }

// Admit executes atomic admission of an upload intent within configured gateway and global quotas.
func (r *Repository) Admit(ctx context.Context, req Admission) (Record, error) {
	req, err := validateAdmission(req)
	if err != nil {
		return Record{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)

	// Global quota serialization precedes Gateway/credential locks so that
	// concurrent reservations on different Gateways observe one committed budget.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(3290, 82)`); err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = authorizeGateway(ctx, tx, req.GatewayID); err != nil {
		return Record{}, err
	}

	existing, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE managed AND gateway_id=$1 AND upload_id=$2`, req.GatewayID, req.UploadID))
	if err == nil {
		if existing.CapturedAt.Equal(req.CapturedAt) && existing.ExpectedSize == req.ExpectedSize && existing.DeclaredSHA256 == req.DeclaredSHA256 {
			if err = tx.Commit(ctx); err != nil {
				return Record{}, safeDependencyError(err)
			}
			return existing, nil
		}
		return Record{}, ErrConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Record{}, safeDependencyError(err)
	}

	var unsettled, pending int
	var storedBytes int64
	err = tx.QueryRow(ctx, `SELECT
 COUNT(*) FILTER (WHERE validation_status IN ('pending','verifying') OR
   (validation_status IN ('rejected','expired') AND cleanup_confirmed_at IS NULL)),
 COUNT(*) FILTER (WHERE gateway_id=$1 AND (validation_status IN ('pending','verifying') OR
   (validation_status IN ('rejected','expired') AND cleanup_confirmed_at IS NULL))),
 COALESCE(SUM(CASE WHEN validation_status='ready' THEN actual_size
   WHEN validation_status IN ('pending','verifying') THEN expected_size ELSE 0 END),0)
 FROM public.media_objects WHERE managed`, req.GatewayID).Scan(&unsettled, &pending, &storedBytes)
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if unsettled >= r.limits.MaxUnsettledIntents || int64(unsettled+1)*MaxUploadBytes > r.limits.TemporaryBytes ||
		pending >= r.limits.MaxPendingPerGateway || storedBytes > r.limits.StoredBytes-req.ExpectedSize {
		return Record{}, ErrQuota
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	id := uuid.New()
	expiry := now.Add(r.limits.IntentTTL)
	created, err := scanRecord(tx.QueryRow(ctx, `INSERT INTO public.media_objects
 (media_id,gateway_id,storage_path,media_type,expected_size,validation_status,created_at,
  managed,upload_id,captured_at,intent_expires_at,declared_sha256)
 VALUES ($1,$2,$3,'video/mp4',$4,'pending',$5,true,$6,$7,$8,$9)
 RETURNING `+recordColumns, id, req.GatewayID, id.String()+".mp4", req.ExpectedSize, now, req.UploadID, req.CapturedAt, expiry, req.DeclaredSHA256))
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, safeDependencyError(err)
	}
	return created, nil
}

// Lookup returns an intent only to the same currently credential-active Gateway.
func (r *Repository) Lookup(ctx context.Context, gatewayID, uploadID string) (Record, error) {
	if gateway.ValidateGatewayID(gatewayID) != nil || !uploadIDPattern.MatchString(uploadID) {
		return Record{}, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, gatewayID); err != nil {
		return Record{}, err
	}
	record, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE managed AND gateway_id=$1 AND upload_id=$2`, gatewayID, uploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, safeDependencyError(err)
	}
	return record, nil
}

func authorizeGateway(ctx context.Context, tx pgx.Tx, id string) error {
	var exists bool
	// Parent SHARE serializes with credential mutations that take the parent
	// UPDATE lock; then lock the credential row. Lock order: Gateway SHARE ->
	// credential row UPDATE -> effective operation/recovery read.
	if err := tx.QueryRow(ctx, `SELECT true FROM public.gateways WHERE gateway_id=$1 FOR SHARE`, id).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrForbidden
		}
		return safeDependencyError(err)
	}
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM public.gateway_mqtt_credentials WHERE gateway_id=$1 FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrForbidden
		}
		return safeDependencyError(err)
	}
	if status != "active" {
		return ErrForbidden
	}
	var blocked bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM public.gateway_mqtt_credential_events
 WHERE gateway_id=$1 AND status IN ('pending','recovery_needed') AND recovery_id IS NULL
   AND public.mqtt_operation_effective(operation_id)
 UNION ALL SELECT 1 FROM public.mqtt_credential_maintenance
 WHERE gateway_id=$1 AND status <> 'completed' AND public.mqtt_operation_effective(operation_id)
 UNION ALL SELECT 1 FROM public.mqtt_credential_recovery
 WHERE gateway_id=$1 AND status='pending' AND public.mqtt_operation_effective(operation_id)
)`, id).Scan(&blocked)
	if err != nil {
		return safeDependencyError(err)
	}
	if blocked {
		return ErrForbidden
	}
	return nil
}

const recordColumns = `media_id,gateway_id,upload_id,storage_path,media_type,expected_size,
 captured_at,declared_sha256,validation_status,terminal_reason,created_at,intent_expires_at,verification_generation,
 verification_attempts,COALESCE(last_verification_error,'')`
const recordSelect = `SELECT ` + recordColumns + ` FROM public.media_objects`

func scanRecord(row pgx.Row) (Record, error) {
	var out Record
	err := row.Scan(&out.MediaID, &out.GatewayID, &out.UploadID, &out.StoragePath, &out.MediaType,
		&out.ExpectedSize, &out.CapturedAt, &out.DeclaredSHA256, &out.ValidationStatus,
		&out.TerminalReason, &out.CreatedAt, &out.IntentExpiresAt, &out.VerificationGeneration,
		&out.VerificationAttempts, &out.LastVerificationError)
	if err != nil {
		return Record{}, err
	}
	out.CapturedAt = out.CapturedAt.UTC().Round(time.Microsecond)
	out.CreatedAt = out.CreatedAt.UTC().Truncate(time.Microsecond)
	out.IntentExpiresAt = out.IntentExpiresAt.UTC().Truncate(time.Microsecond)
	return out, nil
}

func rollback(tx pgx.Tx, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// ReserveGrant is the durable pre-signing fence. A non-nil next_reconcile_at
// means an earlier grant could still have been used; only a verifier's
// confirmed-missing transition may clear it.
func (r *Repository) ReserveGrant(ctx context.Context, req GrantRequest) (GrantReservation, GrantStatus, error) {
	if gateway.ValidateGatewayID(req.GatewayID) != nil || !uploadIDPattern.MatchString(req.UploadID) ||
		req.MediaID == uuid.Nil || req.SigningTimeout <= 0 || req.SigningTimeout > time.Hour ||
		req.GrantTTL <= 0 || req.GrantTTL > 24*time.Hour ||
		req.InFlightGrace < 0 || req.InFlightGrace > 24*time.Hour {
		return GrantReservation{}, "", ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return GrantReservation{}, "", safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, req.GatewayID); err != nil {
		return GrantReservation{}, "", err
	}
	rec, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE managed AND gateway_id=$1 AND upload_id=$2 FOR UPDATE`,
		req.GatewayID, req.UploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return GrantReservation{}, "", ErrNotFound
	}
	if err != nil {
		return GrantReservation{}, "", safeDependencyError(err)
	}
	if rec.MediaID != req.MediaID {
		return GrantReservation{}, GrantConflict, nil
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	switch rec.ValidationStatus {
	case "ready":
		return GrantReservation{}, GrantReady, nil
	case "rejected":
		return GrantReservation{}, GrantRejected, nil
	case "expired":
		return GrantReservation{}, GrantExpired, nil
	case "verifying":
		if !rec.IntentExpiresAt.After(now) {
			if _, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='expired',terminal_reason='intent_expired',
     next_verification_at=NULL,
     cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
     next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE media_id=$1 AND validation_status='verifying'`, rec.MediaID); err != nil {
				return GrantReservation{}, "", safeDependencyError(err)
			}
			if err = tx.Commit(ctx); err != nil {
				return GrantReservation{}, "", safeDependencyError(err)
			}
			return GrantReservation{}, GrantExpired, nil
		}
		return GrantReservation{}, GrantVerifying, nil
	case "pending":
	default:
		return GrantReservation{}, "", safeDependencyError(errors.New("unexpected managed media state"))
	}
	if !rec.IntentExpiresAt.After(now) {
		if _, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='expired',terminal_reason='intent_expired',
     cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
     next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE media_id=$1 AND validation_status='pending'`, rec.MediaID); err != nil {
			return GrantReservation{}, "", safeDependencyError(err)
		}
		if err = tx.Commit(ctx); err != nil {
			return GrantReservation{}, "", safeDependencyError(err)
		}
		return GrantReservation{}, GrantExpired, nil
	}
	unresolved, checkErr := recIntentHasPossiblePut(ctx, tx, rec.MediaID)
	if checkErr != nil {
		return GrantReservation{}, "", safeDependencyError(checkErr)
	}
	if unresolved {
		return GrantReservation{}, GrantRetryAfterCompletion, nil
	}
	reservedUntil := now.Add(req.SigningTimeout + req.GrantTTL + req.InFlightGrace).Truncate(time.Microsecond)
	if !rec.IntentExpiresAt.After(reservedUntil) {
		return GrantReservation{}, GrantPending, nil
	}
	var generation int64
	err = tx.QueryRow(ctx, `UPDATE public.media_objects
 SET upload_grant_generation=upload_grant_generation+1,
      latest_upload_grant_expires_at=GREATEST(COALESCE(latest_upload_grant_expires_at,created_at),$2),
      next_reconcile_at=$2
 WHERE media_id=$1 AND managed AND validation_status='pending' AND intent_expires_at>$2
 RETURNING upload_grant_generation`, rec.MediaID, reservedUntil).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return GrantReservation{}, GrantPending, nil
	}
	if err != nil {
		return GrantReservation{}, "", safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return GrantReservation{}, "", safeDependencyError(err)
	}
	return GrantReservation{GatewayID: req.GatewayID, UploadID: req.UploadID, MediaID: rec.MediaID,
		GrantGeneration: generation, ReservedUntil: reservedUntil, IntentExpiresAt: rec.IntentExpiresAt}, GrantNeedUpload, nil
}

func recIntentHasPossiblePut(ctx context.Context, tx pgx.Tx, id uuid.UUID) (bool, error) {
	var unresolved bool
	err := tx.QueryRow(ctx, `SELECT next_reconcile_at IS NOT NULL FROM public.media_objects WHERE media_id=$1`, id).Scan(&unresolved)
	return unresolved, err
}

// ConfirmGrant is called after signing and immediately before URL publication.
// It never removes the durable possible-PUT fence, including on stale results.
func (r *Repository) ConfirmGrant(ctx context.Context, reservation GrantReservation, actualExpiry time.Time) (bool, error) {
	if gateway.ValidateGatewayID(reservation.GatewayID) != nil || !uploadIDPattern.MatchString(reservation.UploadID) ||
		reservation.MediaID == uuid.Nil || reservation.GrantGeneration < 1 || actualExpiry.IsZero() {
		return false, ErrInvalidRequest
	}
	actualExpiry = actualExpiry.UTC().Truncate(time.Microsecond)
	if !actualExpiry.After(time.Now().UTC()) || actualExpiry.After(reservation.ReservedUntil) {
		return false, ErrStaleOperation
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, reservation.GatewayID); err != nil {
		return false, err
	}
	var valid bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM public.media_objects
 WHERE managed AND media_id=$1 AND gateway_id=$2 AND upload_id=$3
   AND validation_status='pending' AND intent_expires_at>now()
   AND upload_grant_generation=$4 AND next_reconcile_at=$5
 )`, reservation.MediaID, reservation.GatewayID, reservation.UploadID,
		reservation.GrantGeneration, reservation.ReservedUntil).Scan(&valid)
	if err != nil {
		return false, safeDependencyError(err)
	}
	if valid {
		_, err = tx.Exec(ctx, `UPDATE public.media_objects SET last_verification_error=NULL
 WHERE managed AND media_id=$1 AND gateway_id=$2 AND upload_id=$3
   AND validation_status='pending' AND upload_grant_generation=$4`,
			reservation.MediaID, reservation.GatewayID, reservation.UploadID, reservation.GrantGeneration)
		if err != nil {
			return false, safeDependencyError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, safeDependencyError(err)
	}
	return valid, nil
}

// Complete durably schedules verification once; repeated completion is a
// read/replay and does not move due time, attempts, or generation.
func (r *Repository) Complete(ctx context.Context, gatewayID, uploadID string) (Record, error) {
	if gateway.ValidateGatewayID(gatewayID) != nil || !uploadIDPattern.MatchString(uploadID) {
		return Record{}, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, gatewayID); err != nil {
		return Record{}, err
	}
	rec, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE managed AND gateway_id=$1 AND upload_id=$2 FOR UPDATE`, gatewayID, uploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	switch rec.ValidationStatus {
	case "pending":
		if !rec.IntentExpiresAt.After(now) {
			_, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='expired',terminal_reason='intent_expired',
     cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
     next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE media_id=$1 AND validation_status='pending'`, rec.MediaID)
		} else {
			startVerification := rec.LastVerificationError != "OBJECT_MISSING"
			if startVerification {
				_, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='verifying',next_verification_at=$2,last_verification_error=NULL,
      next_reconcile_at=GREATEST(COALESCE(next_reconcile_at,$2),$2)
 WHERE media_id=$1 AND validation_status='pending'`, rec.MediaID, now)
			}
		}
		if err != nil {
			return Record{}, safeDependencyError(err)
		}
	case "verifying":
		if !rec.IntentExpiresAt.After(now) {
			_, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='expired',terminal_reason='intent_expired',
     next_verification_at=NULL,
     cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
     next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE media_id=$1 AND validation_status='verifying'`, rec.MediaID)
			if err != nil {
				return Record{}, safeDependencyError(err)
			}
		}
	case "ready", "rejected", "expired":
	default:
		return Record{}, safeDependencyError(errors.New("unexpected managed media state"))
	}
	out, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE media_id=$1`, rec.MediaID))
	if err != nil {
		return Record{}, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, safeDependencyError(err)
	}
	return out, nil
}

// RecordMissing is the small verifier-facing state transition that releases the
// duplicate-grant fence only after a generation-matched missing-object result.
func (r *Repository) RecordMissing(ctx context.Context, claim VerificationGeneration) error {
	_, _, err := r.RecordMissingResult(ctx, claim)
	return err
}

// RecordMissingResult records the safe missing-object instruction only after
// the grant horizon. The returned row is the committed state for publication.
func (r *Repository) RecordMissingResult(ctx context.Context, claim VerificationGeneration) (Record, bool, error) {
	if gateway.ValidateGatewayID(claim.GatewayID) != nil || !uploadIDPattern.MatchString(claim.UploadID) ||
		claim.MediaID == uuid.Nil || claim.Generation < 1 {
		return Record{}, false, ErrInvalidRequest
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, false, safeDependencyError(err)
	}
	defer rollback(tx, r.timeout)
	if err = authorizeGateway(ctx, tx, claim.GatewayID); err != nil {
		return Record{}, false, err
	}
	rec, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE managed AND media_id=$1 AND gateway_id=$2 AND upload_id=$3 FOR UPDATE`,
		claim.MediaID, claim.GatewayID, claim.UploadID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, false, ErrNotFound
	}
	if err != nil {
		return Record{}, false, safeDependencyError(err)
	}
	// A lease may expire while the row is being rechecked; never report a
	// successful missing observation unless the fenced UPDATE changed a row.
	// Exec returns the update tag on either branch.
	if rec.ValidationStatus != "verifying" || rec.VerificationGeneration != claim.Generation {
		return Record{}, false, ErrStaleOperation
	}
	var reconcileAt *time.Time
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT next_reconcile_at,now() FROM public.media_objects
 WHERE media_id=$1 AND next_verification_at>now() AND intent_expires_at>now()`, rec.MediaID).Scan(&reconcileAt, &databaseNow); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Record{}, false, ErrStaleOperation
		}
		return Record{}, false, safeDependencyError(err)
	}
	now := databaseNow.UTC().Truncate(time.Microsecond)
	if reconcileAt != nil && reconcileAt.After(now) {
		_, err = tx.Exec(ctx, `UPDATE public.media_objects SET next_verification_at=$2,
 verification_attempts=GREATEST(verification_attempts-1,0)
 WHERE media_id=$1 AND verification_generation=$3 AND validation_status='verifying' AND next_verification_at>now()`,
			rec.MediaID, reconcileAt, claim.Generation)
		if err != nil {
			return Record{}, false, safeDependencyError(err)
		}
		out, scanErr := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE media_id=$1`, rec.MediaID))
		if scanErr != nil {
			return Record{}, false, safeDependencyError(scanErr)
		}
		if err = tx.Commit(ctx); err != nil {
			return Record{}, false, safeDependencyError(err)
		}
		return out, false, nil
	}
	var tag pgconn.CommandTag
	if !rec.IntentExpiresAt.After(now) {
		tag, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='expired',terminal_reason='intent_expired',
      next_reconcile_at=NULL,next_verification_at=NULL,
      last_verification_at=$2,last_verification_error='OBJECT_MISSING',
      cleanup_scheduled_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at)),
      next_cleanup_at=GREATEST(intent_expires_at,COALESCE(latest_upload_grant_expires_at,intent_expires_at))
 WHERE media_id=$1 AND verification_generation=$3 AND validation_status='verifying'
 AND next_verification_at>now()`, rec.MediaID, now, claim.Generation)
	} else {
		tag, err = tx.Exec(ctx, `UPDATE public.media_objects
 SET validation_status='pending',next_reconcile_at=NULL,next_verification_at=NULL,
      last_verification_at=$2,last_verification_error='OBJECT_MISSING'
 WHERE media_id=$1 AND verification_generation=$3 AND validation_status='verifying'
 AND intent_expires_at>now() AND next_verification_at>now()`, rec.MediaID, now, claim.Generation)
	}
	if err != nil {
		return Record{}, false, safeDependencyError(err)
	}
	if tag.RowsAffected() != 1 {
		return Record{}, false, ErrStaleOperation
	}
	out, err := scanRecord(tx.QueryRow(ctx, recordSelect+` WHERE media_id=$1`, rec.MediaID))
	if err != nil {
		return Record{}, false, safeDependencyError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Record{}, false, safeDependencyError(err)
	}
	return out, out.ValidationStatus == "pending" && out.LastVerificationError == "OBJECT_MISSING", nil
}
