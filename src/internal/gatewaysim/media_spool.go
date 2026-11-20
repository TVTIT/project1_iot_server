package gatewaysim

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"iot-platform/internal/gateway"
	"iot-platform/internal/mediaprotocol"
)

const mediaSpoolMaxUploadBytes = mediaprotocol.MaxUploadBytes

// Common errors returned by MediaSpool operations.
var (
	ErrMediaSpoolFull     = errors.New("gatewaysim: media spool capacity reached")
	ErrMediaSpoolConflict = errors.New("gatewaysim: media upload identity conflict")
	ErrMediaSpoolRecord   = errors.New("gatewaysim: invalid media spool record")
	ErrMediaSpoolBusy     = errors.New("gatewaysim: media spool is locked")
	ErrMediaSpoolAttempts = errors.New("gatewaysim: media upload attempt limit reached")
)

var mediaUploadIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var mediaIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var safeMediaReason = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

// MediaSpoolConfig binds one active simulator to a private directory and a
// durable SQLite database. Failed/terminal rows remain charged against quota
// until an explicit operator recovery policy is added.
type MediaSpoolConfig struct {
	DBPath      string
	Dir         string
	GatewayID   string
	MaxFiles    int
	MaxBytes    int64
	MaxAttempts int
	OpTimeout   time.Duration
	IntentTTL   time.Duration
}

// MediaUpload is a local durable intent. The spool path is derived from its
// validated ID, never accepted from a caller or MQTT message.
type MediaUpload struct {
	UploadID      string
	Path          string
	SizeBytes     int64
	SHA256        string
	CapturedAt    time.Time
	SourcePath    string
	SourceReceipt string
	Phase         string
	MediaID       string
	Attempts      int
	NextAttempt   time.Time
	Deadline      time.Time
	GrantExpires  time.Time
	Reason        string
	Outcome       string
}

// MediaSpool manages durable on-disk spooled media clips and tracks their admission and delivery lifecycle.
type MediaSpool struct {
	db          *sql.DB
	dir         string
	maxFiles    int
	maxBytes    int64
	maxAttempts int
	opTimeout   time.Duration
	intentTTL   time.Duration
	gatewayID   string
	release     func() error
}

type mediaContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r mediaContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// OpenMediaSpool reuses the outbox's SQLite durability and exclusive-process
// lock conventions while maintaining independent media tables in that DB file.
func OpenMediaSpool(cfg MediaSpoolConfig) (*MediaSpool, error) {
	if !filepath.IsAbs(cfg.DBPath) || filepath.Clean(cfg.DBPath) != cfg.DBPath ||
		!filepath.IsAbs(cfg.Dir) || filepath.Clean(cfg.Dir) != cfg.Dir ||
		cfg.MaxFiles <= 0 || cfg.MaxBytes < 1 || gateway.ValidateGatewayID(cfg.GatewayID) != nil ||
		cfg.MaxAttempts <= 0 || cfg.OpTimeout <= 0 {
		return nil, ErrMediaSpoolRecord
	}
	if cfg.IntentTTL <= 0 {
		cfg.IntentTTL = 24 * time.Hour
	}
	info, err := os.Lstat(cfg.Dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%w: spool directory must be an existing private directory", ErrMediaSpoolRecord)
	}
	release, err := lockOutbox(cfg.DBPath)
	if err != nil {
		if errors.Is(err, ErrOutboxBusy) {
			return nil, ErrMediaSpoolBusy
		}
		return nil, err
	}
	db, err := openOutboxDB(OutboxConfig{Path: cfg.DBPath, OpTimeout: cfg.OpTimeout})
	if err != nil {
		_ = release()
		return nil, err
	}
	s := &MediaSpool{db: db, dir: cfg.Dir, gatewayID: cfg.GatewayID, maxFiles: cfg.MaxFiles, maxBytes: cfg.MaxBytes, maxAttempts: cfg.MaxAttempts, opTimeout: cfg.OpTimeout, intentTTL: cfg.IntentTTL, release: release}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
	defer cancel()
	if err := s.prepare(ctx); err != nil {
		_ = db.Close()
		_ = release()
		return nil, err
	}
	return s, nil
}

func (s *MediaSpool) prepare(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS media_spool_meta (id INTEGER PRIMARY KEY CHECK(id=1), gateway_id TEXT NOT NULL)`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO media_spool_meta(id,gateway_id) VALUES(1,?)", s.gatewayID); err != nil {
		return err
	}
	var bound string
	if err := s.db.QueryRowContext(ctx, "SELECT gateway_id FROM media_spool_meta WHERE id=1").Scan(&bound); err != nil {
		return err
	}
	if bound != s.gatewayID {
		return ErrOutboxIdentity
	}
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS media_spool (
		upload_id TEXT PRIMARY KEY, spool_name TEXT NOT NULL UNIQUE, size_bytes INTEGER NOT NULL,
		sha256 TEXT NOT NULL, captured_at INTEGER NOT NULL, source_path TEXT NOT NULL,
		source_receipt TEXT NOT NULL UNIQUE, phase TEXT NOT NULL, media_id TEXT NOT NULL DEFAULT '',
		attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL, deadline INTEGER NOT NULL DEFAULT 0,
		grant_expires_at INTEGER NOT NULL DEFAULT 0,
		reason TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL,
		CHECK(size_bytes BETWEEN 1 AND 80000000),
		CHECK(phase IN ('awaiting_grant','granted','put_may_have_happened','awaiting_result','ready','failed')))`)
	if err != nil {
		return err
	}
	if err := ensureMediaColumn(ctx, s.db, "grant_expires_at", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE media_spool SET deadline=created_at+? WHERE deadline=0", int64(s.ttlDuration()/time.Microsecond)); err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT upload_id,spool_name FROM media_spool WHERE phase='ready'")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		if !mediaUploadIDPattern.MatchString(id) || name != id+".mp4" {
			continue // unknown/corrupt names are never removed automatically
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return syncDirectory(s.dir)
}

func ensureMediaColumn(ctx context.Context, db *sql.DB, name, definition string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(media_spool)")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	found := false
	for rows.Next() {
		var cid int
		var column, kind string
		var notNull, primary int
		var defaultValue any
		if err := rows.Scan(&cid, &column, &kind, &notNull, &defaultValue, &primary); err != nil {
			return err
		}
		found = found || column == name
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !found {
		_, err = db.ExecContext(ctx, "ALTER TABLE media_spool ADD COLUMN "+name+" "+definition)
	}
	return err
}

func (s *MediaSpool) ttlDuration() time.Duration {
	if s.intentTTL > 0 {
		return s.intentTTL
	}
	return 24 * time.Hour
}

// IngestFinalized copies an explicitly nominated finalized regular file without
// following symlinks. Its source receipt (clean path, length, mtime) makes a
// repeated configured-file scan idempotent after restart. Replacing the file
// under the same upload ID conflicts instead of silently creating new bytes.
func (s *MediaSpool) IngestFinalized(ctx context.Context, uploadID, source string, capturedAt time.Time) (MediaUpload, error) {
	if !mediaUploadIDPattern.MatchString(uploadID) || !filepath.IsAbs(source) || filepath.Clean(source) != source || capturedAt.IsZero() {
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	in, err := openFinalizedInput(source)
	if err != nil {
		return MediaUpload{}, err
	}
	defer func() {
		_ = in.Close()
	}()
	info, err := in.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > mediaSpoolMaxUploadBytes {
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	receipt := sourceReceipt(source, info)
	var receiptID string
	err = s.db.QueryRowContext(ctx, "SELECT upload_id FROM media_spool WHERE source_receipt=?", receipt).Scan(&receiptID)
	if err == nil {
		existing, getErr := s.Get(ctx, receiptID)
		if getErr != nil {
			return MediaUpload{}, getErr
		}
		if existing.SourcePath != source {
			return MediaUpload{}, ErrMediaSpoolConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MediaUpload{}, err
	}
	if existing, e := s.Get(ctx, uploadID); e == nil {
		if existing.SourceReceipt != receipt || existing.SourcePath != source {
			return MediaUpload{}, ErrMediaSpoolConflict
		}
		return existing, nil
	} else if !errors.Is(e, sql.ErrNoRows) {
		return MediaUpload{}, e
	}
	tmp, err := s.createTemp()
	if err != nil {
		return MediaUpload{}, err
	}
	tempName := tmp.Name()
	defer func() {
		if err := os.Remove(tempName); err != nil && !os.IsNotExist(err) {
			slog.Warn("gatewaysim: cleanup temp spool file failed")
		}
	}()
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(mediaContextReader{ctx: ctx, r: in}, mediaSpoolMaxUploadBytes+1))
	if copyErr != nil || n != info.Size() || n < 1 || n > mediaSpoolMaxUploadBytes {
		_ = tmp.Close()
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return MediaUpload{}, err
	}
	if err := tmp.Close(); err != nil {
		return MediaUpload{}, err
	}
	// Confirm the source descriptor still names the same finalized file.
	after, err := in.Stat()
	if err != nil || !os.SameFile(info, after) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	name := uploadID + ".mp4"
	path := filepath.Join(s.dir, name)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MediaUpload{}, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			slog.Warn("gatewaysim: media spool transaction rollback failed")
		}
	}()
	var count, used int64
	if err := tx.QueryRowContext(ctx, "SELECT count(*), COALESCE(sum(size_bytes),0) FROM media_spool WHERE phase!='ready'").Scan(&count, &used); err != nil {
		return MediaUpload{}, err
	}
	if count >= int64(s.maxFiles) || used+n > s.maxBytes {
		return MediaUpload{}, ErrMediaSpoolFull
	}
	// Link is exclusive: never overwrite an existing file, including an
	// unrecognized orphan left by a crash or a prior ambiguous commit.
	if err := os.Link(tempName, path); err != nil {
		return MediaUpload{}, err
	}
	if err := syncDirectory(s.dir); err != nil {
		return MediaUpload{}, err
	}
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `INSERT INTO media_spool(upload_id,spool_name,size_bytes,sha256,captured_at,source_path,source_receipt,phase,next_attempt,deadline,created_at)
		VALUES(?,?,?,?,?,?,?,'awaiting_grant',?,?,?)`, uploadID, name, n, hex.EncodeToString(hash.Sum(nil)), capturedAt.UTC().UnixMicro(), source, receipt, now.UnixMicro(), now.Add(s.ttlDuration()).UnixMicro(), now.UnixMicro())
	if err != nil {
		return MediaUpload{}, err
	}
	if err := tx.Commit(); err != nil {
		return MediaUpload{}, err
	}
	return s.Get(ctx, uploadID)
}

func openFinalizedInput(path string) (*os.File, error) {
	for current := path; ; current = filepath.Dir(current) {
		component, err := os.Lstat(current)
		if err != nil || component.Mode()&os.ModeSymlink != 0 {
			return nil, ErrMediaSpoolRecord
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrMediaSpoolRecord
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscallNoFollow, 0)
	if err != nil {
		return nil, ErrMediaSpoolRecord
	}
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, ErrMediaSpoolRecord
	}
	return f, nil
}

func (s *MediaSpool) createTemp() (*os.File, error) {
	for i := 0; i < 5; i++ {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		path := filepath.Join(s.dir, ".media-"+hex.EncodeToString(b[:])+".tmp")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			return f, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
	}
	return nil, ErrMediaSpoolRecord
}

func sourceReceipt(path string, info os.FileInfo) string {
	value := fmt.Sprintf("%s\x00%d\x00%d", path, info.Size(), info.ModTime().UnixNano())
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// Get retrieves a persisted media upload record by its upload ID.
func (s *MediaSpool) Get(ctx context.Context, uploadID string) (MediaUpload, error) {
	if !mediaUploadIDPattern.MatchString(uploadID) {
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return scanMediaUpload(s.db.QueryRowContext(ctx, mediaSpoolSelect+" WHERE upload_id=?", uploadID), s.dir)
}

const mediaSpoolSelect = `SELECT upload_id,spool_name,size_bytes,sha256,captured_at,source_path,source_receipt,phase,media_id,attempts,next_attempt,deadline,grant_expires_at,reason,outcome FROM media_spool`

func scanMediaUpload(row *sql.Row, dir string) (MediaUpload, error) {
	var r MediaUpload
	var captured, next, deadline, grantExpires int64
	var name string
	if err := row.Scan(&r.UploadID, &name, &r.SizeBytes, &r.SHA256, &captured, &r.SourcePath, &r.SourceReceipt, &r.Phase, &r.MediaID, &r.Attempts, &next, &deadline, &grantExpires, &r.Reason, &r.Outcome); err != nil {
		return MediaUpload{}, err
	}
	if name != r.UploadID+".mp4" || !mediaUploadIDPattern.MatchString(r.UploadID) {
		return MediaUpload{}, ErrMediaSpoolRecord
	}
	r.Path = filepath.Join(dir, name)
	r.CapturedAt, r.NextAttempt = time.UnixMicro(captured).UTC(), time.UnixMicro(next).UTC()
	if deadline != 0 {
		r.Deadline = time.UnixMicro(deadline).UTC()
	}
	if grantExpires != 0 {
		r.GrantExpires = time.UnixMicro(grantExpires).UTC()
	}
	return r, nil
}

// List returns active, non-terminal media uploads whose next attempt is due.
func (s *MediaSpool) List(ctx context.Context, due time.Time) ([]MediaUpload, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, mediaSpoolSelect+` WHERE phase NOT IN ('ready','failed') AND next_attempt<=? ORDER BY next_attempt,upload_id LIMIT ?`, due.UTC().UnixMicro(), s.maxFiles)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]MediaUpload, 0, s.maxFiles)
	for rows.Next() {
		var r MediaUpload
		var captured, next, deadline, grantExpires int64
		var name string
		if err := rows.Scan(&r.UploadID, &name, &r.SizeBytes, &r.SHA256, &captured, &r.SourcePath, &r.SourceReceipt, &r.Phase, &r.MediaID, &r.Attempts, &next, &deadline, &grantExpires, &r.Reason, &r.Outcome); err != nil {
			return nil, err
		}
		if !mediaUploadIDPattern.MatchString(r.UploadID) || name != r.UploadID+".mp4" {
			return nil, ErrMediaSpoolRecord
		}
		r.Path = filepath.Join(s.dir, name)
		r.CapturedAt, r.NextAttempt = time.UnixMicro(captured).UTC(), time.UnixMicro(next).UTC()
		if deadline != 0 {
			r.Deadline = time.UnixMicro(deadline).UTC()
		}
		if grantExpires != 0 {
			r.GrantExpires = time.UnixMicro(grantExpires).UTC()
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// OpenFile opens only the persisted record's expected path, after checking
// that its basename and regular-file metadata still match the durable record.
func (s *MediaSpool) OpenFile(ctx context.Context, record MediaUpload) (*os.File, error) {
	stored, err := s.Get(ctx, record.UploadID)
	if err != nil || stored.Phase == "ready" || stored.SHA256 != record.SHA256 || stored.SizeBytes != record.SizeBytes {
		return nil, ErrMediaSpoolRecord
	}
	path := filepath.Join(s.dir, record.UploadID+".mp4")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != stored.SizeBytes {
		return nil, ErrMediaSpoolRecord
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscallNoFollow, 0)
	if err != nil {
		return nil, ErrMediaSpoolRecord
	}
	got, err := f.Stat()
	if err != nil || !os.SameFile(info, got) {
		_ = f.Close()
		return nil, ErrMediaSpoolRecord
	}
	return f, nil
}

// MarkGranted binds a fresh signed-grant expiry only to an admitted request;
// the URL and token are deliberately never persisted.
func (s *MediaSpool) MarkGranted(ctx context.Context, uploadID, mediaID string, expiresAt time.Time) error {
	if !mediaUploadIDPattern.MatchString(uploadID) || !canonicalMediaID(mediaID) || expiresAt.IsZero() {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET phase='granted',media_id=?,grant_expires_at=?,next_attempt=?
		WHERE upload_id=? AND phase='awaiting_grant' AND attempts>0 AND deadline>? AND
		(media_id='' OR media_id=?) AND grant_expires_at<?`,
		mediaID, expiresAt.UTC().UnixMicro(), time.Now().UTC().UnixMicro(), uploadID, time.Now().UTC().UnixMicro(), mediaID, expiresAt.UTC().UnixMicro())
	return requireMediaRow(res, err)
}

// MarkPutMayHaveStarted transitions an upload in granted phase to indicate a PUT may have begun.
func (s *MediaSpool) MarkPutMayHaveStarted(ctx context.Context, uploadID string, grantExpiry time.Time) error {
	if grantExpiry.IsZero() {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET phase='put_may_have_happened',next_attempt=?
		WHERE upload_id=? AND phase='granted' AND grant_expires_at=? AND grant_expires_at>? AND deadline>?`,
		time.Now().UTC().UnixMicro(), uploadID, grantExpiry.UTC().UnixMicro(), time.Now().UTC().UnixMicro(), time.Now().UTC().UnixMicro())
	return requireMediaRow(res, err)
}

// MarkAwaitingResult transitions an upload to the awaiting_result phase with a given next attempt time.
func (s *MediaSpool) MarkAwaitingResult(ctx context.Context, uploadID string, due time.Time) error {
	if due.IsZero() {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET phase='awaiting_result',next_attempt=?
		WHERE upload_id=? AND phase IN ('granted','put_may_have_happened','awaiting_result') AND deadline>?`,
		due.UTC().UnixMicro(), uploadID, time.Now().UTC().UnixMicro())
	return requireMediaRow(res, err)
}

// RecordAttempt increments the attempt counter for an upload awaiting grant and schedules the next attempt.
func (s *MediaSpool) RecordAttempt(ctx context.Context, uploadID string, due time.Time) error {
	if !mediaUploadIDPattern.MatchString(uploadID) || due.IsZero() {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET attempts=attempts+1,next_attempt=?
		WHERE upload_id=? AND phase='awaiting_grant' AND deadline>? AND attempts<?`,
		due.UTC().UnixMicro(), uploadID, time.Now().UTC().UnixMicro(), s.maxAttempts)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMediaSpoolAttempts
	}
	return nil
}

// SchedulePoll updates the next attempt schedule for an upload in awaiting_result phase.
func (s *MediaSpool) SchedulePoll(ctx context.Context, uploadID string, due time.Time) error {
	if due.IsZero() {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET next_attempt=?
		WHERE upload_id=? AND phase='awaiting_result' AND deadline>?`,
		due.UTC().UnixMicro(), uploadID, time.Now().UTC().UnixMicro())
	return requireMediaRow(res, err)
}

// Fail records an upload as expired with the provided failure reason.
func (s *MediaSpool) Fail(ctx context.Context, uploadID, reason string, now time.Time) error {
	return s.recordResult(ctx, uploadID, "", "expired", reason, now)
}

// RecordTerminal persists a terminal outcome (rejected, expired, or conflict) for an upload.
func (s *MediaSpool) RecordTerminal(ctx context.Context, uploadID, mediaID, status, code string, now time.Time) error {
	if status != "rejected" && status != "expired" && status != "conflict" {
		return ErrMediaSpoolRecord
	}
	if code == "" {
		code = strings.ToUpper(status)
	}
	return s.recordResult(ctx, uploadID, mediaID, status, code, now)
}

// MoveToReconcile schedules an upload for completion reconciliation, optionally binding a media ID.
func (s *MediaSpool) MoveToReconcile(ctx context.Context, uploadID string, due time.Time, mediaID ...string) error {
	if due.IsZero() {
		return ErrMediaSpoolRecord
	}
	var mid string
	if len(mediaID) > 0 && mediaID[0] != "" {
		mid = mediaID[0]
		if !canonicalMediaID(mid) {
			return ErrMediaSpoolRecord
		}
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	var res sql.Result
	var err error
	if mid != "" {
		res, err = s.db.ExecContext(ctx, `UPDATE media_spool SET phase='awaiting_result',next_attempt=?,
			media_id=CASE WHEN media_id='' THEN ? ELSE media_id END
			WHERE upload_id=? AND phase IN ('awaiting_grant','granted','put_may_have_happened','awaiting_result')
			AND (media_id='' OR media_id=?) AND deadline>?`,
			due.UTC().UnixMicro(), mid, uploadID, mid, time.Now().UTC().UnixMicro())
	} else {
		res, err = s.db.ExecContext(ctx, `UPDATE media_spool SET phase='awaiting_result',next_attempt=?
			WHERE upload_id=? AND phase IN ('awaiting_grant','granted','put_may_have_happened','awaiting_result') AND deadline>?`,
			due.UTC().UnixMicro(), uploadID, time.Now().UTC().UnixMicro())
	}
	return requireMediaRow(res, err)
}

func (s *MediaSpool) recordResult(ctx context.Context, uploadID, mediaID, status, reason string, now time.Time) error {
	if !mediaUploadIDPattern.MatchString(uploadID) || now.IsZero() || (status != "ready" && status != "rejected" && status != "expired" && status != "conflict") || len(reason) > 256 {
		return ErrMediaSpoolRecord
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			slog.Warn("gatewaysim: media spool transaction rollback failed")
		}
	}()
	var name, bound, phase, priorOutcome, priorReason string
	err = tx.QueryRowContext(ctx, "SELECT spool_name,media_id,phase,outcome,reason FROM media_spool WHERE upload_id=?", uploadID).Scan(&name, &bound, &phase, &priorOutcome, &priorReason)
	if err != nil {
		return err
	}
	if mediaID != "" && bound != "" && mediaID != bound {
		return ErrMediaSpoolConflict
	}
	if status == "ready" {
		if name != uploadID+".mp4" {
			return ErrMediaSpoolRecord
		}
		if phase == "ready" {
			if bound == "" || bound != mediaID || !canonicalMediaID(mediaID) {
				return ErrMediaSpoolConflict
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			path := filepath.Join(s.dir, name)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			return syncDirectory(s.dir)
		}
		if phase == "failed" || !canonicalMediaID(mediaID) || bound == "" || bound != mediaID {
			return ErrMediaSpoolConflict
		}
		// Commit ready before unlink. A replay or restart can safely retry the
		// unlink, but never deletes a path not derived from this persisted ID.
		if _, err = tx.ExecContext(ctx, "UPDATE media_spool SET phase='ready',media_id=?,reason='',next_attempt=? WHERE upload_id=?", mediaID, now.UTC().UnixMicro(), uploadID); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		path := filepath.Join(s.dir, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncDirectory(s.dir)
	}
	if phase == "ready" {
		return ErrMediaSpoolConflict
	}
	if len(strings.TrimSpace(reason)) == 0 {
		return ErrMediaSpoolRecord
	}
	if !safeMediaReason.MatchString(reason) {
		return ErrMediaSpoolRecord
	}
	if phase == "failed" {
		if priorOutcome == status && priorReason == reason {
			return nil
		}
		return ErrMediaSpoolConflict
	}
	_, err = tx.ExecContext(ctx, "UPDATE media_spool SET phase='failed',outcome=?,reason=?,next_attempt=? WHERE upload_id=? AND phase NOT IN ('ready','failed')", status, reason, now.UTC().UnixMicro(), uploadID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// ApplyMQTTResult accepts only a schema-valid result for this spool's Gateway;
// callers do not translate loosely correlated MQTT bytes into ready state.
func (s *MediaSpool) ApplyMQTTResult(ctx context.Context, payload []byte, now time.Time) error {
	return s.ApplyMQTTResultTopic(ctx, mediaprotocol.UploadResultTopic(s.gatewayID), payload, now)
}

// ApplyMQTTResultTopic validates and applies a decoded MQTT result message matching the spool's gateway.
func (s *MediaSpool) ApplyMQTTResultTopic(ctx context.Context, topic string, payload []byte, now time.Time, retryAt ...time.Time) error {
	if topic != mediaprotocol.UploadResultTopic(s.gatewayID) {
		return ErrMediaSpoolRecord
	}
	message, err := mediaprotocol.Decode(topic, payload)
	if err != nil {
		return ErrMediaSpoolRecord
	}
	result, ok := message.(mediaprotocol.UploadResult)
	if !ok {
		return ErrMediaSpoolRecord
	}
	if result.GatewayID != s.gatewayID {
		return ErrMediaSpoolRecord
	}
	if result.Status == mediaprotocol.ResultReady {
		return s.recordResult(ctx, result.UploadID, result.MediaID, "ready", "", now)
	}
	if result.Status == mediaprotocol.ResultRejected || result.Status == mediaprotocol.ResultExpired || result.Status == mediaprotocol.ResultConflict {
		reason := result.Code
		if reason == "" {
			reason = strings.ToUpper(result.Status)
		}
		return s.recordResult(ctx, result.UploadID, result.MediaID, result.Status, reason, now)
	}
	if result.Status == mediaprotocol.ResultPending && result.Code == "OBJECT_MISSING" {
		if !result.Retryable || !canonicalMediaID(result.MediaID) {
			return ErrMediaSpoolRecord
		}
		due := now
		if len(retryAt) > 0 && !retryAt[0].IsZero() {
			due = retryAt[0]
		}
		ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
		defer cancel()
		res, err := s.db.ExecContext(ctx, `UPDATE media_spool SET phase='awaiting_grant',next_attempt=?,
			media_id=CASE WHEN media_id='' THEN ? ELSE media_id END
			WHERE upload_id=? AND phase='awaiting_result' AND (media_id='' OR media_id=?) AND deadline>?`,
			due.UTC().UnixMicro(), result.MediaID, result.UploadID, result.MediaID, now.UTC().UnixMicro())
		return requireMediaRow(res, err)
	}
	// pending/verifying responses are not terminal; retain their phase and file.
	return nil
}

func canonicalMediaID(id string) bool {
	return mediaIDPattern.MatchString(id)
}

func requireMediaRow(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMediaSpoolRecord
	}
	return nil
}

func syncDirectory(path string) (retErr error) {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if cErr := d.Close(); retErr == nil {
			retErr = cErr
		}
	}()
	return d.Sync()
}

// Close closes the underlying SQLite database and releases the exclusive process lock.
func (s *MediaSpool) Close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	if s.release != nil {
		if e := s.release(); err == nil {
			err = e
		}
		s.release = nil
	}
	return err
}
