package gatewaysim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	// The pure-Go SQLite driver is imported only by this simulator outbox. It is
	// never imported by the server runtime and requires no cgo.
	_ "modernc.org/sqlite"
)

// outboxSchemaVersion is the only SQLite schema version this simulator writes.
// A file with a different version is reported as unsupported instead of being
// recreated, so an operator must migrate or move it deliberately.
const outboxSchemaVersion = 1

// Outbox row statuses. Only these two values exist: a pending row can still be
// published and acknowledged, a quarantined row is retained for inspection but
// never published automatically and never counted as delivered.
const (
	outboxStatusPending     = "pending"
	outboxStatusQuarantined = "quarantined"
)

// maxQuarantineReason bounds the operator-supplied reason stored on a
// quarantined row.
const maxQuarantineReason = 256

// maxAdminPendingLimit bounds an administrative pending listing. It is the
// fixed row cap the read-only admin open advertises so `pending --limit` can
// never load an unbounded slice into memory.
const maxAdminPendingLimit = 1000

// Sentinel errors. Callers can classify an outbox failure without inspecting
// driver error text. ErrOutboxCommitUnknown is deliberately distinct: a failed
// commit is an ambiguous outcome, so the caller must stop the operation and
// reconcile from the file instead of generating a new id or retrying blindly.
var (
	ErrOutboxFull          = errors.New("gatewaysim: outbox capacity reached")
	ErrOutboxBusy          = errors.New("gatewaysim: outbox is locked by another simulator or admin process")
	ErrOutboxIdentity      = errors.New("gatewaysim: outbox file is bound to a different gateway or sensor")
	ErrOutboxSchema        = errors.New("gatewaysim: outbox file has an unsupported schema version")
	ErrOutboxCorrupt       = errors.New("gatewaysim: outbox file is not a usable SQLite database")
	ErrOutboxCommitUnknown = errors.New("gatewaysim: outbox commit outcome is unknown")
	ErrOutboxReason        = errors.New("gatewaysim: quarantine reason is required and bounded")
	ErrOutboxMissing       = errors.New("gatewaysim: outbox file does not exist")
)

// OutboxConfig carries the validated simulator outbox settings. A single file
// is bound to one Gateway/Sensor pair; the identity is checked on every open.
type OutboxConfig struct {
	Path      string
	GatewayID string
	SensorID  string
	MaxRows   int
	MaxBytes  int64
	OpTimeout time.Duration
}

// Outbox is the durable SQLite outbox for one Gateway/Sensor. All externally
// used operations are context-bounded and never hold a SQL transaction open
// while waiting on the network.
type Outbox struct {
	db        *sql.DB
	gatewayID string
	sensorID  string
	maxRows   int
	maxBytes  int64
	opTimeout time.Duration

	now     func() time.Time
	commit  func(*sql.Tx) error
	release func() error
}

// Item is one durable outbox row. Payload is the exact immutable byte slice
// stored at enqueue time and republished unchanged on every resend.
type Item struct {
	ID            int64
	MessageID     uuid.UUID
	GatewayID     string
	SensorID      string
	BootID        uuid.UUID
	Topic         string
	Payload       []byte
	ByteSize      int64
	FirstSequence int64
	SampleCount   int32
	Status        string
	EnqueuedAt    time.Time
}

// EnqueueRequest is the caller-supplied identity, measurement time and sample
// profile for one synthetic batch. The outbox owns sequence allocation and
// serialization; the caller never supplies first_sequence.
type EnqueueRequest struct {
	BootID           uuid.UUID
	MessageID        uuid.UUID
	MeasuredAt       time.Time
	ReceivedAt       time.Time
	SampleCount      int32
	SampleIntervalUS int64
}

// OutboxCounts summarizes retained rows and business bytes by status. Bytes are
// the sum of stored topic and payload sizes, not the on-disk file size.
type OutboxCounts struct {
	PendingRows      int64
	PendingBytes     int64
	QuarantinedRows  int64
	QuarantinedBytes int64
}

// outboxSelectColumns keeps every row read returning the same column set.
const outboxSelectColumns = "id, message_id, gateway_id, sensor_id, boot_id, topic, payload, byte_size, first_sequence, sample_count, status, enqueued_at"

// OpenOutbox locks the sidecar file, opens the durable database and prepares
// the schema. It never deletes or recreates an existing file: a foreign,
// corrupt or unsupported file fails with a classified error.
func OpenOutbox(cfg OutboxConfig) (*Outbox, error) {
	if err := validateOutboxConfig(cfg); err != nil {
		return nil, err
	}

	release, err := lockOutbox(cfg.Path)
	if err != nil {
		return nil, err
	}

	db, err := openOutboxDB(cfg)
	if err != nil {
		_ = release()
		return nil, err
	}

	o := &Outbox{
		db:        db,
		gatewayID: cfg.GatewayID,
		sensorID:  cfg.SensorID,
		maxRows:   cfg.MaxRows,
		maxBytes:  cfg.MaxBytes,
		opTimeout: cfg.OpTimeout,
		now:       time.Now,
		commit:    func(tx *sql.Tx) error { return tx.Commit() },
		release:   release,
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
	defer cancel()
	if err := o.prepare(ctx); err != nil {
		_ = db.Close()
		_ = release()
		return nil, err
	}
	return o, nil
}

// OpenOutboxAdmin opens an existing outbox file for administrative metadata
// reads and quarantine. Unlike OpenOutbox it never creates a file or schema: a
// missing file fails with ErrOutboxMissing, a file whose schema is not the
// supported version fails without being rewritten, and the bound Gateway/Sensor
// identity is read from the file instead of the environment (so an admin
// command needs only the outbox path). It takes the same exclusive lock as a
// running simulator, so it cannot run while a simulator is active and fails
// clearly with ErrOutboxBusy rather than mutating a live outbox.
func OpenOutboxAdmin(path string, opTimeout time.Duration) (*Outbox, error) {
	if strings.Contains(path, "?") || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("gatewaysim: outbox path must be a clean absolute file path without a query string")
	}
	if opTimeout <= 0 {
		return nil, fmt.Errorf("gatewaysim: outbox operation timeout must be positive")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrOutboxMissing
	}

	release, err := lockOutbox(path)
	if err != nil {
		return nil, err
	}
	db, err := openOutboxDB(OutboxConfig{Path: path, OpTimeout: opTimeout})
	if err != nil {
		_ = release()
		return nil, err
	}

	o := &Outbox{
		db:        db,
		maxRows:   maxAdminPendingLimit,
		maxBytes:  1,
		opTimeout: opTimeout,
		now:       time.Now,
		commit:    func(tx *sql.Tx) error { return tx.Commit() },
		release:   release,
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	version, err := readUserVersion(ctx, db)
	if err != nil {
		_ = db.Close()
		_ = release()
		return nil, err
	}
	if version != outboxSchemaVersion {
		_ = db.Close()
		_ = release()
		return nil, fmt.Errorf("%w: file version %d, supported version %d", ErrOutboxSchema, version, outboxSchemaVersion)
	}
	gatewayID, sensorID, err := readBinding(ctx, db)
	if err != nil {
		_ = db.Close()
		_ = release()
		return nil, err
	}
	o.gatewayID = gatewayID
	o.sensorID = sensorID
	return o, nil
}

// readUserVersion reads the SQLite user_version, classifying a driver failure as
// a corrupt outbox so a non-SQLite file is never treated as empty.
func readUserVersion(ctx context.Context, db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
	}
	return version, nil
}

// readBinding reads the single bound Gateway/Sensor identity row. A missing row
// is corruption for an initialized file, not a reason to insert a new binding.
func readBinding(ctx context.Context, db *sql.DB) (string, string, error) {
	var gatewayID, sensorID string
	if err := db.QueryRowContext(ctx, "SELECT gateway_id, sensor_id FROM outbox_meta WHERE id = 1").Scan(&gatewayID, &sensorID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", fmt.Errorf("%w: bound identity row is missing", ErrOutboxCorrupt)
		}
		return "", "", fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
	}
	return gatewayID, sensorID, nil
}

func validateOutboxConfig(cfg OutboxConfig) error {
	if cfg.Path == ":memory:" || strings.Contains(cfg.Path, "mode=memory") {
		return fmt.Errorf("gatewaysim: outbox path must be a durable file, not an in-memory database")
	}
	if !filepath.IsAbs(cfg.Path) || filepath.Clean(cfg.Path) != cfg.Path || strings.Contains(cfg.Path, "?") {
		return fmt.Errorf("gatewaysim: outbox path must be a clean absolute file path without a query string")
	}
	if cfg.GatewayID == "" || cfg.SensorID == "" {
		return fmt.Errorf("gatewaysim: outbox requires a gateway and sensor identity")
	}
	if cfg.MaxRows <= 0 {
		return fmt.Errorf("gatewaysim: outbox max rows must be positive")
	}
	if cfg.MaxBytes <= 0 {
		return fmt.Errorf("gatewaysim: outbox max bytes must be positive")
	}
	if cfg.OpTimeout <= 0 {
		return fmt.Errorf("gatewaysim: outbox operation timeout must be positive")
	}
	return nil
}

// openOutboxDB opens the file with one write connection and the initial
// durable configuration: rollback journal, synchronous=FULL, a finite busy
// timeout and immediate write transactions.
func openOutboxDB(cfg OutboxConfig) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: cfg.Path}
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", cfg.OpTimeout.Milliseconds()))
	q.Add("_pragma", "journal_mode(delete)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_txlock", "immediate")
	dsn := u.String() + "?" + q.Encode()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
	}
	return db, nil
}

// prepare inspects the schema version and either validates the existing binding
// or initializes a fresh file. It refuses a foreign or unsupported file.
func (o *Outbox) prepare(ctx context.Context) error {
	version, err := readUserVersion(ctx, o.db)
	if err != nil {
		return err
	}

	switch version {
	case outboxSchemaVersion:
		return o.verifyBinding(ctx)
	case 0:
		var tables int
		if err := o.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
			return fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
		}
		if tables != 0 {
			return fmt.Errorf("%w: found %d foreign tables, refusing to reuse the file", ErrOutboxSchema, tables)
		}
		return o.createSchema(ctx)
	default:
		return fmt.Errorf("%w: file version %d, supported version %d", ErrOutboxSchema, version, outboxSchemaVersion)
	}
}

func (o *Outbox) createSchema(ctx context.Context) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	statements := []string{
		`CREATE TABLE outbox_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			gateway_id TEXT NOT NULL,
			sensor_id TEXT NOT NULL
		)`,
		`CREATE TABLE outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT NOT NULL UNIQUE,
			gateway_id TEXT NOT NULL,
			sensor_id TEXT NOT NULL,
			boot_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			payload BLOB NOT NULL,
			byte_size INTEGER NOT NULL,
			first_sequence INTEGER NOT NULL,
			sample_count INTEGER NOT NULL,
			sample_interval_us INTEGER NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('pending','quarantined')),
			reason TEXT,
			enqueued_at TEXT NOT NULL
		)`,
		`CREATE INDEX outbox_pending_order ON outbox (status, id)`,
		`CREATE INDEX outbox_stream_lookup ON outbox (gateway_id, sensor_id, boot_id, first_sequence)`,
		`CREATE TABLE outbox_streams (
			gateway_id TEXT NOT NULL,
			sensor_id TEXT NOT NULL,
			boot_id TEXT NOT NULL,
			next_sequence INTEGER NOT NULL,
			PRIMARY KEY (gateway_id, sensor_id, boot_id)
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO outbox_meta (id, gateway_id, sensor_id) VALUES (1, ?, ?)", o.gatewayID, o.sensorID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", outboxSchemaVersion)); err != nil {
		return err
	}
	if err := o.commit(tx); err != nil {
		return fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	committed = true
	return nil
}

// verifyBinding checks the stored Gateway/Sensor identity and inserts it when
// the file predates the binding row. A mismatch is rejected so pending rows for
// one identity are never published or acknowledged under another.
func (o *Outbox) verifyBinding(ctx context.Context) error {
	var gatewayID, sensorID string
	err := o.db.QueryRowContext(ctx, "SELECT gateway_id, sensor_id FROM outbox_meta WHERE id = 1").Scan(&gatewayID, &sensorID)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = o.db.ExecContext(ctx, "INSERT INTO outbox_meta (id, gateway_id, sensor_id) VALUES (1, ?, ?)", o.gatewayID, o.sensorID)
		return err
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOutboxCorrupt, err)
	}
	if gatewayID != o.gatewayID || sensorID != o.sensorID {
		return fmt.Errorf("%w: file=%s/%s configured=%s/%s", ErrOutboxIdentity, gatewayID, sensorID, o.gatewayID, o.sensorID)
	}
	return nil
}

// bounded applies the configured operation timeout unless the caller already
// supplied a deadline, so every externally used operation stays finite.
func (o *Outbox) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, o.opTimeout)
}

// Enqueue allocates the next sequence for the boot stream, checks overflow and
// capacity, builds and serializes the batch exactly once, inserts the row and
// advances the counter, all in one transaction. The returned Item is only
// publishable when the commit succeeded; any error rolls the row and counter
// back together, and a failed commit is surfaced as an ambiguous outcome with
// no automatic id/range regeneration.
func (o *Outbox) Enqueue(ctx context.Context, req EnqueueRequest) (Item, error) {
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return Item{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	firstSequence, err := nextSequence(ctx, tx, o.gatewayID, o.sensorID, req.BootID)
	if err != nil {
		return Item{}, err
	}

	rows, totalBytes, err := outboxUsage(ctx, tx)
	if err != nil {
		return Item{}, err
	}
	if rows >= int64(o.maxRows) {
		return Item{}, ErrOutboxFull
	}

	// Build once with the allocated range; the resulting bytes are stored and
	// republished unchanged. The builder rejects sequence overflow.
	built, err := BuildSyntheticBatch(BatchRequest{
		GatewayID:        o.gatewayID,
		SensorID:         o.sensorID,
		BootID:           req.BootID,
		MessageID:        req.MessageID,
		FirstSequence:    firstSequence,
		MeasuredAt:       req.MeasuredAt,
		SampleCount:      req.SampleCount,
		SampleIntervalUS: req.SampleIntervalUS,
	}, ServerLimits(), req.ReceivedAt)
	if err != nil {
		return Item{}, err
	}

	byteSize := int64(len(built.Topic) + len(built.Payload))
	// Capacity also counts quarantined rows/bytes so isolation never exceeds the
	// configured quota.
	if totalBytes+byteSize > o.maxBytes {
		return Item{}, ErrOutboxFull
	}

	enqueuedAt := o.now().UTC()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (message_id, gateway_id, sensor_id, boot_id, topic, payload, byte_size, first_sequence, sample_count, sample_interval_us, status, enqueued_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.MessageID.String(), o.gatewayID, o.sensorID, req.BootID.String(), built.Topic, built.Payload,
		byteSize, firstSequence, int64(req.SampleCount), req.SampleIntervalUS, outboxStatusPending, enqueuedAt.Format(time.RFC3339Nano))
	if err != nil {
		return Item{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Item{}, err
	}

	newNext := firstSequence + int64(req.SampleCount)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_streams (gateway_id, sensor_id, boot_id, next_sequence) VALUES (?, ?, ?, ?)
		 ON CONFLICT(gateway_id, sensor_id, boot_id) DO UPDATE SET next_sequence = excluded.next_sequence`,
		o.gatewayID, o.sensorID, req.BootID.String(), newNext); err != nil {
		return Item{}, err
	}

	if err := o.commit(tx); err != nil {
		return Item{}, fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	committed = true

	return Item{
		ID:            id,
		MessageID:     req.MessageID,
		GatewayID:     o.gatewayID,
		SensorID:      o.sensorID,
		BootID:        req.BootID,
		Topic:         built.Topic,
		Payload:       built.Payload,
		ByteSize:      byteSize,
		FirstSequence: firstSequence,
		SampleCount:   req.SampleCount,
		Status:        outboxStatusPending,
		EnqueuedAt:    enqueuedAt,
	}, nil
}

// Pending returns at most limit pending rows in enqueue order. The limit is
// bounded so the caller never loads the whole outbox into memory.
func (o *Outbox) Pending(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 || limit > o.maxRows {
		return nil, fmt.Errorf("gatewaysim: pending limit must be between 1 and %d", o.maxRows)
	}
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	rows, err := o.db.QueryContext(ctx,
		"SELECT "+outboxSelectColumns+" FROM outbox WHERE gateway_id = ? AND status = ? ORDER BY id ASC LIMIT ?",
		o.gatewayID, outboxStatusPending, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]Item, 0, limit)
	for rows.Next() {
		item, err := scanOutboxItem(rows.Scan)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// PendingAfter returns at most limit pending rows with id strictly greater than
// afterID, in enqueue order. It is the bounded pagination the scheduler uses to
// walk past pending rows that are already in flight instead of re-reading the
// same first rows forever; a caller advances afterID to the id of the last row
// it saw. limit is bounded so the whole outbox is never loaded into memory.
func (o *Outbox) PendingAfter(ctx context.Context, afterID int64, limit int) ([]Item, error) {
	if limit <= 0 || limit > o.maxRows {
		return nil, fmt.Errorf("gatewaysim: pending limit must be between 1 and %d", o.maxRows)
	}
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	rows, err := o.db.QueryContext(ctx,
		"SELECT "+outboxSelectColumns+" FROM outbox WHERE gateway_id = ? AND status = ? AND id > ? ORDER BY id ASC LIMIT ?",
		o.gatewayID, outboxStatusPending, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]Item, 0, limit)
	for rows.Next() {
		item, err := scanOutboxItem(rows.Scan)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// Lookup returns the row for one message id in the bound Gateway, or ok=false
// when no such row exists. It is a bounded single-row read.
func (o *Outbox) Lookup(ctx context.Context, messageID uuid.UUID) (Item, bool, error) {
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	row := o.db.QueryRowContext(ctx,
		"SELECT "+outboxSelectColumns+" FROM outbox WHERE gateway_id = ? AND message_id = ?",
		o.gatewayID, messageID.String())
	item, err := scanOutboxItem(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Item{}, false, nil
	}
	if err != nil {
		return Item{}, false, err
	}
	return item, true, nil
}

// Counts returns bounded aggregate metadata for pending and quarantined rows.
func (o *Outbox) Counts(ctx context.Context) (OutboxCounts, error) {
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	var c OutboxCounts
	err := o.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'pending' THEN byte_size ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'quarantined' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'quarantined' THEN byte_size ELSE 0 END), 0)
		FROM outbox WHERE gateway_id = ?`, o.gatewayID).Scan(&c.PendingRows, &c.PendingBytes, &c.QuarantinedRows, &c.QuarantinedBytes)
	if err != nil {
		return OutboxCounts{}, err
	}
	return c, nil
}

// DeletePending removes exactly one pending row for the given Gateway and
// message id in a transaction. It returns deleted=true only after the commit
// succeeds. A missing row, a row owned by another Gateway, or a quarantined row
// is a no-op, so a repeated ACK is idempotent.
func (o *Outbox) DeletePending(ctx context.Context, gatewayID string, messageID uuid.UUID) (bool, error) {
	if gatewayID != o.gatewayID {
		return false, fmt.Errorf("%w: %s is not the bound gateway %s", ErrOutboxIdentity, gatewayID, o.gatewayID)
	}
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx,
		"DELETE FROM outbox WHERE gateway_id = ? AND message_id = ? AND status = ?",
		gatewayID, messageID.String(), outboxStatusPending)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := o.commit(tx); err != nil {
		return false, fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	committed = true
	return affected > 0, nil
}

// Quarantine moves exactly one pending row to the quarantined status with a
// bounded reason. It preserves identity, topic, bytes and quota; it never
// regenerates an id or re-enqueues the item.
func (o *Outbox) Quarantine(ctx context.Context, messageID uuid.UUID, reason string) (bool, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > maxQuarantineReason || strings.ContainsAny(reason, "\x00\r\n") {
		return false, fmt.Errorf("%w: length must be 1..%d", ErrOutboxReason, maxQuarantineReason)
	}
	ctx, cancel := o.bounded(ctx)
	defer cancel()

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx,
		"UPDATE outbox SET status = ?, reason = ? WHERE gateway_id = ? AND message_id = ? AND status = ?",
		outboxStatusQuarantined, reason, o.gatewayID, messageID.String(), outboxStatusPending)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := o.commit(tx); err != nil {
		return false, fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	committed = true
	return affected > 0, nil
}

// Close closes the database and releases the sidecar lock. It is idempotent so
// a deferred cleanup after an explicit close is harmless; both steps are
// attempted even if the first fails.
func (o *Outbox) Close() error {
	var errs []error
	if o.db != nil {
		db := o.db
		o.db = nil
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if o.release != nil {
		release := o.release
		o.release = nil
		if err := release(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// nextSequence reads the next sequence for one boot stream, defaulting to 0 for
// a first batch. A negative stored counter is treated as corruption.
func nextSequence(ctx context.Context, tx *sql.Tx, gatewayID, sensorID string, bootID uuid.UUID) (int64, error) {
	var next int64
	err := tx.QueryRowContext(ctx,
		"SELECT next_sequence FROM outbox_streams WHERE gateway_id = ? AND sensor_id = ? AND boot_id = ?",
		gatewayID, sensorID, bootID.String()).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if next < 0 {
		return 0, fmt.Errorf("%w: negative next_sequence", ErrOutboxCorrupt)
	}
	return next, nil
}

// outboxUsage returns the total row count and total business bytes currently
// retained, including quarantined rows.
func outboxUsage(ctx context.Context, tx *sql.Tx) (int64, int64, error) {
	var rows, bytes int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(byte_size), 0) FROM outbox").Scan(&rows, &bytes); err != nil {
		return 0, 0, err
	}
	return rows, bytes, nil
}

// scanOutboxItem decodes one row selected with outboxSelectColumns.
func scanOutboxItem(scan func(dest ...any) error) (Item, error) {
	var item Item
	var messageID, bootID, enqueuedAt string
	if err := scan(&item.ID, &messageID, &item.GatewayID, &item.SensorID, &bootID, &item.Topic,
		&item.Payload, &item.ByteSize, &item.FirstSequence, &item.SampleCount, &item.Status, &enqueuedAt); err != nil {
		return Item{}, err
	}
	mid, err := uuid.Parse(messageID)
	if err != nil {
		return Item{}, fmt.Errorf("%w: invalid message_id", ErrOutboxCorrupt)
	}
	bid, err := uuid.Parse(bootID)
	if err != nil {
		return Item{}, fmt.Errorf("%w: invalid boot_id", ErrOutboxCorrupt)
	}
	at, err := time.Parse(time.RFC3339Nano, enqueuedAt)
	if err != nil {
		return Item{}, fmt.Errorf("%w: invalid enqueued_at", ErrOutboxCorrupt)
	}
	item.MessageID = mid
	item.BootID = bid
	item.EnqueuedAt = at
	return item, nil
}
