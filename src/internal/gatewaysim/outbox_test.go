package gatewaysim

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	outboxBootID    = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	outboxMessageID = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	outboxBaseTime  = time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
)

func testOutboxConfig(path string) OutboxConfig {
	return OutboxConfig{
		Path:      path,
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		MaxRows:   100,
		MaxBytes:  1 << 20,
		OpTimeout: 5 * time.Second,
	}
}

// openTestOutbox opens the outbox and registers an idempotent cleanup close.
func openTestOutbox(t *testing.T, cfg OutboxConfig) *Outbox {
	t.Helper()
	o, err := OpenOutbox(cfg)
	if err != nil {
		t.Fatalf("OpenOutbox: %v", err)
	}
	t.Cleanup(func() { _ = o.Close() })
	return o
}

func enqueueTestBatch(t *testing.T, o *Outbox, boot, message uuid.UUID, measuredAt time.Time) Item {
	t.Helper()
	item, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID:           boot,
		MessageID:        message,
		MeasuredAt:       measuredAt,
		ReceivedAt:       measuredAt.Add(time.Second),
		SampleCount:      100,
		SampleIntervalUS: 10000,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return item
}

func readStoredSequence(t *testing.T, o *Outbox, boot uuid.UUID) int64 {
	t.Helper()
	var next int64
	if err := o.db.QueryRow("SELECT next_sequence FROM outbox_streams WHERE boot_id = ?", boot.String()).Scan(&next); err != nil {
		t.Fatalf("read next_sequence: %v", err)
	}
	return next
}

func countStoredRows(t *testing.T, o *Outbox) int64 {
	t.Helper()
	var rows int64
	if err := o.db.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return rows
}

func TestOutboxRejectsMemoryAndRelativePaths(t *testing.T) {
	for _, path := range []string{":memory:", "outbox.db", "/tmp/x/outbox.db?mode=memory"} {
		if _, err := OpenOutbox(testOutboxConfig(path)); err == nil {
			t.Errorf("path %q: expected error", path)
		}
	}
}

func TestOutboxEnqueueReopenByteExact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	buildBatch := func() BuiltBatch {
		built, err := BuildSyntheticBatch(BatchRequest{
			GatewayID:        "gateway_001",
			SensorID:         "sensor_001",
			BootID:           outboxBootID,
			MessageID:        outboxMessageID,
			FirstSequence:    0,
			MeasuredAt:       outboxBaseTime,
			SampleCount:      100,
			SampleIntervalUS: 10000,
		}, ServerLimits(), outboxBaseTime.Add(time.Second))
		if err != nil {
			t.Fatalf("BuildSyntheticBatch: %v", err)
		}
		return built
	}

	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)
	built := buildBatch()

	if item.Topic != "gateways/gateway_001/telemetry/sensor_001" {
		t.Fatalf("topic = %q", item.Topic)
	}
	if !bytes.Equal(item.Payload, built.Payload) {
		t.Fatalf("stored payload differs from a fresh build")
	}
	if item.ByteSize != int64(len(item.Topic)+len(item.Payload)) {
		t.Fatalf("byte_size = %d", item.ByteSize)
	}
	if item.FirstSequence != 0 || item.SampleCount != 100 {
		t.Fatalf("range = %d/%d", item.FirstSequence, item.SampleCount)
	}
	if item.Status != outboxStatusPending {
		t.Fatalf("status = %q", item.Status)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openTestOutbox(t, testOutboxConfig(path))
	items, err := reopened.Pending(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("pending len = %d", len(items))
	}
	got := items[0]
	if got.MessageID != outboxMessageID || got.BootID != outboxBootID {
		t.Fatalf("ids = %s/%s", got.MessageID, got.BootID)
	}
	if got.GatewayID != "gateway_001" || got.SensorID != "sensor_001" {
		t.Fatalf("identity = %s/%s", got.GatewayID, got.SensorID)
	}
	if got.Topic != built.Topic || !bytes.Equal(got.Payload, built.Payload) {
		t.Fatalf("topic/bytes changed across reopen")
	}
	if got.FirstSequence != 0 || got.SampleCount != 100 || got.ByteSize != item.ByteSize {
		t.Fatalf("range/size changed across reopen")
	}
}

func TestOutboxSequenceProgressionAndCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	first := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)
	if first.FirstSequence != 0 {
		t.Fatalf("first sequence = %d", first.FirstSequence)
	}
	second := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))
	if second.FirstSequence != 100 {
		t.Fatalf("second sequence = %d", second.FirstSequence)
	}
	if got := readStoredSequence(t, o, outboxBootID); got != 200 {
		t.Fatalf("next_sequence = %d", got)
	}

	// A different boot stream starts at 0 and does not share the counter.
	otherBoot := uuid.New()
	third := enqueueTestBatch(t, o, otherBoot, uuid.New(), outboxBaseTime)
	if third.FirstSequence != 0 {
		t.Fatalf("other boot sequence = %d", third.FirstSequence)
	}
}

func TestOutboxCapacityByRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	o.maxRows = 2

	enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))

	_, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime.Add(2 * time.Second), ReceivedAt: outboxBaseTime.Add(3 * time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("expected ErrOutboxFull, got %v", err)
	}
	if rows := countStoredRows(t, o); rows != 2 {
		t.Fatalf("rows = %d, want 2 (failed enqueue must not insert)", rows)
	}
	if got := readStoredSequence(t, o, outboxBootID); got != 200 {
		t.Fatalf("next_sequence = %d, want 200 (failed enqueue must not advance)", got)
	}
}

func TestOutboxCapacityByBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	cfg := testOutboxConfig(path)
	cfg.MaxBytes = 32 // smaller than any one serialized 100-sample batch
	o := openTestOutbox(t, cfg)

	_, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime, ReceivedAt: outboxBaseTime.Add(time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("expected ErrOutboxFull, got %v", err)
	}
	if rows := countStoredRows(t, o); rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
}

func TestOutboxQuarantineCountsTowardQuota(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	o.maxRows = 2

	first := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))

	ok, err := o.Quarantine(context.Background(), first.MessageID, "operator isolated for inspection")
	if err != nil || !ok {
		t.Fatalf("Quarantine = %v/%v", ok, err)
	}

	counts, err := o.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if counts.PendingRows != 1 || counts.QuarantinedRows != 1 {
		t.Fatalf("counts = %+v", counts)
	}
	if counts.QuarantinedBytes != first.ByteSize {
		t.Fatalf("quarantined bytes = %d, want %d", counts.QuarantinedBytes, first.ByteSize)
	}

	// A quarantined row still occupies capacity, so a third enqueue is refused.
	_, err = o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime.Add(2 * time.Second), ReceivedAt: outboxBaseTime.Add(3 * time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("expected ErrOutboxFull, got %v", err)
	}
}

func TestOutboxIdentityBindingMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	other := testOutboxConfig(path)
	other.GatewayID = "gateway_002"
	if _, err := OpenOutbox(other); !errors.Is(err, ErrOutboxIdentity) {
		t.Fatalf("gateway mismatch: expected ErrOutboxIdentity, got %v", err)
	}

	sensorMismatch := testOutboxConfig(path)
	sensorMismatch.SensorID = "sensor_002"
	if _, err := OpenOutbox(sensorMismatch); !errors.Is(err, ErrOutboxIdentity) {
		t.Fatalf("sensor mismatch: expected ErrOutboxIdentity, got %v", err)
	}
}

func TestOutboxUnsupportedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatalf("set user_version: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	if _, err := OpenOutbox(testOutboxConfig(path)); !errors.Is(err, ErrOutboxSchema) {
		t.Fatalf("expected ErrOutboxSchema, got %v", err)
	}
}

func TestOutboxForeignTablesRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if _, err := raw.Exec("CREATE TABLE something_else (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	if _, err := OpenOutbox(testOutboxConfig(path)); !errors.Is(err, ErrOutboxSchema) {
		t.Fatalf("expected ErrOutboxSchema, got %v", err)
	}
}

func TestOutboxCorruptFileNotRecreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	garbage := []byte("this is not a sqlite database at all")
	if err := os.WriteFile(path, garbage, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := OpenOutbox(testOutboxConfig(path)); !errors.Is(err, ErrOutboxCorrupt) {
		t.Fatalf("expected ErrOutboxCorrupt, got %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(after, garbage) {
		t.Fatalf("corrupt file was modified: %q", after)
	}
}

func TestOutboxSingleActiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	first := openTestOutbox(t, testOutboxConfig(path))

	if _, err := OpenOutbox(testOutboxConfig(path)); !errors.Is(err, ErrOutboxBusy) {
		t.Fatalf("second open: expected ErrOutboxBusy, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopening after release must succeed.
	reopened := openTestOutbox(t, testOutboxConfig(path))
	if _, err := reopened.Counts(context.Background()); err != nil {
		t.Fatalf("Counts after reopen: %v", err)
	}
}

func TestOutboxPendingBoundedOrdering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	var ids []int64
	for i := 0; i < 5; i++ {
		item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Duration(i)*time.Second))
		ids = append(ids, item.ID)
	}

	items, err := o.Pending(context.Background(), 2)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("pending len = %d", len(items))
	}
	if items[0].ID != ids[0] || items[1].ID != ids[1] {
		t.Fatalf("ordering = %d,%d want %d,%d", items[0].ID, items[1].ID, ids[0], ids[1])
	}
	if items[0].FirstSequence != 0 || items[1].FirstSequence != 100 {
		t.Fatalf("sequence order = %d,%d", items[0].FirstSequence, items[1].FirstSequence)
	}

	if _, err := o.Pending(context.Background(), 0); err == nil {
		t.Error("expected error for zero limit")
	}
	if _, err := o.Pending(context.Background(), o.maxRows+1); err == nil {
		t.Error("expected error for limit above the row cap")
	}
}

func TestOutboxDeletePendingOnlyAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	keep := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	drop := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))

	deleted, err := o.DeletePending(context.Background(), "gateway_001", drop.MessageID)
	if err != nil || !deleted {
		t.Fatalf("DeletePending = %v/%v", deleted, err)
	}
	// A repeated ACK for the same id is a no-op.
	deleted, err = o.DeletePending(context.Background(), "gateway_001", drop.MessageID)
	if err != nil || deleted {
		t.Fatalf("duplicate DeletePending = %v/%v", deleted, err)
	}

	items, err := o.Pending(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(items) != 1 || items[0].MessageID != keep.MessageID {
		t.Fatalf("pending after delete = %+v", items)
	}

	// Quarantined rows are never deleted automatically by an ACK.
	if ok, err := o.Quarantine(context.Background(), keep.MessageID, "held"); err != nil || !ok {
		t.Fatalf("Quarantine = %v/%v", ok, err)
	}
	deleted, err = o.DeletePending(context.Background(), "gateway_001", keep.MessageID)
	if err != nil || deleted {
		t.Fatalf("delete quarantined = %v/%v", deleted, err)
	}
	if _, found, err := o.Lookup(context.Background(), keep.MessageID); err != nil || !found {
		t.Fatalf("quarantined row missing: found=%v err=%v", found, err)
	}

	if _, err := o.DeletePending(context.Background(), "gateway_999", keep.MessageID); !errors.Is(err, ErrOutboxIdentity) {
		t.Fatalf("cross-gateway delete: expected ErrOutboxIdentity, got %v", err)
	}
}

func TestOutboxQuarantineReasonBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)

	if _, err := o.Quarantine(context.Background(), item.MessageID, "   "); !errors.Is(err, ErrOutboxReason) {
		t.Fatalf("blank reason: expected ErrOutboxReason, got %v", err)
	}
	if _, err := o.Quarantine(context.Background(), item.MessageID, strings.Repeat("x", maxQuarantineReason+1)); !errors.Is(err, ErrOutboxReason) {
		t.Fatalf("long reason: expected ErrOutboxReason, got %v", err)
	}
	if ok, err := o.Quarantine(context.Background(), uuid.New(), "unknown row"); err != nil || ok {
		t.Fatalf("unknown row quarantine = %v/%v", ok, err)
	}
}

// TestOutboxSequenceOverflowRollsBack seeds a near-overflow boot counter and
// checks that the builder rejects the range, leaving the row set and counter
// unchanged.
func TestOutboxSequenceOverflowRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	if _, err := o.db.Exec(
		"INSERT INTO outbox_streams (gateway_id, sensor_id, boot_id, next_sequence) VALUES (?, ?, ?, ?)",
		"gateway_001", "sensor_001", outboxBootID.String(), int64(math.MaxInt64)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	_, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime, ReceivedAt: outboxBaseTime.Add(time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if err == nil {
		t.Fatal("expected sequence overflow error")
	}
	if rows := countStoredRows(t, o); rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
	if got := readStoredSequence(t, o, outboxBootID); got != math.MaxInt64 {
		t.Fatalf("next_sequence = %d, want unchanged MaxInt64", got)
	}
}

// TestOutboxDurabilityPragmas records the on-disk configuration: rollback
// journal, synchronous=FULL and a single connection.
func TestOutboxDurabilityPragmas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	var journalMode string
	if err := o.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if journalMode != "delete" {
		t.Fatalf("journal_mode = %q, want delete (rollback journal)", journalMode)
	}

	var synchronous int
	if err := o.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatalf("synchronous: %v", err)
	}
	if synchronous != 2 {
		t.Fatalf("synchronous = %d, want 2 (FULL)", synchronous)
	}

	if got := o.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("max open connections = %d, want 1", got)
	}
}

// TestOutboxEnqueueRollsBackOnRealSQLFailure injects a real SQLite trigger that
// aborts the insert, then verifies the row and the sequence counter roll back
// together and that a later successful enqueue still works.
func TestOutboxEnqueueRollsBackOnRealSQLFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))

	enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	if got := readStoredSequence(t, o, outboxBootID); got != 100 {
		t.Fatalf("next_sequence = %d, want 100", got)
	}

	if _, err := o.db.Exec(`CREATE TRIGGER fail_after_first BEFORE INSERT ON outbox
		WHEN NEW.first_sequence >= 100 BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	_, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime.Add(time.Second), ReceivedAt: outboxBaseTime.Add(2 * time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if err == nil {
		t.Fatal("expected injected SQL failure")
	}
	if rows := countStoredRows(t, o); rows != 1 {
		t.Fatalf("rows = %d, want 1 (insert must roll back)", rows)
	}
	if got := readStoredSequence(t, o, outboxBootID); got != 100 {
		t.Fatalf("next_sequence = %d, want 100 (counter must roll back)", got)
	}

	if _, err := o.db.Exec("DROP TRIGGER fail_after_first"); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	retried := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))
	if retried.FirstSequence != 100 {
		t.Fatalf("retried sequence = %d, want 100", retried.FirstSequence)
	}
}

// TestOutboxCommitAmbiguitySurfaced uses the small commit seam to check that a
// failed commit is reported as an ambiguous outcome and never leaves the caller
// holding a publishable item that was not durably stored.
func TestOutboxCommitAmbiguitySurfaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	o.commit = func(*sql.Tx) error { return errors.New("simulated commit failure") }

	_, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID: outboxBootID, MessageID: uuid.New(),
		MeasuredAt: outboxBaseTime, ReceivedAt: outboxBaseTime.Add(time.Second),
		SampleCount: 100, SampleIntervalUS: 10000,
	})
	if !errors.Is(err, ErrOutboxCommitUnknown) {
		t.Fatalf("expected ErrOutboxCommitUnknown, got %v", err)
	}
	if rows := countStoredRows(t, o); rows != 0 {
		t.Fatalf("rows = %d, want 0 after failed commit", rows)
	}
	if items, err := o.Pending(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("pending = %d/%v, want 0", len(items), err)
	}
}

// TestOpenOutboxAdminRequiresExistingFileAndAdoptsBinding verifies that the
// administrative open never creates a missing outbox, reads the bound identity
// from the file, and refuses to run while a simulator holds the lock.
func TestOpenOutboxAdminRequiresExistingFileAndAdoptsBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outbox.db")

	if _, err := OpenOutboxAdmin(path, time.Second); !errors.Is(err, ErrOutboxMissing) {
		t.Fatalf("missing file err = %v, want ErrOutboxMissing", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("admin open created a missing outbox file")
	}

	o := openTestOutbox(t, testOutboxConfig(path))
	enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)

	// While the simulator open holds the lock an admin open must fail clearly.
	if _, err := OpenOutboxAdmin(path, time.Second); !errors.Is(err, ErrOutboxBusy) {
		t.Fatalf("admin open while locked err = %v, want ErrOutboxBusy", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	admin, err := OpenOutboxAdmin(path, time.Second)
	if err != nil {
		t.Fatalf("OpenOutboxAdmin: %v", err)
	}
	defer func() { _ = admin.Close() }()
	if admin.gatewayID != "gateway_001" || admin.sensorID != "sensor_001" {
		t.Fatalf("adopted binding = %s/%s", admin.gatewayID, admin.sensorID)
	}
	counts, err := admin.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if counts.PendingRows != 1 {
		t.Fatalf("pending = %d, want 1", counts.PendingRows)
	}
	if items, err := admin.Pending(context.Background(), 10); err != nil || len(items) != 1 {
		t.Fatalf("admin pending = %d/%v, want 1", len(items), err)
	}
}
