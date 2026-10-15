package telemetry

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type telemetryFakeDatabase struct {
	tx       *telemetryFakeTx
	beginErr error
	options  pgx.TxOptions
	deadline bool
	begins   int
}

func (database *telemetryFakeDatabase) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	database.begins++
	database.options = options
	_, database.deadline = ctx.Deadline()
	if database.beginErr != nil {
		return nil, database.beginErr
	}
	return database.tx, nil
}

type telemetryFakeTx struct {
	pgx.Tx
	message       *Message
	duplicate     bool
	commitErr     error
	copyErr       error
	copyCount     int64
	copyRows      int
	commitCalls   int
	rollbackCalls int
	rollbackValid bool
	queryCalls    int
	cancelQuery   context.CancelFunc
}

func (tx *telemetryFakeTx) QueryRow(ctx context.Context, query string, _ ...any) pgx.Row {
	tx.queryCalls++
	if tx.cancelQuery != nil {
		tx.cancelQuery()
		tx.cancelQuery = nil
		return telemetryFakeRow{err: ctx.Err()}
	}
	switch {
	case strings.Contains(query, "SELECT EXISTS") && strings.Contains(query, "public.gateways"):
		return telemetryFakeRow{values: []any{true}}
	case strings.Contains(query, "FROM public.sensors"):
		unit := "C"
		return telemetryFakeRow{values: []any{&unit}}
	case strings.Contains(query, "gateway_entity.id"):
		return telemetryFakeRow{values: []any{uuid.New(), uuid.New(), true}}
	case strings.Contains(query, "INSERT INTO public.processed_messages"):
		if tx.duplicate {
			return telemetryFakeRow{err: pgx.ErrNoRows}
		}
		return telemetryFakeRow{values: []any{"gateway_001"}}
	case strings.Contains(query, "SELECT EXISTS") && strings.Contains(query, "processed_messages"):
		return telemetryFakeRow{values: []any{false}}
	case strings.Contains(query, "SELECT payload_hash, source_topic"):
		metadata, err := ledgerMetadata(tx.message)
		if err != nil {
			return telemetryFakeRow{err: err}
		}
		return telemetryFakeRow{values: []any{
			pgtype.Text{String: metadata.payloadHash, Valid: true},
			pgtype.Text{String: metadata.topic.SourceTopic, Valid: true},
			pgtype.Text{String: metadata.sensorID, Valid: true},
			metadata.bootID,
			pgtype.Int8{Int64: metadata.firstSample.SequenceNum, Valid: true},
			pgtype.Int8{Int64: metadata.lastSample.SequenceNum, Valid: true},
			pgtype.Int4{Int32: metadata.sampleCount, Valid: true},
		}}
	default:
		return telemetryFakeRow{err: errors.New("unexpected query")}
	}
}

func (tx *telemetryFakeTx) CopyFrom(_ context.Context, table pgx.Identifier, columns []string, source pgx.CopyFromSource) (int64, error) {
	if strings.Join(table, ".") != "public.telemetry" || len(columns) != 8 {
		return 0, errors.New("unexpected CopyFrom target")
	}
	for source.Next() {
		if _, err := source.Values(); err != nil {
			return 0, err
		}
		tx.copyRows++
	}
	if err := source.Err(); err != nil {
		return 0, err
	}
	return tx.copyCount, tx.copyErr
}

func (tx *telemetryFakeTx) Commit(context.Context) error {
	tx.commitCalls++
	return tx.commitErr
}

func (tx *telemetryFakeTx) Rollback(ctx context.Context) error {
	tx.rollbackCalls++
	_, tx.rollbackValid = ctx.Deadline()
	return nil
}

type telemetryFakeRow struct {
	values []any
	err    error
}

func (row telemetryFakeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("fake scan column count mismatch")
	}
	for index, destination := range destinations {
		target := reflect.ValueOf(destination)
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return errors.New("fake scan destination must be a pointer")
		}
		target = target.Elem()
		value := reflect.ValueOf(row.values[index])
		if !value.IsValid() {
			target.SetZero()
			continue
		}
		if value.Type().AssignableTo(target.Type()) {
			target.Set(value)
		} else if value.Type().ConvertibleTo(target.Type()) {
			target.Set(value.Convert(target.Type()))
		} else {
			return errors.New("fake scan type mismatch")
		}
	}
	return nil
}

func telemetryRepositoryTestMessage(t *testing.T) *Message {
	t.Helper()
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	payload := messageTestBatchPayload(t, "2026-08-21T10:15:00Z", 12501, []float64{1, 2, 3})
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func newTelemetryFakeRepository(t *testing.T, tx *telemetryFakeTx) (Repository, *telemetryFakeDatabase) {
	t.Helper()
	database := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresRepository(database, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return repository, database
}

func TestNewPostgresRepositoryRejectsInvalidDependencies(t *testing.T) {
	var nilDatabase *telemetryFakeDatabase
	var typedNil TxBeginner = nilDatabase
	for _, test := range []struct {
		name    string
		db      TxBeginner
		timeout time.Duration
	}{
		{name: "nil database", timeout: time.Second},
		{name: "typed nil database", db: typedNil, timeout: time.Second},
		{name: "zero timeout", db: &telemetryFakeDatabase{}, timeout: 0},
		{name: "negative timeout", db: &telemetryFakeDatabase{}, timeout: -time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if repository, err := NewPostgresRepository(test.db, test.timeout); err == nil || repository != nil {
				t.Fatalf("NewPostgresRepository() = %v, %v; want nil repository and error", repository, err)
			}
		})
	}
}

func TestPersistRejectsInvalidMessageBeforeBegin(t *testing.T) {
	database := &telemetryFakeDatabase{}
	repository, err := NewPostgresRepository(database, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []*Message{nil, {}} {
		result, err := repository.Persist(context.Background(), message)
		if reasonCode(err) != ReasonInvalidPayload || result != (PersistResult{}) {
			t.Fatalf("Persist(%v) = %#v, %v; want invalid payload and zero result", message, result, err)
		}
	}
	if database.begins != 0 {
		t.Fatalf("BeginTx called %d times for invalid messages", database.begins)
	}
}

func TestPersistBeginErrorIsDependencyAndUnconfirmed(t *testing.T) {
	database := &telemetryFakeDatabase{beginErr: errors.New("begin failed")}
	repository, err := NewPostgresRepository(database, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.Persist(context.Background(), telemetryRepositoryTestMessage(t))
	if reasonCode(err) != ReasonDependencyUnavailable || result != (PersistResult{}) || !strings.Contains(err.Error(), "begin failed") {
		t.Fatalf("Persist() = %#v, %v; want dependency error with original cause and zero result", result, err)
	}
	if !database.deadline || database.options.IsoLevel != pgx.ReadCommitted {
		t.Fatalf("BeginTx deadline=%v isolation=%v; want bounded READ COMMITTED", database.deadline, database.options.IsoLevel)
	}
}

func TestPersistCommitFailuresRemainUnconfirmedForNewAndDuplicate(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		name := "new message"
		if duplicate {
			name = "duplicate message"
		}
		t.Run(name, func(t *testing.T) {
			tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), duplicate: duplicate, commitErr: errors.New("commit response lost"), copyCount: 3}
			repository, _ := newTelemetryFakeRepository(t, tx)
			result, err := repository.Persist(context.Background(), tx.message)
			if reasonCode(err) != ReasonCommitUnconfirmed || result != (PersistResult{}) || !strings.Contains(err.Error(), "commit response lost") {
				t.Fatalf("Persist() = %#v, %v; want commit-unconfirmed with zero result", result, err)
			}
			if tx.commitCalls != 1 || tx.rollbackCalls != 1 || !tx.rollbackValid {
				t.Fatalf("commit=%d rollback=%d boundedRollback=%v", tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
			}
		})
	}
}

func TestPersistCopyFailuresRollbackWithoutCommit(t *testing.T) {
	for _, test := range []struct {
		name      string
		copyErr   error
		copyCount int64
	}{
		{name: "CopyFrom error", copyErr: errors.New("copy failed"), copyCount: 0},
		{name: "count mismatch", copyCount: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyErr: test.copyErr, copyCount: test.copyCount}
			repository, _ := newTelemetryFakeRepository(t, tx)
			result, err := repository.Persist(context.Background(), tx.message)
			if reasonCode(err) != ReasonDependencyUnavailable || result != (PersistResult{}) {
				t.Fatalf("Persist() = %#v, %v; want dependency error and zero result", result, err)
			}
			if tx.copyRows != 3 || tx.commitCalls != 0 || tx.rollbackCalls != 1 || !tx.rollbackValid {
				t.Fatalf("copied rows=%d commit=%d rollback=%d boundedRollback=%v", tx.copyRows, tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
			}
		})
	}
}

func TestPersistCanceledOperationUsesIndependentRollbackContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), cancelQuery: cancel}
	repository, database := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(ctx, tx.message)
	if reasonCode(err) != ReasonDependencyUnavailable || result != (PersistResult{}) {
		t.Fatalf("Persist() = %#v, %v; want dependency error and zero result", result, err)
	}
	if !database.deadline || tx.rollbackCalls != 1 || !tx.rollbackValid || tx.commitCalls != 0 {
		t.Fatalf("deadline=%v rollback=%d boundedRollback=%v commit=%d", database.deadline, tx.rollbackCalls, tx.rollbackValid, tx.commitCalls)
	}
}

func TestPersistSuccessReturnsCommittedSampleCount(t *testing.T) {
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3}
	repository, database := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != CommitOutcomeCommitted || result.SamplesSaved != 3 || result.Identity.GatewayID != "gateway_001" || result.Identity.SensorID != "sensor_001" {
		t.Fatalf("Persist() = %#v; want committed identity and three samples", result)
	}
	if database.options.IsoLevel != pgx.ReadCommitted || tx.copyRows != 3 || tx.commitCalls != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("isolation=%v copied=%d commit=%d deferredRollback=%d", database.options.IsoLevel, tx.copyRows, tx.commitCalls, tx.rollbackCalls)
	}
}

var _ pgx.Tx = (*telemetryFakeTx)(nil)
