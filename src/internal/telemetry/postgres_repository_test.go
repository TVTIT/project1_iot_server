package telemetry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type telemetryFakeDatabase struct {
	tx       *telemetryFakeTx
	txs      []*telemetryFakeTx
	txIndex  int
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
	if len(database.txs) > 0 {
		if database.txIndex >= len(database.txs) {
			return nil, errors.New("unexpected extra transaction")
		}
		tx := database.txs[database.txIndex]
		database.txIndex++
		return tx, nil
	}
	return database.tx, nil
}

type telemetryFakeTx struct {
	pgx.Tx
	message                *Message
	duplicate              bool
	duplicateTimestamp     *time.Time
	duplicateNullTimestamp bool
	duplicateSensor        string
	duplicateHash          string
	ledgerInsertArgs       []any
	commitErr              error
	rollbackErr            error
	copyErr                error
	copyCount              int64
	copyRows               int
	commitCalls            int
	rollbackCalls          int
	rollbackValid          bool
	queryCalls             int
	cancelQuery            context.CancelFunc

	sensorState        []byte
	sensorVersion      int64
	sensorLastReported *time.Time
	sensorReadErr      error
	execCalls          int
	execQuery          string
	execArgs           []any
	execRows           int64
	execErr            error

	// execs records every Exec in call order so tests can assert the
	// reported-state/temporal ordering. sensorExec and temporalExec override the
	// default execRows/execErr for that specific statement when set.
	execs            []telemetryFakeExec
	sensorExec       *telemetryFakeExecConfig
	temporalExec     *telemetryFakeExecConfig
	batchLedger      map[string]ledgerMessage
	batchMessages    map[string]*Message
	batchOverlap     bool
	batchInput       []*Message
	batchInsertIndex int
}

// telemetryFakeExec records one Exec call for order assertions.
type telemetryFakeExec struct {
	query string
	args  []any
}

// telemetryFakeExecConfig overrides the fake's default Exec result for one
// statement.
type telemetryFakeExecConfig struct {
	rows int64
	err  error
}

func (tx *telemetryFakeTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	tx.queryCalls++
	if tx.cancelQuery != nil {
		tx.cancelQuery()
		tx.cancelQuery = nil
		return telemetryFakeRow{err: ctx.Err()}
	}
	switch {
	case strings.Contains(query, "FROM public.gateways") && strings.Contains(query, "FOR SHARE"):
		return telemetryFakeRow{values: []any{"gateway_001"}}
	case strings.Contains(query, "SELECT EXISTS") && strings.Contains(query, "public.gateways"):
		return telemetryFakeRow{values: []any{true}}
	case strings.Contains(query, "FROM public.sensors"):
		unit := "C"
		return telemetryFakeRow{values: []any{&unit}}
	case strings.Contains(query, "gateway_entity.id"):
		return telemetryFakeRow{values: []any{uuid.New(), uuid.New(), true}}
	case strings.Contains(query, "FROM public.twin_entities") && strings.Contains(query, "FOR UPDATE"):
		return telemetryFakeRow{values: []any{uuid.New()}}
	case strings.Contains(query, "FROM public.twin_states"):
		if strings.Contains(query, "SELECT entity_id") && strings.Contains(query, "FOR UPDATE") {
			return telemetryFakeRow{values: []any{uuid.New()}}
		}
		if tx.sensorReadErr != nil {
			return telemetryFakeRow{err: tx.sensorReadErr}
		}
		return telemetryFakeRow{values: []any{tx.sensorState, tx.sensorVersion, tx.sensorLastReported}}
	case strings.Contains(query, "INSERT INTO public.processed_messages"):
		tx.ledgerInsertArgs = append([]any(nil), args...)
		if tx.batchLedger != nil {
			key := fmt.Sprintf("%v/%v", args[0], args[1])
			if _, exists := tx.batchLedger[key]; exists {
				return telemetryFakeRow{err: pgx.ErrNoRows}
			}
			metadata, err := ledgerMetadata(tx.messageForLedger(args))
			if err != nil {
				return telemetryFakeRow{err: err}
			}
			tx.batchLedger[key] = metadata
			return telemetryFakeRow{values: []any{args[0]}}
		}
		if tx.duplicate {
			return telemetryFakeRow{err: pgx.ErrNoRows}
		}
		return telemetryFakeRow{values: []any{"gateway_001"}}
	case strings.Contains(query, "SELECT EXISTS") && strings.Contains(query, "processed_messages"):
		return telemetryFakeRow{values: []any{tx.batchOverlap}}
	case strings.Contains(query, "SELECT payload_hash, source_topic"):
		message := tx.message
		if tx.batchLedger != nil {
			key := fmt.Sprintf("%v/%v", args[0], args[1])
			metadata := tx.batchLedger[key]
			return telemetryFakeRow{values: []any{
				pgtype.Text{String: metadata.payloadHash, Valid: true}, pgtype.Text{String: metadata.topic.SourceTopic, Valid: true},
				pgtype.Text{String: metadata.sensorID, Valid: true}, metadata.bootID,
				pgtype.Int8{Int64: metadata.firstSample.SequenceNum, Valid: true}, pgtype.Int8{Int64: metadata.lastSample.SequenceNum, Valid: true},
				pgtype.Int4{Int32: metadata.sampleCount, Valid: true},
				pgtype.Timestamptz{Time: metadata.timestamp, Valid: true},
			}}
		}
		metadata, err := ledgerMetadata(message)
		if err != nil {
			return telemetryFakeRow{err: err}
		}
		if tx.duplicateTimestamp != nil {
			metadata.timestamp = *tx.duplicateTimestamp
		}
		if tx.duplicateSensor != "" {
			metadata.sensorID = tx.duplicateSensor
		}
		if tx.duplicateHash != "" {
			metadata.payloadHash = tx.duplicateHash
		}
		return telemetryFakeRow{values: []any{
			pgtype.Text{String: metadata.payloadHash, Valid: true},
			pgtype.Text{String: metadata.topic.SourceTopic, Valid: true},
			pgtype.Text{String: metadata.sensorID, Valid: true},
			metadata.bootID,
			pgtype.Int8{Int64: metadata.firstSample.SequenceNum, Valid: true},
			pgtype.Int8{Int64: metadata.lastSample.SequenceNum, Valid: true},
			pgtype.Int4{Int32: metadata.sampleCount, Valid: true},
			pgtype.Timestamptz{Time: metadata.timestamp, Valid: !tx.duplicateNullTimestamp},
		}}
	default:
		return telemetryFakeRow{err: errors.New("unexpected query")}
	}
}

func (tx *telemetryFakeTx) messageForLedger(args []any) *Message {
	if tx.batchInsertIndex < len(tx.batchInput) {
		message := tx.batchInput[tx.batchInsertIndex]
		tx.batchInsertIndex++
		return message
	}
	if len(args) > 1 {
		if messageID, ok := args[1].(pgtype.UUID); ok && messageID.Valid {
			if message := tx.batchMessages[uuid.UUID(messageID.Bytes).String()]; message != nil {
				return message
			}
		}
	}
	return tx.message
}

func (tx *telemetryFakeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	rows, execErr := tx.execRows, tx.execErr
	switch {
	case query == sensorReportedStateAdvanceSQL && tx.sensorExec != nil:
		rows, execErr = tx.sensorExec.rows, tx.sensorExec.err
	case query == sensorSampleIntervalTemporalInsertSQL && tx.temporalExec != nil:
		rows, execErr = tx.temporalExec.rows, tx.temporalExec.err
	}
	tx.execCalls++
	tx.execQuery = query
	tx.execArgs = args
	tx.execs = append(tx.execs, telemetryFakeExec{query: query, args: args})
	if execErr != nil {
		return pgconn.CommandTag{}, execErr
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rows)), nil
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
	return tx.rollbackErr
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

func TestPersistBatchEmptyAndFiniteBounds(t *testing.T) {
	database := &telemetryFakeDatabase{tx: &telemetryFakeTx{}}
	repository, err := NewPostgresBatchRepository(database, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), nil)
	if len(results) != 0 || database.begins != 0 {
		t.Fatalf("empty batch results=%v begins=%d; want empty and no transaction", results, database.begins)
	}
	group := []*Message{telemetryRepositoryTestMessage(t), telemetryRepositoryTestMessage(t), telemetryRepositoryTestMessage(t)}
	results = repository.PersistBatch(context.Background(), group)
	if len(results) != len(group) || database.begins != 0 {
		t.Fatalf("oversized batch results=%d begins=%d; want per-input bounded error without transaction", len(results), database.begins)
	}
	for index, result := range results {
		if result.InputIndex != index || result.Outcome != 0 || result.Err == nil {
			t.Fatalf("result[%d]=%#v; want indexed unconfirmed error", index, result)
		}
	}
	results = repository.PersistBatch(context.Background(), []*Message{nil})
	if len(results) != 1 || results[0].InputIndex != 0 || results[0].Err == nil || results[0].Outcome != 0 || database.begins != 0 {
		t.Fatalf("malformed batch results=%#v begins=%d; want one unconfirmed error without transaction", results, database.begins)
	}
}

func TestPersistBatchCopiesGroupOnceAndMapsInputs(t *testing.T) {
	first := telemetryRepositoryTestMessage(t)
	second := batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())
	third := batchTestMessage(t, "sensor_001", "2026-08-21T10:15:02Z", 12520, uuid.NewString())
	tx := &telemetryFakeTx{copyCount: 9, execRows: 1}
	database := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(database, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{first, second, third})
	if database.begins != 1 || tx.commitCalls != 1 || tx.copyRows != 9 {
		t.Fatalf("begins=%d commits=%d copied=%d; want one transaction and nine streamed samples", database.begins, tx.commitCalls, tx.copyRows)
	}
	for index, result := range results {
		if result.InputIndex != index || result.Outcome != CommitOutcomeCommitted || result.SamplesSaved != 3 || result.Identity != identityFromMessage([]*Message{first, second, third}[index]) || result.Err != nil {
			t.Fatalf("result[%d]=%#v; want confirmed input-associated commit", index, result)
		}
	}
}

func TestPersistBatchAllDuplicateSkipsCopy(t *testing.T) {
	message := telemetryRepositoryTestMessage(t)
	tx := &telemetryFakeTx{message: message, duplicate: true}
	database := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(database, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{message, message})
	if database.begins != 1 || tx.commitCalls != 1 || tx.copyRows != 0 {
		t.Fatalf("begins=%d commits=%d copied=%d; want committed duplicate-only tx and no COPY", database.begins, tx.commitCalls, tx.copyRows)
	}
	for index, result := range results {
		if result.InputIndex != index || result.Outcome != CommitOutcomeDuplicateCommitted || result.SamplesSaved != 0 || result.Identity != identityFromMessage(message) || result.Err != nil {
			t.Fatalf("result[%d]=%#v; want confirmed zero-sample duplicate", index, result)
		}
	}
}

func TestPersistBatchExactDuplicateWithinGroupSharesLedgerTransaction(t *testing.T) {
	message := telemetryRepositoryTestMessage(t)
	tx := &telemetryFakeTx{message: message, batchLedger: make(map[string]ledgerMessage), copyCount: 3, execRows: 1}
	database := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(database, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{message, message})
	if database.begins != 1 || tx.commitCalls != 1 || tx.copyRows != 3 {
		t.Fatalf("begins=%d commits=%d copied=%d; want one insert plus same-tx duplicate and one message COPY", database.begins, tx.commitCalls, tx.copyRows)
	}
	if results[0].Outcome != CommitOutcomeCommitted || results[0].SamplesSaved != 3 || results[1].Outcome != CommitOutcomeDuplicateCommitted || results[1].SamplesSaved != 0 {
		t.Fatalf("results=%#v; want first new and second exact duplicate", results)
	}
}

func TestPersistBatchConflictsDoNotReturnConfirmedResults(t *testing.T) {
	first := telemetryRepositoryTestMessage(t)
	changedPayload := batchTestMessage(t, "sensor_001", "2026-08-21T10:15:00Z", 12501, first.batch.MessageID.String())
	overlap := batchTestMessage(t, "sensor_001", "2026-08-21T10:15:01Z", 12502, uuid.NewString())
	for _, test := range []struct {
		name string
		msgs []*Message
		fake func(*telemetryFakeTx)
	}{
		{name: "same ID different payload", msgs: []*Message{first, changedPayload}},
		{name: "same ID different provenance", msgs: []*Message{first, batchTestMessage(t, "sensor_002", "2026-08-21T10:15:00Z", 12501, first.batch.MessageID.String())}},
		{name: "overlap with different first sequence", msgs: []*Message{first, overlap}, fake: func(tx *telemetryFakeTx) { tx.batchOverlap = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &telemetryFakeTx{copyCount: 6, execRows: 1, batchLedger: make(map[string]ledgerMessage), batchInput: test.msgs}
			for _, message := range test.msgs {
				meta, err := ledgerMetadata(message)
				if err != nil {
					t.Fatal(err)
				}
				if tx.batchMessages == nil {
					tx.batchMessages = make(map[string]*Message)
				}
				tx.batchMessages[meta.messageID.String()] = message
			}
			if test.fake != nil {
				test.fake(tx)
			}
			database := &telemetryFakeDatabase{tx: tx, txs: []*telemetryFakeTx{tx}}
			repository, err := NewPostgresBatchRepository(database, time.Second, 4)
			if err != nil {
				t.Fatal(err)
			}
			results := repository.PersistBatch(context.Background(), test.msgs)
			for index, result := range results {
				if result.Outcome != 0 || result.Err == nil || result.InputIndex != index {
					t.Fatalf("result[%d]=%#v; conflict must not escape as confirmed", index, result)
				}
			}
			if tx.commitCalls != 0 || tx.copyRows != 0 || tx.rollbackCalls != 1 {
				t.Fatalf("commit=%d copyRows=%d rollback=%d; want rollback without COPY/commit", tx.commitCalls, tx.copyRows, tx.rollbackCalls)
			}
		})
	}
}

func TestPersistBatchConflictFallsBackPerInputAfterRollback(t *testing.T) {
	valid := telemetryRepositoryTestMessage(t)
	poison := batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())
	groupTx := &telemetryFakeTx{copyCount: 6, execRows: 1, batchLedger: make(map[string]ledgerMessage), batchMessages: map[string]*Message{}, batchInput: []*Message{valid, poison}}
	groupTx.batchOverlap = true
	validTx := &telemetryFakeTx{message: valid, copyCount: 3, execRows: 1}
	poisonTx := &telemetryFakeTx{message: poison, batchOverlap: true}
	db := &telemetryFakeDatabase{txs: []*telemetryFakeTx{groupTx, validTx, poisonTx}}
	repository, err := NewPostgresBatchRepository(db, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{valid, poison})
	if db.begins != 3 || groupTx.rollbackCalls != 1 || validTx.commitCalls != 1 || poisonTx.commitCalls != 0 {
		t.Fatalf("begins=%d group rollback=%d valid commits=%d poison commits=%d results=%#v errs=%v/%v", db.begins, groupTx.rollbackCalls, validTx.commitCalls, poisonTx.commitCalls, results, results[0].Err, results[1].Err)
	}
	if results[0].InputIndex != 0 || results[0].Identity != identityFromMessage(valid) || results[0].Outcome != CommitOutcomeCommitted || results[0].SamplesSaved != 3 || results[0].Err != nil {
		t.Fatalf("valid fallback result = %#v", results[0])
	}
	if results[1].InputIndex != 1 || results[1].Identity != identityFromMessage(poison) || results[1].Outcome != 0 || results[1].SamplesSaved != 0 || reasonCode(results[1].Err) != ReasonSequenceOverlap {
		t.Fatalf("poison result = %#v", results[1])
	}
}

func TestPersistBatchCommitUnconfirmedSurvivesAlreadyClosedRollback(t *testing.T) {
	messages := []*Message{telemetryRepositoryTestMessage(t), batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())}
	tx := &telemetryFakeTx{copyCount: 6, execRows: 1, commitErr: errors.New("commit response lost"), rollbackErr: pgx.ErrTxClosed, batchLedger: make(map[string]ledgerMessage), batchMessages: make(map[string]*Message), batchInput: messages}
	for _, message := range messages {
		meta, _ := ledgerMetadata(message)
		tx.batchMessages[meta.messageID.String()] = message
	}
	db := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(db, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), messages)
	if db.begins != 1 || tx.commitCalls != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("begins=%d commit=%d rollback=%d; want no fallback after uncertain commit", db.begins, tx.commitCalls, tx.rollbackCalls)
	}
	for index, result := range results {
		if result.InputIndex != index || result.Outcome != 0 || result.SamplesSaved != 0 || reasonCode(result.Err) != ReasonCommitUnconfirmed || !strings.Contains(result.Err.Error(), "commit response lost") {
			t.Fatalf("result[%d]=%#v; want preserved commit-unconfirmed error with zero outcome", index, result)
		}
	}
}

func TestPersistBatchRollbackFailureBlocksConflictFallback(t *testing.T) {
	message := telemetryRepositoryTestMessage(t)
	tx := &telemetryFakeTx{batchOverlap: true, rollbackErr: errors.New("rollback failed"), batchLedger: make(map[string]ledgerMessage), batchMessages: map[string]*Message{}, batchInput: []*Message{message}}
	db := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(db, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{message})
	if db.begins != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("begins=%d rollback=%d; rollback failure must prevent fallback", db.begins, tx.rollbackCalls)
	}
	if results[0].Outcome != 0 || results[0].Err == nil || !strings.Contains(results[0].Err.Error(), string(ReasonSequenceOverlap)) || !strings.Contains(results[0].Err.Error(), "rollback failed") || reasonCode(results[0].Err) != ReasonDependencyUnavailable {
		t.Fatalf("result=%#v err=%v; want rollback dependency failure retaining conflict context and no outcome", results[0], results[0].Err)
	}
}

func TestBatchFallbackEligibilityIsExplicitAndConservative(t *testing.T) {
	for _, test := range []struct {
		code ReasonCode
		want bool
	}{
		{ReasonMessageConflict, true}, {ReasonSequenceOverlap, true}, {ReasonUnknownGateway, true},
		{ReasonUnknownSensor, true}, {ReasonResourceGraphIncomplete, true}, {ReasonLegacyProvenanceUnknown, true},
		{ReasonDependencyUnavailable, false}, {ReasonCommitUnconfirmed, false}, {ReasonInvalidPayload, false},
	} {
		if got := batchFallbackEligible(&ReasonError{Code: test.code}); got != test.want {
			t.Errorf("batchFallbackEligible(%q)=%v want %v", test.code, got, test.want)
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, errors.New("deadlock detected"), errors.New("unknown")} {
		if batchFallbackEligible(err) {
			t.Errorf("fallback unexpectedly eligible for %v", err)
		}
	}
}

func TestPersistBatchFailuresReturnNoConfirmedOutcomes(t *testing.T) {
	for _, test := range []struct {
		name         string
		configure    func(*telemetryFakeDatabase, *telemetryFakeTx)
		wantExec     int
		wantCopyRows int
	}{
		{name: "begin", configure: func(db *telemetryFakeDatabase, _ *telemetryFakeTx) { db.beginErr = errors.New("begin failed") }},
		{name: "copy error", configure: func(_ *telemetryFakeDatabase, tx *telemetryFakeTx) { tx.copyErr = errors.New("copy failed") }, wantCopyRows: 6},
		{name: "copy count mismatch", configure: func(_ *telemetryFakeDatabase, tx *telemetryFakeTx) { tx.copyCount = 5 }, wantCopyRows: 6},
		{name: "reported state", configure: func(_ *telemetryFakeDatabase, tx *telemetryFakeTx) {
			tx.sensorExec = &telemetryFakeExecConfig{err: errors.New("state failed")}
		}, wantCopyRows: 6, wantExec: 1},
		{name: "temporal", configure: func(_ *telemetryFakeDatabase, tx *telemetryFakeTx) {
			tx.temporalExec = &telemetryFakeExecConfig{err: errors.New("temporal failed")}
		}, wantCopyRows: 6, wantExec: 2},
		{name: "commit ambiguity", configure: func(_ *telemetryFakeDatabase, tx *telemetryFakeTx) { tx.commitErr = errors.New("commit response lost") }, wantCopyRows: 6, wantExec: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			messages := []*Message{telemetryRepositoryTestMessage(t), batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())}
			tx := &telemetryFakeTx{copyCount: 6, execRows: 1, batchLedger: make(map[string]ledgerMessage), batchMessages: make(map[string]*Message), batchInput: messages}
			for _, message := range messages {
				meta, _ := ledgerMetadata(message)
				tx.batchMessages[meta.messageID.String()] = message
			}
			db := &telemetryFakeDatabase{tx: tx}
			test.configure(db, tx)
			repository, err := NewPostgresBatchRepository(db, time.Second, 4)
			if err != nil {
				t.Fatal(err)
			}
			results := repository.PersistBatch(context.Background(), messages)
			for index, result := range results {
				if result.InputIndex != index || result.Outcome != 0 || result.Err == nil {
					t.Fatalf("result[%d]=%#v; failed group must have no confirmed outcome", index, result)
				}
			}
			if tx.commitCalls != 0 && test.name != "commit ambiguity" {
				t.Fatalf("commit=%d after failed phase", tx.commitCalls)
			}
			if tx.copyRows != test.wantCopyRows || len(tx.execs) != test.wantExec {
				t.Fatalf("copyRows=%d execs=%d; want %d/%d, later writes must stop", tx.copyRows, len(tx.execs), test.wantCopyRows, test.wantExec)
			}
			wantRollback := 1
			if test.name == "begin" {
				wantRollback = 0
			}
			if tx.rollbackCalls != wantRollback {
				t.Fatalf("rollback=%d; want %d deferred rollback attempt(s)", tx.rollbackCalls, wantRollback)
			}
			if test.name == "begin" || test.name == "commit ambiguity" {
				if db.begins != 1 {
					t.Fatalf("begins=%d; non-fallback failure must not start individual attempts", db.begins)
				}
			}
		})
	}
}

func TestPersistBatchPreservesSameSensorInputOrderAndTemporalPerMessage(t *testing.T) {
	first := telemetryRepositoryTestMessage(t)
	second := batchTestMessage(t, "sensor_001", "2026-08-21T10:15:01Z", 12510, uuid.NewString())
	tx := &telemetryFakeTx{copyCount: 6, execRows: 1, batchLedger: make(map[string]ledgerMessage), batchMessages: make(map[string]*Message), batchInput: []*Message{first, second}}
	for _, message := range []*Message{first, second} {
		meta, _ := ledgerMetadata(message)
		tx.batchMessages[meta.messageID.String()] = message
	}
	db := &telemetryFakeDatabase{tx: tx}
	repository, err := NewPostgresBatchRepository(db, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{first, second})
	if results[0].InputIndex != 0 || results[1].InputIndex != 1 || results[0].Identity.MessageID != identityFromMessage(first).MessageID || results[1].Identity.MessageID != identityFromMessage(second).MessageID {
		t.Fatalf("input association lost: %#v", results)
	}
	var temporalIDs []uuid.UUID
	var reportedTimes []time.Time
	for _, call := range tx.execs {
		if call.query == sensorSampleIntervalTemporalInsertSQL {
			temporalIDs = append(temporalIDs, call.args[4].(uuid.UUID))
		}
		if call.query == sensorReportedStateAdvanceSQL {
			reportedTimes = append(reportedTimes, call.args[2].(time.Time))
		}
	}
	if !reflect.DeepEqual(temporalIDs, []uuid.UUID{first.batch.MessageID, second.batch.MessageID}) {
		t.Fatalf("temporal message order=%v", temporalIDs)
	}
	firstLast, _ := first.LastSample()
	secondLast, _ := second.LastSample()
	if !reflect.DeepEqual(reportedTimes, []time.Time{firstLast.MeasuredAt, secondLast.MeasuredAt}) {
		t.Fatalf("reported-state application order=%v", reportedTimes)
	}
}

func batchTestMessage(t *testing.T, sensor, measuredAt string, sequence int64, messageID string) *Message {
	t.Helper()
	payload := messageTestBatchPayload(t, measuredAt, sequence, []float64{4, 5, 6})
	payload = []byte(strings.ReplaceAll(string(payload), `"sensor_id":"sensor_001"`, `"sensor_id":"`+sensor+`"`))
	payload = []byte(strings.ReplaceAll(string(payload), decodeTestMessageID, messageID))
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/gateway_001/telemetry/"+sensor, payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestPersistCommitFailuresRemainUnconfirmedForNewAndDuplicate(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		name := "new message"
		if duplicate {
			name = "duplicate message"
		}
		t.Run(name, func(t *testing.T) {
			tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), duplicate: duplicate, commitErr: errors.New("commit response lost"), copyCount: 3, execRows: 1}
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
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, execRows: 1}
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

func TestPersistReportedAndTemporalFailuresRollback(t *testing.T) {
	for _, test := range []struct {
		name         string
		sensorExec   *telemetryFakeExecConfig
		temporalExec *telemetryFakeExecConfig
	}{
		{name: "reported state", sensorExec: &telemetryFakeExecConfig{err: errors.New("reported state failed")}},
		{name: "temporal", temporalExec: &telemetryFakeExecConfig{err: errors.New("temporal failed")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &telemetryFakeTx{
				message:      telemetryRepositoryTestMessage(t),
				copyCount:    3,
				execRows:     1,
				sensorExec:   test.sensorExec,
				temporalExec: test.temporalExec,
			}
			repository, _ := newTelemetryFakeRepository(t, tx)
			result, err := repository.Persist(context.Background(), tx.message)
			if reasonCode(err) != ReasonDependencyUnavailable || result != (PersistResult{}) {
				t.Fatalf("Persist() = %#v, %v; want dependency error and zero result", result, err)
			}
			if tx.commitCalls != 0 || tx.rollbackCalls != 1 || !tx.rollbackValid {
				t.Fatalf("commit=%d rollback=%d boundedRollback=%v", tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
			}
		})
	}
}

func TestPersistCommittedDuplicateReturnsIdentityWithoutSamples(t *testing.T) {
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), duplicate: true}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != CommitOutcomeDuplicateCommitted || result.SamplesSaved != 0 || result.Identity.GatewayID != "gateway_001" || result.Identity.SensorID != "sensor_001" {
		t.Fatalf("Persist() = %#v; want confirmed duplicate identity and no saved samples", result)
	}
	if tx.commitCalls != 1 || tx.copyRows != 0 || tx.execCalls != 0 {
		t.Fatalf("commit=%d copied=%d execs=%d; duplicate must commit without writes", tx.commitCalls, tx.copyRows, tx.execCalls)
	}
}

var _ pgx.Tx = (*telemetryFakeTx)(nil)
