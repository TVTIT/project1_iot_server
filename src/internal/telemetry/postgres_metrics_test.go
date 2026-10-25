package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestRepositoryOperationMetricsSingleAndBatchFallback(t *testing.T) {
	collector := &PersistenceMetrics{}
	message := telemetryRepositoryTestMessage(t)
	first := &telemetryFakeTx{message: message, copyCount: 3, execRows: 1}
	repo, err := NewPostgresRepositoryWithMetrics(&telemetryFakeDatabase{tx: first}, time.Second, collector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Persist(context.Background(), message); err != nil {
		t.Fatal(err)
	}

	poison := batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())
	groupTx := &telemetryFakeTx{copyCount: 6, execRows: 1, batchLedger: make(map[string]ledgerMessage), batchMessages: map[string]*Message{}, batchInput: []*Message{message, poison}, batchOverlap: true}
	validTx := &telemetryFakeTx{message: message, copyCount: 3, execRows: 1}
	poisonTx := &telemetryFakeTx{message: poison, batchOverlap: true}
	db := &telemetryFakeDatabase{txs: []*telemetryFakeTx{groupTx, validTx, poisonTx}}
	batch, err := NewPostgresBatchRepositoryWithMetrics(db, time.Second, 4, collector)
	if err != nil {
		t.Fatal(err)
	}
	batch.PersistBatch(context.Background(), []*Message{message, poison})

	s := collector.Snapshot()
	if s.SingleAttempts != 3 || s.GroupAttempts != 1 || s.FallbackAttempts != 2 {
		t.Fatalf("operation attempts: %+v", s)
	}
	if s.BeginAttempts != 4 || s.BeginSucceeded != 4 || s.ConfirmedCommits != 2 || s.RollbackAttempts != 2 || s.ConfirmedRollbacks != 2 {
		t.Fatalf("transaction metrics: %+v", s)
	}
	if s.CopyCalls != 2 || s.CopyReturnedRows != 6 || s.ConfirmedNewRows != 6 {
		t.Fatalf("COPY metrics: %+v", s)
	}
	if s.GroupMembers != 2 || s.GroupRows != 6 || s.GroupPayloadBytes == 0 {
		t.Fatalf("group metrics: %+v", s)
	}
}

func TestRepositoryOperationMetricsCommitUnconfirmedClosedRollback(t *testing.T) {
	collector := &PersistenceMetrics{}
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, execRows: 1, commitErr: context.DeadlineExceeded}
	repo, err := NewPostgresRepositoryWithMetrics(&telemetryFakeDatabase{tx: tx}, time.Second, collector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Persist(context.Background(), tx.message); err == nil {
		t.Fatal("expected commit error")
	}
	s := collector.Snapshot()
	if s.CommitUnconfirmed != 1 || s.RollbackAttempts != 1 || s.ConfirmedRollbacks != 1 {
		t.Fatalf("metrics: %+v", s)
	}

	tx = &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, execRows: 1, commitErr: context.DeadlineExceeded, rollbackErr: pgx.ErrTxClosed}
	repo, err = NewPostgresRepositoryWithMetrics(&telemetryFakeDatabase{tx: tx}, time.Second, collector)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = repo.Persist(context.Background(), tx.message)
	s = collector.Snapshot()
	if s.RollbackAttempts != 1 || s.ConfirmedRollbacks != 1 {
		t.Fatalf("closed tx counted as rollback: %+v", s)
	}
}

func TestRepositoryOperationMetricsConfirmedGroupAndCopyRows(t *testing.T) {
	collector := &PersistenceMetrics{}
	first := telemetryRepositoryTestMessage(t)
	second := batchTestMessage(t, "sensor_002", "2026-08-21T10:15:01Z", 12510, uuid.NewString())
	tx := &telemetryFakeTx{copyCount: 6, execRows: 1}
	repository, err := NewPostgresBatchRepositoryWithMetrics(&telemetryFakeDatabase{tx: tx}, time.Second, 4, collector)
	if err != nil {
		t.Fatal(err)
	}
	results := repository.PersistBatch(context.Background(), []*Message{first, second})
	for i, result := range results {
		if result.Err != nil || result.Outcome != CommitOutcomeCommitted {
			t.Fatalf("result[%d]=%#v", i, result)
		}
	}
	s := collector.Snapshot()
	if s.GroupAttempts != 1 || s.GroupMembers != 2 || s.GroupRows != 6 || s.GroupPayloadBytes != uint64(len(first.rawPayload)+len(second.rawPayload)) {
		t.Fatalf("group: %+v", s)
	}
	if s.BeginAttempts != 1 || s.BeginSucceeded != 1 || s.ConfirmedCommits != 1 || s.CopyCalls != 1 || s.CopyReturnedRows != 6 || s.ConfirmedNewRows != 6 {
		t.Fatalf("operations: %+v", s)
	}
}
