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
	d := s.Durations
	if d.Begin.Count != 1 || d.GatewayLock.Count != 1 || d.SensorLock.Count != 2 || d.GraphLookup.Count != 2 || d.EntityLock.Count != 2 || d.StateLock.Count != 2 || d.Ledger.Count != 2 || d.Copy.Count != 1 || d.ReportedState.Count != 2 || d.Temporal.Count != 2 || d.Commit.Count != 1 {
		t.Fatalf("group duration call counts: %+v", d)
	}
}

func TestRepositoryPersistenceDurationsRecordExecutedStages(t *testing.T) {
	message := telemetryRepositoryTestMessage(t)
	collector := &PersistenceMetrics{}
	tx := &telemetryFakeTx{message: message, copyCount: 3, execRows: 1}
	repo, err := NewPostgresRepositoryWithMetrics(&telemetryFakeDatabase{tx: tx}, time.Second, collector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Persist(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	d := collector.Snapshot().Durations
	for name, got := range map[string]uint64{
		"begin": d.Begin.Count, "gateway_lock": d.GatewayLock.Count,
		"sensor_lock": d.SensorLock.Count, "graph_lookup": d.GraphLookup.Count,
		"entity_lock": d.EntityLock.Count, "state_lock": d.StateLock.Count,
		"ledger": d.Ledger.Count, "copy": d.Copy.Count,
		"reported_state": d.ReportedState.Count, "temporal": d.Temporal.Count,
		"commit": d.Commit.Count,
	} {
		if got != 1 {
			t.Errorf("%s duration count=%d, want 1", name, got)
		}
	}
}

func TestRepositoryPersistenceDurationsIncludeFailedBeginAndNilCollector(t *testing.T) {
	collector := &PersistenceMetrics{}
	repo, err := NewPostgresRepositoryWithMetrics(&telemetryFakeDatabase{beginErr: context.DeadlineExceeded}, time.Second, collector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Persist(context.Background(), telemetryRepositoryTestMessage(t)); err == nil {
		t.Fatal("expected begin failure")
	}
	if got := collector.Snapshot().Durations.Begin.Count; got != 1 {
		t.Fatalf("failed BeginTx duration count=%d, want 1", got)
	}
	nilMetricsRepo, err := NewPostgresRepository(&telemetryFakeDatabase{tx: &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, execRows: 1}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nilMetricsRepo.Persist(context.Background(), telemetryRepositoryTestMessage(t)); err != nil {
		t.Fatalf("nil-metrics persistence: %v", err)
	}
	var emptyMetrics PersistenceMetrics
	if emptyMetrics.Snapshot().Durations.Begin.Count != 0 {
		t.Fatal("zero-value metrics should have no duration observations")
	}
}
