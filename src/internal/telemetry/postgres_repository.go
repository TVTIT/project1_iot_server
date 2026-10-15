package telemetry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
)

const telemetryRollbackTimeout = 5 * time.Second

type postgresRepository struct {
	database TxBeginner
	timeout  time.Duration
}

// NewPostgresRepository builds the transaction owner for telemetry persistence.
// timeout bounds connection acquisition, row locks, SQL, and commit.
func NewPostgresRepository(database TxBeginner, timeout time.Duration) (Repository, error) {
	if nilTxBeginner(database) {
		return nil, errors.New("telemetry database is required")
	}
	if timeout <= 0 {
		return nil, errors.New("telemetry database timeout must be positive")
	}
	return &postgresRepository{database: database, timeout: timeout}, nil
}

func nilTxBeginner(database TxBeginner) bool {
	if database == nil {
		return true
	}
	value := reflect.ValueOf(database)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (r *postgresRepository) Persist(ctx context.Context, message *Message) (PersistResult, error) {
	metadata, err := ledgerMetadata(message)
	if err != nil {
		return PersistResult{}, err
	}
	if ctx == nil {
		return PersistResult{}, &ReasonError{Code: ReasonInvalidPayload, Field: "context"}
	}
	operationCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	tx, err := r.database.BeginTx(operationCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PersistResult{}, dependencyError("begin telemetry transaction", err)
	}
	defer rollbackTelemetry(tx)

	resource, err := resolveTelemetryResource(operationCtx, tx, message)
	if err != nil {
		return PersistResult{}, err
	}
	disposition, err := insertTelemetryLedger(operationCtx, tx, message)
	if err != nil {
		return PersistResult{}, err
	}
	if disposition == ledgerDispositionDuplicate {
		if err := tx.Commit(operationCtx); err != nil {
			return PersistResult{}, commitUnconfirmedError(err)
		}
		return PersistResult{
			Identity: MessageIdentity{
				GatewayID: resource.GatewayID,
				SensorID:  resource.SensorID,
				BootID:    metadata.bootID.Bytes,
				MessageID: metadata.messageID.Bytes,
			},
			Outcome: CommitOutcomeDuplicateCommitted,
		}, nil
	}
	if disposition != ledgerDispositionNew {
		return PersistResult{}, &ReasonError{Code: ReasonDependencyUnavailable, Field: "ledger_outcome"}
	}

	copyCount, err := tx.CopyFrom(operationCtx,
		pgx.Identifier{"public", "telemetry"},
		[]string{"measured_at", "gateway_id", "sensor_id", "sequence_num", "value", "boot_id", "message_id", "sample_index"},
		&telemetryCopySource{samples: message.samples},
	)
	if err != nil {
		return PersistResult{}, dependencyError("copy telemetry samples", err)
	}
	if copyCount != int64(metadata.sampleCount) {
		return PersistResult{}, fmt.Errorf("telemetry sample copy count mismatch: %w", &ReasonError{Code: ReasonDependencyUnavailable})
	}
	if err := tx.Commit(operationCtx); err != nil {
		return PersistResult{}, commitUnconfirmedError(err)
	}
	return PersistResult{
		Identity: MessageIdentity{
			GatewayID: resource.GatewayID,
			SensorID:  resource.SensorID,
			BootID:    metadata.bootID.Bytes,
			MessageID: metadata.messageID.Bytes,
		},
		Outcome:      CommitOutcomeCommitted,
		SamplesSaved: int(copyCount),
	}, nil
}

func commitUnconfirmedError(err error) error {
	return fmt.Errorf("commit telemetry transaction: %w: %w", &ReasonError{Code: ReasonCommitUnconfirmed}, err)
}

func rollbackTelemetry(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

type telemetryCopySource struct {
	samples []SampleCandidate
	next    int
}

func (source *telemetryCopySource) Next() bool {
	return source.next < len(source.samples)
}

func (source *telemetryCopySource) Values() ([]any, error) {
	if !source.Next() {
		return nil, errors.New("telemetry CopyFrom source exhausted")
	}
	sample := source.samples[source.next]
	source.next++
	return []any{
		sample.MeasuredAt,
		sample.GatewayID,
		sample.SensorID,
		sample.SequenceNum,
		sample.Value,
		sample.BootID,
		sample.MessageID,
		sample.SampleIndex,
	}, nil
}

func (source *telemetryCopySource) Err() error { return nil }
