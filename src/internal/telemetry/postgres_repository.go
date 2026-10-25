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
	database         TxBeginner
	timeout          time.Duration
	maxBatchMessages int
	metrics          *PersistenceMetrics
}

const defaultTelemetryBatchMessages = 32

// NewPostgresRepository builds the transaction owner for telemetry persistence.
// timeout bounds connection acquisition, row locks, SQL, and commit.
func NewPostgresRepository(database TxBeginner, timeout time.Duration) (Repository, error) {
	return newPostgresRepository(database, timeout, nil)
}

// NewPostgresRepositoryWithMetrics shares an optional collector with other repository
// entrypoints; nil disables observation and never changes persistence behavior.
func NewPostgresRepositoryWithMetrics(database TxBeginner, timeout time.Duration, metrics *PersistenceMetrics) (Repository, error) {
	return newPostgresRepository(database, timeout, metrics)
}

func newPostgresRepository(database TxBeginner, timeout time.Duration, metrics *PersistenceMetrics) (Repository, error) {
	if nilTxBeginner(database) {
		return nil, errors.New("telemetry database is required")
	}
	if timeout <= 0 {
		return nil, errors.New("telemetry database timeout must be positive")
	}
	return &postgresRepository{database: database, timeout: timeout, maxBatchMessages: defaultTelemetryBatchMessages, metrics: metrics}, nil
}

// NewPostgresBatchRepository exposes the same PostgreSQL transaction owner as
// Repository.Persist, plus a finite grouped entrypoint. maxMessages is a hard
// repository bound; worker target rows/bytes are a separate flush policy and
// must not reject an otherwise valid standalone message sent to Persist.
func NewPostgresBatchRepository(database TxBeginner, timeout time.Duration, maxMessages int) (BatchRepository, error) {
	return newPostgresBatchRepository(database, timeout, maxMessages, nil)
}

// NewPostgresBatchRepositoryWithMetrics constructs the grouped entrypoint using
// the supplied collector. Use the same pointer for baseline and fallback paths.
func NewPostgresBatchRepositoryWithMetrics(database TxBeginner, timeout time.Duration, maxMessages int, metrics *PersistenceMetrics) (BatchRepository, error) {
	return newPostgresBatchRepository(database, timeout, maxMessages, metrics)
}

func newPostgresBatchRepository(database TxBeginner, timeout time.Duration, maxMessages int, metrics *PersistenceMetrics) (BatchRepository, error) {
	if maxMessages <= 0 {
		return nil, errors.New("telemetry batch message bound must be positive")
	}
	repository, err := newPostgresRepository(database, timeout, metrics)
	if err != nil {
		return nil, err
	}
	postgres := repository.(*postgresRepository)
	postgres.maxBatchMessages = maxMessages
	return postgres, nil
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
	if r.metrics != nil {
		r.metrics.singleAttempts.Add(1)
	}
	metadata, err := ledgerMetadata(message)
	if err != nil {
		return PersistResult{}, err
	}
	if ctx == nil {
		return PersistResult{}, &ReasonError{Code: ReasonInvalidPayload, Field: "context"}
	}
	operationCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if r.metrics != nil {
		r.metrics.beginAttempts.Add(1)
	}
	tx, err := r.database.BeginTx(operationCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PersistResult{}, dependencyError("begin telemetry transaction", err)
	}
	if r.metrics != nil {
		r.metrics.beginSucceeded.Add(1)
	}
	committed := false
	// Cleanup is best-effort; preserve the primary persistence/commit error.
	defer func() { _ = r.rollback(tx, !committed) }()

	result, err := persistTelemetryMessage(operationCtx, tx, message, metadata, r.metrics)
	if err != nil {
		return PersistResult{}, err
	}
	if err := tx.Commit(operationCtx); err != nil {
		if r.metrics != nil {
			r.metrics.commitUnconfirmed.Add(1)
		}
		return PersistResult{}, commitUnconfirmedError(err)
	}
	committed = true
	if r.metrics != nil {
		r.metrics.confirmedCommits.Add(1)
		if result.Outcome == CommitOutcomeCommitted {
			r.metrics.confirmedNewRows.Add(uint64(result.SamplesSaved))
		}
	}
	return result, nil
}

// PersistBatch atomically persists one finite set of validated inputs. Metadata
// validation and stable identities are prepared before a transaction is opened;
// no provisional success escapes if any member or transaction step fails.
func (r *postgresRepository) PersistBatch(ctx context.Context, messages []*Message) []BatchPersistResult {
	operationCtx := ctx
	if ctx != nil {
		var cancel context.CancelFunc
		operationCtx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	results := r.persistBatchGroup(operationCtx, messages)
	if len(results) == 0 || !batchFallbackEligible(results[0].Err) {
		return results
	}
	for index, message := range messages {
		if operationCtx == nil || operationCtx.Err() != nil {
			cause := error(context.Canceled)
			if operationCtx != nil {
				cause = operationCtx.Err()
			}
			for remaining := index; remaining < len(results); remaining++ {
				results[remaining] = BatchPersistResult{InputIndex: remaining, Identity: identityFromMessage(messages[remaining]), Err: cause}
			}
			break
		}
		if deadline, ok := operationCtx.Deadline(); ok && !time.Now().Before(deadline) {
			for remaining := index; remaining < len(results); remaining++ {
				results[remaining] = BatchPersistResult{InputIndex: remaining, Identity: identityFromMessage(messages[remaining]), Err: context.DeadlineExceeded}
			}
			break
		}
		if r.metrics != nil {
			r.metrics.fallbackAttempts.Add(1)
		}
		persisted, err := r.Persist(operationCtx, message)
		results[index] = BatchPersistResult{InputIndex: index, Identity: identityFromMessage(message), Outcome: persisted.Outcome, SamplesSaved: persisted.SamplesSaved, Err: err}
		if err == nil && persisted.Identity != results[index].Identity {
			results[index].Err = &ReasonError{Code: ReasonDependencyUnavailable, Field: "fallback_identity"}
			results[index].Outcome = 0
			results[index].SamplesSaved = 0
		}
	}
	return results
}

func batchFallbackEligible(err error) bool {
	var reason *ReasonError
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &reason) || reason == nil {
		return false
	}
	switch reason.Code {
	case ReasonMessageConflict, ReasonSequenceOverlap, ReasonUnknownGateway, ReasonUnknownSensor, ReasonResourceGraphIncomplete, ReasonLegacyProvenanceUnknown:
		return true
	default:
		return false
	}
}

func (r *postgresRepository) persistBatchGroup(ctx context.Context, messages []*Message) (results []BatchPersistResult) {
	results = make([]BatchPersistResult, len(messages))
	if len(messages) == 0 {
		return results
	}
	if len(messages) > r.maxBatchMessages {
		return batchErrorResults(messages, nil, &ReasonError{Code: ReasonInvalidPayload, Field: "batch_messages"})
	}
	metadata := make([]ledgerMessage, len(messages))
	for index, message := range messages {
		entry, err := ledgerMetadata(message)
		if err != nil {
			results[index] = BatchPersistResult{InputIndex: index, Identity: identityFromMessage(message), Err: err}
			continue
		}
		metadata[index] = entry
		results[index] = BatchPersistResult{InputIndex: index, Identity: identityFromLedger(entry)}
	}
	for _, result := range results {
		if result.Err != nil {
			for i := range results {
				if results[i].Err == nil {
					results[i].Err = result.Err
				}
			}
			return results
		}
	}
	if ctx == nil {
		return batchErrorResults(messages, metadata, &ReasonError{Code: ReasonInvalidPayload, Field: "context"})
	}
	if r.metrics != nil {
		r.metrics.groupAttempts.Add(1)
		r.metrics.groupMembers.Add(uint64(len(messages)))
		for _, message := range messages {
			if message != nil {
				r.metrics.groupRows.Add(uint64(len(message.samples)))
				r.metrics.groupPayloadBytes.Add(uint64(len(message.rawPayload)))
			}
		}
		r.metrics.beginAttempts.Add(1)
	}
	tx, err := r.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return batchErrorResults(messages, metadata, dependencyError("begin telemetry batch transaction", err))
	}
	if r.metrics != nil {
		r.metrics.beginSucceeded.Add(1)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackErr := r.rollback(tx, true)
		if rollbackErr != nil {
			if errors.Is(rollbackErr, pgx.ErrTxClosed) {
				return
			}
			primaryErr := results[0].Err
			var reason *ReasonError
			if errors.As(primaryErr, &reason) && reason.Code == ReasonCommitUnconfirmed {
				primaryErr = fmt.Errorf("%w; rollback telemetry batch transaction failed: %w", primaryErr, rollbackErr)
			} else {
				primaryErr = fmt.Errorf("rollback telemetry batch transaction failed (primary error: %v): %w", primaryErr, dependencyError("rollback telemetry batch transaction", rollbackErr))
			}
			results = batchErrorResults(messages, metadata, primaryErr)
		}
	}()
	resources, err := prepareTelemetryResources(ctx, tx, messages)
	if err != nil {
		return batchErrorResults(messages, metadata, err)
	}
	newMembers := make([]int, 0, len(messages))
	for index, message := range messages {
		entry := metadata[index]
		disposition, insertErr := insertTelemetryLedger(ctx, tx, message)
		if insertErr != nil {
			return batchErrorResults(messages, metadata, insertErr)
		}
		key := telemetryResourceKey{entry.topic.GatewayID, entry.sensorID}
		if _, ok := resources[key]; !ok {
			return batchErrorResults(messages, metadata, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_graph"})
		}
		switch disposition {
		case ledgerDispositionNew:
			newMembers = append(newMembers, index)
		case ledgerDispositionDuplicate:
			results[index].Outcome = CommitOutcomeDuplicateCommitted
		default:
			return batchErrorResults(messages, metadata, &ReasonError{Code: ReasonDependencyUnavailable, Field: "ledger_outcome"})
		}
	}
	if len(newMembers) != 0 {
		var expected int64
		for _, index := range newMembers {
			expected += int64(metadata[index].sampleCount)
		}
		if r.metrics != nil {
			r.metrics.copyCalls.Add(1)
		}
		count, copyErr := tx.CopyFrom(ctx, pgx.Identifier{"public", "telemetry"},
			[]string{"measured_at", "gateway_id", "sensor_id", "sequence_num", "value", "boot_id", "message_id", "sample_index"},
			&telemetryBatchCopySource{messages: messages, members: newMembers})
		if r.metrics != nil && count > 0 {
			r.metrics.copyReturnedRows.Add(uint64(count))
		}
		if copyErr != nil {
			return batchErrorResults(messages, metadata, dependencyError("copy telemetry batch samples", copyErr))
		}
		if count != expected {
			return batchErrorResults(messages, metadata, fmt.Errorf("telemetry batch sample copy count mismatch: %w", &ReasonError{Code: ReasonDependencyUnavailable}))
		}
		for _, index := range newMembers {
			key := telemetryResourceKey{metadata[index].topic.GatewayID, metadata[index].sensorID}
			resource := resources[key]
			if _, err := applySensorReportedState(ctx, tx, resource, messages[index]); err != nil {
				return batchErrorResults(messages, metadata, err)
			}
			if err := appendSensorSampleIntervalTemporal(ctx, tx, resource, messages[index]); err != nil {
				return batchErrorResults(messages, metadata, err)
			}
			results[index].Outcome = CommitOutcomeCommitted
			results[index].SamplesSaved = int(metadata[index].sampleCount)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		if r.metrics != nil {
			r.metrics.commitUnconfirmed.Add(1)
		}
		return batchErrorResults(messages, metadata, commitUnconfirmedError(err))
	}
	committed = true
	if r.metrics != nil {
		r.metrics.confirmedCommits.Add(1)
		for _, result := range results {
			if result.Outcome == CommitOutcomeCommitted && result.SamplesSaved > 0 {
				r.metrics.confirmedNewRows.Add(uint64(result.SamplesSaved))
			}
		}
	}
	return results
}

func identityFromLedger(entry ledgerMessage) MessageIdentity {
	return MessageIdentity{GatewayID: entry.topic.GatewayID, SensorID: entry.sensorID, BootID: entry.bootID.Bytes, MessageID: entry.messageID.Bytes}
}

func identityFromMessage(message *Message) MessageIdentity {
	if message == nil {
		return MessageIdentity{}
	}
	identity := MessageIdentity{GatewayID: message.topic.GatewayID, SensorID: message.topic.SensorID}
	if first, ok := message.FirstSample(); ok {
		identity.BootID, identity.MessageID = first.BootID, first.MessageID
	}
	return identity
}

func batchErrorResults(messages []*Message, metadata []ledgerMessage, err error) []BatchPersistResult {
	results := make([]BatchPersistResult, len(messages))
	for i, message := range messages {
		identity := identityFromMessage(message)
		if i < len(metadata) && metadata[i].payloadHash != "" {
			identity = identityFromLedger(metadata[i])
		}
		results[i] = BatchPersistResult{InputIndex: i, Identity: identity, Err: err}
	}
	return results
}

// persistTelemetryMessage performs the writes for one message in a transaction
// owned by its caller. It deliberately does not begin, commit, or rollback the
// transaction, allowing a future grouped transaction to reuse the same
// single-message write sequence without changing Persist's ownership contract.
func persistTelemetryMessage(ctx context.Context, tx pgx.Tx, message *Message, metadata ledgerMessage, metrics *PersistenceMetrics) (PersistResult, error) {
	resource, err := resolveTelemetryResource(ctx, tx, message)
	if err != nil {
		return PersistResult{}, err
	}
	disposition, err := insertTelemetryLedger(ctx, tx, message)
	if err != nil {
		return PersistResult{}, err
	}
	if disposition == ledgerDispositionDuplicate {
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

	if metrics != nil {
		metrics.copyCalls.Add(1)
	}
	copyCount, err := tx.CopyFrom(ctx,
		pgx.Identifier{"public", "telemetry"},
		[]string{"measured_at", "gateway_id", "sensor_id", "sequence_num", "value", "boot_id", "message_id", "sample_index"},
		&telemetryCopySource{samples: message.samples},
	)
	if metrics != nil && copyCount > 0 {
		metrics.copyReturnedRows.Add(uint64(copyCount))
	}
	if err != nil {
		return PersistResult{}, dependencyError("copy telemetry samples", err)
	}
	if copyCount != int64(metadata.sampleCount) {
		return PersistResult{}, fmt.Errorf("telemetry sample copy count mismatch: %w", &ReasonError{Code: ReasonDependencyUnavailable})
	}
	// Sensor current state and its temporal observation advance in the same
	// transaction as the ledger and raw samples. Any failure here rolls back all
	// of them and no commit outcome is reported.
	if _, err := applySensorReportedState(ctx, tx, resource, message); err != nil {
		return PersistResult{}, err
	}
	if err := appendSensorSampleIntervalTemporal(ctx, tx, resource, message); err != nil {
		return PersistResult{}, err
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

func rollbackTelemetry(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryRollbackTimeout)
	defer cancel()
	return tx.Rollback(ctx)
}

func (r *postgresRepository) rollback(tx pgx.Tx, count bool) error {
	err := rollbackTelemetry(tx)
	if count && r.metrics != nil && !errors.Is(err, pgx.ErrTxClosed) {
		r.metrics.rollbackAttempts.Add(1)
		if err == nil {
			r.metrics.confirmedRollbacks.Add(1)
		}
	}
	return err
}

type telemetryCopySource struct {
	samples []SampleCandidate
	next    int
}

// telemetryBatchCopySource streams each member's existing sample slice; it
// deliberately does not flatten/copy all raw rows into a second allocation.
type telemetryBatchCopySource struct {
	messages       []*Message
	members        []int
	member, sample int
}

func (source *telemetryBatchCopySource) Next() bool {
	for source.member < len(source.members) && source.sample >= len(source.messages[source.members[source.member]].samples) {
		source.member++
		source.sample = 0
	}
	return source.member < len(source.members)
}
func (source *telemetryBatchCopySource) Values() ([]any, error) {
	if !source.Next() {
		return nil, errors.New("telemetry batch CopyFrom source exhausted")
	}
	sample := source.messages[source.members[source.member]].samples[source.sample]
	source.sample++
	return []any{sample.MeasuredAt, sample.GatewayID, sample.SensorID, sample.SequenceNum, sample.Value, sample.BootID, sample.MessageID, sample.SampleIndex}, nil
}
func (source *telemetryBatchCopySource) Err() error { return nil }

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
