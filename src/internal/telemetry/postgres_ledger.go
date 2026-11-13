package telemetry

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ledgerDisposition uint8

const (
	ledgerDispositionUnconfirmed ledgerDisposition = iota
	ledgerDispositionNew
	ledgerDispositionDuplicate
)

type ledgerMessage struct {
	topic       ParsedTopic
	payloadHash string
	sensorID    string
	bootID      pgtype.UUID
	messageID   pgtype.UUID
	firstSample SampleCandidate
	lastSample  SampleCandidate
	sampleCount int32
	timestamp   time.Time
}

// The three ledger statements are shared verbatim by the single-message path and
// the batched group path. The continuation indentation inside each raw string is
// part of the statement text and must stay byte-identical when the constants move
// to package level.
const telemetryLedgerInsertSQL = `INSERT INTO public.processed_messages (
		gateway_id, message_id, payload_hash, source_topic, sensor_id, boot_id,
		first_sequence, last_sequence, sample_count, timestamp
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (gateway_id, message_id) DO NOTHING
	RETURNING gateway_id`

const telemetryLedgerOverlapSQL = `SELECT EXISTS (
			SELECT 1 FROM (
				SELECT last_sequence
				FROM public.processed_messages
				WHERE gateway_id = $1 AND sensor_id = $2 AND boot_id = $3
					AND message_id <> $6
					AND first_sequence <= $4
				ORDER BY first_sequence DESC
				LIMIT 1
			) prev
			WHERE prev.last_sequence >= $5
		)`

const telemetryLedgerConflictSQL = `SELECT payload_hash, source_topic, sensor_id, boot_id,
		first_sequence, last_sequence, sample_count, timestamp
		FROM public.processed_messages
		WHERE gateway_id = $1 AND message_id = $2`

// ledgerConflictRow is the existing conflicting ledger row read back when the
// INSERT reported a conflict. classifyLedgerConflict is the single classifier
// shared by the single-message and the batched path so the two cannot drift.
type ledgerConflictRow struct {
	payloadHash   pgtype.Text
	sourceTopic   pgtype.Text
	sensorID      pgtype.Text
	bootID        pgtype.UUID
	firstSequence pgtype.Int8
	lastSequence  pgtype.Int8
	sampleCount   pgtype.Int4
	timestamp     pgtype.Timestamptz
}

// insertTelemetryLedger inserts a message provenance row or classifies an
// existing key as a duplicate/conflict. It assumes the caller has already
// locked the target Sensor row and owns tx; it never commits or rolls back.
func insertTelemetryLedger(ctx context.Context, tx pgx.Tx, message *Message) (ledgerDisposition, error) {
	entry, err := ledgerMetadata(message)
	if err != nil {
		return ledgerDispositionUnconfirmed, err
	}

	var insertedGatewayID string
	err = tx.QueryRow(ctx, telemetryLedgerInsertSQL,
		entry.topic.GatewayID, entry.messageID, entry.payloadHash,
		entry.topic.SourceTopic, entry.sensorID, entry.bootID,
		entry.firstSample.SequenceNum, entry.lastSample.SequenceNum, entry.sampleCount, entry.timestamp,
	).Scan(&insertedGatewayID)
	if err == nil {
		var overlap bool
		err = tx.QueryRow(ctx, telemetryLedgerOverlapSQL, entry.topic.GatewayID, entry.sensorID, entry.bootID,
			entry.lastSample.SequenceNum, entry.firstSample.SequenceNum, entry.messageID,
		).Scan(&overlap)
		if err != nil {
			return ledgerDispositionUnconfirmed, dependencyError("check telemetry sequence overlap", err)
		}
		if overlap {
			return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonSequenceOverlap, Field: "sequence_range"}
		}
		return ledgerDispositionNew, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ledgerDispositionUnconfirmed, dependencyError("insert telemetry message ledger", err)
	}

	// Use a later statement so READ COMMITTED sees a concurrent conflicting row
	// after the INSERT waited for that transaction to finish.
	row, conflictErr := queryLedgerConflictRow(ctx, tx, entry)
	if conflictErr != nil {
		return ledgerDispositionUnconfirmed, dependencyError("read conflicting telemetry message ledger", conflictErr)
	}
	return classifyLedgerConflict(entry, row)
}

// insertTelemetryLedgerBatch pipelines the per-message ledger work of one batch
// group into a single SendBatch round trip. Each entry queues exactly two
// statements interleaved in message order (INSERT_k, EXISTS_k) so EXISTS_k
// still runs after INSERT_k and before INSERT_{k+1}, exactly like the
// sequential loop; grouping all INSERTs before all EXISTS queries would let
// EXISTS_k observe a later message's row and change the overlap decision.
//
// The conflict SELECT is omitted from the batch on the normal non-conflicting
// path, reducing the batch statements from 3N to 2N (64 statements for 32
// messages). When an INSERT reports a conflict (pgx.ErrNoRows), the conflict
// row is read via queryLedgerConflictRow after the batch completes.
// Every result is consumed in queue order because skipping one would
// desynchronize the batch. The first error stops the loop and closes the batch:
// after a statement error pgx does not advance the batch query index, so reading
// further results would repeat the same failure.
func insertTelemetryLedgerBatch(ctx context.Context, tx pgx.Tx, entries []ledgerMessage) ([]ledgerDisposition, error) {
	if tx == nil {
		return nil, &ReasonError{Code: ReasonInvalidPayload, Field: "tx"}
	}
	if len(entries) == 0 {
		return nil, nil
	}
	batch := &pgx.Batch{}
	queueTelemetryLedgerBatch(batch, entries)
	br := tx.SendBatch(ctx, batch)
	dispositions := make([]ledgerDisposition, len(entries))
	var conflictIndices []int
	for index := range entries {
		disposition, isConflict, err := readTelemetryLedgerBatchEntry(br)
		if err != nil {
			_ = br.Close()
			return nil, err
		}
		if isConflict {
			conflictIndices = append(conflictIndices, index)
		} else {
			dispositions[index] = disposition
		}
	}
	if err := br.Close(); err != nil {
		return nil, dependencyError("close telemetry message ledger batch", err)
	}

	for _, index := range conflictIndices {
		entry := entries[index]
		row, err := queryLedgerConflictRow(ctx, tx, entry)
		if err != nil {
			return nil, dependencyError("read conflicting telemetry message ledger", err)
		}
		disposition, err := classifyLedgerConflict(entry, row)
		if err != nil {
			return nil, err
		}
		dispositions[index] = disposition
	}
	return dispositions, nil
}

// queueTelemetryLedgerBatch queues the two ledger statements of every entry in
// interleaved message order. A unit test can assert the statement count and the
// per-entry order without a database.
func queueTelemetryLedgerBatch(b *pgx.Batch, entries []ledgerMessage) {
	for _, entry := range entries {
		b.Queue(telemetryLedgerInsertSQL,
			entry.topic.GatewayID, entry.messageID, entry.payloadHash,
			entry.topic.SourceTopic, entry.sensorID, entry.bootID,
			entry.firstSample.SequenceNum, entry.lastSample.SequenceNum, entry.sampleCount, entry.timestamp,
		)
		b.Queue(telemetryLedgerOverlapSQL, entry.topic.GatewayID, entry.sensorID, entry.bootID,
			entry.lastSample.SequenceNum, entry.firstSample.SequenceNum, entry.messageID)
	}
}

// readTelemetryLedgerBatchEntry consumes the two queued results of one entry and
// returns the disposition, whether the entry conflicted and needs resolution,
// and any dependency or validation error encountered.
func readTelemetryLedgerBatchEntry(br pgx.BatchResults) (ledgerDisposition, bool, error) {
	var insertedGatewayID string
	err := br.QueryRow().Scan(&insertedGatewayID)
	if err == nil {
		var overlap bool
		if overlapErr := br.QueryRow().Scan(&overlap); overlapErr != nil {
			return ledgerDispositionUnconfirmed, false, dependencyError("check telemetry sequence overlap", overlapErr)
		}
		if overlap {
			return ledgerDispositionUnconfirmed, false, &ReasonError{Code: ReasonSequenceOverlap, Field: "sequence_range"}
		}
		return ledgerDispositionNew, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ledgerDispositionUnconfirmed, false, dependencyError("insert telemetry message ledger", err)
	}

	// The INSERT reported a conflicting (gateway_id, message_id). Consume the
	// queued overlap check to stay in sync with the batch results. Its boolean
	// outcome is ignored because a conflicting key is resolved by the conflict
	// classifier rather than being treated as a sequence overlap.
	var overlap bool
	if overlapErr := br.QueryRow().Scan(&overlap); overlapErr != nil {
		return ledgerDispositionUnconfirmed, false, dependencyError("check telemetry sequence overlap", overlapErr)
	}
	return ledgerDispositionUnconfirmed, true, nil
}

func queryLedgerConflictRow(ctx context.Context, tx pgx.Tx, entry ledgerMessage) (ledgerConflictRow, error) {
	return scanLedgerConflictRow(tx.QueryRow(ctx, telemetryLedgerConflictSQL, entry.topic.GatewayID, entry.messageID))
}

func scanLedgerConflictRow(row pgx.Row) (ledgerConflictRow, error) {
	var conflict ledgerConflictRow
	err := row.Scan(&conflict.payloadHash, &conflict.sourceTopic, &conflict.sensorID, &conflict.bootID,
		&conflict.firstSequence, &conflict.lastSequence, &conflict.sampleCount, &conflict.timestamp)
	if err != nil {
		return ledgerConflictRow{}, err
	}
	return conflict, nil
}

// classifyLedgerConflict compares an existing conflicting ledger row with the
// entry that collided with it. The check order and the reason codes are the ones
// the sequential path has always used: an unusable legacy provenance row is
// unknown, a matching key with different payload or provenance is a conflict,
// and a byte-identical provenance row is a committed duplicate.
func classifyLedgerConflict(entry ledgerMessage, row ledgerConflictRow) (ledgerDisposition, error) {
	if !row.sourceTopic.Valid || !row.sensorID.Valid || !row.bootID.Valid ||
		!row.firstSequence.Valid || !row.lastSequence.Valid || !row.sampleCount.Valid ||
		row.sampleCount.Int32 <= 0 || row.firstSequence.Int64 < 0 ||
		row.lastSequence.Int64 < row.firstSequence.Int64 ||
		row.lastSequence.Int64-row.firstSequence.Int64 != int64(row.sampleCount.Int32)-1 {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonLegacyProvenanceUnknown, Field: "ledger_provenance"}
	}
	if !row.timestamp.Valid {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonLegacyProvenanceUnknown, Field: "ledger_timestamp_unknown"}
	}
	if row.payloadHash.String != entry.payloadHash || row.sourceTopic.String != entry.topic.SourceTopic {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonMessageConflict, Field: "message_id"}
	}
	if row.sensorID.String != entry.sensorID || row.bootID.Bytes != entry.bootID.Bytes ||
		row.firstSequence.Int64 != entry.firstSample.SequenceNum ||
		row.lastSequence.Int64 != entry.lastSample.SequenceNum ||
		row.sampleCount.Int32 != entry.sampleCount ||
		!row.timestamp.Time.UTC().Equal(entry.timestamp) {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonMessageConflict, Field: "message_provenance"}
	}
	return ledgerDispositionDuplicate, nil
}

func ledgerMetadata(message *Message) (ledgerMessage, error) {
	if message == nil {
		return ledgerMessage{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	topic := message.Topic()
	first, hasFirst := message.FirstSample()
	last, hasLast := message.LastSample()
	count := len(message.samples)
	if topic.GatewayID == "" || topic.SensorID == "" || topic.SourceTopic == "" ||
		message.payloadHash == "" || !hasFirst || !hasLast || count == 0 || count > int(^uint32(0)>>1) {
		return ledgerMessage{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	if first.GatewayID != topic.GatewayID || last.GatewayID != topic.GatewayID ||
		first.SensorID != topic.SensorID || last.SensorID != topic.SensorID ||
		first.MessageID == [16]byte{} || first.MessageID != last.MessageID ||
		first.BootID == [16]byte{} || first.BootID != last.BootID ||
		first.SampleIndex != 0 || last.SampleIndex != int32(count-1) ||
		first.SequenceNum < 0 || last.SequenceNum < first.SequenceNum ||
		last.SequenceNum-first.SequenceNum != int64(count)-1 {
		return ledgerMessage{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message_samples"}
	}
	return ledgerMessage{
		topic:       topic,
		payloadHash: message.payloadHash,
		sensorID:    topic.SensorID,
		bootID:      pgtype.UUID{Bytes: first.BootID, Valid: true},
		messageID:   pgtype.UUID{Bytes: first.MessageID, Valid: true},
		firstSample: first,
		lastSample:  last,
		sampleCount: int32(count),
		timestamp:   first.MeasuredAt.UTC(),
	}, nil
}
