package telemetry

import (
	"context"
	"errors"

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
	err = tx.QueryRow(ctx, `INSERT INTO public.processed_messages (
		gateway_id, message_id, payload_hash, source_topic, sensor_id, boot_id,
		first_sequence, last_sequence, sample_count
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (gateway_id, message_id) DO NOTHING
	RETURNING gateway_id`,
		entry.topic.GatewayID, entry.messageID, entry.payloadHash,
		entry.topic.SourceTopic, entry.sensorID, entry.bootID,
		entry.firstSample.SequenceNum, entry.lastSample.SequenceNum, entry.sampleCount,
	).Scan(&insertedGatewayID)
	if err == nil {
		var overlap bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM public.processed_messages
			WHERE gateway_id = $1 AND sensor_id = $2 AND boot_id = $3
				AND first_sequence <= $4 AND last_sequence >= $5
				AND message_id <> $6
		)`, entry.topic.GatewayID, entry.sensorID, entry.bootID,
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
	var payloadHash pgtype.Text
	var sourceTopic pgtype.Text
	var sensorID pgtype.Text
	var bootID pgtype.UUID
	var firstSequence pgtype.Int8
	var lastSequence pgtype.Int8
	var sampleCount pgtype.Int4
	err = tx.QueryRow(ctx, `SELECT payload_hash, source_topic, sensor_id, boot_id,
		first_sequence, last_sequence, sample_count
		FROM public.processed_messages
		WHERE gateway_id = $1 AND message_id = $2`,
		entry.topic.GatewayID, entry.messageID,
	).Scan(&payloadHash, &sourceTopic, &sensorID, &bootID, &firstSequence, &lastSequence, &sampleCount)
	if err != nil {
		return ledgerDispositionUnconfirmed, dependencyError("read conflicting telemetry message ledger", err)
	}
	if !sourceTopic.Valid || !sensorID.Valid || !bootID.Valid ||
		!firstSequence.Valid || !lastSequence.Valid || !sampleCount.Valid ||
		sampleCount.Int32 <= 0 || firstSequence.Int64 < 0 ||
		lastSequence.Int64 < firstSequence.Int64 ||
		lastSequence.Int64-firstSequence.Int64 != int64(sampleCount.Int32)-1 {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonLegacyProvenanceUnknown, Field: "ledger_provenance"}
	}
	if payloadHash.String != entry.payloadHash || sourceTopic.String != entry.topic.SourceTopic {
		return ledgerDispositionUnconfirmed, &ReasonError{Code: ReasonMessageConflict, Field: "message_id"}
	}
	if sensorID.String != entry.sensorID || bootID.Bytes != entry.bootID.Bytes ||
		firstSequence.Int64 != entry.firstSample.SequenceNum ||
		lastSequence.Int64 != entry.lastSample.SequenceNum ||
		sampleCount.Int32 != entry.sampleCount {
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
	}, nil
}
