package telemetry

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestInsertTelemetryLedgerBatchKeepsTwoStatementsPerMessage pins the queue
// order the pipelined ledger batch depends on: each entry queues INSERT, EXISTS
// interleaved in message order, so EXISTS_k runs after INSERT_k and before
// INSERT_{k+1} exactly like the sequential loop. The conflict read is omitted
// from the batch and runs individually only when an INSERT encounters a conflict.
func TestInsertTelemetryLedgerBatchKeepsTwoStatementsPerMessage(t *testing.T) {
	entries := make([]ledgerMessage, 0, 3)
	for index := 0; index < 3; index++ {
		entry, err := ledgerMetadata(telemetryRepositoryTestMessage(t))
		if err != nil {
			t.Fatal(err)
		}
		entry.payloadHash = fmt.Sprintf("payload-hash-%d", index)
		entry.sensorID = fmt.Sprintf("sensor_%03d", index)
		entries = append(entries, entry)
	}
	batch := &pgx.Batch{}
	queueTelemetryLedgerBatch(batch, entries)
	if batch.Len() != 6 {
		t.Fatalf("queued statements = %d; want 6 (two per message)", batch.Len())
	}
	for index, entry := range entries {
		insert := batch.QueuedQueries[index*2]
		overlap := batch.QueuedQueries[index*2+1]
		if !strings.Contains(insert.SQL, "INSERT INTO public.processed_messages") ||
			!strings.Contains(insert.SQL, "ON CONFLICT (gateway_id, message_id) DO NOTHING") {
			t.Fatalf("entry %d first statement is not the ledger insert: %s", index, insert.SQL)
		}
		if !strings.Contains(overlap.SQL, "SELECT EXISTS") ||
			!strings.Contains(overlap.SQL, "ORDER BY first_sequence DESC") ||
			!strings.Contains(overlap.SQL, "LIMIT 1") ||
			!strings.Contains(overlap.SQL, "AND message_id <> $6") {
			t.Fatalf("entry %d second statement is not the O(log N) overlap check: %s", index, overlap.SQL)
		}
		if insert.Arguments[2] != entry.payloadHash || insert.Arguments[4] != entry.sensorID {
			t.Fatalf("entry %d insert args = %#v; want that entry's payload hash and sensor", index, insert.Arguments)
		}
		if len(overlap.Arguments) != 6 || overlap.Arguments[2] != entry.bootID {
			t.Fatalf("entry %d overlap args = %#v; want that entry's gateway/sensor/boot", index, overlap.Arguments)
		}
	}
}

func TestLedgerMetadataRequiresValidatedSampleRange(t *testing.T) {
	if _, err := ledgerMetadata(nil); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil message error = %v, want %s", err, ReasonInvalidPayload)
	}

	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	payload := messageTestBatchPayload(t, "2026-08-21T10:15:00Z", 12501, []float64{1, 2, 3})
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	got, err := ledgerMetadata(message)
	if err != nil {
		t.Fatalf("ledgerMetadata(valid message): %v", err)
	}
	if got.topic.SourceTopic != "gateways/gateway_001/telemetry/sensor_001" ||
		got.payloadHash != message.PayloadHash() || got.firstSample.SequenceNum != 12501 ||
		got.lastSample.SequenceNum != 12503 || got.sampleCount != 3 {
		t.Fatalf("ledger metadata = %#v, want exact topic/hash and checked range", got)
	}

	// Corrupt only the private checked candidate snapshot. The guard must reject
	// inconsistent range metadata instead of writing an internally contradictory ledger row.
	malformed := *message
	malformed.samples = append([]SampleCandidate(nil), message.samples...)
	malformed.samples[len(malformed.samples)-1].SequenceNum++
	if _, err := ledgerMetadata(&malformed); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("inconsistent sample range error = %v, want %s", err, ReasonInvalidPayload)
	}
}

func TestLedgerMetadataPreservesOriginalSampleZeroTimestamp(t *testing.T) {
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	const sourceTimestamp = "2026-08-21T10:15:00.123456+02:00"
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001",
		messageTestBatchPayload(t, sourceTimestamp, 12501, []float64{1, 2}), messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := ledgerMetadata(message)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 21, 8, 15, 0, 123456000, time.UTC)
	if !metadata.timestamp.Equal(want) || metadata.timestamp.Location() != time.UTC {
		t.Fatalf("ledger timestamp = %s (%s), want %s UTC", metadata.timestamp, metadata.timestamp.Location(), want)
	}
}

func TestInsertTelemetryLedgerPersistsAndComparesTimestamp(t *testing.T) {
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001",
		messageTestBatchPayload(t, "2026-08-21T10:15:00.123456Z", 12501, []float64{1, 2}), messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 21, 10, 15, 0, 123456000, time.UTC)
	t.Run("new ledger persists sample-zero timestamp", func(t *testing.T) {
		tx := &telemetryFakeTx{message: message}
		got, err := insertTelemetryLedger(context.Background(), tx, message)
		if err != nil || got != ledgerDispositionNew {
			t.Fatalf("insert disposition = %v, error = %v", got, err)
		}
		if len(tx.ledgerInsertArgs) != 10 {
			t.Fatalf("insert args count = %d, want timestamp as tenth argument", len(tx.ledgerInsertArgs))
		}
		stored, ok := tx.ledgerInsertArgs[9].(time.Time)
		if !ok || !stored.Equal(want) || stored.Location() != time.UTC {
			t.Fatalf("insert timestamp = %#v, want %s UTC", tx.ledgerInsertArgs[9], want)
		}
	})

	t.Run("exact replay is duplicate", func(t *testing.T) {
		tx := &telemetryFakeTx{message: message, duplicate: true}
		got, err := insertTelemetryLedger(context.Background(), tx, message)
		if err != nil || got != ledgerDispositionDuplicate {
			t.Fatalf("replay disposition = %v, error = %v", got, err)
		}
	})
	for _, tc := range []struct {
		name string
		edit func(*telemetryFakeTx)
	}{
		{"timestamp", func(tx *telemetryFakeTx) { other := want.Add(time.Microsecond); tx.duplicateTimestamp = &other }},
		{"sensor", func(tx *telemetryFakeTx) { tx.duplicateSensor = "sensor_other" }},
		{"payload", func(tx *telemetryFakeTx) { tx.duplicateHash = "different-hash" }},
	} {
		t.Run("same ID different "+tc.name+" conflicts", func(t *testing.T) {
			tx := &telemetryFakeTx{message: message, duplicate: true}
			tc.edit(tx)
			got, err := insertTelemetryLedger(context.Background(), tx, message)
			if got != ledgerDispositionUnconfirmed || reasonCode(err) != ReasonMessageConflict {
				t.Fatalf("conflicting replay = %v, %v; want unconfirmed message conflict", got, err)
			}
		})
	}
	t.Run("legacy NULL timestamp remains unconfirmed", func(t *testing.T) {
		tx := &telemetryFakeTx{message: message, duplicate: true, duplicateNullTimestamp: true}
		got, err := insertTelemetryLedger(context.Background(), tx, message)
		if got != ledgerDispositionUnconfirmed || reasonCode(err) != ReasonLegacyProvenanceUnknown {
			t.Fatalf("NULL timestamp replay = %v, %v; want conservative legacy unknown", got, err)
		}
	})
}
