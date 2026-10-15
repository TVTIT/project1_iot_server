package telemetry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewCommitACKWireContract(t *testing.T) {
	idV4 := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	idV7 := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	confirmedAt := time.Date(2026, time.October, 15, 1, 2, 3, 123456789, time.FixedZone("test", 7*60*60))
	wantTime := confirmedAt.UTC()
	for _, tc := range []struct {
		id      uuid.UUID
		outcome CommitOutcome
	}{{idV4, CommitOutcomeCommitted}, {idV7, CommitOutcomeDuplicateCommitted}} {
		topic, ack, err := NewCommitACK("gateway_001", tc.id, tc.outcome, confirmedAt)
		if err != nil {
			t.Fatal(err)
		}
		wantTopic := "gateways/gateway_001/acks/" + tc.id.String()
		if topic != wantTopic {
			t.Fatalf("topic = %q, want %q", topic, wantTopic)
		}
		if ack.ProtocolVersion != 1 || ack.MessageType != "telemetry_ack" || ack.MessageID != tc.id || ack.Status != "committed" || !ack.CommittedAt.Equal(wantTime) || ack.CommittedAt.Location() != time.UTC {
			t.Fatalf("ACK = %#v", ack)
		}
		wire, err := json.Marshal(ack)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(wire, &fields); err != nil {
			t.Fatal(err)
		}
		wantKeys := []string{"protocol_version", "message_type", "message_id", "status", "committed_at"}
		if len(fields) != len(wantKeys) {
			t.Fatalf("wire keys = %v, want exactly %v", fields, wantKeys)
		}
		for _, key := range wantKeys {
			if _, ok := fields[key]; !ok {
				t.Fatalf("wire key %q missing: %s", key, wire)
			}
		}
		var committedAt string
		if err := json.Unmarshal(fields["committed_at"], &committedAt); err != nil {
			t.Fatal(err)
		}
		parsed, err := time.Parse(time.RFC3339Nano, committedAt)
		if err != nil || !parsed.Equal(wantTime) || parsed.Location() != time.UTC {
			t.Fatalf("committed_at = %q, parsed=%v err=%v; want UTC RFC3339 time", committedAt, parsed, err)
		}
	}
}

func TestNewCommitACKRejectsUnconfirmedAndInvalidInputs(t *testing.T) {
	validID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	validTime := time.Date(2026, 10, 15, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name       string
		gatewayID  string
		messageID  uuid.UUID
		outcome    CommitOutcome
		confirmed  time.Time
		wantReason ReasonCode
	}{
		{"unconfirmed", "gateway_001", validID, CommitOutcomeUnconfirmed, validTime, ReasonInvalidPayload},
		{"unknown outcome", "gateway_001", validID, CommitOutcome(255), validTime, ReasonInvalidPayload},
		{"invalid gateway", "backend_service", validID, CommitOutcomeCommitted, validTime, ReasonInvalidPayload},
		{"nil UUID", "gateway_001", uuid.Nil, CommitOutcomeCommitted, validTime, ReasonInvalidPayload},
		{"v3 UUID", "gateway_001", uuid.MustParse("0195e18c-9fc1-37a2-9064-69ea49e63bf2"), CommitOutcomeCommitted, validTime, ReasonInvalidPayload},
		{"non RFC variant", "gateway_001", uuid.MustParse("0195e18c-9fc1-7a42-0064-69ea49e63bf2"), CommitOutcomeCommitted, validTime, ReasonInvalidPayload},
		{"zero time", "gateway_001", validID, CommitOutcomeCommitted, time.Time{}, ReasonInvalidPayload},
		{"UTC year below one", "gateway_001", validID, CommitOutcomeCommitted, time.Date(1, 1, 1, 0, 0, 0, 0, time.FixedZone("plus", 1)), ReasonInvalidPayload},
		{"UTC year above range", "gateway_001", validID, CommitOutcomeCommitted, time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("minus", -2*60*60)), ReasonInvalidPayload},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic, ack, err := NewCommitACK(tc.gatewayID, tc.messageID, tc.outcome, tc.confirmed)
			if reasonCode(err) != tc.wantReason {
				t.Fatalf("error = %v, want %s", err, tc.wantReason)
			}
			if topic != "" || ack != (CommitACK{}) {
				t.Fatalf("failure returned usable output: topic=%q ack=%#v", topic, ack)
			}
		})
	}
}
