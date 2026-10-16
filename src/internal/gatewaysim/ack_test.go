package gatewaysim

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

const ackTestGatewayID = "gateway_001"

// ackTestMessageID is a canonical UUIDv4 (with hex letters, so the uppercase
// topic-rejection case is meaningful) used across the ACK parser tests.
var ackTestMessageID = uuid.MustParse("a2b3c4d5-6e7f-4a8b-9c0d-1e2f3a4b5c6d")

func ackTestTopic(messageID uuid.UUID) string {
	return "gateways/" + ackTestGatewayID + "/acks/" + messageID.String()
}

// validACK builds a wire ACK with the real backend builder so the parser is
// exercised against exactly what the server publishes.
func validACK(t *testing.T, messageID uuid.UUID, at time.Time) (string, []byte) {
	t.Helper()
	topic, ack, err := telemetry.NewCommitACK(ackTestGatewayID, messageID, telemetry.CommitOutcomeCommitted, at)
	if err != nil {
		t.Fatalf("NewCommitACK: %v", err)
	}
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("marshal ACK: %v", err)
	}
	return topic, payload
}

// ackJSON renders the canonical five-field ACK shape with an injected timestamp.
func ackJSON(idText, committedAt string) string {
	return `{"protocol_version":1,"message_type":"telemetry_ack","message_id":"` + idText + `","status":"committed","committed_at":"` + committedAt + `"}`
}

func TestParseCommitACKAcceptsValid(t *testing.T) {
	at := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)

	topic, payload := validACK(t, ackTestMessageID, at)
	ack, err := ParseCommitACK(topic, payload, ackTestGatewayID)
	if err != nil {
		t.Fatalf("ParseCommitACK: %v", err)
	}
	if ack.ProtocolVersion != 1 || ack.MessageType != "telemetry_ack" || ack.Status != "committed" {
		t.Fatalf("fields = %+v", ack)
	}
	if ack.MessageID != ackTestMessageID {
		t.Fatalf("message_id = %s", ack.MessageID)
	}
	if !ack.CommittedAt.Equal(at) || ack.CommittedAt.Location() != time.UTC {
		t.Fatalf("committed_at = %s (%s)", ack.CommittedAt, ack.CommittedAt.Location())
	}

	// A UUIDv7 message id is also accepted.
	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("NewV7: %v", err)
	}
	topicV7, payloadV7 := validACK(t, v7, at.Add(time.Second))
	if got, err := ParseCommitACK(topicV7, payloadV7, ackTestGatewayID); err != nil || got.MessageID != v7 {
		t.Fatalf("v7 ACK = %+v/%v", got, err)
	}

	// Padding whitespace around the single object is tolerated; a trailing
	// second document is rejected (covered by the malformed table).
	padded := append(append([]byte(nil), payload...), ' ', '\n', '\t')
	if _, err := ParseCommitACK(topic, padded, ackTestGatewayID); err != nil {
		t.Fatalf("padded ACK rejected: %v", err)
	}
}

func TestParseCommitACKRejectsMalformed(t *testing.T) {
	idText := ackTestMessageID.String()
	idJSON := `"` + idText + `"`
	const at = "2026-10-16T10:00:00Z"
	atJSON := `"` + at + `"`
	valid := ackJSON(idText, at)
	topic := ackTestTopic(ackTestMessageID)

	otherID := uuid.MustParse("33333333-3333-4333-8333-333333333333")

	cases := []struct {
		name    string
		topic   string
		payload []byte
		want    error
	}{
		{"empty payload", topic, []byte(""), ErrACKMalformed},
		{"not json", topic, []byte("nope"), ErrACKMalformed},
		{"json array", topic, []byte("[]"), ErrACKMalformed},
		{"json string", topic, []byte(`"x"`), ErrACKMalformed},
		{"empty object", topic, []byte("{}"), ErrACKMalformed},
		{
			"missing field",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":` + idJSON + `,"committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"unknown field",
			topic,
			[]byte(strings.TrimSuffix(valid, "}") + `,"extra":1}`),
			ErrACKMalformed,
		},
		{
			"duplicate key",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"committed","status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"protocol_version string",
			topic,
			[]byte(`{"protocol_version":"1","message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"protocol_version two",
			topic,
			[]byte(`{"protocol_version":2,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"protocol_version null",
			topic,
			[]byte(`{"protocol_version":null,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"message_type wrong",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_batch","message_id":` + idJSON + `,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"message_type null",
			topic,
			[]byte(`{"protocol_version":1,"message_type":null,"message_id":` + idJSON + `,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"status wrong",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"duplicate","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"status null",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":null,"committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"message_id null",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":null,"status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"message_id not uuid",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":"not-a-uuid","status":"committed","committed_at":` + atJSON + `}`),
			ErrACKMalformed,
		},
		{
			"message_id v1",
			topic,
			[]byte(ackJSON("6ba7b810-9dad-11d1-80b4-00c04fd430c8", at)),
			ErrACKMalformed,
		},
		{
			"message_id nil",
			topic,
			[]byte(ackJSON("00000000-0000-0000-0000-000000000000", at)),
			ErrACKMalformed,
		},
		{
			"message_id mismatched topic",
			ackTestTopic(otherID),
			[]byte(valid),
			ErrACKMalformed,
		},
		{
			"committed_at null",
			topic,
			[]byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":` + idJSON + `,"status":"committed","committed_at":null}`),
			ErrACKMalformed,
		},
		{
			"committed_at not a time",
			topic,
			[]byte(ackJSON(idText, "2026-10-16")),
			ErrACKMalformed,
		},
		{
			"committed_at non-UTC offset",
			topic,
			[]byte(ackJSON(idText, "2026-10-16T10:00:00+02:00")),
			ErrACKMalformed,
		},
		{"trailing document", topic, []byte(valid + ` {"a":1}`), ErrACKMalformed},
		{"oversize payload", topic, bytes.Repeat([]byte("x"), maxACKBytes+1), ErrACKOversize},
		{"topic wrong gateway", "gateways/gateway_002/acks/" + idText, []byte(valid), ErrACKTopic},
		{"topic wrong action", "gateways/gateway_001/telemetry/" + idText, []byte(valid), ErrACKTopic},
		{"topic too few segments", "gateways/gateway_001/acks", []byte(valid), ErrACKTopic},
		{"topic uppercase uuid", "gateways/gateway_001/acks/" + strings.ToUpper(idText), []byte(valid), ErrACKTopic},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseCommitACK(tc.topic, tc.payload, ackTestGatewayID)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// newACKTestProcessor opens the processor and registers an idempotent Stop that
// runs before the outbox is closed.
func newACKTestProcessor(t *testing.T, o *Outbox, capacity int) *ACKProcessor {
	t.Helper()
	p, err := NewACKProcessor(o, capacity)
	if err != nil {
		t.Fatalf("NewACKProcessor: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	})
	return p
}

func TestNewACKProcessorValidation(t *testing.T) {
	if _, err := NewACKProcessor(nil, 1); err == nil {
		t.Fatal("expected error for nil outbox")
	}
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	for _, capacity := range []int{0, -1, maxACKAdmissionCapacity + 1} {
		if _, err := NewACKProcessor(o, capacity); err == nil {
			t.Fatalf("capacity %d: expected error", capacity)
		}
	}
	if _, err := NewACKProcessor(o, maxACKAdmissionCapacity); err != nil {
		t.Fatalf("max capacity: %v", err)
	}
}

func TestACKProcessorDeletesOnlyExactPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	keep := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	drop := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))
	p := newACKTestProcessor(t, o, 4)

	topic, payload := validACK(t, drop.MessageID, outboxBaseTime.Add(2*time.Second))
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err != nil {
		t.Fatalf("handleAdmission: %v", err)
	}

	if _, found, err := o.Lookup(context.Background(), drop.MessageID); err != nil || found {
		t.Fatalf("acked row still present: found=%v err=%v", found, err)
	}
	if _, found, err := o.Lookup(context.Background(), keep.MessageID); err != nil || !found {
		t.Fatalf("unacked row missing: found=%v err=%v", found, err)
	}
	if got := p.Stats().Acknowledged; got != 1 {
		t.Fatalf("acknowledged = %d, want 1", got)
	}
}

func TestACKProcessorAcceptsEarlyLateAndPriorBoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	early := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	// A different boot id models a row left over from an earlier process boot.
	late := enqueueTestBatch(t, o, uuid.New(), uuid.New(), outboxBaseTime.Add(time.Hour))
	p := newACKTestProcessor(t, o, 4)

	// committed_at before the batch was measured and after it are both valid;
	// there is no max-age window.
	earlyTopic, earlyPayload := validACK(t, early.MessageID, outboxBaseTime.Add(-time.Hour))
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: earlyTopic, payload: earlyPayload}); err != nil {
		t.Fatalf("early handleAdmission: %v", err)
	}
	lateTopic, latePayload := validACK(t, late.MessageID, outboxBaseTime.Add(3*time.Hour))
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: lateTopic, payload: latePayload}); err != nil {
		t.Fatalf("late handleAdmission: %v", err)
	}

	if _, found, _ := o.Lookup(context.Background(), early.MessageID); found {
		t.Fatal("early ACK did not delete")
	}
	if _, found, _ := o.Lookup(context.Background(), late.MessageID); found {
		t.Fatal("late/prior-boot ACK did not delete")
	}
	if got := p.Stats().Acknowledged; got != 2 {
		t.Fatalf("acknowledged = %d, want 2", got)
	}
}

func TestACKProcessorDuplicateUnknownQuarantinedNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	other := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))
	p := newACKTestProcessor(t, o, 4)

	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err != nil {
		t.Fatalf("first handleAdmission: %v", err)
	}
	// The same ACK arrives again after the row was deleted: a no-op.
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err != nil {
		t.Fatalf("duplicate handleAdmission: %v", err)
	}
	// A valid ACK for an id never enqueued is also a no-op.
	unknownTopic, unknownPayload := validACK(t, uuid.New(), outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: unknownTopic, payload: unknownPayload}); err != nil {
		t.Fatalf("unknown handleAdmission: %v", err)
	}
	// A quarantined row is never deleted by an ACK.
	if ok, err := o.Quarantine(context.Background(), other.MessageID, "held for inspection"); err != nil || !ok {
		t.Fatalf("Quarantine = %v/%v", ok, err)
	}
	quarantineTopic, quarantinePayload := validACK(t, other.MessageID, outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: quarantineTopic, payload: quarantinePayload}); err != nil {
		t.Fatalf("quarantined handleAdmission: %v", err)
	}

	if _, found, _ := o.Lookup(context.Background(), other.MessageID); !found {
		t.Fatal("quarantined row was deleted")
	}
	if got := p.Stats().Acknowledged; got != 1 {
		t.Fatalf("acknowledged = %d, want 1", got)
	}
	// Only the first call deleted a row, so exactly one scheduler signal exists.
	if got := len(p.Notify()); got != 1 {
		t.Fatalf("notify len = %d, want 1", got)
	}
}

func TestACKProcessorInvalidChangesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	p := newACKTestProcessor(t, o, 4)

	// Malformed payload on the valid topic.
	malformed := []byte(`{"protocol_version":1}`)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: ackTestTopic(item.MessageID), payload: malformed}); err != nil {
		t.Fatalf("malformed: %v", err)
	}
	// Valid payload but a foreign gateway topic.
	_, okPayload := validACK(t, item.MessageID, outboxBaseTime)
	foreignTopic := "gateways/gateway_002/acks/" + item.MessageID.String()
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: foreignTopic, payload: okPayload}); err != nil {
		t.Fatalf("foreign topic: %v", err)
	}
	// Valid payload whose message_id does not match the topic UUID.
	otherID := uuid.New()
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: ackTestTopic(otherID), payload: okPayload}); err != nil {
		t.Fatalf("mismatch: %v", err)
	}

	if _, found, err := o.Lookup(context.Background(), item.MessageID); err != nil || !found {
		t.Fatalf("row changed by invalid ACK: found=%v err=%v", found, err)
	}
	if got := p.Stats(); got.Acknowledged != 0 || got.DeleteFailed != 0 {
		t.Fatalf("stats = %+v, want no acknowledged or failed delete", got)
	}
	if len(p.Notify()) != 0 {
		t.Fatal("invalid ACK must not signal the scheduler")
	}
}

func TestACKProcessorDeleteCommitUnknownIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	p := newACKTestProcessor(t, o, 1)

	o.commit = func(*sql.Tx) error { return errors.New("simulated delete commit failure") }

	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload})
	if err == nil || !errors.Is(err, ErrOutboxCommitUnknown) {
		t.Fatalf("err = %v, want ErrOutboxCommitUnknown", err)
	}
	if _, found, _ := o.Lookup(context.Background(), item.MessageID); !found {
		t.Fatal("row must remain after an unknown delete")
	}
	stats := p.Stats()
	if stats.Acknowledged != 0 || stats.DeleteFailed != 1 {
		t.Fatalf("stats = %+v, want acknowledged=0 deletefailed=1", stats)
	}
}

func TestACKProcessorDeleteSQLFailureIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	p := newACKTestProcessor(t, o, 1)

	if _, err := o.db.Exec(`CREATE TRIGGER fail_ack_delete BEFORE DELETE ON outbox
		BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	defer func() { _, _ = o.db.Exec("DROP TRIGGER fail_ack_delete") }()

	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err == nil {
		t.Fatal("expected a fatal delete failure")
	}
	if _, found, _ := o.Lookup(context.Background(), item.MessageID); !found {
		t.Fatal("row must remain after a failed delete")
	}
	stats := p.Stats()
	if stats.Acknowledged != 0 || stats.DeleteFailed != 1 {
		t.Fatalf("stats = %+v, want acknowledged=0 deletefailed=1", stats)
	}
}

func TestACKProcessorAdmissionCopyRetainedOversizeFullNoLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	p := newACKTestProcessor(t, o, 2)

	// A retained ACK and an oversized frame are dropped at admission.
	p.Admit("gateways/gateway_001/acks/"+ackTestMessageID.String(), []byte("{}"), true)
	p.Admit("topic", bytes.Repeat([]byte("x"), maxACKBytes+1), false)

	// The admission must own an immutable copy of the caller's buffer.
	original := []byte(`{"a":1}`)
	p.Admit("topic-1", original, false)
	original[0] = '!'
	p.Admit("topic-2", []byte(`{"b":2}`), false)

	// The queue now holds two entries, so a third is dropped, not lost silently.
	p.Admit("topic-3", []byte(`{"c":3}`), false)

	stats := p.Stats()
	if stats.DroppedRetained != 1 || stats.DroppedOversize != 1 || stats.DroppedFull != 1 {
		t.Fatalf("drop stats = %+v", stats)
	}
	if stats.Admitted != 2 {
		t.Fatalf("admitted = %d, want 2", stats.Admitted)
	}
	if len(p.admissions) != 2 {
		t.Fatalf("queue len = %d, want 2", len(p.admissions))
	}

	first := <-p.admissions
	if first.topic != "topic-1" || string(first.payload) != `{"a":1}` {
		t.Fatalf("admission copy mutated: topic=%q payload=%q", first.topic, first.payload)
	}
	second := <-p.admissions
	if second.topic != "topic-2" {
		t.Fatalf("second admission topic = %q", second.topic)
	}
	// No admission is lost: every non-dropped send is either queued or rejected.
	if stats.Admitted+stats.DroppedFull != 3 {
		t.Fatalf("admission accounting = %+v", stats)
	}
}

func TestACKProcessorStartStopNotifyLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	p := newACKTestProcessor(t, o, 1)

	p.Start()
	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	p.Admit(topic, payload, false)

	select {
	case <-p.Notify():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the post-delete notification")
	}
	if _, found, err := o.Lookup(context.Background(), item.MessageID); err != nil || found {
		t.Fatalf("row not deleted: found=%v err=%v", found, err)
	}
	if got := p.Stats(); got.Admitted != 1 || got.Acknowledged != 1 {
		t.Fatalf("stats = %+v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := p.Fatal(); err != nil {
		t.Fatalf("unexpected fatal: %v", err)
	}

	// A late Admit after Stop must not panic or hang; it is dropped once full.
	p.Admit("late-1", []byte("{}"), false)
	p.Admit("late-2", []byte("{}"), false)
	if got := p.Stats().DroppedFull; got != 1 {
		t.Fatalf("post-stop dropped = %d, want 1", got)
	}
}

func TestACKProcessorStopWithoutStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	p := newACKTestProcessor(t, o, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestACKProcessorStopCooperativeWithSlowDelete proves Stop honours its context
// while the worker is inside a slow delete, then completes cleanly once the
// delete finishes, without a closed-channel send or a leaked worker.
func TestACKProcessorStopCooperativeWithSlowDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	p := newACKTestProcessor(t, o, 1)
	p.Start()

	entered := make(chan struct{})
	release := make(chan struct{})
	o.commit = func(tx *sql.Tx) error {
		close(entered)
		<-release
		return tx.Commit()
	}

	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	p.Admit(topic, payload, false)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach the delete commit")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Stop(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop = %v, want context.Canceled", err)
	}

	close(release)
	fresh, cancelFresh := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFresh()
	if err := p.Stop(fresh); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := p.Fatal(); err != nil {
		t.Fatalf("unexpected fatal: %v", err)
	}
	if got := p.Stats().Acknowledged; got != 1 {
		t.Fatalf("acknowledged = %d, want 1", got)
	}
}

func TestACKProcessor_LatencyTracker_DuplicateUnknownQuarantinedProducesNoSample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime)
	other := enqueueTestBatch(t, o, outboxBootID, uuid.New(), outboxBaseTime.Add(time.Second))
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}
	p := newACKTestProcessor(t, o, 4)
	p.SetLatencyRecorder(tracker)

	// Pre-register item in tracker as published
	tracker.RecordFirstPublish(item.MessageID)

	// Valid first ACK: should record 1 sample
	topic, payload := validACK(t, item.MessageID, outboxBaseTime)
	fc.Advance(10 * time.Millisecond)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err != nil {
		t.Fatalf("first handleAdmission: %v", err)
	}
	if snap := tracker.Snapshot(); snap.SampleCount != 1 {
		t.Fatalf("expected 1 sample after first valid ACK, got %d", snap.SampleCount)
	}

	// Duplicate ACK for the same item: must produce no new sample
	fc.Advance(10 * time.Millisecond)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: topic, payload: payload}); err != nil {
		t.Fatalf("duplicate handleAdmission: %v", err)
	}
	if snap := tracker.Snapshot(); snap.SampleCount != 1 {
		t.Fatalf("expected sample count to remain 1 on duplicate ACK, got %d", snap.SampleCount)
	}

	// Unknown ACK
	unknownTopic, unknownPayload := validACK(t, uuid.New(), outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: unknownTopic, payload: unknownPayload}); err != nil {
		t.Fatalf("unknown handleAdmission: %v", err)
	}
	if snap := tracker.Snapshot(); snap.SampleCount != 1 {
		t.Fatalf("expected sample count to remain 1 on unknown ACK, got %d", snap.SampleCount)
	}

	// Quarantined ACK
	if ok, err := o.Quarantine(context.Background(), other.MessageID, "held"); err != nil || !ok {
		t.Fatalf("Quarantine: %v/%v", ok, err)
	}
	tracker.RecordFirstPublish(other.MessageID)
	qTopic, qPayload := validACK(t, other.MessageID, outboxBaseTime)
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: qTopic, payload: qPayload}); err != nil {
		t.Fatalf("quarantined handleAdmission: %v", err)
	}
	if snap := tracker.Snapshot(); snap.SampleCount != 1 {
		t.Fatalf("expected sample count to remain 1 on quarantined ACK, got %d", snap.SampleCount)
	}

	// Malformed ACK
	if err := p.handleAdmission(context.Background(), ackAdmission{topic: "gateways/gateway_001/acks/" + uuid.NewString(), payload: []byte("not-json")}); err != nil {
		t.Fatalf("malformed handleAdmission: %v", err)
	}
	if snap := tracker.Snapshot(); snap.SampleCount != 1 {
		t.Fatalf("expected sample count to remain 1 on malformed ACK, got %d", snap.SampleCount)
	}
}
