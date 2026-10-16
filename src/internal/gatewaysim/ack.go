package gatewaysim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

// v1 committed ACK wire constants. They mirror telemetry.CommitACK and
// telemetry.NewCommitACK in the server package and do not define a second
// protocol: the simulator reads exactly the five fields the backend emits and
// never adds, renames or reorders a field.
const (
	ackProtocolVersion = 1
	ackMessageType     = "telemetry_ack"
	ackStatusCommitted = "committed"
)

// maxACKBytes bounds a raw ACK payload before it is copied into the admission
// queue. The five-field v1 committed ACK serializes to well under 256 bytes, so
// 1024 bytes is a generous finite ceiling that still rejects an oversized or
// hostile frame before any copy or JSON work happens.
const maxACKBytes = 1024

// Admission queue bounds. The broker callback only ever performs a non-blocking
// send to a channel of this fixed capacity; it never grows a per-message
// goroutine and never runs SQL or JSON parsing inline.
const (
	minACKAdmissionCapacity = 1
	maxACKAdmissionCapacity = 4096
)

// ACK rejection reasons. They are stable sentinels so callers never inspect
// driver or JSON error text, which could echo untrusted input.
var (
	// ErrACKOversize marks a payload larger than maxACKBytes.
	ErrACKOversize = errors.New("gatewaysim: ACK exceeds the size bound")
	// ErrACKMalformed marks a payload that is not one strict v1 committed ACK.
	ErrACKMalformed = errors.New("gatewaysim: ACK is not a strict v1 committed telemetry_ack")
	// ErrACKTopic marks a topic that is not the bound gateway's own ACK topic.
	ErrACKTopic = errors.New("gatewaysim: ACK topic is not the own committed ACK topic")
)

// ackFieldNames is the exact v1 committed ACK field set. A payload with any
// other field name, a missing field or a repeated field is rejected.
var ackFieldNames = map[string]struct{}{
	"protocol_version": {},
	"message_type":     {},
	"message_id":       {},
	"status":           {},
	"committed_at":     {},
}

// ParseCommitACK strictly parses one application-level commit ACK. It accepts
// exactly one JSON object with exactly the five v1 field names and rejects a
// duplicate key, an unknown key, a missing key, a null value, a wrong type and
// a trailing second document. It then requires protocol_version=1,
// message_type=telemetry_ack, status=committed, a v4/v7 UUID message_id that
// exactly matches the UUID segment of topic, an exact own topic of the form
// gateways/<gatewayID>/acks/<UUID>, and a UTC RFC3339 committed_at. There is
// deliberately no max-age or replay window: a valid committed ACK stays valid.
//
// The parser is pure: it never touches the outbox, logs, or any credential.
func ParseCommitACK(topic string, payload []byte, gatewayID string) (telemetry.CommitACK, error) {
	if len(payload) > maxACKBytes {
		return telemetry.CommitACK{}, ErrACKOversize
	}
	topicID, err := parseOwnACKTopic(topic, gatewayID)
	if err != nil {
		return telemetry.CommitACK{}, err
	}

	fields, err := decodeACKObject(payload)
	if err != nil {
		return telemetry.CommitACK{}, ErrACKMalformed
	}

	protocolVersion, err := decodeACKInt64(fields["protocol_version"])
	if err != nil || protocolVersion != ackProtocolVersion {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	messageType, err := decodeACKString(fields["message_type"])
	if err != nil || messageType != ackMessageType {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	status, err := decodeACKString(fields["status"])
	if err != nil || status != ackStatusCommitted {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	messageIDRaw, err := decodeACKString(fields["message_id"])
	if err != nil {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	messageID, err := uuid.Parse(messageIDRaw)
	if err != nil || messageID == uuid.Nil || messageID.Variant() != uuid.RFC4122 ||
		(messageID.Version() != 4 && messageID.Version() != 7) {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	if messageID != topicID {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	committedAtRaw, err := decodeACKString(fields["committed_at"])
	if err != nil {
		return telemetry.CommitACK{}, ErrACKMalformed
	}
	committedAt, err := time.Parse(time.RFC3339Nano, committedAtRaw)
	if err != nil || !strings.HasSuffix(committedAtRaw, "Z") || committedAt.IsZero() {
		return telemetry.CommitACK{}, ErrACKMalformed
	}

	return telemetry.CommitACK{
		ProtocolVersion: ackProtocolVersion,
		MessageType:     ackMessageType,
		MessageID:       messageID,
		Status:          ackStatusCommitted,
		CommittedAt:     committedAt.UTC(),
	}, nil
}

// parseOwnACKTopic requires exactly the four-segment own ACK topic for the
// configured gateway and returns its canonical UUID. The UUID segment must
// already be in canonical form so a brace-wrapped, URN-prefixed or uppercase
// variant cannot masquerade as the own topic.
func parseOwnACKTopic(topic, gatewayID string) (uuid.UUID, error) {
	segments := strings.Split(topic, "/")
	if len(segments) != 4 || segments[0] != "gateways" || segments[2] != "acks" || segments[1] != gatewayID {
		return uuid.Nil, ErrACKTopic
	}
	id, err := uuid.Parse(segments[3])
	if err != nil || id == uuid.Nil || id.String() != segments[3] {
		return uuid.Nil, ErrACKTopic
	}
	return id, nil
}

// decodeACKObject reads one JSON object, rejecting an unknown or duplicate key
// and any trailing document. It mirrors the strict object decoder used by the
// server telemetry package instead of trusting encoding/json's lenient struct
// decoding.
func decodeACKObject(payload []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '{' {
		return nil, io.ErrUnexpectedEOF
	}
	fields := make(map[string]json.RawMessage, len(ackFieldNames))
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		if _, known := ackFieldNames[name]; !known {
			return nil, io.ErrUnexpectedEOF
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, io.ErrUnexpectedEOF
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if len(fields) != len(ackFieldNames) {
		return nil, io.ErrUnexpectedEOF
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return fields, nil
}

func decodeACKInt64(raw json.RawMessage) (int64, error) {
	if isACKNull(raw) {
		return 0, io.ErrUnexpectedEOF
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

func decodeACKString(raw json.RawMessage) (string, error) {
	if isACKNull(raw) {
		return "", io.ErrUnexpectedEOF
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func isACKNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// ackAdmission is one raw ACK accepted from the broker callback. The topic and
// bytes are copied once so the worker owns an immutable value independent of
// the driver's buffer.
type ackAdmission struct {
	topic   string
	payload []byte
}

// ACKStats is a point-in-time snapshot of the ACK processor counters. The
// admission counters describe what the callback dropped; Acknowledged counts
// only deletes that committed. A malformed, foreign or duplicate ACK changes no
// row and no counter, so there is deliberately no "invalid" or "no-op" count.
type ACKStats struct {
	// Admitted counts admissions accepted into the bounded queue.
	Admitted int64
	// Acknowledged counts ACKs whose pending row delete committed.
	Acknowledged int64
	// DroppedRetained counts retained ACKs rejected at admission.
	DroppedRetained int64
	// DroppedOversize counts ACKs rejected at admission for size.
	DroppedOversize int64
	// DroppedFull counts ACKs dropped because the admission queue was full.
	DroppedFull int64
	// DeleteFailed counts deletes whose outcome was unknown; each is fatal and
	// never counts as an acknowledged success.
	DeleteFailed int64
}

// ACKProcessor is the bounded single-worker ACK path. Admit is the only method
// the broker callback calls, and it returns immediately. One worker goroutine
// strictly parses each admission and deletes exactly the matching pending row
// in one SQLite transaction, then signals the scheduler after a confirmed
// commit.
type ACKProcessor struct {
	gatewayID string
	outbox    *Outbox

	admissions chan ackAdmission
	notify     chan struct{}
	done       chan struct{}
	finished   chan struct{}
	fatalCh    chan struct{}

	started   atomic.Bool
	stopOnce  sync.Once
	fatalOnce sync.Once

	fatalMu  sync.Mutex
	fatalErr error

	admitted        atomic.Int64
	acknowledged    atomic.Int64
	droppedRetained atomic.Int64
	droppedOversize atomic.Int64
	droppedFull     atomic.Int64
	deleteFailed    atomic.Int64
}

// NewACKProcessor builds the ACK processor for the gateway bound by outbox. The
// configured gateway identity is taken from the outbox so a topic check can
// never be satisfied with a different gateway. admissionCapacity sets the fixed
// size of the callback queue and must be within the documented bounds.
func NewACKProcessor(outbox *Outbox, admissionCapacity int) (*ACKProcessor, error) {
	if outbox == nil {
		return nil, fmt.Errorf("gatewaysim: ACK processor requires an outbox")
	}
	if admissionCapacity < minACKAdmissionCapacity || admissionCapacity > maxACKAdmissionCapacity {
		return nil, fmt.Errorf("gatewaysim: ACK admission capacity must be between %d and %d", minACKAdmissionCapacity, maxACKAdmissionCapacity)
	}
	return &ACKProcessor{
		gatewayID:  outbox.gatewayID,
		outbox:     outbox,
		admissions: make(chan ackAdmission, admissionCapacity),
		notify:     make(chan struct{}, 1),
		done:       make(chan struct{}),
		finished:   make(chan struct{}),
		fatalCh:    make(chan struct{}),
	}, nil
}

// Start launches the single ACK worker. A second call is a no-op.
func (p *ACKProcessor) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer close(p.finished)
		p.run()
	}()
}

// Admit is the broker callback path. It rejects a retained or oversized ACK
// with a counter, then copies the topic and payload into the fixed admission
// queue with a non-blocking send. A full queue drops the ACK and keeps the
// pending row in SQLite for resend. It never parses JSON, opens SQL, or spawns a
// goroutine, so it is safe to call from the broker's callback goroutine.
func (p *ACKProcessor) Admit(topic string, payload []byte, retained bool) {
	if retained {
		p.droppedRetained.Add(1)
		return
	}
	if len(payload) > maxACKBytes {
		p.droppedOversize.Add(1)
		return
	}
	admission := ackAdmission{topic: topic, payload: append([]byte(nil), payload...)}
	select {
	case p.admissions <- admission:
		p.admitted.Add(1)
	default:
		p.droppedFull.Add(1)
	}
}

// Notify returns the bounded scheduler wake signal. It receives at most one
// value until drained and is signalled only after a delete commit.
func (p *ACKProcessor) Notify() <-chan struct{} { return p.notify }

// FatalSignal is closed once when the worker stops on an operational failure.
func (p *ACKProcessor) FatalSignal() <-chan struct{} { return p.fatalCh }

// Fatal returns the first operational failure, or nil while healthy.
func (p *ACKProcessor) Fatal() error {
	p.fatalMu.Lock()
	defer p.fatalMu.Unlock()
	return p.fatalErr
}

// Stats returns a snapshot of the ACK counters.
func (p *ACKProcessor) Stats() ACKStats {
	return ACKStats{
		Admitted:        p.admitted.Load(),
		Acknowledged:    p.acknowledged.Load(),
		DroppedRetained: p.droppedRetained.Load(),
		DroppedOversize: p.droppedOversize.Load(),
		DroppedFull:     p.droppedFull.Load(),
		DeleteFailed:    p.deleteFailed.Load(),
	}
}

// Stop closes the worker and waits for it to finish within ctx. It is
// idempotent and safe to call after a fatal stop or without Start. It returns
// ctx.Err() when the worker does not finish in time and otherwise returns the
// operational fatal error, if any. The admissions channel is never closed, so a
// late Admit after Stop is simply dropped rather than sent on a closed channel.
func (p *ACKProcessor) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.done) })
	if !p.started.Load() {
		return p.Fatal()
	}
	select {
	case <-p.finished:
		return p.Fatal()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the single worker loop. It selects on the queue and the stop signal so
// shutdown is cooperative with no closed-channel sends and no leaked
// goroutines. A fatal delete stops the loop; the composition then reconciles
// from the SQLite file under supervision.
func (p *ACKProcessor) run() {
	for {
		select {
		case <-p.done:
			return
		case admission := <-p.admissions:
			if err := p.handleAdmission(context.Background(), admission); err != nil {
				p.setFatal(err)
				return
			}
		}
	}
}

// handleAdmission is the worker's per-item core: strict parse, then exactly one
// DeletePending transaction. It is separated from run so tests can drive it
// deterministically. Pending lookup is by message id in the bound gateway only;
// it is never gated by an in-RAM in-flight map or the current boot, so an early,
// late or prior-boot ACK still deletes its row.
func (p *ACKProcessor) handleAdmission(ctx context.Context, admission ackAdmission) error {
	ack, err := ParseCommitACK(admission.topic, admission.payload, p.gatewayID)
	if err != nil {
		// A malformed, foreign or mismatched ACK is a normal negative case that
		// changes no row and no counter; the item stays pending for resend.
		return nil
	}

	deleted, err := p.outbox.DeletePending(ctx, p.gatewayID, ack.MessageID)
	if err != nil {
		// The delete outcome is unknown, so the worker must not report an
		// acknowledged success, must not retry automatically, and must let the
		// composition stop the simulator and reconcile from the file.
		p.deleteFailed.Add(1)
		return fmt.Errorf("gatewaysim: ACK delete did not commit: %w", err)
	}
	if !deleted {
		// A duplicate ACK after delete, an unknown message id, or a quarantined
		// row is a no-op that leaves every row and counter intact.
		return nil
	}

	p.acknowledged.Add(1)
	p.signalScheduler()
	return nil
}

// signalScheduler wakes the scheduler only after a confirmed delete. The
// channel is buffered to one and the send never blocks, so a slow or absent
// scheduler cannot stall the worker.
func (p *ACKProcessor) signalScheduler() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// setFatal records the first operational failure and closes the fatal signal
// exactly once. It never records a raw error string as a public counter.
func (p *ACKProcessor) setFatal(err error) {
	p.fatalMu.Lock()
	if p.fatalErr == nil {
		p.fatalErr = err
	}
	p.fatalMu.Unlock()
	p.fatalOnce.Do(func() { close(p.fatalCh) })
}
