// Package mqttingress contains the transport-independent telemetry ingress
// core: bounded admission, a single bounded queue, a fixed worker pool, and the
// post-commit application ACK path.
//
// It owns no MQTT client, TLS, reconnect, broker configuration or Paho types. A
// transport adapter feeds Admit and implements Publisher. Every admitted
// delivery traverses exactly one pipeline, in this order:
//
//	Validator.Validate -> Repository.Persist -> post-commit ACK publish
//
// The callback-side contract (Admit) performs only a cheap topic shape check,
// a size check and a copy; it never decodes JSON, touches the database, spawns
// a goroutine per message, or holds a retry queue.
//
// Failure semantics (no silent success):
//
//   - Pre-commit rejection: retained/oversized/invalid-topic/queue-full
//     deliveries, and validation or Persist failures, produce no database write
//     and no application ACK. The Gateway's durable outbox retries.
//   - Post-commit publish-unconfirmed: once Persist confirms a commit or a
//     committed duplicate, the database result is final and is never rolled
//     back. If the ACK publish then fails or times out, the core records
//     ack_publish_unconfirmed and does not retry in-process. A later Gateway
//     resend of the same message_id is answered by the duplicate branch.
package mqttingress

import (
	"context"
	"time"

	"iot-platform/internal/telemetry"
)

// Validator validates one topic/payload pair at the caller-supplied receipt
// time and returns an immutable message. *telemetry.Validator satisfies it.
type Validator interface {
	Validate(topic string, payload []byte, receivedAt time.Time) (*telemetry.Message, error)
}

// Repository persists one already-validated message and reports the commit
// outcome. telemetry.Repository satisfies it. The core calls Persist at most
// once per admitted delivery and never retries the payload inside the process;
// a Gateway resend of the same message is handled by the existing
// (gateway_id, message_id) deduplication as a duplicate.
type Repository interface {
	Persist(context.Context, *telemetry.Message) (telemetry.PersistResult, error)
}

// Delivery is one post-commit application ACK publication request. QoS and
// Retained are carried on the request so a transport adapter maps them to the
// broker without re-deriving policy; the core always sends QoS 1, non-retained.
type Delivery struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
}

// Publisher publishes a Delivery to the broker.
//
// Responsibility boundary: Publish must honor ctx and must bound the *entire*
// underlying operation, including any broker token wait and any synchronous
// broker write, to a finite lifetime derived from ctx (or from the adapter's own
// configured publish timeout). It must not outlive ctx. When that bound expires
// or the connection is lost, the adapter is responsible for interrupting
// (closing) the affected connection to unblock the broker call, for bounding
// how many publish invocations may be in flight, and for resetting or tearing
// down the affected connection lifetime before accepting further work; it
// returns an error. It must not accumulate unbounded goroutines or replacement
// connections. The core then records ack_publish_unconfirmed, keeps the
// committed database result (never rolls it back), and does not retry the ACK
// in-process.
//
// A returned error is never proof that the ACK failed to reach the Gateway, and
// a returned nil is never proof that the Gateway processed the ACK.
type Publisher interface {
	Publish(ctx context.Context, delivery Delivery) error
}

// Compile-time conformance of the reused telemetry boundary types.
var (
	_ Validator  = (*telemetry.Validator)(nil)
	_ Repository = telemetry.Repository(nil)
)
