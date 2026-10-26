# MQTT Application Protocol v1

> Stage 3 uplink contract finalized on 2026-10-15 (Task 3.0). These are
> protocol contracts, not by themselves evidence of runtime behavior. Downlink
> payload/endpoint rules below remain unchanged; for current Stage 5/P6 runtime
> evidence see the [P6 Local MVP report](../backend/paper-p6-dispatch-and-mixed-load.md).

## Identity and topics

Mosquitto authenticates each Gateway with a username equal to its
`gateway_id`. The backend derives the Gateway identity from the ACL-protected
topic. A conflicting `gateway_id` in a payload is rejected.

```text
gateways/<gateway_id>/telemetry/#   Gateway publishes; backend subscribes
gateways/<gateway_id>/acks/#        Backend publishes; Gateway subscribes
gateways/<gateway_id>/status        Gateway publishes; backend subscribes
gateways/<gateway_id>/commands/#    Backend publishes; Gateway subscribes
gateways/<gateway_id>/responses/#   Gateway publishes; backend subscribes
```

`acks/#` is reserved for an application-level confirmation that the backend
database transaction committed. An MQTT QoS acknowledgement is not this
confirmation. A Gateway must not publish to `acks/#`.

The concrete v1 telemetry topic is
`gateways/<gateway_id>/telemetry/<sensor_id>` (exactly four nonempty segments).
Both identifiers must match the payload. The backend subscribes to the literal
filter `gateways/+/telemetry/#`, as required by its DynSec `subscribeLiteral`
ACL; subscribing to a different, narrower filter is not equivalent permission.
Gateway usernames equal their Gateway IDs. A normal MQTT subscriber does not
receive the original publisher's authenticated username: its identity boundary
is the broker-enforced topic ACL, not an identity claim inside JSON.

Telemetry uses QoS 1; telemetry and application ACKs use retain=false. Ingress
must reject retained telemetry deliveries. Data clients use clean sessions,
resubscribe after reconnect and check SUBACK. The producer's durable outbox,
not the MQTT session, owns application retries. TLS must verify CA and hostname.

## Telemetry batch

```json
{
  "protocol_version": 1,
  "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "message_type": "telemetry_batch",
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "boot_id": "7f2c45f7-0a76-4e28-8a37-7793d7d85b04",
  "first_sequence": 12501,
  "sample_count": 3,
  "measured_at": "2026-08-21T10:15:00.000Z",
  "sample_interval_us": 10000,
  "samples": [25.1, 25.2, 25.3]
}
```

- `message_id` is a UUIDv4 or UUIDv7 generated once from a cryptographically
  secure source.
- `measured_at` is the time of the first sample. Sample `i` occurred at
  `measured_at + i * sample_interval_us`.
- `sample_count` must equal the length of `samples` and must be positive.
- A retry reuses the exact topic and serialized payload, including IDs,
  sequence range and timestamps.
- The Gateway stores the serialized message in a durable outbox before publish
  and deletes it only after receiving a successful commit ACK.
- `(gateway_id, message_id)` identifies a retry. The same ID and payload hash is
  accepted idempotently; the same ID with a different hash is a protocol error.

Operational limits such as maximum payload bytes and samples per message are
configured by deployment and validated by the backend.

### Field validation

All fields in the example are required; reject unknown fields, null values,
malformed JSON and trailing JSON documents. Integer fields must be JSON integers,
not fractional values or strings. Reject the whole message on any invalid field.

| Field | Accepted input |
| --- | --- |
| `protocol_version`, `message_type` | Integer `1`, string `telemetry_batch`. |
| `message_id` | Non-nil RFC UUIDv4/v7, generated once with a cryptographically secure source. |
| `gateway_id`, `sensor_id` | ASCII `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`; Gateway additionally rejects reserved `backend_service`, matching provisioning validators. Exact topic match required. |
| `boot_id` | Non-nil RFC UUID, fixed for one process boot; sequence allocation is per Sensor within that boot. |
| `first_sequence` | Integer >= 0; last sequence `first_sequence + sample_count - 1` must fit signed BIGINT. |
| `sample_count` | Positive integer equal to `len(samples)`, within configured maximum. |
| `measured_at` | RFC3339 with timezone, normalized to UTC, precision no finer than microseconds; reject finer precision rather than rounding. Must be representable by the selected Go/PostgreSQL timestamp path (calendar years 0001–9999). |
| `sample_interval_us` | Positive integer microseconds, within signed BIGINT and safe timestamp/duration arithmetic, and at most 2^53 (9007199254740992) so temporal `value_number` (DOUBLE PRECISION) stores it exactly. |
| `samples` | Nonempty array of finite float64 numbers, no null, NaN, Infinity or overflow. |

For zero-based sample index i, `sequence_num = first_sequence + i` and
`measured_at_i = measured_at + i * sample_interval_us`. Validate arithmetic
before expansion, including overflow and the last timestamp. Batch duration
means the sample span `(sample_count - 1) * sample_interval_us`, not elapsed
MQTT transmission time. Even a one-sample batch must have a positive interval
representable by Go's duration arithmetic and no larger than 2^53, the largest
integer an IEEE-754 double (the temporal `sample_interval_us` value column)
represents exactly.

Task 3.0 demo profile: 65,536 payload bytes, at most 1,000 samples/message,
60s sample span and 2m future skew relative to backend receipt time, checked
against the last sample. Defaults are configurable; no maximum observation age
is imposed initially. Old observations can be replayed from an offline outbox.
The current runtime has not yet wired these limits (its existing payload default
is 1 MiB). See [Task 3.0 configuration contract](../backend/stage-3-task-3.0-baseline-and-contract.md).

Gateway, Sensor, Sensor Twin and state must already exist under the correct
Gateway. Reject unknown resources or an incomplete provisioning graph. Incoming
telemetry never creates sensors/entities or changes resource ownership.

### Retry identity and atomic persistence

`payload_hash` is lowercase hexadecimal SHA-256 of the original payload bytes,
not reserialized or canonical JSON. Store `source_topic` alongside it. The
deduplication key remains `(gateway_id, message_id)` across all Sensors.
Changing whitespace or key order changes the bytes and therefore is not a valid
retry. Hash and original topic must both match the committed ledger entry.

One PostgreSQL transaction covers the dedup marker, all raw samples, the
conditional Sensor reported-state update and temporal metadata. Lock the Sensor
consistently while checking/inserting sequence ranges. No partial sample insert
may be reported as a fully committed batch.

| Delivery | Persistence and application ACK |
| --- | --- |
| New ID, valid nonoverlapping range | Store the whole batch atomically, then ACK after confirmed commit. |
| Same Gateway/ID/hash/topic, already committed | Do not insert or increment versions again; ACK again, including simultaneous retries. |
| Same Gateway/ID, different hash or topic | Protocol conflict; leave committed data unchanged; no committed ACK. |
| New ID, range overlaps a committed range in the same Gateway/Sensor/boot | Reject the whole batch, even if values match; no committed ACK. |
| New ID, gap or late nonoverlapping range | Keep every raw sample at measurement time; do not fabricate missing samples; ACK after commit. |
| Different boot, reused sequence | Independent stream; persist if otherwise valid; compare observation times for current state. |

Legacy markers without the new topic/range provenance cannot establish this
contract by assumption. They require supervised disposition; never invent a
matching provenance, delete the ledger or ACK uncertain legacy payloads.

## Boot and sequence ordering

`boot_id` is generated once per Gateway process boot and remains fixed during
that boot. `first_sequence` is monotonically increasing within one
`(gateway_id, sensor_id, boot_id)` stream. The sequence range is:

```text
[first_sequence, first_sequence + sample_count - 1]
```

Overlaps are rejected and gaps/late nonoverlapping ranges are accepted as above;
MQTT arrival order is never measurement order. A UUID
`boot_id` does not identify which boot is newer. Current reported state is
advanced by observation timestamp and the same-boot tie-breaker below, under a
transaction-safe database condition, so delayed older observations cannot roll
state back.

For equal observation timestamps, advance current state only when both boot IDs
match and the candidate sequence is larger. Across different boots with equal
timestamps, preserve current state. Raw and temporal history still retain the
accepted observation; no UUID ordering is used to select a newer boot.

## Digital Twin identity and storage

Gateway and Sensor entity identifiers are stable:

```text
urn:ngsi-ld:Gateway:<gateway_id>
urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>
```

`sensor_id` is unique only within a Gateway. Provisioning restricts
`gateway_id` and `sensor_id` to identifier-safe ASCII characters so the URN is
unambiguous.

Raw high-frequency samples are stored once in `telemetry`. Selected state
changes and inference values are stored in `twin_temporal_values`; the backend
does not duplicate every 100 Hz sample into both hypertables.

Stage 3 updates the provisioned Sensor Twin with a server-allowlisted merge of
`value`, `observed_at`, `boot_id`, `sequence_num`, `sample_interval_us` from the
batch's final sample. Preserve other reported properties. Unit comes from
provisioned sensor metadata. `last_reported_at` is measurement time;
`updated_at` is server write time. Increase `reported_version` only if the
current observation advances. Never change `desired_state`, `desired_version`,
`last_desired_at` or `last_desired_by` from telemetry. Do not infer Gateway
online/offline status or command execution from a sensor observation.

Store one `twin_temporal_values` observation per new accepted batch:
`property_name=sample_interval_us`, `value_number=sample_interval_us`,
`observed_at` equal to the last sample time, `message_id` from the batch,
metadata containing `unit=us`, `boot_id`, `source=telemetry_batch`.
Use exactly one typed value column. Late batches append at measurement time;
retries append nothing. This property records observed sample timing, not a
confirmed desired configuration. Raw `value` is not copied into this hypertable.
AI inference fields are not part of this telemetry v1 payload.

For the three-sample example above:

| Index | Sequence | UTC measurement time | Raw value |
| --- | --- | --- | --- |
| 0 | 12501 | 2026-08-21T10:15:00.000000Z | 25.1 |
| 1 | 12502 | 2026-08-21T10:15:00.010000Z | 25.2 |
| 2 | 12503 | 2026-08-21T10:15:00.020000Z | 25.3 |

Its topic is `gateways/gateway_001/telemetry/sensor_001`. Assuming no newer
current observation, reported state becomes value 25.3 / sequence 12503 at
`.020000Z`; the temporal interval value is 10000 us at the same time.

## Downlink commands

### Dispatcher operation (P6; no payload or endpoint change)

The backend may be configured to wake its existing dispatcher after a confirmed
command-admission commit. This is an internal hint, not an MQTT client behavior
or a change to the command contract:

- `STAGE5_WAKEUP_ENABLED` is read from the backend process environment and is
  mapped by Compose; its default is `false`. With it off, startup and periodic
  polling continue as before. With it on, a bounded/coalescing notification
  prompts discovery sooner, while startup and periodic scans remain recovery
  paths for missed hints and process restarts.
- `STAGE5_PUBLISH_QUEUE_CAPACITY` defaults to `32` (accepted range 1–4096).
  `STAGE5_SCAN_BATCH` independently defaults to `32` (range 1–1024), and
  `STAGE5_PUBLISH_WORKERS` defaults to `1` (range 1–16). The P6 tested profile
  is therefore scan batch 32, pending-job queue 32, and one publisher worker;
  it was not capacity-tuned. A full in-memory publish queue leaves the durable
  outbox intent pending for a later scan; queue-full is not successful
  publication. Worker-local jobs are bounded by worker count and payload size
  is capped by the configured Stage 5 8 KiB limit. The separate result queue
  defaults to 128 (range 1–4096), with result workers defaulting to 2
  (range 1–16).
- Status meanings do not change: `pending` is committed intent,
  `published` means broker PUBACK only (not execution), and `succeeded` requires
  a valid result accepted by the backend result transaction. Stable command
  bytes and normal TTL/terminal-state checks remain in force.
- Set `STAGE5_WAKEUP_ENABLED=false` to return to polling-only dispatch. This is
  not a rollback of schema migration 24. Deployment defaults for Stage 5 and
  wake-up remain disabled; check the P6 report and operations runbook before
  any isolated fixture exercise. Clients must not publish commands directly
  to MQTT, and this note does not broaden authorization or ACL scope.

Commands originate from authorized user requests through the backend and are
dispatched via the transactional outbox to:

```text
gateways/<gateway_id>/commands/<command_id>
```

Illustrative payload (P3 Sensor configuration control):

```json
{
  "protocol_version": 1,
  "message_type": "command",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "action": "update_configuration",
  "parameters": {
    "sample_interval_us": 10000000
  },
  "desired_version": "3",
  "issued_at": "2026-10-25T08:00:00.000000Z",
  "expires_at": "2026-10-25T08:01:00.000000Z"
}
```

Rules:
- QoS 1, retain=false. Commands are strictly directed to provisioned `Sensor` entities.
- `command_id` is a UUID uniquely identifying the command instance.
- `desired_version` is a non-negative string-encoded BIGINT.
- `sample_interval_us` is an integer between 10.000 and 2^53 (9007199254740992).
- The Gateway must verify `expires_at` and `desired_version`; expired commands are rejected
  with status `failed` and error code `COMMAND_EXPIRED`. Commands with `desired_version <= current`
  are rejected with `STALE_DESIRED_VERSION`.
- The Gateway stores recently processed command receipts and results durably in SQLite.
  Duplicate command delivery with the same ID and content returns the cached execution result
  without re-executing or resetting sampling timers. Same ID with different payload is rejected
  as an identity conflict.

## Command execution results

After executing a command, the Gateway publishes the execution result to:

```text
gateways/<gateway_id>/responses/<command_id>
```

Illustrative payload:

```json
{
  "protocol_version": 1,
  "message_type": "command_result",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "desired_version": "3",
  "status": "succeeded",
  "reported_state": {
    "sample_interval_us": 10000000
  },
  "completed_at": "2026-10-25T08:00:00.250000Z"
}
```

Rules:
- QoS 1, retain=false.
- `status` must be either `succeeded` or `failed`.
- When `status` is `failed`, `error_code` from an allowlist (`COMMAND_EXPIRED`, `STALE_DESIRED_VERSION`,
  `UNKNOWN_SENSOR`, `UNSUPPORTED_CONFIGURATION`, `APPLY_FAILED`) and a safe `error_message` are provided.
- `reported_state` contains confirmed configuration following apply and readback; it is omitted on failure.
- The backend correlates the result with the active intent and commits status and `reported.configuration`
  atomically. Terminal states cannot regress, and late results after timeout are stored as immutable
  late evidence without overwriting newer configuration.
- Broker PUBACK does not prove backend database acceptance. Gateway resends unacknowledged cached results
  periodically with bounded exponential backoff until the command window expires.

## Gateway status and heartbeat

Gateways publish periodic status or LWT (Last Will and Testament) events to:

```text
gateways/<gateway_id>/status
```

Illustrative payload:

```json
{
  "protocol_version": 1,
  "message_type": "status",
  "gateway_id": "gateway_001",
  "status": "online",
  "timestamp": "2026-09-28T10:00:00.000Z"
}
```

Valid statuses are `online` and `offline`.

## Application-level commit ACKs

When the backend commits a batch of telemetry to TimescaleDB, it publishes an ACK to:

```text
gateways/<gateway_id>/acks/<message_id>
```

Illustrative payload:

```json
{
  "protocol_version": 1,
  "message_type": "telemetry_ack",
  "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "status": "committed",
  "committed_at": "2026-09-28T10:15:00.050Z"
}
```

Receipt of this ACK authorizes the Gateway to purge the message from its durable local outbox.

The ACK example correlates the same message ID as the telemetry example (its
timestamp is illustrative). Validate the own-Gateway topic, ID, protocol version,
message type and `status=committed` before deleting exactly that pending item.
Unknown IDs, mismatched topic/body, invalid or duplicate ACKs must not delete
other items. ACK publication uses QoS 1, retain=false.

`committed_at` records when the backend confirmed a successful commit, or a
visible committed ledger entry on retry. It is not PostgreSQL's exact physical
commit timestamp and need not remain identical across repeated ACKs.
`processed_at` is ledger write time and must not be mislabeled as commit time.

Queue acceptance, MQTT PUBACK, an uncommitted ledger row, rollback, database
timeout or uncertain COMMIT never authorizes a committed ACK. When COMMIT is
uncertain, resend later and resolve against the ledger. If the database committed
but ACK publication failed, keep the data and ACK a matching retry; do not undo
the committed batch. There is no database ACK outbox or negative ACK in v1.
Malformed/conflicting items remain pending for operator inspection/quarantine;
producer retry and storage are bounded, with no tight retry loop or automatic
message-ID regeneration. This is at-least-once delivery with application
idempotency, not exactly-once delivery.
