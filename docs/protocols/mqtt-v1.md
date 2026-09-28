# MQTT Application Protocol v1

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

## Boot and sequence ordering

`boot_id` is generated once per Gateway process boot and remains fixed during
that boot. `first_sequence` is monotonically increasing within one
`(gateway_id, sensor_id, boot_id)` stream. The sequence range is:

```text
[first_sequence, first_sequence + sample_count - 1]
```

Gaps, overlaps and out-of-order ranges are recorded or rejected according to
the ingress policy; MQTT arrival order is never measurement order. A UUID
`boot_id` does not identify which boot is newer. Current reported state is
advanced only by a newer observation timestamp under a transaction-safe
database condition, so a delayed message from an older boot cannot roll state
back.

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

## Downlink commands

Commands originate from authorized user requests through the backend and are
dispatched via the transactional outbox to:

```text
gateways/<gateway_id>/commands/<command_id>
```

Illustrative payload:

```json
{
  "protocol_version": 1,
  "message_type": "command",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "entity_id": "urn:ngsi-ld:Gateway:gateway_001",
  "action": "update_configuration",
  "parameters": {
    "sampling_interval_seconds": 5
  },
  "desired_version": 12,
  "issued_at": "2026-09-16T08:31:00Z",
  "expires_at": "2026-09-16T08:32:00Z"
}
```

Rules:
- `command_id` is a UUIDv4/UUIDv7 uniquely identifying the command instance.
- The Gateway must verify `expires_at`; expired commands must be rejected with
  status `failed` and error code `COMMAND_EXPIRED`.
- The Gateway stores recently processed `command_id` values in its durable outbox/cache.
  If a duplicate command arrives due to MQTT at-least-once delivery, the Gateway
  re-publishes the cached result instead of executing the physical action twice.

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
  "status": "succeeded",
  "reported_state": {
    "sampling_interval_seconds": 5
  },
  "completed_at": "2026-09-16T08:31:03Z"
}
```

Rules:
- `status` must be either `succeeded` or `failed`.
- When `status` is `failed`, `error_code` and a safe `error_message` should be provided.
- `reported_state` contains the confirmed physical state following execution.
- The backend reconciles the command status in `twin_commands` and updates `reported_state`
  in `twin_states` in a single database transaction.

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
