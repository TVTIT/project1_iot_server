# AGENTS.md

## 1. Project identity

This repository contains the main implementation of a university IoT
Gateway–Server project (`Đồ án 1`). The system receives sensor data from
heterogeneous Gateways, stores time-series measurements, provides historical
queries and near-real-time streaming to users, authenticates users, and allows
authorized Gateways to upload stored media. The backend also owns a Digital
Twin module that represents Gateways, sensors, and controllable devices; keeps
their current reported and desired states; stores temporal property history;
and coordinates authorized MQTT commands and execution feedback.

The project is implemented by one student. This is the student's first large
university project, and the student has limited experience with Go, Mosquitto,
and MQTT. Prefer the smallest complete and explainable solution over
microservices, premature optimization, or infrastructure that is not required
by the MVP.

Schedule:

- Project start: 2026-08-24.
- Target system completion: 2026-12-31.
- The student has more implementation time during September and October, so
  tasks in those months may be scheduled more aggressively.
- Build and verify a local proof of concept before expanding to the main MVP.

Current progress snapshot supplied by the user on 2026-09-16:

- A local proof of concept has demonstrated
  `Gateway -> MQTT -> Go -> PostgreSQL`.
- Mosquitto, Portmap/TCP, MQTT TLS/CA, per-Gateway credentials, and ACLs have
  been implemented at prototype level.
- Supabase Auth and Supabase Storage have been completed at prototype level.
- Digital Twin, TimescaleDB temporal history, the complete business API,
  realtime client delivery, and end-to-end command reconciliation remain to be
  implemented and verified.

Treat this snapshot as planning context, not proof that a repository feature is
correct. Inspect the code, migrations, configuration, and tests before claiming
that a feature is complete.

Unless the user requests otherwise, explain decisions and write report-ready
text in Vietnamese. Keep code, identifiers, protocol fields, and technical
product names in English.

## 2. MVP scope

The main project must provide these end-to-end capabilities:

1. A Gateway publishes batches of sensor samples through MQTT over TLS.
2. Mosquitto authenticates and authorizes each Gateway independently.
3. Go receives, validates, queues, deduplicates, and stores telemetry.
4. PostgreSQL with TimescaleDB stores relational and time-series data.
5. Users log in with Supabase Auth and call protected Go APIs with a JWT.
6. The Go API checks which Gateways the authenticated user may access.
7. REST APIs return historical data for a requested time interval.
8. WebSocket sends newly committed telemetry to authorized clients.
9. An authenticated Gateway obtains a signed upload URL from Go and uploads an
   image directly to private Supabase Storage.
10. Go maintains a Digital Twin entity for every managed Gateway and sensor,
    including relationships, current `reported_state`, and current
    `desired_state`.
11. Telemetry and AI inference values update Digital Twin reported properties
    and append temporal history to TimescaleDB.
12. An authorized client changes desired state through the Go API; Go records a
    command, publishes it through MQTT, receives Gateway feedback, and updates
    command and reported-state status.
13. A simple Flutter client demonstrates Auth, Gateway lists, current Digital
    Twin state, historical charts, realtime charts, stored-image access, and a
    small set of safe device configuration commands.

The following features are deferred unless the user explicitly restores them:

- gRPC and gRPC-Web.
- A full alert engine.
- LLM-generated commands or autonomous control policies.
- A complete third-party NGSI-LD Context Broker deployment such as Scorpio or
  Orion-LD.
- Full ETSI NGSI-LD API conformance beyond the endpoints and entity mappings
  explicitly implemented and tested by this project.
- Live-video streaming or a dedicated media server.
- Large-video upload, resumable upload, and transcoding.
- A custom administration application.
- Advanced observability, orchestration, or multi-node high availability.

Do not let deferred features complicate the MVP interfaces or schedule.

## 3. Selected architecture

Treat these as selected decisions, not alternatives to compare again:

- Go backend as a modular monolith.
- REST for request/response application APIs.
- WebSocket for near-real-time client delivery.
- Mosquitto as the MQTT broker.
- PostgreSQL with the TimescaleDB extension.
- Self-hosted Supabase Auth, Storage, Studio, and the Supabase API Gateway.
- Flutter for a simple MVP client; prefer one platform, normally Android.
- Nginx as the public HTTP reverse proxy.
- Cloudflare Tunnel for public HTTP/HTTPS access.
- Portmap/TCP forwarding for public MQTT TCP reachability when required.
- Docker Compose for development and server deployment.
- Basic CI for format, lint, unit tests, migration checks, and image builds.
- A Digital Twin module inside the Go modular monolith; do not split it into a
  separate microservice for the MVP.
- NGSI-LD-compatible entity identifiers and JSON-LD representations at the API
  boundary, while PostgreSQL remains the authoritative store.
- PostgreSQL JSONB plus relational constraints for current Digital Twin state;
  TimescaleDB hypertables for temporal property values.
- A transactional outbox for reliable MQTT command publication and a
  reconciliation worker for desired-versus-reported state convergence.

Do not replace these choices without an explicit user request. In particular:

- Do not propose InfluxDB as the primary database. TimescaleDB was selected
  because the project needs time-series operations and relational joins for
  permissions in the same PostgreSQL ecosystem.
- Do not propose Keycloak as the default human-user identity system. Supabase
  Auth was selected, and the user already knows its Flutter SDK.
- Do not expose PostgREST as the public business API. The Go backend owns all
  application APIs. PostgREST may remain internal if the self-hosted Supabase
  stack requires it.
- Do not reintroduce gRPC merely for performance. REST and WebSocket were chosen
  to reduce Protobuf, code-generation, gRPC-Web, and proxy complexity.
- Do not send large images or video through MQTT.
- Do not add Scorpio, Orion-LD, Kafka, Redis, or another database merely to
  implement the first Digital Twin vertical slice. Add such infrastructure only
  when the user explicitly requests it and the repository contains a measured
  need.
- Do not persist Digital Twin state in JSON files or an in-memory Go map. A Go
  map may be a bounded cache only; PostgreSQL is the source of truth.

## 4. Network topology and traffic flows

### 4.1 Client and Go business API

```text
Flutter/Web
    -> HTTPS
    -> Cloudflare
    -> Cloudflare Tunnel
    -> Nginx
    -> Go REST API / WebSocket
```

REST is used for Gateway lists, sensor metadata, historical queries, media
metadata, and signed-read-URL requests. WebSocket is used only for new realtime
telemetry events.

### 4.2 Client and Supabase services

```text
Flutter/Web
    -> HTTPS
    -> Cloudflare
    -> Cloudflare Tunnel
    -> Nginx
    -> Supabase API Gateway
        -> Supabase Auth
        -> Supabase Storage
```

Nginx and the Supabase API Gateway have different responsibilities:

- Nginx is the external reverse proxy. It manages public hostnames/routes,
  request-size limits, timeouts, proxy headers, and WebSocket upgrade headers.
- The Supabase API Gateway is an internal component of the self-hosted stack. It
  routes `/auth/v1/*`, `/storage/v1/*`, and other required Supabase paths and
  applies Supabase-specific CORS, API-key, and header behavior.
- Depending on the pinned self-hosted Supabase version, the API Gateway may be
  Envoy or Kong. Follow the selected upstream Compose version instead of mixing
  configurations from different releases.
- Do not proxy public requests directly from Nginx to the Auth or Storage
  containers unless intentionally replacing and fully reproducing the API
  Gateway behavior.

Recommended public routing:

```text
api.example.com       -> Nginx -> Go Backend
supabase.example.com  -> Nginx -> Supabase API Gateway
studio.example.com    -> protected management route only
```

Internal PostgreSQL, Auth, Storage, Studio, and API Gateway service ports must
not be exposed directly to the Internet.

### 4.3 Gateway telemetry

```text
Gateway
    -> MQTT over TLS
    -> Portmap/TCP forwarding when required
    -> Mosquitto
    -> Go MQTT subscriber
    -> bounded in-memory queue
    -> fixed worker pool
    -> PostgreSQL + TimescaleDB
    -> authorized WebSocket clients
```

Portmap provides TCP reachability only. It does not provide encryption,
authentication, authorization, deduplication, or reliable persistence.

### 4.4 Gateway media upload

```text
Gateway
    -> authenticated signed-URL request
    -> Nginx -> Go Backend
    <- short-lived signed upload URL
    -> HTTPS upload
    -> Nginx -> Supabase API Gateway -> Supabase Storage
    -> Go validates metadata and records status in PostgreSQL
```

The Gateway must never receive or store the Supabase `service_role` key.

### 4.5 Digital Twin uplink synchronization

```text
Gateway
    -> MQTT telemetry/status/response
    -> Mosquitto
    -> Go Digital Twin ingress
    -> validate identity, schema, time, and idempotency
    -> update reported state in PostgreSQL
    -> append temporal properties to TimescaleDB
    -> publish an authorized realtime event after commit
```

The incoming MQTT topic and authenticated MQTT username identify the Gateway.
Do not trust a conflicting `gateway_id` inside the payload. Telemetry updates
only `reported_state`; it must never overwrite `desired_state`.

### 4.6 Digital Twin downlink control

```text
Flutter/Web
    -> HTTPS + Supabase JWT
    -> Go authorization and command validation
    -> transaction: desired state + command + MQTT outbox
    -> outbox publisher
    -> Mosquitto
    -> Gateway executes command
    -> MQTT acknowledgement/result/reported state
    -> Go updates command and Digital Twin
    -> authorized client receives the committed result
```

All application-level Gateway configuration and control commands must pass
through the Go Digital Twin module. Clients must not publish directly to MQTT.
An accepted API request means that the command was recorded; it does not mean
that the physical Gateway has executed it successfully.

### 4.7 Digital Twin read flows

```text
Current state: Client -> Go REST API -> PostgreSQL
History:       Client -> Go REST API -> TimescaleDB
Realtime:      committed state/event -> Go WebSocket -> authorized client
```

Do not rebuild current state by scanning the full temporal history for every
request. PostgreSQL keeps the latest state, while TimescaleDB keeps append-only
historical observations and state changes.

## 5. Service responsibilities

### 5.1 Nginx

- Provide one controlled HTTP entry point behind Cloudflare Tunnel.
- Route Go API/WebSocket and Supabase API Gateway traffic by hostname or path.
- Preserve `Authorization`, forwarded-host/protocol, and WebSocket upgrade
  headers.
- Apply appropriate request-size and timeout limits, especially for media.
- Produce access/error logs without logging bearer tokens or signed URLs.
- Do not attempt to replace Supabase-specific API Gateway behavior.

### 5.2 Mosquitto

- Accept telemetry messages from Gateways.
- Use TLS, persistence where appropriate, unique credentials, ACLs, Last Will
  where useful, and configurable packet-size limits.
- Reject anonymous access.
- Isolate every Gateway to its own MQTT topic namespace.
- Do not perform application validation or database persistence; those belong
  to Go.

### 5.3 Go backend

Keep the implementation as a modular monolith. Logical modules include:

- Configuration, structured logging, health endpoints, and graceful shutdown.
- REST routing and middleware.
- Supabase JWT validation.
- User-to-Gateway application authorization.
- Gateway and sensor management.
- Gateway MQTT and HTTP credential provisioning/revocation.
- MQTT subscriber, payload validation, queue, and worker pool.
- Telemetry deduplication and transactional persistence.
- Historical queries and adaptive `time_bucket` aggregation.
- Authenticated WebSocket connections and realtime fan-out.
- Media upload authorization, signed URLs, metadata, and validation status.
- Digital Twin entity and relationship management.
- NGSI-LD mapping at the API boundary.
- Reported-state ingestion and desired-state updates.
- Command validation, persistence, transactional outbox publication, execution
  acknowledgement, timeout, retry, and reconciliation.
- Temporal Digital Twin property persistence and history queries.

MQTT callbacks must do only cheap parsing/copying and enqueue bounded work. Do
not create an unbounded goroutine per MQTT message.

### 5.4 PostgreSQL and TimescaleDB

- PostgreSQL stores application profiles, Gateway ownership/permissions,
  sensors, media metadata, Gateway credential metadata, and deduplication
  records. It also stores Digital Twin entities, relationships, current state,
  commands, and MQTT outbox rows.
- TimescaleDB hypertables store raw time-series samples.
- TimescaleDB also stores temporal Digital Twin property observations, including
  telemetry values and AI inference values such as `reconstruction_loss` or
  `anomaly_score` when those fields are present in the selected model.
- SQL joins determine whether a user may access a Gateway and therefore its
  sensors, telemetry, realtime stream, and media.
- Use migrations for schemas, constraints, indexes, hypertables, and policies.
- Pin and test compatible PostgreSQL, TimescaleDB, and self-hosted Supabase
  versions. Do not assume an arbitrary Supabase PostgreSQL image already
  contains the required TimescaleDB extension.
- Prefer one documented PostgreSQL topology. Do not silently introduce a second
  application database merely to avoid configuration work.

### 5.5 Supabase Auth

- Handles human-user identity, login, logout, password reset, access tokens,
  refresh tokens, and optional social login.
- Flutter calls Auth through Nginx and the Supabase API Gateway.
- Flutter sends the returned access token as `Authorization: Bearer <JWT>` to
  the Go API.
- Go validates signature, issuer, audience, expiry, and required claims using
  the configured Supabase signing keys/JWKS.
- Do not call Auth on every Go request when local JWT verification is possible.
- A valid JWT proves human identity only. It does not prove permission to a
  requested Gateway.

### 5.6 Supabase Storage

- Use private buckets, initially for images only.
- Storage receives file bytes and verifies its signed upload token.
- Go decides whether a Gateway may upload, selects the bucket and object path,
  restricts expected size/type, and issues a short-lived signed URL.
- After upload, Go or a bounded worker validates declared size/type, file magic
  bytes, checksum, and business metadata when required.
- Users receive signed read URLs only after Go verifies User–Gateway access.
- Storage RLS remains defense in depth and must match the Gateway permission
  model where direct SDK access is allowed.
- Supabase Storage is for stored files, not live-video streaming.

## 6. Identity and authorization model

Human-user and Gateway identities are separate.

### 6.1 Human users

- Supabase Auth owns human accounts and sessions.
- `profiles.id` references `auth.users.id`.
- `user_gateways` records which users may access which Gateways and their role.
- Go must check `user_gateways`; never authorize solely because a JWT is valid
  or because the client supplied a `gateway_id`.

### 6.2 Gateway MQTT identity

Each Gateway has:

- MQTT username equal to its `gateway_id`.
- A unique random MQTT password with at least 128 bits of entropy.
- The broker CA certificate/trust bundle.
- No shared MQTT password and no Supabase human account.

### 6.3 Gateway HTTP identity

If a Gateway calls the signed-upload REST endpoint, use a separate
Gateway-specific HTTP credential or short-lived Gateway JWT issued by Go. Do
not reuse a human Supabase account, a Supabase `service_role` key, or assume the
MQTT password is automatically an HTTP bearer token.

Provision, rotate, and revoke MQTT and HTTP credentials independently. Never
log plaintext credentials or tokens.

### 6.4 Expected relational entities

- `profiles`
- `gateways`
- `user_gateways`
- `sensors`
- `telemetry`
- `processed_messages`
- `media_objects`
- `twin_entities`
- `twin_relationships`
- `twin_states`
- `twin_commands`
- `twin_outbox`
- `twin_temporal_values`
- Gateway HTTP credential/token metadata where required

Typical authorization chain:

```text
auth.users / profiles
    -> user_gateways
    -> gateways
    -> sensors / telemetry / realtime / media_objects
    -> twin_entities / twin_states / twin_commands / twin_temporal_values
```

Use parameterized SQL, least-privilege database roles, and transactions for
related writes.

## 7. MQTT security baseline

Mosquitto configuration must include:

- `allow_anonymous false`.
- A `password_file` managed by `mosquitto_passwd`; never plaintext passwords.
- The strongest password derivation supported by the pinned Mosquitto version.
  If the report needs the previously selected term, document
  `sha512-pbkdf2` accurately for that version.
- MQTT over TLS so usernames and passwords are not exposed in transit.
- `acl_file` or the Dynamic Security plugin for per-Gateway topic isolation.

Minimum topic namespace:

```text
gateways/<gateway_id>/telemetry/#   Gateway may publish
gateways/<gateway_id>/acks/#        Gateway may publish when app ACK is used
gateways/<gateway_id>/status        Gateway may publish when status is in scope
gateways/<gateway_id>/responses/#   Gateway may publish command results
gateways/<gateway_id>/commands/#    Gateway may subscribe; Go may publish
```

Prefer `%u` ACL patterns so the authenticated MQTT username can access only its
own namespace. A Gateway must not publish to `commands/#` or subscribe to
another Gateway's namespace. The backend MQTT principal has only the cross-
Gateway publish/subscribe permissions required by its ingress and command
roles. A leaked credential must be revocable for one Gateway without rotating
every Gateway.

Provision new credentials through an authenticated admin operation. Generate
secrets server-side using a cryptographically secure random generator and
transfer the plaintext secret once through USB, SSH on a trusted local network,
or an explicitly designed one-time enrollment flow.

## 8. TLS and CA lifecycle

- Gateways store the public CA certificate/trust bundle used to verify the
  Mosquitto server certificate.
- Mosquitto stores `server.crt`, `server.key`, and any intermediate chain.
- Keep the root CA private key offline or in a protected CA environment; never
  place it on a Gateway.
- Renewing the Mosquitto server certificate with the same valid CA does not
  require changing Gateways.
- Rotate an expiring CA through overlap: deploy `old CA + new CA`, switch the
  broker to a certificate signed by the new CA, verify migration, then remove
  the old CA.
- Do not disable hostname validation, certificate validation, or device-time
  checks to bypass TLS errors.

Cloudflare manages public edge TLS for HTTP. If `cloudflared`, Nginx, and the
target services share a trusted host or private Docker network, the local hop
may use HTTP. Use origin HTTPS when that hop crosses an untrusted network.

## 9. MQTT telemetry contract

Use `message_id` as the application-level identifier of one MQTT message. A
message may contain a complete batch of samples. Do not introduce `batch_id`
unless one logical batch is explicitly split across multiple MQTT messages.

Illustrative payload:

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

Rules:

- Generate `message_id` once using UUIDv4/UUIDv7 and a cryptographically secure
  source.
- Store the serialized message in a durable Gateway outbox before publishing.
- A retry reuses the same `message_id`, topic, payload, sequence range, and
  measurement timestamps.
- `first_sequence` and `sample_count` detect missing, overlapping, or
  out-of-order sample ranges.
- `boot_id` distinguishes counters across Gateway restarts.
- `measured_at` is measurement time, not server receive time.
- Delete an outbox item only after an application-level ACK confirms that the
  database transaction committed. An MQTT QoS ACK alone does not prove database
  persistence.

If one logical batch must be split, use separate `message_id` values plus a
shared `batch_id`, `part_index`, and `part_count`.

## 10. Digital Twin module

### 10.1 Scope and ownership

The Digital Twin is a logical module inside the Go modular monolith. It owns:

- NGSI-LD-compatible representations of Gateways, sensors, and controllable
  devices.
- Relationships such as `hasSensor`, `connectedTo`, `controls`, and
  `managedBy`.
- Current `reported_state` received from the physical system.
- Current `desired_state` requested by an authorized user.
- Temporal history of telemetry, AI inference values, and selected state
  changes.
- Command creation, MQTT publication, acknowledgement, result handling,
  timeout, retry policy, and desired-versus-reported reconciliation.

PostgreSQL is authoritative. Do not treat an exported NGSI-LD JSON document, a
JSON file on disk, a process-local Go map, WebSocket state, or retained MQTT
messages as the source of truth.

Implement the project-specific subset first. Do not claim complete NGSI-LD
conformance unless the implemented endpoints, JSON-LD processing, contexts,
content types, and error behavior have been tested against the relevant ETSI
requirements.

### 10.2 Entity identity and NGSI-LD mapping

Use stable URN identifiers:

```text
urn:ngsi-ld:Gateway:<gateway_id>
urn:ngsi-ld:Sensor:<sensor_id>
urn:ngsi-ld:Device:<device_id>
```

The database may use internal UUID primary keys, but the public `entity_id`
must remain stable. An illustrative API representation is:

```json
{
  "id": "urn:ngsi-ld:Gateway:gateway_001",
  "type": "Gateway",
  "name": {
    "type": "Property",
    "value": "AM5728 Gateway 001"
  },
  "connectionStatus": {
    "type": "Property",
    "value": "online",
    "observedAt": "2026-09-16T08:30:00Z"
  },
  "hasSensor": {
    "type": "Relationship",
    "object": "urn:ngsi-ld:Sensor:sensor_001"
  },
  "@context": [
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

Keep protocol fields and database columns in `snake_case`. Use NGSI-LD-style
property names at the JSON-LD boundary only through an explicit mapper; do not
scatter naming conversions throughout handlers and repositories. Pin or serve
the selected project context when custom terms are introduced. Do not fetch an
untrusted remote JSON-LD context during each API request.

### 10.3 Reported state and desired state

- `reported_state` describes what the Gateway last confirmed or measured.
- `desired_state` describes the persistent target configuration requested by an
  authorized user.
- Only trusted Gateway telemetry, status, or command-result messages may update
  `reported_state`.
- Only an authorized API operation may update `desired_state`.
- Updating `desired_state` creates a command only when an actionable difference
  from `reported_state` exists.
- A published command must not be treated as a successful state change.
- A command succeeds only after a correlated Gateway result and/or reported
  state confirms the intended effect.
- Keep `reported_version` and `desired_version` counters. Use optimistic
  concurrency for desired-state writes so two client requests do not silently
  overwrite each other.
- Store `last_reported_at`, `last_desired_at`, and the actor responsible for a
  desired-state change.

Use desired state for durable targets such as sampling interval, operating
mode, or enabled/disabled configuration. Use an explicit one-shot command for
actions such as reboot or capture-image, because those actions are not durable
target states.

### 10.4 Current and temporal storage

Minimum relational tables:

```text
twin_entities
    id UUID primary key
    entity_id TEXT unique not null
    entity_type TEXT not null
    name TEXT
    attributes JSONB not null default '{}'
    created_at TIMESTAMPTZ not null
    updated_at TIMESTAMPTZ not null

twin_relationships
    source_entity_id UUID not null
    relationship_type TEXT not null
    target_entity_id UUID not null
    created_at TIMESTAMPTZ not null
    primary key (source_entity_id, relationship_type, target_entity_id)

twin_states
    entity_id UUID primary key
    reported_state JSONB not null default '{}'
    desired_state JSONB not null default '{}'
    reported_version BIGINT not null default 0
    desired_version BIGINT not null default 0
    last_reported_at TIMESTAMPTZ
    last_desired_at TIMESTAMPTZ
    updated_at TIMESTAMPTZ not null

twin_commands
    id UUID primary key
    command_id UUID unique not null
    entity_id UUID not null
    action TEXT not null
    parameters JSONB not null default '{}'
    status TEXT not null
    desired_version BIGINT
    idempotency_key TEXT
    issued_by UUID
    issued_at TIMESTAMPTZ not null
    expires_at TIMESTAMPTZ not null
    acknowledged_at TIMESTAMPTZ
    completed_at TIMESTAMPTZ
    attempt_count INTEGER not null default 0
    error_code TEXT
    error_message TEXT

twin_outbox
    id UUID primary key
    command_id UUID not null
    topic TEXT not null
    payload JSONB not null
    status TEXT not null
    attempt_count INTEGER not null default 0
    next_attempt_at TIMESTAMPTZ not null
    created_at TIMESTAMPTZ not null
    published_at TIMESTAMPTZ
```

Use a TimescaleDB hypertable for temporal values:

```text
twin_temporal_values
    observed_at TIMESTAMPTZ not null
    entity_id UUID not null
    property_name TEXT not null
    value_number DOUBLE PRECISION
    value_text TEXT
    value_boolean BOOLEAN
    metadata JSONB not null default '{}'
    message_id UUID
```

Choose exactly one typed value column for each row. Preserve units and source
metadata. Add constraints and indexes through migrations. Do not duplicate the
same high-frequency sample in both `telemetry` and `twin_temporal_values`
without a documented reason; prefer one physical time-series table with clear
views or mappings where practical.

### 10.5 Uplink ingress behavior

For every MQTT telemetry or status message:

1. Derive the Gateway identity from the authenticated MQTT connection/topic.
2. Validate the topic, protocol version, payload size, schema, timestamps, and
   allowed property names.
3. Apply existing `(gateway_id, message_id)` idempotency rules.
4. Resolve the Gateway, sensor, or device entity.
5. In one database transaction, insert the deduplication record, append temporal
   values, and update current reported state/version.
6. Commit before sending the application-level ACK or realtime event.
7. Publish WebSocket updates only to users authorized for the related Gateway.

Reject unknown entities by default. If automatic sensor discovery is later
enabled, place discovered entities in a pending state until an authorized user
accepts them.

### 10.6 Downlink command behavior

All client-originated control and configuration operations follow this path:

1. Validate the Supabase JWT.
2. Check `user_gateways` and the user's role for the target entity's Gateway.
3. Validate the action and parameters against a server-owned command schema.
4. Start a database transaction.
5. Update desired state when the command represents a durable target.
6. Insert `twin_commands` with status `pending`.
7. Insert the corresponding `twin_outbox` row.
8. Commit the transaction.
9. Let a bounded outbox worker publish to MQTT.
10. Correlate Gateway acknowledgement/result by `command_id`.
11. Update command status and reported state in a transaction.
12. Notify authorized clients after commit.

Never update PostgreSQL and publish MQTT as two unrelated handler operations.
The transactional outbox exists to close that failure gap. MQTT delivery is
at-least-once, so the Gateway must remember recently processed `command_id`
values and return the previous result for a duplicate command instead of
executing it twice.

Illustrative command payload:

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

Illustrative result payload:

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

### 10.7 Command lifecycle and reconciliation

Use this minimum lifecycle:

```text
pending -> published -> acknowledged -> succeeded
                         |              -> failed
                         -> timeout
pending/published -> cancelled when cancellation is still safe
```

- `pending` means committed to the database and waiting for publication.
- `published` means sent to the broker; it does not prove Gateway receipt.
- `acknowledged` means the Gateway accepted the command for processing.
- `succeeded` means the Gateway confirmed the requested effect.
- `failed` contains a stable error code and a safe diagnostic message.
- `timeout` means no valid completion arrived before the deadline.

The reconciliation worker compares desired and reported versions/state. Retry
only operations declared idempotent, respect `expires_at`, use exponential
backoff with jitter, and cap attempts. Do not blindly retry one-shot actions
such as reboot or capture-image. A late result must be recorded and handled by
an explicit policy instead of silently changing a previously final status.

### 10.8 HTTP API boundary

Minimum project API:

```text
POST   /v1/digital-twins
GET    /v1/digital-twins
GET    /v1/digital-twins/{entity_id}
PATCH  /v1/digital-twins/{entity_id}
GET    /v1/digital-twins/{entity_id}/state
PATCH  /v1/digital-twins/{entity_id}/desired-state
GET    /v1/digital-twins/{entity_id}/history
POST   /v1/digital-twins/{entity_id}/commands
GET    /v1/digital-twins/{entity_id}/commands
GET    /v1/commands/{command_id}
```

URL-encode URN entity identifiers in path parameters or use an unambiguous
surrogate route identifier. Desired-state and command requests support an
idempotency key. A successful create response returns `202 Accepted`, the
`command_id`, and current command status; it must not report physical execution
success prematurely.

### 10.9 Go package boundaries

Prefer this logical structure, adapting names to existing repository
conventions rather than duplicating packages:

```text
internal/digitaltwin/
    domain/          entity, relationship, state, command
    service/         entity, state, command, reconciliation, temporal
    repository/      PostgreSQL and TimescaleDB access
    mqtt/            ingress, response handler, command publisher
    mapper/          telemetry and NGSI-LD mappings
    transport/http/  REST handlers and DTOs
    worker/          outbox, timeout, retry, offline detection
```

Domain and service packages must not depend directly on HTTP, MQTT client, or
database-driver types. Repositories and publishers are injected through small
interfaces. Keep transactions at the service/use-case boundary where one
operation spans state, command, and outbox writes.

## 11. Deduplication, ordering, queues, and backpressure

- Identify retries by `(gateway_id, message_id)`.
- Use a regular `processed_messages` table with primary key
  `(gateway_id, message_id)`.
- Store `payload_hash`. Same ID and hash means a retry; same ID with a different
  hash is a protocol/security error.
- Insert the deduplication marker and telemetry samples in the same transaction.
- Use an atomic conflict pattern such as `INSERT ... ON CONFLICT DO NOTHING`.
- Order data by measurement timestamp and sequence, never MQTT arrival order.
- Use a bounded Go queue, fixed worker pool, and bounded PostgreSQL connection
  pool.
- Apply exponential backoff with jitter and propagate sustained pressure to the
  Gateway's durable outbox.
- Never use an unbounded queue or unbounded goroutine creation.

For Linux Gateways, prefer a small SQLite outbox. On constrained
microcontrollers, use a bounded flash/NVS outbox designed to limit flash wear.

## 12. High-frequency telemetry and historical charts

For a 100 Hz sensor, group the 100 samples measured during one second into one
MQTT message. Batching reduces transport and insert overhead; it does not mean
discarding 99 samples.

Default MVP policy:

- Store all raw samples initially.
- Make raw-data retention configurable and choose a value only after measuring
  disk use; 7–30 days is a planning range, not a hard-coded requirement.
- Use TimescaleDB `time_bucket` for long-range chart queries.
- Return raw recent values only for short windows.
- Aim for roughly 500–2,000 points per chart response.
- Prefer an average line with a min/max band so short peaks remain visible.
- Add continuous aggregates only after query measurements show a need; they are
  not mandatory for the first MVP.

If threshold alerts are later restored, evaluate raw samples or features
computed from every raw sample, not only chart-downsampled data.

## 13. Media rules

- MQTT is binary-capable, but the MVP uses MQTT only for telemetry/control
  metadata, not media bytes.
- Support stored images first.
- Go authenticates the Gateway and validates intended object path, content type,
  expected size, and quota before issuing a signed URL.
- The signed URL must be short-lived and limited to the intended operation and
  object path.
- The Gateway uploads directly to private Storage over HTTPS through Nginx and
  the Supabase API Gateway.
- Store `gateway_id`, object path, media type, expected/actual size, checksum,
  timestamps, and validation status in `media_objects`.
- Never expose a public bucket merely to simplify the Flutter demo.
- Large video, resumable/TUS upload, and live streaming remain out of scope.

## 14. Target Gateway platforms

### ESP32

- Use ESP-IDF, ESP-MQTT, ESP-TLS/MbedTLS, and SNTP.
- Store credentials in encrypted NVS when supported.
- Use a bounded flash outbox and account for flash wear.
- Target telemetry and small control messages, not video workflows.

### Luckfox Pico Plus

- Treat it as a constrained Linux Gateway.
- Use a lightweight MQTT/TLS client, time synchronization, and a SQLite/file
  outbox if available in the built image.
- Cross-compile or add dependencies to Buildroot rather than assuming desktop
  packages exist.
- Upload captured images through HTTPS signed URLs.

### TI AM5728 with TI SDK/Arago Linux

- Cross-compile or build dependencies into the TI SDK/Arago image.
- Run the Gateway program as a supervised background service.
- Configure time synchronization, filesystem permissions, log rotation, and a
  SQLite/file durable outbox.
- Benchmark CPU, memory, disk, and network use on the real board.

## 15. Development and deployment rules

- Inspect existing code, Compose files, migrations, documentation, and current
  Git status before making changes.
- Preserve user edits and unrelated dirty-worktree changes.
- Keep secrets out of Git. Commit `.env.example`, never a populated `.env`.
- Pin image and dependency versions; do not use floating `latest` tags for the
  final deployment.
- Use Dockerfiles and Docker Compose with health checks, named volumes, restart
  policies, explicit networks, and persistent data paths.
- Do not expose PostgreSQL, Supabase Studio, Auth, Storage, internal API Gateway
  ports, or Go debug endpoints publicly.
- Protect Studio using Cloudflare Access, VPN, or a trusted management network.
- Use migrations instead of manual production schema edits.
- Add structured logs without passwords, JWTs, private keys, or signed URLs.
- Provide backup and verified restore procedures for PostgreSQL and relevant
  Storage data.
- Basic CI should run Go formatting/linting/tests, migration checks, and
  container builds.
- Keep deployment reversible with versioned images and configuration backups.

## 16. Minimum verification

At minimum, test:

- Fake Gateway -> Mosquitto -> Go -> TimescaleDB end to end.
- One 100 Hz sensor sending one 100-sample message per second.
- Invalid payloads, duplicate delivery, reconnect, retry, lost application ACK,
  and out-of-order messages.
- Queue saturation and temporary database slowdown without unbounded memory use.
- Gateway offline outbox and resend behavior.
- Per-Gateway MQTT ACL isolation and credential revocation.
- Invalid/expired TLS certificates, hostname mismatch, and incorrect time.
- Supabase login/refresh and Go JWT validation.
- User A cannot access User B's Gateway history, WebSocket stream, or media.
- WebSocket authentication, reconnect, disconnect cleanup, and slow-client
  behavior.
- Signed upload/read URL expiry, object-path restriction, size/type rejection,
  and private-bucket denial.
- Telemetry updates the correct Digital Twin reported state and temporal rows in
  the same successful processing path.
- A client cannot write `reported_state` and a Gateway cannot write
  `desired_state`.
- User A cannot read or control User B's Digital Twin.
- A desired-state update creates one command and one outbox item in the same
  transaction.
- Database commit succeeds while MQTT is unavailable; the outbox later
  publishes without losing the command.
- Duplicate command publication does not execute the physical action twice.
- Command acknowledgement, success, failure, expiry, timeout, retry, and late
  result behavior.
- Desired-versus-reported reconciliation after Gateway reconnect.
- Optimistic concurrency rejects a stale desired-state version.
- NGSI-LD entity serialization preserves identifiers, property types,
  relationships, observed timestamps, and the selected context.
- Service restart, persistent data, clean-environment Compose deployment,
  backup, and restore.

Run focused tests while implementing each module. The final integration period
is for acceptance and regression testing, not the first time components are
tested together.

## 17. Schedule and fallback rules

For planning, September and October may use short task durations because the
student has more available time. November and December need integration buffer,
especially around Supabase, WebSocket, Storage, Flutter, and final deployment.

Preferred Digital Twin implementation order:

1. Add entity, relationship, current-state, command, outbox, and temporal
   migrations.
2. Map one Gateway and one sensor into stable NGSI-LD-compatible entities.
3. Complete the uplink vertical slice from telemetry to reported state and
   temporal history.
4. Complete one downlink configuration slice from desired-state API to MQTT
   result and reported-state convergence.
5. Add command query, timeout/retry policy, WebSocket events, and authorization
   tests.
6. Expand supported properties and commands only after the first vertical slice
   passes end-to-end tests on the real AM5728 Gateway.

If schedule slips, reduce scope in this order:

1. Support images only; postpone video and resumable upload.
2. Target one Flutter platform, preferably Android.
3. Provide one historical chart and one realtime chart.
4. Use Supabase Studio for administration instead of building an admin UI.
5. Limit Digital Twin entity types to `Gateway` and `Sensor`, relationships to
   `hasSensor`, and commands to one safe configuration operation.
6. Export NGSI-LD-compatible entity JSON from project APIs without deploying a
   full external Context Broker.
7. Temporarily use short polling if WebSocket cannot be stabilized, while
   preserving the historical REST API.

Do not cut MQTT TLS, per-Gateway credentials/ACLs, human-user Auth,
User–Gateway authorization, telemetry persistence, deduplication, bounded
queues, durable Gateway retry behavior, historical queries, or private media
access. Do not cut the Digital Twin core path of reported state, desired state,
authorized command creation, transactional outbox, MQTT result correlation,
and command status query once device control is presented as an MVP feature.

## 18. Instructions for coding agents

- Implement the smallest complete vertical slice and verify it before adding
  another subsystem.
- Follow the selected REST/WebSocket architecture; do not silently restore
  gRPC-Web.
- Keep Go as a modular monolith unless the user explicitly requests otherwise.
- Do not silently change field names or message semantics. Use `message_id` as
  specified above.
- Distinguish MQTT broker ACK from application-level database-commit ACK.
- Do not claim exactly-once delivery. Implement at-least-once delivery with
  application-level idempotency.
- Do not authorize from request parameters or JWT validity alone; verify the
  resource relationship in PostgreSQL.
- Route Supabase Auth and Storage through the Supabase API Gateway. Nginx is the
  external reverse proxy, not a substitute for that internal gateway.
- Do not give a Gateway a Supabase human account or `service_role` key.
- Prefer static Mosquitto `password_file` and `acl_file` for the first MVP; add
  dynamic administration only if it is required and scheduled.
- Treat retention periods, queue sizes, worker counts, packet limits, upload
  limits, token lifetimes, and timeouts as configurable values.
- Keep all application-level device commands inside the Digital Twin module.
  The client calls Go APIs and never publishes MQTT commands directly.
- Keep PostgreSQL as the authoritative Digital Twin store. Do not create JSON
  configuration files or global Go maps as durable state.
- Keep `reported_state` and `desired_state` separate. Never mark desired state
  as reported until a correlated Gateway message confirms it.
- Persist desired-state changes, commands, and outbox records atomically.
- Use `command_id` for end-to-end correlation and idempotency. Do not claim
  exactly-once command execution.
- Implement one vertical slice first:
  `PATCH desired state -> transaction -> outbox -> MQTT -> Gateway result ->
  reported state -> command query/realtime event`.
- Keep NGSI-LD mapping in one package. Do not leak JSON-LD library types into
  domain services or database repositories.
- Do not add an external Context Broker unless explicitly requested. If added
  later, define which component is authoritative before writing code; never
  allow Go and the broker to become two unsynchronized sources of truth.
- Benchmark before choosing production defaults.
- When the repository does not yet contain a convention, present the simplest
  viable option and its trade-off rather than inventing a permanent decision.
- Explain important implementation choices in plain Vietnamese when handing
  work back to the user.
