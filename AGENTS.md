# AGENTS.md

## 1. Project identity

This repository contains the main implementation of a university IoT
Gateway–Server project (`Đồ án 1`). The system receives sensor data from
heterogeneous Gateways, stores time-series measurements, provides historical
queries and near-real-time streaming to users, authenticates users, and allows
authorized Gateways to upload stored media.

The project is implemented by one student. This is the student's first large
university project, and the student has limited experience with Go, Mosquitto,
and MQTT. Prefer the smallest complete and explainable solution over
microservices, premature optimization, or infrastructure that is not required
by the MVP.

**FL Module Decision (as of 2026-08-31):** Federated Learning will be
implemented using a **Go unified codebase** — both server-side aggregation
and client-side training are written in Go. The FastAPI/Python aggregator from
the initial plan is **removed**. Gateway training uses `gonum` for numerical
operations with manual backprop for a small autoencoder. This decision was
made after evaluating Gorgonia (unmaintained ~3 years) vs gonum (actively
maintained).

Schedule:

- Project start: 2026-08-24.
- Target system completion: 2026-12-31.
- The student has more implementation time during September and October, so
  tasks in those months may be scheduled more aggressively.
- Build and verify a local proof of concept before expanding to the main MVP.
- **FL spike on AM5728 must happen in September** to validate training on
  real hardware.

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
10. A simple Flutter client demonstrates Auth, Gateway lists, historical charts,
    realtime charts, and stored-image access.

**FL MVP scope (added):**

1. Server can create FL rounds, invite gateways, collect weight updates.
2. Gateway (Go binary) trains a small autoencoder locally and uploads weight
   updates.
3. Server aggregates updates (FedAvg) and stores global models.
4. Multiple gateways (at least 2) participate in a round.
5. Gateway giả lập (Docker) and AM5728 board share the same Go training code.

The following features are deferred unless the user explicitly restores them:

- gRPC and gRPC-Web.
- A command-and-control subsystem.
- A full alert engine.
- Live-video streaming or a dedicated media server.
- Large-video upload, resumable upload, and transcoding.
- A custom administration application.
- Advanced observability, orchestration, or multi-node high availability.
- Advanced FL algorithms (trimmed mean, norm clipping) — start with FedAvg
  only.
- Automatic FL scheduler — use admin-triggered rounds first.

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
- **Go unified for FL**: server aggregation and client training both in Go.
- **gonum** for training numerical operations (manual backprop).
- **Flat float32 tensor serialization** for weight exchange between server
  and gateways.

Do not replace these choices without an explicit user request. In particular:

- Do not propose InfluxDB as the primary database. TimescaleDB was selected
  because the project needs time-series operations and relational joins for
  permissions in the same PostgreSQL ecosystem.
- Do not propose Keycloak as the default human-user identity system. Supabase
  Auth was selected, and the user already knows its Flutter SDK.
- Do not expose PostgREST as the public business API. The Go backend owns all
  application APIs. PostgREST may remain internal if the self-hosted Supabase
  stack requires it.
- Do not reintroduce gRPC merely for performance. REST and WebSocket were
  chosen to reduce Protobuf, code-generation, gRPC-Web, and proxy complexity.
- Do not send large images or video through MQTT (execpt weight file from Federated Learning function).
- **Do not reintroduce Python/FastAPI for FL** — the Go unified decision is
  final.

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
metadata, and signed-read-URL requests. WebSocket is used only for new
realtime telemetry events.

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
- The Supabase API Gateway is an internal component of the self-hosted stack.
  It routes `/auth/v1/*`, `/storage/v1/*`, and other required Supabase paths
  and applies Supabase-specific CORS, API-key, and header behavior.
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

### 4.5 FL control and data planes (ADDED)

```text
CONTROL-PLANE (MQTT, lightweight):
Server --(round cmd)--> Gateway (gateways/%u/fl/cmd)
Gateway --(status/ACK)--> Server (gateways/%u/fl/status)

DATA-PLANE (HTTPS -> Supabase Storage):
Server --(signed READ URL)--> Gateway (download global model)
Gateway --(signed WRITE URL)--> Supabase Storage (upload local update)
```

- MQTT carries only control messages (round_id, model_id, deadline_at).
- Weight updates are uploaded as files via HTTPS signed URLs.
- This matches the media upload pattern already in place.

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
- **FL control topics**: add `gateways/%u/fl/cmd` (Gateway subscribe,
  Server publish) and `gateways/%u/fl/status` (Gateway publish,
  Server subscribe).

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
- **FL module**: round lifecycle, aggregation (FedAvg), signed URL management
  for weights, validation (NaN, checksum), metrics.
- **FL training kernel**: dense autoencoder training with gonum (forward +
  backward manual backprop).
- **FL serialization**: flat float32 tensor + model_config.json manifest.

MQTT callbacks must do only cheap parsing/copying and enqueue bounded work. Do
not create an unbounded goroutine per MQTT message.

### 5.4 PostgreSQL and TimescaleDB

- PostgreSQL stores application profiles, Gateway ownership/permissions,
  sensors, media metadata, Gateway credential metadata, and deduplication
  records.
- TimescaleDB hypertables store raw time-series samples.
- SQL joins determine whether a user may access a Gateway and therefore its
  sensors, telemetry, realtime stream, and media.
- Use migrations for schemas, constraints, indexes, hypertables, and policies.
- Pin and test compatible PostgreSQL, TimescaleDB, and self-hosted Supabase
  versions. Do not assume an arbitrary Supabase PostgreSQL image already
  contains the required TimescaleDB extension.
- Prefer one documented PostgreSQL topology. Do not silently introduce a second
  application database merely to avoid configuration work.

**FL tables (add via migration):**

```sql
fl_models(
  model_id          UUID PRIMARY KEY,
  name              TEXT,
  architecture_json JSONB,
  input_window      INT,
  param_count       BIGINT,
  created_at        TIMESTAMPTZ DEFAULT now()
);

fl_global_models(
  model_id     UUID REFERENCES fl_models,
  round_number INT,
  storage_path TEXT,
  checksum     TEXT,
  metrics_json JSONB,
  created_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (model_id, round_number)
);

fl_rounds(
  round_id             UUID PRIMARY KEY,
  model_id             UUID REFERENCES fl_models,
  round_number         INT,
  state                TEXT,
  target_client_count  INT,
  min_updates_required INT,
  deadline_at          TIMESTAMPTZ,
  created_at           TIMESTAMPTZ DEFAULT now()
);

fl_round_participants(
  round_id      UUID REFERENCES fl_rounds,
  gateway_id    TEXT,
  state         TEXT,
  invited_at    TIMESTAMPTZ,
  downloaded_at TIMESTAMPTZ,
  submitted_at  TIMESTAMPTZ,
  PRIMARY KEY (round_id, gateway_id)
);

fl_client_updates(
  update_id         UUID PRIMARY KEY,
  round_id          UUID REFERENCES fl_rounds,
  gateway_id        TEXT,
  message_id        UUID,
  storage_path      TEXT,
  num_samples       INT,
  checksum          TEXT,
  size_bytes        BIGINT,
  train_loss        DOUBLE PRECISION,
  validation_status TEXT,
  received_at       TIMESTAMPTZ DEFAULT now(),
  UNIQUE (round_id, gateway_id),
  UNIQUE (gateway_id, message_id)
);
```

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
- **FL weight storage**: weights are stored as binary float32 arrays in
  Supabase Storage, using the same signed URL mechanism as media uploads.

## 6. Identity and authorization model

Human-user and Gateway identities are separate.

### 6.1 Human users

- Supabase Auth owns human accounts and sessions.
- `profiles.id` references `auth.users.id`.
- `user_gateways` records which users may access which Gateways and their
  role.
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
not reuse a human Supabase account, a Supabase `service_role` key, or assume
the MQTT password is automatically an HTTP bearer token.

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
- `fl_models`, `fl_global_models`, `fl_rounds`,
  `fl_round_participants`, `fl_client_updates`
- Gateway HTTP credential/token metadata where required

Typical authorization chain:

```text
auth.users / profiles
    -> user_gateways
    -> gateways
    -> sensors / telemetry / realtime / media_objects
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
gateways/<gateway_id>/fl/cmd        Gateway: subscribe, Server: publish
gateways/<gateway_id>/fl/status     Gateway: publish, Server: subscribe
```

Prefer `%u` ACL patterns so the authenticated MQTT username can access only its
own namespace. A leaked credential must be revocable for one Gateway without
rotating every Gateway.

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

- Generate `message_id` once using UUIDv4/UUIDv7 and a cryptographically
  secure source.
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

## 10. Deduplication, ordering, queues, and backpressure

- Identify retries by `(gateway_id, message_id)`.
- Use a regular `processed_messages` table with primary key
  `(gateway_id, message_id)`.
- Store `payload_hash`. Same ID and hash means a retry; same ID with a
  different hash is a protocol/security error.
- Insert the deduplication marker and telemetry samples in the same
  transaction.
- Use an atomic conflict pattern such as `INSERT ... ON CONFLICT DO NOTHING`.
- Order data by measurement timestamp and sequence, never MQTT arrival order.
- Use a bounded Go queue, fixed worker pool, and bounded PostgreSQL connection
  pool.
- Apply exponential backoff with jitter and propagate sustained pressure to
  the Gateway's durable outbox.
- Never use an unbounded queue or unbounded goroutine creation.

For Linux Gateways, prefer a small SQLite outbox. On constrained
microcontrollers, use a bounded flash/NVS outbox designed to limit flash wear.

## 11. High-frequency telemetry and historical charts

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

## 12. Media rules

- MQTT is binary-capable, but the MVP uses MQTT only for telemetry/control
  metadata, not media bytes.
- Support stored images first.
- Go authenticates the Gateway and validates intended object path, content
  type, expected size, and quota before issuing a signed URL.
- The signed URL must be short-lived and limited to the intended operation and
  object path.
- The Gateway uploads directly to private Storage over HTTPS through Nginx and
  the Supabase API Gateway.
- Store `gateway_id`, object path, media type, expected/actual size, checksum,
  timestamps, and validation status in `media_objects`.
- Never expose a public bucket merely to simplify the Flutter demo.
- Large video, resumable/TUS upload, and live streaming remain out of scope.

## 13. Target Gateway platforms

### ESP32

- Use ESP-IDF, ESP-MQTT, ESP-TLS/MbedTLS, and SNTP.
- Store credentials in encrypted NVS when supported.
- Use a bounded flash outbox and account for flash wear.
- Target telemetry and small control messages, not video workflows.
- **FL on ESP32**: Not in scope for MVP. Use Go gateways only.

### Luckfox Pico Plus

- Treat it as a constrained Linux Gateway.
- Use a lightweight MQTT/TLS client, time synchronization, and a SQLite/file
  outbox if available in the built image.
- Cross-compile or add dependencies to Buildroot rather than assuming desktop
  packages exist.
- Upload captured images through HTTPS signed URLs.
- **FL on Luckfox**: Not in scope for MVP. Use Go gateways only.

### TI AM5728 with TI SDK/Arago Linux

- Cross-compile or build dependencies into the TI SDK/Arago image.
- Run the Gateway program as a supervised background service.
- Configure time synchronization, filesystem permissions, log rotation, and a
  SQLite/file durable outbox.
- Benchmark CPU, memory, disk, and network use on the real board.
- **FL on AM5728**: Primary target. Go binary cross-compiled with
  `GOOS=linux GOARCH=arm GOARM=7`. Training kernel uses gonum.

## 14. FL specific design decisions

### 14.1 Model architecture

- Autoencoder/forecaster with dense layers, 7–30k parameters.
- `input_window` = `sample_count` from telemetry batch.
- For MVP, use fixed `input_window` per model (e.g., 100). Multiple models
  can exist for different frequencies.
- Output shape = 2 × `input_window` (prediction + reconstruction).

### 14.2 Training on Gateway

- Use **gonum** (`gonum.org/v1/gonum/mat`) for matrix operations.
- Manual backprop for dense layers (ReLU activation, MSE loss).
- Optimizer: SGD with momentum or simple Adam (implement manually, ~50
  lines).
- Train on each telemetry batch as it arrives or collect a small buffer.
- Loss value reported with weight update.

### 14.3 Weight serialization

- Flat `[]float32` array, little-endian binary via `encoding/binary`.
- `model_config.json` describes architecture, param_count, layer sizes,
  weight order.
- All gateways (giả lập and AM5728) use the same serialization code.

### 14.4 Aggregation (Server)

- FedAvg: weighted average by `num_samples`.
- Bounded memory: accumulate one buffer, O(param_count), not
  O(K × param_count).
- Validate: NaN/Inf rejection, checksum, param_count match.
- Store global model in Supabase Storage as flat float32 array.

### 14.5 Round lifecycle

```text
created -> open -> collecting -> aggregating -> completed
                            -> aborted (deadline, insufficient updates)
```

- Go controls state transitions.
- Admin triggers round creation (no auto-scheduler for MVP).
- Go calls aggregation function directly (no separate FastAPI service).

### 14.6 API endpoints (Gateway-facing)

```text
GET  /api/v1/fl/rounds/current?model_id=...
     -> round metadata + signed READ URL for global weights

POST /api/v1/fl/rounds/{round_id}/updates
     -> signed WRITE URL for weight upload

POST /api/v1/fl/rounds/{round_id}/updates/{update_id}/complete
     -> confirm upload complete
```

### 14.7 API endpoints (User-facing)

```text
GET /api/v1/fl/models
GET /api/v1/fl/rounds?model_id=...
GET /api/v1/fl/rounds/{round_id}
```

### 14.8 FL security

- Gateway JWT for authentication.
- `num_samples` cross-checked against TimescaleDB telemetry counts (unique
  advantage of this system).
- Norm clipping as defense against poisoning (start with FedAvg only).
- Checksum validation, NaN/Inf rejection before accepting updates.

## 15. Development and deployment rules

- Inspect existing code, Compose files, migrations, documentation, and current
  Git status before making changes.
- Preserve user edits and unrelated dirty-worktree changes.
- Keep secrets out of Git. Commit `.env.example`, never a populated `.env`.
- Pin image and dependency versions; do not use floating `latest` tags for the
  final deployment.
- Use Dockerfiles and Docker Compose with health checks, named volumes, restart
  policies, explicit networks, and persistent data paths.
- Do not expose PostgreSQL, Supabase Studio, Auth, Storage, internal API
  Gateway ports, or Go debug endpoints publicly.
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
- Queue saturation and temporary database slowdown without unbounded memory
  use.
- Gateway offline outbox and resend behavior.
- Per-Gateway MQTT ACL isolation and credential revocation.
- Invalid/expired TLS certificates, hostname mismatch, and incorrect time.
- Supabase login/refresh and Go JWT validation.
- User A cannot access User B's Gateway history, WebSocket stream, or media.
- WebSocket authentication, reconnect, disconnect cleanup, and slow-client
  behavior.
- Signed upload/read URL expiry, object-path restriction, size/type rejection,
  and private-bucket denial.
- Service restart, persistent data, clean-environment Compose deployment,
  backup, and restore.
- **FL-specific**:
  - Gateway training loop produces expected weight updates.
  - Weight serialization roundtrip (serialize -> deserialize -> same values).
  - Server aggregation produces correct FedAvg result.
  - Cross-compiled ARM7 binary runs on AM5728.
  - Multiple gateways (giả lập + AM5728) can participate in one round.
  - NaN/Inf rejection works.

Run focused tests while implementing each module. The final integration period
is for acceptance and regression testing, not the first time components are
tested together.

## 17. Schedule and fallback rules

For planning, September and October may use short task durations because the
student has more available time. November and December need integration buffer,
especially around Supabase, WebSocket, Storage, Flutter, and final deployment.

**FL-specific milestones:**

| Mốc | Việc | Rủi ro |
|---|---|---|
| 01–15/09 | FL training kernel spike on AM5728 (gonum + cross-compile) | Cao nhất |
| 16–30/09 | FL server: migrations, round lifecycle, API, signed URLs | Trung bình |
| 01–15/10 | FL gateway: training integration, weight upload | Trung bình |
| 16–31/10 | Multi-gateway test (giả lập + real board) | Trung bình |
| 11 | Integration with main system, metrics, charts | Thấp |
| 12 | Final freeze, report | Thấp |

If schedule slips, reduce scope in this order:

1. Support images only; postpone video and resumable upload.
2. Target one Flutter platform, preferably Android.
3. Provide one historical chart and one realtime chart.
4. Use Supabase Studio for administration instead of building an admin UI.
5. Temporarily use short polling if WebSocket cannot be stabilized, while
   preserving the historical REST API.
6. **FL fallback**: All clients are giả lập (Docker); AM5728 only demo
   inference, not training. Bỏ trimmed mean / robust aggregation. Bỏ MQTT
   control plane — gateway polls `GET /fl/rounds/current`.

Do not cut MQTT TLS, per-Gateway credentials/ACLs, human-user Auth,
User–Gateway authorization, telemetry persistence, deduplication, bounded
queues, durable Gateway retry behavior, historical queries, or private media
access.

**FL cannot cut:** Private Storage for weights, Gateway JWT auth,
dedup/idempotency, bounded-memory aggregation, validation NaN/finite.

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
- Route Supabase Auth and Storage through the Supabase API Gateway. Nginx is
  the external reverse proxy, not a substitute for that internal gateway.
- Do not give a Gateway a Supabase human account or `service_role` key.
- Prefer static Mosquitto `password_file` and `acl_file` for the first MVP;
  add dynamic administration only if it is required and scheduled.
- Treat retention periods, queue sizes, worker counts, packet limits, upload
  limits, token lifetimes, and timeouts as configurable values.
- Benchmark before choosing production defaults.
- When the repository does not yet contain a convention, present the simplest
  viable option and its trade-off rather than inventing a permanent decision.
- Explain important implementation choices in plain Vietnamese when handing
  work back to the user.

**FL-specific coding rules:**

- Use `gonum` for matrix ops in training; no Gorgonia.
- Weight serialization: `encoding/binary` little-endian.
- Aggregation: bounded memory, O(param_count), not O(K × param_count).
- Cross-compile flags: `GOOS=linux GOARCH=arm GOARM=7`.
- Test training on laptop before ARM7 spike.
- Log losses and weight norms for debugging.
- Do not store training data on server; only weight updates.
- Use `num_samples` from gateway, but cross-check with TimescaleDB when
  possible.