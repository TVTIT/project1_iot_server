# Task 5.0 — Baseline, control contract and integration boundaries

Date: 2026-10-23. **Task 5.0 only; Stage 5 downlink is NOT implemented.**
Source plan: [Stage 5 detailed plan](../../../docs/backend_plan/stage5_backend_detailed_plan.md),
read in full; sections 3–7 are the authority for the contract below. This record
does not mark Stage 5 complete or authorize automatic execution of later tasks.

## 1. Baseline and evidence boundaries

- `main_src`, branch `develop`, HEAD `9f08278a2bc69d06ca0fa754dc81bf2d951809c5`;
  clean worktree verified before editing. No applicable `AGENTS.md` found in
  this checkout or its ancestors (sibling projects' instructions do not apply).
- Reviewed [Stage 3 handover](stage-3-uplink.md), [Stage 4 handover](stage-4-history-and-twin-api.md)
  including its subsequent NGSI-LD migration, and [membership handover](admin-users-and-gateway-membership.md).
  Their historical run/revision claims are not reassigned to this task.
- Full locally reproducible CI matrix passed on the unchanged baseline **before**
  these edits. Native fixtures used isolated resources; no shared live DB,
  broker store, credentials or deployment `.env` was changed.
- Local execution evidence: `/tmp/opencode/task50-local-ci-9f08278-corrected/`:
  `commands.log`, `results.tsv`, named lane logs, coverage and safe native reports.
  Original failed umask attempt is retained separately at
  `/tmp/opencode/task50-local-ci-9f08278/REPORT.md` as **INVALID ENVIRONMENT**.
  Its failure is not a product defect or valid RED. A separate quoted-pattern
  invocation mistake is also preserved and invalidated, not counted as a test run.
- Evidence categories: source survey establishes integration points; unit spike
  establishes arithmetic feasibility; native regression establishes the existing
  telemetry writer preserves seeded configuration. None establishes downlink.

## 2. Source-grounded integration map

| Area | Baseline source / actual capability | Stage 5 integration boundary (not implemented here) |
| --- | --- | --- |
| Identity / access | `internal/digitaltwin/identity.go`, `postgres_resolver.go`; `internal/usermanagement/postgres_repository.go`: membership is `public.user_gateways`, not JWT-supplied roles | Resolve provisioned Sensor URN and membership again inside mutation transaction; no platform-admin bypass |
| Graph | `internal/gateway/postgres_sensor_provisioning.go`: relational Sensor, Sensor/Gateway Twins, `hasSensor`, state rows; exact replay preserves state | Do not auto-provision or repair an incomplete graph during command admission |
| State | migrations 2, 6, 8: `twin_states` JSONB desired/reported, independent BIGINT versions, `last_desired_by` FK to profiles, desired/reported timestamps | Desired intent and confirmed `reported_state.configuration` have separate writers |
| Commands / outbox | [migration 2](../../migrations/000002_digital_twin_tables.up.sql): UUID `id` plus unique `command_id`, `entity_id`, action/parameters/status/version, actor/key/times/attempt/error; outbox FK command_id, topic/payload/status/next_attempt_at | Existing idempotency index is **not unique**; no no-op receipt, one-active index or stable payload bytes guarantee yet; legacy/sentinel rows must not auto-dispatch |
| Grants | migration 10 grants `iot_backend` SELECT/INSERT/UPDATE on states/commands/outbox and resource tables; app login inherits backend group/BYPASSRLS. Migrations 21/22 add account-management grants and membership DELETE | BYPASSRLS does not authorize humans. New tables/columns require explicit grants and human direct-write denial checks; no runtime DDL |
| Terminal | migration 22 `gateway_membership_revocation`; `scripts/sql/verify-migration-000021.sql`, `verify-migration-000022.sql`, CI migration/native guards | Recheck terminal immediately before later schema work; use next free version (23 if unchanged), update guards/fixtures, never edit applied migrations |
| Uplink writer | `internal/telemetry/postgres_repository.go`, `postgres_resources.go`, `postgres_reported_state.go`, `postgres_temporal.go` | Keep dedup/commit ACK and observation ordering. Reported writer uses `reported_state || $2::jsonb` with allowlisted measurement keys, not whole-object replacement |
| State view | `postgres_twin_reader.go:scanStateRow`, `read_model.go`, HTTP `twin_dto.go:NewTwinStateResponseDTO` retain `json.RawMessage` | Read configuration via `GET /v1/digital-twins/{urn}?format=simplified`; normalized detail and `/temporal` remain measurement observations |
| MQTT ACL | `scripts/mqtt-bootstrap.py:compose_store`: backend telemetry receive/subscribeLiteral/unsubscribeLiteral and ACK publish **only**. `internal/mqttcredential/dynsec_adapter.go:adapterGatewayACLs` already gives own commands receive/subscribe and responses publish | Backend data role must gain commands publish and literal responses subscription/read/unsubscribe. Manager `$CONTROL` principal remains separate; static ACL is not DynSec runtime evidence |
| Transport | `internal/mqttingress`, `cmd/server/ingress.go`, `internal/gatewaysim/transport.go`, `paho.go` | Reuse TLS/lifecycle/bounded callbacks and finite timeouts, add dedicated bounded command/result transport, reconnect/SUBACK and readiness checks in later tasks |
| SQLite / scheduling | `internal/gatewaysim/outbox.go`, `scheduler.go`, `config.go`, `cmd/gateway/main.go` | Schema 1 and static integer-Hz profile currently; dynamic sample controller must be introduced at the boundary in section 5 below |

Production-source search of Go and migrations found no existing owner of
`reported_state.configuration`. Existing `update_configuration` rows in metadata
tests are preservation sentinels, not runtime services. Empty legacy command
package directories and schema definitions do not prove a running downlink.

## 3. Locked contract (plan sections 3–7)

The proposed technical bounds in the plan are adopted here: numeric token/digits
bounded by 128 bytes, absolute exponent at most 128; Idempotency-Key 1–128 ASCII
bytes; list default/max 20/100; default TTL 60s. These are not new minimum-Hz,
authorization, target or action policies. Future configuration must be bounded
and validated as specified in section 8 of the plan, not silently unbounded.

| Concern | Contract for subsequent implementation |
| --- | --- |
| Target / policy | Any provisioned Sensor under an authorized Gateway; one Sensor per command; only `update_configuration` for sampling interval; `0 < frequency_hz <= 100`, no preset list or integer-Hz restriction |
| Decimal | JSON number only, `json.Number` + exact `big.Rat`; reject non-JSON numeric syntax, string/null/bool, NaN/Infinity, oversized token/digits/exponent before big-number parsing. No float64 boundary check or fingerprint |
| Conversion | `T_us = round_half_up(1000000 / Hz)`, integer `[10000, 2^53]`; reject unrepresentable positive Hz with HTTP 400 `frequency_not_representable` before writes, never clamp/underflow. Effective Hz is derived display `1000000/T_us`, not authoritative for no-op or round-trip |
| Arithmetic ceiling | **Keep existing `2^53 = 9007199254740992` µs** storage ceiling. `floor(MaxInt64/1000) = 9223372036854775` µs is larger; the existing duration comment/validator/native expectation are correct. Continue timestamp/calendar/span safety checks; a one-sample batch has zero span |
| Batch / low Hz | Sample only when due; about-one-second batches at higher rates may have varying sample counts. Below 1 Hz send one real sample per batch; no empty/fabricated/future samples. Flush accumulated old samples at reconfiguration, not after waiting another old interval |
| Activation / observation | Independent command wake applies timer/readback before succeeded; do not wait for the first new sample, even at a 100-second interval. No retroactive timestamp/sequence/boot reset; enqueued/retry bytes and IDs remain immutable |
| State ownership | PATCH merges only `desired_state.sample_interval_us`, increments desired_version and records actor/time; never forges reported state. Measurement root keys/last_reported_at belong to telemetry. Valid success merges only `configuration: {sample_interval_us, desired_version: string, applied_at: UTC}` and increments reported_version once while preserving desired/measurement fields |
| Historical measurements | Delayed old-profile telemetry may advance measurement state or only append raw/temporal according to existing ordering. Neither path overwrites confirmed configuration. Results never append a synthetic interval observation to temporal history |
| Mutation permission | Owner **and operator** of Gateway; viewer 403. Missing/invalid JWT 401; nonmember/cross-scope 404, including platform admin without membership. In-scope Gateway target is 400 `unsupported_command_target`. Metadata/admin policies otherwise unchanged |
| Read permission | Owner/operator/viewer membership for command list/detail/state; command must belong to resolved Sensor/Gateway, otherwise 404 |
| Revocation | Recheck DB role in transaction, including replay. Revocation after commit does not cancel accepted intent; cancellation/reconciliation remains Stage 6 |
| Version | `expected_desired_version` required nonnegative decimal BIGINT **string**; invalid/missing/overflow 400, version increment overflow fails, never wraps. Independent from reported_version; preserve exact wire values |
| Fingerprint / receipts | Unique `(actor_id, entity_id, idempotency_key)` receipt for both 202 and no-op 200; fingerprint action + canonical exact Hz rational + expected version. `0.1`, `0.10`, `1e-1` equivalent; different rationals remain different even if rounded interval matches |
| Replay order | Resolve current permission → receipt lookup/replay/conflict → expected version → expiry/busy → no-op or new intent. Replay returns original committed code/body before stale-version/busy checks, not recalculated current command status |
| Atomic intent | One transaction for desired/version/actor/time, UUID command, stable serialized outbox bytes, receipt. Commit before HTTP; ambiguous commit is not proof of rollback, retry same key. SQL unique constraints/locks protect concurrent requests |
| No-op | Only when desired interval AND confirmed configuration interval equal request, AND no active command. Return 200 `unchanged`, command_id null, store receipt, no desired_version increment/command/outbox. Equal desired without confirmation (failed/timeout) is an explicit new user execution/version, not automatic convergence |
| Active slot | At most one active command per Sensor: pending/published and legacy reserved acknowledged. Use partial unique index plus transaction; other Sensors independent, no lock held for TTL. Active conflict 409 `command_in_progress`; stale version 409 `desired_version_conflict`; changed key content 409 `idempotency_conflict` |
| Lifecycle | pending → published only after PUBACK by conditional update; pending → succeeded/failed directly allowed for early result. Terminal succeeded/failed/timeout never regresses. PUBACK means broker receipt, not execution or backend result commit |
| Deadline | UTC server/DB issued/expires, default TTL 60s; Gateway checks expiry immediately before apply. Admission and bounded expiry worker release expired active slots and stop outbox publish under the same locking discipline. No lock/network wait for TTL; timeout means no timely confirmation, not proof of no physical apply |
| Late / duplicate result | Validate command/topic/Entity/Gateway/version/interval/completion time against stored command. First accepted result/hash canonical; same duplicate no-op, conflicting duplicate cannot overwrite. After terminal timeout retain bounded late evidence only; do not reverse terminal/configuration or roll desired back |
| Transport | One active backend; fixed bounded queues/workers/scans/inflight, finite DB/MQTT calls, simple fixed publish backoff, stable payload/ID/expiry on retry. No distributed leases/fencing, execution retries/new commands, exactly-once claim or general Stage 6 recovery |

### REST additions (not routes implemented by Task 5.0)

| Method / path | Input / response |
| --- | --- |
| PATCH `/v1/digital-twins/{entity_id}/desired-state` | `Idempotency-Key`; application/json strict object `{frequency_hz: number, expected_desired_version: string}`; reject unknown/duplicate fields and arbitrary action/parameters/metadata/reported input. New command 202 + Location; body entity_id, command_id, status, requested_frequency_hz, effective_frequency_hz, integer sample_interval_us, string desired_version, expires_at, status_url. Preserve exact requested value; effective display is derived. No-op 200 as above |
| GET `/v1/digital-twins/{entity_id}/commands` | Keyset issued_at + command_id, bounded limit 20/100, next cursor in Link; only this Sensor's commands |
| GET `/v1/digital-twins/{entity_id}/commands/{command_id}` | Exact scoped command; safe status/version/allowlisted parameters/times/errors/confirmed result, never internal payloads/secrets/raw diagnostics |

Reuse cache `no-store`, CORS/Accept, URI decode once, query/body bounds,
X-Request-ID and error envelope from current HTTP boundary. Add route/cache
matchers later. Command DTOs are ordinary JSON, not NGSI-LD Entities. Keep
`/attrs` metadata-only and existing `?format=simplified` state view; do not
restore `/state`, `/history`, `/v1/telemetry/history` or old metadata PATCH.

### MQTT additions (not runtime enabled by Task 5.0)

| Topic | Backend data principal | Gateway with matching ID |
| --- | --- | --- |
| `gateways/{gateway_id}/commands/{command_id}` | publish | receive / subscribeLiteral / unsubscribeLiteral `gateways/{gateway_id}/commands/#` |
| `gateways/{gateway_id}/responses/{command_id}` | receive / subscribeLiteral / unsubscribeLiteral `gateways/+/responses/#` | publish own responses only |

- Both QoS 1, retain=false; retained command/result rejected. Keep telemetry ACK
  ACLs; no `$CONTROL` or broker-wide wildcard grant to data principal.
- Command: protocol_version 1, message_type command, UUID command_id, gateway_id,
  Sensor entity_id, action update_configuration, parameters with **only** integer
  sample_interval_us, string desired_version, issued_at/expires_at UTC in order.
  No MQTT frequency_hz; UUID/topic/payload/URN identities must agree; strict
  fields, including rejection of duplicates. Plan section 7 JSON is canonical,
  not the historical Gateway/seconds examples in `mqtt-v1.md`.
- Result: protocol_version 1, message_type command_result, matching command_id,
  gateway_id, entity_id, string desired_version, completed_at UTC. Succeeded
  only after durable config + active timer/controller apply/readback; its
  reported_state has **only** the matching interval. Backend maps it to the
  configuration branch, never arbitrary root-state keys.
- Failed result has no fake reported state; safe errors allowlisted to
  COMMAND_EXPIRED, STALE_DESIRED_VERSION, UNKNOWN_SENSOR,
  UNSUPPORTED_CONFIGURATION, APPLY_FAILED. Never clamp and claim success.
- Gateway duplicate ID/same hash returns durable cached result before checking
  expiry/version; no reapply. Same ID/different hash cannot replace canonical
  result. Reject older desired version; route only managed Sensors, ignore a
  different Sensor process rather than publishing a competing failure.
- Durable result bytes can be republished boundedly during TTL; PUBACK is not
  backend commit. No result-ACK topic or infinite resend. Keep cached duplicates
  after TTL under the bounded retention configured/documented in the later
  Gateway persistence task; do not evict active receipts. Exact cache quotas and
  retention tuning are implementation configuration work, not new API semantics.

## 4. Locked DB lock discipline

New admission/result/expiry paths use **Gateway FOR SHARE → membership FOR SHARE
(actor-bound admission/replay only) → relational Sensor FOR UPDATE → Sensor
Entity (if locking required, FOR UPDATE) → twin_states FOR UPDATE → command →
outbox/receipt**. Use the same order for expiry inside admission and worker/result
handling. Bound scans; discovery queries outside the transaction do not confer
authority: reacquire resource/state locks and recheck before mutation. Publisher
must not lock outbox first then wait for state/Sensor; network I/O is outside SQL
transactions, with a subsequent conditional update that cannot undo terminal.

Why the lock **mode** matters (not just a diagram):

- `postgres_sensor_patch.go` and current
  `ProvisionSensorForAdminOrOwner` take parent FOR SHARE, membership FOR SHARE,
  then Sensor FOR UPDATE. Metadata patch then locks its Entity.
- `usermanagement/postgres_repository.go` Assign/Remove/Transfer take Gateway
  FOR UPDATE before membership FOR UPDATE. A Stage 5 parent SHARE lock therefore
  serializes authorization with those role changes through intent commit.
- Telemetry does an unlocked Gateway existence check, then **Sensor FOR UPDATE**,
  ledger/raw inserts (including parent FK KEY SHARE), then state FOR UPDATE;
  it does **not** lock Gateway first. Parent FOR SHARE is compatible with its
  later FK KEY SHARE. Taking parent FOR UPDATE while waiting on Sensor would
  recreate the documented FK/parent deadlock cycle. Do not rewrite telemetry to
  claim it already follows the new-path order.
- Older provisioning methods still have parent FOR UPDATE; preserve their
  existing behavior, do not copy that mode into the new control path. No claim
  of deadlock-freedom for every legacy or arbitrary concurrent transaction.

## 5. Locked SQLite migration and activation boundary

**Baseline source:** `outboxSchemaVersion=1`; OpenOutbox takes exclusive sidecar
file lock, opens SQLite, and `prepare` reads PRAGMA user_version. Version 1 checks
identity; version 0 creates schema only for an empty DB; other versions fail
without recreating files. Admin open uses the same exclusive lock, existing-file
and identity/version checks; no concurrent admin migration of a running process.
State consists of outbox_meta binding, immutable outbox payload/topic/identity/
interval/byte_size/status and outbox_streams next_sequence per boot. Pending and
quarantined rows retain their quotas. Schema creation already commits DDL and
user_version in one transaction, with ambiguous commit classified separately.

**Future migration boundary:** under that exclusive lock, after validating the
existing version/identity and before ACK/MQTT workers or Scheduler creation,
transactionally upgrade schema 1 to the next supported SQLite version (2 if no
other schema change intervenes). Add bounded per-Sensor config and command
hash/version/result receipts, retain existing pending/quarantined rows byte-exact,
sequences, binding and byte/row accounting. Advance user_version in the same
transaction; rollback failure leaves old schema/data usable, ambiguous commit
requires supervised reopen/inspection. Unknown/newer/corrupt/foreign files fail
closed; never delete/recreate, reset sequence, silently copy to a fresh file or
upgrade through the read-only admin path. Old binaries cannot read the new
version: rollback needs an explicitly reviewed data-compatible procedure, not
file deletion. No migration code is added in Task 5.0.

**Startup activation:** current `cmd/gateway/main.go` loads static cfg →
OpenOutbox → ACKProcessor → Transport → NewScheduler → start/subscribed readiness
→ Scheduler.Run. Insert migration and durable runtime-config loading **after
OpenOutbox and before NewScheduler / any new batch generation**. Existing env
integer-Hz profile remains bootstrap fallback only, never overwrites a stored
runtime config on reopen. Broker readiness/replay behavior remains intact.

**Runtime activation:** current Scheduler snapshots sampleIntervalUS,
samplesPerBatch/batchSpan, drives cadence/nextGenAt and generation in one owner
goroutine (`reconcile`, `generate`, `armTimer`); generation currently synthesizes
a completed static batch at `now-batchSpan`. It is not a dynamic sampler.
Later work must serialize per-Sensor control through an independent bounded
wake in that owner/controller, flush only already-measured old-profile samples,
apply the new timer, and read back active interval. Durable config and receipt
plus successful controller activation are all required before caching/publishing
succeeded. Persist/apply failures cannot report success. No SQLite transaction
waits on MQTT; an uncertain persist/apply boundary stops for supervised recovery,
not a guessed success. On restart load durable config before any new sample;
already-enqueued telemetry continues as exact stored bytes. No exactly-once
guarantee across power loss or arbitrary driver effects. Simulator remains one
Gateway/Sensor per process, not a restriction on the API's Sensor universe.

## 6. Task 5.0 verification and non-claims

- `stage5_frequency_spike_test.go` is entirely test-only. Tests compiled and
  executed RED on a not-implemented helper, then GREEN after the minimal exact
  rational conversion experiment. Cases: 100/60/1.5/0.5/0.1/0.01 Hz,
  25.6 Hz exact half-up tie, exact 2^53 interval, tiny 1e-10/1e-128 rejection,
  <=0 / decimal infinitesimally >100, canonical decimal equivalence. No
  production models, request parser or service introduced (Task 5.1 remains).
- Native inherited telemetry fixture now seeds confirmed configuration, ingests
  two delayed old-profile batches (one advances observation via the real SQL
  merge; the older one leaves current state untouched), asserts configuration
  including large string version unchanged, measurement interval still 10000
  rather than confirmed 20000, raw/ledger/temporal increments, desired and
  Gateway preservation. Existing behavior was already correct: this regression
  is GREEN without production edits, not a fabricated missing-behavior RED.
- Full `sh scripts/test-stage3-persistence.sh` after the test addition passed
  fresh/upgrade/rollback, compressed late insert, legacy provenance/history,
  existing 2^53 round-trip and cleanup checks on PostgreSQL 16.15 / TimescaleDB
  2.29.2. This is uplink regression evidence, **not** downlink integration.
- Full local CI details and final post-edit checks are recorded in the local
  REPORT.md; GitHub checkout/setup/action upload infrastructure and remote CI
  are NOT RUN, not simulated PASS. Unit tests intentionally skip opt-in native
  fixtures outside their owned environments; required local native lanes were
  separately executed and guarded. Legacy native scenarios explicitly deferred
  by current ci.yml remain deferred, not newly accepted.
- Board/real sensor, Stage 5 REST/MQTT end-to-end, dynamic timer, ACL rollout and
  SQLite migration execution are **NOT RUN / not implemented**, intentionally
  outside Task 5.0. These are later-task gates, not Task 5.0 acceptance claims.
