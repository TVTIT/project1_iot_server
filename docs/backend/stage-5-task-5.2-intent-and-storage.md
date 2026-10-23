# Task 5.2 — Sensor intent, storage and scoped queries

**DONE: Task 5.2 only. STOP here; Stage 5 is not complete.**

Date: 2026-10-23. Baseline: `develop`, HEAD
`9f08278a2bc69d06ca0fa754dc81bf2d951809c5`, plus the authorized uncommitted
Task 5.0/5.1 files. The full external detailed plan and both handoffs were read.
Those inherited files and the external plan were not edited by this task.
No applicable AGENTS.md exists in this checkout or its ancestors.

## Clarification explicitly approved by the user

The initial stop concerned the expiry treatment of reserved legacy
`acknowledged`. The user selected option 1: **all three active statuses — pending,
published, acknowledged — expire identically during admission**. At
`expires_at <=` the locked admission's DB time, transition the command to timeout,
set completion/safe expiry error, stop its pending/published outbox by setting it
to failed, and release that Sensor's active slot. Preserve desired and reported
state; timeout is not proof that the device did not apply the command.

This is implemented by `ExpireSensorCommandsTx`, which does not commit, publish,
scan globally, or roll desired back. Its caller must already hold the documented
Gateway SHARE → membership SHARE (actor paths) → Sensor UPDATE → Entity UPDATE →
state UPDATE locks. Later Task 5.4 can reuse this transaction-level helper after
acquiring the same resource/state locks. **There is no global expiry worker.**

## Files and decisions

New files:

- `migrations/000023_sensor_control_intent.up.sql` — next free migration; no
  previously applied UP migration was edited.
- `scripts/sql/preflight-migration-000023.sql` and `verify-migration-000023.sql`.
- `src/internal/digitaltwin/postgres_control.go` — bounded admission transaction,
  exact receipts, expiry helper, capacity and ambiguous-commit errors.
- `src/internal/digitaltwin/postgres_control_query.go` — safe list/detail repository
  and entity-bound keyset cursor, not HTTP handlers.
- `postgres_control_integration_test.go`, `postgres_control_test.go` in that package.
- `scripts/tests/stage5_intent_fixture.py` — PostgreSQL-only owned native rehearsal;
  **not** a Stage 5 E2E runner or CI lane.
- This task-local handoff.

Existing schema fixtures/guards updated, not disabled:

- `scripts/sql/verify-schema-hardening.sql`, `verify-migration-000022.sql`,
  `scripts/test-stage2-migrations.sh`, `scripts/tests/migration_ci_compat.py`.
- `scripts/tests/stage4_api_runner.py`, `native_compose_smoke.py` and their
  `test_stage4_api_runner.py`, `test_native_compose_smoke.py`,
  `test_migration_ci_compat.py` guard tests.
- `docker-compose.yml`: only mount/run migration 23 in the existing post-Auth
  compatibility phase. No new server composition, routes or feature wiring.

### Schema and preservation

- Migration 23 follows the existing 21/22 post-Auth deferral convention and checks
  the named predecessor. Repeated application preserves the schema/data.
- Authoritative preflight is repeated under DDL locks. Multiple active Sensor
  commands, negative desired/reported versions, or expires <= issued fail clearly
  and atomically; nothing is deleted, relabeled or auto-remediated.
- Legacy actions, parameters, command/status rows, JSONB outbox payloads and
  state remain unchanged. Historical seconds sentinels and unsupported actions
  retain NULL `control_version` / NULL `payload_bytes`: **not dispatchable Stage 5
  intents**. The upgrade fixture preserves even legacy cancelled commands and two
  simultaneously active Gateway-target commands.
- A trigger derives `target_entity_type`, backed by an Entity/type FK. The partial
  unique index applies only to Sensor targets and includes all three active
  statuses, including legacy rows. It does not impose one-active/Gateway.
- Migration 8 had made the old entity/key command index unique, despite the early
  handoff's description of migration 2. Migration 23 replaces it with a non-unique
  lookup index. New receipts uniquely protect `(actor_id, entity_id, key)`, including
  no-op; different actors may reuse the same key after the active slot is released.
- Existing one-outbox/command uniqueness and lifecycle status constraints remain.
  New nonnegative versions/deadline constraints and issued_at/command_id index
  are validated, not left NOT VALID.
- Serialized outbox bytes are bounded to 8 KiB, immutable with topic/command/payload,
  and checked against the JSONB projection. New control intents require bytes;
  legacy outboxes cannot acquire dispatch bytes through that path.
- Each command has at most one canonical and one late result byte/hash/time tuple,
  each <=8 KiB. Built-in PostgreSQL SHA-256 validates the stored bytes; accepted
  evidence is write-once, identical updates remain possible. This is storage
  groundwork and query projection, **not result ingress or a result transaction**.
- Runtime group inherits new-column privileges on existing tables and gets only
  SELECT/INSERT on receipts. Human anon/authenticated direct INSERT/UPDATE/DELETE
  attempts are rejected, with runtime admission/read as positive controls.

### Admission and queries

- Reuses Task 5.1 models/parsers/fingerprint, including the unchanged **2^53**
  interval ceiling. Actor UUID conveys identity only: membership is rechecked in
  SQL, including before replay; platform admin has no bypass.
- Locked receipt replay precedes version/busy checks, including after a newer
  command/version. Store and return original response bytes, not current status.
  Equivalent decimals replay; distinct rationals conflict even at the same rounded
  interval. No-op requires BOTH desired and confirmed configuration intervals and
  no active command, persists 200, creates no command/outbox and does not increment.
- Equal desired without confirmation after failed/timeout explicitly executes
  again at a new version. Version overflow fails before writes; a proven no-op at
  MaxInt64 still works and preserves the version as a JSON string.
- Desired allowlist merge/version/actor/time, command, stable outbox and receipt
  commit together. No MQTT/network waits occur inside SQL. Commit errors return
  `ErrControlCommitUncertain` joined with dependency-unavailable: retry **the same
  key**, never claim rollback from an ambiguous commit.
- Defaults: TTL 60s, DB budget 5s, pending outbox quota 10,000, retained receipt
  quota 100,000. Constructor bounds TTL to 1s–24h (microsecond precision), DB budget
  to 1ms–1min, row quotas to 1–1,000,000. These are repository configuration, not
  new env/server wiring or throughput claims.
- A short final-write advisory critical section serializes capacity counting plus
  insertion across Sensors. It is not a Gateway active-slot rule and holds no
  network/device wait. Capacity errors reject before commit; no eviction. Replay
  remains available at capacity. Legacy pending rows also count toward capacity.
- List defaults/max are 20/100 with `(issued_at DESC, command_id DESC)` keyset;
  cursor is capped at 512 bytes, validates UTC/UUID and is bound to the Sensor URN.
  Detail joins the exact command/Entity/membership scope. Reads recheck membership;
  viewer can read. Legacy arbitrary parameters and raw diagnostics are not selected
  into the public projection; safe error text is fixed/allowlisted. Canonical result
  reads reuse pure Task 5.1 correlation. No late evidence/outbox bytes are exposed.

## Actual TDD and verification evidence

Logs/profiles: `/tmp/opencode/task52/`. Task 5.0's baseline full-matrix report and
Task 5.1 evidence remain inherited context, not reassigned to this task.

| Command (repo root unless noted) | Actual outcome / evidence |
| --- | --- |
| `RED=1 python3 /tmp/opencode/task52/native.py` | Compiled/executed native RED at schema 22: duplicate active Sensor insert accepted; admission stub returned unavailable. `red.log`; owned cleanup verified |
| Same native driver after implementation | GREEN for original permission/replay/expiry/concurrency/rollback tests, `green-first.log` |
| `QUERY_RED=1 .../native.py`, then native driver | Native query RED on missing behavior → GREEN, `red-query.log`, `green-query.log` |
| `SCHEMA_RED=1 .../native.py` | Native RED on mutable stable bytes, then immutable storage GREEN in final fixture; `red-schema.log` |
| `NOOP_RED=1 .../native.py` | Native RED caught root observation incorrectly satisfying empty configuration through Go map reuse; fixed by decoding a fresh configuration map. `red-noop-proof.log` → final native GREEN |
| `CONTROL_TEST_COVERAGE_FILE=/tmp/opencode/task52/native-final-v4.cover python3 scripts/tests/stage5_intent_fixture.py` | PASS: all **11 required native top-level tests**, no skips; fresh, upgrade/preservation/replay, conflicting-active, negative-version and invalid-deadline preflight fixtures. `native-final-v4.log`; all five owned fixture cleanups verified |
| `sh scripts/test-stage2-migrations.sh` (600s bounded wrapper) | PASS historical migrations plus 20–23 fresh/upgrade verification; `stage2-migrations.log`; exact owned cleanup |
| `sh scripts/test-stage2-authorization.sh`, `sh scripts/test-stage2-provisioning.sh`, `sh scripts/test-stage2-admin.sh` | PASS; required native authorization/provisioning tests guarded; respective `stage2-*.log` |
| `python3 scripts/tests/migration_ci_compat.py` | PASS corrected compatibility at schema 20, repeated Auth compatibility, then 21/22/23 and grants; `migration-compat-corrected.log`; owned cleanup |
| `sh scripts/test-stage3-persistence.sh` (600s wrapper) | PASS fresh/upgrade/rollback, telemetry preservation and 2^53 round-trip, `stage3-persistence.log` |
| `sh scripts/test-stage3-uplink.sh --scenario all` (1800s wrapper) | PASS all nine inherited native uplink scenarios, `stage3-uplink.log`; suite/fixture cleanup |
| `MANDATORY_CI=1 sh scripts/test-stage4-api.sh --scenario all` (2100s wrapper) | PASS vertical/contract/queries at verified terminal 23; `stage4-api.log`; fixture and shared-image cleanup |
| `python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/task52/compose-smoke --full-http` | PASS `RBAC_JWT_HTTP_ACCEPTED`, real migrations/exact terminal, `http_accepted=true`, `exact_cleanup_verified=true`; `compose-smoke.log` |
| From `src`: format check, `go vet ./...`, `go build ./...`, `golangci-lint run --timeout=5m` | PASS, 0 lint issues; `static-final-v3.log` |
| From `src`: `go test -json -race -count=1 -coverprofile=/tmp/opencode/task52/unit-race-v3.cover ./...` | PASS; JSON/stderr at matching paths. Ordinary suite intentionally skips opt-in native cases; not counted as native acceptance |
| `python3 scripts/ci_go_test_guard.py --input <unit JSON> --require iot-platform/internal/digitaltwin:TestControlRepositoryBounds` | PASS; required pure repository-boundary test executed |
| Python unittest modules: `test_stage4_api_runner`, `test_native_compose_smoke`, `test_migration_ci_compat`, `test_ci_feature_jobs`, `test_ci_go_test_guard`, `test_stage3_uplink_runner`, `test_stage4_contract_scenario`, `test_stage4_queries_scenario`, `test_stage4_vertical_scenario` under `scripts/tests` | PASS, **375 tests**, `guards-final.log`; mocked runner output is not native evidence |
| `git diff --check` | PASS |

Native assertions include denied actor no side effects; revocation/lock conflict;
same-key concurrent exact replay and two-key one-winner admission; Sensor independence
on the same Gateway; DB uniqueness for all active statuses; exact 202/200 replay;
atomic rollback at all four intent writes **including rollback of preceding legacy
acknowledged expiry/outbox stopping**; real-commit/lost-response simulation and safe
retry; overflow/capacity/2^53 persistence; bounded keyset traversal of 105 commands
with timestamp ties; cross-Sensor/nonmember/admin-without-membership denial; safe
canonical result query; native grants, byte/hash bounds and write-once evidence.

Final ordinary Go JSON contains 4,627 passing test events and **93 opt-in native
skip events**, including nested cases; skips are not native PASS. The separately
required native fixture above executes every Task 5.2 case without skips. Union
of `unit-race-v3.cover` and `native-final-v4.cover` covers **255/292 statements
(87.3%) in the two new production repository files**, not repository-wide
coverage. Exact counts and skip names are in `final-metrics.txt` and
`expected-native-skips-v3.txt`; final unit require-run guard is `go-guard-v3.log`.

### Retained failures and corrections

- Initial `rg` inspection failed because the executable is absent; used dedicated
  grep/read tools instead. Not product RED or verification evidence.
- `green-expanded.log`: capacity test initially assumed an empty global store,
  despite earlier test-owned graphs in the same container. Fixed setup to admit
  exactly one additional pending row/two additional receipts above measured counts;
  the rejection/replay assertions were retained, not weakened.
- `native-all.log`: migration-22 verifier correctly rejected terminal 23. Updated
  its explicit expected-terminal parameter using migration-21's convention and
  retained the predecessor/name/grants checks.
- `migration-compat.log` and sanitized `compat-diagnostic.log`: own guard bug used
  constant `::regclass` on the not-yet-created receipt table at schema 20. Replaced
  with `to_regclass`, then reran the real compatibility fixture successfully.
- Lint caught an unused test parameter and a literal nil-Context test call; fixed
  test spelling without suppressions or changing the validation assertions.

## Limits / NOT RUN / later-task boundaries

- No REST handlers/routes, feature flag/env wiring, server composition, publisher,
  ACL change, global expiry pass, Gateway/SQLite/scheduler, or result ingress/state
  transaction. Tasks **5.3–5.8 remain unimplemented**; no Stage 5 E2E/CI lane or full
  client-document rewrite was added. Parent must stop after this task.
- No live deployment DB/broker/store was inspected or changed; no remote CI,
  AM5728/real sensor, Stage 5 E2E, ARM cross-build or separate gateway image build
  was run. Existing native regression runners built only their own required images.
- Native DB tests use the repo's CGO-disabled test-binary/container pattern. The
  full ordinary Go suite ran with race enabled; that is not a claim that the native
  PostgreSQL binary ran under the race detector.
- Migration 23 is forward-only, with 10s lock acquisition / 60s statement budgets.
  Its atomic DDL/index/backfill takes table locks; **no zero-downtime or production-
  sized migration claim**. Deployment requires operator preflight, suitable window,
  backup and conflict resolution without silently deleting legacy rows. No DOWN or
  destructive rollback is supplied; future rollback must preserve receipts/intent.
- Quotas retain receipts rather than pruning them; eventual exhaustion fails closed.
  No retention/capacity worker is introduced. One active backend/configuration is
  still the assumed deployment; final admission writes serialize for quota safety.
- Task 5.3 must write receipt bytes/status unchanged on replay, map dependency/
  uncertain commit safely, and not mistake 202 for applied configuration. Task 5.4
  must publish only validated `control_version=1` commands with stored bytes and
  publishable command status, never legacy JSONB sentinels. Task 5.6 owns validated
  canonical/late result admission and configuration merge, not this repository.

Self-review: accuracy 4 (native proof, no production-scale claim); completeness 4
(all Task 5.2 gates, explicit later scope); clarity 4 (long evidence table is kept
in the handoff); actionability 4 (reproducible fixture/commands; deployment still
needs operator window); conciseness 4 (file/guard breadth follows mandatory native
coverage). Average 4.0. Highest-value follow-up is parent review and retaining the
local evidence, not starting Task 5.3 automatically.
