# Task 5.3 — REST intent endpoints and basic composition

**Task 5.3 DONE — final verification PASS after the separately authorized Gateway
blocker investigation below.** Cập nhật 24/10/2026: Task 5.4 cũng đã hoàn thành;
Stage 5 hiện **tạm dừng sau 5.4** theo kế hoạch, chưa triển khai 5.5–5.8.
Xem [mốc bàn giao hiện hành](stage-5-task-5.4-publisher-acl-and-expiry.md).
Các hướng dẫn giao Task 5.4 và câu hỏi STOP bên dưới chỉ là lịch sử tại mốc 5.3.

Date: 2026-10-23. Baseline: `develop`, HEAD
`bcce3fec29100e9ce2230a3c8ea926202f9faab0`; worktree was clean before this task.
Read the entire external detailed plan and all Task 5.0/5.1/5.2 handoffs.
No applicable AGENTS.md was found in this checkout or its ancestors. Used TDD,
verification-loop and security-review guidance, subject to the explicit prohibition
on commits/subagents. No commits, pushes, remote CI, deployment DB/broker/credential/
volume changes, external-plan edits or later-task implementation.

## Original STOP / question (history preserved; resolved below)

The final valid invocation, from `src`, was:

```sh
go test -json -race -count=1 -coverprofile=/tmp/opencode/task53/unit-final-v2.cover ./... > /tmp/opencode/task53/unit-final-v2.json 2> /tmp/opencode/task53/unit-final-v2.stderr
```

It exited **1**. The recorded failure is in untouched Gateway code:

```text
=== RUN   TestRunSignalShutdownPreservesPendingAndReopens
    main_test.go:372: run did not stop after cancellation
--- FAIL: TestRunSignalShutdownPreservesPendingAndReopens (5.03s)
FAIL iot-platform/cmd/gateway
```

The user's instruction requires immediate STOP on an unrelated repository defect.
No Gateway code/test was changed, no assertion was weakened, and this failure was
not silently retried or labeled an invalid invocation. Earlier full race runs
passed; that does **not** override this final failure. Timing/load sensitivity is
possible but unproven. **Question: authorize separate investigation/rerun of this
Gateway shutdown failure, or handle it independently before resuming Task 5.3
acceptance?** Do not automatically start Task 5.4 while this gate remains blocked.

## Implemented Task 5.3 scope (uncommitted)

- `PATCH /v1/digital-twins/{entity_id}/desired-state` commits/replays an intent;
  raw receipt status/body bytes are returned unchanged, including after a newer
  version. New pending intent is 202 with Location matching stored status_url;
  confirmed no-op is 200. Existing exact rational fingerprints, string BIGINT
  versions, interval ceiling **2^53**, stable outbox bytes and admission locks are
  reused, not reimplemented with floating-point policy checks.
- `GET .../commands` returns ordinary JSON `{ "commands": [...] }`, default 20 /
  maximum 100, keyset continuation in Link. `GET .../commands/{command_id}` uses
  the existing exact Sensor/Gateway/membership-scoped query projection. Neither
  response pretends to be an NGSI-LD Entity or exposes outbox/raw diagnostics.
- Existing JWT middleware runs before handlers. A preliminary repository
  authorization transaction gives current role/scope errors precedence over body/
  query/unsupported-target errors: owner/operator mutate, viewer reads but gets
  403 on PATCH, nonmember/admin-without-membership gets 404. This preliminary
  check grants no durable authority: admission rechecks under its original locks
  through commit; list/detail also recheck membership. JWT role claims do not
  substitute for membership. Revocation does not cancel previously accepted rows.
- Strict Task 5.1 body/key/media/numeric parser; strict list/detail query allowlists,
  duplicate/empty/malformed query rejection, **existing 1024-byte raw query bound**,
  512-byte decoded cursor bound and existing repository cursor validation. URI
  decoding happens once in net/http, never again in the handler.
- JSON Accept negotiation, safe error envelope/request IDs, exact-origin CORS
  (including Location/Link exposure), no-store on all three route shapes before
  auth/CORS/method dispatch. Existing CORS already allowed Idempotency-Key, so no
  CORS policy expansion was necessary. Separate control cache matcher leaves the
  Stage 4 matcher and removed state/history routes intact.
- One request context/deadline spans authorization, parsing and repository calls;
  repository child timeouts cannot reset/extend it. A context callback interrupts
  blocked HTTP body reads via ResponseController only when that budget ends,
  without extending earlier server read deadlines. A started callback is joined
  before handler return/connection reuse. No unbounded body-read goroutine.
- Desired changes do not write reported/configuration/measurement fields. Existing
  detail and `?format=simplified` state APIs remain in place. `/attrs` remains
  metadata-only. All three active expiry statuses retain the user-approved Task
  5.2 admission-expiry behavior; this task adds no expiry worker.

## Files

New production:

- `src/internal/httpserver/control_handlers.go`
- `src/internal/digitaltwin/postgres_control_authorization.go`
- `src/internal/config/stage5.go`
- `src/cmd/server/stage5.go`

New tests/rehearsal:

- `src/internal/httpserver/control_handlers_test.go`
- `src/internal/config/stage5_test.go`
- `src/cmd/server/stage5_test.go`
- `src/cmd/server/stage5_native_http_test.go`
- `scripts/tests/stage5_http_fixture.py`

Modified wiring/guards/docs:

- `src/internal/httpserver/router.go`, `src/cmd/server/main.go`,
  `src/internal/config/config.go`
- `src/internal/httpserver/stage4_cache.go` (comment only),
  `stage4_headers_test.go`, `stage4_router_test.go` (exact route-surface guard now
  requires the original five plus precisely these three; removed APIs stay banned)
- `.env.example`, `docker-compose.yml`
- `scripts/tests/native_compose_smoke.py`, `test_native_compose_smoke.py`
  (explicit disabled defaults for new Compose interpolations; guards retained)
- This task-local handoff.

No migration, MQTT publisher/ACL/client, Gateway/SQLite feature, result runtime,
global expiry, Stage 5 E2E/CI lane or full client-document work was added. The
separately authorized shutdown investigation subsequently made one narrow existing
Gateway scheduler completion-accounting correction, documented below.

## Configuration / safe usage

`STAGE5_ENABLED=false` by default. Authenticated calls while disabled return safe
503; unauthenticated calls still get 401. Enabled composition fails closed on
missing/typed-nil repository/pool or invalid bounded configuration. Existing
readiness still checks the DB; MQTT readiness/ACL lifecycle is **not** implemented
or claimed in this intent-only task. Ingress and credential feature flags are
independent and unchanged.

| Environment | Default | Bounds |
| --- | --- | --- |
| STAGE5_REQUEST_TIMEOUT | 5s | 1ms–1min; shared request/DB budget |
| STAGE5_COMMAND_TTL | 60s | 1s–24h, microsecond precision |
| STAGE5_MAX_PENDING_OUTBOX | 10000 | 1–1000000 |
| STAGE5_MAX_RECEIPTS | 100000 | 1–1000000 |

Use only an isolated rehearsal at this checkpoint, with migration 23/runtime grants
and owned fixtures. Do not enable on the live deployment: no consumer publishes
the accepted intents or receives execution results yet. The task-local fixture
never loads deployment `.env`, runs an ephemeral real TCP HTTP server with actual
HS256 JWTs and the runtime DB principal, requires both native tests to pass without
skips, and reuses the existing pinned PostgreSQL fixture/migration/owned-cleanup
pattern. It is not the later Task 5.7 MQTT E2E runner.

## Actual RED → GREEN and verification

Evidence directory: `/tmp/opencode/task53/`. Commands below ran from repo root
unless marked `src`. Prior-task/baseline reports are inherited context only.

| Command / target | Actual result |
| --- | --- |
| `src`: `go test ./internal/httpserver -run '^TestControlRoutesAuthenticationAndCache$' -count=1` | Executable RED: all three new routes returned 404 without no-store; `red-routes.log`. Then GREEN; `green-initial-unit.log` |
| `src`: `go test ./internal/config -run '^TestStage5EnvironmentBounds$' -count=1` | Executable RED: new invalid settings ignored; `red-config.log`. GREEN after loader |
| `python3 scripts/tests/stage5_http_fixture.py` with composition runtime stub | Real PostgreSQL/HTTP-test executable RED: missing composition, not compilation/Docker failure; `red-native.log`; owned cleanup verified |
| `src`: `go test ./internal/httpserver -run '^TestControlQueryReusesHTTPRawBound$' -count=1` | Executable RED on this task's initially too-large raw query bound; `red-query-bound.log`. Corrected to existing constant, GREEN in final affected tests |
| `src`: `go test ./internal/httpserver -run '^TestControlRequestBudgetBoundsSlowHTTPBody$' -count=1 -timeout=15s` using corrected raw TCP fixture | Executable RED: body read outlived 50ms shared budget, peer read timed out at 2s; `red-slow-body-corrected.log` |
| `src`: `go test -race ./internal/httpserver -run '^TestControl' -count=1 -timeout=30s` | PASS after body-read interruption/join; `green-control-budget.log` |
| `CONTROL_HTTP_COVERAGE_FILE=/tmp/opencode/task53/native-final-v2.cover python3 scripts/tests/stage5_http_fixture.py` | **PASS, both required native top-level tests, no skips**, final HTTP/body-timeout production code; `native-final-v2.log`; owned fixture cleanup verified |
| `CONTROL_TEST_COVERAGE_FILE=/tmp/opencode/task53/intent-native.cover python3 scripts/tests/stage5_intent_fixture.py` | PASS existing 11 required Task 5.2 native tests plus fresh/upgrade/three preflight-refusal scenarios; `intent-native.log`; all owned cleanup verified |
| `sh scripts/test-stage2-authorization.sh` | PASS, four require-run native authorization tests plus isolated admin check; `authorization-native.log` |
| `sh scripts/test-stage2-admin.sh` | PASS; `admin-native.log` |
| `python3 /tmp/opencode/task53/membership_native.py` | PASS both existing DB repository/removal native tests without skips, schema 23/runtime principal; exact owned cleanup. External task-local driver reuses existing fixture, passes explicit isolated URLs, never deployment `.env`; `membership-native.log` |
| `MANDATORY_CI=1 sh scripts/test-stage4-api.sh --scenario all` | PASS vertical/contract/queries, native read/state/metadata checks and compressed equivalence; `stage4-native.log`; exact fixture/shared-image cleanup. Ran before the final control-only body-read timeout addition; not a post-addition rerun |
| `src`: format check, `go vet ./...`, `go build ./...`, `golangci-lint run --timeout=5m` | PASS on final production/test code, 0 lint issues; `static-final.log` |
| `python3 -m unittest scripts/tests/test_ci_go_test_guard.py scripts/tests/test_ci_feature_jobs.py scripts/tests/test_stage4_api_runner.py scripts/tests/test_stage4_contract_scenario.py scripts/tests/test_stage4_queries_scenario.py scripts/tests/test_stage4_vertical_scenario.py scripts/tests/test_native_compose_smoke.py scripts/tests/test_migration_ci_compat.py` | PASS **287 tests**; `python-final.log`. Mocked runner outputs are not native acceptance |
| `src`: earlier full `go test -json -race -count=1 -coverprofile=... ./...` | PASS; `unit-race-final.json`, 4,648 passing test events and **95 opt-in native skip events**, not native PASS. This predates the final slow-body test/implementation |
| `python3 scripts/ci_go_test_guard.py --input /tmp/opencode/task53/unit-race-final.json` with seven Task 5.3 `--require` entries | PASS, seven required tests verified; `go-guard.log`. Predates final slow-body change, not final full-suite acceptance |
| Final full race command quoted under STOP | **FAIL, unrelated Gateway shutdown test**; `unit-final-v2.json`. Immediate STOP; no final completion/coverage claim |
| `git diff --check` | PASS before the final slow-body change; final post-handoff check NOT RUN due STOP |

Native HTTP assertions cover owner/operator/viewer/nonmember/admin-without-membership,
invalid/missing JWT, injected JWT gateway-role claims ignored, revoked-membership
replay/read, strict duplicate/unknown body fields, media/key/body/query/cursor bounds,
URI once-only decoding, 202/200 and exact 409 codes, Location/status_url polling,
pending DB command+outbox without Gateway, exact 202 replay after newer version and
exact 200 no-op replay, >2^53 version strings and exact 2^53 interval, no side effects
on rejection, reported-state preservation/state readback, metadata config rejection,
cross-Sensor command ID and cursor denial, 105-row traversal/default/max limits,
safe fixed error text instead of private diagnostics, CORS/cache/request IDs.
Confirmed configuration is seeded solely to exercise no-op; it is **not evidence
that a result runtime exists**.

### Retained mistakes and corrections (not product failures)

- `green-native-first.log`: the double-escape test incorrectly assumed Go
  PathEscape escapes `:`; corrected the test to literal `%253A` and added a
  successful `%3A` control. No URI contract changed.
- `green-native-second.log`: pagination seed lacked required
  requested_frequency_hz; added the valid field, did not weaken the schema.
- Initial `scoped-unit.log` failures were directly affected route guards still
  asserting the pre-Task-5.3 surface. Updated exact expectations to eight routes,
  disabled 503 and forbidden command POST 405; removed-route checks preserved.
- Initial Python guard run failed `missing_interpolation` because this task added
  Compose variables without corresponding owned-fixture defaults. Fixed those
  defaults and added a guard; final 287 Python tests pass.
- Initial lint found an unused mock mutation flag; the test now asserts it.
- First slow-body test used an io.Pipe/http.Client combination that hung in the
  **client fixture**, including a tool timeout and a 15s Go-test timeout. Those
  `red-slow-body.log` / `red-slow-body-executed.log` attempts are invalid RED,
  superseded by the corrected raw-TCP executable RED and subsequent GREEN.
- Interim union coverage before the final body-read change was 121/127 statements
  (95.3%) in the four new production files. Final profiles exist, but final union
  coverage was **not computed/claimed** after the unrelated full-suite failure.

## NOT RUN / Task 5.4 handoff after blocker resolution

- Native binaries follow the existing CGO-disabled/container pattern; they were
  not run under race. Ordinary/affected Go tests used race as recorded above.
- No remote CI, live rollout, AM5728/real sensor, Stage 5 HTTP→MQTT→Gateway→result
  E2E, ARM build or full final baseline matrix. No full
  Stage 5 acceptance claim. Existing native Auth-server user-management HTTP E2E
  remains opt-in/skipped; membership DB native and authorization native ran here.
- The originally requested post-blocker final checks/require-run guards and
  uncommitted diff review have now run; see the final verification addendum below.
- Only then assign Task 5.4 separately. It owns backend MQTT ACL changes, bounded
  publisher/reconnect/PUBACK handling, global bounded expiry and transport readiness/
  shutdown. Reuse stored payload bytes, filter dispatchable control_version=1 and
  publishable command status, never dispatch legacy sentinels. Reuse the locked
  expiry helper for pending/published/**acknowledged**, stop outbox on timeout;
  never overwrite terminal state or regenerate a replay receipt.
- Tasks 5.5/5.6 still own Gateway durable application and result confirmation;
  accepted/published never means applied. No Task 5.4+ implementation was performed.

## Authorized Gateway shutdown investigation / final verification addendum

The user authorized this blocker investigation separately, not Task 5.4+. Detailed
commands, RED/GREEN and source analysis: `/tmp/opencode/gateway-shutdown/handoff.md`.
Original failing JSON/stderr/profile above remain unchanged and valid history.

**Cause:** the publish goroutine in `internal/gatewaysim/scheduler.go:612-631`
selected completion delivery against the cancelled run context. Both cases could
be ready, so it could discard a completed result and leave `statePublishing` set.
The shutdown drain (`scheduler.go:774-792`) then waited its default 10-second
budget although Publish had returned; the unchanged signal test requires return
within five seconds. This defect was reproduced, not dismissed as host load:
targeted `-race -count=30` passed, then `-race -count=300` failed twice identically.

**Narrow correction:** select against scheduler `done` instead of run-context
cancellation. Results remain deliverable during cancellation drain; delivery can
still abandon once the scheduler exits, including after drain timeout. No budget,
pending-row/ACK/SQLite/reopen/lock policy or existing assertion changed. Added
`TestSchedulerCancelledPublishesStillCompleteDrain`: valid executed RED lost 16
of 32 completion results before the production edit, then GREEN. Only additional
code files changed by this investigation are `scheduler.go` and `scheduler_test.go`.
All prior Task 5.3 changes were preserved.

Final evidence directory: `/tmp/opencode/gateway-shutdown/`. Commands run from
`src` unless indicated; all final commands below exited 0:

| Command / target | Actual final outcome |
| --- | --- |
| `go test -race ./internal/gatewaysim -run '^TestScheduler(CancelledPublishesStillCompleteDrain\|PublishDrainBudgetSharesDeadline)$' -count=100` | PASS; `green-regression.log` |
| `go test -race ./cmd/gateway ./internal/gatewaysim -count=10 -timeout=300s` | PASS both full packages; `green-lifecycle.log` |
| `go test -race ./cmd/gateway -run '^TestRunSignalShutdownPreservesPendingAndReopens$' -count=1000 -timeout=180s` | PASS 1,000 repetitions; `green-signal-1000.log` |
| `go test -json -race -count=1 -coverprofile=/tmp/opencode/gateway-shutdown/unit-final-v2.cover ./...` | PASS all 19 packages, 4,650 passing test events, 95 expected opt-in native skip events; `unit-final-v2.json`, empty `.stderr`, `events-final.txt` |
| Repo root: `python3 scripts/ci_go_test_guard.py --input /tmp/opencode/gateway-shutdown/unit-final-v2.json` with ten exact `--require` entries in the detailed report | PASS all eight Task 5.3 top-level unit tests (including final slow-body regression), original Gateway signal test and new scheduler regression; `go-guard-final.log` |
| Repo root: `CONTROL_HTTP_COVERAGE_FILE=/tmp/opencode/gateway-shutdown/http-final.cover python3 scripts/tests/stage5_http_fixture.py` | Required native HTTP+DB **2 PASS, no skips**, owned cleanup verified; `http-final.log` |
| Repo root: `CONTROL_TEST_COVERAGE_FILE=/tmp/opencode/gateway-shutdown/intent-final.cover python3 scripts/tests/stage5_intent_fixture.py` | Required native intent 11 PASS plus fresh/upgrade/three refusal scenarios; owned cleanup verified; `intent-final.log` |
| Repo root: `MANDATORY_CI=1 sh scripts/test-stage4-api.sh --scenario all` | Native vertical/contract/queries PASS on current final code, including compressed equivalence; exact fixture/shared-image cleanup verified; `stage4-final.log` |
| `gofmt -l` all Go files, `go vet ./...`, `go build ./...`, `golangci-lint run --timeout=5m` | PASS, no formatting output, 0 lint issues; `static-final.log` |
| Repo root: same eight Python unittest targets listed above | PASS 287 tests; `python-final.log` |
| Repo root: `git diff --check` | PASS after correction and final handoff update; `diff-check-final.log` |

Final ordinary race-suite statement coverage: **78.1% repository-wide**, not an
80%-repository coverage claim. `startPublish` is 100%; shutdown drain 81.8%.
Union of final ordinary + native HTTP + native intent profiles for the four new
Task 5.3 production files: **131/137 statements (95.6%)**; `coverage-final.txt`.
Native fixtures use the existing CGO-disabled container binaries, not race-native
execution. Earlier admin/authorization/membership native runs are retained earlier
evidence, not rerun/full-matrix claims. Other NOT RUN boundaries above still apply.

**Result: DONE for this blocker investigation and verification handoff. Task 5.3
verification can now be accepted. No Task 5.4+ work or full Stage 5 completion.**
