# Task 5.1 — Domain and parsing handoff

**DONE: Task 5.1 only. Stage 5 is not complete.** Baseline `develop`
`9f08278a2bc69d06ca0fa754dc81bf2d951809c5`, plus authorized uncommitted Task 5.0 files.
The external detailed plan was read in full and not modified. Task 5.0 files
(`mqtt-v1.md`, telemetry integration regression, frequency spike and baseline
contract) were preserved, not edited by this task. No commit/push, subagent,
live DB/broker change, dependency change, or telemetry persistence edit.

## Files and contracts

- `src/internal/digitaltwin/control.go`: command envelope, command record,
  desired request, durable receipt, result, BIGINT string version, lifecycle
  statuses, safe error codes/text and canonical request fingerprint.
- `control_frequency.go`: exact decimal JSON grammar and bounded exponent/token
  validation before `big.Rat`; exact business boundary `0 < Hz <= 100`;
  half-up reciprocal rounding to `[10000, 2^53]` microseconds. **2^53 is kept**,
  not replaced by a smaller invented duration ceiling. Requested decimal is
  preserved as `json.Number`; effective Hz is explicitly display-only float64.
- `control_json.go`, `control_mqtt.go`: exact-case object allowlists, duplicate
  rejection including escaped/nested keys, integer-only MQTT interval, canonical
  UUID/topic/URN identity correlation, protocol/action/result validation and
  pure stored-command correlation. No transport, execution, ingestion or storage.
- `src/internal/httpserver/desired_state_request.go`: bounded REST reader and
  parsed request type, Content-Type/query/key/decoded-target checks, safe error
  mapping. **No route, handler, response writer or server wiring.**
- Corresponding `control_test.go`, `desired_state_request_test.go`.

The 8 KiB cap is checked before parsing control envelopes; the HTTP reader honors
a smaller supplied body limit and never reads more than cap+1. Frequency token
length/digits are bounded by 128 bytes and exponent magnitude by 128 before big
arithmetic. Positive but unrepresentable Hz gets `frequency_not_representable`,
not clamping or a new business minimum. Fingerprint is SHA-256 of a framed JSON
array `[action, reduced rational, canonical expected version]`; actor/entity/key
scope belongs to the receipt unique key. Equivalent decimals hash identically;
different rationals remain distinct even when rounded interval is equal.

Version preserves BIGINT values beyond JavaScript's exact-number range as JSON
strings; missing/non-string/nondecimal/negative/overflow is rejected. Leading
zero decimal strings normalize to the same integer. The length cap also bounds
version parsing. Mutation must check increment overflow before writes.

UTC timestamps use `Z`, years 1–9999 and at most six fractional digits, matching
the existing PostgreSQL/telemetry microsecond convention. Expires must be later
than issued. Result correlation requires completion not before issue; succeeded
must match the interval and complete no later than expiry. Expired failures may
complete after expiry. **Arrival time, current clock, late evidence, terminal
state and version ordering against current state remain repository work.**

Success requires reported_state containing only interval and forbids error
fields. Failure forbids reported_state even if null, requires an allowlisted
error code, and generates fixed short safe public text. Optional device error
text is validated as a string within the envelope bound but discarded, never
retained/echoed as diagnostics. Unknown status/code/action/parameter is rejected.
Accepted means receipt HTTP 202 plus pending command, not succeeded. Published
is broker delivery only. Unchanged is a request outcome, not a command status.
Reserved acknowledged counts as active; succeeded/failed/timeout are terminal.

### Error mapping defined for later HTTP handlers

| Domain/boundary condition | Status / safe code |
| --- | --- |
| Invalid JSON, number policy, version, key, query, identity | 400 `invalid_request` |
| Positive Hz outside interval representation | 400 `frequency_not_representable` |
| Non-Sensor control target | 400 `unsupported_command_target` |
| Desired version, active slot, idempotency conflict | 409 `desired_version_conflict`, `command_in_progress`, `idempotency_conflict` |
| Version increment overflow | 409 `desired_version_overflow` |
| Body cap / media type | 413 `payload_too_large` / 415 `unsupported_media_type` |
| Existing forbidden/not found sentinels | 403 `forbidden` / 404 `not_found` |
| Dependency/cancellation/deadline | 503 `service_unavailable` |
| Unexpected error | 500 `internal_error` |

JWT authentication remains the existing boundary's responsibility (401); parsing
is not authorization. Later handlers must not return an in-scope target error
as a substitute for transaction membership checks.

## Actual RED → GREEN evidence

All logs are in `/tmp/opencode/task51/`. Tests were authored first; API-only
runtime stubs returned invalid-input errors (not compile failures), followed by
minimum implementation. No checkpoint commits were made, as explicitly required.

Commands below run from `main_src/src` unless stated otherwise:

1. `go test ./internal/digitaltwin -run '^TestControl' -count=1`
   **RED exit 1**, compiled/executed: valid frequencies, representability error,
   valid MQTT command/result failed on missing behavior. `red.log`.
2. Same command after domain implementation: **GREEN exit 0**.
   `green-domain.log`.
3. `go test ./internal/httpserver -run '^TestDesiredStateRequestBoundary$' -count=1`
   **RED exit 1**, valid request/media/limit/error-mapping behavior missing.
   `red-http.log`. Same command after implementation: **GREEN exit 0**,
   `green-http.log`.
4. Additional precision/strictness/domain/reader tests:
   `go test ./internal/digitaltwin ./internal/httpserver -run '^(TestControl|TestDesiredStateRequest)' -count=1 -coverprofile=/tmp/opencode/task51/scoped.cover`
   **GREEN exit 0**, `green-scoped.log`.

Assertions cover tiny exact >100, <=0, wrong JSON types/syntax, overflow and
exponent/token bounds, half-up tie, ordinary decimal examples, representability
on both sides of the upper rounded interval boundary, duration safety at 2^53,
exact requested response number, canonical decimal equivalence, distinct rational
fingerprints, expected-version identity/overflow/precision, strict nested
fields/duplicates, UTC/calendar/order, identity/topic/UUID, MQTT integer bounds,
retained/oversized payloads, success/failure field rules and safe failure text.

## Final verification actually run

- `test -z "$(gofmt -l .)"`: **PASS**.
- `go vet ./...`: **PASS**, `vet-final.log`.
- `go build ./...`: **PASS**, `build-final.log`.
- `golangci-lint run ./...`: **PASS, 0 issues**, `lint-final.log`.
  Initial lint correctly reported 14 missing exported comments in this task's
  new code; comments were fixed, no suppression or policy change.
- `go test -json -race -count=1 -coverprofile=/tmp/opencode/task51/final.cover ./...`:
  **PASS exit 0**, 4,623 passing test events including subtests; **65 opt-in
  native-fixture test events SKIPPED**, not claimed PASS. Logs:
  `final-tests.json`, `final-tests.stderr`; skip names in
  `expected-native-skips.txt`. Existing auth/membership/telemetry/Gateway/Stage 4
  unit regressions ran with this suite.
- Repository `scripts/ci_go_test_guard.py --input /tmp/opencode/task51/final-tests.json`
  with `--require` for all six `TestControl*` and both `TestDesiredStateRequest*`
  top-level tests: **PASS, eight required tests verified** (`final-go-guard.log`).
- `go tool cover -func=/tmp/opencode/task51/final.cover`: recorded in
  `final-coverage.txt`. Exact five new production paths: **270/285 statements,
  94.7%**; this is not whole-repository coverage. Initial exploratory coverage
  filter also matched unrelated controller files; that 84.4% figure is invalid
  for this scope and is superseded by the exact-path count above.
- From `main_src`:
  `python3 -m unittest scripts/tests/test_ci_go_test_guard.py scripts/tests/test_ci_feature_jobs.py scripts/tests/test_stage3_uplink_runner.py scripts/tests/test_stage4_api_runner.py scripts/tests/test_stage4_contract_scenario.py scripts/tests/test_stage4_queries_scenario.py scripts/tests/test_stage4_vertical_scenario.py`:
  **PASS, 255 tests**, `guards.log`. These are Python runner/contract guards,
  not execution of the native scenarios printed by their mocked tests.
- `git diff --check`: **PASS**. New code reviewed for input bounds, safe errors,
  no SQL/network/file mutation and no float policy/hash path.

One invalid exploratory invocation ran `go tool cover -func=...` from the repo
root, outside the Go module: it failed package resolution and was rerun from
`src`. It is neither a product failure nor RED evidence. No assertion was weakened
to make tests pass.

**NOT RUN:** native DB/broker lanes, full local CI, remote CI, Docker images,
ARM/board/sensor execution, Stage 5 HTTP→MQTT→Gateway→DB E2E. Task 5.0's full local
CI evidence is inherited context only, not reattributed to this task. This task
does not add migrations, query/admission/result repositories, outbox/publishers,
ACLs, SQLite, scheduler or dynamic sampling.

## Task 5.2 handoff / remaining risks

1. Follow the locked Gateway SHARE → membership SHARE → Sensor UPDATE → Entity
   → state → command → outbox/receipt order. Parsing conveys no DB authority.
2. Resolve current actor membership before receipt replay. Scope receipts by
   actor/entity/key; store original committed response bytes including no-op.
   Use `DesiredRequest.Fingerprint()`, never displayed Hz or interval identity.
3. Atomically check version/overflow/expiry/active slot, no-op using BOTH desired
   and confirmed intervals, then desired+command+outbox+receipt. SQL protection
   and stable serialized bytes are not supplied by these domain models.
4. Recheck schema terminal/legacy rows before migration; preserve Task 5.0 data
   semantics and telemetry configuration merge regression. Do not dispatch
   historical sentinel commands or claim confirmed execution at admission.
5. Later result ingestion must correlate a parsed result against its stored
   command before any write, enforce terminal/arrival/version rules and use only
   the configuration fragment. Do not merge this result at reported root.

Self-review (1–5): accuracy 4 (native behavior deliberately unverified);
completeness 4 (Task 5.1 unit/transport parsing only, no E2E claim); clarity 4
(receipt/envelope split requires later adapter mapping); actionability 4 (exact
commands and handoff supplied; parent review pending); conciseness 4 (detailed
evidence is intentionally kept here rather than in the final reply). Average 4.0.
Highest-value next improvement is parent review of these public model contracts
before implementing the Task 5.2 transaction, not expanding this task's scope.
