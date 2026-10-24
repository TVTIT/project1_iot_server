# Task 5.4 — publisher, backend ACL and bounded expiry handoff

**Mốc chốt 24/10/2026: hoàn thành đến Task 5.4; Stage 5 TẠM DỪNG theo thay đổi kế hoạch của người dùng.**
Các kiểm tra local cần thiết đã PASS theo hợp đồng current-composition được duyệt.
Đây là tài liệu bàn giao chính cho mốc dừng, không phải nghiệm thu toàn bộ Stage 5.
Only Task 5.4 was implemented. No Task 5.5/5.6 runtime, Stage 5 E2E/CI or full
Stage 5 completion claim. No subagents, commits, pushes, remote CI or live
DB/broker/store/credential/volume mutations.

Baseline: `develop`, HEAD `bcce3fec29100e9ce2230a3c8ea926202f9faab0`, plus the
authorized uncommitted Task 5.3 work and separately authorized Gateway shutdown
fix. Read the full external plan, all Task 5.0–5.3 handoffs and
`/tmp/opencode/gateway-shutdown/handoff.md`. The inherited changes were preserved;
in particular scheduler completion delivery still selects against `done`, not the
cancelled run context. The external plan was not edited during implementation;
its status was subsequently updated for the user-requested 24/10/2026 pause.

## Mốc hoàn thành 5.4 / trạng thái bàn giao hiện hành

**Quyết định 24/10/2026:** tạm dừng Stage 5 sau Task 5.4. Task 5.0–5.4 đã
hoàn thành trong phạm vi được duyệt; Task 5.5–5.8 **chưa triển khai, hoãn lại**,
không bị hủy và không tự chuyển sang Stage 6. Chỉ tiếp tục khi có yêu cầu mới.
Các đoạn BLOCKED bên dưới là lịch sử đã giải quyết, không phải blocker hiện hành.

### Đã có và chưa có

- Đã có domain/parser Hz decimal, migration 23, admission nguyên tử
  desired/command/outbox/receipt, replay/no-op, command list/detail theo membership;
  ba REST routes và publisher MQTT bounded, expiry, ACL readback, readiness/shutdown.
- `pending` xác nhận intent đã commit; `published` xác nhận broker PUBACK,
  **không phải Gateway đã áp dụng**. `acknowledged` legacy quá hạn cũng chuyển
  timeout/ngừng outbox như pending/published. Timeout không tự rollback desired.
- **Chưa có** Gateway nhận/apply command và durable dedup/config/result (5.5);
  chưa có result workers/transaction xác nhận `reported_state.configuration` (5.6).
  Responses subscription/raw queue chỉ là groundwork; chưa có consumer xác nhận,
  queue có thể đầy/drop. Không coi dữ liệu test seed/simulated success là kết quả
  điều khiển thật, không tuyên bố tần số cảm biến đã đổi.
- Chưa có Stage 5 simulator→Gateway→result E2E, thử AM5728/cảm biến thật hay
  lane CI Stage 5 (5.7); đồng bộ đầy đủ tài liệu client/protocol của 5.8 còn hoãn.

### Bằng chứng và ranh giới vận hành

- Task 5.0–5.2 nằm trong commit `bcce3fe`; Task 5.3/5.4 và sửa shutdown liên quan
  đang là worktree diff trên commit đó tại thời điểm chốt. Chưa có final SHA mới,
  chưa commit/push các thay đổi này; kết quả không được gán cho một remote run.
- Bằng chứng final local: `/tmp/opencode/task54-current/REPORT.md` và bảng kiểm tra
  bên dưới. Native DB+Mosquitto, 13 intent DB tests, 2 native HTTP tests, current
  credential acceptance/bootstrap và các regression chọn lọc PASS; full Go race
  20 packages PASS; format/vet/lint/build và Python guards PASS.
- Đây là affected local verification cho vận hành thông thường và kiểm tra bảo mật
  tập trung, **không phải full local CI/remote CI hoặc production rollout**. Các
  opt-in skip trong unit suite không được tính là native PASS. Đợt chốt tài liệu
  này không chạy lại code tests hay tạo kết quả runtime mới. Log `/tmp/opencode`
  là evidence local tạm thời; bảng kết quả ở tài liệu này lưu phạm vi bàn giao.
- Giữ `STAGE5_ENABLED=false` mặc định; không bật tính năng điều khiển cho client
  trên shared/live deployment ở mốc này. Nếu bật trong fixture, intent có thể chỉ
  published rồi timeout vì chưa có Gateway/result runtime. Không mô tả feature
  flag là công tắc hủy intent đã commit hoặc rollback desired state.
- Migration terminal của source là **23**, không chứng minh database shared/live
  đã được nâng cấp. Deployment sau này cần preflight, backup và migration window;
  DDL có table locks, chưa đo trên database production-sized.
- Broker triển khai trực tiếp vẫn dùng TLS/auth/ACL. Backend không sở hữu global
  MQTT gate; lỗi/shutdown backend không cam kết đóng broker. Gate/controller giữ
  phạm vi legacy component-only, không phải lớp bảo vệ deployment hiện tại.
- Stage 5 được phép dùng manager để **đọc** ACL trước mỗi data-connection lifetime
  dù credential API tắt. Đây không phải credential chỉ-đọc: manager vẫn có quyền
  quản trị; runtime không sửa ACL và data principal không được cấp `$CONTROL`.

Khi được yêu cầu tiếp tục: đọc lại kế hoạch, kiểm tra HEAD/worktree/schema thực tế
và hợp đồng bảo mật đã duyệt; bắt đầu từ **Task 5.5**, sau đó 5.6, không coi 5.7/5.8
hoặc điều kiện hoàn tất toàn bộ Stage 5 đã được thay thế bằng mốc dừng này.

## Authoritative migrated contract / final verification

The user subsequently approved the current simplified deployed composition:
API/startup-readiness fail-closed and real broker TLS/auth/ACL, **without global
MQTT gate closure on backend faults/shutdown**. Compose owns Mosquitto directly;
no gate supervisor is required or added. Retained controller tests are legacy
**component-only**, not mandatory current deployment acceptance and not equivalent
global-gate protection. This explicitly supersedes all historical approval
questions below; do not ask them again. Scope is ordinary operation and focused
security controls, not a new exhaustive fault matrix.

The acceptance entrypoint now selects the existing fixture's direct-broker
`current=True` mode. See [current acceptance scope](stage-2-current-credential-acceptance.md)
and `/tmp/opencode/task54-current/REPORT.md`. Original failed evidence and prior
BLOCKED decisions below are retained as history. Required affected checks have
completed; the final evidence table follows. **STOP after 5.4; no 5.5/5.6
assignment or implementation.**

### Resolution and current security coverage

The original root cause was the default runner selecting a legacy CLOSED
controller fixture for the active simplified backend, which never opens that
controller. The migrated runner uses the existing direct-Mosquitto fixture,
production bootstrap role construction, no controller mount/build/dependency,
and an explicit completion requirement (no optional skip success). Backend data
and manager principals remain separate; no manager rights were added to data.

Real native checks now cover correct CA/SAN/password, wrong CA/SAN/password
denial, allowed subscription and correlated commands/telemetry delivery,
cross-Gateway publish/subscribe denial, control-topic subscription denial for
device and backend data principal, real JWT/admin-SQL role denials without
mutation, provision/replay/rotate/revoke, old/revoked-password denial, no-store,
secret exclusion, restart metadata and graceful/disabled shutdown. Broker login
after backend shutdown demonstrates independent ownership, not gate protection.
`scripts/test-legacy-mqtt-controller.sh` explicitly retains controller mechanics
as **legacy component-only**; its full matrix is NOT RUN/not required here.
CI's credential job now executes the migrated native entrypoint; this is a local
workflow change, not a remote CI PASS or unsupported legacy coverage claim.

A directly related readiness defect was exposed by native rejected-manager
startup: credential API refused work but `/readyz` returned 200. A focused unit
test reproduced 200 versus required 503; the minimal router change checks the
existing enabled credential startup/ownership readiness predicate. Disabled
credentials stay independent and `/healthz` stays live; no global broker
supervision was introduced. Native final revalidation proves readiness/API 503
with rejected manager credentials and no mutation. Only other production edit
in this investigation was a gofmt-only correction in `stage5_transport.go`.
All inherited implementation and the Gateway scheduler fix are preserved.

### Actual final commands / outcomes

Evidence is `/tmp/opencode/task54-current/`. Commands run from repo root unless
marked `src`. Exact driver commands/results: `native-checks.sh`,
`native-commands.log`, `native-results.tsv`, `final-affected.sh`,
`final-native-commands.log`, `final-native-results.tsv`. Both drivers completed
exit 0. The initial driver began before the readiness fix; final affected
backend/MQTT/HTTP/normal-uplink tests were rerun afterward, not reattributed.

| Command / target | Actual result |
| --- | --- |
| `sh scripts/test-stage2-mqtt-credentials.sh` | **PASS on final code**, `credentials-final.log`: real direct-broker TLS/auth/ACL, enabled/unready/disabled startup, four HTTP routes, no skip, verified exact owned cleanup |
| `CONTROL_MQTT_COVERAGE_FILE=/tmp/opencode/task54-current/native-mqtt-final.cover python3 scripts/tests/stage5_mqtt_fixture.py` | **PASS on final code**, `stage54-final.log`: exact native ACL positive/negative controls, durable DB+MQTT lifecycle, QoS1/retain/payload/PUBACK-before-mark, stable-byte retry, expiry/terminal monotonicity, composed readiness/shutdown; strict require-run and owned cleanup |
| `CONTROL_TEST_COVERAGE_FILE=/tmp/opencode/task54-current/native-intent.cover python3 scripts/tests/stage5_intent_fixture.py` | **PASS 13 required native DB tests, no skips**, fresh/upgrade/three preflight refusals, owned cleanup. Digital Twin production source unchanged by subsequent readiness-only fix |
| `CONTROL_HTTP_COVERAGE_FILE=/tmp/opencode/task54-current/native-http-final.cover python3 scripts/tests/stage5_http_fixture.py` | **PASS on final code, 2 required native HTTP tests, no skips**, `http-final.log`, owned cleanup |
| `python3 scripts/tests/dynsec_bootstrap_cli.py --report /tmp/opencode/task54-current/bootstrap-report.json` | **PASS**, exact eight-ACL native store, 17 assertions, collision/no-overwrite and verified owned cleanup |
| `sh scripts/test-stage2-authorization.sh` | **PASS**, 4 required native tests verified by guard, owned fixture teardown |
| `sh scripts/test-stage2-provisioning.sh` | **PASS**, 5 required native tests verified by guard, owned fixture teardown |
| `sh scripts/test-stage2-admin.sh` | **PASS**, native PostgreSQL admin check, owned fixture teardown |
| `sh scripts/test-stage3-persistence.sh` | **PASS**, database-only migration/fresh/upgrade/rollback rehearsals and verified cleanup |
| `sh scripts/test-stage3-uplink.sh --scenario normal` | **PASS on final code**, `stage3-normal-final.log`: two Gateways/200 raw rows/2 ledgers/2 temporal rows, normal SIGTERM, owned fixture/images cleanup |
| `sh scripts/test-stage3-uplink.sh --scenario tls-credential` | **PASS**, actual wrong CA/SAN/password negative controls with positives and unchanged durable state; owned fixture/images cleanup |
| `sh scripts/test-stage3-uplink.sh --scenario acl-control` | **PASS**, native cross-Gateway/control-topic denials with broker evidence, positives and no unauthorized durable writes; owned fixture/images cleanup |
| `STAGE4_REQUIRE_RUN=1 sh scripts/test-stage4-api.sh --scenario all` | **PASS**, vertical/contract/queries, exact data/fixture/images cleanup; no skip |
| `src`: gofmt check; `go vet ./...`; `golangci-lint run --timeout=5m`; `go build ./...` | **PASS after last production change**, lint 0 issues; logs `vet-final.log`, `lint-final.log`, `build-final.log` |
| `src`: `go test -json -race -count=1 -coverprofile=/tmp/opencode/task54-current/unit-race-final.cover ./...` | **PASS after last production change**, all 20 packages; 4690 pass events including package events, 98 expected opt-in native skips, zero fail events/empty stderr. Unit-only statement coverage 77.3%, not a coverage-threshold or race-native claim |
| `python3 scripts/ci_go_test_guard.py --input /tmp/opencode/task54-current/unit-race-final.json ...` | **PASS 8 required security/lifecycle/readiness targets**, no required skips; exact target list in investigation report |
| `PYTHONPATH=scripts/tests python3 -m unittest ...` (15 affected modules) | **PASS 484 tests**, `python-final-v3.log`; includes bootstrap/operator/Compose, credential/current-CI, require-run and Stage3/4 runner guards. Mocked guards are not native acceptance |
| `PYTHONPATH=scripts/tests python3 -m unittest test_ci_smoke_jobs test_dynsec_native_ci` | **PASS 49 additional CI/native-job guard tests**, `python-ci-additional.log` |
| Python `py_compile`, shell `sh -n`, final `git diff --check` | **PASS**; exact commands in report |

The migrated runner selection had test-first guard RED, and the original native
gate failure is retained. Own first direct-broker probe incorrectly used the
manager as a data publisher; corrected to exact bootstrap roles plus backend data
principal (`native-credentials-first.log`, cleanup verified). Native unavailable
manager exposed the real readiness gap (`native-credentials-final-v2.log`);
behavioral unit RED is `red-credential-readyz-behavior.log`, GREEN is
`green-credential-readyz.log`. An earlier missing test import caused compilation
failure (`red-credential-readyz.log`), explicitly NOT behavioral RED. No timeout
inflation, skip, weakening of TLS/auth/ACL, or rerun-until-green explanation.

This final ordinary-operation/security scope supersedes the old unfinished list:
Stage3's unrelated full fault matrix, exhaustive legacy controller failures,
remote CI, board/real sensors, live rollout, and Stage5 Gateway/result E2E are
**NOT RUN**, not required/claimed for this checkpoint under latest steering.
No full-local-CI claim. No arbitrary coverage threshold or combined-profile
percentage is claimed. Remaining Stage5 features are still unimplemented.

All statements below describe their historical checkpoint, not current code or
current acceptance status. In particular the credential fixture files were
untouched at the original failure but have now been explicitly migrated.

## Historical STOP / exact question (superseded)

The sequential native regression driver executed:

```sh
python3 scripts/tests/dynsec_bootstrap_cli.py --report /tmp/opencode/task54/bootstrap-native-report.json
sh scripts/test-stage2-mqtt-credentials.sh
```

First command **PASS**. Second command **FAIL, exit 1**:

```text
PASS standalone startup enabled
PASS owned cleanup verified project=dynsec_spike_f15cac8fe272
...
scripts/tests/task2612_standalone.py:225: gate(True)
scripts/tests/task2612_standalone.py:206:
    require(actual == opened, 'standalone gated TLS OPEN/CLOSED')
RuntimeError: standalone gated TLS OPEN/CLOSED
```

Evidence: `/tmp/opencode/task54/stage2-credentials.log`,
`regression-commands.log`, `regression-results.tsv`. The assertion tests a TLS
handshake on the existing gated broker listener after standalone startup. These
credential/gate fixture files are untouched. Stage 5 defaults false and its new
runtime is inert in that path. This is **not** classified as an invalid invocation,
silently retried, attributed to host load, or dismissed because other tests pass.
Root cause/relationship is not established. The driver uses `set -eu`, stops at
the first failure, and performed verified owned cleanup. No subsequent regression
lane ran.

**Question:** authorize a separately scoped investigation of this standalone
gated-TLS regression, or have another worker resolve it before resuming Task 5.4
acceptance? Do not weaken `gate(True)` or replace native proof with mocks. No
credential/gate fix is included here. Stopped on notification of this failure;
only evidence inspection and handoff writing followed.

## Historical separately authorized gated-TLS investigation — then BLOCKED

**Subsequent steering:** the user selected Option 2 (acceptance migration), NOT
restoration of backend controller ownership. That selection supersedes the
two-way approval question below. The follow-up ownership audit stopped at the
user's explicit no-legitimate-owner condition; no runner/test/product edits were
made. See `/tmp/opencode/task54-gated-investigation/option2-ownership.md` for the
precise remaining question and source-only coverage map. Current root Compose
runs Mosquitto directly and wires no gate/controller supervisor. The retained
controller's component tests cannot establish backend-fault/shutdown-triggered
global MQTT closure in that deployment. Task 5.4 remains BLOCKED; no final checks
or native acceptance are reattributed to this follow-up.

Reproduced the first failure on a fresh owned fixture without changing product
or test code. Diagnostic evidence:
`/tmp/opencode/task54-gated-investigation/reproduce.log`; scoped report:
`/tmp/opencode/task54-gated-investigation/report.md`. Original failures are
preserved. A diagnostic launcher wrapped Python TLS only to print and re-raise
the exception, without changing TLS verification or assertion behavior.
Result: startup ready, then `SSLEOFError: UNEXPECTED_EOF_WHILE_READING` at the
same `gate(True)` assertion, exit 1, verified owned cleanup.

Source establishes a **legacy fixture/current composition contract mismatch**:

- `task266_provision.py:123,136-142,154` selects the legacy lifecycle Compose
  fixture, puts Mosquitto on private loopback 18884 and publishes controller gate
  ports 8883/8884. `mosquitto-credential-controller/main.go:54` starts CLOSED;
  `mqttcredential/lifecycle.go:186-199` requires `OpenVerified` to create listeners.
- Current `cmd/server/credential.go:308-338` runs the simplified DB classifier and
  fresh private manager/backend TLS authentication probes, then reports credential
  readiness. It does not obtain a controller receipt or call `OpenVerified`.
  `internal/config/credential.go:79-80` explicitly calls `ControlDir`
  compatibility-only. Shutdown (`credential.go:413-417`) releases ownership, not
  the old controller gate. These files are unchanged by inherited Task 5.4 work;
  simplified composition is also present in commit `6f51ca8`.
- `task2612_standalone.py:224-225` expects that simplified startup to open the
  legacy gate. Later assertions also expect legacy global CLOSED/restart-recovery
  semantics. This is not a certificate-verification failure, timeout/load finding,
  flaky classification, or a diagnosed Stage 5 publisher defect. The repository
  runner selects this incompatible combination; no operator invocation mistake
  is alleged.

**STOP / approval needed:** preserving this runner's OPEN/CLOSED security contract
requires deciding whether to restore verified controller ownership to the active
simplified startup/fault/shutdown composition, or explicitly retire/migrate the
legacy acceptance contract while retaining separate native gate coverage. Neither
is a narrow mechanical fix. Opening the fixture gate unconditionally, removing
assertions, swapping runners and calling it equivalent, or weakening readiness
was not done. Further regression/static/full-race/native acceptance was NOT RUN
after this security/lifecycle design blocker was established. Prior PASS results
remain historical, not final acceptance. Task 5.4 is **not acceptable yet**.

Latest user steering: **STOP after Task 5.4. No Task 5.5/5.6 assignment or work.**

## Accepted security decision / resolved original blocker

The user explicitly accepted the proposal in `stage-5-task-5.4-blocked.md`:
Stage 5 may load a separate bounded management configuration even when
`MQTT_CREDENTIAL_API_ENABLED=false`, strictly to inspect the backend client's
native ACL policy before dispatch/readiness. This does **not** enable credential
endpoints or credential reconciliation.

Implementation reuses `mqttcredential.DynSecClient` for exactly `getClient`,
`getRole`, `getDefaultACLAccess`. It requires enabled backend client, no groups,
exactly `bootstrap_backend`, compatible optional bound client ID, exactly eight
allow/priority-zero ACLs and all four defaults denied. Missing/extra ACLs, groups,
extra roles, disabled client, malformed/missing required ACL values, incorrect
client binding and broad defaults fail closed. The inspection path calls no
mutation methods. Backend username need not literally be the role name.

**Privilege exposure:** the existing manager credential has control-topic access
and can mutate DynSec. Code being read-only does **not** make that credential
read-only. The supported pinned DynSec MQTT control interface shares one command
topic; no fictitious per-command read-only ACL capability was introduced. Reusing
the existing manager is not a claim of a newly least-privileged inspection user.
Protect the environment/process and private management listener accordingly.
Data and management usernames/passwords must differ. Device data traffic stays on
the backend data principal; no `$CONTROL`, responses-publish or broker-wide grant
is added to it.

Freshness is **per data connection lifetime**, not a perpetual broker-policy
attestation: a fresh bounded management connection/readback runs before each
initial/replacement data connection can subscribe/become ready. Management closes
after inspection. Readback is not a broker-side atomic policy snapshot; privileged
administrative changes must be coordinated. On loss, stale readiness/callbacks
cannot revive the old lifetime. Reconnect requires new readback and a fresh exact
responses-filter QoS-1 SUBACK before readiness/dispatch. Failed verification retries
boundedly, with no data publication or optimistic readiness. Runtime never repairs
ACLs. Cancellation also closes the management client's tracked socket.

## Implemented behavior and files

New production:

- `src/internal/mqttcredential/backend_acl.go`: narrow read-only exact policy check.
- `src/internal/mqttcontrol/runtime.go`, `transport.go`: fixed publisher workers,
  bounded discovery/in-flight set/raw-result queue, separate bounded expiry loop;
  dedicated data client wrapping the existing ingress Paho/TLS supervisor.
- `src/internal/digitaltwin/postgres_control_dispatch.go`: bounded pending discovery,
  locked preparation, conditional post-PUBACK mark, bounded expiry discovery.
- `src/cmd/server/stage5_transport.go`: composition, ACL connection prerequisite,
  composite readiness and shared-budget lifecycle.
- `scripts/mqtt_backend_acl.py`: explicit operator-only existing-store procedure,
  injected authenticated/bounded request capability; not an unattended runtime CLI.

Modified production/config/wiring:

- `scripts/mqtt-bootstrap.py`: four precise additions to the existing backend role.
- `src/internal/mqttingress/transport.go`: optional per-connection prerequisite and
  strict QoS-1 SUBACK requirement for control; existing ingress defaults unchanged.
- `src/internal/config/stage5.go`, `stage5_test.go` (inherited Task 5.3 files extended),
  `config.go`, `src/cmd/server/main.go`, `.env.example`, `docker-compose.yml`.

New tests:

- `src/internal/mqttcredential/backend_acl_test.go`.
- `src/internal/mqttingress/control_lifecycle_test.go`.
- `src/internal/mqttcontrol/runtime_test.go`, `transport_test.go`.
- `src/internal/digitaltwin/postgres_control_dispatch_integration_test.go`,
  `postgres_control_mqtt_integration_test.go`.
- `scripts/tests/stage5_mqtt_fixture.py`, `test_stage5_mqtt_control.py`.

Extended existing test infrastructure/guards:

- `scripts/tests/stage3_mqtt_ingress_runner.py`: a control-only selectable branch
  reuses its exact pinned broker/DB/network/bootstrap/cleanup lifecycle, not a
  parallel fixture architecture. Existing ingress/simulator branches retained.
- `scripts/tests/stage5_intent_fixture.py`: discovers required tests across control
  integration files; no skips accepted by its native require-run check.
- `src/internal/digitaltwin/postgres_control_integration_test.go`: optional known
  fixture Gateway constructor; existing callers keep their random owned graph.
- `scripts/tests/test_task284_bootstrap.py`, `dynsec_bootstrap_cli.py`,
  `test_dynsec_bootstrap_cli.py`: exact eight-ACL expectations, no wildcard relaxation.
- `scripts/tests/native_compose_smoke.py`: explicit disabled-profile defaults for
  new Compose interpolations. Inherited unrelated fixture/test edits preserved.

### Publisher / expiry

- Dispatch discovery requires stored `control_version=1`, pending command/outbox,
  stored bounded bytes, unexpired command and due `next_attempt_at`. Legacy JSONB
  sentinels/null bytes are not dispatchable.
- Preparation reacquires Gateway SHARE -> Sensor UPDATE -> Entity UPDATE -> state
  UPDATE -> command/outbox, reuses `ExpireSensorCommandsTx`, validates stored bytes
  against the strict command parser plus stored URN/ID/version/interval/timestamps,
  then commits before returning. Discovery grants no authority. No actor-membership
  recheck cancels an already committed intent.
- Publish uses the stored bytes unchanged, QoS 1, retain=false, exact command topic;
  rejects oversized/mismatched/expired payloads. Network wait is bounded by both
  configured timeout and command expiry. No SQL transaction spans MQTT.
- Only after PUBACK does a new locked transaction expire/recheck and conditionally
  change pending -> published plus outbox pending -> published. Early simulated
  succeeded remains terminal; timeout cannot regress. No reported/desired rewrite.
- Ambiguous/failed DB mark leaves same-ID/same-byte resend possible. Fixed worker
  backoff and scan cadence; bounded queued+active in-flight IDs suppress duplicate
  dispatch in one process. One active backend assumed; no leases/fencing/execution
  retry/new command creation or exactly-once claim.
- Expiry discovers at most one configured batch of expired Sensor command IDs,
  closes discovery before reacquiring resource/state locks, then calls the same
  transaction helper as admission. All pending/published/legacy acknowledged expire
  to timeout and stop pending/published outboxes, release the Sensor slot, preserve
  desired/reported and never regress terminal. An error bounds/stops that pass;
  next configured pass can continue. Prepared publication is rechecked against
  wall-clock expiry immediately before network dispatch.
- Runtime readiness requires running state, successful discovery and expiry passes,
  verified/subscribed transport, and the existing DB/optional-ingress readiness.
  A broker outage keeps HTTP live, not ready; durable accepted intents remain.
- Shutdown stops scans/admission/readiness/reconnect first, cancels control work,
  and closes/joins using the **same** `SHUTDOWN_TIMEOUT` context already shared by
  HTTP, telemetry ingress and credentials, not a fresh per-component budget.

### No result-ingestion implementation

Responses subscription is required transport groundwork only. Callback bounds
topic/payload/retain, copies into a finite raw queue and returns without JSON/SQL.
There are **no result workers, accepted-result writes, configuration merge or
confirmation semantics**. Queue may fill/drop raw deliveries at this checkpoint;
this is not a receiver acceptance claim. MQTT PUBACK/transport queueing never
means applied/succeeded. Task 5.6 owns validated consumption and transaction rules.

## Configuration (centralized, disabled by default)

Inherited intent settings remain. New settings:

| Variable | Default / bound |
| --- | --- |
| `STAGE5_ENABLED` | explicit false |
| `STAGE5_MQTT_CLIENT_ID` | Compose `backend_control`; required nonempty/bounded in enabled loader; distinct from active telemetry client ID |
| `STAGE5_PUBLISH_WORKERS` | 1; 1–16 |
| `STAGE5_SCAN_BATCH` | 32; 1–1024 |
| `STAGE5_RESULT_QUEUE` | 128; 1–4096; raw groundwork, no result consumers |
| `STAGE5_PAYLOAD_BYTES` | 8192; fixed protocol cap |
| `STAGE5_SCAN_INTERVAL`, `STAGE5_EXPIRE_INTERVAL`, `STAGE5_RETRY_BACKOFF` | 1s each; 1ms–1min |
| `STAGE5_PUBLISH_TIMEOUT` | 3s; 1ms–1min |
| `STAGE5_ACL_TIMEOUT`, `STAGE5_CONNECT_TIMEOUT` | 5s each; 1ms–1min; ACL readback gets the smaller enclosing connection/ACL budget |
| `STAGE5_SUBSCRIBE_TIMEOUT` | 3s; 1ms–1min |
| `STAGE5_RECONNECT_MAX_INTERVAL` | 30s; 1ms–1min; reused supervisor's bounded reconnect backoff |

Enabled Stage 5 also requires existing `MQTT_BROKER_URL`, `MQTT_USERNAME`,
`MQTT_PASSWORD`, `MQTT_TLS_CA_FILE`, `MQTT_INTERNAL_MANAGEMENT_URL`,
`MQTT_DYNSEC_MANAGER_USERNAME`, `MQTT_DYNSEC_MANAGER_PASSWORD`. These do not turn on
the independent telemetry/credential flags. Both URLs must be TLS, CA path clean
absolute, identities/secrets bounded/redacted. Management queue 8, in-flight 1,
payload 64 KiB; exact native responses remain bounded. Data cap/queue/worker/scan
and finite timeouts are validated centrally. Pool reservation includes publisher
workers plus one expiry connection and existing ingress/HTTP/readiness/credential
reservations; shared shutdown must cover configured DB+publish budgets.

Existing broker `message_size_limit=1048576`, `max_packet_size=2097152` are unchanged.
Native tests round-trip exactly 8192 application payload bytes at QoS 1 and reject
8193 in application transport; no packet-limit increase was necessary or made.
This is a transport-bound probe, not a throughput/capacity measurement.

## Explicit existing-store operator procedure / readback

1. Keep Stage 5 disabled. Verify intended broker/container/volume identity,
   protected store ownership/modes, CA/SAN and manager identity using the existing
   operator process. Arrange **exclusive administration** for this change window.
   Do not discover/mutate a live deployment through the test fixture.
2. Preserve an operator-controlled protected backup under existing policy; never
   print/copy hashes or secrets into logs. No bootstrap init, regeneration, new
   replacement store, password reset, volume deletion/reset or broad cleanup.
3. Use the callable procedure in `scripts/mqtt_backend_acl.py` with a trusted,
   authenticated, bounded native request function and the explicit intended data
   username, `exclusive_admin_confirmed=True`. That boolean records operator
   confirmation; it is **not** a distributed lock or broker authorization proof.
4. Preflight reads client/defaults and bounded paginated clients/groups (128/page,
   at most 16 pages per inventory) and refuses a shared backend role, nonempty
   client groups, extra roles, broad defaults or unexpected/partial ACL baseline.
   Inventory overflow is a supervised stop, not permission to skip ownership.
5. From exactly the old four-ACL policy, add **only** command `publishClientSend`
   `gateways/+/commands/#` and response `publishClientReceive`, `subscribeLiteral`,
   `unsubscribeLiteral` `gateways/+/responses/#`, all allow=true/priority=0.
   Existing telemetry/ACK permissions and manager role remain unchanged.
6. Read back the exact eight ACLs, client/defaults and role ownership again. An
   already-exact policy is read-only/idempotent success. A partial/error/ambiguous
   operation stops for inspection; there is no automatic role reset/rollback.
   DynSec has no compare-and-swap for this sequence: exclusivity matters.
7. Check persisted store/readback and fresh authentication/subscriptions using
   the existing operator tools before any rollout. Code/readiness never assumes
   a successful bootstrap is proof of current ACLs.

This procedure was executed **only** against the owned isolated existing-store
fixture: original four-ACL readback; native shared-role refusal/no ACL mutation;
four additions/final readback/idempotence; persisted store comparison preserving
native credentials/client identity/defaults/other roles; subsequent native
positive/negative data-plane controls and reconnect readback. No live store ran it.

## Historical implementation RED/GREEN and checks (before acceptance migration)

All evidence under `/tmp/opencode/task54/`. Commands from repo root unless `src`.
No inherited PASS is reattributed. Runtime stubs were used for executable missing
behavior RED, not compilation failures.

| Command / target | Actual outcome |
| --- | --- |
| `python3 -m unittest scripts/tests/test_task284_bootstrap.py` | Executed RED missing commands/responses ACLs, `red-bootstrap.log` |
| `src`: `go test ./internal/mqttcredential -run '^TestBackendACLReadOnlyExact$' -count=1` | Executed RED valid policy rejected by stub, `red-acl-readback.log`; GREEN with lifecycle test below |
| `src`: `go test ./internal/mqttingress -run '^TestControlConnectionPrerequisiteAndQoS$' -count=1` | Executed RED missing prerequisite calls/data connects without ACL verification, `red-lifecycle.log` |
| `src`: `go test ./internal/mqttcredential ./internal/mqttingress -run 'TestBackendACLReadOnlyExact|TestControlConnectionPrerequisiteAndQoS' -count=1` | GREEN, `green-acl-lifecycle.log` |
| `python3 scripts/tests/stage5_intent_fixture.py` against dispatch stub | Executed native DB RED missing durable pending discovery, `red-native-dispatch.log`; cleanup verified |
| Same fixture after repository implementation | GREEN, `green-native-dispatch-first.log` |
| `src`: `go test ./internal/mqttcontrol -count=1` | Executed RED missing publication/nil dependency rejection, `red-runtime.log`; then separate executable RED valid transport command rejected, `red-transport.log`; core GREEN in subsequent suites |
| `src`: `go test ./internal/mqttcontrol ./internal/config ./cmd/server -count=1` | PASS, `green-scoped-first.log` |
| `src`: `go test ./internal/config -run '^TestStage5TransportConfiguration$' -count=1` | Executed RED enabled config accepted missing data/ACL prerequisites, `red-config.log`; GREEN in subsequent suites |
| `python3 -m unittest scripts/tests/test_dynsec_bootstrap_cli.py` after new exact fixture | Executed RED old guard rejected valid eight-ACL policy, `red-bootstrap-guard.log`; final native bootstrap validates corrected guard |
| `CONTROL_MQTT_COVERAGE_FILE=/tmp/opencode/task54/native-mqtt-final-v2.cover python3 scripts/tests/stage5_mqtt_fixture.py` | **PASS**, real DB+Mosquitto control test, no skips; operator/readback/preservation/exact negative controls, real QoS1/retain/cap, offline intent/reconnect, PUBACK+mark failure/same bytes, deterministic no-SQL-lock-across-network assertion, early terminal conditional mark, missing/extra ACL reconnect denial, composed real HTTP/ingress/control readiness and shared shutdown; exact fixture cleanup. `native-mqtt-final-v2.log` |
| `CONTROL_TEST_COVERAGE_FILE=/tmp/opencode/task54/native-intent-final.cover python3 scripts/tests/stage5_intent_fixture.py` | **PASS 13 required top-level native tests, no skips**, including two new dispatch/expiry tests; fresh/upgrade/three refusal scenarios and owned cleanup. `native-intent-final.log` |
| `CONTROL_HTTP_COVERAGE_FILE=/tmp/opencode/task54/native-http-final.cover python3 scripts/tests/stage5_http_fixture.py` | **PASS 2 required native HTTP tests, no skips**, owned cleanup, `native-http-final.log` |
| `python3 scripts/tests/dynsec_bootstrap_cli.py --report /tmp/opencode/task54/bootstrap-native-report.json` | **PASS native exact eight-ACL bootstrap**, protected permissions, collision/no overwrite and cleanup; `native-bootstrap.log`, report |
| `sh scripts/test-stage2-mqtt-credentials.sh` | **FAIL native standalone gated TLS assertion**, current STOP, `stage2-credentials.log`; cleanup verified |
| `src`: `go test -json -race -count=1 -coverprofile=/tmp/opencode/task54/unit-race-final.cover ./...` | **PASS**, all 20 packages, 4686 passing events including package events, **98 opt-in native skip events**, empty stderr, zero failing events. Started before receiving regression failure notification. JSON/stderr/profile and `unit-race-counts.txt`. Skips are not native acceptance |
| `src`: `go vet ./...` | PASS before final small changes; not final gate acceptance |
| `src`: `golangci-lint run --timeout=5m` | Found own import grouping/unused test parameter; corrected; second run found one remaining import grouping. That import was corrected but **final lint not rerun due STOP**. `lint-first.log`, `lint-corrected.log` |

Native dispatch tests explicitly cover unchanged byte/ID retries, real DB mark
failure, early simulated succeeded, expiry-before-dispatch using real TTL, all
three active statuses, outbox stopping, unchanged state, released admission slot,
limit-one expiry, legacy sentinel refusal and concurrent admission/expiry/mark.
Result simulation is test-only SQL, not a result-ingestion implementation.

### Retained invalid attempts / fixture corrections

- Initial out-of-range source read (`config.go` offset 240) and a tool transport
  interruption applying an empty patch: no product changes/RED claim.
- `green-unit-pre-native.log`: unused native-test import caused compilation failure;
  not RED/PASS. Import became used by native cap probe.
- `native-mqtt-first.log`: own phase poll misused existing helper `expect=False`
  (means require command failure, not allow either outcome). Corrected poll; owned
  cleanup verified. Native timeout from failure to start broker is fixture failure,
  not product RED.
- `native-mqtt-second.log`: Docker reassigned the loopback ephemeral broker port
  after restart; host wait used stale port. Resolve port again from the same owned
  broker after every start/restart. Native transport had already progressed via its
  stable internal alias; no product-policy change. Cleanup verified.
- `native-mqtt-third.log`: native initial lifecycle PASS before later assertions.
- `native-mqtt-composed-first.log`: a nondeterministic lock assertion during retry
  could collide with a legitimate DB mark, falsely calling it a lock-across-network
  defect. Replaced that timing assertion with the stronger already-added exact
  blocked-publisher boundary check, acquiring BOTH Sensor and state locks before
  allowing the real MQTT invocation. No no-network/lock guarantee weakened.
- `native-mqtt-composed-second.log`: own log reader captured only Docker stdout,
  missing slog stderr and falsely rejecting confirmed shutdown. Use existing
  `container_logs` merger. Cleanup verified; not a diagnosed product shutdown bug.
- `native-mqtt-final.log`: native shared-role negative fixture assigned a role to
  its *own connected manager*, causing pinned DynSec session disconnect. Corrected
  test setup to use an owned temporary shadow client, refuse shared ownership,
  remove only that fixture-created assignment/client, then proceed. No original
  client credential or policy reset. Cleanup verified. Final v2 supersedes this
  invalid fixture attempt, not erased history.
- Python operator unit guards were added but **not executed yet**; do not call
  their mocked behavior native evidence. Native operator procedure did run above.

## Historical unfinished verification / resume work (superseded above)

No post-failure retry or further test/static run. Remaining mandatory checks are
**NOT RUN**, not silently waived:

- Resolve current Stage 2 native failure by separately authorized work, then rerun
  that lane and complete affected Stage 2 authorization/provisioning/admin, Stage 3
  persistence/uplink-all and Stage 4 API-all. The original sequential driver's
  later commands are recorded but did not execute.
- Final Go formatting/imports/vet/lint/build and Python guards (including new
  operator guards and affected fixture/Compose/bootstrap guards), require-run
  parsing, final diff/security review; rerun full race/native as needed after fixes.
- Final coverage union/detailed statistics not computed; available profiles are
  not a claim of a particular coverage threshold.
- No remote CI, live rollout, ARM/board/real sensor, Stage 5 E2E, separate new CI
  lane or full client-document rewrite. Stage 3/4 runner image builds did not run
  because the regression sequence stopped before those lanes. Native fixture
  binaries use the inherited CGO-disabled/container pattern, not race-native proof.

No task acceptance while these required checks remain unresolved. Preserve all
failed evidence. `.env.example` still has an inherited intent-only introductory
comment; adjust scoped wording during the final documentation review after STOP
is resolved rather than treating the current handoff as release documentation.

## Task 5.5 / 5.6 boundary

No Task 5.5/5.6 assignment is authorized; stop after 5.4. This task added no Gateway command handling,
SQLite migration, dynamic sampler/runtime configuration, durable device dedup or
result publishing. Existing Gateway scheduler shutdown correction is untouched.
Keep command subscription/restoration and application/readback/dedup in 5.5.

Task 5.6 may consume the bounded raw results transport but still needs its own
tests-first validation/transaction/worker implementation. Reuse resource/state
lock order and expiry helper; preserve terminal/measurement/desired invariants.
Published/PUBACK is only broker receipt, never acceptance or configuration
confirmation. Do not claim Stage 5 complete or automatically start either task.
