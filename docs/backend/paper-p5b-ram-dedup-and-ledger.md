# P5b — RAM dedup và ledger Tier-2: nghiệm thu Local MVP (26/10/2026)

## Kết luận cuối — P5b Local MVP hoàn tất

P5b hoàn tất ở phạm vi **Local MVP trên fixture riêng**: schema Tier-2 migration 24, RAM dedup Tier-1, cache functional/cold fallback, bốn profile so sánh, P3 configuration-preservation regression, Go/schema/Python gates và các regression Stage 3/4 được tác động đã có bằng chứng PASS. Đây không phải production certification, capacity/soak hoặc bằng chứng mọi interleaving. Các handoff theo lát cắt bên dưới được giữ làm lịch sử; những câu “chưa hoàn tất” trong đó chỉ đúng tại thời điểm handoff, không phải trạng thái cuối.

### Baseline, source và schema

- Revision source: `f098569395ec60a9051f4554040dd5023d74886e`, branch `feat/paper-requirement`, **dirty**. P5b là source/test changes chưa commit; không suy diễn thành clean tree hay remote CI.
- Native profile dùng schema 24. Migration `000024_telemetry_ledger_timestamp.up.sql` thêm timestamp gốc và unique index Tier-2 `(gateway_id, sensor_id, message_id, timestamp)`. Backfill chỉ từ telemetry `sample_index=0` có đủ provenance khớp; timestamp không biết giữ NULL; mismatch/ambiguity bị từ chối. Guard ID cũ `(gateway_id,message_id)`, hash/provenance và stream-range overlap được giữ. Fresh/upgrade, backfill, missing legacy NULL, mismatch refusal, reapply/idempotency, ID/range guards đã PASS trên native fixture. Existing schema-24 artifacts đã được đọc; migration lifecycle không chạy lại trong final documentation slice.
- Tier-1 key là `(gateway_id,sensor_id,boot_id,first_sequence)`. Entry chỉ giữ metadata bất biến; `current`/`previous` là hai `sync.Map`, TTL cố định 600 giây từ confirmed commit và lookup kiểm tra từng entry. TTL không gia hạn bởi retry/hit/rotation. Tổng caps mặc định 100,000 entries / 67,108,864 logical bytes. Chỉ new confirmed commit warm cache; miss, expired, full, conflict và cold restart dùng DB. Không giữ payload/samples, không pending coalescing hay global-ID cache; database remains durable source.
- Source config đọc process environment. `.env.example` và Compose map `MQTT_RAM_DEDUP_ENABLED=false`, `MQTT_RAM_DEDUP_MAX_ENTRIES=100000`, `MQTT_RAM_DEDUP_MAX_BYTES=67108864`; TTL không phải env option. Flag độc lập với `MQTT_MICROBATCH_ENABLED`, `MQTT_SHAPER_ENABLED` và `STAGE5_ENABLED`; deployment defaults cả bốn tiếp tục `false`.

### Native correctness, regression và gates

- Functional cache artifact `/var/tmp/opencode/paper-p5b/native-cache-metric-fix-20261026T214831Z-1847022/`: `run.json` ghi schema 24, `p5b_cache_functional_accepted=true`, cold status PASS và exact cleanup 17 containers / 1 network / 7 volumes. Có exact-byte warm replay với real application ACK; bốn conflict đúng guard và không ACK/mutate DB; overlap setup hai mẫu được ghi riêng là fixture functional, không benchmark; controlled backend replacement cho cache rỗng, cold DB duplicate, real ACK và dữ liệu/version bất biến. Cold duplicate có DB transaction begin +1 và confirmed SQL transaction commit +1, nhưng new rows/COPY/ingress new commits = 0. Đây là phân biệt transaction commit với new data commit.
- P3 regression artifact `/var/tmp/opencode/paper-p5b/p3-control-cache-final-20261026T231200Z/`: actual P3 command/result old/new configuration preservation PASS với cache + microbatch bật; metrics-off replacement cũng đọc lại cache flag/caps và command/result ACK path PASS; exact cleanup 17/1/7.
- Four comparable cache-off/on workloads đều `PAPER_P5B_PROFILE_WORKLOAD_ACCEPTED`, schema 24, exact cleanup 16 containers / 1 network / 7 volumes mỗi run. Source profile giữ các cấu hình tương đương, chỉ RAM cache flag khác; mỗi arm có 24 unique real Gateway messages/ACKs, rồi 24 distinct manual exact-byte duplicate replays/ACKs. Bảng tóm tắt bên dưới; chi tiết ACK, workload, resource samples, artifacts và historical qualification được giữ trong các phần sau.

| Samples/message | Replay window off → on | Off DB duplicate tx → on DB duplicate tx | Off hits/repository calls avoided → on | Off/on application ACKs | Off/on persisted DB delta |
| ---: | ---: | ---: | ---: | ---: | --- |
| 100 | 29.524 → 5.247 s | 24 → 0 | 0/0 → 24/24 | 24/24 → 24/24 | Không đổi / không đổi |
| 1 | 29.426 → 5.306 s | 24 → 0 | 0/0 → 24/24 | 24/24 → 24/24 | Không đổi / không đổi |

Mỗi profile tạo 24 unique message; 100-sample profiles ghi 2,400 raw rows, 1-sample profiles ghi 24. Ở on, replay 24/24 là RAM hit trước repository; ở off, 24/24 đi DB duplicate path. Replay không tạo COPY/new rows và không đổi ledger/raw/temporal/state/version. Số liệu “DB transactions/repository calls” là metrics đường xử lý thật, **không phải** SQL statement count. Manual replay ACK timing bao gồm CLI và observer, không phải broker/Gateway latency. ACK/new-message Gateway phase là số đo riêng ở artifact; workload hữu hạn không chứng minh cache tối ưu latency của new messages. Cache logical bytes là ước lượng, không phải RSS; resource snapshots thưa, không đo peak.

- Final Go review `/var/tmp/opencode/paper-p5b/p5b-9a-go-review/`: TTL/caps/concurrency/pending/ACK/cancel/lifecycle tests và race 4 affected packages PASS; gofmt clean, full `go vet ./...`, golangci-lint 0 issues và `go build ./...` PASS.
- Schema/Python handoff `/var/tmp/opencode/paper-p5b/p5b-9b/`: 476 Python tests PASS, compile/syntax/diff checks PASS. Native Stage 3 MQTT ingress, Gateway simulator, uplink normal và Stage 4 vertical/contract/queries PASS sau schema-24 writer fixture correction. Stage 3 600-batch workload NOT RUN. P3 native control run nêu trên PASS.
- Historical failures remain failures: earlier cold assertion/fixture failures, flawed/stale-ACK profile measurements and other failed attempts are not reclassified by corrected runs. Full/remote CI, HIL/AM5728, physical sensor, production deployment/rollout, broad capacity/soak and peak RSS are NOT RUN/out of scope.

### Follow-up sau go-reviewer — ba sửa đổi hẹp

- Runner Stage 2 hiện truyền terminal 24 vào các verifier 21–23. Verifier 23 nhận terminal tường minh, vẫn mặc định 23 cho fixture lịch sử và từ chối terminal không khớp; không sửa migration đã áp dụng.
- Admin metrics phân biệt cache tắt (`disabled`) với cache được cấu hình bật nhưng thiếu nguồn (`unavailable`). CSV exporter không nâng trạng thái `unavailable` tường minh thành `available` chỉ vì có dictionary snapshot.
- Đối chiếu artifact off/100: replay có 24 transaction/24 delivery; 20 transaction/24 member thuộc pha message mới, không phải replay. Đã sửa diễn giải, không thay dữ liệu đo hay phân loại lại FAIL lịch sử.
- Regression đã tái hiện RED trước sửa. Kiểm tra nhanh sau sửa: race toàn package `cmd/server`, vet/lint package (0 issues), 232 Python tests liên quan, shell syntax và `git diff --check` PASS. Đối chiếu báo cáo/artifact PASS. Log: `/var/tmp/opencode/paper-p5b/review-fixes-1x0m9vjh/`.
- Re-review độc lập đã xác nhận cả ba finding ban đầu **RESOLVED**: Stage 2 fresh/upgrade và verifier default/requested/future terminal được kiểm trên PostgreSQL thật; runtime/CSV và đối chiếu phase được tái kiểm; P5b functional, control và cả bốn profile native chạy lại PASS với exact cleanup.
- Đã sửa tiếp các phát hiện mở rộng theo kế hoạch:
  1. Chuyển base fixture của các bài kiểm tra Stage 2, DynSec native và certificate sang `/var/tmp/opencode` vật lý, giữ nguyên guard chống symlink; 24 lỗi Python discovery trước đó được khắc phục hoàn toàn (1.066 tests discovery PASS 100%).
  2. Bọc tmpfs cho `/mosquitto/data` và `/mosquitto/log` trên backend replacement của standalone credential runner và broker/setup của DynSec probe; thực thi native credential runner và native DynSec runner (`dynsec_native_ci.py: SUCCESS`) đều xác nhận exact cleanup không rò rỉ anonymous volume (duy trì ổn định 198 volumes).
  3. Chuẩn hóa `scripts/test-stage2-admin.sh` với UUID container/volume riêng, mount volume tường minh, fail-closed và kiểm tra absence thực tế; `test_stage2_admin_cleanup` PASS và native runner PASS.
  4. Chuẩn hóa Stage 3 uplink/workload runner để quản lý và gỡ bỏ image tag riêng (`io.iot.stage3-image-tag`), không xóa ép shared image ID; native uplink `normal` PASS và suite image cleanup verified.
  5. Thêm chú thích suppression SA1012 tại `history_service_test.go:1018`, giữ nguyên negative test; `staticcheck ./...` và `golangci-lint run` đều 0 issues.
- Toàn bộ Go race tests (21 packages PASS), full Python discovery (1.066 tests PASS), staticcheck, vet, lint, build và các native lane liên quan đã được kiểm tra xanh. Sáu volume cũ từ phiên trước được bảo toàn nguyên trạng theo đúng chính sách an toàn.
- Báo cáo re-review trước đó nằm tại `/var/tmp/opencode/paper-p5b/re-review-full-NBDcSb/report.md`. Remote CI, hardware/HIL và rollout không chạy.

### 3 phát hiện từ đợt kiểm thử độc lập toàn diện cần lưu ý cho các giai đoạn sau

Đợt rà soát và kiểm thử tự động toàn diện độc lập của `@go-reviewer` (ghi nhận tại `/var/tmp/opencode/paper-p5b/independent-full-ofXNCG/report.md`) đã thực thi 26 lượt native invocations (25 PASS, 1 FAIL) và toàn bộ Go/Python suites, xác nhận P5b Local MVP đạt yêu cầu, đồng thời chỉ ra 3 phát hiện cấp MEDIUM thuộc phạm vi fixture/test suite ngoài P5b cần xử lý ở các giai đoạn tiếp theo (Stage 5 / P6 hoặc kiểm thử tích hợp):

1. **[MEDIUM] Xử lý ngoại lệ trong vòng lặp dọn dẹp `task266_provision.py` có thể bỏ sót tài nguyên (`scripts/tests/task266_provision.py:319–329`)**:
   - *Nguyên nhân*: Khối `finally` chỉ bắt `RuntimeError` khi gọi `cleanup_container` hoặc `cleanup_backend_if_present`. Nếu `subprocess.TimeoutExpired` hoặc `OSError` phát sinh từ Docker subprocess, vòng lặp ngắt sớm, bỏ qua dọn dẹp CSDL/proxy và `spike.cleanup()`.
   - *Ảnh hưởng*: Rủi ro rò rỉ container CSDL và proxy nếu daemon gặp lỗi OS/timeout trong lúc xóa backend container.
   - *Hướng xử lý*: Mở rộng bắt `(RuntimeError, subprocess.TimeoutExpired, OSError)` hoặc bọc an toàn trong helper để tiếp tục dọn dẹp các tài nguyên còn lại trước khi fail-closed.

2. **[MEDIUM] Stage 5 intent fixture chưa tương thích với writer schema 24 (`scripts/tests/stage5_intent_fixture.py:133–138`)**:
   - *Nguyên nhân*: Fixture chỉ migrate đến phiên bản 23 nhưng chạy code Go telemetry writer hiện hành (đòi hỏi schema 24 có cột `processed_messages.timestamp`), gây lỗi native SQL `column "timestamp" of relation "processed_messages" does not exist`.
   - *Ảnh hưởng*: `stage5_intent_fixture.py` thất bại ở kịch bản fresh CSDL; 4 kịch bản intent phía sau bị hoãn không chạy được.
   - *Hướng xử lý*: Khi mở lại Stage 5/P6, cập nhật fixture áp dụng schema 24 (kèm điều kiện post-Auth) trước khi gọi current telemetry writer, phân định rõ ràng các bài kiểm tra schema 23 lịch sử.

3. **[MEDIUM] Flaky assertion độ trễ timeline trong `mqttcontrol` (`src/internal/mqttcontrol/metrics_test.go:54–55`)**:
   - *Nguyên nhân*: Assertion `m.QueueWait.Snapshot().Count` quan sát 3 thay vì 2 khi chạy full test suite với 20+ packages cạnh tranh CPU cao (chạy đơn lẻ 20/20 PASS) do race condition giữa thời điểm worker dequeue và publish observation.
   - *Ảnh hưởng*: Gây flaky test ngắt quãng khi chạy full test suite song song chịu tải lớn, dù logic nghiệp vụ của control dispatch không sai.
   - *Hướng xử lý*: Đồng bộ hóa kiểm thử bằng channel barrier / wait group rõ ràng giữa dequeue và publish observation thay vì so sánh cứng số nguyên khi pipeline chưa hoàn toàn ổn định.

### Runner navigation và vận hành an toàn

Từ repository root, chạy sequentially với **mỗi run directory mới, chưa tồn tại**; log ở ngoài thư mục run. Không dùng deployment `.env`/credentials cho disposable fixtures. Go config đọc process environment; Compose map các biến từ `.env` theo `.env.example`.

```sh
set -eu
base=/var/tmp/opencode/paper-p5b/my-run-$(date +%Y%m%d-%H%M%S)-$$
mkdir -p "$base"
profile() {
  mode=$1 dataset=$2 name=$3
  run="$base/$name"; log="$base/$name.log"
  test ! -e "$run"
  sh scripts/test-paper-p5b-profile.sh "$mode" "$dataset" "$run" >"$log" 2>&1
}
profile off 100 off-100
profile on 100 on-100
profile off 1 off-1
profile on 1 on-1
```

Cache functional smoke (warm exact replay, four conflicts, separately labeled two-sample overlap setup, controlled restart/cold DB duplicate) and P3 control-preservation smoke use the existing native runner:

```sh
run="$base/cache-functional"; test ! -e "$run"
python3 scripts/tests/native_compose_smoke.py --prepare "$run" --paper-p5a --mode on --dataset 1 --paper-p5b-cache-smoke >"$base/cache-functional.log" 2>&1
run="$base/p3-cache-control"; test ! -e "$run"
python3 scripts/tests/native_compose_smoke.py --prepare "$run" --paper-p3 --p5a-control-regression --p5b-control-regression >"$base/p3-cache-control.log" 2>&1
```

These commands describe safe new fixture invocations, not new evidence from this documentation change. Keep `STAGE5_ENABLED=false`, `MQTT_SHAPER_ENABLED=false`, `MQTT_MICROBATCH_ENABLED=false`, `MQTT_RAM_DEDUP_ENABLED=false` in deployment. Cache-off is the DB fallback/rollback path; it does not undo additive migration 24. Do not casually remove/roll back migration 24: before any live schema upgrade require target-specific preflight, backup/restore plan and a maintenance window for its table locks, and preserve the existing ID/range guards. RAM proof is process-local: backend restart clears it; external DB restore/edit/maintenance must be fenced from active backend cache by controlled restart/cache cold start. One active backend only is in scope.

See the [single shared limitations/backlog](../dynsec/limitations-and-backlog.md). P5a+P5b close the paper P5 Local MVP only. P6/Stage 6, full CI, board/HIL, physical sensor and live rollout remain not done; stop here unless explicitly asked to continue.

---

## Historical implementation handoffs

The following chronological records are retained to preserve source/artifact qualification and failure history; their “pending/not complete” statuses refer to the individual checkpoint date, not the final status above.

## Handoff P5b.5a — single-message hooks (26/10/2026)

Single worker path đã được nối vào cache dùng chung theo `Ingress`, sau full
validation; cache-hit ACK được build mới dưới outcome `DuplicateCommitted` và
không gọi repository. Chỉ kết quả `Committed` đã kiểm identity/sample count mới
được publish proof, trước ACK, nên ACK lỗi không làm mất proof; DB duplicate,
persist error/unconfirmed và mismatch không warm. Caps/full/expiry đi DB path.
Tests ban đầu: warm/retry (RED→GREEN), ACK failure proof và DB duplicate không
warm; chưa bao phủ microbatch/fallback/lifecycle boundaries. Các hook group,
fallback và cùng-clock/lifecycle join được bổ sung ở handoff 5b.5b bên dưới.
Native fixtures NOT RUN; không có server wiring/exporter hay native acceptance.

## Handoff P5b.5b — group/fallback hooks và đại diện boundary (26/10/2026)

**Đang triển khai, chưa đóng P5b.5.** Lookup được thêm sau validation trong
microbatch collection, trước standalone/grouping, nên hit tạo ACK riêng và không
chiếm batch slot; shaper admission và payload reservation vẫn như cũ. Group commit
quan sát một mốc ngay khi `PersistBatch` trả về, kiểm tra toàn bộ index/identity/
outcome trước publication, rồi chỉ kết quả `Committed` mới warm; fallback standalone
cũng publish confirmed proof. ACK không làm điều kiện publication. Rotation task
được drain-joined sau khi worker shutdown, ngoài worker WaitGroup.

Tests được thêm cho mixed cached/fresh group (hit không vào group; fresh member
warm separately), successful standalone fallback, mismatched group identity không
warm/ACK, confirmed group publication sau khi repository hủy processing context
(metrics-off vẫn giữ counters/proof và release bytes), và Drain join rotation.
Focused unit/race/vet/lint/diff lệnh và kết quả được ghi cuối report. Còn cần thêm
representative integration-path checks cho pending-before-commit, full/expired
fallback, group rollback/commit-unconfirmed/no-warm, và startup-failure cleanup;
native fixtures/server wiring/exporter/P5b acceptance vẫn chưa chạy/hoàn tất.

### Verification P5b.5b (26/10/2026)

- Focused: `go test ./internal/mqttingress -run
  'Test(MicrobatchCacheHit|MicrobatchConfirmedProof|MicrobatchStandalone|MicrobatchMismatched|SingleCommitted|SingleCommitProof|DatabaseDuplicate|DedupCache|Microbatch.*|Drain)' -count=1` PASS.
- Gates: `go test -race ./internal/mqttingress -count=1` PASS; `go vet
  ./internal/mqttingress` PASS; `golangci-lint run --timeout=5m
  ./internal/mqttingress` PASS (0 issues); `git diff --check` PASS.
- Logs: `/var/tmp/opencode/paper-p5b/p5b-5b-focused.log` and
  `/var/tmp/opencode/paper-p5b/p5b-5b-gates.log`. First focused attempt failed on
  test setup (cache key collision in seeded fixture and assertion expected ACK
  suppression after Drain, which cancels only after workers finish); corrected
  fixture to distinct sensor and simulated cancellation in confirmed repository
  return. This failure is retained in the gates log? The initial output was not
  redirected; it is recorded here and not counted as PASS evidence.

## Handoff P5b.4 — generation và lifecycle cache

P5b.4 bổ sung `dedupCache.RunRotation(ctx)`: caller sở hữu context/vòng đời; một
timer trong một invocation xoay generation mỗi 10 phút, và trả về khi context bị
hủy. API không tự tạo goroutine; cache tắt hoặc context đã hủy trước khi gọi sẽ
không tạo timer chạy nền. `Lookup` luôn kiểm tra expiry chính xác trên từng proof,
kể cả khi timer bị trễ; lookup ở `previous` không promote/gia hạn TTL. Cache chỉ
chứa proof bất biến được publish caller-confirmed; không có API pending.

Accounting per-generation cho phép xoay/drop generation và `Snapshot` trong O(1)
thay vì quét map dưới mutex. Drop giải phóng logical entry/byte budget, không đảm
bảo map backing storage hay RSS được runtime trả ngay. Cả hai generation vẫn
chịu tổng caps; miss/full/conflict tiếp tục dành cho DB fallback ở integration
caller. P5b.5a/P5b.6a còn chịu trách nhiệm nối cache dùng chung và lifecycle vào
ingress/server.

Kiểm tra đã chạy từ `src/`: RED ban đầu FAIL khi compile vì API
`RunRotation` chưa tồn tại; sau implementation, `go test ./internal/mqttingress
-run 'TestDedupCache...' -count=1` và `go test -race ./internal/mqttingress
-count=1` PASS; `go vet ./internal/mqttingress` PASS; `golangci-lint run
--timeout=5m ./internal/mqttingress` PASS (0 issues); `git diff --check` PASS.
P5b và các tiêu chí tổng thể chưa hoàn tất.

## Handoff P5b.5c — representative proof-boundary regressions (26/10/2026)

Chỉ bổ sung kiểm thử cho các khoảng trống P5b.5; runtime không đổi. GREEN ngay
trên implementation hiện tại (không có RED runtime reproduction): hai delivery
cùng key trong lúc Persist đầu bị chặn đều vào DB, không có ACK/proof trước khi
release; sau confirmed commit cả hai ACK, một proof canonical. Controlled ingress
clock đẩy replay tới đúng TTL 10 phút: replay quay DB, DB duplicate không gia hạn
proof và new/sample counters giữ nguyên. Cache một-entry full vẫn xác nhận/ACK
message mới; canonical hit vẫn được ACK khi full; key chưa cache quay DB; entries
và logical bytes không vượt caps. Group per-input error kèm provisional
`Committed` không publish member đó; successful partial member vẫn ACK/proof.
Invalid result index và `CommitOutcomeUnconfirmed` triệt tiêu mọi provisional
success/ACK.

BatchRepository contract ở package hiện hữu là per-input `BatchPersistResult`
(không có error return riêng cấp top-level); do đó kiểm tra error override áp
dụng tại trường `Err` của member, không giả lập một top-level error contract mới.
Các test group được đồng bộ bằng collection timer/channel barriers; không dựa vào
sleep để hình thành group. Đây không phải chứng minh exhaustive về group/fault
interleavings. Existing tests tiếp tục cover metrics-off và canceled-after-confirmed
proof.

Verification mới từ `src/`: focused `go test ./internal/mqttingress -run
'Test(PendingSameKey|ExpiredProofUsesDatabase|FullCacheDoesNotBlock|MicrobatchGroupError|MicrobatchInvalidIndex|Microbatch.*|DedupCache.*|Single.*|DatabaseDuplicate)' -count=1` PASS;
`go test -race ./internal/mqttingress -count=1` PASS; `go vet
./internal/mqttingress` PASS; `golangci-lint run --timeout=5m
./internal/mqttingress` PASS (0 issues); gofmt clean; `git diff --check` PASS.
Log final mới: `/var/tmp/opencode/paper-p5b/p5b-5c-final.log`; lượt gates trước
đợt cuối cũng lưu tại `/var/tmp/opencode/paper-p5b/p5b-5c-gates-rerun.log`. Initial race
attempt caught a nondeterministic test setup (Drain flushed after first admission,
so a two-result fake indexed a one-member group); no runtime bug was implicated.
Fixed only the test using the existing controlled collection timer, then reran all
gates. Failure log retained at `/var/tmp/opencode/paper-p5b/p5b-5c-gates.log`.
Native/server/schema/exporter/full P5b acceptance remains NOT RUN; P5b is not
complete and no overall criteria are ticked.

## Handoff P5b.6b — fixed P1 exporter và fixture metadata (26/10/2026)

P1 `metrics.csv` hiện whitelist-export `processMetrics.ram_dedup` theo các snapshot
field thực tế: 14 counter names từ server (`lookup_*_total`, `publication_*_total`,
`capacity_bypass_total`, `rotations_total`, `database_calls_avoided_total`), 4 gauges
(`entries`, `logical_bytes`, hai generation entry counts), và `enabled`, `ttl_seconds`,
`max_entries`, `max_bytes`. Counters dùng unit `operations`, logical/cap bytes dùng
`bytes`, TTL `seconds`, entry counts `entries`; các số nguyên giữ nguyên integer.
CSV columns/header cũ không đổi, không có label động. Counter delta chỉ xuất khi
snapshot cùng process tăng đơn điệu. Snapshot disabled vẫn xuất config có mặt
(availability `available`) còn operation/gauge không đo được được đánh dấu
`disabled`; snapshot thiếu/unavailable để value trống, không thay bằng 0.
`database_calls_avoided_total` phản ánh ingress cache hit đã bỏ repository call,
không phải suy đoán số SQL statements tiết kiệm. Unknown JSON fields không được xuất.

P5a fixture profile hiện giữ các cờ cache độc lập với microbatch/shaper và explicit
`MQTT_RAM_DEDUP_ENABLED=false`, caps mặc định 100000 entries / 67108864 bytes;
TTL metadata 600 giây. `run.json` giữ artifact `schema_version` riêng, đồng thời ghi
`database_schema_version: 24` và `ram_cache` effective config (`configured_not_run`),
không biến các P5a schema-23 timing lịch sử thành cache-off baseline mới. Không
backfill hoặc sửa artifacts cũ. Focused exporter/metadata tests RED trước patch
(field mapping thiếu và database schema metadata thiếu); kết quả GREEN cùng guards,
syntax/compile và diff checks sẽ được ghi trong handoff response. Không chạy stack,
native fixture hay workload. Tiếp theo: native helper có thể thêm P5b-only opt-in
profiles/cache phases, validating effective Compose env, so sánh cold/warm/replay
chỉ trên cùng schema 24; acceptance và P5b completion vẫn chưa đạt.

Kiểm tra slice 6b: hai exporter/metadata tests RED ban đầu trên source thiếu mapping
và schema metadata; sau patch, focused 3 tests PASS. Guards ảnh hưởng P1/P5a/P4/native
compose chạy với `PYTHONPATH=scripts/tests`: 191 tests PASS; lần gọi đầu thiếu
PYTHONPATH nên hai test modules không import được (không phải test assertion fail).
`py_compile` cho ba Python files, `bash -n` cho P1/P4/P5a wrapper scripts và
`git diff --check` PASS. Không chạy Docker, native E2E, stack hay benchmark. Log:
`/var/tmp/opencode/paper-p5b/p5b-6b-red-correct.log`,
`p5b-6b-focused.log`, `p5b-6b-python-guards-rerun.log`.

## Handoff P5b.6a — server composition và fixed cache snapshot (26/10/2026)

Server maps the already validated `MQTT_RAM_DEDUP_*` config into one cache owned
by each `Ingress`; the existing joined rotation/drain lifecycle is reused. Cache
is independent of microbatch and `METRICS_ENABLED`; disabled cache short-circuits
metadata construction on lookup, while validation/hash/TTL/cap logic is unchanged
when metrics are off. Cache snapshots use O(1) generation accounting and do not
scan keys or retain payloads. `logical_bytes` is an estimate, not RSS. A cache hit
is a validated duplicate proof that returns before the repository call, so
`database_calls_avoided_total` equals actual cache-hit repository calls avoided;
it does not estimate SQL statements/transactions avoided.

Fixed `processMetrics.ram_dedup` snapshot fields (all counters/gauges are int64;
no labels): `lookup_hit_total`, `lookup_miss_total`, `lookup_expired_total`,
`lookup_metadata_mismatch_total`, `lookup_disabled_bypass_total`,
`publication_stored_total`, `publication_existing_total`,
`publication_metadata_mismatch_total`, `publication_full_total`,
`publication_invalid_skip_total`, `publication_disabled_bypass_total`,
`capacity_bypass_total`, `rotations_total`, `database_calls_avoided_total`;
gauges `entries`, `logical_bytes`, `current_generation_entries`,
`previous_generation_entries`; configuration `enabled`, `ttl_seconds`,
`max_entries`, `max_bytes`. Disabled ingress reports availability `disabled`;
enabled configuration without a composed snapshot reports `unavailable`; cache
off on a composed ingress has a snapshot with `enabled=false` and availability
`disabled`. Lookup disabled bypass remains zero on the normal ingress path because
metadata building is skipped; publication disabled bypass counts confirmed new
proofs intentionally not cached. Existing `DuplicateCommitted`, `Committed`,
`SamplesNew` and persistence transaction/COPY counters keep their separate
boundaries.

Verification for this slice from `src/`: pre-wiring tests were run RED and failed
on missing cache snapshot/options fields (`/var/tmp/opencode/paper-p5b/p5b-6a-red.log`);
focused server/config/ingress tests PASS (`p5b-6a-focused.log`); race tests for
`./cmd/server ./internal/config ./internal/mqttingress` PASS (`p5b-6a-race.log`);
vet PASS (`p5b-6a-vet.log`); golangci-lint PASS, 0 issues (`p5b-6a-lint.log`);
gofmt clean and `git diff --check` PASS. Native cache E2E, P1 CSV/run.json exporter
mapping (P5b.6b), and overall P5b acceptance remain NOT RUN; no final criteria are
ticked and P5b is not complete.

## Handoff P5b.7a — opt-in fixture profile contract (26/10/2026)

Added `--paper-p5b-cache-smoke` as a selector on the existing P5a fixture only;
it requires `--paper-p5a --mode on --dataset 1`, metrics enabled, and excludes
the separate shutdown smoke. It reuses the same P2 Auth/RS256/HTTPS, MQTT
TLS/DynSec, schema-24 fixture, 2 CPU/4 GiB backend quota, Gateway workload and
cleanup registry. Only `MQTT_RAM_DEDUP_ENABLED` differs from the ordinary P5a
on/1 profile; effective caps remain 100000 entries / 67108864 bytes, TTL 600s,
microbatch and shaper settings are preserved and represented in `run.json`.

**This is selector/profile plumbing, not a native acceptance implementation.**
The currently reused P5a replay is a manual broker publish and observes generic
duplicate/ACK counters; it does not yet prove cache hit/repository-call avoidance,
application ACK identity, four representative DB-guard conflicts, cold replay
after backend replacement, or SQLite outbox state for Gateway traffic. Therefore
do not run this selector as the P5b correctness smoke or infer any of those
assertions from its output. No native stack was executed here. The next bounded
implementation must add the dedicated warm/replay/conflict/cold phases using the
existing helpers and protected lifecycle, then execute separately after approval.

Focused guards: `PYTHONPATH=scripts/tests python3 -m unittest
scripts.tests.test_paper_p3_support scripts.tests.test_paper_p5a_support
scripts.tests.test_native_compose_smoke` — 180 tests PASS; Python compile, P5a
wrapper shell syntax and `git diff --check` PASS. Native execution NOT RUN.

## Handoff P5b.7b — warm exact-replay assertions only (26/10/2026)

Implemented the warm slice in `--paper-p5b-cache-smoke`: after both finite Gateway
writers have completed and their outboxes drained, select an actual captured
Gateway delivery, subscribe under that Gateway's TLS/DynSec principal to its
application ACK before replay, and republish the private saved bytes unchanged.
The checker requires same-process RAM hit and database-call-avoided counter
increments, `DuplicateCommitted`, matching five-field application ACK identity,
unchanged ledger/raw/temporal/state-version fingerprint and counts, and unchanged
transaction/COPY/new-row counters. Safe `warm_replay` diagnostics are written to
`run.json` before the assertion; manual replay outbox readback remains explicitly
not applicable. The cache-smoke acceptance marker now requires this phase.

This slice does **not** establish P5b completion: cold restart/DB fallback and the
conflict matrix remain pending. No Docker/native fixture, Go build, schema/runtime
change, or deployment-default change was made; native execution is NOT RUN.
Focused guards actually run: `PYTHONPATH=scripts/tests python3 -m unittest
test_paper_p5a_support test_native_compose_smoke` (174 tests PASS), `py_compile`
for the changed Python files, `sh -n scripts/test-paper-p5a.sh`, and
`git diff --check` PASS. Logs: `/var/tmp/opencode/paper-p5b/warm-replay-slice/`.

## Handoff P5b.7c — representative conflict assertions (26/10/2026)

The opt-in `--paper-p5b-cache-smoke` fixture now prepares a second Sensor under
each existing Gateway using its normal owner identity, then derives four finite
payloads from an actual captured one-sample Gateway delivery. Mutations cover
same ID/changed sample at the same RAM key, same ID moved to that Gateway's
second Sensor plus a different timestamp/key, same RAM key with a fresh UUID,
and a different first sequence overlapping the actual committed sequence. The
last case is a valid two-sample payload starting one sequence before the known
one-sample committed range; it preserves source envelope fields and uses a past
timestamp. Payloads pass the existing telemetry probe shape validator, with its
sample-count argument generalized for this bounded overlap fixture.

Each serial case subscribes to its exact application-ACK topic before publishing
over the existing TLS/DynSec principal. Same-process metrics require the
expected metadata-mismatch or miss counter, one `PersistFailures` DB-path
classification, no cache hit/repository-call avoidance, no commit/duplicate/new
sample/ACK/validation/rate-limit delta, and exactly one received delivery. DB
ledger/raw/temporal/state fingerprint, counts and reported version must remain
unchanged. Bounded ACK observation supplements those positive rejection
assertions; it is not used alone. Safe per-case ID/hash/count/window diagnostics
are written before assertions. The cache functional marker requires both warm
exact replay and all four conflict cases; cold replay remains explicitly pending.

Focused RED→GREEN: new payload/evidence tests were first run against absent
helpers and failed (`/var/tmp/opencode/paper-p5b/conflict-slice/focused.log`),
then the targeted rerun passed (`focused-rerun.log`). These are helper/profile
assertions, not native DB/cache acceptance. Docker/native fixture execution,
native compose, Go build, cold restart, P3/control, benchmark, and full
migration/cache E2E remain NOT RUN. P5b is not complete.

Final verification for this slice: `PYTHONPATH=scripts/tests python3 -m unittest
test_paper_p5a_support test_native_compose_smoke test_ci_go_test_guard
test_ci_feature_jobs` — 249 tests PASS; `py_compile` for the five changed Python
files, `sh -n scripts/test-paper-p5a.sh`, and `git diff --check` PASS. Log:
`/var/tmp/opencode/paper-p5b/conflict-slice/python-suites-final.log`.

## Handoff P5b.7d / P5b.8 step 5 — controlled cold-cache restart/replay (26/10/2026)

`--paper-p5b-cache-smoke` now adds cold replay after the existing warm replay and
four conflict cases. It reuses the fixture's owned-container registry: gracefully
stop the active backend (allowing the Unix auth listener to unlink its socket),
remove that exact owned container, register a uniquely named replacement, and
start only backend with the same explicit P5a on/1 + RAM-cache profile. It keeps
the existing DB/schema 24, Mosquitto, Gateway data, cache/microbatch/shaper/worker,
quota and security settings. Startup/health is bounded, expected effective env is
read back, protected diagnostics are used on failure, then Nginx is reloaded for
the replacement IP. There is no second active backend or test-only cache hook.

After restart the fixture records a new-process metrics baseline and DB snapshot,
checks enabled cache with zero entries, subscribes using the real Gateway TLS/
DynSec ACK identity, and republishes the exact original captured bytes/topic. The
cold gate requires a new-process identity versus the old process, a miss and no
hit in the *new process's own* monotonic metric window, `DuplicateCommitted`, a
real persistence `begin_attempts` increase, no confirmed commit/new-row/COPY
increase, unchanged publication-stored counter and zero entries after replay,
valid application ACK identity/outcome, unchanged ledger/raw/temporal/state
fingerprint and counts/version. Metrics are never subtracted across process
restarts. Safe replacement/baseline/window/ACK/DB evidence is persisted before
assertion. The top-level `p5b_cache_functional_accepted` marker is now set only
after warm + all conflicts + cold proof + exact owned-resource cleanup pass.

Next native execution invocation (not run in this implementation task):
`python3 scripts/tests/native_compose_smoke.py --prepare /var/tmp/opencode/paper-p5b/cache-cold-native-<unique-run> --paper-p5a --mode on --dataset 1 --paper-p5b-cache-smoke`.

## Native cache correctness smoke — current-source partial failure (26/10/2026)

Fresh invocation on current dirty source:
`python3 scripts/tests/native_compose_smoke.py --prepare <new-private-parent>/run --paper-p5a --mode on --dataset 1 --paper-p5b-cache-smoke`.
The runner target was absent before invocation. Protected parent/artifacts:
`/var/tmp/opencode/paper-p5b/native-cache-verify-zBt5r0/`; wrapper output and
exit status are outside `run/`. Wrapper exit was 1. `run.json` records `FAIL`,
schema 24, source revision `f098569395ec60a9051f4554040dd5023d74886e` on dirty
`feat/paper-requirement`, RS256 and HTTPS verified, and the intended cache-on
profile (100000 entries, 67108864 bytes, 600-second TTL); backend quota was 2 CPU /
4 GiB. Effective P5a ingress bounds and microbatch/shaper settings were retained.
The artifact records the actual image digests; notably backend
`sha256:4f4fbd360364de64bdaec7c97a51e1c28f2804ecd9c47a82ac473522ce7d1b64` and
PostgreSQL/migration image `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`.

Evidence reached before failure:
- Existing P5a functional checks all report PASS, including ledger/raw provenance,
  temporal/state/version checks, TLS/ACL, application ACKs and empty original
  Gateway outboxes. The same-process warm replay reports PASS: cache-hit,
  database-call-avoided and `DuplicateCommitted` each increased by one; persistence
  begin/commit/COPY/new-row deltas were zero; DB fingerprint/counts were unchanged;
  a real application ACK with `committed` status was observed for the exact replay.
- The fixture then failed in `paper_p5a_support.run` before conflict/cold evidence
  was persisted. `smoke-failure.json` says `STARTUP_FAILED` in `paper_p5a` with
  `ValueError` / `unclassified`; the protected failure stack identifies the call
  site but intentionally contains no underlying exception message. Thus the exact
  cause is **not established**; the failure occurred after warm replay and before
  the conflict setup checkpoint, so none of the four intended conflict reasons,
  cold restart/DB duplicate, or cold ACK is proven by this run. No false conflict
  ACK/cache claim is made. Do not infer the cause from the generic marker.
- Cleanup reports `cleanup_verified=true` and 16 containers / 1 network / 7 volumes
  removed; each of the 24 owned resource receipts also records `removed=true`.
  However `exact_cleanup_verified=false`; this does not meet the exact-cleanup
  acceptance gate. Therefore the smoke is FAIL/partial, not native cache acceptance.

Protected evidence: `run/artifacts/run.json`, `run/artifacts/smoke-failure.json`,
`run/artifacts/metrics.csv`, `run/artifacts/resources.csv`, private lifecycle and
cleanup receipts, plus `wrapper.log` and `wrapper.status` in the parent. Logs and
payload artifacts are private; do not publish their contents. No source/runtime
fix or retry was made because the available failure evidence does not identify a
specific ordinary fixture defect. Next slice: add/inspect protected failure
diagnostics that preserve a safe structured failure code for the pre-conflict
setup, then reproduce/fix only after the exact cause is known; run one fresh
native retry only after the focused regression/guards pass. Earlier failures remain
FAIL. This run does not complete P5b: configuration regression, comparable off/on
profiles, final gates and documentation remain outstanding.
Expected: isolated schema-24 fixture; existing workload unchanged; warm exact
replay and conflict assertions pass; controlled single-backend restart preserves
the effective profile; cold exact replay is a DB duplicate with real ACK, miss,
transaction work and unchanged persisted data; exact cleanup verified and only
then functional marker PASS. This is the first native cache functional run; native
Compose/Docker execution, P5b acceptance and performance claims remain NOT RUN.

## Verification for cold replay slice

Focused test additions first showed RED (acceptance incorrectly passed without
cold proof); GREEN then covered cold same-process windows/no cross-process metric
subtraction, DB work/no cache republish, and owned restart ordering/profile.
Focused tests: `PYTHONPATH=scripts/tests python3 -m unittest
test_paper_p5a_support.PaperP5aWorkloadContractTests.test_p5b_cache_acceptance_requires_cold_database_fallback_and_cleanup
test_paper_p5a_support.PaperP5aWorkloadContractTests.test_cold_replay_rejects_cross_process_counter_subtraction_and_requires_db_work
test_paper_p5a_support.PaperP5aWorkloadContractTests.test_cold_backend_replacement_stops_old_then_registers_owned_same_profile`
PASS. Final guards `PYTHONPATH=scripts/tests python3 -m unittest
test_paper_p5a_support test_native_compose_smoke test_paper_p3_support` — 187
tests PASS; `py_compile` for the four changed Python files, `sh -n
scripts/test-paper-p5a.sh`, and `git diff --check` PASS. Logs:
`/var/tmp/opencode/paper-p5b/cold-replay-slice/python-guards-final.log`.
Docker/native execution, Go build, schema/runtime/dependency/default change were
NOT RUN / not made.

## Handoff — conflict-precheck SQL UUID/text fix (26/10/2026)

The P5b native cache smoke failed before conflict acceptance: PostgreSQL reported
`operator does not exist: uuid = text` (safe stderr in
`/var/tmp/opencode/paper-p5b/native-cache-smoke-20261025T201623Z-1688801/run-3/private/db-error-e4bf1b77e137.txt`). The helper cast `gateway_id` parameters to
`uuid`, but migrations 1/20 define `processed_messages.gateway_id` and
`sensor_id` as `TEXT`; only `message_id` and `boot_id` are `UUID`. `psql :'var'`
substitutes quoted text, so UUID comparisons now cast those parameters explicitly
at the UUID columns while gateway/sensor comparisons remain text. Sequence bounds
remain explicitly `BIGINT`. The precheck still requires the exact persisted
payload/provenance or sequence-overlap row; SQL errors are not classified as
conflicts, and safe reason codes are persisted before rejection.

Regression was added first and failed RED because the query builder did not yet
exist; after extraction/fix the static schema-contract test passed for same-ID and
sequence-overlap queries. It reads the migration column declarations and checks
the actual helper query strings; it is explicitly **static, not native PostgreSQL
proof**. `PYTHONPATH=scripts/tests python3 -m unittest test_paper_p5a_support
test_native_compose_smoke` — 180 tests PASS; `py_compile` and `git diff --check`
PASS. Logs: `/var/tmp/opencode/paper-p5b/uuid-text-fix-20261025T202911Z/`.

Historical full fixture attempts in the artifact directory above remain FAIL,
including repeated occurrences of the same SQL typing error; cleanup receipts
remain retained. No Docker/full fixture was rerun for this fix. Next: one fresh
native cache-smoke invocation using the documented command in Handoff P5b.7d,
after reviewing this patch. Native cache acceptance remains NOT PASSED until that
run succeeds with exact cleanup.

## Handoff — P5b cache-smoke UUID/text state-query correction (26/10/2026)

Lượt sửa hẹp xác nhận schema thật trong migration 2: `twin_states.entity_id` là
UUID FK tới `twin_entities.id`; `twin_entities.entity_id` là URN `TEXT`. Các
version snapshots warm/conflict/cold từng dùng `USING(entity_id)`, khiến PostgreSQL
nối UUID với URN text. Đã gom chúng vào `VERSION_SNAPSHOT_SQL` dùng
`JOIN twin_entities e ON e.id=s.entity_id` và lọc `e.entity_id=:'urn'`. State
fingerprint dùng cùng phép nối qua `STATE_FINGERPRINT_SQL`; fingerprint temporal
và state-list vốn đã nối đúng qua `e.id=...`. `reported_version::text` được giữ để
snapshot có cùng JSON string shape như DTO/API version; không ép URN sang UUID.

Test query-builder được thêm trước implementation và FAIL vì chưa có hằng query.
Sau sửa, test tĩnh PASS. Đã chạy `scripts/tests/paper_p5a_sql_regression.py` trên
PostgreSQL disposable fixture hiện có: thực thi thật version snapshot, state
fingerprint, cùng cả bốn conflict-precheck statements (UUID message/boot casts,
TEXT gateway/sensor, BIGINT ranges, và timestamp). Fixture tạo state bằng UUID
entity ID và URN riêng; thêm timestamp column TIMESTAMPTZ cho dữ liệu synthetic
theo shape migration 24. Kết quả PASS và `Fixture cleanup verified: True`.
Đây là xác minh SQL/query-shape có giới hạn trên schema migration 20 cộng timestamp
fixture, không phải full schema-24/Auth/MQTT/cache acceptance.

Log mới ở `/var/tmp/opencode/paper-p5b/uuid-urn-sql-fix/`. Lượt sửa này không chạy
full stack; các FAIL lịch sử và cleanup receipts được giữ nguyên. P5b cache native
acceptance vẫn NOT RUN/PASS chưa đạt. Bước kế tiếp sau review: lệnh fresh native
cache-smoke ở handoff P5b.7d, chỉ khi được điều phối; không suy ra hoàn tất P5b.

## Handoff — conflict fixture contract corrections (26/10/2026)

Sửa đúng ba lỗi contract được tìm thấy sau native failure. `conflict_guard_variables`
giờ serialize `first`/`last` thành decimal strings; test gọi trực tiếp
`native_compose_smoke.db_psql` (thực thi validator thật) với toàn bộ biến, thay vì
mock call của SQL wrapper. Lượt RED ban đầu tái hiện chính xác `db_variable_invalid`
trước khi command/DB được gọi. UUID/TEXT query joins và SQL guard không bị nới lỏng.

Sensor đích của moved-ID case được provision bằng owner-authenticated `PUT` API
trước các snapshot conflict; không seed DB trực tiếp. Overlap case giờ yêu cầu
nguồn hai mẫu độc lập, có sequence dương: fixture gửi một batch hai mẫu mới bằng
Gateway TLS/DynSec principal, đợi application ACK và xác nhận ledger +1/raw +2.
Traffic setup được ghi riêng là `manual_authorized_functional_fixture_not_benchmark`
trước snapshot invariant; payload conflict lấy range đã commit, bắt đầu thấp hơn
một sequence để tạo overlap nhưng có cache key khác. Nguồn một mẫu/sequence 0 bị
từ chối thay vì tạo false “miss” proof.

Mỗi conflict cần structured backend rejection code (`message_conflict` hoặc
`sequence_overlap`) trong protected backend logs, cùng các gate DB/cache/failure/no
ACK/no mutation cũ. Giá trị reason được ghi an toàn trước assertion. Catch lỗi
precheck giờ giữ `ValueError` code an toàn, nên `db_variable_invalid` không còn bị
ẩn thành thông báo query chung. Không thêm API/runtime changes.

RED→GREEN: focused regression lần đầu FAIL vì validator thật trả
`db_variable_invalid`; sau sửa focused 4 tests PASS. Guard Python P5a + Compose:
185 tests PASS. `py_compile` và `git diff --check` PASS. `paper_p5a_sql_regression.py`
chạy disposable PostgreSQL fixture: PASS cho version/state snapshots và cả bốn
conflict prechecks; exact fixture cleanup `True`. Logs mới:
`/var/tmp/opencode/paper-p5b/conflict-fixture-contract/`.

Các full native attempts đã FAIL trong lịch sử report/artifact vẫn giữ nguyên FAIL;
không chạy full Auth/MQTT/cache fixture ở lượt này. Chưa có native P5b PASS, không
đánh dấu acceptance hoàn tất. Sau review, lệnh smoke kế tiếp:
`python3 scripts/tests/native_compose_smoke.py --prepare /var/tmp/opencode/paper-p5b/cache-cold-native-<unique-run> --paper-p5a --mode on --dataset 1 --paper-p5b-cache-smoke`.

## Handoff — bounded P5b native cache verification (26/10/2026)

**Kết luận:** không đạt P5b cache-smoke acceptance; không chạy thêm native attempt.
Lượt cuối đã chứng minh warm replay, bốn conflict guards, setup overlap và cold DB
duplicate thực tế. Nó chỉ FAIL ở assertion `confirmed_commits` trong cold replay.
Không đổi metric/runtime hoặc nới assertion để biến lượt FAIL thành PASS: theo code
hiện hành, metric này tăng cho mọi PostgreSQL transaction commit, kể cả transaction
chỉ xác nhận duplicate. Đây là mâu thuẫn cụ thể giữa điều kiện cold acceptance
“DB duplicate nhưng `confirmed_commits` không tăng” và semantics hiện tại của metric.

### Sửa helper và các nguyên nhân được xác định từ failure stack

- Failure stack của lần trước chỉ tới `paper_p5a_support.py:1199`. Source xác nhận
  `target = specs[1]` (GatewayB) nhưng provisioning dùng `tokens['a']`; đây là lỗi
  membership của fixture, không phải lý do nới ACL. Đổi sang owner token `b`, lưu
  HTTP status số an toàn vào run data trước khi raise và allowlist mã lỗi
  `paper_p5b_conflict_sensor_provision_failed`.
- Fresh run `native-cache-verify-20261025T210801Z-1790648` sau sửa token: warm PASS,
  owner PUT trả `409` vì fixture đã provision Sensor này với tên khác. Đồng bộ tên
  với setup (`Cache conflict sensor`) và chấp nhận status idempotent `200` hoặc
  tạo mới `201`; không seed DB hoặc bỏ qua membership.
- `native-cache-verify-20261025T211033Z-1800185`: owner PUT `200`; stack chỉ tới
  `uuid` chưa được bind trong dựng overlap payload. Tách helper có import `uuid`
  rõ ràng; regression kiểm message ID mới, sequence, hai mẫu và timestamp.
- `native-cache-verify-20261025T212314Z-1814320`: overlap setup đã ACK và DB delta
  đúng ledger +1/raw +2; stack chỉ tới `UnboundLocalError` ở lần đọc log backend.
  Assignment `backend_id` trong shutdown branch tùy chọn tạo local binding dùng
  trước đó. Chuyển binding lên trước lần dùng đầu tiên; regression kiểm thứ tự.
- Lượt cuối `native-cache-verify-20261025T213315Z-1827751` hoàn thành các pha đến
  cold replay rồi FAIL vì metric semantics dưới đây. Không có native retry tiếp.
  Error code của cold-replay reconciliation được thêm vào failure allowlist sau
  lượt chạy để lần sau không bị thu gọn thành `unclassified`; artifact lịch sử vẫn
  giữ giá trị thực tế lúc chạy.

### Kết quả lượt cuối và giới hạn acceptance

Artifact: `/var/tmp/opencode/paper-p5b/native-cache-verify-20261025T213315Z-1827751/`;
wrapper status `1`, `run/artifacts/run.json` là `FAIL`, schema DB `24`, revision
`f098569395ec60a9051f4554040dd5023d74886e` trên dirty `feat/paper-requirement`.
Profile thực tế: cache bật, TTL 600s, 100000 entries / 67108864 bytes; microbatch
bật; shaper tắt; RS256, HTTPS, MQTT TLS/DynSec; backend giới hạn 2 CPU / 4 GiB.
Các cờ ingress/microbatch còn lại và image manifest được lưu trong `run.json`.
Image digest chính: backend `sha256:341f2c86c77fc0d9517d1e50d88eb4d658bc44971805883bcd22c37bfe520159`,
Gateway `sha256:f252ca9f084fcf6bc9ba702288302049109e69b2774aecfa68aff8f480886fd8`,
Postgres/migration `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`,
Mosquitto `sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`.

- Warm replay PASS: same-process cache hit +1, DB call avoided +1, duplicate ACK
  `committed`, persistence begin/commit/COPY/new-row counters unchanged, DB unchanged.
- Four conflict cases PASS, all protected guard preconditions matched, each had
  exactly one persistence failure, no application ACK and unchanged DB/cache
  invariants. Reasons: changed payload `message_conflict`; moved Sensor/key
  `message_conflict`; same RAM key/new ID `sequence_overlap`; distinct first
  sequence overlap `sequence_overlap`.
- Overlap setup was explicitly labeled
  `manual_authorized_functional_fixture_not_benchmark`, had two samples and a real
  application ACK; ledger 12→13, raw 12→14.
- Cold replay: process identity changed; new cache entries 0→0; miss +1, hit +0;
  DB duplicate +1, begin attempts +1; exact application ACK observed and DB stayed
  unchanged. `copy_calls` and `confirmed_new_rows` stayed unchanged. The sole reason
  in `cold_replay.reasons` is `new_persistence_work:confirmed_commits`; metrics CSV
  shows new-process `confirmed_commits` 0→1.
- Code evidence: `src/internal/telemetry/postgres_repository.go` increments
  `confirmedCommits` after successful `tx.Commit` in the batch path (around lines
  318–332), regardless of whether a result was a new commit; only
  `confirmedNewRows` is conditional on `CommitOutcomeCommitted`. Therefore a
  committed DB-duplicate transaction necessarily increments `confirmed_commits`
  under current semantics. Existing `TestPostgresTelemetryIntegration` batch-metrics
  assertions (around `src/internal/telemetry/postgres_integration_test.go:1211–1224`)
  explicitly assert the duplicate group increments `ConfirmedCommits` by 1 while
  COPY and new rows stay unchanged. Resolve the cold acceptance/metric contract
  before any future retry; no runtime metric change was made here.
- P5a base checks passed, including 24 application ACKs, ledger/raw provenance,
  temporal/state, and both original Gateway outboxes read back with pending and
  quarantined rows/bytes all zero.

Cleanup was verified: all 25 owned resource receipts say `removed=true` (17
containers, 1 network, 7 volumes). `cleanup_verified=true`; the run's composite
`exact_cleanup_verified=false` and functional marker is false because cold
acceptance failed. Do not report exact-cleanup acceptance as PASS. No shared or
production resources were touched.

Focused verification after the final source/diagnostic edits:
`PYTHONPATH=scripts/tests python3 -m unittest test_paper_p5a_support test_native_compose_smoke`
— 189 tests PASS; `py_compile` for the four affected Python files and
`git diff --check` PASS. Earlier PostgreSQL SQL fixture PASS remains as recorded in
the preceding handoff; no new native run was made after the bounded final attempt.
P5b overall, other profiles/final gates, and documentation beyond these handoffs
remain incomplete.

## Handoff — cold duplicate transaction metric assertion corrected (26/10/2026)

Không thay đổi runtime/counter/schema/default. Source semantics được đối chiếu với
`src/internal/telemetry/postgres_repository.go`: cả Persist và PersistBatch tăng
`confirmedCommits` sau mọi SQL `tx.Commit` thành công; chỉ `confirmedNewRows` có
điều kiện `CommitOutcomeCommitted`/số sample mới. Vì vậy DB duplicate cold replay
phải ghi nhận transaction begin +1 và confirmed SQL commit +1, nhưng không được
tạo `copy_calls`, confirmed new rows, ingress `Committed` hay `SamplesNew`. Helper
giữ nguyên yêu cầu cache rỗng, miss +1/hit +0, không publish proof, application ACK
đúng và fingerprint/count DB bất biến. Bổ sung test dùng snapshot đại diện
duplicate-only transaction, đồng thời xác nhận delta new-row/COPY/ingress mới làm
evidence FAIL.

### RED → GREEN và native recheck

- Regression test được viết trước: lần chạy với
  `PYTHONPATH=scripts/tests python3 -m unittest
  test_paper_p5a_support.PaperP5aWorkloadContractTests.test_cold_replay_accepts_duplicate_only_transaction_without_new_data`
  FAIL đúng tại assertion PASS của cold replay (`FAIL != PASS`) theo helper cũ.
  Sau sửa helper, test PASS và xác nhận delta `begin_attempts=1`,
  `confirmed_commits=1`, `confirmed_new_rows=0`, `copy_calls=0`, ingress
  `Committed=0`/`SamplesNew=0`; inject delta mới vào mỗi data counter vẫn FAIL.
- Current-source native recheck: run directory
  `/var/tmp/opencode/paper-p5b/native-cache-metric-fix-20261026T214831Z-1847022/`;
  wrapper log `/var/tmp/opencode/paper-p5b/native-cache-metric-fix-wrapper-214831Z-1847022.log`,
  status `/var/tmp/opencode/paper-p5b/native-cache-metric-fix-status-214831Z-1847022.txt`.
  Invocation: `python3 scripts/tests/native_compose_smoke.py --prepare
  <run-directory> --paper-p5a --mode on --dataset 1 --paper-p5b-cache-smoke`.
  Run JSON source revision `f098569395ec60a9051f4554040dd5023d74886e`, dirty;
  schema 24; RS256 + HTTPS, MQTT TLS + ACL; backend quota 2 CPU/4 GiB;
  RAM dedup on (600s, 100000 entries/67108864 bytes), microbatch on, shaper off,
  Stage5 off. Image digests and effective profile are in `artifacts/run.json`.
- Inspected `artifacts/run.json` (not wrapper marker alone): status
  `PAPER_P5A_WORKLOAD_ACCEPTED`, `p5b_cache_functional_accepted=true`,
  `p5b_cold_replay_status=PASS`, and nested `exact_cleanup_verified=true`.
  Warm replay PASS (hit +1, avoided DB call +1, duplicate ACK, unchanged DB and
  persistence counters); all four conflict cases PASS with expected conflict/
  overlap guard reasons, no ACK and unchanged DB; authorized overlap fixture ACKed
  with ledger 12→13/raw 12→14. Cold replay: new process, cache entries 0→0,
  miss +1/hit +0, DB duplicate +1, begin +1/confirmed SQL commit +1, COPY +0,
  confirmed new rows +0, ingress Committed +0/SamplesNew +0, exact application
  ACK, unchanged DB fingerprints/counts. Both original Gateway outboxes passed the
  fixture's readback/drain acceptance. Exact owned cleanup verified: 17 containers,
  1 network, 7 volumes (25 receipts removed).
- Environment/profile images, functional evidence, metrics and cleanup records:
  `artifacts/run.json`, `artifacts/metrics.csv`, `artifacts/resources.csv`,
  `artifacts/startup-summary.json`. The historical
  `native-cache-verify-20261025T213315Z-1827751` remains a FAIL; this recheck is new
  evidence and does not rewrite/reclassify that run.
- Checks after edits: focused RED then GREEN; `PYTHONPATH=scripts/tests python3
  -m unittest scripts.tests.test_paper_p5a_support scripts.tests.test_native_compose_smoke`
  PASS (189 tests); `py_compile` for helper/test PASS; `git diff --check` PASS.

This corrects the cold-replay semantics assertion and the single requested native
cache smoke is functionally accepted. It does not complete P5b overall: other
profiles and P3/final gates/documentation remain outstanding; no benchmark, full CI,
HIL, rollout, or runtime counter change was performed.

## Affected P3 command/result configuration-preservation regression (26/10/2026)

Added a deliberately narrow opt-in to the existing P3 fixture:
`--paper-p3 --p5a-control-regression --p5b-control-regression`. The new flag is
rejected unless both existing P3/P5a control selectors are supplied; ordinary P3
and the existing P5a regression retain their former defaults. This is **not** a
cache off/on comparison or a second P5b cache acceptance run. It reuses the same
schema-24 migration lifecycle, P2 RS256/HTTPS/Unix-auth perimeter, MQTT TLS/DynSec,
owned cleanup registry, 2 CPU / 4 GiB backend quota, four MQTT workers, queue
capacity 256, and existing command/result + two-message old/new control regression.
The fixture's effective RAM dedup settings were enabled with 100000 entries,
67108864 bytes, and the existing 600-second TTL; microbatch stayed enabled and the
shaper stayed off. No control tuning was made.

The metrics-disabled backend replacement explicitly carries and reads back the
same cache enable/caps instead of silently reverting to cache-off. The existing
metrics-off command/result and Gateway application-ACK flow then runs against that
replacement. The new contract guards cover flag independence, rejection without
the P5a opt-in, effective Compose cache profile/schema-24 terminal, narrow result
marker, metrics-off cache flag readback, and the unchanged default P3 profile.

### Current-source native evidence

- Invocation: `python3 scripts/tests/native_compose_smoke.py --prepare
  /var/tmp/opencode/paper-p5b/p3-control-cache-final-20261026T231200Z
  --paper-p3 --p5a-control-regression --p5b-control-regression`. Target did not
  exist beforehand; wrapper output is outside it at
  `/var/tmp/opencode/paper-p5b/p3-control-cache-final-20261026T231200Z.wrapper.log`.
- Artifact run status `PAPER_P5B_CONTROL_PRESERVATION_ACCEPTED`; source revision
  `f098569395ec60a9051f4554040dd5023d74886e`, branch `feat/paper-requirement`,
  dirty worktree. The actual migration lifecycle applied schema 24. RS256/HTTPS,
  MQTT TLS/DynSec and the backend quota/profile checks passed. Backend image
  `sha256:52dfa941b7e77df1be1a879624ce12fab45b96c898f68f59c292749ab0655552`;
  PostgreSQL/migration image
  `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`;
  other service digests are retained in `artifacts/run.json`.
- Before telemetry, an actual P3 command/result had succeeded: desired version 4,
  reported configuration `sample_interval_us=16667`; state API and DB matched.
  With RAM dedup enabled, the actual OLD message was ACKed and persisted as one
  ledger/raw/temporal observation with matching provenance; reported configuration
  and desired state stayed unchanged and the older observation did not roll state
  back. The actual NEW message was likewise ACKed and persisted one each in
  ledger/raw/temporal; it advanced reported version 12→13 while the command's
  `reported.configuration` and desired state remained unchanged. API readbacks
  matched DB state. Microbatch metrics showed two group attempts and two members.
- Cache snapshot reported enabled before and after the telemetry pair. On the
  metrics-off replacement, the cache flag and both caps were explicitly read back;
  the existing metrics-off command/result succeeded, its application ACK drained
  the outbox, and raw/ledger reconciliation passed. This proves metrics-off
  independence for this affected path, not a cache performance claim.
- Final exact cleanup: 17 containers, 1 network, 7 volumes. `run.json`,
  `metrics.csv`, and `startup-summary.json` are in
  `/var/tmp/opencode/paper-p5b/p3-control-cache-final-20261026T231200Z/artifacts/`.
  The earlier same-task accepted run at
  `/var/tmp/opencode/paper-p5b/p3-control-cache-20261026T230500Z/` predates the
  final safe metrics-off readback marker; this documentation cites only the fresh
  final run above.

Focused verification: Python compile passed; 22 focused helper/profile/schema
history tests passed, including the existing no-cache default P3 selector and the
metrics-off replacement tests. An initial combined suite exposed three fixture
assertion wiring mismatches (positional fixture field ordering / expected mocked
write call); they were corrected and the focused rerun passed. The one fresh native
rerun then passed as above. No off/on benchmark, broader P5b criteria, full CI,
HIL, rollout, commit/push, or later phase was run. This is only the requested P3
configuration-preservation regression; overall P5b and remaining gates are still
incomplete.

## Handoff — P5b.8a comparable workload profile/entrypoint (26/10/2026)

Added the thin `--paper-p5b --cache-mode off|on --dataset 100|1` selector and
`scripts/test-paper-p5b-profile.sh`. Both cache arms reuse the finite P5a Gateway
dataset and isolated P2 topology/lifecycle on schema 24. P5b explicitly keeps
microbatch enabled with the existing P5a bounds and wait, while shaper, workers,
pool, quota, security and dataset are invariant; only `MQTT_RAM_DEDUP_ENABLED`
changes. Thus `--cache-mode` does not reuse or overload P5a's `--mode` microbatch
meaning. Existing P5a/P3 profiles and deployment defaults are unchanged.

Prepared `run.json` distinguishes cache mode from `microbatch_mode`, identifies
schema 24, effective flags/caps/600-second TTL, fixed 24 unique Gateway messages
and 2400 or 24 raw samples. Source revision and dirty status are captured during
preparation; the image manifest is explicitly pending capture after image build.
New-message, duplicate replay and drain are `configured_not_run`. The
planned replay uses the same saved original Gateway message repeated 12 times
per Gateway (24 offered duplicate deliveries total), with denominator and manual
replay ACK timing explicitly separate from Gateway generation/publish/drain ACK
timing; manual replay outbox readback is not applicable. This slice does not add
the replay sender, measure phases, or run Docker/native workloads. Existing P5a
schema-23 measurements remain historical context, never a schema-24 baseline.

Exact profile invocation template (not executed):
`sh scripts/test-paper-p5b-profile.sh <off|on> <100|1> [NEW_RUN_DIRECTORY]`.
The wrapper dispatches to `native_compose_smoke.py --paper-p5b --cache-mode ...
--dataset ...`; four independent invocations on schema 24 are needed for the
cache off/on × 100/1 comparison. Next slice must attach the fixed replay/drain
phase hook to the P5a workload runner and populate measured phase denominators;
until then this is profile plumbing only, not benchmark evidence or P5b
acceptance. No checklist criterion is ticked.

Verification for P5b.8a: RED was captured at
`/var/tmp/opencode/paper-p5b/8a/red-correct.log` (new profile helper absent);
GREEN focused profile/CLI tests and guards are in
`/var/tmp/opencode/paper-p5b/8a/final-focused.log` and
`python-guards-final.log`. One intermediate all-effective-values comparison
correctly exposed run-specific generated fixture identities/secrets; the final
assertion compares the deterministic effective profile only, preserving unique
credentials/IDs per fixture.
`PYTHONPATH=scripts/tests python3 -m unittest test_paper_p5a_support
test_native_compose_smoke test_paper_p3_support test_migration24_contract` —
210 tests PASS. Python compile, wrapper `sh -n` and `git diff --check` PASS. No
native/Docker run, duplicate replay, benchmark, or P5b
completion claim.

## Handoff — P5b.8b comparable bounded duplicate replay (26/10/2026)

The `--paper-p5b` workload now adds exactly 12 serial duplicate publishes per
Gateway after the finite unique Gateway writers complete and their real outboxes
are drained (24 offered duplicates/profile). Each publish uses the captured
Gateway payload bytes, original telemetry topic and Gateway identity. Both ACK
observers are started before the phase baseline/publishes; every observed
application ACK is checked against the original message ID and ACK topic. No new
identity, altered payload, direct DB/cache seed, or Gateway outbox replay is used.
Manual replay readback is explicitly `not_applicable_manual_replay`.

The phase stores received/ACK/offered denominators, original payload hashes,
per-publish manual-fixture-to-ACK monotonic spans (including CLI and observer
cost), phase bounds, process identity, DB fingerprint/count/version snapshots,
cache hit/call-avoided deltas and persistence begin/commit/COPY/new-row deltas.
The DB-duplicate outcome count is reconciled as ingress duplicate-outcome delta
minus actual RAM-hit delta (the exporter exposes a combined duplicate outcome,
not a separate SQL-classification counter); DB transaction starts are separately
reported and are not relabeled as SQL duration.
The original evaluator required 24 received duplicate deliveries and matching
ACKs, exact original bytes, stable persisted data, no new commits/COPY rows, and
mode-specific work. Its cache-off rule incorrectly equated one transaction start
with each duplicate; its ACK reader also reused the first matching ACK. These
rules/claims are superseded by the fixture-measurement correction below. Cache-on
must use observed hit/call-avoid evidence rather than a configured flag.
The pre-existing new workload/drain metrics remain separate; Gateway ACK metrics
remain the real unique-delivery measurement. Manual spans are not called broker
latency or SQL duration. No precise percentiles are inferred.

Profile workload acceptance now additionally requires the replay evidence PASS
and exact owned-resource cleanup; the four-profile comparison remains false until
off/on × 100/1 runs are reconciled. This slice adds no native-run result and must
not be treated as P5b completion. Run the four profiles sequentially with fresh
run directories, for example:

```sh
sh scripts/test-paper-p5b-profile.sh off 100 /var/tmp/opencode/paper-p5b/profile-off-100-<unique>
sh scripts/test-paper-p5b-profile.sh on 100 /var/tmp/opencode/paper-p5b/profile-on-100-<unique>
sh scripts/test-paper-p5b-profile.sh off 1 /var/tmp/opencode/paper-p5b/profile-off-1-<unique>
sh scripts/test-paper-p5b-profile.sh on 1 /var/tmp/opencode/paper-p5b/profile-on-1-<unique>
```

Focused RED was recorded at `/var/tmp/opencode/paper-p5b/8b/red.log` (missing
reconciliation helper; diagnostic safety test also exposed raw-payload persistence
risk). Native/Docker runs are intentionally NOT RUN in this implementation slice.

## Handoff — duplicate replay fixture measurement corrections (26/10/2026)

Fixed two fixture-only defects; no ingress runtime, database schema, DTO, SQL
binding, feature flag, deployment default, or security behavior changed.

1. `paper_p1_capture_subscription` historically returned the first matching topic
   in the entire log and did not consume it. The replay loop called it repeatedly,
   so a previously logged ACK could be counted again. It now accepts optional
   `after_count` (default 0 preserves all existing callers) and selects a matching
   PUBLISH at/after that zero-based topic ordinal. The replay establishes the
   current matching-message boundary immediately before each publish, then records
   that ordinal and the monotonic time of the newly observed ACK. A real parser/log
   reader regression uses a controlled append producer, existing first ACK, an
   intervening nonmatching topic, second ACK, bounded missing-ACK result, and invalid
   cursor rejection.
2. Duplicate ingress outcomes can commit in microbatch SQL transactions. The
   evaluator no longer requires `confirmed_commits=0` or one `begin_attempts` per
   delivery. Cache-off requires exactly 24 duplicate outcomes, zero new ingress
   commits/samples, zero COPY/new rows, unchanged persisted fingerprints/counts/
   versions, zero cache hit/avoided calls, and positive begin/confirmed-commit
   counts bounded by duplicate outcomes/begins (transaction grouping is reported
   as measured). Cache-on requires 24 real cache hits and 24 avoided repository
   calls with no DB transaction work. ACK publish counter and fresh observed ACK
   count are independently required to equal 24. No SQL-statement count is inferred.

The replay now takes a bounded ending drain snapshot only after Processed,
DuplicateCommitted and ACKPublished each increase by 24 and QueueDepth, InFlight
and BufferedPayloadBytes are zero. Timeout diagnostics are saved before failure;
the same serial schedule and drain gate apply to both cache arms. Manual ACK spans
remain end-to-end fixture measurements including CLI and observer cost, not broker
latency.

### Verification and retained history

- RED: `/var/tmp/opencode/paper-p5b/duplicate-fixture-fix/red.log` — grouped
  duplicate SQL transactions rejected and cursor test errored on the absent
  `after_count` parameter, as expected before implementation.
- Focused test runs and final results are recorded in the task response; artifacts
  are under `/var/tmp/opencode/paper-p5b/duplicate-fixture-fix/`.
- Historical P5b.8b claim of 24 ACKs is **not valid evidence of 24 distinct
  observations** because the reader reused the first ACK; do not reinterpret that
  run as measured acceptance. Preserve its original run status/history unchanged.
- Both cache-off/on datasets (100 and 1) must be rerun with the corrected cursor,
  fresh-ACK and bounded-drain logic before they can form a comparable baseline.
  No native profile was executed for this correction; no P5b overall criterion or
  comparison baseline is declared accepted.

Verification actually run: focused regression was RED before implementation, then
the nine `PaperP5bComparableReplayTests` passed; the final relevant suite
`PYTHONPATH=scripts/tests python3 -m unittest test_paper_p5a_support
test_native_compose_smoke` passed 205 tests. `py_compile` on the three changed
Python files, `sh -n scripts/test-paper-p5b-profile.sh`, and `git diff --check`
passed. No Docker/native/profile execution was run. Logs are in
`/var/tmp/opencode/paper-p5b/duplicate-fixture-fix/`.

## Corrected comparable cache-off/on datasets 100 and 1 (26/10/2026)

Executed sequentially with the corrected fresh-ACK cursor and bounded-drain
evaluator, using `sh scripts/test-paper-p5b-profile.sh off 100
/var/tmp/opencode/paper-p5b/corrected-off-100-20261026` followed by the same
command with `on` and `corrected-on-100-20261026`. Both run artifacts report
`PAPER_P5B_PROFILE_WORKLOAD_ACCEPTED`, duplicate replay `PASS`, and exact cleanup
verified. This is one corrected 100-sample dataset pair, not the four-profile
comparison or overall P5b acceptance (`p5b_comparison_accepted=false`; the
100/1 × off/on reconciliation remains pending).

Both runs used schema 24, dataset 100 samples/message, 24 unique telemetry
messages and 24 fresh Gateway application ACKs before replay. Persisted baseline
was 24 ledger rows, 2,400 raw rows and 24 temporal rows; state fingerprints and
versions remained unchanged through replay. The protected effective profile
readback matched except `MQTT_RAM_DEDUP_ENABLED=false` vs `true`: microbatch on
(max 32 messages / 500 rows / 4 MiB / 1 s), shaper off, 4 workers, queue 256,
DB pool max 10, backend quota 2 CPU / 4 GiB, RS256, HTTPS, MQTT TLS and ACL.
Cache caps/TTL were 100,000 entries / 64 MiB / 600 s. Only backend quota was
bounded in resource metadata; other listed service resource limits were 0
(unbounded/not configured).

| Replay measurement | Cache off | Cache on |
| --- | ---: | ---: |
| Offered exact-byte duplicate republishes / received / fresh application ACKs | 24 / 24 / 24 | 24 / 24 / 24 |
| Duplicate ingress outcomes | 24 | 24 |
| RAM hits / repository calls avoided | 0 / 0 | 24 / 24 |
| DB duplicate outcomes | 24 | 0 |
| Replay SQL begins / confirmed commits | 24 / 24 | 0 / 0 |
| Replay COPY calls / confirmed new rows | 0 / 0 | 0 / 0 |
| Persisted DB fingerprint/counts/versions changed by replay | No | No |
| Replay cache logical bytes at drain | 0 | 11,952 (24 entries) |
| Duplicate replay fixture window | 29.524 s | 5.247 s |
| Fresh manual-publish-to-application-ACK span, mean (median; min–max) | 1.141 s (1.145; 1.115–1.167) | 0.130 s (0.130; 0.106–0.148) |

Manual ACK spans include publisher CLI and observer overhead and are not broker
latency. The serial offered pattern was 12 exact original-byte republishes for
each Gateway in each arm. Off-path DB duplicate outcomes were accepted as
duplicate-only commits: 24 began/confirmed transactions for 24 deliveries in
this run (not an inferred SQL statement count). On-path all 24 replay duplicate
outcomes were cache hits before repository access; the zero SQL-work figures
are persistence transaction/COPY/new-row counters, not statement-level proof.
The bounded drain completed within its 20 s deadline with QueueDepth, InFlight
and BufferedPayloadBytes all zero. Fresh ACK count was measured using the
per-publish topic cursor; the ending metrics snapshot followed the 24 processed,
duplicate and ACK counter increments.

The workload drain's microbatch snapshot is separate from the exact-replay
phase: off run reported 45 groups / 49 members total (including unique workload
and replay); on run reported 21 groups / 24 members because cache-hit replays
returned before batch collection. In the isolated off duplicate replay
increments, 24 members were grouped into 24 transactions (timer flushes); the
20 transactions / 24 members belong to the off new-message phase, while the
on new-message phase recorded 21 transactions / 24 members. The on replay
generated no batch members/transactions. This fixture grouping is
observed workload-specific behavior, not a guaranteed transaction ratio.
Initial unique workload Gateway ACK summaries were 12 each: off p50 1.027/1.045
s and p95 1.084/1.050 s; on p50 1.044/1.043 s and p95 1.091/1.045 s. The
generation-plus-drain windows were 16.425 s off and 16.398 s on. These timings
are reported independently from duplicate-phase fixture ACK spans.

Resource samples are sparse Docker snapshots, not peak/continuous measurements.
At `p5b_duplicate_replay_before` to `p5b_duplicate_replay_drain`, backend
memory was 25,679,626→25,396,510 bytes off and 15,823,011→15,560,867 bytes on;
PostgreSQL was 67,947,724→66,448,261 bytes off and 63,627,591→63,627,591 bytes
on; Mosquitto was 3,014,656→1,925,185 bytes off and 2,891,972→1,798,307 bytes
on. These observations do not establish peak RSS or cache memory cost. No
capacity, SQL statement-count, or production performance claim follows from
this bounded fixture.

Run artifacts: off `ncs_b0346d978056` at
`/var/tmp/opencode/paper-p5b/corrected-off-100-20261026/artifacts/`; on
`ncs_41743bd2910c` at
`/var/tmp/opencode/paper-p5b/corrected-on-100-20261026/artifacts/`. Each has
`run.json`, `metrics.csv`, `resources.csv` and `startup-summary.json`; each
records schema 24, validated effective profile, RS256/HTTPS/MQTT-TLS/ACL,
backend quota, exact owned cleanup of 16 containers / 1 network / 7 volumes,
and cleanup verified true. Wrapper logs/status are
`/var/tmp/opencode/paper-p5b/corrected-{off,on}-100-wrapper.{log,status}`.
Wrapper stdout also emitted the nested P5a workload summary (`mode=on`), which
is not the cache-arm result; the P5b `run.json` effective profile and
`duplicate_replay.cache_mode` above are authoritative. No failures occurred in
these two corrected runs. Earlier off failure, prior on functional PASS with
the stale-ACK defect, and the explicit measurement-correction RED history above
remain historical and are not replaced or reclassified by this pair.

### Corrected comparable cache-off/on dataset 1 (26/10/2026)

Ran sequentially with fresh run directories: `sh scripts/test-paper-p5b-profile.sh
off 1 /var/tmp/opencode/paper-p5b/dataset1-cacheoff-20261026-055833`, then the
same command with `on` and `/var/tmp/opencode/paper-p5b/dataset1-cacheon-20261026-055833`.
Both wrapper statuses are 0; both `run.json` files report
`PAPER_P5B_PROFILE_WORKLOAD_ACCEPTED`, replay `PASS`, and
`exact_cleanup_verified=true`, with exact cleanup of 16 containers / 1 network /
7 volumes. Each uses schema 24, 24 unique single-sample messages, 24 fresh
Gateway ACKs before replay, and 24 baseline ledger/raw/temporal rows with reported
versions 9 and 9. Effective profiles match except
`MQTT_RAM_DEDUP_ENABLED=false/true`: microbatch on (32 messages / 500 rows / 4 MiB
/ 1 s), shaper off, 4 workers, queue 256, DB pool 10, backend quota 2 CPU / 4 GiB,
RS256/HTTPS and MQTT TLS/ACL. Cache caps/TTL are 100,000 entries / 64 MiB / 600 s;
other services have no configured CPU/memory quota. Revision is
`f098569395ec60a9051f4554040dd5023d74886e` on the dirty feature branch.

| Replay measurement | Cache off | Cache on |
| --- | ---: | ---: |
| Offered exact-byte republishes / received / fresh application ACKs | 24 / 24 / 24 | 24 / 24 / 24 |
| Duplicate ingress outcomes | 24 | 24 |
| RAM hits / repository calls avoided | 0 / 0 | 24 / 24 |
| DB duplicate outcomes | 24 | 0 |
| Replay SQL begins / confirmed commits | 24 / 24 | 0 / 0 |
| Replay COPY calls / confirmed new rows | 0 / 0 | 0 / 0 |
| Persisted fingerprints/counts/versions changed by replay | No | No |
| Cache logical bytes after replay drain | 0 | 11,952 (24 entries) |
| Duplicate replay fixture window | 29.426 s | 5.306 s |
| Fresh manual publish-to-application-ACK span, mean (median; min–max) | 1.138 s (1.143; 1.110–1.158) | 0.133 s (0.134; 0.105–0.151) |

The offered pattern is 12 serial exact captured-byte republishes per Gateway, with
zero publisher retries. Fresh ACKs were separately observed using the per-publish
topic cursor. Both arms reached the bounded drain with QueueDepth, InFlight and
BufferedPayloadBytes zero. Off performed 24 duplicate DB transactions; on had 24
real cache hits / repository calls avoided and no replay transaction/COPY/new-row
work. These are transaction/repository counters, not SQL statement counts. Replay
left ledger/raw/temporal fingerprints, counts, state fingerprints and versions
unchanged. Manual ACK spans include CLI and observer overhead; they are not broker
latency. New-message work was separately 8 grouped attempts / 24 members / 24 rows,
8 transactions and 8 COPY calls in each arm. New-message Gateway ACK p50 was off
1.081/0.840 s and on 1.089/0.801 s; p95 was off 1.136/1.042 s and on
1.142/1.043 s. Generation-plus-drain windows were 6.328 s off and 6.385 s on.
These are workload observations, not evidence that cache improves new-message
latency.

Sparse Docker samples at replay-before → replay-drain showed backend memory
8,462,008→8,597,274 bytes off and 8,281,653→8,364,490 bytes on; PostgreSQL
68,398,612→68,723,671 bytes off and 61,289,267→61,320,724 bytes on; Mosquitto
2,211,446→1,110,441 bytes off and 2,068,840→966,656 bytes on. Samples are not
peak/RSS or cache-cost evidence; 11,952 logical cache bytes are an estimate, not
allocator/RSS usage. No saturation, SQL-statement or production-performance claim
follows from this bounded fixture.

Artifacts: off run `ncs_a9a30a12aaa8` at
`/var/tmp/opencode/paper-p5b/dataset1-cacheoff-20261026-055833/artifacts/`; on
run `ncs_c3bf4dba5f51` at
`/var/tmp/opencode/paper-p5b/dataset1-cacheon-20261026-055833/artifacts/`;
each contains `run.json`, `metrics.csv`, `resources.csv`, and `startup-summary.json`.
Wrapper logs/status are `/var/tmp/opencode/paper-p5b/dataset1-{off,on}-wrapper.{log,status}`.
The nested P5a summary's `mode=on` refers to microbatch, not RAM cache; the actual
cache arm is recorded by `ram_cache.enabled` and `duplicate_replay.cache_mode`.

## P5b.9b — final schema/caller integration audit (26/10/2026)

Audited the actual migration mount/application sequence in root Compose and native
Compose, schema history and migration-24 guards, and the callers that run the
current telemetry writer. Migration 24 is mounted and applied after 21–23 in both
Compose compatibility phases and in the native fixture; the existing terminal/history
and migration-24 tests cover order, backfill/refusal/idempotency and retained guards.
The accepted P5b fresh-owned-database profiles above record `database_schema_version
24` and pinned application/backend image IDs; their `PAPER_P5B_PROFILE_WORKLOAD_ACCEPTED`
run artifacts establish real fresh-schema workload use. Existing native migration
lifecycle evidence (including the schema-23-to-24 upgrade/backfill/refusal cases)
was read from the earlier Stage 3/P5b handoff and was not redundantly rerun here.
Migration 1–23 files were not modified. Historical reader-only schema-20
compatibility checks stay historical; writer fixtures were not exempted from the
new ledger column.

The audit found real stale-schema writer fixtures: Stage 3 MQTT ingress, the Stage
3 uplink and workload runners applied only through 20 before starting current Go
writers, and Stage 4's real vertical scenario applied terminal 23 before publishing
simulator telemetry. Updated those fixtures to apply the existing post-Auth
21–24 compatibility tail before seeding/running writers, and made Stage 4 terminal
history verification require schema 24. A regression guard covers these writer
fixtures and Stage 4's telemetry publish path. Its test was run RED first against
the old terminal/version-20 fixture source; it failed as intended. No applied
migrations, production defaults, runtime Go code, or unrelated legacy readers
changed.

Affected native acceptance after the fixture fixes:

- `sh scripts/test-stage3-mqtt-ingress.sh`: PASS. Real QoS1 telemetry write and
  exact resend against the isolated current schema, ACK and SQL counts/state
  reconciled, normal shutdown and exact fixture cleanup verified.
- `sh scripts/test-stage3-gateway-simulator.sh`: PASS. Actual Gateway CLI/outbox
  wrote through the current backend; ACK drained outbox, independent SQL checks
  passed, exact cleanup verified.
- `sh scripts/test-stage3-uplink.sh --scenario normal`: PASS. Two Gateway writers,
  200 raw / 2 ledger / 2 temporal rows, SQL state checks and exact cleanup passed.
- `STAGE4_REQUIRE_RUN=1 sh scripts/test-stage4-api.sh --scenario all`: PASS for
  vertical, contract and queries; the vertical writer's 3 batches and byte-exact
  resend passed. Backend and Gateway images were built from the current `src/`
  Dockerfile (digests are in `/var/tmp/opencode/paper-p5b/p5b-9b/stage4-api-schema24.log`);
  all fixture resources and shared images were cleaned up. No persistent artifacts
  or resolved Compose secrets were printed.

Affected Python suites, with `PYTHONPATH=scripts/tests`, passed **476 tests**:
`test_stage4_api_runner`, `test_migration_ci_compat`, `test_migration24_contract`,
`test_ci_migration_job`, `test_native_compose_smoke`, P3/P4/P5a support suites,
Stage 3 workload/uplink runner suites, and CI Go-test/feature-job guards. Also
passed `py_compile` for the four changed Python files, `sh -n` for affected Stage
3/Stage 4 wrappers, and `git diff --check`. The full Stage 3 600-batch workload
was not rerun; its fixture now applies schema 24 and the affected normal writer
path was exercised by the bounded MQTT/uplink native smokes above. No full/remote
CI, ARM/HIL, soak, coverage gate, or rollout was run. Historical native FAILs
remain FAIL and are not reclassified.

New logs: `/var/tmp/opencode/paper-p5b/p5b-9b/python-guards-final.log`,
`stage3-mqtt-ingress-schema24.log`, `stage3-gateway-simulator-schema24.log`,
`stage3-uplink-schema24.log`, and `stage4-api-schema24.log`. This completes only
the P5b.9b audit/verification slice; overall P5b criteria and completion remain
unticked pending the final documentation/navigation/backlog step.

### Consolidated four-profile comparison status

All four corrected cache-off/on × dataset 100/1 workload runs are individually
accepted with exact cleanup. Replay comparison summary:

| Samples/message | Cache off/on run artifacts | Replay window off → on | DB duplicate transactions off → on | Hits / repository calls avoided off → on |
| ---: | --- | ---: | ---: | ---: |
| 100 | [off](#corrected-comparable-cache-offon-datasets-100-and-1-26102026), [on](#corrected-comparable-cache-offon-datasets-100-and-1-26102026) | 29.524 → 5.247 s | 24 → 0 | 0/0 → 24/24 |
| 1 | [`ncs_a9a30a12aaa8` off](#corrected-comparable-cache-offon-dataset-1-26102026), [`ncs_c3bf4dba5f51` on](#corrected-comparable-cache-offon-dataset-1-26102026) | 29.426 → 5.306 s | 24 → 0 | 0/0 → 24/24 |

The 100-sample pair's detail and artifact identifiers are recorded above. The
dataset-1 artifact roots are listed in its section. Thus the four-profile bounded
comparison workload is complete, **but this is not overall P5b completion**:
final documentation/index/navigation and verification remain pending, along with
broader CI, HIL, capacity/soak and peak-resource measurement. Preserve all prior
FAIL/NOT RUN history; these measurements are not saturation proof. Deployment
defaults and rollout boundaries are unchanged.

### P5b.9a — focused final Go correctness review (26/10/2026)

Reviewed the actual dirty Go diff and new Go files for cache metadata/provenance,
payload non-retention, confirmed-new-only publication, TTL and generation accounting,
single/group/standalone fallback paths, result identity validation, ACK failure and
cancellation lifecycle, independent config/metrics flags, server metrics snapshots,
and ledger timestamp write/read behavior including conservative legacy NULL handling.
No concrete ordinary correctness defect was established; no runtime fix or new
regression test was needed. Existing four-profile schema-24 native results and the
P3 configuration/metrics-off run above are historical evidence and are not
reclassified as post-review executions. No runtime change means no additional
native recheck is pending from this review.

Focused tests were run independently with `-race -count=1`: TTL exact-boundary and
non-renewal; generation drop/previous lookup; combined entry/byte caps and concurrent
publish/lookup/rotation accounting; same-key pending deliveries, DB-duplicate and
capacity fallback; single commit/ACK-failure/expiry paths; group/cache-hit,
standalone fallback, malformed/mismatched/unconfirmed result paths; and
cancellation, confirmed counters/proof, metrics-off, exactly-once release and
forced lifecycle cancellation. All passed. The test evidence is at
`/var/tmp/opencode/paper-p5b/p5b-9a-go-review/01-ttl-generation.log` through
`06-cancel-metrics-lifecycle.log`.

Required Go gates from `src/` all passed: race tests for
`internal/mqttingress`, `internal/telemetry`, `internal/config`, and `cmd/server`;
`gofmt -l .` (no files listed); `go vet ./...`; `golangci-lint run --timeout=5m`
(0 issues); and `go build ./...`. Exact command output is retained in
`07-required-race.log` through `11-build.log` under the same protected artifact
directory. This is source-level/unit verification, not a repeat of native DB,
MQTT, migration, or profile workloads; opt-in native tests are not counted as
passing evidence here. Full CI, HIL, hardware, soak/capacity, peak RSS, and live
rollout remain outside this check. P5b completion criteria are not marked complete.
