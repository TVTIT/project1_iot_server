# P6 — Dispatch nhanh và tải hỗn hợp (Local MVP)

**Ngày:** 26/10/2026 · **Trạng thái cập nhật:** independent `go-reviewer` đã **APPROVE phạm vi repair B-01/M-01/M-02/M-03**, cả bốn finding RESOLVED trong Local MVP hữu hạn. Full Python discovery vẫn còn 3 failures + 4 errors môi trường, không tuyên bố toàn bộ suite xanh. Evidence Local MVP không phải chứng nhận production. Stage 5 gốc, HIL/AM5728, full/remote CI và rollout vẫn chưa hoàn tất.

## Bổ sung sau review — producer hữu hạn, không thay production semantics

Phần này là evidence mới, tách khỏi các bảng lịch sử bên dưới. Nguồn: branch
`feat/paper-requirement`, HEAD `54e7a90` + dirty tree; artifacts trong
`review-fixes-20261026-sol/` tương đối với protected paper-p6 evidence root.
[Safe JSON](results/p6/review-fixes-20261026.json) và
[CSV](results/p6/review-fixes-20261026.csv) chỉ chứa số liệu đã khử dữ liệu nhạy cảm.
Giữ nguyên mọi FAIL cũ, đặc biệt reviewer run `ncs_4362fca74089` và reproduction
`ncs_ca4ddec72899` (cả hai exact cleanup 17/1/9). Kết luận điều tra dùng bản
owner-qualified `timeout-diagnosis/investigation-20261026-Xk7T2m/reviewed-report.md`:
59 DB commits không chứng minh chỉ 59 enqueued; 415 rate-limit thuộc toàn ingress,
không riêng A. Không loại trừ tuyệt đối delay mạng/DB/scheduling hoặc suy guaranteed
throughput từ tỷ lệ tổng hợp.

Sửa **fixture/test**, không sửa production Go dispatcher/shaper/credential/API:

| Finding | Sửa và regression mới (đã independent review RESOLVED) |
|---|---|
| M-01 | Đọc đúng `error.code`; chỉ HTTP409 + exact `command_in_progress` là busy. Conflict khác/malformed envelope vẫn rejection; không suy replay từ substring. RED hai classifier assertions → GREEN, giữ accepted/no-op/config tests. |
| M-03 | Baseline/lookup/delta dùng `(gateway_id,sensor_id)`; thiếu baseline báo lỗi. Thêm `gateway_id` vào measured/command/overlap rows (additive identity, không đổi CSV cũ). Regression A2/B5, cùng sensor ID, thêm10 mỗi bên →10/10/tổng20; RED missing helper → GREEN. |
| M-02 | Native RED trước khi sửa setup: `injections=0`, `command_in_progress`. Sensor mới version0, sentinel outbox error, injection exactly1, không busy/notify; snapshot target state/desired version/command/outbox/receipt không đổi. Native GREEN: 18 required top-level, no SKIP, schema24 current writer và 5/5 fresh/upgrade/refusal cleanup scenarios. |
| B-01 | Opt-in comparison stop callback; recovery/default callback không đổi signature. Sau sequence cuối succeeded và DB/REST configuration verified trong B burst, SIGTERM cả hai command simulators, chờ actual stopped trong budget hiện hữu; A/B vẫn chạy. Finally vẫn thử cả hai stop độc lập, idempotent, không che primary failure. Mock RED unsupported hook → GREEN; thêm actual callback-order, exception/default recovery-signature và pending/count regressions. |

Giữ 2 Gateways/4 Sensors, cùng auth/ACL/TLS, six commands/versions/keys; A60@1/s,
B12 steady@1/s +24 burst@4/s, dataset100/1, shaper2/Gateway burst4, helper A160s,
completion120s, lifecycle220s, queues/workers/pool và deployment defaults false.
Không còn phụ thuộc vòng `A complete → stop command producers`. SIGTERM **không**
có nghĩa dừng tức thì: ghi riêng stop-request và actual stopped. Pending telemetry
không xóa; sau measured A/B xong mới reopen/drain cùng volume, mỗi drain sinh thêm
1 batch đã tính vào command-Sensor totals. Không chạy hai writer trên cùng SQLite
volume. Metadata mới ghi đúng B12, run.json lưu runner monotonic origin và các mốc
commands-done/stop/stopped/measured-finished/drain; không trừ clock khác process.

Pilot wake-up100 `ncs_231fa25f6699` PASS, cleanup25/1/9, **không** dùng trong final
pairs: sau pilot bổ sung runner-origin/drain timestamp fields rồi freeze source.
Bốn arms dưới chạy tuần tự trên source frozen, không sửa giữa cặp; Docker image
IDs khác nhưng backend/Gateway RootFS layers bằng nhau trong từng cặp (read-only
image inspect, `image-parity.json`). Mọi arm six commands accepted/succeeded,
measured96 messages, exact application ACK/DB ledger/raw/temporal, cuối pending/
quarantine rows/bytes0, config verification và steady/burst overlap PASS.

| Arm / run ID | Measured raw | Command messages/raw (kể cả drain) | Total ledger/raw/temporal | CLI offered/attempts/app ACK | Overlap samples | Cleanup |
|---|---:|---:|---:|---:|---:|---|
| polling100 `ncs_7c96a0fcb410` | 9,600 | 33/1,460 | 129/11,060/129 | 129/162/129 | 238/1,159 | exact25/1/9 |
| wake-up100 `ncs_1f5c396db083` | 9,600 | 32/1,512 | 128/11,112/128 | 128/158/128 | 112/416 | exact25/1/9 |
| polling1 `ncs_34fd4958cd1d` | 96 | 33/80 | 129/176/129 | 129/159/129 | 2/11 | exact25/1/9 |
| wake-up1 `ncs_21a719fa5e83` | 96 | 30/74 | 126/170/126 | 126/153/126 | 1/5 | exact25/1/9 |

Mốc seconds từ cùng runner origin (round3):

| Arm | Commands done | First stop request | Both stopped | A/B finished | Drain start→end |
|---|---:|---:|---:|---:|---:|
| polling100 | 18.284 | 18.307 | 18.597 | 63.771 | 64.990→81.135 |
| wake-up100 | 17.532 | 17.554 | 17.858 | 63.864 | 65.039→76.643 |
| polling1 | 19.217 | 19.238 | 19.548 | 63.798 | 64.984→76.696 |
| wake-up1 | 17.537 | 17.560 | 17.866 | 63.859 | 65.117→76.892 |

Post-stop command pending rows lần lượt 11+3,12+2,2+11,11+1: được giữ và drain,
không seed success/config hoặc bỏ pending để pass. Dispatch p95 upper bucket
polling100/1=1s; wake-up100/1=0.05s. E2E p95 tương ứng1s/0.1s, mỗi histogram6 mẫu;
failed histogram count0 không là latency0. Elapsed83.691/79.179/79.210/79.466s
gồm drain; không phải thời gian phát liên tục. **Không ghép với arms lịch sử hoặc
pilot**, không suy capacity/causal throughput/fairness từ finite smoke này.

Recovery mới `ncs_58415bfa3dae`: authoritative `recovery/fixture/artifacts/p6-recovery.json`
PASS, sibling run.json vẫn PREPARED metadata. Backend start broker-offline503 →
broker healthy vẫn503 → chỉ restart backend với broker giữ nguyên ID/StartedAt →
ready200; stable pending ID/payload, SQL/REST succeeded/result/config và SQLite
2 completed receipts/config watermark match. Origin cũ mất, histogram0→0 là
missing latency, không giả latency0. Drain primary7 ACK/secondary4 ACK (thêm1 batch
mỗi drain), cuối pending/quarantine0, exact cleanup20/1/7. Không đổi credential
retry hoặc revive TTL. Recovery caller dùng default lifecycle, không opt-in stop.

Fresh technical verification: affected Python **342 PASS**; focused helper/native
Compose/provision cuối **211 PASS** (có giao nhau, không cộng). Full Go
`go test -json -race -count=1 ./...` PASS21 packages, top-level1,206 PASS/83 SKIP,
subtest events3,784 PASS/20 SKIP, fail0; **F-03 thật sự PASS trong full suite**.
Go race digitaltwin, gofmt, vet, lint0 issues, build, py_compile/shell syntax/diff
PASS. Full Python discovery **FAIL**:1,102 tests,3 failures+4 errors (certificate/
bootstrap trusted-ancestor guards ở môi trường scratch); không chmod shared
ancestors, không bypass guards. Không gán full Python xanh hoặc coverage%.

Fresh P3 native **không chạy lại**: call graph helper đổi chỉ có P6 comparison và
P6 recovery (đã fresh retest); P3 dùng lifecycle riêng trong native_compose_smoke,
file đó không sửa trong repair này. P3 microbatch/cache/metrics-off evidence
`ncs_36d7646b81a1` của independent review là **reuse lịch sử có qualification**,
không exact-new-run. HTTP/MQTT/credential native không rerun vì production/auth/
transport không đổi. Full/remote integration CI/HIL/physical sensor/rollout NOT RUN.
Original plan ngoài Git không sửa; header/checklist lịch sử còn cần owner quản lý.

### Xác minh độc lập sau repair

`go-reviewer` đã đối chiếu source và evidence mới, chạy lại 342 affected Python
tests PASS, native intent 18 required tests/no SKIP và đủ 5 scenarios cleanup
verified. Fresh independent wake-up/100 `ncs_1edca16397cd` PASS: 6 accepted/succeeded;
A60 + B36 = 96 measured messages/9.600 raw samples; tổng gồm command telemetry
128 ledger/11.093 raw/128 temporal, CLI application ACK128, outbox cuối rỗng,
exact cleanup25/1/9. Commands verified17,357s → stop request17,380s → cả hai
producer stopped17,686s → A/B complete63,711s (cùng runner origin). Budget/profile
không đổi. Lượt độc lập này không thay một arm trong cặp comparison mới.

Reviewer audit/reuse bốn final repair arms, recovery và actual full Go race JSON
(21 packages, F-03 PASS), không gọi chúng là fresh reviewer runs. Bảy lỗi full
Python discovery được tái hiện riêng, giữ nguyên FAIL do trusted ancestors;
không có blocker repair mới, không sửa source/permissions khi review. Báo cáo:
`review-fixes-independent-nmN1IZJ7/report.md` tương đối protected paper-p6 root.

## Phần dưới: evidence lịch sử trước repair vòng đời producer

Các bảng giữ nguyên để truy vết, **không** thay fresh finite-producer comparison
ở trên; trạng thái independent review của repair được ghi riêng ở phần cập nhật.

## Phạm vi và cấu hình

Đánh giá polling so với post-commit wake-up trên fixture cô lập, cùng kết quả khôi phục sau backend restart. Checkout ghi nhận branch `feat/paper-requirement`, HEAD `54e7a90f760243c7298c9d36083c8ef9669d27fb`, trạng thái **dirty** tại các lượt native. Đây là mô tả source tree khi chạy, không phải clean commit và không chứng minh mọi lượt lịch sử đều dùng đúng cây mã cuối cùng. Hai arm trong từng dataset cùng source/schema/quota và cấu hình hiệu lực, chỉ khác cờ wake-up; RootFS image được kiểm tra tương đương trong từng cặp.

Schema fixture `24:telemetry_ledger_timestamp`; quota backend **2 CPU / 4 GiB** (không phải giới hạn riêng của toàn host). Cả hai workload pair dùng `STAGE5_ENABLED=true`, command TTL 60s, scan/expiry 1s, scan batch 32, queue 32, publisher worker 1, result queue/workers 128/2, shared PostgreSQL pool max 12; polling đặt `STAGE5_WAKEUP_ENABLED=false`, wake-up đặt `true`. Cả hai cùng bật shaper (2 msg/s/Gateway, burst 4), microbatch (32 messages/500 rows/4 MiB/1s), RAM dedup (100,000 entries/64 MiB/TTL 600s), MQTT ingress queue/workers 256/4; dùng RS256/HTTPS và MQTT TLS/DynSec. **Profile cố định 32/32/1, không tuyên bố đã tuning.** Deployment defaults vẫn false cho `STAGE5_ENABLED`, `STAGE5_WAKEUP_ENABLED`, `MQTT_SHAPER_ENABLED`, `MQTT_MICROBATCH_ENABLED`, `MQTT_RAM_DEDUP_ENABLED`.

Wake-up là tín hiệu gợi ý sau khi admission transaction commit được xác nhận: channel capacity 1 coalesces thông báo theo cách không chặn; một discovery loop xử lý startup scan, timer và hint, còn periodic scan tiếp tục làm đường phục hồi bền vững. Scan batch và bounded queue tách biệt; có một publisher worker, result queue 128/hai workers; DB pool vẫn dùng chung. DB outbox là nguồn sự thật. `pending` (intent đã commit), `published` (broker PUBACK) và `succeeded` (result hợp lệ đã commit) giữ nguyên nghĩa. Authorization, domain rules, transaction/receipt, stable command bytes, result transaction/configuration write, telemetry retry bytes và public API/payload contract không đổi. Wake-up không bảo đảm đúng một lần physical apply.

## Workload và command outcomes

Bốn lượt được chấp nhận dưới đây đều có 6 command offered/admitted, 6 accepted, 6 có mẫu `D_dispatch` tới PUBACK đầu tiên, và 6 succeeded; failed/timeout/rejected/busy/no-op/replay/error đều 0. Mẫu `D_dispatch` chứng minh broker PUBACK được quan sát cho từng command, nhưng không cho biết có bao nhiêu durable transition sang `published`: metric `published_mark_successful_calls` chỉ là số lời gọi mark thành công (có thể conditional no-op), không phải số state transitions. Mỗi lượt có cùng measured telemetry feed: A 60 messages ở 1 msg/s; B 12 ở 1 msg/s steady rồi 24 ở 4 msg/s burst = **96 messages**; dataset 100 có **9,600 raw samples**, dataset 1 có **96**. Warm-up bằng 0; steady và finite burst là khoảng phát tải, sau đó drain là giai đoạn riêng, deadline 120s. Payload byte totals không có số đo được xác nhận và không được suy ra từ giá trị mặc định. Elapsed là tổng lượt và **không thể quy riêng cho wake-up**: telemetry của command Sensors được tạo theo tần số command và khác theo timing/run. `D_dispatch` là PUBACK broker đầu tiên trừ thời điểm Go HTTP entry. `D_e2e_succeeded` và `D_e2e_failed` là thời điểm confirmed result transaction commit trừ cùng Go HTTP entry, tách theo outcome. Thời gian trước Go tại proxy/auth_request và thời gian HTTPS phía client là số đo riêng, không cộng vào các histogram này.

| Fixture project ID (`run_id`) / run label / mode | Dataset | Elapsed s | D_dispatch n; p50/p95/p99 (s) | D_e2e succeeded n; p50/p95/p99 (s) | Commit→discovery n; p50/p95/p99 (s) | Overlap steady/burst samples | Cleanup |
|---|---:|---:|---|---|---|---:|---|
| `ncs_1b1c1b98df92` / `polling-100-fixed-20261026` / polling | 100 | 129.404 | 6; 1/1/1 | 6; 1/1/1 | 6; 1/1/1 | 253 / 1,175 | exact 25 containers / 1 network / 9 volumes |
| `ncs_137941e15481` / `wakeup-100-fixed-20261026` / wake-up | 100 | 107.195 | 6; 0.025/0.025/0.025 | 6; 0.1/0.1/0.1 | 6; 0.001/0.001/0.001 | 108 / 427 | exact 25 / 1 / 9 |
| `ncs_6f53325c5b07` / `polling-1-fixed-20261026` / polling | 1 | 110.844 | 6; 1/1/1 | 6; 1/1/1 | 6; 1/1/1 | 2 / 12 | exact 25 / 1 / 9 |
| `ncs_67065e1a7d09` / `wakeup-1-fixed-20261026` / wake-up | 1 | 201.747 | 6; 0.025/0.025/0.025 | 6; 0.1/0.1/0.1 | 6; 0.001/0.005/0.005 | 1 / 5 | exact 25 / 1 / 9 |

Các percentile là nearest-rank **ước lượng theo cận trên bucket**; command histogram chỉ có sáu mẫu mỗi arm, nên dữ liệu thưa và bucket có thể rộng. Chúng không phải latency chính xác, giới hạn xác định hay bằng chứng đạt mục tiêu 57/62,5 ms. Arm wake-up có bucket dispatch/discovery quan sát thấp hơn trong lượt này, nhưng không chứng minh capacity hoặc bảo đảm latency. Vì command-Sensor telemetry, elapsed và tổng output thực tế biến thiên theo sampling/timing, so sánh latency buckets không phải so sánh cùng tổng tải đã thực hiện và không chứng minh quan hệ nhân quả về capacity. Số overlap dùng `measured_at` gốc trên cùng host chỉ xác nhận hai luồng cùng xảy ra theo thời gian, không dùng để trừ latency giữa process. Overlap dataset 1 thấp là giới hạn workload hữu hạn.

Độ trễ bên trong broker không có trong dữ liệu; thời gian publish đo tại Go không thay thế nó. Không có timing SQL statement hoặc DB-pool acquire riêng; pool snapshots hiện có là **toàn pool**, không thể quy riêng cho control. Resource snapshots là mẫu rời rạc, không phải peak RSS hay profile liên tục (nguồn: mỗi run `resources.csv`; pool counters từ `metrics.csv`, các snapshot trước/sau). Không có payload-byte total được đo/đưa vào acceptance. Các histogram `discovery`, `queue_wait`, `publish`, `prepare_db_recheck`, `published_mark` được xuất riêng; không có điểm đo DB-acquire/broker-internal để phân rã thêm.

`published_mark_successful_calls` chỉ đếm lời gọi `MarkControlPublished` trả nil, kể cả conditional no-op khi result đã terminal; nó **không** chứng minh durable state transition sang `published`, càng không chứng minh command execution thành công. Thành công thực thi được xác nhận qua result transaction và bằng chứng DB/REST native.

Profile flags của từng arm được lưu trong `summary.json`: control `STAGE5_ENABLED=true`; `MQTT_SHAPER_ENABLED=true`; `MQTT_MICROBATCH_ENABLED=true`; `MQTT_RAM_DEDUP_ENABLED=true`; chỉ `STAGE5_WAKEUP_ENABLED` thay đổi trong mỗi cặp polling/wake-up. Mỗi workload window ghi `missing_origin`, `missing_dispatch`, `missing_result`, `missing_commit_to_discovery`, `missing_commit_to_enqueue`, timeline overflow và pending origins đều bằng 0 trước/sau; histogram windows available, overflow 0 trong cả bốn accepted run.

| Đoạn thời gian (p95 cận trên bucket) | Polling dataset 100 (n / s) | Wake-up dataset 100 (n / s) | Polling dataset 1 (n / s) | Wake-up dataset 1 (n / s) |
|---|---:|---:|---:|---:|
| HTTP admission | 6 / 0.025 | 6 / 0.025 | 6 / 0.025 | 6 / 0.025 |
| Discovery scan query | 126 / 0.005 | 116 / 0.005 | 107 / 0.005 | 211 / 0.005 |
| Publisher queue wait | 6 / 0.0001 | 6 / 0.0001 | 6 / 0.0001 | 6 / 0.0001 |
| Prepare DB/recheck | 6 / 0.025 | 6 / 0.005 | 6 / 0.025 | 6 / 0.005 |
| Publish call tại backend | 6 / 0.005 | 6 / 0.005 | 6 / 0.005 | 6 / 0.005 |
| Published mark call | 6 / 0.025 | 6 / 0.005 | 6 / 0.01 | 6 / 0.025 |
| Result queue wait | 36 / 0.0001 | 36 / 0.005 | 36 / 0.0001 | 36 / 0.005 |
| Result processing | 36 / 0.01 | 36 / 0.025 | 36 / 0.01 | 36 / 0.025 |
| Result receive→confirmed commit | 6 / 0.01 | 6 / 0.025 | 6 / 0.01 | 6 / 0.05 |
| Commit→first discovery / enqueue | 6/6 / 1 / 1 | 6/6 / 0.001 / 0.001 | 6/6 / 1 / 1 | 6/6 / 0.005 / 0.005 |

Các stage có định nghĩa và nguồn đo riêng: result receive→commit chỉ ghi accepted result; result queue/process histogram tính các result events trong process; timeline commit→discovery/enqueue bắt đầu từ admission commit đã xác nhận. Tất cả p95 là ước lượng cận trên bucket; vectors đầy đủ trong JSON. Không có DB pool acquire/SQL timing riêng hoặc latency bên trong broker. Pool/resource quan sát là snapshot toàn pool và lấy mẫu rời rạc; không có thay đổi tuning vì profile batch/queue/workers 32/32/1 và pool 12 được giữ cố định, không thấy bằng chứng nghẽn yêu cầu tăng. Đây không phải kết luận utilization hay peak resource thấp.

| Run label | Measured telemetry DB (ledger/raw/temporal) | Command-Sensor telemetry DB (ledger/raw) | Total DB delta (ledger/raw/temporal) | Ingress Received / Committed / SamplesNew / ACKPublished | CLI offered / publish attempts / application ACK | Shaper RateLimited / QueueFull |
|---|---:|---:|---:|---:|---:|---:|
| `polling-100-fixed-20261026` | 96 / 9,600 / 96 | 159 / 2,063 | 255 / 11,663 / 255 | 497 / 255 / 11,663 / 257 | 255 / 497 / 255 | 240 / 0 |
| `wakeup-100-fixed-20261026` | 96 / 9,600 / 96 | 127 / 1,815 | 223 / 11,415 / 223 | 401 / 223 / 11,415 / 223 | 223 / 401 / 223 | 178 / 0 |
| `polling-1-fixed-20261026` | 96 / 96 / 96 | 126 / 452 | 222 / 548 / 222 | 404 / 222 / 548 / 223 | 222 / 404 / 222 | 181 / 0 |
| `wakeup-1-fixed-20261026` | 96 / 96 / 96 | 314 / 1,211 | 410 / 1,307 / 410 | 811 / 410 / 1,307 / 411 | 410 / 811 / 410 | 400 / 0 |

Các đại lượng được đối chiếu theo đúng đơn vị: ledger = temporal = số message committed = CLI offered = application ACK; raw = số sample mới. `ACKPublished` có thể gồm ACK cho duplicate; CLI publish attempts tính cả retry và không phải số message trên wire broker. Command Sensors tạo lượng telemetry khác nhau theo tần số sampling/timing của command; vì vậy khác biệt tổng ingress/DB/ACK/retry/shaper và elapsed **không được quy hoàn toàn cho wake-up**. Telemetry outbox hữu hạn được drain trong cả bốn lượt accepted; tổng hợp đã khử dữ liệu nhạy cảm nằm trong JSON/CSV.

## Recovery, findings và validation

**Recovery được yêu cầu — accepted run `ncs_ce2eb3785586`.** Tình huống backend khởi động khi broker chưa sẵn sàng làm credential startup readiness false/503. Broker được đưa lên nhưng readiness vẫn 503. Giữ broker liên tục healthy, restart **chỉ backend**; sau đó `/readyz` trả 200, pending command bền vững hoàn tất `succeeded` với cùng ID/payload hash, result được commit, REST và DB `reported.configuration` khớp, SQLite receipt/config watermark đã dừng được xác minh, và bounded outbox drain hoàn tất. Cleanup chính xác 20 containers / 1 network / 7 volumes. `p6-recovery.json` là nguồn acceptance; `run.json` của recovery mode chỉ là metadata `PREPARED_NOT_EXECUTED`. Toàn bộ recovery runs 01–10 (`ncs_aec9ac973eb4`, `ncs_3391d182ef5e`, `ncs_b25e7a6321f2`, `ncs_0bce421e86e0`, `ncs_55a9e6462342`, `ncs_1ab82d60503b`, `ncs_d16f8c1cd72c`, `ncs_999c5c076066`, `ncs_0779d01a0a4a`, `ncs_d7206a335683`) vẫn là FAIL; hai lần restart-with-broker-healthy trước acceptance (`ncs_e956643b91ce`, `ncs_9ad7d4dce652`) cũng giữ FAIL. Đây là hành vi trong cấu hình credential API bật/startup prerequisite; **không** chứng minh mọi MQTT transport tự reconnect hoặc có readiness retry tự động, và không đổi credential semantics. Receipt không chứng minh số physical apply hay exactly-once. Sau restart origin không còn: histogram count 0, `pending_origins=1`; đó là missing latency, không phải latency 0.

| Finding | Trạng thái / bằng chứng |
|---|---|
| F-01 cleanup independence/fail-closed | Đã sửa hẹp; Python regression tiêm lỗi cleanup/setup; credential native xác nhận cleanup bình thường PASS và resource thuộc fixture đã được dọn. Không tuyên bố native fault-injected cleanup. |
| F-02 current writer/schema | Đã sửa lifecycle fixture; giữ assertions/upgrade/refusal lịch sử của schema 23; current writer chạy trên schema 24. Native intent có 18 required top-level tests PASS, không SKIP; 5 scenarios cleanup verified. Giữ nguyên FAIL lịch sử khi current writer chạy schema 23. |
| F-03 flaky metrics assertion | Retry test được đồng bộ bằng barriers; full local Go race mới tại P6.9 PASS: 21 packages, 1,206 top-level PASS / 83 SKIP; tính cả subtest events là 4,990 PASS / 103 SKIP / 0 FAIL (events, không phải unique tests). Giữ FAIL lịch sử của full suite. |

Validation evidence reused từ P6.9 slice A: affected 6-package Go race, `gofmt`, `go vet ./...`, `golangci-lint` 0 issues, `go build ./...`, full local Go race; Python 205 tests, `py_compile`, shell syntax, diff check PASS. P6.9 slice B: fresh P3 regression `ncs_8c50fcbafb15` PASS với microbatch + RAM dedup, old/new/raw preservation, result-driven configuration and one metrics-off command (polling profile; **không** phải native wake-up metrics-off); exact cleanup 17/1/7. Native intent 18 required no-SKIP/schema 24 và credential/MQTT evidence được tái sử dụng theo phạm vi/source của handoff, không gán là mới hoặc exact-final-tree. Unit barrier/hint/queue tests chứng minh bounded/coalescing/fallback trên fake repository, **không** phải native queue saturation. Remote/full integration CI, HIL/AM5728, physical sensor và live rollout NOT RUN. Không đưa ra coverage % hay exhaustive-proof claim.

Các lệnh final evidence được ghi trong handoff P6.9A gồm `go test -race -count=1 ./internal/mqttcontrol ./internal/digitaltwin ./internal/metrics ./internal/config ./internal/httpserver ./cmd/server`, `go test -json -race -count=1 ./...`, `gofmt -l .`, `go vet ./...`, `golangci-lint run --timeout=5m`, `go build ./...`, và `PYTHONPATH=scripts/tests python3 -m unittest -v scripts/tests/test_native_compose_smoke.py scripts/tests/test_task266_provision.py scripts/tests/test_paper_p6_support.py`. P6.9B native P3 invocation: `python3 scripts/tests/native_compose_smoke.py --prepare <new-fixture> --paper-p3 --p5a-control-regression --p5b-control-regression`. Các lần native/schema/recovery được phân biệt là fresh hoặc reuse ở trên, không chạy lại trong lát viết báo cáo này.

## Lệnh tái lập và giới hạn vận hành

Trong repo root, tạo **run directory mới** và log riêng cho mỗi lần chạy; fixture tự quản lý tài nguyên theo path. Dưới đây là cú pháp profile của runner; chỉ dùng khi chủ động muốn chạy lại, không tái sử dụng artifact/run directory cũ:

```sh
RUN_ROOT="/var/tmp/opencode/paper-p6/manual-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_ROOT"
python3 scripts/tests/native_compose_smoke.py --prepare "$RUN_ROOT/polling-100/fixture" --paper-p6 --dispatch-mode polling --dataset 100 >"$RUN_ROOT/polling-100.log" 2>&1
python3 scripts/tests/native_compose_smoke.py --prepare "$RUN_ROOT/wakeup-100/fixture" --paper-p6 --dispatch-mode wake-up --dataset 100 >"$RUN_ROOT/wakeup-100.log" 2>&1
```

Recovery là mode riêng, path mới; xác nhận `fixture/artifacts/p6-recovery.json` làm acceptance source, không dùng `run.json` làm PASS:

```sh
RUN_ROOT="/var/tmp/opencode/paper-p6/recovery-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_ROOT"
python3 scripts/tests/native_compose_smoke.py --prepare "$RUN_ROOT/fixture" --paper-p6 --dispatch-mode wake-up --dataset 100 --p6-recovery-smoke >"$RUN_ROOT/recovery.log" 2>&1
```

Tắt `STAGE5_WAKEUP_ENABLED` để quay về polling-only; startup/periodic scan vẫn hoạt động. **Không** rollback schema 24. Giữ deployment flags false và không rollout/live enablement. Limitations chung (DB/broker cùng host, sampled resources không phải peak RSS, queue/TTL, finite workload, missing origin và các claim chưa đo) thuộc [backlog chung](../dynsec/limitations-and-backlog.md); không tạo backlog cạnh tranh ở đây.

Sanitized repository-managed rollups: [`results/p6/summary.json`](results/p6/summary.json) và [`results/p6/summary.csv`](results/p6/summary.csv). Raw logs/artifacts protected, không được copy vào repository.

### Chỉ mục nguồn artifact an toàn và lịch sử FAIL

Các đường dẫn sau tính tương đối từ thư mục bằng chứng được bảo vệ `paper-p6/`; chỉ trích xuất các trường whitelist vào kết quả Git-managed, không công bố logs/private fixture. Với từng lượt accepted, `run.json` giữ fixture `run_id` thực và `metrics.csv` giữ histogram window/export:

| Run label / project ID | Artifact nguồn |
|---|---|
| `polling-100-fixed-20261026` / `ncs_1b1c1b98df92` | `p6-comparison/polling-100-fixed-20261026/fixture/artifacts/{run.json,metrics.csv}` |
| `wakeup-100-fixed-20261026` / `ncs_137941e15481` | `p6-comparison/wakeup-100-fixed-20261026/fixture/artifacts/{run.json,metrics.csv}` |
| `polling-1-fixed-20261026` / `ncs_6f53325c5b07` | `p6-comparison/polling-1-fixed-20261026/fixture/artifacts/{run.json,metrics.csv}` |
| `wakeup-1-fixed-20261026` / `ncs_67065e1a7d09` | `p6-comparison/wakeup-1-fixed-20261026/fixture/artifacts/{run.json,metrics.csv}` |
| Recovery accepted / `ncs_ce2eb3785586` | `restart-with-broker-healthy/run-20261026-ZSkszu/fixture/artifacts/p6-recovery.json` (authoritative; sibling `run.json` là `PREPARED_NOT_EXECUTED`) |

Lịch sử lỗi giữ riêng, không tính như accepted arm. Lượt comparison đầu `polling-100-20261026` FAIL vì validator so sánh nhầm message ledger (247) với raw sample count (11,545), hai đơn vị không tương đương; corrected invariant là ledger = temporal = ingress committed = CLI offered = application ACK, còn raw = samples. Những baseline P6 trước đó có failure khác: giả định cố định 100 raw samples/message cho commandSensor và các accounting mismatch cụ thể, như ghi tại `p6-baseline/handoff.md`; không nhập chung nguyên nhân với lỗi dimensionality của comparison đầu. Chỉ bốn lượt `*-fixed-20261026` trong bảng trên là accepted. Command-Sensor raw/message outputs và elapsed khác theo sampling/timing từng arm; do đó percentile buckets không phải bằng chứng matched realized total load hoặc capacity causal proof. Không sửa lịch sử FAIL thành PASS.
