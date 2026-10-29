# P8 — Sổ tay hiện hành: công cụ tải, pilot và bàn giao (P8.14)

**Trạng thái hiện tại: `LOCAL_TOOLING_COMPLETE_SOURCE_QUALIFIED`.** Đã hoàn tất bàn giao tài liệu P8.14 và safe results. Pilot local, kiểm tra local cuối và database restore có giới hạn đã được ghi nhận; không nội dung nào là nghiệm thu paper chính thức, chứng nhận capacity, phê duyệt production hay HIL. Raw lanes giữ trạng thái riêng và `full_p8_accepted=false`. Xem [báo cáo nghiệm thu P8](paper-p8-experiments-and-acceptance.md), [overview an toàn](results/p8/overview.json), [checkpoint](results/p8/checkpoint.json) và [summary restore](results/p8/recovery-summary.json). Các ghi chú source lịch sử bên dưới được giữ nguyên làm lịch sử, không phải hướng dẫn trạng thái hiện tại.

## Bắt đầu từ đây

1. Chỉ dùng handbook này với fixture cô lập, thuộc quyền quản lý của lượt chạy và thư mục run private mới. Không dùng deployment `.env`, DB/broker/volume dùng chung hoặc production credentials.
2. Go module nằm trong `src/`; chạy Python runner từ repository root. Xem selector thực tế bằng `python3 scripts/tests/native_compose_smoke.py --help`; flags của runner không thể thay thế public preparation manifest.
3. Build binary theo OS/architecture thực của host đích. Go được pin trong `src/go.mod` (source này dùng 1.27.3); Docker image được pin bằng fixture lock/digest. Không mặc định Linux binary chạy được trên macOS/Windows. Môi trường P8 đã kiểm tra là Linux có Docker, Python 3, Go, OpenSSL và fixture dependencies.
4. Deployment defaults tiếp tục tắt `WEBSOCKET_ENABLED`, `STAGE5_ENABLED`, `STAGE5_WAKEUP_ENABLED`, `MQTT_SHAPER_ENABLED`, `MQTT_MICROBATCH_ENABLED` và `MQTT_RAM_DEDUP_ENABLED`.
5. Bắt đầu từ các recipe local đã chọn bên dưới. Công việc official/repeat/high-load là opt-in và hiện NOT RUN. Không truyền `--run` hoặc native selector chỉ để kiểm tra đóng gói.

### Local tool checks and selected recipes

```sh
# From repository root; private, new output locations only.
PRIVATE_RUN_ROOT="${PRIVATE_RUN_ROOT:?set this to your protected scratch root}"
python3 scripts/tests/native_compose_smoke.py --help
python3 scripts/paper_p8.py --help
python3 scripts/paper_p8_e5.py --help
mkdir -p "$PRIVATE_RUN_ROOT/p8/my-preflight"
(cd src && go build -o "$PRIVATE_RUN_ROOT/p8/my-preflight/paperload" ./cmd/paperload)
# Set ACTUAL_GO_MANIFEST to a private manifest produced by an existing fixture helper.
python3 scripts/paper_p8.py --binary "$PRIVATE_RUN_ROOT/p8/my-preflight/paperload" \
  --preflight --manifest "$ACTUAL_GO_MANIFEST" \
  --output-dir "$PRIVATE_RUN_ROOT/p8/my-preflight/no-run-output"
```

Các ví dụ được lưu trong repository là **public preparation manifests**, không phải private credentials, environment file bền vững của Gateway simulator, hay Go `paperload` manifest/multi-process `run-spec` mà `scripts/paper_p8.py --run` sử dụng. Public manifest mô tả ý định thực nghiệm; fixture helper tạo role manifest thực, private env file, endpoint/CA input và run-spec. Không tự chuyển đổi schema hoặc đặt JWT/password trong public manifest. `--preflight` kiểm tra một Go manifest có sẵn mà không mở network hay ghi file; run-spec được helper/driver và tests kiểm tra, không có một CLI mode riêng chưa được tài liệu hóa. Không đưa public preparation JSON vào vị trí Go manifest.

Chọn `GOOS`/`GOARCH` theo máy đích B và kiểm tra `go tool dist list`; không suy luận binary Linux chạy được trên macOS/Windows. Ví dụ đã kiểm tra trong môi trường Linux: generator `paperload` native Linux/amd64; Gateway ARMv7 được cross-build bằng Go 1.27.3 như dưới đây. Cross-build chỉ chứng minh biên dịch, không phải thực thi trên bo mạch vật lý.

```sh
# Go generator trên Linux/amd64.
(cd src && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -o "$PRIVATE_RUN_ROOT/bin/paperload-linux-amd64" ./cmd/paperload)
# Gateway ARMv7: cross-build; không khẳng định đã chạy trên AM5728.
(cd src && CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build \
  -o "$PRIVATE_RUN_ROOT/bin/gateway-linux-armv7" ./cmd/gateway)
```

Recipe native local dùng một lần (luôn dùng thư mục private mới; Docker chỉ tác động lên resource thuộc fixture của lượt chạy):

```sh
mkdir -p "$PRIVATE_RUN_ROOT/p8"
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e1" --paper-p8-e1
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e2" --paper-p8-e2
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e3-poll" --paper-p8-e3 polling
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e3-wake" --paper-p8-e3 wakeup
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e4-on" --paper-p8-e4 on
python3 scripts/tests/native_compose_smoke.py --prepare "$PRIVATE_RUN_ROOT/p8/my-e4-off" --paper-p8-e4 off
# E5 has a separate context/reference sequence; see E5 section.
```

Các selector này đã chạy local dưới dạng pilot giới hạn, không phải ma trận official. Dùng CLI `--help` để xem arguments được hỗ trợ và validation hiện hành; implementation cuối không hỗ trợ flags tự suy đoán như `--prepare-only` trên các fixture selector này. Không dùng lại output directory. Recovery chỉ dùng `--paper-p8-recovery` với E5 optimized input context mới như tài liệu mô tả; kết quả được tổng hợp riêng bên dưới.

## Runbook A/B (ranh giới thực tế)

**A (fixture/server operator)** quản lý fixture TLS/Auth/Nginx/Go/Postgres/Mosquitto dùng một lần, cấp experiment identity với quyền tối thiểu, thiết lập collection và xác minh DB/REST. **B (máy sinh tải)** chỉ nhận client credential/config theo role và chạy generator Python/Go; không được nhận DB URL/credential, platform-admin JWT, service-role key, Docker socket, broker manager credential, SSH vào A hoặc quyền điều khiển fixture. Operator A có thể dùng fixture helper hiện có; repository không có control-plane từ xa tổng quát hay orchestration một lệnh xuyên host. Public prep manifest và private credential bundle được chuyển riêng qua kênh mã hóa có xác thực. Giữ private file mode 0600 trong thư mục mode 0700, dùng relative path trong private bundle được duyệt; tuyệt đối không đặt secret vào argv, shell history, URL, log hoặc Git.

Các native fixture command hiện có chỉ thao tác fixture local, không cung cấp CLI provision/collect/cleanup cho A/B từ xa. Operator A phải tự provision B bằng private input và Go manifest/run-spec do helper tạo, sau đó chỉ chạy workload child có giới hạn. A thu thập/xác minh kết quả và thực hiện cleanup đúng resource đã sở hữu. Pilot A/B trên máy vật lý vẫn **NOT RUN**. Preparation/preflight không thay thế qualification TLS, DNS, routing, firewall, MTU hoặc resource khi chạy từ xa.

Khi A đã chuyển an toàn **Go manifest thực, run-spec và Gateway env file theo role** sang B, bounded supervisor hiện có hỗ trợ preflight và thực thi (tối đa 16 process thuộc lượt chạy; không chuyển tiếp shell flags tùy ý cho child process). Path trong run-spec được tính tương đối với file run-spec, trừ khi là absolute. `run-spec.json` có `paperload: [{"manifest":"...","output_dir":"...","role":"viewer|telemetry|control|auto","ready_timeout_seconds":30}]`, `gateways: [{"env_file":"...","role":"executor|durable-telemetry","ready_timeout_seconds":30}]` tùy chọn, và `stop_timeout_seconds` tùy chọn (≤60). Với pilot có mọi profile value là `pilot`, run-spec cho phép tối đa hai durable Gateway process. Preflight kiểm tra từng paperload manifest; không đọc Gateway env file, không mở network connection và không khởi động child.

```sh
# On B, in its protected private bundle; A supplies these files separately.
python3 scripts/paper_p8.py --binary "$PRIVATE_BUNDLE/bin/paperload" \
  --gateway-binary "$PRIVATE_BUNDLE/bin/gateway" \
  --run-spec "$PRIVATE_BUNDLE/run-spec.json" --preflight
# Only after A approves this bounded workload and B's credential/CA/network preflight:
python3 scripts/paper_p8.py --binary "$PRIVATE_BUNDLE/bin/paperload" \
  --gateway-binary "$PRIVATE_BUNDLE/bin/gateway" \
  --run-spec "$PRIVATE_BUNDLE/run-spec.json" \
  --run-artifact "$PRIVATE_RESULTS/run.json" --run
# For an official Go manifest only, pass --official-opt-in to both preflight and run.
```

`run` bắt buộc có `--run-artifact`; official manifest yêu cầu opt-in tường minh `--official-opt-in`. Không đặt secret trong run-spec: các giá trị nhạy cảm chỉ nằm trong env/identity file được bảo vệ với permission giới hạn theo role. B gửi safe `run.json`, counter và digest manifest cho A qua kênh mã hóa đã duyệt; A xác minh DB/REST phía fixture và cleanup resource owned. P8 E1–E5 selector driver chỉ chạy trong fixture local, không thể chạy trọn topology A/B từ xa đầu-cuối và không thay thế quy trình chia vai trò thủ công này.

Preflight network/security: public CA chain và SAN/hostname phải khớp HTTPS/WSS và MQTT-TLS endpoint đã cấu hình; không tắt TLS verification. Nếu A nằm trong mạng private, dùng SSH local-forward do operator tạo và chỉ bind loopback; giữ nguyên kiểm tra certificate SAN (hostname endpoint phải được resolve qua mapping được kiểm soát tới tunnel). Tunnel làm thay đổi network path và phải ghi vào run metadata; không tuyên bố topology giống A/B kết nối trực tiếp. Không expose public listener bằng P7 HTTP-only profile. Nginx exact `/v1/ws` mặc định trả 404; P7 overlay chỉ HTTP, không phải WSS profile hướng Internet.

Resource limits apply to measured roles separately: record A backend/container quota and DB/broker resource snapshots, and B generator CPU/RAM/network/FD and scheduling lag. Colocated generators are useful for functional pilot only (`generator_colocated=true`), not capacity. Stop if target-vs-actual rate diverges, scheduling lag grows, limits/pending/disk/drain bounds approach, or a child fails. Stop generation, bounded drain, join all workers, preserve pending/outbox/journal, collect partial evidence, and clean only exact fixture-owned IDs. Never delete pending rows to make drain green. Failed cleanup is an independent finding, not a successful cleanup.

### Workload matrix and interpretation

| Pilot | Current local selector | What it exercises | Main checks / restraint |
|---|---|---|---|
| E1 | `--paper-p8-e1` | WS viewer/subscription plus MQTT telemetry, two namespaces | expected vs received/duplicate/wrong-namespace, close/recheck and exact cleanup; pilot-scale only |
| E2 | `--paper-p8-e2` | shaper/microbatch telemetry, sample profiles | offered/admitted/committed/raw/ACK, DB/COPY counters and drain; not capacity |
| E3 | `--paper-p8-e3 polling|wakeup` | fresh command lifecycle with background telemetry | accepted/succeeded/result/configuration/receipt and measured origin availability; command overlap evidence must be event-based, not inferred from planned time |
| E4 | `--paper-p8-e4 on|off` | unique telemetry, replay/dedup, warm/cold persistence helpers | unique bytes/identity, duplicate no-new-data, DB/cache counters and application ACK; synthetic load journal is not durable Gateway outbox evidence |
| E5 | `scripts/paper_p8_e5.py` / native E5 selector | optimized reference and one-mechanism ablations | arm activation, matching image/source/context, counters and cleanup; see next section |

The source plan’s official rungs (E1: 32→64→128→512→1,000→2,000→5,000→10,000 viewers; E2: 100/1,000/5,000/20,000 offered msg/s; E3: 30 commands/arm; E4: 1,000 unique plus 10% replay and optional real 600s TTL; E5: ≥3 independent repeats) remain selected **opt-in plans**, not executed results. Apply explicit opt-in and finite duration/pending/disk/drain bounds. Do not execute them as part of this handoff.

Cú pháp thực tế của E5 driver:

```sh
python3 scripts/paper_p8_e5.py --root "$PRIVATE_RUN_ROOT/p8/my-e5" --mode pilot \
  --repeat 1 --seed 805 --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan
# Preparation plan only (does not launch workloads); official mode enforces >=3 repeats.
python3 scripts/paper_p8_e5.py --root "$PRIVATE_RUN_ROOT/p8/official-e5-plan" \
  --mode official --repeat 3 --seed 805 \
  --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan --prepare-only
```

Default is preparation only. Native execution additionally requires explicit `--run`; it runs arms sequentially. Each repeat has its own unchanged context and seed; optimized must run first, and each ablation reuses immutable images from its successful optimized reference via `--reference`. The updated driver aborts a matrix on failed run; external ablation needs matching context and reference. Cooldown is 0 in this matrix. Do not pool repeat samples: report per-run first, then median and min–max range across independent comparable runs. Never fabricate a pooled p99, confidence interval, or repeat count. The official matrix/repeats were NOT RUN.

Ví dụ chạy một optimized arm trước, rồi chạy một ablation riêng với cùng context và reference artifact thành công. Thay placeholder bằng artifact vừa tạo trong thư mục private; không dùng lại output directory đã có.

```sh
python3 scripts/paper_p8_e5.py --root "$PRIVATE_RUN_ROOT/p8/e5-opt" \
  --mode pilot --repeat 1 --seed 805 --arm optimized --run
python3 scripts/paper_p8_e5.py --root "$PRIVATE_RUN_ROOT/p8/e5-shaper-off" \
  --mode pilot --repeat 1 --seed 805 --context "$OPTIMIZED_CONTEXT" \
  --reference "$OPTIMIZED_REPORT" --arm shaper_off --run
```

Để xuất safe comparison cho sáu report đã có, CLI thực tế là `e5-summary`; đủ sáu file và output directory phải được cung cấp rõ ràng. Đây chỉ là xuất summary, không chạy tải:

```sh
python3 scripts/paper_p8_comparison.py e5-summary \
  --optimized "$OPTIMIZED_JSON" --shaper-off "$SHAPER_OFF_JSON" \
  --microbatch-off "$MICROBATCH_OFF_JSON" --ram-dedup-off "$RAM_DEDUP_OFF_JSON" \
  --polling "$POLLING_JSON" --fanout-scan "$FANOUT_SCAN_JSON" \
  --output-dir "$PRIVATE_RUN_ROOT/p8/e5-safe-summary"
```

### Metrics, aborts and missing evidence

For each run keep the recorded source revision/dirty fingerprint, exact flags, binary/image digests, quota, seed, workload/context fingerprints, phase windows, counters, resource snapshots, cleanup counts and logical artifact ID. A source-mapped aggregate is not one identical dirty source tree. Context SHA unavailable in raw/public summary stays unavailable; private context verification is separate.

Percentiles must come from retained per-run histograms/samples using the stated estimator and include overflow counts. If a histogram bucket saturates/overflows, report the bound as overflow/censored; do not claim an exact percentile. Missing origins, process-instance mismatch/reset, absent event fields, collection gaps, or invalid windows are `null`/`INCONCLUSIVE` with reason, never zero. Initial command timeline snapshots taken before issuing commands are not evidence of zero command-phase samples. One-way latency across hosts is unavailable unless synchronized clocks and measured error bounds are documented; do not subtract unrelated monotonic clocks. P8 accepted telemetry counters with absent command-window fields remain INCONCLUSIVE, not 0 ms or a replacement RTT.

For paired cost comparison, define `Γ_red = 100 × (C_reference − C_optimized) / C_reference` for a named same-window counter only. E5 microbatch-off observed initial copy/commit/begin attempts 20 vs optimized 16 (20% reduction for those initial operation counters only). RAM-dedup-off had 10 warm begin/commit attempts vs 0 optimized (100% reduction in named warm counter only). Neither supports all-SQL, storage, RSS, latency, or throughput improvement claims. Compare per-run before aggregation, only where arm activation and images/context match.

Generator bottleneck checks: compare target and actual publish rate, scheduler lag, retries, locally queued/pending counts, worker utilization, CPU/RAM/network/FD on B; if actual fails target, classify generator-limited/inconclusive and do not attribute server limits. Record worker/process count and tunnel/topology identically across paired arms. Unexpected stop: stop production, bounded drain, join and verify every owned child; preserve SQLite/outbox/pending rows and private journals. Mark run `PARTIAL` or `FAILED` with remainder and cleanup status. No automatic retry of TLS/auth/authorization failures as PASS.

### Board / AM5728 qualification checklist (NOT RUN)

This is a future hardware checklist, not a result: (1) record board revision, OS/toolchain/Go build target and binary digest; (2) provision TLS CA/SAN and least-privilege gateway credential, prove denied invalid credential; (3) start Gateway and verify readiness; (4) send uplink and verify backend commit plus application ACK; (5) issue a fresh authorized command and verify published is distinct from succeeded; (6) collect runtime result and confirm `reported.configuration` only after verified result; (7) stop/reopen Gateway SQLite and verify receipt, sequence/watermark and pending payload survive; (8) repeat with a physical sensor only when attached and separately identify synthetic simulator values vs physical measurements; (9) preserve logs/digests and exact owned cleanup. AM5728/HIL and physical sensors are NOT RUN. This does not make Stage 5 complete; remote CI/Stage 5 rollout dependencies remain open.

## Checkpoint P8.14 và bằng chứng hiện có

Xem [báo cáo P8](paper-p8-experiments-and-acceptance.md), [JSON recovery an toàn](results/p8/recovery-summary.json) và [overview P8](results/p8/overview.json). E3 có pilot low-load polling PASS và hai run mixed-load polling/wakeup PASS; cả hai mixed-load report đều xác nhận overlap theo runner, bốn receipt, hai configuration, pending sau drain bằng 0 và cleanup 12/1/5. `D_dispatch`/`D_e2e` server-origin là null/unavailable, không được mô tả là đã đo. E5: sáu native driver run đóng băng PASS, mỗi run cleanup chính xác (12 containers/1 network/5 volumes), năm cặp strict-comparable; mọi raw record vẫn `full_p8_accepted=false`. Mỗi arm có 20 unique messages/2.000 raw samples, 10 warm replay; tổng 20 WS events trên hai viewer (10 mỗi viewer), bốn receipt và hai configuration đã xác minh. Fanout scan ghi indexed 20 so với scan 40 evaluation và viewer đúng, nhưng kích hoạt helper vẫn INCONCLUSIVE (`fanout_viewer_readiness_unverified`; thiếu readiness flag). Native command-window timeline vẫn INCONCLUSIVE (`counter_field_missing_events`), không phải latency bằng 0. Một pilot colocated không chứng minh capacity; resource snapshots thưa; overlap storm E3 là bằng chứng riêng, không thuộc lượt E5 này.

P8.12 local source/CI gates and P8.13 bounded DB restore are source-qualified in report. Recovery restored source/restore row counts and content hashes, reopened one committed/unACKed SQLite payload, verified application ACK/drain and access denials; exact fixture cleanup 14 containers/1 network/5 volumes, all four children joined. This was **database restore in the same fixture-owned PostgreSQL cluster**, not whole-platform/HA backup. Earlier original control runner remains PARTIAL; do not rewrite it COMPLETE.

Còn NOT RUN: pilot A/B vật lý từ xa; remote GitHub CI; ≥3 repeats chính thức, high-load/capacity/soak, TTL thực 600s, AM5728/HIL, cảm biến vật lý, live rollout; các CI legacy supervisor/reloader đang DEFERRED. Full Python discovery không chạy lại; failures/errors trước đây vẫn là lịch sử, không phải PASS. Xem [backlog trung tâm](../dynsec/limitations-and-backlog.md).

## Historical P8.0 planned introduction (preserved)

The following text begins the original P8.0 planning record. Its statements that P8 had not run were accurate at that checkpoint and are superseded only by the dated/source-qualified checkpoint above; historical failures and NOT RUN statuses are not rewritten.

# P8 — Hợp đồng workload và đo lường (P8.0)

**Trạng thái: planned input only.** P8.0 khóa nguồn và đặt hợp đồng dự kiến; không có tải nào được chạy, không có số đo mới, và chưa xác nhận công cụ/server. Manifest máy đọc được ở [`results/p8/workload-manifest.json`](results/p8/workload-manifest.json). Mọi giới hạn ở đó là **proposed**, không phải effective runtime values, kết quả đo hay deployment config.

## Source lock khi bắt đầu P8.0

- Branch `feat/paper-requirement`, revision `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`; tracked và untracked tree sạch tại thời điểm kiểm tra. Thay đổi tài liệu hiện tại phát sinh sau snapshot đó.
- Source migration terminal là 24. Chưa truy vấn DB fixture/shared; version source không chứng minh schema thực tế đã migrate.
- Sáu runtime flags `WEBSOCKET_ENABLED`, `STAGE5_ENABLED`, `STAGE5_WAKEUP_ENABLED`, `MQTT_SHAPER_ENABLED`, `MQTT_MICROBATCH_ENABLED`, `MQTT_RAM_DEDUP_ENABLED` vẫn có deployment default `false`; không đổi config/deployment trong P8.0.
- Fingerprint conventions kế thừa source revision + clean/dirty qualification, SHA-256 cho dirty-source material (status/diff/untracked path và contents), và immutable image reference cùng image ID. Hash thiếu phải là `null` kèm lý do; không ghép run có qualification khác nhau. P7 đã có lanes ghi hash verified và lanes hash unavailable; raw records vẫn `full_p7_accepted=false`.
- Gate lịch sử được giữ nguyên, không phải lần chạy mới: P7 full Python discovery có 3 failures + 4 errors (environment/trusted-ancestor; không rerun trong P7.11); full/remote CI, HIL/AM5728, P8 capacity/soak và live rollout `NOT RUN`.

## Topology và cửa sổ đo dự kiến

Máy A giữ một backend active cùng fixture DB/broker/Auth/Nginx và collector đặc quyền; máy B chạy Python điều phối và Go generators hữu hạn, chỉ nhận credential đúng role, không có DB/admin/Docker access. Pilot có thể colocate trên một host nhưng bắt buộc ghi `generator_colocated=true`; đó chỉ là kiểm đường công cụ, không phải capacity evidence cho topology A/B. Remote pilot chưa thực hiện được ghi `NOT RUN`.

Warm-up ordinary pilot đề xuất 30 giây, measured window 120 giây, stop generation rồi drain tối đa 120 giây, cooldown 30 giây; E1 handshake-ramp có lane riêng 120 giây. Provision/login/build/pull nằm ngoài cửa sổ, trừ handshake ramp được chỉ rõ. Collector resource interval đề xuất 5 giây. Toàn bộ là giới hạn dự kiến cần được đối chiếu với runner trước khi gọi là effective.

## Accounting và clocks

Hai workload riêng: 100 samples/message và 1 sample/message. Profile 1 sample không có nghĩa FFT/min-max/avg hoặc downsampling. Đếm riêng unique offered, scheduled/actual publish attempts và retries, broker PUBACK, backend observed/admitted/rejected, new commits/duplicates, committed raw samples, application ACK published/received và outbox deletions, WebSocket expected/received/duplicate/unexpected/missing deliveries, cùng pending count/bytes/drain remainder. Retry không tăng unique; duplicate không tăng raw mới. Payload bytes không phải wire bytes; backend Admit observation không phải broker packet count.

Backend stage durations dùng monotonic clock của process backend; client publish-to-application-ACK hoặc publish-to-WS-receive dùng origin monotonic trong cùng generator process. Không trừ monotonic clocks giữa máy/process và không dùng sensor `measured_at` làm commit clock. One-way delay mặc định unavailable nếu chưa công bố clock synchronization cùng error bound. Missing origin, process reset, scrape thiếu hoặc instance mismatch là `null`/unavailable với lý do, không phải zero. Kế hoạch artifacts kế thừa `run.json`, `metrics.csv`, `resources.csv` theo convention P1; metric rows/buckets không lưu per-message payload.

Telemetry child phát tối đa 16 dòng allowlist `P8_PHASE {JSON}`: warmup/measure/burst begin/end, `generation_stopped`, drain begin/end và `workers_joined`. Mỗi snapshot mang configured phases, counters tại thời điểm chụp và `elapsed_ns` tính từ một origin monotonic duy nhất trong process đó. `generation_stopped` chỉ xác nhận generation đã dừng (publishes đã xếp hàng có thể vẫn chạy); `workers_joined` chỉ sau khi worker wait hoàn thành. Runner ghi `observed_at_runner_monotonic_ns` lúc đọc pipe; đó là thời điểm nhận được, pipe lag không biết được, không phải latency server-side. Chỉ so sánh runner-observed interval trong cùng runner clock; không trừ child elapsed giữa process. Thiếu/malformed/late/out-of-order event hoặc counter không hợp lệ làm measurement unavailable/inconclusive, không thay bằng zero hay lịch cấu hình. Nếu ghi event lỗi, workload không được báo `COMPLETE`.

Instrumentation này chưa phải kết quả native: E2 profile 1/steady-burst chưa chạy; E3 overlap vẫn **NOT RUN**.

### E3 pilot helper slice (P8.8/P8.11)

`scripts/paper_p8_e3_support.py` cung cấp `e3_pilot_arms`, `e3_pilot_manifests`,
`build_e3_execution_plan` và `validate_e3_result`; test tập trung ở
`scripts/tests/test_paper_p8_e3_support.py`. Đây là pure preset/plan/adapter;
native orchestration lives in `scripts/tests/paper_p8_e3_fixture.py`. Background A/B kế
thừa E2 8/16 messages × 100 samples trên Gateway riêng; hai control Sensor ở
Gateway A tách khỏi background. Bốn fresh command schedule có version tăng theo
Sensor và include 1.5/0.5 Hz. Chỉ cờ wake-up khác giữa hai arm; deployment defaults
không đổi. Plan dành đúng hai durable Gateway executor, ba paperload jobs và
private manifests/run-spec; dùng runner `paper_p8.py` để readiness/start/stop.
Result adapter chỉ nhận runner receipt timestamps cho overlap cùng clock, yêu
cầu command lifecycle/terminal, telemetry hoàn tất, stopped runner và receipt
verification được fixture cung cấp. Pipe lag không biết được; HTTP round-trip
không phải server-origin latency. Command raw rows báo theo số thực tế, tách khỏi
background DB expected 24/2400/24; dispatch/e2e histograms collector-only. Slice
này **chưa chạy native E3**, chưa xác minh manifest qua Go CLI/fixture REST/DB/
SQLite. Lượt native retry và trạng thái evidence ghi ở mục tiếp theo.

#### E3 polling retry có hậu kiểm (2026-10-28)

- Lệnh đã chạy đúng một lần: `python3 scripts/tests/native_compose_smoke.py --prepare /var/tmp/opencode/p8/p88-e3-polling-drain-95LGLD/fixture --paper-p8-e3 polling`.
- Kết quả run `ncs_c9ca71cf2f4a`: **FAIL / STARTUP_FAILED**, phase `report_validation`, error type `ValueError`, error code `unclassified`. Cleanup được xác nhận: 12 containers, 1 network, 5 volumes. Artifact tổng hợp: `/var/tmp/opencode/p8/p88-e3-polling-drain-95LGLD/fixture/artifacts/run.json`; lane artifact: `.../artifacts/p8-e3-polling.json`; failure: `.../artifacts/smoke-failure.json`.
- Nguyên nhân đã xác định từ source và failure: helper drain bị đặt giữa thân `run()` làm phần còn lại của `run()` unreachable sau `return` trong helper; native driver nhận `None` thay workload artifact. Đã chuyển helper ra top-level và kiểm tra lại các test liên quan. Lượt native xảy ra trước sửa này; theo giới hạn tác vụ không chạy lượt native thứ hai.
- Do workload fixture không khởi động tới runner, lượt retry này **không có** runner `run.json`, bốn command, overlap, receipt, drain, raw/backend identity, histogram/origin latency hay server-resource evidence. Không gán pass. Lịch sử lỗi pending ban đầu vẫn giữ nguyên: runner `PARTIAL`, không có primary/cleanup error, stop xác nhận; executor đầu có 2 row/803 bytes pending, executor hai 0. Đây chưa phải runner `COMPLETE` và không phải bằng chứng drain.
- Sau sửa, `py_compile` và 43 test chọn lọc E3/P8 PASS (bao gồm ba test từng thất bại do helper placement); không thay thế native retry. Native E3 polling acceptance vẫn **NOT RUN / chưa đạt**. Wakeup, negative probes, E4/E5, official/high-load, full/remote CI và HIL tiếp tục NOT RUN.

## Presets được đề xuất (chưa chạy)

| Bài | Pilot | Official opt-in (lớn; không chạy mặc định) |
|---|---|---|
| E1 | 64 viewers, ít nhất 2 namespace, sparse/dense/overlap, cap+1 denial, revoke/slow viewer và 2 chu kỳ recheck | 32→64→128→512→1.000→2.000→5.000→10.000 viewers |
| E2 | 2 Gateway × 2 Sensor, 20 unique messages cho mỗi sample profile (100 và 1), steady + finite burst + bounded drain | 100/1.000/5.000/20.000 offered msg/s, có cap rate/duration |
| E3 | 4 fresh commands trên ≥2 Sensor, background steady/burst, polling và wake-up | 30 commands/arm; có frequency dưới 1 Hz |
| E4 | 20 unique + 10 replay, ACK-ignore retry, warm/cold reopen và conflict | 1.000 unique, 10% replay; tùy chọn TTL wall-clock thực 600 giây |
| E5 | Reference và từng ablation; chỉ reuse artifacts E1–E4 nếu source/flags/counters tương thích | Tối thiểu 3 run độc lập, seeded order, per-run rồi median/range |

Manifest đặt proposed finite bounds: pilot 30 phút, backend 2 CPU/4 GiB (P0 fixture quota precedent, chưa được đo lại), fixture disk growth 2 GiB, pending 10.000 messages/64 MiB, drain 120 giây; official opt-in mỗi run tối đa 2 giờ, disk growth 10 GiB, pending 100.000 messages/512 MiB, drain 600 giây. Đây là **đề xuất cho review**, chưa được thực thi/enforced. Hit bất kỳ bound nào thì dừng producer, thử drain trong budget, giữ nguyên pending và lưu `PARTIAL`/`FAILED` cùng remainder; không xóa queue/outbox hoặc sửa SLA để đạt PASS. Cleanup lỗi được ghi riêng với lỗi gốc. Cấu hình lớn yêu cầu opt-in rõ ràng.

## Giới hạn P8.0

Tại mốc P8.0, manifest chưa triển khai session cap, budget enforcement, runner, collector, remote topology hay benchmark. Các bước P8 sau ghi riêng implementation đã có và qualification; không suy luận official acceptance hoặc capacity từ pilot; không thay đổi flags mặc định, schema, protocol, hoặc deployment. `NOT RUN` và historical FAIL vẫn giữ đúng nghĩa.

## Tiến độ triển khai sau P8.0 — P8.1b local implementation

### P8.9a — E4 private journal/settings (source slice)

Manifest schema v1 có nhóm `e4` tùy chọn chỉ cho E4 telemetry; `mode=unique|replay`, journal path, giới hạn hữu hạn tối đa 10.000 messages/64 MiB và tối đa 10 ACK-ignore. Preflight chỉ kiểm tra shape/bounds, không mở journal hoặc credential. Private journal tạo mới độc quyền, file 0600/parent 0700, lưu nguyên MQTT topic/payload cùng header/hash; append đồng bộ gọi từ generator owner. Replay reader xácác hash, telemetry payload và composite scope, giữ nguyên bytes. Đây là provenance/replay input, **không** durable outbox, exactly-once hay power-loss assurance.

### P8.9b — Go E4 telemetry runtime integration (source implementation)

`--run` tích hợp `mode=unique|replay`: unique tạo journal độc quyền và append trước enqueue; replay kiểm tra đúng số cơ hội trong manifest, target/sample scope và dùng batch/topic/payload gốc. E4 summary chỉ ghi tối đa 20 safe header/hash samples, counters tách replay khỏi unique, cùng sampling qualification `synthetic_load_only; not SQLite/durable assurance`. ACK-ignore bỏ qua tối đa cấu hình số ACK hợp lệ đã match pending/published; payload vẫn pending và retry bình thường. Invalid/retained/unmatched ACK không tiêu hao budget. Journal/write/close lỗi không bị xem là complete; file được giữ lại.

Đã chạy unit/race-source checks chỉ sau tích hợp; **fixture E4, native/Docker run, SQLite commit/ACK evidence, warm/cold reopen, conflict, E5, capacity/soak, full/remote CI và HIL đều NOT RUN / pending**. Không có claim về durable Gateway, SQLite durability, broker/backend acceptance hoặc native P8 acceptance.

### P8.9c — Gateway CLI ACK-ignore + SQLite reopen seam (source only)

Gateway simulator có fault flags tường minh: `gateway run --batches N --ignore-first-valid-acks K` (N hữu hạn, K=1..10), và tùy chọn `--stop-after-ignored-ack`. Chỉ ACK hợp lệ đã parse và khớp row `pending` của Gateway mới tiêu hao budget; row/payload/sequence được giữ nguyên. Stop mode dừng hợp tác qua shutdown hiện hữu, trả exit khác 0, summary tách `ignored_valid_acks`, `intentional_fault_stop`, `app_acknowledged`, `pending_rows/bytes`, `completed=false`; sau đó `gateway count`/`pending` đọc lại SQLite khi process mới mở file. Không có flag/env production, fixture hay backend/protocol change. Source tests không phải E4 fixture: native/Docker, end-to-end broker/backend ACK-ignore + process reopen **NOT RUN**.

P8.1a đã thay hard ceiling của `WS_MAX_SESSIONS` bằng khoảng cấu hình 1–10.000, default 32; handler dùng cap cấu hình, composition truyền cùng giá trị cho Hub và rechecker. P8.1b đã triển khai aggregate session-reference budget. Ring 32 close observations và các giới hạn subscription/frame/queue không đổi. Đây là implementation bounded, **chưa phải nghiệm thu tải**; không dùng thay đổi này làm profile scale đã được kiểm chứng.

Regression RED → GREEN P8.1a xác nhận config nhận 64/10.000 và handler nhận cap 64. P8.1b bổ sung một shared logical session-reference budget: default 8 MiB, process-env `WS_SESSION_REFERENCE_BYTES` khoảng 1..256 MiB, áp dụng cho từng reference telemetry/control ở queued **và** in-flight. Giữ nguyên queue 256 KiB/session, 64 slots, 16 subscriptions/filters, frame/read bounds và control bound. Payload immutable vẫn dùng chung; không tạo bản copy cho từng subscriber.

### Accounting và loss policy P8.1b

`EventRuntime` snapshot budget (default 8 MiB) và session-reference budget (default 8 MiB) là hai bounds độc lập. Snapshot reservation vẫn sống trong lúc fan-out đồng thời reserve từng reference của session; snapshot chỉ release theo lifecycle runtime hiện hữu, không chuyển accounting sớm sang session. Logical bound của hai lớp cộng lại tối đa 16 MiB ở default, không phải RSS cap; allocator/GC, queue/index/map metadata, socket/kernel buffers, TLS buffers và goroutine stacks nằm ngoài con số này.

Khi shared session-reference capacity cạn nhưng queue riêng vẫn còn chỗ, policy được người dùng duyệt: đóng session bị ảnh hưởng với nguyên nhân/counter `aggregate_capacity`, không gọi là `SlowClose`/slow client; viewer reconnect và dùng REST catch-up theo contract WebSocket không durable. Queue slot/byte overflow riêng tiếp tục theo slow-consumer policy cũ. Unsubscribe/purge/close trả ngay phần queued; dequeue chỉ đổi queued→in-flight và không trả budget. Frame/control reservation writer-owned tiếp tục tính kể cả sau queue close, tới `Release()` idempotent.

### Tính hữu hạn theo cap cấu hình

Mỗi session có tối đa 16 subscription/filter pairs nên giới hạn indexed pairs là `cap × 16`: 512 ở cap 32, 1.024 ở 64 và 160.000 ở 10.000. Giới hạn byte per-session 256 KiB cho tổng queued/in-flight telemetry và controls suy ra upper bound lý thuyết lần lượt 8 MiB, 16 MiB và 2.500 MiB; shared aggregate mặc định 8 MiB chặn tổng logical references xuống tối đa 8 MiB ở cả ba cap (các reference tính lặp theo subscriber dù payload bytes được share). Không phải mọi queue đều có thể đạt cap đồng thời.

Mỗi session được nâng cấp cũng kéo theo socket và các worker/state/index records tương ứng; số goroutine/socket và chi phí kernel/TLS không suy ra từ byte budget. Cap 10.000 vì vậy chỉ là finite admission configuration, không chứng minh tài nguyên host, recheck cadence, fairness, RSS hay load capacity. P8.2a/b đã thêm repository batch contract và recheck chunking/lifecycle bounds ở source; **64-session pilot, native/high-load tests, E1–E5, full pipeline, remote execution và restore vẫn NOT RUN**. Sáu deployment flags vẫn mặc định false; không có deployment/rollout thay đổi.


## P8.2a — hợp đồng batch membership/existence có bound

Trước P8.2b, recheck gọi authorization đơn cho mỗi session-target (tối đa 16 filters/session), nên một sweep có thể xét tới `sessions × filters` tham chiếu. Batch khử trùng theo đầy đủ `(user_id,gateway_id,sensor_id)`: với `D` key khác nhau có thể giảm xuống `ceil(D/256)` lời gọi repository batch thay vì `N` lời gọi target đơn. Đây là so sánh theo cấu trúc code, **không phải timing hoặc throughput đo được**. Không thêm permission cache hay migration.

`gateway.BatchSubscriptionTargetRepository` là extension tùy chọn; interface target đơn cũ vẫn giữ nguyên. Một batch gồm 1–256 key đã validate, không truncate; truy vấn PostgreSQL join `user_gateways`, `gateways`, `sensors`, trả đầy đủ user/Gateway/Sensor/role. Target thiếu, bị từ chối, không tồn tại hoặc sai parent có `Authorized=false`; lỗi query/scan/iteration không trả kết quả. Authorizer kiểm tra identity/kết quả đầy đủ; DB error hoặc batch malformed fail-closed toàn batch, không partial grant. Role vẫn owner/operator/viewer; platform admin không tự có quyền. Phần lập lịch/chunk/lifecycle được nối ở P8.2b.

Kiểm thử batch tập trung PASS sau RED vì contract chưa được khai báo. Có native regression opt-in trong `TestPostgresAuthorizationIntegration` cho hai user, Sensor ID trùng giữa Gateways, target thiếu/sai parent và admin không membership. Runner cô lập `sh scripts/test-stage2-authorization.sh` **PASS** tại slice P8.2a; đây là DB regression hẹp, không phải P8.3 pilot hay scale acceptance. RED/GREEN và logs lịch sử ở `/var/tmp/opencode/p8/p82a-20261028/`.

## P8.2b — batching và lifecycle của periodic sweep (source implementation)

Sweep snapshot-ы sessions đến effective cap, kiểm absolute expiry (kể cả session idle không subscription), khử trùng key chỉ trong sweep theo tuple đầy đủ `(user_id,gateway_id,sensor_id)`, rồi chia chunk tối đa 256 key. Mặc định interval 5s, total sweep budget 2s và concurrency 2 được giữ; không sample bỏ session, không permission cache xuyên sweep. Với `N` snapshot target references và `D` composite key riêng, batch repository path có tối đa `ceil(D/256)` lời gọi batch thay vì `N` lời gọi single-target; đây là arithmetic từ code, **không phải timing/throughput đo được**. Authorizer cũ vẫn tương thích qua per-key fallback với tối đa `D` lời gọi.

Kết quả được áp dụng tại coordinator. Generation snapshot được kiểm trước khi xử lý và so sánh atomically khi đóng; generation thay đổi/đã đóng là stale, không nhận kết quả cũ hoặc được khôi phục subscription. Thiếu/denied key và DB error đóng session snapshot liên quan fail-closed; deadline/cancel chỉ đóng session còn đúng generation nhưng chưa verify, purge qua close path hiện có; kết quả authorizer trả muộn bị bỏ qua. Không cài lại target. Metrics fixed-cardinality tổng hợp sessions examined, distinct keys, chunks đã lập kế hoạch, authorization calls đã bắt đầu, timeout sweeps và duration histogram; không có identity labels, metrics nil thì disabled.

Để timeout không làm tăng authorization calls đang chạy vượt concurrency qua nhiều sweep, worker dùng chung slot limiter của `RecheckWorker`; slot chỉ được trả khi authorizer thực sự kết thúc. Sweep vẫn fail-closed đúng deadline; worker còn lại bị giới hạn bởi concurrency và `StopContext` join worker hợp tác trong budget. Bộ đếm JSON `recheck_queries_started` gồm lời gọi đã bắt đầu nhưng chưa trả kết quả khi timeout; adapter fallback được đếm từng query target đơn, không tính cả adapter là một DB query. Đây không chứng minh database đã thực thi thành công lệnh. RED regression trước sửa ghi nhận 3 call/3 concurrent qua 3 timeout liên tiếp với concurrency=1; sau sửa xác nhận tối đa một call, query-start counts và không tái sử dụng quyền từ sweep trước. Logs mới ở `/var/tmp/opencode/p8/p82b-worker-bound-*`.

Đây là source-level bounds, không phải timing/capacity claim. **Native/high-load, pilot 64-session, E1–E5, full pipeline, remote execution và restore vẫn NOT RUN**; kết quả native/P8.2a lịch sử không được tính là lượt mới P8.2b.

## P8.2c — lỗi build event được giới hạn theo namespace

Kiểm tra A/B nhỏ đã **tái hiện** nghi vấn cũ: trước sửa, một batch hợp lệ của
Gateway/Sensor A làm worker gọi `CloseAllSessions`, đóng cả session chỉ subscribe
Gateway B. RED log: `/var/tmp/opencode/p8/p82c-20261028/red.log`. Sau sửa,
encoder error, encoder trả nil và frame oversize đóng đúng các session có Gateway
filter A hoặc Sensor filter A; overlap bị đóng một lần, B vẫn mở và nhận batch B
 kế tiếp. GREEN log: `/var/tmp/opencode/p8/p82c-20261028/green.log`. Runtime chỉ scope khi
 Gateway ID và Sensor ID của batch vượt validation; nếu composite target
không đáng tin cậy, vẫn dùng global fail-closed để không làm mất gap im lặng. DB,
telemetry persistence và application ACK không đổi. Reservation được xác nhận
release sau target build failure; `build_failures` hiện có tiếp tục đếm build
failure.

Loss policy global được giữ nguyên: queue/snapshot admission exhaustion tiếp tục
coalesce gap và worker đóng toàn bộ sessions; không đổi `aggregate_capacity`
 reason/counter (session-reference exhaustion) thành global overload. Đây là kiểm
thử unit/runtime; **native, high-load, 64-session pilot, P8.3+, E1–E5 và full
pipeline NOT RUN**. Không có claim về capacity, DB acceptance hay native acceptance.

## P8.3a1 — Go native 64-session WSS probe

Probe opt-in đã được thêm trong `src/internal/telemetryws/native_p8_sessions_test.go`; unit tests kiểm private config/bounds, TLS CA/hostname rejection, ACK mismatch, 3-client httptest cap denial/replacement, telemetry recipients/overlap và stop/socket cleanup. Chỉ implementation/unit verification; native 64-session qua Nginx, fixture orchestration, nhận batch thật, revoke integration và slow-viewer **NOT RUN**. Probe không phải P8 acceptance hoặc DB verification.

Contract private input (file mode 0600, private parent; biến môi trường chỉ chứa path `P8_SESSIONS_CONFIG_FILE`): `wss_endpoint` (loopback IP literal, không query/userinfo), `ca_file`, `role_jwts` với keys `A`/`B`, `namespace_targets` (mỗi entry `{"group":"A|B","target":{"gateway_id":"…","sensor_id":"…"}}`; hai Gateway namespace), `sessions` (64 entries native lane; mỗi entry `id`, `group`, `filters`), `max_lifetime_ms` (100–180000), `max_bytes` (1..256 MiB), `max_events` (1..100000), cùng private absolute `ready_path`, `state_path`, `stop_path`, `revoke_group_a_path`, `output_path`. Mỗi filter có tối đa 16/session và toàn bộ filters tối đa 16 identities; JWT không được đặt trong URL/argv. TLS verify dùng CA cung cấp và hostname verification, TLS ≥1.2, không proxy; không có insecure mode.

Ready JSON ghi `ready`, `socket_count`, `principal_count`, `target_count`. State JSON ghi counts `events` (frame telemetry nhận), `unique_events`, `bytes`, `duplicates`, `unexpected_namespace`, `retained_identity_count`, `max_retained_identity_count`, cùng per-viewer `id/group/events/unique_events/bytes/duplicates/unexpected_namespace/closed` và tối đa 8 sampled message IDs/viewer. Orchestrator chờ counts theo từng viewer trước khi tạo `revoke_group_a_path`; A phải nhận close trong khi B còn sống; `stop_path` dừng an toàn. Output luôn ghi `outcome=probe_only_not_p8_acceptance`, cùng `status=completed|failed` và `failure` khi probe lỗi; không chứa JWT/frame payload. Unit tests không khẳng định fixture/native lane đã chạy.

## P8.3a2 — disposable native 64-session fixture orchestration

`native_compose_smoke.py --prepare <fresh-private-dir> --paper-p8-sessions` là
opt-in riêng, không đổi `--full-http` hay raw P7 acceptance. Lane reuse P2
HTTPS/WSS + CA/Origin allowlist, Unix `auth_request`, MQTT TLS/DynSec,
`prepare_paper_workload`, Gateway simulator, admin metrics và exact-owned
registry/cleanup. Chỉ fixture này đặt `WEBSOCKET_ENABLED=true`,
`WS_MAX_SESSIONS=64`, `WS_SESSION_REFERENCE_BYTES=8388608`; cờ Stage 5/wake-up,
shaper, microbatch và RAM dedup vẫn false. Endpoint CA/name được kiểm chứng bởi Go
TLS client; Origin là `https://client.example.test`, không có insecure TLS mode.

Probe nhận config private 0600 với hai user JWT; tạo đúng 64 sockets (32/principal)
trên hai Gateway/Sensor namespace, dùng sensor-only, gateway-wide và Gateway+Sensor
overlap filter. Có 84 filter entries nhưng overlap chỉ tính một delivery/session.
Probe kiểm cap+1 denial rồi đóng một socket và chờ replacement subscription ACK;
identity/dedup dùng `(gateway_id,sensor_id,message_id,boot_id,first_sequence)`.
Sau 41 giây ở effective heartbeat 20s/pong 45s và recheck 5s/budget 2s/concurrency
2, tất cả 64 peers còn mở; metrics ghi delta 512 session examinations (ít nhất
8 sweep-equivalents). Sau đó SQL scoped delete chỉ revoke membership user A,
chờ A đóng và B giữ socket, rồi B nhận batch MQTT TLS thật tiếp theo.

### Kết quả native run và phạm vi claim

Run PASS lane-specific ngày 2026-10-28, artifact tại
`/var/tmp/opencode/p8/p83a2-20261028-070121-30384/artifacts/`:

- Status `PAPER_P8_3A2_SESSION_LANE_PASS`; exact cleanup **15 containers, 1
  network, 7 volumes**. Output giữ `outcome=probe_only_not_p8_acceptance`.
- Source `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`, dirty-source SHA-256
  `b5dc97cb9a37e0dbf68b2aac7ed298741a49ade584c850a53ce26931abb3fbaf`; image IDs,
  schema thực `24:telemetry_ledger_timestamp`, backend quota 2 CPU/4 GiB và flags
  effective được ghi trong `p8-sessions.json`. Generator cùng host
  (`generator_colocated=true`), 2 principals/64 sockets.
- Cap+1 denied, replacement ready; 96/96 WS deliveries đúng namespace (expected /
  actual A=32, B=64), 0 duplicate và 0 unexpected namespace. Sau revoke, toàn bộ
  A đóng; 32 B peers còn mở và nhận batch post-revoke. Recheck delta là 512
  session examinations (8 sweep-equivalents trong cửa sổ đo); tài nguyên có 4
  samples trong `resources.csv`. 3 Gateway batches đều có
  application ACK, tổng ACK=3, outbox pending rows/bytes=0.
- DB delta: ledger 3, raw samples 300, temporal 3. Sau shutdown metrics cho
  sessions/subscriptions/session-reference bytes/queued-in-flight bytes đều 0.
  Probe summary ghi số counts, timer values, fingerprints, resources và cleanup;
  không chứa JWT hoặc private config.

Các lượt FAIL trước đó vẫn được giữ, không đổi thành PASS: `p83a2-20261028-064147-24477`
và `...064441-18094` không upgrade được do fixture P2 overlay chưa bật WS;
`...064736-20800` là compose-preflight YAML fail do indent của overlay;
`...064809-31358` lộ lỗi path `revoke_path` so với contract; `...065006-20348`
đã hoàn tất native probe nhưng outer cleanup phát hiện stop-marker chưa idempotent.
Đã sửa overlay P2 chỉ cho selector này, đúng key control, và stop-marker idempotent;
run PASS nêu trên là lượt riêng sau sửa. Invocation `...064429-22440` bị từ chối vì
output directory đã tồn tại, không có fixture run/artifact.

Tại thời điểm ghi P8.3a2, slow-viewer P8.3a3 chưa chạy. Kết quả và giới hạn mới
được ghi riêng dưới đây; remote A/B, FD/port/worker qualification, E1–E5, full CI,
HIL/AM5728 và official paper experiments vẫn chưa chạy.

## P8.3a3 — finite slow-viewer orchestration (2026-10-28)

Selector opt-in mới: `python3 scripts/tests/native_compose_smoke.py --prepare
<fresh-private-dir> --paper-p8-slow-viewer`. Nó dùng fixture P2 HTTPS/WSS và
P8 effective profile, không ảnh hưởng `--full-http`, P7 raw evidence, hoặc
deployment defaults. Chỉ ba socket được mở: hai healthy Go peers (namespace
A/B) và một Go slow-viewer (namespace A); cap cấu hình vẫn 64, nên đây không
phải cap/64-session acceptance. Healthy probe nhận private config qua
`P8_HEALTHY_PEERS_CONFIG_FILE`; slow probe dùng `slow_viewer_probe_config` và
`P7_SLOW_VIEWER_CONFIG_FILE`. Cả hai xác thực CA P2/hostname WSS và ghi nhận
SUBSCRIBE ACK trước khi bắt đầu phát.

Native command thực thi: `python3 scripts/tests/native_compose_smoke.py --prepare
/var/tmp/opencode/p8/p83a3-native-20261028-073942-1653397/fixture
--paper-p8-slow-viewer`. Kết quả `PAPER_P8_3A3_SLOW_VIEWER_PROBE`, lane PASS,
`full_p8_accepted=false`; artifact đầy đủ ở
`/var/tmp/opencode/p8/p83a3-native-20261028-073942-1653397/fixture/artifacts/`.
Schema runtime được đọc là `24:telemetry_ledger_timestamp`; backend fixture quota
2 CPU/4 GiB. Manual MQTT-TLS workload dùng client IDs riêng cho từng paced wave,
wave 10, ACK subscriber thật và P5 application ACK. Đây là synthetic/manual
ingress, **không** khẳng định Gateway durable outbox.

Workload cuối là A=400 batches × 1.000 samples và B=2 × 1.000. Đã thử profile A=200
ban đầu nhưng không thấy server close trong cửa sổ chờ; profile 400 là finite upper
budget mà task cho phép, không phải capacity result. Effective profile ghi rõ
`WS_HEARTBEAT_INTERVAL=2s`, `WS_PONG_TIMEOUT=4s`, `WS_WRITE_TIMEOUT=1s` (chỉ selector
này; bounds/invariants ở `src/internal/config/websocket.go`, lines 33–57),
session-reference budget 8 MiB, cap 64,
và sáu deployment flags `WEBSOCKET_ENABLED`/Stage 5/wake-up/shaper/microbatch/RAM
dedup theo artifact: WEBSOCKET true riêng trong fixture, năm deployment feature
flags false. Workload vẫn chưa chứng minh queue-overflow slow-close: server metrics
ghi đúng `ping_timeout` sau khoảng 4.001s, sessions 3→2, `slow_closes` 0→0. Sau
resume, probe Go ghi `transport_closed`, EOF, 30 frames/703.230 bytes; không nhận
close frame (`server_close_received=false`). Vì vậy kết luận là server heartbeat
đã đóng socket trong lúc client không đọc, không phải queue-overflow counter.

Validation: application ACK A=400/B=2; healthy peers nhận chính xác A=400/B=2,
không duplicate/unexpected namespace. Scoped DB counts A ledger/raw/temporal
400/400.000/400; B 2/2.000/2. Một replay hợp lệ nhận application ACK `committed`;
DB scoped counts không đổi và healthy WS event/unique counts không đổi. Sau stop/join
metrics sessions/subscriptions/session-reference bytes đều 0. Exact-owned cleanup
PASS: 12 containers, 1 network, 5 volumes; không có shared prune/deletion.

Các lượt thất bại trước PASS được giữ nguyên dưới `/var/tmp/opencode/p8/`:
`p83a3-native-20261028-071555-1560579` và `...071828-1570543` gặp profile
effective WEBSOCKET false từ P2 overlay; `...072028-1577646` lỗi replay publisher
line mode; `...072210-1585902` (200 batches) và `...072450-1595078` (400 batches)
không quan sát được slow close theo timer mặc định; `...072814-1605890` chưa đưa
timer override vào effective Compose config; `...072938-1612555` đã chạy nhưng
metrics `close_observations` là null và lộ lỗi xử lý null trong orchestrator. Mỗi
lượt outer cleanup xác nhận sạch; chúng không được đổi thành PASS. Run cuối sau
các sửa tương ứng là một lần riêng.

Giới hạn còn lại: chỉ qualification 3-peer trên một host; heartbeat ping-timeout
không chứng minh queue-overflow close, tải sustained, fairness, RSS, nhiều viewer,
64-session cap, remote A/B, E1–E5, full CI, HIL/AM5728 hoặc production acceptance.
Raw output tiếp tục `full_p8_accepted=false`. P8.3b và các bước tiếp sau chưa được
thực hiện.

## P8.4.10 — control-only REST producer (implementation slice)

`paperload --run` nhận đúng role đơn `control` cho E3; các role kết hợp bị từ
chối trước khi mở mạng. Producer chỉ dùng HTTPS REST (TLS 1.2+, CA file riêng,
hostname/SAN verify, không proxy/insecure mode), private `role_jwts` JSON dạng
`{"actor-label":"<owner-or-operator-JWT>"}` (file regular mode 0600), và không có
DB/admin/service_role/MQTT credentials. Mỗi entry trong `commands` có
`gateway_id`, `sensor_id`, `frequency_hz` JSON number (giữ decimal token, ví dụ
`1.5` hoặc `0.1`), `offset_ms`, `actor_label`, UUID `idempotency_key` mới và
`expected_desired_version` JSON string. Keys unique trong stream. Control-only
không cần viewers/subscriptions/identity files của MQTT, Gateway credential,
MQTT endpoint/CA, traffic rate, sampling phase, durability profile hay bundle;
chỉ `phases.drain_seconds`, `limits.wall_seconds`, worker count và HTTPS inputs
liên quan. Producer tuần tự hóa command (concurrency 1), ngừng phát khi SIGINT,
và giữ poll/readback trong deadline/drain hữu hạn.

REST contract: `PATCH /v1/digital-twins/{entityURN}/desired-state`, headers
`Authorization: Bearer`, `Content-Type: application/json`, `Idempotency-Key`, body
`{"frequency_hz":<decimal>,"expected_desired_version":"<BIGINT>"}`; 202 receipt
trả `command_id`/`status`, còn 200 là no-op. Poll
`GET /v1/digital-twins/{entityURN}/commands/{command_id}`; chỉ `succeeded` sau đó
được kiểm `GET /v1/digital-twins/{entityURN}?format=simplified` tại
`reported_state.configuration.desired_version` và `sample_interval_us`, cùng
command/result identity. Không so sánh top-level `reported_version` với
`desired_version`: telemetry commits tăng reported version độc lập. `desired_state`,
202 hay MQTT PUBACK không phải success. HTTP errors use the existing `error.code` envelope;
`command_in_progress`, `desired_version_conflict`, idempotency conflict, timeout
and rejection remain distinct. Client round-trip is explicitly not backend
`D_dispatch`/`D_e2e`; server origin is unavailable from this role.

No timestamp comparison is made between client and server clocks. Client request
history is bounded to this command stream: a repeated key is marked as a possible
receipt replay, not assumed fresh. First-use 202 is qualified as a manifest-declared
unique UUID key; REST cannot prove that an earlier byte-identical receipt does not
exist. A 200 no-op and its exact replay are indistinguishable and both stay outside
fresh-success latency. Poll 401/403/404 and malformed/oversized responses are
classified immediately, not retried as transient; bounded 429/5xx/network errors
may be polled again until the deadline. `workload.json` includes planned/sent/
skipped/pending counts and per-command qualifications so skipped work cannot yield
COMPLETE. HTTPS response bodies cap at 64 KiB; connection/pool, connect, header,
and total request waits are bounded.

Bounded command phase observations share the runner's existing `P8_PHASE` pipe:
request/admission/terminal events carry the manifest command ordinal and a
process-local elapsed timestamp from `RunControl` start; the Python supervisor
adds its own `time.monotonic_ns()` receipt timestamp. These clocks are qualified,
not subtracted or compared: pipe observation lag is unknown and neither timestamp
is server dispatch/E2E latency. At most the first nine command ordinals (all 4–6
pilot commands) plus lifecycle events are emitted, with a 32-event hard cap; larger
manifests carry a sampling qualification, not a 1000-command workload restriction.
Observer failure makes measurement PARTIAL while retaining command results and
cleanup. Native E3 remains **NOT RUN**.

Focused httptest covers HTTPS CA/hostname and auth header, exact decimal and
BIGINT request encoding, PATCH→202→terminal result→reported readback, response
size rejection, error classification, and stop-before-next/drain. These are
mock-server unit tests only: **native E3, real REST backend, externally
supervised durable Gateway executor, Python end-to-end orchestration, background
telemetry overlap, and paper E3 acceptance are NOT RUN**. Without the existing
real simulator/executor and result commit, commands time out/incomplete; this
slice does not execute MQTT commands or synthesize success/configuration.

## P8.3b — preflight hữu hạn và hợp đồng A/B (2026-10-28)

`scripts/tests/paper_p8_support.py` bổ sung helper chỉ đọc `read_generator_preflight`
(process RLIMIT_NOFILE, OS `/proc/sys/fs/file-max` + `file-nr`, và
`/proc/sys/net/ipv4/ip_local_port_range`), ước lượng FD
hữu hạn cho pilot64 và rung đề xuất 128/256/10.000, `fixture_process_preflight`
(worker settings, per-process FD limits, FD còn dùng được), và
`read_owned_fixture_resources` để đọc qua `docker exec` đúng service names đã nhận
receipt `nginx`, `backend`, `mosquitto`. Nginx estimate tính **2 connection slots / WS
viewer** cho downstream + upstream proxied, cộng 32 slots reserve; `fits` là
đối chiếu config, không phải tải đã đo. Không đọc được thì trường availability là
`null` kèm reason, không giả định bằng 0. Preflight không thay `ulimit`, sysctl,
Compose deployment limits hoặc quyền ancestor. Effective fixture readback thuộc
native session artifact; máy B phải chạy cùng `read_generator_preflight` trước run.
Các con số rung chỉ là estimate (generator: 64 base FD + 2/viewer), không phải
capacity/sizing guarantee.

### Secure transport, MQTT identity và tunnel

P2 fixture Nginx chỉ publish HTTPS trên host loopback (published port do Compose
cấp). CA TLS riêng cho HTTPS được đưa cho client cùng hostname `localhost`. MQTT
TLS fixture certificate hiện tạo SAN `DNS:mosquitto`, `DNS:localhost`,
`IP:127.0.0.1`; broker TLS listener 8883 hiện chỉ ở Docker network, không được
publish ra host. Broker CA khác HTTPS CA. Không có MQTT host mapping mặc định;
`validate_mqtt_mapping` chỉ chấp nhận mapping **opt-in** của service owned
`mosquitto`, `127.0.0.1:<port> -> 8883/tcp` (port host hữu hạn 1024–65535), từ chối
1883/DB/Docker/admin services và wildcard bind. Helper là contract validation,
không tự thêm mapping / mở port; hiện chưa có remote fixture run.

Khi một operator chủ động bật mapping fixture đã được exact-compose-validated,
hướng dẫn tunnel mẫu (placeholder, không thực thi trong task này):

```sh
# Trên B: tunnel đến host A đã được operator cấp quyền; map HTTPS loopback fixture
ssh -N -L 127.0.0.1:18443:127.0.0.1:<A_NGINX_HTTPS_HOST_PORT> \
  -L 127.0.0.1:18884:127.0.0.1:<A_MQTT_TLS_HOST_PORT> <SSH_USER>@<A_HOST>
# Client trên B: https://localhost:18443 và mqtts://localhost:18884
```

## P8.9d — E4 warm RAM-dedup ON native pilot (2026-10-28)

Lệnh selector đã chạy foreground với fixture mới, parent mode 0700:

```sh
python3 scripts/tests/native_compose_smoke.py \
  --prepare /var/tmp/opencode/p8/p89-e4-warm-on-HONW0L/fixture \
  --paper-p8-e4 on
```

Run `PAPER_P8_E4_WARM_PILOT`: **PASS lane-only**, `full_p8_accepted=false`.
Artifacts (private inputs, identities, CA/journals and run log không đưa vào tài liệu):
`/var/tmp/opencode/p8/p89-e4-warm-on-HONW0L/fixture/artifacts/` — `run.json`,
`p8-e4-on.json`, `p8-e4-on-metrics.csv`, `p8-e4-on-resources.csv`. Runner log
được bảo vệ mode 0600 ở parent `native-e4.log`.

- Unique: Go paperload tạo đúng 20 journal entries / 20 unique offers, 100 samples
  mỗi message. Đã bỏ qua ACK hợp lệ đầu tiên, retry 1 lần, publish 21 lần, nhận
  21 broker PUBACK và 20 application ACK; pending rows/messages = 0.
- Replay: cùng journal, không phát generation mới; 10 exact-prefix messages được
  offer/replay-ACK, 10 broker PUBACK, pending = 0. Scoped DB deltas tổng cộng
  ledger/raw/temporal/configuration = **20/2.000/20/0**; mỗi Gateway có
  10/1.000/10. Fingerprint unique-ACK và replay-end giữ nguyên.
- Same-process admin JSON `ram_dedup` delta: **10 lookup hits, 10
  database_calls_avoided**; persistence delta: 0 begin attempts/transactions,
  0 commits, 0 confirmed new rows, 0 COPY. `DuplicateCommitted=10` được giữ riêng
  là ingress outcome của các RAM re-ACK, không gán nhầm thành DB duplicate.
- Histograms available riêng cho unique/replay. Histogram latency trung bình ACK
  tại generator là 0,0983s / 0,0332s (không phải server dispatch latency).
  Backend histogram: replay ACK-publish p50/p95/p99 = 0,5/0,5/0,5ms; unique
  persistence p50/p95/p99 theo upper bucket = 25/100/250ms. Replay persistence
  không có mẫu nên percentiles là `null`, không phải 0. Metrics CSV chứa buckets
  thực đo; các percentiles là xấp xỉ upper-bucket.
- Resources: 30 fixture service snapshots và 14 generator snapshots; service
  `resources.csv` ghi 2 snapshot mỗi service. Backend max RSS 24,6 MiB; PostgreSQL
  max RSS 71,8 MiB (sparse fixture sampling, không phải peak/continuous profile).
- Exact-owned cleanup PASS: **12 containers, 1 network, 5 volumes**; selector
  không dùng shared prune. Earlier FAILs được giữ nguyên: parent
  `p89-e4-warm-on-D4gB1Z` (manifest ACK-ignore validation), `...9ZtfmF`
  (stale E4 CLI guard), `...WQM0iM` (metrics/CSV validation), và `...ns7p1s`
  (ingress outcome bị diễn giải nhầm thành DB duplicate); mỗi lượt xác nhận
  `cleanup_verified=true`.

Đây chỉ là warm cache-ON local native pilot/synthetic telemetry; không phải
P8 acceptance, capacity, SQLite/durable Gateway restart, cold restart hoặc remote
A/B. Cache-OFF, cold restart, conflicting payloads, E5, WebSocket proof, official /
high-load, remote/full CI và HIL đều **NOT RUN**; không tiếp tục các lane này từ
kết quả E4.

Không thay hostname bằng IP tùy tiện; TLS phải xác minh CA đúng loại và SAN
`localhost`, giữ HTTPS `auth_request`/JWT/Origin và MQTT TLS + DynSec ACL. Không
dùng insecure flags, credentials trong URL/argv/CSV, public cleartext, hoặc đưa DB,
Unix auth/admin, Docker socket, 1883 ra tunnel. A giữ collector/admin/Auth Admin/DB;
B chỉ nhận role JWT và MQTT identity riêng cho mỗi Gateway. Tunneling SSH có thể là
bottleneck và **không** đại diện direct LAN throughput; phải ghi RTT/throughput/tunnel
qualification riêng. Cặp lệnh chỉ là template để điều chỉnh theo published port
đã inspect trên A, không phải endpoint đang hoạt động.

### Client bundle boundary và qualification

`prepare_client_bundle` chuẩn bị public manifest (run ID, HTTPS/MQTT TLS endpoints,
topology, targets, workload, SHA-256 public CA hashes tách riêng `https`/`mqtt`) và
private files mode 0600 tách role JWT với từng Gateway MQTT client ID/username/
password, trong parent mode 0700. Bundle ghi rõ `preparation-only`; không dựng control plane/remote runner.
Public artifact không serialize identities; forbidden admin/Auth Admin/DB/
service_role/operator key bị từ chối. Chuẩn bị bundle không đồng nghĩa B run đã
thực hiện hoặc secret đã được giao an toàn.

Focused tests 16 PASS; `py_compile` và `git diff --check` PASS. Generator preflight
đo tại artifact log: process soft/hard 1.048.576/1.048.576, OS file-max 9.22e18,
file-nr 3264/0/9.22e18, ephemeral 32768–60999 (28.232 ports); estimate FD cần 192
cho pilot64, 320/576/20.064 cho rung 128/256/10.000. Đây là snapshot host-local,
không phải sizing guarantee cho máy B.

Native sessions lane ngắn chạy lại tại
`/var/tmp/opencode/p8/p83b-20261028/fixture-rerun/`: PASS,
`PAPER_P8_3A2_SESSION_LANE_PASS`, `full_p8_accepted=false`; exact cleanup 15
containers / 1 network / 7 volumes. Readback ở 64 sessions: Nginx configured
`worker_processes auto`, `worker_connections=1024`, required slots 160 (2/viewer +
32 reserve), fit=true; Nginx PID1 soft/hard FD 1024/524288. Backend/broker soft FD
limits được đọc, nhưng open-FD listing không đủ dữ liệu để xác định descriptor count
nên availability là null với reason, không diễn giải thành 0. Sau sửa lỗi đếm FD, lượt
đầu `/fixture/` giữ nguyên như historical invalid resource-readback artifact (các
giá trị open=0 không được dùng); history ở `failure-history.txt`. Rerun ghi
`dirty_source_sha256=941cc8b9...`, chỉ fingerprint của rerun này; không tuyên bố
đồng nhất với P8.3a2/a3. Log tập trung tại cùng thư mục `/var/tmp/opencode/p8/p83b-20261028/`.

P8.3a2 64-session và P8.3a3 slow-viewer là các lượt riêng, không cùng fingerprint;
slow-viewer evidence là `ping_timeout`, không phải queue overflow. Remote A/B, actual
SSH tunnel, mapped MQTT TLS over tunnel, remote FD/port readback, E1–E5/Go-Python
load tools, full CI, HIL/AM5728 và official paper run vẫn **NOT RUN**. P8.3b chưa
nghiệm thu P8.

## P8.7 — bounded native E2 tooling pilot (2026-10-28)

Lệnh thực thi:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare \
  /var/tmp/opencode/p8/p87-e2-native-20261028-124524-2057062/fixture --paper-p8-e2
```

Run `ncs_eca873038ba8`: `PAPER_P8_E2_PILOT`, lane PASS, `full_p8_accepted=false`;
exact-owned cleanup PASS (12 containers, 1 network, 5 volumes). Artifacts:
`/var/tmp/opencode/p8/p87-e2-native-20261028-124524-2057062/fixture/artifacts/`;
protected console log is at its parent `console.log`. The command used a fresh
mode-0700 parent; secrets/effective Compose output remain private. Effective
shaper/microbatch profile was read back as 2/s, burst 4, 32 messages, 500 rows,
4 MiB, 50ms; six other feature flags were false. Schema readback was
`24:telemetry_ledger_timestamp`.

For **each** sample profile (100 and 1), runner-observed A steady overlapped B burst
(3.036ms and 0.778ms respectively; pipe observation lag unknown). Each profile had
24 unique application ACKs, 49 backend receives, 24 admitted and 25 shaper rejects;
B generated 16 unique messages but had 41 publish attempts, 25 retries, and 16
application ACKs. A generated/published/ACKed 8/8/8 with no retry. B had pending
messages/bytes 0/0 at completion. Scoped DB deltas were ledger/temporal/configuration
24/24/0 and raw 2,400 (100 samples) or 24 (1 sample); A had 8 ledgers, B 16, with
matching raw/temporal counts and per-child raw identity verification. Per-child
unique payload bytes were 100-sample A/B 8,158/17,364 and 1-sample A/B
2,845/5,698. ACK latency count/mean (client-local, measured through app ACK) was
100-sample A/B 8/77.862ms and 16/1,550.517ms; 1-sample A/B 8/59.117ms and
16/1,621.556ms. Per-child publish attempts, PUBACKs, app ACKs, retries, pending
state, latency count/total, and bytes are preserved in `workloads.{A,B}.telemetry`
in `p8-e2-pilot.json`.

Same-instance stage histogram windows were assembled from `ingress.Durations` for
queue_wait, validation, batch_wait, group_persistence, persistence, and ack_publish.
Bucket counts/ranges and overflow are retained per profile; no stage percentiles
were added. Every window was available; group_persistence observed 23 samples and
the other stages 24 per profile. Client ACK elapsed uses each generator's own clock;
clocks are not compared across children. Resource capture retained 20 server
snapshots and 18 generator samples per profile. Server rows include CPU/memory
percentages and usage by service. A generator CPU baseline sample is unavailable
(`generator_cpu_baseline_unavailable`), not treated as zero. This colocated finite
tooling pilot is not capacity, fairness, or official acceptance evidence.

Historical failed invocations remain FAIL with cleanup verified: the first failed
before workload due to dynamic helper import (`No module named 'scripts'`); the
second rejected effective flag validation because `MQTT_MICROBATCH_MAX_WAIT` is
`50ms` (duration string), not `50`. They were not rewritten as PASS. Remote/official
runs, E3/E4/E5, restore, full/remote CI and HIL/AM5728 remain **NOT RUN**.

## P8.4a–P8.4c — preflight, telemetry synthetic-load và viewer client (2026-10-28)

Đã thêm `src/cmd/paperload` và `src/internal/paperload`: manifest schema v1
được decode strict, validate bounds/combinations trước mọi network access. `--preflight`
vẫn deterministic và không đọc private files/dial/tạo output. P8.4b thêm role telemetry
MQTT TLS, bounded in-memory pending/retry và application ACK matching. P8.4c thêm
viewer-only WSS client; control producer, durable Gateway và Python run lifecycle/
collectors vẫn chưa implement. Original
[`workload-manifest.json`](results/p8/workload-manifest.json) tiếp tục là
`PLANNED_INPUT_NOT_MEASURED`, không phải input executable. Executable-shaped
examples rõ ràng là [`example-e1-pilot.json`](results/p8/example-e1-pilot.json)
và [`example-e2-pilot.json`](results/p8/example-e2-pilot.json); endpoints/file
refs là placeholder, không có credentials hay machine paths.

CLI help hiện hành (`cd src && go run ./cmd/paperload --help`):

```text
paperload — bounded P8 manifest preflight
Usage:
  paperload --manifest FILE [--output-dir DIR] --preflight [--official-opt-in]
  paperload --manifest FILE [--output-dir DIR] --run [--official-opt-in]
```

Ví dụ build binary rồi preflight local E2 hoặc remote E1 (không cần Docker/DB/
admin credentials, không dial, không tạo output/artifacts):

```sh
cd src && go build -o /var/tmp/opencode/p8/paperload ./cmd/paperload
/var/tmp/opencode/p8/paperload --manifest ../docs/backend/results/p8/example-e2-pilot.json \
  --output-dir /var/tmp/opencode/p8/e2-pilot --preflight
/var/tmp/opencode/p8/paperload --manifest ../docs/backend/results/p8/example-e1-pilot.json \
  --output-dir /var/tmp/opencode/p8/e1-pilot --dry-run
```

Python bridge nhận binary/manifest/output paths và cờ boolean; gọi argv (no shell)
với timeout tối đa 30s:

```sh
python3 scripts/paper_p8.py --binary /var/tmp/opencode/p8/paperload \
  --manifest docs/backend/results/p8/example-e1-pilot.json \
  --output-dir /var/tmp/opencode/p8/e1-pilot --preflight
```

Contract gồm E1–E5, arm/run ID/seed, pilot hoặc official, local/remote, roles
`telemetry|viewer|control`, samples/message đúng 1 hoặc 100, gateway/sensor/user/
viewer counts, target/subscription mapping, finite rate/ramp/burst và phase times,
retry/worker/connection/pending/disk/wall bounds, TLS HTTPS/MQTTS endpoints + CA
file refs tách biệt, private role identity file refs và bundle path. Telemetry-only
được khai báo `users=0`, `viewers=0`; estimate `connections`/`selected_role_connections`
đếm Gateway MQTT thực sự được target, không lấy `limits.connections` làm số đo. Expected delivery
được dedupe theo `(viewer,(gateway_id,sensor_id))`, nên Gateway+Sensor overlap chỉ
tính một lần; distinct user được đếm theo user ID, subscription theo composite
identity. Telemetry role khai báo thêm private file ref riêng cho từng Gateway
được target (`gateway_identity_files`), không serialize credential contents.
Giới hạn 16 filter entries/viewer; pilot tối đa 64 viewer, 10.000 chỉ
cho official và luôn bounded. Official profile cần `--official-opt-in`. Endpoint
userinfo/query/fragment, scheme không TLS và restricted service ports bị từ chối.
Preflight không đọc file contents/permissions, không kiểm endpoint reachability
hay effective host capacity. `durability_mode=durable` bị từ chối rõ cho tới lát cắt
Gateway durable sau này; synthetic ghi `durable_gateway_count=0`.

### Synthetic telemetry run contract

Gateway identity file private là JSON mode `0600` (parent directory nên `0700`),
chứa đúng `{"username":"<gateway_id>","password":"<DynSec password>"}`.
Username phải bằng Gateway ID. Không truyền credentials qua argv/environment/manifest
hoặc kết quả; endpoint `mqtts://` được chuyển nội bộ sang Paho `ssl://`, CA và hostname
được xác minh. `mqtt_tls_server_name` chỉ dùng cho tunnel có SAN khác hostname; không
có insecure TLS mode. Output path phải mới và kết quả `workload.json` được tạo mode
`0600`.

Ví dụ chạy nhỏ sau khi operator đã provision fixture/credential và đặt bundle:

```sh
cd src && go build -o /var/tmp/opencode/p8/paperload ./cmd/paperload
/var/tmp/opencode/p8/paperload --manifest ../docs/backend/results/p8/example-e2-pilot.json \
  --output-dir /var/tmp/opencode/p8/p84b-pilot --run
```

Engine chờ own-ACK positive SUBACK trước khi sinh. Nó giới hạn workers, publish queue,
pending messages/bytes và retry interval; open-loop opportunity, generated unique,
unique offered (lần đầu worker thực sự gọi Publish), publish attempt, broker PUBACK và
application `telemetry_ack` là bộ đếm riêng. Payload đã enqueue nhưng chưa được gọi
Publish không tính là offered; retry chỉ được đếm khi lần Publish retry thực sự bắt đầu.
Retry dùng đúng payload bytes/ID/sequence/timestamp. 100 samples/message dùng stream synthetic
2 Hz (99 mẫu cách nhau 0,5s, span 49,5s), 1 sample/message độc lập; cả hai timestamp
được backdate từ workload bound và không đại diện sampling Hz vật lý. Ngừng generation
trước bounded drain; còn pending thì `PARTIAL`, không xóa để gọi PASS. Latency là local
monotonic publish-to-application-ACK ở mức client loop khi có mẫu; không có server
metrics/DB claims.

Validation/focused fakes không thay MQTT native pilot. Trong P8.4b **chưa chạy broker
pilot**, E1/E3/E4/E5, durable Gateway, viewer/control, Python orchestration/collectors,
server-side accounting, full-scale/official runs, remote A/B hoặc HIL; các mục đó vẫn
NOT RUN và không có claim capacity/paper acceptance.

### P8.4c viewer-only contract

`--run` nhận role viewer khi manifest chỉ khai báo `roles=["viewer"]`; kiểm tra
manifest diễn ra trước network. `identity_files.viewer` trỏ tới private regular JSON,
mode `0600` (tối đa 1 MiB):

```json
{"user-a":"<JWT for user-a>","user-b":"<JWT for user-b>"}
```

Định dạng là bare mapping như `prepare_client_bundle` ghi; key khớp
`subscriptions[].user_id` như label, không cần bằng JWT `sub`. Client đọc `sub`/`exp`
chỉ để đếm principal khác nhau và preflight expiry, không xác nhận quyền. Hai label
cùng `sub` tính là một principal. JWT phải còn hạn lâu hơn
`limits.wall_seconds`; server vẫn là bên xác thực/phân quyền thật. Kết nối là
`wss://<https_endpoint-host>/v1/ws`, CA từ `https_ca_file`, xác thực hostname bắt buộc,
subprotocol `iot.telemetry.v1` được kiểm tra sau handshake và `Authorization: Bearer`; không có query credential,
Origin override hoặc TLS bypass. Mỗi socket gửi group filters và chỉ tính subscribed
với ACK chính xác (v1/request/action/targets). Mọi event được kiểm composite Gateway/
Sensor và URN; dedup tuple gồm Gateway, Sensor, message UUID, boot UUID, first_sequence.
`viewer.identity_window` (default 1024, max 100.000) là dedup window trên mỗi viewer;
runtime còn giới hạn tổng cửa sổ 250.000 identity và báo effective size. Settings tùy
chọn: `mode=healthy|slow|reconnect` (default healthy), `slow_pause_ms` 1..30.000,
`reconnect_count` 0..10, `max_messages` tối đa 10.000.000, `max_bytes` tối đa 1 GiB;
`workers` giới hạn đồng thời kết nối/subscribe, slot được thả sau ACK và mỗi socket
được duy trì bởi một joined reader. Warmup rồi measure kết thúc bằng stop sạch; reconnect
mode chủ động đóng/kết nối lại số vòng đã cấu hình trong khoảng run. Malformed event,
namespace sai, oversize frame, byte/message cap, unexpected close đều làm run fail/partial,
không bị bỏ qua. Reader/frame/socket concurrency/run wall đều hữu hạn. Reconnect không tuyên bố durable
replay. Không có publisher origin trong role này nên latency ghi unavailable, không
ước lượng từ `measured_at`.

Artifact mới `workload.json` mode `0600` (output dir phải mới) chứa status, effective
window, socket/principal counts, connected/subscribed/receiving, unique/duplicate/
wrong namespace, bounded message/byte counts, close reason counts, socket lifetime,
latency availability/reason và safe failure class; không có JWT hoặc payload. TLS,
JWT, bad ACK, Origin/authorization denial và socket failures không được retry để che
lỗi; reconnect mode chỉ lặp lại sau read/socket close. Unit httptest không phải native
pilot. P8.4c **native viewer role, E1 pilot/full, remote A/B, 64+ sessions, E1 revoke/
 slow integration, E2–E5, Python lifecycle/collectors, full-scale/official, HIL đều
NOT RUN**; không suy ra E1 complete hay paper/server acceptance.

## P8.6a — E1 mapping, readiness và execution-plan builder (implementation only)

`scripts/tests/paper_p8_support.py` cung cấp `build_e1_mapping(...)` cho topology
hai Gateway, tối thiểu hai Sensor/Gateway, 64 viewer mặc định và hai user. `density`
là `sparse|dense|overlap`; mỗi socket tối đa 16 filters. Recipient map dùng
composite `(gateway_id,sensor_id)` và viewer-set dedup nên Gateway-wide + Sensor
filter chỉ giao một lần/session. `e1_expected_deliveries(mapping, unique_messages_by_target)`
tính từng target từ unique message count thực, không nhân batch tổng với mọi viewer.
Counts gồm sockets, users, filters, targets và receiving viewers. Official rung
preset: 32, 64, 128, 512, 1.000, 2.000, 5.000, 10.000; >64 cần explicit
`official_opt_in=True`. Đây chỉ là mapping/preset, không chạy tải.

Viewer Go ghi `ready.json` mode 0600 bằng rename atomic sau khi toàn bộ socket đã
nhận subscribe ACK hợp lệ và reader goroutine đã chạy; marker không chứa JWT.
Python `--run-spec` sắp role cố định viewer → telemetry → control, chờ marker
bounded (tối đa 120s) trước khi start producer, từ chối child exit/malformed marker.
Viewer giữ socket trong warmup, measure **và drain**, để publisher có drain window
hữu hạn. `workload.json` giữ tối đa 20 header/hash identity samples toàn cục, tối
đa 1/viewer; payload/value không được lưu. Qualification nêu rõ đây là tập mẫu,
không phải full-content audit. `build_e1_execution_plan(...)` nhận hai manifest Go
role đã provisioned (`viewer`, `telemetry`, remote pilot, cùng seed), ghi manifests
và run-spec private; worker/security/queue profile phải giữ giống trong hai inputs.
Ví dụ:

```python
from scripts.tests.paper_p8_support import build_e1_execution_plan
plan = build_e1_execution_plan(output_dir='/secure/run', viewer_manifest='viewer.json',
    telemetry_manifest='telemetry.json', seed=84)
```

Builder chỉ tạo execution-plan; chưa chạy load. P8.6a tests không phải native
evidence; **native E1, remote A/B, full viewer rungs và official experiments NOT
RUN**. Historical P8.5 artifacts không được coi là cặp mới.

## P8.4d — Python bounded Gateway child supervision

`scripts/paper_p8.py` hiện phân biệt bắt buộc `--preflight` (không network/ghi)
và `--run`. Preflight tương thích một manifest đơn; `--run` nhận manifest đơn
hoặc `--run-spec` JSON. Runner giới hạn tổng process owned tối đa 16 và pilot tối đa
2 durable Gateway process, dùng argv không shell, không kế thừa environment deployment (chỉ PATH/LANG/LC_ALL/TZ/TMPDIR)
và không `source .env`. Run spec hỗ trợ tối đa 16 durable simulator con, nhưng
pilot nên dùng tối đa 2. Ví dụ cấu trúc:

```json
{
  "paperload": [{"manifest": "control.json", "output_dir": "out/control"}],
  "gateways": [{"env_file": "private/gateway-a.json", "role": "executor",
                 "ready_timeout_seconds": 30}],
  "stop_timeout_seconds": 30
}
```

Build binary do operator cung cấp, sau đó chạy tường minh:

```sh
cd src && go build -o /var/tmp/opencode/p8/paperload ./cmd/paperload
cd src && go build -o /var/tmp/opencode/p8/gateway ./cmd/gateway
cd ..
python3 scripts/paper_p8.py --binary /var/tmp/opencode/p8/paperload \
  --gateway-binary /var/tmp/opencode/p8/gateway --run-spec run-spec.json \
  --run-artifact /var/tmp/opencode/p8/run.json --run
```

`gateway-*.json` là private regular file mode 0600, flat mapping `GATEWAY_*`
giới hạn theo allowlist của runner. Cần `GATEWAY_CONTROL_ENABLED=true`, Gateway
và Sensor identity đã provision, `GATEWAY_MQTT_USERNAME` giữ nguyên Gateway ID,
client ID MQTT khác identity và unique theo connection, CA file TLS, và
`GATEWAY_OUTBOX_PATH` persistent, riêng từng process. Broker URL vẫn phải TLS;
không có insecure option. Optional `GATEWAY_MQTT_SERVER_NAME` chỉ override tên
xác minh TLS cho tunnel và verification không bị tắt. Không đổi Gateway identity/
membership để giả lập nhiều Gateway. Một Sensor connection/process là simulator
timer-driven; không tính là open-loop offered-rate benchmark.

Runner chờ chuỗi readiness thật `gateway simulator running ...` trước khi start
paperload producer. Dừng producer trước, SIGTERM Gateway để scheduler ngừng tạo
mới và bounded-drain/join worker theo shutdown timeout hiện có; không xóa SQLite.
Sau process exit, gọi `gateway count` read-only để đọc pending/quarantined trước
khi flush `run.json`. Lỗi gốc và cleanup errors tách riêng; thiếu/malformed
`workload.json`, pending rows, timeout hoặc child không joined cho kết quả
`PARTIAL`/failure, không lấy exit code 0 làm PASS. `run.json` lưu role/status,
manifest/env-file hashes, process count, readiness/stop, pending readback, thời
gian runner-local và limitation; không lưu credentials. Outbox path chỉ được
hash trong artifact.

Slice này chỉ wiring remote/B process đã provision sẵn: paperload cần HTTPS/MQTT
endpoint, CA và role private files trong manifest refs; Gateway env file private
chứa MQTT identity. B không cần Docker, DB hoặc admin credential. Local disposable
Compose preparation/cleanup chưa được nối vào runner. E3 durable command end-to-end,
E4 reopen/ACK-loss, resource/server collectors và E1–E5 pilots **NOT RUN**; đây
không phải nghiệm thu P8.4 hoặc paper acceptance. Lifecycle regressions là controlled
fake-child tests, không phải native broker run.

## P8.4f — local E2 telemetry100 disposable smoke

Exact selector and command:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare \
  /var/tmp/opencode/p8/p84f-<run-id>/fixture --paper-p8-load
```

Only this opt-in owned fixture maps Mosquitto TLS target 8883 to dynamic host port
0 bound to `127.0.0.1`; it reads back the assigned port and validates service,
bind and target. No 1883, DB, Auth socket or Docker socket exposure. It reuses P2
HTTPS/Auth request, Unix socket, DynSec/ACL, schema 24, actual Gateway/Sensor
provisioning and exact-owned cleanup. Broker TLS verifies `localhost` SAN and its
fixture CA; no TLS bypass. Go `paperload` and existing Gateway target are built
before services/workload window; old Gateway emitter is not a paperload substitute.

Run is limited to two provisioned Gateway/Sensor targets and one E2 profile:
20 unique batches, 100 samples/message, 20/s × 1s measurement, one Paho transport
per Gateway, 8s bounded drain. Acceptance checks generated/offered/PUBACK/valid
application ACK all equal 20, pending count/bytes zero, DB delta ledger 20, raw
2,000, temporal 20 and reported configuration 0. Artifacts record effective
schema/quota/flags, endpoint and CA hashes, source/image/binary/manifest
fingerprints, workload counters and final exact cleanup. `full_p8_accepted=false`.

Role JWT bundle is owner-only and each Gateway has a separate private credential
file; no client admin/Auth Admin/DB/service-role input. Public manifest contains
only endpoints/topology/workload/CA hashes. Paperload target IDs now use the same
Gateway/Sensor validators as provisioning before any network access; generator
failure cannot become COMPLETE with fewer unique batches. 1-sample profile,
steady/burst comparison, collectors, E1/E3/E4/E5, remote A/B/tunnel, HIL/AM5728,
restore, full CI and official paper experiment remain **NOT RUN**. No capacity
claim follows from this colocated smoke.

## P8.5a — Bộ thu thập tái sử dụng và kế toán cửa sổ / cohort (2026-10-28)


Lát cắt P8.5a bổ sung bộ thu thập `paper_p8_support.BoundedCollector` và kế toán
cửa sổ/cohort vào luồng smoke E2 20-batch hiện hữu, tái sử dụng các quy ước P1:

1. **Thu thập máy A và máy B tách biệt vai trò:**
   - Máy A: Scrape admin metrics qua `/v1/admin/metrics` bằng token admin trong
     closure của server helper, không chuyển token sang paperload hay serialize ra
     public manifest. Request timeout 2s, response giới hạn dưới 64 KiB, chu kỳ lấy
     mẫu 0,5s, tối đa 32 snapshots. Scrape baseline bắt đầu đồng bộ trước khi child
     process chạy; scrape terminal diễn ra đồng bộ sau khi generator và drain hoàn tất.
     Container Docker stats của server chỉ thu thập tại hai mốc biên (`workload_start`
     và `workload_end`) để tránh việc gọi lệnh CLI làm nghẽn chu kỳ lấy mẫu trong cửa sổ đo.
   - Máy B: Thu thập tài nguyên generator process tree qua `/proc` (RSS bytes và CPU
     ticks). Theo dõi theo từng tuple `(pid, starttime)` để khi process con kết thúc
     không bị giảm ticks gây ra delta âm. Chuẩn hóa CPU theo phần trăm host
     (`delta_ticks / CLK_TCK / elapsed / host_cpu_count * 100`), ghi rõ không phải
     Docker per-core quota. Trường hợp thiếu mẫu đầu hoặc lỗi đọc được đánh dấu `unavailable`
     kèm lý do (`generator_cpu_baseline_unavailable`, ...), không gán 0.
   - Khi subprocess kết thúc hoặc gặp bất kỳ ngoại lệ nào (`OSError`, `TimeoutExpired`,
     v.v.), collector được stop và join trong khối `finally` với bounded timeout 12s,
     đảm bảo không còn luồng daemon nào tiếp tục scrape trong giai đoạn dọn dẹp fixture.

2. **Kế toán cửa sổ, histogram và cohort:**
   - Cửa sổ đo pilot: Để cho phép lấy mẫu chu kỳ hữu hạn (interval 0,5s) mà không bị lỡ
     cửa sổ 1s, tham số pilot được điều chỉnh thành 20 batches trong 4s (rate 5 msg/s).
     Cả hai giá trị proposed (20 msg/s, 1s) và effective (5 msg/s, 4s) được ghi minh bạch.
   - Histogram delta: Tái sử dụng `paper_p1_histogram_window`, chỉ so sánh khi cùng
     `process_instance`, delta bucket không âm, percentile dạng upper bound (overflow
     là `infinity`).
   - Ingress metrics backend: Tính toán delta từ các trường thực tế trong `ingress`:
     `Received`, `Accepted`, các bộ đếm từ chối (rejections), `Committed`,
     `DuplicateCommitted`, `SamplesNew`, `ACKPublished`. Phân biệt rõ `backend_observed`
     (số batch nhận ở tầng ứng dụng) với wire traffic. `outbox_deleted` trong synthetic
     mode là `not_applicable_synthetic_workload`.
   - Engine Go `paperload`: Lưu nhãn cohort (`warmup`, `measure`, `burst`) trên từng
     identity; sau khi drain, application ACK được ghi nhận đúng theo cohort gốc thay vì
     tính nhầm vào cửa sổ đo sau ranh giới cutoff.
   - Đối chiếu DB có scope: `sensor_scoped_delta` kiểm tra trên từng cặp `(gateway_id, sensor_id)`,
     từ chối duplicate key, thiếu trường `ledger`/`raw`/`temporal`, hoặc counter reset / delta âm.
     `verify_pilot_raw` kiểm tra tính toàn vẹn của sequence, sample_index và giá trị sóng tam giác.

3. **Lịch sử các lần chạy thất bại được bảo lưu:**
   - Lần 1 (`/var/tmp/opencode/p8/p85a-20261028/fixture`): Thất bại `STARTUP_FAILED` tại phase
     `prepare` do thư mục scratch cha chưa tồn tại (`FileNotFoundError`). Không có tài nguyên hay
     log thô nào được sinh ra.
   - Lần 2 (`/var/tmp/opencode/p8/p85a-run-20261028-103500/fixture`): Thất bại `STARTUP_FAILED`
     với `paper_p1_resources_rejected` do định dạng `MemUsage` của generator có dấu cách
     (`f"{proc['rss_bytes']} B / ..."` thay vì không cách `...B / ...B`). Toàn bộ 15 containers,
     1 network, 7 volumes đã được dọn sạch (`cleanup_verified: true`).

4. **Kết quả xác minh native thành công:**
   - Lệnh thực thi:
     ```sh
     python3 scripts/tests/native_compose_smoke.py \
       --prepare /var/tmp/opencode/p8/p85a-run-20261028-102501/fixture --paper-p8-load
     ```
   - Trạng thái trả về:
     `{"status": "PAPER_P8_4F_E2_TELEMETRY100_SMOKE", "p8_4f_e2_telemetry100_smoke_pass": true, "full_p8_accepted": false, "native_executed": true, "exact_cleanup_verified": true, "cleanup_counts": {"container": 12, "network": 1, "volume": 5}, "artifact": "p8-e2-smoke.json", "schema_version": 1, "project": "ncs_0d5f11a5f40d"}`
   - Thư mục artifact thực tế:
     `/var/tmp/opencode/p8/p85a-run-20261028-102501/fixture/artifacts/` chứa đầy đủ:
     - `metrics.csv`: 9.945 dòng số liệu time-series qua 11 mốc snapshot (`workload_start`, 9 mẫu chu kỳ, `workload_end`).
     - `resources.csv`: Các snapshot tài nguyên server và generator B (`MemUsage`, `CPUPerc`).
     - `run.json`: Chứa metadata, accounting, cohort counters, scoped DB deltas và histogram windows.
     - `p8-e2-smoke.json`: Bản sao bền vững phục vụ đối chiếu.
   - Đối chiếu số liệu thực tế:
     - Workload counters: `generated_unique=20`, `offered_unique=20`, `publish_attempts=20`, `broker_pubacks=20`, `application_acks=20`, `measure_application_acks=20`, `pending_messages=0`, `pending_bytes=0`.
     - Ingress deltas backend: `Received=20`, `Accepted=20`, `Committed=20`, `DuplicateCommitted=0`, `SamplesNew=2000`, `ACKPublished=20`, rejections=0.
     - Database deltas: `ledger=20`, `raw=2000`, `temporal=20`, `configuration=0`.
     - Scoped deltas: Cả hai sensor (`gw_p0_a_279a5018` và `gw_p0_b_9ce046d7`) đều đạt đúng `ledger=10, raw=1000, temporal=10`.
     - Pilot raw verification: Cả hai sensor đều `verified_exact: true`, `ledger_count: 10`.
     - Generator CPU/RSS: 9 mẫu chu kỳ quan sát được CPU từ 0,97% đến 3,91% host-normalized, RSS ~35-36 MiB.
     - Dọn dẹp tài nguyên: Xác nhận dọn sạch chính xác 12 containers, 1 network, 5 volumes.

5. **Giới hạn của lát cắt P8.5a:**
   - Đây là bước hoàn thiện collector và accounting cho smoke cục bộ 20 batch; **chưa phải nghiệm thu E2 chính thức hay bằng chứng năng lực chịu tải**.
   - Chưa triển khai scan mode, so sánh các arm ablation (shaper off, per-message, dedup off), E1-E5 quy mô lớn hay kiểm thử HIL/AM5728. P8.5 chưa hoàn tất.

## P8.5b — Opt-in fan-out scan baseline và hồi quy tương đương (item 10)

Lát cắt P8.5b bổ sung cơ chế scan fan-out opt-in làm baseline so sánh với cơ chế indexed mặc định theo đúng mục 10 của P8.5:

1. **Cấu hình và biến môi trường:**
   - Biến môi trường: `WS_FANOUT_MODE` với giá trị mặc định `indexed` và giá trị opt-in `scan`. Chỉ chấp nhận duy nhất hai giá trị này (chuỗi rỗng mặc định `indexed`; mọi giá trị khác bị từ chối với lỗi validation).
   - Ánh xạ đầy đủ: `.env.example`, `docker-compose.yml`, `src/internal/config/config.go` (`WebSocketConfig.FanoutMode`), `src/internal/config/websocket.go`, `src/cmd/server/websocket.go` truyền sang `telemetryws.HubConfig.FanoutMode`.
   - Sáu cờ feature flags deployment tiếp tục giữ nguyên giá trị mặc định `false`: `WEBSOCKET_ENABLED=false`, `STAGE5_ENABLED=false`, `STAGE5_WAKEUP_ENABLED=false`, `MQTT_SHAPER_ENABLED=false`, `MQTT_MICROBATCH_ENABLED=false`, `MQTT_RAM_DEDUP_ENABLED=false`. Không thêm migration, framework hay external dependency.

2. **Ngữ nghĩa cơ chế Scan Mode:**
   - Scan mode **chỉ thay đổi duy nhất** cách chọn session candidate trong Step 1 của `Hub.FanOut`:
     - `indexed` (mặc định): Tra cứu 2 key O(1) trong `h.targetIndex` (`sensorKey` và `gatewayKey`).
     - `scan` (opt-in): Duyệt tuần tự qua danh sách active session trong `h.queues` (bị chặn bởi session cap cấu hình `<= 10000`, mỗi session `<= 16` filters). Kiểm tra `target.Matches(evt.GatewayID(), evt.SensorID())` cho từng filter đã cài đặt. Khi gặp filter khớp, ghi nhận session và `break` ngay để bảo đảm filter Gateway và Sensor trùng lặp chỉ chọn mỗi session tối đa 1 lần.
   - Giữ nguyên toàn bộ kiến trúc Hub, SessionRegistry, SessionAccessState, generation counter, target authorization, bảo trì song song `targetIndex`, hàng đợi bounded per-session queue, aggregate reference budget, slow consumer isolation (`RevocationReasonSlowConsumer`), aggregate capacity isolation (`RevocationReasonAggregateCapacity`), và shutdown. Tuyệt đối không truy vấn DB per-event.
   - Step 2 (revalidation `sessionMatchesEventLocked` trước khi enqueue), unsubscribe, và purge unsent frames giữ nguyên tính linearizable. Không xóa bỏ auth/recheck/index maintenance để tạo baseline giả tạo.

3. **Quan sát selection-path và metadata:**
   - `telemetryws.MetricsSnapshot` và `Hub.FanoutMode()` công khai `fanout_mode` hiệu lực (`indexed` hoặc `scan`).
   - Bộ đếm quan sát:
     - `fanout_indexed_selections`: Tăng 1 mỗi khi thực hiện tra cứu indexed hai key; bằng 0 trong scan mode.
     - `fanout_scan_evaluations`: Tích lũy số lượt đánh giá session active trong scan mode; bằng 0 trong indexed mode.
   - Không chứa nhãn per-identity, không bịa đặt số liệu timing hay capacity. Ring buffer `closeObservations` giữ nguyên đúng 32 phần tử.

4. **Kiểm thử tập trung (TDD RED → GREEN):**
   - RED log: `/var/tmp/opencode/p8/p85b-scan-red.log` (ghi nhận compile failure trước khi thêm code).
   - GREEN log: `/var/tmp/opencode/p8/p85b-scan-green.log`.
   - Các ca kiểm thử bao phủ:
     - `TestWebSocketFanoutModeValidation`: Mặc định `indexed`, chấp nhận `indexed`/`scan`, chuỗi rỗng -> `indexed`, từ chối các giá trị không hợp lệ.
     - `TestFanoutRecipientEquality_TwoGatewaysSameSensorID`: Cách ly namespace giữa 2 Gateway có cùng Sensor ID.
     - `TestFanoutRecipientEquality_FilterCombinations`: Kiểm tra Gateway-wide, Sensor-specific, Overlap, Non-matching, No-subscription qua cả hai mức cap 32 và 64. Đảm bảo single delivery và tập recipient giống hệt nhau giữa indexed và scan.
     - `TestFanoutRecipientEquality_MetricsNil`: Chạy an toàn không panic khi `Metrics` observer là nil.
     - `TestFanoutRecipientEquality_RepresentativeUnsubscribe`: Session hủy đăng ký không nhận event.
     - `TestFanoutRecipientEquality_StaleFanoutSnapshot`: Revalidation tại Step 2 ngăn chặn phát tới session đã hủy đăng ký hoặc đóng trong quá trình snapshot.
     - `TestFanoutRecipientEquality_SlowQueuePolicy`: Hàng đợi đầy đóng đúng lý do `SLOW_CONSUMER`.
     - `TestFanoutRecipientEquality_AggregateExhaustionPolicy`: Cạn aggregate budget đóng với `AGGREGATE_CAPACITY`, session không lỗi nhận đúng 1 delivery; không yêu cầu thứ tự victim (không claim fairness).
     - `TestFanoutSelectionPathObservation`: Chứng minh scan thực sự quét session active và indexed thực sự tra cứu index qua metrics delta.
     - `TestFanoutCloseObservationRingUnchanged`: Dung lượng ring buffer 32 không đổi.

5. **Giới hạn và so sánh native NOT RUN:**
   - So sánh đo lường native E1 (fan-out benchmark pair scan vs indexed) **chưa chạy trong lát cắt này (NOT RUN)**.
   - Không chạy lại 20-batch WS-off smoke không liên quan.
   - Không tự động commit, push, rollout hay triển khai các arm ablation khác (E1 scenario, runners, collectors refactor). Dừng tại phạm vi P8.5 item 10.

## P8.5c — Bộ hỗ trợ so sánh arm cơ chế, tính tương đương và kế toán (items 8–10, P8.10)

Lát cắt P8.5c bổ sung công cụ độc lập `scripts/paper_p8_comparison.py` và kiểm thử `scripts/tests/test_paper_p8_comparison.py` để định nghĩa hợp đồng arm, kiểm tra điều kiện tương đương trước khi so sánh, xác thực cơ chế thực sự được kích hoạt, tính toán giảm tải $\Gamma_{\text{red}}$ có tên mẫu số rõ ràng, tổng hợp thống kê trung vị/khoảng biến thiên trên các lượt lặp độc lập, và lập lịch thứ tự chạy tuần tự có seed.

1. **Định nghĩa Arm tối ưu tham chiếu và các Arm triệt tiêu đơn yếu tố (one-factor):**
   - **Tham chiếu tối ưu (`optimized`):**
     - `MQTT_SHAPER_ENABLED=true`
     - `MQTT_MICROBATCH_ENABLED=true`
     - `MQTT_RAM_DEDUP_ENABLED=true`
     - `STAGE5_WAKEUP_ENABLED=true`
     - `WS_FANOUT_MODE=indexed`
   - **Năm arm triệt tiêu cơ chế (ablation arms):**
     - `shaper_off`: `MQTT_SHAPER_ENABLED=false` (tắt shaper rate-limiting).
     - `microbatch_off`: `MQTT_MICROBATCH_ENABLED=false` (baseline lưu trữ per-message COPY; giữ nguyên giao dịch và commit-before-ACK, không suy thoái về INSERT đơn lẻ từng dòng).
     - `ram_dedup_off`: `MQTT_RAM_DEDUP_ENABLED=false` (tắt cache RAM dedup trước khi ghi DB).
     - `polling`: `STAGE5_WAKEUP_ENABLED=false` (dispatcher dùng polling loop thay vì event-driven wake-up; giữ nguyên hàng đợi và worker cách ly của control).
     - `fanout_scan`: `WS_FANOUT_MODE=scan` (duyệt scan tuyến tính thay vì index 2 khóa O(1)).
   - **Bất biến bắt buộc giữ nguyên trên mọi arm:**
     - Xác thực/bảo mật: `security_mode` (JWT HS256/RS256, Mosquitto DynSec TLS, không vô hiệu hóa auth).
     - Hạn ngạch: CPU, RAM, dung lượng hàng đợi backend (`quota`).
     - Worker pool: Số lượng worker xử lý và chính sách cách ly hàng đợi control.
     - Tính bền vững: Commit-before-ACK trên cả DB và outbox SQLite.
   - **Quy tắc phân biệt cờ:**
     - Từ chối hai đường chạy có cờ giống hệt nhau (`two_identical_paths_rejected`).
     - Từ chối nếu khác biệt nhiều hơn 1 cờ cơ chế (`multiple_mechanism_differences_rejected`).
     - Từ chối nếu cờ khác biệt không khớp arm dự kiến (`unexpected_mechanism_difference`).

2. **Quy tắc kiểm tra tính tương đương của cặp chạy (`validate_run_comparability`):**
   - Bắt buộc kiểm tra khớp chính xác các đại lượng bất biến:
     - `source_revision`, `binary_sha256`, `paperload_source_sha256`, `image_ids`.
     - `dirty_tree_hash`: Cả hai lượt chạy phải cùng clean hoặc có cùng hash diff của dirty worktree. Các lane P8 dirty tree khác nhau trong lịch sử không được ghép thành một cặp mới.
     - `schema`: Phiên bản migration database.
     - `security_mode`, `quota`, `workers`, `durability` (từ chối so sánh giữa synthetic và durable).
     - `topology`, `seed`, `phase_seconds` (thời lượng warmup/measure/drain), `cohort` (danh sách Sensor/Gateway).
   - **Chuẩn hóa workload (`normalized_workload_fingerprint`):**
     - Chỉ xóa các đường dẫn top-level đã khai báo trong helper: `run_id`, `created_at`, `endpoints`, `generated_fixture_paths`, `temp_dir`, `gateway_env_path`. Không xóa key có cùng tên ở payload/schedule lồng nhau; `timestamp`, `port`, `client_id` và identity sample vẫn là nội dung workload.
     - Không chuẩn hóa/đổi tên namespace fixture; namespace khác nhau làm fingerprint khác. So sánh cần workload không rỗng; thiếu workload/hàm băm nguồn thì không đủ điều kiện.
     - Xuất mã băm chuẩn `sha256:<hex>` để so sánh; đồng thời giữ `manifest_sha256` gốc. Adapter đọc field P8.5a (`source_revision` top-level, `actual_schema`, `backend_quota`, `effective_flags`) nhưng không biến report thiếu evidence thành đủ điều kiện.

3. **Điều kiện hợp lệ kích hoạt cơ chế (`check_mechanism_activation`):**
   - Xác định từ delta bộ đếm thực tế của lượt chạy tham chiếu:
     - `shaper_off`: `ShaperRateLimited > 0` (hoặc có áp lực bão hòa hàng đợi / burst).
     - `microbatch_off`: `multi_member_groups > 0` (đã thực sự gom nhóm > 1 message trong cửa sổ đo).
     - `ram_dedup_off`: `ram_hits > 0` (có lượt hit bộ nhớ đệm RAM sau khi đã xác nhận commit).
     - `polling`: `commands_dispatched > 0` và `wakeups_observed > 0` (có phát lệnh điều khiển và quan sát thấy sự kiện wake-up).
     - `fanout_scan`: `active_subscriptions > 0` và `events_delivered > 0` (có subscription thực và đã phân phối sự kiện qua WebSocket).
   - **Xử lý khi cơ chế không kích hoạt:** Báo trạng thái `INCONCLUSIVE` kèm lý do cụ thể (ví dụ `mechanism_inactive_no_shaper_rate_limiting`). Tuyệt đối không tính toán phần trăm cải thiện giả tạo hay gán 0% là cải thiện.

4. **Kế toán $\Gamma_{\text{red}}$, độ lưu giữ raw và tổng hợp thống kê:**
   - **Công thức $\Gamma_{\text{red}}$:**
     $$\Gamma_{\text{red}} = 100 \times \frac{C_{\text{reference}} - C_{\text{optimized}}}{C_{\text{reference}}}$$
     - Có tên metric định danh rõ ràng (`metric_name`).
     - Nếu $C_{\text{reference}} \le 0$: Báo `unavailable` với lý do `reference_zero_or_negative`, không chia cho 0.
     - Hỗ trợ tùy chọn chuẩn hóa theo số lượng message duy nhất ($C / \text{unique\_messages}$).
   - **Kiểm chứng độ lưu giữ raw (`verify_raw_retention`):**
     - Đối chiếu riêng biệt số lượng dòng raw (`raw_rows`) và mẫu dữ liệu giữa 2 arm; bảo đảm tính toàn vẹn của dữ liệu không bị nhầm lẫn với việc giảm thiểu số lượng thao tác ghi DB.
   - **Tổng hợp lượt lặp độc lập (`aggregate_independent_repeats`):**
     - Tính toán metric trên từng lần chạy độc lập trước (`per-run first`).
     - Báo cáo giá trị trung vị (`median`) và khoảng biến thiên (`min`, `max`, `range`).
     - **Quy tắc cấm:** Không nội suy khoảng tin cậy giả định phân phối chuẩn (synthetic CI) khi cỡ mẫu nhỏ; không gộp các percentile độc lập của từng run thành một phân vị p99 tổng thể giả tạo.
   - **Bộ lập lịch thứ tự chạy tuần tự (`plan_run_order`):**
     - Nhận danh sách arm, số lần lặp và seed khởi tạo.
     - Sinh thứ tự thực thi tuần tự ngẫu nhiên giả (pseudo-randomized) có khả năng tái lặp bằng `random.Random(seed)`.
     - Chỉ xuất kế hoạch chạy; **tuyệt đối không tự động thực thi ma trận thử nghiệm**. Số lần lặp chính thức $\ge 3$ là cấu hình opt-in cho các đợt chạy sau.

5. **Giao diện dòng lệnh (CLI):**
   - `python3 scripts/paper_p8_comparison.py preflight-arm --arm <arm_name>`: Hiển thị cờ hiệu lực dự kiến của arm.
   - `python3 scripts/paper_p8_comparison.py plan --arms <arm1,arm2,...> --repeats <N> --seed <S>`: Xuất kế hoạch thứ tự chạy tuần tự.
   - `python3 scripts/paper_p8_comparison.py compare --ref <ref.json> --arm-file <arm.json> --expected-arm <arm_name>`: Kiểm tra tính tương đương và điều kiện kích hoạt cơ chế của 2 file kết quả.

6. **Giới hạn và trạng thái thực thi:**
   - Chỉ chấp nhận status `PASS`, cleanup xác nhận, SHA-256 đầy đủ cho dirty tree (hoặc clean revision đã xác nhận), effective flags đầy đủ và workload đầy đủ. Dirty boolean không phải fingerprint. So sánh arm kiểm toàn bộ tập `effective_flags`, chỉ cho phép đúng cờ ablation chỉ định thay đổi; mọi cờ/cap/worker/security khác phải hiện diện và giống nhau.
   - Helper không kết luận từ report P8.5a smoke hiện có: report đó `dirty_worktree=true` nhưng không có dirty hash, cleanup chưa xác minh, WebSocket/workers/seed/workload chưa đủ và flags đều tắt. Nó không phải optimized reference; kết quả pair từ các historical dirty tree không được ghép.
   - Counter activation chỉ dùng field adapter rõ ràng: ingress `ShaperRateLimited`/`ShaperPassed`; microbatch `multi_member_groups`; RAM `confirmed_commit_warm_hits`; command `fresh_commands_dispatched`, `fresh_command_wakeups_observed`, `polling_cycles`; fanout `active_subscriptions`, `events_delivered`, `fanout_indexed_selections`, `fanout_scan_evaluations`. Missing/unknown/invalid field trả về `INCONCLUSIVE`/unavailable. Các metric mechanism này chưa có trong P8.5a report, nên helper không tự suy ra số liệu.
   - Gamma/repeats/raw helpers từ chối bool, NaN, Inf, số âm; aggregation cần run ID riêng biệt, `PASS`, cleanup xác nhận, cùng source compatibility key. Raw count trùng không chứng minh content; cần digest provenance bằng nhau.
    - Toàn bộ các kịch bản đo đạc native E1–E5 vẫn **CHƯA CHẠY (NOT RUN)**. P8.5c chỉ là helper Python; không đổi Go, không tạo số liệu, không chạy ma trận.

## P8.6 — E1 nativepilot: lần xác minh wiring (2026-10-28)

- Lượt FAIL lịch sử `/var/tmp/opencode/p8/p86-native-BjuUXruY` được giữ nguyên. Đọc đúng hai manifest trong `fixture/private/p8-e1/plan/`: viewer khai báo `experiment=E1`, `roles=[viewer]`, nhưng helper kế thừa `durability_mode=synthetic-load`; telemetry là `E2`/`telemetry`. Go `Validate` từ chối durability mode trên role không phải telemetry (`manifest.go`, nhánh hiện hữu). Viewer preflight báo `manifest_contract_rejected`, `runner.log` có `children=[]`. Đây là lỗi cấu hình helper, KHÔNG phải role-contract incompatibility; không thay đổi Go role contract/architecture.
- Sửa helper để durability mode chỉ có ở manifest telemetry; manifest viewer khai báo `viewer.mode=healthy` để marker readiness mang mode hợp lệ. Thêm regression cho phân tách role config, readiness marker có sẵn khi child vừa exit, và native viewer rows thiếu counter `wrong_namespace` ở từng row (global counter vẫn được xác thực). Không sửa Go runtime/role contract.
- P2 overlay: lịch sử native `compose_preflight_failed` là lỗi parse YAML của generated `compose.p2.yml` (Go-YAML parser tại backend environment, trước allocation); không có Python traceback/test-failure identifier gắn với log đó. Lệnh unit `scripts.tests.test_paper_p2.PaperP2Tests.test_p8_slow_selector_reuses_p2_overlay_with_websocket_only_in_fixture` PASS. Không có căn cứ để gọi đây là regression của một test P2 đã thất bại; giữ nguyên native historical FAIL, không sửa guard/ancestor permissions.
- Lượt mới nhất: lệnh `python3 scripts/tests/native_compose_smoke.py --prepare /var/tmp/opencode/p8/p86-e1-retry4-20261028/fixture --paper-p8-e1`; raw protected log `/var/tmp/opencode/p8/p86-e1-retry4-20261028/native.log`. E1 role workload đạt `COMPLETE`, validation `valid=true`; evidence hiện tại ghi 64 kết nối/subscription/receiver, 20 batch application ACK, pending 0, DB delta ledger/raw/temporal/configuration = 20/2000/20/0, 530 WS deliveries/writes, indexed fanout 20, drops 0, gauges terminal 0. Bốn target raw verification được ghi trong `p8-e1-pilot.json`; header identity sample giới hạn 20. Đây không phải acceptance: outer native report dừng ở `phase=report_validation`, `error_code=unclassified`; `run.json` là FAIL, `full_p8_accepted=false`; cleanup xác nhận chính xác 12 containers / 1 network / 5 volumes. Không đổi FAIL này thành PASS.
- Verification: E1/P2 selected Python suite PASS (37 tests); Go paperload targeted tests PASS. Chưa chạy full pipeline, official/remote E1–E5, scan/comparison, high-load, HIL/AM5728 hoặc production rollout (NOT RUN). E1 cần tiếp tục chẩn đoán lỗi `report_validation` từ raw fixture report trước khi được gọi native PASS.
## P8.6 — native E1 bounded pilot sau sửa report schema

Lệnh native thực chạy:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare /var/tmp/opencode/p8/p86-e1-final-20261028-0702/fixture --paper-p8-e1
```

Kết quả `PAPER_P8_E1_PILOT`, run `ncs_e053c236430e`; artifact
`/var/tmp/opencode/p8/p86-e1-final-20261028-0702/fixture/artifacts/p8-e1-pilot.json`.
Artifact `status=PASS`, helper validation `{valid:true, failures:[]}`, 64 sockets
trên 2 principals, local/co-located. Workload phụ trợ: 20 unique, 20 publish
attempts/PUBACK/application ACK, retry 0; DB delta 20 ledger / 2.000 raw / 20
temporal / 0 configuration; 530 WebSocket writes, drops/slow close/aggregate
capacity close đều 0. Cửa sổ telemetry cấu hình 4 giây đo + drain tối đa 8 giây
(0 warm-up/cooldown); đây không phải capacity/soak run. DB verification xác nhận
identity ranges cho 20 ledger và 0 raw identity/value mismatch; không phải audit
toàn bộ payload độc lập. Exact owned cleanup
đã xác nhận: 12 container, 1 network, 5 volume. `full_p8_accepted=false` giữ nguyên.

Artifact cũ `/var/tmp/opencode/p8/p86-e1-retry4-20261028/fixture/artifacts/p8-e1-pilot.json`
vẫn được giữ là FAIL của outer report: workload/helper đã PASS nhưng report so
sánh dict `validation` với boolean `True`; đây là lỗi schema handling, không phải
lỗi workload hay contract. Lần thử đầu tại `p86-e1-final-20261028-0700` cũng FAIL
ở prepare vì fixture leaf đã được tạo sẵn; không tính là pilot. Official viewer
rungs, remote A/B, scan-vs-index pair, E2–E5, restore và full pipeline vẫn NOT RUN.

## P8.7a — E2 paired pilot helper slice (implementation only)

`scripts/tests/paper_p8_e2_support.py` cung cấp `e2_pilot_manifests`,
`build_e2_execution_plan`, `e2_official_estimate` và `validate_e2_result`.
Preset tạo hai child telemetry độc lập trên hai Gateway đã provision, với credential
riêng cho từng Gateway/client lifetime; B không cần admin, DB hay Docker. Cả hai
sample profile (100 và 1 synthetic sample/message) giữ chung seed/topology/TLS/
quota/worker policy và 24 unique messages: A gửi 8 ở 1 msg/s trong 8s; B gửi 4
steady + finite burst 12 trong 4s. Retry 16, 500ms, workers 2, connection 1, pending
64/1MiB, drain 12s, wall 30s. “Samples” là synthetic record count, không phải
physical sampling Hz.

Phase validation chỉ dùng `observed_at_runner_monotonic_ns` của runner chung; nó
không trừ child elapsed clocks. Event thiếu/malformed hoặc không overlap làm
kết quả inconclusive/fail. Runner receipt lag không biết được và được ghi
qualification; configured schedule không thay thế phase đã quan sát. Accounting
yêu cầu actual workload 8/16 unique + application ACK, pending 0, ingress
received/admitted/rejected tách biệt, shaper drops actual, cùng DB scoped totals
24 ledger / 24×samples raw / 24 temporal / 0 configuration. Missing/reset/instance
mismatch không được hiểu là zero; histogram/resource references có thể đính kèm
mà không tạo số đo.

Official rungs 100/1.000/5.000/20.000 chỉ qua explicit opt-in và estimate helper
bounded theo duration/messages/raw rows/payload budget; payload estimate dùng kích
thước builder thực hoặc giả định bảo thủ, không phải wire bytes. Bounds bị vượt thì
từ chối, không nới giới hạn. E2 fixture profile đề xuất kế tiếp: shaper và
microbatch bật, cap hiện hữu 32 messages/500 rows/4MiB, wait cố định 50ms hoặc
100ms cho cả hai sample profile; RAM dedup, Stage 5/wakeup và WS tắt. Đây chỉ là
đề xuất helper, chưa nối fixture hay chạy native. Go telemetry JSON thêm generated,
offered-unique và attempted application-payload byte counters; retry đếm attempted
bytes lại nhưng giữ nguyên payload. Không phải MQTT wire bytes. Deployment defaults
giữ nguyên false; native E2 paired pilot/full E2 **NOT RUN**.

## P8 E3 polling — native pilot có giới hạn (2026-10-28)

Chạy cùng selector đã kiểm thử, không bật wake-up:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare \
  /var/tmp/opencode/p8/p88-e3-polling-stbdVK/fixture --paper-p8-e3 polling
```

Raw artifacts và protected console log ở
`/var/tmp/opencode/p8/p88-e3-polling-stbdVK/`; outer status `FAIL`,
`full_p8_accepted=false`. Exact owned cleanup **12 containers / 1 network / 5
volumes** PASS. Runner xác nhận hai Gateway executor process thực sự dừng; mỗi
executor có SQLite riêng. Background A: 8 unique, 8 publish attempts/PUBACKs,
8 application ACKs, 0 retry. B: 16 unique (4 steady + 12 burst), 41 attempts /
PUBACKs, 16 application ACKs, 25 retries. ACK/payload counts không thay thế DB
readback. Fixture exception trước post-run SQL accounting; do đó mục tiêu DB
background **24 ledger / 2.400 raw / 24 temporal** và command-Sensor raw counts
không được xác nhận ở lượt này (không suy đoán là zero).

Control producer hoàn tất 4/4 fresh HTTPS PATCH `202` và mỗi command tới terminal
`succeeded`, gồm **1.5, 0.5, 2, 0.75 Hz**. Hai SQLite sau writer shutdown đều có
2/2 completed receipts; final configuration lần lượt version 2 / 500.000 us và
version 2 / 1.333.333 us. Executor 1 còn **2 pending rows** (sample_count 2,
666.667 us); executor 2 là 0. Đây là blocker thật của run, không được bỏ qua hay
coi là drain. Post-run server-origin dispatch/E2E histograms không được lưu; API
client latency không chứng minh server origin. E3 qualification dùng đúng các
`observed_at_runner_monotonic_ns`: command 1 active overlap A steady, command 2
active overlap B burst (cũng A steady); không dùng offset plan thay observation.
Pipe receipt lag không biết được và child clocks không bị trừ hoặc so sánh.

Offset calibration sau pending outbox ở lần trước: command stream giữ nguyên thứ
tự/frequencies, offsets đổi từ `1000, 3800, 6000, 7200` ms thành **`1000, 3800,
10000, 12000` ms**. Lượt đã hiệu chỉnh vẫn còn hai pending ở executor 1; giữ nguyên
FAIL. Nếu sau này chạy wake-up arm thì phải tái sử dụng cùng lịch stream hiệu
chỉnh và ghi hash/qualification riêng; wake-up chưa chạy.

Các lượt FAIL trước đó được giữ nguyên: `p88-e3-polling-UMjZ5X`
(`paper_p0_profile_rejected`, chưa tạo resource; cleanup marker false),
`...-vuEmhh` (backend worker reservation: tổng 8 workers, pool 10; cleanup
11/1/5), `...-EHzbRh` (sibling import `No module named 'scripts'`, cleanup
12/1/5), `...-Z998kN` (Gateway 1 Hz × 100 samples vượt batch-span validator,
cleanup 12/1/5), `...-gVslKE` (runner partial, pending 5/1; cleanup 12/1/5),
`...-z6d0ix` (pending 2/2; cleanup 12/1/5), và `...-stbdVK` (offset-calibrated,
pending 2/0; cleanup 12/1/5). Mỗi cleanup result được giữ nguyên; không có lượt
nào được đổi thành PASS.

No-op/busy/replay probes, wake-up, E4/E5, restore, official/high-load, remote
A/B, full/remote CI, HIL/AM5728 và live rollout **NOT RUN**. Deployment defaults
không đổi; đây không phải P8 acceptance hay capacity claim.

## P8 E3 polling low-load — native pilot sau sửa finalizer (2026-10-28)

Lượt mới chạy foreground với parent riêng mode `0700` (không tạo sẵn leaf
`fixture`, vì `--prepare` tự tạo leaf):

```sh
python3 scripts/tests/native_compose_smoke.py --prepare \
  /var/tmp/opencode/p8/p88-e3-lowload-probes-final-Ya5lsv/fixture \
  --paper-p8-e3 polling --paper-p8-e3-low-load
```

Kết quả native `PAPER_P8_E3_PILOT`, artifact
`/var/tmp/opencode/p8/p88-e3-lowload-probes-final-Ya5lsv/fixture/artifacts/p8-e3-polling-low-load.json`,
protected console `/var/tmp/opencode/p8/p88-e3-lowload-probes-final-Ya5lsv/native.log`;
`status=PASS`, `p8_e3_pilot_pass=true`, `full_p8_accepted=false`. Control 4/4
commands sent, pending 0; actual DB background delta **12 ledger / 1.200 raw /
12 temporal**. Both SQLite executors were read back after stop, both have 0
pending outbox; REST final configuration and completed receipts verified. Phase
evidence records A steady interval with the first two command-active intervals
inside it; B burst is `NOT APPLICABLE` for low-load (no synthetic overlap is
claimed). Actual probes valid with no failures: offline
admission/replay `202`, busy/stale `409`, no-op/no-op replay `200`; one auxiliary
command, two receipts, outside fresh-command measurement. Exact owned cleanup
verified: **12 containers / 1 network / 5 volumes**.

The earlier low-load artifact at `p88-e3-lowload-probes-n8ruBR` remains an outer
report FAIL: finalizer read the mixed-load filename instead of the helper's
`-low-load.json`, and cleanup acceptance required obsolete pending-probe marker
after real probes had run. Those are now fixed with shared E3 artifact naming
and strict validation of actual probe status/classifications/counts; historical
FAIL artifacts are unchanged. An initial attempt in this session also failed
before setup because its `--prepare` leaf had been pre-created; it is retained
at `p88-e3-lowload-probes-final-QVKJYK` and is not treated as a pilot.

Wake-up, E4/E5, restore, official/high-load, remote A/B, full/remote CI,
HIL/AM5728 and live rollout remain **NOT RUN**. This bounded polling pilot is not
full P8 acceptance or a capacity claim.

## E4 extended diagnostic and proof corrections (2026-10-28)

Extended native E4 diagnostics at
`/var/tmp/opencode/p8/p89-e4-extended-diag-20261028-190310/fixture/artifacts/p8-e4-on.json`
record `warm_validation=true`, WebSocket proof PASS, cold-cache PASS, conflicts
PASS, durable Gateway reopen PASS, and exact cleanup **13 containers / 1 network /
5 volumes**. The historical console stdout still contains `NOT RUN` fields; it is
preserved unchanged and is not retroactively upgraded. This is a source-qualified
extended pilot, not official E4 acceptance: official TTL wall-clock (600s),
HIL/AM5728, E5, full/remote CI, and live rollout remain **NOT RUN**.

Correctness follow-up aligns future reports with validated sections (cache-off
cold lane is `NOT APPLICABLE`, not PASS), makes complete pilot acceptance require
WebSocket/cold-if-enabled/conflict/durable PASS plus exact owned cleanup, and keeps
`full_p8_accepted=false`. Durable interval evidence now names its actual source:
the SHA-256-verified original MQTT payload (raw telemetry has no interval column);
raw sequence/timestamp/count readback is checked separately. Auxiliary DB deltas
are derived strictly from the actual reopen `enqueued` summary (one initial batch
plus newly enqueued batches). Fixture UUID validation now matches runtime v4/v7.
Backend-replacement comparison preserves DNS/security option ordering and only
normalizes known order-insensitive settings. These changes qualify future output;
they do not rewrite prior artifacts. The bootstrap timeout diagnostic/25-second
create fix remains a source-qualified helper/test change, not proof that Docker
journals established all earlier failure causes.
