# P8 E3 — profile helper (low-load / mixed; polling / wakeup)

**Phạm vi:** helper E3 chạy bốn profile workload/dispatch; collector của A bắt đầu
trước khi xác nhận `begin`, B chỉ xuất inventory kết quả đã allowlist. A kiểm tra
độc lập PostgreSQL/REST và so khớp command receipts/configuration từ hai SQLite
executor. Báo cáo luôn giữ `full_p8_accepted=false`; pilot cùng máy ghi
`generator_colocated=true`, `physical_ab=NOT RUN`. Mixed chỉ được chấp nhận khi
runner-clock chứng minh command interval overlap cả A steady lẫn B burst. Không
chứng minh HIL hay triển khai.

## Quy trình operator

Mỗi lượt dùng ROOT mới/private; không tái sử dụng thư mục đã có state, bundle,
SQLite hay artifacts. Cần Docker, Python 3, Go toolchain và thư mục `src/` sạch
đủ dependency để build hai binaries. Build vào scratch private (hoặc chỉ dùng
binaries đã xác minh cùng source revision):

```sh
mkdir -m 700 -p /var/tmp/opencode/p8-e3-lowpoll-build
(cd src && CGO_ENABLED=0 go build -o /var/tmp/opencode/p8-e3-lowpoll-build/paperload ./cmd/paperload)
(cd src && CGO_ENABLED=0 go build -o /var/tmp/opencode/p8-e3-lowpoll-build/gateway ./cmd/gateway)
```

Dùng bản private copy của network config checkpoint tại
`/var/tmp/opencode/p8-e3-prepare-lowpoll-attempt4-20261030/network.json`.
Giữ interface/TLS CA/SAN/hostname và chọn host ports còn trống; không in config
hay credentials vào log. `serve` chạy ở terminal A1/background để A2 có thể điều
khiển:

```sh
ROOT=/var/tmp/opencode/p8-e3-<profile>-<unique-run>
mkdir -m 700 -p "$ROOT"
python3 scripts/paper_p8_server.py serve --root "$ROOT/A" \
  --network-config /path/to/private-network-copy.json --experiment E3 \
  --dispatch-mode polling --workload low_load --deadline-seconds 900
```

`--dispatch-mode` nhận `polling|wakeup`; `--workload` nhận `low_load|mixed`.
Giữa polling/wakeup chỉ đổi `STAGE5_WAKEUP_ENABLED`; các traffic/control manifest,
roles, số liệu profile, quota và semantics giữ nguyên. Với low-load, A/B phát 8/4
messages và B không có burst. Mixed giữ profile khai báo A/B = 8/16 và B burst 12.
Mỗi session vẫn tạo identity/gateway IDs fresh; mapping role→identity và cấu hình
executor tương ứng không đổi giữa arms.

Chờ `E3_PREPARED`/`READY` từ A2 status, chuyển nguyên `A/client-bundle` và
binaries đến B root tách biệt. Trên B:

```sh
python3 scripts/paper_p8_client.py bind --bundle "$ROOT/B/client-bundle" --workspace "$ROOT/B/workspace"
python3 scripts/paper_p8_client.py preflight --workspace "$ROOT/B/workspace" --binary "$ROOT/B/paperload" --gateway-binary "$ROOT/B/gateway"
python3 scripts/paper_p8_client.py check-connection --workspace "$ROOT/B/workspace"
```

Trên A2, đọc session id từ status (không đưa token/config ra terminal). Bắt đầu
collector ở A bằng đúng `SESSION_ID` và phase id lấy từ session state:

```sh
python3 scripts/paper_p8_server.py begin --root "$ROOT/A" --experiment E3 \
  --session-id "$SESSION_ID" --phase READY --phase-id "$PHASE_ID"
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chỉ sau khi A `status` xác nhận `MEASURING` và `collector_ready=true`, chạy B:

```sh
python3 scripts/paper_p8_client.py run --workspace "$ROOT/B/workspace" --binary "$ROOT/B/paperload" --gateway-binary "$ROOT/B/gateway"
python3 scripts/paper_p8_client.py export-results --workspace "$ROOT/B/workspace" --output "$ROOT/B/results"
```

Chuyển results nguyên bản sang A, không sửa projection/inventory. Trên A2, gọi:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$ROOT/A" --experiment E3 \
  --session-id "$SESSION_ID" --phase MEASURING --phase-id "$PHASE_ID" \
  --results "$ROOT/A/results"
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
python3 scripts/paper_p8_server.py finalize --root "$ROOT/A" --experiment E3 \
  --session-id "$SESSION_ID" --phase VERIFYING
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Đọc `validation`, workload outcome, cleanup outcome và server exit code. Đợi
serve exit 0/cleanup VERIFIED trước khi kết luận. Request/result sai bị từ chối; giữ nguyên SQLite,
outbox, journal và artifacts để xử lý, không reseed/ghi đè. Finalize/abort vẫn là
đường đóng phiên khi import lỗi.

## Kiểm tra và giới hạn

Result inventory giới hạn file/kích thước, kiểm tra session/run/phase/roles,
bundle hash, per-file SHA-256, đường dẫn và symlink. Chỉ lấy projection an toàn,
không chạy file/SQL từ B. Collector là A-side bounded polling; command-server
origin latency nếu collector không có histogram vẫn unavailable/inconclusive (không
quy thành 0). Collector snapshots cumulative; báo cáo dùng đúng snapshot terminal
mới nhất, không cộng các snapshots lặp lại. Client PATCH RTT và B-process resource
samples không được diễn giải/thêm vào nếu không có measurement tương ứng.
Background sensor delta tách riêng khỏi command-sensor delta. B phải dừng/join
runner trước khi chiếu SQLite receipts; A reconcile command ID, version, interval,
result hash với DB và REST. Bằng chứng receipt/config không tự nó chứng minh
physical A/B; `low_load` không được diễn giải là mixed overlap.

Trước khi dùng, chạy các test E3/client/supervisor; native acceptance chỉ được
ghi nhận từ một lượt hoàn chỉnh có bundle, collector, workload, import,
reconciliation và exact-owned cleanup cùng artifacts tương ứng. Lỗi bất kỳ không
được báo PASS.

### Native helper CLI low-load polling — attempt 19 (2026-10-30)

- A chuẩn bị session `a1002f4c332234a10234325f77f4c5d1` ở `READY`; B bind,
  Go preflight và HTTPS/MQTT TLS connection checks đều PASS. Collector A xác nhận
  `MEASURING/collector_ready=true` trước khi B chạy.
- Workload B exit 0: telemetry A/B lần lượt 8/8 và 4/4 generated/offered/application
  ACK, không có retry/failure/drop, pending rows/bytes đều 0. Bốn control PATCH trả
  202, terminal `succeeded`; frequency là 1.5, 0.5, 2.0, 0.75 Hz. Hai SQLite
  executor độc lập có mỗi 2 completed receipts và pending outbox 0; captured
  configurations là 500000µs và 1333333µs. Đây chưa phải SQL/REST reconciliation.
- A từ chối kết thúc phase với `e3_rest_config_read_failed` khi lấy reported
  configuration qua REST. Import/reconciliation/validation chưa đạt; workload
  outcome giữ `ABORTED`, không gán PASS. Abort đóng server exit 0 và exact-owned
  cleanup VERIFIED (12 containers / 1 network / 5 volumes).
- Artifacts còn nguyên tại
  `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt19/`. Terminal summary:
  `PAPER_P8_E3_WORKLOAD_VALIDATION_FAILED`; `full_p8_accepted=false`. A-side SQL
  deltas, REST/SQL/SQLite reconciliation, raw DB deltas và server-origin latency
  chưa được xác nhận. Client PATCH RTTs 18–41ms không phải dispatch/E2E latency.
- Provenance offline: attempt17 A/B workload artifacts đúng E2/A-remote,
  E2/B-remote và E3/control scopes; current importer chấp nhận bundle đó. Regression
  test khóa role-to-experiment mapping và vẫn bác stale run-id/sai experiment.
  Attempt17 vẫn là lịch sử thất bại, không được nâng thành PASS.
- Root cause được xác định sau attempt19: prepared `control_targets` là hai dict,
  nhưng các consumer đã unpack mỗi dict thành keys `gateway_id`/`sensor_id`, làm
  REST GET dùng URN giả và SQL baseline/delta sai scope. Các consumer nay dùng
  target projection kiểm tra kiểu; command raw delta phải dương. Projection B-side
  giữ executor `configuration` và result hash thực; reported configuration thiếu
  bị từ chối. Đã đọc trực tiếp hai SQLite attempt19 ở read-only mode: mỗi DB có
  2 completed receipts, pending 0, version 2; raw result bytes và exported result
  hashes tương ứng. Đây là diagnostic artifact cũ, không phải SQL reconciliation.
- Finalization E3 sau khi collector đã bắt đầu không thể coi `NOT_RUN`/abort là
  thành công; chỉ preparation chưa bắt đầu workload hoặc imported `VALIDATED` mới
  cho phép exit 0. Attempt19 exit 0 vẫn giữ nguyên lịch sử; behavior sửa áp dụng
  cho lần chạy mới.

### Native helper CLI low-load polling — attempt 20 (2026-10-30)

- Sau khi sửa target mapping và terminal outcome gate, full helper workflow đạt
  `PAPER_P8_E3_WORKLOAD_VERIFIED`: A READY → B bind/preflight/TLS check → A collector
  ready/MEASURING → B workload/export → A finish/reconciliation → finalize.
  B runner exit 0; A server exit 0; workload `VALIDATED`, validation failures `[]`.
- B ghi **4/4** command PATCH thành công (HTTP 202, terminal succeeded), keys
  distinct: 1.5 Hz và 0.5 Hz từ version 0, tiếp theo 2.0 Hz và 0.75 Hz từ version
  1. SQLite hai executor có mỗi **2 completed receipts**, pending outbox **0**;
  kết quả cuối version 2, interval lần lượt 500000µs và 1333333µs. A reconcile SQL
  receipts + REST reported configuration với SQLite receipts/configuration.
- Telemetry nền: A **8/8**, B **4/4** generated/offered/application ACK; A ghi nhận
  1 retry (không có publish failure/drop, pending 0), B không retry; đây không phải
  zero-retry claim. A-side deltas tách riêng: background
  12 ledger / 1200 raw / 12 temporal; command sensors **36 raw / 24 temporal**.
  Tổng fixture delta là 36 ledger / 1236 raw / 36 temporal / 2 configuration.
- Low-load phase validation available; có command interval giao với A steady
  interval, burst overlap **NOT APPLICABLE**. Snapshot terminal đã lưu histograms
  cumulative: trích offline count 4, dispatch sum 2.678130341s / mean 0.669532585s;
  succeeded E2E sum 2.893375296s / mean 0.723343824s; missing origin/dispatch/pending
  đều 0. Đây là post-hoc extraction từ artifact, không sửa raw report lịch sử.
  PATCH client RTT 19–39ms là số khác, không trộn vào server latency hoặc so đồng hồ A/B.
- Exact-owned cleanup VERIFIED: **12 containers / 1 network / 5 volumes**.
  `full_p8_accepted=false`, `generator_colocated=true`, `physical_ab=NOT RUN`.
  Artifacts: `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt20/`.
  Attempt19 và attempt17 failures vẫn được giữ nguyên.

### Native helper CLI low-load wakeup — lần đầu lỗi, rerun hợp lệ (2026-10-30)

- Lượt đầu `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup/` dừng ở
  preflight, cleanup VERIFIED; chưa chạy workload. Diagnostic private traceback cho
  thấy selector `--p8-arm wakeup` bị bỏ qua khi dựng Fixture (hard-code fallback
  polling), nên `STAGE5_WAKEUP_ENABLED=false` làm sai effective-flags gate. Đây là
  lỗi helper xác định, không phải kết quả profile.
- Sửa selector để lifecycle-hold lấy chính arm đã yêu cầu; regression test polling /
  wakeup. Một lần rerun fresh root đạt `PAPER_P8_E3_WORKLOAD_VERIFIED`, A/B exit 0,
  workload `VALIDATED`, cleanup VERIFIED 12/1/5; effective readback xác nhận
  `STAGE5_ENABLED=true`, `STAGE5_WAKEUP_ENABLED=true`.
- B control **4/4** succeeded HTTP 202, 4 distinct keys; executor mỗi bên 2 completed,
  version 2, pending 0. A/B telemetry **8/8** và **4/4** app ACK; retry/failure/drop
  0. Background DB 12/1200/12; command sensor DB 33 raw / 22 temporal; total
  34 ledger / 1233 raw / 34 temporal / 2 configuration.
- Low-load phase available; command/steady overlap observed, burst **NOT APPLICABLE**.
  Terminal collector histogram count 4: dispatch mean **42.15ms**, succeeded E2E mean
  **137.93ms** (respective sums 0.168590961s / 0.551705864s). Client PATCH RTT
  31–42ms is reported separately. `full_p8_accepted=false`, colocated generator,
  physical A/B NOT RUN. Artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup-rerun/`.

### Native helper CLI mixed polling (2026-10-30)

- `PAPER_P8_E3_WORKLOAD_VERIFIED`, validation `VALIDATED`, runner/server exit 0,
  cleanup VERIFIED 12 containers / 1 network / 5 volumes. Effective flags read back
  `STAGE5_ENABLED=true`, wakeup false. B control **4/4** succeeded HTTP 202 with
  four distinct fresh keys; each executor 2 completed receipts, version 2, pending 0.
- Telemetry A/B **8/8** and **16/16** application ACK, pending 0; retries 2 and 27
  respectively; no publish failure/drop. Background DB **24 ledger / 2400 raw /
  24 temporal**; command sensors **35 raw / 23 temporal**; total **47 / 2435 / 47 /
  2 configuration**.
- Runner-clock A steady interval overlapped B burst **3,321,496ns**; command intervals
  overlapped both steady and burst. Pipe lag remains unknown; no A/B clock subtraction.
  Terminal collector: 4 dispatch observations, sum **2.864832765s**, mean **716.21ms**;
  4 succeeded E2E, sum **3.039973099s**, mean **759.99ms**. Client PATCH RTT
  21–112ms, separate. `full_p8_accepted=false`, colocated, physical A/B NOT RUN.
  Artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-polling/`.

### Native helper CLI mixed wakeup (2026-10-30)

- `PAPER_P8_E3_WORKLOAD_VERIFIED`, validation `VALIDATED`, runner/server exit 0,
  cleanup VERIFIED 12/1/5. Effective flags read back `STAGE5_ENABLED=true`, wakeup
  true. B control **4/4** succeeded HTTP 202 with four distinct fresh keys; each
  executor 2 completed receipts, version 2, pending 0.
- Telemetry A/B **8/8** and **16/16** application ACK, pending 0; retries 0 and 26
  respectively; no publish failure/drop. Background DB **24 / 2400 / 24**;
  command sensors **33 raw / 22 temporal**; total **46 ledger / 2433 raw /
  46 temporal / 2 configuration**.
- Runner-clock A steady interval overlapped B burst **3,742,897ns**; command intervals
  overlapped both. Terminal collector: 4 dispatch observations, sum **0.217684148s**,
  mean **54.42ms**; 4 succeeded E2E, sum **0.374731331s**, mean **93.68ms**.
  Client PATCH RTT 14–121ms, separate. `full_p8_accepted=false`, colocated,
  physical A/B NOT RUN. Artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-wakeup/`.

All three requested missing profiles now have a passing complete helper CLI proof;
low-load polling remains the already accepted attempt20 and was not rerun. Resource
samples are A-side fixture observations only; no B-generator process resource claim.
No E4/E5 or later stage was run.
