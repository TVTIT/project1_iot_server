# P8 — Sổ tay tinh gọn: Công cụ tải và quy trình thực nghiệm E1–E5

> **Cập nhật BS.9a (05/11/2026):** Go baseline checkpoint-local-20261105T062820Z-1068601
> và affected refresh 20261105T074320Z-1191327 PASS; Python 981 PASS/3 SKIP.
> Native full HTTP + portable official E3 3-command run PASS tại checkpoint
> `checkpoint-native-20261105T063844Z-1073531`; E1 128 PASS. Physical A/B,
> official high-load/three-repeat experiments, E4 TTL 600s và capacity claims
> vẫn NOT RUN. Các lệnh dưới đây là native local pilots, không phải A/B remote.

## Quy trình A/B thủ công hiện hành

A `serve`/`status` chuẩn bị fixture và bundle → copy bundle cùng binaries phù hợp
host sang B → B `bind`/`preflight`/`check-connection` → A `begin` và xác nhận
`MEASURING` + `collector_ready=true` → B `run`/`export-results` → copy kết quả
về A → A `finish-phase` (và `next-phase` nếu còn phase) → `finalize`/verified
`cleanup`. Xem [remote lifecycle](paper-p8-remote-lifecycle.md), [bundle
contract](paper-p8-client-bundle.md) và [helper guides](paper-p8-helper-guides.md).
B không có DB/admin/Docker credentials. Không có SSH orchestrator. Nếu abort,
stop generator ở B; giữ nguyên pending/journal, ghi outcome PARTIAL/FAILED và
cleanup riêng. Không diễn giải unavailable counter/window thành 0.

Tài liệu này ưu tiên lệnh chạy E1–E5 và cách đọc kết quả. Để tra cứu thiết kế và lịch sử phát triển, xem [sổ tay chi tiết](paper-p8-load-tools.md), [báo cáo nghiệm thu P8](paper-p8-experiments-and-acceptance.md) và [kế hoạch P8](../../../docs/backend_plan/paper/P8_detail_plan.md).

> **Lưu ý dữ liệu cũ:** Mọi lượt chạy kiểm thử phải luôn tạo thư mục output mới hoàn toàn và độc lập, không phụ thuộc vào bất kỳ artifact hoặc dữ liệu lịch sử nào trước đó.

---

## 1. Bắt đầu nhanh: Chạy thử E1 trên máy cục bộ

**Điều kiện tiên quyết:** Máy chủ cần cài đặt sẵn Docker daemon, Docker Compose, Python 3, Go 1.27.3 và OpenSSL; user thực thi có quyền chạy Docker (không yêu cầu `sudo`). Thực hiện lệnh từ thư mục gốc của repository (`$REPO_ROOT`). Không áp `umask 077` cho toàn bộ tiến trình để tránh làm sai quyền các file cần đọc `0644`.

> **QUY TẮC BẮT BUỘC VỚI `--prepare`:** Tham số `--prepare <leaf_dir>` yêu cầu thư mục lá (`leaf_dir`) **phải là thư mục mới hoàn toàn và CHƯA TỒN TẠI**. Không được chạy `mkdir` trước thư mục lá này vì hàm khởi tạo gọi `mkdir(mode=0o700)` không có `exist_ok=True` và sẽ báo lỗi `FileExistsError`. Chỉ tạo thư mục cha riêng biệt. Native selector tự biên dịch mã nguồn Go nội bộ, không cần build trước binary.

```sh
# 1. Thiết lập biến môi trường và tạo thư mục cha bảo mật (quyền 0700, đúng quy ước /var/tmp/opencode)
export REPO_ROOT="$(pwd)"
export SCRATCH_PARENT="$(mktemp -d /var/tmp/opencode/p8-run-XXXXXXXX)"

# 2. Khởi tạo file log bảo mật (0600) trước khi chuyển hướng đầu ra
touch "$SCRATCH_PARENT/e1.log" && chmod 0600 "$SCRATCH_PARENT/e1.log"

# 3. Chạy native pilot E1 (thư mục lá e1-fixture chưa tồn tại trước lệnh này)
python3 scripts/tests/native_compose_smoke.py \
  --prepare "$SCRATCH_PARENT/e1-fixture" \
  --paper-p8-e1 > "$SCRATCH_PARENT/e1.log" 2>&1

# 4. Kiểm tra nhanh kết quả kết thúc bằng shell one-liner
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
print("Status:                ", data.get("status"))
cleanup = data.get("exact_cleanup_verified")
if cleanup is None: cleanup = data.get("cleanup_verified")
print("Cleanup verified:      ", cleanup)
print("Full P8 accepted:      ", data.get("full_p8_accepted"))
' "$SCRATCH_PARENT/e1-fixture/artifacts/run.json"
```

---

## 2. Phân định vai trò Máy A vs Máy B và ranh giới tự động hóa

Hệ thống phân tách thành hai vai trò:

- **MÁY A (Fixture Server):** Chạy một backend Go active, PostgreSQL/TimescaleDB, Mosquitto DynSec, Supabase Auth, Nginx và collectors (`metrics.csv`, `resources.csv`). Nắm toàn bộ quyền quản trị (Docker socket, DB URL, service-role key, DynSec admin).
- **MÁY B (Load Generator):** Chạy Python runner (`scripts/paper_p8.py`), Go `paperload` và Gateway simulator. **Chỉ nhận thông tin xác thực tối thiểu (least-privilege)** gồm user JWT và Gateway password; tuyệt đối không nhận DB credential, Docker socket, platform-admin token hay quyền SSH vào A.

### Ranh giới tự động hóa: Local Native Selector vs Common Helper CLI (Remote A/B)

Hệ thống cung cấp hai phương thức thực nghiệm rõ ràng:

1. **Local Native Selector (`native_compose_smoke.py`, `paper_p8_e5.py`):**
   - Chạy cả vai trò A và B **cùng trên một máy vật lý A** (`generator_colocated=true`).
   - Tự động hóa khép kín: dựng Docker fixture, cấp phát tài khoản, sinh tải nội bộ, đối chiếu DB/metrics và tự dọn dẹp (`exact_cleanup_verified: true`).
   - **Lưu ý:** Pilot local PASS chứng minh công cụ và logic đo lường hoạt động đúng, **KHÔNG CÓ NGHĨA là đã chạy hoặc nghiệm thu topology remote A/B trên hai máy vật lý tách biệt**.
2. **Quy trình Common Helper CLI (Hỗ trợ Remote A/B đa máy):**
   - Bộ công cụ chuẩn hóa gồm:
     - **Máy A:** `scripts/paper_p8_server.py` chạy server fixture ở chế độ hold (`serve`), quản lý phiên (`status`, `begin`, `finish-phase`, `next-phase`, `finalize`, `abort`, `cleanup`).
     - **Máy B:** `scripts/paper_p8_client.py` quản lý workspace (`bind`, `preflight`, `check-connection`, `run`, `export-results`).
     *(Lưu ý: Các lệnh `e5-viewers` và `e5-stop-viewers` là lệnh **đặc thù riêng cho bài E5** để quản lý 2 continuous viewers xuyên suốt 3 pha; bài E4 có lệnh riêng `durable-stage` và `export-durable-results`; chúng không dùng chung cho mọi bài E1–E4).*
   - **Quy trình phối hợp thủ công:**
     1. Trên A (Terminal A1): Khởi chạy `paper_p8_server.py serve` ở chế độ hold; xuất bundle tự chứa tại `$A/client-bundle`.
     2. Người vận hành sao chép bundle sang B qua kênh bảo mật (SCP/SFTP; thư mục 0700, file 0600).
     3. Trên B: Chạy `paper_p8_client.py bind`, `preflight` và `check-connection` (chỉ nhận `--workspace`).
     4. (Với E5): Trên B (Terminal B1), chạy foreground `e5-viewers` để giữ kết nối 2 viewer liên tục.
     5. Điều phối tuần tự từng pha: A mở pha (`begin`) -> B phát tải (`run`) -> B xuất kết quả (`export-results`) -> người vận hành copy `results.json` và `resources-generator.csv` sang A -> A thẩm định (`finish-phase`) và mở pha kế tiếp (`next-phase`).
     6. Kết thúc phiên: Dừng tải/viewers -> A đóng phiên (`finalize`) và dọn sạch tài nguyên Docker sở hữu.
   - Chi tiết hướng dẫn vận hành từng bài helper: xem [Chỉ mục Hướng dẫn Helper](paper-p8-helper-guides.md).

---

## 3. Hướng dẫn chi tiết từng bài thực nghiệm E1–E5

Các lệnh bên dưới chạy trên **Máy A** qua native selector. Mỗi lần chạy phải dùng một thư mục lá mới (ví dụ nếu đã chạy Quick Start với `e1-fixture`, chọn recipe khác hoặc đặt tên lá mới như `e1-recipe-fixture`).

### E1: Fan-out WebSocket và Mật độ Subscription
- **Đo gì:** Khả năng phân phối tin viễn trắc tới 64 viewer WebSocket đồng thời trên 2 namespace Gateway, xác minh phân tách quyền và lọc trùng. (Lưu ý: Cap+1 và revoke thuộc selector riêng `--paper-p8-sessions`; slow viewer thuộc `--paper-p8-slow-viewer`).
- **Lệnh chạy:**
  ```sh
  python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e1-recipe-fixture" --paper-p8-e1
  ```
- **Output:** `$SCRATCH_PARENT/e1-recipe-fixture/artifacts/p8-e1-pilot.json`, `run.json`, `metrics.csv`, `resources.csv`.
- **Kiểm tra thành công:**
  - `status: "PASS"`, `cleanup_verified: true`, `full_p8_accepted: false`.
  - Outer `run.json`: `status: "PAPER_P8_E1_PILOT"`, `exact_cleanup_verified: true`.
   - Trong `p8-e1-pilot.json`: `mapping.counts.sockets=64`, `mapping.counts.users=2`, `validation.valid=true`. Cộng các giá trị trong `mapping.expected_deliveries_by_target` để biết tổng delivery kỳ vọng; so với `websocket_metrics_window.counters.deliveries_enqueued` và `telemetry_writes_completed`, đồng thời `drops=0`, `availability="available"`. **20 batch đầu vào không phải 20 delivery:** mỗi target có 5 batch, mỗi batch được giao tới các viewer thực sự subscribe target đó.

### E2: Throughput, Phân rã Trễ và Steady/Burst
- **Đo gì:** Đo thông lượng, phân rã trễ từng chặng qua shaper/microbatch giữa 2 cấu hình mẫu (100 samples/message vs 1 sample/message) ở trạng thái steady và burst tải hữu hạn.
- **Lệnh chạy:**
  ```sh
  python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e2-fixture" --paper-p8-e2
  ```
- **Output:** `$SCRATCH_PARENT/e2-fixture/artifacts/p8-e2-pilot.json`, `run.json`, `metrics-100.csv`, `metrics-1.csv`, `resources-100.csv`, `resources-1.csv`.
- **Kiểm tra thành công:**
  - Outer `run.json`: `status: "PAPER_P8_E2_PILOT"`, `exact_cleanup_verified: true`.
  - Trong `p8-e2-pilot.json`: `profiles[]` chứa 2 profiles (100 và 1). Mỗi profile có 24 unique batches (Gateway A: 8 batches, Gateway B: 16 batches). `workloads.{A,B}.telemetry` khớp `generated_unique`, `offered_unique` và `application_acks` (A: 8, B: 16), `pending_messages: 0`, `pending_bytes: 0`.
  - `database_delta`: Khớp chính xác `ledger: 24`, `temporal: 24`, `configuration: 0`, và `raw: 2400` (với profile 100) hoặc `raw: 24` (với profile 1).
  - Ở backend, quan sát `attempts = admitted + rejected` (không lấy `offered = admitted + rejected` vì có retries).

### E3: Vòng điều khiển Lệnh dưới Nền Tải Viễn trắc
- **Đo gì:** Kiểm tra chu trình sống lệnh điều khiển Sensor chạy đồng thời với nền tải viễn trắc; so sánh Polling vs Wake-up.
- **Lệnh chạy:**
  - *Mixed-load (mặc định nền tải bình thường):*
    ```sh
    python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e3-poll" --paper-p8-e3 polling
    python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e3-wake" --paper-p8-e3 wakeup
    ```
  - *Low-load (nền tải thấp):* Thêm cờ `--paper-p8-e3-low-load`:
    ```sh
    python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e3-low" --paper-p8-e3 polling --paper-p8-e3-low-load
    ```
- **Output:** `$SCRATCH_PARENT/e3-poll/artifacts/p8-e3-polling.json` (hoặc `p8-e3-wakeup.json`, low-load sinh `p8-e3-polling-low-load.json`), `run.json`.
- **Kiểm tra thành công:**
  - Outer `run.json`: `status: "PAPER_P8_E3_PILOT"`, `exact_cleanup_verified: true`.
  - Trong báo cáo E3: `accounting.control` có `planned: 4`, `sent: 4`, `skipped: 0`, `pending: 0`; `control_reconciliation` có `receipt_verification` hợp lệ, 2 `sqlite_executors`, và xác minh cấu hình REST `rest_configuration`; `validation.valid: true`.
  - `server_origin_latency`: Chứa `histogram_windows` và `missing_origin`. Thiếu origin ghi nhận `missing_origin`, tuyệt đối không thay bằng 0 ms hay client HTTP RTT.

### E4: Chống trùng lặp (RAM Dedup) và Hiệu quả Lưu trữ
- **Đo gì:** Đo hiệu quả lọc trùng khi Gateway phát lại (do mất ACK), kiểm tra cache hit ấm (warm hit) và phục hồi khi cache rỗng sau khởi động lại backend (cold replacement).
- **Lệnh chạy:**
  ```sh
  python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e4-on" --paper-p8-e4 on
  python3 scripts/tests/native_compose_smoke.py --prepare "$SCRATCH_PARENT/e4-off" --paper-p8-e4 off
  ```
- **Output:** `$SCRATCH_PARENT/e4-on/artifacts/p8-e4-on.json`, `run.json`, `p8-e4-on-cold-metrics.csv`.
- **Kiểm tra thành công:**
  - Outer `run.json`: `status: "PAPER_P8_E4_PILOT"`, `exact_cleanup_verified: true`.
   - Ở chế độ `on`: Khớp 20 unique batches + 10 replay; `validation.valid=true`, `coldrestart="PASS"`, `conflicts.status="PASS"`, `durable_gateway_reopen.status="PASS"`, `new_ws_event_proof.status="PASS"` (không tạo WS event mới trên tin trùng). Đọc `counter_windows` để tách chi phí unique/replay và `cold_cache` để xem DB fallback khi cache rỗng. Ở chế độ `off`, `coldrestart="NOT APPLICABLE"` là đúng vì không có RAM cache để khởi động lại.

### E5: Phân tích Đóng góp từng Cơ chế (Ablation Matrix)
- **Đo gì:** Đánh giá độc lập 5 cơ chế bằng cách so sánh cấu hình chuẩn (`optimized`) với từng cấu hình tắt một yếu tố: `shaper_off`, `microbatch_off`, `ram_dedup_off`, `polling`, `fanout_scan`.
- **Quy tắc điều phối qua `scripts/paper_p8_e5.py`:**
  - Mặc định script chỉ tạo kế hoạch (`summary['status'] = 'PREPARED'`). Muốn chạy native **bắt buộc phải có cờ `--run`** (khi hoàn thành ghi `status: 'RUNS_COMPLETE'`).
  - Thư mục lặp được đặt tên định dạng `repeat-01` (không phải `repeat-1`).
  - Cấu hình `optimized` phải luôn chạy trước để tạo context (`$ROOT/repeat-01/context.json`) và reference report (`$ROOT/repeat-01/arm-optimized/fixture/artifacts/p8-e5-optimized.json`).
  - Cờ `--mode official --repeat 3` chỉ điều phối lặp lại độc lập cấu hình **pilot profile** (>= 3 repeats), không phải chạy tải lớn quy mô capacity.
- **Lệnh chạy toàn bộ 6 arm (tuần tự tự động):**
  ```sh
  python3 scripts/paper_p8_e5.py --root "$SCRATCH_PARENT/e5-matrix" \
    --mode pilot --repeat 1 --seed 805 \
    --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan --run
  ```
- **Output và cách đọc:** `$SCRATCH_PARENT/e5-matrix/driver-run.json` phải có `status="RUNS_COMPLETE"`. Báo cáo từng arm ở `repeat-01/arm-{arm}/fixture/artifacts/p8-e5-{arm}.json`; kiểm `status="PASS"`, `cleanup_verified=true` cho từng lượt trước khi so sánh. Driver chạy xong không đồng nghĩa mọi cơ chế đủ bằng chứng kích hoạt.
- **Lệnh chạy lẻ từng arm:**
  ```sh
  # 1. Chạy arm optimized:
  python3 scripts/paper_p8_e5.py --root "$SCRATCH_PARENT/e5-opt" \
    --mode pilot --repeat 1 --seed 805 --arm optimized --run

  # 2. Chạy arm bóc tách (dùng lại context và reference chính xác của optimized):
  python3 scripts/paper_p8_e5.py --root "$SCRATCH_PARENT/e5-shaper" \
    --mode pilot --repeat 1 --seed 805 \
    --context "$SCRATCH_PARENT/e5-opt/repeat-01/context.json" \
    --reference "$SCRATCH_PARENT/e5-opt/repeat-01/arm-optimized/fixture/artifacts/p8-e5-optimized.json" \
    --arm shaper_off --run
  ```
- **Xuất bảng so sánh tổng hợp (E5 Summary):**
  Sau khi chạy đủ 6 arm của ma trận, dùng `scripts/paper_p8_comparison.py` để xuất kết quả (chỉ phân tích báo cáo, không chạy tải):
  ```sh
  E5_ROOT="$SCRATCH_PARENT/e5-matrix/repeat-01"
  python3 scripts/paper_p8_comparison.py e5-summary \
    --optimized "$E5_ROOT/arm-optimized/fixture/artifacts/p8-e5-optimized.json" \
    --shaper-off "$E5_ROOT/arm-shaper_off/fixture/artifacts/p8-e5-shaper_off.json" \
    --microbatch-off "$E5_ROOT/arm-microbatch_off/fixture/artifacts/p8-e5-microbatch_off.json" \
    --ram-dedup-off "$E5_ROOT/arm-ram_dedup_off/fixture/artifacts/p8-e5-ram_dedup_off.json" \
    --polling "$E5_ROOT/arm-polling/fixture/artifacts/p8-e5-polling.json" \
    --fanout-scan "$E5_ROOT/arm-fanout_scan/fixture/artifacts/p8-e5-fanout_scan.json" \
    --output-dir "$SCRATCH_PARENT/e5-summary"
  ```
  *Output:* Sinh ra `e5-summary.json`, `e5-runs.csv`, `e5-pairs.csv`.
  Đọc từng run trong `e5-runs.csv`, rồi từng cặp trong `e5-pairs.csv`; xem lý do `INCONCLUSIVE` trong JSON trước khi lấy số liệu làm kết luận. `Gamma_red = 100 × (C_arm_tắt − C_optimized) / C_arm_tắt` chỉ áp dụng cho counter C cùng cửa sổ, không phải mức tăng tốc toàn hệ thống. Mẫu số 0 hoặc thiếu bằng chứng thì không tính phần trăm; báo trung vị/min–max giữa các repeat độc lập, không gộp p99 của nhiều run thành một p99 tổng.

- **Quy trình Common Helper CLI đa pha (Manual A/B Workflow cho E5):**
  Khác với bộ điều phối tự động cục bộ `paper_p8_e5.py`, quy trình A/B helper CLI thủ công phân định rõ vai trò hai máy và yêu cầu 4 terminal điều phối độc lập (A1/A2 trên Máy chủ A và B1/B2 trên Máy khách B).
  - *Host A (Terminal A1):* Chạy foreground `paper_p8_server.py serve` ở chế độ hold (cho `optimized` hoặc các arm ablation kèm `--reference` trỏ tới báo cáo optimized).
  - *Host B (Terminal B1):* Chạy foreground `paper_p8_client.py e5-viewers` để duy trì kết nối continuous viewers liên tục xuyên suốt cả 3 pha (`initial`, `warm_replay`, `control`).
  - *Host B (Terminal B2):* Khởi tạo workspace bằng `bind`, kiểm tra `preflight` (với cả `--binary` và `--gateway-binary`) và `check-connection` (chỉ nhận `--workspace`). Sau đó tuần tự phát tải từng pha bằng `run --phase <pha>` và xuất kết quả bằng `export-results --phase <pha>`. Ở cuối pha `control`, gửi tín hiệu dừng viewers bằng `e5-stop-viewers`.
  - *Host A (Terminal A2):* Điều khiển vòng đời từng pha bằng `begin`, nhận tệp kết quả `results.json` chuyển từ B vào `$A/fixture/private/incoming/<pha>/results.json`, thẩm định bằng `finish-phase`, mở pha tiếp theo bằng `next-phase`, và đóng phiên bằng `finalize`.
  - *Xử lý sự cố:* Hỗ trợ hủy phiên an toàn bằng `abort` và dọn dẹp tài nguyên tồn đọng có biên lai bằng `cleanup`.
  
  > **Hướng dẫn từng bước chi tiết:** Xem toàn bộ quy trình, cú pháp lệnh đầy đủ, quy tắc khởi tạo biến môi trường trên từng terminal và xử lý sự cố tại [Sổ tay Quy trình Thực nghiệm E5 Đầy đủ](paper-p8-e5-preparation.md). Báo cáo tổng kết 6 arm đã hoàn tất và kết quả đối chiếu được lưu trữ tại [Chỉ mục Bằng chứng E5 31/10/2026](results/p8/helper-e5-20261031/index.md).

---

## 4. Đọc kết quả nhanh và tránh sai lầm phổ biến

### Phân biệt các lớp Artifact và Trạng thái
- **Outer Artifact (`artifacts/run.json`):** Do `native_compose_smoke.py` ghi nhận. Báo cáo trạng thái theo selector (`PAPER_P8_E1_PILOT`, `PAPER_P8_E2_PILOT`, `PAPER_P8_E3_PILOT`, `PAPER_P8_E4_PILOT`, `PAPER_P8_E5_ARM_PILOT`), kèm trường dọn dẹp `exact_cleanup_verified: true` và `full_p8_accepted: false`.
- **Inner Lane Report (`p8-e*.json`):** Do fixture chuyên biệt ghi nhận. Trạng thái thành công là `status: "PASS"`, `cleanup_verified: true`, `full_p8_accepted: false`.
- **Child Runner Report (`run.json` do `paper_p8.py` ghi qua `--run-artifact`):** Báo `status: "COMPLETE"` nếu toàn bộ child kết thúc sạch và outbox thoát hết; báo `"PARTIAL"` nếu còn hàng đợi pending hoặc lỗi.
- **Ranh giới nghiệm thu:** `full_p8_accepted: false` là ranh giới bắt buộc trong toàn bộ các pilot cục bộ. Không suy diễn kết quả pilot thành nghiệm thu chính thức của đề tài hay chứng nhận production.

### Tránh hiểu nhầm số liệu đo lường
1. **Phân biệt các loại bộ đếm:**
   - `broker PUBACK`: Broker MQTT đã nhận gói tin từ Gateway.
   - `application commit ACK`: Backend Go đã hoàn tất ghi cơ sở dữ liệu và gửi ACK ứng dụng qua MQTT về lại Gateway.
    - `DB raw rows`: Số hàng mẫu thực tế trong bảng `telemetry`, khác số batch đã commit (`ledger`); một batch có thể chứa 1 hoặc 100 samples.
   - Ba số này ở các pha xử lý khác nhau và không bao giờ được gộp chung.
2. **Giá trị `null` hoặc `INCONCLUSIVE` không phải là 0:**
   - Nếu thiếu mốc thời gian máy chủ (server monotonic origin) do khởi động lại hoặc thiếu trường, độ trễ `d_dispatch`/`d_e2e` phải ghi là `null` hoặc `INCONCLUSIVE`. Tuyệt đối không thay bằng `0 ms`.
3. **Độ trễ RTT máy khách ≠ Độ trễ xử lý máy chủ:**
   - Độ trễ đo từ Máy B (generator) bao gồm độ trễ mạng hai chiều, độ trễ hàng đợi và OS scheduler. Không được lấy số này đại diện cho độ trễ server nội bộ.
4. **Nhận biết nút thắt tại Generator (Máy B):**
   - So sánh tốc độ phát dự kiến (`target rate`) với tốc độ thực tế (`actual publish rate`). Nếu xuất hiện `scheduling lag`, CPU/RAM/FD trên Máy B chạm trần hoặc số worker không đáp ứng nổi, bài đo bị nghẽn do máy phát tải (generator-limited), không thể kết luận Máy A bão hòa.

---

## 5. So sánh Pilot hiện có vs Kế hoạch Official (P8 Plan)

| Bài | Pilot có sẵn trong CLI / Code | Kế hoạch Official đề ra (Chưa chạy - NOT RUN) |
|---|---|---|
| **E1** | 128 viewers native selected PASS; historical 64 run vẫn riêng | 32 → 64 → 128 → 512 → 1.000 → 2.000 → 5.000 → 10.000; 5k/10k offline contract, không capacity |
| **E2** | 24 unique tổng A+B/profile, S=100 và S=1; retry được tách | Offered tổng 100/1.000/5.000/20.000 msg/s; chưa chạy high-rate |
| **E3** | 3-command low-load/steady polling+wakeup subset PASS; pilot 4-command compatibility giữ riêng | Official 30 commands/arm; offline opt-in/preflight đã PASS, native 30 NOT RUN |
| **E4** | 8 unique + 2 replay cache on/off PASS; replay không tạo DB/WS mới | Plan 1.000 unique + 100 replay; optional TTL 600 giây cùng process vẫn native NOT RUN |
| **E5** | 6 arms, 1 representative pilot repeat (seed 805); không phải capacity | >=3 independent repeats là kế hoạch kết luận chính; chưa chạy official repeats |

### Giới hạn tài nguyên thực tế của Fixture Backend
- **Hạn ngạch Backend:** Giới hạn Docker **2 vCPU và 4 GiB RAM** (kế thừa từ tiền lệ P0/P1). Giới hạn này **CHỈ áp dụng cho container Go backend**, không áp dụng cho toàn bộ máy chủ, cơ sở dữ liệu hay broker.
- **Pool kết nối Database:** Profile E5 đặt `DATABASE_MAX_CONNS=11`; các selector khác có thể dùng cấu hình khác, cần đọc lại effective profile trong artifact.
- **Biến môi trường Microbatch:** Sử dụng đúng biến môi trường thực tế `MQTT_MICROBATCH_MAX_WAIT=50ms`, `MQTT_MICROBATCH_MAX_MESSAGES=32`, `MQTT_MICROBATCH_MAX_ROWS=500`.

---

## 6. Liên kết tài liệu và điểm dừng

- **Sổ tay chi tiết gốc:** [paper-p8-load-tools.md](paper-p8-load-tools.md) (lưu giữ đầy đủ lịch sử kỹ thuật và văn bản gốc).
- **Báo cáo nghiệm thu P8:** [paper-p8-experiments-and-acceptance.md](paper-p8-experiments-and-acceptance.md) (kết quả và bằng chứng số hóa an toàn).
- **Kế hoạch chi tiết P8:** [P8_detail_plan.md](../../../docs/backend_plan/paper/P8_detail_plan.md) (nằm ngoài git repo).
- **Theo dõi hạn chế:** [limitations-and-backlog.md](../dynsec/limitations-and-backlog.md).
