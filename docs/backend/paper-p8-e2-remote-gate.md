# P8 E2 — helper A/B pilot (source-qualified)

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E2 (telemetry A/B giữa hai cấu hình batching: `samples_per_message=100` và `1`). E2 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST và Mosquitto TLS).

Thử nghiệm chạy tuần tự hai profile trong từng fixture độc lập; mỗi profile được kiểm định dữ liệu và dọn dẹp tài nguyên độc lập. Đây là bài đo telemetry tổng hợp, không chạy Gateway executor và không chứng minh durable restart.

---

## 1. Tổng quan và phân biệt hai chế độ E2

Thử nghiệm E2 hỗ trợ hai chế độ tải (`workload`) nằm hoàn toàn trong phạm vi thử nghiệm E2 (`--experiment E2`), không tạo thêm thí nghiệm E6 hay helper phân tán riêng biệt:

### 1.1 Bảng so sánh hai chế độ E2

| Đặc tính | Chế độ legacy `steady_burst` (Mặc định) | Chế độ `equal_samples` (Bổ sung 2026-10-31) |
|---|---|---|
| **Tham số CLI** | Bỏ qua `--workload` hoặc `--workload steady_burst` | Chỉ định tường minh `--workload equal_samples` |
| **Phạm vi thử nghiệm** | Nằm trong E2 (`--experiment E2`) | Nằm trong E2 (`--experiment E2`) |
| **Cấu trúc Producer** | 2 producer logic A và B (A steady 8 msgs, B steady 4 msgs + burst 12 msgs) | 2 producer logic A và B đều phát steady liên tục 60s, không burst |
| **Khối lượng công việc** | Bất đối xứng: S=100 (2.400 samples) vs S=1 (24 samples) | Đối xứng cùng công: 6.000 samples ở cả hai profile |
| **Tổng unique messages** | 24 messages (A=8, B=16) cho cả hai profile | S=100: 60 messages (A=30, B=30); S=1: 6.000 messages (A=3.000, B=3.000) |
| **Tốc độ phát** | Không đều giữa A và B (A: 1 msg/s, B burst) | Tổng 100 samples/s đều đặn (mỗi producer 50 samples/s) |
| **Cấu hình shaper fixture** | Bật shaper 2 msg/s, burst 4 (chủ đích kích hoạt rate-limit B) | Tắt shaper riêng trong fixture (`MQTT_SHAPER_ENABLED=false`) |
| **Cấu hình microbatch** | Bật microbatch 50ms (`MQTT_MICROBATCH_ENABLED=true`) | Bật microbatch 50ms (giữ nguyên cùng giới hạn và wait 50ms) |
| **Cấu hình triển khai** | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) | Deployment defaults hoàn toàn không thay đổi |

### 1.2 Phân biệt hai Logical Producers và hai Physical Hosts

Cần phân biệt rõ ràng hai khái niệm độc lập:
- **Hai Logical Producers (A và B)**: Là hai luồng tạo tải logic trong workload mô phỏng. Mỗi producer logic sở hữu 1 Gateway và 1 Sensor độc lập với định danh riêng.
- **Hai Physical Hosts (Host A và Host B)**: Host A là máy chủ chạy hạ tầng Go backend, PostgreSQL, Mosquitto, Nginx (`serve`); Host B là máy trạm chạy tiến trình client phát tải (`paperload`).
- **Giới hạn máy đo**: Thử nghiệm được người vận hành chạy trên cùng máy chủ cục bộ, nhưng metadata của `equal_samples` được ghi nhận chuẩn mực là `generator_colocated='unverified'` (định danh kiểm chuẩn `same-host/unverified`), phân biệt rõ với metadata của các lượt legacy cũ trước đây từng ghi `generator_colocated=true`. Chưa chứng minh môi trường hai máy vật lý độc lập qua mạng vật lý (`physical_ab=NOT RUN`).

### 1.3 Quy cách Context mặc định cho `equal_samples`

Context được tạo một lần qua lệnh `e2-context` và chia sẻ nguyên vẹn cho cả hai profile:
- `total_samples` = 6.000 samples tổng cho toàn bộ workload (3.000 samples mỗi producer).
- `sample_rate` = 100 samples/s tổng (50 samples/s mỗi producer).
- `duration` = 60 giây (`total_samples / sample_rate`).
- `seed` = 84.
- **Profile 1 (`samples_per_message=1`)**: Tổng 6.000 messages (3.000 messages/producer), tốc độ 50 msg/s mỗi producer, mỗi message chứa 1 sample (chu kỳ `sample_interval_us` = 20.000 µs).
- **Profile 100 (`samples_per_message=100`)**: Tổng 60 messages (30 messages/producer), tốc độ 0,5 msg/s mỗi producer (phát mỗi 2 giây), mỗi message chứa 100 samples (chu kỳ `sample_interval_us` = 20.000 µs).

### 1.4 Giới hạn hợp lệ của Context đầu vào

Để đảm bảo tính xác định và ngăn ngừa lỗi làm tròn:
- `total_samples`: Số nguyên dương, tối đa 60.000, bắt buộc chia hết cho 200 (để mỗi producer chia tròn cho cả kích thước 1 và 100, không phát sinh batch lẻ ở cuối).
- `sample_rate`: Số nguyên dương chẵn, từ 2 đến 200 samples/s tổng (chia đều 1..100 cho từng producer).
- `duration = total_samples / sample_rate`: Bắt buộc là số nguyên từ 10 đến 120 giây.
- `2.000.000 / sample_rate`: Bắt buộc là số nguyên để `sample_interval_us` là số nguyên microseconds duration-safe.
- Toàn bộ tính toán số mẫu kỳ vọng sử dụng số nguyên và phân số hữu tỉ (rational arithmetic), không dùng float làm tròn.

### 1.5 Dòng dữ liệu chuẩn hóa và quy tắc mốc thời gian (Timestamp)

- **Canonical same stream**: Cả hai profile chia sẻ cùng một dòng dữ liệu mẫu logic. Với cùng seed 84, producer A phát chuỗi sequence logic `0..2.999`, producer B phát `0..2.999`. Giá trị `syntheticValue(sequence)` tại mỗi sequence là trùng khớp tuyệt đối giữa S=100 và S=1.
- **Mốc thời gian (Timestamp Anchor)**: Timestamp của từng mẫu được tính theo công thức: `timestamp = anchor + sequence * sample_interval_us`.
- Mốc `anchor` được backdate về quá khứ để không sinh ra timestamp tương lai (`future timestamp`). Bộ phân tích timestamp của Go backend không áp đặt giới hạn độ tươi về quá khứ (no past-age freshness limit; chỉ từ chối sai lệch tương lai - future skew). Giới hạn tuổi context TTL (`max_context_age_seconds`: mặc định 86.400s, tối đa 2.592.000s) là chính sách riêng của công cụ/helper được kiểm tra ở tiền kiểm tra (preflight); không tự ý thay đổi anchor giữa hai profile.

### 1.6 Cấu hình hạ tầng Fixture

- Trong fixture của `equal_samples`, shaper được tắt có chủ đích (`MQTT_SHAPER_ENABLED=false`) để đo năng lực producer batching dưới cấu hình backend cố định mà không bị shaper can thiệp cắt tỉa gói tin.
- Microbatch được bật giống nhau ở cả hai profile: `MQTT_MICROBATCH_ENABLED=true`, `MQTT_MICROBATCH_MAX_WAIT=50ms`, `MQTT_MICROBATCH_MAX_MESSAGES=32`, `MQTT_MICROBATCH_MAX_ROWS=500`, `MQTT_MICROBATCH_MAX_PAYLOAD_BYTES=4194304`.
- Toàn bộ cấu hình triển khai hệ thống (`docker-compose.yml`, `config/`, `.env.example`, migrations) hoàn toàn không thay đổi.

---

## 2. Hướng dẫn vận hành A/B (Runbook)

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`/path/to/private-network.json`, `$ROOT`, `SESSION_ID_100`). Tuyệt đối không đưa IP máy thật, đường dẫn home cá nhân, thông tin định danh hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`): quyền `0700`.
- **Chống ghi đè tập tin**: Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả/workspace cũ. Mỗi profile phải được khởi tạo trong một thư mục gốc hoàn toàn mới.
- **Ngân sách thời gian (Deadline & JWT budgets)**: Thời hạn deadline của server `--deadline-seconds 900` (15 phút) phải đủ cho toàn bộ chu kỳ: khởi động container, truyền bundle, chạy workload 60s, chờ drain 20s, export/import, hậu kiểm và dọn dẹp. Thời hạn hiệu lực của JWT token được cấp phát với TTL riêng (ví dụ 3.600s trong testbed) và tiền kiểm tra client kiểm tra điều kiện `time.time() < payload['exp'] < time.time() + 86400 * 366`; thời hạn JWT không tự động đồng bộ hóa với tham số `--deadline-seconds` của server. Người vận hành phải bảo đảm token còn hạn vượt quá toàn bộ thời gian thực thi của workload.
- **Bảo tồn phạm vi của E5**: E2 sử dụng các tham số `--context` và `--reference` trong không gian thử nghiệm E2, tuyệt đối không làm thay đổi hoặc ảnh hưởng tới ngữ nghĩa của E5.
- **Giới hạn CLI trực tiếp vs Quy trình Held Lifecycle**:
  - Chạy direct CLI `native_compose_smoke.py --paper-p8-e2` trực tiếp khi không có cấu hình HTTPS sẽ gặp lỗi `AttributeError` do endpoint ở dạng chuỗi ký tự (`str`) thay vì đối tượng `HTTPSEndpoint`.
  - Quy trình vận hành chính thức và đã được kiểm chứng đầy đủ là quy trình held server lifecycle (`paper_p8_server.py serve` kết hợp các lệnh điều khiển theo pha). Không sao chép các lệnh chưa kiểm chứng từ ES7a.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Steps)**: Việc chuyển bundle, binary và kết quả giữa Host A và Host B là thao tác truyền file thủ công của người vận hành (copy an toàn qua đường dẫn hoặc scp), không tuyên bố là tự động hóa qua SSH.

---

### 2.2 Quy trình vận hành chế độ `equal_samples` (Khuyến nghị)

Chạy tuần tự hai profile: Profile S=100 trước, sau đó Profile S=1 sử dụng `--reference` trỏ tới kết quả của S=100 để xác thực tính bất biến của binary và images.

```sh
ROOT=/var/tmp/opencode/p8-e2-equal
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

#### Bước 0 — Tạo Context dùng chung (Host A)

Tạo tập tin context chứa đặc tả dòng dữ liệu chuẩn hóa:

```sh
python3 scripts/paper_p8_server.py e2-context \
  --output "$ROOT/context.json" \
  --total-samples 6000 --sample-rate 100 --seed 84
chmod 0600 "$ROOT/context.json"
```

#### Bước 1 — Khởi động Profile S=100 trên Host A

Khởi chạy tiến trình `serve` (tiến trình này phải được giữ chạy ngầm hoặc trên một terminal riêng biệt cho đến khi hoàn tất):

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s100/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --experiment E2 \
  --samples-per-message 100 \
  --workload equal_samples \
  --context "$ROOT/context.json"
```

Trên terminal điều khiển khác của Host A, kiểm tra trạng thái cho đến khi đạt `preparation_status=E2_PREPARED` và `phase=READY`:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Ghi nhận `session_id` từ kết quả status (gọi là `SESSION_ID_100`). Xác nhận tập tin thực thi lưu trữ đã sẵn sàng tại:
`$ROOT/s100/A/fixture/artifacts/bin/paperload`.

#### Bước 2 — Chuyển giao gói dữ liệu sang Host B (Manual Transfer Step)

Sao chép bundle và binary đã build sang Host B qua kênh truyền duyệt:
1. Sao chép thư mục `$ROOT/s100/A/client-bundle/` sang Host B tại `$ROOT/s100/B/received-bundle/`.
2. Sao chép tập tin thực thi `$ROOT/s100/A/fixture/private/bin/paperload` sang Host B tại `$ROOT/s100/B/paperload`.
3. Phân quyền trên Host B (lưu ý: thư mục `workspace` không được tạo trước vì lệnh `bind` yêu cầu tự tạo mới với quyền `0700`; nếu thư mục `workspace` đã tồn tại từ trước, `bind` sẽ từ chối với lỗi `workspace_output_leaf_exists`):
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-equal
   mkdir -p "$ROOT/s100/B"
   chmod 0700 "$ROOT/s100/B"
   chmod 0700 "$ROOT/s100/B/paperload"
   find "$ROOT/s100/B/received-bundle" -type d -exec chmod 0700 {} +
   find "$ROOT/s100/B/received-bundle" -type f -exec chmod 0600 {} +
   ```

#### Bước 3 — Thiết lập Workspace và kiểm tra kết nối trên Host B

Trên Host B, thực hiện liên kết bundle, tiền kiểm tra và kiểm tra kết nối mạng/xác thực:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$ROOT/s100/B/received-bundle" \
  --workspace "$ROOT/s100/B/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$ROOT/s100/B/workspace" \
  --binary "$ROOT/s100/B/paperload"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$ROOT/s100/B/workspace"
```

#### Bước 4 — Kích hoạt pha đo lường trên Host A

Trên Host A, gửi lệnh bắt đầu pha đo lường và chờ bộ thu thập chỉ số sẵn sàng:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase READY \
  --phase-id "equal-samples-100"

python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Chờ cho đến khi status trả về `phase=MEASURING`, `phase_id=equal-samples-100`, và `collector_ready=true`.

#### Bước 5 — Chạy tải và xuất kết quả trên Host B

Trên Host B, tiến hành chạy tải phát tin và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/s100/B/workspace" \
  --binary "$ROOT/s100/B/paperload"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/s100/B/workspace" \
  --output "$ROOT/s100/B/exported-results"
```

#### Bước 6 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/s100/B/exported-results/` sang Host A tại đường dẫn `$ROOT/s100/A/received-results/`. Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 7 — Hậu kiểm dữ liệu và đóng Fixture S=100 trên Host A

Trên Host A, nhập kết quả để thực hiện xác thực tính toàn vẹn dữ liệu trong cơ sở dữ liệu:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase MEASURING \
  --phase-id "equal-samples-100" \
  --results "$ROOT/s100/A/received-results"

python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Chờ trạng thái chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` (kèm `imported_result.validation_valid=true` và `imported_result.validation_failures=[]`). Sau đó hoàn tất dọn dẹp:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase VERIFYING
```

Chờ tiến trình `serve` của S=100 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
`$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json`.

---

#### Bước 8 — Khởi động Profile S=1 trên Host A (Tái sử dụng Reference S=100)

Khởi động Profile S=1, truyền tham số `--reference` trỏ tới báo cáo S=100 đã hoàn tất để tái sử dụng binary và container images:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s1/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --experiment E2 \
  --samples-per-message 1 \
  --workload equal_samples \
  --context "$ROOT/context.json" \
  --reference "$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json"
```

Kiểm tra status chờ đạt `preparation_status=E2_PREPARED` và `phase=READY`, lấy `SESSION_ID_1`.

#### Bước 9 — Thực hiện chu kỳ đo lường cho Profile S=1

1. **Manual Transfer Step**: Sao chép bundle `$ROOT/s1/A/client-bundle/` sang `$ROOT/s1/B/received-bundle/` và binary `$ROOT/s1/A/fixture/private/bin/paperload` sang `$ROOT/s1/B/paperload`. Xác nhận mã băm SHA256 của `paperload` trùng khớp 100% với S=100.
2. Trên Host B (đảm bảo thư mục `$ROOT/s1/B/workspace` chưa được tạo trước):
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-equal
   python3 scripts/paper_p8_client.py bind --bundle "$ROOT/s1/B/received-bundle" --workspace "$ROOT/s1/B/workspace"
   python3 scripts/paper_p8_client.py preflight --workspace "$ROOT/s1/B/workspace" --binary "$ROOT/s1/B/paperload"
   python3 scripts/paper_p8_client.py check-connection --workspace "$ROOT/s1/B/workspace"
   ```
3. Trên Host A:
   ```sh
   python3 scripts/paper_p8_server.py begin --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase READY --phase-id "equal-samples-1"
   python3 scripts/paper_p8_server.py status --root "$ROOT/s1/A"
   ```
   Chờ `phase=MEASURING`, `phase_id=equal-samples-1`, và `collector_ready=true`.
4. Trên Host B:
   ```sh
   python3 scripts/paper_p8_client.py run --workspace "$ROOT/s1/B/workspace" --binary "$ROOT/s1/B/paperload"
   python3 scripts/paper_p8_client.py export-results --workspace "$ROOT/s1/B/workspace" --output "$ROOT/s1/B/exported-results"
   ```
5. **Manual Transfer Step**: Sao chép kết quả `$ROOT/s1/B/exported-results/` về `$ROOT/s1/A/received-results/`.
6. Trên Host A:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase MEASURING --phase-id "equal-samples-1" --results "$ROOT/s1/A/received-results"
   python3 scripts/paper_p8_server.py status --root "$ROOT/s1/A"
   ```
   Chờ `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` (kèm `imported_result.validation_valid=true` và `imported_result.validation_failures=[]`). Sau đó finalize:
   ```sh
   python3 scripts/paper_p8_server.py finalize --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase VERIFYING
   ```
   Chờ tiến trình `serve` kết thúc mã 0, `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
   `$ROOT/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json`.

---

#### Bước 10 — So sánh đối chứng hai Profile qua `compare-e2`

Thực hiện so sánh read-only trên hai tập tin báo cáo kết quả hoàn chỉnh:

```sh
python3 scripts/paper_p8_server.py compare-e2 \
  --report-1 "$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json" \
  --report-2 "$ROOT/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json" \
  --output "$ROOT/e2-comparison.json"
```

---

### 2.3 Quy trình vận hành chế độ Legacy `steady_burst` (Tham khảo lịch sử)

Khi chạy chế độ legacy, tham số `--workload` được bỏ qua (hoặc để mặc định). Quy trình tương tự như trên nhưng sử dụng phase ID dạng `samples-$SAMPLES`:
- Bắt đầu phase:
  ```sh
  python3 scripts/paper_p8_server.py begin --root "$ROOT/A" \
    --experiment E2 --session-id "$SESSION_ID" --phase READY --phase-id "samples-$SAMPLES"
  ```
- Kết thúc phase:
  ```sh
  python3 scripts/paper_p8_server.py finish-phase --root "$ROOT/A" \
    --experiment E2 --session-id "$SESSION_ID" --phase MEASURING \
    --phase-id "samples-$SAMPLES" --results "$ROOT/A/received-results"
  ```

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

### 3.1 Bảng kết quả thực chạy chế độ Legacy `steady_burst` — 2026-10-30

| Profile | Ledger / raw / temporal / configuration | A/B generated = offered unique | App ACK A/B | Pending A/B cuối lượt | Phase overlap | Cleanup | Serve exit |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 samples/message | 24 / 2.400 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 3.414.891 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |
| 1 sample/message | 24 / 24 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 57.792 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |

Counters riêng B không đồng nghĩa với unique: profile 100 có B=41 publish attempts, 25 retries, 41 broker PUBACKs nhưng 16 unique messages / 16 application ACKs. A có 8 attempts / PUBACKs và 8 application ACKs. Cả hai lượt validation là `valid=true`, `failures=[]`; DB delta theo Gateway là A=8, B=16 ledger/temporal, raw=800/1.600 (profile 100) và raw=8/16 (profile 1).

Artifact roots ngày 2026-10-30:
- Profile 100: `/var/tmp/opencode/p8-e2-helper-100-attempt8/`
- Profile 1: `/var/tmp/opencode/p8-e2-helper-1-attempt1/`
- *Ghi chú thất bại Attempt 8*: Native `serve` trả về 0 và reports/cleanup hợp lệ; script bao ngoài ghi nhận FAIL sau khi server đóng do lỗi đường dẫn đọc workload trong bước tổng hợp. Evidence PASS riêng được dựng bằng cách đọc lại artifacts đã hoàn thành; không chạy lại hay sửa artifact FAIL lịch sử.

---

### 3.2 Bổ sung kiểm tra hồi quy Legacy `steady_burst` — 2026-10-31 (ES.7c)

Thực hiện kiểm tra hồi quy cho tải mặc định bị bỏ qua `--workload` qua hai lượt native độc lập:

#### 1. Attempt 1 (`es7c-20261031-2140`) — FAIL
- **Thực thi và dữ liệu data-plane**:
  - Arm A (Gateway `gw_p0_a_9baac00e`): 8 generated messages, 8 application ACKs, 0 retries, 0 pending, 800 raw samples.
  - Arm B (Gateway `gw_p0_b_04b1198e`): 16 generated messages (4 measure + 12 burst), 16 application ACKs, 27 shaper drops (rate-limited) kèm 27 retries, 0 pending, 1.600 raw samples.
  - Tổng số đếm cơ sở dữ liệu: 24 ledger, 2.400 raw, 24 temporal, 0 configuration.
  - Thoát cạn Ingress: InFlight = 0, BufferedPayloadBytes = 0.
  - Xác thực mẫu raw: `verified_exact = true`, `raw_identity_value_mismatches: 0` trên cả hai nhánh.
  - Dọn dẹp tài nguyên: VERIFIED (12 containers, 1 network, 5 volumes dọn sạch theo receipts).
- **Nguyên nhân thất bại**: `validate_e2_result` báo lỗi xác thực `database_delta_mismatch` (`workload_outcome: ABORTED`, `overall_status: FAIL`) do từ điển `delta` trong `finish_remote_collection` chứa thêm các alias key `'processed': 24, 'telemetry': 24` vi phạm phép so khớp từ điển nghiêm ngặt (strict dict equality `database_delta != expected_total`).
- **Bảo toàn**: Lượt chạy này được giữ nguyên nhãn FAIL tại `es7c-20261031-2140/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `3e4e99af...`), tuyệt đối không đổi nhãn thành PASS.

#### 2. Attempt 2 (`es7c-20261031-2215`) — PASS
- **Khắc phục**: Chuẩn hóa hàm `calculate_canonical_database_delta` chỉ trả về đúng 4 key canonical (`ledger`, `raw`, `temporal`, `configuration`), bảo toàn trình kiểm tra nghiêm ngặt `validate_e2_result` mà không hạ chuẩn kiểm định.
- **Thực thi và kết quả**:
  - Chạy native tại thư mục cô lập `/var/tmp/opencode/e2-equal-implementation/es7c-20261031-2215/`.
  - Kết quả đạt **`PASS`** toàn diện (`status: PASS`, `workload_outcome: VALIDATED`, `validation.valid: true`, `validation.failures: []`).
  - Arm A: 8 generated messages, 8 application ACKs, 0 retries, 0 pending, 800 raw samples.
  - Arm B: 16 generated messages (4 measure + 12 burst), 16 application ACKs, 26 shaper rate-limited drops kèm 26 retries, 0 pending, 1.600 raw samples.
  - Tổng số đếm cơ sở dữ liệu: 24 ledger, 2.400 raw, 24 temporal, 0 configuration.
  - Dọn dẹp tài nguyên: VERIFIED (12 containers, 1 network, 5 volumes — không có container sót lại).
  - Báo cáo kết quả: `es7c-20261031-2215/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `ecff1f98...`) và `startup-summary.json` (SHA256: `6c3ce928...`, `status: PAPER_P8_E2_WORKLOAD_VERIFIED`, `full_p8_accepted: false`).

---

### 3.3 Bảng kết quả thực chạy chế độ `equal_samples` — 2026-10-31 (ES.7b Attempt 4)

Chạy thực nghiệm tại thư mục `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/`:

| Profile | Ledger / raw / temporal / config | A/B generated = offered unique | Attempts A/B | Retries A/B | App ACK A/B | Pending A/B | Raw Verification (0..2.999) | Cleanup | Serve exit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **100 samples/msg** | 60 / 6.000 / 60 / 0 | 30 / 30 | 31 / 31 | 1 / 1 | 30 / 30 | 0 / 0 msgs, 0 B | 0 mismatch (`verified_exact=true`) | 0 orphaned — VERIFIED | 0 |
| **1 sample/msg** | 6.000 / 6.000 / 6.000 / 0 | 3.000 / 3.000 | 3.011 / 3.010 | 11 / 10 | 3.000 / 3.000 | 0 / 0 msgs, 0 B | 0 mismatch (`verified_exact=true`) | 0 orphaned — VERIFIED | 0 |

#### Xác thực dòng dữ liệu toàn vẹn (Full-Stream Raw Verification)
- Toàn bộ 6.000 dòng dữ liệu mẫu trong DB của mỗi profile được đối chiếu từng dòng với đặc tả logic: 0 value mismatch, 0 timestamp mismatch.
- Phân khoảng sequence logic khớp chính xác: Arm A kiểm tra dải [0..2.999], Arm B kiểm tra dải [0..2.999].
- `verified_exact: true` trên cả hai nhánh A và B.

#### Tính bất biến của Binary và Container Images
- **Tập tin thực thi `paperload`**: Mã băm SHA-256 trùng khớp tuyệt đối giữa S=100 và S=1:
  `5cbaf08a0dc23cfbc5e57dc7c03a6783cd9ed1409225d1216e887bd2a80a058a`.
- **Container Images**: Image IDs của 5 thành phần hạ tầng (`backend`, `gateway`, `postgres`, `mosquitto`, `nginx`) trùng khớp tuyệt đối giữa hai lượt chạy, xác nhận không có hành vi biên dịch lại (no rebuild) giữa các profile.

---

### 3.4 Lịch sử các lần thất bại (Attempts 1–3) và giải pháp khắc phục

Lịch sử thất bại được bảo toàn nguyên vẹn trong hồ sơ thử nghiệm:
1. **Attempt 1 (`es7b-pilot-20261031-1900`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Client Preflight với lỗi `P8 client error: PaperloadInputError: stop timeout must be between 0 and 60 seconds`.
   - *Nguyên nhân*: Với N=6.000, R=100, thời lượng tính toán là 90 giây (60s phát + 20s drain + 10s grace). `paper_p8_bundle.py` đã nới lỏng giới hạn lên 300s cho E2, nhưng `scripts/paper_p8.py` tại dòng 214 vẫn còn giữ kiểm tra cũ `stop_timeout > 60`.
   - *Khắc phục*: Cập nhật `scripts/paper_p8.py` cho phép `stop_timeout <= 300` riêng cho các manifest được nhận diện là `equal_samples` (`is_e2_equal_samples`), trong khi các workload legacy E2 khác tiếp tục giữ giới hạn 60s.
2. **Attempt 2 (`es7b-pilot-20261031-1950`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Server `finish-phase` khi import với lỗi `e2_source_manifest_inventory_mismatch`.
   - *Nguyên nhân*: Sai lệch đường dẫn ánh xạ inventory và bản sao manifest khi import kết quả từ client.
   - *Khắc phục*: Chuẩn hóa quy trình sao chép manifest nguồn và đối chiếu tập tin inventory.
3. **Attempt 3 (`es7b-pilot-20261031-2015`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Server `finish-phase` trong phần hậu kiểm DB raw verification với lỗi `db_variable_invalid`.
   - *Nguyên nhân*: Hợp đồng gọi truy vấn psql không chấp nhận tham số số học chưa ép kiểu chuỗi an toàn.
   - *Khắc phục*: Bổ sung hàm kiểm tra ranh giới `_validate_decimal_int_param` trong `scripts/tests/paper_p8_e2_support.py` để kiểm tra kiểu và biên số học (chỉ nhận số nguyên dương, loại bỏ booleans/floats), sau đó ép kiểu thành chuỗi ký tự hợp lệ (`str(val)`) tại ranh giới gọi `native_compose_smoke.db_psql`, thỏa mãn hợp đồng chuỗi nghiêm ngặt của `db_psql`.
4. **Attempt 4 (`es7b-pilot-20261031-2035`) — PASS**:
   - Chạy native thành công trọn vẹn cả hai profile S=100 và S=1, số đếm DB chính xác tuyệt đối, cleanup VERIFIED.

---

### 3.5 Phân định giữa Source sinh kết quả Native và Postprocessor thẩm định

- **Native Producer Source**: Được biên dịch và thực thi tại revision git `8917bd9b7f58ef0173db373510b0566642cbb8a3` (trạng thái dirty).
- **Postprocessor thẩm định (Derived Audit v2)**: Thực hiện xử lý Python ở chế độ **read-only** trên các tập tin artifact gốc của Attempt 4, áp dụng các hàm đánh giá và chuẩn hóa đã sửa lỗi mà không chạy lại bất kỳ tải native hay thay đổi môi trường Docker nào.

---

### 3.6 Hiện tượng Contamination trên cả hai Profile

Cả hai profile trong Attempt 4 đều bị ghi nhận nhiễm (contaminated) do phát sinh retry:
- **Profile 100**: Arm A có 1 retry (31 attempts / 30 ACKs); Arm B có 1 retry (31 attempts / 30 ACKs).
- **Profile 1**: Arm A có 11 retries (3.011 attempts / 3.000 ACKs); Arm B có 10 retries (3.010 attempts / 3.000 ACKs).
- **Nguyên nhân**: Chưa được chứng minh bằng chứng cứ xác thực; nguyên nhân phát sinh retry là chưa biết (`unknown means unknown`). Các retry này không bị che giấu hoặc gọt bỏ để làm đẹp số liệu. Vì vậy, thuộc tính `clean_comparison` là `false` và trạng thái so sánh tổng thể là `CONTAMINATED`.

---

### 3.7 Diễn giải chỉ số theo Thẩm định Audit v2

Mọi diễn giải hiệu năng bắt buộc phải dựa trên **Derived Audit v2** (`/var/tmp/opencode/e2-equal-implementation/es7b-metric-audit-v2`), không sử dụng Audit v1 hoặc bảng so sánh thô ban đầu:

#### Các chỉ số CPU và Giao dịch DB là INCONCLUSIVE (Không thể kết luận)
1. **DB Transactions / sample: INCONCLUSIVE**
   - *Lý do*: `db_transactions_counter_not_retained_in_sampler`.
   - *Bản chất*: Dưới cơ chế Go microbatching, nhiều batch tin nhắn được gom và commit trong cùng một transaction SQL. Số lượng dòng ledger (`ledger_rows_per_sample`: 1.0 cho S1, 0.01 cho S100) không đồng nhất với số giao dịch cơ sở dữ liệu (`ledger != transactions`).
2. **Server Active CPU / sample: INCONCLUSIVE**
   - *Lý do*: `final_commit_boundary_not_observed`.
   - *Bản chất*: Chu kỳ lấy mẫu Docker stats kéo dài ~8 giây và kết thúc ở snapshot 8 trước khi ghi nhận ranh giới commit cuối cùng kèm bộ đếm tài nguyên gắn mốc thời gian.
3. **Server Lifecycle CPU: INCONCLUSIVE**
   - *Lý do*: `lifecycle_cpu_counters_unavailable` (terminal resources là null).
4. **Generator Active CPU / sample: INCONCLUSIVE**
   - *Lý do*: `invalid_delta_ticks_in_active_window:process_appeared`.
   - *Bản chất*: Cửa sổ active [0.023s..60.023s] trên trục thời gian monotonic của Host B chứa Sample 1 (lúc 1.002s) có `delta_ticks = None` do tiến trình con mới xuất hiện khi khởi động; không đủ điều kiện khép kín chuỗi delta ticks hợp lệ.
5. **Generator Lifecycle CPU: INCONCLUSIVE**
   - *Lý do*: `process_change_samples_lack_delta:process_appeared`.
   - *Chỉ số quan sát một phần*: Tổng delta có sẵn được lưu ở trường riêng biệt: `partial_observed_lifecycle_cpu_seconds` đạt 8,56s cho S=1 (0,001427 s/sample) so với 5,19s cho S=100 (0,000865 s/sample).
6. **Ratios CPU Generator**: `UNAVAILABLE`.

#### Phân biệt Offered Unique Bytes vs Backend Admitted Attempt Bytes
- Tỉ lệ dung lượng dữ liệu `bytes_per_sample` (100 / 1) đạt **`0.030783`** (AVAILABLE):
  - **S=100**: Offered unique bytes là 10,664 bytes/sample (tổng 63.984 bytes); Admitted attempt bytes là 11,082 bytes/sample (tổng 66.492 bytes, do có 62 attempts).
  - **S=1**: Offered unique bytes là 358,758 bytes/sample (tổng 2.152.548 bytes); Admitted attempt bytes là 360,007 bytes/sample (tổng 2.160.043 bytes, do có 6.021 attempts).
  - *Định tính*: Đây là dung lượng payload tầng ứng dụng được backend chấp nhận (`backend_admitted_attempt_bytes`), không phản ánh lưu lượng mạng thực tế (wire traffic) hay kích thước đóng gói TLS.

#### Không tuyên bố tăng tốc độ (Speedup) hay Profile chiến thắng
- Do `clean_comparison` là `false`, trường `headline_speedup` là **`UNAVAILABLE`**. Không có tuyên bố tỉ lệ phần trăm tăng tốc độ hay kết luận profile nào chiến thắng.
- Tuyệt đối không ngụy tạo số đo đỉnh (peak), traffic mạng (wire), hay độ trễ thu thập mẫu cảm biến (sensor acquisition latency).

---

### 3.8 Danh mục Checksum và Đường dẫn Artifacts

Các tập tin báo cáo thô ban đầu được bảo toàn nguyên vẹn với mã băm SHA-256 gốc:
- **Profile 1 Report (S=1)**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json`
  - SHA256: `497b4ca8783ba358fc74d3f8373fd4b522f1578acd51f2e485e6b5d7b8722fcf`
- **Profile 100 Report (S=100)**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json`
  - SHA256: `6504243dbea1df89acd489f3c86c1ef8f9d5aa9bd9cb062a459d4a5d2b985397`
- **Báo cáo so sánh gốc**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/e2-comparison.json`
  - SHA256: `8c7180a3aac4aa3f4a1b033ba13ce2a33b46d17c3d058f039ff0a0dd9fd8924a`

Các tập tin thẩm định phái sinh Audit v2 tại `/var/tmp/opencode/e2-equal-implementation/es7b-metric-audit-v2/`:
- `audit-manifest.json`: SHA256 `10319f43ee19beed0041d73501187adf26e3b0ef388066e807a49474f9ee7d43`
- `p8-e2-equal-samples-1-derived-audit.json`: SHA256 `da2b72212f81a8d8a09c522bbb67bad881b70b0b489ae36b3a52b74c9858a278`
- `p8-e2-equal-samples-100-derived-audit.json`: SHA256 `d33626c789a2f0307ddf066f661b0e54be12e9d966f0acdf5887dd6ff5117e08`
- `e2-comparison-derived-audit.json`: SHA256 `484af5c86bc06c82b173a2a0baeef473de9e905400c01c61620f6739cf4c4d10`

Mục lục bằng chứng kiểm thử được lưu trữ tại:
[Chỉ mục Bằng chứng E2 equal_samples](results/p8/e2-equal-samples-20261031/index.md) và [Bản tóm tắt JSON](results/p8/e2-equal-samples-20261031/summary.json). Không sao chép các tệp nhật ký thô riêng tư vào tài liệu.

---

## 4. Giới hạn và trạng thái gate

- **Trạng thái cổng P8**: `full_p8_accepted=false`.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm chạy trên cùng máy chủ cục bộ với metadata ghi nhận chuẩn tắc là `generator_colocated='unverified'` (định danh kiểm chuẩn `same-host/unverified`), phân biệt với các lượt legacy cũ từng ghi `generator_colocated=true`; chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng.
- **Các kịch bản chưa thực hiện**: Các bài kiểm tra lặp lại (3 cặp xen kẽ 100→1, 1→100, 100→1), bài đo năng lực chịu tải (capacity/soak), full CI, kiểm thử phần cứng HIL (AM5728) và triển khai thực tế (live rollout) đều là **NOT RUN**.
- **Phạm vi dừng**: E3–E5 là các phạm vi thử nghiệm lịch sử riêng biệt đã được triển khai và hoàn tất trước đó; nhiệm vụ này không thực hiện thay đổi hay chạy lại bất kỳ thử nghiệm nào của E3–E5 và bảo toàn toàn bộ kết quả đã nghiệm thu của chúng. Dừng lại tại mốc bàn giao E2 `equal_samples`; không tự động mở rộng hay chuyển sang các giai đoạn tiếp theo nếu chưa có yêu cầu mới.
