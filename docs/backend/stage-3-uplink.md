# Tài Liệu Nghiệm Thu Và Bàn Giao Pipeline Uplink Giai Đoạn 3 (Stage 3 Uplink Handover)

**Ngày lập:** 16/10/2026.
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290).
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 0, 1, 2, 4, 5, 6, 7, 9, 10, 11, 12, 15, 16, 17, 18)
- `../docs/backend_plan/stage3_backend_detailed_plan.md` (Kế hoạch tổng thể Stage 3)
- `../docs/backend_plan/task_3.7_detail_plan.md` (Kế hoạch chi tiết Task 3.7)
- Báo cáo các giai đoạn trước:
  - [Task 3.0 — Baseline và Hợp đồng giao thức](stage-3-task-3.0-baseline-and-contract.md)
  - [Task 3.1 — DTO và Kiểm tra tính hợp lệ](stage-3-task-3.1-telemetry-validation.md)
  - [Task 3.2 — Migration 20, Ledger và Raw Persistence](stage-3-task-3.2-persistence.md)
  - [Task 3.3 — Cập nhật Reported State và Temporal Values](stage-3-task-3.3-reported-state-and-temporal.md)
  - [Task 3.4 — MQTT Ingress, Hàng đợi, Worker và Application ACK](stage-3-task-3.4-mqtt-ingress.md)
  - [Task 3.5 — Gateway Simulator và Durable SQLite Outbox](stage-3-task-3.5-gateway-simulator.md)
  - [Task 3.6 — Nghiệm thu Tích hợp Cross-Service và Khả năng Phục hồi](stage-3-task-3.6-uplink-integration.md)
- Tệp số đo kiểm chuẩn allowlisted: [`stage-3-workload-summary.json`](stage-3-workload-summary.json)
- Danh mục tồn đọng kỹ thuật duy nhất: [`docs/dynsec/limitations-and-backlog.md`](../dynsec/limitations-and-backlog.md)

---

## 1. Tổng quan Kiến trúc Pipeline Uplink Giai đoạn 3

Giai đoạn 3 hiện thực trọn vẹn luồng dữ liệu một chiều (uplink) từ thiết bị Gateway lên hệ thống máy chủ Go Backend, lưu trữ an toàn vào PostgreSQL/TimescaleDB và phản hồi xác nhận cấp ứng dụng:

```text
[Gateway Simulator]
   │  1. Sinh mẫu cảm biến tổng hợp (100 Hz, 100 mẫu/batch)
   │  2. Ghi bền vững vào SQLite Outbox (Write-before-publish)
   ▼
[MQTT over TLS] (Mosquitto 2.0.18 với Dynamic Security)
   │  3. Xác thực trạm qua TLS client/credential độc lập
   │  4. Kiểm soát phân quyền topic nghiêm ngặt (ACL %u)
   ▼
[Go Backend - Ingress Subscriber]
   │  5. Tiếp nhận gói tin, sao chép an toàn topic/payload
   │  6. Hàng đợi có giới hạn (Bounded Queue 256)
   ▼
[Fixed Worker Pool] (4 workers cố định)
   │  7. Xác thực định dạng, schema, timestamp, sequence
   │  8. Mở PostgreSQL Transaction (ACID đơn nhất):
   │     ├─ Ghi nhận Dedup Ledger (processed_messages)
   │     ├─ Lưu trữ Raw Samples (telemetry hypertable)
   │     ├─ Khóa hàng cảm biến (SELECT ... FOR UPDATE)
   │     ├─ Cập nhật Reported State & Version (twin_states)
   │     └─ Ghi bản ghi Temporal sample_interval_us (twin_temporal_values)
   │  9. COMMIT Transaction thành công
   ▼
[Post-Commit Application ACK]
   │ 10. Phát gói tin xác nhận lên topic gateways/<id>/acks/...
   ▼
[Gateway Simulator Outbox Cleanup]
   │ 11. Nhận Application ACK hợp lệ
   └─12. Xóa bản ghi đã xác nhận khỏi SQLite Outbox
```

---

## 2. Hợp đồng Giao thức, Định danh và Deduplication

### 2.1 Định danh và Topic Namespace
- **Định danh Trạm:** Khớp chính xác giữa MQTT username và `gateway_id` trong topic (`gateways/<gateway_id>/telemetry/#`). Hệ thống từ chối nếu có sự mâu thuẫn giữa thông tin xác thực MQTT và trường dữ liệu bên trong payload.
- **Định danh Cảm biến:** Ánh xạ qua URN chuẩn NGSI-LD: `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`. Cảm biến bắt buộc phải thuộc quyền sở hữu của trạm gửi tin.
- **Topic phát tin:** `gateways/<gateway_id>/telemetry/<sensor_id>` (hoặc subtopic tương ứng).
- **Topic nhận ACK:** `gateways/<gateway_id>/acks/<sensor_id>`.

### 2.2 Deduplication và Nhận diện Bản tin
- Mỗi bản tin mang một `message_id` duy nhất (chuẩn UUIDv4 hoặc UUIDv7).
- Bảng `processed_messages` lưu trữ khóa chính `(gateway_id, message_id)` cùng với `payload_hash` (chuỗi băm SHA-256 tính trên toàn bộ payload bytes nhận được).
- **Cùng ID, cùng Hash (Byte-Exact Retry):** Được nhận diện là bản tin gửi lại hợp lệ. Giao dịch giữ nguyên trạng thái dữ liệu (bỏ qua insert dữ liệu trùng lặp) và phát lại Application ACK với kết quả `duplicate_committed`.
- **Cùng ID, khác Hash/Topic:** Được phân loại là xung đột giao thức (`conflict`). Backend từ chối ghi dữ liệu, ghi log cảnh báo và không trả ACK thành công.
- Trạm duy trì `boot_id` (UUID) sinh mới mỗi lần khởi động để phân biệt các chu kỳ tuần tự `first_sequence` và `sample_count`, phát hiện chính xác mẫu chồng lấn (overlap) hoặc khoảng trống (gap).

---

## 3. Giao dịch Cơ sở Dữ liệu Đơn nhất và Thứ tự Ghi nhận

Toàn bộ quá trình lưu trữ được đóng gói trong **đúng một PostgreSQL Transaction** do worker thực thi:
1. **Kiểm tra Dedup Ledger:** Thực hiện `INSERT INTO processed_messages ... ON CONFLICT DO NOTHING`.
2. **Lưu trữ Mẫu thô (Raw Telemetry):** Chèn các mẫu đo vào hypertable `telemetry` (TimescaleDB) với timestamp `measured_at` phân bổ theo `sample_interval_us`.
3. **Khóa hàng Cảm biến và Cập nhật Trạng thái:**
   - Thực hiện khóa bi quan: `SELECT id, gateway_id FROM sensors WHERE id = $1 FOR UPDATE`.
   - Đối chiếu thứ tự thời gian: chỉ cho phép cập nhật `reported_state` nếu `measured_at` của bản tin mới hơn hoặc bằng thời điểm ghi nhận gần nhất (`last_reported_at`).
   - Cập nhật trường `reported_state` trong bảng `twin_states` và tăng `reported_version` khi trạng thái thực sự thay đổi.
   - **Bảo toàn tuyệt đối:** Giao dịch không làm thay đổi các trường cấu hình mong muốn (`desired_state`, `desired_version`, `last_desired_at`) hay thuộc tính quản trị của Gateway.
4. **Ghi nhận Thuộc tính Temporal:** Chèn chính xác một bản ghi phản ánh chu kỳ lấy mẫu `sample_interval_us` vào hypertable `twin_temporal_values`.
5. **Rollback an toàn:** Nếu bất kỳ bước nào thất bại (lỗi SQL, timeout, vi phạm ràng buộc), toàn bộ transaction được rollback hoàn toàn, không để lại dữ liệu dở dang hay ledger mồ côi.
6. **Xử lý Dữ liệu Lịch sử (Legacy Provenance):** Các bản ghi lịch sử thiếu provenance (từ trước migration 20) được giữ nguyên giá trị `NULL`; các lượt phát lại bản ghi cũ này trả về mã `legacy_provenance_unknown` và không tự ý bịa sinh ID hay xóa ledger.

---

## 4. Ngữ nghĩa Xác nhận: Commit-before-ACK so với MQTT PUBACK

Hệ thống phân định ranh giới rõ ràng giữa xác nhận tầng mạng và xác nhận tầng ứng dụng:
- **MQTT PUBACK (QoS 1):** Chỉ chứng minh gói tin MQTT đã truyền tới broker Mosquitto thành công; **hoàn toàn không chứng minh** dữ liệu đã được lưu vào cơ sở dữ liệu.
- **Application Commit-before-ACK:** Chỉ được Backend phát lên topic `gateways/<gateway_id>/acks/<sensor_id>` **sau khi** giao dịch PostgreSQL đã `COMMIT` thành công trên đĩa.
- **Cơ chế Outbox của Gateway:** Simulator ghi bản tin vào SQLite outbox trước khi phát (`write-before-publish`). Bản tin chỉ được xóa khỏi outbox khi Gateway nhận được Application ACK hợp lệ khớp đúng `message_id` và `boot_id`. Khi xảy ra sự cố mất ACK trên đường truyền, cơ chế retry của Gateway sẽ phát lại bản tin với nguyên trạng bytes, và Backend dedup an toàn mà không nhân đôi dữ liệu.

---

## 5. Cấu hình Runtime và Giới hạn Tài nguyên (Resource Bounds)

Hệ thống áp dụng các ngưỡng chặn cứng nhằm ngăn ngừa cạn kiệt tài nguyên:

| Thành phần | Tham số cấu hình | Giá trị hiệu lực | Ý nghĩa và hành vi bảo vệ |
| --- | --- | --- | --- |
| **Go Backend** | `MQTT_INGRESS_ENABLED` | Mặc định `false` (ghi đè `true` trong demo/test) | Cờ kích hoạt ingress độc lập với các cờ DynSec credential (`MQTT_CREDENTIAL_API_ENABLED`, `INGRESS_GATE_ENABLED`). |
| | `MQTT_QUEUE_CAPACITY` | `256` | Kích thước hàng đợi RAM trong process; từ chối nhận thêm khi đầy. |
| | `MQTT_WORKER_COUNT` | `4` | Số lượng worker goroutines cố định xử lý hàng đợi. |
| | `MQTT_MAX_PAYLOAD_BYTES`| `65536` (64 KiB) | Giới hạn dung lượng tối đa của một bản tin MQTT. |
| | `DATABASE_MAX_CONNS` | `10` | Số lượng kết nối tối đa trong pool PostgreSQL. |
| | `TELEMETRY_DB_TIMEOUT` | `5s` | Thời gian chờ tối đa cho mỗi thao tác truy vấn cơ sở dữ liệu. |
| | `SHUTDOWN_TIMEOUT` | `15s` | Thời hạn giải phóng hàng đợi và đóng kết nối khi tắt server. |
| **Gateway Sim** | `GATEWAY_MAX_INFLIGHT` | `8` | Số lượng bản tin gửi song song tối đa chờ ACK. |
| | `GATEWAY_ACK_TIMEOUT` | `5s` | Thời gian chờ Application ACK trước khi kích hoạt retry. |
| | `GATEWAY_RETRY_MIN_DELAY` | `1s` | Thời gian chờ tối thiểu giữa các lần thử lại (kèm backoff/jitter). |
| | `GATEWAY_RETRY_MAX_DELAY` | `30s` | Thời gian chờ tối đa giữa các lần thử lại. |
| | `GATEWAY_OUTBOX_MAX_ROWS` | `5000` | Số hàng tối đa trong hàng đợi SQLite outbox. |
| | `GATEWAY_OUTBOX_MAX_BYTES`| `67108864` (64 MiB)| Hạn ngạch dung lượng nghiệp vụ tối đa của outbox. |

---

## 6. Điều kiện Tiên quyết về Khởi tạo (Provisioning Prerequisites)

Trước khi Gateway có thể gửi dữ liệu telemetry, hệ thống yêu cầu:
1. **Khởi tạo Trạm (`gateways`):** Được tạo trước bởi Platform Admin qua hệ thống quản trị (`POST /v1/admin/gateways` hoặc công cụ bootstrap).
2. **Khởi tạo Cảm biến (`sensors`):** Cảm biến phải được Gateway Owner hoặc Admin đăng ký thành công qua API `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`.
3. **Cấp phát MQTT Credential:** Trạm phải được cấp client và role tương ứng trong Mosquitto Dynamic Security với quyền truy cập đúng topic namespace của mình.
4. **Không tự động sinh thực thể (No Auto-Provisioning):** Backend kiên quyết từ chối lưu dữ liệu và không gửi ACK cho các cảm biến chưa được đăng ký trong hệ thống.

---

## 7. Các Lệnh Thực Thi Kiểm Thử và Vận Hành Cục Bộ

### 7.1 Chạy Lượt đo Workload 600 Batches (Khoảng 10 phút)
```bash
# Sử dụng script entrypoint chính thức (cờ --execute là bắt buộc để thực sự khởi chạy đo; không có cờ sẽ chạy preflight dry-run)
./scripts/test-stage3-workload.sh --execute

# Hoặc gọi trực tiếp runner Python với cờ --execute
python3 scripts/tests/stage3_workload_runner.py --execute
```

### 7.2 Chạy Ma trận Tích hợp Cross-Service 9 Kịch bản (Task 3.6)
```bash
# Chạy toàn bộ 9 kịch bản tuần tự
./scripts/test-stage3-uplink.sh --scenario all

# Chạy từng kịch bản riêng lẻ
./scripts/test-stage3-uplink.sh --scenario normal
./scripts/test-stage3-uplink.sh --scenario tls-credential
./scripts/test-stage3-uplink.sh --scenario acl-control
./scripts/test-stage3-uplink.sh --scenario ack-fault
./scripts/test-stage3-uplink.sh --scenario db-timeout
./scripts/test-stage3-uplink.sh --scenario queue-full
./scripts/test-stage3-uplink.sh --scenario gateway-reconnect
./scripts/test-stage3-uplink.sh --scenario process-restart
./scripts/test-stage3-uplink.sh --scenario broker-controlled-restart
```

### 7.3 Chạy Kiểm thử Persistence Tầng Cơ sở Dữ liệu
```bash
./scripts/test-stage3-persistence.sh
```

### 7.4 Kiểm tra Toàn diện Chất lượng Mã nguồn Cục bộ
```bash
# Kiểm tra Go (chạy từ thư mục src do go.mod đặt tại src/)
cd src
go vet ./...
golangci-lint run
go test -race ./...

# Biên dịch thử nghiệm cho kiến trúc ARMv7
GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build -o /dev/null ./cmd/gateway
cd ..

# Kiểm tra Python test suite (chạy từ thư mục gốc repository)
python3 -m unittest discover -s scripts/tests
```

---

## 8. Kết quả Đo Workload Demo 600 Batches và Số Đo Thực Tế

Kết quả dưới đây được trích xuất trực tiếp và nguyên vẹn từ tệp kiểm chuẩn allowlisted [`docs/backend/stage-3-workload-summary.json`](stage-3-workload-summary.json), chạy trên máy Linux x86_64 (12 CPUs, 16 GB RAM):

### 8.1 Thông tin Môi trường và Docker Images Kiểm chuẩn
- **Git Commit Baseline:** `8ceccaa7e9fc75335981499ffbe886d818152b47` (chứa các thay đổi uncommitted của Stage 3).
- **Backend Docker Image:** `sha256:e214e7ced7d1fdbb334479f1db2144113deac0c342ad26a79d488be6df1d69a5`
- **Gateway Docker Image:** `sha256:7aff01c3167bd9e10effeacdeca445da875560ae4b0b16cf288ef7b098ecb551`
- **Mosquitto Image:** `eclipse-mosquitto:2.0.18`
- **TimescaleDB Image:** `timescale/timescaledb:2.29.2-pg16`

### 8.2 Thống kê Số lượng và Tính Toàn vẹn Dữ liệu (Counts & Integrity)
- **Số batch đưa vào outbox (`enqueued_unique`):** `600`
- **Lần phát đầu tiên (`first_publishes`):** `600`
- **Lần thử lại (`retry_attempts`):** `0`
- **Xác nhận Application ACK (`app_acknowledged`):** `600`
- **Mẫu thô đã lưu (`raw_samples`):** `60.000`
- **Số dòng dedup ledger (`ledger_rows`):** `600`
- **Số dòng thuộc tính temporal (`temporal_rows`):** `600`
- **Dữ liệu trùng lặp (`duplicate_committed`):** `0`
- **Số hàng pending còn lại trong SQLite:** `0` (0 bytes)
- **Số hàng cách ly (`quarantined_rows`):** `0` (0 bytes)
- **Đỉnh điểm tồn đọng outbox (`backlog_high_water_rows`):** `1` dòng (`1.340` bytes)
- **Đo lường in-flight:** `unavailable` (không thu thập qua sampling chu kỳ)

### 8.3 Thông lượng (Throughput)
- **Thời gian chạy tổng thể của Runner:** `599.861s` (~10 phút).
- **Cửa sổ từ enqueue đầu đến cuối (`first_to_last_enqueue`):** `599.326s` → Đạt `1.001 batches/s` (`100.112 samples/s`).
- **Cửa sổ từ lúc bắt đầu runner đến enqueue cuối (`run_start_to_last_enqueue`):** `unavailable` (không thu thập mốc timing này; throughput tương ứng là `unavailable`).
- **Cửa sổ từ phát đầu đến drain sạch (`publish_to_drain`):** `599.369s` → Đạt `1.001 batches/s` (`100.105 samples/s`).
- **Cửa sổ chạy từ đầu đến cuối (`runner_total_window`):** `599.861s` → Đạt `1.000 batches/s` (`100.023 samples/s`).

### 8.4 Độ trễ Xác nhận Application ACK (Latency)
Thời gian tính từ thời điểm **phát bản tin lần đầu** đến khi nhận được Application ACK và xóa thành công khỏi SQLite outbox:
- **Số lượng mẫu đo:** `600` mẫu duy nhất.
- **Phương pháp tính quantile:** Nearest-rank trên tập mẫu đã sắp xếp.
- **Median (p50):** **`43.63 ms`**
- **95th percentile (p95):** **`47.86 ms`**
- **Giá trị nhỏ nhất (min):** `40.47 ms`
- **Giá trị lớn nhất (max):** `48.60 ms`

*(Lưu ý kỹ thuật: Tuyệt đối không sao chép nhầm giá trị 18.06ms / 33.34ms vốn thuộc về một bài chạy thử nghiệm nhỏ khác; các giá trị 43.63ms và 47.86ms là số đo kiểm chuẩn chính thức của bài workload 600 batches).*

### 8.5 Mức Tiêu Thụ Tài Nguyên (Resource Usage Sampling)
Được thu thập qua **199 mẫu định kỳ** với chu kỳ trung bình ~`3.022s` (không phải 1Hz tuyệt đối). Các container không đặt quota riêng, chia sẻ tài nguyên host:

| Container | CPU Trung bình (%) | CPU Đỉnh (%) | RAM Trung bình (MB) | RAM Đỉnh (MB) |
| --- | :---: | :---: | :---: | :---: |
| **Go Backend** | 0.20% | 0.43% | 13.52 MB | 14.43 MB |
| **Gateway Simulator** | 0.20% | 0.46% | 13.83 MB | 14.72 MB |
| **Mosquitto Broker** | 0.03% | 0.06% | 0.89 MB | 1.13 MB |
| **PostgreSQL / TimescaleDB** | 0.65% | 4.71% | 239.07 MB | 285.60 MB |

- **Queue High-Water của Backend:** Bằng `0` (theo ngữ nghĩa quan sát tại thời điểm nhận bản tin - `admission-observed`, do worker xử lý tức thời).

### 8.6 Biến thiên Dung lượng Lưu trữ (Storage Deltas)
- **Hypertable `telemetry` (Raw Samples):** Tăng từ 40 KB lên **27.77 MB** (tăng ròng `+27.73 MB`), tạo thêm 1 chunk dữ liệu mới. Trong đó bảng chiếm 9.48 MB, chỉ mục (indexes) chiếm 18.26 MB.
- **Hypertable `twin_temporal_values`:** Tăng từ 24 KB lên **311 KB** (tăng ròng `+286.72 KB`), tạo thêm 1 chunk dữ liệu.
- **Bảng `processed_messages` (Ledger):** Tăng từ 24 KB lên **385 KB** (tăng ròng `+360.45 KB`).
- **Toàn bộ Cơ sở Dữ liệu PostgreSQL:** Tăng từ 10.85 MB lên **39.57 MB** (tăng ròng `+28.72 MB`).
- **Kích thước file SQLite Outbox của Gateway sau khi Drain:** Đạt **36 KB** (giữ cấu trúc trang và schema SQLite sau khi xóa hết 600 bản ghi).

---

## 9. Mẫu Truy Vấn SQL An Toàn Để Đối Chiếu Dữ Liệu Thực Tế

Các câu lệnh SQL dưới đây sử dụng trực tiếp các bảng và trường dữ liệu chính xác của hệ thống:

```sql
-- 1. Xác minh đủ 600 batch duy nhất trong Dedup Ledger
SELECT COUNT(*) AS total_ledger_rows,
       COUNT(DISTINCT message_id) AS unique_messages
FROM processed_messages
WHERE gateway_id = 'gateway_001';

-- 2. Xác minh đủ 60.000 mẫu thô và kiểm tra tính liên tục của chuỗi sequence
SELECT COUNT(*) AS total_raw_samples,
       MIN(sequence_num) AS min_seq,
       MAX(sequence_num) AS max_seq,
       COUNT(DISTINCT sequence_num) AS unique_sequences
FROM telemetry
WHERE gateway_id = 'gateway_001' AND sensor_id = 'sensor_001';

-- 3. Kiểm tra tính toàn vẹn của Reported State trong Digital Twin
SELECT s.reported_state,
       s.reported_version,
       s.last_reported_at,
       s.desired_state,
       s.desired_version
FROM twin_states s
JOIN twin_entities e ON e.id = s.entity_id
WHERE e.entity_id = 'urn:ngsi-ld:Sensor:gateway_001:sensor_001';

-- 4. Xác minh bản ghi temporal sample_interval_us
SELECT COUNT(*) AS total_temporal_records,
       MIN(observed_at) AS first_observation,
       MAX(observed_at) AS last_observation
FROM twin_temporal_values t
JOIN twin_entities e ON e.id = t.entity_id
WHERE e.entity_id = 'urn:ngsi-ld:Sensor:gateway_001:sensor_001'
  AND t.property_name = 'sample_interval_us';
```

---

## 10. Bảng Đối Chiếu Tiêu Chí Nghiệm Thu U01–U15

| Mã | Tiêu Chí Nghiệm Thu | Cấp Bằng Chứng & Revision Thực Hiện | Trạng Thái | Ranh Giới & Giới Hạn Ghi Nhận |
| :---: | --- | --- | :---: | --- |
| **U01** | Batch 100 mẫu hợp lệ qua pipeline | Native Integration (`normal`), images `sha256:e8c6...` & `sha256:dc0a...` | **PASS (Cục bộ)** | 100 raw rows, 1 ledger, reported state cập nhật, 1 temporal row, SQLite drain về 0. |
| **U02** | Retry cùng ID/hash/topic | Native Integration (`ack-fault`), images kiểm chuẩn | **PASS (Cục bộ)** | Bỏ qua ghi trùng, trả duplicate committed, version không tăng thêm. |
| **U03** | Cùng ID khác bytes/topic (Conflict) | Native DB integration: `TestPostgresTelemetryIntegration` (cases topic conflict & changed payload conflict trong `postgres_integration_test.go`); Unit: `TestValidatorHashIsOriginalPayloadSHA256` (`message_test.go`), `TestDecodeBatchTopicMismatch` (`decode_test.go`), `TestPersistDuplicateBypassesReportedStateAndTemporal` (`postgres_temporal_test.go`) | **PASS (Cục bộ)** | Trả lỗi xung đột (`ReasonMessageConflict`), không phát ACK, giữ nguyên ledger cũ. |
| **U04** | Cảm biến lạ (Unknown Sensor) hoặc sai format | Native DB integration: `TestPostgresTelemetryIntegration` (cases unknown Sensor & unknown Gateway trong `postgres_integration_test.go`); Unit: `TestDecodeBatchRejectsUnknownAndTrailingDocuments` (`decode_test.go`), `TestValidatorReturnsNoMessageForInvalidInputs` (`message_test.go`); Bổ trợ dịch vụ thật: `acl-control` | **PASS (Cục bộ)** | Fail-closed, không tự động provision, không ghi dở dang, broker DynSec chặn trước khi tới DB. |
| **U05** | Overlap trong cùng boot / Gap đến muộn | Native DB integration: `TestPostgresTelemetryIntegration` (cases inclusive overlap & gapped sequence trong `postgres_integration_test.go`); Unit: `TestValidateRangeSequenceAndIntervals` (`validation_test.go`) | **PASS (Cục bộ)** | Rollback toàn batch khi overlap (`ReasonSequenceOverlap`); chấp nhận gap hợp lệ. |
| **U06** | Batch cũ đến sau batch mới / Đổi boot | Native DB integration: `TestPostgresTelemetryIntegration` (cases different boot & older-time batch ordering trong `postgres_integration_test.go`); Unit: `TestApplySensorReportedStateOrdering` (`postgres_reported_state_test.go`), `TestValidateRangeFutureSkewAndOldBatches` (`validation_test.go`) | **PASS (Cục bộ)** | Bảo toàn monotonic thời gian; không hạ lùi `last_reported_at` hay `reported_version`. |
| **U07** | Lỗi ghi Twin/Temporal hoặc Commit thất bại | Native DB fixture: Task 3.2 SQL trigger rollback fixture (`scripts/sql/verify-migration-000020.sql`); Unit: `TestPersistReportedStateFailureRollsBackWithoutCommitOrTemporal`, `TestPersistTemporalFailureRollsBackWithoutCommit` (`postgres_temporal_test.go`), `TestPersistCopyFailuresRollbackWithoutCommit` (`postgres_repository_test.go`); Bổ trợ dịch vụ thật: `db-timeout` | **PASS (Cục bộ)** | Rollback toàn bộ transaction; không gửi ACK khi chưa commit; giải phóng lock sạch sẽ. |
| **U08** | Mất Application ACK sau khi DB đã commit | Native Integration (`ack-fault`), images kiểm chuẩn | **PASS (Cục bộ)** | Gateway retry bản tin nguyên trạng, DB dedup an toàn, outbox drain sạch. |
| **U09** | Từ chối Cross-Gateway, Spoof ACK, Control Topic | Native Integration (`acl-control`), images kiểm chuẩn | **PASS (Cục bộ)** | Mosquitto DynSec chặn publish/subscribe chéo; không ảnh hưởng DB. |
| **U10** | Từ chối TLS sai CA, sai Hostname/SAN, sai Mật khẩu | Native Integration (`tls-credential`), images kiểm chuẩn | **PASS (Cục bộ)** | Bị từ chối kết nối dứt khoát; không có insecure fallback. |
| **U11** | Hàng đợi đầy (Queue-Full), DB timeout và phục hồi | Native Integration (`queue-full`, `db-timeout`) | **PASS (Cục bộ)** | Quan sát `QueueFull > 0`, hàng đợi phục hồi trọn vẹn, không mất tin đã ACK. |
| **U12** | Ngắt mạng kết nối, Khởi động lại Backend/Gateway | Native Integration (`gateway-reconnect`, `process-restart`) | **PASS (Cục bộ)** | Tự động reconnect/resubscribe; bảo toàn `boot_id` cũ và sinh `boot_id` mới. |
| **U13** | Khởi động lại có kiểm soát Mosquitto Broker | Native Integration (`broker-controlled-restart`) | **PASS (Cục bộ)** | Bảo toàn credential hiện hành, mật khẩu mới rotate, client revoke và ACL. |
| **U14** | Workload demo 600 batches (10 phút, 60.000 mẫu) | Workload runner, images `sha256:e214...` & `sha256:7aff...` | **PASS (Cục bộ)** | Đạt đủ 600 batch, 60.000 raw samples, 0 pending, p50 43.63ms, p95 47.86ms. |
| **U15** | Kiểm thử Hồi quy Toàn diện (Regressions & CI) | Local run: Python suite (790 tests), Go race/vet/lint, ARMv7 build | **PASS (Cục bộ)**<br>*Remote CI: NOT RUN* | Đầy đủ bài kiểm tra cục bộ; **chưa chạy trên GitHub Actions Remote CI**. |

*(Phân loại cấp bằng chứng: U01, U02, U08–U13 được nghiệm thu trên runner kịch bản dịch vụ thật Task 3.6; U14 được nghiệm thu trên bài đo workload 600 batches dịch vụ thật Task 3.7; U03–U07 kế thừa các ca kiểm thử unit thuần và ca kiểm thử CSDL native thật `TestPostgresTelemetryIntegration` qua runner `scripts/test-stage3-persistence.sh`, trong đó U04 và U07 có thêm kiểm chứng bổ trợ từ các kịch bản dịch vụ thật `acl-control` và `db-timeout`; U15 kiểm chứng bộ regression suite đầy đủ trên máy cục bộ và Remote CI trên GitHub Actions vẫn là NOT RUN).*

---

## 11. Hướng Dẫn Vận Hành CLI, Xử Lý Sự Cố và Giám Sát

### 11.1 Thao Tác Với Gateway CLI
- **Xem số lượng bản tin trong outbox:**
  ```bash
  gateway count --outbox-dir /data/outbox
  ```
- **Liệt kê chi tiết các bản tin đang chờ gửi (`pending`):**
  ```bash
  gateway pending --outbox-dir /data/outbox --limit 20
  ```
- **Xem danh sách các bản tin bị cách ly (`quarantine`):**
  ```bash
  gateway quarantine --outbox-dir /data/outbox
  ```
- **Khởi động lại tiến trình phát:**
  Khi khởi động lại trên cùng một tệp cơ sở dữ liệu SQLite outbox, Gateway sẽ giữ nguyên các bản tin pending chưa được ACK từ chu kỳ trước, đồng thời sinh `boot_id` mới cho các bản tin mới, đảm bảo tính liên tục và không làm xáo trộn sequence.

### 11.2 Xử Lý Sự Cố Publish Bị Kẹt (Stuck-Publish Supervised Restart)
- Nếu kết nối mạng giữa Backend và Broker gặp sự cố khiến lời gọi Paho `Publish` bị treo quá thời hạn `SHUTDOWN_TIMEOUT`, thành phần MQTT Transport sẽ đánh dấu trạng thái bị kẹt (`stuck`), giữ endpoint `/readyz` ở mức `false` để ngăn chặn nhận thêm dữ liệu mới.
- **Biện pháp xử lý:** Tiến hành khởi động lại có giám sát (supervised restart) đối với container Backend. Không áp dụng cơ chế tự động reconnect mù trong cùng process nhằm tránh rò rỉ goroutine hoặc chồng chéo phiên kết nối.

---

## 12. Bàn Giao Kỹ Thuật Cho Giai Đoạn 4 (Stage 4 Handoff)

Stage 4 (Hiện thực hóa API Lịch sử Telemetry và Đọc Thực Thể Digital Twin) tiếp nhận hệ thống với các thông số kỹ thuật chuẩn hóa như sau:

1. **Cấu trúc Dữ liệu và Schema Migration 20:**
   - Dữ liệu chuỗi thời gian thô nằm trong hypertable `telemetry`, được đánh chỉ mục trên `(gateway_id, sensor_id, measured_at DESC, sequence_num)`.
   - Toàn bộ timestamp được chuẩn hóa ở định dạng **UTC với độ chính xác microsecond**.
2. **Nguyên tắc Phân quyền Bắt buộc:**
   - Cơ sở dữ liệu sử dụng role `iot_backend_app` có thuộc tính `BYPASSRLS` để phục vụ vận hành nội bộ của Backend.
   - **CẢNH BÁO:** Quyền `BYPASSRLS` ở tầng DB **tuyệt đối không thay thế** việc kiểm tra phân quyền người dùng. Mọi endpoint đọc dữ liệu của Stage 4 bắt buộc phải xác thực JWT qua Supabase Auth và truy vấn bảng quan hệ `user_gateways` tại Go API để đảm bảo người dùng có quyền truy cập vào Gateway tương ứng.
3. **Mô hình Dữ liệu Digital Twin:**
   - Bảng `twin_states` lưu trữ `reported_state` (JSONB) đại diện cho trạng thái thực tế mới nhất nhận từ trạm, cùng số phiên bản `reported_version`.
   - Bảng `twin_temporal_values` lưu trữ lịch sử các thuộc tính biến đổi theo thời gian (hiện đã ghi nhận `sample_interval_us`).
   - Các trường `desired_state` và `desired_version` tiếp tục được bảo toàn để phục vụ Giai đoạn 5 (Downlink Commands).
4. **Hiện trạng Bộ Định Tuyến API (`src/internal/httpserver/router.go`):**
   - Các route `GET /v1/telemetry/history` và `GET /v1/digital-twins` hiện đang trả về mã lỗi **`501 Not Implemented`** sau khi đã qua bước xác thực JWT hợp lệ (nếu thiếu JWT sẽ trả về `401 Unauthorized`).
   - Tất cả các route chi tiết khác (`/v1/digital-twins/{entity_id}`, `/state`, `/history`, commands, media) hiện **chưa được đăng ký** và sẽ trả về mã lỗi **`404 Not Found`**.
   - Giai đoạn 4 sẽ hiện thực hóa trực tiếp các handler này mà không làm phá vỡ cấu trúc routing hiện tại.
