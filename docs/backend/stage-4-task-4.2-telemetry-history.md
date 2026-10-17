# Báo Cáo Kỹ Thuật Task 4.2 — Telemetry Raw Và Adaptive Time-Bucket History

**Ngày lập:** 2026-10-17  
**Nhánh:** `develop`  
**Baseline commit:** `817fbb06342a8ab49bf61e7fb315f441a825f27f` ("feat(digitaltwin): implement stage 4 query models, bounds, and read authorization")  
**Trạng thái worktree:** **Chưa commit (uncommitted / dirty worktree)** trên nhánh `develop`. Không có commit SHA cuối cùng giả mạo.

---

## 1. Mục Tiêu & Ranh Giới Phạm Vi

Task 4.2 hoàn thành trọn vẹn history query engine cho telemetry đọc từ PostgreSQL/TimescaleDB Schema 20, kế thừa nền tảng bounds/preparation và sensor resolver của Task 4.1:
- Đọc raw telemetry theo khoảng nửa đóng nửa mở `[from, to)`, sắp xếp ổn định theo `measured_at ASC, sample_id ASC`, phát hiện tràn mẫu không cắt xén ngầm.
- Tính toán bucket downsampling SQL với `AVG`, `MIN`, `MAX`, `COUNT(*)` dùng origin `1970-01-01 00:00:00+00` UTC, clipping trước aggregation.
- Tự động chuyển đổi chế độ (`auto`) và tự động chọn bucket resolution nhỏ nhất trong allowlist thỏa mãn `max_points`.
- Kiểm soát quyền truy cập tenant-isolation thông qua CTE `user_gateways` trực tiếp trong data query.
- Phân biệt rành mạch "authorized-empty" (sensor hợp lệ nhưng không có dữ liệu) với "not-found" (không có quyền hoặc sensor không tồn tại).
- Wire projection DTO độc lập (`TelemetryHistoryResponseDTO`) bảo toàn kiểu `int64` (chuỗi số), số thực hữu hạn, và provenance NULL.

**Ranh giới kỹ thuật:**
- **Không kích hoạt HTTP routes:** Các endpoint HTTP tại `cmd/server` vẫn giữ nguyên stub `501 Not Implemented` (thuộc Task 4.6).
- **Không thay đổi writer hay schema:** Schema 20 và pipeline ghi telemetry giữ nguyên tuyệt đối; không tạo migration mới.
- **Không triển khai Digital Twin read/delegation:** Twin list/detail/state thuộc Task 4.3; delegation `value` thuộc Task 4.4.
- **Chưa kiểm thử live HTTP hoặc MQTT-to-HTTP end-to-end:** Phạm vi này thuộc Task 4.7.

---

## 2. API & Cấu Hình Kế Thừa

Task 4.2 kết nối và tái sử dụng trực tiếp các thành phần đã hoàn thành ở Task 4.1:
1. `internal/config/read_bounds.go`:
   - `HISTORY_QUERY_TIMEOUT`: 5 giây
   - `HISTORY_MAX_RANGE`: 744 giờ (31 ngày)
   - `HISTORY_RAW_WINDOW`: 10 giây
   - `HISTORY_DEFAULT_POINTS`: 1.000 điểm
   - `HISTORY_MIN_POINTS`: 100 điểm
   - `HISTORY_MAX_POINTS`: 2.000 điểm
2. `internal/telemetry/history_query.go`:
   - Interface `HistoryReader` và `HistoryService`.
   - Models: `TelemetryHistoryQuery`, `TelemetryHistoryResult`, `RawSamplePoint`, `BucketSamplePoint`.
   - Allowlist 9 độ phân giải bucket: `100ms`, `1s`, `10s`, `1m`, `5m`, `15m`, `1h`, `6h`, `1d`.
   - Các hàm tính toán `ResolutionDuration`, `IntersectingBucketCount`, `ValidateHistoryTimeRange`.
   - Phân loại lỗi miền: `ErrHistoryNotFound`, `ErrHistoryInvalidInput`, `ErrHistoryInvalidResolution`, `ErrHistoryRawLimitExceeded`, `ErrHistoryResolutionTooFine`, `ErrHistoryDependency`, `ErrHistoryUnexpected`.
3. `internal/telemetry/history_preparation.go`:
   - `HistoryPreparation`: dàn xếp kiểm tra giới hạn tham số, sensor resolver và timeout context duy nhất bao phủ toàn bộ vòng đời truy vấn.
4. `internal/digitaltwin/postgres_resolver.go`:
   - `ResolveSensor`: kiểm tra quyền thành viên trong `user_gateways` và lấy đơn vị đo `unit` của sensor.
5. `Schema 20` (`000020_stage3_telemetry_ingestion`):
   - Hypertable `public.telemetry` với PK `(measured_at, sample_id)` và composite B-tree index `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)`.

---

## 3. Ngữ Nghĩa Kỹ Thuật: Raw, Bucket, Auto & Projection

### 3.1. Raw Query Semantics
- **Khoảng thời gian:** `measured_at >= from AND measured_at < to` (nửa đóng, nửa mở).
- **Thứ tự:** `ORDER BY t.measured_at ASC, t.sample_id ASC`. `sample_id` (BIGSERIAL nội bộ) bảo đảm thứ tự ổn định giữa hai mẫu có cùng timestamp hoặc thuộc các boot khác nhau.
- **Phát hiện tràn mẫu:** Truy vấn lấy `LIMIT max_points + 1`. Nếu số dòng trả về vượt quá `max_points`, service trả về `ErrHistoryRawLimitExceeded`, tuyệt đối không cắt xén ngầm về `max_points` rồi báo thành công giả.
- **Provenance nullable:** Giữ nguyên giá trị `boot_id`, `message_id`, `sample_index` (hoặc `nil`/`NULL` đối với legacy rows trước migration 20).

### 3.2. Bucket Query Semantics
- **Hàm bucket TimescaleDB:** `time_bucket($4::interval, t.measured_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')`.
- **Interval allowlist:** Tham số interval được gắn kiểu an toàn thông qua `pgtype.Interval` với microseconds từ `ResolutionDuration`, không nội suy chuỗi từ người dùng.
- **Clipping trước aggregation:** Mệnh đề `WHERE t.measured_at >= $5 AND t.measured_at < $6` được áp dụng bên trong CTE `clipped_telemetry` trước khi tính `AVG(value)`, `MIN(value)`, `MAX(value)`, `COUNT(*)`. Các mẫu ngoài khoảng bị loại bỏ; partial bucket đầu tiên nếu bắt đầu trước `from` cũng chỉ tính các mẫu nằm trong khoảng `[from, to)`.
- **Không tạo dữ liệu giả:** Chỉ trả các bucket có mẫu dữ liệu thực tế (`sample_count > 0`). Không gapfill, không nội suy, không fill 0 cho các bucket rỗng.

### 3.3. Auto Mode & Đơn Ngân Sách Thời Gian (Single Deadline)
- **Auto window ngắn (`diffMicros <= RawWindow` tức ≤ 10s):**
  1. Thử raw probe với `LIMIT max_points + 1`.
  2. Nếu dữ liệu vừa vặn (≤ `max_points`), trả về kết quả raw.
  3. Nếu raw probe bị tràn (`ErrHistoryRawLimitExceeded`), hủy toàn bộ raw probe rows và fallback sang bucket aggregation.
- **Auto window dài (`diffMicros > RawWindow`):**
  - Bỏ qua raw probe, chọn trực tiếp resolution bucket nhỏ nhất trong allowlist thỏa mãn `IntersectingBucketCount(from, to, width) <= max_points`.
- **Ngân sách thời gian duy nhất:** Toàn bộ quá trình resolve sensor, raw probe và bucket fallback đều chia sẻ chung một deadline context từ `HistoryPreparationConfig.QueryTimeout` (5 giây). Context bị hủy giữa probe và fallback sẽ chặn ngay truy vấn kế tiếp mà không cấp phát thêm thời gian.

### 3.4. Kiểm Soát Quyền Membership & Authorized-Empty
- Cả raw và bucket query đều sử dụng CTE `authorized_sensor` kết hợp `LEFT JOIN`:
  ```sql
  WITH authorized_sensor AS (
      SELECT s.gateway_id, s.sensor_id, s.unit
      FROM public.sensors s
      JOIN public.user_gateways ug
        ON ug.gateway_id = s.gateway_id AND ug.user_id = $1
      WHERE s.gateway_id = $2 AND s.sensor_id = $3
  )
  ```
- **Resource không có quyền hoặc không tồn tại:** CTE trả về 0 dòng -> `rowCount == 0` -> phân loại chuẩn xác thành `ErrHistoryNotFound` (không làm lộ sự tồn tại của resource).
- **Authorized-empty:** Người dùng có quyền nhưng khoảng thời gian không có mẫu -> `LEFT JOIN` trả về 1 dòng sentinel với `gateway_id, sensor_id, unit` hợp lệ và `measured_at = NULL` (hoặc `bucket_start = NULL`) -> adapter bỏ qua sentinel row và trả về kết quả thành công hợp lệ với `point_count = 0, points = []`.

### 3.5. Wire Projection (`TelemetryHistoryResponseDTO`)
- `sequence_num` (raw) và `count` (bucket) được chuyển đổi thành chuỗi thập phân (`strconv.FormatInt`), bảo đảm không bị mất độ chính xác khi vượt quá `2^53` trong JSON parser phía client.
- Timestamps được định dạng chuẩn UTC RFC3339 với 6 chữ số microsecond (`YYYY-MM-DDTHH:MM:SS.ffffffZ`).
- `unit`, `boot_id`, `message_id`, `sample_index` giữ nguyên `null` trong JSON khi mang giá trị NULL từ database.

---

## 4. Môi Trường Kiểm Thử Native & Vai Trò (Roles)

Các ca kiểm thử native được thực thi trên hai môi trường cụ thể:

1. **Target 1: Shared Test Database**
   - **PostgreSQL:** 16.15 (Debian 16.15-1.pgdg120+1)
   - **TimescaleDB:** 2.29.2
   - **Schema Migrations Terminal:** 20 (`stage3_telemetry_ingestion`)
   - **Application Role:** `iot_backend_app` (`rolsuper=false`, `rolcanlogin=true`, `rolbypassrls=true`). Mọi câu lệnh SELECT dữ liệu và EXPLAIN đều được thực thi dưới role này.
   - **Setup Role:** `postgres` (superuser), chỉ dùng để seed dữ liệu fixture và dọn dẹp (teardown).
   - **Kết quả:** 6 ca integration nghiệp vụ PASS + 1 ca query measurement PASS. Ca `TestNativeTelemetryHistoryUpgradedLegacyProvenance` được SKIP vì database dùng chung không chứa bảng snapshot của upgrade rehearsal.

2. **Target 2: Isolated Upgrade Fixture**
   - **Container:** Docker image `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
   - **Kịch bản:** Rehearsal nâng cấp từ schema 19 lên schema 20, có 3 bản ghi telemetry legacy (trước migration 20) trong compressed chunk.
   - **Kết quả:** `TestNativeTelemetryHistoryUpgradedLegacyProvenance` thực thi trong container và **PASS**, xác minh legacy NULL provenance được giữ nguyên và tổng hợp bucket chính xác.

3. **Lưu ý về bằng chứng Cancellation & Timeout:**
   - Ca kiểm thử deadline/cancellation native (`TestNativeTelemetryHistoryPersistRetryAndReadIntegrity`) kiểm tra cơ chế context deadline khi hàng đợi acquire connection pool (`pgxpool.Pool`) bị cạn kiệt, xác nhận trả về `ErrHistoryDependency` bọc `context.DeadlineExceeded` kèm `nil` result mà không làm rò rỉ kết nối. Ca này **không** khẳng định khả năng ngắt một truy vấn đang thực thi dở dang bên trong engine PostgreSQL.

---

## 5. Tổng Hợp Kết Quả Kiểm Thử (Verification Summary)

### 5.1. Bộ Kiểm Thử Source Code & Linter (tại thư mục `src`)
- `gofmt -l .`: Clean (không có file sai format).
- `go vet ./...`: Clean (0 cảnh báo).
- `golangci-lint run`: Clean (0 issues).
- `go build ./...`: Build thành công (CGO=0).
- `git diff --check`: Clean (không có whitespace lỗi).
- `go test -race -count=1 ./internal/telemetry ./internal/digitaltwin ./internal/httpserver ./internal/config ./internal/gateway`: **PASS** (100% test unit và logic race detector vượt qua).

### 5.2. Bộ Kiểm Thử Python Fixture Runner
- `python3 -m unittest discover -s scripts/tests`: **792/792 tests PASS** (bao gồm toàn bộ contract, runner và harness tests).
- `bash ./scripts/test-stage3-persistence.sh --fixture normal`: **PASS**.
- `bash ./scripts/test-stage3-persistence.sh --fixture upgrade`: **PASS** (bao gồm `TestNativeTelemetryHistoryUpgradedLegacyProvenance`).
- `bash ./scripts/test-stage3-persistence.sh --fixture rollback`: **PASS**.

### 5.3. Bộ Kiểm Thử Native PostgreSQL / TimescaleDB (chạy opt-in)
| Test Function | Target | Kết quả | Ghi chú |
|---|---|---|---|
| `TestNativeTelemetryHistoryRead` | Shared DB | **PASS** | Đọc raw 4 điểm và bucket 2 điểm với interval binding |
| `TestNativeTelemetryHistoryAuthorizationAndEmptyResult` | Shared DB | **PASS** | Phân lập 2 Gateway cùng Sensor ID, role matrix (owner/operator/viewer), fail-closed denials, authorized-empty |
| `TestNativeTelemetryHistoryIntervalBoundariesAndOrdering` | Shared DB | **PASS** | Biên nửa đóng `[from, to)`, thứ tự ổn định `sample_id`, không dedup nhầm khác boot, sequence > 2^53 |
| `TestNativeTelemetryHistoryBucketArithmeticAndClipping` | Shared DB | **PASS** | Clipping biên partial, avg/min/max/count chính xác, bảo toàn gap, pre-epoch boundary 1969/1970 |
| `TestNativeTelemetryHistoryCapAndOverflow` | Shared DB | **PASS** | Chạm trần chính xác `max_points`, overflow kích hoạt auto fallback sang 100ms bucket |
| `TestNativeTelemetryHistoryPersistRetryAndReadIntegrity` | Shared DB | **PASS** | Read-only integrity (không ghi side-effect), dedup không tác động history, pool deadline timeout |
| `TestNativeTelemetryHistoryQueryPlanMeasurement` | Shared DB | **PASS** | Đo đạc EXPLAIN ANALYZE trên dataset 60.000 raw samples dưới role `iot_backend_app` |
| `TestNativeTelemetryHistoryUpgradedLegacyProvenance` | Shared DB | **SKIP** (NOT RUN) | Bỏ qua do shared DB không có snapshot nâng cấp từ v19 |
| `TestNativeTelemetryHistoryUpgradedLegacyProvenance` | Isolated Upgrade Container | **PASS** | Chạy trong upgrade rehearsal container, xác minh legacy NULL provenance |

---

## 6. Đo Đạc Hiệu Năng & Quyết Định Schema 20

Chi tiết toàn bộ query plan và buffer usage được ghi lại tại:  
👉 [docs/backend/stage-4-task-4.2-query-measurements.md](stage-4-task-4.2-query-measurements.md)

### Tóm tắt số đo trên dataset 60.000 raw samples (10 phút @ 100 Hz, uncompressed):

| Kịch bản truy vấn | EXPLAIN Execution Time | Service Execution Time | Ngân sách | Đánh giá |
|---|---|---|---|---|
| **M1: Short Raw (5s, 500 samples @ 100 Hz, cap=500)** | **0,58 - 0,68 ms** | 2,03 - 2,40 ms | 5.000 ms | Vượt trội |
| **M2: Bucket 10m (60.000 samples, cap=500, width=10s)** | **66,54 - 71,04 ms** | 57,20 - 59,85 ms | 5.000 ms | Vượt trội |
| **M3: Raw Probe (501 rows) + Bucket Fallback (80 pts)** | Tổng 2 EXPLAIN: **1,53 - 2,23 ms** | Service call: **3,24 - 8,58 ms** | 5.000 ms | Vượt trội |

*Ghi chú kỹ thuật về số đo & trạng thái rà soát:*
- Thời gian thực thi EXPLAIN và thời gian thực thi của service adapter là các lần chạy riêng biệt; tổng execution time của 2 lệnh EXPLAIN (Probe 3a + Fallback 3b) không phải là số đo của một lần gọi service end-to-end. Các số đo phản ánh kết quả thử nghiệm riêng lẻ tại thời điểm đo, không cấu thành cam kết bảo đảm hiệu năng cố định (performance guarantees).
- Toàn bộ số liệu buffer usage cũ trước đây đã bị vô hiệu hóa bởi review do hàm thu thập cộng dồn cả bộ đếm bao hàm parent/child trong mã kiểm thử; các số liệu này đang chờ đo đạc đính chính từ worker song song.
- Thời gian thực thi của M2 (~66-71 ms cho SQL và ~57-60 ms cho service adapter) nằm ổn định dưới ngưỡng 75 ms, cách xa ngân sách cam kết 5.000 ms (5 giây).
- **Quyết định Schema 20:** Composite B-tree index hiện hữu `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)` được sử dụng trong query plan cho cả raw range scan lẫn bucket aggregate scan; không có căn cứ thực nghiệm nào từ dataset đo đạc này đòi hỏi thay đổi index hay tạo migration mới. **Giữ nguyên Schema 20, không tạo index suy đoán.**

---

## 7. Giới Hạn Đã Biết (Known Limitations)

1. **Mã nguồn chưa commit:** Các thay đổi của Task 4.2 đang ở trạng thái uncommitted trên nhánh `develop`.
2. **HTTP Routes chưa kích hoạt:** Các endpoint HTTP tại `cmd/server` vẫn trả `501 Not Implemented`. Việc đăng ký router và ánh xạ lỗi sang HTTP status thuộc Task 4.6.
3. **Chưa có Digital Twin Read / Delegation:** Chưa triển khai endpoint Twin hay delegation thuộc tính `value` từ Digital Twin temporal history sang raw telemetry (thuộc Task 4.3 và Task 4.4).
4. **Chưa có kiểm thử tích hợp MQTT -> HTTP:** Chưa có kiểm thử toàn trình từ Gateway simulator qua broker MQTT tới REST query API; nội dung này thuộc Task 4.7.
5. **Dữ liệu nén (Compression):** Đo đạc query plan được thực hiện trên dataset uncompressed; đánh giá hiệu năng trên chunk đã nén được bảo lưu cho Task 4.7.
6. **Cancellation & Timeout:** Kiểm thử context deadline/timeout chỉ xác nhận cơ chế timeout khi hàng đợi acquire connection pool bị cạn kiệt; không khẳng định khả năng hủy hoặc ngắt một truy vấn đang thực thi dở dang bên trong engine PostgreSQL.
7. **Môi trường & Remote CI:** Không khẳng định chạy remote CI cho Task 4.2 hay chứng nhận sẵn sàng production; toàn bộ kiểm thử và đo đạc được thực hiện trên môi trường test cục bộ.
8. **Trạng thái rà soát kỹ thuật (Review Status):** Đợt rà soát kỹ thuật Task 4.2 của `@go-reviewer` (model `openai/gpt-sol-latest#high`) đã xác định 3 vấn đề mức MEDIUM (DTO timestamp năm 0001, bắt lỗi cleanup/verification trong test native, và thu thập buffer counters chỉ từ root node trong test đo đạc). Toàn bộ 3 phát hiện đã được khắc phục hoàn tất kèm các unit/regression tests liên quan; các tài liệu đã được cập nhật đồng bộ.

---

## 8. Bàn Giao Kế Tiếp (Next Handoff APIs)

Các thành phần sẵn sàng bàn giao cho các Task tiếp theo:
- **Cho Task 4.4 (Temporal History & Value Delegation):**
  - Phương thức `TelemetryHistoryService.ExecuteEffectiveHistoryQuery(ctx, actor, q)`: Nhận context đã có sẵn deadline từ caller, không khởi động lại timeout budget 5 giây, dùng trực tiếp cho delegation thuộc tính `value` của Sensor.
- **Cho Task 4.6 (HTTP Handlers & Server Composition):**
  - DTO mapper `httpserver.NewTelemetryHistoryResponseDTO(*telemetry.TelemetryHistoryResult)`: Chuyển đổi an toàn kết quả service thành wire response JSON đúng hợp đồng mục 8.6–8.7 của Task 4.0.
  - Phân loại lỗi miền `telemetry.ErrHistory*` để map sang HTTP status code (400, 404, 503, 500) và error response envelope.
