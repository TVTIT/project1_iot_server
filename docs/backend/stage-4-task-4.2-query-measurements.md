# Báo Cáo Đo Đạc Thực Tế Query Plan Telemetry History (Task 4.2.7)

**Ngày thực hiện:** 2026-10-17  
**Mục tiêu:** Đo lường query plan và hiệu năng thực thi thực tế bằng câu lệnh `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` trên dataset đại diện 60.000 raw samples dưới quyền role ứng dụng `iot_backend_app`, đối chiếu với ngân sách thời gian 5 giây và đưa ra quyết định có cơ sở về việc giữ nguyên Schema 20 hay bổ sung index.

---

## 1. Môi Trường Cơ Sở Dữ Liệu & Cấu Hình Đo Đạc

- **PostgreSQL Version:** 16.15 (Debian 16.15-1.pgdg120+1)
- **TimescaleDB Version:** 2.29.2
- **Schema Migrations Terminal:** Phiên bản 20 (`stage3_telemetry_ingestion`)
- **Application Role:** `iot_backend_app`
  - `rolsuper`: `false` (không có quyền superuser)
  - `rolcanlogin`: `true`
  - `rolbypassrls`: `true` (vai trò backend sở hữu bypass RLS cấp ứng dụng)
- **Pool Settings (`pgxpool.Pool`):**
  - `MaxConns`: 12
  - `MinConns`: 0
  - `MaxConnIdleTime`: 30 phút
- **Trạng thái nén Hypertable (`timescaledb_information.chunks`):**
  - Bảng hypertable `public.telemetry` đang ở trạng thái **uncompressed** (`is_compressed = false` trên toàn bộ 4 chunks hiện hữu).
  - *Lưu ý:* Báo cáo này không xác nhận hiệu năng trên dữ liệu đã nén (compressed chunks). Đánh giá nén được bảo lưu cho Task 4.7.

---

## 2. Dữ Liệu Đại Diện Đã Seed (Representative Dataset)

Dữ liệu được seed tự động và dọn dẹp chính xác theo phạm vi fixture:

1. **Target Dataset (Gateway & Sensor mục tiêu):**
   - Gateway ID: `gw_meas_<runID>`
   - Sensor ID: `sens_meas_<runID>` (unit: `degC`)
   - Tần số đo: **100 Hz** (chu kỳ mẫu 10.000 µs = 10 ms)
   - Thời gian: **10 phút** (600 giây), từ `2026-10-17T12:00:00Z` đến `2026-10-17T12:10:00Z`
   - Tổng số mẫu: **60.000 raw samples**
   - Đóng gói tin nhắn: 600 messages (mỗi batch 100 samples, `sample_index` từ 0 đến 99, `sequence_num` tuần tự 0 đến 59.999)
2. **Distractor Dataset (Gateway & Sensor phân tán - kiểm tra Selectivity):**
   - Gateway ID: `gw_distr_<runID>`
   - Sensor ID: `sens_distr_<runID>` (unit: `ppm`)
   - Tần số đo: 10 Hz (chu kỳ mẫu 100.000 µs = 100 ms)
   - Thời gian: cùng khoảng 10 phút
   - Tổng số mẫu: **6.000 raw samples** (60 messages)
3. **Tổng số bản ghi insert vào `public.telemetry`:** **66.000 rows**.
4. **Quyền truy cập:**
   - User test được cấp quyền `owner` trên `targetGW`.
   - User test **không** có quyền trên `distractGW`.

---

## 3. Kết Quả Đo Đạc Ba Kịch Bản Trọng Yếu

Các câu query đo đạc sử dụng **chính xác** câu lệnh SQL nội bộ của `PostgresHistoryRepository` (`RawTelemetryHistoryQueryForTest` và `BucketTelemetryHistoryQueryForTest`), cùng tham số kiểu dữ liệu (`uuid`, `text`, `timestamptz`, `pgtype.Interval`), origin Unix Epoch `1970-01-01 00:00:00+00` và mệnh đề CTE ủy quyền `user_gateways`.

> **Lưu ý quan trọng về bộ đệm (Buffer Usage) và trạng thái rà soát:**  
> 1. **Hiệu chỉnh số liệu buffer (Root-only Buffer Counters):** Đợt rà soát kỹ thuật (review) đã xác định hàm thu thập `collectPlanSummary` trong mã test trước đây tính tổng buffer bằng cách cộng dồn cả bộ đếm của node cha lẫn node con (`SharedHitBlocks`, `SharedReadBlocks` trong cây EXPLAIN JSON bao hàm nhau), dẫn đến kết quả bị cộng trùng lặp. Vấn đề này đã được khắc phục hoàn tất trong mã nguồn test bằng cách chỉ đọc bộ đếm từ root plan node. Các số đo buffer accesses bên dưới đã được cập nhật chính xác từ root node. Mọi quy đổi dung lượng (MB/KB) đã được loại bỏ vì buffer hits thể hiện số lượt truy cập bộ đệm (cache accesses), không đại diện cho lượng bộ nhớ RAM thường trú duy nhất (resident memory).  
> 2. **Tính chất các số đo thời gian:** Các số đo thời gian thực thi (Execution Time) và số dòng đầu ra hợp lệ được bảo lưu; tuy nhiên, đây là kết quả của các lượt chạy riêng biệt (separate runs) trên môi trường thử nghiệm uncompressed, không phải cam kết bảo đảm hiệu năng cố định (performance guarantees).  
> 3. **Trạng thái rà soát kỹ thuật (Review Status):** Đợt rà soát kỹ thuật của `@go-reviewer` đã xác định 3 vấn đề mức MEDIUM (bao gồm việc sửa bộ thu thập buffer counters trong test đo đạc, kiểm tra range và timeout/cancellation) và toàn bộ 3 vấn đề đã được khắc phục hoàn tất kèm các regression tests liên quan.

### Kịch bản 1: Short Raw Window (Cửa sổ raw ngắn 5 giây, khớp cap 500 mẫu)

- **Cửa sổ thời gian:** `[12:00:00, 12:00:05)` (5 giây = 500 samples @ 100 Hz).
- **Yêu cầu query:** `resolution = raw`, `max_points = 500`, `LIMIT = 501`.
- **Kết quả EXPLAIN (ANALYZE, BUFFERS):**
  - **Planning Time:** 3,551 ms (lần đầu phân tích partition hypertable)
  - **Execution Time:** **0,676 ms** (ngân sách: 5.000 ms)
  - **Actual Rows:** 500 rows
  - **Buffer Usage [Đo đạc đính chính từ root plan]:** Shared Hit = 28 blocks (8KB blocks đọc từ root plan node, không cộng dồn trùng lặp), Shared Read = 0. Không quy đổi dung lượng RAM vì buffer hits phản ánh số lượt truy cập bộ đệm (cache accesses), không đại diện cho lượng RAM thường trú duy nhất.
  - **Plan Nodes:** `Limit` -> `Sort (quicksort)` -> `Nested Loop` -> `Index Scan` trên chunk `_hyper_1_36_chunk`
  - **Index Sử Dụng:** `_hyper_1_36_chunk_idx_telemetry_gateway_sensor_time` (B-tree: `gateway_id, sensor_id, measured_at DESC`)
  - **Quan sát phân vùng (Chunk Scan):** Query plan quan sát được chỉ quét duy nhất một chunk (`_hyper_1_36_chunk`), với điều kiện thời gian được áp trực tiếp lên thuộc tính `measured_at`. Báo cáo chỉ ghi nhận quan sát thực tế này từ plan node, không khẳng định các chunk cụ thể khác đã được prune một cách chứng minh được do EXPLAIN plan không hiển thị trực tiếp danh sách pruned chunks.
- **Kết quả Adapter Go (`GetTelemetryHistory`):**
  - `EffectiveResolution`: `raw`
  - `Points`: 500 raw points
  - `Service Elapsed`: 2,23 - 2,40 ms

### Kịch bản 2: Longer Bucket Query (Cửa sổ bucket dài 10 phút, tự động chọn resolution)

- **Cửa sổ thời gian:** `[12:00:00, 12:10:00)` (10 phút = 600 giây, bao trọn 60.000 samples).
- **Yêu cầu query:** `max_points = 500`. Hàm `SelectAutoBucketResolution` chọn độ phân giải nhỏ nhất thỏa mãn cap: **`10s`** (60 buckets ≤ 500).
- **Tham số Interval:** `10s` (`pgtype.Interval{Microseconds: 10_000_000, Valid: true}`). `LIMIT = 501`.
- **Kết quả EXPLAIN (ANALYZE, BUFFERS):**
  - **Planning Time:** 0,87 - 1,02 ms
  - **Execution Time:** **66,54 - 69,76 ms** (ngân sách: 5.000 ms; lưu ý: kết quả thực tế ~66-70 ms, không phải dưới 50 ms)
  - **Actual Rows:** 60 buckets (mỗi bucket tổng hợp chính xác 1.000 raw samples)
  - **Buffer Usage [Đo đạc đính chính từ root plan]:** Shared Hit = 1.926 blocks (8KB blocks đọc từ root plan node, khắc phục lỗi cộng dồn con trước đây), Shared Read = 0. Đã loại bỏ hoàn toàn ước tính quy đổi MB vì buffer hits phản ánh số lượt truy cập bộ đệm, không phản ánh lượng RAM thường trú duy nhất.
  - **Plan Nodes:** `Limit` -> `Nested Loop` -> `Sort` -> `Aggregate (GroupAggregate / HashAggregate)` -> `Index Scan` trên chunk hypertable
  - **Index Sử Dụng:** `_hyper_1_36_chunk_idx_telemetry_gateway_sensor_time`
  - **Quan sát phân vùng (Chunk Scan):** Query plan quan sát được chỉ quét một chunk (`_hyper_1_36_chunk`) khớp với điều kiện lọc thời gian trên `measured_at`.
- **Kết quả Adapter Go (`ReadTelemetryHistory`):**
  - `EffectiveResolution`: `10s`
  - `Points`: 60 buckets (mỗi bucket có `sample_count = 1000`, `min`, `max`, `avg` chính xác)
  - `Service Elapsed`: 57,20 - 59,85 ms (đây là lần thực thi riêng của service adapter, độc lập với lần chạy EXPLAIN)

### Kịch bản 3: Short Raw Probe Bị Tràn & Fallback Sang Bucket

- **Cửa sổ thời gian:** `[12:00:00, 12:00:08)` (8 giây = 800 samples @ 100 Hz).
- **Yêu cầu query:** `resolution = auto`, `max_points = 500`.
- **Bước 3a - Raw Probe (EXPLAIN riêng):**
  - Vì khoảng thời gian (8 giây) ≤ 10 giây (`RawWindow`), service probe raw với `LIMIT = 501`.
  - **Execution Time:** **0,64 - 1,18 ms**
  - **Actual Rows:** 501 rows (LIMIT chặn ở 501 dòng -> phát hiện overflow 501 > 500).
  - **Buffer Usage [Đo đạc đính chính từ root plan]:** Shared Hit = 31 blocks (8KB blocks từ root plan), Shared Read = 0.
  - Service phát hiện `ErrHistoryRawLimitExceeded` và chuyển tiếp sang bucket fallback.
- **Bước 3b - Bucket Fallback (EXPLAIN riêng):**
  - `SelectAutoBucketResolution` chọn độ phân giải **`100ms`** (8s / 100ms = 80 buckets ≤ 500).
  - Bucket query chạy với interval `100ms`, `LIMIT = 501`.
  - **Execution Time:** **1,05 - 1,59 ms**
  - **Actual Rows:** 80 buckets (mỗi bucket tổng hợp 10 samples).
  - **Buffer Usage [Đo đạc đính chính từ root plan]:** Shared Hit = 31 blocks (8KB blocks từ root plan), Shared Read = 0.
- **Bước 3c - Phân định thời gian thực thi:**
  - Tổng Execution Time của 2 lệnh EXPLAIN riêng biệt (Probe 3a + Fallback 3b): **1,69 - 2,23 ms** (lưu ý: đây là tổng số đo của hai lần chạy EXPLAIN độc lập, không phải số đo end-to-end của service).
  - Thời gian thực thi thực tế của `HistoryService.GetTelemetryHistory` (một lần gọi bao gồm resolver + raw probe + fallback bucket): **4,10 - 8,58 ms**.
  - Kết quả trả về: 80 bucket points, độ phân giải hiệu dụng `100ms`.

---

## 4. Xác Minh Tính Chọn Lọc & Cô Lập Dữ Liệu (Selectivity)

1. Khi thực thi các query cho Target Gateway (`targetGW`), kết quả trả về chỉ chứa duy nhất dữ liệu của Target Gateway; toàn bộ 6.000 samples của `distractGW` bị loại trừ hoàn toàn thông qua CTE join và điều kiện `gateway_id, sensor_id`.
2. Khi user test truy vấn `distractGW` (không có quyền membership), câu lệnh CTE `authorized_sensor` trả về 0 dòng, adapter Go phân loại chuẩn xác thành lỗi `ErrHistoryNotFound`, không làm lộ sự tồn tại của Gateway/Sensor.

---

## 5. Dọn Dẹp Fixture (Teardown Verification)

Quy trình defer hook thực hiện xóa chính xác tài nguyên fixture theo thứ tự khóa ngoại:
1. `DELETE FROM public.telemetry WHERE gateway_id IN (targetGW, distractGW)` (xóa toàn bộ 66.000 dòng).
2. `DELETE FROM public.user_gateways WHERE gateway_id IN (targetGW, distractGW)`.
3. `DELETE FROM public.sensors WHERE gateway_id IN (targetGW, distractGW)`.
4. `DELETE FROM public.gateways WHERE gateway_id IN (targetGW, distractGW)`.
5. `DELETE FROM public.profiles WHERE id = testUserUID`.
6. `DELETE FROM auth.users WHERE id = testUserUID`.
- **Kiểm tra sau dọn dẹp:** Truy vấn `COUNT(*)` xác nhận còn đúng **0 dòng** thuộc về các Gateway fixture. Không có tài nguyên rác tồn đọng.

---

## 6. Quyết Định Về Schema 20 & Index

| Chỉ số | Kết quả đo đạc | Ngân sách cam kết | Đánh giá |
|---|---|---|---|
| M1 (Raw 5s, 500 pts) | **0,58 - 0,68 ms** (Service: 2,23 - 2,40 ms) | 5.000 ms | Đạt (vượt trội) |
| M2 (Bucket 10m, 60 pts / 60k rows) | **66,54 - 69,76 ms** (Service: 57,20 - 59,85 ms) | 5.000 ms | Đạt (vượt trội) |
| M3 (Probe + Fallback EXPLAIN sum) | **1,69 - 2,23 ms** (Service M3: 4,10 - 8,58 ms) | 5.000 ms | Đạt (vượt trội) |

### Kết luận kỹ thuật:
1. **Index hiện hữu được sử dụng hiệu quả:** B-tree composite index `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)` được PostgreSQL và TimescaleDB sử dụng cho cả raw range scan lẫn bucket aggregation range scan; không có lý do hoặc căn cứ thực nghiệm nào từ dataset đo đạc này đòi hỏi phải thay đổi index hay thêm index mới.
2. **Áp dụng điều kiện thời gian trên chunk:** Các điều kiện lọc `measured_at >= $4 AND measured_at < $5` được áp trực tiếp trong query plan, và planner chỉ quét chunk chứa dữ liệu truy vấn thay vì toàn bộ bảng.
3. **Không cần index suy đoán hay migration mới:** Toàn bộ query plan thực thi ổn định dưới 75 ms (với bucket 10 phút / 60k dòng đạt ~66-70 ms, service ~57-60 ms), cách rất xa ngưỡng budget 5.000 ms. Do đó, **giữ nguyên Schema 20** và hệ thống index hiện có, không tạo migration mới mang tính suy đoán.

---

## 7. Giới Hạn & Phạm Vi Kiểm Chứng (Limitations)

- **Dataset & Môi trường uncompressed:** Thử nghiệm được thực hiện trên dataset đại diện 60.000 raw samples (10 phút @ 100 Hz) trên môi trường uncompressed (`is_compressed = false` trên các chunks hiện hữu). Báo cáo **không** khẳng định hiệu năng trên dữ liệu đã nén (compressed hypertable chunks) hoặc khoảng thời gian tối đa 31 ngày chịu tải liên tục; nội dung đó thuộc phạm vi tích hợp Task 4.7.
- **Phân định số đo & Không bảo đảm hiệu năng cố định:** Số đo Execution Time của các lệnh EXPLAIN riêng biệt và thời gian xử lý của service adapter được thực hiện trong các lần chạy độc lập; tổng thời gian EXPLAIN của probe + fallback không đại diện cho độ trễ dịch vụ end-to-end. Các số đo phản ánh kết quả thử nghiệm riêng lẻ tại thời điểm đo, không cấu thành cam kết bảo đảm hiệu năng cố định (performance guarantees).
- **Số liệu Buffer:** Các số liệu buffer blocks được đọc chuẩn xác từ root plan node (không cộng dồn con); buffer hits phản ánh số lượt truy cập bộ đệm (cache accesses) chứ không đại diện cho dung lượng RAM thường trú duy nhất.
- **Cơ chế Timeout / Cancellation:** Thử nghiệm context deadline/timeout trong bộ test native của Task 4.2 (`TestNativeTelemetryHistoryPersistRetryAndReadIntegrity`) chỉ kiểm chứng việc hết hạn context khi hàng đợi xin kết nối từ pool (`pgxpool.Pool`) bị cạn kiệt; thử nghiệm **không** khẳng định khả năng hủy hay ngắt một truy vấn đang thực thi dở dang bên trong engine PostgreSQL (executing-query cancellation).
- **Ranh giới hệ thống:** Báo cáo **không** khẳng định việc kích hoạt các HTTP endpoint (các route HTTP tại `cmd/server` vẫn giữ nguyên stub `501 Not Implemented`, thuộc Task 4.6), không khẳng định kiểm thử tích hợp toàn trình MQTT-to-HTTP (thuộc Task 4.7), không khẳng định chạy CI từ xa (remote CI) cho Task 4.2, và không phải là chứng nhận môi trường production.
- **Trạng thái rà soát kỹ thuật (Review Status):** Đợt rà soát kỹ thuật của `@go-reviewer` đã xác định 3 vấn đề mức MEDIUM (bao gồm việc sửa bộ thu thập buffer counters trong test đo đạc, kiểm tra range và timeout/cancellation) và toàn bộ 3 vấn đề đã được khắc phục hoàn tất kèm các regression tests liên quan.
