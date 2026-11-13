# Tổng hợp Tối ưu Pipeline, Ingress Shaper và Hạ tầng Lưu trữ E2 (P8 Official)

Tài liệu này tổng hợp ngắn gọn chuỗi 6 giải pháp tối ưu hóa hiệu năng được triển khai nhằm giúp hệ thống đạt chuẩn kiểm thử tải khắc nghiệt **P8 E2 Official** (mục tiêu $100.000\text{ messages}$ ở $2.000\text{ msg/s}$ trên môi trường 2 máy chủ độc lập).

---

## 1. Danh mục 6 Giải pháp Tối ưu Kỹ thuật

### 1.1 Tối ưu 1 — L1: Pipeline SQL một group thành 4 SendBatch (`bbc9019`, `1ec3cb8`)
- **Vấn đề trước tối ưu:** Hàm `persistBatchGroup` chạy tuần tự từng message: 3 câu kiểm tra/chèn ledger, 1 câu đọc reported state, 1 câu ghi reported state và 1 câu chèn temporal. Với group 32 messages, hệ thống phải chịu chi phí khoảng 168 vòng đi-về mạng (round-trips) giữa Go backend và PostgreSQL.
- **Giải pháp:** Gộp toàn bộ luồng thực thi trong group thành tối đa **4 chuyến `SendBatch`** của `pgx`:
  1. *Ledger batch:* Gộp kiểm tra và chèn sổ cái.
  2. *Reported state read batch:* Đọc trạng thái một lần duy nhất cho mỗi Sensor trong group, thực hiện tuần tự hóa (project) phiên bản trong Go.
  3. *Reported state update batch:* Cập nhật trạng thái mang theo version guard.
  4. *Temporal batch:* Ghi dữ liệu chuỗi thời gian.
- **Kết quả:** Giảm số round-trip từ 168 xuống còn 4; gom gọn chu kỳ đo metric một lần mỗi group.

### 1.2 Tối ưu 2 — L4: Overlap $O(\log N)$ & Rút gọn Batch Ledger 2N (`5b70d47`)
- **Vấn đề trước tối ưu:** Kiểm tra trùng lặp sequence bằng cách quét dải sequence khiến độ phức tạp tăng theo số lượng message đã lưu trong bảng `processed_messages`. Mỗi message trong batch ledger phải tốn 3 lệnh SQL (xen kẽ INSERT, EXISTS, SELECT PK).
- **Giải pháp:** 
  - Tối ưu truy vấn kiểm tra overlap bằng cách tìm điểm cận trên:  
    `SELECT last_sequence FROM processed_messages WHERE ... ORDER BY first_sequence DESC LIMIT 1`, tận dụng index B-Tree để đạt độ phức tạp **$O(\log N)$**.
  - Rút gọn batch ledger từ $3N$ xuống **$2N$ lệnh**: Chỉ truy vấn Primary Key khi thực tế phát sinh xung đột (`conflict`) thay vì truy vấn mù trên mọi message.
- **Kết quả:** Thời gian xử lý Ledger trong 1 group giảm từ **$3,70\text{ ms}$ xuống $1,34 - 1,36\text{ ms}$ (nhanh hơn gấp 2,7 lần, giảm 63%)**.

### 1.3 Tối ưu 3 — Ingress Shaper: Chuẩn hóa công thức Rate & Burst dự phòng Retry (`9a9f946`)
- **Vấn đề trước tối ưu:** Công thức Shaper cũ tính toán rate và burst quá khít khao với tốc độ danh định. Dưới tải cao, khi có độ trôi lập lịch của OS/TCP hoặc các đợt phát lại (retry attempt), Ingress Shaper đã chặn nhầm hàng chục ngàn tin nhắn hợp lệ (Run15 bị rate-limited $22.167$ lần).
- **Giải pháp:** Xây dựng hàm tính toán tập trung `e2_shaper_caps`:
  - `rate_nominal = (offered_rate + 1) // 2` (rate danh định mỗi gateway).
  - `shaper_rate = math.ceil(round(rate_nominal * 1.10, 6) + retry_rate)` (bù 10% dung sai OS/TCP + lưu lượng retry).
  - `shaper_burst = math.ceil(round(shaper_rate * tolerance_seconds, 6)) + retry_burst + workload_burst` (ngân sách dung sai cửa sổ 1.0s kèm burst).
  - Áp dụng `round(..., 6)` triệt tiêu sai số dấu phẩy động IEEE-754.
- **Kết quả:** Triệt tiêu hoàn toàn hiện tượng bóp nhầm lưu lượng (`ShaperRateLimited` giảm từ $22.167$ lần ở Run15 xuống **$0$ lần** ở Run16–Run20).

### 1.4 Tối ưu 4 — I/O PostgreSQL: Tối ưu WAL & Checkpoint dài hạn (`4ebc460`)
- **Vấn đề trước tối ưu:** Cấu hình mặc định (`min_wal_size=80MB`) khiến PostgreSQL cạn file WAL dự phòng chỉ sau 20 giây ở tải $2.000\text{ msg/s}$. PostgreSQL bị ép tạo file 16MB mới trên đĩa (`IO: WALInitSync`), giữ chặt khóa `WALWriteLock` làm các worker bị treo cứng ngắc. Ngoài ra, checkpoint xả dồn dập làm thời gian chờ đĩa `await` vọt lên 40–80 ms, kéo dài lệnh `COMMIT` lên 12–40 ms.
- **Giải pháp:** Cấu hình tham số khởi động chuyên dụng trong `docker-compose.yml`:
  - `min_wal_size=512MB`, `max_wal_size=2GB`: Duy trì sẵn 32–128 file WAL để PostgreSQL luôn tái chế (recycle/rename) file cũ trong RAM ($< 0,01\text{ ms}$), triệt tiêu `WALInitSync`.
  - `wal_buffers=64MB`: Vùng đệm RAM rộng rãi cách ly worker khỏi đĩa cứng khi có micro-burst.
  - `checkpoint_completion_target=0.9`, `checkpoint_timeout=15min`: Trải đều 90% lượng xả trang bẩn suốt 15 phút, san phẳng băng thông đĩa ở mức trung bình $\approx 7,66\text{ MiB/s}$.
- **Kết quả:** 
  - Độ trễ chốt giao dịch `commit` giảm từ $38,09\text{ ms}$ (Run11) và $12,50\text{ ms}$ (Run16) xuống còn **$1,14 - 1,17\text{ ms}$ (nhanh gấp 11 lần, giảm 90%)**.
  - Đĩa `sda` chỉ bận $2,9\%$ thay vì $15,8\%$; thời gian `await` duy trì mức siêu thấp $0,82\text{ ms/op}$.
  - Giúp **Run18 cán mốc PASS chính thức $100.000\text{ messages}$** (exit code 0, 0 thất thoát dữ liệu).

### 1.5 Tối ưu 5 — Migration 25: Gỡ bỏ Khóa ngoại Hypertable & Đảo chiều Trigger RESTRICT
- **Vấn đề trước tối ưu:** Ràng buộc `fk_telemetry_sensor` trên hypertable `telemetry` kích hoạt System Trigger nội bộ `RI_FKey_check_ins` kiểm tra từng dòng khi nạp dữ liệu. Với kịch bản $S=100$, một group 5 tin có 500 mẫu khiến PostgreSQL phải chạy 500 câu `SELECT 1 ... FOR KEY SHARE` ngầm, làm khâu `copy` tốn tới **$25,52\text{ ms}$ (chiếm 66–72% tổng thời gian group)**.
- **Giải pháp (Migration 25 - `000025_drop_telemetry_sensor_fk`):**
  - Gỡ bỏ `fk_telemetry_sensor` trên bảng `telemetry` (chi phí trigger chiều ghi khi COPY/INSERT giảm về đúng **0,00 ms**).
  - Đảm bảo tính toàn vẹn chiều ghi bằng lệnh khóa `lockTelemetrySensor (... FOR SHARE)` ở đầu transaction của Go backend.
  - Bảo toàn ngữ nghĩa `ON DELETE/UPDATE RESTRICT` ở tầng CSDL bằng trigger đảo chiều `trg_sensors_restrict_telemetry` trên bảng `sensors`: nếu xóa sensor hoặc sửa `sensor_id` khi đã có telemetry, PostgreSQL sẽ chặn đứng và báo lỗi `foreign_key_violation` (`23503`).
- **Kết quả:** Thời gian chèn 500 dòng dữ liệu thô giảm từ **$25,52\text{ ms}$ xuống $\approx 3,82\text{ ms}$ (nhanh gấp 4,6 lần, giảm 78%)**, nâng trần năng lực nuốt của $S=100$ từ $258\text{ msg/s}$ lên xấp xỉ **$\approx 595\text{ msg/s}$ ($59.500\text{ samples/s}$)**.

### 1.6 Tối ưu 6 — L3: Định cỡ Lô Microbatch & Mở trần Flow Control (maxBatchRows & maxBatchMessages)
- **Vấn đề trước tối ưu:** Trần `maxBatchRows = 500` khiến các bản tin mang 100 mẫu ($S=100$) bị ngắt batch non chỉ sau 5 messages ($5 \times 100 = 500$ dòng), làm số lần `COMMIT` vọt lên 2.000 lần trong 10s (200 lần/giây), ép đĩa liên tục và kéo tụt trần thông lượng của 2 sensor leases xuống mức $\approx 300\text{ msg/s}$. Đồng thời trần `maxBatchMessages = 32` giới hạn số lượng tin gom được trong mỗi chu kỳ giao dịch.
- **Giải pháp:**
  - Nâng `maxBatchMessages` từ 32 lên **64** trong Go Ingress Configuration, Repository Batch Guard và E2 Official Test Runner.
  - Nâng `maxBatchRows` từ 500 lên **6.400** theo công thức chuẩn hóa $\text{maxBatchRows} = \text{maxBatchMessages} \times 100$, đảm bảo group có thể gom trọn vẹn 64 messages ở kịch bản $S=100$ mà không bị ngắt non.
  - Ngân sách bộ nhớ RAM của 4 workers với kích thước lô mới là $\approx 24,5\text{ MiB}$, hoàn toàn nằm sâu bên dưới trần an toàn `MaxIngressQueueMemoryBytes` (256 MiB).
- **Kết quả:** Số lần gọi `COMMIT` ở kịch bản $S=100$ giảm tới **12,8 lần** (từ 2.000 lần xuống ~156 lần trong 10s), triệt tiêu hoàn toàn áp lực ép đĩa `WALSync`, nâng mạnh năng lực nuốt của 2 Sensor leases.

### 1.7 Tối ưu 7 — Migration 26: Gỡ bỏ Chỉ mục B-Tree Trùng lặp trên Bảng Telemetry
- **Vấn đề trước tối ưu:** Bảng `telemetry` duy trì đồng thời 3 B-Tree indexes: `telemetry_pkey (measured_at, sample_id)`, `telemetry_message_sample (measured_at, gateway_id, message_id, sample_index)`, và `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)`.
  - Khảo sát mã nguồn thực tế chứng minh:
    1. `telemetry_message_sample` dư thừa 100%: việc chống trùng bản tin được giải quyết triệt để tại bảng sổ cái `processed_messages (gateway_id, message_id)`. Mã nguồn Go backend không bao giờ nạp các bản tin trùng vào bảng `telemetry`.
    2. `telemetry_pkey` không bao giờ được bất kỳ câu truy vấn đọc API nào sử dụng (các câu query đọc luôn lọc theo `(gateway_id, sensor_id)` nên sử dụng `idx_telemetry_gateway_sensor_time`). Đồng thời, TimescaleDB Hypertable không bắt buộc phải có Primary Key (tương tự như hypertable `twin_temporal_values`).
  - Việc duy trì 3 cây B-Tree cho hàng triệu bản ghi tạo ra áp lực ghi đĩa WAL khổng lồ (49,34 MiB/s trong Run22), làm nghẽn cổ chai ổ đĩa và chiếm tới 75% CPU trong lệnh `COPY`.
- **Giải pháp (Migration 26 - `000026_drop_telemetry_redundant_indexes`):**
  - Gỡ bỏ partial unique index `telemetry_message_sample`.
  - Gỡ bỏ ràng buộc Primary Key không cần thiết `telemetry_pkey`.
  - Giữ lại duy nhất 1 index phục vụ đọc: `idx_telemetry_gateway_sensor_time`.
  - Cung cấp script rollback an toàn: `scripts/sql/rollback-migration-000026.sql`.
- **Kết quả định lượng (Đo đạc trên 6.400 dòng/group):**
  - Thời gian chèn `COPY` 6.400 dòng giảm từ **$91,20\text{ ms}$ xuống $69,32\text{ ms}$ (nhanh hơn 24% CPU)**.
  - Dung lượng WAL sinh ra giảm từ **$3,16\text{ MiB}$ xuống $2,29\text{ MiB}$/group (tiết kiệm 27,5% WAL)**.
  - Lưu lượng ghi đĩa giảm từ $49,34\text{ MiB/s}$ về $\approx 32 - 35\text{ MiB/s}$, đưa thời gian chờ I/O đĩa `await` từ $10,09\text{ ms}$ về mức an toàn $\approx 4 - 5\text{ ms/op}$.

---

## 2. Ghi chú Kỹ thuật về Đề xuất L2 (Set-based SQL / Multi-row)

Trong quá trình khảo sát kiến trúc, phương án L2 (gộp các lệnh INSERT thành chuỗi cú pháp multi-row `INSERT INTO ... VALUES (...), (...)` hoặc gộp UPDATE bằng cú pháp `CASE`) đã được xem xét và **quyết định không triển khai**, vì:
1. **Pipelining L1 đã đạt trần hiệu quả mạng:** Kỹ thuật `SendBatch` của `pgx` đã gom các câu lệnh gửi đi trong 1 gói tin TCP duy nhất, triệt tiêu hoàn toàn chi phí network latency mà vẫn giữ nguyên cú pháp SQL chuẩn mực.
2. **Bảo toàn ranh giới lỗi và phân loại:** Giữ các lệnh SQL độc lập trong batch giúp hệ thống xác định chính xác vị trí message bị xung đột (`conflict`) hoặc vi phạm version guard mà không cần bóc tách chuỗi phức tạp trong Go.
3. **Hiệu năng đã được giải phóng vượt kỳ vọng:** Sự kết hợp giữa L1 (4 SendBatch), L4 ($O(\log N)$) và Migration 25 (loại bỏ trigger `RI_FKey`) đã giảm hơn 70% tổng thời gian thực thi của database, đáp ứng trọn vẹn yêu cầu kiểm thử tải mà không làm phát sinh rủi ro sai lệch ngữ nghĩa giao dịch.

---

## 3. Bảng Tổng hợp Hiệu năng Trước và Sau Chuỗi Tối ưu

| Chỉ số / Thành phần | Ban đầu (Chưa tối ưu) | Sau chuỗi Tối ưu | Mức độ cải thiện |
|---|:---:|:---:|:---:|
| **Thời gian chốt COMMIT** | 12,5 – 38,1 ms | **1,14 – 1,17 ms** | **Nhanh gấp 10 – 30 lần (-90%)** ⚡⚡ |
| **Kiểm tra Overlap & Ledger (L4)** | 3,70 – 7,95 ms | **1,34 – 1,36 ms** | **Nhanh gấp 2,7 – 5,8 lần (-63%)** ⚡ |
| **Ghi Raw Telemetry 500 mẫu (M25)** | 25,52 – 29,28 ms | **~3,82 ms** | **Nhanh gấp 4,6 – 6,7 lần (-78%)** 🚀 |
| **Chặn nhầm Ingress Shaper** | 1.098 – 22.167 lần | **0 lần tuyệt đối** | **Triệt tiêu hoàn toàn (-100%)** 🎯 |
| **Thông lượng nuốt ổn định (S=1)** | ~900 – 1.180 msg/s | **1.940 – 1.976 msg/s** | **Tăng +64% đến +119%** (Đỉnh 2.470 msg/s) |
| **Khả năng hoàn thành E2 Official** | Dừng sớm / Thiếu tin (FAIL) | **PASS 100.000 msgs (Exit 0)** | **Nghiệm thu thành công trọn vẹn** ✅ |
