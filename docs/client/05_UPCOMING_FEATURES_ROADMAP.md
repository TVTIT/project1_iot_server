# Tài liệu Client — 05: Lộ trình và Thiết kế Định hướng Tính năng Mới trên Client Dashboard (Upcoming Features Roadmap)

**Cập nhật:** 2026-10-18
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 0, 1, 2, 3, 4, 5.6, 6, 7, 10, 12, 13, 15, 16, 17, 18), `../docs/backend_plan/Backend_summary_plan.md` (Kế hoạch 9 giai đoạn backend), `docs/protocols/stage4-read-and-entity-api.md`, `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/03_USER_GATEWAY_AUTHORIZATION.md`, `docs/client/04_REST_API_CLIENT_CONTRACT.md`, `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`, `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`.

---

## 1. Mục đích tài liệu và Phân định ranh giới tính năng (Architecture Roadmap & Feature Boundaries)

### 1.1. Hiện trạng triển khai tại Go Backend và Kết quả kiểm chứng Stage 4

Hệ thống backend Go đã hoàn thành các mốc kỹ thuật quan trọng theo lộ trình 9 giai đoạn:
- **Giai đoạn 2 (Xác thực, phân quyền và quản lý trạm cơ bản):** Định danh tập trung qua Supabase Auth, kiểm tra JWT nội bộ kết hợp quan hệ `user_gateways` và quyền quản trị `platform_admins`. Các API danh mục (`GET /v1/gateways`, `GET /v1/gateways/{gateway_id}/sensors`) và khởi tạo cảm biến (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`) hoạt động với quy tắc fail-closed chặt chẽ. Quản trị MQTT Dynamic Security được thiết lập qua endpoint admin.
- **Giai đoạn 3 (Uplink hoàn chỉnh: MQTT → Ingress → Lưu DB → Commit ACK):** Pipeline thu thập dữ liệu từ trạm hoàn chỉnh: Gateway simulator lưu SQLite outbox trước khi phát qua MQTT TLS, Go subscriber nhận qua hàng đợi có giới hạn và fixed worker pool, ghi đồng thời vào PostgreSQL transaction (raw telemetry, dedup ledger `processed_messages`, cập nhật `reported_state` cảm biến và bản ghi temporal `sample_interval_us` trong `twin_temporal_values`), chỉ gửi application commit ACK khi giao dịch đã commit thành công.
- **Giai đoạn 4 (API Lịch sử, Đọc Digital Twin & Quản lý Metadata Thực thể - ĐÃ HOÀN TẤT & CHUYỂN ĐỔI SANG 5 ENDPOINT ETSI GS CIM 009 V1.9.1 PROFILE):**
  Hệ thống ban đầu triển khai 7 endpoint Stage 4 (commit `aa7436d`, CI run 39103564379). Sau đó, hệ thống đã hoàn tất kế hoạch chuyển đổi theo chuẩn ETSI GS CIM 009 V1.9.1 profile thành **đúng năm (05) endpoint chính thức** trên Go Backend (`src/internal/httpserver/router.go`), bao gồm:
  1. `GET /v1/digital-twins`: Danh sách thực thể Digital Twin dạng mảng root array `[...]` (`application/ld+json`), lọc `type=Gateway\|Sensor`, phân trang qua HTTP Header `Link: <URI>; rel="next"`.
  2. `POST /v1/digital-twins`: Khởi tạo thực thể mới hoặc phát lại chính xác (create-or-exact-replay) cho Gateway (yêu cầu Platform Admin) hoặc Sensor (yêu cầu Platform Admin hoặc Gateway Owner), trả về header `Location: /v1/digital-twins/<escaped_urn>` khi `201 Created`.
  3. `GET /v1/digital-twins/{entity_id}`: Chi tiết thực thể dạng chuẩn JSON-LD (`application/ld+json`) kèm `@context` V1.9 nội tuyến và mảng quan hệ `hasSensor` (`objectType: "Sensor"`), phân trang quan hệ qua header `Link`. Hỗ trợ extension dự án `?format=simplified` đọc trạng thái phẳng `twin_states` (`application/json`).
  4. `PATCH /v1/digital-twins/{entity_id}/attrs`: Cập nhật siêu dữ liệu thực thể qua Property fragment (Gateway: sửa `name`, `description`; Sensor: sửa `name`; xóa mô tả Gateway bằng sentinel `urn:ngsi-ld:null`; `unit` bất biến). Đường dẫn gốc không có `/attrs` nhận `405 Method Not Allowed`.
  5. `GET /v1/digital-twins/{entity_id}/temporal`: Hợp nhất truy vấn lịch sử viễn trắc telemetry (`attrs=measurement`) và chu kỳ đo (`attrs=sample_interval_us`) cho Sensor; yêu cầu `timerel=between`, `timeAt`/`endTimeAt` UTCZ, hỗ trợ raw tuples và bucket triples với 9 canonical durations; `totalCount` là số nguyên exact integer.
  *(Bốn route cũ `/v1/telemetry/history`, `/{id}/state`, `/{id}/history`, và `PATCH /{id}` đã thu hồi hoàn toàn, trả về 404 hoặc 405).*

  *Bằng chứng kiểm chứng backend:* Bộ kiểm thử tích hợp đa dịch vụ thực `scripts/test-stage4-api.sh --scenario all` đã PASS toàn bộ. Trên hệ thống CI từ xa GitHub Actions, **run 39185129590** tại commit `449697ddbd8ba53173a3cd885c0d6804458cc81e` đã hoàn thành thành công toàn bộ **16/16 jobs** (bao gồm `stage4-api-integration`, `stage3-uplink-integration` và `native-compose-smoke`). Kết quả kiểm chứng CI này xác nhận tính đúng đắn của mã nguồn backend nhưng **không phải là chứng nhận chất lượng cho môi trường sản xuất (production certification)**, và **không đồng nghĩa với việc giao diện người dùng Flutter Web đã được hiện thực hóa hay hoàn thiện**.

### 1.2. Phân định rõ ràng giữa Hợp đồng chính thức Stage 4 và Bản phác thảo dự kiến tương lai

Để các bạn sinh viên và kỹ sư phát triển nắm bắt chính xác khi tích hợp, tài liệu này thiết lập ranh giới kỹ thuật dứt khoát:

1. **Nhóm tính năng ĐÃ HOÀN TẤT VÀ CÓ HỢP ĐỒNG CHÍNH THỨC (Stage 4 Backend):**
   - Các API truy vấn lịch sử (`/v1/telemetry/history`), danh sách Twin (`/v1/digital-twins`), chi tiết Twin (`/v1/digital-twins/{entity_id}`), trạng thái Twin (`/v1/digital-twins/{entity_id}/state`), lịch sử thuộc tính Twin (`/v1/digital-twins/{entity_id}/history`), tạo entity (`POST /v1/digital-twins`) và sửa metadata (`PATCH /v1/digital-twins/{entity_id}`) **đã hoạt động trên Go backend, không còn là stub 501**.
   - **Định tuyến tham chiếu hợp đồng:** Mọi thông số request/response, headers, query parameters, mã lỗi, quy tắc phân quyền, và cấu trúc DTO chính thức của nhóm này được quy định đầy đủ tại [`docs/client/04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md) và tài liệu giao thức backend [`docs/protocols/stage4-read-and-entity-api.md`](../protocols/stage4-read-and-entity-api.md). Không áp dụng quy tắc "mọi payload đều dự kiến" (blanket provisional) cho nhóm Stage 4 này nữa.
2. **Nhóm tính năng TƯƠNG LAI DỰ KIẾN (Giai đoạn 5, 6, 7, 8):**
   - **Kênh truyền thời gian thực (WebSocket Telemetry - Giai đoạn 7):** Endpoint `GET /v1/ws` hiện vẫn là stub giữ chỗ trả về mã `501 Not Implemented` (sau khi xác thực JWT thành công; thiếu token nhận `401 Unauthorized`).
   - **Tương tác Điều khiển Thiết bị (Downlink Commands & Desired State - Giai đoạn 5 & 6):** Các endpoint `PATCH /v1/digital-twins/{entity_id}/desired-state`, `POST /v1/digital-twins/{entity_id}/commands`, `GET /v1/digital-twins/{entity_id}/commands`, và `GET /v1/commands/{command_id}` hiện **chưa được khai báo** trong bộ định tuyến Gin và sẽ nhận mã `404 Not Found` từ `router.NoRoute`.
   - **Đa phương tiện / Hình ảnh lưu trữ (Media Viewer - Giai đoạn 8):** Các route media (`GET /v1/gateways/{gateway_id}/media`, `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`) hiện **chưa được khai báo** và sẽ nhận mã `404 Not Found`.
   - Mọi định dạng JSON, cấu trúc gói tin, sự kiện streaming WebSocket, Signed URL, và payload lệnh cho nhóm tương lai này được trình bày dưới đây **hoàn toàn mang tính chất phác thảo định hướng (provisional)**, phục vụ chuẩn bị kiến trúc trước, **tuyệt đối không phải hợp đồng cam kết** và **không thiết kế giao thức mới** ngoài phạm vi đồ án.

```text
+---------------------------------------------------------------------------------------------------+
|               LỘ TRÌNH ĐỊNH HƯỚNG TÍNH NĂNG CLIENT THEO 9 GIAI ĐOẠN BACKEND                       |
+---------------------------------------------------------------------------------------------------+
| Giai đoạn 1 (Xong):       | Nền tảng Go, schema baseline, hợp đồng giao thức                      |
| Giai đoạn 2 (Xong):       | Đăng nhập, Phân quyền user_gateways, Danh mục Trạm/Cảm biến           |
|                           | Quản trị Hạ tầng Trạm/Cảm biến/MQTT (Chi tiết doc 07)                 |
| Giai đoạn 3 (Uplink Xong):| Pipeline Uplink: Gateway Outbox -> MQTT TLS -> Ingress -> DB Commit   |
|                           | Lưu trữ raw telemetry, ledger, reported state & temporal interval     |
| Giai đoạn 4 (Backend Xong)| API Lịch sử & Đọc/Quản lý Digital Twin (7 endpoints chính thức):       |
|                           | GET history, GET twins, {entity_id}, /state, /history, POST, PATCH    |
|                           | (Backend đã test PASS; giao diện Flutter UI đang ở lộ trình tích hợp) |
| Giai đoạn 5 (Dự kiến):    | Lát cắt Điều khiển Digital Twin (Downlink Control Slice):             |
|                           | PATCH desired-state, Commands outbox, MQTT execution, Gateway result  |
|                           | correlation -> GET command status (Xem mục 5)                         |
| Giai đoạn 6 (Dự kiến):    | Timeout, Retry & Reconciliation: Vòng đời lệnh đầy đủ, retry backoff, |
|                           | đối chiếu hội tụ desired/reported state qua gián đoạn / reconnect     |
| Giai đoạn 7 (Dự kiến):    | WebSocket Telemetry có phân quyền: GET /v1/ws, push event sau DB      |
|                           | commit, fan-out theo quyền truy cập user_gateways (Xem mục 3)         |
| Giai đoạn 8 (Dự kiến):    | Media Ảnh qua Private Supabase Storage: Signed Upload/Read URL,        |
|                           | kiểm tra quyền Gateway/User, bucket private tuyệt đối (Xem mục 4)     |
| Giai đoạn 9 (Dự kiến):    | Nghiệm thu toàn diện, Đo tải & Triển khai: E2E, backup/restore thử,    |
|                           | hoàn thiện tài liệu tích hợp và đóng gói chuyển giao                 |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Tính năng 1: Biểu đồ Chuỗi thời gian Lịch sử (Historical Time-Series Charts — Backend Stage 4 đã hoàn thành)

### 2.1. Rào cản Dữ liệu Lớn và Nguyên lý Gom mẫu Thích ứng trên Backend
- Cảm biến tần số cao trong hệ thống có thể thu thập dữ liệu với tốc độ lên tới **100 Hz** (100 mẫu/giây = 8.640.000 mẫu/ngày cho mỗi kênh đo).
- Giai đoạn 3 của backend đã hoàn thiện việc tiếp nhận và lưu trữ toàn bộ các mẫu thô này vào TimescaleDB hypertable (`telemetry`), ghi nhận bản ghi temporal `sample_interval_us` trong `twin_temporal_values`, và cập nhật `reported_state` cảm biến.
- **Tại Giai đoạn 4**, backend đã chính thức cung cấp endpoint truy vấn chuỗi thời gian hợp nhất theo chuẩn ETSI GS CIM 009 V1.9.1:
  `GET /v1/digital-twins/{entity_id}/temporal` (hỗ trợ `attrs=measurement` cho viễn trắc telemetry và `attrs=sample_interval_us` cho chu kỳ đo).
  *(Lưu ý: Route cũ `GET /v1/telemetry/history` và `GET /v1/digital-twins/{entity_id}/history` đã được thu hồi và trả về `404 Not Found`).*
- Backend sử dụng hàm `time_bucket()` của **TimescaleDB** để tính toán gộp mẫu thích ứng (Adaptive Downsampling). Phía Client sẽ nhận dữ liệu đã được tổng hợp theo các khoảng thời gian đều nhau với số lượng điểm tối ưu từ 500 đến 1.000 điểm cho một biểu đồ hiển thị. Toàn bộ mẫu thô vẫn được lưu trữ đầy đủ trong TimescaleDB để phục vụ phân tích chuyên sâu.

### 2.2. Trạng thái Endpoint và Quy chuẩn Hợp đồng Chính thức

> **Thông tin trạng thái chính thức:** Endpoint lịch sử hiện hành là `GET /v1/digital-twins/{entity_id}/temporal` **ĐÃ HOẠT ĐỘNG HOÀN TOÀN TRÊN BACKEND**. Hợp đồng chính thức với đầy đủ thông số được ghi nhận tại [`docs/client/04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md) (Mục 5.4).

Tóm tắt các quy chuẩn quan trọng mà client cần tuân thủ khi tích hợp:
- **Phương thức & Đường dẫn:** `GET /v1/digital-twins/{entity_id}/temporal` (với `{entity_id}` là URN của Sensor).
- **Xác thực:** Yêu cầu Bearer JWT (`authenticated`). Kiểm tra phân quyền theo bảng `user_gateways` (các vai trò `owner`, `operator`, `viewer` thuộc trạm đều được đọc; người dùng không thuộc trạm nhận `404 Not Found`).
- **Tham số truy vấn (Query Parameters):**
  - `attrs` (bắt buộc): `measurement` (alias `value`) hoặc `sample_interval_us`.
  - `timerel` (bắt buộc): `between`.
  - `timeAt` & `endTimeAt` (bắt buộc): Khoảng thời gian nửa mở $[timeAt, endTimeAt)$ theo định dạng UTC ISO 8601 / RFC 3339 microsecond kết thúc bằng `Z` (ví dụ `2026-10-01T00:00:00.000000Z`). Yêu cầu $timeAt < endTimeAt$ và khoảng cách $(endTimeAt - timeAt) \le 744$ giờ (31 ngày).
  - `aggrPeriodDuration` (tùy chọn): Kích hoạt chế độ aggregated values với 9 canonical durations: `PT0.1S`, `PT1S`, `PT10S`, `PT1M`, `PT5M`, `PT15M`, `PT1H`, `PT6H`, `P1D`.
  - `aggrMethods` (tùy chọn, mặc định `avg,min,max,totalCount`): Tập con các phương thức tổng hợp.
  - `format` (tùy chọn): `temporalValues` (cho raw) hoặc `aggregatedValues` (cho bucket).
- **Cấu trúc DTO phản hồi chính thức từ backend (`200 OK`, `application/ld+json; charset=utf-8`):**
  - Trong chế độ raw (`format=temporalValues`): Property `measurement` chứa mảng `values: [[val, time], ...]`.
  - Trong chế độ gom cụm (`format=aggregatedValues`): Property `measurement` chứa các mảng bộ ba `[kết_quả, bắt_đầu, kết_thúc]` cho `avg`, `min`, `max`, và `totalCount`. Trường `totalCount` là số nguyên JSON exact integer.

### 2.3. Định hướng Trải nghiệm Trực quan hóa Biểu đồ trên Client (Flutter UI Roadmap)
- **Tình trạng UI:** Phía backend đã sẵn sàng phục vụ dữ liệu, tuy nhiên màn hình biểu đồ lịch sử trên Flutter Web hiện **đang nằm trong lộ trình xây dựng giao diện** và chưa được xuất xưởng.
- **Kỹ thuật vẽ dải biên độ (Line with Min-Max Band):** Đối với dữ liệu dạng `bucket`, client vẽ đường liền cho giá trị trung bình (`avg`) và vùng mờ bao quanh giữa giá trị cực tiểu (`min`) và cực đại (`max`) giúp sinh viên dễ dàng quan sát được đỉnh nhọn bất thường mà không cần tải toàn bộ hàng triệu điểm thô.
- **Xử lý khoảng trống dữ liệu (Data Gaps):** Khi khoảng cách thời gian giữa các điểm vượt quá chu kỳ lấy mẫu dự kiến (trạm tắt nguồn hoặc mất mạng), client cần ngắt nét vẽ thay vì nối thẳng giả mạo dữ liệu.

---

## 3. Tính năng 2: Luồng Dữ liệu Thời gian thực (Realtime Streaming qua WebSocket — Dự kiến Giai đoạn 7)

### 3.1. Luồng kiến trúc tổng thể

Pipeline Giai đoạn 3 chịu trách nhiệm tiếp nhận telemetry từ trạm, ghi transaction vào TimescaleDB/PostgreSQL và gửi application commit ACK ngược lại cho trạm qua MQTT; giai đoạn này **chưa bao gồm** việc phát luồng dữ liệu thời gian thực (fan-out) tới Client Dashboard. Chức năng WebSocket telemetry có phân quyền được xếp vào **Giai đoạn 7** trong kế hoạch 9 giai đoạn của backend.

```text
Client Dashboard
    |  Bắt tay WebSocket (wss://) qua HTTPS Reverse Proxy
    v
Nginx Reverse Proxy
    |  Chuyển tiếp HTTP Upgrade giữ nguyên Headers
    v
Go Backend (/v1/ws)
    |-- 1. Xác thực định danh người dùng (Yêu cầu an ninh)
    |-- 2. Kiểm tra danh sách Gateway được ủy quyền (user_gateways)
    |-- 3. Đăng ký Client vào Hub quản lý kết nối
    v
Database Commit Event -> Worker Pool -> WebSocket Fan-Out -> Client Dashboard
```

### 3.2. Yêu cầu An ninh và Thách thức Bắt tay Xác thực trên Trình duyệt
- **Bắt buộc mã hóa `wss://`:** Khi ứng dụng Web chạy trên giao thức `https://`, kết nối WebSocket bắt buộc phải dùng `wss://` để tránh lỗi bảo mật hỗn hợp (Mixed Content) của trình duyệt.
- **Vấn đề xác thực trên chuẩn WebSocket của Trình duyệt:**
  - Đối với ứng dụng chạy trên trình duyệt Web, đối tượng chuẩn `window.WebSocket` **không hỗ trợ đính kèm header tùy biến** (như `Authorization: Bearer <token>`) trong yêu cầu bắt tay HTTP Upgrade ban đầu.
  - Một số giải pháp tình thế là truyền token trong query parameter (ví dụ `?token=<jwt>`). Tuy nhiên, phương thức này mang rủi ro: Access token có thể bị ghi lại trong nhật ký truy cập (access logs) của proxy, lịch sử trình duyệt và hiển thị trên URL.
  - **Quy tắc an ninh bắt buộc:** Không bao giờ ghi log chứa token định danh. Cơ chế bắt tay an toàn cho WebSocket (chẳng hạn như cơ chế vé xác thực tạm thời - ticket-based handshake, cookie bảo mật, hoặc gửi bản tin xác thực đầu tiên sau khi mở kênh) hiện vẫn là **vấn đề thiết kế chưa chốt (unresolved secure handshake design)** cho backend. Ý tưởng truyền JWT qua query string chưa được phê duyệt làm hợp đồng chính thức.
- **Hiện trạng route:** Route `GET /v1/ws` hiện được đăng ký dạng giữ chỗ (`501 Not Implemented`) sau middleware xác thực token (`auth.AuthenticationMiddleware`). Yêu cầu thiếu Bearer token hợp lệ sẽ nhận mã lỗi `401 Unauthorized`; chỉ khi có Bearer token hợp lệ mới nhận phản hồi `501 Not Implemented`. Client chưa thể kết nối WebSocket ở giai đoạn này.
- **Nguyên tắc phân phối dữ liệu:** Chỉ những bản tin đo đạc đã **commit thành công vào cơ sở dữ liệu PostgreSQL/TimescaleDB** mới được đẩy qua luồng realtime đến các client được phân quyền truy cập trạm tương ứng. Mọi cấu trúc khung tin và sự kiện streaming đều mang tính phác thảo định hướng dự kiến (provisional).

### 3.3. Định hướng Xử lý Tải phía Client
- **Điều tiết hiển thị (Throttling / Buffering):** Khi cảm biến gửi dữ liệu tần số cao, client không vẽ trực tiếp từng điểm mà đệm vào hàng đợi và cập nhật biểu đồ theo chu kỳ hiển thị phù hợp (ví dụ 30–60 FPS) để bảo toàn tài nguyên CPU/GPU.
- **Xử lý nền:** Khi ứng dụng bị đưa xuống nền hoặc tab trình duyệt mất tiêu điểm, client cần giới hạn kích thước hàng đợi đệm để tránh hiện tượng dồn ứ hàng nghìn điểm đo gây đơ giao diện khi người dùng kích hoạt lại.

---

## 4. Tính năng 3: Xem Hình ảnh Lưu trữ (Media Viewer qua Supabase Storage — Dự kiến Giai đoạn 8)

### 4.1. Nguyên tắc An toàn Lưu trữ Đa phương tiện
- **Private Bucket tuyệt đối:** Tất cả tệp hình ảnh được lưu trữ trong bucket riêng tư (`private`) của Supabase Storage. Tuyệt đối không mở Public Bucket (`AGENTS.md` Mục 13).
- **Phân tách luồng dữ liệu:** MQTT chỉ sử dụng cho dữ liệu đo đạc và siêu dữ liệu điều khiển; tệp media nhị phân tuyệt đối không truyền qua MQTT. Tính năng tích hợp tệp ảnh riêng tư được xếp vào **Giai đoạn 8** của kế hoạch backend.

### 4.2. Luồng Vận hành 5 bước Định hướng (End-to-End Media Flow)
1. **Gateway tải ảnh lên:** Gateway chụp ảnh, yêu cầu Signed Upload URL từ Go Backend, sau đó tải thẳng tệp nhị phân lên Supabase Storage qua Nginx và Supabase API Gateway.
2. **Client đọc danh mục ảnh:** Client gửi yêu cầu lấy danh sách siêu dữ liệu ảnh của trạm được ủy quyền (route dự kiến `GET /v1/gateways/{gateway_id}/media`).
3. **Client xin Signed Read URL:** Client gửi yêu cầu lấy đường dẫn đọc có chữ ký ngắn hạn (route dự kiến `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`).
4. **Backend thẩm định quyền:** Go Backend kiểm tra quyền trong bảng `user_gateways` trước khi sinh Signed Read URL có thời hạn ngắn (ví dụ 5–15 phút).
5. **Client hiển thị ảnh:** Client sử dụng Signed Read URL để tải ảnh hiển thị.

> **Hiện trạng thực tế:** Các route phục vụ danh mục media (như `GET /v1/gateways/{gateway_id}/media`) và cấp Signed Read URL (`POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`) hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin của Go Backend và sẽ trả về mã lỗi `404 Not Found` từ `router.NoRoute`. Mọi cấu trúc API và payload media đều mang tính phác thảo định hướng dự kiến (provisional).

---

## 5. Tính năng 4: Tương tác Digital Twin và Điều khiển Thiết bị (Downlink Commands — Dự kiến Giai đoạn 5 & 6)

### 5.1. Khái niệm cốt lõi: Phân biệt `reported_state`, `desired_state`, Đọc State và Chỉnh sửa Metadata

Để tránh hiểu nhầm nghiêm trọng trong quá trình phát triển hệ thống IoT, cần nắm vững 4 khái niệm riêng biệt:
1. **`reported_state` (Trạng thái thực tế):** Phản ánh những gì phần cứng Gateway thực tế đã đo đạc hoặc xác nhận thông qua MQTT. Backend cập nhật trường này khi nhận telemetry hoặc kết quả thực thi lệnh (`command_result`) từ Gateway.
2. **`desired_state` (Cấu hình mục tiêu lâu dài):** Cấu hình mong muốn do người dùng có thẩm quyền yêu cầu thiết lập (ví dụ `sampling_interval_seconds = 5`). Cập nhật cấu hình này sẽ sinh ra lệnh gửi xuống thiết bị nếu có sự khác biệt so với trạng thái thực tế.
3. **Đọc trạng thái Digital Twin (`GET /v1/digital-twins/{entity_id}?format=simplified`):** **ĐÃ ĐƯỢC HIỆN THỰC HÓA VÀ KIỂM CHỨNG TẠI STAGE 4 BACKEND**. Endpoint này cho phép người dùng có quyền đọc xem nội dung hiện tại của cả `reported_state` và `desired_state` qua project extension đọc trực tiếp `twin_states` (thay thế route cũ `/{id}/state` đã thu hồi 404). **Lưu ý quan trọng: Quyền đọc cấu hình mong muốn (`desired_state`) hoàn toàn KHÔNG ĐỒNG NGHĨA với quyền ghi hay sửa đổi cấu hình đó.**
4. **Chỉnh sửa siêu dữ liệu (`PATCH /v1/digital-twins/{entity_id}/attrs`):** **ĐÃ ĐƯỢC HIỆN THỰC HÓA VÀ KIỂM CHỨNG TẠI STAGE 4 BACKEND**. Thao tác này chỉ cho phép sửa thông tin mô tả hành chính qua Property fragment (`name`, `description` của Gateway bởi Admin; `name` của Sensor bởi Owner; xóa mô tả Gateway qua `urn:ngsi-ld:null`). Đường dẫn gốc `PATCH /v1/digital-twins/{entity_id}` nhận `405 Method Not Allowed`. **Thao tác này tuyệt đối KHÔNG PHẢI là lệnh điều khiển thiết bị (device command)**, không gửi bất kỳ bản tin nào qua MQTT xuống trạm và không làm thay đổi trạng thái hoạt động của thiết bị vật lý.

### 5.2. Mô hình Phân quyền và Ranh giới An toàn Điều khiển Thiết bị
Theo quy định tại `AGENTS.md` (Mục 6, 6.5, 10, 18):
- **Trạng thái hiện tại đối với thao tác điều khiển thiết bị (Fail-closed):**
  - Toàn bộ các thao tác ghi cấu hình mong muốn (`desired_state`) và gửi lệnh điều khiển thiết bị hiện **chưa được kích hoạt (fail-closed)** trên backend.
  - Vai trò `operator` tại thời điểm hiện tại vẫn được đối xử nghiêm ngặt như **chế độ chỉ đọc (read-only)**. Việc Giai đoạn 4 hoàn thành các API đọc và chỉnh sửa siêu dữ liệu thực thể **tuyệt đối không mở bất kỳ quyền gửi lệnh điều khiển nào cho `operator` hay `owner`**.
- **Phân bổ lộ trình Giai đoạn 5 & 6 của Backend:**
  - **Giai đoạn 5 (Lát cắt Điều khiển Digital Twin - Downlink Control Slice):** Triển khai thao tác ghi cấu hình mong muốn (`PATCH /v1/digital-twins/{entity_id}/desired-state`), sinh lệnh (`twin_commands`), lưu hàng đợi xuất bản transactional outbox (`twin_outbox`) trong cùng một database transaction, worker xuất bản lệnh qua MQTT tới Gateway, tiếp nhận kết quả thực thi `command_result`, cập nhật `reported_state`, và hỗ trợ tra cứu trạng thái lệnh qua `GET /v1/commands/{command_id}`.
  - **Giai đoạn 6 (Reconciliation & Recovery):** Hoàn thiện vòng đời lệnh đầy đủ (`pending -> published -> acknowledged -> succeeded/failed/timeout`), cơ chế timeout, retry backoff có jitter, và đối chiếu hội tụ desired/reported state khi kết nối lại sau sự cố.
- **Chính sách phân quyền mục tiêu khi Giai đoạn 5 & 6 hoàn thành:**
  - `owner`: Có quyền đọc đầy đủ; có quyền quản lý cấu hình lâu dài (`desired_state`) và siêu dữ liệu cảm biến thuộc trạm của mình; có thể gửi các lệnh tác vụ an toàn nằm trong danh sách cho phép (allowlist) của server.
  - `operator`: Có quyền đọc dữ liệu trạm được phân công; chỉ được gửi các lệnh tác vụ vận hành an toàn nằm trong danh sách kiểm duyệt nghiêm ngặt của server (ví dụ thao tác một lần như `capture-image` nếu được hiện thực hóa và kiểm thử); **tuyệt đối bị từ chối** quyền sửa đổi cấu hình thiết bị lâu dài (`desired_state`), sửa đổi cảm biến, thay đổi quyền sở hữu hoặc quản lý chứng thực.
  - `viewer`: Hoàn toàn là quyền chỉ đọc; mọi yêu cầu ghi, cấu hình hay điều khiển đều bị từ chối.
- **Ranh giới an toàn nghiêm ngặt:**
  - Tuyệt đối cấm các lệnh tùy tiện hoặc không an toàn (như lệnh shell thô, payload tùy ý, hoặc khởi động lại không an toàn / unapproved reboot).
  - Không hỗ trợ chính sách điều khiển tự động bằng mô hình ngôn ngữ lớn (No autonomous LLM control).
  - **Client không bao giờ xuất bản trực tiếp lên MQTT Broker:** Mọi lệnh điều khiển bắt buộc phải đi qua Go REST API, được ghi nhận đồng thời vào bảng `twin_commands` và `twin_outbox` trong một transaction cơ sở dữ liệu trước khi worker xuất bản sang Mosquitto.

### 5.3. Quy tắc Bất khả xâm phạm về Phản hồi Lệnh
- Khi gửi yêu cầu lệnh thành công qua API tại Giai đoạn 5, Backend sẽ trả về mã `202 Accepted` kèm `command_id` và trạng thái `pending`.
- **Mã `202 Accepted` CHỈ CÓ NGHĨA LÀ LỆNH ĐÃ ĐƯỢC GHI NHẬN HỢP LỆ VÀ ĐƯA VÀO HÀNG ĐỢI TRANSACTIONAL OUTBOX, KHÔNG CÓ NGHĨA LÀ THIẾT BỊ VẬT LÝ ĐÃ THỰC THI THÀNH CÔNG.**
- Lệnh chỉ được coi là thành công khi Gateway nhận lệnh, thực thi và gửi bản tin kết quả xác nhận qua MQTT, sau đó Go backend cập nhật trạng thái lệnh sang `succeeded` và đồng bộ `reported_state`.
- Client hiển thị trạng thái đang xử lý và chỉ chuyển sang thành công khi nhận được sự kiện cập nhật tương ứng.

> **Hiện trạng thực tế:** Toàn bộ các route ghi cấu hình mong muốn (`PATCH /v1/digital-twins/{entity_id}/desired-state`), phát lệnh (`POST /v1/digital-twins/{entity_id}/commands`), tra cứu danh sách lệnh (`GET /v1/digital-twins/{entity_id}/commands`) và tra cứu chi tiết lệnh (`GET /v1/commands/{command_id}`) hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin và sẽ nhận phản hồi `404 Not Found` từ `router.NoRoute`. Mọi cấu trúc JSON payload và định dạng lệnh điều khiển trong các tài liệu phác thảo đều mang tính chất định hướng dự kiến (provisional).

---

## 6. Khung Tổ chức Mã nguồn Tham khảo Định hướng trên Client

Cấu trúc thư mục dưới đây là **mô hình tham khảo định hướng phân chia tính năng (Feature-First Reference Layout)**, nhằm hỗ trợ sinh viên và kỹ sư phát triển hình dung việc tổ chức mã nguồn Flutter khi tích hợp các tính năng tương lai. Đây **không phải là bằng chứng cho thấy các thành phần giao diện này đã được hiện thực hóa** trên client:

```text
lib/
├── features/
│   ├── auth/                      # Module xác thực Supabase Auth & phiên làm việc
│   ├── gateways/                  # Module danh sách Gateway/Sensor và quản lý cơ bản
│   ├── telemetry/                 # Tích hợp History Charts (Backend Stage 4 đã sẵn sàng API)
│   │   │                          # & Realtime WebSocket (Định hướng Giai đoạn 7)
│   │   ├── bloc/
│   │   │   ├── telemetry_history_cubit.dart
│   │   │   └── telemetry_realtime_bloc.dart
│   │   ├── models/
│   │   │   ├── telemetry_point.dart
│   │   │   └── history_query_params.dart
│   │   ├── repositories/
│   │   │   └── telemetry_repository.dart
│   │   └── widgets/
│   │       ├── time_series_chart.dart
│   │       └── chart_range_selector.dart
│   ├── media/                     # Khung định hướng Giai đoạn 8 (Supabase Storage Image Viewer)
│   │   ├── cubit/
│   │   ├── models/
│   │   ├── repositories/
│   │   └── widgets/
│   │       ├── media_grid.dart
│   │       └── lightbox_dialog.dart
│   └── digital_twin/              # Tích hợp Twin List/Detail/State/History/Metadata (Backend Stage 4)
│                                  # & Khung điều khiển Downlink Commands (Định hướng Giai đoạn 5-6)
│       ├── cubit/
│       ├── models/
│       ├── repositories/
│       └── widgets/
│           ├── twin_state_card.dart
│           └── control_panel.dart
```

### Lưu ý khi triển khai:
- Để biết chi tiết hợp đồng và quy chuẩn tích hợp 7 endpoint Digital Twin & History đã hoạt động ở Stage 4, hãy tham khảo tài liệu chính thức:
  - [`docs/client/04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md) (Quy chuẩn chi tiết từng endpoint, query, body, DTO).
  - [`docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md) (Xử lý trạng thái tải, ranh giới lỗi, phân trang keyset cursor).
  - [`docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md) (Hợp đồng quản trị hạ tầng trạm và chứng thực MQTT).
- Dự án không yêu cầu xây dựng ứng dụng quản trị riêng biệt; việc quản trị tài khoản người dùng và trạm được thực hiện qua Supabase Studio và các công cụ quản trị chuyên dụng của Platform Admin.
- Nền tảng ứng dụng client mục tiêu được lựa chọn theo `AGENTS.md` (Mục 3 và 17) là Flutter Web cho bản MVP; các ứng dụng di động (mobile) nằm ngoài phạm vi đồ án hiện tại và được hoãn lại (deferred). Kiến trúc API và cấu trúc dữ liệu client được thiết kế để có thể tái sử dụng cho ứng dụng di động sau này nếu được kích hoạt lại; không ưu tiên Android hay native-mobile trong phạm vi đồ án hiện hành.
