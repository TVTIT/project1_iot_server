# Tài liệu Client — 05: Lộ trình và Thiết kế Định hướng Tính năng Mới trên Client Dashboard (Upcoming Features Roadmap)

**Cập nhật:** 2026-10-16
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 2, 3, 4, 5.6, 6, 7, 10, 12, 13, 15, 16, 17), `../docs/backend_plan/Backend_summary_plan.md` (Kế hoạch 9 giai đoạn), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/03_USER_GATEWAY_AUTHORIZATION.md`, `docs/client/04_REST_API_CLIENT_CONTRACT.md`, `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`, `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`.

---

## 1. Mục đích tài liệu và Định hướng Thiết kế Đón đầu (Forward-Looking Architecture)

### 1.1. Bối cảnh hệ thống và Hiện trạng API tại Go Backend

Tại thời điểm hiện tại:
- **Giai đoạn 2 (Xác thực, phân quyền và quản lý trạm cơ bản):** Đã hoàn thành nền tảng định danh tập trung qua Supabase Auth, kiểm tra JWT nội bộ kết hợp phân quyền quan hệ `user_gateways` và quyền quản trị `platform_admins` trong PostgreSQL. Hai REST API danh mục (`GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors`) cùng thao tác khởi tạo cảm biến cho Gateway Owner (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`) đã hoạt động đầy đủ với quy tắc fail-closed chặt chẽ cho `operator` và `viewer`. Tầng quản trị hạ tầng MQTT Dynamic Security (cấp, xoay vòng, thu hồi credential và reset có giám sát) đã được thiết lập phía backend.
- **Giai đoạn 3 (Uplink hoàn chỉnh: MQTT → Ingress → Lưu DB → Commit ACK):** Đã hoàn thiện pipeline thu thập dữ liệu từ trạm: Gateway simulator lưu SQLite outbox trước khi phát qua MQTT TLS, Go subscriber nhận qua hàng đợi có giới hạn và fixed worker pool, ghi đồng thời vào PostgreSQL transaction (raw telemetry, dedup ledger `processed_messages`, cập nhật `reported_state` cảm biến và bản ghi temporal `sample_interval_us` trong `twin_temporal_values`), sau đó chỉ gửi application commit ACK khi giao dịch đã commit thành công.
- **Lưu ý cốt lõi về ranh giới dữ liệu:** Dữ liệu đo đạc đã được ghi nhận và lưu trữ trong cơ sở dữ liệu TimescaleDB/PostgreSQL tại Giai đoạn 3 **hoàn toàn không đồng nghĩa** với việc các API đọc dữ liệu lịch sử, API Digital Twin, luồng streaming WebSocket, tệp media hay lệnh điều khiển đã khả dụng cho client. Các chức năng đọc và tương tác này được phân bổ vào các giai đoạn kế tiếp theo đúng lộ trình 9 giai đoạn của backend.

**Hiện trạng thực tế các endpoint tính năng tương lai tại Go Backend (`src/internal/httpserver/router.go`):**
1. **Endpoint đăng ký dạng giữ chỗ (`501 Not Implemented`):** Chỉ có đúng ba (03) route sau được đăng ký trong nhóm route yêu cầu xác thực (`authenticated` - tiền tố `/v1` với middleware `auth.AuthenticationMiddleware`):
   - `GET /v1/telemetry/history`
   - `GET /v1/ws`
   - `GET /v1/digital-twins`

   *Quy tắc xác thực:* Các route này được bọc bởi `auth.AuthenticationMiddleware(deps.TokenVerifier)`. Do đó:
   - Yêu cầu **không có Bearer token hợp lệ** sẽ bị từ chối ngay với mã lỗi `401 Unauthorized` (`{"error":{"code":"unauthorized","message":"authentication required"}}`) trước khi tiếp cận handler `501`.
   - Chỉ khi gửi kèm **Bearer token hợp lệ** (JWT ký đúng bởi Supabase với role `authenticated`), server mới trả về mã lỗi `501 Not Implemented` (`{"error":{"code":"not_implemented","message":"endpoint not implemented"}}`).
2. **Endpoint chưa đăng ký (`404 Not Found`):** Tất cả các endpoint khác được đề cập trong tài liệu này hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin của backend. Khi client gọi đến, Gin sẽ xử lý qua `router.NoRoute` và trả về mã lỗi `404 Not Found` (`{"error":{"code":"not_found","message":"resource not found"}}`), bất kể client có gửi token hay không. Các route chưa đăng ký bao gồm:
   - Đa phương tiện / Media: `GET /v1/gateways/{gateway_id}/media`, `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url` (hoặc các route media khác).
   - Chi tiết thực thể Digital Twin: `GET /v1/digital-twins/{entity_id}`, `PATCH /v1/digital-twins/{entity_id}`.
   - Trạng thái và lịch sử Digital Twin: `GET /v1/digital-twins/{entity_id}/state`, `GET /v1/digital-twins/{entity_id}/history`.
   - Cập nhật cấu hình mong muốn: `PATCH /v1/digital-twins/{entity_id}/desired-state`.
   - Quản lý lệnh điều khiển: `POST /v1/digital-twins/{entity_id}/commands`, `GET /v1/digital-twins/{entity_id}/commands`, `GET /v1/commands/{command_id}`.

### 1.2. Bản chất tài liệu: Thiết kế định hướng, không phải hợp đồng cam kết

Tài liệu này cung cấp **Bản thiết kế kỹ thuật định hướng (Provisional / Forward-Looking Design)** nhằm mục đích:
1. **Định hình luồng nghiệp vụ tương lai:** Mô tả trước tư duy kiến trúc chuỗi thời gian lịch sử, luồng streaming dữ liệu đã commit, lưu trữ tệp ảnh riêng tư và mô hình Digital Twin reported/desired state.
2. **Giới hạn phạm vi và tính chất dự kiến:** Mọi định dạng JSON payload, tham số truy vấn (query parameters), bộ lọc, công thức gom mẫu TimescaleDB (`time_bucket`), cơ chế bắt tay WebSocket và cấu trúc sự kiện trình bày dưới đây đều là **đề xuất kỹ thuật dự kiến (provisional)**, chưa phải là hợp đồng backend chính thức (not backend contract).
3. **Không cam kết "Zero-Refactor" hay chuyển đổi tức thì:** Phía giao diện Client **tuyệt đối không gọi hay phụ thuộc cứng** vào các định dạng dự kiến này khi Backend chưa chính thức triển khai và kiểm thử. Việc tích hợp thực tế khi bước sang các giai đoạn tiếp theo sẽ cần điều chỉnh tương ứng với hợp đồng chính thức, không có bảo đảm chuyển giao mà không chỉnh sửa mã nguồn. Các thành phần giao diện cho những tính năng này hiện chưa được hiện thực hóa trên client.
4. **Giữ vững kiến trúc đã chọn:** Tuân thủ kiến trúc Go modular monolith, REST API cho request/response, WebSocket cho dữ liệu realtime, không đưa gRPC hay Context Broker ngoài (như Scorpio/Orion-LD) vào phạm vi dự án.

```text
+---------------------------------------------------------------------------------------------------+
|               LỘ TRÌNH ĐỊNH HƯỚNG TÍNH NĂNG CLIENT THEO 9 GIAI ĐOẠN BACKEND                       |
+---------------------------------------------------------------------------------------------------+
| Giai đoạn 1 (Xong):       | Nền tảng Go, schema baseline, hợp đồng giao thức                      |
| Giai đoạn 2 (Xong):       | Đăng nhập, Phân quyền user_gateways, Danh mục Trạm/Cảm biến           |
|                           | Quản trị Hạ tầng Trạm/Cảm biến/MQTT (Chi tiết doc 07)                 |
| Giai đoạn 3 (Uplink):     | Pipeline Uplink: Gateway Outbox -> MQTT TLS -> Ingress -> DB Commit   |
|                           | Lưu trữ raw telemetry, ledger, reported state & temporal interval     |
|                           | (Chưa mở API đọc client, WebSocket hay lệnh điều khiển)               |
| Giai đoạn 4 (Định hướng): | API Lịch sử & Đọc Digital Twin: GET /v1/telemetry/history,            |
|                           | GET /v1/digital-twins, {entity_id}, /state, /history                  |
| Giai đoạn 5 (Định hướng): | Lát cắt Điều khiển Digital Twin: PATCH desired-state, Commands outbox, |
|                           | MQTT execution, Gateway result correlation -> GET command status      |
| Giai đoạn 6 (Định hướng): | Timeout, Retry & Reconciliation: Vòng đời lệnh đầy đủ, retry backoff, |
|                           | đối chiếu hội tụ desired/reported state qua gián đoạn / reconnect     |
| Giai đoạn 7 (Định hướng): | WebSocket Telemetry có phân quyền: GET /v1/ws, push event sau DB      |
|                           | commit, fan-out theo quyền truy cập user_gateways                     |
| Giai đoạn 8 (Định hướng): | Media Ảnh qua Private Supabase Storage: Signed Upload/Read URL,        |
|                           | kiểm tra quyền Gateway/User, bucket private tuyệt đối                 |
| Giai đoạn 9 (Định hướng): | Nghiệm thu toàn diện, Đo tải & Bàn giao: E2E, backup/restore thử,      |
|                           | hoàn thiện tài liệu tích hợp Flutter Client                           |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Tính năng 1: Biểu đồ Chuỗi thời gian Lịch sử (Historical Time-Series Charts - Dự kiến Giai đoạn 4)

### 2.1. Rào cản Dữ liệu Lớn và Nguyên lý Gom mẫu Thích ứng
- Cảm biến tần số cao trong hệ thống có thể thu thập dữ liệu với tốc độ lên tới **100 Hz** (100 mẫu/giây = 8.640.000 mẫu/ngày cho mỗi kênh đo).
- Giai đoạn 3 của backend đã hoàn thiện việc tiếp nhận và lưu trữ toàn bộ các mẫu thô này vào TimescaleDB hypertable (`telemetry`), ghi nhận bản ghi temporal `sample_interval_us` trong `twin_temporal_values`, và cập nhật `reported_state` cảm biến.
- Tuy nhiên, việc **truy vấn đọc dữ liệu lịch sử** phục vụ hiển thị trên client thuộc phạm vi **Giai đoạn 4 (API lịch sử và đọc Digital Twin)**. Nếu Client yêu cầu truy vấn khoảng thời gian dài và Backend trả về dữ liệu thô:
  - Băng thông mạng bị chiếm dụng lớn (hàng trăm megabyte JSON).
  - Trình duyệt/Ứng dụng Client sẽ bị quá tải bộ nhớ và tụt khung hình nghiêm trọng khi cố render hàng triệu điểm vector.
- **Định hướng kỹ thuật (Giai đoạn 4):** Backend sẽ sử dụng hàm `time_bucket()` của **TimescaleDB** để tính toán gộp thích ứng (Adaptive Downsampling). Phía Client sẽ nhận dữ liệu đã được tổng hợp theo các khoảng thời gian đều nhau, khống chế số lượng điểm trả về trong khoảng tối ưu: **từ 500 đến 1.000 điểm** cho một biểu đồ hiển thị. Toàn bộ mẫu thô vẫn được lưu trữ đầy đủ trong TimescaleDB để phục vụ phân tích chuyên sâu.

### 2.2. Đề xuất Kỹ thuật Dự kiến (`GET /v1/telemetry/history`)

> **Cảnh báo trạng thái API:** Route `GET /v1/telemetry/history` hiện được đăng ký dạng giữ chỗ (`501 Not Implemented`) sau middleware xác thực token (`auth.AuthenticationMiddleware`). Yêu cầu thiếu Bearer token hợp lệ sẽ nhận mã lỗi `401 Unauthorized` trước khi tới handler; chỉ khi có token hợp lệ mới nhận phản hồi `501 Not Implemented`. Cấu trúc tham số và phản hồi dưới đây là **bản phác thảo định hướng dự kiến (provisional)** cho Giai đoạn 4, không phải hợp đồng API đã kích hoạt. Không tích hợp mã giao diện vào endpoint này ở giai đoạn hiện tại.

- **Phương thức:** `GET`
- **Đường dẫn:** `/v1/telemetry/history`
- **Headers dự kiến:**
  ```http
  Authorization: Bearer <access_token>
  Accept: application/json
  X-Request-ID: <uuidv4>
  ```
- **Tham số Query Parameters dự kiến:**

| Tham số | Kiểu | Bắt buộc | Định dạng / Ví dụ | Mô tả định hướng |
|---|---|:---:|---|---|
| `gateway_id` | `string` | **Có** | `gateway_001` | Mã định danh trạm Gateway cần truy vấn. |
| `sensor_id` | `string` | **Có** | `sensor_001` | Mã định danh cảm biến cần truy vấn. |
| `from` | `string` | **Có** | `2026-10-01T00:00:00Z` | Mốc thời gian bắt đầu theo chuẩn ISO 8601 UTC. |
| `to` | `string` | **Có** | `2026-10-02T00:00:00Z` | Mốc thời gian kết thúc theo chuẩn ISO 8601 UTC. |
| `resolution` | `string` | Không | `1m`, `5m`, `1h`, `auto` | Độ phân giải khung gom thời gian dự kiến (mặc định `auto`). |

- **Định dạng phản hồi dự kiến (`200 OK` - Phác thảo):**

```json
{
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "unit": "°C",
  "from": "2026-10-01T00:00:00Z",
  "to": "2026-10-02T00:00:00Z",
  "resolution_applied": "2m",
  "total_points": 720,
  "points": [
    {
      "timestamp": "2026-10-01T00:00:00Z",
      "avg": 25.42,
      "min": 24.80,
      "max": 26.15,
      "sample_count": 12000
    },
    {
      "timestamp": "2026-10-01T00:02:00Z",
      "avg": 25.48,
      "min": 24.85,
      "max": 26.10,
      "sample_count": 12000
    }
  ]
}
```

### 2.3. Định hướng Trải nghiệm Trực quan hóa Biểu đồ
- **Kỹ thuật đường trung bình kết hợp dải biên độ (Line with Min-Max Band):** Vẽ đường liền cho giá trị trung bình (`avg`) và vùng mờ bao quanh giữa giá trị cực tiểu (`min`) và cực đại (`max`) giúp quan sát được đỉnh nhọn bất thường mà không cần vẽ toàn bộ dữ liệu thô.
- **Xử lý khoảng trống dữ liệu (Data Gaps):** Ngắt nét vẽ nếu khoảng cách thời gian vượt quá chu kỳ lấy mẫu dự kiến để tránh nối giả mạo dữ liệu khi trạm mất kết nối.
- **Công cụ trực quan hóa:** Không bắt buộc phụ thuộc sớm vào một thư viện biểu đồ cụ thể; việc chọn lựa gói biểu đồ sẽ được quyết định tại thời điểm hiện thực hóa tính năng dựa trên hiệu năng hiển thị thực tế trên nền tảng mục tiêu.

---

## 3. Tính năng 2: Luồng Dữ liệu Thời gian thực (Realtime Streaming qua WebSocket - Dự kiến Giai đoạn 7)

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

### 3.2. Yêu cầu An ninh và Vấn đề Bắt tay Xác thực Trình duyệt
- **Bắt buộc mã hóa `wss://`:** Khi ứng dụng Web chạy trên giao thức `https://`, kết nối WebSocket bắt buộc phải dùng `wss://` để tránh lỗi bảo mật hỗn hợp (Mixed Content) của trình duyệt.
- **Vấn đề xác thực trên chuẩn WebSocket của Trình duyệt:**
  - Đối với ứng dụng chạy trên trình duyệt Web, đối tượng chuẩn `window.WebSocket` của trình duyệt **không hỗ trợ đính kèm header tùy biến** (như `Authorization: Bearer <token>`) trong yêu cầu bắt tay HTTP Upgrade ban đầu.
  - Một số giải pháp tình thế thường gặp là truyền token trong query parameter (ví dụ `?token=<jwt>`). Tuy nhiên, phương thức này mang rủi ro bảo mật nghiêm trọng: Access token có thể bị ghi lại trong nhật ký truy cập (access logs) của proxy, lịch sử trình duyệt và hiển thị trên URL.
  - **Quy tắc an ninh bắt buộc:** Không bao giờ ghi log chứa token định danh. Cơ chế bắt tay an toàn cho WebSocket (chẳng hạn như cơ chế vé xác thực tạm thời - ticket-based handshake, cookie bảo mật, hoặc gửi bản tin xác thực đầu tiên sau khi mở kênh) hiện vẫn là **vấn đề thiết kế chưa chốt (unresolved secure handshake design)** cho backend. Ý tưởng truyền JWT qua query string chưa được phê duyệt làm hợp đồng chính thức.
- **Hiện trạng route:** Route `GET /v1/ws` hiện được đăng ký dạng giữ chỗ (`501 Not Implemented`) sau middleware xác thực token (`auth.AuthenticationMiddleware`). Yêu cầu thiếu Bearer token hợp lệ sẽ nhận mã lỗi `401 Unauthorized`; chỉ khi có Bearer token hợp lệ mới nhận phản hồi `501 Not Implemented`. Client chưa thể kết nối WebSocket ở giai đoạn này.
- **Nguyên tắc phân phối dữ liệu:** Chỉ những bản tin đo đạc đã **commit thành công vào cơ sở dữ liệu PostgreSQL/TimescaleDB** mới được đẩy qua luồng realtime đến các client được phân quyền truy cập trạm tương ứng. Mọi cấu trúc khung tin và sự kiện streaming đều mang tính phác thảo định hướng dự kiến (provisional).

### 3.3. Định hướng Xử lý Tải phía Client
- **Điều tiết hiển thị (Throttling / Buffering):** Khi cảm biến gửi dữ liệu tần số cao, client không vẽ trực tiếp từng điểm mà đệm vào hàng đợi và cập nhật biểu đồ theo chu kỳ hiển thị phù hợp (ví dụ 30–60 FPS) để bảo toàn tài nguyên CPU/GPU.
- **Xử lý nền:** Khi ứng dụng bị đưa xuống nền hoặc tab trình duyệt mất tiêu điểm, client cần giới hạn kích thước hàng đợi đệm để tránh hiện tượng dồn ứ hàng nghìn điểm đo gây đơ giao diện khi người dùng kích hoạt lại.

---

## 4. Tính năng 3: Xem Hình ảnh Lưu trữ (Media Viewer qua Supabase Storage - Dự kiến Giai đoạn 8)

### 4.1. Nguyên tắc An toàn Lưu trữ Đa phương tiện
- **Private Bucket tuyệt đối:** Tất cả tệp hình ảnh được lưu trữ trong bucket riêng tư (`private`) của Supabase Storage. Tuyệt đối không mở Public Bucket (`AGENTS.md` Mục 13).
- **Phân tách luồng dữ liệu:** MQTT chỉ sử dụng cho dữ liệu đo đạc và siêu dữ liệu điều khiển; tệp media nhị phân tuyệt đối không truyền qua MQTT. Tính năng tích hợp tệp ảnh riêng tư được xếp vào **Giai đoạn 8** của kế hoạch backend.

### 4.2. Luồng Vận hành 5 bước Định hướng (End-to-End Media Flow)
1. **Gateway tải ảnh lên:** Gateway chụp ảnh, yêu cầu Signed Upload URL từ Go Backend, sau đó tải thẳng tệp nhị phân lên Supabase Storage qua Nginx và Supabase API Gateway.
2. **Client đọc danh mục ảnh:** Client gửi yêu cầu lấy danh sách siêu dữ liệu ảnh của trạm được ủy quyền (route dự kiến `GET /v1/gateways/{gateway_id}/media`).
3. **Client xin Signed Read URL:** Client gửi yêu cầu lấy đường dẫn đọc có chữ ký ngắn hạn (route dự kiến `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`).
4. **Backend thẩm định quyền:** Go Backend kiểm tra quyền trong bảng `user_gateways` trước khi sinh Signed Read URL có thời hạn ngắn (ví dụ 5–15 phút).
5. **Client hiển thị ảnh:** Client sử dụng Signed Read URL để tải ảnh hiển thị.

> **Hiện trạng thực tế:** Các route phục vụ danh mục media (như `GET /v1/gateways/{gateway_id}/media`) và cấp Signed Read URL (`POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`) hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin của Go Backend và sẽ trả về mã lỗi `404 Not Found` từ `router.NoRoute`. Mọi cấu trúc API và payload dưới đây đều mang tính phác thảo định hướng dự kiến (provisional).

---

## 5. Tính năng 4: Tương tác Digital Twin và Gửi lệnh Điều khiển (Digital Twin Downlink Commands - Dự kiến Giai đoạn 4, 5 & 6)

### 5.1. Khái niệm cốt lõi: `reported_state` vs `desired_state`
- `reported_state`: Trạng thái thực tế do phần cứng Gateway báo cáo lên thông qua MQTT và được Go backend xác thực, ghi nhận vào PostgreSQL (đã có bước khởi đầu tại Giai đoạn 3 khi cập nhật trạng thái báo cáo của cảm biến). Việc đọc `reported_state` được mở tại **Giai đoạn 4**.
- `desired_state`: Cấu hình mục tiêu do người dùng có thẩm quyền yêu cầu thiết lập thông qua Go API (được triển khai tại **Giai đoạn 5**).
- Cập nhật `desired_state` chỉ sinh ra lệnh điều khiển khi có sự khác biệt có thể thực thi so với `reported_state`.
- Telemetry và dữ liệu tải lên từ Gateway chỉ cập nhật `reported_state`, tuyệt đối không được ghi đè lên `desired_state`.

### 5.2. Mô hình Phân quyền và Ranh giới An toàn Điều khiển Thiết bị
Theo quy định tại `AGENTS.md` (Mục 6 và 10):
- **Trạng thái hiện tại (Stage 2/3 boundary - Fail-closed):** Cho đến khi nhánh tính năng điều khiển Digital Twin được hoàn thiện đầy đủ với kiểm tra phân quyền theo từng tác vụ, schema kiểm tra tính hợp lệ và bộ kiểm thử hoàn chỉnh, toàn bộ các thao tác ghi và gửi lệnh điều khiển đều **đóng chặt (fail-closed)**. Quyền `operator` được đối xử như chế độ chỉ đọc.
- **Phân bổ theo giai đoạn backend:**
  - **Giai đoạn 4 (Đọc Twin):** Client chỉ đọc thực thể, trạng thái `reported_state` và lịch sử temporal properties (`GET /v1/digital-twins`, `GET /v1/digital-twins/{entity_id}`, `/state`, `/history`).
  - **Giai đoạn 5 (Lát cắt Điều khiển):** Triển khai thao tác ghi cấu hình mong muốn (`PATCH /v1/digital-twins/{entity_id}/desired-state`), sinh lệnh (`twin_commands`), lưu hàng đợi xuất bản transactional outbox (`twin_outbox`) trong cùng một transaction, phát lệnh qua MQTT tới Gateway, nhận kết quả thực thi `command_result` và đồng bộ `reported_state`, tra cứu trạng thái lệnh qua `GET /v1/commands/{command_id}`.
  - **Giai đoạn 6 (Reconciliation & Recovery):** Hoàn thiện vòng đời lệnh (`pending -> published -> acknowledged -> succeeded/failed/timeout`), cơ chế timeout, retry backoff có jitter, và đối chiếu hội tụ desired/reported state khi kết nối lại.
- **Chính sách phân quyền mục tiêu (Target Operational Policy):**
  - `owner`: Có quyền đọc đầy đủ; có quyền quản lý cấu hình lâu dài (`desired_state`) và siêu dữ liệu cảm biến thuộc trạm của mình; có thể gửi các lệnh tác vụ an toàn nằm trong danh sách cho phép (allowlist) của server.
  - `operator`: Có quyền đọc dữ liệu trạm được phân công; chỉ được gửi các lệnh tác vụ vận hành an toàn nằm trong danh sách kiểm duyệt nghiêm ngặt của server (ví dụ thao tác một lần như `capture-image` nếu được hiện thực hóa và kiểm thử); **tuyệt đối bị từ chối** quyền sửa đổi cấu hình thiết bị lâu dài (`desired_state`), sửa đổi cảm biến, thay đổi quyền sở hữu hoặc quản lý chứng thực.
  - `viewer`: Hoàn toàn là quyền chỉ đọc; mọi yêu cầu ghi, cấu hình hay điều khiển đều bị từ chối.
- **Ranh giới an toàn nghiêm ngặt:**
  - Tuyệt đối cấm các lệnh tùy tiện hoặc không an toàn (như lệnh shell thô, payload tùy ý, hoặc khởi động lại không an toàn / unapproved reboot).
  - Không hỗ trợ chính sách điều khiển tự động bằng mô hình ngôn ngữ lớn (No autonomous LLM control).
  - **Client không bao giờ xuất bản trực tiếp lên MQTT Broker:** Mọi lệnh điều khiển bắt buộc phải đi qua Go REST API, được ghi nhận đồng thời vào bảng `twin_commands` và `twin_outbox` trong một transaction cơ sở dữ liệu trước khi worker xuất bản sang Mosquitto.

### 5.3. Quy tắc Bất khả xâm phạm về Phản hồi Lệnh
- Khi gửi yêu cầu lệnh thành công qua API, Backend trả về mã `202 Accepted` kèm `command_id` và trạng thái `pending`.
- **Mã `202 Accepted` CHỈ CÓ NGHĨA LÀ LỆNH ĐÃ ĐƯỢC GHI NHẬN HỢP LỆ VÀ ĐƯA VÀO HÀNG ĐỢI TRANSACTIONAL OUTBOX, KHÔNG CÓ NGHĨA LÀ THIẾT BỊ VẬT LÝ ĐÃ THỰC THI THÀNH CÔNG.**
- Lệnh chỉ được coi là thành công khi Gateway nhận lệnh, thực thi và gửi bản tin kết quả xác nhận qua MQTT, sau đó Go backend cập nhật trạng thái lệnh sang `succeeded` và đồng bộ `reported_state`.
- Client hiển thị trạng thái đang xử lý và chỉ chuyển sang thành công khi nhận được sự kiện cập nhật tương ứng.

> **Hiện trạng thực tế:** Chỉ có route `GET /v1/digital-twins` được đăng ký dạng giữ chỗ (`501 Not Implemented`, yêu cầu Bearer token hợp lệ; thiếu token trả về `401 Unauthorized`). Toàn bộ các route chi tiết Digital Twin (`GET /v1/digital-twins/{entity_id}`, `PATCH /v1/digital-twins/{entity_id}`), trạng thái (`/state`, `/history`), cập nhật desired-state (`PATCH /v1/digital-twins/{entity_id}/desired-state`), bảng lệnh (`POST/GET /v1/digital-twins/{entity_id}/commands`) và tra cứu lệnh (`GET /v1/commands/{command_id}`) hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin và sẽ nhận phản hồi `404 Not Found` từ `router.NoRoute`. Mọi cấu trúc JSON payload và định dạng lệnh dưới đây đều mang tính phác thảo định hướng dự kiến (provisional).

---

## 6. Khung Tổ chức Mã nguồn Tham khảo Định hướng

Cấu trúc thư mục dưới đây là **mô hình tham khảo định hướng phân chia tính năng (Feature-First Reference Layout)**, nhằm hỗ trợ việc chuẩn bị kiến trúc khi hệ thống bước sang các giai đoạn tiếp theo. Đây **không phải là bằng chứng cho thấy các thành phần này đã được hiện thực hóa** trên client:

```text
lib/
├── features/
│   ├── auth/                      # Gợi ý module cho API Auth hiện có, không chứng minh client hoàn tất
│   ├── gateways/                  # Gợi ý module cho danh sách Gateway/Sensor hiện có
│   ├── telemetry/                 # Khung định hướng Giai đoạn 4 (Charts Lịch sử) & Giai đoạn 7 (Realtime WS)
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
│   ├── media/                      # Khung định hướng Giai đoạn 8 (Supabase Storage Viewer)
│   │   ├── cubit/
│   │   ├── models/
│   │   ├── repositories/
│   │   └── widgets/
│   │       ├── media_grid.dart
│   │       └── lightbox_dialog.dart
│   └── digital_twin/              # Khung định hướng Giai đoạn 4 (Đọc Twin State) & Giai đoạn 5-6 (Device Control & Twin Commands)
│       ├── cubit/
│       ├── models/
│       ├── repositories/
│       └── widgets/
│           └── control_panel.dart
```

### Lưu ý khi triển khai:
- Để biết quy chuẩn tích hợp API thực tế và xử lý trạng thái hiện hữu trên client, tham khảo tài liệu `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`.
- Để biết chi tiết hợp đồng API quản trị hạ tầng trạm và chứng thực MQTT, tham khảo tài liệu `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`.
- Dự án không yêu cầu xây dựng ứng dụng quản trị riêng biệt; việc quản trị tài khoản người dùng và trạm được thực hiện qua Supabase Studio và các công cụ quản trị chuyên dụng của Platform Admin.
- Nền tảng ứng dụng client mục tiêu được lựa chọn theo `AGENTS.md` (Mục 3 và 17) là Flutter Web cho bản MVP; các ứng dụng di động (mobile) nằm ngoài phạm vi đồ án hiện tại và được hoãn lại (deferred). Kiến trúc API và cấu trúc dữ liệu client được thiết kế để có thể tái sử dụng cho ứng dụng di động sau này nếu được kích hoạt lại; không ưu tiên Android hay native-mobile trong phạm vi đồ án hiện hành.
