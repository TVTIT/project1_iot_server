# Tài liệu Client — 05: Lộ trình và Thiết kế Định hướng Tính năng Mới trên Client Dashboard (Upcoming Features Roadmap)

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 2, 4, 5.6, 6, 7, 10, 12, 13, 15, 17), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/03_USER_GATEWAY_AUTHORIZATION.md`, `docs/client/04_REST_API_CLIENT_CONTRACT.md`, `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`, `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`.

---

## 1. Mục đích tài liệu và Định hướng Thiết kế Đón đầu (Forward-Looking Architecture)

### 1.1. Bối cảnh hệ thống và Hiện trạng API tại Go Backend

Tại thời điểm hiện tại, hệ thống IoT Gateway–Server đã hoàn thiện tầng nền tảng định danh và ủy quyền:
- Quản lý định danh người dùng tập trung thông qua Supabase Auth (chính sách quản trị tập trung, không mở tự đăng ký công khai).
- Xác thực và ủy quyền đa tầng: Local JWT verification kết hợp kiểm tra quyền truy cập theo bảng quan hệ `user_gateways` và quyền quản trị nền tảng `platform_admins` trong PostgreSQL.
- Hai REST API danh mục đã hoạt động đầy đủ: `GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors`.
- Sáu endpoint quản trị nền tảng (`/v1/admin/*`) phục vụ khởi tạo trạm, cảm biến và quản lý chứng thực MQTT đã được triển khai (xem `07_ADMIN_API_CLIENT_CONTRACT.md`). Vòng đời credential mới dùng native DynSec/controller/Ingress Gate; không dùng `mosquitto_passwd` của runtime legacy Task 2.5. Credential API vẫn tắt trong deployment mặc định. Đây là quản trị hạ tầng, không phải lệnh điều khiển thiết bị vật lý.

**Hiện trạng thực tế các endpoint tính năng tương lai tại Go Backend (`src/internal/httpserver/router.go`):**
1. **Endpoint đăng ký dạng giữ chỗ (`501 Not Implemented`):** Chỉ có đúng ba route sau được đăng ký trong nhóm route yêu cầu xác thực (`authenticated`):
   - `GET /v1/telemetry/history`
   - `GET /v1/ws`
   - `GET /v1/digital-twins`
   Các route này nằm sau middleware xác thực token (`auth.AuthenticationMiddleware`); do đó, yêu cầu không có Bearer token hợp lệ sẽ nhận mã lỗi `401 Unauthorized` trước khi tiếp cận handler `501`.
2. **Endpoint chưa đăng ký (`404 Not Found`):** Tất cả các endpoint khác được đề cập trong tài liệu này như xem đa phương tiện (`/v1/gateways/{gateway_id}/media`, `/v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`), chi tiết thực thể Digital Twin (`/v1/digital-twins/{entity_id}`), trạng thái mong muốn (`/v1/digital-twins/{entity_id}/desired-state`), bảng lệnh (`/v1/digital-twins/{entity_id}/commands`, `/v1/commands/{command_id}`) và quản lý người dùng hiện **hoàn toàn chưa được khai báo** trong bộ định tuyến Gin và sẽ nhận phản hồi `404 Not Found` từ `router.NoRoute`.

### 1.2. Bản chất tài liệu: Thiết kế định hướng, không phải hợp đồng cam kết

Tài liệu này cung cấp **Bản thiết kế kỹ thuật định hướng (Provisional / Forward-Looking Design)** nhằm mục đích:
1. **Định hình luồng nghiệp vụ tương lai:** Mô tả trước tư duy kiến trúc chuỗi thời gian lịch sử, luồng streaming dữ liệu đã commit, lưu trữ tệp ảnh riêng tư và mô hình Digital Twin reported/desired state.
2. **Giới hạn phạm vi và tính chất dự kiến:** Mọi định dạng JSON payload, tham số truy vấn (query parameters), bộ lọc, công thức gom mẫu TimescaleDB (`time_bucket`), cơ chế bắt tay WebSocket và cấu trúc sự kiện trình bày dưới đây đều là **đề xuất kỹ thuật dự kiến (provisional)**, chưa phải là hợp đồng backend chính thức (not backend contract).
3. **Không cam kết "Zero-Refactor" hay chuyển đổi tức thì:** Phía giao diện Client **tuyệt đối không gọi hay phụ thuộc cứng** vào các định dạng dự kiến này khi Backend chưa chính thức triển khai và kiểm thử. Việc tích hợp thực tế khi bước sang Giai đoạn 3 và 4 sẽ cần điều chỉnh tương ứng với hợp đồng chính thức, không có bảo đảm chuyển giao mà không chỉnh sửa mã nguồn. Các thành phần giao diện cho những tính năng này hiện chưa được hiện thực hóa trên client.

```text
+-----------------------------------------------------------------------------------+
|               LỘ TRÌNH ĐỊNH HƯỚNG TÍNH NĂNG CLIENT DASHBOARD                      |
+-----------------------------------------------------------------------------------+
| Giai đoạn 2 (Hiện tại):   | Đăng nhập, Danh mục Trạm, Danh mục Cảm biến           |
|                           | Quản trị Hạ tầng Trạm/Cảm biến/MQTT (Chi tiết doc 07) |
| Giai đoạn 3 (Định hướng): | Biểu đồ Lịch sử, WebSocket Streaming, Media Viewer    |
| Giai đoạn 4 (Định hướng): | Digital Twin State, Bảng lệnh & Đồng bộ Trạng thái     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Tính năng 1: Biểu đồ Chuỗi thời gian Lịch sử (Historical Time-Series Charts - Dự kiến Giai đoạn 3)

### 2.1. Rào cản Dữ liệu Lớn và Nguyên lý Gom mẫu Thích ứng
- Cảm biến tần số cao trong hệ thống có thể thu thập dữ liệu với tốc độ lên tới **100 Hz** (100 mẫu/giây = 8.640.000 mẫu/ngày cho mỗi kênh đo).
- Nếu Client yêu cầu truy vấn khoảng thời gian dài và Backend trả về dữ liệu thô:
  - Băng thông mạng bị chiếm dụng lớn (hàng trăm megabyte JSON).
  - Trình duyệt/Ứng dụng Client sẽ bị quá tải bộ nhớ và tụt khung hình nghiêm trọng khi cố render hàng triệu điểm vector.
- **Định hướng kỹ thuật:** Backend sẽ sử dụng hàm `time_bucket()` của **TimescaleDB** để tính toán gộp thích ứng (Adaptive Downsampling). Phía Client sẽ nhận dữ liệu đã được tổng hợp theo các khoảng thời gian đều nhau, khống chế số lượng điểm trả về trong khoảng tối ưu: **từ 500 đến 1.000 điểm** cho một biểu đồ hiển thị. Toàn bộ mẫu thô vẫn được lưu trữ đầy đủ trong TimescaleDB để phục vụ phân tích chuyên sâu.

### 2.2. Đề xuất Kỹ thuật Dự kiến (`GET /v1/telemetry/history`)

> **Cảnh báo:** Route `GET /v1/telemetry/history` hiện trả về `501 Not Implemented` (và `401 Unauthorized` nếu thiếu Bearer token). Cấu trúc tham số và phản hồi dưới đây là **bản phác thảo định hướng**, không phải hợp đồng API đã kích hoạt. Không tích hợp mã giao diện vào endpoint này ở giai đoạn hiện tại.

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

## 3. Tính năng 2: Luồng Dữ liệu Thời gian thực (Realtime Streaming qua WebSocket - Dự kiến Giai đoạn 3)

### 3.1. Luồng kiến trúc tổng thể

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
- **Hiện trạng route:** `GET /v1/ws` hiện đã được đăng ký nhưng trả về mã `501 Not Implemented` (và `401 Unauthorized` nếu không có token). Client chưa thể kết nối WebSocket ở giai đoạn này.
- **Nguyên tắc phân phối dữ liệu:** Chỉ những bản tin đo đạc đã **commit thành công vào cơ sở dữ liệu PostgreSQL/TimescaleDB** mới được đẩy qua luồng realtime đến các client được phân quyền truy cập trạm tương ứng.

### 3.3. Định hướng Xử lý Tải phía Client
- **Điều tiết hiển thị (Throttling / Buffering):** Khi cảm biến gửi dữ liệu tần số cao, client không vẽ trực tiếp từng điểm mà đệm vào hàng đợi và cập nhật biểu đồ theo chu kỳ hiển thị phù hợp (ví dụ 30–60 FPS) để bảo toàn tài nguyên CPU/GPU.
- **Xử lý nền:** Khi ứng dụng bị đưa xuống nền hoặc tab trình duyệt mất tiêu điểm, client cần giới hạn kích thước hàng đợi đệm để tránh hiện tượng dồn ứ hàng nghìn điểm đo gây đơ giao diện khi người dùng kích hoạt lại.

---

## 4. Tính năng 3: Xem Hình ảnh Lưu trữ (Media Viewer qua Supabase Storage - Dự kiến Giai đoạn 3)

### 4.1. Nguyên tắc An toàn Lưu trữ Đa phương tiện
- **Private Bucket tuyệt đối:** Tất cả tệp hình ảnh được lưu trữ trong bucket riêng tư (`private`) của Supabase Storage. Tuyệt đối không mở Public Bucket (`AGENTS.md` Mục 13).
- **Phân tách luồng dữ liệu:** MQTT chỉ sử dụng cho dữ liệu đo đạc và siêu dữ liệu điều khiển; tệp media nhị phân tuyệt đối không truyền qua MQTT.

### 4.2. Luồng Vận hành 5 bước Định hướng (End-to-End Media Flow)
1. **Gateway tải ảnh lên:** Gateway chụp ảnh, yêu cầu Signed Upload URL từ Go Backend, sau đó tải thẳng tệp nhị phân lên Supabase Storage qua Nginx và Supabase API Gateway.
2. **Client đọc danh mục ảnh:** Client gửi yêu cầu lấy danh sách siêu dữ liệu ảnh của trạm được ủy quyền (route dự kiến `GET /v1/gateways/{gateway_id}/media`).
3. **Client xin Signed Read URL:** Client gửi yêu cầu lấy đường dẫn đọc có chữ ký ngắn hạn (route dự kiến `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`).
4. **Backend thẩm định quyền:** Go Backend kiểm tra quyền trong bảng `user_gateways` trước khi sinh Signed Read URL có thời hạn ngắn (ví dụ 5–15 phút).
5. **Client hiển thị ảnh:** Client sử dụng Signed Read URL để tải ảnh hiển thị.

> **Hiện trạng thực tế:** Các route phục vụ danh mục media và cấp Signed Read URL hiện **chưa được đăng ký** trong Go Backend và sẽ trả về mã `404 Not Found`.

---

## 5. Tính năng 4: Tương tác Digital Twin và Gửi lệnh Điều khiển (Digital Twin Downlink Commands - Dự kiến Giai đoạn 4)

### 5.1. Khái niệm cốt lõi: `reported_state` vs `desired_state`
- `reported_state`: Trạng thái thực tế do phần cứng Gateway báo cáo lên thông qua MQTT và được Go backend xác thực, ghi nhận vào PostgreSQL.
- `desired_state`: Cấu hình mục tiêu do người dùng có thẩm quyền yêu cầu thiết lập thông qua Go API.
- Cập nhật `desired_state` chỉ sinh ra lệnh điều khiển khi có sự khác biệt có thể thực thi so với `reported_state`.
- Telemetry và dữ liệu tải lên từ Gateway chỉ cập nhật `reported_state`, tuyệt đối không được ghi đè lên `desired_state`.

### 5.2. Mô hình Phân quyền và Ranh giới An toàn Điều khiển Thiết bị
Theo quy định tại `AGENTS.md` (Mục 6 và 10):
- **Trạng thái hiện tại (Stage 2 boundary - Fail-closed):** Cho đến khi nhánh tính năng điều khiển Digital Twin được hoàn thiện đầy đủ với kiểm tra phân quyền theo từng tác vụ, schema kiểm tra tính hợp lệ và bộ kiểm thử hoàn chỉnh, toàn bộ các thao tác ghi và gửi lệnh điều khiển đều **đóng chặt (fail-closed)**. Quyền `operator` được đối xử như chế độ chỉ đọc.
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

> **Hiện trạng thực tế:** Chỉ có route `GET /v1/digital-twins` được đăng ký dạng giữ chỗ (`501 Not Implemented`, yêu cầu xác thực). Toàn bộ các route chi tiết Digital Twin, cập nhật desired-state và bảng lệnh hiện **chưa được đăng ký** trong Go Backend và sẽ trả về mã `404 Not Found`.

---

## 6. Khung Tổ chức Mã nguồn Tham khảo Định hướng

Cấu trúc thư mục dưới đây là **mô hình tham khảo định hướng phân chia tính năng (Feature-First Reference Layout)**, nhằm hỗ trợ việc chuẩn bị kiến trúc khi hệ thống bước sang các giai đoạn tiếp theo. Đây **không phải là bằng chứng cho thấy các thành phần này đã được hiện thực hóa** trên client:

```text
lib/
├── features/
│   ├── auth/                      # Gợi ý module cho API Auth hiện có, không chứng minh client hoàn tất
│   ├── gateways/                  # Gợi ý module cho danh sách Gateway/Sensor hiện có
│   ├── telemetry/                 # Khung định hướng Giai đoạn 3 (Charts & Realtime WS)
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
│   ├── media/                      # Khung định hướng Giai đoạn 3 (Supabase Storage Viewer)
│   │   ├── cubit/
│   │   ├── models/
│   │   ├── repositories/
│   │   └── widgets/
│   │       ├── media_grid.dart
│   │       └── lightbox_dialog.dart
│   └── digital_twin/              # Khung định hướng Giai đoạn 4 (Device Control & Twin State)
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
- Nền tảng ứng dụng client mục tiêu được lựa chọn theo `AGENTS.md` là ứng dụng Flutter MVP (thông thường ưu tiên Android, hỗ trợ giao diện responsive); không tự ý đưa ra quyết định thay đổi nền tảng sang Web-first nếu chưa có phê duyệt kiến trúc.
