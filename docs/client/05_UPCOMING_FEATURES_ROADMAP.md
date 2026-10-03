# Tài liệu Client — 05: Lộ trình và Thiết kế Đón đầu Tính năng Mới trên Web Dashboard (Upcoming Features Roadmap)

**Cập nhật:** 2026-10-03  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 2, 4, 5.6, 10, 12, 13, 17), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/03_USER_GATEWAY_AUTHORIZATION.md`, `docs/client/04_REST_API_CLIENT_CONTRACT.md`, `docs/client/05_UI_FLOW_AND_STATE_MANAGEMENT.md`.

---

## 1. Mục đích tài liệu và Định hướng Thiết kế Đón đầu (Forward-Looking Architecture)

### 1.1. Bối cảnh chuyển giao giữa các giai đoạn
Tại thời điểm kết thúc **Giai đoạn 2 (Stage 2)**, hệ thống IoT Gateway–Server đã hoàn thiện vững chắc tầng nền tảng:
- Quản lý định danh người dùng tập trung thông qua Supabase Auth.
- Xác thực và ủy quyền đa tầng (Local JWT verification kết hợp kiểm tra quyền sở hữu trạm qua bảng `user_gateways` trong PostgreSQL).
- Hai API phân quyền danh mục cốt lõi đã sẵn sàng: `GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors`.
- Khung điều hướng giao diện Web Dashboard, hệ thống quản lý trạng thái BLoC/Cubit và xử lý ngoại lệ an toàn trên ứng dụng Flutter Web đã được chuẩn hóa.

Các endpoint phục vụ dữ liệu đo đạc, luồng thời gian thực, tệp đa phương tiện và điều khiển thiết bị hiện đang được đặt chỗ sẵn tại Go Backend (`src/internal/httpserver/router.go`) dưới dạng mã phản hồi `501 Not Implemented`.

### 1.2. Mục tiêu kiến trúc đón đầu trên Web Dashboard
Tài liệu này cung cấp **Bản thiết kế kỹ thuật đón đầu (Forward-looking Architecture Specification)** cho ứng dụng **Flutter Web Dashboard** nhằm phục vụ hai mục tiêu:
1. **Khả năng tương thích hướng tới tương lai (Forward-Compatibility):** Định hình chi tiết hợp đồng dữ liệu (Data Contracts), luồng xử lý bất đồng bộ, chiến lược bộ đệm và sơ đồ giao diện cho các tính năng thuộc **Giai đoạn 3** (Chuỗi thời gian lịch sử, WebSocket realtime, Media Viewer) và **Giai đoạn 4** (Digital Twin & Device Control).
2. **Bảo toàn cấu trúc mã nguồn (Zero-Refactor Principle):** Giúp lập trình viên Flutter xây dựng sẵn khung Domain Models, Repositories, Cubit/BLoC và Web UI Widgets ngay từ bây giờ. Khi Go Backend kích hoạt các endpoint từ `501 Not Implemented` sang `200 OK` hoặc `202 Accepted`, Web Dashboard chỉ cần cắm nối URL và kích hoạt luồng dữ liệu mà **không phải đập đi xây lại cấu trúc mã nguồn hiện hữu**.

```text
+-----------------------------------------------------------------------------------+
|                   LỘ TRÌNH TÍCH HỢP TÍNH NĂNG FLUTTER WEB DASHBOARD                |
+-----------------------------------------------------------------------------------+
| Giai đoạn 2 (Hiện tại):   | Đăng nhập, Danh mục Trạm, Danh mục Cảm biến           |
| Giai đoạn 3 (Sắp tới):    | Biểu đồ Lịch sử Web, WebSocket Streaming, Media Grid  |
| Giai đoạn 4 (Tiếp theo):  | Digital Twin State, Bảng lệnh & Đồng bộ Trạng thái     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Tính năng 1: Biểu đồ Chuỗi thời gian Lịch sử trên Web (Historical Time-Series Charts - Giai đoạn 3)

### 2.1. Bản chất kỹ thuật & Rào cản Dữ liệu Lớn (Big Data Constraint)
- Cảm biến tần số cao trong hệ thống có thể thu thập dữ liệu với tốc độ lên tới **100 Hz** (100 mẫu/giây = 8.640.000 mẫu/ngày).
- Nếu Client yêu cầu truy vấn khoảng thời gian dài và Backend trả về dữ liệu thô:
  - Băng thông mạng sẽ bị nghẽn (hàng trăm megabyte JSON).
  - Trình duyệt Web sẽ bị tràn bộ nhớ Heap của JavaScript engine dẫn đến đơ tab (Browser Tab Crash).
  - Tầng render của trình duyệt (CanvasKit / WebGL) sẽ bị tụt khung hình nghiêm trọng khi render hàng triệu điểm vector.
- **Giải pháp kỹ thuật:** Go Backend sử dụng hàm `time_bucket()` của **TimescaleDB** để tính toán gộp thích ứng (Adaptive Downsampling). Phía Web Dashboard sẽ nhận dữ liệu đã được tổng hợp theo các khoảng thời gian đều nhau, khống chế số lượng điểm trả về luôn dao động trong khoảng tối ưu: **từ 500 đến 1.000 điểm** cho một biểu đồ hiển thị.

### 2.2. Hợp đồng REST API dự kiến (`GET /v1/telemetry/history`)

- **Phương thức:** `GET`
- **Đường dẫn:** `/v1/telemetry/history`
- **Headers bắt buộc:**
  ```http
  Authorization: Bearer <access_token>
  Accept: application/json
  X-Request-ID: <uuidv4>
  ```
- **Tham số Query Parameters:**

| Tham số | Kiểu | Bắt buộc | Định dạng / Ví dụ | Mô tả |
|---|---|:---:|---|---|
| `gateway_id` | `string` | **Có** | `gateway_001` | Mã định danh trạm Gateway cần truy vấn. |
| `sensor_id` | `string` | **Có** | `sensor_001` | Mã định danh cảm biến cần truy vấn. |
| `from` | `string` | **Có** | `2026-10-01T00:00:00Z` | Mốc thời gian bắt đầu theo chuẩn ISO 8601 UTC. |
| `to` | `string` | **Có** | `2026-10-02T00:00:00Z` | Mốc thời gian kết thúc theo chuẩn ISO 8601 UTC. |
| `resolution` | `string` | Không | `1m`, `5m`, `1h`, `auto` | Độ phân giải khung gom thời gian. Mặc định `auto`. |

- **Định dạng phản hồi thành công (`200 OK`):**

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

### 2.3. Trải nghiệm Biểu đồ trên Web Desktop với `fl_chart`

#### Ưu thế Màn hình Rộng (Desktop Landscape):
- **Hiển thị toàn màn hình (Full Width Chart):** Chiều ngang rộng (1200px - 1440px) cho phép các điểm đo giãn cách đều, dễ quan sát xu hướng chuỗi thời gian hơn nhiều so với màn hình điện thoại hẹp.
- **Tương tác Chuột (Mouse Hover & Tooltip Chi tiết):** Khi rê chuột qua từng điểm trên đồ thị, hiển thị Tooltip nổi chứa đầy đủ: Thời gian chính xác, Giá trị trung bình (`avg`), Cực đại (`max`), Cực tiểu (`min`) và Số lượng mẫu (`sample_count`).
- **Kỹ thuật "Line Chart with Min-Max Range Band":**
  1. **Đường nét chính (Solid Line):** Vẽ giá trị trung bình (`avg`).
  2. **Dải bóng mờ bao quanh (Range Band):** Vùng diện tích mờ nằm giữa giá trị cực tiểu (`min`) và cực đại (`max`) thể hiện biên độ dao động.
  3. **Xử lý khoảng trống dữ liệu (Data Gaps):** Ngắt nét vẽ nếu khoảng cách thời gian vượt quá `resolution_applied` để không nối giả mạo dữ liệu khi mất kết nối.
- **Tính năng Phụ trợ trên Web Dashboard:**
  - **Chuyển đổi qua lại giữa Biểu đồ (Chart) và Bảng số liệu (Data Table):** Cho phép người vận hành xem dạng bảng chi tiết từng mốc đo.
  - **Nút Xuất dữ liệu CSV (Export CSV):** Tính năng thiết yếu trên Web Dashboard giúp trích xuất dữ liệu đo đạc phục vụ phân tích ngoại tuyến.

---

## 3. Tính năng 2: Luồng Dữ liệu Thời gian thực trên Web (Realtime Streaming qua WebSocket - Giai đoạn 3)

### 3.1. Kiến trúc kết nối WebSocket trên Trình duyệt

```text
Flutter Web Client (Trình duyệt)
    |  GET /v1/ws?token=<access_token>
    |  Headers: Upgrade: websocket, Connection: Upgrade
    v
Nginx Reverse Proxy
    |  Proxy chuyển tiếp HTTP Upgrade giữ nguyên Headers
    v
Go Backend (/v1/ws)
    |-- 1. Xác minh JWT token từ query param
    |-- 2. Kiểm tra danh sách Gateway trong PostgreSQL
    |-- 3. Đăng ký Client vào WebSocket Hub Manager
    v
TimescaleDB Commit Event -> Worker Pool -> WebSocket Fan-Out -> Trình duyệt Web
```

### 3.2. Yêu cầu An ninh WebSocket trên Web (`wss://`)
- **Bắt buộc dùng `wss://` khi trang chạy `https://`:** Nếu trang web được tải qua HTTPS mà client cố kết nối tới `ws://` (không mã hóa), trình duyệt sẽ lập tức chặn kết nối do vi phạm chính sách **Mixed Content Error**.
- **Cơ chế Xác thực qua Query Parameter:** Vì chuẩn WebSocket API của trình duyệt (`window.WebSocket`) không cho phép thêm header tùy biến trong pha bắt tay, access token được truyền qua query parameter:
  ```text
  wss://iot.example.com/v1/ws?token=<supabase_jwt_access_token>
  ```
- **Cam kết Dữ liệu An toàn:** Chỉ những bản tin đo đạc đã **commit thành công vào cơ sở dữ liệu PostgreSQL/TimescaleDB** mới được đẩy ra WebSocket.

### 3.3. Xử lý Chuyên sâu phía Flutter Web Client
- **Thư viện sử dụng:** `web_socket_channel` (tự động nhận diện nền tảng Web và sử dụng `HtmlWebSocketChannel`).
- **Cơ chế Đệm và Điều tiết Tốc độ (Buffering & Throttling Engine):**
  - Trình duyệt chạy giao diện theo vòng lặp `requestAnimationFrame` (thường là 60Hz hoặc 120Hz).
  - Khi cảm biến phát 100 Hz, Client **không render trực tiếp từng gói tin** mà nạp gói tin vào hàng đợi đệm (`List<TelemetryPoint>`).
  - Dùng `Timer.periodic(Duration(milliseconds: 33), ...)` (tương ứng 30 FPS) hoặc 60 FPS để cập nhật biểu đồ với cửa sổ trượt (Sliding Window 50–100 điểm mới nhất).
- **Xử lý khi Chuyển Tab Trình duyệt (Window Focus / Inactive Tab):**
  - Trình duyệt Web có cơ chế giảm tần số (throttle) các `Timer` trong tab không hoạt động (background tab) để tiết kiệm pin.
  - Khi người dùng quay lại tab (sự kiện Window Focus), Client cần giới hạn kích thước hàng đợi đệm (xả bỏ các điểm đo quá cũ) để tránh hiện tượng biểu đồ nhảy dồn dập hàng nghìn điểm cùng lúc làm đơ tab.

---

## 4. Tính năng 3: Xem Hình ảnh Lưu trữ trên Web (Media Viewer qua Supabase Storage - Giai đoạn 3)

### 4.1. Nguyên tắc An toàn và CORS đối với Media
- **Private Bucket tuyệt đối:** Hình ảnh lưu trong bucket riêng tư (`private`), không bao giờ mở Public Bucket (`AGENTS.md` Mục 13).
- **Cấu hình CORS cho hình ảnh:** Khi trình duyệt tải ảnh từ Nginx (`/storage/v1/*`), Nginx phải hỗ trợ header CORS (`Access-Control-Allow-Origin: *`) để widget `Image.network` của Flutter Web tải được ảnh mà không bị lỗi CanvasKit taint.

### 4.2. Luồng Vận hành 5 bước (End-to-End Media Flow)
1. **Gateway chụp ảnh:** Gateway chụp ảnh, xin Signed Upload URL từ Go Backend, sau đó gửi ảnh thẳng lên Supabase Storage qua Nginx.
2. **Client đọc danh sách ảnh:** Client gửi request `GET /v1/gateways/{gateway_id}/media?page=1&limit=20` để lấy danh mục metadata ảnh (`media_id`, `captured_at`, `file_size`, `content_type`).
3. **Client xin Signed Read URL:** Gửi `POST /v1/gateways/{gateway_id}/media/{media_id}/signed-read-url`.
4. **Backend thẩm định quyền:** Go Backend kiểm tra quyền `user_gateways` của User trước khi sinh Signed Read URL ngắn hạn (5–15 phút).
5. **Client tải ảnh:** Client dùng Signed URL để hiển thị ảnh trên Web.

### 4.3. Giao diện Thư viện Ảnh (Media Gallery) trên Web
- **Lưới ảnh Responsive (Responsive Image Grid):**
  - Màn hình Desktop rộng: Hiển thị lưới 4–6 cột thẻ ảnh thumbnail.
  - Màn hình Tablet/Mobile: Tự động co về 2–3 cột.
- **Xem ảnh Phóng to (Lightbox / Modal Viewer):**
  - Trên Web, khi bấm vào ảnh, mở hộp thoại phóng to (Lightbox Dialog) phủ mờ toàn trang thay vì mở màn hình mới.
  - Hỗ trợ cuộn chuột để phóng to/thu nhỏ (Zoom in/out) và nút tải ảnh gốc về máy tính.

---

## 5. Tính năng 4: Tương tác Digital Twin và Gửi lệnh Điều khiển trên Web (Digital Twin Downlink Commands - Giai đoạn 4)

### 5.1. Khái niệm cốt lõi: `reported_state` vs `desired_state`
- `reported_state`: Trạng thái thực tế do phần cứng Gateway báo cáo lên qua MQTT.
- `desired_state`: Cấu hình mục tiêu do người dùng có thẩm quyền mong muốn thiết lập.
- Client chỉ gửi mong muốn thay đổi cấu hình (`PATCH /v1/digital-twins/{entity_id}/desired-state`).

### 5.2. Ma trận Phân quyền Gửi lệnh trên Giao diện
- `owner`: Có toàn quyền sửa đổi cấu hình thiết bị (`desired_state`) và gửi lệnh an toàn.
- `operator`: Chỉ được gửi các lệnh tác vụ an toàn nằm trong allowlist của server (ví dụ `capture-image`); **bị từ chối** quyền sửa đổi cấu hình lâu dài (`desired_state`).
- `viewer`: Hoàn toàn không có quyền gửi lệnh (giao diện ẩn toàn bộ nút bấm điều khiển).

### 5.3. Giao diện Điều khiển Trạm trên Web Dashboard (Control Panel)
- **Bảng điều khiển dạng Ngăn kéo trượt (Slide-over Drawer):** Khi bấm nút "Điều khiển trạm" trên thanh công cụ chi tiết Gateway, một ngăn kéo trượt ra từ cạnh phải màn hình chứa các công tắc/thanh trượt cấu hình (Tần số lấy mẫu, Chế độ hoạt động, Nút chụp ảnh tức thì).
- **Quy tắc Bất khả xâm phạm về Phản hồi Lệnh:**
  - Khi gửi lệnh thành công, Backend trả về mã `202 Accepted` kèm `command_id` và trạng thái `pending`.
  - **Mã `202 Accepted` KHÔNG CÓ NGHĨA LÀ THIẾT BỊ ĐÃ THỰC THI THÀNH CÔNG.**
  - Giao diện Client hiển thị badge trạng thái đang xử lý (`Đang gửi lệnh...`), và chỉ chuyển sang `Thành công` khi nhận được bản tin xác nhận từ Gateway qua luồng WebSocket (`acknowledged` $\rightarrow$ `succeeded`).

---

## 6. Khuyến nghị Cấu trúc Mã nguồn Sẵn sàng Mở rộng (Feature-First)

Để không phải viết lại mã nguồn khi bước sang Giai đoạn 3 và 4, cấu trúc thư mục `lib/features/` được bố trí sẵn như sau:

```text
lib/
├── features/
│   ├── auth/                      # Đã hoàn thành (Giai đoạn 2)
│   ├── gateways/                  # Đã hoàn thành (Giai đoạn 2)
│   ├── telemetry/                 # Đặt chỗ Giai đoạn 3 (Charts & Realtime WS)
│   │   ├── bloc/
│   │   │   ├── telemetry_history_cubit.dart
│   │   │   └── telemetry_realtime_bloc.dart
│   │   ├── models/
│   │   │   ├── telemetry_point.dart
│   │   │   └── history_query_params.dart
│   │   ├── repositories/
│   │   │   └── telemetry_repository.dart
│   │   └── widgets/
│   │       ├── web_time_series_chart.dart
│   │       └── chart_range_selector.dart
│   ├── media/                      # Đặt chỗ Giai đoạn 3 (Supabase Storage Viewer)
│   │   ├── cubit/
│   │   ├── models/
│   │   ├── repositories/
│   │   └── widgets/
│   │       ├── web_media_grid.dart
│   │       └── web_lightbox_dialog.dart
│   └── digital_twin/              # Đặt chỗ Giai đoạn 4 (Device Control & Twin State)
│       ├── cubit/
│       ├── models/
│       ├── repositories/
│       └── widgets/
│           └── web_control_drawer.dart
```
