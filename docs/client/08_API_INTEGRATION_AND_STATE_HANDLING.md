# Tài liệu Client — 08: Tích hợp API và Quản lý Trạng thái Giao diện (API Integration and State Handling)

**Cập nhật:** 2026-10-14
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 1, 2, 5, 6, 6.5, 10, 16, 18)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Phân quyền người dùng và trạm)
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API chuẩn)
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/dynsec/limitations-and-backlog.md` (Hạn chế kỹ thuật và tồn đọng DynSec)

---

## 1. Mục tiêu, Ranh giới và Tuyên bố Hiện thực (Scope & Implementation Notice)

### 1.1 Mục tiêu tài liệu
Tài liệu này cung cấp **danh mục quy chuẩn (checklist) thực tiễn, ưu tiên endpoint (endpoint-first)** dành cho việc tích hợp API, xử lý trạng thái giao diện (UI State Handling) và bắt lỗi mạng an toàn trên ứng dụng Client (Flutter Web) trong phạm vi Giai đoạn 2 (Stage 2).

### 1.2 Tuyên bố hiện thực (No Premature Client Implementation Claims)
- **Tài liệu này đóng vai trò đặc tả thiết kế và danh mục kiểm thử, KHÔNG khẳng định mã nguồn Client đã được hiện thực hoàn chỉnh.**
- Giao diện người dùng phải tuân thủ nghiêm ngặt các endpoint đã thực sự tồn tại trong Go Backend và GoTrue Auth; tuyệt đối không giả định hoặc tự sáng tác các endpoint chưa có.

### 1.3 Phân định bằng chứng kiểm thử (Evidence Status)
- **Bằng chứng kiểm chứng lịch sử (Historical Test Snapshot):** Bộ kiểm thử E2E cô lập đã từng hoàn tất thực nghiệm 27/27 kịch bản (E01–E27) đạt `PASS` trên hạ tầng kiểm thử cục bộ (`scripts/test-stage2-e2e.sh`, ảnh chụp lịch sử ngày 2026-10-05). Do kế hoạch CI hiện tại đã điều chỉnh thu gọn và hoãn lại các job legacy, tài liệu ghi nhận đây là ảnh chụp lịch sử, **không tuyên bố đã chạy mới toàn bộ bộ test E2E này**.
- **Đóng cửa có giới hạn Giai đoạn 2 (Stage 2 Bounded Closeout ngày 2026-10-14):** Kiểm thử smoke full-HTTP Compose (`native-compose-smoke --full-http`) trên ứng viên commit `b681401` đã xác thực đường dẫn build đa tầng của `src/Dockerfile` (`target: backend`), ghi nhận 14 role denials, 9 JWT denials, 16 unchanged denied-write snapshots.
- **Nghiệm thu CI từ xa và Ngoại lệ Tồn đọng (Remote Acceptance & Deferred Exceptions):** Theo `AGENTS.md`, hệ thống chưa nghiệm thu trọn vẹn CI từ xa và tiếp tục mang theo các ngoại lệ kỹ thuật chuyển tiếp: Nhóm 1 (T07 Reset fail-closed 503 `denyOnlyResetPrerequisite{}`, chưa có quy trình phục hồi khả dụng), Nhóm 2 (T10 & ROT2 kiểm chứng broker cold-restart và độ bền bỉ đĩa chưa được kiểm chứng trong code), Nhóm 3 (T01 DB-commit ACK chưa kiểm chứng trong slice DynSec).
*Lưu ý an toàn:* Tuyệt đối không tuyên bố hệ thống sẵn sàng vận hành sản xuất (production-ready) khi các cổng nghiệm thu từ xa và ngoại lệ kỹ thuật chưa khép lại.

---

## 2. Ranh giới Mạng và Tách biệt Base URL (Network & Base URL Separation)

Client tương tác với hệ sinh thái qua hai miền dịch vụ hoàn toàn tách biệt thông qua Nginx Reverse Proxy:

```text
+-----------------------------------------------------------------------------------+
|                                 Flutter Web Client                                |
+------------------------------------------+----------------------------------------+
                                           |
                    +----------------------+----------------------+
                    |                                             |
                    v (HTTPS)                                     v (HTTPS)
+---------------------------------------+     +-------------------------------------+
|        Auth Base URL (Supabase)       |     |          Go API Base URL            |
|       https://<domain>/auth/v1        |     |       https://<domain>/v1           |
+-------------------+-------------------+     +------------------+------------------+
                    |                                            |
                    v                                            v
        Supabase API Gateway (Envoy)                   Go Backend (cmd/server)
                    |                                            |
                    v                                            v
         Supabase Auth (GoTrue)                         PostgreSQL / TimescaleDB
```

### 2.1 Nguyên tắc Base URL độc lập
1. **Auth Base URL (`/auth/v1`):** Tiếp nhận yêu cầu đăng nhập bằng email/password, cấp phát JWT và thực hiện Refresh Token. Đi qua Nginx đến Envoy API Gateway vào dịch vụ GoTrue.
2. **Business API Base URL (`/v1`):** Tiếp nhận toàn bộ truy vấn dữ liệu nghiệp vụ và phân quyền (`/v1/gateways`, `/v1/gateways/{gateway_id}/sensors`). Đi qua Nginx trực tiếp vào Go Backend.

### 2.2 Quy tắc cấm kỵ về bảo mật Token
- **CẤM truyền token qua URL:** Tuyệt đối không gửi Bearer token hay `user_id` qua URL Query Parameters (ví dụ: `?token=...` hoặc `?user_id=...`). Mọi định danh người dùng phải được Go Backend trích xuất duy nhất từ claim `sub` của JWT.
- **CẤM rò rỉ token qua argv và logs:** Tuyệt đối không truyền token qua đối số dòng lệnh (argv), không in token ra console log hoặc ghi log trình duyệt.
- **Rủi ro LocalStorage XSS trên Web:** Cần lưu ý việc lưu trữ JWT trong `localStorage` của trình duyệt có nguy cơ bị đánh cắp nếu ứng dụng dính lỗ hổng Cross-Site Scripting (XSS). Ưu tiên lưu trữ trong bộ nhớ tạm (in-memory state) kết hợp cơ chế quản lý phiên an toàn.

---

## 3. Trình tự Gọi API Thực tế và Cấu thành Trang Chi tiết (Actual Endpoint Sequence)

### 3.1 Trình tự tuần tự thực tế
Client bắt buộc phải gọi API theo đúng trình tự phân cấp:

```text
[1. Đăng nhập / Refresh] 
       POST /auth/v1/token?grant_type=password  (nhận JWT access_token)
          |
          v
[2. Lấy danh sách Gateway được cấp quyền]
       GET /v1/gateways
       Header: Authorization: Bearer <access_token>
       Response: 200 OK -> { "items": [ gatewayDTO... ] }
          |
          v
[3. Lấy danh sách Sensor của Gateway được chọn]
       GET /v1/gateways/{gateway_id}/sensors
       Header: Authorization: Bearer <access_token>
       Response: 200 OK -> { "gateway_id": "...", "items": [ sensorDTO... ] }
```

### 3.2 Cấu thành Trang Chi tiết (Current Detail Page Composition — No Invented Endpoints)
> **CẢNH BÁO QUAN TRỌNG:**  
> Go Backend **KHÔNG TỒN TẠI** endpoint lấy thông tin 1 Gateway đơn lẻ (`GET /v1/gateways/{gateway_id}`) và **KHÔNG TỒN TẠI** endpoint lấy 1 Sensor đơn lẻ (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`).
>
> - Khi xây dựng màn hình "Chi tiết Gateway" (Gateway Detail Page), Client **bắt buộc phải cấu thành (compose)** dữ liệu từ 2 nguồn có sẵn:
>   1. Thông tin trạm (`gateway_id`, `name`, `description`, `role`, `created_at`): Lấy từ đối tượng tương ứng trong mảng `items[]` của lệnh gọi `GET /v1/gateways`.
>   2. Danh sách cảm biến thuộc trạm: Lấy từ lệnh gọi `GET /v1/gateways/{gateway_id}/sensors`.
> - Tuyệt đối không tự ý phát minh hoặc gọi vào các endpoint đơn lẻ không tồn tại trên server để tránh phát sinh lỗi `404 Not Found` hoặc `405 Method Not Allowed`.

### 3.3 Luồng Khởi tạo Cảm biến cho Chủ sở hữu Trạm (Owner Sensor Provisioning Flow)
Khi người dùng đăng nhập có vai trò `owner` trên trạm Gateway được chọn:
1. Giao diện hiển thị nút "Thêm Cảm biến" (đối với `operator` và `viewer`, nút này bắt buộc phải bị ẩn hoặc vô hiệu hóa).
2. Người dùng nhập `sensor_id` (hợp lệ theo regex `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`), `name` (bắt buộc, 1..256 bytes), và `unit` (tùy chọn, 0..64 bytes).
3. Client gửi request:
   - `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`
   - Header: `Authorization: Bearer <access_token>`, `Content-Type: application/json`
   - Body: `{"name": "...", "unit": "..."}`
4. Xử lý phản hồi:
   - **`201 Created`**: Cảm biến được tạo mới thành công. Cập nhật danh sách cảm biến hiển thị trên giao diện.
   - **`200 OK`**: Phát lại chính xác (Exact Replay trùng khớp `name` và `unit`). Cập nhật giao diện coi như thành công.
   - **`409 Conflict`**: Cảm biến đã tồn tại với `name` hoặc `unit` khác. Giao diện hiển thị thông báo lỗi: *"Cảm biến đã tồn tại với thông tin khác. Hệ thống không hỗ trợ cập nhật cấu hình cảm biến qua endpoint này"*.
   - **`403 Forbidden`**: Người dùng có vai trò `operator` hoặc `viewer`. Hiển thị: *"Chỉ Chủ sở hữu trạm (Owner) mới có quyền khởi tạo cảm biến"*.
   - **`404 Not Found`**: Trạm không tồn tại hoặc người dùng không có quyền truy cập trạm.

### 3.4 Luồng Kiểm kê Toàn cầu của Platform Admin (Admin Gateway Inventory Flow)
Khi tài khoản được xác định là Platform Admin truy cập trang quản trị:
1. Client gửi `GET /v1/admin/gateways?limit=100` (kèm `after=<cursor>` nếu tải trang tiếp theo). Tham số `limit` mặc định là 100 và tối đa là 200 trên môi trường chuẩn (được cấu hình qua biến môi trường `ADMIN_INVENTORY_DEFAULT_LIMIT` và `ADMIN_INVENTORY_MAX_LIMIT` trong dải 1..1000).
2. Backend kiểm tra quyền trong `public.platform_admins` và phản hồi danh mục kiểm kê toàn cầu độc lập với bảng `user_gateways`:
   - `items`: Danh sách Gateway (`gateway_id`, `name`, `description`, `created_at`). Không có `role`, không có `owner_user_id`, không có bí mật MQTT.
   - `next_cursor`: Con trỏ phân trang, dùng cho request kế tiếp (`?after=<next_cursor>`). Khi `next_cursor == null`, danh sách đã duyệt hết.

---

## 4. Ma trận Trạng thái Giao diện Stage 2 (Stage 2 UI States Checklist)

Mọi màn hình trên Client phải xử lý đầy đủ các trạng thái sau để đảm bảo giao diện luôn tường minh và có thể giải thích được:

| Mã Trạng thái UI | Tình huống Kích hoạt từ Server | Hành vi Giao diện Yêu cầu (UI Behavior) |
|---|---|---|
| **`initial`** | Chưa có tương tác tải dữ liệu. | Hiển thị khung chờ, chuẩn bị tải hoặc trạng thái ban đầu của trang. |
| **`loading`** | Đang thực thi request hoặc làm mới dữ liệu. | Hiển thị hiệu ứng tải (progress indicator/shimmer), khóa tạm thời các thao tác trùng lặp. |
| **`success`** | Nhận `200 OK` (hoặc `201 Created` khi tạo mới Sensor) với mảng `items` có dữ liệu. | Hiển thị danh sách Gateway, danh sách Sensor, hoặc thông báo khởi tạo cảm biến thành công. |
| **`empty`** | Nhận `200 OK` nhưng mảng `items: []`. | Hiển thị thông báo rỗng thân thiện (ví dụ: *"Bạn chưa được phân quyền trên trạm nào"* hoặc *"Trạm chưa có cảm biến"*). Không coi đây là lỗi. |
| **`no-privacy 404`** | Nhận `404 Not Found` (`code: "not_found"`). | Xảy ra khi truy cập `gateway_id` lạ hoặc trạm mà người dùng không có quyền trong `user_gateways` (kể cả khi gọi đọc trạm, đọc cảm biến, hoặc gọi tạo cảm biến owner). **Nguyên tắc bảo vệ quyền riêng tư (Anti-Enumeration):** Giao diện chỉ hiển thị *"Không tìm thấy tài nguyên"*, tuyệt đối không suy đoán trạm có tồn tại hay không. |
| **`401 refresh limited`** | Nhận `401 Unauthorized` (`code: "unauthorized"`). | Phiên làm việc hết hạn hoặc token lỗi. Client kích hoạt luồng Refresh Token **giới hạn duy nhất 1 lần (bounded retry)**. Nếu thất bại, xóa session cục bộ và chuyển hướng về màn hình Đăng nhập. Tránh tạo vòng lặp vô hạn. |
| **`403 forbidden`** | Nhận `403 Forbidden` (`code: "forbidden"`). | - Tài khoản thông thường cố gọi API Quản trị `/v1/admin/*`: *"Thao tác yêu cầu quyền Quản trị viên hệ thống"*.<br>- `operator` hoặc `viewer` gọi khởi tạo Sensor (`PUT /v1/gateways/{id}/sensors/{id}`): *"Chỉ Chủ sở hữu trạm (Owner) mới có quyền khởi tạo cảm biến"*. |
| **`409 conflict`** | Nhận `409 Conflict` (`code: "conflict"`). | Khi gọi `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` nhưng cảm biến đã tồn tại với `name` hoặc `unit` khác: Hiển thị thông báo *"Cảm biến đã tồn tại với thông tin khác. Hệ thống không hỗ trợ cập nhật/ghi đè cấu hình cảm biến qua endpoint này"*. |
| **`503 dependency retry`** | Nhận `503 Service Unavailable` (`code: "service_unavailable"` hoặc `code: "credential_runtime_disabled"`). | Quá thời hạn xử lý cơ sở dữ liệu (`AUTHORIZATION_TIMEOUT`), database tạm thời gián đoạn, rào chắn khởi động chưa xong, hoặc tính năng credential bị tắt. Hiển thị *"Hệ thống bận hoặc dịch vụ tạm thời gián đoạn, vui lòng thử lại sau"*, cung cấp nút Thử lại (Retry) kèm giãn cách số mũ. |
| **`500 safe / support request_id`** | Nhận `500 Internal Server Error` (`code: "internal_error"`). | Lỗi máy chủ nội bộ. Server không trả về stack trace. Client **bắt buộc đọc `request_id`** (từ `X-Request-ID` header hoặc payload `error.request_id`) để hiển thị thông báo: *"Đã xảy ra sự cố hệ thống. Mã tra cứu hỗ trợ: `<request_id>`"*. |
| **`501 feature unavailable`** | Nhận `501 Not Implemented` (`code: "not_implemented"`). | Người dùng gọi vào các tính năng chưa triển khai ở Stage 2 (`/telemetry/history`, `/ws`, `/digital-twins`). Hiển thị thông báo tính năng chưa khả dụng. |
| **`405 developer error`** | Nhận `405 Method Not Allowed` (`code: "method_not_allowed"`). | Lỗi logic từ phía lập trình viên (ví dụ: gửi `POST` vào endpoint chỉ hỗ trợ `GET`). Ghi log lỗi phát triển, không cho phép kích hoạt thao tác sai. |

---

## 5. Xử lý Đồng bộ Quyền, Cache và Chống Race Condition

### 5.1 Thay đổi quyền cùng một JWT (Same-JWT Membership Change)
- Khi Quản trị viên thay đổi vai trò (ví dụ: từ `viewer` sang `operator`) hoặc thu hồi quyền truy cập trạm trong CSDL PostgreSQL (`public.user_gateways`), **JWT hiện tại của người dùng không cần cấp phát lại**.
- Thay đổi này có hiệu lực **ngay lập tức trong lần truy vấn CSDL tiếp theo** tại Go Backend.
- **Không có cơ chế Push Realtime:** Trong Stage 2, hệ thống không tự động đẩy thông báo đổi quyền xuống Client qua WebSocket. Client nhận biết quyền mới khi thực hiện thao tác tải lại dữ liệu (Pull-to-refresh hoặc điều hướng lại).

### 5.2 Xóa bộ nhớ đệm cũ (Stale Cache Clear)
- Go Backend luôn trả về header `Cache-Control: no-store` cho mọi dữ liệu nghiệp vụ danh mục.
- Client **không được lưu vĩnh viễn** danh sách Gateway hay Sensor vào bộ nhớ cục bộ lâu dài.
- Khi người dùng chủ động tải lại trang hoặc đăng xuất, toàn bộ bộ nhớ đệm trong RAM của phiên làm việc trước phải được dọn dẹp sạch sẽ.

### 5.3 Bảo vệ chống phản hồi lệch thứ tự (Out-of-Order Response Guarding)
Để giữ giải pháp đơn giản, sáng tỏ và dễ bảo trì (modest explainable):
- Sử dụng cơ chế hủy request (`AbortController` hoặc `CancelToken` của HTTP Client) khi người dùng chuyển trang hoặc đổi trạm liên tục trước khi request cũ hoàn thành.
- Hoặc sử dụng biến đếm thứ tự yêu cầu (`request_sequence_id`): Chỉ cập nhật trạng thái UI nếu phản hồi trả về khớp với mã định danh của yêu cầu mới nhất, loại bỏ hoàn toàn các phản hồi đến muộn (out-of-order responses).

---

## 6. Phân cấp Vai trò Người dùng và Ranh giới Nút Bấm Giao diện (Role Enforcement & UI Boundaries)

### 6.1 Đồng nhất quyền đọc và Ranh giới Khởi tạo Cảm biến của Owner
Căn cứ theo hiện thực CSDL và mã nguồn nghiệp vụ:
- Ba vai trò thành viên trạm gồm `owner`, `operator`, và `viewer` **đều sử dụng chung cùng một tập hợp Read API**:
  - `GET /v1/gateways`
  - `GET /v1/gateways/{gateway_id}/sensors`
- Cả ba vai trò đều đọc được danh sách trạm và danh sách cảm biến thuộc trạm mà mình được gán quyền trong `user_gateways`.
- **Phân định quyền khởi tạo cảm biến (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`):**
  - Duy nhất vai trò **`owner`** của trạm cha được cấp quyền gọi endpoint khởi tạo cảm biến tạo-mới-hoặc-phát-lại-chính-xác.
  - Hai vai trò **`operator`** và **`viewer`** tuyệt đối không có quyền này và sẽ nhận mã lỗi `403 Forbidden` (`code: "forbidden"`, `message: "access denied"`). Giao diện Client bắt buộc phải ẩn hoặc vô hiệu hóa nút "Thêm Cảm biến" đối với hai vai trò này.

### 6.2 Ranh giới Khởi tạo Cảm biến và CẤM BẬT NÚT CẤU HÌNH/ĐIỀU KHIỂN SỚM
- **Ranh giới endpoint khởi tạo cảm biến của Owner:**
  - Endpoint `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` chỉ là cơ chế tạo mới hoặc phát lại chính xác (create-or-exact-replay) siêu dữ liệu (`name`, `unit`) của Sensor.
  - Endpoint này **tuyệt đối KHÔNG PHẢI** là API cập nhật/chỉnh sửa thông tin cảm biến (không hỗ trợ thay đổi `name` hay `unit` đã tạo, gửi payload khác sẽ nhận `409 Conflict`), và **KHÔNG PHẢI** API xóa cảm biến.
- **Cấm bật nút điều khiển và cấu hình thiết bị lâu bền:**
  - Trong phạm vi Giai đoạn 2, các chức năng ghi cấu hình thiết bị lâu bền (`desired_state`), gửi lệnh điều khiển (downlink commands như reboot, capture-image), hoặc phân hệ Digital Twin runtime **CHƯA ĐƯỢC KÍCH HOẠT** trên Go Backend.
  - **Quy tắc giao diện:** Giao diện Client chỉ hiển thị form/nút "Thêm cảm biến" cho người dùng mang vai trò `owner` trên trạm được chọn. **Tuyệt đối không hiển thị hoặc không bật kích hoạt (enable)** các nút bấm cấu hình thiết bị lâu bền, nút gửi lệnh điều khiển, nút khởi động lại hay nút chụp ảnh cho bất kỳ vai trò nào (kể cả `owner` hay `operator`). Tránh gây hiểu nhầm khi backend chưa hỗ trợ.

### 6.3 Phân định Quyền Quản trị Nền tảng (Platform Admin Authority)
- Quyền quản trị nền tảng được kiểm tra độc lập tại bảng `public.platform_admins` trong PostgreSQL.
- **CẤM suy diễn quyền Admin:**
  1. Không được suy diễn quyền Admin từ vai trò `owner` trên một Gateway cụ thể.
  2. Không được suy diễn từ JWT claim `role: "authenticated"`.
  3. Client không bao giờ sở hữu `service_role` key của Supabase.
  4. Hệ thống **không có API tự phong quyền Admin** (No platformAdmin self-API).
- **Platform Admin không bypass quyền thành viên:** Nếu Platform Admin gọi `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` trên trạm mà mình không có quyền `owner` trong `user_gateways`, backend sẽ phản hồi `404 Not Found`. Platform Admin khởi tạo cảm biến độc lập với quyền sở hữu qua endpoint riêng `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`.
- Không cho phép bất kỳ cơ chế vượt quyền nào thông qua cờ giao diện phía Client (No UI flag permission bypass).

---

## 7. Quy trình Quản trị Tùy chọn (Optional Admin Workflow — Liên kết 07)

*Xem chi tiết toàn bộ hợp đồng API Quản trị tại:* `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`.

Khi tích hợp các chức năng quản trị dành cho Platform Admin (danh mục kiểm kê trạm, cấp phát trạm, cấp cảm biến, quản lý MQTT credentials), Client cần tuân thủ các quy chuẩn an toàn:

### 7.1 Xác nhận tường minh và cấm tự động thử lại khi Timeout
- **Xác nhận tường minh (Explicit Confirmation):** Các thao tác tạo trạm, xoay vòng mật khẩu hoặc thu hồi quyền truy cập của Gateway phải luôn có hộp thoại xác nhận từ người vận hành trước khi gửi đi.
- **Cấm tự động thử lại lệnh đột biến (No Auto-Mutating Retry):** Nếu request cấp phát hoặc xoay vòng credential gặp sự cố quá thời gian (timeout) hoặc mất kết nối mạng, Client **tuyệt đối không tự động gửi lại request ngầm**. Việc gửi lại chỉ được thực hiện khi người vận hành xác nhận và phải dùng cùng một khóa định danh/idempotency key để tránh xung đột dữ liệu.

### 7.2 Xử lý Mật khẩu MQTT dùng một lần (One-Time Secret Lifecycle)
- Mật khẩu MQTT được sinh ngẫu nhiên từ server và **chỉ trả về DUY NHẤT MỘT LẦN** trong payload phản hồi đầu tiên của API cấp phát (`POST`, HTTP 201) hoặc xoay vòng (`POST /rotate`, HTTP 200):
  ```json
  {
    "gateway_id": "gw-01",
    "username": "gw-01",
    "password": "<one_time_generated_secure_password_string>",
    "secret_returned": true,
    "status": "active"
  }
  ```
- **Hiển thị tạm thời (Transient Display):** Giao diện chỉ hiển thị mật khẩu này một lần trên màn hình (kèm nút sao chép an toàn) và cảnh báo người vận hành lưu trữ ngay.
- **CẤM lưu trữ lâu dài:** Client **tuyệt đối không lưu mật khẩu này vào LocalStorage, cơ sở dữ liệu Client, hay state vĩnh viễn**.
- **Không có API đọc lại mật khẩu:** Không tồn tại bất kỳ endpoint nào cho phép đọc lại mật khẩu đã cấp. Replay cùng key chỉ trả về `MutationResult` với `secret_returned: false` (trường password bị lược bỏ hoàn toàn). Nếu làm mất mật khẩu, bắt buộc phải thực hiện quy trình xoay vòng mật khẩu mới (`POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`).

### 7.3 Phân biệt lỗi 503 Credential và Ngoại lệ Reset
- Nếu gọi các API MQTT credentials nhận về `HTTP 503` kèm mã lỗi `credential_runtime_disabled`, máy chủ đang tắt API quản trị credential bằng `MQTT_CREDENTIAL_API_ENABLED=false`. Đây không phải lỗi đăng nhập hay lỗi quyền hạn tài khoản.
- Nếu nhận `HTTP 503` kèm `credential_runtime_busy`, hệ thống đang có thao tác credential khác chiếm giữ khóa advisory lock `0x494f545f43524544` hoặc runtime đang bận; chỉ thử lại có giãn cách (exponential backoff) và không thử lại vô hạn.
- **Endpoint Reset và Rào chắn An toàn Fail-closed:** Tuyến đường `POST /v1/admin/gateways/{id}/mqtt-credential/reset` đã được đăng ký và áp dụng bộ phân tích cú pháp nghiêm ngặt (`Idempotency-Key` đúng 1 non-nil UUID, `Content-Type: application/json`, body mang duy nhất trường `reason` hợp lệ, kích thước trong hạn mức `ADMIN_MAX_BODY_BYTES`, không query string). Lưu ý về mã lỗi và điều kiện tiên quyết:
  - Nếu tính năng credential bị tắt (`MQTT_CREDENTIAL_API_ENABLED=false`), server phản hồi `503 Service Unavailable` với mã miền đặc thù `code: "credential_runtime_disabled"` (chứ không phải `service_unavailable`).
  - Nếu vi phạm xác thực, phân quyền hoặc cú pháp request (body sai/rỗng, vượt hạn mức, thiếu `Idempotency-Key`...), server phản hồi các mã lỗi parser chuẩn tương ứng (`401`, `403`, `400`, `413`, `415`).
  - Chỉ khi request hoàn toàn hợp lệ và tính năng credential được kích hoạt, thành phần runtime `denyOnlyResetPrerequisite{}` (hoặc rào chắn khởi động chưa sẵn sàng) mới chặn an toàn với `HTTP 503 Service Unavailable` (`code: "service_unavailable"`, `message: "credential request failed"`).
  - Tuyệt đối không tự động thử lại vào endpoint reset này; các sự cố bất định hoặc mất secret phải được xử lý có giám sát theo runbook hạ tầng.

---

## 8. Giới hạn Quan sát của HTTP Client và Ranh giới Harness (Observation Limits)

Một ứng dụng HTTP Client thông thường (như trình duyệt Web hoặc công cụ REST Client) **chỉ có thể quan sát trạng thái HTTP phản hồi từ Go Backend**.

### 8.1 Những gì HTTP Client KHÔNG THỂ tự kiểm chứng:
1. Tính thực thi quyền hạn native của Mosquitto MQTT ACL trên Broker.
2. Việc ngắt kết nối socket thực sự (disconnect) của Gateway bị thu hồi quyền.
3. Socket TCP của Gateway B có bị ngắt nhầm khi thao tác thu hồi trên Gateway A hay không.
4. Lỗi phản hồi mạng thực sự (True E22 response fault injection).
5. Tính bền bỉ và toàn vẹn của tệp cấu hình Mosquitto Dynamic Security (`dynsec.json`) sau khi khởi động lại.

### 8.2 Ranh giới Kiểm thử
Để kiểm chứng các tính chất hạ tầng tầng sâu kể trên, người vận hành bắt buộc phải sử dụng bộ kịch bản kiểm thử E2E chuyên dụng trên môi trường cô lập:  
*Tham khảo chi tiết tại:* `docs/backend/stage-2-e2e-testing-guide.md`.

---

## 9. Danh mục Kiểm thử Tích hợp cho 6 Loại Tài khoản (Integration Test Checklist)

*Ghi chú bảo mật:* Toàn bộ các tài khoản dưới đây được định danh dưới dạng **biểu tượng quy ước (symbolic only)**, tuyệt đối không sử dụng email thật, mật khẩu thật, token thật hay tên miền triển khai cụ thể.

### 9.1 Định nghĩa 6 Loại Tài khoản Biểu tượng

1. **`Platform Admin`**: Có bản ghi trong `public.platform_admins`; không có bản ghi nào trong `public.user_gateways`.
2. **`Owner A`**: Không có trong `platform_admins`; có liên kết role `'owner'` trên `Gateway A` trong `user_gateways`. Không có quyền trên `Gateway B`.
3. **`Owner B`**: Không có trong `platform_admins`; có liên kết role `'owner'` trên `Gateway B` trong `user_gateways`. Không có quyền trên `Gateway A`.
4. **`Operator`**: Không có trong `platform_admins`; có liên kết role `'operator'` trên `Gateway A`.
5. **`Viewer`**: Không có trong `platform_admins`; có liên kết role `'viewer'` trên `Gateway A`.
6. **`Non-member`**: Tài khoản người dùng hợp lệ trong Supabase Auth; không có bất kỳ dòng nào trong `user_gateways` và `platform_admins`.

### 9.2 Bảng Kiểm tra Tích hợp Phân quyền (Expected Verification Matrix)

| Kịch bản Kiểm thử | Loại Tài khoản Thực thi | Endpoint Gọi | HTTP Status Kỳ vọng | Error Code / Body Kỳ vọng |
|---|---|---|:---:|---|
| **TC-01: Truy cập ẩn danh** | Không có token / Token rác | `GET /v1/gateways` | `401` | `code: "unauthorized"` |
| **TC-02: Tài khoản chưa gán trạm** | `Non-member` | `GET /v1/gateways` | `200` | `{ "items": [] }` |
| **TC-03: Xem trạm được gán** | `Owner A` / `Operator` / `Viewer` | `GET /v1/gateways` | `200` | Danh sách chứa `Gateway A`, không có `Gateway B`. |
| **TC-04: Cách ly giữa các Chủ sở hữu** | `Owner B` | `GET /v1/gateways` | `200` | Danh sách chứa `Gateway B`, không có `Gateway A`. |
| **TC-05: Đọc cảm biến trạm hợp lệ** | `Owner A` / `Operator` / `Viewer` | `GET /v1/gateways/gw-a/sensors` | `200` | `{ "gateway_id": "gw-a", "items": [...] }` |
| **TC-06: Đọc cảm biến trạm lạ (Foreign Sensor)** | `Owner A` gọi trạm của B | `GET /v1/gateways/gw-b/sensors` | `404` | `code: "not_found"` (Bảo vệ riêng tư) |
| **TC-07: Người dùng thường gọi Admin API** | `Owner A` / `Operator` / `Viewer` / `Non-member` | `PUT /v1/admin/gateways/gw-x` | `403` | `code: "forbidden"` |
| **TC-08: Admin gọi API Nghiệp vụ thông thường** | `Platform Admin` | `GET /v1/gateways` | `200` | `{ "items": [] }` (Admin không bypass CSDL nghiệp vụ) |
| **TC-09: Admin gọi API Cấp phát Quản trị** | `Platform Admin` | `PUT /v1/admin/gateways/gw-x` | `200` / `201` | `{ "gateway_id": "gw-x", ... }` |
| **TC-10: Chặn đăng ký công khai** | Ẩn danh (khi khóa tự đăng ký) | `POST /auth/v1/signup` | `422` | `code: "signup_disabled"` |
| **TC-11: Gọi endpoint chưa triển khai** | Có JWT hợp lệ bất kỳ | `GET /v1/telemetry/history` | `501` | `code: "not_implemented"` |
| **TC-12: Gọi sai phương thức (Method)** | Có JWT hợp lệ bất kỳ | `POST /v1/gateways` | `405` | `code: "method_not_allowed"` |
| **TC-13: Gọi đường dẫn không tồn tại** | Có JWT hợp lệ bất kỳ | `GET /v1/unknown-path` | `404` | `code: "not_found"` |
| **TC-14: Owner khởi tạo Sensor mới** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` | `201` | `{ "gateway_id": "gw-a", "sensor_id": "s1", "name": "...", "unit": "...", "entity_id": "urn:ngsi-ld:Sensor:gw-a:s1", ... }` |
| **TC-15: Owner phát lại chính xác Sensor (Exact Replay)** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` | `200` | Trả về thông tin cảm biến trùng khớp, giữ nguyên `created_at` ban đầu |
| **TC-16: Owner gửi cấu hình khác lên Sensor cũ** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` (khác name/unit) | `409` | `code: "conflict"`, `message: "resource conflict"` |
| **TC-17: Operator / Viewer cố tạo Sensor** | `Operator` / `Viewer` | `PUT /v1/gateways/gw-a/sensors/s2` | `403` | `code: "forbidden"`, `message: "access denied"` |
| **TC-18: Non-member gọi tạo Sensor** | `Non-member` hoặc `Owner B` gọi trạm A | `PUT /v1/gateways/gw-a/sensors/s2` | `404` | `code: "not_found"`, `message: "resource not found"` (Anti-Enumeration) |
| **TC-19: Platform Admin duyệt danh mục kiểm kê toàn cầu** | `Platform Admin` | `GET /v1/admin/gateways?limit=100` | `200` | `{ "items": [ ... ], "next_cursor": ... }` |
| **TC-20: Người dùng thường gọi danh mục kiểm kê** | `Owner A` / `Operator` / `Viewer` / `Non-member` | `GET /v1/admin/gateways` | `403` | `code: "forbidden"`, `message: "insufficient permissions"` |
| **TC-21: Gọi endpoint Reset có giám sát** | `Platform Admin` | `POST /v1/admin/gateways/gw-a/mqtt-credential/reset` (kèm body reason và Idempotency-Key hợp lệ khi Credential API được bật) | `503` | `code: "service_unavailable"`, `message: "credential request failed"` (denyOnlyResetPrerequisite fail-closed; nếu API tắt sẽ nhận `credential_runtime_disabled`) |

---

## 10. Mã Giả Minh Họa Tích Hợp Giao Diện (Illustrative Pseudocode Only)

> **LƯU Ý:** Đoạn mã giả dưới đây chỉ nhằm mục đích minh họa trực quan luồng xử lý trạng thái và bắt lỗi chuẩn hóa, không phải là mã nguồn hiện thực hoàn chỉnh và không yêu cầu cài đặt gói phụ thuộc.

```dart
// ============================================================================
// ILLUSTRATIVE PSEUDOCODE ONLY — NOT COMPLETE PRODUCTION IMPLEMENTATION
// ============================================================================

enum ViewStatus { initial, loading, success, empty, error }

class GatewayDetailState {
  final ViewStatus status;
  final GatewayItem? gateway;          // Trích xuất từ mảng GET /v1/gateways
  final List<SensorItem> sensors;      // Lấy từ GET /v1/gateways/{id}/sensors
  final String? errorMessage;
  final String? requestId;             // Luôn lưu vết phục vụ hỗ trợ kỹ thuật

  const GatewayDetailState({
    required this.status,
    this.gateway,
    this.sensors = const [],
    this.errorMessage,
    this.requestId,
  });
}

class GatewayIntegrationController {
  final ApiClient apiClient;
  final SessionManager sessionManager;

  GatewayIntegrationController(this.apiClient, this.sessionManager);

  // Cấu thành trang chi tiết từ danh sách trạm và truy vấn cảm biến
  Future<GatewayDetailState> loadGatewayDetail(String gatewayId) async {
    try {
      // 1. Lấy thông tin trạm từ danh sách đã cấp quyền (tránh gọi endpoint đơn lẻ không tồn tại)
      final gatewaysResponse = await apiClient.get('/v1/gateways');
      final gatewayList = (gatewaysResponse.data['items'] as List)
          .map((json) => GatewayItem.fromJson(json))
          .toList();

      final selectedGw = gatewayList.firstWhere(
        (g) => g.gatewayId == gatewayId,
        orElse: () => throw NotFoundException("Trạm không tồn tại hoặc không có quyền"),
      );

      // 2. Lấy danh sách cảm biến thuộc trạm
      final sensorsResponse = await apiClient.get('/v1/gateways/$gatewayId/sensors');
      final sensorList = (sensorsResponse.data['items'] as List)
          .map((json) => SensorItem.fromJson(json))
          .toList();

      if (sensorList.isEmpty) {
        return GatewayDetailState(
          status: ViewStatus.empty,
          gateway: selectedGw,
          sensors: const [],
        );
      }

      return GatewayDetailState(
        status: ViewStatus.success,
        gateway: selectedGw,
        sensors: sensorList,
      );

    } on UnauthorizedException catch (e) {
      // Xử lý 401: Thử refresh token giới hạn 1 lần duy nhất
      final refreshed = await sessionManager.refreshSessionOnce();
      if (refreshed) {
        return loadGatewayDetail(gatewayId); // Thử lại sau khi đã có token mới
      }
      await sessionManager.clearSessionAndNavigateToLogin();
      return GatewayDetailState(
        status: ViewStatus.error,
        errorMessage: "Phiên đăng nhập đã hết hạn.",
      );

    } on NotFoundException catch (e) {
      // Xử lý 404: Cơ chế bảo vệ riêng tư (Anti-Enumeration)
      return GatewayDetailState(
        status: ViewStatus.error,
        errorMessage: "Không tìm thấy tài nguyên hoặc bạn không có quyền truy cập.",
      );

    } on ServiceUnavailableException catch (e) {
      // Xử lý 503: Lỗi dịch vụ quá tải hoặc gián đoạn
      return GatewayDetailState(
        status: ViewStatus.error,
        errorMessage: "Hệ thống tạm thời bận. Vui lòng bấm thử lại.",
        requestId: e.requestId,
      );

    } on ServerException catch (e) {
      // Xử lý 500: Lỗi máy chủ an toàn kèm mã hỗ trợ kỹ thuật
      return GatewayDetailState(
        status: ViewStatus.error,
        errorMessage: "Đã xảy ra sự cố hệ thống. Mã yêu cầu: ${e.requestId ?? 'N/A'}",
        requestId: e.requestId,
      );
    }
  }
}
```

---

## 11. Ranh giới Tính năng Chưa triển khai (Future Roadmap Features Boundary)

Nhằm đảm bảo sự nhất quán với toàn bộ tài liệu hệ thống và phạm vi kết thúc Giai đoạn 2 (Stage 2 Boundary), các tính năng sau đây **được xác nhận là chưa được kích hoạt**:

1. **WebSocket Truyền phát thời gian thực (`GET /v1/ws`):** Chưa sẵn sàng cho Client; API hiện tại phản hồi `501 Not Implemented`.
2. **Truy vấn Dữ liệu lịch sử Telemetry (`GET /v1/telemetry/history`):** Chưa sẵn sàng; phản hồi `501 Not Implemented`.
3. **Mô hình Bản sao số Digital Twin (`GET /v1/digital-twins`):** Chưa sẵn sàng; phản hồi `501 Not Implemented`.
4. **Tải lên Tệp Đa phương tiện (Media / Supabase Storage Upload):** Chưa khả dụng cho Client trong giai đoạn này.
5. **Lệnh Điều khiển Thiết bị (Downlink Commands & Desired State Reconciliation):** Chưa được kích hoạt tại tầng giao tiếp người dùng.

Client tuyệt đối không xây dựng logic giả lập các luồng dữ liệu trên cho đến khi các Giai đoạn phát triển tiếp theo được nghiệm thu chính thức.
