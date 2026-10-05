# Tài liệu Client — 01: Tổng quan và Kiến trúc ứng dụng Client (Flutter)

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:** `AGENTS.md`, `docs/backend/stage-2-task-2.2-authentication.md`, `docs/backend/stage-2-task-2.3-authorization.md`, `docs/backend/stage-2-task-2.4-provisioning.md`, `docs/backend/stage-2-task-2.6-acceptance.md`, `docs/backend/stage-2-task-2.7-acceptance.md`, `config/nginx/nginx.conf.template`, `docker-compose.yml`.
**Bộ tài liệu Client liên quan:**
- [`02_AUTHENTICATION_AND_SESSION.md`](02_AUTHENTICATION_AND_SESSION.md): Cơ chế xác thực, quản lý phiên và xử lý JWT.
- [`03_USER_GATEWAY_AUTHORIZATION.md`](03_USER_GATEWAY_AUTHORIZATION.md): Mô hình phân quyền User–Gateway (`owner`, `operator`, `viewer`).
- [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md): Hợp đồng REST API người dùng hiện hữu.
- [`05_UPCOMING_FEATURES_ROADMAP.md`](05_UPCOMING_FEATURES_ROADMAP.md): Lộ trình tính năng Giai đoạn 3 (Telemetry, History, WebSocket, Digital Twin).
- [`06_PROJECT_SETUP_AND_ENV.md`](06_PROJECT_SETUP_AND_ENV.md): Hướng dẫn thiết lập môi trường và cấu hình Client.
- [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md): Hợp đồng REST API quản trị nền tảng (Platform Admin) và chính sách công cụ quản trị.
- [`08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md): Hướng dẫn tích hợp API, mô hình dữ liệu và quản lý trạng thái Client.

---

## 1. Mục tiêu ứng dụng Client (Flutter)

Ứng dụng **Flutter Client** đóng vai trò là giao diện giám sát và vận hành cho hệ thống IoT Gateway–Server:

- **Đối tượng người dùng ứng dụng:**
  - **Chủ sở hữu trạm (`owner`):** Theo dõi trạng thái hoạt động, thông số cảm biến và cấu hình thiết bị thuộc quyền quản lý của mình trên trạm được gán.
  - **Người vận hành kỹ thuật (`operator`):** Giám sát trạng thái hoạt động tức thời trên trạm được giao. (Trong Giai đoạn 2, vai trò này tuân thủ nguyên tắc *fail-closed* chỉ đọc; các lệnh vận hành one-shot an toàn theo allowlist của máy chủ sẽ được triển khai ở giai đoạn sau).
  - **Người xem (`viewer`):** Giám sát dữ liệu đo và trạng thái trạm ở chế độ chỉ đọc (Read-only).
  - *(Lưu ý về Platform Admin):* Quản trị viên nền tảng (`platform_admins`) chịu trách nhiệm cấp phát Gateway/Sensor, gán quyền thành viên và quản lý chứng thực. Thao tác quản trị chủ yếu sử dụng công cụ dòng lệnh được bảo vệ hoặc Supabase Studio; hệ thống **không yêu cầu** xây dựng ứng dụng Web Admin riêng biệt (xem [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)).
- **Định hướng nền tảng:**
  - **Nền tảng mục tiêu chuẩn theo `AGENTS.md`:** Căn cứ `AGENTS.md` (Mục 3 và Mục 17), nền tảng mục tiêu mặc định cho ứng dụng MVP là **di động (ưu tiên Android)** nhằm phục vụ mục đích kiểm thử và demo đồ án gọn nhẹ, tin cậy.
  - **Vai trò của Web trong quá trình phát triển:** Việc chạy thử nghiệm trên trình duyệt Web (Flutter Web) trong môi trường phát triển cục bộ là giải pháp hỗ trợ sinh viên kiểm thử nhanh giao diện và theo dõi network qua Developer Tools. Thiết kế kiến trúc phân tầng (Clean Architecture) bảo đảm mã nguồn dùng chung không bị phụ thuộc vào môi trường Web và sẵn sàng đóng gói trực tiếp sang ứng dụng di động Android mà không phải sửa đổi tầng logic cốt lõi.
- **Ranh giới hiện trạng triển khai:**
  - Mọi sơ đồ màn hình, BLoC/Cubit và cấu trúc thư mục `lib/` trong tài liệu này là **hướng dẫn thiết kế kiến trúc mục tiêu (guidance / target architecture)**, không phải bằng chứng là mã nguồn Client UI đã hoàn thành triển khai.
  - Trọng tâm tài liệu này là xác lập ranh giới mạng, ranh giới bảo mật và ma trận endpoint Backend đang thực sự hoạt động để phục vụ tích hợp Client.

---

## 2. Kiến trúc hệ thống và Luồng mạng (Network Topology)

### 2.1. Sơ đồ tổng thể

Mọi yêu cầu mạng từ Client đi vào hạ tầng thông qua **Nginx Reverse Proxy** (chạy tại cổng 80 nội bộ, đứng sau Cloudflare Tunnel trong môi trường triển khai thực tế):

```text
+-------------------------------------------------------------------------+
|                          Flutter Client App                             |
|          (Android Mobile App / Trình duyệt Web khi chạy dev)            |
+------------------------------------+------------------------------------+
                                     |
                                     | HTTP / HTTPS
                                     v
+-------------------------------------------------------------------------+
|                           Nginx Reverse Proxy                           |
|                    (Port 80/443, Quản lý định tuyến)                    |
+---------+----------------+-------------------+----------------+---------+
          |                |                   |                |
          | /healthz       | /auth/v1/*        | /storage/v1/*  | /v1/*
          v                v                   v                v
+-------------------+  +-------------------+  +---------------+  +-------------------+
|    Nginx 200 OK   |  |    Supabase       |  |   Supabase    |  |    Go Backend     |
|   (Direct check)  |  |   API Gateway     |  |  API Gateway  |  | (Modular Monolith |
|                   |  |  (Envoy :8000)    |  | (Envoy :8000) |  |    Port 8080)     |
+-------------------+  +---------+---------+  +-------+-------+  +---------+---------+
                                 |                    |                    |
                                 v                    v                    v
                       +-------------------+  +---------------+  +-------------------+
                       |   Supabase Auth   |  |SupabaseStorage|  | PostgreSQL 16 +   |
                       |     (GoTrue)      |  |(Private Bucket|  |   TimescaleDB     |
                       |   (Port 9999)     |  | (Port 5000)   |  |   (Port 5432)     |
                       +-------------------+  +---------------+  +-------------------+
```

### 2.2. Chi tiết phân luồng qua Nginx

Căn cứ tệp cấu hình thực tế `config/nginx/nginx.conf.template` và `docker-compose.yml`:

1. **Xác thực người dùng (`/auth/v1/*`):**
   - Nginx chuyển tiếp tới Supabase API Gateway (`supabase_envoy:8000`), từ đó Envoy chuyển tiếp tới dịch vụ xác thực `supabase-auth` (GoTrue).
   - Client sử dụng `supabase_flutter` SDK để gọi API đăng nhập (`/auth/v1/token?grant_type=password`), nhận về cặp mã xác thực `access_token` (JWT) và `refresh_token` (xem chi tiết tại [`02_AUTHENTICATION_AND_SESSION.md`](02_AUTHENTICATION_AND_SESSION.md)).
2. **API Nghiệp vụ Go Backend (`/v1/*`):**
   - Nginx chuyển tiếp trực tiếp tới `backend:8080` (Go Backend modular monolith).
   - Yêu cầu nghiệp vụ bắt buộc đính kèm header `Authorization: Bearer <access_token>`.
   - Go Backend tự xác minh chữ ký JWT tại chỗ (HS256) và đối soát quyền truy cập tài nguyên dựa trên CSDL PostgreSQL (`user_gateways` cho người dùng thường, `platform_admins` cho quản trị viên).
3. **Lưu trữ tệp đa phương tiện (`/storage/v1/*`):**
   - Nginx chuyển tiếp tới Supabase API Gateway (Envoy) để tới dịch vụ `supabase-storage`.
   - Đây là hạ tầng lưu trữ đối tượng private (Private Bucket). Hiện tại Go Backend **chưa triển khai API nghiệp vụ media** (`/v1/media/*`), nên Client chưa có luồng lấy Signed Upload/Read URL từ Go Backend.
4. **Đường dẫn WebSocket (`/v1/ws` và `/v1/telemetry/ws`):**
   - Nginx có khối cấu hình `location ~ ^/v1/(ws|telemetry/ws)` chuyển tiếp tới Go Backend và thiết lập header `Upgrade: $http_upgrade`.
   - **Tuy nhiên, tại Go Backend:** Hiện tại route `/v1/ws` mới chỉ đăng ký dạng **stub 501 Not Implemented**, còn route `/v1/telemetry/ws` **chưa được đăng ký** trong Go router (gọi tới sẽ nhận `404 Not Found`). Do đó Client chưa thể kích hoạt kết nối WebSocket thời gian thực ở giai đoạn này.
5. **Kiểm tra trạng thái Nginx (`/healthz`):**
   - Nginx trực tiếp trả về `200 OK` (plain text) cho các bộ cân bằng tải hoặc kịch bản kiểm tra sơ bộ.
6. **Lưu ý về tệp tĩnh và CORS:**
   - Cấu hình Nginx hiện hành **không phục vụ tệp tĩnh (No static Flutter Web hosting)**. Không có khối cấu hình `root /usr/share/nginx/html;` cho giao diện Web.
   - Nginx và Go Backend hiện tại chưa xử lý CORS cho Go API `/v1/*`. Web khác origin cần proxy phát triển cùng origin hoặc cấu hình CORS được phê duyệt và kiểm tra preflight. Không tắt bảo mật trình duyệt để bỏ qua lỗi; xem `06_PROJECT_SETUP_AND_ENV.md`.

### 2.3. Nguyên tắc bảo mật trên Client

- **Tuyệt đối không nhúng `service_role` key vào mã nguồn Client:** `service_role` key là khóa quản trị tối cao của Supabase, chỉ được lưu ở môi trường backend nội bộ. Client chỉ được phép giữ `SUPABASE_ANON_KEY`.
- **Rủi ro bảo mật lưu trữ Web Storage (`localStorage`):**
  - Trên nền tảng Web, SDK `supabase_flutter` mặc định lưu trữ phiên làm việc vào `localStorage` của trình duyệt. Cần nhận thức rõ: **`localStorage` không an toàn tuyệt đối trước các cuộc tấn công XSS (Cross-Site Scripting)** nếu mã nguồn ứng dụng hoặc thư viện bên thứ ba bị chèn mã độc.
  - Trên nền tảng di động Android (nền tảng mục tiêu chính của đồ án), token cần được lưu trữ thông qua cơ chế lưu trữ bảo mật của hệ điều hành (`EncryptedSharedPreferences` / Android Keystore) thông qua các plugin an toàn như `flutter_secure_storage`.
- **JWT hợp lệ không đồng nghĩa với có quyền truy cập:** Token JWT chỉ chứng minh danh tính người dùng (`authenticated`). Mọi quyền đọc/ghi trên từng Gateway đều do Go Backend thẩm định dựa trên bảng `user_gateways` trong PostgreSQL.
- **Không tự tin cậy dữ liệu phía client:** Mọi tham số như `user_id` không được truyền qua URL query hay request body để đòi quyền. Go Backend lấy `user_id` duy nhất từ claim `sub` của JWT đã xác minh.

---

## 3. Hiện trạng API Backend và Ma trận Endpoint (Stage 2 Baseline)

Hệ thống Backend đã hoàn thành kiểm chứng tích hợp Giai đoạn 2 (Local E2E 27/27 kịch bản PASS, CI Review APPROVED, đang chờ xác nhận Remote Final SHA). Tính năng quản trị vòng đời chứng thực MQTT (cờ triển khai `MQTT_CREDENTIAL_API_ENABLED=false`, mã hóa trong Go router qua `deps.CredentialAPIEnabled`) mặc định **TẮT (disabled)** trong môi trường triển khai chuẩn.

Dưới đây là ma trận phân định chính xác giữa các endpoint **đã triển khai**, các endpoint **stub 501**, và các endpoint **chưa đăng ký (404)**:

| Nhóm chức năng | Phương thức & Đường dẫn | Quyền truy cập | Trạng thái hiện tại | Hành vi kỹ thuật |
|---|---|---|---|---|
| **Health & Readiness** | `GET /healthz` | Public (Không cần Auth) | **Đã triển khai** | Trả về `200 OK` (plain text `"OK"`). |
| | `GET /readyz` | Public (Không cần Auth) | **Đã triển khai** | Kiểm tra kết nối CSDL; trả `200 {"status": "ready"}` hoặc `503 {"error": "service_unavailable"}` nếu mất CSDL. |
| | `GET /v1/health` | Public (Không cần Auth) | **Đã triển khai** | Trả về `200 {"status": "running", "service": "iot-backend", "version": "v1"}`. |
| **User Gateway & Sensor** | `GET /v1/gateways` | Authenticated (JWT) | **Đã triển khai** | Trả danh sách Gateway mà user có quyền (`user_gateways`). Nếu chưa được gán trạm nào: trả `200 {"items": []}`. Chi tiết tại [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md). |
| | `GET /v1/gateways/:gateway_id/sensors` | Authenticated (JWT) | **Đã triển khai** | Trả danh sách sensor của trạm. Trả `404 Not Found` nếu trạm không tồn tại hoặc user không thuộc trạm đó. Chi tiết tại [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md). |
| **Admin Provisioning** | `PUT /v1/admin/gateways/:gateway_id` | Platform Admin | **Đã triển khai** | Tạo Gateway (`201`) hoặc replay payload trùng khớp (`200`); thay đổi bản ghi hiện hữu trả `409`, không phải API cập nhật. Non-admin nhận `403`. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id` | Platform Admin | **Đã triển khai** | Tạo Sensor (`201`) hoặc replay trùng khớp (`200`); payload khác trả `409`. Non-admin nhận `403`. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin MQTT Credential** | `GET /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Lấy metadata chứng thực MQTT. Trả về `503 credential_runtime_disabled` khi tắt cờ triển khai (`MQTT_CREDENTIAL_API_ENABLED=false`). Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `POST /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Cấp mới chứng thực MQTT (trả secret 1 lần duy nhất). Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `POST /v1/admin/gateways/:gateway_id/mqtt-credential/rotate` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Xoay vòng chứng thực MQTT (trả secret mới 1 lần). Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `DELETE /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Thu hồi chứng thực MQTT của trạm. Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Authenticated Stubs** | `GET /v1/telemetry/history` | Authenticated (JWT) | **Stub 501** | Trả về `501 Not Implemented` (`{"error": "not_implemented", "message": "endpoint not implemented"}`). Lộ trình tại [`05_UPCOMING_FEATURES_ROADMAP.md`](05_UPCOMING_FEATURES_ROADMAP.md). |
| | `GET /v1/ws` | Authenticated (JWT) | **Stub 501** | Trả về `501 Not Implemented` (`{"error": "not_implemented", "message": "endpoint not implemented"}`). Lộ trình tại [`05_UPCOMING_FEATURES_ROADMAP.md`](05_UPCOMING_FEATURES_ROADMAP.md). |
| | `GET /v1/digital-twins` | Authenticated (JWT) | **Stub 501** | Trả về `501 Not Implemented` (`{"error": "not_implemented", "message": "endpoint not implemented"}`). Lộ trình tại [`05_UPCOMING_FEATURES_ROADMAP.md`](05_UPCOMING_FEATURES_ROADMAP.md). |
| **Chưa đăng ký (Unregistered)** | `GET /v1/telemetry/ws` | Bất kỳ | **Chưa đăng ký (404)** | Không có route trong Go backend. Gin NoRoute trả về `404 Not Found`. |
| | `GET /v1/media/*` hoặc `/v1/gateways/:id/media` | Bất kỳ | **Chưa đăng ký (404)** | Chưa có Go Media API. Supabase Storage tồn tại ở mức hạ tầng, chưa mở qua nghiệp vụ Go. |
| | Mọi route khác ngoài bảng trên | Bất kỳ | **Chưa đăng ký (404)** | Trả về `404 Not Found` (`{"error": "not_found", "message": "resource not found"}`). |

> **Ghi chú về Công cụ Quản trị Nền tảng:**
> Hệ thống áp dụng chính sách quản trị tập trung. Các thao tác quản trị nền tảng (gán quyền membership, cấp phát Gateway, xoay vòng chứng thực) được thực thi thông qua công cụ dòng lệnh được bảo vệ (`scripts/manage-gateway-membership.sh`, `scripts/bootstrap-platform-admin.sh`) hoặc Supabase Studio. Các API `/v1/admin/*` là giao diện lập trình tùy chọn dành cho quản trị viên, **không bắt buộc xây dựng giao diện người dùng Web Admin riêng**. Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md).

---

## 4. Các tính năng ngoài phạm vi (Deferred) hoặc chờ giai đoạn sau

Căn cứ `AGENTS.md` (Mục 2 và Mục 6.5), các tính năng sau **không** thuộc phạm vi triển khai của Client ở giai đoạn hiện tại:

1. **Không có chức năng Đăng ký tài khoản công khai (Public Self-Signup Disabled):**
   - Hệ thống áp dụng chính sách quản trị tập trung (`GOTRUE_DISABLE_SIGNUP=true`). Tài khoản người dùng được cấp phát độc quyền bởi Platform Administrator qua Supabase Studio hoặc công cụ quản trị nội bộ.
   - Giao diện Client **tuyệt đối không có nút hoặc form "Đăng ký" (Sign Up)**.
2. **Không có tính năng Tự nhận Gateway (No Gateway Self-Claiming):**
   - Người dùng không thể tự nhập mã Gateway để chiếm quyền sở hữu. Quan hệ sở hữu và vận hành (`user_gateways`) do Platform Administrator thiết lập tập trung.
3. **Chưa triển khai lệnh điều khiển hai chiều (Device Downlink Control Deferred):**
   - Chưa tích hợp gửi lệnh cấu hình `desired_state` hay các lệnh one-shot (reboot, capture-image) trên giao diện Client. Phân hệ Digital Twin Command & Transactional Outbox thuộc Giai đoạn 3.
4. **Không phát video trực tiếp (No Live-streaming / No Media Server):**
   - Hệ thống không hỗ trợ WebRTC, RTSP hay HLS streaming. Phân hệ Media chỉ phục vụ ảnh tĩnh chụp theo sự kiện khi phân hệ media backend được hoàn thiện.
5. **Không dùng gRPC hay gRPC-Web:**
   - Client giao tiếp thuần túy qua REST (JSON) và WebSocket chuẩn khi được kích hoạt.
6. **Không xây dựng ứng dụng Web Admin riêng biệt:**
   - Việc quản trị nền tảng không yêu cầu giao diện Web Admin; Platform Admin sử dụng công cụ quản trị nội bộ đã được kiểm chứng.

---

## 5. Cấu hình môi trường Client (.env)

### 5.1. Các biến môi trường bắt buộc (Sử dụng Placeholder an toàn)

Ứng dụng Client sử dụng cấu hình biến môi trường dạng placeholder (tuyệt đối không hardcode bí mật, token thật hay đường dẫn máy cá nhân):

```ini
# Base URL của Go Backend REST API (tiền tố /v1)
# Kết nối qua Nginx reverse proxy tại cổng 80 của môi trường triển khai
BACKEND_API_BASE_URL=http://localhost/v1

# URL của Supabase API Gateway (phục vụ Auth & Storage)
SUPABASE_URL=http://localhost

# Khóa công khai của Supabase (Anonymous Key - an toàn khi nhúng Client)
# Sử dụng giá trị placeholder an toàn; thay thế bằng khóa thật từ môi trường triển khai
SUPABASE_ANON_KEY=your-supabase-anon-key-placeholder

# WebSocket URL phục vụ dữ liệu thời gian thực (hiện đang stub 501 tại backend)
BACKEND_WS_URL=ws://localhost/v1/ws
```

> **Lưu ý quan trọng về địa chỉ mạng:**
>
> - Khi chạy trên **Android Emulator**, địa chỉ trỏ về máy host chạy Docker/Nginx là `http://10.0.2.2/v1` và `http://10.0.2.2`.
> - Khi chạy trên **Trình duyệt Web (Local Dev)**, địa chỉ trỏ về Nginx là `http://localhost/v1` và `http://localhost`.
> - Khi chạy trên **Thiết bị thật qua mạng LAN**, sử dụng địa chỉ IP nội bộ của máy host, ví dụ `http://192.168.1.x/v1`.
> - Khi chạy trên **Production**, sử dụng domain định tuyến qua Nginx/Cloudflare, ví dụ `https://api.example.com/v1`.
>
> Chi tiết hướng dẫn thiết lập biến môi trường xem tại [`06_PROJECT_SETUP_AND_ENV.md`](06_PROJECT_SETUP_AND_ENV.md).

---

## 6. Kiến trúc phân tầng mã nguồn Client đề xuất

Để bảo đảm mã nguồn dễ bảo trì, dễ viết kiểm thử đơn vị (Unit Test) và giữ khả năng đóng gói đa nền tảng (trực tiếp lên Android và hỗ trợ Web khi cần), kiến trúc phân tầng đề xuất cho Client được tổ chức theo mô hình **Clean Architecture thực dụng**:

```text
+-------------------------------------------------------------------------+
|                           Presentation Layer                            |
|             (Screens / Pages, Reusable Widgets, Responsive Views)       |
+------------------------------------+------------------------------------+
                                     |
                                     | Dispatches Events / Observes States
                                     v
+-------------------------------------------------------------------------+
|                         State Management Layer                          |
|                     (BLoC / Cubit - flutter_bloc)                       |
+------------------------------------+------------------------------------+
                                     |
                                     | Calls Use Cases / Repositories
                                     v
+-------------------------------------------------------------------------+
|                        Domain & Repository Layer                        |
|       (Entity Models, Repository Interfaces & Implementations)          |
+------------------------------------+------------------------------------+
                                     |
                                     | Invokes Remote Calls / Storage
                                     v
+-------------------------------------------------------------------------+
|                            Data Source Layer                            |
|    (Supabase Client SDK, Go Backend HTTP Client, Secure Storage)        |
+-------------------------------------------------------------------------+
```

### 6.1. Chi tiết các tầng

1. **Presentation Layer (Tầng Giao diện):**
   - **Screens/Pages:** `LoginScreen`, `GatewayListScreen`, `SensorListScreen`.
   - **Widgets:** `GatewayCard`, `SensorListItem`, `RoleBadge`, `ErrorStateView`, `LoadingIndicator`.
   - Lớp này không chứa business logic hay gọi HTTP trực tiếp; chỉ phát sự kiện và lắng nghe trạng thái từ BLoC/Cubit để vẽ giao diện.
2. **State Management Layer (Tầng Quản lý Trạng thái):**
   - Khuyến nghị sử dụng `flutter_bloc` (hoặc Cubit cho các luồng đơn giản), hoạt động độc lập với nền tảng hiển thị.
   - Chi tiết về mô hình trạng thái, quản lý vòng đời dữ liệu và luồng xử lý lỗi được trình bày cụ thể tại [`08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).
3. **Domain & Repository Layer (Tầng Nghiệp vụ & Kho dữ liệu):**
   - **Models / Entities:** `UserModel`, `GatewayModel`, `SensorModel`. Các model có phương thức chuyển đổi `fromJson` khớp chính xác với JSON contract của Backend (xem [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md)).
   - **Repositories:** Chịu trách nhiệm tương tác Data Source và chuyển đổi mã lỗi HTTP thành Domain Exceptions (`UnauthorizedException`, `ResourceNotFoundException`, `ServerException`).
4. **Data Source Layer (Tầng Nguồn dữ liệu & Lưu trữ an toàn):**
   - **`SupabaseAuthDataSource`:** Đóng gói tương tác với `Supabase.instance.client.auth` (xem [`02_AUTHENTICATION_AND_SESSION.md`](02_AUTHENTICATION_AND_SESSION.md)).
   - **`BackendApiClient`:** Đóng gói HTTP client (sử dụng thư viện `dio` hoặc `http`). Cấu hình tự động chèn header `Authorization: Bearer <token>`.
   - **`SecureStorageService`:** Trừu tượng hóa lưu trữ khóa/token an toàn (sử dụng Keystore/EncryptedSharedPreferences trên Android và Web Storage có cảnh báo XSS trên Web).

### 6.2. Cấu trúc thư mục mã nguồn đề xuất (`lib/`)

```text
lib/
├── main.dart                   # Điểm khởi chạy ứng dụng, nạp biến môi trường, cấu hình DI/BlocProviders
├── core/
│   ├── constants/              # Hằng số API endpoints, storage keys
│   ├── errors/                 # Định nghĩa Exception và Failure chuẩn
│   ├── network/                # HTTP Client, Auth Interceptor
│   ├── storage/                # Trừu tượng hóa SecureStorage / LocalStorage
│   ├── theme/                  # Định nghĩa màu sắc, typography
│   └── utils/                  # Format ngày tháng (RFC3339), validator
├── data/
│   ├── datasources/            # Tầng gọi API / SDK thô
│   │   ├── auth_remote_datasource.dart
│   │   └── backend_api_client.dart
│   ├── models/                 # DTOs và mapper parse JSON
│   │   ├── gateway_model.dart
│   │   └── sensor_model.dart
│   └── repositories/           # Triển khai Repository kết nối Data Source
│       ├── auth_repository_impl.dart
│       └── gateway_repository_impl.dart
├── logic/                      # Tầng State Management (BLoC / Cubit)
│   ├── auth/
│   │   ├── auth_bloc.dart
│   │   ├── auth_event.dart
│   │   └── auth_state.dart
│   ├── gateway_list/
│   │   └── gateway_list_cubit.dart
│   └── sensor_list/
│       └── sensor_list_cubit.dart
└── presentation/               # Tầng giao diện người dùng
    ├── screens/
    │   ├── auth/
    │   │   └── login_screen.dart
    │   ├── gateway/
    │   │   ├── gateway_list_screen.dart
    │   │   └── gateway_detail_screen.dart
    │   └── sensor/
    │       └── sensor_list_screen.dart
    └── widgets/
        ├── common_button.dart
        ├── error_banner.dart
        ├── loading_overlay.dart
        └── role_badge.dart
```

---

## 7. Tóm tắt kế hoạch kiểm thử và nghiệm thu Client

1. **Kiểm thử xác thực:** Đăng nhập thành công với tài khoản được cấp trước; kiểm tra xử lý mật khẩu sai; xác nhận token được lưu trữ an toàn và tự động đính kèm vào header các request `/v1/*`.
2. **Kiểm thử đọc dữ liệu phân quyền:**
   - Tài khoản có quyền trên Gateway A: Hiển thị đúng Gateway A và danh sách Sensor của Gateway A.
   - Tài khoản không có quyền trên Gateway B: Không thấy Gateway B; nếu gọi trực tiếp ID của Gateway B thì nhận `404 Not Found`.
   - Tài khoản chưa gán Gateway: Hiển thị danh sách rỗng (`items: []`), không crash.
3. **Kiểm thử xử lý mất phiên (Token Expiry):** Khi access token hết hạn và không thể làm mới, ứng dụng điều hướng an toàn về màn hình đăng nhập.
4. **Kiểm thử mã phản hồi đặc biệt:** Ứng dụng xử lý đúng phản hồi `501 Not Implemented` khi người dùng truy cập các tính năng thuộc giai đoạn sau (lịch sử đo, telemetry stream) với thông báo phù hợp thay vì báo lỗi mất kết nối mạng.
5. **Đồng bộ với hiện trạng kiểm thử Backend:** Phân hệ Backend đã vượt qua 27/27 kịch bản E2E kiểm chứng chuỗi xác thực, phân quyền và cô lập dữ liệu. Các kịch bản kiểm thử Client cần bám sát các trường hợp kiểm thử này để bảo đảm tính nhất quán toàn hệ thống.
