# Tài liệu Client — 01: Tổng quan và Kiến trúc ứng dụng Client (Flutter)

**Cập nhật:** 2026-10-18
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:** `AGENTS.md`, `docs/backend/stage-2-task-2.2-authentication.md`, `docs/backend/stage-2-task-2.3-authorization.md`, `docs/backend/stage-2-task-2.4-provisioning.md`, `docs/backend/stage-2-task-2.6-acceptance.md`, `docs/backend/stage-2-task-2.7-acceptance.md`, `config/nginx/nginx.conf.template`, `docker-compose.yml`.
**Bộ tài liệu Client liên quan:**
- [`02_AUTHENTICATION_AND_SESSION.md`](02_AUTHENTICATION_AND_SESSION.md): Cơ chế xác thực, quản lý phiên và xử lý JWT.
- [`03_USER_GATEWAY_AUTHORIZATION.md`](03_USER_GATEWAY_AUTHORIZATION.md): Mô hình phân quyền User–Gateway (`owner`, `operator`, `viewer`).
- [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md): Hợp đồng REST API người dùng hiện hữu.
- [`05_UPCOMING_FEATURES_ROADMAP.md`](05_UPCOMING_FEATURES_ROADMAP.md): Phân biệt backend Stage 4 đã có với các tính năng tương lai (điều khiển, WebSocket và media).
- [`06_PROJECT_SETUP_AND_ENV.md`](06_PROJECT_SETUP_AND_ENV.md): Hướng dẫn thiết lập môi trường và cấu hình Client.
- [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md): Hợp đồng REST API quản trị nền tảng (Platform Admin) với 7 endpoint khả dụng và 1 tuyến đường reset fail-closed.
- [`08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md): Hướng dẫn tích hợp API, mô hình dữ liệu và quản lý trạng thái Client.
- [Tổng kết Stage 4](../backend/stage-4-history-and-twin-api.md) và [contract Stage 4](../protocols/stage4-read-and-entity-api.md): Nguồn đặc tả history, Twin read và metadata POST/PATCH.

**Mốc kiểm chứng backend:** GitHub Actions [run 39103564379](https://github.com/TVTIT/project1_iot_server/actions/runs/39103564379) hoàn thành 16/16 jobs tại commit `aa7436d8838a6580c21b30930e2eed992999b11c`. Đây là bằng chứng backend/CI, không phải bằng chứng Flutter UI đã được viết hoặc kiểm thử; diff tài liệu hiện tại chưa có run CI riêng.

---

## 1. Mục tiêu ứng dụng Client (Flutter)

Ứng dụng **Flutter Client** đóng vai trò là giao diện giám sát và vận hành cho hệ thống IoT Gateway–Server:

- **Đối tượng người dùng ứng dụng:**
  - **Chủ sở hữu trạm (`owner`):** Đọc danh sách trạm/cảm biến, history và Twin của trạm được gán. Có thể tạo hoặc gửi lại Sensor trùng khớp qua PUT Stage 2 hoặc `POST /v1/digital-twins`, đổi tên Sensor qua `PATCH /v1/digital-twins/{entity_id}`. Không được sửa Gateway, đổi `unit`, xóa Sensor hoặc ghi cấu hình thiết bị (`desired_state`)/gửi lệnh. Xem [contract REST](04_REST_API_CLIENT_CONTRACT.md).
  - **Người vận hành kỹ thuật (`operator`):** Giám sát danh sách trạm và danh sách cảm biến trên trạm được giao ở chế độ hoàn toàn chỉ đọc (fail-closed; mọi yêu cầu ghi hoặc khởi tạo cảm biến đều bị từ chối với `403 Forbidden`). Các lệnh vận hành one-shot an toàn theo allowlist của máy chủ sẽ được triển khai ở giai đoạn sau.
  - **Người xem (`viewer`):** Giám sát danh sách trạm và danh sách cảm biến ở chế độ hoàn toàn chỉ đọc (Read-only); vĩnh viễn không có quyền ghi.
  - *(Lưu ý về Platform Admin):* Quản trị viên nền tảng (`platform_admins`) chịu trách nhiệm kiểm kê Gateway toàn hệ thống độc lập với quyền thành viên, cấp phát Gateway/Sensor, gán quyền thành viên và quản lý chứng thực MQTT thông qua 7 endpoint quản trị khả dụng và 1 endpoint reset fail-closed (xem [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)). Thao tác quản trị chủ yếu sử dụng công cụ dòng lệnh được bảo vệ hoặc Supabase Studio; hệ thống **không yêu cầu** xây dựng ứng dụng Web Admin riêng biệt.
- **Định hướng nền tảng:**
   - **Nền tảng MVP là Flutter Web**, theo `AGENTS.md` mục 3 và 17. Ứng dụng di động nằm ngoài phạm vi luận văn hiện tại; API có thể tái sử dụng cho mobile sau này.
   - Trình duyệt giúp kiểm tra giao diện và request qua Developer Tools. Kiến trúc phân tầng bên dưới là gợi ý tổ chức mã nguồn, không bảo đảm tự động tương thích mọi nền tảng hoặc chứng minh client đã hoàn thiện.
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
|                     (Flutter Web cho MVP)                               |
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
    - Go Backend đã có CORS theo danh sách origin chính xác được cấu hình; preflight hợp lệ được xử lý trước JWT. Client khác origin phải dùng origin được cho phép; không dùng wildcard hoặc tắt bảo mật trình duyệt để vượt lỗi CORS. Xem [headers/CORS trong contract REST](04_REST_API_CLIENT_CONTRACT.md).

### 2.3. Nguyên tắc bảo mật trên Client

- **Tuyệt đối không nhúng `service_role` key vào mã nguồn Client:** `service_role` key là khóa quản trị tối cao của Supabase, chỉ được lưu ở môi trường backend nội bộ. Client chỉ được phép giữ `SUPABASE_ANON_KEY`.
- **Rủi ro bảo mật lưu trữ Web Storage (`localStorage`):**
  - Trên nền tảng Web, SDK `supabase_flutter` mặc định lưu trữ phiên làm việc vào `localStorage` của trình duyệt. Cần nhận thức rõ: **`localStorage` không an toàn tuyệt đối trước các cuộc tấn công XSS (Cross-Site Scripting)** nếu mã nguồn ứng dụng hoặc thư viện bên thứ ba bị chèn mã độc.
   - Nếu bổ sung Android trong tương lai, cần thiết kế lưu token bằng cơ chế bảo mật của hệ điều hành; hướng dẫn mobile không thay đổi phạm vi Flutter Web của MVP.
- **JWT hợp lệ không đồng nghĩa với có quyền truy cập:** Token JWT chỉ chứng minh danh tính người dùng (`authenticated`). Mọi quyền đọc/ghi trên từng Gateway đều do Go Backend thẩm định dựa trên bảng `user_gateways` trong PostgreSQL.
- **Không tự tin cậy dữ liệu phía client:** Mọi tham số như `user_id` không được truyền qua URL query hay request body để đòi quyền. Go Backend lấy `user_id` duy nhất từ claim `sub` của JWT đã xác minh.

---

## 3. Hiện trạng API Backend và Ma trận Endpoint (sau Stage 4)

Backend đã có các API Stage 2 và bảy API Stage 4 bên dưới. Các kết quả Stage 2 cũ là checkpoint lịch sử, không phải chứng nhận toàn hệ thống. Tính năng quản trị vòng đời chứng thực MQTT vẫn có cờ độc lập `MQTT_CREDENTIAL_API_ENABLED` (mặc định tắt); việc mở history/Twin không tự bật credential API.

Dưới đây là ma trận phân định chính xác giữa các endpoint **đã triển khai**, các endpoint **stub 501**, và các endpoint **chưa đăng ký (404)**:

| Nhóm chức năng | Phương thức & Đường dẫn | Quyền truy cập | Trạng thái hiện tại | Hành vi kỹ thuật |
|---|---|---|---|---|
| **Health & Readiness** | `GET /healthz` | Public (Không cần Auth) | **Đã triển khai** | Trả về `200 OK` (plain text `"OK"`). |
| | `GET /readyz` | Public (Không cần Auth) | **Đã triển khai** | Kiểm tra kết nối CSDL; trả `200 {"status": "ready"}` hoặc `503 {"error": "service_unavailable"}` nếu mất CSDL. |
| | `GET /v1/health` | Public (Không cần Auth) | **Đã triển khai** | Trả về `200 {"status": "running", "service": "iot-backend", "version": "v1"}`. |
| **User Gateway & Sensor** | `GET /v1/gateways` | Authenticated (JWT) | **Đã triển khai** | Trả danh sách Gateway mà user có quyền (`user_gateways`). Nếu chưa được gán trạm nào: trả `200 {"items": []}`. Chi tiết tại [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md). |
| | `GET /v1/gateways/:gateway_id/sensors` | Authenticated (JWT) | **Đã triển khai** | Trả danh sách sensor của trạm. Trả `404 Not Found` nếu trạm không tồn tại hoặc user không thuộc trạm đó. Chi tiết tại [`04_REST_API_CLIENT_CONTRACT.md`](04_REST_API_CLIENT_CONTRACT.md). |
| | `PUT /v1/gateways/:gateway_id/sensors/:sensor_id` | Gateway Owner | **Đã triển khai** | Khởi tạo mới hoặc gửi lại trùng khớp Sensor (`201`/`200`); payload khác trả `409 Conflict`, không dùng PUT để đổi tên. Owner còn có POST/PATCH Sensor Stage 4 như bảng dưới. Chi tiết tại [04](04_REST_API_CLIENT_CONTRACT.md). |
| **Admin Inventory** | `GET /v1/admin/gateways` | Platform Admin | **Đã triển khai** | Danh mục kiểm kê Gateway toàn hệ thống độc lập với tư cách thành viên `user_gateways`, hỗ trợ phân trang keyset cursor (`limit`, `after`). Non-admin nhận `403`. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Provisioning** | `PUT /v1/admin/gateways/:gateway_id` | Platform Admin | **Đã triển khai** | Tạo Gateway (`201`) hoặc replay payload trùng khớp (`200`); thay đổi bản ghi hiện hữu trả `409`, không phải API cập nhật. Non-admin nhận `403`. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id` | Platform Admin | **Đã triển khai** | Tạo Sensor (`201`) hoặc replay trùng khớp (`200`); payload khác trả `409`. Non-admin nhận `403`. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin MQTT Credential** | `GET /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Lấy metadata chứng thực MQTT. Trả về `503 credential_runtime_disabled` khi tắt cờ triển khai (`MQTT_CREDENTIAL_API_ENABLED=false`). Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `POST /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Cấp mới chứng thực MQTT (trả secret 1 lần duy nhất). Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `POST /v1/admin/gateways/:gateway_id/mqtt-credential/rotate` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Xoay vòng chứng thực MQTT (trả secret mới 1 lần). Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `DELETE /v1/admin/gateways/:gateway_id/mqtt-credential` | Platform Admin | **Đã triển khai trong code** *(Mặc định TẮT)* | Vô hiệu hóa/thu hồi chứng thực MQTT của trạm (ngắt kết nối socket, không cấp lại secret tự động). Trả về `503 credential_runtime_disabled` khi tắt cờ. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| | `POST /v1/admin/gateways/:gateway_id/mqtt-credential/reset` | Platform Admin | **Đã đăng ký route (Fail-closed)** | Tiếp nhận yêu cầu reset có giám sát kèm lý do (`reason`), nhưng runtime prerequisite `denyOnlyResetPrerequisite{}` luôn fail-closed trả `503` (`service_unavailable` hoặc `credential_runtime_disabled`). Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Stage 4 History** | `GET /v1/telemetry/history` | JWT + membership | **Đã triển khai** | Raw/bucket/auto, khoảng `[from,to)` và số điểm hữu hạn; chi tiết request/DTO tại [04](04_REST_API_CLIENT_CONTRACT.md). |
| **Stage 4 Twin Read** | `GET /v1/digital-twins` | JWT + membership | **Đã triển khai** | Danh sách keyset, chỉ gồm tài nguyên được gán; rỗng hợp lệ trả 200. |
| | `GET /v1/digital-twins/{entity_id}` | JWT + membership | **Đã triển khai** | Detail JSON-LD với inline context và relationship page. |
| | `GET /v1/digital-twins/{entity_id}/state` | JWT + membership | **Đã triển khai** | Current reported/desired và versions, đọc từ `twin_states`; không phải API ghi desired. |
| | `GET /v1/digital-twins/{entity_id}/history` | JWT + membership | **Đã triển khai** | Sensor selectors `value` và `sample_interval_us`; trả JSON, không phải JSON-LD detail. |
| **Stage 4 Metadata** | `POST /v1/digital-twins` | Gateway: admin; Sensor: admin/owner | **Đã triển khai** | 201 mới, 200 exact replay, 409 conflict; không tạo Auth account hoặc MQTT credential. |
| | `PATCH /v1/digital-twins/{entity_id}` | Gateway: admin; Sensor: admin/owner | **Đã triển khai** | Gateway name/description, Sensor name; unit immutable, không sửa state/history/commands. |
| **Authenticated Stub** | `GET /v1/ws` | Authenticated (JWT) | **Stub 501** | WebSocket thuộc Stage 7, chưa có streaming nghiệp vụ. |
| **Chưa đăng ký (Unregistered)** | `GET /v1/telemetry/ws` | Bất kỳ | **Chưa đăng ký (404)** | Không có route trong Go backend. Gin NoRoute trả về `404 Not Found`. |
| | `GET /v1/media/*` hoặc `/v1/gateways/:id/media` | Bất kỳ | **Chưa đăng ký (404)** | Chưa có Go Media API. Supabase Storage tồn tại ở mức hạ tầng, chưa mở qua nghiệp vụ Go. |
| | Mọi route khác ngoài bảng trên | Bất kỳ | **Chưa đăng ký (404)** | Trả về `404 Not Found` (`{"error": "not_found", "message": "resource not found"}`). |

> **Ghi chú về Công cụ Quản trị Nền tảng:**
> Hệ thống áp dụng chính sách quản trị tập trung. Các thao tác quản trị nền tảng (gán quyền membership, cấp phát Gateway, xoay vòng chứng thực) được thực thi thông qua công cụ dòng lệnh được bảo vệ (`scripts/manage-gateway-membership.sh`, `scripts/bootstrap-platform-admin.sh`) hoặc Supabase Studio. Các API `/v1/admin/*` là giao diện lập trình tùy chọn dành cho quản trị viên, **không bắt buộc xây dựng giao diện người dùng Web Admin riêng**. Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md).

**Cách hiểu quyền Stage 4:** Owner/operator/viewer đều đọc được tài nguyên của Gateway được gán; platform admin không tự bypass membership khi đọc. Non-member nhận 404 ở read detail/state/history và Sensor mutation; Gateway mutation từ non-admin nhận 403. List vẫn trả 200 nhưng lọc theo membership. Với request hợp lệ, operator/viewer bị cấm metadata mutation. Bảy paths Stage 4 dùng `Cache-Control: no-store`; POST/PATCH có thêm `Pragma: no-cache`. Mã lỗi là envelope JSON có `code`, `message`, `request_id`; không đọc ví dụ status trong bảng như JSON schema đầy đủ.

---

## 4. Các tính năng ngoài phạm vi (Deferred) hoặc chờ giai đoạn sau

Căn cứ `AGENTS.md` (Mục 2 và Mục 6.5), các tính năng sau **không** thuộc phạm vi triển khai của Client ở giai đoạn hiện tại:

1. **Không có chức năng Đăng ký tài khoản công khai (Public Self-Signup Disabled):**
   - Hệ thống áp dụng chính sách quản trị tập trung (`GOTRUE_DISABLE_SIGNUP=true`). Tài khoản người dùng được cấp phát độc quyền bởi Platform Administrator qua Supabase Studio hoặc công cụ quản trị nội bộ.
   - Giao diện Client **tuyệt đối không có nút hoặc form "Đăng ký" (Sign Up)**.
2. **Không có tính năng Tự nhận Gateway (No Gateway Self-Claiming):**
   - Người dùng không thể tự nhập mã Gateway để chiếm quyền sở hữu. Quan hệ sở hữu và vận hành (`user_gateways`) do Platform Administrator thiết lập tập trung.
3. **Chưa triển khai lệnh điều khiển hai chiều (Device Downlink Control Deferred):**
    - Ghi `desired_state`, command/outbox và MQTT execution feedback thuộc **Stage 5**; timeout/retry/reconciliation thuộc **Stage 6**. Hiện đọc desired trong `/state` và PATCH metadata không kích hoạt thiết bị. WebSocket thuộc Stage 7; media nghiệp vụ thuộc Stage 8.
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
> - Khi chạy **Flutter Web cục bộ (MVP)**, địa chỉ trỏ về Nginx có thể là `http://localhost/v1` và `http://localhost`, tùy cấu hình port; origin của UI phải được cấu hình CORS phù hợp.
> - **Tham khảo ngoài phạm vi MVP:** Android Emulator dùng `http://10.0.2.2/v1` và `http://10.0.2.2` để trỏ về host, nếu sau này thử client Android.
> - Khi chạy trên **Thiết bị thật qua mạng LAN**, sử dụng địa chỉ IP nội bộ của máy host, ví dụ `http://192.168.1.x/v1`.
> - Khi chạy trên **Production**, sử dụng domain định tuyến qua Nginx/Cloudflare, ví dụ `https://api.example.com/v1`.
>
> Chi tiết hướng dẫn thiết lập biến môi trường xem tại [`06_PROJECT_SETUP_AND_ENV.md`](06_PROJECT_SETUP_AND_ENV.md).

---

## 6. Kiến trúc phân tầng mã nguồn Client đề xuất

Để mã Flutter Web dễ bảo trì và kiểm thử, có thể tách trách nhiệm theo các tầng dưới đây. Đây là **kiến trúc đề xuất**, không bắt buộc thêm framework phức tạp hoặc chứng minh mã client đã tồn tại:

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
    - **Screens/Pages gợi ý:** Đăng nhập, danh sách Gateway/Sensor hoặc Twin, Twin detail/current và biểu đồ history.
   - **Widgets:** `GatewayCard`, `SensorListItem`, `RoleBadge`, `ErrorStateView`, `LoadingIndicator`.
   - Lớp này không chứa business logic hay gọi HTTP trực tiếp; chỉ phát sự kiện và lắng nghe trạng thái từ BLoC/Cubit để vẽ giao diện.
2. **State Management Layer (Tầng Quản lý Trạng thái):**
   - Khuyến nghị sử dụng `flutter_bloc` (hoặc Cubit cho các luồng đơn giản), hoạt động độc lập với nền tảng hiển thị.
   - Chi tiết về mô hình trạng thái, quản lý vòng đời dữ liệu và luồng xử lý lỗi được trình bày cụ thể tại [`08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).
3. **Domain & Repository Layer (Tầng Nghiệp vụ & Kho dữ liệu):**
    - **Models / Entities gợi ý:** User, Gateway, Sensor, Twin list/detail/state và history raw/bucket. Mapper phải theo [contract REST](04_REST_API_CLIENT_CONTRACT.md), phân biệt decimal strings (sequence/count/versions) với numeric `point_count`, nullable fields và cursor. JSONB được Go bảo toàn trên wire nhưng JSON decoder của Flutter Web vẫn có thể làm mất độ chính xác số lớn; không suy rằng `fromJson` mặc định tự giải quyết việc này.
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
4. **History và Twin:** Với membership hợp lệ, kiểm read 200/authorized-empty, raw/bucket, pagination và lỗi query/overflow400; thử cross-Gateway404. Metadata phải đúng admin/owner, không cho operator/viewer ghi. Kiểm503 và bỏ response cũ khi người dùng đổi resource/filter; chỉ WebSocket stub còn501 trong các routes đã đăng ký nêu trên.
5. **Phân biệt kế hoạch test client với bằng chứng backend:** Các mục trên là checklist dự kiến cho Flutter Web, không phải client tests đã chạy. Backend evidence hiện tại gồm local/native và remote CI16/16 tại aa7436d; xem [tổng kết Stage 4](../backend/stage-4-history-and-twin-api.md). Không suy CI xanh thành UI hoàn thiện, board runtime hoặc production readiness.
