# Tài liệu Client — 04: Quy chuẩn Hợp đồng REST API (REST API Client Contract) trên Flutter Web

**Cập nhật:** 2026-10-18
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 5, 6, 6.5, 10, 16, 18)
- `docs/protocols/stage4-read-and-entity-api.md` (Đặc tả chi tiết protocol, request/response, validation và errors Stage 4)
- `docs/backend/stage-4-history-and-twin-api.md` (Báo cáo kiến trúc và bàn giao Stage 4)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Phân quyền người dùng và trạm)
- `docs/client/05_UPCOMING_FEATURES_ROADMAP.md` (Lộ trình phát triển hệ thống)
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái Client)
- `src/internal/httpserver/router.go`
- `src/internal/httpserver/stage4.go`
- `src/internal/httpserver/read_handlers.go`
- `src/internal/httpserver/twin_mutation_handlers.go`
- `src/internal/httpserver/history_dto.go`
- `src/internal/httpserver/twin_dto.go`
- `src/internal/httpapi/error.go`
- `src/internal/httpapi/request_id.go`

---

## 1. Mục tiêu, Ranh giới và Chỉ mục Endpoint Hệ thống

### 1.1. Mục tiêu tài liệu

Tài liệu này định nghĩa **Hợp đồng giao tiếp REST API (REST API Client Contract)** chính thức giữa ứng dụng **Flutter Web** (bảng điều khiển Dashboard giám sát và vận hành trung tâm) và **Go Backend** trong hệ thống IoT Gateway–Server.

- **Đối tượng áp dụng:** Lập trình viên phát triển ứng dụng Flutter Web, kiểm thử viên API, và kỹ sư tích hợp hệ thống.
- **Tuyên bố tính chất mã nguồn mẫu (No Premature Client Implementation Claims):** Toàn bộ các đoạn mã Dart/Flutter trong tài liệu này là **đề xuất tham khảo kiến trúc (code suggestions / illustrative reference)**, **KHÔNG PHẢI là bằng chứng chứng minh client đã được xuất xưởng hay hoàn tất (not shipped client proof)**. Quá trình tích hợp trạng thái và kiến trúc client thực tế tuân thủ theo [`docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).
- **Nguyên tắc bảo mật thông tin:** Tài liệu tuyệt đối không chứa tài khoản/mật khẩu thực, đường dẫn máy cá nhân (personal paths), hay tên miền triển khai thực tế. Mọi định danh sử dụng placeholder an toàn (`api.example.com`, `gateway_001`, UUID mẫu).

### 1.2. Chỉ mục toàn bộ Endpoint hiện có trong Go Backend (Current Complete Endpoint Index)

Căn cứ hiện thực mã nguồn định tuyến tại `src/internal/httpserver/router.go`, toàn bộ không gian endpoint của Go Backend bao gồm:

| Nhóm Endpoint | Phương thức | Đường dẫn URL | Yêu cầu xác thực & Quyền | Trạng thái hiện tại | Mô tả chức năng |
|---|:---:|---|---|:---:|---|
| **Public Health** | `GET` | `/healthz` | Công khai (Không auth) | `200 OK` (Text plain) | Liveness probe kiểm tra tiến trình Go đang chạy (phản hồi `"OK"`). |
| **Public Health** | `GET` | `/readyz` | Công khai (Không auth) | `200 OK` / `503 Service Unavailable` | Readiness probe kiểm tra kết nối CSDL PostgreSQL (`{"status": "ready"}`). |
| **Public Health** | `GET` | `/v1/health` | Công khai (Không auth) | `200 OK` (JSON) | Versioned service health (`{"status": "running", "service": "iot-backend", "version": "v1"}`). |
| **Authenticated Read** | `GET` | `/v1/gateways` | Bearer JWT (`authenticated`) | `200 OK` | Lấy danh sách Gateway người dùng có quyền trong `user_gateways` kèm vai trò (`owner`, `operator`, `viewer`). |
| **Authenticated Read** | `GET` | `/v1/gateways/{gateway_id}/sensors` | Bearer JWT (`authenticated`) | `200 OK` / `404 Not Found` | Lấy danh sách Sensor thuộc một Gateway được phân quyền. Trả về 404 nếu không có quyền hoặc trạm không tồn tại. |
| **Authenticated Provisioning** | `PUT` | `/v1/gateways/{gateway_id}/sensors/{sensor_id}` | Bearer JWT (`authenticated`, vai trò `owner` của Gateway cha) | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo mới hoặc phát lại chính xác Sensor bởi Chủ sở hữu trạm (create-or-exact-replay). `201` tạo mới, `200` phát lại trùng khớp `name` và `unit`; `409 Conflict` nếu payload khác (không hỗ trợ cập nhật/upsert); `403` cho `operator`/`viewer`; `404` cho non-member/foreign hoặc Gateway không tồn tại; Platform Admin không bypass quyền sở hữu. Xem chi tiết tại [Mục 5.3](#53-api-3-khởi-tạo-cảm-biến-cho-chủ-sở-hữu-gateway-put-v1gatewaysgateway_idsensorssensor_id). |
| **Admin Inventory** | `GET` | `/v1/admin/gateways` | Bearer JWT + `platform_admins` | `200 OK` / `400` / `401` / `403` / `500` / `503` | Lấy danh mục kiểm kê toàn bộ Gateway trên hệ thống độc lập với tư cách thành viên, có phân trang keyset cursor (`limit`, `after`). Không trả về vai trò hay thông tin xác thực. Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Provisioning** | `PUT` | `/v1/admin/gateways/{gateway_id}` | Bearer JWT + `platform_admins` | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo mới hoặc phát lại chính xác Gateway bởi Platform Admin (create-or-exact-replay), gán `owner_user_id` ban đầu và tạo thực thể Digital Twin: `201` nếu mới tạo, `200` nếu phát lại đúng cấu hình cũ, `409 Conflict` nếu đã tồn tại nhưng gửi cấu hình khác (không upsert). Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Provisioning** | `PUT` | `/v1/admin/gateways/{gateway_id}/sensors/{sensor_id}` | Bearer JWT + `platform_admins` | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo mới hoặc phát lại chính xác Sensor bởi Platform Admin (create-or-exact-replay), tạo thực thể Digital Twin và quan hệ `hasSensor`: `201` nếu mới tạo, `200` nếu phát lại đúng cấu hình cũ; `404` nếu trạm cha không tồn tại; `409 Conflict` nếu cấu hình khác (không upsert). Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `GET` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` | `200 OK` / `400` / `401` / `403` / `404` / `503` | Truy vấn metadata chứng chỉ MQTT của Gateway (không chứa mật khẩu). Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` + `Idempotency-Key` | `201 Created` / `400` / `401` / `403` / `409` / `503` | Cấp phát chứng chỉ MQTT lần đầu; `201` kèm password 1 lần duy nhất; replay cùng key trả về `201` metadata-only (lược bỏ trường password). Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential/rotate` | Bearer JWT + `platform_admins` + `Idempotency-Key` | `200 OK` / `400` / `401` / `403` / `409` / `503` | Xoay vòng mật khẩu MQTT; `200` kèm password mới 1 lần duy nhất; replay cùng key trả về `200` metadata-only. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `DELETE` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` + `Idempotency-Key` | `200 OK` / `400` / `401` / `403` / `503` | Vô hiệu hóa và thu hồi (disable/revoke) chứng chỉ MQTT của Gateway, ngắt kết nối socket hiện hành; không có cơ chế tự động khôi phục bí mật (cho phép re-provision với Idempotency-Key mới khi cần cấp phát lại); `200` metadata-only. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential/reset` | Bearer JWT + `platform_admins` + `Idempotency-Key` | `503 Service Unavailable` / `400` / `401` / `403` / `413` / `415` | Đã đăng ký route và parser payload `reason`, nhưng runtime prerequisite `denyOnlyResetPrerequisite{}` luôn fail-closed với `503` (`service_unavailable`). Chưa phải quy trình phục hồi khả dụng. Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Telemetry History** | `GET` | `/v1/telemetry/history` | Bearer JWT (`authenticated`, thành viên trạm) | `200 OK` / `400` / `401` / `404` / `503` | Truy vấn chuỗi thời gian telemetry raw hoặc bucket qua TimescaleDB `time_bucket`. Hỗ trợ 9 độ phân giải allowlisted hoặc auto mode. Yêu cầu `gateway_id` và `sensor_id`. Non-member hoặc trạm không tồn tại trả về 404. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Read** | `GET` | `/v1/digital-twins` | Bearer JWT (`authenticated`) | `200 OK` / `400` / `401` / `503` | Danh sách thực thể Digital Twin (`Gateway`, `Sensor`) thuộc các Gateway người dùng được phân quyền, hỗ trợ phân trang keyset cursor (`v1.{k,g,t}`). Trả về `items: []` nếu không có quyền trạm nào. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Read** | `GET` | `/v1/digital-twins/{entity_id}` | Bearer JWT (`authenticated`, thành viên trạm) | `200 OK` / `400` / `401` / `404` / `503` | Chi tiết thực thể Digital Twin theo chuẩn NGSI-LD JSON-LD (`Content-Type: application/ld+json`), `@context` inline Object 1.1, phân trang quan hệ `hasSensor` (`v1.{s,r,k}`). Non-member trả về 404. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Read** | `GET` | `/v1/digital-twins/{entity_id}/state` | Bearer JWT (`authenticated`, thành viên trạm) | `200 OK` / `400` / `401` / `404` / `503` | Đọc trạng thái hiện thời trực tiếp từ `twin_states` (`reported_state`, `desired_state`, version nguyên vẹn). Không quét hypertable. Non-member trả về 404. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Read** | `GET` | `/v1/digital-twins/{entity_id}/history` | Bearer JWT (`authenticated`, thành viên trạm) | `200 OK` / `400` / `401` / `404` / `503` | Truy vấn lịch sử thuộc tính Twin của Sensor: `property=value` (ủy quyền sang telemetry history) hoặc `property=sample_interval_us` (đọc `twin_temporal_values`). Non-member trả về 404. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Mutation** | `POST` | `/v1/digital-twins` | Bearer JWT (`authenticated`: Platform Admin cho Gateway; Platform Admin hoặc Gateway Owner cho Sensor) | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo thực thể Digital Twin (create-or-exact-replay). `201` nếu mới tạo, `200` nếu phát lại chính xác cùng thuộc tính; `409` nếu xung đột thuộc tính; `403` nếu không đủ quyền. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Twin Mutation** | `PATCH` | `/v1/digital-twins/{entity_id}` | Bearer JWT (`authenticated`: Platform Admin cho Gateway; Platform Admin hoặc Gateway Owner cho Sensor) | `200 OK` / `400` / `401` / `403` / `404` / `413` / `415` / `500` / `503` | Cập nhật metadata thực thể: Gateway chỉ cho phép `name` và `description` (lưu vào bảng `gateways`, giữ nguyên `twin_entities.attributes`); Sensor chỉ cho phép `name` (chuỗi không rỗng, `unit` là bất biến). No-op trả về `200 OK` giữ nguyên timestamp. Không cập nhật state hay gửi lệnh điều khiển. Xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis). |
| **Authenticated Stub** | `GET` | `/v1/ws` | Bearer JWT (`authenticated`) | `501 Not Implemented` | Khung định tuyến WebSocket thời gian thực (Giai đoạn 7). |

### 1.3. Ranh giới phủ định rõ ràng (Explicit Negative Boundaries)

Để tránh các giả định sai lầm khi lập trình ứng dụng Client, hệ thống xác định rõ các ranh giới phủ định sau trong Go Backend:
1. **Tuyệt đối KHÔNG CÓ route đọc lẻ 1 Gateway (`GET /v1/gateways/{gateway_id}`):** Go router không cung cấp endpoint này. Ứng dụng Client lấy thông tin chi tiết của trạm thông qua danh sách trả về từ `GET /v1/gateways`.
2. **Tuyệt đối KHÔNG CÓ route đọc lẻ 1 Sensor (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`):** Go router không cung cấp endpoint này. Thông tin toàn bộ cảm biến của trạm được trả về đầy đủ qua `GET /v1/gateways/{gateway_id}/sensors`.
3. **Tuyệt đối KHÔNG CÓ route CRUD người dùng hay quản lý thành viên `user_gateways` trên Go API:** Theo chính sách quản trị tập trung (`AGENTS.md` mục 6.5), việc tạo tài khoản người dùng và gán phân quyền thành viên trạm do Platform Admin thực hiện trực tiếp thông qua công cụ quản trị Supabase / SQL, không qua Go REST API.
4. **Ranh giới endpoint cấu hình cảm biến của Gateway Owner (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`):** Đây là thao tác tạo mới hoặc phát lại chính xác (create-or-exact-replay) dành riêng cho vai trò `owner` của trạm cha. Tuyệt đối **KHÔNG PHẢI** là API cập nhật cấu hình cảm biến (không hỗ trợ thay đổi `name` hay `unit` đã tạo, vi phạm sẽ nhận `409 Conflict`), **KHÔNG PHẢI** API cấu hình thiết bị lâu bền (`desired_state`), và **KHÔNG PHẢI** lệnh điều khiển thiết bị (device commands).
5. **Ranh giới tính năng Stage 4 và các tính năng giai đoạn sau (Stage 4 vs Future Stages):** Bảy endpoint Stage 4 (lịch sử telemetry, đọc Digital Twin list/detail/state/history, tạo entity và cập nhật metadata PATCH) đã được hiện thực và kiểm thử tích hợp hoàn chỉnh (xem [Mục 5.4](#54-api-nhóm-stage-4-telemetry-history-và-digital-twin-apis)). Tuy nhiên:
   - Kênh truyền thời gian thực WebSocket (`GET /v1/ws`) hiện trả về `501 Not Implemented` (thuộc Giai đoạn 7).
   - Thiết lập cấu hình điều khiển bền vững (`desired_state`) và hàng đợi lệnh điều khiển thiết bị downlink (`twin_commands`, outbox) chưa khả dụng (thuộc Giai đoạn 5 và 6).
   - Tải lên/đọc tập tin media qua Supabase Storage chưa khả dụng (thuộc Giai đoạn 8).
   - Thao tác `PATCH /v1/digital-twins/{entity_id}` chỉ cập nhật metadata (`name`, `description`), tuyệt đối **KHÔNG** cập nhật trạng thái `desired_state` hay `reported_state`, và **KHÔNG** phát sinh lệnh điều khiển thiết bị.
   - Platform Admin không có tư cách thành viên trạm sẽ bị từ chối `404 Not Found` khi đọc dữ liệu nghiệp vụ (lịch sử telemetry, detail, state, history) của Gateway/Sensor. Admin chỉ được phép thực hiện các thao tác quản trị nền tảng hoặc mutation metadata khi có quyền.
   - Vai trò `operator` và `viewer` là **hoàn toàn chỉ đọc** đối với toàn bộ các endpoint Stage 4 (mọi nỗ lực `POST` hoặc `PATCH` đều nhận `403 Forbidden`).

---

## 2. Quy chuẩn gửi HTTP Request và Cơ chế mạng trên Web

Mọi tương tác từ Flutter Web Client tới Go Backend phải tuân thủ các quy tắc định tuyến, cơ chế bảo mật trình duyệt (CORS) và tiêu chuẩn header sau:

### 2.1. Base URL và Cơ chế mạng trên Flutter Web

Khác với ứng dụng native (Android/iOS) gửi trực tiếp các gói tin socket TCP độc lập, ứng dụng Flutter Web chạy trong môi trường bảo mật của trình duyệt (Browser Sandbox), chịu sự kiểm soát nghiêm ngặt của **Chính sách cùng nguồn gốc (Same-Origin Policy - SOP)** và cơ chế **Chia sẻ tài nguyên liên nguồn gốc (Cross-Origin Resource Sharing - CORS)**.

Toàn bộ HTTP request của ứng dụng Client **không bao giờ kết nối trực tiếp vào cổng nội bộ của Go Backend (port 8080)** mà bắt buộc phải đi qua **Nginx Reverse Proxy**.

#### A. Quy chuẩn Base URL (Đồng bộ với 01_OVERVIEW và 06_PROJECT_SETUP_AND_ENV)

Hệ thống định nghĩa hai biến môi trường Base URL chuẩn hóa:

1. **`BACKEND_API_BASE_URL` (Go REST API Base URL):**
   - **Quy chuẩn bắt buộc:** Luôn kết thúc bằng tiền tố `/v1` (ví dụ `http://localhost/v1` trên Web dev, `http://10.0.2.2/v1` trên Android Emulator, hoặc `https://api.example.com/v1` trên Production).
   - **Quy tắc ghép đường dẫn tương đối (Relative Path Joining Rule):**
      - Chuẩn hóa base thành URL kết thúc bằng `/v1/` trước khi ghép request. Ví dụ: `final base = rawBase.endsWith('/') ? rawBase : '$rawBase/';`. Với `Uri.resolve`, dùng đường dẫn tương đối không có `/` đầu; ví dụ:
       - Lấy danh sách Gateway: gọi `'gateways'` (sẽ được ghép chuẩn thành `http://localhost/v1/gateways`).
       - Lấy danh sách Sensor: gọi `'gateways/$gatewayId/sensors'` (thành `http://localhost/v1/gateways/$gatewayId/sensors`).
     - **Hai bẫy lỗi đường dẫn nghiêm trọng cần tránh:**
        - **Bẫy 1 — `Uri.resolve`:** `Uri.parse('http://localhost/v1/').resolve('/gateways')` bỏ prefix `/v1`; base không có `/` cuối cũng có thể bị xem là tên tệp khi resolve `'gateways'`. Đây là quy tắc URI, không nên quy kết mọi HTTP thư viện đều xử lý dấu `/` giống nhau.
        - **Bẫy 2 — ghép prefix hai lần:** Không nối thêm `v1/gateways` vào base đã chứa `/v1/`. Với Dio, cấu hình base đã chuẩn hóa và path `'gateways'`, rồi kiểm tra URL cuối trong test client (không log token/header).
     - *Lưu ý về danh mục tài liệu:* Mọi bảng kê và tiêu đề endpoint trong tài liệu này vẫn sử dụng định dạng tuyệt đối (ví dụ `GET /v1/gateways`) nhằm mục đích tra cứu thông tin định tuyến trên server.

2. **`SUPABASE_URL` (Supabase API Gateway Root):**
    - Trỏ tới URL public của Supabase qua Nginx (local mặc định `http://localhost`); không dùng cổng nội bộ Envoy làm endpoint client. SDK tự nối tiền tố dịch vụ. Request Auth thuần thêm `/auth/v1`; hostname và cổng thay đổi theo cấu hình môi trường.

#### B. Thực trạng CORS và Khoảng trống kỹ thuật (Declared Gap đối chiếu 06_PROJECT_SETUP_AND_ENV)

Khảo sát trực tiếp từ mã nguồn `src/internal/httpserver/router.go` và cấu hình mẫu `config/nginx/nginx.conf.template`:

1. **Go Backend chưa có CORS Middleware:**
   - Trong `router.go`, Go Backend **hoàn toàn không tích hợp CORS middleware** và không xử lý preflight request `OPTIONS` cho các route nghiệp vụ `/v1/*`.
   - Nếu nhận request `OPTIONS /v1/*`, Go router xử lý theo `router.HandleMethodNotAllowed = true` và phản hồi mã lỗi **`405 Method Not Allowed`** (`method_not_allowed`).
2. **Nginx Reverse Proxy hiện tại CHƯA cấu hình CORS cho `/v1/`:**
   - Trong file cấu hình Nginx hiện có (`config/nginx/nginx.conf.template`), khối `location /v1/` chỉ thực hiện `proxy_pass` đơn thuần, **không thêm bất kỳ header `Access-Control-Allow-*` nào**.
    - Envoy có bộ lọc CORS cho Supabase, nhưng vẫn phải kiểm tra Origin và preflight thực tế của Auth/Storage trên môi trường mục tiêu; sự hiện diện của filter không chứng minh mọi Web request hoạt động.
3. **Hiện tại CHƯA CÓ gói web tĩnh triển khai cùng domain (No Deployed Same-Origin Web Client):**
   - Hệ thống hiện chưa thiết lập việc phục vụ các tệp tĩnh đóng gói của Flutter Web (`flutter build web`) trực tiếp trên Nginx cùng domain với Go Backend.
4. **Hệ quả đối với Web Client chạy Cross-Origin & Hướng xử lý:**
   - Khi chạy ứng dụng Flutter Web ở môi trường phát triển cục bộ (`http://localhost:3000`), mọi request chứa header tùy chỉnh (`Authorization`, `X-Request-ID`) gửi sang Go Backend (`http://localhost:80` hoặc `port 8080`) sẽ kích hoạt Preflight `OPTIONS` và **bị trình duyệt chặn đứng ngay lập tức tại tầng network** do thiếu header CORS phản hồi.
   - **Kế hoạch xử lý:** Môi trường Web chạy cross-origin bắt buộc cần một cấu hình proxy trung gian có kế hoạch (Planned Proxy / Reverse Proxy phía Client cùng origin) hoặc bổ sung cấu hình CORS hợp lệ trên Nginx và xác minh thực tế (verified) trước khi vận hành.
   - **NGHIÊM CẤM:** Tuyệt đối **KHÔNG** tắt tính năng bảo mật của trình duyệt (`--disable-web-security`) và **KHÔNG** sử dụng các tiện ích mở rộng can thiệp CORS (CORS unblocker extensions). Việc này vi phạm tiêu chuẩn bảo mật, che giấu các lỗi cấu hình biên thực tế và tạo ra thói quen kiểm thử sai lệch.

### 2.2. Headers bắt buộc phía Client
Mỗi request gửi tới các endpoint nghiệp vụ `/v1/*` bắt buộc phải chứa các header sau:

| Header | Giá trị mẫu | Bắt buộc | Mục đích |
|---|---|:---:|---|
| `Authorization` | `Bearer <access_token>` | **Có** | Chứa JWT do Supabase Auth cấp khi đăng nhập. Backend xác minh chữ ký tại chỗ (Local HS256). |
| `Accept` | `application/json` | Khuyến nghị | Go handler không bắt buộc header này; client nên yêu cầu JSON. |
| `X-Request-ID` | `c4b1e6e0-2495-4e3a-97a6-6f8d38e21a41` | Khuyến nghị | Mã định danh log-safe (1-128 ký tự ASCII) phục vụ truy vết lỗi phân tán (Distributed Tracing). Backend sinh UUIDv4 ngẫu nhiên nếu thiếu hoặc không hợp lệ. |

### 2.3. Quy tắc xử lý Header `X-Request-ID` (Traceability)
Căn cứ theo hiện thực tại `src/internal/httpapi/request_id.go`:
1. **Kiểm tra tính hợp lệ và văn phạm được chấp nhận (Accepted Grammar):** Go Backend chấp nhận header `X-Request-ID` từ Client nếu thỏa mãn:
   - Request chỉ chứa duy nhất 1 header `X-Request-ID`.
   - Độ dài từ 1 đến 128 ký tự (`len(value) >= 1 && len(value) <= 128`).
   - Ký tự đầu tiên bắt buộc phải là chữ cái ASCII (`A-Z`, `a-z`) hoặc chữ số (`0-9`).
   - Các ký tự tiếp theo chỉ thuộc tập hợp: chữ cái ASCII, chữ số, dấu chấm (`.`), gạch dưới (`_`), hai chấm (`:`), hoặc gạch ngang (`-`).
   - **Lưu ý quan trọng về định dạng:** Server **KHÔNG bắt buộc** giá trị gửi lên phải là UUIDv4! Bất kỳ chuỗi định danh log-safe nào thỏa mãn văn phạm trên đều được chấp nhận nguyên vẹn (ví dụ: `req-001`, `client.trace:12345`, hoặc UUID).
2. **Cơ chế Fallback:** Nếu Client không gửi header này, hoặc giá trị gửi lên vi phạm văn phạm (chứa ký tự lạ, khoảng trắng, quá dài, hoặc gửi nhiều header trùng lặp), Go Backend sẽ **tự động sinh một chuỗi UUIDv4 ngẫu nhiên mới** để gán cho request.
3. **Phản hồi từ Backend:** Trong **MỌI** response (kể cả HTTP 200 thành công hay các response lỗi 4xx, 5xx), Go Backend luôn đính kèm header:
   ```http
   X-Request-ID: <validated_or_generated_id>
   ```
4. **Trách nhiệm của Client:** Client cần đọc header `X-Request-ID` từ response (hoặc lấy từ `error.request_id` trong payload lỗi) để ghi log cục bộ hoặc hiển thị trên giao diện thông báo khi người dùng cần hỗ trợ kỹ thuật.

### 2.4. Phạm vi thiết lập Header chống lưu bộ nhớ đệm (`Cache-Control: no-store` và `Pragma`)
Căn cứ rà soát mã nguồn thực tế tại các handler và middleware:
- **Các endpoint nghiệp vụ đọc dữ liệu và cấp phát:** Go Backend chủ động thiết lập `c.Header("Cache-Control", "no-store")` tại các handler:
  - `GET /v1/gateways` (`listGateways`)
  - `GET /v1/gateways/{gateway_id}/sensors` (`listSensors`)
  - `PUT /v1/admin/gateways/{gateway_id}` (`provisionGateway`)
  - `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}` (`provisionSensor`)
- **Phạm vi gắn thêm `Pragma: no-cache`:**
  - Tuyến đường khởi tạo cảm biến cho chủ sở hữu `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` qua `ownerSensorCacheMiddleware` (`src/internal/httpserver/owner_sensor_handlers.go`) luôn thiết lập:
    ```http
    Cache-Control: no-store
    Pragma: no-cache
    ```
  - Tuyến đường danh mục kiểm kê quản trị `GET /v1/admin/gateways` (`src/internal/httpserver/admin_gateway_handlers.go`) thiết lập `Cache-Control: no-store` và `Pragma: no-cache`.
  - Middleware `credentialCacheMiddleware` trong `src/internal/httpserver/admin_mqtt_credential_handlers.go` chạy trước xác thực và thiết lập cả hai header:
    ```http
    Cache-Control: no-store
    Pragma: no-cache
    ```
    áp dụng cho các tuyến đường quản trị chứng chỉ MQTT khớp tiền tố `/v1/admin/gateways/*/mqtt-credential`.
  - Bảy tuyến đường Stage 4 (cả đọc và ghi) đều thiết lập `Cache-Control: no-store` (các tuyến mutation `POST` và `PATCH` thiết lập thêm `Pragma: no-cache`) ngay từ đầu chu trình handler (trước khi kiểm tra lỗi hoặc phân quyền).
- **Phạm vi không áp dụng (Không quy nạp toàn bộ):** Các endpoint public health (`/healthz`, `/readyz`, `/v1/health`) và stub WebSocket `501 Not Implemented` (`/v1/ws`) **không** thiết lập header `no-store` hay `Pragma`.
- **Không tự suy diễn header cache không hỗ trợ:** Go Backend hiện tại **không** triển khai cơ chế conditional cache (như `ETag`, `If-None-Match`, `Last-Modified` hay `stale-while-revalidate`). Client không gửi các conditional header này để tránh logic dư thừa.
- **Yêu cầu đối với Client:** Không được lưu cache vĩnh viễn danh sách Gateway hay Sensor vào bộ nhớ đệm HTTP tầng trình duyệt hoặc persistent local storage. Quyền của người dùng trên trạm có thể bị Quản trị viên thay đổi hoặc thu hồi bất kỳ lúc nào trên cơ sở dữ liệu. Mọi lần mở lại màn hình Dashboard cần đọc dữ liệu mới nhất.

### 2.5. Quy tắc CẤM KỴ: Không gửi Query Parameter `user_id` (Chống giả mạo IDOR)
- **Quy tắc:** Tuyệt đối **KHÔNG** gửi tham số `user_id` dưới dạng URL query parameter (ví dụ: `GET /v1/gateways?user_id=...` hoặc `GET /v1/gateways?user_id=`).
- **Lý do bảo mật:** Hệ thống xác định danh tính người dùng duy nhất thông qua claim `sub` trong JWT đã được xác minh. Việc truyền `user_id` trong URL là dấu hiệu của hành vi dò quét hoặc tấn công mạo danh (Insecure Direct Object References - IDOR).
- **Hành vi của Backend:** Nếu phát hiện query param `user_id` (kể cả để trống hoặc lặp lại), Go Backend sẽ **lập tức từ chối request với mã lỗi `400 Bad Request` (`invalid_request`)**, hoàn toàn không thực hiện bất kỳ truy vấn nào vào cơ sở dữ liệu.

### 2.6. Thư viện HTTP Client trên Web và Đặc thù Trình duyệt

Khi xây dựng ứng dụng trên nền tảng Flutter Web, lập trình viên cần nắm vững các ràng buộc kỹ thuật sau:

1. **Cơ chế HTTP Client dưới nền tảng Web:**
   - Các thư viện mạng Dart phổ biến như `dio` hoặc `http` trên Flutter Web không thể sử dụng socket nền tảng `dart:io HttpClient` (thư viện `dart:io` bị cấm hoàn toàn trên Web runtime).
   - Thay vào đó, chúng tự động chuyển đổi sang sử dụng `BrowserHttpClient`, ủy quyền việc gửi nhận gói tin cho `fetch` API hoặc `XMLHttpRequest` (XHR) của trình duyệt.
   - Do đó, trong mã nguồn Flutter Web, **tuyệt đối không `import 'dart:io';`** để tránh lỗi biên dịch nền tảng. Thay vào đó, sử dụng các hằng số chuỗi chuẩn (ví dụ `'Accept'`, `'Authorization'`).
2. **Các Header bị trình duyệt hạn chế (Forbidden Header Names):**
   - Theo tiêu chuẩn bảo mật W3C, trình duyệt kiểm soát chặt chẽ và nghiêm cấm mã JavaScript/Dart tự ý can thiệp vào một số header nhạy cảm (Forbidden Headers) như: `User-Agent`, `Host`, `Content-Length`, `Connection`, `Origin`, `Referer`, `Cookie`.
   - Các header phục vụ nghiệp vụ của dự án gồm: `Authorization`, `Accept`, `X-Request-ID`, và `Content-Type` hoàn toàn hợp lệ, không nằm trong danh mục cấm và được trình duyệt cho phép thiết lập tự do.
3. **Cơ chế bắt lỗi mạng và lỗi CORS trên Trình duyệt:**
   - Khi xảy ra sự cố mạng (mất kết nối Internet, máy chủ Nginx chưa khởi động, hoặc request bị chặn bởi chính sách CORS do thiếu header cho phép), trình duyệt Web **chủ động giấu toàn bộ thông tin phản hồi HTTP** vì lý do an toàn bảo mật.
   - Trình duyệt sẽ **không trả về mã trạng thái HTTP cụ thể** (không có response, `statusCode` bằng `null`), mà ném ra ngoại lệ cấp thấp:
     - Trong Dio: `DioExceptionType.connectionError` hoặc `DioExceptionType.unknown`.
     - Trong trình duyệt Console: thông báo lỗi dạng `"XMLHttpRequest error"` hoặc `"Failed to fetch"`.
   - **Xử lý phía Client:** Tầng Service Client (`ApiClient`) phải kiểm tra nếu `response == null`, lập tức bọc thành `NetworkException` mang thông điệp rõ ràng: *"Không thể kết nối đến máy chủ hoặc request bị chặn bởi chính sách CORS của trình duyệt"*, tránh suy diễn sai thành lỗi 500 của server.

---

## 3. Cấu trúc Response lỗi chuẩn (`httpapi.APIErrorEnvelope`)

Khi có lỗi xảy ra (trạng thái HTTP 4xx hoặc 5xx), Go Backend luôn trả về cấu trúc lỗi JSON đồng nhất và an toàn (`src/internal/httpapi/error.go`):

```json
{
  "error": {
    "code": "string",
    "message": "string",
    "request_id": "string"
  }
}
```

### Chi tiết các trường dữ liệu:

1. **`code` (string):**
   - Mã lỗi máy đọc được (machine-readable identifier) dạng snake_case.
   - Ứng dụng Client dùng mã này trong các khối lệnh `switch/case` để phân loại logic xử lý (ví dụ: điều hướng về màn hình đăng nhập, hiển thị thông báo lỗi tham số, hoặc thử lại kết nối).
2. **`message` (string):**
   - Thông điệp mô tả lỗi ngắn gọn, an toàn, thân thiện với người dùng (safe descriptive message).
   - **Nguyên tắc bảo mật:** Go Backend **tuyệt đối không bao giờ** trả về các thông tin nhạy cảm của hệ thống như raw SQL error, pgx database stack trace, chuỗi kết nối (connection string), tên bảng dữ liệu, hay giá trị token trong trường `message`.
3. **`request_id` (string):**
   - Chuỗi định danh duy nhất của request (UUIDv4) tương ứng với request gặp sự cố.
   - Khớp chính xác với giá trị trong response header `X-Request-ID`. Giúp lập trình viên tra cứu chính xác bản ghi lỗi trong file nhật ký (`slog`) của Backend.

---

## 4. Danh mục mã trạng thái HTTP và Error Code chi tiết

Hệ thống quy định danh mục mã lỗi chuẩn hóa và an toàn như sau:

### 4.1. Bảng mã trạng thái HTTP trên các endpoint nghiệp vụ người dùng

| HTTP Status | Error `code` | Error `message` chuẩn | Tình huống kích hoạt phía Server | Hướng xử lý phía Client |
|---|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` | - Cố tình gửi query param `user_id` (chống mạo danh IDOR).<br>- Path param `gateway_id` sai định dạng regex hoặc dùng tên dành riêng (`backend_service`).<br>- Tham số thời gian sai định dạng RFC3339, thiếu timezone rõ ràng, hoặc `to <= from`.<br>- Vượt giới hạn thời gian tối đa (`HISTORY_MAX_RANGE`, mặc định 30 ngày) hoặc khoảng raw (`HISTORY_RAW_WINDOW`, mặc định 1 giờ).<br>- Độ phân giải bucket không nằm trong allowlist 9 mức (`raw_resolution_required`, `invalid_resolution`).<br>- Khoảng thời gian yêu cầu tạo ra quá nhiều bucket vượt `max_points` (`resolution_too_fine`).<br>- Truy vấn raw vượt quá hạn mức điểm dữ liệu (`raw_limit_exceeded`).<br>- Selector thuộc tính không hợp lệ (chỉ hỗ trợ `value` và `sample_interval_us` cho Sensor).<br>- URN sai cú pháp, URN bị double-encode, hoặc loại thực thể không được hỗ trợ.<br>- Keyset cursor sai quy chuẩn base64url hoặc không giải mã được.<br>- Body JSON sai cú pháp, chứa trường lạ, hoặc gửi query parameter trên mutation endpoint. | Sửa logic client, chuẩn hóa định dạng tham số, kiểm tra URL-encode một lần duy nhất. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | - Lỗi định danh (Identity failure): Thiếu header `Authorization`.<br>- Header không đúng scheme `Bearer <token>`.<br>- Token hết hạn, sai chữ ký HS256, hoặc sai `role`/`aud`.<br>*(Server luôn đính kèm header `WWW-Authenticate: Bearer`)*. | Xóa session, kích hoạt luồng Refresh Token (`auth.refreshSession()`) hoặc điều hướng người dùng về màn hình Đăng nhập. |
| **403 Forbidden** | `forbidden` | `insufficient permissions` | - Người dùng không có quyền Quản trị viên trong bảng `platform_admins` khi gọi `/v1/admin/*` hoặc `POST /v1/digital-twins` (loại Gateway).<br>- Người dùng có vai trò `operator` hoặc `viewer` cố tình gọi các thao tác mutation (`POST /v1/digital-twins`, `PATCH /v1/digital-twins/{entity_id}`).<br>- Gateway Owner hoặc bất kỳ ai không phải Platform Admin cố tình gọi `PATCH` trên Gateway cha (trả về `403 Forbidden`). | Hiển thị thông báo tài khoản không có quyền thực hiện thao tác; ẩn hoặc vô hiệu hóa các nút chức năng trên giao diện. |
| **404 Not Found** | `not_found` | `resource not found` | - `gateway_id`, `sensor_id` hoặc `entity_id` không tồn tại trong hệ thống.<br>- Người dùng **không có quyền thành viên** trên tài nguyên yêu cầu (foreign read/mutation, áp dụng cả với Platform Admin khi đọc dữ liệu nghiệp vụ).<br>- Non-member cố tình `PATCH` Sensor (trả về 404 anti-enumeration).<br>- Gọi sai đường dẫn URL không tồn tại (NoRoute). | Hiển thị thông báo không tìm thấy tài nguyên. Tuyệt đối không suy diễn tài nguyên có tồn tại hay không. |
| **405 Method Not Allowed** | `method_not_allowed` | `method not allowed` | - Gọi sai HTTP Method trên endpoint hợp lệ (ví dụ gọi `POST /v1/gateways`). `router.HandleMethodNotAllowed = true`. | Kiểm tra lại phương thức HTTP trong mã nguồn Client. |
| **409 Conflict** | `conflict` | `resource conflict` | - Xung đột cấu hình tạo thực thể (create-or-exact-replay) khi thực thể đã tồn tại nhưng payload gửi lên có thuộc tính khác biệt.<br>- Phát hiện quan hệ đồ thị không nhất quán (`inconsistent_graph`). | Báo lỗi dữ liệu không đồng nhất; không thể ghi đè thực thể đã tồn tại với thuộc tính khác. |
| **413 Request Entity Too Large** | `payload_too_large` | `payload too large` | - Kích thước body gửi lên vượt quá hạn mức tối đa cho phép (`AdminMaxBodyBytes`, mặc định 64KB). | Kiểm tra và thu gọn payload trước khi gửi. |
| **415 Unsupported Media Type** | `unsupported_media_type` | `unsupported media type` | - Thiếu hoặc sai header `Content-Type: application/json` khi gửi mutation request có body. | Đảm bảo luôn gửi đúng header `Content-Type: application/json`. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | - Lỗi máy chủ nội bộ không lường trước (lỗi đọc kết quả SQL, database query fail, panic được khôi phục bởi Recovery middleware). | Hiển thị thông báo sự cố hệ thống và ghi nhận `request_id` để hỗ trợ kỹ thuật tra cứu nhật ký. |
| **501 Not Implemented** | `not_implemented` | `endpoint not implemented` | - Gọi vào WebSocket stub (`/v1/ws`) chưa mở nghiệp vụ. | Không gọi endpoint này cho đến Giai đoạn 7. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | - Lỗi phụ thuộc hạ tầng (Dependency failure):<br>+ Thao tác truy vấn hoặc giải quyết quyền vượt quá thời hạn cho phép (`AuthorizationTimeout` hoặc timeout đọc dữ liệu) hoặc context bị hủy.<br>+ Cơ sở dữ liệu tạm thời gián đoạn khi ping kiểm tra readiness (`GET /readyz`).<br>+ Lỗi commit/network không chắc chắn: **tuyệt đối không suy đoán là thành công** khi gặp lỗi 503. | Hiển thị thông báo máy chủ tạm thời bận hoặc đang bảo trì, cho phép người dùng bấm "Thử lại" (Retry). |

> **LƯU Ý VỀ TÍNH BẢO MẬT CỦA MÃ LỖI 404 (Anti-Enumeration & Foreign Read):**
> Khi gọi `GET /v1/gateways/{gateway_id}/sensors`, nếu Gateway không tồn tại HOẶC người dùng không được phân quyền trong bảng `user_gateways` (foreign gateway), Go Backend **luôn trả về cùng một mã lỗi `404 Not Found` với thông điệp `resource not found`**. Backend tuyệt đối không trả về `403 Forbidden` đối với trạm của người khác, nhằm ngăn chặn kẻ tấn công dò quét xem những mã `gateway_id` nào đang thực sự tồn tại trong hệ thống.

### 4.2. Tổng quan mã lỗi nhóm Admin API (`/v1/admin/*`)

Nhóm endpoint quản trị nền tảng có các ràng buộc nghiêm ngặt và thứ tự xử lý lỗi tiền đề như sau:

1. **Thứ tự kiểm tra tiền đề (Precedence of Middleware Checks):**
   - **Bước 1 (Xác thực):** `AuthenticationMiddleware` luôn chạy đầu tiên: trả về **`401 Unauthorized`** (`unauthorized`) nếu thiếu token hoặc token không hợp lệ.
   - **Bước 2 (Kiểm tra quyền Admin):** `PlatformAdminMiddleware` chạy tiếp theo:
     - Nếu tra cứu bảng `platform_admins` bị timeout hoặc context bị hủy: trả về **`503 Service Unavailable`** (`service_unavailable`).
     - Nếu tài khoản không thuộc danh sách quản trị viên: trả về **`403 Forbidden`** (`forbidden`).
   - *(Hai tầng middleware trên luôn chạy trước và chặn đứng mọi request không hợp lệ TRƯỚC KHI request chạm tới logic kiểm tra tính năng hay nghiệp vụ).*

2. **Mã lỗi đặc thù tầng nghiệp vụ và quản trị chứng chỉ MQTT:**
   - **`503 Service Unavailable` — Lỗi tắt tính năng (`credential_runtime_disabled`):** Khi tính năng quản lý chứng chỉ bị tắt trên môi trường runtime (`CredentialAPIEnabled == false`), Go backend trả về mã lỗi chính xác là **`credential_runtime_disabled`** (đây là mã miền nghiệp vụ riêng biệt, **KHÔNG PHẢI** mã chung `service_unavailable`).
   - **`503 Service Unavailable` — Lỗi rào cản khởi động (`service_unavailable`):** Khi tính năng đã bật nhưng rào cản đồng bộ trạng thái khởi động chưa sẵn sàng (`!CredentialStartupReadiness.Ready()`), handler trả về mã lỗi **`service_unavailable`**.
   - **`413 Request Entity Too Large`:** Trả về `payload_too_large` (tại các route provisioning) hoặc `request_too_large` (tại các route credential) khi kích thước body gửi lên vượt quá hạn mức `AdminMaxBodyBytes`.
   - **`415 Unsupported Media Type`:** Trả về `unsupported_media_type` khi gửi request có body nhưng thiếu hoặc sai `Content-Type: application/json`.
   - **`400 Bad Request` (`invalid_request`):** Body JSON sai cú pháp, thiếu trường bắt buộc, hoặc header `Idempotency-Key` sai quy chuẩn.
   - **`409 Conflict` (`conflict`):** Xung đột trạng thái chứng chỉ MQTT (ví dụ xoay vòng chứng chỉ chưa từng cấp phát, hoặc vi phạm tính lũy kế idempotency).
   - **`503 Service Unavailable` (`service_unavailable`):** Quá trình tương tác với Mosquitto hoặc cơ sở dữ liệu bị timeout vượt quá `CredentialRequestTimeout`.

*(Xem đặc tả chi tiết toàn bộ DTOs, Idempotency-Key và danh mục mã lỗi chuyên sâu của Quản trị viên tại [`docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)).*

---

## 5. Chi tiết từng API hiện có (Stage 2 Specification)

### 5.1. API 1: Lấy danh sách Gateway được phân quyền (`GET /v1/gateways`)

Endpoint này trả về toàn bộ các trạm Gateway mà người dùng hiện tại có liên kết quyền trong bảng `user_gateways`.

- **HTTP Method:** `GET`
- **Đường dẫn:** `/v1/gateways`
- **Yêu cầu xác thực:** Bắt buộc JWT hợp lệ của người dùng (`Principal.UserID` tồn tại).
- **Query Parameters:** **Không có**. (Nghiêm cấm gửi `user_id`).
- **Request Headers:**
  ```http
  GET /v1/gateways HTTP/1.1
  Host: api.yourdomain.com
  Authorization: Bearer <access_token>
  Accept: application/json
  X-Request-ID: 7b848c12-5813-4315-992e-b1fa7a7b8e19
  ```

#### Response thành công (`200 OK`):
- **Response Headers:**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  X-Request-ID: 7b848c12-5813-4315-992e-b1fa7a7b8e19
  ```
- **Response Body mẫu:**
  ```json
  {
    "items": [
      {
        "gateway_id": "gateway_001",
        "name": "AM5728 Gateway 001",
        "description": "Trạm quan trắc nhà kính số 1",
        "role": "owner",
        "created_at": "2026-09-16T08:30:00Z"
      },
      {
        "gateway_id": "gateway_002",
        "name": "ESP32 Gateway 002",
        "description": null,
        "role": "viewer",
        "created_at": "2026-09-18T10:15:30Z"
      }
    ]
  }
  ```

#### Quy tắc dữ liệu (DTO Specification):
1. **`items` (mảng đối tượng):**
   - **Luôn luôn là một mảng JSON (Array), không bao giờ trả về `null`.**
   - Nếu tài khoản hợp lệ nhưng chưa được gán bất kỳ trạm nào, backend trả về mảng rỗng: `{"items": []}`.
2. **`gateway_id` (string):** Mã định danh duy nhất của trạm Gateway.
3. **`name` (string):** Tên định danh của trạm Gateway.
4. **`description` (string | null):** Mô tả trạm. Trường này luôn xuất hiện trong JSON; có thể mang giá trị chuỗi hoặc giá trị `null` nếu trạm chưa có mô tả.
5. **`role` (string):** Vai trò của người dùng trên trạm này, chỉ nhận một trong ba giá trị hợp lệ:
   - `"owner"`: Chủ sở hữu trạm.
   - `"operator"`: Người vận hành trạm.
   - `"viewer"`: Người xem chỉ đọc.
6. **`created_at` (string | null):** Thời điểm trạm được tạo trong cơ sở dữ liệu. Có thể là `null` (đối với bản ghi cũ) hoặc chuỗi thời gian định dạng chuẩn ISO 8601 UTC kết thúc bằng chữ cái `Z` (ví dụ `2026-09-16T08:30:00Z`).
7. **Thứ tự sắp xếp (Sorting):** Danh sách các trạm trong `items` luôn được sắp xếp theo `gateway_id` tăng dần theo bảng mã ký tự ASCII (`COLLATE "C"` trong SQL). Không hỗ trợ phân trang ở bản MVP và không tự ý cắt xén số lượng trạm.

---

### 5.2. API 2: Lấy danh sách Cảm biến của Gateway (`GET /v1/gateways/{gateway_id}/sensors`)

Endpoint này trả về danh sách các cảm biến thuộc về một trạm Gateway cụ thể, với điều kiện người dùng có quyền hợp lệ trên trạm đó.

- **HTTP Method:** `GET`
- **Đường dẫn:** `/v1/gateways/{gateway_id}/sensors`
- **Yêu cầu xác thực:** Bắt buộc JWT hợp lệ của người dùng.
- **Path Parameters:**
  - `gateway_id` (string, bắt buộc): Định danh của Gateway.
  - **Quy tắc hợp lệ (Validation):** Phải khớp hoàn toàn với biểu thức chính quy (Regex):
    ```text
    ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$
    ```
    - Bắt đầu bằng chữ cái hoặc chữ số.
    - Chỉ chứa chữ cái, chữ số, gạch dưới (`_`), hoặc gạch ngang (`-`).
    - Độ dài từ 1 đến 64 ký tự.
    - **Không được phép là chuỗi dành riêng:** `backend_service`.
    - Phân biệt chữ hoa chữ thường (Case-sensitive), không tự động cắt tỉa khoảng trắng. Nếu sai quy tắc, backend trả ngay `400 Bad Request`.
- **Query Parameters:** **Không có**. (Nghiêm cấm gửi `user_id`).
- **Request Headers:**
  ```http
  GET /v1/gateways/gateway_001/sensors HTTP/1.1
  Host: api.yourdomain.com
  Authorization: Bearer <access_token>
  Accept: application/json
  X-Request-ID: 9a12b3c4-d5e6-4789-a012-fe34dc56ba78
  ```

#### Response thành công (`200 OK`):
- **Response Headers:**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  X-Request-ID: 9a12b3c4-d5e6-4789-a012-fe34dc56ba78
  ```
- **Response Body mẫu:**
  ```json
  {
    "gateway_id": "gateway_001",
    "items": [
      {
        "sensor_id": "sensor_001",
        "name": "Nhiệt độ môi trường",
        "unit": "°C",
        "created_at": "2026-09-16T08:30:00Z"
      },
      {
        "sensor_id": "sensor_002",
        "name": "Độ ẩm không khí",
        "unit": "%",
        "created_at": "2026-09-16T08:31:00Z"
      }
    ]
  }
  ```

#### Quy tắc dữ liệu (DTO Specification):
1. **`gateway_id` (string):** Khớp chính xác với tham số `gateway_id` truyền vào trên URL.
2. **`items` (mảng đối tượng):**
   - **Luôn luôn là một mảng JSON (Array), không bao giờ trả về `null`.**
   - **Trường hợp Gateway chưa có cảm biến:** Nếu Gateway tồn tại, người dùng có quyền hợp lệ nhưng trạm đó chưa được lắp đặt hay cấu hình cảm biến nào trong cơ sở dữ liệu, backend trả về mã `200 OK` kèm mảng rỗng:
     ```json
     {
       "gateway_id": "gateway_001",
       "items": []
     }
     ```
3. **`sensor_id` (string):** Mã định danh của cảm biến (duy nhất trong phạm vi trạm Gateway).
4. **`name` (string):** Tên hiển thị của cảm biến.
5. **`unit` (string | null):** Đơn vị đo lường của cảm biến (ví dụ `"°C"`, `"%"`, `"lux"`, `"ppm"`). Trường này luôn xuất hiện trong JSON; có thể mang giá trị `null` nếu cảm biến chưa được cấu hình đơn vị.
6. **`created_at` (string | null):** Thời điểm cảm biến được tạo. Có thể là `null` hoặc chuỗi thời gian ISO 8601 UTC (`Z`).
7. **Thứ tự sắp xếp (Sorting):** Danh sách cảm biến trong `items` luôn được sắp xếp theo `sensor_id` tăng dần theo bảng mã ký tự ASCII.
8. **Phân biệt rạch ròi giữa 200 OK và 404 Not Found:**
   - Trả về `200 OK` (với `items: []`): Khi người dùng **CÓ QUYỀN** trên Gateway, và Gateway đó hiện chưa có cảm biến nào.
   - Trả về `404 Not Found`: Khi `gateway_id` không tồn tại trên hệ thống HOẶC người dùng **KHÔNG CÓ QUYỀN** truy cập Gateway đó.

---

### 5.3. API 3: Khởi tạo Cảm biến cho Chủ sở hữu Gateway (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`)

Endpoint này cho phép **Chủ sở hữu Gateway (`owner`)** tự cấp phát hoặc phát lại chính xác cấu hình cảm biến (create-or-exact-replay) thuộc trạm mà mình quản lý trong `user_gateways`.

- **HTTP Method:** `PUT`
- **Đường dẫn:** `/v1/gateways/{gateway_id}/sensors/{sensor_id}`
- **Yêu cầu xác thực & Quyền hạn:**
  - Bắt buộc Bearer JWT của người dùng đã xác thực (`Principal.UserID` hợp lệ, `role == "authenticated"`).
  - Phải có vai trò `owner` trên trạm `gateway_id` cha trong bảng `public.user_gateways`.
  - **Phân định vai trò thành viên:**
    - `owner`: Được phép tạo mới hoặc phát lại chính xác.
    - `operator` và `viewer`: Bị từ chối với mã **`403 Forbidden`** (`code: "forbidden"`, `message: "access denied"`).
    - Người dùng không có liên kết với trạm (`non-member`), trạm của người khác (`foreign gateway`), hoặc `gateway_id` không tồn tại trong hệ thống: Nhận mã **`404 Not Found`** (`code: "not_found"`, `message: "resource not found"`). Cơ chế này ngăn chặn dò quét sự tồn tại của trạm (Anti-Enumeration).
    - **Platform Admin không bypass tư cách thành viên:** Nếu tài khoản là Platform Admin nhưng không có vai trò `owner` trong `user_gateways` cho Gateway này, endpoint vẫn phản hồi `404 Not Found`. Platform Admin muốn khởi tạo Sensor toàn cục độc lập với quyền thành viên sử dụng endpoint riêng [`PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`](07_ADMIN_API_CLIENT_CONTRACT.md).
- **Ràng buộc định danh (Path Parameters):**
  - `gateway_id` (string, bắt buộc): Khớp regex `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, không được là `backend_service`.
  - `sensor_id` (string, bắt buộc): Khớp regex `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.
- **Query Parameters:** **Tuyệt đối không có**. (Gửi query string hoặc `?` sẽ nhận mã `400 Bad Request` với `code: "invalid_request"`).
- **Request Headers:**
  - `Authorization: Bearer <access_token>`
  - `Content-Type: application/json` (tùy chọn tham số `charset=utf-8` không phân biệt hoa thường). Nếu thiếu hoặc sai Content-Type, server trả về **`415 Unsupported Media Type`** (`code: "unsupported_media_type"`, `message: "unsupported media type"`).
  - `X-Request-ID: <id>` (khuyến nghị, phục vụ truy vết).
  - **LƯU Ý:** Endpoint này **KHÔNG DÙNG** header `Idempotency-Key`. Tính lũy đẳng được đảm bảo bởi định danh tài nguyên URL (`gateway_id`, `sensor_id`) và kiểm tra đối soát payload chính xác.
- **Giới hạn kích thước Body:**
  - Body bị giới hạn bởi cấu hình `ADMIN_MAX_BODY_BYTES` trên server (mặc định là 16384 bytes = 16 KiB; dải giá trị hợp lệ 1..1048576 bytes = 1 MiB theo `src/internal/config/config.go`, không phải hằng số cứng bất biến trên mọi deployment). Nếu kích thước body gửi lên vượt quá hạn mức cấu hình này, server từ chối với mã lỗi **`413 Request Entity Too Large`** (`code: "payload_too_large"`, `message: "payload too large"`).
- **Định dạng Request Body JSON:**
  ```json
  {
    "name": "Nhiệt độ môi trường",
    "unit": "°C"
  }
  ```
  - `name` (string, bắt buộc): Tên cảm biến, độ dài 1–256 bytes UTF-8 hợp lệ, không chứa ký tự null `\x00`, không được để trống hoặc chỉ chứa khoảng trắng (`strings.TrimSpace(name) != ""`). Server giữ nguyên chuỗi gốc, không tự ý cắt xén khoảng trắng.
  - `unit` (string | null, tùy chọn): Đơn vị đo lường, tối đa 64 bytes UTF-8 hợp lệ, không chứa `\x00`. Có thể truyền chuỗi đo lường (ví dụ `"°C"`), chuỗi rỗng `""` (được lưu trữ là đơn vị hợp lệ và phân biệt rạch ròi với null), giá trị `null`, hoặc lược bỏ hoàn toàn trường này khỏi JSON.
  - **Quy tắc JSON khắt khe (Strict JSON Parsing):**
    - Cấm tuyệt đối trường lạ: Bất kỳ trường nào ngoài `name` và `unit` (ví dụ `sensor_type`, `description`...) đều bị server từ chối ngay với mã `400 Bad Request` (`code: "invalid_request"`).
    - Cấm trùng lặp khóa (duplicate keys) trong JSON.
    - Cấm surrogate không hợp lệ (`\uDC00` đơn lẻ) hoặc ký tự null được escape (`\u0000`).
    - Cấm ký tự thừa sau dấu ngoặc nhọn kết thúc `}`.

#### Phản hồi thành công:

1. **Tạo mới cảm biến thành công (HTTP 201 Created):**
   - Áp dụng khi `sensor_id` chưa từng tồn tại trên Gateway này.
   - Trong cùng một transaction CSDL PostgreSQL nguyên tử: bản ghi cảm biến được chèn vào bảng `sensors`, thực thể Digital Twin được tạo trong `twin_entities` với `entity_id` dạng `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`, trạng thái ban đầu được tạo trong `twin_states`, và quan hệ `hasSensor` được tạo trong `twin_relationships` nối từ Gateway cha sang Sensor.
   - Response Headers:
     ```http
     HTTP/1.1 201 Created
     Content-Type: application/json; charset=utf-8
     Cache-Control: no-store
     Pragma: no-cache
     X-Request-ID: e12b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d
     ```
   - Response Body (kèm `unit`):
     ```json
     {
       "gateway_id": "gateway_001",
       "sensor_id": "sensor_001",
       "name": "Nhiệt độ môi trường",
       "unit": "°C",
       "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
       "created_at": "2026-10-14T08:30:00Z"
     }
     ```
   - Response Body (khi `unit` là null hoặc bị lược bỏ trong request):
     ```json
     {
       "gateway_id": "gateway_001",
       "sensor_id": "sensor_002",
       "name": "Cảm biến rung chấn",
       "unit": null,
       "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_002",
       "created_at": "2026-10-14T08:31:00Z"
     }
     ```

2. **Phát lại chính xác — Exact Replay (HTTP 200 OK):**
   - Áp dụng khi `sensor_id` đã tồn tại trên Gateway và request gửi lên có `name` cùng `unit` hoàn toàn khớp với bản ghi hiện hữu (bao gồm so sánh chuỗi rỗng với rỗng, null với null).
   - Server phản hồi mã **`200 OK`** với nội dung JSON phản ánh bản ghi hiện tại, giữ nguyên mốc thời gian `created_at` ban đầu.

#### Phản hồi lỗi xung đột cấu hình (HTTP 409 Conflict):
- Nếu `sensor_id` đã tồn tại nhưng request gửi lên chứa `name` hoặc `unit` khác so với bản ghi hiện có trong CSDL:
  - Server trả về mã **`409 Conflict`** (`code: "conflict"`, `message: "resource conflict"`).
  - **LƯU Ý:** Endpoint này **KHÔNG PHẢI là API cập nhật cấu hình hay upsert**. Server bảo toàn tính toàn vẹn của cảm biến ban đầu và tuyệt đối không ghi đè dữ liệu. Để đổi cấu hình cảm biến hiện có, hệ thống yêu cầu quy trình quản trị chuyên dụng trong các giai đoạn sau.

#### Bảng tổng hợp mã lỗi của `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`:

| HTTP Status | Error `code` | Error `message` | Tình huống kích hoạt |
|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` | - URL chứa query string (`?`).<br>- Định dạng `gateway_id` hoặc `sensor_id` sai regex, hoặc `gateway_id` là `backend_service`.<br>- Body JSON sai cú pháp, duplicate key, surrogate lỗi.<br>- Thiếu trường `name`, hoặc `name` để trống / chỉ có khoảng trắng.<br>- `unit` vượt quá 64 bytes.<br>- Chứa trường lạ không hỗ trợ (ví dụ `sensor_type`). |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Thiếu token, token hết hạn, hoặc token không hợp lệ. |
| **403 Forbidden** | `forbidden` | `access denied` | Người dùng đã xác thực nhưng có vai trò `operator` hoặc `viewer` trên Gateway cha. |
| **404 Not Found** | `not_found` | `resource not found` | - `gateway_id` cha không tồn tại.<br>- Người dùng không có liên kết quyền trong `user_gateways` (non-member / foreign read protection).<br>- Platform Admin không có vai trò `owner` trong `user_gateways` cho trạm này. |
| **409 Conflict** | `conflict` | `resource conflict` | Sensor đã tồn tại nhưng gửi cấu hình `name` hoặc `unit` khác. |
| **413 Payload Too Large** | `payload_too_large` | `payload too large` | Kích thước body vượt quá giới hạn đọc tối đa `AdminMaxBodyBytes`. |
| **415 Unsupported Media Type** | `unsupported_media_type` | `unsupported media type` | Header `Content-Type` bị thiếu hoặc khác `application/json`. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | Lỗi CSDL nội bộ không xác định hoặc đồ thị Digital Twin bị bất nhất. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | Quá thời hạn xử lý cơ sở dữ liệu (`context.DeadlineExceeded`) hoặc CSDL tạm thời gián đoạn. |

> **Ghi chú về định danh URN Digital Twin:**
> Việc gán URN `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>` và tạo thực thể twin cùng quan hệ `hasSensor` trong transaction CSDL đảm bảo tính sẵn sàng cho mô hình dữ liệu NGSI-LD tương thích. Việc này **không đồng nghĩa** toàn bộ phân hệ điều khiển thiết bị, hàng đợi lệnh downlink, hay bản sao số thời gian thực đã được kích hoạt hoàn chỉnh trong giai đoạn này.

---

### 5.4. API Nhóm Stage 4: Telemetry History và Digital Twin APIs

Nhóm 7 endpoint Stage 4 phục vụ đọc chuỗi thời gian lịch sử, đọc mô hình Digital Twin (danh sách, chi tiết NGSI-LD, trạng thái hiện thời, lịch sử thuộc tính) và quản lý metadata thực thể. Xem đặc tả protocol đầy đủ tại [`docs/protocols/stage4-read-and-entity-api.md`](../protocols/stage4-read-and-entity-api.md).

#### 1. Lịch sử Telemetry (`GET /v1/telemetry/history`)
- **Mục đích:** Truy vấn chuỗi thời gian mẫu đo từ hypertable TimescaleDB, hỗ trợ đọc `raw` hoặc downsample `time_bucket`.
- **Yêu cầu xác thực & Quyền:** Bắt buộc JWT hợp lệ. Người dùng phải là thành viên (`owner`, `operator`, `viewer`) của `gateway_id` yêu cầu trong `user_gateways`. Non-member (kể cả Platform Admin không có membership) nhận `404 Not Found`.
- **Query Parameters:**
  - `gateway_id` (string, bắt buộc): Định danh Gateway.
  - `sensor_id` (string, bắt buộc): Định danh Sensor.
  - `from` (string, bắt buộc): Mốc thời gian bắt đầu ISO 8601 / RFC3339, bắt buộc có múi giờ rõ ràng (ví dụ `Z` hoặc `+07:00`), độ chính xác microsecond.
  - `to` (string, bắt buộc): Mốc thời gian kết thúc (phải có `to > from`).
  - `resolution` (string, tùy chọn, mặc định `"auto"`): Nhận `"auto"`, `"raw"`, hoặc 1 trong 9 mức bucket allowlisted: `"100ms"`, `"1s"`, `"10s"`, `"1m"`, `"5m"`, `"15m"`, `"1h"`, `"6h"`, `"1d"`.
  - `max_points` (int, tùy chọn, mặc định `1000` theo cấu hình `HISTORY_DEFAULT_POINTS`, giới hạn hợp đồng min `100` và max `2000` theo cấu hình `HISTORY_MIN_POINTS` và `HISTORY_MAX_POINTS`): Giới hạn số điểm trả về cho một truy vấn history (không dùng giá trị 500 của fixture làm hạn mức chung toàn hệ thống).
- **Ranh giới & Quy tắc:**
  - Khoảng thời gian không vượt `HISTORY_MAX_RANGE` (mặc định 30 ngày); truy vấn `raw` không vượt `HISTORY_RAW_WINDOW` (mặc định 1 giờ).
  - Khoảng thời gian nửa mở: `[from, to)`. Output luôn chuẩn hóa UTC microsecond RFC3339 kết thúc bằng `Z`.
  - Nếu số điểm raw thực tế vượt quá `max_points`, server trả về `400 Bad Request` (`raw_limit_exceeded`), **tuyệt đối không âm thầm cắt bớt dữ liệu**. Chế độ `"auto"` tự động thăm dò raw trong cửa sổ ngắn (<=10s), nếu vượt hạn mức sẽ tự động hạ cấp an toàn sang bucket phù hợp.
  - Kiểu dữ liệu trên dây: `point_count` là số nguyên (numeric int), các trường số nguyên 64-bit (`sequence_num`, `count`) được định dạng chuỗi số nguyên (decimal string) để tránh mất độ chính xác trên JavaScript/Web (`> 2^53`). Điểm raw có `sequence_num` (string) và `value` (number); điểm bucket có `bucket` (string timestamp UTC), `avg`, `min`, `max` (number), và `count` (string). Các bucket rỗng không có dữ liệu sẽ bị bỏ qua (không gapfill số liệu giả).
- **Ví dụ Response Raw (`200 OK`):**
  ```json
  {
    "gateway_id": "gateway_001",
    "sensor_id": "sensor_001",
    "mode": "raw",
    "resolution": "raw",
    "from": "2026-10-18T00:00:00.000000Z",
    "to": "2026-10-18T00:00:05.000000Z",
    "point_count": 2,
    "points": [
      {
        "measured_at": "2026-10-18T00:00:01.000000Z",
        "sequence_num": "1001",
        "value": 25.4
      },
      {
        "measured_at": "2026-10-18T00:00:02.000000Z",
        "sequence_num": "1002",
        "value": 25.6
      }
    ]
  }
  ```

#### 2. Danh sách thực thể Digital Twin (`GET /v1/digital-twins`)
- **Mục đích:** Liệt kê các thực thể Digital Twin (`Gateway`, `Sensor`) mà người dùng có quyền trong `user_gateways`.
- **Yêu cầu xác thực & Quyền:** Bearer JWT. Trả về `items: []` nếu không có quyền trạm nào.
- **Query Parameters:**
  - `entity_type` (string, tùy chọn): Lọc theo `"Gateway"` hoặc `"Sensor"`.
  - `gateway_id` (string, tùy chọn): Lọc theo trạm cụ thể.
  - `limit` (int, tùy chọn, mặc định `20`, min `1`, max `100`).
  - `cursor` (string, tùy chọn): Keyset cursor dạng opaque base64url (`v1.{k,g,t}`). **Lưu ý Client:** Phải reset cursor về null khi thay đổi filter (`entity_type`, `gateway_id`).
- **Response (`200 OK`):** Header `Content-Type: application/json; charset=utf-8`.
  ```json
  {
    "items": [
      {
        "entity_id": "urn:ngsi-ld:Gateway:gateway_001",
        "entity_type": "Gateway",
        "name": "AM5728 Gateway 001",
        "gateway_id": "gateway_001",
        "sensor_id": null,
        "created_at": "2026-10-18T08:30:00.000000Z",
        "updated_at": "2026-10-18T08:30:00.000000Z"
      }
    ],
    "next_cursor": "djEuMS5nYXRld2F5XzAwMS5HYXRld2F5"
  }
  ```

#### 3. Chi tiết thực thể Digital Twin (`GET /v1/digital-twins/{entity_id}`)
- **Mục đích:** Đọc biểu diễn NGSI-LD đầy đủ của thực thể kèm quan hệ phân trang.
- **Yêu cầu xác thực & Quyền:** Bearer JWT (`authenticated`, thành viên trạm). Non-member nhận `404 Not Found`.
- **Path Parameter:** `entity_id` (string, bắt buộc): URN thực thể (ví dụ `urn:ngsi-ld:Gateway:gateway_001`). **Lưu ý:** Client chỉ encode URL một lần (`Uri.encodeComponent`), không double-encode.
- **Query Parameters:** `relation_limit` (mặc định 20), `relation_cursor` (keyset cursor quan hệ riêng `v1.{s,r,k}`).
- **Response (`200 OK`):** Header `Content-Type: application/ld+json; charset=utf-8`.
  ```json
  {
    "id": "urn:ngsi-ld:Gateway:gateway_001",
    "type": "Gateway",
    "name": {
      "type": "Property",
      "value": "AM5728 Gateway 001"
    },
    "description": {
      "type": "Property",
      "value": "Tram quan trac nha kinh"
    },
    "hasSensor": [
      {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
        "observedAt": null
      }
    ],
    "hasSensor_next_cursor": null,
    "@context": {
      "name": "https://uri.etsi.org/ngsi-ld/name",
      "description": "https://uri.etsi.org/ngsi-ld/description",
      "hasSensor": "https://uri.etsi.org/ngsi-ld/hasSensor",
      "observedAt": "https://uri.etsi.org/ngsi-ld/observedAt",
      "unit": "https://uri.etsi.org/ngsi-ld/unitCode"
    }
  }
  ```

#### 4. Trạng thái hiện thời Digital Twin (`GET /v1/digital-twins/{entity_id}/state`)
- **Mục đích:** Đọc nhanh trạng thái `reported_state` và `desired_state` trực tiếp từ bảng `twin_states` (không quét lịch sử hypertable).
- **Yêu cầu xác thực & Quyền:** Bearer JWT (`authenticated`, thành viên trạm). Non-member nhận `404 Not Found`.
- **Response (`200 OK`):** Header `Content-Type: application/json; charset=utf-8`. Lưu ý: payload trả về các trường định danh `id`, `type`, `gateway_id` (không dùng trường `entity_id`).
  ```json
  {
    "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
    "type": "Sensor",
    "gateway_id": "gateway_001",
    "reported_state": {
      "value": 25.4,
      "sample_interval_us": 10000
    },
    "desired_state": {},
    "reported_version": "3",
    "desired_version": "0",
    "last_reported_at": "2026-10-18T08:35:00.000000Z",
    "last_desired_at": null,
    "updated_at": "2026-10-18T08:35:00.000000Z"
  }
  ```
  *(Lưu ý: `reported_version` và `desired_version` được định dạng chuỗi số nguyên để bảo toàn độ chính xác).*

#### 5. Lịch sử thuộc tính Digital Twin (`GET /v1/digital-twins/{entity_id}/history`)
- **Mục đích:** Truy vấn lịch sử thuộc tính cụ thể của thực thể Sensor.
- **Yêu cầu xác thực & Quyền:** Bearer JWT (`authenticated`, thành viên trạm). Non-member nhận `404 Not Found`.
- **Ranh giới Selector:** **Chỉ hỗ trợ thực thể Sensor** với đúng 2 thuộc tính:
  - `property=value`: Tự động ủy quyền (delegate) sang dịch vụ `TelemetryHistoryService` mà không nhân đôi dữ liệu chuỗi thời gian.
  - `property=sample_interval_us`: Đọc từ TimescaleDB hypertable `twin_temporal_values` (đơn vị lấy từ `metadata.unit` của temporal row, không lấy từ `sensors.unit`).
  - Mọi selector khác hoặc truy vấn trên Gateway sẽ bị từ chối với lỗi domain (`400 Bad Request`).
- **Response (`200 OK`):** Giữ nguyên cấu trúc `values` với các trường số nguyên chuỗi, và mảng rỗng `values: []` nếu không có điểm dữ liệu trong khoảng.

#### 6. Khởi tạo thực thể Digital Twin (`POST /v1/digital-twins`)
- **Mục đích:** Tạo thực thể Digital Twin mới hoặc phát lại chính xác (create-or-exact-replay).
- **Yêu cầu xác thực & Quyền:**
  - Loại `"Gateway"`: Bắt buộc quyền Quản trị viên (`platform_admins`).
  - Loại `"Sensor"`: Bắt buộc Quản trị viên (`platform_admins`) hoặc Chủ sở hữu (`owner`) của Gateway cha.
  - Vai trò `operator` và `viewer` luôn bị từ chối `403 Forbidden`.
- **Ranh giới:**
  - Tạo mới thành công trả về `201 Created` (`Created: true`).
  - Phát lại chính xác cùng thuộc tính (Exact Replay) trả về `200 OK` (`Created: false`).
  - Gửi cấu hình khác biệt trên thực thể đã tồn tại trả về `409 Conflict` (`resource conflict`).
  - Không tự động cấp phát tài khoản Supabase Auth hay chứng chỉ MQTT (các quy trình này tách biệt hoàn toàn).

#### 7. Cập nhật Metadata thực thể (`PATCH /v1/digital-twins/{entity_id}`)
- **Mục đích:** Chỉnh sửa siêu dữ liệu (`name`, `description`) của thực thể đã tồn tại.
- **Yêu cầu xác thực & Quyền:**
  - Thực thể `Gateway`: Chỉ dành riêng cho Platform Admin (`platform_admins`). Gateway Owner gọi `PATCH` trên Gateway cha sẽ bị từ chối `403 Forbidden`.
  - Thực thể `Sensor`: Dành cho Platform Admin hoặc Gateway Owner của trạm cha. Non-member nhận `404 Not Found` (anti-enumeration).
  - Vai trò `operator` và `viewer` luôn nhận `403 Forbidden`.
- **Quy tắc cập nhật trường (Field Allowlist):**
  - Thực thể `Gateway`: Chỉ cho phép `name` (string) và `description` (presence-aware: không gửi = giữ nguyên, chuỗi = cập nhật, `null` = xóa về SQL NULL). Dữ liệu `description` được lưu vào bảng quan hệ `gateways.description`; trường `twin_entities.attributes` được bảo toàn nguyên vẹn!
  - Thực thể `Sensor`: Chỉ cho phép duy nhất trường `name` (bắt buộc là chuỗi không rỗng). Trường `unit` là **bất biến (immutable)** và không được phép cập nhật.
- **Ranh giới nghiệp vụ nghiêm ngặt:**
  - Cập nhật cùng giá trị (Same-value No-Op) trả về `200 OK`, không thực hiện ghi SQL, giữ nguyên `updated_at`.
  - Tuyệt đối **KHÔNG** cập nhật trạng thái `desired_state` hay `reported_state`.
  - Tuyệt đối **KHÔNG** phát sinh lệnh điều khiển thiết bị hay gửi tin nhắn MQTT ra broker.
  - Không chấp nhận query parameter trên mutation endpoint (vi phạm nhận `400 Bad Request`).
  - Giới hạn body: `AdminMaxBodyBytes` (mặc định 64KB), vi phạm nhận `413 Payload Too Large`. Bắt buộc `Content-Type: application/json` (thiếu/sai nhận `415 Unsupported Media Type`).

---

## 6. Danh sách Tính năng và API tương lai chưa khả dụng (Future Features Unavailable)

Hệ thống tuân thủ nghiêm ngặt lộ trình phát triển phân tầng kiến trúc (`Backend_summary_plan.md` và `AGENTS.md`). Bảy endpoint Stage 4 (lịch sử chuỗi thời gian, đọc Digital Twin list/detail/state/history, tạo thực thể và cập nhật metadata) đã hoàn thành và hoạt động. Các tính năng dưới đây thuộc về các giai đoạn sau:

| Endpoint / Nhóm tính năng | Method | Trạng thái hiện tại | Kế hoạch triển khai & Ghi chú |
|---|:---:|:---:|---|
| `/v1/ws` | `GET` | `501 Not Implemented` | Kênh truyền dữ liệu thời gian thực WebSocket (Giai đoạn 7). |
| **Device Control & Commands** | `POST /v1/digital-twins/{entity_id}/commands` | **Chưa khả dụng** | Gửi lệnh điều khiển (`twin_commands`) và cơ chế Transactional Outbox (Giai đoạn 5). |
| **Desired State Mutation** | `PATCH /v1/digital-twins/{entity_id}/desired-state` | **Chưa khả dụng** | Cập nhật cấu hình bền vững `desired_state` (Giai đoạn 5). |
| **State Reconciliation** | Worker nền | **Chưa khả dụng** | Worker đối soát hội tụ trạng thái mong muốn và thực tế (Giai đoạn 6). |
| **Media Upload & Read URLs** | `POST` / `GET` | **Chưa khả dụng** | Tạo signed upload URL và đọc ảnh từ private Supabase Storage qua Go REST API (Giai đoạn 8). |

#### Payload phản hồi chuẩn khi gọi vào WebSocket stub `501`:
```http
HTTP/1.1 501 Not Implemented
Content-Type: application/json; charset=utf-8
X-Request-ID: f47ac10b-58cc-4372-a567-0e02b2c3d479

{
  "error": {
    "code": "not_implemented",
    "message": "endpoint not implemented",
    "request_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"
  }
}
```

**Khuyến cáo nghiêm ngặt cho Client:** Lập trình viên Client tuyệt đối không phát triển các màn hình gọi vào các endpoint chưa khả dụng trên cho đến khi có thông báo bàn giao chính thức từ đội ngũ Backend.

---

## 7. Mã nguồn Dart Client mẫu (Flutter Implementation)

> **TUYÊN BỐ VỀ TÍNH CHẤT MÃ NGUỒN MẪU (DISCLAIMER):**
> Toàn bộ các đoạn mã nguồn Dart dưới đây là **gợi ý tham khảo kiến trúc (code suggestions / illustrative reference)** nhằm hỗ trợ đội ngũ phát triển Flutter hình dung cách cài đặt DTO, Interceptor và Exception. Đây **KHÔNG PHẢI là bằng chứng chứng minh client đã được xuất xưởng hay hoàn tất (not shipped client proof)**.
> Chi tiết về kiến trúc Repository, State Management, và luồng kiểm thử giao diện thực tế được quy định tại [`docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).

Dưới đây là mã nguồn minh họa định nghĩa DTO Models và Service Client sử dụng thư viện `dio`:

### 7.1. Định nghĩa Data Transfer Objects (DTO Models)

Tạo file `lib/core/network/dto/api_models.dart`:

```dart
import 'package:flutter/foundation.dart';

/// Vai trò của người dùng đối với trạm Gateway.
enum GatewayRole {
  owner,
  operator,
  viewer;

  static GatewayRole fromString(String role) {
    switch (role.toLowerCase()) {
      case 'owner':
        return GatewayRole.owner;
      case 'operator':
        return GatewayRole.operator;
      case 'viewer':
        return GatewayRole.viewer;
      default:
        throw ArgumentError('Vai trò Gateway không hợp lệ: $role');
    }
  }

  String get displayName {
    switch (this) {
      case GatewayRole.owner:
        return 'Chủ sở hữu';
      case GatewayRole.operator:
        return 'Vận hành';
      case GatewayRole.viewer:
        return 'Người xem';
    }
  }
}

/// DTO biểu diễn một trạm Gateway trong danh sách trả về.
@immutable
class GatewayItemDto {
  final String gatewayId;
  final String name;
  final String? description;
  final GatewayRole role;
  final DateTime? createdAt;

  const GatewayItemDto({
    required this.gatewayId,
    required this.name,
    this.description,
    required this.role,
    this.createdAt,
  });

  factory GatewayItemDto.fromJson(Map<String, dynamic> json) {
    return GatewayItemDto(
      gatewayId: json['gateway_id'] as String,
      name: json['name'] as String,
      description: json['description'] as String?,
      role: GatewayRole.fromString(json['role'] as String),
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'] as String).toLocal()
          : null,
    );
  }

  Map<String, dynamic> toJson() => {
        'gateway_id': gatewayId,
        'name': name,
        'description': description,
        'role': role.name,
        'created_at': createdAt?.toUtc().toIso8601String(),
      };
}

/// DTO phản hồi của API GET /v1/gateways
@immutable
class GatewayListResponse {
  final List<GatewayItemDto> items;

  const GatewayListResponse({required this.items});

  factory GatewayListResponse.fromJson(Map<String, dynamic> json) {
    final rawList = json['items'] as List<dynamic>? ?? [];
    final items = rawList
        .map((e) => GatewayItemDto.fromJson(e as Map<String, dynamic>))
        .toList(growable: false);
    return GatewayListResponse(items: items);
  }
}

/// DTO biểu diễn một cảm biến trong danh sách trả về.
@immutable
class SensorItemDto {
  final String sensorId;
  final String name;
  final String? unit;
  final DateTime? createdAt;

  const SensorItemDto({
    required this.sensorId,
    required this.name,
    this.unit,
    this.createdAt,
  });

  factory SensorItemDto.fromJson(Map<String, dynamic> json) {
    return SensorItemDto(
      sensorId: json['sensor_id'] as String,
      name: json['name'] as String,
      unit: json['unit'] as String?,
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'] as String).toLocal()
          : null,
    );
  }

  Map<String, dynamic> toJson() => {
        'sensor_id': sensorId,
        'name': name,
        'unit': unit,
        'created_at': createdAt?.toUtc().toIso8601String(),
      };
}

/// DTO phản hồi của API GET /v1/gateways/{gateway_id}/sensors
@immutable
class SensorListResponse {
  final String gatewayId;
  final List<SensorItemDto> items;

  const SensorListResponse({
    required this.gatewayId,
    required this.items,
  });

  factory SensorListResponse.fromJson(Map<String, dynamic> json) {
    final rawList = json['items'] as List<dynamic>? ?? [];
    final items = rawList
        .map((e) => SensorItemDto.fromJson(e as Map<String, dynamic>))
        .toList(growable: false);
    return SensorListResponse(
      gatewayId: json['gateway_id'] as String,
      items: items,
    );
  }
}

/// DTO chuẩn cho phong bì lỗi từ Backend
@immutable
class ApiErrorDto {
  final String code;
  final String message;
  final String requestId;

  const ApiErrorDto({
    required this.code,
    required this.message,
    required this.requestId,
  });

  factory ApiErrorDto.fromJson(Map<String, dynamic> json) {
    return ApiErrorDto(
      code: json['code'] as String? ?? 'unknown_error',
      message: json['message'] as String? ?? 'An unexpected error occurred',
      requestId: json['request_id'] as String? ?? '',
    );
  }
}

@immutable
class ApiErrorEnvelopeDto {
  final ApiErrorDto error;

  const ApiErrorEnvelopeDto({required this.error});

  factory ApiErrorEnvelopeDto.fromJson(Map<String, dynamic> json) {
    return ApiErrorEnvelopeDto(
      error: ApiErrorDto.fromJson(json['error'] as Map<String, dynamic>),
    );
  }
}

// ---------------------------------------------------------------------------
// DTOs minh họa cho nhóm Stage 4: Telemetry History & Digital Twin
// Lưu ý an toàn số lớn: Các trường sequence_num, count, reported_version,
// desired_version là decimal string trên wire JSON. Khi parse bằng int.parse(),
// cần lưu ý trên nền tảng JavaScript/Web số nguyên an toàn tối đa là 2^53 - 1.
// Sử dụng BigInt.parse() nếu có khả năng giá trị vượt quá giới hạn này.
// ---------------------------------------------------------------------------

/// Điểm dữ liệu thô (raw telemetry point)
@immutable
class TelemetryRawPointDto {
  final DateTime measuredAt;
  final String sequenceNum; // decimal string
  final double value;

  const TelemetryRawPointDto({
    required this.measuredAt,
    required this.sequenceNum,
    required this.value,
  });

  factory TelemetryRawPointDto.fromJson(Map<String, dynamic> json) {
    return TelemetryRawPointDto(
      measuredAt: DateTime.parse(json['measured_at'] as String).toUtc(),
      sequenceNum: json['sequence_num'] as String,
      value: (json['value'] as num).toDouble(),
    );
  }
}

/// Điểm dữ liệu tổng hợp theo bucket (downsampled bucket point)
@immutable
class TelemetryBucketPointDto {
  final DateTime bucket;
  final double avg;
  final double min;
  final double max;
  final String count; // decimal string

  const TelemetryBucketPointDto({
    required this.bucket,
    required this.avg,
    required this.min,
    required this.max,
    required this.count,
  });

  factory TelemetryBucketPointDto.fromJson(Map<String, dynamic> json) {
    return TelemetryBucketPointDto(
      bucket: DateTime.parse(json['bucket'] as String).toUtc(),
      avg: (json['avg'] as num).toDouble(),
      min: (json['min'] as num).toDouble(),
      max: (json['max'] as num).toDouble(),
      count: json['count'] as String,
    );
  }
}

/// DTO phản hồi lịch sử chuỗi thời gian GET /v1/telemetry/history
@immutable
class TelemetryHistoryResponseDto {
  final String gatewayId;
  final String sensorId;
  final String mode; // "raw" hoặc "bucket"
  final String resolution;
  final DateTime from;
  final DateTime to;
  final int pointCount; // numeric integer
  final List<TelemetryRawPointDto>? rawPoints;
  final List<TelemetryBucketPointDto>? bucketPoints;

  const TelemetryHistoryResponseDto({
    required this.gatewayId,
    required this.sensorId,
    required this.mode,
    required this.resolution,
    required this.from,
    required this.to,
    required this.pointCount,
    this.rawPoints,
    this.bucketPoints,
  });

  factory TelemetryHistoryResponseDto.fromJson(Map<String, dynamic> json) {
    final mode = json['mode'] as String;
    final rawList = json['points'] as List<dynamic>? ?? [];

    List<TelemetryRawPointDto>? rawPoints;
    List<TelemetryBucketPointDto>? bucketPoints;

    if (mode == 'raw') {
      rawPoints = rawList
          .map((e) => TelemetryRawPointDto.fromJson(e as Map<String, dynamic>))
          .toList(growable: false);
    } else {
      bucketPoints = rawList
          .map((e) => TelemetryBucketPointDto.fromJson(e as Map<String, dynamic>))
          .toList(growable: false);
    }

    return TelemetryHistoryResponseDto(
      gatewayId: json['gateway_id'] as String,
      sensorId: json['sensor_id'] as String,
      mode: mode,
      resolution: json['resolution'] as String,
      from: DateTime.parse(json['from'] as String).toUtc(),
      to: DateTime.parse(json['to'] as String).toUtc(),
      pointCount: json['point_count'] as int,
      rawPoints: rawPoints,
      bucketPoints: bucketPoints,
    );
  }
}

/// DTO phản hồi trạng thái hiện thời GET /v1/digital-twins/{entity_id}/state
@immutable
class TwinStateResponseDto {
  final String id;
  final String type;
  final String gatewayId;
  final Map<String, dynamic> reportedState;
  final Map<String, dynamic> desiredState;
  final String reportedVersion; // decimal string
  final String desiredVersion;  // decimal string
  final DateTime? lastReportedAt;
  final DateTime? lastDesiredAt;
  final DateTime updatedAt;

  const TwinStateResponseDto({
    required this.id,
    required this.type,
    required this.gatewayId,
    required this.reportedState,
    required this.desiredState,
    required this.reportedVersion,
    required this.desiredVersion,
    this.lastReportedAt,
    this.lastDesiredAt,
    required this.updatedAt,
  });

  factory TwinStateResponseDto.fromJson(Map<String, dynamic> json) {
    return TwinStateResponseDto(
      id: json['id'] as String,
      type: json['type'] as String,
      gatewayId: json['gateway_id'] as String,
      reportedState: json['reported_state'] as Map<String, dynamic>? ?? {},
      desiredState: json['desired_state'] as Map<String, dynamic>? ?? {},
      reportedVersion: json['reported_version'] as String,
      desiredVersion: json['desired_version'] as String,
      lastReportedAt: json['last_reported_at'] != null
          ? DateTime.parse(json['last_reported_at'] as String).toUtc()
          : null,
      lastDesiredAt: json['last_desired_at'] != null
          ? DateTime.parse(json['last_desired_at'] as String).toUtc()
          : null,
      updatedAt: DateTime.parse(json['updated_at'] as String).toUtc(),
    );
  }
}
```

---

### 7.2. Định nghĩa Ngoại lệ ứng dụng (Custom App Exceptions)

Tạo file `lib/core/network/exceptions/app_exceptions.dart`:

```dart
/// Lớp ngoại lệ cơ sở cho toàn bộ lỗi giao tiếp REST API
abstract class AppException implements Exception {
  final String message;
  final String code;
  final String requestId;
  final int? statusCode;

  const AppException({
    required this.message,
    required this.code,
    required this.requestId,
    this.statusCode,
  });

  @override
  String toString() =>
      '$runtimeType [HTTP $statusCode - $code]: $message (RequestID: $requestId)';
}

/// Lỗi 400 Bad Request
class BadRequestException extends AppException {
  const BadRequestException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 400);
}

/// Lỗi 401 Unauthorized
class UnauthorizedException extends AppException {
  const UnauthorizedException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 401);
}

/// Lỗi 403 Forbidden (Không đủ thẩm quyền, ví dụ thiếu quyền platform_admins)
class ForbiddenException extends AppException {
  const ForbiddenException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 403);
}

/// Lỗi 404 Not Found (Tài nguyên không tồn tại hoặc không có quyền xem)
class NotFoundException extends AppException {
  const NotFoundException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 404);
}

/// Lỗi 503 Service Unavailable (Timeout / DB bận)
class ServiceUnavailableException extends AppException {
  const ServiceUnavailableException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 503);
}

/// Lỗi 500 Internal Server Error
class ServerException extends AppException {
  const ServerException({
    required super.message,
    required super.code,
    required super.requestId,
  }) : super(statusCode: 500);
}

/// Lỗi mất kết nối mạng hoặc bị chặn bởi chính sách CORS trên trình duyệt
class NetworkException extends AppException {
  const NetworkException({required super.message})
      : super(code: 'network_or_cors_error', requestId: '', statusCode: null);
}
```

---

### 7.3. Triển khai HTTP Client Service sử dụng Dio trên Flutter Web

Tạo file `lib/core/network/api_client.dart`:

> **Lưu ý tương thích Web:** Tuyệt đối không import `dart:io` trong file này. Tất cả tên header được sử dụng trực tiếp dưới dạng chuỗi chuẩn (`'Accept'`, `'Authorization'`).

```dart
import 'package:dio/dio.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:uuid/uuid.dart';

import 'dto/api_models.dart';
import 'exceptions/app_exceptions.dart';

class ApiClient {
  final Dio _dio;
  final SupabaseClient _supabase;
  final Uuid _uuid = const Uuid();

  ApiClient({
    required String baseUrl,
    required SupabaseClient supabase,
    Dio? customDio,
  })  : _supabase = supabase,
        _dio = customDio ??
            Dio(
              BaseOptions(
                // BACKEND_API_BASE_URL chuẩn luôn kết thúc bằng '/v1' (ví dụ http://localhost/v1 hoặc https://api.example.com/v1).
                // Các request bên dưới dùng đường dẫn tương đối không có dấu gạch chéo đầu ('gateways') để tránh bẫy URI reset.
                baseUrl: baseUrl.endsWith('/') ? baseUrl : '$baseUrl/',
                connectTimeout: const Duration(seconds: 10),
                receiveTimeout: const Duration(seconds: 10),
                headers: {
                  'Accept': 'application/json',
                },
              ),
            ) {
    _setupInterceptors();
  }

  void _setupInterceptors() {
    _dio.interceptors.add(
      InterceptorsWrapper(
        onRequest: (options, handler) async {
          // 1. Tự động sinh X-Request-ID cho mỗi request để phục vụ Distributed Tracing
          options.headers['X-Request-ID'] = _uuid.v4();

          // 2. Lấy Access Token từ session hiện tại của Supabase
          final session = _supabase.auth.currentSession;
          final accessToken = session?.accessToken;

          if (accessToken != null && accessToken.isNotEmpty) {
            options.headers['Authorization'] = 'Bearer $accessToken';
          }

          // 3. Chốt chặn bảo mật phía Client: Tuyệt đối không cho phép query user_id
          if (options.queryParameters.containsKey('user_id')) {
            return handler.reject(
              DioException(
                requestOptions: options,
                error: 'Vi phạm bảo mật: Không được phép truyền query parameter user_id.',
                type: DioExceptionType.cancel,
              ),
            );
          }

          return handler.next(options);
        },
        onError: (DioException error, handler) {
          // Xử lý chuyển đổi DioException thành AppException chuẩn
          final appException = _mapDioErrorToAppException(error);
          return handler.reject(
            DioException(
              requestOptions: error.requestOptions,
              response: error.response,
              type: error.type,
              error: appException,
            ),
          );
        },
      ),
    );
  }

  AppException _mapDioErrorToAppException(DioException error) {
    final response = error.response;

    // Đặc thù trên Flutter Web: Khi bị chặn bởi CORS hoặc mất kết nối mạng,
    // trình duyệt không trả về response (response == null) và ẩn chi tiết lỗi.
    if (response == null) {
      final isConnectionOrCors = error.type == DioExceptionType.connectionError ||
          error.type == DioExceptionType.connectionTimeout ||
          (error.message != null &&
              (error.message!.contains('XMLHttpRequest') ||
                  error.message!.contains('Failed to fetch')));

      final errorMessage = isConnectionOrCors
          ? 'Không thể kết nối đến máy chủ hoặc request bị chặn bởi chính sách CORS của trình duyệt.'
          : 'Lỗi kết nối mạng: ${error.message ?? 'Không xác định'}';

      return NetworkException(message: errorMessage);
    }

    final statusCode = response.statusCode ?? 500;
    // Yêu cầu Nginx phải có header Access-Control-Expose-Headers: X-Request-ID để Web đọc được
    final requestIdHeader = response.headers.value('x-request-id') ?? '';

    // Trích xuất error envelope từ body nếu có
    String code = 'unknown_error';
    String message = 'Đã xảy ra lỗi không xác định từ máy chủ.';
    String requestId = requestIdHeader;

    if (response.data is Map<String, dynamic>) {
      try {
        final envelope =
            ApiErrorEnvelopeDto.fromJson(response.data as Map<String, dynamic>);
        code = envelope.error.code;
        message = envelope.error.message;
        if (envelope.error.requestId.isNotEmpty) {
          requestId = envelope.error.requestId;
        }
      } catch (_) {
        // Fallback nếu response không theo chuẩn envelope
      }
    }

    switch (statusCode) {
      case 400:
        return BadRequestException(
          code: code,
          message: message,
          requestId: requestId,
        );
      case 401:
        return UnauthorizedException(
          code: code,
          message: message,
          requestId: requestId,
        );
      case 403:
        return ForbiddenException(
          code: code,
          message: message,
          requestId: requestId,
        );
      case 404:
        return NotFoundException(
          code: code,
          message: message,
          requestId: requestId,
        );
      case 503:
        return ServiceUnavailableException(
          code: code,
          message: message,
          requestId: requestId,
        );
      case 500:
      default:
        return ServerException(
          code: code,
          message: message,
          requestId: requestId,
        );
    }
  }

  /// API 1: Lấy danh sách Gateway được phân quyền của người dùng hiện tại
  Future<List<GatewayItemDto>> getGateways() async {
    try {
      // BACKEND_API_BASE_URL đã kết thúc bằng '/v1' (ví dụ http://localhost/v1).
      // Gọi đường dẫn tương đối 'gateways' (KHÔNG có leading slash '/') để Dio ghép chuẩn.
      final response = await _dio.get<Map<String, dynamic>>('gateways');
      final data = response.data;
      if (data == null) return [];
      final responseDto = GatewayListResponse.fromJson(data);
      return responseDto.items;
    } on DioException catch (e) {
      if (e.error is AppException) {
        throw e.error as AppException;
      }
      rethrow;
    }
  }

  /// API 2: Lấy danh sách Cảm biến thuộc về một Gateway
  Future<SensorListResponse> getSensors(String gatewayId) async {
    // Client-side regex check để tối ưu UX trước khi gửi mạng
    final regex = RegExp(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$');
    if (!regex.hasMatch(gatewayId) || gatewayId == 'backend_service') {
      throw const BadRequestException(
        code: 'invalid_request',
        message: 'Mã gateway_id không đúng định dạng hợp lệ.',
        requestId: '',
      );
    }

    try {
      // Đường dẫn tương đối 'gateways/$gatewayId/sensors' không có leading slash
      final response = await _dio.get<Map<String, dynamic>>(
        'gateways/$gatewayId/sensors',
      );
      final data = response.data;
      if (data == null) {
        return SensorListResponse(gatewayId: gatewayId, items: []);
      }
      return SensorListResponse.fromJson(data);
    } on DioException catch (e) {
      if (e.error is AppException) {
        throw e.error as AppException;
      }
      rethrow;
    }
  }

  /// API Nhóm Stage 4: Truy vấn lịch sử Telemetry chuỗi thời gian
  Future<TelemetryHistoryResponseDto> getTelemetryHistory({
    required String gatewayId,
    required String sensorId,
    required DateTime from,
    required DateTime to,
    String resolution = 'auto',
    int? maxPoints,
  }) async {
    try {
      final queryParams = <String, dynamic>{
        'gateway_id': gatewayId,
        'sensor_id': sensorId,
        'from': from.toUtc().toIso8601String(),
        'to': to.toUtc().toIso8601String(),
        'resolution': resolution,
      };
      if (maxPoints != null) {
        queryParams['max_points'] = maxPoints;
      }

      final response = await _dio.get<Map<String, dynamic>>(
        'telemetry/history',
        queryParameters: queryParams,
      );
      final data = response.data;
      if (data == null) {
        throw const ServerException(
          code: 'internal_error',
          message: 'Dữ liệu phản hồi trống.',
          requestId: '',
        );
      }
      return TelemetryHistoryResponseDto.fromJson(data);
    } on DioException catch (e) {
      if (e.error is AppException) {
        throw e.error as AppException;
      }
      rethrow;
    }
  }

  /// API Nhóm Stage 4: Đọc trạng thái Digital Twin hiện thời (twin_states)
  Future<TwinStateResponseDto> getTwinState(String entityId) async {
    try {
      // Chỉ encodeComponent một lần duy nhất cho URN
      final encodedId = Uri.encodeComponent(entityId);
      final response = await _dio.get<Map<String, dynamic>>(
        'digital-twins/$encodedId/state',
      );
      final data = response.data;
      if (data == null) {
        throw const ServerException(
          code: 'internal_error',
          message: 'Dữ liệu trạng thái phản hồi trống.',
          requestId: '',
        );
      }
      return TwinStateResponseDto.fromJson(data);
    } on DioException catch (e) {
      if (e.error is AppException) {
        throw e.error as AppException;
      }
      rethrow;
    }
  }
}
```

---

## 8. Bảng kiểm tra tích hợp Client (Integration Verification Checklist trên Web)

Lập trình viên Client cần đối soát kỹ các trường hợp kiểm thử sau trên trình duyệt (sử dụng Chrome DevTools - tab Network và Console) trước khi bàn giao giao diện:

| STT | Kịch bản kiểm thử | Hành vi mong đợi từ Backend / Nginx | Hành vi xử lý trên Flutter Web Client |
|:---:|---|---|---|
| **1** | Kiểm tra kết nối mạng và CORS Preflight khi Web Client chạy Cross-Origin (ví dụ `localhost:3000` -> `localhost:80/v1`). | Khảo sát đúng thực trạng: Nginx hiện chưa có header CORS cho `/v1/` và Go Backend chưa có CORS middleware (trả về `405 Method Not Allowed` khi nhận preflight `OPTIONS`). | Trình duyệt sẽ chặn request nếu thiếu reverse proxy trung gian. Cần sử dụng planned reverse proxy / cấu hình Nginx CORS có kiểm chứng; tuyệt đối không tắt bảo mật trình duyệt hay dùng tiện ích unblocker. |
| **2** | Đọc header `X-Request-ID` từ response trong mã Dart trên Web. | Response trả về header `X-Request-ID` (giá trị log-safe ASCII hợp lệ do client truyền hoặc UUIDv4 do server sinh fallback). | Mã Dart đọc được chuỗi định danh từ `response.headers.value('x-request-id')` (khi proxy biên có gắn `Access-Control-Expose-Headers`). |
| **3** | Người dùng mới tinh vừa được tạo (chưa được gán vào `user_gateways`). | `HTTP 200 OK`, `{"items": []}`, `Cache-Control: no-store`. | Hiển thị trạng thái trống (Empty State): *"Bạn chưa được phân quyền truy cập trạm quan trắc nào"*. |
| **4** | Gọi API khi Access Token hết hạn (> 1 giờ) hoặc token rác. | `HTTP 401 Unauthorized`, `WWW-Authenticate: Bearer`, code `unauthorized`. | Gọi hàm làm mới token (`auth.refreshSession()`). Nếu thất bại, chuyển về màn hình đăng nhập. |
| **5** | Nhấn vào trạm `gateway_001` có gắn 3 cảm biến. | `HTTP 200 OK`, `{"gateway_id": "gateway_001", "items": [...]}`. | Hiển thị bảng/danh sách cảm biến kèm tên và đơn vị (`unit`). |
| **6** | Nhấn vào trạm `gateway_002` mà trạm này chưa có cảm biến nào. | `HTTP 200 OK`, `{"gateway_id": "gateway_002", "items": []}`. | Hiển thị trạng thái: *"Trạm này hiện chưa lắp đặt cảm biến"*. |
| **7** | Cố tình gọi `GET /v1/gateways/gw_khac/sensors` (trạm của người khác hoặc không tồn tại). | `HTTP 404 Not Found`, code `not_found`, message `resource not found`. | Bắt `NotFoundException`, hiển thị: *"Không tìm thấy thông tin trạm hoặc bạn không có quyền truy cập trạm này"*. |
| **8** | Cố tình truyền `?user_id=...` trên thanh URL. | `HTTP 400 Bad Request`, code `invalid_request`. | Client Interceptor chặn từ sớm không gửi lên mạng; nếu lọt qua, bắt `BadRequestException`. |
| **9** | Truyền `gateway_id` chứa ký tự đặc biệt như dấu cách hoặc tiếng Việt có dấu. | `HTTP 400 Bad Request`, code `invalid_request`. | Client kiểm tra regex trước khi gửi; hiển thị thông báo lỗi định dạng ngay trên giao diện. |
| **10** | Tắt Nginx hoặc mất mạng khi Web client đang chạy. | Trình duyệt ném lỗi `"XMLHttpRequest error"` hoặc `"Failed to fetch"`, `response == null`. | Bắt `NetworkException`, hiển thị cảnh báo: *"Không thể kết nối đến máy chủ hoặc request bị chặn bởi chính sách CORS của trình duyệt"*. |
| **11** | Nhấn F5 / Ctrl+R làm mới trang trên trình duyệt Web. | Phiên đăng nhập được phục hồi từ Web Storage, gửi request với token mới. | Dữ liệu danh sách trạm được nạp lại bình thường, không bị văng đăng nhập hay lỗi routing. |
| **12** | Gọi vào WebSocket stub chưa mở nghiệp vụ (`GET /v1/ws`). | `HTTP 501 Not Implemented`, code `not_implemented`, message `endpoint not implemented`. | Không crash ứng dụng, hiển thị thông báo tính năng thời gian thực đang phát triển (Giai đoạn 7). |
| **13** | Người dùng thông thường cố truy cập endpoint quản trị (`/v1/admin/*`). | `HTTP 403 Forbidden`, code `forbidden`, message `insufficient permissions`. | Bắt `ForbiddenException`, thông báo không đủ quyền quản trị viên nền tảng và điều hướng về trang chủ. |
| **14** | Chủ sở hữu (`owner`) khởi tạo Sensor mới hợp lệ (`PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`). | `HTTP 201 Created`, trả về thông tin Sensor kèm `entity_id` dạng `urn:ngsi-ld:Sensor:<gw>:<sensor>` và `created_at`. Header `Cache-Control: no-store`, `Pragma: no-cache`. | Cập nhật danh sách cảm biến hiển thị trên giao diện, hiển thị thông báo tạo thành công. |
| **15** | Chủ sở hữu phát lại chính xác Sensor đã tồn tại với cùng `name` và `unit` (Exact Replay). | `HTTP 200 OK`, trả về thông tin Sensor nguyên bản, giữ nguyên `created_at` ban đầu. | Xử lý idempotent an toàn, coi như thao tác thành công mà không báo lỗi xung đột. |
| **16** | Chủ sở hữu gửi cấu hình `name` hoặc `unit` khác lên Sensor đã tồn tại. | `HTTP 409 Conflict`, code `conflict`, message `resource conflict`. | Bắt lỗi xung đột cấu hình, thông báo: *"Cảm biến đã tồn tại với thông tin khác. Hệ thống không hỗ trợ cập nhật cảm biến qua endpoint này"*. |
| **17** | Người vận hành (`operator`) hoặc Người xem (`viewer`) gọi khởi tạo Sensor (`PUT /v1/gateways/{id}/sensors/{id}`). | `HTTP 403 Forbidden`, code `forbidden`, message `access denied`. | Bắt `ForbiddenException`, ẩn hoặc vô hiệu hóa nút/giao diện tạo cảm biến đối với vai trò không phải `owner`. |
| **18** | Khởi tạo Sensor nhưng gửi body chứa trường lạ không hỗ trợ (ví dụ `sensor_type`). | `HTTP 400 Bad Request`, code `invalid_request`. | Client kiểm soát form nhập liệu chặt chẽ, chỉ gửi `name` và `unit`. |
| **19** | Khởi tạo Sensor nhưng thiếu hoặc sai header `Content-Type`. | `HTTP 415 Unsupported Media Type`, code `unsupported_media_type`. | Đảm bảo HTTP Client luôn đính kèm header `Content-Type: application/json`. |
| **20** | Thành viên trạm truy vấn lịch sử `GET /v1/telemetry/history` với khoảng thời gian hợp lệ. | `HTTP 200 OK`, `Cache-Control: no-store`, trả về `mode`, `resolution`, `point_count`, mảng `points` kèm `sequence_num` (string) và `value`. | Render biểu đồ chuỗi thời gian chính xác, xử lý điểm raw hoặc bucket points. |
| **21** | Truy vấn lịch sử `raw` nhưng số điểm trong khoảng vượt quá `max_points` (ví dụ >1000 điểm). | `HTTP 400 Bad Request`, code `raw_limit_exceeded`. | Bắt lỗi giới hạn dữ liệu; thông báo người dùng thu hẹp khoảng thời gian hoặc chuyển sang chế độ phân giải `auto` / bucket. |
| **22** | Non-member (hoặc Platform Admin không có membership) đọc lịch sử telemetry hoặc Digital Twin state/detail. | `HTTP 404 Not Found`, code `not_found`, message `resource not found`. | Hiển thị thông báo không tìm thấy tài nguyên (bảo vệ anti-enumeration). |
| **23** | Người dùng xem danh sách Digital Twin (`GET /v1/digital-twins`) có phân trang keyset cursor. | `HTTP 200 OK`, trả về `items` và `next_cursor` (opaque base64url). | Hiển thị danh sách; truyền `cursor=next_cursor` để tải trang tiếp theo; reset cursor về null khi đổi bộ lọc. |
| **24** | Đọc chi tiết Digital Twin (`GET /v1/digital-twins/{entity_id}`). | `HTTP 200 OK`, header `Content-Type: application/ld+json`, đối tượng NGSI-LD kèm inline `@context` Object 1.1 và phân trang quan hệ `hasSensor`. | Hiển thị thông tin thực thể, thuộc tính và danh sách cảm biến gắn kèm. |
| **25** | Gateway Owner cập nhật tên Sensor (`PATCH /v1/digital-twins/{sensor_urn}`). | `HTTP 200 OK`, `Cache-Control: no-store`, `Pragma: no-cache`, cập nhật `name` thành công. | Cập nhật tên cảm biến hiển thị trên giao diện người dùng. |
| **26** | Gateway Owner cố tình `PATCH` Gateway cha hoặc cố cập nhật trường `unit` của Sensor. | Với Gateway: `HTTP 403 Forbidden` (`forbidden`). Với Sensor `unit`: `HTTP 400 Bad Request` (`invalid_request`). | Client vô hiệu hóa form đổi tên Gateway đối với Owner; chặn trường `unit` trên form chỉnh sửa cảm biến. |
| **27** | Vai trò `operator` hoặc `viewer` cố thực hiện `POST` hoặc `PATCH` Digital Twin. | `HTTP 403 Forbidden`, code `forbidden`, message `insufficient permissions`. | Ẩn toàn bộ nút tạo mới và chỉnh sửa trên giao diện đối với người dùng không phải owner/admin. |

---

## 9. Tài liệu liên quan và Bước tiếp theo

1. **Hợp đồng API Quản trị nền tảng (Admin API Contract):**
   Đặc tả chi tiết 7 endpoint dành cho Platform Admin (danh mục kiểm kê Gateway, khởi tạo Gateway/Sensor và quản lý vòng đời chứng chỉ MQTT, kèm ngoại lệ rào chắn reset fail-closed), cấu trúc header `Idempotency-Key`, hạn mức body (`AdminMaxBodyBytes`), và danh mục mã lỗi chuyên sâu (400, 401, 403, 409, 413, 415, 500, 503):
   -> Xem [`docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md).

2. **Quy chuẩn Tích hợp và Quản lý trạng thái giao diện (UI State Handling):**
   Hướng dẫn kiến trúc phân lớp, tầng Repository, xử lý trạng thái Loading/Empty/Error trên Bloc/Riverpod, và quy trình kiểm thử tích hợp thực tế:
   -> Xem [`docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).
