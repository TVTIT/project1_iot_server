# Tài liệu Client — 04: Quy chuẩn Hợp đồng REST API (REST API Client Contract) trên Flutter Web

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 5, 6, 6.5, 10, 16, 18)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Phân quyền người dùng và trạm)
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái Client)
- `src/internal/httpserver/router.go`
- `src/internal/httpserver/gateway_handlers.go`
- `src/internal/httpserver/sensor_handlers.go`
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
| **Admin Provisioning** | `PUT` | `/v1/admin/gateways/{gateway_id}` | Bearer JWT + `platform_admins` | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo mới hoặc phát lại chính xác Gateway (create-or-exact-replay), không phải chỉnh sửa/edit: trả về `201` nếu mới tạo, `200` nếu phát lại đúng cấu hình cũ, `409 Conflict` nếu đã tồn tại nhưng gửi cấu hình khác (tuyệt đối không có cơ chế upsert ghi đè trạng thái đã có). Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Provisioning** | `PUT` | `/v1/admin/gateways/{gateway_id}/sensors/{sensor_id}` | Bearer JWT + `platform_admins` | `201 Created` / `200 OK` / `400` / `401` / `403` / `404` / `409` / `413` / `415` / `500` / `503` | Khởi tạo mới hoặc phát lại chính xác Sensor (create-or-exact-replay), không phải chỉnh sửa/edit: trả về `201` nếu mới tạo, `200` nếu phát lại đúng cấu hình cũ; `404` nếu trạm cha không tồn tại; `409 Conflict` nếu đã tồn tại nhưng gửi cấu hình khác (không upsert ghi đè). Xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md). |
| **Admin Credentials** | `GET` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` | `200 OK` / `403` / `404` / `503` | Truy vấn metadata chứng chỉ MQTT của Gateway (Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)). |
| **Admin Credentials** | `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` | `200 OK` / `400` / `409` / `503` | Cấp phát chứng chỉ MQTT lần đầu (Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)). |
| **Admin Credentials** | `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential/rotate` | Bearer JWT + `platform_admins` | `200 OK` / `400` / `409` / `503` | Xoay vòng (rotate) chứng chỉ MQTT (Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)). |
| **Admin Credentials** | `DELETE` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` | `200 OK` / `400` / `409` / `503` | Thu hồi (revoke) chứng chỉ MQTT (Chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)). |
| **Authenticated Stub** | `GET` | `/v1/telemetry/history` | Bearer JWT (`authenticated`) | `501 Not Implemented` | Khung định tuyến chuỗi thời gian TimescaleDB (Chưa khả dụng). |
| **Authenticated Stub** | `GET` | `/v1/ws` | Bearer JWT (`authenticated`) | `501 Not Implemented` | Khung định tuyến WebSocket thời gian thực (Chưa khả dụng). |
| **Authenticated Stub** | `GET` | `/v1/digital-twins` | Bearer JWT (`authenticated`) | `501 Not Implemented` | Khung định tuyến mô hình Digital Twin (Chưa khả dụng). |

### 1.3. Ranh giới phủ định rõ ràng (Explicit Negative Boundaries)

Để tránh các giả định sai lầm khi lập trình ứng dụng Client, hệ thống xác định rõ các ranh giới phủ định sau trong Go Backend:
1. **Tuyệt đối KHÔNG CÓ route đọc lẻ 1 Gateway (`GET /v1/gateways/{gateway_id}`):** Go router không cung cấp endpoint này. Ứng dụng Client lấy thông tin chi tiết của trạm thông qua danh sách trả về từ `GET /v1/gateways`.
2. **Tuyệt đối KHÔNG CÓ route đọc lẻ 1 Sensor (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`):** Go router không cung cấp endpoint này. Thông tin toàn bộ cảm biến của trạm được trả về đầy đủ qua `GET /v1/gateways/{gateway_id}/sensors`.
3. **Tuyệt đối KHÔNG CÓ route CRUD người dùng hay quản lý thành viên `user_gateways` trên Go API:** Theo chính sách quản trị tập trung (`AGENTS.md` mục 6.5), việc tạo tài khoản người dùng và gán phân quyền thành viên trạm do Platform Admin thực hiện trực tiếp thông qua công cụ quản trị Supabase / SQL, không qua Go REST API.
4. **Các tính năng tương lai chưa khả dụng (Unavailable in Current Stage):** Toàn bộ các tính năng truy vấn lịch sử telemetry, kênh truyền thời gian thực WebSocket, tải lên/đọc tập tin media qua Supabase Storage, và điều khiển trạm / Digital Twin hiện chưa khả dụng và sẽ trả về `501 Not Implemented`.

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
- **Phạm vi gắn thêm `Pragma: no-cache`:** Middleware `credentialCacheMiddleware` trong `src/internal/httpserver/admin_mqtt_credential_handlers.go` chạy trước xác thực và thiết lập cả hai header:
  ```http
  Cache-Control: no-store
  Pragma: no-cache
  ```
  áp dụng **riêng biệt** cho các tuyến đường quản trị chứng chỉ MQTT khớp tiền tố `/v1/admin/gateways/*/mqtt-credential`.
- **Phạm vi không áp dụng (Không quy nạp toàn bộ):** Các endpoint public health (`/healthz`, `/readyz`, `/v1/health`) và các stub `501 Not Implemented` (`/v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins`) **không** thiết lập header `no-store` hay `Pragma`.
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
| **400 Bad Request** | `invalid_request` | `invalid request` | - Cố tình gửi query param `user_id` (chống mạo danh IDOR).<br>- Path param `gateway_id` sai định dạng regex hoặc dùng tên dành riêng (`backend_service`). | Sửa logic client, chuẩn hóa định dạng tham số. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | - Lỗi định danh (Identity failure): Thiếu header `Authorization`.<br>- Header không đúng scheme `Bearer <token>`.<br>- Token hết hạn, sai chữ ký HS256, hoặc sai `role`/`aud`.<br>*(Server luôn đính kèm header `WWW-Authenticate: Bearer`)*. | Xóa session, kích hoạt luồng Refresh Token (`auth.refreshSession()`) hoặc điều hướng người dùng về màn hình Đăng nhập. |
| **403 Forbidden** | `forbidden` | `insufficient permissions` | - Người dùng đã xác thực hợp lệ nhưng **không có quyền Quản trị viên nền tảng** trong bảng `platform_admins` khi cố truy cập các route `/v1/admin/*`. | Hiển thị thông báo tài khoản không có quyền quản trị; ẩn hoặc vô hiệu hóa các menu chức năng admin trên giao diện. |
| **404 Not Found** | `not_found` | `resource not found` | - `gateway_id` không tồn tại trong hệ thống.<br>- Người dùng **không có quyền** trên `gateway_id` yêu cầu (trạm của người khác / foreign read).<br>- Gọi sai đường dẫn URL không tồn tại (NoRoute). | Hiển thị thông báo không tìm thấy tài nguyên. Tuyệt đối không suy diễn tài nguyên có tồn tại hay không. |
| **405 Method Not Allowed** | `method_not_allowed` | `method not allowed` | - Gọi sai HTTP Method trên endpoint hợp lệ (ví dụ gọi `POST /v1/gateways`). `router.HandleMethodNotAllowed = true`. | Kiểm tra lại phương thức HTTP trong mã nguồn Client. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | - Lỗi máy chủ nội bộ không lường trước (lỗi đọc kết quả SQL, database query fail, panic được khôi phục bởi Recovery middleware). | Hiển thị thông báo sự cố hệ thống và ghi nhận `request_id` để hỗ trợ kỹ thuật tra cứu nhật ký. |
| **501 Not Implemented** | `not_implemented` | `endpoint not implemented` | - Gọi vào các authenticated stub đã đăng ký khung route nhưng chưa mở nghiệp vụ (`/v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins`). | Không gọi các endpoint này cho đến các giai đoạn phát triển tiếp theo. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | - Lỗi phụ thuộc hạ tầng (Dependency failure):<br>+ Thao tác truy vấn vượt quá thời hạn cho phép (`AuthorizationTimeout`, mặc định 2s) hoặc context bị hủy.<br>+ Cơ sở dữ liệu tạm thời gián đoạn khi ping kiểm tra readiness (`GET /readyz`).<br>+ Kiểm tra quyền `platform_admins` bị timeout/cancelled. | Hiển thị thông báo máy chủ tạm thời bận hoặc đang bảo trì, cho phép người dùng bấm "Thử lại" (Retry). |

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

## 6. Danh sách Tính năng và API tương lai chưa khả dụng (Future Features Unavailable)

Trong Giai đoạn 2 (Stage 2 Boundary), để bảo toàn định hình kiến trúc toàn diện của hệ thống đã thỏa thuận tại `AGENTS.md`, Go Backend (`src/internal/httpserver/router.go`) đã đăng ký sẵn khung định tuyến cho các endpoint sau nhưng tạm thời gắn handler trả về `501 Not Implemented`:

| Endpoint / Nhóm tính năng | Method | Trạng thái hiện tại | Kế hoạch triển khai & Ghi chú |
|---|:---:|:---:|---|
| `/v1/telemetry/history` | `GET` | `501 Not Implemented` | Truy vấn chuỗi thời gian TimescaleDB & Downsampling (Giai đoạn 3). |
| `/v1/ws` | `GET` | `501 Not Implemented` | Kênh truyền dữ liệu thời gian thực WebSocket (Giai đoạn 3). |
| `/v1/digital-twins` | `GET` | `501 Not Implemented` | Mô hình thực thể Digital Twin & NGSI-LD mapping (Giai đoạn 4). |
| **Media Upload & Read URLs** | `POST` / `GET` | **Chưa khả dụng** | Tạo signed upload URL và đọc ảnh từ private Supabase Storage qua Go REST API (Giai đoạn 4). |
| **Device Control & Commands** | `POST` / `PATCH` | **Chưa khả dụng** | Gửi lệnh điều khiển (`twin_commands`), cập nhật `desired_state`, và cơ chế Transactional Outbox (Giai đoạn 4). |

#### Payload phản hồi chuẩn khi gọi vào các endpoint stub `501`:
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

**Khuyến cáo nghiêm ngặt cho Client:** Lập trình viên Client tuyệt đối không được phát triển các màn hình gọi vào các endpoint chưa khả dụng trên cho đến khi có thông báo bàn giao chính thức từ đội ngũ Backend.

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
| **12** | Gọi vào các endpoint stub chưa mở nghiệp vụ (`GET /v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins`). | `HTTP 501 Not Implemented`, code `not_implemented`, message `endpoint not implemented`. | Không crash ứng dụng, hiển thị thông báo tính năng đang phát triển theo lộ trình. |
| **13** | Người dùng thông thường cố truy cập endpoint quản trị (`/v1/admin/*`). | `HTTP 403 Forbidden`, code `forbidden`, message `insufficient permissions`. | Bắt `ForbiddenException`, thông báo không đủ quyền quản trị viên nền tảng và điều hướng về trang chủ. |

---

## 9. Tài liệu liên quan và Bước tiếp theo

1. **Hợp đồng API Quản trị nền tảng (Admin API Contract):**
   Đặc tả chi tiết 6 endpoint dành cho Platform Admin (khởi tạo Gateway/Sensor và quản lý vòng đời chứng chỉ MQTT), cấu trúc header `Idempotency-Key`, hạn mức body (`AdminMaxBodyBytes`), và danh mục mã lỗi chuyên sâu (400, 403, 409, 413, 415, 503):
   -> Xem [`docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md).

2. **Quy chuẩn Tích hợp và Quản lý trạng thái giao diện (UI State Handling):**
   Hướng dẫn kiến trúc phân lớp, tầng Repository, xử lý trạng thái Loading/Empty/Error trên Bloc/Riverpod, và quy trình kiểm thử tích hợp thực tế:
   -> Xem [`docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](08_API_INTEGRATION_AND_STATE_HANDLING.md).
