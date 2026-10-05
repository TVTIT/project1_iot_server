# Tài liệu Client — 07: Quy chuẩn Hợp đồng Admin API (Platform Admin API Client Contract)

**Cập nhật:** 2026-10-05  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:**  
- `AGENTS.md` (Mục 5, 6, 6.5, 7, 10, 15, 16, 18)  
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan hệ thống)  
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực người dùng và Quản lý phiên)  
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Cơ chế phân quyền người dùng và trạm Gateway)  
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API đọc nghiệp vụ chuẩn)  
- `docs/client/05_UPCOMING_FEATURES_ROADMAP.md` (Lộ trình phát triển tính năng các giai đoạn)  
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)  
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái Client)  
- `docs/backend/stage-2-task-2.4-provisioning.md` (Chi tiết kỹ thuật Admin Provisioning)  
- `docs/backend/stage-2-task-2.6-acceptance.md` (Báo cáo nghiệm thu và kiến trúc Mosquitto DynSec Ingress Gate)  
- `docs/backend/platform-admin-account-management-goals.md` (Mục tiêu và ranh giới quản lý tài khoản)  

---

## 1. Mục tiêu và Ranh giới Phạm vi (Scope & Architectural Boundaries)

Tài liệu này định nghĩa **Hợp đồng giao tiếp HTTP API Quản trị nền tảng (Platform Admin API Client Contract)** chính thức cho 6 endpoint quản trị đã được hiện thực trong mã nguồn Go backend (`src/cmd/server`, `src/internal/httpserver`).

### 1.1 Sáu (06) Endpoint Quản trị Đã Được Hiện Thực

1. `PUT /v1/admin/gateways/{gateway_id}`: Khởi tạo mới hoặc kiểm tra lũy quyền thực thể Gateway, gán người sở hữu (`owner`) ban đầu, và tự động tạo đồ thị Digital Twin (`twin_entities`, `twin_states`).
2. `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`: Khởi tạo mới hoặc kiểm tra lũy quyền Sensor thuộc Gateway cha, tự động tạo đồ thị Digital Twin và thiết lập quan hệ phân cấp `twin_relationships` (`hasSensor`).
3. `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`: Truy vấn metadata trạng thái thông tin xác thực MQTT của Gateway (không chứa mật khẩu).
4. `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`: Cấp phát ban đầu thông tin xác thực MQTT (DynSec username/password) cho Gateway; trả về mật khẩu plaintext một lần duy nhất.
5. `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`: Xoay vòng mật khẩu MQTT cho Gateway đang hoạt động (`active`); trả về mật khẩu mới một lần duy nhất.
6. `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`: Thu hồi thông tin xác thực MQTT của Gateway, ngắt kết nối socket hiện tại và vô hiệu hóa quyền truy cập broker.

### 1.2 Ranh giới Kiến trúc và Mô hình Quản trị Tập trung

- **Không bắt buộc xây dựng ứng dụng Web Admin riêng biệt (No Custom Admin App Requirement):** Các thao tác quản trị nền tảng được thiết kế phục vụ công cụ vận hành tin cậy của Platform Administrator (công cụ dòng lệnh CLI, curl script, hoặc Supabase Studio phối hợp công cụ bảo vệ). Ứng dụng Flutter Web Client thông thường chỉ tiêu thụ các API đọc nghiệp vụ chuẩn ([Tài liệu 04](04_REST_API_CLIENT_CONTRACT.md)) trừ khi người dùng đăng nhập là một Platform Admin đã được thẩm định.
- **Thẩm quyền Platform Admin tách biệt hoàn toàn bảng thành viên (`user_gateways`):**
  - Quyền quản trị tối cao của Platform Admin được định đoạt duy nhất bằng sự tồn tại của `user_id` trong bảng cơ sở dữ liệu `public.platform_admins` tại PostgreSQL.
  - Tư cách thành viên trong `user_gateways` (`owner`, `operator`, `viewer`) **đứng riêng lẻ hoàn toàn không cấp quyền Platform Admin**; người dùng chỉ có vai trò trong `user_gateways` mà không có bản ghi trong `platform_admins` sẽ nhận mã lỗi `403 Forbidden` khi truy cập `/v1/admin/*`.
  - **Tài khoản có thể mang đồng thời cả hai vai trò độc lập (Legitimate Dual-Role):** Một người dùng hoàn toàn có thể vừa là thành viên (ví dụ: `owner` của một Gateway) vừa độc lập là một Platform Admin trong `platform_admins`. Hệ thống thẩm định từng ranh giới độc lập, không cấm tài khoản mang hai vai trò song song.
  - Platform Admin **không tự động bypass** quyền đọc nghiệp vụ: Nếu một Admin chưa được phân quyền thành viên trong `user_gateways` cho một Gateway cụ thể, khi gọi API đọc nghiệp vụ `GET /v1/gateways` sẽ nhận về danh sách rỗng (`{"items":[]}`), và gọi `GET /v1/gateways/{gateway_id}/sensors` sẽ nhận `404 Not Found` (xem [Tài liệu 03](03_USER_GATEWAY_AUTHORIZATION.md)).
- **Không sáng tác endpoint tự kiểm tra quyền Admin (No Admin Self-Query Endpoint):** Go backend hiện tại không cung cấp endpoint kiểu `/v1/admin/me` hay `/v1/admin/whoami`. Phía client không được sáng tác hoặc gọi các endpoint giả định này.
- **Không có API CRUD người dùng hoặc quản lý thành viên qua Go Backend:** Việc tạo tài khoản người dùng và gán phân quyền thành viên `user_gateways` hiện vẫn được thực thi thông qua công cụ quản trị hạ tầng của Supabase Auth (Admin API / Studio) và script nội bộ an toàn (xem [Tài liệu 03](03_USER_GATEWAY_AUTHORIZATION.md)). Khóa hạ tầng đặc quyền `service_role` tuyệt đối không bao giờ được cấu hình hay chia sẻ cho client.
- **Trạng thái cấu hình Root Docker Compose: Tính năng Credential API mặc định TẮT:**
  - Trong tệp cấu hình triển khai `docker-compose.yml`, biến môi trường được đặt mặc định là:
    ```yaml
    MQTT_CREDENTIAL_API_ENABLED: "false"
    ```
  - **Quy trình triển khai có rào chắn (Rollout Gates):** Tính năng này chỉ được kích hoạt có chủ đích bởi kỹ sư vận hành hạ tầng khi triển khai broker DynSec. Phía Client **không thể tự ý bật tính năng này thông qua cờ hay header HTTP**.
  - **Thứ tự ưu tiên trước cờ tính năng (Precedence before feature gate):** Request thiếu hoặc sai JWT luôn nhận `401 Unauthorized` trước; request từ người dùng không phải admin luôn nhận `403 Forbidden` trước. Chỉ khi một Platform Admin đã xác thực hợp lệ gọi vào thì server mới kiểm tra cờ tính năng và phản hồi mã lỗi HTTP `503 Service Unavailable` (`code: "credential_runtime_disabled"`).

---

## 2. Thứ tự Ưu tiên Ủy quyền và Xử lý Lỗi Toàn cục (Authorization Precedence & Global Error Handling)

Mọi request gửi tới 6 endpoint quản trị đều phải trải qua chuỗi middleware bảo mật nghiêm ngặt theo đúng thứ tự ưu tiên sau:

```text
[HTTP Request]
       |
       v
1. Request ID & Cache Gate Middleware
   - Sinh hoặc xác thực X-Request-ID (1..128 ký tự an toàn)
   - Route credential tự động gán: Cache-Control: no-store, Pragma: no-cache
       |
       v
2. Authentication Middleware (/v1/*)
   - Kiểm tra header: Authorization: Bearer <human_supabase_jwt>
   - Xác thực chữ ký HS256 cục bộ, thời hạn hiệu lực, issuer, audience
   - Bắt buộc claim: role == "authenticated"
   -> Thất bại: HTTP 401 Unauthorized (code: "unauthorized", message: "authentication required")
       |
       v
3. Platform Admin Authorization Middleware (/v1/admin/*)
   - Tra cứu user_id trong bảng PostgreSQL public.platform_admins
   -> Lỗi kết nối / timeout / cancelled database: HTTP 503 Service Unavailable (code: "service_unavailable")
   -> Không tồn tại trong platform_admins: HTTP 403 Forbidden (code: "forbidden", message: "insufficient permissions")
       |
       v
4. Endpoint Feature Gate & Startup Barrier (Dành riêng cho 4 route Credential)
   - Kiểm tra cờ cấu hình CredentialAPIEnabled:
     -> Nếu false: HTTP 503 Service Unavailable (code: "credential_runtime_disabled")
   - Kiểm tra rào chắn khởi động CredentialStartupReadiness.Ready():
     -> Nếu chưa sẵn sàng hoặc nil dependency: HTTP 503 Service Unavailable (code: "service_unavailable")
       |
       v
5. Request Parser & Input Validation
   - Kiểm tra Query Parameters, Content-Type, Kích thước Body, Cú pháp JSON khắt khe
   -> Vi phạm: HTTP 400 Bad Request, HTTP 413 Payload Too Large, hoặc HTTP 415 Unsupported Media Type
       |
       v
6. Transactional Business Logic & Database Invariant Verification
   - Kiểm tra lại quyền Platform Admin trong giao dịch database (phòng chống race condition)
   -> Thất bại nghiệp vụ: HTTP 404, HTTP 409, HTTP 503, HTTP 500 với mã lỗi tương ứng
```

### 2.1 Định dạng Phong bì Lỗi Chuẩn (`httpapi.APIErrorEnvelope`)

Toàn bộ các phản hồi lỗi từ Go backend luôn tuân thủ cấu trúc phong bì JSON chuẩn, đồng thời đính kèm header HTTP `X-Request-ID`:

```http
HTTP/1.1 403 Forbidden
Content-Type: application/json; charset=utf-8
Cache-Control: no-store
X-Request-ID: 00000000-0000-0000-0000-000000000001
```

```json
{
  "error": {
    "code": "forbidden",
    "message": "insufficient permissions",
    "request_id": "00000000-0000-0000-0000-000000000001"
  }
}
```

---

## 3. Chi tiết 02 Endpoint Quản trị Khởi tạo Gateway và Sensor (Provisioning APIs)

Cả hai endpoint khởi tạo này đều áp dụng phương thức `PUT`, cho phép tạo mới hoặc thực hiện lại yêu cầu (retry lũy quyền - idempotent) một cách an toàn.

### 3.1 Quy chuẩn Phân tích Cú pháp Nghiêm ngặt (Strict Parser Rules)

Theo hiện thực tại `src/internal/httpserver/provisioning_request.go`:
1. **Cấm tuyệt đối Query Parameters:** Mọi URL chứa query string (kể cả dấu hỏi chấm rỗng `?`) đều lập tức nhận mã lỗi HTTP `400 Bad Request` (`code: "invalid_request"`).
2. **Kiểm tra Media Type (`Content-Type`):** Bắt buộc phải là `application/json`. Tham số duy nhất được phép đi kèm là `charset=utf-8` (không phân biệt hoa thường). Bất kỳ media type hoặc tham số nào khác đều trả về HTTP `415 Unsupported Media Type` (`code: "unsupported_media_type"`).
3. **Giới hạn kích thước Body (`ADMIN_MAX_BODY_BYTES`):** Giới hạn tối đa được cấu hình trên server (mặc định 16384 bytes = 16 KiB; dải hợp lệ 1..1048576 bytes = 1 MiB). Nếu body vượt quá giới hạn, server trả về HTTP `413 Payload Too Large` (`code: "payload_too_large"`).
4. **Cú pháp JSON nghiêm ngặt:**
   - Chỉ chấp nhận duy nhất một đối tượng JSON `{ ... }`.
   - **Cấm trường lạ (unknown fields):** Bất kỳ trường nào ngoài danh sách cho phép đều bị từ chối với HTTP `400 Bad Request`.
   - **Cấm trùng lặp khóa (duplicate keys):** Kể cả khi tên trường được escape khác nhau, server từ chối ngay lập tức với HTTP `400 Bad Request`.
   - **An toàn Unicode:** Chuỗi JSON thô được quét trước; từ chối các ký tự Unicode surrogate đơn lẻ không ghép cặp (`\uD800`–`\uDFFF`) hoặc escape chứa NUL (`\u0000`).
   - **Tính không thể gán null (Non-nullable):** Các trường bắt buộc không được mang giá trị `null`.

---

### 3.2 Khởi tạo Trạm Gateway: `PUT /v1/admin/gateways/{gateway_id}`

Khởi tạo thực thể Gateway mới, gán người sở hữu ban đầu, và tự động khởi tạo thực thể Digital Twin trong một giao dịch cơ sở dữ liệu nguyên tử (`READ COMMITTED`).

#### A. Tham số Đường dẫn (Path Parameters)
- `gateway_id` (string, bắt buộc): Định danh duy nhất của Gateway.
  - Định dạng: Bắt đầu bằng chữ cái hoặc chữ số, chỉ chứa các ký tự `[A-Za-z0-9_-]`, độ dài từ 1 đến 64 ký tự (Regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`).
  - **Cấm giá trị dành riêng:** Tuyệt đối không được đặt tên là `backend_service` (trả về HTTP `400 Bad Request`).

#### B. Cấu trúc Request Body (JSON)
| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `name` | string | **Có** | Tên hiển thị của Gateway. Không được rỗng hoặc chỉ chứa khoảng trắng. Tối đa **256 UTF-8 bytes**. |
| `description` | string hoặc null | Không | Mô tả trạm Gateway. Tối đa **4096 UTF-8 bytes**. Có thể bỏ qua hoặc truyền `null`. |
| `owner_user_id` | string (UUID) | **Có** | Định danh người dùng sở hữu ban đầu. Phải là chuỗi UUID hợp lệ khác nil UUID và **bắt buộc phải tồn tại từ trước trong bảng `public.profiles`**. |

*Ví dụ Request:*
```http
PUT /v1/admin/gateways/gw-station-alpha
Authorization: Bearer <valid_platform_admin_jwt>
Content-Type: application/json
Accept: application/json

{
  "name": "Gateway Tram Quan Trac Alpha",
  "description": "Tram thu thap du lieu moi truong khu vuc A",
  "owner_user_id": "00000000-0000-0000-0000-000000000002"
}
```

#### C. Cấu trúc Response Body (JSON)
- `gateway_id` (string): Khóa định danh của Gateway.
- `name` (string): Tên hiển thị nguyên bản.
- `description` (string hoặc null): Mô tả Gateway.
- `owner_user_id` (string UUID): UUID của người sở hữu ban đầu.
- `entity_id` (string): URN định danh chuẩn NGSI-LD (`urn:ngsi-ld:Gateway:<gateway_id>`).
- `created_at` (string RFC3339 UTC hoặc null): Thời điểm tạo bản ghi trong CSDL.

#### D. Các Mã Trạng thái và Trường hợp Phản hồi
- **HTTP 201 Created:** Khởi tạo thành công Gateway mới cùng đồ thị Digital Twin liên kết.
  ```json
  {
    "gateway_id": "gw-station-alpha",
    "name": "Gateway Tram Quan Trac Alpha",
    "description": "Tram thu thap du lieu moi truong khu vuc A",
    "owner_user_id": "00000000-0000-0000-0000-000000000002",
    "entity_id": "urn:ngsi-ld:Gateway:gw-station-alpha",
    "created_at": "2026-10-05T08:30:00Z"
  }
  ```
- **HTTP 200 OK (Idempotent Retry):** Gateway đã tồn tại và request gửi lên có payload trùng khớp hoàn toàn (`name`, `description`, `owner_user_id` giống hệt bản ghi đã lưu). Server trả về dữ liệu hiện tại với mã `200 OK`.
- **HTTP 400 Bad Request (`code: "invalid_request"`):** Vi phạm định dạng `gateway_id` (chứa ký tự lạ, quá 64 bytes, hoặc là `backend_service`), `name` rỗng, vượt quá giới hạn độ dài bytes, `owner_user_id` không phải UUID hợp lệ hoặc nil UUID, chứa trường lạ, hoặc chứa URL query.
- **HTTP 404 Not Found (`code: "not_found"`):** Không tìm thấy `owner_user_id` trong bảng `public.profiles`.
- **HTTP 409 Conflict (`code: "conflict"`):** Gateway đã tồn tại nhưng request PUT mới có thông tin thay đổi (thay đổi `name`, `description`, hoặc cố tình thay đổi `owner_user_id`). API PUT này là cơ chế khởi tạo và retry lũy quyền, không hỗ trợ đổi chủ sở hữu hoặc cập nhật metadata khác biệt.
- **HTTP 413 Payload Too Large (`code: "payload_too_large"`):** Kích thước body vượt quá `ADMIN_MAX_BODY_BYTES`.
- **HTTP 415 Unsupported Media Type (`code: "unsupported_media_type"`):** `Content-Type` không phải `application/json`.
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Gián đoạn kết nối database khi kiểm tra quyền Platform Admin trong giao dịch hoặc timeout.

---

### 3.3 Khởi tạo Sensor: `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`

Khởi tạo thực thể Sensor mới trực thuộc Gateway cha, tự động khởi tạo thực thể Digital Twin (`twin_entities`, `twin_states`) và thiết lập mối quan hệ phân cấp `twin_relationships` (`hasSensor`) trong cùng một giao dịch.

#### A. Tham số Đường dẫn (Path Parameters)
- `gateway_id` (string, bắt buộc): Định danh của Gateway cha. Phải tuân thủ biểu thức regex và **bắt buộc phải tồn tại từ trước**.
- `sensor_id` (string, bắt buộc): Định danh của Sensor.
  - Định dạng: Bắt đầu bằng chữ cái hoặc chữ số, chỉ chứa các ký tự `[A-Za-z0-9_-]`, độ dài từ 1 đến 64 ký tự (Regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`).
  - *Lưu ý:* Sensor cho phép sử dụng định danh `backend_service` (khác với Gateway cấm giá trị này).

#### B. Cấu trúc Request Body (JSON)
Theo đúng parser `parseSensorProvisioning` trong mã nguồn Go, request body của Sensor **chỉ tiếp nhận duy nhất hai trường: `name` và `unit`**. Tuyệt đối không có trường `sensor_type` (nếu truyền trường này sẽ bị coi là unknown field và nhận HTTP 400 ngay lập tức).

| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `name` | string | **Có** | Tên cảm biến. Không được rỗng hoặc chỉ chứa khoảng trắng. Tối đa **256 UTF-8 bytes**. |
| `unit` | string hoặc null | Không | Đơn vị đo lường (ví dụ: `"degC"`, `"percent"`). Tối đa **64 UTF-8 bytes**. Có thể bỏ qua hoặc truyền `null`. |

*Ví dụ Request:*
```http
PUT /v1/admin/gateways/gw-station-alpha/sensors/sensor-dht22-temp
Authorization: Bearer <valid_platform_admin_jwt>
Content-Type: application/json
Accept: application/json

{
  "name": "Cam Bien Nhiet Do Moi Truong",
  "unit": "degC"
}
```

#### C. Cấu trúc Response Body (JSON)
- `gateway_id` (string): Khóa Gateway cha.
- `sensor_id` (string): Khóa định danh của Sensor.
- `name` (string): Tên Sensor.
- `unit` (string hoặc null): Đơn vị đo.
- `entity_id` (string): URN định danh chuẩn NGSI-LD (`urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`).
- `created_at` (string RFC3339 UTC hoặc null): Thời điểm tạo bản ghi trong CSDL.

#### D. Các Mã Trạng thái và Trường hợp Phản hồi
- **HTTP 201 Created:** Khởi tạo thành công Sensor mới và đồ thị quan hệ phân cấp liên kết với Gateway cha.
  ```json
  {
    "gateway_id": "gw-station-alpha",
    "sensor_id": "sensor-dht22-temp",
    "name": "Cam Bien Nhiet Do Moi Truong",
    "unit": "degC",
    "entity_id": "urn:ngsi-ld:Sensor:gw-station-alpha:sensor-dht22-temp",
    "created_at": "2026-10-05T08:35:00Z"
  }
  ```
- **HTTP 200 OK (Idempotent Retry):** Sensor đã tồn tại dưới Gateway này và request PUT có payload trùng khớp hoàn toàn (`name` và `unit` giống hệt bản ghi đã lưu).
- **HTTP 400 Bad Request (`code: "invalid_request"`):** Vi phạm định dạng `gateway_id` hoặc `sensor_id`, `name` rỗng, vượt quá giới hạn bytes, chứa trường lạ (như `sensor_type`), chứa duplicate keys, hoặc có query string.
- **HTTP 404 Not Found (`code: "not_found"`):** Gateway cha `gateway_id` không tồn tại trong cơ sở dữ liệu.
- **HTTP 409 Conflict (`code: "conflict"`):** Sensor đã tồn tại nhưng payload PUT mới có thông tin khác biệt (`name` hoặc `unit` bị sửa đổi).
- **HTTP 413 Payload Too Large / HTTP 415 Unsupported Media Type:** Vi phạm kích thước body hoặc media type.
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Gián đoạn kết nối database khi kiểm tra quyền trong giao dịch.

---

## 4. Chi tiết 04 Endpoint Quản trị Thông tin Xác thực MQTT (MQTT Credential APIs)

Nhóm endpoint này kiểm soát vòng đời thông tin xác thực trên Mosquitto Dynamic Security (DynSec) và quản lý cổng Ingress Gate của broker.

### 4.1 Quy chuẩn Bắt buộc đối với Request Quản trị Credential

Căn cứ hiện thực tại `src/internal/httpserver/admin_mqtt_credential_request.go`:
1. **Cấm hoàn toàn Request Body (BodyNone):**
   - Cả 4 phương thức `GET`, `POST`, `POST /rotate`, và `DELETE` **bắt buộc phải có body rỗng (`len(body) == 0`)**.
   - Nếu Client gửi kèm bất kỳ nội dung nào trong body (dù chỉ là chuỗi rỗng có độ dài > 0), server sẽ lập tức từ chối với mã lỗi HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
   - Nếu kích thước body gửi lên vượt quá giới hạn đọc tối đa (`limit`), server trả về HTTP `413 Request Entity Too Large` (`code: "request_too_large"`, `message: "request body too large"`).
   - *Lưu ý về Content-Type và HTTP 415:* Do các endpoint Credential yêu cầu body rỗng hoàn toàn, parser không đọc `Content-Type`. Mã lỗi HTTP `415 Unsupported Media Type` **chỉ áp dụng riêng cho 2 endpoint Provisioning PUT**, tuyệt đối không phát sinh trên các endpoint Credential.
2. **Cấm hoàn toàn Query Parameters:**
   - URL tuyệt đối không được chứa query string hoặc ký tự `?` (cả `RawQuery != ""` và `ForceQuery`). Vi phạm sẽ nhận HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
3. **Bắt buộc Header `Idempotency-Key` đối với các thao tác biến đổi trạng thái (Mutations):**
   - Áp dụng cho: `POST` (provision), `POST /rotate`, và `DELETE` (revoke).
   - **Quy chuẩn UUID:** Parser sử dụng `uuid.Parse(values[0])` và kiểm tra `key != uuid.Nil`. Yêu cầu đúng **một header `Idempotency-Key` chứa giá trị UUID hợp lệ khác nil UUID** (server không hạn chế riêng version 4, tuy nhiên khuyến nghị client sinh UUIDv4 hoặc UUIDv7 ngẫu nhiên, ví dụ: `Idempotency-Key: c4b1e6e0-2495-4e3a-97a6-6f8d38e21a41`).
   - Nếu thiếu header này, gửi nhiều hơn một header, hoặc UUID không hợp lệ/nil UUID -> Server trả về HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
   - Đối với phương thức `GET` (truy vấn metadata): Header `Idempotency-Key` không bắt buộc và sẽ bị bỏ qua nếu có.
4. **Header điều khiển bộ nhớ đệm (Cache-Control):**
   - Middleware `credentialCacheMiddleware` chạy trước tầng xác thực để đảm bảo kể cả các phản hồi lỗi nhạy cảm cũng tự động được gán:
     ```http
     Cache-Control: no-store
     Pragma: no-cache
     ```
   - Đảm bảo trình duyệt và các proxy trung gian tuyệt đối không lưu vết dữ liệu mật khẩu hoặc trạng thái xác thực nhạy cảm.

---

### 4.2 Nguyên tắc Phân phối Bí mật Một Lần Duy Nhất và Khôi phục Sự cố (One-Time Secret Delivery & Replay Semantics)

1. **Phân phối một lần duy nhất (First-Response Only):**
   - Mật khẩu MQTT plaintext (được sinh bởi CSPRNG với ít nhất 128 bit entropy) **chỉ xuất hiện duy nhất trong response thành công đầu tiên** của lệnh tạo mới (`POST`, HTTP 201) hoặc xoay vòng (`POST /rotate`, HTTP 200).
   - **Không cam kết zeroization vật lý trong bộ nhớ (No Physical Memory Zeroization Guarantee):** Hàm `ClearSecret()` trong Go runtime chỉ đơn thuần xóa tham chiếu chuỗi mật khẩu trong struct Go (`r.password = ""`) sau khi xử lý response để phục vụ bộ thu gom rác (garbage collection) tự nhiên; runtime không cam kết zeroization bộ nhớ RAM vật lý hay xóa sạch các bản sao do Go runtime/caller tạo ra.
   - Go backend **tuyệt đối không lưu mật khẩu plaintext trong database, không lưu trong file log, không đưa vào bộ nhớ đệm, và không có bất kỳ API nào cho phép export/đọc lại mật khẩu đã cấp**. Không có cam kết exactly-once, durability hay bảo đảm vận chuyển một lần; nếu secret trong response đầu tiên bị thất lạc trên mạng thì secret đó vĩnh viễn không thể đọc lại.
2. **Cơ chế Replay khi dùng cùng `Idempotency-Key`:**
   - Nếu Client thực hiện lại (replay) cùng phương thức, cùng đường dẫn, cùng actor, và cùng `Idempotency-Key`:
     - Server **không sinh mật khẩu mới**, và **tuyệt đối không cấp lại mật khẩu cũ**.
     - Dữ liệu `Metadata` trả về trong kết quả replay phản ánh **thẩm quyền và trạng thái HIỆN TẠI (CURRENT authority)** từ cơ sở dữ liệu, không phải dữ liệu kiểm toán cũ đã lỗi thời.
     - **Phân định rõ kết quả replay theo trạng thái thao tác gốc:**
       - Nếu thao tác trước đó vẫn đang xử lý (`pending`): Server trả về mã lỗi HTTP `409 Conflict` (`code: "operation_in_progress"`).
       - Nếu thao tác trước đó đang ở trạng thái cần phục hồi (`recovery_needed`): Server trả về mã lỗi HTTP `503 Service Unavailable` (`code: "credential_recovery_required"`).
       - Nếu thao tác trước đó đã thất bại (`failed`): Hàm `writeCredentialMutation` ánh xạ sang phản hồi lỗi (mã HTTP 500 hoặc mã lỗi cụ thể từ `ErrorCode`) với thông báo `"credential request failed"`, **tuyệt đối không trả về mã 200/201 thành công**.
       - Nếu thao tác trước đó đã hoàn tất thành công (`succeeded`): Server phản hồi bản ghi **metadata-only** với HTTP 201 (đối với provision) hoặc HTTP 200 (đối với rotate/revoke); trường `secret_returned` mang giá trị `false`, tuyệt đối không có trường `password`, trường `replayed_status` mang giá trị `"succeeded"`, và trường `next_action` hướng dẫn bước thao tác tiếp theo.
     - *Lưu ý về bằng chứng runtime:* Cờ bằng chứng `snapshot_observed` ghi nhận việc tệp snapshot native đã được quan sát, **không phải là bằng chứng fsync hay bằng chứng chống mất điện (power-loss proof)**.
3. **Quy trình Khôi phục khi Mất Mát Phản hồi trên Mạng (Lost Secret Recovery - Kịch bản E22):**
   - Trường hợp Client gửi request thành công, cơ sở dữ liệu đã commit giao dịch, Mosquitto đã nạp mật khẩu mới, nhưng phản hồi HTTP bị đứt gãy trên đường truyền (network drop / client crash) khiến Admin không nhận được mật khẩu:
     - **Không thể lấy lại mật khẩu cũ:** Gửi lại request với `Idempotency-Key` cũ chỉ nhận về bản ghi metadata (`secret_returned: false`), không bao giờ nhận lại mật khẩu.
     - **Biện pháp khôi phục chuẩn:**
       - Nếu trạng thái hiện tại là `active`: Kiểm tra xác thực quyền và thực hiện gọi endpoint xoay vòng mật khẩu (`POST /rotate`) với một **`Idempotency-Key` mới** để cấp phát một cặp mật khẩu mới thay thế.
       - Nếu trạng thái hiện tại là `revoked`: Chỉ thực hiện cấp mới (`POST`) với một **`Idempotency-Key` mới** sau khi các tiến trình đối soát (reconciliation) và rào chắn bảo dưỡng (maintenance fences) đã được giải quyết sạch sẽ.
       - *Cảnh báo rào chắn kiểm toán:* Trường hợp `status == "active"` trong metadata đơn thuần **chưa chứng minh tất cả các checkpoint chưa hoàn tất đã sạch**. Nếu vừa xảy ra tình huống bất định trong commit hoặc hoàn tất (finalization ambiguity), Platform Admin tuyệt đối không được blind-rotate mù quáng; phải kiểm tra kỹ trạng thái kiểm toán và đảm bảo runtime không ở trạng thái recovery trước khi xoay vòng.

---

### 4.3 Truy vấn Metadata Credential: `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`

Truy vấn thông tin kiểm toán và trạng thái kích hoạt thông tin xác thực MQTT hiện tại của Gateway.

#### A. Tham số Request
- Path param: `gateway_id` (string, bắt buộc).
- Header bắt buộc: `Authorization: Bearer <token>`. `Accept: application/json` là khuyến nghị, không phải điều kiện parser.
- Request Body: Rỗng. Không có query parameters.

#### B. Cấu trúc Response Body (`Metadata` DTO)
| Trường | Kiểu dữ liệu | Ý nghĩa & Quy tắc JSON |
|---|---|---|
| `gateway_id` | string | Khóa định danh của Gateway. |
| `username` | string | Tên đăng nhập MQTT của Gateway (thường trùng với `gateway_id`). |
| `credential_version` | integer | Phiên bản credential hiện tại (tăng đơn điệu sau mỗi lần cấp mới/xoay vòng). |
| `status` | string | Trạng thái nghiệp vụ: `"provisioning"`, `"active"`, `"rotating"`, `"revoking"`, `"revoked"`, `"failed"`, `"recovery_needed"`. |
| `last_operation_id` | string (UUID) | Định danh của thao tác kiểm toán gần nhất trên CSDL. |
| `operation_status` | string | Trạng thái thao tác gần nhất: `"pending"`, `"succeeded"`, `"failed"`, `"recovery_needed"`, `"resolved_by_recovery"`. |
| `activated_at` | string hoặc null | Thời điểm kích hoạt gần nhất (RFC3339 UTC). Không có `omitempty` (luôn trả về `null` trong JSON khi chưa kích hoạt). |
| `revoked_at` | string hoặc null | Thời điểm thu hồi gần nhất (RFC3339 UTC). Không có `omitempty` (luôn trả về `null` trong JSON khi chưa thu hồi). |
| `last_error_code` | string hoặc null | Mã lỗi kiểm toán gần nhất. Không có `omitempty` (luôn trả về `null` trong JSON khi không có lỗi). |

*Lưu ý về các trường tùy chọn trong `MutationResult` (`POST`, `POST /rotate`, `DELETE`):*
- Trường `operation_id` (string UUID): Định danh của thao tác hiện tại hoặc thao tác gốc khi replay.
- Trường `secret_returned` (boolean): `true` trong response thành công đầu tiên chứa mật khẩu, `false` trong mọi trường hợp replay hoặc revoke.
- Trường `next_action` (string, `omitempty`): Chỉ xuất hiện khi có chỉ dẫn thao tác tiếp theo (ví dụ: `"rotate_with_new_idempotency_key"` hoặc `"provision_with_new_idempotency_key"`).
- Trường `replayed_status` (string, `omitempty`): Chỉ xuất hiện trong các phản hồi replay, phản ánh trạng thái gốc (`"succeeded"`).
- Trường `error_code` (string, `omitempty`): Chỉ xuất hiện khi phản hồi replay mang thông tin lỗi kiểm toán đã lưu.

*Ví dụ Response (HTTP 200 OK):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null
}
```

---

### 4.4 Cấp mới Credential Ban đầu: `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`

Cấp mới thông tin xác thực MQTT cho Gateway chưa có credential hoặc đã bị thu hồi (`revoked`).

#### A. Tham số Request
- Path param: `gateway_id` (string, bắt buộc).
- Header bắt buộc:
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Idempotency-Key: <unique_non_nil_uuid>`
   - Header khuyến nghị (không bắt buộc): `Accept: application/json`
- Request Body: Bắt buộc rỗng. Không có query parameters.

#### B. Phản hồi Phân phối Mật khẩu Lần đầu (HTTP 201 Created - First Response)
Server phản hồi DTO `SecretResult`: bao gồm toàn bộ các trường của `Metadata`, bổ sung định danh thao tác `operation_id`, cờ `secret_returned: true`, và chuỗi mật khẩu `password`.

*Ví dụ Response (Mô tả trường mật khẩu an toàn, không chứa giá trị thực):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000010",
  "password": "<one_time_generated_secure_password_string>",
  "secret_returned": true
}
```

#### C. Phản hồi Khi Replay Cùng `Idempotency-Key` (HTTP 201 Created - Replay Response)
Server phản hồi DTO `MutationResult`: trường `secret_returned` mang giá trị `false`, tuyệt đối không có trường `password`.

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000010",
  "secret_returned": false,
  "replayed_status": "succeeded",
  "next_action": "rotate_with_new_idempotency_key"
}
```

#### D. Lỗi Xung đột Thường gặp
- **HTTP 409 Conflict (`code: "credential_conflict"`):** Gateway này hiện đã có thông tin xác thực đang ở trạng thái `active`. Để đổi mật khẩu, phải gọi sang endpoint `POST /rotate`.

---

### 4.5 Xoay vòng Credential: `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`

Xoay vòng mật khẩu cho Gateway đang ở trạng thái `active`.

#### A. Tham số Request
- Tương tự endpoint `POST` cấp mới: Yêu cầu header `Idempotency-Key: <unique_non_nil_uuid>`, body rỗng, không có query parameters.

#### B. Phản hồi Phân phối Mật khẩu Lần đầu (HTTP 200 OK - First Response)
Server trả về mã HTTP `200 OK` (khác với mã `201 Created` của lệnh cấp mới). Cấu trúc tương tự `SecretResult`, với `credential_version` được tăng lên (ví dụ: từ 1 lên 2), kèm mật khẩu mới và `secret_returned: true`.

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000020",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T09:00:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000020",
  "password": "<one_time_newly_rotated_password_string>",
  "secret_returned": true
}
```

#### C. Phản hồi Khi Replay Cùng `Idempotency-Key` (HTTP 200 OK - Replay Response)
Trả về HTTP `200 OK` với DTO `MutationResult`, không có mật khẩu, `secret_returned: false`, `next_action: "rotate_with_new_idempotency_key"`.

#### D. Lỗi Xung đột Thường gặp
- **HTTP 409 Conflict (`code: "credential_conflict"`):** Gateway chưa từng có thông tin xác thực hoặc đang ở trạng thái `revoked`, không thể thực hiện xoay vòng.

---

### 4.6 Thu hồi Credential: `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`

Thu hồi toàn bộ quyền truy cập MQTT của Gateway, vô hiệu hóa tài khoản trên DynSec và ngắt socket kết nối.

#### A. Tham số Request
- Method: `DELETE`.
- Header bắt buộc: `Authorization: Bearer <token>`, `Idempotency-Key: <unique_non_nil_uuid>`.
- Request Body: Bắt buộc rỗng. Không có query parameters.

#### B. Phản hồi Thu hồi Thành công (HTTP 200 OK)
Server luôn trả về mã HTTP `200 OK` (không có mật khẩu) với cấu trúc `MutationResult`:
- `status`: `"revoked"`
- `revoked_at`: Thời điểm thu hồi (RFC3339 UTC)
- `secret_returned`: `false`
- `next_action`: `"provision_with_new_idempotency_key"`

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "revoked",
  "last_operation_id": "00000000-0000-0000-0000-000000000030",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T09:00:00Z",
  "revoked_at": "2026-10-05T09:30:00Z",
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000030",
  "secret_returned": false,
  "next_action": "provision_with_new_idempotency_key"
}
```

*Lưu ý về Replay và No-op của Revoke:*
- Nếu gọi lại `DELETE` với **cùng `Idempotency-Key`**: Trả về `200 OK` với `replayed_status: "succeeded"`.
- Nếu gọi `DELETE` với một **`Idempotency-Key` mới** trên một Gateway đã bị thu hồi (`revoked`):
  - Theo đúng hiện thực tại `postgres_operation.go`, CSDL nhận diện đây là thao tác không phát sinh mutation mới và trả về bản ghi no-op replay mang `OperationID: uuid.Nil` và `next_action: "provision_with_new_idempotency_key"`.
  - Dịch vụ Go backend (`service.go`) **chủ động gọi hàm kiểm tra native trên broker runtime (`runtime.CheckRevoked(ctx, in.GatewayID)`)** để xác minh độc lập rằng Mosquitto DynSec thực sự không còn tài khoản client nào của Gateway này.
  - Nếu xác minh thành công và metadata CSDL không bị thay đổi đồng thời: Server trả về `200 OK` với `operation_id: "00000000-0000-0000-0000-000000000000"`, `secret_returned: false`, `next_action: "provision_with_new_idempotency_key"`.
  - Nếu kiểm tra native thất bại hoặc metadata không nhất quán: Server lập tức kích hoạt quy trình thoát an toàn (CloseDrain), đánh dấu trạng thái poisoned để phong tỏa runtime, và trả về mã lỗi HTTP `503 Service Unavailable` (`code: "credential_recovery_required"`, `message: "credential request failed"`).

---

## 5. Bảng Ánh xạ Mã Lỗi Nghiệp vụ Credential Chi tiết (Error Code Mapping Table)

Theo hiện thực tại `writeCredentialError` (`src/internal/httpserver/admin_mqtt_credential_handlers.go`), đối với mọi lỗi nghiệp vụ miền Credential, thông báo lỗi trong phong bì JSON luôn là `message: "credential request failed"`. Các lỗi phát sinh tại tầng middleware hoặc request parser sẽ mang thông báo lỗi chuẩn riêng biệt:

| Mã lỗi HTTP | Mã lỗi nghiệp vụ (`code`) | Thông báo lỗi (`message`) | Nguyên nhân kích hoạt trong hệ thống | Hướng xử lý phía Client / Administrator |
|---|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` *(parser)* hoặc `credential request failed` *(domain)* | Vi phạm cú pháp: Body không rỗng (`len(body) > 0`), URL chứa query string (`?`), `gateway_id` sai regex, hoặc thiếu/sai `Idempotency-Key`. | Kiểm tra lại request: xóa body, xóa query string, truyền đúng 1 UUID hợp lệ khác nil cho `Idempotency-Key`. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Thiếu token JWT, token hết hạn, sai chữ ký HS256, hoặc claim `role != "authenticated"`. | Đăng nhập lại qua Supabase Auth để lấy access token mới. |
| **403 Forbidden** | `forbidden` | `insufficient permissions` *(middleware)* hoặc `credential request failed` *(domain)* | Người dùng đã xác thực nhưng `user_id` không tồn tại trong bảng `public.platform_admins`. | Chỉ Platform Admin mới có quyền thực thi. Quyền `owner` Gateway đơn thuần không cấp quyền admin. |
| **404 Not Found** | `not_found` | `credential request failed` | Gateway không tồn tại trong bảng `public.gateways` hoặc chưa từng được cấp phát credential khi gọi GET. | Kiểm tra lại `gateway_id` hoặc gọi API khởi tạo Gateway trước. |
| **409 Conflict** | `credential_conflict` | `credential request failed` | Xung đột trạng thái credential: Cố tình gọi `POST` (provision) khi đang `active`, hoặc gọi `POST /rotate` khi đang `revoked`/chưa cấp phát. | Kiểm tra lại trạng thái hiện tại qua `GET` để gọi đúng endpoint (xoay vòng hay cấp mới). |
| **409 Conflict** | `operation_in_progress` | `credential request failed` | Tái sử dụng `Idempotency-Key` trong khi thao tác trước đó vẫn đang trong trạng thái xử lý (`pending`). | Chờ thao tác trước đó hoàn tất trước khi thử lại với cùng key. |
| **409 Conflict** | `idempotency_conflict` | `credential request failed` | Tái sử dụng `Idempotency-Key` nhưng gửi khác Actor, khác `gateway_id`, hoặc khác Action (`provision` vs `rotate` vs `revoke`). | Mỗi thao tác hoặc tài nguyên khác nhau bắt buộc phải dùng một `Idempotency-Key` độc lập. |
| **413 Request Entity Too Large** | `request_too_large` | `request body too large` | Kích thước body gửi lên vượt quá giới hạn đọc tối đa của server (khác với mã `payload_too_large` của Provisioning). | Đảm bảo body hoàn toàn rỗng đối với các API Credential. |
| **503 Service Unavailable** | `credential_runtime_disabled` | `credential request failed` | Cấu hình server đang tắt API Credential (`MQTT_CREDENTIAL_API_ENABLED=false`). | Cần rollout gate từ kỹ sư hạ tầng; client không thể tự bật qua cờ hay header HTTP. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` *(middleware/readiness)* hoặc `credential request failed` *(domain)* | Rào chắn khởi động chưa hoàn tất (`CredentialStartupReadiness.Ready() == false`) hoặc lỗi kết nối tra cứu admin CSDL. | Tạm dừng thao tác và thử lại sau ít phút khi dịch vụ hoàn tất chu trình khởi động. |
| **503 Service Unavailable** | `credential_runtime_busy` | `credential request failed` | Hệ thống đang trong cửa sổ bảo dưỡng hoặc xử lý một thao tác credential khác (hạn mức tiếp nhận đồng thời bị chiếm dụng). | Áp dụng cơ chế Exponential Backoff kèm Jitter và thử lại sau. |
| **503 Service Unavailable** | `credential_verification_unavailable` | `credential request failed` | Không thể kiểm chứng tính hợp lệ của credential mới trên broker Mosquitto. | Kiểm tra tình trạng hoạt động của Mosquitto broker native. |
| **503 Service Unavailable** | `credential_recovery_required` | `credential request failed` | Hệ thống rơi vào trạng thái bất định sau sự cố mạng hoặc lỗi ghi snapshot native; Ingress Gate có thể bị đóng (`CLOSED`). | Đòi hỏi Platform Admin can thiệp xử lý phục hồi hệ thống (Recovery checkpoint). |
| **503 Service Unavailable** | `credential_finalization_pending` | `credential request failed` | Thao tác trên Mosquitto đã thực hiện nhưng giao dịch ghi nhận kết quả cuối cùng trên PostgreSQL chưa hoàn tất. | Kiểm tra lại trạng thái qua `GET` metadata trước khi thực hiện bước tiếp theo. |
| **500 Internal Server Error** | `internal_error` | `credential request failed` | Lỗi nội bộ không xác định trong tiến trình Go backend. | Kiểm tra nhật ký hệ thống (log) của backend theo `request_id`. |

---

## 6. Ảnh hưởng Tác vụ Vận hành tới Kết nối Mạng và Trạng thái Broker (Broker Runtime & Network Impacts)

### 6.1 Ảnh hưởng Kết nối Mạng khi Xoay vòng và Cấp mới (Rotate / Provision)
- **Cửa sổ bảo dưỡng toàn cục (Global Maintenance Window) và Khởi động lạnh (Cold Restart):**
  - Khi thực thi xoay vòng hoặc cấp mới, phân hệ controller kích hoạt cửa sổ bảo dưỡng độc quyền, cập nhật cấu hình DynSec và thực hiện **khởi động lạnh có kiểm soát (Cold Restart)** tiến trình broker Mosquitto để đảm bảo tính bất biến của tệp snapshot lưu trữ.
  - **Hiện tượng phía Client/Gateway:** Trong thời gian diễn ra thao tác, toàn bộ các Gateway khác (ví dụ: Gateway B) đang kết nối tới broker sẽ bị ngắt kết nối socket tạm thời và tự động kết nối lại (reconnect) ngay sau khi Ingress Gate chuyển sang trạng thái `OPEN`.

### 6.2 Ảnh hưởng Kết nối Mạng khi Thu hồi (Revoke)
- **Thu hồi có mục tiêu (Targeted Revocation):**
  - Khác với Rotate, thao tác Thu hồi (`DELETE`) sử dụng cơ chế DynSec Dynamic Revoke.
  - Chỉ có kết nối socket của Gateway bị thu hồi (Gateway A) bị chấm dứt và từ chối xác thực vĩnh viễn. Các Gateway khác đang hoạt động bình thường (Gateway B) **vẫn giữ nguyên kết nối TCP socket gốc**, không bị ngắt quãng.

### 6.3 Trạng thái Bất định và Đóng Cổng Toàn cục (Uncertainty & Fail-Closed Gate)
- Nếu xảy ra sự cố nghiêm trọng (mất điện, lỗi ghi đĩa snapshot, hoặc crash tiến trình giữa chừng), cổng Ingress Gate của Mosquitto sẽ lập tức chuyển sang chế độ **Đóng an toàn (Fail-Closed / `CLOSED`)**, từ chối mọi kết nối MQTT mới cho đến khi tiến trình phục hồi (`recovery`) được thực thi.
- **Lưu ý đặc biệt về mã trạng thái:** 6 endpoint quản trị này **chỉ phản hồi các mã trạng thái HTTP 200, 201 hoặc 4xx/5xx; TUYỆT ĐỐI KHÔNG BAO GIỜ trả về mã HTTP `202 Accepted`**. Mã `202 Accepted` là mã dành riêng cho các lệnh điều khiển thiết bị (Downlink Commands) thuộc phân hệ Digital Twin trong các giai đoạn sau.

---

## 7. Khuyến nghị Vận hành và Tích hợp Phía Client (Client Integration Best Practices)

1. **Phân biệt rạch ròi các loại thông tin xác thực:**
   - **Mật khẩu MQTT của Gateway:** Do Mosquitto quản lý, chỉ dùng cho Gateway kết nối MQTT qua TLS.
   - **JWT Supabase của người dùng:** Dùng cho ứng dụng Web/Flutter xác thực với Go Backend (`Authorization: Bearer <jwt>`).
   - **Gateway HTTP Credential / Token:** Dùng cho Gateway khi gọi API lấy Signed URL tải ảnh.
   - **Supabase `service_role` Key:** Khóa bí mật hạ tầng đặc quyền cao nhất của server; tuyệt đối không bao giờ được cấu hình hoặc đưa vào mã nguồn Client.
2. **Xử lý sự cố Timeout khi kết quả không rõ ràng (Handling Timeout / Unknown Outcome):**
   - Khi gọi một mutation (`POST`, `POST /rotate`, `DELETE`) mà gặp lỗi timeout kết nối mạng (Socket Timeout / Network Drop), **tuyệt đối không tự ý sinh một `Idempotency-Key` mới và gửi lại ngay lập tức**. Việc tự ý sinh key mới sẽ gây xung đột trạng thái (`409 Conflict`) hoặc dẫn tới rủi ro tạo ra các bản ghi mồ côi.
   - **Quy trình chuẩn:**
     - Bước 1: Giữ nguyên `Idempotency-Key` vừa sử dụng và gọi lại request mutation đó. Nếu server đã xử lý xong trước đó, phản hồi replay sẽ trả về mã `200 OK` hoặc `201 Created` kèm metadata hiện tại (`secret_returned: false`).
     - Bước 2: Gọi `GET /v1/admin/gateways/{gateway_id}/mqtt-credential` để kiểm tra `credential_version`, `status` và `last_operation_id`.
     - Bước 3: Nếu xác định mật khẩu mới đã kích hoạt thành công trên server (`status == "active"`) nhưng Client bị mất phản hồi (không nhận được mật khẩu), tạo một `Idempotency-Key` mới và thực hiện lệnh `POST /rotate` để cấp mật khẩu mới có kiểm soát. Tuyệt đối không blind-rotate nếu metadata chưa rõ ràng hoặc CSDL còn checkpoint bảo dưỡng chưa hoàn tất.
3. **Tuân thủ ranh giới đọc dữ liệu:**
   - Để hiển thị danh mục Gateway và Sensor cho người dùng thông thường trên bảng điều khiển Dashboard, luôn sử dụng các API đọc nghiệp vụ chuẩn đã được kiểm thử phân quyền chặt chẽ tại [Tài liệu 04](04_REST_API_CLIENT_CONTRACT.md) (`GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors`). Không sử dụng các API quản trị `/v1/admin/*` cho luồng người dùng cuối.
4. **Tham chiếu trạng thái kiểm thử đầu-cuối:**
   - Toàn bộ hành vi xử lý mất mát phản hồi mạng (`E22`), cô lập socket khi thu hồi (`E20`), và xoay vòng mật khẩu đã được kiểm chứng độc lập trên test harness cô lập tại `docs/backend/stage-2-task-2.7-acceptance.md` và hướng dẫn kiểm thử [Tài liệu 08](08_API_INTEGRATION_AND_STATE_HANDLING.md).
