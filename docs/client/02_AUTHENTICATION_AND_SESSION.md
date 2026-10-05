# Tài liệu Client — 02: Cơ chế Xác thực và Quản lý phiên làm việc trên Flutter Web (Authentication & Session Management)

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu chuẩn:** `AGENTS.md`, `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`, `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`, `docs/backend/stage-2-task-2.2-authentication.md`, `docs/backend/stage-2-task-2.2A-centralized-accounts.md`.

---

## 1. Tổng quan cơ chế xác thực và Ranh giới hệ thống

Trong kiến trúc của nền tảng IoT Gateway–Server, ứng dụng **Flutter Web** đóng vai trò là Dashboard giám sát và giao diện vận hành client. Hệ thống phân định ranh giới bảo mật và kiểm soát truy cập thành hai tầng độc lập, có Base URL và chức năng riêng biệt:

1. **Ranh giới Định danh người dùng (User Identity Layer) — Supabase Auth (GoTrue):**
   - **Base URL:** `${SUPABASE_AUTH_URL}/auth/v1/*` (được định tuyến qua Nginx Reverse Proxy tới Supabase API Gateway / Envoy).
   - **Nhiệm vụ:** Quản lý chu trình sống của tài khoản người dùng, xác thực thông tin đăng nhập (password grant), cấp phát cặp mã xác thực `access_token` (JWT) và `refresh_token`, xử lý làm mới phiên (`grant_type=refresh_token`) và thu hồi phiên (`/auth/v1/logout`).
   - Flutter Web tương tác với tầng này thông qua thư viện `supabase_flutter` hoặc REST client tương thích.

2. **Ranh giới Phân quyền nghiệp vụ (Business Authorization Layer) — Go Backend:**
   - **Base URL:** `${API_URL}/v1/*` (được định tuyến qua Nginx Reverse Proxy trực tiếp tới Go Modular Monolith).
   - **Nhiệm vụ:** Cung cấp API nghiệp vụ (Gateway, Sensor, Lịch sử đo TimescaleDB, WebSocket stream, Media metadata và Digital Twin).
   - **Cơ chế xác minh:** Go Backend **không** gọi ngược lại dịch vụ Supabase Auth trong từng request nhằm triệt tiêu độ trễ mạng và loại bỏ điểm nghẽn phụ thuộc. Thay vào đó, Go Backend tự thẩm định chữ ký JWT tại chỗ (**Local HS256 Verification**) với khóa đối xứng bí mật được chia sẻ an toàn ở tầng máy chủ (`SUPABASE_JWT_SECRET`).
   - Sau khi xác minh danh tính người dùng (`sub` claim dạng UUID), Go Backend kiểm tra quyền hạn nghiệp vụ trên tài nguyên dựa trên bảng quan hệ `user_gateways` (`owner`, `operator`, `viewer`) hoặc bảng `platform_admins` trong PostgreSQL. Go Backend hoàn toàn độc lập và **không** quản lý hay cung cấp API CRUD tài khoản người dùng.

```text
+-----------------------------------------------------------------------------------+
|                                 Flutter Web Client                                |
|                       (Trình duyệt Chrome / Edge / Firefox)                       |
|                   (supabase_flutter SDK / HTTP Client minh họa)                   |
+------------------------+----------------------------------+-----------------------+
                         |                                  |
            1. Đăng nhập | POST /auth/v1/token              | 3. Gọi Business API
               (Password |      ?grant_type=password        |    Base URL: /v1/*
                Grant)   | Header apikey: <anon_key>        |    Header:
                         |                                  |    Authorization:
                         v                                  |    Bearer <access_token>
+----------------------------------------------------+      |
|           Nginx Reverse Proxy & Envoy              |      |
+------------------------+---------------------------+      |
                         | (Proxy /auth/v1/*)               | (Proxy /v1/*)
                         v                                  v
+------------------------------------+     +----------------------------------------+
|           Supabase Auth            |     |               Go Backend               |
|              (GoTrue)              |     |           (Modular Monolith)           |
+-----------------+------------------+     +-------------------+--------------------+
                  |                                            |
                  | 2. Cấp Session                             | 4. Local HS256 Verify
                  |    - access_token (HS256)                  |    & Check Membership
                  |    - refresh_token                         |    (bảng user_gateways /
                  v                                            |     platform_admins)
+------------------------------------+                         v
|       Browser Storage (Web)        |     +----------------------------------------+
|    (Origin-scoped localStorage)    |     |       PostgreSQL / TimescaleDB         |
+------------------------------------+     +----------------------------------------+
```

### 1.1. Phân biệt các loại khóa và Token trong hệ thống

Client cần phân định rõ ràng 3 thực thể xác thực để không cấu hình sai lệch:

| Loại khóa / Token | Nơi lưu giữ & Phạm vi sử dụng | Quyền hạn & Mức độ an toàn |
|---|---|---|
| **Public Anon Key** (`SUPABASE_ANON_KEY`) | Nhúng trong cấu hình Client (`.env` client, mã nguồn Flutter Web). Được truyền trong header `apikey` khi client gọi tới Supabase Gateway (`/auth/v1/*`). | **Công khai an toàn (Safe to be public):** Bản thân anon key không trao quyền đọc/ghi dữ liệu nghiệp vụ nào trên Go Backend. Mọi API Go `/v1/*` đều từ chối anon key nếu thiếu JWT người dùng hợp lệ. |
| **Human Access Token** (`access_token` JWT) | Cấp phát cho người dùng sau khi đăng nhập thành công. Được lưu trong bộ nhớ client / storage trình duyệt. Truyền qua header `Authorization: Bearer <access_token>`. | **Có thời hạn (Thường là 3600s):** Chứa danh tính `sub` (UUID) và `role: authenticated`. Cho phép Go Backend thẩm định quyền theo `user_gateways`. Cần bảo vệ chống rò rỉ XSS. |
| **Infrastructure Service Key** (`SUPABASE_SERVICE_ROLE_KEY`) | **Chỉ lưu tại máy chủ nội bộ** (Go Backend, CLI admin script của hạ tầng). **TUYỆT ĐỐI KHÔNG** đưa vào client, web browser hay kho lưu trữ Git. | **Toàn quyền hệ thống (Bypass toàn bộ RLS và Auth):** Chỉ dùng cho các tác vụ quản trị hạ tầng (như `POST /auth/v1/admin/users`). Nếu lộ key này ra client, toàn bộ hệ thống Supabase sẽ bị xâm phạm. |

---

## 2. Chính sách quản lý tài khoản tập trung (Centralized Account Policy)

Căn cứ theo kiến trúc quy định tại `AGENTS.md` (Mục 6.5) và kết quả kiểm thử tại `docs/backend/stage-2-task-2.2A-centralized-accounts.md`:

### 2.1. Cấm đăng ký tài khoản tự do (Public Self-Signup Disabled)
- Hệ thống IoT Gateway–Server phục vụ các trạm đo công nghiệp chuyên dụng, không hỗ trợ đăng ký tài khoản công khai.
- Việc vô hiệu hóa đăng ký được quy định bắt buộc ở tầng cấu hình dịch vụ xác thực:
  ```dotenv
  # Biến môi trường của container GoTrue
  GOTRUE_DISABLE_SIGNUP=true
  ```
- **Hành vi phía Server:** Khi có request `POST /auth/v1/signup` gửi tới API Gateway:
  - **Mã HTTP:** `422 Unprocessable Entity`
  - **Payload phản hồi chuẩn từ GoTrue:**
    ```json
    {
      "code": 422,
      "error_code": "signup_disabled",
      "msg": "Signups not allowed for this instance"
    }
    ```
- **Phạm vi kiểm chứng kỹ thuật (Verification Boundary):**
  - Trạng thái `422 signup_disabled` đã được kiểm chứng tự động trong môi trường kiểm thử cách ly (test fixture `scripts/tests/stage2_auth.py`, kịch bản E02).
  - *Lưu ý quan trọng về mặt kiến trúc:* Kết quả kiểm thử fixture là bằng chứng xác nhận cấu hình chuẩn, nhưng **không phải là sự bảo đảm mặc nhiên cho mọi môi trường triển khai thực tế**. Người vận hành phải thực hiện kiểm thử độc lập trực tiếp tại biên API (`POST /auth/v1/signup`) trên môi trường staging/production trước khi coi hệ thống đã khóa hoàn toàn.
  - Việc ẩn nút "Đăng ký" trên giao diện Flutter Web chỉ là biện pháp trải nghiệm người dùng (UX cosmetic); chốt chặn an ninh quyết định nằm ở cấu hình từ chối tại máy chủ GoTrue.
- **Quy tắc bắt buộc trên Client:**
  - Không tạo màn hình đăng ký (`SignUpScreen`), không tạo biểu mẫu đăng ký.
  - Không đặt liên kết hoặc nút bấm "Đăng ký" trên màn hình đăng nhập.
  - Hiển thị thông báo rõ ràng: *"Tài khoản người dùng được cấp phát tập trung bởi Quản trị viên hệ thống (Platform Administrator)."*

### 2.2. Không hỗ trợ User CRUD và Gateway Self-Claim trên Go Backend
- **Go Backend không có API quản lý tài khoản:** Go Backend không cung cấp endpoint tạo, sửa, xóa người dùng (`User CRUD`).
- **Quyền Platform Admin không tự cấp quyền Auth Admin:** Một người dùng có quyền Quản trị viên hệ sinh thái trong bảng `platform_admins` của PostgreSQL khi gửi JWT cá nhân (`role: authenticated`) tới GoTrue Admin API (`POST /auth/v1/admin/users`) **sẽ bị GoTrue từ chối với mã lỗi 403 Forbidden**. GoTrue chỉ chấp nhận `service_role` key cho các thao tác Admin API này.
- **Không có cơ chế Gateway Self-Claim:** Người dùng không thể tự nhận trạm (claim gateway) bằng cách gửi `gateway_id`. Việc khởi tạo trạm IoT và gán quyền sở hữu (`owner_user_id`) được thực hiện tập trung bởi Platform Admin thông qua Go Admin API (`PUT /v1/admin/gateways/:gateway_id`). Chi tiết được quy định tại `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`.

---

## 3. Luồng Đăng nhập (Sign In Flow)

### 3.1. Trình tự thực thi qua Supabase API Gateway

```text
User (Web Browser)       Flutter Web Client        Nginx / Envoy Gateway        GoTrue (Supabase Auth)
     |                           |                           |                             |
     |--- Nhập Email & Mật khẩu->|                           |                             |
     |--- Nhấn Đăng nhập ------->|                           |                             |
     |                           |-- POST /auth/v1/token --->|                             |
     |                           |   ?grant_type=password    |-- Chuyển tiếp tới GoTrue -->|
     |                           |   Header apikey: <anon>   |                             |
     |                           |   Body: {email, password} |                             |-- Kiểm tra mật khẩu (bcrypt)
     |                           |                           |<-- 200 OK (access+refresh)--|-- Sinh JWT HS256 hợp lệ
     |                           |<-- 200 OK (Session) ------|                             |
     |                           |                                                         |
     |                           |-- Lưu Session vào Web Storage (localStorage)           |
     |                           |-- Cập nhật App State (Authenticated)                    |
     |<-- Chuyển hướng Dashboard-|                                                         |
```

### 3.2. Đặc tả giao thức Đăng nhập (Password Grant)

Khi đăng nhập, client gửi HTTP request tới endpoint của GoTrue:

```http
POST /auth/v1/token?grant_type=password HTTP/1.1
Host: auth.example.com
Content-Type: application/json
apikey: <SUPABASE_ANON_KEY>

{
  "email": "operator.station1@example.com",
  "password": "ExamplePassword123!"
}
```

Phản hồi thành công từ GoTrue (`200 OK`):
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "refresh_token": "8f9a2b...",
  "user": {
    "id": "b2f67232-a589-40ea-9b97-897db6746cf2",
    "aud": "authenticated",
    "role": "authenticated",
    "email": "operator.station1@example.com"
  }
}
```

Phản hồi thất bại từ GoTrue (`400 Bad Request`):
```json
{
  "code": 400,
  "error_code": "invalid_grant",
  "msg": "Invalid login credentials"
}
```
*Lưu ý:* Cấu trúc lỗi này là của GoTrue, khác với chuẩn `APIErrorEnvelope` của Go Backend.

### 3.3. Quy chuẩn thẩm định JWT tại Go Backend (Local HS256 Verification)

Go Backend thẩm định nghiêm ngặt tính hợp lệ của `access_token` tại file `src/internal/auth/verifier.go`. Client cần nắm rõ các tiêu chí kiểm tra:

1. **Thuật toán ký:** Bắt buộc là `HS256` đối xứng (`jwt.SigningMethodHS256`). Mọi thuật toán khác (`none`, `RS256`, `ES256`, `HS512`) đều bị từ chối lập tức.
2. **Khóa bí mật:** Go Backend sử dụng `Secret` cấu hình từ biến môi trường máy chủ (độ dài tối thiểu 32 ký tự). Client tuyệt đối không được biết khóa này.
3. **Không dùng JWKS:** Go Backend **không** gọi endpoint JWKS hay tải public key từ mạng. Mọi xác nhận là tính toán cục bộ bằng HMAC-SHA256.
4. **Đối chiếu Claims bắt buộc trong Payload:**

| Claim | Yêu cầu của Go Backend Verifier | Hành vi xử lý |
|---|---|---|
| `sub` | Bắt buộc (UUID hợp lệ, khác `uuid.Nil`) | Định danh người dùng (`auth.users.id`). Verifier không giới hạn UUID version; quan hệ với `profiles.id` được kiểm tra ở tầng nghiệp vụ. |
| `role` | Bằng chính xác chuỗi `"authenticated"` | Từ chối nếu là `"anon"`, `"service_role"` hoặc giá trị khác. |
| `aud` | Chứa hoặc bằng chuỗi `"authenticated"` | Đảm bảo token cấp cho người dùng đã xác thực. |
| `iss` | Khớp chính xác URL `SUPABASE_JWT_ISSUER` | Khớp cấu hình máy chủ (ví dụ `http://auth.test.local/auth/v1` trên môi trường kiểm thử). |
| `exp` | Bắt buộc (Unix timestamp) | Kiểm tra thời hạn sống. Cho phép độ lệch đồng hồ (`ClockSkew`) tối đa theo cấu hình máy chủ (từ 0s đến 5 phút, mặc định thử nghiệm 30s). |
| `iat` | Bắt buộc (Unix timestamp) | Thời điểm phát hành token. Không được nằm trong tương lai (có tính clock skew). |

Nếu bất kỳ tiêu chuẩn nào ở trên không thỏa mãn, Go Backend từ chối request với mã `401 Unauthorized`.

---

## 4. Quản lý phiên và Cơ chế Làm mới Token (Session & Refresh)

### 4.1. Bản chất lưu trữ phiên trên trình duyệt: Rủi ro XSS của `localStorage`

Một điểm kỹ thuật quan trọng cần làm rõ: **`window.localStorage` trên Web không phải là "kho lưu trữ an toàn" (secure storage) theo nghĩa bảo mật phần cứng.**
- **Không có KeyStore/Keychain:** Trình duyệt web chạy trên sandbox không có quyền truy cập vào Android KeyStore hay iOS Keychain.
- **Rủi ro XSS (Cross-Site Scripting):** Mọi dữ liệu lưu trong `localStorage` đều có thể bị đọc bởi bất kỳ đoạn mã JavaScript nào thực thi trong cùng Origin (`protocol://host:port`). Nếu ứng dụng bị tấn công XSS thông qua thư viện bên thứ ba không kiểm duyệt hoặc nhúng script độc hại, kẻ tấn công có thể đánh cắp `access_token` và `refresh_token`.
- **Biện pháp giảm thiểu rủi ro:**
  - Không lưu trữ token trong các biến toàn cục JavaScript (`window.token = ...`).
  - Thiết lập Content Security Policy (CSP) chặt chẽ trên Nginx: cấm thực thi `unsafe-inline` scripts, chỉ tải tài nguyên từ các nguồn tin cậy.
  - Phục vụ ứng dụng qua HTTPS, kích hoạt cờ bảo mật `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`.
  - Giữ thời gian sống của `access_token` ở mức ngắn hợp lý (ví dụ: 1 giờ / 3600 giây).

### 4.2. Cơ chế làm mới Token (Refresh Token Grant)

Khi `access_token` sắp hết hạn hoặc vừa hết hạn, client gửi yêu cầu làm mới tới GoTrue:

```http
POST /auth/v1/token?grant_type=refresh_token HTTP/1.1
Host: auth.example.com
Content-Type: application/json
apikey: <SUPABASE_ANON_KEY>

{
  "refresh_token": "8f9a2b..."
}
```

GoTrue kiểm tra refresh token trong cơ sở dữ liệu. Nếu hợp lệ, GoTrue cấp một cặp `access_token` và `refresh_token` mới.

### 4.3. Nguyên tắc xử lý Single-Flight và Chống lặp vô hạn khi Refresh

Trong quá trình tương tác với Go Backend API, nếu xảy ra lỗi `401 Unauthorized`, client cần tuân thủ các nguyên tắc xử lý nghiêm ngặt:

1. **Làm mới đơn luồng (Single-Flight Refresh):**
   - Khi người dùng mở dashboard, giao diện có thể gửi đồng thời 4–5 request API (lấy danh sách gateway, cảm biến, cấu hình, trạng thái kết nối).
   - Nếu token vừa hết hạn, tất cả các request này đều nhận về `401`.
   - **Cấm:** Không được gửi đồng thời 4–5 request `grant_type=refresh_token` lên GoTrue. Điều này gây ra lỗi tranh chấp phiên (race condition) và GoTrue sẽ hủy token do nghi ngờ bị lạm dụng (`invalid_grant`).
   - **Quy chuẩn:** Sử dụng cơ chế khóa (Mutex) hoặc Completer đơn lẻ: request đầu tiên kích hoạt làm mới session, các request tiếp theo tạm dừng chờ kết quả của lần làm mới đó rồi mới dùng token mới để gửi lại.
2. **Giới hạn số lần thử lại (Bounded Retry — At most once):**
   - Mỗi request nghiệp vụ chỉ được thử lại tối đa **1 lần duy nhất** sau khi đã refresh token thành công.
   - Tuyệt đối không tạo vòng lặp vô hạn (infinite retry loop) nếu token mới vẫn bị từ chối.
3. **Không tự động gửi lại các mutation không an toàn:**
   - Đối với các thao tác thay đổi dữ liệu cấu hình hoặc tác vụ quản trị (`PUT`, `POST`, `PATCH`), không tự ý gửi lại nếu không đảm bảo tính lũy kế hoặc thiếu khóa xác thực chống trùng lặp (`Idempotency-Key`).
4. **Không đăng xuất hàng loạt trên lỗi 403 hoặc 503:**
   - Mã `403 Forbidden` thể hiện người dùng đã đăng nhập hợp lệ nhưng không đủ quyền trên tài nguyên (ví dụ: không có quyền `owner` hoặc không phải `platform_admin`). **Không được đăng xuất người dùng khi gặp lỗi 403!**
   - Mã `503 Service Unavailable` thể hiện lỗi tạm thời ở cơ sở dữ liệu hoặc hệ thống phụ trợ. **Không được đăng xuất người dùng khi gặp lỗi 503!**
   - Chỉ kích hoạt đăng xuất khi quá trình làm mới token trả về lỗi `invalid_grant` hoặc refresh token đã bị thu hồi/hết hạn hoàn toàn.

---

## 5. Cơ chế đính kèm JWT khi gọi Go Backend API

### 5.1. Định dạng Header chuẩn

Mọi yêu cầu gửi tới Go Backend (`/v1/*`) bắt buộc phải đính kèm `access_token` trong HTTP Header theo chuẩn RFC 6750:

```http
GET /v1/gateways HTTP/1.1
Host: api.example.com
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Accept: application/json
```

**Quy tắc bắt buộc:**
- Scheme xác thực phải là `Bearer` (có khoảng trắng phân cách với chuỗi token).
- Go Backend **không chấp nhận** truyền token qua Query String (`?token=...`) hay JSON Body vì rủi ro lưu vết trong log proxy/trình duyệt.

### 5.2. Cấu trúc phản hồi lỗi an toàn từ Go Backend (Safe Error Envelope)

Khi xác thực hoặc phân quyền thất bại, Go Backend phản hồi theo đúng định dạng `httpapi.WriteError` (`src/internal/httpapi/error.go`):

#### A. Lỗi 401 Unauthorized (Thiếu hoặc sai Access Token)
```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer
X-Request-ID: 7b35f299-e67c-48c5-9276-805c8a4128f0

{
  "error": {
    "code": "unauthorized",
    "message": "authentication required",
    "request_id": "7b35f299-e67c-48c5-9276-805c8a4128f0"
  }
}
```
*Lưu ý bảo mật:* Go Backend luôn trả về `code: "unauthorized"` và `message: "authentication required"` kèm header `WWW-Authenticate: Bearer`. Hệ thống cố ý không giải thích chi tiết lý do (như "chữ ký sai", "token hết hạn", hay "không tìm thấy secret") nhằm ngăn chặn kẻ tấn công thăm dò cấu trúc token.

#### B. Phân biệt 403 Forbidden vs 404 Not Found vs 503 Service Unavailable

Go Backend áp dụng các mã lỗi khác nhau tùy theo bản chất bảo mật của tài nguyên:

| Mã HTTP | Thuộc tính `code` | Thuộc tính `message` | Ngữ cảnh trả về | Hành vi Client |
|---|---|---|---|---|
| **401 Unauthorized** | `"unauthorized"` | `"authentication required"` | Header Authorization thiếu, token sai định dạng, sai chữ ký HS256 hoặc đã hết hạn. | Thực hiện single-flight refresh token. Nếu thành công thì thử lại request 1 lần; nếu thất bại thì đăng xuất. |
| **403 Forbidden** | `"forbidden"` | `"insufficient permissions"` | Token hợp lệ nhưng tài khoản không có quyền Platform Admin khi gọi `/v1/admin/*` (`src/internal/auth/admin_middleware.go`). | Giữ nguyên phiên đăng nhập. Hiển thị thông báo: *"Bạn không có quyền thực hiện thao tác này."* |
| **404 Not Found** | `"not_found"` | `"resource not found"` | Truy cập trạm/cảm biến mà người dùng **không phải là thành viên** trong `user_gateways` (ví dụ: `/v1/gateways/:gateway_id/sensors`). | Giữ nguyên phiên. Go Backend trả về 404 (thay vì 403) đối với Gateway của người khác nhằm ngăn chặn việc dò quét danh sách Gateway trên hệ thống. |
| **503 Service Unavailable** | `"service_unavailable"` | `"service unavailable"` | Cơ sở dữ liệu quá tải, timeout tra cứu phân quyền hoặc hệ thống kiểm tra readiness thất bại. | Giữ nguyên phiên. Hiển thị thông báo hệ thống bận và cho phép thử lại sau ít phút. |

---

## 6. Luồng Đăng xuất và Thực tế Thu hồi Token (Sign Out & Revocation Reality)

### 6.1. Trình tự Đăng xuất trên Client

Khi người dùng nhấn "Đăng xuất" trên Web:
1. **Ngắt kết nối thời gian thực:** Đóng toàn bộ kết nối WebSocket đang mở tới `/v1/ws` để tránh giữ kết nối thừa.
2. **Gửi yêu cầu hủy phiên tới GoTrue:** Gọi `POST /auth/v1/logout` qua Nginx. GoTrue sẽ xóa session và vô hiệu hóa `refresh_token` trong cơ sở dữ liệu xác thực.
3. **Dọn dẹp Web Storage:** Xóa sạch `access_token` và `refresh_token` trong `window.localStorage`.
4. **Xóa State ứng dụng:** Reset toàn bộ bộ nhớ tạm (In-memory State/Bloc/Cubit) về trạng thái ban đầu.
5. **Điều hướng:** Đưa người dùng về màn hình `/login` thông qua GoRouter.

### 6.2. Thực tế kỹ thuật về việc Thu hồi Token (Token Invalidation Reality)

Một điểm then chốt cần ghi nhận chính xác theo thiết kế kiến trúc:
> **Thao tác đăng xuất (gọi `/auth/v1/logout` hoặc xóa `localStorage`) KHÔNG làm cho một `access_token` đã phát hành lập tức bị từ chối trên Go Backend trước khi token đó chạm mốc `exp`.**

**Nguyên nhân:**
- Go Backend thực hiện thẩm định chữ ký số tại chỗ (Local HS256 Verification) và hoàn toàn **không** truy vấn cơ sở dữ liệu của GoTrue trên từng HTTP request.
- Miễn là `access_token` còn thời hạn (`exp`), đúng chữ ký với `SUPABASE_JWT_SECRET`, đúng `iss`, `aud`, `role`, thì bộ phân tích cú pháp của Go Backend vẫn coi token đó là hợp lệ về mặt toán học.
- **Hệ quả kiến trúc:**
  - Nếu token bị rò rỉ qua kênh không an toàn (XSS, nhật ký mạng), việc người dùng bấm đăng xuất trên trình duyệt không thể lập tức vô hiệu hóa token đó ở phía Go Backend cho tới khi hết hạn.
  - Do đó, việc cấu hình thời gian sống của token ngắn (ví dụ 1 giờ), bảo vệ môi trường Web bằng CSP, không in token ra màn hình console và xóa sạch bộ nhớ client khi đăng xuất là các nguyên tắc bảo mật sống còn.

---

## 7. Xử lý các tình huống lỗi và Phản hồi giao diện (UI/UX Mapping)

| Tình huống / Mã phản hồi | Nguồn gốc lỗi | Thông điệp hiển thị trên UI | Hành vi hệ thống |
|---|---|---|---|
| `400 Bad Request` (`invalid_grant`) | GoTrue (Sai email hoặc mật khẩu) | *"Email hoặc mật khẩu không chính xác. Vui lòng kiểm tra lại."* | Dừng quá trình đăng nhập, cho phép người dùng nhập lại. |
| `422 Unprocessable Entity` (`signup_disabled`) | GoTrue (Cố tình gọi signup) | *"Hệ thống không mở tính năng tự đăng ký. Vui lòng liên hệ Quản trị viên hệ thống để được cấp tài khoản."* | Hiển thị thông báo hướng dẫn. |
| `401 Unauthorized` (`unauthorized`) | Go Backend (Token hết hạn/sai) | *"Đang làm mới phiên làm việc..."* | Kích hoạt single-flight refresh token. Nếu refresh thất bại: chuyển hướng về `/login` với thông báo *"Phiên đăng nhập đã hết hạn."* |
| `403 Forbidden` (`forbidden`) | Go Backend (Không có quyền Admin) | *"Bạn không có quyền thực hiện thao tác quản trị này."* | Không đăng xuất, giữ nguyên màn hình hiện tại. |
| `404 Not Found` (`not_found`) | Go Backend (Trạm không tồn tại hoặc không thuộc quyền) | *"Không tìm thấy tài nguyên trạm hoặc bạn không có quyền truy cập."* | Chuyển hướng về danh sách Gateway của người dùng. |
| `503 Service Unavailable` (`service_unavailable`) | Go Backend (Lỗi DB / Timeout) | *"Dịch vụ máy chủ tạm thời gián đoạn. Vui lòng thử lại sau ít phút."* | Cho phép người dùng bấm nút "Thử lại" (Retry). |
| Network / Socket Exception | Mất mạng hoặc Nginx không phản hồi | *"Không thể kết nối đến máy chủ. Vui lòng kiểm tra kết nối mạng."* | Hiển thị trạng thái Offline trên Dashboard. |

---

## 8. Mã nguồn mẫu Client minh họa (Illustrative Reference Scaffolding)

*Lưu ý kiến trúc:* Các đoạn mã Dart dưới đây là khung mã nguồn **mang tính chất minh họa hợp đồng giao tiếp (Illustrative Contract)**, không đại diện cho toàn bộ mã nguồn triển khai thực tế của ứng dụng client. Dự án không tự suy đoán nâng cấp các gói thư viện bên ngoài mà bám sát cơ chế mạng chuẩn.

### 8.1. API Client xử lý Single-Flight Refresh và Token Injection (`api_client.dart`)

```dart
// lib/core/network/api_client.dart
import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:supabase_flutter/supabase_flutter.dart';

class ApiClient {
  final String apiBaseUrl; // Ví dụ: http://localhost/v1
  final http.Client _httpClient = http.Client();

  // Khóa chống thundering herd khi nhiều request 401 cùng lúc
  Completer<String?>? _refreshCompleter;

  ApiClient({required this.apiBaseUrl});

  Future<String?> _getValidToken() async {
    return Supabase.instance.client.auth.currentSession?.accessToken;
  }

  /// Thực hiện Single-Flight Refresh để tránh gửi nhiều request refresh đồng thời
  Future<String?> _singleFlightRefreshToken() async {
    if (_refreshCompleter != null) {
      // Đang có một tiến trình refresh diễn ra, chờ đợi tiến trình đó
      return _refreshCompleter!.future;
    }

    _refreshCompleter = Completer<String?>();
    try {
      final res = await Supabase.instance.client.auth.refreshSession();
      final newToken = res.session?.accessToken;
      _refreshCompleter!.complete(newToken);
      return newToken;
    } catch (e) {
      _refreshCompleter!.complete(null);
      // Khi refresh thất bại hoàn toàn, kích hoạt đăng xuất dọn dẹp phiên
      await Supabase.instance.client.auth.signOut();
      return null;
    } finally {
      _refreshCompleter = null;
    }
  }

  Future<Map<String, String>> _buildHeaders(String? token) async {
    return {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      if (token != null && token.isNotEmpty) 'Authorization': 'Bearer $token',
    };
  }

  /// Gửi GET request có kèm cơ chế retry tối đa 1 lần nếu gặp lỗi 401
  Future<http.Response> get(String endpoint) async {
    final uri = Uri.parse('$apiBaseUrl$endpoint');
    String? token = await _getValidToken();
    var headers = await _buildHeaders(token);

    var response = await _httpClient.get(uri, headers: headers);

    // Xử lý khi Go Backend trả về 401 Unauthorized
    if (response.statusCode == 401) {
      final refreshedToken = await _singleFlightRefreshToken();
      if (refreshedToken != null && refreshedToken.isNotEmpty) {
        // Thử lại request ban đầu đúng 1 lần duy nhất với token mới
        headers = await _buildHeaders(refreshedToken);
        response = await _httpClient.get(uri, headers: headers);
      }
    }

    return response;
  }
}
```

### 8.2. Màn hình Đăng nhập minh họa (`login_screen.dart`)

Màn hình đăng nhập minh họa tuân thủ nghiêm ngặt quy định: không có nút Đăng ký tài khoản, xử lý phím Enter trên Web và hiển thị thông báo chính sách tài khoản tập trung:

```dart
// lib/features/auth/presentation/login_screen.dart
import 'package:flutter/material.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class LoginScreen extends StatefulWidget {
  const LoginScreen({super.key});

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen> {
  final _formKey = GlobalKey<FormState>();
  final _emailController = TextEditingController();
  final _passwordController = TextEditingController();

  bool _isLoading = false;
  bool _obscurePassword = true;
  String? _errorMessage;

  @override
  void dispose() {
    _emailController.dispose();
    _passwordController.dispose();
    super.dispose();
  }

  Future<void> _submitLogin() async {
    if (!_formKey.currentState!.validate()) return;

    setState(() {
      _isLoading = true;
      _errorMessage = null;
    });

    try {
      final response = await Supabase.instance.client.auth.signInWithPassword(
        email: _emailController.text.trim(),
        password: _passwordController.text,
      );

      if (response.session != null && mounted) {
        // Điều hướng sang Dashboard được quản lý bởi Router stream
      }
    } on AuthException catch (e) {
      setState(() {
        if (e.message.contains("Invalid login credentials") || e.statusCode == "400") {
          _errorMessage = "Email hoặc mật khẩu không chính xác.";
        } else {
          _errorMessage = e.message;
        }
      });
    } catch (_) {
      setState(() {
        _errorMessage = "Không thể kết nối đến máy chủ xác thực.";
      });
    } finally {
      if (mounted) setState(() => _isLoading = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Title(
      title: "Đăng nhập | IoT Gateway–Server",
      color: Colors.blueAccent,
      child: Scaffold(
        backgroundColor: Colors.grey.shade100,
        body: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(24.0),
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 420),
              child: Card(
                elevation: 4,
                shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
                child: Padding(
                  padding: const EdgeInsets.all(32.0),
                  child: Form(
                    key: _formKey,
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      crossAxisAlignment: CrossAxisAlignment.stretch,
                      children: [
                        const Icon(Icons.sensors_outlined, size: 56, color: Colors.blueAccent),
                        const SizedBox(height: 12),
                        const Text(
                          "IoT Gateway–Server Platform",
                          textAlign: TextAlign.center,
                          style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                        ),
                        const SizedBox(height: 4),
                        const Text(
                          "Hệ thống Giám sát & Vận hành Trạm quan trắc",
                          textAlign: TextAlign.center,
                          style: TextStyle(color: Colors.grey, fontSize: 13),
                        ),
                        const SizedBox(height: 20),

                        if (_errorMessage != null) ...[
                          Container(
                            padding: const EdgeInsets.all(10),
                            decoration: BoxDecoration(
                              color: Colors.red.shade50,
                              border: Border.all(color: Colors.red.shade200),
                              borderRadius: BorderRadius.circular(8),
                            ),
                            child: Text(
                              _errorMessage!,
                              style: const TextStyle(color: Colors.red, fontSize: 13),
                            ),
                          ),
                          const SizedBox(height: 16),
                        ],

                        TextFormField(
                          controller: _emailController,
                          keyboardType: TextInputType.emailAddress,
                          enabled: !_isLoading,
                          decoration: const InputDecoration(
                            labelText: "Email tài khoản",
                            prefixIcon: Icon(Icons.email_outlined),
                            border: OutlineInputBorder(),
                          ),
                          validator: (v) => (v == null || !v.contains("@")) ? "Email không hợp lệ" : null,
                        ),
                        const SizedBox(height: 16),

                        TextFormField(
                          controller: _passwordController,
                          obscureText: _obscurePassword,
                          enabled: !_isLoading,
                          onFieldSubmitted: (_) => _submitLogin(),
                          decoration: InputDecoration(
                            labelText: "Mật khẩu",
                            prefixIcon: const Icon(Icons.lock_outline),
                            border: const OutlineInputBorder(),
                            suffixIcon: IconButton(
                              icon: Icon(_obscurePassword ? Icons.visibility_outlined : Icons.visibility_off_outlined),
                              onPressed: () => setState(() => _obscurePassword = !_obscurePassword),
                            ),
                          ),
                          validator: (v) => (v == null || v.isEmpty) ? "Vui lòng nhập mật khẩu" : null,
                        ),
                        const SizedBox(height: 24),

                        ElevatedButton(
                          onPressed: _isLoading ? null : _submitLogin,
                          style: ElevatedButton.styleFrom(
                            padding: const EdgeInsets.symmetric(vertical: 14),
                            shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
                          ),
                          child: _isLoading
                              ? const SizedBox(height: 18, width: 18, child: CircularProgressIndicator(strokeWidth: 2))
                              : const Text("ĐĂNG NHẬP", style: TextStyle(fontWeight: FontWeight.bold)),
                        ),
                        const SizedBox(height: 16),

                        const Text(
                          "Hệ thống vận hành trạm nội bộ. Tài khoản người dùng được cấp phát tập trung bởi Quản trị viên hệ thống (Platform Administrator).",
                          textAlign: TextAlign.center,
                          style: TextStyle(fontSize: 12, color: Colors.grey, fontStyle: FontStyle.italic),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
```

---

## 9. Danh mục kiểm tra bảo mật và chất lượng (Security & Quality Checklist)

Trước khi nghiệm thu tích hợp xác thực client:

- [x] **Tách biệt Base URL:** Client gọi đúng Supabase Auth Gateway (`/auth/v1/*`) để đăng nhập/refresh và Go Backend (`/v1/*`) để truy xuất dữ liệu nghiệp vụ.
- [x] **Phân tách khóa & token:** Chỉ dùng `SUPABASE_ANON_KEY` trên client; không đưa `SUPABASE_SERVICE_ROLE_KEY` hay `SUPABASE_JWT_SECRET` vào mã nguồn hoặc môi trường client.
- [x] **Cấm đăng ký tự do:** Không có màn hình hoặc nút bấm "Đăng ký" trên client. Đã kiểm chứng endpoint `POST /auth/v1/signup` bị GoTrue từ chối với `422 signup_disabled` trong test fixture.
- [x] **Ranh giới Auth Admin:** Nhận thức rõ ràng việc người dùng có cờ `platform_admin` trong PostgreSQL không thể gọi `/auth/v1/admin/users` bằng human JWT (bị từ chối `403 Forbidden`). Không có API User CRUD trên Go Backend.
- [x] **Xác thực JWT tại Go Backend:** Go Backend chỉ nhận chữ ký `HS256` với secret chia sẻ, kiểm tra `iss`, `aud`, `role: "authenticated"`, `sub` UUID hợp lệ khác nil và thời hạn `exp` (có tính clock skew); không yêu cầu riêng UUID v4.
- [x] **Chuẩn hóa Error Envelope:** Xử lý đúng cấu trúc lỗi của Go Backend (`{"error": {"code": "unauthorized", "message": "authentication required", "request_id": "..."}}`) và header `WWW-Authenticate: Bearer`.
- [x] **Kiểm soát làm mới Token:** Áp dụng single-flight khi refresh token trên client, giới hạn thử lại tối đa 1 lần, không tạo vòng lặp vô hạn.
- [x] **Phân biệt mã lỗi khi xử lý phiên:** Không tự ý đăng xuất người dùng khi gặp mã lỗi `403 Forbidden` (thiếu quyền) hoặc `503 Service Unavailable` (lỗi tạm thời hạ tầng).
- [x] **Nhận thức thu hồi phiên:** Hiểu rõ việc đăng xuất tại client và GoTrue không lập tức hủy bỏ giá trị xác thực toán học của một access token chưa hết hạn đối với bộ kiểm tra cục bộ của Go Backend; bảo vệ chặt chẽ storage và đặt TTL ngắn.
- [x] **Liên kết tài liệu:** Đã tham chiếu đầy đủ tới `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (hợp đồng quản trị trạm/cảm biến) và `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (tích hợp API và quản lý state).
