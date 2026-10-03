# Tài liệu Client — 04: Quy chuẩn Hợp đồng REST API (REST API Client Contract) trên Flutter Web

**Cập nhật:** 2026-10-03
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 5, 6, 10, 18), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/02_AUTHENTICATION_AND_SESSION.md`, `docs/client/03_USER_GATEWAY_AUTHORIZATION.md`, `docs/backend/stage-2-task-2.3-authorization.md`, `src/internal/httpserver/gateway_handlers.go`, `src/internal/httpserver/sensor_handlers.go`, `src/internal/httpapi/error.go`, `src/internal/httpapi/request_id.go`, `src/internal/httpserver/router.go`.

---

## 1. Mục tiêu và Phạm vi tài liệu

Tài liệu này định nghĩa **Hợp đồng giao tiếp REST API (REST API Client Contract)** chính thức giữa ứng dụng **Flutter Web** (bảng điều khiển Dashboard giám sát và vận hành trung tâm) và **Go Backend** trong hệ thống IoT Gateway–Server.

- **Đối tượng áp dụng:** Lập trình viên phát triển ứng dụng Flutter Web, kiểm thử viên API, và kỹ sư tích hợp hệ thống.
- **Phạm vi giai đoạn hiện tại (Giai đoạn 2 — Stage 2 Boundary):**
  - Tập trung hoàn thiện hai API đọc danh mục phân quyền cốt lõi:
    1. Lấy danh sách Gateway mà người dùng có quyền: `GET /v1/gateways`
    2. Lấy danh sách Sensor thuộc một Gateway cụ thể: `GET /v1/gateways/{gateway_id}/sensors`
  - Định nghĩa chuẩn định dạng Request, Response, cơ chế truy vết lỗi (`X-Request-ID`), định dạng phong bì lỗi an toàn (`httpapi.APIErrorEnvelope`), và quy tắc xử lý ngoại lệ phía Client trên nền tảng trình duyệt Web.
  - Phân định rõ cơ chế mạng trên Web: Môi trường Local Dev với CORS preflight (`OPTIONS`), cấu hình Nginx CORS headers, và kiến trúc Production cùng domain (Same-Origin).
  - Hướng dẫn cấu hình HTTP Client trên Flutter Web (`dio` / `BrowserHttpClient` / `fetch`), lưu ý về Forbidden Header Names và cơ chế phân biệt lỗi mạng / lỗi CORS đặc trưng của trình duyệt.
  - Liệt kê các endpoint đã được đặt chỗ (stub) trả về `501 Not Implemented` để Client không gọi nhầm trong giai đoạn này.

---

## 2. Quy chuẩn gửi HTTP Request và Cơ chế mạng trên Web

Mọi tương tác từ Flutter Web Client tới Go Backend phải tuân thủ các quy tắc định tuyến, cơ chế bảo mật trình duyệt (CORS) và tiêu chuẩn header sau:

### 2.1. Base URL và Cơ chế mạng trên Flutter Web

Khác với ứng dụng native (Android/iOS) gửi trực tiếp các gói tin socket TCP độc lập, ứng dụng Flutter Web chạy trong môi trường bảo mật của trình duyệt (Browser Sandbox), chịu sự kiểm soát nghiêm ngặt của **Chính sách cùng nguồn gốc (Same-Origin Policy - SOP)** và cơ chế **Chia sẻ tài nguyên liên nguồn gốc (Cross-Origin Resource Sharing - CORS)**.

Toàn bộ HTTP request của ứng dụng Client **không bao giờ kết nối trực tiếp vào cổng nội bộ của Go Backend (port 8080)** mà bắt buộc phải đi qua **Nginx Reverse Proxy**:

#### A. Môi trường phát triển cục bộ (Local Development)
- Khi chạy lệnh phát triển `flutter run -d chrome --web-port=3000`, ứng dụng Web chạy trên máy chủ phát triển cục bộ tại:
  ```text
  Origin của Web Client: http://localhost:3000
  ```
- Nginx Reverse Proxy (điều phối Go Backend và Supabase) lắng nghe trên cổng 80 (hoặc cổng HTTP do Docker Compose ánh xạ):
  ```text
  Base URL của API: http://localhost:80 (hoặc http://localhost)
  ```
- **Lưu ý đặc biệt về địa chỉ IP:** Tuyệt đối **không sử dụng `10.0.2.2`** trên Web. Địa chỉ `10.0.2.2` là alias định tuyến dành riêng cho máy ảo Android Emulator để trỏ về máy tính host. Trên Web, trình duyệt chạy trực tiếp trên máy host nên luôn sử dụng `localhost` hoặc `127.0.0.1`.
- **Cơ chế CORS và Preflight Request (`OPTIONS`):**
  - Do Web Client (`http://localhost:3000`) và API Server (`http://localhost:80`) khác cổng (port), trình duyệt coi đây là hai **Cross-Origin** khác nhau.
  - Khi Client gửi các request có chứa header tùy chỉnh như `Authorization` (Bearer JWT) hoặc `X-Request-ID`, trình duyệt sẽ **tự động gửi một request thăm dò trước (Preflight Request)** sử dụng phương thức `OPTIONS` để hỏi máy chủ xem có cho phép các header và method này hay không.
  - **Yêu cầu cấu hình CORS tại Nginx Reverse Proxy hoặc Go Backend:** Máy chủ phải đón nhận các preflight request `OPTIONS`, phản hồi mã `204 No Content` hoặc `200 OK` và thiết lập đầy đủ các header CORS sau:
    ```nginx
    # Cho phép nguồn gửi request (khi dev có thể dùng * hoặc origin cụ thể)
    Access-Control-Allow-Origin: *;
    # Hoặc giới hạn chặt chẽ: Access-Control-Allow-Origin: http://localhost:3000;

    # Cho phép các phương thức HTTP
    Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS;

    # Cho phép các header tùy chỉnh gửi lên từ Web client
    Access-Control-Allow-Headers: Authorization, Content-Type, Accept, X-Request-ID;

    # Cho phép JavaScript/Dart trên trình duyệt đọc được các header trong response
    Access-Control-Expose-Headers: X-Request-ID, Cache-Control;
    ```
  - > **TẦM QUAN TRỌNG CỦA `Access-Control-Expose-Headers`:** Trình duyệt mặc định sẽ ẩn các response header tùy chỉnh đối với mã JavaScript/Dart nhằm đảm bảo an toàn. Nếu thiếu `Access-Control-Expose-Headers: X-Request-ID, Cache-Control`, mã Dart gọi `response.headers.value('x-request-id')` trên Web sẽ luôn nhận về giá trị rỗng hoặc `null` mặc dù trong tab Network của DevTools vẫn nhìn thấy header này!

#### B. Môi trường triển khai thực tế (Production / Staging)
- Khi đóng gói phát hành (`flutter build web --release`), toàn bộ gói mã nguồn tĩnh của Flutter Web (HTML, JS, CanvasKit/WASM) được đặt trực tiếp vào thư mục phục vụ của Nginx và chạy chung domain với API:
  ```text
  Trang Web Dashboard:  https://iot.example.com/
  Đường dẫn REST API:   https://iot.example.com/v1/
  ```
- **Ưu thế kiến trúc Same-Origin:**
  - Vì Web và API cùng chung Origin (`https://iot.example.com`), Client có thể cấu hình Base URL dạng tương đối (**Relative URL**) là `/v1` hoặc dùng domain tuyệt đối `https://iot.example.com`.
  - Toàn bộ cơ chế CORS và các preflight request `OPTIONS` bị **triệt tiêu hoàn toàn**, giúp giảm 50% số lượng request mạng, tăng tốc độ phản hồi và loại bỏ hoàn toàn các lỗi chặn mạng do cấu hình header liên nguồn gốc.

### 2.2. Headers bắt buộc phía Client
Mỗi request gửi tới các endpoint nghiệp vụ `/v1/*` bắt buộc phải chứa các header sau:

| Header | Giá trị mẫu | Bắt buộc | Mục đích |
|---|---|:---:|---|
| `Authorization` | `Bearer <access_token>` | **Có** | Chứa JWT do Supabase Auth cấp khi đăng nhập. Backend xác minh chữ ký tại chỗ (Local HS256). |
| `Accept` | `application/json` | **Có** | Thông báo Client chỉ tiếp nhận dữ liệu phản hồi định dạng JSON. |
| `X-Request-ID` | `c4b1e6e0-2495-4e3a-97a6-6f8d38e21a41` | Khuyến nghị | Mã UUIDv4 định danh duy nhất cho từng lượt request để phục vụ truy vết lỗi phân tán (Distributed Tracing). |

### 2.3. Quy tắc xử lý Header `X-Request-ID` (Traceability)
Căn cứ theo hiện thực tại `src/internal/httpapi/request_id.go`:
1. **Kiểm tra tính hợp lệ:** Go Backend chấp nhận header `X-Request-ID` từ Client nếu thỏa mãn:
   - Request chỉ chứa duy nhất 1 header `X-Request-ID`.
   - Độ dài từ 1 đến 128 ký tự.
   - Bắt đầu bằng chữ cái ASCII (`A-Z`, `a-z`) hoặc chữ số (`0-9`).
   - Các ký tự tiếp theo chỉ thuộc tập hợp: chữ cái, chữ số, dấu chấm (`.`), gạch dưới (`_`), hai chấm (`:`), hoặc gạch ngang (`-`).
2. **Cơ chế Fallback:** Nếu Client không gửi header này, hoặc giá trị không hợp lệ (chứa ký tự lạ, quá dài, hoặc gửi nhiều lần), Go Backend sẽ **tự động sinh một UUIDv4 ngẫu nhiên mới**.
3. **Phản hồi từ Backend:** Trong **MỌI** response (kể cả HTTP 200 thành công hay các response lỗi 4xx, 5xx), Go Backend luôn đính kèm header:
   ```http
   X-Request-ID: <validated_or_generated_uuid>
   ```
4. **Trách nhiệm của Client:** Client cần đọc header `X-Request-ID` từ response (hoặc lấy từ `error.request_id` trong payload lỗi) để ghi log cục bộ hoặc hiển thị trên giao diện thông báo khi người dùng cần hỗ trợ kỹ thuật.

### 2.4. Header chống lưu bộ nhớ đệm (`Cache-Control: no-store`)
- Toàn bộ dữ liệu phản hồi thành công từ hai API phân quyền `GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors` đều là dữ liệu định danh và phân quyền nhạy cảm.
- Backend luôn đính kèm header:
  ```http
  Cache-Control: no-store
  ```
- **Yêu cầu đối với Client:** Không được lưu cache vĩnh viễn danh sách Gateway hay Sensor vào bộ nhớ đệm HTTP tầng trình duyệt hoặc local storage. Quyền của người dùng trên trạm có thể bị Quản trị viên thay đổi hoặc thu hồi bất kỳ lúc nào trên cơ sở dữ liệu.

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

Hệ thống quy định danh mục mã lỗi chuẩn hóa như sau:

| HTTP Status | Error `code` | Error `message` chuẩn | Tình huống kích hoạt phía Server | Hướng xử lý phía Client |
|---|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` | - Cố tình gửi query param `user_id`.<br>- Path param `gateway_id` sai định dạng regex hoặc dùng tên dành riêng (`backend_service`). | Kiểm tra lại logic tạo request, sửa định dạng tham số. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | - Thiếu header `Authorization`.<br>- Header không có tiền tố `Bearer `.<br>- Token hết hạn, sai chữ ký, hoặc sai `role`/`aud`. | Xóa session, kích hoạt luồng Refresh Token hoặc điều hướng người dùng về màn hình Đăng nhập. |
| **404 Not Found** | `not_found` | `resource not found` | - `gateway_id` không tồn tại trong hệ thống.<br>- Người dùng **không có quyền** trên `gateway_id` yêu cầu.<br>- Gọi sai đường dẫn URL (NoRoute). | Hiển thị thông báo không tìm thấy tài nguyên. Không suy diễn tài nguyên có tồn tại hay không. |
| **405 Method Not Allowed** | `method_not_allowed` | `method not allowed` | - Gọi sai HTTP Method trên endpoint hợp lệ (ví dụ `POST /v1/gateways`). | Sửa lại phương thức HTTP tương ứng. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | - Lỗi máy chủ nội bộ không lường trước (lỗi đọc kết quả SQL, panic được khôi phục bởi Recovery middleware). | Hiển thị thông báo sự cố hệ thống và ghi nhận `request_id` để báo hỗ trợ kỹ thuật. |
| **501 Not Implemented** | `not_implemented` | `endpoint not implemented` | - Gọi vào các endpoint chưa được kích hoạt trong Giai đoạn 2 (`/telemetry/history`, `/ws`, `/digital-twins`). | Không gọi các endpoint này cho đến các giai đoạn phát triển tiếp theo. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | - Thao tác truy vấn vượt quá thời hạn cho phép (`AUTHORIZATION_TIMEOUT`, mặc định 2s).<br>- Cơ sở dữ liệu tạm thời gián đoạn. | Hiển thị thông báo máy chủ bận, có thể cho phép người dùng nhấn "Thử lại" (Retry). |

> **LƯU Ý VỀ TÍNH BẢO MẬT CỦA MÃ LỖI 404 (Anti-Enumeration):**
> Khi gọi `GET /v1/gateways/{gateway_id}/sensors`, nếu Gateway không tồn tại HOẶC người dùng không được phân quyền trong bảng `user_gateways`, Go Backend **luôn trả về cùng một mã lỗi `404 Not Found` với thông điệp `resource not found`**. Backend tuyệt đối không trả về `403 Forbidden` đối với trạm của người khác, nhằm ngăn chặn kẻ tấn công dò quét xem những mã `gateway_id` nào đang thực sự tồn tại trong hệ thống.

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

## 6. Danh sách API tạm thời trả về `501 Not Implemented`

Trong Giai đoạn 2 (Task 2.3), để bảo toàn định hình kiến trúc toàn diện của hệ thống đã thỏa thuận tại `AGENTS.md`, Go Backend (`src/internal/httpserver/router.go`) đã đăng ký sẵn khung định tuyến cho các endpoint sau nhưng tạm thời gắn handler trả về `501 Not Implemented`:

| Endpoint | Method | Trạng thái hiện tại | Kế hoạch triển khai |
|---|---|:---:|---|
| `/v1/telemetry/history` | `GET` | `501 Not Implemented` | Giai đoạn 3 (Truy vấn chuỗi thời gian TimescaleDB & Downsampling). |
| `/v1/ws` | `GET` | `501 Not Implemented` | Giai đoạn 3 (Kênh truyền thời gian thực WebSocket). |
| `/v1/digital-twins` | `GET` | `501 Not Implemented` | Giai đoạn 4 (Mô hình Digital Twin & NGSI-LD). |

#### Payload phản hồi khi gọi vào các endpoint này:
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

**Khuyến cáo cho Client:** Client không được phát triển các màn hình gọi vào 3 endpoint trên cho đến khi có thông báo hoàn thành từ đội ngũ Backend.

---

## 7. Mã nguồn Dart Client mẫu (Flutter Implementation)

Dưới đây là mã nguồn hoàn chỉnh định nghĩa DTO Models và Service Client sử dụng thư viện `dio`, sẵn sàng tích hợp trực tiếp vào dự án Flutter.

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
                baseUrl: baseUrl,
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
      final response = await _dio.get<Map<String, dynamic>>('/v1/gateways');
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
      final response = await _dio.get<Map<String, dynamic>>(
        '/v1/gateways/$gatewayId/sensors',
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
| **1** | Kiểm tra CORS Preflight trong môi trường Local Dev (`localhost:3000` -> `localhost:80`). | Trình duyệt gửi `OPTIONS /v1/gateways`, Nginx trả về `HTTP 204` hoặc `200` kèm đầy đủ `Access-Control-Allow-*`. Sau đó gửi tiếp `GET /v1/gateways` trả về `HTTP 200 OK`. | DevTools không báo đỏ lỗi CORS. Client nhận dữ liệu và hiển thị danh sách trạm kèm badge vai trò (`owner`, `operator`, `viewer`). |
| **2** | Đọc header `X-Request-ID` từ response trong mã Dart trên Web. | Response trả về header `X-Request-ID` và Nginx đính kèm `Access-Control-Expose-Headers: X-Request-ID`. | Mã Dart đọc được chuỗi UUID từ `response.headers.value('x-request-id')`, không bị trả về `null`. |
| **3** | Người dùng mới tinh vừa được tạo (chưa được gán vào `user_gateways`). | `HTTP 200 OK`, `{"items": []}`, `Cache-Control: no-store`. | Hiển thị trạng thái trống (Empty State): *"Bạn chưa được phân quyền truy cập trạm quan trắc nào"*. |
| **4** | Gọi API khi Access Token hết hạn (> 1 giờ) hoặc token rác. | `HTTP 401 Unauthorized`, `WWW-Authenticate: Bearer`, code `unauthorized`. | Gọi hàm làm mới token (`auth.refreshSession()`). Nếu thất bại, chuyển về màn hình đăng nhập. |
| **5** | Nhấn vào trạm `gateway_001` có gắn 3 cảm biến. | `HTTP 200 OK`, `{"gateway_id": "gateway_001", "items": [...]}`. | Hiển thị bảng/danh sách cảm biến kèm tên và đơn vị (`unit`). |
| **6** | Nhấn vào trạm `gateway_002` mà trạm này chưa có cảm biến nào. | `HTTP 200 OK`, `{"gateway_id": "gateway_002", "items": []}`. | Hiển thị trạng thái: *"Trạm này hiện chưa lắp đặt cảm biến"*. |
| **7** | Cố tình gọi `GET /v1/gateways/gw_khac/sensors` (trạm của người khác hoặc không tồn tại). | `HTTP 404 Not Found`, code `not_found`, message `resource not found`. | Bắt `NotFoundException`, hiển thị: *"Không tìm thấy thông tin trạm hoặc bạn không có quyền truy cập trạm này"*. |
| **8** | Cố tình truyền `?user_id=...` trên thanh URL. | `HTTP 400 Bad Request`, code `invalid_request`. | Client Interceptor chặn từ sớm không gửi lên mạng; nếu lọt qua, bắt `BadRequestException`. |
| **9** | Truyền `gateway_id` chứa ký tự đặc biệt như dấu cách hoặc tiếng Việt có dấu. | `HTTP 400 Bad Request`, code `invalid_request`. | Client kiểm tra regex trước khi gửi; hiển thị thông báo lỗi định dạng ngay trên giao diện. |
| **10** | Tắt Nginx hoặc mất mạng khi Web client đang chạy. | Trình duyệt ném lỗi `"XMLHttpRequest error"` hoặc `"Failed to fetch"`, `response == null`. | Bắt `NetworkException`, hiển thị cảnh báo: *"Không thể kết nối đến máy chủ hoặc request bị chặn bởi chính sách CORS của trình duyệt"*. |
| **11** | Nhấn F5 / Ctrl+R làm mới trang trên trình duyệt Web. | Phiên đăng nhập được phục hồi từ Web Storage, gửi request với token mới. | Dữ liệu danh sách trạm được nạp lại bình thường, không bị văng đăng nhập hay lỗi routing. |
