# Tài liệu Client — 08: Tích hợp API và Quản lý Trạng thái Giao diện (API Integration and State Handling)

**Cập nhật:** 2026-10-19
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- [`../../AGENTS.md`](../../AGENTS.md) (Mục 1, 2, 5, 6, 6.5, 10, 16, 18)
- [`../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md`](../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md) (Kế hoạch chi tiết quản lý User và Gateway Membership)
- [`../backend/admin-users-and-gateway-membership.md`](../backend/admin-users-and-gateway-membership.md) (Báo cáo tổng kết và bàn giao Task U0–U8)
- `docs/protocols/stage4-read-and-entity-api.md` (Hợp đồng Stage 4 Read & Entity API)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Phân quyền người dùng và trạm)
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API chuẩn)
- `docs/client/05_UPCOMING_FEATURES_ROADMAP.md` (Lộ trình tính năng sắp tới)
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/dynsec/limitations-and-backlog.md` (Hạn chế kỹ thuật và tồn đọng hệ thống)

---

## 1. Mục tiêu, Ranh giới và Tuyên bố Hiện thực (Scope & Implementation Notice)

### 1.1 Mục tiêu tài liệu
Tài liệu này cung cấp **danh mục quy chuẩn (checklist) thực tiễn, ưu tiên endpoint (endpoint-first)** dành cho việc tích hợp API, xử lý trạng thái giao diện (UI State Handling) và bắt lỗi mạng an toàn trên ứng dụng Client (Flutter Web) trong phạm vi Giai đoạn 4 (Stage 4: Lịch sử đo lường, Bản sao số Digital Twin Read & Entity Metadata API).

### 1.2 Tuyên bố hiện thực (No Premature Client Implementation Claims)
- **Tài liệu này đóng vai trò đặc tả thiết kế và danh mục kiểm thử tích hợp cho Client, KHÔNG khẳng định mã nguồn giao diện Flutter Client đã được hiện thực hoặc kiểm thử hoàn chỉnh.**
- Giao diện người dùng phải tuân thủ nghiêm ngặt các endpoint đã thực sự tồn tại trong Go Backend (gồm 7 endpoint Stage 4 và các API nghiệp vụ/quản trị hiện hữu) và GoTrue Auth; tuyệt đối không giả định hoặc tự sáng tác các endpoint chưa có.

### 1.3 Phân định bằng chứng kiểm thử (Evidence Status)
- **Bằng chứng kiểm chứng lịch sử (Historical Test Snapshot):** Bộ kiểm thử E2E cô lập đã từng hoàn tất thực nghiệm 27/27 kịch bản (E01–E27) đạt `PASS` trên hạ tầng kiểm thử cục bộ (`scripts/test-stage2-e2e.sh`, ảnh chụp lịch sử ngày 2026-10-05).
- **Kiểm chứng Tích hợp Backend Stage 4 (Stage 4 Verified Evidence):**
  - Kịch bản tích hợp thực tế `scripts/test-stage4-api.sh --scenario all` đạt `PASS` trên các container dịch vụ thực (PostgreSQL 16.15 / TimescaleDB 2.29.2 schema 20 dưới role `iot_backend_app`, Mosquitto 2.0.18 DynSec TLS).
  - Pipeline GitHub Actions CI từ xa đã được xác thực `PASS` toàn bộ 16/16 jobs tại commit sạch `aa7436d8838a6580c21b30930e2eed992999b11c` (Workflow run `39103564379`), bao gồm `stage4-api-integration`, `stage3-uplink-integration` và `native-compose-smoke`.
- **Ngoại lệ Tồn đọng Chuyển tiếp (Deferred Exceptions & Boundaries):**
  - WebSocket (`/v1/ws`) tiếp tục trả về `501 Not Implemented` (chờ Stage 7).
  - Điều khiển thiết bị lâu bền (`desired_state`, downlink commands, transactional outbox) chưa kích hoạt (chờ Stage 5).
  - API Reset credential có giám sát vẫn giữ rào chắn fail-closed `503 Service Unavailable` (`denyOnlyResetPrerequisite{}`).
*Lưu ý an toàn:* Tuyệt đối không tuyên bố hệ thống sẵn sàng vận hành sản xuất (production-ready) hoặc client Flutter đã hoàn tất khi các cổng nghiệm thu nghiệp vụ và phần cứng thực tế chưa diễn ra.

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
2. **Business API Base URL (`/v1`):** Tiếp nhận toàn bộ truy vấn dữ liệu nghiệp vụ, phân quyền và Bản sao số Digital Twin:
   - Danh sách và cảm biến cơ sở (`/v1/gateways`, `/v1/gateways/{gateway_id}/sensors`).
   - Năm endpoint Digital Twin V1.9.1 profile (`/v1/digital-twins`, `/v1/digital-twins/{entity_id}`, `/v1/digital-twins/{entity_id}/attrs`, `/v1/digital-twins/{entity_id}/temporal`, cùng query extension `/v1/digital-twins/{entity_id}?format=simplified`).
   Đi qua Nginx trực tiếp vào Go Backend.

### 2.2 Quy tắc cấm kỵ về bảo mật Token
- **CẤM truyền token qua URL:** Tuyệt đối không gửi Bearer token hay `user_id` qua URL Query Parameters (ví dụ: `?token=...` hoặc `?user_id=...`). Mọi định danh người dùng phải được Go Backend trích xuất duy nhất từ claim `sub` của JWT.
- **CẤM rò rỉ token qua argv và logs:** Tuyệt đối không truyền token qua đối số dòng lệnh (argv), không in token ra console log hoặc ghi log trình duyệt.
- **Rủi ro LocalStorage XSS trên Web:** Cần lưu ý việc lưu trữ JWT trong `localStorage` của trình duyệt có nguy cơ bị đánh cắp nếu ứng dụng dính lỗ hổng Cross-Site Scripting (XSS). Ưu tiên lưu trữ trong bộ nhớ tạm (in-memory state) kết hợp cơ chế quản lý phiên an toàn.

---

## 3. Trình tự Gọi API Thực tế và Cấu thành Trang Chi tiết (Actual Endpoint Sequence)

### 3.1 Trình tự tuần tự thực tế theo chuẩn ETSI GS CIM 009 V1.9.1 Profile
Client bắt buộc phải gọi API theo đúng trình tự phân cấp:

```text
[1. Đăng nhập / Refresh Token]
       POST /auth/v1/token?grant_type=password  (nhận JWT access_token)
          |
          +----------------------------------------------+
          |                                              |
          v                                              v
[2a. Danh sách Gateway/Sensor quan hệ]         [2b. Danh sách Digital Twin (V1.9.1 Profile)]
       GET /v1/gateways                                GET /v1/digital-twins?type=Gateway&limit=50
       GET /v1/gateways/{id}/sensors                   Query: type, gateway_id, limit, cursor
       Response: 200 OK -> { "items": [...] }          Response: 200 OK -> root array [...]
          |                                            Header: Link: <URI>; rel="next"
          |                                            (Client follow trực tiếp URI trong Link)
          +----------------------+-----------------------+
                                 |
                                 v
[3. Chi tiết thực thể & Đồ thị quan hệ (NGSI-LD JSON-LD)]
       GET /v1/digital-twins/{entity_id}
       Header: Accept: application/ld+json (hoặc application/json)
       Response: 200 OK -> Gateway kèm hasSensor (object: [URI...], objectType: Sensor,
                           phân trang qua header Link); hoặc Sensor kèm isHostedBy,
                           measurement/interval mới nhất và mã unitCode UN/CEFACT.
                                 |
          +----------------------+-----------------------+
          |                                              |
          v                                              v
[4a. Trạng thái hiện thời (State View Extension)] [4b. Lịch sử chuỗi thời gian (Temporal History)]
       GET /v1/digital-twins/{entity_id}?format=simplified  GET /v1/digital-twins/{entity_id}/temporal
       Response: 200 OK (application/json)             Query: attrs=measurement|sample_interval_us,
       reported_state, desired_state,                  timerel=between, timeAt, endTimeAt,
       reported_version, desired_version               optional aggrPeriodDuration, aggrMethods
       (truy vấn trực tiếp twin_states,                Response: 200 OK (application/ld+json)
        không quét toàn bộ hypertable,                  Raw: values [[val, time], ...]
        không context)                                  Bucket: avg, min, max, totalCount (exact int)
```

### 3.2 Cấu thành Trang Chi tiết Thực thể (Detail Page Composition & Real Twin Detail)
> **PHÂN BIỆT RÕ RÀNG VỀ ENDPOINT CHI TIẾT VÀ CHUYỂN ĐỔI V1.9.1:**
> 1. **Các route đơn lẻ kiểu cũ VẪN KHÔNG TỒN TẠI:** Go Backend **KHÔNG TỒN TẠI** `GET /v1/gateways/{gateway_id}` và **KHÔNG TỒN TẠI** `GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`. Client gọi các path này sẽ nhận `404 Not Found`.
> 2. **Bốn route Stage 4 cũ đã bị thu hồi:**
>    - `GET /v1/telemetry/history` $\rightarrow$ Nhận `404 Not Found` (hợp nhất vào `GET /v1/digital-twins/{entity_id}/temporal?attrs=measurement`).
>    - `GET /v1/digital-twins/{entity_id}/state` $\rightarrow$ Nhận `404 Not Found` (thay thế bằng `GET /v1/digital-twins/{entity_id}?format=simplified`).
>    - `GET /v1/digital-twins/{entity_id}/history` $\rightarrow$ Nhận `404 Not Found` (hợp nhất vào `GET /v1/digital-twins/{entity_id}/temporal`).
>    - `PATCH /v1/digital-twins/{entity_id}` $\rightarrow$ Nhận `405 Method Not Allowed` (thay thế bằng `PATCH /v1/digital-twins/{entity_id}/attrs`).
> 3. **Endpoint Chi tiết Thực thể Chuẩn tắc ĐÃ TỒN TẠI trong V1.9.1 Profile:**
>    - `GET /v1/digital-twins/{entity_id}` với `entity_id` là URN chuẩn hóa (`urn:ngsi-ld:Gateway:<gateway_id>` hoặc `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`).
>    - Header: `Accept: application/ld+json` (trả về Content-Type `application/ld+json; charset=utf-8` kèm `@context` V1.9) hoặc `application/json`.
>    - Gateway Detail trả về quan hệ `hasSensor` với mảng URI `object: [...]` và `objectType: "Sensor"`; phân trang quan hệ thông qua header `Link: <URI>; rel="next"`.
>    - Sensor Detail trả về định danh, quan hệ `isHostedBy` trỏ về Gateway cha, cùng Property `measurement` và `sample_interval_us` mới nhất kèm mã `unitCode` UN/CEFACT (như `CEL`, `C81`, `P1`) bên trong `measurement`. Nếu chưa map hoặc chưa có observation, đơn vị thô được giữ dưới Property `measurementUnit` ở root entity (không có root `unitCode`).
> 4. **Trang Chi tiết Hiện đại (Modern Detail View):**
>    - Tải trực tiếp thông tin thực thể từ `GET /v1/digital-twins/{entity_id}`.
>    - Tải trạng thái hiện thời nhanh qua `GET /v1/digital-twins/{entity_id}?format=simplified` (đọc bảng `twin_states`, không quét hypertable).
>    - Tải lịch sử đo lường và chu kỳ đo qua `GET /v1/digital-twins/{entity_id}/temporal`.

### 3.3 Khởi tạo Thực thể Metadata (Entity Creation: POST /v1/digital-twins)
Ngoài endpoint Stage 2 `PUT /v1/gateways/{id}/sensors/{id}` vẫn duy trì tương thích, Stage 4 cung cấp endpoint chuẩn hóa:
1. **Quyền hạn tạo thực thể:**
   - Tạo `Gateway`: Yêu cầu quyền **Platform Admin** (kiểm tra `public.platform_admins`). Phản hồi `201 Created` kèm header `Location: /v1/digital-twins/<escaped_urn>`, hoặc `200 OK` nếu exact replay (không có header Location).
   - Tạo `Sensor`: Yêu cầu quyền **Gateway Owner** trên trạm cha (hoặc Platform Admin). Phản hồi `201 Created` (kèm header `Location`) hoặc `200 OK` (exact replay trùng `name` và `unit`).
2. **Payload tạo Sensor:**
   ```json
   {
     "entity_type": "Sensor",
     "gateway_id": "gw-01",
     "sensor_id": "temp-01",
     "name": "Temperature Sensor",
     "unit": "Cel"
   }
   ```
3. **Mã trạng thái phản hồi:**
   - **`201 Created`**: Tạo mới thực thể thành công; header `Location` chứa đường dẫn tương đối tới thực thể vừa tạo.
   - **`200 OK`**: Exact Replay (dữ liệu gửi lên trùng khớp hoàn toàn bản ghi hiện có, không có header Location).
   - **`409 Conflict`**: Xung đột biểu diễn (cùng ID nhưng khác `name` hoặc `unit`).
   - **`403 Forbidden`**: `operator` hoặc `viewer` cố tạo thực thể.
   - **`404 Not Found`**: Trạm cha không tồn tại hoặc người dùng không có quyền (Anti-Enumeration).

### 3.4 Cập nhật Siêu dữ liệu Thực thể qua Property Fragment (PATCH /v1/digital-twins/{entity_id}/attrs)
> **CẢNH BÁO QUAN TRỌNG VỀ RANH GIỚI METADATA VÀ THIẾT BỊ:**
> Phản hồi thành công `200 OK` từ lệnh `PATCH /v1/digital-twins/{entity_id}/attrs` **CHỈ LÀ CẬP NHẬT SIÊU DỮ LIỆU TẠI CƠ SỞ DỮ LIỆU (Relational / Metadata update ONLY)**, tuyệt đối **KHÔNG PHẢI LÀ XÁC NHẬN THIẾT BỊ ĐÃ THỰC THI (NOT confirmed device execution)**. Thao tác này không tạo lệnh downlink, không ghi vào outbox, và không gửi bản tin MQTT xuống thiết bị phần cứng. Đường dẫn gốc `PATCH /v1/digital-twins/{entity_id}` nhận phản hồi `405 Method Not Allowed`.

1. **Payload Normalized Property Fragment:**
   Khác với JSON phẳng trước đây, body yêu cầu định dạng Property fragment chuẩn hóa theo V1.9.1 profile:
   ```json
   {
     "name": {
       "type": "Property",
       "value": "Tên mới"
     },
     "description": {
       "type": "Property",
       "value": "Mô tả mới"
     }
   }
   ```
2. **Quy tắc Allowlist và Phân quyền PATCH:**
   - **Gateway Entity:** Chỉ **Platform Admin** mới được phép PATCH. Mọi tài khoản không phải Admin (kể cả Owner trạm hoặc Non-member) đều bị từ chối với `403 Forbidden` do rào chắn kiểm tra quyền Admin đầu tiên. Cho phép cập nhật `name` (Property string) và `description` (Property string hoặc sentinel `{"description":{"type":"Property","value":"urn:ngsi-ld:null"}}` để xóa mô tả). Cấm cập nhật các trường khác.
   - **Sensor Entity:** Chỉ **Gateway Owner** (hoặc Platform Admin) mới được phép PATCH. Operator và Viewer nhận `403 Forbidden`. Non-member gọi sẽ nhận `404 Not Found` (Anti-Enumeration qua kiểm tra quan hệ `user_gateways`). Cho phép cập nhật duy nhất trường `name` (Property chuỗi ký tự bắt buộc, 1..256 bytes, không được rỗng hoặc null). Trường `unit` là **bất biến (strictly immutable)**, không thể sửa đổi qua PATCH.
   - **Operator & Viewer:** Hoàn toàn **chỉ đọc (Read-only)** đối với thao tác mutation, mọi yêu cầu PATCH đều bị từ chối với `403 Forbidden`.
3. **Cơ chế No-op và Cập nhật Timestamp:**
   - Nếu Client gửi PATCH với giá trị trùng khớp hoàn toàn giá trị hiện tại: Server ghi nhận No-op, không chạy SQL UPDATE vô ích, commit thành công và giữ nguyên `updated_at` cũ.
   - Khi có thay đổi thực sự: Server cập nhật bảng quan hệ (`gateways` hoặc `sensors`) và cập nhật `twin_entities.updated_at` thành `statement_timestamp()`.

### 3.5 Luồng Kiểm kê Toàn cầu của Platform Admin (Admin Gateway Inventory Flow)
Khi tài khoản được xác định là Platform Admin truy cập trang quản trị:
1. Client gửi `GET /v1/admin/gateways?limit=100` (kèm `after=<cursor>` nếu tải trang tiếp theo). Tham số `limit` mặc định là 100 và tối đa là 200 trên môi trường chuẩn.
2. Backend kiểm tra quyền trong `public.platform_admins` và phản hồi danh mục kiểm kê toàn cầu độc lập với bảng `user_gateways`:
   - `items`: Danh sách Gateway (`gateway_id`, `name`, `description`, `created_at`). Không có `role`, không có `owner_user_id`, không có bí mật MQTT.
   - `next_cursor`: Con trỏ phân trang, dùng cho request kế tiếp (`?after=<next_cursor>`). Khi `next_cursor == null`, danh sách đã duyệt hết.

---

## 4. Ma trận Trạng thái Giao diện Stage 4 (Stage 4 UI States Checklist)

Mọi màn hình trên Client phải xử lý đầy đủ các trạng thái sau để đảm bảo giao diện luôn tường minh và có thể giải thích được:

| Mã Trạng thái UI | Tình huống Kích hoạt từ Server | Hành vi Giao diện Yêu cầu (UI Behavior) |
|---|---|---|
| **`initial`** | Chưa có tương tác tải dữ liệu. | Hiển thị khung chờ, chuẩn bị tải hoặc trạng thái ban đầu của trang. |
| **`loading`** | Đang thực thi request hoặc làm mới dữ liệu. | Hiển thị hiệu ứng tải (progress indicator/shimmer), khóa tạm thời các thao tác trùng lặp. Cho phép hủy request (`cancel`). |
| **`success`** | Nhận `200 OK` (hoặc `201 Created` khi tạo mới Sensor/Gateway) với dữ liệu hợp lệ. | - Danh mục Twin: Hiển thị mảng root array `[...]`.<br>- Chi tiết Twin: Hiển thị thông tin thực thể NGSI-LD và đồ thị quan hệ `hasSensor` (`object` array), trích xuất mã đơn vị UN/CEFACT `unitCode`.<br>- Trạng thái Twin: Hiển thị `reported_state` và `reported_version` từ `?format=simplified`.<br>- Lịch sử Temporal: Hiển thị biểu đồ từ mảng `values` (raw) hoặc các mảng triples `avg`, `min`, `max`, `totalCount` (bucket). |
| **`empty`** | Nhận `200 OK` nhưng mảng rỗng `[]` (List Twin) hoặc lịch sử `values: []`, method arrays rỗng. | Hiển thị thông báo rỗng thân thiện (ví dụ: *"Bạn chưa được phân quyền trên trạm nào"* hoặc *"Không có dữ liệu đo lường trong khoảng thời gian đã chọn"*). Tuyệt đối **không coi đây là lỗi**. |
| **`no-privacy 404`** | Nhận `404 Not Found` (`code: "not_found"`). | Xảy ra khi truy cập `gateway_id` lạ, `entity_id` không tồn tại, trạm mà người dùng không có quyền trong `user_gateways`, hoặc khi gọi vào các route cũ đã thu hồi (`/v1/telemetry/history`, `/{id}/state`, `/{id}/history`). Đối với PATCH Gateway, mọi tài khoản không phải Admin đều nhận `403 Forbidden` trước. **Nguyên tắc Anti-Enumeration:** Giao diện chỉ hiển thị *"Không tìm thấy tài nguyên"*, không đoán định tài nguyên có tồn tại hay không. |
| **`400 validation error`** | Nhận `400 Bad Request` (`code: "invalid_bounds"`, `code: "invalid_time_range"`, `code: "invalid_resolution"`, `code: "invalid_urn"`, `code: "invalid_request"`). | Lỗi cú pháp hoặc tham số truy vấn không hợp lệ (ví dụ: `timeAt >= endTimeAt`, sai định dạng RFC3339, duration không thuộc 9 canonical durations, hoặc URN không đúng chuẩn). Hiển thị thông báo cụ thể từ trường `error.message`. |
| **`400 raw overflow`** | Nhận `400 Bad Request` (`code: "raw_limit_exceeded"`). | Xảy ra khi truy vấn dữ liệu thô (`temporal`) nhưng số lượng quan sát vượt quá `max_points` (phát hiện bằng giới hạn quét `max_points + 1`; mặc định hợp đồng 1000, tối thiểu 100, chặn trên 2000). Giao diện hiển thị: *"Số điểm dữ liệu thô vượt hạn mức yêu cầu. Vui lòng thu hẹp khoảng thời gian hoặc chỉ định tham số gom nhóm aggrPeriodDuration"*. |
| **`401 refresh limited`** | Nhận `401 Unauthorized` (`code: "unauthorized"`). | Phiên làm việc hết hạn hoặc token lỗi. Client kích hoạt luồng Refresh Token **giới hạn duy nhất 1 lần (bounded retry)**. Nếu thất bại, xóa session cục bộ và chuyển hướng về màn hình Đăng nhập. Tránh tạo vòng lặp vô hạn. |
| **`403 forbidden`** | Nhận `403 Forbidden` (`code: "forbidden"`). | - Tài khoản thông thường cố gọi API Quản trị `/v1/admin/*`: *"Thao tác yêu cầu quyền Quản trị viên hệ thống"*.<br>- `operator` hoặc `viewer` gọi tạo/sửa Sensor (`POST /v1/digital-twins`, `PATCH /v1/digital-twins/{id}/attrs`): *"Chỉ Chủ sở hữu trạm (Owner) mới có quyền tạo hoặc chỉnh sửa cảm biến"*.<br>- Mọi tài khoản không phải Platform Admin gọi PATCH Gateway: *"Chỉ Quản trị viên nền tảng mới có quyền sửa thông tin trạm"*.<br>- Origin bị từ chối trong CORS preflight. |
| **`406 not acceptable`** | Nhận `406 Not Acceptable` (`code: "not_acceptable"`). | Header `Accept` gửi lên không tương thích với media type server phục vụ (ví dụ: yêu cầu text/plain hoặc gán `q=0` cho định dạng server trả về). Client cần cấu hình Accept phù hợp (`application/ld+json` hoặc `application/json`). |
| **`409 conflict`** | Nhận `409 Conflict` (`code: "conflict"`). | Khi gọi tạo Sensor hoặc Gateway (`POST /v1/digital-twins` hoặc `PUT sensor`) nhưng thực thể đã tồn tại với thông tin khác (`name`, `unit` khác). Hiển thị: *"Thực thể đã tồn tại với thông tin khác. Hệ thống không cho phép ghi đè cấu hình qua lệnh này"*. |
| **`503 dependency retry`** | Nhận `503 Service Unavailable` (`code: "service_unavailable"`, `code: "dependency_failed"` hoặc `code: "credential_runtime_disabled"`). | Quá thời hạn xử lý cơ sở dữ liệu (`AUTHORIZATION_TIMEOUT`), timeout truy vấn dữ liệu lớn, database tạm thời gián đoạn, rào chắn khởi động chưa xong, hoặc tính năng credential bị tắt. Hiển thị *"Hệ thống bận hoặc dịch vụ tạm thời gián đoạn, vui lòng thử lại sau"*, cung cấp nút Thử lại (Retry) kèm giãn cách số mũ. |
| **`500 safe / support request_id`** | Nhận `500 Internal Server Error` (`code: "internal_error"`). | Lỗi máy chủ nội bộ. Server không trả về stack trace. Client **bắt buộc đọc `request_id`** (từ `X-Request-ID` header hoặc payload `error.request_id`) để hiển thị thông báo: *"Đã xảy ra sự cố hệ thống. Mã tra cứu hỗ trợ: `<request_id>`"*. |
| **`501 feature unavailable`** | Nhận `501 Not Implemented` (`code: "not_implemented"`). | Hiện tại **CHỈ CÒN DUY NHẤT** endpoint WebSocket (`GET /v1/ws`) phản hồi 501. Các API Lịch sử đo lường và Bản sao số Digital Twin **ĐÃ HOẠT ĐỘNG HOÀN TOÀN** trong Stage 4, không còn trả về 501. |
| **`405 developer error`** | Nhận `405 Method Not Allowed` (`code: "method_not_allowed"`). | Lỗi gửi sai phương thức (ví dụ: gửi `PATCH` vào route gốc `/v1/digital-twins/{entity_id}` thay vì `/attrs`). Ghi log lỗi phát triển, không kích hoạt thao tác sai. |

---

## 5. Xử lý Đồng bộ Quyền, Cache, Phân trang và Chống Race Condition

### 5.1 Thay đổi quyền cùng một JWT (Same-JWT Membership Change)
- Khi Quản trị viên thay đổi vai trò (ví dụ: từ `viewer` sang `operator`) hoặc thu hồi quyền truy cập trạm trong CSDL PostgreSQL (`public.user_gateways`), **JWT hiện tại của người dùng không cần cấp phát lại**.
- Thay đổi này có hiệu lực **ngay lập tức trong lần truy vấn CSDL tiếp theo** tại Go Backend.
- **Không có cơ chế Push Realtime:** Trong Stage 4, hệ thống chưa có WebSocket hoạt động để tự động đẩy thông báo đổi quyền xuống Client. Client nhận biết quyền mới khi thực hiện thao tác tải lại dữ liệu (Pull-to-refresh hoặc điều hướng lại). Nếu quyền bị thu hồi, các lệnh gọi tiếp theo sẽ lập tức trả về `404 Not Found` hoặc `403 Forbidden`.

### 5.2 Quản lý Bộ nhớ đệm, Phân trang Header Link và Số nguyên Lớn (Cache, Link Pagination & Precision)
1. **Chính sách Cache-Control:**
   - Go Backend luôn trả về header `Cache-Control: no-store` cho toàn bộ các route của Stage 4 và các danh mục nghiệp vụ.
   - Các API đột biến siêu dữ liệu (`POST`, `PATCH`) và preflight `OPTIONS` trả về kèm `Pragma: no-cache`.
   - Client **không được lưu vĩnh viễn** danh sách thực thể hay dữ liệu chuỗi thời gian vào bộ nhớ cục bộ lâu dài (`localStorage` hay CSDL offline). Toàn bộ bộ nhớ đệm RAM phải được xóa sạch khi người dùng đăng xuất hoặc tải lại ứng dụng.
2. **Quy tắc Duyệt Phân trang bằng Header `Link` (RFC 8288 Next URI Traversal):**
   - Thay vì đọc `next_cursor` từ body (đã bị loại bỏ trong V1.9.1 profile), Client **bắt buộc đọc và theo trực tiếp toàn bộ URI** từ HTTP Header:
     ```http
     Link: </v1/digital-twins?type=Sensor&limit=20&cursor=v1.xxx>; rel="next"
     ```
   - **Quy tắc trích xuất URI:** Parser của Client trích xuất chuỗi URI nằm trong cặp ngoặc nhọn `<...>` có tham số `rel="next"`. Client gửi request tiếp theo trực tiếp đến URI tương đối này, bảo đảm không bị rơi rụng các query parameters (`type`, `gateway_id`, `relationship_limit`). Với biến thể danh sách phẳng `?format=simplified`, URI `Link` cũng bảo toàn `format=simplified` cùng toàn bộ filter, nên Client chỉ cần follow nguyên URI (không tự thêm/bớt `format`).
   - **Trang cuối (Terminal Page):** Khi đến trang cuối cùng hoặc khi danh sách rỗng, server không gửi header `Link`. Client dừng phân trang khi header `Link` vắng mặt.
   - **Ngữ nghĩa trang quan hệ từng phần (Partial-page Semantics):** Quan hệ `hasSensor` trong chi tiết Gateway chỉ đại diện cho một trang cạnh của thực thể (`relationship_limit`); Client không được coi đây là toàn bộ quan hệ nếu header `Link` vẫn tồn tại.
   - **Reset phân trang khi đổi bộ lọc:** Khi người dùng thay đổi tham số lọc (`type`, trạm), Client phải xóa URI phân trang cũ và bắt đầu lại từ URL gốc.
3. **Cấu hình Truy vấn Lịch sử Temporal (`GET /v1/digital-twins/{entity_id}/temporal`):**
   - Hợp nhất truy vấn viễn trắc và chu kỳ lấy mẫu qua tham số `attrs=measurement` hoặc `attrs=sample_interval_us`. Bắt buộc `timerel=between`, `timeAt` và `endTimeAt` UTC RFC3339 `Z`.
   - Gom nhóm qua `aggrPeriodDuration` hỗ trợ đúng 9 canonical durations: `PT0.1S`, `PT1S`, `PT10S`, `PT1M`, `PT5M`, `PT15M`, `PT1H`, `PT6H`, `P1D`.
   - Khi không có `aggrPeriodDuration`, truy vấn ở chế độ thô (raw). Nếu số mẫu vượt `max_points`, server trả `400 raw_limit_exceeded`, không tự động fallback auto ra public API.
4. **Xử lý Số nguyên Lớn và `totalCount` trên Flutter Web (Exact Number Handling):**
   - Theo chuẩn IEEE 754, kiểu `number` trong JavaScript và Web Dart chỉ biểu diễn an toàn số nguyên đến $2^{53} - 1$ (`9,007,199,254,740,991`).
   - Trong phản hồi temporal aggregated, trường **`totalCount` là số nguyên JSON (exact integer)** (ví dụ: `[[120, "...", "..."]]`). Trên môi trường Flutter Web, Client phải giải mã trường này thành số nguyên (`int` / `BigInt`), tuyệt đối không ép kiểu qua JavaScript `double` để tránh làm biến dạng giá trị số mẫu đếm được khi dữ liệu tích lũy lớn.
   - Trường phiên bản (`reported_version`, `desired_version`) ở state view tiếp tục được định dạng dưới dạng chuỗi số nguyên thập phân (decimal string). Client phân tích chuỗi này thành `BigInt` khi cần so sánh phiên bản.

### 5.3 Bảo vệ chống phản hồi lệch thứ tự và Hủy yêu cầu cũ (Out-of-Order Guarding & Cancellation)
Để đảm bảo giao diện phản ánh đúng tương tác người dùng:
- **Hủy yêu cầu cũ khi chuyển ngữ cảnh:** Khi người dùng đổi trạm, đổi cảm biến, hoặc kéo thay đổi khoảng thời gian biểu đồ liên tục, Client bắt buộc kích hoạt cơ chế hủy (`AbortController.abort()` trên Web JS hoặc `CancelToken.cancel()` trên Dio/HttpClient) đối với request đang bay trước khi phát đi request mới.
- **Bỏ qua phản hồi cũ (Discard Stale Responses):** Nếu không thể hủy kết nối ở tầng mạng, Client duy trì biến đếm tuần tự `active_request_id`. Khi một phản hồi trả về, nếu mã định danh không khớp với yêu cầu đang hoạt động gần nhất, toàn bộ dữ liệu phản hồi đó phải bị hủy bỏ ngay lập tức để tránh ghi đè giao diện.
- **Dọn dẹp dữ liệu khi quyền thay đổi:** Khi phát hiện quyền bị thay đổi (nhận `403` hoặc `404`), Client phải lập tức xóa sạch dữ liệu thực thể hoặc biểu đồ đang hiển thị trên màn hình.

---

## 6. Phân cấp Vai trò Người dùng và Ranh giới Nút Bấm Giao diện (Role Enforcement & UI Boundaries)

### 6.1 Đồng nhất quyền đọc cho các vai trò thành viên trạm
Căn cứ theo hiện thực CSDL và mã nguồn nghiệp vụ:
- Ba vai trò thành viên trạm gồm `owner`, `operator`, và `viewer` **đều sử dụng chung cùng một tập hợp Read API** cho các trạm mà mình được gán quyền trong `user_gateways`:
  - `GET /v1/gateways` (danh sách trạm được cấp quyền)
  - `GET /v1/gateways/{gateway_id}/sensors` (danh sách cảm biến thuộc trạm)
  - `GET /v1/digital-twins` (danh sách Bản sao số root array `[...]`, phân trang qua header `Link`); biến thể summary phẳng `GET /v1/digital-twins?format=simplified` (`application/json`, item phẳng `id`/`type`/`name`/`gateway_id`/`modifiedAt`).
  - `GET /v1/digital-twins/{entity_id}` (chi tiết thực thể NGSI-LD JSON-LD và quan hệ `hasSensor`)
  - `GET /v1/digital-twins/{entity_id}?format=simplified` (trạng thái phẳng hiện thời `reported_state` và version)
  - `GET /v1/digital-twins/{entity_id}/temporal` (lịch sử temporal của `measurement` hoặc `sample_interval_us`)
- Cả ba vai trò đều được xem đầy đủ biểu đồ và trạng thái của trạm được phân quyền. Tài khoản `non-member` gọi vào các API trên đều nhận `404 Not Found` (Anti-Enumeration).

### 6.2 Phân định Quyền Khởi tạo và Sửa đổi Siêu dữ liệu (Metadata Mutation Boundaries)
1. **Khởi tạo và sửa đổi cảm biến (Sensor Metadata):**
   - Duy nhất vai trò **`owner`** của trạm cha (hoặc Platform Admin) mới có quyền tạo Sensor mới (`POST /v1/digital-twins` trả `Location` header hoặc `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}`).
   - Duy nhất vai trò **`owner`** mới có quyền đổi tên Sensor (`PATCH /v1/digital-twins/{sensor_urn}/attrs`). Cho phép sửa duy nhất trường `name` qua Property fragment (1..256 bytes, không được để trống hoặc null). Trường `unit` là **bất biến (strictly immutable)**, không thể sửa đổi sau khi tạo.
   - Hai vai trò **`operator`** và **`viewer`** tuyệt đối không có quyền này và sẽ nhận mã lỗi `403 Forbidden` (`code: "forbidden"`, `message: "access denied"`). Tài khoản **`non-member`** gọi PATCH Sensor nhận `404 Not Found` (Anti-Enumeration). Giao diện Client bắt buộc phải ẩn hoặc vô hiệu hóa nút "Thêm Cảm biến" và nút "Sửa tên Cảm biến" đối với các vai trò không phải Owner.
2. **Khởi tạo và sửa đổi trạm (Gateway Metadata):**
   - Chỉ **Platform Admin** mới có quyền tạo trạm mới (`POST /v1/digital-twins` trả `Location` header hoặc qua Admin API) và sửa đổi thông tin trạm (`PATCH /v1/digital-twins/{gateway_urn}/attrs`).
   - Cho phép cập nhật `name` và `description` qua Property fragment (xóa mô tả bằng sentinel `urn:ngsi-ld:null`). Cấm cập nhật các trường hệ thống khác.
   - Mọi tài khoản không phải Platform Admin (kể cả `owner` của trạm hoặc `non-member`) gọi PATCH trạm đều bị từ chối với `403 Forbidden` do rào chắn kiểm tra quyền Admin đầu tiên. Đường dẫn gốc không có `/attrs` nhận `405 Method Not Allowed`.
3. **Ranh giới Siêu dữ liệu và CẤM BẬT NÚT ĐIỀU KHIỂN SỚM:**
   - **Phản hồi Metadata KHÔNG PHẢI Device Execution:** Phản hồi `200 OK` từ lệnh PATCH chỉ là cập nhật quan hệ/siêu dữ liệu trong CSDL máy chủ, **tuyệt đối không gửi lệnh MQTT hay xác nhận thiết bị phần cứng đã thực thi**.
   - **Cấm bật nút điều khiển và cấu hình thiết bị lâu bền:** Trong phạm vi Giai đoạn 4, các chức năng ghi cấu hình thiết bị lâu bền (`desired_state`), gửi lệnh điều khiển downlink (reboot, capture-image), và worker đối soát lệnh thuộc Giai đoạn 5 và 6 (**CHƯA ĐƯỢC KÍCH HOẠT**).
   - **Quy tắc giao diện:** Tuyệt đối không hiển thị hoặc không kích hoạt các nút bấm cấu hình thiết bị lâu bền (`desired_state`), nút gửi lệnh điều khiển, nút khởi động lại hay nút chụp ảnh cho bất kỳ vai trò nào.

### 6.3 Phân định Quyền Quản trị Nền tảng (Platform Admin Authority)
- Quyền quản trị nền tảng được kiểm tra độc lập tại bảng `public.platform_admins` trong PostgreSQL.
- **CẤM suy diễn quyền Admin:**
  1. Không được suy diễn quyền Admin từ vai trò `owner` trên một Gateway cụ thể.
  2. Không được suy diễn từ JWT claim `role: "authenticated"`.
  3. Client không bao giờ sở hữu `service_role` key của Supabase.
  4. Hệ thống **không có API tự phong quyền Admin** (No platformAdmin self-API).
- **Platform Admin không bypass quyền thành viên:** Nếu Platform Admin gọi đọc dữ liệu nghiệp vụ (`GET /v1/gateways`, `GET /v1/digital-twins/{entity_id}`) trên trạm mà mình không có quyền trong `user_gateways`, backend vẫn trả về `200 OK` rỗng hoặc `404 Not Found`. Muốn xem dữ liệu nghiệp vụ, Admin phải được gán quyền cụ thể trong `user_gateways`.
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

### 7.4 Tích hợp Quản trị Người dùng & Quyền Thành viên Gateway (Admin User & Gateway Membership Integration)

*Xem chi tiết toàn bộ hợp đồng API tại:* [docs/client/07_ADMIN_API_CLIENT_CONTRACT.md](07_ADMIN_API_CLIENT_CONTRACT.md#5-chi-tiết-các-endpoint-quản-trị-tài-khoản-người-dùng-và-quyền-thành-viên-gateway-admin-user--gateway-membership-apis).

Khi xây dựng giao diện quản trị dành cho Platform Admin, ứng dụng Client cần áp dụng các quy chuẩn thiết kế biểu mẫu, xử lý lỗi và đồng bộ trạng thái như sau:

#### A. Biểu mẫu Tạo Tài khoản Người dùng Mới (`POST /v1/admin/users`)
1. **Kiểm tra hợp lệ trước khi gửi (Client-side Pre-validation):**
   - `full_name`: Bắt buộc nhập, độ dài 1 đến 256 bytes UTF-8, không được để trống hoặc chỉ có khoảng trắng.
   - `email`: Định dạng email chuẩn RFC 5322, tối đa 254 bytes. Client nên tự động loại bỏ khoảng trắng thừa đầu/cuối trước khi gửi; giữ nguyên chữ hoa/thường.
   - `password`: Độ dài bắt buộc từ 6 ký tự trở lên và tối đa 72 bytes UTF-8 (giới hạn cứng của thuật toán mã hóa mật khẩu bcrypt trên GoTrue).
2. **Xử lý sự cố mạng & Timeout (Supervised Reconcile via User List, No Blind Retry):**
   - Nếu request tạo tài khoản gặp lỗi quá thời gian (Network/Socket Timeout) hoặc mã lỗi `503 Service Unavailable`, **tuyệt đối KHÔNG tự ý gửi lại request tạo mới ngay lập tức (no blind retry)**.
   - Khi timeout xảy ra, tài khoản có thể đã được GoTrue Auth tạo thành công nhưng phản hồi mạng bị thất lạc hoặc việc kiểm tra hồ sơ bị gián đoạn. Gửi lại ngay có thể nhận mã lỗi `409 user_already_exists`.
   - **Quy trình đối soát an toàn (Supervised Reconcile):** Client gọi `GET /v1/admin/users` để kiểm tra danh sách:
     * Nếu email vừa tạo đã có trong danh sách: Tài khoản đã được tạo, kiểm tra hồ sơ có đồng bộ đúng hay không trước khi trích xuất `id`.
     * Việc email chưa xuất hiện trong danh sách **không chứng minh** request cũ không thể hoàn tất muộn hơn trên GoTrue. Quản trị viên cần chờ hệ thống ổn định, đối soát log hoặc kiểm tra lỗi missing-profile trước khi quyết định thử lại thủ công.
   - Backend **tuyệt đối không tự động xóa tài khoản (no automatic account deletion)** khi có lỗi kiểm tra hồ sơ, nhằm bảo toàn dữ liệu phục vụ đối soát có giám sát.

#### B. Đọc Danh sách Người dùng và Hiển thị Tên (`GET /v1/admin/users` & `PATCH /v1/admin/users/{user_id}`)
1. **Nguồn chân lý của tên hiển thị:**
   - Trường `full_name` hiển thị trên bảng người dùng được đọc trực tiếp từ `public.profiles.full_name` trong DTO phản hồi.
   - Trường này có thể là `null` đối với một số tài khoản legacy cũ; giao diện cần xử lý hiển thị fallback an toàn (ví dụ: hiển thị email hoặc `"(Chưa đặt tên)"`, không tự động gửi chuỗi rỗng lên server).
   - Sau khi admin đổi tên qua `PATCH /v1/admin/users/{id}`, tên mới trong `public.profiles` là nguồn chân lý duy nhất. Tuyệt đối không fallback sang Auth metadata.
2. **Xử lý giới hạn dung lượng danh sách (`409 user_list_capacity_exceeded`):**
   - `GET /v1/admin/users` tải toàn bộ danh sách người dùng trong một response duy nhất và không hỗ trợ query pagination/search.
   - Nếu số lượng tài khoản/membership vượt ngưỡng cấu hình server (`USER_LIST_MAX_USERS`, `USER_LIST_MAX_MEMBERSHIPS`, `USER_LIST_MAX_RESPONSE_BYTES`), server trả về mã lỗi `409 Conflict` (`user_list_capacity_exceeded`).
   - Giao diện client phải bắt mã lỗi này và hiển thị thông báo rõ ràng cho quản trị viên liên hệ kỹ sư hạ tầng nâng hạn mức cấu hình server, tránh crash giao diện hoặc hiển thị danh sách rỗng giả định.
3. **Cấm tự hạ quyền Admin (Self-Demotion Guard):**
   - Khi chỉnh sửa tài khoản, nếu tài khoản mục tiêu trùng với tài khoản của admin đang đăng nhập (`actorID == targetID`), giao diện nên **vô hiệu hóa (disable) hoặc ẩn tùy chọn hạ vai trò xuống `"user"`**.
   - Nếu admin cố tình gửi request hạ quyền của chính mình, server sẽ từ chối với mã lỗi `409 Conflict` (`self_admin_demotion_forbidden`) và rollback nguyên tử toàn bộ thay đổi (kể cả phần đổi tên).
   - *Lưu ý về chính sách:* Server không có cơ chế đếm admin cuối cùng (no last-admin count policy). Việc bảo vệ chỉ nhằm ngăn chặn hành vi tự tước quyền của chính caller; tình trạng mất toàn bộ admin vẫn có thể xảy ra nếu hai admin thực hiện hạ quyền chéo đồng thời qua API (`concurrent API cross-demotions`) hoặc do can thiệp trực tiếp ngoài API trên CSDL.

#### C. Hai Luồng Giao diện Phân quyền Gateway Tách biệt Hoàn toàn
Giao diện quản trị phải chia ranh giới rõ ràng thành 2 nút bấm / biểu mẫu độc lập, không gộp chung vào một luồng:
1. **Biểu mẫu Gán Vai trò Thông thường (`PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`):**
   - Hộp thoại gán quyền thành viên chỉ cung cấp 2 lựa chọn: **Operator (Vận hành)** hoặc **Viewer (Xem)**.
   - Tuyệt đối không đưa lựa chọn `Owner` vào danh sách dropdown của biểu mẫu này (gửi `owner` sẽ bị server từ chối `400 Bad Request` với mã `invalid_role`).
   - Nếu người dùng mục tiêu hiện đang là `owner` của trạm, giao diện phải vô hiệu hóa việc đổi vai trò qua biểu mẫu này (server từ chối `409 Conflict` với mã `owner_transfer_required`).
2. **Biểu mẫu Chuyển quyền Sở hữu Gateway (`PUT /v1/admin/gateways/{gateway_id}/owner`):**
   - Hộp thoại chọn một người dùng trong hệ thống để chuyển giao vị thế `owner`.
   - **Thông báo xác nhận bắt buộc (Confirmation Dialog):** Phải hiển thị cảnh báo rõ ràng cho quản trị viên:
     > *"Chủ sở hữu hiện tại của trạm sẽ được chuyển thành Viewer (tiếp tục có quyền đọc dữ liệu nhưng mất toàn bộ quyền quản trị cảm biến và cấu hình trạm). Mỗi trạm chỉ có duy nhất một Owner."*
   - Sau khi chuyển owner thành công, nếu có request tạo Gateway cũ gửi lại (`PUT /v1/admin/gateways/{id}`) với owner cũ, server sẽ từ chối `409 Conflict` (đúng logic replay bất biến).

#### D. Tuyên bố Hiện thực (No Premature Flutter Implementation Claims)
Các mô tả biểu mẫu và quy trình trên là đặc tả chuẩn hóa hành vi và hướng dẫn tích hợp giao diện client. Hệ thống **KHÔNG khẳng định mã nguồn giao diện Flutter Web đã được hiện thực hoặc kiểm thử hoàn chỉnh** ở giai đoạn này.

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

*Ghi chú bảo mật & Phạm vi:*
- Toàn bộ các tài khoản dưới đây được định danh dưới dạng **biểu tượng quy ước (symbolic only)**, tuyệt đối không sử dụng email thật, mật khẩu thật, token thật hay tên miền triển khai cụ thể.
- **Tách biệt ranh giới kiểm thử:** Bảng dưới đây là **danh mục kiểm thử tích hợp KỲ VỌNG (Expected Contract Checklist)** theo hợp đồng API Stage 4 để định hướng phát triển Client, **KHÔNG PHẢI là bằng chứng bộ test đã thực chạy trên giao diện Flutter**.

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
| **TC-11: Đọc lịch sử đo lường Temporal** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/temporal?attrs=measurement&timerel=between&timeAt=...&endTimeAt=...` | `200` | `{ "id": "...", "type": "Sensor", "measurement": { "type": "Property", "values": [...] } }` |
| **TC-11b: Vượt hạn mức dữ liệu thô (Raw Overflow)** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/temporal?...` (mẫu thô > max_points) | `400` | `code: "raw_limit_exceeded"` |
| **TC-12: Gọi sai phương thức (Method)** | Có JWT hợp lệ bất kỳ | `POST /v1/gateways` | `405` | `code: "method_not_allowed"` |
| **TC-13: Gọi đường dẫn không tồn tại / thu hồi** | Có JWT hợp lệ bất kỳ | `GET /v1/telemetry/history` hoặc `/v1/unknown-path` | `404` | `code: "not_found"` (Route cũ đã thu hồi) |
| **TC-14: Owner khởi tạo Sensor mới** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` | `201` | `{ "gateway_id": "gw-a", "sensor_id": "s1", "name": "...", "unit": "...", "entity_id": "urn:ngsi-ld:Sensor:gw-a:s1", ... }` |
| **TC-15: Owner phát lại chính xác Sensor (Exact Replay)** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` | `200` | Trả về thông tin cảm biến trùng khớp, giữ nguyên `created_at` ban đầu |
| **TC-16: Owner gửi cấu hình khác lên Sensor cũ** | `Owner A` | `PUT /v1/gateways/gw-a/sensors/s1` (khác name/unit) | `409` | `code: "conflict"`, `message: "resource conflict"` |
| **TC-17: Operator / Viewer cố tạo Sensor** | `Operator` / `Viewer` | `PUT /v1/gateways/gw-a/sensors/s2` | `403` | `code: "forbidden"`, `message: "access denied"` |
| **TC-18: Non-member gọi tạo Sensor** | `Non-member` hoặc `Owner B` gọi trạm A | `PUT /v1/gateways/gw-a/sensors/s2` | `404` | `code: "not_found"`, `message: "resource not found"` (Anti-Enumeration) |
| **TC-19: Platform Admin duyệt danh mục kiểm kê toàn cầu** | `Platform Admin` | `GET /v1/admin/gateways?limit=100` | `200` | `{ "items": [ ... ], "next_cursor": ... }` (Phân trang qua body cursor riêng của Admin) |
| **TC-20: Người dùng thường gọi danh mục kiểm kê** | `Owner A` / `Operator` / `Viewer` / `Non-member` | `GET /v1/admin/gateways` | `403` | `code: "forbidden"`, `message: "insufficient permissions"` |
| **TC-21: Gọi endpoint Reset có giám sát** | `Platform Admin` | `POST /v1/admin/gateways/gw-a/mqtt-credential/reset` | `503` | `code: "service_unavailable"`, `message: "credential request failed"` |
| **TC-22: Duyệt danh sách Digital Twin (Profile V1.9.1)** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins?type=Gateway&limit=50` | `200` | Mảng root array `[...]`, phân trang qua HTTP header `Link: <URI>; rel="next"` (không cursor body) |
| **TC-23: Đọc chi tiết Digital Twin Gateway** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins/urn:ngsi-ld:Gateway:gw-a` (Accept: `application/ld+json`) | `200` | Header `Content-Type: application/ld+json; charset=utf-8`, quan hệ `hasSensor` (mảng URI `object`) |
| **TC-24: Đọc trạng thái hiện thời Digital Twin (State View)** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1?format=simplified` | `200` | `{ "id": "...", "reported_state": {...}, "reported_version": "..." }` (Route cũ `/state` nhận 404) |
| **TC-25: Đọc lịch sử thuộc tính Digital Twin (Temporal)** | `Owner A` / `Operator` / `Viewer` | `GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/temporal?attrs=sample_interval_us&timerel=between&timeAt=...&endTimeAt=...` | `200` | `{ "id": "...", "type": "Sensor", "sample_interval_us": { "type": "Property", "values": [...] } }` |
| **TC-26: Non-member đọc Digital Twin trạm A** | `Non-member` hoặc `Owner B` | `GET /v1/digital-twins/urn:ngsi-ld:Gateway:gw-a` | `404` | `code: "not_found"` (Anti-Enumeration) |
| **TC-27: Owner cập nhật tên Sensor qua Fragment (PATCH)** | `Owner A` | `PATCH /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/attrs` (body: `{"name": {"type": "Property", "value": "New Name"}}`) | `200` | Cập nhật thành công siêu dữ liệu tên; đường dẫn gốc nhận `405 Method Not Allowed` |
| **TC-28: Operator / Viewer cố PATCH Sensor** | `Operator` / `Viewer` | `PATCH /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/attrs` | `403` | `code: "forbidden"`, `message: "access denied"` |
| **TC-28b: Non-member cố PATCH Sensor** | `Non-member` hoặc `Owner B` gọi trạm A | `PATCH /v1/digital-twins/urn:ngsi-ld:Sensor:gw-a:s1/attrs` | `404` | `code: "not_found"` (Anti-Enumeration) |
| **TC-29: Admin cập nhật thông tin Gateway (PATCH)** | `Platform Admin` | `PATCH /v1/digital-twins/urn:ngsi-ld:Gateway:gw-a/attrs` (body Property fragment) | `200` | Cập nhật thành công siêu dữ liệu trạm (xóa mô tả qua `urn:ngsi-ld:null`) |
| **TC-30: Người dùng không phải Admin cố PATCH Gateway** | `Owner A` / `Operator` / `Viewer` / `Non-member` | `PATCH /v1/digital-twins/urn:ngsi-ld:Gateway:gw-a/attrs` | `403` | `code: "forbidden"`, `message: "access denied"` (Kiểm tra Admin trước) |
| **TC-31: Gọi WebSocket chưa triển khai** | Có JWT hợp lệ bất kỳ | `GET /v1/ws` | `501` | `code: "not_implemented"` |

---

## 10. Mã Giả Minh Họa Tích Hợp Giao Diện (Illustrative Pseudocode Only)

> **LƯU Ý:** Đoạn mã giả dưới đây chỉ nhằm mục đích minh họa trực quan luồng xử lý trạng thái, phân trang keyset, bắt lỗi `raw_limit_exceeded`, và phân tích cú pháp số nguyên lớn an toàn theo hợp đồng API Stage 4. Đây không phải là mã nguồn hiện thực hoàn chỉnh và không xác nhận việc ứng dụng Client đã hoàn thiện.

```dart
// ============================================================================
// ILLUSTRATIVE PSEUDOCODE ONLY — NOT COMPLETE PRODUCTION IMPLEMENTATION
// ============================================================================

enum ViewStatus { initial, loading, success, empty, error }

/// Trạng thái chi tiết thực thể Digital Twin
class TwinDetailState {
  final ViewStatus status;
  final Map<String, dynamic>? entity;    // NGSI-LD JSON-LD từ GET /v1/digital-twins/{id}
  final Map<String, dynamic>? state;     // Direct state từ GET /v1/digital-twins/{id}?format=simplified
  final BigInt reportedVersion;          // Tránh mất độ chính xác JS float cho version > 2^53
  final String? nextRelationshipUri;     // URI phân trang quan hệ trích xuất từ header Link rel="next"
  final String? errorMessage;
  final String? requestId;

  const TwinDetailState({
    required this.status,
    this.entity,
    this.state,
    this.reportedVersion = BigInt.zero,
    this.nextRelationshipUri,
    this.errorMessage,
    this.requestId,
  });
}

/// Trạng thái lịch sử chuỗi thời gian Temporal
class TemporalHistoryState {
  final ViewStatus status;
  final String? property;                // measurement hoặc sample_interval_us
  final List<dynamic> rawValues;         // Tuples [val, observedAt] khi raw
  final Map<String, List<dynamic>> bucketValues; // avg, min, max, totalCount triples
  final BigInt totalCount;               // Định dạng từ token số nguyên JSON gốc (lossless)
  final bool rawOverflow;                // Cờ báo lỗi raw_limit_exceeded (400)
  final String? errorMessage;
  final String? requestId;

  const TemporalHistoryState({
    required this.status,
    this.property,
    this.rawValues = const [],
    this.bucketValues = const {},
    this.totalCount = BigInt.zero,
    this.rawOverflow = false,
    this.errorMessage,
    this.requestId,
  });
}

/// Trạng thái danh sách Digital Twin (phân trang bằng header Link)
class TwinListState {
  final ViewStatus status;
  final List<Map<String, dynamic>> items; // Mảng root array Entities [...]
  final String? nextLinkUri;              // Toàn bộ URI tiếp theo trích xuất từ header Link: <URI>; rel="next"
  final String? errorMessage;
  final String? requestId;

  const TwinListState({
    required this.status,
    this.items = const [],
    this.nextLinkUri,
    this.errorMessage,
    this.requestId,
  });
}

class Stage4IntegrationController {
  final ApiClient apiClient;
  final SessionManager sessionManager;
  CancelToken? _activeCancelToken;       // Token hủy request cũ khi đổi ngữ cảnh

  Stage4IntegrationController(this.apiClient, this.sessionManager);

  /// Helper trích xuất next URI từ Header Link (RFC 8288)
  String? _extractNextLink(String? linkHeader) {
    if (linkHeader == null || linkHeader.isEmpty) return null;
    final match = RegExp(r'<([^>]+)>\s*;\s*rel="?next"?').firstMatch(linkHeader);
    return match?.group(1);
  }

  /// 1. Tải danh sách Digital Twin dạng root array và theo URI từ Link header
  Future<TwinListState> loadTwinList({
    String? type,
    String? gatewayId,
    int limit = 50,
    String? explicitNextUri,
  }) async {
    _activeCancelToken?.cancel("Context changed");
    _activeCancelToken = CancelToken();

    try {
      // Nếu có explicitNextUri từ Link header trước đó, follow trực tiếp whole URI
      final path = explicitNextUri ?? '/v1/digital-twins';
      final queryParams = explicitNextUri != null
          ? null
          : {
              if (type != null) 'type': type,
              if (gatewayId != null) 'gateway_id': gatewayId,
              'limit': limit,
            };

      final resp = await apiClient.get(
        path,
        queryParameters: queryParams,
        headers: {'Accept': 'application/ld+json'},
        cancelToken: _activeCancelToken,
      );

      // Phản hồi là mảng root array Entities trực tiếp
      final items = List<Map<String, dynamic>>.from(resp.data ?? []);
      final nextUri = _extractNextLink(resp.headers.value('link'));

      if (items.isEmpty) {
        return const TwinListState(status: ViewStatus.empty, items: []);
      }

      return TwinListState(
        status: ViewStatus.success,
        items: items,
        nextLinkUri: nextUri,
      );
    } on RequestCancelledException {
      return const TwinListState(status: ViewStatus.initial);
    } catch (e) {
      return TwinListState(status: ViewStatus.error, errorMessage: e.toString());
    }
  }

  /// 2. Tải chi tiết thực thể NGSI-LD và trạng thái phẳng (format=simplified)
  Future<TwinDetailState> loadTwinDetail(String entityUrn) async {
    _activeCancelToken?.cancel("Context changed");
    _activeCancelToken = CancelToken();

    try {
      // a. Đọc thông tin thực thể normalized JSON-LD
      final detailResp = await apiClient.get(
        '/v1/digital-twins/$entityUrn',
        headers: {'Accept': 'application/ld+json'},
        cancelToken: _activeCancelToken,
      );

      // b. Đọc trạng thái phẳng trực tiếp qua query extension ?format=simplified (thay route /state cũ)
      final stateResp = await apiClient.get(
        '/v1/digital-twins/$entityUrn',
        queryParameters: {'format': 'simplified'},
        cancelToken: _activeCancelToken,
      );

      // Parse version an toàn bằng BigInt từ decimal string
      final versionStr = stateResp.data['reported_version']?.toString() ?? '0';
      final version = BigInt.tryParse(versionStr) ?? BigInt.zero;
      final relNextUri = _extractNextLink(detailResp.headers.value('link'));

      return TwinDetailState(
        status: ViewStatus.success,
        entity: detailResp.data,
        state: stateResp.data['reported_state'],
        reportedVersion: version,
        nextRelationshipUri: relNextUri,
      );

    } on RequestCancelledException {
      return const TwinDetailState(status: ViewStatus.initial);
    } on UnauthorizedException {
      final refreshed = await sessionManager.refreshSessionOnce();
      if (refreshed) return loadTwinDetail(entityUrn);
      await sessionManager.clearSessionAndNavigateToLogin();
      return const TwinDetailState(status: ViewStatus.error, errorMessage: "Phiên hết hạn.");
    } on NotFoundException {
      return const TwinDetailState(
        status: ViewStatus.error,
        errorMessage: "Không tìm thấy tài nguyên hoặc bạn không có quyền truy cập.",
      );
    } on ServiceUnavailableException catch (e) {
      return TwinDetailState(
        status: ViewStatus.error,
        errorMessage: "Hệ thống bận, vui lòng thử lại sau.",
        requestId: e.requestId,
      );
    }
  }

  /// 3. Tải lịch sử đo lường Temporal (/temporal) với xử lý số nguyên lớn an toàn
  Future<TemporalHistoryState> loadTemporalHistory({
    required String sensorUrn,
    String attrs = 'measurement', // hoặc 'sample_interval_us'
    required DateTime from,
    required DateTime to,
    String? aggrPeriodDuration,   // ví dụ: 'PT1M', 'PT1H' (1 trong 9 canonical durations)
    String? aggrMethods,          // ví dụ: 'avg,min,max,totalCount'
  }) async {
    _activeCancelToken?.cancel("Range changed");
    _activeCancelToken = CancelToken();

    try {
      final resp = await apiClient.get(
        '/v1/digital-twins/$sensorUrn/temporal',
        queryParameters: {
          'attrs': attrs,
          'timerel': 'between',
          'timeAt': from.toUtc().toIso8601String(),
          'endTimeAt': to.toUtc().toIso8601String(),
          if (aggrPeriodDuration != null) 'aggrPeriodDuration': aggrPeriodDuration,
          if (aggrMethods != null) 'aggrMethods': aggrMethods,
        },
        headers: {'Accept': 'application/ld+json'},
        cancelToken: _activeCancelToken,
      );

      final propData = resp.data[attrs] ?? resp.data['measurement'] ?? resp.data['sample_interval_us'];
      if (propData == null) {
        return const TemporalHistoryState(status: ViewStatus.empty);
      }

      // Xử lý nhánh Raw
      if (propData['values'] != null) {
        final values = List<dynamic>.from(propData['values']);
        if (values.isEmpty) return const TemporalHistoryState(status: ViewStatus.empty);
        return TemporalHistoryState(status: ViewStatus.success, property: attrs, rawValues: values);
      }

      // Xử lý nhánh Bucket Aggregated:
      // Lưu ý lossless parsing trên Flutter Web:
      // Kiểu int trong JS runtime bị ép về double (IEEE 754), mất độ chính xác nếu > 2^53.
      // BigInt.from(alreadyDecodedNumber) KHÔNG khôi phục được số đã bị JS làm tròn!
      // Cần decoder lossless đọc token số nguyên trực tiếp từ raw JSON string trước khi chuyển thành số.
      // Reviver của jsonDecode nhận giá trị đã decode nên không tự khôi phục token đã bị làm tròn:
      BigInt exactTotalCount = BigInt.zero;
      final totalCountList = propData['totalCount'];
      if (totalCountList is List && totalCountList.isNotEmpty && totalCountList[0] is List) {
        // Trích xuất phần tử count từ bộ ba [count, start, end]
        final rawCountToken = totalCountList[0][0].toString();
        exactTotalCount = BigInt.tryParse(rawCountToken) ?? BigInt.zero;
      }

      return TemporalHistoryState(
        status: ViewStatus.success,
        property: attrs,
        bucketValues: {
          if (propData['avg'] != null) 'avg': List<dynamic>.from(propData['avg']),
          if (propData['min'] != null) 'min': List<dynamic>.from(propData['min']),
          if (propData['max'] != null) 'max': List<dynamic>.from(propData['max']),
          if (propData['totalCount'] != null) 'totalCount': List<dynamic>.from(propData['totalCount']),
        },
        totalCount: exactTotalCount,
      );

    } on BadRequestException catch (e) {
      if (e.errorCode == 'raw_limit_exceeded') {
        return const TemporalHistoryState(
          status: ViewStatus.error,
          rawOverflow: true,
          errorMessage: "Khoảng thời gian quá dài cho dữ liệu thô. Vui lòng chọn aggrPeriodDuration.",
        );
      }
      return TemporalHistoryState(status: ViewStatus.error, errorMessage: e.message);
    }
  }
}
```

---

## 11. Ranh giới Tính năng Chưa triển khai (Future Roadmap Features Boundary)

Nhằm đảm bảo sự nhất quán với toàn bộ tài liệu hệ thống và phạm vi phân đoạn kiến trúc, các mốc tính năng được phân định rõ ràng như sau:

### 11.1 Các tính năng ĐÃ HOÀN THÀNH trong Giai đoạn 4 (ETSI GS CIM 009 V1.9.1 Profile)
- Danh sách Bản sao số Digital Twin root array `[...]`, phân trang qua header `Link` (`GET /v1/digital-twins`).
- Khởi tạo thực thể Digital Twin create-or-exact-replay, trả header `Location` khi tạo mới (`POST /v1/digital-twins`).
- Chi tiết Bản sao số NGSI-LD JSON-LD, phân trang quan hệ `hasSensor` qua header `Link`, tích hợp `unitCode` UN/CEFACT (`GET /v1/digital-twins/{entity_id}`).
- Trạng thái trực tiếp Digital Twin dạng JSON phẳng qua query extension (`GET /v1/digital-twins/{entity_id}?format=simplified`).
- Lịch sử chuỗi thời gian và thuộc tính thời gian hợp nhất cho Sensor (`GET /v1/digital-twins/{entity_id}/temporal`).
- Cập nhật siêu dữ liệu thực thể qua Property fragment (`PATCH /v1/digital-twins/{entity_id}/attrs`).
*(Lưu ý: Bốn route cũ `/v1/telemetry/history`, `/{id}/state`, `/{id}/history`, `PATCH /{id}` đã bị thu hồi hoàn toàn, trả về 404 hoặc 405).*

### 11.2 Các tính năng CHƯA TRIỂN KHAI (Future Roadmap Stages)
Theo lộ trình kiến trúc hệ thống (`AGENTS.md` và `Backend_summary_plan.md`), các tính năng dưới đây **chưa được kích hoạt** ở tầng người dùng:

1. **Giai đoạn 5 (Stage 5) — Điều khiển Bản sao số Digital Twin:**
   - Đột biến cấu hình thiết bị lâu bền (`PATCH /v1/digital-twins/{entity_id}/desired-state`).
   - Phát hành lệnh điều khiển downlink (`POST /v1/digital-twins/{entity_id}/commands`).
   - Cơ chế Transactional Outbox và MQTT command topic.
2. **Giai đoạn 6 (Stage 6) — Bộ điều phối đối soát lệnh (Command Reconciliation Worker):**
   - Tự động đối soát trạng thái mong muốn (`desired_state`) và trạng thái thực tế (`reported_state`).
   - Quản lý vòng đời lệnh (`pending`, `published`, `acknowledged`, `succeeded`, `failed`, `timeout`).
3. **Giai đoạn 7 (Stage 7) — Truyền phát thời gian thực qua WebSocket:**
   - API WebSocket (`GET /v1/ws`) hiện tại tiếp tục phản hồi `501 Not Implemented`.
   - Client tuyệt đối không giả lập kết nối realtime.
4. **Giai đoạn 8 (Stage 8) — Lưu trữ và Tải lên Tệp Đa phương tiện:**
   - Tải lên hình ảnh qua Supabase Storage private bucket và URL ký trước (Signed URLs).

Client tuyệt đối không xây dựng logic giả lập các luồng dữ liệu trên cho đến khi các Giai đoạn phát triển tương ứng được nghiệm thu chính thức.
