# Tài liệu Client — 03: Cơ chế Phân quyền Người dùng và Trạm Gateway (User–Gateway Authorization) trên Flutter Web

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 6, 6.4, 6.5, 10, 16, 18)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API chuẩn)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái giao diện)
- `docs/backend/stage-2-task-2.3-authorization.md` (Đặc tả phân quyền Backend Giai đoạn 2)

---

## 1. Nguyên tắc cốt tử: Định danh (Identity) và Phân quyền (Authorization)

Trong các ứng dụng IoT chuyên dụng, sự nhầm lẫn giữa **Xác thực danh tính (Authentication)** và **Ủy quyền truy cập tài nguyên (Authorization)** là một trong những lỗ hổng bảo mật nghiêm trọng nhất.

Ứng dụng Flutter Web Client tuân thủ nghiêm ngặt nguyên tắc bảo mật sau:

> **NGUYÊN TẮC VÀNG:**
> Một JSON Web Token (JWT) hợp lệ từ Supabase Auth chỉ chứng minh danh tính người dùng (`Principal.UserID` tồn tại và phiên đăng nhập hợp lệ), **TUYỆT ĐỐI KHÔNG** chứng minh hay cấp quyền tự động cho người dùng đối với bất kỳ trạm Gateway hay Sensor nào trong hệ thống.

```text
+-------------------------------------------------------------------------------+
|                            Xác thực danh tính (Identity)                      |
|                  Supabase Auth (GoTrue) cấp JWT (HS256)                       |
|           => "Người này là ai?" (Who are you? -> User UUID = sub)             |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                           Phân quyền tài nguyên (Authorization)               |
|            Go Backend thẩm định động qua PostgreSQL (user_gateways)           |
|  => "Người này ĐƯỢC PHÉP LÀM GÌ trên trạm NÀO?" (What can you do on which GW?) |
+-------------------------------------------------------------------------------+
```

### Ranh giới bảo mật phía Client Web:
1. **Không tự suy diễn quyền ở Client Web:** Client không được tự đọc payload của JWT để quyết định mở khóa các chức năng nhạy cảm. Mọi danh sách tài nguyên và quyền thao tác được phép đều bắt nguồn trực tiếp từ dữ liệu phản hồi của Go Backend.
2. **Không truyền `user_id` trong Request Body hoặc URL Query:** Để triệt tiêu nguy cơ tấn công mạo danh tham chiếu đối tượng trực tiếp (IDOR - Insecure Direct Object References), Go Backend **từ chối tức thì (HTTP 400 Bad Request)** mọi request có chứa query param `user_id`. Backend chỉ trích xuất duy nhất `user_id` từ claim `sub` trong JWT đã được xác minh chữ ký mã hóa.
3. **Phân quyền động tại thời điểm truy vấn (Same JWT, Next DB Query, No Live Push):** Quyền của người dùng được kiểm tra trực tiếp trên từng truy vấn SQL tại PostgreSQL. Khi dùng cùng một JWT, nếu quản trị viên cập nhật hoặc thu hồi quyền của người dùng trong cơ sở dữ liệu (`user_gateways`), hiệu lực sẽ được áp dụng ngay tại lần truy vấn cơ sở dữ liệu tiếp theo (next DB query) của HTTP request kế tiếp. Hệ thống không sử dụng cơ chế đẩy quyền trực tiếp (no live push / no WebSocket permission push).
4. **Tạo tài khoản không tự động cấp quyền (Account Creation Grants Nothing):** Việc tạo tài khoản người dùng mới trên Supabase Auth chỉ cấp danh tính (JWT hợp lệ), hoàn toàn không gán quyền cho bất kỳ trạm nào. Kết quả gọi `GET /v1/gateways` ban đầu luôn trả về `200 OK` với mảng rỗng `{"items": []}`.

---

## 2. Phân cấp Quan hệ Thực thể (Entity Relationship Hierarchy)

Cơ chế phân quyền dữ liệu của hệ thống được tổ chức theo cấu trúc phân cấp nghiêm ngặt từ trên xuống dưới trong cơ sở dữ liệu PostgreSQL:

```text
+-------------------------------------------------------------------+
|                     auth.users (Supabase Auth)                    |
|                      (Tài khoản người dùng)                       |
+---------------------------------+---------------------------------+
                                  | 1 : 1
                                  v
+-------------------------------------------------------------------+
|                        public.profiles                            |
|                 (Hồ sơ người dùng trong hệ thống)                 |
+---------------------------------+---------------------------------+
                                  | 1 : N
                                  v
+-------------------------------------------------------------------+
|                     public.user_gateways                          |
|             (Bảng quan hệ cấp quyền: user_id + gateway_id)        |
|               CHECK (role IN ('owner', 'operator', 'viewer'))     |
+---------------------------------+---------------------------------+
                                  | N : 1
                                  v
+-------------------------------------------------------------------+
|                        public.gateways                            |
|                         (Trạm Gateway)                            |
+---------------------------------+---------------------------------+
                                  | 1 : N
                                  v
+-------------------------------------------------------------------+
|                        public.sensors                             |
|                    (Các cảm biến thuộc trạm)                      |
+-------------------------------------------------------------------+
```

### Quy tắc kế thừa quyền của Cảm biến (Sensor Authorization):
- **Cảm biến kế thừa toàn bộ quyền từ Gateway cha (`gateway_id`):** Một người dùng chỉ có thể xem danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`) nếu người dùng đó có liên kết hợp lệ trong bảng `user_gateways` với Gateway chứa cảm biến đó.
- **Không có phân quyền cảm biến độc lập:** Hệ thống không hỗ trợ và không cho phép cấp quyền riêng lẻ trên từng Sensor (ví dụ: chỉ cho phép xem cảm biến A nhưng cấm cảm biến B trên cùng một Gateway). Nếu có quyền trên Gateway, người dùng có quyền đọc toàn bộ cảm biến của Gateway đó.
- **Không có route đọc cảm biến đơn lẻ (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}` không tồn tại):** Backend chỉ cung cấp route đọc tập hợp danh sách cảm biến của trạm `GET /v1/gateways/{gateway_id}/sensors`. Client tuyệt đối không tự bịa ra endpoint đọc chi tiết từng sensor.
- **Phòng thủ chống rò rỉ khi truy cập cảm biến lạ (Foreign Sensor / Foreign Gateway Defense):** Nếu client gửi request đọc danh sách cảm biến của một Gateway mà tài khoản không có quyền trong `user_gateways`, câu truy vấn SQL `JOIN user_gateways ug ... WHERE ug.user_id = $1 AND g.gateway_id = $2` sẽ không trả về dòng dữ liệu nào, và Go Backend trả về HTTP `404 Not Found` (thay vì 403) để bảo vệ hệ thống trước hành vi dò quét mã trạm (anti-enumeration).

---

## 3. Mô hình Phân quyền 3 Vai trò (Three Roles Model) & Thực tế API Giai đoạn 2 (Stage 2)

Trong cơ sở dữ liệu PostgreSQL, ràng buộc kiểm tra schema quy định rõ:
```sql
CHECK (role IN ('owner', 'operator', 'viewer'))
```

### 3.1. Thực tế API Giai đoạn 2 (Stage 2 Reality Check)

> **CẢNH BÁO QUAN TRỌNG VỀ PHẠM VI API HIỆN TẠI (STAGE 2):**
> Trong Giai đoạn 2 (Stage 2), Go Backend **CHỈ TRIỂN KHAI VÀ ĐĂNG KÝ DUY NHẤT 2 ENDPOINT ĐỌC DÀNH CHO NGƯỜI DÙNG CÓ XÁC THỰC**:
> 1. `GET /v1/gateways`: Lấy danh sách các trạm mà người dùng được gán quyền trong `user_gateways`.
> 2. `GET /v1/gateways/{gateway_id}/sensors`: Lấy danh sách cảm biến trực thuộc trạm mà người dùng có quyền.
>
> Tất cả các tài nguyên dữ liệu khác (Telemetry, Lịch sử số đo, WebSocket stream, Digital Twin) hiện chỉ là **stub trả về HTTP `501 Not Implemented`** hoặc **chưa đăng ký route** ở Stage 2:
> - `GET /v1/telemetry/history`: Stub trả về HTTP `501 Not Implemented` (`{"code": "not_implemented", "message": "endpoint not implemented"}`).
> - `GET /v1/ws`: Stub trả về HTTP `501 Not Implemented` (`{"code": "not_implemented", "message": "endpoint not implemented"}`).
> - `GET /v1/digital-twins`: Stub trả về HTTP `501 Not Implemented` (`{"code": "not_implemented", "message": "endpoint not implemented"}`).
> - Media upload / read: Chưa đăng ký HTTP route trong router ở Stage 2.
> - Desired state / Downlink commands: Chưa có endpoint ở Stage 2, hoàn toàn fail-closed.
>
> Do đó, trong Giai đoạn 2 hiện tại, **cả 3 vai trò `owner`, `operator`, và `viewer` ĐỀU CHỈ ĐỌC ĐƯỢC DANH SÁCH TRẠM VÀ DANH SÁCH CẢM BIẾN TỪ 2 ENDPOINT NÊU TRÊN**. Mọi mô tả về đọc telemetry, history, media hay digital twin trong tài liệu trước đây là mục tiêu kiến trúc dài hạn cho các giai đoạn sau, KHÔNG PHẢI là chức năng khả dụng ở Stage 2.

### 3.2. Bảng phân định quyền hạn thực tế và định hướng tương lai

| Vai trò (`role`) | Tên hiển thị (VN) | Quyền hạn dữ liệu thực tế (Stage 2) | Kế hoạch Giai đoạn sau (Downlink/Control/Twin/Telemetry) | Hiển thị Client (Role Badge) |
|---|---|---|---|---|
| `owner` | **Chủ sở hữu** | **Chỉ đọc** danh sách trạm (`GET /v1/gateways`) và danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`) của trạm được gán. | Cấu hình trạm (`desired_state`), quản lý metadata cảm biến, ra lệnh vận hành an toàn trong server allowlist. | `Badge(Color: Amber, Label: "Chủ sở hữu", Icon: admin_panel_settings)` |
| `operator` | **Người vận hành** | **Chỉ đọc** danh sách trạm (`GET /v1/gateways`) và danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`) của trạm được gán (fail-closed). | Được thực thi các lệnh vận hành an toàn trong server allowlist (ví dụ: `capture-image`). **Không** được sửa `desired_state`, **không** được sửa metadata cảm biến. | `Badge(Color: Blue, Label: "Vận hành", Icon: engineering)` |
| `viewer` | **Người xem** | **Chỉ đọc** danh sách trạm (`GET /v1/gateways`) và danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`) của trạm được gán. | Hoàn toàn chỉ đọc (Read-only) telemetry, history, media, digital twin khi các API này sẵn sàng. Vĩnh viễn **không** có quyền ghi, không điều khiển thiết bị, không sửa cấu hình. | `Badge(Color: Grey/Teal, Label: "Người xem", Icon: visibility)` |

### 3.3. Trải nghiệm phân quyền trên Web Dashboard (Desktop / Tablet / Responsive)
Khác với giao diện màn hình hẹp của thiết bị di động vốn chỉ hiển thị danh sách cuộn dọc đơn giản, nền tảng **Flutter Web Dashboard** sở hữu không gian hiển thị rộng rãi, cho phép tối ưu hóa trải nghiệm giám sát thông qua bố cục linh hoạt:

1. **Hiển thị Bảng Dữ liệu (Data Table) trên Desktop / Màn hình rộng:**
   - Khi độ rộng màn hình $\ge 900\,\text{px}$, danh sách Gateway được trình diễn dưới dạng **Data Table** chuyên nghiệp, gồm các cột thông tin rõ ràng:
     * **Mã trạm (`Gateway ID`):** Mã định danh duy nhất (font monospace, dễ tra cứu).
     * **Tên trạm quan trắc (`Name`):** Tên định danh gợi nhớ của trạm.
     * **Mô tả (`Description`):** Vị trí lắp đặt hoặc phần cứng gateway.
     * **Vai trò (`Role Badge`):** Huy hiệu trực quan với icon và màu sắc đặc trưng của từng vai trò (`owner`, `operator`, `viewer`).
     * **Ngày tạo / gán (`Created At`):** Mốc thời gian theo định dạng thân thiện.
     * **Thao tác (`Actions`):** Nút *"Xem chi tiết"* hoặc icon mở bảng điều khiển trạm.
   - **Tương tác chuột (Hover & Tooltip):** Khi rê chuột (hover) qua Role Badge, một Tooltip xuất hiện tức thì giải thích chi tiết ý nghĩa và quyền hạn của vai trò đó trên trạm (ví dụ: *"Vận hành: Được phép thực hiện các thao tác vận hành an toàn"*).
2. **Hiển thị Lưới thẻ (Responsive Card Grid) trên Tablet / Màn hình trung bình:**
   - Khi độ rộng màn hình trong khoảng $600\,\text{px} - 900\,\text{px}$, giao diện tự động chuyển đổi sang dạng **Lưới thẻ (Grid 2 cột)**. Mỗi thẻ Gateway hiển thị tên, mã trạm, Role Badge nổi bật ở góc trên bên phải, cùng tóm tắt số lượng cảm biến và trạng thái.
3. **Danh sách thẻ (List View) trên Mobile Web:**
   - Khi độ rộng màn hình $< 600\,\text{px}$, hệ thống hiển thị dạng danh sách thẻ đơn 1 cột, đảm bảo đầy đủ thông tin mà không bị vỡ bố cục hay tràn khung (overflow).

### 3.4. Chính sách bảo mật Giai đoạn 2 (Stage 2 Boundary & Fail-closed Policy)
1. **Chế độ Fail-closed:** Trong Giai đoạn 2 hiện tại, Go Backend áp dụng nguyên tắc fail-closed toàn diện đối với mọi thao tác ghi hoặc lệnh điều khiển từ phía người dùng thông thường. Cả `owner`, `operator` và `viewer` đều hoạt động ở chế độ đọc an toàn trên 2 route khả dụng.
2. **UI Hiding KHÔNG PHẢI là cơ chế bảo mật (UI Hiding is Not Security):**
   - Việc ẩn nút, ẩn form, hoặc làm mờ giao diện trên Client Web chỉ là kỹ thuật nâng cao trải nghiệm người dùng (UX) nhằm tránh gây bối rối cho người dùng đối với các tính năng chưa hỗ trợ.
   - **Tuyệt đối không coi UI hiding là ranh giới bảo mật:** Client có thể bị can thiệp bởi DevTools hoặc script độc hại. Go Backend luôn là chốt chặn cuối cùng, kiểm tra quyền động trong cơ sở dữ liệu trên từng request và từ chối ngay lập tức nếu request không hợp lệ.
   - Mọi đoạn mã giao diện mẫu (Dart/Flutter) trong tài liệu này là các đoạn mã minh họa kiến trúc (illustrative snippets).

---

## 4. Phân định rạch ròi: Quyền Quản trị Nền tảng (Platform Admin) vs. Quyền Thành viên Trạm (Gateway Roles)

Một trong những nhầm lẫn phổ biến là đồng nhất quyền Quản trị viên hệ thống với quyền Chủ sở hữu trạm. Kiến trúc hệ thống phân định dứt khoát hai cơ chế phân quyền độc lập:

```text
+----------------------------------------------------+----------------------------------------------------+
|        Platform Administrator (Toàn cục)           |              Gateway Role (Từng trạm)              |
+----------------------------------------------------+----------------------------------------------------+
| - Xác định qua bảng PostgreSQL `platform_admins`   | - Xác định qua bảng PostgreSQL `user_gateways`     |
| - Thẩm định bởi `auth.PlatformAdminMiddleware`     | - Thẩm định qua SQL JOIN động tại từng truy vấn    |
| - Quyền quản trị toàn hệ thống (Gateway/Sensors)   | - Gắn liền duy nhất với một `gateway_id` cụ thể    |
| - Cấp phát & quay vòng MQTT credentials cho trạm   | - Là một trong 3 vai trò: `owner`, `operator`,     |
| - Gọi các HTTP Admin Routes thực tế (`/v1/admin/*`)|   hoặc `viewer`                                    |
| - Thao tác qua Supabase Studio hoặc Admin Tooling  | - Thao tác qua ứng dụng Flutter Web Client         |
+----------------------------------------------------+----------------------------------------------------+
```

### 4.1. Các HTTP Route Quản trị Thực tế Hiện Có (Real HTTP Admin Routes)
Khác với các tài nguyên nghiệp vụ thông thường, Go Backend đã triển khai và đăng ký các route quản trị thực tế dưới tiền tố `/v1/admin` được bảo vệ độc quyền bởi `PlatformAdminMiddleware`:

1. **Khởi tạo hoặc Replay Đồng nhất Trạm Gateway (Create or Identical Replay):**
   `PUT /v1/admin/gateways/{gateway_id}`
   Endpoint này áp dụng cơ chế idempotent create-or-identical-replay, **không phải là API cập nhật (update) Gateway đã có**:
   - **Tạo mới trạm (HTTP `201 Created`):** Khi mã `gateway_id` chưa tồn tại trong cơ sở dữ liệu, backend tạo mới Gateway, khởi tạo Digital Twin entity tương ứng, và gán tài khoản chủ sở hữu ban đầu vào `user_gateways` với vai trò `owner` theo trường `owner_user_id` trong body.
   - **Replay đồng nhất (HTTP `200 OK`):** Khi `gateway_id` đã tồn tại và payload gửi lên hoàn toàn trùng khớp với biểu diễn hiện hữu (cùng `name`, `description`, và cùng `owner_user_id`), backend ghi nhận đây là lượt gọi lặp lại an toàn (no-op identical replay) và trả về `200 OK`.
   - **Xung đột biểu diễn (HTTP `409 Conflict`):** Khi `gateway_id` đã tồn tại nhưng payload có bất kỳ thông tin nào sai khác với dữ liệu hiện có (ví dụ khác tên, khác mô tả hoặc khác chủ sở hữu), backend từ chối với mã lỗi `409 Conflict` (`{"code": "conflict", "message": "resource conflict"}`).
2. **Khởi tạo hoặc Replay Đồng nhất Cảm biến trực thuộc Trạm (Create or Identical Replay):**
   `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`
   Tương tự như trạm, endpoint này thực hiện khởi tạo hoặc replay đồng nhất cho cảm biến:
   - **Tạo mới cảm biến (HTTP `201 Created`):** Khi `sensor_id` chưa tồn tại dưới Gateway cha, backend tạo mới bản ghi sensor và Digital Twin entity.
   - **Replay đồng nhất (HTTP `200 OK`):** Nếu `sensor_id` đã tồn tại với các trường thông tin hoàn toàn trùng khớp (`name`, `unit`), backend trả về `200 OK`.
   - **Xung đột biểu diễn (HTTP `409 Conflict`):** Nếu `sensor_id` đã tồn tại nhưng payload chứa dữ liệu khác biệt so với hiện trạng, backend trả về `409 Conflict`.
3. **Bộ 4 Endpoint Quản lý Chứng chỉ MQTT (MQTT Credentials):**
   - `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`: Truy vấn metadata chứng chỉ MQTT của trạm (không bao giờ lộ mật khẩu thô).
   - `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`: Cấp phát ban đầu thông tin xác thực MQTT cho trạm.
   - `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`: Thu hồi chứng chỉ cũ và cấp phát thông tin xác thực MQTT mới.
   - `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`: Thu hồi chứng chỉ MQTT của trạm.

*(Chi tiết đầy đủ về schema, headers và mã lỗi của các endpoint quản trị được quy định tại `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`)*.

### 4.2. Các Nguyên tắc Phân quyền Cốt tử giữa Admin và User

1. **Phân định Vai trò Thành viên Trạm và Quyền Quản trị Độc lập (Membership Roles Do Not Deny Independent Admin Status):**
   - Ràng buộc phân quyền trạm (`user_gateways`) và quyền quản trị nền tảng (`platform_admins`) là hai tập thẩm quyền độc lập. Việc nắm giữ một vai trò thành viên trạm (kể cả `owner`) **không tự động cấp quyền quản trị**, nhưng cũng **không tước bỏ quyền quản trị độc lập** nếu người dùng đó đồng thời được cấp quyền trong `platform_admins` (dual-role user).
   - **Trường hợp non-admin owner:** Nếu một người dùng nắm vai trò `owner` của một Gateway nhưng tài khoản không có tên trong bảng `platform_admins`, khi cố tình gửi request đến các route quản trị `/v1/admin/*`, `PlatformAdminMiddleware` sẽ tra cứu cơ sở dữ liệu và từ chối tức thì với mã lỗi **HTTP `403 Forbidden`** (`{"code": "forbidden", "message": "insufficient permissions"}`).
   - **Trường hợp dual-role:** Một người dùng vừa là Platform Admin vừa là `owner` của một trạm có thể thực thi các lệnh `/v1/admin/*` nhờ tư cách admin. Tuy nhiên, khi gọi các API đọc thông thường của người dùng (`GET /v1/gateways`), tài khoản đó vẫn tuân thủ nguyên tắc không bỏ qua phân quyền đọc (No Admin Read Bypass).
2. **KHÔNG CÓ cơ chế Admin Read Bypass (No Admin Read Bypass):**
   - Khi một Platform Admin đăng nhập vào ứng dụng Web Client và gọi các API đọc thông thường của người dùng (`GET /v1/gateways` hoặc `GET /v1/gateways/{gateway_id}/sensors`), Go Backend **tuyệt đối không** tự động bỏ qua điều kiện lọc quyền.
   - Câu truy vấn SQL của Backend vẫn lọc nghiêm ngặt theo `WHERE ug.user_id = $1`. Nếu Platform Admin không được gán quyền trực tiếp trong bảng `user_gateways` cho một trạm, Admin cũng chỉ nhận về danh sách rỗng `{"items": []}` hoặc lỗi `404 Not Found`.
   - **Mục đích kiến trúc:** Ngăn ngừa việc vô tình làm rò rỉ dữ liệu hoặc làm tràn ngập màn hình giám sát của Admin bằng hàng trăm trạm không liên quan đến phạm vi kiểm thử hay giám sát trực tiếp của tài khoản đó.
3. **Công cụ Quản trị Thành viên Chuyên dụng với Cơ chế Bảo vệ Chủ sở hữu (Protected Membership Tool with Owner Protection, No Public API):**
   - Ngoại trừ việc thiết lập `owner_user_id` ban đầu khi tạo trạm qua `PUT /v1/admin/gateways/{gateway_id}`, việc thêm thành viên mới, chuyển đổi vai trò hoặc thu hồi quyền trong `user_gateways` được thực hiện thông qua **công cụ quản trị thành viên chuyên dụng có ghi nhận nhật ký kiểm toán (như script vận hành quản trị viên `scripts/manage-gateway-membership.sh` hoặc Supabase Studio bởi Platform Admin)**, **tuyệt đối không hướng dẫn chỉnh sửa SQL tùy tiện (no arbitrary SQL guidance)**.
   - **Cơ chế bảo vệ chủ sở hữu (Owner Protection):** Công cụ quản lý thành viên hỗ trợ các hành động `grant`, `change` và `revoke` dành cho hai vai trò `viewer` và `operator`, đồng thời ngăn chặn nghiêm ngặt việc thu hồi vai trò `owner` hoặc tự ý chỉ định thêm `owner` thông qua luồng chia sẻ quyền thông thường.
   - Hệ thống **không cung cấp public API** cho người dùng tự gán trạm (no self-claiming), tự mời người khác (no invitation API) hay tự nâng quyền.
4. **Tạo Tài khoản Mới KHÔNG CẤP BẤT KỲ QUYỀN NÀO (Account Creation Grants Nothing):**
   - Việc tạo tài khoản trên Supabase Auth chỉ cung cấp định danh hợp lệ (JWT). Người dùng mới tạo chưa được gán bất kỳ trạm nào trong `user_gateways`.
   - Khi gọi `GET /v1/gateways`, Backend trả về `200 OK` với mảng rỗng `{"items": []}`.
5. **Cập nhật Quyền trên Cùng một JWT (Same JWT, Next DB Query, No Live Push):**
   - Khi Quản trị viên cập nhật hoặc thu hồi quyền của người dùng trong `user_gateways`, người dùng tiếp tục sử dụng JWT hiện tại mà không cần đăng nhập lại.
   - Quyền mới sẽ có hiệu lực ngay ở lần truy vấn cơ sở dữ liệu tiếp theo (**next DB query**) khi client gửi HTTP request. Hệ thống không sử dụng cơ chế đẩy quyền trực tiếp qua WebSocket hay live push.
6. **Tuyệt đối KHÔNG SÁNG TÁC các Route không tồn tại (No Invented Routes):**
   - **KHÔNG CÓ** route đọc chi tiết 1 Gateway: `GET /v1/gateways/{gateway_id}` (không tồn tại).
   - **KHÔNG CÓ** route đọc chi tiết 1 Sensor: `GET /v1/gateways/{gateway_id}/sensors/{sensor_id}` (không tồn tại).
   - **KHÔNG CÓ** route đọc thông tin Admin bản thân: `GET /v1/admin/self` (không tồn tại).
   - Mọi thông tin Gateway được lấy qua `GET /v1/gateways`, và danh sách cảm biến được lấy qua `GET /v1/gateways/{gateway_id}/sensors`.

---

## 5. Bảng Mã Trạng thái HTTP Thực tế (Actual HTTP Status Codes) & Ứng xử Giao diện Client

Bảng dưới đây quy định chính xác các mã trạng thái HTTP thực tế được sinh ra từ Go Backend và hành vi ứng xử tương ứng của ứng dụng Flutter Web Client:

| Mã HTTP | Mã lỗi (`code`) | Thông báo (`message`) | Nguyên nhân phát sinh thực tế tại Backend | Ứng xử chuẩn của Client Web |
|---|---|---|---|---|
| `200 OK` | *(không có)* | *(payload JSON)* | Gọi `GET /v1/gateways` hoặc `GET /v1/gateways/{gateway_id}/sensors` thành công. Khi `items: []`, đây là trạng thái rỗng hợp lệ (Empty State). | Hiển thị danh sách dữ liệu hoặc giao diện rỗng thân thiện (`EmptyGatewayView`). Không hiển thị lỗi. |
| `400 Bad Request` | `invalid_request` | `invalid request` | Request chứa query param bị cấm `?user_id=...` (chống IDOR) hoặc `gateway_id` sai định dạng kiểm tra. | Bắt lỗi an toàn; loại bỏ query param cấm; hiển thị cảnh báo tham số không hợp lệ. |
| `401 Unauthorized` | `unauthorized` | `authentication required` | Thiếu header `Authorization`, token hết hạn, sai chữ ký JWT hoặc token không đúng định dạng. | Xóa session token trong bộ nhớ, điều hướng người dùng về màn hình đăng nhập (`/login`). |
| `403 Forbidden` | `forbidden` | `insufficient permissions` hoặc `access denied` | Tài khoản không có trong `platform_admins` cố tình gọi các route quản trị `/v1/admin/*`, hoặc vi phạm quyền provisioning. | Hiển thị thông báo không có thẩm quyền quản trị; chặn hoàn toàn quyền truy cập giao diện admin. |
| `404 Not Found` | `not_found` | `resource not found` | Người dùng gọi `GET /v1/gateways/{gateway_id}/sensors` đối với trạm không thuộc quyền trong `user_gateways` (foreign gateway defense), trạm không tồn tại, hoặc route sai. | Giữ nguyên URL trên thanh địa chỉ, hiển thị trang lỗi `GatewayNotFoundView` kèm nút "Quay lại danh sách trạm". |
| `501 Not Implemented` | `not_implemented` | `endpoint not implemented` | Client gọi các endpoint stub Giai đoạn 2 (`/v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins`). | Bắt lỗi an toàn; thông báo chức năng đang trong lộ trình phát triển của các giai đoạn sau. |
| `503 Service Unavailable` | `service_unavailable` | `service unavailable` | Kết nối PostgreSQL lỗi, timeout hoặc context cancelled khi kiểm tra quyền/admin, hoặc credential controller chưa sẵn sàng. | Hiển thị thông báo hệ thống tạm thời gián đoạn kèm nút "Thử lại sau" (Retry với exponential backoff). |

*(Tham khảo thêm chi tiết về cơ chế quản lý trạng thái giao diện và bắt lỗi mạng tại `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`)*.

### 5.1. Hành vi UI đối với Tài khoản Mới (Empty State) trên Web
Theo chính sách quản lý tài khoản tập trung (`AGENTS.md` Mục 6.5 và `02_AUTHENTICATION_AND_SESSION.md`), người dùng không thể tự đăng ký tài khoản. Tài khoản do Quản trị viên tạo sẵn.

#### Kịch bản Người dùng chưa được gán Gateway:
1. Quản trị viên tạo tài khoản cho người dùng nhưng chưa kịp thêm bản ghi vào `user_gateways`.
2. Người dùng đăng nhập thành công vào Web Dashboard (nhận JWT hợp lệ).
3. Flutter Web gọi `GET /v1/gateways`.
4. Go Backend truy vấn cơ sở dữ liệu, không thấy bản ghi nào trong `user_gateways`, trả về:
   - **Mã HTTP:** `200 OK`
   - **Payload:**
     ```json
     {
       "items": []
     }
     ```

#### Hành vi giao diện Web Client (Empty State UX):
- **Không coi đây là lỗi:** Đây là trạng thái nghiệp vụ hoàn toàn bình thường, không hiển thị thông báo lỗi (Error Toast / Alert Dialog).
- **Hiển thị giao diện rỗng thân thiện (Empty State Widget) dạng Card nổi giữa màn hình Web:**
  - Icon minh họa trung tâm: Trạm quan trắc chưa kết nối (`Icons.router_outlined` hoặc `Icons.sensors_off_outlined`).
  - Tiêu đề: *"Chưa có trạm Gateway nào"*
  - Nội dung hướng dẫn: *"Tài khoản của bạn hiện chưa được cấp quyền truy cập trạm quan trắc nào trong hệ thống. Vui lòng liên hệ Quản trị viên (Platform Administrator) để được phân quyền."*
  - Nút hành động: Nút *"Làm mới danh sách"* (`Refresh`) để tải lại danh sách sau khi Quản trị viên hoàn tất việc gán quyền, và nút *"Đăng xuất"* nếu người dùng đăng nhập nhầm tài khoản.

---

## 6. Xử lý Deep Linking, URL Manipulation, F5 Reload & Thu hồi Quyền trên Web

Trên nền tảng Web, hành vi người dùng rất khác biệt so với ứng dụng di động: người dùng có thanh địa chỉ URL (Address Bar), phím F5 / Ctrl+R để tải lại trang, và các nút Back/Forward của trình duyệt. Ứng dụng Web phải xử lý các tình huống này một cách an toàn và nhất quán.

### 6.1. Xử lý Deep Linking và URL Manipulation trên trình duyệt Web
Người dùng có thể gõ trực tiếp một đường dẫn chi tiết trạm vào thanh địa chỉ của trình duyệt, ví dụ:
```text
https://iot.example.com/gateways/gw_999
```

**Kịch bản Người dùng cố tình truy cập Gateway KHÔNG có quyền:**
1. Giả sử người dùng chỉ được cấp quyền xem `gw_001`, nhưng tự ý gõ trên trình duyệt URL `.../gateways/gw_999`.
2. Web Client trích xuất `gatewayId = gw_999` từ path parameter và gửi request gọi dữ liệu cảm biến lên Go Backend:
   ```http
   GET /v1/gateways/gw_999/sensors HTTP/1.1
   Host: iot.example.com
   Authorization: Bearer <valid_jwt_of_user>
   Accept: application/json
   ```
3. Go Backend thực thi câu truy vấn SQL scoped bảo mật:
   ```sql
   SELECT s.sensor_id, s.name, s.unit, s.created_at
   FROM sensors s
   JOIN user_gateways ug ON ug.gateway_id = s.gateway_id
   WHERE ug.user_id = $1 AND ug.gateway_id = $2
   ```
4. Do `ug.user_id` không có liên kết với `gw_999` trong bảng `user_gateways`, Backend trả về:
   - **Mã HTTP:** `404 Not Found`
   - **Payload lỗi:**
     ```json
     {
       "code": "not_found",
       "message": "resource not found"
     }
     ```

#### Cơ chế Phòng thủ Chống dò quét tài nguyên (Anti-Enumeration Defense):
- **Tại sao trả 404 Not Found thay vì 403 Forbidden?**
  Đây là quy tắc bảo mật chuẩn (theo RFC 7231 và khuyến nghị OWASP API Security). Việc trả về `404 Not Found` ngăn chặn kẻ tấn công dò quét (enumerate) xem mã trạm `gw_999` có thực sự tồn tại trong cơ sở dữ liệu hay không. Đối với một người dùng không có quyền truy cập, tài nguyên đó được đối xử như **hoàn toàn không tồn tại**.

#### Ứng xử giao diện Web Client khi gặp lỗi 404:
- **Giữ nguyên URL trên thanh địa chỉ:** Không vội vàng chuyển hướng (redirect) giật cục làm mất dấu vết địa chỉ người dùng vừa gõ.
- **Hiển thị trang lỗi 404 chuyên biệt (Gateway Not Found View):**
  * Icon cảnh báo: `Icons.search_off_outlined` hoặc `Icons.lock_person_outlined`.
  * Tiêu đề: *"404 — Không tìm thấy trạm quan trắc"*
  * Nội dung: *"Trạm quan trắc `gw_999` không tồn tại hoặc tài khoản của bạn chưa được cấp quyền truy cập."*
  * Nút hành động chính: *"Quay lại danh sách trạm"* — khi click, router chuyển hướng về `/gateways` (`context.go('/gateways')`).

```text
[Người dùng gõ: /gateways/gw_999]
                |
                v
       Client Web gửi API:
   GET /v1/gateways/gw_999/sensors
                |
                v
   Go Backend kiểm tra user_gateways -> Không có quyền!
                |
                v
       Trả về HTTP 404 Not Found
                |
                v
       Client bắt mã lỗi 404:
  +-----------------------------------------------+
  |  Trang lỗi 404 trên Web (/gateways/gw_999)     |
  |  "404 - Không tìm thấy trạm hoặc bạn          |
  |   không có quyền truy cập."                   |
  |                                               |
  |     [ Quay lại danh sách trạm (/gateways) ]   |
  +-----------------------------------------------+
```

---

### 6.2. Xử lý sự kiện Tải lại trang (F5 / Reload) tại Trang Chi tiết
Khi người dùng đang mở trang chi tiết một trạm (`https://iot.example.com/gateways/gw_001`) và nhấn **F5** hoặc **Ctrl+R**:
1. Toàn bộ ứng dụng Flutter Web được nạp lại từ đầu. Bộ nhớ RAM của ứng dụng bị giải phóng, các biến state trong Cubit/Controller bị xóa sạch.
2. `go_router` đọc URL từ trình duyệt và phân giải route `/gateways/:id`, trích xuất `gatewayId = "gw_001"`.
3. **Đặc thù kiến trúc API Backend:**
   Trong thiết kế Giai đoạn 2 của Go Backend, hệ thống **không cung cấp** endpoint riêng lẻ `GET /v1/gateways/{id}` mà chỉ cung cấp 2 endpoint:
   - `GET /v1/gateways`: Trả về danh sách tất cả các trạm mà user hiện tại có quyền (chứa metadata: `name`, `description`, `role`, `created_at`).
   - `GET /v1/gateways/{id}/sensors`: Trả về danh sách các cảm biến trực thuộc trạm đó.

#### Chiến lược nạp dữ liệu khôi phục trạng thái (Hydration Strategy khi F5):
Để tái tạo đầy đủ giao diện chi tiết trạm (bao gồm cả Tên trạm, Role Badge và Danh sách cảm biến), Web Client thực hiện nạp dữ liệu song song hoặc phối hợp:
1. **Bước 1 — Nạp Metadata và Xác thực Quyền qua `GET /v1/gateways`:**
   - Client gọi `GET /v1/gateways`.
   - Tìm kiếm bản ghi có `gateway_id == state.pathParameters['id']` trong mảng `items`.
   - **Nếu KHÔNG tìm thấy:** Chứng tỏ trạm này không tồn tại hoặc tài khoản không có quyền -> Chuyển sang hiển thị **Trang lỗi 404**.
   - **Nếu tìm thấy:** Lưu giữ thông tin trạm và vai trò (`role`) của người dùng để gắn Role Badge lên tiêu đề.
2. **Bước 2 — Nạp Danh sách Cảm biến qua `GET /v1/gateways/{id}/sensors`:**
   - Client gọi `GET /v1/gateways/{id}/sensors`.
   - Nếu trả về `200 OK`: Hiển thị danh sách cảm biến trong bảng dữ liệu hoặc lưới thẻ.
   - Nếu trả về `404 Not Found`: Chuyển sang hiển thị **Trang lỗi 404**.

```text
[Người dùng F5 tại /gateways/gw_001]
                 |
                 v
   go_router trích xuất: gatewayId = gw_001
                 |
                 +---------------------------------------+
                 | Gọi song song 2 API:                  |
                 | 1. GET /v1/gateways                   |
                 | 2. GET /v1/gateways/gw_001/sensors    |
                 +-------------------+-------------------+
                                     |
                    +----------------+----------------+
                    |                                 |
            gw_001 có trong                   gw_001 KHÔNG có
           GET /v1/gateways?                 trong danh sách?
                    |                                 |
                  (Có)                             (Không)
                    v                                 v
        Nạp thành công metadata:             Hiển thị Trang lỗi 404:
        Tên trạm + Role Badge + Sensors      "Không tìm thấy trạm hoặc
        (Màn hình hiển thị đầy đủ)            bạn không có quyền truy cập"
```

---

### 6.3. Xử lý Thu hồi Quyền khi đang mở trang chi tiết (Revocation on Next DB Query)
Nếu người dùng đang mở xem chi tiết trạm `gw_001`, và Quản trị viên thực hiện xóa hoặc thu hồi quyền của người dùng đó trong cơ sở dữ liệu (`DELETE FROM user_gateways ...`):
1. **Hiệu lực tức thì tại truy vấn tiếp theo (No Live Push):** Hệ thống không duy trì kết nối WebSocket đẩy sự kiện thu hồi quyền, nhưng ở ngay lần thao tác tiếp theo (ví dụ: bấm nút làm mới danh sách cảm biến, hoặc bộ đếm thời gian tự động đồng bộ):
   - Client gửi `GET /v1/gateways/gw_001/sensors` kèm cùng một JWT hiện tại.
   - Go Backend thực thi câu truy vấn SQL trực tiếp trên PostgreSQL, không thấy bản ghi trong `user_gateways` -> trả về ngay **HTTP `404 Not Found`** (`{"code": "not_found", "message": "resource not found"}`).
2. **Client bắt mã lỗi 404:**
   - Hiển thị ngay thông báo lỗi (Error Banner / Dialog) cảnh báo quyền truy cập đã bị thu hồi hoặc trạm không còn khả dụng.
   - Chuyển giao diện sang **Gateway Not Found View** kèm nút điều hướng người dùng quay lại `/gateways`.

---

## 7. Hướng dẫn Triển khai Mã nguồn Dart/Flutter Tham khảo (Illustrative Snippets)

> **LƯU Ý THIẾT KẾ:**
> Các đoạn mã nguồn Dart và Flutter dưới đây đóng vai trò là **mã minh họa kiến trúc (illustrative snippets)** nhằm trình bày cách tổ chức component, phân tầng xử lý và định tuyến trên môi trường Flutter Web. Các đoạn mã này phục vụ việc đối chiếu với hợp đồng API thực tế và không thay thế cho mã nguồn sản phẩm chính thức.

Dưới đây là các thành phần mã nguồn mẫu, được chuẩn hóa theo phong cách idiomatic Dart và Flutter Web, sẵn sàng tích hợp vào dự án.

### 7.1. Định nghĩa Role Enum và Helper (`gateway_role.dart`)

```dart
import 'package:flutter/material.dart';

/// Đại diện cho 3 vai trò người dùng trên từng Gateway theo schema PostgreSQL:
/// CHECK (role IN ('owner', 'operator', 'viewer'))
enum GatewayRole {
  owner('owner'),
  operator('operator'),
  viewer('viewer');

  final String value;
  const GatewayRole(this.value);

  /// Chuyển đổi chuỗi từ backend sang Enum an toàn
  static GatewayRole fromString(String? roleStr) {
    switch (roleStr?.toLowerCase().trim()) {
      case 'owner':
        return GatewayRole.owner;
      case 'operator':
        return GatewayRole.operator;
      case 'viewer':
      default:
        return GatewayRole.viewer;
    }
  }

  /// Tên tiếng Việt hiển thị trên giao diện Web Dashboard
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

  /// Mô tả ngắn về quyền hạn hiển thị trong Tooltip trên Web
  String get description {
    switch (this) {
      case GatewayRole.owner:
        return 'Toàn quyền cấu hình và giám sát trạm quan trắc';
      case GatewayRole.operator:
        return 'Được phép thực hiện các thao tác vận hành an toàn';
      case GatewayRole.viewer:
        return 'Chỉ xem dữ liệu đo đạc và thông tin trạm (Read-only)';
    }
  }

  /// Màu sắc đại diện cho Badge
  Color get badgeColor {
    switch (this) {
      case GatewayRole.owner:
        return Colors.amber.shade900;
      case GatewayRole.operator:
        return Colors.blue.shade800;
      case GatewayRole.viewer:
        return Colors.blueGrey.shade700;
    }
  }

  /// Màu nền nhẹ của Badge
  Color get badgeBackgroundColor {
    switch (this) {
      case GatewayRole.owner:
        return Colors.amber.shade50;
      case GatewayRole.operator:
        return Colors.blue.shade50;
      case GatewayRole.viewer:
        return Colors.blueGrey.shade50;
    }
  }

  /// Icon minh họa cho từng vai trò
  IconData get icon {
    switch (this) {
      case GatewayRole.owner:
        return Icons.admin_panel_settings_outlined;
      case GatewayRole.operator:
        return Icons.engineering_outlined;
      case GatewayRole.viewer:
        return Icons.visibility_outlined;
    }
  }

  /// Kiểm tra vai trò cấu hình thiết bị (Giai đoạn sau)
  bool get canConfigureDevice => this == GatewayRole.owner;

  /// Kiểm tra vai trò gửi lệnh vận hành an toàn (Giai đoạn sau)
  bool get canExecuteOperations => this == GatewayRole.owner || this == GatewayRole.operator;
}
```

---

### 7.2. Widget Hiển thị Huy hiệu Vai trò (`gateway_role_badge.dart`)
Tối ưu hóa cho Web Dashboard với hai chế độ: `compact` cho ô bảng Data Table và `full` cho tiêu đề trang chi tiết.

```dart
import 'package:flutter/material.dart';
import 'gateway_role.dart';

class GatewayRoleBadge extends StatelessWidget {
  final GatewayRole role;
  final bool compact;

  const GatewayRoleBadge({
    Key? key,
    required this.role,
    this.compact = false,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final badgeWidget = Container(
      padding: EdgeInsets.symmetric(
        horizontal: compact ? 8 : 12,
        vertical: compact ? 3 : 6,
      ),
      decoration: BoxDecoration(
        color: role.badgeBackgroundColor,
        borderRadius: BorderRadius.circular(compact ? 4 : 16),
        border: Border.all(
          color: role.badgeColor.withOpacity(0.4),
          width: 1.0,
        ),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            role.icon,
            size: compact ? 13 : 16,
            color: role.badgeColor,
          ),
          const SizedBox(width: 5),
          Text(
            role.displayName,
            style: TextStyle(
              fontSize: compact ? 12 : 13,
              fontWeight: FontWeight.w600,
              color: role.badgeColor,
            ),
          ),
        ],
      ),
    );

    // Trên Web, luôn bọc Tooltip để người dùng hover chuột là xem được giải thích quyền
    return Tooltip(
      message: '${role.displayName}: ${role.description}',
      waitDuration: const Duration(milliseconds: 300),
      child: badgeWidget,
    );
  }
}
```

---

### 7.3. Widget Bảng Dữ liệu Phân quyền trên Web (`web_gateway_table.dart`)
Hiển thị danh sách Gateway trực quan dạng Data Table cho màn hình rộng (Desktop / Tablet ngang).

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'gateway_role.dart';
import 'gateway_role_badge.dart';

class WebGatewayTable extends StatelessWidget {
  final List<dynamic> gateways;

  const WebGatewayTable({
    Key? key,
    required this.gateways,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 0,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(8),
        side: BorderSide(color: Colors.grey.shade200),
      ),
      child: ClipRRect(
        borderRadius: BorderRadius.circular(8),
        child: DataTable(
          headingRowColor: MaterialStateProperty.all(Colors.grey.shade50),
          headingTextStyle: const TextStyle(
            fontWeight: FontWeight.bold,
            color: Colors.black87,
          ),
          columns: const [
            DataColumn(label: Text('Mã trạm (ID)')),
            DataColumn(label: Text('Tên trạm quan trắc')),
            DataColumn(label: Text('Vai trò của bạn')),
            DataColumn(label: Text('Ngày tạo')),
            DataColumn(label: Text('Thao tác')),
          ],
          rows: gateways.map((gw) {
            final gatewayId = gw['gateway_id'] as String;
            final name = gw['name'] as String? ?? 'Chưa đặt tên';
            final role = GatewayRole.fromString(gw['role'] as String?);
            final createdAt = gw['created_at'] as String? ?? '';

            return DataRow(
              cells: [
                DataCell(
                  Text(
                    gatewayId,
                    style: const TextStyle(
                      fontFamily: 'monospace',
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                DataCell(Text(name)),
                DataCell(GatewayRoleBadge(role: role, compact: true)),
                DataCell(Text(createdAt.split('T').first)),
                DataCell(
                  OutlinedButton.icon(
                    icon: const Icon(Icons.arrow_forward, size: 14),
                    label: const Text('Xem chi tiết'),
                    style: OutlinedButton.styleFrom(
                      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                    ),
                    onPressed: () {
                      // Điều hướng chuẩn Web qua go_router
                      context.go('/gateways/$gatewayId');
                    },
                  ),
                ),
              ],
            );
          }).toList(),
        ),
      ),
    );
  }
}
```

---

### 7.4. Widget Trang lỗi 404 Không tìm thấy hoặc Không có quyền (`gateway_not_found_view.dart`)

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';

class GatewayNotFoundView extends StatelessWidget {
  final String gatewayId;

  const GatewayNotFoundView({
    Key? key,
    required this.gatewayId,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Center(
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(32.0),
        child: Container(
          constraints: const BoxConstraints(maxWidth: 520),
          padding: const EdgeInsets.all(32.0),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: Colors.grey.shade200),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withOpacity(0.04),
                blurRadius: 16,
                offset: const Offset(0, 4),
              ),
            ],
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Container(
                padding: const EdgeInsets.all(20),
                decoration: BoxDecoration(
                  color: Colors.orange.shade50,
                  shape: BoxShape.circle,
                ),
                child: Icon(
                  Icons.search_off_outlined,
                  size: 64,
                  color: Colors.orange.shade800,
                ),
              ),
              const SizedBox(height: 24),
              const Text(
                '404 — Không tìm thấy trạm quan trắc',
                style: TextStyle(
                  fontSize: 20,
                  fontWeight: FontWeight.bold,
                  color: Colors.black87,
                ),
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 12),
              RichText(
                textAlign: TextAlign.center,
                text: TextSpan(
                  style: TextStyle(
                    fontSize: 14,
                    color: Colors.grey.shade700,
                    height: 1.5,
                  ),
                  children: [
                    const TextSpan(text: 'Trạm quan trắc '),
                    TextSpan(
                      text: gatewayId,
                      style: const TextStyle(
                        fontFamily: 'monospace',
                        fontWeight: FontWeight.bold,
                        color: Colors.black87,
                      ),
                    ),
                    const TextSpan(
                      text:
                          ' không tồn tại trên hệ thống hoặc tài khoản của bạn chưa được cấp quyền truy cập.\n\nNếu bạn cho rằng đây là sự nhầm lẫn, vui lòng liên hệ Quản trị viên (Platform Administrator) để được phân quyền.',
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 32),
              ElevatedButton.icon(
                onPressed: () {
                  // Điều hướng an toàn quay lại danh sách trạm trên Web
                  context.go('/gateways');
                },
                icon: const Icon(Icons.arrow_back),
                label: const Text('Quay lại danh sách trạm'),
                style: ElevatedButton.styleFrom(
                  padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 14),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(8),
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
```

---

### 7.5. Controller Chi tiết Trạm xử lý F5 và Deep Linking (`gateway_detail_controller.dart`)
Controller phối hợp nạp dữ liệu từ hai endpoint `GET /v1/gateways` và `GET /v1/gateways/{id}/sensors` để tái tạo trạng thái khi người dùng nhấn F5 hoặc truy cập trực tiếp bằng URL.

```dart
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'gateway_role.dart';

enum GatewayDetailStatus { initial, loading, loaded, notFound, error }

class GatewayDetailData {
  final String gatewayId;
  final String name;
  final String description;
  final GatewayRole role;
  final List<dynamic> sensors;

  GatewayDetailData({
    required this.gatewayId,
    required this.name,
    required this.description,
    required this.role,
    required this.sensors,
  });
}

class GatewayDetailController extends ChangeNotifier {
  final String gatewayId;
  final String accessToken;
  final String apiBaseUrl;

  GatewayDetailStatus status = GatewayDetailStatus.initial;
  GatewayDetailData? data;
  String? errorMessage;

  GatewayDetailController({
    required this.gatewayId,
    required this.accessToken,
    required this.apiBaseUrl,
  });

  /// Nạp thông tin trạm và cảm biến (hỗ trợ cả Deep Link và F5 reload)
  Future<void> loadDetails() async {
    status = GatewayDetailStatus.loading;
    errorMessage = null;
    notifyListeners();

    try {
      final headers = {
        'Authorization': 'Bearer $accessToken',
        'Accept': 'application/json',
      };

      // 1. Gọi song song: Lấy danh sách Gateways để lấy Metadata & Role,
      //    và lấy danh sách Sensors của Gateway cụ thể này.
      final gatewaysFuture = http.get(
        Uri.parse('$apiBaseUrl/v1/gateways'),
        headers: headers,
      );
      final sensorsFuture = http.get(
        Uri.parse('$apiBaseUrl/v1/gateways/$gatewayId/sensors'),
        headers: headers,
      );

      final results = await Future.wait([gatewaysFuture, sensorsFuture]);
      final gatewaysRes = results[0];
      final sensorsRes = results[1];

      // 2. Kiểm tra nếu Sensors endpoint trả về 404 (Không tồn tại hoặc không có quyền)
      if (sensorsRes.statusCode == 404) {
        status = GatewayDetailStatus.notFound;
        notifyListeners();
        return;
      }

      // 3. Phân tích kết quả Gateways list để lấy role và tên trạm
      if (gatewaysRes.statusCode == 200 && sensorsRes.statusCode == 200) {
        final gatewaysBody = json.decode(gatewaysRes.body);
        final gatewayList = gatewaysBody['items'] as List<dynamic>? ?? [];

        // Tìm gateway tương ứng trong danh sách người dùng được phân quyền
        final gwMeta = gatewayList.firstWhere(
          (item) => item['gateway_id'] == gatewayId,
          orElse: () => null,
        );

        if (gwMeta == null) {
          // Trạm không có trong danh sách phân quyền của người dùng!
          status = GatewayDetailStatus.notFound;
          notifyListeners();
          return;
        }

        final sensorsBody = json.decode(sensorsRes.body);
        final sensorList = sensorsBody['items'] as List<dynamic>? ?? [];

        data = GatewayDetailData(
          gatewayId: gatewayId,
          name: gwMeta['name'] as String? ?? gatewayId,
          description: gwMeta['description'] as String? ?? '',
          role: GatewayRole.fromString(gwMeta['role'] as String?),
          sensors: sensorList,
        );
        status = GatewayDetailStatus.loaded;
        notifyListeners();
      } else {
        errorMessage = 'Lỗi nạp dữ liệu máy chủ: ${sensorsRes.statusCode}';
        status = GatewayDetailStatus.error;
        notifyListeners();
      }
    } catch (e) {
      errorMessage = 'Lỗi kết nối mạng: $e';
      status = GatewayDetailStatus.error;
      notifyListeners();
    }
  }
}
```

#### Snippet Cấu hình Định tuyến `go_router` trên Web (`app_router.dart`):

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'screens/gateway_list_screen.dart';
import 'screens/gateway_detail_screen.dart';

final GoRouter appRouter = GoRouter(
  initialLocation: '/gateways',
  routes: [
    GoRoute(
      path: '/gateways',
      name: 'gateways',
      builder: (context, state) => const GatewayListScreen(),
      routes: [
        GoRoute(
          path: ':id',
          name: 'gateway-detail',
          builder: (context, state) {
            // Đọc gatewayId trực tiếp từ pathParameters chuẩn Declarative Routing
            final gatewayId = state.pathParameters['id'] ?? '';
            return GatewayDetailScreen(gatewayId: gatewayId);
          },
        ),
      ],
    ),
  ],
);
```

---

## 8. Bảng kiểm tra kiểm thử phía Client Web (Web Client Verification Checklist)

Trước khi đóng gói hoặc nghiệm thu ứng dụng Flutter Web, các kịch bản kiểm thử phân quyền trên trình duyệt sau bắt buộc phải được xác nhận:

| STT | Kịch bản kiểm thử Web | Thao tác thực hiện | Hành vi mong đợi của Web Client | Kết quả đạt |
|---|---|---|---|---|
| **TC-01** | Tài khoản mới chưa được gán trạm (`items: []`) | Đăng nhập tài khoản mới chưa có quyền trạm nào. | Hiển thị giao diện rỗng (`EmptyGatewayView`) dạng Card nổi giữa màn hình kèm nút "Làm mới", không báo lỗi HTTP. | [ ] |
| **TC-02** | Hiển thị Bảng Dữ liệu Responsive trên Desktop | Mở Web trên màn hình $\ge 900\,\text{px}$. | Hiển thị danh sách dạng Data Table với đầy đủ các cột: Mã trạm, Tên trạm, Vai trò, Ngày tạo, Thao tác. | [ ] |
| **TC-03** | Hiển thị Role Badge & Tooltip vai trò | Rê chuột (hover) qua Role Badge (`owner`, `operator`, `viewer`). | Hiển thị Tooltip giải thích chi tiết quyền hạn và mã màu đại diện trực quan. | [ ] |
| **TC-04** | Deep Link truy cập trạm KHÔNG có quyền | Gõ trực tiếp trên URL: `https://.../gateways/gw_unauthorized`. | Client gọi API, nhận `404 Not Found` -> Giữ nguyên URL, hiển thị `GatewayNotFoundView` kèm nút "Quay lại danh sách trạm". | [ ] |
| **TC-05** | Nhấn F5 / Reload tại trang chi tiết trạm hợp lệ | Đang ở `/gateways/gw_001`, nhấn F5 trên trình duyệt. | Client gọi `GET /v1/gateways` & `GET /v1/gateways/gw_001/sensors`, nạp lại đầy đủ Tên trạm, Role Badge và Danh sách cảm biến. | [ ] |
| **TC-06** | Nhấn F5 / Reload tại trang trạm KHÔNG hợp lệ | Đang ở `/gateways/gw_invalid`, nhấn F5 trên trình duyệt. | Client phân giải path parameter, gọi API nhận 404 -> Hiển thị trang lỗi `GatewayNotFoundView`. | [ ] |
| **TC-07** | Admin thu hồi quyền trong `user_gateways` | Admin xóa bản ghi trong `user_gateways`, user bấm reload cảm biến trên cùng JWT. | Request tiếp theo trả về `404 Not Found` tức thì (Next DB query) -> Client bắt lỗi, chuyển sang thông báo mất quyền / 404. | [ ] |
| **TC-08** | Cố tình chèn query `?user_id=other_uuid` vào URL | Gõ URL kèm tham số `?user_id=...` gọi lên backend. | Backend từ chối `400 Bad Request` (`invalid_request`) -> Client bắt lỗi an toàn, không hiển thị dữ liệu của người khác. | [ ] |
| **TC-09** | Platform Admin đăng nhập vào Web Dashboard | Đăng nhập bằng tài khoản Platform Admin, gọi `GET /v1/gateways`. | Chỉ nhìn thấy các trạm được phân quyền trong `user_gateways` cho Admin đó; không tự bung danh sách toàn hệ thống (No Admin Read Bypass). | [ ] |
| **TC-10** | Gateway Owner (non-admin) gọi route quản trị | Dùng JWT của Gateway `owner` gọi bất kỳ route nào dưới `/v1/admin/*`. | Backend từ chối tức thì với mã lỗi `403 Forbidden` (`insufficient permissions`), chứng minh owner không phải là admin. | [ ] |
| **TC-11** | Gọi các endpoint stub Giai đoạn 2 | Client gửi request tới `/v1/telemetry/history`, `/v1/ws`, hoặc `/v1/digital-twins`. | Backend trả về `501 Not Implemented` (`not_implemented`) -> Client xử lý an toàn, thông báo tính năng chưa khả dụng ở Stage 2. | [ ] |
| **TC-12** | Sự cố cơ sở dữ liệu hoặc timeout lookup quyền | Mô phỏng ngắt kết nối PostgreSQL hoặc timeout tra cứu quyền/admin. | Backend trả về `503 Service Unavailable` (`service_unavailable`) -> Client hiển thị thông báo gián đoạn dịch vụ kèm nút thử lại. | [ ] |
