# Tài liệu Client — 03: Cơ chế Phân quyền Người dùng và Trạm Gateway (User–Gateway Authorization) trên Flutter Web

**Cập nhật:** 2026-10-03  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 6, 6.5), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/client/02_AUTHENTICATION_AND_SESSION.md`, `docs/backend/stage-2-task-2.3-authorization.md`.

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
3. **Phân quyền động tại thời điểm truy vấn (No Membership Cache):** Quyền của người dùng được kiểm tra trực tiếp trên từng truy vấn SQL tại PostgreSQL. Nếu quản trị viên thu hồi quyền của người dùng trên một Gateway trong database, request ngay tiếp theo từ trình duyệt Web sẽ bị từ chối ngay lập tức.

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
- **Cảm biến kế thừa toàn bộ quyền từ Gateway cha (`gateway_id`):** Một người dùng chỉ có thể xem danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`), số đo đo đạc (telemetry) hoặc lịch sử nếu người dùng đó có liên kết hợp lệ trong bảng `user_gateways` với Gateway chứa cảm biến đó.
- **Không có phân quyền cảm biến độc lập:** Hệ thống không hỗ trợ và không cho phép cấp quyền riêng lẻ trên từng Sensor (ví dụ: chỉ cho phép xem cảm biến A nhưng cấm cảm biến B trên cùng một Gateway). Nếu có quyền trên Gateway, người dùng có quyền tương ứng trên toàn bộ cảm biến của Gateway đó.

---

## 3. Mô hình Phân quyền 3 Vai trò (Three Roles Model) & Trải nghiệm Web Dashboard

Trong cơ sở dữ liệu PostgreSQL, ràng buộc kiểm tra schema quy định rõ:
```sql
CHECK (role IN ('owner', 'operator', 'viewer'))
```

Mỗi vai trò mang ý nghĩa nghiệp vụ và phạm vi phân quyền rõ ràng trên từng Gateway cụ thể:

| Vai trò | Tên hiển thị (VN) | Quyền hạn dữ liệu (Stage 2) | Kế hoạch Giai đoạn sau (Downlink/Control) | Hiển thị Client (Role Badge) |
|---|---|---|---|---|
| `owner` | **Chủ sở hữu** | Toàn quyền đọc telemetry, history, media, digital twin của trạm được gán. | Cấu hình trạm (`desired_state`), quản lý metadata cảm biến, ra lệnh an toàn. | `Badge(Color: Amber, Label: "Chủ sở hữu", Icon: admin_panel_settings)` |
| `operator` | **Người vận hành** | Đọc telemetry, history, media, digital twin của trạm được chỉ định. | Được thực thi các lệnh vận hành an toàn trong allowlist (ví dụ: chụp ảnh tức thời). **Không** được sửa `desired_state` hay metadata. | `Badge(Color: Blue, Label: "Vận hành", Icon: engineering)` |
| `viewer` | **Người xem** | Hoàn toàn chỉ đọc (Read-only) telemetry, media, digital twin. | Tuyệt đối không có quyền ghi, không điều khiển thiết bị, không sửa cấu hình. | `Badge(Color: Grey/Teal, Label: "Người xem", Icon: visibility)` |

### 3.1. Trải nghiệm phân quyền trên Web Dashboard (Desktop / Tablet / Responsive)
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

### 3.2. Chính sách bảo mật Giai đoạn 2 (Stage 2 Boundary & Fail-closed Policy)
1. **Chế độ Fail-closed:** Trong Giai đoạn 2 hiện tại, Go Backend mới chỉ triển khai hai API đọc:
   - `GET /v1/gateways`: Lấy danh sách trạm người dùng có quyền.
   - `GET /v1/gateways/{gateway_id}/sensors`: Lấy danh sách cảm biến của trạm.
2. **Quyền của `operator` ở Giai đoạn 2:** Mặc dù theo chính sách mục tiêu, `operator` sẽ được thực thi một số lệnh vận hành sau này, nhưng ở Giai đoạn 2 khi hạ tầng lệnh chưa hoàn thiện, backend áp dụng chính sách **fail-closed** — nghĩa là cả `owner`, `operator` và `viewer` đều hoạt động ở chế độ đọc an toàn.
3. **Ứng xử giao diện Client Web:**
   - Ẩn toàn bộ nút cấu hình, nút gửi lệnh điều khiển hoặc thay đổi metadata cho đến khi các API tương ứng được triển khai và kiểm chứng ở các giai đoạn sau.
   - Đối với tài khoản vai trò `viewer`, client vĩnh viễn không hiển thị bất kỳ giao diện thao tác ghi nào.

---

## 4. Phân biệt rạch ròi: Platform Admin vs. Gateway Roles

Một trong những nhầm lẫn phổ biến là đồng nhất quyền Quản trị viên hệ thống với quyền Chủ sở hữu trạm. Cần phân định dứt khoát:

```text
+-----------------------------------------+-----------------------------------------+
|     Platform Administrator (Toàn cục)   |          Gateway Role (Từng trạm)       |
+-----------------------------------------+-----------------------------------------+
| - Xác định qua bảng `platform_admins`   | - Xác định qua bảng `user_gateways`     |
| - Quản lý tài khoản (tạo user, gán role)| - Gắn liền với một `gateway_id` cụ thể  |
| - Khởi tạo Gateway, cấp MQTT credential | - Là `owner`, `operator`, hoặc `viewer` |
| - Thao tác qua Supabase Studio/Admin API| - Thao tác qua ứng dụng Flutter Web     |
+-----------------------------------------+-----------------------------------------+
```

### Nguyên tắc bảo mật quan trọng:
- **Platform Admin KHÔNG tự động bypass phân quyền trên API người dùng:** Khi một người dùng là Platform Admin đăng nhập vào ứng dụng Web và gọi API `GET /v1/gateways`, Go Backend **không** tự động trả về toàn bộ Gateway trên hệ thống. Platform Admin chỉ nhìn thấy đúng những Gateway mà tài khoản của họ được liên kết trong bảng `user_gateways`.
- **Lý do kiến trúc:** Tránh việc vô tình để lộ dữ liệu nhạy cảm hoặc làm tràn ngập màn hình người dùng của Admin bằng hàng trăm trạm không liên quan đến phạm vi kiểm thử hay giám sát trực tiếp của họ. Mọi thao tác quản trị toàn hệ thống đều đi qua kênh quản trị riêng biệt (`Supabase Studio` hoặc công cụ nội bộ).

---

## 5. Hành vi UI đối với Tài khoản Mới (Empty State) trên Web

Theo chính sách quản lý tài khoản tập trung (`AGENTS.md` Mục 6.5 và `02_AUTHENTICATION_AND_SESSION.md`), người dùng không thể tự đăng ký tài khoản. Tài khoản do Quản trị viên tạo sẵn.

### Kịch bản Người dùng chưa được gán Gateway:
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

### Hành vi giao diện Web Client (Empty State UX):
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

### 6.3. Xử lý Thu hồi Quyền khi đang mở trang chi tiết (Live Revocation)
Nếu người dùng đang mở xem chi tiết trạm `gw_001`, và Quản trị viên thực hiện xóa quyền của người dùng đó trong cơ sở dữ liệu (`DELETE FROM user_gateways ...`):
1. Ở lần thao tác tiếp theo (ví dụ: bấm nút làm mới cảm biến, hoặc cơ chế tự động đồng bộ):
   - Client gửi `GET /v1/gateways/gw_001/sensors`.
   - Go Backend truy vấn PostgreSQL, không thấy quyền -> trả về `404 Not Found`.
2. Client bắt mã lỗi 404:
   - Hiển thị ngay thông báo lỗi (Error Banner / Dialog) cảnh báo quyền truy cập đã bị thu hồi hoặc trạm không còn tồn tại.
   - Chuyển giao diện sang **Gateway Not Found View** kèm nút điều hướng người dùng quay lại `/gateways`.

---

## 7. Hướng dẫn Triển khai Mã nguồn Dart/Flutter Tham khảo

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
| **TC-07** | Admin thu hồi quyền trong khi user đang mở trạm | Admin xóa bản ghi trong `user_gateways`, user bấm reload cảm biến. | Request trả về `404 Not Found` -> Client bắt lỗi, chuyển sang thông báo mất quyền / 404. | [ ] |
| **TC-08** | Cố tình chèn query `?user_id=other_uuid` vào URL | Gõ URL kèm tham số `?user_id=...` gọi lên backend. | Backend từ chối `400 Bad Request` -> Client bắt lỗi an toàn, không hiển thị dữ liệu của người khác. | [ ] |
| **TC-09** | Platform Admin đăng nhập vào Web Dashboard | Đăng nhập bằng tài khoản Platform Admin. | Chỉ nhìn thấy các trạm được phân quyền trong `user_gateways` cho Admin đó; không tự bung danh sách toàn hệ thống. | [ ] |
