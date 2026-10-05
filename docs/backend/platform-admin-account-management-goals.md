# Mục tiêu quản lý tài khoản người dùng bởi Platform Administrator

Tài liệu này xác định mục tiêu, phạm vi chức năng đề xuất và các rào chắn bảo mật
cho năng lực quản lý tài khoản người dùng trong tương lai bởi platform
administrator thông qua Go backend.

> **Lưu ý quan trọng về trạng thái:** Đây là tài liệu xác định mục tiêu và phạm
> vi (goals and scope document), **không phải kế hoạch triển khai (implementation
> plan)**. Toàn bộ API và hành vi được mô tả dưới đây là **năng lực dự kiến cho
> tương lai**, hiện tại chưa được đăng ký, kích hoạt hay triển khai trong Go
> backend. Việc quản trị tài khoản hiện tại vẫn thực hiện qua công cụ quản trị
> hạ tầng của Supabase Auth theo [Task 2.2A](stage-2-task-2.2A-centralized-accounts.md).

---

## 1. Mục tiêu và vị trí trong kiến trúc

### 1.1. Mục tiêu nghiệp vụ tương lai
- Cung cấp giao diện lập trình ứng dụng (Go REST API) cho phép human platform
  administrator quản lý vòng đời tài khoản người dùng (liệt kê, xem chi tiết,
  tạo tài khoản, xóa tài khoản) trực tiếp từ các giao diện quản trị ứng dụng được
  ủy quyền mà không cần truy cập trực tiếp vào hạ tầng database hay Supabase
  Studio.
- Đảm bảo tuân thủ tuyệt đối mô hình quản lý tập trung: không mở public signup,
  không có cơ chế tự đăng ký và không xây dựng kho lưu trữ danh tính song song
  (no parallel identity store).

### 1.2. Vị trí và ranh giới kiến trúc
- **Supabase Auth là nguồn chân lý duy nhất (Authoritative Source of Truth):**
  Mọi vòng đời tài khoản (thông tin định danh, xác thực, mật khẩu, trạng thái tài
  khoản) do Supabase Auth (`auth.users`) quản lý. Go backend không lưu trữ bản sao
  mật khẩu hay quản lý cơ chế xác thực riêng biệt; bảng `profiles` trong PostgreSQL
  kế thừa và tham chiếu trực tiếp khóa chính `auth.users.id`.
- **Ủy quyền qua Auth Admin API phía server:** Go backend thực hiện các thao tác
  quản trị tài khoản bằng cách gọi Supabase Auth Admin API (thông qua Supabase API
  Gateway) sử dụng credential đặc quyền phía server (`service_role` key).
- **Tuyệt đối không rò rỉ `service_role`:** Credential `service_role` là bí mật hạ
  tầng được bảo vệ nghiêm ngặt trong môi trường server của Go backend; tuyệt đối
  không chia sẻ, phản hồi hoặc gửi `service_role` cho Flutter client, web
  application hay bất kỳ Gateway nào.
- **Tách biệt quyền ứng dụng và trạng thái tài nguyên:** Việc tạo tài khoản mới
  chỉ thuần túy khởi tạo danh tính người dùng trong Supabase Auth và profile liên
  kết. Tài khoản mới **không tự động nhận quyền** platform administrator trong
  `platform_admins` và **không tự động được gán quyền** thành viên hay quyền sở
  hữu trên bất kỳ Gateway nào trong `user_gateways`.

---

## 2. Ranh giới truy cập và mô hình định danh

### 2.1. Phân biệt cấp độ truy cập hạ tầng và quyền API ứng dụng
Cần phân biệt rõ hai cấp độ quyền hạn hoàn toàn độc lập:
1. **Infrastructure Credential Access (Quyền hạ tầng):** Quyền của operator hạ
   tầng có quyền truy cập trực tiếp máy chủ, mạng quản trị tin cậy, Supabase
   Studio, database superuser (`postgres`), hoặc nắm giữ trực tiếp `service_role`
   key / database administrator credentials.
2. **User-level API Permission (Quyền API ứng dụng):** Quyền của human platform
   administrator khi tương tác với Go business REST API thông qua HTTP request.
   Actor ở cấp độ này bắt buộc phải gửi Supabase JWT hợp lệ với claim
   `role=authenticated` do Supabase Auth phát hành.

### 2.2. Xác thực và xác định danh tính Actor
- **Danh tính Actor bất biến:** Actor thực hiện request quản trị được trích xuất
  duy nhất từ JWT đã được xác minh chữ ký và tính hợp lệ (`Principal.UserID` từ
  claim `sub`). Tuyệt đối không chấp nhận danh tính actor từ request body, query
  parameter, hay custom header do client tự khai báo.
- **Kiểm tra quyền Platform Administrator:** Go backend xác minh quyền platform
  admin bằng cách tra cứu sự tồn tại của `Principal.UserID` trong bảng
  `platform_admins` tại PostgreSQL. Một JWT hợp lệ đơn thuần không chứng minh
  quyền admin; backend độc lập kiểm tra và từ chối (`403 Forbidden`) nếu actor
  không thuộc `platform_admins`.

---

## 3. Giao diện API đề xuất (Proposed API Surface)

Các endpoint dưới đây được đề xuất cho năng lực tương lai, tuân thủ quy ước URL
nghiệp vụ quản trị của hệ thống (`/v1/admin/...`):

### 3.1. Liệt kê danh sách tài khoản
- **Endpoint đề xuất:** `GET /v1/admin/users`
- **Mục đích:** Cho phép platform administrator xem danh sách người dùng trong hệ
  thống kèm thông tin profile cơ bản.
- **Ràng buộc thiết yếu:**
  - Bắt buộc phân trang có giới hạn biên (bounded/paginated): yêu cầu tham số
    giới hạn (chẳng hạn `limit` có chặn cận trên và `offset`, hoặc cursor-based
    pagination) nhằm ngăn ngừa cạn kiệt bộ nhớ hoặc tấn công từ chối dịch vụ.
  - Phản hồi tuyệt đối không chứa mật khẩu, hash mật khẩu, refresh token hay
    credential kỹ thuật.

### 3.2. Xem thông tin chi tiết một tài khoản
- **Endpoint đề xuất:** `GET /v1/admin/users/{user_id}`
- **Mục đích:** Cho phép platform administrator xem thông tin chi tiết về tài
  khoản của một người dùng cụ thể dựa trên `user_id` (UUID).
- **Ràng buộc thiết yếu:**
  - Trả về thông tin danh tính, email, trạng thái xác nhận và profile liên quan.
  - Áp dụng triệt để nguyên tắc không để lộ dữ liệu nhạy cảm.

### 3.3. Tạo tài khoản người dùng mới
- **Endpoint đề xuất:** `POST /v1/admin/users`
- **Mục đích:** Cho phép platform administrator khởi tạo tài khoản human user mới
  có chủ đích trong hệ thống mà không cần mở public signup.
- **Hành vi nghiệp vụ:**
  - Go backend nhận thông tin yêu cầu (email, thông tin khởi tạo), kiểm tra hợp lệ
    và gọi Auth Admin API để tạo user trong Supabase Auth.
  - Kích hoạt tạo profile tương ứng trong PostgreSQL theo kiến trúc đã có.
  - Không tự ý gán `platform_admins` hoặc tạo bản ghi trong `user_gateways`.
  - Phản hồi trả về thông tin tài khoản được tạo (chứa định danh UUID), không trả
    về mật khẩu hoặc session token bí mật.

### 3.4. Xóa tài khoản người dùng
- **Endpoint đề xuất:** `DELETE /v1/admin/users/{user_id}`
- **Mục đích:** Cho phép platform administrator xóa một tài khoản người dùng khỏi
  hệ thống khi có yêu cầu quản trị hợp lệ.
- **Hành vi nghiệp vụ:**
  - Thu hồi quyền truy cập và xóa danh tính tương ứng tại Supabase Auth thông qua
    Auth Admin API.
  - Xử lý các ràng buộc toàn vẹn dữ liệu trong application database theo các rào
    chắn bảo vệ quy định bên dưới.

---

## 4. Các rào chắn và biện pháp bảo vệ thiết yếu (Essential Safeguards)

### 4.1. Ngăn chặn tuyệt đối hành vi tự xóa (Self-deletion Prevention)
Việc một platform administrator tự xóa tài khoản của chính mình thông qua API ứng
dụng tiềm ẩn rủi ro nghiêm trọng về vận hành (như khóa ngoài hệ thống hoặc mất vết
kiểm toán actor). Do đó:
- **Phía Client (Flutter / Web App):** Giao diện người dùng bắt buộc phải chủ động
  ẩn hoặc vô hiệu hóa hành động xóa tài khoản đối với chính tài khoản của người
  dùng đang đăng nhập (`actor_user_id == target_user_id`).
- **Phía Backend Go (Chốt chặn độc lập và quyết định):**
  - Bảo vệ phía client thuần túy mang tính công thái học và không đủ để đảm bảo an
    toàn.
  - Go backend bắt buộc phải kiểm tra độc lập và từ chối ngay lập tức nếu định
    danh mục tiêu `target_user_id` trùng khớp với `actor_user_id` (trích xuất từ
    JWT đã xác minh). Request tự xóa phải bị bác bỏ với mã lỗi rõ ràng (ví dụ
    `400 Bad Request` hoặc `403 Forbidden`).
- **Phạm vi bảo đảm:** Cơ chế chống tự xóa này được thực thi tại tầng ứng dụng
  (Application API) nhằm bảo vệ platform administrator trong các thao tác vận hành
  thường nhật; cơ chế này **không phải là bảo đảm chống lại operator hạ tầng có
  đặc quyền trực tiếp** (những người có quyền can thiệp thẳng vào GoTrue container
  hoặc database superuser).

### 4.2. Bảo vệ dữ liệu nhạy cảm và kiểm soát phản hồi
- Mọi API phản hồi thông tin tài khoản (cả danh sách và chi tiết) phải lọc bỏ toàn
  bộ các trường nhạy cảm: không bao giờ trả về password, password hash, salt,
  recovery token, refresh token hay Supabase API keys (`service_role` hoặc
  `anon`).
- Danh sách tài khoản phải luôn có giới hạn kích thước tối đa cho mỗi trang để bảo
  vệ tài nguyên server và network payload.

### 4.3. Xử lý dữ liệu phụ thuộc và bảo toàn tính toàn vẹn kiểm toán
Khi xem xét xóa một tài khoản khỏi hệ thống, backend phải giải quyết các quan hệ
phụ thuộc dữ liệu trong PostgreSQL:
- **Hồ sơ và quyền thành viên (`profiles`, `user_gateways`):** Cần đảm bảo tính nhất
  quán khi bản ghi danh tính gốc tại `auth.users` bị xóa, tránh để lại các liên kết
  mồ côi không hợp lệ.
- **Lịch sử kiểm toán bất biến (Immutable Audit Integrity):**
  - Hệ thống lưu vết kiểm toán các sự kiện cấp phát/thu hồi MQTT credentials trong
    `gateway_mqtt_credential_events` (tham chiếu `actor_user_id`), cũng như người
    ra lệnh trong `twin_commands` (`issued_by`).
  - **Tuyệt đối không được ghi đè, làm sai lệch hoặc xóa bỏ** các bản ghi kiểm toán
    lịch sử này.
- **Không tự ý chuyển giao quyền sở hữu hoặc xóa theo tầng tùy tiện:**
  - Không tự động phát minh cơ chế chuyển nhượng quyền sở hữu Gateway (automatic
    ownership transfer) cho một user ngẫu nhiên.
  - Không cam kết thực hiện cascade deletion tùy tiện làm mất mát dữ liệu vận
    hành, đo đạc hoặc lịch sử thiết bị của Gateway.

---

## 5. Các quyết định chính sách cần làm rõ trước khi triển khai

Trước khi bắt tay vào thiết kế kỹ thuật và hiện thực hóa các API này, các câu hỏi
chính sách nghiệp vụ sau đây **cần được người có thẩm quyền phê duyệt và làm rõ**
(đây là các quyết định mở cần giải quyết, không phải chính sách mặc định đã được
chấp thuận):

1. **Chính sách đối với tài khoản đang giữ vai trò Gateway Owner:**
   - Khi một user đang là `owner` của một hoặc nhiều Gateway (ghi nhận trong
     `user_gateways` với `role = 'owner'` hoặc `gateways.owner_user_id`), hệ thống
     sẽ xử lý như thế nào khi nhận lệnh xóa?
   - *Lựa chọn cần làm rõ:* Bắt buộc từ chối xóa và yêu cầu platform admin phải
     chuyển quyền sở hữu Gateway cho một tài khoản khác trước; hay có quy trình xử
     lý thu hồi/vô hiệu hóa Gateway cụ thể?
2. **Chính sách xóa tài khoản giữa các Platform Administrator:**
   - Một platform administrator có được quyền xóa tài khoản của một platform
     administrator khác hay không?
   - *Lựa chọn cần làm rõ:* Có cần yêu cầu hạ quyền admin trước khi xóa? Có cần cơ
     chế bảo vệ "admin cuối cùng" (last admin standing protection) để ngăn ngừa tình
     huống hệ thống không còn bất kỳ platform administrator nào?
3. **Hình thức xóa danh tính (Hard Delete vs. Soft Delete / Deactivation):**
   - Xóa hoàn toàn bản ghi khỏi `auth.users` và `profiles`, hay hỗ trợ cơ chế khóa/vô
     hiệu hóa tài khoản (disable account) nhằm duy trì toàn vẹn dữ liệu cho các báo
     cáo lịch sử dài hạn?

---

## 6. Kết quả kỳ vọng

- Thiết lập tài liệu định hướng mục tiêu chuẩn xác cho khả năng quản lý tài khoản
  của platform administrator, giúp định hình phạm vi phát triển trong các giai
  đoạn tiếp theo.
- Khẳng định tính bất biến của các nguyên tắc kiến trúc: quản lý tập trung, Supabase
  Auth là authoritative source, không public signup, không để lộ `service_role`
  và bảo vệ tuyệt đối tính toàn vẹn của dữ liệu kiểm toán.
- Thiết lập rào chắn ngăn ngừa tự xóa tài khoản ở cả hai lớp (client và backend)
  với vai trò và trách nhiệm rõ ràng của từng tầng.
