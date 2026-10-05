# Stage 2 — Task 2.4: Admin Provisioning Gateway và Sensor

## 1. Phạm vi và trạng thái triển khai

Hai HTTP REST endpoints quản trị bên dưới đã được triển khai, kiểm thử hồi quy độc lập và đã được tích hợp (commit) vào mã nguồn chính tại commit `abcfbbadf563a45da87db9d84a8b7ca6aa996864` (với baseline SHA có CI xanh `5acd0c4`), không còn ở trạng thái worktree chưa commit:

```http
PUT /v1/admin/gateways/{gateway_id}
PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}
```

### Ranh giới phạm vi (Scope Boundaries)

- **Thuộc phạm vi hoàn thành của Task 2.4**:
  - Tạo bản ghi thực thể nghiệp vụ `gateways` và `sensors` trong PostgreSQL.
  - Gán người dùng sở hữu ban đầu (`owner`) trong bảng `user_gateways`.
  - Khởi tạo đồ thị Digital Twin tương ứng trong cùng giao dịch: thực thể (`twin_entities`), trạng thái mặc định (`twin_states`), và quan hệ phân cấp (`twin_relationships` với `relationship_type = 'hasSensor'`).
  - Xử lý tính lũy quyền (idempotent retry) cho request tạo lại trùng khớp hoàn toàn, phát hiện xung đột (`409 Conflict`), và bảo đảm tính nguyên tử của giao dịch (atomic graph rollback khi có lỗi).

- **Tuyệt đối ngoài phạm vi Task 2.4**:
  - Không cấp phát, xoay vòng hay thu hồi thông tin xác thực MQTT (MQTT credentials); việc quản lý credential và Ingress Gate thuộc phạm vi riêng của Task 2.6.
  - Không sửa đổi `password_file` của Mosquitto hay nạp lại broker tĩnh (đã được thay thế bởi kiến trúc DynSec trong Task 2.6).
  - Không cung cấp API quản lý người dùng (user CRUD) hay cho phép người dùng tự đăng ký (public self-signup); tài khoản người dùng được quản trị tập trung qua công cụ Supabase Auth quản trị (Task 2.2A).
  - Không cung cấp API quản lý thành viên (membership management API) hay cơ chế tự nhận thiết bị (self-claiming); việc gán quyền thành viên `user_gateways` được thực hiện qua công cụ vận hành được bảo vệ (Task 2.7).
  - Không hỗ trợ chuyển giao quyền sở hữu (ownership transfer); API PUT này là cơ chế khởi tạo và retry lũy quyền, tuyệt đối không cho phép đổi owner của Gateway đã tồn tại.
  - Không cho phép Gateway Owner tự tạo hoặc cập nhật Sensor (owner-facing sensor writes chưa được triển khai; Owner chỉ có quyền đọc danh sách Sensor qua `GET /v1/gateways/{gateway_id}/sensors`).
  - Không cung cấp Gateway HTTP credentials, không hỗ trợ tải lên tệp đa phương tiện (media upload), không nhận dữ liệu đo từ xa (telemetry ingress), không hỗ trợ luồng WebSocket thời gian thực (realtime streaming).
  - Không bao gồm phân hệ điều khiển thiết bị, trạng thái mong muốn (`desired_state`), lệnh (`commands`), outbox, hay MQTT bridge.
  - URN mapper ở tầng `internal/digitaltwin/mapper` mới chỉ là bộ sinh định danh thực thể theo chuẩn URN, chưa phải là API NGSI-LD hoàn chỉnh hay bộ tuần tự hóa JSON-LD mở rộng.

---

## 2. Authentication và Role Policy

- **Yêu cầu JWT Supabase**:
  - Mọi request đều phải gửi kèm header `Authorization: Bearer <human_access_token>`.
  - Token phải thỏa mãn đầy đủ các ràng buộc bảo mật: chữ ký hợp lệ (HMAC-SHA256 với secret của dự án), đúng issuer, đúng audience, còn hiệu lực thời gian và mang claim vai trò người dùng `authenticated`.
  - Request thiếu token, token sai định dạng, token hết hạn, hoặc mang vai trò `anon` hay `service_role` đều bị từ chối với mã lỗi `401 Unauthorized` (`code: "unauthorized"`, `message: "authentication required"`, kèm header `WWW-Authenticate: Bearer`). Token `service_role` không được coi là tài khoản Platform Admin.
  - Không chấp nhận truyền token hoặc định danh người dùng qua query parameters hoặc request body.

- **Xác thực quyền Platform Admin (Hai lớp kiểm tra)**:
  - Lớp 1 (Middleware): Middleware `RequirePlatformAdmin` kiểm tra sự tồn tại của `user_id` trong bảng `platform_admins` trên cơ sở dữ liệu PostgreSQL.
  - Lớp 2 (Transaction Recheck): Repository thực hiện **kiểm tra lại quyền Platform Admin một lần nữa bên trong giao dịch database** (`auth.NewPostgresPlatformAdminChecker(tx)`) ngay trước khi thực thi bất kỳ thao tác ghi nào.
  - Trường hợp quyền Platform Admin bị thu hồi sau khi vượt qua middleware nhưng trước thời điểm giao dịch thực thi kiểm tra lại, request sẽ bị từ chối với mã lỗi `403 Forbidden` (`code: "forbidden"`, `message: "access denied"`). Cơ chế này giảm thiểu cửa sổ bất định (race window), nhưng không phải là khóa ngăn chặn thu hồi sau thời điểm recheck.

- **Chính sách phân quyền theo vai trò (Role Policy)**:
  - Actor thực hiện request bắt buộc phải là Platform Admin. Các vai trò thông thường trong `user_gateways` (`owner`, `operator`, `viewer`) hoặc người dùng tự do (`non_member`) không có bản ghi trong `platform_admins` đều nhận `403 Forbidden`, kể cả khi người đó đã là Owner của Gateway đích.
  - Trường `owner_user_id` trong request body của Gateway PUT là định danh người dùng mà Platform Admin chỉ định làm Owner ban đầu, không phải là định danh do actor tự khai báo cho chính mình.
  - Người dùng được chỉ định làm Owner bắt buộc phải tồn tại từ trước trong bảng `public.profiles`. API provisioning không tự động tạo tài khoản Auth hay profile người dùng.
  - Quyền đọc dữ liệu nghiệp vụ (`GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors`) tuân thủ nghiêm ngặt bảng phân quyền `user_gateways`:
    - Platform Admin **không tự động bypass** quyền đọc nghiệp vụ. Nếu một Admin chưa được gán quan hệ thành viên trong `user_gateways` cho một Gateway, khi gọi `GET /v1/gateways` hệ thống chỉ trả về danh sách rỗng (`items: []`).
    - Các vai trò `owner`, `operator`, `viewer` chỉ có thể đọc thông tin trong phạm vi các Gateway mà mình được gán.
    - Sensor kế thừa quyền truy cập trực tiếp từ Gateway cha. Khi người dùng không phải thành viên (non-member) hoặc truy vấn Sensor thuộc Gateway không tồn tại / không được gán, API trả về `404 Not Found` để bảo vệ tính cô lập và không làm lộ sự tồn tại của tài nguyên.
  - Các quyền hạn mục tiêu liên quan đến điều khiển an toàn (allowlisted safe operational commands) của vai trò `operator` nêu trong `AGENTS.md` chưa được kích hoạt tại Task 2.4, do phân hệ command execution chưa được triển khai.

---

## 3. Request/Response Contract và Strict Parsing

Các giá trị trong ví dụ dưới đây là placeholder minh họa cấu trúc, không phải ID hoặc secret của môi trường triển khai thực tế.

### 3.1 Gateway Provisioning PUT

```http
PUT /v1/admin/gateways/{gateway_id}
Authorization: Bearer <human_access_token>
Content-Type: application/json
```

**Request Body**:

```json
{
  "name": "<gateway name>",
  "description": null,
  "owner_user_id": "<existing nonzero profile UUID>"
}
```

**Response Body** (`201 Created` khi tạo mới thành công; `200 OK` khi retry lũy quyền hợp lệ; cấu trúc phẳng không có wrapper `items`):

```json
{
  "gateway_id": "<gateway_id>",
  "name": "<gateway name>",
  "description": null,
  "owner_user_id": "<owner UUID>",
  "entity_id": "urn:ngsi-ld:Gateway:<gateway_id>",
  "created_at": "<database timestamp in RFC3339 UTC, or null>"
}
```

### 3.2 Sensor Provisioning PUT

```http
PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}
Authorization: Bearer <human_access_token>
Content-Type: application/json
```

**Request Body**:

```json
{
  "name": "<sensor name>",
  "unit": null
}
```

**Response Body** (`201 Created` khi tạo mới; `200 OK` khi retry trùng khớp):

```json
{
  "gateway_id": "<gateway_id>",
  "sensor_id": "<sensor_id>",
  "name": "<sensor name>",
  "unit": null,
  "entity_id": "urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>",
  "created_at": "<database timestamp in RFC3339 UTC, or null>"
}
```

*Lưu ý về định dạng dữ liệu trả về*:
- Server tự động xây dựng URN định danh theo chuẩn NGSI-LD (`urn:ngsi-ld:Gateway:...` và `urn:ngsi-ld:Sensor:...:...`).
- Tuyệt đối không trả về khóa chính nội bộ (UUID của `twin_entities`), trạng thái chi tiết, hay thông tin xác thực.
- `created_at` được lấy trực tiếp từ database và chuẩn hóa về múi giờ UTC theo định dạng RFC3339 (có thể bao gồm phần thập phân của giây). Nếu bản ghi SQL có giá trị NULL, API trả về `null` trong JSON, không tự tạo timestamp giả định.

### 3.3 Quy tắc kiểm tra tính hợp lệ (Validation)

| Trường dữ liệu | Vị trí | Quy tắc kiểm tra |
|---|---|---|
| `gateway_id` | Path param | Bắt buộc, phân biệt hoa/thường; regex `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`; **cấm giá trị dành riêng `backend_service`** |
| `sensor_id` | Path param | Bắt buộc, cùng biểu thức regex trên; không áp dụng cấm `backend_service` cho Sensor |
| `name` | JSON body | Bắt buộc, kiểu string, không được rỗng hoặc chỉ chứa khoảng trắng, độ dài tối đa **256 UTF-8 bytes** |
| `description` | JSON body | Tùy chọn (string hoặc null), độ dài tối đa **4096 UTF-8 bytes** |
| `unit` | JSON body | Tùy chọn (string hoặc null), độ dài tối đa **64 UTF-8 bytes** |
| `owner_user_id` | JSON body | Bắt buộc, chuỗi UUID v4 hợp lệ khác nil; phải tồn tại trong bảng `public.profiles` |

*Lưu ý*:
- Giới hạn độ dài được tính theo **bytes UTF-8**, không phải số ký tự (character count).
- Dữ liệu metadata được giữ nguyên giá trị gốc sau khi kiểm tra, không tự ý cắt tỉa khoảng trắng (trim), chuyển chữ thường (lowercase) hay chuẩn hóa ký tự.
- Trường hợp thiếu trường tùy chọn (`description` hoặc `unit`) trong JSON được xem là tương đương giá trị `null`; tuy nhiên `null` và chuỗi rỗng `""` được phân biệt rành mạch khi so sánh retry lũy quyền.
- Dữ liệu metadata phải là chuỗi UTF-8 hợp lệ, không chứa ký tự NUL (`\u0000`).

### 3.4 Phân tích cú pháp nghiêm ngặt (Strict JSON Parsing)

Parser áp dụng cơ chế phân tích cú pháp nghiêm ngặt tại `internal/httpserver/provisioning_request.go`:
- **Đúng một JSON object**: Chỉ chấp nhận đúng một JSON object hợp lệ. Từ chối ngay lập tức nếu JSON sai cú pháp, chứa trường lạ (unknown fields), chứa trường lặp lại (duplicate keys, kể cả khi tên trường được escape khác nhau), hoặc có dữ liệu dư thừa sau object (chỉ cho phép ký tự khoảng trắng sau dấu ngoặc đóng `}`).
- **An toàn Unicode**: Kiểm tra chuỗi byte UTF-8 thô; từ chối các ký tự Unicode surrogate đơn lẻ không ghép cặp (`\uD800`–`\uDFFF`) hoặc escape chứa NUL trước khi thư viện chuẩn `encoding/json` có thể tự ý thay thế bằng ký tự Unicode replacement.
- **Không nhận trường ngoài luồng**: Tuyệt đối không chấp nhận các trường như `user_id`, `role`, `entity_id`, hay các trường trạng thái/credential.
- **Cấm Query String**: Mọi query string trên URL, kể cả dấu hỏi chấm rỗng `?`, đều bị từ chối với mã lỗi `400 Bad Request`.
- **Content-Type**: Bắt buộc là `application/json`. Tham số duy nhất được phép đi kèm là `charset=utf-8` (không phân biệt hoa thường). Bất kỳ giá trị hoặc tham số nào khác đều trả về `415 Unsupported Media Type`.
- **Hạn mức kích thước Body**: Giới hạn tối đa được cấu hình qua biến `ADMIN_MAX_BODY_BYTES`: mặc định **16384 bytes (16 KiB)**, phạm vi hợp lệ từ **1 đến 1048576 bytes (1 MiB)**. Nếu cấu hình nằm ngoài dải này, hệ thống sẽ fail ngay khi khởi động. Body vượt quá hạn mức sẽ bị từ chối với mã lỗi `413 Payload Too Large` (parser chỉ đọc tối đa limit + 1 byte để xác định vi phạm).
- **Header điều khiển**: Handlers luôn gán header `Cache-Control: no-store`. Request ID được quản lý và gắn qua middleware thông qua header `X-Request-ID`.

---

## 4. Atomic Graph, Retry, Concurrency và Rollback

### 4.1 Tính toàn vẹn đồ thị và cấp độ cô lập giao dịch

Toàn bộ thao tác provisioning sử dụng connection pool hiện có (`pgxpool`), câu lệnh SQL được tham số hóa hoàn toàn (parameterized queries), và chạy trong giao dịch ở cấp độ cô lập **`READ COMMITTED`** dưới role cơ sở dữ liệu **`iot_backend_app`**. Task 2.4 không yêu cầu nâng quyền superuser, không chạy DDL, và không thêm migration mới (các migrations và grants đến phiên bản `000010` là điều kiện tiên quyết).

Đồ thị phụ thuộc được tạo lập nguyên tử:
```text
Gateway mới: gateways + user_gateways(role='owner') + twin_entities(Gateway) + twin_states
Sensor mới:  sensors + twin_entities(Sensor) + twin_states + twin_relationships(Gateway --hasSensor--> Sensor)
```

1. **Khởi tạo Gateway**:
   - Kiểm tra Platform Admin trong giao dịch.
   - Xác thực sự tồn tại của `owner_user_id` trong `public.profiles`.
   - Thực thi lệnh:
     ```sql
     INSERT INTO public.gateways(gateway_id, name, description)
     VALUES ($1, $2, $3)
     ON CONFLICT (gateway_id) DO NOTHING
     RETURNING created_at;
     ```
   - Nếu chèn thành công: Tiếp tục chèn bản ghi `user_gateways` với `role = 'owner'`, chèn thực thể `twin_entities` (với `entity_type = 'Gateway'`), và chèn bản ghi `twin_states` khởi tạo.
   - Nếu xảy ra xung đột (`pgx.ErrNoRows`): Chuyển sang quy trình xác thực retry (`verifyGatewayRetry`).

2. **Khởi tạo Sensor**:
   - Kiểm tra Platform Admin trong giao dịch.
   - Thực hiện **khóa bi quan trên Gateway cha** để tuần tự hóa các thao tác trên cùng một Gateway:
     ```sql
     SELECT name FROM public.gateways WHERE gateway_id = $1 FOR UPDATE;
     ```
     Nếu Gateway cha không tồn tại, trả về `404 Not Found`.
   - Xác thực Gateway cha có thực thể Twin và trạng thái hợp lệ trong `twin_entities` và `twin_states`. Nếu thiếu, trả về `500 Internal Server Error` (báo hiệu đồ thị dữ liệu bị mất tính nhất quán).
   - Thực thi lệnh:
     ```sql
     INSERT INTO public.sensors(gateway_id, sensor_id, name, unit)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (gateway_id, sensor_id) DO NOTHING
     RETURNING created_at;
     ```
   - Nếu chèn thành công: Chèn thực thể `twin_entities` (với `entity_type = 'Sensor'`), chèn trạng thái `twin_states`, và chèn quan hệ `twin_relationships` (`source_entity_id = parent_twin, relationship_type = 'hasSensor', target_entity_id = sensor_twin`). Nếu thực thể Twin bị xung đột mồ côi (orphan Twin), giao dịch hủy bỏ và trả về `500`.
   - Nếu xảy ra xung đột: Chuyển sang quy trình xác thực retry (`verifySensorRetry`).

3. **Trạng thái Digital Twin ban đầu**:
   Bản ghi `twin_states` được khởi tạo hoàn toàn bằng các giá trị mặc định của schema:
   ```text
   reported_state = '{}'; desired_state = '{}'
   reported_version = 0; desired_version = 0
   last_reported_at = NULL; last_desired_at = NULL; last_desired_by = NULL
   ```
   API không tự ý gán trạng thái online, không tạo cấu hình mong muốn ban đầu, không chèn các bản ghi chuỗi thời gian, và không tạo lệnh hoặc bản ghi outbox.

### 4.2 Xử lý Retry, Conflict và Chính sách Cấm chuyển giao quyền sở hữu (No-Transfer)

Khi lệnh chèn gặp xung đột khóa chính (`ON CONFLICT DO NOTHING`), repository thực hiện xác thực với câu lệnh riêng biệt trong cùng snapshot `READ COMMITTED` sau khi chờ transaction của request cạnh tranh hoàn tất commit:

- **Mã `201 Created`**: Đồ thị hoàn toàn mới được khởi tạo và commit thành công. Không mang hàm ý thiết bị vật lý đã kết nối mạng hay đã thực thi bất kỳ lệnh nào.
- **Mã `200 OK` (Idempotent Retry)**:
  - Gateway: Metadata (`name`, `description`) và `owner_user_id` trùng khớp hoàn toàn với bản ghi hiện có; thực thể Twin và trạng thái `twin_states` tồn tại đầy đủ.
  - Sensor: Metadata (`name`, `unit`) trùng khớp; thực thể Twin, trạng thái `twin_states` và mối quan hệ `hasSensor` nối từ Gateway cha tồn tại đầy đủ.
  - *Nguyên tắc bảo toàn trạng thái*: Quy trình kiểm tra retry chỉ xác minh sự tồn tại hợp lệ của bản ghi trạng thái trong `twin_states`. API **tuyệt đối không so sánh, không ghi đè, và không đặt lại (reset)** các trường `reported_state`, `desired_state`, các phiên bản (version counters), hay timestamps đã tiến triển sau khi thiết bị vận hành.
- **Mã `409 Conflict`**:
  - Trùng ID nhưng sai lệch metadata (`name`, `description` hoặc `unit`).
  - Trùng `gateway_id` nhưng có một Owner khác đang sở hữu Gateway trong cơ sở dữ liệu.
  - **Chính sách cấm chuyển giao quyền sở hữu (No Ownership Transfer)**: API PUT này là endpoint khởi tạo tài nguyên và retry an toàn, **không phải là API cập nhật (update) hay chuyển quyền sở hữu (ownership transfer)**. Nếu client gửi request cho một Gateway đã có chủ nhưng chỉ định một `owner_user_id` khác, hệ thống kiên quyết trả về `409 Conflict`. Mọi hoạt động điều chỉnh quyền sở hữu hoặc thành viên Gateway nằm ngoài phạm vi Task 2.4 và phải thực hiện qua công cụ vận hành chuyên dụng của Platform Admin (Task 2.7).
- **Mã `500 Internal Server Error`**:
  - Phát hiện đồ thị dữ liệu bị gãy hoặc không nhất quán: Gateway không có Owner nào hoặc có nhiều hơn một Owner trong `user_gateways`; thiếu bản ghi `twin_entities`, thiếu `twin_states`, thiếu quan hệ `hasSensor`; hoặc tồn tại Twin mồ côi xung đột.
  - Hệ thống áp dụng nguyên tắc an toàn: Không tự động sửa chữa (no auto-repair), không tự chèn lại quan hệ bị mất, và không tự khôi phục các thành viên đã bị thu hồi hoặc hạ cấp. Trường hợp này đòi hỏi sự can thiệp và đối soát có kiểm soát của người vận hành hệ thống.

### 4.3 Đồng thời (Concurrency) và Ngân sách Thời gian (Timeout)

- Quá trình tạo mới sử dụng ràng buộc duy nhất (uniqueness constraints) và cơ chế `INSERT ... ON CONFLICT DO NOTHING ... RETURNING`, không sử dụng `DO UPDATE`, không dùng mutex toàn cục trong bộ nhớ tiến trình Go.
- Khi hai request đồng thời gửi cùng một ID và cùng dữ liệu: Một request sẽ giành quyền chèn và nhận mã `201 Created`, request còn lại chờ giao dịch của request đầu commit rồi đọc dữ liệu và nhận mã `200 OK`.
- Khi hai request đồng thời gửi cùng một ID nhưng khác dữ liệu: Request chiến thắng nhận `201 Created`, request còn lại nhận `409 Conflict`.
- **Ngân sách thời gian giao dịch**: Service thiết lập deadline riêng cho giao dịch từ cấu hình `AUTHORIZATION_TIMEOUT` (mặc định `2s`), độc lập với timer của middleware. Nếu xảy ra hủy bỏ request (cancellation) hoặc vượt quá thời gian thực thi (deadline exceeded), hệ thống trả về mã `503 Service Unavailable`.
- **Cơ chế Rollback an toàn**: Hàm `rollbackProvisioning(tx)` thực thi rollback thông qua một `context.Background()` độc lập với timeout giới hạn **5 giây**. Điều này bảo đảm rằng việc client ngắt kết nối hoặc hủy context giữa chừng không bao giờ làm gián đoạn lệnh rollback, bảo đảm kết nối luôn được giải phóng và hoàn trả sạch về pool.

### 4.4 Xử lý bất định mạng (Commit Ambiguity)

- Phản hồi HTTP chỉ được coi là thành công khi hàm `tx.Commit()` trả về `nil`.
- Trong trường hợp mất kết nối mạng hoặc timeout xảy ra đúng thời điểm commit, client không thể khẳng định dữ liệu đã được ghi vào database hay chưa dựa trên mã lỗi `500`, `503` hoặc network timeout.
- Client phải gửi lại đúng request ban đầu (cùng path, cùng body và cùng owner). Nếu giao dịch trước đó đã commit thành công và đồ thị dữ liệu chưa bị can thiệp, hệ thống sẽ trả về mã `200 OK`. Client không được tự ý thay đổi owner hoặc body nhằm "sửa lỗi", không giả định rằng hệ thống cung cấp exactly-once, và không giả định mọi lỗi commit đều đồng nghĩa với việc giao dịch đã rollback hoàn toàn.

---

## 5. Safe Errors

Tất cả phản hồi lỗi đều tuân thủ cấu trúc JSON tiêu chuẩn của hệ thống (request ID được tự động đính kèm và đồng bộ qua header `X-Request-ID`):

```json
{
  "error": {
    "code": "conflict",
    "message": "resource conflict",
    "request_id": "<request ID>"
  }
}
```

### Bảng tra cứu mã lỗi

| Mã HTTP | Mã Code | Thông báo chuẩn / Tình huống phát sinh |
|---|---|---|
| `401` | `unauthorized` | `authentication required` — Thiếu hoặc sai JWT; token role không phải `authenticated`; kèm header `WWW-Authenticate: Bearer` |
| `403` | `forbidden` | `insufficient permissions` — Actor không có trong `platform_admins` (từ middleware)<br>`access denied` — Quyền Admin bị thu hồi khi recheck trong transaction |
| `400` | `invalid_request` | `invalid request` — Sai cú pháp JSON, chứa trường lạ, trùng key, sai kiểu dữ liệu, sai regex ID, chứa query string |
| `404` | `not_found` | `resource not found` — Profile của Owner không tồn tại trong `profiles` (Gateway PUT); Gateway cha không tồn tại (Sensor PUT) |
| `409` | `conflict` | `resource conflict` — Trùng ID nhưng sai lệch metadata; hoặc Gateway đã có Owner khác trong cơ sở dữ liệu |
| `413` | `payload_too_large` | `payload too large` — Kích thước request body vượt quá `ADMIN_MAX_BODY_BYTES` |
| `415` | `unsupported_media_type` | `unsupported media type` — Header `Content-Type` không phải `application/json` hoặc tham số khác `charset=utf-8` |
| `503` | `service_unavailable` | `service unavailable` — Lỗi truy vấn kiểm tra quyền Admin hoặc giao dịch bị hủy / quá thời gian (`AUTHORIZATION_TIMEOUT`) |
| `500` | `internal_error` | `internal server error` — Lỗi kết nối database, commit thất bại, hoặc đồ thị dữ liệu vi phạm tính toàn vẹn (inconsistent graph) |

### An toàn thông tin trong nhật ký (Log Secrecy)

- Hệ thống tuyệt đối không ghi chi tiết lỗi SQL thô, tên ràng buộc khóa ngoại (constraint names), chuỗi kết nối database (DSN), token xác thực hay thông tin bí mật vào log.
- Khi provisioning thất bại, log hệ thống chỉ ghi nhận mã lỗi an toàn (`error_code`) và mã truy vết (`request_id`).
- Các chốt chặn xác thực và kiểm tra quyền Admin luôn thực thi trước khi phân tích nội dung body; các request bị từ chối quyền truy cập không có khả năng thăm dò (probe) parser JSON hay cấu trúc cơ sở dữ liệu bên trong.

---

## 6. Verification: Lệnh và Bằng chứng Kiểm chứng

### 6.1 Điều kiện tiên quyết và bộ lệnh kiểm thử

- **Yêu cầu môi trường**: Go toolchain theo `src/go.mod` (Go 1.27.1), Docker daemon, Python 3 và khả năng tải/build các Docker images đã được pin phiên bản.
- **Thực thi kiểm tra mã nguồn Go** (tại thư mục `src/`) và **chạy các harness kiểm thử** (tại thư mục gốc):

```bash
# Kiểm tra đơn vị và race condition trên toàn bộ mã nguồn Go
(cd src && go test -race -count=1 ./...)
(cd src && go vet ./... && go build ./...)

# Chạy harness kiểm thử tích hợp provisioning trên PostgreSQL cô lập
sh scripts/test-stage2-provisioning.sh

# Chạy kiểm thử hồi quy xác thực và phân quyền bằng Python
python3 -m unittest discover -s scripts/tests -p 'test_stage2_auth.py'
sh scripts/test-stage2-auth.sh
sh scripts/test-stage2-authorization.sh
sh scripts/test-stage2-admin.sh
sh scripts/test-stage2-migrations.sh
sh scripts/test-backend-smoke.sh

# Kiểm tra định dạng và khoảng trắng git
git diff --check
```

### 6.2 Phân biệt ranh giới: Bằng chứng Fixture cô lập (Task 2.4) vs. E2E GoTrue thật (Task 2.7)

Cần phân định rành mạch giữa các kết quả kiểm chứng đã thực hiện trong Task 2.4 và kế hoạch kiểm thử tích hợp toàn diện trong Task 2.7:

1. **Bằng chứng trên Fixture cô lập của Task 2.4 (Đã hoàn thành)**:
   - Toàn bộ các kiểm thử tích hợp của Task 2.4 chạy trên container PostgreSQL/TimescaleDB tạm thời, sử dụng port loopback ngẫu nhiên, thông tin đăng nhập sinh động, và migrations được mount ở chế độ chỉ đọc (`read-only`).
   - Kiểm thử HTTP router và repository thực hiện với tài khoản `iot_backend_app`, bật cờ race detector, chạy tuần tự để tránh tranh chấp dữ liệu giữa các ca kiểm thử.
   - Các token JWT trong harness tích hợp được tạo lập thông qua mock/verifier hoặc kiểm thử xác thực cô lập của `test-stage2-auth.sh`, **chưa bao gồm toàn bộ luồng mạng đầu-cuối qua proxy và dịch vụ GoTrue thật cho toàn bộ Stage 2**.

2. **Kiểm thử E2E GoTrue thật theo kế hoạch Task 2.7 (Đang chờ thực hiện — Pending)**:
   - Theo bản kế hoạch chi tiết [Task 2.7 detail plan](/media/trung/SSD2-Data/Project1_ET3290/docs/backend_plan/task_2.7_detail_plan.md) và tài liệu nghiệm thu [stage-2-task-2.7-acceptance.md](stage-2-task-2.7-acceptance.md) (hiện đang ở trạng thái **kế hoạch dự kiến / pending**), toàn bộ luồng tích hợp Stage 2 sẽ được kiểm chứng thông qua:
     `Client -> Nginx HTTPS -> Envoy (Supabase API Gateway) -> GoTrue` kết hợp với `cmd/server` thật, PostgreSQL/TimescaleDB, và Mosquitto DynSec Ingress Gate.
   - Kịch bản kiểm thử E2E E01–E13 của Task 2.7 sẽ sử dụng 6 tài khoản GoTrue thật được tạo lập qua Admin API (Platform Admin, Owner A, Owner B, Operator, Viewer, Non-member) để xác minh lại luồng tạo Gateway/Sensor của Task 2.4, tính cô lập giữa các Owner, và việc chặn đứng truy cập trái phép từ các vai trò không phải Admin.

---

### 6.3 Bằng chứng kiểm chứng lịch sử của Task 2.4 (Historical Evidence)

| Giai đoạn / Artifact | Kết quả kiểm chứng đã được xác nhận |
|---|---|
| Task 2.4.6, `/tmp/opencode/task246-unit.log` | Bộ kiểm thử Go unit tests, race detector và coverage đạt `PASS` |
| Task 2.4.6, `task246-integration.log`, `task246-integration-coverage.log` | PostgreSQL provisioning và router đạt `PASS`: kiểm chứng atomic graph, rollback/fault injection, retry lũy quyền, xử lý đồng thời, timeout và recheck quyền admin |
| Task 2.4.6, `task246-admin.log`, `task246-authorization.log`, `task246-migrations.log`, `task246-smoke.log` | Kiểm thử Admin, authorization, migration và container smoke đạt `PASS` |
| Task 2.4.7, `/tmp/opencode/task247-auth-final.log` | Xác thực GoTrue/proxy provisioning và hồi quy Auth đạt `PASS`; bộ test Python harness đạt **23 tests PASS** |
| Task 2.4.7, `task247-authz.log`, `task247-provisioning.log` | Kiểm thử hồi quy Authorization và provisioning đạt `PASS` |

*Ghi chú về độ phủ mã nguồn lịch sử*: Task 2.4.6 ghi nhận độ phủ kết hợp (unit + integration) riêng cho phân hệ feature là **89.8% (300/334 statements)** dựa trên các profile tại `/tmp/opencode`. Đây là bằng chứng phạm vi tính năng cục bộ, không phải độ phủ của toàn bộ dự án.

---

### 6.4 Rà soát độc lập nghiệm thu Task 2.4.8b (2026-10-03)

Đã hoàn thành rà soát độc lập toàn diện mã nguồn so với baseline commit `887b281968845e8dd05e2e29ca5819cb26412637` trên nhánh `develop`. Mã nguồn đã được commit và CI xác nhận xanh tại commit `abcfbbadf563a45da87db9d84a8b7ca6aa996864`.

| Nội dung kiểm tra | Kết quả thực tế |
|---|---|
| `go test -race -count=1 -coverprofile=... ./...` | `PASS` trên toàn bộ packages |
| `go vet ./...`, `go build ./...`, `gofmt -l .`, `git diff --check` | `PASS`, định dạng chuẩn xác, không có lỗi cú pháp hay khoảng trắng thừa |
| Cross-build với `CGO_ENABLED=0` | `PASS` cho cả hai mục tiêu: server `linux/amd64` và gateway `linux/arm` (GOARM=7) |
| `golangci-lint run --timeout=5m` (bản **v2.14.0**, Go 1.27.1) | `PASS` toàn bộ, không có cảnh báo linter nào |
| `sh scripts/test-stage2-provisioning.sh` (race + coverage) | `PASS`, cả hai suite kiểm thử tích hợp chạy thực tế, không có bài kiểm tra nào bị `SKIP` |
| Các harness Authorization, Admin, Auth/GoTrue, Migrations, Backend Smoke | Đều đạt kết quả `PASS` |
| `python3 -m unittest discover -s scripts/tests` | Toàn bộ **23 tests PASS** |
| Cú pháp toàn bộ shell scripts (`sh -n` hoặc `bash -n`) | `PASS` |
| `actionlint v1.7.7 -shellcheck='' .github/workflows/ci.yml` | `PASS` |
| Ruff linter (`--select E9,F`) trên các tệp Python test | `PASS` |

**Số liệu độ phủ kiểm thử chi tiết (Test Coverage)**:
Độ phủ được tính bằng cách hợp nhất (union) giữa `unit.out` và `integration.out` theo từng khối lệnh thực thi (`numStatements`), tính theo trọng số phát biểu thay vì lấy trung bình cộng tỷ lệ phần trăm:
- **Các tệp mã nguồn nghiệp vụ mới của Task 2.4**: Đạt **89.88% (302/336 statements)** (bao gồm: `mapper`, `model`, `service`, 3 adapters PostgreSQL, 2 HTTP handlers, và `strict parser`).
- **Tập hợp tất cả các tệp có sửa đổi trong Task 2.4**: Đạt **81.64% (467/572 statements)** (bao gồm cả mã nguồn cũ trong `identifier.go`, `router.go`, `config.go`, và `cmd/server/main.go`).
- **Toàn bộ mã nguồn Go trong repository**: Đạt **85.91% (762/887 statements)** (không bao gồm các gói entrypoint `cmd/server` và `cmd/gateway` vốn được kiểm chứng thông qua smoke tests và E2E process riêng biệt).

---

## 7. Trạng thái Nghiệm thu và Kế hoạch Chuyển giao

1. **Trạng thái mã nguồn**:
   - Toàn bộ mã nguồn, cấu hình, kiểm thử và tài liệu của Task 2.4 đã được sáp nhập thành công vào nhánh phát triển chính và đã vượt qua toàn bộ các kiểm tra tích hợp liên tục (CI) từ commit `abcfbbadf563a45da87db9d84a8b7ca6aa996864`.
   - Các công việc tiếp theo về quản lý thông tin xác thực MQTT của Gateway và Ingress Gate dựa trên DynSec đã được hoàn thành độc lập trong Task 2.5 và Task 2.6.

2. **Công việc chuyển giao cho Task 2.7**:
   - **Quy trình vận hành thành viên (Membership Operations)**: Bổ sung công cụ vận hành được bảo vệ dành cho người quản trị hạ tầng (dự kiến `scripts/manage-gateway-membership.sh`) để thực hiện cấp phát, thay đổi vai trò (`operator`, `viewer`) và thu hồi quyền thành viên trong `user_gateways` một cách an toàn, có ghi nhật ký kiểm toán (audit log).
   - **Kiểm thử E2E toàn diện Giai đoạn 2**: Triển khai kịch bản E2E từ E01 đến E27 thông qua harness mới (`scripts/test-stage2-e2e.sh`), kết nối toàn bộ hệ thống thực tế với Supabase Auth (GoTrue), Nginx, Envoy, Go backend, PostgreSQL/TimescaleDB và Mosquitto DynSec.
   - Tài liệu nghiệm thu tổng kết Giai đoạn 2: [stage-2-task-2.7-acceptance.md](stage-2-task-2.7-acceptance.md) (hiện là **kế hoạch dự kiến / đang chờ thực hiện — pending**; kế hoạch chi tiết tham chiếu tại `docs/backend_plan/task_2.7_detail_plan.md`).

3. **Ranh giới bất biến cần duy trì**:
   - Không tự ý mở thêm bất kỳ endpoint nào ngoài hai endpoint PUT đã đăng ký.
   - Không nới lỏng chính sách phân quyền: Gateway Owner không có quyền ghi Sensor; không hỗ trợ chuyển quyền sở hữu; không cung cấp public signup hay self-claim; và không kích hoạt các lệnh điều khiển thiết bị khi chưa triển khai đầy đủ phân hệ command execution.

---

### Tài liệu tham chiếu liên quan

- [Kiến trúc Xác thực — Stage 2 Task 2.2](stage-2-task-2.2-authentication.md)
- [Quản lý Tài khoản Tập trung — Stage 2 Task 2.2A](stage-2-task-2.2A-centralized-accounts.md)
- [Phân quyền Người dùng và Gateway — Stage 2 Task 2.3](stage-2-task-2.3-authorization.md)
- [Hạ tầng Mosquitto Runtime — Stage 2 Task 2.5](stage-2-task-2.5-mosquitto-runtime.md)
- [Quản lý Thông tin xác thực MQTT và DynSec — Stage 2 Task 2.6](stage-2-task-2.6-mqtt-credentials.md)
- [Báo cáo Nghiệm thu Task 2.6](stage-2-task-2.6-acceptance.md)
- [Kế hoạch E2E và Nghiệm thu Giai đoạn 2 — Task 2.7 (Pending)](stage-2-task-2.7-acceptance.md) *(Kế hoạch nguồn: `docs/backend_plan/task_2.7_detail_plan.md`)*
- [Quy chuẩn Dự án — AGENTS.md](../../AGENTS.md)
