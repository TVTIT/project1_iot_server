# Giai đoạn 2 — Task 2.2A: Tài khoản quản lý tập trung

## 1. Phạm vi và cấu hình

MVP không mở public signup. Platform admin/operator hạ tầng tin cậy tạo human user
thông qua công cụ quản trị Supabase Auth; role Gateway `operator` không có quyền
tạo tài khoản. Hiện tại Go backend **chưa triển khai bất kỳ route CRUD quản lý tài
khoản người dùng nào** (`/v1/admin/users*`). Tài liệu mục tiêu kiến trúc và các
rào chắn bảo vệ cho năng lực này trong tương lai đã được xác định tại
[docs/backend/platform-admin-account-management-goals.md](platform-admin-account-management-goals.md),
nhưng đây thuần túy là định hướng tương lai, không phải API hiện có trong mã nguồn.
Hệ thống cũng không có custom admin UI, self-claim Gateway hoặc command API trong
task này.

Repository-root `.env` là nguồn cấu hình deployment:

```dotenv
GOTRUE_DISABLE_SIGNUP=true
```

Compose mapping `${GOTRUE_DISABLE_SIGNUP:-true}` vào GoTrue `v2.196.0`:
thiếu/để trống biến vẫn mặc định chặn signup. Theo chính sách MVP phải giữ
`true`; đặt `false` là mở lại đăng ký, không phải tùy chọn triển khai được
chấp nhận nếu chưa có quyết định thay đổi chính sách. Go business API vẫn dùng
JWT contract [Task 2.2](stage-2-task-2.2-authentication.md).

Với phiên bản đang pin, `POST /auth/v1/signup` qua Nginx → Envoy → GoTrue trả
**422**, `error_code=signup_disabled`, không tạo user/profile. Đây là response
GoTrue, không phải safe error envelope/401 của Go business API.

Chính sách chặn đăng ký công khai được thực thi nghiêm ngặt tại tầng GoTrue /
Supabase API Gateway; việc ẩn nút signup trên giao diện Flutter hay web client
chỉ thuần túy mang tính công thái học UI và không thay thế cho việc khóa tại
backend. Không mở port Auth trực tiếp ra ngoài Internet.

## 2. Quy trình operator tạo tài khoản

### 2.1. Chuẩn bị

- Dùng Supabase Studio qua VPN/trusted management network/Cloudflare Access,
  hoặc protected operator tool gọi Auth Admin API qua Supabase API Gateway.
  Task này không triển khai thêm Studio/public management route.
- Operator có quyền quản trị triển khai và credential phù hợp; không gửi
  service-role key cho Flutter, web client hoặc Gateway. Human platform admin
  vẫn đăng nhập bằng access token `role=authenticated`.
- Bảo vệ `.env`/credential files, không bật `set -x`, không dùng key/password
  trong URL hoặc command arguments, không copy request/response vào ticket.
- Tuân thủ cấu hình bảo mật an toàn: tham chiếu qua biến môi trường hoặc fixture
  bảo vệ, tuyệt đối không in mật khẩu, khóa bí mật thật, hay danh sách tài khoản
  thử nghiệm dạng văn bản thô (plaintext) ra tài liệu hay log.
- Áp dụng application migrations và compatibility migration để trigger tạo
  `profiles` từ `auth.users` hoạt động.

### 2.2. Tạo user có chủ đích (Auth Admin API vs Human JWT)

Trong Studio dùng chức năng quản trị Auth Users mà phiên bản đang triển khai
hỗ trợ. Nếu dùng Admin API, operator tool gửi request sau qua gateway:

```http
POST /auth/v1/admin/users
apikey: <protected-service-role-key>
Authorization: Bearer <protected-service-role-key>
Content-Type: application/json
```

```json
{
  "email": "<user-email-from-protected-operator-input>",
  "password": "<unique-password-from-protected-operator-input>",
  "email_confirm": true
}
```

Đây là mô tả contract, **không phải lệnh để paste secret vào terminal**.
Operator tool nhận credentials qua protected file/environment hoặc stdin;
không in payload/keys. Fixture local sử dụng `email_confirm=true` có chủ đích.
Điều này không chứng minh user sở hữu email; với deployment cần chứng minh
email thì chọn quy trình invite/verification và kiểm chứng SMTP/redirect.

**Phân định giữa Auth Admin API và Human JWT:**
- Endpoint `POST /auth/v1/admin/users` là giao diện quản trị đặc quyền của
  Supabase Auth, bắt buộc phải sử dụng `service_role` key của hạ tầng thông qua
  Supabase API Gateway. Go backend tuyệt đối không phát tán hay chia sẻ
  `service_role` key này cho client hay Gateway.
- Mọi token JWT phát hành cho con người (kể cả tài khoản thuộc `platform_admins`)
  đều mang claim `role=authenticated`. Nếu dùng human JWT này để gọi
  `POST /auth/v1/admin/users`, GoTrue sẽ từ chối thẳng thừng với mã lỗi
  **`403 Forbidden`**. Human token không bao giờ thay thế được service key cho
  thao tác quản trị Auth Admin API.

GoTrue pin trả `200` và **user object có `id` ở top level**, không phải
signup session `{"user": ...}` và không hứa trả access/refresh token. Kiểm tra
UUID, `profiles.id` tương ứng; account mới không tự có row trong
`platform_admins` hoặc `user_gateways`. Không sửa metadata để tự cấp admin.

User đăng nhập qua `/auth/v1/token?grant_type=password`; refresh qua
`/auth/v1/token?grant_type=refresh_token`. Không đưa password hoặc refresh token
vào URL.

### 2.3. Admin đầu tiên, phân định vai trò và Gateway membership

#### 2.3.1. Rào chắn script bootstrap platform admin đầu tiên

Operator hạ tầng tin cậy tạo account đầu tiên mà không cần có human platform
admin trước đó. Sau khi có profile, bootstrap UUID bằng
`scripts/bootstrap-platform-admin.sh` theo
[quy trình Task 2.2](stage-2-task-2.2-authentication.md#6-bootstrap-platform-admin-hiện-có).

Script chạy ở tầng hạ tầng bằng PostgreSQL administrator credential (`psql`),
không dùng backend role hay service-role token. Script chứa cơ chế bảo vệ
chặt chẽ:
1. Thiết lập transaction advisory lock `pg_advisory_xact_lock(3290, 21)` để
   chống tranh chấp khi chạy song song.
2. Kiểm tra `profiles.id` bắt buộc phải tồn tại trong cơ sở dữ liệu.
3. **Rào chắn first-admin duy nhất:** Script kiểm tra:
   `SELECT NOT EXISTS (SELECT 1 FROM platform_admins WHERE user_id <> :'platform_admin_user_id'::UUID)`.
   Nếu đã tồn tại bất kỳ platform admin nào khác trong bảng `platform_admins`,
   lệnh sẽ lập tức bị chặn với exception:
   `'Cannot bootstrap an additional platform admin'`.
4. Trường hợp chạy lại với cùng UUID của admin hiện tại, mệnh đề
   `ON CONFLICT (user_id) DO NOTHING` cho phép replay an toàn và idempotent.

Do đó, script bootstrap chỉ có thể dùng để khởi tạo platform admin đầu tiên
cho hệ thống; tuyệt đối không thể lạm dụng để tự ý cấp quyền platform admin
cho các tài khoản khác khi hệ thống đã có admin.

#### 2.3.2. Phân định vai trò: profiles, platform_admins và user_gateways

Hệ thống quản lý định danh và quyền hạn theo ba tầng tách biệt hoàn toàn:
- **`profiles`:** Khóa chính `id` tham chiếu `auth.users.id`. Tự động tạo qua
  trigger cơ sở dữ liệu `on_auth_user_created` khi tài khoản được tạo tại
  Supabase Auth. Bản ghi `profiles` thuần túy lưu trữ thông tin cá nhân cơ bản;
  việc có profile **không cấp bất kỳ quyền hạn nào** (cả quyền admin lẫn quyền
  truy cập Gateway).
- **`platform_admins`:** Lưu trữ định danh quản trị viên nền tảng (`user_id`
  tham chiếu `profiles.id`). Go backend thẩm định quyền platform admin qua truy
  vấn trực tiếp bảng này cho các thao tác quản trị tài nguyên toàn hệ thống
  (như cấp phát Gateway/Sensor, quản lý MQTT credentials tại `/v1/admin/*`).
  Platform admin **không tự động bypass** quyền đọc dữ liệu nghiệp vụ: nếu
  admin gọi `GET /v1/gateways`, backend chỉ trả về danh sách Gateway mà admin
  được gán trong `user_gateways` (nếu chưa gán sẽ nhận `items: []`).
- **`user_gateways`:** Lưu trữ quan hệ thành viên và vai trò trên từng Gateway
  cụ thể với ràng buộc `role IN ('owner', 'operator', 'viewer')`. Go backend
  thẩm định quyền truy cập dữ liệu Gateway (telemetry, lịch sử, sensor, media,
  Digital Twin) duy nhất qua bảng này.

Tạo tài khoản qua Auth Admin API chỉ tạo bản ghi trong `auth.users` và
`profiles`; số lượng bản ghi trong `platform_admins` và `user_gateways` của user
mới luôn là **0**.

#### 2.3.3. Trạng thái provisioning và công cụ quản lý membership

- **Cấp phát Gateway (Đã triển khai - Task 2.4):** API provisioning Gateway và
  Sensor đã hoàn thành (`PUT /v1/admin/gateways/{gateway_id}` và
  `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`). Khi platform admin
  tạo mới một Gateway, hệ thống tự động gán tài khoản chủ sở hữu được chỉ định
  vào `user_gateways` với `role = 'owner'` một cách nguyên tử (atomic) trong
  cùng transaction.
- **Công cụ quản lý membership (Đang được lên kế hoạch, chưa triển khai):**
  Quy trình động cho phép gán/hạ quyền `operator`, `viewer` hoặc thu hồi
  (revoke) membership của các tài khoản khác trên Gateway hiện **chưa có script
  hoặc API thực thi sẵn trong mã nguồn**. Năng lực này hiện là thiết kế kế
  hoạch thuộc Task 2.7 (Bước 2.7.1, dự kiến triển khai dưới dạng công cụ quản trị
  bảo vệ `scripts/manage-gateway-membership.sh` với advisory lock, kiểm tra
  quyền platform admin của actor, audit journal bất biến và không cho phép
  chuyển nhượng hoặc xóa owner gốc).
- **Không có CRUD user trong Go:** Như đã nêu, tài liệu
  `docs/backend/platform-admin-account-management-goals.md` đề xuất các endpoint
  tương lai nhưng Go backend hiện chưa mở route nào; không dùng đăng ký hay
  biết `gateway_id` làm căn cứ cấp quyền.

### 2.4. Invite bằng email

Invite dùng công cụ quản trị Auth được hỗ trợ, vẫn phải chạy qua gateway và
bảo vệ service credential. Luồng email invite đòi hỏi hệ thống SMTP hoạt động
tin cậy, domain public HTTPS và redirect allowlist được cấu hình đầy đủ.
Trong phạm vi hiện tại, **hạ tầng SMTP và quy trình invite chưa được kiểm chứng
đạt chuẩn (not qualified)**; do đó không được phụ thuộc vào email invite trên
môi trường triển khai thực tế khi chưa kiểm định, và tuyệt đối không mở lại
public signup để thay thế cho invite.

## 3. Áp dụng vào deployment hiện có

Lượt thực hiện task chỉ sửa source/template và chạy stack test isolated;
không sửa `.env` local hoặc recreate stack đang chạy.

Operator tự kiểm tra `.env` được bảo vệ, thêm/giữ `GOTRUE_DISABLE_SIGNUP=true`,
rồi từ repo root chạy khi đã sẵn sàng áp dụng:

```bash
docker compose up -d --no-deps supabase-auth
```

`up` sẽ recreate nếu environment đổi; `docker compose restart` không cập nhật
environment. Chỉ dùng `--no-deps` khi PostgreSQL/roles của deployment đã sẵn
sàng; fresh deployment vẫn phải theo quy trình Compose/migrations đầy đủ.
Sau đó kiểm tra signup denial và login/refresh bằng account được operator tạo.
Không chạy isolated harness với database URL trỏ tới deployment.

Không xóa database/volume. Đóng signup không xóa các account prototype, không
thu hồi access/refresh sessions, không sửa signing secret đã lộ. Rà soát user
cũ và xử lý session/secret rotation là thao tác bảo mật riêng. Script
`gen-keys.py` vẫn có public fallback; không coi Task 2.2A là đã sửa finding đó.

## 4. Verification và bằng chứng TDD

Các lệnh bắt đầu từ repo root; không source `.env`:

```bash
python3 -m unittest discover -s scripts/tests -p 'test_stage2_auth.py'
sh scripts/test-stage2-auth.sh
sh scripts/test-backend-smoke.sh
sh scripts/test-stage2-admin.sh
sh scripts/test-stage2-migrations.sh
(cd src && go test -v -race -coverprofile=coverage.out ./...)
(cd src && go vet ./...)
(cd src && golangci-lint run --timeout=5m)
```

Go/linter và prerequisite Docker/Python theo Task 2.2. CI hiện tại duy trì
**9 jobs** độc lập (`auth-integration` chạy harness cập nhật, không thêm job
hoặc schema mới).

| Guarantee | Test / evidence |
|---|---|
| Signup mặc định đóng trong template | Python config regression cho Compose + `.env.example` |
| Không nhầm proxy lỗi/input lỗi với signup denial | Unit test yêu cầu `422` + `signup_disabled` |
| Signup bị chặn không tạo dữ liệu | HTTP qua Nginx/Envoy + query DB isolated: user/profile/membership/admin đều 0 |
| Admin API không được parse như signup session | Unit test top-level nonzero user UUID; từ chối response/error sai |
| Tạo user không tự cấp Gateway/admin | DB probe sau Admin API: đúng 1 user + 1 profile, 0 membership/admin |
| Human token không tạo account qua Admin API | HTTP `403` + DB probe không tạo user |
| JWT/Auth cũ không regression | Admin-created user login/refresh, verifier/negative tokens, stubs 401/501, WS header, DB admin guard 204/403 |
| Không lộ credentials và không dùng local DB | Log canary check; fixture credentials ngẫu nhiên, loopback ports, resources riêng và cleanup |

**RED đã quan sát:** Python suite lỗi vì config còn mở signup/chưa có helpers;
real Docker harness thất bại vì `/auth/v1/signup` trả `200` và GoTrue tạo user.
**GREEN đã quan sát:** cùng harness sau khi disable signup và chuyển sang Admin
API xác nhận signup denial, DB rows đúng, login/refresh và admin guard pass.
Không tạo checkpoint commit tự động; các thay đổi chờ người dùng review/commit.

Verification local sau thay đổi: **11 Python tests PASS**, Auth integration,
smoke, admin DB integration và migration regression **PASS**; Go race tests,
`go vet ./...`, `go build ./...` **PASS**, coverage Go tổng **81,4%**.
Linter chạy bằng
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run --timeout=5m`
trong `src/`, kết quả **0 issues**. Actionlint và Ruff check/format pass;
Compose render từ `.env.example` và fixture bỏ biến signup đều xác nhận
`GOTRUE_DISABLE_SIGNUP=true`, không đọc `.env` deployment.

**Trạng thái kiểm thử mở rộng và CI hiện tại:**
- **Stage 2 E2E Testing:** Kế hoạch kiểm thử tích hợp E2E toàn diện cho Giai
  đoạn 2 đang được xây dựng theo [Task 2.7 detail plan](../../docs/backend_plan/task_2.7_detail_plan.md)
  (kịch bản E01–E27 kết nối GoTrue, Go API, PostgreSQL, DynSec và Mosquitto).
- **CI Job mới:** Job CI thứ 10 (`stage2-e2e`) phục vụ kiểm thử tích hợp đầu-cuối
  **hiện chưa được triển khai** trên CI workflow; hệ thống CI hiện hữu vẫn đang
  chạy 9 jobs đã được phê duyệt.
- Mọi script kiểm thử và tài liệu phải tuân thủ nghiêm ngặt nguyên tắc an toàn:
  sử dụng credential giả lập tự sinh trong fixture isolated, tham chiếu qua cấu
  hình an toàn, không in mật khẩu thật và không sao chép danh sách tài khoản thử
  nghiệm dạng văn bản thô ra artifact.

Các kiểm tra chỉ chứng minh stack isolated, không chứng nhận deployment thật
đã đóng signup. SMTP invite, User–Gateway business API và device command
authorization chưa nằm trong bằng chứng Task 2.2A. Kết quả Task 2.0 được giữ
nguyên tại tài liệu lịch sử, không sửa lại spike dựa trên chính sách mới.
`scripts/spikes/jwt_contract.py` còn dùng signup theo PoC cũ, nên không dùng
nó làm acceptance test trên deployment đã đóng signup; dùng harness Task 2.2A.
