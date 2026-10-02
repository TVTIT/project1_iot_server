# Giai đoạn 2 — Task 2.2A: Tài khoản quản lý tập trung

## 1. Phạm vi và cấu hình

MVP không mở public signup. Platform admin/operator hạ tầng tin cậy tạo hoặc
mời human user qua công cụ quản trị Supabase Auth; role Gateway `operator`
không có quyền tạo tài khoản. Không thêm Go user-management API, custom admin
UI, self-claim Gateway hoặc command API trong task này.

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

Không cần chặn signup bằng Nginx nếu GoTrue đã thực thi đúng chính sách.
Không mở port Auth trực tiếp, không chỉ ẩn nút signup trên Flutter.

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
- Áp dụng application migrations và compatibility migration để trigger tạo
  `profiles` từ `auth.users` hoạt động.

### 2.2. Tạo user có chủ đích

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

GoTrue pin trả `200` và **user object có `id` ở top level**, không phải
signup session `{"user": ...}` và không hứa trả access/refresh token. Kiểm tra
UUID, `profiles.id` tương ứng; account mới không tự có row trong
`platform_admins` hoặc `user_gateways`. Không sửa metadata để tự cấp admin.

User đăng nhập qua `/auth/v1/token?grant_type=password`; refresh qua
`/auth/v1/token?grant_type=refresh_token`. Không đưa password hoặc refresh token
vào URL. Human token không được dùng thay service key gọi Auth Admin API:
harness xác nhận request tạo user bằng human token bị `403`.

### 2.3. Admin đầu tiên và Gateway membership

Operator hạ tầng tin cậy tạo account đầu tiên mà không cần có human platform
admin trước đó. Sau khi có profile, bootstrap UUID bằng
`scripts/bootstrap-platform-admin.sh` theo [quy trình Task 2.2](stage-2-task-2.2-authentication.md#6-bootstrap-platform-admin-hiện-có).
Script dùng PostgreSQL administrator credential, không dùng backend role hay
service-role token. Không tự sửa schema hoặc thêm endpoint tự cấp admin.

Account thường được tạo tương tự nhưng không chạy bootstrap admin. Quyền
Gateway do platform admin cấp riêng; Task 2.4 dự kiến gán owner atomically khi
provision Gateway. Repository chưa có API provisioning/membership hoàn chỉnh;
không dùng signup hoặc biết `gateway_id` làm bằng chứng ownership.

### 2.4. Invite bằng email

Invite dùng công cụ quản trị Auth được hỗ trợ, vẫn phải chạy qua gateway và
bảo vệ service credential. Trước khi dùng thật, kiểm chứng SMTP, public HTTPS
URL và redirect allowlist, nhận thư và hoàn tất login. Luồng email invite
**chưa được kiểm chứng bởi task này**; không mở signup để thay cho invite.

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

Go/linter và prerequisite Docker/Python theo Task 2.2. CI `auth-integration`
chạy harness cập nhật, không thêm job hoặc schema mới.

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

Các kiểm tra chỉ chứng minh stack isolated, không chứng nhận deployment thật
đã đóng signup. SMTP invite, User–Gateway business API và device command
authorization chưa nằm trong bằng chứng Task 2.2A. Kết quả Task 2.0 được giữ
nguyên tại tài liệu lịch sử, không sửa lại spike dựa trên chính sách mới.
`scripts/spikes/jwt_contract.py` còn dùng signup theo PoC cũ, nên không dùng
nó làm acceptance test trên deployment đã đóng signup; dùng harness Task 2.2A.
