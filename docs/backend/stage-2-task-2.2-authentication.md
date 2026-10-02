# Giai đoạn 2 — Task 2.2: Xác thực và platform-admin guard

**Cập nhật:** 2026-10-02. Nguồn đối chiếu: `src/internal/auth/`,
`src/internal/httpserver/router.go`, `src/internal/config/config.go`,
`docker-compose.yml`, `.env.example` và các harness trong `scripts/`.

## 1. Phạm vi đã triển khai

Go xác minh access token của người dùng Supabase tại chỗ, chuyển danh tính đã
xác minh thành `Principal`, và cung cấp guard tra quyền platform admin trong
PostgreSQL. Không gọi GoTrue cho từng business request, không dùng JWKS trong
contract HS256 hiện tại. Router từ chối khởi tạo nếu thiếu dependency bắt buộc,
kể cả interface chứa typed nil; cấu hình JWT sai làm backend dừng khi startup.

```text
Authorization: Bearer <access_token>
  -> HS256 verifier -> Principal -> authenticated stub (501)
                              -> admin group -> PostgreSQL membership guard
```

Chưa có production admin handler. Guard được kiểm thử bằng route chỉ có trong
test. Chưa triển khai User–Gateway authorization trong business handler,
historical queries, WebSocket upgrade/fan-out hay nghiệp vụ Digital Twin.
`501` chỉ chứng minh request vượt qua authentication tới stub, không chứng minh
người dùng có quyền trên Gateway và không phải tính năng đã hoàn thành. Backend
role có `BYPASSRLS`, nên các handler tương lai phải tự kiểm tra `user_gateways`
trước khi đọc/ghi dữ liệu; xem [Task 2.1](stage-2-task-2.1-migrations.md).

## 2. JWT contract thực tế

Verifier sử dụng `github.com/golang-jwt/jwt/v5` v5.3.0, strict base64 decoding và
chỉ chấp nhận token khớp tất cả điều kiện:

| Thành phần | Điều kiện |
|---|---|
| Algorithm/signature | Chỉ `HS256`, kiểm tra chữ ký với secret cấu hình; không chấp nhận `none`, HS384/HS512 hoặc RSA |
| `iss` | Bắt buộc, bằng chính xác `SUPABASE_JWT_ISSUER`; không tự sửa scheme, hostname, path hoặc trailing slash |
| `aud` | Bắt buộc chứa `authenticated`; chấp nhận chuỗi `authenticated` hoặc array có phần tử này, không yêu cầu array chỉ có một audience |
| `role` | Bằng chính xác `authenticated` |
| `exp` | Bắt buộc; token hết hạn bị từ chối với leeway cấu hình |
| `iat` | Bắt buộc; thời điểm phát hành không được nằm trong tương lai ngoài leeway |
| `nbf` | Không bắt buộc; nếu có, token chưa có hiệu lực ngoài leeway bị từ chối |
| `sub` | Parse được bằng `uuid.Parse`, không phải UUID toàn số 0 |
| Kích thước | Token không rỗng, tối đa 16 KiB (16 × 1024 bytes) |
| Clock skew | `SUPABASE_JWT_CLOCK_SKEW`, mặc định `30s`, cho phép `0s` đến `5m` |

Secret phải có ít nhất 32 bytes sau khi kiểm tra khoảng trắng, nhưng độ dài
không chứng minh entropy. Không dùng secret mẫu hoặc placeholder để triển khai.
Token `anon` và `service_role` không phải human access token, dù cùng signing
secret. `service_role` là quyền kỹ thuật Supabase, **không phải** human platform
admin; không cấp key này cho Gateway hay Flutter. JWT metadata do client sửa
không thay thế được membership trong database.

### Header và Principal

Chỉ nhận một header `Authorization: Bearer <access_token>`; scheme không phân
biệt hoa/thường. Từ chối nhiều header, dấu phẩy, token rỗng, khoảng trắng/tab/newline
trong credential. Query string hoặc request body không phải nguồn token: chỉ
gửi `?token=...` hoặc token trong body vẫn bị `401`. Không đưa token vào URL.

`Principal` gồm `UserID uuid.UUID` và `Role string`. `SetPrincipal` lưu giá trị
typed trong Gin context dưới key nội bộ `authenticated_principal`;
`PrincipalFrom` trả `(Principal, bool)`. Không giữ raw token trong Principal và
không truyền toàn bộ unverified claims sang nghiệp vụ.

## 3. Issuer và cấu hình

Compose lấy cấu hình từ repository-root `.env` (không commit), mapping:

| Biến trong `.env` | Biến trong container / ý nghĩa |
|---|---|
| `JWT_SECRET` | GoTrue: `GOTRUE_JWT_SECRET`; backend: `SUPABASE_JWT_SECRET`; cùng secret HS256 |
| `GOTRUE_JWT_ISSUER` | GoTrue: cùng tên; backend: `SUPABASE_JWT_ISSUER` |
| `SUPABASE_JWT_AUDIENCE` | GoTrue: `GOTRUE_JWT_AUD`; backend: cùng tên; bắt buộc `authenticated` |
| `SUPABASE_JWT_CLOCK_SKEW` | Backend, mặc định `30s` |
| `AUTHORIZATION_TIMEOUT` | Backend, timeout lookup admin, mặc định `2s`, phải dương |
| `JWT_EXPIRY` | GoTrue `GOTRUE_JWT_EXP`, mặc định `3600` giây |
| `SUPABASE_PUBLIC_URL` | Public URL cho Supabase; **không thay thế** issuer explicit |
| `GOTRUE_DISABLE_SIGNUP` | GoTrue; mặc định `true`, giữ `true` theo mô hình quản lý tập trung |
| `DATABASE_URL` | Backend kết nối bằng `iot_backend_app`, không dùng superuser |

Local theo `.env.example`: `SUPABASE_PUBLIC_URL=http://localhost`,
`GOTRUE_JWT_ISSUER=http://localhost/auth/v1`. Production dùng public HTTPS URL
đã chọn, ví dụ sanitized `https://supabase.example.com/auth/v1`, và cấu hình
issuer **giống hệt** ở GoTrue và backend. Issuer là định danh token, không phải
địa chỉ nội bộ `http://supabase-envoy:8000`. Nginx vẫn chuyển Auth/Storage qua
Supabase API Gateway, không bypass Envoy.

Nếu chạy Go trực tiếp thay vì Compose, phải export `SUPABASE_JWT_SECRET`,
`SUPABASE_JWT_ISSUER`, `SUPABASE_JWT_AUDIENCE`, `DATABASE_URL` và cấu hình tùy
chọn trước khi chạy `(cd src && go run ./cmd/server)`. Go không tự đọc `.env`
hay tự mapping `GOTRUE_JWT_ISSUER`; mapping đó nằm trong Compose.

Nếu chỉ cập nhật issuer/audience, recreate Auth/backend bằng `docker compose up -d --build
supabase-auth backend` từ repo root. Token phát hành trước khi cấu hình issuer
(Task 2.0 quan sát thiếu `iss`) không được bỏ qua kiểm tra: refresh hoặc đăng
nhập lại để nhận token mới. Đổi issuer/secret không phải bằng chứng đã thu hồi
refresh sessions; phải xử lý session lifecycle riêng.
Nếu rotate signing secret, phải đồng bộ cả API keys và các dịch vụ dùng chúng
(bao gồm Envoy/Storage); lệnh recreate Auth/backend trên không đủ cho rotation.

### Signing secret và API keys

Sinh secret mới bằng `openssl rand -hex 32`, lưu kín vào `.env` thay cho
`<random-signing-secret>`; không commit, không dùng `set -x`, không chia sẻ
terminal output chứa secret. Checkout/`.env` phải được bảo vệ quyền truy cập;
mode `0600` cho `.env` trên filesystem hỗ trợ Unix permissions.

`scripts/gen-keys.py` hiện ưu tiên CLI argument, rồi biến môi trường
`JWT_SECRET`, rồi đọc `.env`, cuối cùng có **public fallback secret**. Script
in `ANON_KEY`/`SERVICE_ROLE_KEY` ra stdout. Không truyền secret qua argv vì có
thể lộ qua process listing/history; chạy không cấu hình có thể sinh key bằng
fallback đã biết. Đây **không phải workflow production an toàn** cho tới khi
script được sửa/kiểm tra riêng. Không có thay đổi script hay xác nhận đã rotate
secret trong Task tài liệu này. Nếu đang dùng sample secret, cần xử lý bảo mật
riêng, đồng bộ signing secret/API keys/dịch vụ và xác minh lại sessions; không
giả định thay secret tự thu hồi refresh tokens.

## 4. Route matrix hiện tại

| Method | Route | Policy | Response hiện tại |
|---|---|---|---|
| GET | `/healthz` | Public | `200`, text `OK` |
| GET | `/readyz` | Public, ping PostgreSQL có timeout | `200 {"status":"ready"}` hoặc safe error `503` |
| GET | `/v1/health` | Public | `200`, status/service/version |
| GET | `/v1/telemetry/history` | Human Bearer JWT | `401` nếu invalid/missing; `501` nếu valid |
| GET | `/v1/ws` | Human Bearer JWT | `401` nếu invalid/missing; `501` nếu valid, kể cả upgrade request |
| GET | `/v1/digital-twins` | Human Bearer JWT | `401` nếu invalid/missing; `501` nếu valid |

`/v1/admin` là group chứa authentication rồi `PlatformAdminMiddleware`, chưa
đăng ký endpoint production nào. Request tới admin path chưa có handler là
`404`, không phải probe membership. Media upload, Digital Twin detail/write,
desired-state và command routes trong thiết kế MVP chưa được đăng ký.
Một method chưa đăng ký trên path đã có method khác có thể trả `405` thay vì
`404`, ví dụ `POST /v1/digital-twins`. Handler admin tương lai phải đăng ký qua `groups.admin`; tạo group khác cùng
prefix không kế thừa policy trong Gin.

### Guard PostgreSQL fail closed

`NewPostgresPlatformAdminChecker` dùng parameterized SQL:

```sql
SELECT EXISTS (
    SELECT 1 FROM platform_admins WHERE user_id = $1
)
```

`$1` là `Principal.UserID` đã xác minh, không phải ID do body/query chỉ định.
Guard yêu cầu Principal (`401` nếu thiếu), lookup membership mỗi request và
giới hạn bằng `AUTHORIZATION_TIMEOUT` không kéo dài parent deadline. Không có
membership trả `403`; database error, cancellation hoặc timeout trả `503`,
không chạy downstream handler. Không dùng cache stale hoặc bypass khi DB lỗi.
Timer lookup được cancel trước downstream handler. Backend chỉ có `SELECT`
trên `platform_admins`, không được tự cấp/xóa quyền.

## 5. Status và safe error contract

Lỗi HTTP có dạng:

```json
{"error":{"code":"unauthorized","message":"authentication required","request_id":"<request-id>"}}
```

| HTTP | Code | Message / trường hợp |
|---|---|---|
| 401 | `unauthorized` | `authentication required`; lỗi authentication; header `WWW-Authenticate: Bearer` |
| 403 | `forbidden` | `insufficient permissions`; user hợp lệ không có admin membership trên test/admin guard |
| 503 | `service_unavailable` | `service unavailable`; lookup admin lỗi hoặc readiness DB lỗi |
| 501 | `not_implemented` | `endpoint not implemented`; authenticated business stub |
| 404 | `not_found` | `resource not found`; route không tồn tại |
| 405 | `method_not_allowed` | `method not allowed`; sai method trên route đã đăng ký |
| 500 | `internal_error` | `internal server error`; panic qua recovery middleware |

`X-Request-ID` được bảo toàn/tạo mới và có trong error body. Không trả parser
details, SQL errors, token, secret hay connection string cho client. Admin
lookup log chỉ ghi request ID, operation và category `database_error`, `timeout`
hoặc `cancelled`; không log raw database error. Không log credential để debug.

## 6. Bootstrap platform admin hiện có

Operator tạo/mời human user qua công cụ quản trị Supabase Auth trước, để
trigger tạo `profiles`; không dùng public signup. Xem [Task 2.2A](stage-2-task-2.2A-centralized-accounts.md).
Áp dụng migration 000010 và Supabase compatibility migration. Bootstrap là thao tác
operator bằng PostgreSQL administrator credential, **không** bằng backend role
hay Supabase `service_role` key; không có public API tự cấp quyền.

Từ repo root, chuẩn bị connection variables trong shell tin cậy qua cơ chế
quản lý credential của operator; không paste password vào command history:

```bash
# Export trước: PGHOST, PGUSER, PGDATABASE, PGPASSWORD,
# PLATFORM_ADMIN_USER_ID; PGPORT nếu không dùng port mặc định.
: "${PGHOST:?}" "${PGUSER:?}" "${PGDATABASE:?}" "${PGPASSWORD:?}" \
  "${PLATFORM_ADMIN_USER_ID:?}"
sh scripts/bootstrap-platform-admin.sh
```

`PGHOST` phải là địa chỉ operator thực sự truy cập được; không mở PostgreSQL
public để chạy bootstrap. Cần `psql` trên môi trường operator và UUID của user
đã tồn tại, không dùng email/password làm identifier. Script lấy `PGPASSWORD`
từ môi trường, UUID qua psql variable rồi cast UUID, không đưa password vào argv.
Không bật shell tracing hoặc lưu dump môi trường; unset `PGPASSWORD` sau dùng.

Script giữ transaction/advisory lock `(3290,21)`, yêu cầu `profiles` row, insert
`ON CONFLICT DO NOTHING`. Chạy lại cùng UUID là idempotent; từ chối bootstrap
nếu có **bất kỳ admin UUID khác** (kể cả UUID yêu cầu đã là admin). Đây chỉ là
bootstrap admin đầu tiên, không phải công cụ thêm admin tùy ý. Luồng cấp thêm
admin có actor/audit thuộc task sau. Không chạy script từ checkout có thể bị
process không tin cậy sửa.

## 7. Lệnh verification có thể chạy

Tất cả block dưới bắt đầu từ **repo root**, không source `.env`. Go module ở
`src/`: dùng Go **1.27.1** và `golangci-lint` **v2.14.0** như CI; cần C compiler
cho race detector. Docker Engine/Compose và Python **3.10+** cần cho harness;
images được pin trong scripts, cần quyền Docker và khả năng pull/build.

```bash
(cd src && go test -race -count=1 ./internal/auth ./internal/httpserver ./internal/config ./internal/httpapi)
(cd src && test -z "$(gofmt -l .)" && go vet ./...)
(cd src && golangci-lint run --timeout=5m)
(cd src && go test -v -race -coverprofile=coverage.out ./...)
python3 -m unittest discover -s scripts/tests -p 'test_stage2_auth.py'
```

```bash
sh scripts/test-stage2-auth.sh
sh scripts/test-backend-smoke.sh
sh scripts/test-stage2-admin.sh
sh scripts/test-stage2-migrations.sh
```

- Auth: isolated GoTrue/PostgreSQL/backend/Nginx/Envoy; public signup denial
  `422/signup_disabled` không tạo DB rows, Admin API tạo users không tự cấp
  Gateway/admin, human token không tạo user qua Admin API; login, refresh, issuer,
  invalid token matrix, public `200`, missing token `401`, valid stub `501`,
  bootstrap và test-only admin guard (admin `204`, normal user `403`), kiểm tra
  log không chứa test credentials/tokens.
- Smoke: startup config fail-closed, public/secured routes và container probes.
- Admin: Go integration bằng `iot_backend_app`, membership/revocation, thiếu
  quyền insert/delete, cancellation, locked-table deadline và closed pool.
- Migration: clean install/upgrade v9→v10, concurrent/repeated runner,
  bootstrap restrictions, schema/role regression.

Harness dùng tài nguyên Docker riêng và cleanup, không đọc/sửa deployment
`.env`, không dùng Compose stack/volume production. Auth/smoke/admin sinh
credential tạm; migration harness có fixture password riêng chỉ cho test.
Auth/smoke có thể dùng `STAGE2_BACKEND_IMAGE` để reuse **image test đã build từ
code hiện tại**, nếu không sẽ build image test. Không tự set
`ADMIN_TEST_ISOLATED`/database URLs tới deployment: integration fixture là
destructive, chỉ chạy qua `test-stage2-admin.sh`. Unit test bình thường skip
integration này. Xem [CI](../../.github/workflows/ci.yml) để đối chiếu jobs.

## 8. Troubleshooting không log token

- **401:** kiểm tra header duy nhất, access token human (không phải anon/service
  key), secret mapping, issuer exact, audience/role, UUID sub và đồng hồ server.
  Với token trước issuer change: refresh/login lại. Chỉ kiểm tra claims trong
  môi trường local tin cậy, không upload JWT tới decoder bên ngoài và không
  ghi raw token/Authorization vào log/ticket.
- **403:** trên route dùng admin guard, kiểm tra đúng `profiles.id` và
  `platform_admins.user_id` bằng administrator connection. User đăng nhập được
  không mặc nhiên là admin. Không sửa JWT role thành `service_role`.
- **503:** dùng `request_id` và category log để kiểm tra kết nối/pool, quyền
  SELECT, migration và lock/timeout PostgreSQL. Phân biệt readiness với admin
  lookup; không tắt guard hay tăng timeout vô hạn để che lỗi.
- **501/404:** đối chiếu route matrix: business stub/chưa đăng ký không phải
  lỗi token. WebSocket realtime và User–Gateway permission là công việc sau,
  không thêm query token để vượt authentication hiện tại.

Tài liệu này không chứng nhận production deployment, không khẳng định secret
rotation đã diễn ra và không thay thế kết quả chạy verification cụ thể.
