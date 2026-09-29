# Giai đoạn 2 - Task 2.1: Migration runner và schema quản trị

Task 2.1 bổ sung cơ chế áp dụng migration cho database mới và PostgreSQL volume
đã được baseline đến version 9. PostgreSQL tiếp tục là nguồn dữ liệu chính cho platform
administrator, metadata MQTT credential và audit event.

## Thành phần

- `scripts/run-migrations.sh`: runner áp dụng các migration từ version 10.
- `migrations/000010_stage2_auth_and_provisioning.up.sql`: schema Giai đoạn 2.
- `scripts/sql/verify-migration-000010.sql`: kiểm tra schema và database role.
- `scripts/test-stage2-migrations.sh`: integration test clean install và upgrade.
- `scripts/bootstrap-platform-admin.sh`: bootstrap platform admin đầu tiên.
- Compose service `application-migrations`: chạy trước backend trên mỗi lần
  triển khai và kết thúc sau khi migration thành công.

## Migration runner

Runner yêu cầu database đã được baseline đến version 9. Nó giữ session-level
PostgreSQL advisory lock `(3290, 2)` trong cùng một `psql` session, kiểm tra
`schema_migrations`, sau đó chỉ include migration chưa được áp dụng.

Đặc tính:

- Hai runner đồng thời không chạy cùng migration.
- Chạy lại không nhân đôi schema hoặc migration history.
- Version đã tồn tại với name khác làm runner fail.
- Lỗi migration đóng connection và tự giải phóng advisory lock.
- Runner không tự chạy optional development seed.
- Backend chỉ khởi động sau khi runner và Supabase compatibility migration đều
  hoàn thành.

Migration 1-9 vẫn là baseline lịch sử. Runner không phát lại các migration này
trên volume cũ. Volume legacy chưa có `schema_migrations` phải chạy quy trình
preflight và baseline 000009 trong `README.md`; runner fail closed thay vì tự
nhận diện hoặc tự sửa schema không rõ trạng thái.

## Schema 000010

Migration tạo:

- `platform_admins`.
- `gateway_mqtt_credentials`.
- `gateway_mqtt_credential_events`.
- Reverse index `user_gateways(gateway_id, user_id)`.
- Index truy vấn audit event theo Gateway và thời gian.
- Constraint identifier cho `gateway_id` và `sensor_id`.

Identifier được giới hạn bởi:

```text
^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$
```

`backend_service` là Gateway ID bị cấm vì đây là MQTT principal dành riêng cho
backend.

Không có email, user UUID triển khai, password hoặc Gateway ID cụ thể nào được
seed trong migration.

## Database role của backend

Role provisioner tạo:

- `iot_backend`: NOLOGIN, `BYPASSRLS`, không phải superuser và không có quyền
  tạo database/role.
- `iot_backend_app`: LOGIN role, kế thừa quyền DML từ `iot_backend` và có
  `BYPASSRLS` rõ ràng vì thuộc tính role không tự kế thừa qua membership.

`BYPASSRLS` là quyết định có chủ đích: Go business API tự thực hiện
User-Gateway authorization trong parameterized SQL. Các RLS policy hiện tại
dành cho đường truy cập Supabase/direct role và không nhận claim từ pgx pool.

Backend role:

- Có DML theo từng bảng; không có blanket CRUD trên toàn schema.
- Chỉ có `SELECT` trên `platform_admins`.
- Không có `CREATE` trên schema `public`.
- Không có quyền sửa `schema_migrations`.
- Không có quyền xóa credential audit event hoặc sửa/xóa raw telemetry.
- Không sở hữu migration hoặc table.

## Cấu hình local

Tạo một password URL-safe ngẫu nhiên, tối thiểu 128 bit entropy, rồi thêm vào
repository-root `.env`:

```text
BACKEND_DB_PASSWORD=<random-url-safe-password>
DATABASE_URL=postgres://iot_backend_app:<same-password>@postgres:5432/iot_platform?sslmode=disable
```

Không commit `.env`. `BACKEND_DB_PASSWORD` và password trong `DATABASE_URL`
phải giống nhau.

Áp dụng role và migration trên volume đã baseline version 9:

```bash
docker compose up -d --build
```

Compose sẽ chạy theo thứ tự:

```text
postgres healthy
  -> supabase-role-provisioner
      -> application-migrations
      -> supabase-auth healthy
application-migrations + supabase-auth healthy
  -> supabase-compat-migration
application-migrations + supabase-compat-migration
  -> backend
```

Volume legacy chưa có `schema_migrations` phải chạy preflight và baseline
000009 trong `README.md` trước; runner không tự suy đoán trạng thái schema.

## Bootstrap platform admin

User phải đăng ký Supabase trước để trigger tạo `profiles` row. Sau đó chạy
script bằng database administrator credential:

```bash
set -a
. ./.env
set +a

export PGHOST=127.0.0.1
export PGUSER="$POSTGRES_USER"
export PGPASSWORD="$POSTGRES_PASSWORD"
export PGDATABASE="$POSTGRES_DB"
export PLATFORM_ADMIN_USER_ID=<supabase-user-uuid>
sh scripts/bootstrap-platform-admin.sh
```

Script chạy lặp an toàn và từ chối UUID chưa có trong `profiles`. Không có API
public để tự cấp platform admin. Script chỉ bootstrap admin đầu tiên; chạy lại
cùng UUID là idempotent, nhưng từ chối thêm UUID khác. Việc cấp thêm admin cần
luồng quản trị có actor/audit riêng ở task sau.

## Verification

Chạy integration test độc lập:

```bash
sh scripts/test-stage2-migrations.sh
```

Test kiểm tra:

- Upgrade database version 9 lên version 10.
- Hai runner chạy đồng thời.
- Runner chạy lại.
- Clean install đến version 10.
- Identifier và reserved Gateway ID constraints.
- Platform admin bootstrap chạy lặp.
- Backend role có DML nhưng không có DDL hoặc migration-history write.

Test dùng container và database tạm, sau đó cleanup tự động.

## Permission preflight

Các script migration/bootstrap chạy với PostgreSQL administrator credential.
Không chạy chúng từ checkout hoặc `.env` mà user/process không tin cậy có thể
sửa. Trước production deployment, đặt `.env` thành mode `0600` và bảo đảm
script/migration không writable bởi group/others. Workspace hiện nằm trên một
mounted filesystem; nếu mount không hỗ trợ Unix permissions, cần chuyển
deployment checkout và `.env` sang filesystem hỗ trợ permission hoặc remount
với `uid`, `gid`, `fmask`, `dmask` phù hợp.
