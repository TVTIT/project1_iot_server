# Giai đoạn 2 - Task 2.0: Kết quả technical spike

Tài liệu này ghi kết quả kiểm chứng trên local stack ngày 2026-09-28. Các
script tái lập nằm trong `scripts/spikes/` và chỉ dùng container/dữ liệu tạm,
ngoại trừ JWT spike gọi Supabase Auth đang chạy để tạo rồi xóa một user test.

## Cách chạy

Yêu cầu:

- Local Compose stack đang chạy và truy cập được qua `http://127.0.0.1`.
- Repository-root `.env` có `ANON_KEY`, `SERVICE_ROLE_KEY` và `JWT_SECRET`.
- Docker, Docker Compose, Python 3 và OpenSSL.

Chạy toàn bộ:

```bash
sh scripts/spikes/run_stage2_task_2_0.sh
```

Các script không in access token, refresh token, JWT secret hoặc MQTT
password. Không dùng script này với production Auth vì nó tạo và xóa một user
test.

## Spike S1 - Supabase JWT contract

Script: `scripts/spikes/jwt_contract.py`

Luồng đã kiểm tra:

1. Đăng ký user test qua `/auth/v1/signup`.
2. Đăng nhập qua `/auth/v1/token?grant_type=password`.
3. Xác minh chữ ký HMAC bằng `JWT_SECRET`.
4. Refresh token qua `/auth/v1/token?grant_type=refresh_token`.
5. So sánh các claim định danh ổn định.
6. Xóa user test qua Supabase Admin endpoint.

Kết quả thực tế:

```text
alg=HS256
aud=authenticated
role=authenticated
sub=UUID
nbf=absent
iss=absent
refresh contract stable=true
```

Trong lần kiểm chứng ngày 2026-09-28, endpoint
`/auth/v1/.well-known/jwks.json` trả danh sách `keys` rỗng. Điều này phù hợp
với stack đang ký token bằng shared HS256 secret; hiện chưa thể dùng JWKS để
xác minh access token. JWT spike gọi endpoint trong mỗi lần chạy, kiểm tra cấu
trúc response và in `jwks_key_count`; số lượng key là observation, không phải
điều kiện cố định để script pass.

### Quyết định cho Task 2.2

- Dùng local HS256 verification; không gọi GoTrue trên từng request.
- Chỉ cho phép chính xác algorithm `HS256`.
- Kiểm tra signature, `aud`, `exp`, `sub` là UUID và `role=authenticated`.
- Xử lý `nbf` nếu token tương lai có claim này.
- **Chưa được tuyên bố kiểm tra issuer** vì token hiện tại không có `iss`.
- Trước Task 2.2 phải chọn một trong hai phương án:
  1. cấu hình GoTrue phát hành `iss` ổn định rồi bắt buộc kiểm tra claim này;
  2. ghi rõ issuer validation chưa khả dụng trong cấu hình GoTrue hiện tại và
     bù bằng secret riêng cho stack, strict audience/role và algorithm allowlist.

Phương án 1 được ưu tiên. Không được âm thầm thêm `SUPABASE_JWT_ISSUER` rồi bỏ
qua kiểm tra khi claim vắng mặt.

### Kiểm chứng bổ sung cho Task 2.2.0 (2026-09-30)

Kết quả ở trên là quan sát lịch sử trước khi cấu hình issuer, không bị thay thế.
Với `supabase/gotrue:v2.196.0`, đặt `GOTRUE_JWT_ISSUER` trong Compose (local:
`http://localhost/auth/v1`) đã làm access token mới từ login **và** refresh
có claim `iss` khớp chính xác cấu hình. Spike hiện yêu cầu issuer và audience
qua environment, xác minh chữ ký HS256 và fail nếu thiếu hoặc sai `iss` ở một
trong hai token; output chỉ ghi `issuer_matches_expected=true`, không in token
hay secret. Lần chạy local qua Nginx/API Gateway đạt: audience `authenticated`,
role `authenticated`, subject UUID, refresh contract stable và JWKS key count 0.

Sau khi đổi issuer, token cũ không có `iss` phải login/refresh lại trước khi
backend bắt buộc kiểm tra issuer ở các bước tiếp theo của Task 2.2. Trên mỗi
deployment, issuer phải là public Auth URL tương ứng, không sao chép giá trị
`localhost` vào production.

## Spike S2 - Mosquitto static password reload

Script: `scripts/spikes/mosquitto_password_reload.sh`

Môi trường kiểm tra dùng đúng image `eclipse-mosquitto:2.0.18`. Script thực
hiện:

1. Tạo password file tạm với `sha512-pbkdf2`.
2. Khởi động broker tạm bằng static `password_file`.
3. Kết nối Gateway bằng password cũ.
4. Cập nhật bản sao, atomic rename và gửi `SIGHUP` qua container chỉ chia sẻ
   PID namespace với broker.
5. Kiểm tra password cũ/mới sau rotate.
6. Xóa user, reload và kiểm tra reconnect sau revoke.
7. Kiểm tra credential `backend_service` không bị ảnh hưởng.

Kết quả quan sát ngày 2026-09-28:

```text
rotation_kept_existing_session=false
revoke_kept_existing_session=false
old_credentials_rejected_for_new_connections=true
unrelated_backend_credential_remained_valid=true
```

Trong lần chạy này trên image/version đang pin, reload sau rotate hoặc revoke
làm client liên quan bị ngắt; client tự reconnect bằng credential cũ nhận
`CONNACK 5` và bị từ chối. Hai giá trị `*_kept_existing_session` là observation
được script in ra, không quyết định PASS. Guarantee của Giai đoạn 2 vẫn chỉ là
credential cũ không tạo được kết nối mới. Không biến hành vi ngắt session thành
API guarantee dài hạn nếu chưa có yêu cầu và regression test riêng.

### Ràng buộc triển khai cho Task 2.5

- Không mount Docker socket vào backend.
- Sidecar reload chỉ chia sẻ PID namespace với Mosquitto và chỉ gửi `SIGHUP`.
- Backend sửa bản sao trên cùng filesystem rồi atomic rename.
- Spike serialize chuỗi copy, sửa và rename bằng filesystem lock, đồng thời
  dùng tên file tạm riêng cho từng operation. Credential adapter production
  phải mở rộng critical section để bao cả reload/probe trước khi xử lý request
  kế tiếp.
- Password file cuối phải thuộc UID/GID Mosquitto (`1883:1883`) và có mode
  giới hạn.
- Official image entrypoint cố `chown` mount; mount read-only trực tiếp qua
  entrypoint không hoạt động. Hai phương án khả thi là:
  1. chạy broker bằng explicit non-root user và bỏ qua entrypoint, như spike;
  2. cho broker mount volume read-write dù chỉ credential adapter thực hiện
     logic ghi file.
- Chọn phương án 1 trong Task 2.5 để giữ Mosquitto không có quyền ghi volume.
- `acl_file` tiếp tục static; `%u` đã loại nhu cầu sinh ACL riêng từng Gateway.

`mosquitto_passwd` nhận password qua stdin. Client CLI trong test integration
vẫn cần password để thử authenticate; test phải chạy trong container tạm và
không in command/process inspection ra CI log. `mosquitto_pub/sub -P` làm
password thử nghiệm xuất hiện tạm thời trong container metadata; chỉ chấp nhận
điều này cho random one-time spike credentials, không dùng pattern này với
credential thật.

## Spike S3 - Migration upgrade từ version 9

Script: `scripts/spikes/migration_upgrade_probe.sh`

Script khởi tạo database tạm bằng migration hiện có đến version 9, sau đó chạy
một migration probe version 10 dưới PostgreSQL transaction và transaction-level
advisory lock:

```sql
SELECT pg_advisory_xact_lock(3290, 2);
```

Migration probe được chạy hai lần đồng thời rồi chạy lại tuần tự. Kết quả cuối:

```text
schema_migrations version 10 rows=1
probe table rows=1
```

### Quyết định cho Task 2.1

- Migration runner chỉ áp dụng version chưa có trong `schema_migrations`.
- Mỗi migration chạy trong transaction nếu PostgreSQL/TimescaleDB operation
  cho phép.
- Runner giữ advisory lock trong suốt quá trình kiểm tra và áp dụng migration.
- Backend phụ thuộc migration runner hoàn thành thành công.
- CI phải giữ cả hai đường clean install và upgrade từ version 9.
- File migration probe chỉ tồn tại bên trong container tạm; Task 2.0 không thêm
  schema production version 10.

## Tiêu chí hoàn thành Task 2.0

| Guarantee | Bằng chứng | Kết quả |
|---|---|---|
| Token GoTrue thật dùng HS256 và có audience/role/sub dự kiến | `python3 scripts/spikes/jwt_contract.py` | PASS |
| Refresh giữ contract định danh | `python3 scripts/spikes/jwt_contract.py` | PASS |
| Contract hiện không có issuer; JWKS key count được quan sát mỗi lần chạy | JWT script và JWKS probe | ISSUER BLOCKER CONFIRMED |
| Static password file reload không restart broker | `sh scripts/spikes/mosquitto_password_reload.sh` | PASS |
| Rotate/revoke chặn credential cũ và không ảnh hưởng backend credential | Mosquitto spike | PASS |
| Schema version 9 có thể upgrade đúng một lần | `sh scripts/spikes/migration_upgrade_probe.sh` | PASS |
| Concurrent/repeated migration không nhân đôi thay đổi | Migration spike | PASS |

Task 2.0 không triển khai schema, middleware, API hoặc credential manager thật.
Các phần đó lần lượt thuộc Task 2.1 đến Task 2.6.

Issuer blocker được giải quyết trong Task 2.2 bằng GOTRUE_JWT_ISSUER.
