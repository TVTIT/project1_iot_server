# Phạm vi CI native DynSec

`dynsec-native-integration` chạy hai lanes simplified DynSec theo thứ tự:
service (`TestSimplifiedServiceNativeRotateRevoke`) rồi HTTP
(`TestSimplifiedHTTPNativeRotateRevoke`). Test binaries được build trên host
với Go 1.27.1, `CGO_ENABLED=0`, `GOOS=linux`, `GOARCH=amd64` và chạy trong
fixture broker cô lập. Đây **không phải** bằng chứng backend Dockerfile hoặc
root Compose parity.

CI tải trước hai images bằng digest cố định, platform `linux/amd64`:

- `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
- `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`

Runners preflight images local và tạo containers bằng `--pull=never`; không
dùng deployment `.env`, credentials hoặc volumes. Migrations được lấy từ
checkout, fixtures/inputs bí mật nằm trong thư mục riêng ngoài artifacts.
Job có timeout 20 phút; mỗi subprocess native có timeout của runner.

## Gate và artifacts

Unit regressions của supervisor, native runners, schema fixture và shared probe
chạy trước native execution. Supervisor chỉ thành công khi cả hai lanes thực sự
chạy, native status `PASS`, aggregate `SUCCESS`, và cả PostgreSQL, broker, host
cleanup đều là boolean `true` ở kết quả cleanup lẫn aggregate. SKIP, dry-run,
thiếu evidence hoặc cleanup lỗi không được xem là PASS.

Report dùng tên riêng theo `github.run_id` + `github.run_attempt`, ghi atomically
với mode `0600`, không overwrite report cũ. Bước `--validate-report` kiểm exact
schema và SUCCESS trước upload. Chỉ đúng một file JSON allowlisted được upload;
không upload fixtures, logs, stores hay secret responses. Khi native thất bại,
job thất bại và không upload report qua bước success-only này.

SIGINT/SIGTERM khởi động teardown best effort qua `finally`; tín hiệu tiếp theo
không được phép ngắt teardown lần nữa. Cơ chế này **không chứng minh bounded
child cancellation**, và không đảm bảo cleanup sau SIGKILL/OOM hoặc runner bị
buộc dừng. Không có bước `always()` xóa Docker theo wildcard.

## Active và legacy

- `docker-build`: build backend từ Dockerfile và chạy smoke hiện có, không phải
  full root Compose smoke.
- `legacy-runtime-image-build`: giữ build coverage của `auth-init` và `reloader`
  ngoài active backend job; không xóa source/tests legacy.
- `cross-compile`: giữ server/gateway và compile regression initializer/reloader.
- `stage2-e2e`: giữ unified server/controller harness cũ, đặt nhãn legacy.
- `mosquitto-credential-integration`: giữ controller HTTP/lifecycle/fault coverage
  cũ với broker DynSec thật, không thay thế simplified native gate.
- `mosquitto-runtime-integration`: adapter TLS/ACL là native thật; phần topology
  initializer/reloader là legacy. Giữ coverage artifact riêng như trước.
- `lint-and-test`, `authorization-integration`, `provisioning-integration`: giữ
  behavior và coverage riêng; manifest Go unit vẫn có 25 tests bắt buộc.
- `dynsec-bootstrap-tests`: regression unit suites (`test_task284_bootstrap.py`,
  `test_dynsec_bootstrap_cli.py`) và pipeline native runner độc lập kiểm tra
  khởi tạo store, ACL default deny và thu hồi exact volume. Report upload riêng biệt
  sau khi được kiểm tra safe schema.
- `native-compose-smoke`: full root Compose smoke với cờ `--full-http` trên runner
  riêng biệt. Thực thi positive slice, 14 role denials, 9 JWT missing/invalid/expired cases,
  xác minh exact cleanup (10 containers, 1 network, 3 volumes), validate
  `startup-summary.json` thành công trước khi upload artifact.

## Giới hạn và các bước tiếp theo

Pipeline CI đã được kết nối thêm hai jobs `dynsec-bootstrap-tests` và
`native-compose-smoke`. Lưu ý giới hạn hệ thống:
- Bounded cancellation chỉ bảo vệ teardown trong tiến trình khi nhận SIGINT/SIGTERM;
  không thể đảm bảo cleanup sau SIGKILL, OOM hoặc runner GitHub Actions bị ngắt cưỡng bức.
- Kết quả test và smoke exit 0 hiện tại là kết quả thực thi cục bộ (local run),
  chưa phải là bằng chứng remote GitHub Actions run hoặc clean-checkout run từ xa.
- Không sửa `migration-check` hay implementation runners trong slice này; các kiểm tra
  chỉ verify contract wiring. Password persistence sau restart và reset usable vẫn là các gaps riêng.
- Legacy unit contract `test_actual_production_compose_ca_only` vẫn yêu cầu services
  initializer/reloader trong root Compose dù topology active đã đổi. Đây là blocker
  legacy có sẵn, cần chủ sở hữu legacy contract xử lý riêng; không phục hồi services
  cũ để làm test xanh. Không coi các kết quả local là GitHub Actions đã chạy.
