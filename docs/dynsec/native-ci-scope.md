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
- `stage2-e2e` (hiển thị: `Legacy E2E contracts`): giữ unit/contract tests
  `test_stage2_ci.py`, `test_stage2_ci_functional.py`, `test_stage2_response_loss.py`.
  Chỉ loại bỏ native runner `python3 scripts/tests/stage2_ci.py` (run-1/run-2), bước
  native cleanup `--cleanup`, và upload artifact `stage2-e2e-safe-reports`.
  Thêm executable notice step phát `legacy-execution-deferred: stage2-e2e` ra stdout
  và `$GITHUB_STEP_SUMMARY`.
  Disposition: historical remote run thất bại trước khi vào scenarios (nguyên nhân ẩn;
  chia sẻ controller-coupled expectations). Không quy failure này cho fault-lost-ack của runtime job.
  Được deferred vì phụ thuộc legacy harness, không cấp phát tài nguyên native; việc
  current backend được kiểm riêng bởi `native-compose-smoke` và `dynsec-native-integration`;
  không khẳng định hai jobs này tái hiện toàn bộ legacy scenarios.
- `mosquitto-credential-integration` (hiển thị: `Legacy credential checks`): giữ
  unit tests `test_stage2_mqtt_credentials.py`, `test_task2612_standalone.py`,
  linter `golangci-lint v2.14.0`, và source Docker builds (`backend`, `credential-controller`).
  Chỉ loại bỏ native script `sh scripts/test-stage2-mqtt-credentials.sh`, env coverage block,
  và upload artifact `mosquitto-credential-coverage`.
  Thêm executable notice step phát `legacy-execution-deferred: mosquitto-credential-integration`
  ra stdout và `$GITHUB_STEP_SUMMARY`.
  Disposition: standalone controller gate mismatch là source-proven (current `cmd/server`
  không gọi legacy `OpenVerified`, gate 8883 đóng). Gate cutoff không tương đương MQTT
  auth hoặc readiness. Không có claim hoàn tất native reset (production reset prerequisite
  deny-only). Credential lifecycle thực tế hiện do `dynsec-native-integration`
  (`TestSimplifiedServiceNativeRotateRevoke`, `TestSimplifiedHTTPNativeRotateRevoke`)
  bảo đảm. `native-compose-smoke` kiểm business APIs admin inventory/owner sensor,
  Auth/RBAC/JWT, persistence và Dockerfile/Compose, không thay thế credential lifecycle suite.
- `mosquitto-runtime-integration` (hiển thị: `Legacy runtime contracts`): giữ
  unit tests `test_stage2_mosquitto_runtime.py`, Go setup 1.27.1, và thư mục `/tmp/opencode`.
  Chỉ loại bỏ native script `sh scripts/test-stage2-mosquitto-runtime.sh` và native coverage
  upload `mosquitto-runtime-coverage`.
  Thêm executable notice step phát `legacy-execution-deferred: mosquitto-runtime-integration`
  ra stdout và `$GITHUB_STEP_SUMMARY`.
  Disposition: legacy runtime fault-lost-ack exit 2 root cause UNKNOWN; deferred vì
  adapter file/reloader là đường chạy legacy, không phải vì bug đã được sửa.
  `legacy-runtime-image-build` vẫn giữ nguyên riêng biệt.
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
`native-compose-smoke`, đồng thời chuyển 3 legacy jobs sang truthful deferred unit/build checks.
Lưu ý các giới hạn hệ thống và ranh giới xác minh:
- Bounded cancellation chỉ bảo vệ teardown trong tiến trình khi nhận SIGINT/SIGTERM;
  không thể đảm bảo cleanup sau SIGKILL, OOM hoặc runner GitHub Actions bị ngắt cưỡng bức
  đối với các active native runners (`dynsec-native-integration`, `native-compose-smoke`,
  `dynsec-bootstrap-tests`).
- Các deferred jobs (`stage2-e2e`, `mosquitto-credential-integration`,
  `mosquitto-runtime-integration`) hoàn toàn không phân bổ tài nguyên native và không phát
  ra native cleanup hay artifact evidence; không có fake report hay fake coverage artifacts.
- Kết quả test và smoke exit 0 hiện tại là kết quả thực thi cục bộ (local run),
  chưa phải là bằng chứng remote GitHub Actions run hoặc clean-checkout run từ xa.
  Tuyệt đối không tuyên bố legacy PASS, full CI green hay clean checkout thành công.
- Coverage không được tái hiện bởi current runners được ghi nhận là deferred thay vì khẳng
  định tương đương.
- Composition contract cũ
  `test_task2611_composition.CompositionTemplates.test_legacy_compose_cannot_opt_in`
  đã được thay bằng current opt-in contract và negative cases: default disabled,
  manager credential chỉ qua environment, active backend không phụ thuộc controller.
  Legacy template assertions giữ nguyên; composition tests đã PASS local, chưa remote proof.
  Root Compose CA/security suite `test_stage2_mosquitto_runtime.py` đã được cập nhật và
  PASS local; không coi nó là cùng failure hoặc phục hồi services cũ để làm test xanh.
- Admin Inventory integration failure (đề mục 6), linting issues (đề mục 8), và final
  clean-checkout gates vẫn là các blockers thực sự cần xử lý tiếp theo trước khi hoàn tất.
