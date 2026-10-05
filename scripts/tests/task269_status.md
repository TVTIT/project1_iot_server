# Task 2.6.9 — acceptance verification (2026-10-04)

Không commit/push/deploy, không đọc/sửa `.env`, không sửa SQL/schema hay
repository implementation. Database review migration 016: PASS do parent cung
cấp; không suy ra verdict DB từ native test.

## Runtime regression đã sửa

RED thực chạy `TestVerifyRevocationsCorrelatedAbsence/absent`: trước fix trả
`credential recovery required`. GREEN sau fix: adapter và startup dùng chung
protected snapshot absence check, chỉ khi correlated native getClient trả typed
`Client not found`. Generic errors, snapshot unreadable/present, epoch shift đều
fail closed. Không tạo principal giả; không lọc durable revoke inventory; không
thay đổi quyền manager/backend/protected targets.

Absence là bằng chứng principal hiện không thể authenticate ở target đã kiểm
chứng, không phải positive password witness, chứng minh ngắt physical connection,
stale-generation proof hay fsync/power-loss receipt.

## Native Task269

Log thực chạy: `/tmp/opencode/task269-complete-acceptance.log` (append các lượt;
có cả build failure trung gian, không che bằng log chỉ PASS). Các lượt cuối PASS
với owned PG16 app-role, migrations tới 016, pinned DynSec 2.0.18, source-built
PID1/private IPC và protected JSON reader:

- Pending provision/rotate và terminal SUCCESS unfinished checkpoint được cleanup;
  audit terminal không bị viết lại, actor profile đã xóa vẫn recovery được.
- SIGKILL broker sau durable BeginRecovery trước CompleteRecovery; PID1 mới CLOSED;
  startup CAS rebind sang epoch mới, không dùng evidence epoch cũ.
- Snapshot chmod 000 và native save-directory permission fault: Run thất bại,
  durable pending fence còn nguyên, không READY/OPEN; operator sửa quyền rồi retry.
- DB revoked/native enabled snapshot thật được re-disable trước OPEN.
- CompleteRecovery reply-loss **application seam**, commit và resolve UUID bằng
  repository thật; không claim PostgreSQL wire/COMMIT loss.
- Active B fresh private TLS login vẫn thành công; không disable mọi active.
- **Provision thật bằng NEW idempotency key** sau recovery: secret trả một lần,
  current metadata active/version+1 và fresh TLS login bằng secret mới PASS.
- Pagination budget exhaustion CLOSED rồi tăng budget retry PASS. Hai gated
  routes được sample riêng với outcome và thời gian; TCP dial failure không bị
  gọi là authentication-denial evidence.
- Owned container/volume/file cleanup và secret-output scan PASS.

## Checks và coverage

- Adapter265c, Task266, Task267, Task268 native regression rerun: PASS, logs
  `/tmp/opencode/task269-regression-{265c,266,267,268}.log`.
- Real PostgreSQL repository fixture: latest migrations/verifiers, 12 lần race
  CompleteRecovery vs BeginOperation, admission chỉ sau durable cleanup và
  generation+1; legacy/unknown projection rejection, cancelled reads và bounded
  pool-exhaustion check. Log `/tmp/opencode/task269-current-pg.log`.
- `go test -race -coverprofile=/tmp/opencode/task269-current-unit.cover ./...`,
  `go vet ./...`, `go build ./...`; Python discovery 50 tests; gofmt và
  `git diff --check`. Final outcomes ghi sau lần chạy cuối bên dưới.
- Statement-weighted union **current successful** unit + native profile
  `/tmp/opencode/task269-current-native.cover`, cùng source block coordinates;
  không dùng failed native profile hay artifact cũ:

| File | Covered / total | Coverage |
|---|---:|---:|
| reconcile.go | 156 / 172 | 90.70% |
| readiness.go | 2 / 2 | 100% |
| dynsec_startup.go | 72 / 82 | 87.80% |
| postgres_startup.go | 27 / 39 | 69.23% |
| **Tổng scope** | **257 / 295** | **87.12%** |

>=80% là gate tổng scope, không tuyên bố mỗi file >=80%. PG harness coverage
không được cộng vào bảng union này. Domain recovery invariants và uncertainty
classification được exercise bởi existing unit/repository tests.

## Giới hạn rollout

Không claim HA, perfect post-OPEN DB policy, power-loss password durability,
production readiness wiring hay Task10. Post-OPEN DB-loss policy vẫn UNDECIDED.
Legacy projection tests dùng existing isolated upgrade fixtures, không sửa
production data hay chọn historical winner. Các fault sử dụng bounded direct
application seams khi cần, không claim SQL wire-loss testing.

Final verification: **Task9 PASS trong assigned service/startup scope**. Lượt cuối
real PG repository fixture (gồm bounded pool check) PASS; `go test -race ./...`,
vet/build PASS; Python 50 PASS; gofmt/whitespace PASS. Scoped current union được
tính lại sau lượt cuối vẫn **257/295 = 87.12%**. Không mở HTTP routes/production
wiring; parent review Task9 trước khi tiếp tục Task10.
