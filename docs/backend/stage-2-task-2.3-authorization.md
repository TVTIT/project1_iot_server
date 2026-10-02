# Giai đoạn 2 — Task 2.3: Authorization và API đọc Gateway/Sensor

## 1. Trạng thái và phạm vi

**Task 2.3.0 hoàn thành:** kiểm tra baseline và chốt contract dưới đây.
**Task 2.3.1–2.3.2 đã triển khai và kiểm chứng local isolated**;
**Task 2.3.3–2.3.4 đã triển khai và kiểm chứng local**;
**Task 2.3.5 đã triển khai và kiểm chứng local**;
**Task 2.3.6 đã triển khai và kiểm chứng local isolated**;
**Task 2.3.7 chưa triển khai**. Router đã đăng ký cả Gateway list và Sensor
list dưới authenticated group. Hai API đã được kiểm chứng bằng access token
GoTrue thật qua Nginx/Envoy trên stack test; chưa áp dụng hoặc xác nhận trên
deployment thật. Bằng chứng Task 2.3.6 ở mục 9.

Chỉ triển khai:

```http
GET /v1/gateways
GET /v1/gateways/{gateway_id}/sensors
```

Không thêm provisioning, membership API, sensor writes, MQTT credentials,
telemetry, WebSocket hoặc Digital Twin control. Owner/operator/viewer chỉ đọc
Gateway được cấp; platform admin không bypass membership trên API người dùng.
Không tự tạo Gateway/Sensor/profile hoặc quyền từ request.

Tái sử dụng JWT verifier, `auth.Principal`, error envelope, request ID,
PostgreSQL pool và `auth.PlatformAdminChecker` của Task 2.2. Không viết lại
`IsPlatformAdmin`. Schema và indexes hiện đủ cho lát cắt đọc; không dự kiến
migration mới hoặc dependency mới.

## 2. Baseline trước triển khai

Baseline commit: `5264be58aac94b67cc64c90200680136235c607e` trên `develop`.
Worktree sạch trước lượt kiểm tra; local tracking ref đồng bộ `origin/develop`.
CI của đúng SHA đã `completed/success`:

- Workflow: `CI`, run ID `36994439036`.
- Bằng chứng: [GitHub Actions run](https://github.com/TVTIT/project1_iot_server/actions/runs/36994439036).

Các kiểm tra local đã chạy từ repo root, trừ Go/linter chạy trong `src/`:

| Kiểm tra | Kết quả |
|---|---|
| `go test -race -coverprofile=/tmp/opencode/stage23-baseline-coverage.out ./...` | PASS; một số package dùng test cache |
| `go tool cover -func=/tmp/opencode/stage23-baseline-coverage.out` | Tổng 81,4%; entrypoint coverage vẫn 0% |
| `go vet ./...` | PASS |
| `go build ./...` | PASS |
| `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run --timeout=5m` | 0 issues |
| `python3 -m unittest discover -s scripts/tests -p 'test_stage2_auth.py'` | 11 tests PASS |
| `sh scripts/test-stage2-auth.sh` | PASS: signup denial, Admin-created users, login/refresh, JWT negatives, admin guard, log secrecy |
| `sh scripts/test-stage2-admin.sh` | PASS: DB role, admin boundary/revocation, deadline/cancellation |
| `sh scripts/test-stage2-migrations.sh` | PASS: fresh install và upgrade v9 |
| `sh scripts/test-backend-smoke.sh` | PASS: startup negatives, health/Auth/stubs, log secrecy |

Các harness dùng resources isolated và cleanup. Không đọc/sửa `.env`, không
recreate stack deployment, không rotate secret, không đổi database thật.
CI/local baseline không chứng minh deployment đã đóng signup hoặc đã rotate
JWT secret mẫu. Không dùng secret prototype cho acceptance trên hệ thống thật.

## 3. Contract HTTP đã chốt

### 3.1. Danh tính và quyền

- Đăng ký cả hai handler qua `groups.authenticated` hiện có.
- `user_id` chỉ lấy từ JWT đã xác minh, qua `Principal.UserID`; không lấy từ
  URL query, body, header tùy ý hoặc JWT user metadata.
- Query có key `user_id`, kể cả rỗng/lặp, trả `400 / invalid_request` sau
  authentication. GET body không được parse để chọn danh tính hoặc filter.
- Chưa hỗ trợ query pagination/filter/sort. Các query key khác không thay đổi
  kết quả hoặc authorization; không tuyên bố chúng đã được hỗ trợ.
- JWT thiếu/sai phải bị chặn trước khi gọi repository.
- Owner/operator/viewer đều đọc nếu có membership hợp lệ; không query admin
  status để mở rộng kết quả. Không có membership: không có quyền đọc.
- Không cache membership. Request mới sau khi transaction thu hồi quyền
  commit phải phản ánh quyền mới. Request đang chạy có thể đọc snapshot trước
  commit; không tuyên bố thu hồi có thể rút lại response đã gửi.

### 3.2. Gateway list

Response `200` chỉ có top-level `items`. Mỗi item có các trường:

| Field | JSON type / quy tắc |
|---|---|
| `gateway_id` | string |
| `name` | string |
| `description` | string hoặc null, luôn có key |
| `role` | string: owner/operator/viewer, từ membership DB |
| `created_at` | RFC3339 UTC string hoặc null, luôn có key |

Không có membership trả đúng `{"items":[]}`, không phải `null`.
Sắp xếp theo `gateway_id` tăng dần với ASCII ordering (`COLLATE "C"` trong
SQL). Không phân trang ở MVP nhỏ này, không âm thầm cắt danh sách bằng LIMIT.
Nếu quy mô sau này cần giới hạn thì bổ sung pagination contract riêng.

### 3.3. Sensor list

Response `200` có `gateway_id` và `items`. Mỗi item có:

| Field | JSON type / quy tắc |
|---|---|
| `sensor_id` | string |
| `name` | string |
| `unit` | string hoặc null, luôn có key |
| `created_at` | RFC3339 UTC string hoặc null, luôn có key |

Sắp xếp theo `sensor_id` tăng dần, ASCII ordering. Gateway được cấp nhưng chưa
có sensor trả `200`, `items: []`. Gateway không tồn tại và Gateway không thuộc
user trả cùng `404 / not_found`, message `resource not found`; không query
existence không scoped để thông báo rằng Gateway của người khác có tồn tại.

Mọi response thành công của hai API có `Content-Type: application/json` và
`Cache-Control: no-store`. Metadata nullable giữ nguyên ý nghĩa SQL NULL,
không đổi thành empty string hoặc thời gian hiện tại. Timestamp có giá trị
serialize UTC theo RFC3339/RFC3339Nano; không bắt client giả định số chữ số
phần giây cố định. Không trả credentials, owner UUID, MQTT metadata hoặc Twin
state ngoài các field nêu trên.

### 3.4. Identifier, timeout và errors

Gateway ID áp dụng đúng constraint migration 000010:

```text
^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$
```

Case-sensitive, không trim/lowercase tự động; từ chối reserved `backend_service`.
Validator chạy trên path parameter do router cung cấp, không unescape lần hai.
Identifier sai tại route đã match trả `400`; URL không match route giữ NoRoute
`404`. Không sửa routing để ép mọi malformed URL thành `400`.

| Tình huống | Status / code / message |
|---|---|
| JWT thiếu/sai | 401 / unauthorized / authentication required |
| Invalid input | 400 / invalid_request / invalid request |
| Missing/inaccessible Gateway | 404 / not_found / resource not found |
| Query deadline/cancellation khi còn ghi được response | 503 / service_unavailable / service unavailable |
| Lỗi query/scan/internal khác | 500 / internal_error / internal server error |

Dùng `httpapi.WriteError`, error envelope hiện có và request ID. Không trả raw
SQL/pgx error, token, connection string hoặc chi tiết có thể lộ tài nguyên.
Client ngắt kết nối không cần status tùy biến 499 hoặc goroutine retry.

Dùng `AUTHORIZATION_TIMEOUT` hiện có (default 2s, configurable) để giới hạn
toàn bộ operation đọc quyền/tài nguyên. Context dẫn xuất từ request, propagate
xuống query và release timer. Không coi HTTP ReadTimeout là query deadline.
Không thêm goroutine per request để giả lập timeout khi driver đã hỗ trợ context.

## 4. Contract repository và cấu trúc tiếp theo

Package nhỏ `internal/gateway` chứa model, identifier, repository interface,
service và PostgreSQL adapter; handlers/DTO ở `internal/httpserver`.
Service không phụ thuộc Gin/pgx; adapter dùng pool hiện có. Router/main phải
wire dependency bắt buộc, thiếu hoặc typed-nil phải fail initialization.

Operations nhận context và UUID, không nhận bearer token:

```text
ListGatewaysForUser(ctx, userID)
ListSensorsForUserAndGateway(ctx, userID, gatewayID)
GetGatewayRole(ctx, userID, gatewayID)
```

- Gateway list join `user_gateways` với userID parameter; không scan toàn bộ
  tài nguyên rồi filter trong Go.
- Sensor list dùng một scoped query: gateways JOIN user_gateways LEFT JOIN
  sensors ON cùng gateway_id. Không có row -> domain not-found; Gateway row
  có sensor_id NULL -> danh sách rỗng. Không phát sinh sensor item rỗng từ
  LEFT JOIN. Không gọi GetGatewayRole trước rồi query sensor riêng.
- GetGatewayRole chỉ trả role của membership; missing trả domain not-found.
- Query parameterized, kiểm tra scan/rows.Err, đóng rows. Không biến lỗi DB
  thành danh sách rỗng hoặc denial. Không dùng RLS làm authorization duy nhất.
- Schema hiện cho phép description/unit/created_at NULL (migration 000001);
  không thêm NOT NULL chỉ để tránh viết nullable scanning.
- Dùng `iot_backend_app` cho application queries; administrator chỉ seed
  fixture trong DB isolated. Không grant thêm quyền hoặc đổi database role.

## 5. Checklist test và checkpoint tiếp theo

Task 2.3.1 trở đi viết test thất bại trước rồi implement để xanh. Checklist:

- User A/B isolation; user mới và admin không có mapping không thấy Gateway.
- Owner/operator/viewer đều đọc được Gateway/Sensor được cấp.
- Sensor có cùng sensor_id ở hai Gateway không bị trộn.
- Missing/inaccessible Gateway cùng 404; accessible Gateway không sensor 200.
- Invalid ID/query user_id không làm gọi query cho người khác.
- Nullable metadata, stable sorting, empty arrays, safe errors và headers.
- Revocation sau commit, deadline/cancellation, closed pool, row-scan errors.
- PostgreSQL integration dùng đúng backend role và JWT contract hiện có.
- GoTrue thật qua Nginx/Envoy; giữ signup denial và Auth regression.

| Mốc | Nội dung |
|---|---|
| M1 | Models/validation, repository, PostgreSQL harness và CI integration ban đầu |
| M2 | Service/wiring và GET Gateway list |
| M3 | GET Sensor list và isolation tests |
| M4 | GoTrue harness/CI regression cho hai API |
| M5 / CI-3 | Tài liệu nghiệm thu và toàn bộ checks xanh |

Chưa tạo branch/commit/push/PR trong Task 2.3.0. Khi triển khai các mốc tiếp,
chỉ push checkpoint xanh theo yêu cầu người dùng; feature branch cần draft PR
nhắm develop để CI hiện tại được kích hoạt. Không commit failing tests hoặc
tự sửa trạng thái hoàn thành trong file kế hoạch ngoài repository.

## 6. Kết quả Task 2.3.1–2.3.2 (dừng ở repository)

- Thêm model `Gateway`, `Sensor`, `Role` và validator Gateway ID đúng migration
  000010, không normalize caller input. Metadata nullable dùng pointer;
  JSON DTO/UTC serialization thuộc handler ở task sau, chưa được triển khai.
- Thêm Repository interface và PostgreSQL adapter. List Gateway join membership;
  list Sensor dùng một scoped LEFT JOIN để phân biệt parent rỗng với denial.
  GetGatewayRole không query admin status hoặc bypass membership.
- Adapter nhận context, SQL parameters, đóng rows và propagate scan/iteration
  errors. Constructor từ chối nil và typed-nil. Không thêm service hoặc routes.
- Thêm `scripts/test-stage2-authorization.sh` theo pattern isolated của admin
  harness: random credentials/resource name, loopback port, migrations hiện có,
  setup connection riêng, application queries bằng `iot_backend_app`, cleanup.
- Thêm job `authorization-integration` trong CI. Chưa push nên **chưa có bằng
  chứng GitHub đã chạy job mới**; CI xanh ở phần baseline chỉ là code cũ.

### TDD và guarantees

RED đã quan sát: `go test ./internal/gateway` lỗi compile vì các symbol
`ValidateGatewayID`, `Database`, `NewPostgresRepository`, `ErrNotFound` chưa
tồn tại. Đây là test cho capability mới, không phải lỗi setup/dependency.
GREEN: cùng unit suite và PostgreSQL integration pass sau khi implement.
Không tạo RED/GREEN checkpoint commits tự động vì chưa có yêu cầu commit;
bằng chứng giữ ở tests và tài liệu này để người dùng review tại mốc M1.

| Guarantee | Bằng chứng |
|---|---|
| Identifier allowlist/boundaries/reserved ID | `TestValidateGatewayID` |
| Nil/typed-nil DB bị từ chối | `TestRepositoryRejectsNilDatabase` |
| Context/parameters và một list query; errors/rows cleanup | `TestRepositoryErrorPaths` |
| Role query phân biệt denial với lỗi DB | `TestRoleErrors` |
| A/B isolation, owner/operator/viewer, admin không có mapping | `TestPostgresAuthorizationIntegration` |
| Stable ordering, nullable fields, sensor ID trùng giữa Gateway | Cùng integration test |
| Authorized empty Gateway khác missing/inaccessible Gateway | Cùng integration test |
| Membership revoke không đổi quyền Gateway khác | Cùng integration test |
| Cancellation, lock-induced deadlines cho cả ba operations, closed pool | Cùng integration test |

Lệnh kiểm chứng (repo root, Go chạy trong `src/`):

```bash
sh scripts/test-stage2-authorization.sh
```

Harness chạy `-race -count=1 -cover`: package mới **98,0%** khi gộp unit và
integration. Có thể đặt `AUTHORIZATION_TEST_COVERAGE_FILE` tới đường dẫn writable
để lưu profile. Unit `go test ./...` mặc định skip DB integration; job mới gọi
harness để test thật, không suy diễn unit PASS là database đã được kiểm chứng.
Nhánh defensive sensor name NULL chưa được cover vì schema name NOT NULL;
query/scan/iteration error branches đã có unit tests.

Regression sau thay đổi đã chạy và PASS: `go test -race -count=1 ./...`,
`go vet ./...`, `go build ./...`, 11 Python Auth harness tests và các script
Auth/admin/migration/smoke hiện có. Golangci-lint v2.14.0 kết quả **0 issues**;
Actionlint v1.7.7 kiểm tra workflow (tắt shellcheck) PASS. Không coi package
coverage 98,0% là coverage tổng repository hoặc bằng chứng HTTP authorization.

Tại thời điểm kết thúc Task 2.3.2, chưa có service/wiring/API. Kết quả bổ sung
Task 2.3.3–2.3.4 được ghi riêng bên dưới; giữ phân biệt với bằng chứng repository.

## 7. Kết quả Task 2.3.3–2.3.4

- Service validate UUID khác zero; Sensor read use-case validate Gateway ID
  trước query, nhưng chưa expose Sensor handler. Không nhận bearer token trong
  service. Constructor từ chối nil/typed-nil repository và timeout <= 0.
- Deadline lấy từ `AUTHORIZATION_TIMEOUT`, bao phủ cả operation. Context từ
  caller được propagate; dependency trả success sau deadline vẫn bị từ chối.
- `GET /v1/gateways` đăng ký qua authenticated group. Danh tính chỉ từ Principal,
  query key user_id (rỗng/lặp) bị 400; GET body không chọn danh tính.
- DTO đúng 5 fields đã chốt, nullable metadata, UTC timestamp, `items: []`,
  no-store. Lỗi DB 500 và timeout/cancellation 503 dùng safe error envelope;
  logs chỉ có request ID/operation/category, không raw DB errors.
- Main wire repository -> service -> router bằng pool hiện có. Thiếu reader
  (kể cả typed-nil) chặn router initialization. Health/stubs giữ nguyên.
- Adapter bỏ dependency Auth, dùng helper nil nội bộ để service không phụ
  thuộc package Auth/Gin. Không thêm migration, config hoặc dependencies.

### Bằng chứng TDD và integration

RED: `go test ./internal/gateway ./internal/httpserver` lỗi compile vì
NewService/ErrInvalidUser/GatewayReader chưa có. GREEN: unit suite pass sau
implement. Test DTO ban đầu đếm nhầm 6 fields, đã sửa thành 5 đúng contract;
không mở rộng response để làm test sai pass.

| Guarantee | Tests |
|---|---|
| Validation, no repository call khi input sai, deadline/cancellation/errors | TestReadService |
| JWT 401, user_id 400, body không giả danh, đúng DTO/UTC/no-store | TestGatewayListBoundary |
| Empty array, safe 500/503, request ID, log secrecy | TestGatewayListEmptyAndErrors |
| Handler fail closed khi thiếu principal | TestGatewayHandlerRejectsAbsentPrincipal |
| Nil/typed-nil reader chặn router initialization | TestRouterRejectsMissingGatewayReader |
| PostgreSQL thật + JWT test ký strict HS256, role matrix, isolation và revoke | TestGatewayHTTPAuthorizationIntegration |

Harness authorization giờ chạy cả package gateway và httpserver. Setup DB bằng
administrator; router/service/repository dùng `iot_backend_app`. Human JWT test
có issuer/audience/role/iat/exp/sub hợp lệ, secret CSPRNG riêng. Admin có
platform_admins nhưng không mapping nhận items rỗng; thêm một mapping chỉ thấy
đúng Gateway đó, không thấy Gateway user khác. Membership thu hồi phản ánh ở
request tiếp theo, user B không bị ảnh hưởng.

Đây là httptest router với PostgreSQL thật, **không phải GoTrue token thật qua
Nginx/Envoy** cho endpoint mới. Auth harness cũ vẫn pass nhưng không test GET
Gateways; mở rộng luồng đó thuộc Task 2.3.6. Chưa triển khai Task 2.3.5.

Local verification: Go race tests, vet/build, golangci-lint v2.14.0 (0 issues),
authorization harness và Auth/admin/migration/smoke regression PASS. Coverage
unit tổng repository 82,6%, package gateway 86,6%, httpserver 98,8%; service
methods và listGateways handler đạt 100%. Coverage
gateway khi chạy harness unit + DB integration 97,6%. Coverage httpserver của
harness chỉ chạy subset tests, không đại diện coverage toàn package.
Chưa commit/push, chưa xác nhận CI GitHub cho thay đổi này và chưa áp dụng deployment.

## 8. Kết quả Task 2.3.5 — Sensor list API

Thêm `GET /v1/gateways/{gateway_id}/sensors` qua authenticated group. Reader
lấy UUID từ Principal; validate Gateway ID trước gọi service; không parse GET
body thành identity. Query user_id (rỗng/lặp) bị 400. SensorReader là dependency
nhỏ bắt buộc của router, nil/typed-nil chặn startup wiring; main inject cùng
gateway.Service hiện có, không tạo pool hoặc query authorization thứ hai.

Response thành công giữ `gateway_id`, `items` luôn là array; mỗi sensor có
sensor_id/name/unit/created_at, nullable metadata giữ NULL, timestamp UTC và
no-store. Thứ tự từ SQL sensor_id ASC, không sort theo arrival/created_at.

Error mapping đã kiểm chứng:

| Trường hợp | Status / code |
|---|---|
| Missing/invalid JWT | 401 / unauthorized |
| Invalid matched Gateway ID/query user_id | 400 / invalid_request |
| Unknown/inaccessible Gateway | 404 / not_found, cùng message resource not found |
| Deadline/cancellation | 503 / service_unavailable |
| Internal DB errors | 500 / internal_error |

URL không match route vẫn dùng NoRoute. Không kiểm tra existence không scoped
để phân biệt Gateway của người khác với Gateway không tồn tại. Expected denial
không log chi tiết tài nguyên; unexpected failures chỉ log request ID, operation,
safe error category. Không log raw DB error, connection string hoặc token.

### TDD và verification

RED: `go test ./internal/httpserver` compile fail do SensorReader/listSensors
và RouterDependencies.SensorReader chưa tồn tại. Sau implement cùng unit suite
GREEN, không tạo checkpoint commit tự động.

| Guarantee | Test |
|---|---|
| JWT trước reader, matched invalid ID không gọi reader, query/body chống giả danh | TestSensorListBoundary |
| DTO 4 fields, NULL, UTC, no-store và thứ tự | TestSensorListBoundary |
| Empty Gateway 200, safe 404/500/503, request ID và log secrecy | TestSensorListErrorsAndEmpty |
| Missing principal không chạm reader | TestSensorHandlerMissingPrincipal |
| Nil/typed-nil SensorReader fail closed | TestRouterRejectsMissingSensorReader |
| A/B isolation, ba role, admin/no-member denial, cùng sensor ID ở hai Gateway | TestGatewayHTTPAuthorizationIntegration |
| Real PostgreSQL empty parent trước sensor provisioning; revoke và user B không ảnh hưởng | Cùng HTTP integration |
| Khóa bảng sensors -> timeout 503, unlock -> 200 | Cùng HTTP integration |

`sh scripts/test-stage2-authorization.sh` mở rộng chạy Sensor unit/HTTP tests
và PostgreSQL integration dưới `iot_backend_app`; setup fixture bằng admin
connection isolated. JWT test HS256 theo contract hiện có, không dùng secret
deployment. Harness resources đã cleanup, không sửa `.env` hoặc stack thật.

Verification local đã chạy: Go race tests `-count=1`, vet/build, golangci-lint
v2.14.0 **0 issues**, 11 Python harness tests, Auth/admin/migration/smoke
regression và authorization harness đều PASS. Unit coverage tổng **84,0%**;
httpserver **99,2%**, listSensors handler **100%**. Authorization harness
coverage gateway **97,6%**, httpserver **85,4%** (chỉ subset tests, không thay
thế coverage toàn package).

Tại thời điểm kết thúc Task 2.3.5, Auth harness cũ chưa gọi hai API mới với
GoTrue thật qua proxy. Kết quả bổ sung Task 2.3.6 được ghi riêng bên dưới để
không viết lại lịch sử bằng chứng M3. Task 2.3.7 nghiệm thu toàn bộ không được
đánh dấu xong chỉ vì Sensor HTTP test xanh.

## 9. Kết quả Task 2.3.6 — GoTrue và proxy authorization regression

Mở rộng `scripts/tests/stage2_auth.py`, không tạo Auth stack thứ hai. Harness
vẫn tạo user qua GoTrue Admin API với public signup disabled, login và refresh
qua `Nginx -> Envoy -> GoTrue`; sau đó dùng access token thật gọi:

```text
Client fixture -> Nginx -> Go backend -> iot_backend_app -> PostgreSQL
```

Không thêm test-only HTTP endpoint vào backend. Administrator DB connection
chỉ seed Gateway/Sensor/membership trong database isolated; business requests
vẫn đi qua backend database role và SQL scoped theo membership.

### Kịch bản đã kiểm chứng

1. Tạo hai human users bằng Auth Admin API; account mới có profile nhưng chưa
   có Gateway membership hoặc platform-admin grant.
2. Bootstrap user đầu thành platform admin. Trước khi cấp Gateway membership,
   `GET /v1/gateways` bằng token admin thật trả `items: []`: global admin không
   bypass API người dùng.
3. User thứ hai có role `operator` trên Gateway B, chỉ thấy Gateway B và Sensor
   B. User này không đọc được Sensor Gateway A (`404`).
4. Operator fixture cấp user admin role owner trên Gateway A và viewer trên
   Gateway Empty. Gateway list trả đúng thứ tự/role, không lộ Gateway B.
5. Sensor A trả `a,z` theo stable ordering, giữ nullable metadata; Gateway
   Empty trả `200 / items: []`. Gateway B vẫn `404` đối với admin chưa được cấp.
6. Thu hồi membership Gateway A. Cùng access token admin vẫn hợp lệ nhưng list
   chỉ còn Gateway Empty và Sensor A trả `404`; User B vẫn thấy Gateway B.
7. Login/refresh stable claims, public signup denial, human-token Admin API
   denial, platform-admin guard 204/403, JWT negative cases, existing stubs và
   container log secrecy tiếp tục PASS.

Fixture IDs ngẫu nhiên, truyền vào psql bằng JSON qua environment và psql
variable `:'fixture_json'`; không nội suy UUID/ID vào SQL text và không đưa DB
password/token vào argv. Containers/network/image do harness sở hữu được
cleanup; không source/sửa `.env`, database hay stack deployment.

### TDD và kiểm chứng

RED: thêm ba unit tests cho response assertions làm Python suite lỗi vì
`assert_gateway_list`, `assert_sensor_list`, `assert_resource_not_found` chưa
tồn tại. GREEN: 16 Python tests pass sau khi thêm strict shape/order/404 checks.
Thêm unit tests riêng xác nhận fixture/membership SQL dùng parameterized JSON
và secrets đi qua environment thay vì command arguments.

`sh scripts/test-stage2-auth.sh` PASS với dòng bằng chứng:

```text
PASS: real GoTrue tokens through Nginx enforce Gateway/Sensor membership and revocation
```

CI `auth-integration` hiện đã gọi script này nên không cần thêm job hoặc workflow
khác. Tuy nhiên thay đổi chưa push, vì vậy chưa có GitHub Actions run chứng minh
commit Task 2.3.6 xanh. Đây là local isolated evidence, không chứng nhận public
deployment đã cập nhật image/config hoặc JWT secret mẫu đã được rotate.
