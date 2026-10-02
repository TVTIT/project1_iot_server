# Giai đoạn 2 — Task 2.3: Authorization và API đọc Gateway/Sensor

## 1. Trạng thái và phạm vi

**Task 2.3.0 hoàn thành:** kiểm tra baseline và chốt contract dưới đây.
**Task 2.3.1–2.3.2 đã triển khai và kiểm chứng local isolated**;
**Task 2.3.3–2.3.7 chưa triển khai**. Tài liệu này không chứng minh hai API đã
hoạt động. Router hiện chưa đăng ký hai route đọc Gateway/Sensor, nên nhận
`404` từ NoRoute, không phải authenticated stub `501`.

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

Task 2.3.3 sẽ kiểm tra service inputs (bao gồm zero user UUID), timeout use-case
và wiring ở các task tiếp. Repository hiện không cấp quyền cho UUID zero khi
không có membership; không tuyên bố service validation đã hoàn thành. Không
claim JWT thật đã dùng với repository mới hoặc hai HTTP API đã tồn tại.
