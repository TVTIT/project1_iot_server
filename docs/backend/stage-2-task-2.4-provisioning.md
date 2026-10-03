# Stage 2 — Task 2.4: Admin provisioning Gateway/Sensor

## 1. Phạm vi và trạng thái

Hai endpoint dưới đã được triển khai và đăng ký qua `groups.admin.PUT` trong
**worktree chưa commit**, theo kế hoạch Task 2.4 đã duyệt. Tài liệu này mô tả
code hiện tại, không phải xác nhận deployment hoặc nghiệm thu cuối.

```http
PUT /v1/admin/gateways/{gateway_id}
PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}
```

Task này chỉ tạo tài nguyên nghiệp vụ, owner ban đầu và graph Digital Twin
trong PostgreSQL. Không cấp/rotate/revoke MQTT credentials, không sửa
`password_file`/reload broker, không tạo MQTT credential events, không có
membership API, user-management API, self-claim, owner-facing sensor writes,
Gateway HTTP credentials/media, telemetry ingress, WebSocket streaming hay
device control/desired-state/command/outbox. URN mapper chưa phải NGSI-LD API
đầy đủ hoặc JSON-LD serializer.

## 2. Authentication và role policy

- Gửi `Authorization: Bearer <human_access_token>` theo contract Supabase JWT
  hiện có: signature, issuer, audience, thời hạn và human role `authenticated`.
  Thiếu/sai JWT, `anon` hoặc `service_role` trả `401`; `service_role` không phải
  human platform admin. Không lấy token hoặc actor từ query/body.
- Platform-admin middleware kiểm tra `platform_admins` trong PostgreSQL.
  Repository **kiểm tra lại trong transaction** trước mutation. Admin bị thu
  hồi sau middleware nhưng trước lần kiểm tra này bị từ chối (`403`). Đây
  không phải bảo đảm chống mọi revocation xảy ra sau lần recheck.
- `owner`, `operator`, `viewer` không có platform-admin grant riêng đều nhận
  `403`, kể cả khi được gán vào Gateway đích. Platform admin là actor; owner
  trong body chỉ là người admin chỉ định, không phải actor tự khai.
- Owner phải có sẵn trong `profiles`; API không tạo Auth user/profile. User
  provisioning vẫn dùng công cụ Supabase Auth quản trị được bảo vệ.
- `GET /v1/gateways` và `GET /v1/gateways/{gateway_id}/sensors` vẫn kiểm tra
  `user_gateways`: admin **không bypass** quyền đọc user API. Owner/operator/viewer
  đọc trong phạm vi Gateway được gán; sensor kế thừa quyền parent. Admin
  không có mapping không tự nhìn thấy Gateway. Non-member/missing Gateway
  trên Sensor GET cùng trả `404` để giữ read isolation.
- Các quyền command mục tiêu của operator trong AGENTS không được kích hoạt
  bởi Task 2.4; chưa có command slice được authorize/validate/verify.

## 3. Request/response contract

Các giá trị trong ví dụ là placeholder, không phải deployment IDs/secrets.
Thay bằng ID/profile được operator lựa chọn; không copy nguyên placeholder.

### Gateway PUT

```http
PUT /v1/admin/gateways/{gateway_id}
Authorization: Bearer <human_access_token>
Content-Type: application/json
```

```json
{
  "name": "<gateway name>",
  "description": null,
  "owner_user_id": "<existing nonzero profile UUID>"
}
```

Response `201 Created` khi tạo và commit thành công, hoặc `200 OK` cho retry
no-op hợp lệ; cùng shape, không có wrapper `items`:

```json
{
  "gateway_id": "<gateway_id>",
  "name": "<gateway name>",
  "description": null,
  "owner_user_id": "<owner UUID>",
  "entity_id": "urn:ngsi-ld:Gateway:<gateway_id>",
  "created_at": "<database timestamp in RFC3339 UTC, or null>"
}
```

### Sensor PUT

```http
PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}
Authorization: Bearer <human_access_token>
Content-Type: application/json
```

```json
{
  "name": "<sensor name>",
  "unit": null
}
```

Response `201`/`200` theo cùng quy tắc:

```json
{
  "gateway_id": "<gateway_id>",
  "sensor_id": "<sensor_id>",
  "name": "<sensor name>",
  "unit": null,
  "entity_id": "urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>",
  "created_at": "<database timestamp in RFC3339 UTC, or null>"
}
```

Server dựng URN; không trả internal Twin UUID, state hoặc credentials.
`created_at` lấy từ DB và chuyển UTC (có thể có fractional seconds); legacy
SQL NULL trả JSON `null`, không tự bịa timestamp.

### Validation và strict parsing

| Field | Quy tắc hiện tại |
|---|---|
| `gateway_id` | Path, case-sensitive; `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`; cấm `backend_service` |
| `sensor_id` | Cùng regex; không áp dụng reserved Gateway name `backend_service` cho Sensor |
| `name` | Required string, không null/whitespace-only, tối đa **256 UTF-8 bytes** |
| `description` | Optional string/null, tối đa **4096 UTF-8 bytes** |
| `unit` | Optional string/null, tối đa **64 UTF-8 bytes** |
| `owner_user_id` | Required string parse được thành nonzero UUID; phải tồn tại trong `profiles` |

Giới hạn là **bytes**, không phải số ký tự. Metadata giữ nguyên sau validation,
không trim/lowercase/normalize. Missing `description`/`unit` tương đương null;
`null` khác `""` khi so sánh retry. Metadata phải là UTF-8 hợp lệ, không NUL.

Parser chỉ nhận **một JSON object**. Từ chối malformed JSON, unknown fields,
duplicate keys (kể cả tên field được escape tương đương), trailing JSON,
missing required fields, sai kiểu/null ở field bắt buộc, invalid raw UTF-8,
Unicode surrogate không ghép đôi và NUL escape. Chỉ whitespace sau object
được chấp nhận. Không nhận `user_id`, `role`, `entity_id`, state/credential.
Mọi query string, kể cả dấu `?` rỗng, bị từ chối.

`Content-Type` phải là `application/json`; cho phép duy nhất tham số tùy chọn
`charset=utf-8` (value không phân biệt hoa/thường), không nhận tham số khác.
`ADMIN_MAX_BODY_BYTES` lấy qua config: mặc định **16384**, phạm vi hợp lệ
**1–1048576 bytes (1 MiB)**; startup fail nếu cấu hình sai. Body vượt limit
trả `413`; parser chỉ đọc tối đa limit + 1 byte.

Handlers đặt `Cache-Control: no-store`; request ID theo middleware hiện có.
Lỗi authentication/admin middleware có thể được trả trước khi handler chạy;
không coi header của handler là bảo đảm cho mọi lỗi middleware.

## 4. Atomic graph, retry và concurrency

Repository dùng pool hiện có, SQL parameterized và transaction `READ COMMITTED`
dưới role `iot_backend_app`; không mở rộng privilege hoặc thêm migration cho
Task 2.4. Schema/grants đến `000010` là prerequisites.

```text
Gateway mới: gateways + user_gateways(role=owner) + Gateway twin + twin_states
Sensor mới:  sensors + Sensor twin + twin_states + Gateway --hasSensor--> Sensor
```

Gateway owner/Twin/state và Sensor/Twin/state/relationship được commit atomically.
Sensor cần parent Gateway cùng Gateway twin/state đúng type, scope và name;
repository lock parent Gateway bằng `FOR UPDATE` trước khi resolve/tạo Sensor,
serialize provisioning trên cùng parent. Cùng Sensor ID trên hai Gateway tạo
hai URN khác nhau.

State mới dùng defaults schema:

```text
reported_state = {}; desired_state = {}
reported_version = 0; desired_version = 0
last_reported_at = NULL; last_desired_at = NULL; last_desired_by = NULL
```

Không seed online/desired configuration, temporal rows hoặc command/outbox.
Lỗi trước commit rollback toàn graph, không để lại partial creation.

- **201**: graph mới đã commit. Không có nghĩa Gateway online hoặc đã thực thi lệnh.
- **200**: cùng metadata nullable và owner, graph đầy đủ → no-op. Retry chỉ yêu
  cầu state tồn tại, không so sánh/reset state/version/timestamps đã tiến triển.
- **409**: cùng ID nhưng metadata khác, hoặc có đúng một owner khác owner yêu
  cầu. PUT này không phải update API, không silently overwrite.
- **500**: graph thiếu/sai, không có owner hoặc nhiều owner, thiếu Twin/state/
  `hasSensor`, conflicting orphan Twin. Không repair graph, thêm lại relationship
  hay khôi phục membership bị revoke/downgrade. Nếu requested owner profile đã
  mất, bước resolve owner có thể trả `404` trước khi kiểm tra retry graph.

Create dùng uniqueness constraints và `INSERT ... ON CONFLICT DO NOTHING ...
RETURNING`, không `DO UPDATE`, không global mutex. Request đồng thời cùng ID/
dữ liệu: một `201`, request kia `200`; khác dữ liệu: một winner `201`, request
kia `409` khi graph hợp lệ. Gateway retry đọc bằng statement riêng với snapshot
READ COMMITTED mới sau khi chờ winner commit.

Service tạo deadline riêng cho transaction từ `AUTHORIZATION_TIMEOUT` (mặc
định `2s`), không dựa vào timer middleware đã kết thúc sau lookup. Cancellation/
deadline trong transaction trả `503`; lock/query cũng nằm trong deadline.
Rollback cleanup dùng context nền riêng có giới hạn `5s` để caller cancellation
không ngăn trả connection về pool. Không có vòng tự retry DB vô hạn.

**Commit ambiguity:** response chỉ thành công sau `Commit` trả nil. Nếu mất
kết nối/response quanh commit, client không thể suy ra chưa ghi DB từ lỗi
`500`/`503` hay network timeout. Retry cùng path/body để kiểm tra representation;
nếu commit trước đã thành công và graph chưa đổi, nhận `200`. Không đổi owner
hoặc body để “sửa” ambiguity, không tuyên bố exactly-once hay rằng mọi lỗi commit
chắc chắn rollback. Revocation/metadata/graph thay đổi giữa các lần retry vẫn
áp dụng checks hiện tại và có thể trả lỗi.

## 5. Safe errors

Envelope dùng chung (request ID cũng được gán/chuyển tiếp qua `X-Request-ID`):

```json
{"error":{"code":"conflict","message":"resource conflict","request_id":"<request ID>"}}
```

| HTTP | Code | Message / tình huống |
|---|---|---|
| 401 | `unauthorized` | `authentication required` — thiếu/sai human JWT; `WWW-Authenticate: Bearer` |
| 403 | `forbidden` | `insufficient permissions` từ admin middleware; `access denied` khi transaction recheck bị revoke |
| 400 | `invalid_request` | `invalid request` — path/body/query/metadata sai |
| 404 | `not_found` | `resource not found` — owner profile/parent Gateway không tồn tại |
| 409 | `conflict` | `resource conflict` — representation/owner khác |
| 413 | `payload_too_large` | `payload too large` — vượt body limit |
| 415 | `unsupported_media_type` | `unsupported media type` — Content-Type sai |
| 503 | `service_unavailable` | `service unavailable` — admin lookup lỗi hoặc transaction cancellation/deadline |
| 500 | `internal_error` | `internal server error` — DB/commit/invariant lỗi khác |

Không expose SQL/constraint details, DSN, token hay credentials; provisioning
failure log chỉ ghi safe error code và request ID. Authentication/admin guards
chạy trước body validation; request bị deny không có quyền probe parser/DB.

## 6. Verification: lệnh và bằng chứng

Prerequisites: Go toolchain theo `src/go.mod`, Docker daemon, Python 3 và khả
năng pull/build pinned images. Chạy Go checks trong `src/`, harness tại repo root:

```bash
(cd src && go test -race -count=1 ./...)
(cd src && go vet ./... && go build ./...)
sh scripts/test-stage2-provisioning.sh
python3 -m unittest discover -s scripts/tests -p 'test_stage2_auth.py'
sh scripts/test-stage2-auth.sh
sh scripts/test-stage2-authorization.sh
sh scripts/test-stage2-admin.sh
sh scripts/test-stage2-migrations.sh
sh scripts/test-backend-smoke.sh
git diff --check
```

Provisioning harness dùng PostgreSQL/TimescaleDB isolated, ephemeral loopback
port, credentials ngẫu nhiên, migrations mount read-only; không source `.env`
hay dùng DB deployment. Test repository/router với `iot_backend_app`, race
detector và packages chạy serial để fault injection/cleanup không race fixture.
Cleanup chỉ xóa container/volumes thuộc harness; lỗi cleanup trả nonzero, không
global volume prune. Có thể xuất integration coverage bằng biến
`PROVISIONING_TEST_COVERAGE` trỏ tới file ở `/tmp/opencode`.

CI worktree thêm job `provisioning-integration`; `auth-integration` chạy Python
regression và GoTrue thật qua Nginx/Envoy: admin login → hai PUT → kiểm tra
Twin/state → owner GET thấy tài nguyên → non-member/admin-no-mapping không
bypass; giữ signup denial, login/refresh, metadata/NULL/UTC, log secrecy và cleanup.

### Bằng chứng workers trước (lịch sử)

| Nguồn | Kết quả workers trước đã chứng minh |
|---|---|
| Task 2.4.6, `/tmp/opencode/task246-unit.log` | Go unit/race/coverage PASS |
| Task 2.4.6, `task246-integration.log`, `task246-integration-coverage.log` | PostgreSQL provisioning/router PASS: atomic graph, rollback/fault injection, retry, concurrency, timeout và recheck admin |
| Task 2.4.6, `task246-admin.log`, `task246-authorization.log`, `task246-migrations.log`, `task246-smoke.log` | Admin, authorization, migration và container smoke PASS |
| Task 2.4.7, `/tmp/opencode/task247-auth-final.log` | GoTrue/proxy provisioning và Auth regression PASS; Python harness regression **23 tests PASS** theo worker handoff |
| Task 2.4.7, `task247-authz.log`, `task247-provisioning.log` | Authorization và provisioning regression PASS |

Task 2.4.6 có coverage unit + integration kết hợp phần feature **89.8%
(300/334 statements)**, từ `task246-unit.cover`, `task246-integration.cover`
và `task246-feature-combined.cover` ở `/tmp/opencode`. Đây là feature-scoped
evidence lịch sử, **không phải coverage toàn repo**, không phải measurement
mới sau mọi thay đổi Task 2.4.7/2.4.8. Logs/profiles là artifact local tạm thời,
không phải artifact đã commit; reviewer cuối cần chạy lại checks cho tree cuối.

### Final independent Task 2.4.8b — 2026-10-03

Đã review độc lập kế hoạch, diff so với HEAD
`887b281968845e8dd05e2e29ca5819cb26412637` và toàn bộ 21 files untracked.
Branch `develop`; không commit/push, không sửa migrations/grants hoặc `.env`.

| Check chạy mới | Kết quả local |
|---|---|
| `go test -race -count=1 -coverprofile=... ./...` | PASS |
| `go vet ./...`, `go build ./...`, `gofmt -l .`, `git diff --check` | PASS |
| Cross-build `CGO_ENABLED=0`: server linux/amd64, gateway linux/arm GOARM=7 | PASS |
| `golangci-lint run --timeout=5m`, bản **v2.14.0**, Go 1.27.1 | PASS sau sửa import groups/doc comments |
| `sh scripts/test-stage2-provisioning.sh`, race + coverage | PASS, cả hai top-level integration tests chạy thật, không SKIP |
| Authorization, admin, Auth/GoTrue/proxy, migrations, backend smoke harnesses | PASS |
| `python3 -m unittest discover -s scripts/tests` | **23 tests PASS** |
| Syntax scripts theo shebang (`sh -n` hoặc `bash -n`) | PASS |
| `actionlint v1.7.7 -shellcheck='' .github/workflows/ci.yml` | PASS; không có shellcheck binary |
| Ruff full rules trên hai Auth Python files | Còn **14 baseline findings**, không thêm findings mới; không phải gate CI hiện tại |
| Ruff `--select E9,F` trên hai Auth Python files | PASS |

Lần đầu lint Go phát hiện 8 lỗi goimports và 7 thiếu exported comments trong
feature mới; đã sửa tối thiểu, không thay đổi nghiệp vụ. Hai findings mới trong
Python cleanup regression (nested with và implicit `check=False`) cũng được
sửa; import sorting được sửa. Ruff còn lại là baseline: explicit subprocess
check, broad exception tại cleanup/redaction boundary, shebang của test file và
nested with của regression cũ; không mở rộng task để dọn code không liên quan.

**Coverage mới:** merge `unit.out` + `integration.out` theo cùng vị trí block,
lấy union block đã chạy (count > 0), cộng trọng số `numStatements`, không lấy
trung bình percentages. Cả hai profiles dùng atomic coverage/race.

- Production files mới: **302/336 statements = 89.88%** (10 files: identity
  mapper; model, repository interface/service và ba PostgreSQL adapters;
  Gateway/Sensor handlers; strict parser; interface không có executable statements).
- Bao gồm cả toàn bộ production files có sửa trong Task 2.4 (`identifier.go`,
  `router.go`, `config.go`, `cmd/server/main.go`): **467/572 = 81.64%**. Denominator
  gồm cả code cũ trong các files này, không loại startup/main để nâng điểm.
- Toàn repo Go, cùng union profiles: **762/887 = 85.91%**. Không phải coverage
  E2E process của server: `cmd/server`/`cmd/gateway` vẫn 0% trong Go profiles;
  smoke/GoTrue kiểm chứng executable riêng nhưng không cộng vào profile.

Logs/profiles/check outputs nằm ở `/tmp/opencode/task248b/`; artifacts tạm thời,
không commit. Selector CI/harness khớp cả `TestPostgresProvisioningIntegration`
và `TestGatewayProvisioningHTTPIntegration`, bao gồm Sensor/fault/deferred commit/
concurrency/revocation subtests. Packages serial, không `t.Parallel` trong fixtures
chia sẻ DB; triggers fault được gỡ trước HTTP fixtures. Không sửa deployment
containers, không để lại container test thuộc các harness sau cleanup.

Review không phát hiện blocker auth/SQL/atomicity/parser hoặc thay đổi scope.
Revocation sau transaction recheck và network ambiguity quanh commit vẫn là
ranh giới được mô tả ở trên, không tuyên bố authorization lock/exactly-once.

## 7. Nghiệm thu còn chờ và handoff

- M1–M6 là các checkpoint code/tests/docs sẵn sàng cho review, không phải sáu
  commit/push đã thực hiện. Independent integration review và local checks đã
  hoàn tất; remote CI trên SHA mới vẫn còn chờ, chưa nghiệm thu deployment.
- Baseline HEAD `887b281968845e8dd05e2e29ca5819cb26412637` và CI run
  `37048004726` thành công chỉ chứng minh **baseline trước changes**. Các thay
  đổi hiện chưa commit; tiêu chí remote M6/CI-4 đã đạt, CI xanh tại commit `abcfbbadf563a45da87db9d84a8b7ca6aa996864`.
- Chưa xác minh production `.env`, public signup denial trên deployment thực,
  secret rotation hay trạng thái broker. Isolated harness PASS không thay thế
  các kiểm chứng deployment đó.
- Task 2.5–2.6 tiếp tục MQTT runtime credential provisioning/reload và Admin
  credential API; không dựa vào Task 2.4 để tuyên bố Gateway đã kết nối được
  broker. Membership vận hành vẫn qua công cụ operator được bảo vệ, không
  phục hồi bằng retry provisioning.

Tham khảo: [authentication](stage-2-task-2.2-authentication.md),
[centralized accounts](stage-2-task-2.2A-centralized-accounts.md),
[User–Gateway authorization](stage-2-task-2.3-authorization.md), `AGENTS.md`.
