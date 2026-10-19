# Báo cáo Triển khai Task 4.6 — HTTP Handlers và Server Composition

**Ngày lập:** 18/10/2026  
**Trạng thái:** Hoàn thành có giới hạn (Bounded Complete) — Review **WARNING**; đã sửa hai findings sau review, chưa có review lại.  
**Nhánh công tác:** `develop`  
**Git Baseline:** commit `f7a51bb1183fe59edc5ad9b5888aa67e677ce31c` + uncommitted diff của Task 4.6  
**Tài liệu liên quan:**
- Contract giao thức: `docs/protocols/stage4-read-and-entity-api.md`
- Kế hoạch chi tiết: `../docs/backend_plan/task_4.6_detail_plan.md`
- Báo cáo Task 4.1–4.5: `docs/backend/stage-4-task-4.{1,2,3,4,5}*.md`
- Nguyên tắc vận hành: `AGENTS.md` (mục 0, 16)

---

## 1. Mục tiêu và Phạm vi Hoàn thành

Task 4.6 hiện thực hóa tầng biên HTTP (HTTP transport boundary) và nối ghép (composition) toàn diện cho cả bảy endpoint thuộc Stage 4 đã được xây dựng dịch vụ ở Tasks 4.1–4.5:
1. Đăng ký và xử lý đúng 7 routes Stage 4 sau middleware xác thực Supabase JWT (`/v1`).
2. Thay thế hai stub `501 Not Implemented` trước đó (`GET /v1/telemetry/history` và `GET /v1/digital-twins`) bằng business handler hoàn chỉnh.
3. Đăng ký 5 routes mới (`POST /v1/digital-twins`, `GET /v1/digital-twins/:entity_id`, `PATCH /v1/digital-twins/:entity_id`, `GET /v1/digital-twins/:entity_id/state`, `GET /v1/digital-twins/:entity_id/history`).
4. Khởi tạo và liên kết các adapter cơ sở dữ liệu, orchestrator, parser và domain service trong `cmd/server/stage4.go` bằng helper tập trung `composeStage4Dependencies` / `populateStage4Dependencies`, đảm bảo fail-closed ngay khi khởi động nếu thiếu dependency hoặc cấu hình sai.
5. Áp dụng nhất quán chính sách Cache-Control (`no-store`), Pragma (`no-cache` cho mutation), CORS exact-origin, `X-Request-ID`, phân loại mã lỗi HTTP và media type (`application/ld+json` cho detail, `application/json` cho các phản hồi khác).
6. Bảo toàn toàn bộ các route và hành vi Stage 2 (các route admin PUT, owner PUT sensor, credential API).

---

## 2. Bảng Đối chiếu 7 Routes Trước và Sau Task 4.6

| Method | Path | Trạng thái trước Task 4.6 | Trạng thái sau Task 4.6 | Parser & Handler | Response DTO & Media Type | Phân quyền & Cache |
|---|---|---|---|---|---|---|
| `GET` | `/v1/telemetry/history` | Stub `501 Not Implemented` | Handler nghiệp vụ đọc dữ liệu đo lịch sử | `HistoryQueryParser` / `getTelemetryHistory` | `TelemetryHistoryResponseDTO`<br>`application/json; charset=utf-8` | Authenticated (yêu cầu JWT)<br>Lọc theo `user_gateways`<br>`Cache-Control: no-store` |
| `GET` | `/v1/digital-twins` | Stub `501 Not Implemented` | Handler danh sách thực thể Twin theo keyset pagination | `TwinQueryParser` / `listDigitalTwins` | `TwinListResponseDTO`<br>`application/json; charset=utf-8` | Authenticated<br>Lọc theo `user_gateways`<br>`Cache-Control: no-store` |
| `POST` | `/v1/digital-twins` | Chưa đăng ký (404 Not Found) | Khởi tạo mới hoặc exact replay Gateway/Sensor | `parsePostEntityRequest` / `postDigitalTwin` | Tagged Gateway/Sensor Response DTO<br>`application/json; charset=utf-8`<br>(201 Created / 200 OK) | Authenticated<br>Admin (Gateway/Sensor) hoặc Owner (Sensor)<br>`Cache-Control: no-store`<br>`Pragma: no-cache` |
| `GET` | `/v1/digital-twins/:entity_id` | Chưa đăng ký (404 Not Found) | Đọc đồ thị chi tiết thực thể dạng NGSI-LD JSON-LD | `TwinQueryParser` / `getDigitalTwinDetail` | `TwinDetailResponseDTO`<br>`application/ld+json; charset=utf-8` | Authenticated<br>Kiểm tra membership `user_gateways`<br>`Cache-Control: no-store` |
| `PATCH` | `/v1/digital-twins/:entity_id` | Chưa đăng ký (404 Not Found) | Cập nhật metadata (Gateway: name/description; Sensor: name) | `parsePatchEntityRequest` / `patchDigitalTwin` | Tagged Gateway/Sensor Response DTO<br>`application/json; charset=utf-8`<br>(200 OK) | Authenticated<br>Admin (Gateway) hoặc Admin/Owner (Sensor)<br>`Cache-Control: no-store`<br>`Pragma: no-cache` |
| `GET` | `/v1/digital-twins/:entity_id/state` | Chưa đăng ký (404 Not Found) | Đọc trạng thái trực tiếp (`reported_state`, `desired_state`) | Route param `entity_id` / `getDigitalTwinState` | `TwinStateResponseDTO`<br>`application/json; charset=utf-8` | Authenticated<br>Kiểm tra membership `user_gateways`<br>`Cache-Control: no-store` |
| `GET` | `/v1/digital-twins/:entity_id/history` | Chưa đăng ký (404 Not Found) | Đọc chuỗi thời gian temporal (`sample_interval_us`) hoặc ủy quyền telemetry (`value`) | `HistoryQueryParser` / `getDigitalTwinHistory` | `TwinHistoryResponseDTO`<br>`application/json; charset=utf-8` | Authenticated<br>Kiểm tra quan hệ Gateway/Sensor và membership<br>`Cache-Control: no-store` |

*Lưu ý:* Endpoint `/v1/ws` tiếp tục được giữ nguyên trạng thái stub `501 Not Implemented`. Các endpoint tương lai như `/v1/digital-twins/:entity_id/desired-state` hay `/v1/digital-twins/:entity_id/commands` hoàn toàn không được đăng ký trong router ở Task 4.6.

---

## 3. Kiến trúc Main Composition, Quản lý Cấu hình và Ngân sách Thời gian

### 3.1. Composition Helper (`src/cmd/server/stage4.go`)
Để `main.go` và các bộ kiểm thử tích hợp dùng chung một quy trình cấu hình duy nhất, helper `composeStage4Dependencies` khởi tạo đầy đủ 11 thành phần phụ thuộc của Stage 4:
1. `TelemetryHistoryReader`: `telemetry.TelemetryHistoryService`
2. `TwinListReader`: `digitaltwin.ReadService`
3. `TwinDetailReader`: `digitaltwin.ReadService`
4. `TwinStateReader`: `digitaltwin.ReadService`
5. `TwinHistoryReader`: `digitaltwin.ReadService`
6. `HistoryQueryParser`: `httpserver.HistoryQueryParser`
7. `TwinQueryParser`: `httpserver.TwinQueryParser`
8. `EntityCreator`: `digitaltwin.CreateService` (kết hợp `gateway.ProvisioningService` và `gateway.AdminOrOwnerSensorProvisioningService`)
9. `GatewayPatcher`: `digitaltwin.GatewayPatchService`
10. `SensorPatcher`: `digitaltwin.SensorPatchService`
11. `TwinMaxBodyBytes`: Giới hạn kích thước payload JSON (kế thừa từ `cfg.AdminMaxBodyBytes`, mặc định 16 KiB)

Hàm `populateStage4Dependencies` sao chép các thành phần này vào `httpserver.RouterDependencies` trước khi `httpserver.NewRouter` được gọi trong `main.go`.

### 3.2. Kiểm tra Ràng buộc Khởi động (Fail-Closed Guards)
Trước khi khởi tạo bất kỳ dịch vụ nào, `composeStage4Dependencies` kiểm tra nghiêm ngặt:
- Bắt buộc phải có `pool *pgxpool.Pool` (không nil).
- Bắt buộc phải có `gwProvisioner digitaltwin.GatewayProvisioner` (không nil).
- `cfg.AuthorizationTimeout` phải là số dương (`> 0`).
- `cfg.AdminMaxBodyBytes` phải nằm trong khoảng hợp lệ `[1, 1048576]` bytes.
- `config.ValidateHistoryConfig(cfg.History)` và `config.ValidateTwinConfig(cfg.Twin)` phải vượt qua kiểm tra cấu hình biên (giới hạn thời gian truy vấn, max points, pagination limit, allowlisted bucket resolutions).

Nếu bất kỳ điều kiện nào không thỏa mãn, ứng dụng sẽ dừng lại ngay lập tức (`fail-closed`) với thông báo lỗi rõ ràng, tuyệt đối không âm thầm dùng fallback rỗng hoặc stub để báo server sẵn sàng.

### 3.3. Tái sử dụng Database Pool và Quản lý Budget Context
- Toàn bộ các repository đọc và ghi Stage 4 sử dụng chung connection pool hữu hạn đã được cấu hình trong `main.go` (`pool`), không tạo kết nối phụ hay mở kết nối không giới hạn.
- Ngân sách đọc (Read budget): Orchestrator `ReadPreparation` và `HistoryPreparation` thực thi truy vấn phân quyền và truy vấn dữ liệu dưới **một deadline duy nhất** kế thừa từ request context, ngăn chặn tình trạng rò rỉ kết nối hoặc tạo ra budget cục bộ mới ở handler.
- Ngân sách ghi (Mutation budget): Các dịch vụ patch và provision sử dụng timeout `cfg.AuthorizationTimeout` (nhất quán với các thao tác provisioning của Stage 2).

---

## 4. Hành vi Biên HTTP (HTTP Boundary Behavior)

### 4.1. DTO Projection và An toàn Marshalling
- Quy tắc nghiêm ngặt: Handler thực hiện `json.Marshal` toàn bộ DTO trước khi ghi HTTP status code (`writeReadJSONResponse`, `writeReadJSONLDResponse`). Nếu xảy ra lỗi marshal hoặc DTO bị `nil`, handler sẽ chuyển sang trả về lỗi `500 Internal Server Error`, tuyệt đối không ghi `200 OK` dở dang kèm dữ liệu bị ngắt quãng.
- Số nguyên 64-bit (`sequence_number`, `sample_count`) được định dạng chuỗi thập phân (`string`) để tránh mất độ chính xác khi truyền qua JSON cho client JavaScript/Flutter.
- Các trường trạng thái JSONB (`reported_state`, `desired_state`) được bảo toàn nguyên vẹn qua `json.RawMessage`.

### 4.2. Media Type Phân định Rõ ràng
- `GET /v1/digital-twins/:entity_id` trả về `application/ld+json; charset=utf-8` khi thành công, chứa `@context` là inline object versioned (Version 1.1) từ mapper hiện hữu, không phải remote-context array.
- Tất cả các endpoint Stage 4 khác trả về `application/json; charset=utf-8`.
- Mọi phản hồi lỗi (kể cả lỗi từ detail endpoint) đều trả về `application/json; charset=utf-8` theo đúng chuẩn envelope `httpapi.ErrorResponse`.

### 4.3. Phân loại và Ánh xạ Mã lỗi HTTP (Error Mapping)
Hàm `writeReadError` và `writeMutationError` tuân thủ các quy tắc bảo mật sau:
1. **Ưu tiên 503 Service Unavailable:** Các lỗi phụ thuộc cơ sở dữ liệu (`context.DeadlineExceeded`, `context.Canceled`, `ErrDependencyUnavailable`, `ErrHistoryDependency`, `ErrGatewayProvisioningUnavailable`, `ErrAdminLookupFailed`) được kiểm tra trước tiên qua `errors.Is`. Điều này ngăn chặn việc đánh đồng lỗi mạng/timeout thành lỗi 404 (Not Found) hay 400 (Bad Request).
2. **400 Bad Request:** Bắt các lỗi phân tích cú pháp query (`*QueryError`) với mã lỗi allowlist ổn định (`invalid_cursor`, `invalid_resolution`, `raw_limit_exceeded`, `range_too_large`, `invalid_property`, `inconsistent_unit`), hoặc lỗi payload/URN từ parser.
3. **401 Unauthorized:** Xử lý bởi middleware JWT khi thiếu token, token hết hạn hoặc sai chữ ký (`WWW-Authenticate: Bearer`).
4. **403 Forbidden:** Ánh xạ từ `ErrForbidden` hoặc `ErrProvisioningForbidden` khi người dùng không đủ quyền trên tài nguyên.
5. **404 Not Found:** Ánh xạ từ `ErrNotFound` hoặc `ErrHistoryNotFound` khi thực thể không tồn tại hoặc người dùng không có quyền truy cập theo mô hình tenant isolation (`user_gateways`).
6. **409 Conflict:** Ánh xạ từ `ErrConflict` hoặc `ErrEntityConflict` khi có xung đột dữ liệu provisioning.
7. **413 Payload Too Large & 415 Unsupported Media Type:** Trả về khi request body vượt quá giới hạn cấu hình hoặc `Content-Type` không phải `application/json`.
8. **500 Internal Server Error:** Dành cho các lỗi bất thường của hệ thống, lỗi đồ thị không nhất quán hoặc lỗi marshal DTO. Error message gửi tới client được chuẩn hóa an toàn (`internal server error`). Mutation logs chỉ ghi outcome với `request_id`, `operation`, `status`, `error_code`, không ghi raw error hoặc chi tiết driver/database; lỗi projection cũng dùng cùng helper an toàn.

### 4.4. Cache-Control, Pragma, CORS và Request ID
- Middleware `stage4CacheMiddleware()` đặt `Cache-Control: no-store` cho toàn bộ 7 route Stage 4 (áp dụng cho cả phản hồi thành công và lỗi).
- Các thao tác mutation (`POST /v1/digital-twins`, `PATCH /v1/digital-twins/:entity_id`) được bổ sung thêm tiêu đề `Pragma: no-cache`.
- CORS áp dụng chính sách exact-origin đã cấu hình, cho phép các phương thức `GET`, `POST`, `PATCH`, `OPTIONS`, đồng thời expose header `X-Request-ID`. Các preflight request (`OPTIONS`) được xử lý bởi middleware CORS trước khi chạm đến middleware xác thực JWT.
- `X-Request-ID` được sinh hoặc đọc từ request header, tự động gắn vào context và nhật ký `slog` của từng request.

---

## 5. Nguồn gốc Xác minh và Bằng chứng Thực nghiệm (Verification Provenance)

### 5.1. Non-Native Checks (Preceding Worker Execution)
Toàn bộ các bộ kiểm thử đơn vị, router, handler fakes và kiểm tra tĩnh đã được thực thi và xác nhận đạt kết quả **PASS**:
- `go test -race -count=1 ./...`: Toàn bộ các package (`httpserver`, `cmd/server`, `digitaltwin`, `gateway`, `telemetry`, `config`, `auth`) đều PASS.
- Định dạng và phân tích mã nguồn: `gofmt -l .` sạch, `go vet ./...` PASS, `golangci-lint run` (0 issues), `go build ./...` PASS.
- Kiểm tra tính nguyên vẹn: `git diff --check` PASS (không có xung đột hoặc trailing whitespace).

### 5.2. Native HTTP Smoke Test (Earlier Worker Execution)
Kịch bản khép vòng HTTP Native được thực thi độc lập:
```bash
STAGE4_HTTP_TEST_NATIVE=1 go test -race -v -count=1 -run TestNativeStage4HTTP_Smoke ./cmd/server
```
- **Kết quả:** **PASS**
- **Môi trường cơ sở dữ liệu:** PostgreSQL 16.15 / TimescaleDB 2.29.2 trên môi trường chia sẻ kiểm thử (schema version 20), kết nối dưới quyền người dùng ứng dụng `iot_backend_app` (xác minh `rolsuper = false`, `rolcanlogin = true`).
- **Mã định danh phiên kiểm thử (Run ID):** `168a79df`.
- **Dọn dẹp dữ liệu (Cleanup Verification):** Hàm dọn dẹp kiểm tra số bản ghi còn sót lại trên toàn bộ các bảng liên quan sau khi kết thúc test: **0 bản ghi sót lại**.
- **Tiến trình kiểm thử thực tế trên TCP Listener thật (`httptest.NewServer`):**
  1. Admin POST Gateway với `owner_user_id`: `201 Created`, canonical URN; exact replay: `200 OK`. Quyền owner được tạo trong luồng provisioning, không phải bước gán riêng sau replay. Wire response không có trường `created`.
  2. Owner POST Sensor: `201 Created`, canonical URN; exact replay: `200 OK`.
  3. Nạp một lô 5 samples qua repository `Persist`; kiểm committed outcome. Đây là dữ liệu test nạp trực tiếp, **KHÔNG PHẢI MQTT ingress**.
  4. Owner đọc telemetry history raw: 5 points, sequence/value đầu đúng; bucket `1s`: 1 point, count `"5"` (avg 25.3 là giá trị kỳ vọng từ fixture, smoke assert số bucket và count).
  5. Owner đọc danh sách Twin: `200 OK`, 2 items. Đọc Gateway/Sensor detail: `200 OK`, media type JSON-LD; Gateway có `@context` không null, đúng ID/type. Smoke không assert riêng nội dung `hasSensor`.
  6. Owner đọc Sensor state: `200 OK`, `reported_version = "1"`; Twin history `value`: 5 points; `sample_interval_us`: 1 raw point có giá trị `100000`.
  7. Admin PATCH Gateway description và Owner PATCH Sensor name: `200 OK`. GET detail lại xác nhận description/name mới.
  8. Non-member User B đọc **Gateway detail** và **telemetry history**: `404 Not Found`; Gateway detail có error code/request ID đúng. Không có native assertions cho non-member list, Sensor detail/state/Twin history trong smoke này.
  9. Viewer và operator, mỗi role gửi PATCH Sensor, POST Sensor và PATCH Gateway: `403 Forbidden`. Không có request owner PATCH Gateway trong smoke này.
  10. Sau các denials, query DB xác nhận Sensor name, Gateway description và Sensor count không đổi so với giá trị mong đợi. **Không có so sánh state/history trước–sau metadata PATCH trong smoke này**; không suy ra invariant đó từ readback metadata.

*Lưu ý về Native Test Execution:* Run `168a79df` là bằng chứng kế thừa trước các bổ sung boundary tests và sửa logging sau review. Native smoke không chạy lại trong lượt sửa logging/tài liệu này; không có thay đổi nghiệp vụ, SQL hoặc assertions native. Một kiểm thử native bị bỏ qua được ghi nhận là **SKIP / NOT RUN**, không phải PASS mới.

---

## 6. Trạng thái Đánh giá (Review Status) và Ranh giới Tồn đọng

1. **Trạng thái Code Review:** `go-reviewer` (`openai/gpt-6.1-sol#high`) trả **WARNING** với hai MEDIUM: raw mutation error logs và báo cáo vượt native assertions. Đã sửa trực tiếp hai điểm, bao gồm mô tả inline `@context`; chưa review lại, không tự nâng verdict thành APPROVE. Regression `TestMutationHandlers_ErrorLogsExcludeRawCause` đã tái hiện lỗi trước sửa (6 cases FAIL do marker xuất hiện), rồi PASS sau sửa cho POST/PATCH với 500/503/403 và kiểm allowlisted log fields. Các kiểm tra hồi quy lượt sửa ghi bên dưới.
   - Tự chạy sau sửa: `go test -race -count=1 ./internal/httpserver ./cmd/server` **PASS** (native opt-in không bật); `go vet ./...`, `golangci-lint run ./...` (**0 issues**), `go build ./...`, `gofmt` hai file mutation và `git diff --check` **PASS**.
   - Không chạy lại native DB, không ghi DB, không gọi thêm reviewer/subagent và chưa commit. Phần native ở mục 5.2 chỉ là bằng chứng kế thừa đã thu hẹp theo assertions trong source.
2. **Reconciliation Task 4.5:** Task 4.5 đã được commit qua `0d46cf4`, `aab743e`, và `f7a51bb`. Báo cáo Task 4.5 trước đó ghi nhận hai điểm lưu ý ở mức WARNING về test/cleanup; không suy đoán Task 4.5 đã được phê duyệt độc lập mà giữ nguyên ghi chép lịch sử trung thực.
3. **Các hạng mục chưa thực hiện (Giao cho Task 4.7–4.8):**
   - **Task 4.7:** Kiểm thử tích hợp từ đầu đến cuối MQTT Telemetry Ingress $\rightarrow$ Database $\rightarrow$ HTTP History / Twin Read APIs; ma trận nghiệm thu tích hợp đa dịch vụ.
   - **Task 4.8:** Tích hợp pipeline CI (`.github/workflows/ci.yml`), đo đạc hiệu năng nén hypertable và lập tài liệu tổng kết toàn bộ Stage 4.
   - *Đính chính phạm vi & cam kết mã nguồn (Reconciliation ghi nhận ngày 18/10/2026):* Task 4.6 đã được commit đầy đủ qua các commit `df0846a`, `86b1371`, `4f1b4a4`, và `e6e304d`. Theo Kế hoạch tổng thể Stage 4 và Kế hoạch chi tiết Task 4.7, hai hạng mục **tích hợp CI pipeline** và **đo đạc hiệu năng nén hypertable** thuộc phạm vi của **Task 4.7** (đã được hiện thực và kiểm chứng trong Task 4.7), trong khi Task 4.8 chỉ đảm nhận việc lập tài liệu tổng kết Stage 4 (`stage-4-history-and-twin-api.md`) và cập nhật các hợp đồng client API.
   - **Các kiểm thử không thực hiện (NOT RUN):** Remote CI trên GitHub Actions, kiểm thử triển khai môi trường staging/production, kiểm tra nén tự động trên hypertable quy mô lớn.
4. **Điểm nối tái sử dụng cho Task 4.7:**
   - Khởi tạo router và service: Tái sử dụng `composeStage4Dependencies` và `populateStage4Dependencies` trong `cmd/server/stage4.go`.
   - Entrypoint chạy smoke kiểm tra nhanh:
     ```bash
     STAGE4_HTTP_TEST_NATIVE=1 go test -race -v -count=1 -run TestNativeStage4HTTP_Smoke ./cmd/server
     ```
