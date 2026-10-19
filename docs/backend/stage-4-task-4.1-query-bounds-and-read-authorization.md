# Task 4.1 — Query model, bounds và authorization đọc

**Ngày thực hiện:** 17/10/2026.  
**Baseline:** commit `2ce3a89ebd6b95f46e17cd535c5400f2ba5ca6bd`, bảo toàn contract Task 4.0 chưa commit tại `docs/protocols/stage4-read-and-entity-api.md`.  
**Trạng thái:** Hoàn thành có giới hạn (bounded-complete) phần code nền tảng, parser, bounds config, authorization resolution và test fakes tự viết (handwritten fakes); chưa kích hoạt HTTP routes và chưa chạy native DB integration; đã hoàn thành review bởi `go-reviewer` (model `9router/gemini-flash-latest#high`, không có blocker, đã đính chính mô tả công cụ test fake trong tài liệu; không phải chứng nhận/phê duyệt độc lập bên thứ ba hay chứng nhận production).

Tài liệu này tổng kết việc thực hiện Task 4.1 theo [kế hoạch chi tiết Task 4.1](../../../docs/backend_plan/task_4.1_detail_plan.md), [kế hoạch Stage 4](../../../docs/backend_plan/stage4_backend_detailed_plan.md) và [contract thiết kế Task 4.0](../protocols/stage4-read-and-entity-api.md).

---

## 1. Phạm vi thực hiện

Task 4.1 thiết lập toàn bộ nền tảng truy vấn đọc, phân tích cú pháp, ràng buộc tham số và giải quyết quyền truy cập (read authorization) trước khi dữ liệu thực tế được đọc ở các task tiếp theo:

1. **Parser & Typed Query Models:** Cung cấp parser cho cả 5 endpoint đọc Stage 4 (`GET /v1/telemetry/history`, `GET /v1/digital-twins`, `GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}/state`, `GET /v1/digital-twins/{entity_id}/history`).
2. **Strict URN & Keyset Cursors:** Parser URN chuẩn hóa (`urn:ngsi-ld:Gateway:<gateway_id>` và `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`), phân tách hai cursor pagination base64url độc lập cho danh sách entity (`{k,g,t}`) và danh sách quan hệ (`{s,r,k}`).
3. **Cấu hình Bounds & Deadlines:** Nhóm tham số cấu hình `HISTORY_*` và `TWIN_*` với hard bounds bảo vệ, đồng bộ giữa Go loader, `.env.example`, `docker-compose.yml` và test runner smoke.
4. **Membership-Scoped Resolution:** Adapter `PostgresResolver` giải quyết Gateway, Sensor và Entity dựa trên membership thực tế trong `public.user_gateways`.
5. **Single-Budget Preparation Layer:** Lớp điều phối `ReadPreparation` và `HistoryPreparation` thực thi ngân sách thời gian duy nhất cho cả quá trình kiểm tra quyền và đọc dữ liệu qua callback.

---

## 2. Cấu hình, files và internal APIs đã thêm

### 2.1. Biến cấu hình mới

| Biến môi trường | Default contract | Hard cap / Giới hạn | Ý nghĩa |
| --- | --- | --- | --- |
| `HISTORY_QUERY_TIMEOUT` | `5s` | Max `5s` | Deadline cho toàn bộ thao tác đọc history (resolve + data read) |
| `HISTORY_MAX_RANGE` | `744h` (31 ngày) | Max `744h` | Cửa sổ thời gian truy vấn tối đa |
| `HISTORY_RAW_WINDOW` | `10s` | Max `10s` | Khoảng thời gian tối đa cho truy vấn raw resolution |
| `HISTORY_DEFAULT_POINTS`| `1000` | — | Số điểm mặc định khi client không chỉ định |
| `HISTORY_MAX_POINTS` | `2000` | Max `2000` | Giới hạn trên số điểm mẫu tối đa |
| `HISTORY_MIN_POINTS` | `100` | Min `100` | Giới hạn dưới số điểm mẫu tối thiểu |
| `TWIN_LIST_DEFAULT_LIMIT`| `50` | — | Limit mặc định cho `GET /v1/digital-twins` |
| `TWIN_LIST_MAX_LIMIT` | `200` | Max `200` | Limit tối đa cho danh sách Digital Twin |
| `TWIN_RELATIONSHIP_DEFAULT_LIMIT` | `50` | — | Limit mặc định cho quan hệ `hasSensor` |
| `TWIN_RELATIONSHIP_MAX_LIMIT` | `200` | Max `200` | Limit tối đa cho quan hệ `hasSensor` |

### 2.2. Danh sách files mã nguồn mới và cập nhật

- **`src/internal/config/`**:
  * `read_bounds.go` & `read_bounds_test.go`: Khai báo `HistoryConfig`, `TwinConfig`, default values, validation logic chống tràn số và vượt hard caps.
  * `config.go`: Bổ sung nạp `History` và `Twin` vào struct `Config` chung.
- **`src/internal/telemetry/`**:
  * `history_query.go` & `history_query_test.go`: Khai báo `TelemetryHistoryQuery`, `TimeRange`, selector `value`, validation logic cho `[from, to)` và bucket calculation lý thuyết.
  * `history_preparation.go` & `history_preparation_test.go`: Điều phối `HistoryPreparation`, deadline duy nhất và callback `HistoryCallback`.
- **`src/internal/digitaltwin/`**:
  * `protocol.go` & `protocol_test.go`: Định nghĩa hằng số contract Stage 4, độ dài tối đa query (1024B), cursor (768B/512B), URN (160B).
  * `identity.go` & `identity_test.go`: Strict parser cho NGSI-LD URN, từ chối double-decoding, kiểm tra Gateway ID và Sensor ID.
  * `cursor.go` & `cursor_test.go`: Keyset cursor encoder/decoder base64url không padding cho List (`v1.{k,g,t}`) và Relationship (`v1.{s,r,k}`).
  * `errors.go`: Các sentinels lỗi chuẩn hóa miền nghiệp vụ (`ErrNotFound`, `ErrInvalidInput`, `ErrDependencyUnavailable`, `ErrNilActor`, v.v.).
  * `interfaces.go`: Định nghĩa `EntityResolver`, `GatewayResolver`, `ResourceResolver`, `ReadPreparationService`.
  * `validation.go` & `validation_test.go`: Xác thực các query struct `TwinListQuery`, `TwinDetailQuery`, `TwinStateQuery`, `TwinHistoryQuery`.
  * `read_model.go` & `read_model_test.go`: Typed domain structs cho các thực thể đã được resolve.
  * `postgres_resolver.go` & `postgres_resolver_test.go`: Adapter truy vấn PostgreSQL với `JOIN public.user_gateways`.
  * `read_preparation.go` & `read_preparation_test.go`: Lớp chuẩn bị context, validate bounds và ủy quyền qua callback.
- **`src/internal/httpserver/`**:
  * `history_query_parser.go` & `history_query_parser_test.go`: Parser HTTP query cho telemetry history với cap 1024 bytes, phân tích RFC3339 lexical precision.
  * `twin_query_parser.go` & `twin_query_parser_test.go`: Parser HTTP query cho Digital Twin list, detail, state và history.
- **`src/internal/gateway/`**:
  * `postgres_inventory_errors.go` & `postgres_owner_provisioning_errors.go`: Export `ClassifyPostgresAvailabilityError` để tái sử dụng trong phân loại lỗi kết nối cơ sở dữ liệu.
- **Tài liệu cấu hình & kiểm thử**:
  * `.env.example`, `docker-compose.yml`, `scripts/tests/native_compose_smoke.py`: Bổ sung khai báo 10 biến bounds Stage 4.

### 2.3. APIs nội bộ cốt lõi

- **HTTP Query Parsers**: `ParseTelemetryHistoryQuery(rawQuery, maxPointsDefault, maxRange, rawWindow, minPts, maxPts)` và `ParseTwinListQuery`, `ParseTwinDetailQuery`, `ParseTwinStateQuery`, `ParseTwinHistoryQuery`.
- **URN Parsing & Validation**: `ParseEntityURN(raw string) (ParsedIdentity, error)`.
- **Cursor Encoders/Decoders**: `EncodeListCursor(c ListCursor)`, `DecodeListCursor(token string)`, `EncodeRelationshipCursor(c RelationshipCursor)`, `DecodeRelationshipCursor(token string)`.
- **Resource Resolvers**: `EntityResolver.ResolveEntity(ctx, actor, parsed)`, `telemetry.SensorResolver.ResolveSensor(ctx, actor, gatewayID, sensorID)`.
- **Preparation & Budget Orchestration**:
  * `HistoryPreparation.PrepareTelemetryHistory(parentCtx, actor, q, fn HistoryCallback)`
  * `ReadPreparation.PrepareList(parentCtx, actor, q, fn ListCallback)`
  * `ReadPreparation.PrepareDetail(parentCtx, actor, q, fn DetailCallback)`
  * `ReadPreparation.PrepareState(parentCtx, actor, q, fn StateCallback)`
  * `ReadPreparation.PrepareHistory(parentCtx, actor, q, fn HistoryCallback)`

---

## 3. Ngữ nghĩa Single Budget Deadline trong Callback

Một trong những yêu cầu nghiêm ngặt của kiến trúc Stage 4 là kiểm soát thời gian xử lý:

1. **Ngân sách thời gian thống nhất:** Khi `Prepare*` được gọi, một context timeout được tạo lập: `ctx, cancel := context.WithTimeout(parentCtx, cfg.QueryTimeout)`. Ngân sách này (mặc định 5s, hoặc sớm hơn nếu `parentCtx` có deadline ngắn hơn) bao trùm **toàn bộ** vòng đời: từ acquire kết nối DB, truy vấn kiểm tra quyền trong `user_gateways`, cho đến thao tác đọc dữ liệu thực tế do callback `fn(ctx, ...)` thực hiện.
2. **Không cấp mới hoặc restart ngân sách:** Tuyệt đối không có cơ chế cấp thêm 5s cho các subquery tiếp theo hoặc làm mới thời gian sau khi hoàn tất kiểm tra quyền.
3. **Chống báo thành công giả khi quá hạn:** Nếu context bị hủy (`Canceled`) hoặc hết hạn (`DeadlineExceeded`), kể cả khi callback bên trong trả về `nil` (do fake callback hoặc do race), tầng preparation luôn kiểm tra lại `ctx.Err()` và chuyển đổi thành lỗi phụ thuộc `ErrDependencyUnavailable` (hoặc `ErrHistoryDependency`), ngăn chặn việc trả về empty success sai sự thật.

---

## 4. Cơ chế phân quyền và giới hạn của Handwritten Test Fakes

### 4.1. Phân quyền truy cập dựa trên Membership

- Toàn bộ truy vấn giải quyết tài nguyên (`ResolveGateway`, `ResolveSensor`, `ResolveEntity`) đều bắt buộc nhận `actor uuid.UUID` lấy từ Supabase JWT đã xác thực.
- SQL resolver thực hiện `JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1`.
- Nếu người dùng không có membership trong `user_gateways`, truy vấn trả về `pgx.ErrNoRows`, được ánh xạ thành `ErrNotFound` / `ErrHistoryNotFound` nhằm tránh làm lộ sự tồn tại của Gateway/Sensor đối với người dùng không có quyền.
- Platform Admin không có cơ chế bypass quyền đọc tự động ở tầng này.
- Thao tác `PrepareList` đại diện cho tập hợp scoped theo membership; nếu actor không sở hữu Gateway nào, kết quả trả về danh sách rỗng (`[]`) chứ không phải 404.

### 4.2. Cảnh báo quan trọng: Handwritten Test Fakes != Native Database Proof

- Các kiểm thử đơn vị trong `postgres_resolver_test.go` sử dụng các test fakes tự viết (`fakeDatabase`, `fakeRow`) để xác nhận cấu trúc câu lệnh SQL, thứ tự truyền tham số `$1, $2...`, các phép JOIN và việc ánh xạ lỗi `pgx.ErrNoRows` hoặc `conn refused` (không sử dụng thư viện ngoài `pgxmock` hay `sqlmock`).
- **Giới hạn kỹ thuật:** Việc kiểm thử với test fakes thành công **KHÔNG THỂ THAY THẾ** và **KHÔNG PHẢI LÀ BẰNG CHỨNG** cho thấy hệ thống tương thích hoàn toàn với PostgreSQL 16 / TimescaleDB thật, Row-Level Security (RLS) hay kế hoạch thực thi (query plan). Việc chứng minh tính tương thích trên cơ sở dữ liệu thực tế được bảo lưu cho Task 4.2–4.4 và Task 4.7.

---

## 5. Kết quả kiểm tra đã thực hiện

Toàn bộ các kiểm tra sau đây đã được hoàn thành trước khi lập tài liệu bàn giao:

| Kiểm tra | Lệnh / Phạm vi | Kết quả | Ghi chú |
| --- | --- | --- | --- |
| Unit Race Tests | `go test -race -count=1 ./internal/telemetry ./internal/digitaltwin ./internal/httpserver ./internal/config ./internal/gateway ./internal/auth ./internal/mqttingress ./cmd/server ./cmd/gateway` | **PASS** | Kiểm tra toàn bộ package liên quan, không phát hiện race condition |
| Go Vet | `go vet ./...` | **PASS** | Không có cảnh báo tĩnh |
| Static Linting | `golangci-lint run ./...` | **PASS (0 issues)** | Toàn bộ 0 issue |
| Compilation | `go build ./...` | **PASS** | Biên dịch thành công toàn bộ backend và gateway CLI |
| Format Check | `gofmt -l .` | **PASS (empty)** | Toàn bộ codebase đã được định dạng đúng |
| Native Compose Smoke | `native_compose_smoke.py` | **ĐÃ CẬP NHẬT BIẾN** | Bổ sung 10 biến Stage 4 vào DEFAULTS |

*Ghi chú về kiểm thử runtime:* Các kiểm thử native database và live HTTP route **CHƯA CHẠY** (NOT RUN), các endpoint Stage 4 trên HTTP server vẫn giữ nguyên trạng thái stub `501 Not Implemented`.

---

## 6. Trạng thái Review và Bàn giao Tasks 4.2–4.7

### 6.1. Kết quả Review mã nguồn và đính chính kỹ thuật
Quá trình review bởi `go-reviewer` (model `9router/gemini-flash-latest#high`) đã hoàn tất với kết quả **KHÔNG CÓ BLOCKER**. Các kết quả kiểm tra thực tế được ghi nhận:
- **Kiểm thử tĩnh & race:** Race tests tập trung 9 packages (`telemetry`, `digitaltwin`, `httpserver`, `config`, `gateway`, `auth`, `mqttingress`, `cmd/server`, `cmd/gateway`) **PASS**; `go vet ./...` và `golangci-lint` sạch (0 issues); Python test suite 792/792 tests **PASS**; `go build ./...` và `gofmt` sạch.
- **Ranh giới kiểm thử:** Native DB và live HTTP **CHƯA CHẠY** (NOT RUN), các endpoint HTTP Stage 4 vẫn giữ stub `501 Not Implemented`.
- **Đính chính mô tả công cụ test:** Bộ test trong `postgres_resolver_test.go` sử dụng các test fakes tự viết (`fakeDatabase`, `fakeRow`), không sử dụng thư viện ngoài `pgxmock` hay `sqlmock`. Tài liệu đã được đính chính chính xác.
- **Quan sát tối ưu (non-blocking):** Việc parse lại URN trong một số nhánh kiểm tra được ghi nhận là dư thừa nhẹ nhưng hoàn toàn không ảnh hưởng chức năng (không phải lỗi nghiệp vụ); mã nguồn được giữ nguyên và ghi nhận như một dọn dẹp tùy chọn (deferred cleanup) cho các đợt refactor sau mà không tạo thêm framework backlog mới.
- **Lưu ý ranh giới:** Việc hoàn thành review nội bộ này không cấu thành phê duyệt/chứng nhận độc lập từ bên thứ ba (independent provider approval) hay chứng nhận sẵn sàng môi trường production.

### 6.2. Kế hoạch tiếp nối cho các Task còn lại
- **Task 4.2:** Triển khai truy vấn dữ liệu raw từ hypertable TimescaleDB, tính toán bucket thích ứng (`time_bucket`), xử lý overflow và gap detection.
- **Task 4.3:** Triển khai truy vấn danh sách Twin theo keyset pagination, detail graph với quan hệ `hasSensor`, current state và JSON-LD mapper.
- **Task 4.4:** Triển khai truy vấn temporal history, ủy quyền selector `value` về raw telemetry engine.
- **Task 4.5:** Triển khai POST/PATCH metadata của entity với transaction đảm bảo tính nhất quán của quan hệ đồ thị.
- **Task 4.6:** Đăng ký các HTTP handlers chính thức, thay thế 2 stub 501 hiện tại.
- **Task 4.7:** Thực hiện ma trận tích hợp trên container thật, đo kiểm hiệu năng truy vấn và tích hợp CI lane.
