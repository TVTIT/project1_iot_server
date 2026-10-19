# Báo Cáo Kỹ Thuật Task 4.3 — Digital Twin List, Detail Graph, Current State Và Mapper

**Ngày lập:** 2026-10-17
**Nhánh:** `develop`
**Baseline commit:** `ca53aea76ed121fae6d1f81a5c93037cc2958f9f` ("feat(telemetry): triển khai engine truy vấn lịch sử telemetry raw và time_bucket thích ứng")
**Trạng thái worktree:** **Chưa commit (uncommitted / dirty worktree)** trên nhánh `develop`. Tuyệt đối không giả mạo commit hash.

---

## 1. Mục Tiêu & Ranh Giới Kỹ Thuật

Task 4.3 hoàn thành trọn vẹn lớp đọc Digital Twin (list, detail graph, current state) và hệ thống mapper/DTO projection theo đúng thiết kế Stage 4 contract ([docs/protocols/stage4-read-and-entity-api.md](../protocols/stage4-read-and-entity-api.md)), kế thừa nền tảng kiểm soát bounds và preparation từ Task 4.1:
- Đọc danh sách Twin phân trang bằng keyset pagination theo chuẩn collation `"C"` dựa trên public URN, bị chặn chặt chẽ trong phạm vi membership của người dùng qua bảng `public.user_gateways`.
- Đọc chi tiết entity (Gateway và Sensor) kèm xác minh provisioning graph hợp lệ: Gateway kèm phân trang quan hệ `hasSensor`; Sensor kèm đơn vị đo và kiểm tra quan hệ gắn kết với Gateway cha; đảm bảo cô lập hoàn toàn giữa các Gateway sở hữu Sensor ID trùng tên.
- Đọc current state trực tiếp từ bảng quan hệ `public.twin_states`, bảo toàn định dạng `json.RawMessage` JSONB (chống mất độ chính xác với các số nguyên `> 2^53`), chuyển đổi version thành chuỗi số nguyên thập phân, không quét hypertable TimescaleDB, và trả lỗi `ErrUnexpected` (lỗi dữ liệu / data fault) khi entity hợp lệ nhưng thiếu row trạng thái trong `twin_states` (không tự bịa `{}`), phân biệt rõ với thực thể không tồn tại hoặc không có quyền truy cập (`ErrNotFound`).
- Điều phối kiểm tra quyền và truy vấn dữ liệu dưới một deadline context duy nhất thông qua `ReadPreparation`, bảo đảm fail-closed khi context bị hủy hoặc quá hạn mà không để lộ kết quả một phần (partial result).
- Chuẩn hóa hệ thống mapper với inline context version 1.1, các cấu trúc `PropertyDTO`, `RelationshipDTO`, `RelationshipPageDTO` và wire response DTOs (`TwinListResponseDTO`, `TwinDetailResponseDTO`, `TwinStateResponseDTO`).

**Ranh giới kỹ thuật rõ ràng:**
- **Trạng thái HTTP routes:** Route `GET /v1/digital-twins` là stub `501 Not Implemented` hiện hữu trong router; các route detail và state (`GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}/state`) hiện chưa được đăng ký trong router (sẽ được đăng ký và gắn handler tại Task 4.6). Không tuyên bố kiểm chứng trạng thái live HTTP trong task này.
- **Chưa ủy quyền temporal history:** Phương thức `GetTwinHistory` của `readService` trả lỗi an toàn `ErrUnexpected`; logic đọc lịch sử temporal thuộc Task 4.4.
- **Không ghi dữ liệu hay sửa metadata:** Thao tác POST/PATCH metadata thuộc Task 4.5; các use case Task 4.3 là thuần đọc (read-only), không làm thay đổi trạng thái database hay outbox.
- **Chưa kiểm thử live HTTP hoặc MQTT-to-HTTP end-to-end:** Phạm vi tích hợp toàn diện và kiểm thử live thuộc Task 4.7.
- **Chưa đo đạc nén TimescaleDB:** Dữ liệu trong kiểm thử native thực thi trên các chunk chưa nén; kiểm chứng nén thuộc Task 4.7.

---

## 2. Kiến Trúc & Các Thành Phần Đã Triển Khai

### 2.1. Phân Trang Keyset Cho Danh Sách Twin (`ListEntities`)
- **SQL & Scoping:** Truy vấn `public.twin_entities` kết hợp `JOIN public.user_gateways` dựa trên actor `user_id`:
  ```sql
  SELECT e.entity_id, e.entity_type, e.name, e.gateway_id, e.updated_at
  FROM public.twin_entities AS e
  JOIN public.user_gateways AS ug ON ug.gateway_id = e.gateway_id AND ug.user_id = $1
  WHERE e.entity_type IN ('Gateway', 'Sensor')
    ...
  ORDER BY e.entity_id COLLATE "C" ASC
  LIMIT $limit_plus_one
  ```
- **Giới hạn Entity Type phía máy chủ:** Truy vấn luôn kèm vị từ server-owned `e.entity_type IN ('Gateway', 'Sensor')`, độc lập với bộ lọc `entity_type` tùy chọn. Điều này loại bỏ hoàn toàn các thực thể không thuộc phạm vi hỗ trợ (như `Device`) cả trong danh sách không lọc lẫn khi phân trang bằng cursor.
- **Bộ lọc (Filters):** Hỗ trợ lọc tùy chọn theo `gateway_id` và `entity_type` (`Gateway` hoặc `Sensor`).
- **Keyset Cursor:** Sử dụng định dạng cursor chuẩn `v1.{k,g,t}` được mã hóa base64url. Cursor lưu giữ public URN (`k`), bộ lọc Gateway (`g`), và kiểu entity (`t`). Điều kiện tiếp tục sử dụng toán tử so sánh `e.entity_id COLLATE "C" > $cursor_urn`.
- **Phát hiện trang kế:** Truy vấn với giới hạn `limit + 1`. Nếu kết quả trả về `> limit`, hệ thống cắt gọn danh sách về đúng `limit`, lấy item cuối cùng đã trả để mã hóa `next_cursor`. Nếu không còn dòng dư, `next_cursor` trả về `nil`.
- **An toàn Tenant:** Người dùng không có quyền trên Gateway được yêu cầu sẽ nhận về danh sách rỗng (`items = []`, `next_cursor = nil`), không làm lộ thông tin hệ thống. Cursor giả mạo hoặc có bộ lọc không khớp sẽ bị từ chối ngay lập tức với `ErrInvalidCursor`.

### 2.2. Đọc Đồ Thị Chi Tiết & Phân Trang Quan Hệ (`GetDetail`)
- **Gateway Detail (Truy vấn đơn nguyên khối với CTE và LATERAL):**
  - Thực thi thông qua một câu lệnh SQL tham số hóa duy nhất kết hợp `WITH authorized_source AS (...)` và `LEFT JOIN LATERAL (...) AS rel ON true`.
  - CTE `authorized_source` kiểm tra quyền sở hữu nguồn (`user_gateways` theo actor `$1`), bind chính xác internal ID (`$2`), public URN (`$3`), type (`$4`), và gateway ID (`$5`).
  - Nếu nguồn không hợp lệ hoặc không có quyền truy cập, câu lệnh trả về 0 dòng và reader trả `ErrNotFound`.
  - `LEFT JOIN LATERAL` đọc phân trang quan hệ `hasSensor` (fetch `limit + 1`), cô lập target strictly về cùng Gateway (`tgt.gateway_id = src.source_gateway_id`), xác thực URN Sensor canonical trong `public.sensors`.
  - Nếu Gateway hợp lệ nhưng không có sensor, truy vấn trả về 1 dòng với các cột quan hệ là NULL, reader phân biệt chính xác và trả về detail với trang quan hệ rỗng (`items = []`, `Count = 0`, `Partial = false`, `NextCursor = nil`), không bị nhầm lẫn với lỗi thiếu tài nguyên.
- **Sensor Detail & Provisioning Graph Verification:**
  - Sensor detail không chỉ đọc bảng `twin_entities` mà bắt buộc phải xác minh toàn bộ đồ thị quan hệ:
    1. Sensor tồn tại trong `public.sensors` với `sensor_id` và `gateway_id` khớp.
    2. Gateway cha tồn tại trong `public.twin_entities` với `entity_type = 'Gateway'`.
    3. Tồn tại quan hệ `hasSensor` nối từ Gateway cha tới Sensor con trong `public.twin_relationships`.
    4. Người dùng có quyền truy cập Gateway cha trong `public.user_gateways`.
  - Nếu thiếu bất kỳ mắt xích nào trong đồ thị, truy vấn trả về `ErrNotFound`.
  - Đảm bảo cô lập tuyệt đối: Sensor cùng tên `sensor_01` ở hai Gateway khác nhau thuộc hai đồ thị độc lập, không bị rò rỉ dữ liệu chéo.
  - Sensor detail không có quan hệ đi ra (`outgoing relationships`), không trả `hasSensor` hay `relationship_page`.

### 2.3. Đọc Trực Tiếp Trạng Thái Hiện Tại (`GetState`)
- **Nguồn dữ liệu & Missing State Handling:** Truy vấn xuất phát từ thực thể đã được xác thực quyền truy cập (`twin_entities`, `gateways`, `user_gateways`) và `LEFT JOIN public.twin_states`.
  - Nếu người dùng không có quyền hoặc entity không tồn tại: trả về `ErrNotFound` (phản hồi non-member hoàn toàn không thể phân biệt với resource không tồn tại).
  - Nếu entity hợp lệ nhưng thiếu dòng tương ứng trong `twin_states` (lỗi toàn vẹn dữ liệu máy chủ): trả về `ErrUnexpected`, tuyệt đối không tự tạo đối tượng rỗng `{}` để che giấu lỗi.
- **Bảo toàn dữ liệu JSONB:** Quét trực tiếp cột `reported_state` và `desired_state` vào `[]byte` rồi bọc bằng `json.RawMessage`. Tuyệt đối không giải mã qua `map[string]any` hoặc `float64` ở Go backend, ngăn chặn hoàn toàn việc làm méo các số nguyên lớn (`> 2^53`).
- **Phiên bản dạng chuỗi:** Cột `reported_version` và `desired_version` (BIGINT trong PostgreSQL, int64 trong Go domain) được định dạng thành chuỗi số nguyên thập phân trong response DTO (`strconv.FormatInt`), tương thích an toàn với JSON parser của Flutter web.
- **Độc lập với Time-Series Hypertable:** Không thực hiện bất kỳ lệnh quét hay kết nối nào tới hypertable `public.telemetry` hay `public.twin_temporal_values`.
- **Hoàn tất stream kết quả (Stream Completion):** Phương thức `scanStateRow` gọi `rows.Close()` để hoàn tất việc tiêu thụ stream trước khi kiểm tra `rows.Err()` cuối cùng, đảm bảo các lỗi kết thúc stream (terminal errors) không bị bỏ sót và luôn trả về `nil` result kèm nguyên nhân `errors.Is` được phân loại đúng.

### 2.4. Điều Phối Đơn Ngân Sách Thời Gian (`ReadPreparation` & `readService`)
- Lớp điều phối `readService` triển khai interface `digitaltwin.ReadService`.
- Mọi thao tác đọc (`ListTwins`, `GetTwinDetail`, `GetTwinState`) đều được bọc bởi `ReadPreparation` (`PrepareList`, `PrepareDetail`, `PrepareState`).
- `ReadPreparation` tạo một context con với timeout duy nhất (cấu hình mặc định 5 giây). Toàn bộ quá trình giải quyết quyền và truy vấn cơ sở dữ liệu đều chia sẻ ngân sách thời gian này.
- Khi context bị hủy hoặc hết hạn, `readService` trả về lỗi phân loại chính xác (`ErrDependencyUnavailable`), bảo đảm không trả về kết quả dở dang.
- **Service-Level Guard:** Nếu reader trả về `(nil, nil)`, các phương thức dịch vụ `readService` lập tức trả về `ErrUnexpected` thay vì trả về thành công giả tạo.

### 2.5. Hệ Thống Mapper & Wire Response DTOs
- **Inline Context Version 1.1 (`mapper.ProjectInlineContext`):**
  Cung cấp ngữ cảnh JSON-LD bất biến với các term chuẩn hóa: `Property`, `Relationship`, `measurement`, `sampleIntervalUs`, `hasSensor`, `gateway_id`.
- **Representation DTOs (`mapper/representation.go`):**
  - `PropertyDTO`: Đại diện cho thuộc tính NGSI-LD kèm `modifiedAt` (thời điểm ghi metadata máy chủ) và tùy chọn `observedAt` (thời điểm đo từ thiết bị).
  - `RelationshipDTO`: Đại diện cho quan hệ NGSI-LD kèm `object` (public URN) và `createdAt`.
  - `RelationshipPageDTO`: Metadata phân trang quan hệ.
  - `FormatRFC3339Micro`: Định dạng timestamp UTC chuẩn xác với 6 chữ số microsecond (`2006-01-02T15:04:05.000000Z`).
- **Wire Response DTOs (`httpserver/twin_dto.go`):**
  - `TwinListResponseDTO`: Danh sách tóm tắt thực thể (`TwinSummaryDTO`).
  - `TwinDetailResponseDTO`: Chi tiết thực thể JSON-LD. Có custom `MarshalJSON` đảm bảo với Gateway thì `hasSensor` luôn là mảng (kể cả rỗng `[]`), còn với Sensor thì `hasSensor` hoàn toàn bị ẩn (`omitempty`).
  - `TwinStateResponseDTO`: Trạng thái thực thể với `reported_state` và `desired_state` dạng `json.RawMessage`, `reported_version` và `desired_version` dạng chuỗi số nguyên.

---

## 3. Kết Quả Kiểm Thử Thực Tế

Toàn bộ các bộ kiểm thử đã được chạy và xác nhận đạt yêu cầu tại môi trường cục bộ:

### 3.1. Phân Tích Tĩnh & Biên Dịch
- `gofmt`: Kiểm tra định dạng toàn bộ mã nguồn — **PASS** (không có file sai định dạng).
- `go vet ./...`: Kiểm tra ngữ nghĩa mã nguồn Go — **PASS** (0 cảnh báo/lỗi).
- `golangci-lint run ./...`: Linter mã nguồn — **PASS** (0 issues).
- `go build ./...`: Biên dịch toàn bộ gói trong thư mục `src/` — **PASS**.
- `git diff --check`: Kiểm tra lỗi khoảng trắng Git — **PASS** (không có trailing whitespace).

### 3.2. Race Tests Kiểm Thử Đơn Vị (Unit Tests)
Chạy lệnh:
```bash
go test -race -count=1 ./internal/digitaltwin/... ./internal/httpserver/... ./internal/telemetry/... ./internal/config/... ./internal/gateway/...
```
Kết quả:
- `iot-platform/internal/digitaltwin`: **PASS** (1.027s)
- `iot-platform/internal/digitaltwin/mapper`: **PASS** (1.009s)
- `iot-platform/internal/httpserver`: **PASS** (1.176s)
- `iot-platform/internal/telemetry`: **PASS** (1.052s)
- `iot-platform/internal/config`: **PASS** (1.030s)
- `iot-platform/internal/gateway`: **PASS** (1.080s)

Bao gồm toàn bộ các ca kiểm thử:
- Mã hóa và giải mã cursor với bộ lọc.
- Xử lý các boundary limit (đúng limit, thừa 1 item, trang rỗng, trang cuối).
- Kiểm tra custom JSON marshalling cho Gateway và Sensor.
- Kiểm tra bảo toàn số nguyên lớn `> 2^53` trong `RawMessage` JSONB.
- Phân biệt rõ ràng giữa write timestamp (`modifiedAt`) và measurement timestamp (`observedAt`).

### 3.3. Python Test Suite
Chạy lệnh:
```bash
python3 -m unittest discover -s scripts/tests
```
Kết quả:
- **792/792 tests PASS** trong 30.287 giây.

### 3.4. Native Database Integration Tests
Chạy lệnh kiểm thử tích hợp cơ sở dữ liệu thật:
```bash
TWIN_READ_TEST_NATIVE=1 go test -race -v -count=1 -run TestNativeTwinRead ./internal/digitaltwin
```
Thông số môi trường kiểm thử thực tế được ghi nhận bởi test runner:
- **PostgreSQL Version:** `16.15`
- **TimescaleDB Version:** `2.29.2`
- **Database Schema Version:** `20`
- **Setup Connection Role:** `postgres` (superuser dùng để tạo và dọn dẹp fixture)
- **Application Connection Role:** `iot_backend_app` (role chạy dịch vụ thực tế, không có quyền superuser)

Kết quả chi tiết các kịch bản:
- `TestNativeTwinRead/List_TenantIsolation_And_Pagination`: **PASS** (xác minh phân trang keyset, cô lập người dùng User A vs User B, platform admin không có membership bị từ chối; thực thể Device không được hỗ trợ bị loại trừ hoàn toàn khỏi danh sách không lọc và các trang cursor).
- `TestNativeTwinRead/Detail_GatewayAndSensor_And_RelationshipPaging`: **PASS** (xác minh chi tiết Gateway thông qua truy vấn đơn CTE/LATERAL kèm phân trang `hasSensor`, Gateway không có sensor trả trang rỗng với metadata hợp lệ, quan hệ cross-gateway sai lệch bị loại trừ, chi tiết Sensor với xác thực đồ thị provisioning, cô lập hai Gateway có cùng Sensor ID).
- `TestNativeTwinRead/State_DirectRead_And_Precision`: **PASS** (xác minh đọc trực tiếp `twin_states`, thực thể hợp lệ thiếu state row trả `ErrUnexpected` thay vì rỗng hay 404, bảo toàn số nguyên `9007199254740993` không bị làm tròn, versions dạng chuỗi thập phân).
- `TestNativeTwinRead/WireDTO_Projection_And_Serialization`: **PASS** (xác minh chuyển đổi DTO sang wire JSON và custom JSON-LD marshalling).
- `TestNativeTwinRead/ReadOnly_Integrity`: **PASS** (chụp snapshot toàn diện và so sánh sâu nội dung fixture: attributes, names, updated_at của entities; type, target, created_at của relationships; reported_state/desired_state dạng text bytes, reported_version/desired_version dạng int64, các mốc timestamps của states; cũng như metadata của gateways/sensors trước và sau các thao tác đọc, chứng minh tính bất biến tuyệt đối mà không chỉ dừng lại ở đếm dòng).
- **Cleanup:** Tự động dọn dẹp toàn bộ dữ liệu fixture test cô lập mà không làm ảnh hưởng đến cơ sở dữ liệu dùng chung; kiểm tra kỹ lưỡng các lỗi dọn dẹp.

---

## 4. Các Giới Hạn Kỹ Thuật (Known Limitations)

1. **Mã nguồn chưa commit:** Toàn bộ mã nguồn triển khai của Task 4.3 hiện đang ở trạng thái uncommitted trên nhánh `develop` (baseline commit `ca53aea76ed121fae6d1f81a5c93037cc2958f9f`).
2. **HTTP Endpoints chưa kích hoạt:** Endpoint `GET /v1/digital-twins` là stub `501 Not Implemented` hiện hữu trong router; các endpoint `GET /v1/digital-twins/{entity_id}` và `GET /v1/digital-twins/{entity_id}/state` hiện chưa được đăng ký trong router. Việc kích hoạt và gắn handler chính thức sẽ được hoàn thành ở Task 4.6. Không tuyên bố kiểm chứng trạng thái live HTTP trong task này.
3. **Chưa ủy quyền Temporal History:** Phương thức `GetTwinHistory` của `readService` hiện chưa được kết nối với engine truy vấn telemetry raw/temporal (sẽ hoàn thiện tại Task 4.4).
4. **Không có thao tác sửa đổi metadata:** Task 4.3 thuần túy là thao tác đọc dữ liệu. Việc tạo mới (`POST /v1/digital-twins`) và cập nhật metadata (`PATCH /v1/digital-twins/{entity_id}`) sẽ được triển khai tại Task 4.5.
5. **Chưa kiểm thử Live HTTP / MQTT End-to-End:** Kiểm thử tích hợp toàn diện thông qua HTTP proxy và luồng MQTT-to-HTTP được hoãn lại đến Task 4.7.
6. **Chưa đo đạc trên Hypertable nén:** Các truy vấn kiểm tra không tác động lên hypertable; việc đánh giá hiệu năng nén dữ liệu của TimescaleDB thuộc về các tác vụ đo đạc ở Task 4.7.

---

## 5. Giao Diện & Điểm Nối Bàn Giao (Handoff APIs)

### Bàn giao cho Task 4.4 (Temporal History & Property Delegation)
- Gói `internal/digitaltwin` đã chuẩn bị sẵn interface `ReadService` với phương thức `GetTwinHistory(ctx context.Context, actor uuid.UUID, q TwinHistoryQuery) (*TwinHistoryResult, error)`.
- Khi Task 4.4 triển khai, chỉ cần ủy quyền thuộc tính `value` sang `HistoryService` từ Task 4.2 và truy vấn bảng `twin_temporal_values` cho các thuộc tính temporal khác.
- Tái sử dụng `PrepareDetail` và `postgres_resolver.go` để xác thực quyền truy cập entity trước khi truy vấn lịch sử temporal.

### Bàn giao cho Task 4.5 (POST / PATCH Entity Metadata)
- Tái sử dụng quy tắc xác thực URN từ `internal/digitaltwin/identity.go` và quan hệ provisioning `hasSensor` được xác lập trong Task 4.3.
- Khi cập nhật `name` hoặc `description`, lưu ý tính nhất quán giữa bảng thực thể `twin_entities` và các bảng tài nguyên `gateways` / `sensors`.
- Bảo toàn logic đồ thị đã được kiểm thử: Sensor phải luôn gắn liền với một Gateway cha hợp lệ và quan hệ `hasSensor`.

### Bàn giao cho Task 4.6 (HTTP Handlers & Composition)
- Các DTO response đã sẵn sàng trong `internal/httpserver/twin_dto.go`:
  - `NewTwinListResponseDTO` -> trả Content-Type `application/json`.
  - `NewTwinDetailResponseDTO` -> trả Content-Type `application/ld+json` (chuẩn JSON-LD với inline context 1.1).
  - `NewTwinStateResponseDTO` -> trả Content-Type `application/json`.
- Sử dụng trực tiếp `digitaltwin.NewReadService(preparation, reader)` để khởi tạo và inject vào router của Gin.
