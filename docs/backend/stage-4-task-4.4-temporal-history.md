# Báo Cáo Kỹ Thuật Task 4.4 — Temporal History Và Ánh Xạ Value Về Raw

**Ngày lập:** 2026-10-17  
**Nhánh:** `develop`  
**Baseline commit:** `a56e8799d42cca90d1ec68bc0a57436a95d8f33c` ("test(digitaltwin): bổ sung native integration tests và tài liệu bàn giao Task 4.3")  
**Trạng thái worktree:** Chuẩn bị commit theo kế hoạch chia nhỏ trên nhánh `develop`. Toàn bộ 6 findings từ `go-reviewer` đã được sửa tuần tự và được `go-reviewer` duyệt APPROVE (không có blocker mới). Chưa chạy trên remote CI.

---

## 1. Mục Tiêu & Ranh Giới Kỹ Thuật

Task 4.4 hoàn thành trọn vẹn tầng nghiệp vụ, truy vấn dữ liệu và biểu diễn DTO cho lịch sử thuộc tính temporal của Digital Twin theo đúng [Stage 4 Contract (Mục 8.8–8.12 và 11.8.4)](../protocols/stage4-read-and-entity-api.md) và [Kế hoạch chi tiết Task 4.4](../../docs/backend_plan/task_4.4_detail_plan.md):
- **Phân luồng selector rõ ràng:**
  - `property=value`: Tái sử dụng trực tiếp engine `telemetry.TelemetryHistoryService` đã được kiểm chứng ở Task 4.2 thông qua interface ủy quyền `TelemetryHistoryDelegator` (`ExecuteEffectiveHistoryQuery`), không tạo thêm bảng hay nguồn lưu trữ high-frequency samples thứ hai.
  - `property=sample_interval_us`: Đọc trực tiếp từ hypertable `public.twin_temporal_values` qua câu lệnh SQL tham số hóa nguyên khối (single MVCC snapshot) với kiểm tra tenant membership và quan hệ provisioning graph (`Gateway hasSensor Sensor`).
- **Giới hạn selector public:** Chỉ hỗ trợ duy nhất hai selector public `value` và `sample_interval_us` cho thực thể kiểu `Sensor`. Các selector cho `Gateway` hoặc các thuộc tính text/boolean (như inference properties) bị từ chối với lỗi domain `ErrInvalidProperty`.
- **Đơn vị đo (Unit Semantics) — ĐIỀU CHỈNH QUAN TRỌNG:**
  - Thuộc tính temporal `sample_interval_us` lấy đơn vị đo **từ `metadata.unit` được lưu trong từng dòng temporal, TUYỆT ĐỐI KHÔNG lấy từ `sensors.unit`**.
  - Chỉ riêng thuộc tính được ủy quyền `value` mới kế thừa ngữ nghĩa đơn vị đo vật lý của Sensor (`sensors.unit`) thông qua telemetry engine.
  - Đối với temporal, metadata thiếu key `unit` hoặc có giá trị JSON `null` được coi là unknown (trả về unit `nil` nếu toàn bộ cửa sổ đều unknown, không tự bịa đơn vị `"us"`).
  - Cửa sổ số học (numeric series) có nhiều unit khác nhau hoặc pha trộn giữa known và unknown unit sẽ bị từ chối với `ErrInconsistentUnit`.
- **Biểu diễn DTO & Không làm biến đổi trạng thái:** Kết quả wire DTO `TwinHistoryResponseDTO` tương thích hoàn toàn với định dạng chuỗi số nguyên thập phân và mốc thời gian UTC microsecond RFC3339. Truy vấn history là thao tác thuần đọc (read-only), không làm thay đổi trạng thái hiện tại (`twin_states`), không tác động MQTT outbox hay metadata entities.

---

## 2. Kiến Trúc & Các Thành Phần Đã Triển Khai

### 2.1. Điều Phối Nghiệp Vụ & Đơn Ngân Sách Thời Gian (`readService.GetTwinHistory`)
- `readService.GetTwinHistory` sử dụng `ReadPreparation.PrepareHistory` để thực thi dưới một `context.Context` timeout duy nhất (mặc định 5.0 giây). Toàn bộ quá trình kiểm tra định dạng tham số, phân giải thực thể Sensor, kiểm tra quan hệ Gateway cha, xác minh quyền sở hữu (`user_gateways`) và truy vấn dữ liệu đều chia sẻ chung ngân sách này.
- Khi timeout hoặc context bị hủy, phương thức lập tức fail-closed trả về `ErrDependencyUnavailable` mà không để lộ kết quả một phần (partial result).
- **Phân luồng xử lý:**
  - Với `SelectorValue`: Chuyển đổi thành `telemetry.TelemetryHistoryQuery` với đúng `GatewayID` và `SensorID` đã được phân giải chính xác từ URN, gọi `ExecuteEffectiveHistoryQuery` trên `TelemetryHistoryDelegator`. Bọc kết quả thành `TwinHistoryResult` với `Property = "value"` và map các mã lỗi telemetry sang domain Digital Twin tương ứng (`ErrNotFound`, `ErrDependencyUnavailable`, `ErrUnexpected`, `ErrRawLimitExceeded`, `ErrInvalidResolution`, v.v.), bảo toàn nguyên nhân gốc cho `errors.Is`.
  - Với `SelectorSampleIntervalUS`: Ủy quyền cho `TwinReader.ReadTemporalHistory` thực thi truy vấn trực tiếp trên `twin_temporal_values`.
  - Với các selector khác hoặc entity không phải Sensor: Trả về `ErrInvalidProperty`.
- **Guard an toàn dịch vụ:** Nếu reader hoặc delegator trả về `(nil, nil)`, phương thức dịch vụ lập tức trả về `ErrUnexpected` thay vì báo thành công giả.

### 2.2. Chiến Lược Truy Vấn Temporal Snapshot Đơn (`PostgresTwinReader`)
Để tránh lỗi phân mảnh MVCC khi kiểm tra kiểu dữ liệu/đơn vị và truy vấn danh sách điểm dữ liệu, reader sử dụng chiến lược **Single MVCC Snapshot** kết hợp các Common Table Expressions (CTE) trong một câu lệnh SQL duy nhất:
1. **CTE `authorized_sensor`:**
   Xác minh đồng thời:
   - Entity tồn tại trong `public.twin_entities` với đúng internal UUID, public URN, type `'Sensor'` và `gateway_id`.
   - Sensor tồn tại trong `public.sensors` với `sensor_id` và `gateway_id` khớp.
   - Gateway cha tồn tại trong `public.twin_entities` (`entity_type = 'Gateway'`).
   - Tồn tại quan hệ `hasSensor` trong `public.twin_relationships`.
   - Người dùng (actor) có quyền trên Gateway trong `public.user_gateways`.
   Nếu người dùng không có quyền hoặc đồ thị không hợp lệ, CTE trả về 0 dòng, toàn bộ câu lệnh trả về 0 dòng và Go reader trả về `ErrNotFound` (không thể phân biệt với thực thể không tồn tại).
2. **CTE `window_summary`:**
   Tính toán tổng hợp trên toàn bộ cửa sổ thời gian nửa mở `[from, to)`:
   - Đếm số lượng theo kiểu: `total_count`, `number_count`, `text_count`, `boolean_count`.
   - Đếm vi phạm bất biến: `invalid_type_count` (dòng không có đúng 1 cột giá trị non-null), `invalid_interval_count` (giá trị `<= 0`, `> 2^53`, hoặc là số phân số `value_number <> trunc(value_number)`).
   - Thống kê đơn vị: `unknown_unit_count` (thiếu `metadata.unit` hoặc là JSON `null`), `malformed_unit_count` (unit không phải kiểu string), `distinct_unit_count` (số lượng unit phân biệt dạng string), và `min_unit`.
3. **CTE `bounded_observations` (cho chế độ Raw) hoặc `bucket_aggregates` (cho chế độ Bucket):**
   - **Raw:** Lấy tối đa `max_points + 1` dòng, sắp xếp theo `v.observed_at ASC, v.message_id ASC NULLS LAST`.
   - **Bucket:** Nhóm theo TimescaleDB `time_bucket($interval, v.observed_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')`, tính chính xác `AVG(v.value_number)::double precision`, `MIN(v.value_number)::double precision`, `MAX(v.value_number)::double precision` và `COUNT(*)::bigint`. Giới hạn `LIMIT max_points + 1`. Không sinh bucket rỗng (gap preservation, không gapfill).
4. **Sentinel Row & Kết quả rỗng:**
   Sử dụng `CROSS JOIN window_summary s LEFT JOIN bounded_observations b ON true`. Nếu Sensor hợp lệ nhưng không có quan hệ dữ liệu trong `[from, to)`, câu lệnh trả về 1 dòng sentinel với `b.observed_at IS NULL` và `s.total_count = 0`. Reader phân biệt chính xác và trả về chuỗi rỗng có quyền hợp lệ (`PointCount = 0, Points = []`), không bị nhầm lẫn với lỗi `ErrNotFound`.

### 2.3. Phân Loại Cửa Sổ Temporal & Ngữ Nghĩa Đơn Vị Đo (`classifyTemporalWindow`)
Hàm `classifyTemporalWindow` kiểm tra toàn diện các bất biến trên toàn bộ cửa sổ:
- **Đồng nhất kiểu (Type Uniformity):** Yêu cầu toàn bộ các bản ghi trong cửa sổ phải cùng một kiểu dữ liệu (`number`, `text`, hoặc `boolean`). Pha trộn kiểu trả về `ErrUnexpected`.
- **Bất biến khoảng mẫu (Interval Invariants):** Đối với `sample_interval_us`, giá trị lưu trữ phải là số nguyên dương (`v.value_number = trunc(v.value_number)`) thỏa mãn `0 < value <= 2^53` (`9007199254740992`). Giá trị phân số (fractional observation) hoặc vượt ngưỡng này đều bị từ chối với `ErrUnexpected`. Tuy nhiên, kết quả gom nhóm `AVG` của bucket từ các mẫu số nguyên hợp lệ được phép là số phân số hợp lệ (ví dụ: 10000 và 10001 cho AVG = 10000.5).
- **Đơn vị đo temporal (Unit Classification):**
  - Nếu `UnknownUnitCount == TotalCount`: Trả về `Unit = nil` (unknown, không tự gán mặc định).
  - Nếu `UnknownUnitCount == 0` và `DistinctUnitCount == 1`: Trả về `Unit = min_unit` (đồng nhất).
  - Đối với chuỗi số (`valType == "number"`): Nếu có nhiều hơn 1 unit phân biệt, hoặc pha trộn giữa dòng có unit và dòng không có unit, hàm **từ chối ngay lập tức với `ErrInconsistentUnit`**.
  - Đối với chuỗi phi số (text/boolean): Unit trả về `nil` mà không báo lỗi.

### 2.4. Khắc Phục Lỗi Production Trong Kiểm Thử Typed Bucket Aggregation
Trong quá trình phát triển và kiểm thử native trên các kiểu dữ liệu phi số (text/bool), một lỗi tiềm ẩn đã được phát hiện và khắc phục:
- **Mô tả lỗi:** Khi người dùng yêu cầu chế độ bucket trên một chuỗi temporal phi số, câu lệnh SQL trả về `AVG(v.value_number)` là NULL. Trước đây, vòng lặp quét dòng của reader kiểm tra `if avgValue == nil || minValue == nil ...` và lập tức trả về lỗi `ErrUnexpected: incomplete aggregate values in bucket row`. Lỗi này xảy ra **trước khi** `classifyTemporalWindow` được gọi, khiến API trả về mã lỗi 500 (`ErrUnexpected`) thay vì trả về lỗi phân loại đúng theo contract là `ErrInvalidResolution` (hoặc `ErrRawLimitExceeded` trong auto mode).
- **Giải pháp:** Đoạn mã quét dòng được điều chỉnh:
  ```go
  if avgValue == nil || minValue == nil || maxValue == nil || sampleCount == nil {
      if summary != nil && summary.NumberCount == summary.TotalCount {
          return nil, nil, fmt.Errorf("%w: incomplete aggregate values in bucket row", ErrUnexpected)
      }
      continue
  }
  ```
  Nhờ đó, đối với các chuỗi phi số, vòng lặp cho phép hoàn tất việc đọc `summary`, sau đó `classifyTemporalWindow` phát hiện chuỗi phi số và trả về lỗi phân giải hợp lệ theo đúng đặc tả kỹ thuật.

### 2.5. Biểu Diễn Wire DTO (`TwinHistoryResponseDTO`)
- Tệp `src/internal/httpserver/twin_history_dto.go` định nghĩa cấu trúc wire response:
  ```json
  {
    "entity_id": "urn:ngsi-ld:Sensor:gw_01:sens_01",
    "property": "sample_interval_us",
    "unit": "us",
    "from": "2026-10-17T10:00:00.000000Z",
    "to": "2026-10-17T10:05:00.000000Z",
    "mode": "bucket",
    "resolution": "1m",
    "point_count": 5,
    "points": [...]
  }
  ```
- **Bảo toàn số nguyên lớn:** Dữ liệu metadata JSONB được bảo toàn nguyên vẹn qua `json.RawMessage` và `json.Number`, ngăn chặn việc làm tròn số nguyên lớn (`9007199254740993`).
- **Phân nhánh điểm dữ liệu:**
  - Nhánh `value` (raw): Chuyển đổi thành `[]RawHistoryPointDTO` với sequence số thập phân, provenance nullable.
  - Nhánh `value` hoặc `sample_interval_us` (bucket): Chuyển đổi thành `[]BucketHistoryPointDTO` (`bucket_start`, `avg`, `min`, `max`, và `count` dạng decimal string đại diện cho số mẫu trong bucket).
  - Nhánh `sample_interval_us` (raw): Chuyển đổi thành `[]TemporalRawPointDTO` (`observed_at`, `value_type`, `value`, `metadata`, `message_id`).

---

## 3. Bằng Chứng Kiểm Thử Kế Thừa (Inherited Verification Evidence)

Toàn bộ các kiểm thử dưới đây đã được thực thi và xác nhận PASS trong phiên làm việc của các worker trước đó trên môi trường test chia sẻ (PostgreSQL 16.15 / TimescaleDB 2.29.2, schema 20, role `iot_backend_app` non-superuser). Báo cáo này ghi nhận trung thực các kết quả kế thừa, **không tuyên bố đã tự chạy lại**:

### 3.1. Bộ Kiểm Thử Native Integration Database (`TestNativeTemporalHistory_NumericSlice`)
Lệnh thực thi:
```bash
TWIN_TEMPORAL_TEST_NATIVE=1 go test -race -v -count=1 -run TestNativeTemporalHistory_NumericSlice ./internal/digitaltwin
```
Bao gồm 8 subtest cụ thể:
1. `Authorization_TenantIsolation_And_Roles`:
   - Xác minh cô lập người dùng A và B trên cùng tên Sensor `sens_shared`. User B không thể đọc dữ liệu của Gateway A (`ErrNotFound`).
   - Xác minh các role `owner`, `operator`, `viewer` có quyền gán đều đọc được lịch sử temporal.
   - Xác minh Platform Admin không có membership trong `user_gateways` bị từ chối với `ErrNotFound` (không bypass).
   - Sensor không có quan hệ `hasSensor` hợp lệ với Gateway cha bị từ chối với `ErrNotFound`.
   - Sensor hợp lệ nhưng không có bản ghi nào trong khoảng thời gian trả về `point_count = 0, points = []`.
2. `TimeBoundaries_Ties_Provenance_And_Precision`:
   - Kiểm tra cận thời gian nửa mở `[from, to)`: Bản ghi tại đúng `from` được thu nạp; bản ghi tại đúng `to` bị loại bỏ.
   - Bản ghi có timestamp trùng nhau (tie): Cả hai dòng đều được bảo toàn đầy đủ trong kết quả, sắp xếp ổn định với `message_id ASC NULLS LAST`.
   - Bản ghi có `message_id` là NULL được quét an toàn, trả về con trỏ `nil`.
   - Metadata trống `{}` hoặc thiếu `boot_id`/`unit` được giữ nguyên vẹn, không tự ý gán giá trị mặc định.
3. `Raw_Bucket_Auto_Aggregation_And_Gaps`:
   - Kiểm tra giới hạn raw window: Cửa sổ > 10s ở chế độ raw bị từ chối với `ErrRawLimitExceeded`.
   - Kiểm tra overflow raw: Cửa sổ <= 10s có số điểm vượt quá `max_points` bị từ chối với `ErrRawLimitExceeded`.
   - Chế độ bucket: Tính toán chính xác `avg`, `min`, `max`, `count` trên các bucket có dữ liệu. Không tự ý lấp đầy các bucket không có dữ liệu (gap preservation).
   - Chế độ Auto: Cửa sổ ngắn (<= 10s) thử raw probe, tự động chuyển đổi sang bucket tương thích khi vượt quá số điểm cho phép. Cửa sổ dài (> 10s) trực tiếp lựa chọn độ phân giải bucket phù hợp.
4. `UnitSemantics_And_WholeWindowInvariants`:
   - Cửa sổ có 100% dòng thiếu unit hoặc `unit: null`: Trả về kết quả hợp lệ với `Unit = nil`.
   - Cửa sổ có 100% dòng có cùng unit `"us"`: Trả về `Unit = "us"`.
   - Cửa sổ có pha trộn giữa dòng có unit `"us"` và dòng không có unit (hoặc unit `"ms"`): Trả về `ErrInconsistentUnit`.
   - Cửa sổ có metadata unit không phải kiểu chuỗi (ví dụ số nguyên hoặc mảng): Trả về `ErrUnexpected`.
   - Cửa sổ có giá trị interval âm hoặc `<= 0`: Trả về `ErrUnexpected`.
5. `Private_Text_Boolean_And_MetadataPrecision`:
   - Kiểm tra đọc các fixture nội bộ kiểu chuỗi và boolean: Bảo toàn chính xác chuỗi rỗng `""` và boolean `false` (khác với `nil`).
   - Kiểm tra bảo toàn số nguyên lớn trong metadata JSONB: `9007199254740993` và `9007199254740995` được giữ nguyên vẹn qua `json.Number`/`json.RawMessage`.
   - Kiểm tra từ chối gom nhóm (aggregation) trên dữ liệu text/bool: Chế độ bucket trả về `ErrInvalidResolution`, chế độ auto vượt ngưỡng trả về `ErrRawLimitExceeded`.
   - Xác nhận parser public và service từ chối các thuộc tính này nếu được gọi qua public API (`ErrInvalidProperty`).
6. `Real_Persist_Dedup_And_Value_Delegation_Equivalence`:
   - Bơm một batch thực tế qua `telemetry.Persist` (10 mẫu từ 10 đến 28, interval 100.000 µs): Sinh ra đúng 10 dòng raw trong `telemetry`, 1 dòng trong `processed_messages`, và 1 bản ghi temporal `sample_interval_us` trong `twin_temporal_values` tại mốc thời gian của mẫu cuối cùng.
   - Thử gửi lại cùng một batch (duplicate resend): Không làm tăng thêm bất kỳ dòng dữ liệu raw hay temporal nào.
   - Đối chiếu tương đương giá trị ủy quyền `property=value`: Kết quả đọc qua `GetTwinHistory` cho `value` ở các chế độ raw, bucket và auto cho kết quả giống hệt khi đọc trực tiếp qua engine `telemetry.HistoryService` (bucket count 10, min 10, max 28, avg 19).
7. `ReadOnly_Integrity_Snapshots`:
   - So sánh snapshot trước và sau các thao tác đọc: Số lượng bản ghi trong `telemetry`, `processed_messages`, `twin_temporal_values`, nội dung JSON `reported_state`, `desired_state`, các phiên bản version, và toàn bộ metadata của Gateway/Sensor hoàn toàn không bị biến đổi.
8. `BoundedPool_DependencyDeadline`:
   - Kiểm tra hết hạn deadline khi pool kết nối `MaxConns = 1` bị chiếm dụng: Trả về lỗi phân loại chính xác `ErrDependencyUnavailable`. (Bằng chứng này chứng minh việc timeout khi chờ lấy kết nối từ pool, không phải hủy một câu lệnh đang chạy dở trên PostgreSQL server).
- **Dọn dẹp fixture:** Toàn bộ dữ liệu fixture được dọn dẹp sạch sẽ qua `setupPool`, xác nhận zero fixture rows còn sót lại cho từng run hiện tại (dữ liệu leftover từ run cũ `861eb9b2` được bảo lưu nguyên trạng theo chính sách non-destructive).

### 3.2. Kiểm Tra Mã Nguồn & Regressions Toàn Diện
Worker kiểm tra cuối cùng đã thực thi và xác nhận:
- `gofmt -l`: Không có file nào sai định dạng.
- `go test -race -count=1 ./internal/digitaltwin/... ./internal/telemetry/... ./internal/httpserver/... ./internal/config/... ./internal/gateway/...`: **PASS**.
- `go vet ./...`: **PASS**.
- `golangci-lint run`: **0 issues**.
- `go build ./...`: **PASS**.
- `python3 -m unittest discover -s scripts/tests`: **792/792 tests PASS**.
- `git diff --check`: **PASS** (không có trailing whitespace hay xung đột định dạng).

---

## 4. Giới Hạn & Ranh Giới Kỹ Thuật Còn Lại (Limitations)

1. **Chưa kiểm thử Native Legacy NULL Provenance qua ủy quyền `value`:** Trường hợp đọc bản ghi telemetry có provenance NULL kế thừa từ các schema cũ chưa được chạy trong kiểm thử native trên schema 20 dùng chung này (đã được kiểm chứng ở mức unit test và trong upgrade rehearsal container của Task 4.2). Ràng buộc schema không bị làm yếu.
2. **Thứ tự các bản ghi trùng timestamp (Tie Order):** Khi nhiều bản ghi temporal có cùng giá trị `observed_at`, toàn bộ các dòng đều được bảo toàn nhưng không có cam kết thứ tự tuyệt đối giữa chúng. Sắp xếp phụ theo `message_id ASC NULLS LAST` là nỗ lực tối đa (best-effort), không tạo ra tính duy nhất (uniqueness) vì bảng không có khóa chính.
3. **Chưa có luồng ghi AI Inference:** Các fixture text/bool trong kiểm thử chỉ nhằm chứng minh tính đúng đắn của hàm đọc kiểu nội bộ (`readTemporalHistoryInternal`), tuyệt đối không phải bằng chứng hệ thống đã hỗ trợ thu nạp (ingestion) dữ liệu AI inference hay mở rộng selector public.
4. **Chưa gắn HTTP Router và chưa có API công khai:** Các endpoint `GET /v1/digital-twins/{entity_id}/history` hiện chưa được đăng ký trong HTTP router của server (sẽ được cấu hình và đăng ký tại Task 4.6).
5. **Chưa có kiểm thử hiệu năng nén Hypertable:** Toàn bộ dữ liệu kiểm thử native thực thi trên các chunk chưa nén; việc kiểm chứng nén TimescaleDB thuộc Task 4.7.
6. **Không làm thay đổi backlog trùng lặp:** Mọi giới hạn kỹ thuật được theo dõi tập trung tại `docs/dynsec/limitations-and-backlog.md`, không tạo thêm các tài liệu backlog riêng lẻ.

---

## 5. Hướng Dẫn Bàn Giao Cho Các Task Tiếp Theo

- **Bàn giao cho Task 4.5 (Entity Metadata POST/PATCH):**
  - Giữ vững tính bất biến giữa trạng thái hiện tại (`twin_states`) và lịch sử (`twin_temporal_values`). Các thao tác PATCH metadata chỉ được phép cập nhật metadata thực thể (`twin_entities`, `sensors`, `gateways`), không được làm thay đổi hoặc xóa các bản ghi lịch sử temporal đã lưu.
- **Bàn giao cho Task 4.6 (HTTP Handlers & Server Composition):**
  - Khởi tạo `readService` thông qua constructor `NewReadService(preparation, reader, telemetryHistory)` với đầy đủ 3 dependencies không nil.
  - Sử dụng DTO `httpserver.NewTwinHistoryResponseDTO(res)` để ánh xạ kết quả `digitaltwin.TwinHistoryResult` sang wire response.
  - Đăng ký route `GET /v1/digital-twins/{entity_id}/history`, map các lỗi domain (`ErrNotFound` -> 404, `ErrInvalidProperty` / `ErrInvalidInput` / `ErrInvalidResolution` / `ErrRawLimitExceeded` / `ErrResolutionTooFine` / `ErrInconsistentUnit` -> 400, `ErrDependencyUnavailable` -> 503, `ErrUnexpected` -> 500).
- **Bàn giao cho Task 4.7 (Tích Hợp Toàn Diện & Acceptance Tests):**
  - Điểm vào kiểm thử native temporal history kế thừa từ `TestNativeTemporalHistory_NumericSlice` trong `internal/digitaltwin`.
  - Bổ sung kịch bản kiểm thử HTTP end-to-end từ client qua Go API đến TimescaleDB và xác nhận đo đạc latency p50/p95 cho các truy vấn history.
