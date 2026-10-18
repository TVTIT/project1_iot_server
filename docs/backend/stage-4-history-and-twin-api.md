# Báo cáo Kỹ thuật Tổng kết Stage 4 — API Lịch sử Đo đạc và Digital Twin Entity

**Tài liệu:** Báo cáo kỹ thuật tổng kết và bàn giao Giai đoạn 4 (Stage 4 Handover)  
**Mục tiêu hệ thống:** Đồ án 1 (ET3290) — Nền tảng IoT Gateway–Server  
**Ngày lập:** 18/10/2026  
**Trạng thái:** Stage 4 bounded complete; Task 4.8 tài liệu đã đối soát ngày 18/10/2026, chưa commit. Remote CI xác minh cho code tại `aa7436d`, không phải một lần chạy mới cho diff tài liệu này. Không chứng nhận client Flutter/production đã hoàn tất.  
**Git Baseline & Remote CI:**  
- Commit hoàn tất Task 4.7 trên nhánh `develop`: `aa7436d8838a6580c21b30930e2eed992999b11c` (qua chuỗi commit `a526981`, `f3edc3d`, `6257f80`, cùng 2 commit sửa CI `65d727b` và `aa7436d`).
- GitHub Actions: [Run 39103564379](https://github.com/TVTIT/project1_iot_server/actions/runs/39103564379), trạng thái `completed / success`, **16/16 jobs thành công** (bao gồm `stage4-api-integration`, `stage3-uplink-integration` và `native-compose-smoke`).

---

## 1. Mục tiêu, phạm vi và trạng thái Stage 4

### 1.1 Mục tiêu kỹ thuật
Giai đoạn 4 xây dựng tầng giao diện lập trình ứng dụng (HTTP REST API) hoàn chỉnh cho hai năng lực cốt lõi:
1. **Truy vấn chuỗi thời gian lịch sử (Telemetry & Temporal History):** Cung cấp dữ liệu đo lường cảm biến theo thời gian thực ở hai chế độ: dữ liệu thô (`raw`) cho các khoảng thời gian ngắn và dữ liệu tổng hợp gom nhóm (`time_bucket`) cho các khoảng thời gian dài, bảo đảm không làm quá tải băng thông hay bộ nhớ máy khách.
2. **Quản lý thực thể và trạng thái Digital Twin:** Cung cấp mô hình biểu diễn số (Digital Twin) theo chuẩn NGSI-LD cho Gateway và Sensor; hỗ trợ tra cứu danh sách phân trang keyset, xem đồ thị thực thể chi tiết, đọc trực tiếp trạng thái đo đạc mới nhất (`twin_states`), cùng khả năng tạo mới và cập nhật siêu dữ liệu (metadata) an toàn.

### 1.2 Phạm vi hoàn thành (Tasks 4.0 – 4.7)
- **Task 4.0:** Thiết kế hợp đồng API 7 endpoints, định dạng URN chuẩn `urn:ngsi-ld:<Type>:<id>`, phân quyền fail-closed và định dạng cursor base64url ([`docs/protocols/stage4-read-and-entity-api.md`](../protocols/stage4-read-and-entity-api.md)).
- **Task 4.1:** Mô hình truy vấn, parser tham số, cấu hình giới hạn (bounds), giải quyết quan hệ thành viên `user_gateways` và cơ chế quản lý ngân sách thời gian duy nhất (`HistoryPreparation`, `ReadPreparation`).
- **Task 4.2:** Tầng dịch vụ và truy vấn CSDL cho Telemetry History (raw window ngắn, time_bucket epoch UTC 1970, cơ chế adaptive auto fallback, và DTO wire format).
- **Task 4.3:** Tầng dịch vụ Digital Twin đọc (keyset list `v1.{k,g,t}`, detail JSON-LD inline `@context`, relationship pagination `v1.{s,r,k}`, và đọc trạng thái `twin_states` trực tiếp bảo toàn số nguyên lớn `> 2^53`).
- **Task 4.4:** Lịch sử thuộc tính thời gian (Temporal History) cho Sensor: ủy quyền selector `value` sang telemetry engine, đọc chuỗi `sample_interval_us` từ `twin_temporal_values` với ngữ nghĩa đơn vị đo lấy từ metadata chuỗi thời gian.
- **Task 4.5:** Quản lý siêu dữ liệu thực thể (POST tạo mới/replay chính xác theo quyền admin/owner; PATCH đổi tên và mô tả có allowlist nghiêm ngặt, cơ chế chống deadlock và bảo toàn bất biến trạng thái/lịch sử).
- **Task 4.6:** Đăng ký 7 HTTP routes trong Gin router, bộ điều phối composition (`stage4.go`), kiểm soát Content-Type, CORS, body limit và mã lỗi chuẩn hóa.
- **Task 4.7:** Tích hợp đa dịch vụ thực (`test-stage4-api.sh`, `stage4_api_runner.py`), kiểm chứng end-to-end vertical slice, đo lường hiệu năng truy vấn trên dataset 60.000 mẫu (chưa nén vs nén hypertable), và kết nối CI pipeline.

### 1.3 Ranh giới với các giai đoạn tiếp theo (Stage 5 – 9)
Theo [Kế hoạch tổng thể Backend](../../../docs/backend_plan/Backend_summary_plan.md) và [AGENTS.md](../../AGENTS.md):
- **Stage 5:** Lát cắt điều khiển Digital Twin Downlink (ghi nhận `desired_state`, sinh lệnh điều khiển `twin_commands`, transactional outbox và gửi lệnh qua MQTT). **Stage 4 hoàn toàn chưa triển khai ghi desired_state hay gửi lệnh điều khiển.**
- **Stage 6:** Worker đối soát trạng thái (Reconciliation worker: timeout, retry có jitter, xử lý Gateway ngoại tuyến).
- **Stage 7:** WebSocket và truyền dữ liệu thời gian thực cho máy khách (`/v1/ws`).
- **Stage 8:** Quản lý tải lên hình ảnh qua Supabase Storage.
- **Stage 9:** Nghiệm thu hệ thống, đo tải và triển khai (Acceptance, load measurement, and deployment theo `Backend_summary_plan.md`).

---

## 2. Luồng dữ liệu và thành phần tái sử dụng

Hệ thống kết hợp chặt chẽ luồng ghi nhận dữ liệu cảm biến ngầm định (Uplink Ingestion) với luồng đọc/ghi siêu dữ liệu qua HTTP của người dùng:

```text
[Cảm biến / Gateway]
        │
   (MQTT TLS 8883)
        ▼
   [Mosquitto]
        │ (QoS 1)
        ▼
 [Go MQTT Ingress] ──► Bounded Queue ──► Worker Pool
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
     PostgreSQL Transaction (Schema 20)                    Application-Level ACK
     ├── telemetry (raw hypertable)                        (gateways/<id>/acks/<msg_id>)
     ├── processed_messages (dedup)                                   │
     ├── twin_temporal_values (sample_interval_us)                    ▼
     └── twin_states (reported_state & version)           [Gateway Outbox xóa bản tin]
                      ▲
                      │  (Đọc độc lập sau commit)
  ┌───────────────────┴───────────────────────────────────────────────┐
  │                           Go HTTP REST API (Port 8080)             │
  │  ├── GET /v1/telemetry/history  ──► Đọc raw hoặc time_bucket      │
  │  ├── GET /v1/digital-twins/...  ──► Đọc đồ thị thực thể & state    │
  │  ├── POST /v1/digital-twins     ──► Tạo thực thể Gateway / Sensor  │
  │  └── PATCH /v1/digital-twins/.. ──► Cập nhật metadata (tên/mô tả) │
  └───────────────────▲───────────────────────────────────────────────┘
                      │ (HTTPS + Supabase JWT)
              [Flutter Web / Người dùng]
```

### Nguyên lý kiến trúc cốt lõi:
1. **Commit-before-ACK và Read Consistency:** Bản tin cảm biến chỉ được gửi Application ACK về Gateway sau khi đã hoàn tất transaction ghi vào CSDL (gồm `telemetry`, `processed_messages`, `twin_temporal_values` và `twin_states`). Nhờ vậy, ngay khi người dùng gọi API đọc HTTP, dữ liệu đã sẵn sàng mà không gặp hiện tượng đọc bẩn (dirty read).
2. **Đọc trạng thái hiện tại độc lập với Hypertable:** Endpoint `GET /v1/digital-twins/{entity_id}/state` chỉ truy vấn trực tiếp bảng trạng thái `twin_states` kết hợp `twin_entities` (đọc qua chỉ mục khóa chính/khóa ngoại). Tuyệt đối không quét ngược toàn bộ hypertable lịch sử đo đạc để tái dựng trạng thái.
3. **Phân tách rạch ròi giữa Metadata Mutation và Device Control:**
   - **PATCH Metadata (Stage 4):** Chỉ thay đổi các thuộc tính định danh hiển thị của thực thể do con người quản lý (`name`, `description`).
   - **Device Control (Stage 5):** Thay đổi cấu hình vận hành phần cứng (như chu kỳ lấy mẫu, bật/tắt thiết bị) thuộc về `desired_state` và quy trình gửi lệnh MQTT outbox. PATCH metadata không được phép can thiệp vào `desired_state`, `reported_state`, hay sinh lệnh điều khiển.

---

## 3. Danh mục bảy endpoints và contract dữ liệu

Toàn bộ 7 endpoints đều nằm dưới tiền tố `/v1`, yêu cầu xác thực JWT qua header `Authorization: Bearer <token>`, áp dụng header phản hồi `Cache-Control: no-store` (kèm `Pragma: no-cache` cho các thao tác sửa đổi). Chi tiết đặc tả đầy đủ xem tại [Stage 4 Protocol Contract](../protocols/stage4-read-and-entity-api.md).

| Phương thức | Đường dẫn API | Mục đích chính | Vai trò cho phép | DTO / Định dạng trả về | Mã trạng thái HTTP |
|---|---|---|---|---|---|
| **GET** | `/v1/telemetry/history` | Truy vấn lịch sử đo đạc cảm biến (raw hoặc bucket) | Owner, Operator, Viewer (được gán) | `TelemetryHistoryResponseDTO` (`application/json`) | `200 OK`, `400`, `401`, `404`, `503` |
| **GET** | `/v1/digital-twins` | Liệt kê danh sách thực thể Twin theo phân trang keyset | Owner, Operator, Viewer (theo membership) | `TwinListResponseDTO` (`application/json`) | `200 OK`, `400`, `401`, `503` |
| **GET** | `/v1/digital-twins/:entity_id` | Đọc thông tin chi tiết và đồ thị quan hệ NGSI-LD | Owner, Operator, Viewer (được gán) | `TwinDetailResponseDTO` (`application/ld+json`) | `200 OK`, `400`, `401`, `404`, `503` |
| **GET** | `/v1/digital-twins/:entity_id/state` | Đọc trạng thái đo đạc và phiên bản hiện hành | Owner, Operator, Viewer (được gán) | `TwinStateResponseDTO` (`application/json`) | `200 OK`, `400`, `401`, `404`, `503` |
| **GET** | `/v1/digital-twins/:entity_id/history` | Đọc lịch sử thuộc tính Twin (`value` hoặc `sample_interval_us`) | Owner, Operator, Viewer (Sensor được gán) | `TwinHistoryResponseDTO` (`application/json`) | `200 OK`, `400`, `401`, `404`, `503` |
| **POST** | `/v1/digital-twins` | Tạo mới hoặc phát lại thực thể Gateway / Sensor | Admin (Gateway/Sensor), Owner (Sensor) | `PostGatewayResponseDTO` / `PostSensorResponseDTO` | `201 Created`, `200 OK`, `400`, `401`, `403`, `409`, `503` |
| **PATCH** | `/v1/digital-twins/:entity_id` | Cập nhật tên/mô tả thực thể (không đổi state/cấu hình) | Admin (Gateway/Sensor), Owner (Sensor) | `PatchGatewayResponseDTO` / `PatchSensorResponseDTO` | `200 OK`, `400`, `401`, `403`, `404`, `503` |

### Ví dụ minh họa yêu cầu (Sử dụng dữ liệu giả định, tuyệt đối không có bí mật thực):

**1. Đọc lịch sử đo đạc (Raw hoặc Bucket Fallback):**
```http
GET /v1/telemetry/history?gateway_id=gw_demo_01&sensor_id=sensor_temp_01&from=2026-10-18T08:00:00.000000Z&to=2026-10-18T08:00:10.000000Z&resolution=auto HTTP/1.1
Host: api.iot.local:8080
Authorization: Bearer <valid_user_jwt>
```

**2. Đọc trạng thái Digital Twin hiện tại:**
```http
GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw_demo_01:sensor_temp_01/state HTTP/1.1
Host: api.iot.local:8080
Authorization: Bearer <valid_user_jwt>
```
*Phản hồi (JSON):*
```json
{
  "id": "urn:ngsi-ld:Sensor:gw_demo_01:sensor_temp_01",
  "type": "Sensor",
  "gateway_id": "gw_demo_01",
  "reported_state": {
    "sample_interval_us": 10000.0
  },
  "desired_state": {},
  "reported_version": "2",
  "desired_version": "0",
  "last_reported_at": "2026-10-18T08:00:02.000000Z",
  "last_desired_at": null,
  "updated_at": "2026-10-18T08:00:02.105432Z"
}
```

**3. Tạo mới Sensor (Dành cho Gateway Owner hoặc Admin):**
```http
POST /v1/digital-twins HTTP/1.1
Host: api.iot.local:8080
Authorization: Bearer <valid_owner_jwt>
Content-Type: application/json; charset=utf-8

{
  "entity_type": "Sensor",
  "gateway_id": "gw_demo_01",
  "sensor_id": "sensor_temp_01",
  "name": "Nhiệt độ phòng máy",
  "unit": "Cel"
}
```

---

## 4. Quyền truy cập và metadata mutation

### 4.1 Mô hình phân quyền RBAC và phân định đối tượng
Phân quyền được thẩm định nghiêm ngặt tại tầng CSDL thông qua bảng quan hệ thành viên `user_gateways` và bảng quản trị `platform_admins`:
- **Platform Admin:** Người vận hành hệ thống. Có quyền tạo mới Gateway (`POST`), sửa đổi thông tin Gateway (`PATCH`), và thực hiện các quyền quản trị như gán Gateway cho Owner. Tuy nhiên, **Admin không tự động có quyền đọc dữ liệu đo đạc (Telemetry/History/State) nếu không được gán quyền thành viên rõ ràng trong `user_gateways`** (nhận mã `404 Not Found` tương tự người dùng lạ).
- **Gateway Owner:** Chủ sở hữu Gateway. Có toàn quyền đọc toàn bộ dữ liệu (Telemetry, State, History, NGSI-LD Detail) của Gateway được gán; có quyền tạo Sensor mới (`POST`) và đổi tên Sensor (`PATCH`). Không có quyền sửa Gateway (`403 Forbidden`).
- **Gateway Operator & Viewer:** Thành viên vận hành và người quan sát. Có toàn quyền đọc dữ liệu của Gateway được chỉ định trong danh sách thành viên. **Tất cả các hành vi ghi/sửa đổi (POST/PATCH) từ Operator và Viewer đều bị từ chối dứt khoát với mã `403 Forbidden`.**
- **Non-member (Người dùng không thuộc Gateway):** Phân định chặt chẽ theo từng route:
  * `GET /v1/digital-twins` (List): Trả về `200 OK` nhưng phạm vi danh sách được lọc theo membership (chỉ hiển thị các Gateway được gán; trả về danh sách rỗng `[]` nếu người dùng không sở hữu hay tham gia Gateway nào).
  * `GET /v1/digital-twins/{entity_id}` (Detail, State, History): Trả về `404 Not Found` đối với tài nguyên không thuộc quyền truy cập.
  * `PATCH /v1/digital-twins/{entity_id}`: Sửa Sensor không thuộc quyền trả về `404 Not Found`; sửa Gateway không thuộc quyền trả về `403 Forbidden` (do thao tác Gateway đòi hỏi quyền Platform Admin).

### 4.2 Định danh URN tự sinh từ phía máy chủ
Người dùng không tự chỉ định `id` dạng chuỗi ngẫu nhiên. Máy chủ tự động sinh URN chuẩn mực:
- Gateway: `urn:ngsi-ld:Gateway:<gateway_id>`
- Sensor: `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`

### 4.3 Cơ chế Tạo mới (POST) và Idempotent Replay
- **Tạo mới thực thể chưa tồn tại:** Trả về mã `201 Created` kèm DTO siêu dữ liệu phẳng (`PostGatewayResponseDTO` hoặc `PostSensorResponseDTO`), tuyệt đối không trả về đồ thị quan hệ, context hay UUID nội bộ.
- **Phát lại chính xác (Exact Replay):** Gửi lại đúng nguyên văn các trường thông tin đã đăng ký trước đó sẽ được máy chủ chấp nhận an toàn, trả về `200 OK` (idempotent replay, không tạo bản ghi trùng).
- **Xung đột cấu hình (Representation Conflict):** Cùng định danh nhưng thay đổi thuộc tính (ví dụ gửi sai `name` hoặc `unit` ban đầu) sẽ bị từ chối với mã `409 Conflict`.
- **An toàn giao thức:** Tuyệt đối không sinh credential MQTT hoặc tài khoản đăng nhập người dùng khi gọi POST entity.

### 4.4 Quy tắc Cập nhật Metadata (PATCH) và Tính bất biến (Invariants)
1. **Allowlist trường cập nhật:**
   - Gateway: Chỉ cho phép cập nhật `name` (chuỗi hợp lệ) và `description` (chuỗi mới hoặc `null` để xóa bỏ). Không cho phép sửa đổi `owner_user_id`.
   - Sensor: Chỉ cho phép cập nhật duy nhất `name` (chuỗi ký tự không rỗng). **Trường `unit` (đơn vị đo) là bất biến vĩnh viễn** sau khi tạo; mọi yêu cầu PATCH có chứa `unit` đều nhận mã `400 Bad Request`.
2. **Lưu trữ mô tả Gateway:** Mô tả được lưu trữ trực tiếp vào cột quan hệ `gateways.description`. Bảng `twin_entities.attributes` được bảo toàn nguyên vẹn.
3. **Thời gian cập nhật:** Cột `twin_entities.updated_at` sử dụng hàm `statement_timestamp()` để tránh trôi ngược thời gian so với mốc bắt đầu transaction cũ (`now()`), nhưng không đưa ra cam kết về tính đơn điệu tuyệt đối theo đồng hồ vật lý hay đồng hồ phân tán.
4. **Không phát sinh thao tác thừa (No-op PATCH):** Gửi yêu cầu với nội dung không thay đổi so với CSDL sẽ được trả về `200 OK` ngay mà không thực thi câu lệnh SQL UPDATE, giữ nguyên dấu thời gian `updated_at` cũ.
5. **Cơ chế khóa chống Deadlock:** Gateway PATCH áp dụng khóa `FOR UPDATE` tuần tự. Sensor PATCH áp dụng khóa `FOR SHARE` trên Gateway cha trước khi lấy khóa `FOR UPDATE` trên Sensor, giải quyết xung đột deadlock mang tính đại diện với luồng nạp dữ liệu cảm biến ngầm định (telemetry ingestion), nhưng không khẳng định mọi kịch bản đồng thời tùy ý đều không có deadlock.

---

## 5. History bounds, raw/bucket và temporal semantics

### 5.1 Giới hạn truy vấn và cửa sổ thời gian (Query Bounds)
Tất cả các truy vấn lịch sử được kiểm soát chặt chẽ thông qua cấu hình `HistoryConfig`:
- **Độ dài cửa sổ thời gian tối đa (`MaxRange`):** 744 giờ (31 ngày). Khoảng thời gian yêu cầu `[from, to)` vượt quá 744h sẽ bị từ chối ngay với mã `400 Bad Request`.
- **Cửa sổ đọc dữ liệu thô tối đa (`RawWindow`):** 10 giây.
- **Số điểm đo tối đa trên một phản hồi (`MaxPoints`):** Mặc định theo hợp đồng là 1.000 điểm (`ContractDefaultMaxPoints = 1000`); giới hạn cứng tối đa 2.000 điểm (`ContractMaxPointsUpperLimit = 2000`); tối thiểu 100 điểm (`ContractMinPointsLowerLimit = 100`). (Lưu ý: Dataset đo lường trong bài test M1/M3/M4 cấu hình mức trần thử nghiệm `max_points = 500`).
- **Giới hạn phân trang danh sách Twin:** Mặc định 50 mục (`ContractDefaultLimit = 50`), tối đa 200 mục (`ContractMaxLimit = 200`).
- **Ngân sách thời gian xử lý (`QueryTimeout`):** Giới hạn cứng 5,0 giây. Quá thời gian này, giao dịch tự động bị hủy (fail-closed) và trả về `503 Service Unavailable`, không bao giờ trả về dữ liệu thiếu hoặc thành công ảo.

### 5.2 Ngữ nghĩa nửa khoảng `[from, to)` và định dạng thời gian
- Khoảng thời gian luôn tuân thủ quy ước nửa mở nửa đóng: **tính từ `from` (bao gồm) đến trước `to` (loại trừ)**.
- Dấu thời gian yêu cầu chấp nhận định dạng RFC3339 chuẩn có timezone offset tường minh (ví dụ: `+07:00` hoặc `Z`) và giới hạn độ chính xác microsecond. Dữ liệu thời gian trả về luôn được máy chủ chuẩn hóa sang múi giờ UTC theo định dạng RFC3339 microsecond kết thúc bằng `Z` (ví dụ: `2026-10-18T08:00:00.000000Z`).

### 5.3 Chế độ Đọc: Raw vs Downsampled Bucket
1. **Chế độ dữ liệu thô (`resolution=raw`):**
   - Chỉ cho phép nếu `(to - from) <= 10s`. Trả về từng mẫu đo độc lập theo thứ tự `measured_at ASC, sample_id ASC`.
   - Nếu số mẫu đo vượt quá `max_points`, máy chủ từ chối với lỗi `400 Bad Request` kèm mã `raw_limit_exceeded`, tuyệt đối không âm thầm cắt ngắn dữ liệu (silent truncation).
2. **Chế độ tổng hợp gom nhóm (`resolution=<allowlisted>`):**
   - Hỗ trợ chính xác 9 độ phân giải bucket trong allowlist của telemetry: `100ms`, `1s`, `10s`, `1m`, `5m`, `15m`, `1h`, `6h`, `1d`.
   - Sử dụng hàm `time_bucket` của TimescaleDB căn chỉnh theo gốc kỷ nguyên UTC (`1970-01-01 00:00:00+00`).
   - Mỗi xô (bucket) tính toán chính xác 4 chỉ số: `avg` (trung bình), `min` (nhỏ nhất), `max` (lớn nhất), và `count` (số lượng mẫu).
   - **Bảo tồn khoảng trống (Gap Preservation):** Các xô không có dữ liệu đo đạc sẽ bị bỏ qua, tuyệt đối không tự ý điền giá trị 0 (no empty-bucket gapfill) để tránh làm sai lệch biểu đồ phân tích.
3. **Chế độ tự thích ứng (`resolution=auto`):**
   - Với khoảng thời gian ngắn $\le$ 10s: Máy chủ thử truy vấn dữ liệu thô. Nếu số điểm đo không vượt quá giới hạn, trả về dữ liệu `raw`. Nếu phát hiện vượt quá giới hạn, hệ thống tự động hủy kết quả thô và chuyển sang xô tổng hợp thích hợp được chọn tự động từ allowlist dựa trên độ dài khoảng thời gian và `max_points` (ví dụ: trong kịch bản kiểm thử M3 với cửa sổ 8s, độ phân giải `100ms` được chọn vì cho ra 80 buckets $\le$ `max_points`).
   - Với khoảng thời gian dài $>$ 10s: Hệ thống gọi thuật toán `SelectAutoBucketResolution` để chọn độ phân giải bucket mịn nhất trong allowlist sao cho tổng số bucket ước tính không vượt quá `max_points`.

### 5.4 Ngữ nghĩa lịch sử Digital Twin (Temporal Properties)
- Endpoint `GET /v1/digital-twins/{entity_id}/history` chỉ áp dụng cho thực thể loại `Sensor`. Gateway không có thuộc tính đo đạc trực tiếp, yêu cầu sẽ bị từ chối với mã `400`.
- **Selector `property=value`:** Máy chủ ủy quyền trực tiếp sang `telemetry.TelemetryHistoryService`, đọc từ hypertable `telemetry` duy nhất mà không duy trì một kho lưu trữ mẫu tần số cao trùng lặp.
- **Selector `property=sample_interval_us`:** Đọc lịch sử chu kỳ lấy mẫu từ hypertable `twin_temporal_values`.
  * **Quy tắc đơn vị:** Đơn vị của chuỗi `sample_interval_us` được lấy từ trường `metadata.unit` của bản ghi chuỗi thời gian (thường là `"us"` hoặc có thể mang giá trị `null` nếu bản ghi chuỗi thời gian không khai báo đơn vị), tuyệt đối không lấy từ cột `sensors.unit` (vốn là đơn vị đo vật lý của cảm biến như độ C hay áp suất).

---

## 6. Twin representation, pagination và kiểu dữ liệu

### 6.1 Chuẩn hóa biểu diễn NGSI-LD và Inline Context Object
- Phản hồi chi tiết `GET /v1/digital-twins/{entity_id}` là một tài liệu JSON-LD hoàn chỉnh, trả về với header `Content-Type: application/ld+json; charset=utf-8`.
- Khối `@context` là một đối tượng JSON nội tuyến (inline JSON object theo struct `mapper.Context`), không sử dụng mảng tham chiếu URL từ xa nhằm triệt tiêu độ trễ mạng và rủi ro SSRF:
  ```json
  {
    "@context": {
      "@version": 1.1,
      "id": "@id",
      "type": "@type",
      "Property": "https://uri.etsi.org/ngsi-ld/Property",
      "Relationship": "https://uri.etsi.org/ngsi-ld/Relationship",
      "value": "https://uri.etsi.org/ngsi-ld/hasValue",
      "observedAt": "https://uri.etsi.org/ngsi-ld/observedAt",
      "createdAt": "https://uri.etsi.org/ngsi-ld/createdAt",
      "modifiedAt": "https://uri.etsi.org/ngsi-ld/modifiedAt",
      "name": "https://example.org/ngsi-ld/iot/v1#name",
      "description": "https://example.org/ngsi-ld/iot/v1#description",
      "unit": "https://example.org/ngsi-ld/iot/v1#unit",
      "measurement": "https://example.org/ngsi-ld/iot/v1#measurement",
      "sampleIntervalUs": "https://example.org/ngsi-ld/iot/v1#sampleIntervalUs",
      "hasSensor": "https://example.org/ngsi-ld/iot/v1#hasSensor",
      "gateway_id": "https://example.org/ngsi-ld/iot/v1#gatewayId"
    }
  }
  ```
- Các phản hồi danh sách (`GET /v1/digital-twins`), trạng thái (`/state`), lịch sử (`/history`) và các phản hồi lỗi luôn sử dụng định dạng chuẩn `application/json; charset=utf-8`.

### 6.2 Phân trang Keyset (Keyset Cursor Pagination)
Hệ thống sử dụng cơ chế phân trang dựa trên khóa chỉ mục (keyset pagination) thay cho phân trang `offset` truyền thống nhằm tránh suy giảm hiệu năng khi quét bảng lớn:
1. **Cursor danh sách Twin (`v1.{k,g,t}`):** Mã hóa base64url bộ 3 trường `[entity_key, gateway_id, entity_type]`, sắp xếp theo collation `"C"` chuẩn mực.
2. **Cursor phân trang quan hệ (`v1.{s,r,k}`):** Dùng để phân trang danh sách cảm biến (`hasSensor`) trong chi tiết Gateway: mã hóa bộ 3 trường `[source_entity_id, relationship_type, target_entity_id]`.
3. Khi còn dữ liệu ở trang tiếp theo, phản hồi trả về trường `next_cursor` (chuỗi ký tự). Ở trang cuối cùng, `next_cursor` mang giá trị `null`.
4. **Lưu ý về tính nhất quán:** Phân trang keyset không phải là một snapshot cô lập hoàn toàn (MVCC snapshot xuyên suốt nhiều HTTP requests); các thao tác ghi/xóa dữ liệu diễn ra đồng thời giữa các lần gọi có thể làm dịch chuyển ranh giới bản ghi giữa các trang.

### 6.3 Xử lý kiểu dữ liệu số nguyên lớn và lưu ý giải mã phía Web Client
- **Bảo toàn số nguyên lớn (`BIGINT`) trên giao thức:** Do đặc tả IEEE 754 của JavaScript trên trình duyệt (Flutter Web) chỉ biểu diễn chính xác số nguyên an toàn đến $2^{53} - 1$ (`Number.MAX_SAFE_INTEGER`), tất cả các số nguyên 64-bit như số thứ tự mẫu (`sequence_num`), phiên bản trạng thái (`reported_version`, `desired_version`), và số lượng mẫu (`count`) **bắt buộc được máy chủ tuần tự hóa thành chuỗi số thập phân (decimal string)** (ví dụ: `"reported_version": "2"`).
- **Trạng thái JSONB mở rộng:** Trường `reported_state` và `desired_state` được lưu giữ nguyên trạng dưới dạng `json.RawMessage` ở máy chủ Go để bảo toàn chính xác chuỗi byte nguyên bản. **Cảnh báo phía máy khách:** Các bộ giải mã JSON tiêu chuẩn phía client (như `JSON.parse` của trình duyệt hoặc bộ chuyển đổi mặc định của Dart) sẽ tự động ép kiểu các số lớn trong JSONB về dạng số thực dấu phẩy động (float64) gây mất độ chính xác, trừ khi phía client sử dụng cơ chế giải mã BigInt chuyên biệt.

---

## 7. Cách kiểm tra và môi trường đã chạy

### 7.1 Công cụ và kịch bản thực thi tích hợp
Hệ thống kiểm thử tích hợp được đóng gói tự động hóa thông qua script `scripts/test-stage4-api.sh` (điều phối bộ thực thi `scripts/tests/stage4_api_runner.py`), hỗ trợ các cờ lệnh:
- `sh scripts/test-stage4-api.sh --scenario vertical`: Chạy chu trình tích hợp dọc end-to-end.
- `sh scripts/test-stage4-api.sh --scenario contract`: Kiểm chứng hợp đồng giao tiếp, boundaries và ma trận phân quyền.
- `sh scripts/test-stage4-api.sh --scenario queries`: Thực thi đo lường kế hoạch truy vấn chưa nén và đã nén.
- `sh scripts/test-stage4-api.sh --scenario all`: Thực thi toàn bộ cả 3 kịch bản trên một cách tuần tự.
- Cờ bắt buộc an toàn: `--require-run` (chống false-PASS khi môi trường thiếu Docker) và `--report <file>` (xuất báo cáo JSON tóm tắt).

### 7.2 Môi trường thực thi thực tế (Isolated Real Services)
Các dịch vụ phục vụ kiểm thử chạy độc lập trên các container Docker chuyên dụng:
- **Hệ quản trị CSDL:** PostgreSQL 16.15 kết hợp TimescaleDB 2.29.2 (Schema 20 terminal: `20:stage3_telemetry_ingestion`).
  * *Lưu ý về kết nối:* Môi trường fixture nội bộ sử dụng chuỗi kết nối `postgres://iot_backend_app:...@database:5432/fixture_service?sslmode=disable`. Việc MQTT dùng TLS không đồng nghĩa kết nối CSDL nội bộ có TLS.
- **MQTT Broker:** Eclipse Mosquitto 2.0.18 với Dynamic Security Plugin (DynSec), kết nối bảo mật qua cổng TLS 8883 với chứng chỉ CA nội bộ.
- **Bản dựng Backend & Gateway:** Được biên dịch đa tầng trực tiếp từ `src/Dockerfile` (`target: backend` và `target: gateway`).
- **Phân định cơ chế xác thực Fixture:** Bộ test tích hợp Stage 4 sử dụng token JWT HS256 tạm thời (ephemeral token) được xác minh qua `TokenVerifier` nội bộ của Backend. Bộ test này **không khởi động toàn bộ stack Supabase Auth / GoTrue / Envoy** (stack này đã được xác minh riêng biệt trong bài diễn tập Stage 2 Rehearsal).
- **Phạm vi dọn dẹp (Cleanup Scope):** Sau khi hoàn thành bài test, toàn bộ các bảng dữ liệu liên quan đến fixture được xóa sạch (0 hàng tồn đọng), và các tag hình ảnh của suite kiểm thử được thu hồi sạch sẽ.

---

## 8. Kết quả tích hợp và query measurements

### 8.1 Kết quả kiểm thử chu trình dọc (Vertical Slice)
1. **Khởi tạo và Uplink:** Admin tạo Gateway A, Owner tạo Sensor A (201 Created); phát lại nguyên văn trả về 200 OK. Gateway B và Sensor B được tạo làm đối chứng cô lập.
2. **Nạp dữ liệu cảm biến:** Gateway simulator gửi 2 batches $\times$ 100 mẫu qua MQTT TLS. Backend ghi nhận thành công: 200 mẫu raw vào bảng `telemetry`, 2 dòng sổ cái `processed_messages`, 2 bản ghi `twin_temporal_values`, và cập nhật `reported_version = 2` trên `twin_states`. Toàn bộ outbox của Gateway được xóa sau khi nhận Application ACK.
3. **Đọc dữ liệu:** Cả Owner, Operator, Viewer đều đọc được 200 điểm đo raw, đồ thị thực thể JSON-LD chi tiết, và trạng thái hiện tại. Người dùng B (thuộc Gateway B) bị chặn hoàn toàn khi cố đọc Gateway A (`404 Not Found`).
4. **Cập nhật Metadata và Bất biến dữ liệu:** Owner đổi tên Sensor; Admin cập nhật mô tả Gateway (`200 OK`). Toàn bộ 200 mẫu đo cũ, dấu thời gian trạng thái, và chuỗi lịch sử được bảo toàn nguyên vẹn 100%.
5. **Nạp tiếp và Lọc trùng lặp:** Simulator gửi thêm 1 batch $\times$ 100 mẫu, tổng số mẫu tăng lên đúng 300 mẫu, `reported_version` tăng lên 3, tên mới của Sensor vẫn được giữ vững. Khi simulator cố tình gửi lại bản tin cũ, backend nhận diện trùng lặp, lập tức phản hồi ACK `committed` và không thêm bất kỳ hàng nào vào CSDL.

### 8.2 Bảng kết quả đo lường truy vấn (Query Measurements Checkpoint)
Các phép đo được thực hiện trên dataset chuẩn gồm:
- **Dữ liệu mục tiêu:** 60.000 mẫu telemetry thô @ 100 Hz (khoảng thời gian 10 phút) + 580 bản ghi temporal interval.
- **Dữ liệu gây nhiễu (Distractor):** 6.000 mẫu telemetry thô @ 10 Hz + 60 bản ghi temporal interval.
- **Trạng thái so sánh:** Thực hiện đo dưới vai trò ứng dụng `iot_backend_app` (non-superuser), so sánh giữa trạng thái **Chưa nén (Uncompressed)** và **Đã nén (Compressed)** sau khi gọi `compress_chunk` trên các chunk mục tiêu (`_hyper_1_1_chunk` và `_hyper_2_2_chunk`).

*Bảng dữ liệu trích xuất từ checkpoint gốc trước review (lưu độc lập với summary run sau sửa sáu lỗi):*

| Mã | Loại truy vấn | Cấu hình & Phạm vi kiểm tra | Chưa nén (Uncompressed)<br>Plan / Exec / Hits | Đã nén (Compressed)<br>Plan / Exec / Hits | Thời gian Service<br>(Unc / Comp) | Thời gian HTTP<br>(Unc / Comp) | Nút thực thi chính (Compressed Plan) |
|---|---|---|---|---|---|---|---|
| **M1** | Raw Telemetry | Cửa sổ 5s, 500 mẫu @ 100Hz, cap=500 | 3.395ms / 0.601ms / 32 hits | 5.510ms / 0.805ms / 57 hits | 1.865ms / 2.038ms | 14.084ms / 6.426ms | Index Scan, Custom Scan |
| **M2** | Bucket Telemetry | Cửa sổ 10m, auto gom 10s $\rightarrow$ 60 buckets | 0.972ms / 56.457ms / 2.101 hits | 1.154ms / 48.156ms / 418 hits | 46.284ms / 40.141ms | 50.659ms / 41.701ms | Custom Scan, Aggregate |
| **M3** | Overflow Fallback | Probe 8s (800 mẫu vượt cap 500) $\rightarrow$ gom 100ms (80 buckets) | 0.696ms / 1.595ms (tổng) | 1.120ms / 1.797ms (tổng) | 3.282ms / 4.030ms | 3.969ms / 4.088ms | Probe Index Scan $\rightarrow$ Fallback Custom Scan |
| **M4** | Raw Temporal | Cửa sổ 10s, 10 observations, cap=500 | 4.730ms / 0.588ms / 32 hits | 7.136ms / 1.336ms / 68 hits | 2.857ms / 3.143ms | 8.096ms / 5.517ms | Custom Scan, Hash Join |
| **M5** | Bucket Temporal | Cửa sổ 10m, gom 1m $\rightarrow$ 10 buckets (gap kiểm chứng: bucket 5 = 40 mẫu) | 1.589ms / 1.431ms / 50 hits | 2.122ms / 1.746ms / 26 hits | 3.951ms / 4.244ms | 4.877ms / 4.550ms | Custom Scan, Aggregate |

### 8.3 Nhận xét kỹ thuật và Quyết định CSDL
1. **Hiệu quả nén vượt trội:** Trên truy vấn M2 (tính toán thống kê trên 60.000 mẫu), việc nén hypertable giúp giảm lượng bộ đệm đĩa truy xuất (buffer hit blocks) từ 2.101 blocks xuống còn 418 blocks (**giảm ~80% số khối nhớ đọc**), giảm thời gian thực thi câu lệnh SQL từ 56,45ms xuống 48,15ms nhờ cơ chế TimescaleDB Custom Scan đọc trực tiếp dữ liệu nén theo cột.
2. **Tuân thủ ngân sách thời gian:** Toàn bộ 5 phép đo truy vấn đều hoàn thành dưới 60ms tại tầng CSDL và dưới 55ms tại tầng phản hồi HTTP, nằm sâu bên trong ngưỡng an toàn của ngân sách cấu hình 5,0s.
3. **Quyết định CSDL chính thức:** **Hệ thống giữ nguyên Schema 20 và cấu trúc chỉ mục sẵn có.** Không bổ sung thêm chỉ mục phỏng đoán (speculative index), không tạo continuous aggregate (tránh phức tạp hóa việc bảo trì dữ liệu ở quy mô MVP).

---

## 9. Acceptance và CI

### 9.1 Bảng đối chiếu tiêu chí nghiệm thu H01 – H15
Bảng đối chiếu giữ đúng ý nghĩa tiêu chí đã được xác lập trong [Kế hoạch chi tiết Stage 4](../../../docs/backend_plan/stage4_backend_detailed_plan.md), phân định rõ nguồn gốc bằng chứng: **NEW** (kiểm thử mới xây dựng ở Task 4.7), **INHERITED** (kế thừa từ unit/native test của các tasks trước), và **NOT RUN** (các hạng mục ngoài phạm vi hoặc chưa chạy):

| Mã | Nội dung tiêu chí nghiệm thu | Bằng chứng kiểm thử & Test Case cụ thể | Trạng thái | Ghi chú & Giới hạn kỹ thuật |
|:---:|---|---|:---:|---|
| **H01** | Từ chối truy cập khi thiếu/sai/hết hạn JWT; chặn tiêm user/role qua query/cursor | NEW HTTP: `stage4_api_runner.py` (contract auth cases) | PASS | Xác thực qua JWT fixture; không thay thế toàn bộ luồng đăng nhập Supabase |
| **H02** | Hai Gateway có cùng Sensor ID; cách ly dữ liệu triệt để; Admin không đọc global nếu thiếu membership | NEW HTTP: `stage4_api_runner.py` (vertical cross-tenant & admin non-member read) | PASS | List chỉ trả về tài nguyên có quyền; truy cập trái phép trả về 404 |
| **H03** | Owner/Operator/Viewer đọc Gateway được gán; non-member/missing trả 404; rỗng hợp lệ trả 200 | NEW HTTP: vertical role reads & contract empty cases | PASS | Được kiểm chứng trên toàn bộ 5 endpoints đọc dữ liệu |
| **H04** | Nửa khoảng thời gian `[from, to)`, chuẩn microsecond, mẫu trùng thời gian qua các lần boot, legacy NULL provenance | NEW HTTP M1; NEW native upgrade `TestNativeTelemetryHistoryUpgradedLegacyProvenance`; INHERITED unit tests | PASS (Mixed) | Dữ liệu cũ thiếu provenance chạy tốt trên native; kiểm thử qua HTTP delegator cho dữ liệu legacy **NOT RUN** do Schema 20 hiện tại ép NOT NULL |
| **H05** | Xử lý vượt giới hạn dữ liệu thô (raw overflow), khoảng thời gian vượt cap, không trả về thành công giả khi timeout | NEW HTTP contract & M3; INHERITED `TestPrepareTelemetryHistory_CanceledOrExpiredNilSuccessRejected` | PASS (Mixed) | M3 tự động fallback về bucket 100ms khi thô vượt 500 điểm; timeout fail-closed an toàn |
| **H06** | Tính toán bucket đúng chuẩn (avg/min/max/count), cắt cúp khoảng thời gian, bảo toàn gap dữ liệu | NEW HTTP M2/M3/M5; INHERITED `TestNativeTelemetryHistoryBucketArithmeticAndClipping` | PASS (Mixed) | Khớp công thức số học; không điền 0 vào xô trống |
| **H07** | Phân trang danh sách Twin qua cursor keyset `v1.{k,g,t}`, định dạng URN chuẩn, trang quan hệ | NEW HTTP contract pagination & vertical graph; INHERITED `TestNativeTwinRead` | PASS (Mixed) | Không lặp/bỏ trong dataset kiểm thử ổn định; cursor không tạo snapshot xuyên nhiều requests khi có ghi đồng thời |
| **H08** | Đọc trực tiếp trạng thái `twin_states`, bảo toàn kiểu dữ liệu JSON, bảo toàn phiên bản và số lớn | NEW HTTP vertical state; INHERITED `TestNativeTwinRead`, `TestTwinStateResponseDTO` | PASS (Mixed) | Câu lệnh SQL đọc thẳng bảng state; hỗ trợ số nguyên lớn `> 2^53` kế thừa từ Task 4.3 |
| **H09** | Lịch sử thuộc tính thời gian (`sample_interval_us` đúng đơn vị; `value` ủy quyền sang telemetry) | NEW HTTP vertical / M4 / M5; INHERITED `TestNativeTemporalHistory_NumericSlice` | PASS (Mixed) | Đơn vị lấy từ `metadata.unit` chuỗi thời gian; ủy quyền đọc telemetry hoạt động trơn tru |
| **H10** | POST Gateway (Admin), POST Sensor (Admin/Owner), URN tự sinh, exact replay, conflict 409 | NEW HTTP vertical & contract POST cases; INHERITED `TestNativeEntityMetadata_MutationSlice` | PASS (Mixed) | Không sinh MQTT credential hay tài khoản Supabase khi tạo entity |
| **H11** | Chặn thao tác sửa đổi phi quyền (Operator/Viewer/Non-member); không gây tác dụng phụ | NEW HTTP contract denials; Full-row snapshot xác minh tính bất biến | PASS | Snapshot bảo đảm không hàng dữ liệu nào bị sửa đổi hay tạo thêm khi bị từ chối |
| **H12** | PATCH metadata (tên/mô tả), rollback đồng thời khi lỗi SQL, bảo toàn trạng thái/lịch sử/outbox | NEW HTTP vertical PATCH/ingest; INHERITED `TestNativeEntityMetadata_MutationSlice` | PASS (Mixed) | Cập nhật tên Sensor và mô tả Gateway thành công; dữ liệu đo đạc nạp tiếp bình thường |
| **H13** | Xử lý timeout/hủy bỏ kết nối CSDL, giải phóng connection pool, thực thi giới hạn truy vấn | NEW HTTP contract bounds; INHERITED `TestNativeTemporalHistory_NumericSlice` (pool acquire timeout) | PASS (Mixed) | Hết timeout chờ pool sẽ fail-closed; ngắt câu lệnh đang chạy ở server CSDL **NOT RUN** |
| **H14** | Chu trình dọc: MQTT commit $\rightarrow$ Application ACK $\rightarrow$ HTTP đọc dữ liệu đúng phân quyền | NEW HTTP `run_vertical_scenario` (300 mẫu raw, 3 ledger, 3 temporal, version 3) | PASS | Bản tin gửi lại byte-exact được lọc trùng thành công mà không tăng bản ghi |
| **H15** | Kiểm thử tích hợp tự động, làm sạch môi trường, hồi quy đầy đủ, CI tích hợp và contract đồng bộ | Run tích hợp hoàn tất; 16/16 jobs CI hoàn thành; báo cáo và contract được cập nhật | PASS | Remote CI đã PASS tại commit `aa7436d`; tài liệu tổng kết Stage 4 hoàn thành |

### 9.2 Bằng chứng CI Pipeline từ xa (Remote CI Evidence)
Trạng thái kiểm định CI từ xa đã được thẩm tra trực tiếp thông qua GitHub Actions metadata:
- **Workflow Run:** [39103564379](https://github.com/TVTIT/project1_iot_server/actions/runs/39103564379)
- **Commit:** `aa7436d8838a6580c21b30930e2eed992999b11c`
- **Kết quả:** **16/16 jobs completed successfully** (Toàn bộ 16/16 jobs đều thành công 100%).
  * Job chuyên biệt `stage4-api-integration`: Thực hiện build sạch từ `src/Dockerfile`, kích hoạt container dịch vụ thật, chạy toàn bộ `scripts/test-stage4-api.sh --scenario all`, trích xuất an toàn tóm tắt không chứa bí mật.
  * Các jobs hồi quy: `stage3-uplink-integration`, `native-compose-smoke`, `go-test-race`, `golangci-lint`, `python-tests` đều đạt kết quả xanh.
- **Lịch sử sửa lỗi CI:** Run trước đó `39100732763` từng bị lỗi tại commit `6257f80` do xung đột đường dẫn báo cáo tạm thời và định danh job; hai commit kế tiếp `65d727b` và `aa7436d` đã khắc phục triệt để và đưa CI về trạng thái xanh hoàn hảo.

---

## 10. Giới hạn và bàn giao Stage 5

### 10.1 Các giới hạn kỹ thuật được ghi nhận (Technical Limitations)
Tuân thủ Mục 0.4 của [AGENTS.md](../../AGENTS.md), các giới hạn sau đây được ghi nhận chính thức vào backlog hệ thống, không coi là lỗi che giấu:
1. **Kiểm thử hủy truy vấn máy chủ CSDL (Executing-Query Cancellation):** Bằng chứng hiện có mới chứng minh việc hủy bỏ và trả lỗi khi hết thời gian chờ mượn kết nối từ connection pool (`acquire timeout`), chưa thực hiện ngắt cưỡng bức một truy vấn SQL nặng đang quét dở trên máy chủ PostgreSQL.
2. **Kiểm thử dữ liệu cũ thiếu thông tin nguồn qua HTTP (Legacy NULL Provenance via Twin HTTP):** Bài test nâng cấp native đã kiểm chứng việc đọc các mẫu dữ liệu cũ có `boot_id/message_id` mang giá trị NULL, nhưng chưa chạy qua tầng HTTP delegator vì Schema 20 hiện hành đã áp đặt ràng buộc `NOT NULL`.
3. **Thứ tự các mẫu đo trùng thời gian (Temporal Timestamp Ties):** Các mẫu đo có cùng thời gian đo đạc trên microsecond từ các lần khởi động khác nhau được bảo toàn đầy đủ về số lượng, nhưng không cam kết một thứ tự hiển thị tuyệt đối giữa các mẫu trùng nhau đó.
4. **Giới hạn quy mô tải (Scale & Soak Limits):** Bài test đo lường được thực hiện trên dataset chuẩn 60.000 mẫu (tương đương 10 phút đo đạc ở tần số 100 Hz). Hệ thống chưa thực hiện các chiến dịch thử tải dài ngày (soak testing) hoặc tải cực hạn phân tán.
5. **Giao diện người dùng và Thiết bị phần cứng:** Các ví dụ DTO trong tài liệu chỉ phục vụ giao tiếp tầng API. Giao diện Flutter Web và việc chạy trên bo mạch nhúng thực tế (TI AM5728 / Luckfox) chưa được triển khai tại giai đoạn này.

### 10.2 Nền tảng bàn giao sẵn sàng cho Stage 5 (Digital Twin Downlink)
Toàn bộ mã nguồn, cấu hình và kịch bản của Stage 4 tạo thành bệ phóng vững chắc để bước vào Stage 5 (Lát cắt điều khiển thiết bị):
- **Hệ thống thực thể và URN chuẩn mực:** Toàn bộ Gateway và Sensor đã có định danh NGSI-LD ổn định, được lưu trữ đồng bộ giữa bảng quan hệ và bảng đồ thị `twin_entities`.
- **Cơ chế xác thực và phân quyền RBAC:** Bộ giải quyết quyền `user_gateways` và kiểm tra quyền Platform Admin sẵn sàng tái sử dụng cho các API gửi lệnh điều khiển.
- **Mô hình trạng thái hiện tại:** Cấu trúc bảng `twin_states` với hai phiên bản `reported_version` và `desired_version` độc lập, bảo đảm an toàn cho các thao tác cập nhật cấu hình mong muốn theo cơ chế khóa lạc quan (optimistic concurrency control).
- **Hạ tầng kiểm thử tích hợp:** Runner `test-stage4-api.sh` và bộ tạo dữ liệu mô phỏng Gateway simulator sẵn sàng mở rộng để kiểm chứng luồng nhận lệnh điều khiển qua MQTT (`gateways/<id>/commands/#`) và phản hồi kết quả (`gateways/<id>/responses/#`).

### 10.3 Hạng mục cần thực hiện trong Stage 5:
1. Xây dựng endpoint cập nhật cấu hình mong muốn: `PATCH /v1/digital-twins/{entity_id}/desired-state`.
2. Thiết kế bảng hàng đợi lệnh gửi đi (`twin_commands`) và bảng outbox giao dịch (`twin_outbox`).
3. Xây dựng Outbox Publisher gửi lệnh an toàn qua giao thức MQTT tới topic của Gateway.
4. Xử lý phản hồi xác nhận thực thi từ Gateway, cập nhật kết quả lệnh và đồng bộ trạng thái `reported_state` sau khi thiết bị xác nhận hoàn thành.
