# Báo cáo Kỹ thuật Task 4.7 — Tích hợp API Stage 4, Đo lường Truy vấn và CI Pipeline

**Ngày thực hiện:** 18/10/2026  
**Trạng thái:** Follow-up `go-reviewer` (`openai/gpt-6.1-sol#high`) trả **APPROVE**, xác nhận 1 HIGH và 5 MEDIUM đều CLOSED. Native `all` sau sửa **PASS**; remote CI **NOT RUN**. Hai ghi chú LOW về prose được chỉnh sau review, không đổi code.  
**Baseline Git:** Commit `e6e304d41b3915b2be51e27fee6bf100b04e60ea` + diff Task 4.7 chưa commit.  
**Tài liệu thẩm quyền căn cứ:** Assertions thực tế và artifact `docs/backend/stage-4-task-4.7-summary.json`, cập nhật từ native rerun `/tmp/opencode/stage4-api-six-fixes.json`. Bảng số đo mục 4 giữ checkpoint trước review để không trộn hai run.

---

## 1. Mục tiêu và Ranh giới Kỹ thuật

Task 4.7 hoàn thành lát cắt tích hợp cuối cùng của Giai đoạn 4 theo [Kế hoạch chi tiết Task 4.7](../../docs/backend_plan/task_4.7_detail_plan.md) và [AGENTS.md](../../AGENTS.md) (Mục 0 & 16):
1. **Runner tích hợp đa dịch vụ thực:** Xây dựng `scripts/test-stage4-api.sh` và runner `scripts/tests/stage4_api_runner.py` chạy trên container dịch vụ thật (Mosquitto 2.0.18 TLS, TimescaleDB PG16, Go backend, Gateway simulator).
2. **Vertical Slice End-to-End:** Kiểm chứng chu trình khép kín: HTTP Provisioning $\rightarrow$ MQTT TLS Ingress $\rightarrow$ Bounded Queue/Worker $\rightarrow$ PostgreSQL Commit $\rightarrow$ Application ACK $\rightarrow$ HTTP Reads (History, Detail, State) $\rightarrow$ Metadata PATCH $\rightarrow$ Ingest tiếp tục $\rightarrow$ Deduplication byte-exact.
3. **HTTP Contract & Phân quyền RBAC:** Bao phủ toàn bộ 7 endpoints, xác thực JWT, phân quyền theo role (`owner`, `operator`, `viewer`, `user_b` non-member), cursor pagination, boundary validation, error envelopes, và snapshot bất biến sau khi từ chối ghi.
4. **Đo lường Truy vấn Chưa nén và Đã nén (Query Measurements):** Thực hiện trên dataset chuẩn 60.000 raw samples + 580 temporal observations dưới role ứng dụng `iot_backend_app` (non-superuser), so sánh trước và sau khi gọi `compress_chunk`.
5. **Tích hợp CI Pipeline:** Bổ sung job `stage4-api-integration` vào `.github/workflows/ci.yml`, bảo đảm preflight, guards, và chỉ trích xuất summary không chứa secret.

### Ranh giới và Đính chính Quan trọng
- **Phân định vai trò Database:** Role `fixture_admin` (superuser) chỉ dùng trong các bước setup fixture (tạo tài khoản, seed dataset, gọi API nén `compress_chunk`). Toàn bộ truy vấn của Go backend và các phép đo query plans chạy nghiêm ngặt dưới role ứng dụng `iot_backend_app` (non-superuser).
- **sslmode của CSDL:** CSDL trong fixture Docker nội bộ kết nối qua `postgres://iot_backend_app:...@database:5432/fixture_service?sslmode=disable`. Không được suy diễn DB có TLS từ việc MQTT dùng TLS (port 8883).
- **Phân định Auth Fixture:** Stage 4 fixture sử dụng JWT HS256 tạm thời (ephemeral signing secret) được backend xác thực qua TokenVerifier cục bộ. Fixture này **không khởi chạy toàn bộ stack Supabase Auth / Envoy / GoTrue** (stack đó đã được kiểm chứng riêng biệt trong bài diễn tập Stage 2 Auth Rehearsal).
- **Ngân sách 5.0s (Budget):** Đây là thời gian chờ tối đa được cấu hình cho request context (`HistoryPreparation` / `ReadPreparation`), **không phải là cam kết SLA vận hành sản xuất**.
- **Cơ chế fallback M3:** Khi raw probe vượt giới hạn `max_points` (500), hệ thống tự động fallback về độ phân giải bucket **100ms** (theo cấu hình mặc định), **không phải 1m**.
- **Hành vi từ chối Cross-Tenant:** List trả `200 OK` nhưng loại tài nguyên không có membership; không nhất thiết toàn list rỗng nếu actor có Gateway riêng. GET detail/state/history của non-member trả `404`; non-member Sensor PATCH trả `404`, còn Gateway PATCH trả `403` do thiếu platform admin. Operator/viewer mutation và owner Gateway PATCH trả `403`.
- **Media Type:** Header `Content-Type: application/ld+json; charset=utf-8` chỉ áp dụng nghiêm ngặt cho phản hồi thành công của `GET /v1/digital-twins/{entity_id}` (Detail). Phản hồi `GET /state` và phản hồi lỗi luôn dùng `application/json; charset=utf-8`.
- **Truy vấn trạng thái Twin:** Phản hồi từ `GET /state` không quét hypertable; tính chất này được chứng minh từ cấu trúc câu lệnh SQL của adapter (chỉ đọc bảng `twin_states` kết hợp `twin_entities`), không phải chỉ suy ra từ nội dung JSON trả về.
- **Bảo toàn số nguyên lớn > 2^53:** Đã được kiểm chứng trong unit/native tests của Task 4.3; các kịch bản tích hợp kế thừa cơ chế này mà không cần lặp lại toàn bộ ma trận số lớn trong payload HTTP.

---

## 2. Thông tin Môi trường và Bản dựng (Environment & Artifacts)

| Thành phần | Định danh / Phiên bản | Ghi chú |
|---|---|---|
| **Git Revision** | `e6e304d41b3915b2be51e27fee6bf100b04e60ea` | Baseline HEAD + diff Task 4.7 |
| **Go Backend Image** | `sha256:1a45b6b087384fa5e96f9dd7346f444684d08fb7541a5e1c10456d1c686831f9` | Build đa tầng từ `src/Dockerfile` (`target: backend`) |
| **Gateway Image** | `sha256:84c588f374532b4595a4cc6e6be12c90cbf6c5cdcf5e89aea31aef63e204fb1e` | Build đa tầng từ `src/Dockerfile` (`target: gateway`) |
| **TimescaleDB** | `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a` | PostgreSQL 16.15 + TimescaleDB 2.29.2; metadata lấy từ cùng constant của DatabaseFixture |
| **Mosquitto** | `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9` | Mosquitto 2.0.18 với DynSec plugin |
| **Schema Version** | `20` (terminal: `stage3_telemetry_ingestion`) | Giữ nguyên Schema 20, không thêm migration |
| **Go Toolchain** | `go version go1.27.2 linux/amd64` | CGO_ENABLED=0 |
| **Python** | `Python 3.13.5` | Chạy bộ test harness và test runner |

---

## 3. Entrypoint và Các Kịch bản Kiểm thử Tích hợp

Bộ kiểm thử tích hợp được điều khiển thông qua entrypoint `scripts/test-stage4-api.sh` (gọi `scripts/tests/stage4_api_runner.py`), hỗ trợ 4 selector:
- `--scenario vertical`: Thực thi Bước 4.7.3 — Vertical slice từ MQTT commit đến HTTP read, PATCH metadata và ingest tiếp tục.
- `--scenario contract`: Thực thi Bước 4.7.4 — HTTP contract, boundaries, và ma trận phân quyền RBAC.
- `--scenario queries`: Thực thi Bước 4.7.5 — Đo lường query plans trên dataset chưa nén và sau khi nén hypertable.
- `--scenario all` (mặc định): Thực thi tuần tự cả 3 kịch bản bắt buộc trên.
- Cờ phụ trợ: `--lifecycle-only` xác minh vòng đời khởi tạo và dọn dẹp fixture; `--require-run` chống false-PASS khi thiếu docker; `--report <path>` xuất summary.

### 3.1 Kết quả Kịch bản Vertical Slice (4.7.3)
1. **Khởi tạo thực thể (Provisioning):**
   - Admin gọi `POST /v1/digital-twins` tạo Gateway A: `201 Created` (`urn:ngsi-ld:Gateway:gw_vert_...`).
   - Owner gọi `POST /v1/digital-twins` tạo Sensor A thuộc Gateway A: `201 Created` (`urn:ngsi-ld:Sensor:gw_vert_...:sens_vert_...`).
   - Gọi lại exact replay: trả về `200 OK` với cùng cấu trúc URN.
   - Tạo Gateway B và Sensor B làm đối chứng cô lập (positive control).
2. **Uplink Ingestion và Application ACK:**
   - Simulator Gateway A gửi 2 batches $\times$ 100 samples qua MQTT TLS port 8883.
    - Nhận application-level ACK trên topic `gateways/<gateway_id>/acks/<message_id>`, outbox SQLite giảm về 0.
   - Kiểm tra CSDL: Bảng `telemetry` có 200 rows, `processed_messages` có 2 rows, `twin_temporal_values` có 2 observations interval.
   - Bảng `twin_states` cập nhật: `reported_version = 2`, `reported_state` chứa `sample_interval_us = 10000.0`. Các giá trị `desired_state`, `desired_version` (0) và thông tin Gateway được giữ nguyên.
3. **Đọc dữ liệu HTTP theo quyền:**
   - Owner, Operator, Viewer đọc thành công:
     * `GET /v1/telemetry/history` (raw window ngắn $\le$ 10s): đúng 200 điểm đo, giá trị khớp công thức deterministic `synthetic_value`.
      * `GET /v1/telemetry/history` (bucket): response thành công, mode bucket và points không rỗng. Vertical không trực tiếp assert min/max/avg; kiểm arithmetic riêng ở queries M2/M5 và native tests được dẫn trong H06.
     * `GET /v1/digital-twins`: danh sách hiển thị đúng Gateway A và Sensor A.
     * `GET /v1/digital-twins/{entity_id}`: trả về `application/ld+json`, `@context` nội tuyến và quan hệ `hasSensor`.
     * `GET /v1/digital-twins/{entity_id}/state`: `reported_version = 2`.
     * `GET /v1/digital-twins/{entity_id}/history?property=value`: 200 điểm đo ủy quyền sang telemetry history.
     * `GET /v1/digital-twins/{entity_id}/history?property=sample_interval_us`: 2 điểm đo với unit đọc từ metadata temporal (`us`).
   - **Cô lập người dùng (Isolation):** User B (chủ Gateway B) đọc tài nguyên của Gateway A nhận `404 Not Found`; danh sách `GET /v1/digital-twins` của User B không chứa Gateway A.
4. **Metadata PATCH & Bất biến:**
    - Sau sửa review, snapshot full rows scoped theo hai Gateway fixture cho state/timestamps, raw/temporal, ledger, metadata, relationships, memberships và bảng control/credential liên quan; chỉ dùng dataset vertical nhỏ, không snapshot workload 60.000 mẫu.
   - Owner gọi `PATCH /v1/digital-twins/{sensor_urn}` đổi tên Sensor: `200 OK`.
   - Admin gọi `PATCH /v1/digital-twins/{gw_urn}` cập nhật mô tả Gateway: `200 OK`.
    - Các fields được phép đổi là `sensors.name`, Sensor `twin_entities.name`, `gateways.description` và `twin_entities.updated_at` của hai entity được PATCH. Comparator mới so toàn bộ nội dung còn lại theo fixture scope, bao gồm từng sample/observation và state timestamps; không claim toàn database không đổi. Checkpoint trước review chỉ so history counts, chưa đủ chứng minh same-cardinality content preservation.
   - Admin (không thuộc `user_gateways`) sau khi PATCH vẫn bị từ chối `404 Not Found` khi gọi `GET /state` và `GET /history`.
5. **Ingest tiếp tục và Bỏ qua tin trùng lặp (Deduplication):**
   - Simulator Gateway A gửi tiếp 1 batch $\times$ 100 samples: tổng số mẫu đạt đúng 300 raw, 3 ledger, 3 temporal observations; `reported_version` tiến lên 3; tên Sensor và mô tả Gateway mới vẫn được bảo toàn.
   - Simulator gửi lại chính xác byte-exact một bản tin cũ: backend nhận diện dedup qua `(gateway_id, message_id)`, trả về ACK `status: committed`; số lượng hàng trong CSDL không tăng thêm.

### 3.2 Kết quả Kịch bản HTTP Contract & Phân quyền (4.7.4)
- **Xác thực JWT:** Token thiếu, token sai chữ ký, hoặc token hết hạn đều bị từ chối ngay với `401 Unauthorized` và mã lỗi `'unauthorized'`.
- **Từ chối sửa đổi phi quyền (RBAC Denials):**
  * Owner gọi `PATCH Gateway`: `403 Forbidden` (chỉ Platform Admin mới có quyền sửa Gateway).
  * Operator gọi `POST Sensor`, `PATCH Sensor`, `PATCH Gateway`: đều nhận `403 Forbidden`.
  * Viewer gọi `POST Sensor`, `PATCH Sensor`, `PATCH Gateway`: đều nhận `403 Forbidden`.
   * Non-member PATCH Sensor: `404 Not Found`; PATCH Gateway: `403 Forbidden` (admin-only).
- **Snapshot Immutability sau Denials:** Snapshot mới capture full rows scoped theo Gateway, gồm mọi Sensor/entity của Gateway để bắt denied POST tạo thêm tài nguyên, cùng state/timestamps/raw/temporal/control rows. Không chỉ so count; diagnostic chỉ nêu tên nhóm thay đổi, không dump nội dung rows.
- **Ranh giới tham số và cấu trúc lỗi:**
  * Raw window vượt giới hạn (`> 10s`): trả về `400 Bad Request` với mã `'raw_limit_exceeded'`. Khi dùng `resolution=auto`, hệ thống fallback thành công về bucket `100ms` (`200 OK`).
  * Tham số thời gian sai (`from > to`, khoảng cách `> 744h`), độ phân giải không hợp lệ, property không hỗ trợ: `400 Bad Request`.
  * URL-encoded URN: single-encoded hợp lệ (`200 OK`), double-encoded bị từ chối (`400 Bad Request`).
  * Mutation body: cấm sửa trường không thuộc allowlist (`unit` trên Sensor, `owner_user_id` trên Gateway), cấm duplicate keys, cấm query string trên route mutation: `400 Bad Request`.
  * Sai Content-Type (gửi `text/plain`): `415 Unsupported Media Type`. Payload `> 65536 bytes`: `413 Payload Too Large`.
  * Header bắt buộc: Mọi phản hồi đều chứa `Cache-Control: no-store`; route mutation có thêm `Pragma: no-cache`. Lỗi trả về luôn có envelope chuẩn gồm `code`, `message`, và `request_id`.

---

## 4. Bảng Kết quả Đo lường Truy vấn (checkpoint trước review)

Các phép đo được thực hiện trên dataset chuẩn gồm:
- **Target Gateway/Sensor:** 60.000 raw samples @ 100 Hz (khoảng thời gian 10 phút) + 580 temporal interval observations (tần suất 1/s, gap 20s tại `[300s, 320s)`).
- **Distractor Gateway/Sensor:** 6.000 raw samples @ 10 Hz + 60 temporal interval observations.
- Phép đo so sánh giữa trạng thái **Chưa nén (Uncompressed)** và **Đã nén (Compressed)** sau khi gọi `compress_chunk` trên các chunks mục tiêu (`_hyper_1_1_chunk` và `_hyper_2_2_chunk`).

| Mã | Loại truy vấn | Cấu hình & Phạm vi | Uncompressed Plan / Exec / Hits | Compressed Plan / Exec / Hits | Service Elapsed (Unc / Comp) | HTTP Elapsed (Unc / Comp) | Nodes chính (Compressed) |
|---|---|---|---|---|---|---|---|
| **M1** | Raw Telemetry | 5s window, 500 samples @ 100Hz, cap=500 | 3.395ms / 0.601ms / 32 hits | 5.510ms / 0.805ms / 57 hits | 1.865ms / 2.038ms | 14.084ms / 6.426ms | Index Scan, Custom Scan |
| **M2** | Bucket Telemetry | 10m window, auto width 10s $\rightarrow$ 60 buckets | 0.972ms / 56.457ms / 2101 hits | 1.154ms / 48.156ms / 418 hits | 46.284ms / 40.141ms | 50.659ms / 41.701ms | Custom Scan, Aggregate |
| **M3** | Overflow Fallback | 8s probe (800 samples $\rightarrow$ 501 limit) $\rightarrow$ fallback 100ms (80 buckets) | 0.696ms plan / 1.595ms exec | 1.120ms plan / 1.797ms exec | 3.282ms / 4.030ms | 3.969ms / 4.088ms | Probe Index Scan $\rightarrow$ Fallback Custom Scan |
| **M4** | Raw Temporal | 10s window, 10 observations, cap=500 | 4.730ms / 0.588ms / 32 hits | 7.136ms / 1.336ms / 68 hits | 2.857ms / 3.143ms | 8.096ms / 5.517ms | Custom Scan, Hash Join |
| **M5** | Bucket Temporal | 10m window, 1m resolution $\rightarrow$ 10 buckets (gap verified: bucket 5 = 40) | 1.589ms / 1.431ms / 50 hits | 2.122ms / 1.746ms / 26 hits | 3.951ms / 4.244ms | 4.877ms / 4.550ms | Custom Scan, Aggregate |

### Đánh giá Kết quả và Quyết định Schema:
1. **Hiệu quả nén:** Truy vấn M2 (bucket tổng hợp trên 60.000 mẫu) giảm số buffer hits từ 2.101 xuống còn 418 blocks (~80% reduction in hit blocks) và thời gian thực thi giảm từ 56.45ms xuống 48.15ms nhờ cơ chế TimescaleDB Custom Scan đọc trực tiếp dữ liệu nén theo cột.
2. **Tuân thủ ngân sách:** Cả 5 kịch bản truy vấn (cả chưa nén và đã nén) đều hoàn thành dưới 60ms ở tầng CSDL và dưới 55ms ở tầng HTTP, nằm sâu bên trong ngân sách timeout 5.0s được cấu hình.
3. **Quyết định Schema:** **Giữ nguyên Schema 20 và hệ thống chỉ mục hiện tại**. Không cần bổ sung speculative index, không tạo continuous aggregate hay thay đổi cấu hình nén.

---

## 5. Bảng Đối Chiếu Ma Trận Nghiệm Thu H01–H15

Bảng dưới phân định nguồn gốc bằng chứng: **ACTUAL / NEW** (kịch bản mới chạy trong Task 4.7), **INHERITED** (kế thừa từ unit/native test có sẵn của Tasks 4.1–4.6), hoặc **NOT RUN** (nêu rõ lý do kỹ thuật).

| Mã | Nội dung Nghiệm thu | Tầng Kiểm thử & Test Case Cụ thể | Trạng thái Thực tế | Giới hạn & Ghi chú Kỹ thuật |
|:---:|---|---|:---:|---|
| **H01** | Read thiếu/sai/expired JWT, user/role injection | NEW HTTP: `run_contract_scenario` auth cases | Local PASS trước review | JWT fixture, không phải full login stack |
| **H02** | Hai Gateway cùng Sensor ID, tenant isolation, admin không đọc global | NEW HTTP: `run_vertical_scenario` cross-tenant và admin non-member reads | Local PASS trước review | List lọc membership; không gọi mọi route đều 404 |
| **H03** | Owner/operator/viewer read, non-member/missing404, authorized-empty200 | NEW HTTP: vertical role reads và contract empty cases | Local PASS trước review | Chỉ các requests/assertions trong scenarios |
| **H04** | Raw `[from,to)`, microseconds, ties qua boot, legacy NULL provenance | NEW HTTP M1; NEW native upgrade `TestNativeTelemetryHistoryUpgradedLegacyProvenance` qua `test-stage3-persistence.sh`; INHERITED `TestNativeTelemetryHistoryIntervalBoundariesAndOrdering`, `TestReadTelemetryHistory_TwoBootsEqualTimestampsRetained` | Mixed evidence | Legacy NULL raw native mới chạy; HTTP delegation legacy **NOT RUN**. NULL là boot_id/message_id/sample_index, không phải recorded_at |
| **H05** | Raw overflow/range/query sai, bounded fallback, không false success khi timeout | NEW HTTP contract/M3; INHERITED `TestTelemetryHistoryService_GetTelemetryHistory`, `TestPrepareTelemetryHistory_CanceledOrExpiredNilSuccessRejected` | Mixed evidence | M3 probe vượt cap500 → bucket100ms; timeout không suy từ happy path |
| **H06** | Bucket arithmetic, clipping/gap/resolution/cap | NEW HTTP M2/M3/M5; INHERITED `TestNativeTelemetryHistoryBucketArithmeticAndClipping` | Mixed evidence | Nén đúng chunks fixture, không đại diện mọi window744h |
| **H07** | List pagination/URN/relationship pages/context | NEW HTTP contract pagination và vertical graph; INHERITED `TestNativeTwinRead` | Mixed evidence | Relation type/object/context được assert |
| **H08** | Current state/mapper, JSON types và reported/desired/version | NEW HTTP vertical state; INHERITED `TestNativeTwinRead`, `TestTwinStateResponseDTO` | Mixed evidence | SQL state đọc trực tiếp; >2^53 và typed branches kế thừa, không claim native mới |
| **H09** | Temporal interval type/unit/boot/time/message và value delegation | NEW HTTP vertical/M4/M5; INHERITED `TestNativeTemporalHistory_NumericSlice`, `TestNewTwinHistoryResponseDTO_DelegatedValueRaw` | Mixed evidence | Compression comparator mới so field thật; legacy NULL qua Twin HTTP **NOT RUN** |
| **H10** | POST Gateway admin, Sensor admin/owner, atomic graph/replay/conflict/no credentials | NEW HTTP vertical/contract; INHERITED `TestNativeEntityMetadata_MutationSlice` | Mixed evidence | Transaction internals không được suy từ status HTTP đơn lẻ |
| **H11** | Forbidden metadata writes, không side effects, body injection | NEW HTTP contract denials; full-row snapshot sau sửa | Native rerun PASS | Snapshot scoped bắt thêm Sensor/entities và nội dung cùng count; không dump rows |
| **H12** | PATCH metadata/rollback/invariants, ingest tiếp | NEW HTTP vertical PATCH/ingest; INHERITED `TestNativeEntityMetadata_MutationSlice` SQL rollback/concurrency | Mixed evidence | Full-content preservation native rerun PASS; SQL rollback/concurrency native vẫn là lịch sử Task4.5 |
| **H13** | DB timeout/cancel/release và query/body/cursor bounds | NEW HTTP contract bounds; INHERITED `TestPrepareTelemetryHistory_CanceledOrExpiredNilSuccessRejected`, `TestNativeTemporalHistory_NumericSlice` acquire-pool case | Mixed evidence | Không mới chạy native acquire-pool; executing-query cancellation **NOT RUN** |
| **H14** | MQTT commit ACK → HTTP data đúng quyền | NEW `run_vertical_scenario`, independent SQL + outbox drain + resend | Native rerun sau sửa PASS | A300 raw/3ledger/3temporal, version3, dedup không thêm rows |
| **H15** | Native/cleanup/regressions/CI và contract client | Native rerun PASS; CI wired; follow-up review APPROVE cho sáu findings | Chưa close toàn Stage4 | Remote CI **NOT RUN**, client/docs closeout thuộc Task4.8 |

---

## 6. Trạng thái CI Pipeline và Hồi quy (Regressions Status)

1. **Kết nối CI (`.github/workflows/ci.yml`):**
    - Lane `stage4-api-integration` trên `ubuntu-latest` thực hiện preflight, build từ `src/Dockerfile`, chạy `scripts/test-stage4-api.sh --scenario all` với `MANDATORY_CI=1` và biến `STAGE4_API_REPORT` trỏ tới file trong `runner.temp` (không hard-code `/tmp/stage4-summary.json`).
   - Bổ sung kiểm tra guard `test_ci_feature_jobs.py` xác minh hợp đồng job bằng job ID (tránh lỗi display name).
    - Upload artifact an toàn `stage4-api-safe-report` từ đường dẫn `${{ runner.temp }}/stage4-api-${{ github.run_id }}-${{ github.run_attempt }}.json`, không upload env/token/DSN/private keys.
2. **Kết quả Kiểm tra Hồi quy Cục bộ (Local Verification Checkpoint):**
   - Bộ runner tích hợp Stage 4: `scripts/test-stage4-api.sh --scenario all` **PASS** (bao gồm `vertical`, `contract`, `queries`).
   - Format, Lint, Build & Race: `gofmt`, `go vet ./...`, `golangci-lint run ./...` (**0 issues**), `go build ./...`, `go test -race ./...` **PASS**.
   - Python Test Suite: Toàn bộ **852 tests PASS** (`unittest discover -s scripts/tests`).
   - CSDL & Migrations: `stage3-persistence` (kiểm tra upgrade legacy schema) **PASS**; Stage 2 suites (`test-stage2-authorization.sh`, `test-stage2-provisioning.sh`, `stage2_auth.py`, `backend_smoke`) **PASS**.
3. **Phân định rõ các kiểm thử KHÔNG THỰC HIỆN (NOT RUN):**
   - **Remote CI trên GitHub Actions:** **NOT RUN** (chờ push commit lên remote repository theo phân quyền dự án).
   - **Không đo lại Workload 600 batches Stage 3:** Không chạy lại vì đường dẫn thu thập dữ liệu (ingestion core) không bị thay đổi trong Task 4.7.
   - **Hủy bỏ truy vấn đang thực thi (Executing Query Cancellation):** Chưa kiểm chứng ngắt câu lệnh đang chạy ở máy chủ CSDL (bằng chứng hiện có chỉ kiểm chứng việc hết timeout khi chờ mượn connection từ pool).
   - **Triển khai Production / Bo mạch ARM / Soak Testing:** Không thuộc phạm vi MVP Stage 4.

---

## 7. Ranh giới Tồn đọng và Bàn giao Task 4.8

1. **Reconciliation Task 4.6:** Báo cáo Task 4.6 có đoạn văn ghi nhận giao CI và nén hypertable cho Task 4.8. Đính chính chính thức: **Hai hạng mục này thuộc phạm vi Task 4.7 và đã được hoàn thành đầy đủ trong Task 4.7**. Task 4.8 chỉ đảm nhận việc lập tài liệu tổng kết Stage 4 (`docs/backend/stage-4-history-and-twin-api.md`), cập nhật tài liệu hợp đồng client (contracts 04, 05, 07, 08), và chuẩn bị bàn giao Giai đoạn 5 (WebSocket & Realtime).
2. **Review Status:** Review trước BLOCK (1 HIGH + 5 MEDIUM); follow-up reviewer đã trả APPROVE và đóng cả sáu findings. Hai LOW về prose đã chỉnh sau review; không suy thành nghiệm thu toàn Stage4 hay production.
3. **Bàn giao cho Task 4.8:**
   - 7 HTTP endpoints đã sẵn sàng và được kiểm chứng tích hợp hoàn chỉnh.
   - CSDL Schema 20 giữ nguyên không đổi.
   - Bảng số đo truy vấn và kết quả nén làm căn cứ tham chiếu cho tài liệu tổng kết.
    - Artifact tóm tắt non-secret lưu tại `docs/backend/stage-4-task-4.7-summary.json`.

## 8. Kiểm chứng sau sửa sáu findings

- HIGH: Không quyết định exit/report/PASS trong `finally`; SIGINT/SIGTERM mang trạng thái interrupted, cleanup vẫn chạy. Regression mô phỏng signal ở build/setup/scenario không dùng services; có kiểm partial fixture cleanup.
- Cleanup image: chỉ gỡ tag suite, không `rm -f` image ID dùng chung. Image ID còn dùng cho provenance; key/marker lịch sử `suite_images_removed` nghĩa là owned tag references đã gỡ, không khẳng định shared image content đã bị xóa.
- Gateway timeout: đăng ký tên container trước khi gọi Docker, cleanup trong `finally`, fixture registry retry nếu cần; regression `TimeoutExpired` giữ FAIL và dọn đúng name. Shared Stage3 helper chỉ thêm optional `container_name`, mặc định cũ không đổi.
- Compression: comparator kiểm field presence (missing khác null), response unit và full DTO points; raw provenance `boot_id/message_id/sample_index`, temporal `value_type/message_id/metadata`, timestamps/values/counts. Temporal raw ties so multiset; không claim dataset mới là legacy NULL.
- Snapshot: full rows scoped các Gateway fixture thay vì chỉ counts; regression bắt sample đổi cùng count và thêm Sensor ngoài ý muốn, vẫn chấp nhận đúng metadata allowlist.
- Báo cáo: H01–H15 giữ nghĩa trong `stage4_backend_detailed_plan.md`; tên test thật, metadata image thật, status403/404 và ACK topic đã đối soát.

**Native mới chạy:** `sh scripts/test-stage4-api.sh --scenario all --require-run --report /tmp/opencode/stage4-api-six-fixes.json` **PASS**, cả vertical/contract/queries, hai chunks compressed, database query-fixture rows về0 và toàn fixture cleanup verified. Backend `sha256:9e6acf90d81657c3d76c7740cd35140f9c8f1e96896249478154225db92cf99a`; Gateway `sha256:58fe698a9e5e6586dd8c2b7c8017e2095e270be2e728ad4c3356edad44a48d09`. Bảng môi trường và mục4 phía trên là checkpoint cũ, summary JSON hiện là run sau sửa.

**Hồi quy mới chạy:** Python `unittest discover -s scripts/tests` **860 tests PASS**; `go test -race -count=1 ./internal/httpserver ./cmd/server ./internal/telemetry ./internal/digitaltwin` **PASS**; `git diff --check` **PASS**. Full Go/static và Stage2/3 native checkpoint mục6 là kế thừa, không báo chạy lại trong lượt sửa Python/docs này. Reviewer tự chạy 119 targeted Python tests, vet/build-race/lint và root-buffer tests PASS; native rerun do parent chạy, reviewer không chạy lại DB. Chưa commit/push, remote CI NOT RUN.
