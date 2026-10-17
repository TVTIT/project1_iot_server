# Contract đọc lịch sử và quản lý entity Stage 4

> **Trạng thái: Task 4.0 COMPLETE / Stage 4 API DESIGN ONLY — các endpoint Stage 4 CHƯA được triển khai.**
> Tài liệu này đã hoàn thành toàn bộ **Task 4.0** (bao gồm **Task 4.0.1** baseline/đối
> chiếu nguồn, **Task 4.0.2** danh sách endpoint/identity/quyền — mục 7, **Task 4.0.3**
> telemetry/temporal history — mục 8, **Task 4.0.4** Twin list/detail/state + cursor
> keyset — mục 9, **Task 4.0.5** DTO POST/PATCH metadata — mục 10, **Task 4.0.6**
> mapper representation/context versioned/HTTP error dùng chung — mục 11, và
> **Task 4.0.7** rà soát tính đầy đủ, sửa đổi selector theo kế hoạch và bàn giao — mục 11.8).
> Đây **không** phải bằng chứng rằng bất kỳ endpoint nào trong bảy endpoint
> (`GET /v1/telemetry/history`, `GET /v1/digital-twins`,
> `GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}/state`,
> `GET /v1/digital-twins/{entity_id}/history`, `POST /v1/digital-twins`,
> `PATCH /v1/digital-twins/{entity_id}`) đã hoạt động trên runtime. Hai route
> `GET /v1/telemetry/history` và `GET /v1/digital-twins` hiện vẫn là stub
> `501 not_implemented` trong `router.go`; năm route còn lại chưa được đăng ký.
> CI xanh của Stage 3 (`2ce3a89`, run 38891798340) **không** phải chứng nhận cho
> API Stage 4. Mục 7–11 là contract **đã hoàn tất chốt thiết kế để implement**;
> Task 4.1–4.8 chưa được triển khai.

**Ngày lập:** 16/10/2026 (hoàn tất Task 4.0: 17/10/2026).

**Phạm vi đã hoàn tất:** Toàn bộ Task 4.0.1 – 4.0.7 theo đúng kế hoạch chi tiết.
Đã phục hồi selector API HTTP `value` và `sample_interval_us` cho Twin history
theo đúng Stage 4 plan; mapper representation JSON-LD giữ public terms `measurement`
và `sampleIntervalUs` với bảng ánh xạ tường minh; đã kiểm tra và xác thực toàn bộ
JSON fences, cursor schemas, bounds, links và whitespace.

Các đường dẫn source trong dấu backtick tính từ gốc `main_src/`. Liên kết Markdown
tính từ vị trí tài liệu này.

---

## 1. Baseline

### 1.1. Git revision và worktree tại thời điểm khảo sát

Ghi nhận thực tế lúc bắt đầu (lệnh `git rev-parse`/`git status`):

```text
branch:  develop
HEAD:    2ce3a89ebd6b95f46e17cd535c5400f2ba5ca6bd
worktree: clean (không có thay đổi chưa commit, không file untracked)
upstream: up to date with 'origin/develop'
HEAD subject: test(ci): định danh job bằng yaml id thay vì tên hiển thị name
```

Revision này **trùng** mốc khảo sát của [Task 4.0 detail plan](../../../docs/backend_plan/task_4.0_detail_plan.md)
và [Stage 4 plan](../../../docs/backend_plan/stage4_backend_detailed_plan.md). Đây là
điểm khởi đầu; khi triển khai Task 4.1+ phải ghi lại revision/worktree thực tế, không
giả định HEAD giữ nguyên.

### 1.2. Bằng chứng CI (lịch sử, không phải Stage 4)

- Ghi từ [AGENTS.md](../../AGENTS.md): GitHub Actions run **38891798340** tại commit
  `2ce3a89ebd6b95f46e17cd535c5400f2ba5ca6bd` (viết ngắn `2ce3a89`), cả **15 jobs** hoàn thành thành công, bao gồm
  `stage3-uplink-integration` và `native-compose-smoke`.
  Run URL: <https://github.com/TVTIT/project1_iot_server/actions/runs/38891798340>.
- Run trước đó `38890958598` từng fail do workflow contract test phụ thuộc tên hiển
  thị job đã bị xoá; commit `2ce3a89` sửa cách định danh theo job id và run sau pass.
- Đây là **baseline lịch sử của Stage 3**. Tài liệu này **không** tự truy vấn lại
  GitHub và **không** gán PASS cho revision Stage 4. Nếu HEAD đổi, giữ run này làm mốc
  lịch sử, không suy diễn API Stage 4 đã được test.

### 1.3. Schema, DB role, RLS và retention

- Schema hiện hành: **terminal migration 20** — `20:stage3_telemetry_ingestion`
  (`migrations/000020_stage3_telemetry_ingestion.up.sql`). Migration tracking nằm ở
  bảng `schema_migrations` (tạo ở `migrations/000009_...`).
- PostgreSQL 16.15 / TimescaleDB 2.29.2 (ghim, theo AGENTS/Stage 4 plan). Các migration
  tạo hypertable cho `telemetry` và `twin_temporal_values`.
- **Hai role tách biệt** (provision ở `migrations/000000_configure_supabase_roles.sh`):
  - `iot_backend`: NOLOGIN, `BYPASSRLS`, có quyền DML nghiệp vụ.
  - `iot_backend_app`: LOGIN, `INHERIT`/member của `iot_backend`, được cấp `BYPASSRLS`
    **tường minh** (thuộc tính này không tự kế thừa qua role membership). Ứng dụng Go
    kết nối bằng `iot_backend_app` (xem `DATABASE_URL` trong `.env.example`; giá trị
    thật không được ghi vào tài liệu).
  - **Hệ quả:** vì app role có `BYPASSRLS`, các RLS policy chỉ áp dụng cho truy cập
    trực tiếp qua PostgREST/Supabase (`authenticated`, `anon`, `service_role`). Go
    **bắt buộc** tự scope quyền người dùng bằng `user_gateways` trong SQL; không được
    dựa vào RLS làm quyền người dùng.
- RLS đã được bật trên: `profiles`, `user_gateways`, `gateways`, `sensors`,
  `media_objects` (`migrations/000004_supabase_compat.up.sql`) và `platform_admins`,
  `gateway_mqtt_credentials`, `gateway_mqtt_credential_events`
  (`migrations/000010_stage2_auth_and_provisioning.up.sql`).
- **Retention đang TẮT:** migration 1 và 2 có `add_retention_policy(... INTERVAL '30 days')`,
  nhưng `migrations/000008_schema_hardening.up.sql` đã gọi `remove_retention_policy`
  cho cả `telemetry` và `twin_temporal_values`. Compression policy 7 ngày **vẫn giữ**
  (segment theo `gateway_id, sensor_id` / `entity_id, property_name`).
- Ngữ nghĩa commit: `Persist` commit thành công xác nhận transaction, **không** phải
  bằng chứng fsync/phần cứng hay chống mất điện
  (`src/internal/telemetry/postgres_repository.go`).

### 1.4. Quy ước HTTP hiện có (điểm nối Stage 4 phải theo)

- Router: Gin, `src/internal/httpserver/router.go`, `HandleMethodNotAllowed = true`.
  Middleware dùng chung: `httpapi.RequestIDMiddleware`, CORS, `httpapi.RecoveryMiddleware`,
  cùng cache/audit middleware cho route admin/owner.
  - `NoRoute` → `404 not_found`; `NoMethod` → `405 method_not_allowed`.
  - `/healthz`, `/readyz`, `/v1/health` không xác thực.
- Error envelope ổn định: `{"error":{"code","message","request_id"}}`
  (`src/internal/httpapi/error.go`). `401` kèm header `WWW-Authenticate: Bearer`.
- Protected response đặt `Cache-Control: no-store` (+ `Pragma: no-cache` ở route
  admin/owner).
- CORS **exact-origin** (`src/internal/httpserver/cors.go`): allowlist origin chính xác,
  preflight chỉ chấp nhận method/header trong allowlist (`authorization`, `content-type`,
  `idempotency-key`, `x-request-id`), expose `X-Request-ID`.
- Principal JWT: `src/internal/auth/principal.go` (`Principal{UserID, Role}`),
  `src/internal/auth/verifier.go` yêu cầu claim `role == "authenticated"`.
  `src/internal/auth/admin_middleware.go` kiểm platform admin qua DB
  (`platform_admins`), không nhận role từ client.
- Read list hiện có (`src/internal/httpserver/gateway_handlers.go`,
  `sensor_handlers.go`): trả `{"items":[...]}`, luôn `Cache-Control: no-store`, và
  **từ chối** query param `user_id` (`400`) — không nhận identity từ query.

### 1.5. Route hiện có (đọc trực tiếp từ router, không sao chép ví dụ)

| Method / route | Trạng thái hiện tại | Ghi chú |
| --- | --- | --- |
| `GET /v1/admin/gateways` | Đã có | Platform admin inventory, keyset `after`/`limit` (default 100, max 200). |
| `PUT /v1/admin/gateways/:gateway_id` | Đã có | Admin provision Gateway (create-or-exact-replay). |
| `PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id` | Đã có | Admin provision Sensor. |
| `PUT /v1/gateways/:gateway_id/sensors/:sensor_id` | Đã có | Owner provision Sensor. |
| `GET /v1/me/permissions` | Đã có | Trả quyền của principal. |
| `GET /v1/gateways` | Đã có | List Gateway theo membership của user. |
| `GET /v1/gateways/:gateway_id/sensors` | Đã có | List Sensor theo gateway + membership. |
| `GET /v1/telemetry/history` | **501 stub** | `notImplemented`. |
| `GET /v1/ws` | **501 stub** | WebSocket chưa triển khai. |
| `GET /v1/digital-twins` | **501 stub** | Twin list chưa triển khai. |

Các route Stage 4 (`/v1/digital-twins/{entity_id}`, `/state`, `/history`, `POST`, `PATCH`)
**chưa được đăng ký** trong `router.go`.

---

## 2. Bảng source-of-truth theo nhóm response

Task 4.0.1 (bước 4) liệt kê **chín** bảng: `gateways`, `sensors`, `user_gateways`,
`platform_admins`, `twin_entities`, `twin_relationships`, `twin_states`, `telemetry`,
`twin_temporal_values`. Bổ sung `profiles` làm bảng thứ mười vì POST Gateway yêu cầu
owner profile tồn tại và cả `platform_admins` lẫn `user_gateways` đều tham chiếu
`profiles.id`. Mọi chi tiết dưới đây đọc từ **migration files trong Git** (không phải
catalog runtime — xem mục 6).

Nguồn chính: `migrations/000001_init_schema.up.sql`, `migrations/000002_digital_twin_tables.up.sql`,
`migrations/000004_supabase_compat.up.sql`, `migrations/000006_add_gateway_ownership_columns.up.sql`,
`migrations/000007_backfill_twin_gateway_ownership.up.sql`, `migrations/000008_schema_hardening.up.sql`,
`migrations/000009_migration_tracking_and_sensor_urn.up.sql`,
`migrations/000010_stage2_auth_and_provisioning.up.sql`,
`migrations/000020_stage3_telemetry_ingestion.up.sql`.

### 2.1. `profiles`

Nguồn: migration 1 (+ FK `auth.users` ở migration 4).

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `id` | UUID | NOT NULL | PK; FK `profiles.id → auth.users.id ON DELETE CASCADE` (migration 4). |
| `full_name` | TEXT | NULL | |
| `created_at` | TIMESTAMPTZ | NULL | `DEFAULT now()`. |

RLS bật (migration 4). `platform_admins.user_id` và `user_gateways.user_id` tham chiếu
`profiles.id`.

### 2.2. `gateways`

Nguồn: migration 1; constraint identifier ở migration 10.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `gateway_id` | TEXT | NOT NULL | **PK**. |
| `name` | TEXT | NOT NULL | |
| `description` | TEXT | NULL | |
| `created_at` | TIMESTAMPTZ | NULL | `DEFAULT now()` (không `NOT NULL`). |

Constraint `chk_gateways_identifier`: `gateway_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'`
AND `gateway_id <> 'backend_service'`. RLS bật (migration 4).

### 2.3. `sensors`

Nguồn: migration 1; constraint identifier ở migration 10.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `sensor_id` | TEXT | NOT NULL | PK ghép (phần 2). |
| `gateway_id` | TEXT | NOT NULL | FK `gateways(gateway_id) ON DELETE CASCADE`; phần 1 của PK (PK suy ra NOT NULL). |
| `name` | TEXT | NOT NULL | |
| `unit` | TEXT | NULL | |
| `created_at` | TIMESTAMPTZ | NULL | `DEFAULT now()`. |

**PK = `(gateway_id, sensor_id)`.** Constraint `chk_sensors_identifier`:
`sensor_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'`. **Không có cột `sensors.id`** — Sensor
chỉ được định danh trong phạm vi Gateway cha. RLS bật (migration 4).

### 2.4. `user_gateways`

Nguồn: migration 1; check role ở migration 8; index ở migration 10.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `user_id` | UUID | NOT NULL | FK `profiles(id) ON DELETE CASCADE`. |
| `gateway_id` | TEXT | NOT NULL | FK `gateways(gateway_id) ON DELETE CASCADE`. |
| `role` | TEXT | NOT NULL | `DEFAULT 'viewer'`; check `role IN ('owner','operator','viewer')`. |
| `created_at` | TIMESTAMPTZ | NULL | `DEFAULT now()`. |

**PK = `(user_id, gateway_id)`.** Index `idx_user_gateways_gateway_user
(gateway_id, user_id)`. RLS bật (migration 4). Đây là bảng scope quyền chính cho mọi
read API Stage 4.

### 2.5. `platform_admins`

Nguồn: migration 10.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `user_id` | UUID | NOT NULL | **PK**; FK `profiles(id) ON DELETE CASCADE`. |
| `granted_at` | TIMESTAMPTZ | NOT NULL | `DEFAULT now()`. |
| `granted_by` | UUID | NULL | FK `profiles(id) ON DELETE SET NULL`. |

Index `idx_platform_admins_granted_by (granted_by) WHERE granted_by IS NOT NULL`.
RLS bật (migration 10).

### 2.6. `twin_entities`

Nguồn: migration 2; `gateway_id` migration 6/7/8; index migration 8.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `id` | UUID | NOT NULL | **PK**, `DEFAULT gen_random_uuid()`. Không lộ ra ngoài. |
| `entity_id` | TEXT | NOT NULL | **UNIQUE**; URN public ổn định. |
| `entity_type` | TEXT | NOT NULL | `Gateway`/`Sensor` (migration 2 ghi chú thêm `Device`). |
| `name` | TEXT | NULL | |
| `attributes` | JSONB | NOT NULL | `DEFAULT '{}'`. |
| `created_at` | TIMESTAMPTZ | NOT NULL | `DEFAULT now()`. |
| `updated_at` | TIMESTAMPTZ | NOT NULL | `DEFAULT now()`. |
| `gateway_id` | TEXT | NOT NULL | FK `gateways(gateway_id) ON DELETE RESTRICT` (thêm nullable ở m6, backfill m7, `NOT NULL` + FK ở m8). |

Index: `idx_twin_entities_type (entity_type)`, `idx_twin_entities_gateway_type
(gateway_id, entity_type)`. Index unique của `entity_id` là key cho keyset list.

### 2.7. `twin_relationships`

Nguồn: migration 2.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `source_entity_id` | UUID | NOT NULL | FK `twin_entities(id) ON DELETE CASCADE`. |
| `relationship_type` | TEXT | NOT NULL | Ví dụ `hasSensor`. |
| `target_entity_id` | UUID | NOT NULL | FK `twin_entities(id) ON DELETE CASCADE`. |
| `created_at` | TIMESTAMPTZ | NOT NULL | `DEFAULT now()`. |

**PK = `(source_entity_id, relationship_type, target_entity_id)`.** Index
`idx_twin_relationships_target (target_entity_id, relationship_type)`.
`hasSensor` được tạo ở provisioning Sensor (migration/repo,
`src/internal/gateway/postgres_sensor_provisioning.go`) và được xác minh khi resolve
telemetry (`src/internal/telemetry/postgres_resources.go`).

### 2.8. `twin_states`

Nguồn: migration 2; `last_desired_by` migration 6/8.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `entity_id` | UUID | NOT NULL | **PK**; FK `twin_entities(id) ON DELETE CASCADE`. |
| `reported_state` | JSONB | NOT NULL | `DEFAULT '{}'`. |
| `desired_state` | JSONB | NOT NULL | `DEFAULT '{}'`. |
| `reported_version` | BIGINT | NOT NULL | `DEFAULT 0`. |
| `desired_version` | BIGINT | NOT NULL | `DEFAULT 0`. |
| `last_reported_at` | TIMESTAMPTZ | NULL | |
| `last_desired_at` | TIMESTAMPTZ | NULL | |
| `updated_at` | TIMESTAMPTZ | NOT NULL | `DEFAULT now()`. |
| `last_desired_by` | UUID | NULL | FK `profiles(id) ON DELETE SET NULL` (m6/m8). |

`reported_state`/`desired_state` giữ nguyên kiểu số trong JSONB — `reported_version`/
`desired_version` là BIGINT cần contract an toàn khi trả ra JSON (Task 4.0.3/4.0.4).

### 2.9. `telemetry` (TimescaleDB hypertable)

Nguồn: migration 1; provenance/PK mới ở migration 20.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `measured_at` | TIMESTAMPTZ | NOT NULL | Trục hypertable; phần 1 của PK. |
| `gateway_id` | TEXT | NOT NULL | |
| `sensor_id` | TEXT | NOT NULL | |
| `sequence_num` | BIGINT | NOT NULL | |
| `value` | DOUBLE PRECISION | NOT NULL | |
| `sample_id` | BIGINT | NOT NULL | `DEFAULT nextval('public.telemetry_sample_id_seq')`; phần 2 của PK; tie-breaker nội bộ. |
| `boot_id` | UUID | NULL | Legacy có thể NULL. |
| `message_id` | UUID | NULL | Legacy có thể NULL. |
| `sample_index` | INTEGER | NULL | Legacy có thể NULL. |

- **PK = `(measured_at, sample_id)`** (migration 20 thay PK cũ).
- Check `telemetry_new_provenance`: `boot_id`/`message_id`/`sample_index` NOT NULL và
  `sample_index >= 0`, `sequence_num >= 0` — nhưng `NOT VALID` (không ép row cũ).
- Unique index `telemetry_message_sample (measured_at, gateway_id, message_id, sample_index)
  WHERE message_id IS NOT NULL`.
- Index range `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)`
  (migration 1).
- FK `fk_telemetry_sensor (gateway_id, sensor_id) → sensors ON DELETE RESTRICT` (migration 8).
- Hypertable `measured_at`, chunk 1 ngày; compression 7 ngày segment `gateway_id, sensor_id`,
  order `measured_at DESC, sequence_num DESC`. Retention TẮT.

Raw insert dùng `COPY` (`src/internal/telemetry/postgres_repository.go`) với các cột
`measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index`.

### 2.10. `twin_temporal_values` (TimescaleDB hypertable)

Nguồn: migration 2; check value ở migration 8.

| Cột | Kiểu | Null | Ghi chú |
| --- | --- | --- | --- |
| `observed_at` | TIMESTAMPTZ | NOT NULL | Trục hypertable. |
| `entity_id` | UUID | NOT NULL | FK `twin_entities(id) ON DELETE CASCADE`. |
| `property_name` | TEXT | NOT NULL | |
| `value_number` | DOUBLE PRECISION | NULL | |
| `value_text` | TEXT | NULL | |
| `value_boolean` | BOOLEAN | NULL | |
| `metadata` | JSONB | NOT NULL | `DEFAULT '{}'`. |
| `message_id` | UUID | NULL | |

Check `chk_twin_temporal_exactly_one_value`: `num_nonnulls(value_number, value_text,
value_boolean) = 1`. Index `idx_twin_temporal_entity_prop_time (entity_id,
property_name, observed_at DESC)`. Hypertable `observed_at`, chunk 1 ngày; compression 7
ngày segment `entity_id, property_name`, order `observed_at DESC`. **Không có PK/unique
constraint.** Retention TẮT.

Writer hiện tại chỉ ghi **một** property: `sample_interval_us` (typed `value_number`),
metadata `{unit:"us", boot_id, source:"telemetry_batch"}`
(`src/internal/telemetry/postgres_temporal.go`).

### 2.11. Bảng ngoài phạm vi response (tham chiếu ngữ cảnh)

Các bảng này **không** thuộc nhóm response Stage 4 nhưng ảnh hưởng contract khi cần
đối chiếu:

- `processed_messages` (dedup/ledger): PK `(gateway_id, message_id)`, `payload_hash NOT
  NULL`; migration 20 thêm `source_topic`, `sensor_id`, `boot_id`, `first_sequence`,
  `last_sequence`, `sample_count` (đều nullable legacy, check `NOT VALID`), index
  `processed_stream_range (gateway_id, sensor_id, boot_id, first_sequence, last_sequence)`.
  Stage 4 **không** sửa ledger.
- `twin_commands` và `twin_outbox`: tồn tại trong schema (migration 2) nhưng thuộc
  downlink/Stage 5; Stage 4 **không** tạo command/outbox.
- `media_objects`: thuộc media/Stage 8; không thuộc Task 4.0.

---

## 3. Corrections đối chiếu report/source

Các điểm dưới đây là nơi mô tả tóm tắt trong report chưa khớp source hiện hành. Lấy
protocol/implementation làm chuẩn khi chốt API; **không** mở lại implementation hay
kiểm chứng Stage 2/3 chỉ để sửa lỗi diễn đạt.

1. **ACK topic theo `message_id`:** topic đúng là
   `gateways/<gateway_id>/acks/<message_id>` và ACK wire luôn `status="committed"` cho
   **cả** batch mới lẫn duplicate (duplicate chỉ là outcome nội bộ). ACK có năm field,
   **không** có `boot_id`. Nguồn: `src/internal/telemetry/ack.go`.
2. **Sensor key ghép, không có `sensors.id`:** Sensor được khóa theo
   `(gateway_id, sensor_id)`; raw/catalog dùng PK ghép. Không có cột `sensors.id` như
   một số đoạn mô tả minh họa. Nguồn: migration 1 + `src/internal/telemetry/postgres_resources.go`.
3. **Lock order:** telemetry resolve Gateway (EXISTS) rồi **khóa Sensor row
   `FOR UPDATE`** trước khi ghi ledger/raw/reported/temporal; state row được khóa
   `FOR UPDATE` sau đó. Provisioning Sensor khóa Gateway cha `FOR UPDATE` và đọc role
   `FOR SHARE`. Thứ tự khóa nhất quán cần giữ khi thêm PATCH Stage 4.
4. **Quy tắc equal timestamp:** current reported state chỉ **tiến** khi
   `last_reported_at` NULL, hoặc measurement time mới hơn, hoặc time bằng nhau **cùng
   `boot_id` hợp lệ** và `sequence_num` lớn hơn. Boot/sequence legacy hỏng → giữ nguyên
   state. Không dùng UUID ordering để đoán boot mới hơn. Nguồn:
   `src/internal/telemetry/postgres_reported_state.go`.
5. **Commit ≠ power-loss durability:** commit thành công xác nhận transaction; không
   phải bằng chứng fsync/phần cứng. `committed_at` là lúc caller xác nhận thành công.
6. **Index raw thực tế:** index range hiện có là
   `idx_telemetry_gateway_sensor_time (gateway_id, sensor_id, measured_at DESC)` (migration 1),
   **không** có sẵn tie-breaker `sample_id` trong index này; `sample_id` là tie-breaker
   qua PK `(measured_at, sample_id)` và index unique `telemetry_message_sample`. Không
   giả định index có cột `sequence_num`/`sample_id` chỉ vì report mô tả.
7. **Route provisioning là `PUT`, không phải `POST` minh họa:** route hiện hữu là
   `PUT /v1/admin/gateways/:gateway_id`,
   `PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id`, và
   `PUT /v1/gateways/:gateway_id/sensors/:sensor_id` (create-or-exact-replay). Không sao
   chép method minh họa sai sang API Stage 4.
8. **App role là `iot_backend_app` (không phải `iot_backend`):** `iot_backend` là role
   NOLOGIN sở hữu quyền; `iot_backend_app` là role LOGIN dùng cho kết nối, kế thừa quyền
   và có `BYPASSRLS` tường minh.

---

## 4. Giới hạn kế thừa (ảnh hưởng contract Stage 4)

1. **Retention đang tắt** cho `telemetry` và `twin_temporal_values` (migration 8 gỡ
   policy). Stage 4 không bật TTL/xóa raw; compression 7 ngày vẫn bật.
2. **Legacy provenance có thể NULL:** `telemetry.boot_id/message_id/sample_index`,
   `processed_messages.*` mới, `gateways.created_at`, `sensors.unit`,
   `gateways.description` đều nullable. Contract phải giữ NULL, không thay bằng zero/UUID giả.
3. **Temporal hiện chỉ có `sample_interval_us`** được ghi; **chưa có inference ingress**.
   Payload telemetry v1 không có field inference. Không seed một dòng SQL rồi tuyên bố
   inference đã hoàn thành, không quảng bá writer chưa tồn tại.
4. **Chưa có Stage 4 query measurements:** chưa chạy `EXPLAIN`/đo query; các bound
   (timeout 5s, range 744h, raw window 10s, `max_points` 1000/100–2000) chỉ là thiết kế
   config cho task sau, chưa phải giới hạn đã kiểm.
5. **Số đo ACK Stage 3 không phải SLA history:** p50 43,63ms / p95 47,86ms là số đo
   fixture (1 Gateway × 1 Sensor × 100 Hz), không phải cam kết cho truy vấn history.
6. **RLS bị app role bypass:** mọi quyền người dùng phải do Go/SQL scope theo
   `user_gateways`; platform admin read không tự bypass membership.
7. **Ba route còn là stub/chưa có:** `GET /v1/telemetry/history` và
   `GET /v1/digital-twins` là `501`; `/v1/ws` là `501`; detail/state/history/POST/PATCH
   chưa đăng ký.
8. **`twin_temporal_values` không có PK/unique:** read phải tự đảm bảo sắp xếp ổn định
   (ví dụ theo `observed_at`) và không giả định unique.

---

## 5. Chi tiết validator/provisioning hữu ích cho task kế tiếp

Ghi từ source để Task 4.1–4.6 không phải tự suy diễn lại.

### 5.1. Validator identifier

`src/internal/gateway/identifier.go`:

- `ValidateGatewayID`: khớp `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, **cấm** `backend_service`;
  **không** normalize/unescape.
- `ValidateSensorID`: cùng regex, không có reserved name.
- Validator trả sentinel `ErrInvalidGatewayID`/`ErrInvalidSensorID`.

### 5.2. Validator metadata provisioning

`src/internal/gateway/provisioning_model.go`:

- `name`: không rỗng sau `TrimSpace`, `<= 256` byte, UTF-8 hợp lệ, không chứa NUL.
- `description` (Gateway): nullable, `<= 4096` byte (nếu có).
- `unit` (Sensor): nullable, `<= 64` byte (nếu có).
- `ValidateProvisionGatewayInput` yêu cầu `owner_user_id` hợp lệ (không Nil); repository
  mới kiểm owner tồn tại trong transaction.

### 5.3. Strict JSON conventions cho body (tái dùng cho POST/PATCH)

`src/internal/httpserver/provisioning_request.go` (`parseProvisioningObject`):

- Query string phải rỗng (`RawQuery == "" && !ForceQuery`) → nếu có bất kỳ query → `400`.
- `Content-Type` phải là `application/json`; param chỉ cho phép `charset=utf-8` → nếu sai
  `415`.
- Body giới hạn `limit+1` byte; vượt → `413`.
- Validate Unicode escape thủ công (reject NUL, surrogate lẻ, escape `\u0000`) — không
  để `encoding/json` thay thế Unicode hỏng.
- Top-level phải là object; **duplicate key**, **unknown key**, trailing document đều
  `400`; field non-nullable nhận `null` → `400`; field bắt buộc thiếu → `400`.

### 5.4. Ngữ nghĩa create-or-exact-replay (provisioning hiện có)

`src/internal/gateway/postgres_gateway_provisioning.go` /
`postgres_sensor_provisioning.go`:

- **Gateway:** owner profile phải tồn tại (`SELECT id FROM profiles WHERE id=$1`); nếu
  không có → `ErrProvisioningNotFound` (HTTP `404`). `INSERT ... ON CONFLICT DO NOTHING`;
  khi conflict đọc `FOR UPDATE` và so `name` + `description` (nullable so chính xác);
  lệch → `ErrProvisioningConflict` (`409`). Kiểm cả số lượng owner membership và đúng một
  Gateway entity + state.
- **Sensor:** Gateway cha `FOR UPDATE`; kiểm role `owner` (`FOR SHARE` trên
  `user_gateways`); `INSERT ... ON CONFLICT DO NOTHING`; replay so `name` + `unit`; kiểm
  `hasSensor` relationship + đúng một Sensor entity.
- HTTP mapping (`admin_gateway_handlers.go`, `owner_sensor_handlers.go`): `201` khi mới,
  `200` khi exact replay, `409` conflict, `404` not found, `403` forbidden, `400` invalid,
  `413/415` body/media type, `503` admin-lookup/timeout/unavailable, `500` khác.

### 5.5. URN mapper

`src/internal/digitaltwin/mapper/identity.go`:

- `GatewayEntityID(gatewayID)` = `urn:ngsi-ld:Gateway:<gateway_id>`.
- `SensorEntityID(gatewayID, sensorID)` = `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`.
- Pure mapper, không normalize, không fetch JSON-LD context.

### 5.6. Telemetry resolve/order graph

`src/internal/telemetry/postgres_resources.go`: xác nhận Gateway tồn tại → khóa Sensor
`FOR UPDATE` → xác minh Twin graph (Gateway + Sensor entity + `hasSensor` + Gateway/Sensor
state). Sai graph → `ReasonResourceGraphIncomplete`; Gateway/Sensor không tồn tại →
`ReasonUnknownGateway`/`ReasonUnknownSensor`.

---

## 6. Chưa kiểm chứng (NOT CHECKED)

- **Catalog runtime CHƯA được kiểm.** Toàn bộ bảng cột/index/RLS ở mục 2 lấy từ migration
  files trong Git, **không** query `information_schema`/`pg_indexes` trên DB thật. Nếu cần
  xác nhận catalog, chỉ đọc schema/index/version trên DB test đã xác định đúng đích và ghi
  rõ nguồn; không seed/reset dữ liệu để lấy baseline.
- Không chạy `EXPLAIN`/đo query, không chạy suite Go, không chạy native integration, không
  re-run CI.
- Không kiểm quyền thực tế của `iot_backend_app`/`iot_backend` bằng `has_*_privilege` trên
  runtime (chỉ đọc từ migration/script).
- Không kiểm secret; tài liệu không ghi credential nào (kể cả `DATABASE_URL` thật).

---

## 7. Contract Task 4.0.2 — danh sách endpoint, identity và quyền

Bước **4.0.2** chốt **đúng bảy** endpoint, cách resolve identity công khai và hai ma
trận quyền. DTO chi tiết, examples, time range `[from,to)`, raw/bucket,
cursor/pagination, representation/context và bảng error đầy đủ thuộc Task
4.0.4–4.0.6 và **chưa** được chốt ở đây. Toàn bộ mục này là **thiết kế**; không
endpoint nào trong số này đang hoạt động (xem khối trạng thái đầu tài liệu).

### 7.1. Bảy endpoint

| # | Method / path | Trạng thái hiện tại | Input chính (tóm tắt) | Output chính (tóm tắt) |
| --- | --- | --- | --- | --- |
| 1 | `GET /v1/telemetry/history` | stub `501 not_implemented` (đã đăng ký) | `gateway_id`, `sensor_id`, `from`, `to`; optional `resolution`, `max_points` | một series raw hoặc bucket đã scope membership + metadata range/unit/mode/số điểm |
| 2 | `GET /v1/digital-twins` | stub `501 not_implemented` (đã đăng ký) | optional `gateway_id`, `entity_type=Gateway\|Sensor`, `limit`, `after` | `items` (summary) + `next_cursor`; chỉ entity thuộc membership |
| 3 | `GET /v1/digital-twins/{entity_id}` | chưa đăng ký | `entity_id` URN URL-encoded; tham số phân trang quan hệ | entity view NGSI-LD-compatible + trang quan hệ bounded + `next_cursor` |
| 4 | `GET /v1/digital-twins/{entity_id}/state` | chưa đăng ký | `entity_id` URN URL-encoded | reported/desired JSONB, versions, `last_*_at`, audit được phép đọc |
| 5 | `GET /v1/digital-twins/{entity_id}/history` | chưa đăng ký | `entity_id`; `property`, `from`, `to`; optional `resolution`, `max_points` | series temporal (`property=value` ánh xạ raw telemetry; `property=sample_interval_us` đọc typed temporal) |
| 6 | `POST /v1/digital-twins` | chưa đăng ký | DTO admin: `entity_type` + identifier + name/metadata allowlist (Gateway cần owner profile) | `201` mới / `200` exact replay / `409` conflict |
| 7 | `PATCH /v1/digital-twins/{entity_id}` | chưa đăng ký | `entity_id` + allowlist `name`/`description` có presence/null semantics | `200` sau transaction metadata (không side effect state/command) |

Chỉ #1 và #2 đã có route và vẫn là stub `501` (chưa hoạt động); #3–#7 **chưa** tồn
tại trong `router.go` và không được coi là khả dụng. Field/defaults/bounds, ví dụ và
cursor chi tiết do Task 4.0.3–4.0.6 chốt (history/raw/bucket/temporal đã chốt ở mục 8;
list/detail/state/POST/PATCH còn ở 4.0.4–4.0.6); 4.0.2 chỉ cố định method/path + input/output
tóm tắt + identity + quyền.

### 7.2. Public URN, encode URL và resolve identity

- **Mapper công khai (đã có, không normalize):** `mapper.GatewayEntityID(gatewayID)`
  = `urn:ngsi-ld:Gateway:<gateway_id>` và
  `mapper.SensorEntityID(gatewayID, sensorID)`
  = `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`
  (`src/internal/digitaltwin/mapper/identity.go`).
- **Encode URL:** client gửi `{entity_id}` đã URL-encode trong path (ví dụ `%3A` cho
  `:`); server decode một lần qua `c.Param` rồi validate trên chuỗi đã decode, không
  normalize/unescape lần hai.
- **Validate + parse:** chuỗi phải khớp `urn:ngsi-ld:` + đúng `Gateway` hoặc
  `Sensor`; `entity_type` lấy **từ URN**, không lấy từ field client gửi (lệch → `400`).
  Tách phần sau type theo `:`:
  - `Gateway`: đúng một component, phải qua `ValidateGatewayID`.
  - `Sensor`: đúng hai component `(gateway_id, sensor_id)`, cả hai phải qua
    `ValidateGatewayID`/`ValidateSensorID`.
  - Vì regex identifier `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` không chứa `:`, việc tách
    theo `:` là không nhập nhằng.
- **Resolve:** tra `twin_entities.entity_id` (UNIQUE) khớp chính xác; không dựng lại
  URN từ query, không so khớp mờ. Sau resolve, dùng `twin_entities.id` (UUID nội bộ)
  cho JOIN và **không** trả ra ngoài.
- **Sensor identity ghép:** Sensor chỉ duy nhất trong Gateway cha
  (`sensors` PK `(gateway_id, sensor_id)`), nên cùng `sensor_id` ở hai Gateway cho hai
  URN khác nhau; mọi truy vấn Sensor phải kèm `gateway_id`.
- **Cấm chọn UUID nội bộ:** request **không** được cung cấp `twin_entities.id` (không
  có cột `sensors.id`); nếu client gửi UUID nội bộ, hoặc type/Gateway không nhất quán
  với URN → `400` (không phải `404`).

### 7.3. Ma trận quyền đọc (deterministic)

| Endpoint đọc | Platform admin | Owner | Operator | Viewer | Non-member |
| --- | --- | --- | --- | --- | --- |
| history/list/detail/state/temporal (endpoint #1–#5) | chỉ theo membership, **không** bypass | Gateway được gán | Gateway được gán | Gateway được gán | Từ chối |

- JWT hợp lệ **chỉ** chứng minh identity (`Principal{UserID, Role}`, claim
  `role=="authenticated"`); quyền đọc không đến từ claim, query, body hay cursor.
- Mọi truy vấn đọc scope `user_gateways` theo `principal.UserID` và `gateway_id` của
  resource (JOIN/EXISTS), **không** query toàn cục rồi lọc ở Go; không cache quyền qua
  request.
- Param `user_id` (hoặc role/identity) do client gửi bị từ chối `400` như list handler
  hiện có; không được dùng làm căn cứ cấp quyền.
- **Platform admin không tự bypass membership** trên năm read endpoint này; muốn đọc
  phải có `user_gateways` như user thường.
- Contract inventory admin hiện có (`GET /v1/admin/gateways`, default 100 / max 200,
  keyset `after`) **không** thay đổi.

### 7.4. Ma trận quyền mutation và thứ tự kiểm an toàn

| Mutation | Platform admin | Owner (Gateway cha) | Operator | Viewer | Non-member |
| --- | --- | --- | --- | --- | --- |
| `POST /v1/digital-twins` (`entity_type=Gateway`) | DB `platform_admins` | Không | Không | Không | Không |
| `POST /v1/digital-twins` (`entity_type=Sensor`) | DB `platform_admins` | Được | Không | Không | Không |
| `PATCH /v1/digital-twins/{entity_id}` (Gateway) | DB `platform_admins` | Không (không thêm quyền mới) | Không | Không | Không |
| `PATCH /v1/digital-twins/{entity_id}` (Sensor) | DB `platform_admins` | Được | Không | Không | Không |

- Capability admin kiểm **bằng DB** `platform_admins` (middleware/checker hiện có),
  **không** nhận role/user từ query/body/cursor và **không** dựa vào JWT `service_role`.
- **Thứ tự an toàn (không tiết lộ resource cho người không có quyền):**
  - Route **admin-only** (`POST` Gateway, `PATCH` Gateway): kiểm capability admin (DB)
    **trước**; non-admin → `403` ngay, **trước khi** tra resource đích, nên sự tồn tại
    của Gateway không bị tiết lộ. `POST` Gateway không có resource đích. Chỉ sau khi
    capability đạt mới validate/resolve resource (PATCH: resource thiếu → `404`).
  - Route **admin-hoặc-owner** (`POST`/`PATCH` Sensor), trong **một** transaction theo
    đúng thứ tự `ProvisionSensorForOwner` hiện có: (1) JWT → principal, validate
    identifier (sai → `400`); (2) kiểm admin DB, nếu admin thì tiếp; (3) khóa hàng
    Gateway cha `FOR UPDATE`, thiếu → `404`; (4) tra `user_gateways (user_id,
    gateway_id) FOR SHARE`, không có hàng → `404` (non-member **không phân biệt được**
    với Gateway không tồn tại); (5) role phải là `owner`; operator/viewer (có hàng
    nhưng role không đủ) → `403`; (6) ghi metadata trong cùng transaction, commit nếu
    không lỗi.
  - Kết quả: non-member → `404`, "nhìn thấy nhưng role không đủ" → `403` (khớp mục 3
    kế hoạch Stage 4).
- **Đối chiếu source:** `ProvisionSensorForOwner`
  (`src/internal/gateway/postgres_sensor_provisioning.go` dòng 69–88) hiện đã trả
  `ErrProvisioningNotFound` khi Gateway cha thiếu **hoặc** khi không có hàng
  membership, và `ErrProvisioningForbidden` chỉ khi có hàng với `role != owner`. Vậy
  thứ tự mong muốn khớp hành vi hiện có; Stage 2 `PUT` giữ nguyên.

### 7.5. Quy tắc status/empty/error

- Đọc: resource ngoài quyền **hoặc** không tồn tại → cùng `404 not_found`, không phân
  biệt.
- List không có tài nguyên trong quyền → `200` với `items=[]` (`next_cursor` rỗng/không
  có).
- Mutation vào resource nhìn thấy nhưng role không đủ → `403 forbidden`; non-member →
  `404` (theo thứ tự 7.4).
- `POST`/`PATCH` metadata: `201` mới, `200` exact replay, `409` conflict; `400` input,
  `413/415` body/media type (bảng đầy đủ ở 4.0.6).
- **Lỗi DB khi kiểm quyền/ghi không bao giờ thành `404`, `items=[]` hay `200`:**
  timeout/cancel/unavailable → `503 service_unavailable`; lỗi bất ngờ → `500
  internal_error`. Không trả success khi commit lỗi hoặc bất định.

### 7.6. Ranh giới phạm vi

- Chỉ hai entity type `Gateway`/`Sensor` và graph provisioning hiện có (`Gateway` +
  owner membership + Twin + state; `Sensor` + Twin + `hasSensor` + state). Không
  entity/relationship tùy ý.
- `/state` **đọc** cả reported và desired, nhưng API metadata (`POST`/`PATCH`)
  **không** ghi reported/desired/versions và không đụng `sample_interval_us`.
- Các endpoint này **không** phát MQTT và **không** tạo command/outbox/credential/human
  account; không cấp quyền ghi `desired_state` (thuộc Stage 5).
- Stage 2 `PUT` giữ nguyên semantics create-or-exact-replay và inventory:
  `PUT /v1/admin/gateways/:gateway_id`,
  `PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id`,
  `PUT /v1/gateways/:gateway_id/sensors/:sensor_id`; các `POST`/`PATCH` Stage 4 không
  thay tên hay thay thế chúng.

---

## 8. Contract Task 4.0.3 — telemetry và temporal history

Bước **4.0.3** chốt request/response, thời gian, raw/bucket, temporal typed history,
safe BIGINT/NULL và error cho hai endpoint history (**#1** `GET /v1/telemetry/history`
và **#5** `GET /v1/digital-twins/{entity_id}/history`, đánh số theo mục 7.1). Toàn bộ
mục này là **thiết kế**: `#1` vẫn là stub `501 not_implemented`, `#5` chưa đăng ký.
Không thêm config/`.env`/migration/code ở Task 4.0.3; các bounds dưới đây là thiết kế
config cho Task 4.1–4.2. Mã lỗi ở đây là đề xuất khớp style hiện có; bảng error đầy đủ
thuộc 4.0.6.

### 8.1. Endpoint #1 request

| Param | Bắt buộc | Kiểu | Default | Ràng buộc |
| --- | --- | --- | --- | --- |
| `gateway_id` | Có | string | — | `ValidateGatewayID` (`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, khác `backend_service`). |
| `sensor_id` | Có | string | — | `ValidateSensorID` (cùng regex). Sensor chỉ duy nhất trong Gateway cha. |
| `from` | Có | string RFC3339 có offset | — | Chuẩn hóa UTC microsecond (8.2). |
| `to` | Có | string RFC3339 có offset | — | `from < to`. |
| `resolution` | Không | string | `auto` | allowlist ở 8.4. |
| `max_points` | Không | int (base-10) | `1000` | `100..2000`. |

- Dùng lại `ValidateGatewayID`/`ValidateSensorID` (`src/internal/gateway/identifier.go`)
  cho identifier; sai → `400`.
- Query param **unknown** (`?foo=`) hoặc **lặp** (`?from=a&from=b`) → `400`. Query
  `user_id`/`role`/identity → `400` như list handler hiện có; không dùng làm căn cứ
  cấp quyền (mục 7.3).
- Sai/thiếu field bắt buộc, `max_points` không phải int hoặc ngoài khoảng → `400`.

### 8.2. Thời gian

- `from`/`to` là RFC3339 **có offset bắt buộc** (`+07:00` hoặc `Z`); thiếu offset → `400`.
  Ví dụ đổi timezone: `2026-08-21T17:15:00+07:00` = `2026-08-21T10:15:00Z`.
- Chuẩn hóa về **UTC microsecond** (khớp cột `TIMESTAMPTZ`/`measured_at`). Fractional
  > 6 chữ số (ví dụ `.1234567`) → `400`, **không** làm tròn; chấp nhận 0–6 chữ số.
- Cửa sổ **`[from,to)`**: `from` **bao gồm**, `to` **loại trừ**. SQL là
  `measured_at >= :from AND measured_at < :to`. `from >= to` → `400`.
- Ví dụ boundary (`resolution=raw`, `from=10:15:00Z`, `to=10:15:01Z`): sample
  `measured_at=2026-08-21T10:15:00.000000Z` **có** trong kết quả; sample
  `2026-08-21T10:15:01.000000Z` **bị loại**.
- `to - from > 744h` → `400 range_too_large`.

### 8.3. Bounds (thiết kế config cho task sau; không thêm env trong 4.0.3)

| Bound | Giá trị | Ghi chú |
| --- | --- | --- |
| Query timeout | `5s` | Áp cho toàn use case gồm acquire/query; hết hạn → `503`, không cắt kết quả im lặng. |
| Max range | `744h` (31 ngày) | Không cam kết mọi query 744h đều nhanh. |
| Raw window max | `10s` | Chỉ dùng cho `resolution=raw` và nhánh raw của `auto`. |
| `max_points` default | `1000` | |
| `max_points` range | `100..2000` | Ngoài khoảng → `400`. |

Đây là giới hạn **response/service**, **không** phải cam kết số row DB tối đa bị scan;
bucket query vẫn có thể scan nhiều raw.

### 8.4. `resolution` và chọn mode

Allowlist server-owned: `auto` (default), `raw`, `100ms`, `1s`, `10s`, `1m`, `5m`, `15m`,
`1h`, `6h`, `1d`. Token khác → `400 invalid_resolution`.

- **`raw`**: chỉ hợp lệ khi `to-from <= 10s`; đếm rows trong `[from,to)`; window > 10s
  **hoặc** rows > `max_points` → `400 raw_limit_exceeded`. Không `LIMIT` rồi trả success
  thiếu dữ liệu.
- **`auto`**: window `<= 10s` đọc raw `LIMIT max_points+1`; `rows <= max_points` → mode
  raw; ngược lại chuyển bucket (8.5). Window `> 10s` chọn bucket trực tiếp.
- **`<bucket>` explicit**: dùng đúng width; nếu `B(W) > max_points` (8.5, có partial) →
  `400 resolution_too_fine`; không tự đổi width rồi giấu client.

### 8.5. Origin, công thức bucket, auto selection và clipping

- **Origin cố định:** `O = 1970-01-01T00:00:00Z`. Mọi width allowlist chia hết `O`.
  Contract định nghĩa công thức toán học độc lập default origin của TimescaleDB; SQL dùng
  `time_bucket(W, measured_at, origin => TIMESTAMPTZ '1970-01-01 00:00:00+00')`.
- Với width `W` (µs) và instant `t` (µs kể từ epoch): `k(t) = floor((t - O)/W)` và
  `bucket_start = O + k*W` (floor xuống vô cực).
- Số bucket **giao** `[from,to)` (đếm cả partial hai biên):
  `B(W) = floor((to - 1µs - O)/W) - floor((from - O)/W) + 1`.
  Đã reject precision dưới µs nên `to - 1µs` xác định và `B(W) >= 1`.
- **Auto bucket = width nhỏ nhất** trong allowlist có `B(W) <= max_points`. `744h`/`1d`
  = 31 bucket `< max_points` min `100`, nên auto luôn có lời giải.
- **Clipping trước aggregate:** subquery `WHERE measured_at >= :from AND measured_at < :to`
  **rồi mới** `time_bucket` + `GROUP BY`. `avg/min/max/count` chỉ gồm mẫu trong cửa sổ.
  `bucket_start` của bucket đầu có thể **< from** (partial edge).
- **Gap:** bucket **không** có mẫu **không** được trả; không fill `0`, không nội suy,
  không biến bucket rỗng thành sample đo. Không xóa/downsample raw trong DB.

### 8.6. Envelope và field names (#1 raw/bucket)

| Field | Kiểu JSON | Raw | Bucket | Ghi chú |
| --- | --- | --- | --- | --- |
| `gateway_id`, `sensor_id` | string | ✓ | ✓ | identity đã scope membership. |
| `unit` | string \| null | ✓ | ✓ | `sensors.unit` nullable → giữ `null`. |
| `from`, `to` | string | ✓ | ✓ | RFC3339 UTC microsecond `…ssssssZ`. |
| `mode` | string | ✓ | ✓ | `"raw"` hoặc `"bucket"`. |
| `resolution` | string | ✓ | ✓ | effective: `"raw"` hoặc token bucket (`"1s"`…). |
| `point_count` | int | ✓ | ✓ | `len(points)`; **bounded số nguyên** (`<= max_points <= 2000`). |
| `points` | array | ✓ | ✓ | xem dưới. |
| raw `measured_at` | string | ✓ | | RFC3339 UTC µs. |
| raw `value` | number | ✓ | | `DOUBLE PRECISION`. |
| raw `sequence_num` | string | ✓ | | BIGINT → chuỗi thập phân (8.7). |
| raw `boot_id`, `message_id` | string \| null | ✓ | | UUID legacy có thể `null`. |
| raw `sample_index` | int \| null | ✓ | | cột INTEGER; legacy `null` giữ `null`. |
| bucket `bucket_start` | string | | ✓ | RFC3339 UTC µs. |
| bucket `avg`, `min`, `max` | number | | ✓ | `DOUBLE PRECISION`. |
| bucket `count` | string | | ✓ | `COUNT(*)` **BIGINT → chuỗi thập phân**; khác `point_count`. |

- Sort raw: `measured_at ASC, sample_id ASC`. `sample_id` là tie-breaker nội bộ (PK
  `(measured_at, sample_id)`) và **không** trả ra. Không dedup observation cùng
  `measured_at` nhưng khác `sample_id`.
- `point_count` (số điểm response, int bounded) **khác** `count` (số mẫu raw trong một
  bucket, BIGINT → chuỗi). `point_count = B(W)` không đúng khi có gap (bucket rỗng không
  trả) nên luôn là số điểm **thực trả**.

### 8.7. Safe BIGINT và nullable provenance (dùng chung #1 và #5)

- Field BIGINT public trả **chuỗi thập phân** để Flutter web/JS không mất độ chính xác:
  `sequence_num`, bucket `count`, và `reported_version`/`desired_version` (thống nhất khi
  chốt ở 4.0.4). **Không** decode/re-encode qua `float64`.
- Số **bounded** giữ JSON number: `point_count`, `sample_index` (INTEGER), `limit`.
- Provenance legacy **giữ NULL**: `boot_id`, `message_id`, `sample_index`; không thay
  `null` bằng `0`/UUID giả. `sequence_num` NOT NULL nhưng có thể > 2^53 → chuỗi.

### 8.8. Endpoint #5 request và `property` selector

Query giống 8.1 (cùng time/bounds/`resolution`), thay `gateway_id`/`sensor_id` bằng
`entity_id` (URN URL-encoded, mục 7.2) và thêm `property` **bắt buộc**.

| Selector API (`property=...`) | Entity | Engine thực thi | Storage property tương ứng | Public term mapper (JSON-LD detail) | Unit |
| --- | --- | --- | --- | --- | --- |
| `value` | Sensor | **telemetry raw/bucket engine** (#1) sau resolve ownership + `hasSensor` | `telemetry.value` (sensor raw) | `measurement` | symbol từ `sensors.unit` |
| `sample_interval_us` | Sensor | **typed temporal** `twin_temporal_values` (`value_number`) | `property_name='sample_interval_us'` — property duy nhất hiện có writer (`src/internal/telemetry/postgres_temporal.go`) | `sampleIntervalUs` | `metadata.unit` (`us`) |

- **Phục hồi selector API theo đúng Stage 4 plan:** `property` là tham số query HTTP độc lập,
  **bắt buộc**, chấp nhận đúng hai selector: `value` (ánh xạ telemetry engine của sensor) và
  `sample_interval_us` (ánh xạ hypertable temporal). Mọi selector khác (kể cả `measurement`,
  `sampleIntervalUs`, hay bất kỳ token nào) → `400 invalid_property`.
- **Lý do phân định selector API và mapper JSON-LD:** Việc JSON-LD quy định cấu trúc `Property`
  có keyword nội bộ `@value` / `hasValue` khiến mapper entity detail phải đặt tên public term
  là `measurement` để tránh va chạm thuộc tính trong JSON-LD object. Tuy nhiên, việc lồng
  keyword JSON-LD này **KHÔNG** đòi hỏi phải đổi tên tham số query selector HTTP độc lập.
  Selector API HTTP tuân thủ nghiêm ngặt Stage 4 plan và Task 4.0 detail plan (`value` và
  `sample_interval_us`), được ánh xạ tường minh sang public term mapper tại bảng mục 11.1.
- **Inference chưa triển khai:** `anomaly_score`, `reconstruction_loss` và mọi property
  text/boolean là **reserved, chưa selectable** trong Stage 4 (không có writer). Không đưa
  selector chưa hỗ trợ vào allowlist chỉ để minh họa kiểu tương lai.
- Property **hợp lệ** nhưng chưa có observation → `200` series rỗng (`point_count=0`,
  `points=[]`), phân biệt với `404` (ngoài quyền/không tồn tại).

### 8.9. Typed temporal read rules (#5 temporal)

Point shape typed: `observed_at` (string RFC3339 UTC µs), `value_type`
(`"number"|"text"|"boolean"`), `value` (number|string|bool), `metadata` (object, **không
null**), `message_id` (string|null).

- `value_number` non-null → numeric: bucket được (avg/min/max/count) **chỉ khi unit đồng
  nhất**; `value`/`avg`/`min`/`max` JSON number.
- `value_text` non-null → text: **không** aggregate; trả observations có bound như raw
  (8.4). JSON string.
- `value_boolean` non-null → boolean: như text, JSON bool. Không tự gán số/`0/1`.
- Đúng **một** trong ba cột non-null mỗi row (`chk_twin_temporal_exactly_one_value`);
  reader giữ **nullable** của hai cột còn lại, không bịa giá trị.
- `metadata` là JSONB **NOT NULL** (có thể `{}`); `unit`/`boot_id` có thể vắng — trả
  nguyên trạng. "NULL metadata" = field nullable vắng, **không** phải cột JSON `null`.
- `message_id` nullable → giữ `null`.
- Unit không đồng nhất trong cửa sổ (numeric) → `400 inconsistent_unit`; không trộn đơn vị.
- Window/observations vượt raw bound với kiểu không aggregate được → `400
  raw_limit_exceeded` (yêu cầu thu hẹp), không mất rows im lặng.
- Numeric temporal bucket dùng chung origin/công thức/clipping 8.5 và envelope 8.6
  (thay identity bằng `entity_id`/`property`, `unit` từ `metadata.unit`).

### 8.10. Temporal ordering / tie (schema hiện tại)

- `twin_temporal_values` **không có PK/unique** (mục 2.10). Read **không dedup**: nhiều
  observation cùng `(observed_at, property_name)` hợp lệ đều được trả.
- Sort `observed_at ASC`. Vì không có key duy nhất, thứ tự **giữa các row cùng
  `observed_at` không được schema bảo đảm**. Best-effort thứ cấp (không phải cam kết):
  `message_id ASC NULLS LAST`. Bảo đảm thứ tự trong tie đòi migration thêm key —
  **ngoài** phạm vi Task 4.0; ghi là limitation.
- Với `property=value` (engine #1), tie dùng `sample_id` (PK) nên ổn định (8.6).

### 8.11. Examples (JSON parseable; placeholder; DESIGN ONLY)

**(a) Raw** — legacy NULL và `sequence_num` > 2^53 (`from=2026-08-21T10:15:00Z`,
`to=2026-08-21T10:15:02Z`, `resolution=raw`):

```json
{
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "unit": "degC",
  "from": "2026-08-21T10:15:00.000000Z",
  "to": "2026-08-21T10:15:02.000000Z",
  "mode": "raw",
  "resolution": "raw",
  "point_count": 2,
  "points": [
    {
      "measured_at": "2026-08-21T10:15:00.500000Z",
      "value": 25.1,
      "sequence_num": "12501",
      "boot_id": "7f2c45f7-0a76-4e28-8a37-7793d7d85b04",
      "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
      "sample_index": 0
    },
    {
      "measured_at": "2026-08-21T10:15:01.250000Z",
      "value": 25.2,
      "sequence_num": "9007199254740993",
      "boot_id": null,
      "message_id": null,
      "sample_index": null
    }
  ]
}
```

**(b) Bucket partial/gap** — `resolution=1s`, `from=…10:15:00.250000Z`,
`to=…10:15:03.750000Z`. `B(1s)=4` (bucket `00,01,02,03`); bucket `10:15:02` không có mẫu
nên **vắng**, `point_count=3`. Bucket `00` và `03` là partial (biên bị clip):

```json
{
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "unit": "degC",
  "from": "2026-08-21T10:15:00.250000Z",
  "to": "2026-08-21T10:15:03.750000Z",
  "mode": "bucket",
  "resolution": "1s",
  "point_count": 3,
  "points": [
    { "bucket_start": "2026-08-21T10:15:00.000000Z", "avg": 25.15, "min": 25.1, "max": 25.2, "count": "100" },
    { "bucket_start": "2026-08-21T10:15:01.000000Z", "avg": 25.30, "min": 25.2, "max": 25.4, "count": "100" },
    { "bucket_start": "2026-08-21T10:15:03.000000Z", "avg": 25.05, "min": 25.0, "max": 25.1, "count": "40" }
  ]
}
```

**(c) Authorized empty** — Sensor hợp lệ, cửa sổ không có mẫu → `200` series rỗng
(`unit` cũng có thể `null`):

```json
{
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "unit": null,
  "from": "2026-08-21T10:20:00.000000Z",
  "to": "2026-08-21T10:20:05.000000Z",
  "mode": "raw",
  "resolution": "raw",
  "point_count": 0,
  "points": []
}
```

**(d) Raw overflow** — `resolution=raw` vượt window `10s` hoặc vượt `max_points` → `400`,
envelope lỗi hiện có:

```json
{
  "error": {
    "code": "raw_limit_exceeded",
    "message": "raw window or point limit exceeded; use auto or a bucket resolution, or narrow the time range",
    "request_id": "req_placeholder"
  }
}
```

**(e) Temporal numeric** — `property=sample_interval_us`, khớp writer hiện có
(`value_number`, metadata `{unit,boot_id,source}`):

```json
{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "property": "sample_interval_us",
  "unit": "us",
  "from": "2026-08-21T10:15:00.000000Z",
  "to": "2026-08-21T10:15:02.000000Z",
  "mode": "raw",
  "resolution": "raw",
  "point_count": 1,
  "points": [
    {
      "observed_at": "2026-08-21T10:15:01.900000Z",
      "value_type": "number",
      "value": 10000,
      "metadata": { "unit": "us", "boot_id": "7f2c45f7-0a76-4e28-8a37-7793d7d85b04", "source": "telemetry_batch" },
      "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2"
    }
  ]
}
```

**(f) Typed text/boolean/NULL-metadata — reader fixture minh họa, KHÔNG phải writer claim hay active selector.**
Các property dưới đây là **reserved, chưa selectable** trong Stage 4 (không có writer; server trả `400 invalid_property`
nếu client gửi trong query); ví dụ dưới đây **chỉ** dùng làm test fixture chứng minh reader xử lý bảo toàn typed value
và nullable metadata nếu sau này có pipeline ingestion tương ứng, hoàn toàn không phải selector đang hoạt động hay khẳng
định dữ liệu đang tồn tại:

```json
{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "property": "<reserved-property-not-selectable>",
  "unit": null,
  "from": "2026-08-21T10:15:00.000000Z",
  "to": "2026-08-21T10:15:02.000000Z",
  "mode": "raw",
  "resolution": "raw",
  "point_count": 3,
  "points": [
    { "observed_at": "2026-08-21T10:15:00.000000Z", "value_type": "text",    "value": "auto", "metadata": {},               "message_id": null },
    { "observed_at": "2026-08-21T10:15:00.500000Z", "value_type": "boolean", "value": true,   "metadata": { "unit": null },  "message_id": null },
    { "observed_at": "2026-08-21T10:15:01.000000Z", "value_type": "number",  "value": 12.5,   "metadata": { "unit": "degC" }, "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2" }
  ]
}
```

### 8.12. Ràng buộc nguồn cho 4.0.3 (đối chiếu code/schema)

- `telemetry` PK `(measured_at, sample_id)`; `sequence_num BIGINT NOT NULL`; `value
  DOUBLE PRECISION NOT NULL`; `boot_id/message_id` UUID NULL; `sample_index INTEGER NULL`
  (`migrations/000001`, `migrations/000020`). Tie-breaker `sample_id` tồn tại; index range
  hiện có `(gateway_id, sensor_id, measured_at DESC)` (mục 3.6) — không giả định index có
  `sequence_num`/`sample_id` cho tới khi query measurement (Task 4.2) xác nhận; thêm index
  là quyết định của task query, không phải 4.0.3.
- `twin_temporal_values`: `observed_at`, `entity_id`, `property_name`,
  `value_number/value_text/value_boolean` (đúng một non-null), `metadata JSONB NOT NULL`,
  `message_id UUID NULL`; **không PK/unique** (`migrations/000002`, `migrations/000008`).
- Writer temporal hiện tại chỉ ghi `sample_interval_us` với `value_number`, metadata
  `{unit:"us", boot_id, source:"telemetry_batch"}`, `observed_at` = measurement time của
  sample cuối batch, `message_id` = `message_id` batch
  (`src/internal/telemetry/postgres_temporal.go`).
- `sample_interval_us` validate `0 < x <= 2^53` (`maxExactFloat64Integer`,
  `src/internal/telemetry/validation.go`) — không có property khác được ghi.
- Không có `time_bucket`/history engine trong `src/internal/telemetry/` hiện tại; mục 8 là
  thiết kế mới, chưa được implement/đo bằng `EXPLAIN`.

---

## 9. Contract Task 4.0.4 — Twin list/detail/state và phân trang quan hệ

Bước **4.0.4** chốt ba endpoint **#2** `GET /v1/digital-twins`, **#3**
`GET /v1/digital-twins/{entity_id}` và **#4** `GET /v1/digital-twins/{entity_id}/state`
(đánh số theo mục 7.1), gồm filter/summary, cursor list, projection detail, phân trang
quan hệ `hasSensor` và state field. Toàn bộ mục này là **thiết kế**: `#2` vẫn là stub
`501 not_implemented`; `#3`/`#4` **chưa** đăng ký. Không thêm code/DTO/migration/config ở
bước này. Protected response dùng `Cache-Control: no-store` theo middleware hiện có
(mục 1.4), không lặp lại. BIGINT/NULL/JSON-number dùng chung **8.7**; time range
`[from,to)` (8.2) thuộc history, **không** áp cho list/detail/state.

### 9.1. List `GET /v1/digital-twins`

**Request** (query; unknown/lặp → `400`; `user_id`/role/identity → `400` như list hiện có):

| Param | Bắt buộc | Kiểu | Default | Ràng buộc |
| --- | --- | --- | --- | --- |
| `gateway_id` | Không | string | — | `ValidateGatewayID` (5.1). |
| `entity_type` | Không | string | — | Allowlist **`Gateway`**, **`Sensor`**; token khác (kể cả `Device`) → `400`. |
| `limit` | Không | int base-10 | `50` | `1..200`; ngoài khoảng/không phải digit → `400`. |
| `after` | Không | string | — | Cursor list v1 (9.2); sai → `400 invalid_cursor`. |

**Query scope membership** (thay cho kiểm từng entity; platform admin **không** bypass, 7.3):

```sql
FROM twin_entities e
JOIN user_gateways ug ON ug.gateway_id = e.gateway_id AND ug.user_id = $principal
WHERE ($gateway_id IS NULL OR e.gateway_id = $gateway_id)
  AND ($entity_type IS NULL OR e.entity_type = $entity_type)
  AND ($after IS NULL OR e.entity_id COLLATE "C" > $after COLLATE "C")
ORDER BY e.entity_id COLLATE "C" ASC
LIMIT $limit + 1
```

**Envelope** (không trả total; `count` chỉ là số item trang; `items` là summary ở bảng
dưới, `next_cursor` là cursor v1 hoặc `null`):

```json
{ "items": [], "next_cursor": null }
```

- `limit+1` chỉ để phát hiện còn trang; `next_cursor` = cursor dựng từ `entity_id` cuối
  **đã trả** khi còn dư, ngược lại `null`. Không `offset`, không `COUNT` toàn bộ.
- Không có tài nguyên trong quyền → `200 items=[]`, `next_cursor: null`.

**Summary projection** (không nhúng relationship/property/state; không load cả graph):

| Field | Nguồn authoritative | Ghi chú |
| --- | --- | --- |
| `id` | `twin_entities.entity_id` | URN public. |
| `type` | `twin_entities.entity_type` | `Gateway`/`Sensor`. |
| `name` | `twin_entities.name` | Cột nullable → giữ `null`. |
| `gateway_id` | `twin_entities.gateway_id` | Parent scope (NOT NULL). |
| `updated_at` | `twin_entities.updated_at` | NOT NULL. |

- Thứ tự keyset: `entity_id COLLATE "C" ASC` (khớp `gateway_id COLLATE "C"` của admin
  inventory, `src/internal/gateway/postgres_repository.go`). Không dùng `twin_entities.id`
  (internal UUID) hay offset làm contract public.

### 9.2. Cursor list — unsigned, versioned, base64url

```text
after = "v1." + base64url_nopad(UTF8 JSON)
JSON  = {"k":"<entity_id cuối trang>","g":"<gateway_id filter>","t":"<entity_type filter>"}
```

- `v1.` là **version**; thiếu prefix/version khác → `400`.
- `base64url_nopad`: RFC 4648 §5 (bảng URL `-`/`_`), **không** padding `=`.
- JSON canonical, đúng ba key; `g`/`t` = `null` khi request không có filter tương ứng.

**Bound và validation** (mọi vi phạm → `400 invalid_cursor`, không leak nội dung decode):

| Bound | Giá trị | Vi phạm |
| --- | --- | --- |
| Version prefix | `v1.` | `400` |
| Độ dài `after`/`next_cursor` (encoded, gồm `v1.`) | `<= 768` byte | `400` |
| Độ dài JSON decoded | `<= 512` byte | `400` |
| Key JSON | đúng `{k, g, t}` | `400` |
| `k` | URN public hợp lệ (7.2), type khớp filter | `400` |
| base64url/padding/JSON | decode + parse được | `400` |
| Query string raw | `<= 1024` byte | `400` |
| `limit` | `1..200` | `400` |

> **Reconciliation độ dài cursor (4.0.6):** bound cũ (encoded `<= 512`, JSON decoded
> `<= 256`) không đủ cho URN Sensor dài: `{"k":"urn:ngsi-ld:Sensor:<gw64>:<sid64>",
> "g":"<gw64>","t":"Sensor"}` decoded tới **240 byte** (vẫn < 256), nhưng cursor
> **quan hệ** (9.4) `{"s":"urn:ngsi-ld:Gateway:<gw64>","r":"hasSensor","k":"urn:ngsi-ld:
> Sensor:<gw64>:<sid64>"}` decoded tới **263 byte** — vượt 256. Vì vậy bound JSON
> decoded tăng lên **512** và bound encoded tăng lên **768** (đủ cho JSON 512 byte +
> prefix `v1.`, base64url); worst case thực tế vẫn < 360 byte encoded (đo tại 4.0.6, mục
> 11.4).

**Filter binding** (cursor phải khớp filter request hiện tại):

- Request không filter → cursor `g=null, t=null`.
- Request `entity_type=Sensor` → cursor `t="Sensor"`.
- Request `gateway_id=gateway_001` → cursor `g="gateway_001"`.
- Không khớp (đổi filter giữa hai trang) → `400 invalid_cursor`.

**Cursor KHÔNG phải authority:** sau decode, query vẫn scope `user_gateways` theo
principal; cursor do user/filter khác tạo không mở rộng quyền (filter bị re-validate).
**Mỗi page kiểm membership lại**, không tin cursor.

**Không cam kết snapshot pagination:** nếu entity/membership thay đổi giữa hai request,
kết quả có thể chèn/mất/đổi giữa các trang; cursor chỉ bảo đảm tiếp tục sau `k` theo đúng
filter, **không** phải ảnh chụp cố định.

Cursor Stage 4 (**versioned base64url**) khác `after` admin inventory (`gateway_id` thô,
validate identifier). Contract admin `GET /v1/admin/gateways` **không** đổi.

### 9.3. Detail `GET /v1/digital-twins/{entity_id}`

- `{entity_id}` URL-encoded, decode một lần, validate/parse/resolve URN và membership theo
  7.2; ngoài quyền **hoặc** không tồn tại → `404 not_found`. Internal UUID không trả ra.
- Read-only; **không** `FOR UPDATE`, không dựng current từ history.
- Slice chỉ có quan hệ `hasSensor`.

**Query phân trang quan hệ** (unknown/lặp → `400`):

| Param | Bắt buộc | Kiểu | Default | Ràng buộc |
| --- | --- | --- | --- | --- |
| `relationship_limit` | Không | int base-10 | `50` | `1..200`. |
| `relationship_after` | Không | string | — | Cursor **quan hệ** v1 (9.4); sai → `400 invalid_cursor`. |

**Detail projection** (entity JSON-LD — chốt ở 4.0.6, mục 11.1–11.3):

| Member | Representation | Nguồn authoritative | Ghi chú |
| --- | --- | --- | --- |
| `@context` | inline object | hằng số versioned (11.3) | Không fetch remote context. |
| `id` | JSON-LD `id` | `twin_entities.entity_id` | URN public ổn định. |
| `type` | JSON-LD `type` | `twin_entities.entity_type` | Lấy từ URN (7.2). |
| `name` | `Property` | Gateway: `gateways.name`; Sensor: `sensors.name` | `value` = tên; `modifiedAt` = `twin_entities.updated_at`. |
| `description` | `Property` (chỉ Gateway) | `gateways.description` | `value` nullable → giữ `null`; cột Gateway authoritative. |
| `unit` | `Property` (chỉ Sensor) | `sensors.unit` | `value` = unit **symbol** (không phải `unitCode`); nullable → `null`. |
| `gateway_id` | JSON string (scope) | `twin_entities.gateway_id` | Parent scope; không phải Property. |
| `hasSensor` | mảng `Relationship` (chỉ Gateway) | `twin_relationships` JOIN `twin_entities` target | Mỗi object: `type`, `object` (URN), `createdAt`. Trang lại ở 9.4. |
| `relationship_page` | JSON object | suy ra từ page | Metadata phân trang (9.3). |

- Timestamp NGSI-LD: attribute metadata dùng `modifiedAt` = server write time
  (`twin_entities.updated_at`); object `hasSensor` dùng `createdAt` =
  `twin_relationships.created_at`. **Không** có `observedAt` cho metadata entity vì
  không phải measurement (11.1).
- List **không** tự nhúng toàn bộ Sensor graph; detail chỉ trả **một trang** `hasSensor`
  cùng metadata phân trang.
- Identity (`id`, `type`, `gateway_id`) và nguồn dữ liệu trùng list summary/state; shape
  và context xem mục 11.

**Envelope detail** (ví dụ `relationship_limit=1`; response `application/ld+json`):

```json
{
  "@context": {
    "@version": 1.1,
    "id": "@id",
    "type": "@type",
    "Property": "https://uri.etsi.org/ngsi-ld/Property",
    "Relationship": "https://uri.etsi.org/ngsi-ld/Relationship",
    "value": "https://uri.etsi.org/ngsi-ld/hasValue",
    "createdAt": "https://uri.etsi.org/ngsi-ld/createdAt",
    "modifiedAt": "https://uri.etsi.org/ngsi-ld/modifiedAt",
    "name": "https://example.org/ngsi-ld/iot/v1#name",
    "description": "https://example.org/ngsi-ld/iot/v1#description",
    "unit": "https://example.org/ngsi-ld/iot/v1#unit",
    "measurement": "https://example.org/ngsi-ld/iot/v1#measurement",
    "sampleIntervalUs": "https://example.org/ngsi-ld/iot/v1#sampleIntervalUs",
    "hasSensor": "https://example.org/ngsi-ld/iot/v1#hasSensor",
    "gateway_id": "https://example.org/ngsi-ld/iot/v1#gatewayId"
  },
  "id": "urn:ngsi-ld:Gateway:gateway_001",
  "type": "Gateway",
  "name": { "type": "Property", "value": "AM5728 Gateway 001", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "description": { "type": "Property", "value": null, "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "gateway_id": "gateway_001",
  "hasSensor": [
    { "type": "Relationship", "object": "urn:ngsi-ld:Sensor:gateway_001:sensor_001", "createdAt": "2026-10-15T08:30:00.000000Z" }
  ],
  "relationship_page": {
    "relationship_type": "hasSensor",
    "limit": 1,
    "count": 1,
    "partial": true,
    "next_cursor": "v1.eyJzIjoidXJuOm5nc2ktbGQ6R2F0ZXdheTpnYXRld2F5XzAwMSIsInIiOiJoYXNTZW5zb3IiLCJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDEifQ"
  }
}
```

- `@context` inline xuất hiện **một lần** ở đầu entity; không lặp lại trong từng Property.
- `count` = số relationship **trả trong trang này**.
- `partial=true` khi còn quan hệ chưa trả (`next_cursor != null`); `false` khi đã hết.
- **Không silent truncate:** còn `hasSensor` ngoài `relationship_limit` thì `next_cursor`
  bắt buộc có và `partial=true`; API không bỏ phần còn lại.
- `next_cursor` trong ví dụ là **encoding thật** của `{"s","r","k"}` (giải mã được, mục
  9.4), không phải placeholder.

### 9.4. Cursor quan hệ — tách khỏi cursor list

```text
relationship_after = "v1." + base64url_nopad(UTF8 JSON)
JSON               = {"s":"<source entity_id URN>","r":"hasSensor","k":"<target entity_id URN cuối trang>"}
```

| Bound | Giá trị | Vi phạm |
| --- | --- | --- |
| Version prefix | `v1.` | `400` |
| Độ dài encoded (gồm `v1.`) | `<= 768` byte | `400` |
| Độ dài JSON decoded | `<= 512` byte | `400` |
| Key JSON | đúng `{s, r, k}` | `400` |
| `s` | = URN public entity detail hiện tại | `400` |
| `r` | = `hasSensor` (allowlist) | `400` |
| `k` | URN public hợp lệ (7.2) | `400` |

- **Binding:** `s` ràng buộc source entity, `r` ràng buộc loại quan hệ; đổi entity/loại
  giữa hai trang → `400 invalid_cursor`.
- Cursor quan hệ **tách namespace** cursor list (khác key); không dùng lẫn. Cursor vẫn
  **không phải authority**: membership entity source kiểm lại mỗi page.

**Keyset quan hệ** — thứ tự `(relationship_type, target public URN)`:

```sql
SELECT tgt.entity_id
FROM twin_relationships re
JOIN twin_entities tgt ON tgt.id = re.target_entity_id
WHERE re.source_entity_id = $src
  AND re.relationship_type = 'hasSensor'
  AND ($after IS NULL
       OR (re.relationship_type COLLATE "C", tgt.entity_id COLLATE "C") > ($r, $k))
ORDER BY re.relationship_type COLLATE "C", tgt.entity_id COLLATE "C" ASC
LIMIT $relationship_limit + 1
```

- PK `(source_entity_id, relationship_type, target_entity_id)` hỗ trợ prefix `source` cố
  định; index `idx_twin_relationships_target (target_entity_id, relationship_type)` là cho
  chiều tra ngược.
- Order dùng `COLLATE "C"`; PK/index hiện dùng collation mặc định → là **caveat query-plan**
  ghi limitation, không đổi ngữ nghĩa (đo ở Task 4.2/4.3, không thêm index ở 4.0.4).

### 9.5. State `GET /v1/digital-twins/{entity_id}/state`

Chín cột `twin_states` là authoritative: `entity_id`, `reported_state`, `desired_state`,
`reported_version`, `desired_version`, `last_reported_at`, `last_desired_at`, `updated_at`,
`last_desired_by`. Response **chỉ** map các cột này (không bịa field audit khác):

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "type": "Sensor",
  "gateway_id": "gateway_001",
  "reported_state": { },
  "desired_state": { },
  "reported_version": "8",
  "desired_version": "0",
  "last_reported_at": "2026-08-21T10:15:01.250000Z",
  "last_desired_at": null,
  "updated_at": "2026-10-15T09:30:00.000000Z"
}
```

- **BIGINT → chuỗi thập phân** (đồng nhất 8.7): `reported_version`, `desired_version`.
  Số bounded (`count`, `limit`) vẫn JSON number.
- **Audit:** cột duy nhất ngoài timestamp là `last_desired_by` (UUID profile). Contract
  **không** trả cột này trong slice (định danh principal khác; không cần cho read API) —
  biểu diễn actor để xem lại khi có nhu cầu. Timestamp trả: `last_reported_at`,
  `last_desired_at`, `updated_at`.
- Đọc current **trực tiếp** từ `twin_states`, không scan history, không `FOR UPDATE`.

**Bảo toàn số nguyên JSONB server-side:**

- `reported_state`/`desired_state` trả **nguyên bytes JSONB** (`json.RawMessage`); server
  **không** decode vào `map[string]any`/`float64` rồi re-encode, để số nguyên lớn (ví dụ
  `sequence_num` int64 `> 2^53`) giữ đúng.
- **Cảnh báo Flutter web:** `JSON.parse` (Number IEEE-754) mất chính xác JSON number
  `> 2^53`. Client đọc số lớn từ **string field** (`reported_version`, `desired_version` và
  BIGINT khác theo 8.7); với số nguyên **nhúng trong** `reported_state`/`desired_state`,
  contract **không cam kết** Flutter giữ chính xác mọi JSON number sau JS parse. Client giữ
  kiểu JSON state, không re-encode.

**No observation ≠ empty object ≠ SQL NULL (không có Gateway online giả):**

| Trạng thái | Storage | Ý nghĩa |
| --- | --- | --- |
| `reported_state`/`desired_state` = `{}` | JSONB **NOT NULL DEFAULT '{}'** | Chưa có property nào (object rỗng), **khác** `null`. |
| `last_reported_at`/`last_desired_at` = `null` | Cột nullable | Chưa từng có observation/desired change. |
| Sensor chưa có telemetry | `reported_state={}`, `reported_version=0`, `last_reported_at=null` | **Không** bịa `value`/`observed_at`/measurement time. |
| desired chưa đặt | `desired_state={}`, `desired_version=0`, `last_desired_at=null` | Không tạo giá trị giả. |
| Gateway | `reported_state` **không** được telemetry ghi | Telemetry chỉ ghi reported_state của **Sensor** (key `value`, `observed_at`, `boot_id`, `sequence_num`, `sample_interval_us`, `unit` optional — `postgres_reported_state.go` `merge()`). Không có property `connectionStatus`/online → **Gateway không tự online** từ telemetry Sensor. |

- Timestamp response từ **cột state** (`last_reported_at`, `last_desired_at`, `updated_at`),
  **không** lấy request time. `observed_at` nhúng trong `reported_state` là measurement time
  do writer ghi và có thể **khác** `last_reported_at` (server write time).

### 9.6. Examples (JSON parseable; DESIGN ONLY)

Các token cursor trong ví dụ là **encoding thật** của payload đã định nghĩa (9.2/9.4) và
giải mã được; URN/timestamp là giá trị giả định.

**(a) List hai page** — `GET /v1/digital-twins?entity_type=Sensor&limit=2` (summary
projection, `application/json`):

Page 1:

```json
{
  "items": [
    { "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001", "type": "Sensor", "name": "Temperature", "gateway_id": "gateway_001", "updated_at": "2026-10-15T09:00:00.000000Z" },
    { "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_002", "type": "Sensor", "name": "Humidity", "gateway_id": "gateway_001", "updated_at": "2026-10-15T09:00:00.000000Z" }
  ],
  "next_cursor": "v1.eyJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIiLCJnIjpudWxsLCJ0IjoiU2Vuc29yIn0"
}
```

Page 2 — `GET /v1/digital-twins?entity_type=Sensor&limit=2&after=v1.eyJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIiLCJnIjpudWxsLCJ0IjoiU2Vuc29yIn0`
(`k` = `urn:ngsi-ld:Sensor:gateway_001:sensor_002`, `g=null`, `t=Sensor`):

```json
{
  "items": [
    { "id": "urn:ngsi-ld:Sensor:gateway_002:sensor_001", "type": "Sensor", "name": "Pressure", "gateway_id": "gateway_002", "updated_at": "2026-10-15T09:05:00.000000Z" }
  ],
  "next_cursor": null
}
```

**(b) Detail hai page quan hệ** — `GET /v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agateway_001?relationship_limit=2`
(Gateway có 3 `hasSensor`; response `application/ld+json`). Hai page mang `@context`
inline **giống hệt 9.3**; page 2 lược `@context` cho gọn.

Page 1:

```json
{
  "@context": {
    "@version": 1.1,
    "id": "@id",
    "type": "@type",
    "Property": "https://uri.etsi.org/ngsi-ld/Property",
    "Relationship": "https://uri.etsi.org/ngsi-ld/Relationship",
    "value": "https://uri.etsi.org/ngsi-ld/hasValue",
    "createdAt": "https://uri.etsi.org/ngsi-ld/createdAt",
    "modifiedAt": "https://uri.etsi.org/ngsi-ld/modifiedAt",
    "name": "https://example.org/ngsi-ld/iot/v1#name",
    "description": "https://example.org/ngsi-ld/iot/v1#description",
    "unit": "https://example.org/ngsi-ld/iot/v1#unit",
    "measurement": "https://example.org/ngsi-ld/iot/v1#measurement",
    "sampleIntervalUs": "https://example.org/ngsi-ld/iot/v1#sampleIntervalUs",
    "hasSensor": "https://example.org/ngsi-ld/iot/v1#hasSensor",
    "gateway_id": "https://example.org/ngsi-ld/iot/v1#gatewayId"
  },
  "id": "urn:ngsi-ld:Gateway:gateway_001",
  "type": "Gateway",
  "name": { "type": "Property", "value": "AM5728 Gateway 001", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "description": { "type": "Property", "value": null, "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "gateway_id": "gateway_001",
  "hasSensor": [
    { "type": "Relationship", "object": "urn:ngsi-ld:Sensor:gateway_001:sensor_001", "createdAt": "2026-10-15T08:30:00.000000Z" },
    { "type": "Relationship", "object": "urn:ngsi-ld:Sensor:gateway_001:sensor_002", "createdAt": "2026-10-15T08:31:00.000000Z" }
  ],
  "relationship_page": {
    "relationship_type": "hasSensor",
    "limit": 2,
    "count": 2,
    "partial": true,
    "next_cursor": "v1.eyJzIjoidXJuOm5nc2ktbGQ6R2F0ZXdheTpnYXRld2F5XzAwMSIsInIiOiJoYXNTZW5zb3IiLCJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIifQ"
  }
}
```

Page 2 — `…&relationship_after=v1.eyJzIjoidXJuOm5nc2ktbGQ6R2F0ZXdheTpnYXRld2F5XzAwMSIsInIiOiJoYXNTZW5zb3IiLCJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIifQ`
(`s` = Gateway URN, `r` = `hasSensor`, `k` = `urn:ngsi-ld:Sensor:gateway_001:sensor_002`):

```json
{
  "id": "urn:ngsi-ld:Gateway:gateway_001",
  "type": "Gateway",
  "name": { "type": "Property", "value": "AM5728 Gateway 001", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "description": { "type": "Property", "value": null, "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "gateway_id": "gateway_001",
  "hasSensor": [
    { "type": "Relationship", "object": "urn:ngsi-ld:Sensor:gateway_001:sensor_003", "createdAt": "2026-10-15T08:32:00.000000Z" }
  ],
  "relationship_page": {
    "relationship_type": "hasSensor",
    "limit": 2,
    "count": 1,
    "partial": false,
    "next_cursor": null
  }
}
```

**(c) Sensor chưa có observation** — `/state`: empty object **khác** `null`, không bịa
measurement time:

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_009",
  "type": "Sensor",
  "gateway_id": "gateway_001",
  "reported_state": {},
  "desired_state": {},
  "reported_version": "0",
  "desired_version": "0",
  "last_reported_at": null,
  "last_desired_at": null,
  "updated_at": "2026-10-15T09:00:00.000000Z"
}
```

**(d) `desired != reported` và version `> 2^53`** — `/state`; `reported_version` là chuỗi,
`sequence_num` nhúng là JSON number lớn (client đọc string field cho version):

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "type": "Sensor",
  "gateway_id": "gateway_001",
  "reported_state": { "value": 25.2, "observed_at": "2026-08-21T10:15:01.250000Z", "boot_id": "7f2c45f7-0a76-4e28-8a37-7793d7d85b04", "sequence_num": 9007199254740993, "sample_interval_us": 10000, "unit": "degC" },
  "desired_state": { "sample_interval_us": 5000000 },
  "reported_version": "9007199254740993",
  "desired_version": "5",
  "last_reported_at": "2026-08-21T10:15:01.250000Z",
  "last_desired_at": "2026-10-15T09:30:00.000000Z",
  "updated_at": "2026-10-15T09:30:00.000000Z"
}
```

**(e) Legacy provenance NULL trong JSONB opaque** — reader trả nguyên `null`, không thay
`0`/UUID giả (đối chiếu 8.7; raw/temporal legacy NULL minh họa ở 8.11(a)):

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_010",
  "type": "Sensor",
  "gateway_id": "gateway_001",
  "reported_state": { "value": 25.0, "boot_id": null },
  "desired_state": {},
  "reported_version": "1",
  "desired_version": "0",
  "last_reported_at": "2026-08-21T10:20:00.000000Z",
  "last_desired_at": null,
  "updated_at": "2026-08-21T10:20:00.000000Z"
}
```

### 9.7. Ràng buộc nguồn cho 4.0.4

- `twin_entities` (migration 2/6/7/8): `entity_id UNIQUE` (key keyset), `entity_type`,
  `name` NULL, `gateway_id NOT NULL` FK; index `idx_twin_entities_gateway_type
  (gateway_id, entity_type)`. Internal `id` UUID không trả ra.
- Order `COLLATE "C"` khớp admin inventory (`postgres_repository.go`) — không dùng default
  collation cho keyset list.
- `twin_relationships` PK `(source_entity_id, relationship_type, target_entity_id)` +
  `idx_twin_relationships_target (target_entity_id, relationship_type)` (migration 2); PK
  phục vụ prefix `source` cố định; collation PK mặc định vs `COLLATE "C"` là caveat
  query-plan ghi limitation.
- `twin_states` đúng chín cột (migration 2 + `last_desired_by` migration 6/8) — không có
  audit field khác; contract không bịa.
- Sensor identity ghép (`sensors` PK `(gateway_id, sensor_id)`, mục 2.3/5.1): detail Sensor
  lấy `name`/`unit` từ `sensors`; **không** có `sensors.id`.
- Gateway detail lấy `name`/`description` từ `gateways`; telemetry chỉ ghi reported_state
  của Sensor (`src/internal/telemetry/postgres_reported_state.go` `merge()`) → Gateway
  reported_state không bị telemetry ghi, không suy diễn online.
- Cross-ref, không lặp: BIGINT/NULL/JSON-number → 8.7; time/bucket/raw → 8.2–8.6; URN
  resolve/encode → 7.2; quyền → 7.3.

---

## 10. Contract Task 4.0.5 — DTO POST/PATCH metadata

Bước **4.0.5** chốt DTO mutation metadata cho hai endpoint **#6** `POST /v1/digital-twins`
và **#7** `PATCH /v1/digital-twins/{entity_id}` (đánh số theo mục 7.1). Toàn bộ mục này
là **thiết kế**: cả hai route **chưa** được đăng ký trong `router.go` (mục 1.5) và không
được coi là khả dụng; Task 4.0 **không** sửa provisioning, không thêm code/migration và
không thực thi HTTP write nào. Field/type/length dưới đây **lấy từ validator provisioning
hiện có** (mục 5.2–5.4, mục 10.8), không tự đặt lại. Quyền dùng lại ma trận 7.4; identity
URN dùng lại 7.2; status/error dùng lại 7.5; BIGINT/NULL dùng lại 8.7. Phần **#6** tái
dùng **đúng** transaction provisioning Stage 2 (`PUT`
admin/owner) chứ không tạo một pipeline ghi thứ hai.

### 10.1. Field POST theo entity type

Body là **JSON object** với **allowlist key**; identity do server derive — client
**không** gửi URN/`entity_id`/UUID nội bộ (7.2). `entity_type` quyết định nhánh DTO.

**`POST /v1/digital-twins` (`entity_type=Gateway`)** — nguồn validator:
`ValidateProvisionGatewayInput` + `parseGatewayProvisioning`
(`src/internal/gateway/provisioning_model.go`, `src/internal/httpserver/provisioning_request.go`):

| Field | Kiểu | Bắt buộc | Null | Vắng (omitted) | Ràng buộc (từ source) |
| --- | --- | --- | --- | --- | --- |
| `entity_type` | string | Có | Không (null → `400`) | `400` | Phải **đúng** `"Gateway"`; token khác → `400`. |
| `gateway_id` | string | Có | Không | `400` | `ValidateGatewayID`: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, **khác** `backend_service`; ≤64 ký tự; không normalize/unescape. |
| `name` | string | Có | Không | `400` | Không rỗng sau `TrimSpace`; **≤256 byte**; UTF-8 hợp lệ; **không** chứa NUL. Giá trị **không** bị trim khi lưu. |
| `description` | string | Không | **Có** | = SQL `NULL` | Nếu có: **≤4096 byte**, UTF-8 hợp lệ, không NUL. `null` tường minh → SQL `NULL`. |
| `owner_user_id` | string (UUID) | Có | Không | `400` | Parse UUID hợp lệ (sai → `400`); profile **phải tồn tại** trong `profiles` (thiếu → `404`). Server derive membership `owner`. |

**`POST /v1/digital-twins` (`entity_type=Sensor`)** — nguồn validator:
`ValidateProvisionSensorInput` + `parseSensorProvisioning`:

| Field | Kiểu | Bắt buộc | Null | Vắng | Ràng buộc (từ source) |
| --- | --- | --- | --- | --- | --- |
| `entity_type` | string | Có | Không | `400` | Phải **đúng** `"Sensor"`. |
| `gateway_id` | string | Có | Không | `400` | `ValidateGatewayID`; Gateway cha phải tồn tại (thiếu → `404`). |
| `sensor_id` | string | Có | Không | `400` | `ValidateSensorID`: cùng regex, **không** reserved name. Sensor chỉ duy nhất trong Gateway cha (2.3). |
| `name` | string | Có | Không | `400` | Như trên (`≤256` byte, non-empty sau trim, không NUL). |
| `unit` | string | Không | **Có** | = SQL `NULL` | Nếu có: **≤64 byte**, UTF-8 hợp lệ, không NUL; `null` → SQL `NULL`. |

- **Owner tồn tại + derive URN:** Gateway yêu cầu `owner_user_id` trỏ tới profile thật;
  repository kiểm trong **cùng transaction** (`SELECT id FROM public.profiles WHERE id=$1`,
  thiếu → `ErrProvisioningNotFound` → `404`). Server tự sinh
  `urn:ngsi-ld:Gateway:<gateway_id>` / `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`
  (`mapper.GatewayEntityID`/`SensorEntityID`) — client **không** cung cấp `entity_id`.
- Sensor **không** nhận `owner_user_id`/`description`; Gateway **không** nhận `sensor_id`/`unit`.

### 10.2. POST reuse transaction provisioning + exact replay

**#6 tái dùng đúng hai transaction Stage 2** (`ProvisionGateway`,
`ProvisionSensor`/`ProvisionSensorForOwner`, mục 5.4): cùng bước ghi, cùng bất biến, cùng
thứ tự khóa. Không thêm graph/relationship/credential nào mới.

**Gateway transaction** (so `src/internal/gateway/postgres_gateway_provisioning.go`):

1. `BeginTx` ReadCommitted.
2. Kiểm **platform admin** qua `platform_admins` **trong transaction**; non-admin → `403`.
3. Kiểm `owner_user_id` tồn tại → thiếu `404`.
4. `INSERT INTO gateways ... ON CONFLICT (gateway_id) DO NOTHING RETURNING created_at`.
5. **Tạo mới:** `user_gateways(role='owner')` + `twin_entities('Gateway')` + `twin_states`.
6. **Replay:** `verifyGatewayRetry` (xem dưới).
7. `Commit`.

**Sensor transaction:** nhánh admin (kiểm admin) **hoặc** nhánh owner (khóa Gateway cha
`FOR UPDATE`, `user_gateways FOR SHARE`, role `owner`) theo thứ tự an toàn 7.4; resolve
parent twin; `INSERT INTO sensors ON CONFLICT (gateway_id,sensor_id) DO NOTHING`; tạo mới →
`twin_entities('Sensor')` + `twin_states` + `twin_relationships('hasSensor')`; replay →
`verifySensorRetry`.

**Exact replay fields (đọc từ source — không suy diễn):**

| Entity | Field so sánh replay | Quy tắc |
| --- | --- | --- |
| Gateway | `name` | Khớp **chính xác** chuỗi. |
| Gateway | `description` | Nullable **so chính xác** (`sameNullableString`: cùng `NULL` hoặc cùng chuỗi). |
| Gateway | **owner membership** | Gateway row **không** có cột owner; replay đọc `user_gateways` với `role='owner'` và so **đúng một** owner + `user_id = owner_user_id`. Khác owner → **`409`**. |
| Gateway | Twin graph | Đúng **một** Gateway entity khớp URN + `entity_type` + `gateway_id` + `name` và có hàng `twin_states`; state **không** bị so/ghi. |
| Sensor | `name` | Khớp chính xác. |
| Sensor | `unit` | Nullable so chính xác. |
| Sensor | Twin graph | Đúng **một** Sensor entity khớp URN + type + `gateway_id` + `name` + relationship `hasSensor` từ parent + hàng `twin_states`; state **không** bị so/ghi. |

→ **Lưu ý owner:** vì replay so **membership**, gửi cùng `name`/`description` nhưng khác
`owner_user_id` là **conflict đại diện** (`409`), không phải no-op.

**Status `#6`:** `201 Created` khi tạo mới; `200 OK` khi **exact replay** (no-op);
`409 conflict` khi cùng identity nhưng đại diện khác (`name`/`description`/`unit`/owner);
`404` khi owner profile (Gateway) hoặc Gateway cha (Sensor) không tồn tại; `403` thiếu
capability; `400/413/415` input/body/media; `503` admin-lookup/timeout/unavailable;
graph **bất nhất** (`ErrProvisioningInconsistent`) → `500` (không phải `409`), khớp mapping
handler hiện có (mục 5.4).

**Không side effect credential/account:** provisioning chỉ ghi `gateways`,
`user_gateways`, `twin_entities`, `twin_states`, `sensors`, `twin_relationships`. **Không**
tạo `gateway_mqtt_credentials`/`gateway_mqtt_credential_events`, **không** tạo
`auth.users`/profile human, **không** phát MQTT. MQTT credential vẫn chỉ qua route admin
riêng (Stage 2); `#6` **không** sinh credential hay account.

### 10.3. PATCH allowlist (`#7`)

Body là JSON object allowlist; `{entity_id}` URL-encoded (7.2), validate/parse/resolve URN
và membership theo 7.4 (`PATCH` Gateway: admin-only; `PATCH` Sensor: admin-hoặc-owner).
Query phải rỗng.

| Field | Entity | Kiểu | Bắt buộc | Null | Vắng | Ràng buộc |
| --- | --- | --- | --- | --- | --- | --- |
| `name` | Gateway/Sensor | string | Không | **Không** (null → `400`) | giữ nguyên | Không rỗng sau `TrimSpace`, `≤256` byte, UTF-8 hợp lệ, không NUL; ghi `twin_entities.name` **và** cột `gateways.name`/`sensors.name`. |
| `description` | **Gateway only** | string | Không | **Có** | giữ nguyên | `null` → **xóa** (SQL `NULL`); nếu có chuỗi: `≤4096` byte, UTF-8 hợp lệ, không NUL; ghi `gateways.description`. Sensor gửi `description` → `400` (unknown field). |

- **Omitted = unchanged:** field không có trong body **không** bị ghi đè (không set `NULL`,
  không set giá trị mặc định).
- **`name` không NULL/rỗng:** `null` hoặc rỗng (sau trim) → `400`.
- **`description` NULL để xóa:** chỉ Gateway; `null` tường minh là **clear** hợp lệ.
- **Empty patch bị từ chối:** body `{}` (không có field allowlist nào) → `400`
  `invalid_request` (không có gì để đổi).
- **Same-value no-op:** gửi đúng giá trị hiện tại → **no-op**, không ghi row, không đổi
  `updated_at`, **không** đổi state/version; trả `200`.
- **`unit` create-only:** PATCH **không** đổi `unit` (raw history dùng `unit` tại thời điểm
  ghi; đổi unit của history là **ngoài** Stage 4). Gửi `unit` trong PATCH → `400` (unknown
  field). Muốn `unit` khác phải tạo Sensor mới (create-only) — không backfill.

### 10.4. Field bị từ chối + strict JSON (`#6` và `#7`)

DTO là **allowlist**, **không** nhận arbitrary JSON-LD để tạo graph. Mọi key ngoài allowlist
→ `400 invalid_request`:

- **Identity/type/ownership (immutable):** `entity_id`, `id`, `entity_type` (PATCH),
  `gateway_id`/`sensor_id` (PATCH), `owner_user_id`/`user_id`, UUID nội bộ `twin_entities.id`.
- **Attributes/relationships tùy ý:** `attributes`, `hasSensor`, `relationships`, mọi
  JSON-LD/Property/Relationship thô.
- **State/version:** `reported_state`, `desired_state`, `reported_version`,
  `desired_version`, `last_reported_at`/`last_desired_at`/`last_desired_by`, `updated_at`,
  `sample_interval_us`.
- **Membership/credential:** `role`, `memberships`, `credentials`, `mqtt_*`, `password`,
  bất kỳ field DynSec/auth.

**Giữ nguyên strict JSON conventions** (`parseProvisioningObject`, mục 5.3):

| Vi phạm | Kết quả |
| --- | --- |
| Query string khác rỗng (`RawQuery != "" || ForceQuery`), kể cả param không hỗ trợ | `400` |
| `Content-Type` khác `application/json` hoặc charset khác `utf-8` | `415` |
| Body vượt bound (design **16 KiB**, chốt ở 4.0.6) | `413` |
| Top-level **không** phải object | `400` |
| **Key trùng** (duplicate) hoặc **key lạ** (unknown) | `400` |
| **Trailing document** sau object (JSON thứ hai) | `400` |
| Field non-nullable nhận `null`; field bắt buộc thiếu | `400` |
| Sai kiểu JSON (number/bool/array chỗ string) | `400` |
| NUL, lone surrogate, `\u0000`, Unicode không hợp lệ trong string | `400` |

- Query `user_id`/`role`/identity → `400`; **không** dùng làm căn cứ cấp quyền (7.3/7.4).

### 10.5. Invariants cho task implementation sau

- **Kiểm quyền trong transaction:** admin kiểm `platform_admins` **trong cùng transaction**
  (`NewPostgresPlatformAdminChecker(tx)`); owner kiểm membership `FOR SHARE` **trong
  transaction** như `ProvisionSensorForOwner`. Không nhận role từ query/body/cursor.
- **Metadata nhất quán:** `name` ghi **đồng thời** `twin_entities.name` **và** cột
  `gateways.name`/`sensors.name`; `twin_entities.updated_at` cập nhật theo server write time.
  `description` **chỉ** ở `gateways.description` (authoritative, mục 9.3), **không** lưu
  trong `twin_entities`. `unit` **chỉ** ở `sensors.unit`.
- **Field không gửi không bị ghi đè:** chỉ ghi field **có mặt** trong body; các cột khác
  giữ nguyên giá trị hiện tại.
- **Không side effect state/temporal/command:** metadata PATCH **không** tăng
  `reported_version`/`desired_version`, **không** ghi `twin_temporal_values`, **không** tạo
  `twin_commands`/`twin_outbox`, **không** chạm `sample_interval_us`.
- **Same-value là no-op;** ghi đè cùng field **đồng thời** dùng **last-write-wins** (đã
  công bố) — metadata **không** có optimistic version; không thêm cơ chế version cho metadata.
- **Lock order nhất quán** với provisioning (mục 3, điểm 3): PATCH Sensor khóa parent/row theo
  cùng thứ tự; tránh deadlock với provisioning/telemetry.
- **Không mutation journal / `Idempotency-Key` mới cho metadata:** idempotency dựa vào
  create-or-exact-replay (`#6`) và same-value no-op (`#7`), không mở framework mới.

### 10.6. Response metadata của mutation

- Response trả **metadata capability đã cấp**, **không** nhúng full `reported_state`/
  `desired_state`: `#6` Gateway → `gateway_id`, `name`, `description`, `owner_user_id`,
  `entity_id`, `created_at`; `#6` Sensor → `gateway_id`, `sensor_id`, `name`, `unit`,
  `entity_id`, `created_at`; `#7` → `id`/`type`/`gateway_id` + `name`/`description`/`unit` +
  `updated_at`. Đây là **mutation metadata view** (plain JSON, cùng nguồn field như detail
  9.3) — **không** kèm `reported_state`/`desired_state`, **không** `@context`, **không**
  relationship (11.2).
- **Admin non-member:** response mutation vẫn **chỉ metadata** — API **không** vô tình trả
  state/telemetry cho admin không có `user_gateways` (admin capability **không** tự cấp
  quyền đọc, 7.3).
- **Commit lỗi/bất định → lỗi, không success:** timeout/cancel/admin-lookup/unavailable →
  `503`; lỗi bất ngờ → `500`; commit owner bất định (`ErrOwnerProvisioningUnavailable`) →
  `503` (không trả `200/201` giả). Không lộ SQL/DSN/token/secret.
- Protected response `Cache-Control: no-store` theo middleware hiện có (1.4).

### 10.7. Examples (JSON parseable; placeholder; DESIGN ONLY)

Không có HTTP write nào được thực thi ở Task 4.0.

**(a) POST Gateway — request** (`entity_type":"Gateway"`):

```json
{
  "entity_type": "Gateway",
  "gateway_id": "gateway_001",
  "name": "AM5728 Gateway 001",
  "description": "Bàn giao tại phòng lab AM5728",
  "owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"
}
```

**(a2) POST Gateway — `201 Created`** (tạo mới):

```json
{
  "gateway_id": "gateway_001",
  "name": "AM5728 Gateway 001",
  "description": "Bàn giao tại phòng lab AM5728",
  "owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001",
  "entity_id": "urn:ngsi-ld:Gateway:gateway_001",
  "created_at": "2026-10-17T08:00:00.000000Z"
}
```

**(a3) POST Gateway — exact replay `200 OK`:** body **y hệt** (a), response **trùng** (a2)
trừ `created_at` giữ giá trị **gốc** đã tạo (không đổi). Đổi owner/`name`/`description` →
**(a4) `409 Conflict`**:

```json
{
  "error": {
    "code": "conflict",
    "message": "resource conflict",
    "request_id": "req_placeholder"
  }
}
```

**(b) POST Sensor — request** (`entity_type":"Sensor"`):

```json
{
  "entity_type": "Sensor",
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "name": "Temperature",
  "unit": "degC"
}
```

**(b2) POST Sensor — `201 Created`:**

```json
{
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "name": "Temperature",
  "unit": "degC",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "created_at": "2026-10-17T08:05:00.000000Z"
}
```

**(b3) POST Sensor — replay `200 OK`:** `name` và `unit` **khớp chính xác** (nullable so
chính xác; `unit:null` chỉ replay với `unit:null`). Lệch `name`/`unit` → `409` như (a4).

**(c) PATCH name — request** `PATCH /v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001`:

```json
{ "name": "Temperature (phòng lab)" }
```

**(c2) PATCH — `200 OK`** (metadata-only, không state):

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "type": "Sensor",
  "gateway_id": "gateway_001",
  "name": "Temperature (phòng lab)",
  "unit": "degC",
  "updated_at": "2026-10-17T08:10:00.000000Z"
}
```

**(d) PATCH clear description — request** (Gateway only; `null` = xóa):

```json
{ "description": null }
```

**(d2) PATCH — `200 OK`:**

```json
{
  "id": "urn:ngsi-ld:Gateway:gateway_001",
  "type": "Gateway",
  "gateway_id": "gateway_001",
  "name": "AM5728 Gateway 001",
  "description": null,
  "updated_at": "2026-10-17T08:12:00.000000Z"
}
```

**(e) Forbidden body** — gửi `desired_state`/`attributes`/`role`/unknown key → `400`:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "invalid request",
    "request_id": "req_placeholder"
  }
}
```

**(f) Role denial** — operator/viewer ghi Gateway (hoặc non-member) → `403` nếu có hàng
membership nhưng role không đủ; non-member Sensor → `404` (theo 7.4):

```json
{
  "error": {
    "code": "forbidden",
    "message": "access denied",
    "request_id": "req_placeholder"
  }
}
```

### 10.8. Ràng buộc nguồn cho 4.0.5

- Validator: `src/internal/gateway/identifier.go`
  (`ValidateGatewayID`/`ValidateSensorID`, regex `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`,
  cấm `backend_service`); `src/internal/gateway/provisioning_model.go`
  (`name` ≤256/non-empty, `description` ≤4096, `unit` ≤64, cả ba UTF-8 + non-NUL;
  `ValidateProvisionGatewayInput` đòi `owner_user_id != Nil`).
- Strict JSON: `src/internal/httpserver/provisioning_request.go`
  (`parseProvisioningObject`: query rỗng, `application/json`, `charset=utf-8`, body
  `limit+1`, `validJSONUnicode`, object top-level, duplicate/unknown/trailing, nullable vs
  non-nullable, required). Parser provisioning hiện đọc identity từ **path**; `#6` đưa
  identity vào **body** nhưng **tái dùng cùng** validator/allowlist semantics.
- Transaction/replay: `src/internal/gateway/postgres_gateway_provisioning.go`
  (`ProvisionGateway`, `verifyGatewayRetry`, `sameNullableString`),
  `src/internal/gateway/postgres_sensor_provisioning.go`
  (`ProvisionSensor`, `ProvisionSensorForOwner`, `executeSensorGraphProvisioning`,
  `verifySensorRetry`).
- Status mapping: `src/internal/httpserver/admin_gateway_handlers.go` (`provisionGateway`),
  `src/internal/httpserver/owner_sensor_handlers.go` (`provisionSensorForOwner`);
  uncertainty: `src/internal/gateway/postgres_owner_provisioning_errors.go`
  (`ErrOwnerProvisioningUnavailable`), `src/internal/gateway/postgres_provisioning_repository.go`
  (`ErrProvisioningAdminLookup`).
- URN: `src/internal/digitaltwin/mapper/identity.go`. Schema cột: mục 2.2/2.3/2.6/2.7/2.8.
- Cross-ref, không lặp: quyền → 7.4; URN/encode → 7.2; status/empty/error → 7.5, bảng
  error đầy đủ → 4.0.6; BIGINT/NULL/JSON-number → 8.7; collect source-of-truth → mục 2.

---

## 11. Contract Task 4.0.6 — representation mapper, context và HTTP/error dùng chung

Bước **4.0.6** chốt bảng mapping tập trung, một context dự án versioned inline, quy ước
`Content-Type`/header/request ID/CORS/bounds và bảng error status/code dùng chung. Toàn bộ là
**thiết kế**: không endpoint Stage 4 nào hoạt động; không thêm route/framework/migration/config;
quyền giữ nguyên 7.3–7.4; BIGINT/NULL/JSON-number giữ nguyên 8.7. Bước 4.0.7 rà tính đầy đủ
sau bước này.

### 11.1. Bảng mapping tập trung

Mapper **duy nhất** chuyển storage/source → public term; handler không tự đổi snake_case.
"View field" (snake_case) là field của response dạng *view* (summary/state/history/mutation
metadata); "public term" là tên attribute trong entity JSON-LD (detail).

| Storage / source | Key state view (`twin_states`) | Selector HTTP API history (`property=...`) | Public term mapper (Detail JSON-LD) | Kiểu attribute | JSON type | Unit | Nguồn `observedAt` / `modifiedAt` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `twin_entities.entity_id` | `id` | — | `id` | JSON-LD `@id` (keyword) | string (URN) | — | — |
| `twin_entities.entity_type` | `type` | — | `type` | JSON-LD `@type` (keyword) | `"Gateway"`/`"Sensor"` | — | — |
| `gateways.name` / `sensors.name` | — | — | `name` | `Property` | string | — | `modifiedAt`: `twin_entities.updated_at` |
| `gateways.description` | — | — | `description` | `Property` (Gateway) | string \| null | — | `modifiedAt`: `twin_entities.updated_at` |
| `sensors.unit` | — | — | `unit` | `Property` (Sensor) | string \| null | **symbol** (`degC`) | `modifiedAt`: `twin_entities.updated_at` |
| `twin_entities.gateway_id` | `gateway_id` | — | `gateway_id` | JSON field (scope) | string | — | — |
| `twin_relationships('hasSensor')` → target | — | — | `hasSensor` | `Relationship` | array\<Relationship\> | — | `createdAt`: `twin_relationships.created_at` |
| `telemetry.value` | `value` (bên trong `reported_state`) | `value` | `measurement` | `Property` | number | symbol từ `sensors.unit` | `observedAt`: `telemetry.measured_at` |
| `twin_temporal_values(property_name='sample_interval_us')` | `sample_interval_us` (trong `reported_state`) | `sample_interval_us` | `sampleIntervalUs` | `Property` | number | `metadata.unit` (`us`) | `observedAt`: `twin_temporal_values.observed_at` |
| `twin_states.reported_state` / `desired_state` | `reported_state` / `desired_state` (verbatim) | — | (opaque JSONB state) | JSON object verbatim | object | — | measurement time trong JSONB key `observed_at` / `updated_at` |
| `twin_states.reported_version` / `desired_version` | `reported_version` / `desired_version` | — | — | JSON field | string thập phân (BIGINT → string) | — | — |
| `twin_states.last_reported_at` / `last_desired_at` | `last_reported_at` / `last_desired_at` | — | — | JSON field (state view) | string RFC3339 µs \| null | — | — |
| `telemetry.measured_at` (raw point) | `measured_at` | — | — | protocol field | string RFC3339 µs | — | nguồn `observedAt` |
| `twin_temporal_values.observed_at` (point) | `observed_at` | — | — | protocol field | string RFC3339 µs | — | measurement time |

Ghi chú:

- **`observedAt` = measurement time** (`telemetry.measured_at`, `twin_temporal_values.observed_at`);
  **`modifiedAt` = server write time** (`twin_entities.updated_at`, `twin_relationships.created_at`,
  `twin_states.updated_at`). Hai nguồn **khác nhau**; response **không** lấy request time thay thế.
  Metadata entity (`name`/`description`/`unit`/`hasSensor`) **không** có `observedAt` vì không
  phải measurement.
- **`unit` symbol ≠ `unitCode`:** `unit` giữ symbol thô (`degC`, `us`); dự án **chưa** có mapping
  sang `unitCode` chuẩn nên **không** gọi symbol là `unitCode`.
- **BIGINT → string:** `sequence_num`, bucket `count`, `reported_version`/`desired_version` (8.7).
  **NULL giữ NULL:** `boot_id`/`message_id`/`sample_index` legacy và `unit`/`description` nullable —
  không thay zero/UUID giả.
- **Tách biệt HTTP Query Selector, State View Key và Mapper Public Term:**
  - `reported_state` là JSONB verbatim lưu trực tiếp cấu trúc telemetry uplink (`value`,
    `sample_interval_us`).
  - HTTP history API (`GET /v1/digital-twins/{entity_id}/history`) chấp nhận selector độc lập
    `property=value` và `property=sample_interval_us` theo đúng Stage 4 plan và Task 4.0 detail plan.
  - Mapper representation JSON-LD detail sử dụng public term `measurement` và `sampleIntervalUs`.
- **Lý do JSON-LD keyword nesting không làm đổi HTTP query selector:**
  Keyword JSON-LD `value` (`hasValue` / `@value`) là keyword bên trong một `Property` object
  (`{"type":"Property","value":...}`), nên thuộc tính cấp root của Sensor trong JSON-LD không thể
  đặt tên là `value` mà phải dùng public term `measurement` để tránh xung đột cấu trúc JSON-LD.
  Tuy nhiên, **JSON-LD keyword nesting hoàn toàn KHÔNG đòi hỏi phải đổi tên tham số query selector
  HTTP độc lập**. Tham số query HTTP `property=value` và `property=sample_interval_us` thuộc giao diện
  REST API URL, không lồng cấu trúc JSON-LD, do đó được giữ nguyên vẹn theo kế hoạch đã phê duyệt.

### 11.2. Representation theo response (chốt nhất quán list/detail/state/history/mutation)

| Endpoint | Shape | `Content-Type` | Identity | Attribute/relationship | Timestamp |
| --- | --- | --- | --- | --- | --- |
| #3 detail | **entity JSON-LD** | `application/ld+json; charset=utf-8` | `id`/`type`/`gateway_id` | `Property`/`Relationship` | `modifiedAt`/`createdAt` |
| #2 list | summary view | `application/json; charset=utf-8` | `id`/`type`/`gateway_id` | scalar `name` (= `name.value`) | `updated_at` |
| #4 state | state view | `application/json; charset=utf-8` | `id`/`type`/`gateway_id` | `reported_state`/`desired_state` verbatim | `last_*_at`/`updated_at` |
| #1/#5 history | series view | `application/json; charset=utf-8` | `gateway_id`/`sensor_id` hoặc `entity_id`/`property` | point list | `measured_at`/`observed_at`/`bucket_start` |
| #6/#7 mutation | mutation metadata view | `application/json; charset=utf-8` | `gateway_id`/`sensor_id`/`entity_id`/`id`/`type` | scalar metadata | `created_at`/`updated_at` |
| error | error envelope | `application/json; charset=utf-8` | — | `error{code,message,request_id}` | — |

- Mọi shape dùng **cùng public term và nguồn** (11.1); identity `id`/`type`/`gateway_id` giống
  nhau. Khác biệt `Property` (detail) vs scalar (view) là **quy ước có chủ đích** cho response
  dạng *view*, không phải hai định nghĩa dữ liệu mâu thuẫn.
- Server write time: detail dùng per-attribute `modifiedAt`; các view dùng field `updated_at` —
  **cùng nguồn** `twin_entities.updated_at`/`twin_states.updated_at`.
- **Không fake unobserved:** entity metadata nullable giữ `null` (Property `value: null` hoặc
  field `null`), không bịa `observedAt`/measurement; Gateway không tự `online`.

### 11.3. Context dự án versioned (một context inline)

Một context duy nhất, versioned, **inline** ở đầu entity JSON-LD; custom term IRI dùng namespace
ví dụ dành riêng (RFC 2606), **không** dùng domain thật của dự án:

```json
{
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
```

- **Không fetch context remote** (kể cả context do client gửi); server là pure mapper, **không**
  chạy JSON-LD processing framework, **không** thêm route phục vụ context. Context là hằng số
  versioned (namespace `…/iot/v1#`) lưu trong source/tài liệu.
- Chỉ **subset NGSI-LD-compatible** của dự án; **không** tuyên bố full conformance. Term NGSI-LD
  lõi giữ IRI ETSI làm hằng chuỗi; custom term map sang namespace ví dụ. Không nhận `@context` do
  client chọn và không nhận arbitrary JSON-LD để tạo graph (10.4).

### 11.4. Content-Type, header, request ID, CORS và bounds

`Content-Type` theo shape (11.2): entity JSON-LD `application/ld+json; charset=utf-8`; mọi shape
JSON khác `application/json; charset=utf-8`. Body mutation (#6/#7) yêu cầu `application/json`
(charset `utf-8`), khác → `415` (5.3).

Header/middleware (theo 1.4, không thêm header mới):

- `Cache-Control: no-store` mọi protected response; route admin/owner thêm `Pragma: no-cache`.
- `X-Request-ID`: echo nếu hợp lệ (regex 1.4), ngược lại sinh UUID; expose qua CORS.
- `401` kèm `WWW-Authenticate: Bearer`. CORS exact-origin; allow header `authorization,
  content-type, idempotency-key, x-request-id`; allow method gồm `PATCH` cho #7.

Bounds:

| Đối tượng | Bound | Vi phạm |
| --- | --- | --- |
| Body mutation (#6/#7) | `<= 16384` byte (16 KiB, khớp `ADMIN_MAX_BODY_BYTES`) | `413` |
| `gateway_id`/`sensor_id` | `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (≤64 ký tự) | `400` |
| Public URN `{entity_id}` (đã decode) | `<= 160` byte (max Sensor URN 148) | `400` |
| Query string raw | `<= 1024` byte | `400` |
| Cursor `after`/`next_cursor`/`relationship_after` encoded | `<= 768` byte | `400` |
| Cursor JSON decoded | `<= 512` byte | `400` |
| `limit` / `relationship_limit` | `1..200` | `400` |
| `max_points` | `100..2000` | `400` |

- Worst case URN: `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>` = 19 + 64 + 1 + 64 = **148** byte
  → bound 160 đủ. Cursor quan hệ worst case decoded **263** byte (9.2/9.4), nên bound decoded là
  **512** và encoded **768** (không còn 256/512).

### 11.5. Bảng error status/code dùng chung

Envelope ổn định `{"error":{"code","message","request_id"}}` (`src/internal/httpapi/error.go`).
`message` luôn generic, **không** lộ SQL/DSN/token/row hay sự tồn tại của resource ngoài quyền.

| Status | `code` (dùng thật + Stage 4) | Nghĩa | Nguồn |
| --- | --- | --- | --- |
| `400` | `invalid_request` | input/query/body sai (chung) | handlers hiện có; #1–#7 |
| `400` | `invalid_cursor` | cursor sai/không khớp filter | 9.2/9.4 |
| `400` | `invalid_resolution` | `resolution` ngoài allowlist | 8.4 |
| `400` | `raw_limit_exceeded` | raw window/`max_points` vượt | 8.4/8.9 |
| `400` | `resolution_too_fine` | bucket explicit quá mịn | 8.5 |
| `400` | `range_too_large` | `to-from > 744h` | 8.2 |
| `400` | `invalid_property` | `property` ngoài allowlist | 8.8 |
| `400` | `inconsistent_unit` | unit không đồng nhất (numeric temporal) | 8.9 |
| `401` | `unauthorized` | thiếu/sai JWT | 1.4 |
| `403` | `forbidden` | role/capability không đủ | 7.4 |
| `404` | `not_found` | không tồn tại **hoặc** ngoài quyền | 7.5 |
| `405` | `method_not_allowed` | sai method | 1.4 |
| `409` | `conflict` | representation/owner khác (exact replay) | 5.4/10.2 |
| `413` | `payload_too_large` | body vượt 16 KiB | 5.3 |
| `415` | `unsupported_media_type` | Content-Type sai | 5.3 |
| `500` | `internal_error` | lỗi bất ngờ (không lộ chi tiết) | 7.5 |
| `501` | `not_implemented` | hai route stub #1/#2 hiện tại | 1.5 |
| `503` | `service_unavailable` | timeout/unavailable/commit bất định | 5.4/7.5 |

- `code` là chuỗi ổn định; status `400` dùng chung family với các sub-code trên (giữ
  `invalid_request` cho input chung, sub-code cho lỗi Stage 4 cụ thể).
- Lỗi DB khi **kiểm quyền/ghi** **không** bao giờ thành `404`/`items=[]`/`200` (7.5); commit bất
  định → `503`, không success.

### 11.6. Examples canonical (JSON parseable; DESIGN ONLY)

**(a) Detail Sensor** (`application/ld+json`; `@context` inline như 11.3, lược cho gọn), `unit` giữ
symbol (`degC`) và **không** bịa measurement/`observedAt`; Sensor không có `hasSensor`/`description`:

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "type": "Sensor",
  "name": { "type": "Property", "value": "Temperature", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "unit": { "type": "Property", "value": "degC", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "gateway_id": "gateway_001"
}
```

**(b) Detail Sensor chưa đặt `unit`** — Property `value` = `null` (không bịa), cùng `@context`
11.3 (lược cho gọn):

```json
{
  "id": "urn:ngsi-ld:Sensor:gateway_001:sensor_004",
  "type": "Sensor",
  "name": { "type": "Property", "value": "Raw ADC", "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "unit": { "type": "Property", "value": null, "modifiedAt": "2026-10-15T09:00:00.000000Z" },
  "gateway_id": "gateway_001"
}
```

**(c) Cursor encoding thật** — hai cursor dưới **giải mã đúng** schema 9.2/9.4 (không placeholder):

```text
after (9.2)     = v1.eyJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIiLCJnIjpudWxsLCJ0IjoiU2Vuc29yIn0
decoded         = {"k":"urn:ngsi-ld:Sensor:gateway_001:sensor_002","g":null,"t":"Sensor"}

rel_after (9.4) = v1.eyJzIjoidXJuOm5nc2ktbGQ6R2F0ZXdheTpnYXRld2F5XzAwMSIsInIiOiJoYXNTZW5zb3IiLCJrIjoidXJuOm5nc2ktbGQ6U2Vuc29yOmdhdGV3YXlfMDAxOnNlbnNvcl8wMDIifQ
decoded         = {"s":"urn:ngsi-ld:Gateway:gateway_001","r":"hasSensor","k":"urn:ngsi-ld:Sensor:gateway_001:sensor_002"}
```

**(d) Error envelope** (`application/json`):

```json
{
  "error": {
    "code": "invalid_cursor",
    "message": "invalid cursor",
    "request_id": "req_placeholder"
  }
}
```

### 11.7. Ràng buộc nguồn cho 4.0.6

- Mapper identity hiện có: `src/internal/digitaltwin/mapper/identity.go` (chỉ URN, **chưa** có
  JSON-LD representation — Stage 4 mở rộng tại đây, chưa viết ở 4.0).
- Error envelope/request ID/CORS: `src/internal/httpapi/error.go`,
  `src/internal/httpapi/request_id.go`, `src/internal/httpserver/cors.go`,
  `src/internal/httpserver/router.go` (1.4). 401 thêm `WWW-Authenticate: Bearer` tại `WriteError`.
- Body bound 16 KiB khớp `ADMIN_MAX_BODY_BYTES` default `16384` (`src/internal/config/config.go`,
  `.env.example`); strict JSON/content-type xem 5.3.
- Context: chỉ tài liệu + mapper; **không** route/framework/thư viện JSON-LD mới.
- Cross-ref, không lặp: quyền → 7.3–7.4; URN/encode → 7.2; time/bucket → 8.2–8.6; BIGINT/NULL →
  8.7; cursor → 9.2/9.4; mutation view → 10.6.

### 11.8. Bàn giao Task 4.0 và xác nhận kiểm tra tài liệu (Task 4.0.7)

Bước 4.0.7 hoàn tất việc rà soát toàn diện contract API Stage 4 và chốt bàn giao:

#### 11.8.1. Rà soát bao phủ bảy endpoint và sự thật mã nguồn

1. **Bao phủ 7 endpoint:** Toàn bộ 7 endpoint của Stage 4 đã được đặc tả đầy đủ request parameters,
   defaults, bounds, authorization matrix, success response, empty authorized series/list, error
   envelope và JSON parseable examples:
   - #1 `GET /v1/telemetry/history` (mục 8.1–8.7, 8.11).
   - #2 `GET /v1/digital-twins` (mục 9.1–9.2, 9.6).
   - #3 `GET /v1/digital-twins/{entity_id}` (mục 9.3–9.4, 9.6, 11.2, 11.6).
   - #4 `GET /v1/digital-twins/{entity_id}/state` (mục 9.5, 9.6, 11.2).
   - #5 `GET /v1/digital-twins/{entity_id}/history` (mục 8.8–8.11, 11.1).
   - #6 `POST /v1/digital-twins` (mục 10.1–10.4, 10.6, 10.7).
   - #7 `PATCH /v1/digital-twins/{entity_id}` (mục 10.5–10.7).
2. **Đối chiếu các sự thật mã nguồn dễ sai lệch:**
   - *Router:* Hiện chỉ có route #1 và #2 tồn tại dưới dạng stub trả `501 not_implemented` trong
     `src/internal/httpserver/router.go`. Các route #3–#7 chưa được khai báo.
   - *Membership & Admin:* Các truy vấn đọc sử dụng `user_gateways` (các role `owner`, `operator`, `viewer`),
     trong khi năng lực ghi quản trị (admin write capability) kiểm tra `platform_admins` (Gateway Owner được
     quản lý metadata sensor được gán theo ma trận quyền); không phải mọi truy vấn đọc/ghi đều đòi hỏi cả hai
     bảng đồng thời. Ứng dụng Go chạy với role DB `iot_backend_app` có cờ `BYPASSRLS`, do đó Go code phải tự
     thực thi lọc quyền trong SQL; không dựa vào Supabase RLS.
   - *Public URN:* Định dạng URN chuẩn hoá `urn:ngsi-ld:Gateway:<gateway_id>` và
     `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>` được áp dụng thống nhất, URL-encoded khi nằm trên path.
   - *Nullable Provenance:* Các trường `boot_id`, `message_id`, `sample_index` đối với các mẫu dữ
     liệu legacy/untracked được giữ `null` nguyên bản, tuyệt đối không bịa UUID giả hay số 0.
   - *Audit State & Timestamps:* Bảng `twin_states` bảo lưu chính xác `last_reported_at`,
     `last_desired_at`, `updated_at`, `last_desired_by`. Timestamp `observed_at` bên trong
     `reported_state` là measurement time của thiết bị, phân biệt với server write time `modifiedAt`/`updated_at`.
   - *Replay & Conflict:* `POST /v1/digital-twins` hỗ trợ exact-replay trả `200` khi DTO trùng khớp
     hoàn toàn bản ghi đã tồn tại, và trả `409 conflict` khi khác metadata/owner profile.

#### 11.8.2. Phục hồi selector API HTTP và giải thích kiến trúc

- Đã phục hồi chính xác hai selector API HTTP: `value` (truy vấn telemetry raw/bucket) và
  `sample_interval_us` (truy vấn typed temporal) cho endpoint `GET /v1/digital-twins/{entity_id}/history`,
  loại bỏ hoàn toàn việc đổi tên selector trái phép trước đó.
- Bảng ánh xạ tập trung (mục 11.1) làm rõ sự tách biệt giữa: (1) Key trong DB, (2) Key trong state view
  JSONB, (3) Selector API HTTP độc lập, và (4) Public term mapper JSON-LD.
- Khẳng định kiến trúc: Quy định lồng keyword nội bộ `@value`/`hasValue` trong cấu trúc JSON-LD `Property`
  chỉ yêu cầu mapper entity detail dùng public term `measurement`, hoàn toàn **KHÔNG** đòi hỏi phải đổi tên
  tham số query selector HTTP của REST API.

#### 11.8.3. Khử mâu thuẫn chéo và kiểm tra bounds

- **Cursor quan hệ worst-case:**
  Chuỗi JSON decoded tối đa: `{"s":"urn:ngsi-ld:Gateway:<gw64>","r":"hasSensor","k":"urn:ngsi-ld:Sensor:<gw64>:<sid64>"}`
  đạt 259 byte (tối đa 263 byte với khoảng trắng). Chuỗi base64url encoded gồm prefix `v1.` đạt khoảng
  349–354 byte. Cả hai đều nằm an toàn bên trong các giới hạn đã chốt:
  `Độ dài JSON decoded <= 512 byte` và `Độ dài encoded <= 768 byte`.
- **Khẳng định tính chất ví dụ text/boolean:** Các thuộc tính text và boolean trong mục 8.11 (f) được
  ghi chú rõ ràng là test fixture minh họa cho reader xử lý đa kiểu dữ liệu; hoàn toàn không phải selector
  đang hoạt động trong Stage 4 hay khẳng định dữ liệu đang tồn tại.
- **Giới hạn phân trang và buffer nhất quán:** List `limit` (1..200, default 50), `relationship_limit`
  (1..200, default 50), `max_points` (100..2000, default 1000), time range window tối đa 744h, raw window
  tối đa 10s, mutation body tối đa 16 KiB nhất quán xuyên suốt các mục.

#### 11.8.4. Quyết định bàn giao kỹ thuật cho các Task tiếp theo

1. **Bàn giao cho Task 4.1 (Query Models, Bounds & Read Authorization):**
   - Thêm parser, DTO model và use-case interfaces cho history, Twin list/detail/state/history; dùng identity
     principal và validator identifier/URN đã có.
   - Nối bounds config và request deadline; kiểm thực range, resolution, max points, repeated/unknown query,
     cursor keyset versioned (`v1.`), giải mã base64url không padding đúng 3 key và URL encoding.
   - Repository scope Gateway/Sensor/entity bằng `user_gateways` cho mọi đường đọc; xử lý fail-closed phân
     biệt rõ ràng giữa authorized-empty (trả `200` mảng rỗng) và forbidden/missing (trả `404` thống nhất),
     không cho phép platform-admin bypass quyền đọc.
   - *Lưu ý kiến trúc:* Task 4.1 tập trung hoàn toàn vào tầng query models, bounds, repository và read
     authorization; **KHÔNG** thực hiện đăng ký hay kích hoạt route HTTP tại bước này.
2. **Bàn giao cho Task 4.2 (Telemetry Raw & Adaptive time_bucket History):**
   - Triển khai raw lookup bounded với stable ordering (`sample_id`) và bảo toàn provenance legacy NULL.
   - Triển khai bucket SQL (`avg`, `min`, `max`, `count`) theo UTC origin, range được cắt chính xác `[from, to)`
     trước khi gom nhóm, allowlisted resolution và tự động chọn resolution không vượt trần `max_points` (kể cả
     partial buckets).
   - Trả rõ ràng mode, resolution, unit, counts, points; xử lý empty series, gap và raw overflow đúng contract;
     định dạng an toàn BIGINT (`sequence_num`, bucket `count`) thành chuỗi thập phân, không làm biến đổi hay
     nhân đôi dữ liệu raw đã commit.
3. **Bàn giao cho Task 4.3 (Twin List, Detail, State, Mapper & Relationships):**
   - Triển khai membership-scoped keyset list (phân trang theo `entity_id COLLATE "C"` với `LIMIT limit + 1`
     để dựng `next_cursor`), detail graph và current state trực tiếp từ PostgreSQL.
   - Endpoint `/state` đọc snapshot trực tiếp từ 9 cột của `twin_states`, không dựng lại từ lịch sử và bảo
     toàn nguyên vẹn kiểu JSONB của `reported_state` và `desired_state`.
   - Mapper tập trung một nơi cho Property, Relationship, context, observedAt; giữ nguyên public URN, entity
     type, số/NULL JSONB, không tạo observation hoặc quan hệ giả.
   - Phân trang quan hệ `hasSensor` tại chính endpoint detail với keyset
     `(relationship_type COLLATE "C", target_public_urn COLLATE "C")`, trong đó target key `k` bắt buộc phải là
     target PUBLIC URN (`tgt.entity_id`), tuyệt đối **KHÔNG** dùng UUID column `target_entity_id`.
4. **Bàn giao cho Task 4.4 (Temporal History & Value Delegation):**
   - Resolve entity/ownership trước khi truy cập dữ liệu; property `value` của Sensor được ủy quyền (delegate)
     trực tiếp đến raw history engine của Task 4.2, không tạo history store thứ hai.
   - Truy vấn temporal theo entity, property, time range; bảo toàn typed value (`value_number`), metadata
     (`unit`, `boot_id`, `source`), message ID, hỗ trợ đầy đủ `sample_interval_us`.
   - Numeric aggregation chỉ thực hiện khi hợp nghĩa và đơn vị đồng nhất; text/boolean bảo toàn typed
     observations và bounds nghiêm ngặt (chứng minh read path, không khẳng định inference ingestion khi chưa
     có contract writer).
5. **Bàn giao cho Task 4.5 (Entity Metadata POST & PATCH):**
   - Tái sử dụng transaction provisioning hiện có cho `POST /v1/digital-twins` (tạo entity, quan hệ `hasSensor`,
     và bản ghi `twin_states` đồng thời; thực thi admin Gateway và admin/owner Sensor policy).
   - Xử lý exact-replay trả `200` và conflict trả `409`.
   - `PATCH /v1/digital-twins/{entity_id}` chỉ cho phép cập nhật metadata allowlist (`name`, `description`),
     cập nhật đồng thời relational metadata và entity metadata trong cùng một transaction; bảo toàn unit, URN,
     ownership, desired/reported state, version, audit và temporal history; tuyệt đối không tác động MQTT hay
     tạo command/outbox.
   - Thực thi kiểm tra quyền: admin/owner positive, operator/viewer/non-member denied không gây bất kỳ side
     effect nào.
6. **Bàn giao cho Task 4.6 (HTTP Handlers & Server Composition):**
   - Đăng ký và mở các route HTTP: thay thế 2 authenticated stub hiện có (`GET /v1/telemetry/history` và
     `GET /v1/digital-twins`) bằng handler thực tế; đăng ký và kích hoạt 5 route mới:
     `GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}/state`,
     `GET /v1/digital-twins/{entity_id}/history`, `POST /v1/digital-twins`, và
     `PATCH /v1/digital-twins/{entity_id}`. Giữ `/v1/ws` và các endpoint giai đoạn sau nguyên trạng chưa khả dụng.
   - Wire toàn bộ service, repository, config vào server/router: tích hợp middleware JWT, role checks, error
     mapping, request ID, no-store headers, CORS. Tuyệt đối không gọi Supabase Auth từ mọi request và không mở
     thêm public business route.
   - Tích hợp kiểm thử HTTP request/response, status codes, media types, schemas và constructor/config error
     handling; bảo đảm API Stage 2 create-or-exact-replay tiếp tục hoạt động theo contract cũ.

#### 11.8.5. Kết quả kiểm tra tài liệu thuần túy (Document-only Verification)

Mọi kiểm tra tài liệu đã được thực hiện trực tiếp trên worktree và đạt kết quả:
- **Parse JSON Fences:** 31/31 khối JSON trong tài liệu phân tích cú pháp hợp lệ (0 lỗi cú pháp).
- **Giải mã Cursor Keyset:** Toàn bộ 3 mẫu cursor base64url trong tài liệu (`after` list, `relationship_after`
  detail, `next_cursor` list) giải mã chính xác cấu trúc JSON `{k, g, t}` và `{s, r, k}`, độ dài hợp lệ.
- **Liên kết Markdown:** 8/8 liên kết nội bộ trong tài liệu phân giải chính xác đến các file tồn tại trong repo.
- **Định dạng & Whitespace:** Lệnh `git diff --check` thực thi không phát hiện bất kỳ lỗi khoảng trắng nào (whitespace only).
- **Ranh giới thực thi:** Công việc thuần túy ở tầng tài liệu contract thiết kế; không chạy build Go, không chạy
  test native/CI, không thay đổi runtime code hay cấu hình server.

---

## 12. Tham chiếu

- [AGENTS.md](../../AGENTS.md) — chính sách MVP mục 0 và 16.
- [Task 4.0 detail plan](../../../docs/backend_plan/task_4.0_detail_plan.md).
- [Stage 4 backend detailed plan](../../../docs/backend_plan/stage4_backend_detailed_plan.md) — mục 1–4.
- [MQTT Application Protocol v1](mqtt-v1.md).
- [Bàn giao Stage 3](../backend/stage-3-uplink.md).
- CI lịch sử: run 38891798340 tại `2ce3a89`
  (<https://github.com/TVTIT/project1_iot_server/actions/runs/38891798340>).
- Migrations: `migrations/000001` … `migrations/000020` (đường dẫn tính từ `main_src/`).
- HTTP: `src/internal/httpserver/router.go`, `src/internal/httpapi/error.go`,
  `src/internal/httpserver/cors.go`, `src/internal/auth/`.
- Gateway: `src/internal/gateway/`.
- Telemetry: `src/internal/telemetry/`.
- Mapper: `src/internal/digitaltwin/mapper/identity.go`.

**Trạng thái contract:** Task 4.0 (bao gồm Task 4.0.1–4.0.7) đã **HOÀN THÀNH**. Contract đã được chốt
thiết kế kỹ thuật đầy đủ để triển khai (Stage 4 API DESIGN ONLY). Các endpoint Stage 4 **chưa khả dụng**
trên runtime. **Task 4.1–4.8 chưa được triển khai** và không được suy diễn từ tài liệu thiết kế này.
