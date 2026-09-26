# IoT Gateway-Server Platform (Đồ án 1 - ET3290)

Hệ thống IoT Gateway–Server phục vụ thu thập dữ liệu cảm biến thời gian thực từ các Gateway không đồng nhất (heterogeneous Gateways: ESP32, Luckfox Pico Plus, TI AM5728), lưu trữ chuỗi thời gian (time-series) trên TimescaleDB, hỗ trợ truy vấn lịch sử, streaming dữ liệu thời gian thực qua WebSocket, xác thực và phân quyền người dùng thông qua Supabase Auth/RLS, quản lý tải lên media (hình ảnh) qua private storage, và tích hợp phân hệ **Digital Twin** (quản lý thực thể theo chuẩn NGSI-LD, đồng bộ trạng thái `reported_state` / `desired_state`, điều khiển Gateway qua MQTT Transactional Outbox và lưu trữ lịch sử thuộc tính biến thiên theo thời gian).

---

## 1. Kiến trúc tổng thể (System Architecture)

```text
[Heterogeneous Gateways: ESP32 / Luckfox / AM5728]
   │
   ├── Telemetry / Status / Command Results (MQTT over TLS / Port 8883) ──► [Mosquitto Broker]
   │   ◄── MQTT Commands (Downlink via Port 8883) ──────────────────────────────│
   │                                                                            │
   │                                                                     (internal mTLS/TCP)
   │                                                                            ▼
   │                                                                    [Go Backend Monolith]
   │                                                                            │
   │                                                                            ├──► PostgreSQL 16 + TimescaleDB
   │                                                                            │    ├── Relational & Permissions
   │                                                                            │    ├── Digital Twin Core & Outbox
   │                                                                            │    └── Hypertables (Telemetry & Temporal)
   │                                                                            │
   ├── Media Upload (Images via HTTPS signed URL) ──────────────────────────────┼──► [Supabase API Gateway (Envoy)]
   │                                                                            │    │
   ▼                                                                            │    ├──► Supabase Auth (GoTrue)
[Nginx Reverse Proxy] ◄─── Cloudflare Tunnel (cloudflared)                      │    └──► Supabase Storage API (Local FS)
   │                                                                            │
   ├── /v1/* (REST API & WebSocket Stream) ─────────────────────────────────────┘
   └── /auth/v1/*, /storage/v1/* ──────────────────────────► Supabase Gateway (Envoy)
   ▲
   │ (HTTPS / WSS)
[Clients: Flutter App / Web Dashboard]
```

### Các thành phần chính trong hệ thống:
1. **Nginx (`config/nginx/nginx.conf.template`)**: HTTP reverse proxy duy nhất nhận traffic từ Cloudflare Tunnel / mạng ngoài, điều hướng các prefix:
   - `/v1/*`: Định tuyến vào Go Backend (REST API).
   - `/v1/ws`, `/v1/telemetry/ws`: Reverse proxy WebSocket cho Go Backend.
   - `/auth/v1/*`: Định tuyến tới Supabase Auth (GoTrue qua Envoy gateway).
   - `/storage/v1/*`: Định tuyến tới Supabase Storage API (qua Envoy gateway).
2. **Mosquitto MQTT Broker (`config/mosquitto/`)**:
   - Chạy MQTTS (port `8883` công khai qua TLS, `18830` cục bộ).
   - Xác thực Gateway độc lập bằng username/password (`config/mosquitto/passwd`).
   - Phân quyền theo topic (`config/mosquitto/acl`) với pattern `%u` cách ly từng Gateway: telemetry, status, responses (Gateway publish), commands (Gateway subscribe).
3. **Go Backend Modular Monolith (`src/cmd/server`)**:
   - Xử lý xác thực JWT Supabase, phân quyền quan hệ User–Gateway (`user_gateways`).
   - Thu thập telemetry từ Mosquitto, deduplication bằng `processed_messages`, lưu trữ TimescaleDB.
   - API truy vấn lịch sử (`time_bucket`), WebSocket streaming realtime cho client được cấp quyền.
   - Cấp Signed Upload/Read URL cho media (ảnh chụp từ Gateway), xác thực metadata và lưu trạng thái vào `media_objects`.
   - **Digital Twin Subsystem**: Quản lý thực thể Gateway/Sensor/Device chuẩn NGSI-LD, đồng bộ `reported_state` từ uplink, tiếp nhận `desired_state` từ người dùng, quản lý lệnh điều khiển qua Transactional Outbox và đối soát trạng thái (reconciliation).
4. **PostgreSQL 16 + TimescaleDB**:
   - Dữ liệu quan hệ & bảo mật: `profiles`, `gateways`, `user_gateways`, `sensors`, `media_objects`, `processed_messages`.
   - Dữ liệu Digital Twin: `twin_entities`, `twin_relationships`, `twin_states`, `twin_commands`, `twin_outbox`. Mỗi entity có `gateway_id` bắt buộc để phân quyền qua `user_gateways`.
   - Hypertables:
     - `telemetry`: Phân vùng 1 ngày (`chunk_time_interval => '1 day'`) và tự động nén columnar sau 7 ngày.
     - `twin_temporal_values`: Lưu chuỗi biến thiên giá trị thuộc tính số, chữ, boolean và AI inference metrics (reconstruction loss, anomaly score).
   - Chưa bật retention tự động cho dữ liệu chuỗi thời gian. Thời hạn lưu sẽ được cấu hình sau khi đo dung lượng thực tế.
   - Các khóa ngoại dùng `ON DELETE RESTRICT`: phải archive hoặc dọn dữ liệu phụ thuộc có chủ đích trước khi xóa Gateway/Sensor.
   - FK từ `telemetry` tới `sensors` bảo vệ tính toàn vẹn trên đường ghi nóng; cần benchmark batch insert 100 Hz trước khi chốt cấu hình production.
   - Row Level Security (RLS) bảo vệ dữ liệu theo User–Gateway mapping (`migrations/000004_supabase_compat.up.sql`).
5. **Self-hosted Supabase Services**:
   - **Envoy Gateway (`supabase-envoy`)**: Đóng vai trò API Gateway nội bộ điều phối `/auth/v1` và `/storage/v1` (tương thích Kong alias).
   - **GoTrue Auth (`supabase-auth`)**: Quản lý người dùng, issue JWT HS256.
   - **Storage API (`supabase-storage`)**: Quản lý lưu trữ file cục bộ (private bucket `media-images`).
6. **Cloudflare Tunnel (`cloudflared`)**: Xuất bản dịch vụ ra Internet an toàn mà không cần mở port NAT trực tiếp trên router.

---

## 2. Cấu trúc thư mục (Repository Structure)

```text
.
├── .env.example                     # Mẫu biến môi trường cho Docker Compose & Backend
├── docker-compose.yml               # Cấu hình toàn bộ stack dịch vụ (8 container)
├── AGENTS.md                        # Đặc tả hệ thống, MVP scope và quyết định kiến trúc
├── migrations/                      # SQL migrations cho Database
│   ├── 000001_init_schema.up.sql    # Relational entities và telemetry hypertable
│   ├── 000002_digital_twin_tables.up.sql # Digital Twin, command, outbox và temporal hypertable
│   ├── 000003_supabase_roles.up.sql # Khởi tạo role cho GoTrue Auth và Storage
│   ├── 000004_supabase_compat.up.sql# Khóa ngoại profiles-auth.users, trigger user mới và RLS policies
│   ├── 000005_seed_dev_data.up.sql  # Gateway, sensor và Digital Twin mẫu cho development
│   ├── 000006_add_gateway_ownership_columns.up.sql # Cột ownership và desired-state actor
│   ├── 000007_backfill_twin_gateway_ownership.up.sql # Backfill ownership cho entity hiện hữu
│   ├── 000008_schema_hardening.up.sql # FK, CHECK, UNIQUE và gỡ retention mặc định
│   └── 000009_migration_tracking_and_sensor_urn.up.sql # Theo dõi version và URN Sensor theo Gateway
├── config/
│   ├── nginx/
│   │   └── nginx.conf.template      # Cấu hình Nginx reverse proxy mẫu
│   ├── mosquitto/
│   │   ├── mosquitto.conf           # Cấu hình broker Mosquitto (TLS, auth, ACL)
│   │   ├── passwd                   # File mật khẩu mosquitto (băm PBKDF2)
│   │   └── acl                      # Cấu hình topic isolation theo %u
│   └── envoy/                       # Cấu hình Supabase Envoy Gateway
│       ├── envoy.yaml               # Bootstrap config
│       ├── cds.yaml                 # Cluster discovery (auth, storage services)
│       ├── lds.template.yaml        # Listener routing template
│       └── docker-entrypoint.sh     # Script thế biến môi trường khởi chạy Envoy
├── scripts/
│   ├── gen-certs.sh                 # Tạo Root CA và Server Certificate cho Mosquitto TLS
│   ├── gen-keys.py                  # Sinh ANON_KEY và SERVICE_ROLE_KEY từ JWT_SECRET
│   ├── init-buckets.sh              # Khởi tạo private storage bucket (media-images)
│   ├── backup-db.sh                 # Sao lưu dữ liệu PostgreSQL + TimescaleDB ra file nén .sql.gz
│   └── sql/                         # Preflight, verification và rollback schema hardening
└── src/
    ├── cmd/
    │   ├── server/                  # Điểm khởi chạy Go Backend Server
    │   └── gateway/                 # Điểm khởi chạy Gateway Simulator (ESP32 / Luckfox / AM5728)
    ├── internal/                    # Các module nghiệp vụ (auth, mqtt, telemetry, digitaltwin, media)
    └── pkg/                         # Thư viện tiện ích dùng chung
```

---

## 3. Hướng dẫn cài đặt & Triển khai (Setup Guide)

### 3.1 Yêu cầu hệ thống
- Linux / macOS / WSL2
- Docker Engine >= 24.0 & Docker Compose v2
- OpenSSL (để tạo chứng chỉ TLS cho MQTT)
- Python 3 (để chạy script sinh Supabase keys)

### 3.2 Các bước cấu hình ban đầu

1. **Chuẩn bị file môi trường (`.env`)**:
   ```bash
   cp .env.example .env
   ```

2. **Khởi tạo JWT Secret & Supabase API Keys**:
   Chạy script để sinh `ANON_KEY` và `SERVICE_ROLE_KEY` tương ứng với `JWT_SECRET` của bạn (độ dài tối thiểu 32 ký tự):
   ```bash
   python3 scripts/gen-keys.py "super-secret-jwt-token-with-at-least-32-characters-long"
   ```
   Cập nhật các giá trị in ra vào file `.env`:
   - `JWT_SECRET`
   - `ANON_KEY`
   - `SERVICE_ROLE_KEY`

3. **Khởi tạo chứng chỉ TLS cho Mosquitto Broker**:
   ```bash
   chmod +x scripts/gen-certs.sh
   ./scripts/gen-certs.sh
   ```
   Script sẽ sinh CA và server certificates lưu tại `config/mosquitto/certs/`.

4. **Khởi chạy Docker Compose**:
   ```bash
   docker compose up -d --build
   ```

5. **Khởi tạo Storage Buckets**:
   Sau khi các service khởi động hoàn tất:
   ```bash
   chmod +x scripts/init-buckets.sh
   SERVICE_ROLE_KEY="<service_role_key_trong_env>" ./scripts/init-buckets.sh
   ```
   Script sẽ tự động tạo private bucket:
   - `media-images`: Lưu trữ ảnh chụp từ Gateway (không cấp quyền truy cập công khai; truy cập đọc thông qua Signed URL do Go Backend cấp).

6. **Sao lưu cơ sở dữ liệu định kỳ**:
   ```bash
   chmod +x scripts/backup-db.sh
   ./scripts/backup-db.sh
   # File backup sẽ được lưu tại thư mục ./backups/
   ```

### 3.2.1 Chạy và kiểm tra Go Backend

Backend yêu cầu `DATABASE_URL` hợp lệ và sẽ dừng ngay nếu không kết nối được
PostgreSQL. Giới hạn pool, HTTP timeout và các giới hạn MQTT dự kiến đều được
cấu hình qua `.env`; xem `.env.example` để biết tên biến.

```bash
docker compose up -d backend
docker compose exec backend wget -qO- http://127.0.0.1:8080/healthz
docker compose exec backend wget -qO- http://127.0.0.1:8080/readyz
```

- `/healthz` chỉ xác nhận tiến trình HTTP còn hoạt động.
- `/readyz` xác nhận backend hiện truy cập được dependency bắt buộc.
- `docker compose stop backend` gửi `SIGTERM` để backend hoàn tất request đang
  xử lý và đóng PostgreSQL pool.

### 3.3 Áp dụng migration schema hardening

Các file trong `migrations/` được mount vào `/docker-entrypoint-initdb.d` và chỉ tự chạy khi PostgreSQL khởi tạo một data volume mới. Restart container không áp dụng migration mới lên database hiện hữu.

Migration `000009` tạo bảng `schema_migrations` sau khi schema hiện hữu đã được đối chiếu. Trên database hiện hữu, chỉ áp dụng migration này sau khi `000006`–`000008` đã được xác minh; không chạy lại các migration hardening khi constraint đã tồn tại.

`AUTH_DB_PASSWORD` và `STORAGE_DB_PASSWORD` là hai mật khẩu riêng. Service
one-shot `supabase-role-provisioner` tạo hoặc xoay vòng hai role database trước
khi GoTrue và Storage khởi động; không dùng lại mật khẩu PostgreSQL superuser
cho hai dịch vụ này. Vì upstream nhận PostgreSQL URI, hai mật khẩu chỉ dùng ký
tự URL-safe: chữ cái, chữ số, dấu `.`, `_`, `~` và `-`.

`000005_seed_dev_data.up.sql` là dữ liệu development tùy chọn. Trên database
mới file này được bỏ qua trong init vì GoTrue chưa tạo `auth.users`; nếu cần dữ
liệu demo, chạy file một lần sau khi `supabase-compat-migration` hoàn tất.

Trước khi nâng cấp database hiện hữu, tạo backup và chạy preflight:

```bash
set -a && source .env && set +a
bash scripts/backup-db.sh
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < scripts/sql/preflight-schema-hardening.sql
```

Sau khi preflight thành công, áp dụng lần lượt:

```bash
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < migrations/000006_add_gateway_ownership_columns.up.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < migrations/000007_backfill_twin_gateway_ownership.up.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < migrations/000008_schema_hardening.up.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < scripts/sql/verify-schema-hardening.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < scripts/sql/preflight-migration-000009.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < migrations/000009_migration_tracking_and_sensor_urn.up.sql
docker exec -i iot_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < scripts/sql/verify-migration-000009.sql
```

Các lệnh trên giả định `POSTGRES_USER` và `POSTGRES_DB` đã được export từ `.env`. Không chạy `docker compose down -v` trên database có dữ liệu cần giữ.

---

## 4. Đặc tả API & Giao thức (API & Protocol Specs)

### 4.1 REST API (Go Backend)

#### Endpoint hệ thống & Telemetry:
| Phương thức | Đường dẫn | Xác thực | Mô tả |
|---|---|---|---|
| `GET` | `/healthz` | Không | Healthcheck cấp Nginx/container (trả về `OK`) |
| `GET` | `/v1/health` | Không | Healthcheck Go Backend (`{"status":"running",...}`) |
| `GET` | `/v1/telemetry/history` | Bearer JWT (Supabase) | Lấy chuỗi lịch sử mẫu đo cảm biến (`time_bucket` downsampling) |
| `GET` | `/v1/ws` | Bearer JWT / Query token | Nâng cấp kết nối WebSocket để streaming telemetry theo thời gian thực |
| `POST` | `/v1/media/upload-url` | Bearer Gateway JWT | Gateway yêu cầu signed upload URL cho ảnh chụp |

#### Digital Twin & Device Control API (`/v1/*`):
| Phương thức | Đường dẫn | Xác thực | Mô tả |
|---|---|---|---|
| `POST` | `/v1/digital-twins` | Bearer JWT (Supabase) | Tạo mới thực thể Digital Twin (Gateway / Sensor / Device) |
| `GET` | `/v1/digital-twins` | Bearer JWT (Supabase) | Lấy danh sách thực thể người dùng có quyền truy cập |
| `GET` | `/v1/digital-twins/{entity_id}` | Bearer JWT (Supabase) | Lấy thông tin chi tiết thực thể (định dạng NGSI-LD JSON) |
| `PATCH` | `/v1/digital-twins/{entity_id}` | Bearer JWT (Supabase) | Cập nhật metadata / thuộc tính tĩnh của thực thể |
| `GET` | `/v1/digital-twins/{entity_id}/state` | Bearer JWT (Supabase) | Xem trạng thái hiện thời (`reported_state` vs `desired_state`) |
| `PATCH` | `/v1/digital-twins/{entity_id}/desired-state` | Bearer JWT (Supabase) | Đặt cấu hình mong muốn (tạo command & outbox trong transaction) |
| `GET` | `/v1/digital-twins/{entity_id}/history` | Bearer JWT (Supabase) | Truy vấn lịch sử thuộc tính biến thiên theo thời gian (TimescaleDB) |
| `POST` | `/v1/digital-twins/{entity_id}/commands` | Bearer JWT (Supabase) | Phát lệnh điều khiển trực tiếp (vd: reboot, capture_image) |
| `GET` | `/v1/digital-twins/{entity_id}/commands` | Bearer JWT (Supabase) | Lấy danh sách chỉ lệnh đã phát cho thực thể |
| `GET` | `/v1/commands/{command_id}` | Bearer JWT (Supabase) | Kiểm tra trạng thái thực thi của command |

### 4.2 Supabase Auth & Storage API (qua Nginx)

| Đường dẫn | Dịch vụ tiếp nhận | Mô tả |
|---|---|---|
| `/auth/v1/signup` | Supabase GoTrue | Đăng ký tài khoản người dùng mới (tự kích hoạt profile qua trigger) |
| `/auth/v1/token?grant_type=password` | Supabase GoTrue | Đăng nhập lấy access_token (JWT) |
| `/storage/v1/object/sign/*` | Supabase Storage API | Cấp Signed URL đọc/ghi cho private bucket |
| `/storage/v1/object/*` | Supabase Storage API | Upload/Download tệp tin trực tiếp qua Signed URL |

### 4.3 MQTT Topic Contract

Đặc tả đầy đủ về payload, retry, sequence, application ACK và định danh Sensor
nằm tại `docs/protocols/mqtt-v1.md`.

- **Port TLS**: `8883`
- **Topic Namespace theo chuẩn ACL (`%u` isolation)**:
  - `gateways/<gateway_id>/telemetry/#`: Gateway publish các batch mẫu đo lường.
  - `gateways/<gateway_id>/acks/#`: Gateway nhận application-level commit ACK từ Go Backend khi cần xác nhận đã lưu DB.
  - `gateways/<gateway_id>/status`: Gateway publish trạng thái kết nối / hoạt động (`online`, `offline`).
  - `gateways/<gateway_id>/commands/#`: Gateway subscribe nhận lệnh điều khiển từ Go Backend (`update_configuration`, `reboot`, `capture_image`).
  - `gateways/<gateway_id>/responses/#`: Gateway publish kết quả thực thi lệnh (`succeeded`, `failed`, kèm `reported_state`).

#### Định dạng gói tin telemetry (`telemetry_batch`):
```json
{
  "protocol_version": 1,
  "message_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "message_type": "telemetry_batch",
  "gateway_id": "gateway_001",
  "sensor_id": "sensor_001",
  "boot_id": "7f2c45f7-0a76-4e28-8a37-7793d7d85b04",
  "first_sequence": 12501,
  "sample_count": 3,
  "measured_at": "2026-08-21T10:15:00.000Z",
  "sample_interval_us": 10000,
  "samples": [25.1, 25.2, 25.3]
}
```

#### Định dạng gói tin lệnh điều khiển từ Server (`command`):
```json
{
  "protocol_version": 1,
  "message_type": "command",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "entity_id": "urn:ngsi-ld:Gateway:gateway_001",
  "action": "update_configuration",
  "parameters": {
    "sampling_interval_seconds": 5
  },
  "desired_version": 12,
  "issued_at": "2026-09-16T08:31:00Z",
  "expires_at": "2026-09-16T08:32:00Z"
}
```

#### Định dạng gói tin kết quả thực thi từ Gateway (`command_result`):
```json
{
  "protocol_version": 1,
  "message_type": "command_result",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "status": "succeeded",
  "reported_state": {
    "sampling_interval_seconds": 5
  },
  "completed_at": "2026-09-16T08:31:03Z"
}
```

---

## 5. Phân hệ Digital Twin & Quản lý điều khiển (Digital Twin Subsystem)

Thay vì tích hợp Federated Learning (đã được loại bỏ để tập trung vào mục tiêu trọng tâm của đồ án), hệ thống sở hữu phân hệ **Digital Twin** hoàn chỉnh được thiết kế theo kiến trúc Modular Monolith bên trong Go Backend:

### 5.1 Chuẩn định danh & Thực thể NGSI-LD
- Mỗi thực thể vật lý (Gateway, Sensor, Device/Actuator) có định danh URN bền vững:
  - `urn:ngsi-ld:Gateway:<gateway_id>`
  - `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`
  - `urn:ngsi-ld:Device:<device_id>`
- Lưu trữ quan hệ thực thể trong `twin_relationships` (`hasSensor`, `connectedTo`, `controls`, `managedBy`).
- Cung cấp biểu diễn JSON-LD tại API boundary với `@context: ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]`.

### 5.2 Cơ chế phân tách Trạng thái (Reported vs Desired State)
- **`reported_state`**: Đại diện cho trạng thái thực tế cuối cùng do Gateway xác nhận qua MQTT (telemetry, status, command result). Client tuyệt đối không được ghi trực tiếp vào `reported_state`.
- **`desired_state`**: Đại diện cho cấu hình đích do người dùng mong muốn và thiết lập qua REST API. Gateway không được ghi vào `desired_state`.
- Quản lý phiên bản độc lập (`reported_version` và `desired_version`), áp dụng cơ chế **Optimistic Concurrency** để ngăn xung đột ghi đồng thời từ nhiều client.

### 5.3 Quy trình Điều khiển Downlink & Transactional Outbox
1. Người dùng gửi yêu cầu đổi trạng thái (`PATCH /desired-state`) hoặc gửi lệnh (`POST /commands`).
2. Go Backend kiểm tra quyền của người dùng trên Gateway liên kết (`user_gateways`).
3. Mở **Database Transaction**:
   - Cập nhật `desired_state` và tăng `desired_version`.
   - Ghi bản ghi chỉ lệnh vào `twin_commands` với trạng thái `pending`.
   - Ghi bản ghi thông điệp vào `twin_outbox`.
   - Commit transaction (đảm bảo không xảy ra tình trạng đã ghi DB nhưng mất tin MQTT hoặc ngược lại).
4. **Outbox Worker** lấy tin từ `twin_outbox`, publish qua Mosquitto topic `gateways/<gateway_id>/commands/<command_id>`, cập nhật trạng thái `published`.
5. Gateway nhận lệnh, kiểm tra idempotency (dựa trên `command_id`), thực thi cấu hình phần cứng.
6. Gateway publish kết quả về topic `gateways/<gateway_id>/responses/<command_id>`.
7. Go Backend đối soát (reconcile) kết quả: cập nhật `twin_commands` (`acknowledged -> succeeded / failed`) và cập nhật `reported_state` tương ứng trong cùng transaction.
8. Bắn sự kiện realtime qua WebSocket cho client đang theo dõi.

### 5.4 Lưu trữ Lịch sử Biến thiên Thời gian (TimescaleDB)
- Bảng hypertable `twin_temporal_values` ghi lại mọi biến thiên thuộc tính theo thời gian:
  - Dữ liệu cảm biến và các chỉ số suy luận AI (`reconstruction_loss`, `anomaly_score`, ...).
  - Lịch sử thay đổi trạng thái hoạt động của thiết bị.
  - Phục vụ biểu đồ xu hướng lịch sử và đối soát vận hành mà không cần quét toàn bộ bảng trạng thái hiện hành.
