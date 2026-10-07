# IoT Gateway-Server Platform (Đồ án 1 - ET3290)

Hệ thống IoT Gateway–Server phục vụ thu thập dữ liệu cảm biến thời gian thực từ các Gateway không đồng nhất (heterogeneous Gateways: ESP32, Luckfox Pico Plus, TI AM5728), lưu trữ chuỗi thời gian (time-series) trên TimescaleDB, hỗ trợ truy vấn lịch sử, streaming dữ liệu thời gian thực qua WebSocket, xác thực và phân quyền người dùng thông qua Supabase Auth/RLS, quản lý tải lên media (hình ảnh) qua private storage, và tích hợp phân hệ **Digital Twin** (quản lý thực thể theo chuẩn NGSI-LD, đồng bộ trạng thái `reported_state` / `desired_state`, điều khiển Gateway qua MQTT Transactional Outbox và lưu trữ lịch sử thuộc tính biến thiên theo thời gian).

Đây là kiến trúc/mục tiêu MVP, không phải toàn bộ tính năng đã hoàn thành:
- **Trạng thái phát triển hiện tại (Stage 2)**:
  - **Task 2.2**: Đã triển khai xác thực JWT qua Supabase Auth và phân quyền platform admin trên PostgreSQL (`platform_admins`).
  - **Task 2.3**: Đã triển khai API đọc danh sách Gateway (`/v1/gateways`) và danh sách Sensor (`/v1/gateways/{gateway_id}/sensors`) theo quan hệ thành viên (`user_gateways`).
  - **Task 2.4**: Đã triển khai hai Admin PUT provision Gateway/Sensor và đồ thị Digital Twin (`twin_entities`, `twin_relationships`, `twin_states`) trong cùng transaction atomically.
  - **Task 2.5 (Static Legacy Baseline)**: Đã hoàn thành hạ tầng runtime Mosquitto tĩnh (quản lý file mật khẩu `password_file` trong named volume `mosquitto_auth`, atomic replace có lock/fsync, reload qua Unix domain socket sidecar không dùng Docker socket, và probe kiểm chứng kết nối MQTT TLS).
  - **Task 2.6 (Dynamic Security Lifecycle)**: Đã hoàn thành triển khai 4 endpoint Admin MQTT Credential (`/v1/admin/gateways/{gateway_id}/mqtt-credential*`: GET metadata, POST provision, POST rotate, DELETE revoke), Mosquitto Dynamic Security (DynSec) adapter, hàng rào Ingress Gate fail-closed (khởi động `CLOSED`, chỉ `OPEN` sau khi đối soát authority DB, epoch và recovery checkpoint), recovery checkpoints và chuỗi migration CSDL đến version 16 (`000016_mqtt_event_authority_guard.up.sql`).
- **Mặc định môi trường triển khai & Cổng nghiệm thu riêng (Rollout Gates)**:
  - Trên môi trường triển khai chính (root deployment), tính năng quản lý credential MQTT mặc định bị **vô hiệu hóa** (`MQTT_CREDENTIAL_API_ENABLED=false`, các endpoint trả về HTTP `503 credential_runtime_disabled`).
  - Việc kích hoạt trên production yêu cầu các cổng chuyển đổi riêng biệt (separate rollout gates) chưa hoàn thành: kiểm chứng topology Rathole thật không bypass qua Internet, chính sách xử lý mất kết nối CSDL sau khi Ingress Gate OPEN, và quy trình nghiệm thu bàn giao secret qua phần cứng USB/encrypted storage.
- **Tình trạng CI & E2E (Task 2.7)**:
  - Pipeline GitHub Actions hiện bao gồm **10 jobs độc lập** (9 jobs thành phần và job tích hợp thứ 10 `stage2-e2e`, xem mục 3.4).
  - Job thứ 10 **`stage2-e2e`** đã được triển khai: thực thi 2 lượt chạy cô lập liên tiếp (`run-1` và `run-2`) bao phủ toàn bộ 27 kịch bản E01–E27 qua tất cả các selector, giới hạn thời gian 30 phút (`timeout-minutes: 30`), xuất báo cáo có cấu trúc đã qua quét an toàn (`safe-reports`), và kiểm chứng dọn dẹp tài nguyên sở hữu (`verified owned cleanup`).
  - Các lượt chạy cục bộ tương đương CI (CI-equivalent local runs) đã PASS theo báo cáo của worker.
  - Việc nghiệm thu CI từ xa trên GitHub Actions và chứng nhận commit exact-final-SHA vẫn ở trạng thái **PENDING** (chưa có remote exact-final-SHA acceptance; không tuyên bố toàn bộ Stage 2 đạt 10/10 remote hay production-ready). Các cổng kích hoạt môi trường triển khai thực tế (production rollout gates) tiếp tục được giữ tách biệt.
- **Hạ tầng Mosquitto**:
  - Phân biệt giữa hạ tầng file tĩnh legacy (Task 2.5: named volume `mosquitto_auth` mới chỉ seed tài khoản nội bộ `backend_service`, không tự động import tài khoản từ file prototype tracked trong git) và chu trình sống Dynamic Security (Task 2.6: quản lý tài khoản động qua DynSec adapter, Ingress Gate, global maintenance và thu hồi phiên tức thời).
- Xem thêm:
  [contract xác thực và verification](docs/backend/stage-2-task-2.2-authentication.md),
  [quản trị tài khoản tập trung Task 2.2A](docs/backend/stage-2-task-2.2A-centralized-accounts.md),
  [contract authorization Task 2.3](docs/backend/stage-2-task-2.3-authorization.md),
  [contract provisioning Task 2.4](docs/backend/stage-2-task-2.4-provisioning.md),
  [hạ tầng runtime Mosquitto Task 2.5](docs/backend/stage-2-task-2.5-mosquitto-runtime.md),
  [sổ tay vận hành MQTT Credential Task 2.6](docs/backend/stage-2-task-2.6-mqtt-credentials.md) và
  [nghiệm thu E2E Task 2.7](docs/backend/stage-2-task-2.7-acceptance.md)
  để phân biệt phần đã triển khai với telemetry/WebSocket/device control tương lai.

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
   - Chạy MQTTS (port `8883` công khai qua TLS, `18830` cục bộ cho mTLS/TCP nội bộ).
   - Phân biệt hai cơ chế xác thực và vòng đời runtime:
     - **Mô hình Static Legacy (Task 2.5 baseline)**: Xác thực đọc từ file tĩnh `password_file` (`/mosquitto/auth/passwd`) trong named volume `mosquitto_auth`, atomic replace có lock/fsync, reload qua Unix domain socket sidecar (`mosquitto-reloader`), phân quyền qua file `config/mosquitto/acl` với pattern `%u` cách ly từng Gateway. File `config/mosquitto/passwd` hiện là template prototype tracked trong git và không tự động import vào volume mới.
     - **Chu trình sống Dynamic Security (DynSec - Task 2.6 đã triển khai)**: Sử dụng plugin Mosquitto Dynamic Security (`mosquitto_dynamic_security.so`), điều khiển qua Go DynSec adapter. Cơ chế Ingress Gate fail-closed: khởi động ở trạng thái `CLOSED`, chỉ chuyển sang `OPEN` sau khi đối soát authority DB, epoch và recovery checkpoint; hỗ trợ bảo trì toàn cục (global maintenance: `CloseDrain`, cold restart, fresh verification); khi rotate credential sẽ ngắt session cũ và từ chối CONNECT cũ; khi revoke sẽ ngắt active session ngay lập tức và từ chối fresh CONNECT; ghi nhật ký audit bất biến trong CSDL (`mqtt_credential_events`). Mặc định môi trường triển khai tắt API này (`MQTT_CREDENTIAL_API_ENABLED=false`).
   - Phân quyền theo topic với pattern `%u` cách ly tuyệt đối từng Gateway: telemetry, status, responses (Gateway publish), acks, commands (Gateway subscribe).
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
├── client_rathole.toml.example      # Mẫu cấu hình client Rathole chuyển tiếp cổng MQTT TCP
├── docker-compose.yml               # Cấu hình toàn bộ stack dịch vụ (8 container)
├── AGENTS.md                        # Đặc tả hệ thống, MVP scope và quyết định kiến trúc
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI pipeline: lint, test, cross-compile, migration, docker smoke
├── docs/
│   └── protocols/
│       └── mqtt-v1.md               # Đặc tả giao thức MQTT v1, payload batch, retry, command và URN
├── migrations/                      # SQL migrations cho Database (đến version 16)
│   ├── 000000_configure_supabase_roles.sh # Provision và xoay vòng role database cho Supabase Auth/Storage
│   ├── 000001_init_schema.up.sql    # Relational entities và telemetry hypertable
│   ├── 000002_digital_twin_tables.up.sql # Digital Twin, command, outbox và temporal hypertable
│   ├── 000003_supabase_roles.up.sql # Khởi tạo role cho GoTrue Auth và Storage
│   ├── 000004_supabase_compat.up.sql# Khóa ngoại profiles-auth.users, trigger user mới và RLS policies
│   ├── 000005_seed_dev_data.up.sql  # Gateway, sensor và Digital Twin mẫu cho development
│   ├── 000006_add_gateway_ownership_columns.up.sql # Cột ownership và desired-state actor
│   ├── 000007_backfill_twin_gateway_ownership.up.sql # Backfill ownership cho entity hiện hữu
│   ├── 000008_schema_hardening.up.sql # FK, CHECK, UNIQUE và gỡ retention mặc định
│   ├── 000009_migration_tracking_and_sensor_urn.up.sql # Theo dõi version và URN Sensor theo Gateway
│   ├── 000010_stage2_auth_and_provisioning.up.sql # Bảng platform_admins, gateway_mqtt_credentials và ràng buộc định danh
│   ├── 000011_mqtt_credential_operations.up.sql # Nhật ký audit bất biến mqtt_credential_events và idempotency
│   ├── 000012_mqtt_credential_maintenance.up.sql # Quản lý khóa bảo trì toàn cục mqtt_credential_maintenance
│   ├── 000013_mqtt_maintenance_admission.up.sql # Ràng buộc trạng thái và điều kiện chấp thuận bảo trì
│   ├── 000014_mqtt_credential_recovery.up.sql # Điểm checkpoint và cơ chế xử lý khôi phục sự cố
│   ├── 000015_mqtt_recovery_authority_guards.up.sql # Rào chắn bảo vệ quyền thẩm định và khôi phục
│   └── 000016_mqtt_event_authority_guard.up.sql # Rào chắn trigger đảm bảo tính bất biến của audit log
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
    Sinh signing secret ngẫu nhiên, lưu kín vào `JWT_SECRET` trong `.env`:
    ```bash
    openssl rand -hex 32
    ```
    Không dùng sample secret/placeholder; không commit hoặc chia sẻ output.
    `ANON_KEY` và `SERVICE_ROLE_KEY` phải ký bằng cùng secret. Script hiện có
    `scripts/gen-keys.py` hỗ trợ `JWT_SECRET` đã export, nhưng cũng có **public
    fallback** nếu không cấu hình và in keys ra stdout. Không truyền secret qua
    argv (lộ process listing/history), không bật `set -x`; bảo vệ output và
    kiểm tra biến không rỗng nếu dùng cho local. Script chưa phải workflow
    production an toàn cho tới khi được sửa/kiểm tra riêng. Không có xác nhận
    secret đã rotate; đổi secret không mặc nhiên thu hồi refresh sessions.

    Cấu hình `GOTRUE_JWT_ISSUER=http://localhost/auth/v1` cho local;
    production dùng public HTTPS issuer đã chọn. Compose mapping biến này vào
    `SUPABASE_JWT_ISSUER` của Go, audience/role phải là `authenticated`.
    Token cũ thiếu issuer phải refresh/login lại. `service_role` không phải
    human platform admin và không được cấp cho Gateway/Flutter. Xem
    [hướng dẫn Task 2.2](docs/backend/stage-2-task-2.2-authentication.md).

    MVP áp dụng mô hình quản trị tài khoản tập trung:
    - **Yêu cầu kiến trúc (Architecture Requirement)**: Hệ thống cấm hoàn toàn tính năng tự đăng ký công khai (no public self-signup). Mọi tài khoản người dùng phải được tạo bởi platform administrator thông qua Supabase Auth Admin API (hoặc Supabase Studio được bảo vệ). Trong file `.env` và Docker Compose, bắt buộc cấu hình `GOTRUE_DISABLE_SIGNUP=true` (Compose mặc định `true`).
    - **Thực tế kiểm chứng (Verification Status)**: Việc ẩn nút đăng ký trên giao diện người dùng (Flutter/Web) chỉ là biện pháp hình thức bên ngoài (purely cosmetic) và không mang giá trị bảo mật. Tính năng cấm đăng ký phải được kiểm chứng kỹ thuật bằng cách gửi request trực tiếp `POST /auth/v1/signup` tới Nginx / Envoy API Gateway và nhận phản hồi lỗi HTTP `422 signup_disabled`, không tạo ra bất kỳ bản ghi nào trong `auth.users`, `profiles`, hay `user_gateways`. Trong môi trường prototype / local chưa thực hiện bài kiểm tra trực tiếp này, không thể mặc định coi là hệ thống đã được khóa hoàn toàn.
    - **Không tự cấp quyền**: Việc tạo tài khoản không tự động cấp quyền truy cập Gateway hay quyền platform admin (`platform_admins`). Quyền truy cập Gateway phải được platform admin phân quyền tường minh qua bảng `user_gateways`.
    - Quy trình bootstrap, áp dụng vào stack cũ và giới hạn kiểm chứng tại [Task 2.2A](docs/backend/stage-2-task-2.2A-centralized-accounts.md).

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

Compose yêu cầu `JWT_SECRET`, `GOTRUE_JWT_ISSUER` và
`SUPABASE_JWT_AUDIENCE=authenticated`; backend nhận các biến
`SUPABASE_JWT_SECRET`/`SUPABASE_JWT_ISSUER` qua mapping Compose.
`SUPABASE_JWT_CLOCK_SKEW` mặc định `30s` (0–5m),
`AUTHORIZATION_TIMEOUT` mặc định `2s` cho lookup platform admin.

- **Cấu hình tiến trình & kết nối CSDL**:
  - `SERVER_PORT` (mặc định: `8080`), `SERVER_ENV` (`development` / `production`).
  - `DATABASE_URL`: URI kết nối PostgreSQL bắt buộc có scheme `postgres://` hoặc `postgresql://`.
  - `DATABASE_MAX_CONNS` (10), `DATABASE_MIN_CONNS` (1), `DATABASE_CONNECT_TIMEOUT` (10s).
  - Timeouts HTTP: `READINESS_TIMEOUT` (2s), `HTTP_READ_HEADER_TIMEOUT` (5s), `HTTP_READ_TIMEOUT` (15s), `HTTP_WRITE_TIMEOUT` (30s), `HTTP_IDLE_TIMEOUT` (60s), `SHUTDOWN_TIMEOUT` (10s).
  - Giới hạn MQTT: `MQTT_QUEUE_CAPACITY` (256), `MQTT_WORKER_COUNT` (4), `MQTT_MAX_PAYLOAD_BYTES` (1MB).

```bash
docker compose up -d backend
docker compose exec backend wget -qO- http://127.0.0.1:8080/healthz
docker compose exec backend wget -qO- http://127.0.0.1:8080/readyz
```

- `/healthz`: Liveness probe chỉ xác nhận tiến trình HTTP còn hoạt động (trả về `OK`).
- `/readyz`: Readiness probe xác nhận backend hiện truy cập được CSDL PostgreSQL (`{"status":"ready"}` HTTP 200 hoặc safe error envelope `service_unavailable` HTTP 503).
- Toàn bộ HTTP request/response được gán hoặc chuyển tiếp header `X-Request-ID` (tự động tạo UUIDv4 nếu chưa có) phục vụ distributed tracing.
- `docker compose stop backend` gửi tín hiệu `SIGTERM` để backend dừng tiếp nhận kết nối mới, hoàn tất request đang xử lý trong thời gian `SHUTDOWN_TIMEOUT` và đóng connection pool PostgreSQL an toàn.

### 3.3 Áp dụng migration

Hệ thống quản lý schema CSDL thông qua chuỗi **16 migrations** (`000001` đến `000016`):
- Các migration `000001` đến `000009` trong `/docker-entrypoint-initdb.d` chỉ tự chạy khi PostgreSQL khởi tạo data volume mới.
- Từ version 10 trở đi (`000010` đến `000016`), service one-shot `application-migrations` sử dụng bảng `schema_migrations` và PostgreSQL advisory lock để tự động áp dụng các migration chưa chạy cho cả volume mới và volume hiện hữu đã baseline:
  - `000010`: Bổ sung bảng `platform_admins`, `gateway_mqtt_credentials`, ràng buộc định dạng định danh Gateway/Sensor, và phân quyền tối thiểu (least-privilege) cho role `iot_backend_app`.
  - `000011`: Bổ sung bảng nhật ký audit bất biến `mqtt_credential_events`, quản lý thế hệ `generation` và khóa `idempotency_key`.
  - `000012`: Bổ sung bảng khóa bảo trì toàn cục `mqtt_credential_maintenance`.
  - `000013`: Bổ sung ràng buộc trạng thái bảo trì và điều kiện tiếp nhận (admission constraints).
  - `000014`: Quản lý các điểm checkpoint khôi phục sự cố (`recovery_checkpoint`) và phương án xử lý (`recovery_disposition`).
  - `000015`: Bổ sung rào chắn bảo vệ quyền thẩm định và ranh giới phục hồi lỗi.
  - `000016`: Trigger và rào chắn bảo vệ tính bất biến tuyệt đối của nhật ký audit (`mqtt_event_authority_guard`).

Migration `000009` tạo bảng `schema_migrations` sau khi schema hiện hữu đã được đối chiếu. Trên database hiện hữu, chỉ áp dụng migration này sau khi `000006`–`000008` đã được xác minh; không chạy lại các migration hardening khi constraint đã tồn tại.

`AUTH_DB_PASSWORD`, `STORAGE_DB_PASSWORD` và `BACKEND_DB_PASSWORD` là ba mật
khẩu riêng. Service one-shot `supabase-role-provisioner` tạo hoặc xoay vòng các role database trước
khi GoTrue và Storage khởi động; không dùng lại mật khẩu PostgreSQL superuser
cho các dịch vụ này. Password phải có ít nhất 22 ký tự URL-safe. Backend kết
nối bằng `iot_backend_app`, không dùng PostgreSQL superuser; role này bị tước
toàn bộ quyền DDL, không thể chỉnh sửa `schema_migrations` và không thể xóa dữ liệu audit log.

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

Sau khi database đã có baseline version 9, các migration từ `000010` đến `000016` được áp dụng tự
động khi chạy Compose thông qua service `application-migrations`. Để kiểm chứng toàn diện quy trình nâng cấp CSDL, chạy:

```bash
sh scripts/test-stage2-migrations.sh
```

Script sẽ kiểm tra: cài đặt mới (clean install), nâng cấp tuần tự từ version 9 lên 16, thực thi đồng thời hai runner, chạy kiểm tra tính lũy kế (idempotency), chạy toàn bộ các file xác minh `scripts/sql/verify-migration-000010.sql` đến `000016.sql`, và xác nhận role `iot_backend_app` bị từ chối mọi thao tác DDL hay can thiệp vào `schema_migrations`.

Xem thêm quy trình Task 2.1, backend role và bootstrap platform admin tại [Tài liệu Task 2.1](docs/backend/stage-2-task-2.1-migrations.md).

### 3.4 Kiểm thử tự động & CI (Continuous Integration)

Repository tích hợp kiểm thử qua GitHub Actions (`.github/workflows/ci.yml`), dùng Go **1.27.1**, hiện bao gồm **10 jobs CI độc lập**:

1. **`lint-and-test`**:
   - Kiểm tra định dạng code Go với `gofmt`.
   - Phân tích tĩnh code bằng `go vet` và `golangci-lint` (v2.14.0).
   - Chạy toàn bộ Unit Tests với bộ phát hiện tương tranh (`-race`) và xuất báo cáo độ bao phủ (`coverage.out`):
     ```bash
     cd src && go test -v -race -coverprofile=coverage.out ./...
     ```
2. **`cross-compile`**:
   - Biên dịch độc lập Go Backend cho `linux/amd64`.
   - Biên dịch chéo Gateway Simulator (`linux/armv7` với `GOARM=7`) dành cho mục tiêu phần cứng bo TI AM5728.
   - Biên dịch chéo các công cụ runtime phụ trợ (`mosquitto-auth-init`, `mosquitto-reloader`) cho cả `linux/amd64` và `linux/armv7`.
3. **`migration-check`**:
   - Khởi động container TimescaleDB độc lập với cấu hình phân quyền ngẫu nhiên (`ci_admin`, `ci_custom_db`).
   - Tự động kiểm tra chuỗi migration đầy đủ đến **version 16** (`000016_mqtt_event_authority_guard.up.sql`) và bảng theo dõi `schema_migrations`.
   - Chạy toàn bộ các bộ script xác minh schema hardening, URN, platform admin, credential metadata, maintenance locks, recovery checkpoints và terminal event guard (`verify-migration-000009.sql` đến `000016.sql`).
   - Giả lập bảng `auth.users`, kiểm tra migration tương thích Supabase (`000004_supabase_compat.up.sql`), xác minh 6 policies RLS, trigger tự động tạo profile và khóa ngoại liên kết.
   - Kiểm tra khả năng kết nối độc lập của các role CSDL Supabase và quyền tối thiểu của `iot_backend_app`.
   - Chạy kiểm tra nâng cấp CSDL Stage 2 qua `sh scripts/test-stage2-migrations.sh`.
4. **`docker-build`**:
   - Đóng gói các container images (`backend`, `auth-init`, `reloader`) qua Docker Buildx.
   - Khởi chạy container backend song song với TimescaleDB, thực hiện smoke test liveness (`/healthz`) và readiness probe (`/readyz`) qua `sh scripts/test-backend-smoke.sh`.
5. **`authorization-integration`**:
   - Kiểm chứng repository Gateway/Sensor bằng PostgreSQL thật dưới quyền `iot_backend_app`: cách ly người dùng (user isolation), ma trận vai trò (role matrix), metadata nullable, thu hồi quyền thành viên (membership revocation) và kiểm tra deadline.
   - Từ repo root: `sh scripts/test-stage2-authorization.sh`; sử dụng CSDL isolated, không đọc/sửa file `.env` deployment.
   - Kiểm tra API đọc danh sách Gateway qua router thật, CSDL PostgreSQL và token JWT; kiểm tra API Sensor, phân biệt rõ Gateway rỗng với từ chối truy cập và timeout khi CSDL bị khóa.
   - Dọn dẹp an toàn các anonymous volume thuộc container test; lỗi cleanup khiến harness trả về mã lỗi nonzero.
   - Chi tiết contract và kết quả tại [Tài liệu Task 2.3](docs/backend/stage-2-task-2.3-authorization.md).
6. **`provisioning-integration`**:
   - Kiểm chứng trên PostgreSQL isolated dưới quyền `iot_backend_app` với router thật: kiểm tra tính nguyên tử (atomic) khi tạo Gateway/Sensor/Twin, cơ chế thử lại (retry), xử lý xung đột (conflict), rollback khi gặp lỗi, tương tranh (concurrency), timeout và thu hồi quyền admin.
   - Từ repo root: `sh scripts/test-stage2-provisioning.sh`; không đọc/sửa file `.env`.
   - Chi tiết contract, câu lệnh và giới hạn bằng chứng tại [Tài liệu Task 2.4](docs/backend/stage-2-task-2.4-provisioning.md).
7. **`auth-integration`**:
   - Kiểm thử hồi quy xác thực bằng harness Python, kiểm chứng cấm đăng ký công khai (`POST /auth/v1/signup` trả về `422 signup_disabled`), tạo tài khoản fixture qua Auth Admin API, đăng nhập/làm mới token GoTrue qua Nginx/Envoy và bảo vệ platform admin trong PostgreSQL.
   - Sử dụng access token GoTrue thật truyền qua Nginx tới Go backend để kiểm chứng cách ly Gateway/Sensor, platform admin không bypass quyền đọc nếu chưa có membership, và việc thu hồi quyền thành viên có hiệu lực ngay ở request kế tiếp.
   - So sánh Sensor metadata/NULL/UTC với fixture và kiểm tra Gateway không tồn tại/không có quyền đều trả về `404` qua proxy.
   - Từ repo root: `sh scripts/test-stage2-auth.sh` và `sh scripts/test-stage2-admin.sh`. Chi tiết tại [Tài liệu Task 2.2](docs/backend/stage-2-task-2.2-authentication.md).
8. **`mosquitto-credential-integration`**:
   - Kiểm chứng toàn diện 4 endpoint Admin MQTT Credential thông qua HTTPS Nginx, PostgreSQL thật, và Mosquitto Dynamic Security (DynSec) adapter.
   - Kiểm chứng hàng rào Ingress Gate fail-closed (khởi động `CLOSED`, chỉ `OPEN` sau khi đối soát authority DB, epoch và checkpoint).
   - Kiểm chứng chu trình đột biến credential (provision cấp mới secret 1 lần, rotate trong cửa sổ bảo trì toàn cục với drain/cold restart, revoke ngắt active session ngay lập tức và cấm fresh CONNECT).
   - Kiểm chứng cơ chế startup recovery, đối soát sau sự cố (fault injection), bảo vệ tính bất biến của nhật ký audit (`mqtt_credential_events`) và xuất báo cáo độ bao phủ code.
   - Từ repo root: `sh scripts/test-stage2-mqtt-credentials.sh` và các unit test liên quan. Chi tiết tại [Sổ tay vận hành Task 2.6](docs/backend/stage-2-task-2.6-mqtt-credentials.md).
9. **`mosquitto-runtime-integration`**:
   - Kiểm chứng hạ tầng runtime Mosquitto tĩnh legacy (Task 2.5 baseline): khởi tạo volume auth, atomic replace file mật khẩu `password_file` có lock/fsync, gửi tín hiệu reload qua Unix domain socket sidecar (không dùng Docker socket, không cần host PID).
   - Kiểm chứng kết nối MQTT TLS thật, cách ly phân quyền ACL `%u`, probe xác thực fresh connection (phân biệt rõ negative probe chỉ kiểm chứng bằng password cũ đã biết, không dùng password ngẫu nhiên), và cơ chế tự động rollback/recovery khi có sự cố.
   - Chạy 14 tests contract trong Python harness và tích hợp native Go adapter.
   - Từ repo root: `sh scripts/test-stage2-mosquitto-runtime.sh` và `python3 -m unittest discover -s scripts/tests -p 'test_stage2_mosquitto_runtime.py'`. Chi tiết tại [Tài liệu Task 2.5](docs/backend/stage-2-task-2.5-mosquitto-runtime.md).
10. **`stage2-e2e`**:
   - Kiểm thử tích hợp đầu cuối toàn diện cho Stage 2 (tích hợp chuỗi GoTrue thật, Envoy gateway, Nginx reverse proxy, Go backend, PostgreSQL/TimescaleDB và Mosquitto DynSec Ingress Gate).
   - Được giám sát bởi supervisor CI (`scripts/tests/stage2_ci.py`) với thời gian chờ tối đa 30 phút (`timeout-minutes: 30`).
   - Thực thi 2 lượt chạy độc lập, cô lập hoàn toàn (`run-1` và `run-2`), biên dịch trực tiếp từ mã nguồn checkout hiện tại, chạy toàn bộ 27 kịch bản E01–E27 qua tất cả các selector (8 bộ chọn).
   - Thu thập báo cáo có cấu trúc đã qua quét an toàn không rò rỉ secret/token (`safe-reports`), tự động kiểm chứng dọn dẹp sạch sẽ tài nguyên sở hữu (`verified owned cleanup`), kể cả trường hợp hủy hoặc thất bại (`if: always()`).
   - Các lượt chạy cục bộ tương đương CI (CI-equivalent local runs) đã PASS theo báo cáo của worker. Chi tiết tại [Báo cáo nghiệm thu Task 2.7](docs/backend/stage-2-task-2.7-acceptance.md).

*Lưu ý về tình trạng kiểm chứng và nghiệm thu CI*:
- Việc pipeline CI đạt trạng thái xanh trên remote repository và chứng nhận commit exact-final-SHA hiện vẫn ở trạng thái **PENDING** (chưa có xác nhận độc lập thông qua exact remote run URL và final commit SHA cụ thể; không đưa ra tuyên bố remote 10/10 hay toàn bộ Stage 2 đã hoàn tất sẵn sàng production).
- Các cổng chuyển đổi và kích hoạt trên môi trường triển khai thực tế (production rollout gates) tiếp tục được giữ tách biệt, phụ thuộc vào kiểm chứng topology Rathole thật, chính sách ngắt CSDL sau Ingress Gate OPEN và quy trình nghiệm thu bàn giao secret phần cứng.
- Không tuyên bố các công cụ vận hành dự kiến chưa có, các API quản lý tài khoản tương lai, hay toàn bộ Stage 2 là đã hoàn thành.

---

## 4. Đặc tả API & Giao thức (API & Protocol Specs)

### 4.1 REST API (Go Backend)

Mọi HTTP request/response của Go Backend đều được gán hoặc bảo toàn header truy vết `X-Request-ID`. Hệ thống phân định rõ ràng 4 nhóm trạng thái endpoint:

#### 1. Nhóm Endpoint đã triển khai (Implemented):
Các route này đã được đăng ký trong router Go backend, có handler xử lý logic nghiệp vụ, phân quyền và kiểm thử tương ứng:

| Phương thức | Đường dẫn | Xác thực & Quyền hạn | Mô tả chi tiết | Trạng thái hiện tại |
|---|---|---|---|---|
| `GET` | `/healthz` | Không yêu cầu | Liveness probe kiểm tra tiến trình HTTP backend còn sống | Hoạt động (HTTP 200 `OK`) |
| `GET` | `/readyz` | Không yêu cầu | Readiness probe kiểm tra kết nối CSDL PostgreSQL | Hoạt động (HTTP 200 `{"status":"ready"}` hoặc 503) |
| `GET` | `/v1/health` | Không yêu cầu | Healthcheck dịch vụ Go backend | Hoạt động (HTTP 200 `{"status":"running",...}`) |
| `GET` | `/v1/gateways` | Bearer JWT `authenticated` | Lấy danh sách Gateway người dùng được cấp quyền theo `user_gateways`. Platform admin không bypass nếu chưa có membership | Đã triển khai: HTTP 200 `{"items":[...]}`, HTTP 401 nếu thiếu/sai JWT |
| `GET` | `/v1/gateways/{gateway_id}/sensors` | Bearer JWT `authenticated` | Lấy danh sách Sensor thuộc Gateway mà người dùng có quyền truy cập | Đã triển khai: HTTP 200 `{"items":[...]}`, HTTP 404 nếu Gateway không tồn tại hoặc không thuộc quyền, HTTP 401 |
| `GET` | `/v1/me/permissions` | Bearer JWT `authenticated` | Khám phá quyền hạn UI (trả về `{"is_platform_admin": true/false}` theo bảng `platform_admins` trong PostgreSQL; không cấp bypass quyền) | Đã triển khai: HTTP 200 `{"is_platform_admin":...}`, HTTP 401 nếu thiếu/sai JWT |
| `PUT` | `/v1/admin/gateways/{gateway_id}` | Bearer JWT + `platform_admins` | Platform admin tạo mới Gateway, gán owner ban đầu và tạo thực thể Digital Twin trong cùng 1 transaction | Đã triển khai: HTTP 201 tạo mới, HTTP 200 retry no-op, HTTP 409 conflict, HTTP 401/403 |
| `PUT` | `/v1/admin/gateways/{gateway_id}/sensors/{sensor_id}` | Bearer JWT + `platform_admins` | Platform admin tạo mới Sensor, tạo thực thể Digital Twin và quan hệ `hasSensor` trong cùng 1 transaction | Đã triển khai: HTTP 201 tạo mới, HTTP 200 retry no-op, HTTP 409 conflict, HTTP 404 nếu Gateway cha không tồn tại, HTTP 401/403 |
| `GET` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` | Lấy metadata credential MQTT của Gateway (không trả về secret hoặc hash) | Đã triển khai: HTTP 200 metadata (mặc định deployment tắt: HTTP 503), HTTP 404, HTTP 401/403 |
| `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` + `Idempotency-Key` | Cấp mới credential MQTT cho Gateway kèm password Base64URL CSPRNG dùng 1 lần | Đã triển khai: HTTP 201 kèm secret lần đầu; replay cùng key trả về 201 metadata-only; HTTP 409 nếu đã có credential active; HTTP 503 nếu deployment tắt; HTTP 401/403 |
| `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential/rotate` | Bearer JWT + `platform_admins` + `Idempotency-Key` | Xoay vòng credential MQTT trong cửa sổ bảo trì toàn cục (drain/cold restart) | Đã triển khai: HTTP 200 kèm secret mới 1 lần; replay cùng key trả về 200 metadata-only; HTTP 409 nếu không active; HTTP 503 nếu deployment tắt; HTTP 401/403 |
| `DELETE` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Bearer JWT + `platform_admins` + `Idempotency-Key` | Thu hồi vĩnh viễn (revoke) credential MQTT của Gateway, lập tức ngắt session active và cấm fresh CONNECT | Đã triển khai: HTTP 200 revoked; HTTP 503 nếu deployment tắt; HTTP 401/403 |

*Lưu ý về 4 endpoint Admin MQTT Credential*: Các route này đã được lập trình hoàn chỉnh trong code và kiểm thử trong harness test, nhưng trên cấu hình deployment thực tế mặc định bị vô hiệu hóa qua biến `MQTT_CREDENTIAL_API_ENABLED=false` (trả về lỗi an toàn HTTP `503 credential_runtime_disabled`).

#### 2. Nhóm Endpoint Stub có xác thực (Authenticated Stubs):
Các route này đã được khai báo và đăng ký trong router Go backend nhưng chưa triển khai logic nghiệp vụ; yêu cầu Bearer JWT hợp lệ trước khi trả về HTTP `501 Not Implemented`:

| Phương thức | Đường dẫn | Xác thực & Quyền hạn | Mô tả chi tiết | Trạng thái hiện tại |
|---|---|---|---|---|
| `GET` | `/v1/telemetry/history` | Bearer JWT `authenticated` | Truy vấn chuỗi lịch sử đo lường cảm biến (hỗ trợ downsampling `time_bucket`) | HTTP 401 nếu thiếu/sai JWT; HTTP 501 Not Implemented nếu JWT hợp lệ |
| `GET` | `/v1/ws` | Bearer JWT `authenticated` (header) | Nâng cấp kết nối WebSocket để streaming dữ liệu cảm biến thời gian thực | HTTP 401 nếu thiếu/sai JWT; HTTP 501 Not Implemented nếu JWT hợp lệ |
| `GET` | `/v1/digital-twins` | Bearer JWT `authenticated` | Lấy danh sách thực thể Digital Twin mà người dùng có quyền truy cập | HTTP 401 nếu thiếu/sai JWT; HTTP 501 Not Implemented nếu JWT hợp lệ |

*Ghi chú*: Cả 3 stub trên bắt buộc xác thực token người dùng trước khi phản hồi `501`. Mã lỗi `501` chỉ phản ánh việc endpoint chưa có handler nghiệp vụ, tuyệt đối không được coi là bằng chứng đã kiểm tra quyền User–Gateway thành công.

#### 3. Nhóm Endpoint dự kiến theo thiết kế MVP (Planned Endpoints):
Các endpoint dưới đây thuộc thiết kế kiến trúc MVP của đồ án nhưng **chưa được đăng ký trong router** Go backend. Mọi request gửi tới các đường dẫn này sẽ nhận phản hồi HTTP `404 Not Found` (hoặc `405 Method Not Allowed` nếu path trùng route khác):

| Phương thức | Đường dẫn | Xác thực dự kiến | Mô tả theo thiết kế MVP |
|---|---|---|---|
| `POST` | `/v1/digital-twins` | Bearer JWT (Supabase) | Tạo mới thực thể Digital Twin (Gateway / Sensor / Device) |
| `GET` | `/v1/digital-twins/{entity_id}` | Bearer JWT (Supabase) | Lấy thông tin chi tiết thực thể (định dạng NGSI-LD JSON) |
| `PATCH` | `/v1/digital-twins/{entity_id}` | Bearer JWT (Supabase) | Cập nhật metadata / thuộc tính tĩnh của thực thể |
| `GET` | `/v1/digital-twins/{entity_id}/state` | Bearer JWT (Supabase) | Xem trạng thái hiện thời (`reported_state` vs `desired_state`) |
| `PATCH` | `/v1/digital-twins/{entity_id}/desired-state` | Bearer JWT (Supabase) | Đặt cấu hình mong muốn (tạo lệnh và outbox trong transaction) |
| `GET` | `/v1/digital-twins/{entity_id}/history` | Bearer JWT (Supabase) | Truy vấn lịch sử thuộc tính biến thiên theo thời gian (TimescaleDB) |
| `POST` | `/v1/digital-twins/{entity_id}/commands` | Bearer JWT (Supabase) | Phát lệnh điều khiển trực tiếp (vd: reboot, capture_image) |
| `GET` | `/v1/digital-twins/{entity_id}/commands` | Bearer JWT (Supabase) | Lấy danh sách chỉ lệnh đã phát cho thực thể |
| `GET` | `/v1/commands/{command_id}` | Bearer JWT (Supabase) | Kiểm tra trạng thái thực thi của command |
| `POST` | `/v1/media/upload-url` | Gateway HTTP Credential riêng | Cấp Signed Upload URL cho Gateway tải ảnh chụp lên private Storage |

*Lưu ý quan trọng về các tuyến đường không tồn tại*:
- Router Go backend hiện **KHÔNG CÓ** endpoint xem chi tiết Gateway đơn lẻ (`GET /v1/gateways/{gateway_id}`) hay Sensor đơn lẻ (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`). Việc đọc danh sách Sensor được thực hiện thông qua route cha `GET /v1/gateways/{gateway_id}/sensors`.
- Hệ thống hiện **KHÔNG CÓ** REST API công khai hay admin để quản lý thành viên (`user_gateways`). Việc phân quyền, thay đổi vai trò (Owner / Operator / Viewer) hoặc thu hồi quyền thành viên Gateway được thực hiện thông qua công cụ vận hành nội bộ được bảo vệ (protected operator tooling) can thiệp trực tiếp CSDL. Không tuyên bố các API quản lý tài khoản tương lai này là đã hoàn thành.

#### 4. Danh mục tính năng tạm hoãn ngoài phạm vi MVP (Deferred Features per AGENTS.md §2):
Các tính năng sau đây đã được quyết định loại bỏ hoặc hoãn lại để đảm bảo tính khả thi cho đồ án đơn thành viên, tránh làm phức tạp kiến trúc và giao diện:
- **gRPC và gRPC-Web**: Toàn bộ giao tiếp sử dụng REST và WebSocket nhằm loại bỏ chi phí Protobuf, code-generation và proxy phức tạp.
- **Đăng ký công khai người dùng**: Tự đăng ký (`/auth/v1/signup`) bị vô hiệu hóa; chỉ platform admin được tạo tài khoản người dùng.
- **Gateway tự đăng ký hoặc tự nhận quyền**: Không hỗ trợ Gateway self-claiming hoặc automated enrollment; Gateway và credential chỉ do platform admin cấp phát tập trung.
- **Context Broker NGSI-LD bên thứ ba**: Không triển khai Scorpio hay Orion-LD; CSDL PostgreSQL/TimescaleDB là nguồn chân lý duy nhất (source of truth) và Go backend tự xuất định dạng NGSI-LD tại biên API.
- **Live-video streaming & Media Server**: Không hỗ trợ truyền video trực tiếp (RTSP/WebRTC/HLS) hay media server chuyên dụng; phân hệ media chỉ hỗ trợ lưu trữ ảnh tĩnh qua Supabase Storage.
- **Tải lên video lớn, Resumable/TUS upload & Chuyển mã (Transcoding)**: Nằm ngoài phạm vi của MVP.
- **Phân hệ cảnh báo hoàn chỉnh (Full Alert Engine)**: Tạm hoãn; tập trung vào thu thập, lưu trữ và Digital Twin.
- **Lệnh điều khiển tự trị hoặc sinh bởi LLM**: Toàn bộ chỉ lệnh điều khiển phải xuất phát từ người dùng được ủy quyền qua REST API.
- **Ứng dụng web quản trị tùy biến (Custom Admin App)**: Tạm hoãn; việc quản trị vận hành sử dụng Supabase Studio và công cụ dòng lệnh được bảo vệ.

### 4.2 Supabase Auth & Storage API (qua Nginx / Envoy API Gateway)

| Đường dẫn | Dịch vụ tiếp nhận | Xác thực | Mô tả & Trạng thái hiện tại |
|---|---|---|---|
| `POST /auth/v1/signup` | Supabase GoTrue | API Key | Đăng ký tài khoản công khai: **BỊ VÔ HIỆU HÓA** (`GOTRUE_DISABLE_SIGNUP=true`, trả về HTTP 422 `signup_disabled`). Phải được kiểm chứng độc lập ở biên API thay vì chỉ dựa vào việc ẩn nút trên giao diện. |
| `POST /auth/v1/token?grant_type=password` | Supabase GoTrue | API Key + Email/Password | Đăng nhập tài khoản người dùng, trả về cặp `access_token` (JWT HS256) và `refresh_token`. |
| `POST /auth/v1/token?grant_type=refresh_token` | Supabase GoTrue | API Key + Refresh Token | Làm mới phiên đăng nhập, cấp `access_token` mới khi token cũ hết hạn. |
| `POST /storage/v1/object/sign/*` | Supabase Storage API | Service Role (Backend) | Go backend tạo Signed Upload/Read URL có thời hạn cho private bucket `media-images`. |
| `PUT /storage/v1/object/*` | Supabase Storage API | Signed URL | Gateway upload trực tiếp tệp ảnh qua HTTPS lên private storage thông qua Signed Upload URL do backend cấp. |
| `GET /storage/v1/object/*` | Supabase Storage API | Signed URL | Client tải ảnh chụp từ private storage thông qua Signed Read URL do backend cấp sau khi kiểm tra quyền User–Gateway. |

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

Thay vì tích hợp Federated Learning (đã được loại bỏ để tập trung vào mục tiêu trọng tâm của đồ án), hệ thống thiết kế phân hệ **Digital Twin** theo kiến trúc Modular Monolith bên trong Go Backend. Các mục dưới mô tả hành vi mục tiêu; schema đã có nhưng business handlers/đường điều khiển chưa hoàn thành:

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
