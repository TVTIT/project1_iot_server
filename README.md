# IoT Gateway-Server Platform (Đồ án 1 - ET3290)

Hệ thống IoT Gateway–Server phục vụ thu thập dữ liệu cảm biến thời gian thực từ các Gateway không đồng nhất (heterogeneous Gateways), lưu trữ chuỗi thời gian (time-series), hỗ trợ truy vấn lịch sử, streaming dữ liệu thời gian thực qua WebSocket, xác thực và phân quyền người dùng thông qua Supabase Auth/RLS, quản lý tải lên media qua private storage và hỗ trợ huấn luyện phân tán (Federated Learning) bằng Go thuần (gonum).

---

## 1. Kiến trúc tổng thể (System Architecture)

```text
[Heterogeneous Gateways: ESP32 / Luckfox / AM5728]
   │
   ├── Telemetry (MQTT over TLS / Port 8883) ─────────────► [Mosquitto Broker]
   │                                                               │
   │                                                        (internal mTLS/TCP)
   │                                                               ▼
   │                                                       [Go Backend Monolith]
   │                                                               │
   │                                                               ├──► PostgreSQL 16 + TimescaleDB
   │                                                               │    (Hypertable, Columnar Compression, RLS)
   │                                                               │
   ├── FL Weights & Media Upload (HTTPS signed URL)                │
   │                                                               ▼
   ▼                                                       [Supabase API Gateway (Envoy)]
[Nginx Reverse Proxy] ◄─── Cloudflare Tunnel (cloudflared)         │
   │                                                               ├──► Supabase Auth (GoTrue)
   ├── /v1/* (REST API & WebSockets) ────► Go Backend Monolith    └──► Supabase Storage API (Local FS)
   └── /auth/v1/*, /storage/v1/* ────────► Supabase Gateway
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
   - Chạy MQTTS (port `8883` công khai, `18830` cục bộ).
   - Xác thực Gateway độc lập bằng username/password (`config/mosquitto/passwd`).
   - Phân quyền theo topic (`config/mosquitto/acl`) với pattern `%u` cách ly từng Gateway.
3. **Go Backend Modular Monolith (`src/cmd/server`)**:
   - Xử lý xác thực JWT Supabase, phân quyền quan hệ User–Gateway.
   - Thu thập telemetry từ Mosquitto, deduplication bằng `processed_messages`, lưu trữ TimescaleDB.
   - API truy vấn lịch sử (`time_bucket`), WebSocket streaming realtime.
   - Cấp Signed Upload/Read URL cho media và Federated Learning model weights.
   - Quản lý vòng đời Federated Learning (FL rounds, FedAvg aggregation).
4. **PostgreSQL 16 + TimescaleDB**:
   - Dữ liệu quan hệ: `profiles`, `gateways`, `user_gateways`, `sensors`, `media_objects`, `processed_messages`.
   - Dữ liệu Federated Learning: `fl_models`, `fl_global_models`, `fl_rounds`, `fl_round_participants`, `fl_client_updates`.
   - Hypertable: Bảng `telemetry` phân vùng 1 ngày (`chunk_time_interval => '1 day'`), tự động nén columnar sau 7 ngày, retention sau 30 ngày.
   - Row Level Security (RLS) bảo vệ dữ liệu theo User–Gateway mapping (`migrations/000004_supabase_compat.up.sql`).
5. **Self-hosted Supabase Services**:
   - **Envoy Gateway (`supabase-envoy`)**: Đóng vai trò API Gateway nội bộ điều phối `/auth/v1` và `/storage/v1` (tương thích Kong alias).
   - **GoTrue Auth (`supabase-auth`)**: Quản lý người dùng, issue JWT HS256.
   - **Storage API (`supabase-storage`)**: Quản lý lưu trữ file cục bộ (private buckets).
6. **Cloudflare Tunnel (`cloudflared`)**: Xuất bản dịch vụ ra Internet an toàn mà không cần mở port NAT trực tiếp trên router.

---

## 2. Cấu trúc thư mục (Repository Structure)

```text
.
├── .env.example                     # Mẫu biến môi trường cho Docker Compose & Backend
├── docker-compose.yml               # Cấu hình toàn bộ stack dịch vụ (8 container)
├── AGENTS.md                        # Đặc tả hệ thống, MVP scope và quyết định kiến trúc
├── migrations/                      # SQL migrations cho Database
│   ├── 000001_init_schema.up.sql    # Relational entities, TimescaleDB hypertable, compression, retention
│   ├── 000002_fl_tables.up.sql      # Bảng dữ liệu Federated Learning
│   ├── 000003_supabase_roles.up.sql # Khởi tạo role cho GoTrue Auth và Storage
│   └── 000004_supabase_compat.up.sql# Khóa ngoại profiles-auth.users, trigger user mới và RLS policies
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
│   ├── init-buckets.sh              # Khởi tạo các private storage buckets (media-images, fl-models, fl-weights)
│   └── backup-db.sh                 # Sao lưu dữ liệu PostgreSQL + TimescaleDB ra file nén .sql.gz
└── src/
    ├── cmd/
    │   ├── server/                  # Điểm khởi chạy Go Backend Server
    │   └── gateway/                 # Điểm khởi chạy Gateway Simulator / FL Agent
    ├── internal/                    # Các module nghiệp vụ (auth, mqtt, telemetry, fl, etc.)
    └── pkg/                         # Thư viện dùng chung (flkernel)
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
   Script sẽ tự động tạo 3 private bucket:
   - `media-images`: Lưu trữ ảnh chụp từ Gateway.
   - `fl-models`: Lưu trữ siêu dữ liệu & manifest kiến trúc Federated Learning.
   - `fl-weights`: Lưu trữ tensor trọng số toàn cục và cập nhật từ client.

6. **Sao lưu cơ sở dữ liệu định kỳ**:
   ```bash
   chmod +x scripts/backup-db.sh
   ./scripts/backup-db.sh
   # File backup sẽ được lưu tại thư mục ./backups/
   ```

---

## 4. Đặc tả API & Giao thức (API & Protocol Specs)

### 4.1 REST API (Go Backend - `/v1/*`)

| Phương thức | Đường dẫn | Xác thực | Mô tả |
|---|---|---|---|
| `GET` | `/healthz` | Không | Healthcheck cấp Nginx/container (trả về `OK`) |
| `GET` | `/v1/health` | Không | Healthcheck Go Backend (`{"status":"running",...}`) |
| `GET` | `/v1/telemetry/history` | Bearer JWT (Supabase) | Lấy chuỗi lịch sử mẫu đo cảm biến theo khoảng thời gian |
| `GET` | `/v1/ws` | Bearer JWT / Query token | Nâng cấp kết nối WebSocket để nhận dữ liệu thời gian thực |
| `GET` | `/v1/fl/rounds/current` | Bearer Gateway/User JWT | Lấy thông tin vòng huấn luyện FL hiện tại & signed read URL |

### 4.2 Supabase Auth & Storage API (qua Nginx)

| Đường dẫn | Dịch vụ tiếp nhận | Mô tả |
|---|---|---|
| `/auth/v1/signup` | Supabase GoTrue | Đăng ký tài khoản người dùng mới (tự kích hoạt profile qua trigger) |
| `/auth/v1/token?grant_type=password` | Supabase GoTrue | Đăng nhập lấy access_token (JWT) |
| `/storage/v1/object/sign/*` | Supabase Storage API | Cấp Signed URL đọc/ghi cho private bucket |
| `/storage/v1/object/*` | Supabase Storage API | Upload/Download tệp tin trực tiếp qua Signed URL |

### 4.3 MQTT Telemetry Topic Contract

- **Port TLS**: `8883`
- **Quy tắc Topic**:
  - `gateways/<gateway_id>/telemetry/#`: Gateway publish gói tin đo lường.
  - `gateways/<gateway_id>/acks/#`: Gateway nhận application-level commit ACK từ Server.
  - `gateways/<gateway_id>/fl/cmd`: Server gửi chỉ lệnh vòng huấn luyện FL cho Gateway.
  - `gateways/<gateway_id>/fl/status`: Gateway gửi trạng thái tham gia FL cho Server.

**Định dạng gói tin telemetry (`telemetry_batch`)**:
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

---

## 5. Federated Learning (FL) Subsystem

Hệ thống sử dụng **Go unified codebase** cho cả máy chủ tổng hợp và gateway huấn luyện (sử dụng thư viện `gonum` cho tính toán ma trận và lan truyền ngược thủ công, không phụ thuộc Python):
- **Trọng số trao đổi**: Mảng `float32` nhị phân tuần tự hóa little-endian phẳng qua `encoding/binary`.
- **Tổng hợp (Server)**: Thuật toán FedAvg tích lũy trọng số theo số lượng mẫu (`num_samples`) với bộ nhớ giới hạn $O(\text{param\_count})$.
- **Kênh điều khiển & truyền dữ liệu**:
  - Control-plane: Thông điệp kích thước nhỏ qua MQTT (`gateways/<id>/fl/cmd` và `gateways/<id>/fl/status`).
  - Data-plane: Tải model toàn cục và tải lên weight cập nhật qua HTTPS Signed URL vào Supabase Storage (`fl-models`, `fl-weights`).
