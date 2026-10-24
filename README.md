# IoT Gateway-Server Platform (Đồ án 1 - ET3290)

Hệ thống IoT Gateway–Server phục vụ thu thập dữ liệu cảm biến thời gian thực từ các Gateway không đồng nhất (heterogeneous Gateways: ESP32, Luckfox Pico Plus, TI AM5728), lưu trữ chuỗi thời gian (time-series) trên TimescaleDB, hỗ trợ truy vấn lịch sử, streaming dữ liệu thời gian thực qua WebSocket, xác thực và phân quyền người dùng thông qua Supabase Auth/RLS, quản lý tải lên media (hình ảnh) qua private storage, và tích hợp phân hệ **Digital Twin** (quản lý thực thể theo chuẩn NGSI-LD, đồng bộ trạng thái `reported_state` / `desired_state`, điều khiển Gateway qua MQTT Transactional Outbox và lưu trữ lịch sử thuộc tính biến thiên theo thời gian).

Đây là kiến trúc/mục tiêu MVP, không phải toàn bộ tính năng đã hoàn thành:
- **Trạng thái phát triển hiện tại**:
  - **Giai đoạn 2 (Stage 2 Bounded Closeout)**: Đã triển khai xác thực JWT qua Supabase Auth, phân quyền platform admin (`platform_admins`), danh sách trạm/cảm biến (`/v1/gateways*`), provisioning Gateway/Sensor, và chu trình sống Dynamic Security (DynSec) Mosquitto.
  - **Giai đoạn 3 (Stage 3 Telemetry Uplink Pipeline)**: Đã triển khai pipeline thu thập telemetry từ Gateway qua MQTT TLS đến TimescaleDB, khử trùng lặp `processed_messages`, xác nhận commit-before-ACK qua MQTT application ACK, Gateway simulator với outbox SQLite bền vững, và kiểm chứng tích hợp 9 kịch bản uplink trên runner dịch vụ thật.
   - **Giai đoạn 4 (Stage 4 History & Digital Twin API - ETSI GS CIM 009 V1.9.1 Profile)**: Đã triển khai và chuẩn hóa 5 endpoint canonical Digital Twin (`GET /v1/digital-twins`, `POST /v1/digital-twins`, `GET /v1/digital-twins/{id}`, `PATCH /v1/digital-twins/{id}/attrs`, `GET /v1/digital-twins/{id}/temporal` kèm query extension `?format=simplified`).
   - **Giai đoạn 5 — TẠM DỪNG tại mốc hoàn thành Task 5.4 (24/10/2026)**: Task 5.0–5.4 đã có decimal validation, migration 23, admission/receipt nguyên tử, ba REST routes nhận desired/tra cứu command và MQTT publisher/ACL/expiry. **Chưa có** Gateway áp dụng cấu hình/durable command result (5.5), result ingress/xác nhận configuration (5.6), Stage 5 E2E/board/CI lane (5.7) và đồng bộ client/protocol đầy đủ (5.8). `202`/`published` không phải xác nhận thực thi. Không triển khai tiếp nếu chưa được yêu cầu mới. Xem [mốc bàn giao 5.4](docs/backend/stage-5-task-5.4-publisher-acl-and-expiry.md) và [kế hoạch chi tiết](../docs/backend_plan/stage5_backend_detailed_plan.md).
  - **Quản trị Người dùng & Quyền Thành viên Gateway (Admin User & Gateway Membership - Tasks U0–U8 & Follow-up D1–D5)**: Đã triển khai Migration 21 (`000021_admin_user_management.up.sql`), Migration 22 (`000022_gateway_membership_revocation.up.sql`) và 6 endpoint quản trị tập trung (`POST /v1/admin/users`, `GET /v1/admin/users`, `PATCH /v1/admin/users/{user_id}`, `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`, `PUT /v1/admin/gateways/{gateway_id}/owner`, `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}`), adapter Supabase Auth Admin (với `email_confirm: true`), cơ chế khóa Gateway cha tuần tự, cấm tự hạ quyền admin (`409 Conflict`), chuyển giao owner nguyên tử (owner cũ trở thành viewer), và thu hồi quyền thành viên viewer/operator (hoặc no-op khi không có membership: `204 No Content` body rỗng; cấm xóa owner khi chưa chuyển giao: `409 Conflict` `owner_transfer_required`; Gateway thiếu: `404 Not Found`; tuyệt đối không xóa tài khoản, profile hay global role, không đổi Gateway khác). Chi tiết xem [báo cáo quản trị người dùng & membership](docs/backend/admin-users-and-gateway-membership.md) và [kế hoạch bổ sung DELETE membership](../docs/backend_plan/gateway_membership_delete_detailed_plan.md).
    - **Tình trạng mã nguồn & kiểm chứng (24/10/2026)**: `a5f9f812f3212413f772ef60e1299f58cca43837` đã commit Task 5.3/5.4 và sửa shutdown liên quan; mô tả worktree diff trên `bcce3fe` trong handoff là lịch sử trước commit. Source terminal migration **23**, không phải xác nhận shared/live DB đã migrate. Các native/affected local checks của mốc 5.4, full Go race 20 packages, format/vet/lint/build và Python guards đã **PASS** theo [bảng evidence](docs/backend/stage-5-task-5.4-publisher-acl-and-expiry.md), không phải kết quả chạy mới của P0. **Chưa chạy** full local CI/remote CI cho revision P0, Stage 5 Gateway/result E2E, board/cảm biến thật hoặc rollout live; không gán remote PASS lịch sử cho mốc này. Chưa cập nhật đầy đủ Flutter/Bruno theo Stage 5. Xem [baseline/testbed P0](docs/backend/paper-p0-baseline-and-testbed.md) để biết kết quả riêng của profile paper.
  - **Kiểm chứng CI từ xa (Remote CI Verification)**: GitHub Actions Run [39272190456](https://github.com/TVTIT/project1_iot_server/actions/runs/39272190456) tại commit `35b6136f22c5a1c819afefc12003dba3ba613062` đã hoàn thành thành công toàn bộ **16/16 jobs**, bao gồm `lint-and-test` (job 118127109089), `stage4-api-integration` (job 118127109577), `stage3-uplink-integration` (job 118127109787) và `native-compose-smoke` (job 118129034156).
- **Mặc định môi trường triển khai & Cổng nghiệm thu riêng (Rollout Gates)**:
   - Trên môi trường triển khai chính (root deployment), tính năng quản lý credential MQTT mặc định bị **vô hiệu hóa** (`MQTT_CREDENTIAL_API_ENABLED=false`, các endpoint trả về HTTP `503 credential_runtime_disabled`).
   - Stage 5 cũng mặc định **tắt** (`STAGE5_ENABLED=false`). Giữ tắt shared/live và không mở điều khiển client tại mốc 5.4: chưa có Gateway execution/result confirmation. ACL update/store migration chỉ được kiểm chứng trên fixture sở hữu, không phải rollout live.
   - **Security contract hiện hành:** Compose chạy Mosquitto trực tiếp với TLS/auth/ACL; backend chịu API/readiness fail-closed và bounded shutdown, **không cam kết đóng broker khi backend lỗi hoặc dừng**. Gate/controller legacy chỉ còn component-only; không phải rollout gate bắt buộc cho kiến trúc này. Xem [nghiệm thu credential hiện hành](docs/backend/stage-2-current-credential-acceptance.md). Topology mạng và bàn giao secret thực tế vẫn cần nghiệm thu riêng trước rollout; không suy diễn local fixture thành production-ready.
- **Tình trạng CI & E2E**:
   - `.github/workflows/ci.yml` là nguồn danh sách job hiện hành. Báo cáo 10 jobs/27 kịch bản của Task 2.7 mô tả checkpoint legacy, không phải cam kết mọi job/scenario đó đang được chạy trong composition hiện tại.
   - Entry point credential hiện hành là `sh scripts/test-stage2-mqtt-credentials.sh`, kiểm thử native TLS/auth/ACL trên broker trực tiếp; test gate/controller legacy giữ riêng tại `scripts/test-legacy-mqtt-controller.sh`. CI credential job đã được cập nhật nhưng **chưa có remote run** cho diff 5.3/5.4.
   - Mốc 5.4 có affected local verification PASS, không tuyên bố full-local-CI hoặc remote exact-final-SHA PASS; các remote run đã nêu phía trên chỉ là evidence lịch sử.
- **Hạ tầng Mosquitto**:
   - Phân biệt hạ tầng file tĩnh legacy (Task 2.5), controller/gate legacy component và composition DynSec hiện hành. Composition hiện hành quản lý tài khoản động qua DynSec adapter và broker TLS/ACL trực tiếp; không suy diễn global gate/maintenance từ component legacy thành bảo đảm của deployment.
- Xem thêm:
  [contract xác thực và verification](docs/backend/stage-2-task-2.2-authentication.md),
  [quản trị tài khoản tập trung Task 2.2A](docs/backend/stage-2-task-2.2A-centralized-accounts.md),
  [contract authorization Task 2.3](docs/backend/stage-2-task-2.3-authorization.md),
  [contract provisioning Task 2.4](docs/backend/stage-2-task-2.4-provisioning.md),
  [hạ tầng runtime Mosquitto Task 2.5](docs/backend/stage-2-task-2.5-mosquitto-runtime.md),
  [sổ tay vận hành MQTT Credential Task 2.6](docs/backend/stage-2-task-2.6-mqtt-credentials.md),
  [kiến trúc và hướng dẫn vận hành DynSec Task 2.8a](docs/dynsec/INDEX.md) và
  [nghiệm thu E2E Task 2.7](docs/backend/stage-2-task-2.7-acceptance.md)
  để phân biệt phần đã triển khai với telemetry/WebSocket/device control tương lai.
- **Gateway Simulator (Stage 3, Task 3.5)** — công cụ phát triển phát telemetry tổng hợp qua MQTT/TLS với outbox SQLite bền vững và lệnh `run`/`pending`/`count`/`quarantine`: xem [hướng dẫn Gateway Simulator](docs/backend/stage-3-task-3.5-gateway-simulator.md).
- **P0 — testbed paper local (24/10/2026): PASS** trên fixture riêng: Auth/API/Storage, schema 23, hai Gateway/200 mẫu/commit ACK/outbox drain và denial chéo Gateway; backend quota 2 vCPU/4 GiB. Chạy từ repo root bằng `sh scripts/test-paper-p0.sh` (tự tạo fixture mới, smoke rồi cleanup). Xem [baseline, số đo và lịch sử FAIL/PASS P0](docs/backend/paper-p0-baseline-and-testbed.md). Không đổi backend/API, không nghiệm thu production/benchmark, không tự mở P1 hoặc Stage 5.
- **P1 — metrics và bộ ghi baseline (24/10/2026): PASS** trên fixture riêng: endpoint `/v1/admin/metrics` bảo vệ bằng JWT platform-admin, xuất `run.json`, `metrics.csv`, `resources.csv`; 2 Gateway × 20 batch (40 message / 4.000 samples) đối chiếu khớp DB; đo payload thực 1.066,2 B/msg; 1 replay byte-exact không tăng dữ liệu; cặp kiểm chứng `--metrics-off` PASS. Chạy từ repo root bằng `sh scripts/test-paper-p1.sh --mode baseline <NEW_DIR>`. Xem [báo cáo metrics và baseline P1](docs/backend/paper-p1-metrics-and-baseline.md). Không đổi Stage 5 (vẫn tắt), không tự mở P2.
- **P2 — RS256/JWKS và HTTP perimeter (24/10/2026): PASS** trên fixture riêng: Auth cấp RS256 user token kèm legacy verify-only key, Go public JWKS verifier, Nginx client-to-gateway HTTPS kết hợp xác thực ngoại vi `auth_request` qua Unix socket nội bộ trong cùng tiến trình Go; Storage xác thực bằng public JWKS tĩnh và giữ URL signing HMAC; kiểm chứng rotation K1/K2 overlap, retry Storage bounded phân loại 503, fail-closed khi upstream gián đoạn; 2 Gateway × 20 batch (40 message / 4.000 samples) + 1 replay byte-exact, DB đối chiếu khớp, đọc temporal 199 điểm qua HTTPS; dọn dẹp chính xác 19 containers/1 network/7 volumes. Chạy từ repo root bằng `sh scripts/test-paper-p2.sh [NEW_RUN_DIRECTORY]`. Xem [báo cáo RS256 và HTTP perimeter P2](docs/backend/paper-p2-auth-and-http-perimeter.md). Giữ mặc định deployment là HS256/HTTP; không tự mở P3; Stage 5 tiếp tục giữ `STAGE5_ENABLED=false` (dừng sau 5.4); không xác nhận production hoặc zero-downtime.

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
      - **Dynamic Security hiện hành**: Sử dụng plugin Mosquitto Dynamic Security (`mosquitto_dynamic_security.so`), Go DynSec adapter và broker trực tiếp trong Compose. API/readiness fail-closed không đồng nghĩa global MQTT gate đóng khi backend lỗi/shutdown. Rotate/revoke và từ chối credential cũ/đã revoke được kiểm chứng trên native fixture; mặc định API tắt (`MQTT_CREDENTIAL_API_ENABLED=false`). Cơ chế gate `CLOSED`/verified `OPEN`/global maintenance còn trong component legacy, không phải lifecycle đang wire vào deployment. Hợp đồng hiện hành tại [current credential acceptance](docs/backend/stage-2-current-credential-acceptance.md).
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
  - MQTT ingress (tắt mặc định): `MQTT_INGRESS_ENABLED` (false), độc lập với credential API và legacy runtime; khi bật yêu cầu `MQTT_BROKER_URL` (`ssl://`, không userinfo/query/fragment), `MQTT_CLIENT_ID`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_TLS_CA_FILE`.
  - Giới hạn MQTT: `MQTT_QUEUE_CAPACITY` (256), `MQTT_WORKER_COUNT` (4), `MQTT_MAX_PAYLOAD_BYTES` (mặc định 64KB, tối đa 1MB).
  - **Quản trị người dùng & Thành viên Gateway (Admin User Management)**:
    - `AUTH_ADMIN_BASE_URL`: URL gọi Supabase Auth Admin qua Envoy Gateway (mặc định: `http://supabase-envoy:8000`, ưu tiên cao hơn `SUPABASE_URL`).
    - `SUPABASE_SERVICE_ROLE_KEY`: Khóa dịch vụ đặc quyền máy chủ nội bộ gọi Auth Admin API (ưu tiên cao hơn `SERVICE_ROLE_KEY`; tuyệt đối không cấp cho client). Nếu để trống, server vẫn khởi động bình thường, 4 endpoint CSDL vẫn hoạt động, chỉ `POST /v1/admin/users` trả HTTP 503 `service_unavailable`.
    - `AUTH_ADMIN_TIMEOUT`: Ngân sách timeout bao trùm toàn bộ nghiệp vụ user-management service operation (mặc định: `10s`, ưu tiên cao hơn `USER_MANAGEMENT_TIMEOUT`, dải: 100ms..60s).
    - `AUTH_ADMIN_MAX_BODY_BYTES`: Giới hạn đọc phản hồi từ dịch vụ Auth thượng nguồn (upstream Auth response read bound trong `SupabaseAuthAdapterConfig`, mặc định: `16384` bytes, dải: 1..1048576). Request body từ client gửi lên backend được giới hạn bởi `ADMIN_MAX_BODY_BYTES` (`cfg.AdminMaxBodyBytes`).
    - `USER_LIST_MAX_USERS`: Giới hạn số lượng tài khoản tối đa trong danh sách (mặc định: `1000`, dải: 1..10000).
    - `USER_LIST_MAX_MEMBERSHIPS`: Giới hạn số lượng membership tối đa trong danh sách (mặc định: `10000`, dải: 1..100000, bắt buộc $\ge$ `USER_LIST_MAX_USERS`).
    - `USER_LIST_MAX_RESPONSE_BYTES`: Giới hạn kích thước phản hồi JSON danh sách (mặc định: `2097152` bytes = 2 MiB, dải: 1024..10485760).

```bash
docker compose up -d backend
docker compose exec backend wget -qO- http://127.0.0.1:8080/healthz
docker compose exec backend wget -qO- http://127.0.0.1:8080/readyz
```

- `/healthz`: Liveness probe chỉ xác nhận tiến trình HTTP còn hoạt động (trả về `OK`).
- `/readyz`: Readiness probe xác nhận backend hiện truy cập được CSDL PostgreSQL (`{"status":"ready"}` HTTP 200 hoặc safe error envelope `service_unavailable` HTTP 503).
- Toàn bộ HTTP request/response được gán hoặc chuyển tiếp header `X-Request-ID` (tự động tạo UUIDv4 nếu chưa có) phục vụ distributed tracing.
- **Cấu hình CORS cho Go Business REST API (`CORS_ALLOWED_ORIGINS`)**:
  - `CORS_ALLOWED_ORIGINS` nhận danh sách exact web origin phân tách bằng dấu phẩy. Nếu để trống hoặc không cấu hình, backend tự động vô hiệu hóa quyền cross-origin của trình duyệt nhưng vẫn cho phép các request không có header `Origin` (như `/healthz`, `/readyz` và gọi trực tiếp giữa các service).
  - **Development (Flutter Web)**: Cấu hình cổng cố định cho Flutter Web, ví dụ `http://localhost:12345` trong `.env` (`flutter run -d chrome --web-port=12345`).
  - **Production**: Cấu hình tường minh HTTPS origin của frontend (ví dụ `https://dashboard.example.com`). Origin phải có scheme `http` hoặc `https`, hostname, port số hợp lệ nếu có; cấm path, trailing slash `/`, userinfo, query (kể cả `?` rỗng), fragment (kể cả `#` rỗng) hoặc wildcard `*`; port được khai báo tường minh không được rỗng. Host chỉ chấp nhận `localhost`, IPv4/IPv6 literal hợp lệ, hoặc DNS labels ASCII thông thường. Unicode domain bị từ chối vì backend không thực hiện IDNA; dùng hostname ASCII punycode đã được chuyển đổi (ví dụ `xn--...`) nếu cần. Trailing slash bị từ chối lúc khởi động để bảo đảm khớp origin chính xác.
  - **Tái tạo container khi đổi biến môi trường**: Khi chỉnh sửa biến môi trường trong `.env`, cần chạy `docker compose up -d --force-recreate backend` (hoặc `docker compose up -d`) để Docker Compose tái tạo container backend với biến môi trường mới thay vì `docker compose restart` (lệnh restart không đọc lại `.env`).
  - **Phạm vi độc lập (Scope)**: Middleware này chỉ áp dụng cho Go business REST API (`/v1/*`, `/healthz`, `/readyz`). Không bao gồm hay thay thế CORS của Supabase Auth/Storage trên Envoy API Gateway, và không thay thế Origin check khi upgrade kết nối WebSocket (`/v1/ws`).
- `docker compose stop backend` gửi tín hiệu `SIGTERM` để backend dừng tiếp nhận kết nối mới, hoàn tất request đang xử lý trong thời gian `SHUTDOWN_TIMEOUT` và đóng connection pool PostgreSQL an toàn.

### 3.3 Áp dụng migration

Hệ thống quản lý schema CSDL thông qua chuỗi **22 migrations** (`000001` đến `000022`):
- Các migration `000001` đến `000009` trong `/docker-entrypoint-initdb.d` chỉ tự chạy khi PostgreSQL khởi tạo data volume mới.
- Từ version 10 trở đi (`000010` đến `000022`), service one-shot `application-migrations` sử dụng bảng `schema_migrations` và PostgreSQL advisory lock để tự động áp dụng các migration chưa chạy cho cả volume mới và volume hiện hữu đã baseline:
  - `000010`: Bổ sung bảng `platform_admins`, `gateway_mqtt_credentials`, ràng buộc định dạng định danh Gateway/Sensor, và phân quyền tối thiểu (least-privilege) cho role `iot_backend_app`.
  - `000011`: Bổ sung bảng nhật ký audit bất biến `mqtt_credential_events`, quản lý thế hệ `generation` và khóa `idempotency_key`.
  - `000012`: Bổ sung bảng khóa bảo trì toàn cục `mqtt_credential_maintenance`.
  - `000013`: Bổ sung ràng buộc trạng thái bảo trì và điều kiện tiếp nhận (admission constraints).
  - `000014`: Quản lý các điểm checkpoint khôi phục sự cố (`recovery_checkpoint`) và phương án xử lý (`recovery_disposition`).
  - `000015`: Bổ sung rào chắn bảo vệ quyền thẩm định và ranh giới phục hồi lỗi.
  - `000016`: Trigger và rào chắn bảo vệ tính bất biến tuyệt đối của nhật ký audit (`mqtt_event_authority_guard`).
  - `000020`: Bổ sung schema telemetry ingestion, hypertable raw samples, deduplication ledger và temporal observations cho Stage 3.
  - `000021`: Bổ sung quyền tối thiểu cho quản trị người dùng (`profiles.full_name`, `platform_admins`), tạo hàm bảo mật `list_admin_users_projection` đọc thông tin định danh an toàn từ `auth.users` cho backend.
  - `000022`: Cấp quyền `DELETE` trên bảng `public.user_gateways` cho role nhóm `iot_backend` (`000022_gateway_membership_revocation.up.sql`), phục vụ thu hồi membership trong API admin; không thay đổi cấu trúc bảng, ràng buộc hay dữ liệu người dùng.

Trên volume mới chưa có Auth, `application-migrations` dừng phần áp dụng thực tế ở schema 20; migrations `000021` và `000022` được hoãn. Service `supabase-compat-migration` chạy sau GoTrue, thiết lập compatibility bằng migration `000004`, rồi áp dụng `000021` và `000022` theo thứ tự. Không xem việc runner bỏ qua các migration hậu Auth là đã hoàn tất schema 22.

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

Repository tích hợp kiểm thử qua GitHub Actions (`.github/workflows/ci.yml`), dùng Go **1.27.3**, hiện bao gồm **10 jobs CI độc lập**:

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

*Lưu ý về 4 endpoint Admin MQTT Credential*: Các route này đã được lập trình hoàn chỉnh trong code và kiểm thử trong harness test, nhưng trên cấu hình deployment thực tế mặc định bị vô hiệu hóa qua biến `MQTT_CREDENTIAL_API_ENABLED=false` (trả về lỗi an toàn HTTP `503 credential_runtime_disabled`). Chi tiết quy trình triển khai, hợp đồng API và giới hạn kỹ thuật xem tại [Tài liệu Kiến trúc DynSec](docs/dynsec/INDEX.md).

#### 2. Nhóm Endpoint Digital Twin Stage 4 (Chuẩn hóa theo ETSI GS CIM 009 V1.9.1 Profile):
Các route này đã được triển khai hoàn chỉnh và kiểm thử tích hợp trên Go backend, hoạt động theo chuẩn ETSI GS CIM 009 V1.9.1 profile:

| Phương thức | Đường dẫn | Xác thực & Quyền hạn | Mô tả chi tiết | Trạng thái hiện tại |
|---|---|---|---|---|
| `GET` | `/v1/digital-twins` | Bearer JWT `authenticated` | Lấy danh sách thực thể Digital Twin mà người dùng có quyền (root array `[...]`, lọc theo `type=Gateway\|Sensor`, phân trang qua header `Link: <URI>; rel="next"`) | Đã triển khai: HTTP 200 OK (`application/ld+json`) |
| `POST` | `/v1/digital-twins` | Bearer JWT (Gateway: Admin; Sensor: Admin/Owner) | Tạo mới hoặc phát lại chính xác thực thể Digital Twin (create-or-exact-replay) | Đã triển khai: HTTP 201 Created kèm header `Location: /v1/digital-twins/<escaped_urn>`; HTTP 200 OK replay (không Location); HTTP 409 Conflict |
| `GET` | `/v1/digital-twins/{entity_id}` | Bearer JWT `authenticated` (thành viên trạm) | Lấy chi tiết thực thể dạng chuẩn JSON-LD normalized kèm context V1.9, mảng quan hệ `hasSensor`, phân trang qua header `Link`; hỗ trợ extension `?format=simplified` đọc trạng thái phẳng `twin_states` | Đã triển khai: HTTP 200 OK (`application/ld+json` cho detail, `application/json` cho state view) |
| `PATCH` | `/v1/digital-twins/{entity_id}/attrs` | Bearer JWT (Gateway: Admin; Sensor: Admin/Owner) | Cập nhật siêu dữ liệu hiển thị (`name`, `description`) qua normalized Property fragment; Gateway xóa mô tả bằng `urn:ngsi-ld:null`; Sensor cấm sửa `unit` | Đã triển khai: HTTP 200 OK (no-op giữ nguyên timestamp). Đường dẫn gốc không có `/attrs` nhận HTTP 405 Method Not Allowed |
| `GET` | `/v1/digital-twins/{entity_id}/temporal` | Bearer JWT `authenticated` (thành viên trạm) | Hợp nhất truy vấn lịch sử viễn trắc (`attrs=measurement`) và chu kỳ đo (`attrs=sample_interval_us`) cho Sensor; hỗ trợ raw tuples và bucket triples với 9 canonical durations; `totalCount` là số nguyên exact integer | Đã triển khai: HTTP 200 OK (`application/ld+json`) |

*Ghi chú về 4 route cũ đã thu hồi:* Sau chuyển đổi V1.9.1, 4 route cũ gồm `GET /v1/telemetry/history` (404), `GET /v1/digital-twins/{entity_id}/state` (404), `GET /v1/digital-twins/{entity_id}/history` (404), và `PATCH /v1/digital-twins/{entity_id}` (405) đã bị thu hồi hoàn toàn.

#### 3. Nhóm Endpoint Quản trị Tài khoản Người dùng & Quyền Thành viên Gateway (Admin User & Gateway Membership APIs):
Các route này đã được triển khai hoàn chỉnh theo Kế hoạch chi tiết, Migration 21 (`000021_admin_user_management.up.sql`) và Migration 22 (`000022_gateway_membership_revocation.up.sql`), dành riêng cho Platform Admin:

| Phương thức | Đường dẫn | Xác thực & Quyền hạn | Mô tả chi tiết | Trạng thái hiện tại |
|---|---|---|---|---|
| `POST` | `/v1/admin/users` | Bearer JWT + `platform_admins` | Tạo tài khoản người dùng mới (auto-confirm qua Supabase Auth Admin API), tự động tạo profile | Đã triển khai: HTTP 201 Created kèm `Location: /v1/admin/users/{id}`; HTTP 409 nếu trùng email |
| `GET` | `/v1/admin/users` | Bearer JWT + `platform_admins` | Lấy danh sách toàn bộ người dùng trong bound kèm `platform_role` và `gateway_roles: [{gateway_id, role}]`; không query/search | Đã triển khai: HTTP 200 OK (`items: [...]`); HTTP 409 nếu vượt giới hạn cấu hình (capacity exceeded) |
| `PATCH` | `/v1/admin/users/{user_id}` | Bearer JWT + `platform_admins` | Cập nhật nguyên tử tên hiển thị `full_name` (profiles) và/hoặc vai trò `platform_role` (platform_admins) | Đã triển khai: HTTP 200 OK; HTTP 409 nếu tự hạ quyền admin (`self_admin_demotion_forbidden`) |
| `PUT` | `/v1/admin/gateways/{gateway_id}/users/{user_id}` | Bearer JWT + `platform_admins` | Gán vai trò `operator` hoặc `viewer` cho người dùng trên Gateway trong `user_gateways` | Đã triển khai: HTTP 200 OK; HTTP 400 nếu gán `owner`; HTTP 409 nếu hạ owner hiện tại |
| `PUT` | `/v1/admin/gateways/{gateway_id}/owner` | Bearer JWT + `platform_admins` | Chuyển giao quyền sở hữu trạm Gateway nguyên tử; owner cũ chuyển thành `viewer` (duy trì quyền đọc) | Đã triển khai: HTTP 200 OK; HTTP 409 nếu trạng thái trạm không có hoặc thừa owner |
| `DELETE` | `/v1/admin/gateways/{gateway_id}/users/{user_id}` | Bearer JWT + `platform_admins` | Thu hồi quyền thành viên (`operator`/`viewer`) trên Gateway; không có body/query. Thao tác có tính lũy đẳng (idempotent), không xóa tài khoản/profile/global role và không ảnh hưởng Gateway khác | Đã triển khai: HTTP 204 No Content (body rỗng) khi xóa thành công hoặc khi membership không tồn tại; HTTP 409 Conflict (`owner_transfer_required`) nếu target là owner (yêu cầu chuyển owner trước); HTTP 404 Not Found nếu Gateway không tồn tại; HTTP 400 nếu có body/query hoặc ID sai |

#### 4. Nhóm Endpoint Stub có xác thực (Authenticated Stubs):
Hiện tại chỉ còn duy nhất 1 route stub cho WebSocket truyền dữ liệu thời gian thực (thuộc Giai đoạn 7):

| Phương thức | Đường dẫn | Xác thực & Quyền hạn | Mô tả chi tiết | Trạng thái hiện tại |
|---|---|---|---|---|
| `GET` | `/v1/ws` | Bearer JWT `authenticated` (header) | Nâng cấp kết nối WebSocket để streaming dữ liệu cảm biến thời gian thực | HTTP 401 nếu thiếu/sai JWT; HTTP 501 Not Implemented nếu JWT hợp lệ |

#### 5. Endpoint điều khiển Sensor — đã triển khai đến mốc 5.4, mặc định tắt:

Các route đã đăng ký nhưng chỉ phục vụ nhận/tra cứu intent; authenticated request
khi Stage 5 tắt trả 503, thiếu/sai JWT trả 401. Chưa có Gateway áp dụng cấu hình hay
backend xác nhận kết quả; không bật cho client/shared/live tại mốc tạm dừng này.

| Phương thức | Đường dẫn | Quyền thành viên Gateway | Trạng thái mốc 5.4 |
|---|---|---|---|
| `PATCH` | `/v1/digital-twins/{entity_id}/desired-state` | Owner/operator; Sensor đã provision | Nhận `frequency_hz` decimal + `expected_desired_version` string, bắt buộc Idempotency-Key; 202 nhận intent, 200 confirmed no-op; replay giữ status/body gốc |
| `GET` | `/v1/digital-twins/{entity_id}/commands` | Owner/operator/viewer | Danh sách scope Sensor, keyset/Link, limit mặc định 20/tối đa 100 |
| `GET` | `/v1/digital-twins/{entity_id}/commands/{command_id}` | Owner/operator/viewer | Chi tiết command đúng Sensor/Gateway; không trả outbox/raw diagnostics |

Không có `POST .../commands`, `/v1/commands/{command_id}`, reboot hay capture-image.
Chỉ hỗ trợ intent `update_configuration`/`sample_interval_us`. Global admin không
tự bypass membership. Chi tiết tại [bàn giao 5.4](docs/backend/stage-5-task-5.4-publisher-acl-and-expiry.md).

**Endpoint media dự kiến**, chưa được đăng ký trong router:

| Phương thức | Đường dẫn | Xác thực dự kiến | Mô tả theo thiết kế MVP | Giai đoạn |
|---|---|---|---|---|
| `POST` | `/v1/media/upload-url` | Gateway HTTP Credential riêng | Cấp Signed Upload URL cho Gateway tải ảnh chụp lên private Storage | Stage 8 |

*Lưu ý quan trọng về các tuyến đường không tồn tại*:
- Router Go backend hiện **KHÔNG CÓ** endpoint xem chi tiết Gateway đơn lẻ (`GET /v1/gateways/{gateway_id}`) hay Sensor đơn lẻ (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}`). Việc đọc danh sách Sensor được thực hiện thông qua route cha `GET /v1/gateways/{gateway_id}/sensors` hoặc endpoint Digital Twin detail `GET /v1/digital-twins/{entity_id}`.
- Chức năng quản trị tài khoản và phân quyền thành viên Gateway hiện gồm 6 API quản trị cho Platform Admin (nhóm 3 ở trên). Tuy nhiên, **phạm vi được giới hạn có chủ đích (bounded scope)**: không có API Delete/ban tài khoản, đổi email, đổi/reset mật khẩu, hay gửi email lời mời. Việc xóa/thu hồi quyền thành viên qua `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` chỉ gỡ liên kết trong `user_gateways`, tuyệt đối không xóa tài khoản Auth, hồ sơ `profiles` hay quyền platform admin và không ảnh hưởng tới Gateway khác.
- **Mã nguồn ứng dụng Flutter Web client và bộ sưu tập Bruno API collections chưa được cập nhật theo hợp đồng mới** trong đợt bàn giao tài liệu này.

#### 6. Danh mục tính năng tạm hoãn ngoài phạm vi MVP (Deferred Features per AGENTS.md §2):
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
   - `gateways/<gateway_id>/commands/#`: backend đã có publisher `update_configuration` cho Sensor; Gateway command handler chưa triển khai (5.5). Không có reboot/capture_image.
   - `gateways/<gateway_id>/responses/#`: namespace kết quả theo thiết kế; backend mới subscribe/raw queue, chưa xử lý/commit result (5.6).

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

#### Gói tin command hiện hành (publisher đã có ở 5.4, Gateway execution chưa có):
```json
{
  "protocol_version": 1,
  "message_type": "command",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "action": "update_configuration",
  "parameters": {
    "sample_interval_us": 10000000
  },
  "desired_version": "12",
  "issued_at": "2026-09-16T08:31:00Z",
  "expires_at": "2026-09-16T08:32:00Z"
}
```

#### Gói tin result theo contract (minh họa tương lai 5.5/5.6, chưa chạy runtime):
```json
{
  "protocol_version": 1,
  "message_type": "command_result",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "desired_version": "12",
  "status": "succeeded",
  "reported_state": {
    "sample_interval_us": 10000000
  },
  "completed_at": "2026-09-16T08:31:03Z"
}
```

---

## 5. Phân hệ Digital Twin & Quản lý điều khiển (Digital Twin Subsystem)

Thay vì tích hợp Federated Learning (đã được loại bỏ để tập trung vào mục tiêu trọng tâm của đồ án), hệ thống thiết kế phân hệ **Digital Twin** theo kiến trúc Modular Monolith bên trong Go Backend. Tại mốc 5.4 đã có schema/admission/REST/publisher/expiry; vòng điều khiển vẫn chưa hoàn tất vì thiếu Gateway execution và result confirmation. Stage 5 đang tạm dừng.

### 5.1 Chuẩn định danh & Thực thể NGSI-LD
- Mỗi thực thể vật lý (Gateway, Sensor, Device/Actuator) có định danh URN bền vững:
  - `urn:ngsi-ld:Gateway:<gateway_id>`
  - `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`
  - `urn:ngsi-ld:Device:<device_id>`
- Lưu trữ quan hệ thực thể trong `twin_relationships` (`hasSensor`, `connectedTo`, `controls`, `managedBy`).
- Cung cấp biểu diễn JSON-LD tại API boundary với `@context: ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]`.

### 5.2 Cơ chế phân tách Trạng thái (Reported vs Desired State)
- **`reported_state`**: Đại diện cho trạng thái thực tế cuối cùng do Gateway xác nhận qua MQTT (telemetry, status, command result). Client tuyệt đối không được ghi trực tiếp vào `reported_state`.
- **Hiện trạng 5.4:** chỉ telemetry đang ghi observation thực tế. PATCH desired không ghi reported; nhánh `reported_state.configuration` dành cho result xác nhận còn chờ 5.6, không coi dữ liệu seed trong test là config đã apply.
- **`desired_state`**: Đại diện cho cấu hình đích do người dùng mong muốn và thiết lập qua REST API. Gateway không được ghi vào `desired_state`.
- Quản lý phiên bản độc lập (`reported_version` và `desired_version`), áp dụng cơ chế **Optimistic Concurrency** để ngăn xung đột ghi đồng thời từ nhiều client.

### 5.3 Quy trình Điều khiển Downlink & Transactional Outbox
**Bước 1–4 đã có tại mốc 5.4; bước 5–7 hoãn cùng 5.5/5.6, không phải luồng đã chạy E2E.**

1. Người dùng gửi `PATCH /v1/digital-twins/{entity_id}/desired-state`; không có API direct `POST /commands`.
2. Go Backend kiểm tra quyền của người dùng trên Gateway liên kết (`user_gateways`).
3. Mở **Database Transaction**:
   - Cập nhật `desired_state` và tăng `desired_version`.
   - Ghi bản ghi chỉ lệnh vào `twin_commands` với trạng thái `pending`.
    - Ghi bản ghi thông điệp vào `twin_outbox`.
    - Ghi request receipt để replay đúng status/body, kể cả no-op đã được xác nhận; no-op không tạo command/outbox hoặc tăng version.
   - Commit transaction (đảm bảo không xảy ra tình trạng đã ghi DB nhưng mất tin MQTT hoặc ngược lại).
4. **Outbox Worker** lấy stored bytes từ `twin_outbox`, publish qua Mosquitto topic `gateways/<gateway_id>/commands/<command_id>`; chỉ sau PUBACK mới conditional mark `published`, không coi đó là thực thi thành công. Expiry kết thúc command quá hạn và ngừng outbox, không rollback desired.
5. Gateway nhận lệnh, kiểm tra idempotency (dựa trên `command_id`), thực thi cấu hình phần cứng.
6. Gateway publish kết quả về topic `gateways/<gateway_id>/responses/<command_id>`.
7. Theo kế hoạch 5.6, backend correlate result rồi cập nhật command và merge `reported_state.configuration` trong cùng transaction; không ghi đè telemetry/desired. Hiện chưa triển khai.

Command status dùng REST polling; không có WebSocket command events trong Stage 5.

### 5.4 Lưu trữ Lịch sử Biến thiên Thời gian (TimescaleDB)
- Bảng hypertable `twin_temporal_values` ghi lại mọi biến thiên thuộc tính theo thời gian:
  - Dữ liệu cảm biến và các chỉ số suy luận AI (`reconstruction_loss`, `anomaly_score`, ...).
  - Lịch sử thay đổi trạng thái hoạt động của thiết bị.
  - Phục vụ biểu đồ xu hướng lịch sử và đối soát vận hành mà không cần quét toàn bộ bảng trạng thái hiện hành.
