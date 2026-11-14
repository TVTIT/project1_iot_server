# Hướng Dẫn Triển Khai Và Vận Hành Hệ Thống DynSec

Tài liệu này hướng dẫn chi tiết quy trình chuẩn bị, triển khai từ gốc (Root Deployment) và quy trình vận hành an toàn (Safe Operator Runbook) cho mô-đun Eclipse Mosquitto Dynamic Security (DynSec) phối hợp cùng Go backend và PostgreSQL.

Mọi nội dung đều được đối chiếu trực tiếp từ cấu hình và mã nguồn gốc:
- `docker-compose.yml`
- `config/mosquitto/mosquitto.conf`
- `config/nginx/nginx.conf.template`
- `scripts/mqtt-bootstrap.py`
- `docs/runbooks/mqtt-dynsec-bootstrap.md`
- `src/internal/config/credential.go`
- `src/cmd/server/credential.go`

---

## 1. Chuẩn Bị Triển Khai Dựa Trên Cấu Hình Gốc (Root Deployment Preparation)

### 1.1 Phiên bản dịch vụ và kiểm định nền tảng (Pinned Versions & Platform Qualification)

Hệ thống bắt buộc sử dụng các phiên bản container và định danh đã được kiểm định:

| Dịch Vụ | Tham Chiếu Phiên Bản / Image Tag Đã Ghim | Mục Đích Sử Dụng |
|---|---|---|
| **Mosquitto Broker** | `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9` (Phiên bản Mosquitto `2.0.18`) | Broker MQTT và plugin Dynamic Security (`/usr/lib/mosquitto_dynamic_security.so`). |
| **Go Backend** | Xây dựng từ `src/Dockerfile` (Base builder: `golang:1.27.3-alpine`, Runtime: Mosquitto base) | Dịch vụ backend đơn khối (Modular Monolith) chạy dưới danh tính `1883:1883`. |
| **Nginx Ingress** | `nginx:1.25-alpine` | Reverse proxy biên, định tuyến HTTP và WebSocket. |
| **Supabase Envoy** | `envoyproxy/envoy:v1.39.1` | API Gateway nội bộ của Supabase, định tuyến `/auth/v1/*` và `/storage/v1/*`. |
| **GoTrue Engine** | `supabase/gotrue:v2.196.0` | Động cơ xác thực người dùng Supabase Auth. |
| **Nền tảng thực thi** | `linux/amd64` (`MQTT_DYNSEC_PLATFORM=linux/amd64`) | Kiến trúc CPU/OS đã được thử nghiệm và phê duyệt. |

> **Ràng buộc kiểm định phiên bản phát hành (`NativeDisableQualification`):**
> Go backend kiểm tra tính toàn vẹn của chuỗi biến môi trường:
> - `MQTT_DYNSEC_IMAGE_REFERENCE`: Bắt buộc khớp chính xác với chuỗi digest đã ghim của `eclipse-mosquitto`.
> - `MQTT_DYNSEC_IMAGE_CONFIG_ID`: Bắt buộc khớp với mã nhận diện cấu hình kiểm định (`sha256:d12c8f...`).
> - `MQTT_DYNSEC_PLATFORM`: Bắt buộc là `linux/amd64`.
> Nếu bất kỳ giá trị nào bị sai lệch hoặc để trống, backend sẽ từ chối khởi động (`ErrInvalidInput`).

---

### 1.2 Yêu Cầu Tiên Quyết Cho Bootstrap Offline Trên Host (Host Prerequisite)

Tiến trình khởi tạo ban đầu tệp lưu trữ DynSec (`scripts/mqtt-bootstrap.py`) được thực thi **trực tiếp trên Linux host**, không chạy trong container Mosquitto:

1. **Lý do không chạy trong container Mosquitto:** Image `eclipse-mosquitto:2.0.18` là một image tối giản, không bao gồm môi trường Python 3.
2. **Yêu cầu nhị phân trên Host (Host Dependency):**
   - Host phải được cài đặt sẵn công cụ `mosquitto_ctrl` và thư viện liên kết `libmosquitto` phiên bản **chính xác 2.0.18**.
   - Tệp thực thi `mosquitto_ctrl` và toàn bộ các thư mục cha (ancestor directories) bắt buộc phải:
     * Thuộc sở hữu của `root` (`UID == 0`).
     * Không phải là symlink (đường dẫn thực tế, không chứa liên kết mềm).
     * Không có quyền ghi cho nhóm hoặc mọi người (`mode & 0o022 == 0`).
   - Lệnh kiểm tra dependency an toàn trên host (không đọc stdin, không chạm volume):
     ```bash
     python3 scripts/mqtt-bootstrap.py --check-native-tool --native-tool /usr/bin/mosquitto_ctrl
     ```
     Nếu công cụ thiếu hoặc sai phiên bản, lệnh sẽ thoát với mã lỗi khác 0 và in thông báo lỗi. **Platform Operator phải dừng lại trước khi tạo bất kỳ secret nào.**

---

### 1.3 Khởi Tạo Tệp Lưu Trữ Native Store Mới (`dynamic-security.json`)

1. **Nguyên tắc tệp lưu trữ hoàn toàn mới (Fresh Native Store):**
   - Chỉ khởi tạo trên một volume/thư mục hoàn toàn mới, chưa từng gắn với broker đang chạy.
   - **Tuyệt đối không import** tệp password tĩnh cũ (`passwd`), tệp ACL tĩnh (`acl`), hoặc trạng thái legacy cũ vào store này.
2. **Quyền hạn thư mục và tệp:**
   - Thư mục đích phải có quyền `0700`, thuộc quyền sở hữu của UID/GID tương ứng với tiến trình ghi broker (thông thường `1883:1883` trên Linux host trong môi trường rootful Docker).
   - Tệp sinh ra mang tên `dynamic-security.json`, quyền `0600`, chỉ chứa native password hashes (PBKDF2) của tài khoản `Manager` và tài khoản `Backend`.
   - Lệnh thực thi bootstrap đọc secret qua stdin từ một tệp cấu hình bảo vệ tạm thời (xem quy trình chi tiết tại `docs/runbooks/mqtt-dynsec-bootstrap.md`), không in secret ra terminal hay shell history.

---

### 1.4 Gắn Kết Volume Thư Mục Chỉ Đọc Và Ánh Xạ UID Writer

Cấu hình gắn kết volume trong `docker-compose.yml` tuân thủ các nguyên tắc bảo mật quan trọng:

```yaml
# 1. Dịch vụ Mosquitto Broker (Tiến trình ghi - Writer)
mosquitto:
  image: eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9
  user: "1883:1883"
  volumes:
    - ./config/mosquitto/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
    - ./config/mosquitto/certs:/mosquitto/config/certs:ro
    - mosquitto_dynsec:/mosquitto/dynsec          # Volume ghi cho plugin DynSec
    - mosquitto_data:/mosquitto/data

# 2. Dịch vụ Go Backend (Tiến trình đọc - Reader)
backend:
  user: "1883:1883"
  volumes:
    # Bắt buộc mount CẢ THƯ MỤC CHỨA ở chế độ CHỈ ĐỌC (:ro)
    - mosquitto_dynsec:/mosquitto/dynsec:ro
    # Chỉ mount tệp CA công khai; tuyệt đối không mount khóa riêng broker
    - type: bind
      source: ${MQTT_TLS_CA_SOURCE:-./config/mosquitto/certs/ca.crt}
      target: ${MQTT_TLS_CA_FILE:-/mosquitto/config/certs/ca.crt}
      read_only: true
```

**Các lưu ý kỹ thuật cốt lõi:**
- **Tại sao phải mount cả thư mục thay vì một tệp đơn lẻ?**
  Plugin Mosquitto DynSec cập nhật tệp `dynamic-security.json` bằng kỹ thuật ghi ra tệp tạm thời rồi thực hiện syscall đổi tên nguyên tử (`rename`). Nếu Go backend mount một tệp đơn lẻ (`dynamic-security.json:ro`), Linux mount namespace sẽ tiếp tục trỏ vào inode cũ đã bị unlink sau khi rename, khiến backend không thấy được nội dung cập nhật mới. Do đó, Go backend bắt buộc phải mount **cả thư mục chứa** (`/mosquitto/dynsec:ro`) để theo dõi được inode mới.
- **Xác thực UID tiến trình ghi (`MQTT_DYNSEC_WRITER_UID`):**
  Bộ đọc snapshot `DynSecSnapshotReader` kiểm tra quyền sở hữu của tệp bằng syscall `openat/O_NOFOLLOW` và so sánh `stat.st_uid` với biến môi trường `MQTT_DYNSEC_WRITER_UID`.
  * Cấu hình bắt buộc: `MQTT_DYNSEC_WRITER_UID` phải là chuỗi số nguyên không dấu 32-bit (base10 uint32, ví dụ `1883`).
  * Trong môi trường **Rootless Docker / Podman**: UID của container được ánh xạ sang một dải UID con trên host thông qua `/etc/subuid`. Platform Operator phải kiểm tra bảng ánh xạ thực tế (`podman unshare` hoặc `id -u`) để điền giá trị UID thực tế của host, **không đoán mò từ UID đăng nhập**.
  * Nghiêm cấm chạy lệnh `chmod 777` hoặc nới rộng quyền thư mục store để vượt qua lỗi kiểm tra UID.

---

### 1.5 Cấu Hình Mạng, Chứng Chỉ TLS Và Cơ Sở Dữ Liệu

1. **Ranh giới TLS CA-Only cho Backend:**
   - Go backend chỉ cần tệp chứng chỉ CA tin cậy (`ca.crt`) để thẩm định chữ ký số và hostname của Mosquitto broker qua kênh TLS nội bộ.
   - **Tuyệt đối không mount** khóa riêng tư của broker (`server.key`), khóa CA riêng (`ca.key`), hoặc unix domain socket vào container backend.
2. **Khớp nối Hostname và TLS SAN:**
   - Địa chỉ URL quản trị nội bộ `MQTT_INTERNAL_MANAGEMENT_URL` bắt buộc có định dạng `ssl://<hostname>:<port>` (ví dụ: `ssl://mosquitto:8883`).
   - Chứng chỉ số của Mosquitto (`server.crt`) bắt buộc phải có trường Subject Alternative Name (SAN) chứa chính xác hostname nội bộ này (ví dụ `DNS:mosquitto`).
3. **Cơ sở dữ liệu PostgreSQL & TimescaleDB:**
   - Áp dụng đầy đủ các migration schema từ `000001` tới migration `000019_mqtt_simplified_journal.up.sql`.
   - Vai trò CSDL của backend: Sử dụng role có đặc quyền tối thiểu `iot_backend` (hoặc `iot_backend_app`).
   - **Lưu ý về Role `postgres` trong GoTrue:** Migration nội bộ `20240612123726` của Supabase GoTrue yêu cầu sự tồn tại của role `postgres`. Trong cấu hình gốc `docker-compose.yml`, container PostgreSQL đã khởi tạo mặc định biến `POSTGRES_USER: postgres` nên role này đã tồn tại sẵn. Đối với các môi trường tùy biến sử dụng username CSDL khác, cần tạo trước role `CREATE ROLE postgres NOLOGIN;` để GoTrue hoàn thành migration.
4. **Phân tách thông tin tài khoản Manager và Backend:**
   - `MQTT_DYNSEC_MANAGER_USERNAME` (ví dụ `credential_manager`) và mật khẩu `MQTT_DYNSEC_MANAGER_PASSWORD` (tối thiểu 128 bit entropy).
   - `MQTT_USERNAME` (mặc định `backend_service`) và mật khẩu `MQTT_PASSWORD` (tối thiểu 128 bit entropy).
   - Backend kiểm tra lúc nạp cấu hình (`config/credential.go`): Hai username và hai mật khẩu này bắt buộc phải **hoàn toàn khác nhau**.
5. **Ma trận Topic ACL và Quy tắc Đăng ký Nguyên văn (`subscribeLiteral`):**
   - Plugin DynSec của Mosquitto hoạt động theo nguyên tắc **Default Deny** (`defaultACLAccess = false`).
   - `Manager` được cấp quyền publish/subscribe trên topic nội bộ `$CONTROL/dynamic-security/v1` và `$CONTROL/dynamic-security/v1/response`.
   - `Backend` (`bootstrap_backend`) **chỉ được cấp quyền thu thập telemetry**:
     * Nhận telemetry: `publishClientReceive` và `subscribeLiteral` trên `gateways/+/telemetry/#`.
     * Gửi ACK: `publishClientSend` trên `gateways/+/acks/#`.
     * **Cảnh báo:** Backend hiện **chưa có quyền** `publishClientSend` trên `gateways/+/commands/#` và chưa có quyền nhận `gateways/+/status` hay `gateways/+/responses/#`.
   - **Quy tắc `subscribeLiteral`:** Mosquitto DynSec yêu cầu bộ lọc subscription khi client gọi lệnh `SUBSCRIBE` phải **trùng khớp chính xác từng ký tự** với chuỗi topic được cấp quyền trong store. Do đó, backend bắt buộc phải subscribe chuỗi `gateways/+/telemetry/#`; nếu subscribe chuỗi con cụ thể như `gateways/gw_001/telemetry/abc`, broker sẽ từ chối với lỗi `"All subscription requests were denied."`.

---

### 1.6 Luồng Định Tuyến Nginx Và Supabase Envoy Gateway

1. **Định tuyến qua Nginx:**
   - Toàn bộ lưu lượng HTTP bên ngoài đi qua Nginx reverse proxy.
   - Tuyến `/auth/v1/*` được chuyển tiếp tới Supabase Envoy Gateway (`http://kong:8000` / `http://supabase-envoy:8000`).
   - Tuyến `/v1/*` (bao gồm `/v1/admin/*` và WebSocket `/v1/ws`) được chuyển tiếp tới Go backend (`http://backend_api:8080`).
2. **Lưu ý về Nhật ký Nginx (Nginx Access Log Caveat):**
   - Cấu hình mặc định của Nginx ghi lại biến `$request`, bao gồm cả URI đầy đủ và các query parameters.
   - **Cảnh báo vận hành:** Không tuyên bố Nginx tự động che giấu query strings hay signed URLs trong access logs. Platform Operator phải lưu ý không truyền các thông tin nhạy cảm vào URL query string khi thao tác qua Nginx.
3. **Khóa Đăng Ký Công Khai (Public Signup Disabled):**
   - Đảm bảo tính năng tự đăng ký người dùng công khai trên Supabase Auth được tắt hoàn toàn (`GOTRUE_DISABLE_SIGNUP=true`).
   - Người dùng mới chỉ được cấp phát bởi Platform Operator qua Supabase Studio hoặc Admin API có bảo vệ.

---

### 1.7 Bật Cờ Tính Năng Và Cấu Hình Giới Hạn Thời Gian (Timeouts & Feature Gates)

1. **Trạng thái cờ tính năng an toàn:**
   - Trong `docker-compose.yml`, cờ `MQTT_CREDENTIAL_API_ENABLED` mặc định là `"false"`.
   - Cờ runtime legacy `MQTT_CREDENTIAL_RUNTIME_ENABLED` được đặt cố định là `"false"`.
   - **Chỉ bật `MQTT_CREDENTIAL_API_ENABLED="true"`** sau khi:
     * Tệp store `dynamic-security.json` đã được bootstrap hợp lệ và mount vào backend.
     * CSDL đã chạy đầy đủ migration 000019.
     * Các biến môi trường `MQTT_DYNSEC_MANAGER_PASSWORD`, `MQTT_DYNSEC_WRITER_UID` đã được thiết lập chính xác.
2. **Cấu hình ngân sách thời gian chờ (Timeout Budgets):**
   Bộ nạp cấu hình `src/internal/config/credential.go` thẩm định tính nhất quán của các tham số thời gian chờ:
   - `MQTT_CREDENTIAL_DB_TIMEOUT`: Mặc định 2s (tối đa 1m).
   - `MQTT_CREDENTIAL_FINALIZE_TIMEOUT`: Mặc định 5s (bắt buộc `>= MQTT_CREDENTIAL_DB_TIMEOUT`).
   - `MQTT_DYNSEC_TIMEOUT`: Mặc định 3s (thời gian chờ lệnh broker, bắt buộc `<= MQTT_CREDENTIAL_OPERATION_TIMEOUT`).
   - `MQTT_CREDENTIAL_OPERATION_TIMEOUT`: Mặc định 10s.
   - `MQTT_CREDENTIAL_REQUEST_TIMEOUT`: Mặc định 40s (bắt buộc `> OperationTimeout + FinalizeTimeout`).
   - `MQTT_CREDENTIAL_RECOVERY_TIMEOUT`: Mặc định 5s.
   - `HTTP_WRITE_TIMEOUT`: Bắt buộc lớn hơn tổng ngân sách `AuthorizationTimeout + RequestTimeout + RecoveryTimeout`.

---

## 2. Quy Trình Vận Hành An Toàn Dành Cho Operator (Safe Operator Runbook)

### 2.1 Sao Lưu Dữ Liệu Trước Khi Bảo Trì

Trước khi thực hiện xoay vòng mật khẩu hàng loạt hoặc bảo trì hệ thống, Platform Operator bắt buộc phải thực hiện sao lưu:
1. **Sao lưu Cơ sở dữ liệu PostgreSQL:**
   ```bash
   pg_dump -h <db_host> -U postgres -d iot_platform -F c -b -v -f "backup_iot_platform_$(date +%Y%m%d_%H%M%S).dump"
   ```
2. **Sao lưu Volume Native Store:**
   Tạo bản sao lưu nén của tệp `dynamic-security.json` trong volume `mosquitto_dynsec` mà không thay đổi quyền của tệp gốc.

---

### 2.2 Xử Lý Downtime Khi Xoay Vòng Mật Khẩu (Rotate)

Khi Platform Admin gọi lệnh `POST /v1/admin/gateways/{id}/mqtt-credential/rotate`:
1. **Ngắt kết nối tức thì:** Broker lập tức ngắt phiên làm việc TLS hiện thời của Gateway (socket đóng với lỗi EOF).
2. **Giai đoạn gián đoạn (Downtime Window):** Gateway sẽ liên tục bị từ chối kết nối lại (TLS Auth Denial) do vẫn đang sử dụng mật khẩu cũ.
3. **Nạp mật khẩu mới thủ công:**
   - Platform Operator lấy chuỗi mật khẩu từ phản hồi HTTP duy nhất một lần (`SecretResult`).
   - Nạp mật khẩu mới vào trạm Gateway thông qua kênh tin cậy cục bộ: Kết nối console USB hoặc SSH qua mạng LAN cô lập.
   - Sau khi Gateway lưu cấu hình mới và khởi động lại dịch vụ MQTT client, kết nối sẽ được khôi phục thành công.
   - **Chấp nhận Downtime:** Đây là đặc tính thiết kế có chủ đích cho mốc đồ án nhằm đảm bảo an toàn tuyệt đối, không triển khai cơ chế mật khẩu kép (dual-password) phức tạp.

---

### 2.3 Ứng Phó Khi Mất Mát Phản Hồi Chứa Mật Khẩu (Lost One-Time Secret)

Nếu server đã cập nhật CSDL và broker thành công, nhưng kết nối mạng bị đứt khiến Platform Admin không nhận được phản hồi HTTP chứa mật khẩu:
1. **Không thể Replay để lấy lại mật khẩu:**
   - Gửi lại request với cùng `Idempotency-Key` sẽ chỉ nhận về bản ghi `MutationResult` chứa metadata (`secret_returned: false`), tuyệt đối không có mật khẩu.
2. **Biện pháp xử lý duy nhất:**
   - Kiểm tra trạng thái metadata qua lệnh `GET`. Nếu trạng thái là `active`:
   - Thực hiện một lệnh **Rotate mới** với một **`Idempotency-Key` hoàn toàn mới** (`uuidgen`).
   - Lệnh này sẽ sinh ra một mật khẩu mới khác và trả về trong phản hồi mới.

---

### 2.4 Xử Lý Các Thao Tác Bất Định Hoặc Cần Phục Hồi (`recovery_needed`)

Nếu một thao tác gặp sự cố giữa chừng và trả về lỗi `503 Service Unavailable` (`code: "credential_recovery_required"`):
1. **Hiện tượng:** Bản ghi trong bảng `gateway_mqtt_credentials` chuyển sang trạng thái `recovery_needed`. Tiến trình Go backend bị đánh dấu đầu độc (`poisoned`), từ chối các thao tác credential tiếp theo.
2. **Các điều TUYỆT ĐỐI CẤM (Forbidden Actions):**
   - **KHÔNG** dùng lệnh SQL thủ công để sửa trạng thái bản ghi trong CSDL sang `active` hoặc `succeeded`.
   - **KHÔNG** chạy lệnh `force_ready` hay bỏ qua các bước kiểm tra.
   - **KHÔNG** tự ý chạy lại script bootstrap để ghi đè lên store `dynamic-security.json` đang hoạt động của broker.
   - **KHÔNG** khởi động lại broker như một biện pháp khắc phục định kỳ mù quáng.
3. **Quy trình xử lý an toàn:**
   - Khởi động lại container Go backend để giải phóng cờ `poisoned` trong RAM (khi khởi động lại, `SimplifiedStartupClassifier` sẽ phân loại an toàn trạng thái CSDL).
   - Kiểm tra log chi tiết của broker Mosquitto và Go backend để xác định nguyên nhân (mất mạng, timeout, hoặc sai lệch ACL).
   - Thực hiện đánh giá trực tiếp trạng thái của Gateway trên broker qua công cụ dòng lệnh được cấp quyền trước khi tiến hành các bước tiếp theo.

---

### 2.5 Xử Lý Sự Cố Khi Subscribe Bị Từ Chối (`All subscription requests were denied`)

> **Ghi chú lịch sử:** Cảnh báo downlink trong phần dưới mô tả trạng thái
> credential/ACL của checkpoint DynSec cũ, không phải trạng thái hiện hành của
> Stage 5. P6 dùng quyền data-principal hiện hành để dispatch/nhận result theo
> TLS/auth/ACL đã cấu hình; tham khảo [protocol MQTT](../protocols/mqtt-v1.md)
> và [báo cáo P6](../backend/paper-p6-dispatch-and-mixed-load.md). Không tự cấp
> thêm quyền hoặc thay ACL dựa trên đoạn lịch sử này.

Nếu một client MQTT (như công cụ kiểm thử `mosquitto_sub` sử dụng tài khoản `backend_service`) kết nối TLS thành công (xác thực mật khẩu hợp lệ) nhưng lệnh đăng ký topic lập tức bị từ chối với thông báo:
```text
All subscription requests were denied.
```

1. **Nguyên nhân cốt lõi:**
   - Mosquitto DynSec quản lý quyền subscribe dưới dạng `subscribeLiteral` kết hợp nguyên tắc Default Deny (`defaultACLAccess = false`).
   - Quyền được cấp cho `backend_service` là chuỗi nguyên văn `gateways/+/telemetry/#`.
   - Nếu client gửi lệnh subscribe với topic cụ thể (ví dụ: `-t "gateways/gateway_001/telemetry/abc"`), broker so sánh chuỗi đăng ký với danh sách `subscribeLiteral` và từ chối vì không khớp nguyên văn chuỗi được cấp, mặc dù về mặt logic topic này nằm trong không gian wildcard `+` và `#`.
2. **Cách khắc phục khi kiểm thử:**
   - Đăng ký chính xác bằng chuỗi topic filter đã được cấp quyền trong store:
     ```bash
     mosquitto_sub -h <host> -p 8883 -t "gateways/+/telemetry/#" -u "backend_service" --cafile <ca_path>
     ```
   - Nhập mật khẩu qua prompt an toàn hoặc qua file, tránh dùng `-P <password>` lộ mật khẩu ra process list.
3. **Cảnh báo về quyền điều khiển thiết bị (Downlink Commands):**
   - Tại checkpoint lịch sử của tài liệu này, tài khoản `backend_service` được ghi nhận là chưa có quyền publish `gateways/+/commands/#` hoặc subscribe `gateways/+/responses/#`; nhận định đó đã được supersede bởi Stage 5/P3–P6 hiện hành.
   - Các ví dụ probe ACL bên trên chỉ kiểm tra subscription telemetry; chúng không phải hướng dẫn sửa ACL Stage 5 hiện hành.

### 2.6 Phục hồi startup Stage 5 khi broker vắng mặt (P6 Local MVP evidence)

Phần này ghi lại quy trình đã được kiểm chứng trong **fixture cô lập** với
credential API bật; đây không phải ủy quyền chạy lệnh điều khiển trên shared/live
deployment. Tham khảo [báo cáo P6](../backend/paper-p6-dispatch-and-mixed-load.md)
và [artifact rollup đã loại dữ liệu nhạy cảm](../backend/results/p6/summary.json).

Khi backend khởi động trong lúc broker chưa sẵn sàng, kiểm tra readiness của
credential runtime có thể thất bại đóng và `/readyz` trả HTTP 503. Trong lần
được thử nghiệm, broker đã trở lại khỏe mạnh nhưng backend vẫn readiness 503;
không có automatic retry/readiness recovery được chứng minh, và không giả định
thay đổi semantics. Recovery được chấp nhận là run `ncs_ce2eb3785586`: giữ broker
khỏe liên tục rồi restart **chỉ backend**. `p6-recovery.json` là artifact có thẩm
quyền cho acceptance; `run.json` cùng chế độ chỉ là metadata
`PREPARED_NOT_EXECUTED`, không phải execution evidence. Các recovery attempts
FAIL trước đó vẫn được giữ trong report.

#### Quy trình operator trong phạm vi đã thử nghiệm

1. Trước khi restart, xác nhận broker healthy và các điều kiện TLS certificate/CA,
   authentication, data ACL và manager ACL/readback đang đúng; backend Stage 5
   cần cấu hình hợp lệ. Không tắt TLS, sửa ACL tự động hoặc dùng principal khác
   để làm readiness xanh. Giữ broker hoạt động ổn định trong toàn bộ recovery.
2. Nếu credential API đang bật và startup xảy ra khi broker mất kết nối, kiểm tra
   `/readyz`. Sau khi broker đã khỏe, nếu backend vẫn trả 503, không chờ một
   automatic retry chưa được xác nhận và không restart broker. Dùng đúng tên
   service Compose do operator sở hữu; ví dụ minh họa, không phải lệnh cho
   deployment cụ thể:
   ```sh
   docker compose restart <backend-service>
   ```
   Không restart DB, broker, proxy hay Gateway trong quy trình đã nghiệm thu.
3. Chờ readiness của backend trả HTTP 200 (`/readyz`). Nếu Nginx trả 504,
   đó là proxy/upstream timeout và cần phân biệt với HTTP 503 do Go readiness;
   nếu recreate backend làm đổi địa chỉ upstream động, operator quản lý proxy
   có thể cần reload cấu hình Nginx của chính họ. Không reload proxy như một
   cách chữa Go readiness 503.
4. Tra command hiện có qua API/DB, kiểm tra nó đi tới trạng thái terminal đúng
   deadline gốc, rồi đối chiếu `reported.configuration` trong DB và REST sau
   khi valid result transaction commit. TTL có thể hết hạn trong outage; command
   timeout phải giữ terminal, và late result không được phép ghi đè
   configuration. Không gia hạn/reset deadline, không tạo lại command đã
   published để thay cho command cũ, không sửa state thủ công.
5. Xác nhận bounded Gateway outbox drain và receipt/configuration watermark
   trong evidence phù hợp. Native run dừng và kiểm tra SQLite receipt; điều này
   không chứng minh số lần physical apply hoặc exactly-once execution.

**Evidence qualification:** `ncs_ce2eb3785586` xác nhận broker stable
`ID`/`startTime`, readiness 200 sau restart backend-only, pending command giữ
cùng ID/payload hash và thành `succeeded`, DB/REST configuration agreement,
stopped SQLite receipt/config watermark và bounded outbox drain; cleanup exact
20 containers / 1 network / 7 volumes. Sau restart, process-local latency origin
bị mất và được ghi missing, không bịa latency 0. Bằng chứng chỉ thuộc
credential-enabled startup prerequisite và fixture này; không chứng minh mọi
kiểu MQTT reconnect, tự phục hồi readiness, physical apply count, exactly-once,
hay live recovery.

### 2.7 Chuẩn bị rehearsal/rollout điều khiển Sensor Stage 5

Phần này là checklist chuẩn bị cho **môi trường rehearsal do operator sở hữu**,
không phải lệnh enable shared/live hay production acceptance. Contract API nằm
tại [Stage 5 Sensor Control API](../protocols/stage5-sensor-control-api.md);
transport và result tại [MQTT v1](../protocols/mqtt-v1.md). Shared/live enable,
target migration/key/credential rehearsal và demo thuộc Stage 9, chỉ thực hiện
khi có yêu cầu riêng.

#### Preflight trước khi bật

1. Chốt đúng checkout/config đích và xác nhận schema trong chính DB mục tiêu:
   đọc `schema_migrations`, so với migration terminal trong source, kiểm tra
   grants runtime cho `iot_backend_app`/`iot_backend`; không suy ra DB đã migrate
   từ source. Với DB có dữ liệu, lập backup đã kiểm tra và cửa sổ bảo trì trước
   migration có thể khóa/chạm dữ liệu. Không chạy migration tùy thứ tự, không
   dùng fixture disposable credentials lên deployment DB.
2. Kiểm tra broker URL, CA trust chain, SAN/hostname khớp endpoint thật, thời gian
   UTC/certificate trên backend, broker và Gateway. Không tắt TLS hostname
   verification để chữa lỗi.
3. Duy trì tách biệt backend data principal với DynSec manager. Trước mỗi
   data-connection lifetime, xác minh chính xác ACL readback bằng manager kể cả
   khi credential API tắt. Manager có quyền quản trị; không đưa manager secret,
   DB credential, service-role hay broker manager quyền cho Gateway/client.
   Không tự sửa ACL runtime hoặc cấp quyền `$CONTROL`.
4. Đối chiếu cấu hình thật với bounded defaults/limits trong `.env.example` và
   `internal/config`: `STAGE5_PUBLISH_QUEUE_CAPACITY=32`,
   `STAGE5_SCAN_BATCH=32`, `STAGE5_PUBLISH_WORKERS=1`,
   `STAGE5_RESULT_QUEUE=128`, `STAGE5_RESULT_WORKERS=2`,
   `STAGE5_COMMAND_TTL=60s`; payload cap mặc định 8 KiB. Đây là cấu hình mặc
   định, không phải sizing/capacity evidence. `STAGE5_WAKEUP_ENABLED` chỉ bật
   hint; periodic/startup scan vẫn là fallback. Không gia hạn TTL hoặc biến
   timeout thành retry/reconciliation guarantee.
5. Xác nhận `/readyz`, backend process health, broker TLS/auth, subscription/
   publish readiness, membership và Sensor đã provision; thực hiện readback ACL
   trước khi gửi intent. Readiness không thay thế kiểm tra permission hoặc
   chứng minh thiết bị đã apply.

#### Theo dõi và xử lý sự cố

- Theo dõi admission/API errors, command list/detail qua REST polling, trạng thái
  expiry, queue/worker saturation, broker publish/connect, result queue/worker,
  result validation/commit và agreement giữa command terminal với
  `reported.configuration`. Đối chiếu state qua `?format=simplified`; sampling
  observations/history qua `/temporal`. Không dùng PUBACK làm bằng chứng apply.
- Rehearsal hữu hạn cần kiểm tra broker unavailable và DB unavailable riêng,
  request timeout/queue bounds, cùng controlled backend stop/reopen. Ghi status,
  deadline gốc, readiness và dữ liệu bền đã đối chiếu; lỗi DB/broker không được
  diễn giải thành success. Reuse P3/P6 recovery evidence: P6 đã kiểm tra backend
  restart khi broker được giữ ổn định; không chứng minh mọi dạng broker outage,
  tự phục hồi readiness, physical apply count hoặc exactly-once. Gateway SQLite
  receipt/outbox bền, nhưng execution vật lý không thể suy ra chỉ từ receipt.
- Khi dừng có kiểm soát, operator ngừng gửi request mới trước, dùng REST polling
  theo dõi command đang active tới terminal/TTL gốc, rồi disable bằng feature
  controls hiện có trong protected environment/configuration (`STAGE5_ENABLED`
  và, nếu đang chạy Gateway control runtime, `GATEWAY_CONTROL_ENABLED`) và dùng
  quy trình restart hiện hành của môi trường sở hữu. Không thêm quiesce API,
  không xóa command/outbox và không gọi timeout là hoàn tác. Disable/rollback
  binary hoặc flag không đảo ngược hành động thiết bị đã xảy ra; late result
  không tự ghi đè configuration mới.

Ví dụ API dùng placeholder; inject URL/token từ secret manager hoặc protected
file (ví dụ header file riêng mode `0600`; không đặt secret trong shell history
hay command arguments):

```sh
curl --fail-with-body \
  -H "@${AUTH_HEADER_FILE}" \
  -H 'Accept: application/json' \
  --get "${API_BASE_URL}/v1/digital-twins/${SENSOR_ENTITY_ID}/commands" \
  --data-urlencode 'limit=20'
```

`AUTH_HEADER_FILE` ở ví dụ GET có Bearer token của viewer (owner/operator cũng
có quyền đọc). Chỉ owner/operator dùng header file tương ứng để PATCH; mỗi
intent mới phải có version vừa đọc và idempotency key mới:

```sh
curl --fail-with-body \
  -H "@${OWNER_OR_OPERATOR_AUTH_HEADER_FILE}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: <NEW_OPAQUE_KEY>' \
  --request PATCH \
  --data '{"frequency_hz":1.5,"expected_desired_version":"<CURRENT_VERSION>"}' \
  "${API_BASE_URL}/v1/digital-twins/${SENSOR_ENTITY_ID}/desired-state"
```

Viewer không được gọi PATCH; backend vẫn enforcement membership/role ở server.
Không retry intent chưa rõ kết quả bằng key/body mới.

Không `source .env` deployment để chạy thử, không in resolved Compose config
hoặc credential. `STAGE5_ENABLED=false`, `STAGE5_WAKEUP_ENABLED=false`,
`GATEWAY_CONTROL_ENABLED=false` và các cờ shaper/microbatch/RAM-dedup/WebSocket
đều giữ mặc định độc lập false; bật control không ngầm bật tối ưu hóa hoặc WS.
AM5728 chưa có: board synthetic runtime và HIL cảm biến thật **NOT RUN**; simulator
không thay thế hardware. Remote CI và mọi thao tác target/live chưa được thực
hiện trong phần tài liệu này.
