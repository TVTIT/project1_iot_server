# Hướng dẫn Vận hành Kiểm thử E2E Giai đoạn 2 (Stage 2 E2E Testing Guide)

Tài liệu này hướng dẫn người vận hành thực hiện kiểm thử tích hợp đầu-cuối (End-to-End - E2E) cho hệ thống Backend IoT Platform trong Giai đoạn 2 (Stage 2) theo hai phương thức:
1. **Kiểm thử tự động trên cụm hạ tầng cô lập (Automated Isolated Harness)**: Sử dụng kịch bản runner chuẩn hóa và bộ giám sát CI độc lập.
2. **Kiểm thử thủ công bằng Bruno (Manual Bruno Testing)**: Sử dụng bộ sưu tập API Bruno trên môi trường thử nghiệm kiểm soát.

---

## 1. Ranh giới Nghiệm thu và Phạm vi Giai đoạn 2 (Stage 2)

### 1.1 Ranh giới kỹ thuật và luồng kiến trúc đã hoàn thành
Giai đoạn 2 thiết lập và bảo đảm an toàn cho các luồng mạng tách biệt:
- **Luồng xác thực người dùng (Auth Flow)**:
  `Client -> HTTPS -> Nginx -> Supabase API Gateway (Envoy) -> Supabase Auth (GoTrue)`
  Người dùng đăng nhập nhận JWT mang role chuẩn `authenticated`.
- **Luồng nghiệp vụ Backend (Business API Flow)**:
  `Client -> HTTPS -> Nginx -> Go Backend (cmd/server) -> PostgreSQL (TimescaleDB)`
  Go Backend tiếp nhận yêu cầu, tự xác thực chữ ký/thời hạn JWT cục bộ qua khóa ký Supabase (JWKS/secret) mà không cần gọi ngược GoTrue trên từng request, sau đó đối soát phân quyền trong CSDL.
- **Luồng truyền thông Gateway (MQTT Flow)**:
  `Gateway -> MQTT over TLS -> Mosquitto Broker (Dynamic Security / DynSec Plugin) -> Go Ingress`
  Mosquitto quản trị quyền truy cập qua plugin Dynamic Security (DynSec) với các role/group được gán tường minh theo từng Gateway, không dùng cơ chế `%u` truyền thống.
- **Chính sách tài khoản tập trung (Centralized Accounts Policy)**:
  Vô hiệu hóa hoàn toàn tự đăng ký tài khoản công khai (public self-signup) và tự nhận Gateway (self-claiming).
  *Lưu ý triển khai*: Việc đăng ký công khai bị vô hiệu hóa đã được kiểm chứng và khóa chặt trên fixture cô lập (`GOTRUE_DISABLE_SIGNUP: "true"`). Trên các môi trường staging hoặc triển khai thực tế, người vận hành **bắt buộc phải kiểm tra và xác nhận cấu hình GoTrue/API Gateway**, không được mặc định suy đoán mọi môi trường đều đã khóa nếu chưa qua kiểm tra.
- **Phân định quyền hạn**: Quyền quản trị nền tảng (`platform_admins`) tách biệt hoàn toàn với quyền thành viên trên từng Gateway (`user_gateways`).

### 1.2 Ranh giới tính năng chưa triển khai (Deferred / Out of Scope)
Các tính năng sau **chưa** nằm trong phạm vi bàn giao của Stage 2 và được giữ ở trạng thái stub hoặc bảo lưu cho giai đoạn tiếp theo:
- **Lưu trữ dữ liệu đo lường thời gian thực (Telemetry Persistence)** và xác nhận mức ứng dụng (Application ACK) cho chuỗi mẫu cảm biến.
- **Truy vấn lịch sử đo lường** (`GET /v1/telemetry/history`): Yêu cầu JWT hợp lệ; trả về `501 Not Implemented`.
- **Kênh truyền thời gian thực WebSocket** (`GET /v1/ws`): Yêu cầu JWT hợp lệ; trả về `501 Not Implemented`.
- **Mô hình Digital Twin đầy đủ** (`GET /v1/digital-twins`): Yêu cầu JWT hợp lệ; trả về `501 Not Implemented`.
- **Tải lên tệp hình ảnh/đa phương tiện** (Supabase Storage upload / signed URL).
- **Lệnh điều khiển thiết bị (Downlink Commands / Desired State Reconciliation)**.

### 1.3 Tuyên bố tính hợp lệ của bằng chứng (Evidence Status)
- **Kiểm chứng cục bộ (Local Evidence)**: Đã hoàn tất thực nghiệm 27/27 kịch bản (E01–E27) qua 2 lần chạy liên tiếp trên harness cô lập và 2 lượt chạy mô phỏng CI supervisor đạt exit code 0 (`PASS`).
- **Thẩm duyệt CI (CI Review)**: Reviewer độc lập đã phê duyệt cấu hình và kịch bản supervisor.
- **Nghiệm thu CI từ xa (Remote CI Acceptance)**: Trạng thái `PENDING`. Bằng chứng trên GitHub Actions từ xa gắn với commit SHA cuối cùng (final SHA) chưa được hoàn thành.
- **Cảnh báo sẵn sàng triển khai**: Tuyệt đối **không** tuyên bố hệ thống đã sẵn sàng cho môi trường sản xuất (no production-ready claims) khi các cổng kiểm định thực tế trên hạ tầng triển khai từ xa chưa hoàn tất.

---

## 2. Chế độ 1: Kiểm thử Tự động trên Harness Cô lập (Automated Isolated Harness)

### 2.1 Bản chất hạ tầng cô lập (Owned Isolated Stack)
Kiểm thử tự động Stage 2 chạy trên một cụm container Docker tạm thời (ephemeral stack) do kịch bản kiểm thử tự khởi tạo và tự quản lý vòng đời.
- **Quy tắc bất biến**: Tuyệt đối **không** source tệp cấu hình `.env` của môi trường deployment.
- Mọi mật khẩu CSDL, JWT secret, khóa ký, chứng chỉ CA và chứng chỉ TLS đều được sinh ngẫu nhiên trong bộ nhớ cho riêng phiên chạy đó.
- Không gây ảnh hưởng, không ghi đè dữ liệu lên bất kỳ CSDL hoặc broker Mosquitto nào đang hoạt động trên máy chủ.

### 2.2 Yêu cầu tiên quyết (Prerequisites)
Người vận hành cần chuẩn bị môi trường chạy cục bộ với các phiên bản công cụ và container image cố định:
- **Hệ điều hành**: Linux (Ubuntu 22.04 / 24.04 khuyến nghị)
- **Docker Engine & Docker Compose**: Compose v2 hỗ trợ cờ gán nhãn container.
- **Go**: Phiên bản `1.27.1` (theo `src/go.mod`).
- **Python**: Phiên bản `3.10+` (yêu cầu các module chuẩn: `unittest`, `urllib`, `ssl`, `hmac`, `hashlib`, `subprocess`).
- **OpenSSL**: Khả thi trên dòng lệnh để hỗ trợ kiểm tra chứng chỉ.
- **Container Images (phân định rõ Digest SHA256 và Phiên bản Tag đã kiểm chứng)**:
  - Ghim bằng Digest SHA256 cố định:
    - PostgreSQL / TimescaleDB: `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
    - Mosquitto Broker: `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`
  - Ghim bằng Phiên bản Tag (được CI Supervisor đối soát mã định danh `image inspect`):
    - Envoy API Gateway: `envoyproxy/envoy:v1.39.1`
    - Nginx Reverse Proxy: `nginx:1.25-alpine`
    - Supabase GoTrue Auth: `supabase/gotrue:v2.196.0`

### 2.3 Khởi chạy Runner từ thư mục gốc mã nguồn
Mọi thao tác kiểm thử phải được thực thi từ thư mục gốc repository (`<REPO_ROOT>`):

```bash
# 1. Chuyển vào thư mục gốc repository
cd "<REPO_ROOT>"

# 2. Thiết lập định danh tệp nhật ký mới và duy nhất (tránh dùng chung tệp log lịch sử)
UNIQUE_RUN_ID="$(date +%s)-$$"
export STAGE2_E2E_LOG="/tmp/task27-e2e-${UNIQUE_RUN_ID}.log"

# 3. Nhập mật khẩu kiểm thử được bảo vệ (không echo ra màn hình, không qua argv dòng lệnh)
printf "Nhập mật khẩu kiểm thử Stage 2: "
stty -echo
read -r STAGE2_TEST_PASSWORD
stty echo
printf "\n"
export STAGE2_TEST_PASSWORD

# 4. Thực thi kịch bản Runner chính
sh scripts/test-stage2-e2e.sh
```

**Lưu ý bảo mật về `STAGE2_TEST_PASSWORD`**:
- Mật khẩu chỉ được truyền qua biến môi trường của tiến trình hiện tại.
- Kịch bản tự động quét nhật ký sau khi chạy (`scan_log_for_secrets`); nếu phát hiện mật khẩu, Bearer token hoặc private key bị lộ trong tệp log, kịch bản sẽ báo lỗi nghiêm trọng (`RuntimeError`).

### 2.4 Cấu trúc 8 Bộ chọn (Selectors) và 27 Kịch bản (E01–E27)
Runner tổ chức 27 kịch bản kiểm thử vào 8 bộ chọn chức năng:

| STT | Tên Selector (`named selector`) | Kịch bản trực thuộc | Mục tiêu kiểm chứng chính |
|---|---|---|---|
| 1 | `accounts/auth` | E01 – E06 | Khởi tạo 6 tài khoản theo vai trò; đăng nhập; xác thực Bearer token; chặn đăng ký tự do (422 `signup_disabled`); chặn người dùng thường gọi Auth Admin API (403). |
| 2 | `provisioning` | E07 – E09 | Platform Admin cấp phát Gateway A, Gateway B và Sensor; xác thực tính idempotent khi gửi lại cùng payload; xử lý xung đột 409. |
| 3 | `memberships/roles` | E10 – E13 | Cách ly danh sách Gateway giữa các người dùng; kiểm tra quyền đọc sensor; kiểm tra công cụ thay đổi role động (`scripts/manage-gateway-membership.sh`) với cùng một JWT. |
| 4 | `credentials/ACL` | E14 – E17 | Cấp phát MQTT credentials; bảo đảm mật khẩu trả về 1 lần duy nhất; kiểm tra kết nối TLS và cách ly namespace topic Mosquitto qua role DynSec tường minh. |
| 5 | `rotate/revoke/replay` | E18 – E22 | Xoay vòng mật khẩu (E18); kiểm tra tính bất biến của sự kiện; thu hồi quyền có mục tiêu (E20 - Gateway B giữ nguyên socket TCP); cấp phát lại (E21); kiểm chứng mất phản hồi trên mạng và replay metadata-only (E22). |
| 6 | `loss/recovery` | E23 – E25 | Khôi phục sau sự cố quyền ghi snapshot `dynsec.json` (E23 - chmod 0444 gây lỗi SAVE); khởi động lại đồng thời CSDL và Broker (E24); kiểm chứng mất kết nối CSDL trả về 503 không làm giả danh sách rỗng (E25). |
| 7 | `restart/fences` | E26 | Kiểm chứng chế độ credential vô hiệu hóa (Admin nhận 503 `credential_runtime_disabled`, Non-admin nhận 403 `forbidden`); kiểm chứng các endpoint stubs trả về 501 khi có JWT hợp lệ. |
| 8 | `upgrade/repeatability` | E27 | Diễn tập nâng cấp schema CSDL trên volume bền vững; kiểm chứng tính lặp lại toàn vẹn của toàn bộ luồng. |

### 2.5 Hành vi phụ thuộc giữa các Selector (Targeted Selectors Dependency)
Người vận hành có thể truyền tên selector cụ thể làm đối số (ví dụ: `python3 scripts/tests/stage2_e2e.py provisioning`). Tuy nhiên, cần hiểu rõ rào cản phụ thuộc nội tại:
- **Trạng thái tích lũy trong bộ nhớ**: Kịch bản `stage2_e2e.py` lưu trữ token đăng nhập và ID của tài khoản tại cấu trúc `self.created_accounts`, lưu Gateway tại `self.gateways`.
- **Ràng buộc phụ thuộc**:
  - `provisioning` yêu cầu token của `Platform Admin` và ID của `Owner A`, `Owner B` sinh ra từ `accounts/auth`.
  - `memberships/roles` yêu cầu các Gateway đã tạo trong `provisioning`.
  - `credentials/ACL` yêu cầu Gateway đã tồn tại.
- **Hành vi thực tế**: Nếu chạy một targeted selector riêng lẻ mà bỏ qua các selector khởi tạo trước đó, tiến trình kiểm thử sẽ gặp lỗi truy cập khóa (`KeyError`) và bị đánh dấu `BLOCKED` hoặc `FAIL`.
- **Khuyến nghị vận hành**: Luôn thực thi đầy đủ cả 8 selector theo đúng thứ tự mặc định để bảo đảm môi trường có đầy đủ trạng thái dữ liệu.

### 2.6 Giám sát CI với Supervisor (`stage2_ci.py`)
Khi tích hợp vào luồng CI hoặc chạy mô phỏng CI cục bộ:

```bash
# 1. Khởi tạo thư mục gốc cho đợt kiểm thử CI với định danh duy nhất
CI_ROOT="/tmp/opencode/stage2-ci-local-$(date +%s)-$$"

# 2. Lượt chạy 1: Supervisor yêu cầu thư mục STAGE2_CI_DIR chưa từng tồn tại trước đó
STAGE2_CI_DIR="${CI_ROOT}/run-1" python3 scripts/tests/stage2_ci.py

# 3. Lượt chạy 2: Kiểm chứng tính lặp lại trên thư mục độc lập mới hoàn toàn
STAGE2_CI_DIR="${CI_ROOT}/run-2" python3 scripts/tests/stage2_ci.py

# 4. Dọn dẹp hậu kiểm khi cần thiết (Supervisor quét toàn bộ ${CI_ROOT}/run-*/owner.json)
STAGE2_CI_DIR="${CI_ROOT}" python3 scripts/tests/stage2_ci.py --cleanup
```

- **Phân biệt cơ chế "Two-run"**:
  - Tiến trình `stage2_ci.py` là một **single-run supervisor**: Mỗi lần gọi, nó tạo một thư mục chạy độc lập (`exist_ok=False`), tự sinh mật khẩu ngẫu nhiên trong bộ nhớ, giám sát tiến trình con và xuất báo cáo vào `STAGE2_CI_DIR/safe/report.json`.
  - Luồng GitHub Actions (`.github/workflows/ci.yml`) chủ động gọi supervisor **2 lần tuần tự** (`run-1` và `run-2`) trên 2 thư mục con độc lập để chứng minh tính lặp lại (reproducibility) tuyệt đối từ mã nguồn sạch.
- **Quy tắc thoát nghiêm ngặt (Strict Fail-Closed)**:
  - Không chấp nhận bỏ qua (no skip): Bất kỳ kịch bản nào bị `FAIL`, `BLOCKED`, hoặc `NOT RUN` đều khiến tiến trình thoát với mã lỗi `exit 1`.
  - Chỉ khi đủ **27/27 kịch bản PASS**, **8/8 selector PASS**, và toàn bộ tài nguyên Docker/volume được dọn dẹp sạch (`cleanup_verified: true`), tiến trình mới trả về `exit 0`.
- **Dọn dẹp và Thu thập Báo cáo An toàn**:
  - Sau khi kết thúc, supervisor xóa bỏ các tệp log thô (`private.log`, `private-console.log`).
  - Chỉ có tệp báo cáo `safe/report.json` được giữ lại làm artifact sau khi đã kiểm tra không chứa secret, private key hay chuỗi nhạy cảm.

---

## 3. Chế độ 2: Kiểm thử Thủ công bằng Bruno (Manual Bruno Testing)

### 3.1 Cảnh báo An toàn Môi trường
- **Quy tắc bất khả xâm phạm**: Kiểm thử thủ công, đặc biệt là các kịch bản tạo mới, xoay vòng, thu hồi khóa hoặc khởi động lại dịch vụ, **CHỈ ĐƯỢC PHÉP THỰC HIỆN TRÊN MÔI TRƯỜNG TEST / DISPOSABLE DEPLOYMENT**.
- Tuyệt đối **không** chạy các kịch bản gây đột biến dữ liệu hoặc kiểm thử chịu lỗi trên môi trường đang phục vụ thực tế (live deployment).

### 3.2 Ma trận Loại Tài khoản (Account Types Matrix) & Phân quyền Gateway
Để kiểm thử đúng bản chất phân quyền của hệ thống, người vận hành cần chuẩn bị 6 loại tài khoản định danh mang tính ngữ nghĩa. **Không sử dụng email hoặc thông tin tài khoản thật trong tài liệu hướng dẫn**:

| Loại tài khoản (Account Type) | Vai trò trong CSDL (`platform_admins` / `user_gateways`) | Quyền hạn trên Gateway A | Quyền hạn trên Gateway B | Mục đích kiểm thử thủ công |
|---|---|---|---|---|
| **Platform Admin** | Có trong `platform_admins`; Không có dòng nào trong `user_gateways`. | Quản trị hạ tầng (Cấp Gateway/Sensor/MQTT Credential). | Quản trị hạ tầng (Cấp Gateway/Sensor/MQTT Credential). | Kiểm chứng quyền gọi API `/v1/admin/*`. Gọi `/v1/gateways` trả về danh sách rỗng (không bypass quyền đọc dữ liệu nghiệp vụ). |
| **Owner A** | Không có trong `platform_admins`; `user_gateways`: role = `'owner'` trên Gateway A. | Quyền xem Gateway A và Sensor của A. Cấp quyền quản trị provisioning thuộc Admin. | Không có quyền (nhận `404 Not Found`). | Kiểm chứng cách ly tài nguyên: Thấy Gateway A, hoàn toàn không thấy Gateway B. Trong Stage 2, các quyền ghi thiết bị/desired state chưa kích hoạt. |
| **Owner B** | Không có trong `platform_admins`; `user_gateways`: role = `'owner'` trên Gateway B. | Không có quyền (nhận `404 Not Found`). | Quyền xem Gateway B và Sensor của B. | Kiểm chứng cách ly hai chiều giữa các chủ sở hữu. |
| **Operator** | Không có trong `platform_admins`; `user_gateways`: role = `'operator'` trên Gateway A. | Quyền xem Gateway A và Sensor của A (Trong Stage 2: chỉ đọc). | Không có quyền (nhận `404 Not Found`). | Trong Stage 2, các lệnh điều khiển downlink chưa triển khai; tài khoản đóng vai trò kiểm tra phân quyền đọc và kiểm tra nâng/hạ cấp role động. |
| **Viewer** | Không có trong `platform_admins`; `user_gateways`: role = `'viewer'` trên Gateway A. | Chỉ đọc danh sách Gateway A và Sensor của A. | Không có quyền (nhận `404 Not Found`). | Kiểm chứng quyền chỉ đọc; bị từ chối mọi thao tác ghi; kiểm chứng khi bị thu hồi quyền qua công cụ quản trị. |
| **Non-member** | Không có trong `platform_admins`; Không có bất kỳ dòng nào trong `user_gateways`. | Không có quyền (`404 Not Found`). | Không có quyền (`404 Not Found`). | Người dùng có tài khoản hợp lệ nhưng chưa được gán vào Gateway nào: `GET /v1/gateways` trả `200` với mảng `[]`. |

### 3.3 Quy trình Thiết lập Tài khoản và Gán quyền Chuẩn mực
1. **Khởi tạo tài khoản người dùng**:
   - Sử dụng Supabase Studio được bảo vệ nội bộ hoặc gọi trực tiếp Supabase Auth Admin API (`POST /auth/v1/admin/users`) với `service_role` key trên mạng quản trị nội bộ.
   - **Tuyệt đối không cấp phát `service_role` key cho client Bruno hay người dùng cuối.**
   - Mọi tài khoản sau khi đăng nhập qua `/auth/v1/token` đều nhận JWT mang claim `role: "authenticated"`. Token này **không** mặc nhiên mang quyền quản trị.
2. **Kích hoạt Platform Admin qua Script Bootstrap**:
   - Thiết lập cấu hình bảo vệ qua file mật khẩu `PGPASSFILE` hoặc biến môi trường runtime; tránh truyền mật khẩu dạng gõ trực tiếp trên dòng lệnh:
     ```bash
     export PGHOST="127.0.0.1"
     export PGPORT="5432"
     export PGUSER="postgres"
     export PGDATABASE="iot_platform"
     export PGPASSFILE="${HOME}/.pgpass_protected"
     export PLATFORM_ADMIN_USER_ID="${ADMIN_USER_UUID}"

     sh scripts/bootstrap-platform-admin.sh
     ```
   - *Cơ chế bảo vệ*: Script `bootstrap-platform-admin.sh` chỉ cho phép bootstrap Platform Admin **đầu tiên** (hoặc chạy lại idempotent cho chính admin đó). Nếu CSDL đã có một admin khác, script sẽ chặn lại (`Cannot bootstrap an additional platform admin`). Không dùng các câu lệnh SQL INSERT tùy tiện.
3. **Gán quyền thành viên Gateway (`user_gateways`)**:
   - Việc tạo người dùng **không** tự động cấp quyền truy cập Gateway.
   - Để gán Owner A cho Gateway A và Owner B cho Gateway B, thực hiện qua API Provisioning của Admin (`PUT /v1/admin/gateways/{id}` với trường `owner_user_id`).
   - Để gán hoặc thay đổi quyền `operator` hoặc `viewer`, sử dụng công cụ quản trị an toàn với tệp journal được bảo vệ:
     ```bash
     PROTECTED_JOURNAL="${HOME}/.membership_audit.journal"
     touch "${PROTECTED_JOURNAL}"
     chmod 0600 "${PROTECTED_JOURNAL}"

     sh scripts/manage-gateway-membership.sh \
       --actor "${ADMIN_USER_UUID}" \
       --target-user "${TARGET_USER_UUID}" \
       --gateway "${TARGET_GATEWAY_ID}" \
       --action grant \
       --role viewer \
       --journal-file "${PROTECTED_JOURNAL}"
     ```
   - *Quy tắc bảo vệ Owner*: Công cụ `manage-gateway-membership.sh` chỉ chấp nhận role `viewer` hoặc `operator` cho các hành động `grant` và `change`, và từ chối tuyệt đối việc thu hồi (`revoke`) role `owner`.

---

## 4. Thứ tự Thực thi Kiểm thử trên Bruno và Kết quả Kỳ vọng

Khi kiểm thử bằng bộ sưu tập Bruno (nằm tại thư mục cấu hình bộ sưu tập của bạn, ký hiệu `<BRUNO_COLLECTION_DIR>`), người vận hành cần cấu hình các biến môi trường Bruno (`Environment`) bằng các placeholder tương ứng:
- `authBaseUrl`: Địa chỉ dịch vụ Auth (ví dụ: `https://supabase.test.local` hoặc `https://api.test.local` tùy theo cấu hình định tuyến Nginx).
- `baseUrl`: Địa chỉ API nghiệp vụ Go Backend (ví dụ: `https://api.test.local`).
- `adminToken`, `ownerAToken`, `ownerBToken`, `operatorToken`, `viewerToken`, `nonMemberToken`: Chứa token JWT sau khi đăng nhập.
- `adminRefreshToken`, `userRefreshToken`: Chứa refresh token dùng cho kiểm tra xoay vòng token.
- `adminUserId`, `ownerAUserId`, `ownerBUserId`, `operatorUserId`, `viewerUserId`, `nonMemberUserId`: Chứa UUID tài khoản tương ứng.
- `gatewayIdA`, `gatewayIdB`, `sensorIdShared`: Các định danh tượng trưng.

---

### Bước 1: Xác thực, Cấp mới Token và Kiểm tra Hàng rào Bảo vệ (Auth Baseline)

#### 1.1 Đăng nhập lấy JWT (Sign In)
- **Request**: `POST {{authBaseUrl}}/auth/v1/token?grant_type=password`
- **Headers**: `apikey: {{anonKey}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "email": "<USER_EMAIL>",
    "password": "<USER_PASSWORD>"
  }
  ```
- **Kết quả kỳ vọng**: HTTP `200 OK`. Phản hồi trả về `access_token` và `refresh_token`. Lưu `access_token` vào biến tương ứng (`adminToken`, `ownerAToken`, ...), lưu `refresh_token` vào biến runtime được bảo vệ.

#### 1.2 Làm mới Token Đăng nhập (Refresh Token)
- **Request**: `POST {{authBaseUrl}}/auth/v1/token?grant_type=refresh_token`
- **Headers**: `apikey: {{anonKey}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "refresh_token": "{{userRefreshToken}}"
  }
  ```
- **Kết quả kỳ vọng**: HTTP `200 OK`. Phản hồi cấp `access_token` mới và `refresh_token` mới. Không in hoặc xuất token ra tệp không bảo vệ.

#### 1.3 Kiểm tra Hàng rào Chặn Đăng ký Tự do (Public Signup Denial)
- **Request**: `POST {{authBaseUrl}}/auth/v1/signup`
- **Headers**: `apikey: {{anonKey}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "email": "<PROBE_SIGNUP_EMAIL>",
    "password": "<STRONG_PASSWORD>"
  }
  ```
- **Kết quả kỳ vọng**: Bị từ chối chính xác với HTTP `422 Unprocessable Entity`. Phản hồi chứa:
  ```json
  {
    "code": 422,
    "error_code": "signup_disabled",
    "msg": "Signups not allowed for this instance"
  }
  ```
  Kiểm tra CSDL xác nhận không có bất kỳ bản ghi probe nào được tạo trong `auth.users`.

#### 1.4 Kiểm tra Truy cập Ẩn danh hoặc Token Giả mạo (Anonymous / Invalid Token)
- **Request**: `GET {{baseUrl}}/v1/gateways`
- **Headers**: Không truyền `Authorization`, hoặc truyền `Authorization: Bearer invalid-garbage-token`.
- **Kết quả kỳ vọng**: HTTP `401 Unauthorized`. Headers phản hồi chứa `WWW-Authenticate: Bearer`. Phản hồi dạng JSON chuẩn:
  ```json
  {
    "error": {
      "code": "unauthorized",
      "message": "authentication required",
      "request_id": "<REQUEST_ID>"
    }
  }
  ```

#### 1.5 Kiểm tra Hàng rào Chặn Người dùng Thường gọi Auth Admin API
- **Request**: `POST {{authBaseUrl}}/auth/v1/admin/users`
- **Headers**: `apikey: {{anonKey}}`, `Authorization: Bearer {{ownerAToken}}` (dùng JWT của người dùng thường).
- **Body**:
  ```json
  {
    "email": "<PROBE_ADMIN_EMAIL>",
    "password": "<STRONG_PASSWORD>"
  }
  ```
- **Kết quả kỳ vọng**: HTTP `403 Forbidden`. Chỉ có `service_role` key trên mạng nội bộ mới được phép thực thi endpoint này; tuyệt đối không tạo thêm tài khoản nào.

---

### Bước 2: Khởi tạo Gateway và Sensor (Platform Admin Provisioning)

#### 2.1 Cấp phát Gateway A (Gán Owner A)
- **Request**: `PUT {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "name": "Gateway Khu Vuc A",
    "description": "Gateway thu nghiem A",
    "owner_user_id": "{{ownerAUserId}}"
  }
  ```
  *(Lưu ý: API chỉ chấp nhận các trường `name`, `owner_user_id` và `description`. Bất kỳ trường lạ nào khác sẽ gây lỗi 400 invalid_request).*
- **Kết quả kỳ vọng**: HTTP `201 Created`. Headers chứa `Cache-Control: no-store`. Phản hồi trả về:
  ```json
  {
    "gateway_id": "{{gatewayIdA}}",
    "name": "Gateway Khu Vuc A",
    "description": "Gateway thu nghiem A",
    "owner_user_id": "{{ownerAUserId}}",
    "entity_id": "urn:ngsi-ld:Gateway:{{gatewayIdA}}",
    "created_at": "<ISO8601_TIMESTAMP>"
  }
  ```
- **Kiểm tra tính Idempotent**: Gửi lại chính xác request trên với cùng payload $\rightarrow$ HTTP `200 OK` (phản hồi dữ liệu hiện hành, không tạo mới hay xung đột).
- **Kiểm tra Xung đột (Conflict 409)**: Gửi request với cùng `{{gatewayIdA}}` nhưng đổi `owner_user_id` thành UUID khác $\rightarrow$ HTTP `409 Conflict`.

#### 2.2 Cấp phát Gateway B (Gán Owner B)
- **Request**: `PUT {{baseUrl}}/v1/admin/gateways/{{gatewayIdB}}`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "name": "Gateway Khu Vuc B",
    "description": "Gateway thu nghiem B",
    "owner_user_id": "{{ownerBUserId}}"
  }
  ```
- **Kết quả kỳ vọng**: HTTP `201 Created`.

#### 2.3 Cấp phát Sensor trên Gateway A và Gateway B
- **Request A**: `PUT {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/sensors/{{sensorIdShared}}`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "name": "Cam Bien Nhiet Do PT100",
    "unit": "celsius"
  }
  ```
  *(Lưu ý DTO: Handler chỉ chấp nhận `name` và `unit`. Không truyền các trường không hỗ trợ).*
- **Kết quả kỳ vọng**: HTTP `201 Created` với `entity_id`: `urn:ngsi-ld:Sensor:{{gatewayIdA}}:{{sensorIdShared}}`.
- **Request B**: `PUT {{baseUrl}}/v1/admin/gateways/{{gatewayIdB}}/sensors/{{sensorIdShared}}`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Content-Type: application/json`
- **Body**:
  ```json
  {
    "name": "Cam Bien Nhiet Do PT100 Khu B",
    "unit": "celsius"
  }
  ```
- **Kết quả kỳ vọng**: HTTP `201 Created` với `entity_id`: `urn:ngsi-ld:Sensor:{{gatewayIdB}}:{{sensorIdShared}}`.

---

### Bước 3: Kiểm chứng Cách ly Tài nguyên và Phân quyền API Nghiệp vụ

#### 3.1 Kiểm tra Danh sách Gateway của Từng Vai trò
- **Owner A gọi**: `GET {{baseUrl}}/v1/gateways` với `Authorization: Bearer {{ownerAToken}}`
  - **Kết quả**: HTTP `200 OK`. Danh sách `items` **chỉ chứa Gateway A**, hoàn toàn không có Gateway B.
- **Owner B gọi**: `GET {{baseUrl}}/v1/gateways` với `Authorization: Bearer {{ownerBToken}}`
  - **Kết quả**: HTTP `200 OK`. Danh sách `items` **chỉ chứa Gateway B**.
- **Non-member gọi**: `GET {{baseUrl}}/v1/gateways` với `Authorization: Bearer {{nonMemberToken}}`
  - **Kết quả**: HTTP `200 OK`. Danh sách `items: []` rỗng.
- **Platform Admin gọi**: `GET {{baseUrl}}/v1/gateways` với `Authorization: Bearer {{adminToken}}`
  - **Kết quả**: HTTP `200 OK`. Danh sách `items: []` rỗng (Platform Admin không tự ý bypass để đọc danh sách nghiệp vụ nếu không được gán vào `user_gateways`).

#### 3.2 Kiểm tra Truy cập Trộm Sensor của Gateway khác (Cross-Gateway Isolation)
- **Owner A gọi**: `GET {{baseUrl}}/v1/gateways/{{gatewayIdB}}/sensors` với `Authorization: Bearer {{ownerAToken}}`
  - **Kết quả**: HTTP `404 Not Found`. Hệ thống từ chối và không tiết lộ sự tồn tại của Gateway B cho Owner A.

#### 3.3 Kiểm tra Chặn Người dùng Thường gọi API Admin
- **Owner A gọi**: `PUT {{baseUrl}}/v1/admin/gateways/any_id` với `Authorization: Bearer {{ownerAToken}}`
  - **Kết quả**: HTTP `403 Forbidden` (`platform administrator access required`). Không có thay đổi nào xảy ra trong CSDL.

---

### Bước 4: Kiểm tra Thay đổi Vai trò Thành viên Động với CÙNG MỘT JWT

Kịch bản này chứng minh Go backend kiểm tra quyền qua bảng `user_gateways` trên từng request thay vì tin tưởng mù quáng vào claim tĩnh của JWT:
1. Gán tài khoản Viewer vào Gateway A bằng script `manage-gateway-membership.sh` với vai trò `viewer`.
2. Dùng JWT của Viewer (`{{viewerToken}}`) gọi: `GET {{baseUrl}}/v1/gateways` $\rightarrow$ Thấy Gateway A với `role: "viewer"`.
3. Trong khi Viewer **vẫn giữ nguyên phiên đăng nhập (không đăng nhập lại, giữ nguyên JWT)**, người vận hành dùng script nâng cấp quyền của Viewer lên `operator`:
   ```bash
   sh scripts/manage-gateway-membership.sh \
     --actor "${ADMIN_USER_UUID}" \
     --target-user "${VIEWER_USER_UUID}" \
     --gateway "${GATEWAY_ID_A}" \
     --action change \
     --role operator \
     --journal-file "${PROTECTED_JOURNAL}"
   ```
4. Dùng **chính xác token `{{viewerToken}}` cũ** gọi lại: `GET {{baseUrl}}/v1/gateways` $\rightarrow$ Kết quả lập tức phản ánh `role: "operator"`.
5. Tiếp tục thu hồi quyền của tài khoản bằng hành động `--action revoke`.
6. Dùng **chính xác token `{{viewerToken}}` cũ** gọi: `GET {{baseUrl}}/v1/gateways/{{gatewayIdA}}/sensors` $\rightarrow$ Lập tức nhận HTTP `404 Not Found`.

---

### Bước 5: Vòng đời MQTT Credential của Gateway (Platform Admin)

> ⚠️ **HƯỚNG DẪN BẮT BUỘC VỀ IDEMPOTENCY-KEY TRONG BRUNO**:
> Trong bộ sưu tập Bruno hiện tại, các file `.bru` (như `Provision MQTT Credential.bru`, `Rotate MQTT Credential.bru`) có chứa đoạn script `pre-request`:
> ```javascript
> const { v4: uuidv4 } = require('uuid');
> bru.setVar('idempotencyKey', uuidv4());
> ```
> Đoạn script này khiến Bruno **tự động sinh mới UUID mỗi khi bạn nhấn nút "Send"**.
> Để kiểm chứng tính **Idempotent (Replay)** thành công, người vận hành **BẮT BUỘC** phải:
> 1. Mở tab **Script** $\rightarrow$ **Pre Request** trong Bruno.
> 2. Tạm thời comment hoặc vô hiệu hóa đoạn script trên (thêm `//` vào đầu dòng).
> 3. Thiết lập biến `idempotencyKey` cố định trong tab **Vars** hoặc nhập trực tiếp một UUID cố định vào Header `Idempotency-Key`.
> 4. Quy tắc phát hành key: Cùng phương thức HTTP, cùng URL và cùng một hành vi nghiệp vụ thì dùng lại key cũ để kiểm tra Replay. Khi chuyển sang hành động nghiệp vụ mới, bắt buộc phải sinh một UUID mới.

*Lưu ý DTO*: Toàn bộ các endpoint Credential (`POST`, `DELETE`, `GET`) đều yêu cầu **Body: None** và **không có Query parameters**. Truyền body hoặc query sẽ bị trả về lỗi 400 `invalid_request`.

#### 5.1 Cấp phát MQTT Credential Lần đầu (Provision)
- **Request**: `POST {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/mqtt-credential`
- **Headers**:
  - `Authorization: Bearer {{adminToken}}`
  - `Idempotency-Key: <UUID_CO_DINH>`
- **Body**: None
- **Kết quả kỳ vọng**: HTTP `201 Created`.
  - Phản hồi chứa: `gateway_id`, `credential_version: 1` (cho fixture mới khởi tạo), `password: "<RANDOM_PASSWORD>"`, `secret_returned: true`.
  - Headers chứa: `Cache-Control: no-store`, `Pragma: no-cache`.

#### 5.2 Kiểm chứng Replay Cùng Idempotency-Key (Same-Key Replay)
- **Thao tác**: Giữ nguyên `Idempotency-Key: <UUID_CO_DINH>` như ở Bước 5.1 và bấm **Send** lần 2.
- **Kết quả kỳ vọng**: HTTP `201 Created`.
  - Phản hồi trả về **METADATA-ONLY**: `credential_version: 1`, `secret_returned: false`.
  - **Tuyệt đối không có trường `password`**: Mật khẩu chỉ hiển thị đúng 1 lần duy nhất lúc tạo; không bao giờ cấp lại qua cơ chế replay.

#### 5.3 Đọc Thông tin Metadata (Get Credential Metadata)
- **Request**: `GET {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/mqtt-credential`
- **Headers**: `Authorization: Bearer {{adminToken}}`
- **Body**: None
- **Kết quả kỳ vọng**: HTTP `200 OK`.
  - Trả về thông tin trạng thái: `credential_version`, `status: "active"`, các mốc thời gian.
  - **Không bao giờ chứa trường mật khẩu**.
  - Headers chứa: `Cache-Control: no-store`, `Pragma: no-cache`.

#### 5.4 Xoay vòng Mật khẩu (Rotate Credential)
- **Thao tác**: Sinh một UUID mới cho `Idempotency-Key: <UUID_MOI>`.
- **Request**: `POST {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/mqtt-credential/rotate`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Idempotency-Key: <UUID_MOI>`
- **Body**: None
- **Kết quả kỳ vọng**: HTTP `200 OK`.
  - Trả về mật khẩu mới dùng một lần: `credential_version: 2`, `password: "<NEW_PASSWORD>"`, `secret_returned: true`.
- **Kiểm chứng Replay Rotate**: Gửi lại cùng `<UUID_MOI>` $\rightarrow$ HTTP `200 OK`, `secret_returned: false`, không có trường `password`.

#### 5.5 Thu hồi Mật khẩu (Revoke Credential)
- **Request**: `DELETE {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/mqtt-credential`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Idempotency-Key: <UUID_REVOKE>`
- **Body**: None
- **Kết quả kỳ vọng**: HTTP `200 OK`. Trạng thái chuyển thành `revoked`.

#### 5.6 Cấp phát lại sau khi Thu hồi (Reprovision)
- **Request**: `POST {{baseUrl}}/v1/admin/gateways/{{gatewayIdA}}/mqtt-credential`
- **Headers**: `Authorization: Bearer {{adminToken}}`, `Idempotency-Key: <UUID_REPROVISION>`
- **Body**: None
- **Kết quả kỳ vọng**: HTTP `201 Created`. `credential_version: 3`, mật khẩu mới được cấp, chuỗi sự kiện kiểm toán được ghi nhận thêm 1 bản ghi bất biến.

---

### Bước 6: Kiểm tra Chế độ Vô hiệu hóa (Disabled Mode) và Tuyến đường Stub (501 / 404)

1. **Khi Credential API bị vô hiệu hóa (`CredentialAPIEnabled = false`) hoặc Mosquitto chưa sẵn sàng**:
   - Phân biệt rõ hai mã lỗi:
     - Nếu bị vô hiệu hóa bởi cờ cấu hình (`CredentialAPIEnabled = false`): Platform Admin gọi route credential $\rightarrow$ HTTP `503 Service Unavailable` với mã lỗi `credential_runtime_disabled`.
     - Nếu broker Mosquitto mất kết nối hoặc startup barrier chưa hoàn tất: Platform Admin gọi route credential $\rightarrow$ HTTP `503 Service Unavailable` với mã lỗi `service_unavailable`.
   - Người dùng thường (Non-admin) gọi cùng URL đó $\rightarrow$ Bị chặn ngay tại middleware ủy quyền với HTTP `403 Forbidden` (`platform administrator access required`), quyền hạn được kiểm tra trước khi kiểm tra trạng thái dịch vụ.
2. **Các tuyến đường Stub đang phát triển**:
   - Khi **không có JWT hợp lệ**: Gọi các route stub $\rightarrow$ HTTP `401 Unauthorized`.
   - Khi **có JWT hợp lệ**:
     - `GET {{baseUrl}}/v1/telemetry/history` $\rightarrow$ HTTP `501 Not Implemented`.
     - `GET {{baseUrl}}/v1/ws` $\rightarrow$ HTTP `501 Not Implemented`.
     - `GET {{baseUrl}}/v1/digital-twins` $\rightarrow$ HTTP `501 Not Implemented`.
3. **Các tuyến đường chưa khai báo**:
   - Gọi bất kỳ route không tồn tại (ví dụ: `POST {{baseUrl}}/v1/media`) $\rightarrow$ HTTP `404 Not Found`.

---

## 5. Ranh giới Phân định: Những gì Bruno CÓ THỂ và KHÔNG THỂ Chứng minh

Người vận hành cần nắm rõ giới hạn của công cụ kiểm thử HTTP (như Bruno) để không đưa ra các kết luận sai lệch về tính sẵn sàng của toàn hệ thống:

### 5.1 Những gì Bruno CÓ THỂ Chứng minh
- Tính hợp lệ của chuỗi phản hồi HTTP qua Nginx và Envoy.
- Xác thực chữ ký và thời hạn của JWT từ Supabase GoTrue.
- Phân quyền theo vai trò người dùng trong CSDL (`platform_admins` và `user_gateways`).
- Tính idempotent của các API Admin khi nhận lại cùng một `Idempotency-Key`.
- Hành vi bảo mật của API Credential: mật khẩu chỉ trả về một lần, phản hồi replay không lộ mật khẩu, header `Cache-Control: no-store`.
- Phản hồi mã lỗi chuẩn hóa (`401`, `403`, `404`, `409`, `413`, `415`, `422`, `501`, `503`).

### 5.2 Những gì Bruno KHÔNG THỂ Chứng minh (Bắt buộc dùng Native Test Harness)
1. **Bắt tay TLS và Xác thực Chứng chỉ CA của Mosquitto**:
   - Bruno chỉ gửi request HTTPS tới Nginx. Nó không kết nối cổng 8883 của Mosquitto và không kiểm tra được tính hợp lệ của chứng chỉ CA/Server TLS trên broker.
2. **Cách ly Quyền Mosquitto ACL trên Topic qua Plugin Dynamic Security**:
   - Bruno không thể chứng minh việc Gateway A kết nối MQTT có bị chặn khi publish sang topic của Gateway B (`gateways/<gateway_b>/telemetry`) hay không. Mosquitto DynSec thực thi việc này tại tầng MQTT socket thông qua role gán tường minh.
3. **Ngắt Phiên Targeted Revoke (E20) so với Global Maintenance (E18)**:
   - Trong kịch bản E18 (Rotate), hệ thống áp dụng bảo trì ngắt toàn bộ phiên.
   - Trong kịch bản E20 (Targeted Revoke), Mosquitto DynSec chỉ chấm dứt socket của Gateway A, trong khi **Gateway B vẫn duy trì socket TCP gốc (Original Socket)** và tiếp tục gửi PINGRESP/SUBACK bình thường mà không cần kết nối lại.
   - Bruno chỉ thấy mã HTTP `200 OK` từ backend; chỉ có kịch bản Python (`stage2_e2e.py`) với các socket MQTT thật mới đối soát được socket của Gateway B có bị ngắt hay không.
4. **Hiện tượng Mất Mát Phản hồi trên Mạng Thật (True Wire-Level Transport Loss - E22)**:
   - Nếu người vận hành bấm "Cancel" trên Bruno hoặc ngắt client sau khi gửi request, đó chỉ là hành vi **người gọi chủ động hủy (caller-side discard)**.
   - Thử nghiệm E22 thực thụ đòi hỏi proxy trung gian (`stage2_response_loss.py`) đọc trọn vẹn dữ liệu từ Nginx/Backend, xác nhận CSDL đã commit trạng thái terminal (`succeeded`) và cập nhật DynSec, sau đó chủ động cắt kết nối TCP (gửi EOF/RST) về phía client.
   - **Xử lý sự cố mất mật khẩu trên đường truyền (Lost Secret Recovery)**:
     - Gửi lại cùng idempotency key **không bao giờ cấp lại mật khẩu đã mất** (để chống tấn công nghe lén/replay).
     - Nếu trạng thái hiện tại của Gateway vẫn là active/verified: Phát hành một lệnh **Rotate** mới với Idempotency-Key mới để nhận mật khẩu thay thế.
     - Nếu trạng thái đã bị thu hồi (revoked) hoặc yêu cầu đồng bộ lại (reconcile): Thực hiện quy trình **Provision** mới hoặc quy trình đối soát thẩm quyền; không có đường tắt tùy tiện.
5. **Độ Bền của Tệp Snapshot khi Mất Nguồn Điện Đột Ngột**:
   - Bruno không thể kiểm chứng tính toàn vẹn của tệp cấu hình `dynsec.json` hay CSDL PostgreSQL khi bị mất nguồn điện đột ngột hoặc lỗi phân vùng ổ cứng.
6. **Lưu trữ và Xử lý Mẫu Telemetry Thô**:
   - Bruno không chứng minh được dữ liệu đo lường 100 Hz có được ghi nhận vào hypertable TimescaleDB hay không.

> **Khuyến cáo an toàn**: Không tự ý cài đặt các công cụ can thiệp sâu (intrusive fault injection / proxy cắt gói) lên môi trường triển khai thực tế. Các kịch bản này đã được chuẩn hóa và thực nghiệm an toàn bên trong cụm harness cô lập (`scripts/test-stage2-e2e.sh`).

---

## 6. Bảng kiểm Tra soát và Thực hành An toàn Vận hành (Checklist & Safety Guidelines)

### 6.1 Bảng kiểm Tra soát Nhanh trước khi Ký Nghiệm thu (Operator Checklist)

| Hạng mục kiểm tra | Tiêu chí đạt (Pass Criteria) | Trạng thái |
|---|---|:---:|
| **1. Chạy Tự động Cô lập** | `sh scripts/test-stage2-e2e.sh` chạy đạt 27/27 PASS, 8/8 selector PASS, mã thoát `0`. | [ ] |
| **2. Quét Rò rỉ Nhật ký** | `scan_log_for_secrets` đạt; tệp log không chứa mật khẩu, private key hay Bearer token. | [ ] |
| **3. Cổng Đăng ký Tự do** | `POST /auth/v1/signup` bị từ chối 422 `signup_disabled`; không có tài khoản probe nào được tạo trong `auth.users`. | [ ] |
| **4. Cách ly Gateway** | Owner A chỉ thấy Gateway A; Owner B chỉ thấy Gateway B; Non-member thấy danh sách rỗng. | [ ] |
| **5. Bảo vệ API Admin** | Mọi tài khoản không có trong `platform_admins` gọi `/v1/admin/*` đều nhận 403 Forbidden. | [ ] |
| **6. Idempotent & One-time Secret** | Replay cùng key khi cấp credential trả về 201 nhưng metadata-only (`secret_returned: false`, không có password). | [ ] |
| **7. Không Lưu Mật khẩu** | `GET /v1/admin/gateways/{id}/mqtt-credential` không bao giờ trả về trường mật khẩu; có header `no-store`. | [ ] |
| **8. Bảo vệ Owner trong Tooling** | `manage-gateway-membership.sh` từ chối gán/thu hồi role `owner`. | [ ] |
| **9. Stubs được bảo lưu** | `/v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins` trả về đúng 501 Not Implemented khi có JWT hợp lệ. | [ ] |
| **10. Dọn dẹp Tài nguyên** | Toàn bộ container, volume kiểm thử được giải phóng sạch sẽ sau khi kết thúc. | [ ] |

### 6.2 Thực hành An toàn Thông tin Bắt buộc (Redaction & Data Hygiene)
- **Không bao giờ sao lưu mật khẩu**: Không lưu trữ mật khẩu plaintext của Gateway hay tài khoản vào các tệp ghi chú không mã hóa.
- **Không đưa Secret vào phiên làm việc**: Không gán cứng `service_role` key vào môi trường Bruno dùng chung hoặc commit tệp environment chứa secret lên Git.
- **Che mờ khi lập báo cáo**: Khi chụp ảnh màn hình hoặc xuất kết quả kiểm thử, luôn che mờ toàn bộ chuỗi token JWT (`Bearer [REDACTED]`), chuỗi DSN kết nối CSDL và các giá trị hash mật khẩu.
- **Dọn dẹp môi trường thử nghiệm**: Sau khi hoàn thành kiểm thử thủ công trên môi trường test, thực hiện thu hồi các thông tin xác thực kiểm thử và dọn dẹp các bản ghi Gateway rác được tạo trong quá trình kiểm tra.
