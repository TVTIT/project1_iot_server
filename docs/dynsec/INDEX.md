# Kiến Trúc Và Tài Liệu Quản Lý MQTT Credential DynSec

Tài liệu này là chỉ mục (index) và tổng quan kiến trúc cho hệ thống quản lý Dynamic Security (DynSec) trên broker Eclipse Mosquitto, tích hợp cùng Go backend modular monolith và cơ sở dữ liệu PostgreSQL / TimescaleDB trong dự án IoT Gateway–Server Platform (`Đồ án 1`).

Toàn bộ nội dung trong tài liệu này và các tài liệu thành phần được xây dựng và đối chiếu trực tiếp trên **MÃ NGUỒN VÀ CẤU HÌNH THỰC TẾ** tại thời điểm kiểm tra, không dựa trên các kế hoạch ghi nhớ trong quá khứ.

---

## 1. Cấu Trúc Tài Liệu DynSec

Bộ tài liệu kỹ thuật DynSec được phân chia thành các chuyên đề chuyên sâu:

1. **[docs/dynsec/INDEX.md](INDEX.md) (Tài liệu này):**
   - Kiến trúc tổng thể và sơ đồ ranh giới trách nhiệm (Responsibilities & Authority).
   - Mô hình ủy quyền, nguyên tắc phân tách tài khoản người dùng và thiết bị.
   - Bảng tổng hợp đối chiếu trạng thái và bằng chứng thực thi (Evidence & Status Matrix).
2. **[docs/dynsec/flows-and-api.md](flows-and-api.md):**
   - Các luồng vận hành chi tiết: Provision, Rotate (ROT1 sequence), Revoke, Replay, Xử lý bất định (Ambiguity) và Đầu độc tiến trình (Poisoning).
   - Quy trình rà soát và phân loại trạng thái khởi động (Startup Classifier) và giám sát sở hữu khóa (Owner Loss).
   - Hợp đồng HTTP API chi tiết: Endpoints, Headers, Hợp đồng Empty Body khắt khe, mã trạng thái và các ví dụ lệnh `curl` minh họa an toàn.
3. **[docs/dynsec/deployment-and-operations.md](deployment-and-operations.md):**
   - Thủ tục chuẩn bị triển khai dựa trên cấu hình gốc (`docker-compose.yml`, `config/mosquitto/mosquitto.conf`, `config/nginx/nginx.conf.template`).
   - Yêu cầu tiên quyết cho bootstrap offline trên host (`scripts/mqtt-bootstrap.py`, phụ thuộc binary `mosquitto_ctrl 2.0.18`).
   - Cấu hình phân quyền tệp, ánh xạ UID writer (`MQTT_DYNSEC_WRITER_UID`), mount volume thư mục chỉ đọc (`ro`).
   - Ranh giới mạng, TLS CA-only backend mount, luồng ủy quyền qua Nginx và Supabase Envoy Gateway.
   - Sổ tay vận hành an toàn cho Platform Operator (sao lưu, xử lý downtime khi rotate, ứng phó mất secret).
4. **[docs/dynsec/limitations-and-backlog.md](limitations-and-backlog.md):**
   - Phân tích chi tiết các giới hạn kỹ thuật: Rủi ro lưu trữ mật khẩu sau rotate (ROT1 vs ROT2), giới hạn mất điện (`fsync`), reset ở trạng thái chỉ từ chối (`deny-only`), thiếu hàng rào phân tán (`distributed fencing`).
   - Phân định ranh giới giữa kiểm thử diễn tập (rehearsal container) và tính tương đương bản build Dockerfile gốc (`root build parity`).
   - Danh mục công việc tồn đọng (Backlog) được phân cấp theo thứ tự ưu tiên rõ ràng.
   - Bảng đối chiếu ma trận nghiệm thu có giới hạn T01–T12 và các ngoại lệ đóng Giai đoạn 2.

---

## 2. Kiến Trúc Tổng Thể Và Ranh Giới Trách Nhiệm (Authority Boundaries)

### 2.1 Sơ đồ tương tác hệ thống

```text
[Platform Admin (Curl / Operator CLI)]
       │
       ▼ HTTPS (Supabase JWT: Bearer <token>)
[Cloudflare Tunnel / Nginx (Port 80/443)]
       │
       ├─► /auth/v1/*  ──► [Supabase Envoy API Gateway (Port 8000)] ──► [GoTrue Auth Engine]
       │
       └─► /v1/admin/* ──► [Go Backend Modular Monolith (Port 8080)]
                                │
                                ├── (1) Tra cứu quyền Admin & Khóa giao dịch
                                │       ▼
                                │   [PostgreSQL Database]
                                │   - Table: platform_admins (Kiểm tra quyền)
                                │   - Table: gateway_mqtt_credentials (Metadata projection)
                                │   - Table: gateway_mqtt_credential_events (Durable intent journal)
                                │   - Advisory Lock: Session Lock (0x494f545f43524544) & Xact Lock
                                │
                                └── (2) Thao tác quản trị DynSec qua TLS nội bộ
                                        ▼ (Mutual TLS / CA-verified: ssl://mosquitto:8883)
                                    [Eclipse Mosquitto 2.0.18 Broker]
                                    - Plugin: mosquitto_dynamic_security.so
                                    - Authority: Quản lý Client, Password Hash, Roles, ACLs
                                        │
                                        ▼ Ghi tệp nguyên tử (Atomic write + rename)
                                    [Native Store Volume (mosquitto_dynsec)]
                                    - dynamic-security.json (Mode 0600, UID 1883)
                                        ▲
                                        │ (3) Đọc kiểm tra snapshot qua mount thư mục chỉ đọc (:ro)
                                    [Go Backend DynSecSnapshotReader]
```

### 2.2 Ranh giới thẩm quyền giữa các thành phần

| Thành Phần | Thẩm Quyền Tối Cao (Authoritative Boundary) | Trách Nhiệm Cụ Thể | Những Điều TUYỆT ĐỐI KHÔNG Thực Hiện |
|---|---|---|---|
| **Mosquitto DynSec Plugin** | **MQTT Runtime & Credential Data** | - Xác thực client kết nối qua giao thức MQTT.<br>- Lưu trữ và kiểm tra password hash của Gateway.<br>- Thực thi phân quyền topic (ACL) theo namespace.<br>- Duy trì tệp lưu trữ gốc `dynamic-security.json`. | - Không biết về cấu trúc nghiệp vụ Gateway, quyền sở hữu của người dùng, hay dữ liệu sensor.<br>- Không kết nối hay tương tác trực tiếp với PostgreSQL.<br>- Không gửi tin nhắn nghiệp vụ hay phát tán secret. |
| **PostgreSQL Database** | **Durable Intent, Audit & Application Grants** | - Xác thực vai trò Platform Admin qua bảng `platform_admins`.<br>- Lưu trữ nhật ký kiểm toán bất biến `gateway_mqtt_credential_events`.<br>- Lưu trữ metadata trạng thái `gateway_mqtt_credentials`.<br>- Cung cấp khóa đồng thời `pg_advisory_xact_lock` và session lock cho backend. | - **Tuyệt đối không lưu plaintext password** của Gateway.<br>- Không lưu password hash hay salt của Mosquitto.<br>- Không duy trì bản sao chép (replica) của tệp `dynamic-security.json`.<br>- Bản ghi `recovery_needed` trong CSDL không tự động ngắt kết nối socket trên broker. |
| **Go Backend (Modular Monolith)** | **Điều Phối Nghiệp Vụ & Xác Thực Tập Trung** | - Tiếp nhận yêu cầu HTTP từ Platform Admin, kiểm tra Supabase JWT.<br>- Kiểm tra quyền admin trong CSDL trước mọi thao tác.<br>- Tạo bản ghi intent bền vững trước khi gửi lệnh tới broker.<br>- Sinh mật khẩu ngẫu nhiên qua CSPRNG trong RAM (tối thiểu 128 bit entropy).<br>- Điều phối thứ tự mutation trên broker qua client quản trị riêng biệt.<br>- Kiểm tra kết nối login mới (`fresh TLS login`) và đọc snapshot tệp chỉ đọc.<br>- Commit giao dịch CSDL trước khi phản hồi mật khẩu một lần (`SecretResult`). | - Không lưu mật khẩu tạm ra đĩa hay biến toàn cục dài hạn.<br>- Không gửi mật khẩu hay media qua giao thức MQTT.<br>- Không tự động phục hồi hay thử lại khi gặp trạng thái bất định (fail-closed). |
| **Supabase Auth (GoTrue + Envoy)** | **Định Danh Người Dùng (Human Identity)** | - Xác thực tài khoản người dùng, cấp phát access token JWT mang claim `role: "authenticated"`.<br>- Quản lý đăng nhập, đổi mật khẩu, phiên làm việc người dùng. | - Không quản lý tài khoản hay thông tin xác thực của Gateway.<br>- Không cấp quyền Platform Admin (quyền này do CSDL quản lý).<br>- Tính năng đăng ký công khai (`public signup`) bắt buộc phải tắt trên Envoy/GoTrue. |

---

## 3. Ranh Giới Định Danh Và Phân Quyền (Identity & Authorization)

Hệ thống tuân thủ nghiêm ngặt nguyên tắc phân tách định danh quy định tại `AGENTS.md`:

1. **Phân tách hoàn toàn giữa Người dùng (Human) và Trạm Gateway:**
   - Người dùng sử dụng tài khoản Supabase Auth, đăng nhập qua HTTP để nhận JWT.
   - Gateway sử dụng tài khoản MQTT độc lập (với username trùng với `gateway_id`), xác thực bằng username/password trực tiếp với Mosquitto DynSec qua cổng TLS 8883.
   - Gateway **không có tài khoản Supabase**, không nhận JWT và tuyệt đối không bao giờ được cấp khóa `service_role`.
2. **Quyền hạn quản lý Credential chỉ thuộc về Platform Admin:**
   - Các vai trò trong bảng thành viên `user_gateways` (`owner`, `operator`, `viewer`) chỉ có quyền trên dữ liệu nghiệp vụ theo phân cấp, **hoàn toàn không có quyền thao tác trên các endpoint quản lý credential** (`/v1/admin/gateways/*/mqtt-credential*`).
   - Mọi request gọi tới route credential từ người dùng không có bản ghi trong bảng `public.platform_admins` đều bị từ chối ngay lập tức với mã lỗi HTTP `403 Forbidden` (`code: "forbidden"`) **trước khi bất kỳ thao tác nào diễn ra trên CSDL hay broker**.
   - Trường hợp tài khoản vừa là Gateway Owner vừa là Platform Admin: Hệ thống kiểm tra độc lập; quyền quản trị credential được cấp dựa trên tư cách Platform Admin trong `platform_admins`, không kế thừa từ vai trò `owner`.
   - **Lưu ý về các endpoint quản trị Gateway và Sensor:** Cả endpoint duyệt danh mục quản trị toàn hệ thống `GET /v1/admin/gateways` (Admin Gateway Inventory) và endpoint cấp phát/khởi tạo cảm biến cho chủ sở hữu `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` (Owner Sensor Provisioning) đều **ĐÃ ĐƯỢC HIỆN THỰC (IMPLEMENTED)** trong mã nguồn backend (`src/internal/httpserver/admin_gateway_handlers.go`, `src/internal/httpserver/owner_sensor_handlers.go`); việc hoàn tất nghiệm thu tổng hợp (aggregate acceptance) cho toàn bộ gói tính năng đang trong giai đoạn chờ kết nối nghiệm thu cuối, không phải là tính năng chưa được triển khai.
3. **Phân tách ba Principal MQTT độc lập và Ma trận Phân quyền Topic (ACL Matrix):**
   - Mặc định toàn hệ thống: `defaultACLAccess` được đặt là `false` cho toàn bộ các hành động (`publishClientSend: false`, `publishClientReceive: false`, `subscribe: false`, `unsubscribe: false`). Mọi quyền truy cập bắt buộc phải được khai báo tường minh qua Role và ACL.
   - `Manager Principal` (`MQTT_DYNSEC_MANAGER_USERNAME`): Chỉ dùng để gửi lệnh cấu hình DynSec qua topic điều khiển `$CONTROL/dynamic-security/v1` và nhận kết quả tại `$CONTROL/dynamic-security/v1/response`. Không tham gia vào luồng publish/subscribe dữ liệu sensor.
   - `Backend Principal` (`MQTT_USERNAME`, mặc định `backend_service`): Tài khoản của dịch vụ Go backend dùng cho luồng thu thập dữ liệu (telemetry ingress).
     * **Quyền hiện tại được cấp trong `bootstrap_backend`:**
       - `subscribeLiteral` & `unsubscribeLiteral`: `gateways/+/telemetry/#` (chỉ cho phép đăng ký/hủy đăng ký đúng chuỗi bộ lọc này).
       - `publishClientReceive`: `gateways/+/telemetry/#` (cho phép nhận tin nhắn khớp với pattern này).
       - `publishClientSend`: `gateways/+/acks/#` (cho phép gửi tin nhắn ACK xác nhận đã lưu CSDL).
     * **CÁC QUYỀN CHƯA ĐƯỢC CẤP TRÊN BACKEND HIỆN TẠI:**
       - Backend **chưa có quyền** `publishClientSend` trên `gateways/+/commands/#` (chưa thể gửi lệnh điều khiển xuống Gateway).
       - Backend **chưa có quyền** `subscribeLiteral` hay `publishClientReceive` trên `gateways/+/status` hay `gateways/+/responses/#` (chưa thể nhận trạng thái hay kết quả thực thi lệnh từ Gateway).
     * **Quy tắc đăng ký nguyên văn (`subscribeLiteral`):** Mosquitto DynSec yêu cầu bộ lọc khi gọi `SUBSCRIBE` phải **khớp chính xác từng ký tự** với chuỗi topic được cấp quyền. Ví dụ: backend bắt buộc phải subscribe chuỗi `gateways/+/telemetry/#`; nếu cố tình subscribe topic cụ thể của một gateway (như `gateways/gw_001/telemetry/temp`), broker sẽ từ chối ngay với lỗi `"All subscription requests were denied."`.
   - `Gateway Principal` (Username là `gateway_id`): Được cấp role riêng `gateway_<gateway_id>` và bị cô lập hoàn toàn trong namespace riêng `gateways/<gateway_id>/#`.
     * Cho phép publish: `gateways/<gateway_id>/telemetry/#`, `gateways/<gateway_id>/status`, `gateways/<gateway_id>/responses/#`.
     * Cho phép subscribe: `gateways/<gateway_id>/acks/#`, `gateways/<gateway_id>/commands/#`.
     * **Cảnh báo ranh giới:** Việc Gateway có sẵn ACL cho topic `commands/#` và `responses/#` trong role của nó **không đồng nghĩa với việc luồng điều khiển thiết bị (downlink command pipeline) đã hoạt động**, vì `backend_service` chưa được cấp quyền tương ứng để phát lệnh hay thu nhận phản hồi.
   - Mật khẩu của Manager và Backend bắt buộc phải khác nhau và có độ dài tối thiểu đảm bảo an toàn.

| Principal / Role | Thao Tác (ACL Type) | Topic Pattern | Trạng Thái Hiện Tại |
|---|---|---|---|
| **Manager** (`bootstrap_manager`) | `publishClientSend` | `$CONTROL/dynamic-security/v1` | **Đã kích hoạt** (Quản trị DynSec) |
| | `publishClientReceive`<br>`subscribeLiteral`<br>`unsubscribeLiteral` | `$CONTROL/dynamic-security/v1/response` | **Đã kích hoạt** |
| **Backend** (`bootstrap_backend`) | `subscribeLiteral`<br>`unsubscribeLiteral`<br>`publishClientReceive` | `gateways/+/telemetry/#` | **Đã kích hoạt** (Thu thập telemetry) |
| | `publishClientSend` | `gateways/+/acks/#` | **Đã kích hoạt** (Gửi ACK cam kết CSDL) |
| | `publishClientSend` | `gateways/+/commands/#` | **CHƯA CẤP QUYỀN** (Chờ Giai đoạn 2 Digital Twin) |
| | `publishClientReceive`<br>`subscribeLiteral` | `gateways/+/status`<br>`gateways/+/responses/#` | **CHƯA CẤP QUYỀN** (Chờ Giai đoạn 2 Digital Twin) |
| **Gateway** (`gateway_<id>`) | `publishClientSend` | `gateways/<id>/telemetry/#`<br>`gateways/<id>/status`<br>`gateways/<id>/responses/#` | **Đã cấp trong Role của Gateway** |
| | `subscribeLiteral`<br>`unsubscribeLiteral`<br>`publishClientReceive` | `gateways/<id>/acks/#`<br>`gateways/<id>/commands/#` | **Đã cấp trong Role của Gateway** (Downlink chưa khả dụng phía backend) |
| | Mọi topic khác (`#`, `$CONTROL/...`, v.v.) | Bất kỳ | **Bị từ chối (Default Deny)** |

---

## 4. Bảng Tổng Hợp Trạng Thái Và Bằng Chứng Thực Thi (Evidence & Status Matrix)

Bảng đối chiếu dưới đây phân định ranh giới minh bạch giữa những gì đã được hiện thực trong mã nguồn, kiểm thử đơn vị, kiểm thử native thực tế, diễn tập container độc lập và các hạng mục còn chờ triển khai sản xuất.

> **Ràng buộc mã nguồn (Source-Manifest Caveat):** Toàn bộ bằng chứng thực thi được ràng buộc với Git HEAD `7536265dcbe92720d53efe5e1d902103ba95d985` cùng mã băm SHA-256 của các tệp nguồn chưa commit tại thời điểm thực thi. Đây là bằng chứng trên môi trường kiểm thử tập trung, không phải tuyên bố đã commit hay đã deploy trên môi trường production.

| Tính Năng / Cơ Chế An Toàn | Trạng Thái Phân Định | Mức Độ Kiểm Chứng Thực Tế | Tệp Nguồn / Tệp Kiểm Thử Cụ Thể | Bằng Chứng Thực Thi Đã Xác Minh |
|---|---|---|---|---|
| **Chặn phân quyền phi Admin (M01)** | **SERVICE NATIVE VERIFIED** | Chặn đứng yêu cầu phi admin, trả mã `403 Forbidden`, không sinh intent, không gọi executor broker. | `src/internal/mqttcredential/service.go`<br>`src/internal/httpserver/admin_mqtt_credential_handlers.go`<br>`src/internal/mqttcredential/service_simplified_native_test.go` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`<br>`task28_http_native_smoke_execution_20261012_753626.json` |
| **Quy trình Revoke & Ngắt kết nối (M02)** | **SERVICE NATIVE VERIFIED** | Target session nhận EOF, password cũ bị từ chối TLS denial, snapshot reader `VerifyRevoked` xác nhận client bị disabled và role/ACL giữ nguyên. | `src/internal/mqttcredential/dynsec_simplified_adapter.go`<br>`src/internal/mqttcredential/dynsec_snapshot.go`<br>`src/internal/mqttcredential/service_simplified_native_test.go` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`<br>`task28_http_native_smoke_execution_20261012_753626.json` |
| **Quy trình Rotate theo trình tự ROT1 (M03)** | **SERVICE NATIVE VERIFIED** | Trình tự Disable → Disabled Readback → Exact Role Readback → SetPassword → Enable → Enabled Readback → Fresh TLS Login. Version tăng đơn điệu. | `src/internal/mqttcredential/dynsec_simplified_adapter.go`<br>`src/internal/mqttcredential/service.go`<br>`src/internal/mqttcredential/service_simplified_native_test.go` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`<br>`task28_http_native_smoke_execution_20261012_753626.json` |
| **Cô lập phiên kết nối Gateway khác (M04)** | **SERVICE NATIVE VERIFIED (Giới hạn giao thức)** | Observer session không bị ngắt, publish QoS 1 nhận PUBACK, fresh TLS login thành công sau rotate/revoke của target. *Khoảng trống:* Chưa kiểm chứng receiver delivery tới subscriber hoặc TimescaleDB. | `src/internal/mqttcredential/service_simplified_native_test.go` | `a8_native_rotate_revoke_attempt4_20261011_161344.json` |
| **Idempotency Replay cùng Actor/Key (M05)** | **SERVICE NATIVE VERIFIED** | Replay cùng actor và idempotency key chỉ trả về metadata hiện tại, không gọi lại broker, không lộ mật khẩu (`secret_returned: false`). | `src/internal/mqttcredential/service.go`<br>`src/internal/mqttcredential/postgres_simplified_operation.go`<br>`src/internal/mqttcredential/service_simplified_native_test.go` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`<br>`task28_http_native_smoke_execution_20261012_753626.json` |
| **Xử lý xung đột Idempotency Key (M05 Mismatch)** | **UNIT & DB INTEGRATION TESTED (CHƯA CÓ NATIVE PROOF)** | Gửi lại cùng key nhưng khác `gateway_id` hoặc khác `action` bị từ chối với lỗi `CodeIdempotencyConflict` (409 Conflict) trong unit/DB integration test. | `src/internal/mqttcredential/postgres_simplified_unit_test.go`<br>`src/internal/mqttcredential/postgres_simplified_integration_test.go` | Đã kiểm chứng qua unit và DB integration test; **chưa thiết lập bằng chứng kiểm thử native riêng** trong fixture A8; chưa bao hàm toàn bộ Task 2.8 hay toàn bộ các trường hợp M05 binding. |
| **Không tiết lộ secret khi commit bất định (M06)** | **UNIT VERIFIED** | Lỗi finalization hoặc mất kết nối sau mutation kích hoạt cơ chế đầu độc tiến trình (`poison()`), trả về `CodeFinalizationPending`, tuyệt đối không lộ secret. | `src/internal/mqttcredential/service_simplified_test.go` (`TestSimplifiedService_FinalizeBeforeDisclosure`) | `service_simplified_test.go` offline pass. |
| **Rà soát trạng thái dở dang khi khởi động** | **UNIT VERIFIED** | `SimplifiedStartupClassifier` quét trang, chuyển trạng thái pending sang `recovery_needed`, chặn khởi động nếu gặp dữ liệu bất thường. | `src/internal/mqttcredential/simplified_startup.go`<br>`src/internal/mqttcredential/simplified_startup_test.go` | Bộ kiểm thử offline `simplified_startup_test.go` pass. |
| **Khóa sở hữu phiên CSDL chuyên dụng** | **UNIT TESTED & NATIVE/REHEARSAL EXERCISED (GIỚI HẠN PHẠM VI)** | `credentialOwner` chiếm giữ `pg_try_advisory_lock(0x494f545f43524544)` trên kết nối unshared hijacked. Mất kết nối dẫn tới fail-closed vĩnh viễn, không tự tái chiếm. | `src/cmd/server/credential_owner.go`<br>`src/cmd/server/credential_test.go`<br>`src/cmd/server/credential_owner_native_test.go` | `credential_test.go` offline pass. Rehearsal Part 1 và Part 2 thực tế đã vận hành acquire owner thành công, nhưng **chưa chứng minh mọi trường hợp mất quyền sở hữu (owner loss), đứt kết nối ngầm hay khởi động lại đa tiến trình**. |
| **Tầng HTTP API & Header Verifier (Native)** | **HTTP NATIVE SMOKE VERIFIED** | Định tuyến HTTP, middleware xác thực HS256, kiểm tra header `Idempotency-Key`, hợp đồng Empty Body, phản hồi `Cache-Control: no-store`. | `src/cmd/server/credential_http_native_test.go`<br>`scripts/tests/task28_http_credential_native.py` | `task28_http_native_smoke_execution_20261012_753626.json` |
| **Định tuyến Nginx Container Reverse Proxy** | **REHEARSAL VERIFIED (Part 1)** | Nginx container thực tế nhận request, chuyển tiếp sang Go backend container chạy nhị phân tĩnh gắn kết read-only, xử lý thành công toàn bộ luồng rotate/revoke. | `config/nginx/nginx.conf.template`<br>`scripts/tests/task28_nginx_compose_rehearsal.py` | `nginx-rehearsal-20261012-4069a8201541.json` |
| **Xác thực Đăng nhập Supabase Auth Thực Tế** | **REHEARSAL VERIFIED (Part 2)** | GoTrue Auth v2.196.0 + Envoy v1.39.1 thực tế: đăng nhập bằng password thực qua Envoy cấp token hợp lệ, được Nginx chuyển tiếp tới Go backend và chấp nhận thành công. | `scripts/tests/task28_supabase_auth_rehearsal.py` | `auth-rehearsal-20261012-63a6a259b0bf.json` |
| **Triển khai Root Compose Parity** | **DEPLOYMENT PENDING** | Cấu hình `docker-compose.yml` gốc với build multi-stage `src/Dockerfile`, volume phân quyền sản xuất, khởi tạo bootstrap thực tế trên máy chủ đích. | `docker-compose.yml`<br>`src/Dockerfile`<br>`scripts/mqtt-bootstrap.py` | Chưa chạy cutover hay deployment trên môi trường staging/production. |
| **Cổng duyệt độc lập (Independent Reviewer)** | **PENDING** | Đánh giá độc lập theo quy trình phát triển dự án chưa được miễn trừ. | Kế hoạch dự án (`AGENTS.md`) | Đang chờ nghiệm thu. |
