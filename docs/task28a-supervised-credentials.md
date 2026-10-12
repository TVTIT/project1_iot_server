# Báo Cáo Nghiệm Thu Và Hướng Dẫn Vận Hành Task 2.8a: Quản Lý Credential Có Giám Sát

Tài liệu này ghi nhận kết quả kiểm thử tích hợp (focused integration), bằng chứng thực thi, giới hạn an toàn và hướng dẫn vận hành cho luồng **Rotate** và **Revoke** MQTT credential thuộc Task 2.8a (mốc tối giản cho đồ án sinh viên).

---

## 1. Kết Quả Tổng Thể Và Ràng Buộc Bằng Chứng

### 1.1 Kết quả cốt lõi
- **Kiểm thử tích hợp dịch vụ native:** `TestSimplifiedServiceNativeRotateRevoke` (lượt **Attempt 4** ngày **2026-10-11**) đã chạy và hoàn thành với kết quả **SUCCESS** (return code `0`, thời lượng `11.101` giây). Bằng chứng thực thi: `/home/trung/.opencode/plan/task28-evidence/simplified/a8_native_rotate_revoke_attempt4_20261011_161344.json`.
- **Kiểm thử HTTP Native Smoke Test (JWT Verifier Seam):** `TestSimplifiedHTTPNativeRotateRevoke` ngày **2026-10-12** hoàn thành với kết quả **SUCCESS** (return code `0`). Xác nhận router HTTP, header `Idempotency-Key`, hợp đồng Empty Body, JWT verifier seam, và phân quyền admin. Bằng chứng: `/home/trung/.opencode/plan/task28-evidence/simplified/task28_http_native_smoke_execution_20261012_753626.json`.
- **Diễn tập Nginx Container Reverse Proxy (Part 1):** Kịch bản `task28_nginx_compose_rehearsal.py` ngày **2026-10-12** chạy thành công (**SUCCESS**). Xác nhận Nginx container thực tế định tuyến chính xác tới backend container chạy nhị phân tĩnh mount read-only, xử lý đầy đủ các bước rotate/revoke qua Nginx. Bằng chứng: `/home/trung/.opencode/plan/task28-evidence/simplified/nginx-rehearsal-20261012-4069a8201541.json`.
- **Diễn tập Supabase Auth Thực Tế (Part 2):** Kịch bản `task28_supabase_auth_rehearsal.py` ngày **2026-10-12** chạy thành công (**SUCCESS**). Xác nhận đăng nhập người dùng thực bằng mật khẩu qua Supabase Envoy Gateway + GoTrue Auth v2.196.0; token được Nginx chuyển tiếp sang Go backend và chấp nhận hợp lệ; tính năng public signup bị vô hiệu hóa an toàn. Bằng chứng: `/home/trung/.opencode/plan/task28-evidence/simplified/auth-rehearsal-20261012-63a6a259b0bf.json`.
- Mọi bài kiểm thử diễn tập đều đã xác nhận dọn dẹp sạch sẽ tài nguyên sau khi chạy (`pg_cleanup_verified: true`, `broker_cleanup_verified: true`, `host_cleanup_verified: true`).

### 1.2 Ràng buộc source code và mã băm manifest
Bằng chứng thực thi không chỉ gắn với commit HEAD tĩnh mà được ràng buộc chặt chẽ với mã băm SHA-256 của các tệp nguồn chưa commit tại thời điểm chạy:
- **Git HEAD:** `7536265dcbe92720d53efe5e1d902103ba95d985`
- **Tệp thực thi và mã nguồn gắn kết:**
  - `scripts/tests/task28_rotate_revoke_native.py`: `65d97f339ddf3a752e3c295c4855e9a088b88c0f913593d38995438a1744d928`
  - `scripts/tests/test_task28_rotate_revoke_native.py`: `10d44cb8514ad39f7b751802b211cc937e3d8c3c7f2ec093aaf9f0eab042ce3b`
  - `src/internal/mqttcredential/service.go`: `e20d2c9cf624d63a6640637e450bd074e4b672eabc3704a41d4ffb88dc1fdcf4`
  - `src/internal/mqttcredential/dynsec_simplified_adapter.go`: `a2b8be5dbad085cd6a8af828a8efd733a37c40389f92b56547682159182874ef`
  - `src/internal/mqttcredential/service_simplified_native_test.go`: `dd902f7f83902eb4ffcad68d451be320c5abdad8c2f7799af41d98af7d16cc66`
  - `src/internal/mqttcredential/service_simplified_native_input_test.go`: `14ef7121a43c31eadfca37271992b4272a2ad07fe2e490684ea4b40b15554735`

### 1.3 Phạm vi và ranh giới chứng minh
- **Những gì ĐÃ được chứng minh:** Tích hợp thực giữa Go service đơn khối, PostgreSQL repository/journal, executor DynSec tối giản và Mosquitto broker (phiên bản ghim `2.0.18`). Xác nhận việc tạo durable intent trước mutation, chặn truy cập phi admin, ngắt kết nối phiên làm việc của target gateway, từ chối password cũ, cho phép login bằng password mới, kiểm tra snapshot qua `DynSecSnapshotReader` (P1 `VerifyRevoked`), và replay idempotent trả về metadata thuần túy không lộ mật khẩu. Ngoài ra, các bài kiểm thử HTTP native và rehearsal containers đã chứng minh định tuyến Nginx, xử lý header và luồng đăng nhập Supabase Auth thực tế cấp JWT cho Platform Admin.
- **Những gì CHƯA được chứng minh:** Chưa chứng minh tính tương đương bản build của `src/Dockerfile` đa tầng và cấu hình root `docker-compose.yml` (các rehearsal sử dụng nhị phân tĩnh biên dịch trên host rồi bind-mount vào container); observer client trong kiểm thử chỉ chứng minh session không bị ngắt và publish QoS 1 nhận PUBACK trong namespace cho phép, chưa chứng minh việc nhận tin correlated qua subscriber hay lưu trữ telemetry vào TimescaleDB; chưa có kiểm thử sao lưu/phục hồi dữ liệu trên môi trường triển khai thực tế. Chi tiết xem tại [docs/dynsec/INDEX.md](dynsec/INDEX.md).

---

## 2. Bảng Đối Chiếu Bằng Chứng Nghiệm Thu Tối Thiểu (M01–M06)

Không tuyên bố toàn bộ M01–M06 đã PASS hoàn toàn; bảng dưới đây phân định minh bạch giữa kiểm thử đơn vị offline, kiểm thử native thực tế và các khoảng trống (gaps) còn lại:

| Mã Kiểm Thử | Tên / Mục Tiêu | Loại Kiểm Thử & Tên Hàm Thực Tế | Bằng Chứng / Tài Liệu Lưu Vết | Tình Trạng & Đánh Giá Trung Thực |
|---|---|---|---|---|
| **M01** | **Authorization** (Chặn phi admin trước mutation) | Native service test: `TestSimplifiedServiceNativeRotateRevoke`; Offline unit: `TestSimplifiedService_AuthorizationAndResetBlocker` | `a8_native_rotate_revoke_attempt4_20261011_161344.json` | **PASS (Native & Offline)**: Tài khoản không có cờ `platform_admins` bị trả mã `CodeForbidden`, executor broker không bị kích hoạt. *Lưu ý:* Chưa bao gồm luồng cấp phát Supabase JWT từ client thực tế. |
| **M02** | **Revoke** (Target session bị ngắt, password bị từ chối, snapshot xác nhận) | Native service test: `TestSimplifiedServiceNativeRotateRevoke`; Offline executor: `TestSimplifiedDynSecExecutor_PublicRevoke_QualifiedSuccess`, `TestSimplifiedDynSecExecutor_PrivateRevoke` | `a8_native_rotate_revoke_attempt4_20261011_161344.json` | **PASS (Native & Offline)**: Target phiên đang mở nhận EOF sau `disableClient`; connect mới bằng password bị từ chối TLS auth denial; snapshot file reader `VerifyRevoked` xác nhận client bị disabled và role/ACL không đổi. |
| **M03** | **Rotate** (Disabled readback trước setPassword, password mới PASS, cũ FAIL) | Native service test: `TestSimplifiedServiceNativeRotateRevoke`; Offline executor: `TestSimplifiedDynSecExecutor_Rotate_Success`, `TestSimplifiedDynSecExecutor_Rotate_IntermediateReadbackFaults` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`, `rot1-ordered-rotate-flow-20261011.md` | **PASS (Native & Offline)**: Thứ tự executor thực thi nghiêm ngặt (Disable → Readback → SetPassword → Enable); fresh login password mới thành công; password cũ bị từ chối; credential version tăng từ 1 lên 2. |
| **M04** | **Isolation** (Observer gateway duy trì traffic khi target bị mutate) | Native service test: `TestSimplifiedServiceNativeRotateRevoke` | `a8_native_rotate_revoke_attempt4_20261011_161344.json` | **PASS CÓ GIỚI HẠN (Native Protocol/Auth Isolation)**: Observer session duy trì kết nối, publish QoS 1 nhận PUBACK, và fresh TLS login thành công sau rotate/revoke. **Khoảng trống (Gap):** Do ACL cấm subscribe chéo, chưa có kiểm thử đầu nhận (receiver delivery) correlated hay ghi nhận telemetry vào database. |
| **M05** | **Retry & Idempotency** (Replay cùng key chỉ trả metadata; key binding sai bị từ chối) | Native service: `TestSimplifiedServiceNativeRotateRevoke`; Offline unit: `TestSimplifiedService_ReplayAndRevoked`, `TestExecutionContract_Validate`, `TestBeginSimplifiedOperation_UnitErrors`, `TestModelOperationInvariantsAndReplay` | `a8_native_rotate_revoke_attempt4_20261011_161344.json`, `postgres_simplified_unit_test.go` | **PHÂN ĐỊNH**: Replay cùng actor và cùng key không gọi lại executor và không lộ password đã PASS native. Lỗi lệch key binding (`CodeIdempotencyConflict`) đã có assertion ở tầng unit model (`TestModelOperationInvariantsAndReplay`) và Postgres integration test (`postgres_simplified_integration_test.go`), nhưng chưa thiết lập bằng chứng kiểm thử native riêng (native test proof not established) cho kịch bản payload gửi sai lệch key binding. |
| **M06** | **Failure / No-Secret** (Bất định không trả secret; lỗi startup/unfinished intent) | Offline unit: `TestSimplifiedService_FinalizeBeforeDisclosure`, `TestSimplifiedService_FinalizeErrorPreservesSuccessAndReplay`, `TestSimplifiedStartupClassifier_*` | `service_simplified_test.go`, `simplified_startup_test.go` | **PHÂN ĐỊNH**: Cơ chế bất định finalization không cấp secret và đầu độc tiến trình (`poison()`) đã PASS offline unit. Việc xử lý unfinished intent khi khởi động lại backend đã được kiểm chứng qua các trường hợp kiểm thử trong bộ `simplified_startup_test.go`, nhưng chưa chạy kịch bản fault injection trực tiếp trong fixture native A8. |

---

## 3. Hướng Dẫn Vận Hành Hệ Thống (Operator Guide)

### 3.1 Khóa cấu hình bắt buộc (Exact Current Configuration Keys)
Hệ thống sử dụng các biến môi trường sau để kích hoạt và vận hành mô-đun quản lý credential. Không chèn giá trị bí mật hoặc đường dẫn thực tế của môi trường triển khai vào mã nguồn:

- `MQTT_CREDENTIAL_API_ENABLED`: Cờ kiểu boolean (`true`/`false`), bắt buộc đặt `true` để kích hoạt API quản lý credential.
- `INGRESS_GATE_ENABLED`: Cờ boolean (mặc định chấp nhận `true` nếu không thiết lập; nếu thiết lập phải là chuỗi parse được sang boolean hợp lệ). Để kích hoạt API quản lý credential (`MQTT_CREDENTIAL_API_ENABLED=true`), yêu cầu cấu hình bắt buộc là: cờ `INGRESS_GATE_ENABLED` phải hợp lệ và legacy runtime (`MQTTCredentialRuntime`, tức `MQTT_CREDENTIAL_RUNTIME_ENABLED`) không được kích hoạt (`cfg.MQTTCredentialRuntime.Enabled == false`).
- `MQTT_DYNSEC_MANAGER_USERNAME`: Username tài khoản quản trị DynSec (khác với `BackendUsername`).
- `MQTT_DYNSEC_MANAGER_PASSWORD`: Mật khẩu tài khoản quản trị DynSec (tối thiểu 128 bit entropy).
- `MQTT_PASSWORD`: Mật khẩu tài khoản backend ingestion. Bắt buộc khác với mật khẩu của manager.
- `MQTT_INTERNAL_MANAGEMENT_URL`: Địa chỉ URL nội bộ kết nối Mosquitto qua TLS, bắt đầu bằng giao thức `ssl://` và bao gồm hostname cùng port (ví dụ cấu hình mẫu: `ssl://mosquitto:8883`).
- `MQTT_TLS_CA_FILE`: Đường dẫn tuyệt đối, định dạng chuẩn (clean absolute path) tới tệp chứng chỉ CA tin cậy.
- `MQTT_DYNSEC_JSON_PATH`: Đường dẫn tuyệt đối, định dạng chuẩn tới tệp lưu trữ native DynSec (`dynamic-security.json`).
- `MQTT_DYNSEC_WRITER_UID`: Giá trị số nguyên base10 không dấu 32-bit (`uint32`), chấp nhận mọi giá trị từ `0` đến `4294967295` (bao gồm `0`). Giá trị này phải khớp chính xác với UID thực tế của tiến trình ghi tệp native store trong môi trường triển khai thực tế. Giá trị `1883` chỉ là danh tính quy ước riêng của môi trường kiểm thử (fixture identity), không phải giá trị mặc định của triển khai.
- `MQTT_DYNSEC_IMAGE_REFERENCE`: Định danh tham chiếu ảnh container Mosquitto được chuẩn hóa (ví dụ: `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`).
- `MQTT_DYNSEC_IMAGE_CONFIG_ID`: Mã nhận diện cấu hình kiểm định tương thích.
- `MQTT_DYNSEC_PLATFORM`: Chuỗi định danh nền tảng OS/kiến trúc thực thi (ví dụ: `linux/amd64`).
- Các thông số thời gian chờ và giới hạn: `MQTT_CREDENTIAL_DB_TIMEOUT`, `MQTT_CREDENTIAL_FINALIZE_TIMEOUT`, `MQTT_CREDENTIAL_OPERATION_TIMEOUT`, `MQTT_CREDENTIAL_RECOVERY_TIMEOUT`, `MQTT_DYNSEC_TIMEOUT`, `MQTT_DYNSEC_MAX_SNAPSHOT_BYTES`.

### 3.2 Cơ chế gắn kết tệp Native-Store và Quản lý Quyền
- **Mount thư mục chỉ đọc (Private Read-Only Directory Mount):** Tệp `dynamic-security.json` được Mosquitto cập nhật thông qua cơ chế ghi tệp tạm và đổi tên nguyên tử (`rename`). Do đó, Go backend cần mount **thư mục chứa** tệp này ở chế độ chỉ đọc (`read-only`), không mount một file đơn lẻ nhằm theo dõi được inode mới sau khi đổi tên.
- **Xác thực UID ghi tệp (Writer UID Verification):** Bộ đọc `DynSecSnapshotReader` kiểm tra quyền sở hữu tệp bằng syscall `openat/O_NOFOLLOW` và so sánh UID của tệp với `MQTT_DYNSEC_WRITER_UID` (chấp nhận `uint32` bao gồm cả `0` nếu được cấu hình rõ ràng). Bộ đọc từ chối nếu UID không khớp chính xác với tiến trình ghi thực tế. Nghiêm cấm cấp quyền ghi cho backend, không chạy lệnh `chmod 777` hoặc thay đổi quyền tùy tiện làm lộ dữ liệu native store.
- **Ràng buộc kiểm định phiên bản phát hành (Release Qualification Binding):** Kiểm định `NativeDisableQualification` là sự ràng buộc danh tính nội bộ giữa phiên bản ảnh Docker và cấu hình phát hành đã thử nghiệm; đây không phải là cơ chế remote attestation qua mạng với broker.
- **Quản lý đơn quyền và ranh giới khóa (Single Management Writer & Lock Boundaries):** Cần phân biệt rõ hai loại khóa cơ sở dữ liệu:
  1. *Khóa giao dịch tuần tự hóa thao tác (`pg_advisory_xact_lock`):* Được thực hiện trong `BeginSimplifiedOperation` dựa trên hash của `ActorUserID + ":" + IdempotencyKey` để tuần tự hóa các yêu cầu đồng thời có cùng actor và idempotency key trong phạm vi giao dịch PostgreSQL.
  2. *Khóa sở hữu phiên chuyên dụng (`credentialOwner` trong `cmd/server/credential_owner.go`):* Sử dụng PostgreSQL session advisory lock (`pg_try_advisory_lock` với khóa cố định `credentialOwnerLockKey`) trên một kết nối cơ sở dữ liệu chuyên biệt (dedicated connection) để đảm bảo chỉ một tiến trình Go backend đóng vai trò quản lý credential.
  Khóa sở hữu này chỉ loại trừ xung đột giữa các tiến trình backend cùng kết nối tới PostgreSQL; nó **không cung cấp cơ chế phân tán hàng rào (no distributed fencing)** và không đảm bảo trạng thái tĩnh lặng của broker (no broker quiescence). Việc chỉ có duy nhất một writer quản trị broker là tiền đề tin cậy vận hành (operator trust prerequisite), không được đảm bảo cho các tác nhân quản lý bên ngoài tùy ý bởi các khóa cơ sở dữ liệu. Nếu kết nối cơ sở dữ liệu bị ngắt, phiên PostgreSQL sẽ tự động giải phóng session lock; hệ thống **không tự động tái chiếm quyền (no automatic reacquisition)** mà chuyển sang trạng thái dừng xử lý fail-closed.

---

## 4. Quy Trình Vận Hành Rotate Và Revoke

### 4.1 Quy trình Rotate (Cấp đổi mật khẩu)
1. **Kiểm tra quyền:** Go backend xác minh quyền platform admin trong database PostgreSQL.
2. **Ghi nhận Intent:** Hệ thống ghi nhận trạng thái `PhaseIntent` vào nhật ký sự kiện (`gateway_mqtt_credential_events`) trước khi gửi lệnh tới broker.
3. **Sinh mật khẩu:** Mật khẩu ngẫu nhiên mới được tạo trực tiếp trong RAM thông qua CSPRNG, không lưu tạm vào file hay bộ đệm trung gian.
4. **Trình tự tương tác Broker:**
   - Gửi lệnh `disableClient` để ngắt kết nối và đóng phiên làm việc của target gateway.
   - Thực hiện readback xác nhận client đã ở trạng thái disabled.
   - Gửi lệnh `setClientPassword` để cập nhật mật khẩu mới trong DynSec plugin.
   - Gửi lệnh `enableClient` và thực hiện readback xác nhận client đã kích hoạt trở lại cùng với role/ACL không bị thay đổi.
5. **Kiểm tra đăng nhập:** Backend thực hiện một kết nối kiểm tra (`fresh TLS login`) bằng đúng mật khẩu mới qua kênh TLS đã xác thực CA.
6. **Finalize DB & Trả mật khẩu một lần:** Database chuyển trạng thái sang `PhaseFinalize` và `OperationSucceeded`. Mật khẩu plaintext chỉ được trả về **duy nhất một lần** trong phản hồi HTTP đầu tiên (`SecretResult`). Mật khẩu không bao giờ được lưu vào cơ sở dữ liệu hay log hệ thống.
7. **Cài đặt tại Gateway & Downtime:** Quản trị viên lấy mật khẩu một lần và nạp thủ công vào Gateway thông qua kênh tin cậy (kết nối USB hoặc SSH qua mạng nội bộ cô lập). Thiết bị sẽ chịu thời gian gián đoạn (downtime) từ lúc broker đổi mật khẩu cho đến khi hoàn tất nạp cấu hình mới trên Gateway.

### 4.2 Quy trình Revoke (Thu hồi quyền truy cập)
1. **Kiểm tra quyền & Intent:** Tương tự như Rotate, hệ thống xác thực quyền admin và tạo bản ghi intent nguyên tử trong database.
2. **Không sinh mật khẩu:** Luồng Revoke tuyệt đối không gọi bộ sinh mật khẩu, không đặt lại mật khẩu và không kích hoạt lại client.
3. **Thực thi thu hồi trên Broker:** Gửi lệnh `disableClient` tới broker để ngắt toàn bộ phiên kết nối hiện thời của Gateway target.
4. **Đọc lại trực tiếp trên Broker (Live Readback):** Thực hiện đọc lại trạng thái live của client (`GetClient`) để xác nhận client đã ở trạng thái disabled, đồng thời kiểm tra lại vai trò và ACL (`ensureRole(create=false)`) trên broker đang chạy trước khi chuyển sang bước kiểm tra tệp lưu trữ.
5. **Kiểm tra tệp lưu trữ:** `DynSecSnapshotReader.VerifyRevoked` đọc tệp native store (`dynamic-security.json`), xác nhận client đã bị vô hiệu hóa trên tệp lưu trữ và cấu trúc role/ACL được giữ nguyên vẹn.
6. **Finalize DB trước khi trả phản hồi:** Database chuyển trạng thái sang `PhaseFinalize` và `OperationSucceeded`, cập nhật bản ghi credential sang `CredentialRevoked` và giữ nguyên `credential_version`. Chỉ sau khi giao dịch database hoàn tất thành công (`ConditionalFinalize`), hệ thống mới trả về kết quả metadata (`MutationResult`), tuyệt đối không chứa trường mật khẩu.

### 4.3 Khóa Idempotency và Xử Lý Bất Định
- Mọi yêu cầu Rotate và Revoke bắt buộc phải có `idempotency_key` (chuỗi UUID).
- **Phạm vi Idempotency theo Actor (Actor Scoping):** Khóa idempotency được định danh và tra cứu theo từng actor (`ActorUserID`). Trong `BeginSimplifiedOperation`, hệ thống kiểm tra sự tồn tại của sự kiện trước đó qua `WHERE actor_user_id=$1 AND idempotency_key=$2 AND NOT legacy_projection`. Nếu một actor khác sử dụng trùng giá trị UUID này cho thao tác của họ, điều đó **không tự động cấu thành lỗi CodeIdempotencyConflict** mà được xử lý như một thao tác độc lập của actor đó (sau khi đã vượt qua các bước xác thực quyền admin).
- **Lệch ràng buộc key cùng Actor:** Lỗi `CodeIdempotencyConflict` xảy ra khi **cùng một actor** gửi lại một `idempotency_key` đã tồn tại nhưng có sự xung đột về ngữ cảnh thao tác: khác `gateway_id` hoặc khác `action` so với bản ghi ban đầu (`p.GatewayID != parent || p.Action != req.Input.Action`).
- **Replay hợp lệ cùng Actor/Key:** Nếu cùng actor gửi lại đúng `idempotency_key`, cùng `gateway_id` và cùng `action`, hệ thống chỉ trả về metadata hiện tại (`Metadata`), tuyệt đối **không thực thi lại lệnh trên broker** và **không cấp lại mật khẩu**.
- **Trạng thái bất định khi Finalize:** Nếu lệnh trên broker đã thực hiện xong nhưng thao tác commit vào database bị gián đoạn hoặc quá thời gian chờ, hệ thống đánh dấu tiến trình bị đầu độc (`poison`), trả về lỗi `CodeFinalizationPending` và **tuyệt đối không trả mật khẩu ra ngoài**, kể cả khi database sau đó xác nhận giao dịch đã commit.

---

## 5. Xử Lý Sự Cố Và Giới Hạn Khôi Phục

### 5.1 Nguyên Tắc Fail-Closed Tuyệt Đối
- **Không tự động thử lại (No Automatic Replay):** Khi tương tác với broker thất bại hoặc trả kết quả không chắc chắn, hệ thống dừng xử lý ngay lập tức.
- **Không tự động bù trừ hay kích hoạt lại (No Compensation / Speculative Enable):** Hệ thống không tự ý phát lệnh đảo ngược hoặc tự tiện kích hoạt lại client khi gặp lỗi giữa chừng.
- **Không khởi động lại Broker tự động (No Auto-Restart):** Backend không có quyền và không tự động khởi động lại container Mosquitto để che giấu các lỗi đồng bộ.

### 5.2 Xử Lý Reset: Trạng Thái Chỉ Từ Chối (Deny-Only)
- Lệnh Reset credential trong cấu hình máy chủ hiện tại được gán kiểu runtime `denyOnlyResetPrerequisite` (trong `cmd/server/credential.go`), thỏa mãn giao diện `ResetPrerequisite` bằng cách fail-closed trả về `CodeServiceUnavailable`.
- Mọi yêu cầu gọi tới luồng Reset sẽ bị từ chối và chặn lại.
- **Không có lệnh phục hồi tự động:** Không tự ý chế tạo câu lệnh khôi phục khẩn cấp. Khi có sự cố bất định hoặc phát hiện dấu hiệu đầu độc tiến trình, hệ thống yêu cầu dừng vận hành và chuyển giao cho quản trị viên chẩn đoán trực tiếp. Quy trình reset qua cấp quyền file thuộc danh mục backlog riêng.

---

## 6. Giới Hạn Kỹ Thuật (Technical Limitations)

Hệ thống được thiết kế và nghiệm thu trong khuôn khổ đồ án môn học, với các giới hạn an toàn sau (chi tiết xem tại [docs/dynsec/limitations-and-backlog.md](dynsec/limitations-and-backlog.md)):

1. **Rủi ro lưu trữ mật khẩu sau Rotate:** Mốc Task 2.8a tối giản đã hoãn lại cơ chế kiểm tra nội dung mật khẩu ghi trên đĩa (ROT2 exact persisted-new-password verifier). Mật khẩu mới được xác nhận hoạt động trên RAM của broker đang chạy thông qua fresh TLS login. Nếu Mosquitto gặp lỗi khi flush bộ nhớ ra tệp `dynamic-security.json`, việc khởi động lại broker sau đó có thể nạp lại mật khẩu cũ.
2. **Không đảm bảo an toàn mất điện đột ngột (No Power-Loss / fsync Guarantee):** Việc đọc kiểm tra snapshot file chỉ xác nhận tệp đã được ghi nhận ở tầng hệ điều hành, không chứng minh việc gọi lệnh `fsync` xuống phần cứng đĩa cứng vật lý.
3. **Cơ chế Reset và Quiescence ở trạng thái Deny-Only:** Endpoint reset trong cấu hình runtime hiện tại trả về mã 503 `CodeServiceUnavailable`, chưa có quy trình phục hồi tự động khi gặp sự cố bất định hoặc mất secret.
4. **Thiếu hàng rào phân tán (No Distributed Fencing):** PostgreSQL session advisory lock loại trừ cạnh tranh giữa các tiến trình Go backend, nhưng không loại trừ được các tác nhân bên ngoài can thiệp vào broker nếu có tài khoản quản trị.
5. **Ranh giới tương đương bản build (Build Parity Caveat):** Các bài diễn tập Nginx và Supabase Auth thành công sử dụng nhị phân tĩnh host biên dịch rồi bind-mount vào container (`build_scope: offline host static binary mounted read-only`), chưa phải là bản build multi-stage từ `src/Dockerfile` (`target: backend`) trong `docker-compose.yml` gốc.
6. **Phạm vi cô lập lưu lượng còn hẹp (M04):** Bằng chứng cô lập chỉ chứng minh phiên kết nối của Observer gateway sống sót và publish QoS 1 nhận PUBACK; chưa kiểm chứng việc luân chuyển dữ liệu telemetry tới subscriber hay TimescaleDB.
7. **Cổng kiểm duyệt độc lập (Independent Reviewer Gate) chưa được miễn trừ:** Nhãn hoàn thành H1 và đánh giá độc lập theo kế hoạch ban đầu vẫn đang ở trạng thái chờ duyệt.
8. **Chưa đạt chuẩn Production-Ready:** Toàn bộ Task 2.8 và quy trình triển khai phân tán chưa hoàn thành. Không sử dụng tài liệu này như một chứng chỉ sẵn sàng vận hành thương mại.

---

## 7. Các Bước Hành Động Tiếp Theo (Next Actions)

Để tiếp tục hoàn thiện đồ án theo đúng phạm vi quản lý:

1. **Rà soát tài liệu DynSec chuẩn hóa:** Người phụ trách rà soát báo cáo này cùng bộ tài liệu hoàn chỉnh tại [docs/dynsec/INDEX.md](dynsec/INDEX.md).
2. **Thực hiện các kiểm tra tĩnh tập trung:** Chạy kiểm tra định dạng và tính toàn vẹn cú pháp của tài liệu bằng `git diff --check`. Không tự động chạy lại các fixture native tốn kém tài nguyên.
3. **Ủy quyền Commit độc lập:** Báo cáo lại cho người giám sát để tiến hành commit riêng các thay đổi nguồn và tài liệu khi được phê duyệt.
4. **Lập kế hoạch giải quyết Backlog:** Ưu tiên xử lý các hạng mục P1 (ROT2 exact persisted password verifier) và P4 (kiểm tra build parity với Dockerfile gốc).
