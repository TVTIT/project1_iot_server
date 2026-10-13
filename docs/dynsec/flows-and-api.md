# Quy Trình Nghiệp Vụ Và Hợp Đồng HTTP API DynSec

Tài liệu này mô tả chi tiết các luồng vận hành (Operation Flows) và Hợp đồng giao tiếp HTTP API (API Contract) cho mô-đun quản lý Dynamic Security (DynSec) trên Eclipse Mosquitto và Go backend.

Mọi nội dung đều được đối chiếu trực tiếp từ mã nguồn triển khai tại:
- `src/internal/mqttcredential/service.go`
- `src/internal/mqttcredential/dynsec_simplified_adapter.go`
- `src/internal/mqttcredential/postgres_simplified_operation.go`
- `src/internal/httpserver/admin_mqtt_credential_handlers.go`
- `src/internal/httpserver/admin_mqtt_credential_request.go`
- `src/cmd/server/credential.go`
- `src/cmd/server/credential_owner.go`

---

## 1. Chi Tiết Các Luồng Vận Hành (Operation Flows)

### 1.1 Luồng Provision (Cấp phát thông tin xác thực ban đầu)

Áp dụng cho Gateway chưa từng có credential hoặc đã ở trạng thái thu hồi (`revoked`).

```text
[HTTP POST /v1/admin/gateways/{id}/mqtt-credential]
       │
       ▼
 1. Xác thực HTTP: Kiểm tra JWT role "authenticated", kiểm tra cờ API và rào chắn StartupReady
       │
       ▼
 2. Kiểm tra thẩm quyền Admin: Tra cứu CSDL PostgreSQL bảng platform_admins (trả về 403 nếu không phải admin)
       │
       ▼
 3. Ghi nhận Intent bền vững:
    - Chiếm khóa giao dịch pg_advisory_xact_lock(hash(actor_id:idempotency_key))
    - Kiểm tra replay: Nếu đã tồn tại sự kiện cùng key và cùng actor -> Trả Replay (xem Mục 1.4)
    - Tạo OperationID mới (UUIDv4/v7)
    - INSERT gateway_mqtt_credential_events với phase='intent', status='pending', action='provision'
    - Cập nhật gateway_mqtt_credentials sang status='provisioning'
       │
       ▼
 4. Sinh mật khẩu trong RAM:
    - CSPRNG sinh ngẫu nhiên chuỗi mật khẩu trong bộ nhớ RAM (tối thiểu 128 bit entropy)
    - Không ghi mật khẩu ra file tạm, không lưu vào cơ sở dữ liệu
       │
       ▼
 5. Tương tác Broker (Qua SimplifiedDynSecExecutor):
    - Kiểm tra quyền sở hữu khóa phiên pg_try_advisory_lock(0x494f545f43524544) trước khi gọi broker
    - Khởi tạo client kết nối quản trị mới (NewClient) qua TLS
    - ensureRole(create=true): Tạo role "gateway_<id>" nếu chưa có, gán quyền publish/subscribe theo namespace
    - createClient: Tạo principal cho gateway trên broker
    - disableClient: Tạm thời vô hiệu hóa client
    - setClientPassword: Nạp mật khẩu mới vào plugin DynSec
    - addClientRole: Gán role vào client
    - enableClient: Kích hoạt client hoạt động trở lại
       │
       ▼
 6. Đọc lại và Kiểm tra thực tế (Readback & Live Probe):
    - Đọc lại GetClient: Xác nhận client đang ở trạng thái enabled=true và gán đúng role
    - Đọc lại ensureRole: Xác nhận ACL không bị sai lệch (RAMApplied=true)
    - Probe kết nối mới (Fresh TLS Login): Thực hiện một kết nối MQTT TLS mới bằng đúng mật khẩu vừa tạo
      (FreshPositiveVerified=true)
    - Kiểm tra lại quyền sở hữu khóa phiên sau khi tương tác broker
       │
       ▼
 7. Finalize CSDL & Phân phối mật khẩu:
    - Giao dịch CSDL ConditionalFinalize:
      Cập nhật event sang status='succeeded', phase='finalize'
      Cập nhật gateway_mqtt_credentials sang status='active', tăng credential_version
    - Đọc lại xác nhận trạng thái (ResolveCommitAmbiguity & GetMetadata)
    - Trả về mã HTTP 201 Created cùng đối tượng SecretResult chứa mật khẩu plaintext duy nhất một lần
```

---

### 1.2 Luồng Rotate (Xoay vòng mật khẩu theo trình tự ROT1)

Áp dụng cho Gateway đang hoạt động bình thường (`active`). Bắt buộc tuân thủ trình tự có trật tự ROT1 (Ordered Sequence) để đảm bảo phiên kết nối cũ bị đóng hoàn toàn trước khi mật khẩu mới được nạp vào.

```text
[HTTP POST /v1/admin/gateways/{id}/mqtt-credential/rotate]
       │
       ▼
 1. Kiểm tra Admin & Ghi nhận Intent:
    - Xác minh quyền trong platform_admins
    - Kiểm tra trạng thái hiện tại phải là 'active'
    - Ghi nhận intent vào CSDL với version mới tăng đơn điệu (credential_version = previous + 1)
       │
       ▼
 2. Sinh mật khẩu mới: CSPRNG sinh mật khẩu mới trực tiếp trong RAM
       │
       ▼
 3. Trình tự tương tác Broker ROT1 nghiêm ngặt:
    Bước 3.1: disableClient(gateway_id)
              -> Mosquitto ngắt kết nối socket TLS hiện thời của Gateway (target nhận tín hiệu EOF)
    Bước 3.2: Đọc lại trạng thái Disabled (Disabled Readback)
              -> Gọi GetClient xác nhận client đã disabled=true và role không đổi
    Bước 3.3: Đọc lại quyền hạn Role/ACL (Exact ACL Readback)
              -> Gọi ensureRole(create=false) kiểm tra toàn vẹn ACL
    Bước 3.4: setClientPassword(gateway_id, new_password)
              -> Chỉ được phép gọi sau khi CẢ HAI bước đọc lại trên đã thành công
    Bước 3.5: enableClient(gateway_id)
              -> Kích hoạt client trở lại trên broker
    Bước 3.6: Đọc lại trạng thái Enabled (Enabled Readback)
              -> Gọi GetClient xác nhận disabled=false (RAMApplied=true)
       │
       ▼
 4. Thẩm định kết nối thực tế (Fresh TLS Login):
    - Thực hiện kết nối mới qua TLS bằng đúng mật khẩu mới vừa nạp
    - Kết nối thành công chứng minh mật khẩu mới đang hoạt động trong RAM của broker
      (FreshPositiveVerified=true)
       │
       ▼
 5. Finalize CSDL & Trả mật khẩu một lần:
    - Cập nhật trạng thái CSDL sang 'active', version mới được lưu vết
    - Phản hồi HTTP 200 OK kèm đối tượng SecretResult chứa mật khẩu mới
    - Mật khẩu chỉ xuất hiện một lần duy nhất; Gateway chịu thời gian downtime cho tới khi
      Platform Operator nạp mật khẩu mới vào thiết bị qua SSH hoặc USB nội bộ
```

---

### 1.3 Luồng Revoke (Thu hồi quyền truy cập)

Áp dụng để vô hiệu hóa khẩn cấp một Gateway đang hoạt động (`active`). Luồng Revoke **tuyệt đối không sinh mật khẩu, không đặt lại mật khẩu, không kích hoạt lại client và không trả về trường mật khẩu**.

```text
[HTTP DELETE /v1/admin/gateways/{id}/mqtt-credential]
       │
       ▼
 1. Kiểm tra Admin & Ghi nhận Intent:
    - Xác minh quyền trong platform_admins
    - Ghi nhận intent action='revoke' vào CSDL (giữ nguyên credential_version hiện tại)
       │
       ▼
 2. Kiểm tra trạng thái cơ sở (Baseline Verification):
    - Đọc trạng thái live trên broker: Gateway phải đang tồn tại và ở trạng thái active
       │
       ▼
 3. Thực thi vô hiệu hóa trên Broker:
    - Gửi lệnh disableClient(gateway_id)
    - Mọi phiên kết nối của Gateway ngay lập tức bị ngắt kết nối TLS (nhận EOF)
    - Mọi nỗ lực kết nối mới bằng mật khẩu cũ bị từ chối kết nối (TLS Auth Denial)
       │
       ▼
 4. Đọc lại trực tiếp trên Broker (Live Readback):
    - GetClient xác nhận client đã chuyển sang disabled=true
    - ensureRole(create=false) xác nhận cấu trúc role/ACL vẫn nguyên vẹn
    - Ghi nhận RAMApplied=true và NativeDisabledObserved=true
       │
       ▼
 5. Quan sát tệp Native Store bảo vệ (Protected Snapshot Observation):
    - DynSecSnapshotReader.VerifyRevoked mở tệp dynamic-security.json qua mount thư mục chỉ đọc
    - Kiểm tra quyền sở hữu UID (phải khớp MQTT_DYNSEC_WRITER_UID), kích thước tệp và chế độ bảo mật
    - Phân tích cú pháp tệp JSON lưu trữ:
      * Xác nhận client tồn tại và trường disabled mang giá trị true
      * Xác nhận role và ACL được lưu trữ chính xác trên đĩa
    - Ghi nhận NativePersistedRoleACLObserved=true
       │
       ▼
 6. Finalize CSDL & Phản hồi Metadata:
    - ConditionalFinalize cập nhật gateway_mqtt_credentials sang status='revoked', revoked_at=now()
    - Phản hồi HTTP 200 OK mang đối tượng MutationResult (metadata thuần túy, secret_returned=false,
      tuyệt đối không chứa trường password)
```

---

### 1.4 Cơ Chế Replay Và Phạm Vi Idempotency Key (Actor Scoping)

Mọi thao tác thay đổi trạng thái (`POST`, `POST /rotate`, `DELETE`) bắt buộc phải truyền header `Idempotency-Key` dưới dạng chuỗi UUID hợp lệ:

1. **Phạm vi khóa theo từng Actor (Actor-Scoped):**
   - Khóa idempotency được gắn kết với định danh của người thực hiện (`actor_user_id`).
   - Việc hai actor khác nhau tình cờ gửi trùng một chuỗi UUID không tự động cấu thành xung đột; hệ thống coi đó là hai thao tác độc lập sau khi đã xác thực quyền Platform Admin của từng người.
2. **Xung đột khóa cùng Actor (`CodeIdempotencyConflict` - HTTP 409):**
   - Nếu **cùng một actor** gửi lại một `Idempotency-Key` đã tồn tại trong nhật ký sự kiện nhưng có sự sai lệch về ngữ cảnh: khác `gateway_id` hoặc khác `action` (`p.GatewayID != target || p.Action != req.Action`).
   - Server từ chối ngay lập tức với mã lỗi HTTP `409 Conflict` (`code: "idempotency_conflict"`).
3. **Replay hợp lệ (Cùng Actor, Cùng Key, Cùng Gateway, Cùng Action):**
   - Server **không thực thi lại bất kỳ lệnh nào trên broker**.
   - Server **tuyệt đối không cấp lại mật khẩu** (`secret_returned: false`, trường `password` bị loại bỏ).
   - Server tra cứu trạng thái CSDL hiện tại và phản hồi:
     - Nếu thao tác trước đó đã thành công: Trả về HTTP 200 (hoặc 201 cho provision) với đối tượng `MutationResult` chứa metadata hiện tại và `replayed_status: "succeeded"`.
     - Nếu thao tác trước đó thất bại: Ánh xạ lỗi và trả về mã lỗi thích hợp (HTTP 500 hoặc mã nghiệp vụ gốc), không bao giờ báo 200/201 thành công giả tạo.
     - Nếu là replay của một thao tác Revoke đã hoàn tất từ trước: Hệ thống gọi `CheckRevoked` để đọc lại trạng thái live của client trên broker và tệp native store nhằm xác nhận client vẫn đang thực sự bị vô hiệu hóa trước khi trả metadata.

---

### 1.5 Xử Lý Bất Định Sau Đột Biến (Mutation Uncertainty) Và Đầu Độc Tiến Trình (Poisoning)

Trong trường hợp xảy ra sự cố giữa các bước thực thi:

```text
[Bắt đầu tương tác Broker]
       │
       ▼ Gửi lệnh thay đổi đầu tiên (disableClient / createClient...)
  Đánh dấu Mutated = true
       │
       ├─────────────────────────────────────────┐
       ▼ (Gặp lỗi mạng / timeout / broker crash)   ▼ (Giao dịch hoàn tất thành công)
 [Xử lý Bất định / Thất bại đột biến]          [Finalize CSDL]
       │                                                 │
       ├─► Đánh dấu Outcome = ExecutionRecoveryRequired  ├─► Gặp lỗi commit / timeout / mất ACK
       ├─► Ghi nhận status='recovery_needed' vào CSDL    │         │
       ├─► Gọi s.poison(): ĐẦU ĐỘC TIẾN TRÌNH GO         │         ▼
       │   (Khóa s.poisoned = true)                      │   [Xử lý Bất định Commit (M06)]
       ├─► Trả về lỗi CodeRecoveryRequired (HTTP 503)    │   ├─► ResolveCommitAmbiguity đọc lại
       └─► TUYỆT ĐỐI KHÔNG TRẢ MẬT KHẨU RA NGOÀI         │   ├─► Nếu sai lệch / mất kết nối:
                                                         │   │   Gọi s.poison(), trả CodeFinalizationPending
                                                         │   │   TUYỆT ĐỐI KHÔNG TRẢ MẬT KHẨU
                                                         │   └─► Nếu khớp chính xác: Phản hồi secret
                                                         ▼
                                                [Phân phối Secret một lần duy nhất]
```

**Nguyên tắc xử lý:**
- Khi tiến trình Go bị đánh dấu `poisoned`: Mọi yêu cầu quản lý credential tiếp theo đều bị chặn lại với mã lỗi `CodeRuntimeBusy` (HTTP 503) nhằm ngăn chặn việc tiếp tục ghi đè trạng thái sai lệch lên broker.
- Hệ thống **không tự động gửi lệnh đảo ngược (no automatic rollback)** và **không tự động bật lại client (no speculative enable)** vì không thể xác định broker đã thực sự nhận và áp dụng những lệnh nào.
- Yêu cầu Platform Operator phải can thiệp giám sát trực tiếp để chẩn đoán.

---

### 1.6 Rà Soát Khởi Động (Startup Classifier) Và Mất Sở Hữu Khóa Phiên (Owner Loss)

Khi Go backend khởi động lại hoặc gặp sự cố kết nối CSDL:

1. **Rà soát phân loại trạng thái dở dang (`SimplifiedStartupClassifier`):**
   - Tiến trình khởi động quét bảng `gateway_mqtt_credential_events` tìm các bản ghi đang ở trạng thái `pending` thuộc các phase dở dang (`intent`, `dynsec_mutation`, `snapshot_readback`).
   - Các bản ghi này được phân loại và cập nhật chuyển sang trạng thái `recovery_needed`.
   - Nếu quá trình quét bị lỗi kết nối hoặc vượt quá giới hạn trang (`MaxPages`): Hệ thống đánh dấu trạng thái khởi động không hoàn tất (`readiness.ready = false`), server từ chối phục vụ API credential.
2. **Kiểm tra đăng nhập ban đầu của Broker:**
   - Backend thực hiện kiểm tra đăng nhập mới bằng tài khoản `Manager` và tài khoản `Backend` qua TLS.
   - Nếu một trong hai tài khoản đăng nhập thất bại: Rào chắn khởi động không mở (`Ready() == false`).
3. **Mất quyền sở hữu khóa phiên CSDL (`Owner Loss`):**
   - Hệ thống duy trì một kết nối PostgreSQL chuyên biệt (hijacked connection từ pool) để giữ khóa phiên `pg_try_advisory_lock(0x494f545f43524544)`.
   - Khóa này được kiểm tra tự động trước và sau mỗi thao tác trên broker thông qua `ownershipGatedExecutor`.
   - Nếu kết nối CSDL bị đứt:
     * CSDL PostgreSQL sẽ tự động giải phóng session lock.
     * `CheckOwnership` phát hiện mất khóa và ngay lập tức kích hoạt `readiness.markOwnerLost()`.
     * Cờ sẵn sàng `Ready()` chuyển vĩnh viễn sang `false` (fail-closed).
     * Hệ thống **tuyệt đối không tự động chiếm lại khóa (no automatic reacquisition)** để phòng tránh tình huống split-brain với một tiến trình backend khác.

---

## 2. Hợp Đồng HTTP API Chi Tiết (HTTP Endpoints Contract)

Tất cả các endpoint quản trị đều nằm dưới tiền tố `/v1/admin/gateways/{gateway_id}/mqtt-credential`.

### 2.1 Bảng tổng hợp các Route

| Phương thức HTTP | Đường dẫn Route | Mục đích | Headers Bắt buộc | Request Body | Mã trạng thái thành công | DTO Phản hồi |
|---|---|---|---|---|:---:|---|
| **GET** | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Đọc metadata kiểm toán hiện tại | `Authorization: Bearer <JWT>` | **BẮT BUỘC RỖNG** | **200 OK** | `Metadata` |
| **POST** | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Cấp phát credential ban đầu | `Authorization: Bearer <JWT>`<br>`Idempotency-Key: <UUID>` | **BẮT BUỘC RỖNG** | **201 Created** | `SecretResult` (First)<br>`MutationResult` (Replay) |
| **POST** | `/v1/admin/gateways/{gateway_id}/mqtt-credential/rotate` | Xoay vòng mật khẩu (ROT1) | `Authorization: Bearer <JWT>`<br>`Idempotency-Key: <UUID>` | **BẮT BUỘC RỖNG** | **200 OK** | `SecretResult` (First)<br>`MutationResult` (Replay) |
| **DELETE** | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | Thu hồi credential (Revoke) | `Authorization: Bearer <JWT>`<br>`Idempotency-Key: <UUID>` | **BẮT BUỘC RỖNG** | **200 OK** | `MutationResult` |
| **POST** | `/v1/admin/gateways/{gateway_id}/mqtt-credential/reset` | Đặt lại khẩn cấp có giám sát | `Authorization: Bearer <JWT>`<br>`Idempotency-Key: <UUID>`<br>`Content-Type: application/json` (tùy chọn `charset=utf-8`) | `{"reason": "..."}` | **LUÔN BỊ TỪ CHỐI TẠI RUNTIME PREREQUISITE** | **503 Service Unavailable** (`service_unavailable`) nếu yêu cầu hợp lệ; có thể nhận **401**, **403**, **400**, **413**, **415** trước khi tới runtime prerequisite |

---

### 2.2 Quy Chuẩn Hợp Đồng Body Rỗng Khắt Khe (Strict Empty Body Contract)

Hàm phân tích cú pháp `parseCredentialRequest` tại `src/internal/httpserver/admin_mqtt_credential_request.go` áp dụng quy tắc kiểm tra nghiêm ngặt:

```go
data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
if err != nil || len(data) != 0 || gateway.ValidateGatewayID(id) != nil || r.URL.RawQuery != "" || r.URL.ForceQuery {
    return uuid.Nil, http.StatusBadRequest
}
```

- **Độ dài body bắt buộc bằng 0 (`len(data) == 0`):** Đối với các endpoint `GET`, `POST`, `POST /rotate`, và `DELETE`, client **tuyệt đối không được gửi kèm bất kỳ nội dung body nào**. Nếu gửi kèm body (kể cả `{}` rỗng), server sẽ trả về mã lỗi HTTP `400 Bad Request` (`code: "invalid_request"`).
- **Cấm URL Query Parameters:** Mọi URL chứa chuỗi truy vấn (ví dụ `?foo=bar` hoặc kể cả ký tự `?` rỗng) đều bị từ chối với HTTP `400 Bad Request`.
- **Cấm định dạng ID không hợp lệ:** `gateway_id` phải khớp regex chuẩn `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` và không được là giá trị dành riêng `backend_service`.
- **Header Header Cache-Control:** Middleware tự động chèn `Cache-Control: no-store` và `Pragma: no-cache` cho toàn bộ các phản hồi của route credential.

---

### 2.3 Endpoint Reset: Cơ Chế Chỉ Từ Chối Tại Runtime (Runtime Deny-Only)

Mặc dù router đã đăng ký đường dẫn `POST /v1/admin/gateways/:gateway_id/mqtt-credential/reset` và bộ phân tích cú pháp `parseCredentialResetRequest` chấp nhận payload JSON dạng `{"reason": "recovery_after_unknown"}` hoặc `{"reason": "lost_secret_recovery"}`:

- **Thứ tự kiểm tra trước khi tới Runtime Prerequisite:**
  1. **Xác thực JWT (401 Unauthorized):** Thiếu header `Authorization`, JWT sai cú pháp, hết hạn, hoặc role không phải `authenticated` sẽ trả về `401` ngay tại middleware auth.
  2. **Ủy quyền Platform Admin (403 Forbidden):** Người dùng có JWT hợp lệ nhưng không nằm trong bảng `platform_admins` sẽ bị chặn `403` trước khi xử lý nghiệp vụ.
  3. **Kiểm tra trạng thái Service/Runtime (503):** Nếu cờ `MQTT_CREDENTIAL_API_ENABLED=false` hoặc hệ thống startup readiness chưa sẵn sàng, trả về mã lỗi 503 tương ứng (`credential_runtime_disabled` hoặc `service_unavailable`).
  4. **Phân tích cú pháp Request (400, 413, 415):**
     - Parser chấp nhận `Content-Type: application/json` với tham số charset tùy chọn (nếu có phải là `charset=utf-8`). Nếu header Content-Type không hợp lệ sẽ trả về `415 Unsupported Media Type` (`code: "invalid_request"`).
     - Nếu body vượt quá `ADMIN_MAX_BODY_BYTES`, trả về `413 Payload Too Large` (`code: "request_too_large"`).
     - Nếu `Idempotency-Key` thiếu/sai UUID, URL chứa query string, `gateway_id` sai regex, JSON rỗng, JSON malformed, hoặc trường `reason` khác hai giá trị cho phép (`recovery_after_unknown`, `lost_secret_recovery`), parser trả về `400 Bad Request` (`code: "invalid_request"`).
- **Cấu hình runtime hiện tại gán kiểu:** `denyOnlyResetPrerequisite` (trong `src/cmd/server/credential.go`).
- **Hành vi thực tế đối với yêu cầu hợp lệ được chấp nhận (Valid Authorized Admissible Request):** Khi request vượt qua toàn bộ các bước xác thực, phân quyền và kiểm tra cú pháp trên, hàm `SupervisedQuiescencePrerequisite` trả về trực tiếp lỗi `CodeServiceUnavailable`.
- **Kết quả trả về:** Một yêu cầu hợp lệ, có thẩm quyền và đúng cú pháp vẫn **luôn fail-closed và nhận về phản hồi HTTP 503 Service Unavailable** (`code: "service_unavailable"`). Reset không bao giờ trả về 503 cho toàn bộ mọi request mà không xét thứ tự 401/403/400/415.
- **Quy tắc vận hành:** **Tuyệt đối không mô tả endpoint này như một công cụ khôi phục khả dụng.** Tính năng reset tự động chưa được nghiệm thu cho môi trường vận hành thực tế.

---

### 2.4 Bảng Mã Lỗi Nghiệp Vụ Chuẩn (Error Codes Mapping)

Toàn bộ các phản hồi lỗi từ mô-đun credential tuân thủ cấu trúc phong bì chuẩn `{"error": {"code": "...", "message": "...", "request_id": "..."}}`:

| Mã Lỗi Nghiệp Vụ (`code`) | Mã HTTP Tương Ứng | Nguyên Nhân Phát Sinh |
|---|:---:|---|
| `unauthorized` | **401 Unauthorized** | Thiếu header `Authorization`, JWT không hợp lệ, hết hạn, hoặc role không phải `authenticated`. |
| `forbidden` | **403 Forbidden** | Người dùng đã đăng nhập nhưng không có bản ghi trong bảng `public.platform_admins`. |
| `invalid_request` | **400 Bad Request** | Body không rỗng (trên các route mutation khác), URL chứa query string, `gateway_id` sai định dạng, thiếu/sai `Idempotency-Key`, hoặc payload reset không đúng định dạng. |
| `request_too_large` | **413 Payload Too Large** | Body vượt quá giới hạn `ADMIN_MAX_BODY_BYTES`. |
| `invalid_request` | **415 Unsupported Media Type** | Header `Content-Type` không phải `application/json` (với tùy chọn `charset=utf-8`) trên endpoint reset. |
| `not_found` | **404 Not Found** | Không tìm thấy Gateway tương ứng trong hệ thống. |
| `credential_conflict` | **409 Conflict** | Xung đột trạng thái nghiệp vụ (ví dụ: gọi provision khi credential đang ở trạng thái `active`). |
| `idempotency_conflict` | **409 Conflict** | Cùng một actor gửi lại `Idempotency-Key` cũ nhưng thay đổi `gateway_id` hoặc `action`. |
| `operation_in_progress` | **409 Conflict** | Yêu cầu trùng lặp tới một thao tác đang trong quá trình thực thi trên hệ thống. |
| `credential_runtime_disabled`| **503 Service Unavailable** | Cờ `MQTT_CREDENTIAL_API_ENABLED` đang đặt là `false`. |
| `service_unavailable` | **503 Service Unavailable** | Rào chắn khởi động chưa sẵn sàng (`Ready() == false`), mất khóa sở hữu phiên, hoặc gọi vào endpoint reset (khi yêu cầu hợp lệ). |
| `credential_recovery_required`| **503 Service Unavailable** | Thao tác trên broker gặp lỗi sau khi đã gửi lệnh đột biến; hệ thống chuyển sang trạng thái cần phục hồi có giám sát. |
| `finalization_pending` | **503 Service Unavailable** | Lệnh broker đã thực thi nhưng quá trình commit CSDL bị gián đoạn/mất kết nối; mật khẩu bị giữ lại an toàn. |
| `internal_error` | **500 Internal Server Error** | Lỗi không mong muốn trong quá trình xử lý nội bộ của server. |

---

## 3. Các Ví Dụ Lệnh `curl` Minh Họa An Toàn

Các ví dụ dưới đây tuân thủ nghiêm ngặt các quy tắc an toàn bảo mật và ranh giới vận hành:
- **Không truyền token trên command-line argv:** Sử dụng tệp header được bảo vệ (`mode 0600`) hoặc luồng cấu hình curl từ tệp/stdin (`-K -` / `-H @file`). Tuyệt đối không sử dụng `-H "Authorization: Bearer ${ADMIN_JWT}"` vì cơ chế thay thế biến môi trường của shell vẫn mở rộng giá trị token vào danh sách đối số tiến trình (`process argv`), có thể bị đọc trộm qua lệnh `ps aux` hoặc công cụ giám sát tiến trình.
- **Không lưu token vào lịch sử shell:** Token được trích xuất trực tiếp từ client xác thực đáng tin cậy hoặc chuyển giao qua tệp được bảo vệ; không thực hiện gán token dạng `export ADMIN_JWT=...` trực tiếp trên terminal.
- **Lưu giữ phản hồi chứa mật khẩu một lần (Secret Response) an toàn:** Với các thao tác cấp mới hoặc xoay vòng trả về mật khẩu một lần, phản hồi curl được lưu trực tiếp vào tệp kết quả với cờ tạo độc quyền (`exclusive creation`) quyền riêng tư `0600` nằm trong thư mục bảo vệ `0700` thuộc sở hữu của operator. Tuyệt đối không in mật khẩu ra `stdout`/terminal, không dùng `cat`, `jq` in secret vào màn hình hay nhật ký. Sau khi nạp mật khẩu an toàn vào Gateway qua kênh nội bộ (USB/SSH cô lập), operator thực hiện dọn dẹp cục bộ tệp kết quả (không xóa lan rộng).
- **Điều kiện tiên quyết có thẩm quyền (Explicit Authorization Prerequisite):** Mọi lệnh thay đổi (POST, POST /rotate, DELETE) chỉ được thực thi khi đã có ủy quyền thao tác rõ ràng (explicit mutation authorization).
- **Cấu hình động qua biến môi trường:** Sử dụng `${API_BASE}` và `${TARGET_GATEWAY}` được inject từ môi trường thay vì gán cứng các định danh hay domain triển khai cụ thể.
- **Hợp đồng Body hoàn toàn rỗng:** Đối với các route GET, POST provision, POST rotate, DELETE revoke, tuyệt đối không truyền body (sử dụng `--data ""` hoặc không truyền `-d`).

---

### 3.1 Thiết lập thư mục bảo vệ và cấu hình xác thực

```bash
# 1. Thiết lập thư mục làm việc bảo vệ (0700) thuộc sở hữu của operator
OP_WORK_DIR="${HOME}/.iot_op_$(id -u)"
mkdir -p -m 0700 "${OP_WORK_DIR}"

# 2. Tạo tệp header xác thực quyền riêng tư (0600)
# Token được ghi an toàn từ client xác thực (không gán export token vào shell history)
AUTH_HEADER_FILE="${OP_WORK_DIR}/admin_auth.header"
(umask 077 && touch "${AUTH_HEADER_FILE}")
# Ví dụ nạp header từ client đáng tin cậy:
# trusted-auth-cli print-auth-header > "${AUTH_HEADER_FILE}"
# Đảm bảo nội dung tệp có dạng: Authorization: Bearer <valid_jwt>

# 3. Thiết lập biến môi trường cấu hình đích (inject từ môi trường)
# API_BASE: Địa chỉ Go Backend (ví dụ: http://127.0.0.1:8080)
# TARGET_GATEWAY: Định danh Gateway đích (ví dụ: gw-station-alpha)
: "${API_BASE:?API_BASE must be set in environment}"
: "${TARGET_GATEWAY:?TARGET_GATEWAY must be set in environment}"
```

---

### 3.2 Truy vấn Metadata (GET)

```bash
curl -s -S -X GET "${API_BASE}/v1/admin/gateways/${TARGET_GATEWAY}/mqtt-credential" \
  -H @"${AUTH_HEADER_FILE}" \
  -H "Accept: application/json"
```

*Phản hồi mẫu thành công (HTTP 200 OK):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "operation_status": "succeeded",
  "activated_at": "2026-10-12T08:00:00Z",
  "revoked_at": null,
  "last_error_code": null
}
```

---

### 3.3 Cấp phát mới Credential (POST - Body rỗng, có ủy quyền thao tác)

Thao tác cấp mới credential ban đầu tạo mật khẩu một lần. Phản hồi được chuyển trực tiếp vào tệp kết quả bảo vệ (`0600`) trong thư mục của operator, không in ra màn hình hoặc ghi vào log:

```bash
# Chỉ thực thi khi ĐÃ CÓ ỦY QUYỀN THAO TÁC RÕ RÀNG (Explicit Mutation Authorization)
IDEMPOTENCY_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"
PROVISION_OUT="${OP_WORK_DIR}/provision_${TARGET_GATEWAY}_${IDEMPOTENCY_KEY}.json"

# Chuẩn bị tệp đầu ra độc quyền mode 0600
(umask 077 && touch "${PROVISION_OUT}")

curl -s -S -X POST "${API_BASE}/v1/admin/gateways/${TARGET_GATEWAY}/mqtt-credential" \
  -H @"${AUTH_HEADER_FILE}" \
  -H "Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -H "Accept: application/json" \
  --data "" \
  -o "${PROVISION_OUT}"

# Xử lý sau cấp phát: Operator sử dụng công cụ client đáng tin cậy để nạp mật khẩu
# từ "${PROVISION_OUT}" vào Gateway qua kênh nội bộ (USB/SSH cô lập).
# Sau khi hoàn tất nạp thành công, dọn dẹp cục bộ tệp mật khẩu tạm:
# shred -u "${PROVISION_OUT}" 2>/dev/null || rm -f "${PROVISION_OUT}"
```

*Cấu trúc DTO phản hồi mẫu lưu trong tệp (HTTP 201 Created - First Response):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "operation_status": "succeeded",
  "activated_at": "2026-10-12T08:00:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "password": "<one_time_generated_password_entropy_128bit>",
  "secret_returned": true
}
```

*Cấu trúc DTO khi Replay cùng `Idempotency-Key` (HTTP 201 Created - Không lộ secret):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "operation_status": "succeeded",
  "activated_at": "2026-10-12T08:00:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
  "secret_returned": false,
  "next_action": "rotate_with_new_idempotency_key",
  "replayed_status": "succeeded"
}
```

---

### 3.4 Xoay vòng mật khẩu (POST /rotate - Body rỗng, có ủy quyền thao tác)

Thao tác xoay vòng mật khẩu vô hiệu hóa phiên cũ, tạo mật khẩu mới và trả về một lần duy nhất:

```bash
# Chỉ thực thi khi ĐÃ CÓ ỦY QUYỀN THAO TÁC RÕ RÀNG (Explicit Mutation Authorization)
ROTATE_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"
ROTATE_OUT="${OP_WORK_DIR}/rotate_${TARGET_GATEWAY}_${ROTATE_KEY}.json"

(umask 077 && touch "${ROTATE_OUT}")

curl -s -S -X POST "${API_BASE}/v1/admin/gateways/${TARGET_GATEWAY}/mqtt-credential/rotate" \
  -H @"${AUTH_HEADER_FILE}" \
  -H "Idempotency-Key: ${ROTATE_KEY}" \
  -H "Accept: application/json" \
  --data "" \
  -o "${ROTATE_OUT}"

# Xử lý sau xoay vòng: Operator nạp mật khẩu mới vào Gateway qua kênh nội bộ,
# sau đó dọn dẹp cục bộ tệp tạm:
# shred -u "${ROTATE_OUT}" 2>/dev/null || rm -f "${ROTATE_OUT}"
```

*Cấu trúc DTO phản hồi mẫu lưu trong tệp (HTTP 200 OK - First Response):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "active",
  "last_operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "operation_status": "succeeded",
  "activated_at": "2026-10-12T08:15:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "password": "<new_one_time_generated_password_entropy_128bit>",
  "secret_returned": true
}
```

---

### 3.5 Thu hồi quyền truy cập (DELETE - Body rỗng, có ủy quyền thao tác)

Thao tác thu hồi (Revoke) không sinh mật khẩu, không trả secret. Kết quả chỉ bao gồm metadata kiểm toán:

```bash
# Chỉ thực thi khi ĐÃ CÓ ỦY QUYỀN THAO TÁC RÕ RÀNG (Explicit Mutation Authorization)
REVOKE_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"

curl -s -S -X DELETE "${API_BASE}/v1/admin/gateways/${TARGET_GATEWAY}/mqtt-credential" \
  -H @"${AUTH_HEADER_FILE}" \
  -H "Idempotency-Key: ${REVOKE_KEY}" \
  -H "Accept: application/json"
```

*Phản hồi mẫu thành công (HTTP 200 OK - Metadata thuần túy):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "revoked",
  "last_operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf4",
  "operation_status": "succeeded",
  "activated_at": "2026-10-12T08:15:00Z",
  "revoked_at": "2026-10-12T08:30:00Z",
  "last_error_code": null,
  "operation_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf4",
  "secret_returned": false,
  "next_action": "provision_with_new_idempotency_key"
}
```

---

### 3.6 Dọn dẹp cục bộ môi trường thao tác của Operator

Sau khi kết thúc phiên vận hành:
```bash
# Xóa an toàn tệp header xác thực cục bộ (không xóa nhầm các thư mục hay dữ liệu khác)
rm -f "${AUTH_HEADER_FILE}"
```
