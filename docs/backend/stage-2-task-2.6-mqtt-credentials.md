# Stage 2 — Task 2.6: Quản lý MQTT Credential (Dynamic Security & Ingress Gate)

Tài liệu này là Sổ tay Vận hành (Operating Runbook), đặc tả trạng thái triển khai endpoint, ranh giới an toàn và biên bản bàn giao cho Task 2.7 thuộc phân hệ Backend Đồ án 1 (`Project1_ET3290`).

> **Cảnh báo vận hành & Phê duyệt:**
> - Tài liệu này **KHÔNG** cấp phép tự ý chỉnh sửa `.env` môi trường, import credential hàng loạt, xoay vòng mật khẩu thiết bị production, recreate container stack hoặc thay đổi named volume đang chạy.
> - API MQTT Credential trong `docker-compose.yml` gốc được **chủ động vô hiệu hóa** (`MQTT_CREDENTIAL_API_ENABLED=false`). Template vòng đời và cấu hình runtime không đồng nghĩa với việc phê duyệt tự động bật trên môi trường production.
> - Mọi thao tác dưới đây phải thực hiện có giám sát bởi Quản trị viên Nền tảng (Platform Admin).

---

## 1. Trạng thái Triển khai Endpoint & Ranh giới Thẩm quyền

### 1.1. Bốn tuyến quản trị MQTT Credential (`/v1/admin/gateways/{gateway_id}/mqtt-credential*`)

Tất cả 4 endpoint quản trị đều được đăng ký dưới nhóm route bảo vệ `groups.admin`, yêu cầu:
1. **Xác thực danh tính (Authentication):** Header `Authorization: Bearer <human_jwt>` hợp lệ cấp bởi Supabase Auth (role `authenticated`, đúng issuer/audience/thời hạn).
2. **Ủy quyền nền tảng (Authorization):** Actor `user_id` phải tồn tại trong bảng `public.platform_admins` trên PostgreSQL. Quyền sở hữu Gateway (`owner`), vận hành (`operator`) hay người xem (`viewer`) trong `user_gateways` **KHÔNG CÓ QUYỀN** quản lý credential và sẽ nhận `403 Forbidden`.
3. **Chống lưu đệm (Anti-Caching):** Middleware `credentialCacheMiddleware` tự động gán `Cache-Control: no-store` và `Pragma: no-cache` cho toàn bộ các request vào prefix credential.
4. **Không nhận body (Strict No-Body):** GET/POST/DELETE đều không nhận body hoặc query. Body không rỗng, kể cả whitespace, nhận `400`; vượt giới hạn cấu hình `ADMIN_MAX_BODY_BYTES` nhận `413`. `AdminMaxBodyBytes` không phải 0: mặc định 16 KiB, tối đa 1 MiB, dùng làm giới hạn đọc trước khi kiểm tra empty-body.
5. **Idempotency bắt buộc cho Mutation:** Cả 3 hành động thay đổi trạng thái (Provision, Rotate, Revoke) bắt buộc truyền header `Idempotency-Key: <UUIDv4/UUIDv7>`. Không nhận key rỗng/sai cú pháp hoặc truyền lặp lại header.
6. **Bảo mật phản hồi lỗi:** Khi gặp lỗi nghiệp vụ hoặc adapter, API trả về mã lỗi an toàn từ `DomainError`. Tuyệt đối **không tự bịa đặt `operation_id`** trong error envelope khi tầng service chưa xác nhận bản ghi intent bền vững.

| Phương thức | Đường dẫn HTTP | Header bắt buộc | Mô tả nghiệp vụ | HTTP Status & Response Shape |
|---|---|---|---|---|
| `GET` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | `Authorization` | Truy vấn metadata credential hiện hành của Gateway | `200 OK` → `Metadata` JSON |
| `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | `Authorization`, `Idempotency-Key` | Cấp phát (Provision) credential ban đầu cho Gateway | `201 Created` → `SecretResult` (chứa password 1 lần duy nhất) |
| `POST` | `/v1/admin/gateways/{gateway_id}/mqtt-credential/rotate` | `Authorization`, `Idempotency-Key` | Xoay vòng (Rotate) mật khẩu trong cửa sổ bảo trì | `200 OK` → `SecretResult` (chứa password 1 lần duy nhất) |
| `DELETE` | `/v1/admin/gateways/{gateway_id}/mqtt-credential` | `Authorization`, `Idempotency-Key` | Thu hồi dứt khoát (Revoke) quyền kết nối MQTT | `200 OK` → `MutationResult` (không có secret) |

---

## 2. Mô hình Thẩm quyền Dữ liệu & Cơ chế Bảo vệ

Hệ thống phân định rạch ròi 4 khái niệm dữ liệu và vòng đời; hoàn tất một bước **không mặc nhiên đồng nghĩa** với các bước còn lại:

```text
[Durable Business Intent & Authority] (PostgreSQL: gateway_mqtt_credentials & events)
                 │
                 ▼
[Maintenance Window & Checkpoint Fence] (PostgreSQL: mqtt_credential_maintenance)
                 │
                 ▼
[Runtime Dynamic Security Mutation] (Broker RAM: $CONTROL/dynamic-security/v1)
                 │
                 ▼
[Native Snapshot Readback & Positive Verification] (dynamic-security.json & TLS Probe)
                 │
                 ▼
[Terminal DB Finalization] → [Verified OPEN & Completed Maintenance]
                 │
                 ▼
[One-Time Secret Output] (ClearSecret clears references, not physical zeroization)
                 │
                 ▼
[Manual Offline USB / Encrypted Hardware Handoff] (Operator Physical Action -> Device Flash)
```

1. **Thẩm quyền Nghiệp vụ (Desired Credential Authority):** Lưu tại PostgreSQL (`gateway_mqtt_credentials` và `gateway_mqtt_credential_events`). Business DB không lưu mật khẩu plaintext/hash; native DynSec store giữ dữ liệu xác thực của broker. `ClearSecret()` loại bỏ tham chiếu trong DTO, không bảo đảm xóa vật lý mọi bản sao trong bộ nhớ Go/HTTP.
2. **Chốt chặn Bảo trì (Maintenance Checkpoint & Fence):** Bảng `public.mqtt_credential_maintenance` ghi nhận trạng thái cửa sổ bảo trì (`in_progress`, `completed`, `recovery_needed`). Mọi mutation đều có fence epoch/nonce; trạng thái `terminal success` của operation trên DB không đồng nghĩa với việc Ingress Gate đã `OPEN`.
3. **Bản ghi Phục hồi Liên kết (Linked Recovery Record):** Bảng `public.mqtt_credential_recovery` liên kết chặt chẽ với operation cần phục hồi qua `recovery_id`. Bản ghi này bảo toàn lịch sử kiểm toán (audit trail), không bao giờ ghi đè hoặc xóa event gốc.
4. **Bàn giao Thiết bị (Device Handoff Receipt):** Việc nạp mật khẩu mới vào bộ nhớ mã hóa của Gateway (ESP32 NVS, Luckfox/AM5728 flash) được thực hiện thủ công offline qua USB bởi operator. **Tuyệt đối KHÔNG gửi mật khẩu tới Gateway qua MQTT**. Phản hồi API thành công (`terminal success`) chỉ xác nhận broker đã nạp và verify; trạng thái bàn giao thiết bị (`delivery_status`) vẫn là `unknown` cho đến khi có biên nhận nạp phần cứng độc lập.

---

## 3. Kiến trúc Vòng đời Khởi động: Ingress Gate & Fail-Closed

1. **Mặc định ĐÓNG (Startup CLOSED):**
   - Khi tiến trình Go Backend hoặc Mosquitto khởi động, Ingress Gate (proxy TCP/TLS kiểm soát ingress của Gateway) mặc định ở trạng thái `CLOSED`. Cổng này đóng toàn bộ lưu lượng Gateway từ cổng host ngoài lẫn Rathole reverse tunnel.
   - Trạng thái đóng được giữ vững trước khi kết nối cơ sở dữ liệu được thiết lập và trước khi quy trình đối soát khởi động hoàn tất.
2. **Quy trình Đối soát Khởi động (Startup Reconciliation):**
   - `StartupReconciler` quét unresolved operations, unfinished maintenance (kể cả event đã succeeded), pending recovery, durable revokes và current inventory. Batch/max-pages/timeouts cấu hình được; mặc định batch 64, max-pages 16. Scan thiếu hoặc cạn budget không được coi là inventory sạch.
   - `DisableStartup` đối chiếu RAM disabled và protected snapshot. Principal vắng mặt chỉ được chấp nhận với correlated native not-found và protected snapshot absence trong đúng epoch; generic error/file không đọc được không phải bằng chứng absence. Snapshot observed không phải fsync/power-loss proof.
   - Kiểm tra xác thực tài khoản nội bộ `backend_service` và manager trên listener nội bộ của broker trong cùng epoch khởi động.
3. **Điều kiện MỞ (OPEN Barrier):**
   - Chỉ khi toàn bộ danh sách thu hồi được xác minh thành công, snapshot đồng nhất và chu kỳ broker chứng minh đúng `epoch` và `nonce` khớp với checkpoint, Ingress Gate mới mở cổng cho Gateway kết nối (`OpenVerified`).
4. **Bảo vệ khi Sự cố Khởi động:**
   - Nếu mất kết nối database, file snapshot sai lệch UID/quyền hạn, hoặc broker phản hồi bất thường, Ingress Gate duy trì trạng thái **ĐÓNG (CLOSED)**, giải phóng tài nguyên cục bộ (`CloseDrain`) và yêu cầu operator can thiệp. Không bao giờ tự động mở cổng khi chưa xác minh.
5. **Thời hạn Độc lập `FinalizeTimeout`:**
   - Các giá trị mặc định cấu hình được: `DBTimeout=2s` cho read/admission và ambiguity lookup, `FinalizeTimeout=5s` cho `ConditionalFinalize`/`FailKnown`/`RequireRecovery`, `RequestTimeout=40s` cho service request. Caller deadline vẫn là giới hạn trên; detached recovery có budget riêng. Timeout COMMIT là outcome unknown, không chứng minh rollback.

---

## 4. Hướng dẫn Vận hành Chi tiết (Operation Runbook)

### 4.1. Điều kiện Tiên quyết An toàn

- Client quản trị trên máy tin cậy, ví dụ Bruno, dùng URL deployment được cấu hình và human JWT của platform admin; không dùng `service_role` để gọi Go business API.
- Token/password dùng secret storage hoặc biến runtime không persist, không Git sync/export/log. HTTPS bắt buộc trên đường mạng không tin cậy; không đưa token vào URL/argv.
- Xác định Gateway đã provision và readiness của lifecycle; bật feature flag không thay thế qualification topology.
- Giữ riêng UUID idempotency cho từng operation. Retry cùng operation dùng lại key; không tự sinh key mới trên mỗi retry. Trong Bruno phải kiểm tra pre-request script để không phá replay semantics.

---

### 4.2. Cấp phát Mới Credential (Provision)

Quy trình áp dụng khi Gateway đã được tạo qua Admin PUT (`/v1/admin/gateways/{gateway_id}`) nhưng chưa từng có credential hoặc credential trước đó đã ở trạng thái `revoked`/`failed`:

1. **Chuẩn bị Idempotency Key:** UUID hợp lệ khác nil, không trùng operation khác.
2. **Gửi bằng client quản trị:** POST credential path với `Authorization: Bearer <human_jwt>` và `Idempotency-Key: <operation_uuid>`; body None, không query. Không xuất response secret ra terminal/log hoặc lưu trong collection.
3. **Xử lý phản hồi (HTTP 201 Created):**
   Phản hồi JSON chứa mật khẩu sinh ngẫu nhiên CSPRNG (256-bit entropy):
   ```json
   {
      "gateway_id": "<target_gateway_id>",
      "username": "<target_gateway_id>",
     "credential_version": 1,
     "status": "active",
     "last_operation_id": "0195e18c-...",
     "operation_status": "succeeded",
     "password": "<one-time-base64url-secret>",
     "secret_returned": true
   }
   ```
4. **Nạp thiết bị ngoại tuyến (Offline Handoff):**
   - Sao chép giá trị `password` trực tiếp vào USB flash drive mã hóa hoặc thiết bị nạp phần cứng an toàn.
   - Nạp vào phân vùng lưu trữ bảo mật (NVS/Flash) của Gateway mục tiêu.
   - Xóa clipboard và file tạm chứa mật khẩu trên máy trạm operator.

---

### 4.3. Xoay vòng Credential trong Cửa sổ Bảo trì (Rotate)

Quy trình áp dụng định kỳ hoặc khi nghi ngờ lộ mật khẩu, khi credential hiện tại đang `active`:

1. **Chuẩn bị key mới:** chỉ tạo key mới khi thực sự yêu cầu lượt rotate mới.
2. **Gửi bằng client quản trị:** POST credential `/rotate` với human JWT và UUID key; body None. Retry giữ nguyên key.
3. **Hành vi Hệ thống trong Cửa sổ Bảo trì:**
    - Hệ thống đóng Ingress Gate và drain toàn bộ kết nối hiện hữu: Gateway B cũng có thể gián đoạn trong global maintenance nhưng phải reconnect bằng credential không đổi. Không hứa zero outage.
   - Cập nhật mật khẩu mới trên DynSec RAM qua TLS quản trị nội bộ.
   - Cold reload broker từ snapshot đĩa và probe kiểm tra đăng nhập thành công bằng mật khẩu mới trên listener nội bộ.
   - Finalize cập nhật phiên bản mới (`credential_version = n + 1`) vào PostgreSQL.
   - Mở lại Ingress Gate và trả về mật khẩu mới duy nhất một lần (`HTTP 200 OK`).
4. **Nạp thiết bị ngoại tuyến:** Operator nạp mật khẩu mới cho Gateway qua USB tương tự mục 4.2.

---

### 4.4. Thu hồi Credential Khẩn cấp (Revoke)

Quy trình áp dụng khi Gateway bị vô hiệu hóa, bị mất cắp hoặc ngắt dịch vụ:

1. **Chuẩn bị key cho revoke:** UUID hợp lệ; retry cùng key.
2. **Gửi bằng client quản trị:** DELETE credential path với human JWT và UUID key; body None. Revoke không sinh hoặc trả password.
3. **Xử lý phản hồi (HTTP 200 OK):**
   ```json
   {
      "gateway_id": "<target_gateway_id>",
      "username": "<target_gateway_id>",
     "credential_version": 2,
     "status": "revoked",
     "last_operation_id": "0195e18c-...",
     "operation_status": "succeeded",
     "secret_returned": false,
     "next_action": "provision_with_new_idempotency_key"
   }
   ```
    *Lưu ý:* Revoke thành công có verification native disable/session disconnect và fresh rejection trong fixture. Historical event bất biến nhưng current authority có thể thay đổi qua authorized re-provision thế hệ mới; không coi revoke là cấm cấp lại vĩnh viễn.

---

### 4.5. Phát lại Idempotent & Xử lý Mất Mật khẩu (Lost Secret Cases)

Hệ thống thiết kế theo nguyên tắc bảo mật tối cao: **Không bao giờ lưu và không bao giờ xuất lại mật khẩu cũ**.

#### Trường hợp A: Phát lại với cùng Idempotency Key (Idempotent Replay)
Nếu client gọi lại request POST provision/rotate với cùng `Idempotency-Key` đã hoàn tất thành công:
- Dùng lại đúng method/path và key trong client quản trị; không thay key khi gửi lại.
- Provision replay trả `201`, rotate replay trả `200`; đều metadata-only, `secret_returned: false`, không `password`, không chạy lại broker mutation.
- Trường `next_action` gợi ý: `"rotate_with_new_idempotency_key"`.

#### Trường hợp B: Operator đánh mất mật khẩu khi vừa nhận (Lost Secret)
- **Nếu trạng thái hiện tại là `active` và maintenance window đã kết thúc bình thường (`completed`):**
  - Tuyệt đối **không cố đọc lại mật khẩu từ DB** (DB không lưu).
  - Không rollback về mật khẩu cũ (broker đã nạp và từ chối mật khẩu cũ).
  - **Giải pháp duy nhất:** Thực hiện một lượt **Rotate mới** với `Idempotency-Key` hoàn toàn mới để sinh mật khẩu mới và nạp lại vào thiết bị.
- **Nếu thao tác bị gián đoạn/sập nguồn giữa chừng (`recovery_needed`):**
  - Xem quy trình xử lý phục hồi tại mục 4.6. Không thể dùng API rotate thông thường khi trạng thái đang bị khóa recovery.

---

### 4.6. Chẩn đoán Sự cố & Quy trình Phục hồi (`recovery_needed`)

Gián đoạn có thể để lại operation pending/recovery-needed, hoặc terminal succeeded nhưng maintenance chưa hoàn tất. Không kết luận chỉ từ một trường status; cần đối chiếu authority, event, checkpoint và recovery record. Không có power-loss durability guarantee.

#### Dấu hiệu nhận biết:
- Recovery-required/finalization-pending/runtime-busy được HTTP mapper trả `503`. `409` dành cho conflict theo contract, không phải mặc định cho recovery. Safe error envelope nằm trong trường `error`:
  ```json
   {"error": {"code": "credential_recovery_required", "message": "credential request failed", "request_id": "<request_id>"}}
  ```
- Đọc metadata qua GET bằng client quản trị; kiểm tra checkpoint/recovery qua protected operator DB tooling. Metadata có thể còn active/succeeded khi checkpoint unresolved; không diễn giải thành quyền thực hiện operation tiếp theo.

#### Quy trình Xử lý Phục hồi An toàn:
1. **Khởi động lại Backend để kích hoạt Startup Reconciliation:**
    - Chỉ restart theo change window được duyệt; startup quét cả unresolved events, unfinished checkpoints và pending recovery, không chỉ `recovery_needed`.
   - Adapter thực hiện gọi `DisableStartup` trên broker để vô hiệu hóa dứt khoát client trên RAM và ghi nhận snapshot.
    - Durable recovery intent được ghi trước disable. Khi proof đúng epoch đủ, transaction chuyển recovery sang **`disabled`**, current authority sang `revoked` và checkpoint sang `completed`, liên kết disposition mà không sửa terminal event gốc. Chưa đủ proof thì giữ pending/fence/CLOSED.
2. **Cấm bypass thủ công:**
   - **Tuyệt đối KHÔNG** can thiệp trực tiếp sửa đổi các dòng kiểm toán trong database (`UPDATE ... SET status='active'`).
   - **Tuyệt đối KHÔNG** tự ý hạ fence bảo trì (`mqtt_credential_maintenance`) hoặc sửa file `dynamic-security.json` bằng tay.
    - Sửa tay có thể vi phạm guards hoặc tạo contradictory authority phải diagnosis; không hứa có thể tự sửa hay khẳng định một lỗi sẽ khóa vĩnh viễn.
3. **Cấp phát lại mật khẩu mới:**
   - Sau khi tiến trình phục hồi đưa credential về `revoked`, Quản trị viên sử dụng API **Provision** với một `Idempotency-Key` mới để cấp lại mật khẩu sạch từ đầu và nạp USB cho Gateway.

---

## 5. Lệnh Kiểm chứng Độc lập (Isolated Verification Commands)

Các lệnh kiểm chứng dưới đây sử dụng harness kiểm thử độc lập, không yêu cầu và không chỉnh sửa file `.env` triển khai thật:

1. **Kiểm thử Đơn vị & Tương tranh Go (Race Detection & Unit Tests):**
   ```bash
   cd src && go test -v -race -coverprofile=coverage.out ./internal/mqttcredential/... ./internal/httpserver/...
   ```
2. **Kiểm thử Tích hợp Adapter & Controller DynSec Mock:**
   ```bash
   cd src && go test -v -race -run 'TestDynSec.*' ./internal/mqttcredential/...
   ```
3. **Kiểm thử startup đơn vị (live integration có thể SKIP nếu chưa có fixture):**
   ```bash
   cd src && go test -v -race -run 'TestStartup.*' ./internal/mqttcredential/...
   ```
4. **Harness triển khai Task 2.6, chạy HTTP standalone và selectors native thật:**
   ```bash
   sh scripts/test-stage2-mqtt-credentials.sh
   ```
    Xem [báo cáo nghiệm thu](stage-2-task-2.6-acceptance.md) cho actual PASS/selectors/coverage. Chạy spike provenance riêng bằng `python3 scripts/tests/task26_credential_spike.py --scenario all`; không thay fixture deployment acceptance bằng spike hoặc skipped Go test.

---

## 6. Ranh giới Migration CSDL & Ràng buộc Rollback

### 6.1. Lịch sử Migration (000010 đến 000016)
- `000010_stage2_auth_and_provisioning.up.sql`: Khởi tạo bảng `platform_admins`, cấu trúc sơ khởi credential.
- `000011_mqtt_credential_operations.up.sql`: Bổ sung bảng nhật ký thao tác `gateway_mqtt_credential_events`.
- `000012_mqtt_credential_maintenance.up.sql`: Thiết lập bảng chốt chặn cửa sổ bảo trì `mqtt_credential_maintenance`.
- `000013_mqtt_maintenance_admission.up.sql`: Siết chặt điều kiện tiếp nhận bảo trì và fence chống ghi đè.
- `000014_mqtt_credential_recovery.up.sql`: Thiết lập bảng quản lý phục hồi bất định `mqtt_credential_recovery`.
- `000015_mqtt_recovery_authority_guards.up.sql`: Ràng buộc toàn vẹn thế hệ và khóa ngoại kiểm soát phục hồi.
- `000016_mqtt_event_authority_guard.up.sql`: Ràng buộc chống sửa đổi dữ liệu kiểm toán lịch sử và khóa chống nhảy version.

> **Quy tắc Bất di Bất dịch:**
> Tuyệt đối **KHÔNG ĐƯỢC CHỈNH SỬA** các file migration lịch sử từ `000010` đến `000016`. Mọi thay đổi schema (nếu có trong tương lai) bắt buộc phải tạo migration tăng dần (`000017+`).

### 6.2. Ràng buộc Rollback Triển khai (Deployment Rollback Constraints)
- Nếu rollback mã nguồn Go Backend về phiên bản trước Task 2.6:
  - Phải duy trì schema database ở version hiện tại (không chạy `down.sql` làm mất mát dữ liệu kiểm toán lịch sử).
  - Vô hiệu hóa tính năng credential bằng cách đặt `MQTT_CREDENTIAL_API_ENABLED=false`.
   - Task 2.5 legacy còn trong source để regression, không phải automatic fallback từ DynSec. Chuyển runtime phải có approved cutover, backup và re-provision; không bật lại credential cũ để bypass revoke.

---

## 7. Giới hạn Nghiệm thu Còn lại (Remaining Limits)

Trước khi nghiệm thu toàn bộ Giai đoạn 2 (Stage 2 acceptance), các rào cản và giới hạn sau cần được ghi nhận trung thực:
1. **CI Task 2.6:** Xem acceptance report cho run `37204985289`, SHA `55cdcd5`, 9 jobs xanh. Không áp bằng chứng này cho các thay đổi Task 2.7 chưa commit hoặc job `stage2-e2e` chưa nghiệm thu.
2. **Chưa Triển khai Production:** Tính năng bị tắt mặc định trên Compose gốc (`MQTT_CREDENTIAL_API_ENABLED=false`). Chưa thực hiện deploy lên máy chủ staging/production.
3. **Chưa Thẩm định Phần cứng Thật (Hardware Qualification):** Quy trình nạp USB offline và khả năng lưu trữ mã hóa an toàn trên các vi điều khiển/bo mạch vật lý (ESP32, Luckfox Pico Plus, TI AM5728) chưa được nghiệm thu thực tế.
4. **Chính sách Mất DB sau khi MỞ (Post-OPEN DB Loss):** Hiện tại hệ thống chưa tích hợp cơ chế tự động ngắt kết nối Gateway nếu PostgreSQL bị sập sau khi Ingress Gate đã mở thành công (`OPEN`).
5. **Rathole Bypass & Mạng Ngoài:** Kiểm chứng thực tế tính không thể bypass của Ingress Gate khi đi qua đường hầm Rathole và môi trường Internet công cộng cần được thẩm định riêng trong cấu hình hạ tầng mạng cuối.
6. **Các Kịch bản Không Chạy (NOT RUN):** Kịch bản mất gói tin phản hồi HTTP sau commit transport (`HTTP transport-response loss`) và lỗi đứt cáp SQL wire-loss trong tích tắc commit transaction vật lý không nằm trong phạm vi tự động hóa của regression test.

---

## 8. Biên bản Bàn giao cho Task 2.7 (Handoff to Task 2.7)

Task 2.7 chịu trách nhiệm nghiệm thu tích hợp E2E toàn bộ Giai đoạn 2 (Stage 2 Integration & Acceptance). Dưới đây là các giao điểm kỹ thuật bàn giao từ Task 2.6:

1. **Hợp đồng API Đã Khóa:** Bốn endpoint `/v1/admin/gateways/{gateway_id}/mqtt-credential*` đã hoàn tất triển khai trong codebase, kiểm thử unit và HTTP integration pass 100%. Task 2.7 có thể xây dựng kịch bản kiểm thử E2E dựa trên đúng HTTP contract này.
2. **Tài liệu Kế hoạch Bên ngoài:** Lưu ý rằng file kế hoạch chi tiết `docs/backend_plan/task_2.6_detail_plan.md` nằm bên ngoài git repository `main_src/`. Mọi cập nhật trạng thái của Task 2.6 đã được ghi nhận trực tiếp vào tài liệu kế hoạch đó.
3. **Báo cáo Nghiệm thu:** Tham chiếu bằng chứng và ma trận lỗi tại [Task 2.6 acceptance](stage-2-task-2.6-acceptance.md); [Task 2.7 acceptance](stage-2-task-2.7-acceptance.md) vẫn pending cho E2E/CI mới.
4. **Phạm vi Tiếp theo của Task 2.7:**
    - E2E: real GoTrue login/refresh → Admin Gateway/Sensor provisioning → credential lifecycle → gated MQTT TLS/ACL → user Gateway/Sensor reads. Telemetry ingestion/history/WebSocket/media/control không thuộc Stage 2 acceptance.
   - Thẩm định quy trình phân quyền membership `user_gateways` phối hợp với tài khoản quản trị.
   - Đóng gói tài liệu bàn giao tổng thể cho toàn bộ Milestone Backend Giai đoạn 2.
