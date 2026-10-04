# Stage 2 — Task 2.6: Báo cáo Nghiệm thu và Bằng chứng Kiểm chứng

## 1. Mục tiêu và Ranh giới Nghiệm thu

Tài liệu này là báo cáo nghiệm thu kỹ thuật và tổng hợp bằng chứng thực nghiệm cho **Task 2.6: Admin MQTT Credential Lifecycle API, DynSec Ingress Gate và PostgreSQL Transactional Operations**.

Kế hoạch nguồn có thẩm quyền (authoritative plan) được lưu trữ tại `docs/backend_plan/task_2.6_detail_plan.md`. Báo cáo này đóng vai trò xác nhận mức độ hoàn thành triển khai mã nguồn, bộ kiểm thử, kết quả rà soát (code/database/security review) và kết quả chạy hồi quy độc lập cho toàn bộ các gói công việc từ Task 2.6.0 đến Task 2.6.13.

### 1.1 Ranh giới phạm vi (Scope Boundaries)

- **Thuộc phạm vi hoàn thành của Task 2.6**:
  - Thiết kế và triển khai vòng đời thông tin xác thực MQTT của Gateway: Provision (cấp mới), Rotate (xoay vòng trong maintenance window), Revoke (thu hồi có kiểm chứng) và Metadata Query (truy vấn trạng thái).
  - Phân hệ kiểm soát cổng Ingress Gate dựa trên Mosquitto Dynamic Security (DynSec) plugin, đảm bảo nguyên tắc mặc định đóng (fail-closed / default CLOSED) khi khởi động hoặc gặp sự cố bất định.
  - Quản lý giao dịch PostgreSQL phân tầng với các bảng `gateway_mqtt_credentials`, `gateway_mqtt_credential_events`, `mqtt_credential_maintenance`, `mqtt_credential_recovery`, và các trigger guard migration 000015/000016 ngăn chặn sửa đổi trạng thái trái phép.
  - Bộ sinh mật khẩu an toàn CSPRNG (ít nhất 128 bit entropy) và cơ chế chỉ trả mật khẩu plaintext một lần duy nhất qua HTTP API, tuyệt đối không lưu trữ plaintext trong cơ sở dữ liệu hay bộ nhớ dài hạn.
  - Triển khai đầy đủ REST API dành cho Platform Admin tại `cmd/server` và `internal/httpserver` với kiểm tra quyền quản trị nền tảng (`platform_admins`), bảo vệ bằng Supabase JWT, vô hiệu hóa mặc định trong cấu hình production (`MQTT_CREDENTIAL_API_ENABLED=false`).
  - Toàn bộ các kiểm thử đơn vị, kiểm thử tích hợp PostgreSQL cô lập, kiểm thử broker native Mosquitto 2.0.18, kiểm thử tiến trình độc lập `cmd/server`, và pipeline hồi quy tích hợp Task 2.6.

- **Ngoài phạm vi Task 2.6 (chuyển giao cho Task 2.7 và các giai đoạn tiếp theo)**:
  - Task 2.6 không tuyên bố hoàn thành toàn bộ Giai đoạn 2 (Stage 2). Task 2.7 chịu trách nhiệm về kiểm thử luồng tích hợp đầu-cuối (E2E) toàn Giai đoạn 2, quy trình vận hành phân quyền thành viên Gateway (`user_gateways`), và nghiệm thu tổng thể Stage 2.
  - Không bao gồm việc kích hoạt mặc định Admin Credential API trên môi trường production.
  - Không bao gồm quy trình nạp thông tin xác thực vật lý qua cổng USB hoặc lưu trữ mã hóa phần cứng (flash/NVS) trên các thiết bị nhúng thực tế (ESP32, Luckfox Pico Plus, TI AM5728).
  - Không bao gồm chính sách xử lý mất kết nối cơ sở dữ liệu sau khi Ingress Gate đã mở (post-OPEN database-loss policy).

---

## 2. Kết quả Rà soát Kỹ thuật (Review Scope & Findings)

Trước khi thực hiện chạy hồi quy nghiệm thu cuối cùng, mã nguồn và thiết kế của Task 2.6 đã trải qua ba nội dung rà soát chuyên sâu: Go Code Review, Database Review và Security Review.

### 2.1 Phát hiện chung giữa Go Code Review và Database Review

Cả hai đợt rà soát Go Code Review và Database Review đã độc lập nhận diện **cùng một phát hiện kỹ thuật duy nhất** (single common finding), không phải hai lỗi phân tách:
- **Hiện tượng**: Giá trị cấu hình `FinalizeTimeout` trong `internal/config` chưa được đấu nối (unwired) vào tầng khởi tạo repository của PostgreSQL, khiến hạn mức thời gian cho giao dịch hoàn tất (finalization transaction budget) có nguy cơ bị gộp chung hoặc không được áp dụng chính xác theo cấu hình.
- **Biện pháp khắc phục**: Đã bổ sung hàm dựng `NewPostgresRepositoryWithFinalizationTimeout` trong gói `internal/mqttcredential` và đấu nối trực tiếp tại `src/cmd/server/credential.go`:
  ```go
  repo, e := mqttcredential.NewPostgresRepositoryWithFinalizationTimeout(pool, c.DBTimeout, c.FinalizeTimeout, c.BatchSize)
  ```
- **Ý nghĩa kiến trúc**: Hàm dựng mới phân tách rõ ràng ngân sách thời gian của giao dịch hoàn tất (`FinalizeTimeout`) khỏi hạn mức của các thao tác đọc và kiểm tra tiếp nhận ngắn hạn (`DBTimeout`). Đồng thời, các thao tác dọn dẹp khi rollback và truy vấn trạng thái bất định (ambiguity lookups) tiếp tục giữ hạn mức truy vấn tiêu chuẩn độc lập, ngăn ngừa việc nghẽn kết nối kéo dài làm cạn kiệt connection pool.
- **Xác minh**: Mã nguồn Go sau khi sửa đã vượt qua kiểm thử đơn vị tập trung và kiểm thử tích hợp giao dịch PostgreSQL (`TestPostgresSeparateFinalizationBudget`, `TestPostgresFinalizationIntegration/separate_finalization_budget_and_short_admission_reads`).

### 2.2 Đánh giá An toàn Thông tin (Security Review)

Phân hệ mã nguồn mới và các kịch bản kiểm thử trong fixture cô lập đạt chuẩn an toàn thông tin:
1. **Quản lý bí mật (Secret Lifecycle)**: Mật khẩu Gateway được sinh bởi CSPRNG với độ dài tối thiểu quy định, mã hóa và truyền trực tiếp qua stdin của broker native; plaintext chỉ được trả về một lần trong phản hồi HTTP POST và lập tức bị xóa khỏi bộ nhớ tiến trình (zeroed/garbage collected). Không lưu plaintext hoặc hash tạm thời trong database hay log.
2. **Ủy quyền nghiêm ngặt (Platform Admin Authorization)**: Toàn bộ các route `/v1/admin/gateways/:gateway_id/mqtt-credential*` đều bắt buộc xác thực qua middleware Supabase JWT và kiểm tra quyền platform admin thông qua truy vấn bảng `platform_admins`. Mọi vai trò khác (`owner`, `operator`, `viewer` trong `user_gateways`) hoặc người dùng vãng lai đều bị từ chối với mã lỗi `403 Forbidden` mà không gây ra bất kỳ tác dụng phụ nào trên hệ thống.
3. **Cách ly Topic Broker**: Áp dụng cơ chế Mosquitto DynSec với ACL tĩnh tường minh gán theo tiền tố URN của Gateway, bảo đảm tính cách ly tuyệt đối: Gateway chỉ được phép publish/subscribe trên không gian topic của chính mình (`gateways/<gateway_id>/...`).
4. **Nguyên tắc Mặc định Đóng (Fail-Closed Ingress Gate)**: Cổng Ingress Gate của Mosquitto chỉ mở (`OPEN`) khi cả ba điều kiện được xác nhận: broker RAM đã áp dụng, snapshot file đã ghi nhận, và kiểm chứng kết nối mới bằng TLS thành công. Bất kỳ lỗi nào trong quá trình thực hiện đều giữ cổng ở trạng thái `CLOSED`.

### 2.3 Bảo vệ Tầng Cơ sở Dữ liệu (Database Migration Guards)

Các trigger guard trong migration lịch sử `000015_mqtt_recovery_authority_guards.up.sql` và `000016_mqtt_event_authority_guard.up.sql` đã qua rà soát và hoạt động chính xác:
- Ngăn chặn mọi thao tác cập nhật trực tiếp hoặc nhảy cóc trạng thái trên bảng `gateway_mqtt_credentials` nếu không có bản ghi nghiệp vụ hợp lệ tương ứng trong `gateway_mqtt_credential_events`.
- Bắt buộc kiểm tra phiên bản (version CAS) và bảo toàn dữ liệu trạng thái trước đó (`previous_status`, `previous_credential_version`).
- Khóa bi quan (`SELECT ... FOR UPDATE`) trên Gateway đích ngăn chặn các race condition giữa hai tiến trình quản trị đồng thời.

---

## 3. Bằng chứng Thực nghiệm và Hồi quy Nghiệm thu

Toàn bộ các nội dung kiểm thử đã được thực thi và xác nhận thông qua bản ghi hồi quy nghiệm thu sạch tại `/tmp/opencode/task2613-final-regression.log`. Quá trình kiểm thử kết thúc với trạng thái **`COMPLETE GREEN RUN`**, tất cả 6 bộ chọn (named selectors) bắt buộc đều đạt kết quả **`PASS`** mà không có bất kỳ bộ chọn nào bị bỏ qua (`SKIP`) hay thất bại (`FAIL`).

### 3.1 Phân hệ 1: Kiểm thử Repository PostgreSQL và Trigger Guards

- **Lệnh thực thi**:
  ```sh
  env TASK263_COVERAGE_OUT=/tmp/opencode/task2613-postgres.cover sh scripts/test-task263a-repository.sh
  ```
- **Kết quả**: `PASS` (Thời gian chạy suite chính: ~5.9s).
- **Chi tiết các ca kiểm thử tiêu biểu**:
  - `TestPostgresAuthorityGuards`: 10 ca kiểm thử kiểm chứng trigger guard ngăn chặn cập nhật trái phép trạng thái, phiên bản, actor, và timestamp.
  - `TestPostgresFinalizationIntegration`: 10 ca kiểm thử giao dịch hoàn tất, bao gồm phân tách budget `FinalizeTimeout`, bằng chứng CAS, tính đơn điệu của recovery, race condition giữa finalize và recovery, độ trễ commit an toàn, và xử lý sự cố database.
  - `TestPostgresAdmissionIntegration`: 9 ca kiểm thử kiểm chứng tiếp nhận thao tác, xung đột khóa đồng thời giữa các Gateway khác nhau, duy trì thế hệ khi revoke, và timeout khi commit không cấp quyền thực thi.
  - `TestPostgresMaintenanceIntegration`: 4 ca kiểm thử kiểm tra rollback toàn bộ khi intent checkpoint thất bại, fencing bằng broker epoch khi có thao tác mới, và tuần tự hóa giữa completion và admission mới.
  - `TestPostgresRecoveryIntegration`: 7 ca kiểm thử kịch bản bất định trong provision/rotate, phục hồi sau sự cố, và chẩn đoán phân hệ cũ khi khởi động.
  - `TestPostgresSeparateFinalizationBudget`, `TestPostgresStartupRecoveryContractBlocker`: Tất cả đều đạt `PASS`.

### 3.2 Phân hệ 2: Kiểm thử Tích hợp Credential và Standalone Server (`cmd/server`)

- **Lệnh thực thi**:
  ```sh
  env TASK2612_COVERAGE_OUT=/tmp/opencode/task2613-http.cover \
      TASK266_COVERAGE_OUT=/tmp/opencode/task2613-provision.cover \
      TASK267_COVERAGE_OUT=/tmp/opencode/task2613-rotate.cover \
      TASK268_COVERAGE_OUT=/tmp/opencode/task2613-revoke.cover \
      TASK269_COVERAGE_OUT=/tmp/opencode/task2613-startup.cover \
      TASK2611_COVERAGE_OUT=/tmp/opencode/task2613-composition.cover \
      sh scripts/test-stage2-mqtt-credentials.sh
  ```
- **Kết quả**: `PASS`.
- **Kiểm chứng tiến trình độc lập `cmd/server` (Standalone Server Execution)**:
  - Khởi chạy tiến trình `cmd/server` thực tế trong fixture mạng cô lập (loopback private broker, chỉ mở port TLS qua Ingress Gate).
  - Xác nhận 4 route HTTP (`GET`, `POST`, `POST /rotate`, `DELETE`) hoạt động chính xác với cơ sở dữ liệu PostgreSQL thực tế và xác thực JWT.
  - Kiểm tra per-request database checker hoạt động fail-closed khi mất kết nối cơ sở dữ liệu.
  - Kiểm tra các tình huống lỗi bất định: lỗi ghi snapshot native dẫn đến phản hồi HTTP 503 và Ingress Gate giữ `CLOSED`; kiểm tra khôi phục sau khi tiến trình bị SIGKILL và khởi động lại.
  - Kiểm tra tắt dịch vụ duyên nợ (bounded SIGTERM shutdown) đảm bảo đóng cổng Ingress Gate trước khi tiến trình thoát.
   - Kiểm tra trường hợp cấu hình tắt (`MQTT_CREDENTIAL_API_ENABLED=false`): toàn bộ route credential bị vô hiệu hóa an toàn mà không khởi tạo bất kỳ kết nối broker, CA hay controller nào.
- **6 Bộ chọn Tên Bắt buộc (Six Required Named Selectors)**:

| STT | Tên Selector | Thời gian chạy | Kết quả | Nội dung kiểm chứng chính |
|---|---|---|---|---|
| 1 | `TestCredentialHTTPIsolated` | 5.64s | `PASS` | Kiểm chứng đầy đủ 4 route HTTP, phân quyền actor, kiểm tra parsing input, cách ly ACL tường minh, và phục hồi sau lỗi SQL finalization. |
| 2 | `TestProvisionServicePinnedBrokerPostgres` | 9.95s | `PASS` | Cấp mới thông tin xác thực với broker native và PostgreSQL; kiểm chứng từ chối non-admin, ghi intent/finalization, kiểm tra login TLS thực tế, và xử lý các lỗi nhánh (save fault, rng fault, disconnect trước OPEN ACK). |
| 3 | `TestRotateServicePinnedBrokerPostgres` | 19.35s | `PASS` | Xoay vòng mật khẩu trong maintenance window; từ chối mật khẩu cũ (cold OLD rejection), chấp nhận mật khẩu mới (NEW acceptance), replay an toàn, và xử lý ngắt kết nối trước OPEN ACK. |
| 4 | `TestRevokeServicePinnedBrokerPostgres` | 12.80s | `PASS` | Thu hồi mật khẩu có kiểm chứng RAM và snapshot; ngắt kết nối session hiện tại; idempotent replay; kiểm chứng kịch bản save fault chỉ lưu RAM thì giữ trạng thái recovery CLOSED. |
| 5 | `TestStartupServicePinnedBrokerPostgres` | 13.53s | `PASS` | Quy trình đối soát khi khởi động (startup reconciliation); dọn dẹp các intent bị mất hoặc chưa hoàn tất; bảo đảm tính bất biến của sự kiện; xác nhận cổng Ingress Gate giữ `CLOSED` khi có lỗi. |
| 6 | `TestCredentialCompositionIsolated` | 3.35s | `PASS` | Kiểm chứng composition root kết nối backend, snapshot, CA certs, repository, adapter, reconciler; xác nhận cổng Ingress Gate chuyển `OPEN` khi đủ điều kiện và `CLOSED` khi shutdown. |

### 3.3 Phân hệ 3: Hồi quy Mosquitto Runtime Thực tế

- **Lệnh thực thi**:
  ```sh
  sh scripts/test-stage2-mosquitto-runtime.sh
  ```
- **Kết quả**: `PASS`.
- **Nội dung kiểm tra**:
  - Broker Mosquitto 2.0.18 native chạy trên nền Alpine 3.18.9 dưới người dùng `UID 1883` (PID 1).
  - Sử dụng công cụ native `mosquitto_passwd` sinh mã băm PBKDF2 ($7$).
  - Giao tiếp tín hiệu reload qua Unix Domain Socket trong không gian mạng cô lập (`network_mode: none`).
  - Chạy `TestProductionRuntimeIntegration` (7.44s) và `TestProductionRuntimeFaultIntegration` (kiểm tra các lỗi: probe failure, ENOSPC, lost-ack, killed-reloader, offline-broker).
  - Quét an toàn thông tin (secret scan) trên log của toàn bộ container và đầu ra của adapter: xác nhận không có rò rỉ plaintext secret hay private key.

### 3.4 Phân hệ 4: Bộ Kiểm thử Python Discovery

- **Lệnh thực thi**:
  ```sh
  python3 -m unittest discover -s scripts/tests -p 'test_*.py'
  ```
- **Kết quả**: `PASS` (Ran 58 tests in 1.053s, OK).

### 3.5 Phân hệ 5: Bộ Công cụ Go, Kiểm tra Lỗi Đua, Biên dịch và Linting

- **Kiểm tra Race Condition và Unit Tests**:
  ```sh
  go -C src test -race -count=1 -coverprofile=/tmp/opencode/task2613-unit.cover ./...
  ```
  Kết quả: `PASS` toàn bộ các package trong `src`.
- **Kiểm tra Tĩnh (Go Vet)**:
  ```sh
  go -C src vet ./...
  ```
  Kết quả: `PASS` (không phát hiện cảnh báo nào).
- **Biên dịch Mã nguồn Go (Go Build)**:
  ```sh
  go -C src build ./...
  ```
  Kết quả: `PASS`.
- **Kiểm tra Quy chuẩn Mã nguồn (Golangci-lint)**:
  ```sh
  golangci-lint version
  golangci-lint run --timeout=5m
  ```
  Kết quả: Golangci-lint phiên bản 2.14.0 (built with go1.27.1) báo cáo **`0 issues`**.
- **Biên dịch Docker Images**:
  ```sh
  docker build --target backend -t task2613-backend:local src
  docker build --target credential-controller -t task2613-controller:local src
  ```
  Kết quả: Cả hai mục tiêu Docker build thành công (`PASS`).
- **Kiểm tra Whitespace và Định dạng Git**:
  ```sh
  git diff --check
  ```
  Kết quả: `PASS` (không có lỗi khoảng trắng thừa hay xung đột định dạng).

### 3.6 Dọn dẹp Tài nguyên Thực nghiệm (Resource Cleanup Verification)

Kết quả kiểm tra hậu thực thi (`POST-RUN CHECK`) ghi nhận toàn bộ **9 dự án Docker (credential và runtime)** được sinh ra trong quá trình kiểm thử đã được dọn dẹp triệt để:
- 7 dự án thuộc suite DynSec spike: `dynsec_spike_f3e295fb0c76`, `dynsec_spike_79816389f78d`, `dynsec_spike_de14f080ed5c`, `dynsec_spike_0c3ee23c082e`, `dynsec_spike_bf45340fdf87`, `dynsec_spike_bc816625866b`, `dynsec_spike_4ef66a3804cd`.
- 2 dự án thuộc suite Mosquitto runtime & python test: `mqtt_poc_ebc25ed68740`, `mqtt_poc_06b930eaa299`.
- Xác nhận: 0 container, 0 mạng (networks) và 0 volume tồn dư; các tài nguyên không liên quan trên máy chủ được bảo toàn nguyên vẹn.

---

## 4. Báo cáo Độ bao phủ Mã nguồn (Code Coverage Evidence)

Theo quy định kiểm định nghiêm ngặt, số liệu độ bao phủ mã nguồn (code coverage) phải dựa trên **bằng chứng hợp nhất câu lệnh (statement union) được xác định chính xác theo phạm vi mã nguồn**, tuyệt đối không cộng gộp số học các tỷ lệ phần trăm hoặc tổng hợp số lượng kịch bản kiểm thử.

Trong đợt chạy hồi quy nghiệm thu Task 2.6.13, độ bao phủ hợp nhất toàn cục (cumulative union) qua tất cả các suite không được tính toán lại thành một file profile đơn nhất, do đó được đánh dấu minh bạch là **chưa tính toán lại (not recomputed)** nhằm tránh việc đưa ra số liệu suy diễn sai lệch.

Dưới đây là số liệu độ bao phủ câu lệnh thực tế được đo lường độc lập trên từng profile artifact với mẫu số (denominator) được giới hạn rõ ràng:

### 4.1 Độ bao phủ Kiểm thử Đơn vị Toàn diện (`task2613-unit.cover`)

Lệnh đo lường: `go -C src tool cover -func=/tmp/opencode/task2613-unit.cover`.  
Độ bao phủ toàn bộ dự án (`./...`): **`69.1%` tổng số câu lệnh**.

Chi tiết từng package chủ chốt:
- `iot-platform/internal/mqttcredential`: **68.5%**
- `iot-platform/internal/httpserver`: **93.8%**
- `iot-platform/internal/auth`: **95.8%**
- `iot-platform/internal/config`: **94.6%**
- `iot-platform/internal/database`: **94.3%**
- `iot-platform/internal/httpapi`: **91.4%**
- `iot-platform/internal/mosquittoreload`: **90.1%**
- `iot-platform/internal/digitaltwin/mapper`: **100.0%**
- `iot-platform/cmd/mosquitto-reloader`: **84.6%**
- `iot-platform/cmd/mosquitto-credential-controller`: **29.0%**
- `iot-platform/cmd/mosquitto-auth-init`: **9.4%**
- `iot-platform/cmd/server`: **6.3%** (chỉ đo trong unit scope không kèm integration server)

### 4.2 Độ bao phủ Kiểm thử Tích hợp Độc lập

Các bài kiểm thử tích hợp đo lường trên phạm vi 5 package: `./cmd/server`, `./internal/mqttcredential`, `./internal/httpserver`, `./internal/auth`, `./internal/gateway`:
- **HTTP Integration Suite** (`task2613-http.cover`): **41.1%**
- **Provision Service Integration** (`task2613-provision.cover`): **31.9%**
- **Rotate Service Integration** (`task2613-rotate.cover`): **32.3%**
- **Revoke Service Integration** (`task2613-revoke.cover`): **32.9%**
- **Startup Reconciliation Integration** (`task2613-startup.cover`): **39.5%**
- **Composition Integration** (`task2613-composition.cover`): **10.9%**
- **PostgreSQL Repository Suite** (`task2613-postgres.cover` - đo trên phạm vi `internal/mqttcredential`): **18.9%**

Mỗi profile đại diện cho một lát cắt tích hợp chuyên biệt (vertical slice) tập trung vào nhánh nghiệp vụ và kịch bản lỗi tương ứng.

---

## 5. Phân định Tầng Kiểm chứng Thực nghiệm

Để bảo đảm tính trung thực của báo cáo nghiệm thu, các cấp độ kiểm chứng được phân định rành mạch như sau:

```text
┌────────────────────────────────────────────────────────────────────────┐
│ 1. Source Review                                                       │
│    - Rà soát mã nguồn Mosquitto v2.0.18 (signals.c, plugin.c, loop.c)  │
│    - Rà soát hàm PL/pgSQL và trigger guards (Migration 000015/000016)   │
├────────────────────────────────────────────────────────────────────────┤
│ 2. Unit & Mock Testing                                                 │
│    - go test -race trên internal/mqttcredential, httpserver, config     │
│    - Sử dụng in-memory mock cho database và controller                 │
├────────────────────────────────────────────────────────────────────────┤
│ 3. Isolated PostgreSQL Testing                                         │
│    - scripts/test-task263a-repository.sh                               │
│    - Kết nối container PostgreSQL 17.6 cô lập                          │
│    - Kiểm chứng transaction rollback, advisory lock, và timeout        │
├────────────────────────────────────────────────────────────────────────┤
│ 4. Pinned Native Broker Testing                                        │
│    - Docker container Mosquitto 2.0.18 chính thức (digest được ghim)   │
│    - Kiểm chứng DynSec plugin, stdin bootstrap, SIGHUP reload          │
├────────────────────────────────────────────────────────────────────────┤
│ 5. Actual Standalone Process Testing                                   │
│    - Tiến trình cmd/server độc lập chạy trong container                │
│    - Gửi request HTTP thực tế qua loopback network                     │
│    - Kiểm chứng fail-closed, SIGTERM shutdown và khôi phục sự cố       │
├────────────────────────────────────────────────────────────────────────┤
│ 6. GitHub Actions CI                                                   │
│    - Baseline CI lịch sử: Run 37110877029 (commit b856ecfb...)          │
│    - Uncommitted changes hiện tại: CHƯA CÓ EXACT-SHA CI TRÊN GITHUB     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Giới hạn Nghiệm thu và Cảnh báo Bắt buộc (Required Limitations)

Báo cáo này công bố minh bạch các giới hạn kỹ thuật và các nội dung **chưa được thực hiện (NOT RUN)** theo đúng nguyên tắc của kế hoạch phát triển:

1. **Chưa có Exact-SHA GitHub CI**: Các kết quả kiểm thử trong báo cáo này được thực hiện trên môi trường máy trạm cục bộ sạch (`/tmp/opencode/task2613-final-regression.log`). Do các thay đổi mã nguồn chưa được commit và push theo quy trình được phê duyệt, **hiện tại chưa có GitHub Actions run tương ứng với mã băm commit (exact-SHA) của mã nguồn này**.
2. **Trạng thái Hoàn tất Task 2.6.13**: Task 2.6.13 chỉ được coi là hoàn tất toàn bộ sau khi tài liệu nghiệm thu này được lưu trữ, mã nguồn được commit và push theo đúng quy trình ủy quyền, và pipeline CI trên GitHub trả về kết quả xanh (green).
3. **Cấu hình Mặc định Production**: Cờ cấu hình `MQTT_CREDENTIAL_API_ENABLED` tiếp tục giữ giá trị `false` theo mặc định. Không tự ý kích hoạt cờ này trên production cho đến khi các bài kiểm thử staging được hoàn tất.
4. **Giới hạn Cấu trúc Mạng và Phần cứng Thực tế**: Chưa kiểm chứng topology triển khai thực tế của mô hình một broker/controller đã chọn, reverse proxy Nginx production, đường hầm Rathole thực tế tới Gateway vật lý, hoặc quy trình nạp khóa USB và lưu trữ an toàn trên flash/NVS của các bo mạch ESP32, Luckfox Pico Plus, TI AM5728. Triển khai đa node/HA không thuộc MVP.
5. **Chính sách Mất Database sau khi Mở Cổng (Post-OPEN Database Loss)**: Chính sách ứng xử khi mất kết nối cơ sở dữ liệu sau khi Ingress Gate đã mở (`OPEN`) vẫn ở mức thiết kế fail-closed cho các yêu cầu mới, chưa có cơ chế cô lập tự động broker nếu database gặp sự cố kéo dài khi broker đang phục vụ lưu lượng kết nối trực tiếp.
6. **Các Kịch bản Wire-Loss Không Thực hiện (NOT RUN)**:
   - Kịch bản rớt gói tin HTTP thực tế ngay sau khi transaction đã commit trên database (`dropped HTTP transport-response`) là **NOT RUN** (kiểm thử hủy bỏ kết nối phía caller không tương đương với rớt gói tin tầng mạng).
   - Kịch bản mất gói tin xác nhận commit trên đường truyền SQL (`SQL wire/commit-ACK loss`) là **NOT RUN** (kiểm thử trigger lỗi hoặc ngắt kết nối nhân tạo không phản ánh đầy đủ trạng thái in-flight packet drop).
7. **Bản chất của Quan sát Snapshot**: Quan sát thấy snapshot file được ghi trên broker native (`snapshot_observed = true`) là quan sát mức ứng dụng, **không tương đương với chứng chỉ fsync phần cứng hay chống sập nguồn vật lý (power-loss proof)**.
8. **Bảo đảm Phân phối Thông điệp**: Hệ thống tuân thủ nguyên tắc phân phối ít nhất một lần (at-least-once) kết hợp với tính lũy đẳng cấp ứng dụng (idempotency key và operation ID), **tuyệt đối không cam kết hoặc tuyên bố cơ chế exactly-once delivery**.

---

## 7. Kết luận và Bàn giao sang Task 2.7

### 7.1 Kết luận Nghiệm thu Task 2.6

Phân hệ mã nguồn và bộ kiểm thử của **Task 2.6: Admin MQTT Credential Lifecycle API, DynSec Ingress Gate và PostgreSQL Transactional Operations** đã hoàn thành toàn bộ các yêu cầu kỹ thuật:
- Đã khắc phục triệt để finding duy nhất của đợt review liên quan đến `FinalizeTimeout`.
- Đã hoàn thành bộ suite kiểm thử tích hợp đầy đủ gồm 6 bộ chọn tên, kiểm thử tiến trình standalone `cmd/server`, hồi quy runtime Mosquitto, và kiểm tra công cụ Go/Docker.
- Mọi tài nguyên kiểm thử tạm thời đã được dọn dẹp sạch sẽ, không ảnh hưởng đến môi trường phát triển chung.

### 7.2 Bàn giao sang Task 2.7

- Toàn bộ các API và cơ chế kiểm soát cổng Ingress Gate đã sẵn sàng ở trạng thái kiểm chứng cục bộ (Proof Conditional / Decision Ready).
- Bàn giao phân hệ credential cho **Task 2.7** để tiến hành:
  1. Tích hợp E2E toàn Giai đoạn 2: kết nối luồng xác thực người dùng Supabase Auth, phân quyền thành viên Gateway (`user_gateways`), nạp thông tin xác thực MQTT, và kiểm chứng Gateway gửi telemetry qua broker tới Go backend.
  2. Bổ sung các kịch bản kiểm thử tích hợp với người dùng thực tế và hoàn thiện runbook vận hành tổng thể Stage 2.
  3. Tiến hành các thủ tục commit, phê duyệt push, và kích hoạt gate CI chính thức trên GitHub.
