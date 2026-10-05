# Giai đoạn 2 - Task 2.1: Migration runner và tiến trình schema quản trị (Baseline v9 đến v16)

Tài liệu này ghi nhận kiến trúc migration runner, phân tầng database role và tiến trình phát triển schema quản trị Giai đoạn 2 (Stage 2). Khởi đầu từ Task 2.1 với schema khởi tạo version 10, hệ thống schema quản trị và kiểm soát xác thực MQTT đã hoàn thiện đến phiên bản mới nhất hiện hành là **version 16** (`000016_mqtt_event_authority_guard.up.sql`).

PostgreSQL (TimescaleDB) là nguồn chân lý duy nhất (single source of truth) quản lý quyền platform admin, phân quyền thành viên Gateway, siêu dữ liệu (metadata) credential, chốt chặn bảo trì (maintenance checkpoint), hồ sơ phục hồi bất định và chuỗi sự kiện kiểm toán bất biến (immutable audit events).

---

## 1. Nguồn gốc và quy tắc bất biến lịch sử (History & Provenance)

- **Bối cảnh lịch sử:** Task 2.1 đã thiết lập cơ chế chạy migration tự động có khóa phiên, xác lập baseline version 9 và phân quyền tối thiểu cho backend role.
- **Tiến trình schema Stage 2:** Qua các giai đoạn phát triển tiếp theo (Task 2.4, Task 2.6 và chuẩn bị cho Task 2.7), các yêu cầu siết chặt kiểm soát thế hệ credential, bảo trì tập trung, phục hồi lỗi và kiểm toán ủy quyền hai chiều đã bổ sung tuần tự các migration từ 000011 đến 000016.
- **Quy tắc bất biến lịch sử (Strict History Preservation):**
  - Tuyệt đối **KHÔNG ĐƯỢC CHỈNH SỬA** các file migration lịch sử từ `000001` đến `000016`.
  - Mọi thay đổi schema (nếu phát sinh trong tương lai) bắt buộc phải tạo migration tăng dần mới (`000017+`).
  - Runner không được phép sửa đổi hoặc xóa bỏ các bản ghi kiểm toán lịch sử để giải quyết xung đột dữ liệu cũ.

---

## 2. Thành phần hệ thống migration

1. **Migration Runner:** `scripts/run-migrations.sh` — script quản lý áp dụng các migration từ version 10 trở lên, kiểm tra baseline và khóa chống chạy đua.
2. **Tập tin Schema Migrations (000010 đến 000016):**
   - `migrations/000010_stage2_auth_and_provisioning.up.sql`: Khởi tạo platform admin, quan hệ phân quyền và cấu trúc sơ khai của MQTT credential.
   - `migrations/000011_mqtt_credential_operations.up.sql`: Mở rộng nhật ký thao tác `gateway_mqtt_credential_events` (bổ sung `idempotency_key`, các cờ quan sát RAM/snapshot/positive verification, và cờ phân tách `legacy_projection`).
   - `migrations/000012_mqtt_credential_maintenance.up.sql`: Bổ sung bảng ghi nhận cửa sổ bảo trì `mqtt_credential_maintenance`, lưu trữ checkpoint epoch broker và các pha xử lý (`admitted`, `drained`, `restarted`, `completed`, `failed`).
   - `migrations/000013_mqtt_maintenance_admission.up.sql`: Bổ sung kiểm soát nhập cuộc (admission control) và hàng rào (fence) ngăn ngừa xung đột intent đồng thời trên cùng Gateway hoặc idempotency key.
   - `migrations/000014_mqtt_credential_recovery.up.sql`: Thiết lập bảng quản lý trạng thái phục hồi bất định `mqtt_credential_recovery` (`recovery_id`, `broker_epoch`, `attempted_version`, lý do phục hồi) liên kết với maintenance và events.
   - `migrations/000015_mqtt_recovery_authority_guards.up.sql`: Thiết lập trigger guard kiểm tra tính toàn vẹn thế hệ (`credential_version`), ràng buộc khóa ngoại và logic xác thực dự phóng (projection authority) trên bảng `gateway_mqtt_credentials`.
   - `migrations/000016_mqtt_event_authority_guard.up.sql` (Latest v16): Bổ sung constraint trigger trì hoãn (`DEFERRABLE INITIALLY DEFERRED`) `check_mqtt_event_authority` trên bảng `gateway_mqtt_credential_events`, hoàn thiện kiểm tra authority hai chiều (bidirectional authority guards) tại thời điểm COMMIT giao dịch.
3. **Bộ kiểm tra ngữ nghĩa (Semantic Verifiers):**
   - Bộ SQL verification theo schema: `scripts/sql/verify-migration-000010.sql` đến `scripts/sql/verify-migration-000016.sql`.
   - Bộ SQL verification nâng cấp (upgrade verifiers): `scripts/sql/verify-migration-000011-upgrade.sql`, `scripts/sql/verify-migration-000013-upgrade.sql`, `scripts/sql/verify-migration-000014-upgrade.sql`, `scripts/sql/verify-migration-000015-upgrade.sql`.
4. **Harness kiểm thử tích hợp:** `scripts/test-stage2-migrations.sh` — kiểm tra clean install v16, nâng cấp từ các phiên bản volume cũ, chạy đua đồng thời và khả năng fail-closed.
5. **Công cụ quản trị hạ tầng (Operator Tools):**
   - `scripts/bootstrap-platform-admin.sh`: Khởi tạo platform admin đầu tiên một cách idempotent.
   - Công cụ quản trị membership nội bộ (dự kiến trong Task 2.7) cho các thao tác phân quyền `user_gateways`.
6. **Compose Service Orchestration:** Dịch vụ `application-migrations` chạy tự động trước khi `backend` khởi động và thoát ngay sau khi hoàn thành.

---

## 3. Cơ chế hoạt động của Migration Runner

Runner yêu cầu database đã được baseline hợp lệ đến version 9 (`migration_tracking_and_sensor_urn`). Trong cùng một phiên `psql`, runner thực thi:

1. **Session-Level Advisory Lock:** Giữ khóa `(3290, 2)` bằng `SELECT pg_advisory_lock(3290, 2)` với `statement_timeout = '60s'` trong lúc chờ khóa, sau đó đặt lại timeout về `0` để chạy migration.
2. **Kiểm tra Baseline:** Xác nhận bảng `schema_migrations` tồn tại và đã ghi nhận version 9 với đúng tên `migration_tracking_and_sensor_urn`. Nếu thiếu baseline, runner `RAISE EXCEPTION` và fail-closed lập tức.
3. **Thực thi Idempotent:** Quét tất cả các file `0*.up.sql` có version >= 10. Với từng version:
   - Nếu chưa áp dụng: Thực thi file và ghi nhận version vào `schema_migrations`.
   - Nếu đã áp dụng: Kiểm tra tên migration đã lưu có trùng khớp tuyệt đối với file hay không. Nếu trùng tên, bỏ qua an toàn; nếu khác tên, báo lỗi và dừng toàn bộ quá trình.
4. **Giải phóng khóa:** Giải phóng advisory lock ở cuối script hoặc tự động giải phóng khi phiên kết nối bị ngắt do lỗi cú pháp/ràng buộc.
5. **Thứ tự phụ thuộc khởi động:**
   ```text
   postgres healthy
     -> supabase-role-provisioner
         -> application-migrations
         -> supabase-auth healthy
   application-migrations + supabase-auth healthy
     -> supabase-compat-migration
   application-migrations + supabase-compat-migration
     -> backend
   ```

*Lưu ý:* Migration 1-9 là baseline lịch sử. Volume cũ chưa có `schema_migrations` phải trải qua quy trình preflight và áp dụng baseline version 9 thủ công theo hướng dẫn trong `README.md`; runner tuyệt đối không tự ý suy đoán trạng thái schema không rõ ràng.

---

## 4. Chi tiết Schema và Ràng buộc Nghiệp vụ (v10 đến v16)

### 4.1. Schema 000010: Quản trị và phân quyền nền tảng
- `platform_admins`: Lưu trữ `user_id` tham chiếu `profiles.id`, xác định thẩm quyền quản trị tối cao.
- `gateway_mqtt_credentials`: Bảng projection lưu trạng thái hiện tại của credential trên broker.
- `gateway_mqtt_credential_events`: Bảng nhật ký kiểm toán ghi nhận mọi yêu cầu provision, rotate, revoke.
- Chỉ mục đảo (reverse index): `user_gateways(gateway_id, user_id)` phục vụ tra cứu ngược quyền sở hữu/truy cập.
- Chỉ mục audit: `gateway_mqtt_credential_events(gateway_id, created_at DESC)`.
- Ràng buộc định dạng Identifier cho `gateway_id` và `sensor_id`:
  ```text
  ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$
  ```
- Cấm định danh dành riêng: `backend_service` bị cấm sử dụng làm `gateway_id` vì đây là MQTT principal của backend.

### 4.2. Schema 000011 đến 000014: Vận hành Credential, Bảo trì và Phục hồi
- `gateway_mqtt_credential_events` được mở rộng với `idempotency_key` (UUID), `phase`, `action` (`provision`, `rotate`, `revoke`), `status` (`pending`, `succeeded`, `failed`, `recovery_needed`), và các cờ quan sát độc lập:
  - `ram_applied`: Xác nhận plugin DynSec đã nạp vào bộ nhớ RAM.
  - `snapshot_observed`: Xác nhận tệp snapshot trên đĩa đã ghi nhận thay đổi.
  - `fresh_positive_verified`: Xác nhận kết nối thử nghiệm tích cực bằng mật khẩu mới thành công qua TLS.
  - `legacy_projection`: Cờ đánh dấu các bản ghi cũ từ thời điểm trước khi có hệ thống kiểm toán nâng cao.
- `mqtt_credential_maintenance`: Kiểm soát cửa sổ bảo trì toàn cục. Bắt buộc ghi nhận `operation_id`, `gateway_id`, `broker_epoch` (chuỗi hash SHA256 64 ký tự), và `status` tiến trình (`admitted` -> `drained` -> `restarted` -> `completed` / `failed`).
- `mqtt_credential_recovery`: Lưu vết các ca phục hồi bất định khi quá trình xoay vòng hoặc thu hồi gặp sự cố giữa chừng, đảm bảo không bị mất dấu sự cố hạ tầng.

### 4.3. Schema 000015 & 000016: Ràng buộc Ủy quyền Hai chiều (Bidirectional Authority Guards)
- **Migration 000015:** Tạo trigger guard trên `gateway_mqtt_credentials`. Mọi thay đổi trạng thái hoặc phiên bản credential trong bảng projection bắt buộc phải có bản ghi sự kiện tương ứng trong `gateway_mqtt_credential_events` và checkpoint bảo trì hợp lệ.
- **Migration 000016:** Bổ sung trigger trì hoãn `check_mqtt_event_authority` (`DEFERRABLE INITIALLY DEFERRED`) trên `gateway_mqtt_credential_events`. Khi một sự kiện nghiệp vụ hiện đại được cập nhật hoặc tạo mới, hàm `validate_mqtt_projection_authority(gateway_id)` được kích hoạt tại thời điểm COMMIT:
  - Ngăn chặn việc cập nhật sự kiện thành `succeeded` mà thiếu các cờ quan sát thực tế (`ram_applied`, `snapshot_observed`, `fresh_positive_verified`).
  - Ngăn chặn việc thay đổi metadata credential mà không qua transaction có intent event đi kèm.
  - Ngăn chặn tình trạng nhảy phiên bản credential bất hợp lệ hoặc mâu thuẫn giữa trạng thái nghiệp vụ và checkpoint bảo trì.

---

## 5. Phân tầng Database Role của Backend

Hệ thống tuân thủ nguyên tắc đặc quyền tối thiểu (least privilege):

1. **`iot_backend`:** NOLOGIN role, sở hữu thuộc tính `BYPASSRLS`. Không phải superuser, không có quyền tạo database hoặc role.
2. **`iot_backend_app`:** LOGIN role, kế thừa quyền DML từ `iot_backend` và được cấp `BYPASSRLS` rõ ràng (do thuộc tính này không tự kế thừa qua role membership).
   - Quyết định sử dụng `BYPASSRLS`: Go Backend tự thực hiện toàn bộ kiểm tra quyền User-Gateway trong các câu lệnh parameterized SQL. Các RLS policy chỉ áp dụng cho truy cập trực tiếp từ phía Supabase PostgREST client.

**Giới hạn quyền hạn của `iot_backend_app`:**
- **Không có quyền DDL:** Tuyệt đối không được `CREATE TABLE`, `ALTER TABLE`, hoặc `DROP TABLE`.
- **Không có quyền trên lịch sử migration:** Bị cấm ghi, sửa, xóa trên bảng `schema_migrations`.
- **Quyền DML có chọn lọc:**
  - Chỉ có quyền `SELECT` trên `platform_admins`.
  - Không có quyền xóa dữ liệu kiểm toán: Bị cấm `DELETE` trên bảng `gateway_mqtt_credential_events`.
  - Không có quyền xóa checkpoint: Bị cấm `DELETE` trên bảng `mqtt_credential_maintenance`.
  - Không có quyền sửa đổi raw telemetry hoặc giả mạo dữ liệu lịch sử.

---

## 6. Các phiên bản nâng cấp được hỗ trợ và Kiểm tra Ngữ nghĩa (Supported Upgrades & Semantic Checks)

### 6.1. Ma trận nâng cấp được hỗ trợ (Supported Upgrade Versions)
Trong harness kiểm thử tích hợp (`scripts/test-stage2-migrations.sh`), các đường nâng cấp sau đã được thẩm định tự động:
- **Nâng cấp từ v9 (Baseline):** Nâng cấp tuần tự từ version 9 lên version 16.
- **Nâng cấp từ v10, v13, v14, v15 lên v16:** Được kiểm thử với các bộ dữ liệu seed mô phỏng trạng thái kế thừa (legacy state seeds).
- **Cài đặt mới hoàn toàn (Clean Install):** Khởi tạo từ thư mục migrations đầy đủ trực tiếp đến **v16**.
- **Lưu ý về v11 / v12:** Các đường nâng cấp xuất phát từ v11 hoặc v12 chỉ được bổ sung rehearsal khi có seed và hợp đồng nghiệp vụ rõ ràng; hệ thống không tự tuyên bố đã bao phủ các trạng thái chưa được cung cấp fixture kiểm chứng.

### 6.2. Kiểm tra Ngữ nghĩa Toàn diện (Semantic Checks)
Các script `verify-migration-*.sql` không chỉ đếm số lượng dòng hoặc bảng mà thực hiện các kiểm tra ngữ nghĩa sâu:
1. **Kiểm tra Cấu trúc và Quyền:** Xác minh kiểu dữ liệu từng cột, chỉ mục, ràng buộc CHECK regex, và quyền truy cập DML cụ thể của `iot_backend_app`.
2. **Bảo toàn Dữ liệu Kiểm toán Cũ (Legacy Data Preservation):**
   - Script `verify-migration-000011-upgrade.sql` đối chiếu dữ liệu golden record của các bản ghi sự kiện, credentials, profiles và gateways cũ.
   - **Chống làm giả bằng chứng cũ (Anti-fabrication):** Kiểm tra nghiêm ngặt cấm xuất hiện các bản ghi có tiền tố `legacy11_%` mà lại mang cờ `legacy_projection = false` hoặc có cờ kiểm chứng hiện đại (`ram_applied`, `snapshot_observed`, `fresh_positive_verified`) hay tự gán `idempotency_key`.
3. **Ràng buộc Hàng rào Vận hành Cũ:** Đảm bảo intent hiện đại không được phép bỏ qua các thao tác cũ đang ở trạng thái `pending` chưa giải quyết.
4. **Kiểm tra Trigger Guard Hai chiều v16:** Script `verify-migration-000016.sql` chứng minh:
   - Thao tác chỉ sửa event (event-only) mà không cập nhật projection sẽ bị từ chối tại COMMIT.
   - Thao tác chỉ sửa metadata credential (metadata-only) mà không có audit event tương ứng sẽ bị từ chối tại COMMIT.
   - Cả hai hướng cập nhật đều hợp lệ khi được thực thi đồng bộ trong cùng một transaction ở ranh giới deferred constraint.
5. **Kiểm tra Đua tranh Đồng thời (Concurrency Race Tests):** Thử nghiệm chạy đua giữa 2 tiến trình ghi cùng actor/key trên 2 Gateway khác nhau hoặc 2 key khác nhau trên cùng 1 Gateway; đảm bảo đúng 1 intent được chấp nhận và commit vào cơ sở dữ liệu.

---

## 7. Kế hoạch Rehearsal trên Persistent Volume

- **Mục tiêu:** Kiểm chứng quá trình nâng cấp cơ sở dữ liệu trên Docker persistent volume thực tế, duy trì tính toàn vẹn dữ liệu qua chu kỳ dừng, khởi động lại và tái tạo container database.
- **Quy trình chuẩn dự kiến:**
  1. Khởi tạo volume với dữ liệu baseline hoặc phiên bản cũ được hỗ trợ.
  2. Dừng và xóa container database cũ mà vẫn giữ nguyên volume.
  3. Khởi chạy container database mới gắn kèm volume nói trên.
  4. Thực thi `application-migrations` (chạy `scripts/run-migrations.sh`).
  5. Chạy toàn bộ bộ verifier ngữ nghĩa (`verify-migration-*.sql`).
  6. Thực thi runner lần hai và giả lập hai runner chạy đồng thời để xác nhận tính idempotent trên volume persistent.
- **Tình trạng thực tế (Implementation Status):**
  - Cơ chế kiểm thử trên container tạm với mock baseline đã được tự động hóa trong `scripts/test-stage2-migrations.sh`.
  - **Rehearsal trên persistent volume thực tế vẫn đang trong kế hoạch (still planned where not executed)** theo lộ trình Task 2.7 (Bước 2.7.5); chưa thực hiện trên môi trường deployment cuối cùng.

---

## 8. Ranh giới Phê duyệt và Phân định Nâng cấp DB với Chuyển đổi Runtime (Approval Boundary & DB-Upgrade vs Static-to-DynSec Cutover)

Để đảm bảo an toàn tuyệt đối cho hệ thống, quy trình vận hành phân định rõ ranh giới giữa việc nâng cấp cơ sở dữ liệu và chuyển đổi runtime của broker MQTT:

### 8.1. Tách biệt Nâng cấp DB và Chuyển đổi Broker Runtime
- **Business DB Upgrade:** Quá trình áp dụng migration PostgreSQL (từ v9 lên v16) là một thao tác độc lập, chịu trách nhiệm nâng cấp schema, cài đặt trigger guard và cập nhật cấu trúc dữ liệu kiểm toán.
- **Static-to-DynSec Cutover:** Quá trình chuyển đổi Mosquitto từ cơ chế tệp tĩnh (`password_file` + `acl_file`) sang Dynamic Security plugin (`mosquitto_dynamic_security.so`) là một quy trình vận hành runtime riêng biệt.
- Hai thao tác này **KHÔNG ĐƯỢC** gộp làm một và không được thực hiện ngầm định. Việc nâng cấp DB thành công không đồng nghĩa với việc runtime broker đã chuyển đổi sang DynSec.

### 8.2. Nghiêm cấm Nhập Hash và Bảo toàn Credential Trong suốt (No Hash Import or Transparent Credential Preservation)
- Mosquitto static password file lưu mật khẩu dưới dạng chuỗi băm (PBKDF2/SHA512).
- **Không có cơ chế nhập hash tự động:** Chuỗi băm mật khẩu tĩnh cũ KHÔNG THỂ và KHÔNG ĐƯỢC nhập tự động hoặc chuyển đổi trong suốt sang PostgreSQL hay DynSec.
- **Không dùng tài khoản broker làm authority:** Danh sách tài khoản trong broker không thể tự biến thành dữ liệu ủy quyền trong cơ sở dữ liệu.
- Mọi Gateway khi chuyển sang cơ chế quản lý mới bắt buộc phải trải qua quy trình cấp phát lại (authorized re-provisioning) có thẩm quyền từ platform admin thông qua backend API, tạo mật khẩu ngẫu nhiên mới và thực hiện bàn giao offline (offline handoff) an toàn.

### 8.3. Cấm Làm giả Phương án Xử lý Dữ liệu Kế thừa (No Fabricated Legacy Disposition)
- Đối với các Gateway hoặc bản ghi credential cũ chưa có phương án xử lý (disposition) được hỗ trợ:
  - Hệ thống phải giữ trạng thái đóng (`CLOSED`).
  - Ghi nhận và báo cáo rào cản (blocker) tới platform administrator để xin phê duyệt phương án xử lý thủ công.
  - Tuyệt đối **KHÔNG ĐƯỢC** tự ý tạo ra các bản ghi audit giả (fabricated audit events), không tự điền cờ phục hồi, và không tự ý dỡ bỏ hàng rào bảo vệ (fences).

### 8.4. Ranh giới Phê duyệt (Approval Boundary) và Chính sách Rollback
- Mọi hoạt động nâng cấp database trên môi trường triển khai thực tế và mọi bước trong quy trình chuyển đổi runtime đều phải có sự phê duyệt tường minh (explicit operator approval).
- **Chính sách Rollback khi nâng cấp thất bại:**
  - Rollback dựa trên bản sao lưu (backup) cơ sở dữ liệu đã được xác thực, cấu hình đã lưu và container image cố định.
  - Tuyệt đối **KHÔNG ĐƯỢC** chạy `down.sql` để rollback schema, vì thao tác này sẽ phá hủy chuỗi kiểm toán lịch sử không thể phục hồi.
  - Nếu cần rollback mã nguồn backend về phiên bản cũ: Duy trì schema database ở version hiện tại, tắt tính năng credential bằng cờ `MQTT_CREDENTIAL_API_ENABLED=false`, và giữ nguyên runtime tệp tĩnh của Mosquitto.

---

## 9. Hướng dẫn Cấu hình và Vận hành Cục bộ

### 9.1. Biến môi trường
Tạo mật khẩu an toàn (tối thiểu 128 bit entropy) và cấu hình vào file `.env` ở root repository (không commit `.env` vào Git):

```text
BACKEND_DB_PASSWORD=<random-url-safe-password>
DATABASE_URL=postgres://iot_backend_app:<same-password>@postgres:5432/iot_platform?sslmode=disable
```

### 9.2. Khởi chạy và Áp dụng Migration
Trên volume đã có baseline version 9:

```bash
docker compose up -d --build
```

Dịch vụ `application-migrations` sẽ thực thi `scripts/run-migrations.sh` để nâng cấp database lên phiên bản mới nhất hiện hành (v16) trước khi dịch vụ `backend` được phép khởi chạy.

### 9.3. Khởi tạo Platform Admin đầu tiên (Bootstrap Platform Admin)
Tài khoản người dùng phải được tạo trước trong Supabase Auth để trigger tạo bản ghi trong bảng `profiles`. Sau đó thực thi script bằng thông tin quản trị cơ sở dữ liệu:

```bash
set -a
. ./.env
set +a

export PGHOST=127.0.0.1
export PGUSER="$POSTGRES_USER"
export PGPASSWORD="$POSTGRES_PASSWORD"
export PGDATABASE="$POSTGRES_DB"
export PLATFORM_ADMIN_USER_ID=<supabase-user-uuid>
sh scripts/bootstrap-platform-admin.sh
```

- Script có tính chất idempotent: Chạy lại với cùng một UUID sẽ thành công và không làm thay đổi dữ liệu.
- Script fail-closed nếu UUID chưa tồn tại trong `profiles` hoặc nếu cố tình chỉ định một UUID thứ hai khi đã có admin đầu tiên.
- Việc gán thêm platform admin trong tương lai bắt buộc phải thông qua luồng quản trị có kiểm toán riêng biệt.

### 9.4. Kiểm tra Quyền Hạn Tệp Tin (Permission Preflight)
Các script migration và bootstrap yêu cầu credential quản trị cao nhất của PostgreSQL. Trước khi triển khai trên production:
- Đặt file `.env` về chế độ bảo mật `0600`.
- Đảm bảo các script và file migration không có quyền ghi từ phía group hoặc others.
- Nếu workspace nằm trên mounted filesystem không hỗ trợ quyền Unix, cần di chuyển deployment checkout và file `.env` sang filesystem chuẩn hoặc remount với `uid`, `gid`, `fmask=0177`, `dmask=0077`.

---

## 10. Kiểm thử Xác thực (Verification)

Chạy integration test độc lập cho toàn bộ chu trình migration Stage 2:

```bash
sh scripts/test-stage2-migrations.sh
```

Suite kiểm thử sẽ tự động xác minh:
1. Nâng cấp thành công database từ baseline version 9 lên version 16.
2. Kiểm tra tính an toàn khi 2 runner chạy đồng thời (advisory lock hoạt động chính xác).
3. Kiểm tra tính lặp an toàn khi runner chạy lại nhiều lần (idempotency).
4. Khởi tạo thành công database mới trực tiếp từ bộ migration đến version 16 (clean install).
5. Ràng buộc regex trên `gateway_id`, `sensor_id` và chặn định danh dành riêng `backend_service`.
6. Tính idempotent và loại trừ cạnh tranh của script bootstrap platform admin.
7. Role `iot_backend_app` chỉ có quyền DML hạn chế; bị chặn hoàn toàn khi cố tình tạo bảng (DDL), sửa lịch sử migration, hoặc xóa bản ghi kiểm toán.
8. Trigger guard hai chiều của migration 000015 và 000016 hoạt động chính xác tại ranh giới deferred constraint.
9. Dọn dẹp sạch sẽ toàn bộ container và thư mục tạm sau khi hoàn tất.

---

## 11. Trạng thái Hiện tại và Giới hạn Nghiệm thu (Status & Limitations)

1. **Schema và Verifiers Đã Hoàn Tất:** Toàn bộ chuỗi migration từ 000010 đến 000016 cùng các script kiểm tra ngữ nghĩa tương ứng đã hoàn thiện trong codebase, được kiểm chứng 100% qua `scripts/test-stage2-migrations.sh`.
2. **Các tài liệu khác đang được cập nhật đồng thời:** Các tài liệu kiến trúc liên quan khác trong `docs/backend/` đang được các subagent/tiến trình khác cập nhật đồng bộ cho mốc nghiệm thu Stage 2.
3. **Kiểm thử E2E Task 2.7 đang triển khai:** Kịch bản kiểm thử E2E toàn diện (Task 2.7) kết hợp giữa Auth, Provisioning, Credential API và Runtime Mosquitto đang trong quá trình thực hiện liên tục.
4. **CI Job chưa triển khai:** Job CI mới `stage2-e2e` trên GitHub Actions CI chưa được cấu hình hoàn tất trong luồng CI chính thức.
5. **Rehearsal Persistent Volume:** Thử nghiệm nâng cấp trên volume persistent thực tế vẫn đang trong kế hoạch triển khai của Task 2.7 và chưa được nghiệm thu trên môi trường máy chủ staging/production.
