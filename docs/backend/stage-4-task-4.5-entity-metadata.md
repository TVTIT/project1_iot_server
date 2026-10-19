# Báo Cáo Kỹ Thuật Task 4.5 — Tạo Thực Thể Và Chỉnh Sửa Metadata Có Phân Quyền

**Ngày lập:** 2026-10-17  
**Nhánh:** `develop`  
**Baseline commit:** `42dda31518ca907e14a78b0e54625131c4ca0a43` ("test(digitaltwin): bổ sung native integration tests cho temporal history và tài liệu bàn giao")  
**Trạng thái worktree:** Mã nguồn và kiểm thử Task 4.5 đã hoàn thành tại tầng domain/service/repository/DTO và chưa commit trên nhánh `develop`.  
**Trạng thái Review:** Đợt review ban đầu của `go-reviewer` trả về kết quả **BLOCK** (5 phát hiện kỹ thuật và 1 phát hiện tài liệu). Sau đợt sửa ban đầu, đợt follow-up review mới nhất đã **đóng Findings 1–4**, nhưng đưa ra kết luận **WARNING** với 2 phát hiện mức MEDIUM về code/test đang được các worker xử lý tuần tự (thiếu so sánh `issued_by`/`error_message` của command và `actor_user_id`/`request_id` của event trong snapshot sentinels; và xử lý lỗi tra cứu `auth.users` khi dọn dẹp fixture tránh báo thành công giả), cùng yêu cầu chỉnh lý tài liệu mức LOW về mốc thời gian `statement_timestamp()` và tính đơn điệu. Các sửa đổi tài liệu được hoàn tất tại báo cáo này; hai điểm MEDIUM về code/test đang được worker xử lý tiếp nối, **TUYỆT ĐỐI CHƯA ĐƯỢC PHÊ DUYỆT TRƯỚC (REVIEW-APPROVED)**. Chưa có remote CI hay live mutation HTTP endpoints.

---

## 1. Mục Tiêu & Ranh Giới Kỹ Thuật

Task 4.5 hoàn thiện trọn vẹn nghiệp vụ tạo mới (POST) và chỉnh sửa metadata (PATCH) cho hai loại thực thể `Gateway` và `Sensor` của Digital Twin theo đúng đặc tả kỹ thuật tại [Stage 4 Contract (Mục 7.4–7.5, 10.1–10.6)](../protocols/stage4-read-and-entity-api.md) và [Kế hoạch chi tiết Task 4.5](../../../docs/backend_plan/task_4.5_detail_plan.md):

- **Tái sử dụng luồng Provisioning hiện hữu cho POST:**
  - `POST /v1/digital-twins` phân luồng theo `entity_type` (`Gateway` hoặc `Sensor`), tái sử dụng trực tiếp các transactional provisioner của gói `gateway`: `ProvisioningService.ProvisionGateway` cho Gateway và `AdminOrOwnerSensorProvisioningService.ProvisionSensorForAdminOrOwner` cho Sensor.
  - Tự động sinh NGSI-LD URN chuẩn tắc (`urn:ngsi-ld:Gateway:<gateway_id>` và `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`) qua `digitaltwin/mapper`.
  - Cơ chế Replay chính xác: Nếu thực thể đã tồn tại với thông tin biểu diễn khớp hoàn toàn, trả về thành công với `Created = false` (sẽ được Task 4.6 ánh xạ thành HTTP `200 OK`). Nếu thông tin xung đột hoặc đồ thị bất nhất, từ chối với lỗi xung đột/bất nhất (`ErrProvisioningConflict`, `ErrProvisioningInconsistent`).
- **Cập nhật Metadata có kiểm soát cho PATCH:**
  - `PATCH /v1/digital-twins/{entity_id}` phân tích URN chuẩn tắc từ đường dẫn (đã decode một lần bởi tầng HTTP, không double-decode) và chỉ cho phép cập nhật các trường metadata trong allowlist:
    - `Gateway`: `name` (tùy chọn, chuỗi không rỗng, $\le 256$ byte), `description` (tùy chọn, chuỗi $\le 4096$ byte hoặc explicit `null` để xóa về SQL NULL).
    - `Sensor`: duy nhất `name` (bắt buộc, chuỗi không rỗng, $\le 256$ byte). Thuộc tính `unit` là bất biến (immutable), TUYỆT ĐỐI không cho phép PATCH.
  - **Lưu trữ Gateway Description — ĐIỀU CHỈNH QUAN TRỌNG:**
    - Giá trị `description` của Gateway **CHỈ ĐƯỢC GHI VÀO CỘT `gateways.description`** trong cơ sở dữ liệu quan hệ.
    - Cột `twin_entities.attributes` **TUYỆT ĐỐI GIỮ NGUYÊN**, không ghi `description` vào JSONB attributes.
    - Khi có thay đổi thực tế về `description` (hoặc `name`), mốc thời gian `twin_entities.updated_at` được cập nhật đồng thời bằng `statement_timestamp()` của server để phản ánh thời điểm biến đổi metadata.
  - Cơ chế No-Op: Nếu giá trị PATCH trùng khớp hoàn toàn với dữ liệu hiện tại trong cơ sở dữ liệu dưới khóa dòng, transaction không thực thi bất kỳ câu lệnh SQL `UPDATE` nào, commit an toàn và bảo toàn mốc thời gian `updated_at` cũ của Twin.
- **Ranh giới thực thi & Không có side effects:**
  - Cập nhật metadata không làm thay đổi trạng thái vận hành: `reported_state`, `desired_state`, `reported_version`, `desired_version`, `last_reported_at`, `last_desired_at` được giữ nguyên vẹn.
  - Không thêm/sửa/xóa chuỗi thời gian trong `telemetry` hoặc `twin_temporal_values`.
  - Không sinh lệnh điều khiển (command), không ghi bản ghi `twin_outbox`, không xuất bản MQTT.
  - Không sửa đổi quyền thành viên (`user_gateways`) hay thông tin đăng nhập broker MQTT.
- **Ranh giới Router & Endpoint HTTP (Chưa kích hoạt):**
  - Toàn bộ router, handler HTTP và wiring tại `cmd/server` cho các endpoint mutation này **CHƯA ĐƯỢC KÍCH HOẠT** (hoãn lại Task 4.6).
  - Các mã trạng thái HTTP `201 Created`, `200 OK`, `400`, `403`, `404`, `409`, `413`, `415`, `500`, `503` được mô tả trong tài liệu này là các quy tắc ánh xạ được thiết kế sẵn sàng cho Task 4.6, không phải kết quả kiểm thử trên endpoint HTTP live.

---

## 2. Kiến Trúc & Thiết Kế Nghiệp Vụ

### 2.1. Phân Quyền Theo Mô Hình Tập Trung (Centralized Authorization)

| Thao tác | Đối tượng | Phân quyền yêu cầu | Quy tắc từ chối |
| --- | --- | --- | --- |
| `POST` | `Gateway` | Duy nhất `platform_admins` | Không phải Admin $\rightarrow$ Từ chối `ErrProvisioningForbidden` trước khi kiểm tra tài nguyên. |
| `POST` | `Sensor` | `platform_admins` HOẶC Gateway `owner` | - Gateway cha không tồn tại hoặc Non-member $\rightarrow$ `ErrProvisioningNotFound` (404).<br>- Có membership nhưng role là `operator` hoặc `viewer` $\rightarrow$ `ErrProvisioningForbidden` (403). |
| `PATCH` | `Gateway` | Duy nhất `platform_admins` | Không phải Admin $\rightarrow$ `ErrForbidden` (403) trước khi tra cứu tài nguyên đích. |
| `PATCH` | `Sensor` | `platform_admins` HOẶC Gateway `owner` | - Gateway cha không tồn tại hoặc Non-member $\rightarrow$ `ErrNotFound` (404).<br>- Có membership nhưng role là `operator` hoặc `viewer` $\rightarrow$ `ErrForbidden` (403).<br>- Sensor không tồn tại trên Gateway $\rightarrow$ `ErrNotFound` (404). |

*Lưu ý quan trọng:*
- Toàn bộ việc kiểm tra quyền Platform Admin và quyền thành viên `owner` đều được thực thi bằng câu lệnh SQL bên trong cùng transaction nghiệp vụ dưới khóa hàng thích hợp. Tuyệt đối không kiểm tra quyền ngoài transaction để tránh race condition (TOCTOU).
- Phân biệt rõ ràng giữa `actor` (UUID của người thực hiện request từ JWT) và `owner_user_id` (trường trong body của POST Gateway chỉ định chủ sở hữu được gán cho Gateway mới). Việc truyền `owner_user_id` trong body không mang lại quyền thực thi cho actor.

### 2.2. Ngữ Nghĩa Input Presence-Aware & Kiểm Tra Dữ Liệu

Gói `internal/digitaltwin` và `internal/httpserver` triển khai cấu trúc dữ liệu nhận biết sự hiện diện của trường (presence-aware):
- `PatchGatewayInput`:
  - `Name *string`: `nil` nghĩa là omitted (không cập nhật). Khi có giá trị, phải là chuỗi không rỗng sau khi trim, độ dài $\le 256$ byte, UTF-8 hợp lệ, không chứa ký tự NUL. Chuỗi gốc được lưu nguyên vẹn (không tự ý trim khoảng trắng).
  - `DescriptionSet bool`: Đánh dấu trường `"description"` có xuất hiện trong JSON payload hay không.
  - `Description *string`: Nếu `DescriptionSet == true` và `Description == nil`, đại diện cho việc xóa mô tả về SQL `NULL`. Nếu `Description != nil`, độ dài $\le 4096$ byte, UTF-8 hợp lệ, không chứa NUL.
- `PatchSensorInput`:
  - `Name *string`: Bắt buộc phải có (non-nil), chuỗi không rỗng sau khi trim, độ dài $\le 256$ byte.
- **Từ chối Input không hợp lệ:**
  - Payload rỗng `{}` hoặc thiếu toàn bộ trường cần sửa bị từ chối với `ErrInvalidInput`.
  - Body chứa các trường lạ (unknown keys), trường inject (`entity_id`, `entity_type`, `unit`, `created_at`, `updated_at`, `state`, `attributes`, `relationships`, `owner_user_id`, v.v.) đều bị bộ phân tích JSON strict của `httpserver` từ chối với HTTP 400.
  - Body vượt quá giới hạn `ADMIN_MAX_BODY_BYTES` (16 KiB) bị từ chối với HTTP 413. Header `Content-Type` không phải `application/json` bị từ chối với HTTP 415.

### 2.3. Thứ Tự Khóa (Lock Order) & Điều Chỉnh Chế Độ Khóa Ngăn Chặn Deadlock

Để đảm bảo tính nhất quán và loại bỏ nguy cơ deadlock khi chạy đồng thời với luồng nạp telemetry (`Persist`) hoặc luồng provisioning, thứ tự khóa hàng trong cơ sở dữ liệu được quy định nghiêm ngặt:

#### Thứ tự khóa của Gateway PATCH (`PostgresGatewayMetadataPatcher`):
1. Bắt đầu transaction (`ReadCommitted`).
2. Kiểm tra `platform_admins` bằng `SELECT 1 FROM public.platform_admins WHERE user_id = $1` (thất bại $\rightarrow$ `ErrForbidden`).
3. Khóa hàng Gateway trong `public.gateways` bằng `SELECT ... FOR UPDATE`.
4. Khóa thực thể Twin trong `public.twin_entities` bằng `SELECT ... FOR UPDATE OF e` kết hợp kiểm tra tính toàn vẹn của đồ thị (số lượng thực thể Gateway của gateway này đúng bằng 1, state tồn tại trong `twin_states`, và tên khớp giữa hai bảng).
5. Thực thi no-op check hoặc ghi cập nhật nguyên tử $\rightarrow$ Commit.

#### Thứ tự khóa của Sensor PATCH (`PostgresSensorMetadataPatcher`) — ĐIỀU CHỈNH QUAN TRỌNG:
1. Bắt đầu transaction (`ReadCommitted`).
2. Kiểm tra `platform_admins` bằng câu lệnh SQL trên transaction.
3. **Khóa Gateway cha trong `public.gateways` bằng `SELECT name FROM public.gateways WHERE gateway_id = $1 FOR SHARE`:**
   - *Lý do kỹ thuật điều chỉnh:* Trong kế hoạch ban đầu, bước này dự kiến dùng `FOR UPDATE`. Tuy nhiên, trong quá trình kiểm thử native đồng thời với luồng telemetry ingestion (`Persist`), luồng telemetry giữ khóa hàng Sensor (`public.sensors FOR UPDATE`) và khi kiểm tra khóa ngoại (FK check) ngầm định của PostgreSQL, engine cần một khóa `FOR KEY SHARE` trên `public.gateways`. Nếu Sensor PATCH giữ `FOR UPDATE` trên Gateway và sau đó yêu cầu `FOR UPDATE` trên Sensor, một chu kỳ deadlock điển hình sẽ xảy ra. Việc chuyển sang khóa **`FOR SHARE`** trên Gateway cha vừa bảo vệ Gateway không bị xóa/thay đổi khóa chính trong suốt transaction, vừa hoàn toàn tương thích với khóa ngoại của telemetry ingestion, loại bỏ nguy cơ deadlock trên lịch trình đại diện này.
4. Nếu actor không phải admin: Khóa hàng membership trong `public.user_gateways` bằng `SELECT role ... FOR SHARE` (thiếu $\rightarrow$ `ErrNotFound`, role $\neq$ `owner` $\rightarrow$ `ErrForbidden`).
5. Khóa hàng Sensor đích trong `public.sensors` bằng `SELECT name, unit ... FOR UPDATE`.
6. Xác minh Gateway Twin cha (`has_state = true`, đúng 1 thực thể Gateway).
7. Khóa thực thể Sensor Twin trong `public.twin_entities` bằng `SELECT ... FOR UPDATE OF e` và xác minh quan hệ `hasSensor` trong `public.twin_relationships` cùng state trong `twin_states`.
8. Thực thi no-op check hoặc ghi cập nhật nguyên tử $\rightarrow$ Commit.

#### Thứ tự khóa của Sensor POST Replay (`ProvisionSensorForAdminOrOwner`):
- Luồng tạo mới hoặc replay chính xác Sensor qua `ProvisionSensorForAdminOrOwner` trong `internal/gateway/postgres_sensor_provisioning.go` cũng áp dụng khóa Gateway cha bằng **`SELECT name FROM public.gateways WHERE gateway_id = $1 FOR SHARE`**.
- *Lý do:* Tương tự Sensor PATCH, nếu POST replay lấy `FOR UPDATE` trên Gateway cha rồi sau đó chờ khóa dòng Sensor, trong khi luồng telemetry `Persist` giữ khóa dòng Sensor và chờ `KEY SHARE` trên Gateway cha (qua FK check của telemetry/processed_messages), chu kỳ deadlock `40P01` sẽ xảy ra. Chuyển sang `FOR SHARE` loại bỏ hoàn toàn chu kỳ này vì `FOR SHARE` tương thích với `KEY SHARE`, trong khi vẫn ngăn chặn Gateway cha bị sửa đổi/xóa bởi các transaction khác.
- Các luồng PUT Sensor hiện hữu từ Stage 2 (`ProvisionSensor` trong `OwnerSensorProvisioningService`) được giữ nguyên không đổi (`FOR UPDATE`).

*Lưu ý:* Báo cáo ghi nhận giải pháp đã giải quyết deadlock trên các lịch trình kiểm thử đại diện giữa Sensor PATCH/POST Replay và Telemetry Ingestion, nhưng không tuyên bố rằng mọi lịch trình tương tranh tùy ý trong hệ thống phân tán đều miễn nhiễm với deadlock. Gateway PATCH vẫn tiếp tục sử dụng `FOR UPDATE` trên Gateway vì nó trực tiếp biến đổi thông tin của Gateway.

### 2.4. Tính Nguyên Tử Khi Ghi & Phục Hồi Dữ Liệu (Atomicity & Rollback)

- Khi `name` thay đổi: Cả hai bảng (`gateways`/`sensors` và `twin_entities`) được cập nhật đồng thời trong transaction. Mốc thời gian `twin_entities.updated_at` được gán bằng `statement_timestamp()` của PostgreSQL (thay vì `CURRENT_TIMESTAMP`), được tính tại thời điểm câu lệnh SQL UPDATE thực thi sau khi đã nắm giữ khóa hàng. Điều này ngăn chặn việc `updated_at` bị thụt lùi (regress) nếu transaction phải xếp hàng chờ khóa sau một transaction khác đã commit.
- Khi `description` của Gateway thay đổi: Chỉ có `gateways.description` được cập nhật, và `twin_entities.updated_at` được cập nhật bằng `statement_timestamp()`. Bảng `twin_entities.attributes` giữ nguyên vẹn.
- Khi same-value PATCH (no-op): Không chạy câu lệnh SQL UPDATE, commit an toàn và bảo toàn mốc thời gian `updated_at` cũ của Twin.
- *Lưu ý về mốc thời gian:* `statement_timestamp()` được đánh giá tại câu lệnh SQL UPDATE metadata cuối cùng sau khi đã sở hữu các khóa dòng, giúp tránh mốc thời gian bắt đầu transaction cũ (stale transaction-start time) trong lịch trình tuần tự hóa đã kiểm thử. Hệ thống không cam kết tính đơn điệu theo đồng hồ thực (wall-clock monotonicity) hay tính đơn điệu phân tán (distributed monotonicity).
- Kiểm thử Native Rollback xác minh rằng: nếu câu lệnh cập nhật `twin_entities` gặp lỗi SQL thật (ví dụ lỗi SQLSTATE `42P01` do decorator giả lập lỗi bảng không tồn tại), toàn bộ transaction sẽ bị rollback hoàn toàn. Bảng quan hệ (`gateways.name` / `gateways.description`) không bị thay đổi dở dang, và snapshot dữ liệu được bảo toàn tuyệt đối.
- Khi xảy ra lỗi mất kết nối trong quá trình commit (`tx.Commit`), hệ thống trả về lỗi phân loại `ErrMetadataPatchUnavailable` (hoặc `ErrGatewayProvisioningUnavailable`/`ErrAdminOrOwnerProvisioningUnavailable`), phản ánh trạng thái commit không chắc chắn (commit-uncertain) chứ không tuyên bố là dữ liệu đã chắc chắn rollback.

### 2.5. Chi Tiết Năm Điểm Sửa Chữa Mã Nguồn & Kiểm Thử Sau Review BLOCK Ban Đầu

Sau khi đợt review ban đầu của `go-reviewer` đưa ra kết luận BLOCK trên 5 điểm kỹ thuật và 1 điểm tài liệu (Finding 6), các worker đã hoàn thành đầy đủ 5 nội dung sửa chữa mã nguồn và kiểm thử tương ứng:

1. **Finding 1 — Hạch toán WaitGroup và giải phóng context trước Bounded Join:**
   - *Vấn đề:* Trong test harness đồng thời của Gateway PATCH, việc quản lý `sync.WaitGroup` chưa hạch toán độc lập theo từng goroutine được khởi chạy thực tế, dẫn đến nguy cơ deadlock hoặc rò rỉ đơn vị WaitGroup nếu transaction đầu tiên gặp lỗi sớm trước khi phát tín hiệu khóa hàng.
   - *Sửa chữa:* Helper `startGatewayPatch` thực hiện `wg.Add(1)` chính xác cho từng goroutine được tạo. Đồng thời, đăng ký `defer boundedWaitGroupJoin(...)` trước `defer cancel(...)` để đảm bảo context luôn được hủy trước khi hàm join bắt đầu chờ goroutine kết thúc. Bổ sung kịch bản regression `Concurrent_Gateway_PATCH_First_Tx_Failure_Before_Lock_Signal_Bounded` kiểm chứng việc hủy điều phối sớm không làm rò rỉ đơn vị WaitGroup nào.
2. **Finding 2 — Chu kỳ Deadlock giữa Sensor POST Replay và Telemetry Ingestion:**
   - *Vấn đề:* Hàm tạo/replay Sensor kết hợp `ProvisionSensorForAdminOrOwner` ban đầu lấy khóa `FOR UPDATE` trên Gateway cha. Khi chạy đồng thời với `telemetry.Persist` (giữ khóa dòng Sensor và kiểm tra khóa ngoại FK `KEY SHARE` trên Gateway), engine PostgreSQL phát hiện chu kỳ khóa và trả về lỗi deadlock SQLSTATE `40P01`.
   - *Sửa chữa:* Đổi chế độ khóa Gateway cha trong `ProvisionSensorForAdminOrOwner` sang `FOR SHARE`. Chế độ này tương thích với `KEY SHARE` của telemetry, loại bỏ hoàn toàn chu kỳ deadlock. Giữ nguyên các luồng PUT Stage 2 hiện hữu (`ProvisionSensor`). Bổ sung kịch bản regression `Sensor_POST_Exact_Replay_Concurrent_With_Telemetry_Persist` (từng tái hiện lỗi `40P01` trước khi sửa và đã PASS hoàn toàn sau khi sửa).
3. **Finding 3 — Cập nhật `updated_at` bằng `statement_timestamp()` sau khi có khóa dòng:**
   - *Vấn đề:* Việc sử dụng `CURRENT_TIMESTAMP` khiến PostgreSQL lấy thời điểm bắt đầu transaction (`transaction_timestamp()`). Khi hai transaction tương tranh, transaction A bắt đầu trước nhưng phải chờ khóa hàng do transaction B nắm giữ; sau khi B commit với timestamp mới, transaction A cập nhật sẽ ghi đè `updated_at` bằng mốc thời gian bắt đầu của A, làm mốc thời gian bị thụt lùi (regress).
   - *Sửa chữa:* Chuyển toàn bộ các câu lệnh SQL UPDATE `twin_entities` (Gateway đổi tên/mô tả, Sensor đổi tên) sang dùng `statement_timestamp()`, được đánh giá tại thời điểm câu lệnh UPDATE thực thi sau khi đã sở hữu khóa hàng. Same-value no-op không chạy UPDATE và giữ nguyên timestamp cũ. Bổ sung kịch bản regression `PATCH_Metadata_UpdatedAt_Is_PostLock_Statement_Time` (từng fail trước khi sửa và đã PASS sau khi sửa).
4. **Finding 4 — Phân loại hẹp lỗi khả dụng tại ranh giới Gateway Provisioning:**
   - *Vấn đề:* `classifyGatewayProvisioningError` cần phân biệt chính xác lỗi kết nối/hạ tầng (503 Service Unavailable) với lỗi lập trình/ràng buộc giải mã dữ liệu (`pgx.ScanArgError`, cần giữ nguyên lỗi để trả về 500 Internal Server Error).
   - *Sửa chữa:* Kiểm tra `errors.As(err, &scan)` cho `pgx.ScanArgError` để không ngụy trang lỗi giải mã thành lỗi hạ tầng; chỉ ủy quyền lỗi mạng/context cho `ClassifyPostgresAvailabilityError(err, ErrGatewayProvisioningUnavailable)`. Đã chạy lại cô lập và PASS toàn bộ 5 guarded suites trong `scripts/test-stage2-provisioning.sh`.
5. **Finding 5 — Kiểm tra lỗi truy vấn Snapshot và so sánh nội dung Sentinels cùng lực lượng:**
   - *Vấn đề:* Bộ kiểm tra sentinels cần bắt được lỗi khi truy vấn/scan snapshot, và bộ so sánh không được chỉ kiểm tra số lượng dòng (count-only) mà phải so sánh chi tiết nội dung (content) để phát hiện biến đổi cùng lực lượng (same-cardinality).
   - *Sửa chữa & Ranh giới đối soát:* Bổ sung unit tests `TestMetadataSnapshotAcquisitionErrorPropagation` và `TestMetadataSnapshotContentComparatorDetectsMutation`. Bổ sung hàm `controlCredentialContentEqual` so sánh nội dung các trường trọng yếu trong `Commands`, `Outbox`, `Credentials`, `CredentialEvents`. Tái khẳng định bất biến: Gateway description CHỈ ghi vào `gateways.description`, cột `twin_entities.attributes` tuyệt đối giữ nguyên.
   - *Lưu ý về phạm vi audit snapshot:* Không tuyên bố bộ so sánh phát hiện được mọi biến đổi trường đơn lẻ hay toàn bộ trường kiểm toán tùy ý. Đợt follow-up review đã chỉ ra 2 phát hiện mức MEDIUM đang được code worker tiếp tục xử lý: bổ sung các trường audit còn thiếu trong snapshot sentinels (`issued_by`/`error_message` cho command, `actor_user_id`/`request_id` cho credential event) và bẫy lỗi dọn dẹp fixture `auth.users` tránh false-success.

---

## 3. Bằng Chứng Kiểm Thử Kế Thừa (Inherited Verification Evidence)

Toàn bộ các kết quả kiểm thử dưới đây được kế thừa nguyên trạng từ phiên làm việc của các worker tiền nhiệm, thực thi trên môi trường test dùng chung (PostgreSQL 16.15 / TimescaleDB 2.29.2, schema 20, role ứng dụng phi-superuser `iot_backend_app` cho các thao tác nghiệp vụ, role `postgres` cho setup/teardown fixture). Báo cáo này ghi nhận trung thực các kết quả kế thừa, **không tuyên bố đã tự chạy lại các lệnh này**:

### 3.1. Mười Bốn Kịch Bản Kiểm Thử Native Integration (`TestNativeEntityMetadata_MutationSlice`)

Lệnh thực thi lựa chọn chính xác suite native:
```bash
TWIN_METADATA_TEST_NATIVE=1 go test -race -v -count=1 -run ^TestNativeEntityMetadata_MutationSlice$ ./internal/digitaltwin
```
Bao gồm đầy đủ 14 kịch bản subtest native từ mã nguồn `src/internal/digitaltwin/postgres_metadata_integration_test.go`:

1. `Admin_POST_Gateway_Create_Replay_And_Conflict`:
   - Admin tạo Gateway thành công: tạo dòng quan hệ trong `gateways`, membership `owner` trong `user_gateways`, thực thể `twin_entities` kiểu `Gateway` với URN chuẩn tắc, và state ban đầu trong `twin_states`.
   - Replay chính xác với cùng body trả về `Created = false` và giữ nguyên timestamp/state.
   - Gửi yêu cầu với `name` khác hoặc `owner_user_id` khác dẫn đến lỗi xung đột `ErrProvisioningConflict`.
2. `Admin_And_Owner_POST_Sensor_Isolation_Replay_And_Conflict`:
   - Cả Platform Admin (không cần membership) và Gateway Owner đều tạo được Sensor thành công, thiết lập quan hệ `hasSensor` và state tương ứng.
   - Cô lập tài nguyên: Sensor cùng tên `sensor_id` trên hai Gateway khác nhau (`gw_meta_a` và `gw_meta_b`) tồn tại hoàn toàn độc lập, không xung đột URN hay dữ liệu.
   - Replay chính xác thành công (`Created = false`); đổi `unit` hoặc `name` sinh lỗi xung đột `ErrProvisioningConflict`.
3. `Gateway_PATCH_Fields_Clear_And_NoOp_Semantics`:
   - Admin PATCH đổi `name` $\rightarrow$ cập nhật đồng thời `gateways.name` và `twin_entities.name`, `updated_at` tiến lên.
   - Admin PATCH đổi `description` $\rightarrow$ chỉ cập nhật `gateways.description`, `twin_entities.attributes` giữ nguyên, `updated_at` tiến lên.
   - Admin PATCH xóa description (`"description": null`) $\rightarrow$ `gateways.description` chuyển thành SQL `NULL`.
   - Same-value PATCH (gửi lại đúng giá trị hiện tại) $\rightarrow$ No-op, không chạy câu lệnh SQL UPDATE, giữ nguyên `updated_at` cũ.
4. `Sensor_PATCH_Fields_And_NoOp_Semantics`:
   - Admin và Owner PATCH đổi `name` thành công, cập nhật đồng thời cả hai bảng và cập nhật `updated_at`.
   - Same-value PATCH cho Sensor $\rightarrow$ No-op, giữ nguyên mốc thời gian `updated_at` cũ.
5. `POST_After_PATCH_Conflict_And_Current_Replay`:
   - Sau khi Gateway hoặc Sensor đã bị PATCH đổi metadata (ví dụ đổi tên), việc gửi lại request POST với body ban đầu sẽ bị từ chối với lỗi `ErrProvisioningConflict` (không cho phép ghi đè).
   - Nếu gửi POST với body chứa metadata mới nhất sau PATCH, hệ thống chấp nhận là replay chính xác hợp lệ.
6. `Authorization_Denials_And_Data_Preservation`:
   - Gateway POST/PATCH bởi Non-admin bị từ chối `ErrProvisioningForbidden` / `ErrForbidden`.
   - Sensor POST/PATCH bởi Operator hoặc Viewer bị từ chối với `ErrProvisioningForbidden` / `ErrForbidden`.
   - Sensor POST/PATCH bởi Non-member bị từ chối với `ErrProvisioningNotFound` / `ErrNotFound` (ngăn lộ sự tồn tại của Gateway).
   - So sánh snapshot trước và sau các yêu cầu bị từ chối xác nhận không có bất kỳ dòng dữ liệu nào bị thêm, sửa, hoặc xóa ngoài ý muốn.
7. `Inconsistent_Graph_And_State_Rejection`:
   - Đồ thị bị phá vỡ nhân tạo (ví dụ xóa mất `twin_states`, lệch tên giữa `sensors` và `twin_entities`, hoặc mất quan hệ `hasSensor`) bị hệ thống phát hiện và từ chối fail-closed với `ErrInconsistentGraph` / `ErrProvisioningInconsistent`.
   - Hệ thống tuyệt đối không tự ý "sửa chữa" (repair) đồ thị hỏng khi thực hiện PATCH.
8. `Provisioning_Resource_Isolation_And_Sentinels`:
   - Xác minh toàn diện các sentinels: POST và PATCH không sinh lệnh điều khiển trong `twin_commands`, không chèn dòng vào `twin_outbox`, không sửa đổi credential events, không sinh bản ghi thừa trong `user_gateways`.
9. `Gateway_PATCH_Atomic_Rollback_On_Real_SQL_Failure`:
   - Sử dụng decorator giả lập lỗi SQL `42P01` ở bước cập nhật bảng `twin_entities` (sau khi đã cập nhật `gateways`).
   - Kết quả: Toàn bộ transaction bị rollback, tên và mô tả trong `gateways` được hoàn nguyên về giá trị cũ, snapshot trước và sau thất bại hoàn toàn trùng khớp.
10. `Sensor_PATCH_Concurrent_With_Telemetry_Persist_And_Subsequent_Batch`:
    - Thực thi đồng thời Sensor PATCH và hai batch nạp telemetry (`telemetry.Persist`).
    - Cả hai luồng hoàn tất thành công trong budget mà không bị deadlock (nhờ Gateway được khóa `FOR SHARE` thay vì `FOR UPDATE`).
    - Dữ liệu thu được: 25 mẫu raw, 2 dòng ledger, 2 bản ghi temporal quan sát, `reported_version` tiến lên 2, trong khi `desired_state`, `unit` và các metadata được bảo toàn trọn vẹn. Batch nạp tiếp theo sau PATCH vẫn hoạt động hoàn hảo.
11. `Sensor_POST_Exact_Replay_Concurrent_With_Telemetry_Persist`:
    - Kiểm thử tương tranh giữa POST Sensor exact replay và telemetry `Persist`.
    - Trước khi sửa (dùng `FOR UPDATE` trên parent Gateway), PostgreSQL phát hiện chu kỳ khóa ngoại và báo lỗi deadlock SQLSTATE `40P01`.
    - Sau khi sửa (dùng `FOR SHARE` trên parent Gateway trong `ProvisionSensorForAdminOrOwner`), hai luồng tuần tự hóa hoàn hảo mà không deadlock: telemetry nạp 12 raw samples, 1 ledger row, 1 temporal observation, `reported_version` tiến lên 1, `desired_state` không bị reset, và exact replay trả về `Created = false` thành công.
12. `Concurrent_Gateway_PATCH_Field_Preservation_And_Same_Field_Last_Write_Wins`:
    - Hai transaction PATCH Gateway khác trường (Tx1 đổi `name`, Tx2 đổi `description`) chạy đồng thời: cả hai trường đều được ghi nhận mà không làm mất trường omitted của nhau.
    - Hai transaction PATCH cùng trường (`name`): cơ chế tuần tự hóa hàng áp dụng Last-Write-Wins có thể dự đoán được, kết thúc với giá trị của transaction commit sau cùng.
13. `Concurrent_Gateway_PATCH_First_Tx_Failure_Before_Lock_Signal_Bounded`:
    - Kịch bản regression cho Finding 1: Transaction 1 gặp lỗi Scan giả lập tại câu lệnh `SELECT ... FOR UPDATE` trước khi phát tín hiệu đã lấy khóa.
    - Điều phối viên hủy và không khởi chạy transaction 2. Hàm `boundedWaitGroupJoin` hoàn thành ngay lập tức (dưới 5ms) mà không bị treo hay rò rỉ đơn vị `sync.WaitGroup`.
14. `PATCH_Metadata_UpdatedAt_Is_PostLock_Statement_Time`:
    - Kịch bản regression cho Finding 3: Transaction A bắt đầu trước nhưng tạm dừng trước khi lấy khóa dòng; transaction B bắt đầu sau, lấy khóa, cập nhật và commit; transaction A sau đó tiếp tục, lấy khóa vừa được giải phóng, cập nhật và commit.
    - Với `statement_timestamp()`, mốc thời gian `updated_at` của transaction A (commit sau) phản ánh đúng thời điểm câu lệnh UPDATE thực thi sau khi đã có khóa, không bị thụt lùi (regress) sau mốc thời gian đã commit của B. Kịch bản xác minh thành công cho cả Gateway PATCH và Sensor PATCH.

Ngoài ra, hai bài unit test trong cùng file kiểm chứng tính toàn vẹn của harness snapshot:
- `TestMetadataSnapshotAcquisitionErrorPropagation`: Kiểm chứng các lỗi truy vấn/scan snapshot được lan truyền và fail-fast, không trả về snapshot một phần rỗng.
- `TestMetadataSnapshotContentComparatorDetectsMutation`: Kiểm chứng bộ so sánh nội dung phát hiện biến đổi trên các trường sentinel trọng yếu được theo dõi dù có cùng lực lượng dòng (same-cardinality). Không tuyên bố bộ so sánh tự động bao phủ mọi trường kiểm toán (các trường `issued_by`/`error_message` của command và `actor_user_id`/`request_id` của event đang được bổ sung bởi code worker).

### 3.2. Nguồn Gốc Bằng Chứng Khóa & Giới Hạn Dọn Dẹp Fixture

- **Nguồn gốc bằng chứng khóa (Lock Evidence Provenance):**
  - Trong các bài kiểm tra tương tranh, câu lệnh `SELECT count(*) FROM pg_locks WHERE NOT granted` được dùng để thăm dò xem PostgreSQL engine có ghi nhận ít nhất một transaction đang bị block chờ khóa hay không.
  - Cần ghi nhận giới hạn trung thực: `SELECT count(*) FROM pg_locks WHERE NOT granted` là một metric **toàn cục** của PostgreSQL instance, **KHÔNG PHẢI** bằng chứng được giới hạn theo fixture hay participant cụ thể.
  - Bằng chứng thực sự chứng minh hai transaction chạy đè lên nhau (overlap) và xảy ra tương tranh đến từ sự điều phối tất định qua Go channels và fault-injecting/pause decorators (`pauseBeforeLockDB`, `newPauseBeforeLockDB`, `telemetrySensorLockHeldP`, `replayAttemptingSensorLockP`, v.v.). Các assertion này kết hợp với channel signaling để xác nhận trạng thái tranh chấp một cách chặt chẽ mà không phụ thuộc đơn độc vào số đếm toàn cục.
- **Phạm vi dọn dẹp fixture (Per-run Cleanup):**
  - Quy trình dọn dẹp (`performMetadataFixtureCleanup`) chỉ dọn dẹp các tài nguyên thuộc về phiên chạy hiện tại (`f.RunID`), không dọn dẹp toàn bộ cơ sở dữ liệu dùng chung.
  - Dữ liệu thử nghiệm cũ từ các phiên trước (ví dụ Run ID `861eb9b2` của Task 4.4) nằm ngoài phạm vi dọn dẹp của fixture Task 4.5 và được giữ nguyên.
  - Worker gần nhất đã dọn dẹp phần dư của run lỗi `da67664b` sau khi sửa đúng thứ tự dọn dẹp khóa ngoại. Tuyệt đối không tuyên bố "toàn bộ DB test trống rỗng".
  - Thứ tự dọn dẹp thực tế: `gateway_mqtt_credentials` phải được xóa **TRƯỚC** `gateway_mqtt_credential_events` vì khóa ngoại `fk_gateway_mqtt_credentials_last_operation` trỏ từ credentials sang events. Sentinels không tạo mật khẩu hay hash thực sự. Báo cáo không chứa bất kỳ secret hay thông số triển khai môi trường nào.
  - Phiên chạy kiểm thử mới nhất ghi nhận dọn dẹp sạch sẽ 0 dòng còn sót cho run `5e01d786` (trước đó là run `b66d04c3`).

### 3.3. Các Bộ Kiểm Thử Hồi Quy & Công Cụ Đã Vượt Qua (Inherited Evidence)

Toàn bộ các kết quả kiểm thử dưới đây là bằng chứng kế thừa từ các worker thực thi trước đó:
- **Go Race Tests:** `go test -race` trên các gói `digitaltwin`, `gateway`, `httpserver`, `telemetry`, `config` PASS.
- **Go Vet & Format:** `go vet ./...`, `gofmt` và `git diff --check` PASS (không có lỗi cú pháp hay định dạng).
- **Linter:** `golangci-lint run` hoàn tất với 0 issue.
- **Biên dịch:** `go build ./...` PASS.
- **Python Test Suite:** Toàn bộ bộ kiểm thử Python (`unittest discover -s scripts/tests`) vượt qua 792/792 tests PASS (kế thừa từ checkpoint Task 4.4).
- **Kiểm thử Native Cô lập (Isolated Shell Scripts):**
  - `sh scripts/test-stage2-provisioning.sh`: 5 guarded native suites PASS (đã được chạy lại trong quá trình sửa Finding 4).
  - `sh scripts/test-stage2-authorization.sh`: 4 suites PASS (kế thừa).
- **Kiểm thử Native Đã Có Trên DB Chia Sẻ:**
  - Twin Read (Task 4.3): 5 suites PASS (kế thừa).
  - Temporal History (Task 4.4): 8 subtests PASS (kế thừa).
  - Metadata POST/PATCH (Task 4.5): 14 kịch bản subtests của `TestNativeEntityMetadata_MutationSlice` PASS.

---

## 4. Danh Mục Lỗi & Ký Hiệu Domain Thực Tế

Tránh nhầm lẫn với các tên lỗi giả định trong các báo cáo sơ bộ trước đây (như `ErrBadRequest` hay `ErrConflict`), hệ thống sử dụng chính xác các ký hiệu domain sentinel sau:

- **Trong gói `internal/digitaltwin`:**
  - `ErrNilActor`: Actor truyền vào là `uuid.Nil`.
  - `ErrNotFound`: Không tìm thấy thực thể hoặc người dùng không có quyền truy cập (`user_gateways`).
  - `ErrInvalidInput`: Tham số đầu vào không hợp lệ (chuỗi rỗng, vượt độ dài, thiếu trường bắt buộc, payload rỗng).
  - `ErrForbidden`: Từ chối quyền thao tác (ủy quyền từ `gateway.ErrProvisioningForbidden`).
  - `ErrInconsistentGraph`: Phát hiện dữ liệu đồ thị hoặc quan hệ thực thể bất nhất (ủy quyền từ `gateway.ErrProvisioningInconsistent`).
  - `ErrMetadataPatchUnavailable`: Lỗi hạ tầng hoặc commit không chắc chắn khi thực hiện PATCH (bọc `ErrDependencyUnavailable`).
  - `ErrDependencyUnavailable`: Lỗi kết nối cơ sở dữ liệu hoặc context timeout/cancel.
  - `ErrUnexpected`: Lỗi không lường trước trong quá trình xử lý nội bộ.
- **Trong gói `internal/gateway`:**
  - `ErrProvisioningForbidden`: Vi phạm phân quyền tạo tài nguyên.
  - `ErrProvisioningConflict`: Xung đột dữ liệu khi replay hoặc trùng lặp tài nguyên khác thuộc tính.
  - `ErrProvisioningNotFound`: Không tìm thấy Gateway cha hoặc người dùng được chỉ định.
  - `ErrProvisioningInconsistent`: Đồ thị thực thể hoặc trạng thái bị hỏng.
  - `ErrGatewayProvisioningUnavailable`: Lỗi kết nối khi provisioning Gateway.
  - `ErrAdminOrOwnerProvisioningUnavailable`: Lỗi kết nối khi provisioning Sensor.

---

## 5. Giới Hạn Còn Lại & Hướng Dẫn Bàn Giao

### 5.1. Giới Hạn Kỹ Thuật Còn Lại (Remaining Technical Limitations)
1. **Trạng thái Follow-up Review (WARNING — Đang xử lý 2 điểm MEDIUM):** Đợt review lại đã đóng các Findings 1–4 ban đầu, nhưng đưa ra kết luận WARNING do còn 2 phát hiện mức MEDIUM về code/test (bổ sung các trường audit còn thiếu trong so sánh snapshot sentinels và bẫy lỗi dọn dẹp `auth.users` tránh false-success). Các code worker đang tiếp tục khắc phục tuần tự các điểm này; tài liệu đã chỉnh sửa chuẩn xác mốc thời gian `statement_timestamp()` và làm rõ ranh giới đơn điệu; toàn bộ nội dung sửa đổi chưa được phê duyệt trước.
2. **Chưa đăng ký route HTTP live (Task 4.6):** Các route `POST /v1/digital-twins` và `PATCH /v1/digital-twins/{entity_id}` mới chỉ hoàn thành tầng parser, DTO và use-case service; chưa được đăng ký vào router Gin của `httpserver` và chưa được khởi tạo trong `cmd/server`.
3. **Chưa có kiểm thử E2E live HTTP và MQTT $\rightarrow$ HTTP (Task 4.7):** Toàn bộ việc kiểm thử mới được thực hiện ở cấp độ unit và native database integration test. Việc kiểm thử đầu-cuối qua proxy HTTP thật và luồng tích hợp MQTT $\rightarrow$ HTTP sẽ được thực hiện ở Task 4.7.
4. **Không cam kết chứng minh mọi lịch trình Deadlock tùy ý:** Các điều chỉnh khóa (`FOR SHARE` trên Gateway cha cho Sensor PATCH và Sensor POST replay) đã giải quyết triệt để deadlock trên các lịch trình tương tranh đại diện với luồng nạp telemetry; tuy nhiên không tuyên bố chứng nhận miễn nhiễm deadlock cho mọi lịch trình tương tranh tùy ý trong hệ thống phân tán.
5. **Không cam kết đơn điệu Wall-clock hay Đơn điệu phân tán:** Việc sử dụng `statement_timestamp()` được đánh giá tại câu lệnh UPDATE metadata cuối cùng sau khi đã nắm giữ khóa, giúp tránh sử dụng mốc thời gian bắt đầu transaction cũ trong lịch trình tuần tự hóa đã kiểm thử; hệ thống không cam kết tính đơn điệu tuyệt đối theo đồng hồ thực (wall-clock monotonicity) hay giữa nhiều node cơ sở dữ liệu (distributed monotonicity).
6. **Legacy NULL Provenance Chưa Kiểm Thử Native:** Kiểm thử native cho dữ liệu legacy có provenance NULL thông qua ủy quyền `value` của Task 4.4 vẫn chưa chạy trên schema chia sẻ hiện tại (được bảo vệ bởi unit test và rehearsal container).
7. **Không cam kết tính bền vững trước sự cố phần cứng / mất nguồn đột ngột:** Hoạt động của transaction tuân thủ mức độ bền vững của cấu hình PostgreSQL hiện tại, không có cam kết đặc biệt về fsync phần cứng hay chống sập nguồn đột ngột.
8. **Chưa có Remote CI:** Toàn bộ kiểm thử được chạy cục bộ; chưa có pipeline CI remote mới cho Stage 4.

### 5.2. Nội Dung Bàn Giao Cho Task 4.6 (HTTP Handlers & Server Composition)
- **Constructors & Services sẵn sàng:**
  - `digitaltwin.NewCreateService(gwProvisioner, sensorProvisioner)`: Nhận `GatewayProvisioner` và `SensorProvisioner`, điều phối tạo mới thực thể.
  - `digitaltwin.NewGatewayPatchService(patcher, timeout)`: Điều phối PATCH Gateway với bounded timeout.
  - `digitaltwin.NewSensorPatchService(patcher, timeout)`: Điều phối PATCH Sensor với bounded timeout.
- **Bộ phân tích cú pháp HTTP Request (Strict Parsers):**
  - `httpserver.parsePostEntityRequest(r, limit)`: Phân tích payload POST strict, trả về `PostEntityRequest` và status (0, 400, 413, 415).
  - `httpserver.parsePatchEntityRequest(r, decodedEntityID, limit)`: Phân tích URN và payload PATCH strict, trả về `PatchEntityRequest` và status (0, 400, 413, 415).
- **Ánh xạ Response DTOs & Mã trạng thái:**
  - `NewPostGatewayResponseDTO` & `NewPostSensorResponseDTO`: Chuyển đổi kết quả provisioning sang wire DTO với mốc thời gian UTC microsecond.
  - `PostEntityStatus(created bool)`: Trả về `201 Created` nếu `created == true`, hoặc `200 OK` nếu là idempotent replay.
  - `NewPatchGatewayResponseDTO` & `NewPatchSensorResponseDTO`: Chuyển đổi kết quả PATCH sang wire DTO.
- **Ánh xạ lỗi HTTP chuẩn tắc:**
  - `ErrInvalidInput` $\rightarrow$ 400 Bad Request.
  - `ErrForbidden` / `ErrProvisioningForbidden` $\rightarrow$ 403 Forbidden.
  - `ErrNotFound` / `ErrProvisioningNotFound` $\rightarrow$ 404 Not Found.
  - `ErrProvisioningConflict` $\rightarrow$ 409 Conflict.
  - `ErrInconsistentGraph` / `ErrProvisioningInconsistent` / `ErrUnexpected` $\rightarrow$ 500 Internal Server Error.
  - `ErrMetadataPatchUnavailable` / `ErrDependencyUnavailable` / context timeout $\rightarrow$ 503 Service Unavailable.

### 5.3. Nội Dung Bàn Giao Cho Task 4.7 (Tích Hợp, Query Measurements & CI)
- Tái sử dụng các suite kiểm thử native trong `postgres_metadata_integration_test.go` làm nền tảng cho kịch bản tích hợp dọc (vertical slice) tại `scripts/test-stage4-api.sh`.
- Kế thừa các bất biến về lock order (`FOR SHARE` trên Gateway cho Sensor PATCH) để phối hợp nhịp nhàng giữa luồng HTTP mutation và luồng MQTT telemetry ingestion.
