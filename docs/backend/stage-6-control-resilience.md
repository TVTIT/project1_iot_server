# Báo cáo Nghiệm thu Kỹ thuật Giai đoạn 6 (Stage 6) — Control Resilience & Reconciliation

**Mốc kiểm tra:** 19/11/2026, đối chiếu mã nguồn và kết quả CI từ xa.
**Revision mã nguồn:** Nhánh `develop`, commit HEAD `a648ec1c8e22b99d3a185aa480a8b17534f7df8f` (kế thừa commit `6250cc8` bổ sung retention/upgrade, và `ce831ef` tích hợp kiểm thử điều khiển).
**Trạng thái tổng thể Stage 6:** **PARTIAL** (các lát cắt chức năng R0–R8 và R2 follow-up đã hoàn tất và pass CI từ xa; script retention còn finding review chưa đóng; rollout/target preflight/board NOT RUN/DEFERRED).

---

## 1. Phạm vi và Các Lát cắt Đã Triển khai (Completed Slices)

Giai đoạn 6 mở rộng vòng đời điều khiển Sensor (tiếp nối Stage 5 và Paper P3/P6), cung cấp khả năng chịu lỗi kênh truyền, phục hồi sau gián đoạn và đối soát hội tụ tự động giữa server và Gateway:

1. **6-R0 & 6-R1 — Thiết kế Hợp đồng & Envelope:**
   - Hoàn thiện đặc tả hợp đồng MQTT và REST trong [Stage 5 Sensor Control API](../protocols/stage5-sensor-control-api.md) và [MQTT v1](../protocols/mqtt-v1.md).
   - Thiết kế các envelope MQTT mới: `command_receipt` (device receipt), `configuration_query` (downlink query), `configuration_snapshot` (uplink readback).
2. **6-R2 — Lưu trữ Bền vững & Nâng cấp Schema (PostgreSQL & SQLite):**
   - Migration 27 (`000027_stage6_control_resilience.up.sql`): bổ sung các cột quản lý dòng dõi (`origin`, `root_command_id`, `parent_command_id`), số lần thử (`attempt_count`, `first_reserved_at`), trạng thái hủy (`cancelled_at`, `cancelled_by`), device receipt (`acknowledged_at`), lý do dừng retry (`retry_stop_reason`), cờ khóa đối soát (`reconcile_blocked`), và thông tin đối soát trên `twin_states`. Ràng buộc khóa ngoại `ON DELETE RESTRICT` bảo vệ dòng dõi lệnh.
   - Nâng cấp SQLite store của Gateway simulator lên `user_version = 3`: lưu trữ receipt/reservation trước apply, quản lý outbox/responses queue bền vững.
3. **6-R3 — Lịch Retry Lũy thừa có Jitter & Dispatch Reservation:**
   - Thuật toán exponential backoff kết hợp jitter ngẫu nhiên trong khoảng 50–100%.
   - Cấp quyền gửi (dispatch reservation) bền vững trong DB với ngân sách cấp quyền mặc định 5 lượt (cấu hình qua `STAGE6_RETRY_MAX_ATTEMPTS`); lease/TTL hữu hạn chống worker treo; fencing chống stale worker.
   - Phát lại nguyên byte MQTT (`payload_bytes`, `desired_version`, `issued_at`, `expires_at`, cùng topic và command ID), không thay đổi payload hay parameters qua các lượt retry.
4. **6-R4 — Endpoint Hủy Lệnh An toàn (Safe Cancellation):**
   - Endpoint `POST /v1/digital-twins/{entity_id}/commands/{command_id}/cancel` (body rỗng, yêu cầu quyền owner hoặc operator).
   - Ranh giới hủy an toàn tuyệt đối: chỉ cho phép hủy khi lệnh ở trạng thái `pending`, chưa từng được cấp quyền dispatch (`attempt_count == 0` và `first_reserved_at IS NULL`), và outbox chưa dispatch.
   - Khi hủy thành công: chuyển `twin_commands.status = 'cancelled'`, dừng outbox (`status = 'failed'`), đánh dấu `reconcile_blocked = true` trên cả command và root intent để ngăn chặn reconciliation tự động phát lại. Trả về `200 OK`.
   - Idempotent replay: gọi lại lệnh cancel đã hủy bởi caller có thẩm quyền trả về đúng kết quả lưu trữ `200 OK` (cùng `cancelled_at`).
   - Conflict dứt khoát: nếu lệnh đã bước vào trạng thái active khác (`published`, `acknowledged`) hoặc terminal (`succeeded`, `failed`, `timeout`), từ chối dứt khoát `409 Conflict` (`command_not_cancellable`). Tuyệt đối không có remote abort qua MQTT.
5. **6-R5 — Device Receipt & Trạng thái Acknowledged:**
   - Gateway simulator opt-in `GATEWAY_STAGE6_ENABLED=true` tạo canonical receipt nguyên tử cùng reservation trước khi gọi sampler apply; phát `command_receipt` lên topic `gateways/<gateway_id>/responses/<command_id>`.
   - Backend `ResultConsumer` nhận và kiểm tra strict parser; chuyển command sang trạng thái active `acknowledged` (`acknowledged_at`).
   - Phân định rõ: `acknowledged` chỉ xác nhận Gateway đã lưu receipt bền vững, **KHÔNG PHẢI xác nhận áp dụng thành công**. Cấu hình chỉ được xác nhận khi Gateway gửi `command_result` (`succeeded`).
6. **6-R6 — Đối soát Cấu hình & Đọc lại Trạng thái (Readback):**
   - Backend quét định kỳ (scan batch 16, scan interval 10s, cooldown 30s) hoặc sau reconnect, phát `configuration_query` (TTL 5s) downlink.
   - Gateway opt-in responder đọc committed watermark trong SQLite và sampler cadence thực tế đang chạy, phản hồi `configuration_snapshot` uplink.
   - Ephemeral correlation trong RAM backend quản lý tương quan theo `request_id` (trần toàn cục 32 query); an toàn qua restart tiến trình.
7. **6-R7 — Engine Đối soát Hội tụ Tự động (Reconciliation Engine):**
   - So sánh cấu hình mong muốn (`desired`) và cấu hình thực tế đọc được (`snapshot`).
   - Guard kiểm tra nghiêm ngặt: nếu intent đã bị cancel (`reconcile_blocked`), hoặc đã dùng hết ngân sách (`auto_successors_used >= max_auto_successors = 1`), hoặc Gateway store hỏng / unsupported $\to$ ghi nhận `blocked_cancelled`, `blocked_budget`, `corrupt_unavailable`, hoặc `manual_unconfirmed`, dừng tự động (fail-closed), không lặp vô hạn.
   - Khi đủ điều kiện hội tụ: sinh nguyên tử successor command với **command ID mới, desired version mới tăng 1, cùng thông số mong muốn**. Command cũ hết hạn giữ nguyên `timeout` (terminal bất biến).
   - Khi Gateway thực thi successor thành công $\to$ cập nhật `reported.configuration` chính xác.
8. **6-R8 — Tích hợp CI và Diễn tập Hồi quy Toàn diện:**
   - Tích hợp các kịch bản kiểm thử điều khiển vào workflow CI chuẩn `.github/workflows/ci.yml`.
   - Xác minh toàn bộ các luồng Stage 6 hoạt động ổn định trên môi trường CI từ xa.
9. **6-R2 Follow-up — Kiểm chứng Nâng cấp & Script Retention:**
   - Nâng cấp schema 26→27 trên dữ liệu Stage 5; kiểm chứng script dọn dẹp lịch sử `scripts/sql/stage6-command-retention.sql` (retention 30 ngày) ở chế độ dry-run và apply có kiểm soát.

---

## 2. Bằng chứng Kiểm chứng CI Từ xa (Remote CI Evidence)

Kết quả kiểm thử từ xa được xác minh trực tiếp từ GitHub Actions cho code revision hiện hành:

- **Workflow Run:** [Run 44501549344](https://github.com/TVTIT/project1_iot_server/actions/runs/44501549344)
- **Branch:** `develop`
- **Exact Commit SHA:** `a648ec1c8e22b99d3a185aa480a8b17534f7df8f`
- **Kết luận chung (Conclusion):** **SUCCESS**

### Chi tiết các Jobs trong Run 44501549344:

| Tên Job | Kết quả | Nội dung kiểm tra & Bằng chứng |
|---|:---:|---|
| `migration-check` | **SUCCESS** | Kiểm tra tính tương thích của migrations 000001–000027 trên PostgreSQL/Supabase. |
| `dynsec-bootstrap-tests` | **SUCCESS** | Kiểm tra test harness, native Mosquitto DynSec bootstrap và xác thực report. |
| `stage5-control-integration` | **SUCCESS** | Chạy toàn diện 3 kịch bản kiểm thử điều khiển:<br>1. P3 baseline require-run (vòng điều khiển Stage 5 cơ bản).<br>2. P3 opt-in Stage 6 (vòng điều khiển mở rộng với retry, cancel, receipt).<br>3. Owned MQTT retry/convergence (kiểm thử hội tụ, retry schedule và recovery). |
| `native-compose-smoke` | **SUCCESS** | Khởi chạy Docker Compose topology hoàn chỉnh và chạy kiểm tra smoke native. |

*Lưu ý bảo toàn lịch sử:* Run trước đó (Run 44497956985 trên commit `6250cc8`) bị thất bại do kiểm tra metadata file thực thi trên host trong test harness bootstrap (`test_task284_bootstrap.py`); native bootstrap bị bỏ qua (skipped) trong lần chạy đó. Lỗi test-only này đã được sửa tại commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f` mà không làm thay đổi logic nghiệp vụ. Lịch sử thất bại của Run 44497956985 được giữ nguyên ghi nhận, không xóa bỏ.

Kết quả CI từ xa nói trên xác nhận mã nguồn tại commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f`; các cập nhật tài liệu sau đó không làm thay đổi mã nguồn và chưa được kiểm thử bởi lần chạy CI đó.

---

## 3. Bằng chứng Kiểm chứng Cục bộ 7-Lane (Local Verification Checkpoint)

Tại checkpoint kiểm chứng kỹ thuật cục bộ tại source trước khi áp dụng patch test CI (kèm retention diff; không gán là cùng lần chạy với revision remote):

1. **Lane 1 — Nâng cấp Migration 26→27:** Diễn tập nâng cấp thành công trên dữ liệu Stage 5 hiện hữu; kiểm tra reapply idempotent và DDL lock bounds.
2. **Lane 2 — Native Intent DB Fixture (`stage5_intent_fixture.py`):** 39 required top-level tests PASS, 0 skips, xác minh 5 kịch bản cleanup.
3. **Lane 3 — Native HTTP Fixture (`stage5_http_fixture.py`):** 3 required tests PASS, 0 skips.
4. **Lane 4 — Native MQTT Fixture:** Vòng đời Stage 5 + Stage 6 (roundtrip receipt/result, readback snapshot, convergence successor), dọn dẹp chính xác tài nguyên.
5. **Lane 5 — P3 Baseline Require-Run:** `PAPER_P3_ACCEPTED`, dọn dẹp chính xác 17 containers / 1 network / 7 volumes.
6. **Lane 6 — P3 Opt-in Stage 6 Require-Run:** Kiểm thử vòng điều khiển có bật đầy đủ tính năng Stage 6.
7. **Lane 7 — Full HTTP Scenario:** Chạy toàn diện kịch bản HTTP Stage 4/Stage 5.

- **Retention Script:** Kiểm chứng `stage6-command-retention.sql` trên fixture cô lập: dry-run hoàn tất trong ~0.96s (1.000 root/10 sensor), apply xử lý batch 100 lineage trong ~1.01s. Kịch bản duplicate-key (RED→GREEN) xác nhận script giữ nguyên các lineage lỗi và chỉ xóa các lineage terminal hợp lệ.
- **Python Guards:** 109 test guards CI chạy PASS riêng biệt (không gộp với native acceptance).

---

## 4. Các Giới hạn Còn Mở & Ranh giới Vận hành (Limitations & Boundaries)

Dù mã nguồn và CI từ xa đã hoàn tất, Stage 6 vẫn được phân loại là **PARTIAL** do các giới hạn thực tế sau:

1. **Finding Review Độc lập đối với Script Retention (Chưa đóng):**
   - **MEDIUM:** Trong `scripts/sql/stage6-command-retention-candidates.sql`, selector `json_each` phụ thuộc vào thứ tự đánh giá toán tử `AND` khi gặp wrapper `reported_state` là scalar trước object. Dù dry-run và apply qua entrypoint chính thức đều PASS (không tái hiện lỗi abort hay mất dữ liệu), việc sửa guard bằng `CASE` kiểm tra kiểu object tường minh vẫn cần được thực hiện trước khi vận hành trên môi trường dùng chung.
   - **LOW:** Assertion snapshot trong test regression của script chưa bao gồm trường timestamp (`result_received_at`, `late_result_received_at`, `completed_at`).
   - **Chỉ thị An toàn:** Tuyệt đối **KHÔNG chạy retention apply trên database dùng chung / production** khi finding này chưa được đóng.
2. **Cấu hình Mặc định Tiếp tục Tắt:**
   - Toàn bộ các cờ triển khai tiếp tục giữ mặc định là `false`: `STAGE5_ENABLED=false`, `STAGE6_ENABLED=false`, `GATEWAY_STAGE6_ENABLED=false`, `WEBSOCKET_ENABLED=false`.
   - Việc kích hoạt trên môi trường triển khai thực tế nằm ngoài phạm vi nghiệm thu này.
3. **Phần cứng và Rollout Chưa Thực hiện:**
   - **Board AM5728 & Cảm biến Vật lý / HIL:** Tiếp tục ở trạng thái **DEFERRED / NOT RUN** do chưa có phần cứng. Gateway simulator không thay thế cho việc xác nhận trên chip cảm biến thật.
   - **Target Preflight & Backup/Restore Thực tế:** Chưa diễn tập preflight schema và restore backup trên target cluster thực tế.

---

## 5. Bàn giao cho Giai đoạn 9 (Stage 9 Handoff)

Các nội dung bàn giao chính cho Giai đoạn 9 (Tích hợp Hệ thống, Bàn giao & Triển khai):

1. **Trạng thái Mã nguồn:**
   - Mã nguồn Stage 6 tại commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f` là baseline kỹ thuật đã được remote CI xác nhận.
   - Bất kỳ thay đổi logic mã nguồn nào phát sinh trong Giai đoạn 9 đều bắt buộc phải chạy lại quy trình kiểm thử và requalification.
2. **Nội dung Giai đoạn 9 Cần Thực hiện:**
   - Thực hiện preflight schema, kiểm tra phân quyền và diễn tập backup/restore thực tế trên target database trước khi kích hoạt cờ tính năng.
   - Xây dựng kịch bản demo tích hợp (demo flow) cho tính năng điều khiển cảm biến và hiển thị trên giao diện người dùng.
   - Khi có phần cứng board AM5728 và cảm biến thật, tiếp nhận và hoàn tất các mục kiểm thử HIL (R3–R5 của Stage 5 gốc).
   - Xem xét và đóng finding review của script retention trước khi đưa vào lịch vận hành định kỳ.
3. **Tài liệu Tham chiếu:**
   - Hợp đồng REST & MQTT: [Stage 5 Sensor Control API](../protocols/stage5-sensor-control-api.md) và [MQTT v1](../protocols/mqtt-v1.md).
   - Sổ tay vận hành: [Deployment & Operations](../dynsec/deployment-and-operations.md).
   - Danh mục tồn đọng trung tâm: [Limitations & Backlog](../dynsec/limitations-and-backlog.md).
