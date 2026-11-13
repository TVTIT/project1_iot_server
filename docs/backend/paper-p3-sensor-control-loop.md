# P3 — Vòng điều khiển Sensor trên simulator

## Sửa hai lỗi HIGH sau review P3 (25/10/2026)

### Phạm vi và thay đổi

- `internal/mqttcontrol/results.go`: chỉ cập nhật outcome counters khi observer khác nil. Parsing và repository transaction vẫn chạy khi `METRICS_ENABLED=false`; không dùng panic recovery để che lỗi.
- `internal/gatewaysim/transport.go`: delivery trước SUBACK hoặc giữa SUBACK và install được sao chép vào buffer riêng của connection lifetime. Chỉ drain sau khi toàn bộ subscriptions được xác nhận và lifetime được install. Staging, drain và live admission được tuần tự hóa bằng cùng khóa; không parse JSON, truy cập SQLite hoặc gọi network trong vùng khóa. Callback lifetime đã thay thế không được quay lại staging.
- Buffer startup cố định tối đa 32 messages, payload tối đa 8 KiB/message, topic tối đa 256 bytes. Quá giới hạn phát `ErrTransportStartupOverflow`, dừng reconnect và giữ not-ready; không tuyên bố reconnect sẽ phục hồi message đã được Paho auto-ACK. Giới hạn queues downstream vẫn giữ nguyên.
- Fixture P3 bổ sung một lệnh 50 Hz khi metrics tắt. Backend cũ được graceful-stop rồi remove theo exact-owned registry trước khi tạo backend thay thế; Nginx reload upstream sau thay đổi. Không có hai Go backend active đồng thời, không tác động deployment chung.

### Bằng chứng RED → GREEN

Regression ban đầu được thực thi bằng:

```sh
go test ./internal/mqttcontrol ./internal/gatewaysim -run 'TestResultConsumerWithoutMetrics|TestTransportStagesDeliveryUntilInstall' -count=1
```

- **RED:** bốn outcome với observer nil đều panic; delivery startup chỉ nhận được 1 thay vì 3 messages khi control tắt, 1 thay vì 4 khi control bật.
- **GREEN:** cùng target PASS sau sửa. Các regression liên quan còn được chạy `-race -count=20`, PASS.
- `results_test.go`: observer nil với accepted/duplicate/late/unknown; worker thật tiếp tục xử lý qua success, conflict, DB error và malformed input; shutdown theo budget.
- `control_transport_test.go`: delivery trước SUBACK và trước install, sao chép bytes, command hợp lệ vào inbox thật và ACK riêng; SUBACK denied/downgrade, loss/shutdown; stale callbacks; capacity/payload/topic limits và inclusive boundary; live callback không vượt qua startup drain. Test dùng fake client và channel barriers, không dùng timing ngẫu nhiên để tạo cửa sổ lỗi.

### Kiểm chứng mới — không thay thế kết quả lịch sử bên dưới

- Native final: **`ncs_64809f03cce5` PASS**, `PAPER_P3_ACCEPTED`, `full_p3_accepted: true`, `exact_cleanup_verified: true`; dọn sạch **17 containers / 1 network / 7 volumes**.
- **6 commands:** 5 command bật metrics theo workload cũ; command thứ sáu 50 Hz (`sample_interval_us=20000`, desired version 5) chạy khi metrics tắt. Xác minh env thực tế, `/v1/admin/metrics` trả 404, result `succeeded` có canonical bytes trong DB, cấu hình khớp REST, backend còn hoạt động và telemetry application ACK drain được outbox.
- Timeline và result counters trong artifact chỉ thuộc **5 command bật metrics, trước khi restart backend**. Không cộng command metrics-off vào histogram hoặc gán thời gian bằng 0. Final DB accounting gồm cả hai đoạn: 15 ledgers, 714 raw samples, 15 temporal observations, 2 configurations; số raw samples đối chiếu theo từng committed batch PASS.
- Artifacts: `/var/tmp/opencode/p3-review-fixes-EblFYP/native-02/artifacts/`; Go race log: `/var/tmp/opencode/p3-review-fixes-EblFYP/go-race.log`.
- Lượt trước **`ncs_375e9aebcede` FAIL**, `health_failed` tại đoạn thay backend để tắt metrics, sau 5 command bật metrics. Cleanup verified (16 containers / 1 network / 7 volumes). Helper ban đầu force-remove backend, chưa graceful-stop để Unix listener dọn socket; log startup chi tiết của backend thay thế chưa được thu ở lượt đó, nên không khẳng định nguyên nhân từ log. Sau đó helper thêm graceful-stop và protected startup diagnostics. Giữ nguyên FAIL này, không đổi thành PASS hồi tố.
- Go race **8 packages ảnh hưởng PASS**; `gofmt` clean, `go vet ./...` clean, `golangci-lint` **0 issues**, `go build ./...` và ARMv7 Gateway build PASS. Python guards **224 tests PASS**, gồm 6 tests cho helper thay backend exact-owned.
- Không chạy lại native DB intent fixture riêng, full CI, remote CI hoặc board/HIL trong lần sửa này. Không đặt coverage gate hay tuyên bố mọi fault interleaving được chứng minh. Deployment tiếp tục mặc định `STAGE5_ENABLED=false`; không commit/push, không mở P6/Stage 6.

## Tóm tắt bàn giao mức Local MVP — lượt nghiệm thu ban đầu (25/10/2026)

- **Trạng thái & Nghiệm thu Native:** Bàn giao P3 mức Local MVP hoàn tất sau khi hoàn thành toàn bộ các task P3.0 → P3.10 và rà soát đủ 8/8 điều kiện hoàn thành. Lượt chạy native E2E đầy đủ `ncs_79ba9068cca0` trên profile HTTPS/RS256 cô lập đạt **PASS** (`status: PASS`, `full_p3_accepted: true`, `PAPER_P3_ACCEPTED`, `exact_cleanup_verified: true`, dọn sạch 15 containers / 1 network / 7 volumes).
- **Mã nguồn & Artifacts:** Commit `4c3cdaf9f3f152f64ed930b1a9e67173928b356f`, branch `feat/paper-requirement`, `dirty: true` (bảo toàn uncommitted changes). Artifacts an toàn tại `/tmp/opencode/paper-p3-e2e-after-acl-20261025-07/artifacts/` (3 files: `run.json`, `metrics.csv`, `startup-summary.json`).
- **Vòng điều khiển khép kín trên Simulator:** Thực thi trọn vẹn luồng `PATCH /v1/digital-twins/{entity_id}/desired-state` (tần số thập phân, quyền `owner`/`operator`) → transaction intent PostgreSQL → MQTT command → Gateway simulator đổi scheduler/readback/durable dedup → MQTT result → transaction tiếp nhận backend → merge `reported.configuration` → tra cứu REST.
- **5 lệnh kiểm chứng đại diện:**
  1. `100 Hz` (`10.000 us`) trên Sensor chính của Gateway A (`desired_version = 1`) → `succeeded`, confirmed.
  2. `100 Hz` (`10.000 us`) trên Sensor thứ hai cùng thuộc Gateway A (`desired_version = 1`) → `succeeded`, confirmed (chứng minh hai Sensor độc lập trên cùng Gateway).
  3. `1.5 Hz` (`666.667 us`, số thập phân) trên Sensor chính (`desired_version = 2`) → `succeeded`, confirmed, thu thập viễn trắc chu kỳ lẻ chính xác.
  4. `0.1 Hz` (`10.000.000 us`, tần số thấp < 1 Hz) có kích hoạt lỗi DB tạm thời tại backend (`p3_hold_result`) → Gateway resend kết quả đã cache với exponential backoff → sau khi xóa hold, backend tiếp nhận và xác nhận `succeeded` thành công (`desired_version = 3`). Kết quả xác nhận có trước khi mẫu đầu tiên đo xong.
  5. `60 Hz` (`16.667 us`) trên Sensor chính (`desired_version = 4`) kèm kịch bản controlled SIGTERM stop và persistent SQLite reopen → Gateway nạp cấu hình đã lưu, khôi phục hàng đợi viễn trắc byte-exact, và xử lý duplicate command bằng kết quả đã cache mà không thực thi lại.
- **Số liệu đo lường trễ toàn tuyến thực tế:**
  - `D_dispatch` (từ HTTP entry tại Go tới broker PUBACK đầu tiên): 5 mẫu hợp lệ, tổng `2,691 s`, trung bình `0,538 s` (phù hợp với chu kỳ scan 1s); phân bố: 2 mẫu trong `(0.25; 0.5] s`, 3 mẫu trong `(0.5; 1.0] s`.
  - `D_e2e_succeeded` (từ cùng HTTP entry tới confirmed result commit tại DB Go): 5 mẫu hợp lệ, tổng `4,312 s`, trung bình `0,862 s`; phân bố: 2 mẫu trong `(0.25; 0.5] s`, 2 mẫu trong `(0.5; 1.0] s`, 1 mẫu trong `(1.0; 2.5] s`.
  - `missing_origin = 0`, `missing_dispatch = 0`, `missing_result = 0`, `overflow = 0`.
  - Kết quả xử lý result tại backend: 5 `accepted`, 1 `db_error` (lỗi cố ý được tiêm), 12 `duplicate`, 0 `late`, 0 `rejected`, 0 `queue_full`.
  - Kiểm toán dữ liệu DB: `configuration: 2` (cả 2 Sensor được xác nhận), `ledger: 10`, `raw: 437`, `temporal: 10`. Raw samples khớp chính xác số mẫu của từng batch đã commit.
- **Xác minh kỹ thuật P3.9:**
  - Go `-race` 8 packages ảnh hưởng PASS (`internal/gatewaysim`, `cmd/gateway`, `internal/digitaltwin`, `internal/mqttcontrol`, `internal/metrics`, `internal/httpserver`, `cmd/server`, `internal/config`).
  - `gofmt -l .` clean; `go vet ./...` clean; `golangci-lint run --timeout=5m` (v2.14.0) **0 issues**; `go build ./...` clean; ARMv7 cross-build Gateway clean.
  - Python guard tests 218 tests PASS; `git diff --check` clean.
  - Native DB intent runner (`stage5_intent_fixture.py`): 17 required top-level tests PASS, no skips, 5 kịch bản cleanup verified (fresh, upgrade, 3 preflight refusals).
- **Ranh giới & Giới hạn cốt lõi (chi tiết tại [backlog chung](../dynsec/limitations-and-backlog.md)):**
  - Nghiệm thu cục bộ mức local MVP trên profile cô lập; cấu hình deployment mặc định tiếp tục là `STAGE5_ENABLED=false`. Việc live rollout lên shared deployment nằm ngoài phạm vi P3.
  - Vòng điều khiển Sensor tối thiểu hoàn tất trên simulator; tuy nhiên Stage 5 gốc chưa hoàn tất toàn bộ do còn phần kiểm thử tích hợp trên board AM5728 / HIL, lane remote CI require-run cho revision cuối, và bộ tài liệu vận hành toàn diện (Task 5.7–5.8). Các hạng mục này được theo dõi riêng tại checkpoint P8.
  - Dữ liệu viễn trắc trên simulator là chuỗi sóng tam giác tổng hợp (Synthetic Data); chưa chứng minh thay đổi tốc độ chuyển đổi vật lý bên trong chip cảm biến thật.
  - Remote CI và kiểm thử phần cứng nhúng (board AM5728 / HIL) tiếp tục giữ trạng thái NOT RUN.
  - Không tự mở P6 (tối ưu dispatch dưới tải viễn trắc cao) hoặc Stage 6 (reconciliation/cancellation tổng quát).

---

## 1. Baseline và điểm nối (P3.0)

- **Git root:** `development`, nhánh `feat/paper-requirement`, HEAD `4c3cdaf9f3f152f64ed930b1a9e67173928b356f`, `dirty: true`.
- **Môi trường:** Go 1.27.3 linux/amd64, Docker Engine 29.8.2, golangci-lint v2.14.0, gói `acl` v2.3.2 (`setfacl`/`getfacl`).
- **PostgreSQL schema terminal:** 23 (`sensor_control_intent`). Bảng `twin_commands` đã có các cột `target_entity_type`, `control_version`, `requested_frequency_hz`, `result_bytes`, `result_hash`, `result_received_at`, `late_result_bytes`, `late_result_hash`, `late_result_received_at`. Không cần thêm migration PostgreSQL mới.
- **Tái sử dụng:** Parser domain decimal/interval (`internal/digitaltwin`), khóa tài nguyên `withControlLock`, trigger write-once và immutable result, outbox publisher có điều kiện, MQTT transport groundwork, profile HTTPS/RS256 từ P2.

---

## 2. Các thành phần đã triển khai (P3.1 – P3.8)

### P3.1 — SQLite Durable Store và Upgrade (`control_store.go`)
- Nâng cấp schema SQLite từ 1 lên 2, bổ sung hai bảng:
  - `sensor_configuration`: Lưu `sample_interval_us`, `desired_version`, `applied_at` (duy nhất 1 dòng `id=1`).
  - `command_receipts`: Lưu `command_id` (PK), `command_hash`, `command_topic`, `command_bytes`, `desired_version`, `sample_interval_us`, `expires_at`, `status` (`prepared` / `completed`), `result_bytes`, `retain_until`.
  - Ràng buộc: `one_prepared_command` (duy nhất 1 lệnh ở trạng thái `prepared`), chỉ mục dọn dẹp `command_receipts_retention`.
- Hỗ trợ nâng cấp tự động (upgrade) từ file outbox schema 1 cũ trong một transaction duy nhất, bảo toàn nguyên vẹn toàn bộ hàng viễn trắc pending/quarantined, sequence watermark, quota và file lock.
- Quota kiểm soát riêng: Tối đa 256 receipts, tối đa 4 MiB business bytes; retention tối thiểu 24 giờ sau `max(deadline, completion)`. Khi hết dung lượng, từ chối an toàn bằng lỗi rõ, không xóa receipt đang xử lý hoặc kết quả trong cửa sổ retry.

### P3.2 — Bộ lập lịch lấy mẫu động và ranh giới chu kỳ (`sampling.go`, `scheduler.go`)
- Quản lý nhịp lấy mẫu dựa trên timer thực tế theo microsecond (`sample_interval_us`), hỗ trợ trọn vẹn dải tần số thập phân và tần số thấp (`100 → 60 → 1.5 → 0.1 Hz`).
- Tách biệt rõ ràng: Chỉ ghi nhận các mẫu đã đến hạn đo, không sinh trước mẫu ở tương lai.
- Dưới 1 Hz: Phát một-sample batch sau mỗi lần lấy mẫu thật; tuyệt đối không phát batch rỗng trong thời gian chờ.
- Chuyển đổi cấu hình an toàn tại ranh giới (batch boundary): Khi đổi chu kỳ, flush các mẫu đã tích lũy của cấu hình cũ ra outbox, cập nhật mốc lấy mẫu kế tiếp theo cấu hình mới, không chờ mẫu của chu kỳ cũ, không reset boot ID hay sequence watermark.
- Ưu tiên cấu hình đã lưu: Khi reopen, nạp cấu hình từ `sensor_configuration` thay vì reset về giá trị bootstrap môi trường.

### P3.3 — Bộ điều khiển thực thi và chống trùng bền vững (`control.go`)
- `Controller`: Quản lý tuần tự (serialized) các lệnh theo Sensor.
- Kiểm tra tính hợp lệ và đối soát receipt:
  - Duplicate cùng ID và cùng nội dung: Trả lại kết quả thực thi đã lưu, không thực thi lại, kể cả sau khi lệnh đã hết hạn.
  - Cùng ID khác nội dung: Từ chối xung đột danh tính (`ErrControlConflict`), bảo vệ kết quả canonical.
  - Lệnh có `desired_version <= current`: Từ chối `STALE_DESIRED_VERSION`.
  - Lệnh quá hạn trước khi áp dụng: Từ chối `COMMAND_EXPIRED`.
- Quy trình áp dụng 2 pha: (1) Lưu trạng thái `prepared` bền vững; (2) Thực hiện đổi chu kỳ qua adapter và đọc lại chu kỳ hoạt động; (3) Commit kết quả `completed` và cấu hình mới vào SQLite.
- Xử lý gián đoạn / reopen: Trước khi bắt đầu tạo batch mới, nạp cấu hình đã lưu và hoàn tất xử lý các receipt dở dang một cách idempotent.

### P3.4 — Tích hợp MQTT và Vòng lặp phát lại kết quả (`control_runtime.go`)
- `CommandInbox`: Tiếp nhận bản tin `gateways/{gateway_id}/commands/#`, tách biệt hoàn toàn khỏi đường ACK viễn trắc (`acks/#`), đệm hàng đợi hữu hạn 32 message × 8 KiB.
- Yêu cầu SUBACK QoS 1 cho cả subscription commands và acks; tự động khôi phục cả hai khi reconnect.
- Phát lại kết quả có backoff (1s – 30s) trong suốt thời hạn hiệu lực của lệnh (`ExpiresAt`). Nhận PUBACK từ broker không đồng nghĩa backend đã commit; Gateway duy trì quyền phát lại cho đến khi cửa sổ retry kết thúc.

### P3.5 — Transaction tiếp nhận Result tại Backend (`postgres_control_result.go`)
- Hàm `AcceptControlResult`: Tiếp nhận bản tin kết quả từ Gateway, khóa tài nguyên theo đúng thứ tự (`withControlLock`: Gateway SHARE → Sensor UPDATE → Entity UPDATE → state UPDATE → command/outbox).
- Kiểm tra toàn diện tương quan: ID, Gateway, Entity, DesiredVersion, Parameters, thời gian hoàn thành.
- Succeeded: Merge duy nhất nhánh `reported_state.configuration` (`sample_interval_us`, `desired_version`, `applied_at`), tăng `reported_version` một lần; bảo toàn nguyên vẹn measurement fields và observation viễn trắc. Dừng hàng outbox tương ứng.
- Failed: Không sửa cấu hình; lưu mã lỗi allowlist.
- Duplicate: No-op, không tăng phiên bản.
- Late result (đến sau timeout/deadline): Ghi nhận vào cột `late_result_bytes`, `late_result_hash`, `late_result_received_at` dưới dạng write-once immutable evidence, không đảo ngược trạng thái `timeout` và không ghi đè cấu hình mới hơn.

### P3.6 — Consumer kết quả và Cấu hình Server (`results.go`, `stage5_transport.go`)
- `ResultConsumer`: Cố định 2 worker (cấu hình qua `STAGE5_RESULT_WORKERS`, cho phép 1–16) đọc từ kênh `Transport.Results()`.
- Xử lý bất đồng bộ ngoài callback MQTT; kiểm tra tải trọng và timeout DB hữu hạn. Lỗi DB hoặc đầy hàng đợi không xác nhận thành công giả.
- Tích hợp vòng đời: `composeStage5Transport` khởi động và dừng `ResultConsumer` đồng bộ với `Transport` và `Runtime` xuất bản lệnh. Readiness checker phản ánh trạng thái hoạt động thực của cả bộ worker xuất bản và tiếp nhận.

### P3.7 — Đo lường trễ điều khiển thời gian thực (`control_timeline.go`)
- `ControlTimeline`: Bảng tương quan trong bộ nhớ (tối đa 1024 entries, TTL 10 phút).
- Ghi nhận chính xác:
  - `D_dispatch = first successful PUBACK − HTTP entry tại Go`.
  - `D_e2e_succeeded = confirmed result commit tại Go − cùng HTTP entry`.
- Ghép nối an toàn qua Gin context (`WithControlHTTPEntry`); xử lý được trường hợp PUBACK hoặc result commit về trước khi HTTP response trả về xong.
- Đếm rõ ràng các trường hợp thiếu dữ liệu (`missing_origin`, `missing_dispatch`, `missing_result`, `overflow`), không điền 0 giả mạo hoặc trừ đồng hồ hai máy khác nhau.
- Xuất dữ liệu thống nhất qua `GET /v1/admin/metrics` và `metrics.csv` với các nhóm cố định `command_result` và `command_timeline`.

### P3.8 — Bộ kiểm thử tích hợp E2E trên Fixture cô lập (`paper_p3_support.py`, `test-paper-p3.sh`)
- Sử dụng runner `scripts/test-paper-p3.sh [DIR]` tạo môi trường Docker compose cô lập, áp dụng profile RS256/HTTPS từ P2, quota backend 2 vCPU / 4 GiB, cơ chế xác thực thật qua Nginx và Mosquitto DynSec.
- Tự động dọn dẹp sạch sẽ toàn bộ tài nguyên (containers, network, volumes, private inputs) sau khi kiểm thử kết thúc.

---

## 3. Lệnh chạy và Kết quả thực tế

### Lệnh chạy kiểm thử E2E P3

```sh
sh scripts/test-paper-p3.sh /tmp/opencode/paper-p3-e2e-final
```

### Kết quả ghi nhận từ `startup-summary.json` và `run.json`

- **Project ID:** `ncs_79ba9068cca0`
- **Trạng thái:** `PAPER_P3_ACCEPTED` (`full_p3_accepted: true`, `status: "PASS"`)
- **Dọn dẹp chính xác:** `cleanup_verified: true`, gồm 15 containers, 1 network, 7 volumes.
- **Bảng tổng hợp các bước kiểm tra (Checks Summary):**
  - `real_rs256_login`: PASS (đăng nhập user qua Supabase Auth nhận token RS256 thật).
  - `configuration_not_seeded`: PASS (xác nhận ban đầu không có `reported.configuration`).
  - `grant_viewer` / `viewer_reads`: PASS (user B được cấp vai trò `viewer`, đọc được danh sách command).
  - `patch_100_0_403`: PASS (`viewer` cố tình PATCH desired-state bị chặn với HTTP 403).
  - `grant_operator`: PASS (chuyển vai trò user B thành `operator`).
  - `patch_100_0_202` & `confirmed_10000`: PASS (lệnh 100 Hz cho Sensor chính thành công).
  - `provision_second_sensor` & `two_sensors_same_gateway`: PASS (Sensor thứ hai được tạo trên cùng Gateway, nhận lệnh độc lập và không xung đột).
  - `patch_1.5_1_202` & `confirmed_666667`: PASS (lệnh tần số thập phân 1.5 Hz thành công, viễn trắc thu thập đúng interval 666667 us).
  - `db_failure_never_confirms`: PASS (lệnh 0.1 Hz bị lỗi DB cố ý không làm xác nhận sai cấu hình).
  - `confirmed_10000000`: PASS (Gateway resend kết quả đã cache sau khi bỏ lỗi DB, backend tiếp nhận và xác nhận thành công).
  - `low_hz_result_before_sample`: PASS (xác nhận lệnh 0.1 Hz hoàn thành trước khi mẫu chu kỳ 10s đầu tiên hoàn tất).
  - `patch_60_3_202` & `confirmed_16667`: PASS (lệnh 60 Hz thành công).
  - `stop_p3-a` & `durable_config_receipts_pending`: PASS (dừng Gateway A, kiểm tra file SQLite giữ đúng cấu hình 16667 us, 4 receipts và các bản tin viễn trắc pending).
  - `reopen_active_readback`: PASS (khởi động lại Gateway A với outbox bền vững, scheduler đọc lại đúng chu kỳ 16667 us).
  - `canonical_receipts_byte_exact`: PASS (receipts và cấu hình sau reopen khớp byte-exact với trước khi dừng).
  - `pending_replay_byte_exact`: PASS (bản tin viễn trắc pending được phát lại chính xác từng byte sau khi kết nối lại).
  - `secondary_application_ack`: PASS (Gateway thứ hai chạy độc lập hoàn thành 2 batch và outbox rỗng).
  - `runtime_durations_once`: PASS (đo lường trễ `D_dispatch` và `D_e2e` đúng 5 lần cho 5 command mới, không nhân đôi khi resend).
  - `raw_samples_match_committed_batches`: PASS (toàn bộ 437 dòng viễn trắc raw khớp chính xác với `sample_count` của các batch đã commit).

### Bảng số liệu độ trễ quan sát được (`durations`)

| Tuyến đo lường | Số mẫu (N) | Tổng thời gian (Sum) | Giá trị trung bình (Mean) | Phân bố Histogram Buckets |
|---|:---:|:---:|:---:|---|
| **D_dispatch** (HTTP entry → first PUBACK) | 5 | 2,691 s | 0,538 s | 2 mẫu trong `(0.25; 0.5] s`<br>3 mẫu trong `(0.5; 1.0] s` |
| **D_e2e_succeeded** (HTTP entry → result commit) | 5 | 4,312 s | 0,862 s | 2 mẫu trong `(0.25; 0.5] s`<br>2 mẫu trong `(0.5; 1.0] s`<br>1 mẫu trong `(1.0; 2.5] s` |
| **D_e2e_failed** | 0 | 0 s | N/A | Không có command nào thất bại ở lượt chạy này |

*Lưu ý:* Thời gian `D_dispatch` trung bình ~0,54s là do backend sử dụng cơ chế định kỳ quét outbox (`STAGE5_SCAN_INTERVAL=1s`) mà chưa có cơ chế post-commit dispatcher wake-up tức thì (thuộc phạm vi tối ưu P6).

---

## 4. Đối chiếu Điều kiện hoàn thành P3

- [x] **Gateway thực sự đổi scheduler/readback, hỗ trợ decimal/low-Hz và không chờ sampling timer cũ để xử lý command:** Đã chứng minh qua các mốc 100 Hz, 1.5 Hz, 0.1 Hz và 60 Hz. Lệnh 0.1 Hz hoàn thành và nhận kết quả trước khi mẫu 10s đầu tiên hoàn thành đo.
- [x] **SQLite upgrade/reopen giữ durable config/receipt/result và telemetry pending; duplicate không apply lần hai trong contract đã công bố:** Đã chứng minh qua kịch bản SIGTERM và reopen Gateway A; bản tin pending giữ nguyên byte, cấu hình giữ 16667 us, gửi lại lệnh duplicate nhận kết quả cache không đổi.
- [x] **Backend consume responses thật, validate/correlate và commit command/configuration nguyên tử; early/duplicate/late/timeout không làm terminal hoặc config đi lùi:** Đã chứng minh qua 17 unit/integration tests trên PostgreSQL và 5 lệnh E2E thực tế.
- [x] **Không thay measurement/temporal semantics; pending telemetry retry giữ bytes và application commit ACK vẫn là điều kiện xóa outbox:** Đã chứng minh qua kiểm toán 437 dòng raw hypertable, 10 bản ghi temporal values và outbox chỉ xóa khi nhận commit ACK.
- [x] **Simulator HTTP → MQTT → apply → result → DB → REST PASS; quyền/ACL, one-active-per-Sensor và negative cases đại diện đúng:** Đã chứng minh qua lượt native E2E đầy đủ `ncs_79ba9068cca0`. Quyền `operator` được PATCH, `viewer` bị chặn 403, non-member bị 404, hai Sensor trên cùng Gateway hoạt động độc lập.
- [x] **Có D_dispatch/D_e2e từ runtime thật, sample count/biên đo rõ và không gán PUBACK/replay thành execution success:** Đã ghi nhận đầy đủ 5 mẫu hợp lệ, không điền 0 giả mạo, tách biệt rõ ràng giữa broker PUBACK và DB execution success.
- [x] **Tests/regressions ảnh hưởng, formatting/vet/lint/build phù hợp PASS; artifacts an toàn và cleanup chính xác:** Go race 8 packages PASS, lint v2.14.0 0 issues, vet clean, build clean, Python 218 tests PASS, dọn dẹp sạch sẽ 15 containers / 1 network / 7 volumes.
- [x] **Report/client/protocol/backlog được đồng bộ; deployment control vẫn tắt, board/CI/Stage 5 tổng thể được phân định đúng, không tự mở P6 hoặc Stage 6:** Tài liệu `mqtt-v1.md`, các tài liệu client, `README.md`, `limitations-and-backlog.md` đã được đồng bộ đầy đủ. Phân định rõ: P3 hoàn thành ở mức Local MVP trên simulator; Stage 5 gốc còn phần board AM5728/HIL, remote CI requirement lane và live rollout riêng (ghi nhận riêng tại P8). Deployment mặc định tiếp tục `STAGE5_ENABLED=false`.
