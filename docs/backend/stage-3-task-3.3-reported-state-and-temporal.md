# Task 3.3 — Sensor reported state và temporal trong cùng transaction

**Ngày hoàn tất:** 15/10/2026.

**Trạng thái:** Hoàn thành bốn đầu việc Task 3.3.1–3.3.4 và kiểm nghiệp vụ DB-only Task 3.3.5. Báo cáo này ghi bằng chứng đúng phạm vi; chưa nối MQTT/ACK runtime.

**Baseline:** nhánh `develop`, HEAD `671ed358935d027b589ee60a384d765ee924bb66` ("chore(agent): xoá file agent.md khỏi git"), branch đang ahead `origin/develop` 8 commit. Các thay đổi Task 3.3 **chưa commit** (dirty worktree). Go `1.27.2`; migration schema hiện hành là 20; không tạo migration mới.

## 1. Đối chiếu implementation với kế hoạch

Đã đọc `git status` (gồm file untracked) và `git diff` đầy đủ. Toàn bộ file kế hoạch yêu cầu đều hiện diện, không mất file:

**Sửa (tracked, chưa commit):**

- `src/internal/telemetry/postgres_repository.go` — gọi hai helper Twin/temporal sau khi `CopyFrom` đủ `sample_count`, trước `tx.Commit`.
- `src/internal/telemetry/repository.go` — cập nhật comment `Repository`/`PersistResult`: committed xác nhận ledger + raw + current state + temporal cùng commit.
- `src/internal/telemetry/validation.go` — thêm bound `maxExactFloat64Integer = 2^53` cho `sample_interval_us`.
- `src/internal/telemetry/postgres_integration_test.go` — fixture mở rộng.
- `src/internal/telemetry/postgres_repository_test.go`, `validation_test.go` — unit test bổ sung.
- `docs/protocols/mqtt-v1.md` — contract `sample_interval_us` ghi bound 2^53.

**Thêm mới (untracked):**

- `src/internal/telemetry/postgres_reported_state.go` (+ `_test.go`) — helper reported state.
- `src/internal/telemetry/postgres_temporal.go` (+ `_test.go`) — helper temporal `sample_interval_us`.

Đối chiếu các điểm correctness cụ thể (không mở rộng proof gate):

- **Không mất file:** cả bốn helper untracked đã nằm trong worktree và được unit test bao phủ.
- **An toàn cast JSON cho state legacy:** `storedSensorProvenance` decode bằng `UseNumber`; JSON hỏng, `boot_id` không phải string, `sequence_num` không phải số nguyên không âm đều trả "không khả dụng" → giữ current state, **không** làm lỗi transaction cho batch raw hợp lệ (đúng kế hoạch 3.3.2.3). Không suy thứ tự boot từ UUID.
- **Bảo toàn desired:** SQL advance chỉ set `reported_state`, `reported_version`, `last_reported_at`, `updated_at`; unit test `TestSensorReportedStateSQLKeepsAllowlistAndLock` khẳng định SQL không chứa `desired`; integration so snapshot `desired_state/desired_version/last_desired_at/last_desired_by` và Gateway Twin trước/sau.
- **Ordering dưới row lock:** `resolveTelemetryResource` giữ `SELECT ... FOR UPDATE` trên `sensors` (resource), `applySensorReportedState` đọc `twin_states ... FOR UPDATE` rồi UPDATE có guard `reported_version = $4`; điều kiện thứ tự nằm trong transaction, không kiểm Go trước rồi update vô điều kiện.
- **Temporal chính xác:** đúng một hàng `twin_temporal_values` cho batch `new`, `property_name='sample_interval_us'`, `value_number=float64(interval)`, `value_text/value_boolean=NULL` (thỏa `num_nonnulls=1`), `observed_at` = measurement time mẫu cuối, `message_id` = batch, metadata `unit=us`,`boot_id`,`source=telemetry_batch`; retry đi nhánh duplicate trước nên không append lần hai.
- **Duplicate bypass:** nhánh `ledgerDispositionDuplicate` commit ledger rồi return, không chạy raw/Twin/temporal.
- **Rollback / commit ambiguity:** lỗi helper trả error (zero result) → deferred rollback; lỗi `Commit` → `commit_unconfirmed`, không tự retry và không coi là thành công.

Không phát hiện defect correctness/security cụ thể cần sửa.

## 2. API semantics

- `Repository.Persist(ctx, *Message) (PersistResult, error)` sở hữu một `pgx.Tx`: `resolveTelemetryResource` (Gateway/Sensor + Twin graph, Sensor `FOR UPDATE`) → ledger disposition → nếu duplicate: commit và trả `CommitOutcomeDuplicateCommitted` → nếu new: `CopyFrom` đủ raw → `applySensorReportedState` → `appendSensorSampleIntervalTemporal` → `Commit`.
- `applySensorReportedState` chỉ **tiến** khi `last_reported_at IS NULL`, measurement time mới hơn, hoặc bằng nhau + stored `boot_id` hợp lệ khớp + stored `sequence_num` nhỏ hơn. Tiến thì merge JSONB allowlist (`value`, `observed_at`, `boot_id`, `sequence_num`, `sample_interval_us`, và `unit` nếu Sensor có unit provision) và `reported_version + 1`. Thiếu hàng `twin_states` → lỗi graph, rollback.
- `appendSensorSampleIntervalTemporal` từ chối interval ≤ 0 hoặc > 2^53 trước khi ghi (không âm thầm làm tròn qua `DOUBLE PRECISION`).
- `CommitOutcomeCommitted` giờ xác nhận ledger + raw + current state + temporal cùng commit; zero value và `commit_unconfirmed` **không** phải thành công. Repository vẫn **không** publish ACK (Task 3.4).

## 3. Database và bài kiểm native

Fixture cô lập tái sử dụng `scripts/test-stage3-persistence.sh` → `scripts/tests/stage3_persistence_runner.py`:

- Image ghim: `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`.
- Runtime thực tế: PostgreSQL **16.15**, TimescaleDB **2.29.2**.
- App connection role `iot_backend_app`; setup/admin connection chỉ seed/assert/cài trigger lỗi.

### Normal path (batch 100 mẫu đầu tiên)

1 ledger; 100 raw; Sensor `reported_state.value` = mẫu cuối (`99.25`); `boot_id`/`sequence_num`/`observed_at` (`t0+990ms`)/`sample_interval_us=10000` đúng; `unit` từ metadata provision (`degC`); property khác (`temperature:"21.5"`) giữ nguyên; `reported_version` 7→8 (**+1**); `last_reported_at` = measurement time mẫu cuối; đúng **1** temporal row (`value_number=10000`, `value_text/value_boolean NULL`, `message_id`, `observed_at`, metadata `unit=us/boot_id/source=telemetry_batch`); `desired_state/desired_version/last_desired_at/last_desired_by` và Gateway Twin không đổi.

### Per-case deltas (từ assertion trong `postgres_integration_test.go`)

- **Retry chính xác:** `DuplicateCommitted`; version giữ 8; temporal giữ 1.
- **Conflict (cùng ID khác topic) và overlap inclusive:** reject trước bước Twin; version giữ 8; temporal giữ 1.
- **Ba batch cũ (new, không overlap):** adjacent `12601`, gapped `12603`, batch boot khác — mỗi batch commit (+1 ledger/raw/temporal) nhưng current/version giữ 8; tổng temporal = 4.
- **Same-ID race (2 connection):** một commit, một duplicate; version `+1` so baseline race; temporal `+1`.
- **Range race (2 ID overlap):** một commit, một `sequence_overlap`; version = 9; temporal = 6.
- **Sensor cùng ID ở Gateway khác (`fixture_gw2`):** 1 temporal, version 1; Sensor `fixture_gw` giữ 9.
- **Ordering (seed state version 50, `last_reported_at=t0+2s`):**
  - (a) time mới hơn, boot khác → advance version 51, seq 4001, value 7.5; +1 ledger/raw/temporal.
  - (b) time cũ hơn → hold 50; +1 mỗi loại.
  - (c) time bằng, boot khác → hold 50; +1 mỗi loại.
  - (d) time bằng, cùng boot, sequence lớn hơn → advance 51 (seq 5001, value 10.5); +1 mỗi loại.
  - (e) time bằng, cùng boot, sequence bằng → hold 50; +1 mỗi loại.
  - (f) time bằng, cùng boot, sequence nhỏ hơn → hold 50; +1 mỗi loại.
  - (g) legacy thiếu boot+sequence / thiếu sequence / boot hỏng / sequence hỏng → hold 50; +1 mỗi loại.
- **Hai connection đảo thứ tự qua Sensor lock (đo `wait_event_type='Lock'`, không sleep):**
  - (1) batch time mới hơn xếp hàng trước → advance 51; batch cũ hơn xếp sau → hold; cả hai +1 ledger/raw/temporal; desired không đổi.
  - (2) FIFO: cùng time/cùng boot, sequence tăng — batch seq 9501 được cấp lock trước, batch 9502 sau → version cuối 52 (`orderVersion+2`), value 52.5. Queue inversion sẽ chỉ còn +1 nên phép đếm này chứng minh đúng thứ tự cấp lock.
- **SQL failure injection sau raw CopyFrom:** trigger `BEFORE UPDATE` trên `twin_states` và trigger `BEFORE INSERT` trên `twin_temporal_values` — mỗi ca `Persist` trả zero result + error; ledger/raw/temporal giữ nguyên; current và desired giữ nguyên; dữ liệu commit trước đó nguyên vẹn.
- **Round-trip 2^53:** interval `9007199254740992` lưu `value_number` chính xác `float64(2^53)` và `reported_state.sample_interval_us` đúng chuỗi; version advance 51.
- **Cuối:** desired state/version/audit của Sensor không đổi qua toàn bộ ca ordering/rollback.

Runner in tổng fixture sau khi chạy (`raw_count=226`, `ledger_count=23`, `hypertables=2` với `telemetry` + `twin_temporal_values`) — đây là **tổng tích lũy toàn bài integration**, không dùng làm bằng chứng per-case; các delta per-case ở trên lấy từ assertion của test.

## 4. Lệnh và kết quả

Chạy trong `src/` trừ khi ghi chú khác:

| Lệnh | Kết quả |
| --- | --- |
| `gofmt -l internal/telemetry/*.go internal/gateway/*.go` | PASS (không file nào bị liệt kê) |
| `go build ./...` | PASS |
| `git diff --check` (repo root) | PASS (không whitespace error) |
| `go test -race -count=1 ./internal/telemetry ./internal/gateway` | PASS (`ok` cả hai package) |
| `go vet ./...` | PASS (exit 0) |
| `golangci-lint run ./internal/telemetry/... --timeout=5m` | PASS (`0 issues.`) |
| `golangci-lint run ./... --timeout=6m` | PASS (`0 issues.`) |
| `sh scripts/test-stage3-persistence.sh` | PASS — log `/tmp/opencode/stage3-task33.log` |

Native runner PASS cả ba fixture: **FRESH** (migration 20 + grants + replay + Go integration normal), **UPGRADE** (v19 data preservation + late-data compressed chunk + legacy marker replay), **ROLLBACK** (injected DDL failure rollback + operator job restore). Marker Go integration: `INTEGRATION CASES PASS: ... atomic Sensor reported-state advance + one sample_interval_us temporal per new batch, desired/Gateway preservation, DB ordering..., deterministic two-connection Sensor-lock ordering, twin_states/temporal SQL rollback, 2^53 interval round-trip`; `--- PASS: TestPostgresTelemetryIntegration (0.22s)`. Cả ba fixture dọn và xác minh cleanup `True`.

Không chạy lại các migration/provisioning regression Stage 2 vì Task 3.3 không đổi migration, schema hay provisioning; runner Stage 3 đã bao gồm rehearsal migration 20 (PASS trong log cùng lượt).

## 5. Giới hạn và NOT RUN

- **Không** chạy `go test` tích hợp mặc định (test tự skip trừ khi `TELEMETRY_TEST_ISOLATED=1`); chỉ chạy qua native runner.
- **Không** triển khai migration lên database đang dùng; migration chỉ chạy trong fixture cô lập.
- **Không** nối MQTT subscriber, queue/worker, server composition hay application ACK (Task 3.4). `Persist` vẫn không publish ACK.
- **Không** có simulator (3.5), broker E2E/TLS/ACL (3.6), workload sustained hay CI lane (3.7).
- **Không** có REST đọc Twin, history API, realtime event hay downlink command trong Task 3.3.
- **Không** chạy remote CI, board runtime, hay database deployment thật.
- Fixture nhỏ; các số liệu không phải benchmark tải.
- U01–U15 **chưa** được nghiệm thu end-to-end chỉ từ kiểm thử DB này.

## 6. Bàn giao cho Task 3.4

Task 3.4 có thể dùng `Repository.Persist` với outcome chỉ thành công sau transaction đầy đủ (ledger + raw + current state + temporal) và nhánh duplicate chỉ xác nhận ledger. Caller sau commit mới được cân nhắc publish application ACK; repository không thực hiện việc đó.
