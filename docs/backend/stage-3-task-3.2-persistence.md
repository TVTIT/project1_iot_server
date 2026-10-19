# Task 3.2 — Migration, ledger và raw persistence

**Ngày hoàn tất:** 15/10/2026.

**Trạng thái:** Hoàn thành Task 3.2.1–3.2.7 trong phạm vi persistence DB-only.
**Baseline:** `e98717d` cộng các thay đổi Task 3.2 chưa commit; Go `1.27.2`, dependency giữ nguyên.

Task 3.2 đưa schema từ migration 19 lên 20 và bổ sung repository lưu một telemetry message đã qua `Validator.Validate` thành ledger cùng các raw sample trong một transaction. Bằng chứng này chưa hoàn thiện luồng uplink end-to-end: server chưa nối repository vào MQTT subscriber/queue, chưa ghi Twin reported/temporal và chưa publish application ACK.

## Phần đã thực hiện

### Phân công tuần tự

Toàn bộ mười lượt được thực hiện tuần tự bởi GPT-6-Luna; không có chỉnh sửa đồng thời. Lượt 1–6 xây repository/migration và unit coverage; lượt 7 tích hợp DB; lượt 8–9 cập nhật compatibility/regression Stage 2; lượt 10 chạy final checks và viết bàn giao.

| Lượt | Agent | Công việc |
| --- | --- | --- |
| 1 | `task32_repository` | Transaction repository interface và ranh giới implementation. |
| 2 | `task32_migration` | Migration 20, preflight và phương án maintenance. |
| 3 | `task32_migration` | SQL fixtures/verifiers thật cho fresh, upgrade và failure. |
| 4 | `task32_repository` | Resource resolution, ownership/graph và Sensor lock. |
| 5 | `task32_repository` | Ledger idempotency, conflict và sequence overlap. |
| 6 | `task32_repository` | Raw `CopyFrom`, commit outcomes, orchestration/unit tests. |
| 7 | `task32_integration` | PostgreSQL integration và các chỉnh hẹp theo review. |
| 8 | `task32_migration` | Regression migration Stage 2 và tương thích schema 20. |
| 9 | `task32_migration` | Compatibility/provisioning regression. |
| 10 | `task32_integration` | Final checks và tài liệu bàn giao. |

Migration [`000020_stage3_telemetry_ingestion.up.sql`](../../migrations/000020_stage3_telemetry_ingestion.up.sql) thêm provenance cho ledger và telemetry, `sample_id`, index/ràng buộc cho dữ liệu mới và quyền ghi cho `iot_backend_app`; giữ hai hypertable hiện có `telemetry` và `twin_temporal_values`, không tạo hypertable mới. Legacy rows được giữ nguyên với provenance NULL; CHECK mới là `NOT VALID` để áp dụng với rows mới mà không giả lập metadata lịch sử. Migration pause compression job bên ngoài transaction DDL, đợi trạng thái đang chạy trong giới hạn đã định, thực hiện thay đổi/backfill/recompression/tracking trong transaction, rồi khôi phục lịch chạy khi migration thành công. Nếu DDL thất bại, pause autocommit không tự rollback; operator phải khôi phục lịch từ snapshot mà runner in ra. Fixture đã diễn tập lỗi DDL và khôi phục job thành công.

SQL preflight, seed upgrade và verifiers nằm trong `scripts/sql/`. Runner [stage3_persistence_runner.py](../../scripts/tests/stage3_persistence_runner.py), gọi bằng `sh scripts/test-stage3-persistence.sh`, tái sử dụng `DatabaseFixture`: image ghim, container riêng không publish ports, receipt ownership, thư mục/env private, timeout và cleanup xác minh. Runner compile test binary Go với `CGO_ENABLED=0`, copy binary vào TimescaleDB container và chạy tại đó. App connection TCP đăng nhập bằng `iot_backend_app`; setup/admin connection chỉ seed, assert và cài trigger lỗi fixture. DSN được ghép trong shell từ container environment, không đưa credential vào argv/stdout.

Trước khi chạy native entrypoint trên máy mới, tạo thư mục tạm mà `DatabaseFixture` hiện dùng: `mkdir -p /tmp/opencode`. Để chỉ xác minh schema trên database đã migrate, operator có thể dùng `psql -v stage3_verify_writes=false -f scripts/sql/verify-migration-000020.sql` với biến `PG*` được cấp qua environment quản lý an toàn; không đặt credential thành tham số argv. Mặc định `stage3_verify_writes=true` của verifier dành cho DML fixture, không chạy mặc định trên database có dữ liệu thật.

Nếu migration DDL lỗi sau khi job đã pause, giữ snapshot `job_id`/`scheduled` được in ra và đợi kết nối DDL kết thúc. Không chạy lại migration mù khi chưa biết trạng thái. Khôi phục từng job từ snapshot, thay placeholder bằng literal đã quan sát:

```sql
SELECT alter_job(<job_id>, scheduled => <saved_boolean>);
```

Sau đó truy vấn lại `timescaledb_information.jobs` để xác nhận lịch. Trước khi retry migration, operator cần xác minh schema/tracking hiện trạng và chạy preflight lại.

`NewPostgresRepository(TxBeginner, timeout)` sở hữu `BeginTx`/`Commit`/rollback. `Persist(ctx, *Message)` xác nhận Gateway/Sensor và Twin graph đã provision, khóa Sensor, thực hiện ledger idempotency/conflict/overlap, sau đó `CopyFrom` đủ samples và kiểm số hàng trước commit. Error trả zero result; lỗi Commit được phân loại `commit_unconfirmed`. Repository không sửa Twin state/temporal, không gửi MQTT và không tạo ACK. Chi tiết interface và helpers ở [`src/internal/telemetry/`](../../src/internal/telemetry/).

## Bằng chứng kiểm thử

Native fixture dùng image `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`; runtime thực tế PostgreSQL `16.15`, TimescaleDB `2.29.2`. Fresh DDL runner mất khoảng `0.44s`; upgrade runner khoảng `0.35s` trong fixture nhỏ. Upgrade seed có ba legacy telemetry rows và một ledger marker. Kích thước hypertable được báo trước/sau gồm cả dữ liệu do test SQL/Go chèn, nên không thể dùng làm ước lượng thuần overhead migration hoặc extrapolate sang production.

Lệnh chính:

```sh
sh scripts/test-stage3-persistence.sh
```

Fresh fixture, upgrade v19 có dữ liệu, SQL verifiers, quyền app, DDL failure/rollback và migration replay đều PASS; cả ba fixtures được dọn và xác minh. Go integration đã chạy thực sự (không skip) với app role, bao gồm:

- Một message qua validator, 100 samples: kiểm từng value/index/Gateway/Sensor/boot/message/time; sequence `12501–12600`, mẫu cuối tại `t0 + 990ms`; ledger khớp hash/topic/range/count. App `current_user` là `iot_backend_app`.
- Retry chính xác trả duplicate; đổi bytes bằng whitespace và đổi topic sang Sensor đã provision trả `message_conflict`. Ledger và raw rows được snapshot theo nội dung và giữ nguyên.
- Unknown Gateway/Sensor và graph thiếu relationship, Sensor state hoặc Gateway state; entity sai Gateway ownership. Các ca từ chối không tạo ledger/raw. Cùng Sensor ID ở hai Gateway được lưu đúng theo Gateway trong topic.
- Range endpoint inclusive overlap bị từ chối; range adjacent, gap và sequence/time dùng lại trên boot khác được chấp nhận.
- Trigger fixture gây SQL error tại `sample_index=50`; transaction rollback cả marker và raw batch, dữ liệu trước đó giữ nguyên.
- Hai app connections được acquire riêng trước barrier: cùng ID/hash/topic cho đúng một commit, một duplicate và đúng một ledger/100 raw rows. Hai ranges khác ID overlap cho một commit, một `sequence_overlap`, đúng ledger/raw count của batch thắng; không giả định worker thắng.
- Snapshot Twin `reported_state`, `desired_state`, versions trước Persist và temporal row count được so sánh sau các thao tác; giá trị giữ nguyên.
- Upgrade fixture replay marker provenance legacy trả `legacy_provenance_unknown`, giữ marker/raw nguyên vẹn. `CopyFrom` cũng chèn được late data vào compressed chunk legacy; mốc thời gian được đọc từ legacy snapshot trong compressed chunk.

Kiểm tra cuối:

```sh
gofmt -l internal/telemetry/*.go
go test -race -count=1 ./internal/telemetry ./internal/gateway
go vet ./internal/telemetry/...
golangci-lint run ./internal/telemetry/... --timeout=5m
go build ./...
git diff --check
```

Các lệnh trên PASS; lint báo `0 issues`. `python3 -m py_compile scripts/tests/stage3_persistence_runner.py` và `sh -n scripts/test-stage3-persistence.sh` cũng PASS. Các regression bị ảnh hưởng đã chạy PASS: `sh scripts/test-stage2-migrations.sh`, `python3 scripts/tests/migration_ci_compat.py`, `sh scripts/test-stage2-provisioning.sh`, và `python3 -m unittest scripts/tests/test_ci_migration19.py scripts/tests/test_ci_migration_job.py scripts/tests/test_migration_ci_compat.py` (50 tests). Những Stage 2 acceptance khác không được mở lại.

## Bàn giao và giới hạn

Task 3.3 có thể dùng entity resolution, Sensor `FOR UPDATE`, resolved entity UUID, các helper cùng `pgx.Tx` và điểm trước lần Commit trong `Persist`. Hãy thêm Twin state/temporal writes tại transaction đó, bảo toàn `desired_state` và kiểm rollback chung với raw/ledger. Task 3.4 có thể dùng outcome committed/duplicate, reason codes, timeout và `commit_unconfirmed`; chỉ caller sau commit mới được cân nhắc ACK. Task 3.2 không chứng minh ACK delivery.

Legacy ledger thiếu topic/range/boot không thể suy ra provenance; retry cần disposition có giám sát và hiện trả `legacy_provenance_unknown`. Nếu migration DDL lỗi sau khi compression job đã pause, cần operator restore lịch chạy từ snapshot. Cảnh báo layout compression đã được ghi nhận; workload lớn chưa đo và fixture nhỏ không phải benchmark.

**NOT RUN:** MQTT/TLS/ACL, application ACK, simulator, tải sustained/workload, CI remote, deployment database thật và build/deploy backend trên server thật. U01–U15 và pipeline uplink end-to-end vẫn chưa được nghiệm thu. Migration chỉ được chạy trong fixture cô lập, không được triển khai vào database đang dùng.
