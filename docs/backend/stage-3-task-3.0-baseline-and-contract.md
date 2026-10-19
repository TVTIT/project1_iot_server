# Task 3.0 — Baseline, contract và kết quả spike

**Ngày thực hiện:** 15/10/2026 theo ngày của phiên làm việc.  
**Revision xuất phát:** `6de2c9542290fa1223072015cea3d5fd316b8bfe`, worktree sạch trước thay đổi. Kết quả áp dụng cho revision đó cộng các file Task 3.0 đang chưa commit.  
**Kết quả:** Hoàn thành 3.0.1–3.0.5 trong phạm vi contract và spike. Chưa triển khai uplink hoặc nghiệm thu U01–U15.

## 1. Baseline đã đối chiếu — 3.0.1

| Thành phần | Kết quả |
| --- | --- |
| Toolchain | Host Go 1.27.1 `linux/amd64`; source, Docker builder và CI dùng Go 1.27.1. CI Gateway cross-build `CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7`. |
| TimescaleDB image | `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`; fixture chạy PostgreSQL **16.15**, TimescaleDB **2.29.2**. |
| Mosquitto | DynSec active trong root Compose/config; runtime image `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`. Chỉ kiểm source/image hiện có, chưa chạy lại broker/ACL. |
| Data principal | `bootstrap_backend`: nhận/subscribe literal `gateways/+/telemetry/#`, publish `gateways/+/acks/#`; manager riêng trên `$CONTROL`. Chưa cần bổ sung downlink/status ACL cho Task 3.0. |
| Schema source | Migrations đến `000019`; runner hiện có; chưa có migration 20. |
| Schema fixture | Bootstrap bằng migrations và role script hiện hữu; verify migration 19 PASS. Tracking có 1–3 và 6–19. Migration 4 Auth compatibility chưa áp dụng do fixture không có GoTrue/auth.users; migration 5 là optional dev seed. Đây là fixture schema ứng dụng, không phải toàn bộ Supabase stack. |
| Telemetry/ledger | Hai hypertable raw/temporal, dedup PK `(gateway_id,message_id)`. Telemetry thiếu boot/message/index; ledger thiếu topic/stream/range. FK Sensor/Gateway đã có. |
| Twin | Entity ownership/`hasSensor`, JSONB reported/desired, versions/timestamps, typed temporal constraint có sẵn; chưa có uplink writer. |
| Policies | Compression sau 7 ngày ở raw/temporal; retention đã bị migration 8 gỡ. |
| Go runtime | Server chưa compose data MQTT subscriber/queue/worker/ACK; `cmd/gateway` là placeholder. Queue/workers/payload đã parse; MQTT URL/client ID/username chỉ được Compose truyền cho data path chưa triển khai. Credential config có cách đọc riêng khi bật admin credential API. |

Nguồn đối chiếu: `src/internal/config/config.go`, `config/credential.go`, `cmd/server/main.go`, `cmd/gateway/main.go`, `src/go.mod`, `src/Dockerfile`, `.github/workflows/ci.yml`, `docker-compose.yml`, `scripts/mqtt-bootstrap.py`, migrations và `docs/dynsec/native-ci-scope.md`.

Không đọc/sửa `.env` deployment, dùng credential/volume deployment, rotate credential thật hoặc recreate stack đang chạy. Fixture tái sử dụng `DatabaseFixture` của `scripts/tests/task28_simplified_schema_runner.py`: credentials ngẫu nhiên, tmpfs, không publish port, cleanup bằng exact container ID và kiểm absence.

## 2. Contract đã chốt — 3.0.2

Đã cập nhật [MQTT protocol v1](../protocols/mqtt-v1.md), giữ field và phần downlink/status cũ:

- Topic cụ thể `gateways/<gateway_id>/telemetry/<sensor_id>`; subscription literal không đổi; topic/payload phải khớp identity được broker ACL bảo vệ.
- Field types, số hữu hạn, UUID/identifier, count/overflow, RFC3339 UTC microsecond, timestamp mẫu cuối, sample span/future skew; raw v1 chưa có inference fields.
- Hash SHA-256 trên bytes nguyên bản, `source_topic` binding; replay/conflict, reject overlap cùng boot, chấp nhận gap/late nonoverlap, sequence độc lập qua boot.
- Transaction ledger + raw + reported + temporal; current state ordering tại DB, equal-time policy rõ ràng, giữ desired fields và reported properties khác.
- Một temporal metadata observation `sample_interval_us`/batch; không copy raw values sang hypertable thứ hai.
- Commit-before-ACK, `committed_at` là thời điểm xác nhận, ACK retry có thể đổi timestamp, không NACK/DB ACK outbox và không xóa pending item bằng MQTT PUBACK.

Ví dụ 3 mẫu đã đối chiếu: sequences 12501/12502/12503 tại 0/10/20 ms; candidate current là 25.3 tại 20 ms; temporal interval 10000 us cùng timestamp. Ví dụ là dữ liệu tổng hợp, không phải message đã chạy qua pipeline.

## 3. DDL spike — 3.0.3

Các file có thể chạy lại: [runner](../../scripts/spikes/stage3-task30/ddl_spike.py), [seed](../../scripts/spikes/stage3-task30/seed.sql), [DDL nháp](../../scripts/spikes/stage3-task30/candidate.sql), [SQL assertions](../../scripts/spikes/stage3-task30/verify.sql).

Chạy từ repository root:

```sh
python3 scripts/spikes/stage3-task30/ddl_spike.py
```

Yêu cầu Docker và pinned image; helper hiện hữu dùng thư mục tạm `/tmp/opencode`. Runner không thực hiện deployment migration. Hai lượt fixture cô lập đều PASS; lượt cuối đã bao gồm số đo trước/sau và xác minh lỗi trực tiếp đúng nguyên nhân.

### 3.1. Dữ liệu thử và kết quả

- Baseline có 1 Gateway, 1 Sensor, hai Twin entities, `hasSensor`, hai state rows; **3 raw rows**, **1 legacy marker**. Một chunk compressed và một chunk chưa compressed trước DDL.
- Thử thêm cột với default `nextval(...)` trực tiếp khi columnstore bật bị từ chối đúng lỗi: `cannot add column with non-constant default expression to a hypertable that has columnstore enabled`.
- Candidate: lưu danh sách chunk compressed và trạng thái scheduled của compression job → pause job → decompress → tắt compression của bảng → đổi schema → bật lại compression → recompress đúng chunk cũ → khôi phục scheduled → commit. Tất cả trong một transaction fixture, lock timeout 5s/statement timeout 30s.
- Đổi PK thành `(measured_at,sample_id)`, dùng sequence BIGINT server-side; backfill surrogate cho raw cũ. Thêm unique partial index `(measured_at,gateway_id,message_id,sample_index)` khi message ID khác NULL, và index stream/range cho ledger.
- Legacy giữ provenance NULL; constraint `CHECK ... NOT VALID` vẫn kiểm new writes, nên không cho insert raw/marker mới thiếu provenance. Hai CHECK này có `convalidated=false` có chủ đích; chưa thể `VALIDATE` khi legacy rows vẫn thiếu provenance. Không bịa boot/message UUID cho dữ liệu cũ.
- SQL assertions: bảo toàn toàn bộ count/giá trị/time/sequence cũ; hai raw samples cùng time/sequence nhưng boot/message khác insert được; duplicate sample identity bị từ chối; raw/marker mới thiếu provenance bị từ chối.
- `SET ROLE iot_backend_app`: insert/select theo grant sẵn có và sequence USAGE mới thành công; CREATE TABLE bị từ chối. Đây là role authorization check trong session fixture, không phải kiểm lại HTTP hoặc TCP credential login.
- Sensor FK, historical index, hai hypertable, compressed chunk cũ và compression job còn đúng; retention vẫn tắt; `schema_migrations` vẫn có max version 19. Sau verification có 5 raw rows và 3 chunks (một chunk mới do insert date thử), vẫn đúng một chunk cũ compressed.
- Cleanup container/tmpfs/thư mục fixture được helper xác minh **True** sau mỗi lượt; không giữ volume thử.

### 3.2. Số đo và giới hạn

Lượt cuối: DDL khoảng **0,099 giây** với 3 raw rows; `hypertable_size('telemetry')` trước DDL **172.032 bytes**, sau DDL **và thêm 2 raw rows/chunk kiểm chứng** **278.528 bytes**. Đây không phải overhead thuần của DDL hoặc estimate upgrade database thật. Decompression làm tăng nhu cầu dung lượng khi chuyển đổi; phải kiểm dung lượng thực trước migration chính thức.

Timescale phát warning rằng `sample_id`, `message_id`, `sample_index` nên tham gia segment/order khi có các khóa mới. Không có lỗi correctness trong SQL assertions hiện tại; giữ warning cho Task 3.2, chưa tùy tiện đổi compression layout hoặc tuyên bố hiệu năng production đã đạt.

### 3.3. Quyết định bàn giao Task 3.2

1. Dùng hướng candidate đã thử; migration dự kiến `000020_stage3_telemetry_ingestion.up.sql` nếu vẫn là version kế tiếp. **Chưa tạo file migration chính thức.**
2. Preflight schema/legacy data/chunks/job state và dung lượng; dừng writers khi upgrade. DDL nháp có thử pause scheduled nhưng chưa thử job đang chạy; migration/runbook chính thức phải chờ compression job đang chạy kết thúc trước maintenance.
3. Legacy NULL chỉ được đọc là legacy, không suy đoán topic/hash binding; marker legacy cần disposition có giám sát trước ACK. Raw/ledger mới bắt buộc đầy đủ provenance.
4. Backend giữ quyền DML hiện có, chỉ thêm USAGE sequence; không cấp DDL/superuser. Dùng `READ COMMITTED` và khóa Sensor trong repository sau này để bảo vệ range/atomic writes; spike chưa implement nghiệp vụ này.
5. Thử migration chính thức fresh/upgrade, error rollback và affected regressions ở Task 3.2. Spike này không chứng minh concurrent ingestion, commit ACK, exact crash/power-loss hoặc migration trên database deployment.

## 4. SQLite driver — 3.0.4

**Chọn:** `modernc.org/sqlite v1.60.1`, transitive `modernc.org/libc v1.77.1` đúng dependency được driver pin. Module và checksums nằm riêng trong [spike SQLite](../../scripts/spikes/stage3-task30/sqlite/go.mod); `src/go.mod`/`src/go.sum` không đổi.

Đã đọc [tài liệu chính thức của driver v1.60.1](https://pkg.go.dev/modernc.org/sqlite@v1.60.1): pure Go, hỗ trợ Linux amd64/arm và yêu cầu giữ libc version tương ứng. Quyết định dựa thêm vào build/smoke thực tế dưới đây, không chỉ dựa bảng support.

Chương trình spike chỉ tạo bảng `probe`, insert một row trong transaction, commit, đóng DB, mở connection mới và kiểm count/value. Connection pool giới hạn 1 và context 10s. Nó không chứa schema outbox, sequence allocation hoặc MQTT producer.

| Kiểm tra | Kết quả |
| --- | --- |
| Host Go 1.27.1, CGO=0 build + run amd64 | PASS; SQLite **3.53.4**, exact persisted row sau close/reopen. |
| Linux ARMv7 CGO=0 cross-build | PASS; `file` xác nhận ELF32 ARM EABI5 static. Chưa chạy trên AM5728/ARM thật hoặc emulator. |
| Docker builder `golang:1.27.1-alpine`, runtime Mosquitto đã ghim | PASS; binary chạy UID 1883, read-only root, network none, tmpfs writable; cùng SQLite 3.53.4 và persisted-row assertion. |
| `go vet ./...` trong module spike | PASS. |

Lệnh tái lập (cache/output ở thư mục tạm; chạy các lệnh Go từ thư mục `scripts/spikes/stage3-task30/sqlite`):

```sh
GOMODCACHE=/tmp/task30-go-mod GOCACHE=/tmp/task30-go-build go mod download
GOMODCACHE=/tmp/task30-go-mod GOCACHE=/tmp/task30-go-build CGO_ENABLED=0 go build -trimpath -o /tmp/task30-sqlite-amd64 .
/tmp/task30-sqlite-amd64
GOMODCACHE=/tmp/task30-go-mod GOCACHE=/tmp/task30-go-build CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -trimpath -o /tmp/task30-sqlite-armv7 .
GOMODCACHE=/tmp/task30-go-mod GOCACHE=/tmp/task30-go-build CGO_ENABLED=0 go vet ./...
docker build --pull=false -t task30-sqlite-spike:local .
docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges:true --tmpfs /tmp:rw,uid=1883,gid=1883,mode=0700,size=16m task30-sqlite-spike:local
```

Dockerfile spike dùng đúng builder/runtime của dự án; không chỉnh `src/Dockerfile` hoặc thêm target Gateway. Task 3.5 sẽ đưa exact pin sang module chính và kiểm build tích hợp khi simulator thực sự import driver; đặc biệt giữ libc version đúng khi giải dependency. Spike đóng/mở file không chứng nhận durable outbox đầy đủ hoặc power-loss durability.

Container SQLite tự xóa bằng `--rm`; đã kiểm không còn container theo image thử và xóa tag image tạm sau smoke. Go cache/binary tạm ở `/tmp` phục vụ tái lập, không chứa deployment secrets.

## 5. Cấu hình đã chốt cho implementation — 3.0.5

Đây là **contract cấu hình**, chưa wiring trong `internal/config`, `.env.example` hoặc Compose. Biến deployment/secret vẫn lấy từ environment. Không thêm manager credential dependency vào data ingress.

### 5.1. Backend

Ký hiệu: **G** đã parse ở Go core, **C** mới truyền qua Compose cho data path, **M** mới đề xuất. Với cờ ingress tắt, biến data-specific chưa bắt buộc; khi bật thiếu/sai thì startup fail rõ.

| Biến | Trạng thái hiện tại | Kiểu/default đích; demo | Validation và task nối |
| --- | --- | --- | --- |
| `MQTT_INGRESS_ENABLED` | M | bool, false; demo true | Parse bool nghiêm ngặt; độc lập credential flag; 3.4. |
| `MQTT_BROKER_URL` | C | URL, required khi bật | Chỉ `ssl://host:port`, không userinfo/query/fragment, CA/hostname verify; 3.4. |
| `MQTT_CLIENT_ID` | C | string required | Không rỗng, riêng ingress, không dùng cùng manager/probe/Gateway ID; 3.4. |
| `MQTT_USERNAME` | C | string required | Backend data principal, không manager; 3.4. |
| `MQTT_PASSWORD` | C; credential config đọc riêng khi bật admin API | secret required | Không dùng shared/default secret; không log/argv; 3.4. |
| `MQTT_TLS_CA_FILE` | C; credential config có đọc riêng | path required | Readable CA certificate, không private key, không insecure fallback; 3.4. |
| `MQTT_QUEUE_CAPACITY` | G: 256, range 1–1.000.000 | positive int; demo 256 | Queue hữu hạn, startup kiểm tổng memory budget cấu hình; 3.4. |
| `MQTT_WORKER_COUNT` | G: 4, range 1–10.000 | positive int; demo 4 | Chừa connection pool cho HTTP/credential owner; 3.4. |
| `MQTT_MAX_PAYLOAD_BYTES` | G: 1.048.576 | positive bytes; demo 65.536 | Giữ default hiện có cho tới đổi có chủ ý; byte check trước decode/allocation; 3.1/3.4. |
| `TELEMETRY_MAX_SAMPLES_PER_MESSAGE` | M | positive int, 1.000 | Phù hợp INTEGER sample index/count, bounded cùng payload; 3.1/3.4. |
| `TELEMETRY_MAX_BATCH_DURATION` | M | duration, 60s | >0, kiểm sample span, multiplication không overflow; 3.1/3.4. |
| `TELEMETRY_MAX_FUTURE_SKEW` | M | duration, 2m | >=0, kiểm timestamp mẫu cuối với receipt time; 3.1/3.4. |
| `TELEMETRY_DB_TIMEOUT` | M | duration, 5s | >0, áp cả acquire/transaction; nhỏ hơn shutdown budget; 3.2/3.4. |
| `MQTT_CONNECT_TIMEOUT` | M | duration, 5s | >0, bound mỗi connect attempt; 3.4. |
| `MQTT_SUBSCRIBE_TIMEOUT` | M | duration, 3s | >0, check SUBACK, chưa subscribed thì chưa ready; 3.4. |
| `MQTT_PUBLISH_TIMEOUT` | M | duration, 3s | >0, ACK publish wait hữu hạn; token timeout không được mặc định là publish đã hủy; 3.4. |
| `MQTT_RECONNECT_MIN_DELAY`, `MQTT_RECONNECT_MAX_DELAY` | M | durations, 1s / 30s | 0 < min <= max; reconnect backoff; 3.4. |
| `DATABASE_MAX_CONNS`, `SHUTDOWN_TIMEOUT` | G; Compose có truyền | Core 10 / 10s; Compose shutdown 55s | Tái sử dụng; demo dùng Compose 10 / 55s; drain trong ngân sách còn lại, không đổi default ở task này. |

Queue demo bytes thô tối đa `256 × 65.536 = 16 MiB`, chưa tính decoded samples/MQTT/workers. Default payload cũ có thể tạo 256 MiB queue; phải ghi profile thực dùng. Ack pending/in-flight và MQTT internal offline queues phải bounded ở 3.4; chưa chọn implementation queue/publisher trong task này.

Broker packet-size setting gồm MQTT overhead, khác `MQTT_MAX_PAYLOAD_BYTES` chỉ tính payload. Task 3.4 tính overhead từ maximum topic/packet format thực dùng và đồng bộ broker template; không đặt cùng 65.536 rồi vô tình chặn payload hợp lệ. Hiện broker config chưa đặt packet-size limit này.

### 5.2. Simulator (Task 3.5)

| Biến đích | Kiểu/default demo | Validation |
| --- | --- | --- |
| `GATEWAY_ID`, `SENSOR_ID` | required identifiers | Cùng validator/topic contract. |
| `GATEWAY_MQTT_BROKER_URL`, `GATEWAY_MQTT_CLIENT_ID` | required URL/string | TLS; client ID không trùng backend hoặc Gateway khác. |
| `GATEWAY_MQTT_USERNAME`, `GATEWAY_MQTT_PASSWORD`, `GATEWAY_MQTT_CA_FILE` | required | Username = Gateway ID; own credential/CA, không manager/human JWT. |
| `GATEWAY_OUTBOX_PATH` | required path | SQLite file writable trong private directory; không dùng memory DB. |
| `GATEWAY_SAMPLE_RATE_HZ`, `GATEWAY_SAMPLES_PER_BATCH` | ints, 100 / 100 | >0; interval microsecond nguyên (`1.000.000 % Hz = 0`); batch phải thỏa server profile. |
| `GATEWAY_ACK_TIMEOUT` | duration, 5s | >0; MQTT PUBACK không xóa item. |
| `GATEWAY_RETRY_MIN_DELAY`, `GATEWAY_RETRY_MAX_DELAY` | durations, 1s / 30s | 0 < min <= max; jitter ±20%, clamp actual delay về max. |
| `GATEWAY_MAX_INFLIGHT` | int, 8 | >0, không vượt row bound. |
| `GATEWAY_OUTBOX_MAX_ROWS`, `GATEWAY_OUTBOX_MAX_BYTES` | ints, 5.000 / 67.108.864 | >0; bytes tính tổng topic + serialized payload của pending items; thêm item vượt bound thì dừng tạo mẫu/báo lỗi, không drop cũ. SQLite file/index/WAL overhead cần disk headroom, 64 MiB không phải giới hạn file vật lý tuyệt đối. |

Boot ID, message ID, protocol field names và topic shapes là protocol/runtime values, không biến thành secret/config deployment. Retention raw và cleanup ledger chưa bật; không thêm max-age hoặc TTL trái với producer retry.

### 5.3. Reason codes nội bộ

Không phải HTTP status hay NACK mới. Lỗi trước commit không phát ACK `committed`. Retry luôn giữ nguyên ID/topic/bytes; lỗi input không tự sửa bytes hoặc tự cấp ID khác.

| Code | Trường hợp | Cách xử lý retry |
| --- | --- | --- |
| `invalid_topic` | Sai shape/identifier hoặc suffix | Permanent input; giữ/cách ly item, sửa producer cho message mới. |
| `unsupported_protocol` | Sai version/type | Permanent input; cần producer tương thích. |
| `payload_too_large` | Vượt byte bound | Permanent với profile hiện tại; chia batch trước enqueue mới hoặc chỉnh profile có chủ ý. |
| `invalid_payload` | JSON/field/type/count/value/UUID sai | Permanent input; không partial write. |
| `invalid_timestamp` | Parse/precision/range/arithmetic sai | Permanent input; kiểm đồng hồ/producer. |
| `timestamp_in_future` | Timestamp mẫu cuối vượt receipt + skew | Kiểm đồng hồ; có thể resend cùng payload khi thời gian đã phù hợp, không tight loop. |
| `invalid_sequence_range` | Sequence/count overflow hoặc interval/span không hợp lệ | Permanent input với contract/profile hiện tại. |
| `unknown_gateway`, `unknown_sensor` | Chưa provision hoặc sai parent | Cần operator sửa provisioning/producer; không auto-create. |
| `resource_graph_incomplete` | Twin/state/ownership graph thiếu | Configuration intervention; retry sau sửa có kiểm soát. |
| `legacy_provenance_unknown` | Marker cũ không đủ topic/range | Supervised disposition; không ACK bằng giả định. |
| `message_conflict` | Same key khác hash/topic | Permanent conflict; giữ ledger cũ, operator kiểm tra. |
| `sequence_overlap` | New ID overlap cùng Sensor/boot | Permanent conflict; giữ batch cũ, không bỏ qua sample rồi success. |
| `queue_full` | Callback không enqueue được | Transient pressure; producer giữ outbox, retry có backoff. |
| `dependency_unavailable` | DB/acquire/transaction lỗi trước known commit | Transient; resend và đi qua dedup. |
| `commit_unconfirmed` | COMMIT trả lỗi/bất định | Không kết luận rollback/success; resend để resolve ledger. |
| `ack_publish_unconfirmed` | DB đã commit, chuyển ACK lỗi/timeout | Dữ liệu vẫn committed; retry được ACK lại, không rollback DB. |

Kết quả nghiệp vụ thành công dự kiến là `committed` hoặc `duplicate_committed`; wire ACK của cả hai vẫn `status=committed`, không đổi protocol shape.

## 6. Bàn giao và kiểm tra cuối

| Bước | Trạng thái |
| --- | --- |
| 3.0.1 baseline/source + DB fixture | DONE trong phạm vi ứng dụng; không kiểm deployment/Auth stack thật. |
| 3.0.2 protocol | DONE; đặc tả, chưa có Go validator/ingress mới. |
| 3.0.3 DDL spike | PASS assertions, expected direct-DDL rejection, candidate và cleanup. |
| 3.0.4 SQLite spike | PASS native + Docker runtime, ARMv7 compile; ARM runtime NOT RUN. |
| 3.0.5 config/error contract | DONE tài liệu; runtime wiring để 3.1–3.5. |

Bàn giao: Task 3.1 nhận protocol/config/reason codes; 3.2 nhận SQL candidate, legacy/NOT VALID và compression maintenance; 3.4 nhận topology/config/ACK semantics; 3.5 nhận SQLite pin/build và producer config. Không cần build lại server hoặc toàn bộ native CI vì module chính/runtime chưa thay đổi. Không chạy telemetry E2E, workload 100 Hz, ACL/restart acceptance, fault-injection campaign, remote CI hoặc board runtime trong task này.

Kiểm tra cuối: `git diff --check`, Python syntax, `gofmt`, `go vet` và `go mod verify` của module spike PASS. Các JSON examples parse được và phép tính timestamp/sequence khớp; phần downlink/status được kiểm giữ nguyên. Không có secret thật trong các file tạo mới.
