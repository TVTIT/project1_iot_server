# Task 3.4 — MQTT client, queue, worker, ACK và vòng đời

**Ngày hoàn tất:** 16/10/2026.

**Trạng thái:** Hoàn thành năm đầu việc Task 3.4.1–3.4.5, phần server lifecycle 3.4.6,
unit/race tests và một smoke thật qua broker + TimescaleDB cho 3.4.7, và bàn giao
3.4.8. Báo cáo ghi đúng phạm vi đã kiểm; **không** phải nghiệm thu end-to-end đầy
đủ Stage 3 và **không** đánh dấu U01–U15 PASS hoàn toàn.

## 1. Đối chiếu baseline và phạm vi thay đổi

**Baseline thực tế:** nhánh `develop`, HEAD `2c4999e253b9ae0aa7ddeb3d121327a9c80cf9ce`
("test(telemetry): bổ sung kiểm thử tích hợp DB thật và báo cáo nghiệm thu Task 3.3").
Toàn bộ thay đổi Task 3.4 **chưa commit** (dirty worktree). Go `1.27.2`; schema
hiện hành vẫn là **20** (Task 3.4 không tạo migration mới).

Đã đọc `git status` (gồm file untracked) và `git diff` đầy đủ trước khi kiểm. Không
mất file kế hoạch yêu cầu.

**Sửa (tracked, chưa commit):**

- `src/internal/config/config.go` — bỏ ba trường `MQTTQueueCapacity/MQTTWorkerCount/MQTTMaxPayloadBytes` khỏi `Config`, gọi `loadIngress(...)` sau credential; validate ingress theo `DATABASE_MAX_CONNS`, cờ credential và `SHUTDOWN_TIMEOUT`.
- `src/cmd/server/main.go` — một đường shutdown duy nhất dùng chung `SHUTDOWN_TIMEOUT` cho HTTP + ingress drain + credential close; compose/start ingress; readiness ghép DB + ingress; `pool.Close()` vẫn deferred sau shutdown.
- `src/cmd/server/credential.go` — tách `closeContext(ctx)` để credential close dùng chung deadline shutdown (không tự đặt timeout riêng).
- `.env.example`, `docker-compose.yml`, `README.md` — đồng bộ flag/biến ingress mới, default payload 64 KiB.
- `config/mosquitto/mosquitto.conf` — thêm `max_packet_size 2097152` và `message_size_limit 1048576`.

**Thêm mới (untracked):**

- `src/internal/mqttingress/` — package ingress thuần transport-independent:
  - `interfaces.go` — `Validator`/`Repository`/`Publisher` nhỏ + conformance compile-time với `telemetry`.
  - `ingress.go` — admission bounded, queue, worker pool cố định, pipeline `Validate → Persist → post-commit ACK`.
  - `counters.go` — counters bounded-cardinality + `Snapshot` không chứa payload/credential.
  - `transport.go` + `paho.go` — data client Paho/TLS, supervisor reconnect, epoch/lifetime, publish semaphore, teardown fail-closed.
  - `ingress_test.go`, `lifecycle_test.go`, `lifecycle_race_test.go`, `transport_test.go` — unit/race tests.
- `src/cmd/server/ingress.go` — composition `ingressRuntime`, transport/ingress, composite readiness.
- `src/cmd/server/ingress_test.go`, `src/internal/config/ingress.go`, `src/internal/config/ingress_test.go`.
- `scripts/test-stage3-mqtt-ingress.sh` + `scripts/tests/stage3_mqtt_ingress_runner.py` — smoke thật disposable.

## 2. Cấu hình: default, caps và ngân sách

Nguồn: `src/internal/config/ingress.go`. Đây là giá trị khởi đầu, **chưa phải số đo capacity**.

| Nhóm | Biến | Default | Bound |
| --- | --- | --- | --- |
| Flag | `MQTT_INGRESS_ENABLED` | `false` | Độc lập `MQTT_CREDENTIAL_API_ENABLED` và `MQTT_CREDENTIAL_RUNTIME_ENABLED` |
| Identity | `MQTT_BROKER_URL` | — (bắt buộc khi bật) | Chỉ `ssl://`, có host+port, không userinfo/path/query/fragment/opaque |
| | `MQTT_CLIENT_ID` / `MQTT_USERNAME` / `MQTT_PASSWORD` | — (bắt buộc khi bật) | ≤128 / ≤128 / ≤512 ký tự, không NUL/newline, không trim |
| | `MQTT_TLS_CA_FILE` | — (bắt buộc khi bật) | Absolute path "sạch"; đọc + parse CA ở startup, ≤1 MiB, regular file |
| Queue/workers | `MQTT_QUEUE_CAPACITY` / `MQTT_WORKER_COUNT` | 256 / 4 | 1..1e6 / 1..1e4 |
| Payload | `MQTT_MAX_PAYLOAD_BYTES` | 65536 (64 KiB) | 1..1048576 (1 MiB) |
| Bộ nhớ queue | `capacity × payload` | 16 MiB | cap 256 MiB, overflow-safe |
| Validator | `TELEMETRY_MAX_SAMPLES_PER_MESSAGE` | 1000 | 1..MaxInt32 |
| | `TELEMETRY_MAX_BATCH_DURATION` | 60s | 1s..1h |
| | `TELEMETRY_MAX_FUTURE_SKEW` | 2m | 0..1h |
| DB | `TELEMETRY_DB_TIMEOUT` | 5s | 1ms..1m (bao acquire/lock/SQL/commit của `Persist`) |
| MQTT | `MQTT_CONNECT_TIMEOUT` / `MQTT_SUBSCRIBE_TIMEOUT` / `MQTT_PUBLISH_TIMEOUT` | 5s / 3s / 3s | 1ms..1m |
| | `MQTT_RECONNECT_MAX_INTERVAL` | 30s | 1s..10m |

**Ngân sách connection pool:** khi ingress bật, `MQTT_WORKER_COUNT` không được chiếm
hết `DATABASE_MAX_CONNS`; chừa **≥2** connection cho HTTP/readiness, thêm **1** nữa
khi credential API giữ một owner connection trong cùng pool
(`checkIngressPoolReservation`). Bộ test `TestIngressPoolReservation` kiểm đúng biên
này (ví dụ `DATABASE_MAX_CONNS=10`: 8 workers OK khi credential tắt, 7 khi bật).

**Ngân sách shutdown:** khi bật ingress, `SHUTDOWN_TIMEOUT` phải ≥
`TELEMETRY_DB_TIMEOUT + MQTT_PUBLISH_TIMEOUT` (default 10s ≥ 8s). Giữ default
shutdown hiện có, không cắt ngắn credential flow.

**Broker (pinned Mosquitto 2.0.18):** `max_packet_size 2097152` (chứa payload 1 MiB +
topic + headers) và `message_size_limit 1048576` (một PUBLISH payload), `allow_anonymous false`.
`max_packet_size` luôn lớn hơn `message_size_limit`, không đặt bằng nhau.

## 3. TLS, filter và principal

- **Data client** (Paho v1.5.1): MQTT **3.1.1** (protocol 4), `CleanSession=true`,
  `AutoReconnect=false`, `ConnectRetry=false`, `OrderMatters=true`, `ResumeSubs=false`.
  Tắt reconnect tự động của Paho để **chỉ supervisor sở hữu vòng reconnect** (không
  có hai vòng retry và không giữ publish trong offline store).
- **TLS:** `MinVersion = TLS 1.2`, `RootCAs` từ file CA đã đọc/parse ở startup,
  `ServerName = hostname` của `MQTT_BROKER_URL` → xác minh hostname/SAN. Không có
  đường insecure fallback.
- **Filter exact:** subscribe QoS 1 **đúng chuỗi** `gateways/+/telemetry/#`
  (`DefaultTelemetryFilter`). Token hoàn tất **không** đủ; transport đọc `Result()`
  và yêu cầu SUBACK granted hợp lệ (khác `0x80`, ≤ QoS yêu cầu). Chỉ **positive
  SUBACK của connection hiện tại** mới bật `Ready()`.
- **Principal:** backend dùng `bootstrap_backend` (không dùng manager, không dùng
  Gateway credential). ACL hiện hành cho `bootstrap_backend`: `subscribeLiteral`
  `gateways/+/telemetry/#` và publish `gateways/+/acks/#`. Task 3.4 **không** mở
  thêm status/responses/commands/control topic. ACK publish tới
  `gateways/<gateway_id>/acks/<message_id>` QoS 1, retained=false.

## 4. Ngữ nghĩa queue → commit ACK

Một delivery chỉ đi **một lần** qua `Validator.Validate → Repository.Persist →
post-commit ACK`.

- **Callback (`Admit`) chỉ làm việc rẻ:** kiểm started/stopping, retained, kích thước,
  hình dạng topic; **copy** topic + payload trước khi return; enqueue non-blocking.
  Không JSON decode, không DB, không goroutine mỗi message, không ACK.
- **Queue đầy:** bỏ delivery khỏi RAM, tăng `queue_full`, **không** application ACK.
  MQTT PUBACK của thư viện chỉ là transport receipt; Gateway tự retry từ outbox bền
  vững (Task 3.5).
- **Worker:** gọi `Validate` với `receivedAt` đã chụp rồi `Persist` bằng context hữu
  hạn (`TELEMETRY_DB_TIMEOUT`).
- **ACK chỉ được gửi khi:** `Persist` trả `nil` **và** outcome `Committed` hoặc
  `DuplicateCommitted` **và** identity khớp (gateway/sensor/message_id). Error,
  outcome zero, hoặc identity mismatch **đều thắng** và triệt ACK.
- **Dedup:** retry cùng `(gateway_id, message_id)` đi nhánh duplicate → ACK lại,
  không ghi raw/Twin/temporal lần hai (do Task 3.3 xử lý trong transaction).
- **Publish timeout / disconnect:** giữ nguyên kết quả DB đã commit, ghi
  `ack_publish_unconfirmed`, **không** rollback và **không** retry nóng trong process;
  phục hồi qua Gateway resend cùng ID/hash/topic. Số lượng publish in-flight bị chặn
  bởi semaphore kích thước cố định (`MaxConcurrentPublishes = WorkerCount`).

## 5. Timeout, reconnect, readiness và shutdown

- **Timeout hữu hạn:** connect, SUBACK và socket write đều có bound; backoff
  reconnect dạng equal-jitter exponential, trần `MQTT_RECONNECT_MAX_INTERVAL`.
  Mỗi connection mới connect rồi subscribe lại; mất kết nối đặt `Ready=false` ngay.
  Callback/SUBACK của lifetime cũ (epoch cũ) **không** thể bật ready hay vô hiệu
  lifetime thay thế.
- **Readiness (`/readyz`):** chỉ true khi **DB ping** (bounded) **và** ingress đang
  nhận việc **và** transport có connection hiện tại với positive SUBACK. `/healthz`
  giữ liveness. Broker tạm unavailable → HTTP vẫn live nhưng chưa ready trong lúc
  supervisor retry.
- **Shutdown một đường/ngân sách chung:** `SHUTDOWN_TIMEOUT` dùng chung cho HTTP
  shutdown, ingress drain và credential close. Thứ tự: dừng admission + readiness
  trước → dừng reconnect loop nhưng **giữ connection hiện tại mở** để workers còn
  phát ACK sau commit trong drain budget → `server.Shutdown` → drain workers +
  đóng MQTT → credential close. `pool.Close()` là defer sau cùng, **không bao giờ
  đóng pool khi workers còn chạy**. Hết budget thì cancel DB/publish còn chạy, đóng
  MQTT và chờ workers kết thúc hữu hạn.
- **Fail-closed khi publish treo (worked-around bằng supervised restart):** nếu một
  lời gọi Paho `Publish` vẫn kẹt sau khi socket đã bị đóng trong budget teardown,
  transport **đánh dấu stuck**, dừng reconnect và giữ readiness false cho tới khi
  process được restart có giám sát. Đây là thay thế an toàn cho việc thay lifetime
  trong một vòng lặp sẽ rò mỗi waiter/reconnect; không tự retry vô hạn và không báo
  "shutdown complete" nếu drain/close chưa xác nhận.

## 6. Kiểm thử 3.4.7 và kết quả

### 6.1 Đối chiếu bảng tình huống bắt buộc (3.4.7.1)

Bảng tình huống bắt buộc đã được phủ đầy đủ bởi unit/race tests với fake nhỏ và
barrier/channel (không `sleep`):

| Tình huống yêu cầu | Test đảm nhiệm |
| --- | --- |
| Persist chưa trả/commit chưa xong → chưa ACK, sau xác nhận mới ACK đúng topic/body/QoS/nonretained | `TestACKOnlyAfterConfirmedCommit`, `assertDeliveryShape`/`decodeACK` |
| Rollback/error/`commit_unconfirmed`, zero outcome không thành success | `TestNoACKOnErrorUnconfirmedOrMismatch` |
| Duplicate committed → ACK lại, không ghi raw/Twin lần hai | `TestDuplicateCommittedReACKs` |
| Invalid, retained, oversized, queue full → không Persist/enqueue, không ACK | `TestAdmissionRejectionsDoNotPersist`, `TestAdmitBeforeStartRejected` |
| Callback return và caller đổi buffer → worker nhận đúng topic/bytes/time copy | `TestAdmissionCopiesPayloadAndTopic` |
| Mất kết nối, SUBACK denied, stale callback → ready false, chỉ SUBACK hiện tại bật ready | `TestLostClearsReadyImmediately`, `TestConnectOnceSubscribeDenied`, `TestConnectOnceSubscribeMissingGrant`, `TestConnectOnceRejectsUnfinishedSubscribe`, `TestConnectOnceRejectsInvalidGrantedQoS`, `TestConnectOnceFailsWhenLostBeforeInstall`, `TestStaleCallbacksCannotInvalidateReplacement` |
| Publish không hoàn tất → pending hữu hạn, không retry nóng, old client đóng trước replacement, DB giữ committed + ACK unconfirmed | `TestPublishTimeoutMarksFailedAndBlocksFurther`, `TestPublishBoundsBlockingPahoCallAndRejectsNewAdmits`, `TestPublishConcurrencyIsBounded`, `TestPublishSlotHeldUntilInvocationReturns`, `TestSupervisorReconnectsAfterPublishTimeout`, `TestSupervisorFailClosedWhenTeardownStuck` |
| Stop lúc idle/queue còn việc/DB hoặc publish bị chặn → ngừng admission, drain/cancel đúng deadline, không panic channel/worker | `TestDrainIdleStopsAdmission`, `TestDrainProcessesQueuedItems`, `TestDrainForcedCancelBlockedRepository`, `TestDrainForcedCancelBlockedPublisher`, `TestStartDrainConcurrent`, `TestStartAfterDrainRefused` |
| Flag ingress/credential bật tắt độc lập; HTTP-only không tự nối MQTT; validation phát hiện thiếu/sai khi bật | `TestIngressFlagIndependence`, `TestIngressDisabledIgnoresIdentityAndCA`, `TestIngressEnabledRequiresIdentity`, `TestComposeIngressDisabledIsInert`, `TestComposeIngressIsEnabled` (readiness) |

Các bound config phụ cũng có test: payload default/max, memory bound, pool reservation,
shutdown budget, numeric/duration bounds, redaction (`src/internal/config/ingress_test.go`).

### 6.2 Smoke thật (3.4.7.2–3.4.7.5)

Entrypoint `scripts/test-stage3-mqtt-ingress.sh` → `stage3_mqtt_ingress_runner.py`. Fixture
disposable dưới `/tmp/opencode`, tái dùng `DatabaseFixture`, bootstrap DynSec container,
migration runner và `task26_harness_helpers`. Không dùng `.env` triển khai, Auth/Storage,
simulator SQLite hay certificate triển khai.

Thành phần: TimescaleDB ghim `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
(PostgreSQL 16.15 / TimescaleDB 2.29.2), broker ghim Mosquitto 2.0.18
`eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`
dùng chính `config/mosquitto/mosquitto.conf`, backend là `go build ./cmd/server` thật
theo composition mới. Producer MQTT tối giản TLS với Gateway credential riêng
(`gw_stage3_smoke`), subscribe own ACK filter và xác minh SUBACK **trước** khi publish.

Kết quả (log `/tmp/opencode/stage3-task34-smoke.log`):

1. **DynSec control traffic với packet limits mới: PASS** — `createClient`/`createRole`/`addClientRole` qua control topic vẫn hoạt động với `max_packet_size`/`message_size_limit` mới.
2. **`/readyz` + `/healthz` sau positive subscription: PASS**.
3. **Application ACK sau commit: PASS** — ACK 5 field đúng (`protocol_version=1`, `message_type=telemetry_ack`, `message_id` khớp, `status=committed`, `committed_at` UTC), topic đúng `gateways/gw_stage3_smoke/acks/<message_id>`, QoS1, non-retained.
4. **SQL độc lập xác nhận:** `telemetry` **100 raw**; `processed_messages` **1 ledger**; `twin_temporal_values` **1 temporal** (`property_name='sample_interval_us'`, `value_number=10000`, `value_text/value_boolean=NULL`, metadata `unit=us/source=telemetry_batch/boot_id` khớp payload, `observed_at=2026-10-01T00:00:00.990Z` = measurement time mẫu cuối); Sensor `reported_state` từ mẫu cuối (`value=1099`, `sequence_num=1099`, `sample_interval_us=10000`, `unit=degC`, `boot_id` khớp payload, `observed_at=2026-10-01T00:00:00.99Z`), `reported_version 0→1` (**+1**); `desired_state/desired_version` của Sensor và reported/desired của Gateway **không đổi**.
5. **Resend chính xác topic/bytes/id: PASS** — nhận ACK lại, các count và version không tăng (nhánh duplicate).
6. **SIGTERM thường: PASS** — container exit code 0, log có `backend shutdown complete`, không có `backend shutdown incomplete`/`panic:`.
7. **Cleanup: PASS** — container/network/volume/workdir của fixture đều vắng sau khi chạy.

## 7. Lệnh và kết quả (chạy trong `src/` trừ khi ghi chú khác)

| Lệnh | Kết quả |
| --- | --- |
| `gofmt -l .` | PASS (không file nào liệt kê) |
| `go build ./...` | PASS |
| `go vet ./...` | PASS (exit 0) |
| `golangci-lint run ./... --timeout=6m` | PASS (`0 issues.`) |
| `go test -race -count=1 ./internal/config ./internal/mqttingress ./internal/telemetry ./internal/gateway ./cmd/server ./internal/httpserver` | PASS (`ok` cả 6 package) |
| `git diff --check` (repo root) | PASS (không whitespace error) |
| `sh scripts/test-stage3-mqtt-ingress.sh` | PASS — log `/tmp/opencode/stage3-task34-smoke.log` |

**Retain bằng chứng Task 3.3:** không sửa package `telemetry` (validator/repository/
reported-state/temporal) và không đổi schema, nên bằng chứng native Task 3.3 vẫn
nguyên hiệu lực; smoke Task 3.4 còn chạy lại đường `Persist` đầy đủ (100 raw / 1
ledger / 1 temporal / reported state) qua broker thật. Vì vậy không chạy lại
fixture DB-only Task 3.3 trong lượt này (đúng policy: không chạy lại chỉ vì tự tin).
Không mở lại/không hạ thấp các acceptance lịch sử Stage 2/Task 3.2/3.3.

## 8. Giới hạn và NOT RUN

- **Task 3.5 (NOT RUN):** simulator Gateway có SQLite durable outbox, restart với pending item, outbox full, resend boot cũ/mới.
- **Task 3.6 (NOT RUN):** ma trận denial TLS/credential đầy đủ, cross-Gateway publish/subscribe, ACK spoof, control-topic; forced disconnect/broker cold restart; overload/queue-full E2E. Smoke chỉ chạy **positive** path.
- **Task 3.7 (NOT RUN):** workload sustained 600 batch/60.000 mẫu, đo p50/p95, queue high-water, RAM/CPU; lane CI Stage 3. **Không** có remote CI PASS.
- **Không** deployment migration lên database thật, **không** board runtime (ESP32/Luckfox/AM5728), **không** build parity `src/Dockerfile` cho slice này (smoke mount nhị phân tĩnh biên dịch trên host, read-only).
- **Broker cold-restart persistence** (ROT2/T10) vẫn **chưa** kiểm chứng E2E; các giới hạn lịch sử Stage 2 không thay đổi.
- **U01–U15 chưa** được nghiệm thu end-to-end; kết quả Task 3.4 chỉ là một message/retry thật qua MQTT → DB → ACK cho một Gateway/Sensor, chưa phải toàn bộ acceptance Stage 3.
- **Fail-closed stuck publish** cần supervised restart thủ công (không tự phục hồi).
- Queue đầy bỏ delivery trong RAM không application ACK; phục hồi phụ thuộc outbox bền vững của Gateway (Task 3.5).
- Counters/log hiện được thu thập trong process (`Snapshot`) và structured log reason-coded, **không** phơi ra endpoint metrics; chưa cần observability stack.

## 9. Bàn giao

- **Task 3.5:** topic/payload bytes, contract ACK 5 field, producer tối giản
  (`GatewayProducer`) và các bound/config làm điểm khởi đầu cho simulator SQLite.
- **Task 3.6:** smoke/helpers (`test-stage3-mqtt-ingress.sh`, runner) để mở rộng sang
  denial/restart/pressure.
- **Task 3.7:** counters/profile khởi đầu (`Snapshot` của ingress và transport) để đo
  workload.
