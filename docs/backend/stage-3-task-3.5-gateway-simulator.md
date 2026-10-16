# Task 3.5 — Gateway Simulator: CLI, outbox bền vững và xử lý pending

**Ngày:** 16/10/2026.

**Phạm vi tài liệu này:** mô tả cách chạy, cấu hình và vận hành Gateway
Simulator sau khi hoàn thành slice **3.5.6** (`pending`/`count`/`quarantine`),
**3.5.7.1–3.5.7.2** (entrypoint CLI và cấu hình `GATEWAY_*`/hướng dẫn) và
**3.5.7.3–3.5.7.4** (named target `gateway` trong `src/Dockerfile` và
build/cross-build; xem §7.1).
Thư viện `internal/gatewaysim` (slice 3.5.1–3.5.5) đã có sẵn và được tái sử dụng.

**Trạng thái:** Task 3.5 **hoàn thành có giới hạn (bounded complete), chờ commit**
trên `develop`; revision thực tế khi chạy là HEAD
`21447d82bacfb42dd1edd7cc2d5566aa28acb0cd` (worktree dirty: Task 3.4 + các file
Task 3.5 chưa commit). Toàn bộ các ca **bắt buộc** của Task 3.5 đã có bằng chứng
chạy thật: named target `gateway` + build/cross-build (3.5.7.3–3.5.7.4) **PASS**
(§7.1); smoke end-to-end **3.5.8.2** (CLI `cmd/gateway` + outbox SQLite bền vững
qua broker Mosquitto + TimescaleDB + backend thật) **PASS** (§7.2); và các ca
3.5.8.1 ở **cấp unit/CLI với SQLite file thật** **PASS** dưới `go test -race`,
đối chiếu từng ca ở §7.3. Chỉ những ca được chứng minh chạy PASS mới được ghi
PASS.

Các hạng mục **KHÔNG** phải gate nghiệm thu của Task 3.5 và vẫn **NOT RUN** được
ghi rõ, không suy diễn thành PASS: (a) ma trận **cross-service** của 3.5.8 (mất
application ACK sau DB commit xuyên broker/backend, cold-crash nhiều dịch vụ,
restart broker/backend, queue-full xuyên dịch vụ) — thuộc **Task 3.6**; (b)
**ARM board runtime** (chỉ mới cross-build ARMv7, chưa chạy SQLite trên bo mạch);
(c) power-loss durability. Bộ **9 lỗi Python** còn lại là **drift có sẵn** của
harness `native_compose_smoke` (còn ghim schema 19 trong khi migration
`000020_stage3_telemetry_ingestion` đã commit ở HEAD), **không** phải hành vi
simulator Task 3.5, **không** được tính PASS và đã được ghi vào backlog. Lát cắt
này **không** kèm thay đổi triển khai (deployment) tương ứng và **không** tuyên
bố U01–U15 đã nghiệm thu đầy đủ.

Simulator là công cụ phát triển/tích hợp: nó phát **dữ liệu mô phỏng** (synthetic),
**không** phải số đo cảm biến thật và **không** phải firmware thiết bị.

## 1. Điều kiện tiền đề

- Gateway và Sensor mục tiêu **phải được provision trước** trên server (bảng
  `gateways`, `sensors`, cùng đồ thị Digital Twin tương ứng). Simulator **không**
  tự tạo entity và **không** tự provision sensor theo telemetry đến (đúng chính
  sách "no telemetry auto-provisioning" của AGENTS.md §6.5).
- Gateway cần một **MQTT credential** hợp lệ (username = `GATEWAY_ID`, mật khẩu
  riêng, CA công khai) do platform admin cấp qua luồng admin hiện có.
- CA công khai dùng để xác thực broker phải tồn tại tại đường dẫn tuyệt đối
  trong `GATEWAY_MQTT_CA_FILE`. Không truyền bí mật qua argv.

## 2. Cấu hình `GATEWAY_*`

Simulator có loader **riêng** (`internal/gatewaysim/config.go`), **không** dùng
`internal/config` của server: Gateway không có `DATABASE_URL` hay khóa Supabase.
Cấu hình chỉ đọc các biến có tiền tố `GATEWAY_`. Đây là giá trị khởi đầu, **chưa
phải số đo capacity**.

| Biến | Bắt buộc | Default | Bound / ràng buộc |
| --- | --- | --- | --- |
| `GATEWAY_ID` | có | — | định danh Gateway hợp lệ |
| `SENSOR_ID` | có | — | định danh Sensor hợp lệ |
| `GATEWAY_MQTT_BROKER_URL` | có | — | `ssl://host:port`, không userinfo/path/query/fragment |
| `GATEWAY_MQTT_CLIENT_ID` | có | — | khác `GATEWAY_ID`, ≤ 128 ký tự |
| `GATEWAY_MQTT_USERNAME` | có | — | phải **bằng** `GATEWAY_ID`, ≤ 128 ký tự |
| `GATEWAY_MQTT_PASSWORD` | có | — | không rỗng, ≤ 512 ký tự, không ký tự điều khiển |
| `GATEWAY_MQTT_CA_FILE` | có | — | đường dẫn tuyệt đối sạch |
| `GATEWAY_OUTBOX_PATH` | có | — | đường dẫn tuyệt đối sạch tới file SQLite bền vững, **không** `:memory:` |
| `GATEWAY_SAMPLE_RATE_HZ` | không | `100` | `1..100000` |
| `GATEWAY_SAMPLES_PER_BATCH` | không | `100` | `1..1000`; batch phải nằm trong giới hạn telemetry của server |
| `GATEWAY_ACK_TIMEOUT` | không | `5s` | `1ms..10m` |
| `GATEWAY_RETRY_MIN_DELAY` | không | `1s` | `1ms..10m`, ≤ `RETRY_MAX_DELAY` |
| `GATEWAY_RETRY_MAX_DELAY` | không | `30s` | `1ms..10m`, ≥ `RETRY_MIN_DELAY` |
| `GATEWAY_MAX_INFLIGHT` | không | `8` | `1..4096`, ≤ `OUTBOX_MAX_ROWS` |
| `GATEWAY_OUTBOX_MAX_ROWS` | không | `5000` | `1..1000000` |
| `GATEWAY_OUTBOX_MAX_BYTES` | không | `67108864` (64 MiB) | `1..8589934592` (8 GiB) |
| `GATEWAY_MQTT_CONNECT_TIMEOUT` | không | `10s` | `1ms..10m` |
| `GATEWAY_MQTT_SUBSCRIBE_TIMEOUT` | không | `5s` | `1ms..10m` |
| `GATEWAY_MQTT_PUBLISH_TIMEOUT` | không | `5s` | `1ms..10m` |
| `GATEWAY_SQLITE_TIMEOUT` | không | `5s` | `1ms..10m` |
| `GATEWAY_SHUTDOWN_TIMEOUT` | không | `10s` | `1ms..10m` |

Các biến trên đã được đưa vào `.env.example` với placeholder an toàn (không có
bí mật thật, không domain/ca nhân). Mật khẩu MQTT và CA phải được đặt trong môi
trường hoặc file bảo vệ, **không** qua argv và **không** qua ví dụ đã commit.

`Config.String()`/`GoString()` che mọi trường nên log chẩn đoán không lộ
credential hay đường dẫn.

## 3. Các lệnh CLI

Entrypoint: `cmd/gateway`. `run` là lệnh mặc định khi không truyền tham số.

```text
usage: gateway [command] [flags]

commands:
  run                     phát telemetry tổng hợp đến khi dừng (mặc định)
  pending [--limit N]     liệt kê item pending (chỉ metadata)
  count                   in tổng rows/bytes pending/quarantined
  quarantine --message-id UUID --reason TEXT
                          chuyển một item pending sang quarantined (simulator đã dừng)
  help                    hiển thị hướng dẫn

run flags:
  --batches N             dừng sau N batch duy nhất (0 = đến khi dừng)
  --completion-timeout D  thời gian chờ application ACK sau khi đạt mục tiêu batch
```

### 3.1 `run`

- Không có `--batches` (hoặc `0`): chạy liên tục đến khi nhận `SIGINT`/`SIGTERM`.
- `--batches N` (hữu hạn): dừng phát sau N batch duy nhất **nhưng vẫn chờ** các
  application ACK còn thiếu trong `--completion-timeout` trước khi báo hoàn thành.
- `--batches` hợp lệ `0..100000` (`maxFiniteBatches`); `--completion-timeout` hợp
  lệ `100ms..10m` (default `30s`).
- Một MQTT transport duy nhất chỉ báo `running` sau khi subscription ACK riêng đã
  được cấp (readiness gate) trong budget `CONNECT_TIMEOUT + SUBSCRIBE_TIMEOUT`.
- **Chỉ báo `completed=true`** khi mọi batch mục tiêu đều đã có application ACK
  và không còn item phù hợp đang chờ. Nếu hết budget mà vẫn còn pending, tiến
  trình trả mã lỗi `1` và **giữ nguyên** item pending cho lần chạy sau
  (`ErrSchedulerCompletion`).
- Không tạo unbounded goroutine: queue admission, worker và in-flight đều bị chặn
  bởi `GATEWAY_MAX_INFLIGHT`/`GATEWAY_OUTBOX_MAX_ROWS`.

### 3.2 `pending [--limit N]`

- Chỉ đọc `GATEWAY_OUTBOX_PATH` — **không** cần MQTT password/CA/GW env.
- Đọc định danh Gateway/Sensor **từ chính file** (không tạo file, không đổi binding).
- `--limit` hợp lệ `1..1000` (default `20`); kết quả có chặn nên không nạp cả
  outbox vào bộ nhớ.
- Mỗi dòng in metadata: `message_id`, `boot_id`, dải `sequence=first..last`,
  `sample_count`, `bytes`, `status`. Sau đó in tổng pending/quarantined.
- **Không** in raw payload, đường dẫn, DSN hay credential.

### 3.3 `count`

- Chỉ đọc `GATEWAY_OUTBOX_PATH`, in `pending_rows/pending_bytes/quarantined_rows/quarantined_bytes`.

### 3.4 `quarantine --message-id UUID --reason TEXT`

- `--message-id` phải là UUID hợp lệ, khác nil; `--reason` bắt buộc `1..256` ký tự.
- Đổi **đúng một** row `pending` sang `quarantined` trong một transaction; giữ
  nguyên `id`, `topic`, `bytes`, `boot_id`; **không** re-enqueue và **không** đổi
  `message_id`.
- Nếu không có row pending khớp → không thay đổi gì, in `no pending item ...` và
  trả mã `1`.
- Chỉ nên chạy khi simulator đã dừng: cả `run` và các lệnh admin dùng **cùng một
  khóa flock độc quyền** trên file, nên lệnh admin **fail rõ ràng** (`ErrOutboxBusy`)
  nếu simulator đang chạy.

### 3.5 Mã thoát

| Tình huống | Mã |
| --- | --- |
| Thành công / dừng theo signal | `0` |
| Lỗi cấu hình | `2` |
| Flag/đối số sai (`--batches`, `--completion-timeout`, `--limit`, `--reason`, `--message-id`) | `2` |
| Admin: file outbox thiếu, hoặc quarantine không khớp row | `1` |
| Khởi động thất bại (outbox/processor/transport/scheduler) | `1` |
| Run hữu hạn hết budget mà còn pending | `1` |

## 4. Outbox SQLite: bền vững và chạy lại cùng file

- `GATEWAY_OUTBOX_PATH` là một file SQLite **bền vững** (không in-memory). File
  được mở với `journal_mode=delete`, `synchronous=FULL`, `busy_timeout` hữu hạn
  và `BEGIN IMMEDIATE`, một kết nối ghi duy nhất.
- `PRAGMA user_version = 1` là schema duy nhất được ghi. File lạ/khác version bị
  từ chối (`ErrOutboxSchema`) thay vì bị ghi đè; file không phải SQLite bị báo
  `ErrOutboxCorrupt`.
- Một file bị **ràng buộc** với đúng một cặp `Gateway/Sensor` (`outbox_meta`);
  mở với định danh khác bị từ chối (`ErrOutboxIdentity`).
- Bảng: `outbox_meta`, `outbox` (payload BLOB nguyên vẹn, `byte_size`,
  `first_sequence`, `sample_count`, `sample_interval_us`, `status`, `reason`,
  `enqueued_at`), `outbox_streams` (cấp phát sequence theo boot stream).
- **Ghi trước, publish sau:** một batch chỉ được publish sau khi transaction
  enqueue đã commit. Một ACK chỉ xóa đúng row pending sau khi delete commit.
- **Chạy lại cùng file:** dùng lại cùng `GATEWAY_OUTBOX_PATH` cho các lượt chạy
  tuần tự. Item pending từ boot cũ được resend **nguyên bytes** (cùng
  `message_id`, `topic`, dải sequence, timestamp đo); batch mới dùng `boot_id`
  mới nên không trùng range trong cùng boot.
- **Không có catch-up/backfill tự động:** simulator phát tiếp dữ liệu mới, không
  tự sinh bù các mẫu đã mất; nó chỉ đảm bảo các batch đã enqueue không bị mất
  trước khi được ACK.
- Xóa file outbox là thao tác **thủ công, có chủ đích**; CLI không bao giờ tự xóa
  hay tạo lại file.

## 5. Xử lý item không được application ACK

V1 **không có NACK**, nên **timeout tự nó không chứng minh dữ liệu invalid**. Quy
trình có giám sát:

1. Kiểm cấu hình, provisioning Gateway/Sensor, trạng thái backend và log server
   trước (MQTT QoS ACK chỉ chứng minh broker nhận, **không** chứng minh DB commit).
2. Dừng simulator (để giải phóng flock).
3. `gateway pending --limit N` và `gateway count` để xem metadata +
   pending/quarantined rows/bytes.
4. Chỉ **cách ly có chủ đích** bằng `gateway quarantine` khi biết item không thể
   xử lý (ví dụ dữ liệu sai không thể chấp nhận). **Không** tự cách ly chỉ vì quá
   số lần retry, **không** tự xóa/quên item.
5. Khởi động lại simulator với cùng file để resend item chưa ACK; item đã cách ly
   **không** được phát lại và **không** tính là đã giao.

**Commit không xác định (unknown commit):** nếu một commit outbox thất bại với
kết quả không rõ (`ErrOutboxCommitUnknown` / `ErrSchedulerCommitUnknown`), CLI
dừng và yêu cầu khôi phục có giám sát — **không** sinh message_id mới và **không**
retry mù. Đối chiếu/khôi phục dựa trên trạng thái file.

**Cách ly không giải phóng quota:** row quarantined vẫn tính vào
`OUTBOX_MAX_ROWS`/`OUTBOX_MAX_BYTES`; khi đầy, generator dừng rõ ràng và **không**
drop item cũ.

## 6. Dung lượng đĩa (disk headroom)

- Bytes tính quota là **business bytes** (`byte_size` = topic + payload), **không**
  phải kích thước file trên đĩa.
- File SQLite có thể lớn hơn tổng business bytes do overhead trang/journal
  (`journal_mode=delete`). Nên để headroom đĩa lớn hơn `GATEWAY_OUTBOX_MAX_BYTES`
  (khuyến nghị tối thiểu 2×) và theo dõi `count` trong các lượt chạy dài.
- Simulator **không** tự compaction/VACUUM; quản lý file outbox là thao tác vận
  hành thủ công.

## 7. Kiểm chứng đã chạy trong slice này

Chạy trên Go 1.27.2, `modernc.org/sqlite v1.60.1` (pure-Go, không cgo). Tất cả
kiểm thử SQLite dùng **file thật** trong thư mục tạm, không dùng mock in-memory.

| Kiểm | Lệnh (đã chạy lại trong lượt đối chiếu này) | Kết quả |
| --- | --- | --- |
| `gofmt` | `gofmt -l ./cmd/gateway ./internal/gatewaysim` | sạch (không file nào) |
| `go test -race` | `go test -race -count=1 ./internal/gatewaysim/ ./cmd/gateway/ ./internal/telemetry/ ./internal/config/ ./internal/mqttingress/ ./cmd/server/` | **PASS** cả 6 package |
| `go vet` | `go vet ./...` | sạch, exit 0 |
| `golangci-lint` | `golangci-lint run ./...` | `0 issues` |
| `go build` | `CGO_ENABLED=0 go build ./...` | thành công (không cgo; driver `modernc.org/sqlite` pure-Go) |
| Dependency diff | `git diff go.mod` | chỉ thêm `modernc.org/sqlite v1.60.1` (+ indirect `libc v1.77.1`, `mathutil`, `memory`, `ncruces/go-strftime`, `remyoudompheng/bigfft`, `dustin/go-humanize`); `golang.org/x/sys` **giữ nguyên `v0.49.0`**, không nâng/hạ ngoài ý muốn |

Go 1.27.2. Tất cả kiểm thử SQLite dùng **file thật** trong thư mục tạm (không có
`t.Skip`, không mock in-memory; các ca `:memory:` là negative test từ chối cấu hình
in-memory).

Kiểm thử CLI bao phủ: admin chỉ-cần-path đọc file SQLite thật; file không tồn tại
**không** bị tạo; `quarantine` giữ nguyên quota; run hữu hạn thành công khi mọi
batch được ACK; run hữu hạn **thất bại** khi còn pending (giữ item); dừng theo
signal rồi mở lại cùng file; dọn dẹp khi CA không hợp lệ. Test thư viện bao phủ
thêm ngân sách shutdown chia sẻ của scheduler và `OpenOutboxAdmin` yêu cầu file
tồn tại + đọc binding từ file.

**Sửa lỗi scheduler:** `waitPublishes` trước đây tự cộng thêm một
`ShutdownTimeout` đầy đủ, khiến tổng teardown có thể gấp đôi ngân sách. Đã thêm
`BeginShutdown(deadline)`/`Done()` và `publishDrainBudget()` để scheduler chia sẻ
**một** deadline shutdown với CLI; CLI chỉ tạo một `context.WithTimeout(...,
SHUTDOWN_TIMEOUT)` dùng chung cho scheduler drain, ACK worker và transport stop.

### 7.1 Named target `gateway` và build/cross-build (slice 3.5.7.3–3.5.7.4)

Revision khi chạy: `develop` HEAD `21447d82bacfb42dd1edd7cc2d5566aa28acb0cd`,
worktree dirty (Task 3.4 + các file Task 3.5 chưa commit). Output build đặt trong
`/tmp/opencode/gw-build-3-5-7/`; **không** có binary nào vào Git.

**Thay đổi `src/Dockerfile`:** thêm một dòng build
`CGO_ENABLED=0 GOOS=linux go build -o /app/gateway ./cmd/gateway` trong stage
`builder`, và stage `gateway` (đặt **trước** `backend`) dựa trên base pinned sẵn
có `eclipse-mosquitto@sha256:d12c8f8…` (stage `runtime`, Alpine 3.18). Stage tạo
thư mục writable `/data/outbox` (khai báo `VOLUME`) và `/data/ca`, rồi chạy dưới
`USER 1883:1883` giống các binary Go khác. `auth-init`/`reloader`/
`credential-controller`/`backend` **giữ nguyên**; `backend` vẫn là stage cuối nên
vẫn là target **default**. **Không** thêm service Gateway nào vào
`docker-compose.yml` root; image **không** nhúng credential, private key hay
database.

| Kiểm | Lệnh | Kết quả |
| --- | --- | --- |
| Host build amd64 | `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/opencode/gw-build-3-5-7/gateway-linux-amd64 ./cmd/gateway` | **PASS** — ELF 64-bit x86-64, statically linked |
| Cross-build ARMv7 | `CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -trimpath -o /tmp/opencode/gw-build-3-5-7/gateway-linux-armv7 ./cmd/gateway` | **PASS** — ELF 32-bit ARM EABI5, statically linked |
| Docker build target `gateway` | `docker build --target gateway -f src/Dockerfile src -t main_src-gateway:3.5.7` | **PASS** — manifest list `sha256:de85dec215a9cadb8b0f4f316532419cff1cb6c62822dde244d399825f5d371e` |
| Docker build target `backend` (builder dùng chung) | `docker build --target backend -f src/Dockerfile src -t main_src-backend:3.5.7` | **PASS** — manifest list `sha256:b0ecd78e0c693b0c32638ce844d0574b5bd3e5c437240a3a176813f4b9c45c41` |

**Runtime trong image** (bind mount `outbox` writable + public CA read-only;
container chạy `--user <host-uid>:<host-gid>` để thư mục bind-mount host ghi được
— kiểm tra driver SQLite đã biên dịch thật, không dùng `--help`):

| Bước | Lệnh (rút gọn env) | Kết quả |
| --- | --- | --- |
| Tạo DB thật trong image | `… main_src-gateway:3.5.7 run --batches 1 --completion-timeout 1s` với `GATEWAY_MQTT_BROKER_URL=ssl://127.0.0.1:1` (broker không tồn tại) | exit `1` (`MQTT transport did not reach a subscribed ready state`); file `gateway_001_outbox.db` được tạo: `SQLite 3.x, user version 1` |
| Seed + đọc rows | seed 2 row `pending` bằng fixture tạm dùng `gatewaysim.OpenOutbox`/`Enqueue` (fixture **đã xóa**, không commit), rồi `… count` | `pending_rows=2 pending_bytes=1918 quarantined_rows=0` |
| Metadata | `… pending --limit 5` | 2 dòng, `sequence=0..99` và `100..199` cùng một `boot_id` |
| Ghi/commit trong image | `… quarantine --message-id <id2> --reason task3.5.7-build-smoke` | `quarantined …`, exit `0`; sau đó `count` = `pending_rows=1 pending_bytes=958 quarantined_rows=1 quarantined_bytes=960` |
| Recreation | `docker rm` container cũ rồi chạy container **mới** cùng bind mount, `… count` | kết quả y hệt (`pending_rows=1 quarantined_rows=1`); file host bền vững (`md5 da3e6002505863cab82c6c7391159996`) |

DB do chính image tạo ở bước đầu (DDL), các bước sau đọc/ghi/UPDATE-commit trên
cùng file, nên đã chứng minh `modernc.org/sqlite v1.60.1` (pure-Go, CGO=0) được
biên dịch và hoạt động trong image. Public CA dùng cho bước `run` là CA dev công
khai `config/mosquitto/certs/ca.crt` (mount read-only), **không** phải credential.

**Kiểm Go liên quan** (chạy lại trong slice này): `gofmt -l ./cmd/gateway
./internal/gatewaysim` sạch; `go vet ./cmd/gateway/... ./internal/gatewaysim/...`
sạch; `go test -count=1 ./cmd/gateway/... ./internal/gatewaysim/...` **PASS**;
`git diff --check` sạch.

**Không bao gồm:** slice 3.5.7.3–.4 **không** chạy broker + TimescaleDB end-to-end;
điều đó được thực hiện riêng ở slice 3.5.8.2 (§7.2, **PASS**). Slice này cũng
**chưa** chạy binary ARM trên bo mạch (ARM runtime **NOT RUN**); fixture seed chỉ
là tạm và đã bị xóa.

### 7.2 Smoke end-to-end thật qua broker + TimescaleDB (slice 3.5.8.2)

Revision: `develop` HEAD `21447d82bacfb42dd1edd7cc2d5566aa28acb0cd`, worktree dirty
(Task 3.4 + Task 3.5 chưa commit). Docker 29.9.1, Mosquitto
`eclipse-mosquitto@sha256:d12c8f80dfc6…` (2.0.18), TimescaleDB `timescale/timescaledb`
pinned của `DatabaseFixture`, Go 1.27.2. Mount `/media/.../SSD2-Data` là **`noexec`**,
nên wrapper phải gọi qua `sh` (không dùng `./`).

Chạy một lượt fixture dùng chung (broker + TimescaleDB + backend thật), **tái sử
dụng** runner Task 3.4 (`scripts/tests/stage3_mqtt_ingress_runner.py`) qua cờ
`--mode simulator`, thay vì copy harness. Mọi tài nguyên (container/network/volume/
workdir) tạo và xóa theo **đúng ID** dưới `/tmp/opencode`; credential/cert sinh
một lần trong phiên và **không** ghi vào Git.

| Bước | Lệnh (rút gọn) | Kết quả |
| --- | --- | --- |
| Wrapper (PASS/SKIP/FAIL nghiêm ngặt) | `sh scripts/test-stage3-gateway-simulator.sh` | **PASS**, exit `0` |
| CLI hữu hạn 1 batch | `gateway run --batches 1 --completion-timeout 30s` (TLS `ssl://localhost:<port>`, cert SAN `DNS:localhost`) | exit `0`; summary `completed=true enqueued=1 app_acknowledged=1 pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0` |
| Admin chỉ-cần-path | `gateway count` / `gateway pending --limit 5` (cùng `GATEWAY_OUTBOX_PATH`) | `pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0`; `pending` in **không** dòng item |
| Backend readiness | `GET /readyz`, `GET /healthz` sau khi subscribe ingress | **PASS** |
| Shutdown | `docker stop -t 30` trên backend | exit `0`, log `backend shutdown complete` |
| Teardown | kiểm tra lại bằng `docker inspect`/`network inspect`/`volume inspect` | `container/network/volume/workdir absent` |

**Assert SQL độc lập** (`assert_phase`, envelope tổng hợp biết trước) xác nhận sau
commit: **100 raw** row (kiểm cả công thức giá trị `20 + clamped(seq%2000)/100`,
`sample_index = seq - first`, `boot_id`, và span thời gian = `(count-1)*interval`),
**1 ledger** (`processed_messages`: `first/last/count/sensor_id/source_topic`), **1
temporal** (`sample_interval_us`: `value_number=10000`, `value_text/boolean NULL`,
`metadata.unit='us'`, `metadata.source='telemetry_batch'`, `metadata.boot_id`,
`observed_at` = `max(measured_at)` của batch), reported-state của Sensor tiến từ mẫu
cuối (giá trị cuối `20.99`, `sequence_num=99`, `sample_interval_us`, `unit='degC'`,
`boot_id`, `observed_at`) với `reported_version` **+1** (0 → 1), và **giữ nguyên**
`desired_state` Sensor (`{"sentinel":"keep"}`, version 7) cùng reported/desired của
Gateway. Không in raw payload/secret.

**Bằng chứng restart cấp CLI (outbox SQLite thật, thiếu trong test cũ):** thêm
`TestRunRestartResendsPriorBootByteExact` (`src/cmd/gateway/main_test.go`) chạy
**hai lượt CLI thật** trên **một** file SQLite bền vững qua seam transport (ranh
giới MQTT, fake): lượt 1 để lại 1 row pending chưa ACK; lượt 2 phát lại **nguyên
byte** row boot cũ (cùng topic/payload) rồi ACK, đồng thời sinh **đúng một** batch
mới dưới `boot_id` **khác**. Đây là test cấp unit/CLI; **không** dựng ma trận
lost-ACK xuyên dịch vụ.

**Không hồi quy Task 3.4:** chế độ `--mode ingress` (mặc định) giữ nguyên hành vi;
chạy lại `sh scripts/test-stage3-mqtt-ingress.sh` **PASS** toàn bộ (post-commit ACK
5 trường, SQL 100 raw/1 ledger/1 temporal + reported +1, resend byte-exact re-ACK,
SIGTERM, cleanup), nên đường Task 3.4 **không** bị bỏ qua.

**Kiểm Python:** `python3 -m py_compile scripts/tests/stage3_mqtt_ingress_runner.py`
**OK**. `python3 -m unittest discover -s scripts/tests -p 'test_*.py'`: **671 test,
8 failure + 1 error = 9 lỗi**, còn lại **toàn bộ** ở `test_native_compose_smoke`.

- **Đã sửa (đồng bộ fixture — thuộc phạm vi slice):** `docker-compose.yml` (HEAD,
  **không** bị sửa trong task này) tham chiếu **9 khóa** Task 3.4 chưa có trong
  `DEFAULTS` của `native_compose_smoke.py`: `MQTT_INGRESS_ENABLED`,
  `MQTT_CONNECT_TIMEOUT`, `MQTT_SUBSCRIBE_TIMEOUT`, `MQTT_PUBLISH_TIMEOUT`,
  `MQTT_RECONNECT_MAX_INTERVAL`, `TELEMETRY_MAX_SAMPLES_PER_MESSAGE`,
  `TELEMETRY_MAX_BATCH_DURATION`, `TELEMETRY_MAX_FUTURE_SKEW`,
  `TELEMETRY_DB_TIMEOUT`. `require_inputs` fail-closed trên **mọi** khóa root chưa
  khai báo, nên trước fix có **69 lỗi** (phần lớn là `missing_interpolation` tại
  `prepare()`); sau fix còn **9**. Đã thêm 9 default an toàn (khớp fallback `:-`
  của compose và `.env.example`) + test hồi quy
  `test_task34_telemetry_ingress_defaults_explicit`. **Không** hạ yếu assertion,
  **không** refactor rộng. Không file test nào import runner đã sửa
  (`stage3_mqtt_ingress_runner.py`).
- **9 lỗi còn lại (ngoài phạm vi Task 3.5 — ghi nhận chính xác, KHÔNG tính PASS):**
  cùng root cause `schema19_exact_rejected`. `native_compose_smoke.startup_only`
  còn ghim `expected.endswith('19:mqtt_simplified_journal')` trong khi `migrations/`
  đã có `000020_stage3_telemetry_ingestion.up.sql` (**đã commit ở HEAD**), nên phase
  `application-migrations` bị từ chối và các assertion giữ nguyên phase
  (`'application-migrations' != 'positive_http'|'report_validation'|'supabase-envoy'`,
  `'application-migrations' is not None`) fail theo. Đây là **drift có sẵn** của
  harness native-compose (lane Stage 2/Task 3.2/3.4), **không** phải hành vi
  simulator Task 3.5. Đã xác nhận không hồi quy: chạy lại 9 test này với fixture
  HEAD cho kết quả **fail y hệt**; không test nào trước đây PASS nay FAIL.
  > **Đã xử lý (16/10/2026, sau ghi nhận trên):** ghim schema của harness đã được
  > đồng bộ với migration mới nhất. `native_compose_smoke.py` nay dùng hằng
  > `EXPECTED_SCHEMA_TERMINAL = "20:stage3_telemetry_ingestion"` cùng helper
  > `expected_schema_history()` / `schema_history_verified(...)`; kiểm tra vẫn
  > **chính xác** toàn bộ lịch sử `version:name` và từ chối thiếu/thừa/sai tên/lỗi
  > migration (không nới lỏng thành chấp nhận mọi suffix). Mã lỗi
  > `schema19_exact_rejected` đổi thành `schema_exact_rejected`; thêm 7 test hồi
  > quy `SchemaHistoryTests`. Kết quả mới: `test_native_compose_smoke` 103/103
  > PASS; `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` 678/678
  > PASS (trước fix: 671 test với 8 failure + 1 error). Quan sát 9 lỗi ở trên là
  > bằng chứng lịch sử trước fix, **không** bị xóa.

### 7.3 Đối chiếu ca 3.5.8 với test thật (SQLite file)

Bảng dưới ánh xạ **từng ca yêu cầu của 3.5.8.1** sang test đã có (SQLite file
thật, fake chỉ ở ranh giới clock/MQTT), tất cả **PASS** dưới `go test -race`.

| Ca (3.5.8.1) | Test bao phủ | Kết quả |
| --- | --- | --- |
| Enqueue/close/reopen byte-exact; chỉ publish sau commit | `TestOutboxEnqueueReopenByteExact`, `TestOutboxDurabilityPragmas` | PASS |
| Lỗi enqueue/capacity rollback row+counter; không publish batch chưa lưu | `TestOutboxCapacityByRows`, `TestOutboxCapacityByBytes`, `TestOutboxSequenceOverflowRollsBack`, `TestOutboxEnqueueRollsBackOnRealSQLFailure`, `TestOutboxCommitAmbiguitySurfaced` | PASS |
| Restart với pending (2 lượt cùng file, boot mới) | `TestSchedulerRestartReplaysOldBootAndStartsFreshSequence`, `TestRunRestartResendsPriorBootByteExact` (CLI) | PASS |
| MQTT PUBACK không application ACK → pending còn; resend ID/topic/bytes | `TestTransportPUBACKDoesNotDeleteOutboxRow`, `TestRunSignalShutdownPreservesPendingAndReopens` | PASS |
| ACK sai/malformed/retained hoặc Gateway/ID/status/version mismatch → không xóa | `TestParseCommitACKRejectsMalformed`, `TestACKProcessorInvalidChangesNothing`, `TestACKProcessorAdmissionCopyRetainedOversizeFullNoLoss` | PASS |
| ACK sớm/trễ/boot cũ/lặp → không mất ACK hợp lệ, xóa đúng một lần | `TestACKProcessorAcceptsEarlyLateAndPriorBoot`, `TestACKProcessorDuplicateUnknownQuarantinedNoOp`, `TestSchedulerEarlyAckWhilePublishInFlight`, `TestSchedulerLateAckAfterTimeoutAndDuplicateAck` | PASS |
| Retry/backoff/disconnect (fake clock/RNG, jitter cap, no hot loop) | `TestSchedulerRetryBackoffNoHotLoop`, `TestRetryDelayJitterAndCap` | PASS |
| Outbox rows/bytes full có item cách ly; dừng generation, không drop | `TestSchedulerCapacityFullIncludingQuarantine`, `TestOutboxQuarantineCountsTowardQuota` | PASS |
| Stop hữu hạn, mở lại còn pending, không báo committed khi còn pending | `TestSchedulerFiniteCompletion`, `TestSchedulerFiniteBudgetExpiryIsNotCompleted`, `TestRunFiniteAckSuccess`, `TestRunFinitePendingFailurePreserves` | PASS |
| Quarantine giữ quota/identity; admin chỉ cần path | `TestAdminQuarantineKeepsQuotaAndValidates`, `TestOpenOutboxAdminRequiresExistingFileAndAdoptsBinding`, `TestOutboxIdentityBindingMismatch` | PASS |
| File lạ/khác version/corrupt bị từ chối, không recreate | `TestOutboxUnsupportedSchema`, `TestOutboxForeignTablesRefused`, `TestOutboxCorruptFileNotRecreated` | PASS |
| Khóa độc quyền một simulator/admin | `TestOutboxSingleActiveLock` | PASS |

**Không bao gồm (Task 3.6, NOT RUN):** mất application ACK **sau DB commit** xuyên
broker/backend, cold-crash cấp tiến trình **nhiều dịch vụ**, restart
broker/backend, queue-full/backpressure xuyên dịch vụ — đây là ma trận cross-service
của Task 3.6, không phải test unit/CLI của 3.5.8.

## 8. Giới hạn đã biết / NOT RUN

- **Smoke thật giới hạn đã chạy (3.5.8.2, PASS — §7.2):** end-to-end "1 batch 100
  mẫu → application ACK → outbox về 0 → 100 raw/1 ledger/1 temporal, reported
  đúng, desired/Gateway giữ nguyên" đã được xác nhận qua Mosquitto + TimescaleDB +
  backend thật với CLI `cmd/gateway` và outbox SQLite bền vững. Các ca 3.5.8 **cấp
  unit/CLI** (enqueue/reopen byte-exact, capacity rollback, restart pending, ACK
  sai/trễ/lặp/sớm/boot cũ, PUBACK không kèm app ACK, retry/backoff, quarantine giữ
  quota, stop hữu hạn) đã có test SQLite file thật **PASS** (§7.3). **Chưa chạy**
  phần **cross-service** của 3.5.8 — thuộc **Task 3.6**: mất application ACK sau
  DB commit xuyên broker/backend, cold-crash cấp tiến trình nhiều dịch vụ,
  restart broker/backend, queue-full xuyên dịch vụ.
- **Đã thêm** named target `gateway` trong `src/Dockerfile` (giữ `backend` là
  target cuối/default) và build/cross-build (3.5.7.3–.4) — bằng chứng ở §7.1.
  **Chưa chạy** binary ARM trên bo mạch: cross-build ARM **không** đồng nghĩa
  chạy SQLite trên board (ARM runtime **NOT RUN**).
- **Restart cấp CLI** (2 lượt thật trên 1 file SQLite: resend byte-exact row boot
  cũ + sinh batch mới dưới boot mới) đã có bằng chứng unit/CLI ở §7.2/§7.3. **Chưa
  kiểm** cold-crash/restart/mất ACK **xuyên dịch vụ** (broker/backend, nhiều
  process) — thuộc Task 3.6.
- **Harness Python:** đã đồng bộ 9 default Task 3.4 vào `DEFAULTS` của
  `native_compose_smoke.py` (+ test hồi quy), giảm 69→9 lỗi. **9 lỗi còn lại**
  (`schema19_exact_rejected`, harness native-compose còn ghim
  `expected.endswith('19:mqtt_simplified_journal')` trong khi migration
  `000020_stage3_telemetry_ingestion` đã commit ở HEAD) là **drift có sẵn**
  ngoài phạm vi Task 3.5 — **không** được tính là PASS và đã được ghi vào backlog
  (`docs/dynsec/limitations-and-backlog.md`, mục "Cập nhật Task 3.5") để xử lý ở
  lane Stage 2/Task 3.2/3.4. **Đã xử lý 16/10/2026** (xem ghi chú ở §7.2): harness
  ghim `EXPECTED_SCHEMA_TERMINAL = "20:stage3_telemetry_ingestion"`, mã lỗi đổi
  thành `schema_exact_rejected`, thêm 7 test hồi quy `SchemaHistoryTests`; kết quả
  mới `test_native_compose_smoke` 103/103 và full suite 678/678 PASS. Quan sát 9
  lỗi ở trên là bằng chứng lịch sử trước fix.
- **Task 3.5 hoàn thành có giới hạn, chờ commit:** thư viện `internal/gatewaysim`
  + CLI `cmd/gateway` + named target `gateway` + smoke thật đã có bằng chứng
  (§7.1–§7.3); **chưa commit** trên `develop` (baseline `21447d8`, worktree
  dirty). Các hạng mục NOT RUN ở trên (cross-service matrix, ARM board runtime,
  power-loss) **không** phải gate của Task 3.5.
- Không tuyên bố power-loss durability, ARM board runtime, denial/restart matrix
  hay workload đã đạt. Không sửa/xóa item pending ngoài `quarantine` có chủ đích.
- Thư viện `internal/gatewaysim` và `cmd/gateway` vẫn **chưa commit** (dirty
  worktree); `go.mod`/`go.sum` đã thêm SQLite pure-Go cùng các module indirect.

## 9. Bàn giao bước tiếp theo

- **Task 3.5.7.3–3.5.7.4 (đã xong trong slice này):** named target `gateway` đã
  thêm trong `src/Dockerfile` (giữ `backend` là target cuối/default); build
  `linux/amd64`, cross-build ARM `GOARM=7`, và kiểm Docker build/runtime với
  SQLite driver đã import thật đều **PASS** (§7.1). Runtime ARM trên board vẫn
  **NOT RUN**.
- **Task 3.5.8:** smoke **3.5.8.2** (tái sử dụng fixture Task 3.4, chế độ
  `--mode simulator`) đã chạy **PASS** (§7.2); các ca 3.5.8 cấp unit/CLI với SQLite
  file thật đã **PASS** và được đối chiếu ở §7.3. Phần còn lại là ma trận **cross-service**
  (mất application ACK sau commit, cold-crash/restart broker/backend) — chuyển sang
  **Task 3.6** để diễn tập trước khi nghiệm thu.
- **Task 3.6/3.7:** giữ pending để diễn tập mất ACK; dùng chế độ batch hữu hạn và
  counters cho đo workload.
