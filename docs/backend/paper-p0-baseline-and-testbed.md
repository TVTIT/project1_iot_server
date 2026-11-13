# P0 — Baseline và testbed server

**Hiện hành: P0.0–P0.9 hoàn thành cục bộ ngày 24/10/2026.** Lượt cuối exit 0,
`PAPER_P0_ACCEPTED`, sau hai bản sửa runner được người dùng cho phép. Backend,
API `/temporal`, migrations và cấu hình deployment không thay đổi. Hai lần FAIL
lịch sử được giữ riêng bên dưới, không đổi thành PASS. Chưa commit/push; không
tự bắt đầu P1 hoặc mở lại Stage 5.

## P0.0 — Baseline trước triển khai (24/10/2026)

- Branch `feat/paper-requirement`, HEAD
  `a5f9f812f3212413f772ef60e1299f58cca43837`, tracked/untracked working tree sạch.
  Không có diff so với baseline khảo sát của kế hoạch. `a5f9f81` đã commit 5.3–5.4;
  mô tả worktree diff trên `bcce3fe` trong handoff cũ là lịch sử trước commit.
- Source terminal migration **23: sensor_control_intent**. Đây chưa phải xác nhận
  schema DB fixture hoặc shared/live. Giữ thứ tự trước Auth tới 20, compatibility
  migration 4 và 21–23 sau GoTrue; không sửa migration.
- Module `iot-platform`, Go **1.27.2**; `src/go.mod`/`go.sum`: pgx/v5 **5.11.0**,
  Paho **1.5.1**, modernc SQLite **1.60.1**. Dockerfile builder
  `golang:1.27.2-alpine`, targets `backend`/`gateway`, CGO disabled;
  runtime dùng cùng digest Mosquitto bên dưới. Không dùng binary host thay image.
- Image references: TimescaleDB
  `timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`;
  Mosquitto
  `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`;
  GoTrue `supabase/gotrue:v2.196.0`; Storage `supabase/storage-api:v1.74.0`;
  Envoy `envoyproxy/envoy:v1.39.1`; Nginx `nginx:1.25-alpine`.
  Tags không tương đương digest immutable; image ID thực phải ghi từ lượt smoke.
- Auth hiện **HS256**, membership từ DB; WebSocket `/v1/ws` là stub 501;
  control tắt, Stage 5 vẫn dừng 5.4; media chỉ kiểm hạ tầng, chưa có workflow upload.
  PASS/FAIL/NOT RUN cũ thuộc revision/phạm vi handoff cũ, không là kết quả P0.

## P0.1–P0.4 — Profile được chọn

- Reuse `native_compose_smoke.py --paper-p0` cùng fragment
  `scripts/tests/compose.paper-p0.override.yml`; không đổi nghĩa mode cũ.
- 7 service dài hạn: PostgreSQL thật, Mosquitto, Go backend, GoTrue, Storage,
  Envoy, Nginx. Ba init jobs dùng cùng image TimescaleDB chỉ chạy role provisioning,
  application migrations và post-Auth compatibility, không là ba DB server.
  Hai Gateway CLI chạy hữu hạn, lần lượt trên cùng mạng và SQLite volume riêng;
  không có backend thứ hai. Rathole/Cloudflared không bật.
- HTTP: host loopback/cổng Docker cấp → Nginx → Go (`/v1/*`, `/readyz`);
  Nginx → Envoy → GoTrue (`/auth/v1/*`) hoặc Storage (`/storage/v1/*`).
  HTTP local dùng user/token fixture một lần, **chưa chứng minh HTTPS public**.
- MQTT: Gateway/backend → `ssl://mosquitto:8883`, CA mới/SAN `mosquitto`,
  DynSec manager/data/Gateway principals tách biệt. TLS + password, không mTLS.
  Không public DB, MQTT, Envoy admin; không mount broker private key vào Go/Gateway.
  Broker config giữ `tls_version tlsv1.2`, `require_certificate false`,
  `max_packet_size=2097152`, `message_size_limit=1048576` byte; không tune storm.
- P0: ingress **true**, credential API **true** để setup, legacy credential runtime
  **false**, Stage 5 **false**. Queue **256**, workers **4**, payload **65.536 byte**,
  DB max pool **10** (đủ worker + owner connection + HTTP/readiness).
  Body/timeouts/bounds khác giữ nguyên fixture, không tăng để che startup lỗi.
- Backend quota cấu hình **2 vCPU / 4 GiB = 4.294.967.296 byte**.
  Tất cả cùng host Docker, chưa phân bổ máy DB/load generator riêng; tài nguyên
  service khác và quota effective được ghi riêng từ lượt smoke cuối bên dưới.
- Tên/project/volumes/mạng/env/secrets/TLS/store được tạo mới. Không đọc `.env`
  deployment; process env lọc theo helper. Storage có volume/ownership riêng.
  Cleanup qua registry hiện có, exact names/IDs/labels, không prune/shared reset.

## P0.5–P0.9 — Kết quả cuối và cách chạy lại

Từ repo root, Docker + Compose >= 2.24.4, Python3, OpenSSL và `setfacl` có sẵn;
chuẩn bị các upstream images theo references ở P0.0 trong Docker cache. Wrapper
không tự pull/upgrade images và không dùng deployment env. Thư mục chỉ định phải
**chưa tồn tại**; không tái sử dụng DB/store/outbox của lượt trước:

```sh
sh scripts/test-paper-p0.sh /tmp/opencode/NEW-P0-DIRECTORY
```

Có thể bỏ đối số để wrapper tự đặt tên mới. Lệnh tạo → build → start → smoke →
cleanup, không để lại testbed live. Mode tương đương là `native_compose_smoke.py
--prepare <new-directory> --paper-p0`; không kết hợp với mode smoke khác.

Lệnh thực tế đã PASS:

```sh
sh scripts/test-paper-p0.sh /tmp/opencode/paper-p0-20261024-final
```

Project `ncs_a476497fe73c`, exit **0**, `PAPER_P0_ACCEPTED`,
`exact_cleanup_verified=true`. Summary an toàn:
`/tmp/opencode/paper-p0-20261024-final/artifacts/startup-summary.json`.
Các số liệu/IDs cần đọc lâu dài đã chép vào report này, không chỉ trỏ tới `/tmp`.
Revision **`a5f9f812f3212413f772ef60e1299f58cca43837` + uncommitted P0 diff**,
branch **`feat/paper-requirement`**, **dirty=true**, không phải clean-checkout CI.

| Kiểm tra cùng stack P0 | Kết quả thực tế |
| --- | --- |
| Build backend và Gateway từ hai target Dockerfile | PASS; không mount binary host |
| Ba init jobs, compatibility và schema | Exit 0; history trước Auth tới 20, history cuối đúng terminal 23 và constraints |
| Readiness qua Nginx, Storage qua Nginx/Envoy | 200; Storage GET bucket list bằng setup service-role, không upload media |
| GoTrue login, API provisioning/quyền | Ba user login thật; hai Gateway/Sensor và credential riêng; user A đọc A |
| Hai message, mỗi Gateway một batch 100 mẫu | SQL oracle kiểm đúng 200 raw, 2 ledgers, 2 temporal observations và provenance/measurement |
| Application commit ACK và SQLite count | Hai ACK; cả hai outbox có 0 pending/0 quarantined |
| Canonical HTTP temporal raw cho Sensor A | 100 điểm và giá trị khớp synthetic; timestamp request UTC Z |
| Quyền chéo Gateway | A đọc A → B → A: **200 → 404 → 200**, đúng code `not_found` |
| Stage 5 | Tắt; không seed hoặc tạo `reported.configuration` từ uplink |
| Metadata Git trong private HOME | PASS: chỉ ba lệnh đọc, trust đúng ROOT theo từng lệnh, không Git global; `--no-optional-locks` |
| Cleanup cuối | **15 containers, 1 network, 6 volumes** removed, gồm Storage/outbox/bootstrap |

Simulator profile: 100 samples/batch, 100 Hz (`sample_interval_us=10000`), mỗi
Gateway chỉ một batch hữu hạn. Message IDs của lượt cuối: Gateway A `161aea00-2fe5-4e35-bdc3-2819a1d64776`,
Gateway B `247635ad-7419-42fb-b877-d055e7529ad5` (100 mẫu/message).

### Tài nguyên thực và snapshot cuối

Host chung: Linux kernel **6.12.113+deb13-amd64**, **x86_64**, **12 logical CPUs**,
RAM **16.681.447.424 byte** (~15,54 GiB). Docker Engine **29.9.2**, Compose **5.6.1**,
Python **3.13.5**; OpenSSL host đã kiểm riêng **3.5.7**. Toolchain build theo P0.0.
Hai Gateway CLI/load controller cùng host; không có máy DB/load generator độc lập.

Backend inspect: **NanoCpus=2.000.000.000**, **Memory=4.294.967.296 byte** =
**2 vCPU / 4 GiB** (`CpuQuota=CpuPeriod=0` vì dùng NanoCPUs). PostgreSQL, Mosquitto,
GoTrue, Storage, Envoy và Nginx có `NanoCpus=CpuQuota=CpuPeriod=Memory=0`, tức
**không quota riêng**, cùng tranh chấp host. Không nhận là mô hình ba máy tham chiếu.

Mỗi ô là **Docker CPU% / memory used**; CPU% theo Docker, không phải % của quota
2 vCPU. Backend memory denominator là 4 GiB; các service không quota là host
15,54 GiB. Giữ nguyên MiB/KiB, không gọi đó là MB/KB.

| Service | Ready | Gateway A run | Gateway B run | Drained |
| --- | --- | --- | --- | --- |
| Backend | 0,00% / 11,71 MiB | 0,00% / 11,8 MiB | 0,00% / 11,81 MiB | 0,02% / 11,81 MiB |
| PostgreSQL | 0,01% / 45,45 MiB | 2,83% / 47,06 MiB | 0,01% / 47,12 MiB | 0,02% / 47,13 MiB |
| Mosquitto | 0,01% / 932 KiB | 0,02% / 932 KiB | 0,24% / 932 KiB | 0,01% / 932 KiB |
| GoTrue | 0,00% / 17,4 MiB | 0,02% / 17,4 MiB | 0,01% / 17,4 MiB | 0,01% / 17,4 MiB |
| Storage | 0,00% / 216,4 MiB | 0,03% / 216,4 MiB | 0,00% / 216,4 MiB | 0,00% / 216,4 MiB |
| Envoy | 0,21% / 31,26 MiB | 0,26% / 31,26 MiB | 0,19% / 31,26 MiB | 0,20% / 31,26 MiB |
| Nginx | 0,00% / 14,82 MiB | 0,00% / 14,82 MiB | 0,00% / 14,82 MiB | 0,00% / 14,82 MiB |

Khoảng gọi `docker stats --no-stream`, ngày 24/10/2026, **UTC**:

| Phase | Bắt đầu | Kết thúc |
| --- | --- | --- |
| Ready | 09:25:13.735056 | 09:25:15.764693 |
| Gateway A run | 09:25:16.318489 | 09:25:17.780800 |
| Gateway B run | 09:25:19.199592 | 09:25:21.224276 |
| Drained | 09:25:22.201321 | 09:25:23.236071 |

CLI có thể hoàn tất trong cửa sổ stats. Đây là **snapshot smoke**, không phải peak,
continuous profiling, throughput hoặc p95/p99; 0,00% là số làm tròn, không phải
không tốn CPU. Không thêm metrics/exporter/MODE của P1.

### Image IDs cuối, guard tests và bàn giao

References/tag/digest đã liệt kê ở P0.0; image IDs thực dùng trong lượt cuối:

| Service/target | Image ID (`sha256:`) |
| --- | --- |
| PostgreSQL + ba init jobs | `289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a` |
| Mosquitto | `d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9` |
| GoTrue | `c0c25187a6b835e65a6f6e6c6b39d090e832d40e6de5186f2c038e0411944232` |
| Storage | `f1546fac6d1c7e345428ac904bfaa7be7cecd50a1f549fe1cf38c628a7b15c85` |
| Envoy | `57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4` |
| Nginx | `516475cc129da42866742567714ddc681e5eed7b9ee0b9e9c015e464b4221a00` |
| `src/Dockerfile#backend` | `2e54c688ae7f22413097cbcd4946f3171656bbda57163e675f140d926a9c3bb7` |
| `src/Dockerfile#gateway` | `28182ae0d1f593079148504acf2e727fbb775c1b32feb1400d08ab51126deeba` |

- **205 guards PASS** sau sửa cuối: 116 `test_native_compose_smoke` + 89 Stage 3
  helper tests. Ba guard Git mới RED (helper chưa có) → GREEN, gồm đọc Git thật
  với private HOME; chỉ cho phép `rev-parse HEAD`, `branch --show-current`,
  `status --porcelain`. Không global/wildcard trust hoặc bỏ metadata khi lỗi.
- Python compile, shell syntax, `git diff --check` PASS. Không sửa bootstrap hoặc
  Stage 3 helpers, không chạy lại mọi fixture. Guard tests không thay native smoke.
- Private inputs lượt cuối được runner xóa sau cleanup. Hai lượt FAIL cũ đã được
  re-inspect **đúng resource names trong receipts**, xác nhận absence rồi xóa đúng
  private directories do phiên này tạo. Chỉ giữ artifacts an toàn; hai FAIL lịch sử
  không bị xóa/đổi status. Build images/cache giữ theo lifecycle hiện có, không prune.
  Hậu kiểm ban đầu giả định mọi receipt có `test.run` và dừng trước xóa; kiểm lại
  schema registry cho thấy Compose network/volume dùng `io.iot.native-smoke.run`.
  Hậu kiểm đúng cả hai loại label đã PASS; không bỏ qua ownership/absence checks.
- Full local/remote CI, Go/race/vet/lint ngoài Docker build, board/HIL, HTTPS public,
  WebSocket runtime, execution/result Stage 5 và media upload: **NOT RUN/ngoài P0**.
  HS256, MQTT TLS + DynSec, một backend vẫn giữ nguyên. Limitation/ảnh hưởng và mốc
  xem lại tại [backlog chung](../dynsec/limitations-and-backlog.md).
- Bước tiếp theo là P1 **chỉ khi được giao**; P0 không tự mở roadmap hay Stage 5.
  Checklist kế hoạch ngoài repo đã tick; không nằm trong commit `main_src`.

<details>
<summary>Hai lượt FAIL lịch sử trước nghiệm thu — giữ nguyên checkpoint và số đo</summary>

## Lượt thực hiện đầu — DỪNG tại P0.7

**P0 chưa nghiệm thu.** Lệnh thực tế:

```sh
sh scripts/test-paper-p0.sh /tmp/opencode/paper-p0-20261024-first
```

Exit **1**, `STARTUP_FAILED`, phase `paper_p0`, code `paper_p0_read_failed`,
`cleanup_verified=true`. Không sửa runner hoặc chạy lại sau lỗi; chờ người dùng
cho phép. Không có thay đổi Go, migration, deployment env hoặc shared/live stack;
chưa commit/push. Revision runtime là baseline trên cộng uncommitted P0 diff.

### Đã xác nhận trong lượt này

- **P0.0–P0.6:** profile riêng đã clean-start tới Nginx; backend/Gateway đều build
  từ target Dockerfile thật. PostgreSQL, GoTrue, backend, Storage healthy;
  Mosquitto/Envoy/Nginx qua startup probe. Ba init jobs exit 0; history trước Auth
  và history terminal **23** khớp chính xác source, compatibility constraints có.
- Probe `GET /storage/v1/bucket` qua Nginx/Envoy bằng server-side service-role
  trả 200/list; `/readyz` qua proxy 200. Không thử upload/private media.
- Ba user login thật, admin tạo hai Gateway qua API, mỗi owner tạo Sensor;
  credential API cấp hai credential riêng. User A đọc detail Sensor A thành công.
- Trong P0.7, cả hai CLI đều `enqueued=1 publish_attempts=1 app_acknowledged=1
  pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0
  completed=true`. Hai lần CLI `count` độc lập cùng xác nhận outbox rỗng.
  SQL oracle của Stage 3 cho Gateway A đã hoàn tất (runner đi tới Gateway B);
  failure code không phải lỗi SQL. Không gán toàn bộ assertions cuối lượt thành PASS.
- Assertion quota runtime **2 vCPU / 4 GiB** đã đi qua trước các Gateway CLI;
  không có bảng snapshot được lưu thành công: samples đang ở biến local khi lỗi
  xảy ra. Không khôi phục/bịa số CPU/RAM từ YAML hoặc build log.
- Cleanup receipts xác nhận **15 containers, 1 network, 6 volumes** đều removed,
  không còn intent allocated chưa removed. Gồm Storage, hai outbox và bootstrap
  store; không prune hay xóa tài nguyên shared. Build images/cache được giữ lại
  theo lifecycle hiện có. Private inputs của lượt lỗi được giữ để chẩn đoán,
  **có secrets, không upload/commit**.

### Điểm vướng và phạm vi cần cho phép

Đọc source sau lỗi xác định một lỗi trong **runner P0 mới**, không cần thay
hợp đồng backend: query `/temporal` đang dùng `datetime.isoformat()` cho
`timeAt`/`endTimeAt`, tạo timezone suffix `+00:00`; parser hiện hành bắt buộc
UTC hậu tố **`Z`** (`history_query_parser.go`, `parseRFC3339MicroUTCZ`).
Query này chắc chắn không hợp lệ theo contract, dù offset biểu diễn cùng UTC.

Chẩn đoán runtime hiện chỉ lưu mã chung `paper_p0_read_failed`, chưa lưu status/body
của temporal hoặc checkpoint chi tiết từng read. Vì vậy **chưa có response HTTP
gốc để khẳng định độc lập rằng đây là assertion đầu tiên thất bại**. Đây là lỗi
runner cụ thể phải sửa trước lần chạy tiếp; không kết luận backend bị lỗi.

Đề xuất chờ duyệt: thêm regression cho timestamp UTC `Z`, sửa riêng serialization
của runner, lưu checkpoint/status an toàn để phân biệt temporal với read khác,
sau đó chạy lại guard tests và **một** smoke trên fixture mới. Không sửa parser,
không nới auth/TLS/ACL, không mở Stage 5 hoặc P1. Denial chéo Gateway và nghiệm thu
HTTP history **chưa được xác nhận**; P0.8/report số đo và đóng P0.9 còn chưa xong.

### Affected checks đã chạy

- `python3 -m unittest scripts/tests/test_native_compose_smoke.py`: **111 PASS**.
  Ba test P0 ban đầu RED vì mode/quota chưa có; sau triển khai GREEN. Guard model
  phát hiện `mem_limit` của Compose JSON là string; đã normalize trước native run,
  không bỏ kiểm quota.
- `python3 -m unittest scripts/tests/test_stage3_uplink_runner.py`: **89 PASS**.
- `python3 -m py_compile` runner/test module, `sh -n scripts/test-paper-p0.sh`,
  `git diff --check`: **PASS trước native run**. Guard tests không thay native PASS.
- Full Go/race/vet/lint/CI, board/HIL, HTTPS public: **NOT RUN**, không sửa Go behavior.

Safe failure summary: `/tmp/opencode/paper-p0-20261024-first/artifacts/smoke-failure.json`.
Private startup progress và exact cleanup receipts cùng lượt đã được đọc;
chỉ các trường an toàn được ghi lại ở đây. Backend image:
`sha256:f0fa2141dde1a0de61226742ed5f6517039df2a8bf03a14071fe0d4a27dfe95e`.
Không có summary P0 ACCEPTED hoặc baseline benchmark mới.

## Lượt hai — sửa runner UTC Z đã được duyệt; dừng tại metadata Git

Người dùng đã chấp nhận sửa riêng runner, không đổi backend hoặc response
`/temporal`. Bổ sung `paper_p0_temporal_query`: UTC `Z`, tối đa 6 chữ số phần lẻ,
không suy timezone cho timestamp naive. Hai regression mới RED (helper chưa có),
sau sửa GREEN. Chẩn đoán private nay lưu checkpoint/status/count và snapshot,
không lưu token hay response body. Không đổi `src/`, migrations, Compose root
hoặc config triển khai. README đã sửa ghi chú SHA trước-commit lỗi thời.

Lệnh thực tế:

```sh
python3 -m unittest scripts/tests/test_native_compose_smoke.py scripts/tests/test_stage3_uplink_runner.py
sh scripts/test-paper-p0.sh /tmp/opencode/paper-p0-20261024-utc-z
```

- Guards **202 PASS** (113 native-Compose guards + 89 Stage 3 helper tests).
  Python compile, shell syntax và `git diff --check` PASS trước smoke.
- Native entrypoint **FAIL**, exit 1, phase `paper_p0`, error type `Exception`,
  code `unclassified`, `cleanup_verified=true`. Stderr: Git báo
  **`detected dubious ownership in repository`** tại lệnh lấy revision.
- **P0.5–P0.7 PASS trong lượt hai:** clean-start/schema 23, readiness/Storage,
  ba login thật, provisioning hai graph/credentials, hai commit ACK/empty outboxes,
  SQL oracle cho đủ **200 raw / 2 ledgers / 2 temporal observations**, reported
  measurement đúng, không tạo `reported.configuration`. HTTP temporal raw trả đủ
  **100 điểm**, giá trị khớp synthetic; positive/negative/positive **200/404/200**.
  Checkpoint cuối `cross_gateway_bracket` ghi chính xác `[200,404,200]`, chỉ tới
  được sau khi temporal đã qua assertion; không phải response bị bỏ qua.
- Cleanup cả **15 containers / 1 network / 6 volumes** xác nhận removed;
  không còn allocated intent chưa removed. Private inputs của hai lượt lỗi
  vẫn giữ kín, không upload; artifacts failure giữ nguyên. Không có summary ACCEPTED.

### Quota thực và snapshot của lượt hai (không phải benchmark)

Inspect backend: `NanoCpus=2000000000`, `Memory=4294967296`; `CpuQuota=0`,
`CpuPeriod=0` vì CPU limit biểu diễn bằng NanoCPUs. Các service còn lại trong bảng
có cả bốn trường trên bằng **0**, tức **không đặt quota container riêng**, không
phải sử dụng 0 tài nguyên. Tất cả cùng host, chưa có máy DB/load generator riêng.

Mỗi ô dưới là **CPU% / memory used** từ Docker stats; memory hiển thị theo đơn vị
Docker, không đổi MiB thành MB. Backend có limit 4 GiB; denominator của các service
không quota là host 15,54 GiB, không phải giới hạn riêng được cấu hình.

| Service | Ready | Gateway A run | Gateway B run | Drained |
| --- | --- | --- | --- | --- |
| Backend | 0,00% / 7,984 MiB | 0,00% / 8,043 MiB | 0,00% / 8,051 MiB | 0,00% / 8,059 MiB |
| PostgreSQL | 0,01% / 47,93 MiB | 0,01% / 49,51 MiB | 2,75% / 49,58 MiB | 0,02% / 49,58 MiB |
| Mosquitto | 0,01% / 928 KiB | 0,01% / 928 KiB | 0,22% / 928 KiB | 0,01% / 928 KiB |
| GoTrue | 0,02% / 16,56 MiB | 0,02% / 16,55 MiB | 0,00% / 16,55 MiB | 0,01% / 16,55 MiB |
| Storage | 0,01% / 210,2 MiB | 0,00% / 210,2 MiB | 0,00% / 210,2 MiB | 2,13% / 210,2 MiB |
| Envoy | 0,26% / 29,12 MiB | 0,20% / 29,11 MiB | 0,22% / 29,12 MiB | 0,26% / 29,12 MiB |
| Nginx | 0,00% / 10,45 MiB | 0,00% / 10,45 MiB | 0,00% / 10,45 MiB | 0,00% / 10,45 MiB |

Khoảng gọi `stats --no-stream` ngày 24/10/2026, **UTC**:

| Phase | Bắt đầu | Kết thúc |
| --- | --- | --- |
| Ready | 09:13:25.818556 | 09:13:27.848367 |
| Gateway A run | 09:13:28.378455 | 09:13:29.859618 |
| Gateway B run | 09:13:31.292249 | 09:13:33.316825 |
| Drained | 09:13:34.224392 | 09:13:35.327603 |

Các cửa sổ gọi stats bắt đầu sau khi start Gateway; CLI hữu hạn có thể hoàn tất
trong cửa sổ. Đây không phải continuous profiling/peak hay CPU bằng 0 thực sự;
không suy throughput, p95/p99 hoặc capacity từ hai message và số làm tròn 0,00%.

### Vướng mắc mới — cần cho phép trước khi tiếp tục

Runner gọi Git bằng process env đã lọc, HOME là private fixture mới, nên không
kế thừa cấu hình trust Git đang cho phép checkout này trong shell của phiên.
Git từ chối đọc revision do ownership checkout. Đây là lỗi thu thập metadata của
runner; không phải lỗi dữ liệu, TLS/auth hay `/temporal`. Metadata/report cuối
P0.8 và đóng P0.9 vẫn chưa hoàn tất. Không dùng shell ngoài để gán PASS giả cho
entrypoint đang exit 1.

Đề xuất chờ duyệt: trust **đúng ROOT repo** chỉ trong lệnh Git read-only bằng
`git -c safe.directory=<validated-repository-root> ...`, giữ process env lọc;
capture/sanitize lỗi, thêm guard và chạy một lượt fixture mới. **Không** chạy
`git config --global`, không trust wildcard `*`, không đổi owner/quyền repo,
không sửa backend hoặc bỏ qua revision/dirty metadata để đạt PASS.

Nguồn của bảng trên: `private/paper-p0-resources.json`,
`private/paper-p0-observations.json`, `private/paper-p0-read-progress.json` và
cleanup receipts trong fixture `paper-p0-20261024-utc-z`. Chỉ bản tóm tắt an toàn
ở report này được dùng lâu dài; không upload cả `private/`.

</details>

Kế hoạch: [P0 detail](../../../docs/backend_plan/paper/P0_detail_plan.md).
Ranh giới hiện hành: [5.4 handoff](stage-5-task-5.4-publisher-acl-and-expiry.md),
[credential contract](stage-2-current-credential-acceptance.md),
[backlog chung](../dynsec/limitations-and-backlog.md).
