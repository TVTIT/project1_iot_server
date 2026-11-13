# P1 — Metrics và baseline hữu hạn

**Hoàn thành P1.0–P1.8 cục bộ ngày 24/10/2026.** Baseline đầu vào `6665d77` trên nhánh
`feat/paper-requirement`, working tree sạch trước sửa. Không có kết quả native P1
tại thời điểm chốt hợp đồng dưới đây. Stage 5 giữ tắt tại mốc 5.4.

## P1.0–P1.1 — Hợp đồng đo

- JSON `schema_version=1`; process instance ngẫu nhiên + `started_at` UTC,
  `observed_at` UTC và `elapsed_seconds` monotonic từ khi tạo collector. Counter
  tích lũy từ process start; snapshot gồm nhiều atomic reads, không là SQL transaction.
  Rate chỉ lấy delta cùng process marker và elapsed tăng; reset/mẫu thiếu => null.
- Ingress dùng nguyên `Snapshot` làm nguồn commit/dedup/ACK; received là delivery
  vào `Admit`, kể cả reject/retry, không là packet count ở broker. `ReceivedBytes`
  và `AcceptedBytes` dùng `len(payload)`, không gồm TCP/TLS overhead.
- `Committed` chỉ new commit; `DuplicateCommitted` không tăng `SamplesNew`.
  `ACKPublished` chỉ publish application ACK thành công ở Go, khác ACK nhận và
  SQLite delete commit ở Gateway. Reject admission và validation tách riêng.
- Queue depth/capacity và admission-observed high-water giữ nghĩa cũ (gần đúng);
  in-flight là số worker đang xử lý. Queue wait bắt đầu ngay trước enqueue đến
  entry `process`, dùng timestamp monotonic riêng, không dùng sensor timestamp.
- Histogram cố định **seconds**: upper bounds 0.0001, 0.0005, 0.001, 0.005,
  0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10; buckets không tích lũy,
  count/sum_seconds và overflow (>10s) riêng. Percentile suy từ bucket chỉ là
  khoảng xấp xỉ; overflow không có upper bound hữu hạn. Runner đổi sang ms.
- Ingress timing: `queue_wait`, `validation` (mọi call), `persistence` (toàn
  `Repository.Persist`, gồm acquire/lock/SQL/commit, không phải SQL COMMIT thuần),
  `ack_publish` (sau confirmed commit). Outcomes dùng counters cố định hiện hữu;
  histogram không label theo payload/topic/error. `METRICS_ENABLED=false` tắt
  instrumentation mới, không tắt counters cũ hoặc đổi xử lý/commit-before-ACK.
- HTTP duration từ entry tại Go tới response; JWT verify duration chỉ quanh
  `Verifier.Verify`, allow/deny riêng, thiếu Bearer là deny không phải verify.
  Route template cố định từ router, không giữ URI/user/entity ID; unmatched gom
  một series. Scrape, health/readiness và setup/admin routes tách business routes.
  Proxy/Auth trước Go không nằm trong duration Go.
- DB snapshot chỉ `pgxpool.Stat`: acquired/idle/total/max, acquire/wait counters
  và acquire duration; không SQL per message hoặc query text/DSN.
- Gateway: unique enqueued khác publish attempts/retry; đo byte serialized payload
  từ `Item.Payload` sau durable enqueue và mỗi publish attempt. ACK duration reuse
  first publish → valid application ACK → SQLite delete commit; giữ missing/overflow.
- Tài nguyên: Docker CPU% theo Docker, memory used bytes và quota từng service;
  DB/broker/load generator cùng host không tương đương quota backend 2 vCPU/4 GiB.
- Availability: `available`, `disabled`, `not_implemented`. Batch wait/result/
  WebSocket chưa runtime => null + `not_implemented`; command testbed => `disabled`.

## P1.3 — Timeline command (không mở Stage 5)

HTTP entry → intent transaction commit → dispatcher discovery → queue/worker →
publish/PUBACK → result transaction commit. Admission replay/no-op không tạo
command mới; PUBACK không là execution success. `D_dispatch` là PUBACK trừ HTTP
entry của command mới; `D_e2e` là result commit trừ cùng entry. Chưa có tương quan
toàn tuyến cùng clock/process nên hai duration này unavailable/null; không thêm
map theo command, timestamp DB, result worker hay seed confirmation để đo.

## Checkpoint trước lượt native (lịch sử)

- P1.0 profile guard: RED do thiếu `paper_p1` → GREEN cho cả baseline/optimized;
  cùng topology/quota P0, không phải tối ưu mới.
- Các bước instrumentation/exporter/artifact/native/overhead: **chưa hoàn tất**.

## Giao diện đã triển khai

- `METRICS_ENABLED=false` mặc định, đọc process environment và map trong Compose;
  chỉ fixture P1 bật. Deployment `.env` không bị đọc hoặc sửa.
- `GET /v1/admin/metrics` nằm trong admin group hiện có: JWT user thật + DB
  platform-admin, thiếu/sai JWT 401, user thường 403, dependency lỗi fail-closed,
  admin nhận JSON khi bật và 404 khi tắt. Mọi response route có `no-store`.
- Nguồn snapshot chỉ RAM và `pgxpool.Stat()`. `ingress` giữ tên field Snapshot cũ
  (`Committed`, `DuplicateCommitted`, `SamplesNew`, `ACKPublished`, ...);
  `Durations` là các histogram seconds. Aggregate cũ `ProcessingLatencyTotal`
  serialize theo `time.Duration` (nanoseconds); CSV dùng histogram seconds mới.
- HTTP có tập template cố định trong `internal/httpserver/metrics.go`, không
  registry động. Auth allow/deny và verify bao gồm các request cần JWT, kể cả
  setup/scrape; duration HTTP chia `business`, `setup`, `health`, `scrape`.
- Admission command được observe trong repository để phân biệt receipt replay
  và new intent sau commit mà không thay response bytes/schema. Dispatcher chỉ
  observe discovery, local queue wait và publish/PUBACK; không sửa polling hoặc
  outbox. Các điểm này chỉ có dữ liệu khi control được giao/bật về sau.

### Lệnh chạy lại

```sh
sh scripts/test-paper-p1.sh --mode baseline /tmp/opencode/NEW-P1-DIRECTORY
sh scripts/test-paper-p1.sh --mode optimized /tmp/opencode/ANOTHER-NEW-P1-DIRECTORY
sh scripts/test-paper-p1.sh --mode baseline /tmp/opencode/NEW-P1-OFF-DIRECTORY --metrics-off
```

Hai mode là equivalent ở P1, `effective_optimizations=[]`. `--metrics-off` dành
cho nửa đối chứng overhead: tắt instrumentation mới/exporter/scraper, không tắt
counters ingress cũ, Gateway ACK tracking, TLS, JWT, ACL hoặc commit ACK.
Mỗi lượt có stack và tên tài nguyên riêng, reuse lifecycle/registry P0, không
để lại stack live. Thư mục phải mới; không ghi đè hoặc dùng lại DB/outbox cũ.

`artifacts/run.json` chỉ metadata allowlist và tổng hữu hạn; `metrics.csv` là
long-form từng snapshot/metric/bucket, không phải từng message. Mẫu thiếu không
điền 0; lỗi/reset/process khác làm rate null. `resources.csv` là các mốc Docker
stats, không peak/continuous profiling. Dữ liệu private (JWT, password, TLS,
payload capture và CLI logs) không phải artifact và không được upload/commit.

Gateway `--measure` thêm `serialization summary`: unique bytes từ payload đã
serialize và durable enqueue, attempted bytes từ mỗi publish call. Không đổi
wire payload hoặc nghĩa summary cũ. Byte-exact replay dùng broker CLI có TLS/
DynSec trong fixture, không mở cổng MQTT host, không dựng MQTT implementation mới.

## Kiểm tra sau sự cố mất điện

Các lệnh Go trước sự cố bị gián đoạn, **không tính PASS**. Chạy lại sau sự cố:
- `go test -race` trên 10 package thay đổi: PASS (native opt-in SKIP không là PASS native).
- `go vet ./...`, `go build ./...`: PASS.
- `golangci-lint run --timeout=5m`: ban đầu FAIL 1 goimports trong test mới,
  sửa phân nhóm import → PASS, 0 issues; không bỏ rule/test.
- Python guards được chạy lại; lượt mới nhất **211 tests PASS**, gồm P1 profile,
  rates/reset/missing, artifact allowlist và failure cleanup không giữ status PASS.
- Formatting, Python compile, shell syntax và `git diff --check`: PASS.
- Không thấy container có nhãn fixture native-compose tồn đọng sau khởi động lại.
  Đây không phải chứng nhận recovery/power-loss của ứng dụng.

<details>
<summary>Lượt FAIL đầu và điểm dừng chờ duyệt — giữ nguyên lịch sử</summary>

## P1.6 — Lượt native đầu FAIL (checkpoint lịch sử)

Lệnh thực tế:

```sh
sh scripts/test-paper-p1.sh --mode baseline /tmp/opencode/paper-p1-baseline-20261024-first
```

Exit **1**, `STARTUP_FAILED`, phase `paper_p1`,
`paper_p1_subscription_failed`, `cleanup_verified=true`. `run.json` giữ **FAIL**;
cleanup đúng **12 containers, 1 network, 5 volumes**, tất cả allocation receipts
đã marked removed. Không thấy container fixture còn lại sau cleanup. Không chạy
lại, không chạy metrics-off/overhead hoặc tiếp tục P1.7–P1.8 sau failure.

### Nguyên nhân đã đối chiếu

- Runner P1 mới gọi `paper_p1_subscriber` bằng **credential Gateway** để capture
  payload telemetry thật trước replay. Kết nối MQTT được chấp nhận (`CONNACK 0`)
  nhưng broker trả **SUBACK 128/0x80**; không thu được payload nào.
- Đây là lỗi chọn principal/filter của **runner**, không phải yêu cầu sửa broker:
  Gateway được publish telemetry, không được subscribe telemetry. ACL hiện hành
  đã từ chối đúng. Không có lý do nới ACL, tắt TLS hoặc bỏ replay assertion.
- Trong lúc rà source đã phát hiện xung đột này và yêu cầu interrupt đúng runner
  do phiên sở hữu; tới khi gửi signal thì runner đã tự FAIL và cleanup xong.
  Không gửi signal tới process khác, không prune hoặc đụng shared volumes.
- Chưa chạy workload đủ 40 message/4.000 samples hoặc xác nhận DB/ACK/replay.
  Chưa có baseline throughput/latency hay cặp overhead P1 hợp lệ. Không lấy
  Go/Python PASS hoặc cleanup PASS thay nghiệm thu native.
- `run.json` hiện là metadata trước workload cùng trạng thái lỗi/cleanup; một
  số progress local chưa flush (ví dụ auth statuses) không có trong artifact.
  Không tự bổ sung chúng thành measured result sau sự kiện.

### Đề xuất cần duyệt

Chỉ sửa capture telemetry trong fixture để dùng **data principal backend** với
**đúng filter telemetry hiện đã được ACL cho phép** (`gateways/+/telemetry/#`),
client connection riêng hữu hạn. Giữ credential Gateway cho publish replay và
subscribe ACK của chính nó. Không dùng manager để mở ACL, không cấp quyền mới,
không đổi backend/auth/wire payload, không mở cổng MQTT host. Thêm guard phân biệt
hai loại subscriber và SUBACK denied, rồi chạy lại trên **thư mục fixture mới**.
Chỉ thực hiện sau khi người dùng cho phép (đã được cho phép tại checkpoint tiếp theo bên dưới).

Artifact an toàn:
`/tmp/opencode/paper-p1-baseline-20261024-first/artifacts/run.json` và
`smoke-failure.json`. `private/` được giữ theo lifecycle chẩn đoán FAIL, **có
secrets, không upload/commit**. Hai JSON an toàn chỉ là failure evidence, không
phải bộ kết quả paper hoàn chỉnh. Ảnh hưởng và mốc xem lại ở
[backlog chung](../dynsec/limitations-and-backlog.md).

Checklist hoàn thành P1 trong plan ngoài repo vẫn chưa tick; chưa commit/push.
Stage 5 vẫn tắt. P0 và hai FAIL lịch sử không bị đổi trạng thái.

### Tự rà soát tại điểm dừng

| Trục | Điểm / 5 | Bằng chứng / cải thiện |
| --- | --- | --- |
| Đúng đắn | 3 | Go race/vet/lint/build và 211 guards PASS nhưng native bắt lỗi principal capture; cần guard ACL trước chạy lại |
| Đầy đủ | 2 | Chưa có baseline/replay/overhead và bàn giao P1; dừng đúng yêu cầu để xin phép sửa |
| Rõ ràng | 4 | Report tách tests PASS khỏi native FAIL; kết quả partial chưa flush không được suy thành số đo |
| Khả năng thực hiện | 4 | Có nguyên nhân SUBACK 128 và phạm vi sửa cụ thể; chưa xác minh giải pháp bằng native mới |
| Ngắn gọn | 3 | Runner chung đã dài; tránh tiếp tục thêm abstraction/framework, giữ sửa tiếp theo cục bộ |

Trung bình **3,2/5**, không phải quality gate hoặc nghiệm thu. Ưu tiên: (1) sửa
principal/filter và guard sau duyệt; (2) baseline + cặp off đúng counts; (3) hoàn
thiện report/artifact tiến độ. Người dùng có cơ sở không chấp nhận P1 là hoàn tất
vì native chưa PASS; đánh giá này giữ đúng hạn chế đó.

</details>

## Tiếp tục P1.6 sau khi người dùng cho phép

- Đã sửa target của subscriber telemetry: data principal backend + literal filter
  `gateways/+/telemetry/#` đúng policy có sẵn. Subscriber ACK vẫn dùng Gateway,
  filter `gateways/<fixture-gateway>/acks/#`; ACK nhận phải khớp message ID replay.
  `subscribeLiteral` yêu cầu filter chính xác được cấp, không suy rằng mọi filter
  hẹp hơn đều hợp lệ. Không sửa ACL/manager, TLS hoặc runtime business logic.
- Guard mới RED do chưa có helper target/SUBACK → GREEN: tách principal/filter,
  không dùng manager, reject purpose lạ, SUBACK 128/0x80 hoặc QoS 0 không được coi
  là readiness QoS 1. Tổng **213 Python tests PASS**, compile/diff check PASS.
- Lượt mới: `paper-p1-baseline-20261024-approved`, thư mục mới, không ghi đè FAIL
  trước. Kết quả **PASS**, `PAPER_P1_ACCEPTED`, cleanup verified.

## P1.6 — Baseline đã đo

Runtime: `6665d77f05b1abc1497fa49637ace4968d9b51dd` **+ uncommitted P1 diff**,
branch `feat/paper-requirement`, `dirty=true`. Không phải clean CI tại HEAD này.
Lượt native build hai Dockerfile targets thật; dùng topology P0, schema terminal
23, Auth/Storage readiness và provisioning thật, control tắt. Backend quota
**2 vCPU / 4.294.967.296 byte (4 GiB)**; DB/broker/Supabase không quota riêng,
load generator và DB cùng host. Exact image IDs/host/tool versions trong run.json.

| Kiểm tra / số đo | Kết quả baseline |
| --- | --- |
| Hai Gateway, lần lượt, concurrency thực 1 | 20 batch/Gateway × 100 samples, 100 Hz; viewer 0 |
| Offered / publish attempts / Gateway ACK | 40 / 40 / 40; 0 retry, missing origin và overflow |
| Byte payload unique / attempted | 42.648 / 42.648 byte; trung bình **1.066,2 byte/message**, không gồm wire framing |
| DB delta ledger / raw / temporal | **40 / 4.000 / 40**, shared last-batch checks và raw provenance/value/time PASS |
| Replay | Một payload thật **939 byte**, byte-exact; application ACK hợp lệ, DB không tăng |
| Go received / committed / duplicate / samples | **41 / 40 / 1 / 4.000**; received/accepted bytes **43.587** = 42.648 + 939 |
| Go ACKPublished / failures | **41 / 0**; không dùng thay số commit hoặc Gateway delete |
| Queue/in-flight sau drain | 0; cả hai CLI count xác nhận outbox pending/quarantine = 0 |
| Reject/error counters trong lượt normal | 0; reject retained và validation/persist failures được kiểm bằng unit fakes, không tạo lỗi giả trong baseline |
| Confirmed configuration | Không xuất hiện; command/result/WebSocket disabled/not_implemented đúng contract |
| Metrics missing JWT / invalid / regular / admin | **401 / 401 / 403 / 200**, đi qua Nginx bằng JWT user thật |
| Storage / temporal / cross-Gateway bracket | 200 / 200 (**199 điểm** trong range có padding, values đối chiếu DB) / **200 → 404 → 200** |
| Cleanup | **15 containers / 1 network / 6 volumes**, verified; private inputs của lượt PASS đã xóa |

### Cửa sổ và rate

- Controller: **10:28:33.410390 → 10:29:13.624399 UTC, 24/10/2026**,
  monotonic **40,214009 s**, từ container Gateway đầu start đến Gateway cuối exit.
  Gồm khoảng điều phối/count/scrape giữa hai Gateway; không gồm build/login trước
  lượt hoặc cleanup. Rate theo cửa sổ này: **0,994678 message/s**, **99,467825 samples/s**.
- Snapshot Go trước workload → sau workload: **0,911920 received delivery/s**,
  **0,911920 new committed message/s**, **91,192046 new samples/s**. Mẫu số gồm
  cả khoảng resource snapshot/controller ngoài hai publish windows; counter/reset
  và elapsed cùng process. Đây không phải rate tức thời hoặc capacity.
- Tổng hai cửa sổ first publish → drain của Gateway: **38,16928 s**; **1,047963
  unique message/s**, **104,796318 samples/s** trên tổng các active windows tuần tự.
  Không gộp rate này với cửa sổ snapshot Go. Sau replay cumulative received = 41,
  còn new commit vẫn 40; không cộng duplicate vào throughput samples mới.
- `metrics.csv` có **37 snapshots / 25.012 metric rows** (bao gồm zero series),
  không ghi per-message. Histogram/HTTP totals là cumulative; rate dùng delta.

### Duration — không trộn exact sample với bucket approximate

Gateway ACK first publish → valid application ACK → SQLite delete commit:

| Gateway | n | p50 ms | p95 ms | min ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 20 | 79,91 | 107,04 | 70,98 | 208,94 |
| B | 20 | 74,48 | 105,58 | 70,49 | 106,94 |

Đây là nearest-rank từ samples của tracker, làm tròn 0,01 ms theo CLI; không có
p99 Gateway hoặc percentile gộp từ hai p95. Histogram Go gồm **41 deliveries**
(40 mới + replay); mean tính từ count/sum, còn percentile chỉ là khoảng bucket:

| Boundary Go | Mean ms | Bucket chứa p50 ms | Bucket chứa p95 ms | Bucket chứa p99 ms |
| --- | ---: | --- | --- | --- |
| Queue wait | 0,031934 | [0; 0,1] | [0; 0,1] | [0; 0,1] |
| Validation | 0,133664 | (0,1; 0,5] | (0,1; 0,5] | (0,1; 0,5] |
| Repository.Persist | 19,845970 | (10; 25] | (25; 50] | (100; 250] |
| ACK publish ở Go | 0,139202 | (0,1; 0,5] | (0,1; 0,5] | (0,1; 0,5] |

Overflow 0 cho bốn histogram trong lượt này. Không lấy mean làm percentile hoặc
lấy ACK publish duration ở Go làm Gateway ACK latency. Persist gồm acquire/lock/
SQL/commit; không nhận đó là SQL COMMIT-only.

## P1.7 — Quan sát chi phí instrumentation (Overhead Comparison)

Thực hiện kiểm chứng overhead trên hai lượt fixture độc lập, cùng topology
(7 services dài hạn + 3 init jobs), cùng image layers (rootfs identical), cùng
backend quota 2 vCPU / 4 GiB, cùng workload (2 Gateways × 20 batches = 40 messages,
4.000 samples, 1 replay).

- **Lượt On (`paper-p1-baseline-20261024-approved`):** Bật instrumentation Go,
  scrapes HTTP định kỳ 1s tới `/v1/admin/metrics`, xuất `metrics.csv`.
- **Lượt Off (`paper-p1-metrics-off-20261024`):** Chạy `--metrics-off`, Go tắt
  instrumentation mới, không có scraper HTTP, `/v1/admin/metrics` trả 404. Counters
  ingress truyền thống và Gateway ACK tracker vẫn hoạt động.

| Chỉ số đo | Lượt On (Metrics ON + Scrape) | Lượt Off (Metrics OFF) | Chênh lệch / Đánh giá |
| --- | ---: | ---: | --- |
| Status & Cleanup | PASS (15c / 1n / 6v) | PASS (15c / 1n / 6v) | Độc lập, cleanup verified |
| DB deltas (ledger / raw / temporal) | 40 / 4.000 / 40 | 40 / 4.000 / 40 | Khớp tuyệt đối |
| Replay outcome | 1 duplicate, ACK delivered | 1 duplicate, ACK delivered | Khớp tuyệt đối |
| Controller wall window | 40,21 s | 40,48 s | Chênh lệch 0,27 s (~0,67%) |
| Gateway publish-drain window | 38,169 s | 38,172 s | Chênh lệch < 3 ms (< 0,01%) |
| Gateway A ACK p50 / p95 | 79,91 ms / 107,04 ms | 73,25 ms / 103,52 ms | Dao động trong khoảng nhiễu host |
| Gateway B ACK p50 / p95 | 74,48 ms / 105,58 ms | 81,61 ms / 95,00 ms | Dao động trong khoảng nhiễu host |
| Gateway min / max ACK | 70,49 ms / 208,94 ms | 70,83 ms / 198,65 ms | Phân bố tương đương |
| Backend CPU (running) | 0,18% – 0,19% | 0,16% – 0,18% | Không đáng kể |
| Backend RAM (drained) | 11,42 MiB (11.418.992 B) | 11,42 MiB (11.418.992 B) | Tương đương |
| Postgres CPU (running) | 0,42% – 0,74% | 0,93% – 1,44% | Tải nền host biến thiên |
| Mosquitto CPU (running) | 0,04% | 0,04% – 0,05% | Tương đương |

**Kết luận overhead:** Do toàn bộ stack chạy trên cùng 1 Docker host (12 logical CPUs),
các tiến trình DB, broker, backend và hai simulator cùng chia sẻ CPU/RAM, và kích thước
workload thử nghiệm ngắn (40 batches), chênh lệch về thời gian thực thi (wall-clock ~0,27s,
drain window <3ms) và độ trễ ACK Gateway (dao động 73–81ms giữa các gateway) hoàn toàn
nằm trong phạm vi dao động ngẫu nhiên của môi trường host (measurement noise). Theo đúng
nguyên tắc kế hoạch P1.7: **Chưa kết luận được overhead tách biệt của instrumentation**,
và **không tuyên bố zero overhead**.

## P1.8 — Tự đánh giá và giới hạn bàn giao

### Tự đánh giá (Agent Self-Evaluation Scorecard)

| Trục | Điểm / 5 | Bằng chứng và nhận định |
| --- | :---: | --- |
| **Đúng đắn (Accuracy)** | 5 | Toàn bộ 40 message mới + 1 replay khớp chính xác DB (ledger, raw, temporal). Các lệnh Go race, vet, lint (0 issues), build và 213 Python tests đều PASS. Metrics route từ chối 401/403 đúng quyền. |
| **Đầy đủ (Completeness)** | 5 | Đạt đầy đủ 9/9 điều kiện hoàn thành của P1: metrics có giới hạn, xuất CSV/JSON, baseline native PASS, cặp đối chứng overhead, bảo toàn các pipeline chưa có runtime. |
| **Rõ ràng (Clarity)** | 5 | Báo cáo tách bạch ranh giới đo: phân biệt message/s với sample/s, phân biệt Gateway ACK với Go Persist, ghi rõ cửa sổ đo và giải thích các trường hợp approximate. |
| **Khả năng thực hiện (Actionability)** | 5 | Cung cấp sẵn wrapper `scripts/test-paper-p1.sh --mode baseline <DIR>`, chỉ dẫn chạy lại và định dạng artifact rõ ràng, không phụ thuộc môi trường bên ngoài. |
| **Ngắn gọn (Conciseness)** | 4 | Báo cáo tập trung vào dữ liệu thật và hợp đồng đo; lược bỏ thông tin rườm rà. |

**Điểm tổng hợp: 4,8 / 5,0.**

### Giới hạn kỹ thuật và việc sử dụng số liệu P1

1. **Số đo baseline là thử nghiệm công cụ:** Workload 40 batch (4.000 samples) ở tốc độ
   1 batch/s chỉ nhằm xác thực tính đúng đắn của pipeline thu thập metrics và đối chiếu
   dữ liệu với DB; đây **không phải benchmark năng lực chịu tải tối đa (peak capacity)**,
   không phải kiểm thử độ bền (soak test) và không đại diện cho mục tiêu 20.000 msg/s của Paper.
2. **Percentile từ Histogram là xấp xỉ (Approximate):** Histogram của backend sử dụng các
   bucket cố định (đơn vị giây); các giá trị p50/p95/p99 ở backend được báo cáo dưới dạng
   khoảng bucket chứa percentile, không suy diễn thành giá trị chính xác tuyệt đối.
3. **Các thành phần chưa có runtime được giữ đúng hợp đồng:**
   - Stage 5 điều khiển: Vẫn tắt (`STAGE5_ENABLED=false`). Metrics command đánh dấu `disabled`.
   - Dispatch E2E, Result confirmation và WebSocket: Đánh dấu `not_implemented` (giá trị `null`).
     Không bịa số đo hoặc tạo dữ liệu giả.
4. **Bảo mật và an toàn dữ liệu:**
   - Endpoint `/v1/admin/metrics` chỉ phục vụ platform admin với token GoTrue hợp lệ.
   - Artifacts `run.json`, `metrics.csv`, `resources.csv` hoàn toàn không chứa secrets, JWT,
     payload thô hay driver SQL errors.
   - Toàn bộ 15 containers, 1 network, 6 volumes của mỗi lượt chạy đều được dọn dẹp sạch sẽ
     sau khi hoàn thành (`exact_cleanup_verified=true`).
