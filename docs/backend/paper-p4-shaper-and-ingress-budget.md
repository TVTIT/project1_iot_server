# P4 — Shaper, admission và ngân sách ingress

## Tóm tắt hiện trạng — 25/10/2026

**P4 hoàn tất ở phạm vi Local MVP trên fixture cô lập; không phải nghiệm thu production.** Baseline `ncs_4e31bc6aa1b1` và optimized `ncs_bd005abcca19` đều PASS (`PAPER_P4_ACCEPTED`, `full_p4_accepted=true`, cleanup exact: 18 containers / 1 network / 7 volumes); P3 regression mới tại cùng revision dirty `a1b2c9f` là `ncs_705f0713b225` PASS (`PAPER_P3_ACCEPTED`, cleanup exact: 17 / 1 / 7). Không ghi đè các lượt FAIL lịch sử ở phần dưới.

P4 chứng minh workload hữu hạn 59 unique batches / 4.010 samples (A: 35/3.500, B: 24/510), 35/24 temporal observations và drain/reopen byte-hash của 4 pending rows. Khi shaper bật, A tiếp tục commit trong burst/backlog hữu hạn của B; đây **không** phải hard fairness, capacity benchmark hay chứng minh tải 10 message/s kéo dài suốt cửa sổ 20,42 giây. Mọi số liệu resource là snapshot thưa, không phải peak. Xem [backlog/giới hạn P4](../dynsec/limitations-and-backlog.md#p4-shaper-và-ingress-budget--local-mvp-hoàn-tất-giới-hạn-kỹ-thuật-25102026).

**Ranh giới:** full/remote CI, board/HIL và live rollout NOT RUN/ngoài phạm vi. Deployment defaults vẫn `STAGE5_ENABLED=false` và `MQTT_SHAPER_ENABLED=false`. P4 không đổi protocol/auth; không tuyên bố 20k msg/s, BIBS, global quota, lock-free hay hard fairness. Dừng ở P4; không tự mở P5/P6/Stage 6.

## P4.0 — Khóa baseline và hợp đồng (ghi nhận lịch sử, 25/10/2026)

Đây là báo cáo khảo sát/hợp đồng đầu vào, không phải nghiệm thu triển khai P4.
Không sửa runtime, config, fixture hay checklist kế hoạch trong bước này.

### Baseline hiện hành và baseline lịch sử

- Đã kiểm tra checkout: branch `feat/paper-requirement`, HEAD `a1b2c9f`
  (`docs(paper-p3): cập nhật báo cáo sensor control loop, hợp đồng mqtt và rà
  soát tài liệu`); Git báo working tree **clean** trước khi thêm báo cáo này.
  Đây là P3 đã commit, khác baseline ghi trong kế hoạch chi tiết P4 (HEAD
  `4c3cdaf9f3f152f64ed930b1a9e67173928b356f`, dirty với P3 chưa commit).
- Master plan giữ baseline khảo sát ban đầu `a5f9f812...` trên `develop`, sạch,
  trước P0–P3; đó là lịch sử, không phải trạng thái source hiện nay. Report P0/P1/P2
  cũng có baseline riêng; kết quả PASS/FAIL/NOT RUN trong các report là evidence
  theo revision/lượt chạy được ghi tại đó, không được chuyển thành P4 evidence.
- P3 report ghi lượt native mới nhất `ncs_64809f03cce5` PASS sau sửa review;
  các lượt cũ, gồm lượt FAIL được lưu lại, không bị ghi đè. P3 hoàn tất local MVP
  trên simulator; board/HIL, remote CI và live rollout vẫn NOT RUN/ngoài phạm vi.
  Giữ deployment `STAGE5_ENABLED=false`.

### Hợp đồng admission

Thứ tự phải giữ trong callback `Ingress.Admit` (`src/internal/mqttingress/ingress.go`):

1. Kiểm tra ingress đã start và chưa stopping (barrier `admitMu`); loại retained,
   payload quá `MaxPayloadBytes`, và topic sai bằng `telemetry.ParseTopic`.
2. Lấy `gateway_id` từ topic đã parse/validate (topic shape hiện là
   `gateways/{gateway_id}/telemetry/{sensor_id}`), không lấy từ JSON hay header.
   Các Sensor cùng Gateway dùng chung token bucket; bucket chỉ điều tiết delivery,
   không cấp quyền, xác nhận provisioning, hay thay kiểm tra worker/repository.
3. Với cheap checks hợp lệ, tính một delivery attempt và debit đúng một token
   per-Gateway. Đơn vị `r` là attempts/second và `b` là burst attempts/Gateway;
   retry và duplicate cũng tiêu token. Không phải sample/s hoặc unique commits/s.
   Nếu token thiếu hoặc bucket state capacity hết thì reject.
4. Reserve `len(payload)` trong ngân sách raw buffered payload đồng bộ, trước
   `copy`; nếu không đủ thì reject. Sau đó copy payload và thử enqueue channel
   không chờ. Queue đầy thì release reservation ngay. Token đã debit không refund
   khi byte reserve hoặc enqueue thất bại.
5. Worker xử lý theo thứ tự đang có: `Validator.Validate` →
   `Repository.Persist` (transaction/commit) → chỉ khi commit được xác nhận,
   identity khớp, mới publish application commit ACK. Duplicate đã commit có thể
   được ACK lại theo hợp đồng hiện hữu; lỗi validate/persist/commit chưa xác nhận,
   identity mismatch hoặc ACK publish lỗi thì không được báo application ACK.

Các từ chối admission (lifecycle, retained, size/topic, rate/state, byte budget,
queue) không enqueue, không ghi DB và không phát application ACK. Paho/MQTT PUBACK,
nếu xảy ra, chỉ là transport receipt; không biến reject thành application ACK.
Nguồn hiện tại ghi rõ `AdmissionDecision` và Gateway durable outbox retry tại
`ingress.go`; không đổi semantics này.

### Vòng đời byte reservation và quan sát

Reservation đại diện **payload bytes được ingress copy và còn được giữ**, bắt đầu
trước copy, tồn tại qua callback/enqueue, nằm trong queue hoặc trong một worker,
và chỉ release khi worker đã xử lý xong (hoặc queue refusal / shutdown path bỏ item).
Không release khi dequeue: worker còn giữ payload. Admission stop là barrier,
`Drain` ngưng nhận rồi để worker drain trong shutdown context; mọi return path cần
release reservation đúng một lần, kể cả validation/persistence/unconfirmed commit/
ACK failure và cancellation. Cơ chế reservation này chưa được triển khai.

Đây không phải queue depth, cũng không phải RSS: queue depth hiện loại trừ items
worker đã lấy; payload-byte budget không tính topic/metadata, decoded samples,
Paho/network buffers, shaper state, allocator/runtime overhead hoặc broker memory.
Snapshot nhiều atomic reads không phải transaction nhất quán. Hôm nay
`Snapshot`/`counters.go` có queue depth/capacity/high-water, in-flight và byte
counters `ReceivedBytes`/`AcceptedBytes`, nhưng chưa có active buffered payload
bytes hoặc shaper metrics. `MetricsEnabled` chỉ bật/tắt instrumentation mới;
enforcement và accounting an toàn phải chạy dù exporter tắt. Exporter chỉ là reader,
không là điểm thực thi giới hạn.

### Đề xuất P4.1–P4.4 (chưa triển khai)

Tên cấu hình đề xuất, theo env conventions hiện tại (`internal/config/ingress.go`)
và cần được chốt/validate khi bắt đầu P4.1:

| Biến | Đề xuất khởi đầu / giới hạn hỗ trợ |
| --- | --- |
| `MQTT_SHAPER_ENABLED` | false mặc định, độc lập `METRICS_ENABLED` |
| `MQTT_SHAPER_RATE_PER_SECOND` | attempts/s/Gateway; hữu hạn, đề xuất range `[1, 10000]`, default thử nghiệm `2` |
| `MQTT_SHAPER_BURST` | attempts/Gateway; `[1, 100000]`, default thử nghiệm `4` |
| `MQTT_SHAPER_MAX_GATEWAYS` | entry toàn cục qua mọi shard; `[1, 100000]`, ứng viên default `4096` |
| `MQTT_SHAPER_IDLE_RETENTION` | khoảng idle hữu hạn, `[1m, 24h]`, ứng viên default `10m` |
| `MQTT_INGRESS_BUFFERED_BYTES` | payload-held byte cap, `[65536, 256 MiB]`, default `16 MiB` |

Các con số rate/state là giới hạn thiết kế đề xuất, không phải capacity đã đo.
Profile smoke có thể thử `r=2`, `b=4` khi Gateway bình thường phát batch 1/s;
không dùng làm khuyến nghị phổ quát. Giữ queue 256, max payload 65.536 bytes,
4 workers, DB pool reservation và guard hiện có `capacity × max payload <= 256 MiB`.
Giới hạn byte 16 MiB là raw payload-held copy budget, không RSS cap. Bất kỳ config
không hữu hạn/overflow/cross-field sai nào phải fail startup; khi shaper tắt byte
và count bounds vẫn có hiệu lực.

Sharder đề xuất: 64 shard mutex, hash `gateway_id`, max entry là tổng toàn shard,
không nhân giới hạn 64 lần. Cleanup theo batch scan có quota nhỏ mỗi lượt/tick,
không scan toàn state trên mỗi callback; chỉ evict entry idle đủ lâu để refill đã
đạt `b`, để tạo lại bucket không cấp burst thêm. Nếu đầy thì reject key mới, không
evict bucket hoạt động/reset. Dùng monotonic elapsed time; restart reset state RAM,
không tuyên bố quota phân tán.

| Lát cắt | File/điểm vào dự kiến |
| --- | --- |
| P4.1 config | `src/internal/config/ingress.go` (`IngressConfig`, `loadIngress`, bounds/guard) và `ingress_test.go`; sau đó mapping `.env.example`, `docker-compose.yml`/profile fixture |
| P4.2 shaper | `src/internal/mqttingress/ingress.go` (`AdmissionDecision`, `Options`, `New`, `Admit`), có thể file shaper cùng package; fixed outcomes/counters tại `counters.go` và tests cùng package |
| P4.3 byte lifetime | item/ownership `ingress.go`; `worker`, `drainQueue`, `process`, `Drain`, `StopAdmission`; kiểm tra release một lần ở mọi nhánh, snapshot byte current/high-water/cap |
| P4.4 wiring/metrics | `src/cmd/server/ingress.go` (`composeIngress`, summary) wire options; `src/cmd/server/metrics.go` export snapshot hiện hữu; exporter tắt không làm tắt guard |

Runner/surface khảo sát được: `scripts/tests/stage3_mqtt_ingress_runner.py` là native
TLS/Mosquitto ingress smoke; `scripts/tests/native_compose_smoke.py` cung cấp
Compose lifecycle/preflight, còn P1 report xác định metrics CSV/JSON và sampler
là surfaces để reuse. Những fixture/exporter này **chưa được sửa hoặc chạy cho P4**.
Broker root config/hiệu lực fixture, inflight/queued limits, workload hai Gateway,
CPU/RAM và retry/drain P4 đều **NOT RUN** trong lát cắt này; shaper Go không bảo vệ
phần tải đã tới broker trước callback.

### Trạng thái tại lát cắt P4.0 (lịch sử, không phải trạng thái hiện hành)

- Đã đọc: P4 detail plan; master plan sections P4/P5/P6; tóm tắt/report P0/P1/P2/P3;
  ingress core/counters, transport interface, config, server composition/metrics,
  và native ingress/Compose fixture entrypoints.
- Chỉ tạo tài liệu P4.0. P4.1–P4.8, code/config/tests, broker review, native workload,
  integration tests, benchmark, HIL và rollout: **NOT RUN / chưa triển khai**.
- Không chạy integration theo chính sách docs-only. `git diff --check` là kiểm tra
  duy nhất sau thay đổi tài liệu.

## P4.5 — Broker budget review và fixture compatibility (bounded review; lịch sử trước native acceptance)

### Pin, defaults được tài liệu hóa và cấu hình đang đặt

- `docker-compose.yml`, `scripts/tests/stage2_mosquitto_runtime.py`,
  `scripts/tests/stage3_mqtt_ingress_runner.py` và fixture P0/P1 cùng pin image
  `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`.
  Đọc `mosquitto -h` từ chính image bằng một disposable invocation, không kết nối
  mạng: image báo **Mosquitto 2.0.18**. Digest là định danh thực; source comment
  cũng đã được sửa để tránh dùng nó thay cho kiểm tra image.
- Tài liệu tham chiếu cùng version: [Mosquitto v2.0.18
  mosquitto.conf man page (upstream XML)](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.18/man/mosquitto.conf.5.xml).
  Defaults được tài liệu hóa cho 2.0.18: `max_packet_size` không có giới hạn,
  `message_size_limit` 0 (không giới hạn), `max_inflight_messages` 20,
  `max_inflight_bytes` 0 (không giới hạn), `max_queued_messages` 1000/client,
  `max_queued_bytes` 0 (không giới hạn). Đây là defaults trong tài liệu, không
  phải runtime readback của fixture.
- Review quyết định đặt các bound explicit trong source config dùng chung bởi
  runner (runner private config là copy byte-for-byte): `max_packet_size=2097152`,
  `message_size_limit=1048576`, `max_inflight_messages=20`,
  `max_inflight_bytes=1048576`, `max_queued_messages=256`,
  `max_queued_bytes=16777216`. Packet cap vẫn lớn hơn payload cap tối đa 1 MiB,
  chừa tối thiểu 1 KiB cho topic/header/properties; app ingress mặc định 64 KiB.
  Stage 5 control payload cap vẫn 8192 bytes. TLS, DynSec, `allow_anonymous false`,
  persistence và ACL không đổi.
- `max_inflight_*` và `max_queued_*` giới hạn **mỗi client** (cấu hình option có
  hiệu lực toàn broker). Queue là QoS 1/2 outgoing messages phía trên inflight;
  khi count hoặc byte limit đạt trước, Mosquitto có thể drop deliveries tiếp theo.
  Chúng không tạo global memory ceiling: nhiều client nhân rộng ngân sách; giới
  hạn bytes không chặn tuyệt đối một message đơn vượt ngưỡng (message đó có thể
  được gửi một mình). Không đặt `memory_limit` vì khả năng build-time memory
  tracking và tác động drop/disconnect cần acceptance riêng.

### Fixture defaults và memory/backpressure boundary

- Fixture native Compose dùng synthetic, cố định và chỉ local defaults cho sáu
  interpolation P4: `MQTT_SHAPER_ENABLED=false`, rate `2`, burst `4`,
  `MQTT_SHAPER_MAX_GATEWAYS=4096`, idle retention `10m`, và
  `MQTT_INGRESS_BUFFERED_BYTES=16777216`. Giá trị không lấy từ shell/deployment
  `.env`; strict `require_inputs` vẫn fail-closed nếu xuất hiện interpolation mới.
- P3 ingress queue giữ tối đa 256 item với payload trần 65.536 bytes, tương ứng
  tối đa 16 MiB payload thô nếu đầy; P4 byte budget khởi đầu cũng 16 MiB và tính
  payload copy được admission giữ qua queue/worker, không tính metadata/topic,
  decoded samples, Paho/network buffers, worker-local allocation, shaper map/key,
  mutex, allocator hay Go runtime. Shaper state được giới hạn entry/retention nhưng
  có overhead allocator; không có RSS cap hoặc hard fairness/capacity claim.
- Backend shaper chỉ chạy sau khi broker/Paho đã nhận và có thể đã cấp phát bản tin.
  Vì vậy nó không bound network ingress, broker buffers, parser allocations trước
  admission hoặc aggregate broker RAM. Broker limits ở trên giới hạn một phần
  outgoing per-client delivery buffering, không phải inbound queue/global RAM.
- Gateway simulator lưu telemetry outbox bền vững cho tới application commit ACK;
  broker PUBACK chỉ xác nhận transport và không xóa outbox. Gateway retry theo
  backoff 1–30 giây trong thời hạn hiệu lực của message/command; config hiện tại
  mặc định giới hạn publish inflight ở 8, SQLite outbox ở 5.000 rows/64 MiB.
  Các bound Gateway này độc lập giới hạn queue/inflight Mosquitto per-client.
  Broker có thể
  drop delivery vượt per-client limits; Gateway durable outbox/retry là cơ chế
  recovery, không phải guarantee rằng mỗi publish được backend enqueue/commit.
  P3 control receipts có quota riêng (256 entries, 4 MiB business bytes) và
  retention tối thiểu 24 giờ; không tăng quota/workers/pool trong review này.
- Đây là phần review/config của giai đoạn trước khi bắt đầu lượt native P4; không
  được hiểu là runtime readback/acceptance của workload sau này. Các assertion
  trong fixture xác minh profile config được tạo và effective environment backend,
  nhưng không chứng minh broker memory/global ceiling hay backpressure vượt tải.

## P4.8 — Bàn giao triển khai và evidence cuối (25/10/2026)

P4.0 và P4.5 phía trên là các lát cắt lịch sử; những câu NOT RUN trong đó không mô tả trạng thái cuối. Phần này ghi evidence cuối được lưu an toàn, không đổi nhãn những lượt FAIL.

### Cấu hình runtime và hợp đồng admission thực tế

`src/internal/config/ingress.go` đọc process environment. Cấu hình shaper/buffer
được parse/validate cả khi ingress/shaper tắt; giá trị hỏng fail-fast. Giá trị
hiện hành:

| Biến | Default | Miền hợp lệ | Ý nghĩa |
| --- | --- | --- | --- |
| `MQTT_SHAPER_ENABLED` | `false` | boolean | Opt-in, không phụ thuộc metrics |
| `MQTT_SHAPER_RATE_PER_SECOND` | `2` | 1–10.000 | Token refill attempts/s/Gateway |
| `MQTT_SHAPER_BURST` | `4` | 1–100.000 | Burst attempts/Gateway |
| `MQTT_SHAPER_MAX_GATEWAYS` | `4096` | 1–100.000 | Tổng entry logic qua mọi shard |
| `MQTT_SHAPER_IDLE_RETENTION` | `10m` | 1m–24h | Ngưỡng idle eviction |
| `MQTT_INGRESS_BUFFERED_BYTES` | `16.777.216` | max(`MQTT_MAX_PAYLOAD_BYTES`), 256 MiB | Payload bytes đang được ingress giữ; không phải RSS |

Các giới hạn queue hiện có vẫn là `MQTT_QUEUE_CAPACITY=256`,
`MQTT_MAX_PAYLOAD_BYTES=65536`, `MQTT_WORKER_COUNT=4`; phép nhân queue × payload
vẫn có guard overflow và trần 256 MiB. Smoke profile dùng DB pool max 10, backend
quota 2 CPU/4 GiB, `STAGE5_ENABLED=false`, `METRICS_ENABLED=true`.

Trong `Ingress.Admit`, thứ tự thật là lifecycle/stopping → retained → payload
size → parse/validate topic → lấy Gateway ID từ parsed topic → token-bucket
attempt admission → reserve payload bytes → copy payload → nonblocking enqueue.
Shaper debit bị giữ nguyên nếu byte reserve thất bại hoặc queue đầy (không refund).
Queue full giải phóng byte reservation; mọi admission reject không enqueue, không
ghi DB và không phát application ACK. `Received`/`ShaperPassed`/`Accepted`/commit
metrics là các giai đoạn khác nhau, không thể thay thế nhau.

Shaper dùng 64 shard với mutex; `MQTT_SHAPER_MAX_GATEWAYS` là global logical cap,
không nhân theo shard. Mỗi Allow kiểm tối đa 8 slot trong một shard cleanup turn;
mỗi lượt cleanup chọn một shard theo round-robin, không quét toàn bộ shard/state
trên mỗi callback. Entry chỉ bị evict khi retention đã qua **và**
bucket đã refill đầy đủ tới burst, nên recreate không tạo thêm token so với refill
bình thường. State capacity đầy thì Gateway mới bị từ chối, active bucket không bị
đuổi để nhường chỗ. Enforcement và byte accounting không bị metrics exporter tắt;
metrics off chỉ giảm observability, không bỏ shaper hay safety bounds. Metrics-off
enforcement là implementation contract; hai native workload P4 chạy metrics-on,
còn P3 regression riêng trong lượt P4 mới có một command metrics-off.

Byte accounting reserve `len(payload)` trước `append` copy; reservation đi cùng
item trong channel và worker, không giải phóng khi dequeue, mà sau khi worker xử lý
xong (gồm persist và nhánh publish application ACK). Drain chờ worker kết thúc;
queue refusal/shutdown cleanup giải phóng reservation tương ứng đúng một lần. Đây là
logical budget cho payload copy đang ingress-owned, không phải memory/RSS cap.
Go maps và slice backing arrays có thể giữ allocator capacity lịch sử dù entry đã
bị xóa/length giảm; decoded samples, metadata, Paho/network buffers, worker-local
allocations, shaper state và runtime allocator overhead đều nằm ngoài cap.

### Mosquitto limits: config, defaults và phạm vi khẳng định

Fixture ghim Mosquitto **2.0.18** image
`eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`.
Source config, được copy vào fixture, đặt explicit:

| Option | Giá trị fixture | Phạm vi |
| --- | ---: | --- |
| `max_packet_size` | 2.097.152 bytes | inbound packet cap |
| `message_size_limit` | 1.048.576 bytes | payload cap |
| `max_inflight_messages` | 20 | outgoing QoS 1/2 in-flight per client |
| `max_inflight_bytes` | 1.048.576 bytes | outgoing in-flight byte limit per client |
| `max_queued_messages` | 256 | outgoing queued messages per client |
| `max_queued_bytes` | 16.777.216 bytes | outgoing queued bytes per client |

`max_packet_size` cao hơn `message_size_limit` 1 KiB để có chỗ cho topic/header;
ingress app profile vẫn 64 KiB. 2.0.18 documentation defaults (`max_packet_size`
unlimited, `message_size_limit=0`, inflight 20/0 bytes, queued 1000/0 bytes) khác
các explicit values phía trên. Fixture guard kiểm tra các giá trị trong config
rendered/copy và backend effective environment; đó **không** phải broker runtime
readback assertion cho từng option hay bằng chứng global broker memory ceiling.
In-flight/queued limits scoped per client, không nhân thành global cap. Cả backend
shaper lẫn các egress queue limits không ngăn broker/client library đã nhận dữ liệu
hoặc allocate trước lúc Go callback admission được gọi.

### Wrapper và workload so sánh hữu hạn

Wrapper syntax:

```sh
sh scripts/test-paper-p4.sh baseline [NEW_RUN_DIRECTORY]
sh scripts/test-paper-p4.sh optimized [NEW_RUN_DIRECTORY]
```

`baseline` đặt `MQTT_SHAPER_ENABLED=false`; `optimized` đặt true. Cả hai giữ chung
rate 2, burst 4, state cap 4096, retention 10m, byte budget 16 MiB, queue 256,
max payload 64 KiB, workers 4, DB pool 10, 2 CPU/4 GiB, Stage 5 off, metrics on,
Auth/HTTPS/TLS/ACL/persistence và broker profile. P4 optimization thật trong
`effective_optimizations` là `mqtt_shaper` (empty list ở baseline), không
được ghi giả là P5/P6 hay metrics optimization.

Offered workload hai Gateway có 59 unique batches / 4.010 samples:

| Giai đoạn | Gateway A | Gateway B | Ghi chú |
| --- | --- | --- | --- |
| Steady | 35 batches × 100 samples | 3 batches × 100 samples | A 1 batch/s; B steady trước burst |
| Burst | A tiếp tục 1 batch/s | 20 batches × 10 samples | B offered 10 batches/s trong khoảng phát hữu hạn; completion deadline 4s ở optimized |
| Drain/reopen | Hoàn tất A | 1 batch × 10 samples và replay outbox sau reopen | deadline drain 80s; pending payload hash được đối chiếu sau reopen |

Toàn lượt có deadline 90s; A completion timeout là 75s. Workload hữu hạn này không
phải capacity benchmark. Fixture so sánh A progress/application ACK trong lúc B
burst, B rejection/pending outbox, drain, unique ledger/raw/temporal counts,
duplicate replay, retry payload hash, auth/namespace denial, CPU/RAM snapshots.
Copy DB/SQLite pending state chỉ sau khi B container đã stopped; SHA-256 payload
đang pending được so với `processed_messages.payload_hash` sau retry/commit. Wire
bytes không được exporter đo (`null`/unavailable); payload bytes tách biệt với
MQTT wire bytes. Resource snapshots sparse, không biểu diễn peak/continuous profile.

### Kết quả so sánh native cuối và khôi phục

| Chỉ số | Shaper off — `ncs_4e31bc6aa1b1` | Shaper on — `ncs_bd005abcca19` |
| --- | ---: | ---: |
| Backend delivery attempts / admitted | 60 / 60 | 81 / 60 |
| New commits / duplicate / rate rejects / retries | 59 / 1 / 0 / 0 | 59 / 1 / 21 / 21 |
| Gateway publish attempts | 59 | 80 |
| Gateway A commit tiến triển trong burst/backlog | tham chiếu workload hữu hạn | 15 → 35 commits trong 20,42s |
| Gateway B unique batches / samples | 24 / 510 | 24 / 510 |
| Tổng unique / samples; temporal | 59 / 4.010; A 35, B 24 | 59 / 4.010; A 35, B 24 |
| Payload bytes backend ghi nhận | 50.114 | 58.731 |
| Cuối phát sinh / reopen | — | 4 pending rows / 1.642 bytes; 4/4 SHA-256 khớp DB sau reopen; sau drain 0 pending |
| Buffered payload gauge cuối; shaper entries / cap | — | 0 bytes; 2 / 4.096 |
| Cleanup | exact: 18 / 1 / 7 | exact: 18 / 1 / 7 |

Baseline 60 delivery attempts gồm 59 commit mới + 1 duplicate và 60 ACK; Gateway publish attempts 59, không retry. Optimized 81 backend attempts gồm 60 admitted + 21 rate rejection/retry; 59 new commit + 1 duplicate; 60 ACK. Gateway có 80 publish attempts; chênh lệch so với backend attempt giải thích bởi manual dedup replay (không cộng vào backend received). `backend_received` là callback-observed, **không phải** số broker ingress/received; broker-received và wire bytes không khả dụng. Payload-byte counts không phải wire traffic. Shaper pass không đồng nghĩa enqueue accepted/commit. Không rút ra throughput/hard performance claim.

Burst window ghi A tiến từ commit 15 tới 35 trong 20,42s trong khi B phát một đợt hữu hạn và sau đó còn backlog/ACK chờ; không có nghĩa B liên tục gửi 10 batches/s suốt toàn bộ 20,42s. CPU/RAM chỉ có sparse snapshots, không có peak/continuous measurement. Fixture quota backend 2 CPU / 4 GiB; các container dịch vụ khác không có quota riêng.

P3 regression mới tại revision dirty này: `ncs_705f0713b225` PASS (`PAPER_P3_ACCEPTED`, `full_p3_accepted=true`), 6 commands (5 metrics-on + 1 metrics-off), cleanup exact 17 containers / 1 network / 7 volumes; DB delta 2 configuration, 14 ledgers, 666 raw samples, 14 temporal.

Native PTY/readiness hardening dùng bounded PTY readiness handshake, nhập credential qua stdin sau khi tắt echo, private capture và observer client ID riêng; không sửa ACL. Namespace denial xác định từ bản ghi broker `Denied PUBLISH` mới, không dựa riêng exit status client QoS 1. Không có thay đổi MQTT protocol/auth.

### Verification, lịch sử native và trạng thái

- Go evidence trong `/var/tmp/opencode/paper-p4/verification-subagent-20261025-155114/`:
  `go test -race` trên 7 affected packages (`internal/mqttingress`,
  `internal/config`, `cmd/server`, `internal/gatewaysim`, `cmd/gateway`,
  `internal/mqttcontrol`, `internal/metrics`) PASS; gofmt clean; `go vet ./...`
  PASS; golangci-lint v2.14.0 0 issues; `go build ./...` PASS; ARMv7 gateway
  cross-build PASS. Đây là evidence Go, không phải native acceptance.
- Python cuối: lệnh `PYTHONPATH=scripts/tests python3 -m unittest scripts.tests.test_native_compose_smoke scripts.tests.test_paper_p4_support scripts.tests.test_paper_p3_support scripts.tests.test_ci_feature_jobs scripts.tests.test_ci_smoke_jobs` — 228 tests PASS; log `/var/tmp/opencode/paper-p4/python-guards.log`. Lượt gọi đầu thiếu `PYTHONPATH` gặp `ModuleNotFoundError`; đây là lỗi invocation, không phải native FAIL.
- Go evidence ở trên; không có coverage gate hay tổng unique Go test count được báo.
- Preserve native failures: `ncs_27ec3b2e6892` subscription failure trước workload
  (cleanup 12 containers / 1 network / 7 volumes); `ncs_8f62d49d6082` vô tình là
  P1, không tính P4 (cleanup 9 / 1 / 4); `ncs_15799eee3f3f` PTY/stdin rejection
  (cleanup 12 / 1 / 7); `ncs_02d8ffc9dc62` và `ncs_f2b9899b8c54` failure chưa phân
  loại (mỗi lượt cleanup 15 / 1 / 7). Cleanup counts theo thứ tự containers /
  networks / volumes. Không lượt nào được đổi nhãn thành PASS.
- `ncs_ce97cc3709fd` đạt 59/4.010 và drain nhưng FAIL provenance do SQL thừa `)`; cleanup exact 18/1/7. SQL đã sửa trước final baseline PASS. Lượt structural `ValueError` sau burst liên quan observer tuple-unpack/fixture mismatch; root cause không được khẳng định vượt phần đã xác định. Các lượt prepare thất bại không tạo resources được ghi riêng là prepare evidence, không gán cleanup PASS. Các lượt FAIL khác đã liệt kê ở trên vẫn FAIL.
- Final native P4 off/on và P3 regression đều PASS ở revision source `a1b2c9f24fd7f240bbbc214270b806d2cc4ad94b`, dirty; artifacts ở `/var/tmp/opencode/paper-p4/{baseline-final-20261025-162257-1037638,optimized-shaper-on-20261025-162609-1048147,p3-regression-20261025-163046-1059962}/artifacts/`.
- Full/remote CI, board/HIL và live rollout NOT RUN/ngoài phạm vi. P4 Local MVP đã hoàn tất; không mở phase kế tiếp tự động.

### Lệnh chạy lại an toàn

Chạy từ repo root. `NEW_RUN_DIRECTORY` phải **chưa tồn tại** khi gọi; log phải nằm ngoài directory đó. Ví dụ dùng tên mới mỗi lượt:

```sh
run_dir="/var/tmp/opencode/paper-p4/my-baseline-$(date +%Y%m%d-%H%M%S)"
log="/var/tmp/opencode/paper-p4/my-baseline-$(date +%Y%m%d-%H%M%S).log"
test ! -e "$run_dir" && sh scripts/test-paper-p4.sh baseline "$run_dir" >"$log" 2>&1
```

Đổi `baseline` thành `optimized` để bật `mqtt_shaper`; không dùng deployment `.env`/credentials cho fixture. Lệnh trên là ví dụ cú pháp, không phải lần chạy trong handoff này.

Tiếp theo P4, lát cắt P5a Local MVP đã hoàn tất. Runner dùng wrapper riêng `scripts/test-paper-p5a.sh`; bốn profile off/on × 100/1 sample, shutdown smoke, cách tạo run directory mới/log ngoài và rollback `off` được hướng dẫn trong [báo cáo P5a](paper-p5a-microbatch-persistence.md). P5a không thay đổi các lệnh/flags P4 ở trên.

**Kết luận:** P4 Local MVP hoàn tất, nhưng finite smoke không chứng minh capacity/soak, fairness, RSS cap hay global broker memory. Giữ các limitation trong backlog. Dừng ở P4; không tự triển khai P5/P6/Stage 6, không rollout.
