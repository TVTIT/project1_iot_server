# P8 E4 — tuần tự UNIQUE → WARM REPLAY → tùy chọn COLD REPLAY

**Checkpoint sau sửa validator:** warm subset cache-on đã PASS qua toàn bộ CLI,
kể cả report và server exit 0. Đây chưa phải toàn bộ E4; các kết quả FAIL lịch sử
bên dưới vẫn giữ nguyên.

## Evidence warm gate sau sửa — 2026-10-30

- Root: `/var/tmp/opencode/e4-warm-verified-63qy44cd/`; `gate-result.json` ghi
  từng lệnh CLI, exit code và cleanup. Báo cáo chính:
  `A/fixture/artifacts/p8-e4-on.json` (`status=PASS`,
  `workload_status=WARM_SUBSET_VALIDATED`, `validation.failures=[]`).
- UNIQUE: 20 batch/20 application ACK, một ACK cố ý bỏ qua rồi retry;
  DB tăng 20 ledger / 2.000 raw / 20 temporal / 0 configuration; WS 20 events,
  không drop. REPLAY: 10 replay/10 ACK, DB và WS delta đều 0.
- Replay headers khớp đúng prefix theo `replay_count` trong manifest A;
  journal SHA-256 không đổi giữa hai phase:
  `663cd24ee35616712da592fa256a083fa71de65b63aa13ef2b01d5f39ba4c209`.
  Cache có 10 warm hits/10 repository calls avoided; transaction/COPY/new rows
  trong replay đều 0. DB snapshot/fingerprint equality vẫn được kiểm độc lập.
- `phase_history=["unique","replay"]`, session `CLOSED/VALIDATED`, server exit 0;
  exact cleanup VERIFIED: 12 containers / 1 network / 5 volumes.
- Các lượt `/var/tmp/opencode/e4-validator-parent-7iSBjqhF/` và
  `/var/tmp/opencode/e4-validator-final-j9xo6dcb/` có phase workload validated
  nhưng lỗi tổng kết/exit; giữ là FAIL, không dùng thay bằng chứng gate PASS.
  Sau đó đã sửa phạm vi image metadata của callback và đọc đúng tầng counter
  trong báo cáo warm, có regression riêng.
- Chỉ là pilot cùng host: `generator_colocated=true`, `physical_ab=NOT RUN`,
  `full_p8_accepted=false`; cold restart, durable reopen và conflict vẫn NOT RUN.

Hướng dẫn cho warm subset của E4 với A foreground và B chạy workload. Cold replay
là phase tùy chọn cache-on, bật rõ bằng `--cold-replay`; native qualification
chưa chạy. A giữ quyền
DB/metrics/fixture; B chỉ nhận bundle và credential cần thiết, chạy một telemetry
phase cùng viewer, rồi xuất projection an toàn. Đây không phải P8 acceptance: luôn
giữ `full_p8_accepted=false`; cold restart, durable Gateway reopen và conflicts là
`NOT RUN`; cold replay chỉ được ghi nhận khi chạy chính profile opt-in riêng.

## A: tạo fixture bằng server CLI

Chọn ROOT mới, private remote profile đã cấu hình TLS SAN đúng advertised hostname,
và để nguyên A foreground. Không tái sử dụng thư mục hoặc xoá artifact cũ.

```sh
ROOT=/var/tmp/opencode/p8-e4-<unique-run>
mkdir -m 700 -p "$ROOT"
A="$ROOT/A"   # server tạo A; không mkdir A trước
B="$ROOT/B"
mkdir -m 700 -p "$B"
python3 scripts/paper_p8_server.py serve --root "$A" \
  --network-config /path/to/private-network.json --experiment E4 \
  --cache-mode on --cold-replay --deadline-seconds 900 >"$ROOT/server.log" 2>&1 &
SERVER_PID=$!
```

Từ A2 đọc status tới `preparation_status=E4_PREPARED` và `phase=READY`; lưu
`session_id`/`run_id`. Trạng thái ban đầu không có `collector_ready`; cờ đó chỉ được
đặt sau lệnh `begin` thành công.

```sh
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Chuyển bundle nguyên trạng sang B. Chuyển cả hai binary A đã build cùng fixture;
không build binary khác hoặc sửa digest/provenance. Không thay manifest, viewer identity, TLS inputs,
run-spec hay hashes. Trong bundle hai immutable telemetry manifests `unique` và
`replay` dùng chung tham chiếu journal `outputs/e4-journal.sqlite`; journal là mutable
state do B sở hữu. Mỗi phase có output, runner record, provenance và result export
riêng. Chỉ viewer + đúng một telemetry writer được chọn trong mỗi lệnh `run`.

## B: bind và preflight offline trước workload

Bind vào workspace mới trên B, sau đó kiểm tra cả hai manifest/credential và TLS
trước khi phát bất kỳ traffic nào:

```sh
cp -a "$A/client-bundle" "$B/client-bundle"
cp "$A/fixture/private/bin/paperload" "$B/paperload"
cp "$A/fixture/private/bin/gateway" "$B/gateway"
chmod 700 "$B/paperload" "$B/gateway"
python3 scripts/paper_p8_client.py bind \
  --bundle "$B/client-bundle" --workspace "$B/workspace"
python3 scripts/paper_p8_client.py preflight \
  --workspace "$B/workspace" --binary "$B/paperload" \
  --gateway-binary "$B/gateway"
python3 scripts/paper_p8_client.py check-connection --workspace "$B/workspace"
```

Sau preflight, chuyển trạng thái thủ công theo đúng thứ tự dưới đây. Mỗi `begin`
   phải được A xác nhận `MEASURING` và `collector_ready=true` sau `begin` trước `run`;
   mỗi `finish-phase` chỉ sau export/copy kết quả sang `$A/fixture/private/incoming/<phase>`.

### UNIQUE

Trên A2:

```sh
python3 scripts/paper_p8_server.py begin --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase READY --phase-id unique
```

Đợi `MEASURING` và `collector_ready=true`. Trên B, chỉ chạy phase này:

```sh
python3 scripts/paper_p8_client.py run --workspace "$B/workspace" \
  --binary "$B/paperload" --gateway-binary "$B/gateway" --phase unique
python3 scripts/paper_p8_client.py export-results --workspace "$B/workspace" \
  --output "$B/unique-results" --phase unique
```

Copy nguyên `unique-results` vào `$A/fixture/private/incoming/unique/`. A2:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase MEASURING \
  --phase-id unique --results "$A/fixture/private/incoming/unique"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Chỉ `workload_outcome=VALIDATED`, `phase_history=["unique"]` và import result hợp lệ
mới cho phép chuyển tiếp. Validator kiểm tra phase/session/run/bundle digest và file
inventory; 20 unique application ACK, một ignored-valid-ACK cùng retry; hai viewer
đã subscribe và ghi nhận 20 unique events thật; scoped DB deltas `20 ledger / 2000 raw / 20 temporal /
0 configuration`; raw rows khớp identity samples. Nếu A từ chối hoặc report
`VALIDATION_FAILED`, **không** chạy replay.

### WARM REPLAY

Chỉ sau UNIQUE hợp lệ, operator yêu cầu A mở phase tiếp:

```sh
python3 scripts/paper_p8_server.py next-phase --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase VERIFYING --phase-id replay
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Sau status `READY`, bắt đầu baseline mới cho replay:

```sh
python3 scripts/paper_p8_server.py begin --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase READY --phase-id replay
```

Chờ `MEASURING` rồi B chạy/export replay; B client yêu cầu state UNIQUE validated,
giữ lock workspace và so sánh journal SHA-256/header identity trước-sau. Replay phải
dùng cùng 10 serialized journal entries, không tạo IDs/sequence/timestamp mới.

```sh
python3 scripts/paper_p8_client.py run --workspace "$B/workspace" \
  --binary "$B/paperload" --gateway-binary "$B/gateway" --phase replay
python3 scripts/paper_p8_client.py export-results --workspace "$B/workspace" \
  --output "$B/replay-results" --phase replay
```

Copy `replay-results` vào `$A/fixture/private/incoming/replay/`, rồi A2 xác minh:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase MEASURING \
  --phase-id replay --results "$A/fixture/private/incoming/replay"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

A đối chiếu snapshot DB sau UNIQUE với replay: không có ledger/raw/temporal/config
mới; kiểm ingress duplicate outcomes, cache lookup/repository-avoidance cùng process
window, WebSocket zero-new-event window và viewer đã subscribe nhưng không nhận event
mới. Cuối cùng A2:

```sh
python3 scripts/paper_p8_server.py finalize --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase VERIFYING
```

Đợi `$SERVER_PID` thoát và xác nhận exit code 0. Chỉ report `PAPER_P8_E4_WARM_SUBSET_VALIDATED` khi
phase history đúng, cả hai phase validated, tổng DB delta đúng, cache/WS/raw evidence
đầy đủ, B runner dừng và cleanup chính xác. Artifact `p8-e4-on.json` giữ
`full_p8_accepted=false`, `generator_colocated=true`, `physical_ab=NOT RUN`, cùng
`coldrestart=NOT RUN`, `durable_gateway_reopen=NOT RUN`, `conflicts=NOT RUN`.

### COLD REPLAY (chỉ khi `serve --cold-replay`)

Không thay bundle, journal, credentials hoặc inputs đã bind. Sau khi WARM REPLAY
được import `VALIDATED`, A nhận `next-phase --phase-id cold-replay`; trước khi A
xác nhận `READY`, nó dừng/xóa đúng backend đã đăng ký, khởi chạy backend thay thế
với cùng image/env/mount/HostConfig, reload Nginx, cập nhật resource receipts và
đọc lại process identity khác cùng RAM dedup cache rỗng. Lỗi ở barrier không tạo
READY; giữ session ở trạng thái có thể abort/recover.

B chỉ chạy manifest `cold-replay` cùng viewer, giữ workspace lock và journal cũ;
manifest có run ID/output/provenance riêng nhưng dùng mode `replay` với đúng 10
record đã khai báo. Dùng tên output mới:

```sh
python3 scripts/paper_p8_server.py next-phase --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase VERIFYING --phase-id cold-replay
# Chỉ tiếp tục sau khi status xác nhận READY
python3 scripts/paper_p8_server.py begin --root "$A" \
  --experiment E4 --session-id "$SESSION_ID" --phase READY --phase-id cold-replay
python3 scripts/paper_p8_client.py run --workspace "$B/workspace" \
  --binary "$B/paperload" --gateway-binary "$B/gateway" --phase cold-replay
python3 scripts/paper_p8_client.py export-results --workspace "$B/workspace" \
  --output "$B/cold-replay-results" --phase cold-replay
```

Copy `cold-replay-results` vào `incoming/cold-replay/` rồi dùng `finish-phase`
với `--phase-id cold-replay`. Cold evidence yêu cầu process mới trong cùng một
window, cache miss > 0/hit = 0, duplicate DB transaction/commit > 0 nhưng new
rows/COPY = 0, đúng 10 application ACK, DB fingerprint không đổi và zero replay
WebSocket events. Journal hash/header prefix cũng phải liên tục qua cả ba phase.
Kết quả hợp lệ sẽ là `PAPER_P8_E4_COLD_REPLAY_SUBSET_VALIDATED`, không phải P8
acceptance: `full_p8_accepted=false`, durable reopen/conflicts vẫn `NOT RUN`.
**Native cold replay qualification: NOT RUN.** Cache-off và warm-only vẫn dùng
luồng hai phase cũ; `--cold-replay` với cache-off hoặc experiment khác bị từ chối.

## Abort và recovery

Tùy chọn durable SQLite fault-stop/reopen có quy trình riêng tại
[`paper-p8-e4-durable-reopen.md`](paper-p8-e4-durable-reopen.md).

Lỗi phase, import, kết nối hoặc collector thì giữ cả bundle, B journal, từng phase
outputs/results và A artifacts nguyên trạng; không retry vào workspace dùng rồi,
không sửa state JSON trực tiếp và không khởi động phase kế tiếp. Ghi nhận `status`,
phase error và cleanup report. Dùng recovery/cleanup theo đúng resource receipts
hiện hữu; không dùng `docker compose down`, `prune` hoặc xóa shared resources. Một
lượt retry (nếu còn trong quota) phải dùng ROOT mới, và không vượt 2 GB dữ liệu/cache
gia tăng tổng cộng. Barrier/manual copy là tuần tự; không để hai journal writers chạy
đồng thời.

## Kết quả native warm-gate ngày 2026-10-30 — attempt-03

Attempt duy nhất được cho phép trong lượt này **không đạt `WARM_SUBSET`**. Giữ
nguyên các artifacts phase và lịch sử FAIL; không chạy lại dưới cùng authorization.
Profile mạng chỉ được copy vào scratch riêng, nội dung không ghi vào tài liệu.

- UNIQUE validated: 20 generated/offered, 20 application ACK; một ACK hợp lệ bị
  cố ý bỏ qua rồi retry; pending 0. A-side DB delta `20 ledger / 2000 raw / 20
  temporal / 0 configuration`; raw identity exact. Hai viewer COMPLETE, subscribed
  và nhận 20 event; A-side WS delta 20 submissions/new/enqueued/written, 0 drops.
- Replay thực thi đủ 10 offered/10 replay application ACK; journal SHA-256 ở cả
  hai phase là `794b23ece5dff9b983c4097ab0eca758146e441bd2acad10cea79b8bb3b2a7b7`.
  Replay DB delta `0/0/0/0`; ingress có 10 duplicate outcomes và 10 ACK; cache có
  10 warm lookup hits, 10 repository calls avoided, 0 commits/copy/new rows. Hai
  viewer COMPLETE và subscribed; 0 receiving/unique events. WS delta toàn số 0.
- A từ chối replay với `replay_unique_snapshot_mismatch`: guard hiện so sánh toàn
  bộ 10 replay headers với toàn bộ 20 UNIQUE headers, trong khi 10 replay headers
  khớp chính xác 10 header đầu của UNIQUE. Đây là lỗi validation, không phải bằng
  chứng chấp nhận warm subset. Replay phase chưa được ghi VALIDATED và final
  `p8-e4-on.json` vẫn `FAIL`; không tuyên bố E4/P8 acceptance.
- Đã gửi lệnh abort qua server protocol sau lỗi; session đóng `ABORTED`, cleanup
  verified: 12 containers / 1 network / 5 volumes. `full_p8_accepted=false`;
  physical A/B, cold restart, durable reopen và conflicts vẫn `NOT RUN`.
- Supplemental report: `/var/tmp/opencode/e4-warm-slice-20261030/attempt-03.d0qtmX/attempt-result.json`.
  Artifacts gốc: `/var/tmp/opencode/e4-warm-slice-20261030/attempt-03.d0qtmX/A/fixture/artifacts/`;
  phase results và logs nằm cùng attempt root. `attempt-02` và artifacts lịch sử
  không bị sửa.

## Xử lý validator và lần thử sau sửa — 2026-10-30

- Sửa guard replay để lấy `replay_count` từ manifest replay đã được A chuẩn bị và
  so sánh chính xác `UNIQUE[:replay_count]` với toàn bộ replay headers. Thiếu,
  sai kiểu, vượt giới hạn, sai thứ tự/số lượng/identity đều bị từ chối. Kiểm tra
  SHA-256 continuity của journal và so sánh snapshot DB độc lập vẫn giữ nguyên.
- Regression offline qua `finish_remote_phase` xác nhận prefix 10/20 hợp lệ; journal
  hash đổi và DB snapshot/fingerprint đổi vẫn thất bại. Dữ liệu attempt-03 đã lưu
  cũng qua được kiểm tra prefix mới (`header_prefix_identity=true`); artifacts và
  kết quả FAIL lịch sử không được sửa hoặc đổi thành PASS.
- Wrapper run tại
  `/var/tmp/opencode/e4-warm-fixed-20261030/attempt-01/A/fixture/artifacts/` bị
  operator abort trước `begin`: lần chạy đã nhầm lẫn việc `collector_ready` vắng ở
  trạng thái ban đầu READY là lỗi; theo lifecycle cờ này chỉ xuất hiện SAU begin.
  Artifact `FAIL` / `workload_status=NOT_RUN`, cleanup exact `12/1/5` verified.
- Fresh authorized run tại
  `/var/tmp/opencode/e4-warm-fixed-20261030/attempt-02/A/fixture/artifacts/` đạt
  `E4_PREPARED`/`READY`, bind B thành công, nhưng B preflight dừng với
  `PaperloadInputError: input file does not exist`: orchestration chưa chuyển hai
  binary A sang các đường dẫn B đã truyền cho preflight. Đã abort qua protocol;
  artifact là `FAIL` / `workload_status=NOT_RUN`, cleanup exact `12/1/5` verified.
  Không có workload hoặc WARM gate PASS. Binary A vẫn ở
  `A/fixture/private/bin/{paperload,gateway}`; guide này đã được sửa để mô tả việc
  chuyển nguyên binary và bundle sang B trước preflight.
- E4 WARM vẫn chưa được native-validated sau sửa. Phải giữ
  `full_p8_accepted=false`, `generator_colocated=true`, `physical_ab=NOT RUN`; cold
  restart, durable Gateway reopen và conflicts tiếp tục `NOT RUN`.

## E4 CACHE-OFF unique/replay gate — 2026-10-30

### Cách đọc gate cache-off

- Cache-off không yêu cầu RAM hit. Bằng chứng thay thế phải là các counter đo độc
  lập trong replay process window: `begin_attempts > 0`,
  `confirmed_commits > 0`, `confirmed_new_rows = 0`, `copy_calls = 0`; DB snapshot
  và fingerprint sau UNIQUE/replay phải bằng nhau. `DuplicateCommitted` ở ingress
  chỉ là kết quả ACK/ingress, không tự nó chứng minh DB đã xử lý duplicate.
- Replay vẫn phải dùng cùng journal và prefix 10 record khai báo trong replay
  manifest, đủ 10 application ACK, không tạo WS event mới. `coldrestart=NOT
  APPLICABLE`; durable reopen và conflicts vẫn `NOT RUN`, không gán full E4/P8.
- Lệnh run dùng cho evidence này là server foreground
  `scripts/paper_p8_server.py serve --experiment E4 --cache-mode off`, sau đó
  tuần tự `begin unique` → B `run/export unique` → A `finish-phase unique` →
  `next-phase replay` → `begin replay` → B `run/export replay` → A
  `finish-phase replay` → `finalize`. Trước mỗi B run phải chờ A xác nhận
  `MEASURING` và `collector_ready=true`. Binaries cần copy nguyên bản từ A trước
  `preflight`; A được tạo bởi server, không tạo thư mục A trước.

### Native attempt — giữ nguyên FAIL

- Root: `/var/tmp/opencode/e4-cacheoff-native-20261030/`; orchestration là bản
  riêng từ parent `run_gate.py`, chỉ đổi cache mode sang `off` và final report path
  sang `p8-e4-off.json`. Bundle/B binaries được copy, bind, preflight và TLS
  connection check đều exit 0. Cả UNIQUE và REPLAY được import/validate; replay
  dùng cùng journal SHA-256
  `a7161007e06559a2602361e4a7c02c3191c7f4509cf104fba458f80a7e2924e4`.
- UNIQUE: 20 unique / 20 application ACK, có 1 ACK hợp lệ bị cố ý bỏ qua rồi retry;
  DB delta `20 ledger / 2000 raw / 20 temporal / 0 configuration`.
  REPLAY: 10 offered / 10 application ACK, pending 0; replay window ghi nhận
  `10 begin_attempts / 10 confirmed_commits / 10 database_duplicates / 0
  confirmed_new_rows / 0 copy_calls`. RAM cache `enabled=false`, 0 hits và 0
  repository calls avoided là đúng mode, không phải failure. Ingress duplicate
  outcomes là 10. Replay DB delta 0; WS unique window 20 events, replay window
  toàn số 0, không drop.
- Gate vẫn **FAIL**, không sửa lịch sử thành PASS: server exit 1 ở
  `report_validation`, dù phase history `unique,replay`, session cleanup
  `VERIFIED`, và exact cleanup `12 containers / 1 network / 5 volumes`. Nguyên nhân
  là report assembler không đưa proof `database_proof` vào replay counter window;
  validator vì vậy không thể xác nhận fingerprint equality ở report cuối. Lượt
  này không retry. Regression sau attempt đã sửa để giữ proof từ snapshot UNIQUE
  tới replay; sửa source không thay đổi artifact FAIL.
- Evidence chính: `gate-result.json`, `A/server.log`,
  `A/fixture/artifacts/p8-e4-off.json`, `p8-e4-off-{unique,replay}-phase.json`
  và log lệnh protected dưới `logs/`. Giữ `full_p8_accepted=false`,
  `generator_colocated=true`, `physical_ab=NOT RUN`; cache-off `coldrestart=NOT
  APPLICABLE`, durable reopen/conflicts `NOT RUN`.

### Native cache-off gate sau regression/fix report — PASS subset

- Giữ nguyên FAIL lịch sử ở root `e4-cacheoff-native-20261030`; không sửa artifact
  đó thành PASS. Lượt được phép tiếp theo chạy từ script orchestrator copy riêng tại
  `/var/tmp/opencode/e4-cacheoff-final-20261030/run_gate.py` (cache mode `off`, report
  `p8-e4-off.json`), dùng profile mạng đã kiểm tra một lần
  `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt19/network.json`.
- CLI sequence đầy đủ: bind, preflight, TLS connection, begin/run/export/finish
  UNIQUE, next-phase/begin/run/export/finish REPLAY, finalize. Mỗi lệnh exit 0;
  A đóng `CLOSED/VALIDATED`, server exit 0, exact cleanup VERIFIED `12 containers /
  1 network / 5 volumes`. B đã copy cả `paperload` và `gateway` từ binaries A trước
  preflight, với SHA-256 equality được xác nhận. Artifact chính:
  `/var/tmp/opencode/e4-cacheoff-final-20261030/A/fixture/artifacts/p8-e4-off.json`;
  orchestration/protected command logs: cùng root (`gate-result.json`, `logs/`).
- Report `PASS`, `CACHE_OFF_SUBSET_VALIDATED`, marker
  `PAPER_P8_E4_CACHE_OFF_SUBSET`; `full_p8_accepted=false`,
  `generator_colocated=true`, `physical_ab=NOT RUN`. UNIQUE 20/20 application ACK,
  một ignored-valid ACK rồi retry; database delta `20 ledger / 2000 raw / 20
  temporal / 0 configuration`. REPLAY 10 offered/10 application ACK, pending 0;
  database delta 0. Persistence window ghi nhận 10 attempts / 10 confirmed commits /
  10 database duplicates / 0 confirmed new rows / 0 COPY. Cache RAM disabled; 0 hit
  và 0 repository-call-avoided đúng như kỳ vọng.
- Journal SHA-256 cả hai phase bằng
  `1f00e6ec1c318612324a4998b7ac6b4c35677c4ca12919c9776ba64a669b5b5f`;
  `journal_continuity=true`, khai báo prefix replay được validate. DB snapshots và
  fingerprints ở UNIQUE ACK và replay end bằng nhau. WS: UNIQUE 20 event/0 drop;
  REPLAY submissions/new/enqueued/writes/drop đều 0. `coldrestart=NOT APPLICABLE`;
  durable Gateway reopen và conflicts `NOT RUN` — đây chỉ là cache-off unique/replay
  subset, không phải full E4/P8.
- Native source qualification: revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`,
  dirty source fingerprint SHA-256
  `e1a0adab69606fd7629a1a7bfcd1b5517e299e04e6b58692c04066a79ce965ab`; report giữ
  backend/Mosquitto/Gateway image IDs và metadata từ `prepared['image_ids']`.

## Native cold-replay gate — attempt-01 FAIL, regression fixed offline

- Root/artifacts: `/var/tmp/opencode/e4-cold-gate-20261030-01/`; giữ nguyên
  `gate-result.json`, `A/server.log`, `A/fixture/private/session.json`, bundle/workspace,
  incoming results và `logs/`. Parent/orchestrator xác nhận attempt này là FAIL;
  không sửa report lịch sử thành PASS.
- UNIQUE và WARM REPLAY đều được bind/run/export/import thành `VALIDATED`. Cả hai
  phase dùng journal SHA-256
  `213a87de51d82fb4487b67b5d17dc8183f32048df33e762fee3278a5eb9b4c5c`.
  WARM REPLAY: 10/10 application ACK, 10 ingress duplicate outcomes, 10 warm cache
  hits, 10 repository calls avoided, 0 transaction commits/COPY/new rows; DB và WS
  delta đều 0.
- Lệnh `next-phase cold-replay` thực sự chạy barrier thay backend trước READY.
  Runtime settings/image/mount/HostConfig không đổi; backend ID đổi
  `b726ce45b49d03a87334e34573ab4556740d1d85832b4d5267dfae17d2bee73d` →
  `bb75d56f30861acdd2de72a1da1558226dd38f1c2e7b3c766d23e0d7b23655e5`; process
  instance `c5da18bb-8cf1-49b2-b8c6-be205b4ba285` →
  `74d6bd5f-eb67-43c3-aec2-b062138d43b6`; cache entries sau restart = 0.
  Sau cold `begin`, B dừng trước khi phát workload: client eligibility path đọc
  `binding.json` nhưng kỳ vọng `binding.workload.cache_mode`, trường không có trong
  kết quả bind thực. Lỗi chính xác: `E4 cold replay phase is not enabled or not
  next`. Vì vậy cold cache miss/hit, duplicate DB commits, ACK, DB/WS window đều
  **không đo được**; không suy diễn từ WARM REPLAY.
- Lịch sử session: `phase_history=["unique","replay"]`, sau operator abort session
  đóng `CLOSED/ABORTED`; server exit 1. Cleanup receipt-verified: 13 containers /
  1 network / 5 volumes. `full_p8_accepted=false`, durable reopen/conflicts và
  physical A/B vẫn `NOT RUN`. Artifact `p8-e4-on.json` là FAIL; outer summary không
  được dùng để gán cold PASS.
- Regression đã sửa offline: binder giữ nguyên raw `bundle.json` trong
  `runtime/bundle-metadata.json`; client kiểm source-bundle SHA, session và inventory
  đối chiếu rồi mới đọc `workload.cache_mode`. Integration test dùng exporter +
  binder thật và chạy eligibility với phase state `last_completed_phase=replay`;
  tamper, cache-off, thiếu cold manifest và sai phase đều bị từ chối. Cũng đã sửa
  outer `coldrestart` summary để chỉ PASS khi cold report cuối được acceptance
  validate, và preparation metadata lấy phase list/count từ bundle thực (cold: 4
  manifests gồm viewer). Artifact cũ vẫn giữ nguyên `manifest_count=3` dù run-spec
  có 4 jobs; không sửa lịch sử.
- Lượt native duy nhất sau sửa phải dùng ROOT/workspace mới; kết quả được ghi tiếp
  bên dưới. Không resume hoặc chạy lại attempt-01; cache counters không được trừ
  qua process restart.

### Native cold-replay gate sau regression/fix — PASS subset

- Root/artifacts: `/var/tmp/opencode/e4-cold-gate-20261030-02/`; orchestrator copy
  và `gate-result.json` ở root, report A tại
  `A/fixture/artifacts/p8-e4-on.json`, phase exports/logs và cleanup receipts trong
  cùng run tree. Attempt-01 vẫn FAIL nguyên trạng. Lần chạy này tuần tự
  `unique → replay → cold-replay`; mỗi phase `VALIDATED`, session `CLOSED/VALIDATED`,
  server exit 0, cleanup receipt-verified `13 containers / 1 network / 5 volumes`.
- UNIQUE ghi nhận 20 ledger / 2000 raw / 20 temporal / 0 configuration. REPLAY
  cùng journal và prefix 10 records, 10 application ACK, 10 warm cache hits, 0
  transaction commits/COPY/new rows; DB snapshot/fingerprint giữ nguyên. Journal
  SHA-256 ở cả ba phase là
  `c24363aca32f9735a6b83542bf2a4bced620c8807f893ac3ef968b62a6dbe80e`.
- Cold restart được chứng minh bằng backend container ID
  `6ac92918607fe8e041a3fa67d9b4dfe7c91f399afea0710cf93bace069f66aca` →
  `915c14b4d6f0a57eec1b8c233d32a0ba9b5f5b2a89a1fcef6955f08b9ab3d065` và process
  instance `7d92e13d-7317-48c7-96d6-2466460ac6fd` →
  `125b347c-edd6-4966-8bbb-934cf05b618e`. Image sha, environment, mounts,
  HostConfig, metrics readback đều khớp; cache entries trước cold workload = 0.
  Cold workload: 10 cache misses / 0 hits; persistence window 10 begin attempts /
  10 confirmed commits / 0 new rows / 0 COPY. Ingress: 10 processed/accepted,
  `DuplicateCommitted=10`, 10 ACK published; 0 new samples/commits. DB delta
  cold-window `0 ledger / 0 raw / 0 temporal / 0 configuration`; tổng report
  remains `20 / 2000 / 20 / 0`. Hai viewer connected/subscribed, nhận 0 event;
  cold WebSocket submissions/new/enqueued/writes/drops đều 0.
- Final report `status=PASS`, `workload_status=COLD_REPLAY_SUBSET_VALIDATED`,
  marker `PAPER_P8_E4_COLD_REPLAY_SUBSET`, `coldrestart=PASS`,
  `journal_continuity=true`, replay/cold header-prefix validation true. Prepared
  bundle có 4 manifests (unique, replay, cold-replay, viewer), bundle SHA-256
  `9329e451b4304f6235aad27fccb299ee5e7c2df00a264646f7f63662bee730f7`. Source
  revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`, dirty source fingerprint
  `f40534a29f0aecf6d1a6ed91186e8abb2497d715810360d6b7cbe3c11ec17353`.
- Offline targeted E4/P8 suite: 310 tests PASS; `py_compile` và `git diff --check`
  PASS. Không chạy thêm native fixture sau gate này. Giới hạn không đổi:
  `full_p8_accepted=false`, `generator_colocated=true`, physical A/B, durable
  Gateway reopen và conflicts `NOT RUN`; kết quả chỉ xác nhận cold-replay subset,
  không phải full P8 hoặc production acceptance.
