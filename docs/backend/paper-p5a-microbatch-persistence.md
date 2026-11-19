# P5a — Microbatch persistence giữ nguyên raw và commit ACK

**Trạng thái cuối (25/10/2026): P5a Local MVP hoàn tất trên fixture cô lập.** Các acceptance dưới đây được đánh giá theo evidence có liên kết; implementation/tests hiện vẫn là dirty worktree, không phải clean commit hoặc production certification. Không chạy lại native stacks cho lần đồng bộ tài liệu này.

## Kết quả cuối và lối vào evidence

- Bốn workload full schema-23/Auth/RS256/HTTPS/MQTT TLS/DynSec-ACL đều PASS, 24 messages/profile: 100 samples/message giữ 2.400 raw samples; 1 sample/message giữ 24. Cả bốn có 24 ledger, 24 temporal observations, 24 ACK, số raw samples đúng profile, outbox 0 và exact cleanup 16 containers / 1 network / 7 volumes. Runtime group nhiều member đã được quan sát ở cả hai profile.
- Source qualification của bốn workload và control regression: branch `feat/paper-requirement`, baseline revision `3441d997976782ac8b9d68c9c47ee9ff6c3632b5`, `dirty=true`; artifacts ghi image IDs/build thực tế. Workload timings predates narrow final counters fix ở P5a.9a; không gán chúng cho source sau fix.
- Control regression xác nhận command/result thật và cấu hình `reported.configuration`/desired state được bảo toàn qua OLD/NEW telemetry microbatch; metrics-off bổ sung cũng PASS. Shutdown recheck mới nhất sau counter fix xác nhận partial group flush/commit/ACK, bounded exit và byte gauge drain về 0.
- P5a.9a focused Go review sửa một defect counters sau confirmed commit khi Drain cancellation; regression RED→GREEN. Race 4 packages, gofmt, vet, lint (0 issues), build và diff PASS. Python affected guards: 378 tests PASS; py_compile/shell syntax/diff PASS. Đây không phải full CI.
- Không đổi schema/migration/dependency/deployment defaults. `STAGE5_ENABLED=false`, `MQTT_SHAPER_ENABLED=false`, `MQTT_MICROBATCH_ENABLED=false` tiếp tục là default. P5b, P6/Stage 6, HIL/AM5728/physical sensor, full/remote CI và live rollout chưa thực hiện.

### So sánh workload off/on

| Profile | Lưu bền vững và operation thực đo | Group / batch wait | Gateway application ACK p50/p95 | Thời gian generation+drain |
| --- | --- | --- | --- | --- |
| Off, 100 samples/message | 24 ledger / 2.400 raw / 24 temporal / 24 ACK; 24 single tx, 24 COPY, 2.400 COPY rows; outbox 0 | 1 member/tx; batch wait disabled | A 44,45/81,10 ms; B 44,23/46,03 ms | 15,431 s |
| On, 100 samples/message | Cùng raw/temporal/ACK; 20 group tx/COPY, 24 members, 2.400 COPY rows; fallback 0; outbox 0 | max 2 members; max-wait config 1s; 24 observations, batch-wait sum 20,011s | A 1.045,11/1.081,45 ms; B 1.028,83/1.046,05 ms | 16,363 s |
| Off, 1 sample/message | 24 ledger / 24 raw / 24 temporal / 24 ACK; 24 single tx/COPY, 24 COPY rows; outbox 0 | 1 member/tx; batch wait disabled | A 81,11/119,29 ms; B 29,19/58,06 ms | 4,462 s |
| On, 1 sample/message | Cùng raw/temporal/ACK; 8 group tx/COPY, 24 members, 24 COPY rows; fallback 0; outbox 0 | max 5 members; max-wait config 1s; 24 observations, batch-wait sum 21,099s | A 1.094,62/1.148,37 ms; B 829,86/1.040,33 ms | 6,279 s |

Kết quả này cho thấy transaction/COPY operation giảm (24→20 và 24→8) nhưng ACK latency và tổng cửa sổ hoàn tất cao hơn ở hai workload on; không chứng minh throughput tối ưu. Batch-wait sum là tổng histogram ở phase drain, không phải latency mỗi message hay bằng chứng mỗi delivery chờ đúng 1s; group có timer tối đa cấu hình 1s. Mỗi Gateway ACK có 12 mẫu. Tất cả cùng 24 message, security/profile, workload và raw/history semantics; operation counts là số thực đo chứ không suy ra từ target rows. Resource sampling chỉ 4–5 snapshot/service/run, không phải peak, RSS bound hoặc continuous profile. Chi tiết, lịch sử FAIL và artifacts phía dưới.

**Tốc độ hoàn tất quan sát được:** lấy `accounting.committed_unique` hoặc `accounting.raw_samples` chia cho `generation_plus_drain_seconds` thực trong từng `run.json`: off100 **1,555 messages/s / 155,531 samples/s**; on100 **1,467 / 146,675**; off1 **5,378 / 5,378**; on1 **3,822 / 3,822**. Đây là cửa sổ wall-clock hữu hạn bao gồm generation, observer/CLI orchestration và application-ACK drain, không phải cửa sổ DB-only, throughput bão hòa hay capacity. Không dùng thời gian generation+drain làm latency ACK; latency ACK ở bảng lấy riêng từ Gateway measurement.

**Artifact chính:** workload `/var/tmp/opencode/paper-p5a/continued-workloads-20261025-221406/{off-100-attempt3,on-100,off-1-attempt2,on-1}/run/artifacts/`; control `/var/tmp/opencode/paper-p5a/control-regression-20261025-230815-1443471/artifacts/`; shutdown current-source recheck `/var/tmp/opencode/paper-p5a/shutdown-current-20261025-233253-1477807/run/artifacts/`; final review `/var/tmp/opencode/paper-p5a/final-review-20261025/`; Python/final harness guards `/var/tmp/opencode/paper-p5a/final-gates-20261025-233206-1476916/`.

### Lệnh an toàn để tái chạy

Từ repo root, dùng directory mới/chưa tồn tại, log riêng ở bên ngoài run directory và fixture runner (không dùng deployment `.env` credentials):

```sh
set -e
base=/var/tmp/opencode/paper-p5a
mkdir -p "$base"
run_profile() {
  mode=$1 dataset=$2 name=$3
  stamp="$(date +%Y%m%d-%H%M%S)-$$"
  run_dir="$base/$name-$stamp"
  log="$base/$name-$stamp.log"
  test ! -e "$run_dir" && sh scripts/test-paper-p5a.sh "$mode" "$dataset" "$run_dir" >"$log" 2>&1
}
run_profile off 100 off-100
run_profile on 100 on-100
run_profile off 1 off-1
run_profile on 1 on-1
# Shutdown-only, profile on/1:
stamp="$(date +%Y%m%d-%H%M%S)-$$"
run_dir="$base/shutdown-$stamp"
log="$base/shutdown-$stamp.log"
test ! -e "$run_dir" && sh scripts/test-paper-p5a.sh on 1 "$run_dir" --shutdown-smoke >"$log" 2>&1
```

Rollback/ablation là đổi mode `on` → `off`, không đổi giao thức: `MQTT_MICROBATCH_ENABLED` mặc định false, độc lập với `STAGE5_ENABLED=false` và `MQTT_SHAPER_ENABLED=false`. Các flags deployment vẫn tắt. Telemetry payload/protocol, per-message identity, commit-before-ACK và application ACK từng message giữ nguyên. Xem runner guide tại [P5a.10 safe invocation](#lệnh-an-toàn-để-tái-chạy) và [README](../../README.md).

## Acceptance checklist P5a

- [x] Flag độc lập/default off, baseline per-message còn; nhóm có messages/rows/bytes/wait caps, low-load/shutdown flush và standalone large message có bound.
- [x] Ledger/raw/reported/temporal của group commit nguyên tử; DB dedup/conflict/overlap và ordering state/configuration giữ đúng contract.
- [x] Mixed-error fallback hữu hạn, rollback/DB failure/commit ambiguity không ACK giả hoặc làm message hợp lệ retry mãi cùng poison message.
- [x] ACK riêng từng input có identity đúng sau confirmed commit; Gateway outbox retry byte-exact, ACK failure không rollback DB.
- [x] P4 shaper/raw-payload reservation không bị bypass trong lúc gom/persist/ACK; byte gauges về 0 sau drain, enforcement đúng khi metrics off.
- [x] Native DB/E2E và affected regressions PASS; có runtime group nhiều message và off/on cho cả 100 và 1 sample/message.
- [x] Có actual transaction/COPY counts, batch sizes/wait, throughput/ACK latency và resource limitations; không claim tối ưu từ công thức target rows.
- [x] Report/navigation/backlog đồng bộ, checklist chỉ đóng P5a; P5b/P6/full CI/hardware/live rollout phân định đúng.

## Ranh giới nghiệm thu

Đây là Local MVP trên simulator/fixture, không phải production certification, soak/capacity/fairness proof hay chứng minh mọi interleaving. Full/remote CI, HIL/AM5728, cảm biến vật lý và live rollout **NOT RUN**. DB Tier-2 timestamp/index còn thuộc P5b/P5; P5b/P6/Stage 6 không triển khai. Limitations/workaround/revisit duy trì duy nhất tại [backlog chung](../dynsec/limitations-and-backlog.md).

## Checkpoint lịch sử

### Review follow-up lifecycle metrics — 26/10/2026

- Sửa MEDIUM trong ingress: defer của collection closure kết thúc processing quá sớm khi delivery còn trong nhóm; metrics-off cũng bỏ mất legacy processing aggregates. Lifecycle nay bắt đầu khi worker nhận xử lý và chỉ kết thúc một lần tại release, sau wait/persist/ACK hoặc nhánh lỗi/cancel. Reservation/confirmed-commit counters và protocol không đổi.
- `ProcessingLatencyTotal` là tổng thời gian lifetime từng delivery (delivery-nanoseconds), không phải tổng thời gian SQL độc lập; `ProcessingCount` đếm delivery hoàn tất ngay cả metrics-off. InFlight/histograms vẫn theo MetricsEnabled; group-persistence histogram và cách ghi duration persistence hiện hữu không thay đổi.
- Regression controlled clock/timer + collection/persist/ACK barriers: RED thật (exit 1) trước runtime fix, GREEN (exit 0) sau fix; thêm kiểm tra release lặp/unstarted. Focused Microbatch/byte-budget/stop/drain/ACK tests, `go test -race ./internal/mqttingress -count=1`, `go vet ./internal/mqttingress`, full `golangci-lint run --timeout=5m` (0 issues) PASS. Logs/status: `/var/tmp/opencode/paper-p5a/lifecycle-review-34UsgxEr/`.
- Native stacks/workload/full CI/HIL **NOT RERUN** ở follow-up này; không thay số đo hiệu năng hoặc gán evidence cũ cho source mới. Không commit/push/rollout; deployment defaults giữ nguyên.

Các checkpoint bên dưới được giữ nguyên để bảo toàn trình tự điều tra và mọi kết quả FAIL; trạng thái “còn lại/pending/chưa đóng” chỉ đúng tại ngày checkpoint tương ứng. Kết luận cuối và evidence mới nhất ở phần đầu report có ưu tiên.

## Checkpoint tạm dừng P5a.8a — 25/10/2026

- Các task đã thực hiện: P5a.0 contract/lock order; P5a.1 config/mapping; P5a.2–4 API, resource locks, transaction/COPY và fallback; P5a.5–6 worker/ACK/ownership; P5a.7 operation/group metrics và composition. Chưa đóng checklist nghiệm thu P5a.
- Theo kết quả subagent: `go test -race ./cmd/server ./internal/config ./internal/mqttingress ./internal/telemetry -count=1` PASS sau wiring; focused tests/gofmt/diff checks đã chạy ở các lát cắt. Đây không phải native acceptance hoặc full CI.
- Native command `python3 scripts/tests/stage3_persistence_runner.py --fixture group`: **FAIL** tại `src/internal/telemetry/postgres_integration_test.go`, assertion cuối mong `CopyCalls=2`, thực tế `CopyCalls=3`. Log ghi `GroupAttempts=4`, `FallbackAttempts=2`, `ConfirmedCommits=3`, `ConfirmedRollbacks=3`, `CopyReturnedRows=7`, `ConfirmedNewRows=7`. Không sửa assertion hoặc chạy lại sau failure.
- Giải thích sơ bộ từ source: một COPY nhóm đầu, một COPY message hợp lệ trong fallback, và một COPY có lỗi tiêm thử nghiệm đều là actual calls; cần duyệt xác minh assertion theo từng pha trước khi sửa. Không coi FAIL này là PASS hoặc kết luận toàn bộ runtime đúng.
- Evidence: `/var/tmp/opencode/paper-p5a/native-group-20261025/stage3-group.log` và `exit-status.txt`. Runner ghi `Fixture cleanup verified: True`; không khởi chạy deployment/shared Compose.
- Còn lại: xử lý failure được duyệt, hoàn thiện native cases/regressions, exporter và workload off/on cho cả hai profile 100/1 sample, control/configuration regression, final gates và bàn giao. Full/remote CI, HIL và live rollout NOT RUN; không commit/push, P5b/P6 chưa triển khai.

## Follow-up trực tiếp — delta metrics và regressions, 25/10/2026

Theo yêu cầu mới của người dùng, coordinator trực tiếp kiểm source counters, thêm assertion delta toàn bộ `PersistenceMetricsSnapshot` sau mỗi pha, chạy native với kỳ vọng cuối cũ trước khi sửa. Bốn pha đều khớp delta, test chỉ FAIL ở kỳ vọng tổng COPY=2; sau đó chỉ đổi kỳ vọng tổng thành 3 và giữ các kiểm tra dữ liệu/rollback. Không sửa runtime/counter để làm test PASS.

| Pha | Begin thành công | Commit xác nhận | Rollback xác nhận | Fallback | COPY calls | COPY returned rows | Confirmed new rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Nhóm mới | 1 | 1 | 0 | 0 | 1 | 6 | 6 |
| Replay DB duplicate | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| Mixed conflict/fallback | 3 | 1 | 2 | 2 | 1 | 1 | 1 |
| COPY bị tiêm lỗi | 1 | 0 | 1 | 0 | 1 | 0 | 0 |
| Tổng | 6 | 3 | 3 | 2 | 3 | 7 | 7 |

- Native group **PASS**: ledger=4, raw=7, temporal=4; failed COPY không để lại ledger/raw/temporal mới. Native baseline per-message/fresh **PASS** (dedup/conflict, overlap, graph, ordering/concurrency, state/temporal rollback và configuration preservation hiện có). Native upgrade **PASS** (compressed late data, legacy marker, raw/history provenance và bucket aggregation). Cả ba fixture báo `Fixture cleanup verified: True`.
- Chạy bằng API có sẵn của `stage3_persistence_runner`: `compile_integration_binary(work)`, rồi `run_group_fixture`, `run_normal_fixture`, `run_upgrade_fixture` trên cùng binary build từ source mới; mỗi fixture DB riêng. Binary/logs đặt dưới root artifact được bảo vệ bên dưới, không dùng temp binary tại `/tmp` chung. Đây là DB-only fixture schema 20 hiện có, không phải chứng minh full composition schema 23, command/result E2E hoặc workload MQTT P5a.
- `go test -race ./internal/telemetry ./internal/mqttingress ./internal/config ./cmd/server -count=1`: **PASS**; native opt-in skips trong lệnh này không được tính native PASS.
- `PYTHONPATH=scripts/tests python3 -m unittest scripts/tests/test_task28_simplified_schema_runner.py scripts/tests/test_paper_p4_support.py scripts/tests/test_stage3_uplink_runner.py scripts/tests/test_stage3_workload_runner.py`: **131 tests PASS**.
- `gofmt -l internal/telemetry/postgres_integration_test.go`, `git diff --check`, `go vet ./...`: **PASS** trước cập nhật checkpoint này.
- `golangci-lint run --timeout=5m`: **FAIL**, 7 issues có sẵn trong implementation/tests P5a: unchecked rollback return (`postgres_repository.go`), import order (`microbatch_test.go`, `postgres_resources_test.go`), comment của `MicrobatchSnapshot`, hai unused parameters (`microbatch_test.go`), redundant same-type assertion (`cmd/server/ingress_test.go`). Dừng ngay, chưa sửa các lỗi lint; `go build ./...` trong lượt này **NOT RUN** vì gate trước FAIL.
- Evidence: `/var/tmp/opencode/paper-p5a/metrics-delta-ahmypwgu/` gồm `native-group-red.log`, `native-group-green.log`, `native-normal-green.log`, `native-upgrade-green.log`, `go-race.log`, `python-regressions.log`, `go-vet.log`, `golangci-lint.log` và exit-status tương ứng. Giữ nguyên FAIL ban đầu và FAIL tái hiện, không đổi lịch sử thành PASS. Không sửa deployment defaults, shared resources, schema hoặc commit/push.

## Follow-up lint tối thiểu — 25/10/2026

- Theo yêu cầu người dùng, trực tiếp sửa đúng 7 lỗi trong 5 file Go: explicit discard kết quả rollback cleanup theo semantics best-effort sẵn có (giữ primary error); tách import local ở hai test files; sửa/move comment đúng exported type; đổi hai parameter không dùng thành `_`; thay same-type assertion dư bằng nil check (baseline interface đã được compiler bảo đảm). Không đổi transaction, fallback, ACK hoặc counters.
- `golangci-lint run --timeout=5m`: **PASS, 0 issues**; `go vet ./...`, `go build ./...`, formatting và `git diff --check`: **PASS**.
- `go test -race ./internal/telemetry ./internal/mqttingress ./internal/config ./cmd/server -count=1`: **PASS**. Bộ Python regressions cùng bốn modules ở follow-up trước: **131 tests PASS**.
- Native group DB chạy lại từ binary mới: **PASS**, delta theo bốn pha giữ nguyên; tổng COPY=3, returned/confirmed new rows=7, ledger/raw/temporal=4/7/4. Runner xác nhận cleanup. Native baseline/upgrade đã PASS ở follow-up trước, không chạy lại trong lát cắt chỉ sửa lint này. Không suy native opt-in SKIP thành PASS.
- Evidence: `/var/tmp/opencode/paper-p5a/lint-fixes-6jmnxs2a/` gồm logs và exit-status cho lint/vet/build/race/Python/native group, cùng snapshot source trước sửa để đối chiếu diff tối thiểu. FAIL lint và native trước đó vẫn giữ nguyên trong lịch sử. Chưa full CI, workload MQTT off/on, HIL/live rollout; không commit/push hoặc tự mở P5b/P6.

## Baseline và phạm vi

### Chốt lát cắt P5a.8a — native DB, 25/10/2026

- Ba cases bổ sung đã chạy thật: duplicate ngay trong cùng group chỉ ghi một dataset; hai ID có inclusive sequence overlap bị rollback nhóm rồi fallback hữu hạn; hai `PersistBatch` đồng thời với một ID chung chỉ có một new và một duplicate, mỗi message riêng vẫn commit. Concurrency dùng quan sát lock wait trên hai PostgreSQL connections; không giả định worker A thắng trước, kiểm current state/version theo thứ tự commit quan sát được.
- Tổng fixture sau cả bảy pha: ledger/raw/temporal=9/13/9, 8 group attempts, 4 fallback attempts, 12 begin thành công, 7 confirmed commits, 5 confirmed rollbacks, 7 COPY calls, 13 COPY returned/confirmed new rows. Riêng concurrency: 2 transactions/COPY, 3 new rows, message chung có đúng một ledger/raw/temporal; duplicate không tiến reported version.
- Sau sửa boolean test tối thiểu: `golangci-lint run --timeout=5m` **PASS, 0 issues**; `go vet ./internal/telemetry`, `go build ./...`, `go test -race ./internal/telemetry -count=1`, formatting/diff check và native group **PASS**. Cleanup fixture được xác nhận.
- Evidence mới: `/var/tmp/opencode/paper-p5a/native-group-lint-final-jhljurog/`. Các lượt compile/lint FAIL trước đó ở `native-group-cases-20261025-202034` và `native-group-cases-fix-20261025-202549-1240268` vẫn là lịch sử FAIL, không đổi thành PASS.
- Đóng **lát cắt native DB P5a.8a**, không đóng toàn P5a.8/P5a. Fixture vẫn schema 20 của Stage 3; full Compose/schema 23, MQTT application ACK/SQLite outbox, workload off/on 100/1 sample và configuration qua command/result thật còn ở các task tiếp theo. Không thay schema/deployment defaults, commit/push hoặc rollout.

- Baseline source: branch `feat/paper-requirement`, HEAD `3441d997976782ac8b9d68c9c47ee9ff6c3632b5`, worktree sạch khi khảo sát. Migration terminal `000023_sensor_control_intent.up.sql`.
- Deployment defaults không đổi: `STAGE5_ENABLED=false`, `MQTT_SHAPER_ENABLED=false` trong `.env.example`. Microbatch P5a là một flag độc lập, opt-in/default-off khi được triển khai; không bật các cờ deployment hiện hành.
- Theo master plan P5 đã cập nhật: P5a giữ ledger/schema hiện tại trừ khi implementation chứng minh nhu cầu schema cụ thể. Timestamp/index Tier-2 là việc P5 còn lại, sẽ được cụ thể hóa cùng P5b; hoàn tất P5a không hoàn tất P5. Không sửa migration đã áp dụng hoặc bỏ guard `(gateway_id, message_id)`.
- P5a chỉ gom nhiều MQTT deliveries ở backend. Không đổi protocol, Gateway outbox/ACK, sample data, measurement timestamps, message IDs hay sequence ranges. P5b RAM dedup, P6 và Stage 6 ngoài phạm vi.

## Cấu hình và ngân sách microbatch (implementation đã nghiệm thu local)

Compose ánh xạ `MQTT_MICROBATCH_ENABLED=false` cùng các giới hạn ứng viên: tối đa 32 messages, 500 raw rows, 4.194.304 payload bytes và thời gian gom tối đa 1s. Đây là điểm bắt đầu có giới hạn, không phải profile đã đo/tối ưu; bật microbatch độc lập, không làm thay đổi mặc định `STAGE5_ENABLED=false` hoặc `MQTT_SHAPER_ENABLED=false`.

Ngân sách nhóm đồng thời giới hạn số messages, số rows đã decode và tổng raw payload bytes. Raw-byte cap không bao gồm overhead của samples đã decode, metadata mỗi delivery hay cấu trúc Go; các phần này có accounting bound riêng. Với mỗi worker, accounting cộng group đang gom/xử lý và tối đa một carried message cùng decoded/metadata allowances; tổng qua workers phải nằm trong cap ingress 256 MiB hiện có. Một message hợp lệ riêng lẻ lớn hơn target group cap được xử lý nguyên vẹn qua single-message path theo giới hạn hiện có; không chia message hoặc từ chối chỉ vì target nhóm nhỏ hơn.

Thời hạn gom tính từ member đầu tiên, không được gia hạn khi có traffic mới. Flush/persist/fallback và drain dùng deadline chung hữu hạn; không giữ transaction khi chờ gom. Runtime behavior được kiểm chứng qua unit/race, native workload/control/shutdown artifacts nêu trong phần kết quả cuối; các giới hạn này không phải chứng nhận capacity hoặc mọi interleaving.

## Kết quả per-input và validation

Batch persistence cần trả kết quả theo đúng input, không chỉ một outcome tổng:

```go
type BatchPersistResult struct {
    InputIndex int
    Identity   MessageIdentity // GatewayID, SensorID, BootID, MessageID
    Outcome    CommitOutcome   // committed hoặc duplicate_committed; zero = unconfirmed
    Err        error           // lỗi riêng của input; nil chỉ khi có outcome được xác nhận
    SamplesSaved int           // chỉ samples mới, không cộng duplicate
}
```

Đây là shape hợp đồng được hiện thực trong API batch. Mỗi input delivery có đúng một result; `InputIndex` luôn trỏ về delivery gốc dù repository phân nhóm/sắp thứ tự lock. Kết quả không xác nhận sau transaction rollback/commit ambiguity không được biến thành success hoặc duplicate có thể ACK.

Worker validate riêng từng delivery trước khi đưa vào group, dùng `receivedAt` ban đầu. Malformed/validation failure bị loại khỏi group và không mở transaction. Resource graph, ledger identity/hash/provenance, sequence overlap và duplicate là các kiểm tra DB trong transaction; không được thay bằng cache hoặc kiểm tra chỉ trong RAM. `MessageIdentity` khớp đầy đủ Gateway, Sensor, boot ID và message ID với message đã validate trước khi ACK; regression `TestMicrobatchFullIdentityMismatchSuppressesACK` nằm trong package race suite cuối.

Trong một group, các message cùng Sensor được xử lý theo thứ tự `InputIndex` tăng dần (thứ tự worker nhận từ queue), không sort theo measurement time/sequence. Như vậy reported-state ordering tiếp tục dùng logic hiện hữu: measurement mới hơn thắng; bằng timestamp chỉ sequence tăng trong cùng boot tiến; late/không thể sắp thứ tự thì giữ current state. Mỗi message mới vẫn có một temporal observation; duplicate không ghi raw/state/temporal.

Chỉ sau khi transaction chung commit được xác nhận mới trả `Committed`/`DuplicateCommitted` cho inputs tương ứng. Sau đó ingress kiểm tra identity đầy đủ và phát ACK riêng từng delivery. ACK lỗi không rollback DB; Gateway durable outbox retry byte-exact. Không ACK từ row ledger vừa thấy trong transaction đã rollback, cũng không ACK từ `CommitUnconfirmed`.

## Lock protocol đề xuất cho transaction nhóm

Stable sorting là cần thiết nhưng **không đủ**: hiện telemetry khóa Sensor trước, rồi ghi ledger/raw có foreign key về Gateway; provisioning và control khóa Gateway trước Sensor. Parent lock phải được lấy trước child lock trên toàn group, không phải resource-by-resource xen kẽ.

Sau khi group đã validate và flush-ready, bắt đầu một transaction ngắn, rồi theo các pha sau:

1. Lập tập Gateway duy nhất và Sensor duy nhất từ members; canonical Sensor entity URNs/IDs có thể dựng từ mapper mà không đọc graph trước lock. Không bắt đầu transaction trong lúc chờ timer. Lấy Gateway relational row `FOR SHARE` theo `gateway_id` tăng dần **trước mọi Sensor lock**. Telemetry không kiểm membership/actor; không thêm membership lock vào đường này.
2. Lấy mọi relational Sensor row `FOR UPDATE` theo `(gateway_id, sensor_id)` tăng dần, trước Twin entity/state locks. Xác nhận resource tồn tại và lấy `unit` như `resolveTelemetryResource` hiện nay. Không resolve graph bằng snapshot trước khi chờ Sensor locks; chỉ xác minh graph sau khi có các locks cần thiết.
3. Xác minh graph provisioned hiện có (Gateway entity/state, Sensor entity, `hasSensor`); sau khi toàn bộ Sensor row locks đã có, lấy Sensor Twin entity rows `FOR UPDATE` theo canonical `(gateway_id, sensor_id)` tăng dần. Resource record trả về giữ entity ID và unit để helper state dùng.
4. Lấy mọi Sensor `twin_states` rows `FOR UPDATE` theo cùng thứ tự canonical. Chỉ sau khi đã có đủ resource/state locks mới classify/insert ledger và tiến hành writes.
5. Xử lý ledger, raw `CopyFrom`, reported state và temporal theo `InputIndex` ổn định trong từng Sensor (và thứ tự input ổn định giữa Sensors). Một lỗi ghi bất kỳ rollback toàn transaction.

Gateway `FOR SHARE` được chọn vì đó là mode tương thích với FK `KEY SHARE` của insert telemetry/ledger, đồng thời đồng bộ với provisioning hiện hành; không dùng Gateway `FOR UPDATE` trên telemetry. Các Gateway khác nhau được khóa theo ID để tránh hai batch đa-Gateway xin parent lock ngược thứ tự. Sau toàn bộ Gateway locks, khóa toàn bộ Sensor trước Twin entity/state: control transaction hiện khóa một Gateway → một Sensor → một entity → một state, nên nó không thể giữ Sensor của batch đang chờ rồi đòi một resource child đã bị batch giữ trước khi khóa Sensor tiếp theo. Kiểm định concurrency bằng test/native evidence vẫn là việc cần làm; đây không phải chứng minh mọi interleaving.

Reuse hoặc trích xuất tối thiểu, không tạo pipeline song song:

- `resolveTelemetryResource` (`telemetry/postgres_resources.go`) hiện vừa kiểm Gateway, khóa Sensor, vừa resolve graph. Khi tách, giữ logic graph/canonical-ID/unit; tách riêng `lockTelemetryGatewayShare` và `lockTelemetrySensor` để batch thực hiện các pha cha-rồi-con. Giữ wrapper single-message theo cùng contract/lock order.
- `insertTelemetryLedger` và `ledgerMetadata` (`telemetry/postgres_ledger.go`) giữ guard `(gateway_id,message_id)`, payload hash/source/provenance và overlap inclusive. Tách phần “classify/insert trên transaction đã khóa resource” chỉ nếu cần để xử lý nhiều members, không thay logic comparison.
- `applySensorReportedState` / `sensorStateAdvances` (`telemetry/postgres_reported_state.go`) giữ merge allowlist và ordering; chỉ tách helper nhận locked/current state nếu batch cần để không lặp `FOR UPDATE` hoặc đọc state nhiều lần. Mỗi member mới vẫn apply candidate riêng theo InputIndex.
- `appendSensorSampleIntervalTemporal` (`telemetry/postgres_temporal.go`) tiếp tục ghi một temporal record cho mỗi message mới.
- `postgresRepository.Persist` (`telemetry/postgres_repository.go`) tiếp tục đường single-message baseline/fallback; transaction boundary/timeout/commit-unconfirmed semantics dùng lại, không để API hiện hữu lặng lẽ chuyển sang batch.

Gateway metadata PATCH lấy Gateway `FOR UPDATE` trước Twin entity (`digitaltwin/postgres_gateway_patch.go`); Sensor metadata PATCH lấy Gateway `FOR SHARE`, membership `FOR SHARE`, Sensor `FOR UPDATE`, sau đó Sensor entity `FOR UPDATE` (`postgres_sensor_patch.go`). Control admission dùng Gateway `FOR SHARE` → membership `FOR SHARE` → Sensor `FOR UPDATE` → entity `FOR UPDATE` → state `FOR UPDATE` (`postgres_control.go:236–275`, `:98–118`). Control result/expiry reuse `withControlLock` (`postgres_control_dispatch.go:155–194`), còn result rows được khóa riêng trong callback (`postgres_control_result.go:48–57`). Sensor provisioning admin/owner dùng Gateway `FOR SHARE` → membership `FOR SHARE` → graph provisioning; legacy owner path `ProvisionSensorForOwner` dùng Gateway `FOR UPDATE` (`gateway/postgres_sensor_provisioning.go:56–100`). Vì vậy batch cần đúng Gateway `FOR SHARE` trước child locks; không đổi provisioning/control path trong P5a này.

Gateway Twin parent entity/state được kiểm tra graph nhưng hiện không bị khóa bởi telemetry hoặc control resource path khi xử lý Sensor. Không thêm parent Twin entity/state lock mặc định: thêm lock mới tạo thứ tự cạnh tranh chưa có hợp đồng. Nếu implementation chứng minh graph mutation đồng thời cần parent Twin lock, phải xác định lock mode/order tương thích với Gateway/Sensor provisioning trước khi sửa.

## Conflict fallback, deadline và payload ownership

- Một group transaction có thể bị rollback bởi business/resource/provenance conflict có thể quy về member: duplicate cạnh tranh/khác payload, sequence overlap, unknown Gateway/Sensor, incomplete graph hoặc legacy provenance không xác định. Rollback toàn group, rồi chạy `Repository.Persist` cũ tối đa một lượt cho mỗi member, với số member bị chặn bởi message cap và **một deadline flush chung**. Chỉ kết quả confirmed từ fallback transaction riêng được ACK; member conflict vẫn lỗi, member hợp lệ có thể commit. Ghi nhận fallback count.
- Validation errors bị loại trước transaction, không kích hoạt group fallback.
- Không bulk fallback với DB unavailable, acquire/query/COPY/state/temporal SQL error, lock timeout/deadlock, cancellation/deadline, lỗi không phân loại chắc chắn, hoặc commit-unconfirmed. Trả error/unconfirmed per input, ACK không được phát; Gateway retry. Batch retry/fallback không tự lặp vô hạn.
- Deadline thực dụng: dùng timeout persistence hiện có làm trần cho toàn bộ flush (group attempt cộng fallback), bị giới hạn thêm bởi shutdown context/deadline nếu sớm hơn. Các `Persist` fallback hiện tự đặt timeout con; truyền context flush để tổng thời gian vẫn bị deadline cha chặn. Không giữ transaction/connection khi gom timer hoặc khi publish ACK. ACK tiếp tục dùng publish timeout hiện hữu cho từng publish; Drain deadline chung vẫn có quyền hủy worker.
- `Admit` reserve `len(payload)` trước khi copy, và hiện `process` giữ reservation đến khi ACK publish kết thúc. Batch giữ từng reservation qua queue → validate → group wait → transaction/fallback → ACK cho delivery đó. Validation rejection/queue-full hoặc item bị loại release đúng một lần; message đã commit nhưng ACK lỗi vẫn giữ reservation tới khi publish attempt kết thúc, rồi release. Không release khi chỉ dequeue/commit; không tạo raw payload copy thứ hai để gom. Worker/group memory ngoài raw-byte budget phải bounded riêng theo caps/worker count và được công bố là overhead, không gọi payload budget là RSS cap.

## Rủi ro, bằng chứng và checkpoint lịch sử trước nghiệm thu cuối

> Phần này ghi nhận trạng thái lúc checkpoint ban đầu, trước các kết quả cuối ở trên. Các phát biểu “chưa nghiệm thu/pending” trong lịch sử bên dưới không mô tả trạng thái hiện tại; FAIL/NOT RUN lịch sử vẫn được giữ nguyên.

Fake transaction sẵn có ở `telemetry/postgres_repository_test.go` (`telemetryFakeDatabase`, `telemetryFakeTx`) có thể xác minh begin/commit/rollback, COPY count và query/Exec order. Ingress dùng `fakeRepo`, `fakePublisher`, `fakeValidator` cùng channel barriers trong `mqttingress/ingress_test.go`, `lifecycle_test.go`, `byte_budget_test.go`; shaper clock fake không phải batch timer clock. Native regression có thể tái dùng `scripts/test-stage3-persistence.sh` / Stage 3 persistence fixture. Chưa chạy test nào cho contract này.

Rủi ro cần kiểm chứng ở implementation: khóa all-Sensor rồi all-entity/state có chủ ý khác việc gọi trọn pipeline theo từng message; phải test concurrency với control admission, provisioning, Gateway metadata PATCH và telemetry batches nhiều resource. Transaction group dài hơn single transaction có thể tăng lock contention; fallback conflict có chi phí thêm transaction; decoded samples/metadata không nằm trong raw payload reservation; flush wait làm tăng application ACK latency. Native results P0–P4 là historical evidence trên fixture/revision riêng, không phải evidence P5a.

**Giới hạn trạng thái:** Không migration mới được đề xuất ở baseline này; không có benchmark, test, native fixture, full CI, HIL hay live rollout evidence P5a. Chưa nghiệm thu; P5b, P6, full/remote CI, hardware và live rollout vẫn ngoài phạm vi. Giữ nguyên deployment defaults nêu trên.

## Native workload acceptance — bốn profile, 25/10/2026

Đã chạy tuần tự trên branch `feat/paper-requirement`, HEAD `3441d997976782ac8b9d68c9c47ee9ff6c3632b5`, `dirty=true`; mỗi profile build backend/Gateway từ source lúc chạy, dùng full schema-23/post-Auth fixture, RS256/HTTPS, MQTT TLS/DynSec/ACL, quota backend 2 vCPU/4 GiB và project/resources cô lập. So sánh giữ workload, workers/pool/queue, shaper/security và offered pattern như nhau; chỉ flag microbatch khác.

| Profile | Ledger / raw / temporal / ACK; outbox cuối | Group và DB operation thực đo | Gateway ACK (12 mẫu mỗi Gateway; p50/p95 ms) | Generation / publish-drain, Gateway A/B; tổng generation+drain |
| --- | --- | --- | --- | --- |
| off, 100 samples/message | 24 / 2.400 / 24 / 24; 0 row, 0 byte | 24 single tx, 24 COPY, 2.400 COPY rows | A 44,45/81,10; B 44,23/46,03 | A 11,009/11,091s; B 11,006/11,051s; tổng 15,431s |
| on, 100 samples/message | 24 / 2.400 / 24 / 24; 0 / 0 | 20 groups, 24 members (2.400 rows), `max_members=2`; 20 group tx, 20 COPY, 2.400 COPY rows; fallback 0 | A 1.045,11/1.081,45; B 1.028,83/1.046,05 | A 11,005/12,088s; B 11,006/12,026s; tổng 16,363s |
| off, 1 sample/message | 24 / 24 / 24 / 24; 0 / 0 | 24 single tx, 24 COPY, 24 COPY rows | A 81,11/119,29; B 29,19/58,06 | A 0,175/0,234s; B 0,132/0,150s; tổng 4,462s |
| on, 1 sample/message | 24 / 24 / 24 / 24; 0 / 0 | 8 groups, 24 members (24 rows), `max_members=5`; 8 group tx, 8 COPY, 24 COPY rows; fallback 0 | A 1.094,62/1.148,37; B 829,86/1.040,33 | A 1,192/2,219s; B 0,878/1,897s; tổng 6,279s |

- Mỗi lượt xác minh raw/provenance/timestamp, temporal per message, current state/version, 24 application ACKs, SQLite outbox rỗng, duplicate replay không đổi DB, namespace ACL denial, HTTPS reads và RS256/TLS/ACL. `on` tạo group nhiều member ở cả hai dataset. Metrics chỉ có aggregate groups/members/max (không có histogram group-size); không suy ra phân phối singleton chi tiết. `on` có ACK p50/p95 cao hơn trong các workload hữu hạn này; đây không phải tuning/soak/capacity claim.
- `resources.csv` có 4–5 snapshot cho mỗi service/profile, không phải continuous profiling. Backend snapshot: off100 6,16–8,57 MB, CPU sampled 0–0,00%; on100 6,18–8,63 MB, 0–0,19%; off1 5,96–7,25 MB, 0–5,05%; on1 6,04–7,41 MB, 0–0,18%. Dữ liệu services đầy đủ, thời điểm snapshot và exporter rows nằm trong artifacts; không diễn giải các snapshot thưa thành peak/RSS bound.
- Cả bốn lượt cuối có `PAPER_P5A_WORKLOAD_ACCEPTED`, `full_p5a_workload_accepted=true`, `cleanup_verified=true`, `exact_cleanup_verified=true`, cleanup chính xác 16 containers / 1 network / 7 volumes; mỗi run có metrics/resources CSV và safe summary. `run.json` ghi image IDs đầy đủ (bao gồm image backend/Gateway mới build), flags, resources và latency samples. Artifacts/logs: `/var/tmp/opencode/paper-p5a/continued-workloads-20261025-221406/{off-100-attempt3,on-100,off-1-attempt2,on-1}/`; stdout/stderr/exit status được giữ riêng ngay dưới parent. Các FAIL trước đây được giữ nguyên: nested aggregate SQL tại `native-workloads-20261025-214256/`, temporal read contract/raw-window failures ở `resume-workloads-20261025-215924/` và hai lượt off100 đầu trong `continued-workloads-20261025-221406/`, observer capture off1 ở lượt đầu. Không đổi các FAIL đó thành PASS; chỉ các run directory liệt kê đầu mục này là accepted.
- Sửa helper theo các lỗi quan sát được: Twin simplified response field names theo API (`reported_state`/`reported_version`); temporal measured bounds được phân hoạch thành query nửa-mở dưới fixture raw-window 10s và cộng một microsecond để gồm sample cuối; subscriber quan sát đủ 24 offered messages để lấy payload của cả hai Gateway trong burst 1 sample. `273` Python tests thuộc P5a/P4/Stage3/native-compose PASS sau sửa; py_compile, shell syntax và `git diff --check` PASS. Trước lượt cuối, một lần chạy test tổng hợp có một assertion test sai (`query_windows` thay cho field `windows`); test được chỉnh và cùng bộ 273 test chạy lại PASS.
- Đây chỉ đóng bằng chứng workload của P5a.8 và regression preservation hẹp bên dưới, không đóng toàn P5a: shutdown scope, final affected gates và đồng bộ tài liệu/backlog còn lại. Full/remote CI, HIL/AM5728 và live rollout NOT RUN; giữ nguyên deployment defaults. Không claim throughput tối ưu, SQL reduction ngoài operation counts đo thực, hay capacity/soak.

## Regression native P5a — bảo toàn cấu hình qua command/result, 25/10/2026

- Run `control-regression-20261025-230815-1443471`: **PASS**, `PAPER_P5A_CONTROL_REGRESSION_ACCEPTED`, `full_p3_accepted=true`, `microbatch_control_regression_accepted=true`, `MQTT_MICROBATCH_ENABLED=true`; source revision `3441d997976782ac8b9d68c9c47ee9ff6c3632b5`, branch `feat/paper-requirement`, `dirty=true`. Artifact: `/var/tmp/opencode/paper-p5a/control-regression-20261025-230815-1443471/artifacts/run.json`; wrapper stdout/status ở parent (`control-regression-20261025-230815-1443471.wrapper.log`, `.exit-status.txt`, exit 0).
- Fixture build/run dùng P3/P2 isolated Compose topology: backend 2 vCPU/4 GiB, RS256 login và HTTPS verified; startup checks áp migrations pre-Auth 1–20 rồi post-Auth compatibility 21–23 và xác minh đúng terminal schema 23. Mosquitto TLS/DynSec ACL và resource ownership dùng lifecycle fixture hiện có. Không đụng root/shared Compose, deployment env, production ports/volumes hoặc prune.
- Luồng điều khiển thật: 5 command P3 có metrics-on, gồm 5 result `succeeded` và xác nhận `reported.configuration` (kết thúc desired version `4`, interval `16667` µs), cùng một command P3 metrics-off. Sau khi các Gateway writers đã stop/reopen/drain theo P3 regression, fixture publish thủ công đúng hai MQTT telemetry messages (`old`, `new`) qua ingress microbatch bật; đây **không** phải batch generation/workload do Gateway tạo. Cả hai có application ACK riêng, mỗi message có ledger=1, raw=1 khớp identity/hash/provenance/timestamp/value, temporal=1; DB/API HTTPS readback xác nhận configuration và desired-state/version giữ nguyên. OLD không lùi reported state/version; NEW tiến reported version 12→13. Không có timestamp tương lai.
- Metrics-on readback trước/sau regression cho thấy microbatch enabled, delta **2 group attempts / 2 members**. Chỉ báo cáo operation counts này; không suy ra group nhiều-member (hai lượt có thể là nhóm singleton). Metrics khả dụng trong regression; sau đó helper chạy regression P3 metrics-off riêng, không gán metrics-on evidence cho lượt metrics-off.
- Toàn P3 fixture báo 6 command cycles (5 metrics-on + 1 metrics-off), DB cuối ledger/raw/temporal/configuration `17/642/17/2`; exact cleanup verified: **17 containers / 1 network / 7 volumes**. Những lượt native trước assertion fix vẫn là FAIL và được giữ nguyên: run `...230329-1424199` và `...230612-1434041`, đều cleanup verified `15/1/7`; lỗi đầu là assertion so sánh configuration bỏ qua `applied_at`. Sửa helper chỉ so desired version/sample interval đã xác nhận từ command result và yêu cầu `applied_at` có mặt; các kiểm tra preservation sau mỗi telemetry vẫn so nguyên configuration/desired state, không nới identity/ACK/DB assertions.
- Regression helper unit tests: `PYTHONPATH=scripts/tests python3 -m unittest scripts/tests/test_paper_p3_support.py` **8 tests PASS**; `py_compile` PASS sau sửa. Đây là checkpoint lịch sử trước shutdown/final gates/docs; phần chưa hoàn tất khi đó đã được xử lý theo các mục P5a.9a/9b và kết quả cuối.

## Controlled shutdown — partial microbatch native smoke, 25/10/2026

- Thêm opt-in riêng `--paper-p5a-shutdown-smoke` (wrapper: `sh scripts/test-paper-p5a.sh on 1 <NEW_RUN_DIRECTORY> --shutdown-smoke`); chỉ hợp lệ với profile on/1. Không đổi bốn profile so sánh hay default microbatch-off. Shutdown-only profile nâng `MQTT_MICROBATCH_MAX_WAIT` từ 1s lên **2s** để có cửa sổ quan sát bounded trước SIGTERM; cùng Compose environment đặt `SHUTDOWN_TIMEOUT=55s` (budget dùng trong native fixture). Không phải deployment setting/benchmark profile.
- Unit coverage có sẵn đã bao phủ `TestMicrobatchStopFlushesPartialGroup`, `TestMicrobatchDrainCancellationReleasesBlockedGroupAndQueuedItem`, timer/cap flush và byte ownership; không thêm ma trận lỗi native. Helper regression mới khóa scope on/1, 2s wait và bất biến các giá trị profile khác.
- Native run `/var/tmp/opencode/paper-p5a/shutdown-smoke-20261025-232022-1460766`: **PASS**, source `3441d997976782ac8b9d68c9c47ee9ff6c3632b5`, dirty; source-built backend/Gateway, isolated schema-23/Auth/RS256/HTTPS, MQTT TLS/DynSec/ACL, backend quota 2 vCPU/4 GiB. Bốn workload mode/dataset comparison traffic unchanged; post-workload, sau khi Gateway writers dừng, helper subscribe QoS/application ACK trước publish, rồi publish đúng **một** fresh 1-sample telemetry message qua TLS/DynSec. Metrics barrier chứng minh message đã được xử lý, còn 356 buffered payload bytes; DB ledger vẫn 0 trước tín hiệu; thời điểm quan sát <2s max-wait.
- Helper gửi SIGTERM cho đúng backend container đang chạy. Backend thoát code **0**, drain 0,291s trong 55s common shutdown budget; trước timer target (2s). Sau shutdown readback: ledger/raw/temporal **1/1/1**, application ACK riêng đúng message được kiểm tra bằng ACK verifier; lifecycle log báo `buffered_payload_bytes=0`. Mosquitto được giữ sống trong thời gian drain/ACK. Đây chứng minh partial group được flush/commit theo graceful composition, không phải timer expiry hoặc ACK giả.
- Run còn giữ `full_p5a_workload_accepted=true`; normal workload accounting 24 ledger/raw/temporal/ACK, outbox 0, on/1 group metrics 8 groups/24 members/max 5. Exact owned-resource cleanup: **16 containers / 1 network / 7 volumes**; `exact_cleanup_verified=true`. Artifacts: `/var/tmp/opencode/paper-p5a/shutdown-smoke-20261025-232022-1460766/artifacts/{run.json,metrics.csv,resources.csv}`; wrapper log/status ở `/var/tmp/opencode/paper-p5a/shutdown-smoke-20261025-232022-1460766.wrapper.log` và `.wrapper.exit-status.txt` (exit 0). Top-level `PAPER_P5A_WORKLOAD_ACCEPTED` là workload marker hẹp, không phải xác nhận toàn bộ P5a/final gates.
- Checks trong lát cắt này: `PYTHONPATH=scripts/tests python3 -m unittest scripts/tests/test_paper_p5a_support.py scripts/tests/test_native_compose_smoke.py` **166 tests PASS**; `py_compile`, shell syntax, `git diff --check` **PASS**; `go test -race ./internal/mqttingress ./cmd/server -count=1` **PASS**. Selector-only regex invocation cho hai package cũng PASS (`cmd/server` không có test khớp selector); package-wide run ở trên là kết quả Go lifecycle cuối.
- Giới hạn tại checkpoint shutdown-smoke này: một native representative case, không native forced-cancellation test (cancellation được covered bởi focused unit test); không suy diễn thành bảo đảm mọi scheduling/interleaving, soak/capacity, full/remote CI, HIL/AM5728 hoặc rollout. Khi đó final affected gates/docs/backlog còn mở; trạng thái cuối được cập nhật ở phần đầu report. Giữ deployment defaults off; không P5b/P6.

## P5a.9a — final focused Go correctness review, 25/10/2026

- Rà source Go hiện hành tại HEAD `3441d99` trên dirty worktree (không phải revision sạch). Tìm thấy một lỗi cụ thể trong `mqttingress.persistValidatedGroup`: nếu `PersistBatch` trả về các outcome commit đã xác nhận đúng lúc `Drain` hủy context, kiểm tra `ctx.Err()` chạy trước khi đọc kết quả và bỏ mất confirmed message/sample counters. ACK đã bị chặn an toàn; tuy nhiên hủy context không được xóa bằng chứng commit đã xác nhận.
- Đã thêm regression `TestMicrobatchConfirmedResultsAreCountedAfterDrainCancellation`: fake batch repository chờ cancellation rồi trả confirmed results; lần đầu **RED** (`committed=0 samples=0`), sửa tối thiểu bằng cách xử lý/đếm các kết quả hợp lệ trước và chỉ để cancellation ngăn ACK, sau đó **GREEN**. Lượt thử đầu tiên của test harness chưa chờ group timer/carry và hết timeout trước khi repository được gọi; đã thay bằng barrier controlled-clock để đảm bảo member nằm trong group trước Drain, không nới assertion. Test xác nhận counters `1/2`, không phát ACK sau cancellation, payload bytes về 0. Không đổi contract persistence/ACK.
- Mapping review → tests: transaction/dedup/overlap/resource ordering/raw-state-temporal rollback và commit ambiguity: `internal/telemetry` package race suite, gồm `TestPersistBatchExactDuplicateWithinGroupSharesLedgerTransaction`, `TestPersistBatchConflictFallsBackPerInputAfterRollback`, `TestPersistBatchFailuresReturnNoConfirmedOutcomes` và các failure/metrics cases; input identity/reordered/error + ACK-failure/counters/cancellation: `internal/mqttingress` suite (`TestMicrobatchFullIdentityMismatchSuppressesACK`, `TestMicrobatchReorderedResultsAndErrorOverride`, `TestMicrobatchACKFailureDoesNotBlockRemainingACKs`, cancellation/confirmed-counter regression); cap/carry/partial stop/byte ownership/metrics-off/shaper: cùng suite gồm `TestMicrobatchDeadlineDoesNotResetAndCarriesCapExceedingItem`, `TestMicrobatchStopFlushesPartialGroup`, `TestMicrobatchDrainCancellationReleasesBlockedGroupAndQueuedItem`, `TestAdmissionShaperOffAndOnShareByteBudgetAndMetricsOffEnforces` và byte-budget regressions. Các suite chạy nguyên package nên các test này thực thi, không chỉ được chọn lọc bằng tên.
- Kết quả hiện tại: `go test -race ./internal/telemetry ./internal/mqttingress ./internal/config ./cmd/server -count=1` **PASS**; test regression riêng **PASS**; `gofmt -l .` không có output; `go vet ./...`, `golangci-lint run --timeout=5m` (0 issues), `go build ./...` và `git diff --check` **PASS**. Go test chạy không đặt `umask 077`.
- Logs: `/var/tmp/opencode/paper-p5a/final-review-20261025/{regression-red.log,regression-green.log,go-race.log,gofmt.log,go-vet.log,golangci-lint.log,go-build.log}`. Vì runtime correction chỉ thay cách ghi nhận counters sau khi repository đã trả kết quả confirmed (không đổi DB, protocol, hay ACK), không chạy lại native workload/control/shutdown stack. Evidence native ở các mục trước vẫn là evidence lịch sử trên source `3441d99`, dirty, không được nâng qualification thành test native ở revision sau sửa này. Chưa chạy full CI; Python final guards và đồng bộ backlog/docs còn mở. Không có migration/config default/dependency change; P5b/P6, HIL/live rollout ngoài phạm vi.

## P5a.9b — Python guards và native shutdown recheck cuối, 25/10/2026

- Bộ affected Python guards chạy hiện tại: `PYTHONPATH=scripts/tests python3 -m unittest scripts/tests/test_native_compose_smoke.py scripts/tests/test_paper_p5a_support.py scripts/tests/test_paper_p3_support.py scripts/tests/test_paper_p4_support.py scripts/tests/test_task28_simplified_schema_runner.py scripts/tests/test_stage3_uplink_runner.py scripts/tests/test_stage3_workload_runner.py scripts/tests/test_ci_go_test_guard.py scripts/tests/test_ci_feature_jobs.py` — **378 tests PASS**. Đây gồm mode/profile/metadata/exporter/cleanup, P3/P4 preservation, P2 simplified-schema exporter, Stage 3 uplink/workload và CI guards; không phải full CI.
- `py_compile` cho các Python helper/runner/test bị ảnh hưởng, `sh -n scripts/test-paper-p5a.sh scripts/test-stage3-persistence.sh` và `git diff --check`: **PASS**. Logs/status trong `/var/tmp/opencode/paper-p5a/final-gates-20261025-233206-1476916/`.
- Một fresh native run bằng đúng wrapper opt-in đã có: `sh scripts/test-paper-p5a.sh on 1 /var/tmp/opencode/paper-p5a/shutdown-current-20261025-233253-1477807/run --shutdown-smoke` — exit **0**, `native_executed=true`, `PAPER_P5A_WORKLOAD_ACCEPTED`, **không** phải full-P5a acceptance. Đây là recheck shutdown trên dirty source hiện hành sau sửa counter ở 9a, không phải lượt benchmark off/on mới. Fixture rebuild backend từ source lúc chạy; startup guard xác minh chính xác migration pre-Auth 1–20, post-Auth compatibility 21–23 và terminal schema 23. Run readback ghi RS256, HTTPS, MQTT TLS/DynSec ACL và các kiểm tra thật.
- Workload hữu hạn thường của profile on/1 cũng đạt: 24 ledger/raw/temporal/application ACK, 8 groups/24 members, hai Gateway outbox rỗng. Shutdown-specific assertions PASS: một message hợp lệ được quan sát trong partial group; trước SIGTERM ledger chưa tồn tại và buffered payload là 356 bytes; backend thoát code 0, commit ledger/raw/temporal `1/1/1` rồi ACK đúng application message trong **0,322s** (budget chung 55s, trước timer 2s); buffered bytes về 0. Exact owned cleanup PASS: **16 containers / 1 network / 7 volumes**.
- Artifact an toàn: `/var/tmp/opencode/paper-p5a/shutdown-current-20261025-233253-1477807/run/artifacts/{run.json,metrics.csv,resources.csv,startup-summary.json}`; wrapper log/status nằm ngoài run tại `.../shutdown-current-20261025-233253-1477807/{wrapper.log,wrapper.exit-status.txt}`. Metadata ghi revision nền `3441d99`, branch `feat/paper-requirement`, `dirty=true`; native run dùng working source có dirty counter fix, không biến bốn profile comparison hay control regression lịch sử tại `3441d99` thành kết quả sau sửa.
- Không có migration, dependency, deployment default hay resource/security profile change. Full/remote CI, HIL/board, physical sensor và live rollout **NOT RUN**. P5a.9b hoàn tất; checklist overall P5a chưa tick cho đến docs/navigation/backlog task kế tiếp; không triển khai P5b/P6.
