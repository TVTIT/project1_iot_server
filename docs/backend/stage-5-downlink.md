# Bàn giao điều khiển downlink Stage 5 — kiểm chứng tích hợp cục bộ

**Mốc kiểm tra:** 14/11/2026, cục bộ, có giới hạn theo source. Checkout
`development/`, nhánh `develop`, HEAD baseline
`9a348709d6c92c394237fc0078759468c944a83d` cùng các thay đổi R1/R2/5.8-R1
đã hoàn tất và dirty, cộng tài liệu chưa track
[`stage5-sensor-control-api.md`](../protocols/stage5-sensor-control-api.md).
Đây không phải revision đã commit sạch. Lượt kiểm chứng cuối không sửa repo.
Migration source hiện đến phiên bản 26; điều này **không** xác nhận phiên bản
hay trạng thái schema của bất kỳ database dùng chung/triển khai nào.

## Bằng chứng và kết quả

Handoff riêng và log nằm tại
`/var/tmp/opencode/stage5-final-checks-20261114-010339-110973/`. Handoff là chỉ
mục bằng chứng; không sao chép raw log, secrets, SQLite hay chẩn đoán riêng của
fixture vào đây. Bằng chứng R1/R2 trước đó được phân loại riêng tại
`/var/tmp/opencode/stage5-r1-20261114/p3-0f9c18bb-fd91-4d0a-9527-e289be31ec6d/`
và `/var/tmp/opencode/stage5-r2-20261114-local-rehearsal/`; đây là các lượt
trước, không phải checkpoint cuối.

| Phạm vi | Kết quả |
| --- | --- |
| Simulator/control trên source hiện hành | **PASS** — lane control cuối báo `PAPER_P3_ACCEPTED`, `native_executed=true`, `full_p3_accepted=true`, `exact_cleanup_verified=true`; fixture sở hữu đã dọn chính xác 17 containers, 1 network, 7 volumes. Kết quả chỉ xác nhận profile simulator P3. |
| Diễn tập control tương đương CI cục bộ | **PASS** — require-run và staging safe-summary được thực hiện cục bộ với fixture sở hữu; đây không phải remote workflow run. |
| Các kiểm tra cục bộ đã chọn ở checkpoint cuối | **PASS** — Go race/static/build/ARMv7; migration hiện hành 20–26 và compatibility; các lane Stage 2 hiện hành được chọn (auth/authorization/provisioning/admin/credentials/DynSec/bootstrap); Stage 3 persistence và toàn bộ uplink scenarios; Stage 4 mandatory/all; Python/CI harnesses bị ảnh hưởng; kiểm tra tài liệu/source-alignment và diff như được liệt kê trong handoff riêng. |
| Remote CI | **NOT RUN** — không dispatch và không có kết quả remote cho source qualification này. |
| Runtime board AM5728 với dữ liệu tổng hợp | **NOT RUN** — không có bằng chứng chạy trên board. |
| Sensor vật lý / HIL | **NOT RUN** — không có board, acquisition adapter hay bằng chứng sensor; R3/R4/R5 được người dùng chấp thuận hoãn. |
| Rollout shared/live | **NOT RUN** — chưa diễn tập schema/key/credential tại target, chưa enable và chưa demo. |

Các lệnh kiểm chứng cuối bao gồm:

```sh
# Chạy từ development/src/
go test -json -race -count=1 ./...
gofmt -l .
go vet ./...
golangci-lint run --timeout=5m
go build ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -o <private-artifact> ./cmd/gateway

# Chạy từ development/ (migration và control hiện hành)
timeout ... sh scripts/test-stage2-migrations.sh
timeout --signal=TERM --kill-after=180s 2100s \
  sh scripts/test-paper-p3.sh <fresh-private-run-leaf>
```

Python CI guard xác nhận đủ 35 required Go tests; các Python suites bị ảnh
hưởng chạy gộp kết thúc `Ran 694 tests ... OK`. Đây là số liệu của từng lệnh,
không phải tổng test duy nhất giữa các suite. Native opt-in skips không được
tính là PASS. Không đặt coverage gate hoặc tuyên bố tổng unique-test count.
Control fixture dùng profile simulator P3 trên topology Auth/HTTPS, database
và MQTTS cô lập. CI wiring tại [`ci.yml`](../../.github/workflows/ci.yml)
khai báo TimescaleDB và Mosquitto theo digest
(`timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`,
`eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`),
cùng Nginx `1.25-alpine`, GoTrue `v2.196.0`, Envoy `v1.39.1`, Storage API
`v1.74.0`. Đây là image references được cấu hình cho CI, không có nghĩa remote
workflow đã chạy. Handoff ghi nhận dependency pulls thành công và có safe
summary provenance cùng cleanup counts.

Các kiểm tra YAML/job separation, 132 local Markdown link targets,
REST route/parser/domain alignment, shell syntax, so sánh assignment/value trong
`.env.example` (không đổi giá trị; các defaults liên quan vẫn false) và
`git diff --check` cũng PASS tại checkpoint cuối.

Lệnh Go race ban đầu chạy từ repo root nên lỗi vì không ở trong Go module; chạy
lại từ `src/` thì PASS. Một assertion source-alignment ad-hoc ban đầu kiểm tra
HTTP parser cho JSON fields do domain sở hữu; đã sửa để kiểm tra
`digitaltwin/control.go` và PASS. Đây là lỗi lệnh/kiểm tra, không phải lỗi
product test. Các argparse negative-test diagnostics trong Python suite là
hành vi dự kiến; kết quả cuối suite là OK.

Remote Actions run cũ được README nhắc tới (`39272190456`, checkout
`35b6136f22c5a1c819afefc12003dba3ba613062`) chỉ là bằng chứng CI lịch sử; run
này có trước Stage 5 control job và không phải remote acceptance của checkpoint
này.

Trong đợt bàn giao này không sửa acquisition/builder/outbox/runtime; vì vậy
không chạy lại P5a/P5b control-preservation, P6/P7, performance hoặc E1–E5,
theo impact. Các lane Stage 2 legacy deferred về supervisor và Mosquitto
runtime/reloader cũng không chạy. Chưa có bằng chứng full CI workflow, board
testing hoặc production readiness.

## Hợp đồng và tài liệu vận hành

- [REST control contract](../protocols/stage5-sensor-control-api.md) và
  [MQTT v1](../protocols/mqtt-v1.md)
- [Vận hành và triển khai](../dynsec/deployment-and-operations.md)
- Báo cáo simulator control [P3](paper-p3-sensor-control-loop.md) và báo cáo
  [P6 dispatch](paper-p6-dispatch-and-mixed-load.md) (bằng chứng lịch sử, mỗi
  báo cáo có source qualification riêng)
- [CI workflow hiện hành](../../.github/workflows/ci.yml), job
  `stage5-control-integration`; rehearsal cục bộ không phải remote acceptance
- [Backlog giới hạn canonical](../dynsec/limitations-and-backlog.md)

Giữ các deployment defaults `STAGE5_ENABLED=false`,
`STAGE5_WAKEUP_ENABLED=false`, `WEBSOCKET_ENABLED=false`,
`MQTT_SHAPER_ENABLED=false`, `MQTT_MICROBATCH_ENABLED=false` và
`MQTT_RAM_DEDUP_ENABLED=false`. Không thay đổi giá trị triển khai.

## Ranh giới bàn giao

Phạm vi simulator cục bộ và control tương đương CI cục bộ hiện có đã hoàn tất;
điều này **không** đáp ứng nghiệm thu Stage 5 gốc đầy đủ hoặc chứng nhận
production readiness. Chuẩn bị board R3, acquisition/readback R4 và board E2E
R5 vẫn **DEFERRED / NOT RUN**, giữ nguyên yêu cầu nghiệm thu gốc. Stage 6 vẫn
sở hữu full retry policy, cancellation, reconciliation và mọi acknowledged
contract mới. Stage 9 tái sử dụng bằng chứng local/CI/board thực sự đã có, đồng
thời sở hữu target preflight, rollout và demo cuối khi được giao riêng. Media
Stage 8/P9 và official paper experiments vẫn là các hạng mục roadmap riêng.
