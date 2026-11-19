# P8 — Báo cáo thực nghiệm và bàn giao checkpoint server

**Trạng thái cập nhật (05/11/2026): `LOCAL_TOOLING_COMPLETE_SOURCE_QUALIFIED`.** BS.9b chốt tài liệu/kết quả/checklist cho local tooling và checkpoint P8 trong giới hạn MVP. Đây không phải nghiệm thu paper chính thức, production acceptance, chứng nhận capacity/HIL, hoặc hoàn tất yêu cầu board Stage 5 gốc. Mọi raw native P8 artifact tiếp tục giữ `full_p8_accepted=false`; không tạo aggregate `true`. Deployment flags P8/Stage 5 tiếp tục false. Dừng tại P8; không tiếp tục Stage 6/P9.

## Phạm vi và source qualification

P8 đã triển khai admission/accounting WebSocket session có giới hạn cấu hình, batch membership recheck và một số công cụ E1–E5; báo cáo này liên kết bằng chứng local đã ghi nhận nhưng không khẳng định mọi gate dùng cùng một dirty source tree. Artifact lịch sử giữ fingerprint riêng; không gộp artifact có source SHA khác nhau. Context SHA không có trong raw E5 schema và tiếp tục unavailable trong summary công khai; kiểm chứng context riêng được lập chỉ mục private. Cập nhật tài liệu không chạy lại integration.

Bảng nguồn/bằng chứng (cố ý không đưa private paths):

| Nhóm bằng chứng | Logical artifact / source | Kết quả và qualification |
|---|---|---|
| E1 pilot WS/MQTT giới hạn | `P8.6-E1-final-pilot`; chi tiết run source-qualified trong handbook | PASS, 64 sockets/2 principals, 20 unique messages, 2.000 raw rows, 530 WS writes, không ghi nhận drop; cleanup chính xác 12/1/5. Pilot ngắn, colocated; không phải capacity. FAIL ở outer report trước đó được giữ nguyên. |
| E2 pilot telemetry ghép cặp | `P8.7-E2-final-pilot`; `E2 equal_samples` (31/10/2026) | Pilot native cuối PASS sau hai lượt FAIL được lưu; bổ sung workload `equal_samples` (Attempt 4 functional PASS, source-qualified, contaminated pair, CPU/DBtx inconclusive). Xem [chỉ mục bằng chứng E2 equal_samples](results/p8/e2-equal-samples-20261031/index.md) và [runbook E2](paper-p8-e2-remote-gate.md). |
| E3 polling tải thấp và overlap polling/wakeup | `P8.8-E3-low-load-polling` cùng hai raw reports E3 mixed-load | Low-load PASS: bốn command terminal, pending 0, DB nền 12 ledger/1.200 raw/12 temporal; burst overlap không áp dụng. Hai run mixed-load polling và wakeup đều PASS, overlap được runner quan sát và kiểm tra hợp lệ; mỗi run có cleanup 12/1/5, background DB 24 ledger/2.400 raw/24 temporal, bốn receipt, hai configuration, 21 temporal command observations và lần lượt 33/32 raw samples command thực tế; pending sau drain bằng 0. Cả hai dùng source revision `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`, dirty SHA-256 `02b4122157a62bed89de17d8e6dfe5d81b948ba4be8f33f2a4b04292f57721f4`. Server-origin `D_dispatch`/`D_e2e` không có trong các raw report (null/unavailable), không được tuyên bố là đã đo. Các FAIL/PARTIAL cũ vẫn giữ nguyên. |
| E4 chẩn đoán mở rộng | `P8.9-E4-extended-on` | Kiểm tra warm, WS, cold-cache, conflict và durable Gateway reopen PASS; cleanup chính xác 13/1/5. Đây là pilot mở rộng có giới hạn, không phải TTL 600s hay nghiệm thu E4 chính thức. |
| P8.10 E5 các arm đóng băng | `p8-e5-optimized-7039c0a9eb731fa0`, `p8-e5-shaper_off-090fc844cf1bbd68`, `p8-e5-microbatch_off-3aafb955dd42a600`, `p8-e5-ram_dedup_off-9252b4ecd604539a`, `p8-e5-polling-0c6d6097e17acd84`, `p8-e5-fanout_scan-0cadf405faabd3c8`; source `52fd9456…`, dirty SHA `b4bd87ae…` | Sáu lượt driver/native PASS; cleanup mỗi lượt 12 containers/1 network/5 volumes; năm cặp strict-comparable; từng raw vẫn `full_p8_accepted=false`. Chỉ một repeat. |
| P8.12 kiểm tra source cuối | `p812-final-source-20261029`, source `52fd9456…`, dirty SHA `b73d8624…` | Go formatting, vet, golangci-lint 2.14.0, build, full race và 35 required test guards PASS; coverage 77,7%, không phải gate; 106 native opt-in skips vẫn là SKIP. Cross-build và paperload checks PASS. Selected Python guards/harnesses PASS, không phải full Python discovery. |
| DB/Auth | `p812-db-auth-retry-20261029` | Migration 17 owned-cleanup markers, compatibility, bốn authorization, năm provisioning và admin checks PASS. Migration-readiness FAIL đầu tiên vẫn được giữ, nguyên nhân chưa rõ. Auth FAIL ban đầu do perimeter expectation cũ; native auth retry đã sửa PASS. |
| MQTT/DynSec/bootstrap | `p812-native-mqtt-20261028` | Credential TLS/auth/ACL/API/readiness/shutdown; DynSec service/HTTP/report validation; bootstrap assertions/collision/port/pinned digest/NOT_READY/exact cleanup đều PASS. |
| Stage 3 | `p812-native-stage3-20261029` | Persistence fresh/upgrade/rollback và cả chín uplink scenario PASS; cleanup chính xác, không skip. |
| Stage 4 | `p812-native-stage4-20261028` | API vertical/contract/query PASS; fixture truy vấn 60.000 raw và 580 temporal; nén thành hai chunk; cleanup chính xác, không skip. |
| Compose/Docker | `p812-final-compose-docker-20261028` | Full HTTP RBAC/JWT acceptance, 14 role denial và 9 JWT denial, cleanup chính xác 10/1/3; backend/auth-init/reloader/credential-controller unique-tag builds và backend negative-config smoke PASS. Legacy unit/CI harness checks PASS với Python path bắt buộc; native execution legacy bị hoãn vẫn DEFERRED. Source `121e0deb…`. |
| P8.13 phục hồi cơ sở dữ liệu | `p8-recovery` (final), source `52fd9456…`, dirty SHA `b73d8624…` | Cả tám phase PASS, cleanup chính xác 14 containers/1 network/5 volumes, bốn child được theo dõi đã join, số dòng và content hash source/restore bằng nhau. Raw report SHA-256 `a6f6dc99…`; dump backup SHA-256 riêng `9567e934…` (430.003 bytes, mode 0600). Phạm vi là DB restore trong cùng PostgreSQL cluster thuộc fixture, không phải backup toàn platform/HA. Original control runner PARTIAL được giữ lại. |

### BS.6c — Parameterized E4 native verification (2026-11-05)

Root cause của runtime-incomplete đã được chứng minh và sửa: unique completion
so sánh với duration×rate thay vì E4 `max_messages`; phase replay vẫn mặc định
schedule 10 opportunities và cleanup validator còn 20/10. Thêm regression Go
(`TestE4UniqueOpenLoopStopsAtParameterizedJournalCount`) và Python validation cho
phase schedule / cleanup counts. Cache-on và cache-off native 8 unique + 2 replay
đều PASS với real DB/MQTT/WS: 8 ledger / 800 raw / 8 temporal, replay 2 ACK,
DB fingerprint không đổi, WS unique 8 new events và replay 0 new events, pending 0.
Cleanup lần lượt 13/1/5 (cache-on có cold + conflict + durable) và 12/1/5
(cache-off, cold N/A); conflict PASS cả hai. Durable reopen PASS: fault giữ pending
1 row / 974 bytes, payload SHA-256 và message identity giữ nguyên, reopen ACK xong
pending 0; main dataset không đổi. Các FAIL lịch sử trước khi sửa còn được giữ lại.

Tại checkpoint BS.6c ban đầu, official 1.000 + 100 + expiry chỉ có
`VALIDATED_PLAN_ONLY` từ `serve --dry-run` (1.000 entries, 49.152.000-byte journal
bound, budget 1.164 giây); real TTL 600s NOT RUN. Follow-up portable offline
contract ngày 05/11 được ghi ở phụ lục cuối báo cáo; real TTL vẫn NOT RUN. Bằng chứng/lệnh đầy đủ ở
[E4 runbook](paper-p8-e4-preparation.md). BS.6 chỉ đóng checklist mục retry,
retention/ACK-ignore/reopen; official A/B prepare→bundle→bind/import ở 1.000+100,
real TTL, Gamma_red computation remain NOT RUN/unavailable; lịch sử E4 20+10
không bị ghi đè.

Dấu ba chấm trong fingerprint ở trên chỉ rút gọn khi hiển thị; source revision đầy đủ và digest artifact/report nằm trong `results/p8/overview.json` và private evidence index. Raw-report SHA-256 của E5 có trong `results/p8/e5/e5-summary.json`; safe CSV/JSON E5 hiện hữu không bị thay đổi.

## Pilot local và giới hạn tuyên bố

| Tuyên bố | Module/tool | Metric/bằng chứng | Logical artifact | Giới hạn |
|---|---|---|---|---|
| WS cap cấu hình được và hữu hạn | `internal/config/websocket.go`, `internal/telemetryws` | effective session cap mặc định 32/tối đa 10.000; logical reference budget mặc định 8 MiB/tối đa 256 MiB; membership sweep interval 5s/budget 2s/concurrency 2 | Bằng chứng P8.1–P8.3 trong handbook | Cấu hình hữu hạn không chứng minh tải 10k, RSS, fairness hay capacity host. Close observation ring vẫn 32. |
| E1 viewer/namespace hoạt động ở pilot giới hạn | P8 E1 native fixture và WS fanout | 64 sockets/hai principals; 20 unique và 2.000 raw; 530 WS writes; cleanup chính xác | `P8.6-E1-final-pilot` | Không phải A/B từ xa hoặc capacity; resource snapshots thưa. |
| E2 đã chạy accounting telemetry và persistence | `cmd/paperload`, `internal/paperload`, backend persistence/shaper | hai sample profile giới hạn, DB/application ACK accounting | `P8.7-E2-final-pilot` | Không phải rung chính thức 100–20.000 msg/s hoặc soak; giữ hai FAIL trước đó. |
| E3 đã kiểm tra đường command/control | command fixture, Gateway simulator, P3/P6 control packages | Pilot tải thấp: bốn command terminal, persistence nền 12/1.200/12, outbox pending 0. Mixed-load polling và wakeup: bốn command, runner xác nhận overlap steady/burst, bốn receipt và hai config | `P8.8-E3-low-load-polling`, cùng hai raw E3 mixed-load reports và bằng chứng P3/P6 riêng | `D_dispatch`/`D_e2e` server-origin null/unavailable; không gán giá trị đo. E5 không thay thế overlap storm E3. |
| E4 đã chạy replay/dedup và đường warm/cold | paperload journal, Gateway SQLite helper và persistence | warm/WS/cold/conflict/durable reopen checks PASS; cleanup chính xác | `P8.9-E4-extended-on` | Không phải TTL 600s hoặc nghiệm thu E4 đầy đủ; synthetic journal tự nó không phải durable Gateway outbox. |
| E5 đã ghi nhận pilot cơ chế ghép cặp | E5 driver và native fixture | Mỗi arm: 20 unique messages/2.000 raw samples, 10 warm replay; tổng 20 WS events trên hai viewer, 10 event mỗi viewer; 4 receipt; 2 lần kiểm tra config | Sáu logical ID E5 ở trên | Một repeat colocated; chưa tổng hợp repeat chính thức. |
| Microbatch cost counters khác nhau trong initial window đã nêu | persistence metric counters | optimized 16 so với `microbatch_off` 20 begin/commit/copy calls; Γ_red=20% chỉ cho counter này | `p8-e5-microbatch_off-3aafb955dd42a600` | Không phải cải thiện toàn bộ SQL/storage/RSS/latency/throughput. |
| RAM dedup warm counter khác nhau | RAM dedup/repository counters | optimized 0 so với `ram_dedup_off` 10 warm begin/commit attempts; Γ_red=100% chỉ cho warm counter này | `p8-e5-ram_dedup_off-9252b4ecd604539a` | Không phải cải thiện toàn bộ SQL/storage/RSS/latency/throughput. |
| Fanout scan liệt kê đúng candidates dự kiến | `internal/telemetryws` fanout | raw indexed 20 so với scan 40; viewer counters đúng | `p8-e5-fanout_scan-0cadf405faabd3c8` | Helper activation INCONCLUSIVE: `fanout_viewer_readiness_unverified`; không công nhận scan arm là eligible. |
| Metric command-window còn thiếu | command timeline collector | `INCONCLUSIVE`, `counter_field_missing_events`, count null | cả sáu E5 reports | Initial timeline zeroes xảy ra trước command; không tuyên bố latency/zero samples bằng 0 hoặc thay bằng RTT. |
| Lát cắt DB recovery đã restore data và pending delivery | P8.13 recovery fixture, PostgreSQL restore và Gateway SQLite reopen | schema 24; 2.035 raw rows; row-count/content-hash source/restore bằng nhau; payload SQLite 341-byte đã commit/chưa ACK còn sau restart/restore; enqueue 1, application ACK 2, pending 0 | `p8-recovery` | Cùng fixture-owned DB cluster; roles/Auth/broker/keys/socket được giữ riêng. Chỉ DB restore, không phải backup toàn platform/HA. |

### Gate local cuối cùng hiện có

Kiểm tra source hiện được ghi nhận tại revision `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`, dirty SHA-256 `b73d86246e44e5d0c4afa6c46fc4c74459170b769cbe1e0bdbc95318aa168800`: `gofmt`, `go vet ./...`, `golangci-lint` 2.14.0, `go build ./...`, full Go race và 35 required test guards PASS; coverage 77,7% chỉ để tham khảo, không phải coverage gate. 106 native opt-in skip events vẫn là SKIP. Cross-build và paperload checks PASS. Selected Python CI guards/harnesses PASS, không phải full Python discovery; không cộng các test events chồng lặp. Lỗi Auth/migration readiness lịch sử vẫn được ghi có qualification trong bảng bằng chứng. Remote GitHub CI chưa chạy.

Lượt phục hồi cuối P8.13 có đủ tám phase PASS và cleanup chính xác; checksum dữ liệu source/restore trùng khớp, trạng thái simulator đã commit nhưng chưa ACK còn nguyên, và cả bốn child process đã join. Original control runner trước đó vẫn PARTIAL. Lượt phục hồi nhỏ thành công này không xóa bằng chứng lỗi trước đó hoặc mở rộng phạm vi restore.

## Chưa chạy / lịch sử được giữ nguyên

- Ma trận/repeats chính thức E1–E5 (≥3), high-load/capacity/soak, A/B trên máy vật lý từ xa, TTL wall-clock thực 600s, AM5728/HIL, cảm biến vật lý và live rollout: **NOT RUN**.
- Remote GitHub CI: **NOT RUN**. Các CI job legacy supervisor/reloader được hoãn: **DEFERRED**, không phải PASS. Không push để lấy remote CI.
- Không chạy full Python discovery tại source checkpoint cuối; failures/errors của discovery lịch sử vẫn là lịch sử. Nguyên nhân migration-readiness failure đầu tiên chưa biết. Auth failure ban đầu được xử lý bằng lượt retry; lỗi lịch sử vẫn được lưu.
- E5 fanout activation và command-window timeline trong bộ kết quả 29/10/2026 ở trên vẫn INCONCLUSIVE, không phải zero. Không chạy whole-run happy mocked E5 test; workload native thực đã chạy. Một pilot colocated và snapshot CPU/RAM thưa không phải bằng chứng capacity.
- Deployment settings giữ nguyên và false. Kết quả raw P7 và trạng thái raw P8 không bị viết lại. Không tuyên bố Stage 5 hoàn tất toàn bộ: board/HIL, full CI requirement/remote lane và live rollout còn là phụ thuộc.

---

## Phụ lục Checkpoint Helper CLI & E5 Ablation Matrix (31/10/2026)

**Trạng thái cập nhật (31/10/2026): `P8_E5_HELPER_PILOT_COMPLETE_QUALIFIED`.**
Hoàn tất việc chạy tuần tự độc lập cả 6 arm ma trận E5 qua quy trình A/B helper CLI (`paper_p8_server.py`, `paper_p8_client.py`, `native_compose_smoke.py`), cùng với việc chuẩn hóa bộ adapter đối chiếu báo cáo offline (`paper_p8_comparison.py`).

### 1. Dấu vân tay mã nguồn và đầu vào bất biến
- **Mã nguồn thực thi native của 6 lượt chạy:** Revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`, branch `feat/paper-requirement`, `dirty_source_sha256`: `9a1e11276048f6e2c88bc14949bdae3a3c4b2c11b4c1d51cfe4fb9fa61f5eff0`.
- **Mã nguồn công cụ adapter báo cáo offline sau khi sửa:** Revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`, `dirty_source_sha256`: `f3bf86d35e8e94658579dc0dc0bf8393973f6969a30157641227e98c3e985457` (không chạy lại native sau khi sửa adapter báo cáo).
- **Context ghim:** Seed 805, Dataset SHA-256 `2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175`, 35.684 bytes.
- **Binaries ghim:**
  - `paperload`: SHA-256 `f562ba9bfacbd351718d0c8b81d1fe77c337864265e5d324f706ea69d95cc63f`
  - `gateway`: SHA-256 `3be849df6522f437e26bbc2d978b84b828b4712c8d2ef7e19ce184eda435c247`
- **Tái sử dụng Image IDs:** 100% container image IDs từ arm `optimized` được tái sử dụng byte-for-byte qua cờ `--reference`, không rebuild bất kỳ container image nào.

### 2. Kết quả thực thi 6 Arm Ma trận Native E5
Mỗi arm đều hoàn tất đủ 3 phase (`initial` → `warm_replay` → `control`), server A và viewer holder B đều thoát mã 0, và dọn dẹp chính xác 12 containers / 1 network / 5 volumes per-run:

| Arm | Vai trò | Trạng thái | Báo cáo Artifact | SHA-256 Artifact | Đánh giá Kích hoạt Cơ chế |
|---|---|:---:|---|---|---|
| `optimized` | Reference | **PASS** | `p8-e5-optimized.json` | `3f6617d0e80cd913ad399cd13bf86c73dd8efec98e7ce18af89909994cabb876` | Tham chiếu cơ sở đầy đủ |
| `ram_dedup_off` | Ablation 1 | **PASS** | `p8-e5-ram_dedup_off.json` | `1d0fece293e012ec5aa546062097438a32c4dceff6fe9066a4ea3d206aec0661` | `ELIGIBLE` (`lookup_hit_total: 10`) |
| `fanout_scan` | Ablation 2 | **PASS** | `p8-e5-fanout_scan.json` | `1ca3528b241396170c5d61f86455643b3c1dfbf6f6ec81f64236d86f89fdaee1` | `ELIGIBLE` (`indexed: 0, scan: 40`) |
| `shaper_off` | Ablation 3 | **PASS** | `p8-e5-shaper_off.json` | `a88d2c95c6be0cf049eaba14febf60a61445c25d54acb64d26f06a122969868b` | `ELIGIBLE` (`ShaperRateLimited: 0` vs `24`) |
| `microbatch_off` | Ablation 4 | **PASS** | `p8-e5-microbatch_off.json` | `88ea478cfe0b2dc0c0feb7416139bab6d227e3b035109a8d9e0c65e9c02e881d` | `ELIGIBLE` (`groups: 0` vs `16`) |
| `polling` | Ablation 5 | **PASS** | `p8-e5-polling.json` | `01f58ccde39544221c50ca0d9f522abcd99feb5341eafe95fea43d96ee5f7473` | `ELIGIBLE` (`notifications: 0` vs `4`, `scans: 17`) |

### 3. Kết quả xuất bản Báo cáo Tổng kết (Sanitized Summary Export)
Bộ so sánh `scripts/paper_p8_comparison.py` xuất khẩu thành công tại `results/p8/helper-e5-20261031/`:
- `e5-summary.json` (SHA-256: `78047b28328411df029f7ce6559a5e6dd568ee15674264426b6d84e4e2c26a6c`): Báo cáo JSON đã kiểm tra an toàn, loại bỏ toàn bộ chuỗi mật khẩu, token và thông tin nhạy cảm.
- `e5-runs.csv` (SHA-256: `63e1daa0974c961a8897d6ce6b65c646914996430a00df078bcbe152576d8111`): Bảng dữ liệu 6 lượt chạy.
- `e5-pairs.csv` (SHA-256: `25bd5c8026360ba8773ea8c3fe91fa9f3ceb98b785fd6be2376f3e36e428de31`): Bảng đối chiếu 5 cặp ablation với `optimized`; cả 5 cặp đều đạt `comparability: COMPARABLE`.
- Bằng chứng lịch sử: Toàn bộ 9 lượt native FAIL trước đó được bảo toàn nguyên vẹn nhãn FAIL. Ranh giới tiêu chuẩn tiếp tục giữ `full_p8_accepted: false`, `physical_ab: NOT RUN`.

---

Xem [sổ tay công cụ tải P8](paper-p8-load-tools.md) để biết cách gọi lại, ngữ nghĩa abort/drain/cleanup, ràng buộc A/B an toàn và đề xuất ma trận chính thức. Các giới hạn chung được lưu trong [backlog trung tâm](../dynsec/limitations-and-backlog.md).

## Phụ lục — Khóa nguồn và đối chiếu bổ sung E1–E5 (BS.0, 05/11/2026)

Đây là snapshot khảo sát BS.0 tại checkout `development`, không phải lượt chạy mới; các KS rows dưới đây ghi baseline findings theo thời điểm khảo sát, còn trạng thái closeout mới nằm tại BS.9b phụ lục cuối và supplement checklist. HEAD khi snapshot là `cddb6019403719f2bf382ed531256d447bbefbe0`, branch `feat/paper-requirement`, tracked worktree sạch. Go `1.27.3 linux/amd64`; Docker `29.8.2`. Các image tag có sẵn gồm `eclipse-mosquitto:2.0.18`, `nginx:1.25-alpine`, `timescale/timescaledb:latest-pg16`, `supabase/gotrue:v2.196.0`, `supabase/storage-api:v1.74.0` và `development-backend:latest`; đây là inventory local, không khẳng định image nào đang chạy hay được dùng trong artifact lịch sử. Không có standalone `paperload`/`gateway` binary versioned ở `src/cmd/`; artifact E5 lịch sử có binary hashes riêng tại phụ lục 31/10. Manifest/fixture P8 dùng `schema_version: 1` (`scripts/tests/paper_p8_support.py`; remote contract tại `paper_p8_remote.py`); report recovery ghi fixture DB schema 24. `.env.example` giữ `WEBSOCKET_ENABLED`, `STAGE5_ENABLED`, `STAGE5_WAKEUP_ENABLED`, `MQTT_SHAPER_ENABLED`, `MQTT_MICROBATCH_ENABLED`, `MQTT_RAM_DEDUP_ENABLED=false`. Không đọc `.env`, credentials, hoặc trạng thái shared DB.

### Findings → phần kế thừa và việc còn lại

| Finding | Đối chiếu source/evidence và cách xử lý trong BS plan |
|---|---|
| KS-01 | `scripts/tests/paper_p8_e1_support.py`: `mixed_e1_mapping`/validator hiện giữ 64 viewers, 20 unique và expected counts cố định; Go manifest có cap rộng hơn. Nối các seam này và manifest/bundle/import ở BS.1/3; giữ pilot 64/20. |
| KS-02 | `paper_p8_e2_support.py`: `build_e2_execution_plan(profile='official', official_opt_in=...)` và `e2_official_estimate` đã tồn tại, nhưng plan execution còn giới hạn target/connection và server E2 mặc định dùng pilot (`paper_p8_server.py`, steady burst 24). BS.1/4 chỉ nối lifecycle/expected counts; không viết generator mới. |
| KS-03 | `equal_samples` hiện có functional PASS nhưng cặp lịch sử vẫn contaminated; giữ qualification. BS.2/4 hoàn thiện counters/windows cần thiết; retry không bị loại khỏi accounting. |
| KS-04 | E3 **đã có** server-origin timeline trong runbook 30/10: bốn profile có terminal histogram count=4 (xem `paper-p8-e3-helper-preparation.md` §3.1–3.6). Timeline runtime là `src/internal/metrics/control_timeline.go`; các E3 raw reports cũ ngày 29/10 vẫn null/unavailable. Không sửa timeline chỉ vì báo cáo cũ; BS.5 kiểm cửa sổ/count của lượt mới. |
| KS-05 | E4 có warm/cold/conflict/durable-reopen; `paper_p8_e4_support.py` vẫn cố định replay 10 và các validator/phase có hằng pilot. BS.6 mở dataset/replay theo plan; real TTL 600s vẫn NOT RUN. |
| KS-06 | E5 helper 31/10 có sáu native arm PASS, 5/5 cặp comparable và `activation: ELIGIBLE`, gồm fanout scan (`indexed: 0, scan: 40`); xem `results/p8/helper-e5-20261031/index.md`. Đây là evidence của run 31/10, không sửa nhãn INCONCLUSIVE của report/run 29/10. Adapter trong `paper_p8_e5_remote.py` và fixture còn ánh xạ `command_timeline` sang `events/retries`; BS.7 đọc schema histogram/counter thật, không thêm field giả. |
| KS-07 | Helper A/B, TLS/bundle và runbook đã có; physical A/B vẫn NOT RUN. BS.1/8/9 xác nhận hợp đồng cho profile được mở, phối hợp A/B thủ công vẫn đủ. |
| KS-08 | Readback lịch sử 64-session là worker_connections 1024, không phải config live hiện tại; sizing proxy cần downstream + upstream và reserve. BS.2/3 đọc limits hữu hiệu trước rung, không suy capacity từ cap. |
| KS-09 | Go workload wall tới 7.200s còn server session tới 3.600s; một số readiness/collector pilot caps tồn tại. BS.1/2 phải truyền ngân sách hữu hạn nhất quán; chưa chạy workload dài. |
| KS-10 | Có estimate `disk_growth_bytes` nhưng chưa thấy guard thực tăng dung lượng tương ứng. BS.2 bổ sung guard nhỏ cho tài nguyên fixture-owned; không chạm shared storage. |
| KS-11 | Một số readers giới hạn manifest 1 MiB; mapping/result/bundle cũng có bounds riêng. Đo size dry-run trước, chỉ mở cap có giới hạn nếu cần (BS.1/3); không coi rung lớn đã chạy. |
| KS-12 | Report chính có chronology E3/E5 cũ; phụ lục E3 30/10 và E5 31/10 qualified hơn. Đồng bộ diễn giải theo từng run/source; checklist independent-host/accounting vẫn mở đến khi thực hiện đúng phạm vi (BS.0/9). |

### Preset và phạm vi pilot của phần bổ sung

| Bài | Pilot giữ/kiểm trong lượt triển khai | Official preset cần hỗ trợ (không đồng nghĩa đã chạy) |
|---|---|---|
| E1 | 64 sockets, 2 principals, sparse/dense/overlap, dataset 20 unique; một rung parameterized nhỏ. | 32/64/128/512/1.000/2.000/5.000/10.000; user count tách socket count. |
| E2 | Profiles 100 và 1 sample/message, steady + burst/drain nhỏ, counts/ACK/pending. | Offered 100/1.000/5.000/20.000 msg/s theo rung chọn; official estimate/preflight, không high-load run. |
| E3 | 4–6 fresh commands, subset polling/wakeup với executor thật, timeline window; plan 30 lệnh offline. | 30 commands/arm dưới workload E2 được chọn. |
| E4 | 20 unique + 10 replay; warm/cold, conflict và durable reopen nhỏ. | 1.000 unique + 100 replay; expiry TTL 600s optional, dry-run không chờ TTL. |
| E5 | Reference + các arm bị ảnh hưởng; nếu path chung đổi thì một bounded matrix sáu arm; repeat plan dry-run. | Sáu arm, workload đại diện chọn trước, order/seed và tối thiểu ba independent repeats cho kết quả chính khi được chạy. |

Các official workload chỉ cần được generate/preflight qua contract an toàn; không chạy 5k/20k msg/s, 5k/10k viewers, TTL thực hoặc ba official repeats trong phạm vi bổ sung nếu chưa được giao. Nếu có remote pilot, physical A/B qualification phải được ghi riêng; cùng-host không thay thế. Deployment defaults vẫn false.

### Hướng BS.1 hẹp — seam hiện có, chưa triển khai

1. E1: mở count/expected recipients tại `mixed_e1_mapping` và role-manifest/validator của `paper_p8_e1_support.py`; giữ constructor/default pilot. Truyền count/density qua preparation → bundle metadata → B run → import projection, thay vì parameterize Go viewer trước khi Python contract chấp nhận.
2. E2: dùng execution-plan builder/estimator hiện có trong `paper_p8_e2_support.py`; điểm nghẽn hẹp là restriction `len(targets)==1`/credential count trong builder và đường `paper_p8_server.py` chọn `steady_burst` 24 messages. Mở profile/expected counts từ manifest có opt-in, giữ single-target pilot defaults đến khi workload yêu cầu thêm topology.
3. E3/E4/E5: không mở rộng BS.1 thành refactor riêng. Chỉ truyền workload metadata/profile qua runner/bundle seams hiện có; để command count, replay count và histogram adapter ở BS.5–7.

BS.0 kết luận: source/report đã được khóa và findings đều có đường xử lý hoặc evidence kế thừa xác định; chưa thay đổi runtime, chưa chạy integration, và BS.1 chưa bắt đầu. PASS/FAIL/PARTIAL/NOT RUN lịch sử ở phần trên không bị thay thế bởi phụ lục này.

## Phụ lục — BS.9b: kết luận checkpoint cuối local (05/11/2026)

Đây là closure tài liệu dựa trên việc đối chiếu trực tiếp report/checkpoint và
safe native artifact hiện hữu; không chạy lại integration/workload, không đổi code
hoặc cấu hình và không tuyên bố mọi lượt cùng một source tree. Mỗi bằng chứng
giữ revision/fingerprint riêng như ghi ở checkpoint tương ứng.

| Evidence | Kết quả đã kiểm tra | Phạm vi / qualification |
|---|---|---|
| Local technical checkpoint `checkpoint-local-20261105T062820Z-1068601` | Full Go race, formatting, vet, lint (0 issues), build và CI guard 41 required tests PASS; Python suite 981 tests tổng, trong đó 978 PASS / 3 SKIP. | Source HEAD `cddb601…`, dirty; không tính SKIP là PASS. |
| Refresh `checkpoint-refresh-20261105T074320Z-1191327` | Affected `paperload` race/static/build, ARMv7 cross-build cho gateway/paperload, CI guard 4 tests và consolidated Python 981 tests tổng: 978 PASS / 3 SKIP. | Dirty fingerprint `faba7a2e…`; refresh không sửa source. Không gộp fingerprint với checkpoint/native runs. |
| Native checkpoint `checkpoint-native-20261105T063844Z-1073531` | Full HTTP PASS, cleanup verified 10 containers / 1 network / 3 volumes; portable E3 official-opt-in low-load 3-command roundtrip PASS, cleanup verified 12 / 1 / 5. | E3 `generator_colocated=true`; command, DB/background/raw evidence đối chiếu trong handoff. Không phải physical A/B hay `full_p8_accepted`. Earlier failed attempts remain recorded. |
| E1 selected native 128-viewer `ncs_241a3c42e4d8-e1` | 128/128 viewers, 426/426 expected deliveries, 0 drop/duplicate/wrong namespace; 8 telemetry PUBACK/application ACK, 0 pending; DB delta 8 ledger / 800 raw / 8 temporal / 0 configuration; exact cleanup 12 / 1 / 5. | `generator_colocated=true`, `physical_ab=NOT RUN`, `full_p8_accepted=false`; content verification is bounded header/identity sampling, not full payload audit. |
| Prior source-qualified supplements | E3 30/10 timeline evidence and E5 six-arm helper matrix are recorded in their guides; E2 portable/native and E4 cache on/off 8+2 evidence are qualified per run/source in their guides/checkpoints. | Chronology matters: E3 timeline runs are 30/10 and E5 helper matrix is 31/10; November 5 E3/E5 runs address different selected seams and do not rewrite older runs. E5 Nov 5 representative one-repeat: six arm artifacts PASS/cleanup exact, but orchestration timed out; shaper activation remains inconclusive. |

### Kết luận và phần chưa nghiệm thu

BS.9b hoàn tất documentation/safe-results closure cho local tooling và checkpoint
theo policy MVP: normal paths, representative business/negative cases và affected
checks có evidence qualified; limitations được giữ. Đây không biến raw artifact sang
paper acceptance. Những lượt FAIL/PARTIAL/NOT RUN cũ không bị sửa nhãn. Không có
claim rằng các checkpoint Go/Python/native dùng cùng fingerprint hoặc cùng dirty tree.

Các cửa sổ evidence có sẵn: technical full Go race/static/build/41 CI guards,
  consolidated Python checkpoint/refresh 981 tổng (978 PASS / 3 SKIP); refresh affected Go + ARMv7; native full
HTTP; portable E3 3-command; E1 128-viewer; E4 8+2 cache on/off; E5 six-arm
representative one-repeat. Cửa sổ không có/không đủ gồm E1 5k/10k capacity, E2
official high rates/soak/clean equal-samples comparison, E3 official 30 native dưới tải
và burst overlap, E4 official native execution/import cùng real TTL 600s, E5 >=3
independent official repeats/đủ shaper activation, physical A/B, remote CI,
AM5728/HIL, physical sensors và live rollout. Giữ đúng trạng thái NOT RUN hoặc
INCONCLUSIVE tương ứng; không suy hiệu năng từ estimate/pilot.

Các giới hạn vận hành hiện tại: E1/E3 pilots colocated; snapshots tài nguyên hữu
hạn, không đại diện peak/RSS/capacity; E1 content audit chỉ sampled; E2 retry và
qualification theo profile; E4 real TTL và Γ_red cho comparison chưa được xác lập;
E5 shaper arm chưa eligible để tuyên bố lợi ích. Xem [handbook P8](paper-p8-load-tools.md),
runbooks E1–E5 liên kết trong handbook và [backlog canonical](../dynsec/limitations-and-backlog.md).
Không thay đổi deployment defaults, shared services hay raw evidence.

### Bổ sung BS.9b — E4 offline contract, guard và regression refresh (05/11/2026)

Artifact `e4-official-1000-100-expiry.stdout.json` là `VALIDATED_PLAN_ONLY`,
`native_executed=false`, `physical_ab=NOT RUN`. E4 official opt-in tạo plan 1.000
unique + 100 replay + expiry replay, journal bound 49,152,000 bytes (limit
67,108,864), TTL khai báo 600s và required token lifetime 1,164s. Bundle 4,922
bytes qua export/inspect/bind; 3 manifests được bind với profile official/session
đúng. Ba Go manifest preflight exit 0 trả `PREFLIGHT_ONLY_NOT_RUN`. Expected counts
là 1,000 unique ACK/batches, 100 replay ACK/batches, 100,000 unique raw samples,
10,000 replay payload samples và 0 replay-created raw/delivery. Result projection
schema-valid nhưng status `PARTIAL`, `native_completed_acceptance=false`; identities
là unsigned schema-only fixtures, không dùng trên mạng. Đây là bằng chứng portable
offline contract, không phải E4 native A/B run hoặc TTL execution.

Regression refresh sau các offline changes chạy consolidated Python command trên
source HEAD `cddb6019403719f2bf382ed531256d447bbefbe0`, dirty fingerprint
`d93edfa75e7283b51b40273906613108477cafd91391784ce1c75bf4dc5bc006`:
`983` tests tổng, 980 PASS / 3 SKIP. Private log/command/fingerprint được lưu
trong logical checkpoint `checkpoint-offline-closure-20261105T101500Z`.
Không chạy Go/native/integration sau offline-only changes. Python regression xác
nhận component SQLite pending outbox giữ một row với ID/payload/hash/bytes; guard
stop cho PARTIAL, child joined, artifact flushed; cleanup outcome riêng. Remote B
pending vẫn `NOT_VERIFIED`, không tuyên bố đã được xác minh. Budget regressions
nhận workload 7,200s khi session budget 8,100s và từ chối 8,000s; không yêu cầu
native DB-fill hai giờ theo MVP.

Γ_red được định nghĩa riêng cho từng counter C:
`100 × (C_reference − C_optimized) / C_reference`. C chỉ là counter delta cùng
ý nghĩa (ví dụ transaction begin/commit, COPY calls, hoặc repository calls), lấy
từ cùng process instance và cùng workload/window; nếu workload counts khác thì
chỉ so khi có denominator normalization theo cùng unique messages. Denominator
là counter thực của reference trong đúng cửa sổ, không phải số hàng ledger hoặc
estimate; nếu denominator bằng 0, thiếu/reset/process mismatch/window mismatch,
ghi `unavailable`, không gán 0%. Raw retention xác minh độc lập bằng raw row/sample
counts/content checks; không suy từ Γ_red. Hiện chưa có cặp official cùng-window
denominator đủ qualification cho E4, nên không công bố Γ_red headline. Real TTL
600s vẫn NOT RUN, không bắt buộc cho local tooling closure.

## Phụ lục — Tiến độ bổ sung từ runbook E1–E5 (05/11/2026)

Các kết quả dưới đây giữ nguyên phạm vi của từng lượt, không gộp các dirty source
khác nhau và không nâng `full_p8_accepted=false` thành nghiệm thu chính thức.

### E1 — tài nguyên fixture và pilot

Pilot 128 viewer dùng overlay riêng của fixture: `WS_MAX_SESSIONS=128`, reference
budget 8 MiB, Nginx 2 worker × 145 connection. Giới hạn nofile thực tế của Nginx,
backend và Mosquitto lần lượt 145, 224 và 98; cấu hình hiệu lực và FD được đọc lại
trước workload. Không thay đổi cấu hình triển khai hay sysctl toàn máy. Có 128/128
viewer subscribe trước telemetry; mapping hai principal, bốn target dự kiến
106/106/106/108 delivery, thực tế 426/426, không drop, trùng hay sai namespace.
Có 8 PUBACK và 8 application ACK, pending 0; DB tăng 8 ledger / 800 raw / 8
temporal / 0 configuration. Cleanup chính xác 12 container / 1 network / 5 volume.
Artifact: `/var/tmp/opencode/p8-final-20261105/e1-128-124609-1052818/artifacts/p8-e1-pilot.json`.
Readback trong `private/e1-resource-readback.json`, Compose hiệu lực trong
`private/effective.json` và Nginx config trong `private/nginx.conf` thuộc cùng
run root. Backend có 212 FD khả dụng/12 mở, broker 90/8; hàng đợi không tăng
theo số viewer. Raw identity chỉ được kiểm theo mẫu, không phải audit toàn bộ.

Pilot 32 viewer riêng đạt 20 unique, 20 PUBACK/application ACK, pending 0, DB
20/2.000/20/0 và 530 WebSocket delivery, không drop; cleanup 12/1/5. Artifact:
`/var/tmp/opencode/p8-supplement/e1/pilot-32-final/artifacts/p8-e1-pilot.json`.
Lượt này xác nhận đủ subscription ACK trước telemetry, readiness deadline 22s,
hai principal và bốn target; `WS_MAX_SESSIONS=64`, quota fixture 2 CPU / 4 GiB,
không cần overlay. Receipt nằm ở `private/resource-receipts.json`, outer report
ở `artifacts/run.json`; các lượt lỗi trước được giữ nguyên. Kiểm tra liên quan:
`python3 -m unittest scripts.tests.test_paper_p8_e1 scripts.tests.test_paper_p8_e1_support scripts.tests.test_paper_p8`.
Lệnh pilot: `python3 scripts/tests/native_compose_smoke.py --paper-p8-e1 --p8-e1-viewers 32 --prepare /var/tmp/opencode/p8-supplement/e1/pilot-32-final`.
Dry-run 5.000 và 10.000 viewer đã qua mapping, export/inspect/bind và Go preflight;
lần lượt dự kiến 41.665 và 83.330 delivery. Đây chỉ là kiểm tra offline với
identity dùng cho schema, không chứng minh capacity hay physical A/B.

Rung offline 5.000 có mapping 797.372 byte, viewer manifest 737.897 byte,
telemetry manifest 1.438 byte, inventory 739.618 byte, 8.333 filter và expected
delivery theo target 10.415/10.415/10.415/10.420. Rung 10.000 lần lượt là
1.594.019 / 1.474.527 / 1.438 / 1.476.248 byte, 16.666 filter và
20.835/20.830/20.830/20.835 delivery. Cả hai dùng hai principal và bốn target;
kích thước nằm dưới cap file 8 MiB và inventory 32 MiB. Fixture schema-only có
ramp 100/120 giây, measurement 130 giây, drain 30 giây, wall 168 giây,
readiness deadline 120 giây; token lifetime 50 giây trong lượt bind offline không
phải khuyến nghị cho lượt thực. Kết quả kiểm tra offline được ghi là 113 PASS, một SKIP;
bao gồm các trường hợp giới hạn WS/FD thấp và readback không khả dụng. Các số
ước tính không phải readback tài nguyên hay kết quả capacity.

### E2 — hai sample profile và portable lifecycle

Native pilot cùng host chạy lần lượt S=100 và S=1 trên một backend: mỗi profile
có 24 unique được tạo/đề nghị, 49 attempt/PUBACK, 25 retry, 24 application ACK
và pending 0. DB tăng lần lượt 24 ledger / 2.400 raw / 24 temporal và 24 / 24 /
24; configuration đều 0. Producer A có 8 unique, B có 16 unique và 25 retry;
shaper phân loại các retry này. Đối chiếu identity/raw hai Gateway không phát hiện
sai giá trị. Overlap được quan sát qua pipe là 3,57 ms và 0,83 ms, nhưng độ trễ
quan sát không biết nên không phải bằng chứng fairness. Cleanup 12/1/5. Artifact:
`/var/tmp/opencode/p8-supplement/e2/native-final-s1-20261105/artifacts/p8-e2-pilot.json`.

Snapshot đầu/cuối cùng `process_instance` ghi 23 commit cho mỗi profile, với
2.400 và 24 dòng mới tương ứng. PostgreSQL lần lượt 48,06→60,29 MiB / CPU
0,04→3,66% và 60,32→60,69 MiB / CPU 0,03→0,02%; đây là snapshot, không phải
giá trị đỉnh. Một số cửa sổ CPU tiến trình sinh tải không khả dụng, không gán 0.

Lượt portable S=100 cùng host đã qua export, bind, preflight, run, import và
finalize: `validation_valid=true`, `workload_outcome=VALIDATED`,
`cleanup_outcome=VERIFIED`. Guard A lưu 8 mẫu, guard B lưu 13 bản ghi, tăng
42.809 byte so với ngưỡng 16 MiB; không chứng minh threshold-stop hay A/B vật lý.
Artifact: `/var/tmp/opencode/p8-supplement/e2/native-final-portable-s100-fixed-20261105/`.
Lượt import đầu bị `e2_result_correlation_rejected` do khác hình dạng
`session.profile`; sau regression và sửa đối chiếu profile, lượt mới PASS. Các
lượt phase ID sai, import thất bại và root tạo sẵn vẫn được giữ nguyên.

Ở checkpoint đo đạc trung gian, stop budget của tiến trình thuộc fixture được
giới hạn 30 giây, độc lập với thời lượng workload; guard kiểm tra tăng trưởng
storage thuộc fixture và tách nguyên nhân dừng khỏi cleanup, pending chưa thu thập
vẫn `NOT_VERIFIED`. Counter persistence và CPU thiếu/reset không được coi là 0;
ledger rows không thay cho transaction counters. 86 kiểm tra E2 support PASS;
các plan S=100/1 tại 100, 1.000, 5.000 và 20.000 msg/s được kiểm tra offline với
deadline 1.200 giây. Deadline 900 giây bị từ chối vì cần 961 giây: đây là từ chối
preflight, không phải lỗi workload. Không chạy tải cao hay chứng minh so sánh
`equal_samples` sạch; ghi chú ban đầu cho rằng direct fixture cần network config là
nhầm lẫn lịch sử, không xóa kết quả lịch sử đó.

Chi tiết bổ sung của checkpoint E2: direct fixture tạo credential cô lập, dùng
P2/TLS và `HTTPSEndpoint`, không đọc `.env` deployment. Producer A có 8 attempt,
8 PUBACK/ACK và không retry; B có 41 attempt/PUBACK, 16 ACK và 25 retry. Không
có publish failure, ACK queue drop, ACK không khớp, duplicate đã commit hoặc
pending còn lại. Mỗi profile có 15 cửa sổ CPU với tập tiến trình ổn định; RSS
được giới hạn theo tập tiến trình, không phải RSS đỉnh của host. S=1 bắt đầu từ
snapshot cuối của S=100, không khởi backend mới.

Lượt portable lỗi phase ID và lỗi correlation nằm tại
`/var/tmp/opencode/p8-supplement/e2/native-final-portable-s100-final-20261105/`;
importer sau sửa chấp nhận đúng schema cũ hoặc đúng session có profile đã chuẩn
bị, vẫn từ chối sai profile/provenance. Ở thời điểm BS.8c đầu tiên, A-side guard,
kiểm tra hạn JWT thực và phục hồi timeout driver E5 còn chưa được xác minh;
đây là trạng thái lịch sử, không phủ nhận bằng chứng follow-up ở BS.9b. Guard A
có ngưỡng tăng trưởng và dự trữ trống mặc định 64 MiB, chu kỳ một giây, tối đa
128 mẫu; budget dương nhỏ hơn trong manifest được ưu tiên. Guard không thể ra
lệnh dừng B từ xa; native threshold-stop vẫn chưa được nghiệm thu. Snapshot
cuối lấy sau stop/drain, không chứng minh mọi commit thuộc steady window.
Stop budget không được tăng vượt cap 300 giây của bundle để giả làm thời gian
producer; pilot telemetry này không chứng minh Gateway outbox/restart bền vững.

### E3 — subset ba lệnh và chẩn đoán probe

Pilot polling `low_load/steady` ba lệnh PASS: ba command `succeeded`, background
12 ledger / 1.200 raw / 12 temporal, command 24 raw / 16 temporal; SQLite,
REST và PostgreSQL receipts khớp, executor pending 0. Timeline cùng process có
3/3 dispatch trung bình 0,4244 giây và 3/3 E2E succeeded trung bình 0,4662 giây;
missing, overflow và pending origin đều 0; cleanup 12/1/5. Lượt mixed hai lệnh
trước đó vẫn FAIL `no_command_burst_overlap`.

Lượt wakeup trước sửa có ba lệnh chính hoàn tất nhưng vẫn FAIL vì probe được admit
trước khi executor subscribe; PUBACK không chứng minh executor đã nhận hay thực
thi. Chẩn đoán ghi command/outbox `published`, 4 PUBACK nhưng chỉ 3 result được
chấp nhận. Helper sau đó tách kiểm tra admission/replay offline khỏi kiểm tra
delivery online sau `READY_MARKER`/SUBACK; 48 regression tests PASS. Pilot wakeup
sau sửa PASS, ba dispatch trung bình 0,0609 giây, ba E2E succeeded trung bình
0,0973 giây, pending/missing/overflow 0; cleanup 12/1/5. Offline admission không
được gọi là delivery/success. Các lượt chẩn đoán và validation FAIL vẫn được giữ.
Artifact PASS: `/var/tmp/opencode/p8-supplement/e3/wakeup-probe-fixed-20261105-064356/`.

Trong pilot polling, Sensor A nhận hai receipt với desired version 1→2, Sensor B
nhận một receipt version 1; ordinal 3 bắt đầu sau khi ordinal 1 cùng Sensor
terminal. Lượt wakeup trước sửa tuy primary stream đạt 3/3 (dispatch trung bình
0,1158s, E2E 0,1785s; command 24 raw / 15 temporal), vẫn FAIL do probe chưa được
thực thi. Chẩn đoán bổ sung tại
`/var/tmp/opencode/p8-supplement/e3/wakeup-diagnostic-20261105-062841/` xác nhận
desired version 3, interval 250000µs, 4 `enqueued`, 4 `puback`, 4 notification
được chấp nhận và không có publish/scan/queue failure; result collector chỉ
nhận 3. Hai lượt `...-062152/`, `...-062528/` không đủ evidence vì lỗi truy vấn
nhưng cleanup đã xác minh. Không kết luận lỗi wakeup backend.

Sau sửa, offline admission/replay trả 202/202, busy/stale 409/409 và terminal
`published`; online apply trả 202, no-op/replay 200/200, replay byte-identical và
không đổi ledger/raw/temporal. Pilot wakeup có command 23 raw / 14 temporal.
Lệnh regression là `python3 -m unittest scripts.tests.test_paper_p8_e3_fixture
scripts.tests.test_paper_p8_e3_support scripts.tests.test_native_p8_e3_selector`.
Lượt `/var/tmp/opencode/p8-supplement/e3/wakeup-probe-fixed-20261105-063904/`
trước đó FAIL validation vì gate cleanup vẫn đòi sáu loại probe cũ; cleanup đã
xác minh, kết quả này không bị đổi thành PASS.

Plan official 30 lệnh đã qua bundle export/inspect/bind và ba Go preflight offline;
control dùng profile `official`, telemetry nền giữ `pilot`. Snapshot projection
`PARTIAL` không phải completed import. Sau đó portable official ba lệnh thực sự
đã PASS như bảng BS.9b ở trên; không nhầm với official 30 lệnh native vẫn NOT RUN.
Lỗi `manifest_contract_rejected` trước đó xuất phát từ việc thiếu
`publish_timeout_ms` bắt buộc của E2; builder bổ sung 3000 ms, không nới Go
validation. Kết quả plan lưu tại
`/var/tmp/opencode/p8-final-20261105/e3/official30-dry-run.json`.

### E4 — kiểm định counts và replay

Lỗi helper ban đầu là unique completion tính theo duration × rate thay vì số
journal, replay còn cố định 10 và cleanup validator còn cố định 20/10. Regression
Go ban đầu ghi 9 scheduled / 8 generated / 1 deferred, sau sửa PASS; không nới
điều kiện viewer hoặc transport. Cache-on 8+2 PASS: unique 8 ACK, một ACK hợp lệ
bị cố ý bỏ qua để thử retry, 9 PUBACK, pending 0; replay 2 ACK, DB fingerprint
không đổi, 8 WS event mới ở unique và 0 ở replay. Cold replay, conflict và
durable reopen PASS; cleanup 13/1/5. Cache-off 8+2 có cùng DB delta 8/800/8/0,
replay không đổi DB, conflict và durable reopen PASS, cold không áp dụng; cleanup
12/1/5. Các lượt import, workload, replay schedule và report validation FAIL
trước đó được giữ tại `/var/tmp/opencode/p8-supplement/e4/`.

Plan offline 1.000+100+expiry và giới hạn TTL/Γ_red được ghi chi tiết tại phụ lục
BS.9b phía trên; real TTL 600 giây và official native vẫn NOT RUN.

Chi tiết chẩn đoán: viewer manifest có `measure_seconds=40`, còn unique pilot
4 giây × 5 msg/s mặc dù journal chỉ có 8 record; sau 8 record producer vẫn
đợi đủ rate × duration và trả `telemetry_generation_incomplete`, dẫn tới
`runtime_incomplete`/`measure_failed` thứ cấp dù đã thấy 8 event. Regression
`go test ./internal/paperload -run '^TestE4UniqueOpenLoopStopsAtParameterizedJournalCount$' -count=1`
đã RED rồi GREEN. Lượt cache-on ghi 8 generated/offered/ACK, một ACK hợp lệ bị bỏ
qua, 9 PUBACK; hai viewer nhận 8 event không duplicate. Lượt cache-off giữ cold
`NOT APPLICABLE`. Hai artifact thành công nằm tại
`/var/tmp/opencode/p8-supplement/e4/on-8-2-final/` và
`/var/tmp/opencode/p8-supplement/e4/off-8-2-final/`.
Các artifact lỗi giữ nguyên: import fallback `/on-8-2/`, workload
`/on-8-2-fixed/` và `/off-8-2/`, replay schedule `/on-8-2-fixed4/`, report
validation `/on-8-2-fixed5/`, cùng dưới thư mục `/var/tmp/opencode/p8-supplement/e4/`.
Hai lượt native dùng `scripts/tests/native_compose_smoke.py --paper-p8-e4 on|off
--p8-e4-unique-count 8 --p8-e4-replay-count 2` với `--prepare` lần lượt trỏ
vào hai thư mục `on-8-2-final` và `off-8-2-final` nêu trên.

### E5 — representative một repeat và timeout

Lượt representative đầu, seed 805, FAIL/incomplete tại `warm_replay` arm
`optimized` với `e5_runner_nonzero_warm_replay`; initial không có readiness hoặc
bằng chứng application ACK, control và năm arm còn lại NOT RUN. Cleanup 12/1/5
được xác minh; không suy ra delivery từ pending 0. Artifact:
`/var/tmp/opencode/p8-supplement/e5/native-representative-20261105T013300Z-4069/`.
Regression liên quan có 144 tests, trong đó một SKIP.

Sau khi sửa lịch replay 10 cơ hội không khớp journal prefix 2 và validator còn
cố định 20/10, một repeat mới có sáu artifact arm PASS và cleanup 12/1/5 mỗi arm;
driver tổng vượt timeout 900 giây, không tạo `driver-run.json`, nên chỉ đối chiếu
và xuất summary từ sáu artifact riêng. Source revision `cddb6019403719f2bf382ed531256d447bbefbe0`,
dirty hash `a7b3662f3851da395c70c291a6d615bc2a29138237073899880db38713d1001e`.
Hai lượt dùng `scripts/paper_p8_e5.py --mode pilot --repeat 1 --workload-preset
representative --seed 805 --arms optimized shaper_off microbatch_off
ram_dedup_off polling fanout_scan --run`, với `--root` là thư mục artifact của
từng lượt được nêu tại đây.
Mỗi arm xử lý 8 batch × 100 samples, warm replay 2, bốn command receipt, hai
configuration và hai viewer; DB initial 8 ledger / 800 raw / 8 temporal, replay không tăng DB.
Microbatch-off có 8 so với 4 operation counter ở optimized (Γ 50%); RAM dedup-off
có 2 so với 0 warm transaction (Γ 100%). Shaper không rate-limit ở baseline nên
cặp INCONCLUSIVE; polling và fanout đủ bằng chứng cơ chế nhưng benefit unavailable.
Command window có 4/4 dispatch và result; resource snapshot thưa không phải peak.
Artifact: `/var/tmp/opencode/p8-supplement/e5/native-representative-fixed-20261105T015509Z-6334/`.
Đây là một repeat colocated, không phải ít nhất ba repeat chính thức hay capacity.

Lượt FAIL đầu có context SHA-256
`da7b1acf4b145d18c52b96c25c30a63298e20c597182899e5944397c50ad5644` và
workload-selection SHA-256
`9b7d628b2e348b7b9526c24b3fcbe90edd88ce7ab42e597bdd9d970bf1532cd7`;
dirty-source hash không có trong artifact. Initial child báo COMPLETE/exit 0
nhưng `ready=false`, warm child PARTIAL/exit 1; initial/warm raw, ACK và các
cửa sổ tài nguyên không có đủ bằng chứng. Lượt sau dùng workload SHA-256
`7477e6386ea89cf50d0cecb0aa1200ce5bbd257553faf7e8feeefa143f91304b` và
context SHA-256 `90f822e0a4d840ea4439d71d5babc0053fe3742bb4af3137f7b3c180c82e9a3b`.
Hash binary, image ID và journal giống nhau trên sáu arm của lượt này.

Chi tiết sáu arm: optimized có 4 microbatch groups / 8 members, 2 RAM dedup hit,
8 indexed fanout và 0 shaper rate-limit. `shaper_off` tắt shaper nhưng baseline
cũng không rate-limit; `microbatch_off` có 0 group, 8 thay vì 4 COPY/commit/begin;
`ram_dedup_off` có 0 hit và 2 thay vì 0 warm attempt/commit/transaction;
`polling` có 0 wakeup signal; `fanout_scan` có 16 scan / 0 indexed so với 8 / 0
của optimized. Cả hai arm fanout nhận 8 event qua 2 viewer. Summary sau adapter
là `PILOT_RUNS_COMPLETE_QUALIFIED`, `run_count=6`, `independent_repeats=1`;
readiness của hai viewer đã đối chiếu. Cửa sổ command là từ trước control tới
sau drain result; chỉ có 8 mẫu tài nguyên initial và 12 mẫu warm mỗi arm,
không có cửa sổ tài nguyên control.

Driver hiện tách nhóm tiến trình, gửi SIGINT khi timeout/ngắt rồi mới nâng cấp
SIGTERM/SIGKILL nếu cần; nguyên nhân chính và cleanup được ghi riêng. Không coi
cleanup đã xác minh chỉ vì tiến trình bị cưỡng chế dừng. Regression dùng child
thật kiểm tra dừng êm, join và escalation; không hồi tố thay đổi artifact cũ.
Kiểm tra tập trung dùng `PYTHONPATH=scripts/tests python3 -m unittest test_paper_p8_e5_driver`;
đây không phải bằng chứng nghiệm thu một arm E5.

## Bổ sung — Sửa ngân sách FD Nginx và pilot E1 1.000 viewer (05/11/2026)

Lượt A/B do operator chạy trước sửa có 825/1.000 viewer connected/subscribed,
175 `connect_failed`; telemetry chưa được khởi động vì không đạt readiness.
Log Nginx A trong cùng cửa sổ ghi 175 `socket() failed (24: No file descriptors
available)` và 365 `accept4() failed (24: No file descriptors available)`.
Overlay cũ chia 2.034 connection slots cho 16 worker, rồi đặt cả
`worker_connections` và nofile mỗi worker bằng 128; thiếu dự phòng FD nội bộ
và phân bố kết nối không đều. Lượt đó giữ nguyên FAIL, không thay đổi raw evidence.

Overlay mới tách connection budget và FD budget: `base = ceil(total_slots / W)`,
`C = max(256, 2 * base + 32)`, `nofile = max(1024, C + 128)`. Gate đọc cả
master và từng worker thực, từ chối thiếu worker/thiếu giới hạn hoặc FD khả dụng;
chỉ master limit cao không còn đủ điều kiện PASS. Queue/reference budgets và
deployment defaults không thay đổi. Công thức là sizing hữu hạn để kiểm chứng,
không cam kết mọi phân bố kết nối hoặc mọi rung tải đều hoạt động.

Pilot native mới `native1000-gyk_ajud` dùng source `9d00d08…` cùng dirty changes
của bản sửa; chạy colocated, profile official opt-in, hai principal, bốn target,
5 message/target. Readback: 16 worker, C=288, nofile 1.024 ở mọi worker; FD còn
lại thấp nhất trước workload là 999. Viewer `COMPLETE`: 1.000 attempt,
1.000 connected/subscribed/receiving, không duplicate/sai namespace hay close
reason lỗi. Telemetry: 20 generated/offered, 20 PUBACK/application ACK, pending
0 message / 0 byte. DB delta 20 ledger / 2.000 raw / 20 temporal / 0 configuration;
WebSocket 8.330 enqueued/completed writes khớp expected 8.330, drops 0.
Validation PASS, runner COMPLETE không primary/cleanup error; cleanup đã xác minh
12 containers / 1 network / 5 volumes. Artifact:
`/var/tmp/opencode/p8-e1-fd-fix/native1000-gyk_ajud/fixture/artifacts/p8-e1-pilot.json`.
Đây không phải lượt chạy hai máy của operator, capacity/soak hay full acceptance;
`physical_ab=NOT RUN`, `full_p8_accepted=false` giữ nguyên.

Regression trước sửa tái hiện gate chấp nhận overlay 128/128 và thiếu worker
readback; sau sửa bộ affected có 382 tests tổng (381 PASS / 1 SKIP), bộ shared
readback E2/E3/E4 có 162 tests tổng (161 PASS / 1 SKIP). Các lệnh và log ở
`/var/tmp/opencode/p8-e1-fd-fix/`. Một lần gọi native ban đầu dùng nhầm metadata
deadline của hold lane bị argparse từ chối trước khi tạo fixture; giữ log riêng
`native1000-hkmwvbhf/native.log`, không tính là workload FAIL hay PASS.

## E1R.7 — Sửa luồng large-viewer

Checkpoint: 06/11/2026.

Đợt E1R.0–E1R.6 nối lại sáu defect trong execution path E1, giữ nguyên opt-in,
resource gates, giới hạn backend và quy trình fixture. Nguồn nền là branch
`feat/paper-requirement`, HEAD `c025dd6706ce31df86d13121d089e52277ceb771`, dirty.
Artifact giữ source qualification riêng: technical/Go/offline gate ghi nhận branch
`feat/paper-requirement`, HEAD `c025dd6706ce31df86d13121d089e52277ceb771` với dirty
source inventory; native 2.000 report ghi cùng revision và `generator_colocated=true`;
portable64 report ghi cùng revision, `dirty_worktree=true`. Không có một dirty
fingerprint chung được xác nhận cho các lane này, vì vậy không gán chúng về cùng
một dirty tree. Native 2.000 được chạy trước sửa riêng held-callback regression;
portable64 sau đó bao gồm regression/fix đó. Kết quả tại đây là local evidence,
không phải release provenance hay P8 acceptance.

### Sáu defect đã sửa

1. Manifest viewer tính ramp/readiness/wall nhất quán cho ba rung lớn, thay wall
   ngắn cũ không bao phủ ramp.
2. Readiness budget E1 được truyền qua execution plan, run-spec, bundle export/bind
   và spec reader; chỉ E1 viewer manifest hợp lệ mới vượt cap 120 giây.
3. Bundle lifetime suy từ actual role manifests, dùng tổng bảo thủ wall các role
   chạy staggered; session/token guard bao gồm lifecycle và collector A yêu cầu
   remaining session đủ lifecycle + 60 giây copy/import + 30 giây cleanup reserve.
4. Viewer receive counters được đặt tường minh, hữu hạn, theo expected deliveries
   và 8.192 bytes/delivery; overflow vẫn bị từ chối, không clamp. Đây là receive
   accounting bound, không phải RSS, allocated memory hay wire-byte bound.
5. Go viewer runtime thực thi ramp theo lịch viewer xác định được, giữ concurrency
   bounded và cancellation/join; ready vẫn phụ thuộc subscribe barrier. Regression
   xác nhận held callback không thoát/join giả.
6. Offline contract và prepare thật dùng chung execution plan; role preflights
   kiểm tra hợp đồng runtime đồng nhất thay vì phase override riêng che lỗi.

### Bằng chứng đã kiểm tra

| Lượt | Bằng chứng và kết quả | Qualification |
|---|---|---|
| E1R.6 final `e1r6-final-20261106T220000Z-1510609` | 770 Python tests: 768 PASS, 2 SKIP, 0 FAIL/ERROR; `git diff --check` PASS. | Kết quả theo `final-gate-report.txt`; không cộng với các lượt test khác. |
| E1R.6a Go/offline `e1r6a-20261105T192230Z-1464362` | Go race `./internal/paperload ./cmd/paperload` và `./internal/telemetryws -run Frame`, gofmt, `go vet ./...`, golangci-lint (0 issues), `go build ./...` đều PASS. Offline report xác nhận ba rung 2.000/5.000/10.000 `VALIDATED_PLAN_ONLY`, mỗi rung có viewer và telemetry Go role preflight exit 0, `PREFLIGHT_ONLY_NOT_RUN` (sáu role preflights tổng). | Chỉ schema/manifest/preflight; không có workload/capacity claim. |
| E1R.6b native2.000 `e1r6b-native2000-20261105T192848Z-1466818` | 2.000/2.000 viewer; 16.665/16.665 delivery; 20 application ACK; DB delta 20 ledger / 2.000 raw / 20 temporal / 0 configuration; cleanup verified 12 container / 1 network / 5 volume. | Colocated finite native run. Chạy trước held-callback fix; không dùng làm bằng chứng cho fix đó. `full_p8_accepted=false`, `physical_ab=NOT RUN`. |
| E1R.6c portable64 `e1r6c-final-20261106T214200Z-2` | 530/530 delivery; collector 0 errors; observed delay 63.000082811 giây, lớn hơn bound cũ 62 giây; session finalized `CLOSED`, cleanup `VERIFIED` (12/1/5). | Roundtrip xác minh importer/collector sau fix, không phải capacity test. `full_p8_accepted=false`, `physical_ab=NOT RUN`. |

Go gate và ba rung offline giữ source/dirty context theo từng artifact; native
2.000 và portable64 cũng giữ qualification riêng theo raw reports. Không suy diễn
rằng tất cả artifact dùng một dirty tree/fingerprint giống nhau.

Các lỗi lịch sử vẫn được giữ: lượt `e1r6c-portable-20261106T193902Z-1480621`
phát hiện portable loader bug, sau đó được xác nhận bằng regression rồi sửa;
lỗi `FileExists` và `SameFile` xảy ra trước workload trong driver orchestration,
không phải kết quả capacity, và không được đổi nhãn thành PASS. Những lượt FAIL
này không bị gộp vào kết quả thành công bên trên.

**NOT RUN / giới hạn:** native 5.000 và 10.000; physical A/B; capacity/soak; HIL;
remote/full final CI. Mọi raw artifact vẫn `full_p8_accepted=false`. Backend
reference ceiling 8 MiB không thay đổi. Không mở P9/Stage 6.

Runbook vận hành đã được cập nhật tại [P8 E1 remote gate](paper-p8-e1-remote-gate.md).

---

<a id="r5b-checkpoint-2026-11-06"></a>
## Checkpoint R0–R5 — Sửa luồng portable E2/E4/E5 (2026-11-06)

**Qualification:** Đây là bằng chứng portable có giới hạn, đã source-qualify sau khi review final gates; không khẳng định mọi lượt dùng cùng một dirty worktree cuối. Raw artifacts giữ fingerprint/cấu hình riêng và `full_p8_accepted=false`. Không chạy full CI, HIL, physical A/B, TTL expiry 600s, capacity/soak hay rollout.

| Mốc | Kết quả / bằng chứng | Ranh giới |
|---|---|---|
| R0 — Baseline/contracts | Đối chiếu artifacts E2/E4/E5, hợp đồng CLI và lịch sử source/failure trước khi cập nhật tài liệu. | Không sửa generated report; giữ nguyên nhãn FAIL/PARTIAL/NOT RUN lịch sử. |
| R1 — Official opt-in E2/E4 | E2 S=100 và S=1, 100 messages/s tổng trong 1s: cả hai PASS, serve exit 0, CLOSED, cleanup chính xác 12/1/5. S=100: 100 application ACK, 10.000 raw, 100 ledger, 100 temporal, 0 config; 154 attempts/PUBACK gồm 54 duplicate retries. S=1: 100 ACK, 100 raw, 100 ledger, 100 temporal, pending 0. E4 official opt-in 1000/100 cache-on/off cũng PASS; unique→warm-replay portable lifecycle hoàn tất, serve exit 0/CLOSED và cleanup verified. | E2 là hai profile hữu hạn, không phải equal-sample speedup/capacity/wire-rate claim. E4 tại đây chỉ là warm-subset lifecycle; cold/expiry/conflict/durable acceptance chưa được chứng minh. Artifact roots tương đối: E2 `e2-actual-s100-attempt9`, `e2-actual-s1-attempt1`; E4 `e4-official-on-1000-100-r5a-final`, `e4-official-off-1000-100-r5a-final`. |
| R2 — E4 dynamic counts | Cập nhật client/runner để official counts được xử lý động theo manifest/context; phase import đã validated trong các lượt liên quan. | Giữ nguyên giới hạn schedule, pending cap và sampling raw; không suy rộng sang các pha E4 khác. |
| R3 — E5 representative portable | Cặp `arm-optimized-final3` / `arm-ram-dedup-off-final`: cả hai PASS; initial 8 ACK / 8 ledger / 800 raw / 8 temporal / 0 config; warm 2 ACK, không tăng DB/WS; control 4 receipts / 2 configurations trên hai Gateway SQLite executors; viewers 8 events, 4 mỗi viewer; planned stop/join 0; serve exit 0, CLOSED/VALIDATED/VERIFIED, cleanup 12/1/5. Parent comparability: `comparable=true`, chỉ khác `MQTT_RAM_DEDUP_ENABLED`. | Cùng fingerprint `85e5c9ca…`, binary/context/dataset/images/quota; session metadata là representative nhưng role manifests giữ pilot. Chỉ là cặp hai arm, không phải sáu-arm repeat/performance claim. Artifact root tương đối: `e5-context-pilot-representative-final/`. |
| R4 — Repeat aggregate CLI | `e5-aggregate` nhận 3–10 summary đủ sáu arm, có cấu hình/provenance hợp lệ; xuất JSON/CSV. | Không aggregate ba native repeat nào trong lượt này. CLI kiểm tra cấu trúc/digest, không phải cryptographic attestation thực thi. Cặp hai arm không phải repeat đủ sáu arm; ba component fixtures là component proof, không phải ba native experiments. |
| R5 — Native verification, final gates và docs | Final gates xác nhận các run đã ghi nhận; E4/E5 evidence và accounting được giữ ở R1/R3. Fingerprint E4 `8afcd4…` giống on/off nhưng expiry-budget adjustment sau đó nghĩa là không tuyên bố cùng final tree. | Raw identity proof E4 lấy mẫu 10 IDs mỗi sensor, không full raw enumeration; chỉ unique→warm-replay lifecycle PASS, chưa nghiệm thu cold/expiry/conflict/durable. Không có latency-benefit claim. Xem thêm phần final-gate outcome bên dưới. |

Accounting E4 ở cả hai lượt: UNIQUE có 1.000 application ACK, DB delta 1.000 ledger / 100.000 raw / 1.000 temporal / 0 configuration; REPLAY có 100 ACK, DB và WS delta bằng 0, pending messages/bytes bằng 0. Cache ON ghi nhận 100 repository calls avoided; cache OFF ghi nhận 100 duplicate DB commits. Schedule giữ 5 messages/s (200s UNIQUE, 20s REPLAY), pending cap 64; journal/disk budget dẫn xuất là 49.152.000 bytes, không vượt cap 64 MiB. Đây không phải so sánh lợi ích hiệu năng.

Các artifact roots tương đối trong bảng nằm dưới protected evidence root `p8-client-blockers-r5a-20261106T044511Z-1648640`; không chuyển chúng thành bằng chứng cùng source bằng cách sửa manifest hoặc report.

### Lịch sử lỗi và kết quả final gates (R5)

- E5 `e5_initial_database_delta_mismatch`: baseline trước/sau DB không được lưu, nguyên nhân **chưa biết**; không quy cho lỗi fingerprint local-b về sau. Lỗi raw-scope verifier, expected viewer count cố định 20 và fallback fingerprint `local-b` là các lỗi riêng; các lượt FAIL vẫn được giữ.
- E4 250 msg/s FAIL do pending cap 64 được giữ. Lượt 100/10 trước đó có **phase import validated**, nhưng outer report bị từ chối bởi kiểm tra cố định 20/10; server exit 1. Đây là lỗi outer report, không phải phase-import failure. Những lỗi này không bị đổi nhãn bởi lượt 1000/100 PASS.
- Python consolidated affected suite: 742 tests, 1 failure, 3 skips. Failure `test_cleanup_failure_deadline_with_preceding_http_failure`: expected `cleanup_step=check_deadline`, observed `remove_resource`. Test riêng lẻ PASS; module `native_compose_smoke` rerun 220 PASS; supplemental remote module 6 PASS. Các lượt chồng lặp không cộng thành tổng mới; consolidated suite vẫn có failure.
- Go format, race (`internal/paperload`, `cmd/paperload`, `internal/gatewaysim`, `cmd/gateway`), vet, golangci-lint (0 issues), build, ARMv7 Gateway build, Python AST và `git diff --check`: PASS. Không chạy full CI/native rerun trong final-gate verification.

Chi tiết nằm trong protected R5a handoff; artifact roots tương đối được nêu theo từng mốc. Final-gate evidence root: `p8-client-blockers-final-gates-20261106T000000Z`. R5b chỉ cập nhật tài liệu, không sửa raw/generated reports hay deployment defaults.

## E2R — Handoff dừng tại N1 (2026-11-07)

### Sửa retry/diagnostics sau đối chiếu N1

N1 giữ nguyên kết quả native `ncs_58056e3cbc51`: `PARTIAL` rồi `ABORTED`,
`full_p8_accepted=false`; cleanup chính xác 12 containers / 1 network / 5
volumes. Không chạy lại native hoặc N2/N3/N4. Artifact chẩn đoán chính là
`p8-e2-diagnostics.json`; workload A/B, outer result và biên lai cleanup nằm
ngoài Git tại
`/var/tmp/opencode/p8-n1-20261107T190000-new/attempt-n1-01/fixture/artifacts/`
(xem thêm audit đối chiếu
`/var/tmp/opencode/p8-n1-audit-20261107/reconciliation.md`). Native dùng revision
`a7e66ea5f395334e47cf1da626728071627403df` với dirty worktree. Sau các sửa cục
bộ, source hiện tại khác source native N1; các sửa cuối không được chạy native.
Fingerprint checkout tại lúc bàn giao tài liệu: branch `feat/paper-requirement`,
cùng revision `a7e66ea5f395334e47cf1da626728071627403df`, dirty. Không gán
fingerprint native cho source sau local fixes; dirty fingerprint mới được wire
vào diagnostics nhưng final source chưa được chạy native.

Follow-up source cục bộ đã sửa/kiểm tra lỗi standalone CLI export, ánh xạ sáu
histogram ingress theo DTO `HistogramSnapshot` thật, và nối fingerprint dirty
source. RED/GREEN và kiểm tra đã ghi tại
`/var/tmp/opencode/p8-e2-retry-repair/fix-followup-20261107/handoff.md` và
`review-follow-up.md`: exporter subprocess với `PYTHONPATH` unset hỗ trợ kết quả
PARTIAL nhưng không nâng acceptance; regression diagnostics/schema/fingerprint
và `python3 -m unittest scripts.tests.test_paper_p8_e2_support scripts.tests.test_paper_p8_client`
(179 tests, 1 skip) PASS; riêng client 74 PASS, 1 skip. Các bước này chỉ xác
nhận source/local tests. Lỗi export lịch sử N1 vẫn là FAIL trong native record;
histogram cũng không có trong N1 diagnostic, không được bổ sung hồi tố.

Parent kiểm tra lại source sau local fixes: selected Python regression gồm
runner/client/bundle/E2/supervisor/remote/shared collector/E4/E5/native-smoke chạy
634 tests, 2 skips, không failure/error; skips không tính PASS. Go race
`./internal/paperload ./cmd/paperload`, `go vet ./...`, golangci-lint (0 issues),
`go build ./...`, gofmt và `git diff --check` PASS. Đây là các kiểm tra cục bộ,
không full CI hoặc native rerun; không cộng các suite overlap với lịch sử.
Log parent: `/var/tmp/opencode/p8-e2-retry-repair/parent-final-WI7Mjl/`.
Checklist E2R.0/2/3/4/6 đã đóng ở phạm vi code/local verification và bàn giao N1;
E2R.1 còn qualification native sau sửa histogram/provenance, E2R.5 còn các lane
người dùng tự chạy. Không coi toàn bộ kế hoạch đã hoàn tất.

### Workload, counters và diễn giải

N1 là E2 official `steady_burst`, S=100, tổng offered 1.000 msg/s trong 10s,
burst 0; A/B mỗi bên 5.000 unique (10.000 tổng), không phải capacity test.
Pending cap thực chạy: 8.192 messages / 8 MiB mỗi producer. Retry policy opt-in:
`bounded_backoff_v1`, initial 500ms, max 8.000ms, jitter 10%, retry rate 100/s và
burst 100 mỗi producer. Shaper fixture giữ 500 attempts/s, burst 500 mỗi Gateway;
drain giữ 30s. Không có headroom retry được dành riêng khi đang phát đều.
Producer ghi 17.989 attempts gồm 7.989 retries; 5.945 application ACK và 4.055
pending, cộng đúng 10.000 offered. Backend terminal scrape nhận 17.989 = 6.206
accepted + 10.691 queue-full + 1.092 shaper-rate-limited. PersistFailures và
ACKPublishFailures đều 0 trong scrape này. Queue-full chiếm phần lớn admission
rejects, nhưng shaper cũng từ chối; dữ liệu này không chứng minh DB là bottleneck
ban đầu.

Backend terminal accounting: `Committed=6.125` + `DuplicateCommitted=55` =
`ACKPublished=6.180`; `Processed=6.200` nhiều hơn ACKPublished 20. Các metric là
các stage/window quan sát riêng nên chênh lệch 20 không tự chứng minh mất dữ liệu.
DB được đọc tuần tự sau scrape: 6.151 ledger và 615.100 raw (A 3.174/317.400,
B 2.977/297.700), raw khớp 100 mẫu/ledger. Các lần đọc không atomic với metrics
hay producer ACK; chênh 26 giữa commit counter và DB có thể gồm drain/commit sau
scrape, không đủ căn cứ kết luận corruption hoặc mất/thừa rows.

Chỉ có 5 workload snapshots trong khoảng ~40s deadline (cách nhau xấp xỉ 8–9s),
collector ghi `collector_deadline_exceeded`; đây không phải chuỗi đo liên tục.
Terminal metrics có sẵn, nhưng terminal resource scrape từng service timeout;
các service vẫn có resource samples trước đó. Không có histogram persisted trong
artifact N1 và không suy histogram bằng 0. Lịch sử setup `NOT RUN` và startup
failure giữ nguyên như đã ghi trong run artifacts; không đổi thành PASS.

**Ranh giới:** N1 cho counters hữu ích và exact cleanup, không phải workload
completion, capacity acceptance, sustained throughput, soak hay P8 acceptance.
N1 cũng không chứng minh nguyên nhân DB persistence. N2/N3/N4 và qualification
native trên final source chưa chạy.

### Bổ sung — Persistence diagnostics local smoke (2026-11-07)

Source mới bổ sung diagnostics có giới hạn: tự thu thập 11 stage cố định
`persistence.durations` trong remote E2 official `steady_burst` (stage `begin` gồm pool
acquisition; lock SQL duration không đồng nghĩa pure lock wait), giữ lại các
trường `db` của pgx pool, và ghi baseline/samples/terminal PostgreSQL/Linux với
timestamp độc lập. Tài liệu vận hành, công thức delta và giới hạn diễn giải nằm
trong [runbook E2](paper-p8-e2-remote-gate.md#chẩn-đoán-persistence-e2-source-mới-chạy-riêng).

Smoke reader fixture đầu tiên FAIL do thiếu dấu ngoặc đóng ngoài cùng của SQL; exact
cleanup vẫn đúng. Regression được sửa, smoke tiếp theo PASS: PostgreSQL 16.15
(`server_version_num=160015`),
30 I/O rows, cgroup quota 0,5 CPU, Docker one-shot, read-only và receipt không
đổi, exact cleanup. Artifacts giữ tại
`/var/tmp/opencode/p8-persistence-diagnostics/native-reader-hzvnxr/summary.json`
(FAIL) và
`/var/tmp/opencode/p8-persistence-diagnostics/native-reader-H7g3DZ/summary.json`
(PASS). Đây là fixture reader-only cô lập: quota 0,5 CPU không phải quota E2
hay workload telemetry; không tạo telemetry, và `full_p8_accepted=false`.

Đây **không** phải lượt E2 remote đo persistence. Chưa chạy cùng `run06` trên
source mới; cần người dùng phối hợp Host B theo runbook, chọn leaf mới, chuyển
bundle/binary mới, giữ S=100 samples/message và phase `samples-100`, export cả khi PARTIAL, rồi finish/abort và
exact cleanup. Không bảo đảm PASS và không phải chẩn đoán phần cứng. Giữ nguyên
N1 `PARTIAL`/`ABORTED`, mọi FAIL/PARTIAL/NOT RUN lịch sử, cùng trạng thái chưa
đóng E2R.1/5 và chưa hoàn tất P8. Parent Go race telemetry/ingress/cmd-server,
build, vet, lint và gofmt PASS; combined Python final chạy 571 tests, 1 skipped,
không failures/errors (không phải 571 PASS). Commands/logs và final local
verification ở `/var/tmp/opencode/p8-persistence-diagnostics/final-verification-lKOHyi/handoff.md`;
không có claim coverage hoặc full CI.

### Bổ sung — Bounded active diagnostics retention và kiểm chứng cục bộ (2026-11-07)

**Bối cảnh:** Ở lượt chạy `run07` trước đó, dữ liệu chẩn đoán của đợt active burst ban
đầu đã bị đẩy ra ngoài (evicted) khỏi vòng đệm xoay vòng cũ (rolling ring 64 mẫu và
512 KiB) do bộ lấy mẫu tiếp tục gom các mẫu trong khoảng thời gian rảnh khi người
vận hành chờ điều phối giữa các bước. Mã nguồn hiện hành đã hoàn tất cơ chế lưu
giữ chủ động có giới hạn (bounded active side capture) ở phạm vi cục bộ:

1. **Phạm vi và luồng lấy mẫu:**
   - Cơ chế bounded active side capture chỉ áp dụng cho E2 official `steady_burst`.
     Các chế độ standalone, pilot, hoặc `equal_samples` giữ nguyên hành vi trước đây.
   - Luồng đọc chậm riêng biệt (separate slow-reader thread) cho PostgreSQL/Linux/Docker
     hoạt động độc lập, không làm nghẽn tiến trình lấy mẫu HTTP định kỳ.

2. **Trigger và cửa sổ yêu cầu:**
   - Trigger kích hoạt active CHỈ ĐƯỢC XÁC ĐỊNH khi CÙNG một tiến trình backend
     (`process_instance`) và số lượng `ingress.Received` (dạng số nguyên hợp lệ)
     tăng so với baseline HTTP trên Host A (`scrape_finished_monotonic` / `scrape_finished_timestamp`).
     Không tính thời điểm `begin`, không phụ thuộc tiến trình đọc PG hay đồng hồ Host B.
   - Vùng đệm tiền kích hoạt (pre-trigger buffer) lưu tối đa $\le 4$ mẫu và $\le 64\text{ KiB}$,
     được tính gộp trong tổng giới hạn 128 mẫu / 2 MiB. Hệ thống có thể chờ trước khi
     có trigger; thời lượng thu thập không chạy sớm khi chưa có lưu lượng thực tế.
   - Cửa sổ yêu cầu sau trigger = $\max(\text{tổng thời lượng các pha } A, B) + 10\text{s}$
     (với kịch bản 10s đo lường + 30s drain $\to$ 50s sau trigger). Session absolute
     deadline độc lập chặn cận trên; ngân sách thời gian được tính lại sau baseline
     chậm và không được gia hạn.

3. **Chính sách lưu trữ và bao phủ:**
   - Sau khi trigger kích hoạt, hệ thống bảo toàn chứng cứ sớm nhất (earliest evidence),
     tuyệt đối không eviction để lấy chỗ cho mẫu idle. Đóng băng toàn bộ khi hết cửa
     sổ tự nhiên; các khoảng lặng tạm thời không kích hoạt freeze.
   - Cạn 128 mẫu hoặc 2 MiB sẽ đóng băng ngay với nhãn `truncated` / `incomplete`.
     Thiếu baseline, identity, trigger, hoặc bị reset/dừng sớm thì kết quả không
     được coi là hoàn tất (`complete`).
   - Tiến trình đọc đang chạy được phép hoàn tất với timestamp riêng; không khởi động
     nguồn mới sau khi đóng; nguồn chưa chạy ghi `unavailable`. Baseline/terminal tách biệt;
     stop/join có chặn thời gian.

4. **Giới hạn dung lượng mẫu và tệp chẩn đoán:**
   - Ngân sách mẫu lưu giữ theo kích thước JSON cho `postgres_linux` là **128 mẫu và 2 MiB**
     (dung lượng mẫu serialized/logical được giữ lại, không phải phân bổ RAM cứng);
     ngân sách vòng đệm HTTP rolling ring giữ **3.600 mẫu và 2 MiB**.
   - Tổng dung lượng TỆP JSON chẩn đoán (`p8-e2-diagnostics.json`) được giới hạn cứng
     ở mức **8 MiB**, đo bằng chuỗi mã hóa thực tế qua `json.dumps` mặc định bởi helper
     chuẩn tắc `native_compose_smoke.paper_p8_e2_persist_diagnostics(path, payload)`
     (dùng thống nhất cho early, final, và fallback writes). Phân định rõ từng ngân sách
     mẫu tuần tự hóa với trần file JSON tổng thể trên đĩa (cả hai đều không phải process RSS).
     Vượt quá 8 MiB sẽ fail-closed với trạng thái giới hạn kích thước.
   - Thao tác xuất (`finish-phase` / `abort`) trước khi dọn dẹp bảo toàn dữ liệu active
     sớm nhất; sự chậm trễ của người vận hành sau khi freeze không làm mất dữ liệu.
     Trường `full_p8_accepted: false` được bảo toàn trong diagnostics; artifact xuất
     được và cleanup exact không đồng nghĩa nghiệm thu hiệu năng.

**Kết quả kiểm chứng cục bộ (Parent Final Local Verification):**
- Bài kiểm tra đơn vị và hồi quy: **566 selected Python unit/mock regressions PASS**
  (chạy trong một lệnh duy nhất, không cộng dồn các lượt trước), cú pháp Python phân tích
  hợp lệ (syntax parse PASS), `git diff --check` PASS (mã thoát 0).
- Bảo toàn mã nguồn: Toàn bộ 9 tệp mã Go khớp chính xác SHA256 theo
  `/var/tmp/opencode/p8-retention-e708uz/preexisting-go-work.json` và 4 tệp Python khớp
  theo `verified-python-hashes.json`. Lệnh và log chi tiết được lưu giữ tại
  `/var/tmp/opencode/p8-retention-e708uz/final-verification-w1s2zeu2/checks.json` và
  `python-tests.log`.
- **Ranh giới kỹ thuật:** Native DB/broker/E2 official trên scheduler+retention source
  là **NOT RUN**; không có chứng minh năng lực chịu tải 1.000 msg/s. Các số liệu trước
  đây (451, 223, v.v.) là từ các phiên bản source cũ hơn, không cộng dồn hay nâng cấp
  các lượt chưa hoàn tất. Mọi FAIL/PARTIAL lịch sử vẫn được bảo toàn.
- **Kế hoạch tiếp theo:** Khi người vận hành điều phối chạy E2 official mới trên Host B,
  cần tạo bundle/binary mới, workspace độc lập, giữ nguyên cấu hình `run07` (rate 1.000 msg/s,
  S=100, duration 10s, drain 30s, 4 workers, pool 10, shaper on, retry bounded_backoff_v1
  initial 500ms max 8000ms jitter 10%, rate 100/burst 100 mỗi producer; backend 2 CPU / 4 GiB).
  Không tự ý tuning hay chuyển sang N2/N3/N4. Sửa đổi retention cục bộ không phải là cải
  thiện hiệu năng đo đạc từ xa và không đưa ra kết luận nâng cấp phần cứng.
