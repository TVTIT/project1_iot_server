# P8 — Báo cáo thực nghiệm và bàn giao checkpoint server

**Trạng thái (29/10/2026): `LOCAL_TOOLING_COMPLETE_SOURCE_QUALIFIED`.** Hoàn tất bàn giao công cụ/tài liệu P8.14 trong phạm vi được cho phép; đây không phải nghiệm thu paper chính thức, nghiệm thu production, chứng nhận capacity/HIL, hoặc hoàn tất yêu cầu board Stage 5 gốc. Mọi raw artifact E5 vẫn giữ `full_p8_accepted=false`. Deployment flags P8/Stage 5 tiếp tục false. Không tiếp tục Stage 6/P9.

## Phạm vi và source qualification

P8 đã triển khai admission/accounting WebSocket session có giới hạn cấu hình, batch membership recheck và một số công cụ E1–E5; báo cáo này liên kết bằng chứng local đã ghi nhận nhưng không khẳng định mọi gate dùng cùng một dirty source tree. Artifact lịch sử giữ fingerprint riêng; không gộp artifact có source SHA khác nhau. Context SHA không có trong raw E5 schema và tiếp tục unavailable trong summary công khai; kiểm chứng context riêng được lập chỉ mục private. Cập nhật tài liệu không chạy lại integration.

Bảng nguồn/bằng chứng (cố ý không đưa private paths):

| Nhóm bằng chứng | Logical artifact / source | Kết quả và qualification |
|---|---|---|
| E1 pilot WS/MQTT giới hạn | `P8.6-E1-final-pilot`; chi tiết run source-qualified trong handbook | PASS, 64 sockets/2 principals, 20 unique messages, 2.000 raw rows, 530 WS writes, không ghi nhận drop; cleanup chính xác 12/1/5. Pilot ngắn, colocated; không phải capacity. FAIL ở outer report trước đó được giữ nguyên. |
| E2 pilot telemetry ghép cặp | `P8.7-E2-final-pilot` | Pilot native cuối PASS sau hai lượt FAIL được lưu; hai sample profile, workload giới hạn và cleanup owned chính xác. Không đổi nhãn lỗi lịch sử hoặc tuyên bố đã chạy offered-rate rung chính thức. |
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
