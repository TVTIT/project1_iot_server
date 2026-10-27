# P7 — WebSocket telemetry và fan-out (Local MVP)

**Trạng thái:** Local MVP đã được thực thi và nguồn được đối chiếu; P7.11 report/protocol/results unit hoàn tất. Các native runs bên dưới đều ghi `full_p7_accepted=false` vì mỗi run là lane riêng. Tổng hợp này đủ căn cứ mô tả P7 Local MVP, không sửa các trường acceptance gốc và không phải chứng nhận production. Checkout tại các run: branch `feat/paper-requirement`, HEAD `104a2cde82c3f49c6b73d123aa24be3ed8521a38`, dirty; không phải claim clean/commit snapshot chung cho mọi artifact. Runtime/config cuối được kiểm tra tại HEAD+dirty; các artifacts giữ fingerprint riêng theo từng lượt.

## Thiết kế và giới hạn runtime

Endpoint `GET /v1/ws`; triển khai bằng `github.com/coder/websocket`. Native gửi `Authorization: Bearer …` và `iot.telemetry.v1`; browser gửi `iot.telemetry.v1` cùng `bearer.<JWT>` trong `Sec-WebSocket-Protocol`. Middleware dùng verifier JWT chung, xác thực trước upgrade và chuyển verified principal/expiry; chỉ app protocol được negotiate, credential không được echo. Nginx P2 template chuyển credential tới Unix auth verifier, nhưng **Origin allowlist được kiểm tra tại Go WS handler**, không được coi Nginx/CORS hoặc Origin là identity. Origin không có giá trị được chấp nhận cho native; nếu có thì phải đúng một origin hợp lệ trong allowlist.

`WEBSOCKET_ENABLED=false` mặc định. Default `config/nginx/nginx.conf.template` trả `404` cho exact `/v1/ws`, độc lập với cờ backend. Khi request tới Go router trực tiếp/qua route thích hợp mà flag tắt, authenticated compatibility handler trả `501`; đây là hành vi khác. P2 HTTPS template có exact WS location và perimeter auth, nhưng handler vẫn áp dụng availability flag. P7 opt-in Compose/Nginx profile tạo route `/v1/ws` qua Unix auth socket tới Go backend; profile đó hiện là HTTP (`listen 80`), không tự cung cấp TLS/WSS, production topology hay rollout. Xem [runbook opt-in](../../config/nginx/P7-WS-OPT-IN.md). Deployment defaults gồm `WEBSOCKET_ENABLED`, `STAGE5_ENABLED`, `STAGE5_WAKEUP_ENABLED`, `MQTT_SHAPER_ENABLED`, `MQTT_MICROBATCH_ENABLED`, `MQTT_RAM_DEDUP_ENABLED` đều false.

Effective hard caps trong `internal/config/websocket.go`: 32 sessions; 16 filters/session; global 64 pending event work slots / 8 MiB logical reservation; session 64 slots / 256 KiB tính queued + in-flight telemetry và control bytes; frame 66,560 bytes; incoming control text 4,096 bytes; 16 targets/request; request ID tối đa 64 ký tự; 10 control requests/s/session. Một fixed event worker (bounded) encode/fan-out. Đây là logical accounting, không phải RSS. `WS_RECHECK_INTERVAL=5s`, `WS_RECHECK_SWEEP_BUDGET=2s` (ngân sách sweep toàn cục, không phải mỗi session), concurrency 2; không quảng bá thu hồi tức thì. Ping mỗi 20s, pong/read inactivity budget 45s; write timeout 5s. Reader luôn hoạt động để xử lý pong. JWT expiry dựa claims đã verify; không có refresh trong socket. `CloseNow`/transport failure có thể khiến browser báo abnormal close 1006, không hứa graceful close code cho mọi nguyên nhân.

Hub nhận event chỉ sau confirmed persistence outcome của message mới ở single, standalone oversized và member mới của group. DB duplicate, RAM dedup hit, rollback/uncertain không tạo event; duplicate ACK/replay không phát event lần nữa. Một session nhận tối đa một lần khi Gateway/Sensor filter overlap. Stream best-effort, không durable replay/exactly-once/total order. Global submission/build/frame/queue loss dẫn tới close/resync policy; slow viewer bị đóng riêng khi session queue overflow, không block persistence/ACK/viewer khỏe. DB và application ACK semantics không đổi, raw samples không bị downsample/cắt tại DB.

Frame cap không bảo đảm mọi legal MQTT payload vừa khung: JSON float canonicalization có thể nở; oversized event không phát telemetry-frame mà kích resync/đóng session, còn DB commit và ACK nguyên vẹn. Client dùng REST temporal `attrs=measurement`; cửa sổ bounded (endpoint maxPoints 256 trong probe) chia thành subwindows liên tục bán mở, end được mở rộng 1µs để giữ điểm cuối, tuples `[value, observedAt]` được so khớp theo microsecond và giữ duplicates cùng timestamp. Late batch cần refresh cửa sổ; gap không bảo đảm error frame hay clean close. REST raw tuples thiếu đầy đủ message/boot/sequence provenance nên merge exactly-once không được hứa.

Metrics-off giữ nguyên auth/bounds/policy. Close-cause telemetry là ring cuối 32 observations, ghi first observed trigger/cause class, operation và process-local elapsed/deadline; identity-free, không lưu token/claims/address/error text. Trigger quan sát được không đồng nghĩa physical root cause.

## Native evidence (sanitized rollup)

Schema 24; isolated fixture; backend quota 2 CPU / 4 GiB. Nguồn sanitized ở [`results/p7/summary.json`](results/p7/summary.json) và [`summary.csv`](results/p7/summary.csv); detailed safe measurement table is [`measurements.csv`](results/p7/measurements.csv). Raw artifacts/private captures stay under protected `/var/tmp/opencode/paper-p7/` and are not copied here.

| Lane | Run ID | Kết quả / chứng cứ tóm tắt | Cleanup |
|---|---|---|---|
| Lifecycle | `ncs_53d9f50403ce` | Expiry/revocation, healthy peer, ACK/outbox, shutdown/restart; accepted sublane | 20/1/6 |
| Telemetry grouped | `ncs_7683e0d0ccb9` | 8 ACK; 8 ledger / 800 raw / 8 temporal; 4 events/viewer; replay DB unchanged, no replay event | 17/1/7 |
| Single path | `ncs_60528ce1704e` | 8 ACK; 8/800/8; 4 events/viewer; exact replay unchanged | 17/1/7 |
| Slow viewer | `ncs_6c8b46e346c9` | 1000 ACK; 1000/1,000,000/1000; healthy viewer A namespace 1000, healthy viewer B namespace 0 (B không ingest); slow probe sau resume đọc hàng trăm frame; one queue-overflow close at 31.286s | 13/1/5 |
| 0/2 viewer descriptive pair | `ncs_8170a8d33dfc` / `ncs_2c0e4d70d51d` | each 40 ACK, 40/4000/40; respectively 0 vs 40 writes. Equivalent runtime binaries/config independently audited despite image-ID mismatch | 16/1/7;17/1/7 |
| P6+WS affected regression | `ncs_b8c56847b265` | 6 commands succeeded/config verified; 128 ACK; 11,135 raw; viewers 60/36 | 26/1/9 |
| Handshake | `ncs_c6652c3afa5a` | 101, negative controls, perimeter/direct WS deny isolated; no credential leak | 15/1/5 |
| Reconnect | `ncs_3e03c73b006b` | 100 live; catch-up window 200/200 DB/model rows; 3 ACK, outbox empty | 16/1/6 |

Overhead pair resources are sparse samples only: zero viewers CPU mean/min/max-sampled 1.163/0/5.29%, RAM 8,170,713/7,356,809/8,442,085 **bytes** (n=10); two viewers CPU 1.487/0.58/5.08%, RAM 8,130,192/7,704,936/8,360,296 bytes (n=9). Histograms are process-local monotonic cumulative observations, not causal/statistical overhead or percentiles. Zero-viewer write histogram count 0 means no write observations, not zero latency. No capacity, peak-RSS, zero-overhead, fairness or soak claim.

P7.10: fresh perimeter and reconnect lanes PASS; corrected affected Python selection 297 PASS (selected/overlapping invocation count, not summed); Node/syntax and recorded Go race/static/build/ARMv7 checks PASS as qualified in verification report. Full Python discovery's 3 failures + 4 errors is retained historical evidence, not rerun in this P7 finalization. Historical records: `ncs_9347970642a6` failed browser capture at `MAX_LIFETIME_MS`/scope; `ncs_db8ce208242e` failed REST 400-point request versus 256-point truncation; `ncs_79210b22a678` old perimeter counter oracle; `ncs_7d90f4dbc982` historical reconnect MIME/Accept failure; original strict image-ID comparator rejection and prior Nginx/prepare/oracle failures remain as recorded. `ncs_ac73c962fdbd` exited 0 with functional and cleanup PASS but only PARTIAL because formatter did not complete; not a native FAIL. `ncs_375e9aebcede` is P3 history, not a P7 native failure. No historical result is reclassified. No P8, full/remote CI, HIL/AM5728, capacity/soak, or live rollout.

### Handoff evidence map and checklist

Status: `LOCAL_MVP_COMPLETE_SOURCE_QUALIFIED`. Each lane below is native PASS only for its bounded sublane and has `full_p7_accepted=false`; this aggregate does not alter those fields. Evidence: grouped/single lanes cover commit-only event/dedup; lifecycle covers expiry/revocation/shutdown; handshake covers perimeter negatives; reconnect covers finite REST catch-up; slow-viewer lane covers isolation; P6+WS lane is affected regression; zero/two-viewer pair is descriptive and retains qualified binary/runtime pair audit, not strict image-ID equivalence. Runtime bounds, protocol and runbook are source-checked. P7.11 docs/navigation/backlog/checklist mapping is completed in this handoff. Full Python discovery history remains not rerun; full/remote CI, HIL, P8/capacity/soak and rollout are NOT RUN. The eight original acceptance items are mapped respectively to handshake+protocol, lifecycle, grouped/single, slow-viewer+metrics, lifecycle, reconnect, viewer pair+P6 regression (all bounded/source-qualified), and this documentation set; no raw run represents whole-P7 acceptance.

## Tái lập (fixture riêng)

Chỉ khi chủ động muốn lặp lại, từ repo root dùng run directory mới và log private; không source `.env`, không in Compose config đã resolve và không tái sử dụng path cũ:

```sh
RUN_ROOT="/var/tmp/opencode/paper-p7/manual-$(date +%Y%m%d-%H%M%S)"
old_umask=$(umask)
umask 077
mkdir -p "$RUN_ROOT"
: >"$RUN_ROOT/run.log"
chmod 600 "$RUN_ROOT/run.log"
umask "$old_umask"
PYTHONPATH=scripts/tests python3 scripts/tests/native_compose_smoke.py --prepare "$RUN_ROOT/fixture" --paper-p7 --paper-p7-mode telemetry_pilot >>"$RUN_ROOT/run.log" 2>&1
```

Đây là cú pháp runner, không phải lệnh đã chạy trong lượt tài liệu này. Output protected; kiểm cleanup fixture theo receipt. Dừng tại P7 Local MVP.
