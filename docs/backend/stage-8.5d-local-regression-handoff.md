# Stage 8.5d — Hồi quy cục bộ và bàn giao

**Phạm vi:** kiểm tra cục bộ cuối cho Stage 8.5d, không mở feature hay rollout.
Các fixture chỉ dùng tài nguyên cô lập do runner sở hữu; không đọc `.env` triển
khai, không gọi VPS/FRP/Cloudflare hay endpoint bên ngoài. Log runner được giữ
quyền riêng tại `/var/tmp/opencode/stage8-5d-final-20261120/`; không lưu URL ký,
query token hoặc credential trong tài liệu.

## Kết quả hồi quy 8.5d ban đầu (trước follow-up restart)

| Lệnh | Kết quả |
|---|---|
| `python3 scripts/tests/stage8_media_acl_fixture.py` | PASS: exact ACL off/on readback; A/B đúng chiều media up/down; chặn sai chiều, cross-Gateway, `$CONTROL`; revoke A ngăn xác thực kết nối mới. Broker, DynSec volume và scratch riêng được dọn. |
| `sh scripts/test-stage2-mqtt-credentials.sh` | PASS: credential acceptance TLS/auth/ACL, provision/rotate/revoke, startup enabled/disabled và cleanup sở hữu. |
| `sh scripts/test-stage3-uplink.sh --scenario normal` | PASS: hai Gateway, mỗi Gateway 1 batch / 100 raw samples / 1 ledger / 1 temporal; cả hai application-ACK, outbox về 0; tổng 2 ledger / 200 raw / 2 temporal; cleanup chính xác. |
| `python3 scripts/tests/stage5_mqtt_fixture.py` | PASS: Stage 5 MQTT lifecycle/ACL, Stage 6 native retry và gateway query/receipt roundtrip; fixture cleanup xác nhận. Fixture dùng profile control riêng, không bật media. |
| `python3 scripts/tests/stage8_media_db_fixture.py` | PASS: đúng migration terminal `28:stage8_media_metadata`; 8 PostgreSQL require-run tests PASS, không skip; fixture cleanup xác nhận. Bao gồm authorization/replay/conflict, quota serialize, grant denial/terminal replay, expiry và cleanup fence/claim. |
| `go test -race -count=1 ./internal/media ./internal/mediaprotocol ./internal/mediastorage ./internal/mediaworker ./internal/mqttmedia ./internal/gatewaysim ./internal/mqttcredential ./internal/config ./cmd/server` | PASS trên cả 9 package. |
| `go vet ./internal/media ./internal/mediaprotocol ./internal/mediastorage ./internal/mediaworker ./internal/mqttmedia ./internal/gatewaysim ./internal/mqttcredential ./internal/config ./cmd/server` | PASS. |
| `sh scripts/test-stage8-media-storage-http.sh --stage8-local-restart` | **FAIL, giữ nguyên:** phase `gateway_restart_gate_wait`, `restart_gate_expected_one_envoy_put_got_0`; cleanup fixture chính xác vẫn `true`. Xem phân tích bên dưới. |

### Lịch sử FAIL restart — được giữ nguyên

Trong lượt trên, fixture đã quan sát đúng một managed row `verifying`, một
Storage object và spool `awaiting_result` trong khi gate chặn commit `ready`;
nhưng helper `envoy_media_put_count` không tìm thấy dòng `"PUT HTTP/` trong
Envoy logs (count 0), nên dừng trước khi SIGTERM/reopen Gateway. Đây là sai
lệch giữa quan sát qua DB/Storage và bộ đếm log fixture; chưa đủ bằng chứng để
xác định product regression. FAIL lịch sử này được giữ nguyên; exact
owned-resource cleanup đã được xác nhận.

## Follow-up restart — hai lượt mới PASS

Thay đổi chỉ dành cho evidence fixture: Envoy access log thêm trường route name
`route=stage8-media-signed-put` (không log path/query), để lọc đúng media PUT
thay vì P2 provisioning PUT. Harness chờ hữu hạn tối đa 20 giây cho count đúng
1 và fail ngay nếu count >1. Sau restart, harness giữ assertion không có PUT
thứ hai và quan sát tối đa 35 giây, đòi count ổn định trong ít nhất 4 giây.
Unit tests kiểm tra delayed visibility, đếm đúng route, từ chối duplicate ngay
và từ chối duplicate xuất hiện muộn.

| Lượt | PUT trước khi stop (quan sát / thời gian chờ) | PUT sau restart (quan sát / cửa sổ) | E2E và cleanup |
|---|---:|---:|---|
| 1 | 1 / 6 quan sát, 2.521 s chờ log; gate signal tại 0.752 s từ lúc Gateway chạy | 1 / 7 quan sát, 4.268 s; count ổn định 4.113 s | 1 row `ready`, 1 object private, 1.546 bytes, hash khớp; spool được gỡ; 53.079 s gateway-to-ready; cleanup chính xác |
| 2 | 1 / 9 quan sát, 3.652 s chờ log; gate signal tại 0.754 s | 1 / 7 quan sát, 4.183 s; count ổn định 4.025 s | 1 row `ready`, 1 object private, 1.546 bytes, hash khớp; spool được gỡ; 49.768 s gateway-to-ready; cleanup chính xác |

Hai lượt xác nhận duy nhất một request trên route media qua Envoy trước và sau
restart; kết luận này không dựa vào object count (no-overwrite có thể che lần
PUT lặp). `PYTHONPATH=scripts/tests python3 -m unittest
test_stage8_media_restart_logs test_stage8_media_storage` PASS: 16 tests.
Các hồi quy Stage 2/3/5, ACL và DB trong bảng kết quả trên là kết quả lượt
trước, không chạy lại trong follow-up này. Log follow-up được giữ riêng tại
`/var/tmp/opencode/stage8-restart-followup-20261120/`, không chứa URL ký hay
token; không có feature, endpoint hoặc rollout thay đổi.

## Bằng chứng Stage 8 cục bộ liên quan

- Exact-cap Gateway E2E trong [8.5b](stage-8.5b-exact-cap-local-gateway-upload.md)
  là kết quả đã ghi nhận trước lượt này, không chạy lại trong lượt 8.5d: đúng
  **80.000.000 byte**, SHA-256
  `1133f8f033b20f3c87f89fb7e8311241ad8e97e64894469dbedfe67ea8120a91`;
  1 managed `ready` row / 1 object trong bucket private (`public=false`),
  readback hash/size trùng, spool copy được gỡ sau `ready`, source không đổi.
  Thời gian Gateway-to-ready ghi nhận **11.117 s trong lượt đó**.
- [8.3c](stage-8.3c-storage-upload-route.md) ghi nhận sai path/method, token
  hết hạn, route admin/alternate và overwrite bị từ chối; upload
  **80.000.001 byte** trả `EntityTooLarge`. Đây là evidence của fixture 8.3c,
  không phải lần chạy lại 8.5d.
- ACL fixture vừa chạy chứng minh quyền theo Gateway, sai chiều/cross-Gateway
  và revoke (fresh auth bị từ chối). Native DB fixture vừa chạy kiểm tra
  authorization, terminal replay, expired cleanup scheduling và giữ ready khỏi
  cleanup. Race suite chạy các test cục bộ về invalid content (MIME/container/
  hash), retry/restart/spool terminal retention; đây không phải bằng chứng
  Cloudflare hay full end-to-end cho từng negative.
- Khi lỗi terminal, Gateway giữ spool/file và lý do để đối soát; chỉ xóa spool
  sau `upload_result: ready` khớp ID. Cleanup Storage lỗi đến giới hạn retry
  giữ quota; xem [8.4c](stage-8.4c-media-cleanup.md), không giải phóng quota
  bằng cách sửa trạng thái/path thủ công.

## Cách chạy an toàn và ranh giới

- Fixture cô lập: `sh scripts/test-stage8-media-storage-http.sh` với
  `--prepare-stage8-e2e`, `--stage8-local-e2e`, `--stage8-local-e2e-cap` hoặc
  `--stage8-local-restart`;
  mỗi lệnh tự dựng fixture loopback/private và tự dọn. Không dùng URL/token từ
  fixture trong ticket/log công khai.
- Gateway demo operator là đường riêng, dùng `gateway run` với file MP4 đã
  finalized và spool riêng theo cấu hình trong
  [8.2b](stage-8.2b-media-config.md). Chỉ bật sau khi operator đã chuẩn bị
  broker/backend/Storage profile được cấp quyền, ACL readback và secret qua
  kênh bảo vệ; không trỏ fixture vào VPS hoặc biến fixture thành demo public.
- Backend và Gateway source defaults đều là `MEDIA_UPLOAD_ENABLED=false`
  (`src/internal/config/config.go`, `src/internal/gatewaysim/config.go`);
  ACL fixture bootstrap cũng giữ media off trước bước transition tường minh.
  Stage 3 normal uplink và Stage 5 control smoke ở trên chạy không làm media
  work; không đổi default flags.
- **8.8A FRP/VPS không triển khai**. Kiểm thử **8.5 từ mạng ngoài qua FRP và
  Cloudflare: NOT RUN**, chờ người dùng triển khai VPS. Không mở endpoint công
  khai, không release/remote CI; **Phần 2 chưa mở**. Bằng chứng 8.5 chỉ áp dụng
  cho fixture local.

## Follow-up nghiệm thu tích hợp cục bộ sau sửa lỗi Review (20/11/2026)

**Revision & Scope:** Git HEAD `f0dda7d6a0ddbaf36f32515512ea1fdd4413de4e`, working tree dirty (chứa các bản vá Stage 8: HIGH-1..5, MEDIUM-1..5, LOW). Nghiệm thu hoàn toàn trên môi trường cục bộ/fixture cô lập, không đọc `.env` triển khai hay secret thật; không có khẳng định nào cho môi trường remote, FRP hay mạng ngoài.

### 1. Bảng đối chiếu Issue → Test (RED / GREEN)

| Issue | Vấn đề cốt lõi | Test hồi quy (RED / GREEN) | Kết quả kiểm chứng |
|---|---|---|---|
| **HIGH-1** | Envoy chặn nhầm Supabase Storage bucket/object paths | `scripts/tests/stage8_media_storage_http_fixture.py --stage8-local-base` | PASS: Phân biệt route `media-video` theo đúng vị trí bucket; các route legacy khác hoạt động bình thường. |
| **HIGH-2** | Gateway filter media nhầm lẫn với Gateway ID `"media"` | `src/internal/gatewaysim/transport_test.go` | PASS: `isMediaNamespaceTopic` kiểm tra segment thứ 3; không drop điều khiển/ACK của Gateway ID `"media"`. |
| **HIGH-3** | Mất grant ban đầu / đối soát thiếu object kẹt spool | `internal/mediaprotocol`, `internal/mediaworker`, `internal/gatewaysim` | PASS: Bổ sung `media_id` vào `retry_after_completion` & `pending+OBJECT_MISSING`; spool gán bền vững ID, chống xung đột, chuyển về `awaiting_grant`. |
| **HIGH-4** | Context timeout của Storage hủy nhầm ngữ cảnh ghi DB | `internal/mediaworker/verifier_test.go`, `cleanup_test.go` | PASS: Cô lập context Storage timeout; ngữ cảnh worker parent còn sống thực thi đầy đủ retry/exhaustion lên DB. |
| **HIGH-5** | Duplicate `upload_request` hủy sớm pending intent | `internal/media/repository_integration_test.go` | PASS: Tách kiểm tra deadline thực tế khỏi grant horizon; intent còn sống trả `GrantRetryAfterCompletion` hoặc `GrantPending` mà không ghi `expired`. |
| **MEDIUM-1** | Starvation lease hàng loạt trong CleanupWorker & sai config | `internal/mediaworker/cleanup_test.go`, `cmd/server`, `internal/config` | PASS: Worker chỉ claim 1 item khi slot rảnh; kết nối đúng cấu hình `Cleanup*`; chặn cận dưới `CleanupMaxAttempts >= 2`. |
| **MEDIUM-2** | Gateway đóng spool/giải phóng flock khi worker chưa join | `cmd/gateway/main_test.go` | PASS: Giữ nguyên kết nối SQLite và khóa flock nếu worker chưa dừng; báo lỗi thoát nonzero (`1`). |
| **MEDIUM-3** | Gateway chu chuyển upload lỗi để chiếm slot unsettled | `internal/media/repository_integration_test.go` | PASS: Vị từ `MaxPendingPerGateway` đếm cả intent `rejected`/`expired` chưa xác nhận dọn dẹp xong. |
| **MEDIUM-4** | Parser MP4 chấp nhận mdat rỗng & không kiểm tra dung lượng bảng | `internal/mediaworker/content_test.go` | PASS: Từ chối mdat rỗng (kể cả zero-to-EOF); kiểm tra dung lượng vật lý bảng trước khi duyệt (an toàn ARM 32-bit); từ chối `stsc` không đơn điệu và `moof`. |
| **MEDIUM-5** | Client HTTP Storage nội bộ rò rỉ qua proxy môi trường | `internal/mediastorage/client_test.go` | PASS: Cấp private production transport với `Proxy: nil`; subprocess test chứng minh dummy proxy nhận 0 request. |
| **LOW** | Chẩn đoán staticcheck ST1005 viết hoa thông báo lỗi | `staticcheck -checks=ST1005 ./internal/mediastorage` | PASS: 12 thông báo lỗi được chuẩn hóa viết thường chữ cái đầu; 0 chẩn đoán. |

### 2. Kết quả 5 làn nghiệm thu tích hợp cục bộ

1. **Làn 1 (Go race, vet, build, cross-build, lint):**
   - `go test -race -count=1` trên cả 10 package (`internal/media`, `mediaprotocol`, `mediastorage`, `mediaworker`, `mqttmedia`, `gatewaysim`, `mqttcredential`, `config`, `cmd/server`, `cmd/gateway`): PASS.
   - `go vet` trên cả 10 package: PASS (0 issues).
   - `go build ./...`: PASS.
   - Cross-build ARMv7 32-bit (`cmd/gateway`, `cmd/server` vào `/var/tmp/opencode/armv7-scratch`): PASS (ELF 32-bit LSB ARM).
   - `staticcheck -checks=ST1005 ./internal/mediastorage`: PASS (0 diagnostics).
   - Các bài test linh kiện tập trung (Lost grant, Storage timeout, MP4 physical bounds, Spool shutdown, Config wiring, Proxy disabled): 100% PASS (chứng minh cục bộ, phân biệt rõ với native E2E).
2. **Làn 2 (Stage 8 PostgreSQL require-run):**
   - `sh scripts/test-stage8-media-db.sh`: PASS (10 required PostgreSQL integration tests PASS, 0 skips, exact schema terminal 28 verified, fixture cleanup verified: True).
3. **Làn 3 (Storage true-base, cấu hình và HTTP native):**
   - `python3 scripts/tests/stage8_media_storage_http_fixture.py --stage8-local-base`: PASS (`exact_cleanup_verified: true`).
    - `python3 scripts/tests/test_stage8_media_storage.py`: PASS (14 test Python cho cấu hình route và bootstrap bucket; không phải bằng chứng HTTP native cho cap+1).
    - `sh scripts/test-stage8-media-storage-http.sh`: PASS ở lượt xác minh bổ sung trên bản vá cuối. Storage ảnh ghi/đọc được qua tuyến cũ; signed PUT hợp lệ ghi object có SHA-256 khớp; đường dẫn/phương thức sai, URL hết hạn, thao tác quản trị và ghi đè bị từ chối. Storage v1.74 từ chối **80.000.001 byte** với HTTP 400, mã `EntityTooLarge` và `statusCode:"413"` trong body. Kiểm tra log/artifact không lộ token và dọn tài nguyên fixture đều PASS (`exact_cleanup_verified: true`).
4. **Làn 4 (Gateway Local E2E Lanes — Chạy tuần tự):**
   - `--stage8-local-e2e` (1.546 byte): PASS (`gateway_to_ready_seconds: 1.897s`, `exact_cleanup_verified: true`).
   - `--stage8-local-e2e-cap` (80.000.000 byte): PASS (`actual_bytes: 80000000`, `gateway_to_ready_seconds: 9.312s`, `exact_cleanup_verified: true`).
   - `--stage8-local-restart`: PASS (Gateway restart duy trì cùng spool/upload_id, đúng 1 PUT duy nhất qua Envoy trước và sau restart, `gateway_to_ready_seconds: 6.56s`, `exact_cleanup_verified: true`).
5. **Làn 5 (Credential, Telemetry & Control Smoke):**
   - `python3 scripts/tests/stage8_media_acl_fixture.py`: PASS (exact off/on readback, QoS 1 up/down, wrong-direction/cross-Gateway/control/revoked denials, cleanup verified).
   - `sh scripts/test-stage2-mqtt-credentials.sh`: PASS (all standalone routes, TLS/auth/ACL, rotation/revocation, cleanup verified).
   - `sh scripts/test-stage3-uplink.sh --scenario normal`: PASS (2 Gateway, 0 pending, 2 ledgers / 200 raw / 2 temporal, cleanup verified).
   - `python3 scripts/tests/stage5_mqtt_fixture.py`: PASS (Task 5.4 lifecycle, Stage 6 retry resilience, query/receipt roundtrip, cleanup verified).

### 3. Ranh giới vận hành và Explicit NOT RUN

- **Môi trường & Tài nguyên:** Toàn bộ log và artifact được cách ly tại `/var/tmp/opencode/`; tài nguyên Docker (containers, networks, volumes) được dọn dẹp chính xác sau mỗi làn chạy.
- **Ranh giới:**
  - **Mục 8.8A (FRP/VPS) và kiểm thử qua Cloudflare/8.8A: NOT RUN** (chưa triển khai hạ tầng VPS ngoài).
  - **Remote CI / Full CI GitHub Actions: NOT RUN** (chỉ thực hiện nghiệm thu cục bộ theo kế hoạch một người làm).
  - **Live Rollout / Production Deployment: NOT RUN** (giữ nguyên cờ mặc định `MEDIA_UPLOAD_ENABLED=false`).

## Follow-up 22/11/2026 — Hai nhánh phục hồi 8.5 còn thiếu

**Revision & Scope:** HEAD `fbd077b325f6508c4e46167a041394232193a1bc` cùng dirty worktree; từng runner build từ source local lúc chạy, không tuyên bố đây là revision sạch hoặc mọi lượt dùng byte-identical diff. Tất cả dùng fixture Docker cô lập và clip H.264 1.546 byte. Không đọc `.env` triển khai. Các run độc lập, cleanup chính xác; kết quả chỉ chứng minh local fixture.

| Lượt | Trạng thái trung gian đã quan sát | Kết quả cuối |
|---|---|---|
| `--stage8-local-lost-grant` | Gateway `gw_stage8_ad1b32a25755` bỏ grant đầu cho `upload_680a65c722e22b52`; nhận `OBJECT_MISSING` khi Envoy PUT count còn 0. | Cùng upload ID/media ID `1937ad54-1b5b-437e-9b33-fa483b3621f5` được cấp lại; đúng 1 PUT, row và SQLite `ready`, object private/path đúng, spool file gỡ. SHA-256 `133b3ccc3377dfcb2f7d6b17bb3a7a4e3a3cb38e80aec6b094b2e1a734da04cc`; cleanup PASS. |
| `--stage8-local-verifier-timeout` | Gateway `gw_stage8_9bba375d81c8`; với Storage timeout 1s, proxy trì hoãn response GET đầu 5s: DB `verifying` / `STORAGE_UNAVAILABLE`, `next_verification_at` có giá trị; object đã tồn tại, Envoy PUT count 1, SQLite `awaiting_result` và spool file còn. | Proxy ghi nhận 2 lượt GET; lần sau xác minh object hiện hữu thành công. DB/SQLite `ready`, spool file gỡ, vẫn đúng 1 PUT; cùng SHA-256 trên; cleanup PASS. |
| `--stage8-local-e2e` | Luồng chuẩn clip nhỏ. | PASS, một row/object, hash khớp, cleanup PASS. |
| `--stage8-local-restart` | Backend commit `ready` khi Gateway dừng; spool/source/upload ID giữ nguyên. | Gateway khởi động lại, đúng một PUT trước và sau restart, spool chỉ gỡ sau `ready`; cleanup PASS. |

Các báo cáo runtime kiểm tra không có signed URL/query token hoặc Authorization header trong log. Fixture chỉ giảm `MEDIA_UPLOAD_INFLIGHT_GRACE`/scan interval cho lượt mất grant và chỉ bật Storage proxy cùng timeout/retry profile ở lượt verifier-timeout; mặc định runner không tiêm lỗi. Các lượt thử trước profile/route cuối vẫn là **FAIL**: hai lost-grant run hết cửa sổ quan sát do grace mặc định 2 phút; hai timeout run chưa khớp path GET thực tế nên số GET bị chặn là 0. Cleanup các lượt này PASS; không lượt nào được tính lại thành PASS.

**Verification:** `go test -race ./internal/gatewaysim ./cmd/gateway -count=1`, `go vet ./internal/gatewaysim ./cmd/gateway`, `go build ./internal/gatewaysim ./cmd/gateway`, `golangci-lint` (0 issues), ARMv7 cross-build, Python bundle/driver 22 tests, `py_compile` và `git diff --check`: PASS. Remote CI, Cloudflare, backend-process restart thuộc 8.2 và live rollout vẫn NOT RUN.

## Follow-up 23/11/2026 — Signed PUT qua Nginx chung (local only)

> Ghi nhận trạng thái triển khai trung gian hai-cờ; đã được hợp nhất thành một
> cờ trong mục follow-up cuối tài liệu.

**Phạm vi:** thêm route signed `PUT` chính xác cho `media-video/<uuid>.mp4` vào
`config/nginx/nginx.conf.template`, mặc định đóng bằng
`MEDIA_UPLOAD_EDGE_ENABLED=false`. Khi bật, Nginx yêu cầu route key không rỗng,
chỉ ghi đè header `X-Stage8-Media-Route` ở location upload rồi chuyển nguyên
request tới Envoy. Storage ảnh và các route API/Auth hiện hữu giữ nguyên; tunnel
`media-upload`, listener `stage8-media-upload` và origin hiện tại chưa bị gỡ hay
đổi. Cổng 80 không đổi.

### RED → GREEN và kiểm chứng local

- RED: hai kiểm tra cấu hình mới thất bại trước implementation: fixture TLS còn
  proxy tới `stage8-media-upload`, và Compose/Nginx chung chưa có công tắc edge.
- GREEN: fixture TLS hiện trỏ tới `nginx.conf.template` qua một alias nội bộ;
  cùng HTTPS edge được dùng để kiểm tra readiness/API, Storage ảnh và upload.
  Base fixture xác nhận route signed trả 404 khi media edge tắt; upload hợp lệ
  khi bật đi qua Nginx → Envoy → Storage.
- `python3 -m unittest scripts.tests.test_stage8_media_storage`: **16 tests PASS**.
- `python3 scripts/tests/stage8_media_storage_http_fixture.py --stage8-local-base`:
  PASS, gồm media-off, Storage ảnh và cleanup chính xác.
- Fixture mặc định: PASS — signed PUT nhỏ, readback/hash, no-overwrite, sai path/
  method, token hết hạn, route quản trị/đọc bị chặn, cap +1 bị Storage từ chối,
  không thấy token trong log/artifact; cleanup chính xác.
- `--stage8-local-e2e`: PASS, 1.546 byte, DB/SQLite `ready`, 1 object, hash
  khớp; `--stage8-local-e2e-cap`: PASS đúng **80.000.000 byte**, hash khớp.
- `--stage8-local-restart`: PASS, Gateway dùng lại cùng spool/source/upload ID,
  đúng một PUT; `--stage8-local-lost-grant`: PASS, object vắng trước PUT đầu và
  chỉ một PUT sau recovery.
- `--stage8-local-verifier-timeout`: PASS sau khi chờ hữu hạn để Envoy access
  log ghi nhận PUT; quan sát DB `verifying` / `STORAGE_UNAVAILABLE`, SQLite
  `awaiting_result`, object đã tồn tại, sau đó `ready` mà không PUT lần hai.
- `--stage8-local-cleanup`: PASS, cleanup worker xóa object terminal, giữ row/
  object `ready`, giải phóng quota sau xác nhận.
- `py_compile` cho fixture/unit files và `git diff --check`: PASS. Các native
  fixture xác nhận `docker compose config` và khởi động Nginx thành công.

### Lịch sử lỗi trong lượt này và giới hạn

- Lượt `--stage8-local-e2e` đầu: readiness timeout vì fixture khởi động backend
  trước khi cấp origin tạm hợp lệ; đã sửa fixture setup và chạy lại PASS. Lượt
  `--stage8-local-verifier-timeout` đầu: Envoy log count còn 0 tại thời điểm
  assertion; thêm chờ hữu hạn cho visibility của log và lượt chạy lại PASS.
  Các FAIL ban đầu được giữ nguyên, không tính lại thành PASS.
- **Không gọi Cloudflare/tunnel hay mạng ngoài; user tự làm smoke/cutover.**
  Remote CI, live deployment, đổi đồng bộ hai origin, dừng tunnel cũ và gỡ
  listener cũ đều NOT RUN. Mặc định `MEDIA_UPLOAD_EDGE_ENABLED=false` được giữ;
  chưa có commit cho thay đổi này.

## Follow-up 23/11/2026 — Dùng một cờ bật media

Phần cấu hình hai cờ ở follow-up trước đã được thay thế: `MEDIA_UPLOAD_ENABLED`
điều khiển cả backend media lẫn signed PUT route trên Nginx chung. Xóa
`MEDIA_UPLOAD_EDGE_ENABLED` khỏi Compose, `.env.example`, Nginx template và
fixture. Không cần `stage8-media` trong `COMPOSE_PROFILES` để dùng route chung;
profile/listener `stage8-media-upload` vẫn được giữ riêng như đường rollback.

**Bằng chứng cục bộ:** test contract mới có RED trước khi đổi wiring vì Nginx
chưa nhận `MEDIA_UPLOAD_ENABLED`; sau thay đổi, `python3 -m unittest
scripts.tests.test_stage8_media_storage` PASS (16 tests). Fixture
`--stage8-local-base` PASS với media false/404; fixture mặc định PASS với signed
PUT qua Nginx chung; `--stage8-local-e2e` PASS với media true, 1.546-byte clip,
hash khớp, một object `ready`, và cleanup chính xác. Các fixture Compose chỉ
start service Nginx chung và test TLS edge, không start listener thuộc profile
`stage8-media`.

Lượt mặc định đầu sau khi gộp cờ FAIL khi backend media được bật nhưng fixture
chưa có HTTPS origin hợp lệ; fixture được sửa để cấp origin tạm và bootstrap
media DynSec theo đúng cờ, sau đó chạy lại PASS. Không gọi Cloudflare/tunnel hay
mạng ngoài; không sửa `.env` triển khai, đổi origin, dừng tunnel hoặc gỡ
listener. Không commit.

## 8.2R — Phục hồi khi restart tiến trình Go backend

**Phạm vi:** fixture Compose cô lập, Gateway simulator Linux, private Storage
và HTTPS local. Code chạy tại baseline `6994d3eba0f0f34f69922ed7b9c1e0dfda9bdf40`
cùng diff fixture SHA-256 `8300907bcf1163b22a045d0c7b8a529fdd18be60a3eafa53d8815e121c02a2bb`;
không phải commit sạch hay bằng chứng remote.

`sh scripts/test-stage8-media-storage-http.sh --stage8-local-backend-restart`
**PASS**, cleanup chính xác. Với một verifier, Storage timeout 45 giây, DB
timeout 5 giây và scan 250 ms, fixture giữ phản hồi GET đầu khi clip đang
`verifying`, lease 60 giây còn hiệu lực và spool ở `awaiting_result`. Gateway
được xác nhận dừng (`SIGSTOP`, trạng thái Linux `T`). Backend dừng thật rồi chạy
lại cùng container, PID/`StartedAt` mới, `/readyz` và media SUBACK mới. Không
sửa hàng DB hoặc chỉnh lease để thúc đẩy recovery.

Backend mới đọc đúng object lần thứ hai và chốt `ready` ở generation/attempt
2, từ 1, khi Gateway vẫn dừng; lần đọc DB thấy `ready` khoảng 0,48 giây sau hết
lease. Sau `SIGCONT`, cùng `upload_id`/`media_id` về `ready` trong SQLite, file
spool được gỡ và file nguồn còn. Chỉ có một row managed, một object private,
một PUT; đối soát 1.546 byte, `video/mp4` và SHA-256 khớp. Báo cáo JSON đã lọc
và fingerprint được giữ riêng dưới
`/var/tmp/opencode/stage8-2r-c-durable-20261123-031706/`.

Hồi quy media-off: fixture `--stage8-local-base` **PASS** với signed PUT
`404`, ảnh đọc/ghi được và cleanup chính xác; `TestMediaRuntimeOffIsInert`
**PASS**. 38 unit tests Python liên quan, `py_compile` và `git diff --check`
đạt. Không kiểm Cloudflare, VPS, phần cứng hoặc rollout.
