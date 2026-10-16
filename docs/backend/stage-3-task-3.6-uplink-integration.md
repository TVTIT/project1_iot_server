# Task 3.6 — Tích hợp TLS/ACL, Disconnect, Restart và Áp lực Hữu hạn

**Ngày:** 16/10/2026.

**Phạm vi tài liệu này:** Báo cáo nghiệm thu tích hợp Task 3.6 cho toàn bộ chín kịch bản cross-service của đường uplink:
- **3.6.1–3.6.2:** Entrypoint tích hợp, hai Gateway độc lập cùng dùng chung một `sensor_id`, đối chiếu SQL và SQLite outbox sau application ACK.
- **3.6.3 (hàng 1–3):** Từ chối TLS/credential (sai CA, sai hostname/SAN, sai password) với positive controls có đối chứng.
- **3.6.3 (hàng 4–5, 7):** Từ chối ACL (cross-Gateway publish, cross-Gateway subscribe, DynSec control-topic) với positive controls và đối chứng receiver/DB.
- **3.6.3 (hàng 6) + 3.6.4:** Từ chối ACK spoofing và xử lý mất application ACK sau khi DB đã commit bằng byte-exact resend không nhân đôi dữ liệu.
- **3.6.5 (bước 1–2):** Database timeout trên Sensor row lock và phục hồi bằng resend.
- **3.6.5 (bước 3–6):** Áp lực queue-full hữu hạn, quan sát `QueueFull > 0`, không ACK sớm và phục hồi trọn vẹn sau drain.
- **3.6.6 (bước 1):** Ngắt kết nối mạng Gateway thông thường (ordinary network disconnect), tự động reconnect, resubscribe và resend nguyên trạng pending item.
- **3.6.6 (bước 2–3):** Khởi động lại có kiểm soát backend (SIGTERM) và Gateway CLI khi còn pending item, hạch toán chính xác `boot_id` cũ và mới.
- **3.6.7:** Khởi động lại Mosquitto broker có kiểm soát trên volume bền vững, xác minh lưu trữ và hiệu lực của credential hiện hành, credential đã rotate (mật khẩu mới được chấp nhận, mật khẩu cũ bị từ chối), client bị vô hiệu hóa (`disabled=true`) và cấu trúc ACL.

**Trạng thái:** Task 3.6 **hoàn thành có giới hạn (bounded complete), chờ commit** trên nhánh `develop`.
- **Baseline HEAD:** `160a246` ("fix(harness): đồng bộ schema 20 cho native compose smoke và cập nhật backlog"), worktree có các thay đổi chưa commit.
- **Bằng chứng thực tế:**
  1. Toàn bộ 9 kịch bản bắt buộc đã chạy và **PASS** trong một lần gọi `--scenario all` trên môi trường dịch vụ thật (Mosquitto 2.0.18 + TimescaleDB schema 20) với Docker images build từ `src/Dockerfile`:
     - Backend image: `sha256:a883aca1cfbd176fbff4610862037905e89a60fe1fde156649c2518fe44a3afa`
     - Gateway image: `sha256:dc924c0aad32d78ff7524c644f279fe95b7cc4aaa24637c262c4ebee75420468`
  2. Bản sửa đổi logging an toàn sau đó (loại bỏ raw connection error trong gatewaysim transport, thay bằng allowlisted reason codes qua hàm `classifyConnectError`) đã chạy lại kịch bản `tls-credential` và **PASS**, kèm theo kiểm thử Go race/vet/lint tập trung và toàn bộ test suite Python (**766 tests**, 0 lỗi).
  3. **Phân định rõ revision bằng chứng:** Không tuyên bố tất cả 9 kịch bản đều được chạy lại sau bản sửa logging-only; bằng chứng toàn bộ 9 kịch bản thuộc về lượt chạy `--scenario all` trước đó, và bản sửa logging-only chỉ ảnh hưởng tới mã hiển thị log khi kết nối lỗi đã được kiểm chứng riêng bằng kịch bản `tls-credential`.
- **Giới hạn và phạm vi chưa thực hiện:**
  - Kịch bản workload 600 batches / 60.000 samples (**U14**) được giữ lại cho **Task 3.7**.
  - Không bao gồm chạy thử nghiệm trên bo mạch phần cứng ARM thật (chỉ mới cross-build ARMv7 từ Task 3.5).
  - Không tuyên bố độ bền sau mất điện đột ngột (power-loss durability) hay fsync phần cứng.
  - Không bao gồm bộ phân tích hash mật khẩu tĩnh trên đĩa cứng (ROT2 disk verifier) hay cơ chế distributed failover.
  - Không bao gồm API tự động reset credential (reset route vẫn duy trì fail-closed 503 có chủ đích).
  - Không có migration môi trường production trong lượt này.
  - Tài liệu này **KHÔNG** phải tài liệu bàn giao tổng thể `stage-3-uplink.md` (tài liệu đó thuộc Task 3.7).

---

## 1. Entrypoint công khai và các bộ chọn kịch bản (Selectors)

Entrypoint công khai của bộ kiểm thử tích hợp là script:
```bash
scripts/test-stage3-uplink.sh [options]
```
Script này gọi runner Python `scripts/tests/stage3_uplink_runner.py`. Runner quản lý vòng đời của Docker network riêng, container TimescaleDB, container Mosquitto 2.0.18, container Backend và container Gateway Simulator, đảm bảo dọn dẹp sạch sẽ tài nguyên sau khi chạy.

### 1.1 Các bộ chọn kịch bản hỗ trợ

| Lệnh chạy | Kịch bản tương ứng | Nội dung kiểm chứng chính |
| --- | --- | --- |
| `./scripts/test-stage3-uplink.sh --scenario all` | Cả 9 kịch bản tuần tự | Chạy trọn vẹn toàn bộ ma trận tích hợp Task 3.6 trên cùng fixture disposable. |
| `./scripts/test-stage3-uplink.sh --scenario normal` | 3.6.1–3.6.2 | Đường uplink bình thường với hai Gateway A/B, đối chiếu độc lập PostgreSQL và SQLite outbox. |
| `./scripts/test-stage3-uplink.sh --scenario tls-credential` | 3.6.3 (hàng 1–3) | Từ chối TLS sai CA, sai hostname/SAN, và sai password; có positive controls đối chứng. |
| `./scripts/test-stage3-uplink.sh --scenario acl-control` | 3.6.3 (hàng 4–5, 7) | Từ chối cross-Gateway publish, cross-Gateway subscribe, và DynSec control-topic. |
| `./scripts/test-stage3-uplink.sh --scenario ack-fault` | 3.6.3 (hàng 6) + 3.6.4 | Từ chối ACK spoofing và xử lý mất application ACK sau DB commit qua byte-exact resend. |
| `./scripts/test-stage3-uplink.sh --scenario db-timeout` | 3.6.5 (bước 1–2) | Chặn worker bằng Sensor row lock quá `TELEMETRY_DB_TIMEOUT`, xác minh timeout và phục hồi sau khi mở lock. |
| `./scripts/test-stage3-uplink.sh --scenario queue-full` | 3.6.5 (bước 3–6) | Hồ sơ 1 worker / queue capacity 1, ép nghẽn hàng đợi quan sát `QueueFull > 0`, phục hồi không mất/nhân đôi. |
| `./scripts/test-stage3-uplink.sh --scenario gateway-reconnect` | 3.6.6 (bước 1) | Ngắt mạng container Gateway (`docker network disconnect`), reconnect, resubscribe và resend nguyên vẹn. |
| `./scripts/test-stage3-uplink.sh --scenario process-restart` | 3.6.6 (bước 2–3) | Khởi động lại backend (SIGTERM) và khởi động lại Gateway CLI khi còn pending item, bảo toàn `boot_id`. |
| `./scripts/test-stage3-uplink.sh --scenario broker-controlled-restart` | 3.6.7 | Khởi động lại broker trên volume bền vững, kiểm tra credential hiện hành, rotated, revoked và ACLs. |

### 1.2 Nguyên tắc cách ly nhị phân và môi trường

1. **Thực thi trong Docker Image thật:** Không bind-mount nhị phân từ máy host vào `/app/server` hay `/app/gateway`. Cả backend và simulator đều chạy nhị phân được biên dịch sẵn bên trong Docker images build từ `src/Dockerfile` (`target: backend` và `target: gateway`).
2. **Không đọc cấu hình triển khai:** Fixture sinh CA riêng, tự cấp phát credential ngẫu nhiên CSPRNG trong thư mục tạm, tự tạo Docker network riêng biệt, và không bao giờ đọc tệp `.env` hay chứng chỉ triển khai thực tế của host.
3. **Masking Volume để chống rò rỉ:** Các đường dẫn volume khai báo trong Dockerfile (như `/mosquitto/data`, `/mosquitto/log`, `/data/outbox`) đều được mount tường minh tới các thư mục tạm có kiểm soát, ngăn chặn việc phát sinh anonymous volumes ngoài tầm quản lý của Docker daemon.

---

## 2. Chi tiết 9 kịch bản kiểm chứng và Oracles

### 2.1 Kịch bản 1: `normal` — Hai Gateway độc lập cùng dùng chung một `sensor_id` (3.6.1–3.6.2)

- **Mục tiêu:** Chứng minh đường truyền dữ liệu bình thường từ hai Gateway độc lập qua broker DynSec tới backend thật và lưu trữ TimescaleDB.
- **Thiết lập cách ly:** Gateway A (`gateway_a`) và Gateway B (`gateway_b`) đều gửi dữ liệu cho cảm biến có cùng định danh `sensor_stage3_uplink_shared`. Mỗi Gateway có URN riêng (`urn:ngsi-ld:Gateway:gateway_a`, `urn:ngsi-ld:Gateway:gateway_b`), URN Sensor riêng (`urn:ngsi-ld:Sensor:gateway_a:sensor_stage3_uplink_shared`, `urn:ngsi-ld:Sensor:gateway_b:sensor_stage3_uplink_shared`), và sentinel cấu hình (`desired_state`) khác nhau để làm chứng thực cách ly.
- **Tiến trình:**
  - Mỗi Gateway khởi chạy CLI `cmd/gateway run --batches 1 --samples 100 --sample-interval 10000us`.
  - Hai batch được gửi độc lập qua MQTT TLS (QoS 1).
  - Backend xác thực, lưu vào TimescaleDB, và xuất bản application ACK tương ứng về topic `gateways/<id>/acks/<sensor_id>`.
  - Cả hai Gateway nhận được application ACK hợp lệ, xóa row pending trong SQLite outbox về 0.
- **Oracle & Số liệu đối chiếu SQL độc lập:**
  - Bảng `telemetry`: Đúng 200 dòng raw (100 dòng cho Gateway A, 100 dòng cho Gateway B). Dữ liệu của A gắn đúng URN của A, dữ liệu của B gắn đúng URN của B.
  - Bảng `processed_messages`: Đúng 2 dòng ledger (1 cho A, 1 cho B) với payload hash và sequence range chính xác.
  - Bảng `twin_temporal_values`: Đúng 2 dòng quan sát temporal metadata `sample_interval_us`.
  - Bảng `twin_states`: Trạng thái `reported_state` của Sensor A và Sensor B đều được cập nhật từ mẫu cuối cùng (mẫu index 99) với `reported_version` tăng đúng +1.
  - Các trường `desired_state` và Gateway sentinel của cả A và B được giữ nguyên vẹn không bị ghi đè.

### 2.2 Kịch bản 2: `tls-credential` — Từ chối TLS và Credential có đối chứng (3.6.3 hàng 1–3)

- **Hàng 1 (Sai CA):**
  - Gateway CLI được cấp file CA giả mạo không ký chứng chỉ của broker.
  - Oracle: Gateway không thể thiết lập phiên kết nối TLS, log lý do allowlisted `tls_unknown_authority`, tiến trình kết thúc với mã lỗi 1, không nhận được application ACK, và hàng pending trong SQLite outbox được giữ nguyên vẹn (không bị xóa hay thay đổi).
  - Positive Control: Chạy trước và sau với CA đúng, kết nối thành công, nhận ACK và xóa outbox.
- **Hàng 2 (Sai Hostname / SAN):**
  - Một tiến trình backend ứng viên độc lập được cấu hình trỏ tới broker qua alias mạng `san-negative` (alias này trỏ vào IP broker nhưng không nằm trong Subject Alternative Names của chứng chỉ server: `localhost`, `broker`).
  - Oracle: Backend từ chối kết nối TLS với broker, ghi nhận lỗi `tls_hostname_mismatch`, endpoint `/readyz` không bao giờ trả về HTTP 200, và tuyệt đối không fallback về kết nối insecure không mã hóa.
- **Hàng 3 (Sai Password):**
  - Probe client kết nối với username Gateway A nhưng password sai.
  - Oracle: Mosquitto broker dứt khoát từ chối kết nối với mã CONNACK `0x05` (Connection Refused, not authorised). Gateway CLI thực tế khi gặp mật khẩu sai ghi log `authentication_rejected`, thoát với mã lỗi 1, và hàng pending trong SQLite outbox giữ nguyên.
  - Positive Control: Đăng nhập với mật khẩu đúng thành công (CONNACK `0x00`).

### 2.3 Kịch bản 3: `acl-control` — Từ chối ACL Namespace và Control-Topic (3.6.3 hàng 4–5, 7)

- **Hàng 4 (Cross-Gateway Publish):**
  - Gateway A dùng credential của A để publish một payload telemetry hợp lệ cho B vào topic của B (`gateways/gateway_b/telemetry/sensors/sensor_stage3_uplink_shared`).
  - Oracle: Mosquitto ghi nhận log `Denied PUBLISH from 'gateway_a' for gateways/gateway_b/...`. Snapshot CSDL của B xác nhận: 0 dòng raw, 0 dòng ledger, 0 dòng temporal, và version của Sensor B không thay đổi. Gateway B không nhận được application ACK giả mạo nào.
  - Positive Control: Gateway B tự gửi dữ liệu lên topic của chính mình thì được chấp nhận và commit thành công.
- **Hàng 5 (Cross-Gateway Subscribe):**
  - Gateway A gửi gói SUBSCRIBE tới topic ACK của B (`gateways/gateway_b/acks/#`).
  - Oracle: Broker trả về mã từ chối SUBACK `0x80` (Failure). Khi Gateway B gửi một batch hợp lệ và nhận application ACK của B, receiver của A quan sát thấy nhận đúng 0 message, chứng minh không có sự rò rỉ thông tin ACK giữa các Gateway.
  - Positive Control: Gateway A subscribe topic ACK của chính mình (`gateways/gateway_a/acks/#`) nhận được SUBACK `0x01` thành công.
- **Hàng 7 (DynSec Control Topic):**
  - Gateway A cố tình gửi một bản tin yêu cầu DynSec hợp lệ (yêu cầu tạo client thăm dò với correlation ID riêng) vào topic quản trị `$CONTROL/dynamic-security/v1`.
  - Oracle: Broker từ chối publish. Readback độc lập qua tài khoản quản trị `manager` xác nhận client thăm dò không bao giờ được tạo trong DynSec store. Đồng thời, Gateway A thử SUBSCRIBE vào `$CONTROL/dynamic-security/v1/response` bị broker từ chối với SUBACK `0x80`.
  - Positive Control: Tài khoản `manager` gửi yêu cầu quản trị trên topic này được xử lý bình thường.

### 2.4 Kịch bản 4: `ack-fault` — Mất Application ACK sau Commit và Chống ACK Spoofing (3.6.3 hàng 6 + 3.6.4)

- **3.6.4 (Mất Application ACK & Byte-Exact Resend):**
  - Sử dụng hook fixture có thể đảo ngược: Gán role `deny-receive-ack` (chặn quyền receive trên `gateways/+/acks/#`) cho Gateway A thông qua DynSec manager.
  - Gateway A gửi 1 batch telemetry. Backend tiếp nhận, xử lý và commit thành công vào TimescaleDB (1 ledger, 100 raw, 1 temporal được xác nhận qua SQL độc lập).
  - Do quyền receive bị chặn, application ACK không tới được Gateway A. Tiến trình simulator giữ nguyên item trong SQLite outbox ở trạng thái `pending`.
  - Simulator thực hiện gửi lại (resend) với nội dung byte-exact: giữ nguyên `message_id`, payload hash, khoảng sequence, timestamps và `boot_id`.
  - Backend nhận bản tin trùng lặp, kích hoạt nhánh deduplication: không chèn thêm dòng raw, không tạo ledger mới, không cập nhật lại temporal, giữ nguyên `reported_version`.
  - Gỡ bỏ role chặn nhận ACK và reconnect phiên. Lần resend tiếp theo nhận được application ACK hợp lệ, xóa sạch hàng outbox về 0. Kiểm tra qua SQLite read-only connection và lệnh `cmd/gateway count` xác nhận outbox đã hoàn toàn trống.
- **3.6.3 Hàng 6 (Chống ACK Spoofing):**
  - Tạm dừng (pause) container backend để giữ 2 batch đang pending của Gateway A và Gateway B chưa được commit.
  - Gateway A cố tình đóng vai backend, publish một application ACK 5 trường hợp lệ cho chính mình và cho Gateway B.
  - Oracle: Broker DynSec chặn toàn bộ hành vi publish vào topic `gateways/+/acks/#` từ credential Gateway (Gateways chỉ có quyền SUBSCRIBE vào acks, không có quyền PUBLISH). Cả hai Gateway không nhận được ACK giả mạo, pending items trong SQLite giữ nguyên.
  - Sau khi resume backend, backend xử lý hợp lệ, gửi ACK thật và outbox của cả hai bên được giải phóng an toàn.

### 2.5 Kịch bản 5: `db-timeout` — Database Timeout trên Row Lock và Phục Hồi Bằng Resend (3.6.5 bước 1–2)

- **Mục tiêu:** Kiểm chứng hành vi khi CSDL bị trễ hoặc deadlock cục bộ: backend không được gửi ACK thành công sớm, và dữ liệu phải được phục hồi toàn vẹn khi hết lỗi.
- **Tiến trình:**
  - Một kết nối SQL quản trị của fixture chủ động giữ khóa hàng `SELECT ... FOR UPDATE` trên Sensor của Gateway A trong PostgreSQL.
  - Fixture kiểm tra view `pg_stat_activity` để đảm bảo worker backend thực sự đang đợi khóa hàng này.
  - Gateway A publish một batch telemetry. Do worker backend bị treo khi đợi lock quá thời hạn cấu hình `TELEMETRY_DB_TIMEOUT=5s`, transaction bị hủy (rollback).
  - Backend ghi nhận lỗi timeout trong log: bộ đếm `persist_failures` tăng lên trong `ingress.logSummary()`, không có dòng raw/ledger/temporal nào được ghi vào CSDL, và không có application ACK nào được phát hành. Gateway A giữ nguyên item trong SQLite outbox.
  - Fixture giải phóng khóa SQL (`ROLLBACK`).
  - Simulator gửi lại (resend) bản tin với cùng payload bytes.
  - Backend xử lý thành công: 100 dòng raw, 1 ledger, 1 temporal được ghi nhận; application ACK được gửi đi; SQLite outbox của Gateway A trở về 0.

### 2.6 Kịch bản 6: `queue-full` — Áp Lực Nghẽn Hàng Đợi Hữu Hạn và Phục Hồi (3.6.5 bước 3–6)

- **Mục tiêu:** Kiểm tra khả năng chịu áp lực và chống tràn hàng đợi khi tài nguyên bị bão hòa.
- **Thiết lập áp lực:**
  - Backend được khởi động với cấu hình tối thiểu: `MQTT_WORKER_COUNT=1`, `MQTT_QUEUE_CAPACITY=1`.
  - Worker duy nhất bị chặn tại hàng Sensor bằng khóa `FOR UPDATE` trong PostgreSQL.
  - Gateway Simulator phát liên tiếp 5 batches hợp lệ vào broker với tốc độ cao.
- **Oracle & Quan sát thực tế:**
  - Hàng đợi của backend đầy ngay lập tức (1 slot đang xử lý, 1 slot trong queue).
  - Bộ đếm `QueueFull > 0` được ghi nhận thực tế trong structured log summary của backend (`telemetry ingress summary ... queue_full=3 ...`).
  - Không có application ACK nào được gửi non cho các batch bị từ chối do tràn hàng đợi.
  - Giải phóng khóa SQL. Các workers tiếp tục xử lý các batch còn lại và các lượt retry từ simulator.
  - Toàn bộ 5 batches cuối cùng đều được commit thành công (đúng 5 dòng ledger, 500 dòng raw, 5 dòng temporal quan sát được qua SQL).
  - Tất cả 5 application ACKs được nhận và outbox của Gateway trở về 0 không còn tồn đọng.

### 2.7 Kịch bản 7: `gateway-reconnect` — Ngắt Kết Nối Mạng và Phục Hồi Tự Động (3.6.6 bước 1)

- **Tiến trình:**
  - Khi Gateway đang có 1 pending item trong outbox, fixture thực hiện ngắt kết nối mạng của container Gateway bằng lệnh `docker network disconnect`.
  - Tầng transport của Gateway Simulator ghi nhận mất kết nối: ghi log an toàn `event=connection_lost` và `event=disconnected`.
  - Fixture kết nối lại mạng bằng lệnh `docker network connect`.
  - Supervisor của Gateway tự động kích hoạt kết nối lại, xác thực TLS với Mosquitto, ghi log `event=connected` (epoch tăng), và tự động đăng ký lại topic ACK (`event=subscribed filter=gateways/gateway_001/acks/#`).
  - Simulator tiến hành gửi lại (resend) pending item với nguyên vẹn payload bytes.
  - Dữ liệu được backend commit đúng 1 lần vào PostgreSQL, gửi application ACK, và outbox của Gateway trở về 0. CSDL không xuất hiện bất kỳ bản ghi trùng lặp nào.

### 2.8 Kịch bản 8: `process-restart` — Khởi Động Lại Có Kiểm Soát Backend và Gateway CLI (3.6.6 bước 2–3)

- **Khởi động lại Backend:**
  - Khi Gateway đang có pending item trong outbox, fixture gửi tín hiệu `SIGTERM` tới container Backend.
  - Backend thực hiện shutdown sạch trong thời hạn `SHUTDOWN_TIMEOUT`: dừng nhận dữ liệu, drain workers, đóng kết nối CSDL và thoát với mã 0.
  - Fixture khởi động lại Backend với cùng image và cấu hình. Backend khởi động, đạt `/readyz` và đăng ký lại exact filter `gateways/+/telemetry/#`.
  - Gateway resend pending item. Backend tiếp nhận, xử lý, commit CSDL và gửi application ACK. Outbox của Gateway về 0.
- **Khởi động lại Gateway CLI với Outbox Bền Vững:**
  - Dừng tiến trình Gateway CLI khi đang có 1 pending item trong tệp SQLite outbox.
  - Khởi động lại CLI Gateway trỏ vào cùng tệp SQLite outbox, yêu cầu phát thêm 1 batch mới (`--batches 1`).
  - Hạch toán chính xác `boot_id`:
    * Pending item từ boot cũ được tự động phát lại trước, giữ nguyên `boot_id` cũ và sequence cũ.
    * Batch mới được sinh ra và gửi đi mang `boot_id` mới.
  - CSDL TimescaleDB xác nhận cả 2 batches đều được lưu trữ chính xác với 2 `boot_id` khác nhau (tổng cộng 200 dòng raw, 2 ledger, 2 temporal). Cả hai nhận được ACK và outbox sạch hoàn toàn.

### 2.9 Kịch bản 9: `broker-controlled-restart` — Khởi Động Lại Broker và Độ Bền Cấu Hình DynSec (3.6.7)

- **Thiết lập trạng thái trước khi khởi động lại:**
  - Khởi tạo credential cho Gateway A và Gateway B.
  - Thực hiện xoay vòng (Rotate) credential cho một Gateway thăm dò: mật khẩu mới hoạt động, mật khẩu cũ bị từ chối.
  - Thu hồi (Revoke/Disable) một client thăm dò khác: client mang cờ `disabled=true` trong DynSec.
  - Cấu hình đầy đủ các ACL rules cô lập namespace cho từng Gateway.
- **Khởi động lại Broker:**
  - Thực hiện dừng Mosquitto có kiểm soát (`docker stop broker`).
  - Giữ nguyên volume bền vững `/mosquitto/data` và tệp `dynamic-security.json`.
  - Khởi động lại Mosquitto broker trên cùng volume dữ liệu.
- **Xác minh tính bền vững sau khi khởi động lại:**
  - **Credential hiện hành:** Cả Gateway A và Gateway B kết nối fresh TLS với mật khẩu hiện tại thành công.
  - **Rotated Credential:** Mật khẩu mới sau rotate tiếp tục xác thực fresh TLS thành công; mật khẩu cũ dứt khoát bị từ chối (CONNACK `0x05`).
  - **Revoked / Disabled Client:** Client bị vô hiệu hóa tiếp tục bị từ chối kết nối (CONNACK `0x05`), xác nhận cờ `disabled=true` được nạp lại nguyên vẹn từ store trên đĩa.
  - **ACL Rules:** Quyền truy cập vào topic của chính mình vẫn hoạt động bình thường; quyền truy cập trái phép cross-Gateway tiếp tục bị từ chối với `Denied PUBLISH`.
  - **Uplink phục hồi:** Sau khi broker khởi động lại, backend và gateway tự động kết nối lại, gửi một batch mới, backend commit vào TimescaleDB và gửi ACK hoàn tất.

---

## 3. Nhật ký An Toàn và Dọn Dẹp Tài Nguyên

### 3.1 Nhật ký không rò rỉ bí mật (Safe Structured Logging)

Trong suốt quá trình kiểm thử, không có bất kỳ thông tin nhạy cảm nào bị rò rỉ ra standard output, Docker logs hoặc tệp log kiểm thử:
1. **Ingress Structured Summary (`ingress.logSummary()`):**
   Chỉ xuất bản các trường số đếm tổng hợp ở thời điểm shutdown:
   ```text
   msg="telemetry ingress summary" accepted=... queue_full=... processed=... validation_rejected=... persist_failures=... commit_unconfirmed=... identity_mismatch=... ack_build_failures=... committed=... duplicate_committed=... samples_new=... ack_published=... ack_publish_failures=... queue_depth=... queue_capacity=256
   ```
2. **Phân loại lỗi kết nối Gateway (`classifyConnectError`):**
   Mọi lỗi kết nối mạng và TLS của Gateway Simulator đều được ánh xạ về một mã lý do chuẩn hóa nằm trong allowlist an toàn:
   - `tls_unknown_authority`
   - `tls_hostname_mismatch`
   - `tls_certificate_invalid`
   - `tls_certificate_verification`
   - `authentication_rejected`
   - `connection_closed`
   - `connection_failed`
   - `deadline_exceeded`
   - `context_canceled`
   Tuyệt đối không in raw error message từ driver, không in chuỗi URL broker có chứa thông tin nhạy cảm, không in username hoặc mật khẩu ra log.

### 3.2 Dọn dẹp tài nguyên chính xác (Exact Resource Cleanup)

Mỗi lần chạy của runner sinh ra một mã định danh ngẫu nhiên duy nhất (ví dụ `hexid="a1b2c3d4e5f6"`). Mọi tài nguyên Docker đều được gán nhãn và đặt tên theo tiền tố `uplink_<hexid>_*`:
- Container: `uplink_<hexid>_db`, `uplink_<hexid>_broker`, `uplink_<hexid>_backend`, v.v.
- Volume: `uplink_<hexid>_db_data`, `uplink_<hexid>_broker_data`, v.v.
- Network: `uplink_<hexid>_net`

Khối `finally` của runner thực hiện dừng và xóa chính xác theo danh sách ID/tên đã ghi nhận. Sau khi xóa, runner thực hiện quét kiểm tra lại để khẳng định không còn bất kỳ container, volume hay network nào sót lại.

---

## 4. Bảng Kết Quả Nghiệm Thu Trọng Tâm (U01–U15)

Dưới đây là bảng tổng hợp trạng thái các chỉ tiêu nghiệm thu Stage 3 (U01–U15) theo kế hoạch chi tiết, có phân định rõ **cấp độ kiểm chứng (Evidence LEVEL)** tương ứng:

| Mã | Tình huống kiểm chứng | Cấp độ kiểm chứng (LEVEL) | Kết quả | Ghi chú & Giới hạn thực tế |
| --- | --- | --- | --- | --- |
| **U01** | Batch 100 mẫu hợp lệ | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng qua các kịch bản `normal`, `ack-fault`, `process-restart`. Đúng 100 raw, 1 ledger, 1 temporal, reported state cập nhật, ACK trả về và SQLite outbox về 0. |
| **U02** | Retry cùng ID/hash/topic | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trong `ack-fault` (3.6.4), `gateway-reconnect`, và `process-restart`. Deduplication hoạt động chính xác: không chèn thêm dòng, version giữ nguyên, gửi lại ACK. |
| **U03** | Cùng ID khác bytes/topic | **Unit / Native DB** (Kế thừa Task 3.1 & 3.3) | **PASS** | Đã được kiểm chứng ở tầng nghiệp vụ core và transaction CSDL trong Task 3.1 và 3.3. Không thay đổi hợp đồng. |
| **U04** | Unknown Sensor, Gateway mismatch, malformed batch | **Unit / Native DB & Services thật** | **PASS** | Kiểm chứng qua unit test `internal/telemetry` và kịch bản `acl-control` (broker từ chối trước khi tới ứng dụng; CSDL không có dữ liệu rác). |
| **U05** | Overlap khác ID cùng boot; gap/nonoverlap đến muộn | **Unit / Native DB** (Kế thừa Task 3.3) | **PASS** | Đã được kiểm chứng bằng native DB fixtures trong Task 3.3: rollback toàn batch khi overlap, lưu trữ đủ khi có gap. |
| **U06** | Batch cũ đến sau batch mới, boot đổi/equal timestamp | **Unit / Native DB** (Kế thừa Task 3.3) | **PASS** | Đã kiểm chứng trong Task 3.3: lịch sử được ghi nhận đầy đủ, `reported_state` không bị lùi, desired fields giữ nguyên. |
| **U07** | Write lỗi hoặc transaction rollback | **Unit / Native DB & Services thật** | **PASS** | Kiểm chứng qua Task 3.3 (SQL rollback) và Task 3.6 kịch bản `db-timeout` (timeout trên lock giải phóng sạch, không có partial commit hay ACK non). |
| **U08** | Commit thành công, application ACK bị mất | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trọn vẹn trong `ack-fault` (3.6.4): giữ pending, resend byte-exact, dedup nhận diện, phục hồi nhận ACK và xóa outbox. |
| **U09** | Cross-Gateway, spoof ACK, control topic | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trong `acl-control` (3.6.3 hàng 4, 5, 7) và `ack-fault` (3.6.3 hàng 6): Broker từ chối, CSDL không side-effect, positive control chạy tốt. |
| **U10** | Sai CA, hostname/SAN, hoặc credential | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trong `tls-credential` (3.6.3 hàng 1–3): Từ chối kết nối dứt khoát, không fallback insecure, SQLite pending giữ nguyên. |
| **U11** | Queue đầy, DB chậm/timeout rồi phục hồi | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trong `db-timeout` và `queue-full` (3.6.5): Quan sát được `PersistFailures > 0` và `QueueFull > 0`, không ACK non, phục hồi đầy đủ dữ liệu. |
| **U12** | MQTT disconnect, restart tiến trình | **Services thật** (Docker Images + Broker + TimescaleDB) | **PASS** | Kiểm chứng trong `gateway-reconnect` và `process-restart` (3.6.6): Reconnect/resubscribe tự động, bảo toàn `boot_id` cũ và mới, outbox drain về 0. |
| **U13** | Broker controlled restart | **Services thật có giới hạn** (Mosquitto 2.0.18 Volume) | **PASS (Có giới hạn)** | Kiểm chứng trong `broker-controlled-restart` (3.6.7): Giữ credential hiện hành, rotated new/old denial, disabled status và ACLs qua controlled restart. *Không chứng minh crash durability hay fsync phần cứng*. |
| **U14** | Workload demo 600 batches / 60.000 mẫu | **Chưa thực hiện** | **NOT RUN** | Thuộc phạm vi nghiệm thu của **Task 3.7**. |
| **U15** | Regression liên quan | **Regressions thực tế đã chạy** | **PASS** | Bộ test Python (766/766 tests PASS), các gói Go liên quan (`go test -race`, `go vet`, `golangci-lint` PASS), build targets Docker thành công, smoke Task 3.4/3.5 giữ nguyên hiệu lực. |

*Cảnh báo phạm vi:* Việc đạt được các kịch bản trên không đồng nghĩa với việc toàn bộ U01–U15 đã hoàn thành tuyệt đối. U14 vẫn là **NOT RUN**. Các hạng mục kiểm thử tải nặng kéo dài, thử nghiệm bo mạch phần cứng ARM thật, kiểm chứng mất điện cưỡng bức, kiểm tra hash tĩnh ROT2 trên đĩa và remote CI trên GitHub Actions đều chưa thực hiện hoặc nằm ngoài phạm vi solo MVP.

---

## 5. Kết Luận và Bàn Giao Task 3.7

1. **Kết quả:** Task 3.6 hoàn thành đầy đủ chín kịch bản tích hợp dịch vụ thật bắt buộc theo kế hoạch chi tiết, mang lại bằng chứng thực nghiệm rõ ràng cho các cơ chế bảo mật (TLS/ACL), độ tin cậy (dedup, byte-exact resend, timeout recovery) và khả năng phục hồi sau sự cố (reconnect, restart tiến trình, restart broker).
2. **Trạng thái mã nguồn:** Mã nguồn runtime và các helper kiểm thử đã sẵn sàng; toàn bộ thay đổi hiện ở trạng thái chưa commit trên nhánh `develop` (baseline HEAD `160a246`).
3. **Bàn giao sang Task 3.7:**
   - Sử dụng lại image backend và gateway đã xây dựng cùng các fixture hiện có để thực hiện bài đo workload demo: 1 Gateway × 1 Sensor × 100 Hz trong 10 phút (600 batches, 60.000 samples).
   - Đo đạc các chỉ số hiệu năng thực tế (thời gian xử lý ACK p50/p95, dung lượng bộ nhớ, mức độ tăng trưởng CSDL).
   - Tích hợp pipeline kiểm thử Stage 3 vào hệ thống CI cơ bản.
   - Biên soạn tài liệu bàn giao tổng thể `docs/backend/stage-3-uplink.md` để kết thúc Giai đoạn 3 và chuẩn bị bước vào Giai đoạn 4 (Digital Twin & API đọc dữ liệu lịch sử/realtime).
