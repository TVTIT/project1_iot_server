# P8 E1 — chuẩn bị, chạy pilot và nhập kết quả (source-qualified)

> E1 B nhận bundle + `paperload` binary, không nhận admin/DB/Docker credentials.
> Các hướng dẫn 64 sockets bên dưới mô tả baseline lịch sử; tham số E1 lớn đã
> sửa và bằng chứng mới nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e1r7--sửa-luồng-large-viewer).

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E1 (pilot hữu hạn giữa hai host A và B). E1 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST và Mosquitto TLS).

Trong thử nghiệm này, Host A sở hữu fixture và collector; Host B liên kết bundle, kiểm tra kết nối, thực thi hai role workload bằng Go binary (`paperload`), rồi xuất một projection kết quả đã giới hạn để Host A xác minh đối chiếu với cơ sở dữ liệu và telemetry thực tế. Thử nghiệm này là một pilot hữu hạn, không phải nghiệm thu toàn diện P8 (`full_p8_accepted=false`).

---

## 1. Tổng quan và nguyên tắc vận hành

Chi tiết kết quả pilot 128 viewer và qualification nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e1--tài-nguyên-fixture-và-pilot).

### 1.1 Phạm vi và mục tiêu thử nghiệm E1

- **Mô hình phối hợp hai host**:
  - **Host A (Server / Fixture / Collector)**: Khởi chạy môi trường dịch vụ backend đầy đủ (Nginx, Go backend, TimescaleDB, Mosquitto DynSec), cấp phát tài nguyên mẫu (provisioning), tạo manifest và đóng gói bundle gửi cho client, đồng thời thực hiện đo lường tài nguyên và hậu kiểm dữ liệu.
  - **Host B (Client Runner)**: Nhận bundle và binary từ Host A, thực hiện liên kết workspace, tiền kiểm tra (preflight), kiểm tra kết nối mạng/xác thực, thực thi workload mô phỏng và xuất tập kết quả đo lường (projection).
- **Topology và thuật ngữ thử nghiệm E1**:
  - **Ánh xạ mặc định chuẩn (legacy default mapping — `default64`)**:
    - 2 Gateways (`gw-0`, `gw-1`), 4 Sensors (2 sensor/gateway, tạo thành 4 targets: `gw-0/sensor-0`, `gw-0/sensor-1`, `gw-1/sensor-0`, `gw-1/sensor-1`).
    - 2 Users / Principals (`user-000`, `user-001`): Mỗi user sở hữu quyền truy cập gateway tương ứng theo phân quyền ACL; hệ thống không bypass quyền quản trị.
    - 64 viewer sockets: Mỗi viewer socket tương ứng một client kết nối WebSocket độc lập được cấp định danh giả danh `viewer-00000` đến `viewer-00063`.
    - 106 filters đăng ký: Với mật độ phân bổ mặc định `density=mixed` (kết hợp các bộ lọc sparse, dense và overlap), 64 viewer sockets đăng ký tổng cộng 106 filters.
    - Workload phát sinh 20 unique messages (4 targets × 5 messages/target; mỗi message chứa 100 samples, tương ứng 2.000 raw samples đẩy qua MQTT TLS).
    - **530 expected deliveries (WebSocket writes)**: Khớp chính xác với công thức phân bổ người nhận theo ánh xạ chuẩn `expected_deliveries_by_target` (tổng số lượt nhận trên 64 receiving viewers = 530). Đây là giá trị chuẩn xác của topology mặc định, **tuyệt đối không thay đổi tùy tiện**.
  - **Mức mở rộng có chọn lọc (`selected128`)**:
    - 128 viewer sockets (`viewer-00000` đến `viewer-00127`), 2 principals, 4 targets, 213 filters, 1.065 expected deliveries (5 messages/target). Yêu cầu chỉ định rõ profile `official` và cờ `--official-opt-in`.
  - **Phân biệt với hợp đồng offline (`offline5000` / `offline10000`) và tải thực tế**:
    - Các mức 5.000 và 10.000 viewers chỉ áp dụng trong hợp đồng offline schema-only (`--dry-run`); chỉ dùng để kiểm tra tính hợp lệ của manifest, bundle export, bundle inspect, bundle bind và Go `paperload --preflight`.
    - Hợp đồng offline **không mở kết nối mạng hay container Docker**, và **tuyệt đối không phải là chứng nhận năng lực chịu tải (capacity claim) hay nghiệm thu tải cao (capacity acceptance)**.
- **Ranh giới thử nghiệm**:
  - Thử nghiệm E1 là pilot hữu hạn trên môi trường kiểm chuẩn cục bộ (same-host smoke) sử dụng giao diện mạng riêng (advertised private interface) thực tế; **không chứng minh hai máy vật lý độc lập qua mạng** (`generator_colocated=true`, `physical_ab=NOT RUN`).
  - Các thử nghiệm E2–E5, collector phân tán và triển khai thực tế (live rollout) hoàn toàn nằm ngoài phạm vi E1.

### 1.2 Phân định ranh giới bảo mật giữa Host A và Host B

- **Cô lập chứng thư và quyền hạn**:
  - Client bundle được tạo ra tuyệt đối **không chứa** chứng thư quản trị (`admin`), thông tin đăng nhập cơ sở dữ liệu (`DB credentials`), hay quyền quản lý DynSec (`manager credential`). Host A không gửi bất kỳ chứng thư đặc quyền nào sang Host B.
  - Host B chỉ nhận token và chứng thư TLS ứng với vai trò client/viewer thông thường đã được phân quyền tối thiểu.
- **Kênh truyền dữ liệu độc lập (Manual Transfer Step)**:
  - Việc chuyển giao bundle và binary từ Host A sang Host B, cũng như chuyển tập kết quả từ Host B về Host A, được thực hiện qua các thao tác sao chép tập tin thủ công đã được kiểm duyệt (qua đường dẫn lưu trữ hoặc kênh truyền an toàn); công cụ không tích hợp bất kỳ cơ chế điều khiển tự động qua SSH hay control-plane từ xa nào.
- **Tính bất biến của định danh phiên (Session Invariance)**:
  - Mã định danh pha `phase_id` được cố định là `pilot` trong phân đoạn thử nghiệm này.
  - Tuyệt đối không chỉnh sửa `phase_id`, `session_id`, `run_id` trong manifest hoặc kết quả xuất ra.

### 1.3 Quy định an toàn và phân quyền tập tin nghiêm ngặt

- **Placeholders và bảo vệ dữ liệu**:
  - Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`/path/to/private-network.json`, `$ROOT`, `SESSION_ID`).
  - Tuyệt đối không đưa địa chỉ IP máy thật, đường dẫn home cá nhân, thông tin định danh hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`): quyền `0700`.
- **Chống ghi đè tập tin và bảo vệ Workspace**:
  - Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả hay workspace cũ.
  - Thư mục `workspace` trên Host B **không được tạo trước** bằng lệnh `mkdir`; lệnh `bind` yêu cầu tự tạo mới thư mục này với quyền `0700`. Nếu thư mục đã tồn tại, `bind` sẽ từ chối thực hiện với lỗi `workspace_output_leaf_exists`.
- **Ngân sách thời gian (Deadline Budget)**:
  - Với E1 large-viewer, dùng ramp/readiness/wall đã sinh bởi execution plan và manifest; không dùng thời lượng cũ cố định hoặc kế hoạch schema-only khác prepare thật. Mốc đo hiện hành: 2.000 viewer ramp/ready/wall = 40/100/130 giây; 5.000 = 100/160/190 giây; 10.000 = 120/180/210 giây. Mặc định workload là measure 14 giây và drain 8 giây.
  - Bundle lifetime tính bảo thủ bằng tổng wall của các role chạy staggered (viewer và telemetry), không lấy riêng `measure_seconds` hay wall của một role. Collector A phải còn đủ lifecycle + 60 giây copy/import, đồng thời giữ 30 giây cleanup reserve; `begin` phải từ chối trước workload nếu session không còn đủ ngân sách.
  - `--deadline-seconds` vẫn phải bao phủ toàn chu kỳ vận hành trên A và B; đây là deadline của fixture, không thay thế giới hạn role/session/token đã kiểm tra.
- **Ranh giới thẩm tra kết quả và ngưỡng từ chối tài nguyên (Refusal Boundaries)**:
  - Trên Host A, trước khi khởi chạy child workload, fixture thực hiện đối chiếu các ngưỡng tài nguyên (`e1_resource_overlay`) với thông số đọc thực tế từ tiến trình (`readback`):
    - `WS_MAX_SESSIONS` của backend phải lớn hơn hoặc bằng số lượng viewers (`session_cap >= viewers`).
    - Nginx cần ngân sách connection và FD riêng: đặt `base = ceil((2 * viewers + 2 + 32) / worker_processes)`, `worker_connections = max(256, 2 * base + 32)`, `nofile = max(1024, worker_connections + 128)`. Phần dự phòng dành cho phân bố kết nối không đều và FD nội bộ; không coi tổng connection chia đều là capacity đã được chứng minh.
    - Số lượng file descriptor khả dụng của backend phải $\ge$ `viewers + 32` và của broker Mosquitto phải $\ge$ `2 + 32`.
    - Nếu bất kỳ chỉ số thực tế nào không đáp ứng, fixture lập tức từ chối (`e1_resource_gate_denied:*`) và dừng lại trước khi bắt đầu tải.
  - Lệnh `begin` chỉ được chấp nhận (ACK) khi status xác nhận `phase=MEASURING` và `collector_ready=true`.
  - Lệnh `finish-phase` chỉ chuyển trạng thái sang `VERIFYING` sau khi đã thực hiện đầy đủ các bước đối soát tương quan (correlation), kiểm tra projection, đối chiếu delta cơ sở dữ liệu, kiểm tra raw telemetry và các xác minh WebSocket terminal.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

Quy trình vận hành sử dụng ba terminal độc lập được phân định rõ:
- **Host A — Terminal A1**: Khởi chạy tiến trình `serve` và giữ fixture ở chế độ foreground.
- **Host A — Terminal A2**: Thực hiện các lệnh điều khiển phiên (`status`, `begin`, `finish-phase`, `finalize`, `abort`).
- **Host B — Terminal B**: Thực hiện các thao tác client (`bind`, `preflight`, `check-connection`, `run`, `export-results`).

Thiết lập biến môi trường thư mục gốc trên Host A (chỉ tạo thư mục cha `$ROOT`, **không tạo trước** thư mục lá `$ROOT/A` vì lệnh `serve` yêu cầu tự tạo mới):

```sh
ROOT=/var/tmp/opencode/p8-e1-run
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

### Bước 1 — Khởi động Server Fixture trên Host A (Terminal A1)

Tạo tập tin cấu hình mạng riêng tư theo tài liệu [`paper-p8-remote-lifecycle.md`](paper-p8-remote-lifecycle.md), chỉ định địa chỉ IPv4 riêng tư của Host A, các cổng HTTPS/MQTT-TLS hợp lệ và cấu hình SAN phù hợp. Tuyệt đối không ghi deployment secrets vào tập tin cấu hình.

Tiến trình `serve` hỗ trợ các tham số cấu hình workload E1:
- `--experiment E1`: Mặc định là `E1`.
- `--profile {pilot, official}`: Mặc định là `pilot`. Khi sử dụng profile `official` hoặc số lượng viewers > 64, bắt buộc phải có cờ `--official-opt-in`.
- `--e1-viewers`: Số lượng viewer sockets (mặc định 64; pilot mở rộng: 128; hỗ trợ từ 1 đến 10.000).
- `--e1-principals`: Số lượng principal / user (mặc định 2; cố định 2 cho topology 2 gateways).
- `--e1-density {mixed, sparse, dense, overlap}`: Mật độ phân bổ filter (mặc định `mixed`).
- `--e1-messages-per-target`: Số tin nhắn phát sinh trên mỗi target (mặc định 5; với 4 targets sẽ tạo tổng cộng 20 messages).
- `--deadline-seconds`: Ngân sách thời gian toàn phiên (mặc định 900 giây).

Khởi chạy tiến trình `serve` ở foreground trên Terminal A1 theo một trong hai công thức được hỗ trợ:

Đối với rung official large-viewer 2.000/5.000/10.000, chỉ chọn count sau resource preflight A/B và dùng profile `official` cùng `--official-opt-in`. Plan mặc định tương ứng ramp/ready/wall 40/100/130, 100/160/190, 120/180/210 giây; measure/drain mặc định 14/8 giây. Đây là time budget hữu hạn, không phải cam kết tốc độ handshake hay capacity. Bộ đếm nhận viewer được giới hạn hữu hạn theo `expected_deliveries × 8192` bytes; đây là receive accounting bound cho dataset, không phải RSS, bộ nhớ cấp phát trước hay wire bytes.

Mỗi lượt cần tạo fixture A mới và workspace/output B mới. Không sửa manifest, bundle hoặc results đã sinh và không dựa vào hot reload: artifacts được tạo từ snapshot fixture/workspace cụ thể, nên thay đổi tham số cần một lượt mới từ prepare.

**Công thức 1A — Chạy pilot mặc định chuẩn 64 viewers (`default64`)**:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --experiment E1 \
  --profile pilot \
  --e1-viewers 64 \
  --e1-principals 2 \
  --e1-density mixed \
  --e1-messages-per-target 5
```

**Công thức 1B — Chạy pilot mở rộng có chọn lọc 128 viewers (`selected128`)**:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --experiment E1 \
  --profile official \
  --official-opt-in \
  --e1-viewers 128 \
  --e1-principals 2 \
  --e1-density mixed \
  --e1-messages-per-target 5
```

*Lưu ý về cơ chế Fixture-owned Resource Overlay & Readback Gate*:
- Fixture tính toán overlay tài nguyên hữu hạn tương ứng (`e1_resource_overlay`): trần session `WS_MAX_SESSIONS = viewers`, trần bộ nhớ tham chiếu `WS_SESSION_REFERENCE_BYTES = 8.388.608` (8 MiB cố định, không nhân theo viewers), số worker Nginx `W = min(32, max(1, (viewers + 63) // 64))`. Với `base = ceil((2 * viewers + 2 + 32) / W)`, đặt `C = max(256, 2 * base + 32)` connections/worker và `nofile = max(1024, C + 128)`. Readback phải có đủ W worker thực, mỗi worker có soft limit đạt nofile và FD còn lại ít nhất `C + 128`. FD backend khả dụng phải đạt `viewers + 32`, broker Mosquitto đạt `2 + 32`. Ví dụ 1.000 viewer dùng 16 worker, C=288 và nofile=1024; đây là sizing để kiểm chứng, không phải capacity guarantee.
- Ranh giới từ chối: Trong pha khởi tạo, fixture đọc thực tế (`readback`) cấu hình và file descriptor khả dụng từ các tiến trình đang chạy. Nếu `WS_MAX_SESSIONS < viewers`, hoặc Nginx `worker_connections` / soft FD không đủ, hoặc FD khả dụng của backend/broker thấp hơn ngưỡng budget, fixture sẽ lập tức từ chối (`e1_resource_gate_denied:*`) và dừng lại trước khi sinh tải.
- Tiến trình `serve` tự động provision 2 Gateways, 2 Users, 4 Sensors, khởi tạo role manifests và xuất client bundle tại thư mục `$ROOT/A/client-bundle/`. Nhật ký foreground được ghi nhận tại `$ROOT/A/server.log`.

### Bước 2 — Kiểm tra trạng thái READY và lấy Session ID trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái của server:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E1
```

Chờ cho đến khi server phản hồi trạng thái:
- `preparation_status=E1_PREPARED`
- `phase=READY`
- Bundle artifact chứa đầy đủ `session_id`, `run_id`, `phase=pilot`, digest và topology xác thực:
  - Đối với `default64`: 2 Gateways / 4 Sensors (4 targets) / 2 Users/principals / 64 viewer sockets (`viewer-00000` đến `viewer-00063`) / 106 filters / 530 expected deliveries.
  - Đối với `selected128`: 2 Gateways / 4 Sensors (4 targets) / 2 Users/principals / 128 viewer sockets (`viewer-00000` đến `viewer-00127`) / 213 filters / 1.065 expected deliveries (5 messages/target).

Lưu `session_id` nhận được vào biến môi trường trên Terminal A2 để sử dụng cho toàn bộ các lệnh điều khiển tiếp theo (thay thế chuỗi placeholder `<session_id>` bằng giá trị hex thực tế nhận được từ lệnh status):

```sh
SESSION_ID=<session_id>
# Ví dụ: SESSION_ID=a1b2c3d4e5f6...
```

Đồng thời, Host A đã biên dịch sẵn binary thực thi đã qua kiểm duyệt tại đường dẫn `$ROOT/A/fixture/private/bin/paperload`. Kiểm tra và ghi nhận mã băm SHA-256 của binary:

```sh
sha256sum "$ROOT/A/fixture/private/bin/paperload"
```

### Bước 3 — Chuyển giao gói dữ liệu và binary sang Host B (Manual Transfer Step)

Thực hiện sao chép thủ công bundle và binary đã duyệt từ Host A sang Host B qua kênh truyền an toàn:
1. Sao chép thư mục `$ROOT/A/client-bundle/` sang Host B tại đường dẫn `$ROOT/B/received-bundle/`.
2. Sao chép tập tin thực thi `$ROOT/A/fixture/private/bin/paperload` sang Host B tại đường dẫn `$ROOT/B/paperload` (đảm bảo giữ nguyên vẹn từng byte và mode thực thi).
3. Thiết lập phân quyền nghiêm ngặt trên Host B (lưu ý: thư mục `$ROOT/B/workspace` tuyệt đối **không được tạo trước** để tránh lỗi `workspace_output_leaf_exists`):

```sh
ROOT=/var/tmp/opencode/p8-e1-run
mkdir -p "$ROOT/B" && chmod 0700 "$ROOT/B"
chmod 0700 "$ROOT/B/paperload"
find "$ROOT/B/received-bundle" -type d -exec chmod 0700 {} +
find "$ROOT/B/received-bundle" -type f -exec chmod 0600 {} +
```

Đối chiếu mã băm SHA-256 của tập tin `paperload` trên Host B để bảo đảm trùng khớp 100% với mã băm đã ghi nhận tại Bước 2 trên Host A. Máy chủ Host A sẽ kiểm tra hash của binary trong kết quả gửi về; binary do Host B tự biên dịch độc lập nếu có sai lệch byte sẽ bị từ chối dù cùng revision mã nguồn.

### Bước 4 — Thiết lập Workspace và kiểm tra kết nối trên Host B (Terminal B)

Trên Terminal B của Host B, tiến hành liên kết bundle, chạy tiền kiểm tra môi trường và kiểm tra kết nối mạng/xác thực thực tế:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$ROOT/B/received-bundle" \
  --workspace "$ROOT/B/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$ROOT/B/workspace"
```

- Lệnh `bind` kiểm tra cấu trúc bundle, tính toàn vẹn chữ ký/thời hạn token và sinh ra cấu hình thực thi `runtime/execution/run-spec.json`.
- Lệnh `preflight` thực thi Go binary `paperload` với cờ `--preflight` để xác thực cú pháp manifest và kiểm tra quyền tập tin cục bộ; bước này **hoàn toàn không mở socket mạng hay thực hiện kết nối**.
- Lệnh `check-connection` thăm dò thực tế kết nối HTTPS, thực hiện bắt tay WebSocket `/v1/ws` và probe kết nối MQTT TLS với các credentials không đặc quyền trong bundle.
- Tuyệt đối không bắt đầu workload nếu bước `preflight` hoặc kết nối TLS/Auth/WS/MQTT thất bại.

### Bước 5 — Kích hoạt pha đo lường trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh kích hoạt bắt đầu pha đo lường và kiểm tra trạng thái sẵn sàng của bộ thu thập chỉ số:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id pilot \
  --experiment E1

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E1
```

Lệnh `begin` chỉ phát hành yêu cầu khởi động collector. Tuyệt đối **chưa được chạy** workload trên Host B cho đến khi lệnh `status` phản hồi xác nhận:
- `phase=MEASURING`
- `phase_id=pilot`
- `collector_ready=true`

Toàn bộ các số đếm cơ sở dữ liệu cơ sở (baseline aggregate và per-target DB counts) được Host A ghi nhận chính xác trước khi bộ thu thập bắt đầu thu thập chỉ số.

### Bước 6 — Chạy tải phát tin và xuất kết quả trên Host B (Terminal B)

Trên Terminal B của Host B, tiến hành chạy tải phát tin mô phỏng và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/exported-results"
```

- Lệnh `run` thực thi workload pilot theo đúng manifest đã liên kết; tiến trình giám sát (supervisor) phía Host B có giới hạn thời gian thực thi và bảo toàn bằng chứng kiểm thử nếu runner gặp sự cố. Lưu ý: Lệnh `run` và `export-results` của E1 **không truyền cờ `--phase`** (cờ `--phase` là tham số riêng của E4/E5).
- Lệnh `export-results` chỉ xuất thông tin tóm tắt runner (runner summary), nguồn gốc binary (provenance), bộ đếm workload theo vai trò (viewers sử dụng ID giả danh `viewer-NNNNN`, không lộ thông tin user) và tối đa 20 header-hash identity samples. Lệnh này tuyệt đối không xuất nhật ký thô, nội dung payload, chứng thư hay đường dẫn riêng tư của hệ thống. Không tự ý tổng hợp tập tin JSON thủ công để thay thế result bundle chuẩn tắc.

### Bước 7 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/B/exported-results/` từ Host B sang Host A tại thư mục riêng biệt `$ROOT/A/received-results/` qua thao tác sao chép tập tin đã duyệt.

Trên Host A, thiết lập phân quyền an toàn:

```sh
chmod 0700 "$ROOT/A/received-results"
find "$ROOT/A/received-results" -type f -exec chmod 0600 {} +
```

*Lưu ý: Tuyệt đối không sao chép đè vào thư mục client-bundle gốc.*

### Bước 8 — Hậu kiểm dữ liệu và hoàn tất dọn dẹp trên Host A (Terminal A2)

Trên Terminal A2 của Host A, nhập tập kết quả đã nhận để thực hiện xác thực và đối soát tính toàn vẹn dữ liệu:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase MEASURING \
  --phase-id pilot \
  --results "$ROOT/A/received-results" \
  --experiment E1

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E1
```

Chờ cho đến khi trạng thái chuyển sang `phase=VERIFYING`. Kiểm tra trường `imported_result` để xác nhận:
- `validation_valid=true`
- `validation_failures=[]`
- `workload_outcome=VALIDATED`

Hệ thống thẩm tra đối chiếu nghiêm ngặt:
- **Cơ sở dữ liệu delta**: Khớp chính xác 20 messages, 2.000 raw samples, 20 ledgers, 20 temporal rows, 0 configuration changes.
- **WebSocket deliveries / writes**: Khớp chính xác 530 writes (với `default64`) hoặc 1.065 writes (với `selected128`, 5 messages/target).
- **Fanout mode**: Bắt buộc là `indexed` (`fanout_not_indexed` bị từ chối); số lượng frame drops bằng 0 (`drops=0`).
- **Raw identity**: Mismatch bằng 0 (`raw_identity_value_mismatches=0`).

Bất kỳ kết quả nào sai lệch lược đồ (schema), thiếu role, sai lệch session/run/phase/binary/manifest provenance, chứa đường dẫn lạ, symlink hoặc vượt ngưỡng giới hạn đều sẽ bị từ chối; hệ thống sẽ giữ nguyên ở `phase=MEASURING` và không cấp chứng nhận PASS. Khi đó người vận hành có thể gửi tập kết quả hợp lệ khác hoặc phát lệnh hủy bỏ (`abort`).

Khi trạng thái đã đạt `phase=VERIFYING` và kết quả được xác nhận hợp lệ, thực hiện hoàn tất và dọn dẹp môi trường:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase VERIFYING \
  --experiment E1
```

*(Trường hợp workload hoặc bước import gặp lỗi không thể khắc phục, người vận hành phát lệnh hủy phiên từ phase hiện tại; thay thế `<phase-hiện-tại>` bằng phase ghi nhận từ status, ví dụ `READY`, `MEASURING` hoặc `VERIFYING`)*:

```sh
python3 scripts/paper_p8_server.py abort \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase <phase-hiện-tại> \
  --experiment E1
```

Chờ cho đến khi tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Sau đó kiểm tra trạng thái kết thúc trên Terminal A2:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E1
```

Xác nhận các trường trạng thái cuối cùng:
- `phase=CLOSED`
- `cleanup_outcome=VERIFIED`

Toàn bộ báo cáo JSON, các tệp số liệu CSV (`metrics.csv`, `resources.csv`) và bằng chứng khởi động/thực thi được lưu trữ an toàn trong `$ROOT/A/fixture/artifacts/`. Các bằng chứng này ghi lại đầy đủ trạng thái mã nguồn (source/dirty state), lược đồ DB, quota/flags, image IDs/digests, ánh xạ topology, kết quả xác thực và các chỉ số vận hành thực tế.

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

### 3.1 Bảng kết quả thực chạy pilot E1 (`P8.6-E1-final-pilot`)

Kết quả thử nghiệm pilot E1 được ghi nhận chính thức trong hồ sơ kiểm định hệ thống:

| Hạng mục kiểm định | Giá trị thực nghiệm | Ghi chú và Ranh giới kỹ thuật |
|---|---:|---|
| **Trạng thái tổng thể (Status)** | **PASS** | `PAPER_P8_E1_VERIFIED` (source-qualified) |
| **Logical Artifact ID** | `P8.6-E1-final-pilot` | Định danh đợt chạy pilot chính thức |
| **Mã băm báo cáo thô (Raw Report SHA256)** | `d3c19bf2b3e2af898a169532ce9f6bd029b64f09f25d360ac1e6aba4614b8b0f` | Checksum SHA-256 của tệp JSON kết quả gốc |
| **Cấu trúc Topology** | 2 Gateways, 4 Sensors (4 targets), 2 Users/principals, 64 viewer sockets, 106 filters | 64 sockets kết nối / 2 principals |
| **Unique Messages cam kết** | 20 messages | 20 unique messages được ghi nhận thành công |
| **Raw Samples cơ sở dữ liệu** | 2.000 samples | Khớp chính xác 100 samples/message x 20 messages |
| **WebSocket Writes** | 530 writes | 530 expected deliveries theo mapping chuẩn (106 filters / 4 targets), hoàn tất 100%, không frame drop |
| **Dọn dẹp tài nguyên (Cleanup)** | 12 containers, 1 network, 5 volumes | **VERIFIED** — dọn sạch chính xác tuyệt đối theo receipts |
| **Tuyên bố năng lực (Capacity Claim)** | `false` | Workload ngắn hữu hạn; không phải kiểm tra năng lực chịu tải |
| **Mã thoát của Server (Serve exit)** | 0 | Tiến trình `serve` kết thúc sạch, không lỗi |

*Lưu ý về tiến độ bổ sung*: Kết quả thử nghiệm pilot mở rộng 128 viewers (`selected128`) và pilot 32 viewers (`selected32`) cùng các phân tích chi tiết về tài nguyên readback đã được chuyển và lưu trữ đầy đủ tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#e1--tài-nguyên-fixture-và-pilot).

### 3.2 Lịch sử kiểm thử và bảo toàn các kết quả thất bại

- **Bảo toàn kết quả FAIL lịch sử**: Lượt chạy thất bại ở outer report trước đó do lỗi bao ngoài được bảo toàn nguyên vẹn trong nhật ký kiểm thử lịch sử, không xóa bỏ hoặc sửa đổi nhãn FAIL cũ thành PASS.
- **Tính bất biến của binary và source revision**: Mã băm binary `paperload` được đối chiếu chính xác giữa Host A và Host B. Mọi artifact lưu giữ fingerprint mã nguồn riêng biệt; không suy diễn hay gán ghép kết quả giữa các phiên bản mã nguồn khác nhau.

---

## 4. Giới hạn và trạng thái gate

- **Trạng thái cổng P8**: `full_p8_accepted=false`. Kết quả E1 là một pilot hữu hạn xác thực cơ chế phân quyền, luồng kết nối và xác minh dữ liệu đa vai trò; đây không phải là nghiệm thu toàn diện P8.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm được thực hiện trên cùng máy chủ cục bộ với giao diện mạng riêng (`generator_colocated=true`); chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng vật lý.
- **Phân định rõ ranh giới các mức thử nghiệm**:
  - `default64`: Pilot hữu hạn baseline phục vụ xác thực toàn trình.
  - `selected128`: Pilot hữu hạn mở rộng trên môi trường kiểm chuẩn cục bộ có áp dụng overlay và opt-in.
  - `offline5000` và `offline10000`: Hợp đồng dry-run kiểm tra schema và plan, không phải kiểm tra năng lực chịu tải (capacity) và không được coi là nghiệm thu tải thực tế.
- **Các kịch bản chưa thực hiện**:
  - Ma trận lặp lại chính thức E1–E5 (≥ 3 lượt chạy lặp).
  - Thử nghiệm tải cao, dung lượng và ngâm tải dài hạn (high-load / capacity / soak).
  - Đo đạc A/B trên máy vật lý từ xa qua đường truyền mạng thực tế.
  - Kiểm thử thời gian sống thực tế của token với TTL wall-clock 600s.
  - Kiểm thử trên phần cứng thật HIL (bo mạch AM5728) và chip cảm biến vật lý.
  - Triển khai thực tế trên môi trường production (live rollout).
  Toàn bộ các hạng mục trên đều được ghi nhận là **NOT RUN**.
- **Ranh giới dừng**: E1 là bước pilot khởi đầu; dừng lại tại mốc bàn giao E1 đã được nghiệm thu cục bộ. Không tự động mở rộng sang E2–E5, không thay đổi cấu hình triển khai mặc định, và không thực hiện commit/push/rollout nếu chưa có yêu cầu mới.

---

## Phụ lục — Offline contract và A resource gate

Dry-run kiểm tra mapping → role manifests → bundle export → inspect → bind → Go `paperload --preflight --official-opt-in`. Không đọc `$ROOT`/network config, không mở kết nối mạng hoặc Docker, không khởi chạy workload; đối số đường dẫn vẫn bắt buộc theo giao diện CLI. Identity trong bundle là JWT unsigned `schema-only`, không dùng với dịch vụ thật. Không coi dry-run là load/capacity acceptance.

Hàm `build_e1_offline_contract` trong mã nguồn hỗ trợ các mức rung `(128, 5000, 10000)` với đúng 2 principals (`e1_fixture_principal_topology_unsupported` nếu khác 2; `e1_offline_rung_unsupported` nếu ngoài danh sách trên). Khi chạy dry-run với profile `official`, bắt buộc phải có cờ `--official-opt-in`.

```sh
for n in 5000 10000; do
  python3 scripts/paper_p8_server.py serve \
    --root /var/tmp/opencode/p8-supplement/e1/unused \
    --network-config /var/tmp/opencode/p8-supplement/e1/unused.json \
    --dry-run --experiment E1 --profile official --official-opt-in \
    --e1-viewers "$n" --e1-principals 2 --e1-density mixed \
    --e1-messages-per-target 5
done
```

Kết quả và kích thước của các lượt đã chạy được ghi tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#e1--tài-nguyên-fixture-và-pilot). Input users=2 là topology identities, không phải 5.000/10.000 principals. Chỉ hai principals được fixture hỗ trợ; principal khác 2 bị từ chối.

Plan schema-only và prepare thật dùng chung execution plan. Với 2.000/5.000/10.000 viewer, ramp/ready/wall lần lượt là 40/100/130, 100/160/190 và 120/180/210 giây; mặc định measure/drain là 14/8 giây. Readiness 160/180 giây chỉ áp dụng cho E1 viewer manifest hợp lệ, không nới cap cho role/bài khác. Bundle lifetime là tổng bảo thủ của wall các role staggered; A collector/session guard tính lifecycle + 60 giây copy/import và giữ thêm 30 giây cleanup reserve. Receive cap tường minh dựa trên expected deliveries × 8192 bytes, với số lượng hữu hạn và trần validator; không diễn giải thành RSS/wire bound. Resource estimate dùng overlay connection/FD có dự phòng như Bước 1, backend FDs available >= viewers + 32 và broker >= 2 telemetry + 32 reserve. Offline dry-run không đọc cấu hình Host A (`effective_a_limits = not_read_in_offline_dry_run`); effective A thresholds luôn phải được readback mới trước fixture E1 thực.

Trong E1 fixture prepare, trước khi khởi child workload, code đọc `WS_MAX_SESSIONS`, Nginx `worker_connections`, master nofile và giới hạn/FD đang mở của từng worker. Thiếu worker readback, thiếu connection/FD dự phòng hoặc thiếu FD backend/broker đều khiến gate từ chối. Overlay chỉ thuộc fixture, không đổi sysctl hay deployment defaults. Ví dụ session cap 64 không đủ rung 5.000; Nginx 1 worker × 1.024 connections cũng không đủ. Giá trị ví dụ không khẳng định cấu hình hiện tại; chỉ readback của fixture thực mới quyết định có được khởi workload.
