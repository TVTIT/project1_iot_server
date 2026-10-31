# P8 E1 — chuẩn bị, chạy pilot và nhập kết quả (source-qualified)

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E1 (pilot hữu hạn giữa hai host A và B). E1 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST và Mosquitto TLS).

Trong thử nghiệm này, Host A sở hữu fixture và collector; Host B liên kết bundle, kiểm tra kết nối, thực thi hai role workload bằng Go binary (`paperload`), rồi xuất một projection kết quả đã giới hạn để Host A xác minh đối chiếu với cơ sở dữ liệu và telemetry thực tế. Thử nghiệm này là một pilot hữu hạn, không phải nghiệm thu toàn diện P8 (`full_p8_accepted=false`).

---

## 1. Tổng quan và nguyên tắc vận hành

### 1.1 Phạm vi và mục tiêu thử nghiệm E1

- **Mô hình phối hợp hai host**:
  - **Host A (Server / Fixture / Collector)**: Khởi chạy môi trường dịch vụ backend đầy đủ (Nginx, Go backend, TimescaleDB, Mosquitto DynSec), cấp phát tài nguyên mẫu (provisioning), tạo manifest và đóng gói bundle gửi cho client, đồng thời thực hiện đo lường tài nguyên và hậu kiểm dữ liệu.
  - **Host B (Client Runner)**: Nhận bundle và binary từ Host A, thực hiện liên kết workspace, tiền kiểm tra (preflight), kiểm tra kết nối mạng/xác thực, thực thi workload mô phỏng và xuất tập kết quả đo lường (projection).
- **Topology thử nghiệm E1**:
  - 2 Gateways, 4 Sensors (2 sensor/gateway), 2 Users, 2 Viewers (sử dụng định danh giả danh `viewer-NNNNN`), 64 sockets kết nối WebSocket / MQTT.
  - Workload phát sinh 20 unique messages (mỗi tin nhắn chứa 100 samples, tương ứng 2.000 raw samples) và 530 lượt ghi WebSocket (WebSocket writes).
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
  - Thời hạn deadline của server `--deadline-seconds 900` (15 phút) phải bảo đảm đủ cho toàn bộ chu kỳ: khởi động container, truyền bundle và binary, kiểm tra kết nối, thực thi workload pilot, truyền kết quả về, hậu kiểm dữ liệu và dọn dẹp tài nguyên.
- **Ranh giới thẩm tra kết quả**:
  - Trên Host A, lệnh `begin` chỉ được chấp nhận (ACK) khi status xác nhận `phase=MEASURING` và `collector_ready=true`.
  - Lệnh `finish-phase` chỉ chuyển trạng thái sang `VERIFYING` sau khi đã thực hiện đầy đủ các bước đối soát tương quan (correlation), kiểm tra projection, đối chiếu delta cơ sở dữ liệu, kiểm tra raw telemetry và các xác minh WebSocket terminal.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

Quy trình vận hành sử dụng ba terminal độc lập được phân định rõ:
- **Host A — Terminal A1**: Khởi chạy tiến trình `serve` và giữ fixture ở chế độ foreground.
- **Host A — Terminal A2**: Thực hiện các lệnh điều khiển phiên (`status`, `begin`, `finish-phase`, `finalize`).
- **Host B — Terminal B**: Thực hiện các thao tác client (`bind`, `preflight`, `check-connection`, `run`, `export-results`).

Thiết lập biến môi trường thư mục gốc trên Host A (chỉ tạo thư mục cha `$ROOT`, **không tạo trước** thư mục lá `$ROOT/A` vì lệnh `serve` yêu cầu tự tạo mới):

```sh
ROOT=/var/tmp/opencode/p8-e1-run
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

### Bước 1 — Khởi động Server Fixture trên Host A (Terminal A1)

Tạo tập tin cấu hình mạng riêng tư theo tài liệu [`paper-p8-remote-lifecycle.md`](paper-p8-remote-lifecycle.md), chỉ định địa chỉ IPv4 riêng tư của Host A, các cổng HTTPS/MQTT-TLS hợp lệ và cấu hình SAN phù hợp. Tuyệt đối không ghi deployment secrets vào tập tin cấu hình.

Khởi chạy tiến trình `serve` ở foreground trên Terminal A1 (tiến trình này phải được giữ mở trong suốt quá trình chạy thử nghiệm):

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900
```

Tiến trình `serve` sẽ khởi dựng profile E1, tự động provision 2 Gateway, 2 User, 4 Sensor, khởi tạo role manifests và xuất client bundle tại thư mục `$ROOT/A/client-bundle/`. Nhật ký foreground được ghi nhận tại `$ROOT/A/server.log`.

### Bước 2 — Kiểm tra trạng thái READY và lấy Session ID trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái của server:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A"
```

Chờ cho đến khi server phản hồi trạng thái:
- `preparation_status=E1_PREPARED`
- `phase=READY`
- Bundle artifact chứa đầy đủ `session_id`, `run_id`, `phase=pilot`, digest và topology chuẩn 2 Gateways / 4 Sensors / 2 Users / 2 Viewers.

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

Tuyệt đối không bắt đầu workload nếu bước `preflight` hoặc kết nối TLS/Auth/WS/MQTT thất bại.

### Bước 5 — Kích hoạt pha đo lường trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh kích hoạt bắt đầu pha đo lường và kiểm tra trạng thái sẵn sàng của bộ thu thập chỉ số:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id pilot

python3 scripts/paper_p8_server.py status --root "$ROOT/A"
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

- Lệnh `run` thực thi workload pilot theo đúng manifest đã liên kết; tiến trình giám sát (supervisor) phía Host B có giới hạn thời gian thực thi và bảo toàn bằng chứng kiểm thử nếu runner gặp sự cố.
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
  --results "$ROOT/A/received-results"

python3 scripts/paper_p8_server.py status --root "$ROOT/A"
```

Chờ cho đến khi trạng thái chuyển sang `phase=VERIFYING`. Kiểm tra trường `imported_result` để xác nhận:
- `validation_valid=true`
- `validation_failures=[]`
- `workload_outcome=VALIDATED`

Bất kỳ kết quả nào sai lệch lược đồ (schema), thiếu role, sai lệch session/run/phase/binary/manifest provenance, chứa đường dẫn lạ, symlink hoặc vượt ngưỡng giới hạn đều sẽ bị từ chối; hệ thống sẽ giữ nguyên ở `phase=MEASURING` và không cấp chứng nhận PASS. Khi đó người vận hành có thể gửi tập kết quả hợp lệ khác hoặc phát lệnh hủy bỏ (`abort`).

Khi trạng thái đã đạt `phase=VERIFYING` và kết quả được xác nhận hợp lệ, thực hiện hoàn tất và dọn dẹp môi trường:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```

*(Trường hợp workload hoặc bước import gặp lỗi không thể khắc phục, người vận hành phát lệnh hủy phiên từ phase hiện tại; thay thế `<phase-hiện-tại>` bằng phase ghi nhận từ status, ví dụ `READY`, `MEASURING` hoặc `VERIFYING`)*:

```sh
python3 scripts/paper_p8_server.py abort \
  --root "$ROOT/A" \
  --session-id "$SESSION_ID" \
  --phase <phase-hiện-tại>
```

Chờ cho đến khi tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Sau đó kiểm tra trạng thái kết thúc trên Terminal A2:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A"
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
| **Cấu trúc Topology** | 2 Gateways, 4 Sensors, 2 Users, 2 Viewers | 64 sockets kết nối / 2 principals |
| **Unique Messages cam kết** | 20 messages | 20 unique messages được ghi nhận thành công |
| **Raw Samples cơ sở dữ liệu** | 2.000 samples | Khớp chính xác 100 samples/message x 20 messages |
| **WebSocket Writes** | 530 writes | Toàn bộ frames được gửi thành công, không ghi nhận frame drop |
| **Dọn dẹp tài nguyên (Cleanup)** | 12 containers, 1 network, 5 volumes | **VERIFIED** — dọn sạch chính xác tuyệt đối theo receipts |
| **Tuyên bố năng lực (Capacity Claim)** | `false` | Workload ngắn hữu hạn; không phải kiểm tra năng lực chịu tải |
| **Mã thoát của Server (Serve exit)** | 0 | Tiến trình `serve` kết thúc sạch, không lỗi |

### 3.2 Lịch sử kiểm thử và bảo toàn các kết quả thất bại

- **Bảo toàn kết quả FAIL lịch sử**: Lượt chạy thất bại ở outer report trước đó do lỗi bao ngoài được bảo toàn nguyên vẹn trong nhật ký kiểm thử lịch sử, không xóa bỏ hoặc sửa đổi nhãn FAIL cũ thành PASS.
- **Tính bất biến của binary và source revision**: Mã băm binary `paperload` được đối chiếu chính xác giữa Host A và Host B. Mọi artifact lưu giữ fingerprint mã nguồn riêng biệt; không suy diễn hay gán ghép kết quả giữa các phiên bản mã nguồn khác nhau.

---

## 4. Giới hạn và trạng thái gate

- **Trạng thái cổng P8**: `full_p8_accepted=false`. Kết quả E1 là một pilot hữu hạn xác thực cơ chế phân quyền, luồng kết nối và xác minh dữ liệu đa vai trò; đây không phải là nghiệm thu toàn diện P8.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm được thực hiện trên cùng máy chủ cục bộ với giao diện mạng riêng (`generator_colocated=true`); chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng vật lý.
- **Các kịch bản chưa thực hiện**:
  - Ma trận lặp lại chính thức E1–E5 (≥ 3 lượt chạy lặp).
  - Thử nghiệm tải cao, dung lượng và ngâm tải dài hạn (high-load / capacity / soak).
  - Đo đạc A/B trên máy vật lý từ xa qua đường truyền mạng thực tế.
  - Kiểm thử thời gian sống thực tế của token với TTL wall-clock 600s.
  - Kiểm thử trên phần cứng thật HIL (bo mạch AM5728) và chip cảm biến vật lý.
  - Triển khai thực tế trên môi trường production (live rollout).
  Toàn bộ các hạng mục trên đều được ghi nhận là **NOT RUN**.
- **Ranh giới dừng**: E1 là bước pilot khởi đầu; dừng lại tại mốc bàn giao E1 đã được nghiệm thu cục bộ. Không tự động mở rộng sang E2–E5, không thay đổi cấu hình triển khai mặc định, và không thực hiện commit/push/rollout nếu chưa có yêu cầu mới.
