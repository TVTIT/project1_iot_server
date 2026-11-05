# P8 E3 — profile helper (source-qualified)

> Pure helper ở `scripts/paper_p8_e3_support.py`; native fixture ở
> `scripts/tests/paper_p8_e3_fixture.py`. Official 30-command validation cần
> `--official-opt-in`. Offline Sensor chỉ chứng minh
> committed admission/replay (desired/command/receipt); không chứng minh offline
> delivery/success. Delivery/result chỉ được kết luận từ online executor sau
> READY_MARKER/SUBACK. Kết quả từng lượt nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e3--subset-ba-lệnh-và-chẩn-đoán-probe).

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E3 (khảo sát năng lực điều khiển Sensor phối hợp viễn trắc qua bốn profile kết hợp giữa hai chế độ tải `low_load` / `mixed` và hai cơ chế dispatch `polling` / `wakeup`). E3 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST và Mosquitto TLS) kết hợp với Gateway simulator executor hai arm độc lập.

Thử nghiệm chạy tuần tự các profile trong từng fixture độc lập; mỗi profile được kiểm định dữ liệu và dọn dẹp tài nguyên độc lập. Báo cáo luôn giữ `full_p8_accepted=false`; pilot cùng máy ghi nhận `generator_colocated=true` (hoặc `same-host/unverified`), `physical_ab=NOT RUN`. Không chứng minh phần cứng HIL (bo mạch AM5728) hay triển khai môi trường live rollout.

---

## 1. Tổng quan và phân biệt bốn profile E3

Thử nghiệm E3 khảo sát sự tương tác giữa luồng dữ liệu viễn trắc (telemetry uplink) và luồng lệnh điều khiển cảm biến (sensor control loop) qua 4 profile độc lập nằm hoàn toàn trong phạm vi thử nghiệm E3 (`--experiment E3`):

### 1.1 Ma trận cấu hình 4 Profile của E3

| Đặc tính / Cấu hình | Profile 1: Low-Load Polling | Profile 2: Low-Load Wakeup | Profile 3: Mixed Polling | Profile 4: Mixed Wakeup |
|---|---|---|---|---|
| **Tham số `--workload`** | `low_load` | `low_load` | `mixed` | `mixed` |
| **Tham số `--dispatch-mode`** | `polling` | `wakeup` | `polling` | `wakeup` |
| **Cờ `STAGE5_WAKEUP_ENABLED`** | `false` | `true` | `false` | `true` |
| **Cờ `STAGE5_ENABLED`** | `true` | `true` | `true` | `true` |
| **Telemetry Arm A (Steady)** | 8 messages (1 msg/s) | 8 messages (1 msg/s) | 8 messages (1 msg/s) | 8 messages (1 msg/s) |
| **Telemetry Arm B (Traffic)** | 4 messages (1 msg/s, không burst) | 4 messages (1 msg/s, không burst) | 16 messages (4 steady + 12 burst) | 16 messages (4 steady + 12 burst) |
| **Tổng Telemetry Unique Messages** | 12 messages | 12 messages | 24 messages | 24 messages |
| **Lệnh điều khiển (Control Commands)** | 4 lệnh (2 lệnh / executor) | 4 lệnh (2 lệnh / executor) | 4 lệnh (2 lệnh / executor) | 4 lệnh (2 lệnh / executor) |
| **Tần số lệnh (Frequency Hz)** | 1.5, 0.5, 2.0, 0.75 Hz | 1.5, 0.5, 2.0, 0.75 Hz | 1.5, 0.5, 2.0, 0.75 Hz | 1.5, 0.5, 2.0, 0.75 Hz |
| **Quy tắc Overlap Clock** | Burst overlap: NOT APPLICABLE | Burst overlap: NOT APPLICABLE | Bắt buộc overlap cả A steady & B burst | Bắt buộc overlap cả A steady & B burst |
| **Cấu hình triển khai** | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) |

### 1.2 Giải thích cờ `STAGE5_WAKEUP_ENABLED` và Tính tương đương Cấu hình

- **Bản chất chuyển đổi Polling vs Wakeup**: Giữa hai cơ chế dispatch `polling` và `wakeup`, hệ thống chỉ thay đổi duy nhất một cờ môi trường `STAGE5_WAKEUP_ENABLED` (`false` ở polling, `true` ở wakeup; cả hai đều có `STAGE5_ENABLED=true`).
- **Bảo toàn ngữ nghĩa**: Toàn bộ manifest luồng dữ liệu (traffic manifest), manifest lệnh điều khiển (control manifest), định danh roles, các thông số profile, hạn ngạch quota và ngữ nghĩa nghiệp vụ của API được giữ nguyên vẹn 100%, không bị sửa đổi giữa polling và wakeup.
- **Sự khác biệt giữa `low_load` và `mixed`**:
  - `low_load`: Arm A phát 8 messages (steady 1 msg/s), Arm B phát 4 messages (steady 1 msg/s, không kích hoạt burst). Tổng telemetry unique messages là 12. Do Arm B không có burst, chỉ tiêu burst overlap là **NOT APPLICABLE**; hệ thống chỉ kiểm tra command interval giao với A steady interval.
  - `mixed`: Giữ nguyên cấu hình tải hỗn hợp với Arm A phát 8 messages (steady 1 msg/s) và Arm B phát 16 messages (4 steady + 12 burst). Tổng telemetry unique messages là 24. Yêu cầu bắt buộc phải chứng minh sự giao thoa thời gian (overlap) giữa lệnh điều khiển và cả hai luồng A steady lẫn B burst.

### 1.3 Phân biệt hai Logical Producers và hai Physical Hosts

Cần phân biệt rõ ràng hai khái niệm độc lập:
- **Hai Logical Producers / Gateway Arms (A và B)**:
  - Là hai nhánh mô phỏng tải logic trong workload:
    - Arm A: Sở hữu 1 Gateway và 1 Sensor cho luồng telemetry nền (`steady`).
    - Arm B: Sở hữu 1 Gateway và 1 Sensor cho luồng telemetry (`steady` ở low_load; `steady` + `burst` ở mixed).
  - Đồng thời, E3 vận hành 2 Gateway executor độc lập (`gw_a`, `gw_b`) nhận và thực thi 4 lệnh điều khiển sensor (mỗi executor nhận 2 lệnh tuần tự).
  - Mỗi session tạo mới định danh Gateway và Sensor fresh; ánh xạ role-to-identity và cấu hình executor tương ứng không thay đổi giữa các arm.
- **Hai Physical Hosts (Host A và Host B)**:
  - **Host A (Server Host)**: Máy chủ chạy hạ tầng Go backend, PostgreSQL/TimescaleDB, Mosquitto DynSec, Nginx reverse proxy, và bộ thu thập chỉ số phía server (`collector`).
  - **Host B (Client Host)**: Máy trạm chạy tiến trình phát tải viễn trắc (`paperload`) và tiến trình executor nhận lệnh điều khiển (`gateway`).
  - **Giới hạn máy đo**: Thử nghiệm được người vận hành chạy trên cùng máy chủ cục bộ với metadata ghi nhận là `generator_colocated=true` (hoặc `same-host/unverified`); chưa chứng minh môi trường hai máy vật lý độc lập qua mạng vật lý (`physical_ab=NOT RUN`).

### 1.4 Quy tắc Overlap Clock của chế độ Mixed

- **Bắt buộc chứng minh Overlap**: Trong chế độ `mixed`, điều kiện tiên quyết để nghiệm thu kết quả là đồng hồ phía runner (`runner-clock`) phải chứng minh khoảng thời gian thực thi lệnh điều khiển (`command interval`) giao thoa đồng thời với cả luồng A steady và luồng B burst.
- **Độ trễ ống dẫn chưa xác định (Unknown Pipe Lag)**: Độ trễ truyền thông qua ống dẫn mạng giữa các tiến trình là chưa xác định (`unknown`); tuyệt đối không thực hiện phép trừ đồng hồ giữa hai máy Host A và Host B (no A/B clock subtraction) để tránh sai số trôi đồng hồ (clock drift) ngụy tạo độ trễ.
- **Chế độ `low_load`**: Do Arm B không phát burst, trường xác thực burst overlap được ghi nhận chuẩn mực là **`NOT APPLICABLE`**, không đánh giá FAIL.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`/path/to/private-network-copy.json`, `$ROOT`, `$SESSION_ID`). Tuyệt đối không đưa IP máy thật, đường dẫn home cá nhân, thông tin định danh hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ, SQLite và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`, `gateway`): quyền `0700`.
- **Chống ghi đè tập tin và cách ly môi trường**: Mỗi lượt chạy bắt buộc sử dụng một thư mục gốc hoàn toàn mới (`ROOT=/var/tmp/opencode/p8-e3-<profile>-<unique-run>`). Tuyệt đối không tái sử dụng thư mục đã có state, bundle, SQLite hay artifacts cũ.
- **Ngân sách thời gian (Deadline budget)**: Thời hạn deadline của server `--deadline-seconds 900` (15 phút) phải đủ cho toàn bộ chu kỳ: khởi động container, truyền bundle và 2 binary, chạy workload, chờ drain, export/import, hậu kiểm đối soát SQL/REST/SQLite và dọn dẹp.
- **Trình tự đồng bộ Collector nghiêm ngặt**: Bộ thu thập phía server trên Host A bắt buộc phải được kích hoạt và xác nhận sẵn sàng (`phase=MEASURING`, `collector_ready=true`) TRƯỚC KHI tiến trình client trên Host B được phép khởi chạy phát tải.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Steps)**: Việc chuyển bundle, 2 file nhị phân (`paperload`, `gateway`) và kết quả đo giữa Host A và Host B là thao tác truyền file thủ công của người vận hành (copy an toàn qua đường dẫn hoặc scp), không tuyên bố là tự động hóa qua SSH.
- **Xử lý sự cố và dọn dẹp an toàn**: Khi request hoặc kết quả bị từ chối, giữ nguyên hiện trạng các file SQLite, outbox, journal và artifacts để phục vụ chẩn đoán lỗi; không tự ý reseed hay ghi đè. Quy trình `finalize` (hoặc `abort`) vẫn là kênh đóng phiên chính thức để giải phóng an toàn 100% tài nguyên Docker.

---

### 2.2 Quy trình vận hành chi tiết 8 bước

Áp dụng cho bất kỳ profile nào trong 4 profile E3 bằng cách chọn cặp cờ tương ứng:
- `--dispatch-mode polling|wakeup`: Cơ chế điều phối lệnh (thay đổi cờ `STAGE5_WAKEUP_ENABLED`).
- `--workload low_load|mixed`: Chế độ tải viễn trắc nền (12 messages ở low_load, 24 messages ở mixed).

**Các tùy chọn số lượng lệnh, lịch trình và hồ sơ vận hành:**
- **Mặc định:** Tạo 4 lệnh điều khiển (`--e3-command-count 4`), lịch phát dồn `--e3-schedule smoke`, và hồ sơ `--profile pilot` với ngân sách `--deadline-seconds 900`.
- **Stream hữu hạn (`--e3-command-count` và `--e3-schedule`):**
  - Hỗ trợ số lượng lệnh từ 1 đến 30 (`--e3-command-count 1..30`).
  - Lịch phát `--e3-schedule smoke|steady`: Chế độ `smoke` chỉ hợp lệ với đúng 4 lệnh; chế độ `steady` giãn đều lịch phát khai báo 1 giây/lệnh (khoảng cách 1000 ms giữa các `offset_ms`).
- **Chế độ nghiệm thu chính thức (`--profile official` và `--official-opt-in`):**
  - Khi chỉ định `--profile official`, bắt buộc phải có cờ chấp thuận tường minh `--official-opt-in`.
  - Quy tắc ràng buộc nghiệm thu official của E3:
    - *Official 30 lệnh (`official30`):* `--e3-command-count 30 --e3-schedule steady` (hỗ trợ cả `--workload low_load` lẫn `mixed`; khuyến nghị ngân sách `--deadline-seconds 1200` tương đương 20 phút).
    - *Bounded official 3 lệnh (`official3`):* `--e3-command-count 3 --e3-schedule steady --workload low_load` (cấu hình kiểm định seam hữu hạn).
    - Mọi tổ hợp tham số khác khi bật `--profile official` sẽ bị từ chối ngay lập tức trước khi tạo fixture với lỗi `E3_official_requires_30_or_low_load_3_steady_commands`.
  - **Tách biệt hồ sơ:** Khi chạy official, phiên đo (session) và luồng lệnh điều khiển mang profile `official`, trong khi luồng phát tải viễn trắc nền vẫn duy trì hồ sơ `pilot` (`background_profile` theo E2).
- **Cơ chế thứ tự pending và terminal:** Lịch khai báo offset (`offset_ms`) không cam đoan gửi lệnh đúng offset tuyệt đối nếu lệnh trước đó trên cùng Sensor chưa đạt trạng thái terminal (`succeeded`, `failed`, v.v.). Producer REST phía client thăm dò theo deadline của lệnh và chỉ phát lệnh tiếp theo khi lệnh trước đã terminal; các lệnh chưa kịp phát hoặc hoãn được ghi nhận là skipped/not-sent, tuyệt đối không ngụy tạo bước nhảy version.
- **Đo lường timeline và đối soát:**
  - Viễn trắc nền `low_load` / `mixed` giữ nguyên manifest E2 hiện hành và cùng tải giữa polling/wakeup; chỉ cờ wakeup thay đổi.
  - E3 đo timeline thông qua cùng-process `ControlTimeline` snapshot tại hai mốc `workload_start` và `terminal` sau khi hoàn tất stop/drain; báo cáo sử dụng hiệu số (delta) histogram/counter, không cộng dồn snapshot tích lũy và không dùng client RTT làm server latency.
  - Các tập kiểm tra phải đối soát số lượng dispatch/success khớp với fresh commands, missing/overflow bằng 0, và pending origins bằng 0.

Thiết lập thư mục gốc làm việc riêng biệt cho lượt chạy:

```sh
ROOT=/var/tmp/opencode/p8-e3-<profile>-<unique-run>
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

#### Bước 0 — Chuẩn bị nhị phân paperload và gateway

Biên dịch hai file nhị phân từ thư mục mã nguồn `src/` sạch vào thư mục scratch riêng biệt với cờ `CGO_ENABLED=0`:

```sh
BIN_DIR=/var/tmp/opencode/p8-e3-binaries
mkdir -p "$BIN_DIR" && chmod 0700 "$BIN_DIR"
(cd src && CGO_ENABLED=0 go build -o "$BIN_DIR/paperload" ./cmd/paperload)
(cd src && CGO_ENABLED=0 go build -o "$BIN_DIR/gateway" ./cmd/gateway)
chmod 0700 "$BIN_DIR/paperload" "$BIN_DIR/gateway"
```

*(Lưu ý: Chỉ sử dụng các binary đã được xác minh cùng revision mã nguồn với backend)*.

#### Bước 1 — Khởi động Server Fixture E3 trên Host A (Terminal A1)

Sử dụng bản sao cấu hình mạng riêng (`private-network-copy.json`), giữ nguyên interface/TLS CA/SAN/hostname và chọn host ports còn trống.

**1. Khởi chạy thông thường (Pilot mặc định, 4 lệnh smoke):**
Chạy lệnh `serve` (giữ tiến trình này chạy ngầm hoặc trên Terminal A1 riêng biệt):

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network-copy.json \
  --experiment E3 \
  --dispatch-mode polling \
  --workload low_load \
  --deadline-seconds 900
```

*(Thay thế `--dispatch-mode wakeup` hoặc `--workload mixed` tương ứng với profile cần chạy)*.

**2. Khởi chạy với hồ sơ Official (`--profile official` và `--official-opt-in`):**
- Đối với luồng `official30` (30 lệnh steady, ngân sách 20 phút):
```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network-copy.json \
  --experiment E3 \
  --profile official \
  --official-opt-in \
  --dispatch-mode polling \
  --workload low_load \
  --e3-command-count 30 \
  --e3-schedule steady \
  --deadline-seconds 1200
```
- Đối với bài kiểm tra seam hữu hạn `official3` (3 lệnh steady, bắt buộc `low_load`):
```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network-copy.json \
  --experiment E3 \
  --profile official \
  --official-opt-in \
  --dispatch-mode polling \
  --workload low_load \
  --e3-command-count 3 \
  --e3-schedule steady \
  --deadline-seconds 1200
```

**3. Kiểm tra hợp đồng offline (Dry-run contract):**
Kiểm tra tính hợp lệ của manifest và kế hoạch chạy mà không cần khởi tạo fixture, container hay tiến trình mạng (sử dụng thư mục dummy `/not-used`):
```sh
python3 scripts/paper_p8_server.py serve --root /not-used \
  --network-config /not-used --dry-run --experiment E3 --profile official \
  --official-opt-in --dispatch-mode polling --workload low_load \
  --e3-command-count 30 --e3-schedule steady --deadline-seconds 1200
```

**4. Kiểm định native seam hữu hạn qua selector fixture:**
Chạy trực tiếp kiểm định seam 3 lệnh official hữu hạn trên fixture cách ly sạch:
```sh
python3 scripts/tests/native_compose_smoke.py --prepare "$ROOT/private" \
  --paper-p8-e3 polling --paper-p8-e3-low-load \
  --paper-p8-e3-command-count 3 --paper-p8-e3-schedule steady \
  --p8-profile official --p8-official-opt-in \
  --p8-deadline-seconds 1200
```

#### Bước 2 — Kiểm tra trạng thái và lưu session_id trên Host A (Terminal A2)

Trên terminal điều khiển A2 của Host A, kiểm tra trạng thái cho đến khi hệ thống sẵn sàng:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ cho đến khi status trả về `preparation_status=E3_PREPARED` và `phase=READY`. Đặt `session_id` nhận được vào biến môi trường (thay thế chuỗi placeholder `<session_id>` bằng giá trị hex thực tế từ status):

```sh
SESSION_ID=<session_id>
# Ví dụ: SESSION_ID=a1b2c3d4e5f6...
```

Đồng thời thiết lập định danh `PHASE_ID` theo đúng cấu trúc `e3-<workload>-<dispatch_mode>` của profile đang chạy (ví dụ `e3-low_load-polling`, `e3-low_load-wakeup`, `e3-mixed-polling`, `e3-mixed-wakeup`):

```sh
# Khai báo theo profile đang chạy:
PHASE_ID="e3-low_load-polling"
# Hoặc nếu chạy profile khác:
# PHASE_ID="e3-low_load-wakeup"
# PHASE_ID="e3-mixed-polling"
# PHASE_ID="e3-mixed-wakeup"
```

#### Bước 3 — Chuyển giao gói dữ liệu và nhị phân sang Host B (Manual Transfer Step)

Sao chép bundle cấu hình và 2 file nhị phân đã biên dịch sang Host B qua kênh truyền duyệt an toàn:
1. Sao chép thư mục `$ROOT/A/client-bundle/` sang Host B tại `$ROOT/B/received-bundle/`.
2. Sao chép hai tập tin thực thi nhị phân `$BIN_DIR/paperload` và `$BIN_DIR/gateway` sang Host B tại `$ROOT/B/bin/paperload` và `$ROOT/B/bin/gateway`.
3. Thiết lập phân quyền nghiêm ngặt trên Host B (lưu ý: thư mục `workspace` không được tạo trước vì lệnh `bind` yêu cầu tự tạo mới với quyền `0700`; nếu thư mục `workspace` đã tồn tại từ trước, `bind` sẽ từ chối với lỗi `workspace_output_leaf_exists`):

```sh
mkdir -p "$ROOT/B/bin"
chmod 0700 "$ROOT/B" "$ROOT/B/bin"
chmod 0700 "$ROOT/B/bin/paperload" "$ROOT/B/bin/gateway"
find "$ROOT/B/received-bundle" -type d -exec chmod 0700 {} +
find "$ROOT/B/received-bundle" -type f -exec chmod 0600 {} +
```

#### Bước 4 — Thiết lập Workspace và kiểm tra kết nối trên Host B

Trên Host B, thực hiện liên kết bundle, tiền kiểm tra cả hai nhị phân và kiểm tra kết nối mạng/xác thực TLS:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$ROOT/B/received-bundle" \
  --workspace "$ROOT/B/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/bin/paperload" \
  --gateway-binary "$ROOT/B/bin/gateway"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$ROOT/B/workspace"
```

Xác nhận preflight và check-connection đều trả về `PASS`.

#### Bước 5 — Kích hoạt pha đo lường trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh bắt đầu pha đo lường bằng đúng `$SESSION_ID` và `$PHASE_ID`:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id "$PHASE_ID"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ cho đến khi status trả về `phase=MEASURING`, `phase_id=$PHASE_ID`, và **`collector_ready=true`**.

#### Bước 6 — Chạy tải và xuất kết quả trên Host B

Chỉ sau khi bước 5 xác nhận `collector_ready=true`, trên Host B tiến hành chạy tải phát tin viễn trắc kết hợp thực thi lệnh điều khiển và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/bin/paperload" \
  --gateway-binary "$ROOT/B/bin/gateway"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/exported-results"
```

#### Bước 7 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/B/exported-results/` sang Host A tại đường dẫn `$ROOT/A/received-results/`.
Thiết lập phân quyền an toàn trên Host A: thư mục quyền `0700` và tập tin quyền `0600`:

```sh
chmod 0700 "$ROOT/A/received-results"
find "$ROOT/A/received-results" -type f -exec chmod 0600 {} +
```

*(Lưu ý: Giữ nguyên vẹn tập tin projection/inventory, không can thiệp sửa đổi nội dung)*.

#### Bước 8 — Hậu kiểm dữ liệu, đối soát và đóng Fixture trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh kết thúc pha đo lường kèm đường dẫn kết quả nhận được để kích hoạt quy trình đối soát dữ liệu (reconciliation):

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase MEASURING \
  --phase-id "$PHASE_ID" \
  --results "$ROOT/A/received-results"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Kiểm tra status chờ chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` (kèm `validation_valid=true` và `validation_failures=[]`). Sau đó tiến hành dọn dẹp và đóng server:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu trong thư mục:
`$ROOT/A/fixture/artifacts/`.

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

### 3.1 Bảng tổng hợp kết quả 4 Profile E3 — 2026-10-30

Toàn bộ 4 profile của E3 đã được kiểm định thực tế qua native CLI runner trên hệ thống ngày 2026-10-30:

| Profile | Workload Outcome | Validation Failures | Control Succeeded | SQLite Receipts / Pending | Telemetry App ACK (A / B) | Background DB (Ledger/Raw/Temp) | Command DB (Raw/Temp) | Total DB Delta (Ledger/Raw/Temp/Config) | Succeeded E2E Latency (Mean) | Runner Overlap | Exact Cleanup Verified | Serve Exit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Low-Load Polling** (Att. 20) | `VALIDATED` | `[]` | 4/4 (HTTP 202) | 2 completed / 0 pending (mỗi bên) | 8 / 4 (A có 1 retry, B 0 retry) | 12 / 1.200 / 12 | 36 / 24 | 36 / 1.236 / 36 / 2 | 723,34 ms (sum 2,893s) | Steady overlap (Burst N/A) | 12 ctn, 1 net, 5 vol | 0 |
| **Low-Load Wakeup** (Rerun) | `VALIDATED` | `[]` | 4/4 (HTTP 202) | 2 completed / 0 pending (mỗi bên) | 8 / 4 (0 retry) | 12 / 1.200 / 12 | 33 / 22 | 34 / 1.233 / 34 / 2 | 137,93 ms (sum 0,552s) | Steady overlap (Burst N/A) | 12 ctn, 1 net, 5 vol | 0 |
| **Mixed Polling** | `VALIDATED` | `[]` | 4/4 (HTTP 202) | 2 completed / 0 pending (mỗi bên) | 8 / 16 (A: 2 retries, B: 27 retries) | 24 / 2.400 / 24 | 35 / 23 | 47 / 2.435 / 47 / 2 | 759,99 ms (sum 3,040s) | Overlap burst: 3.321.496 ns | 12 ctn, 1 net, 5 vol | 0 |
| **Mixed Wakeup** | `VALIDATED` | `[]` | 4/4 (HTTP 202) | 2 completed / 0 pending (mỗi bên) | 8 / 16 (A: 0 retry, B: 26 retries) | 24 / 2.400 / 24 | 33 / 22 | 46 / 2.433 / 46 / 2 | 93,68 ms (sum 0,375s) | Overlap burst: 3.742.897 ns | 12 ctn, 1 net, 5 vol | 0 |

---

### 3.2 Chi tiết thực nghiệm Low-Load Polling — 2026-10-30

#### 1. Attempt 19 (`lowpoll-attempt19`) — FAIL
- **Thực thi và dữ liệu data-plane**:
  - Host A chuẩn bị session `a1002f4c332234a10234325f77f4c5d1` ở trạng thái `READY`; Host B bind, Go preflight và HTTPS/MQTT TLS connection checks đều PASS. Bộ thu thập chỉ số Collector trên Host A xác nhận `MEASURING` và `collector_ready=true` trước khi Host B khởi chạy.
  - Tiến trình client Host B kết thúc với mã thoát 0: telemetry Arm A đạt 8/8 và Arm B đạt 4/4 generated / offered / application ACK, không phát sinh retry, lỗi hay drop, pending outbox 0.
  - Bốn lệnh điều khiển PATCH đều trả về HTTP 202 Accepted, đạt trạng thái terminal `succeeded`; các giá trị tần số thiết lập là 1.5, 0.5, 2.0, 0.75 Hz.
  - Hai SQLite executor độc lập trên Host B ghi nhận mỗi bên 2 completed receipts và pending outbox 0; cấu hình được ghi nhận là 500.000 µs và 1.333.333 µs.
- **Nguyên nhân thất bại**:
  - Tại bước kết thúc phase trên Host A, hệ thống từ chối import với mã lỗi `e3_rest_config_read_failed` khi truy vấn reported configuration qua giao diện REST.
  - Quy trình import, reconciliation và validation không hoàn tất; trạng thái workload giữ nguyên nhãn `ABORTED`, tuyệt đối không gán PASS.
  - Quy trình abort kích hoạt dọn dẹp an toàn: server exit mã 0 và exact-owned cleanup VERIFIED (12 containers, 1 network, 5 volumes).
  - Báo cáo kết quả: `PAPER_P8_E3_WORKLOAD_VALIDATION_FAILED`, `full_p8_accepted=false`.
- **Phân tích nguyên nhân gốc (Root Cause Analysis)**:
  - Trường `control_targets` được chuẩn bị dưới dạng hai từ điển (dictionaries), nhưng các hàm tiêu thụ (consumers) giải nén mỗi từ điển thành các khóa `gateway_id` và `sensor_id`, dẫn đến việc REST GET truy vấn bằng định danh URN không hợp lệ và truy vấn đường cơ sở SQL baseline/delta sai phạm vi (scope).
  - Hướng khắc phục: Chuẩn hóa target projection để kiểm tra kiểu dữ liệu; yêu cầu command raw delta phải là số dương; projection phía Host B lưu giữ executor `configuration` và mã băm kết quả thực.
- **Bảo toàn bằng chứng**: Thư mục artifacts được lưu giữ nguyên vẹn tại:
  `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt19/`.
  Lịch sử thất bại của Attempt 19 được bảo toàn nguyên vẹn, không chỉnh sửa thành PASS.

#### 2. Attempt 20 (`lowpoll-attempt20`) — PASS
- **Khắc phục**: Sửa lỗi target mapping và áp dụng cổng kiểm soát kết quả terminal nghiêm ngặt (chỉ cho phép finalization kết thúc với mã 0 khi workload đạt `VALIDATED`).
- **Thực thi và kết quả**:
  - Toàn bộ quy trình helper hoàn tất trọn vẹn: A READY → B bind/preflight/TLS check → A collector ready/MEASURING → B workload/export → A finish/reconciliation → finalize.
  - Cả Host B runner và Host A server đều kết thúc với mã thoát 0; kết quả đạt **`PAPER_P8_E3_WORKLOAD_VERIFIED`** (`workload_outcome: VALIDATED`, `validation_failures: []`).
  - Host B ghi nhận **4/4** command PATCH thành công (HTTP 202, terminal succeeded) với 4 khóa định danh phân biệt: 1.5 Hz và 0.5 Hz từ version 0, tiếp theo là 2.0 Hz và 0.75 Hz từ version 1.
  - Hai SQLite executor độc lập ghi nhận mỗi bên **2 completed receipts**, pending outbox **0**; kết quả cấu hình cuối cùng đạt version 2, chu kỳ lấy mẫu tương ứng là 500.000 µs (2.0 Hz) và 1.333.333 µs (0.75 Hz).
  - Đối soát dữ liệu (Reconciliation): Host A đối chiếu khớp 100% giữa SQL receipts + REST reported configuration và SQLite receipts + configuration từ Host B.
  - Telemetry nền: Arm A đạt **8/8**, Arm B đạt **4/4** generated / offered / application ACK. Arm A ghi nhận 1 retry (không có publish failure hay drop, pending outbox 0), Arm B không retry.
  - Phân tách số đếm cơ sở dữ liệu (DB deltas): Luồng telemetry nền ghi nhận 12 ledger / 1.200 raw / 12 temporal; các cảm biến nhận lệnh điều khiển ghi nhận **36 raw / 24 temporal**. Tổng delta toàn fixture là **36 ledger / 1.236 raw / 36 temporal / 2 configuration**.
  - Độ trễ điều khiển (Control Latency): Trích xuất từ snapshot terminal cumulative histogram (count = 4):
    - Thời gian điều phối (Dispatch latency): tổng 2,678130341 s, giá trị trung bình **669,53 ms**.
    - Thời gian hoàn tất vòng điều khiển (Succeeded E2E latency): tổng 2,893375296 s, giá trị trung bình **723,34 ms**.
    - Các trường missing origin, missing dispatch, missing pending đều bằng 0.
    - Thời gian phản hồi mạng client PATCH RTT đạt từ 19 ms đến 39 ms (được báo cáo riêng biệt, không gộp vào độ trễ xử lý của server).
  - Dọn dẹp tài nguyên: VERIFIED (12 containers, 1 network, 5 volumes dọn sạch theo receipts).
  - Thư mục artifacts: `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt20/`.

---

### 3.3 Chi tiết thực nghiệm Low-Load Wakeup — 2026-10-30

#### 1. Lần chạy đầu (`low-load-wakeup-first`) — FAIL (Lỗi Selector Helper)
- Lượt chạy đầu tại `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup/` dừng ngay tại bước preflight, dọn dẹp tài nguyên VERIFIED, chưa chạy workload.
- Traceback chẩn đoán cho thấy tham số `--p8-arm wakeup` bị bỏ qua khi dựng Fixture (do cơ chế hard-code fallback về polling trong mã nguồn helper cũ), dẫn đến việc `STAGE5_WAKEUP_ENABLED=false` làm sai lệch cổng kiểm tra cờ hiệu lực (effective-flags gate).
- Đây là lỗi của công cụ helper, không phải lỗi của profile backend. Thất bại này được giữ nguyên trong lịch sử kiểm thử.

#### 2. Lần chạy lại hợp lệ (`low-load-wakeup-rerun`) — PASS
- **Khắc phục**: Sửa selector để cơ chế lifecycle-hold nhận diện chính xác arm được yêu cầu; bổ sung regression test cho cả polling và wakeup.
- **Thực thi và kết quả**:
  - Khởi chạy fresh root tại `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup-rerun/`.
  - Kết quả đạt **`PAPER_P8_E3_WORKLOAD_VERIFIED`**: Host A và Host B exit mã 0, workload `VALIDATED`, cleanup VERIFIED (12 containers, 1 network, 5 volumes).
  - Cờ hiệu lực đọc lại xác nhận chuẩn xác: `STAGE5_ENABLED=true`, `STAGE5_WAKEUP_ENABLED=true`.
  - Lệnh điều khiển: **4/4** PATCH succeeded (HTTP 202, 4 keys phân biệt); mỗi executor hoàn thành 2 receipts, đạt version 2, pending outbox 0.
  - Telemetry nền: Arm A đạt **8/8**, Arm B đạt **4/4** application ACK; không có retry, failure hay drop.
  - Phân tách số đếm cơ sở dữ liệu: Luồng nền ghi nhận 12 ledger / 1.200 raw / 12 temporal; cảm biến điều khiển ghi nhận 33 raw / 22 temporal. Tổng delta: **34 ledger / 1.233 raw / 34 temporal / 2 configuration**.
  - Kiểm tra overlap: Low-load phase available; command interval giao thoa với A steady interval; burst overlap ghi nhận **NOT APPLICABLE**.
  - Độ trễ điều khiển (Terminal collector histogram count = 4):
    - Dispatch latency trung bình: **42,15 ms** (tổng 0,168590961 s).
    - Succeeded E2E latency trung bình: **137,93 ms** (tổng 0,551705864 s).
    - Client PATCH RTT: 31 ms đến 42 ms (báo cáo tách biệt).
  - Thư mục artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup-rerun/`.

---

### 3.4 Chi tiết thực nghiệm Mixed Polling — 2026-10-30

- Thực thi tại thư mục `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-polling/`.
- Kết quả đạt **`PAPER_P8_E3_WORKLOAD_VERIFIED`** (`workload_outcome: VALIDATED`), cả runner và server đều kết thúc mã thoát 0, cleanup VERIFIED (12 containers, 1 network, 5 volumes).
- Cờ hiệu lực đọc lại xác nhận: `STAGE5_ENABLED=true`, `STAGE5_WAKEUP_ENABLED=false`.
- Lệnh điều khiển: **4/4** PATCH succeeded (HTTP 202 với 4 keys phân biệt); mỗi executor hoàn tất 2 receipts, đạt version 2, pending outbox 0.
- Telemetry nền: Arm A đạt **8/8** application ACK (phát sinh 2 retries); Arm B đạt **16/16** application ACK (phát sinh 27 retries do shaper rate-limit pha burst); không có publish failure hay drop; pending outbox 0.
- Phân tách số đếm cơ sở dữ liệu: Luồng nền ghi nhận 24 ledger / 2.400 raw / 24 temporal; cảm biến điều khiển ghi nhận 35 raw / 23 temporal. Tổng delta: **47 ledger / 2.435 raw / 47 temporal / 2 configuration**.
- Xác thực Overlap Clock: Đồng hồ runner-clock chứng minh khoảng thời gian A steady interval giao thoa với B burst interval là **3.321.496 ns**; khoảng thời gian thực thi lệnh (command intervals) bao trùm đồng thời cả pha steady và pha burst. Pipe lag giữa các tiến trình chưa xác định; không thực hiện phép trừ đồng hồ giữa hai máy.
- Độ trễ điều khiển (Terminal collector histogram count = 4):
  - Dispatch latency: tổng 2,864832765 s, giá trị trung bình **716,21 ms**.
  - Succeeded E2E latency: tổng 3,039973099 s, giá trị trung bình **759,99 ms**.
  - Client PATCH RTT: 21 ms đến 112 ms (báo cáo tách biệt).
- Thư mục artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-polling/`.

---

### 3.5 Chi tiết thực nghiệm Mixed Wakeup — 2026-10-30

- Thực thi tại thư mục `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-wakeup/`.
- Kết quả đạt **`PAPER_P8_E3_WORKLOAD_VERIFIED`** (`workload_outcome: VALIDATED`), cả runner và server kết thúc mã thoát 0, cleanup VERIFIED (12 containers, 1 network, 5 volumes).
- Cờ hiệu lực đọc lại xác nhận: `STAGE5_ENABLED=true`, `STAGE5_WAKEUP_ENABLED=true`.
- Lệnh điều khiển: **4/4** PATCH succeeded (HTTP 202 với 4 keys phân biệt); mỗi executor hoàn tất 2 receipts, đạt version 2, pending outbox 0.
- Telemetry nền: Arm A đạt **8/8** application ACK (0 retry); Arm B đạt **16/16** application ACK (phát sinh 26 retries trong pha burst); không có publish failure hay drop; pending outbox 0.
- Phân tách số đếm cơ sở dữ liệu: Luồng nền ghi nhận 24 ledger / 2.400 raw / 24 temporal; cảm biến điều khiển ghi nhận 33 raw / 22 temporal. Tổng delta: **46 ledger / 2.433 raw / 46 temporal / 2 configuration**.
- Xác thực Overlap Clock: Đồng hồ runner-clock chứng minh khoảng thời gian A steady interval giao thoa với B burst interval là **3.742.897 ns**; khoảng thời gian thực thi lệnh bao trùm đồng thời cả pha steady và pha burst.
- Độ trễ điều khiển (Terminal collector histogram count = 4):
  - Dispatch latency: tổng 0,217684148 s, giá trị trung bình **54,42 ms**.
  - Succeeded E2E latency: tổng 0,374731331 s, giá trị trung bình **93,68 ms**.
  - Client PATCH RTT: 14 ms đến 121 ms (báo cáo tách biệt).
- Thư mục artifacts: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-wakeup/`.

---

### 3.6 Cơ chế kiểm định kết quả và đối soát nghiêm ngặt (Reconciliation)

Quy trình hậu kiểm của E3 thực hiện đối soát đa tầng giữa client, broker, database và REST API:
1. **Kiểm tra Result Inventory**:
   - Chỉ cho phép các tập tin kết quả nằm trong danh sách trắng (allowlist), giới hạn kích thước tối đa.
   - Kiểm tra chặt chẽ các trường session ID, run ID, phase ID, phân vai roles, bundle hash, mã băm SHA-256 từng file, kiểm tra đường dẫn an toàn và tuyệt đối cấm symlink.
   - Phía Host A chỉ tiếp nhận hình chiếu an toàn (safe projection), tuyệt đối không thực thi tập tin nhị phân hay mã SQL từ Host B.
2. **Đối soát biên lai điều khiển (Command Receipts Reconciliation)**:
   - Host B bắt buộc phải dừng và join runner hoàn toàn trước khi xuất hình chiếu receipts từ 2 cơ sở dữ liệu SQLite cục bộ.
   - Host A đối chiếu chéo (cross-reconcile) giữa dữ liệu receipts trong PostgreSQL và cấu hình reported configuration qua REST API với biên lai SQLite receipts và configuration hash từ Host B: kiểm tra khớp 100% command ID, version, sample_interval_us, và result hash.
3. **Phân tách số liệu đo lường (Telemetry Separation)**:
   - Dữ liệu viễn trắc của các cảm biến nền (background sensors) được phân tách tuyệt đối khỏi dữ liệu của các cảm biến nhận lệnh điều khiển (command sensors), ngăn ngừa hiện tượng ô nhiễm dữ liệu mẫu.

---

### 3.7 Phân định chỉ số đo lường và Giới hạn diễn giải

- **Bản chất số liệu Snapshot Collector**: Bộ thu thập chỉ số phía server (`collector`) hoạt động theo chu kỳ polling có giới hạn (bounded polling). Snapshots thu thập mang tính tích lũy (cumulative); báo cáo chính thức chỉ sử dụng duy nhất snapshot terminal mới nhất ở ranh giới kết thúc pha, tuyệt đối không cộng dồn các snapshot lặp lại.
- **Độ trễ điều phối và E2E**:
  - Được tính toán từ các mốc thời gian origin do server gắn nhãn khi tiếp nhận lệnh và mốc hoàn thành khi tiếp nhận kết quả thực thi từ Gateway. Nếu collector không thu thập được histogram, chỉ số này được ghi nhận là `unavailable` hoặc `inconclusive`, không quy đổi về 0.
  - Thời gian phản hồi mạng client PATCH RTT (14 ms đến 121 ms) là số đo độc lập tại máy khách, phản ánh thời gian giao tiếp HTTP qua Nginx, không đồng nhất và không được trộn lẫn với độ trễ điều phối nội bộ (dispatch latency) hay độ trễ hoàn thành vòng điều khiển (succeeded E2E latency) của backend.
- **Giới hạn đo tài nguyên**: Các mẫu tài nguyên CPU/RAM được ghi nhận là quan sát từ fixture phía Host A; không đại diện cho tài nguyên tiêu thụ của tiến trình phát tải và executor phía Host B.

---

### 3.8 Danh mục Checksum và Đường dẫn Artifacts

Toàn bộ báo cáo và dấu vết thực nghiệm được lưu trữ nguyên bản tại các thư mục artifacts trên máy chủ kiểm thử:
- **Low-Load Polling (Attempt 19 — FAIL)**:
  - Thư mục: `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt19/`
  - Trạng thái: `PAPER_P8_E3_WORKLOAD_VALIDATION_FAILED` (bảo toàn lịch sử thất bại).
- **Low-Load Polling (Attempt 20 — PASS)**:
  - Thư mục: `/var/tmp/opencode/p8-e3-lowpoll-native-20261030/attempt20/`
  - Báo cáo kết quả: `PAPER_P8_E3_WORKLOAD_VERIFIED`, cleanup 12/1/5 VERIFIED.
- **Low-Load Wakeup (Rerun — PASS)**:
  - Thư mục: `/var/tmp/opencode/p8-e3-profiles-20261030/low-load-wakeup-rerun/`
  - Báo cáo kết quả: `PAPER_P8_E3_WORKLOAD_VERIFIED`, cleanup 12/1/5 VERIFIED.
- **Mixed Polling (PASS)**:
  - Thư mục: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-polling/`
  - Báo cáo kết quả: `PAPER_P8_E3_WORKLOAD_VERIFIED`, cleanup 12/1/5 VERIFIED.
- **Mixed Wakeup (PASS)**:
  - Thư mục: `/var/tmp/opencode/p8-e3-profiles-20261030/mixed-wakeup/`
  - Báo cáo kết quả: `PAPER_P8_E3_WORKLOAD_VERIFIED`, cleanup 12/1/5 VERIFIED.

---

## 4. Giới hạn và trạng thái gate

### 4.1 Các giới hạn vận hành và ranh giới nghiệm thu

- **Trạng thái cổng P8**: `full_p8_accepted=false`.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm được thực hiện trên cùng máy chủ cục bộ với metadata ghi nhận `generator_colocated=true` (hoặc `same-host/unverified`); chưa chứng minh được hành vi phân tán trên hai máy vật lý độc lập qua môi trường mạng vật lý tách biệt.
- **Các kịch bản chưa thực hiện**: Các bài kiểm tra phần cứng HIL trên bo mạch AM5728, bài kiểm tra đầy đủ trên hệ thống CI từ xa (full remote CI), bài đo năng lực chịu tải và ngâm tải dài hạn (capacity/soak testing), cùng quy trình triển khai sản xuất (live rollout) đều là **NOT RUN**.
- **Official30 native vẫn là NOT RUN**: Việc nghiệm thu luồng 30 lệnh chính thức trên môi trường native phân tán đầy đủ vẫn là **NOT RUN**. Hiện mới chỉ hoàn thành hợp đồng offline dry-run và bài kiểm tra native seam hữu hạn 3 lệnh (`official3`).
- **Ranh giới nghiệm thu kết quả và quy trình import**: Quy trình nhập kết quả (result import) và đối soát dữ liệu (reconciliation) đã hoàn thành và đạt trạng thái `VALIDATED` trong 4 profile 4-lệnh chuẩn (§3) cũng như bài kiểm tra bounded seam 3-lệnh ghi nhận trong báo cáo P8; không tuyên bố một cách sai lệch hay lỗi thời rằng completed result import là universally NOT RUN. Chi tiết số liệu đo đạc từng lượt được tập trung tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#e3--subset-ba-lệnh-và-chẩn-đoán-probe).
- **Ranh giới giữa Offline Admission và Online Delivery**: Thao tác offline sensor chỉ chứng minh việc tiếp nhận/cam kết và replay trong CSDL (committed admission/replay gồm desired state, command request, và receipt); tuyệt đối không chứng minh việc chuyển giao (delivery) hay thực thi lệnh thành công (success). Việc chuyển giao và kết quả (delivery/result) chỉ được kết luận từ online executor thực tế sau khi nhận `READY_MARKER` và `SUBACK`.
- **Ngân sách vòng đời và an toàn JWT**: Mọi quy trình đều tuân thủ chặt chẽ ngân sách thời gian có hạn (`--deadline-seconds` 900s hoặc 1200s), thời hạn ngắn của JWT token, dọn dẹp chính xác tài nguyên (`exact_cleanup_verified`), và phân quyền tập tin nghiêm ngặt (`0700`/`0600`).
- **Phạm vi dừng (checkpoint lịch sử)**: Bốn profile E3 đã được ghi nhận hoàn thành ở checkpoint trước thông qua helper CLI. Kết quả bổ sung không thay đổi qualification của các lượt profile lịch sử; xem [báo cáo P8](paper-p8-experiments-and-acceptance.md#e3--subset-ba-lệnh-và-chẩn-đoán-probe).

### 4.2 Hợp đồng offline Official30 và kiểm định native seam hữu hạn

Kiểm tra tính hợp lệ của luồng 30 lệnh official dự kiến mà không cần khởi tạo fixture hay tiến trình mạng/workload (ngân sách phiên 20 phút được thiết lập có chủ đích):

```sh
python3 scripts/paper_p8_server.py serve --root /not-used \
  --network-config /not-used --dry-run --experiment E3 --profile official \
  --official-opt-in --dispatch-mode polling --workload low_load \
  --e3-command-count 30 --e3-schedule steady --deadline-seconds 1200
```

Dry-run dùng identity chỉ dành cho schema, không dùng credential trên mạng. E3 control/session là `official`, còn telemetry nền E2 giữ profile `pilot`. Official cần opt-in; preset là 30 lệnh `steady`, hoặc 3 lệnh `low_load/steady` để kiểm tra seam hữu hạn. Tổ hợp không hỗ trợ bị từ chối trước khi tạo fixture. Không coi dry-run là nghiệm thu 30 lệnh; kết quả thực tế nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e3--subset-ba-lệnh-và-chẩn-đoán-probe).

Đối với kiểm định seam native hữu hạn, sử dụng đường dẫn native fixture hiện có với công thức 3 lệnh `low_load`/`steady` có opt-in official và một thư mục gốc cách ly mới. Preset official30 đã chọn vẫn sẵn sàng để gọi trực tiếp mà không cần sửa mã nguồn:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare "$ROOT/private" \
  --paper-p8-e3 polling --paper-p8-e3-low-load \
  --paper-p8-e3-command-count 3 --paper-p8-e3-schedule steady \
  --p8-profile official --p8-official-opt-in \
  --p8-deadline-seconds 1200
```

Lệnh này dùng fixture E3 hữu hạn, cần các gate cleanup/readback thông thường và không phải nghiệm thu official 30 lệnh.
