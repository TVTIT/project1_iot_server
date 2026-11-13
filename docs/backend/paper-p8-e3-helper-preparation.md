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
- **Chống ghi đè tập tin và cách ly môi trường**: Mỗi lượt chạy bắt buộc sử dụng một thư mục gốc hoàn toàn mới (ví dụ: `ROOT=/var/tmp/opencode/p8-e3-lowpoll-run01`). Tuyệt đối không tái sử dụng thư mục đã có state, bundle, SQLite hay artifacts cũ.
- **Ngân sách thời gian (Deadline budget)**: Thời hạn deadline của server `--deadline-seconds 900` (15 phút cho pilot mặc định) hoặc `--deadline-seconds 1200` (20 phút cho official30/official3) phải đủ cho toàn bộ chu kỳ: khởi động container, biên dịch/truyền bundle và 2 binary, chạy workload, chờ drain, export/import, hậu kiểm đối soát SQL/REST/SQLite và dọn dẹp.
- **Trình tự đồng bộ Collector nghiêm ngặt**: Bộ thu thập phía server trên Host A bắt buộc phải được kích hoạt và xác nhận sẵn sàng (`phase=MEASURING`, `collector_ready=true`) TRƯỚC KHI tiến trình client trên Host B được phép khởi chạy phát tải.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Steps)**: Việc chuyển bundle, 2 file nhị phân (`paperload`, `gateway`) và kết quả đo giữa Host A và Host B là thao tác truyền file thủ công của người vận hành (sao chép an toàn qua đường dẫn lưu trữ hoặc công cụ mạng nội bộ), không tuyên bố là tự động hóa qua SSH.
- **Xử lý sự cố và dọn dẹp an toàn**: Khi request hoặc kết quả bị từ chối, giữ nguyên hiện trạng các file SQLite, outbox, journal và artifacts để phục vụ chẩn đoán lỗi; không tự ý reseed hay ghi đè. Quy trình `finalize` (hoặc `abort`) vẫn là kênh đóng phiên chính thức để giải phóng an toàn 100% tài nguyên Docker.

---

### 2.2 Phân định Terminal và Thiết lập biến độc lập

Quy trình vận hành sử dụng ba terminal độc lập được phân định rõ ràng giữa Host A và Host B:
- **Host A — Terminal A1**: Khởi chạy tiến trình máy chủ `serve` và duy trì fixture ở chế độ foreground (hoặc theo dõi nhật ký).
- **Host A — Terminal A2**: Thực hiện các lệnh điều khiển phiên đo (`status`, `begin`, `finish-phase`, `finalize`, `abort`, `cleanup`).
- **Host B — Terminal B**: Thực hiện các thao tác client (`bind`, `preflight`, `check-connection`, `run`, `export-results`).

#### Bảng cấu hình 4 Profile E3 và Định danh Pha (PHASE_ID)

Mỗi lượt chạy khảo sát một trong 4 profile độc lập. Định danh pha `PHASE_ID` bắt buộc phải có dạng `e3-<workload>-<dispatch_mode>` tương ứng:

| Profile | Chế độ Dispatch (`DISPATCH_MODE`) | Tải Viễn trắc (`WORKLOAD`) | Cờ `STAGE5_WAKEUP_ENABLED` | Định danh pha (`PHASE_ID`) | Ghi chú Tải |
|---|---|---|---|---|---|
| **Profile 1: Low-Load Polling** (Baseline mặc định) | `polling` | `low_load` | `false` | `e3-low_load-polling` | Arm A: 8 msgs, Arm B: 4 msgs (không burst). Tổng 12 msgs. |
| **Profile 2: Low-Load Wakeup** | `wakeup` | `low_load` | `true` | `e3-low_load-wakeup` | Arm A: 8 msgs, Arm B: 4 msgs (không burst). Tổng 12 msgs. |
| **Profile 3: Mixed Polling** | `polling` | `mixed` | `false` | `e3-mixed-polling` | Arm A: 8 msgs, Arm B: 16 msgs (4 steady + 12 burst). Tổng 24 msgs. |
| **Profile 4: Mixed Wakeup** | `wakeup` | `mixed` | `true` | `e3-mixed-wakeup` | Arm A: 8 msgs, Arm B: 16 msgs (4 steady + 12 burst). Tổng 24 msgs. |

#### Thiết lập biến độc lập trên A1, A2 và B

Dán **cùng khối Bash dưới đây vào từng terminal A1, A2 và B**; các terminal không chia sẻ biến ngầm. Chỉ chọn bốn giá trị đầu trước khi bắt đầu lượt chạy. Mỗi lần chạy lại phải đổi `RUN_LABEL`; không sửa các biến giữa chừng trong một session.

```sh
PRESET=official30         # smoke4 | official30 | official3
DISPATCH_MODE=polling     # polling | wakeup
WORKLOAD=low_load         # low_load | mixed; official3 chỉ nhận low_load
RUN_LABEL=run01           # Đổi nhãn khi chạy lại cùng cấu hình
NETWORK_CONFIG=/path/to/private-network-copy.json

case "$PRESET" in
  smoke4) COMMAND_COUNT=4; SCHEDULE=smoke; PROFILE=pilot; DEADLINE_SECONDS=900 ;;
  official30) COMMAND_COUNT=30; SCHEDULE=steady; PROFILE=official; DEADLINE_SECONDS=1200 ;;
  official3) COMMAND_COUNT=3; SCHEDULE=steady; PROFILE=official; DEADLINE_SECONDS=1200 ;;
  *) printf '%s\n' 'PRESET không hợp lệ' >&2; false ;;
esac
test "$DISPATCH_MODE" = polling || test "$DISPATCH_MODE" = wakeup
test "$WORKLOAD" = low_load || test "$WORKLOAD" = mixed
test "$PRESET" != official3 || test "$WORKLOAD" = low_load

ROOT="/var/tmp/opencode/p8-e3-${PRESET}-${WORKLOAD}-${DISPATCH_MODE}-${RUN_LABEL}"
PHASE_ID="e3-${WORKLOAD}-${DISPATCH_MODE}"
OFFICIAL_ARGS=()
if [ "$PROFILE" = official ]; then
  OFFICIAL_ARGS=(--official-opt-in)
fi
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

Các phép kiểm tra trên phải thành công trước khi tiếp tục. Không tạo trước `$ROOT/A` hay `$ROOT/B/workspace`. Khối lệnh dùng Bash (có array); không chạy bằng `sh`.

| Tham số | Ý nghĩa |
|---|---|
| `--root` / `--network-config` | Fixture A mới và cấu hình mạng riêng IPv4/TLS; thay đường dẫn cấu hình trước khi chạy. |
| `--dispatch-mode` | `polling` hoặc `wakeup`; thay cơ chế dispatch, giữ nguyên workload của cặp so sánh. |
| `--workload` | `low_load` hoặc `mixed` theo bảng trên; không phải lựa chọn rate cao E2. |
| `--e3-command-count` / `--e3-schedule` | Preset `smoke4`: 4/smoke; `official30`: 30/steady; `official3`: 3/steady và low_load. `steady` khai báo offset 1 giây/lệnh nhưng vẫn chờ command trước trên cùng Sensor terminal. |
| `--profile` / `--official-opt-in` | Chọn pilot/official và ủy quyền official tường minh; array tự thêm opt-in khi chọn official. Telemetry nền vẫn giữ role profile pilot. |
| `--deadline-seconds` | Budget toàn session, không chỉ thời lượng đo; ví dụ 900/1200s, phải còn đủ cho chuyển tệp, workload, import và cleanup. |

---

### 2.3 Quy trình vận hành chi tiết 8 bước

#### Bước 0 — Chuẩn bị và kiểm tra nguồn nhị phân

Cơ chế server fixture của E3 tự động biên dịch cả hai file nhị phân `paperload` và `gateway` từ thư mục mã nguồn `src/` vào thư mục nội bộ fixture trên Host A trong Bước 1:
- Nhị phân `paperload`: `$ROOT/A/fixture/private/bin/paperload`
- Nhị phân `gateway`: `$ROOT/A/fixture/private/bin/gateway`

Người vận hành không cần tự biên dịch vào thư mục scratch ngoại lai. Nếu muốn kiểm tra trước tính hợp lệ của mã nguồn và công cụ biên dịch Go trên Host A, có thể thực hiện kiểm tra nhanh không tạo file rác:

```sh
(cd src && CGO_ENABLED=0 go build -o /dev/null ./cmd/paperload ./cmd/gateway)
```

#### Bước 1 — Khởi động Server Fixture E3 trên Host A (Terminal A1)

Khởi chạy tiến trình `serve` trên Terminal A1 sử dụng trực tiếp các biến môi trường đã thiết lập:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config "$NETWORK_CONFIG" \
  --experiment E3 \
  --profile "$PROFILE" \
  --dispatch-mode "$DISPATCH_MODE" \
  --workload "$WORKLOAD" \
  --e3-command-count "$COMMAND_COUNT" \
  --e3-schedule "$SCHEDULE" \
  --deadline-seconds "$DEADLINE_SECONDS" \
  "${OFFICIAL_ARGS[@]}"
```

*(Lưu ý về các biến cấu hình: Lệnh trên tự động nạp đúng `$DISPATCH_MODE`, `$WORKLOAD`, `$COMMAND_COUNT`, `$SCHEDULE` và `$DEADLINE_SECONDS` từ môi trường đã chọn)*.

**Các tùy chọn khởi chạy mở rộng (nếu chạy kịch bản official hoặc kiểm định offline)**:

- **Official30/official3** dùng chính lệnh `serve` trên, sau khi chọn preset ở khối biến. Không chạy thêm một `serve` thứ hai trong cùng root. Telemetry nền vẫn giữ profile pilot; không có cờ tốc độ cao E2.

- **Kiểm tra hợp đồng offline Dry-Run 30 lệnh (`official30 dry-run`)**:
  *(Kiểm tra tính hợp lệ của manifest schema và kế hoạch chạy mà không mở container hay mạng)*:
  ```sh
  python3 scripts/paper_p8_server.py serve --root /not-used \
    --network-config /not-used --dry-run --experiment E3 --profile official \
    --official-opt-in --dispatch-mode polling --workload low_load \
    --e3-command-count 30 --e3-schedule steady --deadline-seconds 1200
  ```

- **Kiểm định native seam hữu hạn 3 lệnh qua smoke selector**:
  *(Sử dụng thư mục độc lập riêng biệt `ROOT_SEAM` để tránh ảnh hưởng đến thư mục phiên chính)*:
  ```sh
  ROOT_SEAM=/var/tmp/opencode/p8-e3-native-seam
  mkdir -p "$ROOT_SEAM" && chmod 0700 "$ROOT_SEAM"

  python3 scripts/tests/native_compose_smoke.py --prepare "$ROOT_SEAM/private" \
    --paper-p8-e3 polling --paper-p8-e3-low-load \
    --paper-p8-e3-command-count 3 --paper-p8-e3-schedule steady \
    --p8-profile official --p8-official-opt-in \
    --p8-deadline-seconds 1200
  ```

#### Bước 2 — Kiểm tra trạng thái và lưu session_id trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái server cho đến khi hoàn tất khởi tạo:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ cho đến khi status trả về `preparation_status=E3_PREPARED` và `phase=READY`.

Trích xuất `session_id` an toàn từ JSON bằng Python với assertion đảm bảo trường tồn tại và không rỗng:

```sh
SESSION_ID=$(python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3 | python3 -c 'import sys, json; data = json.load(sys.stdin); sid = data.get("session_id"); assert sid, "session_id missing or empty in status JSON"; print(sid)')
echo "SESSION_ID=$SESSION_ID"
```

Đồng thời, ghi nhận mã băm SHA-256 của hai file nhị phân đã được fixture biên dịch sẵn tại `$ROOT/A/fixture/private/bin/`:

```sh
sha256sum "$ROOT/A/fixture/private/bin/paperload" "$ROOT/A/fixture/private/bin/gateway"
```

#### Bước 3 — Chuyển giao gói dữ liệu và nhị phân sang Host B (Manual Transfer Step)

Người vận hành thực hiện chuyển giao thủ công các tài nguyên cần thiết từ Host A sang Host B:
- **Tài nguyên 1 — Client bundle**:
  - Nguồn (Host A): `$ROOT/A/client-bundle/`
  - Đích (Host B): `$ROOT/B/received-bundle/`
- **Tài nguyên 2 — Nhị phân `paperload`**:
  - Nguồn (Host A): `$ROOT/A/fixture/private/bin/paperload`
  - Đích (Host B): `$ROOT/B/bin/paperload`
- **Tài nguyên 3 — Nhị phân `gateway`**:
  - Nguồn (Host A): `$ROOT/A/fixture/private/bin/gateway`
  - Đích (Host B): `$ROOT/B/bin/gateway`

Sau khi hoàn tất sao chép, thiết lập phân quyền nghiêm ngặt và đối chiếu mã băm trên Host B (Terminal B):

```sh
mkdir -p "$ROOT/B/bin"
chmod 0700 "$ROOT/B" "$ROOT/B/bin"
chmod 0700 "$ROOT/B/bin/paperload" "$ROOT/B/bin/gateway"
find "$ROOT/B/received-bundle" -type d -exec chmod 0700 {} +
find "$ROOT/B/received-bundle" -type f -exec chmod 0600 {} +

# Đối chiếu mã băm SHA-256 trên Host B (bắt buộc khớp 100% với Bước 2 trên Host A):
sha256sum "$ROOT/B/bin/paperload" "$ROOT/B/bin/gateway"
```

*(Lưu ý quan trọng: Thư mục `$ROOT/B/workspace` tuyệt đối **không được tạo trước** bằng lệnh `mkdir`. Lệnh `bind` tại Bước 4 yêu cầu tự tạo mới thư mục này với quyền `0700`; nếu thư mục đã tồn tại từ trước, `bind` sẽ từ chối thực hiện với lỗi `workspace_output_leaf_exists`)*.

#### Bước 4 — Thiết lập Workspace và kiểm tra kết nối trên Host B (Terminal B)

Trên Terminal B của Host B, tiến hành liên kết bundle, tiền kiểm tra cả hai nhị phân và kiểm tra kết nối mạng/xác thực TLS:

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

- Lệnh `bind` kiểm tra bundle, giải nén và sinh ra cấu hình thực thi `runtime/execution/run-spec.json` (chứa 2 jobs telemetry nền, 1 job control, và 2 gateway executor jobs).
- Lệnh `preflight` thực thi Go binary `paperload` và kiểm tra `gateway` binary với cờ `--preflight` để xác thực cú pháp manifest, môi trường executor và schema DB cục bộ; bước này **hoàn toàn không mở socket mạng**.
- Lệnh `check-connection` thăm dò thực tế kết nối HTTPS REST API và kết nối MQTT TLS với credentials không đặc quyền trong bundle.

**Cổng an toàn**: Cả `preflight` và `check-connection` đều bắt buộc phải trả về `PASS`. Tuyệt đối không kích hoạt phát tải nếu tiền kiểm tra thất bại.

#### Bước 5 — Kích hoạt pha đo lường trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh bắt đầu pha đo lường với `$SESSION_ID` và `$PHASE_ID` tương ứng:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id "$PHASE_ID"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Kiểm tra xác nhận trạng thái bằng Python JSON assertion:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3 | python3 -c 'import sys, json; data = json.load(sys.stdin); ph = data.get("phase"); cr = data.get("collector_ready"); sid = data.get("session_id"); assert ph == "MEASURING", f"Expected phase MEASURING, got {ph}"; assert cr is True, f"Expected collector_ready True, got {cr}"; print(f"Confirmed MEASURING with collector_ready=True for session {sid}")'
```

**Cổng an toàn đồng bộ (Collector Gate)**: Host B **TUYỆT ĐỐI CHƯA ĐƯỢC CHẠY** Bước 6 cho đến khi Bước 5 xác nhận `collector_ready=true`. Lệnh `begin` chỉ phát hành yêu cầu; bộ thu thập phía server cần thời gian ghi nhận snapshot đường cơ sở SQL và telemetry trước khi sẵn sàng tiếp nhận tải.

#### Bước 6 — Chạy tải và xuất kết quả trên Host B (Terminal B)

Chỉ sau khi Bước 5 đã xác nhận `collector_ready=true`, trên Terminal B của Host B tiến hành chạy tải kết hợp và xuất tập kết quả:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/bin/paperload" \
  --gateway-binary "$ROOT/B/bin/gateway"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/exported-results"
```

- Lệnh `run` khởi chạy đồng thời hai luồng telemetry nền phát tin MQTT TLS (Arm A và Arm B), một luồng điều khiển Sensor gửi các lệnh PATCH tuần tự qua HTTPS REST API, và hai tiến trình Gateway executor độc lập (`gw_a`, `gw_b`) nhận và thực thi lệnh qua MQTT TLS. *(Lưu ý: Lệnh `run` của E3 không sử dụng cờ `--phase`)*.
- Lệnh `export-results` trích xuất hình chiếu kết quả an toàn gồm:
  - Bản tóm tắt thực thi của các roles (`workloads/*/workload.json`).
  - Dữ liệu biên lai từ 2 CSDL SQLite (`runner/executor-receipts.json`) gồm command ID, version, sample_interval_us, kết quả SHA-256 và outbox pending count.
  - Thông tin nguồn gốc nhị phân (binary provenance SHA-256).
  *(Lưu ý: Lệnh `export-results` của E3 không dùng cờ `--phase`; không xuất file nhị phân, secrets hay nội dung payload thô)*.

#### Bước 7 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Người vận hành sao chép thủ công toàn bộ thư mục kết quả đo từ Host B về Host A:
- Nguồn (Host B): `$ROOT/B/exported-results/`
- Đích (Host A): `$ROOT/A/received-results/`

Thiết lập phân quyền an toàn trên Host A:

```sh
chmod 0700 "$ROOT/A/received-results"
find "$ROOT/A/received-results" -type f -exec chmod 0600 {} +
```

*(Lưu ý: Giữ nguyên vẹn toàn bộ tập tin kết quả, không chỉnh sửa hash, session_id hay metadata; tuyệt đối không sao chép đè vào thư mục client-bundle)*.

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

Chờ cho đến khi lệnh `status` chuyển sang `phase=VERIFYING`.

**Cổng kiểm định kết quả (Validation Gate)**: Kiểm tra các trường trong status JSON bằng Python assertion:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3 | python3 -c 'import sys, json; data = json.load(sys.stdin); result = data.get("imported_result", {}); assert data.get("phase") == "VERIFYING"; assert data.get("workload_outcome") == "VALIDATED"; assert result.get("validation_valid") is True; assert result.get("validation_failures") == []; print("Confirmed workload VALIDATED")'
```

Hệ thống thực hiện quy trình thẩm tra đối chiếu tự động nghiêm ngặt:
- Đối chiếu biên lai command receipts trong SQLite của Host B khớp 100% với PostgreSQL trên Host A (khớp command ID, desired version, sample_interval_us, result_sha256).
- Đối chiếu cấu hình reported configuration qua REST GET API khớp với giá trị đã áp dụng trong SQLite.
- Phân tách luồng viễn trắc nền: Telemetry messages (12 ở low_load, 24 ở mixed) không bị ô nhiễm vào dữ liệu của các cảm biến điều khiển.
- Kiểm tra tính hợp lệ của overlap clock (ở chế độ mixed, kiểm tra khoảng giao thoa thời gian giữa A steady và B burst).

Sau khi xác nhận `workload_outcome=VALIDATED`, tiến hành dọn dẹp và hoàn tất phiên:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Kiểm tra status xác nhận:
- `phase=CLOSED`
- `cleanup_outcome=VERIFIED` (toàn bộ 12 containers, 1 network, 5 volumes được dọn dẹp sạch theo receipts).

Báo cáo kết quả và artifacts được lưu trữ an toàn trong thư mục:
`$ROOT/A/fixture/artifacts/`.

---

### 2.4 Quy trình hủy phiên (Abort) và thu hồi tài nguyên (Cleanup Recovery)

#### 1. Hủy phiên khẩn cấp (Abort Recipe)

Khi gặp lỗi không thể khắc phục trong quá trình thực thi (preflight fail, lỗi mạng/TLS, import bị từ chối, hoặc validation failures), người vận hành phát lệnh `abort` trên Terminal A2 từ phase hiện tại để dừng phiên và dọn dẹp tài nguyên Docker an toàn:

```sh
# Trích xuất phase hiện tại từ status JSON bằng Python:
CURRENT_PHASE=$(python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3 | python3 -c 'import sys, json; data = json.load(sys.stdin); ph = data.get("phase"); assert ph in ("READY", "MEASURING", "VERIFYING"), f"Invalid phase for abort: {ph}"; print(ph)')

# Phát lệnh hủy phiên:
python3 scripts/paper_p8_server.py abort \
  --root "$ROOT/A" \
  --experiment E3 \
  --session-id "$SESSION_ID" \
  --phase "$CURRENT_PHASE"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3
```

Chờ cho đến khi tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Toàn bộ artifacts và nhật ký lỗi được bảo toàn để phục vụ chẩn đoán nguyên nhân gốc.

#### 2. Dọn dẹp tài nguyên mồ côi (Stale Fixture Cleanup Recovery)

Trường hợp một phiên chạy trước đó bị gián đoạn đột ngột (tiến trình bị SIGKILL, mất điện, hoặc lỗi crash máy chủ) khiến containers, network hoặc volume Docker còn sót lại mà tiến trình `serve` không còn hoạt động, người vận hành thực hiện thu hồi tài nguyên an toàn thông qua cơ chế receipt-verified cleanup:

```sh
# Trích xuất project identifier từ status JSON bằng Python:
PROJECT_ID=$(python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E3 | python3 -c 'import sys, json; data = json.load(sys.stdin); prj = data.get("project"); assert prj, "project missing or empty in status JSON"; print(prj)')

# Thực hiện dọn dẹp an toàn dựa trên biên lai project:
python3 scripts/paper_p8_server.py cleanup \
  --root "$ROOT/A" \
  --project "$PROJECT_ID" \
  --experiment E3
```

Lệnh `cleanup` thu hồi chính xác các container và mạng thuộc về project đã chỉ định theo đúng danh sách biên lai, tuyệt đối không ảnh hưởng đến các container hoặc dịch vụ khác đang chạy trên hệ thống.

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
- **Phạm vi dừng / qualification**: Bốn profile E3 đã được ghi nhận hoàn thành ở checkpoint trước thông qua helper CLI; các chỉnh sửa runbook này không chạy lại native E3 và không nâng qualification của các lượt đó. Official30 native, full CI, HIL, physical A/B, capacity/soak và live rollout vẫn NOT RUN. Xem [báo cáo P8](paper-p8-experiments-and-acceptance.md#e3--subset-ba-lệnh-và-chẩn-đoán-probe) và checkpoint R0–R5.

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

### 4.3 Diễn giải đối chiếu kỳ vọng kết quả giữa Pilot 4 lệnh và Official 30 lệnh

Bảng dưới đây tổng hợp sự khác biệt về tham số cấu hình, hành vi điều phối và kỳ vọng kết quả kiểm tra đối soát giữa đợt chạy Pilot 4 lệnh (đã nghiệm thu thực tế trong §3) và luồng Official 30 lệnh (kế hoạch official30):

| Tiêu chí đối chiếu | Pilot 4 lệnh (`smoke4` — Baseline thực tế) | Official 30 lệnh (`official30` — Kế hoạch mở rộng) | Bounded Seam 3 lệnh (`official3` — Seam test) |
|---|---|---|---|
| **Hồ sơ (`--profile`)** | `pilot` | `official` (bắt buộc kèm `--official-opt-in`) | `official` (bắt buộc kèm `--official-opt-in`) |
| **Số lượng lệnh (`--e3-command-count`)** | `4` lệnh (2 lệnh / sensor) | `30` lệnh (15 lệnh / sensor) | `3` lệnh (2 lệnh sensor 0, 1 lệnh sensor 1) |
| **Lịch phát (`--e3-schedule`)** | `smoke` (dồn: 1s, 3.8s, 10s, 12s) | `steady` (giãn đều 1s/lệnh: 1s, 2s, ..., 30s) | `steady` (giãn đều 1s/lệnh: 1s, 2s, 3s) |
| **Tải viễn trắc nền (`--workload`)** | Hỗ trợ cả `low_load` lẫn `mixed` | Hỗ trợ cả `low_load` lẫn `mixed` | Bắt buộc `--workload low_load` |
| **Hồ sơ viễn trắc nền (Background)** | Profile `pilot` (1 msg/s, 100 samples/msg) | Duy trì profile `pilot` (1 msg/s, 100 samples/msg; không có cờ tốc độ cao) | Profile `pilot` (1 msg/s, 100 samples/msg) |
| **Ngân sách thời gian (`--deadline-seconds`)** | `900` giây (15 phút) | `1200` giây (20 phút) | `1200` giây (20 phút) |
| **Thời lượng đo lường (Control Measure/Wall)** | Measure: 14s, Drain: 30s, Wall: 60s | Measure: 32s (`max(8, 30s+2s)`), Drain: 30s, Wall: 62s | Measure: 8s, Drain: 30s, Wall: 60s |
| **Kỳ vọng Desired Version cuối** | `ctl-a`: 2, `ctl-b`: 2 | `ctl-a`: 15, `ctl-b`: 15 | `ctl-a`: 2, `ctl-b`: 1 |
| **Biên lai SQLite Receipts (mỗi Executor)** | 2 completed / 0 pending (tổng 4 receipts) | 15 completed / 0 pending (tổng 30 receipts) | 2 completed / 1 completed (tổng 3 receipts) |
| **Cơ chế điều phối (Dispatch order)** | Tuần tự có kiểm tra terminal | Tuần tự nghiêm ngặt (chờ terminal trước khi gửi lệnh kế tiếp) | Tuần tự có kiểm tra terminal |
| **Cập nhật cấu hình CSDL (Config DB)** | 2 rows (1 bản ghi/sensor) | 30 lần cập nhật cấu hình tuần tự qua REST | 3 lần cập nhật cấu hình |
| **Trạng thái nghiệm thu (Acceptance)** | `VALIDATED` (4 profile đã chạy thành công §3) | **NOT RUN** (chỉ có hợp đồng offline dry-run) | `PASS` (trên fixture native seam cô lập) |

#### Các quy tắc kiểm định then chốt của Official 30 lệnh:

1. **Nguyên tắc điều phối tuần tự và bảo vệ Version (Version Monotonicity)**:
   - Các lệnh điều khiển trong `official30` được phát tuần tự qua REST API với cơ chế kiểm tra trạng thái terminal: client chỉ phát lệnh kế tiếp cho một cảm biến khi lệnh trước đó trên cùng cảm biến đó đã đạt trạng thái terminal (`succeeded`, `failed`).
   - Tuyệt đối không phát dồn đồng thời 15 lệnh trên một cảm biến; không tạo bước nhảy version ngụy tạo nếu có lệnh bị timeout hay hoãn.
2. **Bảo tồn tính độc lập của Viễn trắc nền (No Background High-Rate Leakage)**:
   - Mặc dù phiên đo E3 mang nhãn `--profile official`, luồng viễn trắc nền E2 bên dưới vẫn duy trì hồ sơ `pilot` với tốc độ chuẩn 1 msg/s (100 samples/msg).
   - E3 không tích hợp và không chấp nhận các cờ phát tải viễn trắc tốc độ cao (như `--e2-offered-rate 1000/5000/20000`). Bất kỳ giả định nào về việc chạy viễn trắc nền tốc độ cao trong E3 đều không được công cụ helper hỗ trợ.
3. **Phân biệt ranh giới chứng thực**:
   - Việc vượt qua kiểm tra offline dry-run (`official30 dry-run`) chỉ xác nhận tính đúng đắn của manifest schema và run-spec; không được coi là đã kiểm định năng lực chịu tải hay nghiệm thu luồng 30 lệnh trên môi trường phân tán thực tế.
   - Trạng thái tổng thể toàn dự án vẫn duy trì `full_p8_accepted=false` và `physical_ab=NOT RUN`.
