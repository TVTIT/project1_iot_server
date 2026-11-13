# P8 E5 — Ma trận bóc tách cơ chế tối ưu (source-qualified)

> Cặp portable representative (8/2) một repeat đã source-qualify; kết quả và giới hạn nằm trong [checkpoint R0–R5 của báo cáo P8](paper-p8-experiments-and-acceptance.md#r5b-checkpoint-2026-11-06). Đây không phải ma trận official sáu arm hay tuyên bố capacity/performance.

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E5 (ma trận bóc tách cơ chế tối ưu đa pha trên 6 arm thực nghiệm: `optimized`, `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`). E5 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, WebSocket telemetry fanout, HTTP/REST và Mosquitto TLS).

Manual A/B hỗ trợ 1-repeat `pilot` và `representative` portable context; full matrix tiếp tục gồm sáu arm tuần tự, mỗi arm fixture riêng. Hai continuous viewers giữ kết nối xuyên initial/warm/control và chỉ được stop sau khi control hoàn tất. One-repeat representative pair không phải ma trận sáu arm, capacity/soak hay HIL evidence.

---

## 1. Tổng quan và kiến trúc thử nghiệm E5

### 1.1 Ma trận 6 Arm thử nghiệm E5

Ma trận E5 khảo sát tác động độc lập của từng cơ chế tối ưu hóa bằng phương pháp bóc tách (ablation study). Arm tham chiếu `optimized` kích hoạt toàn bộ các cơ chế tối ưu hóa; mỗi arm bóc tách tắt đi đúng một cờ cấu hình tương ứng so với `optimized`:

| Arm | Cờ cấu hình môi trường | Cơ chế kiểm chứng | Hành vi mong đợi |
|---|---|---|---|
| **`optimized`** (Tham chiếu) | `MQTT_MICROBATCH_ENABLED=true`<br>`MQTT_RAM_DEDUP_ENABLED=true`<br>`MQTT_SHAPER_ENABLED=true`<br>`STAGE5_ENABLED=true`<br>`STAGE5_WAKEUP_ENABLED=true`<br>`WEBSOCKET_ENABLED=true`<br>`WS_FANOUT_MODE=indexed` | Kích hoạt toàn bộ cơ chế tối ưu hóa | Toàn bộ 6 cơ chế vận hành song song; phục vụ 2 continuous viewers, hấp thụ 20 tin initial, lọc 10 tin warm replay, thực thi 4 lệnh control |
| **`ram_dedup_off`** | `MQTT_RAM_DEDUP_ENABLED=false` | Bộ đệm RAM khử trùng lặp tin nhắn | Tắt bộ nhớ RAM dedup; buộc backend phải truy vấn cơ sở dữ liệu để kiểm tra trùng lặp trong pha `warm_replay` |
| **`fanout_scan`** | `WS_FANOUT_MODE=scan` | Điều phối phân phát WebSocket Indexed vs Scan | Chuyển từ bảng chỉ mục `indexed` sang duyệt quét tuyến tính `scan` danh sách kết nối viewer cho từng tin nhắn telemetry |
| **`shaper_off`** | `MQTT_SHAPER_ENABLED=false` | Bộ điều tiết tốc độ Ingress (Token Bucket) | Tắt bộ lọc shaper; toàn bộ gói tin gửi đến được tiếp nhận trực tiếp vào hàng đợi mà không bị giới hạn tốc độ tức thời |
| **`microbatch_off`** | `MQTT_MICROBATCH_ENABLED=false` | Gom cụm vi mô tầng lưu trữ (Microbatching) | Tắt bộ gom cụm; mỗi tin nhắn telemetry được ghi xuống cơ sở dữ liệu qua một giao dịch SQL độc lập |
| **`polling`** | `STAGE5_WAKEUP_ENABLED=false` | Đánh thức điều khiển hướng sự kiện vs Thăm dò | Tắt cơ chế wakeup tức thời; worker điều khiển phụ thuộc hoàn toàn vào chu kỳ thăm dò cơ sở dữ liệu định kỳ (polling) |

### 1.2 Kiến trúc 4 Terminal và phân định hai Physical Hosts

Quy trình vận hành E5 yêu cầu 4 terminal độc lập phân tách rõ ràng trên hai máy vật lý nhằm bảo đảm nguyên tắc đặc quyền tối thiểu (Least Privilege) và duy trì 2 continuous viewers liên tục xuyên suốt 3 pha thử nghiệm:

```text
[MÁY CHỦ A - Fixture Server]                    [MÁY KHÁCH B - Load Generator]
Terminal A1: foreground `serve` (hold mode)      Terminal B1: foreground `e5-viewers` (continuous holder)
Terminal A2: CLI điều phối (status, begin,       Terminal B2: CLI tác vụ (bind, preflight, run,
             finish-phase, next-phase, finalize,               export, e5-stop-viewers)
             abort, cleanup)
```

- **MÁY CHỦ A (Host A — Quyền quản trị đầy đủ):** Chạy backend Go, PostgreSQL/TimescaleDB, Mosquitto DynSec, Supabase Auth, Nginx và các collectors (`metrics.csv`, `resources.csv`). Chịu trách nhiệm tạo bundle, cấp phát tài khoản, đối soát dữ liệu và dọn dẹp Docker container.
- **MÁY KHÁCH B (Host B — Quyền hạn tối thiểu - Least Privilege):** Chỉ chạy nhị phân Go (`paperload`, `gateway`) và script client Python. Nhận JWT người dùng và mật khẩu Gateway; tuyệt đối **không nhận** Docker socket, DB URL quản trị, service-role key hay quyền SSH quản trị/điều khiển Host A.
- **Giới hạn máy đo**: Thử nghiệm được thực hiện trên môi trường máy chủ cục bộ với metadata ghi nhận `generator_colocated=true` (hoặc co-located network). Chưa chứng minh môi trường hai máy vật lý độc lập qua mạng vật lý (`physical_ab=NOT RUN`).

### 1.3 Quy trình 3 Pha tuần tự và 2 Continuous Viewers xuyên suốt

Mỗi arm trong ma trận trải qua 3 pha đo lường tuần tự, trong đó 2 WebSocket continuous viewers (`e5-viewer-a`, `e5-viewer-b`) được duy trì kết nối liên tục từ trước Pha 1 cho đến cuối Pha 3. Số lượng tin nhắn viễn trắc và phát lại kiểm tra trùng lặp phụ thuộc vào preset được cấu hình (`representative` 8/2 hoặc `pilot` 20/10):

1. **Pha 1 — `initial` (Replay Telemetry tin nhắn đầu vào):** Máy khách B gửi tin nhắn viễn trắc đo lường mới (8 tin nhắn với preset `representative` mang 800 mẫu; hoặc 20 tin nhắn với preset `pilot` mang 2.000 mẫu; mỗi tin nhắn mang 100 mẫu đo lường). Backend tiếp nhận, áp dụng shaper, gom cụm microbatch, ghi cơ sở dữ liệu và chuyển tiếp WebSocket tới 2 viewers.
2. **Pha 2 — `warm_replay` (Replay Telemetry tin nhắn kiểm tra lọc trùng):** Máy khách B phát lại một tiền tố tin nhắn đã gửi ở Pha 1 (2 tin nhắn với preset `representative`; hoặc 10 tin nhắn với preset `pilot`). Kiểm tra năng lực lọc trùng lặp tại bộ nhớ RAM hoặc cơ sở dữ liệu mà không ghi trùng mẫu dữ liệu raw.
3. **Pha 3 — `control` (Thực thi 4 lệnh điều khiển bền vững & Dừng Viewer):** Máy khách B gửi 4 lệnh điều khiển desired-state, 2 gateway executors tiếp nhận lệnh và gửi ACK báo cáo kết quả. Sau khi chu kỳ điều khiển hoàn tất, gửi tín hiệu đóng 2 continuous viewers êm ái với 6 checkpoints hợp lệ.

### 1.4 Quy cách Context và Dataset dùng chung cho toàn ma trận

Context được tạo qua lệnh chuẩn bị chuyên dụng của E5 (`scripts/paper_p8_e5.py --prepare-only`; lưu ý: `paper_p8_server.py` không có subcommand `e5-context`):
- `seed` = 805 cho lượt lặp đầu tiên (ở chế độ `--mode official --repeat 3`, các lượt lặp kế tiếp dùng `seed + repeat`, tức 806 và 807).
- `anchor`: Mốc thời gian được tạo tự động lùi 1 giờ so với thời điểm chuẩn bị (`anchor = datetime.now() - 1 hour`, bỏ microseconds) để bảo đảm mốc thời gian an toàn trong quá khứ. Việc sinh lại lệnh này ở các thời điểm khác nhau sẽ tạo chuỗi băm dataset khác nhau; do đó trong một lượt lặp bắt buộc phải **ghim và giữ nguyên vẹn tập tin context đó cho toàn bộ 6 arm của lượt lặp**.
- Chuỗi băm dataset lịch sử ghi nhận: SHA-256 `2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175`, dung lượng 35.684 bytes (đối với preset `pilot` seed 805).
- Chuẩn bị mã nguồn sạch và nhị phân trước lượt `optimized`, tuyệt đối không sửa đổi source code, tài liệu hay fixture untracked trong suốt quá trình chạy 6 arm vì cơ chế kiểm tra reference sẽ đối chiếu cả `source_dirty_sha256`.

### 1.5 Cấu hình Khối lượng Công việc (Workload Presets: `representative` (8/2) vs `pilot` (20/10))

Ma trận E5 hỗ trợ hai preset khối lượng công việc được định nghĩa trong `scripts/tests/paper_p8_e5_support.py`:

| Thuộc tính metadata | Preset `representative` (8/2) | Preset `pilot` (20/10) | Ý nghĩa kỹ thuật |
|---|:---:|:---:|---|
| `profile` | `representative` | `pilot` | Định danh hồ sơ cấu hình tải |
| `telemetry_messages` | 8 | 20 | Số gói tin viễn trắc phát trong pha `initial` |
| `samples_per_message` | 100 | 100 | Số điểm đo mẫu (samples) trong mỗi gói tin viễn trắc |
| `initial_replay_count` | 8 | 20 | Tổng số tin viễn trắc ghi nhận (800 mẫu vs 2.000 mẫu) |
| `warm_replay_prefix` | 2 | 10 | Số gói tin tiền tố phát lại trong pha `warm_replay` để kiểm tra lọc trùng |
| `command_count` | 4 | 4 | Số lệnh điều khiển desired-state trong pha `control` (2 lệnh/gateway) |
| `viewer_count` | 2 | 2 | Số kết nối WebSocket continuous viewer duy trì liên tục |
| `initial_replay_rate_per_second` | 1 | 1 | Tốc độ phát tin viễn trắc đầu vào (1 tin/giây) |

> `export-results` hiện lấy counts từ bound context/manifest; representative 8/2 được source-qualified trên held portable client. Pilot 20/10 vẫn được hỗ trợ. Kết quả native-driver không thay thế portable-path evidence và ngược lại; qualification từng lane được ghi riêng trong báo cáo P8.

**Cơ chế lựa chọn Preset giữa Driver tự động và Quy trình Manual A/B:**
- **Trong Driver tự động (`scripts/paper_p8_e5.py`)**: Lựa chọn qua cờ `--workload-preset representative` (cho bài đo native cục bộ) hoặc `--workload-preset pilot` (mặc định), hoặc nạp lại ngữ cảnh qua `--context <path>` chứa tệp `context.json` đã sinh trước đó.
- **Trong quy trình Manual A/B (`paper_p8_server.py` + `paper_p8_client.py`)**: Preset được quyết định tại bước chuẩn bị context trên Host A (`python3 scripts/paper_p8_e5.py --root "$INPUTS_DIR" --prepare-only --workload-preset pilot`). Tệp `context.json` sinh ra là nguồn sự thật duy nhất mang siêu dữ liệu `workload_selection` và chuỗi băm xác thực `workload_selection_sha256`. Lệnh máy chủ `paper_p8_server.py serve` chỉ tiếp nhận `--context "$CTX_A"` và suy diễn mọi thông số tải từ context; **tuyệt đối không có và không tự tiện sáng chế cờ `--workload-preset` trên `paper_p8_server.py`**. Máy khách `paper_p8_client.py` cũng giải mã và tuân thủ các thông số tải này thông qua bundle được sinh ra từ context.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`$ROOT_A`, `$ROOT_B`, `$REP`, `$ARM`, `$SESSION_ID`). Tuyệt đối không đưa IP máy thật, đường dẫn cá nhân hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`, `gateway`): quyền `0700`.
- **Chống ghi đè tập tin và cô lập Fresh Fixture/Workspace**: Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả hay workspace cũ. Thư mục lá `$A_ARM` trên Host A và `$WORKSPACE` trên Host B **phải chưa tồn tại** trước khi chạy lệnh; các lệnh `serve` và `bind` yêu cầu tự tạo mới với quyền bảo vệ (`0700`). Mỗi arm trong từng lượt lặp có thư mục lá fixture và workspace lá độc lập.
- **Tính bất biến của Reference trong từng lượt lặp (Repeat-local Reference)**: Trong cùng một lượt lặp `$REP`, arm tham chiếu `optimized` phải được chạy trước để tạo tệp báo cáo `p8-e5-optimized.json` đạt `PASS` (`cleanup_verified: true`, `full_p8_accepted: false`). Tệp báo cáo này là bất biến trong suốt lượt lặp đó và được dùng làm `--reference` cho cả 5 arm bóc tách. Mỗi lượt lặp mới (`$REP=02`, `$REP=03`) có seed và anchor độc lập nên sẽ chạy `optimized` mới để tạo reference riêng cho lượt đó.
- **Ngân sách thời gian (Deadline 900s và Child Timeout 3900s)**:
  - *Chế độ máy chủ thủ công (Terminal A1/B1)*: `serve` và `e5-viewers` dùng deadline 900s trong ví dụ; JWT phải còn hạn đủ cho viewer deadline. Hoàn tất ba pha, stop/join, import và finalize trước khi deadline hết.
  - *Chế độ Native Driver (`scripts/paper_p8_e5.py`)*: Tham số `--child-timeout-seconds` có giá trị mặc định là `3900` giây (phạm vi ràng buộc hợp lệ: `1..7200` giây). Ngân sách 3900s này được tính toán hữu hạn nhằm bao phủ 3600s thời gian sống của viewer (wall time) cộng với 300s thời gian phụ trợ khởi tạo và dọn dẹp (lifecycle overhead) cho mỗi arm. Ngân sách khai báo tổng thể trong báo cáo tóm tắt là `(child_timeout_seconds + 60) * số_arm * repeat`.
- **Tính bất biến của nhị phân và container images**: Trong cùng một ma trận, 2 file nhị phân (`paperload`, `gateway`) và 12 container images phải được tái sử dụng nguyên vẹn byte-for-byte giữa arm `optimized` và 5 arm bóc tách qua cờ `--reference`.
- **Người vận hành tự xử lý chuyển giao dữ liệu (User-handled transfers)**: Việc chuyển bundle, binary và kết quả giữa Host A và Host B là thao tác của người vận hành. Tài liệu chỉ xác định chính xác đường dẫn nguồn và đích tương ứng, không đưa ra các lệnh `scp`/`rsync` chi tiết có thể sai khác về hạ tầng mạng hoặc quyền truy cập của người dùng.
- **Dừng Continuous Viewers B-local khi xảy ra lỗi/abort**: A không có cơ chế dừng B từ xa. Nếu control đã hoàn tất, dùng `e5-stop-viewers` trên B2; nếu lỗi xảy ra trước khi control hoàn tất hoặc stop không phản hồi, nhấn `Ctrl-C` trong chính terminal B1 đang giữ viewer rồi đợi process kết thúc. Xem Bước 11; không sửa phase state hoặc kill PID lấy từ file lock để vượt gate.

---

### 2.2 Quy trình vận hành chi tiết ma trận 6 Arm (Manual A/B Runbook)

Thiết lập thư mục gốc làm việc trên cả hai máy:
```sh
# Trên Host A:
ROOT_A=/var/tmp/opencode/p8-e5-run
mkdir -p "$ROOT_A" && chmod 0700 "$ROOT_A"

# Trên Host B:
ROOT_B=/var/tmp/opencode/p8-e5-b
mkdir -p "$ROOT_B" && chmod 0700 "$ROOT_B"
```

> **Nguyên tắc Tham số hóa Lượt lặp và Arm**:
> Toàn bộ các lệnh bên dưới được tham số hóa theo lượt lặp `$REP` (ví dụ `01`, `02`, `03`) và arm thực nghiệm `$ARM` (`optimized`, `shaper_off`, `microbatch_off`, `ram_dedup_off`, `polling`, `fanout_scan`).
> Trong mỗi lượt lặp `$REP`, arm `optimized` bắt buộc phải chạy đầu tiên để tạo tệp báo cáo tham chiếu `$REF_OPT`. Sau đó 5 arm bóc tách chạy tuần tự và tái sử dụng `$REF_OPT` qua cờ `--reference`.
> Mỗi terminal (A1, A2, B1, B2) được định nghĩa biến môi trường độc lập, copy-paste runnable mà không giả định biến truyền ngầm giữa các terminal.

#### Bước 1 — Chuẩn bị Context, Dataset và Kế hoạch Thứ tự các Arm (`--prepare-only`)

Chạy lệnh chuẩn bị context một lần trên Host A. Lệnh `scripts/paper_p8_e5.py` yêu cầu tự tạo thư mục `--root` với quyền `0700` và từ chối nếu thư mục đã tồn tại; do đó **chỉ tạo thư mục cha `$ROOT_A`**, tuyệt đối **không `mkdir` trước thư mục lá `$INPUTS_DIR`**.

##### Bước 1.1 — Lựa chọn Preset khối lượng công việc
- **Preset `pilot` (20/10)**: 20 telemetry messages (2,000 samples), warm replay 10, 4 commands, 2 viewers; giữ làm workload option.
- **Preset `representative` (8/2)**: 8 telemetry messages (800 samples), warm replay 2, 4 commands, 2 viewers; được hỗ trợ trong portable Manual A/B qua counts động từ context.

##### Bước 1.2 — Lệnh chuẩn bị Context cho 1 lượt lặp pilot hoặc $\ge 3$ lượt lặp official

**Trường hợp A — Bài đo 1 lượt lặp pilot (`--mode pilot --repeat 1`):**
```sh
# Trên Host A (Terminal A1 hoặc Terminal điều phối):
export ROOT_A=/var/tmp/opencode/p8-e5-run
mkdir -p "$ROOT_A" && chmod 0700 "$ROOT_A"
export INPUTS_DIR="$ROOT_A/inputs"

python3 scripts/paper_p8_e5.py \
  --root "$INPUTS_DIR" \
  --mode pilot \
  --repeat 1 \
  --seed 805 \
  --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan \
  --workload-preset pilot \
  --prepare-only

chmod 0600 "$INPUTS_DIR/repeat-01/context.json"
```
Tập tin context được tạo tại: `$INPUTS_DIR/repeat-01/context.json`.

**Trường hợp B — Bài đo chính thức $\ge 3$ lượt lặp (`--mode official --repeat 3`):**
```sh
# Trên Host A (Terminal A1 hoặc Terminal điều phối):
export ROOT_A=/var/tmp/opencode/p8-e5-run
mkdir -p "$ROOT_A" && chmod 0700 "$ROOT_A"
export INPUTS_DIR="$ROOT_A/inputs"

python3 scripts/paper_p8_e5.py \
  --root "$INPUTS_DIR" \
  --mode official \
  --repeat 3 \
  --seed 805 \
  --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan \
  --workload-preset pilot \
  --prepare-only

chmod 0600 "$INPUTS_DIR"/repeat-*/context.json
```
Lệnh sẽ sinh 3 thư mục độc lập trong cùng `$INPUTS_DIR`:
- `$INPUTS_DIR/repeat-01/context.json` (seed 805)
- `$INPUTS_DIR/repeat-02/context.json` (seed 806)
- `$INPUTS_DIR/repeat-03/context.json` (seed 807)

##### Bước 1.3 — Kế hoạch thực thi và thứ tự các Arm từ `driver-run.json`
Lệnh `--prepare-only` ghi lại kế hoạch thứ tự thực thi ngẫu nhiên hóa xác định vào `$INPUTS_DIR/driver-run.json`. Người vận hành kiểm tra thứ tự thực thi chính xác cho từng lượt lặp:
```sh
python3 - "$INPUTS_DIR/driver-run.json" <<'EOF'
import json, sys
with open(sys.argv[1], encoding='utf-8') as f:
    data = json.load(f)
for i, p in enumerate(data.get('plans', [])):
    items = p.get('plan', [])
    order_str = ' -> '.join(x['arm'] for x in items) if items else 'no arms planned'
    seed = items[0].get('run_seed', 'N/A') if items else 'N/A'
    print(f"Repeat {i+1} (seed {seed}): {order_str}")
EOF
```
Quy tắc lập lịch: Trong mỗi lượt lặp, `optimized` luôn đứng đầu tiên để sinh reference. 5 arm bóc tách kế tiếp được sắp xếp theo thứ tự ngẫu nhiên hóa dựa trên seed của lượt đó (`seed + repeat`). Người vận hành thực hiện Manual A/B theo đúng thứ tự này.

---

#### Bước 2 — Khởi chạy Server Fixture trên Terminal A1 (Host A)

Trên Terminal A1 (Host A), thiết lập các biến môi trường cho lượt lặp `$REP` và arm thực nghiệm `$ARM`, sau đó khởi chạy server ở chế độ foreground (hold mode; lưu ý thư mục lá `$A_ARM` **phải chưa tồn tại** trước khi chạy):

```sh
# Terminal A1 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"                      # Lượt lặp: 01, 02, 03...
export ARM="optimized"               # Arm: optimized, shaper_off, microbatch_off, ram_dedup_off, polling, fanout_scan
export INPUTS_DIR="$ROOT_A/inputs"
export ROOT_A_ARM="$ROOT_A/repeat-$REP/$ARM"
export A_ARM="$ROOT_A_ARM/A"         # Thư mục lá A phải CHƯA TỒN TẠI trước khi chạy
export NET_A="/path/to/private-network.json"
export CTX_A="$INPUTS_DIR/repeat-$REP/context.json"
export REF_OPT="$ROOT_A/repeat-$REP/optimized/A/fixture/artifacts/p8-e5-optimized.json"

mkdir -m 700 -p "$ROOT_A_ARM" && chmod 700 "$ROOT_A_ARM"
```

Khởi chạy tiến trình `serve`:
- Nếu `ARM="optimized"` (arm tham chiếu đầu tiên của lượt lặp, không truyền `--reference`):
```sh
python3 scripts/paper_p8_server.py serve \
  --root "$A_ARM" \
  --experiment E5 \
  --arm "$ARM" \
  --context "$CTX_A" \
  --network-config "$NET_A" \
  --deadline-seconds 900
```
- Nếu `$ARM` là một trong 5 arm bóc tách (`shaper_off`, `microbatch_off`, `ram_dedup_off`, `polling`, `fanout_scan`), bắt buộc bổ sung cờ `--reference "$REF_OPT"`:
```sh
python3 scripts/paper_p8_server.py serve \
  --root "$A_ARM" \
  --experiment E5 \
  --arm "$ARM" \
  --context "$CTX_A" \
  --reference "$REF_OPT" \
  --network-config "$NET_A" \
  --deadline-seconds 900
```

**Giải thích tham số lệnh `serve`:**
- `--root "$A_ARM"`: Đường dẫn thư mục lá chứa fixture và Docker Compose. Lệnh yêu cầu tự tạo thư mục này với quyền `0700` và sẽ từ chối nếu thư mục đã tồn tại.
- `--experiment E5`: Định danh thử nghiệm E5.
- `--arm "$ARM"`: Tên arm khảo sát trong ma trận 6 arm.
- `--context "$CTX_A"`: Tệp context JSON mang siêu dữ liệu tải, hạt giống seed, anchor và dataset. Máy chủ tự động đọc cấu hình tải từ context này; tuyệt đối không dùng cờ `--workload-preset`.
- `--reference "$REF_OPT"`: Đường dẫn tệp báo cáo `p8-e5-optimized.json` của arm `optimized` đã đạt PASS trong cùng repeat `$REP`. Bắt buộc đối với các arm bóc tách để đối chiếu bất biến nhị phân và container images.
- `--network-config "$NET_A"`: Tệp cấu hình mạng riêng tư cho Nginx và Mosquitto TLS SAN/IP.
- `--deadline-seconds 900`: Ngân sách thời gian tối đa (15 phút) cho toàn bộ vòng đời 3 pha của arm.

*(Tiến trình Terminal A1 phải được giữ chạy foreground xuyên suốt toàn bộ các pha cho đến khi hoàn tất finalize).*

Trên Terminal A2 (Host A), khai báo độc lập các biến shell tương ứng và kiểm tra trạng thái:

```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"                      # Khớp với Terminal A1
export ARM="optimized"               # Khớp với Terminal A1
export A_ARM="$ROOT_A/repeat-$REP/$ARM/A"
export INCOMING="$A_ARM/fixture/private/incoming"

python3 scripts/paper_p8_server.py status --root "$A_ARM" --experiment E5
```

Đợi cho đến khi status trả về:
```json
{
  "phase": "READY",
  "preparation_status": "E5_PREPARED",
  "collector_ready": null,
  "workload_outcome": "NOT_RUN"
}
```

Trích xuất `session_id` vào biến `SESSION_ID` để dùng cho các lệnh điều phối kế tiếp:
```sh
SESSION_ID=$(python3 -c "import json; print(json.load(open('$A_ARM/fixture/private/session.json'))['session_id'])")
echo "Session ID: $SESSION_ID"
sha256sum "$A_ARM/fixture/private/bin/paperload" "$A_ARM/fixture/private/bin/gateway"
```

---

#### Bước 3 — Chuyển giao gói dữ liệu và nhị phân sang Host B (Manual Transfer Step)

Người vận hành tự thực hiện chuyển giao tệp tin an toàn giữa hai máy với các vị trí nguồn và đích xác định:

Trước khi nhận tệp, trên B2 khai báo độc lập cùng repeat/arm với A1:

```sh
ROOT_B=/var/tmp/opencode/p8-e5-b
REP=01
ARM=optimized
mkdir -p "$ROOT_B/bin" && chmod 0700 "$ROOT_B" "$ROOT_B/bin"
```

1. **Chuẩn bị 2 file nhị phân trên Host B (`paperload` và `gateway`):**
   - Ghi nhận SHA-256 thực tế của hai binary trong lượt đang chạy. Không dùng checksum của pilot lịch sử ở Mục 3 làm giá trị bắt buộc cho source hiện tại. Giữ nguyên binary trên B giữa sáu arm của cùng repeat.
   - Nếu hai host cùng kiến trúc:
     - Nguồn trên Host A: `$A_ARM/fixture/private/bin/paperload` và `$A_ARM/fixture/private/bin/gateway`
     - Đích trên Host B: `$ROOT_B/bin/paperload` và `$ROOT_B/bin/gateway` (quyền `0700`).
   - Nếu khác môi trường, biên dịch trực tiếp từ mã nguồn trên Host B:
     ```sh
     # Trên Host B
     mkdir -m 700 -p "$ROOT_B/bin" && chmod 700 "$ROOT_B/bin"
     (cd src && CGO_ENABLED=0 go build -o "$ROOT_B/bin/paperload" ./cmd/paperload)
     (cd src && CGO_ENABLED=0 go build -o "$ROOT_B/bin/gateway" ./cmd/gateway)
     chmod 700 "$ROOT_B/bin/paperload" "$ROOT_B/bin/gateway"
     ```
2. **Chuyển giao client bundle từ Host A sang Host B:**
   - Nguồn trên Host A: Thư mục `$A_ARM/client-bundle/`
   - Đích trên Host B: Thư mục `$ROOT_B/repeat-$REP/bundles/$ARM/`
   - Thiết lập quyền an toàn trên Host B:
     ```sh
     mkdir -m 700 -p "$ROOT_B/repeat-$REP/bundles/$ARM"
     find "$ROOT_B/repeat-$REP/bundles/$ARM" -type d -exec chmod 0700 {} +
      find "$ROOT_B/repeat-$REP/bundles/$ARM" -type f -exec chmod 0600 {} +
      chmod 0700 "$ROOT_B/bin/paperload" "$ROOT_B/bin/gateway"
      sha256sum "$ROOT_B/bin/paperload" "$ROOT_B/bin/gateway"
     ```

---

#### Bước 4 — Thiết lập Workspace và tiền kiểm tra trên Terminal B2 (Host B)

Trên Terminal B2 (Host B), thiết lập các biến môi trường độc lập và ràng buộc workspace (chú ý thư mục lá `$WORKSPACE` **phải chưa tồn tại** trước khi chạy lệnh `bind`):

```sh
# Terminal B2 (Host B)
export ROOT_B=/var/tmp/opencode/p8-e5-b
export REP="01"                      # Khớp với lượt lặp trên Host A
export ARM="optimized"               # Khớp với arm trên Host A
export WORKSPACE="$ROOT_B/repeat-$REP/ws-$ARM"
export BUNDLE="$ROOT_B/repeat-$REP/bundles/$ARM"
export BIN_PL="$ROOT_B/bin/paperload"
export BIN_GW="$ROOT_B/bin/gateway"

# 1. Ràng buộc workspace (tự tạo thư mục $WORKSPACE với quyền 0700)
python3 scripts/paper_p8_client.py bind --bundle "$BUNDLE" --workspace "$WORKSPACE"

# 2. Tiền kiểm tra preflight offline (cần cả hai nhị phân)
python3 scripts/paper_p8_client.py preflight \
  --workspace "$WORKSPACE" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW"

# 3. Kiểm tra kết nối mạng (HTTPS, WS handshake, MQTT TLS)
python3 scripts/paper_p8_client.py check-connection --workspace "$WORKSPACE"
```

**Giải thích tham số lệnh client:**
- `bind --bundle "$BUNDLE" --workspace "$WORKSPACE"`: Giải nén bundle và thiết lập môi trường workspace mới với quyền `0700`.
- `preflight --workspace "$WORKSPACE" --binary "$BIN_PL" --gateway-binary "$BIN_GW"`: Kiểm tra tính hợp lệ của hai tệp nhị phân, đối chiếu kiến trúc và quyền thực thi mà không mở kết nối mạng.
- `check-connection --workspace "$WORKSPACE"`: Kiểm tra khả năng kết nối thực tế tới cổng HTTPS REST, bắt tay WebSocket và cổng Mosquitto TLS trên Host A theo token và chứng thư được cấp trong bundle.

---

#### Bước 5 — Khởi chạy Continuous Viewers trên Terminal B1 (Host B)

Khởi chạy tiến trình giữ kết nối 2 continuous viewers (`e5-viewer-a`, `e5-viewer-b`) xuyên suốt trên Terminal B1 (khai báo độc lập các biến shell cho Terminal B1):

```sh
# Terminal B1 (Host B)
export ROOT_B=/var/tmp/opencode/p8-e5-b
export REP="01"                      # Khớp với Terminal B2
export ARM="optimized"               # Khớp với Terminal B2
export WORKSPACE="$ROOT_B/repeat-$REP/ws-$ARM"
export BIN_PL="$ROOT_B/bin/paperload"
export BIN_GW="$ROOT_B/bin/gateway"

python3 scripts/paper_p8_client.py e5-viewers \
  --workspace "$WORKSPACE" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW" \
  --deadline-seconds 900
```

**Vòng đời Continuous Viewers (`READY -> hold -> stop/join`):**
1. **Khởi tạo và báo READY**: Tiến trình chiếm giữ tệp khóa `$WORKSPACE/viewer-holder.lock`, kết nối 2 WebSocket viewer sockets độc lập, thiết lập đồng hồ monotonic địa phương duy nhất và in thông báo `READY`.
2. **Duy trì (Hold mode)**: Tiến trình chạy foreground xuyên initial/warm/control để ghi sự kiện và checkpoints; không dừng sau warm.
3. **Dừng và thoát êm ái (Stop/Join)**: Chỉ sau khi `run --phase control` kết thúc và phase completion đã được export, dùng `e5-stop-viewers` trên B2. Lệnh chỉ gửi stop request; chờ Terminal B1 tự thoát, xác nhận exit 0, `STOPPED`/planned-stop, joined child return code 0 và `outputs/e5-viewer/workload.json` còn tồn tại trước export control. Không coi stop-request đơn lẻ là join proof.
4. **Xử lý sự cố B-local**: Host A không thể ngắt B từ xa. Nếu chưa hoàn tất control, `e5-stop-viewers` sẽ từ chối; ngắt bằng `Ctrl-C` trong chính terminal B1 đang giữ viewer, đợi tiến trình kết thúc và bảo toàn artifacts. Nếu control đã hoàn tất, dùng lệnh stop chuẩn ở Bước 8.

---

#### Bước 6 — Thực hiện Pha 1 (`initial` — Replay Telemetry 20 tin pilot)

1. **Trên Host A (Terminal A2):** Bắt đầu pha `initial` và kiểm tra collector:
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py begin \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase READY \
     --phase-id initial

   python3 scripts/paper_p8_server.py status --root "$A_ARM" --experiment E5
   ```
   Chờ status trả về: `phase=MEASURING`, `phase_id=initial`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Thực thi tải và xuất kết quả pha `initial`:
   ```sh
   # Terminal B2 (Host B)
   export EXP_INITIAL="$ROOT_B/repeat-$REP/export-initial-$ARM"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE" --binary "$BIN_PL" --phase initial
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE" --phase initial --output "$EXP_INITIAL"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Người vận hành tự sao chép an toàn tập tin kết quả từ Host B sang Host A:
     - Nguồn (Host B): `$EXP_INITIAL/results.json` và `$EXP_INITIAL/resources-generator.csv`
     - Đích (Host A): Thư mục `$INCOMING/initial/`
   - Chuẩn bị thư mục và phân quyền trên Host A (Terminal A2):
     ```sh
     mkdir -m 700 -p "$INCOMING/initial" && chmod 700 "$INCOMING/initial"
     # Sau khi chuyển tệp vào thư mục trên:
     chmod 600 "$INCOMING/initial"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả và mở pha tiếp theo:
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase MEASURING \
     --phase-id initial \
     --results "$INCOMING/initial/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial"]`. Sau đó mở pha tiếp theo:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING \
     --phase-id warm_replay
   ```
   Chờ status đạt `phase=READY`, `phase_id=warm_replay`.

---

#### Bước 7 — Thực hiện Pha 2 (`warm_replay` — Replay Telemetry 10 tin lọc trùng pilot)

1. **Trên Host A (Terminal A2):** Bắt đầu pha `warm_replay`:
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py begin \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase READY \
     --phase-id warm_replay

   python3 scripts/paper_p8_server.py status --root "$A_ARM" --experiment E5
   ```
   Chờ status: `phase=MEASURING`, `phase_id=warm_replay`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Chạy và xuất kết quả pha `warm_replay`:
   ```sh
   # Terminal B2 (Host B)
   export EXP_WARM="$ROOT_B/repeat-$REP/export-warm-$ARM"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE" --binary "$BIN_PL" --phase warm_replay
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE" --phase warm_replay --output "$EXP_WARM"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Người vận hành tự sao chép an toàn tập tin kết quả:
     - Nguồn (Host B): `$EXP_WARM/results.json` và `$EXP_WARM/resources-generator.csv`
     - Đích (Host A): Thư mục `$INCOMING/warm_replay/`
   - Chuẩn bị thư mục và phân quyền trên Host A (Terminal A2):
     ```sh
     mkdir -m 700 -p "$INCOMING/warm_replay" && chmod 700 "$INCOMING/warm_replay"
     # Sau khi chuyển tệp vào thư mục trên:
     chmod 600 "$INCOMING/warm_replay"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả và mở pha điều khiển:
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase MEASURING \
     --phase-id warm_replay \
     --results "$INCOMING/warm_replay/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial", "warm_replay"]`. Sau đó mở pha tiếp theo:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING \
     --phase-id control
   ```
   Chờ status đạt `phase=READY`, `phase_id=control`.

---

#### Bước 8 — Thực hiện Pha 3 (`control` — Thực thi 4 lệnh điều khiển & Dừng Continuous Viewers)

1. **Trên Host A (Terminal A2):** Bắt đầu pha `control`:
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py begin \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase READY \
     --phase-id control

   python3 scripts/paper_p8_server.py status --root "$A_ARM" --experiment E5
   ```
   Chờ status: `phase=MEASURING`, `phase_id=control`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Thực thi lệnh điều khiển, dừng viewers và xuất kết quả:
   ```sh
   # Terminal B2 (Host B)
   export EXP_CONTROL="$ROOT_B/repeat-$REP/export-control-$ARM"

   # 1. Chạy 4 lệnh điều khiển desired-state và 2 gateway executors
   python3 scripts/paper_p8_client.py run \
     --workspace "$WORKSPACE" \
     --binary "$BIN_PL" \
     --gateway-binary "$BIN_GW" \
     --phase control

    # 2. Dừng continuous viewers sau khi run control đã hoàn tất
    python3 scripts/paper_p8_client.py e5-stop-viewers --workspace "$WORKSPACE"
    # Đợi Terminal B1 kết thúc; xác nhận exit 0, holder STOPPED/joined và
    # outputs/e5-viewer/workload.json được ghi trước khi export control.
    # Nếu tiến trình B1 không phản hồi, chuyển sang Terminal B1 và nhấn Ctrl-C trực tiếp.

    # 3. Chỉ sau khi xác minh stop/join ở B1, xuất kết quả pha control
   python3 scripts/paper_p8_client.py export-results \
     --workspace "$WORKSPACE" \
     --phase control \
     --output "$EXP_CONTROL"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Người vận hành tự sao chép an toàn tập tin kết quả:
     - Nguồn (Host B): `$EXP_CONTROL/results.json` và `$EXP_CONTROL/resources-generator.csv`
     - Đích (Host A): Thư mục `$INCOMING/control/`
   - Chuẩn bị thư mục và phân quyền trên Host A (Terminal A2):
     ```sh
     mkdir -m 700 -p "$INCOMING/control" && chmod 700 "$INCOMING/control"
     # Sau khi chuyển tệp vào thư mục trên:
     chmod 600 "$INCOMING/control"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả pha control (không gọi `next-phase` vì đây là pha cuối):
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_ARM" \
     --experiment E5 \
     --session-id "$SESSION_ID" \
     --phase MEASURING \
     --phase-id control \
     --results "$INCOMING/control/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial", "warm_replay", "control"]`.

---

#### Bước 9 — Đóng phiên, Finalize và dọn dẹp Arm

Khi pha `control` đã đạt trạng thái `VERIFYING` và `workload_outcome: VALIDATED`:

```sh
# Terminal A2 (Host A)
python3 scripts/paper_p8_server.py finalize \
  --root "$A_ARM" \
  --experiment E5 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```

Quan sát và đối chiếu:
1. **Tiến trình Terminal A1**: Tự động đối chiếu, dọn dẹp sạch chính xác 12 containers / 1 network / 5 volumes sở hữu và thoát với mã `0`.
2. **Kiểm tra trạng thái session `$A_ARM/fixture/private/session.json`**:
   - `phase`: `"CLOSED"`
   - `workload_outcome`: `"VALIDATED"`
   - `cleanup_outcome`: `"VERIFIED"`
   - `phase_history`: `["initial", "warm_replay", "control"]`
3. **Báo cáo kết quả của arm được lưu tại**:
   `$A_ARM/fixture/artifacts/p8-e5-$ARM.json`
   Nếu `ARM="optimized"`, tệp `$A_ARM/fixture/artifacts/p8-e5-optimized.json` này chính là reference `$REF_OPT` bất biến cho 5 arm bóc tách kế tiếp trong cùng lượt lặp `$REP`.

---

#### Bước 10 — Vận hành tuần tự các Arm bóc tách (`ablation`) với `--reference`

Trong cùng một lượt lặp `$REP`, sau khi arm `optimized` hoàn tất (Bước 2 đến Bước 9), gán biến reference:
```sh
# Trên cả Terminal A1 và A2:
export REF_OPT="$ROOT_A/repeat-$REP/optimized/A/fixture/artifacts/p8-e5-optimized.json"
```

Lần lượt thực hiện cho từng arm bóc tách (`shaper_off`, `microbatch_off`, `ram_dedup_off`, `polling`, `fanout_scan`) theo đúng thứ tự đã lập trong kế hoạch (từ `driver-run.json` ở Bước 1.3):

1. **Trên Terminal A1:**
   - Đặt biến arm khảo sát: `export ARM="shaper_off"` (hoặc tên arm tương ứng: `microbatch_off`, `ram_dedup_off`, `polling`, `fanout_scan`).
   - Thiết lập `ROOT_A_ARM="$ROOT_A/repeat-$REP/$ARM"`, `A_ARM="$ROOT_A_ARM/A"`.
   - Tạo thư mục cha: `mkdir -m 700 -p "$ROOT_A_ARM" && chmod 700 "$ROOT_A_ARM"`.
   - Khởi chạy `paper_p8_server.py serve` với `--arm "$ARM"` và bổ sung cờ `--reference "$REF_OPT"`.
2. **Trên Terminal A2:**
   - Đặt `export ARM="shaper_off"`, `A_ARM="$ROOT_A/repeat-$REP/$ARM/A"`, `INCOMING="$A_ARM/fixture/private/incoming"`.
   - Chạy `status` cho đến khi `phase=READY`, trích xuất `SESSION_ID` mới.
3. **Chuyển giao Bundle:**
   - Người vận hành sao chép `$A_ARM/client-bundle/` sang `$ROOT_B/repeat-$REP/bundles/$ARM/` trên Host B.
4. **Trên Terminal B2:**
   - Đặt `export ARM="shaper_off"`, `WORKSPACE="$ROOT_B/repeat-$REP/ws-$ARM"`, `BUNDLE="$ROOT_B/repeat-$REP/bundles/$ARM"`.
   - Chạy `bind` (vào workspace lá mới chưa tồn tại), `preflight`, `check-connection`.
5. **Trên Terminal B1:**
   - Đặt `export ARM="shaper_off"`, `WORKSPACE="$ROOT_B/repeat-$REP/ws-$ARM"`.
   - Khởi chạy `e5-viewers` ở chế độ foreground (báo `READY` và giữ lock).
6. **Thực thi 3 pha:**
   - Thực hiện Bước 6 (`initial`), Bước 7 (`warm_replay`), Bước 8 (`control` và dừng viewers), Bước 9 (`finalize` và dọn dẹp).
7. **Lặp lại cho các arm còn lại**:
   - Hoàn tất tuần tự toàn bộ 5 arm bóc tách với workspace và fixture riêng biệt.
8. **Chuyển sang lượt lặp tiếp theo (đối với $\ge 3$ repeats official)**:
   - Đặt `export REP="02"` (hoặc `03`), dùng context `$INPUTS_DIR/repeat-$REP/context.json`, chạy lại `optimized` trước để sinh `REF_OPT` mới cho lượt đó, rồi chạy các ablation arms theo thứ tự plan của lượt tương ứng.

---

#### Bước 11 — Hướng dẫn xử lý sự cố, Hủy bỏ (`abort`), Dừng Viewer B-local và Dọn dẹp tài nguyên (`cleanup`)

Khi xảy ra sự cố trong quá trình chạy (lỗi mạng, tiến trình con bị treo, timeout hoặc lỗi kiểm tra dữ liệu), người vận hành phải tuân thủ quy trình dừng an toàn, **tuyệt đối không chạy `docker system prune` hay `docker compose down -v` bừa bãi** làm ảnh hưởng tới các volume dữ liệu khác.

> **Quy định quan trọng về Dừng Viewer trên Máy B:**
> Máy chủ A không có quyền can thiệp hay gửi tín hiệu ngắt sang Máy B qua mạng (`physical_ab=NOT RUN`). Khi xảy ra lỗi hoặc lệnh `abort` trên Host A, tiến trình continuous viewer trên Host B vẫn đang chạy foreground và giữ khóa `viewer-holder.lock`.

Nếu control **đã hoàn tất**, người vận hành trên B2 dùng lệnh dừng chuẩn:
```sh
# Terminal B2 (Host B)
python3 scripts/paper_p8_client.py e5-stop-viewers --workspace "$WORKSPACE"
```
Lệnh gửi stop request qua control directory của viewer holder. **Nếu control chưa hoàn tất**, lệnh sẽ từ chối với `e5_stop_viewers_requires_control_phase_completed`: không sửa state để vượt gate; chuyển sang terminal B1 và nhấn `Ctrl-C`. Cũng dùng B1/Ctrl-C nếu holder không phản hồi. Đợi tiến trình dừng, giữ nguyên outputs/journal; không coi việc ngắt giữa chừng là planned-stop acceptance và không tra cứu rồi kill PID một cách mù quáng.

**Hành vi xử lý Timeout và Ngắt tín hiệu của Driver (`scripts/paper_p8_e5.py --run`):**
Lệnh `scripts/paper_p8_e5.py --run` khởi chạy từng native fixture trong một process group độc lập (`start_new_session=True`). Khi xảy ra quá hạn thời gian (`timeout`) hoặc nhận tín hiệu ngắt từ người dùng (`Ctrl-C` / `KeyboardInterrupt`), driver ưu tiên gửi tín hiệu `SIGINT` trước tiên tới process group để khối `finally` thông thường của fixture, quy trình đối soát biên lai (receipt reconciliation) và thao tác giải phóng tài nguyên chính xác có cơ hội thực thi. Driver chờ một khoảng thời gian ân hạn dọn dẹp hữu hạn (`cleanup_grace=10` giây); nếu nhóm tiến trình vẫn chưa giải phóng hoàn toàn, driver sẽ leo thang hữu hạn qua `SIGTERM` (chờ thêm grace), và cuối cùng là `SIGKILL` nếu bắt buộc. Tệp tóm tắt dở dang `driver-run.json` giữ nguyên nguyên nhân lỗi chính (`primary_failure`, ví dụ `child_timeout` với mã thoát 124, hoặc `interrupted` với mã thoát 130) tách biệt hoàn toàn với kết quả dọn dẹp của nhóm tiến trình con (`cleanup`: `already_exited`, `joined_after_interrupt`, `joined_after_terminate`, `joined_after_kill`, hoặc `unresolved`). Cờ `cleanup_verified` của lượt chạy chỉ được ghi nhận là `true` khi cả hai điều kiện sau đồng thời thỏa mãn: (1) kết quả đọc lại artifact của fixture xác nhận `cleanup_verified: true`, VÀ (2) nhóm tiến trình đã kết thúc êm ái (`joined_after_interrupt` hoặc `already_exited`). Việc leo thang tín hiệu (`SIGTERM`/`SIGKILL`) hoặc thiếu vắng artifact fixture được kiểm chứng tuyệt đối KHÔNG đồng nghĩa với việc tài nguyên fixture đã được dọn dẹp an toàn; người vận hành không được tự ý tuyên bố đã dọn dẹp tài nguyên sau khi leo thang nếu chưa xác minh qua artifact. Khi đó, cần kiểm tra các biên lai/artifact tài nguyên còn lưu và thực thi lệnh dọn dẹp đối soát biên lai hiện có (`scripts/paper_p8_server.py cleanup ...`) cho các tài nguyên native chưa được giải phóng; tuyệt đối không dùng các lệnh hạ Compose diện rộng (`docker compose down -v`) hay dọn dẹp hệ thống bừa bãi (`docker system prune`). Các lượt chạy lịch sử không có `driver-run.json` vẫn giữ nguyên trạng và không được tái công nhận hồi tố.

##### 1. Hủy bỏ Phiên đang Hoạt động (`abort`)
Nếu server A1 vẫn đang chạy nhưng phiên bị lỗi ở một phase nào đó, gửi lệnh `abort` trên Terminal A2:
```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"                      # Lượt lặp bị lỗi
export ARM="optimized"               # Arm bị lỗi
export A_ARM="$ROOT_A/repeat-$REP/$ARM/A"
export CURRENT_PHASE=$(python3 -c "import json; print(json.load(open('$A_ARM/fixture/private/session.json'))['phase'])")
export SESSION_ID=$(python3 -c "import json; print(json.load(open('$A_ARM/fixture/private/session.json'))['session_id'])")

python3 scripts/paper_p8_server.py abort \
  --root "$A_ARM" \
  --experiment E5 \
  --session-id "$SESSION_ID" \
  --phase "$CURRENT_PHASE"
```
Lệnh `abort` sẽ yêu cầu supervisor thu hồi tài nguyên, ghi nhận `workload_outcome: ABORTED`, thực hiện dọn dẹp Docker container và chuyển phiên về `CLOSED`. Sau đó tiến trình server A1 sẽ thoát với mã 0.

##### 2. Dọn dẹp Tài nguyên Tồn đọng Sau sự cố (`cleanup`)
Trong trường hợp tiến trình server đã bị ngắt bất thường (crash hoặc bị kill) khiến các container Docker còn sót lại:
```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"
export ARM="optimized"
export A_ARM="$ROOT_A/repeat-$REP/$ARM/A"
export PROJECT=$(python3 -c "import json; print(json.load(open('$A_ARM/fixture/private/session.json'))['project'])")

python3 scripts/paper_p8_server.py cleanup \
  --root "$A_ARM" \
  --experiment E5 \
  --project "$PROJECT"
```

> **Biện pháp Bảo vệ Nghiêm ngặt của Lệnh Cleanup:**
> - Lệnh chỉ hoạt động khi tiến trình server A1 **đã dừng hoàn toàn** (không chạy cleanup khi server còn active).
> - Kiểm tra đối chiếu biên lai tài nguyên `resource-*.json` trong `$A_ARM/fixture/private/`. Lệnh chỉ xóa đúng các container, network và volume có biên lai xác nhận do chính phiên đó tạo ra.
> - Bảo toàn tuyệt đối mọi cơ sở dữ liệu SQLite, tệp nhật ký journal, tệp outbox pending của Gateway để phục vụ phân tích nguyên nhân lỗi (RCA).
> - Máy chủ A không có quyền can thiệp vào Máy B qua mạng; việc dọn dẹp các tiến trình con trên B phải do người vận hành xử lý cục bộ trên Máy B.

---

#### Bước 12 — So sánh đối chứng ma trận và tổng kết qua `compare` và `e5-summary`

##### 1. Đối chiếu Từng cặp Bằng CLI `compare`
Thực hiện so sánh read-only đối chiếu giữa arm tham chiếu `optimized` và từng arm bóc tách trong cùng lượt lặp `$REP`:
```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"
export REF_OPT="$ROOT_A/repeat-$REP/optimized/A/fixture/artifacts/p8-e5-optimized.json"
export ARM_FILE="$ROOT_A/repeat-$REP/shaper_off/A/fixture/artifacts/p8-e5-shaper_off.json"

python3 scripts/paper_p8_comparison.py compare \
  --ref "$REF_OPT" \
  --arm-file "$ARM_FILE" \
  --expected-arm shaper_off
```
Kết quả trả về đạt chuẩn:
```json
{
  "status": "COMPARABLE",
  "comparability": {
    "comparable": true,
    "reason": null,
    "expected_arm": "shaper_off",
    "differing_flag": "MQTT_SHAPER_ENABLED"
  },
  "mechanism_activation": {
    "eligible": true,
    "status": "ELIGIBLE",
    "reason": null
  }
}
```

##### 2. Xuất Báo cáo Tổng kết Ma trận Bằng CLI `e5-summary`
Sau khi toàn bộ 6 arm của một lượt lặp đều hoàn tất:
```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export REP="01"
export ART_DIR="$ROOT_A/repeat-$REP"
export EXPORT_SUMMARY_DIR="$ROOT_A/repeat-$REP/summary-export"

python3 scripts/paper_p8_comparison.py e5-summary \
  --optimized "$ART_DIR/optimized/A/fixture/artifacts/p8-e5-optimized.json" \
  --shaper-off "$ART_DIR/shaper_off/A/fixture/artifacts/p8-e5-shaper_off.json" \
  --microbatch-off "$ART_DIR/microbatch_off/A/fixture/artifacts/p8-e5-microbatch_off.json" \
  --ram-dedup-off "$ART_DIR/ram_dedup_off/A/fixture/artifacts/p8-e5-ram_dedup_off.json" \
  --polling "$ART_DIR/polling/A/fixture/artifacts/p8-e5-polling.json" \
  --fanout-scan "$ART_DIR/fanout_scan/A/fixture/artifacts/p8-e5-fanout_scan.json" \
  --output-dir "$EXPORT_SUMMARY_DIR"
```

Lệnh sinh ra 3 tệp tin đã được lọc sạch thông tin nhạy cảm:
- `e5-summary.json`: Báo cáo JSON tổng hợp trạng thái `PILOT_RUNS_COMPLETE_QUALIFIED`.
- `e5-runs.csv`: Số liệu chi tiết của từng arm.
- `e5-pairs.csv`: Bảng đối chiếu 5 cặp ablation so với `optimized`.

> **Phân biệt summary một repeat và aggregate nhiều repeat:** `e5-summary` tạo `e5-summary.json` cho một lần tổng hợp sáu arm; lệnh `e5-aggregate` (xem [Mục 4.1](#e5-aggregate-summary-của-các-repeat-đủ-sáu-arm)) nhận 3–10 summary có đủ sáu arm và provenance/cấu hình hợp lệ. Validator kiểm tra schema, digest và tính nhất quán cấu trúc; đây không phải xác thực mật mã rằng experiments đã thực sự chạy. E5 representative hai arm không phải summary sáu arm. Ba component fixtures có thể được đưa vào tooling dạng cấu trúc nếu thỏa validator, nhưng chỉ là component proof, không phải ba native experiments hoặc ba independent repeats. Aggregate output không nâng `full_p8_accepted`.

---

### 2.3 Quy trình thực thi tự động qua Native Driver (`scripts/paper_p8_e5.py`)

Bên cạnh quy trình Manual A/B 4 terminal trên hai máy độc lập, E5 cung cấp công cụ Native Driver `scripts/paper_p8_e5.py` hỗ trợ lập lịch và chạy tuần tự các arm thử nghiệm trên cùng môi trường cô lập:

> **Phân định Ranh giới Kỹ thuật của Native Driver**:
> Lệnh `scripts/paper_p8_e5.py --run` là công cụ **điều phối cục bộ (local orchestration)** trên một máy chủ (khởi chạy các fixture Docker native qua `native_compose_smoke.py`).
> Lệnh này **KHÔNG PHẢI** là cơ chế điều khiển Host B từ xa qua mạng (`physical_ab=NOT RUN`). Với môi trường hai máy vật lý thực sự A và B, việc vận hành phải tuân thủ quy trình Manual A/B ở Mục 2.2.

#### Bước 2.3.1 — Chế độ vận hành (`--mode`) và số lượt lặp (`--repeat`)
- `--mode pilot` (mặc định): Yêu cầu bắt buộc `--repeat 1`. Nếu truyền cờ `--context`, cờ này chỉ được áp dụng cho đúng 1 lượt lặp pilot.
- `--mode official`: Yêu cầu bắt buộc `--repeat >= 3`. Đây là quy định lặp lại bài đo trên cùng khối lượng công việc đã chọn (`workload_preset`) nhằm kiểm chứng độ tin cậy và phương sai; **tuyệt đối không phải là kiểm thử dung lượng/soak (capacity test)**. Chế độ `official` quản lý việc lập lịch các lượt lặp độc lập (mỗi lượt dùng seed `seed + repeat` và anchor riêng), không làm thay đổi kích thước tải của từng lượt. Cờ `--context` bị từ chối ở chế độ official; context của từng repeat được sinh tự động tại `repeat-01/`, `repeat-02/`, `repeat-03/`.

#### Bước 2.3.2 — Lựa chọn Arm và quy tắc thứ tự / tham chiếu (`--arm`, `--arms`, `--reference`)
- Lựa chọn arm: Sử dụng `--arm <arm>` cho kiểm thử gỡ lỗi một arm đơn lẻ, hoặc `--arms <arm1> <arm2> ...` cho một tập con các arm tuần tự (không dùng đồng thời cả hai cờ). Cờ `--run` bắt buộc phải đi kèm `--arm` hoặc `--arms` rõ ràng; driver không bao giờ ngầm định chạy toàn bộ 6 arm.
- Quy tắc ma trận (`optimized` cùng các arm ablation): Khi danh sách arm có chứa `optimized` cùng với các arm ablation (`matrix = true`), driver luôn lập lịch thực thi `optimized` đầu tiên để tạo tệp báo cáo tham chiếu `p8-e5-optimized.json`. Các arm ablation tiếp theo trong cùng lượt lặp sẽ tự động tái sử dụng tệp tham chiếu này theo thứ tự ngẫu nhiên hóa dựa trên seed (`seed + repeat`).
- Quy tắc chạy ablation riêng lẻ: Nếu danh sách được chọn chỉ gồm các arm ablation (không chứa `optimized`), lệnh `--run` bắt buộc phải nhận cả `--context` tái sử dụng và `--reference <path>` trỏ tới tệp báo cáo `p8-e5-optimized.json` hợp lệ đã đạt `PASS`, dọn dẹp sạch tài nguyên (`cleanup_verified: true`) và khớp fingerprint/seed/anchor/workload_selection.

#### Bước 2.3.3 — Chuẩn bị trước (`--prepare-only`) so với Thực thi Native (`--run`)
- `--prepare-only` (mặc định): Tạo cấu trúc thư mục private ngoài Git tree, sinh dataset và `context.json`, ghi `driver-run.json` với trạng thái `PREPARED`. Không khởi chạy bất kỳ container Docker hay fixture native nào.
- `--run`: Thực thi trực tiếp các fixture Docker native tuần tự thông qua `native_compose_smoke.py`. Hai cờ `--run` và `--prepare-only` xung đột và không được dùng chung.

#### Bước 2.3.4 — Ngân sách thời gian con (`--child-timeout-seconds`)
- Tham số `--child-timeout-seconds` có giá trị mặc định là `3900` giây (phạm vi hợp lệ: `1..7200` giây).
- Ngân sách hữu hạn 3900s bao phủ 3600s thời gian sống của viewer (wall time) cộng với 300s thời gian phụ trợ vòng đời mỗi arm. Driver giám sát tiến trình con và kích hoạt chu trình ngắt/dọn dẹp an toàn (`SIGINT` -> `SIGTERM` -> `SIGKILL`) nếu vượt quá hạn mức này.
- Ngân sách khai báo tổng thể trong báo cáo tóm tắt là `(child_timeout_seconds + 60) * số_arm * repeat`.

#### Bước 2.3.5 — Các lệnh mẫu thực thi qua Driver

```sh
# 1. Chuẩn bị context và kế hoạch cho 1 lượt lặp pilot (representative preset, seed 805):
python3 scripts/paper_p8_e5.py \
  --root /var/tmp/opencode/p8-e5-prep-pilot \
  --mode pilot \
  --repeat 1 \
  --workload-preset representative \
  --seed 805 \
  --prepare-only

# 2. Chuẩn bị context và kế hoạch cho >= 3 lượt lặp official đầy đủ 6 arm:
python3 scripts/paper_p8_e5.py \
  --root /var/tmp/opencode/p8-e5-prep-official \
  --mode official \
  --repeat 3 \
  --seed 805 \
  --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan \
  --workload-preset representative \
  --prepare-only

# 3. Thực thi native cục bộ 1 arm tham chiếu optimized (pilot):
python3 scripts/paper_p8_e5.py \
  --root /var/tmp/opencode/p8-e5-opt \
  --mode pilot \
  --repeat 1 \
  --workload-preset representative \
  --seed 805 \
  --arm optimized \
  --run \
  --child-timeout-seconds 3900

# 4. Thực thi native cục bộ toàn bộ ma trận 6 arm chính thức (official 3 repeats):
python3 scripts/paper_p8_e5.py \
  --root /var/tmp/opencode/p8-e5-matrix-official \
  --mode official \
  --repeat 3 \
  --seed 805 \
  --arms optimized shaper_off microbatch_off ram_dedup_off polling fanout_scan \
  --workload-preset representative \
  --run \
  --child-timeout-seconds 3900
```

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

### 3.1 Bảng kết quả thực chạy ma trận 6 Arm pilot (Seed 805)

Dữ liệu thực nghiệm thực chạy trên bộ tham số hạt giống seed 805 tại mã nguồn revision `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`:

| Arm | Status | Cleanup | Dọn dẹp tài nguyên | Cờ khác biệt so với `optimized` | Initial Phase | Warm Phase | Control Phase | DB Ledger / Raw / Temporal / Config | Viewers / Events | Cơ chế kích hoạt |
|---|:---:|:---:|---|---|:---:|:---:|:---:|:---:|:---:|---|
| **`optimized`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | *(Baseline tham chiếu)* | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | Microbatch: 16 groups, RAM hits: 10, Shaper: limited, Fanout: 20 indexed |
| **`shaper_off`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | `MQTT_SHAPER_ENABLED=false` | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | Shaper không can thiệp (rate_limited = 0); các cơ chế khác giữ nguyên |
| **`microbatch_off`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | `MQTT_MICROBATCH_ENABLED=false` | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | Tắt microbatch: 20 transactions riêng lẻ (thay vì 16 trong `optimized`) |
| **`ram_dedup_off`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | `MQTT_RAM_DEDUP_ENABLED=false` | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | RAM lookup hits = 0; 10 warm transactions truy vấn DB trực tiếp |
| **`polling`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | `STAGE5_WAKEUP_ENABLED=false` | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | Worker điều khiển vận hành qua polling định kỳ; không nhận wakeup tức thời |
| **`fanout_scan`** | **PASS** | VERIFIED | 12 containers, 1 net, 5 vols | `WS_FANOUT_MODE=scan` | COMPLETE | COMPLETE | COMPLETE | 20 / 2.000 / 20 / 2 | 2 / 20 | Phân phát WebSocket duyệt qua 40 scan evaluations (thay vì 20 indexed) |

Toàn bộ 6 arm đều đạt `cleanup_verified: true`, `exact_cleanup_verified: true` với 12 containers, 1 network, 5 volumes dọn sạch theo đúng receipts được cấp phát; không có tài nguyên Docker rò rỉ.

---

### 3.2 Bảng đối chiếu cặp cơ chế (Pairwise Ablation Comparisons)

Bảng đối chiếu 5 cặp bóc tách so với arm tham chiếu `optimized` (trích xuất từ `e5-pairs.csv` và `e5-summary.json`; lưu ý: trong summary export tổng hợp, các trường Gamma chính thức được ghi nhận là `UNAVAILABLE` do thiếu `phase_windows` snapshot, các tỷ lệ phần trăm bên dưới là đối chiếu trực tiếp từ các bộ đếm thao tác thô trong cửa sổ phase của đợt chạy pilot này):

| Cặp đối chiếu (Ablation vs Optimized) | Tính khả so (Comparability) | Cờ khác biệt | Trạng thái kích hoạt cơ chế | Cửa sổ quan sát | Mẫu số đánh giá | Chỉ số suy giảm / Khác biệt quan sát được |
|---|:---:|---|:---:|---|---|---|
| **`microbatch_off` vs `optimized`** | **COMPARABLE** | `MQTT_MICROBATCH_ENABLED` | **ELIGIBLE** | Initial import phase | Single pilot; seed 805 | Thao tác thô: 16 vs 20 lượt (`copy_calls`, `commits`, `begin_attempts`). Tỷ lệ quan sát: **20.0%**.<br>*(Trong summary export: `UNAVAILABLE: process_instance_mismatch_or_missing`)* |
| **`ram_dedup_off` vs `optimized`** | **COMPARABLE** | `MQTT_RAM_DEDUP_ENABLED` | **ELIGIBLE** | Warm replay phase | Single pilot; seed 805 | Thao tác thô: 0 vs 10 lượt (`warm_attempts`, `warm_commits`, `warm_transactions`). Tỷ lệ quan sát: **100.0%**.<br>*(Trong summary export: `UNAVAILABLE: process_instance_mismatch_or_missing`)* |
| **`shaper_off` vs `optimized`** | **COMPARABLE** | `MQTT_SHAPER_ENABLED` | **ELIGIBLE** | Ingress rate window | Single pilot; seed 805 | Kích hoạt bộ điều tiết; chỉ số lợi ích tổng hợp là `UNAVAILABLE` |
| **`polling` vs `optimized`** | **COMPARABLE** | `STAGE5_WAKEUP_ENABLED` | **ELIGIBLE** | Control dispatch | Single pilot; seed 805 | Vận hành điều khiển qua polling; chỉ số lợi ích tổng hợp là `UNAVAILABLE` |
| **`fanout_scan` vs `optimized`** | **COMPARABLE** | `WS_FANOUT_MODE` | **INCONCLUSIVE** | WebSocket fanout | Single pilot; seed 805 | `fanout_viewer_readiness_unverified`; 40 scan evaluations vs 20 indexed |

---

### 3.3 Phân tích kích hoạt cơ chế và hiệu quả thực nghiệm

Phân tích đối chứng giữa arm tham chiếu `optimized` và các arm bóc tách được phân định rạch ròi giữa việc **Kích hoạt cơ chế (Mechanism Activation)** theo thiết kế thí nghiệm và **Lợi ích đo đạc tổng hợp (Observed Benefit)**:

- **Cửa sổ quan sát kỳ vọng của các pha**:
  - *Cửa sổ import `initial`*: Tải đo lường mới đầu vào (20 tin nhắn với `pilot` / 8 tin nhắn với `representative`; mỗi tin 100 mẫu).
  - *Cửa sổ phát lại `warm_replay`*: Kiểm tra năng lực lọc trùng lặp (10 tin nhắn với `pilot` / 2 tin nhắn với `representative`).
  - *Cửa sổ điều phối `control`*: Thực thi 4 lệnh điều khiển desired-state, thu nhận kết quả từ 2 gateway executors và đóng êm ái 2 continuous viewers.

1. **Cơ chế gom cụm vi mô tầng lưu trữ (`microbatch_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Trong pha `initial`, với cùng 2.000 dòng mẫu dữ liệu viễn trắc (`raw_samples_retained = 2000`), arm `optimized` gom nhóm thành 16 giao dịch commit cơ sở dữ liệu (`begin_attempts = 16`, `confirmed_commits = 16`, `copy_calls = 16`), trong khi `microbatch_off` phải thực hiện 20 giao dịch commit độc lập (20 vs 16).
   - *Tỉ lệ suy giảm*: Đạt mức giảm **20.0%** số lượt mở giao dịch và thao tác COPY dữ liệu ($\Gamma_{\text{red}} = 20.0\%$). (Trong bài đo representative: 4 vs 8 thao tác, đạt $\Gamma_{\text{red}} = 50.0\%$).
   - *Lợi ích tổng hợp và giới hạn*: Trong tệp xuất `e5-summary.json`, chỉ số Gamma tổng hợp được ghi nhận là `UNAVAILABLE` (`process_instance_mismatch_or_missing`) do thiếu snapshot `phase_windows` đối chiếu định danh tiến trình backend, không phải lợi ích bằng 0. Mức giảm này áp dụng cho các bộ đếm thao tác SQL ban đầu, không đại diện cho toàn bộ tài nguyên hệ thống hay thông lượng đỉnh.

2. **Cơ chế lọc trùng lặp bộ nhớ RAM (`ram_dedup_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Trong pha `warm_replay`, 10 tin nhắn phát lại đã được arm `optimized` lọc sạch hoàn toàn tại bộ nhớ RAM (`ram_lookup_hits = 10`, `ram_db_calls_avoided = 10`), dẫn đến số giao dịch và commit cơ sở dữ liệu phát sinh bằng 0 (`warm_attempts = 0`, `warm_commits = 0`, `warm_transactions = 0`). Ngược lại, `ram_dedup_off` phát sinh toàn bộ 10 giao dịch kiểm tra cơ sở dữ liệu (`warm_attempts = 10`, `warm_commits = 10`). (Trong bài đo representative: 0 vs 2 warm transactions).
   - *Tỉ lệ suy giảm*: Đạt mức giảm **100.0%** tải giao dịch cơ sở dữ liệu trong pha warm replay ($\Gamma_{\text{red}} = 100.0\%$).
   - *Lợi ích tổng hợp*: Ghi nhận `UNAVAILABLE` trong bảng summary do cùng lý do thiếu snapshot tiến trình như trên.

3. **Cơ chế điều tiết tốc độ Ingress (`shaper_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`** khi phát sinh lưu lượng vượt ngưỡng điều tiết token bucket (tốc độ định mức 2 tin/giây, burst 4 tin).
   - *Hiệu quả ghi nhận*: Arm `optimized` kích hoạt giới hạn tốc độ token bucket khi phát sinh lưu lượng vượt ngưỡng (`shaper_rate_limited > 0`), trong khi `shaper_off` không ghi nhận sự kiện giới hạn tốc độ.
   - *Hiện tượng Shaper Inactivity (Không can thiệp khi tải thấp)*: Khi lưu lượng đầu vào được phát đều đặn ở mức 1 tin/giây (dưới ngưỡng tốc độ 2 tin/s), token bucket của shaper không kích hoạt điều tiết (`rate_limited = 0`); đây là trạng thái không can thiệp (inactivity) bình thường theo thiết kế khi tải nằm trong hạn mức an toàn, không phải là cơ chế bị lỗi hay không hoạt động. Ở các bài đo không có đợt phát vượt burst, cặp đối chiếu này ghi nhận `INCONCLUSIVE` về mặt lợi ích tốc độ.

4. **Cơ chế đánh thức điều khiển hướng sự kiện (`polling` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Cả hai arm đều hoàn tất 4 lệnh điều khiển và cập nhật 2 cấu hình; tuy nhiên `polling` dựa trên chu kỳ thăm dò cơ sở dữ liệu định kỳ, trong khi `optimized` sử dụng kênh wakeup tức thời.
   - *Lợi ích tổng hợp*: Chỉ số lợi ích tổng hợp ghi nhận `UNAVAILABLE` do thiếu snapshot tiến trình tương tự các cặp trên.

5. **Cơ chế phân phát WebSocket (`fanout_scan` vs `optimized`):**
   - *Kích hoạt*: Ghi nhận **`INCONCLUSIVE`** do nguyên nhân `fanout_viewer_readiness_unverified`.
   - *Bản chất kỹ thuật*: Arm `fanout_scan` thực hiện 40 lượt đánh giá quét (`scan_evaluations = 40`, `indexed_selections = 0`), trong khi `optimized` sử dụng 20 lượt trỏ chỉ mục trực tiếp (`indexed_selections = 20`, `scan_evaluations = 0`). Cả hai đều phân phát chính xác các sự kiện viễn trắc tới 2 viewers.
   - *Độ sẵn sàng (Viewer Readiness)*: Mặc dù số lượt duyệt đánh giá quét phản ánh đúng sự khác biệt cơ chế, tại ranh giới bắt đầu đo lường chưa có cơ chế kiểm chứng độc lập độ sẵn sàng (`readiness`) tuyệt đối của kết nối viewer; do đó kết quả được đánh giá thận trọng là `INCONCLUSIVE` thay vì khẳng định cải thiện hiệu năng.

---

### 3.4 Tính bất biến của Binary và Container Images qua 6 Arm

Để bảo đảm tính hợp lệ của phương pháp bóc tách khoa học, toàn bộ mã thực thi và container images được duy trì bất biến tuyệt đối giữa 6 arm:

- **Tập tin thực thi nhị phân phía Client (Host B):**
  - `paperload`: SHA-256 `d29f58f30646c972b36dce3862b151f6e6ffcec634ff695c6ce75d7ae8a3b8c8`
  - `gateway`: SHA-256 `993da9f59407d270ac8fccf298e0afa419cf7fb26fdb777f41c6e67f42635afe`
- **12 Container Images phía Server (Host A):**
  - `application-migrations`: `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
  - `auth-socket-init`: `sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`
  - `backend`: `sha256:924679a9aa50bc773ebfc2230d2ddc2534a15e83a758c448a5154fa5a0b410ec`
  - `gateway`: `sha256:9c82293c3eafafc125abd67df61f04b2cfcfeb8484dd928d584021a6226f6fac`
  - `mosquitto`: `sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`
  - `nginx`: `sha256:516475cc129da42866742567714ddc681e5eed7b9ee0b9e9c015e464b4221a00`
  - `postgres`: `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
  - `supabase-auth`: `sha256:c0c25187a6b835e65a6f6e6c6b39d090e832d40e6de5186f2c038e0411944232`
  - `supabase-compat-migration`: `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
  - `supabase-envoy`: `sha256:57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4`
  - `supabase-role-provisioner`: `sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a`
  - `supabase-storage`: `sha256:f1546fac6d1c7e345428ac904bfaa7be7cecd50a1f549fe1cf38c628a7b15c85`
- **Mã nguồn và dấu vết môi trường:**
  - `source_revision`: `52fd9456a9dbaa3316196eb92de40d14ba4ef32e`
  - `source_dirty_sha256`: `b4bd87aec7600da3357b2f72ad0e5d05611449fb8f703f3fde5a4ea5f14fa223`
  - `journal_sha256`: `b10e8d998e056342549392f4b733baf2dd50caba38b7e02620c512264223dbbe`
  - `workload_sha256`: `6a4da3560f24686cf66dc709d74a59cbb6458ef2222fcb8a2eb4584ff28aeaab`

---

### 3.5 Danh mục Checksum và Đường dẫn Artifacts

Các tập tin báo cáo kết quả thô của 6 arm được lưu giữ với mã băm SHA-256 toàn vẹn:
- **`optimized`**:
  - SHA-256: `7039c0a9eb731fa0bd17735d1f9a851d271c2e249f9ec6f0aec80d1b46937188`
  - Artifact ID: `p8-e5-optimized-7039c0a9eb731fa0`
- **`shaper_off`**:
  - SHA-256: `090fc844cf1bbd688384d10f5902d8a865c186e8c39651068efeb7d593ecf2a0`
  - Artifact ID: `p8-e5-shaper_off-090fc844cf1bbd68`
- **`microbatch_off`**:
  - SHA-256: `3aafb955dd42a60037f3f39bbdb65c764c2218f49892a68a258e63c2ef31c3d9`
  - Artifact ID: `p8-e5-microbatch_off-3aafb955dd42a600`
- **`ram_dedup_off`**:
  - SHA-256: `9252b4ecd604539a26e426fc0ac3272284af148a329024f04f20e4ff7f3880d4`
  - Artifact ID: `p8-e5-ram_dedup_off-9252b4ecd604539a`
- **`polling`**:
  - SHA-256: `0c6d6097e17acd8477af68e07bb3a1100bf9b55aa327923f20ba2103ccda3739`
  - Artifact ID: `p8-e5-polling-0c6d6097e17acd84`
- **`fanout_scan`**:
  - SHA-256: `0cadf405faabd3c83630f5354d4ee075fc03336db0f53dd64df6540a5d4c88de`
  - Artifact ID: `p8-e5-fanout_scan-0cadf405faabd3c8`

Các tệp tin tổng kết ma trận được lưu trữ tại `docs/backend/results/p8/e5/` (và bản sao kiểm chuẩn `docs/backend/results/p8/helper-e5-20261031/`):
- `e5-summary.json` (Trạng thái tổng thể: `PILOT_RUNS_COMPLETE_QUALIFIED`, `full_p8_accepted: false`)
- `e5-runs.csv`
- `e5-pairs.csv`

---

### 3.6 Xử lý sự cố và các lỗi thường gặp trong lịch sử E5

| Mã lỗi / Tình huống | Nguyên nhân gốc | Hướng xử lý an toàn |
|---|---|---|
| `e5_reference_required` | Chạy ablation arm mà quên truyền `--reference` | Bắt buộc truyền `--reference` trỏ tới báo cáo `p8-e5-optimized.json` đã đạt `PASS`. |
| `FileExistsError` khi `serve` hoặc `bind` | Thư mục lá `$ROOT_A/A` hoặc `$WORKSPACE` đã tồn tại từ trước | Chỉ tạo thư mục cha; đặt biến thư mục lá sang tên mới hoàn toàn (chưa tạo trước). |
| `runner_drain_unconfirmed_control` | Gateway simulator còn hàng tồn outbox chưa ACK và xả không hết | Không sửa file thủ công hay chạy lại export; kiểm tra log tiến trình simulator xem có bị ngắt giữa chừng không, bảo tồn tệp SQLite pending, sửa lỗi và chạy lại một lượt mới. |
| `missing_fingerprint_gateway_binary_sha256` hoặc `..._paperload_...` | Thiếu một trong hai hash nhị phân thành phần trên B | Biên dịch và cung cấp đầy đủ cả 2 file nhị phân `paperload` và `gateway` trên Máy B trước khi preflight. |
| `window_duration_mismatch` | Cửa sổ khai báo cấu hình (`declared_measurement_windows`) bị sai lệch | Giữ nguyên context và cấu hình thời gian khai báo bất biến giữa các arm; biến thiên mili-giây của thời gian đo thực tế (`observed`) được chấp nhận hợp lệ. |
| `invalid_fingerprint_phase_seconds` | Cửa sổ khai báo có chứa boolean `True`, giá trị âm, hoặc thiếu pha | Cửa sổ khai báo bắt buộc phải là số thực hữu hạn dương và đủ 3 pha: `initial`, `warm_replay`, `control`. |

---

## 4. Giới hạn và trạng thái gate

Các lượt representative ngày 05/11/2026, kể cả FAIL và timeout driver, được ghi tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#e5--representative-một-repeat-và-timeout).

- **Trạng thái cổng P8**: `full_p8_accepted=false`.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm chạy trên cùng máy chủ cục bộ với metadata ghi nhận chuẩn tắc là `generator_colocated=true` (hoặc co-located network); chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng.
- **Kích hoạt Cơ chế ≠ Tăng tốc Toàn diện**: Trạng thái `ELIGIBLE` của các cặp đối chiếu chỉ chứng minh cơ chế được kích hoạt đúng theo thiết kế thí nghiệm (ví dụ: shaper có giới hạn tốc độ, microbatch có gom nhóm, RAM dedup có trúng cache, polling không nhận wakeup). Không tuyên bố mức tăng tốc thông lượng/độ trễ toàn hệ thống từ bài đo pilot đơn lẻ này.
- **Chỉ số Gamma Persistence**: Báo cáo cuối rút gọn đưa vào summary không chứa `phase_windows` với ảnh chụp quản trị/process instance mà bộ tính delta yêu cầu. Vì vậy $\Gamma_{\text{red}}$ của summary này là `UNAVAILABLE` (`process_instance_mismatch_or_missing`), không phải 0. Các artifact phase chi tiết vẫn được giữ riêng; không suy từ summary thiếu snapshot rằng mọi snapshot gốc đã mất.
- **Mẫu định danh hữu hạn**: Viewer chỉ trích xuất 2 mẫu định danh đại diện (`e5-viewer-a`, `e5-viewer-b`) để đối soát tính toàn vẹn (bounded sample provenance), không liệt kê toàn bộ 20 sự kiện.
- **Không trừ chéo đồng hồ**: Đồng hồ kiểm tra checkpoint là monotonic cục bộ của tiến trình continuous viewer holder trên Máy B; không trừ chéo đồng hồ giữa các máy khác nhau.
- **Mẫu tài nguyên là thưa**: Các số liệu CPU/RAM chỉ là mẫu định kỳ của tiến trình, không phản ánh đỉnh tải RSS hay tổng tài nguyên máy chủ.
- **Phạm vi dừng**: Dừng lại tại mốc nghiệm thu E5 pilot ma trận 6 arm; không tự động mở rộng hay chuyển sang các giai đoạn tiếp theo nếu chưa có yêu cầu mới.

<a id="e5-aggregate-summary-cua-cac-repeat-du-sau-arm"></a>
### 4.1 Aggregate summary của các repeat đủ sáu arm

Chỉ aggregate 3–10 repeat summaries đã được export/qualified, mỗi summary phải đại diện cho đủ sáu arm (`optimized`, `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`) và provenance/config/repeat validation đầy đủ. Một cặp portable E5 hai arm không phải full-repeat summary; ba component fixtures cũng không phải ba native repeats. Nếu summary cũ thiếu provenance, re-export từ immutable full six-arm reports; không bổ sung provenance bằng tay.

Trên máy giữ summary inputs, dùng private directories/files (directory `0700`, input/output `0600`), inputs tối đa 5 MiB mỗi file. Tạo output directory mới, không tồn tại và không phải symlink. Lệnh từ chối input/output thiếu qualification, provenance/config mismatch, repeat trùng hoặc output overwrite/symlink. Đường dẫn dưới đây là placeholders; thêm `--repeat-summary` một lần cho mỗi summary:

```sh
export SUMMARY_01=/path/to/qualified/repeat-01/e5-summary.json
export SUMMARY_02=/path/to/qualified/repeat-02/e5-summary.json
export SUMMARY_03=/path/to/qualified/repeat-03/e5-summary.json
export AGGREGATE_OUT=/path/to/new/private/e5-aggregate-output

python3 scripts/paper_p8_comparison.py e5-aggregate \
  --repeat-summary "$SUMMARY_01" \
  --repeat-summary "$SUMMARY_02" \
  --repeat-summary "$SUMMARY_03" \
  --output-dir "$AGGREGATE_OUT"
```

Outputs are `e5-aggregate.json` and `e5-aggregate.csv`. The descriptive report includes eligible metrics and median/range; it does not pool histograms, create a performance/capacity claim, or set `full_p8_accepted=true`. Record unavailable/ineligible reasons rather than silently dropping them. Actual CLI accepts 3–10 summaries; three is the minimum, not proof that three such native repeats have been completed.
