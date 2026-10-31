# P8 E5 — Ma trận bóc tách cơ chế tối ưu (source-qualified)

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E5 (ma trận bóc tách cơ chế tối ưu đa pha trên 6 arm thực nghiệm: `optimized`, `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`). E5 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, WebSocket telemetry fanout, HTTP/REST và Mosquitto TLS).

Thử nghiệm chạy tuần tự 6 arm trong từng fixture độc lập; mỗi arm được kiểm định dữ liệu và dọn dẹp tài nguyên độc lập. Đây là bài đo telemetry và control tổng hợp đa pha với 2 continuous viewers kết nối liên tục, không phải kiểm thử dung lượng/soak lặp lại $\ge 3$ lần hay chứng nhận phần cứng thực tế (HIL/AM5728).

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

Mỗi arm trong ma trận trải qua 3 pha đo lường tuần tự, trong đó 2 WebSocket continuous viewers (`e5-viewer-a`, `e5-viewer-b`) được duy trì kết nối liên tục từ trước Pha 1 cho đến cuối Pha 3:

1. **Pha 1 — `initial` (Replay Telemetry 20 tin nhắn đầu vào):** Máy khách B gửi 20 tin nhắn viễn trắc đo lường mới. Backend tiếp nhận, áp dụng shaper, gom cụm microbatch, ghi cơ sở dữ liệu và chuyển tiếp WebSocket tới 2 viewers.
2. **Pha 2 — `warm_replay` (Replay Telemetry 10 tin nhắn kiểm tra lọc trùng):** Máy khách B phát lại 10 tin nhắn đã gửi ở Pha 1. Kiểm tra năng lực lọc trùng lặp tại bộ nhớ RAM hoặc cơ sở dữ liệu mà không ghi trùng mẫu dữ liệu raw.
3. **Pha 3 — `control` (Thực thi 4 lệnh điều khiển bền vững & Dừng Viewer):** Máy khách B gửi 4 lệnh điều khiển desired-state, 2 gateway executors tiếp nhận lệnh và gửi ACK báo cáo kết quả. Sau khi chu kỳ điều khiển hoàn tất, gửi tín hiệu đóng 2 continuous viewers êm ái với 6 checkpoints hợp lệ.

### 1.4 Quy cách Context và Dataset dùng chung cho toàn ma trận

Context được tạo một lần qua lệnh chuẩn bị chuyên dụng của E5 (`scripts/paper_p8_e5.py --prepare-only`; lưu ý: `paper_p8_server.py` không có subcommand `e5-context`) và chia sẻ nguyên vẹn cho toàn bộ 6 arm trong ma trận:
- `seed` = 805 (hạt giống sinh số ngẫu nhiên xác định).
- `anchor`: Mốc thời gian được tạo tự động lùi 1 giờ so với thời điểm chuẩn bị (`anchor = datetime.now() - 1 hour`, bỏ microseconds) để bảo đảm mốc thời gian an toàn trong quá khứ. Việc sinh lại lệnh này ở các thời điểm khác nhau sẽ tạo chuỗi băm dataset khác nhau; do đó trong một đợt đo bắt buộc phải **ghim và giữ nguyên vẹn tập tin context đó cho toàn bộ 6 arm**.
- Chuỗi băm dataset lịch sử ghi nhận: SHA-256 `2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175`, dung lượng 35.684 bytes.
- Chuẩn bị mã nguồn sạch và nhị phân trước lượt `optimized`, tuyệt đối không sửa đổi source code, tài liệu hay fixture untracked trong suốt quá trình chạy 6 arm vì cơ chế kiểm tra reference sẽ đối chiếu cả `source_dirty_sha256`.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`$ROOT_A`, `$ROOT_B`, `$SESSION_ID_OPTIMIZED`, `$SESSION_ID_<ARM>`). Tuyệt đối không đưa IP máy thật, đường dẫn cá nhân hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`, `gateway`): quyền `0700`.
- **Chống ghi đè tập tin**: Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả hay workspace cũ. Thư mục lá `$A` trên Host A và `$WORKSPACE` trên Host B **phải chưa tồn tại** trước khi chạy lệnh; các lệnh `serve` và `bind` yêu cầu tự tạo mới với quyền bảo vệ.
- **Ngân sách thời gian (Deadline 900s)**: Tiến trình `serve` trên Terminal A1 và `e5-viewers` trên Terminal B1 có thời hạn hiệu lực `--deadline-seconds 900` (15 phút). Toàn bộ 3 pha phải được hoàn tất trước khi hết hạn ngân sách này.
- **Tính bất biến của nhị phân và container images**: Trong cùng một ma trận, 2 file nhị phân (`paperload`, `gateway`) và 12 container images phải được tái sử dụng nguyên vẹn byte-for-byte giữa arm `optimized` và 5 arm bóc tách qua cờ `--reference`.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Step)**: Việc chuyển bundle, binary và kết quả giữa Host A và Host B là thao tác truyền file thủ công của người vận hành (qua lệnh `scp` hoặc `rsync` an toàn), không tuyên bố là tự động hóa ngầm.

---

### 2.2 Quy trình vận hành chi tiết ma trận 6 Arm

Thiết lập thư mục gốc làm việc trên cả hai máy:
```sh
# Trên Host A:
ROOT_A=/var/tmp/opencode/p8-e5-run
mkdir -p "$ROOT_A" && chmod 0700 "$ROOT_A"

# Trên Host B:
ROOT_B=/var/tmp/opencode/p8-e5-b
mkdir -p "$ROOT_B" && chmod 0700 "$ROOT_B"
```

#### Bước 1 — Tạo Context và Dataset dùng chung cho toàn ma trận (Host A)

Chạy lệnh chuẩn bị context một lần duy nhất khi bắt đầu ma trận trên Host A. Lưu ý: lệnh `paper_p8_e5.py` yêu cầu tự tạo thư mục `--root` với quyền `0700` và từ chối nếu thư mục đã tồn tại; do đó **chỉ tạo thư mục cha `$ROOT_A`**, tuyệt đối **không `mkdir` trước thư mục lá `$INPUTS_DIR`**:

```sh
# Trên Host A (Terminal A1 hoặc Terminal điều phối):
export ROOT_A=/var/tmp/opencode/p8-e5-run
mkdir -p "$ROOT_A" && chmod 0700 "$ROOT_A"
export INPUTS_DIR="$ROOT_A/inputs"

python3 scripts/paper_p8_e5.py --root "$INPUTS_DIR" --seed 805 --prepare-only
chmod 0600 "$INPUTS_DIR/repeat-01/context.json"
```

Tập tin context được tạo tại: `$INPUTS_DIR/repeat-01/context.json`. Tập tin này phải được giữ cố định không đổi cho toàn bộ 6 arm.

---

#### Bước 2 — Khởi chạy Arm tham chiếu `optimized` trên Terminal A1 (Host A)

Trên Terminal A1 (Host A), thiết lập biến môi trường và khởi chạy server ở chế độ foreground (hold mode; lưu ý thư mục lá `$A_OPT` phải chưa tồn tại trước khi chạy):

```sh
# Terminal A1 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export ROOT_A_OPT="$ROOT_A/optimized"
export A_OPT="$ROOT_A_OPT/A"
export NET_A="/path/to/private-network.json"
export CTX_A="$ROOT_A/inputs/repeat-01/context.json"

mkdir -m 700 -p "$ROOT_A_OPT" && chmod 700 "$ROOT_A_OPT"

python3 scripts/paper_p8_server.py serve \
  --root "$A_OPT" \
  --experiment E5 \
  --arm optimized \
  --context "$CTX_A" \
  --network-config "$NET_A" \
  --deadline-seconds 900
```

*(Tiến trình Terminal A1 phải được giữ chạy foreground xuyên suốt toàn bộ các pha cho đến khi hoàn tất finalize).*

Trên Terminal A2 (Host A), khai báo lại biến môi trường cần thiết (biến shell không tự động chia sẻ giữa các terminal) và kiểm tra trạng thái:

```sh
# Terminal A2 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-run"
export A_OPT="$ROOT_A/optimized/A"

python3 scripts/paper_p8_server.py status --root "$A_OPT" --experiment E5
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

Đặt `session_id` nhận được vào biến `SESSION_ID_OPTIMIZED` để dùng cho các lệnh sau:
```sh
SESSION_ID_OPTIMIZED=<session_id>
```
*(Hoặc có thể trích xuất tự động qua lệnh: `SESSION_ID_OPTIMIZED=$(python3 -c "import json; print(json.load(open('$A_OPT/fixture/private/session.json'))['session_id'])")`)*

---

#### Bước 3 — Chuyển giao gói dữ liệu và nhị phân sang Host B (Manual Transfer Step)

1. **Chuẩn bị 2 file nhị phân trên Host B (`paperload` và `gateway`):**
   - Nếu hai host cùng kiến trúc, sao chép trực tiếp từ Host A:
     - Tập tin `$A_OPT/fixture/private/bin/paperload` sang `$ROOT_B/bin/paperload`.
     - Tập tin `$A_OPT/fixture/private/bin/gateway` sang `$ROOT_B/bin/gateway`.
   - Nếu khác môi trường, biên dịch từ cùng mã nguồn trên Host B:
     ```sh
     # Trên Host B
     mkdir -m 700 -p "$ROOT_B/bin" && chmod 700 "$ROOT_B/bin"
     (cd src && CGO_ENABLED=0 go build -o "$ROOT_B/bin/paperload" ./cmd/paperload)
     (cd src && CGO_ENABLED=0 go build -o "$ROOT_B/bin/gateway" ./cmd/gateway)
     chmod 700 "$ROOT_B/bin/paperload" "$ROOT_B/bin/gateway"
     ```
2. **Sao chép client bundle từ Host A sang Host B (Manual Transfer Step):**
   - Sao chép toàn bộ thư mục `$A_OPT/client-bundle/` sang Host B tại `$ROOT_B/bundles/optimized/`.
   - Phân quyền trên Host B:
     ```sh
     mkdir -m 700 -p "$ROOT_B/bundles/optimized"
     find "$ROOT_B/bundles/optimized" -type d -exec chmod 0700 {} +
     find "$ROOT_B/bundles/optimized" -type f -exec chmod 0600 {} +
     ```

---

#### Bước 4 — Thiết lập Workspace và tiền kiểm tra trên Terminal B2 (Host B)

Trên Terminal B2 (Host B), thiết lập các biến môi trường và ràng buộc workspace (chú ý thư mục lá `$WORKSPACE_OPT` **chưa được tạo trước**):

```sh
# Terminal B2 (Host B)
export ROOT_B=/var/tmp/opencode/p8-e5-b
export WORKSPACE_OPT="$ROOT_B/ws-optimized"
export BUNDLE_OPT="$ROOT_B/bundles/optimized"
export BIN_PL="$ROOT_B/bin/paperload"
export BIN_GW="$ROOT_B/bin/gateway"
# Điền thông tin đích truyền tệp và đường dẫn tuyệt đối trên Host A:
export A_TARGET="operator@host-a"
export A_INCOMING_REMOTE="/var/tmp/opencode/p8-e5-run/optimized/A/fixture/private/incoming"

# 1. Ràng buộc workspace
python3 scripts/paper_p8_client.py bind --bundle "$BUNDLE_OPT" --workspace "$WORKSPACE_OPT"

# 2. Tiền kiểm tra preflight offline (cần cả hai nhị phân)
python3 scripts/paper_p8_client.py preflight \
  --workspace "$WORKSPACE_OPT" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW"

# 3. Kiểm tra kết nối mạng (HTTPS, WS handshake, MQTT TLS)
python3 scripts/paper_p8_client.py check-connection --workspace "$WORKSPACE_OPT"
```

---

#### Bước 5 — Khởi chạy Continuous Viewers trên Terminal B1 (Host B)

Khởi chạy tiến trình giữ kết nối 2 continuous viewers xuyên suốt trên Terminal B1 (khai báo lại các biến shell cho Terminal B1 độc lập):

```sh
# Terminal B1 (Host B)
export ROOT_B=/var/tmp/opencode/p8-e5-b
export WORKSPACE_OPT="$ROOT_B/ws-optimized"
export BIN_PL="$ROOT_B/bin/paperload"
export BIN_GW="$ROOT_B/bin/gateway"

python3 scripts/paper_p8_client.py e5-viewers \
  --workspace "$WORKSPACE_OPT" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW" \
  --deadline-seconds 900
```

*(Tiến trình Terminal B1 sẽ chạy foreground, chiếm giữ khóa `viewer-holder.lock`, báo `READY`, và duy trì đồng hồ monotonic địa phương duy nhất để ghi nhận 6 checkpoints).*

---

#### Bước 6 — Thực hiện Pha 1 (`initial` — Replay Telemetry 20 tin nhắn)

1. **Trên Host A (Terminal A2):** Thiết lập biến thư mục incoming và bắt đầu pha `initial`:
   ```sh
   # Terminal A2 (Host A)
   export INCOMING="$A_OPT/fixture/private/incoming"

   python3 scripts/paper_p8_server.py begin \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase READY \
     --phase-id initial

   python3 scripts/paper_p8_server.py status --root "$A_OPT" --experiment E5
   ```
   Chờ cho đến khi status trả về: `phase=MEASURING`, `phase_id=initial`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Chạy và xuất kết quả pha `initial`:
   ```sh
   export EXP_INITIAL="$ROOT_B/export-initial-optimized"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE_OPT" --binary "$BIN_PL" --phase initial
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE_OPT" --phase initial --output "$EXP_INITIAL"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Trên Host A (Terminal A2), tạo thư mục nhận:
     ```sh
     mkdir -m 700 -p "$INCOMING/initial" && chmod 700 "$INCOMING/initial"
     ```
   - Trên Host B (Terminal B2), sao chép kết quả sang Host A:
     ```sh
     scp "$EXP_INITIAL/results.json" "$EXP_INITIAL/resources-generator.csv" "$A_TARGET:$A_INCOMING_REMOTE/initial/"
     ```
   - Trên Host A (Terminal A2), xác lập quyền đọc an toàn:
     ```sh
     chmod 600 "$INCOMING/initial"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả và mở pha tiếp theo:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase MEASURING \
     --phase-id initial \
     --results "$INCOMING/initial/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial"]`. Sau đó mở pha tiếp:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase VERIFYING \
     --phase-id warm_replay
   ```
   Chờ status đạt `phase=READY`, `phase_id=warm_replay`.

---

#### Bước 7 — Thực hiện Pha 2 (`warm_replay` — Replay Telemetry 10 tin nhắn kiểm tra lọc trùng)

1. **Trên Host A (Terminal A2):** Bắt đầu pha `warm_replay`:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase READY \
     --phase-id warm_replay
   ```
   Chờ status: `phase=MEASURING`, `phase_id=warm_replay`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Chạy và xuất kết quả pha `warm_replay`:
   ```sh
   export EXP_WARM="$ROOT_B/export-warm-optimized"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE_OPT" --binary "$BIN_PL" --phase warm_replay
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE_OPT" --phase warm_replay --output "$EXP_WARM"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Trên Host A (Terminal A2):
     ```sh
     mkdir -m 700 -p "$INCOMING/warm_replay" && chmod 700 "$INCOMING/warm_replay"
     ```
   - Trên Host B (Terminal B2):
     ```sh
     scp "$EXP_WARM/results.json" "$EXP_WARM/resources-generator.csv" "$A_TARGET:$A_INCOMING_REMOTE/warm_replay/"
     ```
   - Trên Host A (Terminal A2):
     ```sh
     chmod 600 "$INCOMING/warm_replay"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả và mở pha điều khiển:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase MEASURING \
     --phase-id warm_replay \
     --results "$INCOMING/warm_replay/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial", "warm_replay"]`. Sau đó mở pha tiếp:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase VERIFYING \
     --phase-id control
   ```
   Chờ status đạt `phase=READY`, `phase_id=control`.

---

#### Bước 8 — Thực hiện Pha 3 (`control` — Thực thi 4 lệnh điều khiển bền vững & Dừng Viewer)

1. **Trên Host A (Terminal A2):** Bắt đầu pha `control`:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase READY \
     --phase-id control
   ```
   Chờ status: `phase=MEASURING`, `phase_id=control`, `collector_ready=true`.

2. **Trên Host B (Terminal B2):** Thực thi lệnh điều khiển, dừng viewer và xuất kết quả:
   ```sh
   export EXP_CONTROL="$ROOT_B/export-control-optimized"

   # 1. Chạy 4 lệnh điều khiển và 2 gateway executors
   python3 scripts/paper_p8_client.py run \
     --workspace "$WORKSPACE_OPT" \
     --binary "$BIN_PL" \
     --gateway-binary "$BIN_GW" \
     --phase control

   # 2. Dừng continuous viewers sau khi run control đã hoàn tất
   python3 scripts/paper_p8_client.py e5-stop-viewers --workspace "$WORKSPACE_OPT"
   # Quan sát: Tiến trình Terminal B1 thoát êm ái với exit code 0 (planned_stop=true)

   # 3. Xuất kết quả pha control
   python3 scripts/paper_p8_client.py export-results \
     --workspace "$WORKSPACE_OPT" \
     --phase control \
     --output "$EXP_CONTROL"
   ```

3. **Chuyển giao kết quả từ Host B sang Host A (Manual Transfer Step):**
   - Trên Host A (Terminal A2):
     ```sh
     mkdir -m 700 -p "$INCOMING/control" && chmod 700 "$INCOMING/control"
     ```
   - Trên Host B (Terminal B2):
     ```sh
     scp "$EXP_CONTROL/results.json" "$EXP_CONTROL/resources-generator.csv" "$A_TARGET:$A_INCOMING_REMOTE/control/"
     ```
   - Trên Host A (Terminal A2):
     ```sh
     chmod 600 "$INCOMING/control"/*
     ```

4. **Trên Host A (Terminal A2):** Thẩm định kết quả pha control (không gọi `next-phase` vì đây là pha cuối):
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$A_OPT" \
     --experiment E5 \
     --session-id "$SESSION_ID_OPTIMIZED" \
     --phase MEASURING \
     --phase-id control \
     --results "$INCOMING/control/results.json"
   ```
   Chờ status đạt `phase=VERIFYING`, `workload_outcome=VALIDATED`, `phase_history=["initial", "warm_replay", "control"]`.

---

#### Bước 9 — Đóng phiên, Finalize và dọn dẹp Arm tham chiếu `optimized`

Khi pha `control` đã đạt trạng thái `VERIFYING` và `workload_outcome: VALIDATED`:

```sh
# Terminal A2 (Host A)
python3 scripts/paper_p8_server.py finalize \
  --root "$A_OPT" \
  --experiment E5 \
  --session-id "$SESSION_ID_OPTIMIZED" \
  --phase VERIFYING
```

Quan sát và đối chiếu:
1. **Tiến trình Terminal A1**: Tự động đối chiếu, dọn dẹp sạch chính xác 12 containers / 1 network / 5 volumes sở hữu và thoát với mã `0`.
2. **Kiểm tra trạng thái session `$A_OPT/fixture/private/session.json`**:
   - `phase`: `"CLOSED"`
   - `workload_outcome`: `"VALIDATED"`
   - `cleanup_outcome`: `"VERIFIED"`
   - `phase_history`: `["initial", "warm_replay", "control"]`
3. **Báo cáo kết quả của arm tham chiếu được lưu tại**:
   `$A_OPT/fixture/artifacts/p8-e5-optimized.json`

---

#### Bước 10 — Khởi chạy các Arm bóc tách (`ablation`) với `--reference`

Với từng arm bóc tách (`ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`), bắt buộc truyền tham số `--reference` trỏ tới báo cáo của arm tham chiếu `optimized` đã đạt `PASS` (`$A_OPT/fixture/artifacts/p8-e5-optimized.json`).

Ví dụ chi tiết cho arm **`shaper_off`**:

1. **Khởi chạy Server trên Terminal A1 (Host A):**
   ```sh
   # Terminal A1 (Host A)
   export ROOT_A_SHAPER="$ROOT_A/shaper_off"
   export A_SHAPER="$ROOT_A_SHAPER/A"
   export REF_OPT="$A_OPT/fixture/artifacts/p8-e5-optimized.json"

   mkdir -m 700 -p "$ROOT_A_SHAPER" && chmod 700 "$ROOT_A_SHAPER"

   python3 scripts/paper_p8_server.py serve \
     --root "$A_SHAPER" \
     --experiment E5 \
     --arm shaper_off \
     --context "$CTX_A" \
     --reference "$REF_OPT" \
     --network-config "$NET_A" \
     --deadline-seconds 900
   ```

2. **Kiểm tra trạng thái trên Terminal A2 (Host A):**
   ```sh
   # Terminal A2 (Host A)
   python3 scripts/paper_p8_server.py status --root "$A_SHAPER" --experiment E5
   ```
   Chờ status trả về `phase=READY` và `preparation_status=E5_PREPARED`.

   Đặt `session_id` nhận được vào biến `SESSION_ID_SHAPER_OFF` để dùng cho các lệnh sau:
   ```sh
   SESSION_ID_SHAPER_OFF=<session_id>
   ```
   *(Hoặc có thể trích xuất tự động qua lệnh: `SESSION_ID_SHAPER_OFF=$(python3 -c "import json; print(json.load(open('$A_SHAPER/fixture/private/session.json'))['session_id'])")`)*

3. **Chuyển giao bundle sang Host B (Manual Transfer Step):**
   Sao chép thư mục `$A_SHAPER/client-bundle/` sang Host B tại `$ROOT_B/bundles/shaper_off/`.

4. **Thiết lập Workspace lá mới và khởi chạy viewers trên Host B:**
   - Trên Terminal B2:
     ```sh
     export WORKSPACE_SHAPER="$ROOT_B/ws-shaper_off"
     python3 scripts/paper_p8_client.py bind --bundle "$ROOT_B/bundles/shaper_off" --workspace "$WORKSPACE_SHAPER"
     python3 scripts/paper_p8_client.py preflight --workspace "$WORKSPACE_SHAPER" --binary "$BIN_PL" --gateway-binary "$BIN_GW"
     python3 scripts/paper_p8_client.py check-connection --workspace "$WORKSPACE_SHAPER"
     ```
   - Trên Terminal B1:
     ```sh
     python3 scripts/paper_p8_client.py e5-viewers --workspace "$WORKSPACE_SHAPER" --binary "$BIN_PL" --gateway-binary "$BIN_GW" --deadline-seconds 900
     ```

5. **Thực thi 3 pha tuần tự và finalize:**
   Thực hiện tương tự Bước 6, Bước 7, Bước 8 và Bước 9. Cần lưu ý cập nhật đầy đủ các biến môi trường cho arm tương ứng trước khi chạy:
   - Trên Terminal A2:
     ```sh
     export A_ARM="$ROOT_A_SHAPER/A"
     export INCOMING="$A_ARM/fixture/private/incoming"
     export SESSION_ID="$SESSION_ID_SHAPER_OFF"
     ```
     Sử dụng `$A_ARM` cho cờ `--root` và `$SESSION_ID` cho cờ `--session-id` trong các lệnh server (`begin`, `finish-phase`, `next-phase`, `finalize`).
   - Trên Terminal B2:
     ```sh
     export WORKSPACE_ARM="$ROOT_B/ws-shaper_off"
     export A_INCOMING_REMOTE="/var/tmp/opencode/p8-e5-run/shaper_off/A/fixture/private/incoming"
     ```
     Sử dụng `$WORKSPACE_ARM` cho cờ `--workspace` khi chạy `run`, `export-results` và lệnh truyền kết quả `scp`.

6. **Lặp lại cho các arm bóc tách còn lại:**
   Thực hiện tuần tự cho `microbatch_off` (`SESSION_ID_MICROBATCH_OFF`), `ram_dedup_off` (`SESSION_ID_RAM_DEDUP_OFF`), `polling` (`SESSION_ID_POLLING`), và `fanout_scan` (`SESSION_ID_FANOUT_SCAN`). Mỗi arm bắt buộc sử dụng workspace lá mới trên Host B, thư mục `$ROOT_A/<arm>` mới trên Host A và cờ `--reference` trỏ về báo cáo kết quả của `optimized`.

---

#### Bước 11 — Hướng dẫn xử lý sự cố, Hủy bỏ (`abort`) và Dọn dẹp tài nguyên tồn đọng (`cleanup`)

Khi xảy ra sự cố trong quá trình chạy (lỗi mạng, tiến trình con bị treo, timeout hoặc lỗi xác thực), người vận hành phải tuân thủ quy trình dừng an toàn, **tuyệt đối không chạy `docker system prune` hay `docker compose down -v` bừa bãi** làm ảnh hưởng tới các volume dữ liệu khác.

##### 1. Hủy bỏ Phiên đang Hoạt động (`abort`)
Nếu server A1 vẫn đang chạy nhưng phiên bị lỗi ở một phase nào đó, gửi lệnh `abort` trên Terminal A2 (truyền đúng đường dẫn `$A` của arm đang chạy và `$SESSION_ID` tương ứng):
```sh
# Terminal A2 (Host A)
# Đặt biến $A trỏ tới thư mục fixture của arm bị lỗi (ví dụ arm optimized hoặc shaper_off):
export A="$ROOT_A/optimized/A"
export CURRENT_PHASE=$(python3 -c "import json; print(json.load(open('$A/fixture/private/session.json'))['phase'])")
export SESSION_ID="$SESSION_ID_OPTIMIZED"

python3 scripts/paper_p8_server.py abort \
  --root "$A" \
  --experiment E5 \
  --session-id "$SESSION_ID" \
  --phase "$CURRENT_PHASE"
```
Lệnh `abort` sẽ yêu cầu supervisor thu hồi tài nguyên, ghi nhận `workload_outcome: ABORTED`, thực hiện dọn dẹp Docker container và chuyển phiên về `CLOSED`. Sau đó tiến trình server A1 sẽ thoát với mã 0.

##### 2. Dọn dẹp Tài nguyên Tồn đọng Sau sự cố (`cleanup`)
Trong trường hợp tiến trình server đã bị ngắt bất thường (crash hoặc bị kill) khiến các container Docker còn sót lại:
```sh
# Terminal A2 (Host A)
export PROJECT=$(python3 -c "import json; print(json.load(open('$A/fixture/private/session.json'))['project'])")

python3 scripts/paper_p8_server.py cleanup \
  --root "$A" \
  --experiment E5 \
  --project "$PROJECT"
```

> **Biện pháp Bảo vệ Nghiêm ngặt của Lệnh Cleanup:**
> - Lệnh chỉ hoạt động khi tiến trình server A1 **đã dừng hoàn toàn** (không chạy cleanup khi server còn active).
> - Kiểm tra đối chiếu biên lai tài nguyên `resource-*.json` trong `$A/fixture/private/`. Lệnh chỉ xóa đúng các container, network và volume có biên lai xác nhận do chính phiên đó tạo ra.
> - Bảo toàn tuyệt đối mọi cơ sở dữ liệu SQLite, tệp nhật ký journal, tệp outbox pending của Gateway để phục vụ phân tích nguyên nhân lỗi (RCA).
> - Máy chủ A không có quyền can thiệp vào Máy B qua mạng; việc dọn dẹp các tiến trình con trên B phải do người vận hành xử lý cục bộ trên Máy B.

---

#### Bước 12 — So sánh đối chứng ma trận và tổng kết qua `compare` và `e5-summary`

##### 1. Đối chiếu Từng cặp Bằng CLI `compare`
Thực hiện so sánh read-only đối chiếu giữa arm tham chiếu `optimized` và từng arm bóc tách:
```sh
python3 scripts/paper_p8_comparison.py compare \
  --ref "$A_OPT/fixture/artifacts/p8-e5-optimized.json" \
  --arm-file "$ROOT_A/shaper_off/A/fixture/artifacts/p8-e5-shaper_off.json" \
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
Sau khi toàn bộ 6 arm đều hoàn tất:
```sh
export EXPORT_SUMMARY_DIR="$ROOT_A/summary-export"

python3 scripts/paper_p8_comparison.py e5-summary \
  --optimized "$A_OPT/fixture/artifacts/p8-e5-optimized.json" \
  --shaper-off "$ROOT_A/shaper_off/A/fixture/artifacts/p8-e5-shaper_off.json" \
  --microbatch-off "$ROOT_A/microbatch_off/A/fixture/artifacts/p8-e5-microbatch_off.json" \
  --ram-dedup-off "$ROOT_A/ram_dedup_off/A/fixture/artifacts/p8-e5-ram_dedup_off.json" \
  --polling "$ROOT_A/polling/A/fixture/artifacts/p8-e5-polling.json" \
  --fanout-scan "$ROOT_A/fanout_scan/A/fixture/artifacts/p8-e5-fanout_scan.json" \
  --output-dir "$EXPORT_SUMMARY_DIR"
```

Lệnh sinh ra 3 tệp tin đã được lọc sạch thông tin nhạy cảm:
- `e5-summary.json`: Báo cáo JSON tổng hợp trạng thái `PILOT_RUNS_COMPLETE_QUALIFIED`.
- `e5-runs.csv`: Số liệu chi tiết của từng arm.
- `e5-pairs.csv`: Bảng đối chiếu 5 cặp ablation so với `optimized`.

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

1. **Cơ chế gom cụm vi mô tầng lưu trữ (`microbatch_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Trong pha `initial`, với cùng 2.000 dòng mẫu dữ liệu viễn trắc (`raw_samples_retained = 2000`), arm `optimized` gom nhóm thành 16 giao dịch commit cơ sở dữ liệu (`begin_attempts = 16`, `confirmed_commits = 16`, `copy_calls = 16`), trong khi `microbatch_off` phải thực hiện 20 giao dịch commit độc lập (20 vs 16).
   - *Tỉ lệ suy giảm*: Đạt mức giảm **20.0%** số lượt mở giao dịch và thao tác COPY dữ liệu ($\Gamma_{\text{red}} = 20.0\%$).
   - *Giới hạn diễn giải*: Mức 20% này chỉ áp dụng cụ thể cho các bộ đếm thao tác import ban đầu trong cấu hình thử nghiệm, không đại diện cho toàn bộ tài nguyên SQL, dung lượng lưu trữ, RSS hay thông lượng toàn hệ thống.

2. **Cơ chế lọc trùng lặp bộ nhớ RAM (`ram_dedup_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Trong pha `warm_replay`, 10 tin nhắn phát lại đã được arm `optimized` lọc sạch hoàn toàn tại bộ nhớ RAM (`ram_lookup_hits = 10`, `ram_db_calls_avoided = 10`), dẫn đến số giao dịch và commit cơ sở dữ liệu phát sinh bằng 0 (`warm_attempts = 0`, `warm_commits = 0`, `warm_transactions = 0`). Ngược lại, `ram_dedup_off` phát sinh toàn bộ 10 giao dịch kiểm tra cơ sở dữ liệu (`warm_attempts = 10`, `warm_commits = 10`).
   - *Tỉ lệ suy giảm*: Đạt mức giảm **100.0%** tải giao dịch cơ sở dữ liệu trong pha warm replay ($\Gamma_{\text{red}} = 100.0\%$).

3. **Cơ chế điều tiết tốc độ Ingress (`shaper_off` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Arm `optimized` kích hoạt cơ chế giới hạn tốc độ token bucket khi phát sinh lưu lượng vượt ngưỡng (`shaper_rate_limited > 0`), trong khi `shaper_off` không ghi nhận sự kiện giới hạn tốc độ.

4. **Cơ chế đánh thức điều khiển hướng sự kiện (`polling` vs `optimized`):**
   - *Kích hoạt*: Đạt trạng thái **`ELIGIBLE`**.
   - *Hiệu quả ghi nhận*: Cả hai arm đều hoàn tất 4 lệnh điều khiển và cập nhật 2 cấu hình; tuy nhiên `polling` dựa trên chu kỳ thăm dò cơ sở dữ liệu định kỳ, trong khi `optimized` sử dụng kênh wakeup tức thời.

5. **Cơ chế phân phát WebSocket (`fanout_scan` vs `optimized`):**
   - *Kích hoạt*: Ghi nhận **`INCONCLUSIVE`** do nguyên nhân `fanout_viewer_readiness_unverified`.
   - *Bản chất kỹ thuật*: Arm `fanout_scan` thực hiện 40 lượt đánh giá quét (`scan_evaluations = 40`, `indexed_selections = 0`), trong khi `optimized` sử dụng 20 lượt trỏ chỉ mục trực tiếp (`indexed_selections = 20`, `scan_evaluations = 0`). Cả hai đều phân phát chính xác 20 sự kiện viễn trắc tới 2 viewers; tuy nhiên do chưa có cơ chế kiểm chứng độ sẵn sàng của viewer ở ranh giới đo lường, kết quả được đánh giá thận trọng là `INCONCLUSIVE`.

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

- **Trạng thái cổng P8**: `full_p8_accepted=false`.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm chạy trên cùng máy chủ cục bộ với metadata ghi nhận chuẩn tắc là `generator_colocated=true` (hoặc co-located network); chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng.
- **Kích hoạt Cơ chế ≠ Tăng tốc Toàn diện**: Trạng thái `ELIGIBLE` của các cặp đối chiếu chỉ chứng minh cơ chế được kích hoạt đúng theo thiết kế thí nghiệm (ví dụ: shaper có giới hạn tốc độ, microbatch có gom nhóm, RAM dedup có trúng cache, polling không nhận wakeup). Không tuyên bố mức tăng tốc thông lượng/độ trễ toàn hệ thống từ bài đo pilot đơn lẻ này.
- **Chỉ số Gamma Persistence**: Báo cáo cuối rút gọn đưa vào summary không chứa `phase_windows` với ảnh chụp quản trị/process instance mà bộ tính delta yêu cầu. Vì vậy $\Gamma_{\text{red}}$ của summary này là `UNAVAILABLE` (`process_instance_mismatch_or_missing`), không phải 0. Các artifact phase chi tiết vẫn được giữ riêng; không suy từ summary thiếu snapshot rằng mọi snapshot gốc đã mất.
- **Mẫu định danh hữu hạn**: Viewer chỉ trích xuất 2 mẫu định danh đại diện (`e5-viewer-a`, `e5-viewer-b`) để đối soát tính toàn vẹn (bounded sample provenance), không liệt kê toàn bộ 20 sự kiện.
- **Không trừ chéo đồng hồ**: Đồng hồ kiểm tra checkpoint là monotonic cục bộ của tiến trình continuous viewer holder trên Máy B; không trừ chéo đồng hồ giữa các máy khác nhau.
- **Mẫu tài nguyên là thưa**: Các số liệu CPU/RAM chỉ là mẫu định kỳ của tiến trình, không phản ánh đỉnh tải RSS hay tổng tài nguyên máy chủ.
- **Phạm vi dừng**: Dừng lại tại mốc nghiệm thu E5 pilot ma trận 6 arm; không tự động mở rộng hay chuyển sang các giai đoạn tiếp theo nếu chưa có yêu cầu mới.
