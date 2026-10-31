# P8 E5 — Quy trình thực nghiệm A/B Helper đầy đủ (Complete A/B Helper Workflow)

**Trạng thái quy trình:** Quy trình A/B helper đa pha E5 đã được hiện thực, kiểm chứng và hoàn tất thực thi cả 6 arm ma trận pilot (`optimized`, `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`) ở mức độ local helper qualification.
**Ranh giới tiêu chuẩn:** `full_p8_accepted=false`, `physical_ab=NOT RUN`, `generator_colocated=true` (hoặc co-located network). Thử nghiệm pilot đơn lẻ (seed 805) trên tập dataset nhỏ không phải kiểm thử dung lượng/soak lặp lại $\ge 3$ lần hay chứng nhận phần cứng thực tế (HIL/AM5728).

---

## 1. Phân định Vai trò và Kiến trúc 4 Terminal

Quy trình vận hành E5 yêu cầu 4 terminal độc lập (2 trên Máy chủ A và 2 trên Máy khách B) để phân tách rõ quyền hạn và duy trì 2 continuous viewers liên tục xuyên suốt các phase:

```text
[MÁY CHỦ A - Fixture Server]                    [MÁY KHÁCH B - Load Generator]
Terminal A1: foreground `serve` (hold mode)      Terminal B1: foreground `e5-viewers` (continuous holder)
Terminal A2: CLI điều phối (status, begin,       Terminal B2: CLI tác vụ (bind, preflight, run,
             finish-phase, next-phase, finalize,               export, stop-viewers)
             abort, cleanup)
```

- **MÁY A (Quyền quản trị đầy đủ):** Chạy backend Go, PostgreSQL/TimescaleDB, Mosquitto DynSec, Supabase Auth, Nginx và các collectors (`metrics.csv`, `resources.csv`). Chịu trách nhiệm tạo bundle, cấp phát tài khoản, đối soát dữ liệu và dọn dẹp Docker container.
- **MÁY B (Quyền hạn tối thiểu - Least Privilege):** Chỉ chạy nhị phân Go (`paperload`, `gateway`) và script client Python. Nhận JWT người dùng và mật khẩu Gateway; tuyệt đối **không nhận** Docker socket, DB URL quản trị, service-role key hay quyền SSH quản trị/điều khiển Máy A. SCP/SFTP bên dưới là thao tác truyền tệp của operator, không phải control-plane của client; có thể dùng tài khoản chỉ được truyền tệp hoặc máy quản trị riêng.

> **Quy tắc an toàn shell & quyền hạn:**
> - Mọi biến shell phải được khởi tạo tường minh trên từng terminal/host; các biến **không tự động chia sẻ** giữa các máy hay terminal khác nhau.
> - Mọi thư mục scratch phải dùng quyền `0700` và file cấu hình/chứng chỉ/kết quả dùng quyền `0600`.
> - Tuyệt đối không hardcode mật khẩu, token JWT, địa chỉ IP mạng nội bộ hay đường dẫn cá nhân trong các mẫu lệnh.

---

## 2. Phía Máy chủ A: Khởi tạo Fixture và Export Portable Bundle

### 2.1. Khởi tạo Context và Dataset Dùng chung cho Ma trận
Mỗi ma trận kiểm thử mới bắt đầu bằng việc sinh một context và dataset dùng chung cho toàn bộ các arm trong repeat đó:
```sh
# Chạy một lần duy nhất khi bắt đầu ma trận (trên Máy A hoặc máy quản trị):
export INPUTS_DIR="/var/tmp/opencode/p8-e5-inputs"
mkdir -m 700 -p "$INPUTS_DIR" && chmod 700 "$INPUTS_DIR"

python3 scripts/paper_p8_e5.py --root "$INPUTS_DIR" --seed 805 --prepare-only
# Sinh ra context tại: $INPUTS_DIR/repeat-01/context.json
```

> **Lưu ý về Dấu vết Thời gian & Giá trị Băm:**
> - Lệnh `--prepare-only` sử dụng mốc thời gian thực hiện (`anchor = datetime.now()`) để định danh tập dữ liệu. Do đó, việc chạy lại lệnh này ở các thời điểm khác nhau sẽ tạo ra chuỗi băm dataset khác nhau kể cả khi dùng cùng hạt giống `--seed 805`.
> - Giá trị băm ghi nhận trong đợt chạy trước (SHA-256 `2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175`, 35.684 bytes) là **bằng chứng lịch sử của đợt đo cụ thể**, không phải kết quả bắt buộc của mọi lần sinh mới.
> - Trong một đợt đo, sau khi sinh context, **phải ghim và giữ nguyên vẹn file context đó cho toàn bộ 6 arm**, tuyệt đối không sinh lại giữa các arm.
> - Chuẩn bị checkout và binary trước lượt optimized, rồi **không sửa source, docs, tests hoặc file source untracked** cho tới khi chạy xong mọi arm. Reference kiểm tra cả dirty-source fingerprint; báo cáo lịch sử không tự động dùng được làm reference của checkout hiện tại sau các sửa đổi reporting/tài liệu.
> - Mỗi ma trận mới tạo reference optimized riêng. Chỉ chạy arm tiếp theo sau khi server/holder của arm trước đã dừng và cleanup được xác minh; mỗi arm dùng workspace/export lá mới trên B. Thứ tự ablation đã kiểm chứng với seed 805: `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`.

Máy A cần Docker/Compose, Python, Go và OpenSSL; Máy B chỉ cần Python và hai binary Go tương thích. Xem [profile mạng và lifecycle](paper-p8-remote-lifecycle.md) để chuẩn bị file mạng private IPv4 và CA/SAN trước khi chạy; không tắt TLS verification hoặc dùng credentials deployment.

### 2.2. Terminal A1: Khởi chạy Server Fixture ở chế độ Hold (Foreground)
Thiết lập thư mục riêng biệt cho từng arm. Thư mục `$ROOT_A/A` **phải chưa tồn tại** trước khi chạy lệnh (chỉ tạo thư mục cha `$ROOT_A` với quyền `0700`):

- **Với arm tham chiếu (`optimized`):**
```sh
# Terminal A1 (Host A)
export ROOT_A="/var/tmp/opencode/p8-e5-optimized-run"
export A="$ROOT_A/A"
export NET_A="/path/to/protected/network.json"
export CTX_A="/var/tmp/opencode/p8-e5-inputs/repeat-01/context.json"

mkdir -m 700 -p "$ROOT_A" && chmod 700 "$ROOT_A"
python3 scripts/paper_p8_server.py serve \
  --root "$A" \
  --experiment E5 \
  --arm optimized \
  --context "$CTX_A" \
  --network-config "$NET_A" \
  --deadline-seconds 900
```

- **Với các arm bóc tách (`ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`):**
Bắt buộc truyền tham số `--reference` trỏ tới file báo cáo `$ART/p8-e5-optimized.json` đã đạt `PASS` của arm optimized để tái sử dụng chính xác 100% container image IDs (byte-for-byte, không build lại image):
```sh
# Terminal A1 (Host A - Ví dụ cho shaper_off)
export ROOT_A="/var/tmp/opencode/p8-e5-shaper_off-run"
export A="$ROOT_A/A"
export NET_A="/path/to/protected/network.json"
export CTX_A="/var/tmp/opencode/p8-e5-inputs/repeat-01/context.json"
export REF_A="/var/tmp/opencode/p8-e5-optimized-run/A/fixture/artifacts/p8-e5-optimized.json"

mkdir -m 700 -p "$ROOT_A" && chmod 700 "$ROOT_A"
python3 scripts/paper_p8_server.py serve \
  --root "$A" \
  --experiment E5 \
  --arm shaper_off \
  --context "$CTX_A" \
  --reference "$REF_A" \
  --network-config "$NET_A" \
  --deadline-seconds 900
```

> **Cảnh báo về Hạn ngạch Thời gian (Deadline 900s):**
> Tiến trình `serve` sẽ tự động thoát và hủy bỏ phiên nếu toàn bộ quy trình không hoàn tất trong vòng 900 giây. Người vận hành phải theo dõi sát sao tiến trình foreground A1.

### 2.3. Terminal A2: Kiểm tra Trạng thái Sẵn sàng của Máy chủ
```sh
# Terminal A2 (Host A)
export A="/var/tmp/opencode/p8-e5-optimized-run/A"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E5
```
Đợi cho đến khi JSON in ra:
```json
{
  "phase": "READY",
  "preparation_status": "E5_PREPARED",
  "collector_ready": null,
  "workload_outcome": "NOT_RUN"
}
```
*(Ghi chú: Tại trạng thái ban đầu `READY`, `collector_ready` bằng `null` là đúng thiết kế; collector chỉ kích hoạt và báo `true` sau khi A2 gửi lệnh `begin` của phase tương ứng).*

Lấy `session_id` để sử dụng cho các lệnh điều khiển tiếp theo:
```sh
export SID=$(python3 -c "import json; print(json.load(open('$A/fixture/private/session.json'))['session_id'])")
```

---

## 3. Phía Máy khách B: Cài đặt Workspace và Chạy Continuous Viewers

### 3.1. Chuẩn bị Nhị phân và Chuyển giao Bundle (A -> B)
1. **Chuẩn bị 2 file nhị phân trên Máy B (`paperload` và `gateway`):**
   - Nếu Máy A và Máy B tương thích ABI (cùng Linux OS/architecture), có thể sao chép trực tiếp từ `$A/fixture/private/bin/` sang thư mục nhị phân trên B.
   - Nếu khác môi trường, biên dịch từ cùng mã nguồn checkout trên B:
     ```sh
     # Trên Máy B (từ thư mục mã nguồn tương ứng)
     export BIN_DIR="/var/tmp/opencode/p8-e5-b/bin"
     mkdir -m 700 -p "$BIN_DIR" && chmod 700 "$BIN_DIR"
     (cd src && CGO_ENABLED=0 go build -o "$BIN_DIR/paperload" ./cmd/paperload)
     (cd src && CGO_ENABLED=0 go build -o "$BIN_DIR/gateway" ./cmd/gateway)
     chmod 700 "$BIN_DIR/paperload" "$BIN_DIR/gateway"
     ```
   *(Lưu ý: Các chuỗi băm nhị phân lịch sử như `f562ba9b...` và `3be849df...` là bằng chứng của bản build cụ thể trên máy đo mẫu, không phải giá trị băm bắt buộc của mọi nền tảng khác nhau. Tuy nhiên, trong cùng một ma trận, 2 file nhị phân này phải giữ cố định không đổi xuyên suốt cả 6 arm).*

2. **Chuyển bundle từ A sang B:**
   Người vận hành sao chép toàn bộ thư mục `$A/client-bundle` sang `$BUNDLE_DEST` trên Máy B (qua công cụ truyền tệp an toàn như `scp` hoặc `rsync` có mã hóa; bảo đảm thư mục đích có quyền `0700` và các file có quyền `0600`).

### 3.2. Terminal B2: Ràng buộc Workspace và Kiểm tra Sẵn sàng
Mỗi arm kiểm thử phải sử dụng một thư mục workspace lá **mới hoàn toàn** trên Máy B (chỉ tạo thư mục cha với quyền `0700`, **không được `mkdir` thư mục lá workspace** trước khi chạy `bind`):

```sh
# Terminal B2 (Host B - Thiết lập riêng cho từng arm)
export B_ROOT="/var/tmp/opencode/p8-e5-b"
export WORKSPACE="$B_ROOT/ws-optimized"   # Thư mục lá này CHƯA TỒN TẠI
export BUNDLE="/path/to/transferred/client-bundle"
export BIN_PL="$B_ROOT/bin/paperload"
export BIN_GW="$B_ROOT/bin/gateway"
# Operator điền đích truyền tệp và đường dẫn TUYỆT ĐỐI trên A, đúng arm hiện hành:
export A_TRANSFER_TARGET="operator@host-a"
export A_INCOMING_REMOTE="/var/tmp/opencode/p8-e5-optimized-run/A/fixture/private/incoming"

# 1. Tạo thư mục cha bảo vệ (0700)
mkdir -m 700 -p "$B_ROOT" && chmod 700 "$B_ROOT"

# 2. Ràng buộc workspace (Dataset được tạo liên kết/sao chép với mode 0600 cho Go reader)
python3 scripts/paper_p8_client.py bind --bundle "$BUNDLE" --workspace "$WORKSPACE"

# 3. Kiểm tra preflight offline (cần cả hai nhị phân; không ghi tệp output, không mở mạng)
python3 scripts/paper_p8_client.py preflight \
  --workspace "$WORKSPACE" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW"

# 4. Kiểm tra kết nối mạng (chỉ nhận tham số --workspace; kiểm tra HTTPS, WS handshake, MQTT TLS)
python3 scripts/paper_p8_client.py check-connection --workspace "$WORKSPACE"
```

### 3.3. Terminal B1: Khởi chạy Continuous Viewer Holder (Foreground)
Khởi động tiến trình duy trì 2 viewer liên tục xuyên suốt 3 pha:
```sh
# Terminal B1 (Host B)
export WORKSPACE="/var/tmp/opencode/p8-e5-b/ws-optimized"
export BIN_PL="/var/tmp/opencode/p8-e5-b/bin/paperload"
export BIN_GW="/var/tmp/opencode/p8-e5-b/bin/gateway"

python3 scripts/paper_p8_client.py e5-viewers \
  --workspace "$WORKSPACE" \
  --binary "$BIN_PL" \
  --gateway-binary "$BIN_GW" \
  --deadline-seconds 900
```
Tiến trình B1 sẽ chạy foreground, chiếm giữ khóa `viewer-holder.lock`, xác nhận trạng thái `READY`, và duy trì đồng hồ monotonic địa phương duy nhất để ghi nhận chính xác 6 checkpoint.

---

## 4. Trình tự Điều phối Tuần tự 3 Pha (`initial` → `warm_replay` → `control`)

Quy trình thực thi bắt buộc phải phối hợp chặt chẽ theo từng bước giữa Terminal A2 và Terminal B2. **Không chạy gộp tất cả các phase cùng lúc.**

```text
  Host A (Terminal A2)                      Host B (Terminal B2)
  --------------------                      --------------------
  [1] begin --phase-id <P>
      (đợi MEASURING & collector_ready)
                                            [2] run --phase <P>
                                            [3] (nếu control: e5-stop-viewers)
                                            [4] export-results --phase <P>
  [5] nhận results.json từ B
  [6] finish-phase --results <file>
      (đợi VERIFYING & VALIDATED)
  [7] next-phase --phase-id <P_next>
      (đợi READY cho pha kế tiếp)
```

### 4.1. Pha 1: `initial` (Replay Telemetry 20 tin nhắn đầu vào)
1. **Trên A (Terminal A2):** Bắt đầu pha `initial`:
   ```sh
   python3 scripts/paper_p8_server.py begin --root "$A" --experiment E5 --session-id "$SID" --phase READY --phase-id initial
   # Kiểm tra: python3 scripts/paper_p8_server.py status --root "$A" --experiment E5
   # Đợi: phase=MEASURING, phase_id=initial, collector_ready=true
   ```
2. **Trên B (Terminal B2):** Chạy và xuất kết quả pha `initial`:
   ```sh
   export EXP_INITIAL="/var/tmp/opencode/p8-e5-b/export-initial-optimized"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE" --binary "$BIN_PL" --phase initial
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE" --phase initial --output "$EXP_INITIAL"
   ```
3. **Chuyển giao tệp từ B sang A (Manual Transfer):**
   - Trên Máy A (Terminal A2), tạo trước thư mục incoming với quyền `0700`:
     ```sh
     export INCOMING="$A/fixture/private/incoming"
     mkdir -m 700 "$INCOMING" && chmod 700 "$INCOMING"
     mkdir -m 700 "$INCOMING/initial" && chmod 700 "$INCOMING/initial"
     ```
   - **Trên B2**, sau khi A2 tạo thư mục đích, operator chuyển hai tệp bằng các biến đã khai báo trên B2. `$A_INCOMING_REMOTE` là chuỗi đường dẫn trên A, không phải thư mục trên B. Nếu truyền từ máy quản trị, cần đưa hai tệp tới máy đó và đặt lại các biến nguồn/đích tại chính terminal truyền tệp:
      ```sh
      # Terminal B2 — chỉ truyền tệp, không thực thi lệnh điều khiển trên A:
      scp "$EXP_INITIAL/results.json" "$EXP_INITIAL/resources-generator.csv" "$A_TRANSFER_TARGET:$A_INCOMING_REMOTE/initial/"
      ```
   - **Trên A2**, xác lập quyền cho các tệp đã nhận:
      ```sh
      chmod 600 "$INCOMING/initial"/*
      ```
4. **Trên A (Terminal A2):** Thẩm định kết quả và mở pha tiếp theo:
   ```sh
   # Tham số --results phải chỉ định rõ tệp results.json, không truyền thư mục:
   python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E5 --session-id "$SID" --phase MEASURING --phase-id initial --results "$INCOMING/initial/results.json"
   # Đợi status: phase=VERIFYING, workload_outcome=VALIDATED, phase_history=["initial"]

   python3 scripts/paper_p8_server.py next-phase --root "$A" --experiment E5 --session-id "$SID" --phase VERIFYING --phase-id warm_replay
   # Đợi status: phase=READY, phase_id=warm_replay
   ```

### 4.2. Pha 2: `warm_replay` (Replay Telemetry 10 tin nhắn kiểm tra lọc trùng)
1. **Trên A (Terminal A2):** Bắt đầu pha `warm_replay`:
   ```sh
   python3 scripts/paper_p8_server.py begin --root "$A" --experiment E5 --session-id "$SID" --phase READY --phase-id warm_replay
   # Đợi: phase=MEASURING, phase_id=warm_replay, collector_ready=true
   ```
2. **Trên B (Terminal B2):** Chạy và xuất kết quả pha `warm_replay`:
   ```sh
   export EXP_WARM="/var/tmp/opencode/p8-e5-b/export-warm-optimized"
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE" --binary "$BIN_PL" --phase warm_replay
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE" --phase warm_replay --output "$EXP_WARM"
   ```
3. **Chuyển giao tệp từ B sang A:**
    ```sh
    # Terminal A2 — tạo thư mục đích trước khi operator truyền tệp:
    mkdir -m 700 "$INCOMING/warm_replay" && chmod 700 "$INCOMING/warm_replay"
    ```
    ```sh
    # Terminal B2 — dùng các biến nguồn và đích của B2:
    scp "$EXP_WARM/results.json" "$EXP_WARM/resources-generator.csv" "$A_TRANSFER_TARGET:$A_INCOMING_REMOTE/warm_replay/"
    ```
    ```sh
    # Terminal A2 — sau khi truyền xong:
    chmod 600 "$INCOMING/warm_replay"/*
    ```
4. **Trên A (Terminal A2):** Thẩm định kết quả và mở pha tiếp theo:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E5 --session-id "$SID" --phase MEASURING --phase-id warm_replay --results "$INCOMING/warm_replay/results.json"
   # Đợi status: phase=VERIFYING, workload_outcome=VALIDATED, phase_history=["initial", "warm_replay"]

   python3 scripts/paper_p8_server.py next-phase --root "$A" --experiment E5 --session-id "$SID" --phase VERIFYING --phase-id control
   # Đợi status: phase=READY, phase_id=control
   ```

### 4.3. Pha 3: `control` (Thực thi 4 lệnh điều khiển bền vững & Dừng Viewer)
1. **Trên A (Terminal A2):** Bắt đầu pha `control`:
   ```sh
   python3 scripts/paper_p8_server.py begin --root "$A" --experiment E5 --session-id "$SID" --phase READY --phase-id control
   # Đợi: phase=MEASURING, phase_id=control, collector_ready=true
   ```
2. **Trên B (Terminal B2):** Thực thi lệnh điều khiển, dừng viewer và xuất kết quả:
   ```sh
   export EXP_CONTROL="/var/tmp/opencode/p8-e5-b/export-control-optimized"

   # Bước 1: Chạy 4 lệnh điều khiển và 2 durable gateway executors (chạy foreground):
   # (Ghi chú: Nếu có hàng tồn outbox chưa ACK, hàm _complete_e3_poststop_drain sẽ tự động
   # xả hàng tồn và đọc lại tươi bằng CLI gateway count ngay trong bước run này trước khi kết thúc)
   python3 scripts/paper_p8_client.py run --workspace "$WORKSPACE" --binary "$BIN_PL" --gateway-binary "$BIN_GW" --phase control

   # Bước 2: Chỉ gửi yêu cầu dừng viewer SAU KHI lệnh run control đã hoàn tất hoàn toàn:
   python3 scripts/paper_p8_client.py e5-stop-viewers --workspace "$WORKSPACE"
   # Quan sát: Tiến trình Terminal B1 thoát êm ái với exit code 0 (planned_stop=true)

   # Bước 3: Xuất kết quả control (chỉ kiểm tra và đóng gói bằng chứng tĩnh đã xả sạch):
   python3 scripts/paper_p8_client.py export-results --workspace "$WORKSPACE" --phase control --output "$EXP_CONTROL"
   ```
3. **Chuyển giao tệp từ B sang A:**
    ```sh
    # Terminal A2 — tạo thư mục đích trước khi operator truyền tệp:
    mkdir -m 700 "$INCOMING/control" && chmod 700 "$INCOMING/control"
    ```
    ```sh
    # Terminal B2:
    scp "$EXP_CONTROL/results.json" "$EXP_CONTROL/resources-generator.csv" "$A_TRANSFER_TARGET:$A_INCOMING_REMOTE/control/"
    ```
    ```sh
    # Terminal A2 — sau khi truyền xong:
    chmod 600 "$INCOMING/control"/*
    ```
4. **Trên A (Terminal A2):** Thẩm định pha control (không gọi `next-phase` cho pha cuối cùng):
   ```sh
   python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E5 --session-id "$SID" --phase MEASURING --phase-id control --results "$INCOMING/control/results.json"
   # Đợi status: phase=VERIFYING, workload_outcome=VALIDATED, phase_history=["initial", "warm_replay", "control"]
   ```

---

## 5. Đóng Phiên, Finalize và Đánh giá Nghiệm thu

### 5.1. Terminal A2: Gửi lệnh Finalize
Khi pha `control` đã đạt trạng thái `VERIFYING` và `workload_outcome: VALIDATED`:
```sh
# Terminal A2 (Host A)
python3 scripts/paper_p8_server.py finalize --root "$A" --experiment E5 --session-id "$SID" --phase VERIFYING
```

### 5.2. Quan sát và Đọc kết quả Đích thực
1. **Quan sát Terminal A1:** Tiến trình server A1 hoàn tất đối chiếu, dọn dẹp sạch chính xác 12 containers / 1 network / 5 volumes sở hữu và thoát với mã `0`. **Tuyệt đối không coi việc gửi lệnh finalize là hoàn tất; phải đợi tiến trình A1 thoát hẳn với mã 0.**
2. **Kiểm tra trạng thái session `$A/fixture/private/session.json`:**
   Phải đạt đầy đủ 4 điều kiện:
   - `phase`: `"CLOSED"`
   - `workload_outcome`: `"VALIDATED"`
   - `cleanup_outcome`: `"VERIFIED"`
   - `phase_history`: `["initial", "warm_replay", "control"]`
   *(Lưu ý: Trạng thái `CLOSED` và `cleanup_outcome: VERIFIED` chỉ chứng minh việc dọn dẹp môi trường thành công, không thay thế cho việc đánh giá `workload_outcome: VALIDATED` trên báo cáo artifact).*
3. **Đánh giá Báo cáo Sản xuất `$A/fixture/artifacts/p8-e5-<arm>.json`:**
   Báo cáo hợp lệ phải thỏa mãn đồng thời:
   - `status`: `"PASS"`
   - `workload_status`: `"WORKLOAD_COMPLETE_PENDING_CLEANUP"`
   - `cleanup_verified`: `true`
   - `validation.valid`: `true` (và `validation.failures` rỗng)
   - `processes_stopped`: `true` và `runner_stopped`: `true`
   *(Lưu ý: `paper_p8_e5_acceptance` là một hàm logic kiểm tra trong mã Python, không phải trường chuỗi lưu trữ trong tệp JSON).*

---

## 6. Hướng dẫn Hủy bỏ (Abort) và Dọn dẹp Tài nguyên Tồn đọng (Stale Cleanup)

Khi xảy ra sự cố trong quá trình chạy (lỗi mạng, tiến trình con bị treo, timeout hoặc lỗi xác thực), người vận hành phải tuân thủ quy trình dừng an toàn, **tuyệt đối không chạy `docker system prune` hay `docker compose down -v` bừa bãi** làm ảnh hưởng tới các volume dữ liệu khác.

### 6.1. Hủy bỏ Phiên đang Hoạt động (`abort`)
Nếu server A1 vẫn đang chạy nhưng phiên bị lỗi ở một phase nào đó, gửi lệnh `abort` trên Terminal A2:
```sh
# Terminal A2 (Host A)
export CURRENT_PHASE=$(python3 -c "import json; print(json.load(open('$A/fixture/private/session.json'))['phase'])")

python3 scripts/paper_p8_server.py abort \
  --root "$A" \
  --experiment E5 \
  --session-id "$SID" \
  --phase "$CURRENT_PHASE"
```
Lệnh `abort` sẽ yêu cầu supervisor thu hồi tài nguyên, ghi nhận `workload_outcome: FAILED`, thực hiện dọn dẹp Docker container và chuyển phiên về `CLOSED`. Sau đó tiến trình server A1 sẽ thoát.

### 6.2. Dọn dẹp Tài nguyên Tồn đọng Sau sự cố (`cleanup`)
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

Trên B, nếu chưa tới `control.end`, không dùng `e5-stop-viewers` để giả lập hoàn tất: lệnh planned stop yêu cầu đủ sáu checkpoint. Operator có thể ngắt tiến trình foreground B1/B2 bằng Ctrl-C để đi qua nhánh lỗi/dừng có giới hạn; giữ nguyên log và SQLite, không coi lần dừng này là PASS. Khi mất liên lạc, A chỉ có thể thu hồi fixture của A sau abort/deadline; không chứng minh B đã dừng nếu chưa có xác nhận cục bộ.

---

## 7. So sánh Đối chiếu Cặp và Xuất Báo cáo Tổng kết

### 7.1. Đối chiếu Từng cặp Bằng CLI `compare`
```sh
python3 scripts/paper_p8_comparison.py compare \
  --ref /path/to/p8-e5-optimized.json \
  --arm-file /path/to/p8-e5-shaper_off.json \
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

### 7.2. Xuất Báo cáo Tổng kết Ma trận Bằng CLI `e5-summary`
Sau khi cả 6 arm đều hoàn tất:
```sh
export EXPORT_SUMMARY_DIR="/var/tmp/opencode/p8-e5-summary-export"

python3 scripts/paper_p8_comparison.py e5-summary \
  --optimized /path/to/p8-e5-optimized.json \
  --shaper-off /path/to/p8-e5-shaper_off.json \
  --microbatch-off /path/to/p8-e5-microbatch_off.json \
  --ram-dedup-off /path/to/p8-e5-ram_dedup_off.json \
  --polling /path/to/p8-e5-polling.json \
  --fanout-scan /path/to/p8-e5-fanout_scan.json \
  --output-dir "$EXPORT_SUMMARY_DIR"
```
*(Tùy chọn: nếu có chuỗi băm context riêng biệt của đợt đo, có thể bổ sung `--private-context <context_sha256>`).*

Lệnh sinh ra 3 tệp tin đã được lọc sạch thông tin nhạy cảm:
- `e5-summary.json`: Báo cáo JSON tổng hợp trạng thái `PILOT_RUNS_COMPLETE_QUALIFIED`.
- `e5-runs.csv`: Số liệu chi tiết của từng arm.
- `e5-pairs.csv`: Bảng đối chiếu 5 cặp ablation so với `optimized`.

---

## 8. Xử lý Sự cố & Lỗi Thường gặp

| Mã lỗi / Tình huống | Nguyên nhân gốc | Hướng xử lý an toàn |
|---|---|---|
| `e5_reference_required` | Chạy ablation arm mà quên truyền `--reference` | Bắt buộc truyền `--reference` trỏ tới báo cáo `p8-e5-optimized.json` đã đạt `PASS`. |
| `FileExistsError` khi `serve` hoặc `bind` | Thư mục lá `$ROOT_A/A` hoặc `$WORKSPACE` đã tồn tại từ trước | Chỉ tạo thư mục cha; đặt biến thư mục lá sang tên mới hoàn toàn (chưa tạo trước). |
| `runner_drain_unconfirmed_control` | Gateway simulator còn hàng tồn outbox chưa ACK và xả không hết | Không sửa file thủ công hay chạy lại export; kiểm tra log tiến trình simulator xem có bị ngắt giữa chừng không, bảo tồn tệp SQLite pending, sửa lỗi và chạy lại một lượt mới. |
| `missing_fingerprint_gateway_binary_sha256` hoặc `..._paperload_...` | Thiếu một trong hai hash nhị phân thành phần trên B | Biên dịch và cung cấp đầy đủ cả 2 file nhị phân `paperload` và `gateway` trên Máy B trước khi preflight. |
| `window_duration_mismatch` | Cửa sổ khai báo cấu hình (`declared_measurement_windows`) bị sai lệch | Giữ nguyên context và cấu hình thời gian khai báo bất biến giữa các arm; biến thiên mili-giây của thời gian đo thực tế (`observed`) được chấp nhận hợp lệ. |
| `invalid_fingerprint_phase_seconds` | Cửa sổ khai báo có chứa boolean `True`, giá trị âm, hoặc thiếu pha | Cửa sổ khai báo bắt buộc phải là số thực hữu hạn dương và đủ 3 pha: `initial`, `warm_replay`, `control`. |

---

## 9. Ranh giới Giới hạn Bắt buộc

1. **Ranh giới tiêu chuẩn P8:** Mọi kết quả E5 luôn ghi nhận `full_p8_accepted: false` và `physical_ab: NOT RUN`.
2. **Kích hoạt Cơ chế ≠ Tăng tốc Toàn diện:** Trạng thái `ELIGIBLE` của các cặp đối chiếu chỉ chứng minh cơ chế được kích hoạt đúng theo thiết kế thí nghiệm (ví dụ: shaper có giới hạn tốc độ, microbatch có gom nhóm, RAM dedup có trúng cache, polling không nhận wakeup). Không tuyên bố mức tăng tốc thông lượng/độ trễ toàn hệ thống từ bài đo pilot đơn lẻ này.
3. **Chỉ số Gamma Persistence:** Report cuối rút gọn đưa vào summary không chứa `phase_windows` với ảnh chụp quản trị/process instance mà bộ tính delta yêu cầu. Vì vậy $\Gamma_{\text{red}}$ của summary này là `UNAVAILABLE` (`process_instance_mismatch_or_missing`), không phải 0. Các artifact phase chi tiết vẫn được giữ riêng; không suy từ summary thiếu snapshot rằng mọi snapshot gốc đã mất.
4. **Mẫu định danh hữu hạn:** Viewer chỉ trích xuất 2 mẫu định danh đại diện (`e5-viewer-a`, `e5-viewer-b`) để đối soát tính toàn vẹn (bounded sample provenance), không liệt kê toàn bộ 20 sự kiện.
5. **Không trừ chéo đồng hồ:** Đồng hồ kiểm tra checkpoint là monotonic cục bộ của tiến trình continuous viewer holder trên Máy B; không trừ chéo đồng hồ giữa các máy khác nhau.
6. **Mẫu tài nguyên là thưa:** Các số liệu CPU/RAM chỉ là mẫu định kỳ của tiến trình, không phản ánh đỉnh tải RSS hay tổng tài nguyên máy chủ.
