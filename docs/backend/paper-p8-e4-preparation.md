# P8 E4 — helper A/B pilot: UNIQUE → WARM REPLAY → tùy chọn COLD hoặc TTL EXPIRY (source-qualified)

> TTL 600s chỉ là optional same-backend-process phase sau commit-age xác nhận,
> không ghép cold/durable/conflict; wall-clock TTL native vẫn NOT RUN. Đừng chờ
> TTL trong pilot thông thường hay rút ngắn production TTL. Kết quả từng lượt
> nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e4--kiểm-định-counts-và-replay).

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E4 (kiểm định tính bất biến dữ liệu, khử trùng lặp và xác thực bộ đệm qua các pha tuần tự UNIQUE → WARM REPLAY → tùy chọn COLD REPLAY). E4 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST, WebSocket telemetry và Mosquitto TLS).

Thử nghiệm chạy tuần tự các phase trong cùng một session fixture; từng phase được kiểm định dữ liệu và đối chiếu snapshot cơ sở dữ liệu độc lập. `--expiry-replay` là phase tùy chọn riêng, không kết hợp với `--cold-replay`, durable reopen hoặc conflicts. Nó chờ đủ TTL production 600 giây sau commit được A xác nhận, giữ nguyên backend process và journal; không giảm TTL hoặc seed cache. Lệnh dry-run kiểm tra 1000+100 mà không chờ. Native wall-clock expiry hiện **NOT RUN**; không coi cấu hình/dry-run là bằng chứng TTL thực.

---

## 1. Tổng quan và đặc thù quy trình đa pha E4

Thử nghiệm E4 thực hiện kiểm chứng khả năng khử trùng lặp dữ liệu viễn trắc qua các pha chạy nối tiếp trên cùng một phiên làm việc (`--experiment E4`):

### 1.1 Phân định các Phase trong E4

| Đặc tính | UNIQUE (Bắt buộc) | WARM REPLAY (Bắt buộc) | COLD REPLAY (Tùy chọn) | TTL EXPIRY (Tùy chọn, loại trừ cold) |
|---|---|---|---|---|
| **Mục đích** | Ghi nhận dữ liệu gốc lần đầu | Phát lại tập con dữ liệu khi backend đang chạy | Phát lại sau khi backend bị dừng và khởi động mới | Phát lại sau khi khóa RAM hết hạn TTL 600s trên cùng tiến trình backend |
| **Kích hoạt** | Đầu tiên (`--phase-id unique`) | Sau UNIQUE (`--phase-id replay`) | Yêu cầu `serve --cold-replay` (`--phase-id cold-replay`) | Yêu cầu `serve --expiry-replay` (`--phase-id expiry-replay`), sau replay |
| **Tác động Database** | Tăng đúng số mẫu: $U$ ledger, $U \times 100$ raw, $U$ temporal (pilot: 20/2.000/20/0; official: 1.000/100.000/1.000/0) | DB delta giữ nguyên 0 (zero new rows, zero delta across ledger/raw/temporal/config) | DB delta giữ nguyên 0 (zero new rows, zero delta across ledger/raw/temporal/config) | DB delta giữ nguyên 0 (zero new rows, zero delta across ledger/raw/temporal/config) |
| **Hành vi Bộ đệm** | Ghi nhận khóa dedup vào RAM | Cache hit khi còn TTL (tránh gọi repository) | Cache miss sau process replacement (RAM rỗng); DB transaction xử lý duplicate | Cache miss do hết hạn TTL 600s; DB transaction xử lý duplicate trên cùng tiến trình |
| **WebSocket Events** | Phát đúng $U$ sự kiện viễn trắc tới viewers (pilot: 20; official: 1.000) | 0 sự kiện phát mới tới viewers | 0 sự kiện phát mới tới viewers | 0 sự kiện phát mới tới viewers |
| **Journal SQLite** | Khởi tạo và ghi nhận $U$ records gốc vào `outputs/e4-journal.sqlite` | Tái sử dụng cùng journal; đọc chính xác $R$ records đầu (pilot: 10; official: 100) | Tái sử dụng cùng journal; đọc chính xác $R$ records đầu (SHA-256 bất biến) | Tái sử dụng cùng journal; đọc chính xác $R$ records đầu (SHA-256 bất biến) |

### 1.2 Cơ chế dùng chung SQLite Journal (`outputs/e4-journal.sqlite`)

- **Mutable State thuộc quyền Host B**: Trong client bundle, các manifest bất biến (`unique`, `replay`, cùng `cold-replay` hoặc `expiry-replay` nếu có) chia sẻ chung tham chiếu đường dẫn journal SQLite tại `outputs/e4-journal.sqlite`.
- **Bảo toàn chuỗi dữ liệu (Journal Continuity)**:
  - Ở pha UNIQUE, client tạo journal và ghi lại toàn bộ thông tin của $U$ messages (headers, payload hash, sequence, timestamp, message_id). Mặc định pilot là $U=20$; ở official offline là $U=1.000$ (giới hạn pilot $U \le 256$, $U$ bắt buộc là số chẵn từ 2 đến 10.000 để chia đều cho 2 Sensors).
  - Ở các pha phát lại (WARM REPLAY, COLD REPLAY, TTL EXPIRY), client không tạo dữ liệu mới, không sinh UUID hay timestamp mới. Client đọc chính xác tiền tố $R$ bản ghi đầu tiên (`replay_count=R`, mặc định pilot $R=10$; official offline $R=100$; yêu cầu $1 \le R \le U$) từ journal đã ghi ở pha UNIQUE.
  - Mã băm SHA-256 của file `outputs/e4-journal.sqlite` bắt buộc phải bất biến tuyệt đối giữa các pha chạy. Bất kỳ sự thay đổi mã băm nào đều bị validator từ chối.

### 1.3 Phân biệt Host A (Server) và Host B (Client)

Cần phân biệt rõ ranh giới hai máy trong mô hình vận hành:
- **Host A (Server Fixture)**: Là máy chủ nắm giữ toàn bộ hạ tầng backend (Go modular monolith, TimescaleDB, Mosquitto, Nginx), chịu trách nhiệm sinh bundle, kiểm soát vòng đời session, thu thập metrics và xác thực snapshot cơ sở dữ liệu.
- **Host B (Workload Generator)**: Là máy trạm nhận client bundle, binary thực thi (`paperload`, `gateway`), thực hiện liên kết workspace, kích hoạt tải phát viễn trắc và kết nối WebSocket viewer.
- **Giới hạn máy đo**: Hiện tại các đợt chạy kiểm chuẩn được thực hiện trên cùng máy chủ cục bộ (`generator_colocated=true`). Chưa chứng minh môi trường hai máy vật lý độc lập qua mạng vật lý (`physical_ab=NOT RUN`).

### 1.4 Định nghĩa chỉ số Γ_red và kiểm định raw retention độc lập

Cho từng counter $C$ cụ thể (ví dụ: transaction begin/commit, COPY calls, repository calls), chỉ số giảm thiểu được định nghĩa:

```text
Γ_red(C) = 100 × (C_reference − C_optimized) / C_reference
```

Trong đó:
- `C_reference` là counter delta thực của nhánh reference trong đúng cửa sổ đo (window).
- `C_optimized` là delta cùng định nghĩa từ nhánh optimized.
- **Ràng buộc so sánh**: Cả hai nhánh phải cùng process instance, cùng khối lượng workload/dataset/window và cùng đơn vị đo lường. Nếu số lượng unique workload khác nhau, chỉ được so sánh sau khi đã chuẩn hóa theo cùng số lượng unique messages.
- **Điều kiện trung thực ghi `unavailable`**: Mẫu số (`C_reference`) bằng 0, counter bị thiếu/reset, lệch tiến trình (process mismatch), lệch cửa sổ (window mismatch), hoặc thiếu denominator đều phải ghi nhận rõ là `unavailable` (tuyệt đối không được gán 0% giả tạo).
- **Ranh giới kế toán**: Tuyệt đối không dùng số dòng ledger thay thế cho transaction count, không gộp các chỉ số counter-specific $\Gamma$ thành tuyên bố chung "giảm toàn bộ SQL/storage".
- **Kiểm định raw retention độc lập**: Tính bảo toàn dữ liệu viễn trắc thô được kiểm chứng độc lập thông qua số lượng raw samples ($U \times 100$ mẫu, 100 samples/message), mã băm MD5/fingerprint dữ liệu thô và bằng chứng xuất xứ/nội dung (provenance evidence), không suy diễn từ số dòng ledger.
- **Hiện trạng**: Chưa có denominator E4 official trong cùng cửa sổ đo được qualification; do đó hiện tại chưa có giá trị $\Gamma_{\text{red}}$ headline chính thức cho E4. Không cần ép giá trị hiệu năng để hoàn thành local tooling MVP. Toàn bộ kết quả thực nghiệm chi tiết thuộc về [báo cáo P8](paper-p8-experiments-and-acceptance.md#e4--kiểm-định-counts-và-replay).

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`/path/to/private-network.json`, `$ROOT`, `$SESSION_ID`). Tuyệt đối không đưa IP máy thật, đường dẫn home cá nhân, thông tin định danh hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`, `gateway`): quyền `0700`.
- **Chống ghi đè tập tin**: Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả/workspace cũ. Mỗi lần chạy phải được khởi tạo trong một thư mục gốc hoàn toàn mới.
- **Ngân sách thời gian (Deadline & Budgets)**: Tham số `--deadline-seconds 900` (15 phút) trên server phải đủ cho toàn bộ các phase: khởi động container, truyền bundle, chạy UNIQUE, chạy REPLAY, cold restart (nếu có), export/import và dọn dẹp.
- TTL recipe dùng `--deadline-seconds` tối thiểu theo preflight (600s TTL + readiness/workload/drain/import/cleanup); token còn hạn thực tế cũng phải đủ toàn bộ budget. A từ chối trước khi tạo workload nếu session/token hoặc journal/output disk bounds không đủ. Với phase này không bật đồng thời cold/durable/conflicts.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Step)**: Việc chuyển bundle, binary và kết quả giữa Host A và Host B là thao tác truyền file thủ công của người vận hành (copy an toàn qua đường dẫn hoặc scp), không tuyên bố là tự động hóa qua SSH.
- **Thực thi tuần tự**: Toàn bộ quá trình chuyển pha phải tuân thủ nghiêm ngặt: `status` xác nhận `READY` → `begin` → xác nhận `MEASURING` và `collector_ready=true` → client `run` → client `export-results` → chuyển kết quả về Host A → server `finish-phase` → xác nhận `VERIFYING` và `VALIDATED`.

---

### 2.2 Quy trình vận hành tuần tự chi tiết

Thiết lập thư mục gốc làm việc trên Host A:

```sh
ROOT=/var/tmp/opencode/p8-e4-<unique-run>
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

#### Bước 1 — Khởi động Server Fixture E4 trên Host A (Terminal A1)

Khởi chạy tiến trình `serve` trên Terminal A1 (tiến trình này phải được giữ chạy ngầm hoặc trên một terminal riêng biệt cho đến khi hoàn tất toàn bộ các phase):

```sh
# Chế độ tiêu chuẩn Pilot (Cache-On, Warm Replay, mặc định 20 unique, 10 replay):
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/A" \
  --network-config /path/to/private-network.json \
  --experiment E4 \
  --cache-mode on \
  --deadline-seconds 900

# Tùy chỉnh counts trong Pilot Profile (tối đa 256 unique, 256 replay; unique chẵn):
# python3 scripts/paper_p8_server.py serve \
#   --root "$ROOT/A" \
#   --network-config /path/to/private-network.json \
#   --experiment E4 \
#   --cache-mode on \
#   --e4-unique-count 40 \
#   --e4-replay-count 20 \
#   --deadline-seconds 900

# Chế độ Official Profile (yêu cầu bắt buộc cả --profile official và --official-opt-in):
# python3 scripts/paper_p8_server.py serve \
#   --root "$ROOT/A" \
#   --network-config /path/to/private-network.json \
#   --experiment E4 \
#   --profile official \
#   --official-opt-in \
#   --e4-unique-count 1000 \
#   --e4-replay-count 100 \
#   --cache-mode on \
#   --deadline-seconds 1200

# Hoặc nếu bật thêm pha tùy chọn Cold Replay, thêm cờ --cold-replay (loại trừ --expiry-replay):
# python3 scripts/paper_p8_server.py serve \
#   --root "$ROOT/A" \
#   --network-config /path/to/private-network.json \
#   --experiment E4 \
#   --cache-mode on \
#   --cold-replay \
#   --deadline-seconds 900

# Hoặc nếu bật pha tùy chọn TTL Expiry Replay (same-process, chờ TTL 600s, loại trừ --cold-replay):
# python3 scripts/paper_p8_server.py serve \
#   --root "$ROOT/A" \
#   --network-config /path/to/private-network.json \
#   --experiment E4 \
#   --cache-mode on \
#   --expiry-replay \
#   --deadline-seconds 1200
```

Để chỉ kiểm tra plan expiry official offline (không khởi chạy container, không truy cập mạng, không chờ TTL):

```sh
python3 scripts/paper_p8_server.py serve --dry-run \
  --root /path/to/placeholder-root \
  --network-config /path/to/private-network.json \
  --experiment E4 \
  --profile official --official-opt-in \
  --e4-unique-count 1000 --e4-replay-count 100 \
  --expiry-replay --deadline-seconds 1200
```

*Phân tích lệnh Dry-Run và ranh giới nghiệm thu*:
- **Ràng buộc tham số bắt buộc**: Lệnh `serve` luôn yêu cầu `--root` và `--network-config` ở tầng phân tích CLI parser (`argparse`), ngay cả khi có cờ `--dry-run`. Do đó, cần cung cấp các đường dẫn placeholder an toàn (ví dụ `/path/to/placeholder-root` và `/path/to/private-network.json`); parser không mở hay kết nối mạng tới các đường dẫn này trong chế độ dry-run.
- **Ngân sách thời gian hữu hạn**: Tham số `--deadline-seconds 1200` thỏa mãn ngân sách thời gian hữu hạn yêu cầu bởi `validate_e4_expiry_budget` ($600\text{s TTL} + 368\text{s workload} + 16\text{s drain} + 30\text{s import} + 30\text{s cleanup} + 120\text{s readiness} = 1164\text{s}$).
- **Kết quả trả về**: Đầu ra JSON có `status: "VALIDATED_PLAN_ONLY"`, bao gồm cấu hình manifest `expiry-replay` trỏ cùng file SQLite journal và ngân sách TTL 600s.
- **Validation-only vs Native TTL proof**: Lệnh dry-run chỉ kiểm tra tính hợp lệ của cú pháp, schema manifest, giới hạn journal và công thức budget offline. Đây **KHÔNG** phải bằng chứng nghiệm thu native TTL 600s. Việc thực thi wall-clock TTL 600s trên hệ thống thực tế hiện vẫn là **NOT RUN**. Tuyệt đối không được rút ngắn TTL production 600s hoặc tuyên bố đã hoàn tất nghiệm thu TTL native chỉ dựa trên dry-run.
- Kết quả kiểm tra offline và giới hạn nghiệm thu được ghi trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#bổ-sung-bs9b--e4-offline-contract-guard-và-regression-refresh-05112026).

*Lưu ý*: Server tự tạo thư mục `$ROOT/A`, người vận hành không tạo trước thư mục này.

#### Bước 2 — Kiểm tra trạng thái và lưu session_id trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái cho đến khi đạt `preparation_status=E4_PREPARED` và `phase=READY`:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Lưu `session_id` nhận được vào biến môi trường trên Terminal A2 để sử dụng cho toàn bộ các lệnh server tiếp theo (thay thế chuỗi placeholder `<session_id>` bằng giá trị hex thực tế nhận được từ status):

```sh
SESSION_ID=<session_id>
# Ví dụ: SESSION_ID=c1d2e3f4a5b6...
```

#### Bước 3 — Chuyển giao gói dữ liệu và binaries sang Host B (Manual Transfer Step)

Sao chép bundle và cả 2 file nhị phân (`paperload`, `gateway`) mà Host A đã biên dịch sẵn sang Host B:

1. Sao chép thư mục `$ROOT/A/client-bundle/` sang Host B tại `$ROOT/B/received-bundle/`.
2. Sao chép tập tin thực thi `$ROOT/A/fixture/private/bin/paperload` sang Host B tại `$ROOT/B/paperload`.
3. Sao chép tập tin thực thi `$ROOT/A/fixture/private/bin/gateway` sang Host B tại `$ROOT/B/gateway`.
4. Thiết lập phân quyền nghiêm ngặt trên Host B:
   ```sh
   ROOT=/var/tmp/opencode/p8-e4-<unique-run>
   mkdir -p "$ROOT/B" && chmod 0700 "$ROOT/B"
   chmod 0700 "$ROOT/B/paperload" "$ROOT/B/gateway"
   find "$ROOT/B/received-bundle" -type d -exec chmod 0700 {} +
   find "$ROOT/B/received-bundle" -type f -exec chmod 0600 {} +
   ```
   *Lưu ý*: Thư mục `$ROOT/B/workspace` tuyệt đối không được tạo trước; lệnh `bind` yêu cầu tự tạo mới với quyền `0700`.

#### Bước 4 — Thiết lập Workspace, tiền kiểm tra và kiểm tra kết nối trên Host B

Trên Host B, thực hiện liên kết bundle, tiền kiểm tra cả hai binary và kiểm tra kết nối mạng/xác thực:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$ROOT/B/received-bundle" \
  --workspace "$ROOT/B/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload" \
  --gateway-binary "$ROOT/B/gateway"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$ROOT/B/workspace"
```

---

### Chu kỳ Pha 1: UNIQUE

#### Bước 5 — Kích hoạt pha UNIQUE trên Host A (Terminal A2)

Gửi lệnh bắt đầu pha `unique` và chờ bộ thu thập chỉ số sẵn sàng:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id unique

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Chờ cho đến khi status trả về `phase=MEASURING`, `phase_id=unique`, và `collector_ready=true`.

#### Bước 6 — Chạy tải và xuất kết quả pha UNIQUE trên Host B

Trên Host B, kích hoạt luồng phát dữ liệu gốc và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload" \
  --gateway-binary "$ROOT/B/gateway" \
  --phase unique

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/unique-results" \
  --phase unique
```

*Kỳ vọng tải và ngữ nghĩa viễn trắc pha UNIQUE*:
- Phát ra đúng $U$ messages viễn trắc gốc (mặc định 20 ở profile pilot; hoặc giá trị `--e4-unique-count` đã chọn, ví dụ 1.000 ở official offline).
- Mỗi message viễn trắc chứa đúng 100 raw samples theo batch semantics duration-safe (tổng cộng $U \times 100$ raw samples; ví dụ 2.000 raw samples ở pilot mặc định, 100.000 raw samples ở official 1.000 messages).
- Client cố tình bỏ qua 1 valid ACK đầu tiên (`ignored_valid_acks=1`) để chứng minh cơ chế retry của client Gateway (`retries >= 1`).
- Kết quả telemetry client ghi nhận: `generated_unique = U`, `offered_unique = U`, `application_acks = U`, `pending_messages = 0`, `pending_bytes = 0`.
- Toàn bộ $U$ records gốc được ghi nhận tuần tự vào file SQLite journal dùng chung `outputs/e4-journal.sqlite`.

#### Bước 7 — Chuyển giao kết quả pha UNIQUE về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/B/unique-results/` sang Host A tại đường dẫn `$ROOT/A/fixture/private/incoming/unique/`. Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 8 — Xác minh pha UNIQUE trên Host A (Terminal A2)

Nhập kết quả để thực hiện kiểm định tính toàn vẹn dữ liệu:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase MEASURING \
  --phase-id unique \
  --results "$ROOT/A/fixture/private/incoming/unique"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Chờ trạng thái chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` kèm `phase_history=["unique"]`. Nếu phase UNIQUE thất bại, **tuyệt đối không** chuyển sang pha REPLAY.

*Tiêu chí đối chiếu dữ liệu tự động của Server*:
- **Database delta**: Tăng đúng $U$ ledger rows, $U \times 100$ raw telemetry samples, $U$ temporal values, 0 configuration changes (`{'ledger': U, 'raw': U * 100, 'temporal': U, 'configuration': 0}`). Cụ thể: 20/2.000/20/0 ở pilot mặc định 20 messages; 1.000/100.000/1.000/0 ở official 1.000 messages.
- **Phân bổ cảm biến (Sensor Scope)**: Dữ liệu phân bổ đều 50/50 cho 2 Sensors trong fixture: mỗi sensor nhận đúng $U / 2$ ledger, $(U / 2) \times 100$ raw samples, $U / 2$ temporal observations.
- **Tính toàn vẹn Raw Identity**: Kiểm tra raw samples độc lập xác nhận `verified_exact=true` trên cả 2 sensors, bảo đảm dữ liệu thô không bị cắt xén hay suy hao.
- **WebSocket Events**: Phát đúng $U$ sự kiện viễn trắc tới viewers (20 ở pilot mặc định).
- **Bộ đệm RAM**: Ghi nhận đúng $U$ khóa dedup vào bộ đệm RAM của Go backend.

---

### Chu kỳ Pha 2: WARM REPLAY

#### Bước 9 — Chuyển tiếp sang pha WARM REPLAY trên Host A (Terminal A2)

Gửi lệnh mở pha tiếp theo và kiểm tra trạng thái sẵn sàng:

```sh
python3 scripts/paper_p8_server.py next-phase \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING \
  --phase-id replay

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Chờ cho đến khi status trả về `phase=READY` và `phase_id=replay`.

#### Bước 10 — Kích hoạt pha WARM REPLAY trên Host A (Terminal A2)

Bắt đầu pha đo lường khử trùng lặp WARM REPLAY:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id replay

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Chờ cho đến khi status trả về `phase=MEASURING`, `phase_id=replay`, và `collector_ready=true`.

#### Bước 11 — Chạy tải và xuất kết quả pha WARM REPLAY trên Host B

Trên Host B, chạy phát lại tải từ journal SQLite có sẵn (tiền tố $R$ bản ghi đầu tiên, mặc định 10 ở pilot; hoặc giá trị `--e4-replay-count` đã chọn, ví dụ 100 ở official):

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload" \
  --gateway-binary "$ROOT/B/gateway" \
  --phase replay

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/replay-results" \
  --phase replay
```

*Kỳ vọng tải pha WARM REPLAY*:
- Client đọc chính xác $R$ bản ghi đầu từ `outputs/e4-journal.sqlite` (không sinh mới dữ liệu, message_id hay timestamp).
- Client nhận đủ $R$ application ACKs (`replay_application_acks = R`, `replay_offered = R`, `generated_unique = 0`, `application_acks = 0`).

#### Bước 12 — Chuyển giao kết quả pha WARM REPLAY về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/B/replay-results/` sang Host A tại đường dẫn `$ROOT/A/fixture/private/incoming/replay/`. Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 13 — Hậu kiểm dữ liệu WARM REPLAY và Đóng Fixture trên Host A (Terminal A2)

Nhập kết quả và xác thực:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase MEASURING \
  --phase-id replay \
  --results "$ROOT/A/fixture/private/incoming/replay"

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Chờ trạng thái chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED`.

*Tiêu chí xác thực WARM REPLAY*:
- **Database snapshot**: Zero new rows, zero new delta trên toàn bộ các bảng (`ledger: 0, raw: 0, temporal: 0, configuration: 0`).
- **Database proof**: Snapshot MD5/fingerprint của cơ sở dữ liệu trước và sau REPLAY phải trùng khớp tuyệt đối (`database_proof.exact_equal = true`).
- **Bộ đệm RAM (`--cache-mode on`)**: Đạt ít nhất $R$ warm cache hits (`warm_lookup_hits >= R`), tránh được $R$ lần gọi repository cơ sở dữ liệu (`repository_calls_avoided >= R`), không phát sinh duplicate transaction ở database.
- **Chế độ Cache-Off (`--cache-mode off`)**: 0 cache hits, xử lý duplicate an toàn tại tầng database transaction (`transaction_commits > 0`, `confirmed_new_rows = 0`).
- **WebSocket Events**: Đúng 0 sự kiện viễn trắc phát mới tới viewers.
- **Journal Continuity**: Mã băm SHA-256 của `outputs/e4-journal.sqlite` bất biến byte-exact so với pha UNIQUE.

Nếu **không** bật `--cold-replay` và **không** bật `--expiry-replay`, tiến hành đóng và hoàn tất dọn dẹp fixture:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```

Chờ tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại: `$ROOT/A/fixture/artifacts/p8-e4-on.json`.

Nếu bật `--cold-replay`, tiếp tục thực hiện Bước 14. Nếu bật `--expiry-replay`, chuyển tiếp sang Mục 2.4.

---

### Chu kỳ Pha 3: COLD REPLAY (Tùy chọn khi bật `--cold-replay`)

#### Bước 14 — Quy trình pha tùy chọn COLD REPLAY

Pha COLD REPLAY chỉ thực hiện khi server đã khởi động với cờ `--cold-replay` và sau khi WARM REPLAY đã được xác nhận `VALIDATED`:

1. **Khởi động rào cản khởi động lại (Cold Restart Barrier) trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING \
     --phase-id cold-replay

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   *Lưu ý*: Server thực hiện dừng/xóa container backend cũ, khởi chạy container backend mới với cùng cấu hình, nạp lại Nginx và xóa trắng bộ đệm RAM dedup. Chờ trạng thái đạt `phase=READY` và `phase_id=cold-replay`.

2. **Kích hoạt đo lường COLD REPLAY trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase READY \
     --phase-id cold-replay

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   Chờ status trả về `phase=MEASURING`, `phase_id=cold-replay`, và `collector_ready=true`.

3. **Chạy tải và xuất kết quả trên Host B**:
   ```sh
   python3 scripts/paper_p8_client.py run \
     --workspace "$ROOT/B/workspace" \
     --binary "$ROOT/B/paperload" \
     --gateway-binary "$ROOT/B/gateway" \
     --phase cold-replay

   python3 scripts/paper_p8_client.py export-results \
     --workspace "$ROOT/B/workspace" \
     --output "$ROOT/B/cold-replay-results" \
     --phase cold-replay
   ```

4. **Chuyển giao kết quả về Host A (Manual Transfer Step)**:
   Sao chép `$ROOT/B/cold-replay-results/` sang `$ROOT/A/fixture/private/incoming/cold-replay/` (quyền `0700`/`0600`).

5. **Xác minh và Đóng Fixture trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase MEASURING \
     --phase-id cold-replay \
     --results "$ROOT/A/fixture/private/incoming/cold-replay"

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   Xác nhận `phase=VERIFYING` và `workload_outcome=VALIDATED` (chứng minh: process instance mới, cache miss > 0, cache hit = 0, zero new DB rows, zero WebSocket events). Sau đó đóng fixture:
   ```sh
   python3 scripts/paper_p8_server.py finalize \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING
   ```
    Chờ tiến trình `serve` kết thúc mã 0, `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại `$ROOT/A/fixture/artifacts/p8-e4-on.json`.

---

### 2.3 Quy trình xử lý sự cố (Abort và Recovery)

- Tùy chọn kiểm tra lỗi dừng và mở lại SQLite bền vững có quy trình riêng tại [Tài liệu Durable Reopen](paper-p8-e4-durable-reopen.md).
- Khi phát sinh lỗi ở bất kỳ pha nào (lỗi kết nối, sai lệch dữ liệu, timeout collector):
  - Giữ nguyên trạng client bundle, SQLite journal trên Host B, outputs và kết quả các pha.
  - Không thử lại trên cùng workspace đã bị lỗi. Không chỉnh sửa thủ công các file state JSON.
  - Gửi lệnh hủy (`abort`) qua giao thức server CLI (lấy phase hiện tại từ status, ví dụ `READY`, `MEASURING` hoặc `VERIFYING`):
    ```sh
    CURRENT_PHASE=$(python3 -c "import json; print(json.load(open('$ROOT/A/fixture/private/session.json'))['phase'])")

    python3 scripts/paper_p8_server.py abort \
      --root "$ROOT/A" \
      --experiment E4 \
      --session-id "$SESSION_ID" \
      --phase "$CURRENT_PHASE"
    ```
  - Dọn dẹp tài nguyên theo đúng receipts tài nguyên ghi nhận trong fixture; không sử dụng `docker compose down` bừa bãi hay lệnh `docker system prune` làm ảnh hưởng đến các tài nguyên dùng chung khác.
  - Nếu thực hiện chạy lại, bắt buộc phải sử dụng một thư mục `$ROOT` hoàn toàn mới.

---

### 2.4 Quy trình tùy chọn TTL EXPIRY 600 giây (thay cho COLD, chưa chạy native)

Pha TTL EXPIRY là quy trình kiểm định tùy chọn chỉ thực hiện khi server đã khởi động với cờ `--expiry-replay` (đi kèm `--cache-mode on`), sau khi WARM REPLAY đã được import và xác nhận `VALIDATED`.

*Ranh giới và tiền điều kiện nghiêm ngặt*:
- **Cùng tiến trình (Same-Process)**: Khác với COLD REPLAY (thay thế container backend), TTL EXPIRY bắt buộc chạy trên cùng một tiến trình backend đang hoạt động liên tục; không dừng hay khởi động lại container backend.
- **Tính loại trừ**: Tuyệt đối **không** kết hợp đồng thời `--expiry-replay` với `--cold-replay`, `--durable-reopen`, hoặc `--conflicts`.
- **Ràng buộc ngân sách thời gian và token**: Tham số `--deadline-seconds` (session budget) và thời hạn JWT token còn lại phải đủ cho toàn bộ chu kỳ preflight (tối thiểu 1.164s, khuyến nghị `--deadline-seconds 1200` theo công thức: 600s TTL + 368s workload + 16s drain + 30s import + 30s cleanup + 120s readiness). Ràng buộc kích thước file journal tối đa 64MB và output tối đa 1MB. Nếu ngân sách không đủ, Host A sẽ từ chối trước khi bắt đầu; không retry pha làm mới tuổi mà phải abort và khởi tạo session/workspace mới.
- **Bảo toàn thời gian thực**: Server chờ đủ 600 giây TTL production tính từ lúc commit pha UNIQUE được Host A xác nhận; không rút ngắn TTL và không can thiệp xóa thủ công cache RAM.

#### Các bước vận hành chi tiết:

1. **Chuyển pha sang `expiry-replay` trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py next-phase \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING \
     --phase-id expiry-replay

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   Chờ trạng thái đạt `phase=READY` và `phase_id=expiry-replay`.

2. **Kích hoạt đo lường `expiry-replay` trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase READY \
     --phase-id expiry-replay

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   *Hành vi bên trong của Server trong lệnh `begin`*:
   - Kiểm tra mã băm SHA-256 của file SQLite journal giữa UNIQUE và REPLAY đồng nhất tuyệt đối.
   - Kiểm tra `process_instance` của backend trước khi chờ trùng khớp với lúc commit pha UNIQUE.
   - Tính toán thời gian đã trôi qua kể từ lúc commit pha UNIQUE được Host A xác nhận (`a_confirmed_commit_time`). Nếu thời gian chưa đủ 600 giây, server tự động tạm dừng (sleep) cho đủ 600 giây TTL production.
   - Sau khi chờ đủ 600s, kiểm tra lại `process_instance` để bảo đảm tiến trình backend không bị thay thế.
   - Khởi động bộ thu thập chỉ số và chuyển trạng thái sang `phase=MEASURING`, `phase_id=expiry-replay`, `collector_ready=true`.
   - **Bắt buộc người vận hành phải kiểm tra status và chờ `collector_ready=true` trước khi phát tải trên Host B.**

3. **Chạy tải và xuất kết quả trên Host B**:
   ```sh
   python3 scripts/paper_p8_client.py run \
     --workspace "$ROOT/B/workspace" \
     --binary "$ROOT/B/paperload" \
     --gateway-binary "$ROOT/B/gateway" \
     --phase expiry-replay

   python3 scripts/paper_p8_client.py export-results \
     --workspace "$ROOT/B/workspace" \
     --output "$ROOT/B/expiry-replay-results" \
     --phase expiry-replay
   ```
   Client đọc lại chính xác $R$ bản ghi đầu tiên ($R=10$ ở pilot mặc định, 100 ở official offline) từ SQLite journal dùng chung `outputs/e4-journal.sqlite`.

4. **Chuyển giao kết quả về Host A (Manual Transfer Step)**:
   Sao chép toàn bộ thư mục `$ROOT/B/expiry-replay-results/` sang Host A tại `$ROOT/A/fixture/private/incoming/expiry-replay/` (thiết lập quyền thư mục `0700` và tập tin `0600`).

5. **Nhập kết quả và xác thực trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase MEASURING \
     --phase-id expiry-replay \
     --results "$ROOT/A/fixture/private/incoming/expiry-replay"

   python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
   ```
   Chờ trạng thái chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED`.
   *Tiêu chuẩn xác thực tự động (Evidence Validation)*:
   - Tuổi commit được xác nhận $\ge 600$ giây (`confirmed_age_seconds >= 600`).
   - Cùng một tiến trình backend (`process_instance` đồng nhất, không có container restart).
   - Mã băm SHA-256 của file SQLite journal bất biến byte-exact so với UNIQUE và WARM REPLAY.
   - Hành vi bộ đệm: Khóa dedup RAM đã hết hạn sau 600s TTL $\rightarrow$ RAM cache miss (`lookup_misses >= R`, `warm_lookup_hits == 0`).
   - Hành vi cơ sở dữ liệu: Duplicate được xử lý an toàn tại tầng DB transaction (`transaction_commits > 0`), zero dòng mới (`confirmed_new_rows = 0`, `copy_calls = 0`, zero delta trên ledger, raw, temporal, configuration).
   - WebSocket: 0 sự kiện viễn trắc phát mới tới viewers.
   - Client nhận đủ $R$ application ACKs (`replay_application_acks = R`).

6. **Đóng Fixture và hoàn tất dọn dẹp trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py finalize \
     --root "$ROOT/A" \
     --experiment E4 \
     --session-id "$SESSION_ID" \
     --phase VERIFYING
   ```
   Chờ tiến trình `serve` trên Terminal A1 kết thúc với exit code 0, kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại `$ROOT/A/fixture/artifacts/p8-e4-on.json`.

*Cảnh báo nghiệm thu*: Runbook này mô tả các ràng buộc hợp đồng và hỗ trợ tooling. Việc thực thi wall-clock TTL 600s trên hệ thống thực tế hiện vẫn là **NOT RUN**. Tuyệt đối không tuyên bố đã hoàn tất nghiệm thu TTL native khi chưa có đợt chạy thực tế trên máy vật lý được chứng nhận.

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

Kết quả E4 bổ sung ngày 05/11/2026 và các lượt lỗi được giữ tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#e4--kiểm-định-counts-và-replay).

### 3.1 Bảng kết quả thực chạy chế độ Cache-On (Warm Subset) sau sửa Validator — 2026-10-30

Chạy thực nghiệm tại thư mục `/var/tmp/opencode/e4-warm-verified-63qy44cd/`:

| Pha thực thi | Unique Messages / App ACKs | Database Delta (ledger / raw / temporal / config) | Ingress Outcomes & ACKs | RAM Cache (Hits / Avoided Calls) | WebSocket Events (Received / Dropped) | Journal SHA-256 | Cleanup & Exit Code |
|---|---:|---:|---:|---:|---:|---|---:|
| **UNIQUE** | 20 / 20 | 20 / 2.000 / 20 / 0 | 20 admitted, 20 ACK (1 retry có chủ đích) | N/A (ghi nhận keys) | 20 / 0 drops | `663cd24ee356...` | N/A (chuyển tiếp) |
| **WARM REPLAY** | 10 / 10 | 0 / 0 / 0 / 0 | 10 duplicate committed, 10 ACK | 10 hits / 10 calls avoided | 0 / 0 drops | `663cd24ee356...` | 12 containers, 1 network, 5 volumes — VERIFIED (Exit 0) |

- **Báo cáo chính**: `A/fixture/artifacts/p8-e4-on.json` (`status=PASS`, `workload_status=WARM_SUBSET_VALIDATED`, `validation.failures=[]`).
- **Xác thực dữ liệu**: Toàn bộ 10 bản ghi replay khớp chính xác tiền tố 10 bản ghi đầu của pha UNIQUE. Cơ sở dữ liệu đạt tính bất biến tuyệt đối giữa hai snapshot trước và sau REPLAY (`database_proof` verified).
- **Tính liên tục của Journal**: Mã băm SHA-256 của `outputs/e4-journal.sqlite` đồng nhất giữa hai pha: `663cd24ee35616712da592fa256a083fa71de65b63aa13ef2b01d5f39ba4c209`.

---

### 3.2 Bảng kết quả thực chạy chế độ Cache-Off sau sửa — 2026-10-30

Chạy thực nghiệm tại thư mục `/var/tmp/opencode/e4-cacheoff-final-20261030/`:

| Pha thực thi | Unique Messages / App ACKs | Database Delta (ledger / raw / temporal / config) | Ingress Outcomes & ACKs | Persistence Window Counters | WebSocket Events | Journal SHA-256 | Cleanup & Exit Code |
|---|---:|---:|---:|---|---:|---|---:|
| **UNIQUE** | 20 / 20 | 20 / 2.000 / 20 / 0 | 20 admitted, 20 ACK | N/A | 20 / 0 drops | `1f00e6ec1c31...` | N/A (chuyển tiếp) |
| **REPLAY** | 10 / 10 | 0 / 0 / 0 / 0 | 10 duplicate committed, 10 ACK | 10 attempts, 10 commits, 10 duplicates, 0 new rows, 0 COPY | 0 / 0 drops | `1f00e6ec1c31...` | 12 containers, 1 network, 5 volumes — VERIFIED (Exit 0) |

- **Báo cáo chính**: `A/fixture/artifacts/p8-e4-off.json` (`status=PASS`, `workload_status=CACHE_OFF_SUBSET_VALIDATED`, marker `PAPER_P8_E4_CACHE_OFF_SUBSET`).
- **Đặc thù Cache-Off**: Bộ đệm RAM bị vô hiệu hóa (`enabled=false`, 0 hits); tính bất biến dữ liệu được đảm bảo thông qua tầng kiểm tra persistence window của PostgreSQL/TimescaleDB (10 giao dịch duplicate commit, zero dòng mới được thêm vào).
- **Tính liên tục của Journal**: SHA-256 bất biến: `1f00e6ec1c318612324a4998b7ac6b4c35677c4ca12919c9776ba64a669b5b5f`.

---

### 3.3 Bảng kết quả thực chạy chế độ Cold-Replay sau sửa — 2026-10-30

Chạy thực nghiệm tại thư mục `/var/tmp/opencode/e4-cold-gate-20261030-02/`:

| Pha thực thi | Unique Messages / App ACKs | Database Delta | Ingress Outcomes & ACKs | Cache Counters | WebSocket Events | Backend Process ID | Cleanup & Exit Code |
|---|---:|---:|---:|---|---:|---|---:|
| **UNIQUE** | 20 / 20 | 20 / 2.000 / 20 / 0 | 20 admitted, 20 ACK | N/A | 20 / 0 drops | Instance 1 (`7d92e13d...`) | N/A (chuyển tiếp) |
| **WARM REPLAY** | 10 / 10 | 0 / 0 / 0 / 0 | 10 duplicate, 10 ACK | 10 hits, 10 avoided | 0 / 0 drops | Instance 1 (`7d92e13d...`) | N/A (chuyển tiếp) |
| **COLD REPLAY** | 10 / 10 | 0 / 0 / 0 / 0 | 10 duplicate, 10 ACK | 10 misses, 0 hits | 0 / 0 drops | Instance 2 (`125b347c...`) | 13 containers, 1 network, 5 volumes — VERIFIED (Exit 0) |

- **Báo cáo chính**: `A/fixture/artifacts/p8-e4-on.json` (`status=PASS`, `workload_status=COLD_REPLAY_SUBSET_VALIDATED`, marker `PAPER_P8_E4_COLD_REPLAY_SUBSET`, `coldrestart=PASS`).
- **Bằng chứng Cold Restart**:
  - Container ID thay đổi: `6ac92918607f...` → `915c14b4d6f0...`.
  - Tiến trình backend thay đổi: `7d92e13d-7317-48c7-96d6-2466460ac6fd` → `125b347c-edd6-4966-8bbb-934cf05b618e`.
  - Bộ đệm RAM trước khi chạy COLD REPLAY là 0 bản ghi; khi phát tải ghi nhận 10 cache misses / 0 hits; DB xử lý 10 giao dịch duplicate nhưng không sinh dòng mới.
- **Tính liên tục của Journal**: SHA-256 bất biến qua cả 3 phase: `c24363aca32f9735a6b83542bf2a4bced620c8807f893ac3ef968b62a6dbe80e`.

---

### 3.4 Lịch sử các lần thất bại và giải pháp khắc phục

Mọi kết quả thất bại lịch sử đều được bảo toàn nguyên trạng trong hồ sơ lưu trữ:

1. **Attempt 3 Warm-Gate (`e4-warm-slice-20261030/attempt-03.d0qtmX`) — FAIL**:
   - *Hiện tượng*: Thất bại ở bước xác thực replay với lỗi `replay_unique_snapshot_mismatch`.
   - *Nguyên nhân*: Bộ kiểm tra so sánh toàn bộ 10 replay headers với toàn bộ 20 UNIQUE headers thay vì chỉ so khớp với 10 header đầu tiên.
   - *Khắc phục*: Cập nhật guard đối chiếu lấy tham số `replay_count` từ manifest để so sánh chính xác lát cắt `UNIQUE[:replay_count]`.
2. **Attempt 1 Warm-Fixed (`e4-warm-fixed-20261030/attempt-01`) — FAIL**:
   - *Hiện tượng*: Bị người vận hành hủy (`abort`) trước lệnh `begin`.
   - *Nguyên nhân*: Nhầm lẫn việc cờ `collector_ready` chưa xuất hiện ở trạng thái ban đầu `READY` là lỗi (theo hợp đồng, cờ này chỉ xuất hiện sau khi lệnh `begin` thực thi thành công).
3. **Attempt 2 Warm-Fixed (`e4-warm-fixed-20261030/attempt-02`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Client Preflight với lỗi `PaperloadInputError: input file does not exist`.
   - *Nguyên nhân*: Thiếu thao tác sao chép 2 file nhị phân `paperload` và `gateway` từ Host A sang Host B trước khi chạy preflight.
   - *Khắc phục*: Bổ sung hướng dẫn sao chép binary tường minh vào runbook.
4. **Các lượt tiền thẩm định Warm (`e4-validator-parent-7iSBjqhF/` và `e4-validator-final-j9xo6dcb/`) — FAIL**:
   - *Hiện tượng*: Workload từng phase đã đạt `VALIDATED` nhưng thất bại tại bước tổng kết báo cáo và thoát tiến trình server (lỗi exit code).
   - *Nguyên nhân*: Lỗi phạm vi metadata image của callback và đọc sai tầng counter trong báo cáo warm.
   - *Bảo toàn*: Giữ nguyên trạng thái FAIL lịch sử, không dùng thay thế cho bằng chứng gate PASS. Sau đó đã sửa phạm vi metadata image và cập nhật logic đọc counter với regression test riêng.
5. **Native Cache-Off Attempt 1 (`e4-cacheoff-native-20261030`) — FAIL**:
   - *Hiện tượng*: Server kết thúc với exit code 1 tại bước xác thực báo cáo (`report_validation`).
   - *Nguyên nhân*: Module tổng hợp báo cáo không đưa trường `database_proof` vào cửa sổ counter của replay, khiến validator không đối chiếu được tính đồng nhất của DB fingerprint.
   - *Khắc phục*: Sửa bộ tổng hợp báo cáo giữ lại proof từ snapshot của UNIQUE tới replay.
6. **Native Cold-Replay Attempt 1 (`e4-cold-gate-20261030-01`) — FAIL**:
   - *Hiện tượng*: Tiến trình client dừng trước khi phát tải cold replay với lỗi `E4 cold replay phase is not enabled or not next`.
   - *Nguyên nhân*: Lỗi đường dẫn kiểm tra eligibility phía client đọc nhầm trường `binding.workload.cache_mode` không tồn tại trong `binding.json`.
   - *Khắc phục*: Sửa binder giữ nguyên file `bundle.json` gốc trong `runtime/bundle-metadata.json` để kiểm tra điều kiện kích hoạt an toàn.

---

### 3.5 Danh mục Checksum và Đường dẫn Artifacts

- **Cache-On (Warm Subset Verified)**:
  - Root: `/var/tmp/opencode/e4-warm-verified-63qy44cd/`
  - Báo cáo chính: `A/fixture/artifacts/p8-e4-on.json`
  - Journal SHA-256: `663cd24ee35616712da592fa256a083fa71de65b63aa13ef2b01d5f39ba4c209`
- **Cache-Off (Subset Verified)**:
  - Root: `/var/tmp/opencode/e4-cacheoff-final-20261030/`
  - Báo cáo chính: `A/fixture/artifacts/p8-e4-off.json`
  - Journal SHA-256: `1f00e6ec1c318612324a4998b7ac6b4c35677c4ca12919c9776ba64a669b5b5f`
  - Source revision: `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244` (dirty fingerprint: `e1a0adab69606fd7629a1a7bfcd1b5517e299e04e6b58692c04066a79ce965ab`)
- **Cold-Replay (Subset Verified)**:
  - Root: `/var/tmp/opencode/e4-cold-gate-20261030-02/`
  - Báo cáo chính: `A/fixture/artifacts/p8-e4-on.json`
  - Journal SHA-256: `c24363aca32f9735a6b83542bf2a4bced620c8807f893ac3ef968b62a6dbe80e`
  - Bundle SHA-256: `9329e451b4304f6235aad27fccb299ee5e7c2df00a264646f7f63662bee730f7`
  - Source revision: `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244` (dirty fingerprint: `f40534a29f0aecf6d1a6ed91186e8abb2497d715810360d6b7cbe3c11ec17353`)

---

## 4. Giới hạn và trạng thái gate

- **Trạng thái cổng P8**: `full_p8_accepted=false`. Kết quả thử nghiệm chỉ xác nhận tính hợp lệ của các tập con E4 (`WARM_SUBSET_VALIDATED`, `CACHE_OFF_SUBSET_VALIDATED`, `COLD_REPLAY_SUBSET_VALIDATED`), tuyệt đối không được coi là nghiệm thu toàn diện cho P8 hoặc chứng nhận production.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Toàn bộ các lượt chạy thực nghiệm được thực hiện trên cùng máy chủ cục bộ (`generator_colocated=true`), chưa chứng minh được hành vi phân tán trên hai máy chủ vật lý độc lập kết nối qua mạng vật lý.
- **Các kịch bản chưa thực hiện**: Kiểm định trôi thời gian thực TTL 600 giây (`expiry_ttl_600s=NOT RUN`), thử nghiệm mở lại Gateway bền vững khi gặp lỗi SQLite (`durable_gateway_reopen=NOT RUN`), xung đột ghi nhận đồng thời (`conflicts=NOT RUN`), kiểm thử phần cứng HIL trên chip AM5728, kiểm thử Full CI và triển khai thực tế (live rollout) đều là **NOT RUN**.
- **Phạm vi dừng**: Dừng lại tại mốc bàn giao E4 đã được xác thực; không tự động mở rộng sang các thử nghiệm khác hoặc thay đổi cấu hình triển khai nếu chưa có yêu cầu mới.
