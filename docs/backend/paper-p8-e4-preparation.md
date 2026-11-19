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
- **Giới hạn tooling đối với $\Gamma_{\text{red}}$ / Paired CLI**: Công cụ máy chủ `paper_p8_server.py` cung cấp lệnh so sánh tự động `compare-e2` cho thử nghiệm E2, nhưng hiện **chưa có lệnh `compare-e4` hay CLI tự động ghép cặp/tổng hợp $\Gamma_{\text{red}}$** giữa các báo cáo E4 Cache-On (`p8-e4-on.json`) và Cache-Off (`p8-e4-off.json`). Việc tính toán $\Gamma_{\text{red}}$ theo công thức trên hiện được thực hiện thông qua trích xuất chỉ số độc lập từ hai báo cáo artifact riêng biệt; tài liệu vận hành tuyệt đối không giả định hay bịa đặt một lệnh CLI tổng hợp không tồn tại trong mã nguồn.

---

## 2. Hướng dẫn vận hành từng bước (Runbook)

> **Trạng thái đã source-qualify:** E4 official 1.000 unique / 100 replay đã PASS trên held server với portable client cho cache-on và cache-off. Kết quả/checkpoint R0–R5, accounting, fingerprint và giới hạn raw proof được tập trung tại [báo cáo P8](paper-p8-experiments-and-acceptance.md#r5b-checkpoint-2026-11-06). Các FAIL lịch sử bên dưới vẫn giữ nguyên; không suy ra lợi ích độ trễ, full raw enumeration, TTL acceptance hay full P8 acceptance.

### 2.1 Quy định an toàn, phân quyền tập tin và biến môi trường độc lập

- **Phân định ba Terminal độc lập**:
  - **Host A — Terminal A1**: Khởi chạy tiến trình `serve` ở chế độ foreground và duy trì fixture trong suốt chu kỳ các pha.
  - **Host A — Terminal A2**: Thực hiện các lệnh điều khiển phiên của máy chủ (`status`, `begin`, `finish-phase`, `next-phase`, `finalize`, `abort`, `cleanup`).
  - **Host B — Terminal B**: Thực hiện các thao tác phía client (`bind`, `preflight`, `check-connection`, `run`, `export-results`).

- **Thiết lập biến môi trường độc lập trên từng Terminal**:
  - *Trên Host A (Terminal A1)*:
    ```sh
    RUN_ID="run-01"
    ROOT="/var/tmp/opencode/p8-e4-${RUN_ID}"
    NETWORK_CONFIG="/path/to/private-network.json"
    mkdir -p "$ROOT" && chmod 0700 "$ROOT"
    ```
    *Lưu ý*: Chỉ tạo thư mục cha `$ROOT`; **tuyệt đối không tạo trước** `$ROOT/A` vì lệnh `serve` yêu cầu tự tạo mới thư mục này với quyền `0700`.
  - *Trên Host A (Terminal A2)*:
    ```sh
    RUN_ID="run-01"
    ROOT="/var/tmp/opencode/p8-e4-${RUN_ID}"
    # Chỉ đọc SESSION_ID tại Bước 2 sau khi status xác nhận READY.
    ```
  - *Trên Host B (Terminal B)*:
    ```sh
    RUN_ID="run-01"
    ROOT="/var/tmp/opencode/p8-e4-${RUN_ID}"
    mkdir -p "$ROOT/B" && chmod 0700 "$ROOT/B"
    ```
    *Lưu ý*: Thư mục `$ROOT/B/workspace` **tuyệt đối không được tạo trước** bằng `mkdir`; lệnh `bind` yêu cầu tự tạo mới với quyền `0700` (lỗi `workspace_output_leaf_exists` nếu đã tồn tại). Cả Host A và Host B dùng chung giá trị `RUN_ID` để đồng nhất cấu trúc đường dẫn.

- **Quy tắc thư mục mới cho mỗi lượt chạy độc lập vs. Dùng chung Journal trong cùng session**:
  - Mỗi lượt chạy (run) độc lập—dù là Cache-On, Cache-Off, hay kịch bản có tùy chọn Cold Replay / TTL Expiry—bắt buộc phải sử dụng một thư mục gốc hoàn toàn mới (ví dụ `/var/tmp/opencode/p8-e4-on-01`, `/var/tmp/opencode/p8-e4-off-01`). Tuyệt đối không tái sử dụng `$ROOT/A` hay workspace/output trên Host B cho một lượt chạy mới.
  - Ngược lại, trong cùng một lượt chạy đa pha (multi-phase session), các pha nối tiếp (UNIQUE → WARM REPLAY → COLD REPLAY hoặc EXPIRY REPLAY) bắt buộc phải dùng chung `$ROOT/A` và `$ROOT/B`, và tái sử dụng nguyên vẹn cùng một file SQLite journal (`outputs/e4-journal.sqlite`) mà pha UNIQUE đã tạo ra.

- **Phân quyền tập tin nghiêm ngặt và kiểm tra toàn vẹn**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`, `gateway`): quyền `0700`.
  - Kiểm tra tính liên tục của Journal: Mã băm SHA-256 của file `outputs/e4-journal.sqlite` bắt buộc phải bất biến byte-exact giữa pha UNIQUE và toàn bộ các pha REPLAY tiếp sau.

- **Ranh giới truyền dữ liệu thủ công (Manual Transfer Step)**:
  - Việc chuyển giao bundle, binary và kết quả giữa Host A và Host B được thực hiện thủ công bởi người vận hành (chỉ định chính xác đường dẫn nguồn và đích). Công cụ không sử dụng SSH tự động hay control plane từ xa.

- **Ngân sách thời gian (Deadline Budget)**:
  - Với ví dụ pilot không expiry, deadline 900s vẫn phải đủ thời gian thực tế. E4 phát tải theo schedule hiện tại 5 messages/s: U=1,000 cần 200s, R=100 cần 20s; pilot U=20/R=10 cần 4s/2s. Workload formula cho U=1,000/R=100 là 756s (200+20+2×200+2×8+120). Mỗi phase tối đa 3,600s, schedule sum bị chặn ở 7,200s; bundle lifetime guard cũng áp dụng. Đây là budget schedule, không phải cam kết wall-clock hoàn thành.
  - Với expiry TTL 600s, expiry guard cần ít nhất 1,552s: 600 TTL + 756 workload + 16 drain + 30 import + 30 cleanup + 120 readiness. Dùng `--deadline-seconds 1800` làm ví dụ và bảo đảm JWT còn hạn ít nhất bằng deadline yêu cầu; server có thể từ chối nếu token/session budget không đủ. Không dùng ví dụ 900s cho expiry.

---

### 2.2 Bảng tra cứu tham số lệnh `serve`, quy tắc loại trừ và kỳ vọng động

#### Bảng tham số CLI hỗ trợ cho E4 (`scripts/paper_p8_server.py serve`):

| Tham số | Giá trị mặc định | Khoảng hợp lệ / Lựa chọn | Mục đích & Ràng buộc theo mã nguồn |
|---|---|---|---|
| `--experiment` | `E1` | Bắt buộc chọn `E4` | Chỉ định thử nghiệm E4. Server từ chối các cờ của E1/E2/E3/E5. |
| `--profile` | `pilot` | `pilot`, `official` | Cấu hình thử nghiệm. Khi chọn `official`, bắt buộc phải có `--official-opt-in`. |
| `--official-opt-in` | `False` | Cờ (flag) | Xác nhận chủ động kích hoạt profile `official`. |
| `--cache-mode` | `on` | `on`, `off` | `on`: Bật RAM dedup cache. Artifact xuất: `$ROOT/A/fixture/artifacts/p8-e4-on.json`.<br>`off`: Tắt RAM dedup cache. Artifact xuất: `$ROOT/A/fixture/artifacts/p8-e4-off.json`. |
| `--e4-unique-count` | `20` | Số chẵn $2 \le U \le 10.000$ | Số message viễn trắc gốc ở pha UNIQUE. Chia đều cho 2 sensors ($U/2$ mỗi sensor). Pilot: tối đa 256. Official: tối đa 10.000 (chuẩn: 1.000). |
| `--e4-replay-count` | `10` | $1 \le R \le U$ | Số message phát lại từ tiền tố SQLite journal ở các pha REPLAY. Pilot: tối đa 256. Official: tối đa 10.000 (chuẩn: 100). |
| `--deadline-seconds` | `900` | $60 \le T \le 86.400$ | Ngân sách toàn phiên, phải đủ chu kỳ thực tế. E4 official 1000/100 expiry example dùng 1800s; JWT còn hạn cũng phải đủ expiry budget. |
| `--cold-replay` | `False` | Cờ (flag) | Bật thêm pha COLD REPLAY sau WARM REPLAY. Bắt buộc `--cache-mode on`. Loại trừ tương hỗ với `--expiry-replay`. |
| `--expiry-replay` | `False` | Cờ (flag) | Bật thêm pha TTL EXPIRY REPLAY sau WARM REPLAY (chờ 600s TTL, cùng tiến trình backend). Bắt buộc `--cache-mode on`. Loại trừ tương hỗ với `--cold-replay`, `--durable-reopen`, `--conflicts`. |
| `--durable-reopen` | `False` | Cờ (flag) | Bật pha kiểm tra dừng/mở lại SQLite Gateway bền vững (xem `paper-p8-e4-durable-reopen.md`). |
| `--conflicts` | `False` | Cờ (flag) | Bật pha kiểm tra từ chối xung đột viễn trắc (xem `paper-p8-e4-conflicts.md`). |
| `--dry-run` | `False` | Cờ (flag) | Kiểm tra tính hợp lệ của plan offline mà không mở Docker container hay mạng. Cho phép kiểm tra hợp đồng official 1.000+100 và ngân sách TTL. |
| `--root` | *Không có* | Đường dẫn thư mục | Đường dẫn thư mục gốc trên Host A (server sẽ tự tạo `$ROOT/A`). |
| `--network-config` | *Không có* | Đường dẫn file JSON | Đường dẫn tập tin cấu hình mạng riêng tư IPv4/TLS. |

#### Quy tắc loại trừ tương hỗ (Mutual Exclusions):
1. **Cache-Off vs. Cold/Expiry**: `--cache-mode off` không được dùng chung với `--cold-replay` hoặc `--expiry-replay` (vì cả hai pha này đều được thiết kế để kiểm chứng hành vi của bộ đệm RAM dedup sau khi xóa hoặc hết hạn TTL).
2. **Cold Replay vs. TTL Expiry**: `--cold-replay` (thay thế container backend, tiến trình mới) và `--expiry-replay` (liên tục cùng một tiến trình backend, giữ nguyên container) loại trừ lẫn nhau tuyệt đối trong cùng một lệnh `serve`.
3. **TTL Expiry vs. Durable/Conflicts**: `--expiry-replay` hiện chỉ hỗ trợ kết hợp với các pha `unique` và `replay`; không kết hợp đồng thời với `--durable-reopen` hoặc `--conflicts`.
4. **Official vs. Opt-In**: Bất kỳ lệnh nào chỉ định `--profile official` đều bắt buộc phải kèm cờ `--official-opt-in`.

#### Công thức tính toán kỳ vọng động (Dynamic Expected Counts) dựa trên $(U, R)$:
- **Số batch viễn trắc**: UNIQUE phát sinh đúng $U$ batches; REPLAY phát lại đúng $R$ batches.
- **Số raw samples (100 samples/message)**:
  - UNIQUE: Tạo mới $U \times 100$ raw samples ghi vào DB (pilot mặc định: 2.000; official chuẩn: 100.000).
  - REPLAY (Warm / Cold / Expiry): Client gửi $R \times 100$ raw samples trong payload, nhưng DB delta ghi nhận **đúng 0 mẫu mới** (`raw: 0`).
- **Database delta**:
  - UNIQUE: `{'ledger': U, 'raw': U * 100, 'temporal': U, 'configuration': 0}`.
  - REPLAY: `{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}` (zero new rows, zero new delta across all tables).
- **Phân bổ cảm biến (Sensor Scope - 2 Sensors)**:
  - UNIQUE: Mỗi sensor nhận đúng $U / 2$ ledger, $(U / 2) \times 100$ raw samples, $U / 2$ temporal values.
  - REPLAY: 0 delta trên cả 2 sensors.
- **Tính toàn vẹn Raw Identity**: UNIQUE yêu cầu cả 2 sensors đạt `verified_exact=true` với `ledger_count = U / 2`.
- **WebSocket Events**: UNIQUE phát đúng $U$ sự kiện viễn trắc tới viewers; REPLAY phát đúng 0 sự kiện viễn trắc mới tới viewers.
- **Hành vi Bộ đệm RAM và Cơ sở dữ liệu theo từng pha**:
  - *Warm Replay (`--cache-mode on`)*: $\ge R$ warm lookup hits (`warm_lookup_hits >= R`), $\ge R$ repository calls avoided, 0 transaction commits ở DB.
  - *Cache-Off Replay (`--cache-mode off`)*: 0 lookup hits, xử lý duplicate an toàn tại tầng DB transaction (`attempts >= R`, `transaction_commits >= R`, `confirmed_new_rows = 0`, `copy_calls = 0`).
  - *Cold Replay*: Tiến trình backend mới $\rightarrow$ RAM cache miss $\ge R$, 0 cache hits, xử lý duplicate tại DB transaction, 0 dòng mới.
  - *TTL Expiry Replay*: Cùng tiến trình backend $\rightarrow$ khóa RAM hết hạn sau 600s TTL $\rightarrow$ RAM cache miss $\ge R$, 0 cache hits, xử lý duplicate tại DB transaction, 0 dòng mới.
**Official native đã source-qualify:** 1.000/100 cache-on/off PASS được ghi trong checkpoint R0–R5 của [báo cáo P8](paper-p8-experiments-and-acceptance.md#r5b-checkpoint-2026-11-06). Đây là hai lượt bounded; lịch sử lỗi và các giới hạn subset phía dưới không bị viết lại.

---

### 2.3 Danh mục các công thức khởi động Server (Terminal A1)

Người vận hành chọn **chính xác một** trong các công thức sau để khởi chạy trên Terminal A1 của Host A:

#### Nhóm 1: Các công thức Pilot Profile (Khả thi thực thi native end-to-end)

- **Công thức 1 — Pilot chuẩn Cache-On (Warm Replay: 20 unique, 10 replay)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile pilot \
    --cache-mode on \
    --e4-unique-count 20 \
    --e4-replay-count 10 \
    --deadline-seconds 900
  ```

- **Công thức 2 — Pilot Cache-Off (Warm Replay: 20 unique, 10 replay)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile pilot \
    --cache-mode off \
    --e4-unique-count 20 \
    --e4-replay-count 10 \
    --deadline-seconds 900
  ```

- **Công thức 3 — Pilot Cache-On kết hợp Cold Replay (3 pha: UNIQUE → WARM → COLD)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile pilot \
    --cache-mode on \
    --cold-replay \
    --e4-unique-count 20 \
    --e4-replay-count 10 \
    --deadline-seconds 900
  ```

- **Công thức 4 — Pilot Cache-On kết hợp TTL Expiry 600s (3 pha: UNIQUE → WARM → EXPIRY, cùng tiến trình backend, chờ 600s TTL)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile pilot \
    --cache-mode on \
    --expiry-replay \
    --e4-unique-count 20 \
    --e4-replay-count 10 \
    --deadline-seconds 1200
  ```

#### Nhóm 2: Các công thức Official Profile (bounded native runbook; không đồng nghĩa TTL/full acceptance)

> Các công thức này là runbook; bằng chứng hai held-portable cache-on/off lượt official 1000/100, source qualification và accounting nằm trong báo cáo P8 R1–R5. Công thức 9 chỉ kiểm tra offline contract, không thay thế native workload hay TTL acceptance.

- **Công thức 5 — Official Cache-On tiêu chuẩn (1.000 unique, 100 replay)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile official \
    --official-opt-in \
    --cache-mode on \
    --e4-unique-count 1000 \
    --e4-replay-count 100 \
    --deadline-seconds 1800
  ```

- **Công thức 6 — Official Cache-Off (1.000 unique, 100 replay)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile official \
    --official-opt-in \
    --cache-mode off \
    --e4-unique-count 1000 \
    --e4-replay-count 100 \
    --deadline-seconds 1800
  ```

- **Công thức 7 — Official Cache-On kết hợp Cold Replay (1.000 unique, 100 replay, 3 pha)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile official \
    --official-opt-in \
    --cache-mode on \
    --cold-replay \
    --e4-unique-count 1000 \
    --e4-replay-count 100 \
    --deadline-seconds 1800
  ```

- **Công thức 8 — Official Cache-On kết hợp TTL Expiry 600s (1.000 unique, 100 replay, 3 pha same-process)**:
  ```sh
  python3 scripts/paper_p8_server.py serve \
    --root "$ROOT/A" \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile official \
    --official-opt-in \
    --cache-mode on \
    --expiry-replay \
    --e4-unique-count 1000 \
    --e4-replay-count 100 \
    --deadline-seconds 1800
  ```

- **Công thức 9 — Official Offline Contract / Schema Validation (`--dry-run` — Khả thi thực thi offline)**:
  ```sh
  python3 scripts/paper_p8_server.py serve --dry-run \
    --root /path/to/placeholder-root \
    --network-config "$NETWORK_CONFIG" \
    --experiment E4 \
    --profile official \
    --official-opt-in \
    --e4-unique-count 1000 \
    --e4-replay-count 100 \
    --expiry-replay \
    --deadline-seconds 1800
  ```
  *Ý nghĩa của Công thức 9*: Lệnh dry-run thực hiện thẩm định schema manifest, bundle bind, Go `paperload --preflight` và tính toán ngân sách thời gian hữu hạn offline mà không mở container Docker hay kết nối mạng. Đây là kiểm tra hợp đồng offline schema-only, **KHÔNG PHẢI** là nghiệm thu native TTL 600s hay capacity claim.

---

### 2.4 Quy trình vận hành chính: Khởi tạo, Chu kỳ UNIQUE và Chu kỳ WARM REPLAY

#### Bước 1 — Khởi động Server Fixture trên Host A (Terminal A1)
Chọn một công thức phù hợp tại Mục 2.3 và khởi chạy ở foreground trên Terminal A1. Giữ terminal này mở trong suốt chu kỳ các pha.
*Lưu ý*: Server tự tạo thư mục `$ROOT/A`, người vận hành không tạo trước thư mục này.

#### Bước 2 — Kiểm tra trạng thái và lưu SESSION_ID trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái cho đến khi đạt `preparation_status=E4_PREPARED` và `phase=READY`:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```

Lưu `session_id` nhận được vào biến môi trường trên Terminal A2 để sử dụng cho toàn bộ các lệnh server tiếp theo:

```sh
# Trích xuất tự động session_id từ session.json:
SESSION_ID="$(python3 -c "import json; print(json.load(open('$ROOT/A/fixture/private/session.json'))['session_id'])")"
# Hoặc gán thủ công từ chuỗi hex hiển thị trong kết quả lệnh status:
# SESSION_ID="c1d2e3f4a5b6"
```

Trên Terminal A2, ghi nhận SHA-256 của hai binary fixture để đối chiếu sau khi chuyển sang B:

```sh
sha256sum "$ROOT/A/fixture/private/bin/paperload" "$ROOT/A/fixture/private/bin/gateway"
```

#### Bước 3 — Chuyển giao gói dữ liệu và binaries sang Host B (Manual Transfer Step)

Sau khi máy chủ đã đạt `E4_PREPARED`, thực hiện chuyển giao thủ công các tệp tin sang Host B:
- **Client Bundle**: Thư mục nguồn `$ROOT/A/client-bundle/` trên Host A $\rightarrow$ Thư mục đích `$ROOT/B/received-bundle/` trên Host B.
- **Binary `paperload`**: Tệp nguồn `$ROOT/A/fixture/private/bin/paperload` trên Host A $\rightarrow$ Tệp đích `$ROOT/B/paperload` trên Host B.
- **Binary `gateway`**: Tệp nguồn `$ROOT/A/fixture/private/bin/gateway` trên Host A $\rightarrow$ Tệp đích `$ROOT/B/gateway` trên Host B.

Thiết lập phân quyền nghiêm ngặt trên Host B (Terminal B):
```sh
chmod 0700 "$ROOT/B/paperload" "$ROOT/B/gateway"
find "$ROOT/B/received-bundle" -type d -exec chmod 0700 {} +
find "$ROOT/B/received-bundle" -type f -exec chmod 0600 {} +
sha256sum "$ROOT/B/paperload" "$ROOT/B/gateway"
```
Đối chiếu từng SHA-256 với Host A trước khi tiếp tục; không thay binary giữa các pha. Thư mục `$ROOT/B/workspace` tuyệt đối không được tạo trước; lệnh `bind` yêu cầu tự tạo mới với quyền `0700`.

#### Bước 4 — Thiết lập Workspace, tiền kiểm tra và kiểm tra kết nối trên Host B (Terminal B)

Trên Terminal B của Host B, thực hiện liên kết bundle, tiền kiểm tra cả hai binary và kiểm tra kết nối mạng/xác thực:

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

#### Bước 6 — Chạy tải và xuất kết quả pha UNIQUE trên Host B (Terminal B)

Trên Terminal B của Host B, kích hoạt luồng phát dữ liệu gốc và xuất kết quả đo lường:

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

Chuyển giao thư mục kết quả từ Host B về Host A:
- **Nguồn**: Thư mục `$ROOT/B/unique-results/` trên Host B.
- **Đích**: Thư mục `$ROOT/A/fixture/private/incoming/unique/` trên Host A.
- Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

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
- **Tính toàn vẹn Raw Identity**: Kiểm tra raw samples độc lập xác nhận `verified_exact=true` trên cả 2 sensors (`ledger_count = U / 2`), bảo đảm dữ liệu thô không bị cắt xén hay suy hao.
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

#### Bước 11 — Chạy tải và xuất kết quả pha WARM REPLAY trên Host B (Terminal B)

Trên Terminal B của Host B, chạy phát lại tải từ journal SQLite có sẵn (tiền tố $R$ bản ghi đầu tiên, mặc định 10 ở pilot; hoặc giá trị `--e4-replay-count` đã chọn, ví dụ 100 ở official):

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

Chuyển giao thư mục kết quả từ Host B về Host A:
- **Nguồn**: Thư mục `$ROOT/B/replay-results/` trên Host B.
- **Đích**: Thư mục `$ROOT/A/fixture/private/incoming/replay/` trên Host A.
- Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 13 — Hậu kiểm dữ liệu WARM REPLAY trên Host A (Terminal A2)

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
- **Bộ đệm RAM (`--cache-mode on`)**: Đạt ít nhất $R$ warm cache hits (`warm_lookup_hits >= R`), tránh được $\ge R$ lần gọi repository cơ sở dữ liệu (`repository_calls_avoided >= R`), không phát sinh duplicate transaction ở database (`transaction_commits = 0`).
- **Chế độ Cache-Off (`--cache-mode off`)**: 0 cache hits, xử lý duplicate an toàn tại tầng database transaction (`attempts >= R`, `transaction_commits >= R`, `confirmed_new_rows = 0`, `copy_calls = 0`).
- **WebSocket Events**: Đúng 0 sự kiện viễn trắc phát mới tới viewers.
- **Journal Continuity**: Mã băm SHA-256 của `outputs/e4-journal.sqlite` bất biến byte-exact so với pha UNIQUE.

#### Bước 13b — Đóng Fixture đối với kịch bản 2 pha tiêu chuẩn (Terminal A2)

Nếu **không** bật `--cold-replay` và **không** bật `--expiry-replay`, tiến hành đóng và hoàn tất dọn dẹp fixture:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```

Chờ tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
- Với `--cache-mode on`: `$ROOT/A/fixture/artifacts/p8-e4-on.json`.
- Với `--cache-mode off`: `$ROOT/A/fixture/artifacts/p8-e4-off.json`.

Nếu bật `--cold-replay`, tiếp tục thực hiện Mục 2.5. Nếu bật `--expiry-replay`, chuyển tiếp sang Mục 2.6.

---

### 2.5 Chu kỳ Pha tùy chọn COLD REPLAY (khi khởi động với `--cold-replay`)

Pha COLD REPLAY chỉ thực hiện khi server đã khởi động với cờ `--cold-replay` (bắt buộc đi kèm `--cache-mode on`) và sau khi WARM REPLAY đã được xác nhận `VALIDATED`:

#### Bước 14.1 — Khởi động rào cản khởi động lại (Cold Restart Barrier) trên Host A (Terminal A2)
```sh
python3 scripts/paper_p8_server.py next-phase \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING \
  --phase-id cold-replay

python3 scripts/paper_p8_server.py status --root "$ROOT/A" --experiment E4
```
*Hành vi của Server*: Dừng và xóa container Go backend cũ, khởi chạy container Go backend mới với cùng cấu hình, nạp lại Nginx và xóa trắng bộ đệm RAM dedup. Chờ trạng thái đạt `phase=READY` và `phase_id=cold-replay`.

#### Bước 14.2 — Kích hoạt đo lường COLD REPLAY trên Host A (Terminal A2)
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

#### Bước 14.3 — Chạy tải và xuất kết quả COLD REPLAY trên Host B (Terminal B)
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

#### Bước 14.4 — Chuyển giao kết quả COLD REPLAY về Host A (Manual Transfer Step)
Chuyển giao thư mục kết quả từ Host B về Host A:
- **Nguồn**: Thư mục `$ROOT/B/cold-replay-results/` trên Host B.
- **Đích**: Thư mục `$ROOT/A/fixture/private/incoming/cold-replay/` trên Host A.
- Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 14.5 — Xác minh COLD REPLAY trên Host A (Terminal A2)
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
Xác nhận `phase=VERIFYING` và `workload_outcome=VALIDATED` (chứng minh: container ID mới, tiến trình backend mới, cache miss $\ge R$, 0 cache hits, zero new DB rows, zero WebSocket events).

#### Bước 14.6 — Đóng Fixture COLD REPLAY trên Host A (Terminal A2)
```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/A" \
  --experiment E4 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```
Chờ tiến trình `serve` trên Terminal A1 kết thúc với exit code 0, status `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại `$ROOT/A/fixture/artifacts/p8-e4-on.json`.

---

### 2.6 Chu kỳ Pha tùy chọn TTL EXPIRY 600 giây (khi khởi động với `--expiry-replay`)

Pha TTL EXPIRY là quy trình kiểm định tùy chọn chỉ thực hiện khi server đã khởi động với cờ `--expiry-replay` (bắt buộc đi kèm `--cache-mode on`), sau khi WARM REPLAY đã được import và xác nhận `VALIDATED`.

*Ranh giới và tiền điều kiện nghiêm ngặt*:
- **Cùng tiến trình (Same-Process)**: Khác với COLD REPLAY (thay thế container backend), TTL EXPIRY bắt buộc chạy trên cùng một tiến trình backend đang hoạt động liên tục; không dừng hay khởi động lại container backend.
- **Tính loại trừ**: Tuyệt đối **không** kết hợp đồng thời `--expiry-replay` với `--cold-replay`, `--durable-reopen`, hoặc `--conflicts`.
- **Ràng buộc ngân sách thời gian và token**: Với expiry TTL 600s, expiry guard cần ít nhất 1,552s (600 TTL + 756 workload + 16 drain + 30 import + 30 cleanup + 120 readiness); dùng deadline 1,800s và JWT còn hạn đủ lâu. Journal bound là `min(64 MiB, max(1 MiB, U × 48 KiB))`, U=1,000 tương ứng 49,152,000 bytes; pending cap giữ nguyên 64, hữu hạn. Không coi các giới hạn này là unbounded queue hay raw full-enumeration proof.
- **Bảo toàn thời gian thực**: Server chờ đủ 600 giây TTL production tính từ lúc commit pha UNIQUE được Host A xác nhận; không rút ngắn TTL và không can thiệp xóa thủ công cache RAM.

#### Bước 15.1 — Chuyển pha sang `expiry-replay` trên Host A (Terminal A2)
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

#### Bước 15.2 — Kích hoạt đo lường `expiry-replay` trên Host A (Terminal A2)
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

#### Bước 15.3 — Chạy tải và xuất kết quả `expiry-replay` trên Host B (Terminal B)
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

#### Bước 15.4 — Chuyển giao kết quả `expiry-replay` về Host A (Manual Transfer Step)
Chuyển giao thư mục kết quả từ Host B về Host A:
- **Nguồn**: Thư mục `$ROOT/B/expiry-replay-results/` trên Host B.
- **Đích**: Thư mục `$ROOT/A/fixture/private/incoming/expiry-replay/` trên Host A.
- Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 15.5 — Nhập kết quả và xác thực trên Host A (Terminal A2)
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

#### Bước 15.6 — Đóng Fixture và hoàn tất dọn dẹp trên Host A (Terminal A2)
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

### 2.7 Quy trình xử lý sự cố và dọn dẹp phục hồi (Abort & Recovery Cleanup)

- Tùy chọn kiểm tra lỗi dừng và mở lại SQLite bền vững có quy trình riêng tại [Tài liệu Durable Reopen](paper-p8-e4-durable-reopen.md).
- Khi phát sinh lỗi ở bất kỳ pha nào (lỗi kết nối, sai lệch dữ liệu, timeout collector, hoặc lỗi preflight):
  - Giữ nguyên trạng client bundle, SQLite journal trên Host B, outputs và kết quả các pha để phục vụ phân tích nguyên nhân gốc.
  - Không thử lại trên cùng workspace đã bị lỗi. Không chỉnh sửa thủ công các file state JSON.
  - Gửi lệnh hủy (`abort`) qua giao thức server CLI trên Terminal A2 (lấy phase hiện tại từ status, ví dụ `READY`, `MEASURING` hoặc `VERIFYING`):
    ```sh
    CURRENT_PHASE=$(python3 -c "import json; print(json.load(open('$ROOT/A/fixture/private/session.json'))['phase'])")

    python3 scripts/paper_p8_server.py abort \
      --root "$ROOT/A" \
      --experiment E4 \
      --session-id "$SESSION_ID" \
      --phase "$CURRENT_PHASE"
    ```
  - **Dọn dẹp tài nguyên phục hồi có hỗ trợ (Supported Recovery Cleanup)**: Nếu container hoặc tài nguyên fixture chưa được dọn sạch hoàn toàn do tiến trình `serve` bị ngắt đột ngột, sử dụng lệnh `cleanup` của máy chủ với mã project ghi nhận trong session/status:
    ```sh
    PROJECT=$(python3 -c "import json; print(json.load(open('$ROOT/A/fixture/private/session.json')).get('project', ''))")

    python3 scripts/paper_p8_server.py cleanup \
      --root "$ROOT/A" \
      --project "$PROJECT" \
      --experiment E4
    ```
    Lệnh `cleanup` thực hiện thu hồi tài nguyên chính xác theo biên lai (receipt-verified cleanup) đã ghi nhận trong fixture; không sử dụng `docker compose down` bừa bãi hay lệnh `docker system prune` làm ảnh hưởng đến các tài nguyên dùng chung khác trên máy chủ.
  - Nếu thực hiện chạy lại, bắt buộc phải sử dụng một thư mục `$ROOT` hoàn toàn mới.

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
