# P8 E2 — helper A/B pilot (source-qualified)

> Offered rate là tổng hai producer logic A+B, không phải rate cho từng
> producer: A 8 và B 16 unique pilot messages; tổng 24 mỗi profile.
> S=100 và S=1 trả lời hai câu hỏi sample-cardinality tương ứng, không phải
> `equal_samples`. Equal-samples là workload riêng (cùng sample stream/count ở
> hai profile) và cặp performance lịch sử contaminated; không gọi thành so sánh
> clean. Kết quả và giới hạn từng lượt nằm trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#e2--hai-sample-profile-và-portable-lifecycle).

Tiến độ native, portable lifecycle và các giới hạn từng checkpoint được ghi tại
[báo cáo P8](paper-p8-experiments-and-acceptance.md#e2--hai-sample-profile-và-portable-lifecycle).

Tài liệu hướng dẫn vận hành và báo cáo bằng chứng cho thử nghiệm P8 E2 (telemetry A/B giữa hai cấu hình batching: `samples_per_message=100` và `1`). E2 được thực hiện trên cùng nền tảng kiến trúc backend (Go modular monolith, Mosquitto DynSec, TimescaleDB, HTTP/REST và Mosquitto TLS).

Thử nghiệm chạy tuần tự hai profile trong từng fixture độc lập; mỗi profile được kiểm định dữ liệu và dọn dẹp tài nguyên độc lập. Đây là bài đo telemetry tổng hợp, không chạy Gateway executor và không chứng minh durable restart.

---

## 1. Tổng quan và phân biệt hai chế độ E2

Thử nghiệm E2 hỗ trợ hai chế độ tải (`workload`) nằm hoàn toàn trong phạm vi thử nghiệm E2 (`--experiment E2`), không tạo thêm thí nghiệm E6 hay helper phân tán riêng biệt:

### 1.1 Bảng so sánh hai chế độ E2

| Đặc tính | Chế độ legacy `steady_burst` (Mặc định) | Chế độ `equal_samples` (Bổ sung 2026-10-31) |
|---|---|---|
| **Tham số CLI** | Bỏ qua `--workload` hoặc `--workload steady_burst` | Chỉ định tường minh `--workload equal_samples` |
| **Phạm vi thử nghiệm** | Nằm trong E2 (`--experiment E2`) | Nằm trong E2 (`--experiment E2`) |
| **Cấu trúc Producer** | 2 producer logic A và B (A steady 8 msgs, B steady 4 msgs + burst 12 msgs) | 2 producer logic A và B đều phát steady liên tục 60s, không burst |
| **Khối lượng công việc** | Bất đối xứng: S=100 (2.400 samples) vs S=1 (24 samples) | Đối xứng cùng công: 6.000 samples ở cả hai profile |
| **Tổng unique messages** | 24 messages (A=8, B=16) cho cả hai profile | S=100: 60 messages (A=30, B=30); S=1: 6.000 messages (A=3.000, B=3.000) |
| **Tốc độ phát** | Không đều giữa A và B (A: 1 msg/s, B burst) | Tổng 100 samples/s đều đặn (mỗi producer 50 samples/s) |
| **Cấu hình shaper fixture** | Bật shaper 2 msg/s, burst 4 (chủ đích kích hoạt rate-limit B) | Tắt shaper riêng trong fixture (`MQTT_SHAPER_ENABLED=false`) |
| **Cấu hình microbatch** | Bật microbatch 50ms (`MQTT_MICROBATCH_ENABLED=true`) | Bật microbatch 50ms (giữ nguyên cùng giới hạn và wait 50ms) |
| **Cấu hình triển khai** | Deployment defaults giữ nguyên (`STAGE5_ENABLED=false`...) | Deployment defaults hoàn toàn không thay đổi |

### 1.2 Phân biệt hai Logical Producers và hai Physical Hosts

Cần phân biệt rõ ràng hai khái niệm độc lập:
- **Hai Logical Producers (A và B)**: Là hai luồng tạo tải logic trong workload mô phỏng. Mỗi producer logic sở hữu 1 Gateway và 1 Sensor độc lập với định danh riêng.
- **Hai Physical Hosts (Host A và Host B)**: Host A là máy chủ chạy hạ tầng Go backend, PostgreSQL, Mosquitto, Nginx (`serve`); Host B là máy trạm chạy tiến trình client phát tải (`paperload`).
- **Giới hạn máy đo**: Thử nghiệm được người vận hành chạy trên cùng máy chủ cục bộ, nhưng metadata của `equal_samples` được ghi nhận chuẩn mực là `generator_colocated='unverified'` (định danh kiểm chuẩn `same-host/unverified`), phân biệt rõ với metadata của các lượt legacy cũ trước đây từng ghi `generator_colocated=true`. Chưa chứng minh môi trường hai máy vật lý độc lập qua mạng vật lý (`physical_ab=NOT RUN`).

### 1.3 Quy cách Context mặc định cho `equal_samples`

Context được tạo một lần qua lệnh `e2-context` và chia sẻ nguyên vẹn cho cả hai profile:
- `total_samples` = 6.000 samples tổng cho toàn bộ workload (3.000 samples mỗi producer).
- `sample_rate` = 100 samples/s tổng (50 samples/s mỗi producer).
- `duration` = 60 giây (`total_samples / sample_rate`).
- `seed` = 84.
- **Profile 1 (`samples_per_message=1`)**: Tổng 6.000 messages (3.000 messages/producer), tốc độ 50 msg/s mỗi producer, mỗi message chứa 1 sample (chu kỳ `sample_interval_us` = 20.000 µs).
- **Profile 100 (`samples_per_message=100`)**: Tổng 60 messages (30 messages/producer), tốc độ 0,5 msg/s mỗi producer (phát mỗi 2 giây), mỗi message chứa 100 samples (chu kỳ `sample_interval_us` = 20.000 µs).

### 1.4 Giới hạn hợp lệ của Context đầu vào

Để đảm bảo tính xác định và ngăn ngừa lỗi làm tròn:
- `total_samples`: Số nguyên dương, tối đa 60.000, bắt buộc chia hết cho 200 (để mỗi producer chia tròn cho cả kích thước 1 và 100, không phát sinh batch lẻ ở cuối).
- `sample_rate`: Số nguyên dương chẵn, từ 2 đến 200 samples/s tổng (chia đều 1..100 cho từng producer).
- `duration = total_samples / sample_rate`: Bắt buộc là số nguyên từ 10 đến 120 giây.
- `2.000.000 / sample_rate`: Bắt buộc là số nguyên để `sample_interval_us` là số nguyên microseconds duration-safe.
- Toàn bộ tính toán số mẫu kỳ vọng sử dụng số nguyên và phân số hữu tỉ (rational arithmetic), không dùng float làm tròn.

### 1.5 Dòng dữ liệu chuẩn hóa và quy tắc mốc thời gian (Timestamp)

- **Canonical same stream**: Cả hai profile chia sẻ cùng một dòng dữ liệu mẫu logic. Với cùng seed 84, producer A phát chuỗi sequence logic `0..2.999`, producer B phát `0..2.999`. Giá trị `syntheticValue(sequence)` tại mỗi sequence là trùng khớp tuyệt đối giữa S=100 và S=1.
- **Mốc thời gian (Timestamp Anchor)**: Timestamp của từng mẫu được tính theo công thức: `timestamp = anchor + sequence * sample_interval_us`.
- Mốc `anchor` được backdate về quá khứ để không sinh ra timestamp tương lai (`future timestamp`). Bộ phân tích timestamp của Go backend không áp đặt giới hạn độ tươi về quá khứ (no past-age freshness limit; chỉ từ chối sai lệch tương lai - future skew). Giới hạn tuổi context TTL (`max_context_age_seconds`: mặc định 86.400s, tối đa 2.592.000s) là chính sách riêng của công cụ/helper được kiểm tra ở tiền kiểm tra (preflight); không tự ý thay đổi anchor giữa hai profile.

### 1.6 Cấu hình hạ tầng Fixture

- Trong fixture của `equal_samples`, shaper được tắt có chủ đích (`MQTT_SHAPER_ENABLED=false`) để đo năng lực producer batching dưới cấu hình backend cố định mà không bị shaper can thiệp cắt tỉa gói tin.
- Microbatch được bật giống nhau ở cả hai profile: `MQTT_MICROBATCH_ENABLED=true`, `MQTT_MICROBATCH_MAX_WAIT=50ms`, `MQTT_MICROBATCH_MAX_MESSAGES=32`, `MQTT_MICROBATCH_MAX_ROWS=500`, `MQTT_MICROBATCH_MAX_PAYLOAD_BYTES=4194304`.
- Toàn bộ cấu hình triển khai hệ thống (`docker-compose.yml`, `config/`, `.env.example`, migrations) hoàn toàn không thay đổi.

### 1.7 Tham số cấu hình CLI, Quy tắc hạch toán và Ranh giới từ chối

#### 1.7.1 Các tham số CLI hỗ trợ và dải giá trị hợp lệ
- `--profile {pilot, official}`: Bộ chọn profile khối lượng tải có giới hạn, mặc định là `pilot`. Profile `official` bắt buộc phải đi kèm cờ `--official-opt-in`. Chú ý: Profile `official` chỉ hỗ trợ kịch bản tải `steady_burst`, không hỗ trợ `equal_samples`.
- `--official-opt-in`: Cờ boolean ủy quyền tường minh cho việc chuẩn bị và chạy profile `official`.
- `--dry-run`: Cờ boolean cho phép kiểm tra tính hợp lệ của kế hoạch thực thi, tham số workload và mô hình ước lượng kích thước mà không khởi động container, không tạo mạng và không truy cập tài nguyên thật (trả về trạng thái `VALIDATED_PLAN_ONLY`).
- `--samples-per-message {100, 1}`: Bắt buộc chỉ định rõ ràng `100` hoặc `1` (E2 yêu cầu cấu hình batching tường minh, không chấp nhận giá trị mặc định ngầm định).
- `--e2-offered-rate {100, 200, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000}`: Tốc độ đề nghị gộp toàn hệ thống, mặc định `100` tin nhắn/giây.
- `--e2-duration-seconds`: Thời lượng phát tải đo lường (số nguyên >= 1, mặc định `10` giây; tối đa 7.200 giây trong ước lượng sizing).
- `--e2-burst-messages`: Số lượng tin nhắn phát dồn (burst), số nguyên từ `0` đến `100000`, mặc định `0`.
- `--e2-pending-messages` và `--e2-pending-bytes`: giới hạn pending **mỗi producer** của E2 official, mặc định lần lượt `64` và `1048576`. Số message từ 1 đến 100000; bytes từ 1 đến 1073741824. Cặp giá trị còn phải thỏa `2 * (pending_bytes + pending_messages * 4096) <= 256 MiB` cho hai producer. Đây là ngân sách logic ước lượng, không phải cam kết RSS.
- `--deadline-seconds`: Thời hạn hiệu lực của phiên làm việc (session deadline), số nguyên từ `60` đến `86400` giây, mặc định `900` giây.

#### 1.7.2 Quy tắc hạch toán tốc độ, burst và số dòng mẫu
- **Tốc độ gộp (Aggregate Offered Rate)**: Giá trị `--e2-offered-rate` là tốc độ tổng hợp của cả hai producer logic A và B cộng lại, không phải tốc độ riêng lẻ của từng producer. Tốc độ được chia đều giữa hai nhánh:
  - Arm A nhận tốc độ: `rates['A'] = offered_rate // 2`.
  - Arm B nhận tốc độ: `rates['B'] = offered_rate - (offered_rate // 2)`.
  - Ví dụ: Với tốc độ 100 msg/s, Arm A phát 50 msg/s và Arm B phát 50 msg/s. Với tốc độ 1.000 msg/s, Arm A phát 500 msg/s và Arm B phát 500 msg/s.
- **Hạch toán burst (Burst Accounting)**: Tham số `--e2-burst-messages` chỉ áp dụng duy nhất cho Arm B (`burst_messages if arm == 'B' else 0`), Arm A không phát burst.
- **Hạch toán tin nhắn unique**: Tổng số tin nhắn unique phát ra trong toàn bộ lượt đo:
  $$\text{total\_messages} = \text{offered\_rate} \times \text{duration\_seconds} + \text{burst\_messages}$$
  Trong đó:
  - Arm A phát: `(offered_rate // 2) * duration_seconds`.
  - Arm B phát: `(offered_rate - (offered_rate // 2)) * duration_seconds + burst_messages`.
- **Hạch toán số dòng mẫu (Sample Rows Accounting)**:
  $$\text{raw\_rows} = \text{total\_messages} \times \text{samples\_per\_message}$$
  - Với Profile S=100: Mỗi tin nhắn chứa 100 mẫu dữ liệu, tạo ra $100 \times \text{total\_messages}$ dòng raw trong cơ sở dữ liệu.
  - Với Profile S=1: Mỗi tin nhắn chứa 1 mẫu dữ liệu, tạo ra $1 \times \text{total\_messages}$ dòng raw trong cơ sở dữ liệu.

Với bundle official mới, estimate tính cả burst vào message, sample và storage; budget pending cũng nằm trong metadata và được đối chiếu với cả hai manifest khi bind. Bundle cũ giữ cách diễn giải estimate cũ. Đổi budget phải tạo session/bundle mới, không sửa manifest đã bind. Tăng budget không bảo đảm backend theo kịp, không thay đổi tốc độ open-loop hay bảo đảm bài test PASS.

#### 1.7.3 Ranh giới tổ hợp tham số được chấp nhận
Không phải mọi tổ hợp tham số đều được hệ thống chấp nhận:
- Profile `official` **chỉ** hỗ trợ chế độ `steady_burst` (`e2-telemetry-ab-official`), **không** hỗ trợ chế độ `equal_samples`. Nếu kết hợp `--profile official` với `--workload equal_samples`, server sẽ từ chối với lỗi `official_equal_samples_not_supported`.
- Chế độ `steady_burst` **không** chấp nhận `--context` hoặc `--reference`. Nếu truyền các tham số này, server sẽ báo lỗi `E2 steady_burst does not accept --context or --reference`.
- Chế độ `equal_samples` **bắt buộc** phải có `--context` (chạy dưới profile `pilot`). Profile S=1 có thể nhận thêm `--reference` trỏ tới báo cáo kết quả của S=100.
- Chế độ `equal_samples` **không** sử dụng các cờ `--e2-offered-rate`, `--e2-duration-seconds`, `--e2-burst-messages` trên lệnh `serve`; cấu hình tải của nó được quy định chặt chẽ trong file `context.json`.

#### 1.7.4 Cơ chế từ chối chu kỳ sống, JWT và tài nguyên
- **Từ chối ngân sách chu kỳ sống (Lifecycle Budget Refusal)**:
  Server kiểm tra ngân sách thời gian phiên `--deadline-seconds` (60..86400s).
  - Với profile `official`: Yêu cầu `deadline_seconds >= e2_duration_seconds + 60` (nhằm bảo đảm thời gian cho phát tải, 30s drain, xuất/nhập kết quả và dọn dẹp).
  - Với profile `equal_samples`: Yêu cầu `deadline_seconds >= duration_seconds + drain_seconds + grace_seconds`.
  - Nếu `--deadline-seconds` không đủ (ví dụ: đặt 900s trong khi bài đo yêu cầu 961s), server sẽ từ chối khởi động ngay tại bước tiền kiểm tra với lỗi từ chối ngân sách chu kỳ sống (`P8 session deadline rejected`).
- **Từ chối hạn hiệu lực JWT (JWT Expiry Refusal)**:
  Token JWT cấp cho client được kiểm tra tính hợp lệ `time.time() < payload['exp'] < time.time() + 86400 * 366`. Tiền kiểm tra phía client (`preflight`) sẽ từ chối thực thi nếu token đã hết hạn hoặc không đủ hạn sống để trang trải toàn bộ thời gian chạy tải và drain.
- **Từ chối vượt ngưỡng tài nguyên ước lượng (Resource Refusal)**:
  Khi chuẩn bị profile `official`, hàm `e2_official_estimate` tính toán kích thước dự báo thận trọng. Server sẽ từ chối khởi động với lỗi `e2_predicted_bound_exceeded` nếu dự báo vượt quá các giới hạn sau:
  - Dung lượng lưu trữ tăng trưởng (`total_storage_growth_bytes_estimate`) > 10 GiB (`max_disk_bytes = 10 << 30`).
  - Budget pending và retry của cả hai producer vượt 256 MiB theo công thức logic ở trên bị từ chối là `e2_estimate_input_rejected`; phần bookkeeping retry được tính trong khoản 4.096 byte/message, không là đo RSS.
  - Băng thông ứng dụng (`application_bandwidth_bytes_per_second`) > 100 MiB/s (`100 << 20`).
- **Thoát cạn hữu hạn (Finite Drain)**:
  Mọi kịch bản đều áp dụng thời gian thoát cạn hữu hạn (20 giây đối với `equal_samples`, 30 giây đối với `official`) để toàn bộ các tin nhắn đang truyền trên đường truyền (in-flight) được backend tiếp nhận, commit vào cơ sở dữ liệu và phát ACK trước khi chuyển sang pha hậu kiểm.

---

## 2. Hướng dẫn vận hành A/B (Runbook)

Quy trình vận hành sử dụng ba terminal độc lập được phân định rõ:
- **Host A — Terminal A1**: Khởi chạy tiến trình `serve` và giữ fixture ở chế độ foreground.
- **Host A — Terminal A2**: Thực hiện các lệnh điều khiển phiên (`status`, `begin`, `finish-phase`, `finalize`, `abort`, `cleanup`, `e2-context`, `compare-e2`).
- **Host B — Terminal B**: Thực hiện các thao tác client workspace (`bind`, `preflight`, `check-connection`, `run`, `export-results`).

### 2.1 Quy định an toàn và nguyên tắc vận hành

- **Placeholders và bảo vệ dữ liệu**: Chỉ sử dụng placeholders hoặc biến môi trường mẫu (`/path/to/private-network.json`, `$ROOT`, `SESSION_ID_100`). Tuyệt đối không đưa IP máy thật, đường dẫn home cá nhân, thông tin định danh hay secrets vào tài liệu vận hành.
- **Phân quyền tập tin nghiêm ngặt**:
  - Thư mục cha và thư mục làm việc: quyền `0700`.
  - Tập tin cấu hình, bundle, chứng chỉ và kết quả: quyền `0600`.
  - Tập tin thực thi binary (`paperload`): quyền `0700`.
- **Chống ghi đè tập tin và Quản lý Thư mục Lá (Fresh Leaves)**:
  - Tuyệt đối không ghi đè hoặc tái sử dụng thư mục kết quả/workspace cũ. Mỗi profile phải được khởi tạo trong một thư mục gốc hoàn toàn mới.
  - Trên Host A: Thư mục cha `$ROOT` được tạo trước (`mkdir -p "$ROOT" && chmod 0700 "$ROOT"`), nhưng thư mục lá của `--root` (ví dụ `$ROOT/s100/A` hoặc `$WORK_DIR`) **không được tạo trước bằng `mkdir`**; lệnh `serve` yêu cầu tự tạo mới với quyền `0700`.
  - Trên Host B: Thư mục `workspace` **không được tạo sẵn trước** khi gọi lệnh `bind`. Nếu thư mục đã tồn tại từ trước, `bind` sẽ từ chối thực thi với lỗi `workspace_output_leaf_exists`.
- **Ngân sách thời gian (Deadline & JWT budgets)**:
  - Thời hạn deadline của server `--deadline-seconds` (60..86400s) phải đủ cho toàn bộ chu kỳ: khởi động container, truyền bundle, chạy workload, thời gian thoát cạn drain (20s đối với `equal_samples`, 30s đối với `official`), truyền kết quả, hậu kiểm DB và dọn dẹp. Với `equal_samples`, yêu cầu $\text{deadline} \ge \text{duration} + \text{drain} + \text{grace}$. Với `official`, yêu cầu $\text{deadline} \ge \text{duration} + 60$ (khuyến nghị 1.200s).
  - Thời hạn hiệu lực của JWT token được cấp phát với TTL riêng (ví dụ 3.600s trong testbed) và tiền kiểm tra client kiểm tra điều kiện `time.time() < payload['exp'] < time.time() + 86400 * 366`; thời hạn JWT không tự động đồng bộ hóa với tham số `--deadline-seconds` của server. Người vận hành phải bảo đảm token còn hạn vượt quá toàn bộ thời gian thực thi của workload.
- **Bảo tồn phạm vi của E5**: E2 sử dụng các tham số `--context` và `--reference` trong không gian thử nghiệm E2, tuyệt đối không làm thay đổi hoặc ảnh hưởng tới ngữ nghĩa của E5.
- **Phân định Direct fixture cục bộ và Quy trình Held Lifecycle**:
  - Luồng Direct local `native_compose_smoke.py --prepare <fresh-root> --paper-p8-e2` là luồng pilot cục bộ có giới hạn được hỗ trợ: cơ chế này khởi tạo fixture P2/TLS độc lập và bọc endpoint thành đối tượng `HTTPSEndpoint`; luồng này không yêu cầu `--network-config` hay deployment credentials.
  - Luồng `paper_p8_server.py serve` là quy trình chu kỳ sống portable được giữ (held portable lifecycle) và bắt buộc phải có tệp cấu hình mạng riêng nghiêm ngặt qua `--network-config`. Không áp đặt yêu cầu định tuyến của held route này lên luồng direct fixture cục bộ.
- **Ranh giới an toàn và quyền hạn của Host B (Client-Side Isolation)**:
  - Máy khách Host B tuyệt đối không có quyền truy cập cơ sở dữ liệu PostgreSQL/TimescaleDB (no DB access), không có quyền quản trị máy chủ backend/admin và không có quyền truy cập Docker daemon/socket trên Host A.
  - Mọi thao tác trên Host B chỉ được thực thi bên trong thư mục workspace cục bộ với quyền `0700`, sử dụng bundle dữ liệu và binary `paperload` đã được sao chép sang, chỉ kết nối tới Host A thông qua các cổng dịch vụ mạng đã công khai và bảo vệ bằng TLS (HTTPS và MQTTS).
- **Dọn dẹp tài nguyên và Tính nghiêm ngặt của Biên lai (Receipt-Verified Cleanup)**:
  - Mọi pha đo lường khi kết thúc bình thường phải thực hiện đầy đủ chuỗi đối soát biên lai (`finish-phase` rồi `finalize`) để đảm bảo dọn dẹp sạch sẽ toàn bộ tài nguyên theo receipts; không để sót container hoặc volume mồ côi.
  - Trường hợp xảy ra lỗi hoặc gián đoạn giữa chừng, sử dụng lệnh `abort` (nếu session còn sống) hoặc lệnh `cleanup` (với `--project "$PROJECT_ID"`) để thu hồi tài nguyên an toàn theo biên lai mà không làm ảnh hưởng tới các container khác trên hệ thống.
- **Dán nhãn truyền dữ liệu thủ công (Manual Transfer Steps)**: Việc chuyển bundle, binary và kết quả giữa Host A và Host B là thao tác truyền file thủ công của người vận hành qua kênh truyền an toàn độc lập; tài liệu chỉ mô tả ngắn gọn đường dẫn nguồn và đích, không giả định điều khiển tự động qua SSH hay scp/rsync.

---

### 2.2 Quy trình vận hành chế độ `equal_samples` (Khuyến nghị)

Chạy tuần tự hai profile: Profile S=100 trước, sau đó Profile S=1 sử dụng `--reference` trỏ tới kết quả của S=100 để xác thực tính bất biến của binary và images.

Dán khối thiết lập sau trên cả A1 và A2. Trên B dùng cùng giá trị `ROOT` tại bước nhận bundle; nếu chạy lại cặp đo, chọn một `ROOT` mới trên tất cả terminal.

```sh
ROOT=/var/tmp/opencode/p8-e2-equal
mkdir -p "$ROOT" && chmod 0700 "$ROOT"
```

#### Bước 0 — Tạo Context dùng chung (Host A)

Tạo tập tin context chứa đặc tả dòng dữ liệu chuẩn hóa:

```sh
python3 scripts/paper_p8_server.py e2-context \
  --output "$ROOT/context.json" \
  --total-samples 6000 --sample-rate 100 --seed 84
chmod 0600 "$ROOT/context.json"
```

#### Bước 1 — Khởi động Profile S=100 trên Host A

Khởi chạy tiến trình `serve` (tiến trình này phải được giữ chạy ngầm hoặc trên một terminal riêng biệt cho đến khi hoàn tất). Người vận hành có thể tùy chọn chạy kiểm tra trước cấu hình với cờ `--dry-run`:

```sh
# (Tùy chọn) Kiểm tra tính hợp lệ của kế hoạch và tham số với --dry-run
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s100/A" \
  --network-config /path/to/private-network.json \
  --dry-run \
  --profile pilot \
  --experiment E2 \
  --samples-per-message 100 \
  --workload equal_samples \
  --context "$ROOT/context.json"
```

Khởi chạy phiên làm việc chính thức:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s100/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --profile pilot \
  --experiment E2 \
  --samples-per-message 100 \
  --workload equal_samples \
  --context "$ROOT/context.json"
```

Trên terminal điều khiển khác của Host A, kiểm tra trạng thái cho đến khi đạt `preparation_status=E2_PREPARED` và `phase=READY`:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Đặt `session_id` nhận được vào biến `SESSION_ID_100` để dùng cho các lệnh sau (trích xuất tự động từ tệp `session.json` của session đang chạy, hoặc gán chuỗi hex thực tế từ lệnh `status`):

```sh
SESSION_ID_100=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['session_id'])" "$ROOT/s100/A/fixture/private/session.json")
# Hoặc gán thủ công từ kết quả status: SESSION_ID_100="e817d576fffef0..."
```

#### Bước 2 — Chuyển giao gói dữ liệu sang Host B (Manual Transfer Step)

Sao chép bundle và binary đã build sang Host B qua kênh truyền duyệt:
1. Sao chép thư mục `$ROOT/s100/A/client-bundle/` sang Host B tại `$ROOT/s100/B/received-bundle/`.
2. Sao chép tập tin thực thi `$ROOT/s100/A/fixture/private/bin/paperload` sang Host B tại `$ROOT/s100/B/paperload`.
3. Phân quyền trên Host B (lưu ý: thư mục `workspace` không được tạo trước vì lệnh `bind` yêu cầu tự tạo mới với quyền `0700`; nếu thư mục `workspace` đã tồn tại từ trước, `bind` sẽ từ chối với lỗi `workspace_output_leaf_exists`):
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-equal
   mkdir -p "$ROOT/s100/B"
   chmod 0700 "$ROOT/s100/B"
   chmod 0700 "$ROOT/s100/B/paperload"
   find "$ROOT/s100/B/received-bundle" -type d -exec chmod 0700 {} +
   find "$ROOT/s100/B/received-bundle" -type f -exec chmod 0600 {} +
   ```

#### Bước 3 — Thiết lập Workspace và kiểm tra kết nối trên Host B

Trên Host B, thực hiện liên kết bundle, tiền kiểm tra và kiểm tra kết nối mạng/xác thực:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$ROOT/s100/B/received-bundle" \
  --workspace "$ROOT/s100/B/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$ROOT/s100/B/workspace" \
  --binary "$ROOT/s100/B/paperload"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$ROOT/s100/B/workspace"
```

#### Bước 4 — Kích hoạt pha đo lường trên Host A

Trên Host A, gửi lệnh bắt đầu pha đo lường và chờ bộ thu thập chỉ số sẵn sàng:

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase READY \
  --phase-id "equal-samples-100"

python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Chờ cho đến khi status trả về `phase=MEASURING`, `phase_id=equal-samples-100`, và `collector_ready=true`.

#### Bước 5 — Chạy tải và xuất kết quả trên Host B

Trên Host B, tiến hành chạy tải phát tin và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$ROOT/s100/B/workspace" \
  --binary "$ROOT/s100/B/paperload"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$ROOT/s100/B/workspace" \
  --output "$ROOT/s100/B/exported-results"
```

#### Bước 6 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Sao chép nguyên vẹn thư mục kết quả `$ROOT/s100/B/exported-results/` sang Host A tại đường dẫn `$ROOT/s100/A/received-results/`. Thiết lập phân quyền thư mục `0700` và tập tin `0600`.

#### Bước 7 — Hậu kiểm dữ liệu và đóng Fixture S=100 trên Host A

Trên Host A, nhập kết quả để thực hiện xác thực tính toàn vẹn dữ liệu trong cơ sở dữ liệu:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase MEASURING \
  --phase-id "equal-samples-100" \
  --results "$ROOT/s100/A/received-results"

python3 scripts/paper_p8_server.py status --root "$ROOT/s100/A"
```

Chờ trạng thái chuyển sang `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` (kèm `imported_result.validation_valid=true` và `imported_result.validation_failures=[]`). Sau đó hoàn tất dọn dẹp:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$ROOT/s100/A" \
  --experiment E2 \
  --session-id "$SESSION_ID_100" \
  --phase VERIFYING
```

Chờ tiến trình `serve` của S=100 kết thúc với mã thoát 0. Kiểm tra status xác nhận `phase=CLOSED` và `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
`$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json`.

---

#### Bước 8 — Khởi động Profile S=1 trên Host A (Tái sử dụng Reference S=100)

Khởi động Profile S=1, truyền tham số `--reference` trỏ tới báo cáo S=100 đã hoàn tất để tái sử dụng binary và container images. Người vận hành có thể tùy chọn kiểm tra tính hợp lệ với `--dry-run`:

```sh
# (Tùy chọn) Kiểm tra tính hợp lệ của Profile S=1 kèm reference với --dry-run
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s1/A" \
  --network-config /path/to/private-network.json \
  --dry-run \
  --profile pilot \
  --experiment E2 \
  --samples-per-message 1 \
  --workload equal_samples \
  --context "$ROOT/context.json" \
  --reference "$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json"
```

Khởi chạy phiên làm việc chính thức cho Profile S=1:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$ROOT/s1/A" \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900 \
  --profile pilot \
  --experiment E2 \
  --samples-per-message 1 \
  --workload equal_samples \
  --context "$ROOT/context.json" \
  --reference "$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json"
```

Kiểm tra status chờ đạt `preparation_status=E2_PREPARED` và `phase=READY`. Đặt `session_id` nhận được vào biến `SESSION_ID_1` để dùng cho các lệnh sau (trích xuất tự động từ tệp `session.json` của session đang chạy, hoặc gán chuỗi hex thực tế từ lệnh `status`):

```sh
SESSION_ID_1=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['session_id'])" "$ROOT/s1/A/fixture/private/session.json")
# Hoặc gán thủ công từ kết quả status: SESSION_ID_1="f928e461b2a3..."
```

#### Bước 9 — Thực hiện chu kỳ đo lường cho Profile S=1

1. **Manual Transfer Step**: Sao chép bundle `$ROOT/s1/A/client-bundle/` sang `$ROOT/s1/B/received-bundle/` và binary `$ROOT/s1/A/fixture/private/bin/paperload` sang `$ROOT/s1/B/paperload`. Xác nhận mã băm SHA256 của `paperload` trùng khớp 100% với S=100.
2. Trên Host B (đảm bảo thư mục `$ROOT/s1/B/workspace` chưa được tạo trước):
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-equal
   python3 scripts/paper_p8_client.py bind --bundle "$ROOT/s1/B/received-bundle" --workspace "$ROOT/s1/B/workspace"
   python3 scripts/paper_p8_client.py preflight --workspace "$ROOT/s1/B/workspace" --binary "$ROOT/s1/B/paperload"
   python3 scripts/paper_p8_client.py check-connection --workspace "$ROOT/s1/B/workspace"
   ```
3. Trên Host A:
   ```sh
   python3 scripts/paper_p8_server.py begin --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase READY --phase-id "equal-samples-1"
   python3 scripts/paper_p8_server.py status --root "$ROOT/s1/A"
   ```
   Chờ `phase=MEASURING`, `phase_id=equal-samples-1`, và `collector_ready=true`.
4. Trên Host B:
   ```sh
   python3 scripts/paper_p8_client.py run --workspace "$ROOT/s1/B/workspace" --binary "$ROOT/s1/B/paperload"
   python3 scripts/paper_p8_client.py export-results --workspace "$ROOT/s1/B/workspace" --output "$ROOT/s1/B/exported-results"
   ```
5. **Manual Transfer Step**: Sao chép kết quả `$ROOT/s1/B/exported-results/` về `$ROOT/s1/A/received-results/`.
6. Trên Host A:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase MEASURING --phase-id "equal-samples-1" --results "$ROOT/s1/A/received-results"
   python3 scripts/paper_p8_server.py status --root "$ROOT/s1/A"
   ```
   Chờ `phase=VERIFYING` và xác nhận trường `workload_outcome=VALIDATED` (kèm `imported_result.validation_valid=true` và `imported_result.validation_failures=[]`). Sau đó finalize:
   ```sh
   python3 scripts/paper_p8_server.py finalize --root "$ROOT/s1/A" --experiment E2 --session-id "$SESSION_ID_1" --phase VERIFYING
   ```
   Chờ tiến trình `serve` kết thúc mã 0, `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
   `$ROOT/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json`.

---

#### Bước 10 — So sánh đối chứng hai Profile qua `compare-e2` (Terminal A2)

Thực hiện so sánh read-only trên hai tập tin báo cáo kết quả hoàn chỉnh:

```sh
python3 scripts/paper_p8_server.py compare-e2 \
  --report-1 "$ROOT/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json" \
  --report-2 "$ROOT/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json" \
  --output "$ROOT/e2-comparison.json"
```

*Lưu ý về tính hợp lệ của phép so sánh*: Do các lần chạy thực tế lịch sử (ví dụ Attempt 4) ghi nhận phát sinh retry trên cả hai profile (1 retry ở S=100, 21 retries ở S=1), thuộc tính `clean_comparison` ghi nhận là `false` và trạng thái tổng thể là `CONTAMINATED`. Trường `headline_speedup` là `UNAVAILABLE`; kết quả này không cấu thành bằng chứng tăng tốc sạch (no clean speedup evidence). Toàn bộ bằng chứng và phân tích tài nguyên chi tiết thuộc về [Báo cáo P8 E2](paper-p8-experiments-and-acceptance.md#e2--hai-sample-profile-và-portable-lifecycle).

#### Xử lý sự cố và dọn dẹp lỗi trong chế độ `equal_samples`

1. **Hủy phiên đang chạy (`abort`)**:
   Khi xảy ra sự cố không thể phục hồi trong bất kỳ pha nào (ví dụ client preflight lỗi, mạng đứt hoặc import thất bại), người vận hành phát lệnh hủy phiên trên Terminal A2 từ phase hiện tại (trích xuất tự động từ `session.json` hoặc truyền tường minh `READY`, `MEASURING`, `VERIFYING`):
   ```sh
   CURRENT_PHASE=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1])).get('phase', 'READY'))" "$ROOT/s100/A/fixture/private/session.json")
   python3 scripts/paper_p8_server.py abort \
     --root "$ROOT/s100/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_100" \
     --phase "$CURRENT_PHASE"
   ```

2. **Dọn dẹp tài nguyên bị cô lập/mồ côi (`cleanup`)**:
   Trường hợp tiến trình `serve` trên Terminal A1 bị dừng đột ngột (crash hoặc nhận tín hiệu SIGKILL) khiến các container Docker chưa kịp dọn dẹp, trích xuất mã dự án Compose (`project`, định dạng `ncs_[a-f0-9]{12}`) ghi nhận trong tệp `session.json` (hoặc `server.log`), sau đó thực hiện dọn dẹp an toàn theo biên lai:
   ```sh
   PROJECT_ID=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1])).get('project', ''))" "$ROOT/s100/A/fixture/private/session.json")
   python3 scripts/paper_p8_server.py cleanup \
     --root "$ROOT/s100/A" \
     --project "$PROJECT_ID" \
     --experiment E2
   ```

---

### 2.3 Quy trình vận hành chế độ Fixed Message Count S100/S1 và Profile Official (steady_burst)

Chế độ `steady_burst` là chế độ đo tải với số lượng tin nhắn cố định giữa hai profile S=100 và S=1, nhằm trả lời câu hỏi về dung lượng mẫu trên mỗi tin nhắn (sample-cardinality), không phải so sánh cùng công (`equal_samples`).

**Ranh giới cấm nghiêm ngặt**:
- Quy trình này **tuyệt đối KHÔNG sử dụng** tham số `--context` hoặc `--reference` (nếu truyền sẽ bị server từ chối ngay tại bước phân tích cú pháp với lỗi `E2 steady_burst does not accept --context or --reference`).
- Chế độ `equal_samples` **tuyệt đối KHÔNG hỗ trợ** dưới profile `official` (kết hợp `--profile official` với `--workload equal_samples` sẽ bị từ chối với lỗi `official_equal_samples_not_supported`).

---

#### 2.3.1 Kịch bản Fixed Message Count Pilot (24 unique messages)

- **Cấu trúc tải**: Cố định 24 unique messages cho mỗi profile (Arm A phát 8 messages steady với tốc độ 1 msg/s; Arm B phát 4 messages steady kèm 12 messages burst dồn dập).
- **Hạch toán dòng mẫu**: Profile S=100 tạo ra $24 \times 100 = 2.400$ dòng raw samples; Profile S=1 tạo ra $24 \times 1 = 24$ dòng raw samples.
- **Quy ước Phase ID**: Sử dụng tường minh `phase-id="samples-100"` cho profile 100 và `phase-id="samples-1"` cho profile 1.

##### Phần A — Thực thi Profile S=100 (Pilot 24 messages)

Trên A2, thiết lập biến trước khi dùng các lệnh điều khiển ở bước 2; các giá trị phải khớp A1:

```sh
ROOT=/var/tmp/opencode/p8-e2-pilot-steady
RUN_LABEL=run01
```

1. **Khởi động server trên Host A (Terminal A1)**:
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-pilot-steady
   RUN_LABEL="run01"        # Nhãn phân biệt đợt chạy (tránh xung đột thư mục lá khi chạy lặp lại)
   mkdir -p "$ROOT" && chmod 0700 "$ROOT"

   python3 scripts/paper_p8_server.py serve \
     --root "$ROOT/steady-s100-${RUN_LABEL}/A" \
     --network-config /path/to/private-network.json \
     --deadline-seconds 900 \
     --profile pilot \
     --experiment E2 \
     --samples-per-message 100 \
     --workload steady_burst
   ```

2. **Kiểm tra trạng thái READY và lấy Session ID (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s100-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ phản hồi `preparation_status=E2_PREPARED` và `phase=READY`. Ghi nhận Session ID:
   ```sh
   SESSION_ID_100=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['session_id'])" "$ROOT/steady-s100-${RUN_LABEL}/A/fixture/private/session.json")
   sha256sum "$ROOT/steady-s100-${RUN_LABEL}/A/fixture/private/bin/paperload"
   ```

3. **Chuyển giao dữ liệu sang Host B (Manual Transfer Step)**:
   - Nguồn bundle: `$ROOT/steady-s100-${RUN_LABEL}/A/client-bundle/` $\rightarrow$ Đích: `$ROOT/steady-s100-${RUN_LABEL}/B/received-bundle/`
   - Nguồn binary: `$ROOT/steady-s100-${RUN_LABEL}/A/fixture/private/bin/paperload` $\rightarrow$ Đích: `$ROOT/steady-s100-${RUN_LABEL}/B/paperload`
   - Thiết lập quyền trên Host B (thư mục `workspace` không tạo trước bằng `mkdir`):
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-pilot-steady
   RUN_LABEL="run01"
   mkdir -p "$ROOT/steady-s100-${RUN_LABEL}/B" && chmod 0700 "$ROOT/steady-s100-${RUN_LABEL}/B"
   chmod 0700 "$ROOT/steady-s100-${RUN_LABEL}/B/paperload"
   find "$ROOT/steady-s100-${RUN_LABEL}/B/received-bundle" -type d -exec chmod 0700 {} +
   find "$ROOT/steady-s100-${RUN_LABEL}/B/received-bundle" -type f -exec chmod 0600 {} +
   ```

4. **Thiết lập Workspace và kiểm tra kết nối (Terminal B)**:
   ```sh
   python3 scripts/paper_p8_client.py bind \
     --bundle "$ROOT/steady-s100-${RUN_LABEL}/B/received-bundle" \
     --workspace "$ROOT/steady-s100-${RUN_LABEL}/B/workspace"

   python3 scripts/paper_p8_client.py preflight \
     --workspace "$ROOT/steady-s100-${RUN_LABEL}/B/workspace" \
     --binary "$ROOT/steady-s100-${RUN_LABEL}/B/paperload"

   python3 scripts/paper_p8_client.py check-connection \
     --workspace "$ROOT/steady-s100-${RUN_LABEL}/B/workspace"
   ```

5. **Kích hoạt pha đo lường trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$ROOT/steady-s100-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_100" \
     --phase READY \
     --phase-id "samples-100"

   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s100-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ status xác nhận `phase=MEASURING`, `phase_id=samples-100`, và `collector_ready=true`.

6. **Chạy tải và xuất kết quả trên Host B (Terminal B)**:
   ```sh
   python3 scripts/paper_p8_client.py run \
     --workspace "$ROOT/steady-s100-${RUN_LABEL}/B/workspace" \
     --binary "$ROOT/steady-s100-${RUN_LABEL}/B/paperload"

   python3 scripts/paper_p8_client.py export-results \
     --workspace "$ROOT/steady-s100-${RUN_LABEL}/B/workspace" \
     --output "$ROOT/steady-s100-${RUN_LABEL}/B/exported-results"
   ```

7. **Chuyển giao kết quả về Host A (Manual Transfer Step)**:
   - Nguồn: `$ROOT/steady-s100-${RUN_LABEL}/B/exported-results/` $\rightarrow$ Đích: `$ROOT/steady-s100-${RUN_LABEL}/A/received-results/`
   - Quyền tại Host A: thư mục `0700`, tập tin `0600`.

8. **Hậu kiểm dữ liệu và hoàn tất phiên trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$ROOT/steady-s100-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_100" \
     --phase MEASURING \
     --phase-id "samples-100" \
     --results "$ROOT/steady-s100-${RUN_LABEL}/A/received-results"

   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s100-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ `phase=VERIFYING` và xác nhận `workload_outcome=VALIDATED`. Sau đó hoàn tất dọn dẹp:
   ```sh
   python3 scripts/paper_p8_server.py finalize \
     --root "$ROOT/steady-s100-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_100" \
     --phase VERIFYING
   ```
   Chờ tiến trình `serve` kết thúc mã 0, `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
   `$ROOT/steady-s100-${RUN_LABEL}/A/fixture/artifacts/p8-e2-samples-100-remote-result.json`.

##### Phần B — Thực thi Profile S=1 (Pilot 24 messages)

Trên A2, khai báo độc lập các giá trị khớp A1:

```sh
ROOT=/var/tmp/opencode/p8-e2-pilot-steady
RUN_LABEL=run01
```

Thực hiện trong thư mục gốc riêng biệt `$ROOT/steady-s1-${RUN_LABEL}/` với đầy đủ các lệnh độc lập:

1. **Khởi động server trên Host A (Terminal A1)**:
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-pilot-steady
   RUN_LABEL="run01"        # Nhãn phân biệt đợt chạy (khớp với phiên tương ứng)
   mkdir -p "$ROOT" && chmod 0700 "$ROOT"

   python3 scripts/paper_p8_server.py serve \
     --root "$ROOT/steady-s1-${RUN_LABEL}/A" \
     --network-config /path/to/private-network.json \
     --deadline-seconds 900 \
     --profile pilot \
     --experiment E2 \
     --samples-per-message 1 \
     --workload steady_burst
   ```

2. **Kiểm tra trạng thái READY và lấy Session ID (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s1-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ phản hồi `preparation_status=E2_PREPARED` và `phase=READY`. Ghi nhận Session ID:
   ```sh
   SESSION_ID_1=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['session_id'])" "$ROOT/steady-s1-${RUN_LABEL}/A/fixture/private/session.json")
   sha256sum "$ROOT/steady-s1-${RUN_LABEL}/A/fixture/private/bin/paperload"
   ```

3. **Chuyển giao dữ liệu sang Host B (Manual Transfer Step)**:
   - Nguồn bundle: `$ROOT/steady-s1-${RUN_LABEL}/A/client-bundle/` $\rightarrow$ Đích: `$ROOT/steady-s1-${RUN_LABEL}/B/received-bundle/`
   - Nguồn binary: `$ROOT/steady-s1-${RUN_LABEL}/A/fixture/private/bin/paperload` $\rightarrow$ Đích: `$ROOT/steady-s1-${RUN_LABEL}/B/paperload`
   - Thiết lập quyền trên Host B:
   ```sh
   ROOT=/var/tmp/opencode/p8-e2-pilot-steady
   RUN_LABEL="run01"
   mkdir -p "$ROOT/steady-s1-${RUN_LABEL}/B" && chmod 0700 "$ROOT/steady-s1-${RUN_LABEL}/B"
   chmod 0700 "$ROOT/steady-s1-${RUN_LABEL}/B/paperload"
   find "$ROOT/steady-s1-${RUN_LABEL}/B/received-bundle" -type d -exec chmod 0700 {} +
   find "$ROOT/steady-s1-${RUN_LABEL}/B/received-bundle" -type f -exec chmod 0600 {} +
   ```

4. **Thiết lập Workspace và kiểm tra kết nối (Terminal B)**:
   ```sh
   python3 scripts/paper_p8_client.py bind \
     --bundle "$ROOT/steady-s1-${RUN_LABEL}/B/received-bundle" \
     --workspace "$ROOT/steady-s1-${RUN_LABEL}/B/workspace"

   python3 scripts/paper_p8_client.py preflight \
     --workspace "$ROOT/steady-s1-${RUN_LABEL}/B/workspace" \
     --binary "$ROOT/steady-s1-${RUN_LABEL}/B/paperload"

   python3 scripts/paper_p8_client.py check-connection \
     --workspace "$ROOT/steady-s1-${RUN_LABEL}/B/workspace"
   ```

5. **Kích hoạt pha đo lường trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py begin \
     --root "$ROOT/steady-s1-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_1" \
     --phase READY \
     --phase-id "samples-1"

   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s1-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ status xác nhận `phase=MEASURING`, `phase_id=samples-1`, và `collector_ready=true`.

6. **Chạy tải và xuất kết quả trên Host B (Terminal B)**:
   ```sh
   python3 scripts/paper_p8_client.py run \
     --workspace "$ROOT/steady-s1-${RUN_LABEL}/B/workspace" \
     --binary "$ROOT/steady-s1-${RUN_LABEL}/B/paperload"

   python3 scripts/paper_p8_client.py export-results \
     --workspace "$ROOT/steady-s1-${RUN_LABEL}/B/workspace" \
     --output "$ROOT/steady-s1-${RUN_LABEL}/B/exported-results"
   ```

7. **Chuyển giao kết quả về Host A (Manual Transfer Step)**:
   - Nguồn: `$ROOT/steady-s1-${RUN_LABEL}/B/exported-results/` $\rightarrow$ Đích: `$ROOT/steady-s1-${RUN_LABEL}/A/received-results/`
   - Quyền tại Host A: thư mục `0700`, tập tin `0600`.

8. **Hậu kiểm dữ liệu và hoàn tất phiên trên Host A (Terminal A2)**:
   ```sh
   python3 scripts/paper_p8_server.py finish-phase \
     --root "$ROOT/steady-s1-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_1" \
     --phase MEASURING \
     --phase-id "samples-1" \
     --results "$ROOT/steady-s1-${RUN_LABEL}/A/received-results"

   python3 scripts/paper_p8_server.py status --root "$ROOT/steady-s1-${RUN_LABEL}/A" --experiment E2
   ```
   Chờ `phase=VERIFYING` và xác nhận `workload_outcome=VALIDATED`. Sau đó hoàn tất dọn dẹp:
   ```sh
   python3 scripts/paper_p8_server.py finalize \
     --root "$ROOT/steady-s1-${RUN_LABEL}/A" \
     --experiment E2 \
     --session-id "$SESSION_ID_1" \
     --phase VERIFYING
   ```
   Chờ tiến trình `serve` kết thúc mã 0, `phase=CLOSED`, `cleanup_outcome=VERIFIED`. Báo cáo kết quả được lưu tại:
   `$ROOT/steady-s1-${RUN_LABEL}/A/fixture/artifacts/p8-e2-samples-1-remote-result.json`.

---

#### 2.3.2 Kịch bản Tải chuẩn Official (`--profile official` — steady_burst)

> **Qualification hiện tại:** E2 official held-portable S=100 và S=1 đã PASS ở workload hữu hạn; xem checkpoint R1 trong [báo cáo P8](paper-p8-experiments-and-acceptance.md#r5b-checkpoint-2026-11-06). Không diễn giải thành equal-samples speedup/capacity claim; các FAIL lịch sử vẫn được giữ.
>
> **Lịch sử seam (đã khắc phục, không còn là blocker hiện tại):** Các lượt trước từng bị chặn do client không forward official opt-in vào preflight/run. R1 xác nhận luồng held portable đã chạy PASS; đoạn này chỉ ghi lại nguyên nhân lịch sử, không phải hướng dẫn vận hành hiện tại.

##### 1. Đặc tả Tham số, Quy tắc Hạch toán và Giới hạn Năng lực

- **Cờ bắt buộc**: `--profile official` và `--official-opt-in`.
- **Dải tham số đầu vào được chấp nhận**:
  - `--e2-offered-rate`: Chọn một trong các mức tốc độ gộp được định nghĩa trong CLI: `100`, `200`, `500`, `1000`, `2000`, `5000`, `8000`, `10000`, `15000`, hoặc `20000` msg/s.
  - `--samples-per-message`: Bắt buộc chỉ định tường minh `100` hoặc `1`.
  - `--e2-duration-seconds`: Thời lượng phát tải đo lường (số nguyên từ `1` đến `7200` giây, mặc định `10` giây).
  - `--e2-burst-messages`: Số lượng tin nhắn burst bổ sung cho Arm B (số nguyên từ `0` đến `100000`, mặc định `0`).
  - `--deadline-seconds`: Ngân sách thời gian phiên, bắt buộc $\ge \text{duration\_seconds} + 60$ (khuyến nghị `1200` giây để bao phủ toàn bộ thời gian khởi động, phát tải, 30s drain, chuyển tệp, hậu kiểm DB và dọn dẹp).
- **Phân bổ Tốc độ giữa hai Producer (Rate Accounting)**:
  Tốc độ `--e2-offered-rate` là tốc độ gộp toàn hệ thống và được chia đều cho hai producer logic:
  - Arm A nhận tốc độ: `rate_A = offered_rate // 2`.
  - Arm B nhận tốc độ: `rate_B = offered_rate - (offered_rate // 2)`.
  | Offered Rate | Tốc độ Arm A (`gw-0/sensor-0`) | Tốc độ Arm B (`gw-1/sensor-1`) | Ghi chú phân bổ |
  |---:|---:|---:|---|
  | **100** msg/s | 50 msg/s | 50 msg/s | Chia đều 1:1 |
  | **1.000** msg/s | 500 msg/s | 500 msg/s | Chia đều 1:1 |
  | **5.000** msg/s | 2.500 msg/s | 2.500 msg/s | Chia đều 1:1 |
  | **20.000** msg/s | 10.000 msg/s | 10.000 msg/s | Chia đều 1:1 |
- **Phân bổ Burst**: Tham số `--e2-burst-messages` chỉ áp dụng duy nhất cho Arm B (`burst_messages if arm == 'B' else 0`); Arm A luôn phát đều (0 burst).
- **Hạch toán Tin nhắn Unique và Dòng Dữ liệu Cơ sở dữ liệu (Expected Counts)**:
  - Tổng số tin nhắn unique:
    $$\text{total\_unique} = \text{offered\_rate} \times \text{duration\_seconds} + \text{burst\_messages}$$
    Trong đó Arm A phát `(offered_rate // 2) * duration_seconds` tin nhắn; Arm B phát `(offered_rate - (offered_rate // 2)) * duration_seconds + burst_messages` tin nhắn.
  - Số dòng raw samples trong TimescaleDB:
    $$\text{raw\_rows} = \text{total\_unique} \times \text{samples\_per\_message}$$
    - Với S=100 (offered=1.000, duration=10s, burst=0): $10.000 \times 100 = 1.000.000$ dòng raw.
    - Với S=1 (offered=1.000, duration=10s, burst=0): $10.000 \times 1 = 10.000$ dòng raw.
  - Số dòng ledger: $\text{total\_unique}$ dòng (khớp 1:1 với số message unique đã commit).
  - Số dòng temporal: $\text{total\_unique}$ dòng.
  - Số dòng configuration: 0 dòng.
- **Quy chuẩn Phase ID bắt buộc (Sửa lỗi Audit Finding)**:
  Trong mã nguồn `scripts/tests/paper_p8_e2_support.py` (hàm `prepare_remote`, dòng 219) và callback `native_compose_smoke.py` (dòng 6668), `phase_id` được gán chính xác là:
  $$\text{phase\_id} = \text{f'samples-\{samples\_per\_message\}'}$$
  cho toàn bộ các kịch bản `steady_burst` (cả pilot lẫn official). Do đó, `phase_id` bắt buộc là **`samples-100`** (khi S=100) hoặc **`samples-1`** (khi S=1). Việc truyền giá trị `official-100` hay `official-1` vào lệnh `begin` hoặc `finish-phase` sẽ bị từ chối ngay với lỗi `e2_phase_id_mismatch`.
- **Cấu hình Tự động Shaper và Microbatch trong Profile Official**:
  Hạ tầng fixture tự động tính toán cấu hình backend tương thích với mức tải đề nghị:
  - `shaper_rate = (offered_rate + 1) // 2` (tốc độ shaper cho mỗi gateway).
  - `shaper_burst = max((burst_messages + 1) // 2, shaper_rate)`.
  - `microbatch_rows = min(500, max(1, shaper_rate * samples_per_message))`.

##### Retry policy E2 official (opt-in)

Các flag sau chỉ áp dụng `steady_burst` official; cần khai báo đủ cả sáu giá
trị hoặc bỏ toàn bộ để giữ legacy fixed-retry semantics. Manifest cũ không có
policy vẫn là legacy; không gán policy mới hồi tố cho artifact cũ.

| Flag | Đơn vị / giới hạn |
|---|---|
| `--e2-retry-mode bounded_backoff_v1` | Chỉ mode được hỗ trợ |
| `--e2-retry-initial-interval-ms N` | ms, 100..60.000 |
| `--e2-retry-max-interval-ms N` | ms, 100..60.000; phải >= initial |
| `--e2-retry-jitter-percent N` | %, 0..100 |
| `--e2-retry-rate-per-second N` | retry attempts/s/producer, 1..10.000 |
| `--e2-retry-burst N` | retry attempts/producer, 1..100.000 |

`bounded_backoff_v1` dùng backoff/jitter và retry token budget riêng mỗi
producer; retries không làm tăng unique offered rate, tối đa 16 retries/message
vẫn giữ nguyên, wall/drain không kéo dài ngầm. Retry budget không dành sẵn
shaper capacity. Dry-run có thể báo `retry_headroom_not_reserved` nếu unique
rate + retry budget vượt shaper rate/gateway; đây là cảnh báo qualification,
không tự dự đoán workload sẽ PASS hay FAIL. Không tắt shaper, đổi trần hoặc
30s drain để ép kết quả.

Sizing tính riêng pending và retry queue logical allowance; với policy mới
`pending_retry_memory_bytes_estimate` gồm pending estimate cộng
`2 * pending_messages * 16` byte retry queue estimate. Giữ gate tổng hiện hành
`2 * (pending_bytes + pending_messages * 4096) <= 256 MiB`; policy retry cộng
thêm `2 * pending_messages * 16` byte vào pending estimate, và combined estimate
phải nằm trong 256 MiB pending memory budget (retry estimate cũng có gate riêng).
Đây là estimate logic, không phải RSS. Dùng pending per
producer theo từng rung (N1/N2 5.000, N3 10.000, N4 arm B 20.000) và tính lại
preflight; cap thấp hơn có thể dừng workload trước khi đo được backend.

Ví dụ ngắn (thay placeholder bằng path/session riêng; không dùng lại leaf):

| Lane | `--e2-offered-rate` | duration | S | burst | unique / raw | pending per producer |
|---|---:|---:|---:|---:|---:|---:|
| N1 | 1.000 | 10s | 100 | 0 | 10.000 / 1.000.000 | 5.000 |
| N2 | 5.000 | 2s | 100 | 0 | 10.000 / 1.000.000 | 5.000 |
| N3 | 20.000 | 1s | 100 | 0 | 20.000 / 2.000.000 | 10.000 |
| N4 | 20.000* | 1s* | 100 | 10.000 | 30.000* / 3.000.000* | 20.000 mỗi producer |

`*` N4 chỉ sau khi N1/N2/N3 có đủ diagnostics và không có blocker; rung thấp
hơn đã đo phải ghi lại expected counts, không gọi là 20k qualification. User
chạy từng lane bằng held lifecycle mới: `serve --root "$ROOT/<lane>/A"
--network-config /path/to/private-network.json --profile official
--official-opt-in --experiment E2 --samples-per-message 100 --workload
steady_burst --e2-offered-rate <rate> --e2-duration-seconds <seconds>
--e2-burst-messages <burst> --e2-pending-messages <per-producer>
--e2-pending-bytes <bytes> --deadline-seconds <>=duration+60` (và sáu retry
flag nếu opt-in). Tiếp tục theo lifecycle hiện có: status READY → transfer
bundle/binary → bind/preflight/check-connection → begin `samples-100` → run →
export → finish-phase → finalize. N2/N3/N4 hiện `NOT RUN`; recipes không phải
native verification.

Held `serve` thật yêu cầu network config private có mode `0600`, interface RFC1918
hợp lệ và ports còn trống; `--dry-run` không xác minh mode file hay readiness
runtime. Không đưa địa chỉ thật vào tài liệu. Kiểm artifacts
`fixture/artifacts/p8-e2-diagnostics.json` trước khi kết thúc. Khi B trả PARTIAL,
vẫn chạy `export-results`, chuyển export và gọi `finish-phase` để lưu projection/
diagnostics trung thực; không sửa expected counts. Nếu export/import từ chối hoặc
flow không thể tiếp tục, giữ artifact lỗi và gọi `abort`/cleanup theo receipt;
không gắn acceptance. Diagnostics best-effort chỉ trong lifecycle trước cleanup.

#### Chẩn đoán persistence E2 (source mới, chạy riêng)

Remote E2 official `steady_burst` tự thu thập 11 stage cố định `persistence.durations`; stage
`begin` bao gồm cả thời gian lấy connection từ pool. `lock SQL duration` là
thời lượng câu SQL liên quan, **không** phải thời gian chờ lock thuần. Báo cáo
giữ lại các trường pool của `pgx` (`db`), nhưng không diễn giải chúng thành
PostgreSQL server wait.

Lượt source mới chưa có phép đo remote hoàn chỉnh. Khi được người dùng điều phối
Host B, chạy lại cùng workload với leaf hoàn toàn mới (ví dụ `retention-check-<new-run-id>`,
tuyệt đối không tái dùng workspace hay artifact cũ), giữ nguyên toàn bộ thông số:

```text
rate=1000, S=100, duration=10, burst=0,
pending=8192/8388608, deadline=3600,
retry flags=500/8000/10/100/100 (cùng mode bounded_backoff_v1: initial 500ms, max 8000ms, jitter 10%,
rate=100 và burst=100 cho mỗi producer)
```

Giữ 4 workers, 500 rows, pool 10, shaper bật, backend quota 2 CPU/4 GiB và
drain 30 giây. Tiếp tục đúng lifecycle từng bước ở trên: tạo bundle/binary mới
và chuyển thủ công sang B; `begin` dùng `--phase-id samples-100` (100 samples/message,
không phải yêu cầu 100 mẫu diagnostics); B export kết quả kể cả
khi `PARTIAL`; A `finish-phase` hoặc `abort` theo kết quả thật, rồi xác minh
exact cleanup. Không hứa lượt chạy sẽ PASS; việc phối hợp vận hành trên B thuộc
người dùng. Không tự ý áp dụng tinh chỉnh tải hay chuyển sang N2/N3/N4 khi chưa
có yêu cầu riêng. Việc sửa đổi cơ chế lưu giữ chẩn đoán cục bộ không phải là
cải thiện hiệu năng đo đạc từ xa và không đưa ra kết luận nâng cấp phần cứng.

Diagnostics bổ sung chỉ đọc trên PostgreSQL 16 của fixture. `postgres_linux`
giữ baseline, từng sample và terminal; mỗi nguồn có timestamp riêng, nên không
ghép chúng như cùng một thời điểm. Truy vấn chẩn đoán tự làm tăng một phần DB
counters; `pg_stat_database` là database-scoped, `pg_stat_wal` và `pg_stat_io`
là cluster-scoped, không riêng telemetry. Trước khi lấy delta phải xác nhận
server/database identity và `stats_reset` không đổi; nếu reset hoặc identity
không khớp thì delta không hợp lệ. View chưa được hỗ trợ, cgroup v2 không đọc
được hoặc rootless Docker được biểu diễn `unavailable`, không phải zero.
`track_io_timing` và `track_wal_io_timing` nếu OFF giữ thời gian là
`null`/unavailable; mã đo **không bật** các setting này.

##### Cơ chế lưu giữ chẩn đoán active (Bounded Active Side Retention)

Ở các phiên bản thử nghiệm trước (như lượt `run07`), bộ lấy mẫu phụ sử dụng
vòng đệm xoay vòng (rolling ring) 64 mẫu và 512 KiB; khi người vận hành chờ
điều phối giữa các bước, vòng đệm tiếp tục thu thập các mẫu trong giai đoạn rảnh
(idle), dẫn đến việc loại bỏ (eviction) và làm mất toàn bộ chứng cứ active burst
ban đầu. Để khắc phục hiện tượng mất dữ liệu này mà không làm phình bộ nhớ,
mã nguồn hiện hành quy định chặt chẽ:

1. **Phạm vi áp dụng:**
   Chỉ riêng profile E2 official `steady_burst` mới kích hoạt cơ chế lưu giữ
   chủ động có giới hạn (bounded active side capture). Các chế độ standalone,
   pilot hoặc `equal_samples` tiếp tục giữ nguyên hành vi trước đây. Tiến trình
   đọc chậm riêng biệt (separate slow-reader thread) cho PostgreSQL/Linux/Docker
   hoạt động độc lập, không gây nghẽn tiến trình lấy mẫu HTTP định kỳ.

2. **Điều kiện kích hoạt (Trigger):**
   Trigger kích hoạt giai đoạn active CHỈ ĐƯỢC XÁC ĐỊNH khi thỏa mãn đồng thời:
   - CÙNG một phiên bản tiến trình Go backend (`process_instance`).
   - Chỉ số `ingress.Received` (dạng số nguyên hợp lệ) ghi nhận sự gia tăng
     so với baseline của HTTP scraper trên Host A, căn cứ theo tem thời gian
     hoàn tất cào `scrape_finished_monotonic` / `scrape_finished_timestamp`.
   - Tuyệt đối không tính thời điểm kích hoạt từ lệnh `begin`, không phụ thuộc
     vào tiến trình đọc PostgreSQL hay đồng hồ wall-clock trên Host B.
   - Vùng đệm tiền kích hoạt (pre-trigger buffer) lưu tối đa $\le 4$ mẫu và
     $\le 64\text{ KiB}$, được **tính gộp** trong tổng giới hạn 128 mẫu / 2 MiB.
     Hệ thống có thể chờ trước khi trigger xuất hiện; thời lượng thu thập không
     chạy sớm khi chưa có lưu lượng thực tế.

3. **Cửa sổ thu thập yêu cầu (Requested Capture Window):**
   - Thời lượng cửa sổ yêu cầu sau trigger = $\max(\text{tổng thời lượng các pha của A}, \text{tổng thời lượng các pha của B}) + 10\text{s}$
     xử lý hậu kỳ (bao gồm đầy đủ các pha: warmup + measure + drain + cooldown).
   - Với kịch bản chuẩn của `run07` (10s đo lường + 30s drain), thời lượng
     yêu cầu sau trigger là $10\text{s} + 30\text{s} + 10\text{s} = 50\text{ giây}$.
   - Hạn chót tuyệt đối của phiên (session absolute deadline) độc lập giới hạn
     thời điểm kết thúc thực tế; ngân sách thời gian được tính toán lại sau các
     lần đo baseline chậm và không được gia hạn vượt deadline.

4. **Chính sách bảo toàn chứng cứ sớm (Earliest Evidence Preservation):**
   - Sau khi trigger kích hoạt, hệ thống **bảo toàn các mẫu sớm nhất**, TUYỆT ĐỐI
     KHÔNG thực hiện đẩy mẫu cũ (eviction) để lấy chỗ cho các mẫu rảnh (idle).
   - Khi hết cửa sổ yêu cầu tự nhiên (natural window end), toàn bộ mẫu được
     đóng băng (freeze); các khoảng lặng tạm thời trong cửa sổ không đóng băng sớm.
   - Nếu chạm trần số lượng mẫu (128 mẫu) hoặc trần dung lượng (2 MiB), bộ lấy
     mẫu lập tức đóng băng và gắn nhãn `truncated` / `incomplete`.
   - Trường hợp thiếu baseline, sai lệch identity, thiếu trigger, tiến trình backend
     bị reset/thay đổi, dừng sớm hoặc bị cắt bởi deadline, kết quả KHÔNG được
     coi là hoàn tất (`complete`). Không tuyên bố độ bao phủ đầy đủ chỉ vì thời
     gian cửa sổ đã trôi qua; lỗi hoặc trạng thái thiếu của từng nguồn riêng
     biệt vẫn giữ nguyên qualification.

5. **Kết thúc Reader và Bao phủ Nguồn:**
   - Tiến trình đọc (reader) đã khởi chạy trước mốc đóng băng được phép hoàn tất
     với tem thời gian thực tế vượt qua ranh giới cửa sổ; tuyệt đối không khởi
     chạy thêm nguồn đọc mới sau khi đã kết thúc.
   - Các nguồn chưa kịp khởi chạy được đánh dấu `unavailable` với mã lỗi cố định;
     độ bao phủ nguồn từng phần được ghi nhận trung thực.
   - Baseline và terminal luôn được giữ tách biệt ngoài tập mẫu định kỳ.
   - Thao tác dừng/chờ (stop/join) có giới hạn thời gian (bounded join), không
     cam kết ngắt tức thời thời gian thực (hard real-time cancellation) đối với
     tiến trình đọc ngoại vi đang bị nghẽn I/O.

6. **Giới hạn dung lượng mẫu và tệp chẩn đoán (Sample & File Caps):**
   - Ngân sách mẫu lưu giữ theo kích thước JSON cho `postgres_linux` là **128 mẫu và 2 MiB**
     (đây là dung lượng mẫu serialized/logical được giữ lại, không phải phân bổ RAM cứng;
     thay cho mức 64 mẫu và 512 KiB rolling cũ).
   - Ngân sách vòng đệm HTTP metrics giữ nguyên **3.600 mẫu và 2 MiB**.
   - Tổng dung lượng TỆP JSON chẩn đoán (`p8-e2-diagnostics.json`) được giới hạn
     cứng ở mức **8 MiB**, được đo đạc bằng dung lượng chuỗi mã hóa thực tế qua
     hàm `json.dumps` mặc định bởi helper chuẩn tắc
     `native_compose_smoke.paper_p8_e2_persist_diagnostics(path, payload)`.
     Helper này được dùng thống nhất cho mọi lượt ghi (sớm, hoàn tất, hoặc fallback).
   - Cần phân định rạch ròi: ngân sách dung lượng mẫu tuần tự hóa (2 MiB) tách biệt với giới
     hạn tệp JSON tổng thể trên đĩa (8 MiB), và cả hai đều không đại diện cho mức tiêu
     thụ RSS của toàn tiến trình. Nếu tệp vượt quá 8 MiB, hệ thống fail-closed
     với trạng thái giới hạn dung lượng nhỏ gọn, không gán độ bao phủ sạch.

7. **Xuất kết quả và ranh giới nghiệm thu:**
   - Các lệnh `finish-phase` hoặc `abort` thực thi trước khi dọn dẹp để xuất
     toàn bộ baseline, samples, coverage, terminal của `postgres_linux` và các
     dữ liệu active sớm nhất; sự chậm trễ của người vận hành sau khi mẫu đã
     freeze không thể ghi đè vùng dữ liệu active đã lưu.
   - Ranh giới nghiệm thu tách biệt: trường `full_p8_accepted: false` tiếp tục
     được duy trì trong dữ liệu chẩn đoán, bảo toàn đầy đủ các kết quả
     `PARTIAL`, `FAIL` và dữ liệu bị khuyết. Việc xuất thành công tệp chẩn đoán
     và dọn dẹp sạch tài nguyên (exact cleanup) KHÔNG PHẢI là nghiệm thu hiệu
     năng hay cam kết về năng lực chịu tải.

Chỉ khi counter/timestamp/identity hợp lệ mới tính delta theo khoảng đo
`delta_t` giây:

- CPU cores trung bình = `delta usage_usec / (1e6 * delta_t)`.
- Tỷ lệ kỳ đo bị throttled = `delta nr_throttled / delta nr_periods` khi
  `delta nr_periods > 0`; đây không phải phần trăm throughput bị mất.
- Disk await trung bình (ms/op) =
  `(delta read_time_ms + delta write_time_ms) / (delta reads_completed + delta writes_completed)`
  khi mẫu số lớn hơn 0.
- Average queue = `delta weighted_io_time_ms / (1000 * delta_t)`.

Không dùng delta sau reset và không cộng các device phân lớp (layered devices),
vì có thể đếm cùng I/O nhiều lần. Host `diskstats` không đo latency riêng của
container; `sampledRAM` không phải peak. Quota 2 CPU chỉ áp cho backend, không
giới hạn toàn stack và không pin CPU. Đây là chẩn đoán, không phải chẩn đoán
phần cứng hay bằng chứng bottleneck/capacity.

- **Ngân sách Dự báo và Ranh giới Từ chối Tài nguyên (`e2_official_estimate`)**:
  Server tự động tính toán mô hình ước lượng kích thước trước khi khởi động. Server sẽ từ chối khởi động ngay tại bước `prepare` với lỗi `e2_predicted_bound_exceeded` nếu dự báo vượt quá các giới hạn an toàn:
  - Dung lượng lưu trữ tăng trưởng (`total_storage_growth_bytes_estimate`) > 10 GiB (`10 << 30`).
  - Bộ nhớ hàng đợi pending (`pending_memory_bytes`) > 256 MiB (`256 << 20`).
  - Bộ nhớ lưu trữ retry (`retry_memory_bytes`) > 256 MiB (`256 << 20`).
  - Băng thông ứng dụng (`application_bandwidth_bytes_per_second`) > 100 MiB/s (`100 << 20`).
- **Thoát cạn Hữu hạn (Finite Drain)**:
  Profile `official` quy định thời gian thoát cạn hữu hạn cố định là **30 giây** để toàn bộ tin nhắn đang truyền trên đường truyền (in-flight) được backend tiếp nhận, commit vào cơ sở dữ liệu và phát ACK trước khi chuyển sang pha đối soát.
- **Ranh giới Kỹ thuật và Giới hạn Năng lực Thực tế**:
  - Các mức 1.000, 5.000, 20.000 msg/s là các bậc thang kiểm tra tải cao có kiểm soát; **tuyệt đối không phải là cam kết năng lực chịu tải (capacity guarantee)** hay chứng nhận backend có thể đạt 20.000 msg/s trên mọi cấu hình phần cứng.
  - Mosquitto DynSec, Nginx và Go backend có thể xuất hiện độ trễ xử lý, shaper drops (`rate_limited`) hoặc client connection/publish retries khi máy chủ bị bão hòa tài nguyên ở các mức tải cao.
  - Metadata thử nghiệm vẫn được gắn nhãn chuẩn tắc là `physical_ab=NOT RUN` và `generator_colocated='unverified'`.

---

##### 2. Quy trình Thực thi End-to-End cho Mọi Rung Official

Người vận hành chọn các biến đầu vào mong muốn và theo dõi quy trình từng bước dưới đây; các giới hạn nghiệm thu vẫn theo qualification ở báo cáo P8:

###### Bước 0 — Thiết lập Biến cấu hình Đầu vào

Thiết lập biến độc lập trên các terminal tương ứng:

**Trên Host A — Terminal A1 & A2**:
```sh
ROOT=/var/tmp/opencode/p8-e2-official
OFFERED_RATE=1000        # Chọn 1 trong: 100, 200, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000 (msg/s gộp cả hai producer)
SAMPLES=100              # Chọn 1 trong: 100 hoặc 1 (batching samples per message)
DURATION=10              # Thời lượng phát đo lường (giây, mặc định 10, hợp lệ 1..7200)
BURST=0                  # Tin nhắn burst bổ sung cho Arm B (mặc định 0, hợp lệ 0..100000)
PENDING_MESSAGES=1024    # Mỗi producer; ví dụ thử nghiệm, không bảo đảm đủ
PENDING_BYTES=8388608    # Mỗi producer; 8 MiB, cần đủ theo kích thước payload
DEADLINE=1200            # Ngân sách thời gian toàn phiên (giây, >= DURATION + 60, khuyến nghị 1200)
RUN_LABEL="run01"        # Nhãn đợt chạy cụ thể để tránh xung đột thư mục lá khi đo lặp lại

WORK_DIR="$ROOT/official-s${SAMPLES}-r${OFFERED_RATE}-${RUN_LABEL}/A"
PHASE_ID="samples-${SAMPLES}"   # Luôn dùng 'samples-100' hoặc 'samples-1' theo chuẩn prepare_remote

mkdir -p "$ROOT" && chmod 0700 "$ROOT"
# Lưu ý: Thư mục lá $WORK_DIR TUYỆT ĐỐI KHÔNG tạo trước bằng mkdir, lệnh serve yêu cầu tự tạo mới!
```

**Trên Host B — Terminal B**:
```sh
ROOT=/var/tmp/opencode/p8-e2-official
OFFERED_RATE=1000        # Khớp với giá trị đã chọn trên Host A
SAMPLES=100              # Khớp với giá trị đã chọn trên Host A
RUN_LABEL="run01"        # Khớp với nhãn đợt chạy trên Host A

WORK_DIR="$ROOT/official-s${SAMPLES}-r${OFFERED_RATE}-${RUN_LABEL}/B"
mkdir -p "$WORK_DIR" && chmod 0700 "$WORK_DIR"
# Lưu ý: Thư mục $WORK_DIR/workspace TUYỆT ĐỐI KHÔNG tạo trước bằng mkdir, lệnh bind yêu cầu tự tạo mới!
```

---

###### Bước 1 — Tiền kiểm tra Kế hoạch và Ước lượng Tài nguyên với `--dry-run` (Terminal A1 hoặc A2)

Chạy kiểm tra tính hợp lệ của tham số, kế hoạch thực thi và mô hình ước lượng tài nguyên mà không khởi động container hay tạo mạng (truyền cùng `--deadline-seconds "$DEADLINE"` để đồng nhất kiểm tra ngân sách chu kỳ sống):

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$WORK_DIR" \
  --network-config /path/to/private-network.json \
  --deadline-seconds "$DEADLINE" \
  --dry-run \
  --profile official \
  --official-opt-in \
  --experiment E2 \
  --samples-per-message "$SAMPLES" \
  --e2-offered-rate "$OFFERED_RATE" \
  --e2-duration-seconds "$DURATION" \
  --e2-burst-messages "$BURST" \
  --e2-pending-messages "$PENDING_MESSAGES" \
  --e2-pending-bytes "$PENDING_BYTES"
```

Lệnh trên in ra cấu trúc JSON chứa `e2_official_plan` với `expected_counts`, `producer_rates_per_second` (A và B chia đều) và `estimate` (dự báo thận trọng về lưu trữ, bộ nhớ pending/retry và băng thông). Đảm bảo không có lỗi `e2_predicted_bound_exceeded` hay `paper_p8_e2_official_schedule_rejected`.

---

###### Bước 2 — Khởi động Server Fixture trên Host A (Terminal A1)

Khởi chạy tiến trình `serve` ở chế độ foreground và giữ terminal này trong suốt phiên làm việc:

```sh
python3 scripts/paper_p8_server.py serve \
  --root "$WORK_DIR" \
  --network-config /path/to/private-network.json \
  --deadline-seconds "$DEADLINE" \
  --profile official \
  --official-opt-in \
  --experiment E2 \
  --samples-per-message "$SAMPLES" \
  --e2-offered-rate "$OFFERED_RATE" \
  --e2-duration-seconds "$DURATION" \
  --e2-burst-messages "$BURST" \
  --e2-pending-messages "$PENDING_MESSAGES" \
  --e2-pending-bytes "$PENDING_BYTES"
```

Tiến trình tự động khởi động các container hạ tầng (Go backend, TimescaleDB, Mosquitto DynSec, Nginx), cấp phát 2 Gateways (`gw_p0_a_*`, `gw_p0_b_*`), sinh role manifests và xuất client bundle tại thư mục `$WORK_DIR/client-bundle/`. Nhật ký foreground được ghi nhận liên tục tại `$WORK_DIR/server.log`.

---

###### Bước 3 — Kiểm tra trạng thái READY và lấy Session ID trên Host A (Terminal A2)

Trên Terminal A2 của Host A, kiểm tra trạng thái của server:

```sh
python3 scripts/paper_p8_server.py status --root "$WORK_DIR" --experiment E2
```

Chờ cho đến khi server phản hồi:
- `preparation_status=E2_PREPARED`
- `phase=READY`
- `profile=official`
- `workload=steady_burst` (hoặc `e2-telemetry-ab-official`)

Lưu `session_id` nhận được vào biến môi trường trên Terminal A2 bằng cách trích xuất tự động từ tệp `session.json` của phiên:

```sh
SESSION_ID=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['session_id'])" "$WORK_DIR/fixture/private/session.json")
# Hoặc gán thủ công từ lệnh status: SESSION_ID="e4a2d810f92b..."
```

Đồng thời, ghi nhận mã băm SHA-256 của tập tin thực thi `paperload` do Host A chuẩn bị:

```sh
sha256sum "$WORK_DIR/fixture/private/bin/paperload"
```

---

###### Bước 4 — Chuyển giao gói bundle và binary sang Host B (Manual Transfer Step)

Người vận hành tự thực hiện sao chép qua kênh an toàn giữa Host A và Host B:
- **Gói bundle**: Sao chép thư mục `$WORK_DIR/client-bundle/` trên Host A sang `$WORK_DIR/received-bundle/` trên Host B.
- **Tập tin binary**: Sao chép tập tin thực thi `$WORK_DIR/fixture/private/bin/paperload` trên Host A sang `$WORK_DIR/paperload` trên Host B.

Sau khi sao chép, thiết lập phân quyền nghiêm ngặt trên Host B (lưu ý: thư mục `$WORK_DIR/workspace` tuyệt đối **không được tạo trước**):

```sh
chmod 0700 "$WORK_DIR/paperload"
find "$WORK_DIR/received-bundle" -type d -exec chmod 0700 {} +
find "$WORK_DIR/received-bundle" -type f -exec chmod 0600 {} +
```

Đối chiếu mã băm SHA-256 của `$WORK_DIR/paperload` trên Host B để đảm bảo trùng khớp 100% với mã băm đã ghi nhận tại Bước 3 trên Host A:

```sh
sha256sum "$WORK_DIR/paperload"
```

---

###### Bước 5 — Thiết lập Workspace và kiểm tra kết nối trên Host B (Terminal B)

Trên Terminal B của Host B, thực hiện liên kết bundle, tiền kiểm tra môi trường và kiểm tra kết nối mạng/xác thực thực tế:

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle "$WORK_DIR/received-bundle" \
  --workspace "$WORK_DIR/workspace"

python3 scripts/paper_p8_client.py preflight \
  --workspace "$WORK_DIR/workspace" \
  --binary "$WORK_DIR/paperload"

python3 scripts/paper_p8_client.py check-connection \
  --workspace "$WORK_DIR/workspace"
```

- Lệnh `bind` kiểm tra cấu trúc bundle, tính toàn vẹn chữ ký/thời hạn token và sinh cấu hình thực thi `runtime/execution/run-spec.json`.
- Lệnh `preflight` thực thi Go binary `paperload --preflight` để xác thực cú pháp manifest và quyền tập tin cục bộ; hoàn toàn không mở socket mạng. *(Lưu ý: hiện tại preflight ở profile official sẽ báo lỗi do client opt-in forwarding blocker như đã cảnh báo ở trên)*.
- Lệnh `check-connection` thăm dò thực tế kết nối HTTPS và MQTTS với credentials không đặc quyền trong bundle.
- Tuyệt đối không tiếp tục nếu `preflight` hoặc kết nối TLS/Auth/MQTT thất bại.

---

###### Bước 6 — Kích hoạt pha đo lường trên Host A (Terminal A2)

Trên Terminal A2 của Host A, gửi lệnh kích hoạt bắt đầu pha đo lường (sử dụng biến `$PHASE_ID`, tương ứng `samples-100` hoặc `samples-1`):

```sh
python3 scripts/paper_p8_server.py begin \
  --root "$WORK_DIR" \
  --experiment E2 \
  --session-id "$SESSION_ID" \
  --phase READY \
  --phase-id "$PHASE_ID"

python3 scripts/paper_p8_server.py status --root "$WORK_DIR" --experiment E2
```

Chờ cho đến khi lệnh `status` phản hồi xác nhận:
- `phase=MEASURING`
- `phase_id=$PHASE_ID` (ví dụ `samples-100` hoặc `samples-1`)
- `collector_ready=true`

Toàn bộ các số đếm cơ sở dữ liệu cơ sở (baseline DB counts) được Host A ghi nhận chính xác trước khi bộ thu thập chỉ số bắt đầu đo lường.

---

###### Bước 7 — Chạy tải phát tin và xuất kết quả trên Host B (Terminal B)

Trên Terminal B của Host B, tiến hành chạy tải phát tin mô phỏng và xuất kết quả đo lường:

```sh
python3 scripts/paper_p8_client.py run \
  --workspace "$WORK_DIR/workspace" \
  --binary "$WORK_DIR/paperload"

python3 scripts/paper_p8_client.py export-results \
  --workspace "$WORK_DIR/workspace" \
  --output "$WORK_DIR/exported-results"
```

Tiến trình `run` thực hiện phát tin theo kế hoạch `run-spec.json`:
- Arm A phát với tốc độ `offered_rate // 2` msg/s trong suốt `DURATION` giây.
- Arm B phát với tốc độ `offered_rate - (offered_rate // 2)` msg/s trong suốt `DURATION` giây kèm `BURST` tin nhắn dồn dập.
- Khi thời lượng phát hoàn tất, tiến trình client duy trì thời gian thoát cạn hữu hạn (30 giây) để backend tiếp nhận đầy đủ toàn bộ tin nhắn in-flight và nhận Application ACK trước khi kết thúc.
- Lệnh `export-results` xuất tập kết quả đo lường đã giới hạn (projection) tại thư mục `$WORK_DIR/exported-results/`.

---

###### Bước 8 — Chuyển giao kết quả đo về Host A (Manual Transfer Step)

Người vận hành tự thực hiện sao chép qua kênh an toàn giữa Host B và Host A:
- **Tập kết quả**: Sao chép nguyên vẹn thư mục kết quả `$WORK_DIR/exported-results/` trên Host B sang `$WORK_DIR/received-results/` trên Host A.
- Thiết lập phân quyền nghiêm ngặt trên Host A: thư mục `0700` và tập tin `0600`:

```sh
chmod 0700 "$WORK_DIR/received-results"
find "$WORK_DIR/received-results" -type f -exec chmod 0600 {} +
```

---

###### Bước 9 — Hậu kiểm dữ liệu và Hoàn tất phiên trên Host A (Terminal A2)

Trên Terminal A2 của Host A, nhập tập kết quả để hệ thống thực hiện đối chiếu tương quan với cơ sở dữ liệu và telemetry thực tế:

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root "$WORK_DIR" \
  --experiment E2 \
  --session-id "$SESSION_ID" \
  --phase MEASURING \
  --phase-id "$PHASE_ID" \
  --results "$WORK_DIR/received-results"

python3 scripts/paper_p8_server.py status --root "$WORK_DIR" --experiment E2
```

Chờ cho đến khi trạng thái chuyển sang `phase=VERIFYING`. Kiểm tra các trường xác thực trong `status`:
- `workload_outcome=VALIDATED`
- `imported_result.validation_valid=true`
- `imported_result.validation_failures=[]`
- Delta cơ sở dữ liệu đối soát khớp chính xác:
  - $\Delta \text{ledger} = \text{total\_unique}$
  - $\Delta \text{temporal} = \text{total\_unique}$
  - $\Delta \text{raw} = \text{total\_unique} \times \text{SAMPLES}$
  - $\Delta \text{configuration} = 0$

Sau khi kết quả được xác nhận hợp lệ, phát lệnh hoàn tất để dọn dẹp hạ tầng:

```sh
python3 scripts/paper_p8_server.py finalize \
  --root "$WORK_DIR" \
  --experiment E2 \
  --session-id "$SESSION_ID" \
  --phase VERIFYING
```

---

###### Bước 10 — Kiểm tra trạng thái CLOSED và Xác thực Dọn dẹp (Terminal A2)

Chờ cho đến khi tiến trình `serve` trên Terminal A1 kết thúc với mã thoát 0. Sau đó kiểm tra trạng thái cuối cùng trên Terminal A2:

```sh
python3 scripts/paper_p8_server.py status --root "$WORK_DIR" --experiment E2
```

Xác nhận các trường trạng thái:
- `phase=CLOSED`
- `cleanup_outcome=VERIFIED`

Báo cáo kết quả thử nghiệm đầy đủ được lưu trữ tại:
`$WORK_DIR/fixture/artifacts/p8-e2-samples-${SAMPLES}-remote-result.json`.

---

###### Bước 11 — Xử lý sự cố, Hủy phiên (`abort`) và Dọn dẹp lỗi (`cleanup`)

1. **Hủy phiên đang chạy dở dang (`abort`)**:
   Khi phát hiện lỗi ở bước client preflight, kết nối mạng hoặc lỗi xác thực import không thể cứu vãn, người vận hành phát lệnh hủy phiên trên Terminal A2 từ trạng thái hiện tại (trích xuất tự động từ `session.json` hoặc truyền tường minh `READY`, `MEASURING`, `VERIFYING`):
   ```sh
   CURRENT_PHASE=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1])).get('phase', 'READY'))" "$WORK_DIR/fixture/private/session.json")
   python3 scripts/paper_p8_server.py abort \
     --root "$WORK_DIR" \
     --experiment E2 \
     --session-id "$SESSION_ID" \
     --phase "$CURRENT_PHASE"
   ```

2. **Dọn dẹp tài nguyên khi Server bị gián đoạn/crash (`cleanup`)**:
   Trường hợp tiến trình `serve` trên Terminal A1 bị tắt đột ngột (ví dụ do mất điện, out-of-memory killer, hoặc bị gửi tín hiệu SIGKILL) khiến các container Docker còn sót lại, lấy mã dự án Compose (`project`, chuỗi định dạng `ncs_[a-f0-9]{12}`) ghi nhận trong tệp `session.json` (hoặc `server.log`), sau đó thực thi lệnh dọn dẹp thu hồi tài nguyên an toàn theo biên lai:
   ```sh
   PROJECT_ID=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1])).get('project', ''))" "$WORK_DIR/fixture/private/session.json")
   python3 scripts/paper_p8_server.py cleanup \
     --root "$WORK_DIR" \
     --project "$PROJECT_ID" \
     --experiment E2
   ```
   Lệnh trên chỉ thu hồi đúng các container, mạng và volume được xác thực bởi biên lai `resource-*.json` của đợt chạy này mà không thực hiện hành vi Docker compose down diện rộng.

---

#### 2.3.3 Ranh giới dữ liệu và Dẫn chiếu Báo cáo

- Các lần chạy thực tế theo chế độ `steady_burst` trước đây phát sinh retry tại Arm B do shaper can thiệp cắt tỉa gói tin burst (`rate_limited`), dẫn tới trạng thái `clean_comparison=false`.
- Các so sánh bị nhiễm (contaminated comparisons) không được coi là bằng chứng tăng tốc sạch (no clean speedup evidence).
- Tiến độ nghiệm thu, kết quả đo lường chi tiết và phân tích giới hạn của các đợt chạy đã được ghi nhận đầy đủ tại [Báo cáo P8 E2](paper-p8-experiments-and-acceptance.md#e2--hai-sample-profile-và-portable-lifecycle).

---

## 3. Bằng chứng thực nghiệm và lịch sử kiểm thử

> **Lưu ý điều hướng**: Tiến độ kiểm thử native, kết quả đo lường phái sinh (Derived Audit v2) và giới hạn nghiệm thu mới nhất đã được di chuyển và tập trung hóa tại [Báo cáo P8 E2](paper-p8-experiments-and-acceptance.md#e2--hai-sample-profile-và-portable-lifecycle). Phần dưới đây lưu giữ các bảng đối soát và lịch sử khắc phục sự cố nhằm phục vụ việc kiểm tra và đối chiếu vận hành; không sao chép thêm kết quả thực nghiệm mới vào runbook.

### 3.1 Bảng kết quả thực chạy chế độ Legacy `steady_burst` — 2026-10-30

| Profile | Ledger / raw / temporal / configuration | A/B generated = offered unique | App ACK A/B | Pending A/B cuối lượt | Phase overlap | Cleanup | Serve exit |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 samples/message | 24 / 2.400 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 3.414.891 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |
| 1 sample/message | 24 / 24 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 57.792 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |

Counters riêng B không đồng nghĩa với unique: profile 100 có B=41 publish attempts, 25 retries, 41 broker PUBACKs nhưng 16 unique messages / 16 application ACKs. A có 8 attempts / PUBACKs và 8 application ACKs. Cả hai lượt validation là `valid=true`, `failures=[]`; DB delta theo Gateway là A=8, B=16 ledger/temporal, raw=800/1.600 (profile 100) và raw=8/16 (profile 1).

Artifact roots ngày 2026-10-30:
- Profile 100: `/var/tmp/opencode/p8-e2-helper-100-attempt8/`
- Profile 1: `/var/tmp/opencode/p8-e2-helper-1-attempt1/`
- *Ghi chú thất bại Attempt 8*: Native `serve` trả về 0 và reports/cleanup hợp lệ; script bao ngoài ghi nhận FAIL sau khi server đóng do lỗi đường dẫn đọc workload trong bước tổng hợp. Evidence PASS riêng được dựng bằng cách đọc lại artifacts đã hoàn thành; không chạy lại hay sửa artifact FAIL lịch sử.

---

### 3.2 Bổ sung kiểm tra hồi quy Legacy `steady_burst` — 2026-10-31 (ES.7c)

Thực hiện kiểm tra hồi quy cho tải mặc định bị bỏ qua `--workload` qua hai lượt native độc lập:

#### 1. Attempt 1 (`es7c-20261031-2140`) — FAIL
- **Thực thi và dữ liệu data-plane**:
  - Arm A (Gateway `gw_p0_a_9baac00e`): 8 generated messages, 8 application ACKs, 0 retries, 0 pending, 800 raw samples.
  - Arm B (Gateway `gw_p0_b_04b1198e`): 16 generated messages (4 measure + 12 burst), 16 application ACKs, 27 shaper drops (rate-limited) kèm 27 retries, 0 pending, 1.600 raw samples.
  - Tổng số đếm cơ sở dữ liệu: 24 ledger, 2.400 raw, 24 temporal, 0 configuration.
  - Thoát cạn Ingress: InFlight = 0, BufferedPayloadBytes = 0.
  - Xác thực mẫu raw: `verified_exact = true`, `raw_identity_value_mismatches: 0` trên cả hai nhánh.
  - Dọn dẹp tài nguyên: VERIFIED (12 containers, 1 network, 5 volumes dọn sạch theo receipts).
- **Nguyên nhân thất bại**: `validate_e2_result` báo lỗi xác thực `database_delta_mismatch` (`workload_outcome: ABORTED`, `overall_status: FAIL`) do từ điển `delta` trong `finish_remote_collection` chứa thêm các alias key `'processed': 24, 'telemetry': 24` vi phạm phép so khớp từ điển nghiêm ngặt (strict dict equality `database_delta != expected_total`).
- **Bảo toàn**: Lượt chạy này được giữ nguyên nhãn FAIL tại `es7c-20261031-2140/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `3e4e99af...`), tuyệt đối không đổi nhãn thành PASS.

#### 2. Attempt 2 (`es7c-20261031-2215`) — PASS
- **Khắc phục**: Chuẩn hóa hàm `calculate_canonical_database_delta` chỉ trả về đúng 4 key canonical (`ledger`, `raw`, `temporal`, `configuration`), bảo toàn trình kiểm tra nghiêm ngặt `validate_e2_result` mà không hạ chuẩn kiểm định.
- **Thực thi và kết quả**:
  - Chạy native tại thư mục cô lập `/var/tmp/opencode/e2-equal-implementation/es7c-20261031-2215/`.
  - Kết quả đạt **`PASS`** toàn diện (`status: PASS`, `workload_outcome: VALIDATED`, `validation.valid: true`, `validation.failures: []`).
  - Arm A: 8 generated messages, 8 application ACKs, 0 retries, 0 pending, 800 raw samples.
  - Arm B: 16 generated messages (4 measure + 12 burst), 16 application ACKs, 26 shaper rate-limited drops kèm 26 retries, 0 pending, 1.600 raw samples.
  - Tổng số đếm cơ sở dữ liệu: 24 ledger, 2.400 raw, 24 temporal, 0 configuration.
  - Dọn dẹp tài nguyên: VERIFIED (12 containers, 1 network, 5 volumes — không có container sót lại).
  - Báo cáo kết quả: `es7c-20261031-2215/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `ecff1f98...`) và `startup-summary.json` (SHA256: `6c3ce928...`, `status: PAPER_P8_E2_WORKLOAD_VERIFIED`, `full_p8_accepted: false`).

---

### 3.3 Bảng kết quả thực chạy chế độ `equal_samples` — 2026-10-31 (ES.7b Attempt 4)

Chạy thực nghiệm tại thư mục `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/`:

| Profile | Ledger / raw / temporal / config | A/B generated = offered unique | Attempts A/B | Retries A/B | App ACK A/B | Pending A/B | Raw Verification (0..2.999) | Cleanup | Serve exit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **100 samples/msg** | 60 / 6.000 / 60 / 0 | 30 / 30 | 31 / 31 | 1 / 1 | 30 / 30 | 0 / 0 msgs, 0 B | 0 mismatch (`verified_exact=true`) | 0 orphaned — VERIFIED | 0 |
| **1 sample/msg** | 6.000 / 6.000 / 6.000 / 0 | 3.000 / 3.000 | 3.011 / 3.010 | 11 / 10 | 3.000 / 3.000 | 0 / 0 msgs, 0 B | 0 mismatch (`verified_exact=true`) | 0 orphaned — VERIFIED | 0 |

#### Xác thực dòng dữ liệu toàn vẹn (Full-Stream Raw Verification)
- Toàn bộ 6.000 dòng dữ liệu mẫu trong DB của mỗi profile được đối chiếu từng dòng với đặc tả logic: 0 value mismatch, 0 timestamp mismatch.
- Phân khoảng sequence logic khớp chính xác: Arm A kiểm tra dải [0..2.999], Arm B kiểm tra dải [0..2.999].
- `verified_exact: true` trên cả hai nhánh A và B.

#### Tính bất biến của Binary và Container Images
- **Tập tin thực thi `paperload`**: Mã băm SHA-256 trùng khớp tuyệt đối giữa S=100 và S=1:
  `5cbaf08a0dc23cfbc5e57dc7c03a6783cd9ed1409225d1216e887bd2a80a058a`.
- **Container Images**: Image IDs của 5 thành phần hạ tầng (`backend`, `gateway`, `postgres`, `mosquitto`, `nginx`) trùng khớp tuyệt đối giữa hai lượt chạy, xác nhận không có hành vi biên dịch lại (no rebuild) giữa các profile.

---

### 3.4 Lịch sử các lần thất bại (Attempts 1–3) và giải pháp khắc phục

Lịch sử thất bại được bảo toàn nguyên vẹn trong hồ sơ thử nghiệm:
1. **Attempt 1 (`es7b-pilot-20261031-1900`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Client Preflight với lỗi `P8 client error: PaperloadInputError: stop timeout must be between 0 and 60 seconds`.
   - *Nguyên nhân*: Với N=6.000, R=100, thời lượng tính toán là 90 giây (60s phát + 20s drain + 10s grace). `paper_p8_bundle.py` đã nới lỏng giới hạn lên 300s cho E2, nhưng `scripts/paper_p8.py` tại dòng 214 vẫn còn giữ kiểm tra cũ `stop_timeout > 60`.
   - *Khắc phục*: Cập nhật `scripts/paper_p8.py` cho phép `stop_timeout <= 300` riêng cho các manifest được nhận diện là `equal_samples` (`is_e2_equal_samples`), trong khi các workload legacy E2 khác tiếp tục giữ giới hạn 60s.
2. **Attempt 2 (`es7b-pilot-20261031-1950`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Server `finish-phase` khi import với lỗi `e2_source_manifest_inventory_mismatch`.
   - *Nguyên nhân*: Sai lệch đường dẫn ánh xạ inventory và bản sao manifest khi import kết quả từ client.
   - *Khắc phục*: Chuẩn hóa quy trình sao chép manifest nguồn và đối chiếu tập tin inventory.
3. **Attempt 3 (`es7b-pilot-20261031-2015`) — FAIL**:
   - *Hiện tượng*: Thất bại tại bước Server `finish-phase` trong phần hậu kiểm DB raw verification với lỗi `db_variable_invalid`.
   - *Nguyên nhân*: Hợp đồng gọi truy vấn psql không chấp nhận tham số số học chưa ép kiểu chuỗi an toàn.
   - *Khắc phục*: Bổ sung hàm kiểm tra ranh giới `_validate_decimal_int_param` trong `scripts/tests/paper_p8_e2_support.py` để kiểm tra kiểu và biên số học (chỉ nhận số nguyên dương, loại bỏ booleans/floats), sau đó ép kiểu thành chuỗi ký tự hợp lệ (`str(val)`) tại ranh giới gọi `native_compose_smoke.db_psql`, thỏa mãn hợp đồng chuỗi nghiêm ngặt của `db_psql`.
4. **Attempt 4 (`es7b-pilot-20261031-2035`) — PASS**:
   - Chạy native thành công trọn vẹn cả hai profile S=100 và S=1, số đếm DB chính xác tuyệt đối, cleanup VERIFIED.

---

### 3.5 Phân định giữa Source sinh kết quả Native và Postprocessor thẩm định

- **Native Producer Source**: Được biên dịch và thực thi tại revision git `8917bd9b7f58ef0173db373510b0566642cbb8a3` (trạng thái dirty).
- **Postprocessor thẩm định (Derived Audit v2)**: Thực hiện xử lý Python ở chế độ **read-only** trên các tập tin artifact gốc của Attempt 4, áp dụng các hàm đánh giá và chuẩn hóa đã sửa lỗi mà không chạy lại bất kỳ tải native hay thay đổi môi trường Docker nào.

---

### 3.6 Hiện tượng Contamination trên cả hai Profile

Cả hai profile trong Attempt 4 đều bị ghi nhận nhiễm (contaminated) do phát sinh retry:
- **Profile 100**: Arm A có 1 retry (31 attempts / 30 ACKs); Arm B có 1 retry (31 attempts / 30 ACKs).
- **Profile 1**: Arm A có 11 retries (3.011 attempts / 3.000 ACKs); Arm B có 10 retries (3.010 attempts / 3.000 ACKs).
- **Nguyên nhân**: Chưa được chứng minh bằng chứng cứ xác thực; nguyên nhân phát sinh retry là chưa biết (`unknown means unknown`). Các retry này không bị che giấu hoặc gọt bỏ để làm đẹp số liệu. Vì vậy, thuộc tính `clean_comparison` là `false` và trạng thái so sánh tổng thể là `CONTAMINATED`.

---

### 3.7 Diễn giải chỉ số theo Thẩm định Audit v2

Mọi diễn giải hiệu năng bắt buộc phải dựa trên **Derived Audit v2** (`/var/tmp/opencode/e2-equal-implementation/es7b-metric-audit-v2`), không sử dụng Audit v1 hoặc bảng so sánh thô ban đầu:

#### Các chỉ số CPU và Giao dịch DB là INCONCLUSIVE (Không thể kết luận)
1. **DB Transactions / sample: INCONCLUSIVE**
   - *Lý do*: `db_transactions_counter_not_retained_in_sampler`.
   - *Bản chất*: Dưới cơ chế Go microbatching, nhiều batch tin nhắn được gom và commit trong cùng một transaction SQL. Số lượng dòng ledger (`ledger_rows_per_sample`: 1.0 cho S1, 0.01 cho S100) không đồng nhất với số giao dịch cơ sở dữ liệu (`ledger != transactions`).
2. **Server Active CPU / sample: INCONCLUSIVE**
   - *Lý do*: `final_commit_boundary_not_observed`.
   - *Bản chất*: Chu kỳ lấy mẫu Docker stats kéo dài ~8 giây và kết thúc ở snapshot 8 trước khi ghi nhận ranh giới commit cuối cùng kèm bộ đếm tài nguyên gắn mốc thời gian.
3. **Server Lifecycle CPU: INCONCLUSIVE**
   - *Lý do*: `lifecycle_cpu_counters_unavailable` (terminal resources là null).
4. **Generator Active CPU / sample: INCONCLUSIVE**
   - *Lý do*: `invalid_delta_ticks_in_active_window:process_appeared`.
   - *Bản chất*: Cửa sổ active [0.023s..60.023s] trên trục thời gian monotonic của Host B chứa Sample 1 (lúc 1.002s) có `delta_ticks = None` do tiến trình con mới xuất hiện khi khởi động; không đủ điều kiện khép kín chuỗi delta ticks hợp lệ.
5. **Generator Lifecycle CPU: INCONCLUSIVE**
   - *Lý do*: `process_change_samples_lack_delta:process_appeared`.
   - *Chỉ số quan sát một phần*: Tổng delta có sẵn được lưu ở trường riêng biệt: `partial_observed_lifecycle_cpu_seconds` đạt 8,56s cho S=1 (0,001427 s/sample) so với 5,19s cho S=100 (0,000865 s/sample).
6. **Ratios CPU Generator**: `UNAVAILABLE`.

#### Phân biệt Offered Unique Bytes vs Backend Admitted Attempt Bytes
- Tỉ lệ dung lượng dữ liệu `bytes_per_sample` (100 / 1) đạt **`0.030783`** (AVAILABLE):
  - **S=100**: Offered unique bytes là 10,664 bytes/sample (tổng 63.984 bytes); Admitted attempt bytes là 11,082 bytes/sample (tổng 66.492 bytes, do có 62 attempts).
  - **S=1**: Offered unique bytes là 358,758 bytes/sample (tổng 2.152.548 bytes); Admitted attempt bytes là 360,007 bytes/sample (tổng 2.160.043 bytes, do có 6.021 attempts).
  - *Định tính*: Đây là dung lượng payload tầng ứng dụng được backend chấp nhận (`backend_admitted_attempt_bytes`), không phản ánh lưu lượng mạng thực tế (wire traffic) hay kích thước đóng gói TLS.

#### Không tuyên bố tăng tốc độ (Speedup) hay Profile chiến thắng
- Do `clean_comparison` là `false`, trường `headline_speedup` là **`UNAVAILABLE`**. Không có tuyên bố tỉ lệ phần trăm tăng tốc độ hay kết luận profile nào chiến thắng.
- Tuyệt đối không ngụy tạo số đo đỉnh (peak), traffic mạng (wire), hay độ trễ thu thập mẫu cảm biến (sensor acquisition latency).

---

### 3.8 Danh mục Checksum và Đường dẫn Artifacts

Các tập tin báo cáo thô ban đầu được bảo toàn nguyên vẹn với mã băm SHA-256 gốc:
- **Profile 1 Report (S=1)**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json`
  - SHA256: `497b4ca8783ba358fc74d3f8373fd4b522f1578acd51f2e485e6b5d7b8722fcf`
- **Profile 100 Report (S=100)**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json`
  - SHA256: `6504243dbea1df89acd489f3c86c1ef8f9d5aa9bd9cb062a459d4a5d2b985397`
- **Báo cáo so sánh gốc**:
  - Đường dẫn: `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/e2-comparison.json`
  - SHA256: `8c7180a3aac4aa3f4a1b033ba13ce2a33b46d17c3d058f039ff0a0dd9fd8924a`

Các tập tin thẩm định phái sinh Audit v2 tại `/var/tmp/opencode/e2-equal-implementation/es7b-metric-audit-v2/`:
- `audit-manifest.json`: SHA256 `10319f43ee19beed0041d73501187adf26e3b0ef388066e807a49474f9ee7d43`
- `p8-e2-equal-samples-1-derived-audit.json`: SHA256 `da2b72212f81a8d8a09c522bbb67bad881b70b0b489ae36b3a52b74c9858a278`
- `p8-e2-equal-samples-100-derived-audit.json`: SHA256 `d33626c789a2f0307ddf066f661b0e54be12e9d966f0acdf5887dd6ff5117e08`
- `e2-comparison-derived-audit.json`: SHA256 `484af5c86bc06c82b173a2a0baeef473de9e905400c01c61620f6739cf4c4d10`

Mục lục bằng chứng kiểm thử được lưu trữ tại:
[Chỉ mục Bằng chứng E2 equal_samples](results/p8/e2-equal-samples-20261031/index.md) và [Bản tóm tắt JSON](results/p8/e2-equal-samples-20261031/summary.json). Không sao chép các tệp nhật ký thô riêng tư vào tài liệu.

---

## 4. Giới hạn và trạng thái gate

- **Trạng thái cổng P8**: `full_p8_accepted=false`.
- **E2 Official — source-qualified native acceptance có giới hạn**: Luồng held-server/portable-client đã PASS cho profile S=100 và S=1 trong một lượt tải official hữu hạn; chi tiết accounting, source qualification và giới hạn diễn giải ở [báo cáo P8, checkpoint R1/R2](paper-p8-experiments-and-acceptance.md#checkpoint-r1r2--e2-official-portable-2026-11-06). Đây không phải equal-sample speedup/capacity claim. Các FAIL lịch sử vẫn giữ nguyên nhãn; full CI, HIL, physical A/B, soak và full P8 vẫn NOT RUN.
- **Giới hạn topo mạng**: `physical_ab=NOT RUN`. Thử nghiệm chạy trên cùng máy chủ cục bộ với metadata ghi nhận chuẩn tắc là `generator_colocated='unverified'` (định danh kiểm chuẩn `same-host/unverified`), phân biệt với các lượt legacy cũ từng ghi `generator_colocated=true`; chưa chứng minh được hành vi phân tán trên 2 máy vật lý độc lập qua mạng.
- **Các kịch bản chưa thực hiện**: Các bài kiểm tra lặp lại (3 cặp xen kẽ 100→1, 1→100, 100→1), bài đo năng lực chịu tải (capacity/soak), full CI, kiểm thử phần cứng HIL (AM5728) và triển khai thực tế (live rollout) đều là **NOT RUN**.
- **Phạm vi dừng**: E3–E5 là các phạm vi thử nghiệm lịch sử riêng biệt đã được triển khai và hoàn tất trước đó; nhiệm vụ này không thực hiện thay đổi hay chạy lại bất kỳ thử nghiệm nào của E3–E5 và bảo toàn toàn bộ kết quả đã nghiệm thu của chúng. Dừng lại tại mốc bàn giao E2 `equal_samples`; không tự động mở rộng hay chuyển sang các giai đoạn tiếp theo nếu chưa có yêu cầu mới.
