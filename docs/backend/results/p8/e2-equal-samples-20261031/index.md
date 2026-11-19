# Chỉ mục Bằng chứng P8 E2 Equal Samples Workload (31/10/2026)

Tài liệu này ghi nhận chỉ mục bằng chứng thực nghiệm và xuất khẩu dữ liệu chính thức cho thử nghiệm **P8 E2 chế độ `equal_samples`** (so sánh đối xứng cùng khối lượng công việc giữa hai cấu hình batching: `samples_per_message=100` và `1`).

---

## 1. Tổng quan và Trạng thái Nghiệm thu (Acceptance Status)

- **Trạng thái nghiệm thu:** `ES.7_LOCAL_MVP_COMPLETE_QUALIFIED`
- **Ranh giới tiêu chuẩn P8:**
  - `full_p8_accepted: false`
  - `physical_ab: NOT RUN`
  - `generator_colocated: 'unverified'` (định danh kiểm chuẩn `same-host/unverified`)
- **Trạng thái so sánh đối chứng:** `status: CONTAMINATED`, `clean_comparison: false` (do xuất hiện retry trên cả hai profile trong Attempt 4).
- **Nguyên nhân retry:** Chưa được chứng minh bằng chứng cứ xác thực; nguyên nhân phát sinh retry là chưa biết (`unknown means unknown`), tuyệt đối không đưa ra giả định suy đoán.
- **Diễn giải chỉ số theo Thẩm định Audit v2:**
  - Toàn bộ chỉ số CPU (Server Active/Lifecycle, Generator Active/Lifecycle) và DB transactions/sample: **`INCONCLUSIVE`**.
  - Chỉ số dung lượng tải ứng dụng trên mỗi mẫu (`bytes_per_sample`): **`AVAILABLE`** (S100 = 11,082 bytes/sample vs S1 = 360,007 bytes/sample, tỉ lệ 100/1 = **`0.030783`**; phản ánh `backend_admitted_attempt_bytes`, không phản ánh lưu lượng mạng thực tế wire traffic hay TLS).
  - Tuyên bố tốc độ tăng tốc (`headline_speedup`): **`UNAVAILABLE`** (do lượt chạy bị nhiễm retry; không tuyên bố phần trăm cải thiện hay profile chiến thắng).
- **Kết luận Local MVP:** Cặp chạy native `equal_samples` Attempt 4 thành công về mặt chức năng kết hợp với lượt chạy hồi quy native legacy `steady_burst` Attempt 2 thành công hoàn tất phạm vi **ES.7 Local MVP**, không phải nghiệm thu P8 toàn diện hay chứng nhận môi trường sản xuất.

---

## 2. Ánh xạ Tiêu chí Chấp nhận (AC-01 .. AC-10 Mapping)

Bảng đối chiếu 10 tiêu chí chấp nhận được định nghĩa trong Kế hoạch thực hiện (`E2_workload_equal_samples_plan.md` Mục 10) với các bài kiểm tra tự động và bằng chứng native thực tế:

| Mã AC | Nội dung Tiêu chí | Kỳ vọng Kiểm chuẩn | Bằng chứng Thực nghiệm & Kết quả Native |
|:---:|:---|:---|:---|
| **AC-01** | Chạy E2 không truyền workload | Chế độ legacy 24-message steady/burst và kiểm định cũ được giữ nguyên; CLI của E3/E5 không bị ảnh hưởng | - Unit tests: `test_legacy_e2_pilot_manifests_regression` và `test_finish_remote_collection_legacy_steady_burst_exact_canonical_delta_pass` trong `test_paper_p8_e2_support.py`; `test_serve_e2_workload_omitted_is_steady_burst` và `test_p8_e2_cli_rejects_other_p8_lanes` trong `test_native_compose_smoke.py` PASS.<br>- Native legacy hồi quy: Attempt 1 tại 21:40 FAIL (27 retries trên Arm B, lỗi `database_delta_mismatch` do từ điển delta chứa extra alias keys); sau khi chuẩn hóa 4-key delta, Attempt 2 tại 22:15 PASS trọn vẹn (26 retries trên Arm B, DB delta 24/2.400/24/0, `imported_result.validation_valid: true`, `workload_outcome: VALIDATED`, cleanup VERIFIED 12/1/5). |
| **AC-02** | Tạo context default, sinh S=1/100 | Cùng 6.000 logical samples, 60s, expected 6.000/60 messages; counts từng producer logic đúng | - Lệnh CLI `paper_p8_server.py e2-context` tạo tập tin context chuẩn SHA256 `51d74bec...`.<br>- Unit tests `test_paper_p8_e2_support.py` kiểm chứng số học phân số hữu tỉ (rational arithmetic), sinh đúng 3.000 mẫu/producer (50 mẫu/s/producer) ở cả S=1 và S=100. |
| **AC-03** | Truyền N/R không hợp lệ hoặc vượt bound | Từ chối ngay trước khi cấp phát fixture; không tự động làm tròn hoặc cắt đuôi dữ liệu | - Table-driven tests trong `test_paper_p8_e2_support.py` từ chối: `sample_rate` lẻ, `total_samples` không chia hết cho 200, `duration` ngoài [10..120], NaN/float/âm, và overflow. |
| **AC-04** | Chạy stream qua ranh giới cuối / timer | Đếm chính xác số mẫu, không thêm/bớt tin nhắn cuối; chuỗi sequence, giá trị và timestamp đồng nhất | - Trình phát tải Go trong `src/internal/paperload/equal_samples.go` và `telemetry.go` bảo đảm tính xác định lịch phát và chuẩn hóa stream; các bài kiểm thử trong `src/internal/paperload/equal_samples_test.go` (`TestEqualSamplesDeterministicScheduleBoundaries`, `TestEqualSamplesNormalizedFullStreamIdentical`, `TestEqualSamplesByteExactRetryInvariants`) và `telemetry_test.go` PASS (chạy `-race`).<br>- Hậu kiểm SQL thực tế trên 6.000 mẫu của mỗi profile: 0 value mismatch, 0 timestamp mismatch. |
| **AC-05** | Bind bundle relocated, export/import | Context và schedule được xác thực, không phụ thuộc đường dẫn Host A; sai lệch băm bị từ chối | - `scripts/paper_p8_bundle.py` (`bind_bundle`) yêu cầu thư mục workspace chưa tồn tại và tạo mới với quyền 0700, kiểm chứng SHA-256 từng file.<br>- Tests `test_paper_p8_bundle.py` và `test_paper_p8_client.py` PASS. |
| **AC-06** | Native equal_samples hai profile | Raw = 6.000, số đếm theo scoped Gateway đúng, pending = 0, config = 0, không bypass auth, cleanup VERIFIED | - Lượt chạy native Attempt 4 (`es7b-pilot-20261031-2035`):<br>  * S=100: 60 ledger / 6.000 raw / 60 temporal / 0 config, 30/30 ACKs, pending=0, cleanup VERIFIED.<br>  * S=1: 6.000 ledger / 6.000 raw / 6.000 temporal / 0 config, 3.000/3.000 ACKs, pending=0, cleanup VERIFIED. |
| **AC-07** | Collector chờ operator rồi chạy 60s | Cửa sổ active giữ đủ dữ liệu hoặc đánh dấu unavailable; không dùng all-zero scrapes làm bằng chứng | - Bộ phân tích `infer_host_a_active_window` và generator active window trong Audit v2 đánh giá fail-closed thành `INCONCLUSIVE` do ranh giới commit cuối không quan sát được và tiến trình con xuất hiện lúc khởi động; không ngụy tạo số đo 0. |
| **AC-08** | Cặp chạy khác binary / image / context / flags | Báo cáo so sánh từ chối hoặc kết luận không tương thích có lý do; không thay đổi kết quả chức năng gốc | - Unit tests `test_paper_p8_e2_compare.py` từ chối đối chiếu khi lệch context seed, binary SHA256 hoặc container image IDs.<br>- Trong Attempt 4: binary `paperload` SHA-256 (`5cbaf08a...`) và 5 image IDs hạ tầng trùng khớp 100%. |
| **AC-09** | Hai profile đúng, chuẩn hóa chỉ số | Chuẩn hóa theo sample và đúng cửa sổ thời gian; không phát sinh tuyên bố sai về wire/latency/peak | - Audit v2 chuẩn hóa `bytes_per_sample` (11,082 vs 360,007 bytes/sample, tỉ lệ 0,030783).<br>- CPU và DB transactions per sample được giữ nguyên `INCONCLUSIVE`.<br>- Không ngụy tạo wire traffic hay peak latency; `headline_speedup` là `UNAVAILABLE`. |
| **AC-10** | Xử lý lỗi import, abort / finalize | Bằng chứng lỗi được giữ nguyên vẹn, collector dọn dẹp an toàn; tách biệt workload và cleanup | - Bảo toàn nguyên vẹn hồ sơ thất bại: Attempt 1 (stop timeout), Attempt 2 (inventory mismatch), Attempt 3 (psql contract).<br>- Cả hai profile của Attempt 4 và hai lần chạy legacy đều xác nhận exact cleanup thành công (0 container rò rỉ). |

---

## 3. Bảng Kết quả Chức năng Native — Attempt 4 (2026-10-31)

Dữ liệu thực chạy native tại thư mục kiểm thử độc lập `/var/tmp/opencode/e2-equal-implementation/es7b-pilot-20261031-2035/`:

| Đại lượng / Chỉ số | Profile S=100 (100 samples/msg) | Profile S=1 (1 sample/msg) | Tương thích / Đối chiếu |
|:---|---:|---:|:---:|
| **Workload Mode** | `equal_samples` | `equal_samples` | Giống nhau (đối xứng) |
| **Trạng thái chức năng** | `PASS` (`valid: true`, 0 failures) | `PASS` (`valid: true`, 0 failures) | Cả hai đạt chuẩn chức năng |
| **Tổng số mẫu quy định (N)** | 6.000 samples | 6.000 samples | Đồng nhất tuyệt đối |
| **Thời lượng phát quy định** | 60 giây | 60 giây | Đồng nhất tuyệt đối |
| **Tốc độ chào mẫu (R)** | 100 samples/s (50 samples/s/producer) | 100 samples/s (50 samples/s/producer) | Tốc độ chào mẫu bằng nhau |
| **Unique messages phát ra (offered)** | 60 messages (A=30, B=30) | 6.000 messages (A=3.000, B=3.000) | Đúng đặc tả batching |
| **Lượt gửi thực tế (Attempts)** | Arm A: 31, Arm B: 31 | Arm A: 3.011, Arm B: 3.010 | Ghi nhận lượt retry |
| **Lượt retry thực tế** | Arm A: 1, Arm B: 1 | Arm A: 11, Arm B: 10 | **Phát sinh retry (Contaminated)** |
| **Application ACKs nhận được** | Arm A: 30, Arm B: 30 (Tổng: 60) | Arm A: 3.000, Arm B: 3.000 (Tổng: 6.000) | 100% tin nhắn unique được ACK |
| **Tin nhắn tồn đọng (Pending)** | 0 messages, 0 bytes | 0 messages, 0 bytes | Thoát cạn hoàn toàn (drained) |
| **Số đếm CSDL: Ledger rows** | 60 hàng (A=30, B=30) | 6.000 hàng (A=3.000, B=3.000) | Khớp chính xác số unique messages |
| **Số đếm CSDL: Raw telemetry rows** | 6.000 hàng (A=3.000, B=3.000) | 6.000 hàng (A=3.000, B=3.000) | Khớp chính xác 6.000 mẫu |
| **Số đếm CSDL: Temporal values** | 60 hàng (A=30, B=30) | 6.000 hàng (A=3.000, B=3.000) | Khớp chính xác |
| **Số đếm CSDL: Configuration delta**| 0 | 0 | Không có đột biến cấu hình ngoài luồng |
| **Hậu kiểm SQL dữ liệu raw (0..2.999)**| 0 mismatch (`verified_exact: true`) | 0 mismatch (`verified_exact: true`) | Dòng dữ liệu mẫu trùng khớp 100% |
| **Thu dọn tài nguyên (Exact Cleanup)**| 12 containers, 1 network, 5 volumes | 12 containers, 1 network, 5 volumes | VERIFIED (0 container sót lại) |
| **Mã thoát tiến trình server (`serve`)**| 0 | 0 | Thoát bình thường |

---

## 4. Bảo toàn Lịch sử Thất bại (Preserved Historical Failures)

Tất cả các lần chạy native thất bại trong quá trình phát triển được lưu trữ nguyên vẹn bằng chứng và nhãn FAIL, không bị ghi đè hoặc chuyển thành PASS:

### 4.1 Lịch sử chế độ `equal_samples` (Attempts 1–3)

1. **Attempt 1 (`es7b-pilot-20261031-1900`) — FAIL**:
   - *Hiện tượng*: Thất bại tại tiền kiểm tra client (preflight) với lỗi: `PaperloadInputError: stop timeout must be between 0 and 60 seconds`.
   - *Nguyên nhân*: Với N=6.000, R=100, tổng thời lượng chạy tải là 90s (60s phát + 20s drain + 10s grace). `paper_p8_bundle.py` cho phép đến 300s cho E2, nhưng `scripts/paper_p8.py` dòng 214 còn kiểm tra cũ `stop_timeout > 60`.
   - *Khắc phục*: Cập nhật `scripts/paper_p8.py` cho phép `stop_timeout <= 300` riêng cho các manifest được nhận diện là `equal_samples` (`is_e2_equal_samples`), trong khi các workload legacy E2 khác tiếp tục giữ giới hạn 60s.
2. **Attempt 2 (`es7b-pilot-20261031-1950`) — FAIL**:
   - *Hiện tượng*: Thất bại tại Server `finish-phase` khi import kết quả với lỗi: `e2_source_manifest_inventory_mismatch`.
   - *Nguyên nhân*: Lệch đường dẫn ánh xạ inventory và bản sao manifest nguồn khi import kết quả từ client.
   - *Khắc phục*: Chuẩn hóa quy trình sao chép manifest nguồn và đối chiếu tập tin inventory.
3. **Attempt 3 (`es7b-pilot-20261031-2015`) — FAIL**:
   - *Hiện tượng*: Thất bại tại Server `finish-phase` trong phần hậu kiểm SQL dữ liệu raw với lỗi: `ValueError: db_variable_invalid`.
   - *Nguyên nhân*: Hàm `verify_e2_equal_samples_raw` truyền các giá trị số nguyên (`int`) trong từ điển `params` vào `native_compose_smoke.db_psql`, trong khi `db_psql` kiểm tra nghiêm ngặt `isinstance(value, str)` đối với mọi tham số `-v key=value`.
   - *Khắc phục*: Bổ sung hàm kiểm tra ranh giới `_validate_decimal_int_param` trong `scripts/tests/paper_p8_e2_support.py` để kiểm tra kiểu và biên số học (chỉ nhận số nguyên dương, loại bỏ booleans/floats), sau đó ép kiểu thành chuỗi ký tự hợp lệ (`str(val)`) tại ranh giới gọi `native_compose_smoke.db_psql`.

### 4.2 Lịch sử kiểm tra hồi quy Legacy `steady_burst` (ES.7c)

1. **Attempt 1 (`es7c-20261031-2140`) — FAIL**:
   - *Hiện tượng*: Workload chạy xong nhưng server báo lỗi xác thực: `database_delta_mismatch`, `workload_outcome: ABORTED`, `overall_status: FAIL`.
   - *Nguyên nhân*: Trong `finish_remote_collection`, từ điển `delta` chứa thêm các alias key `'processed': 24, 'telemetry': 24` vi phạm phép so khớp từ điển nghiêm ngặt (strict dict equality `database_delta != expected_total`) của `validate_e2_result`.
   - *Bảo toàn*: Lượt chạy này được giữ nguyên nhãn FAIL và lưu trữ tại `es7c-20261031-2140/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `3e4e99af...`).
2. **Attempt 2 (`es7c-20261031-2215`) — PASS**:
   - Sau khi áp dụng bản sửa `calculate_canonical_database_delta` (chuẩn hóa đúng 4 key canonical: `ledger`, `raw`, `temporal`, `configuration`), lượt chạy native hồi quy thứ hai đạt **PASS** toàn diện (`status: PASS`, `workload_outcome: VALIDATED`, `validation_valid: true`, 0 failures; Arm B có 26 rate-limited retries, cleanup VERIFIED 12/1/5).
   - Lưu trữ tại `es7c-20261031-2215/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` (SHA256: `ecff1f98...`).

---

## 5. Danh mục Báo cáo và Checksum SHA-256 Bất biến

Bảng đối chiếu đường dẫn và mã băm SHA-256 giữa các tập tin native thô gốc (Attempt 4) và các tập tin thẩm định phái sinh (Audit v2):

| Nhóm Báo cáo | Tên Tập tin / Định danh | Đường dẫn Lưu trữ / Gốc | Mã băm SHA-256 |
|:---|:---|:---|:---|
| **Attempt 4 Gốc** | Profile 100 Report | `.../es7b-pilot-20261031-2035/s100/A/fixture/artifacts/p8-e2-equal-samples-100-remote-result.json` | `6504243dbea1df89acd489f3c86c1ef8f9d5aa9bd9cb062a459d4a5d2b985397` |
| **Attempt 4 Gốc** | Profile 1 Report | `.../es7b-pilot-20261031-2035/s1/A/fixture/artifacts/p8-e2-equal-samples-1-remote-result.json` | `497b4ca8783ba358fc74d3f8373fd4b522f1578acd51f2e485e6b5d7b8722fcf` |
| **Attempt 4 Gốc** | So sánh Đối chứng | `.../es7b-pilot-20261031-2035/e2-comparison.json` | `8c7180a3aac4aa3f4a1b033ba13ce2a33b46d17c3d058f039ff0a0dd9fd8924a` |
| **Thẩm định v2** | Profile 100 Audit | `.../es7b-metric-audit-v2/p8-e2-equal-samples-100-derived-audit.json` | `d33626c789a2f0307ddf066f661b0e54be12e9d966f0acdf5887dd6ff5117e08` |
| **Thẩm định v2** | Profile 1 Audit | `.../es7b-metric-audit-v2/p8-e2-equal-samples-1-derived-audit.json` | `da2b72212f81a8d8a09c522bbb67bad881b70b0b489ae36b3a52b74c9858a278` |
| **Thẩm định v2** | So sánh Thẩm định | `.../es7b-metric-audit-v2/e2-comparison-derived-audit.json` | `484af5c86bc06c82b173a2a0baeef473de9e905400c01c61620f6739cf4c4d10` |
| **Thẩm định v2** | Bảng kê Thẩm định | `.../es7b-metric-audit-v2/audit-manifest.json` | `10319f43ee19beed0041d73501187adf26e3b0ef388066e807a49474f9ee7d43` |
| **Legacy Hồi quy**| Attempt 1 (FAIL) | `.../es7c-20261031-2140/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` | `3e4e99af684ce3fe10b2598b8c739d8d01a0f8c1497ae84ed33a2446add8818a` |
| **Legacy Hồi quy**| Attempt 2 (PASS) | `.../es7c-20261031-2215/s100/A/fixture/artifacts/p8-e2-samples-100-remote-result.json` | `ecff1f98b0c220ef58b4d6f1c672b68089bb19a4b449821b2317103af6af965c` |
| **Legacy Hồi quy**| Startup Summary (PASS)| `.../es7c-20261031-2215/s100/A/fixture/artifacts/startup-summary.json` | `6c3ce9286d61eca0968720c6b984736448ccdacd1aa83efab227ecffcf96a13e` |

### Tính bất biến của Binary và Container Images
- **Tập tin thực thi `paperload`**: SHA-256 trùng khớp tuyệt đối giữa S=100 và S=1:
  `5cbaf08a0dc23cfbc5e57dc7c03a6783cd9ed1409225d1216e887bd2a80a058a`.
- **Container Images**: Image IDs của 5 thành phần dịch vụ lõi (`backend`, `gateway`, `postgres`, `mosquitto`, `nginx`) trùng khớp tuyệt đối giữa hai profile, xác nhận việc tái sử dụng nguyên vẹn images (no rebuild).

---

## 6. Phân định Fingerprints giữa Native Producer và Audit Postprocessor

Để đảm bảo tính minh bạch tuyệt đối về nguồn gốc dữ liệu:
- **Native Producer Source**: Được thực thi tại revision git `8917bd9b7f58ef0173db373510b0566642cbb8a3` (trạng thái dirty). Chỉ số `HEAD+dirty` không đủ để chứng minh nguồn gốc chính xác; báo cáo gốc ghi nhận mã băm nguồn dirty xác thực là `dirty_source_sha256: ad75d3696deb2fac438e98d2bf7d8ffcb223a0df3e413318447ff816f85590a4` (nhánh `feat/paper-requirement`).
- **Audit Postprocessor (Thẩm định v2)**: Script `scripts/tests/paper_p8_e2_support.py` mang mã băm SHA-256 `d05f47a74b5ed788e965a20bc06902bad1fd66587da4a6b94e9234b80dbe7ac5` tại cùng revision dirty.
- **Ranh giới kiểm chuẩn**: Postprocessor hoạt động ở chế độ đọc thuần túy (read-only reassessment) trên các tập tin báo cáo thô của Attempt 4, áp dụng các chuẩn hóa tính toán mà không chạy lại bất kỳ tiến trình native hay môi trường Docker nào. Tuyệt đối không khẳng định rằng quá trình chạy native đã thực thi trên cây mã nguồn mới nhất.

---

## 7. Diễn giải Chỉ số Hiệu năng theo Thẩm định Audit v2

Mọi diễn giải hiệu năng bắt buộc phải tuân theo các kết luận trong **Derived Audit v2**:

1. **DB Transactions / sample: `INCONCLUSIVE`**
   - *Nguyên nhân*: `db_transactions_counter_not_retained_in_sampler`.
   - Dưới cơ chế Go microbatching, nhiều batch tin nhắn được gom và commit trong cùng một SQL transaction. Số lượng hàng ledger ghi nhận (`ledger_rows_per_sample`: 1.0 cho S1 vs 0.01 cho S100) không đồng nhất với số transaction (`ledger != transactions`). Do bộ đếm `confirmed_commits` không được giữ lại trong sampler rút gọn của Attempt 4, số giao dịch DB thực tế không thể kết luận.
2. **Server Active CPU / sample: `INCONCLUSIVE`**
   - *Nguyên nhân*: `final_commit_boundary_not_observed`.
   - Chu kỳ lấy mẫu Docker stats kéo dài ~8s và kết thúc ở snapshot 8 trước khi ghi nhận ranh giới commit cuối cùng kèm bộ đếm tài nguyên gắn mốc thời gian; không thể xác định chính xác cửa sổ tiêu thụ CPU active.
3. **Server Lifecycle CPU: `INCONCLUSIVE`**
   - *Nguyên nhân*: `lifecycle_cpu_counters_unavailable` (terminal resources là null).
4. **Generator Active CPU / sample: `INCONCLUSIVE`**
   - *Nguyên nhân*: `invalid_delta_ticks_in_active_window:process_appeared`.
   - Cửa sổ active trên trục thời gian monotonic của Host B chứa Sample 1 (lúc 1.002s) có `delta_ticks = None` do tiến trình con mới xuất hiện khi khởi động; không đủ điều kiện khép kín chuỗi delta ticks hợp lệ.
5. **Generator Lifecycle CPU: `INCONCLUSIVE`**
   - *Nguyên nhân*: `process_change_samples_lack_delta:process_appeared`.
   - Số liệu CPU quan sát một phần được tách riêng: `partial_observed_lifecycle_cpu_seconds` đạt 8,56s cho S=1 (0,001427 s/sample) so với 5,19s cho S=100 (0,000865 s/sample).
6. **Payload Bytes per Sample: `AVAILABLE`**
   - Tỉ lệ `bytes_per_sample` (100 / 1) đạt **`0.030783`** (S100 = 11,082 bytes/sample vs S1 = 360,007 bytes/sample). Đây là dung lượng payload tầng ứng dụng được backend chấp nhận (`backend_admitted_attempt_bytes`), không phản ánh lưu lượng mạng thực tế (wire traffic) hay kích thước đóng gói TLS.
7. **Headline Speedup: `UNAVAILABLE`**
   - Do cả hai profile đều bị nhiễm retry (`clean_comparison: false`, `status: CONTAMINATED`), trường `headline_speedup` là **`UNAVAILABLE`**. Không có tuyên bố tỉ lệ phần trăm tăng tốc độ hay kết luận profile nào chiến thắng.

---

## 8. Bằng chứng Kỹ thuật Tự động (Automated Technical Evidence)

Tập hợp kết quả kiểm tra tự động toàn diện trên cây mã nguồn hiện hành (current working tree):

### 8.1 Bộ Kiểm thử Python P8 (Discovery Run)
- **Lệnh thực thi chính thức:**
  ```sh
  PYTHONPATH=.:scripts:scripts/tests python3 -m unittest discover -s scripts/tests -p 'test_paper_p8*.py'
  ```
- **Kết quả:** **559 tests**, **2 skipped**, **0 failures**, **0 errors** (thời gian chạy: 19.806s).
- **Nhật ký lưu trữ:** `/var/tmp/opencode/e2-equal-implementation/final-p8-tests-qualified.log`.
- **Ghi nhận bài kiểm thử bỏ qua (skipped):** 2 bài kiểm thử native opt-in được đánh dấu skip theo thiết kế khi không có cờ môi trường native; tuyệt đối không cộng gộp các bài test này vào số lượng pass.
- **Bảo toàn lịch sử gọi lệnh ban đầu (Preserved Environment Invocation Error):** Lượt chạy thăm dò ban đầu của parent khi chưa truyền biến `PYTHONPATH` (`python3 -m unittest discover -s scripts/tests -p 'test_paper_p8*.py'`) phát sinh 2 lỗi import (`ModuleNotFoundError: No module named 'paper_p8'` trong `test_paper_p8.py` và `ModuleNotFoundError: No module named 'paper_p8_comparison'` trong `test_paper_p8_comparison.py`, nhật ký `/var/tmp/opencode/e2-equal-implementation/final-p8-tests.log`). Sau khi bổ sung đúng `PYTHONPATH=.:scripts:scripts/tests`, toàn bộ 559 tests vượt qua hoàn toàn. Sự cố này được bảo toàn trung thực dưới dạng lỗi cấu hình môi trường gọi lệnh (environment invocation failure), không phải lỗi kỹ thuật native của sản phẩm.

### 8.2 Các Cổng Kiểm tra Kỹ thuật Go (Go Technical Gates)
Cây mã nguồn Go hiện hành hoàn toàn bất biến so với mốc ES.7c delta fix (`es7c-delta-fix.txt`), tài liệu kế thừa nguyên vẹn bằng chứng kiểm thử đã ghi nhận mà không cần chạy lại:
- **Go Race Test (3 packages ảnh hưởng):**
  `cd src && go test -race -count=1 ./internal/paperload ./internal/gatewaysim ./cmd/paperload` đạt **PASS** (`paperload`: 19.261s, `gatewaysim`: 14.154s, `cmd/paperload`: 1.040s).
- **Định dạng mã nguồn (`gofmt`):** `cd src && gofmt -l .` đạt **PASS** (0 khác biệt định dạng).
- **Kiểm tra tĩnh (`go vet`):** `cd src && go vet ./...` đạt **PASS** (0 lỗi tĩnh).
- **Linter (`golangci-lint`):** `cd src && golangci-lint run --timeout=5m` đạt **PASS** (0 issues).
- **Biên dịch (`go build`):** `cd src && go build ./...` đạt **PASS** (biên dịch sạch).
- **Ranh giới số lượng test:** Tuyệt đối không cộng dồn các tổng số test chồng chéo giữa Go và Python hoặc giữa các bộ test con.

---

## 9. Giới hạn Kỹ thuật và Điểm dừng Bàn giao

- **An toàn dữ liệu**: Tài liệu không chứa thông tin nhạy cảm (secrets), mật khẩu, mã token JWT thực, địa chỉ IP mạng máy thật, hay đường dẫn thư mục cá nhân. Không sao chép các tệp nhật ký thô riêng tư vào tài liệu.
- **Ranh giới chưa thực hiện (NOT RUN)**:
  - Thử nghiệm trên 2 máy vật lý độc lập qua mạng vật lý (`physical_ab: NOT RUN`).
  - Các bài đo lặp lại nhiều lần ($\ge 3$ repeats).
  - Kiểm thử trên bo mạch phần cứng nhúng HIL (AM5728).
  - Kiểm thử tích hợp toàn diện trên CI từ xa (full remote CI).
  - Triển khai thực tế trên môi trường sản xuất (live rollout).
- **Ranh giới dừng**: Hoàn tất và dừng lại tại mốc bàn giao bằng chứng E2 `equal_samples` (ES.8b); không tự động mở rộng hay tiếp tục bất kỳ công việc nào khác nếu chưa có yêu cầu mới.
