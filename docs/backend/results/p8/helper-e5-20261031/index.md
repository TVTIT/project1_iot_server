# Chỉ mục Bằng chứng E5 Helper Pilot Matrix (31/10/2026)

Tài liệu này ghi nhận chỉ mục bằng chứng xuất khẩu chính thức từ quy trình công cụ A/B helper CLI cho thí nghiệm **E5 Pilot Matrix** (1 arm tham chiếu `optimized` + 5 ablation arms theo thứ tự hạt giống: `ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`).

---

## 1. Tổng quan Trạng thái Chấp nhận (Acceptance Status)

- **Trạng thái tổng thể:** `PILOT_RUNS_COMPLETE_QUALIFIED`
- **Ranh giới tiêu chuẩn P8:** `full_p8_accepted: false`, `physical_ab: NOT RUN`, `generator_colocated: true`
- **Số lượt chạy độc lập:** 1 repeat (seed 805, phi chính thức; chưa phải kiểm thử dung lượng/soak lặp lại $\ge 3$ lần).
- **Kết quả:** 6/6 lượt chạy native đạt `status: PASS`, `workload_status: WORKLOAD_COMPLETE_PENDING_CLEANUP`, `paper_p8_e5_acceptance: true`, server exit code 0, holder exit code 0, thu dọn chính xác 12 containers / 1 network / 5 volumes per-run.
- **Tính tương thích so sánh:** 5/5 cặp đối chiếu đạt `comparability: COMPARABLE`, kích hoạt cơ chế `activation: ELIGIBLE`.

---

## 2. Danh mục Báo cáo Native Gốc (Original Native Reports)

Mỗi file báo cáo native thô được tạo lập tại môi trường scratch độc lập và bảo toàn băm SHA-256 bất biến:

| STT | Arm Thí nghiệm | Vai trò | Lệnh/Cờ sai biệt duy nhất | Report Path (Gốc) | SHA-256 Báo cáo Gốc |
|:---:|:---|:---|:---|:---|:---|
| 0 | `optimized` | Reference | Cấu hình tối ưu đầy đủ | `.../p8-e5-optimized.json` | `3f6617d0e80cd913ad399cd13bf86c73dd8efec98e7ce18af89909994cabb876` |
| 1 | `ram_dedup_off` | Ablation 1 | `MQTT_RAM_DEDUP_ENABLED=false` | `.../p8-e5-ram_dedup_off.json` | `1d0fece293e012ec5aa546062097438a32c4dceff6fe9066a4ea3d206aec0661` |
| 2 | `fanout_scan` | Ablation 2 | `WS_FANOUT_MODE=scan` | `.../p8-e5-fanout_scan.json` | `1ca3528b241396170c5d61f86455643b3c1dfbf6f6ec81f64236d86f89fdaee1` |
| 3 | `shaper_off` | Ablation 3 | `MQTT_SHAPER_ENABLED=false` | `.../p8-e5-shaper_off.json` | `a88d2c95c6be0cf049eaba14febf60a61445c25d54acb64d26f06a122969868b` |
| 4 | `microbatch_off` | Ablation 4 | `MQTT_MICROBATCH_ENABLED=false` | `.../p8-e5-microbatch_off.json` | `88ea478cfe0b2dc0c0feb7416139bab6d227e3b035109a8d9e0c65e9c02e881d` |
| 5 | `polling` | Ablation 5 | `STAGE5_WAKEUP_ENABLED=false` | `.../p8-e5-polling.json` | `01f58ccde39544221c50ca0d9f522abcd99feb5341eafe95fea43d96ee5f7473` |

---

## 3. Kết quả Đối chiếu Cặp (Pairwise Comparability & Activation)

Được xuất khẩu tự động bởi lệnh `python3 scripts/paper_p8_comparison.py e5-summary`:

| Cặp đối chiếu | Cờ kiểm thử cơ chế | Tương thích cấu hình | Kích hoạt cơ chế | Bằng chứng cơ chế thực tế |
|:---|:---|:---:|:---:|:---|
| `shaper_off` vs `optimized` | `MQTT_SHAPER_ENABLED` (false vs true) | `COMPARABLE` | `ELIGIBLE` | Reference giới hạn tốc độ 24 lần (`ShaperRateLimited: 24`), ablation nhận ngay 20 tin nhắn không nghẽn (`ShaperRateLimited: 0`). |
| `microbatch_off` vs `optimized` | `MQTT_MICROBATCH_ENABLED` (false vs true) | `COMPARABLE` | `ELIGIBLE` | Reference gom nhóm (`groups: 16, members: 20, max_members: 2`), ablation không gom nhóm (`groups: 0, members: 0, max_members: 0`). |
| `ram_dedup_off` vs `optimized` | `MQTT_RAM_DEDUP_ENABLED` (false vs true) | `COMPARABLE` | `ELIGIBLE` | Reference đạt 10 lần trúng RAM cache (`lookup_hit_total: 10`). |
| `polling` vs `optimized` | `STAGE5_WAKEUP_ENABLED` (false vs true) | `COMPARABLE` | `ELIGIBLE` | Reference nhận 4 tín hiệu đánh thức (`notification_signals_accepted: 4`), ablation 0 tín hiệu đánh thức (`scans: 17`, quét theo chu kỳ). |
| `fanout_scan` vs `optimized` | `WS_FANOUT_MODE` (scan vs indexed) | `COMPARABLE` | `ELIGIBLE` | Reference duyệt theo index (`indexed: 20, scan: 0`), ablation quét tuần tự (`indexed: 0, scan: 40`). |

---

## 4. Dữ liệu Đầu vào & Danh tính Cố định (Fixed Invariants)

- **Dataset & Context ghim:** Định danh logic `e5-inputs/repeat-01/context.json`, SHA-256 `2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175`, 35.684 bytes, seed 805.
- **Binaries ghim:**
  - `paperload`: SHA-256 `f562ba9bfacbd351718d0c8b81d1fe77c337864265e5d324f706ea69d95cc63f`
  - `gateway`: SHA-256 `3be849df6522f437e26bbc2d978b84b828b4712c8d2ef7e19ce184eda435c247`
- **Container Images tái sử dụng:** 100% image IDs từ reference `optimized` được tái sử dụng byte-for-byte (`backend: sha256:5df114...`, `gateway: sha256:448c7d...`, `postgres: sha256:289d55...`, `mosquitto: sha256:d12c8f...`, `nginx: sha256:516475...`, `supabase-*`), không rebuild bất kỳ image nào.
- **Mã nguồn thực thi native:** Revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`, `dirty_source_sha256`: `9a1e11276048f6e2c88bc14949bdae3a3c4b2c11b4c1d51cfe4fb9fa61f5eff0`.
- **Mã nguồn công cụ báo cáo sau sửa adapter:** Revision `8dc3c38865d12a7ca78c0cebf82b0d6c0c1e4244`, `dirty_source_sha256`: `f3bf86d35e8e94658579dc0dc0bf8393973f6969a30157641227e98c3e985457` (chỉ sửa adapter và unit tests báo cáo offline; không rerun native).

---

## 5. Bảo toàn Lịch sử Thất bại (Retained Historical Failures)

Tất cả 9 lượt native FAIL trước đó được giữ nguyên vẹn bằng chứng và nhãn FAIL, không bị thay đổi trạng thái:
1. `ncs_3cac71a0a9f9`: Thất bại quyền file CA staging (mode 0644).
2. `ncs_edd6d9d232e8`: Thất bại cấu hình TLS control manifest chứa nhầm MQTT fields.
3. `ncs_2f8a32822323`: Thất bại quyền dataset mode 0400 vi phạm kiểm tra private file của Go reader.
4. `ncs_3bfb1d720aa0`: Thất bại allowlist phase events regex thiếu tên phase E5.
5. `ncs_a26d33658c0b`: Thất bại kiểm tra trường chưa biết `timer_driven_gateway_workload` trong importer runner schema allowlist.
6. `ncs_56d52892e4c7`: Thất bại kiểm tra trạng thái holder sau initial checkpoint (READY -> PHASE).
7. `ncs_b4c2c0850f9d`: Thất bại ánh xạ SQLite executor projection (command ID vs idempotency key).
8. `ncs_42b3c8f56c50`: Thất bại assertion mẫu định danh viewer (1 sample/viewer vs 20 exhaustive) và overwrite artifact prepare-hold.
9. `ncs_531f234f72b1`: Thất bại export do snapshot outbox không được làm mới sau khi post-stop drain đã xả sạch outbox.

---

## 6. Các Tệp Xuất khẩu trong Thư mục Này

- `e5-summary.json`: Báo cáo tổng hợp JSON đã làm sạch (sanitized), ghi nhận đầy đủ siêu dữ liệu cấu hình, bộ đếm các pha và 5 cặp so sánh.
- `e5-runs.csv`: Dữ liệu bảng cho 6 lượt chạy (các bộ đếm DB, ingress delta, viewer và cơ chế).
- `e5-pairs.csv`: Dữ liệu bảng đối chiếu cho 5 cặp ablation so với optimized reference.
