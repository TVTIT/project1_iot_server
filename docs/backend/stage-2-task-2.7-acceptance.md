# Stage 2 — Task 2.7: Báo cáo Nghiệm thu E2E và Runbook Vận hành

> **Trạng thái tài liệu:**
> - **Nghiệm thu cục bộ (Local E2E Execution):** `PASS` (Hoàn tất kiểm chứng thực nghiệm 27/27 kịch bản E01–E27 qua 2 lần chạy liên tiếp trên harness cô lập, và 2 lượt chạy mô phỏng CI supervisor tăng cường đạt exit 0, 27/27 PASS, 8/8 selector PASS, cleanup verified).
> - **Thẩm duyệt CI (CI Review Status):** `APPROVED` (Reviewer độc lập đã phê duyệt sau khi toàn bộ các phát hiện mức High và Warning được khắc phục triệt để trên supervisor tăng cường; reviewer độc lập chạy 23 scoped tests / syntax / shell / diff đạt; worker chạy 111 all Python tests đạt; không gộp lẫn bằng chứng độc lập).
> - **Nghiệm thu CI từ xa (Remote CI Acceptance):** `PENDING` (Job `stage2-e2e` đã được triển khai trong `.github/workflows/ci.yml` là job thứ 10 với thời lượng 30 phút; thực thi thực tế trên hạ tầng GitHub Actions từ xa, upload artifact từ xa, và bằng chứng exact FINAL commit SHA vẫn giữ trạng thái `PENDING`. Mã commit cục bộ hiện hành `55cdcd5` còn chứa dirty uncommitted work nên KHÔNG phải final SHA acceptance; hủy thực tế trên GitHub NOT RUN; ShellCheck chưa chạy).
> - **Cổng sẵn sàng triển khai (Production Rollout Gates):** `PENDING / BLOCKED` (Các rào cản kỹ thuật tại Mục 7.2 vẫn giữ nguyên trạng thái chờ kiểm chứng trên hạ tầng thực tế; tuyệt đối không tuyên bố full production readiness).
>
> **Thời điểm thực nghiệm:** 2026-10-04 22:38:08 UTC – 22:41:57 UTC (chạy harness ban đầu) và 2026-10-05 06:07:00 UTC – 06:18:00 UTC (chạy mô phỏng CI supervisor cục bộ).  
> **Phạm vi:** Nghiệm thu tích hợp đầu-cuối (E2E) Giai đoạn 2 (Stage 2) của Backend IoT Platform.  
> **Bản chất bằng chứng:** Bằng chứng thực nghiệm gồm: (1) Nhật ký runner trực tiếp cục bộ (`/tmp/opencode/task27-e22-all-1a025d37cf17.log/.json` và `/tmp/opencode/task27-e22-all-8c4514fe9576.log/.json`); (2) Hai báo cáo CI supervisor cục bộ tăng cường đã qua thẩm duyệt (`/tmp/opencode/stage2-ci-reviewed-1791156194/run-1/safe/report.json` và `/tmp/opencode/stage2-ci-reviewed-1791156194/run-2/safe/report.json`). Cặp báo cáo trước đó (`stage2-ci-verified`) đã được thay thế (superseded) bởi bằng chứng supervisor tăng cường này, giữ nguyên ngữ cảnh lịch sử. Toàn bộ là tệp ephemeral cục bộ, không phải chứng nhận commit exact-SHA từ xa trên GitHub. Bản ghi nhật ký kiểm thử lịch sử bị nhiễm bẩn do preflight trước đây (historical polluted log) đã được thay thế (superseded), không xóa bỏ nhằm bảo đảm tính liên tục của lịch sử kiểm định.

---

## 1. Trạng thái Hiện tại và Ranh giới Nghiệm thu

### 1.1 Trạng thái baseline hiện hành
1. **Mã nguồn và CI Task 2.6**: Phân hệ Admin MQTT Credential Lifecycle API, DynSec Ingress Gate và PostgreSQL Transactional Operations đã hoàn thành nghiệm thu cục bộ tại [`docs/backend/stage-2-task-2.6-acceptance.md`](stage-2-task-2.6-acceptance.md).
2. **CI Pipeline và Thẩm duyệt Thống nhất (CI Review Status: APPROVED)**: Hệ thống CI trên GitHub hiện đã cấu hình job thứ 10 (`stage2-e2e`) trong `.github/workflows/ci.yml` (nâng tổng số lên **10 jobs** độc lập, `timeout-minutes: 30`).
   - **Kết quả thẩm định của CI Reviewer**: Reviewer độc lập đưa ra phán quyết **APPROVE** sau khi toàn bộ các phát hiện mức High và Warning được khắc phục triệt để:
     * Định danh phiên chạy đầy đủ do supervisor sinh (`run_id` 32 ký tự hex, `STAGE2_CI_RUN_ID`), tách biệt hoàn toàn với biến môi trường runner.
     * Thư mục fixture cục bộ chuẩn hóa (`base / 'fixture'`), cấm tuyệt đối symlink và đường dẫn tương đối.
     * Xác thực hợp đồng manifest (`owner.json`) trước khi thực hiện bất kỳ lệnh Docker nào (`validate_owner`).
     * Cơ chế dọn dẹp tài nguyên sở hữu sử dụng ID tài nguyên chính xác kết hợp đối soát nhãn sở hữu (`io.iot.stage2-ci-run=<run_id>`) và nhãn compose project (`com.docker.compose.project`), loại bỏ hoàn toàn cơ chế xóa theo chuỗi con (no substring deletion).
     * Ranh giới vận hành: Bộ dọn dẹp được thiết kế chuyên biệt cho **disposable GitHub-hosted runner**, không áp dụng trên shared production daemon.
     * Cổng gating nghiêm ngặt: Yêu cầu bắt buộc 8 OBSERVED selector PASS và 27 unique scenarios (`SCENARIO E01..E27: PASS`), giữ nguyên trạng thái `NOT RUN` và từ chối/báo lỗi trạng thái `UNKNOWN`.
     * Thu thập provenance thực tế: HEAD commit, trạng thái dirty của worktree, ngữ cảnh thực thi (`local` / `github_actions`), event SHA, và digest SHA256 của các container image đã ghim.
     * Bounded optional metadata và báo cáo safe allowlisted được bảo vệ nguyên tử: ghi tệp tạm quyền `0600`, gọi `flush()`, `os.fsync()`, và thay thế nguyên tử `os.replace()` (lưu ý kỹ thuật: không tuyên bố độ bền bỉ thư mục khi mất điện nguồn / no power-loss directory durability claim).
   - **Bằng chứng thực nghiệm mới nhất của Worker**: Hai lượt chạy mô phỏng CI supervisor cục bộ tăng cường (`/tmp/opencode/stage2-ci-reviewed-1791156194/run-1/safe/report.json` và `run-2/safe/report.json`) đạt `exit 0`, 27 PASS, 8 selector PASS, `cleanup_verified: true`, thời gian lần lượt là 104.02 giây và 115.02 giây. Cặp báo cáo `stage2-ci-verified` trước đây được thay thế (superseded).
   - **Kiểm định độc lập**: Reviewer độc lập đã trực tiếp chạy 23 scoped tests / syntax / shell / diff đều đạt; worker đã chạy 111 all Python tests đều đạt (không gộp lẫn bằng chứng kiểm tra độc lập giữa reviewer và worker). Các kịch bản native không cần chạy lại cho các bản sửa lỗi chỉ liên quan đến manifest/parser.
   - **Ranh giới nghiệm thu còn lại**: Việc thực thi thực tế trên GitHub Actions từ xa, upload artifact từ xa, và xác nhận exact FINAL commit SHA vẫn ở trạng thái `PENDING`. Commit HEAD cục bộ hiện hành (`55cdcd5`) còn chứa các thay đổi chưa commit (dirty worktree) nên KHÔNG phải là final SHA acceptance. Kiểm chứng hủy thực tế trên GitHub (actual cancellation on GitHub) chưa thực hiện (`NOT RUN`), và ShellCheck chưa chạy. Không đưa ra tuyên bố sẵn sàng sản xuất toàn diện (no full production readiness claim). Worker không tự động dispatch, commit hoặc push lên remote.
3. **Công cụ Vận hành Membership**: Kế hoạch vận hành phân quyền thành viên Gateway đã được triển khai chính thức tại `scripts/manage-gateway-membership.sh`, tích hợp kiểm toán journal bền vững và đã qua kiểm chứng thực tế trong kịch bản E11.
4. **E2E Runner và Kết quả Thực nghiệm**: Kịch bản runner (`scripts/test-stage2-e2e.sh`, `scripts/tests/stage2_e2e.py`) đã hoàn thành triển khai và thực thi thành công toàn bộ 27 kịch bản (E01–E27) qua 8 bộ chọn tên độc lập. Kết quả đã được đối soát độc lập qua 2 lượt chạy liên tiếp thoát mã 0 (`exit 0`), không lỗi traceback, 1 bảng tổng kết (summary) duy nhất mỗi lần chạy, và dọn dẹp sạch sẽ tài nguyên sau thực thi (`post_cleanup_scan: true`).

### 1.2 Ranh giới phạm vi Task 2.7
- **Thuộc phạm vi**:
  - Tích hợp chuỗi xác thực và ủy quyền đầy đủ: Supabase Auth (GoTrue) → Nginx Reverse Proxy → Supabase API Gateway (Envoy) → Go Business API (`cmd/server`) → PostgreSQL (TimescaleDB) → Mosquitto DynSec Ingress Gate → Gated MQTT TLS.
  - Kiểm chứng 6 vai trò tài khoản định danh độc lập, bảo đảm nguyên tắc cách ly tài nguyên tuyệt đối giữa các Gateway.
  - Kiểm chứng 27 kịch bản đầu-cuối (E01–E27) qua 8 bộ chọn tên (named selectors).
  - Quy trình vận hành membership có bảo vệ (protected operator tooling) dành cho Platform Admin.
  - Kiểm chứng diễn tập nâng cấp cơ sở dữ liệu trên volume bền vững (persistent volume).
- **Ngoài phạm vi (chuyển giao giai đoạn tiếp theo)**:
  - Xử lý lưu trữ dữ liệu đo lường thời gian thực (telemetry persistence), application-level ACK cho chuỗi mẫu đo.
  - Truy vấn dữ liệu lịch sử (`/v1/telemetry/history`), kết nối WebSocket (`/v1/ws`), và truy vấn Digital Twin state (`/v1/digital-twins`). Hiện các route này giữ nguyên trạng thái stub (`501 Not Implemented`).
  - Tải lên tệp đa phương tiện (Supabase Storage upload / signed URL).
  - Cơ chế ra lệnh điều khiển thiết bị (Digital Twin downlink commands / desired state reconciliation).
  - Đăng ký công khai (public self-signup), tự nhận Gateway (self-claiming), chuyển nhượng quyền sở hữu Gateway (ownership transfer), và giao diện Web Admin.

---

## 2. Chính sách Tài khoản Định danh và An toàn Bí mật

### 2.1 Ma trận tài khoản định danh E2E
Mỗi tài khoản đóng một vai trò chuyên biệt để chứng minh tính phân quyền và cách ly tài nguyên. Toàn bộ mật khẩu được quản trị động trong môi trường fixture cô lập; **tuyệt đối không in, không ghi log và không lưu trữ giá trị mật khẩu hay token trong tài liệu này**.

| Email đăng nhập | Tên vai trò kiểm thử | Quyền trong CSDL (`platform_admins` & `user_gateways`) | Mục đích kiểm chứng E2E |
|---|---|---|---|
| `admin@example.com` | Platform Admin | Có trong `platform_admins`; không gán membership Gateway | Quản trị nền tảng: tạo Gateway/Sensor, cấp/xoay/thu hồi MQTT credentials. Gọi `GET /v1/gateways` nhận `items: []` (không bypass quyền đọc nghiệp vụ). |
| `owner_a@example.com` | Owner A (Chủ GW A) | Không có trong `platform_admins`; `user_gateways`: role = `'owner'` trên Gateway A | Chỉ xem được Gateway A và Sensor của A; không thấy Gateway B; gọi API `/v1/admin/*` bị chặn `403 Forbidden`. |
| `owner_b@example.com` | Owner B (Chủ GW B) | Không có trong `platform_admins`; `user_gateways`: role = `'owner'` trên Gateway B | Chỉ xem được Gateway B; chứng minh cách ly hai chiều giữa Owner A và Owner B. |
| `operator@example.com` | Operator User | Không có trong `platform_admins`; được gán role = `'operator'` trên Gateway A | Kiểm chứng cấp quyền động với cùng một token JWT; gọi API xem được Gateway A; gọi API `/v1/admin/*` bị chặn `403 Forbidden`. |
| `viewer@example.com` | Viewer User | Không có trong `platform_admins`; được gán role = `'viewer'` trên Gateway A | Kiểm chứng quyền chỉ đọc; kiểm chứng hạ cấp/nâng cấp role (Viewer ↔ Operator) và thu hồi quyền (sau khi revoke, token cũ lập tức nhận `404 Not Found`). |
| `non_member@example.com` | Non-member User | Không có trong `platform_admins`; không có dòng nào trong `user_gateways` | Người dùng hợp lệ nhưng chưa được gán Gateway: `GET /v1/gateways` trả `200` với `items: []`; đoán `gateway_id` trả `404`; gọi API Admin bị chặn `403`. |

### 2.2 Quy định an toàn thông tin (Secrecy & Redaction Guidelines)
- **Tạo tài khoản và bí mật**: Mật khẩu kiểm thử định danh được sinh ngẫu nhiên khi khởi tạo và đưa vào tiến trình thông qua biến môi trường được bảo vệ `STAGE2_TEST_PASSWORD`. Trong CI supervisor (`scripts/tests/stage2_ci.py`), mỗi lượt chạy tự sinh một mật khẩu fixture ngẫu nhiên riêng biệt (`secrets.token_urlsafe(36) + 'Aa1!'`), chỉ truyền qua biến môi trường (runtime random fixture secret env-only) và được che mờ tự động trên GitHub Actions qua lệnh `::add-mask::`. Tuyệt đối không hardcode giá trị này trong mã nguồn, kịch bản, hay tài liệu.
- **Tuyệt đối không rò rỉ**: Không truyền token, password hoặc private key qua đối số dòng lệnh (`argv`), biến môi trường toàn cục công khai, hoặc log chuẩn (`stdout`/`stderr`).
- **Lưu trữ artifact quét an toàn (Allowlisted Scanned Artifacts Only)**: Trong CI, các tệp log thô (`private.log`, `private-console.log`) bị xóa bỏ hoàn toàn (`unlink`) sau khi hoàn tất. Chỉ có tệp `safe/report.json` được phép xuất xưởng làm artifact. Trước khi ghi, báo cáo này phải vượt qua bộ lọc quét bảo mật nghiêm ngặt: nếu phát hiện mật khẩu fixture, `Bearer`, `PRIVATE KEY`, hoặc tiền tố JWT `eyJ`, tiến trình lập tức ném ngoại lệ hủy bỏ (`RuntimeError('artifact rejected')`). Quá trình ghi tệp áp dụng kỹ thuật nguyên tử: ghi ra tệp tạm quyền `0600` (`safe/report.tmp`), gọi `flush()` và `os.fsync(fileno)` để bảo đảm dữ liệu được đẩy xuống đĩa trước khi tráo đổi nguyên tử qua `os.replace(temporary, safe / 'report.json')`. Lưu ý kỹ thuật: cơ chế này bảo vệ tính toàn vẹn của tệp trước lỗi ứng dụng cục bộ, không tự nhận khả năng chống chịu mất điện đột ngột của toàn bộ thư mục (no power-loss directory durability claim).
- **Không ghi nhận secret**: Báo cáo nghiệm thu và artifact chỉ chứa kết quả Pass/Fail, thời gian thực thi, số lượng assertions, mã lỗi chẩn đoán không chứa chuỗi nhạy cảm, và log đã được che mờ (redacted).
- **Vòng đời secret trong bộ nhớ**: Mọi plaintext secret trong các kịch bản kiểm chứng (như E22 response-loss relay) chỉ được lưu giữ tạm thời trong bộ nhớ tiến trình để phục vụ oracle xác minh kết nối và bị giải phóng ngay sau đó.

---

## 3. Chính sách Phân quyền Vận hành Thành viên (Protected Membership Policy)

Căn cứ Mục 6.5 của `AGENTS.md` và Mục 2.7.1 của kế hoạch nguồn:

1. **Thẩm quyền Quản trị Nền tảng**:
   - Chỉ có người vận hành hạ tầng tin cậy (Platform Administrator) có bản ghi trong bảng `platform_admins` mới có quyền thực hiện gán, thay đổi, hoặc thu hồi tư cách thành viên Gateway.
   - Thao tác này được thực hiện qua công cụ vận hành nội bộ được bảo vệ (`scripts/manage-gateway-membership.sh`), sử dụng kết nối CSDL có thẩm quyền quản trị. Tuyệt đối không mở endpoint HTTP public cho việc quản lý membership.
2. **Nguyên tắc Phân quyền Vai trò (`user_gateways`)**:
   - Bảng `user_gateways` áp dụng ràng buộc nghiêm ngặt: `CHECK (role IN ('owner', 'operator', 'viewer'))`.
   - `owner`: Có toàn quyền đọc dữ liệu telemetry, trạng thái, và quản trị cấu hình cảm biến thuộc Gateway của mình. Không được tự ý gán quyền cho người dùng khác hoặc chuyển giao quyền sở hữu.
   - `operator`: Trong Giai đoạn 2 (Stage 2), vai trò `operator` tuân thủ nguyên tắc **fail-closed** (hoạt động như quyền chỉ đọc). Không được phép ghi đè cấu hình, sửa đổi cảm biến, hay can thiệp quản trị. Khi phân hệ điều khiển Digital Twin được triển khai ở giai đoạn sau, `operator` mới được cấp quyền thực thi các lệnh vận hành trong allowlist an toàn.
   - `viewer`: Hoàn toàn chỉ đọc (read-only). Mọi yêu cầu ghi hoặc thao tác quản trị đều bị từ chối.
3. **Bảo vệ Tính Toàn vẹn Quyền Sở hữu**:
   - Công cụ vận hành membership chỉ cho phép cấp hoặc điều chỉnh vai trò `operator` và `viewer`, hoặc thu hồi thành viên không phải chủ sở hữu (non-owner).
   - Tuyệt đối không cho phép sửa đổi, hạ cấp hoặc xóa tài khoản `owner` ban đầu qua công cụ chia sẻ quyền này.
4. **Nhật ký Kiểm toán Bắt buộc (Protected Audit Journal)**:
   - Mọi thao tác thay đổi membership phải ghi nhận journal gồm: `operator_id`, `operation_id`, `gateway_id`, `target_user_id`, `action`, `role_before`, `role_after`, `timestamp_utc`, và `outcome`.
   - Journal tuyệt đối không chứa mật khẩu, token hay thông tin nhạy cảm. Thao tác và journal phải thực thi trong cùng một transaction CSDL. Nếu transaction gặp sự cố, hệ thống phải rollback toàn bộ và báo lỗi, không tự ý suy diễn thành công.
5. **Hiệu lực Tức thì với Token Đang Sử dụng**:
   - Không áp dụng cơ chế lưu quyền tĩnh trong JWT. Mọi yêu cầu nghiệp vụ tới Go backend đều đối soát trực tiếp với bảng `user_gateways` trong PostgreSQL.
   - Khi một người dùng được cấp quyền, đổi vai trò, hoặc bị thu hồi quyền, các yêu cầu HTTP kế tiếp sử dụng cùng một access token JWT hợp lệ phải phản ánh ngay lập tức trạng thái quyền mới (cấp quyền → thấy tài nguyên; thu hồi → nhận `404 Not Found`).

---

## 4. Tóm tắt Kịch bản E01–E27 theo 8 Bộ chọn (Grouped Selectors)

Toàn bộ 27 kịch bản E2E được phân bổ thành 8 bộ chọn tên (named selectors) độc lập theo đúng kiến trúc của bộ runner `scripts/tests/stage2_e2e.py`:

```text
┌────────────────────────────────────────────────────────────────────────┐
│ Bộ chọn 1: accounts/auth       │ Kịch bản: E01, E02, E03, E04, E05, E06 │ (6)
│ Bộ chọn 2: provisioning        │ Kịch bản: E07, E08, E09                │ (3)
│ Bộ chọn 3: memberships/roles   │ Kịch bản: E10, E11, E12, E13           │ (4)
│ Bộ chọn 4: credentials/ACL     │ Kịch bản: E14, E15, E16, E17           │ (4)
│ Bộ chọn 5: rotate/revoke/replay│ Kịch bản: E18, E19, E20, E21, E22      │ (5)
│ Bộ chọn 6: loss/recovery       │ Kịch bản: E23, E24, E25                │ (3)
│ Bộ chọn 7: restart/fences      │ Kịch bản: E26                          │ (1)
│ Bộ chọn 8: upgrade/repeatability│ Kịch bản: E27                         │ (1)
└────────────────────────────────────────────────────────────────────────┘
```

### Chi tiết các Kịch bản và Kết quả Thực nghiệm

| Mã | Bộ chọn | Nội dung kịch bản kiểm thử | Kết quả kỳ vọng và Kết quả Thực nghiệm (Observed Assertions) | Trạng thái |
|---|---|---|---|:---:|
| **E01** | `accounts/auth` | Khởi tạo schema CSDL v16 và kiểm tra kết nối role | Backend kết nối qua role `iot_backend_app`, không sở hữu quyền superuser/DDL hay quyền xóa nhật ký audit. | `PASS (Local)` |
| **E02** | `accounts/auth` | Đăng ký tài khoản công khai (Anonymous signup) | Trả về `422 signup_disabled` qua cả Nginx proxy và API Gateway nội bộ; không tạo bản ghi `auth.users` hay `profiles`. | `PASS (Local)` |
| **E03** | `accounts/auth` | Tạo 6 tài khoản định danh qua Auth Admin API | Trả về `200 OK`, sinh UUID hợp lệ và bản ghi profile tương ứng; tài khoản ban đầu chưa có quyền admin hay membership. | `PASS (Local)` |
| **E04** | `accounts/auth` | Bootstrap Platform Admin đầu tiên | Thành công với token/khóa hợp lệ; thử nghiệm bootstrap trùng lặp hoặc trái phép bị từ chối; không tự gán quyền Gateway. | `PASS (Local)` |
| **E05** | `accounts/auth` | Đăng nhập và làm mới token (Login & Token Refresh) | Trả về `200 OK` với JWT thật mang issuer/audience/role hợp lệ; token người dùng gọi Auth Admin API bị từ chối `403`. | `PASS (Local)` |
| **E06** | `accounts/auth` | Kiểm tra token sai lệch, giả mạo, hoặc hết hạn | Trả về `401 Unauthorized` trên toàn bộ các route có bảo vệ; không làm thay đổi trạng thái hệ thống. | `PASS (Local)` |
| **E07** | `provisioning` | Platform Admin tạo Gateway A, B và Sensor tương ứng | Trả về `201 Created`; thiết lập chính xác URN, quan hệ `hasSensor`, trạng thái Digital Twin rỗng ban đầu, version bằng 0. | `PASS (Local)` |
| **E08** | `provisioning` | Xử lý yêu cầu PUT trùng lặp, sai định dạng, xung đột | Replay đúng nội dung trả về `200 OK`; khác payload trả về `409 Conflict`; định dạng sai trả về `400/413/415`; không có mutation thừa. | `PASS (Local)` |
| **E09** | `provisioning` | Rollback giao dịch khi gặp sự cố SQL trigger | Nhận phản hồi `500 Internal Error`; toàn bộ thực thể Gateway/Sensor/Twin liên quan được rollback sạch sẽ, không để lại bản ghi mồ côi. | `PASS (Local)` |
| **E10** | `memberships/roles` | Đọc danh sách Gateway và Sensor theo quyền hạn | Owner A/B chỉ thấy tài nguyên của mình; Admin không gán quyền và Non-member nhận danh sách rỗng (`items: []`); truy vấn Gateway lạ trả về `404 Not Found`. | `PASS (Local)` |
| **E11** | `memberships/roles` | Cấp quyền động, đổi vai trò, và thu hồi quyền | Thực hiện qua `scripts/manage-gateway-membership.sh`: sau khi cấp quyền, cùng JWT cũ đọc được tài nguyên; sau khi thu hồi, lập tức nhận `404 Not Found`. | `PASS (Local)` |
| **E12** | `memberships/roles` | Người dùng thường gọi API quản trị và API credentials | Các vai trò Owner, Operator, Viewer, Non-member gọi 2 route PUT Admin và 4 route Credential đều nhận `403 Forbidden`; zero mutation. | `PASS (Local)` |
| **E13** | `memberships/roles` | Chống giả mạo danh tính (Anti-impersonation) | Truyền tham số giả mạo trong body/query, gateway_id lạ, hoặc ID dành riêng đều bị từ chối (`400/404`); target strictness enforced. | `PASS (Local)` |
| **E14** | `credentials/ACL` | Cấp mới MQTT credential và kết nối TLS | Admin POST nhận `201 Created` kèm mật khẩu Base64URL 43 ký tự một lần; GET trả về metadata không chứa secret/hash; Gateway kết nối TLS thành công. | `PASS (Local)` |
| **E15** | `credentials/ACL` | Kiểm tra tính lũy đẳng (Idempotency) khi cấp mới | Replay cùng khóa trả về `201 Created` chỉ chứa metadata; đổi payload với cùng khóa trả về `409`; yêu cầu sai định dạng trả về `400/413`. | `PASS (Local)` |
| **E16** | `credentials/ACL` | Cách ly không gian Topic Mosquitto qua DynSec | Gateway chỉ được publish/subscribe trên topic của chính mình; cố tình publish sang Gateway khác hoặc topic điều khiển DynSec bị broker từ chối. | `PASS (Local)` |
| **E17** | `credentials/ACL` | Xử lý mật khẩu sai, truy cập nặc danh và lỗi TLS | Broker từ chối xác thực; các lỗi sai CA, sai hostname, hoặc chứng chỉ hết hạn được phân loại riêng biệt với lỗi từ chối credential. | `PASS (Local)` |
| **E18** | `rotate/revoke/replay` | Xoay vòng mật khẩu trong maintenance window | Admin POST rotate nhận `200 OK` kèm mật khẩu mới; global drain ngắt kết nối session cũ; mật khẩu cũ bị từ chối, mật khẩu mới kết nối thành công; Gateway B giữ nguyên. | `PASS (Local)` |
| **E19** | `rotate/revoke/replay` | Replay xoay vòng mật khẩu và bảo vệ cache | Replay cùng khóa trả lời `200 OK` không kèm mật khẩu; cấm lưu trữ cache trên các phản hồi chứa thông tin xác thực (`no-store/no-cache`). | `PASS (Local)` |
| **E20** | `rotate/revoke/replay` | Thu hồi thông tin xác thực (Revoke) | Admin DELETE nhận `200 OK`; session Gateway A bị ngắt lập tức, mật khẩu cũ (v1/v2) bị từ chối; session Gateway B trên ORIGINAL socket duy trì liên tục và hoàn tất ping/SUBACK/publish status (không reconnect). Bản vá scoped fix được `go-reviewer` phê duyệt. Lưu ý: QoS 0 không phải bằng chứng lưu trữ bền vững hay phân phối chắc chắn. | `PASS (Local)` |
| **E21** | `rotate/revoke/replay` | Tái cấp phát (Reprovision) sau khi đã thu hồi | Cấp phát lại làm tăng `credential_version` và `operation_id` tuần tự theo thẩm quyền CSDL; xác nhận trạng thái active mới; snapshot audit bất biến được bảo toàn. Phiên bản được định danh theo version CSDL (không gọi là thế hệ độc lập). | `PASS (Local)` |
| **E22** | `rotate/revoke/replay` | Xử lý mất secret khi hoàn tất trên đường truyền | Cơ chế `scripts/tests/stage2_response_loss.py`: bounded loopback relay chuyển tiếp yêu cầu rotate tới Nginx/Go qua HTTPS đã xác thực chứng chỉ; relay nhận upstream 200 chứa secret thật, đọc toàn bộ rồi đóng kết nối downstream không gửi HTTP header/body; caller nhận `RemoteDisconnected`. CSDL xác nhận transaction chuyển `succeeded` với đúng 1 event. Replay cùng key trả về metadata-only (không mutation thừa; snapshot/events giữ nguyên). Rotate với key mới thành công; gated MQTT từ chối secret bị mất và chấp nhận secret khôi phục mới. Secret chỉ lưu tạm thời trong RAM relay. | `PASS (Local)` |
| **E23** | `loss/recovery` | Khôi phục sau sự cố ghi snapshot CSDL/Broker | Lỗi ghi snapshot trả về `503 Service Unavailable`, Ingress Gate giữ `CLOSED`; sau khi gỡ lỗi và khởi động lại, hệ thống phục hồi an toàn về trạng thái nhất quán OPEN. | `PASS (Local)` |
| **E24** | `loss/recovery` | Đối soát khởi động khi CSDL gián đoạn | Khi CSDL và broker khởi động lại, volume bền vững được bảo toàn, thẩm quyền được đối soát chính xác về verified OPEN, Gateway B kết nối lại thành công. | `PASS (Local)` |
| **E25** | `loss/recovery` | Xử lý timeout khi truy vấn CSDL quản trị | Trả về `503 Service Unavailable`, không làm sai lệch danh sách hay biến đổi trạng thái dữ liệu; khôi phục sạch sẽ khi CSDL hoạt động trở lại. | `PASS (Local)` |
| **E26** | `restart/fences` | Hành vi khi tắt tính năng credential và các route stub | Khi tắt cờ cấu hình, route credential trả lời `503 Disabled` cho admin và `403` cho non-admin; các route lịch sử/WebSocket/Digital Twin trả lời stub `501 Not Implemented`. | `PASS (Local)` |
| **E27** | `upgrade/repeatability` | Quét sạch bí mật và tính lặp lại của bộ kiểm thử | Diễn tập nâng cấp migration từ v9/10/13/14/15 lên v16 trên volume bền vững thành công; không rò rỉ secret trong log/CSDL/artifact; toàn bộ tài nguyên tạm thời được dọn dẹp sạch; chạy lại toàn bộ suite 2 lần liên tiếp đều đạt `exit 0`. | `PASS (Local)` |

---

## 5. Danh mục Lệnh Kiểm thử và Hợp đồng Runner

### 5.1 Các lệnh hiện hành có thể chạy kiểm chứng ngay (Runnable Current Commands)
Các lệnh này thuộc bộ kiểm thử hồi quy độc lập từ Task 2.0 đến Task 2.6, hiện có sẵn trong kho mã nguồn:

```sh
# 1. Kiểm thử Repository PostgreSQL và Trigger Guards
sh scripts/test-task263a-repository.sh

# 2. Kiểm thử Standalone Server và HTTP Credential API (6 named selectors)
sh scripts/test-stage2-mqtt-credentials.sh

# 3. Kiểm thử Mosquitto Runtime và native DynSec plugin
sh scripts/test-stage2-mosquitto-runtime.sh

# 4. Kiểm thử Xác thực Supabase Auth cô lập
sh scripts/test-stage2-auth.sh

# 5. Kiểm thử Ranh giới Quản trị Platform Admin
sh scripts/test-stage2-admin.sh

# 6. Kiểm thử Cấp phát Gateway và Sensor (Provisioning)
sh scripts/test-stage2-provisioning.sh

# 7. Kiểm thử Phân quyền và Đọc tài nguyên (Authorization)
sh scripts/test-stage2-authorization.sh

# 8. Kiểm thử Nâng cấp Schema CSDL (Migration 000001 - 000016)
sh scripts/test-stage2-migrations.sh

# 9. Kiểm tra Khói Backend tổng thể
sh scripts/test-backend-smoke.sh

# 10. Chạy toàn bộ Unit Tests Go với Race Detector và Coverage
go -C src test -race -count=1 ./...

# 11. Kiểm tra tĩnh và chuẩn hóa mã nguồn
go -C src vet ./...
golangci-lint run --timeout=5m
```

### 5.2 Hợp đồng Runner E2E và Bộ Kiểm thử Hồi quy Python (88 Regressions)

Bộ runner E2E tuân thủ nghiêm ngặt hợp đồng thực thi an toàn:
1. **Biến môi trường bảo vệ (`STAGE2_TEST_PASSWORD`)**: Mật khẩu kiểm thử được truyền qua biến môi trường bảo vệ, không hardcode trong mã nguồn, không xuất hiện trong tham số CLI (`argv`) hay lưu vết vào tài liệu.
2. **Đường dẫn Log duy nhất (`STAGE2_E2E_LOG`)**: Mỗi phiên thực thi chỉ định một đường dẫn log chuyên biệt (ví dụ `/tmp/opencode/task27-e22-all-1a025d37cf17.log`), ngăn chặn xung đột hoặc ghi đè log giữa các lần chạy.
3. **Bộ kiểm thử hồi quy Python (Python regressions & CI guards)**: Toàn bộ kiểm thử đơn vị trong `scripts/tests/test_*.py` (bao gồm unit guards cho runner, mô phỏng client/adapter DynSec, vòng đời credential, response loss relay `test_stage2_response_loss.py`, CI supervisor guard `test_stage2_ci.py`, và các ràng buộc phân quyền/auth) phải vượt qua trước khi chạy suite tích hợp:
   ```sh
   python3 -m unittest discover -s scripts/tests -p 'test_*.py'
   ```
4. **Thực thi toàn diện bộ 27 kịch bản E2E**:
   ```sh
   # Chạy toàn bộ 27 kịch bản E2E trên stack dịch vụ thật (toàn bộ 8 selectors)
   export STAGE2_TEST_PASSWORD="<protected-random-secret>"
   export STAGE2_E2E_LOG="/tmp/opencode/task27-run-$(date +%s).log"
   sh scripts/test-stage2-e2e.sh
   ```
5. **Thực thi từng bộ chọn tên riêng biệt**:
   ```sh
   python3 scripts/tests/stage2_e2e.py --selector accounts/auth
   python3 scripts/tests/stage2_e2e.py --selector provisioning
   python3 scripts/tests/stage2_e2e.py --selector memberships/roles
   python3 scripts/tests/stage2_e2e.py --selector credentials/ACL
   python3 scripts/tests/stage2_e2e.py --selector rotate/revoke/replay
   python3 scripts/tests/stage2_e2e.py --selector loss/recovery
   python3 scripts/tests/stage2_e2e.py --selector restart/fences
   python3 scripts/tests/stage2_e2e.py --selector upgrade/repeatability
   ```
6. **Công cụ vận hành membership có bảo vệ (Protected Membership CLI)**:
   ```sh
   sh scripts/manage-gateway-membership.sh --actor-email admin@example.com \
       --gateway-id gateway_001 \
       --target-email operator@example.com \
       --action grant \
       --role operator
   ```
7. **Thực thi qua CI Supervisor (CI-Equivalent Local Execution)**:
   ```sh
   # Chạy qua CI supervisor mô phỏng đúng hợp đồng GitHub Actions (job timeout: 30m, env-only random secret)
   STAGE2_CI_DIR="/tmp/opencode/stage2-ci-run-1" python3 scripts/tests/stage2_ci.py
   STAGE2_CI_DIR="/tmp/opencode/stage2-ci-run-2" python3 scripts/tests/stage2_ci.py

   # Kiểm chứng hoặc kích hoạt dọn dẹp tài nguyên sở hữu (hậu kiểm thử hoặc sau hủy tiến trình)
   python3 scripts/tests/stage2_ci.py --cleanup
   ```

---

## 6. Kế hoạch Nâng cấp Database và Giới hạn Chuyển đổi Runtime

### 6.1 Các nhánh nâng cấp Persistent Volume được hỗ trợ
Hệ thống kiểm chứng nâng cấp CSDL trên volume lưu trữ bền vững (persistent volume) đã được xác minh thực tế qua kịch bản E27:
- **Cài đặt mới (Fresh install)**: Khởi tạo trực tiếp từ schema v16 (`000016_mqtt_event_authority_guard`).
- **Nâng cấp tuần tự được hỗ trợ (Supported Upgrades)**: Khởi tạo từ các phiên bản lịch sử v9, v10, v13, v14, v15 và áp dụng migration nâng cấp lên v16. Toàn bộ dữ liệu, chỉ mục, ràng buộc khóa ngoại, trigger guard và quyền của role ứng dụng (`iot_backend_app`) được bảo toàn tuyệt đối.
- **Giới hạn v11 và v12**: Các phiên bản v11 và v12 chỉ được đưa vào diễn tập nâng cấp khi có dữ liệu mẫu (seed data) và hợp đồng nghiệp vụ rõ ràng; tuyệt đối không tự ý tuyên bố đã hỗ trợ nếu chưa có kịch bản kiểm chứng.

### 6.2 Phân định ranh giới giữa CSDL và Mosquitto DynSec
- **Tách biệt hoàn toàn**: Quá trình nâng cấp CSDL nghiệp vụ (`schema_migrations`) hoàn toàn độc lập với việc chuyển đổi cấu hình Mosquitto từ `password_file` tĩnh sang plugin `DynSec`.
- **Không tự động chuyển mã băm**: Không có cơ chế tự động chuyển đổi mã băm mật khẩu từ Mosquitto vào PostgreSQL, cũng như không biến tài khoản broker thành thẩm quyền trong CSDL.
- **Không tự ý sửa chữa (No Auto-heal)**: Nếu phát hiện bản chiếu trạng thái cũ (legacy projection) không có hướng xử lý được hỗ trợ, Ingress Gate bắt buộc giữ trạng thái `CLOSED`, thông báo điểm nghẽn và chờ phê duyệt phương án thủ công; tuyệt đối không tự ý xóa rào cản kiểm toán (audit fence).
- **Ranh giới diễn tập**: Mọi diễn tập chuyển đổi runtime trong Task 2.7 chỉ thực hiện trên môi trường test cô lập, không thực hiện chuyển đổi trên môi trường production thực tế.

---

## 7. Quy trình Dọn dẹp và Rào cản Kỹ thuật Trước Khi Rollout

### 7.1 Cơ chế dọn dẹp tài nguyên kiểm thử (Cleanup Verification)
- Tất cả các script kiểm thử đăng ký hàm thu hồi qua `trap cleanup EXIT INT TERM` (với Shell) và khối `try...finally` (với Python).
- Trong CI supervisor tăng cường (`scripts/tests/stage2_ci.py`), quy trình dọn dẹp tài nguyên sở hữu (owned resource cleanup) tuân thủ hợp đồng nghiêm ngặt:
  * **Xác thực manifest tiên quyết (`validate_owner`)**: Kiểm tra tệp `owner.json` và `identity` (32 ký tự hex), kiểm tra đường dẫn canonical tuyệt đối (cấm symlink) trước khi thực hiện bất kỳ truy vấn hay thao tác Docker nào.
  * **Truy vấn bằng nhãn và đối soát ID chính xác (Exact Resource IDs & Inspected Labels)**: Sử dụng bộ lọc nhãn `--filter label=io.iot.stage2-ci-run=<run_id>`. Mỗi tài nguyên ứng viên đều được `docker inspect` kiểm tra tính hợp lệ: nhãn sở hữu phải trùng khớp `run_id`, nhãn Compose project phải khớp chính xác `project` (`dynsec_spike_[a-f0-9]{12}`), và tên tài nguyên phải khớp tuyệt đối theo mẫu danh mục (direct matching). **Loại bỏ hoàn toàn cơ chế xóa dựa trên tiền tố chuỗi con (no substring deletion)** nhằm loại trừ nguy cơ xóa nhầm tài nguyên lân cận.
  * **Phạm vi môi trường runner**: Thiết kế dọn dẹp này được xây dựng chuyên biệt cho **disposable GitHub-hosted runner** (môi trường máy ảo tạm thời của GitHub Actions), **tuyệt đối không áp dụng trên shared production daemon**.
  * **Dọn dẹp thư mục fixture an toàn**: Thư mục fixture thuộc quyền sở hữu UID 1883 (broker) được dọn dẹp bằng container tiện ích Mosquitto cô lập hoàn toàn (`--network none`, `--cap-drop=ALL`, `--read-only`, mount duy nhất thư mục con đích).
- Trong CI workflow (`.github/workflows/ci.yml`), bước dọn dẹp `python3 scripts/tests/stage2_ci.py --cleanup` được gắn điều kiện `if: always()`.
- Tiêu chí hoàn thành: Kiểm tra hậu thực thi xác nhận `cleanup_verified: true` và `complete: true` (0 container, 0 network, 0 volume tồn dư).
- **Ranh giới thực nghiệm**: Mặc dù logic bẫy tín hiệu hủy (`SIGTERM`/`SIGINT`) và fallback cleanup đã được đối soát qua unit tests (`test_stage2_ci.py`), việc kích hoạt kịch bản hủy thực tế trên hạ tầng GitHub Actions từ xa **CHƯA TỪNG CHẠY (Actual cancellation on GitHub NOT RUN)**.

### 7.2 Các rào cản kỹ thuật trước khi Rollout Production (Production Blockers)
Việc đạt kết quả `PASS` toàn bộ 27 kịch bản E2E cục bộ và nhận phán quyết `APPROVE` từ CI Reviewer là mốc hoàn thành kiểm chứng kỹ thuật cốt lõi cho Task 2.7, nhưng **tuyệt đối không tương đương với việc tuyên bố Stage 2 hay hệ thống đã sẵn sàng cho môi trường production (no full Stage 2 / prod readiness claim)**. Hai rào cản bắt buộc (hard blockers) ngăn chặn việc rollout thực tế gồm:
1. **Chưa có Remote GitHub Actions Execution và Exact-SHA Commit Proof**: Phân hệ `stage2-e2e` đã hoàn tất triển khai trong `.github/workflows/ci.yml` là job thứ 10 (`timeout-minutes: 30`, 2 lượt chạy tuần tự, dọn dẹp sở hữu tự động, artifact allowlist `safe/report.json`, secret ngẫu nhiên env-only). CI Reviewer độc lập đã phê duyệt (`APPROVED`) kiến trúc và mã nguồn supervisor. Hai lượt chạy mô phỏng CI tương đương cục bộ mới nhất đã xác nhận `exit 0`, 27/27 PASS, 8/8 selector PASS, cleanup verified. Tuy nhiên:
   - Việc kích hoạt thực thi thực tế trên GitHub Actions từ xa và bước upload artifact từ xa vẫn giữ trạng thái `PENDING`.
   - Bằng chứng commit SHA cuối cùng (exact FINAL SHA) tiếp tục `PENDING`. Mã commit cục bộ hiện hành (`55cdcd5`) có chứa các thay đổi chưa commit trong worktree (`relevant_worktree_dirty: true`) nên **tuyệt đối KHÔNG phải là final committed acceptance**.
   - Kịch bản hủy thực tế trên GitHub Actions chưa diễn ra (`NOT RUN`), và công cụ kiểm tra cú pháp ShellCheck chưa được kích hoạt chạy (`ShellCheck not run`).
   - Worker không có thẩm quyền điều phối từ xa (no remote dispatch), commit hay push lên kho mã nguồn.
2. **Các rào cản vận hành hạ tầng thực tế chưa kiểm chứng**:
   - **Topology Mạng Thực tế và Đường hầm Rathole**: Chưa có kiểm chứng thực nghiệm trên đường truyền Internet thực tế qua Cloudflare Tunnel và Rathole Reverse TCP Proxy tới phần cứng Gateway thực.
   - **Chính sách Mất CSDL Sau Khi Mở Cổng (Post-OPEN Database Loss Policy)**: Cần phân biệt rõ giữa việc quan sát snapshot cấu hình tại thời điểm kiểm thử (snapshot observed at test time) và độ bền vững vận hành liên tục (runtime durability). Snapshot trong DynSec chỉ phản ánh cấu hình RAM được lưu vào tệp tại thời điểm ghi; hệ thống chưa có cơ chế watchdog tự động cô lập broker nếu CSDL gặp sự cố mất kết nối kéo dài sau khi cổng Ingress Gate đã mở cho lưu lượng thực tế.
   - **Quy trình Nạp Thông tin Xác thực Phần cứng**: Chưa kiểm chứng quy trình nạp thông tin xác thực an toàn qua cổng USB hoặc lưu trữ mã hóa phần cứng (flash/NVS) trên các bo mạch Gateway thực tế (ESP32, Luckfox Pico Plus, TI AM5728).
   - **Cấu hình Mặc định**: Cờ cấu hình `MQTT_CREDENTIAL_API_ENABLED` bắt buộc giữ giá trị `false` trên toàn bộ môi trường ngoài phạm vi kiểm thử cô lập.

---

## 8. Bằng chứng Thực nghiệm Kiểm chứng Cục bộ (Local E2E Verification Evidence)

### 8.1 Bằng chứng kiểm thử trực tiếp trên harness cô lập (Existing Local E22/E20 Evidence)

Bằng chứng thực nghiệm được tổng hợp từ hai lượt chạy kiểm thử toàn diện liên tiếp độc lập trên harness cô lập, được đối soát độc lập bởi parent session:

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                  BẰNG CHỨNG KIỂM THỬ THỰC NGHIỆM CỤC BỘ                    │
├──────────────────────────┬─────────────────────────────────────────────────┤
│ Git Commit SHA           │ [PENDING - CHỜ CỐ ĐỊNH TRÊN REMOTE CI PIPELINE] │
│ GitHub Actions Run URL   │ [PENDING - JOB ĐÃ CẤU HÌNH; CHỜ REMOTE RUN]     │
│ Khung thời gian chạy UTC │ 2026-10-04 22:38:08 UTC — 22:41:57 UTC          │
│ Thời gian địa phương     │ Rạng sáng 2026-10-05                            │
│ Môi trường kiểm thử      │ Linux x86_64, Docker Compose isolated harness    │
│ Phiên bản phần mềm       │ Go 1.27.1, Mosquitto 2.0.18, TimescaleDB v16     │
├──────────────────────────┴─────────────────────────────────────────────────┤
│ CHI TIẾT HAI LẦN CHẠY LIÊN TIẾP ĐỘC LẬP (EPHEMERAL OPERATOR ARTIFACT PATHS)│
├──────────────────────────┬────────────────────────┬────────────────────────┤
│ Thông số đối soát        │ Lượt chạy 1 (Run 1)    │ Lượt chạy 2 (Run 2)    │
├──────────────────────────┼────────────────────────┼────────────────────────┤
│ Run Identifier           │ task27-e22-all-        │ task27-e22-all-        │
│                          │ 1a025d37cf17           │ 8c4514fe9576           │
│ Thời điểm bắt đầu (UTC)  │ 2026-10-04T22:38:08Z   │ 2026-10-04T22:39:53Z   │
│ Thời điểm kết thúc (UTC) │ 2026-10-04T22:39:53Z   │ 2026-10-04T22:41:57Z   │
│ Mã thoát tiến trình      │ exit 0                 │ exit 0                 │
│ Số lượng Execution Table │ 1 summary duy nhất     │ 1 summary duy nhất     │
│ Số lượng lỗi Traceback   │ 0 tracebacks           │ 0 tracebacks           │
│ Kịch bản hoàn thành      │ 27 / 27 PASS (E01-E27) │ 27 / 27 PASS (E01-E27) │
│ Post-cleanup scan        │ true                   │ true                   │
│ Đường dẫn tệp nhật ký    │ /tmp/opencode/task27-  │ /tmp/opencode/task27-  │
│ (ephemeral artifact log) │ e22-all-1a025d37cf17   │ e22-all-8c4514fe9576   │
│                          │ .log                   │ .log                   │
│ Đường dẫn tệp JSON       │ /tmp/opencode/task27-  │ /tmp/opencode/task27-  │
│ (ephemeral artifact json)│ e22-all-1a025d37cf17   │ e22-all-8c4514fe9576   │
│                          │ .json                  │ .json                  │
├──────────────────────────┴────────────────────────┴────────────────────────┤
│ KẾT QUẢ ĐỐI SOÁT THEO TỪNG BỘ CHỌN (NAMED SELECTORS)                       │
├──────────────────────────┬──────────┬──────────┬─────────────┬─────────────┤
│ Tên Selector             │ Số tests │ Kết quả  │ Tình trạng  │ Dọn dẹp     │
├──────────────────────────┼──────────┼──────────┼─────────────┼─────────────┤
│ accounts/auth            │ 6 / 6    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ provisioning             │ 3 / 3    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ memberships/roles        │ 4 / 4    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ credentials/ACL          │ 4 / 4    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ rotate/revoke/replay     │ 5 / 5    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ loss/recovery            │ 3 / 3    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ restart/fences           │ 1 / 1    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
│ upgrade/repeatability    │ 1 / 1    │ PASS     │ Đạt chuẩn   │ Hoàn tất    │
├──────────────────────────┼──────────┼──────────┼─────────────┼─────────────┤
│ TỔNG HỢP TOÀN BỘ SUITE   │ 27 / 27  │ PASS     │ 100% ĐẠT    │ post_clean  │
├──────────────────────────┴──────────┴──────────┴─────────────┴─────────────┤
│ BẰNG CHỨNG BẢO VỆ VÀ AN TOÀN                                               │
├──────────────────────────┬─────────────────────────────────────────────────┤
│ Quét rò rỉ secret        │ Secret scan passed: zero leaks detected         │
│ Quét secret sau dọn dẹp  │ Post-cleanup known-secret scan passed           │
│ Dọn dẹp tài nguyên       │ post_cleanup_scan: true (0 containers/networks) │
│ Trạng thái log lịch sử   │ Historical polluted log superseded (not deleted)│
│ Bằng chứng còn thiếu     │ - Kích hoạt chạy thực tế job stage2-e2e trên CI │
│                          │ - Remote CI Run URL và Exact Final Commit SHA   │
└──────────────────────────┴─────────────────────────────────────────────────┘
```

### 8.2 Bằng chứng hai lượt chạy mô phỏng CI supervisor tăng cường (Hardened CI-Equivalent Supervisor Evidence)

Nhằm kiểm chứng tính tương đương chính xác với quy trình CI trên GitHub Actions trước khi push/merge, phân hệ giám sát tăng cường `scripts/tests/stage2_ci.py` (hợp đồng của job thứ 10 `stage2-e2e`, thời lượng tối đa 30 phút) đã được kích hoạt thực thi 2 lượt liên tiếp trên môi trường cục bộ:

```text
┌────────────────────────────────────────────────────────────────────────────┐
│         BẰNG CHỨNG HAI LƯỢT CHẠY MÔ PHỎNG CI SUPERVISOR (STAGE2_CI.PY)     │
├──────────────────────────┬─────────────────────────────────────────────────┤
│ CI Job                   │ stage2-e2e (Job thứ 10 trong ci.yml)            │
│ Job Timeout              │ 30 minutes (timeout-minutes: 30)                │
│ Quản lý mật khẩu         │ Runtime random fixture secret env-only          │
│                          │ (STAGE2_TEST_PASSWORD sinh động, add-mask CI)   │
│ Chính sách Artifact      │ Safe report.json allowlisted scanned only       │
│                          │ (ghi atomic 0600 flush/fsync os.replace)        │
│ Dọn dẹp tài nguyên       │ Owned cleanup verified (exact IDs + inspect)    │
│ Trạng thái thẩm duyệt    │ APPROVED (Reviewer độc lập đã phê duyệt)        │
├──────────────────────────┼────────────────────────┬────────────────────────┤
│ Thông số đối soát        │ Lượt chạy 1 (Run 1)    │ Lượt chạy 2 (Run 2)    │
├──────────────────────────┼────────────────────────┼────────────────────────┤
│ Đường dẫn Artifact an toàn│ /tmp/opencode/stage2-  │ /tmp/opencode/stage2-  │
│ (allowlisted report.json)│ ci-reviewed-1791156194 │ ci-reviewed-1791156194 │
│                          │ /run-1/safe/report.json│ /run-2/safe/report.json│
│ Thời lượng thực thi      │ 104.02 giây            │ 115.02 giây            │
│ Mã thoát tiến trình      │ exit 0 (harness_exit 0)│ exit 0 (harness_exit 0)│
│ Kịch bản hoàn thành      │ 27 / 27 PASS (E01-E27) │ 27 / 27 PASS (E01-E27) │
│ Bộ chọn quan sát được    │ 8 / 8 PASS (Selectors) │ 8 / 8 PASS (Selectors) │
│ Trạng thái hoàn tất      │ complete: true         │ complete: true         │
│ Dọn dẹp tài nguyên       │ cleanup_verified: true │ cleanup_verified: true │
│ Local Git SHA thời điểm  │ 55cdcd57892d35df33c0   │ 55cdcd57892d35df33c0   │
│ thực nghiệm              │ caaa52bf6038613399e7   │ caaa52bf6038613399e7   │
│ Tình trạng SHA           │ Local HEAD 55cdcd5 có  │ Local HEAD 55cdcd5 có  │
│                          │ dirty uncommitted work │ dirty uncommitted work │
│                          │ (KHÔNG PHẢI FINAL SHA) │ (KHÔNG PHẢI FINAL SHA) │
│ Provenance Worktree      │ relevant_worktree_     │ relevant_worktree_     │
│                          │ dirty: true            │ dirty: true            │
│ Provenance Context       │ execution_context:     │ execution_context:     │
│                          │ local                  │ local                  │
│ Provenance Event SHA     │ event_sha: unavailable │ event_sha: unavailable │
├──────────────────────────┴────────────────────────┴────────────────────────┤
│ KẾT QUẢ ĐỐI SOÁT CHI TIẾT 27/27 KỊCH BẢN VÀ 8/8 SELECTORS (RUN 1 & RUN 2)   │
├───────────────────────────────────────────────────┬────────────────────────┤
│ E01: PASS | E02: PASS | E03: PASS | E04: PASS     │ accounts/auth (6/6)    │
│ E05: PASS | E06: PASS                             │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E07: PASS | E08: PASS | E09: PASS                 │ provisioning (3/3)     │
│                                                   │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E10: PASS | E11: PASS | E12: PASS | E13: PASS     │ memberships/roles (4/4)│
│                                                   │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E14: PASS | E15: PASS | E16: PASS | E17: PASS     │ credentials/ACL (4/4)  │
│                                                   │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E18: PASS | E19: PASS | E20: PASS | E21: PASS     │ rotate/revoke/replay   │
│ E22: PASS                                         │ (5/5) OBSERVED: PASS   │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E23: PASS | E24: PASS | E25: PASS                 │ loss/recovery (3/3)    │
│                                                   │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E26: PASS                                         │ restart/fences (1/1)   │
│                                                   │ OBSERVED: PASS         │
├───────────────────────────────────────────────────┼────────────────────────┤
│ E27: PASS                                         │ upgrade/repeatability  │
│                                                   │ (1/1) OBSERVED: PASS   │
├───────────────────────────────────────────────────┴────────────────────────┤
│ ĐỐI SOÁT ĐỘC LẬP VÀ PHÂN ĐỊNH RANH GIỚI BẰNG CHỨNG                         │
├──────────────────────────┬─────────────────────────────────────────────────┤
│ Reviewer độc lập         │ 23 scoped tests / syntax / shell / diff PASS    │
│ Worker thực thi          │ 111 all Python tests PASS (test_*.py)           │
│ Nguyên tắc bằng chứng    │ Không gộp lẫn bằng chứng độc lập giữa Reviewer  │
│                          │ và Worker; kịch bản native không rerun cho các  │
│                          │ bản sửa lỗi chỉ liên quan manifest/parser       │
│ Lịch sử artifact         │ Cặp báo cáo cũ stage2-ci-verified-1791155125    │
│                          │ được thay thế (superseded) bởi stage2-ci-       │
│                          │ reviewed-1791156194; giữ ngữ cảnh lịch sử       │
├──────────────────────────┼─────────────────────────────────────────────────┤
│ Trạng thái Thẩm duyệt CI │ APPROVED (Reviewer độc lập đã phê duyệt)        │
│ Remote GitHub Actions Run│ PENDING (Chưa kích hoạt chạy trên remote GitHub) │
│ Remote Artifact Upload   │ PENDING (Chưa upload artifact trên GitHub)      │
│ Exact FINAL Commit SHA   │ PENDING (Chờ hoàn tất commit/push sạch sẽ)      │
│ Kịch bản Hủy trên GitHub │ NOT RUN (Chưa diễn ra trên remote runner)       │
│ ShellCheck               │ NOT RUN (Chưa chạy trong phân hệ này)           │
│ Sẵn sàng Sản xuất        │ NO FULL PRODUCTION READINESS CLAIM              │
│ Thẩm quyền Worker        │ Không dispatch remote, không commit, không push │
└──────────────────────────┴─────────────────────────────────────────────────┘
```

---

## 9. Liên kết Kế thừa Kỹ thuật và Tài liệu Liên quan

Để tránh trùng lặp nội dung chi tiết, các phân hệ kỹ thuật nền tảng được kế thừa trực tiếp từ các runbook và tài liệu đã được phê duyệt:
1. **Kiến trúc Vòng đời Credential và Ingress Gate**: Xem chi tiết tại [`docs/backend/stage-2-task-2.6-mqtt-credentials.md`](stage-2-task-2.6-mqtt-credentials.md).
2. **Quy trình Khôi phục Sự cố và Báo cáo Nghiệm thu Task 2.6**: Xem chi tiết tại [`docs/backend/stage-2-task-2.6-acceptance.md`](stage-2-task-2.6-acceptance.md).
3. **Đặc tả Phân quyền Đọc và Ranh giới Vai trò Task 2.3**: Xem chi tiết tại [`docs/backend/stage-2-task-2.3-authorization.md`](stage-2-task-2.3-authorization.md).
4. **Chính sách Quản trị Tài khoản Tập trung Task 2.2A**: Xem chi tiết tại [`docs/backend/stage-2-task-2.2A-centralized-accounts.md`](stage-2-task-2.2A-centralized-accounts.md).
5. **Kế hoạch Chi tiết Nguồn cho Task 2.7**: Xem tại `docs/backend_plan/task_2.7_detail_plan.md`.
