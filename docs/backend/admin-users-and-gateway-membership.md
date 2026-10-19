# Báo cáo Tổng kết và Nghiệm thu: Quản trị Người dùng và Quyền Thành viên Gateway (Tasks U0–U8)

**Ngày lập:** 19/10/2026
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Trạng thái:** Hoàn thành có giới hạn (Tasks U0–U8 Bounded Complete; mã nguồn và bài kiểm thử U0–U7 đã commit trên `develop` qua `35b6136f22c5a1c819afefc12003dba3ba613062`; Task U8 tài liệu hoàn tất).
**Baseline khảo sát:** Nhánh `develop`, commit `573abde1c0872e49d4942b47a8c85afe7abf235b`.
**Commit kiểm chứng CI từ xa:** `35b6136f22c5a1c819afefc12003dba3ba613062` (GitHub Actions Run [39272190456](https://github.com/TVTIT/project1_iot_server/actions/runs/39272190456), hoàn thành thành công **16/16 jobs**).
**Tài liệu tham chiếu:**
- [`../../AGENTS.md`](../../AGENTS.md) (Mục 0, 5, 6, 6.5, 7, 10, 15, 16, 18)
- [`../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md`](../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md) (Kế hoạch chi tiết nghiệp vụ U0–U8)
- [`../client/07_ADMIN_API_CLIENT_CONTRACT.md`](../client/07_ADMIN_API_CLIENT_CONTRACT.md) (Hợp đồng API Quản trị nền tảng)
- [`../client/02_AUTHENTICATION_AND_SESSION.md`](../client/02_AUTHENTICATION_AND_SESSION.md) (Xác thực và Phiên làm việc)
- [`../client/03_USER_GATEWAY_AUTHORIZATION.md`](../client/03_USER_GATEWAY_AUTHORIZATION.md) (Cơ chế phân quyền người dùng và Gateway)
- [`../client/08_API_INTEGRATION_AND_STATE_HANDLING.md`](../client/08_API_INTEGRATION_AND_STATE_HANDLING.md) (Tích hợp API và Quản lý trạng thái Client)
- [`../dynsec/limitations-and-backlog.md`](../dynsec/limitations-and-backlog.md) (Danh mục tồn đọng và hạn chế kỹ thuật chung)
- [`../../migrations/000021_admin_user_management.up.sql`](../../migrations/000021_admin_user_management.up.sql) (Migration CSDL cấp quyền và security-definer function)

---

## 1. Mục tiêu và Ranh giới Phạm vi Triển khai

### 1.1 Mục tiêu tính năng
Tính năng Quản trị Người dùng và Quyền Thành viên Gateway được triển khai nhằm giải quyết khoảng trống vận hành lịch sử: trước đây, hệ thống IoT áp dụng mô hình quản trị tài khoản tập trung nhưng thiếu các API trong Go backend để Platform Admin tạo tài khoản người dùng và quản lý quyền thành viên Gateway, buộc phải thực hiện thủ công qua Supabase Studio hoặc script SQL trực tiếp.

Lát cắt Tasks U0–U8 cung cấp năm (05) endpoint canonical trong Go backend đặt dưới nhóm route quản trị `/v1/admin/*`, vận hành theo nguyên tắc phòng thủ đa tầng (defense-in-depth):
1. `POST /v1/admin/users`: Tạo tài khoản người dùng mới (gọi Supabase Auth Admin API với `email_confirm: true`), tự động tạo bản ghi hồ sơ `public.profiles`, trả về `201 Created` kèm header `Location: /v1/admin/users/{id}`.
2. `GET /v1/admin/users`: Lấy danh sách toàn bộ người dùng trong giới hạn cấu hình (`items: [...]`), kèm thông tin vai trò nền tảng `platform_role` và danh sách quyền Gateway `gateway_roles: [{gateway_id, role}]`; không có query pagination/search trên API; vượt giới hạn trả về `409 Conflict` (không cắt xén partial list).
3. `PATCH /v1/admin/users/{user_id}`: Cập nhật nguyên tử trong một transaction tên hiển thị `full_name` (trong `public.profiles`) và/hoặc vai trò nền tảng `platform_role` (cấp/thu hồi `public.platform_admins`), cấm tự hạ quyền admin (`409 Conflict`).
4. `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`: Gán vai trò thành viên `operator` hoặc `viewer` cho người dùng trên Gateway trong `public.user_gateways`; cấm gán `owner` qua route này (`400 Bad Request`); nếu người dùng đang là owner thì từ chối (`409 Conflict`).
5. `PUT /v1/admin/gateways/{gateway_id}/owner`: Chuyển giao quyền sở hữu trạm Gateway nguyên tử trong `public.user_gateways`: người nhận trở thành `owner`, owner cũ được hạ thành `viewer` (bảo toàn quyền đọc, không xóa membership), bảo toàn ràng buộc duy nhất một owner trên trạm.

### 1.2 Ranh giới kiến trúc và bảo mật bất di bất dịch
- **Khóa hạ tầng đặc quyền (`SUPABASE_SERVICE_ROLE_KEY`):** Khóa này được lưu giữ duy nhất tại biến môi trường nội bộ của server backend. **Tuyệt đối không bao giờ chia sẻ, gửi về client hoặc lưu trữ trong frontend/mobile.** Human caller chỉ gửi JWT cá nhân với claim chuẩn `role: "authenticated"`. Nếu khóa này không được cấu hình, server vẫn khởi động bình thường; 4 endpoint CSDL vẫn hoạt động, chỉ riêng `POST /v1/admin/users` (CreateUser) trả về HTTP 503 `service_unavailable`.
- **Thẩm quyền Platform Admin dựa trên CSDL (`public.platform_admins`):** Go Backend tự kiểm tra quyền hạn của caller thông qua bảng CSDL `public.platform_admins` trong PostgreSQL. Token JWT của người dùng dù hợp lệ nhưng nếu thiếu bản ghi trong bảng này sẽ bị từ chối ngay với mã HTTP `403 Forbidden`.
- **Admin không tự động bypass quyền đọc dữ liệu viễn trắc/Digital Twin:** Toàn bộ các API nghiệp vụ đều áp dụng cơ chế lọc theo tư cách thành viên trạm (`user_gateways`). Danh sách trạm (`GET /v1/gateways`) được lọc theo thành viên và trả về `200 OK` với mảng rỗng `{"items":[]}` nếu admin không có membership trên trạm nào; các truy vấn chi tiết thực thể hoặc cảm biến không thuộc quyền (`GET /v1/digital-twins/{id}`, `GET /v1/digital-twins/{id}?format=simplified`, `GET /v1/digital-twins/{id}/temporal`, `GET /v1/gateways/{id}/sensors`) đều trả về `404 Not Found` (không có cơ chế blanket read bypass cho Platform Admin).
- **Phạm vi CRUD tài khoản được giới hạn có chủ đích (Bounded Account CRUD):**
  - **ĐÃ TRIỂN KHAI:** Tạo tài khoản kèm mật khẩu (auto-confirm), xem danh sách toàn bộ kèm quyền trạm, đổi tên hiển thị, cấp/thu hồi quyền Platform Admin, gán quyền operator/viewer, và chuyển quyền sở hữu trạm Gateway.
  - **NGOÀI PHẠM VI (KHÔNG TRIỂN KHAI):** Xóa tài khoản (`Delete`), khóa/ban tài khoản, đổi email, đổi mật khẩu người dùng, reset mật khẩu, gửi email lời mời (invitations), tự đăng ký công khai (public signup), hoặc xóa hoàn toàn bản ghi thành viên (`DELETE membership`).

---

## 2. Thay đổi Cơ sở dữ liệu và Cấu hình

### 2.1 Migration 21: `000021_admin_user_management.up.sql`
- **Khóa tuần tự & Điều kiện tiên quyết:** Sử dụng PostgreSQL advisory lock `pg_advisory_lock(3290, 2)` và `\set ON_ERROR_STOP on`. Kiểm tra bắt buộc migration tiền nhiệm version 20 (`stage3_telemetry_ingestion`) đã được áp dụng.
- **Xử lý môi trường khởi tạo (Pre-Auth vs Post-Auth):**
  - Nếu bảng `auth.users` chưa tồn tại (khi chạy ở bước khởi tạo đầu của database volume), migration ghi log bỏ qua và **không ghi nhận version 21 vào `schema_migrations`**, tránh làm sai lệch phiên bản trước khi Auth khởi chạy.
  - Sau khi container Supabase Auth khởi chạy thành công (tạo bảng `auth.users`), service `supabase-compat-migration` áp dụng migration 4 (`000004_supabase_compat.up.sql` tạo trigger `on_auth_user_created`) và migration 21 tuần tự nối tiếp (`&&`).
- **Hàm bảo mật đọc danh sách người dùng (`public.list_admin_users_projection`):**
  - Được khai báo `SECURITY DEFINER` với `SET search_path = ''` (khóa hoàn toàn đường dẫn tìm kiếm chống tấn công leo thang đặc quyền SQL).
  - Kiểm tra thẩm quyền actor trong transaction: bắt buộc `pa.user_id = p_actor_id` trong `public.platform_admins`.
  - Chỉ chiếu các cột cho phép: `user_id`, `email`, `full_name`, `platform_role`, `created_at`, `profile_exists`. **Tuyệt đối không đọc mật khẩu, password hash, hay token.**
  - Thu hồi toàn bộ quyền thực thi: `REVOKE ALL ON FUNCTION ... FROM PUBLIC, anon, authenticated;` và chỉ cấp quyền duy nhất cho role backend: `GRANT EXECUTE ON FUNCTION ... TO iot_backend;`.
- **Cấp quyền tối thiểu (Least-Privilege Grants) cho role `iot_backend`:**
  - `GRANT UPDATE (full_name) ON TABLE public.profiles TO iot_backend;`: Chỉ cho phép sửa cột `full_name`, không cho phép sửa khóa chính `id`.
  - `GRANT INSERT, DELETE ON TABLE public.platform_admins TO iot_backend;`: Cho phép cấp và gỡ vai trò platform admin.

### 2.2 Cấu hình Môi trường (Environment Knobs)
Hệ thống bổ sung cấu hình có kiểm tra ranh giới chặt chẽ tại `src/internal/config/usermanagement.go`:

| Biến môi trường | Giá trị mặc định | Dải hợp lệ | Thứ tự ưu tiên & Ý nghĩa |
|---|---|---|---|
| `AUTH_ADMIN_BASE_URL` | `http://supabase-envoy:8000` | HTTP/HTTPS URL hợp lệ, không userinfo | Ưu tiên cao hơn `SUPABASE_URL`. Địa chỉ gọi GoTrue Admin API qua Envoy Gateway. |
| `SUPABASE_SERVICE_ROLE_KEY` | *(trống)* | Chuỗi tùy chọn | Ưu tiên cao hơn `SERVICE_ROLE_KEY`. Khóa đặc quyền hạ tầng gọi Auth Admin API. Nếu để trống, server vẫn khởi động bình thường, 4 endpoint CSDL vẫn hoạt động, chỉ `POST /v1/admin/users` trả HTTP 503. |
| `AUTH_ADMIN_TIMEOUT` | `10s` | `100ms` đến `60s` | Ưu tiên cao hơn `USER_MANAGEMENT_TIMEOUT`. Ngân sách timeout bao trùm toàn bộ nghiệp vụ user-management service operation (actor recheck, adapter call, profile verification, repository transaction). |
| `AUTH_ADMIN_MAX_BODY_BYTES` | `16384` (16 KiB) | `1` đến `1.048.576` bytes | Giới hạn kích thước đọc phản hồi từ dịch vụ Auth thượng nguồn (upstream Auth response read bound trong `SupabaseAuthAdapterConfig`), KHÔNG PHẢI giới hạn body request client gửi lên (request body client được giới hạn bởi `cfg.AdminMaxBodyBytes`). |
| `USER_LIST_MAX_USERS` | `1000` | `1` đến `10.000` | Số lượng tài khoản tối đa trong một lần đọc danh sách người dùng. |
| `USER_LIST_MAX_MEMBERSHIPS` | `10000` | `1` đến `100.000` (phải $\ge$ `MaxUsers`) | Số lượng bản ghi thành viên trạm tối đa khi đọc danh sách. |
| `USER_LIST_MAX_RESPONSE_BYTES` | `2097152` (2 MiB) | `1.024` đến `10.485.760` bytes | Giới hạn dung lượng JSON serialize tối đa của response danh sách người dùng. |

---

## 3. Lịch sử Triển khai Từng Lát cắt và Bằng chứng RED/GREEN (Tasks U0–U7)

Tiến trình triển khai tính năng được phân định rõ ràng giữa các mốc kiểm tra điều kiện tiên quyết, xuất xứ bằng chứng kiểm thử (Behavioral RED vs Compilation Failures), và trạng thái đạt chuẩn (GREEN):

| Lát cắt | Mục tiêu nghiệp vụ | Bài kiểm thử tương ứng | Trạng thái RED (Behavioral Fail) | Trạng thái GREEN (Pass) |
|---|---|---|---|---|
| **U0** | Chốt prerequisites, baseline, khóa DTOs và error envelopes | `src/internal/config/usermanagement_test.go` | **Không áp dụng (Baseline check)**: Xác minh điều kiện tiên quyết và cấu hình, không áp dụng RED hành vi. | `PASS`: Đọc cấu hình đúng, fallback đúng thứ tự ưu tiên, bảo vệ che giấu secret khi format log. |
| **U1** | Request validators, parsers và hàng rào phân quyền middleware | `src/internal/usermanagement/validation_test.go`, `src/internal/httpserver/admin_user_handlers_test.go` | **Xuất xứ RED chưa được kiểm chứng từ handoffs**: Không có artifact log RED lưu lại để khẳng định chi tiết. | `PASS`: Validate khắt khe mọi input; parser từ chối unknown fields (400), payload quá lớn (413), sai MIME (415). |
| **U2** | Migration 21, SQL projection và repository quản lý profile/role | `src/internal/usermanagement/postgres_repository_test.go` | **Xuất xứ RED chưa được kiểm chứng từ handoffs**: Không có artifact log RED lưu lại để khẳng định chi tiết. | `PASS`: Migration 21 áp dụng thành công; hàm projection đọc đúng cột; UPDATE `full_name` và INSERT/DELETE `platform_admins` thực thi nguyên tử. |
| **U3** | Auth Admin HTTP adapter và service tạo tài khoản (`POST /users`) | `src/internal/usermanagement/auth_adapter_test.go`, `service_test.go`, `service_native_test.go` | **Thất bại hành vi trên stub chưa triển khai**: Service stub chưa hiện thực ném lỗi not implemented. | `PASS`: Adapter gọi qua Envoy với `email_confirm: true`; không theo redirect; kiểm tra profile sau tạo; tài khoản đăng nhập được ngay bằng mật khẩu; duplicate email trả 409. |
| **U4** | Lấy danh sách user (`GET`) và cập nhật profile/role (`PATCH`) | `src/internal/usermanagement/service_test.go`, `admin_user_handlers_test.go` | **Thất bại hành vi trên stub chưa triển khai**: Service stub chưa hiện thực ném lỗi not implemented. | `PASS`: List đọc `bound + 1`, overflow trả 409 `user_list_capacity_exceeded` không partial list; PATCH khóa dòng `FOR UPDATE`, tự hạ quyền bị chặn 409 `self_admin_demotion_forbidden`. |
| **U5** | Gán vai trò operator/viewer trên Gateway (`PUT /users/{id}`) | `src/internal/usermanagement/postgres_repository_test.go`, `service_test.go` | **Thất bại hành vi trên stub chưa triển khai**: Service stub chưa hiện thực ném lỗi not implemented. | `PASS`: Gán `owner` trả 400 `invalid_role`; hạ owner hiện tại trả 409 `owner_transfer_required`; gán operator/viewer khóa Gateway `FOR UPDATE`, bảo toàn `created_at`. |
| **U6** | Chuyển giao quyền sở hữu Gateway nguyên tử (`PUT /owner`) | `src/internal/usermanagement/postgres_repository_test.go`, `service_test.go` | **Thất bại hành vi trên stub chưa triển khai**: Service stub chưa hiện thực ném lỗi not implemented. | `PASS`: Khóa Gateway cha, xác minh đúng 1 owner hiện hữu; hạ owner cũ thành viewer (bảo toàn quyền đọc); thăng cấp target thành owner; xác minh còn đúng 1 owner trước khi commit. |
| **U7** | Tích hợp composition server, live HTTP E2E và hồi quy hệ thống | `src/cmd/server/usermanagement_native_http_test.go`, các runner CI | **Lỗi biên dịch ban đầu (Compilation Failures)** tại tầng wiring config/composition; lỗi hành vi ở tầng handler là HTTP 500 vs 503/403. | `PASS`: Chạy thành công bài kiểm thử E2E cục bộ kế thừa (`TestNativeUserManagement_E2EFlow`) và hoàn thành toàn bộ 16/16 jobs CI từ xa trên GitHub Actions. |

---

## 4. Bằng chứng Thực thi, Rà soát Độc lập và Xử lý Tồn đọng

### 4.1 Bằng chứng Kiểm thử Nội bộ và Môi trường Chia sẻ
- **Môi trường chạy thực tế:** Bài kiểm thử native E2E (`TestNativeUserManagement_E2EFlow`) và native service tests chạy trên môi trường test chia sẻ (shared test environment) gồm PostgreSQL 16.15 / TimescaleDB 2.29.2 (Schema version 21), Supabase GoTrue v2.196.0 qua Envoy v1.39.1, và role ứng dụng `iot_backend_app` (non-superuser). Kết quả thời gian chạy 0.51s và 13 kịch bản là mốc kiểm tra cục bộ kế thừa (inherited local checkpoint), không phải số liệu đo lường mới trên CI từ xa.
- **Kỷ luật dọn dẹp tài nguyên (Owned Cleanup Discipline):**
  - Bài test chỉ xóa các bản ghi do chính lần chạy đó sở hữu thông qua danh sách UUID mục tiêu và email chính xác do bài test đăng ký trước (`SELECT id FROM auth.users WHERE email = $1`), **tuyệt đối không xóa theo tiền tố rộng**.
  - Câu lệnh đếm kiểm chứng xác nhận còn lại **0 dòng dữ liệu** (`0 rows remaining`) đối với các ID/email sở hữu của bài test. Không thực hiện reset database/volume chung.

### 4.2 Lịch sử Rà soát Mã nguồn và Khắc phục Findings F1–F3
- **Đợt rà soát ban đầu (Initial Review):** Chuyên viên rà soát mã nguồn (`go-reviewer`) từng trả về kết quả **BLOCK** với 5 phát hiện (R1–R5: thứ tự migration, native opt-in và cleanup, environment forwarding, log token, và assertion field tags). Toàn bộ 5 phát hiện này đã được xử lý trong các bản sửa trực tiếp trước khi chuyển sang đợt follow-up.
- **Đợt rà soát tiếp theo (Follow-up Review):** Cả chuyên viên Go review và Security review đều trả về kết quả **WARNING** (không còn finding HIGH/CRITICAL) với 3 phát hiện mức độ MEDIUM:
  1. **Finding F1 (MEDIUM — Repository native cleanup):** Trong `postgres_repository_test.go`, các câu lệnh `Exec` trong hàm dọn dẹp `defer` từng bỏ qua lỗi (`_, _ = ...`), dẫn đến nguy cơ test báo PASS dù bản ghi test chưa được dọn dẹp sạch.
     - *Khắc phục:* Đã chuyển toàn bộ sang sử dụng hàm `checkedCleanup(t, setupPool, cleanupCtx, ...)` và bổ sung câu lệnh truy vấn tổng số dòng còn lại trên cả 5 bảng liên quan, khẳng định `remaining == 0`.
  2. **Finding F2 (MEDIUM — Service-level Auth fixture cleanup & CSPRNG passwords):** Trong `service_native_test.go`, danh sách dọn dẹp tài khoản Auth chỉ ghi nhận `createdUserIDs` sau khi hàm `CreateUser` trả về thành công. Nếu cuộc gọi gặp timeout mạng hoặc lỗi kiểm tra hồ sơ, tài khoản đã được tạo trên Auth nhưng không có ID trong danh sách để dọn dẹp. Đồng thời mật khẩu fixture từng dùng chuỗi cố định.
     - *Khắc phục:* Cập nhật fixture để đăng ký trước email sở hữu duy nhất (`createdUserEmails = append(...)`) trước bất kỳ request tạo tài khoản nào; hàm cleanup thực hiện truy vấn `SELECT id FROM auth.users WHERE email = $1` để tìm và xóa sạch tài khoản mồ côi; mật khẩu fixture được sinh ngẫu nhiên bằng CSPRNG (`randomTestHex(24)`) đạt độ dài an toàn và entropy cao; bổ sung kịch bản kiểm thử giả lập mất phản hồi (`lostCreateResponse`) xác nhận tài khoản được dọn dẹp sạch sẽ chỉ dựa trên email.
  3. **Finding F3 (MEDIUM — Stage 4 runner terminal schema evidence):** Runner Stage 4 (`scripts/tests/stage4_api_runner.py`) từng gán hằng số schema version 21 vào tệp summary JSON ngay cả khi database fixture bỏ qua migration 21 do thiếu bảng Auth.
     - *Khắc phục:* Runner Stage 4 được bổ sung bước thiết lập bảng Auth tối thiểu, chạy migration 4 và migration 21, sau đó đọc trực tiếp toàn bộ chuỗi lịch sử phiên bản `json_agg(version::text || ':' || name ORDER BY version)` từ CSDL trước khi ghi nhận vào báo cáo tóm tắt. *Lưu ý về ranh giới:* Fixture bảng Auth tối thiểu trong runner Stage 4 là giải pháp tương thích để kiểm tra migration 21, **không thay thế và không tương đương** với việc kiểm thử tích hợp dịch vụ GoTrue thật.
- **Tuyên bố Ranh giới Phê duyệt:** Cả ba phát hiện F1, F2, F3 đã được khắc phục trực tiếp trong mã nguồn và bài kiểm thử. Hệ thống **KHÔNG tuyên bố có đợt tái thẩm định (re-approval) độc lập sau cùng**; các kết quả đạt được là kiểm chứng có giới hạn (bounded checks) đã vượt qua các bài test tương ứng và 16/16 jobs CI.

### 4.3 Khắc phục Sự cố Tương tranh Flaky Race trong `mqttingress` và Phân định Lỗi CI
Lịch sử kiểm thử ghi nhận hai sự cố kiểm tra CI độc lập với nguyên nhân khác nhau:
1. **Lỗi `gatewaysim` tại Run [39265402512](https://github.com/TVTIT/project1_iot_server/actions/runs/39265402512) (commit `dfe3a30`):** Job `lint-and-test` từng ghi nhận lỗi test trong package `gatewaysim`. CI test guard đã làm đúng nhiệm vụ bảo mật che giấu chi tiết lỗi động, và lỗi này không thể tái hiện được trong 30+50 lượt chạy lặp lại cục bộ cũng như trong toàn bộ test suite Go. Nguyên nhân gốc chính xác của lần thất bại này chưa được khẳng định (unverified root cause).
2. **Lỗi `mqttingress` Flaky Race tại Run [39269645274](https://github.com/TVTIT/project1_iot_server/actions/runs/39269645274) (commit `7d2207c`):** Hoàn toàn độc lập với lỗi gatewaysim, bài kiểm thử `TestACKOnlyAfterConfirmedCommit` trong package `internal/mqttingress` gặp lỗi tương tranh ngẫu nhiên do hàm test đọc `ing.Snapshot()` trước khi worker tăng bộ đếm `ACKPublished`. Lỗi này đã được tái hiện thành công cục bộ và được khắc phục triệt để tại commit `35b6136f22c5a1c819afefc12003dba3ba613062` bằng lệnh đồng bộ `ing.Drain(drainCtx)` trước khi đọc snapshot (không sửa đổi mã nguồn runtime). Độ ổn định đã được kiểm chứng qua 1.000 lượt chạy `-race` và 1.000 lượt chạy `-race -cover` liên tiếp đều PASS.

### 4.4 Kết quả Kiểm chứng CI từ xa trên GitHub Actions
Pipeline CI trên GitHub Actions đã hoàn thành thành công toàn bộ **16/16 jobs** tại commit sạch `35b6136f22c5a1c819afefc12003dba3ba613062` (Run [39272190456](https://github.com/TVTIT/project1_iot_server/actions/runs/39272190456)):
- `migration-check` (Job 118127108649): PASS (48s)
- `dynsec-native-integration` (Job 118127109008): PASS (1m42s)
- `stage2-e2e` (Job 118127109080): PASS (29s)
- `lint-and-test` (Job 118127109089): PASS (3m38s, bao gồm `go vet`, `golangci-lint`, và toàn bộ unit tests với `-race`)
- `provisioning-integration` (Job 118127109121): PASS (2m15s)
- `cross-compile` (Job 118127109153): PASS (1m24s)
- `legacy-runtime-image-build` (Job 118127109262): PASS (1m16s)
- `dynsec-bootstrap-tests` (Job 118127109389): PASS (15s)
- `mosquitto-runtime-integration` (Job 118127109472): PASS (14s)
- `stage4-api-integration` (Job 118127109577): PASS (2m26s)
- `docker-build` (Job 118127109624): PASS (1m41s)
- `authorization-integration` (Job 118127109643): PASS (1m38s)
- `mosquitto-credential-integration` (Job 118127109683): PASS (2m22s)
- `auth-integration` (Job 118127109778): PASS (2m51s)
- `stage3-uplink-integration` (Job 118127109787): PASS (5m36s)
- `native-compose-smoke` (Job 118129034156): PASS (2m29s)

*Lưu ý bảo toàn bằng chứng:* Thành công của các job CI kế thừa lịch sử (deferred legacy jobs) không ngụ ý các kịch bản native mở rộng ngoài phạm vi đã chạy. Tệp báo cáo chẩn đoán cục bộ `/tmp/opencode/admin-users-three-fixes-stage4.json` lưu vết xuất xứ của baseline `573abde` và bộ ảnh container `be646cf...` / `a3a99bbc...`, được giữ nguyên vẹn như một bằng chứng lịch sử và không bị gán nhầm cho commit CI hiện tại. Bộ kiểm thử chẩn đoán Python đạt **898 tests PASS** (ghi nhận tại `/tmp/opencode/ci-diagnostic-python-tests.log`; các số liệu 892 và 887 trước đây là mốc lịch sử).

---

## 5. Quy trình Vận hành và Đối soát An toàn (Safe Operational Procedures)

### 5.1 Xử lý Sự cố Timeout khi Tạo Tài khoản (`POST /v1/admin/users`)
- Khi gửi yêu cầu tạo tài khoản mà gặp lỗi gián đoạn mạng hoặc phản hồi HTTP `503 Service Unavailable`, **tuyệt đối không tự động gửi lại request ngay lập tức (no blind retry)**.
- Do Go backend gọi sang Supabase Auth trước rồi mới thẩm định lại profile, có thể tài khoản Auth đã được ghi nhận nhưng phản hồi mạng về Go bị chậm/mất gói hoặc xử lý profile bị gián đoạn.
- **Việc email chưa xuất hiện trong danh mục `GET /v1/admin/users` KHÔNG CHỨNG MINH rằng request cũ không thể hoàn tất muộn hơn trên GoTrue.**
- **Quy trình đối soát chuẩn của Administrator (Supervised Reconciliation):**
  1. Chờ hệ thống ổn định và gọi `GET /v1/admin/users`.
  2. Nếu danh sách báo lỗi hoặc trả về thiếu hồ sơ (missing profile), quản trị viên cần đối soát trực tiếp log hệ thống hoặc cơ sở dữ liệu trước khi hành động tiếp.
  3. Chỉ khi xác nhận chắc chắn tài khoản chưa được tạo và không còn tiến trình dở dang, quản trị viên mới tiến hành gửi lại request tạo tài khoản bằng tay.
  4. Go backend **không bao giờ tự ý xóa tài khoản Auth** nếu bước kiểm tra profile gặp lỗi, đảm bảo dữ liệu không bị thất thoát và luôn sẵn sàng cho việc đối soát thủ công.
  5. *Lưu ý về biến môi trường:* Khi thay đổi cấu hình môi trường trong `.env`, quản trị viên bắt buộc phải tái tạo container (`docker compose up -d --force-recreate backend` hoặc `docker compose up -d`), việc chỉ chạy `docker compose restart` không áp dụng biến môi trường mới.

### 5.2 Xử lý Replay Xung đột khi Khởi tạo Trạm (`PUT /admin/gateways/{id}`) sau Chuyển Owner
- Theo quy tắc bất biến của hệ thống, một request khởi tạo trạm (`PUT /v1/admin/gateways/{gateway_id}`) khi gửi lại trùng khớp (replay) bắt buộc phải có `owner_user_id` trùng khớp với chủ sở hữu hiện tại trong CSDL.
- Sau khi thực hiện chuyển giao quyền sở hữu qua `PUT /v1/admin/gateways/{gateway_id}/owner`, nếu hệ thống hoặc script tự động gửi lại request PUT cũ chứa `owner_user_id` của chủ sở hữu cũ, Go backend sẽ phản hồi HTTP **`409 Conflict`**.
- Đây là **hành vi an toàn có chủ đích** nhằm bảo vệ quyền của chủ sở hữu mới; quản trị viên cần cập nhật script vận hành với `owner_user_id` mới nếu muốn thực hiện kiểm tra lũy đẳng.

### 5.3 Xử lý Lỗi Giới hạn Dung lượng Danh sách (`409 user_list_capacity_exceeded`)
- API `GET /v1/admin/users` được thiết kế phục vụ quy mô vừa và nhỏ của đồ án đơn thành viên, tải toàn bộ danh sách trong một phản hồi duy nhất có kiểm soát bộ nhớ.
- Khi số lượng người dùng vượt quá `USER_LIST_MAX_USERS` (mặc định 1.000) hoặc số lượng quyền trạm vượt quá `USER_LIST_MAX_MEMBERSHIPS` (mặc định 10.000), hoặc dung lượng JSON vượt quá `USER_LIST_MAX_RESPONSE_BYTES` (mặc định 2 MiB), server sẽ từ chối với HTTP 409 thay vì trả danh sách bị cắt xén.
- **Biện pháp xử lý:** Quản trị viên nâng các giá trị biến môi trường tương ứng trong tệp `.env` của backend và tái tạo container backend (`docker compose up -d --force-recreate backend`).

---

## 6. Hạn chế Kỹ thuật và Tồn đọng (Limitations & Backlog)

Tuân thủ quy định tại `AGENTS.md` Mục 0 và 16, các hạn chế dưới đây được ghi nhận chính thức vào danh mục tồn đọng chung tại [`docs/dynsec/limitations-and-backlog.md`](../dynsec/limitations-and-backlog.md), không làm phát sinh hệ thống backlog mới:

1. **Không có chính sách đếm để bảo vệ Admin cuối cùng (No Last-Admin Count Policy):**
   - Hệ thống áp dụng quy tắc cấm tự hạ quyền admin của chính caller (`self_admin_demotion_forbidden`), nhưng không thực hiện khóa đếm số lượng admin còn lại trong CSDL.
   - Tình huống mất toàn bộ quyền admin có thể xảy ra nếu hai admin thực hiện hạ quyền chéo đồng thời qua API (`concurrent API cross-demotions`), hoặc do các can thiệp trực tiếp ngoài API trên CSDL.
   - *Cách xử lý:* Quản trị vận hành bằng quy trình con người. Khi cần thiết, kỹ sư hệ thống có thể cấp lại quyền admin trực tiếp bằng lệnh SQL vào `public.platform_admins`.
2. **Owner cũ trở thành Viewer và giữ nguyên quyền đọc:**
   - Khi chuyển giao quyền sở hữu trạm, owner cũ không bị xóa khỏi trạm mà tự động chuyển thành `viewer`, tiếp tục đọc được dữ liệu viễn trắc và Digital Twin của trạm đó (chỉ bị thu hồi quyền ghi cảm biến).
   - *Cách xử lý:* Đây là thiết kế chủ động nhằm tránh mất quyền giám sát đột ngột của người bàn giao. Nếu muốn thu hồi hoàn toàn quyền đọc của owner cũ, quản trị viên sử dụng script CSDL nội bộ.
3. **Phạm vi quản trị tài khoản có giới hạn (Bounded CRUD Scope):**
   - Không có API xóa tài khoản (`Delete`), khóa/ban tài khoản, đổi email, đổi mật khẩu, reset mật khẩu, gửi email lời mời (invitations) hay xóa hoàn toàn membership qua Go Backend.
   - *Cách xử lý:* Các tác vụ này được thực hiện thông qua Supabase Studio hoặc CLI nội bộ có thẩm định.
4. **Danh sách người dùng không phải snapshot liên tục qua nhiều request:**
   - Danh sách trả về từ `GET /v1/admin/users` là kết quả truy vấn tại một thời điểm; hệ thống không hỗ trợ cơ chế phân trang cursor phức tạp hay đảm bảo snapshot nhất quán nếu có các thao tác tạo/sửa user diễn ra đồng thời giữa các lần gọi.
5. **Không đưa ra cam kết về độ bền phần cứng hay tương tranh tùy ý:**
   - Môi trường kiểm thử hiện tại tuân thủ quy chế test-only theo `AGENTS.md` Mục 0.5. Hệ thống chưa kiểm chứng độ bền khi mất điện đột ngột phần cứng (hardware power loss), chưa chạy trên bo mạch nhúng ARM, và không khẳng định mọi lịch trình đồng thời tùy ý đều không gặp lỗi.

---

## 7. Kết luận và Bàn giao Tiếp theo

Lát cắt Tasks U0–U8 về Quản trị Người dùng và Quyền Thành viên Gateway đã hoàn thành có giới hạn theo mục tiêu đề ra, vượt qua các bài kiểm thử đơn vị, kiểm thử tích hợp native E2E kế thừa, và được xác nhận xanh 16/16 jobs trên GitHub Actions Remote CI.

Theo kế hoạch tổng thể của dự án (`Backend_summary_plan.md` và `AGENTS.md`), hệ thống đã sẵn sàng cho các giai đoạn tiếp theo:
- **Giai đoạn 5 (Stage 5):** Điều khiển thiết bị Bản sao số (Digital Twin Downlink Control: `desired_state` mutation, Transactional Outbox, MQTT command publication, và feedback correlation).
- **Giai đoạn 6 (Stage 6):** Tiến trình đối soát trạng thái điều khiển (Command reconciliation worker).
- **Giai đoạn 7 (Stage 7):** WebSocket truyền dữ liệu viễn trắc thời gian thực (Realtime telemetry streaming).
- **Giai đoạn 8 (Stage 8):** Quản lý tải lên hình ảnh qua Supabase Storage.
- **Giai đoạn 9 (Stage 9):** Nghiệm thu, đo tải và triển khai (Acceptance, workload measurement & deployment).