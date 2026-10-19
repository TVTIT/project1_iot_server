# Tài liệu Client — 07: Quy chuẩn Hợp đồng Admin API (Platform Admin API Client Contract)

**Cập nhật:** 2026-10-19
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:**  
- `AGENTS.md` (Mục 0, 5, 6, 6.5, 7, 10, 15, 16, 18; Stage 4 & Admin User Management Progress & Reconciliation)
- [`docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md`](../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md) (Kế hoạch chi tiết quản lý User và Gateway Membership)
- [`docs/backend/admin-users-and-gateway-membership.md`](../backend/admin-users-and-gateway-membership.md) (Báo cáo tổng kết và bàn giao Task U0–U8)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan hệ thống)  
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực người dùng và Quản lý phiên)  
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Cơ chế phân quyền người dùng và trạm Gateway)  
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API đọc nghiệp vụ chuẩn và Digital Twin)
- `docs/client/05_UPCOMING_FEATURES_ROADMAP.md` (Lộ trình phát triển tính năng các giai đoạn)  
- `docs/client/06_PROJECT_SETUP_AND_ENV.md` (Cấu hình môi trường và biến bảo mật)  
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái Client)  
- `docs/protocols/stage4-read-and-entity-api.md` (Đặc tả giao thức Stage 4 Read & Entity Mutation API)
- `docs/backend/stage-4-history-and-twin-api.md` (Tài liệu tổng kết và bàn giao Stage 4 API)
- `src/internal/httpserver/admin_user_handlers.go` (Mã nguồn Admin User & Gateway Membership Handlers)
- `src/internal/usermanagement/service.go` (Domain Service quản lý User và Membership)
- `src/internal/usermanagement/postgres_repository.go` (PostgreSQL Repository & Transactional Logic)
- `src/internal/usermanagement/auth_adapter.go` (Supabase Auth Admin HTTP Adapter qua Envoy)
- `src/internal/httpserver/admin_gateway_handlers.go` (Mã nguồn Admin Gateway Inventory & Provisioning)
- `src/internal/httpserver/admin_sensor_handlers.go` (Mã nguồn Admin Sensor Provisioning)
- `src/internal/httpserver/admin_mqtt_credential_handlers.go` (Mã nguồn Admin MQTT Credential Handlers)
- `src/internal/httpserver/mutation_handlers.go` (Mã nguồn HTTP Handlers POST/PATCH /v1/digital-twins)
- `src/internal/httpserver/mutation_dto.go` (DTOs & Phân loại lỗi ClassifyMutationError)
- `src/internal/digitaltwin/create_service.go` (Domain Service khởi tạo thực thể POST)
- `src/internal/digitaltwin/patch_service.go` (Domain Service cập nhật metadata PATCH)
- `src/internal/digitaltwin/postgres_gateway_patch.go` (Triển khai Transactional Gateway PATCH)
- `src/internal/digitaltwin/postgres_sensor_patch.go` (Triển khai Transactional Sensor PATCH)
- `src/cmd/server/credential.go` (Tầng composition Credential & denyOnlyResetPrerequisite)
- `migrations/000021_admin_user_management.up.sql` (Migration CSDL cấp quyền và security-definer function)
- [docs/dynsec/INDEX.md](../dynsec/INDEX.md) (Tổng quan kiến trúc và ma trận bằng chứng DynSec)
- [docs/dynsec/flows-and-api.md](../dynsec/flows-and-api.md) (Chi tiết các luồng vận hành và hợp đồng HTTP API DynSec)
- [docs/dynsec/limitations-and-backlog.md](../dynsec/limitations-and-backlog.md) (Hạn chế kỹ thuật và tồn đọng DynSec)
- **Kiểm chứng Remote CI:** GitHub Actions Run [39272190456](https://github.com/TVTIT/project1_iot_server/actions/runs/39272190456) tại commit `35b6136f22c5a1c819afefc12003dba3ba613062` (16/16 jobs hoàn thành thành công, bao gồm `lint-and-test` job 118127109089, `stage4-api-integration` job 118127109577, `stage3-uplink-integration` job 118127109787, và `native-compose-smoke` job 118129034156).

---

## 1. Mục tiêu và Ranh giới Phạm vi (Scope & Architectural Boundaries)

Tài liệu này định nghĩa **Hợp đồng giao tiếp HTTP API Quản trị nền tảng (Platform Admin API Client Contract)** chính thức cho:
1. Bảy (07) endpoint quản trị hạ tầng Stage 2 thuộc tiền tố `/v1/admin/*` đã được hiện thực trong mã nguồn Go backend (`src/cmd/server`, `src/internal/httpserver`), cùng 1 tuyến đường reset có giám sát đã đăng ký cú pháp nhưng áp dụng rào chắn an toàn fail-closed.
2. Hai (02) endpoint khởi tạo thực thể và biến đổi siêu dữ liệu (Entity Creation & Metadata Mutation) Stage 4 thuộc tiền tố `/v1/digital-twins` (`POST /v1/digital-twins` và `PATCH /v1/digital-twins/{entity_id}/attrs`) có liên quan trực tiếp đến thẩm quyền Platform Admin và Gateway Owner.
3. Năm (05) endpoint quản trị tài khoản người dùng và quyền thành viên Gateway (Admin User & Gateway Membership Management) mới được hiện thực theo Kế hoạch chi tiết và Migration 21 (`000021_admin_user_management.up.sql`): `POST /v1/admin/users`, `GET /v1/admin/users`, `PATCH /v1/admin/users/{user_id}`, `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`, và `PUT /v1/admin/gateways/{gateway_id}/owner`.

### 1.1 Danh mục Endpoint Quản trị Hệ thống

#### A. Nhóm Endpoint Quản trị Hạ tầng Stage 2 (`/v1/admin/*`):
1. `GET /v1/admin/gateways`: Danh mục kiểm kê Gateway toàn hệ thống (Admin Gateway Inventory) cho Platform Admin, độc lập với tư cách thành viên, hỗ trợ phân trang keyset cursor (`limit`, `after`).
2. `PUT /v1/admin/gateways/{gateway_id}`: Khởi tạo mới hoặc kiểm tra lũy quyền thực thể Gateway (Create-or-Exact-Replay), gán người sở hữu (`owner`) ban đầu, và tự động tạo đồ thị Digital Twin (`twin_entities`, `twin_states`).
3. `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`: Khởi tạo mới hoặc kiểm tra lũy quyền Sensor thuộc Gateway cha (Create-or-Exact-Replay) bởi Platform Admin, tự động tạo đồ thị Digital Twin và thiết lập quan hệ phân cấp `twin_relationships` (`hasSensor`).
4. `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`: Truy vấn metadata trạng thái thông tin xác thực MQTT của Gateway (không chứa mật khẩu).
5. `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`: Cấp phát ban đầu thông tin xác thực MQTT (DynSec username/password) cho Gateway; trả về mật khẩu plaintext một lần duy nhất (`secret_returned: true`); replay cùng key trả về `201 Created` metadata-only (lược bỏ trường password).
6. `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`: Xoay vòng mật khẩu MQTT cho Gateway đang hoạt động (`active`); trả về mật khẩu mới một lần duy nhất; replay cùng key trả về `200 OK` metadata-only.
7. `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`: Vô hiệu hóa và thu hồi thông tin xác thực MQTT của Gateway, ngắt kết nối session active và vô hiệu hóa quyền truy cập broker (`200 OK` metadata-only; không có cơ chế tự động khôi phục bí mật, nhưng cho phép re-provision với key mới).

- **Ngoại lệ Tuyến đường Reset Đã đăng ký Rào chắn An toàn (Registered Deny-Only Reset Route):**
  - Tuyến đường `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/reset` đã được đăng ký trong router (`router.go`) và bộ phân tích cú pháp `parseCredentialResetRequest` kiểm tra nghiêm ngặt payload lý do (`reason`), nhưng tại tầng composition runtime (`src/cmd/server/credential.go`), thành phần phụ thuộc được gắn kiểu `denyOnlyResetPrerequisite{}`.
  - Khi tính năng credential được kích hoạt (`MQTT_CREDENTIAL_API_ENABLED=true`), runtime này **LUÔN LUÔN từ chối (fail-closed)** mọi yêu cầu hợp lệ vượt qua parser với mã lỗi HTTP **`503 Service Unavailable`** (`code: "service_unavailable"`, `message: "credential request failed"`). Nếu tính năng credential bị tắt (`MQTT_CREDENTIAL_API_ENABLED=false`), server phản hồi HTTP **`503 Service Unavailable`** với mã lỗi `code: "credential_runtime_disabled"` (chứ không phải `service_unavailable`). Nếu vi phạm xác thực, phân quyền hoặc cú pháp request, server phản hồi các mã lỗi parser tương ứng (`401`, `403`, `400`, `413`, `415`). Đây là rào chắn an toàn kỹ thuật, **CHƯA PHẢI là quy trình phục hồi khả dụng** (xem chi tiết tại [Mục 4.7](#47-ngoại-lệ-đặt-lại-credential-có-giám-sát-post-v1admingatewaysgateway_idmqtt-credentialreset)).

#### B. Nhóm Endpoint Khởi tạo Thực thể và Sửa đổi Metadata Stage 4 (`/v1/digital-twins`):
8. `POST /v1/digital-twins`: Khởi tạo mới hoặc gửi lại trùng khớp (Create-or-Exact-Replay) thực thể Digital Twin Gateway hoặc Sensor theo chuẩn NGSI-LD; trả về `201 Created` kèm header `Location: /v1/digital-twins/<escaped_urn>` khi tạo mới hoặc `200 OK` (không Location) khi replay trùng khớp hoàn toàn payload cũ; trả về `409 Conflict` nếu trùng định danh nhưng sai lệch thông tin biểu diễn.
9. `PATCH /v1/digital-twins/{entity_id}/attrs`: Cập nhật siêu dữ liệu hiển thị (`name`, `description`) của thực thể Gateway hoặc Sensor thông qua Property fragment chuẩn hóa theo V1.9.1 profile; trả về `200 OK` kèm bản ghi metadata đã cập nhật (hoặc giữ nguyên timestamp nếu gửi bản vá trùng giá trị hiện tại — no-op). Đường dẫn gốc không có `/attrs` nhận `405 Method Not Allowed`.

#### C. Nhóm Endpoint Quản trị Tài khoản Người dùng và Thành viên Gateway (`/v1/admin/users`, `/v1/admin/gateways/...`):
10. `POST /v1/admin/users`: Tạo tài khoản người dùng mới (được cấu hình `email_confirm: true` qua Supabase Auth Admin API), tự động tạo bản ghi hồ sơ `public.profiles`, trả về `201 Created` kèm header `Location: /v1/admin/users/{id}` và bản ghi tài khoản không chứa secret.
11. `GET /v1/admin/users`: Truy vấn danh sách toàn bộ người dùng trong giới hạn tài nguyên cấu hình (`items: [...]`), kèm thông tin vai trò nền tảng `platform_role` và danh sách quyền Gateway `gateway_roles: [{gateway_id, role}]`, không hỗ trợ query pagination/search, vượt giới hạn trả về `409 Conflict` (không cắt xén partial list).
12. `PATCH /v1/admin/users/{user_id}`: Cập nhật nguyên tử trong một transaction tên hiển thị `full_name` (trong `public.profiles`) và/hoặc vai trò nền tảng `platform_role` (cấp/thu hồi `public.platform_admins`), cấm tự hạ quyền admin (`409 Conflict`).
13. `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`: Gán vai trò thành viên `operator` hoặc `viewer` cho người dùng trên Gateway trong `public.user_gateways`; từ chối gán `owner` qua route này (`400 Bad Request`); nếu người dùng đang là owner thì từ chối (`409 Conflict`).
14. `PUT /v1/admin/gateways/{gateway_id}/owner`: Chuyển giao quyền sở hữu trạm Gateway nguyên tử trong `public.user_gateways`: người nhận trở thành `owner`, owner cũ được hạ thành `viewer` (bảo toàn quyền đọc, không xóa membership), bảo toàn ràng buộc duy nhất một owner trên trạm.

**Ranh giới phạm vi nghiệp vụ tài khoản (Account CRUD Bounded Scope):**
Chức năng quản trị tài khoản qua Go Backend được giới hạn nghiêm ngặt theo danh mục 5 endpoint trên:
- **ĐÃ TRIỂN KHAI:** Tạo tài khoản kèm mật khẩu (auto-confirm), xem danh sách toàn bộ kèm quyền trạm, đổi tên hiển thị, cấp/thu hồi quyền Platform Admin, gán quyền operator/viewer, và chuyển quyền sở hữu trạm Gateway.
- **NGOÀI PHẠM VI (KHÔNG TRIỂN KHAI):** Xóa tài khoản (`Delete`), khóa/ban tài khoản, đổi email, đổi mật khẩu người dùng, reset mật khẩu, gửi email lời mời (invitations), tự đăng ký công khai (public signup), hoặc xóa hoàn toàn bản ghi thành viên (`DELETE membership`). Khi cần xử lý các tác vụ ngoài phạm vi này, quản trị viên sử dụng công cụ quản trị hạ tầng (Supabase Studio / CLI) dưới sự giám sát trực tiếp.

---

### 1.2 Phân định Rạch ròi giữa `/v1/digital-twins` và `/v1/admin/*` (Giải thích Dễ hiểu)

Để tránh nhầm lẫn trong thiết kế và tích hợp client, các kỹ sư cần nắm rõ sự khác biệt cốt lõi giữa hai nhóm API:

| Tiêu chí | Nhóm Quản trị Hạ tầng `/v1/admin/*` (Stage 2) | Nhóm Thực thể & Siêu dữ liệu `/v1/digital-twins` (Stage 4) |
|---|---|---|
| **Tiền tố URL (Path Prefix)** | Bắt buộc bắt đầu bằng `/v1/admin/...` | Bắt đầu bằng `/v1/digital-twins/...` (nằm trong nhóm route nghiệp vụ xác thực `/v1/*`). |
| **Phạm vi Phân quyền (RBAC)** | **Admin toàn cầu (Globally Admin-Only):** Bắt buộc người dùng phải tồn tại trong bảng `public.platform_admins`. Mọi người dùng thường (kể cả Gateway `owner`) đều bị từ chối `403 Forbidden`. | **Không phải toàn bộ là Admin-only!** Phân quyền phụ thuộc vào loại thực thể (`entity_type`) và hành động cụ thể. |
| **Quyền tạo thực thể (`POST`)** | N/A (Stage 2 dùng `PUT` create-or-exact-replay). | - Tạo Gateway: **Bắt buộc Platform Admin** (vì liên quan chỉ định chủ sở hữu `owner_user_id`).<br>- Tạo Sensor: **Platform Admin HOẶC Gateway Owner** (người sở hữu trạm có quyền thêm cảm biến cho trạm của mình). Trả về `Location` khi `201`. |
| **Quyền sửa metadata (`PATCH`)** | N/A (Stage 2 không hỗ trợ PATCH; gọi PUT sửa thông tin sẽ nhận `409 Conflict`). | - Sửa Gateway: **Bắt buộc Platform Admin** (sửa `name`, `description`). Mọi tài khoản không phải Admin (kể cả Gateway `owner`) đều bị từ chối `403 Forbidden`.<br>- Sửa Sensor: **Platform Admin HOẶC Gateway Owner** (chỉ sửa `name`; `unit` là bất biến).<br>- Path bắt buộc là `/attrs`, nhận Property fragment.<br>- Vai trò `operator` / `viewer`: Luôn bị từ chối `403 Forbidden`.<br>- Người không phải thành viên trạm: Bị từ chối `404 Not Found`. |
| **Tác động tới quyền Đọc dữ liệu** | Platform Admin không tự động đọc được dữ liệu nghiệp vụ của trạm nếu không có quyền trong `user_gateways`. | **Admin sửa metadata KHÔNG bypass quyền đọc!** Platform Admin tạo hoặc patch Gateway/Sensor vẫn nhận `404 Not Found` khi gọi `GET /v1/digital-twins/{id}`, `GET /v1/digital-twins/{id}?format=simplified` hay `GET /v1/digital-twins/{id}/temporal` nếu chưa được gán thành viên trong `user_gateways`. |
| **Khởi tạo tài khoản & xác thực** | Trực tiếp cấp phát / xoay vòng / thu hồi mật khẩu MQTT DynSec cho trạm. | **TUYỆT ĐỐI KHÔNG cấp mật khẩu MQTT hay tài khoản Auth!** Lệnh POST tạo Gateway/Sensor chỉ ghi nhận thực thể vào bảng CSDL quan hệ và Digital Twin; việc cấp mật khẩu MQTT vẫn phải thực hiện qua API Credential hoặc quy trình riêng. |

---

### 1.3 Ranh giới Kiến trúc và Mô hình Quản trị Tập trung

- **Không bắt buộc xây dựng ứng dụng Web Admin riêng biệt (No Custom Admin App Requirement):** Các thao tác quản trị nền tảng được thiết kế phục vụ công cụ vận hành tin cậy của Platform Administrator (công cụ dòng lệnh CLI, curl script, hoặc Supabase Studio phối hợp công cụ bảo vệ). Ứng dụng Flutter Web Client thông thường chỉ tiêu thụ các API đọc nghiệp vụ chuẩn ([Tài liệu 04](04_REST_API_CLIENT_CONTRACT.md)) trừ khi người dùng đăng nhập là một Platform Admin đã được thẩm định.
- **Thẩm quyền Platform Admin tách biệt hoàn toàn bảng thành viên (`user_gateways`):**
  - Quyền quản trị tối cao của Platform Admin được định đoạt duy nhất bằng sự tồn tại của `user_id` trong bảng cơ sở dữ liệu `public.platform_admins` tại PostgreSQL.
  - Tư cách thành viên trong `user_gateways` (`owner`, `operator`, `viewer`) **đứng riêng lẻ hoàn toàn không cấp quyền Platform Admin**; người dùng chỉ có vai trò trong `user_gateways` mà không có bản ghi trong `platform_admins` sẽ nhận mã lỗi `403 Forbidden` khi truy cập `/v1/admin/*`.
  - **Tài khoản có thể mang đồng thời cả hai vai trò độc lập (Legitimate Dual-Role):** Một người dùng hoàn toàn có thể vừa là thành viên (ví dụ: `owner` của một Gateway) vừa độc lập là một Platform Admin trong `platform_admins`. Hệ thống thẩm định từng ranh giới độc lập, không cấm tài khoản mang hai vai trò song song.
  - **Đối chiếu giữa API Kiểm kê Quản trị và API Đọc nghiệp vụ:**
    - `GET /v1/admin/gateways` là danh mục kiểm kê toàn cầu độc lập với quyền sở hữu, chỉ dành cho Platform Admin (`platform_admins`). Trả về danh sách Gateway kèm phân trang keyset cursor (`limit`, `after`), không chứa thông tin vai trò hay bí mật.
    - `GET /v1/gateways` là API đọc nghiệp vụ người dùng, chỉ trả về các trạm mà người dùng hiện tại có liên kết quyền trong `user_gateways` kèm vai trò (`role`). Một Platform Admin nếu chưa được gán quyền trong `user_gateways` cho trạm nào thì khi gọi `GET /v1/gateways` vẫn nhận mảng rỗng `{"items":[]}` (Admin không tự động bypass CSDL nghiệp vụ).
  - **Ranh giới Phân quyền Đọc dữ liệu Chi tiết (Read Isolation Boundary):**
    - Toàn bộ các API truy vấn chuỗi thời gian và trạng thái trạm (`GET /v1/digital-twins`, `GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}?format=simplified`, `GET /v1/digital-twins/{entity_id}/temporal`) đều thực thi thẩm định quyền thành viên thông qua CTE SQL `user_gateways`. Các route cũ (`/v1/telemetry/history`, `/{id}/state`, `/{id}/history`) đã thu hồi và luôn trả về `404 Not Found`.
    - **Platform Admin KHÔNG được tự động bypass bộ lọc này:** Một Platform Admin thực hiện khởi tạo trạm (`POST /v1/digital-twins` hoặc `PUT /v1/admin/gateways/{id}`) và gán cho người khác làm `owner_user_id` thì chính Admin đó **KHÔNG THỂ** đọc dữ liệu viễn trắc, trạng thái hiện thời, hay lịch sử của trạm đó (nhận `404 Not Found`), trừ khi Admin được thêm bản ghi vào `user_gateways` cho trạm tương ứng.
- **Không sáng tác endpoint tự kiểm tra quyền Admin (No Admin Self-Query Endpoint):** Go backend hiện tại không cung cấp endpoint kiểu `/v1/admin/me` hay `/v1/admin/whoami`. Phía client không được sáng tác hoặc gọi các endpoint giả định này.
- **Phạm vi API Quản lý Tài khoản & Quyền Thành viên qua Go Backend:**
  - Khác với giai đoạn trước (khi toàn bộ việc tạo tài khoản phải thực hiện thủ công qua Supabase Studio), Go Backend hiện cung cấp năm (05) endpoint quản trị tài khoản và quyền thành viên dưới `/v1/admin/users` và `/v1/admin/gateways/...` dành riêng cho Platform Admin (thẩm định qua `public.platform_admins`).
  - **Khóa hạ tầng đặc quyền `SUPABASE_SERVICE_ROLE_KEY` TUYỆT ĐỐI KHÔNG chia sẻ cho client:** Backend sử dụng khóa này ở tầng server nội bộ để gọi sang Supabase Auth Admin API thông qua Envoy Gateway (`http://supabase-envoy:8000/auth/v1/admin/users`). Human caller chỉ gửi JWT cá nhân (`role: "authenticated"`).
  - **Ranh giới Bounded CRUD:** Backend chỉ hỗ trợ tạo tài khoản, đọc danh sách, đổi tên/vai trò toàn cục, gán operator/viewer và chuyển owner. Backend **KHÔNG** cung cấp API Delete/ban tài khoản, đổi email, đổi/reset mật khẩu, gửi email lời mời, hay xóa hoàn toàn membership.
- **Trạng thái cấu hình Root Docker Compose: Tính năng Credential API mặc định TẮT:**
  - Trong tệp cấu hình triển khai `docker-compose.yml`, biến môi trường được đặt mặc định là:
    ```yaml
    MQTT_CREDENTIAL_API_ENABLED: "false"
    ```
  - **Quy trình triển khai có rào chắn (Rollout Gates):** Tính năng này chỉ được kích hoạt có chủ đích bởi kỹ sư vận hành hạ tầng khi triển khai broker DynSec. Phía Client **không thể tự ý bật tính năng này thông qua cờ hay header HTTP**.
  - **Thứ tự ưu tiên trước cờ tính năng (Precedence before feature gate):** Request thiếu hoặc sai JWT luôn nhận `401 Unauthorized` trước; request từ người dùng không phải admin luôn nhận `403 Forbidden` trước. Chỉ khi một Platform Admin đã xác thực hợp lệ gọi vào thì server mới kiểm tra cờ tính năng và phản hồi mã lỗi HTTP `503 Service Unavailable` (`code: "credential_runtime_disabled"`).

---

## 2. Thứ tự Ưu tiên Ủy quyền và Xử lý Lỗi Toàn cục (Authorization Precedence & Global Error Handling)

Mọi request gửi tới các endpoint quản trị đều phải trải qua chuỗi middleware bảo mật nghiêm ngặt theo đúng thứ tự ưu tiên sau:

```text
[HTTP Request]
       |
       v
1. Request ID & Cache Gate Middleware
   - Sinh hoặc xác thực X-Request-ID (1..128 ký tự an toàn)
   - Tuyến đường admin user management (/v1/admin/users, /v1/admin/gateways/.../users, /v1/admin/gateways/.../owner):
     Gán Cache-Control: no-store trên toàn bộ 5 route; gán thêm Pragma: no-cache trên các mutation (POST, PATCH, PUT)
   - Tuyến đường admin inventory (GET /v1/admin/gateways) và credential gán: Cache-Control: no-store, Pragma: no-cache
   - Tuyến đường provisioning (PUT) gán: Cache-Control: no-store
       |
       v
2. Authentication Middleware (/v1/*)
   - Kiểm tra header: Authorization: Bearer <human_supabase_jwt>
   - Xác thực chữ ký HS256 cục bộ, thời hạn hiệu lực, issuer, audience
   - Bắt buộc claim: role == "authenticated"
   -> Thất bại: HTTP 401 Unauthorized (code: "unauthorized", message: "authentication required")
       |
       v
3. Platform Admin Authorization Middleware (/v1/admin/*)
   - Tra cứu user_id trong bảng PostgreSQL public.platform_admins
   -> Lỗi kết nối / timeout / cancelled database: HTTP 503 Service Unavailable (code: "service_unavailable")
   -> Không tồn tại trong platform_admins: HTTP 403 Forbidden (code: "forbidden", message: "insufficient permissions")
       |
       v
4. Endpoint Feature Gate & Startup Barrier (Dành riêng cho nhóm route Credential)
   - Kiểm tra cờ cấu hình CredentialAPIEnabled:
     -> Nếu false: HTTP 503 Service Unavailable (code: "credential_runtime_disabled")
   - Kiểm tra rào chắn khởi động CredentialStartupReadiness.Ready():
     -> Nếu chưa sẵn sàng hoặc nil dependency: HTTP 503 Service Unavailable (code: "service_unavailable")
       |
       v
5. Request Parser & Input Validation
   - Kiểm tra Query Parameters (cấm query lạ/thừa trên POST/PATCH/PUT/GET /v1/admin/users)
   - Kiểm tra Content-Type (bắt buộc application/json trên mutation body)
   - Giới hạn kích thước Body nghiêm ngặt (ADMIN_MAX_BODY_BYTES)
   - Kiểm tra cú pháp JSON khắt khe (cấm duplicate keys, unknown fields, null values trên non-nullable fields)
   -> Vi phạm: HTTP 400 Bad Request, HTTP 413 Payload/Request Too Large, hoặc HTTP 415 Unsupported Media Type
       |
       v
6. Transactional Business Logic & Database Invariant Verification
   - Kiểm tra lại quyền Platform Admin trong giao dịch database (phòng chống race condition)
   -> Thất bại nghiệp vụ: HTTP 404, HTTP 409, HTTP 503, HTTP 500 với mã lỗi tương ứng
```

### 2.1 Định dạng Phong bì Lỗi Chuẩn (`httpapi.APIErrorEnvelope`)

Toàn bộ các phản hồi lỗi từ Go backend luôn tuân thủ cấu trúc phong bì JSON chuẩn, đồng thời đính kèm header HTTP `X-Request-ID`:

```http
HTTP/1.1 403 Forbidden
Content-Type: application/json; charset=utf-8
Cache-Control: no-store
X-Request-ID: 00000000-0000-0000-0000-000000000001
```

```json
{
  "error": {
    "code": "forbidden",
    "message": "insufficient permissions",
    "request_id": "00000000-0000-0000-0000-000000000001"
  }
}
```

---

## 3. Chi tiết Các Endpoint Quản trị Danh mục và Khởi tạo Gateway/Sensor (Admin Inventory & Provisioning APIs)

### 3.1 Danh mục Kiểm kê Gateway Toàn hệ thống: `GET /v1/admin/gateways`

Endpoint này cho phép Platform Admin truy vấn danh mục toàn bộ các trạm Gateway hiện có trong hệ thống, độc lập hoàn toàn với việc Admin có được phân quyền thành viên trong `user_gateways` hay không.

- **HTTP Method:** `GET`
- **Đường dẫn:** `/v1/admin/gateways`
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của người dùng đã xác thực (`role == "authenticated"`).
  - Bắt buộc phải có quyền Platform Admin trong bảng `public.platform_admins`.
  - Không yêu cầu tư cách thành viên trong `user_gateways`.
- **Query Parameters (Hỗ trợ phân trang keyset cursor):**
  - `limit` (tùy chọn, integer): Số lượng Gateway tối đa trong một trang.
    - Giá trị mặc định: `100` (được cấu hình qua biến môi trường `ADMIN_INVENTORY_DEFAULT_LIMIT`, dải hợp lệ 1..1000 theo `src/internal/config/config.go`).
    - Giá trị tối đa: `200` (được cấu hình qua biến môi trường `ADMIN_INVENTORY_MAX_LIMIT`, dải hợp lệ 1..1000 và bắt buộc $\ge$ default limit).
    - Dải hợp lệ trên môi trường mặc định: `1` đến `200` (dải này co giãn theo giá trị cấu hình server thực tế, không phải hằng số cứng bất biến).
    - Bộ phân tích cú pháp (`listAdminGateways`) chỉ chấp nhận chuỗi chứa toàn bộ chữ số nguyên dương `[0-9]+`. Nếu truyền số âm, chữ cái, chuỗi rỗng (`limit=`), hoặc vượt quá giá trị max limit cấu hình -> nhận ngay **`400 Bad Request`** (`code: "invalid_request"`).
  - `after` (tùy chọn, string): Con trỏ phân trang, là `gateway_id` của bản ghi cuối cùng của trang trước. Phải thỏa mãn biểu thức regex của Gateway ID (`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, khác `backend_service`). Nếu truyền `after=` rỗng hoặc sai regex -> nhận **`400 Bad Request`** (`code: "invalid_request"`).
  - **Quy tắc Query nghiêm ngặt:**
    - Cấm tuyệt đối tham số lạ: Bất kỳ query parameter nào ngoài `limit` và `after` đều bị từ chối với **`400 Bad Request`** (`code: "invalid_request"`).
    - Cấm tham số trùng lặp (ví dụ `?limit=50&limit=100` hoặc `?after=gw1&after=gw2`) -> **`400 Bad Request`**.
    - Cấm dấu chấm phẩy `;` trong query string -> **`400 Bad Request`**.
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Accept: application/json` (khuyến nghị)
  - `X-Request-ID: <id>` (khuyến nghị)
- **Response Headers:**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  X-Request-ID: 00000000-0000-0000-0000-000000000005
  ```

#### Cấu trúc Response Body (JSON):

- **Trường hợp có dữ liệu (Populated Response):**
  ```json
  {
    "items": [
      {
        "gateway_id": "gw-station-alpha",
        "name": "AM5728 Gateway 001",
        "description": "Tram quan trac nha kinh so 1",
        "created_at": "2026-10-14T08:00:00Z"
      },
      {
        "gateway_id": "gw-station-beta",
        "name": "ESP32 Gateway 002",
        "description": null,
        "created_at": "2026-10-14T08:15:00Z"
      }
    ],
    "next_cursor": "gw-station-beta"
  }
  ```

- **Trường hợp danh mục rỗng hoặc trang cuối cùng (Empty / End-of-List Response):**
  ```json
  {
    "items": [],
    "next_cursor": null
  }
  ```

#### Quy tắc dữ liệu (DTO Specification):
1. **`items` (mảng đối tượng):** Luôn là một mảng JSON, không bao giờ `null`. Mỗi phần tử bao gồm:
   - `gateway_id` (string): Khóa định danh của Gateway.
   - `name` (string): Tên hiển thị nguyên bản của Gateway.
   - `description` (string | null): Mô tả Gateway, có thể là chuỗi hoặc `null`.
   - `created_at` (string | null): Mốc thời gian tạo bản ghi định dạng RFC3339 UTC kết thúc bằng `Z`, hoặc `null`.
   - **Ranh giới bảo mật thông tin:** DTO này **KHÔNG BAO GỒM** vai trò người dùng (`role`), **KHÔNG BAO GỒM** thông tin chủ sở hữu (`owner_user_id`), và **TUYỆT ĐỐI KHÔNG TIẾT LỘ** bất kỳ bí mật xác thực hay mật khẩu MQTT nào.
2. **`next_cursor` (string | null):** Chuỗi `gateway_id` của phần tử cuối cùng trong trang hiện tại nếu còn dữ liệu tiếp theo trong cơ sở dữ liệu. Mang giá trị `null` khi đã duyệt hết trang cuối cùng. Client truyền giá trị này vào query `?after=<next_cursor>` để tải trang kế tiếp.
3. **Không sáng tác các trường không có trong code:** DTO phản hồi **KHÔNG CÓ** trường `total`, **KHÔNG CÓ** trường `count`, **KHÔNG CÓ** `page`, `page_size`. Phân trang thuần túy dựa trên keyset cursor.
4. **Thứ tự sắp xếp (Sorting):** Danh sách được sắp xếp tăng dần theo mã `gateway_id` theo bảng mã ASCII trong SQL (`ORDER BY gateway_id COLLATE "C" ASC`).

#### Bảng mã trạng thái HTTP của `GET /v1/admin/gateways`:

| HTTP Status | Error `code` | Error `message` | Tình huống kích hoạt |
|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` | Tham số query lạ, trùng lặp tham số, chứa dấu `;`, `limit` không phải chữ số, `limit` ngoài dải hợp lệ [1..max_limit] (mặc định [1..200]), hoặc con trỏ `after` sai regex. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Thiếu hoặc sai token JWT. |
| **403 Forbidden** | `forbidden` | `insufficient permissions` *(middleware)* hoặc `access denied` *(service)* | Người dùng đã xác thực nhưng không có quyền trong `public.platform_admins`. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | Lỗi truy vấn hoặc scan dữ liệu SQL không lường trước. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | Quá thời hạn kiểm tra quyền admin hoặc gián đoạn kết nối CSDL PostgreSQL. |

---

### 3.2 Quy chuẩn Phân tích Cú pháp Nghiêm ngặt của Nhóm Khởi tạo (Strict Parser Rules)

Theo hiện thực tại `src/internal/httpserver/provisioning_request.go`:
1. **Cấm tuyệt đối Query Parameters:** Mọi URL chứa query string (kể cả dấu hỏi chấm rỗng `?`) đều lập tức nhận mã lỗi HTTP `400 Bad Request` (`code: "invalid_request"`).
2. **Kiểm tra Media Type (`Content-Type`):** Bắt buộc phải là `application/json`. Tham số duy nhất được phép đi kèm là `charset=utf-8` (không phân biệt hoa thường). Bất kỳ media type hoặc tham số nào khác đều trả về HTTP `415 Unsupported Media Type` (`code: "unsupported_media_type"`).
3. **Giới hạn kích thước Body (`ADMIN_MAX_BODY_BYTES`):** Giới hạn tối đa được cấu hình trên server (mặc định 16384 bytes = 16 KiB; dải hợp lệ 1..1048576 bytes = 1 MiB theo `src/internal/config/config.go`, không phải hằng số cứng bất biến). Nếu body vượt quá giới hạn, server trả về HTTP `413 Request Entity Too Large` (`code: "payload_too_large"`, `message: "payload too large"`).
4. **Cú pháp JSON nghiêm ngặt:**
   - Chỉ chấp nhận duy nhất một đối tượng JSON `{ ... }`.
   - **Cấm trường lạ (unknown fields):** Bất kỳ trường nào ngoài danh sách cho phép đều bị từ chối với HTTP `400 Bad Request`.
   - **Cấm trùng lặp khóa (duplicate keys):** Kể cả khi tên trường được escape khác nhau, server từ chối ngay lập tức với HTTP `400 Bad Request`.
   - **An toàn Unicode:** Chuỗi JSON thô được quét trước; từ chối các ký tự Unicode surrogate đơn lẻ không ghép cặp (`\uD800`–`\uDFFF`) hoặc escape chứa NUL (`\u0000`).
   - **Tính không thể gán null (Non-nullable):** Các trường bắt buộc không được mang giá trị `null`.

---

### 3.3 Khởi tạo Trạm Gateway: `PUT /v1/admin/gateways/{gateway_id}`

Khởi tạo thực thể Gateway mới, gán người sở hữu ban đầu, và tự động khởi tạo thực thể Digital Twin trong một giao dịch cơ sở dữ liệu nguyên tử (`READ COMMITTED`).

#### A. Tham số Đường dẫn (Path Parameters)
- `gateway_id` (string, bắt buộc): Định danh duy nhất của Gateway.
  - Định dạng: Bắt đầu bằng chữ cái hoặc chữ số, chỉ chứa các ký tự `[A-Za-z0-9_-]`, độ dài từ 1 đến 64 ký tự (Regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`).
  - **Cấm giá trị dành riêng:** Tuyệt đối không được đặt tên là `backend_service` (trả về HTTP `400 Bad Request`).

#### B. Cấu trúc Request Body (JSON)
| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `name` | string | **Có** | Tên hiển thị của Gateway. Không được rỗng hoặc chỉ chứa khoảng trắng. Tối đa **256 UTF-8 bytes**. |
| `description` | string hoặc null | Không | Mô tả trạm Gateway. Tối đa **4096 UTF-8 bytes**. Có thể bỏ qua hoặc truyền `null`. |
| `owner_user_id` | string (UUID) | **Có** | Định danh người dùng sở hữu ban đầu. Phải là chuỗi UUID hợp lệ khác nil UUID và **bắt buộc phải tồn tại từ trước trong bảng `public.profiles`**. |

*Ví dụ Request:*
```http
PUT /v1/admin/gateways/gw-station-alpha
Authorization: Bearer <valid_platform_admin_jwt>
Content-Type: application/json
Accept: application/json

{
  "name": "Gateway Tram Quan Trac Alpha",
  "description": "Tram thu thap du lieu moi truong khu vuc A",
  "owner_user_id": "00000000-0000-0000-0000-000000000002"
}
```

#### C. Cấu trúc Response Body (JSON)
- `gateway_id` (string): Khóa định danh của Gateway.
- `name` (string): Tên hiển thị nguyên bản.
- `description` (string hoặc null): Mô tả Gateway.
- `owner_user_id` (string UUID): UUID của người sở hữu ban đầu.
- `entity_id` (string): URN định danh chuẩn NGSI-LD (`urn:ngsi-ld:Gateway:<gateway_id>`).
- `created_at` (string RFC3339 UTC hoặc null): Thời điểm tạo bản ghi trong CSDL.

#### D. Các Mã Trạng thái và Trường hợp Phản hồi
- **HTTP 201 Created:** Khởi tạo thành công Gateway mới cùng đồ thị Digital Twin liên kết.
  ```json
  {
    "gateway_id": "gw-station-alpha",
    "name": "Gateway Tram Quan Trac Alpha",
    "description": "Tram thu thap du lieu moi truong khu vuc A",
    "owner_user_id": "00000000-0000-0000-0000-000000000002",
    "entity_id": "urn:ngsi-ld:Gateway:gw-station-alpha",
    "created_at": "2026-10-05T08:30:00Z"
  }
  ```
- **HTTP 200 OK (Idempotent Retry):** Gateway đã tồn tại và request gửi lên có payload trùng khớp hoàn toàn (`name`, `description`, `owner_user_id` giống hệt bản ghi đã lưu). Server trả về dữ liệu hiện tại với mã `200 OK`.
- **HTTP 400 Bad Request (`code: "invalid_request"`):** Vi phạm định dạng `gateway_id` (chứa ký tự lạ, quá 64 bytes, hoặc là `backend_service`), `name` rỗng, vượt quá giới hạn độ dài bytes, `owner_user_id` không phải UUID hợp lệ hoặc nil UUID, chứa trường lạ, hoặc chứa URL query.
- **HTTP 404 Not Found (`code: "not_found"`):** Không tìm thấy `owner_user_id` trong bảng `public.profiles`.
- **HTTP 409 Conflict (`code: "conflict"`):** Gateway đã tồn tại nhưng request PUT mới có thông tin thay đổi (thay đổi `name`, `description`, hoặc cố tình thay đổi `owner_user_id`). API PUT này là cơ chế khởi tạo và retry lũy quyền, không hỗ trợ đổi chủ sở hữu hoặc cập nhật metadata khác biệt.
- **HTTP 413 Request Entity Too Large (`code: "payload_too_large"`, `message: "payload too large"`):** Kích thước body vượt quá giới hạn cấu hình `ADMIN_MAX_BODY_BYTES`.
- **HTTP 415 Unsupported Media Type (`code: "unsupported_media_type"`, `message: "unsupported media type"`):** `Content-Type` không phải `application/json` (cho phép `charset=utf-8`).
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Gián đoạn kết nối database khi kiểm tra quyền Platform Admin trong giao dịch hoặc timeout.

---

### 3.4 Khởi tạo Sensor bởi Platform Admin: `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`

Khởi tạo thực thể Sensor mới trực thuộc Gateway cha bởi Platform Admin, tự động khởi tạo thực thể Digital Twin (`twin_entities`, `twin_states`) và thiết lập mối quan hệ phân cấp `twin_relationships` (`hasSensor`) trong cùng một giao dịch.

#### A. Tham số Đường dẫn (Path Parameters)
- `gateway_id` (string, bắt buộc): Định danh của Gateway cha. Phải tuân thủ biểu thức regex và **bắt buộc phải tồn tại từ trước**.
- `sensor_id` (string, bắt buộc): Định danh của Sensor.
  - Định dạng: Bắt đầu bằng chữ cái hoặc chữ số, chỉ chứa các ký tự `[A-Za-z0-9_-]`, độ dài từ 1 đến 64 ký tự (Regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`).
  - *Lưu ý:* Sensor cho phép sử dụng định danh `backend_service` (khác với Gateway cấm giá trị này).

#### B. Cấu trúc Request Body (JSON)
Theo đúng parser `parseSensorProvisioning` trong mã nguồn Go, request body của Sensor **chỉ tiếp nhận duy nhất hai trường: `name` và `unit`**. Tuyệt đối không có trường `sensor_type` (nếu truyền trường này sẽ bị coi là unknown field và nhận HTTP 400 ngay lập tức).

| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `name` | string | **Có** | Tên cảm biến. Không được rỗng hoặc chỉ chứa khoảng trắng. Tối đa **256 UTF-8 bytes**. |
| `unit` | string hoặc null | Không | Đơn vị đo lường (ví dụ: `"degC"`, `"percent"`). Tối đa **64 UTF-8 bytes**. Có thể bỏ qua hoặc truyền `null`. |

*Ví dụ Request:*
```http
PUT /v1/admin/gateways/gw-station-alpha/sensors/sensor-dht22-temp
Authorization: Bearer <valid_platform_admin_jwt>
Content-Type: application/json
Accept: application/json

{
  "name": "Cam Bien Nhiet Do Moi Truong",
  "unit": "degC"
}
```

#### C. Cấu trúc Response Body (JSON)
- `gateway_id` (string): Khóa Gateway cha.
- `sensor_id` (string): Khóa định danh của Sensor.
- `name` (string): Tên Sensor.
- `unit` (string hoặc null): Đơn vị đo.
- `entity_id` (string): URN định danh chuẩn NGSI-LD (`urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`).
- `created_at` (string RFC3339 UTC hoặc null): Thời điểm tạo bản ghi trong CSDL.

#### D. Các Mã Trạng thái và Trường hợp Phản hồi
- **HTTP 201 Created:** Khởi tạo thành công Sensor mới và đồ thị quan hệ phân cấp liên kết với Gateway cha.
  ```json
  {
    "gateway_id": "gw-station-alpha",
    "sensor_id": "sensor-dht22-temp",
    "name": "Cam Bien Nhiet Do Moi Truong",
    "unit": "degC",
    "entity_id": "urn:ngsi-ld:Sensor:gw-station-alpha:sensor-dht22-temp",
    "created_at": "2026-10-05T08:35:00Z"
  }
  ```
- **HTTP 200 OK (Idempotent Retry):** Sensor đã tồn tại dưới Gateway này và request PUT có payload trùng khớp hoàn toàn (`name` và `unit` giống hệt bản ghi đã lưu).
- **HTTP 400 Bad Request (`code: "invalid_request"`):** Vi phạm định dạng `gateway_id` hoặc `sensor_id`, `name` rỗng, vượt quá giới hạn bytes, chứa trường lạ (như `sensor_type`), chứa duplicate keys, hoặc có query string.
- **HTTP 404 Not Found (`code: "not_found"`):** Gateway cha `gateway_id` không tồn tại trong cơ sở dữ liệu.
- **HTTP 409 Conflict (`code: "conflict"`):** Sensor đã tồn tại nhưng payload PUT mới có thông tin khác biệt (`name` hoặc `unit` bị sửa đổi).
- **HTTP 413 Request Entity Too Large (`code: "payload_too_large"`, `message: "payload too large"`):** Kích thước body vượt quá giới hạn cấu hình `ADMIN_MAX_BODY_BYTES`.
- **HTTP 415 Unsupported Media Type (`code: "unsupported_media_type"`, `message: "unsupported media type"`):** Header `Content-Type` bị thiếu hoặc khác `application/json` (cho phép `charset=utf-8`).
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Gián đoạn kết nối database khi kiểm tra quyền trong giao dịch.

---

### 3.5 Khởi tạo Thực thể Digital Twin (Gateway & Sensor): `POST /v1/digital-twins`

Endpoint này được bổ sung trong Giai đoạn 4, cho phép khởi tạo thực thể Digital Twin Gateway hoặc Sensor theo mô hình dữ liệu NGSI-LD chuẩn hóa, tự động ánh xạ quan hệ phân cấp và bảo đảm tính nhất quán nguyên tử giữa các bảng CSDL quan hệ (`gateways`, `sensors`, `user_gateways`) và các bảng Digital Twin (`twin_entities`, `twin_states`, `twin_relationships`).

- **HTTP Method:** `POST`
- **Đường dẫn:** `/v1/digital-twins`
- **Quy chuẩn Header:**
  - `Authorization: Bearer <jwt>` (bắt buộc).
  - `Content-Type: application/json` (cho phép tham số `charset=utf-8`).
  - `Cache-Control: no-store` và `Pragma: no-cache` tự động được server thiết lập.
- **Quy tắc Parser nghiêm ngặt:**
  - Cấm tuyệt đối Query Parameters: URL chứa query string (kể cả `?`) nhận ngay **`400 Bad Request`** (`code: "invalid_request"`).
  - Giới hạn kích thước Body: Giới hạn bởi `ADMIN_MAX_BODY_BYTES` (mặc định 16 KiB; dải hợp lệ 1..1048576 bytes). Vượt quá nhận **`413 Request Entity Too Large`** (`code: "payload_too_large"`).
  - Cú pháp JSON khắt khe: Chỉ nhận duy nhất một đối tượng JSON `{ ... }`, cấm trường lạ (unknown fields), cấm trùng lặp khóa (duplicate keys), các trường bắt buộc không được mang giá trị `null`.
- **Ranh giới Phân quyền (RBAC Policy):**
  - **Khởi tạo Gateway (`entity_type: "Gateway"`):** Chỉ dành riêng cho **Platform Admin** (kiểm tra tồn tại trong `public.platform_admins`). Người dùng thường (kể cả có JWT hợp lệ) nhận ngay **`403 Forbidden`** (`code: "forbidden"`, `message: "access denied"`).
  - **Khởi tạo Sensor (`entity_type: "Sensor"`):** Dành cho **Platform Admin HOẶC Gateway Owner** (người dùng có vai trò `owner` của trạm đó trong `public.user_gateways`).
    - Vai trò `operator` hoặc `viewer` của trạm bị từ chối **`403 Forbidden`** (`code: "forbidden"`, `message: "access denied"`).
    - Người dùng không có liên kết thành viên với trạm nhận **`404 Not Found`** (`code: "not_found"`, `message: "resource not found"`).
- **Ranh giới Xác thực & Tài khoản:** Lệnh POST này **TUYỆT ĐỐI KHÔNG cấp phát thông tin xác thực MQTT** và **KHÔNG tạo tài khoản người dùng Auth**. Mật khẩu MQTT phải được cấp độc lập qua `/v1/admin/gateways/{id}/mqtt-credential`.

#### A. Cấu trúc Request Body khi Tạo Gateway (`entity_type: "Gateway"`)
| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `entity_type` | string | **Có** | Bắt buộc là giá trị `"Gateway"`. |
| `gateway_id` | string | **Có** | Định danh Gateway (regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`, cấm giá trị `backend_service`). |
| `name` | string | **Có** | Tên trạm Gateway (1..256 UTF-8 bytes, không rỗng hoặc chỉ chứa khoảng trắng). |
| `description` | string hoặc null | Không | Mô tả trạm Gateway (tối đa 4096 UTF-8 bytes, có thể bỏ qua hoặc truyền `null`). |
| `owner_user_id` | string (UUID) | **Có** | UUID của người sở hữu ban đầu. Phải khác nil UUID và **bắt buộc đã tồn tại từ trước trong bảng `public.profiles`**. |

*Ví dụ Request tạo Gateway:*
```http
POST /v1/digital-twins
Authorization: Bearer <platform_admin_jwt>
Content-Type: application/json

{
  "entity_type": "Gateway",
  "gateway_id": "gw-station-alpha",
  "name": "Gateway Tram Quan Trac Alpha",
  "description": "Tram thu thap du lieu khu vuc A",
  "owner_user_id": "00000000-0000-0000-0000-000000000002"
}
```

*Cấu trúc Response Body Gateway (`PostGatewayResponseDTO`):*
```json
{
  "gateway_id": "gw-station-alpha",
  "name": "Gateway Tram Quan Trac Alpha",
  "description": "Tram thu thap du lieu khu vuc A",
  "owner_user_id": "00000000-0000-0000-0000-000000000002",
  "entity_id": "urn:ngsi-ld:Gateway:gw-station-alpha",
  "created_at": "2026-10-18T08:30:00.123456Z"
}
```

#### B. Cấu trúc Request Body khi Tạo Sensor (`entity_type: "Sensor"`)
| Trường | Kiểu dữ liệu | Bắt buộc | Ràng buộc giá trị & Giới hạn |
|---|---|:---:|---|
| `entity_type` | string | **Có** | Bắt buộc là giá trị `"Sensor"`. |
| `gateway_id` | string | **Có** | Định danh của Gateway cha (phải tồn tại từ trước). |
| `sensor_id` | string | **Có** | Định danh Sensor (regex: `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`). |
| `name` | string | **Có** | Tên Sensor (1..256 UTF-8 bytes, không rỗng hoặc chỉ chứa khoảng trắng). |
| `unit` | string hoặc null | Không | Đơn vị đo lường (tối đa 64 UTF-8 bytes, ví dụ `"degC"`, `"percent"` hoặc `null`). |

*Ví dụ Request tạo Sensor:*
```http
POST /v1/digital-twins
Authorization: Bearer <owner_or_admin_jwt>
Content-Type: application/json

{
  "entity_type": "Sensor",
  "gateway_id": "gw-station-alpha",
  "sensor_id": "sensor-dht22-temp",
  "name": "Cam Bien Nhiet Do Moi Truong",
  "unit": "degC"
}
```

*Cấu trúc Response Body Sensor (`PostSensorResponseDTO`):*
```json
{
  "gateway_id": "gw-station-alpha",
  "sensor_id": "sensor-dht22-temp",
  "name": "Cam Bien Nhiet Do Moi Truong",
  "unit": "degC",
  "entity_id": "urn:ngsi-ld:Sensor:gw-station-alpha:sensor-dht22-temp",
  "created_at": "2026-10-18T08:35:00.654321Z"
}
```

#### C. Mã Trạng thái HTTP của `POST /v1/digital-twins`
- **HTTP 201 Created:** Khởi tạo thành công thực thể mới. Lưu ý: Cờ boolean `Created: true` chỉ là cờ kết quả nội bộ tại tầng domain (`digitaltwin.CreateEntityResult.Created`), **HOÀN TOÀN KHÔNG PHẢI là trường trong JSON response wire**. Phía client nhận biết kết quả tạo mới thành công duy nhất thông qua mã trạng thái HTTP `201 Created`.
- **HTTP 200 OK (Idempotent Replay):** Thực thể đã tồn tại và request gửi lên trùng khớp hoàn toàn mọi trường (tạo mới hoặc gửi lại trùng khớp). Lưu ý: Cờ `Created: false` là cờ logic nội bộ, **TUYỆT ĐỐI KHÔNG xuất hiện trong JSON response wire**; mã trạng thái HTTP `200 OK` chính là dấu hiệu định danh thao tác gửi lại trùng khớp thành công.
- **HTTP 400 Bad Request (`code: "invalid_request"`):** Sai cú pháp JSON, thiếu trường bắt buộc, trường lạ, định danh sai regex, `name` rỗng, hoặc có URL query.
- **HTTP 401 Unauthorized (`code: "unauthorized"`):** Thiếu hoặc sai token JWT.
- **HTTP 403 Forbidden (`code: "forbidden"`):** Không đủ quyền: Người dùng không phải Platform Admin khi tạo Gateway; vai trò `operator`/`viewer` của trạm khi tạo Sensor (sau khi request đã vượt qua kiểm tra cấu trúc cú pháp hợp lệ).
- **HTTP 404 Not Found (`code: "not_found"`):** Không tìm thấy `owner_user_id` trong `profiles` (Gateway); không tìm thấy Gateway cha hoặc người gọi không có liên kết thành viên trạm (Sensor).
- **HTTP 409 Conflict (`code: "conflict"`):** Thực thể đã tồn tại nhưng payload mới có thông tin sai lệch (`name`, `description`, `unit` hoặc `owner_user_id` khác với dữ liệu đã lưu).
- **HTTP 413 Payload Too Large (`code: "payload_too_large"`):** Kích thước body vượt quá `ADMIN_MAX_BODY_BYTES`.
- **HTTP 415 Unsupported Media Type (`code: "unsupported_media_type"`):** `Content-Type` khác `application/json`.
- **HTTP 500 Internal Server Error (`code: "internal_error"`):** Lỗi bất nhất đồ thị thực thể (`ErrInconsistentGraph`) hoặc lỗi server không xác định.
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Database timeout, gián đoạn pool kết nối, hoặc bất định trong commit giao dịch.

---

### 3.6 Cập nhật Siêu dữ liệu Digital Twin: `PATCH /v1/digital-twins/{entity_id}/attrs`

Endpoint này cho phép cập nhật có kiểm soát các trường siêu dữ liệu hiển thị (`name`, `description`) của Gateway hoặc Sensor thông qua mã định danh URN chuẩn NGSI-LD và normalized Property fragment.

- **HTTP Method:** `PATCH`
- **Đường dẫn:** `/v1/digital-twins/{entity_id}/attrs` (Đường dẫn gốc không có `/attrs` nhận phản hồi `405 Method Not Allowed`).
- **Tham số Đường dẫn (`entity_id`):** Bắt buộc là URN chuẩn NGSI-LD:
  - Gateway URN: `urn:ngsi-ld:Gateway:<gateway_id>`
  - Sensor URN: `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`
  - **Quy tắc giải mã URL (Single URL-Decode Rule):** URN trong path được giải mã đúng một lần bởi tầng HTTP router (`c.Param("entity_id")`). Parser kiểm tra trực tiếp định dạng URN và **TUYỆT ĐỐI KHÔNG thực hiện giải mã lần 2**; các URN bị mã hóa 2 lần (ví dụ `%253A`) sẽ lập tức nhận **`400 Bad Request`**.
- **Quy chuẩn Header:**
  - `Authorization: Bearer <jwt>` (bắt buộc).
  - `Content-Type: application/json` hoặc `application/ld+json` (cho phép `charset=utf-8`). Nếu là JSON-LD, context bắt buộc phải trùng khớp context khóa của dự án.
  - `Cache-Control: no-store` và `Pragma: no-cache` tự động được thiết lập.
- **Ranh giới Phân quyền (RBAC Policy):**
  - **PATCH Gateway:** Chỉ dành riêng cho **Platform Admin** (kiểm tra `public.platform_admins` trước tiên trong giao dịch). Mọi người dùng khác (kể cả Gateway `owner`) nhận **`403 Forbidden`** (`code: "forbidden"`, `message: "access denied"`).
  - **PATCH Sensor:** Dành cho **Platform Admin HOẶC Gateway Owner** (`user_gateways.role == "owner"`).
    - Vai trò `operator` hoặc `viewer`: Bị từ chối **`403 Forbidden`** (`code: "forbidden"`, `message: "access denied"`) sau khi request đã vượt qua kiểm tra cấu trúc cú pháp hợp lệ.
    - Người không phải thành viên trạm: Nhận **`404 Not Found`** (`code: "not_found"`, `message: "resource not found"`).
- **Ranh giới Bất biến Dữ liệu Nghiệp vụ (State & Invariant Boundaries):**
  - PATCH này **CHỈ cập nhật siêu dữ liệu hiển thị** (`name`, `description`).
  - **TUYỆT ĐỐI KHÔNG can thiệp** vào `reported_state`, `desired_state`, các phiên bản (`reported_version`, `desired_version`), dữ liệu viễn trắc (`telemetry`), chuỗi giá trị thời gian (`twin_temporal_values`), lệnh điều khiển (`twin_commands`), outbox, hay thông tin xác thực broker.
  - **Admin sửa metadata KHÔNG bypass quyền đọc:** Platform Admin patch trạm nhưng không có quyền trong `user_gateways` thì vẫn nhận `404 Not Found` khi truy vấn dữ liệu đo hoặc trạng thái trạm.

#### A. Quy chuẩn Body và Cơ chế Hiện diện của Gateway PATCH (Property Fragment)
- Định dạng yêu cầu: **Normalized Property Fragment** theo V1.9.1 profile.
- Danh sách trường được phép gửi (Allowlist): **Duy nhất hai Property `name` và `description`**.
  - `name`: Object Property có `type: "Property"` và `value` (string, 1..256 UTF-8 bytes, không được rỗng hoặc chỉ chứa khoảng trắng). **Không được là `null`** (nếu truyền `null` nhận ngay 400).
  - `description`: Object Property có `type: "Property"` và `value` (string tối đa 4096 UTF-8 bytes, hoặc chuỗi sentinel để xóa). Áp dụng **ngữ nghĩa nhận biết sự hiện diện (Presence-Aware Ternary Semantics)**:
    - **Omitted (không truyền Property `description`):** Giữ nguyên mô tả hiện tại, không sửa đổi.
    - **Truyền chuỗi văn bản (`"value": "..."`):** Cập nhật mô tả mới vào CSDL.
    - **Xóa mô tả về SQL `NULL`:** Truyền sentinel chuẩn NGSI-LD:
      ```json
      {
        "description": {
          "type": "Property",
          "value": "urn:ngsi-ld:null"
        }
      }
      ```
      *(Không nhận JSON literal null `{"description": null}`).*
  - **Quy tắc Lưu trữ Description (Description Storage Invariant):** Trường `description` được ghi **DUY NHẤT vào cột quan hệ `public.gateways.description`**; trường `twin_entities.attributes` tuyệt đối không bị ghi đè hay biến đổi.
  - Cấm trường lạ (ví dụ `owner_user_id`, `gateway_id`, `unit`, `state`): Nhận ngay **`400 Bad Request`**.
  - Cấm Body rỗng `{}`: Bắt buộc phải có ít nhất một trường (`name` hoặc `description`).

*Ví dụ Request PATCH Gateway:*
```http
PATCH /v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-station-alpha/attrs
Authorization: Bearer <platform_admin_jwt>
Content-Type: application/json

{
  "name": {
    "type": "Property",
    "value": "Gateway Tram Quan Trac Alpha V2"
  },
  "description": {
    "type": "Property",
    "value": "Tram moi truong khu vuc A da nang cap"
  }
}
```

*Cấu trúc Response Body (`PatchGatewayResponseDTO`):*
```json
{
  "id": "urn:ngsi-ld:Gateway:gw-station-alpha",
  "type": "Gateway",
  "gateway_id": "gw-station-alpha",
  "name": "Gateway Tram Quan Trac Alpha V2",
  "description": "Tram moi truong khu vuc A da nang cap",
  "updated_at": "2026-10-18T09:00:00.123456Z"
}
```

#### B. Quy chuẩn Body và Tính Bất biến Đơn vị của Sensor PATCH
- Định dạng yêu cầu: Property fragment chỉ chứa duy nhất trường `name`.
  - `name`: Object Property có `type: "Property"` và `value` (string bắt buộc, 1..256 UTF-8 bytes, không rỗng, không null).
  - **ĐƠN VỊ ĐO LƯỜNG LÀ BẤT BIẾN (Unit is Immutable):** Trường `unit` của Sensor được khóa bất biến sau khi khởi tạo nhằm bảo đảm tính toàn vẹn của chuỗi dữ liệu đo trong hypertable TimescaleDB. **Nếu request PATCH có chứa trường `unit` (dù là cùng giá trị, giá trị mới, hay null), bộ phân tích cú pháp sẽ lập tức từ chối với HTTP `400 Bad Request` (`code: "invalid_request"`)**.
  - Cấm trường lạ (như `sensor_id`, `gateway_id`, `unit`, `sensor_type`).
  - Cấm Body rỗng `{}`.

*Ví dụ Request PATCH Sensor:*
```http
PATCH /v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-station-alpha%3Asensor-dht22-temp/attrs
Authorization: Bearer <owner_or_admin_jwt>
Content-Type: application/json

{
  "name": {
    "type": "Property",
    "value": "Cam Bien Nhiet Do Moi Truong Chinh Xac Cao"
  }
}
```

*Cấu trúc Response Body (`PatchSensorResponseDTO`):*
```json
{
  "id": "urn:ngsi-ld:Sensor:gw-station-alpha:sensor-dht22-temp",
  "type": "Sensor",
  "gateway_id": "gw-station-alpha",
  "sensor_id": "sensor-dht22-temp",
  "name": "Cam Bien Nhiet Do Moi Truong Chinh Xac Cao",
  "unit": "degC",
  "updated_at": "2026-10-18T09:05:00.654321Z"
}
```

#### C. Cơ chế No-op và Cập nhật Mốc thời gian Đơn điệu (No-op Semantics & Monotonic Timestamps)
1. **Phát hiện No-op (Same-Value No-Op Detection):**
   - Nếu client gửi bản vá PATCH có giá trị trùng khớp hoàn toàn với dữ liệu hiện tại trong CSDL (ví dụ gửi lại cùng `name` và `description` đang lưu):
     - Hệ thống thực thi **0 câu lệnh SQL UPDATE** trên cả bảng quan hệ lẫn bảng Digital Twin.
     - Giao dịch commit thành công và server trả về mã **HTTP `200 OK`**.
     - **Mốc thời gian `updated_at` cũ được giữ nguyên vẹn**, không bị làm mới giả tạo.
2. **Cập nhật Mốc thời gian khi có Thay đổi Thực sự:**
   - Khi có ít nhất một trường thay đổi giá trị thực tế, cột `twin_entities.updated_at` được cập nhật bằng `statement_timestamp()` (thời điểm thực thi câu lệnh SQL sau khi đã chiếm khóa, tránh hiện tượng trôi ngược mốc thời gian sau các giao dịch đồng thời).

#### D. Mã Trạng thái HTTP của `PATCH /v1/digital-twins/{entity_id}/attrs`
- **HTTP 200 OK:** Cập nhật thành công siêu dữ liệu (hoặc ghi nhận no-op trùng khớp).
- **HTTP 400 Bad Request (`code: "invalid_request"`):** URN sai định dạng, truyền trường `unit` khi patch Sensor, truyền `name: null`, body rỗng `{}`, chứa trường lạ, sai kiểu Property, hoặc chứa URL query.
- **HTTP 401 Unauthorized (`code: "unauthorized"`):** Thiếu hoặc sai token JWT.
- **HTTP 403 Forbidden (`code: "forbidden"`):** Không đủ quyền: Người dùng không phải Platform Admin khi patch Gateway (kể cả Gateway `owner`); vai trò `operator`/`viewer` khi patch Sensor (sau khi request đã vượt qua kiểm tra cấu trúc cú pháp hợp lệ).
- **HTTP 404 Not Found (`code: "not_found"`):** Thực thể không tồn tại trong hệ thống, hoặc người gọi không có quyền thành viên trạm (đối với Sensor).
- **HTTP 405 Method Not Allowed (`code: "method_not_allowed"`):** Gửi `PATCH` vào đường dẫn gốc không có `/attrs`.
- **HTTP 413 Payload Too Large (`code: "payload_too_large"`):** Body vượt quá `ADMIN_MAX_BODY_BYTES`.
- **HTTP 415 Unsupported Media Type (`code: "unsupported_media_type"`):** `Content-Type` khác `application/json` hoặc `application/ld+json`.
- **HTTP 500 Internal Server Error (`code: "internal_error"`):** Bất nhất đồ thị thực thể (`ErrInconsistentGraph`) hoặc lỗi server không lường trước.
- **HTTP 503 Service Unavailable (`code: "service_unavailable"`):** Database timeout, gián đoạn kết nối, hoặc bất định trong commit giao dịch.

---

### 3.7 So sánh Đối chiếu giữa Khởi tạo PUT (Stage 2) và POST/PATCH (Stage 4)

Bảng đối chiếu dưới đây làm rõ mục đích sử dụng và hành vi của các endpoint khởi tạo và cập nhật thực thể:

| Đặc điểm | `PUT /v1/admin/gateways/{id}` (Stage 2) | `PUT /v1/admin/gateways/{id}/sensors/{id}` (Stage 2) | `POST /v1/digital-twins` (Stage 4) | `PATCH /v1/digital-twins/{entity_id}` (Stage 4) |
|---|---|---|---|---|
| **Mục đích chính** | Khởi tạo Gateway mới hoặc gửi lại trùng khớp (Create-or-Exact-Replay). | Khởi tạo Sensor mới hoặc gửi lại trùng khớp (Create-or-Exact-Replay). | Khởi tạo Gateway hoặc Sensor theo mô hình chuẩn NGSI-LD. | **Cập nhật siêu dữ liệu hiển thị** (`name`, `description`) của thực thể đã tồn tại. |
| **Quyền thực thi** | Platform Admin. | Platform Admin. | - Gateway: Platform Admin.<br>- Sensor: Platform Admin hoặc Gateway Owner. | - Gateway: Platform Admin.<br>- Sensor: Platform Admin hoặc Gateway Owner. |
| **Hành vi khi thực thể đã tồn tại & trùng thông tin** | `200 OK` (Gửi lại trùng khớp). | `200 OK` (Gửi lại trùng khớp). | `200 OK` (Gửi lại trùng khớp). | `200 OK` (No-op, giữ nguyên `updated_at`). |
| **Hành vi khi thực thể đã tồn tại & khác thông tin** | **`409 Conflict`** (Không cho phép cập nhật thông tin qua PUT). | **`409 Conflict`** (Không cho phép cập nhật thông tin qua PUT). | **`409 Conflict`** (Không cho phép cập nhật thông tin qua POST). | **`200 OK`** (Cập nhật thành công siêu dữ liệu mới). |
| **Định danh trong Request** | `gateway_id` nằm trong URL path. | `gateway_id` và `sensor_id` nằm trong URL path. | Nằm trong JSON body (`gateway_id`, `sensor_id`, `owner_user_id`). | URN chuẩn trong URL path (`urn:ngsi-ld:Gateway:...` hoặc `urn:ngsi-ld:Sensor:...`). |
| **Quy chuẩn sửa trường** | Không hỗ trợ sửa. | Không hỗ trợ sửa. | Không hỗ trợ sửa. | Hỗ trợ presence-aware (`description: null` để xóa; cấm sửa `unit` của Sensor). |

*Lưu ý cốt lõi:* Hai endpoint PUT của Giai đoạn 2 (`/v1/admin/gateways/{id}` và `/v1/admin/gateways/{id}/sensors/{id}`) **VẪN ĐƯỢC DUY TRÌ HOÀN TOÀN** trong hệ thống và tiếp tục đóng vai trò là cơ chế khởi tạo và idempotent replay ban đầu. Các endpoint PUT này **tuyệt đối không được hiểu là API cập nhật tùy ý (arbitrary update)**. Khi muốn thay đổi tên hoặc mô tả của một trạm/cảm biến đã có, Client bắt buộc phải sử dụng API `PATCH /v1/digital-twins/{entity_id}`.

---

### 3.8 Bảng Mã trạng thái, Xung đột (Conflict) và Quy tắc Thử lại (Retry / Commit Uncertainty) của Metadata

#### A. Bảng Tổng hợp Trạng thái và Phân loại Lỗi Metadata (`ClassifyMutationError`):
| Mã HTTP | Error `code` | Error `message` | Trường hợp xảy ra đối với Metadata POST/PATCH |
|---|---|---|---|
| **201 Created** | *(Thành công)* | *(N/A)* | Khởi tạo thành công Gateway hoặc Sensor mới qua `POST /v1/digital-twins`. |
| **200 OK** | *(Thành công)* | *(N/A)* | - Replay trùng khớp hoàn toàn payload cũ qua `POST /v1/digital-twins`.<br>- Cập nhật thành công hoặc no-op trùng giá trị qua `PATCH /v1/digital-twins/{entity_id}`.<br>- Gửi lại trùng khớp thành công qua `PUT /v1/admin/...`. |
| **400 Bad Request** | `invalid_request` | `invalid request` | Sai định dạng URN, thiếu trường bắt buộc, tên rỗng, trường lạ, cố tình gửi `unit` khi patch Sensor, hoặc gửi query parameters. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Token JWT bị thiếu, hết hạn, hoặc sai chữ ký. |
| **403 Forbidden** | `forbidden` | `access denied` | Không có quyền: Người dùng không phải Platform Admin khi tạo/patch Gateway (kể cả Gateway `owner` khi patch Gateway); vai trò `operator`/`viewer` cố tạo/patch Sensor (sau khi request hợp lệ về cú pháp). |
| **404 Not Found** | `not_found` | `resource not found` | `owner_user_id` không tồn tại trong `profiles` (Gateway POST); không tìm thấy Gateway cha; không tìm thấy thực thể cần patch; hoặc người gọi không có liên kết thành viên trạm (Sensor POST/PATCH). |
| **409 Conflict** | `conflict` | `resource conflict` | Thực thể đã tồn tại nhưng thông tin gửi lên bị xung đột (khác tên, khác mô tả, khác đơn vị, hoặc khác chủ sở hữu). |
| **413 Payload Too Large** | `payload_too_large` | `payload too large` | Kích thước body vượt quá giới hạn cấu hình `ADMIN_MAX_BODY_BYTES`. |
| **415 Unsupported Media Type** | `unsupported_media_type` | `unsupported media type` | Header `Content-Type` bị thiếu hoặc khác `application/json`. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | Phát hiện bất nhất đồ thị thực thể (`ErrInconsistentGraph`, ví dụ bản ghi quan hệ tồn tại nhưng thiếu entity Twin tương ứng) hoặc lỗi CSDL nội bộ. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | Database pool bị quá tải, timeout giao dịch, hủy context, hoặc tình trạng bất định sau commit (uncertain commit outcome). |

#### B. Quy tắc Thử lại khi Gặp Bất định Kết quả (Commit Uncertainty & Retry Policy):
1. **Nguyên tắc "Không suy đoán thành công khi chưa chắc chắn" (Never return success for uncertain action):**
   - Khi client gửi request POST hoặc PATCH mà gặp sự cố mạng (network drop, socket timeout, hoặc nhận HTTP 503 do database deadline timeout), Client **tuyệt đối không được coi là thao tác đã thất bại hoàn toàn, và cũng không được coi là đã thành công**.
2. **Quy trình Xác minh Dữ liệu trước khi Thử lại (Verification before Retry):**
   - **Thẩm định khả năng đọc lại (Conditional Readback):** Việc gọi API đọc chi tiết `GET /v1/digital-twins/{entity_id}` phụ thuộc nghiêm ngặt vào tư cách thành viên trong `user_gateways`. Nếu caller là Platform Admin nhưng **không có tư cách thành viên trạm**, việc gọi `GET /v1/digital-twins/{entity_id}` sẽ nhận **`404 Not Found`**; mã 404 này **KHÔNG CHỨNG MINH** thực thể chưa tồn tại hoặc thao tác metadata đã thất bại, mà chỉ phản ánh giới hạn phân quyền đọc.
   - **Xác minh qua Danh mục Kiểm kê Admin (`GET /v1/admin/gateways`):** Platform Admin có thể gọi endpoint kiểm kê này để xác nhận sự tồn tại của Gateway, nhưng endpoint này **CHỈ trả về các trường `gateway_id`, `name`, `description`, `created_at`**, hoàn toàn không trả về thực thể Sensor, không phản ánh các trường PATCH của Sensor, và không thay thế cho việc đọc chi tiết Digital Twin.
   - **Xử lý khi không có quyền đọc lại (No Permitted Readback):** Trường hợp caller không có quyền gọi API đọc lại phù hợp (ví dụ Admin không là thành viên kiểm tra Sensor, hoặc kiểm tra các trường không nằm trong inventory), caller phải sử dụng công cụ vận hành tin cậy (operator tooling / supervised database reconciliation) hoặc thực hiện gửi lại có giới hạn (bounded retry) với **cùng định danh và payload trùng khớp hoàn toàn** (đối với POST) hoặc **cùng các trường bản vá** (đối với PATCH), giữ rõ trạng thái bất định và tuyệt đối không gửi payload mới gây ghi đè tùy ý lên metadata mới hơn.
   - **Trường hợp có quyền đọc lại (Caller có membership hoặc là Owner):**
     - **Đối với POST:** Nếu đọc lại thấy thực thể đã tồn tại với thông tin mong muốn, client chỉ cần tiếp nhận dữ liệu hiện tại (thao tác trước đó thực chất đã commit thành công). Nếu gửi lại đúng payload cũ, server phản hồi `200 OK` (gửi lại trùng khớp).
     - **Đối với PATCH:** Nếu kiểm tra thấy `name` hoặc `description` đã được cập nhật sang giá trị mới, không cần gửi lại. Nếu chưa cập nhật, thực hiện gửi lại request PATCH với cùng nội dung (hệ thống hỗ trợ no-op an toàn, không gây side-effect tiêu cực).
3. **Phân biệt Cơ chế Retry giữa Metadata và Credential:**
   - **Metadata API (POST/PATCH):** Không sử dụng header `Idempotency-Key` (dựa trên định danh thực thể ổn định `gateway_id`/`sensor_id`/URN để đối soát gửi lại trùng khớp và no-op). Phản hồi gửi lại trùng khớp trả về dữ liệu metadata hoàn chỉnh.
   - **Credential API (`/mqtt-credential`):** Bắt buộc dùng `Idempotency-Key: <UUID>`. Mật khẩu plaintext chỉ xuất hiện trong response đầu tiên (`secret_returned: true`); phản hồi replay **tuyệt đối không cấp lại mật khẩu** (`secret_returned: false`, trường password bị lược bỏ hoàn toàn).

---

## 4. Chi tiết Các Endpoint Quản trị Thông tin Xác thực MQTT (MQTT Credential APIs)

Nhóm endpoint này kiểm soát vòng đời thông tin xác thực trên Mosquitto Dynamic Security (DynSec) theo kiến trúc nhật ký đơn chuyến (single-flight journal engine) với khóa đồng thời PostgreSQL session advisory lock (`0x494f545f43524544`).

### 4.1 Quy chuẩn Bắt buộc đối với Request Quản trị Credential

Căn cứ hiện thực tại `src/internal/httpserver/admin_mqtt_credential_request.go` và `admin_mqtt_credential_handlers.go`:
1. **Yêu cầu Body đối với từng Endpoint:**
   - Các phương thức `GET`, `POST` (provision), `POST /rotate`, và `DELETE` (revoke) **bắt buộc phải có body rỗng (`len(body) == 0`)**. Nếu gửi kèm bất kỳ nội dung nào trong body, server sẽ từ chối ngay với mã lỗi HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`). Nếu kích thước vượt quá giới hạn đọc (`limit` được cấu hình bởi `ADMIN_MAX_BODY_BYTES`, mặc định 16384 bytes = 16 KiB), server trả về HTTP `413 Request Entity Too Large` (`code: "request_too_large"`, `message: "request body too large"`).
   - **Ngoại lệ Reset (`POST /reset`):** Bắt buộc phải có body JSON mang trường `reason` (xem [Mục 4.7](#47-ngoại-lệ-đặt-lại-credential-có-giám-sát-post-v1admingatewaysgateway_idmqtt-credentialreset)) và header `Content-Type: application/json`.
   - *Lưu ý về Content-Type và HTTP 415:* Các endpoint Credential yêu cầu body rỗng không kiểm tra `Content-Type`. Mã lỗi HTTP `415 Unsupported Media Type` chỉ áp dụng cho endpoint `POST /reset` và 2 endpoint Provisioning PUT.
2. **Cấm hoàn toàn Query Parameters:**
   - URL tuyệt đối không được chứa query string hoặc ký tự `?` (cả `RawQuery != ""` và `ForceQuery`). Vi phạm sẽ nhận HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
3. **Bắt buộc Header `Idempotency-Key` đối với các thao tác biến đổi trạng thái (Mutations):**
   - Áp dụng cho: `POST` (provision), `POST /rotate`, `DELETE` (revoke), và `POST /reset`.
   - **Quy chuẩn UUID:** Parser sử dụng `uuid.Parse(values[0])` và kiểm tra `key != uuid.Nil`. Yêu cầu đúng **một header `Idempotency-Key` chứa giá trị UUID hợp lệ khác nil UUID** (khuyến nghị client sinh UUIDv4 hoặc UUIDv7 ngẫu nhiên, ví dụ: `Idempotency-Key: c4b1e6e0-2495-4e3a-97a6-6f8d38e21a41`).
   - Nếu thiếu header này, gửi nhiều hơn một header, hoặc UUID không hợp lệ/nil UUID -> Server trả về HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
   - Đối với phương thức `GET` (truy vấn metadata): Header `Idempotency-Key` không bắt buộc và sẽ bị bỏ qua nếu có.
4. **Header điều khiển bộ nhớ đệm (Cache-Control):**
   - Middleware `credentialCacheMiddleware` chạy trước tầng xác thực để đảm bảo kể cả các phản hồi lỗi nhạy cảm cũng tự động được gán:
     ```http
     Cache-Control: no-store
     Pragma: no-cache
     ```
   - Đảm bảo trình duyệt và các proxy trung gian tuyệt đối không lưu vết dữ liệu mật khẩu hoặc trạng thái xác thực nhạy cảm.

---

### 4.2 Nguyên tắc Phân phối Bí mật Một Lần Duy Nhất và Khôi phục Sự cố (One-Time Secret Delivery & Replay Semantics)

1. **Phân phối một lần duy nhất (First-Response Only):**
   - Mật khẩu MQTT plaintext (được sinh bởi CSPRNG với ít nhất 128 bit entropy) **chỉ xuất hiện duy nhất trong response thành công đầu tiên** của lệnh tạo mới (`POST`, HTTP 201) hoặc xoay vòng (`POST /rotate`, HTTP 200).
   - **Không cam kết zeroization vật lý trong bộ nhớ (No Physical Memory Zeroization Guarantee):** Hàm `ClearSecret()` trong Go runtime chỉ đơn thuần xóa tham chiếu chuỗi mật khẩu trong struct Go (`r.password = ""`) sau khi xử lý response để phục vụ bộ thu gom rác (garbage collection) tự nhiên; runtime không cam kết zeroization bộ nhớ RAM vật lý hay xóa sạch các bản sao do Go runtime/caller tạo ra.
   - Go backend **tuyệt đối không lưu mật khẩu plaintext trong database, không lưu trong file log, không đưa vào bộ nhớ đệm, và không có bất kỳ API nào cho phép export/đọc lại mật khẩu đã cấp**. Không có cam kết exactly-once, durability hay bảo đảm vận chuyển một lần; nếu secret trong response đầu tiên bị thất lạc trên mạng thì secret đó vĩnh viễn không thể đọc lại.
2. **Cơ chế Replay khi dùng cùng `Idempotency-Key`:**
   - Nếu Client thực hiện lại (replay) cùng phương thức, cùng đường dẫn, cùng actor, và cùng `Idempotency-Key`:
     - Server **không sinh mật khẩu mới**, và **tuyệt đối không cấp lại mật khẩu cũ**.
     - Dữ liệu `Metadata` trả về trong kết quả replay phản ánh **thẩm quyền và trạng thái HIỆN TẠI (CURRENT authority)** từ cơ sở dữ liệu, không phải dữ liệu kiểm toán cũ đã lỗi thời.
     - **Phân định rõ kết quả replay theo trạng thái thao tác gốc:**
       - Nếu thao tác trước đó vẫn đang xử lý (`pending`): Server trả về mã lỗi HTTP `409 Conflict` (`code: "operation_in_progress"`).
       - Nếu thao tác trước đó đang ở trạng thái cần phục hồi (`recovery_needed`): Server trả về mã lỗi HTTP `503 Service Unavailable` (`code: "credential_recovery_required"`).
       - Nếu thao tác trước đó đã thất bại (`failed`): Hàm `writeCredentialMutation` ánh xạ sang phản hồi lỗi (mã HTTP 500 hoặc mã lỗi cụ thể từ `ErrorCode`) với thông báo `"credential request failed"`, **tuyệt đối không trả về mã 200/201 thành công**.
       - Nếu thao tác trước đó đã hoàn tất thành công (`succeeded`): Server phản hồi bản ghi **metadata-only** với HTTP 201 (đối với provision) hoặc HTTP 200 (đối với rotate/revoke); trường `secret_returned` mang giá trị `false`, trường `password` **bị lược bỏ hoàn toàn khỏi JSON** (omitted, không phải `password: null`), trường `replayed_status` mang giá trị `"succeeded"`, và trường `next_action` hướng dẫn bước thao tác tiếp theo.
     - *Lưu ý về bằng chứng runtime:* Cờ bằng chứng `snapshot_observed` ghi nhận việc tệp snapshot native đã được quan sát, **không phải là bằng chứng fsync hay bằng chứng chống mất điện (power-loss proof)**.
3. **Quy trình Khôi phục khi Mất Mát Phản hồi trên Mạng (Lost Secret Recovery) và Cảnh báo Bất định:**
   - Trường hợp Client gửi request thành công, cơ sở dữ liệu đã commit giao dịch, Mosquitto đã nạp mật khẩu mới, nhưng phản hồi HTTP bị đứt gãy trên đường truyền (network drop / client crash) khiến Admin không nhận được mật khẩu:
     - **Không thể lấy lại mật khẩu cũ:** Gửi lại request với `Idempotency-Key` cũ chỉ nhận về bản ghi metadata (`secret_returned: false`), không bao giờ nhận lại mật khẩu.
     - **Biện pháp khôi phục chuẩn:**
       - Kiểm tra lại trạng thái hiện tại bằng lệnh `GET /v1/admin/gateways/{gateway_id}/mqtt-credential` để xem `credential_version`, `status` và `last_operation_id`.
       - Nếu trạng thái hiện tại là `active`: Kiểm tra xác thực quyền và thực hiện gọi endpoint xoay vòng mật khẩu (`POST /rotate`) với một **`Idempotency-Key` mới** để cấp phát một cặp mật khẩu mới thay thế.
       - Nếu trạng thái hiện tại là `revoked`: Chỉ thực hiện cấp mới (`POST`) với một **`Idempotency-Key` mới** sau khi các tiến trình đối soát (reconciliation) đã được giải quyết sạch sẽ.
       - **CẢNH BÁO BẢO MẬT & VẬN HÀNH (Operational Warning):**
         * Trường hợp `status == "active"` trong metadata đơn thuần **chưa chứng minh tất cả các checkpoint chưa hoàn tất đã sạch**. Nếu vừa xảy ra tình huống bất định trong commit hoặc hoàn tất (finalization ambiguity), Platform Admin **tuyệt đối không được blind-rotate mù quáng hoặc tự động thử lại vô hạn**.
         * Mã lỗi `credential_runtime_busy` có thể phản ánh runtime đang bị khóa hoặc bị nghi ngờ (poisoned state do thao tác trước chưa giải phóng lock `0x494f545f43524544`), **không đơn thuần là tình trạng quá tải tạm thời**. Chỉ retry khi đã xác định an toàn theo tài liệu vận hành; khi cần thiết phải leo thang cho kỹ sư vận hành hạ tầng kiểm tra nhật ký CSDL.
         * Tuyệt đối không tự động gọi vào endpoint `POST /reset` vì endpoint này đang bị khóa fail-closed 503 (`denyOnlyResetPrerequisite{}`).

---

### 4.3 Truy vấn Metadata Credential: `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`

Truy vấn thông tin kiểm toán và trạng thái kích hoạt thông tin xác thực MQTT hiện tại của Gateway.

#### A. Tham số Request
- Path param: `gateway_id` (string, bắt buộc).
- Header bắt buộc: `Authorization: Bearer <token>`. `Accept: application/json` là khuyến nghị, không phải điều kiện parser.
- Request Body: Rỗng. Không có query parameters.

#### B. Cấu trúc Response Body (`Metadata` DTO)
| Trường | Kiểu dữ liệu | Ý nghĩa & Quy tắc JSON |
|---|---|---|
| `gateway_id` | string | Khóa định danh của Gateway. |
| `username` | string | Tên đăng nhập MQTT của Gateway (do Go backend thiết lập đồng nhất bằng chính `gateway_id` của trạm). |
| `credential_version` | integer | Phiên bản credential hiện tại (tăng đơn điệu sau mỗi lần cấp mới/xoay vòng). |
| `status` | string | Trạng thái nghiệp vụ: `"provisioning"`, `"active"`, `"rotating"`, `"revoking"`, `"revoked"`, `"failed"`, `"recovery_needed"`. |
| `last_operation_id` | string (UUID) | Định danh của thao tác kiểm toán gần nhất trên CSDL. |
| `operation_status` | string | Trạng thái thao tác gần nhất: `"pending"`, `"succeeded"`, `"failed"`, `"recovery_needed"`, `"resolved_by_recovery"`. |
| `activated_at` | string hoặc null | Thời điểm kích hoạt gần nhất (RFC3339 UTC). Không có `omitempty` (luôn trả về `null` trong JSON khi chưa kích hoạt). |
| `revoked_at` | string hoặc null | Thời điểm thu hồi gần nhất (RFC3339 UTC). Không có `omitempty` (luôn trả về `null` trong JSON khi chưa thu hồi). |
| `last_error_code` | string hoặc null | Mã lỗi kiểm toán gần nhất. Không có `omitempty` (luôn trả về `null` trong JSON khi không có lỗi). |

*Lưu ý về các trường tùy chọn trong `MutationResult` (`POST`, `POST /rotate`, `DELETE`):*
- Trường `operation_id` (string UUID): Định danh của thao tác hiện tại hoặc thao tác gốc khi replay.
- Trường `secret_returned` (boolean): `true` trong response thành công đầu tiên chứa mật khẩu, `false` trong mọi trường hợp replay hoặc revoke.
- Trường `next_action` (string, `omitempty`): Chỉ xuất hiện khi có chỉ dẫn thao tác tiếp theo (ví dụ: `"rotate_with_new_idempotency_key"` hoặc `"provision_with_new_idempotency_key"`).
- Trường `replayed_status` (string, `omitempty`): Chỉ xuất hiện trong các phản hồi replay, phản ánh trạng thái gốc (`"succeeded"`).
- Trường `error_code` (string, `omitempty`): Chỉ xuất hiện khi phản hồi replay mang thông tin lỗi kiểm toán đã lưu.

*Ví dụ Response (HTTP 200 OK):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null
}
```

---

### 4.4 Cấp mới Credential Ban đầu: `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`

Cấp mới thông tin xác thực MQTT cho Gateway chưa có credential hoặc đã bị thu hồi (`revoked`).

#### A. Tham số Request
- Path param: `gateway_id` (string, bắt buộc).
- Header bắt buộc:
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Idempotency-Key: <unique_non_nil_uuid>`
   - Header khuyến nghị (không bắt buộc): `Accept: application/json`
- Request Body: Bắt buộc rỗng. Không có query parameters.

#### B. Phản hồi Phân phối Mật khẩu Lần đầu (HTTP 201 Created - First Response)
Server phản hồi DTO `SecretResult`: bao gồm toàn bộ các trường của `Metadata`, bổ sung định danh thao tác `operation_id`, cờ `secret_returned: true`, và chuỗi mật khẩu `password`.

*Ví dụ Response (Mô tả trường mật khẩu an toàn, không chứa giá trị thực):*
```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000010",
  "password": "<one_time_generated_secure_password_string>",
  "secret_returned": true
}
```

#### C. Phản hồi Khi Replay Cùng `Idempotency-Key` (HTTP 201 Created - Replay Response)
Server phản hồi DTO `MutationResult`: trường `secret_returned` mang giá trị `false`, tuyệt đối không có trường `password`.

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 1,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000010",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T08:40:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000010",
  "secret_returned": false,
  "replayed_status": "succeeded",
  "next_action": "rotate_with_new_idempotency_key"
}
```

#### D. Lỗi Xung đột Thường gặp
- **HTTP 409 Conflict (`code: "credential_conflict"`):** Gateway này hiện đã có thông tin xác thực đang ở trạng thái `active`. Để đổi mật khẩu, phải gọi sang endpoint `POST /rotate`.

---

### 4.5 Xoay vòng Credential: `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`

Xoay vòng mật khẩu cho Gateway đang ở trạng thái `active`.

#### A. Tham số Request
- Tương tự endpoint `POST` cấp mới: Yêu cầu header `Idempotency-Key: <unique_non_nil_uuid>`, body rỗng, không có query parameters.

#### B. Phản hồi Phân phối Mật khẩu Lần đầu (HTTP 200 OK - First Response)
Server trả về mã HTTP `200 OK` (khác với mã `201 Created` của lệnh cấp mới). Cấu trúc tương tự `SecretResult`, với `credential_version` được tăng lên (ví dụ: từ 1 lên 2), kèm mật khẩu mới và `secret_returned: true`.

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "active",
  "last_operation_id": "00000000-0000-0000-0000-000000000020",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T09:00:00Z",
  "revoked_at": null,
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000020",
  "password": "<one_time_newly_rotated_password_string>",
  "secret_returned": true
}
```

#### C. Phản hồi Khi Replay Cùng `Idempotency-Key` (HTTP 200 OK - Replay Response)
Trả về HTTP `200 OK` với DTO `MutationResult`, không có mật khẩu, `secret_returned: false`, `next_action: "rotate_with_new_idempotency_key"`.

#### D. Lỗi Xung đột Thường gặp
- **HTTP 409 Conflict (`code: "credential_conflict"`):** Gateway chưa từng có thông tin xác thực hoặc đang ở trạng thái `revoked`, không thể thực hiện xoay vòng.

---

### 4.6 Vô hiệu hóa và Thu hồi Credential: `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`

Vô hiệu hóa và thu hồi (disable/revoke) toàn bộ quyền truy cập MQTT của Gateway, vô hiệu hóa tài khoản trên Mosquitto DynSec (`disabled=true`) và ngắt socket kết nối hiện hành. Thao tác này không có cơ chế tự động khôi phục bí mật (no automatic secret recovery); tuy nhiên, đây không phải là trạng thái khóa vĩnh viễn không thể khôi phục: khi Gateway đã ở trạng thái `revoked`, Platform Admin hoàn toàn có thể cấp mới credential (`POST /v1/admin/gateways/{gateway_id}/mqtt-credential`) bằng một `Idempotency-Key` mới độc lập.

#### A. Tham số Request
- Method: `DELETE`.
- Header bắt buộc: `Authorization: Bearer <token>`, `Idempotency-Key: <unique_non_nil_uuid>`.
- Request Body: Bắt buộc rỗng. Không có query parameters.

#### B. Phản hồi Thu hồi Thành công (HTTP 200 OK)
Server luôn trả về mã HTTP `200 OK` (không có mật khẩu) với cấu trúc `MutationResult`:
- `status`: `"revoked"`
- `revoked_at`: Thời điểm thu hồi (RFC3339 UTC)
- `secret_returned`: `false`
- `next_action`: `"provision_with_new_idempotency_key"`

```json
{
  "gateway_id": "gw-station-alpha",
  "username": "gw-station-alpha",
  "credential_version": 2,
  "status": "revoked",
  "last_operation_id": "00000000-0000-0000-0000-000000000030",
  "operation_status": "succeeded",
  "activated_at": "2026-10-05T09:00:00Z",
  "revoked_at": "2026-10-05T09:30:00Z",
  "last_error_code": null,
  "operation_id": "00000000-0000-0000-0000-000000000030",
  "secret_returned": false,
  "next_action": "provision_with_new_idempotency_key"
}
```

*Lưu ý về Replay và No-op của Revoke:*
- Nếu gọi lại `DELETE` với **cùng `Idempotency-Key`**: Trả về `200 OK` với `replayed_status: "succeeded"`.
- Nếu gọi `DELETE` với một **`Idempotency-Key` mới** trên một Gateway đã bị thu hồi (`revoked`):
  - CSDL nhận diện đây là thao tác không phát sinh mutation mới và trả về bản ghi no-op replay mang `OperationID: uuid.Nil` và `next_action: "provision_with_new_idempotency_key"`.
  - Dịch vụ Go backend (`service.go`) chủ động kiểm tra native trên broker runtime (`runtime.CheckRevoked(ctx, in.GatewayID)`) để xác minh Mosquitto DynSec thực sự không còn tài khoản client nào của Gateway này (`disabled=true` hoặc đã bị xóa).
  - Nếu xác minh thành công: Server trả về `200 OK` với `operation_id: "00000000-0000-0000-0000-000000000000"`, `secret_returned: false`, `next_action: "provision_with_new_idempotency_key"`.
  - Nếu kiểm tra native thất bại hoặc metadata không nhất quán: Server chuyển thao tác sang trạng thái cần phục hồi và trả về mã lỗi HTTP `503 Service Unavailable` (`code: "credential_recovery_required"`, `message: "credential request failed"`).

---

### 4.7 Ngoại lệ Đặt lại Credential Có giám sát: `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/reset`

Tuyến đường này đã được định tuyến tại `src/internal/httpserver/admin_mqtt_credential_handlers.go`, tiếp nhận body JSON chỉ định lý do đặt lại (`reason`). Tuy nhiên, tại tầng composition runtime (`src/cmd/server/credential.go`), thành phần phụ thuộc được gắn kiểu `denyOnlyResetPrerequisite{}`.

#### A. Tham số Request và Quy chuẩn Phân tích Cú pháp (`parseCredentialResetRequest`)
- Method: `POST`
- Đường dẫn: `/v1/admin/gateways/{gateway_id}/mqtt-credential/reset`
- Ràng buộc tham số và headers:
  - `gateway_id` (string, bắt buộc): Phải đúng regex gateway ID (`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`), không được là `backend_service`. Tuyệt đối không chứa query string hoặc ký tự `?` (`r.URL.RawQuery != ""` hoặc `r.URL.ForceQuery`). Nếu sai -> HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
  - `Authorization: Bearer <token>`: Bắt buộc xác thực hợp lệ với claim `role == "authenticated"`. Nếu thiếu hoặc sai token -> HTTP `401 Unauthorized` (`code: "unauthorized"`, `message: "authentication required"`).
  - Phân quyền Admin: Người dùng bắt buộc phải tồn tại trong bảng `public.platform_admins`. Nếu là người dùng thường -> HTTP `403 Forbidden` (`code: "forbidden"`, `message: "insufficient permissions"`).
  - `Idempotency-Key: <unique_non_nil_uuid>`: Bắt buộc đúng 1 header chứa chuỗi UUID hợp lệ khác nil UUID (phân tích qua `uuid.Parse(values[0])`). Nếu thiếu, trùng lặp header, hoặc UUID không hợp lệ/nil -> HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).
  - `Content-Type`: Bắt buộc là `application/json` (tùy chọn tham số `charset=utf-8` không phân biệt hoa thường). Bất kỳ media type hoặc tham số nào khác -> HTTP `415 Unsupported Media Type` (`code: "unsupported_media_type"`, `message: "unsupported media type"`).
  - Giới hạn body: Giới hạn bởi `ADMIN_MAX_BODY_BYTES` (mặc định 16384 bytes = 16 KiB). Nếu kích thước vượt quá giới hạn đọc -> HTTP `413 Request Entity Too Large` (`code: "request_too_large"`, `message: "request body too large"`).
- Request Body (JSON bắt buộc, không được để trống `len(body) == 0`):
  ```json
  {
    "reason": "recovery_after_unknown"
  }
  ```
  hoặc:
  ```json
  {
    "reason": "lost_secret_recovery"
  }
  ```
- **Quy tắc phân tích cú pháp nghiêm ngặt:**
  - Trường `reason` bắt buộc phải là một trong hai giá trị định trước: `"recovery_after_unknown"` hoặc `"lost_secret_recovery"`.
  - Cấm tuyệt đối các trường lạ (bất kỳ trường nào ngoài `reason` đều bị từ chối), cấm giá trị `null`, cấm duplicate keys, cấm ký tự thừa sau dấu ngoặc nhọn kết thúc `}`. Vi phạm -> HTTP `400 Bad Request` (`code: "invalid_request"`, `message: "invalid request"`).

#### B. Hành vi Phản hồi Runtime và Các Điều kiện Tiên quyết (Prerequisites & Fail-Closed Behavior)
1. **Trường hợp Tính năng Credential bị Tắt (`MQTT_CREDENTIAL_API_ENABLED=false`):**
   - Handler kiểm tra cờ `deps.CredentialAPIEnabled` trước khi kiểm tra tầng readiness/resetter.
   - Nếu cờ này tắt, server phản hồi HTTP **`503 Service Unavailable`** với mã lỗi đặc thù:
     ```json
     {
       "error": {
         "code": "credential_runtime_disabled",
         "message": "credential request failed",
         "request_id": "<request_id>"
       }
     }
     ```
   - *Lưu ý quan trọng:* Không phải mọi request gọi vào endpoint reset đều trả về `service_unavailable`; nếu tính năng bị tắt thì mã lỗi trả về là `credential_runtime_disabled`.
2. **Trường hợp Tính năng Credential được Bật nhưng Rào chắn Khởi động chưa Sẵn sàng:**
   - Nếu `CredentialStartupReadiness.Ready() == false`, server phản hồi HTTP **`503 Service Unavailable`** (`code: "service_unavailable"`, `message: "credential request failed"`).
3. **Trường hợp Vượt qua Parser nhưng Thành phần Phụ thuộc là `denyOnlyResetPrerequisite{}`:**
   - Khi một request hợp lệ vượt qua toàn bộ tầng middleware (401, 403), cờ tính năng (bật), và request parser (400, 413, 415), thành phần `denyOnlyResetPrerequisite{}` tại tầng composition luôn chặn đứng việc thực thi và trả về lỗi:
     ```http
     HTTP/1.1 503 Service Unavailable
     Content-Type: application/json; charset=utf-8
     Cache-Control: no-store
     Pragma: no-cache
     X-Request-ID: 00000000-0000-0000-0000-000000000040
     ```
     ```json
     {
       "error": {
         "code": "service_unavailable",
         "message": "credential request failed",
         "request_id": "00000000-0000-0000-0000-000000000040"
       }
     }
     ```

#### C. Tuyên bố Ranh giới An toàn Vận hành (Operational Safety Boundary):
- **CHƯA PHẢI LÀ QUY TRÌNH PHỤC HỒI KHẢ DỤNG:** Trạng thái fail-closed HTTP 503 này là một rào chắn an toàn kỹ thuật nhằm ngăn ngừa rủi ro sai lệch dữ liệu trong Giai đoạn 2 (được ghi nhận theo ngoại lệ T07 hoãn lại tại `AGENTS.md`).
- **Các thành phần chưa được hiện thực:**
  - Chưa có cơ chế chứng minh đường truyền điều khiển đã hoàn toàn tĩnh lặng (proven control path quiescence).
  - Chưa có hàng rào bảo vệ ngăn chặn việc ghi đè các mutation cũ chưa dứt điểm (stale mutation overwrites).
  - Chưa kiểm chứng khôi phục bí mật thất lạc (lost secret recovery) trong điều kiện vận hành thực tế.
- **Khuyến nghị Client/Admin:** Tuyệt đối không lập trình tự động thử lại (no automated retry loop) vào endpoint reset này. Mọi tình huống cần can thiệp phục hồi phải tuân thủ runbook vận hành hạ tầng có sự giám sát trực tiếp của kỹ sư hệ thống.

---

## 5. Chi tiết Các Endpoint Quản trị Tài khoản Người dùng và Quyền Thành viên Gateway (Admin User & Gateway Membership APIs)

Nhóm 5 endpoint này được hiện thực nhằm đáp ứng năng lực quản trị người dùng và phân quyền Gateway tập trung dành cho Platform Admin theo Kế hoạch chi tiết và Migration 21 (`000021_admin_user_management.up.sql`), giải quyết hoàn toàn hạn chế trước đây khi phải can thiệp thủ công qua Supabase Studio.

### 5.1 Tạo Tài khoản Người dùng Mới: `POST /v1/admin/users`

Endpoint này cho phép Platform Admin tạo tài khoản người dùng mới trực tiếp thông qua Go Backend. Backend gọi Supabase Auth Admin API (thông qua Envoy Gateway) với cờ `email_confirm: true`, tự động sinh bản ghi hồ sơ trong `public.profiles`.

- **HTTP Method:** `POST`
- **Đường dẫn:** `/v1/admin/users`
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của người dùng đã xác thực (`role == "authenticated"`).
  - Bắt buộc phải có quyền Platform Admin trong bảng `public.platform_admins`.
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Content-Type: application/json`
  - `X-Request-ID: <id>` (khuyến nghị)
- **Response Headers:**
  - `Cache-Control: no-store`
  - `Pragma: no-cache`
  - `Location: /v1/admin/users/<user_id>` (khi `201 Created`)
- **Quy tắc Kiểm tra Cú pháp & Ranh giới Dữ liệu (Strict Validation):**
  - Cấm query string (`?`) trên URL; vi phạm trả về `400 Bad Request` (`code: "invalid_request"`).
  - Kích thước Body client gửi lên bị giới hạn bởi `ADMIN_MAX_BODY_BYTES` (mặc định 16 KiB, tối đa 1 MiB); vượt quá trả về `413 Payload Too Large` (`code: "payload_too_large"`). Phản hồi từ dịch vụ Auth thượng nguồn được giới hạn đọc an toàn bởi `AUTH_ADMIN_MAX_BODY_BYTES` trong Go adapter.
  - Cấm trường lạ (unknown fields) hoặc trùng lặp khóa JSON -> `400 Bad Request`.
  - `full_name` (string, bắt buộc): Chuỗi UTF-8 hợp lệ, độ dài từ 1 đến 256 bytes, không được để trống hoặc chỉ chứa khoảng trắng. Không nhận alias `name`.
  - `email` (string, bắt buộc): Định dạng email hợp lệ theo RFC 5322, tối đa 254 bytes. Khoảng trắng ngoài rìa được chuẩn hóa tự động; giữ nguyên chữ hoa/thường (case-preserving). Cấm chứa ký tự điều khiển, newline, hoặc display-name.
  - `password` (string, bắt buộc): Mật khẩu do Admin nhập. Độ dài bắt buộc từ 6 ký tự (runes) đến tối đa 72 bytes UTF-8 (phù hợp với giới hạn thuật toán bcrypt của GoTrue). Không được để trống.
  - Cấm client gửi `platform_role`, membership, `email_confirm`, hay UUID trong body POST.
- **Hành vi Xử lý Phía Server:**
  - Backend kiểm tra quyền Platform Admin của actor trong CSDL trước khi tạo tác dụng phụ sang Auth.
  - Backend gọi sang Supabase Auth Admin API nội bộ với `email_confirm: true` và `user_metadata: {"full_name": ...}`.
  - Sau khi Auth tạo tài khoản, backend xác minh bản ghi hồ sơ `public.profiles` đã được tạo tương ứng với đúng tên, đồng thời xác nhận tài khoản mới chưa có quyền `platform_admin` và chưa có membership Gateway nào.
  - Tài khoản mới tạo là người dùng thông thường (`platform_role: "user"`) và chưa có quyền trên bất kỳ trạm nào (`gateway_roles: []`). Người dùng có thể đăng nhập ngay bằng email/mật khẩu qua `POST /auth/v1/token?grant_type=password` mà không cần xác nhận email qua SMTP.
- **Request Body Mẫu:**
  ```json
  {
    "full_name": "Nguyen Van A",
    "email": "nguyenvana@example.com",
    "password": "SecurePassword123!"
  }
  ```
- **Response Phản hồi Thành công (`201 Created`):**
  ```http
  HTTP/1.1 201 Created
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  Location: /v1/admin/users/11111111-2222-3333-4444-555555555555
  X-Request-ID: 00000000-0000-0000-0000-000000000101

  {
    "id": "11111111-2222-3333-4444-555555555555",
    "email": "nguyenvana@example.com",
    "full_name": "Nguyen Van A",
    "platform_role": "user",
    "created_at": "2026-10-19T10:00:00Z"
  }
  ```
  *(Lưu ý an toàn: Phản hồi tuyệt đối không chứa mật khẩu, hash mật khẩu, hoặc token xác thực).*
- **Các Trường hợp Lỗi Nghiệp vụ:**
  - `409 Conflict` (`code: "user_already_exists"`): Email đã được đăng ký trong hệ thống.
  - `400 Bad Request` (`code: "invalid_request"`): Vi phạm định dạng email, mật khẩu quá ngắn (<6 ký tự) hoặc quá dài (>72 bytes), tên để trống hoặc chứa UTF-8 không hợp lệ.
  - `503 Service Unavailable` (`code: "service_unavailable"`): Dịch vụ Supabase Auth không phản hồi, bị timeout, hoặc kiểm tra hồ sơ sau tạo thất bại.
  - *Xử lý sự cố timeout/không rõ kết quả (Uncertain Outcome):* Nếu request gặp timeout hoặc lỗi 503, backend **tuyệt đối không tự ý xóa tài khoản (no automatic rollback deletion)** và client **không được tự động thử lại mù quáng (no blind retry)**. Quản trị viên cần gọi `GET /v1/admin/users` để kiểm tra tài khoản đã tồn tại hay chưa trước khi tiến hành bước tiếp theo.

---

### 5.2 Truy vấn Toàn bộ Danh sách Người dùng: `GET /v1/admin/users`

Endpoint này cung cấp danh sách toàn bộ tài khoản người dùng trong hệ thống, bao gồm vai trò nền tảng (`platform_role`) và danh sách các quyền trạm Gateway (`gateway_roles`), phục vụ giao diện quản trị phân quyền mà không cần thực hiện N+1 truy vấn.

- **HTTP Method:** `GET`
- **Đường dẫn:** `/v1/admin/users`
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của Platform Admin (`public.platform_admins`).
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
- **Response Headers:**
  - `Cache-Control: no-store`
  - `Pragma: no-cache`
- **Quy tắc Kiểm tra Cú pháp & Giới hạn Tài nguyên (Bounds & Capacity):**
  - **Cấm tuyệt đối Query Parameters:** Bất kỳ query string nào (ví dụ: `?limit=10`, `?search=abc`) đều bị từ chối với `400 Bad Request` (`code: "invalid_request"`). Không hỗ trợ phân trang hay tìm kiếm phía server.
  - **Cấm Request Body:** Body gửi kèm phải hoàn toàn rỗng; vi phạm trả về `400 Bad Request`.
  - **Ranh giới tài nguyên cấu hình:**
    - `USER_LIST_MAX_USERS` (mặc định 1.000, cấu hình từ 1 đến 10.000).
    - `USER_LIST_MAX_MEMBERSHIPS` (mặc định 10.000, cấu hình từ 1 đến 100.000).
    - `USER_LIST_MAX_RESPONSE_BYTES` (mặc định 2 MiB, cấu hình từ 1 KiB đến 10 MiB).
  - **Cơ chế Phát hiện Tràn Giới hạn (Overflow Detection):** Backend truy vấn cơ sở dữ liệu với giới hạn `bound + 1`. Nếu số lượng người dùng vượt quá `MaxUsers`, số lượng membership vượt quá `MaxMemberships`, hoặc dung lượng JSON vượt quá `MaxResponseBytes`, server phản hồi ngay lập tức mã lỗi **`409 Conflict`** (`code: "user_list_capacity_exceeded"`), **tuyệt đối không âm thầm cắt ngắn danh sách (no silent truncation)** và không trả về danh sách một phần.
- **Hành vi Sắp xếp & Nguồn Dữ liệu:**
  - Danh sách người dùng được sắp xếp ổn định: `created_at ASC, id ASC`.
  - Danh sách quyền trạm `gateway_roles` của mỗi người dùng được sắp xếp ổn định theo `gateway_id ASC`.
  - Trường `full_name` được lấy duy nhất từ `public.profiles.full_name` (dạng con trỏ nullable `*string` để giữ nguyên tính trung thực của các tài khoản legacy nếu thiếu tên, không tự bịa chuỗi rỗng `""`). Sau khi đổi tên qua PATCH, tên mới là nguồn chân lý duy nhất (không fallback sang Auth metadata).
  - Dữ liệu định danh được đọc thông qua hàm bảo mật `public.list_admin_users_projection` (quyền hạn hẹp, không để lộ mật khẩu, hash hay token). Nếu phát hiện tài khoản Auth tồn tại nhưng thiếu bản ghi `profiles`, hàm trả lỗi bất nhất (`ErrInconsistentProfile`) thay vì bỏ qua tài khoản.
- **Response Phản hồi Thành công (`200 OK`):**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  X-Request-ID: 00000000-0000-0000-0000-000000000102

  {
    "items": [
      {
        "id": "00000000-0000-4000-8000-000000000001",
        "email": "admin@example.com",
        "full_name": "System Administrator",
        "platform_role": "platform_admin",
        "created_at": "2026-08-24T00:00:00Z",
        "gateway_roles": [
          {
            "gateway_id": "gw-001",
            "role": "owner"
          }
        ]
      },
      {
        "id": "11111111-2222-3333-4444-555555555555",
        "email": "nguyenvana@example.com",
        "full_name": "Nguyen Van A",
        "platform_role": "user",
        "created_at": "2026-10-19T10:00:00Z",
        "gateway_roles": []
      }
    ]
  }
  ```
  *(Nếu hệ thống chưa có người dùng nào, phản hồi trả về `{"items": []}`).*

---

### 5.3 Cập nhật Tên hiển thị và Quyền Nền tảng: `PATCH /v1/admin/users/{user_id}`

Endpoint này cho phép cập nhật tên hiển thị người dùng và/hoặc gán/thu hồi quyền Platform Admin toàn hệ thống trong một giao dịch cơ sở dữ liệu duy nhất.

- **HTTP Method:** `PATCH`
- **Đường dẫn:** `/v1/admin/users/{user_id}` (với `{user_id}` là chuỗi UUID v4 hợp lệ khác nil).
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của Platform Admin.
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Content-Type: application/json`
- **Response Headers:**
  - `Cache-Control: no-store`
  - `Pragma: no-cache`
- **Quy tắc Kiểm tra Request:**
  - Kích thước Body giới hạn theo `ADMIN_MAX_BODY_BYTES`.
  - Body phải chứa ít nhất một trong hai trường: `full_name` hoặc `platform_role`.
  - Cấm truyền giá trị `null` cho các trường có mặt trong payload (`400 Bad Request`).
  - `full_name` (string, tùy chọn): 1 đến 256 bytes UTF-8, không được chứa toàn khoảng trắng. Sửa trực tiếp vào `public.profiles.full_name`.
  - `platform_role` (string, tùy chọn): Chỉ chấp nhận đúng 2 giá trị `"platform_admin"` hoặc `"user"`. Mọi giá trị khác (kể cả `"authenticated"`, `"service_role"`, `"owner"`) đều bị từ chối với `400 Bad Request`.
- **Nguyên tắc Giao dịch & An toàn Nghiệp vụ:**
  - **Thực thi nguyên tử trong Transaction:** Cập nhật tên và vai trò diễn ra trong cùng một transaction PostgreSQL có khóa dòng `FOR UPDATE` trên bản ghi profile. Nếu việc gán vai trò thất bại, phần đổi tên cũng tự động được rollback.
  - **Cấm tuyệt đối tự hạ quyền Admin (Self-Demotion Forbidden):**
    - Nếu `actor_id == user_id` và yêu cầu `platform_role: "user"`, server từ chối ngay lập tức với mã HTTP **`409 Conflict`** (`code: "self_admin_demotion_forbidden"`).
    - Quy tắc này áp dụng ngay cả khi request kết hợp cả đổi tên và hạ quyền: toàn bộ request bị từ chối và tên không bị thay đổi.
    - Quản trị viên được phép tự đổi tên của chính mình (`full_name`) hoặc gửi `platform_role: "platform_admin"` cho chính mình dưới dạng no-op.
  - **Không có chính sách bảo vệ admin cuối cùng (No Last-Admin Count Policy):** Hệ thống không thực hiện đếm tổng số admin để khóa việc hạ quyền của admin khác. Một admin có thể hạ quyền của một admin khác miễn là không tự hạ quyền chính mình.
  - **Xử lý No-op an toàn:** Nếu gán vai trò `"platform_admin"` cho người đã là admin, hệ thống thực thi no-op (`ON CONFLICT DO NOTHING`), bảo toàn nguyên vẹn thông tin người cấp ban đầu (`granted_by`) và thời điểm cấp (`granted_at`).
  - **Hiệu lực quyền tức thì ở request kế tiếp:** Việc thêm hoặc xóa dòng trong `public.platform_admins` có hiệu lực ngay lập tức ở lần kiểm tra CSDL tiếp theo. Token JWT không cần cấp lại và không cần chờ token hết hạn.
- **Request Body Mẫu:**
  ```json
  {
    "full_name": "Nguyen Van A (Cap nhat)",
    "platform_role": "platform_admin"
  }
  ```
- **Response Phản hồi Thành công (`200 OK`):**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  X-Request-ID: 00000000-0000-0000-0000-000000000103

  {
    "id": "11111111-2222-3333-4444-555555555555",
    "full_name": "Nguyen Van A (Cap nhat)",
    "platform_role": "platform_admin"
  }
  ```
- **Các Trường hợp Lỗi Nghiệp vụ:**
  - `409 Conflict` (`code: "self_admin_demotion_forbidden"`): Admin tự gửi yêu cầu hạ quyền admin của chính mình.
  - `404 Not Found` (`code: "not_found"`): Không tìm thấy người dùng mục tiêu trong `public.profiles`.
  - `400 Bad Request` (`code: "invalid_request"`): Body rỗng, chứa trường lạ, hoặc giá trị role không hợp lệ.

---

### 5.4 Gán Quyền Operator / Viewer trên Gateway: `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`

Endpoint này cho phép Platform Admin gán hoặc cập nhật vai trò thành viên thông thường (`operator` hoặc `viewer`) cho người dùng trên một trạm Gateway cụ thể.

- **HTTP Method:** `PUT`
- **Đường dẫn:** `/v1/admin/gateways/{gateway_id}/users/{user_id}`
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của Platform Admin.
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Content-Type: application/json`
- **Response Headers:**
  - `Cache-Control: no-store`
  - `Pragma: no-cache`
- **Quy tắc Kiểm tra Request:**
  - `gateway_id`: Định dạng Gateway ID hợp lệ (1..64 ký tự, regex chuẩn, khác `backend_service`).
  - `user_id`: UUID v4 hợp lệ khác nil.
  - Body: JSON object chứa đúng một trường `role`.
  - `role` (string, bắt buộc): **Chỉ chấp nhận `"operator"` hoặc `"viewer"`.**
  - **Cấm vai trò `owner` qua endpoint này:** Nếu gửi `{"role": "owner"}`, server phản hồi mã HTTP **`400 Bad Request`** với mã lỗi `code: "invalid_role"` và thông báo `"owner role requires owner transfer endpoint"`. Thao tác gán owner bắt buộc phải qua endpoint chuyển giao quyền sở hữu riêng biệt.
- **Nguyên tắc Giao dịch & An toàn Nghiệp vụ:**
  - **Khóa trạm Gateway (`FOR UPDATE`):** Transaction khóa dòng của Gateway mục tiêu, đảm bảo các thao tác gán quyền và chuyển giao chủ sở hữu trên cùng trạm được tuần tự hóa an toàn.
  - **Xác thực tồn tại:** Cả trạm Gateway và người dùng mục tiêu đều phải tồn tại trong CSDL (`404 Not Found` nếu thiếu một trong hai).
  - **Bảo vệ vị thế Chủ sở hữu hiện tại (Existing Owner Protection):** Nếu người dùng mục tiêu hiện đang là `owner` của Gateway này, server từ chối hạ quyền với mã HTTP **`409 Conflict`** (`code: "owner_transfer_required"`). Không thể hạ quyền owner thành operator/viewer thông qua API này.
  - **Bảo toàn `created_at` khi cập nhật:**
    - Nếu người dùng chưa có membership: Thêm mới bản ghi vào `public.user_gateways`.
    - Nếu người dùng đã có vai trò operator/viewer: Cập nhật trường `role`. Thời điểm cấp ban đầu (`created_at`) được giữ nguyên vẹn.
    - Nếu gửi lại đúng vai trò hiện tại: Server thực hiện no-op an toàn và trả về `200 OK`.
- **Request Body Mẫu:**
  ```json
  {
    "role": "operator"
  }
  ```
- **Response Phản hồi Thành công (`200 OK`):**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  X-Request-ID: 00000000-0000-0000-0000-000000000104

  {
    "gateway_id": "gw-001",
    "user_id": "11111111-2222-3333-4444-555555555555",
    "role": "operator"
  }
  ```

---

### 5.5 Chuyển Quyền Sở hữu Gateway: `PUT /v1/admin/gateways/{gateway_id}/owner`

Endpoint này cho phép Platform Admin chuyển giao quyền chủ sở hữu (`owner`) của trạm Gateway cho một người dùng khác một cách nguyên tử tuyệt đối.

- **HTTP Method:** `PUT`
- **Đường dẫn:** `/v1/admin/gateways/{gateway_id}/owner`
- **Yêu cầu xác thực & Thẩm quyền:**
  - Bearer JWT của Platform Admin.
- **Request Headers:**
  - `Authorization: Bearer <platform_admin_jwt>`
  - `Content-Type: application/json`
- **Response Headers:**
  - `Cache-Control: no-store`
  - `Pragma: no-cache`
- **Quy tắc Kiểm tra Request:**
  - `gateway_id`: Định dạng Gateway ID hợp lệ khác `backend_service`.
  - Body: JSON object chứa trường `user_id` của chủ sở hữu mới.
  - `user_id` (string, bắt buộc): Chuỗi UUID v4 hợp lệ khác nil của người dùng nhận quyền sở hữu.
- **Nguyên tắc Giao dịch Nguyên tử (Atomic Owner Transfer Semantics):**
  Thao tác diễn ra trong đúng một transaction duy nhất với các bước kiểm tra chặt chẽ:
  1. Tái thẩm định quyền Platform Admin của actor trong transaction.
  2. Khóa dòng trạm Gateway mục tiêu bằng `SELECT ... FOR UPDATE` (cùng cơ chế khóa với endpoint 5.4).
  3. Xác thực người dùng mục tiêu tồn tại trong `public.profiles` (`404 Not Found` nếu không tồn tại).
  4. Đọc danh sách owner hiện tại của trạm kèm khóa `FOR UPDATE`: Bắt buộc phải có **đúng 1 owner hiện hữu**. Nếu trạm có 0 owner hoặc nhiều hơn 1 owner, server fail-closed và trả về mã lỗi **`409 Conflict`** (`code: "gateway_owner_inconsistent"`), tuyệt đối không tự ý sửa dữ liệu legacy.
  5. Nếu người dùng mục tiêu chính là owner hiện tại: Transaction commit ngay dưới dạng no-op an toàn và trả về `200 OK`.
  6. **Chuyển giao và Hạ quyền Owner cũ thành Viewer (Demote Old Owner to Viewer):**
     - Bản ghi membership của owner cũ được cập nhật thành `role = 'viewer'`.
     - Owner cũ **tiếp tục duy trì quyền đọc** dữ liệu trạm và viễn trắc; quyền ghi cảm biến bị thu hồi (`403 Forbidden`). Thời điểm tạo ban đầu (`created_at`) của owner cũ được bảo toàn.
  7. **Thăng cấp Người nhận thành Owner:**
     - Nếu người nhận đã có vai trò `operator` hoặc `viewer`: Cập nhật thành `role = 'owner'`, giữ nguyên `created_at`.
     - Nếu người nhận chưa có membership: Thêm mới bản ghi với `role = 'owner'`.
  8. **Kiểm tra Hậu điều kiện (Post-condition Verification):** Truy vấn lại số lượng owner của trạm trong transaction, khẳng định còn đúng 1 owner duy nhất trước khi Commit.
- **Tác động Nghiệp vụ Quan trọng:**
  - Thao tác chuyển owner **chỉ thay đổi bảng thành viên `public.user_gateways`**. Không đặt lại hoặc ảnh hưởng đến thực thể Digital Twin, `reported_state`, `desired_state`, chuỗi thời gian hay thông tin xác thực MQTT của Gateway.
  - **Tác động tới Gateway Provisioning Replay:** Sau khi chuyển owner, nếu một request tạo Gateway cũ gửi lại (`PUT /v1/admin/gateways/{gateway_id}`) với thông tin `owner_user_id` là owner cũ, Go backend sẽ phản hồi **`409 Conflict`** do vi phạm kiểm tra trùng khớp chủ sở hữu (theo đúng quy chuẩn replay bất biến). Quản trị viên và client cần ghi nhận hành vi chuẩn này.
- **Request Body Mẫu:**
  ```json
  {
    "user_id": "11111111-2222-3333-4444-555555555555"
  }
  ```
- **Response Phản hồi Thành công (`200 OK`):**
  ```http
  HTTP/1.1 200 OK
  Content-Type: application/json; charset=utf-8
  Cache-Control: no-store
  Pragma: no-cache
  X-Request-ID: 00000000-0000-0000-0000-000000000105

  {
    "gateway_id": "gw-001",
    "owner_user_id": "11111111-2222-3333-4444-555555555555"
  }
  ```
- **Các Trường hợp Lỗi Nghiệp vụ:**
  - `409 Conflict` (`code: "gateway_owner_inconsistent"`): Trạng thái CSDL trạm bất thường (không có owner hoặc có nhiều owner).
  - `404 Not Found` (`code: "not_found"`): Không tìm thấy trạm Gateway hoặc người dùng mục tiêu.
  - `400 Bad Request` (`code: "invalid_request"`): Định dạng `user_id` hoặc `gateway_id` không hợp lệ.

---

## 6. Bảng Ánh xạ Mã Lỗi Nghiệp vụ Toàn diện (Error Code Mapping Table)

### 6.1 Mã Lỗi Nghiệp vụ Quản trị Người dùng & Thành viên (User Management Error Codes)

Các endpoint `/v1/admin/users*` và `/v1/admin/gateways/{id}/users*`, `/owner` trả về phong bì lỗi chuẩn (`httpapi.APIErrorEnvelope`) với các mã lỗi nghiệp vụ rõ ràng:

| Mã lỗi HTTP | Mã lỗi nghiệp vụ (`code`) | Thông báo lỗi (`message`) | Nguyên nhân kích hoạt trong hệ thống | Hướng xử lý phía Client / Administrator |
|---|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` | Payload sai định dạng, thiếu trường bắt buộc, chứa trường lạ, chứa query string, body quá lớn, tên/mật khẩu sai kích thước. | Kiểm tra kỹ payload JSON theo đúng đặc tả DTO của từng endpoint. |
| **400 Bad Request** | `invalid_role` | `owner role requires owner transfer endpoint` | Gửi `role: "owner"` tới `PUT /v1/admin/gateways/{id}/users/{user_id}`. | Dùng endpoint riêng `PUT /v1/admin/gateways/{id}/owner` để chuyển quyền owner. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Thiếu token JWT, token hết hạn, sai chữ ký, hoặc claim `role != "authenticated"`. | Đăng nhập lại qua Supabase Auth để lấy access token mới. |
| **403 Forbidden** | `forbidden` | `access denied` | Caller không có quyền Platform Admin trong `public.platform_admins`. | Chỉ Platform Admin mới có quyền gọi các API quản trị này. |
| **404 Not Found** | `not_found` | `resource not found` | Không tìm thấy người dùng mục tiêu trong `public.profiles` hoặc Gateway trong `public.gateways`. | Kiểm tra lại tính tồn tại của UUID người dùng và Gateway ID. |
| **409 Conflict** | `user_already_exists` | `user already exists` | Email gửi trong `POST /v1/admin/users` đã tồn tại trong Supabase Auth. | Kiểm tra danh sách người dùng qua `GET /v1/admin/users` hoặc dùng email khác. |
| **409 Conflict** | `user_list_capacity_exceeded` | `user list capacity exceeded` | Số lượng user, membership hoặc dung lượng JSON vượt quá ngưỡng cấu hình của server. | Nâng ngưỡng `USER_LIST_MAX_USERS`, `USER_LIST_MAX_MEMBERSHIPS` hoặc `USER_LIST_MAX_RESPONSE_BYTES` trên server. |
| **409 Conflict** | `self_admin_demotion_forbidden` | `self admin demotion forbidden` | Admin tự gửi yêu cầu hạ quyền admin của chính mình (`actorID == targetID` và `platform_role: "user"`). | Một admin khác phải thực hiện thao tác hạ quyền này nếu cần thiết. |
| **409 Conflict** | `owner_transfer_required` | `owner transfer required` | Cố tình gọi `PUT /users/{id}` để hạ quyền của người dùng hiện đang là `owner`. | Sử dụng `PUT /owner` để chuyển owner cho người khác; người cũ sẽ tự động trở thành `viewer`. |
| **409 Conflict** | `gateway_owner_inconsistent` | `gateway owner inconsistent` | Trạm Gateway hiện tại có 0 owner hoặc nhiều hơn 1 owner trong `public.user_gateways`. | Dữ liệu CSDL không nhất quán; liên hệ kỹ sư CSDL để đối soát, server fail-closed không tự sửa. |
| **413 Payload Too Large** | `payload_too_large` | `payload too large` | Kích thước body gửi lên vượt quá giới hạn cấu hình `ADMIN_MAX_BODY_BYTES`. | Rút gọn payload trong giới hạn cho phép. |
| **415 Unsupported Media Type** | `unsupported_media_type` | `unsupported media type` | Header `Content-Type` thiếu hoặc khác `application/json`. | Đính kèm `Content-Type: application/json; charset=utf-8`. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` | Supabase Auth không phản hồi, timeout, hoặc kiểm tra hồ sơ sau tạo thất bại. | Kiểm tra trạng thái dịch vụ Supabase Auth/Envoy; đối soát danh sách qua `GET` trước khi thử lại. |
| **500 Internal Server Error** | `internal_error` | `internal server error` | Lỗi máy chủ CSDL hoặc lỗi nội bộ backend chưa phân loại. | Kiểm tra nhật ký hệ thống (log) của backend theo `request_id`. |

### 6.2 Mã Lỗi Nghiệp vụ Quản trị Credential MQTT (MQTT Credential Error Codes)

Theo hiện thực tại `writeCredentialError` (`src/internal/httpserver/admin_mqtt_credential_handlers.go`), đối với mọi lỗi nghiệp vụ miền Credential, thông báo lỗi trong phong bì JSON luôn là `message: "credential request failed"`. Các lỗi phát sinh tại tầng middleware hoặc request parser sẽ mang thông báo lỗi chuẩn riêng biệt:

| Mã lỗi HTTP | Mã lỗi nghiệp vụ (`code`) | Thông báo lỗi (`message`) | Nguyên nhân kích hoạt trong hệ thống | Hướng xử lý phía Client / Administrator |
|---|---|---|---|---|
| **400 Bad Request** | `invalid_request` | `invalid request` *(parser)* hoặc `credential request failed` *(domain)* | Vi phạm cú pháp: Body không rỗng đối với GET/POST/DELETE thông thường, URL chứa query string (`?`), `gateway_id` sai regex, thiếu/sai `Idempotency-Key`, hoặc body reset sai trường `reason`. | Kiểm tra lại request: xóa body (hoặc gửi đúng body reason cho reset), xóa query string, truyền đúng 1 UUID hợp lệ khác nil cho `Idempotency-Key`. |
| **401 Unauthorized** | `unauthorized` | `authentication required` | Thiếu token JWT, token hết hạn, sai chữ ký HS256, hoặc claim `role != "authenticated"`. | Đăng nhập lại qua Supabase Auth để lấy access token mới. |
| **403 Forbidden** | `forbidden` | `insufficient permissions` *(middleware)* hoặc `credential request failed` *(domain)* | Người dùng đã xác thực nhưng `user_id` không tồn tại trong bảng `public.platform_admins`. | Chỉ Platform Admin mới có quyền thực thi. Quyền `owner` Gateway đơn thuần không cấp quyền admin. |
| **404 Not Found** | `not_found` | `credential request failed` | Gateway không tồn tại trong bảng `public.gateways` hoặc chưa từng được cấp phát credential khi gọi GET. | Kiểm tra lại `gateway_id` hoặc gọi API khởi tạo Gateway trước. |
| **409 Conflict** | `credential_conflict` | `credential request failed` | Xung đột trạng thái credential: Cố tình gọi `POST` (provision) khi đang `active`, hoặc gọi `POST /rotate` khi đang `revoked`/chưa cấp phát. | Kiểm tra lại trạng thái hiện tại qua `GET` để gọi đúng endpoint (xoay vòng hay cấp mới). |
| **409 Conflict** | `operation_in_progress` | `credential request failed` | Tái sử dụng `Idempotency-Key` trong khi thao tác trước đó vẫn đang trong trạng thái xử lý (`pending`). | Chờ thao tác trước đó hoàn tất trước khi thử lại với cùng key. |
| **409 Conflict** | `idempotency_conflict` | `credential request failed` | Tái sử dụng `Idempotency-Key` nhưng gửi khác Actor, khác `gateway_id`, hoặc khác Action (`provision` vs `rotate` vs `revoke`). | Mỗi thao tác hoặc tài nguyên khác nhau bắt buộc phải dùng một `Idempotency-Key` độc lập. |
| **413 Request Entity Too Large** | `request_too_large` | `request body too large` | Kích thước body gửi lên vượt quá giới hạn đọc tối đa của server. | Đảm bảo body rỗng (hoặc body reset tối giản) đối với các API Credential. |
| **415 Unsupported Media Type** | `unsupported_media_type` | `unsupported media type` | Header `Content-Type` bị thiếu hoặc khác `application/json` trên endpoint tiếp nhận body (`POST /reset`). | Đính kèm `Content-Type: application/json` khi gửi request có body. |
| **503 Service Unavailable** | `credential_runtime_disabled` | `credential request failed` | Cấu hình server đang tắt API Credential (`MQTT_CREDENTIAL_API_ENABLED=false`). | Cần rollout gate từ kỹ sư hạ tầng; client không thể tự bật qua cờ hay header HTTP. |
| **503 Service Unavailable** | `service_unavailable` | `service unavailable` *(middleware/readiness)* hoặc `credential request failed` *(domain)* | Rào chắn khởi động chưa hoàn tất (`CredentialStartupReadiness.Ready() == false`), lỗi kết nối tra cứu admin CSDL, hoặc gọi vào `POST /reset` bị chặn an toàn bởi `denyOnlyResetPrerequisite{}`. | Tạm dừng thao tác; kiểm tra trạng thái khởi động dịch vụ hoặc tham chiếu runbook phục hồi có giám sát. |
| **503 Service Unavailable** | `credential_runtime_busy` | `credential request failed` | Đang có thao tác credential khác nắm giữ khóa session advisory lock (`0x494f545f43524544`) hoặc runtime đang bận xử lý. Có thể phản ánh runtime bị nghi ngờ/khóa (poisoned state do thao tác trước chưa dứt điểm). | Áp dụng Exponential Backoff có giới hạn; nếu kéo dài cần kiểm tra nhật ký CSDL, không thử lại vô hạn. |
| **503 Service Unavailable** | `credential_verification_unavailable` | `credential request failed` | Không thể kiểm chứng tính hợp lệ của credential mới trên broker Mosquitto. | Kiểm tra tình trạng hoạt động của Mosquitto broker native. |
| **503 Service Unavailable** | `credential_recovery_required` | `credential request failed` | Thao tác rơi vào trạng thái bất định hoặc CSDL ghi nhận trạng thái `recovery_needed` (ví dụ: phát hiện intent chưa hoàn tất khi backend khởi động lại). | Đòi hỏi Platform Admin can thiệp xử lý theo quy trình phục hồi có giám sát. |
| **503 Service Unavailable** | `credential_finalization_pending` | `credential request failed` | Thao tác trên Mosquitto đã thực hiện nhưng giao dịch ghi nhận kết quả cuối cùng trên PostgreSQL chưa hoàn tất. | Kiểm tra lại trạng thái qua `GET` metadata trước khi thực hiện bước tiếp theo. |
| **500 Internal Server Error** | `internal_error` | `credential request failed` | Lỗi nội bộ không xác định trong tiến trình Go backend. | Kiểm tra nhật ký hệ thống (log) của backend theo `request_id`. |

---

## 7. Ảnh hưởng Tác vụ Vận hành tới Kết nối Mạng và Trạng thái Broker / Hệ thống (Runtime & Network Impacts)

### 7.1 Cơ chế Khóa Đồng thời và Trật tự Thực thi (Local Concurrency Control)
- **PostgreSQL Session Advisory Lock (`0x494f545f43524544`):**
  - Mọi thao tác biến đổi credential (`POST`, `POST /rotate`, `DELETE`, `POST /reset`) đều phải chiếm giữ khóa session advisory lock trên CSDL PostgreSQL trước khi thực thi lệnh native trên broker.
  - Cơ chế này đảm bảo tại một thời điểm chỉ có duy nhất một tiến trình backend được phép thực hiện mutation trên broker (single active mutation owner per database).
  - *Lưu ý về ranh giới:* Advisory lock cung cấp cơ chế loại trừ tương hỗ (mutual exclusion) cục bộ trong phạm vi Go backend, **không phải là distributed fencing hay chứng minh tĩnh lặng (quiescence) trên toàn bộ broker**.
- **PostgreSQL Row-level Locks cho User và Gateway Membership:**
  - `PATCH /v1/admin/users/{user_id}`: Khóa dòng `public.profiles` của target bằng `FOR UPDATE`.
  - `PUT /v1/admin/gateways/{id}/users/{id}` & `PUT /v1/admin/gateways/{id}/owner`: Khóa dòng `public.gateways` của target bằng `FOR UPDATE`.
  - Thứ tự khóa nhất quán: Khóa Gateway cha trước khi kiểm tra hoặc thao tác trên dòng con `public.user_gateways` (`FOR UPDATE`), ngăn chặn triệt để nguy cơ deadlock hai chiều với các tiến trình provisioning.

### 7.2 Bảo vệ Khởi động lại Backend (`SimplifiedStartupClassifier`)
- Khi tiến trình Go backend khởi động lại, thành phần `SimplifiedStartupClassifier` sẽ tự động rà quét các thao tác kiểm toán chưa kết thúc (ở các phase `intent`, `dynsec_mutation`, hoặc `snapshot_readback`) trong bảng `gateway_mqtt_credential_events`.
- Các thao tác dở dang này được chuyển trạng thái an toàn sang `recovery_needed`.
- Hệ thống **tuyệt đối KHÔNG tự ý tự động phát lại (no auto-replay)** các mutation chưa hoàn tất khi khởi động lại, nhằm bảo vệ tính toàn vẹn của broker.

### 7.3 Hạn chế Kỹ thuật Tồn đọng và Ranh giới Nghiệm thu (Technical Limitations & Backlog)
Căn cứ tài liệu kỹ thuật chuẩn hóa [`docs/dynsec/limitations-and-backlog.md`](../dynsec/limitations-and-backlog.md):
1. **Kiểm chứng đĩa sau khởi động lạnh (Cold Restart Acceptance - T10 & ROT2):** Kiểm chứng kiểm tra mật khẩu mới đã ghi bền vững xuống tệp cấu hình trên đĩa (`ROT2`) và kiểm chứng khởi động lạnh E2E cho broker Mosquitto hiện vẫn đang ở phạm vi hoãn lại (deferred).
2. **Thời điểm bền bỉ đĩa DynSec (Persistence Timing):** Thời điểm plugin Mosquitto DynSec thực hiện flush/fsync dữ liệu xuống đĩa chưa được đo đạc trong mã nguồn, tiềm ẩn nguy cơ rollback cấu hình nếu broker gặp sự cố mất điện đột ngột trước khi ghi đĩa.
3. **Thu hồi có mục tiêu (Targeted Revocation):** Thao tác `DELETE` ngắt kết nối session active của trạm bị thu hồi và từ chối kết nối mới của trạm đó, trong khi các session của Gateway khác vẫn duy trì kết nối TCP socket.
4. **Mã phản hồi HTTP:** Nhóm endpoint quản trị chỉ phản hồi các mã HTTP 200, 201 hoặc 4xx/5xx; **tuyệt đối không bao giờ trả về mã HTTP `202 Accepted`**.

---

## 8. Khuyến nghị Vận hành và Tích hợp Phía Client (Client Integration Best Practices)

1. **Phân biệt rạch ròi các loại thông tin xác thực:**
   - **Mật khẩu MQTT của Gateway:** Do Mosquitto quản lý, chỉ dùng cho Gateway kết nối MQTT qua TLS.
   - **JWT Supabase của người dùng:** Dùng cho ứng dụng Web/Flutter xác thực với Go Backend (`Authorization: Bearer <jwt>`).
   - **Gateway HTTP Credential / Token:** Dùng cho Gateway khi gọi API lấy Signed URL tải ảnh.
   - **Supabase `service_role` Key:** Khóa bí mật hạ tầng đặc quyền cao nhất của server; tuyệt đối không bao giờ được cấu hình hoặc đưa vào mã nguồn Client.
2. **Xử lý sự cố Timeout khi kết quả không rõ ràng (Handling Timeout / Unknown Outcome):**
   - Khi gọi một mutation (`POST`, `POST /rotate`, `DELETE`) mà gặp lỗi timeout kết nối mạng (Socket Timeout / Network Drop), **tuyệt đối không tự ý sinh một `Idempotency-Key` mới và gửi lại ngay lập tức**. Việc tự ý sinh key mới sẽ gây xung đột trạng thái (`409 Conflict`) hoặc dẫn tới rủi ro tạo ra các bản ghi mồ côi.
   - **Quy trình chuẩn cho Credential:**
     - Bước 1: Giữ nguyên `Idempotency-Key` vừa sử dụng và gọi lại request mutation đó. Nếu server đã xử lý xong trước đó, phản hồi replay sẽ trả về mã `200 OK` hoặc `201 Created` kèm metadata hiện tại (`secret_returned: false`, trường password bị lược bỏ hoàn toàn).
     - Bước 2: Gọi `GET /v1/admin/gateways/{gateway_id}/mqtt-credential` để kiểm tra `credential_version`, `status` và `last_operation_id`.
     - Bước 3: Nếu xác định mật khẩu mới đã kích hoạt thành công trên server (`status == "active"`) nhưng Client bị mất phản hồi (không nhận được mật khẩu), tạo một `Idempotency-Key` mới và thực hiện lệnh `POST /rotate` để cấp mật khẩu mới có kiểm soát. Tuyệt đối không blind-rotate nếu metadata chưa rõ ràng hoặc CSDL còn checkpoint bảo dưỡng chưa hoàn tất.
   - **Quy trình chuẩn cho User Creation (`POST /v1/admin/users`):**
     - Khi tạo user gặp timeout hoặc lỗi 503 từ server, **tuyệt đối không gửi lại ngay lập tức (no blind retry)**.
     - Bước 1: Chờ hệ thống ổn định và gọi `GET /v1/admin/users` để kiểm tra xem email dự kiến tạo đã xuất hiện trong danh sách hay chưa.
     - Bước 2: Nếu tài khoản đã xuất hiện: kiểm tra trạng thái profile có đồng bộ đúng hay không trước khi sử dụng `id`.
     - Bước 3: Lưu ý việc email chưa xuất hiện trong danh sách **không chứng minh** request cũ không thể hoàn tất muộn hơn trên GoTrue. Quản trị viên cần thực hiện quy trình đối soát có giám sát (supervised reconciliation), kiểm tra log hoặc cơ sở dữ liệu nếu nghi ngờ request đang treo trước khi quyết định thử lại thủ công.
     - Server tuyệt đối không tự động xóa tài khoản (no automatic rollback deletion) khi xảy ra lỗi profile verification, bảo toàn nguyên vẹn nhật ký để đối soát có giám sát.
3. **Tuân thủ ranh giới đọc dữ liệu:**
   - Để hiển thị danh mục Gateway và Sensor cho người dùng thông thường trên bảng điều khiển Dashboard, luôn sử dụng các API đọc nghiệp vụ chuẩn đã được kiểm thử phân quyền chặt chẽ tại [Tài liệu 04](04_REST_API_CLIENT_CONTRACT.md) (`GET /v1/gateways`, `GET /v1/gateways/{gateway_id}/sensors`, và các API Digital Twin `GET /v1/digital-twins`, `GET /v1/digital-twins/{entity_id}`, `GET /v1/digital-twins/{entity_id}?format=simplified`, `GET /v1/digital-twins/{entity_id}/temporal`).
   - Tuyến đường `GET /v1/admin/gateways` là danh mục kiểm kê hạ tầng toàn cầu dành riêng cho Platform Admin, không lọc theo tư cách thành viên và không chứa dữ liệu trạng thái hay đo lường.
   - **Ranh giới bất di bất dịch:** Platform Admin sửa đổi hoặc tạo metadata của trạm **KHÔNG tự động có quyền đọc** viễn trắc hay trạng thái trạm đó nếu chưa được gán quyền thành viên trong `public.user_gateways`.
4. **Quy tắc tích hợp Metadata Mutation (POST & PATCH /v1/digital-twins):**
   - **Phân định thẩm quyền:** Chỉ Platform Admin mới có quyền tạo Gateway (`POST /v1/digital-twins` với `entity_type: "Gateway"`, bắt buộc `owner_user_id` đã tồn tại trong `public.profiles`) và sửa Gateway (`PATCH /v1/digital-twins/{entity_id}/attrs`); mọi tài khoản không phải Admin (kể cả Gateway `owner`) khi PATCH Gateway đều nhận `403 Forbidden`. Gateway Owner có quyền tạo Sensor cho trạm mình sở hữu và sửa tên Sensor (`PATCH /v1/digital-twins/{entity_id}/attrs`); vai trò `operator`/`viewer` nhận `403 Forbidden` (sau khi request hợp lệ về cấu trúc); người không có membership trạm nhận `404 Not Found`.
   - **Đơn vị Sensor là bất biến (`unit` is immutable):** Tuyệt đối không gửi trường `unit` trong request PATCH Sensor; vi phạm sẽ nhận `400 Bad Request`.
   - **Xử lý sự cố mạng khi gọi POST/PATCH:** Không đoán định thành công hay thất bại. Việc đọc lại kiểm tra trạng thái (`GET /v1/digital-twins/{entity_id}`) chỉ khả dụng nếu caller có quyền thành viên trạm (không suy đoán mã 404 là lỗi metadata hay thực thể chưa tồn tại). Nếu không có quyền đọc lại, sử dụng công cụ đối soát có giám sát (supervised reconciliation) hoặc gửi lại có giới hạn với cùng thông tin ban đầu, tuyệt đối không tùy tiện ghi đè metadata. Hệ thống hỗ trợ gửi lại trùng khớp (`200 OK`) cho POST và no-op (`200 OK`, giữ nguyên `updated_at`) cho PATCH an toàn.
   - **Tách biệt với xác thực:** Khởi tạo thực thể Digital Twin qua POST không tạo tài khoản Supabase Auth và không cấp mật khẩu MQTT DynSec.
5. **Quy tắc tích hợp Quản trị Người dùng & Quyền Thành viên Gateway:**
   - **Giao diện phân quyền tách biệt:** Phía client tách biệt hoàn toàn hai nút/luồng thao tác: “Gán vai trò operator/viewer” (`PUT /gateways/{id}/users/{id}`) và “Chuyển quyền sở hữu Gateway” (`PUT /gateways/{id}/owner`).
   - **Hiển thị rõ ràng hậu quả chuyển Owner:** Biểu mẫu chuyển owner cần thông báo rõ ràng cho quản trị viên rằng: *“Chủ sở hữu hiện tại sẽ được chuyển thành viewer (tiếp tục có quyền đọc dữ liệu trạm nhưng không thể quản trị cảm biến hay cấu hình trạm)”*.
   - **Cấm tự hạ quyền Admin:** Giao diện cần vô hiệu hóa (disable) hoặc cảnh báo nếu admin đang chỉnh sửa chính tài khoản của mình mà chọn hạ vai trò thành `"user"`, tránh nhận lỗi `409 self_admin_demotion_forbidden`.
   - **Giới hạn danh sách người dùng:** Giao diện gọi `GET /v1/admin/users` cần xử lý trường hợp mã lỗi `409 user_list_capacity_exceeded` để hiển thị thông báo liên hệ quản trị viên nâng ngưỡng cấu hình server, không giả định danh sách luôn có thể tải toàn bộ nếu hệ thống phình to.
6. **Tham chiếu tài liệu hạn chế kỹ thuật:**
   - Chi tiết các giới hạn vận hành, tồn đọng và ma trận nghiệm thu được duy trì chuẩn hóa tại [`docs/dynsec/limitations-and-backlog.md`](../dynsec/limitations-and-backlog.md) và [`docs/dynsec/flows-and-api.md`](../dynsec/flows-and-api.md).
