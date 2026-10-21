# Tài liệu Client — 03: Cơ chế Phân quyền Người dùng và Trạm Gateway (User–Gateway Authorization) trên Flutter Web

**Cập nhật:** 2026-10-20 (Bổ sung đặc tả cơ chế thu hồi thành viên Gateway qua endpoint `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}`; baseline `HEAD 8f9f984` + uncommitted code; migration 22 đã kiểm chứng cục bộ, chưa chạy remote CI và chưa rollout trên backend đang chạy)
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- [`../../AGENTS.md`](../../AGENTS.md) (Mục 6, 6.4, 6.5, 10, 16, 18)
- [`../../../docs/backend_plan/gateway_membership_delete_detailed_plan.md`](../../../docs/backend_plan/gateway_membership_delete_detailed_plan.md) (Kế hoạch bổ sung DELETE Gateway membership)
- [`../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md`](../../../docs/backend_plan/admin_users_and_gateway_membership_detailed_plan.md) (Kế hoạch quản lý User & Gateway Membership gốc)
- [`../backend/admin-users-and-gateway-membership.md`](../backend/admin-users-and-gateway-membership.md) (Báo cáo tổng kết và bàn giao Task U0–U8 — lưu giữ bằng chứng lịch sử)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API chuẩn)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng API Quản trị nền tảng)
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái giao diện)
- `docs/backend/stage-2-task-2.3-authorization.md` (Đặc tả phân quyền Backend Giai đoạn 2)
- `migrations/000021_admin_user_management.up.sql` (Migration quản trị người dùng và thành viên U0–U8)
- `migrations/000022_gateway_membership_revocation.up.sql` (Migration cấp quyền DELETE trên user_gateways cho runtime backend principal)

---

## 1. Nguyên tắc cốt tử: Định danh (Identity) và Phân quyền (Authorization)

Trong các ứng dụng IoT chuyên dụng, sự nhầm lẫn giữa **Xác thực danh tính (Authentication)** và **Ủy quyền truy cập tài nguyên (Authorization)** là một trong những lỗ hổng bảo mật nghiêm trọng nhất.

Ứng dụng Flutter Web Client tuân thủ nghiêm ngặt nguyên tắc bảo mật sau:

> **NGUYÊN TẮC VÀNG:**
> Một JSON Web Token (JWT) hợp lệ từ Supabase Auth chỉ chứng minh danh tính người dùng (`Principal.UserID` tồn tại và phiên đăng nhập hợp lệ), **TUYỆT ĐỐI KHÔNG** chứng minh hay cấp quyền tự động cho người dùng đối với bất kỳ trạm Gateway hay Sensor nào trong hệ thống.

```text
+-------------------------------------------------------------------------------+
|                            Xác thực danh tính (Identity)                      |
|                  Supabase Auth (GoTrue) cấp JWT (HS256)                       |
|           => "Người này là ai?" (Who are you? -> User UUID = sub)             |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                           Phân quyền tài nguyên (Authorization)               |
|            Go Backend thẩm định động qua PostgreSQL (user_gateways)           |
|  => "Người này ĐƯỢC PHÉP LÀM GÌ trên trạm NÀO?" (What can you do on which GW?) |
+-------------------------------------------------------------------------------+
```

### Ranh giới bảo mật phía Client Web:
1. **Không tự suy diễn quyền ở Client Web:** Client không được tự đọc payload của JWT để quyết định mở khóa các chức năng nhạy cảm. Mọi danh sách tài nguyên và quyền thao tác được phép đều bắt nguồn trực tiếp từ dữ liệu phản hồi của Go Backend.
2. **Không truyền `user_id` trong Request Body hoặc URL Query:** Để triệt tiêu nguy cơ tấn công mạo danh tham chiếu đối tượng trực tiếp (IDOR - Insecure Direct Object References), Go Backend **từ chối tức thì (HTTP 400 Bad Request)** mọi request có chứa query param `user_id`. Backend chỉ trích xuất duy nhất `user_id` từ claim `sub` trong JWT đã được xác minh chữ ký mã hóa.
3. **Phân quyền động tại thời điểm truy vấn (Same JWT, Next DB Query, No Live Push):** Quyền của người dùng được kiểm tra trực tiếp trên từng truy vấn SQL tại PostgreSQL. Khi dùng cùng một JWT, nếu quản trị viên cập nhật hoặc thu hồi quyền của người dùng trong cơ sở dữ liệu (`user_gateways` hoặc `platform_admins`), hiệu lực sẽ được áp dụng ngay tại lần truy vấn cơ sở dữ liệu tiếp theo (next DB query) của HTTP request kế tiếp được gửi đến sau khi transaction cơ sở dữ liệu đã commit thành công. Hệ thống không tác động đến các request HTTP hay câu lệnh SQL đang trong quá trình thực thi dở dang tại thời điểm commit, và không sử dụng cơ chế đẩy quyền trực tiếp (no live push / no WebSocket permission push).
4. **Tạo tài khoản không tự động cấp quyền (Account Creation Grants Nothing):** Việc tạo tài khoản người dùng mới trên Supabase Auth (kể cả qua API `POST /v1/admin/users`) chỉ cấp danh tính (JWT hợp lệ), hoàn toàn không gán quyền cho bất kỳ trạm nào. Kết quả gọi `GET /v1/gateways` ban đầu luôn trả về `200 OK` với mảng rỗng `{"items": []}`.
5. **Tách biệt tuyệt đối giữa Quyền Nền tảng (`platform_admins`) và Quyền Trạm (`user_gateways`):**
   - Quyền Platform Admin là quyền quản trị hạ tầng toàn hệ thống (cho phép gọi các API `/v1/admin/*`), được xác thực động trực tiếp từ bảng cơ sở dữ liệu `public.platform_admins` trên từng request (DB-backed), tuyệt đối không suy diễn hay tin cậy từ role claim trong JWT.
   - Quyền này **hoàn toàn tách biệt** với quyền thành viên trạm trong `public.user_gateways`.
   - **Platform Admin KHÔNG BAO GIỜ có quyền bypass đọc dữ liệu viễn trắc/Digital Twin (Never Gains Blanket Read Bypass):** Toàn bộ các API nghiệp vụ đều áp dụng cơ chế lọc theo tư cách thành viên trạm (`user_gateways`). Danh sách trạm (`GET /v1/gateways`) được lọc theo thành viên và trả về `200 OK` với mảng rỗng `{"items": []}` nếu admin không có membership trên trạm nào; các truy vấn chi tiết thực thể hoặc cảm biến không thuộc quyền (`GET /v1/digital-twins/{id}`, `GET /v1/digital-twins/{id}?format=simplified`, `GET /v1/digital-twins/{id}/temporal`, `GET /v1/gateways/{id}/sensors`) đều trả về `404 Not Found` (tuyệt đối không có cơ chế blanket per-Gateway read bypass cho Platform Admin). Kể cả khi Platform Admin chủ động thu hồi quyền `viewer`/`operator` của chính mình trên một trạm, admin đó ngay lập tức mất quyền đọc nghiệp vụ trên trạm đó.
   - **Quản trị viên tự thu hồi quyền (Self-Removal without Global Demotion):** Platform Admin được phép tự xóa quyền `viewer` hoặc `operator` của chính mình trên trạm mà không bị hạ quyền Quản trị nền tảng toàn cục (`platform_admins` giữ nguyên). Tuy nhiên, nếu admin đang là `owner` của trạm, admin vẫn bị chặn với lỗi `409 Conflict` (`owner_transfer_required`) và không có cơ chế tự miễn trừ (no self-owner bypass).
   - Ngược lại, việc là chủ sở hữu (`owner`) của một trạm không cấp bất kỳ quyền hạn nào để gọi các API quản trị `/v1/admin/*` (nhận `403 Forbidden`).

---

## 2. Phân cấp Quan hệ Thực thể (Entity Relationship Hierarchy)

Cơ chế phân quyền dữ liệu của hệ thống được tổ chức theo cấu trúc phân cấp nghiêm ngặt từ trên xuống dưới trong cơ sở dữ liệu PostgreSQL:

```text
+-------------------------------------------------------------------+
|                     auth.users (Supabase Auth)                    |
|                      (Tài khoản người dùng)                       |
+---------------------------------+---------------------------------+
                                  | 1 : 1
                                  v
+-------------------------------------------------------------------+
|                        public.profiles                            |
|                 (Hồ sơ người dùng trong hệ thống)                 |
+---------------------------------+---------------------------------+
                                  | 1 : N
                                  v
+-------------------------------------------------------------------+
|                     public.user_gateways                          |
|             (Bảng quan hệ cấp quyền: user_id + gateway_id)        |
|               CHECK (role IN ('owner', 'operator', 'viewer'))     |
+---------------------------------+---------------------------------+
                                  | N : 1
                                  v
+-------------------------------------------------------------------+
|                        public.gateways                            |
|                         (Trạm Gateway)                            |
+---------------------------------+---------------------------------+
                                  | 1 : N
                                  v
+-------------------------------------------------------------------+
|                        public.sensors                             |
|                    (Các cảm biến thuộc trạm)                      |
+-------------------------------------------------------------------+
```

### Quy tắc kế thừa quyền của Cảm biến (Sensor Authorization):
- **Cảm biến kế thừa toàn bộ quyền từ Gateway cha (`gateway_id`):** Một người dùng chỉ có thể xem danh sách cảm biến (`GET /v1/gateways/{gateway_id}/sensors`) nếu người dùng đó có liên kết hợp lệ trong bảng `user_gateways` với Gateway chứa cảm biến đó.
- **Không có phân quyền cảm biến độc lập:** Hệ thống không hỗ trợ và không cho phép cấp quyền riêng lẻ trên từng Sensor (ví dụ: chỉ cho phép xem cảm biến A nhưng cấm cảm biến B trên cùng một Gateway). Nếu có quyền trên Gateway, người dùng có quyền đọc toàn bộ cảm biến của Gateway đó.
- **Không có route đọc cảm biến đơn lẻ (`GET /v1/gateways/{gateway_id}/sensors/{sensor_id}` không tồn tại):** Backend chỉ cung cấp route đọc tập hợp danh sách cảm biến của trạm `GET /v1/gateways/{gateway_id}/sensors`. Client tuyệt đối không tự bịa ra endpoint đọc chi tiết từng sensor.
- **Phòng thủ chống rò rỉ khi truy cập cảm biến lạ (Foreign Sensor / Foreign Gateway Defense):** Nếu client gửi request đọc danh sách cảm biến của một Gateway mà tài khoản không có quyền trong `user_gateways`, câu truy vấn SQL `JOIN user_gateways ug ... WHERE ug.user_id = $1 AND g.gateway_id = $2` sẽ không trả về dòng dữ liệu nào, và Go Backend trả về HTTP `404 Not Found` (thay vì 403) để bảo vệ hệ thống trước hành vi dò quét mã trạm (anti-enumeration).

---

## 3. Mô hình Phân quyền 3 Vai trò (Three Roles Model) & Thực tế API Giai đoạn 2 (Stage 2)

Trong cơ sở dữ liệu PostgreSQL, ràng buộc kiểm tra schema quy định rõ:
```sql
CHECK (role IN ('owner', 'operator', 'viewer'))
```

### 3.1. Hiện trạng API và Cơ chế Quản lý Thành viên Gateway

> **CẬP NHẬT HIỆN THỰC HỆ THỐNG:**
> 1. **Nhóm API Đọc Nghiệp vụ & Bản sao số Digital Twin (Stage 4 & NGSI-LD Profile):**
>    - `GET /v1/gateways` & `GET /v1/gateways/{gateway_id}/sensors`: Đọc danh sách trạm và cảm biến mà người dùng có quyền thành viên trong `user_gateways`.
>    - `GET /v1/digital-twins`: Danh sách tóm tắt thực thể Digital Twin theo chuẩn NGSI-LD (phân trang keyset cursor).
>    - `GET /v1/digital-twins/{entity_id}`: Chi tiết thực thể JSON-LD chuẩn hóa kèm quan hệ phân cấp `hasSensor`.
>    - `GET /v1/digital-twins/{entity_id}?format=simplified`: Truy vấn trạng thái hiện thời (`reported_state`, `desired_state`, versions) trực tiếp từ `twin_states` không quét hypertable.
>    - `GET /v1/digital-twins/{entity_id}/temporal`: Lịch sử biến thiên chuỗi thời gian cho cảm biến (`attrs=measurement` hoặc `attrs=sample_interval_us`), hỗ trợ cả dạng chuỗi thô (raw tuples) và tổng hợp TimescaleDB `time_bucket` (bucket triples).
>    - Toàn bộ các API đọc trên đều thẩm định tư cách thành viên qua CTE SQL `user_gateways`. Cả ba vai trò `owner`, `operator`, `viewer` đều có quyền đọc các tài nguyên này trên trạm được phân quyền. Người không phải thành viên nhận HTTP `404 Not Found`.
>
> 2. **Ba Cơ chế Phân quyền Quản trị Thành viên Gateway Tập trung (Platform Admin only, DB-backed):**
>    Việc gán thành viên, chuyển chủ sở hữu trạm, và thu hồi tư cách thành viên do Platform Admin thực thi thông qua 3 endpoint chuyên dụng (được thẩm định trực tiếp qua cơ sở dữ liệu `public.platform_admins`, không phụ thuộc JWT role):
>    - **Gán vai trò thông thường (`PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`):**
>      * Chỉ chấp nhận `{"role": "operator"}` hoặc `{"role": "viewer"}`.
>      * Cấm gán `owner` qua endpoint này (trả về `400 Bad Request`, `code: "invalid_role"`).
>      * Cấm hạ quyền của người dùng hiện đang là `owner` qua endpoint này (trả về `409 Conflict`, `code: "owner_transfer_required"`).
>      * Nếu người dùng đã có vai trò tương ứng: Server thực hiện no-op an toàn và bảo toàn thời điểm tạo ban đầu (`created_at`).
>    - **Chuyển giao quyền chủ sở hữu (`PUT /v1/admin/gateways/{gateway_id}/owner`):**
>      * Thực thi nguyên tử trong một transaction: người nhận trở thành `owner`; owner cũ được chuyển thành `viewer`.
>      * **Chính sách bảo toàn quyền đọc của owner cũ:** Owner cũ không bị xóa khỏi trạm mà chuyển thành `viewer`, tiếp tục duy trì quyền đọc dữ liệu giám sát và viễn trắc; các quyền ghi và tạo cảm biến bị thu hồi (`403 Forbidden`).
>      * Hệ thống bắt buộc luôn duy trì **đúng một owner duy nhất** trên mỗi trạm (`gateway_owner_inconsistent` nếu sai lệch).
>      * **Tác động tới Replay Provisioning:** Sau khi chuyển owner, nếu gửi lại request khởi tạo trạm cũ (`PUT /v1/admin/gateways/{id}`) với thông tin owner cũ, server sẽ phản hồi `409 Conflict` (bảo vệ quyền sở hữu hiện tại).
>    - **Thu hồi vai trò thành viên trạm (`DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}`):**
>      * **Thẩm quyền thực thi:** Chỉ tài khoản có tên trong bảng `public.platform_admins` tại thời điểm xử lý request (DB-backed verification) mới có quyền gọi. Caller không phải platform admin (kể cả owner của trạm) nhận ngay `403 Forbidden`.
>      * **Phạm vi thu hồi:** Xóa bỏ tư cách thành viên `operator` hoặc `viewer` của người dùng chỉ định khỏi trạm tương ứng (`DELETE FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2 AND role IN ('operator', 'viewer')`). Khi commit transaction thành công, server phản hồi **HTTP `204 No Content`** với body rỗng hoàn toàn.
>      * **Xử lý thành viên không tồn tại (Idempotent No-Op):** Nếu Gateway tồn tại nhưng target user không có bản ghi membership trong `user_gateways` (bao gồm cả trường hợp target UUID hợp lệ nhưng chưa từng được gán trạm, hoặc tài khoản/profile của target không tồn tại trên hệ thống), server thực hiện no-op an toàn và trả về **`204 No Content`** (trạng thái mong muốn "người dùng không còn membership trên trạm" đã được thỏa mãn).
>      * **Bảo vệ Chủ sở hữu tuyệt đối (Owner Protection — Always 409):** Nếu target user đang giữ vai trò `owner` của trạm, server **LUÔN từ chối với HTTP `409 Conflict`** kèm mã lỗi `owner_transfer_required`; bản ghi membership của owner được bảo toàn nguyên vẹn. Tuyệt đối không cho phép xóa trực tiếp owner, kể cả khi chính Platform Admin tự gọi xóa quyền owner của chính mình (**no self-owner bypass**).
>      * **Quy trình chuẩn khi cần thu hồi quyền owner:** Bắt buộc phải thực hiện quy trình 2 bước: (1) Gọi `PUT /v1/admin/gateways/{gateway_id}/owner` để chuyển giao quyền sở hữu cho người khác (khi đó owner cũ tự động chuyển thành `viewer`); (2) Gọi `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` để xóa vai trò `viewer` của người đó khỏi trạm.
>      * **Xử lý Gateway không tồn tại:** Nếu `gateway_id` không tồn tại trong hệ thống, server trả về **HTTP `404 Not Found`** (`code: "not_found"`).
>      * **Tính cô lập tài nguyên (Resource Isolation):** Thao tác DELETE membership chỉ xóa đúng bản ghi liên kết trên trạm chỉ định. Tuyệt đối **không xóa tài khoản người dùng (`auth.users`)**, không xóa hay sửa đổi hồ sơ cá nhân (`public.profiles`), không ảnh hưởng đến quyền Quản trị nền tảng toàn cục (`public.platform_admins`), và hoàn toàn không ảnh hưởng đến quyền thành viên của người dùng trên bất kỳ Gateway nào khác.
>      * **Quản trị viên tự thu hồi quyền (Self-Removal):** Platform Admin được phép tự xóa quyền `viewer` hoặc `operator` của chính mình trên trạm mà không làm mất hay suy giảm quyền Quản trị nền tảng toàn cục (`platform_admins` giữ nguyên; không bị global demotion). Tuy nhiên, nếu admin đang là owner của trạm, admin vẫn bị chặn với lỗi `409 Conflict` (không có self-owner bypass).
>      * **Ranh giới hiệu lực phân quyền:** Việc thu hồi quyền chỉ ảnh hưởng đến việc thẩm định quyền thành viên ở các request HTTP gửi đến sau khi transaction cơ sở dữ liệu đã commit thành công (`Revocation on Next DB Query`). Không ảnh hưởng đến các request đang xử lý dở dang tại thời điểm commit và không phụ thuộc vào cơ chế realtime ngắt socket tức thời nào chưa có.
>      * **Tuân thủ nguyên tắc không bypass đọc:** Platform Admin sau khi tự thu hồi membership của chính mình (hoặc kể cả khi đang có quyền admin toàn cục) tuyệt đối không có cơ chế blanket per-Gateway read bypass trên các API nghiệp vụ (`GET /v1/gateways`, `GET /v1/gateways/{id}/sensors`, Digital Twin). Các request đọc nghiệp vụ đối với trạm đó sẽ nhận `404 Not Found` hoặc bị loại khỏi danh sách `GET /v1/gateways`.
>
> 3. **Quyền Biến đổi Siêu dữ liệu Cảm biến:**
>    - Vai trò `owner` có quyền tạo cảm biến mới (`PUT /v1/gateways/{id}/sensors/{id}` hoặc `POST /v1/digital-twins`) và sửa tên cảm biến (`PATCH /v1/digital-twins/{entity_id}/attrs`).
>    - Vai trò `operator` và `viewer` hoàn toàn không có quyền tạo hoặc sửa cảm biến (`403 Forbidden`).
>
> 4. **Đối soát hạn chế lịch sử và Tiến độ Hiện thực:**
>    - Tại giai đoạn bàn giao Task U0–U8 trước đây (migration 21), hệ thống ghi nhận hạn chế lịch sử là *"chưa có endpoint API thu hồi membership Gateway, phải sử dụng script vận hành hoặc can thiệp trực tiếp DB"*. Hạn chế lịch sử này nay đã được giải quyết triệt để bằng endpoint chuẩn hóa `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` và migration 22 (`000022_gateway_membership_revocation.up.sql`).
>    - Tính năng này đã được kiểm chứng cục bộ đầy đủ (D1–D4: unit/contract/repo/service/integration HTTP tests PASS) trên baseline `HEAD 8f9f984` + uncommitted code; migration 22 đã áp dụng vào DB test cục bộ. Lưu ý: Tính năng này chưa chạy remote CI và chưa rollout trên môi trường backend đang chạy. Các báo cáo và bằng chứng của Task U0–U8 được bảo lưu nguyên vẹn theo đúng revision lịch sử.

### 3.2. Bảng phân định quyền hạn thực tế và định hướng tương lai

| Vai trò (`role`) | Tên hiển thị (VN) | Quyền hạn dữ liệu thực tế | Kế hoạch Giai đoạn sau (Downlink/Control/WebSocket/Media) | Hiển thị Client (Role Badge) |
|---|---|---|---|---|
| `owner` | **Chủ sở hữu** | Đọc toàn bộ danh sách trạm, cảm biến, chi tiết Digital Twin, trạng thái, và lịch sử đo lường (`measurement`, `sample_interval_us`). Có quyền tạo cảm biến (`PUT /v1/gateways/{id}/sensors/{id}` hoặc `POST /v1/digital-twins`) và sửa tên cảm biến (`PATCH /v1/digital-twins/{id}/attrs`). | Cấu hình trạm (`desired_state`), ra lệnh điều khiển thiết bị an toàn trong server allowlist, nhận WebSocket realtime stream. | `Badge(Color: Amber, Label: "Chủ sở hữu", Icon: admin_panel_settings)` |
| `operator` | **Người vận hành** | Đọc toàn bộ danh sách trạm, cảm biến, chi tiết Digital Twin, trạng thái, và lịch sử đo lường. Không có quyền tạo/sửa cảm biến hoặc sửa trạm (`403 Forbidden`). | Được thực thi các lệnh vận hành an toàn trong server allowlist (ví dụ: `capture-image`). **Không** được sửa `desired_state`, **không** được sửa metadata cảm biến. | `Badge(Color: Blue, Label: "Vận hành", Icon: engineering)` |
| `viewer` | **Người xem** | Hoàn toàn chỉ đọc (Read-only) danh sách trạm, cảm biến, chi tiết Digital Twin, trạng thái, và lịch sử đo lường. Vĩnh viễn không có quyền ghi (`403 Forbidden`). | Tiếp tục duy trì trạng thái chỉ đọc trên telemetry, history, media, digital twin, WebSocket. Không có quyền gửi lệnh điều khiển. | `Badge(Color: Grey/Teal, Label: "Người xem", Icon: visibility)` |

### 3.3. Trải nghiệm phân quyền trên Web Dashboard (Desktop / Tablet / Responsive)
Khác với giao diện màn hình hẹp của thiết bị di động vốn chỉ hiển thị danh sách cuộn dọc đơn giản, nền tảng **Flutter Web Dashboard** sở hữu không gian hiển thị rộng rãi, cho phép tối ưu hóa trải nghiệm giám sát thông qua bố cục linh hoạt:

1. **Hiển thị Bảng Dữ liệu (Data Table) trên Desktop / Màn hình rộng:**
   - Khi độ rộng màn hình $\ge 900\,\text{px}$, danh sách Gateway được trình diễn dưới dạng **Data Table** chuyên nghiệp, gồm các cột thông tin rõ ràng:
     * **Mã trạm (`Gateway ID`):** Mã định danh duy nhất (font monospace, dễ tra cứu).
     * **Tên trạm quan trắc (`Name`):** Tên định danh gợi nhớ của trạm.
     * **Mô tả (`Description`):** Vị trí lắp đặt hoặc phần cứng gateway.
     * **Vai trò (`Role Badge`):** Huy hiệu trực quan với icon và màu sắc đặc trưng của từng vai trò (`owner`, `operator`, `viewer`).
     * **Ngày tạo / gán (`Created At`):** Mốc thời gian theo định dạng thân thiện.
     * **Thao tác (`Actions`):** Nút *"Xem chi tiết"* hoặc icon mở bảng điều khiển trạm.
   - **Tương tác chuột (Hover & Tooltip):** Khi rê chuột (hover) qua Role Badge, một Tooltip xuất hiện tức thì giải thích chi tiết ý nghĩa và quyền hạn của vai trò đó trên trạm (ví dụ: *"Vận hành: Được phép thực hiện các thao tác vận hành an toàn"*).
2. **Hiển thị Lưới thẻ (Responsive Card Grid) trên Tablet / Màn hình trung bình:**
   - Khi độ rộng màn hình trong khoảng $600\,\text{px} - 900\,\text{px}$, giao diện tự động chuyển đổi sang dạng **Lưới thẻ (Grid 2 cột)**. Mỗi thẻ Gateway hiển thị tên, mã trạm, Role Badge nổi bật ở góc trên bên phải, cùng tóm tắt số lượng cảm biến và trạng thái.
3. **Danh sách thẻ (List View) trên Mobile Web:**
   - Khi độ rộng màn hình $< 600\,\text{px}$, hệ thống hiển thị dạng danh sách thẻ đơn 1 cột, đảm bảo đầy đủ thông tin mà không bị vỡ bố cục hay tràn khung (overflow).

### 3.4. Chính sách bảo mật Giai đoạn 2 (Stage 2 Boundary & Fail-closed Policy)
1. **Chế độ Fail-closed:** Trong Giai đoạn 2 hiện tại, Go Backend áp dụng nguyên tắc fail-closed toàn diện đối với mọi thao tác ghi hoặc lệnh điều khiển từ phía người dùng thông thường. Cả `owner`, `operator` và `viewer` đều hoạt động ở chế độ đọc an toàn trên 2 route khả dụng.
2. **UI Hiding KHÔNG PHẢI là cơ chế bảo mật (UI Hiding is Not Security):**
   - Việc ẩn nút, ẩn form, hoặc làm mờ giao diện trên Client Web chỉ là kỹ thuật nâng cao trải nghiệm người dùng (UX) nhằm tránh gây bối rối cho người dùng đối với các tính năng chưa hỗ trợ.
   - **Tuyệt đối không coi UI hiding là ranh giới bảo mật:** Client có thể bị can thiệp bởi DevTools hoặc script độc hại. Go Backend luôn là chốt chặn cuối cùng, kiểm tra quyền động trong cơ sở dữ liệu trên từng request và từ chối ngay lập tức nếu request không hợp lệ.
   - Mọi đoạn mã giao diện mẫu (Dart/Flutter) trong tài liệu này là các đoạn mã minh họa kiến trúc (illustrative snippets).

---

## 4. Phân định rạch ròi: Quyền Quản trị Nền tảng (Platform Admin) vs. Quyền Thành viên Trạm (Gateway Roles)

Một trong những nhầm lẫn phổ biến là đồng nhất quyền Quản trị viên hệ thống với quyền Chủ sở hữu trạm. Kiến trúc hệ thống phân định dứt khoát hai cơ chế phân quyền độc lập:

```text
+----------------------------------------------------+----------------------------------------------------+
|        Platform Administrator (Toàn cục)           |              Gateway Role (Từng trạm)              |
+----------------------------------------------------+----------------------------------------------------+
| - Xác định qua bảng PostgreSQL `platform_admins`   | - Xác định qua bảng PostgreSQL `user_gateways`     |
| - Thẩm định bởi `auth.PlatformAdminMiddleware`     | - Thẩm định qua SQL JOIN động tại từng truy vấn    |
| - Quyền quản trị toàn hệ thống (Gateway/Sensors)   | - Gắn liền duy nhất với một `gateway_id` cụ thể    |
| - Cấp phát & quay vòng MQTT credentials cho trạm   | - Là một trong 3 vai trò: `owner`, `operator`,     |
| - Gọi các HTTP Admin Routes thực tế (`/v1/admin/*`)|   hoặc `viewer`                                    |
| - Thao tác qua Supabase Studio hoặc Admin Tooling  | - Thao tác qua ứng dụng Flutter Web Client         |
+----------------------------------------------------+----------------------------------------------------+
```

### 4.1. Các HTTP Route Quản trị Thực tế Hiện Có (Real HTTP Admin Routes)
Khác với các tài nguyên nghiệp vụ thông thường, Go Backend đã triển khai và đăng ký 7 endpoint quản trị khả dụng cùng 1 tuyến đường reset có rào chắn an toàn dưới tiền tố `/v1/admin` được bảo vệ độc quyền bởi `PlatformAdminMiddleware`:

1. **Danh mục Kiểm kê Gateway Toàn hệ thống (Admin Gateway Inventory):**
   `GET /v1/admin/gateways`
   Endpoint này cho phép Platform Admin truy vấn toàn bộ Gateway trên hệ thống hoàn toàn độc lập với tư cách thành viên `user_gateways`, hỗ trợ phân trang keyset cursor (`limit`, `after`). Không bao giờ lộ bí mật MQTT hay thông tin quyền thành viên. Non-admin luôn nhận mã lỗi `403 Forbidden` (xem chi tiết tại [`07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md)).
2. **Khởi tạo hoặc Replay Đồng nhất Trạm Gateway (Create or Identical Replay):**
   `PUT /v1/admin/gateways/{gateway_id}`
   Endpoint này áp dụng cơ chế idempotent create-or-identical-replay, **không phải là API cập nhật (update) Gateway đã có**:
   - **Tạo mới trạm (HTTP `201 Created`):** Khi mã `gateway_id` chưa tồn tại trong cơ sở dữ liệu, backend tạo mới Gateway, khởi tạo Digital Twin entity tương ứng, và gán tài khoản chủ sở hữu ban đầu vào `user_gateways` với vai trò `owner` theo trường `owner_user_id` trong body.
   - **Replay đồng nhất (HTTP `200 OK`):** Khi `gateway_id` đã tồn tại và payload gửi lên hoàn toàn trùng khớp với biểu diễn hiện hữu (cùng `name`, `description`, và cùng `owner_user_id`), backend ghi nhận đây là lượt gọi lặp lại an toàn (no-op identical replay) và trả về `200 OK`.
   - **Xung đột biểu diễn (HTTP `409 Conflict`):** Khi `gateway_id` đã tồn tại nhưng payload có bất kỳ thông tin nào sai khác với dữ liệu hiện có (ví dụ khác tên, khác mô tả hoặc khác chủ sở hữu), backend từ chối với mã lỗi `409 Conflict` (`{"code": "conflict", "message": "resource conflict"}`).
3. **Khởi tạo hoặc Replay Đồng nhất Cảm biến trực thuộc Trạm (Create or Identical Replay):**
   `PUT /v1/admin/gateways/{gateway_id}/sensors/{sensor_id}`
   Tương tự như trạm, endpoint này thực hiện khởi tạo hoặc replay đồng nhất cho cảm biến bởi Platform Admin độc lập với quyền sở hữu:
   - **Tạo mới cảm biến (HTTP `201 Created`):** Khi `sensor_id` chưa tồn tại dưới Gateway cha, backend tạo mới bản ghi sensor và Digital Twin entity.
   - **Replay đồng nhất (HTTP `200 OK`):** Nếu `sensor_id` đã tồn tại với các trường thông tin hoàn toàn trùng khớp (`name`, `unit`), backend trả về `200 OK`.
   - **Xung đột biểu diễn (HTTP `409 Conflict`):** Nếu `sensor_id` đã tồn tại nhưng payload chứa dữ liệu khác biệt so với hiện trạng, backend trả về `409 Conflict`.
4. **Bộ 4 Endpoint Quản lý Thông tin Xác thực MQTT (MQTT Credentials):**
   - `GET /v1/admin/gateways/{gateway_id}/mqtt-credential`: Truy vấn metadata thông tin xác thực MQTT của trạm (không bao giờ lộ mật khẩu thô).
   - `POST /v1/admin/gateways/{gateway_id}/mqtt-credential`: Cấp phát ban đầu thông tin xác thực MQTT cho trạm (`201 Created` trả password một lần duy nhất).
   - `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/rotate`: Xoay vòng mật khẩu MQTT mới (`200 OK` trả password mới một lần duy nhất).
   - `DELETE /v1/admin/gateways/{gateway_id}/mqtt-credential`: Vô hiệu hóa và thu hồi thông tin xác thực MQTT của trạm (`200 OK` metadata-only; ngắt socket kết nối, không có cơ chế khôi phục secret tự động nhưng cho phép cấp mới lại với key mới).
5. **Ngoại lệ Tuyến đường Đặt lại Credential Có giám sát (Registered Fail-Closed Reset Route):**
   - `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/reset`: Tuyến đường đã được đăng ký và kiểm tra cú pháp payload lý do (`reason`), nhưng luôn fail-closed với HTTP `503 Service Unavailable` (`service_unavailable` khi API bật, hoặc `credential_runtime_disabled` khi API tắt). Chưa phải quy trình phục hồi khả dụng.
6. **Bộ Endpoint Quản trị Người dùng và Thành viên Gateway (User & Gateway Membership Management):**
   - `GET /v1/admin/users`: Truy vấn danh sách người dùng toàn hệ thống kèm thông tin hồ sơ và mảng `gateway_roles: [{"gateway_id": "...", "role": "..."}]`.
   - `POST /v1/admin/users`: Tạo tài khoản người dùng mới trên Supabase Auth và khởi tạo hồ sơ `public.profiles`. Chỉ cấp danh tính (JWT), hoàn toàn không tự động gán quyền trạm nào.
   - `PATCH /v1/admin/users/{user_id}`: Cập nhật nguyên tử `full_name` và/hoặc `platform_role`; cấm admin tự hạ quyền toàn cục của chính mình. Không đồng nhất thao tác này với thu hồi membership.
   - `PUT /v1/admin/gateways/{gateway_id}/users/{user_id}`: Gán hoặc cập nhật vai trò thành viên thông thường (`operator`, `viewer`) cho người dùng trên trạm. Cấm gán `owner` (`400 Bad Request`) và cấm hạ quyền trực tiếp của `owner` (`409 Conflict`).
   - `PUT /v1/admin/gateways/{gateway_id}/owner`: Chuyển giao quyền sở hữu trạm cho người dùng mới; người nhận trở thành `owner`, đồng thời owner cũ được tự động chuyển thành `viewer` trong cùng một transaction.
   - `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}`: Thu hồi vai trò thành viên `operator` hoặc `viewer` của người dùng khỏi trạm (`204 No Content` với body rỗng hoàn toàn). Idempotent no-op (`204 No Content`) nếu membership không tồn tại hoặc target UUID hợp lệ không tồn tại; từ chối `409 Conflict` (`owner_transfer_required`) nếu target user đang là `owner`; trả về `404 Not Found` nếu Gateway không tồn tại.

*(Chi tiết đầy đủ về schema, headers và mã lỗi của các endpoint quản trị khả dụng và endpoint reset được quy định tại [`docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`](07_ADMIN_API_CLIENT_CONTRACT.md))*.

### 4.2. Các Nguyên tắc Phân quyền Cốt tử giữa Admin và User

1. **Phân định Vai trò Thành viên Trạm và Quyền Quản trị Độc lập (Membership Roles Do Not Deny Independent Admin Status):**
   - Ràng buộc phân quyền trạm (`user_gateways`) và quyền quản trị nền tảng (`platform_admins`) là hai tập thẩm quyền độc lập. Việc nắm giữ một vai trò thành viên trạm (kể cả `owner`) **không tự động cấp quyền quản trị**, nhưng cũng **không tước bỏ quyền quản trị độc lập** nếu người dùng đó đồng thời được cấp quyền trong `platform_admins` (dual-role user).
   - **Trường hợp non-admin owner:** Nếu một người dùng nắm vai trò `owner` của một Gateway nhưng tài khoản không có tên trong bảng `platform_admins`, khi cố tình gửi request đến các route quản trị `/v1/admin/*`, `PlatformAdminMiddleware` sẽ tra cứu cơ sở dữ liệu và từ chối tức thì với mã lỗi **HTTP `403 Forbidden`** (`{"code": "forbidden", "message": "insufficient permissions"}`).
   - **Trường hợp dual-role:** Một người dùng vừa là Platform Admin vừa là `owner` của một trạm có thể thực thi các lệnh `/v1/admin/*` nhờ tư cách admin. Tuy nhiên, khi gọi các API đọc thông thường của người dùng (`GET /v1/gateways`), tài khoản đó vẫn tuân thủ nguyên tắc không bỏ qua phân quyền đọc (No Admin Read Bypass).
   - **Admin tự xóa quyền thành viên (Self-Removal without Global Demotion):** Platform Admin có thể tự xóa quyền `viewer` hoặc `operator` của chính mình trên trạm qua `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` mà không bị tước hay suy giảm quyền quản trị toàn cục (`platform_admins` giữ nguyên). Tuy nhiên, nếu admin đang là `owner` của trạm, admin vẫn bị chặn với lỗi `409 Conflict` (không có self-owner bypass).
2. **KHÔNG CÓ cơ chế Admin Read Bypass (No Admin Read Bypass):**
   - Khi một Platform Admin đăng nhập vào ứng dụng Web Client và gọi các API đọc thông thường của người dùng (`GET /v1/gateways` hoặc `GET /v1/gateways/{gateway_id}/sensors`), Go Backend **tuyệt đối không** tự động bỏ qua điều kiện lọc quyền (never gains blanket per-Gateway read bypass).
   - Câu truy vấn SQL của Backend vẫn lọc nghiêm ngặt theo `WHERE ug.user_id = $1`. Nếu Platform Admin không được gán quyền trực tiếp trong bảng `user_gateways` cho một trạm, Admin cũng chỉ nhận về danh sách rỗng `{"items": []}` hoặc lỗi `404 Not Found`.
   - **Mục đích kiến trúc:** Ngăn ngừa việc vô tình làm rò rỉ dữ liệu hoặc làm tràn ngập màn hình giám sát của Admin bằng hàng trăm trạm không liên quan đến phạm vi kiểm thử hay giám sát trực tiếp của tài khoản đó.
3. **Cơ chế Quản trị Thành viên Chính thức qua HTTP Admin API và Bảo vệ Chủ sở hữu (Protected Membership API with Owner Protection, No Public API):**
   - **Tiến hóa từ Script sang API chính thức:** Trước đây tại giai đoạn bàn giao Task U0–U8, hệ thống hỗ trợ gán quyền (`PUT .../users/{user_id}`) và chuyển owner (`PUT .../owner`), nhưng việc thu hồi thành viên chưa có endpoint API mà phải thông qua script vận hành quản trị viên `scripts/manage-gateway-membership.sh` hoặc can thiệp trực tiếp DB (hạn chế lịch sử U0–U8). Hiện tại, trên baseline `HEAD 8f9f984` + uncommitted code cùng migration 22 (`000022_gateway_membership_revocation.up.sql`) đã kiểm chứng cục bộ (chưa rollout remote CI / backend đang chạy), hệ thống đã chính thức bổ sung endpoint `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` dành riêng cho Platform Admin để thu hồi thành viên một cách chuẩn hóa, an toàn và có kiểm toán.
   - **Cơ chế bảo vệ chủ sở hữu (Owner Protection):** Cả hai thao tác PUT và DELETE đều ngăn chặn tuyệt đối việc xóa hoặc giáng cấp trực tiếp vai trò `owner` (`409 Conflict`, `code: "owner_transfer_required"`). Khi cần thay đổi chủ sở hữu, bắt buộc phải sử dụng API chuyển giao quyền sở hữu `PUT /v1/admin/gateways/{gateway_id}/owner` (owner cũ tự động chuyển thành `viewer`), sau đó mới có thể thu hồi quyền `viewer` qua `DELETE`.
   - **Không cung cấp Public API:** Các endpoint quản lý thành viên chỉ nằm dưới tiền tố `/v1/admin/*`, yêu cầu thẩm định quyền Platform Admin trực tiếp từ CSDL (`public.platform_admins`). Hệ thống **tuyệt đối không cung cấp public API** cho người dùng tự gán trạm (no self-claiming), tự mời người khác (no invitation API) hay tự nâng quyền.
4. **Tạo Tài khoản Mới KHÔNG CẤP BẤT KỲ QUYỀN NÀO (Account Creation Grants Nothing):**
   - Việc tạo tài khoản trên Supabase Auth chỉ cung cấp định danh hợp lệ (JWT). Người dùng mới tạo chưa được gán bất kỳ trạm nào trong `user_gateways`.
   - Khi gọi `GET /v1/gateways`, Backend trả về `200 OK` với mảng rỗng `{"items": []}`.
5. **Cập nhật Quyền trên Cùng một JWT (Same JWT, Next DB Query, No Live Push):**
   - Khi Quản trị viên cập nhật hoặc thu hồi quyền của người dùng trong `user_gateways`, người dùng tiếp tục sử dụng JWT hiện tại mà không cần đăng nhập lại.
   - Quyền mới sẽ có hiệu lực ngay ở lần truy vấn cơ sở dữ liệu tiếp theo (**next DB query**) khi client gửi HTTP request. Hệ thống không sử dụng cơ chế đẩy quyền trực tiếp qua WebSocket hay live push.
6. **Tuyệt đối KHÔNG SÁNG TÁC các Route không tồn tại (No Invented Routes):**
   - **KHÔNG CÓ** route đọc chi tiết 1 Gateway: `GET /v1/gateways/{gateway_id}` (không tồn tại).
   - **KHÔNG CÓ** route đọc chi tiết 1 Sensor: `GET /v1/gateways/{gateway_id}/sensors/{sensor_id}` (không tồn tại).
   - **KHÔNG CÓ** route đọc thông tin Admin bản thân: `GET /v1/admin/self` (không tồn tại).
   - Mọi thông tin Gateway được lấy qua `GET /v1/gateways`, và danh sách cảm biến được lấy qua `GET /v1/gateways/{gateway_id}/sensors`.

---

## 5. Bảng Mã Trạng thái HTTP Thực tế (Actual HTTP Status Codes) & Ứng xử Giao diện Client

Bảng dưới đây quy định chính xác các mã trạng thái HTTP thực tế được sinh ra từ Go Backend và hành vi ứng xử tương ứng của ứng dụng Flutter Web Client:

| Mã HTTP | Mã lỗi (`code`) | Thông báo (`message`) | Nguyên nhân phát sinh thực tế tại Backend | Ứng xử chuẩn của Client Web |
|---|---|---|---|---|
| `200 OK` | *(không có)* | *(payload JSON)* | Gọi `GET /v1/gateways` hoặc `GET /v1/gateways/{gateway_id}/sensors` thành công. Khi `items: []`, đây là trạng thái rỗng hợp lệ (Empty State). | Hiển thị danh sách dữ liệu hoặc giao diện rỗng thân thiện (`EmptyGatewayView`). Không hiển thị lỗi. |
| `204 No Content` | *(không có)* | *(response rỗng)* | Gọi `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` thành công: xóa bỏ thành công vai trò `viewer`/`operator`, hoặc membership không tồn tại / target UUID hợp lệ không có liên kết trên trạm (idempotent no-op). | Ghi nhận thao tác thành công, đóng modal/dialog xác nhận, làm mới danh sách thành viên/trạm. Không phân tích body JSON; không yêu cầu header `Content-Length: 0` phải hiện diện. |
| `400 Bad Request` | `invalid_request` | `invalid request` | Request chứa query param bị cấm `?user_id=...` (chống IDOR), `gateway_id` sai định dạng kiểm tra, hoặc request DELETE chứa body/query không rỗng. | Bắt lỗi an toàn; loại bỏ query param cấm; hiển thị cảnh báo tham số không hợp lệ. |
| `401 Unauthorized` | `unauthorized` | `authentication required` | Thiếu header `Authorization`, token hết hạn, sai chữ ký JWT hoặc token không đúng định dạng. | Xóa session token trong bộ nhớ, điều hướng người dùng về màn hình đăng nhập (`/login`). |
| `403 Forbidden` | `forbidden` | `insufficient permissions` hoặc `access denied` | Tài khoản không có trong `platform_admins` cố tình gọi các route quản trị `/v1/admin/*` (kể cả owner của Gateway khi gọi DELETE membership), hoặc vi phạm quyền provisioning. | Hiển thị thông báo không có thẩm quyền quản trị; chặn hoàn toàn quyền truy cập giao diện admin. |
| `404 Not Found` | `not_found` | `resource not found` | Người dùng gọi `GET /v1/gateways/{gateway_id}/sensors` đối với trạm không thuộc quyền trong `user_gateways` (foreign gateway defense), trạm không tồn tại, route sai, hoặc gọi `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` với `gateway_id` không tồn tại. | Giữ nguyên URL trên thanh địa chỉ, hiển thị trang lỗi `GatewayNotFoundView` kèm nút "Quay lại danh sách trạm" (hoặc báo trạm không tồn tại trong luồng admin). |
| `409 Conflict` | `owner_transfer_required` hoặc `conflict` | `owner transfer required` hoặc `resource conflict` | Cố tình gọi `DELETE /v1/admin/gateways/{gateway_id}/users/{user_id}` khi target user đang là `owner` của trạm (bắt buộc chuyển owner trước); cố hạ quyền owner qua PUT; hoặc xung đột cấu hình replay provisioning. | Bắt mã lỗi 409; hiển thị thông báo yêu cầu chuyển giao quyền chủ sở hữu trạm trước khi xóa hoặc thay đổi vai trò; không cho phép bypass. |
| `501 Not Implemented` | `not_implemented` | `endpoint not implemented` | Client gọi các endpoint stub Giai đoạn 2 (`/v1/telemetry/history`, `/v1/ws`, `/v1/digital-twins`). | Bắt lỗi an toàn; thông báo chức năng đang trong lộ trình phát triển của các giai đoạn sau. |
| `503 Service Unavailable` | `service_unavailable` | `service unavailable` | Kết nối PostgreSQL lỗi, timeout hoặc context cancelled khi kiểm tra quyền/admin, hoặc credential controller chưa sẵn sàng. | Hiển thị thông báo hệ thống tạm thời gián đoạn kèm nút "Thử lại sau" (Retry với exponential backoff). |

*(Tham khảo thêm chi tiết về cơ chế quản lý trạng thái giao diện và bắt lỗi mạng tại `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md`)*.

### 5.1. Hành vi UI đối với Tài khoản Mới (Empty State) trên Web
Theo chính sách quản lý tài khoản tập trung (`AGENTS.md` Mục 6.5 và `02_AUTHENTICATION_AND_SESSION.md`), người dùng không thể tự đăng ký tài khoản. Tài khoản do Quản trị viên tạo sẵn.

#### Kịch bản Người dùng chưa được gán Gateway:
1. Quản trị viên tạo tài khoản cho người dùng nhưng chưa kịp thêm bản ghi vào `user_gateways`.
2. Người dùng đăng nhập thành công vào Web Dashboard (nhận JWT hợp lệ).
3. Flutter Web gọi `GET /v1/gateways`.
4. Go Backend truy vấn cơ sở dữ liệu, không thấy bản ghi nào trong `user_gateways`, trả về:
   - **Mã HTTP:** `200 OK`
   - **Payload:**
     ```json
     {
       "items": []
     }
     ```

#### Hành vi giao diện Web Client (Empty State UX):
- **Không coi đây là lỗi:** Đây là trạng thái nghiệp vụ hoàn toàn bình thường, không hiển thị thông báo lỗi (Error Toast / Alert Dialog).
- **Hiển thị giao diện rỗng thân thiện (Empty State Widget) dạng Card nổi giữa màn hình Web:**
  - Icon minh họa trung tâm: Trạm quan trắc chưa kết nối (`Icons.router_outlined` hoặc `Icons.sensors_off_outlined`).
  - Tiêu đề: *"Chưa có trạm Gateway nào"*
  - Nội dung hướng dẫn: *"Tài khoản của bạn hiện chưa được cấp quyền truy cập trạm quan trắc nào trong hệ thống. Vui lòng liên hệ Quản trị viên (Platform Administrator) để được phân quyền."*
  - Nút hành động: Nút *"Làm mới danh sách"* (`Refresh`) để tải lại danh sách sau khi Quản trị viên hoàn tất việc gán quyền, và nút *"Đăng xuất"* nếu người dùng đăng nhập nhầm tài khoản.

---

## 6. Xử lý Deep Linking, URL Manipulation, F5 Reload & Thu hồi Quyền trên Web

Trên nền tảng Web, hành vi người dùng rất khác biệt so với ứng dụng di động: người dùng có thanh địa chỉ URL (Address Bar), phím F5 / Ctrl+R để tải lại trang, và các nút Back/Forward của trình duyệt. Ứng dụng Web phải xử lý các tình huống này một cách an toàn và nhất quán.

### 6.1. Xử lý Deep Linking và URL Manipulation trên trình duyệt Web
Người dùng có thể gõ trực tiếp một đường dẫn chi tiết trạm vào thanh địa chỉ của trình duyệt, ví dụ:
```text
https://iot.example.com/gateways/gw_999
```

**Kịch bản Người dùng cố tình truy cập Gateway KHÔNG có quyền:**
1. Giả sử người dùng chỉ được cấp quyền xem `gw_001`, nhưng tự ý gõ trên trình duyệt URL `.../gateways/gw_999`.
2. Web Client trích xuất `gatewayId = gw_999` từ path parameter và gửi request gọi dữ liệu cảm biến lên Go Backend:
   ```http
   GET /v1/gateways/gw_999/sensors HTTP/1.1
   Host: iot.example.com
   Authorization: Bearer <valid_jwt_of_user>
   Accept: application/json
   ```
3. Go Backend thực thi câu truy vấn SQL scoped bảo mật:
   ```sql
   SELECT s.sensor_id, s.name, s.unit, s.created_at
   FROM sensors s
   JOIN user_gateways ug ON ug.gateway_id = s.gateway_id
   WHERE ug.user_id = $1 AND ug.gateway_id = $2
   ```
4. Do `ug.user_id` không có liên kết với `gw_999` trong bảng `user_gateways`, Backend trả về:
   - **Mã HTTP:** `404 Not Found`
   - **Payload lỗi:**
     ```json
     {
       "code": "not_found",
       "message": "resource not found"
     }
     ```

#### Cơ chế Phòng thủ Chống dò quét tài nguyên (Anti-Enumeration Defense):
- **Tại sao trả 404 Not Found thay vì 403 Forbidden?**
  Đây là quy tắc bảo mật chuẩn (theo RFC 7231 và khuyến nghị OWASP API Security). Việc trả về `404 Not Found` ngăn chặn kẻ tấn công dò quét (enumerate) xem mã trạm `gw_999` có thực sự tồn tại trong cơ sở dữ liệu hay không. Đối với một người dùng không có quyền truy cập, tài nguyên đó được đối xử như **hoàn toàn không tồn tại**.

#### Ứng xử giao diện Web Client khi gặp lỗi 404:
- **Giữ nguyên URL trên thanh địa chỉ:** Không vội vàng chuyển hướng (redirect) giật cục làm mất dấu vết địa chỉ người dùng vừa gõ.
- **Hiển thị trang lỗi 404 chuyên biệt (Gateway Not Found View):**
  * Icon cảnh báo: `Icons.search_off_outlined` hoặc `Icons.lock_person_outlined`.
  * Tiêu đề: *"404 — Không tìm thấy trạm quan trắc"*
  * Nội dung: *"Trạm quan trắc `gw_999` không tồn tại hoặc tài khoản của bạn chưa được cấp quyền truy cập."*
  * Nút hành động chính: *"Quay lại danh sách trạm"* — khi click, router chuyển hướng về `/gateways` (`context.go('/gateways')`).

```text
[Người dùng gõ: /gateways/gw_999]
                |
                v
       Client Web gửi API:
   GET /v1/gateways/gw_999/sensors
                |
                v
   Go Backend kiểm tra user_gateways -> Không có quyền!
                |
                v
       Trả về HTTP 404 Not Found
                |
                v
       Client bắt mã lỗi 404:
  +-----------------------------------------------+
  |  Trang lỗi 404 trên Web (/gateways/gw_999)     |
  |  "404 - Không tìm thấy trạm hoặc bạn          |
  |   không có quyền truy cập."                   |
  |                                               |
  |     [ Quay lại danh sách trạm (/gateways) ]   |
  +-----------------------------------------------+
```

---

### 6.2. Xử lý sự kiện Tải lại trang (F5 / Reload) tại Trang Chi tiết
Khi người dùng đang mở trang chi tiết một trạm (`https://iot.example.com/gateways/gw_001`) và nhấn **F5** hoặc **Ctrl+R**:
1. Toàn bộ ứng dụng Flutter Web được nạp lại từ đầu. Bộ nhớ RAM của ứng dụng bị giải phóng, các biến state trong Cubit/Controller bị xóa sạch.
2. `go_router` đọc URL từ trình duyệt và phân giải route `/gateways/:id`, trích xuất `gatewayId = "gw_001"`.
3. **Đặc thù kiến trúc API Backend:**
   Trong thiết kế Giai đoạn 2 của Go Backend, hệ thống **không cung cấp** endpoint riêng lẻ `GET /v1/gateways/{id}` mà chỉ cung cấp 2 endpoint:
   - `GET /v1/gateways`: Trả về danh sách tất cả các trạm mà user hiện tại có quyền (chứa metadata: `name`, `description`, `role`, `created_at`).
   - `GET /v1/gateways/{id}/sensors`: Trả về danh sách các cảm biến trực thuộc trạm đó.

#### Chiến lược nạp dữ liệu khôi phục trạng thái (Hydration Strategy khi F5):
Để tái tạo đầy đủ giao diện chi tiết trạm (bao gồm cả Tên trạm, Role Badge và Danh sách cảm biến), Web Client thực hiện nạp dữ liệu song song hoặc phối hợp:
1. **Bước 1 — Nạp Metadata và Xác thực Quyền qua `GET /v1/gateways`:**
   - Client gọi `GET /v1/gateways`.
   - Tìm kiếm bản ghi có `gateway_id == state.pathParameters['id']` trong mảng `items`.
   - **Nếu KHÔNG tìm thấy:** Chứng tỏ trạm này không tồn tại hoặc tài khoản không có quyền -> Chuyển sang hiển thị **Trang lỗi 404**.
   - **Nếu tìm thấy:** Lưu giữ thông tin trạm và vai trò (`role`) của người dùng để gắn Role Badge lên tiêu đề.
2. **Bước 2 — Nạp Danh sách Cảm biến qua `GET /v1/gateways/{id}/sensors`:**
   - Client gọi `GET /v1/gateways/{id}/sensors`.
   - Nếu trả về `200 OK`: Hiển thị danh sách cảm biến trong bảng dữ liệu hoặc lưới thẻ.
   - Nếu trả về `404 Not Found`: Chuyển sang hiển thị **Trang lỗi 404**.

```text
[Người dùng F5 tại /gateways/gw_001]
                 |
                 v
   go_router trích xuất: gatewayId = gw_001
                 |
                 +---------------------------------------+
                 | Gọi song song 2 API:                  |
                 | 1. GET /v1/gateways                   |
                 | 2. GET /v1/gateways/gw_001/sensors    |
                 +-------------------+-------------------+
                                     |
                    +----------------+----------------+
                    |                                 |
            gw_001 có trong                   gw_001 KHÔNG có
           GET /v1/gateways?                 trong danh sách?
                    |                                 |
                  (Có)                             (Không)
                    v                                 v
        Nạp thành công metadata:             Hiển thị Trang lỗi 404:
        Tên trạm + Role Badge + Sensors      "Không tìm thấy trạm hoặc
        (Màn hình hiển thị đầy đủ)            bạn không có quyền truy cập"
```

---

### 6.3. Xử lý Thu hồi Quyền khi đang mở trang chi tiết (Revocation on Next DB Query)
Nếu người dùng đang mở xem chi tiết trạm `gw_001`, và Quản trị viên (Platform Admin) thực hiện thu hồi tư cách thành viên của người dùng đó thông qua endpoint `DELETE /v1/admin/gateways/gw_001/users/{user_id}` (hoặc thao tác quản trị cơ sở dữ liệu):
1. **Ranh giới hiệu lực phân quyền (Commit-then-Verify, No In-Flight Disruption, No Realtime Push):**
   - **Chỉ áp dụng cho request sau commit:** Lệnh DELETE xóa bỏ vai trò `operator` hoặc `viewer` trong bảng `public.user_gateways`. Hiệu lực thu hồi quyền chỉ ảnh hưởng đến việc thẩm định quyền thành viên ở các request HTTP gửi đến sau khi transaction cơ sở dữ liệu đã commit thành công (`Revocation on Next DB Query`).
   - **Không hủy request đang chạy dở dang:** Thao tác thu hồi không can thiệp hay hủy bỏ các request HTTP hoặc câu lệnh SQL đang trong quá trình thực thi tại thời điểm transaction commit.
   - **Không dựa vào cơ chế realtime chưa có:** Hệ thống không triển khai cơ chế đẩy sự kiện thời gian thực (no WebSocket push / no SSE permission push) để ngắt socket hay đóng phiên của client ngay lập tức.
2. **Hành vi tại truy vấn kế tiếp (Next DB Query):**
   - Ở ngay thao tác tiếp theo của client (ví dụ: người dùng bấm nút làm mới danh sách cảm biến, chuyển tab, hoặc bộ đếm thời gian tự động đồng bộ gửi request):
     * Client gửi request nghiệp vụ (ví dụ `GET /v1/gateways/gw_001/sensors` hoặc `GET /v1/digital-twins/gw_001`) kèm cùng một JWT hiện tại.
     * Go Backend thực thi câu truy vấn SQL có gắn điều kiện lọc `user_gateways`. Do bản ghi membership đã bị xóa, câu truy vấn không trả về dòng dữ liệu nào.
     * Backend phản hồi ngay **HTTP `404 Not Found`** (`{"code": "not_found", "message": "resource not found"}`) theo cơ chế phòng thủ chống dò quét tài nguyên (anti-enumeration defense).
   - Nếu gọi `GET /v1/gateways`: Trạm `gw_001` tự động biến mất khỏi danh sách `items: [...]`. Nếu người dùng không còn membership trên trạm nào khác, danh sách trả về `items: []` (Empty State).
3. **Hiệu ứng đối với Quản trị viên tự thu hồi quyền (No Admin Read Bypass):**
   - Nếu chính Platform Admin gọi `DELETE` để tự xóa vai trò `viewer` hoặc `operator` của bản thân trên trạm `gw_001`, tài khoản admin vẫn giữ nguyên quyền quản trị nền tảng toàn cục trong `public.platform_admins`.
   - Tuy nhiên, do nguyên tắc **Platform Admin không có blanket per-Gateway read bypass**, ở request tiếp theo khi admin gọi `GET /v1/gateways/gw_001/sensors` hay Digital Twin, backend vẫn trả về `404 Not Found`. Admin chỉ có thể xem danh mục kiểm kê trạm toàn hệ thống qua route quản trị riêng biệt `GET /v1/admin/gateways`.
4. **Client bắt mã lỗi 404 và chuyển đổi giao diện:**
   - Hiển thị ngay thông báo lỗi (Error Banner / Snackbar) thông báo quyền truy cập vào trạm đã bị thu hồi hoặc trạm không còn khả dụng.
   - Chuyển giao diện sang **Gateway Not Found View** kèm nút điều hướng người dùng quay lại danh sách trạm (`/gateways`).

---

## 7. Hướng dẫn Triển khai Mã nguồn Dart/Flutter Tham khảo (Illustrative Snippets)

> **LƯU Ý THIẾT KẾ:**
> Các đoạn mã nguồn Dart và Flutter dưới đây đóng vai trò là **mã minh họa kiến trúc (illustrative snippets)** nhằm trình bày cách tổ chức component, phân tầng xử lý và định tuyến trên môi trường Flutter Web. Các đoạn mã này phục vụ việc đối chiếu với hợp đồng API thực tế và không thay thế cho mã nguồn sản phẩm chính thức.

Dưới đây là các thành phần mã nguồn mẫu, được chuẩn hóa theo phong cách idiomatic Dart và Flutter Web, sẵn sàng tích hợp vào dự án.

### 7.1. Định nghĩa Role Enum và Helper (`gateway_role.dart`)

```dart
import 'package:flutter/material.dart';

/// Đại diện cho 3 vai trò người dùng trên từng Gateway theo schema PostgreSQL:
/// CHECK (role IN ('owner', 'operator', 'viewer'))
enum GatewayRole {
  owner('owner'),
  operator('operator'),
  viewer('viewer');

  final String value;
  const GatewayRole(this.value);

  /// Chuyển đổi chuỗi từ backend sang Enum an toàn
  static GatewayRole fromString(String? roleStr) {
    switch (roleStr?.toLowerCase().trim()) {
      case 'owner':
        return GatewayRole.owner;
      case 'operator':
        return GatewayRole.operator;
      case 'viewer':
      default:
        return GatewayRole.viewer;
    }
  }

  /// Tên tiếng Việt hiển thị trên giao diện Web Dashboard
  String get displayName {
    switch (this) {
      case GatewayRole.owner:
        return 'Chủ sở hữu';
      case GatewayRole.operator:
        return 'Vận hành';
      case GatewayRole.viewer:
        return 'Người xem';
    }
  }

  /// Mô tả ngắn về quyền hạn hiển thị trong Tooltip trên Web
  String get description {
    switch (this) {
      case GatewayRole.owner:
        return 'Toàn quyền cấu hình và giám sát trạm quan trắc';
      case GatewayRole.operator:
        return 'Được phép thực hiện các thao tác vận hành an toàn';
      case GatewayRole.viewer:
        return 'Chỉ xem dữ liệu đo đạc và thông tin trạm (Read-only)';
    }
  }

  /// Màu sắc đại diện cho Badge
  Color get badgeColor {
    switch (this) {
      case GatewayRole.owner:
        return Colors.amber.shade900;
      case GatewayRole.operator:
        return Colors.blue.shade800;
      case GatewayRole.viewer:
        return Colors.blueGrey.shade700;
    }
  }

  /// Màu nền nhẹ của Badge
  Color get badgeBackgroundColor {
    switch (this) {
      case GatewayRole.owner:
        return Colors.amber.shade50;
      case GatewayRole.operator:
        return Colors.blue.shade50;
      case GatewayRole.viewer:
        return Colors.blueGrey.shade50;
    }
  }

  /// Icon minh họa cho từng vai trò
  IconData get icon {
    switch (this) {
      case GatewayRole.owner:
        return Icons.admin_panel_settings_outlined;
      case GatewayRole.operator:
        return Icons.engineering_outlined;
      case GatewayRole.viewer:
        return Icons.visibility_outlined;
    }
  }

  /// Kiểm tra vai trò cấu hình thiết bị (Giai đoạn sau)
  bool get canConfigureDevice => this == GatewayRole.owner;

  /// Kiểm tra vai trò gửi lệnh vận hành an toàn (Giai đoạn sau)
  bool get canExecuteOperations => this == GatewayRole.owner || this == GatewayRole.operator;
}
```

---

### 7.2. Widget Hiển thị Huy hiệu Vai trò (`gateway_role_badge.dart`)
Tối ưu hóa cho Web Dashboard với hai chế độ: `compact` cho ô bảng Data Table và `full` cho tiêu đề trang chi tiết.

```dart
import 'package:flutter/material.dart';
import 'gateway_role.dart';

class GatewayRoleBadge extends StatelessWidget {
  final GatewayRole role;
  final bool compact;

  const GatewayRoleBadge({
    Key? key,
    required this.role,
    this.compact = false,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final badgeWidget = Container(
      padding: EdgeInsets.symmetric(
        horizontal: compact ? 8 : 12,
        vertical: compact ? 3 : 6,
      ),
      decoration: BoxDecoration(
        color: role.badgeBackgroundColor,
        borderRadius: BorderRadius.circular(compact ? 4 : 16),
        border: Border.all(
          color: role.badgeColor.withOpacity(0.4),
          width: 1.0,
        ),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            role.icon,
            size: compact ? 13 : 16,
            color: role.badgeColor,
          ),
          const SizedBox(width: 5),
          Text(
            role.displayName,
            style: TextStyle(
              fontSize: compact ? 12 : 13,
              fontWeight: FontWeight.w600,
              color: role.badgeColor,
            ),
          ),
        ],
      ),
    );

    // Trên Web, luôn bọc Tooltip để người dùng hover chuột là xem được giải thích quyền
    return Tooltip(
      message: '${role.displayName}: ${role.description}',
      waitDuration: const Duration(milliseconds: 300),
      child: badgeWidget,
    );
  }
}
```

---

### 7.3. Widget Bảng Dữ liệu Phân quyền trên Web (`web_gateway_table.dart`)
Hiển thị danh sách Gateway trực quan dạng Data Table cho màn hình rộng (Desktop / Tablet ngang).

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'gateway_role.dart';
import 'gateway_role_badge.dart';

class WebGatewayTable extends StatelessWidget {
  final List<dynamic> gateways;

  const WebGatewayTable({
    Key? key,
    required this.gateways,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 0,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(8),
        side: BorderSide(color: Colors.grey.shade200),
      ),
      child: ClipRRect(
        borderRadius: BorderRadius.circular(8),
        child: DataTable(
          headingRowColor: MaterialStateProperty.all(Colors.grey.shade50),
          headingTextStyle: const TextStyle(
            fontWeight: FontWeight.bold,
            color: Colors.black87,
          ),
          columns: const [
            DataColumn(label: Text('Mã trạm (ID)')),
            DataColumn(label: Text('Tên trạm quan trắc')),
            DataColumn(label: Text('Vai trò của bạn')),
            DataColumn(label: Text('Ngày tạo')),
            DataColumn(label: Text('Thao tác')),
          ],
          rows: gateways.map((gw) {
            final gatewayId = gw['gateway_id'] as String;
            final name = gw['name'] as String? ?? 'Chưa đặt tên';
            final role = GatewayRole.fromString(gw['role'] as String?);
            final createdAt = gw['created_at'] as String? ?? '';

            return DataRow(
              cells: [
                DataCell(
                  Text(
                    gatewayId,
                    style: const TextStyle(
                      fontFamily: 'monospace',
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                DataCell(Text(name)),
                DataCell(GatewayRoleBadge(role: role, compact: true)),
                DataCell(Text(createdAt.split('T').first)),
                DataCell(
                  OutlinedButton.icon(
                    icon: const Icon(Icons.arrow_forward, size: 14),
                    label: const Text('Xem chi tiết'),
                    style: OutlinedButton.styleFrom(
                      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                    ),
                    onPressed: () {
                      // Điều hướng chuẩn Web qua go_router
                      context.go('/gateways/$gatewayId');
                    },
                  ),
                ),
              ],
            );
          }).toList(),
        ),
      ),
    );
  }
}
```

---

### 7.4. Widget Trang lỗi 404 Không tìm thấy hoặc Không có quyền (`gateway_not_found_view.dart`)

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';

class GatewayNotFoundView extends StatelessWidget {
  final String gatewayId;

  const GatewayNotFoundView({
    Key? key,
    required this.gatewayId,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Center(
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(32.0),
        child: Container(
          constraints: const BoxConstraints(maxWidth: 520),
          padding: const EdgeInsets.all(32.0),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: Colors.grey.shade200),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withOpacity(0.04),
                blurRadius: 16,
                offset: const Offset(0, 4),
              ),
            ],
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Container(
                padding: const EdgeInsets.all(20),
                decoration: BoxDecoration(
                  color: Colors.orange.shade50,
                  shape: BoxShape.circle,
                ),
                child: Icon(
                  Icons.search_off_outlined,
                  size: 64,
                  color: Colors.orange.shade800,
                ),
              ),
              const SizedBox(height: 24),
              const Text(
                '404 — Không tìm thấy trạm quan trắc',
                style: TextStyle(
                  fontSize: 20,
                  fontWeight: FontWeight.bold,
                  color: Colors.black87,
                ),
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 12),
              RichText(
                textAlign: TextAlign.center,
                text: TextSpan(
                  style: TextStyle(
                    fontSize: 14,
                    color: Colors.grey.shade700,
                    height: 1.5,
                  ),
                  children: [
                    const TextSpan(text: 'Trạm quan trắc '),
                    TextSpan(
                      text: gatewayId,
                      style: const TextStyle(
                        fontFamily: 'monospace',
                        fontWeight: FontWeight.bold,
                        color: Colors.black87,
                      ),
                    ),
                    const TextSpan(
                      text:
                          ' không tồn tại trên hệ thống hoặc tài khoản của bạn chưa được cấp quyền truy cập.\n\nNếu bạn cho rằng đây là sự nhầm lẫn, vui lòng liên hệ Quản trị viên (Platform Administrator) để được phân quyền.',
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 32),
              ElevatedButton.icon(
                onPressed: () {
                  // Điều hướng an toàn quay lại danh sách trạm trên Web
                  context.go('/gateways');
                },
                icon: const Icon(Icons.arrow_back),
                label: const Text('Quay lại danh sách trạm'),
                style: ElevatedButton.styleFrom(
                  padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 14),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(8),
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
```

---

### 7.5. Controller Chi tiết Trạm xử lý F5 và Deep Linking (`gateway_detail_controller.dart`)
Controller phối hợp nạp dữ liệu từ hai endpoint `GET /v1/gateways` và `GET /v1/gateways/{id}/sensors` để tái tạo trạng thái khi người dùng nhấn F5 hoặc truy cập trực tiếp bằng URL.

```dart
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'gateway_role.dart';

enum GatewayDetailStatus { initial, loading, loaded, notFound, error }

class GatewayDetailData {
  final String gatewayId;
  final String name;
  final String description;
  final GatewayRole role;
  final List<dynamic> sensors;

  GatewayDetailData({
    required this.gatewayId,
    required this.name,
    required this.description,
    required this.role,
    required this.sensors,
  });
}

class GatewayDetailController extends ChangeNotifier {
  final String gatewayId;
  final String accessToken;
  final String apiBaseUrl;

  GatewayDetailStatus status = GatewayDetailStatus.initial;
  GatewayDetailData? data;
  String? errorMessage;

  GatewayDetailController({
    required this.gatewayId,
    required this.accessToken,
    required this.apiBaseUrl,
  });

  /// Nạp thông tin trạm và cảm biến (hỗ trợ cả Deep Link và F5 reload)
  Future<void> loadDetails() async {
    status = GatewayDetailStatus.loading;
    errorMessage = null;
    notifyListeners();

    try {
      final headers = {
        'Authorization': 'Bearer $accessToken',
        'Accept': 'application/json',
      };

      // 1. Gọi song song: Lấy danh sách Gateways để lấy Metadata & Role,
      //    và lấy danh sách Sensors của Gateway cụ thể này.
      final gatewaysFuture = http.get(
        Uri.parse('$apiBaseUrl/v1/gateways'),
        headers: headers,
      );
      final sensorsFuture = http.get(
        Uri.parse('$apiBaseUrl/v1/gateways/$gatewayId/sensors'),
        headers: headers,
      );

      final results = await Future.wait([gatewaysFuture, sensorsFuture]);
      final gatewaysRes = results[0];
      final sensorsRes = results[1];

      // 2. Kiểm tra nếu Sensors endpoint trả về 404 (Không tồn tại hoặc không có quyền)
      if (sensorsRes.statusCode == 404) {
        status = GatewayDetailStatus.notFound;
        notifyListeners();
        return;
      }

      // 3. Phân tích kết quả Gateways list để lấy role và tên trạm
      if (gatewaysRes.statusCode == 200 && sensorsRes.statusCode == 200) {
        final gatewaysBody = json.decode(gatewaysRes.body);
        final gatewayList = gatewaysBody['items'] as List<dynamic>? ?? [];

        // Tìm gateway tương ứng trong danh sách người dùng được phân quyền
        final gwMeta = gatewayList.firstWhere(
          (item) => item['gateway_id'] == gatewayId,
          orElse: () => null,
        );

        if (gwMeta == null) {
          // Trạm không có trong danh sách phân quyền của người dùng!
          status = GatewayDetailStatus.notFound;
          notifyListeners();
          return;
        }

        final sensorsBody = json.decode(sensorsRes.body);
        final sensorList = sensorsBody['items'] as List<dynamic>? ?? [];

        data = GatewayDetailData(
          gatewayId: gatewayId,
          name: gwMeta['name'] as String? ?? gatewayId,
          description: gwMeta['description'] as String? ?? '',
          role: GatewayRole.fromString(gwMeta['role'] as String?),
          sensors: sensorList,
        );
        status = GatewayDetailStatus.loaded;
        notifyListeners();
      } else {
        errorMessage = 'Lỗi nạp dữ liệu máy chủ: ${sensorsRes.statusCode}';
        status = GatewayDetailStatus.error;
        notifyListeners();
      }
    } catch (e) {
      errorMessage = 'Lỗi kết nối mạng: $e';
      status = GatewayDetailStatus.error;
      notifyListeners();
    }
  }
}
```

#### Snippet Cấu hình Định tuyến `go_router` trên Web (`app_router.dart`):

```dart
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'screens/gateway_list_screen.dart';
import 'screens/gateway_detail_screen.dart';

final GoRouter appRouter = GoRouter(
  initialLocation: '/gateways',
  routes: [
    GoRoute(
      path: '/gateways',
      name: 'gateways',
      builder: (context, state) => const GatewayListScreen(),
      routes: [
        GoRoute(
          path: ':id',
          name: 'gateway-detail',
          builder: (context, state) {
            // Đọc gatewayId trực tiếp từ pathParameters chuẩn Declarative Routing
            final gatewayId = state.pathParameters['id'] ?? '';
            return GatewayDetailScreen(gatewayId: gatewayId);
          },
        ),
      ],
    ),
  ],
);
```

---

## 8. Bảng kiểm tra kiểm thử phía Client Web (Web Client Verification Checklist)

Trước khi đóng gói hoặc nghiệm thu ứng dụng Flutter Web, các kịch bản kiểm thử phân quyền trên trình duyệt sau bắt buộc phải được xác nhận:

| STT | Kịch bản kiểm thử Web | Thao tác thực hiện | Hành vi mong đợi của Web Client | Kết quả đạt |
|---|---|---|---|---|
| **TC-01** | Tài khoản mới chưa được gán trạm (`items: []`) | Đăng nhập tài khoản mới chưa có quyền trạm nào. | Hiển thị giao diện rỗng (`EmptyGatewayView`) dạng Card nổi giữa màn hình kèm nút "Làm mới", không báo lỗi HTTP. | [ ] |
| **TC-02** | Hiển thị Bảng Dữ liệu Responsive trên Desktop | Mở Web trên màn hình $\ge 900\,\text{px}$. | Hiển thị danh sách dạng Data Table với đầy đủ các cột: Mã trạm, Tên trạm, Vai trò, Ngày tạo, Thao tác. | [ ] |
| **TC-03** | Hiển thị Role Badge & Tooltip vai trò | Rê chuột (hover) qua Role Badge (`owner`, `operator`, `viewer`). | Hiển thị Tooltip giải thích chi tiết quyền hạn và mã màu đại diện trực quan. | [ ] |
| **TC-04** | Deep Link truy cập trạm KHÔNG có quyền | Gõ trực tiếp trên URL: `https://.../gateways/gw_unauthorized`. | Client gọi API, nhận `404 Not Found` -> Giữ nguyên URL, hiển thị `GatewayNotFoundView` kèm nút "Quay lại danh sách trạm". | [ ] |
| **TC-05** | Nhấn F5 / Reload tại trang chi tiết trạm hợp lệ | Đang ở `/gateways/gw_001`, nhấn F5 trên trình duyệt. | Client gọi `GET /v1/gateways` & `GET /v1/gateways/gw_001/sensors`, nạp lại đầy đủ Tên trạm, Role Badge và Danh sách cảm biến. | [ ] |
| **TC-06** | Nhấn F5 / Reload tại trang trạm KHÔNG hợp lệ | Đang ở `/gateways/gw_invalid`, nhấn F5 trên trình duyệt. | Client phân giải path parameter, gọi API nhận 404 -> Hiển thị trang lỗi `GatewayNotFoundView`. | [ ] |
| **TC-07** | Admin thu hồi quyền `operator`/`viewer` qua API | Admin gọi `DELETE /v1/admin/gateways/{id}/users/{id}` thu hồi vai trò `operator` hoặc `viewer`. User bị thu hồi bấm reload cảm biến trên cùng JWT. | Backend commit DB, trả về `204 No Content` body rỗng hoàn toàn. Request tiếp theo của user trả về `404 Not Found` (Next DB query) -> Client bắt lỗi, chuyển sang thông báo mất quyền / `GatewayNotFoundView`. | [ ] |
| **TC-07a** | Admin cố thu hồi vai trò của `owner` qua DELETE | Admin gọi `DELETE /v1/admin/gateways/{id}/users/{id}` khi target user đang là `owner` của trạm (kể cả admin tự xóa chính mình). | Backend từ chối với HTTP `409 Conflict` (`code: "owner_transfer_required"`), bảo vệ owner tuyệt đối (no self-owner bypass). Yêu cầu chuyển owner trước (`PUT .../owner`), sau đó mới thu hồi `viewer`. | [ ] |
| **TC-07b** | Thu hồi membership không tồn tại (Idempotent No-op) | Admin gọi `DELETE /v1/admin/gateways/{id}/users/{id}` trên trạm tồn tại nhưng target user không có membership (hoặc target UUID hợp lệ không tồn tại). | Backend xử lý no-op an toàn, trả về `204 No Content` với body rỗng hoàn toàn, không gây lỗi và không tạo bản ghi mới. | [ ] |
| **TC-07c** | Thu hồi thành viên trên Gateway không tồn tại | Admin gọi `DELETE /v1/admin/gateways/invalid_gw/users/{id}` với `gateway_id` không có trong hệ thống. | Backend trả về HTTP `404 Not Found` (`code: "not_found"`), bảo vệ tính toàn vẹn định tuyến tài nguyên cha. | [ ] |
| **TC-07d** | Non-admin (kể cả Gateway Owner) gọi DELETE membership | Dùng JWT của người dùng không có trong `public.platform_admins` gọi `DELETE /v1/admin/gateways/{id}/users/{id}`. | Backend từ chối tức thì với HTTP `403 Forbidden` (`code: "forbidden"`), chứng minh quyền trạm không thể thay thế quyền Platform Admin. | [ ] |
| **TC-07e** | Platform Admin tự thu hồi quyền `viewer`/`operator` của chính mình | Admin gọi `DELETE` tự xóa membership của mình trên một trạm; sau đó gọi `GET /v1/gateways/{id}/sensors`. | DELETE trả về `204 No Content`. Quyền Platform Admin toàn cục giữ nguyên (không bị hạ quyền), nhưng request đọc cảm biến trạm trả về `404 Not Found` (tuân thủ nghiêm ngặt nguyên tắc No Admin Read Bypass). | [ ] |
| **TC-08** | Cố tình chèn query `?user_id=other_uuid` vào URL | Gõ URL kèm tham số `?user_id=...` gọi lên backend. | Backend từ chối `400 Bad Request` (`invalid_request`) -> Client bắt lỗi an toàn, không hiển thị dữ liệu của người khác. | [ ] |
| **TC-09** | Platform Admin đăng nhập vào Web Dashboard | Đăng nhập bằng tài khoản Platform Admin, gọi `GET /v1/gateways`. | Chỉ nhìn thấy các trạm được phân quyền trong `user_gateways` cho Admin đó; không tự bung danh sách toàn hệ thống (No Admin Read Bypass). | [ ] |
| **TC-10** | Gateway Owner (non-admin) gọi route quản trị | Dùng JWT của Gateway `owner` gọi bất kỳ route nào dưới `/v1/admin/*`. | Backend từ chối tức thì với mã lỗi `403 Forbidden` (`insufficient permissions`), chứng minh owner không phải là admin. | [ ] |
| **TC-11** | Gọi các endpoint stub Giai đoạn 2 | Client gửi request tới `/v1/telemetry/history`, `/v1/ws`, hoặc `/v1/digital-twins`. | Backend trả về `501 Not Implemented` (`not_implemented`) -> Client xử lý an toàn, thông báo tính năng chưa khả dụng ở Stage 2. | [ ] |
| **TC-12** | Sự cố cơ sở dữ liệu hoặc timeout lookup quyền | Mô phỏng ngắt kết nối PostgreSQL hoặc timeout tra cứu quyền/admin. | Backend trả về `503 Service Unavailable` (`service_unavailable`) -> Client hiển thị thông báo gián đoạn dịch vụ kèm nút thử lại. | [ ] |
