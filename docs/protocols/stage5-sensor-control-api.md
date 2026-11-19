# Stage 5 Sensor Control REST API

**Phạm vi:** Contract hiện hành của API điều khiển lấy mẫu Sensor. Đây là tài
liệu tích hợp/rehearsal Local MVP, không phải chấp thuận rollout production.
Source of truth về MQTT là [MQTT v1](mqtt-v1.md); đọc Twin/metadata/history theo
[Stage 4 API](stage4-read-and-entity-api.md).

## Routes và quyền

Sensor phải được provision trước. JWT xác định người dùng; backend kiểm tra
membership Gateway hiện hành trong DB cho từng request (kể cả replay). `owner`
và `operator` được thay đổi sampling; `viewer` chỉ đọc. Platform admin không
tự bypass membership. Không có `POST .../commands`, `/v1/commands/{id}` hay API
để client ghi reported state.

| Method | Path | Hành vi |
| --- | --- | --- |
| `PATCH` | `/v1/digital-twins/{entity_id}/desired-state` | Gửi desired sampling intent |
| `GET` | `/v1/digital-twins/{entity_id}/commands` | Đọc command list của Sensor; `limit` mặc định 20, tối đa 100; có `cursor` và `Link` trang tiếp |
| `GET` | `/v1/digital-twins/{entity_id}/commands/{command_id}` | Đọc command thuộc đúng Sensor/Gateway; không nhận query string |
| `POST` | `/v1/digital-twins/{entity_id}/commands/{command_id}/cancel` | Hủy an toàn command đã commit; chỉ được đăng ký khi `STAGE6_ENABLED=true`, body phải rỗng tuyệt đối |

`entity_id` là URN Sensor `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>` (URL
path-escape khi tạo URL). Các API đọc khác vẫn là `GET
/v1/digital-twins/{entity_id}?format=simplified` cho state, `PATCH
/v1/digital-twins/{entity_id}/attrs` cho metadata, và `GET
/v1/digital-twins/{entity_id}/temporal` cho history; chúng không thay thế API
desired-state.

## PATCH request

Gửi `Content-Type: application/json`, Bearer JWT, và đúng một header
`Idempotency-Key` (1–128 ký tự ASCII in được; control characters bị từ chối).
Body tối đa 8 KiB, chỉ nhận đúng hai field sau (không field lạ); query string
không được phép:

```http
PATCH /v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001/desired-state
Authorization: Bearer <USER_ACCESS_TOKEN>
Content-Type: application/json
Idempotency-Key: <NEW_OPAQUE_KEY>

{"frequency_hz":1.5,"expected_desired_version":"12"}
```

`frequency_hz` phải là JSON decimal number, `0 < Hz <= 100`. Parser giữ chính
xác decimal/rational (không dùng float64 để quyết định), đổi sang
`sample_interval_us = round-half-up(1,000,000 / Hz)` dạng integer. Interval hữu
hiệu phải nằm trong `[10,000, 2^53]` microseconds; do đó không phải mọi số Hz
trong range toán học đều biểu diễn được. Ví dụ `0.1` Hz → `10,000,000` µs,
`1.5` Hz → `666,667` µs. Giá trị effective cadence tương ứng là
`1,000,000 / sample_interval_us` Hz; đây là cadence được yêu cầu từ runtime,
không cam kết cadence vật lý chính xác.

`expected_desired_version` bắt buộc là JSON string chứa số thập phân không âm
trong miền PostgreSQL BIGINT; không gửi JSON number (JavaScript precision).
Version được kiểm tra optimistic concurrency. Khi cần intent mới, đọc version
hiện tại rồi dùng key mới; khi retry cùng intent, gửi nguyên body và cùng key.

Ví dụ `202 Accepted` nghĩa là intent đã commit (desired + command + outbox +
receipt); chưa xác nhận Gateway đã áp dụng. Response có `Location` trỏ tới
command detail:

```json
{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "command_id": "<COMMAND_ID>",
  "status": "pending",
  "requested_frequency_hz": 1.5,
  "effective_frequency_hz": 1.499999250000375,
  "sample_interval_us": 666667,
  "desired_version": "13",
  "expires_at": "<UTC_TIMESTAMP>",
  "status_url": "/v1/digital-twins/<ESCAPED_ENTITY_ID>/commands/<COMMAND_ID>"
}
```

Nếu interval desired và confirmed reported configuration đều đã khớp, trả
`200 OK` với request outcome
`"status":"unchanged"`; không tạo command/outbox hoặc tăng version. Replay cùng
actor/entity/key và fingerprint trả nguyên status/body receipt ban đầu, kể cả
receipt `200 unchanged`; authorization hiện hành vẫn được kiểm trước replay.
Dùng key cũ với body/version khác trả conflict. Chỉ một command active được
phép cho mỗi Sensor; request mới trong lúc active trả `409 command_in_progress`.

## Theo dõi và kết quả

Poll list/detail bằng REST. Command status `pending` là intent đã commit;
`published` chỉ là broker PUBACK, không phải thực thi. Kết quả `succeeded` chỉ
hợp lệ sau result hợp lệ và transaction backend commit command cùng
`reported.configuration`. `failed`/`timeout` không xác nhận cấu hình mới.
Gateway có thể giữ requested/desired khác với confirmed configuration; sampling
observations trong telemetry/temporal là dữ liệu quan sát, không phải desired
state. Đọc state bằng `?format=simplified`; đọc `sample_interval_us` history qua
`/temporal?attrs=sample_interval_us&timerel=between&timeAt=<UTC-Z>&endTimeAt=<UTC-Z>`.

Các lỗi thường gặp dùng JSON `code` an toàn: `400 invalid_request`,
`400 frequency_not_representable`, `400 unsupported_command_target`,
`401 unauthorized`, `403 forbidden`, `404 not_found` (bao gồm entity/Gateway
không được phép lộ), `409 desired_version_conflict`,
`409 idempotency_conflict`, `409 command_in_progress`,
`409 command_not_cancellable`, `409 desired_version_overflow`, `413 payload_too_large`,
`415 unsupported_media_type`, `503 service_unavailable` (feature/dependency/
deadline), hoặc `500 internal_error`. Dùng detail API để kiểm tra command đang
active trước khi quyết định bước tiếp; không tự retry với key/body mới khi chưa
biết request trước đã commit chưa.

`acknowledged` là trạng thái legacy-reserved, không phải receipt runtime/API
acknowledgement mới. Expiry có thể đưa `pending`/`published`/legacy
`acknowledged` sang `timeout` và dừng outbox; desired không bị rollback. MQTT
transport, result payload/ACL và late-result semantics được quy định tại
[MQTT v1](mqtt-v1.md). Không có auto reconciliation/cancellation hoặc hứa hẹn
rollback hành động thiết bị.

## [Giai đoạn 6 — BỔ SUNG ĐIỀU KHIỂN & KHẢ NĂNG PHỤC HỒI] Vòng đời mở rộng và cơ chế Retry/Reservation bền vững

> **Lưu ý ranh giới:** Mục này mô tả thiết kế và trạng thái thực hiện của Giai đoạn 6 (Stage 6).
> - **Đã triển khai trong mã nguồn backend và Gateway simulator (Opt-in qua `STAGE6_ENABLED=true` / `GATEWAY_STAGE6_ENABLED=true`, mặc định `false`):**
>   - Lịch retry lũy thừa kèm jitter ngẫu nhiên 50–100%, bảo vệ fencing chống stale worker, giới hạn thời gian publish theo lease/TTL (R3).
>   - Endpoint hủy an toàn `POST /v1/digital-twins/{entity_id}/commands/{command_id}/cancel` (chỉ hủy trước reservation, trả 409 nếu đã cấp quyền dispatch, trả 200 idempotent lặp lại) (R4).
>   - Envelope `command_receipt` và trạng thái `acknowledged` (Gateway lưu receipt bền vững trước apply, backend chuyển status `acknowledged`; không thay thế result thành công) (R5).
>   - Envelopes `configuration_query` và `configuration_snapshot` cho phép backend đọc lại cấu hình thực tế từ Gateway sampler và SQLite watermark sau reconnect/discovery (R6).
>   - Engine đối soát hội tụ tự động (Reconciliation Engine): phát hiện sai lệch, kiểm tra điều kiện (chưa bị cancel, còn budget <= 1), tạo command successor với ID mới, version mới tăng 1, cùng giá trị desired sampling (R7).
>   - Tích hợp CI và diễn tập hồi quy (R8): GitHub Actions run 44501549344 đã PASS trên commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f`.
>   - Kiểm chứng nâng cấp schema migration 26→27 và script dọn dẹp retention 30 ngày (R2 follow-up).
> - **Tổng thể Stage 6 vẫn PARTIAL:** Script retention còn finding review độc lập (MEDIUM guard `json_each` thứ tự đánh giá, LOW timestamp snapshot); target demo/preflight/restore và rollout shared/live NOT RUN; board AM5728 / real sensor / HIL DEFERRED / NOT RUN. Mọi cờ triển khai vẫn mặc định `false`.

### 1. Ma trận chuyển trạng thái vòng đời (State Transition Matrix)

Vòng đời command được mở rộng để phân định rõ ràng giữa:
- **Active states:** `pending`, `published`, `acknowledged` (bảo vệ khe lệnh duy nhất của Sensor theo ràng buộc `uq_twin_commands_active_sensor`).
- **Terminal states (bất biến, không thể đảo ngược):** `succeeded`, `failed`, `timeout`, `cancelled` (trạng thái kết thúc mới).

Phân định state machine: `twin_commands.status` sở hữu vòng đời nghiệp vụ ở trên; `twin_outbox.status` sở hữu vòng đời vận chuyển (`pending`, `published`, `failed` theo ràng buộc `chk_twin_outbox_status`). Khi command kết thúc hoặc bị hủy, outbox sử dụng quy ước terminal tối thiểu hiện có (`status='failed'`) để dừng vĩnh viễn việc quét và gửi lại.

#### Bảng chuyển trạng thái chi tiết

| Trạng thái hiện tại | Sự kiện / Thao tác | Điều kiện kích hoạt | Trạng thái tiếp theo | Tác động trạng thái & Database side effects |
| --- | --- | --- | --- | --- |
| *(None)* | **Admit / Commit Intent** | PATCH desired-state hợp lệ, version khớp, sensor rảnh | `pending` | Ghi nguyên tử `twin_commands` (`status='pending'`), `twin_outbox` (`status='pending'`, `attempt_count=0`, `next_attempt_at=now()`), và `twin_command_requests` (receipt). Không ghi `reported.configuration`. |
| `pending` | **First Dispatch Reservation** | Worker `PrepareControl`, `next_attempt_at <= now()`, chưa từng reserve | `pending` | Tăng `attempt_count=1` trong cả `twin_outbox` và mirrored `twin_commands`. Cập nhật `twin_outbox.next_attempt_at = max(lease_deadline, now + delay(1))` (với `lease_deadline` là mốc biên suy diễn logic). **Chính thức vượt qua ranh giới được hủy (cancel barrier)**. Outbox vẫn `pending`. |
| `pending` | **Safe Cancel Request** | `POST .../cancel`, `status='pending'`, `expires_at > now()`, có lịch sử Stage 6 xác nhận chưa từng có reservation (`attempt_count=0` và zero confirmed grants) | `cancelled` | Chuyển `twin_commands.status='cancelled'`, ghi `cancelled_at`, `cancelled_by`. Dừng outbox bằng quy ước terminal tối thiểu hiện có (`twin_outbox.status='failed'`). Đánh dấu intent/version không tự động reconcile phát lại. `desired_state` giữ nguyên. |
| `cancelled` | **Repeated Cancel (Idempotent)** | `POST .../cancel` trên command đã `cancelled` | `cancelled` | Kiểm tra lại JWT và quyền `user_gateways` hiện hành; trả về **deterministic 200 OK** với cùng schema kết quả hủy, **kể cả khi `expires_at` gốc đã trôi qua**. Tính bất biến của terminal state thắng quy trình expiry. |
| `pending` / `published` / `acknowledged` | **Cancel sau Reservation hoặc Legacy** | `POST .../cancel` khi `attempt_count >= 1`, hoặc đã qua reservation, hoặc bản ghi legacy `UNKNOWN` history | *(Không đổi)* | **Từ chối dứt khoát 409 `command_not_cancellable`**. Không thể bảo đảm an toàn trên kênh truyền do lệnh có thể đã hoặc đang được đưa lên broker/thiết bị. |
| `pending` | **Broker PUBACK (Lần đầu)** | MQTT Publish thành công lần đầu, `MarkControlPublished` | `published` | Cập nhật `twin_commands.status='published'`, `twin_outbox.status='published'` có điều kiện theo `attempt_count`. Chưa chứng minh Gateway đã nhận hay thực thi. |
| `published` | **Broker PUBACK (Gửi lại)** | MQTT Publish gửi lại thành công khi đang `published` | `published` | Ghi nhận hoàn tất lượt gửi có điều kiện theo `attempt_count` của reservation hiện tại; giữ nguyên `published` mà không làm hồi quy trạng thái. |
| `acknowledged` | **Late PUBACK Mark** | PUBACK hoàn tất sau khi Gateway receipt đã được xử lý | `acknowledged` | **Strict no-op status completion.** Giữ nguyên `acknowledged`, không mở lại outbox đã dừng, không thay đổi `retry_stop_reason` hay lịch `next_attempt_at`. Terminal/receipt luôn thắng. |
| `pending` / `published` | **Durable Device Receipt** | Nhận `command_receipt` hợp lệ từ Gateway qua MQTT responses | `acknowledged` | Cập nhật `twin_commands.status='acknowledged'`, ghi `acknowledged_at`. Dừng outbox retry (`twin_outbox.status='failed'`). **Không cập nhật `reported.configuration`**, không thay đổi `expires_at`. |
| `acknowledged` | **Duplicate Receipt** | Nhận `command_receipt` trùng khớp payload hash | `acknowledged` | Idempotent no-op (chấp nhận kết quả lặp, không báo lỗi, không đổi DB). |
| `acknowledged` | **Conflicting Receipt** | Nhận `command_receipt` khác payload hash | `acknowledged` | Từ chối với `control_result_conflict`. Giữ nguyên dữ liệu ban đầu. |
| `pending` / `published` / `acknowledged` | **Early Result (Thành công)** | Nhận `command_result` (`status='succeeded'`) trước PUBACK hoặc receipt | `succeeded` | **Chỉ tại bước này mới cập nhật `reported.configuration`** nguyên tử với `twin_states` qua `jsonb_set`. Dừng outbox (`status='published'`). |
| `pending` / `published` / `acknowledged` | **Early Result (Thất bại)** | Nhận `command_result` (`status='failed'`) hợp lệ | `failed` | Cập nhật command `status='failed'`, lưu lỗi. Dừng outbox (`status='failed'`). Không cập nhật `reported.configuration`. |
| `pending` / `published` / `acknowledged` | **Authoritative Expiry** | `clock_timestamp() >= expires_at` tại bất kỳ điểm kiểm tra DB | `timeout` | Expiry priority: `ExpireSensorCommandsTx` chuyển command sang `timeout`, dừng outbox (`status='failed'`). Desired state giữ nguyên. Chỉ áp dụng cho active states, không ảnh hưởng terminal states. |
| `timeout` | **Late Result** | Nhận `command_result` sau khi command đã sang `timeout` | `timeout` | Không làm sống lại command. Lưu bằng chứng vào `late_result_bytes`, `late_result_hash`, `late_result_received_at` (outcome `late`). **Tuyệt đối không cập nhật `reported.configuration`**. |
| `cancelled` | **Late Receipt / Result** | Nhận receipt hoặc result của command đã hủy | `cancelled` | Bị từ chối / ghi nhận conflict. **Tuyệt đối không làm thay đổi trạng thái và không cập nhật `reported.configuration`**. |
| `succeeded` / `failed` | **Duplicate Result** | Nhận `command_result` trùng khớp bytes đã commit | `succeeded` / `failed` | Idempotent duplicate (outcome `duplicate`), không thay đổi DB. |
| `succeeded` / `failed` | **Conflicting Result** | Nhận `command_result` khác bytes đã commit | `succeeded` / `failed` | Từ chối với lỗi `control_result_conflict`. Trạng thái terminal giữ nguyên. |
| *(Bất kỳ Terminal)* | **Bất kỳ Event nào** | Result, Receipt, Mark, hoặc Expiry đến muộn | *(Bất biến)* | Các trạng thái `succeeded`, `failed`, `timeout`, `cancelled` là **immutable**, không bao giờ quay lại active. |

---

### 2. Nguồn sự thật cho Attempts, Fencing và Thứ tự khóa (Authoritative Attempts, Fencing & Lock Order)

1. **Nguồn sự thật cho lịch dispatch:**
   - Trong schema hiện hữu, **`twin_outbox.attempt_count`** và **`twin_outbox.next_attempt_at`** là nguồn sự thật tối cao điều phối hàng đợi và lịch dispatch. Cột `next_attempt_at` chỉ tồn tại trên `twin_outbox`, không có trên `twin_commands`.
   - Để bảo đảm tính nhất quán toàn vẹn quan hệ, `twin_commands.attempt_count` được phản ánh đồng bộ (transactional mirror) trong cùng transaction cấp phát reservation (`PrepareControl`).
2. **Ý nghĩa của Attempt Count:**
   - `attempt_count` đại diện cho **số lần cấp quyền gửi (confirmed dispatch grants / reservations)**, KHÔNG PHẢI số lần gọi mạng Publish thực tế.
   - Nếu tiến trình gặp sự cố (crash / restart) sau khi transaction reservation đã commit nhưng trước khi hoàn tất MQTT publish, reservation đó vẫn tính là đã tiêu thụ một lượt attempt.
   - **Xử lý lỗi commit reservation:** Mọi lỗi hoặc tính bất định khi commit transaction reservation đều **tuyệt đối không cho phép thực hiện publish mạng** (fail-closed). Quá trình phục hồi bắt buộc thực hiện qua việc re-read/quét lại trạng thái bền vững từ DB trong các vòng quét discovery tiếp theo.
3. **Cơ chế Fencing chống Stale Worker và Giới hạn Publish Context:**
   - Mỗi lần reserve attempt, transaction tăng `attempt_count` và đặt lịch `next_attempt_at = max(lease_deadline, now + delay(n))`.
   - Fencing được thực thi đồng thời bởi **định danh attempt (`attempt_count`)** VÀ **trạng thái hiện tại hợp lệ (`allowed current states`)**:
     - Lần gửi đầu: `UPDATE twin_commands SET status='published' WHERE command_id=$1 AND status='pending' AND attempt_count=$2`.
     - Lần gửi lại (resend): hoàn tất gửi mà không làm hồi quy trạng thái khi lệnh đã ở `published`, kiểm tra đúng `attempt_count=$2`.
   - Stale worker của lượt attempt cũ ($n-1$) hoàn tất muộn sẽ thất bại khi thực hiện conditional write vì `attempt_count` đã tăng lên $n$.
   - **Giới hạn thời gian Publish:** Database fencing không thể thu hồi side effect mạng một khi packet đã lên đường truyền. Cơ chế lease bảo đảm các lần gửi không chồng lấn dựa trên giả định thời gian thực hiện publish được giới hạn chặt chẽ:
     $$\text{publish\_context\_timeout} = \min\left(\text{Config.PublishTimeout},\, \text{command remaining TTL},\, \text{remaining usable lease}\right)$$
   - **Fail-closed của quyền gửi (managed dispatch):** Một dispatch được quản lý (`Stage6`) chỉ được phát mạng khi có đủ `attempt > 0`, `expires_at` và `lease_at` còn hiệu lực. Thiếu bất kỳ thành phần nào, hoặc quyền đã hết hạn tại thời điểm worker bắt đầu, thì worker **bỏ qua hoàn toàn lệnh gọi mạng** và đếm vào bộ đếm `grants_skipped_before_network`. Một worker bị trễ không thể bắt đầu gửi bằng quyền đã hết hạn.
4. **Quy tắc an toàn cho Legacy Data (Dữ liệu cũ):**
   - Các bản ghi cũ trong database có `attempt_count = 0` được coi là **lịch sử không xác định (UNKNOWN history)**, vì runtime cũ không theo dõi lượt reservation.
   - Nguyên tắc an toàn đóng (fail-closed): **Tuyệt đối không coi `attempt_count = 0` cũ là bằng chứng chưa gửi lệnh.** Mọi yêu cầu hủy an toàn (`cancel`) đối với các bản ghi legacy này đều bị từ chối với `409 command_not_cancellable`.
5. **Thứ tự khóa và Phân tách Mạng / Cơ sở dữ liệu:**
   - **Tách biệt tuyệt đối:** Không bao giờ giữ transaction cơ sở dữ liệu trong khi thực hiện I/O mạng MQTT (Publish hoặc đợi PUBACK).
   - **Thứ tự khóa chuẩn cho Actor paths (Admission, Cancel):**
     `gateways FOR SHARE` → `user_gateways FOR SHARE` → `sensors FOR UPDATE` → `twin_entities FOR UPDATE` → `twin_states FOR UPDATE` → `twin_commands/twin_outbox FOR UPDATE`.
   - **Thứ tự khóa chuẩn cho Internal worker paths (Prepare, Mark, Expire, Result, Receipt):**
     `gateways FOR SHARE` → `sensors FOR UPDATE` → `twin_entities FOR UPDATE` → `twin_states FOR UPDATE` → `twin_commands/twin_outbox FOR UPDATE`.

---

### 3. Quy chuẩn toán học cho Retry, Jitter và Lease (Retry Schedule, Jitter & Lease Specification)

1. **Công thức tính Backoff và Jitter:**
   - Gọi $n \in [1, \text{max\_attempts}]$ là số thứ tự attempt chuẩn bị cấp quyền (với $n=1$ là lần gửi đầu, $n=2$ là lần retry thứ nhất).
   - Tham số cấu hình đề xuất:
     - $\text{base} = \text{STAGE5\_RETRY\_BACKOFF}$ (mặc định $1\,\text{s}$).
     - $\text{cap} = 8\,\text{s}$.
     - $\text{max\_attempts} = 5$ lượt cấp quyền (grants).
   - Khoảng thời gian chờ cơ sở trước khi áp dụng jitter:
     $$\text{backoff}(n) = \min\left(\text{cap},\, \text{base} \times 2^{n-1}\right)$$
     Cụ thể: $n=1 \to 1\,\text{s}$; $n=2 \to 2\,\text{s}$; $n=3 \to 4\,\text{s}$; $n=4 \to 8\,\text{s}$; $n=5 \to 8\,\text{s}$.
   - Công thức Jitter ngẫu nhiên (50% – 100% của backoff):
     $$\text{delay}(n) = \text{backoff}(n) \times \left(0.5 + 0.5 \times U\right)$$
     với $U \in [0, 1)$ là biến ngẫu nhiên phân phối đều. Kết quả $\text{delay}(n)$ luôn dương, nằm trong khoảng $[0.5 \times \text{backoff}(n),\, \text{backoff}(n)]$ và an toàn chống tràn số nguyên (overflow-safe).
2. **Thời hạn Lease của Attempt và xác định `next_attempt_at`:**
   - Để ngăn chặn việc hai worker xử lý chồng chéo cùng một command, mỗi reservation được bảo vệ bởi một khoảng giữ chỗ an toàn logic:
     $$\text{lease\_duration} = \text{PublishTimeout} + \text{safety\_margin} \quad (\text{ví dụ: } 3\,\text{s} + 1\,\text{s} = 4\,\text{s})$$
     $$\text{lease\_deadline} = \text{reservation\_time} + \text{lease\_duration}$$
   - Mốc thử lại tiếp theo (`next_attempt_at` trên `twin_outbox`) được xác định không được chồng lấn với lease đang hiệu lực:
     $$\text{next\_attempt\_at} = \max\left(\text{lease\_deadline},\, \text{reservation\_time} + \text{delay}(n)\right)$$
     `lease_deadline` là mốc biên suy diễn trong bộ nhớ khi tính toán, không lưu thành cột thừa trong DB.
3. **Giới hạn bởi hạn chót của Command (`expires_at`):**
   - Hạn chót hiệu lực của lệnh (`expires_at`) là trần tối thượng.
   - **Điều kiện cấp quyền (grant):** Một reservation chỉ được cấp khi **toàn bộ lease hữu hạn** còn lại vừa nằm trong thời gian còn lại của command:
     $$\text{reservation\_time} + \text{lease\_duration} < \text{expires\_at}$$
     Nếu điều kiện này không thỏa mãn thì **không cấp quyền, không tăng `attempt_count`, không phát sinh lịch retry mới**; lịch được đặt tạm ở `next_attempt_at = expires_at` và lý do dừng **không** được ghi là `retry_exhausted` (hạn TTL ngắn không đồng nghĩa hết lượt cấp quyền).
   - Nếu $\text{next\_attempt\_at} \ge \text{expires\_at}$, hoặc khoảng thời gian còn lại trước $\text{expires\_at}$ không đủ để chứa một lượt $\text{lease\_duration}$, thì **không lên lịch retry tiếp theo**:
     $$\text{next\_attempt\_at} = \text{expires_at}$$
   - Command sẽ giữ trạng thái active (`pending` hoặc `published`) cho đến khi `clock_timestamp() >= expires_at`, lúc đó tiến trình định kỳ sẽ timeout lệnh theo đúng quy trình chuẩn.
4. **Nguyên tắc Worker và Hàng đợi:**
   - **Không sleep trên worker:** Khi gặp lỗi publish hoặc mark, worker giải phóng tác vụ ngay lập tức, không dùng `time.Sleep` hay timer giữ worker thread. Lượt retry sẽ được phát hiện tự nhiên qua câu truy vấn `o.next_attempt_at <= clock_timestamp()` trong lần quét tiếp theo.
   - **Không timer/goroutine theo từng command:** Không tạo goroutine hay timer riêng lẻ trong bộ nhớ cho mỗi command.
   - **Tín hiệu Wake-up không bỏ qua thời gian DB:** Cơ chế wake-up sau commit chỉ kích hoạt vòng quét discovery; câu lệnh SQL discovery luôn bắt buộc `o.next_attempt_at <= clock_timestamp()`. Wake-up không thể làm lệnh chạy sớm hơn lịch hẹn DB.
   - **Transport chưa sẵn sàng:** Nếu `Publisher.Ready()` trả về false trước khi gọi reservation, worker bỏ qua lượt xử lý mà **không tiêu thụ attempt** (không tăng `attempt_count`).
   - **Nhịp giãn (pacing) của worker:** Worker được quản lý của Stage 6 **giải phóng slot ngay lập tức** khi publish hoặc mark lỗi, kể cả khi lỗi xảy ra ở bước `PrepareControl` hoặc khi commit bị bất định; lịch bền vững trong DB quyết định thời điểm lượt kế tiếp. Đường dispatch legacy của Stage 5 giữ nguyên nhịp giãn `STAGE5_RETRY_BACKOFF` để không thay đổi hành vi tương thích. Chế độ runtime vẫn được xác định rõ kể cả khi không có dispatch nào được trả về.
   - **Không vòng quét nóng:** Việc phát hiện lại công việc đến hạn chỉ đi qua vòng quét định kỳ có giới hạn (`STAGE5_SCAN_INTERVAL`) và tín hiệu wake-up đã gộp; không có scheduler thứ hai, không có timer/goroutine riêng cho từng command. **Khi một lượt Stage 6 không có quyền dispatch hoặc gặp lỗi** (ví dụ `dispatch == nil`, `PrepareControl` lỗi, hoặc commit không chắc chắn), worker **không** phát tín hiệu wake-up hoàn tất: hàng đó không có tiến trình mới để đánh thức ngay. Với tình huống này, **vòng quét định kỳ có giới hạn là đường phục hồi**, nhịp kiểm tra bám theo `STAGE5_SCAN_INTERVAL`. Tín hiệu wake-up sau một lượt **thành công**, tín hiệu thông báo sau admission và toàn bộ hành vi khi Stage 6 tắt được giữ nguyên.
5. **Tính bất biến của Payload khi Retry và Hiện tượng Bất định (Ambiguity):**
   - Khi gửi lại lệnh (kể cả khi đã ở trạng thái `published`), outbox giữ nguyên 100% `payload_bytes`, `command_id`, `desired_version`, `issued_at`, và `expires_at`.
   - Khi đã nhận `acknowledged`, mọi hành vi gửi lại command tự động đều bị triệt tiêu (suppressed), outbox dừng lịch gửi.
   - **Hết lượt cấp quyền (Retry Exhaustion):** Khi $n \ge \text{max\_attempts}$ mà chưa nhận được receipt/result (bao gồm cả trường hợp khởi động lại với policy `max_attempts` giảm thấp hơn số lần đã thử), hệ thống ghi nhận lý do `retry_exhausted` vào metadata và đặt `next_attempt_at = expires_at`. Command **vẫn được giữ ở trạng thái active** để chờ result từ thiết bị cho đến khi hết hạn TTL, **tuyệt đối không tự bịa đặt trạng thái `failed`** khi kết quả thực tế chưa rõ, và hàng đã hết hạn retry không tiếp tục chiếm giữ prefix discovery.
   - **Xử lý sự cố bất định:** Nếu xảy ra timeout publish hoặc lỗi cơ sở dữ liệu khi ghi nhận mark sau PUBACK, hệ thống duy trì tính bất định an toàn bằng cách cho phép quét lại theo lịch hẹn với cùng định danh và nội dung lệnh.
6. **Bộ đếm vận hành (Operational Counters — nhãn cố định, không có ID lệnh/Sensor):**
   - `stage6_reservations`: số **reservation Stage 6 đã xác nhận** (grant bền vững). Dispatch legacy của Stage 5 **không** được tính vào bộ đếm này.
   - `publish_calls`: số lần gọi `Publish` thực sự (cả Stage 5 lẫn Stage 6), tách biệt khỏi `puback` (chỉ tính sau khi broker trả PUBACK).
   - `stage6_retry_publish_calls`: số lượt gửi lại **được quản lý** của Stage 6 (`attempt > 1`), **bao gồm cả lượt thất bại**; không tính lượt đầu tiên.
   - `grants_skipped_before_network`: số lượt được cấp quyền nhưng **không đưa lên mạng** (transport chưa sẵn sàng sau reservation, hoặc quyền đã hết hạn trước lúc gửi).
   - Toàn bộ bộ đếm dùng **nhãn cố định**; tuyệt đối không dùng `command_id`/`sensor_id` làm nhãn metric, và không ghi nhận độ trễ suy ra từ HTTP origin cho các command không có origin đó.

---

### 4. Ngân sách tải (Capacity), Giữ chậm và Giới hạn cam kết (Capacity, Retention & Boundaries)

1. **Ngân sách tải lệnh đang xử lý (Outstanding Capacity):**
   - Trong Giai đoạn 6, giới hạn dung lượng hàng đợi `MaxPendingOutbox` (mặc định 10.000) được tái sử dụng làm chốt chặn cứng cho **tất cả các control command đang active (`pending`, `published`, `acknowledged`)**, tính qua câu truy vấn `SELECT count(*) FROM twin_commands WHERE target_entity_type='Sensor' AND status IN ('pending','published','acknowledged')`. Điều này bảo đảm các lệnh đã gửi (`published`) hoặc đã nhận receipt (`acknowledged`) đang chờ kết quả từ thiết bị không thể né tránh giới hạn dung lượng hệ thống.
   - Khi tắt Giai đoạn 6 (`STAGE6_ENABLED=false`), vị từ kiểm tra kế thừa cũ (`twin_outbox WHERE status='pending'`) được bảo toàn nguyên vẹn.
   - Dung lượng lưu biên nhận HTTP idempotency (`twin_command_requests`, giới hạn bởi `MaxReceipts`) hoàn toàn độc lập với dung lượng outbox. Lệnh successor do tiến trình đối soát sinh ra không phát sinh HTTP receipt nhưng vẫn chịu sự chi phối chặt chẽ của trần active command nói trên.
2. **Quy ước trạng thái Outbox Terminal:**
   - Ràng buộc kiểm tra `chk_twin_outbox_status` trên bảng `twin_outbox` chỉ cho phép ba giá trị: `status IN ('pending', 'published', 'failed')`.
   - Vòng đời nghiệp vụ của `twin_commands` và vòng đời vận chuyển của `twin_outbox` là hai state machine riêng biệt:
     - Khi command thành công (`succeeded`): outbox duy trì `status = 'published'` theo quy ước kết quả hiện hành.
     - Khi command thất bại (`failed`), hết hạn (`timeout`), hoặc bị hủy (`cancelled`): outbox được dừng vĩnh viễn bằng quy ước terminal tối thiểu hiện có: `twin_outbox.status = 'failed'`.
     - Điều kiện quét và cấp quyền dispatch của scheduler Stage 6 bao quát các command ở trạng thái `c.status IN ('pending', 'published')` với outbox `o.status IN ('pending', 'published')`, `c.expires_at > clock_timestamp()`, `o.next_attempt_at <= clock_timestamp()`, chưa nhận receipt thiết bị (`c.status != 'acknowledged'`), và `o.attempt_count < max_attempts`. Command ở trạng thái terminal (`succeeded`, `failed`, `timeout`, `cancelled`) bị loại trừ tuyệt đối bởi điều kiện `c.status`, do đó kể cả outbox của lệnh thành công có ở `status = 'published'` thì cũng không bao giờ bị quét lại hay gửi lại nhầm lẫn. Khi tắt Stage 6, vị từ cũ `c.status = 'pending' AND o.status = 'pending'` được giữ nguyên.
3. **Chính sách lưu giữ có giới hạn (Bounded Retention):**
   - Các bản ghi lệnh terminal, receipt và kết quả trễ tuân thủ thời hạn lưu trữ hữu hạn (retention window) theo chính sách vận hành của hệ thống, không xây dựng hệ thống event-store lưu trữ vô hạn.
4. **Giới hạn cam kết thực tế:**
   - Hệ thống cam kết vận chuyển tin cậy với cơ chế deduplication idempotent phía thiết bị, **không tuyên bố bảo đảm thực thi đúng một lần (exactly-once execution)**.
   - Cơ chế tự phục hồi chỉ áp dụng cho gián đoạn truyền thông hữu hạn trên cấu hình sampling; không bảo đảm tự động khắc phục các lỗi hỏng hóc vật lý của thiết bị hoặc mất nguồn hoàn toàn.

---

### 5. Đặc tả API Hủy an toàn (POST .../commands/{command_id}/cancel)

#### 5.1. Định tuyến, Quyền hạn và Phạm vi
- **Method & Path:** `POST /v1/digital-twins/{entity_id}/commands/{command_id}/cancel`
- **Xác thực & Phân quyền:**
  - Bắt buộc Bearer JWT hợp lệ; thiếu hoặc sai token trả về lỗi chuẩn `401 unauthorized`.
  - Phân quyền kiểm tra trực tiếp qua quan hệ `user_gateways` đối với Gateway sở hữu Sensor:
    - Vai trò `owner` hoặc `operator`: **Cho phép thực hiện hủy**.
    - Vai trò `viewer`: Từ chối với lỗi chuẩn `403 forbidden`.
    - Người dùng không thuộc Gateway, hoặc `entity_id`/`command_id` không tồn tại, hoặc không thuộc quyền quản lý: Từ chối với lỗi chuẩn `404 not_found` (chính sách an toàn: không làm lộ sự tồn tại của thiết bị/gateway khác).
    - Platform admin không tự bypass kiểm tra membership này.
- **Quy tắc Thân yêu cầu (Request Body) & Thương lượng Nội dung:**
  - **Đọc thân yêu cầu có giới hạn hữu hạn (Finite Body Reading):** Handler thực hiện đọc tối đa 1 byte từ `r.Body` trong thời hạn `ControlRequestTimeout` thông qua `io.LimitReader(r.Body, 1)`. Tuyệt đối không gọi `io.ReadAll` không giới hạn dung lượng chỉ để kiểm tra rỗng. Nếu đọc được bất kỳ byte nào ($n > 0$, kể cả khoảng trắng, `{}`, hay chuỗi rác), yêu cầu bị từ chối ngay với lỗi chuẩn `400 invalid_request`.
  - **Query parameters:** Cấm hoàn toàn query string; nếu `r.URL.RawQuery != ""` trả về lỗi chuẩn `400 invalid_request`.
  - **Thương lượng Accept & Content-Type:**
    - Hàm `negotiateAccept(c, mimeJSON)` kiểm tra header `Accept`; nếu không chấp nhận `application/json` thì từ chối với lỗi chuẩn `406 not_acceptable`.
    - Header `Content-Type` không bắt buộc khi body rỗng; nếu client có gửi header này thì bắt buộc phải là `application/json`, các media type khác bị từ chối với `415 unsupported_media_type`.
  - **Giới hạn thời gian:** Toàn bộ request được bao bọc trong `ControlRequestTimeout` (mặc định 5s) với read controller deadline.

#### 5.2. Tính ổn định khi gọi lặp (Idempotent Repeat) và Bất định Commit
- Thao tác cancel **không sử dụng `Idempotency-Key`** hay bảng `twin_command_requests` (để tránh xung đột với receipt của admission 200/202).
- Nếu command đã ở trạng thái `cancelled`:
  - Hệ thống kiểm tra lại tính hợp lệ của token và quyền membership hiện tại của caller.
  - Trả về kết quả **`200 OK` ổn định (deterministic)** cùng schema hủy ban đầu.
  - Kết quả 200 này được duy trì ổn định **kể cả sau khi `expires_at` gốc của command đã trôi qua**, vì tính bất biến của terminal state thắng quy trình expiry thông thường.
- **Xử lý Bất định Commit (Commit Ambiguity):** Nếu transaction hủy gặp lỗi cơ sở dữ liệu không chắc chắn đã commit hay chưa (`control_commit_uncertain`), handler trả về lỗi an toàn `503 service_unavailable` với mã lỗi `service_unavailable`. Client được hướng dẫn kiểm tra trạng thái lệnh qua `GET .../commands/{command_id}` hoặc gọi lại `POST .../cancel` (vốn an toàn tuyệt đối và trả về 200 nếu lệnh thực tế đã hủy).

#### 5.3. Ranh giới Hủy an toàn (Safe Cancel Boundary)
- **Điều kiện chấp thuận hủy (200 OK):**
  - Command thuộc đúng `entity_id` và Gateway được phân quyền.
  - Trạng thái command hiện tại là `pending`.
  - Hạn chót chưa trôi qua: `clock_timestamp() < expires_at`.
  - Lịch sử dispatch được quản lý bởi Stage 6 và **chưa từng có bất kỳ reservation nào**: `first_reserved_at IS NULL`, cả `twin_commands.attempt_count` và `twin_outbox.attempt_count` bằng `0` và khớp nhau, outbox còn `pending`, `retry_stop_reason IS NULL`.
- **Điều kiện từ chối hủy (409 Conflict):**
  - Command đã có ít nhất một reservation (`attempt_count >= 1`), hoặc đã ở trạng thái `published` hay `acknowledged`: Từ chối với **`409 command_not_cancellable`**.
  - Command là bản ghi cũ trước Stage 6 có `attempt_count = 0` (lịch sử UNKNOWN history): Từ chối với **`409 command_not_cancellable`** (fail-closed, không suy đoán là chưa gửi).
  - Command đã ở các trạng thái terminal khác (`succeeded`, `failed`, `timeout`): Từ chối với **`409 command_not_cancellable`**.
- **Tác động Database khi Hủy thành công:**
  - `twin_commands.status = 'cancelled'`.
  - `twin_commands.completed_at = now()`, `cancelled_at = now()`, `cancelled_by = actor_id`.
  - `twin_outbox.status = 'failed'` (outbox stopped theo quy ước terminal tối thiểu).
  - Đánh dấu chặn: Intent/version của command này bị chặn (`reconcile_blocked = true`) không cho phép tiến trình reconciliation tự động phát lại.
  - `desired_state` và `reported_state` trên `twin_states` **giữ nguyên vẹn 100%** (không rollback version; không ghi reported).
  - Không gửi bất kỳ lệnh remote abort nào xuống Gateway.

#### 5.4. Schema Phản hồi Chi tiết

**Thành công (200 OK — Lần đầu hoặc Gọi lặp lại):**
```json
{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "status": "cancelled",
  "desired_version": "13",
  "cancelled_at": "2026-11-14T14:30:00.000000Z",
  "status_url": "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001/commands/0195e18c-9fc1-7a42-9064-69ea49e63bf3"
}
```

**Từ chối khi không thể hủy an toàn (409 Conflict theo chuẩn APIErrorEnvelope):**
```json
{
  "error": {
    "code": "command_not_cancellable",
    "message": "command not cancellable",
    "request_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3"
  }
}
```

#### 5.5. Bổ sung trường DTO trên API List/Detail
Trên API `GET .../commands` và `GET .../commands/{command_id}`:
- `cancelled_at`: timestamp RFC3339 UTC có độ chính xác tối đa microsecond, hoặc `null` nếu command chưa bị hủy. Chuỗi JSON có thể bỏ các chữ số `0` thừa ở cuối phần thập phân; client không được yêu cầu đúng sáu chữ số.
- `attempt_count`: số nguyên biểu thị số lượt reservation đã cấp. Với **mọi** bản ghi `stage6_managed=false` (kể cả bản ghi Stage 5 có `control_version=1` và bản ghi legacy), trường này trả `null` để biểu thị lịch sử UNKNOWN; tuyệt đối không trả số `0` giả mạo.
- `acknowledged_at`: timestamp UTC hoặc `null` nếu chưa nhận receipt.
- `retry_stop_reason`: chuỗi nullable thuộc allowlist vận hành đã lưu, ví dụ `"retry_exhausted"` hoặc `"cancelled"`.
- `origin`: `"client"`, `"reconciliation"` hoặc `null`. Giá trị mặc định do migration cấp cho dữ liệu cũ không phải bằng chứng provenance; vì vậy bản ghi `stage6_managed=false` trả `null`.
- `root_command_id`, `parent_command_id`: UUID lineage nullable; chỉ được chiếu khi provenance Stage 6 đã được chứng minh.
- `reconcile_blocked`: block marker nullable cho chính command, chỉ khi lineage hợp lệ; `root_reconcile_blocked` và `root_auto_successors_used` là block/budget của root đã xác thực, không phải budget của child.
- `current_reconciliation`: decision/time của **Sensor state hiện tại**, chỉ có khi `command.desired_version` bằng `twin_states.desired_version`; không phải kết quả lịch sử của command cũ. Decision thuộc allowlist hữu hạn; không có snapshot thô hoặc payload.
- Khi PATCH thực sự tạo desired version mới, transaction xóa `last_reconcile_decision`/`last_reconciled_at` cũ cùng lúc cập nhật desired; replay và no-op không làm mới/che khuất lịch sử hiện tại.

DTO không chứa `cancelled_by`, payload/outbox bytes, raw SQL, receipt bytes hoặc chẩn đoán riêng của thiết bị.

#### 5.6. Trạng thái hiện thực (cập nhật 14/11/2026, Giai đoạn 6 — R4.3)

- **Đã hiện thực ở tầng domain + PostgreSQL:** transaction hủy nằm trong gói `digitaltwin`
  (`CancelControl`) theo đúng thứ tự khóa chung (Gateway SHARE → membership SHARE → Sensor UPDATE →
  Entity UPDATE → state UPDATE → command/outbox), không có I/O mạng. Ranh giới an toàn được chứng minh
  bằng bằng chứng DB: `stage6_managed = true`, `status = 'pending'`, chưa hết hạn theo
  `clock_timestamp()`, `first_reserved_at IS NULL`, và **cả hai** bộ đếm `twin_commands.attempt_count` và
  `twin_outbox.attempt_count` bằng 0 và khớp nhau; bất kỳ sai lệch nào (kể cả bản ghi legacy
  `stage6_managed = false` với `attempt_count = 0`) đều bị từ chối.
- **Đã hiện thực ở tầng HTTP (opt-in):** route `POST .../commands/{command_id}/cancel` chỉ được đăng ký
  khi cùng cờ đã kiểm tra `STAGE6_ENABLED=true` dùng cho repository. Khi cờ tắt, route trả `404`; gọi
  repository trực tiếp trong mode Stage 5 fail-closed bằng `ErrCommandNotCancellable`. Handler kiểm tra
  membership hiện tại như một mutation trước khi đọc/parse body, dùng một `ControlRequestTimeout` chung cho
  authorization + parse + transaction, đọc tối đa một byte body, cấm query string, thương lượng JSON và
  dùng `http.ResponseController` để ngắt một `Body.Read` thật bị block khi deadline hết hạn.
- **Hiệu ứng DB của lần hủy thành công:** `twin_commands.status = 'cancelled'`, `completed_at`,
  `cancelled_at`, `cancelled_by` (snapshot UUID của actor thật, không có actor hệ thống giả),
  `retry_stop_reason = 'cancelled'`, `reconcile_blocked = true`; `twin_outbox.status = 'failed'`
  (dừng outbox theo quy ước terminal tối thiểu). `desired_state`, `reported_state` và mọi version giữ
  nguyên 100%. Bằng chứng hủy là bất biến (trigger của migration 27).
- **Gọi lặp:** command đã `cancelled` trả lại đúng kết quả hủy đã lưu (cùng `cancelled_at`,
  `desired_version`, `status_url`) sau khi kiểm tra membership hiện tại, kể cả sau khi `expires_at`
  gốc đã trôi qua. Đường expiry hiện hữu không bao giờ biến `cancelled` thành `timeout`.
- **Ánh xạ lỗi HTTP:** `ErrCommandNotCancellable` → `409 command_not_cancellable`; `ErrNotFound` →
  `404 not_found`; `ErrForbidden` → `403 forbidden`; input sai → `400 invalid_request`; lỗi media type →
  `415 unsupported_media_type`; `Accept` không tương thích → `406 not_acceptable`; lỗi DB hoặc commit
  không chắc chắn (`ErrDependencyUnavailable`/`ErrControlCommitUncertain`) → `503 service_unavailable`,
  không bao giờ trả kết quả thành công giả. Tất cả dùng chung `APIErrorEnvelope` có `request_id`.
- **Nghiệm thu local R4.3 (14/11/2026):** đã kiểm chứng normal/business/negative, ranh giới
  cancel-vs-reservation, late result/root block, HTTP bounds/auth/error/projection và affected MQTT
  Stage 5 + Stage 6 retry regression trên fixture sở hữu; các required native test PASS, không skip và
  cleanup được xác minh. Đây là local source qualification tại HEAD + dirty tree, **không phải** remote
  CI, board/HIL, production certification hoặc rollout approval.
- **Đã hiện thực một phần (6-R5.1):** parser/serializer receipt riêng và backend `AcceptControlReceipt`
  transaction đã được kiểm tra trên PostgreSQL thật. Phạm vi R5.1 không bao gồm Gateway hoặc MQTT
  runtime; các phần Gateway thuộc R5.2 được ghi riêng bên dưới.
- **Đã hiện thực một phần (6-R5.2):** Gateway simulator opt-in `GATEWAY_STAGE6_ENABLED` tạo canonical
  receipt từ nguyên command bytes, lưu receipt/hash/timestamp nguyên tử cùng reservation trước apply,
  enqueue không-blocking và resend receipt/result qua một bounded fair response worker. Prepared recovery
  dùng receipt đã lưu; legacy rows không được cấp receipt lịch sử. Khi Stage 6 tắt, receipt evidence cũ
  vẫn giữ nguyên trong storage nhưng không được phát; periodic retry scan vẫn có lượt chạy hữu hạn khi
  response queue liên tục có dữ liệu và Stage 5 giữ ngân sách scan 8 result. Đây không phải exactly-once.
 - **Đã hiện thực (6-R5.3, local acceptance 17/11/2026):** receipt và result dùng chung bounded `ResultConsumer`; consumer
  phân loại hẹp `message_type`, rồi gọi parser strict type-specific trước repository tương ứng.
  Retained delivery bị từ chối cả khi transport admission và tại queue consumer; receipt có counters
  riêng, không thêm result/timeline latency observation. Server chỉ bật receipt repository path qua
  `STAGE6_ENABLED`; mặc định deployment vẫn `false`. Focused consumer/server tests PASS.
 - **Nghiệm thu native 6-R5.3:** Stage 5 MQTT fixture chạy hết lifecycle Stage 5, Stage 6 retry và
   `TestNativeMQTTStage6GatewayReceiptRoundTrip`, không skip; cleanup container/network/volume/workdir
   được xác minh. Bài test dùng Gateway simulator thật (SQLite, TLS MQTT) và composed backend consumer
   với PostgreSQL fixture: receipt canonical được publish/nhận và khớp bytes SQLite trong khi apply
   đang bị giữ; trạng thái reported chưa đổi. Sau khi thả apply, result thành công cập nhật reported;
   phát lại command nguyên bản chỉ gửi lại receipt/result bytes đã lưu, không gọi sampler thêm lần nữa.
   Intent fixture PASS 33 test top-level, không skip, 5 scenario cleanup; P3 require-run PASS
   (`PAPER_P3_ACCEPTED`, cleanup 17 containers/1 network/7 volumes). Affected Go race/static/build,
   Python guards và ARMv7 build PASS. Đây là local source qualification tại HEAD + dirty tree, không
   phải remote CI, board/HIL, production certification hoặc rollout approval.
 - **Lịch sử lỗi được giữ nguyên:** một lượt trước đó báo Mosquitto exit code 3 và stdout Docker logs
   rỗng; cause cụ thể không được xác định và lỗi không tái hiện ở các lượt hoàn chỉnh sau đó. Fixture
   hiện lưu combined broker logs cùng state/mount access diagnostics nếu startup/port mapping lỗi;
   không xem lịch sử này là blocker hiện tại hoặc đổi thành một root cause chưa được chứng minh.
   Bằng chứng được tổng hợp trong các báo cáo nghiệm thu kỹ thuật và tài liệu nguồn.
- **R6.1 đã hiện thực (local unit/source scope):** strict parser/serializer riêng cho query/snapshot và
  Gateway Stage 6 opt-in read-only responder sau recovery/subscription readiness. Snapshot đọc
  committed SQLite configuration và actual sampler dưới cùng apply mutex; queue/TTL hữu hạn, không lưu
  snapshot, không tạo command result/receipt. Các cờ vẫn mặc định `false`.
- **R6.2 implementation và native intent DB qualification:** backend bounded discovery/cooldown reservation
  qua `twin_states.next_query_at`, một publisher typed query path, fixed scan/publish workers, tối đa 32
  correlation RAM-only, snapshot strict demux và current-context recheck. Snapshot chỉ được chuyển tiếp
  như untrusted candidate; không ghi snapshot/desired/reported/command/result hay tạo successor. Focused
  mqttcontrol tests, package race/vet/lint/build pass. Native `stage5_intent_fixture.py` require-ran 34
  top-level tests (including query due traversal, durable cooldown across repository reconstruction,
  and stale-context rejection), no skips; fresh/upgrade and three preflight-refusal scenarios all passed
  with cleanup verified.
- **R6.3 và R7.1–R7.3 đã nghiệm thu:** R6.3 native TLS/DynSec query → Gateway SQLite+sampler readback;
  R7.1 transaction/decision; R7.2 runtime composition, REST projection (`origin`, `root_command_id`,
  `parent_command_id`, `reconcile_blocked`, `current_reconciliation`) và metrics nhãn cố định; R7.3 native
  convergence trên fixture sở hữu (mất result $\to$ timeout root $\to$ snapshot readback $\to$ tạo successor
  với ID/version mới $\to$ Gateway áp dụng thành công $\to$ cập nhật reported). Lệnh hủy trực tiếp qua MQTT
  không tồn tại (no remote abort).
- **R8 tích hợp CI và R2 follow-up đã hoàn tất:** CI từ xa qua GitHub Actions run 44501549344 đã PASS
  toàn bộ các job `stage5-control-integration` (P3 baseline, P3 opt-in Stage 6, MQTT retry/convergence),
  `dynsec-bootstrap-tests`, `migration-check` và `native-compose-smoke` trên commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f`.
  Kiểm chứng nâng cấp schema migration 26→27 và script retention PASS trên fixture cục bộ.
- **Trạng thái tổng thể Stage 6 vẫn PARTIAL:** Do script retention còn finding review chưa đóng,
  target demo/preflight/restore và rollout shared/live NOT RUN, board AM5728 / real sensor / HIL DEFERRED / NOT RUN.

---

### 6. Đặc tả Envelope MQTT Device Receipt (command_receipt)

#### 6.1. Chủ đề và Tham số Vận chuyển
- **Topic:** `gateways/<gateway_id>/responses/<command_id>` (tái sử dụng namespace responses hiện hành, không tạo topic mới).
- **QoS & Retain:** QoS 1, `retain: false`.
- **Kích thước tối đa:** 8192 bytes (khớp `MaxControlPayloadBytes`; kích thước thông thường ~300 bytes).

#### 6.2. Cấu trúc Envelope `command_receipt`
Bắt buộc có đầy đủ 8 trường sau, không chấp nhận trường lạ, không có trailing comma, không có giá trị null:
1. `protocol_version`: số nguyên `1` (JSON number).
2. `message_type`: chuỗi cố định `"command_receipt"`.
3. `command_id`: chuỗi UUID canonical 36 ký tự (phải khớp chính xác với `<command_id>` trên topic).
4. `gateway_id`: chuỗi định danh gateway (phải khớp chính xác với `<gateway_id>` trên topic).
5. `entity_id`: chuỗi URN Sensor `urn:ngsi-ld:Sensor:<gateway_id>:<sensor_id>`.
6. `desired_version`: chuỗi JSON chứa số nguyên BIGINT không âm (khớp với `desired_version` của command).
7. `received_at`: chuỗi thời gian RFC3339 UTC có đuôi 'Z' ghi nhận thời điểm Gateway lưu bền vững lệnh vào SQLite.
8. `command_hash`: chuỗi hex chữ thường 64 ký tự biểu diễn mã băm SHA-256 của **toàn bộ payload bytes nguyên bản của command nhận được**.

#### 6.3. Ví dụ Payload Chuẩn
```json
{
  "protocol_version": 1,
  "message_type": "command_receipt",
  "command_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "desired_version": "13",
  "received_at": "2026-11-14T14:30:01.123456Z",
  "command_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

#### 6.4. Xử lý tại Gateway và Backend
- **Gateway Simulator:**
  - Khi nhận command qua MQTT, Gateway kiểm tra tính hợp lệ và thực hiện ghi bền vững vào SQLite (`ReserveControl`).
  - Canonical receipt bytes được tạo và lưu trữ bền vững trong SQLite TRƯỚC KHI áp dụng cấu hình (apply).
  - Khi nhận lại lệnh trùng lặp hoặc khi phục hồi sau khởi động lại (reopen), Gateway tái sử dụng nguyên văn `received_at` và bytes ban đầu đã lưu để phát lại.
  - Gateway phát `command_receipt` lên topic responses (QoS 1) theo lịch gửi có giới hạn hữu hạn trong TTL.
  - Tuyệt đối không thực hiện network I/O trong transaction SQLite.
  - Việc mất receipt trên đường truyền không được phép chặn quá trình apply và gửi kết quả `command_result`.
- **Backend Acceptance:**
  - Backend đối soát topic identity, format JSON, và xác minh `command_hash` khớp với mã băm của `twin_outbox.payload_bytes`.
  - Transaction cập nhật `twin_commands.status = 'acknowledged'`, ghi nhận `acknowledged_at`, lưu bằng chứng receipt và dừng retry outbox.
  - **Tuyệt đối không cập nhật `reported.configuration`** và không thay đổi `expires_at`.
  - **Định nghĩa Duplicate Receipt:** Duplicate receipt có nghĩa là **trùng khớp hoàn toàn canonical receipt bytes và receipt hash** (nếu cùng `command_id` nhưng khác `received_at` hoặc nội dung thì coi là xung đột `control_result_conflict`).
  - Receipt đến khi command đã ở terminal state (`succeeded`, `failed`, `timeout`, `cancelled`): bị từ chối / ghi nhận late evidence, không làm sống lại lệnh và không tạo thêm lịch sử gói tin vô hạn.
  - **Độc lập Kết quả & Không có Vòng lặp ACK:** `command_result` hoàn toàn độc lập với `command_receipt`; nếu result đến trước thì backend vẫn cập nhật `succeeded` bình thường. Backend không bao giờ gửi tin nhắn ACK ngược lại cho receipt.

---

### 7. Đặc tả Envelope MQTT Đối soát (configuration_query & configuration_snapshot)

#### 7.1. Định tuyến Chủ đề và Phân quyền
- **Query Downlink:** `gateways/<gateway_id>/commands/<request_id>`
- **Snapshot Uplink:** `gateways/<gateway_id>/responses/<request_id>`
- **Nguyên tắc an toàn:** Tái sử dụng trọn vẹn topic pattern `commands/#` và `responses/#` hiện có của Mosquitto DynSec. Không mở thêm topic mới, không dùng topic wildcard ngoài tầm kiểm soát.
- **QoS & Retain:** QoS 1, `retain: false`.
- **Kích thước tối đa:** 1024 bytes.

#### 7.2. Envelope `configuration_query` (Backend → Gateway)
Gửi yêu cầu truy vấn cấu hình thực tế của một Sensor cụ thể. Mỗi Sensor tại một thời điểm chỉ cho phép tối đa một query in-flight.
Bao gồm đúng 7 trường sau:
1. `protocol_version`: số nguyên `1`.
2. `message_type`: chuỗi cố định `"configuration_query"`.
3. `request_id`: chuỗi UUID v4/v7 do backend sinh ngẫu nhiên cho từng yêu cầu.
4. `gateway_id`: chuỗi định danh gateway.
5. `entity_id`: chuỗi URN Sensor mục tiêu.
6. `issued_at`: chuỗi thời gian RFC3339 UTC thời điểm phát hành.
7. `expires_at`: chuỗi thời gian RFC3339 UTC hạn chót phản hồi (thời hạn ngắn, ví dụ $5\,\text{s}$).

**Ví dụ Payload `configuration_query`:**
```json
{
  "protocol_version": 1,
  "message_type": "configuration_query",
  "request_id": "0195e18d-1111-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "issued_at": "2026-11-14T14:35:00.000000Z",
  "expires_at": "2026-11-14T14:35:05.000000Z"
}
```

#### 7.3. Envelope `configuration_snapshot` (Gateway → Backend)
Gateway phản hồi trạng thái cấu hình thực tế đọc trực tiếp từ bộ điều khiển. Bao gồm đúng 10 trường sau:
1. `protocol_version`: số nguyên `1`.
2. `message_type`: chuỗi cố định `"configuration_snapshot"`.
3. `request_id`: chuỗi UUID khớp với `request_id` của query.
4. `gateway_id`: chuỗi định danh gateway.
5. `entity_id`: chuỗi URN Sensor mục tiêu.
6. `watermark_version`: chuỗi JSON chứa số nguyên BIGINT không âm biểu thị version cao nhất đã commit trong SQLite store (ví dụ `"12"`, hoặc `"0"` nếu chưa có cấu hình hoặc lỗi store).
7. `configuration_state`: chuỗi biểu thị trạng thái cấu hình, chỉ nhận một trong ba giá trị: `"configured"`, `"unconfigured"`, hoặc `"unavailable"`.
8. `durable_sample_interval_us`: số nguyên microsecond biểu thị interval đã lưu bền vững trong SQLite store; hoặc `null` nếu trạng thái là `"unconfigured"` hoặc `"unavailable"`.
9. `actual_sample_interval_us`: số nguyên microsecond biểu thị cadence lấy mẫu thực tế đang chạy trên sampler (`c.sampler.ActiveSampleInterval()`), báo cáo trung thực tốc độ đang chạy kể cả khi chưa có cấu hình; hoặc `null` nếu sampler gặp sự cố phần cứng không đọc được.
10. `readback_at`: chuỗi thời gian RFC3339 UTC thời điểm đọc snapshot.

**Ví dụ Payload `configuration_snapshot` (Đã cấu hình hợp lệ):**
```json
{
  "protocol_version": 1,
  "message_type": "configuration_snapshot",
  "request_id": "0195e18d-1111-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "watermark_version": "12",
  "configuration_state": "configured",
  "durable_sample_interval_us": 666667,
  "actual_sample_interval_us": 666667,
  "readback_at": "2026-11-14T14:35:01.500000Z"
}
```

**Ví dụ Payload `configuration_snapshot` (Chưa từng cấu hình / Trống):**
```json
{
  "protocol_version": 1,
  "message_type": "configuration_snapshot",
  "request_id": "0195e18d-1111-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "watermark_version": "0",
  "configuration_state": "unconfigured",
  "durable_sample_interval_us": null,
  "actual_sample_interval_us": 1000000,
  "readback_at": "2026-11-14T14:35:01.500000Z"
}
```

**Ví dụ Payload `configuration_snapshot` (Sự cố / Lỗi lưu trữ / Không khả dụng):**
```json
{
  "protocol_version": 1,
  "message_type": "configuration_snapshot",
  "request_id": "0195e18d-1111-7a42-9064-69ea49e63bf3",
  "gateway_id": "gateway_001",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "watermark_version": "0",
  "configuration_state": "unavailable",
  "durable_sample_interval_us": null,
  "actual_sample_interval_us": null,
  "readback_at": "2026-11-14T14:35:01.500000Z"
}
```

#### 7.4. Quy tắc Bất biến và Cơ chế Xử lý Đối soát
1. **Phân biệt Rõ Store Interval và Actual Sampler Cadence:**
   - Trường `durable_sample_interval_us` lấy từ cấu hình đã commit trong SQLite store (`c.out.Configuration()`).
   - Trường `actual_sample_interval_us` **bắt buộc phải đọc trực tiếp từ bộ lấy mẫu thực tế** (`c.sampler.ActiveSampleInterval()`) dưới khóa bảo vệ nhất quán (`c.mu.Lock()`).
   - Gateway tuyệt đối không lấy giá trị target desired từ command để gán vào snapshot.
   - Nếu SQLite store chưa có cấu hình, sampler vẫn có thể đang chạy với cadence mặc định (ví dụ $1.000.000\,\mu\text{s}$): snapshot báo cáo trung thực `actual_sample_interval_us: 1000000` và `durable_sample_interval_us: null`, tuyệt đối không điền 0 giả mạo.
2. **Tính Tạm thời của Tương quan (Transient Correlation in RAM):**
   - Sự tương quan giữa `request_id` và câu hỏi đối soát được lưu tạm trong bộ nhớ RAM của tiến trình backend với TTL hữu hạn và dung lượng giới hạn.
   - Không lưu trữ correlation query trong database.
   - Nếu tiến trình backend restart, toàn bộ correlation trong RAM bị hủy bỏ. Mọi snapshot trả về có `request_id` không khớp đều bị bỏ qua an toàn mà không gây lỗi.
3. **Không lưu Snapshot trong SQLite:**
   - Gateway simulator không lưu trữ các snapshot này vào SQLite store; việc tạo snapshot hoàn toàn là thao tác đọc trạng thái tức thời dưới khóa apply của controller.
4. **Không Đột biến Trực tiếp Trạng thái:**
   - **Tuyệt đối không bao giờ dùng `configuration_snapshot` để ghi đè trực tiếp `reported.configuration` hay `desired_state` trong DB.**
   - Snapshot chỉ là thông tin quan sát đầu vào cho transaction đối soát. Mọi sự hội tụ cấu hình thực tế lên `reported.configuration` bắt buộc phải thông qua chu trình xác nhận kết quả của một command hợp lệ.

---

### 8. Quy tắc Chung cho Tất cả các Envelopes Mới (Shared Strict Envelope Rules)

Mọi envelope JSON mới trong Giai đoạn 6 (`command_receipt`, `configuration_query`, `configuration_snapshot`) đều phải tuân thủ nghiêm ngặt các quy tắc xác thực sau:
1. **Từ chối khóa lạ và khóa trùng lặp:** Parser chỉ chấp nhận đúng các trường được khai báo trong schema allowlist của từng loại message; bất kỳ trường lạ nào (unknown fields) hoặc khóa bị lặp lại (duplicate keys) đều dẫn đến lỗi từ chối `invalid_input`.
2. **Quy tắc Null và Dữ liệu dư thừa:** Không chấp nhận giá trị `null` ở bất kỳ trường nào, ngoại trừ các trường có điều kiện nullable đã được định nghĩa rõ ràng (`durable_sample_interval_us`, `actual_sample_interval_us`). Từ chối mọi tài liệu JSON có chứa dữ liệu thừa phía sau (trailing characters / multiple JSON documents qua `d.Token() == io.EOF`).
3. **Quy chuẩn Kiểu dữ liệu & UTF-8:**
   - Số nguyên (integers): Phải là JSON numbers nguyên thủy, không chứa phần thập phân hoặc số mũ khoa học (exponent).
   - Version: Phải là JSON string chứa các chữ số thập phân không âm trong khoảng PostgreSQL BIGINT `[0, 2^63 - 1]`.
   - Chuỗi: Bắt buộc là UTF-8 hợp lệ, không chứa ký tự điều khiển (ASCII < 0x20) hoặc ký tự null `\u0000`.
   - Khoảng thời gian (intervals): Số nguyên microsecond dương trong khoảng an toàn `[10000, 2^53]`.
4. **Định danh UUID và Topic Matching:**
   - Trường `command_id` và `request_id` phải là UUID canonical định dạng 36 ký tự hex-hyphenated không nil.
   - Định danh `gateway_id` và `entity_id` phải khớp chính xác giữa nội dung envelope, đường dẫn MQTT topic, và context cơ sở dữ liệu.
5. **Dấu thời gian và Thời hạn Cục bộ:**
   - Timestamps bắt buộc định dạng RFC3339 UTC với hậu tố 'Z', độ chính xác microsecond, đồng bộ với miền giá trị timestamp của Go/PostgreSQL (`0001-01-01T00:00:00.000000Z` đến `9999-12-31T23:59:59.999999Z`).
   - **Thời hạn cục bộ của Server quyết định:** Việc chấp nhận snapshot hay receipt hoàn toàn do mốc thời gian cục bộ của backend quyết định (`clock_timestamp() < deadline`). **Tuyệt đối không đo độ trễ hoặc tính toán thời hạn bằng phép trừ đồng hồ giữa hai máy khác nhau (cross-machine clock subtraction)** và không lấy thời gian của Gateway làm hạn chót của Server.
6. **Cờ MQTT Retain:** Enforce `retain == false` khi nhận; mọi gói tin có cờ retain đều bị từ chối ngay lập tức.
7. **Giới hạn Kích thước & TTL:** `configuration_query` và `configuration_snapshot` có trần kích thước 1024 bytes; `command_receipt` có trần kích thước 8192 bytes. Tất cả các yêu cầu đều có TTL hữu hạn.

---

### 9. Đặc tả Đối soát và Dòng dõi Command (Reconciliation Decision Engine & Lineage)

#### 9.1. Bảng Quyết định Đối soát (Reconciliation Decision Table)

**Thứ tự ưu tiên của các chốt chặn (Guard Precedence):**
1. **Active Busy Guard:** Nếu Sensor đang có command active (`pending`, `published`, `acknowledged`) $\to$ **NO_ACTION** (bỏ qua, không tạo command cạnh tranh).
2. **Precondition Guard:** Chỉ áp dụng auto recovery cho Sensor đã được provision VÀ đã có **committed user intent thực sự** trong cơ sở dữ liệu. Tuyệt đối không tự động sinh successor từ initial desired defaults khi chưa từng có user intent nào được commit.
3. **Safety & Block Guards (Ưu tiên cao hơn các nhánh mismatch):**
   - Gateway version cao hơn server, hoặc store SQLite hỏng (`configuration_state == "unavailable"`), hoặc không thể xác minh nguồn gốc $\to$ **UNCONFIRMED / MANUAL**. Không tự nâng server watermark theo số Gateway tự khai.
   - Lỗi unsupported / unknown Sensor / corrupt store hoặc readback configured nhưng thiếu `actual_sample_interval_us` $\to$ **MANUAL ACTION**, không được tự động tạo successor. `UNKNOWN_SENSOR`, `UNSUPPORTED_CONFIGURATION`, `APPLY_FAILED` và error code lưu không nhận diện được fail closed; timeout/mất result và `COMMAND_EXPIRED`/`STALE_DESIRED_VERSION` không bị nhầm thành lỗi permanent.
   - Cancelled block guard: Nếu intent của desired version hiện tại đã bị hủy (`reconcile_blocked == true`) $\to$ **BLOCKED**.
   - Budget guard: Nếu root intent đã sử dụng hết ngân sách tự động (`auto_successors_used >= max_auto_successors`) $\to$ **BLOCKED**.
4. **Convergence Check:** Desired == Reported == Durable == Actual VÀ Desired version == Watermark $\to$ **CONVERGED**. Version xác nhận cấu hình lấy từ `reported.configuration.desired_version`; `twin_states.reported_version` là bộ đếm báo cáo tổng hợp, không phải config version.
5. **Recovery Branches (Chỉ khi vượt qua toàn bộ các guards trên):**
   - Thiết bị đã áp dụng nhưng thiếu xác nhận reported $\to$ **CREATE_SUCCESSOR** (không lấy snapshot ghi reported).
   - Watermark thấp hơn / unconfigured / readback lệch $\to$ **CREATE_SUCCESSOR**.
   - Cùng version nhưng sai lệch cấu hình $\to$ **CREATE_SUCCESSOR** với version mới cao hơn.

| Tình huống khảo sát | Điều kiện kiểm tra thực tế | Quyết định của Engine | Hành động tiếp theo |
| --- | --- | --- | --- |
| **1. Đang có lệnh Active** | Sensor đang có command ở trạng thái `pending`, `published`, hoặc `acknowledged` | **NO_ACTION** | Bỏ qua, không tạo command cạnh tranh. Để command active tự xử lý qua chu trình retry/result/expiry. |
| **2. Đã Hội tụ Hoàn toàn** | `desired.sample_interval_us == reported.configuration.sample_interval_us == snapshot.durable_sample_interval_us == snapshot.actual_sample_interval_us` VÀ `desired_version == snapshot.watermark_version` | **CONVERGED** | Không tạo command mới, không tăng version. Ghi nhận thời điểm đối soát thành công `last_reconciled_at = now()`. |
| **3. Thiết bị đã Áp dụng nhưng Thiếu Xác nhận** | Gateway đã apply thành công (`durable == actual == desired` VÀ `watermark == desired_version`), nhưng server chưa xác nhận `reported.configuration` (do result bị mất hoặc đến sau timeout) | **CREATE_SUCCESSOR** (nếu intent chưa bị cancel và còn budget) | **Tuyệt đối không lấy snapshot ghi reported**. Tạo một successor command chính quy với desired version mới để thiết bị xác nhận lại chuẩn tắc. |
| **4. Sai lệch Cadence / Thiếu Cấu hình** | Gateway có watermark thấp hơn server, hoặc `configuration_state == "unconfigured"`, hoặc `actual_sample_interval_us != desired`; intent chưa bị cancel và còn budget | **CREATE_SUCCESSOR** | Tạo một successor command với version mới cao hơn (`desired_version + 1`) và cùng desired interval để đưa thiết bị về cấu hình mong muốn. |
| **5. Cùng Version nhưng Sai lệch Cấu hình** | Gateway watermark bằng `desired_version` của server nhưng `actual` hoặc `durable` khác với desired interval | **CREATE_SUCCESSOR** | Vì Gateway từ chối lệnh có version nhỏ hơn hoặc bằng watermark, bắt buộc phải tăng desired version (`desired_version + 1`) để tạo successor. |
| **6. Gateway Version cao hơn / Dữ liệu Bất thường** | Gateway watermark cao hơn `desired_version` của server, hoặc store SQLite hỏng (`configuration_state == "unavailable"`), hoặc không xác minh được nguồn gốc | **UNCONFIRMED / MANUAL** | **Tuyệt đối không tự nâng server watermark theo số Gateway tự khai**. Đánh dấu trạng thái unconfirmed, ghi log cảnh báo và yêu cầu operator can thiệp đối soát. |
| **7. Intent đã bị Hủy (Cancelled)** | Intent của desired version hiện tại đã bị hủy an toàn (`reconcile_blocked == true`) | **BLOCKED** | Không tạo successor, không tự động phát lại intent đã hủy. Quyết định hủy của người dùng được bảo toàn. |
| **8. Hết Ngân sách Tự động (Budget Exhausted)** | Root intent này đã sử dụng hết ngân sách tự động (`auto_successors_used >= max_auto_successors`) | **BLOCKED** | Ngừng tạo successor tự động để tránh lặp vô hạn. Chờ người dùng can thiệp bằng PATCH mới. |
| **9. Snapshot Lỗi thời / Không khớp** | Snapshot có `request_id` không khớp, hoặc đã hết hạn, hoặc context đã thay đổi trong khi chờ | **DISCARD** | Hủy bỏ snapshot an toàn, không thực hiện bất kỳ thay đổi nào trong cơ sở dữ liệu. |

#### 9.2. Dòng dõi Command (Command Lineage) & Ngân sách Tự phục hồi (Budget)
1. **Trường Dữ liệu Dòng dõi (Lineage Fields):**
   - `origin`: enum string phân định nguồn gốc lệnh, nhận giá trị `"client"` (lệnh phát sinh từ PATCH của người dùng) hoặc `"reconciliation"` (successor do hệ thống tự sinh).
   - `root_command_id`: UUID của command gốc ban đầu do người dùng phát hành qua PATCH.
   - `parent_command_id`: UUID của command trực tiếp trước đó trong chuỗi phục hồi.
   - `original_actor_id`: UUID của người dùng đã thực hiện PATCH ban đầu (bảo toàn thông tin kiểm toán; hệ thống không giả mạo actor hệ thống thành người dùng).
2. **Ngân sách Khởi đầu và Khóa Chặn (Budget Bounds):**
   - **Tối đa 1 successor tự động:** Mỗi root intent của người dùng chỉ được phép cấp tối đa một command phục hồi tự động (`max_auto_successors = 1`).
   - Ngân sách được lưu bền vững trong cơ sở dữ liệu gắn liền với root intent.
   - Successor command không được tự sinh ra successor tiếp theo (không tạo chuỗi vô hạn).
   - Nếu successor command tiếp tục bị timeout hoặc failed, hệ thống dừng lại ở trạng thái chưa hội tụ và hiển thị trạng thái cho người vận hành.
3. **Giao dịch Tạo Successor Nguyên tử:**
   - Trong cùng một SQL transaction tuân thủ thứ tự khóa chuẩn:
     - Tăng `desired_version` lên 1 đơn vị trên `twin_states`.
     - Tạo command mới trong `twin_commands` với cùng `sample_interval_us`, version mới, command ID mới, payload mới, `origin = 'reconciliation'`, `root_command_id`, `parent_command_id`.
     - Tạo bản ghi outbox mới trong `twin_outbox`.
     - Đánh dấu đã tiêu thụ ngân sách (`auto_successors_used = 1`).
   - Nếu transaction bị rollback, ngân sách không bị tiêu hao.
   - Tái khởi động backend hoặc snapshot đến lặp lại không thể sinh ra successor thứ hai.
   - Successor **không tạo ra root mới và không tạo ra HTTP receipt giả** trong `twin_command_requests`.

#### 9.3. Quy trình Người dùng Mở lại Intent sau khi Cancel hoặc Hết Budget
- Khi một intent đã bị hủy (`cancelled`) hoặc hết ngân sách tự động (`blocked`):
  - Người dùng muốn tái áp dụng cấu hình bắt buộc phải gửi một **PATCH hợp lệ mới** tới `/v1/digital-twins/{entity_id}/desired-state` với `Idempotency-Key` mới và `expected_desired_version` khớp với version hiện hành trên server.
  - Khi request PATCH mới được chấp thuận (`202 Accepted`): Một **root intent hoàn toàn mới** được tạo ra, tăng version, cấp command ID mới, xóa cờ cancel block cũ và cấp lại ngân sách successor mới.
  - Một yêu cầu replay cùng idempotency key cũ hoặc một request không thay đổi (`status = "unchanged"`, 200 OK) **tuyệt đối không được xóa cờ cancel block hoặc đặt lại budget**.
  - **Trường hợp Re-assertion cùng giá trị interval khi đã bị cancel:** Nếu giá trị interval mong muốn trùng với giá trị cũ nhưng intent trước đó đã bị cancel, request PATCH mới với key mới và version hiện tại sẽ được chấp thuận tạo command mới (`202 Accepted`, tăng version), không bị nhầm lẫn trả về 200 unchanged.

---

### 10. Quản lý Điều phối Query và Giới hạn Tài nguyên (Query Discovery & Resource Bounds)

1. **Giới hạn Tải và Chu kỳ Điều phối Query:**
   - **Số lượng worker & Hàng đợi:** Bộ điều phối đối soát sử dụng các giá trị mặc định cụ thể: `ReconcileWorkers = 1`, `ReconcileQueueCapacity = 32`.
   - **Trần query toàn cục:** Tối đa `ReconcileGlobalOutstandingMax = 32` query in-flight đồng thời trên toàn hệ thống; mỗi Sensor chỉ cho phép duy nhất một `configuration_query` đang chờ phản hồi.
   - **Quét phân trang hữu hạn:** Discovery pass quét theo lô phân trang `ReconcileScanBatch = 16` sensors cho mỗi lượt quét.
   - **Thời hạn Query (Query TTL):** Hạn chót của một query là `ReconcileQueryTTL = 5s`. Quá thời hạn này, correlation trong RAM tự động hết hiệu lực.
   - **Thời gian chờ giữa các lần query (Cooldown):** Sau khi query một sensor, sensor đó rơi vào trạng thái cooldown tối thiểu `ReconcileCooldown = 30s` trước khi được phép query lại.
   - **Tần suất quét định kỳ:** Vòng lặp nền thực hiện quét discovery với chu kỳ `ReconcileScanInterval = 10s`.
   - **Lưu trữ bền vững:** Các thông tin `next_query_at`, `last_reconciled_at`, và ngân sách đối soát được lưu bền vững theo Sensor/Root intent trong cơ sở dữ liệu; correlation UUID `request_id` chỉ lưu tạm thời trong RAM.
2. **Quản lý Tương quan Tạm thời (Ephemeral Correlation in RAM):**
   - Sự tương quan giữa `request_id` và câu hỏi đối soát được lưu tạm trong bộ nhớ RAM của tiến trình backend qua bảng băm có giới hạn kích thước tối đa.
   - Nếu tiến trình backend restart, toàn bộ correlation trong RAM bị xóa sạch. Mọi snapshot muộn trả về đều bị bỏ qua an toàn mà không có tác dụng phụ.
   - Không sử dụng goroutine hay timer riêng lẻ cho từng query.
3. **Tự động Tìm lại Thiết bị khi Reconnect:**
   - Khi Gateway kết nối lại (reconnect), vòng quét định kỳ của backend sẽ tự động phát hiện trạng thái cần đối soát và gửi query mà không đòi hỏi dịch vụ presence chuyên biệt.
4. **Hiển thị Vận hành trên REST và Metrics:**
   - API `GET .../commands/{command_id}` trả về thông tin `origin`, `root_command_id`, `parent_command_id`.
   - **Giới hạn Cardinality của Metrics:** Toàn bộ metrics đối soát sử dụng các counter có nhãn cố định (`reconcile_queries_total`, `reconcile_converged_total`, `reconcile_successors_total`, `reconcile_blocked_total`, `reconcile_conflicts_total`). Tuyệt đối không dùng `command_id` hay `sensor_id` làm nhãn metric để tránh bùng nổ bộ nhớ.
   - **Đo lường Độ trễ:** Đối với successor command do hệ thống sinh ra (không có HTTP origin), trường độ trễ từ lúc nhận yêu cầu đến lúc dispatch được ghi nhận là `"unavailable"` hoặc `"missing"`, **tuyệt đối không điền số 0 giả mạo**.

---

### 11. Ma trận Tính năng và Quy trình Hạ cấp An toàn (Feature Matrix & Rollback Safety)

#### 11.1. Cấu hình Cờ Tính năng Đề xuất
- **Phía Backend:** `STAGE6_ENABLED=false` (mặc định tắt). Cờ này chỉ hợp lệ khi `STAGE5_ENABLED=true`. Nếu cấu hình `STAGE6_ENABLED=true` trong khi `STAGE5_ENABLED=false`, tiến trình backend **bắt buộc phải fail startup ngay lập tức với thông báo lỗi rõ ràng** (`invalid configuration: STAGE6 requires STAGE5`), tuyệt đối không được âm thầm bỏ qua hay tắt ngầm.
- **Phía Gateway Simulator:** `GATEWAY_STAGE6_ENABLED=false` (mặc định tắt). Tương tự, nếu `GATEWAY_STAGE6_ENABLED=true` trong khi `GATEWAY_CONTROL_ENABLED=false`, Gateway simulator bắt buộc fail startup rõ ràng.

#### 11.2. Ma trận Phối hợp Triển khai (Deployment Matrix)

| Backend Stage 6 | Gateway Stage 6 | Hành vi Hệ thống & Khả năng Tương thích |
| :---: | :---: | --- |
| **OFF** | **OFF** | **Chế độ chuẩn Stage 5 hiện hành:** Chạy hoàn toàn theo cơ chế strict command-result v1; không có receipt, không có API cancel, không có query đối soát. |
| **ON** | **OFF** | **Chế độ An toàn Bán phần:** Backend áp dụng lịch retry bền vững có jitter và hỗ trợ API cancel. Khi Backend gửi `configuration_query`, Gateway Stage 5 cũ không hiểu envelope này và sẽ bỏ qua $\to$ Backend timeout query và ghi nhận trạng thái thiết bị là `unavailable`, không tự hội tụ nhưng không gây lỗi hay hỏng dữ liệu. |
| **OFF** | **ON** | **Không Khuyến nghị:** Gateway có thể gửi `command_receipt` lên topic responses, nhưng Backend Stage 5 cũ sẽ bỏ qua hoặc từ chối do không khớp schema result v1. Hệ thống vẫn tiếp tục hoạt động khi Gateway gửi `command_result`. |
| **ON** | **ON** | **Chế độ Hoàn chỉnh Stage 6:** Hoạt động đầy đủ tính năng mở rộng: durable receipt (`acknowledged`), cancellation an toàn trước dispatch, lịch retry bền vững và tự động đối soát sau reconnect. |

#### 11.3. Thứ tự Kích hoạt và Quy trình Hạ cấp An toàn (Quiesce & Rollback Safety)
- **Thứ tự Kích hoạt Chuẩn:** Bật `STAGE6_ENABLED=true` trên Backend trước, sau đó bật `GATEWAY_STAGE6_ENABLED=true` trên Gateway.
- **Thứ tự Tắt Chuẩn:** Tắt `GATEWAY_STAGE6_ENABLED=false` trên Gateway trước, sau đó tắt `STAGE6_ENABLED=false` trên Backend.
- **Quy trình Hạ cấp / Rollback về Binary Cũ:**
  - Không có tính tương thích lùi ngầm định (no hidden rollback compatibility): Kể cả khi toàn bộ hàng đợi outbox đã được drain sạch, lịch sử các bản ghi đã `cancelled` trong cơ sở dữ liệu vẫn có thể gây lỗi cho DTO hoặc parser của binary Stage 5 cũ (do Stage 5 cũ không nhận biết trạng thái `cancelled`).
  - Do đó, việc hạ cấp binary về Stage 5 cũ là **hoàn toàn không được hỗ trợ** nếu không có quy trình phục hồi/sao lưu schema tương thích đã được kiểm chứng; việc chỉ drain outbox không chứng minh hay bảo đảm tính tương thích hạ cấp.

#### 11.4. Ghi chú Schema Thực tế và Giới hạn Tiền kiểm tra / Sao lưu (Actual Schema Notes & Preflight Limitations)
1. **Áp dụng Migration PostgreSQL 27:**
   - Yêu cầu quiesce hệ thống (ngừng tiếp nhận yêu cầu điều khiển mới), xác lập maintenance window và thực hiện sao lưu cơ sở dữ liệu có kiểm chứng trước khi chạy Migration 27 (`000027_stage6_control_resilience.up.sql`).
   - Ràng buộc khóa ngoại `root_command_id` và `parent_command_id` sử dụng `ON DELETE RESTRICT`: Chính sách dọn dẹp lưu trữ định kỳ (retention purge) bắt buộc phải xóa các lệnh successor terminal phụ thuộc trước khi xóa lệnh root cha; không thể xóa lệnh root khi đang có successor trỏ vào.
   - Trường `cancelled_by` và `original_actor_id` lưu trữ snapshot UUID của actor tại thời điểm thao tác (không gắn FK tới `profiles`); snapshot kiểm toán này được bảo toàn khi user profile bị xóa, nhưng không cam kết lưu trữ vĩnh viễn vượt ngoài thời hạn retention của bảng lệnh.
2. **Nâng cấp SQLite Gateway Simulator (user_version 3):**
   - Quá trình nâng cấp SQLite diễn ra tự động khi mở tệp cơ sở dữ liệu (`OpenOutbox`). Binary Gateway cũ hỗ trợ `user_version = 2` sẽ từ chối mở tệp đã nâng lên `user_version = 3` (`file version 3, supported version 2`).
3. **Giới hạn Hạ cấp Ứng dụng (Application Rollback Limitations):**
   - Hạ cấp binary ứng dụng không được chứng minh hay bảo đảm chỉ bằng việc gạt cờ tính năng hay drain hàng đợi outbox. Dữ liệu lệnh có trạng thái `cancelled` hay các cột mới sẽ không tương thích với DTO/parser của binary Stage 5 cũ nếu không có bản sao lưu schema tương thích được phục hồi.
4. **Ranh giới Thực hiện:**
   - Các lát cắt R0–R8 và R2 follow-up đã hoàn tất cài đặt trong mã nguồn backend (`internal/digitaltwin`, `internal/mqttcontrol`, `internal/httpserver`, `cmd/server`) và Gateway simulator (`internal/gatewaysim`, `cmd/gateway`), và đã PASS CI từ xa (GitHub Actions run 44501549344 trên commit `a648ec1c8e22b99d3a185aa480a8b17534f7df8f`).
   - Mặc định triển khai vẫn là **TẮT** (`STAGE6_ENABLED=false`, `GATEWAY_STAGE6_ENABLED=false`).
   - Tổng thể Stage 6 vẫn **PARTIAL** do script retention còn finding review chưa đóng, target demo/preflight/restore và rollout shared/live NOT RUN, board AM5728 / real sensor / HIL DEFERRED / NOT RUN.

---

### 12. Ví dụ Luồng REST và Thao tác Vận hành Thực tế (Worked REST Examples & Operational Flows)

Các ví dụ dưới đây minh họa chính xác các luồng REST thực tế trên API kiểm soát cảm biến Stage 6 dựa trên mã nguồn và hợp đồng hiện hành.

#### 12.1. Luồng 1: PATCH Tạo Intent $\to$ Tra cứu Status $\to$ Hủy An toàn (Safe Cancel)

##### Bước 1.1: Gửi yêu cầu thay đổi tần số lấy mẫu (PATCH Desired State)
```http
PATCH /v1/digital-twins/urn:ngsi-ld:Sensor:gateway_001:sensor_001/desired-state HTTP/1.1
Host: api.example.com
Authorization: Bearer <OWNER_OR_OPERATOR_JWT>
Content-Type: application/json
Idempotency-Key: patch-intent-20261118-001

{
  "frequency_hz": 1.5,
  "expected_desired_version": "12"
}
```

Phản hồi chấp thuận (`202 Accepted`):
```http
HTTP/1.1 202 Accepted
Content-Type: application/json; charset=utf-8
Location: /v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001/commands/0195e18d-0001-7a42-9064-69ea49e63bf1

{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "status": "pending",
  "requested_frequency_hz": "1.5",
  "effective_frequency_hz": 1.5,
  "sample_interval_us": 666667,
  "desired_version": "13",
  "expires_at": "2026-11-18T10:01:00.000000Z",
  "status_url": "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001/commands/0195e18d-0001-7a42-9064-69ea49e63bf1"
}
```

##### Bước 1.2: Tra cứu trạng thái lệnh (GET Command Detail)
```http
GET /v1/digital-twins/urn:ngsi-ld:Sensor:gateway_001:sensor_001/commands/0195e18d-0001-7a42-9064-69ea49e63bf1 HTTP/1.1
Host: api.example.com
Authorization: Bearer <VIEWER_OWNER_OPERATOR_JWT>
Accept: application/json
```

Phản hồi (`200 OK` — quan sát tại thời điểm lệnh vẫn ở trạng thái pending, attempt count = 0):
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "status": "pending",
  "desired_version": "13",
  "parameters": {
    "sample_interval_us": 666667
  },
  "issued_at": "2026-11-18T10:00:00.000000Z",
  "expires_at": "2026-11-18T10:01:00.000000Z",
  "attempt_count": 0,
  "origin": "client",
  "root_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "parent_command_id": null,
  "reconcile_blocked": false,
  "root_reconcile_blocked": false,
  "root_auto_successors_used": 0
}
```
*(Lưu ý: Việc `GET` thấy `status: "pending"` chỉ là quan sát tại thời điểm đọc, không bảo đảm lệnh chưa bị worker cấp quyền dispatch ngay sau đó. Thao tác cancel tiếp theo sẽ tái kiểm tra có khóa trong cơ sở dữ liệu; nếu lệnh đã được reserve, API sẽ trả về 409 Conflict).*

##### Bước 1.3: Hủy an toàn khi lệnh chưa được cấp quyền dispatch (POST Cancel)
```http
POST /v1/digital-twins/urn:ngsi-ld:Sensor:gateway_001:sensor_001/commands/0195e18d-0001-7a42-9064-69ea49e63bf1/cancel HTTP/1.1
Host: api.example.com
Authorization: Bearer <OWNER_OR_OPERATOR_JWT>
Content-Length: 0
```
*(Lưu ý: Endpoint cancel yêu cầu body rỗng. Nếu gửi body hoặc Content-Type lạ sẽ bị từ chối 415/400 trước khi chạm logic database).*

Phản hồi hủy thành công (`200 OK`):
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "status": "cancelled",
  "desired_version": "13",
  "cancelled_at": "2026-11-18T10:00:01.234567Z",
  "status_url": "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agateway_001%3Asensor_001/commands/0195e18d-0001-7a42-9064-69ea49e63bf1"
}
```

- **Phát lại lệnh hủy (Replay Cancel):** Gọi lại chính xác `POST .../cancel` trên command đã `cancelled` bởi caller có thẩm quyền sẽ trả về đúng `200 OK` với cùng kết quả lưu trữ (cùng `cancelled_at`), kể cả sau khi `expires_at` gốc đã trôi qua.
- **Trường hợp từ chối hủy (Conflict 409):** Nếu worker đã cấp quyền dispatch (`first_reserved_at != nil` hoặc `attempt_count > 0`), hoặc lệnh đã bước vào trạng thái active/terminal khác (`published`, `acknowledged`, `succeeded`, `failed`, hoặc `timeout`), server trả về mã lỗi và thông điệp chuẩn:
```http
HTTP/1.1 409 Conflict
Content-Type: application/json; charset=utf-8

{
  "error": {
    "code": "command_not_cancellable",
    "message": "command not cancellable",
    "request_id": "0195e18d-0000-7a42-9064-69ea49e63bf0"
  }
}
```

---

#### 12.2. Luồng 2: Lệnh Timeout $\to$ Readback $\to$ Successor Mới $\to$ Succeeded

##### Bước 2.1: Lệnh ban đầu bị Timeout (mất gói tin result trên mạng)
Lệnh root `0195e18d-0001-7a42-9064-69ea49e63bf1` được phát với desired version `"14"` (interval `666667` us).
Sau các lần thử theo ngân sách cấp quyền dispatch (mặc định 5 lượt, cấu hình qua `STAGE6_RETRY_MAX_ATTEMPTS`) và hết hạn TTL (`expires_at`), kết quả thực thi không đến.
Khi tra cứu `GET .../commands/0195e18d-0001-7a42-9064-69ea49e63bf1`:
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "status": "timeout",
  "desired_version": "14",
  "parameters": {
    "sample_interval_us": 666667
  },
  "attempt_count": 5,
  "retry_stop_reason": "retry_exhausted",
  "origin": "client",
  "root_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "parent_command_id": null,
  "reconcile_blocked": false,
  "root_reconcile_blocked": false,
  "root_auto_successors_used": 0
}
```
**`timeout` là trạng thái kết thúc (terminal)**, không bị xóa hay đảo ngược thành `succeeded`.

##### Bước 2.2: Reconnect và Readback qua `configuration_query` / `configuration_snapshot`
Khi Gateway kết nối lại, backend discovery quét thấy Sensor cần đối soát (`desired_version: "14"`, `reported.configuration.desired_version: "13"`).
Backend gửi `configuration_query` downlink. Gateway phản hồi `configuration_snapshot` uplink:
- Gateway báo `watermark_version: "13"`, `configuration_state: "configured"`, `actual_sample_interval_us: 1000000`.
- Mismatch được xác thực: Gateway đang ở watermark `"13"` cũ, cadence thực tế `1000000` us lệch so với server mong muốn `666667` us.
- Guard kiểm tra: chưa bị cancel (`reconcile_blocked: false`), ngân sách còn (`auto_successors_used: 0 < 1`).

##### Bước 2.3: Reconciliation Engine sinh Successor Command mới
Engine tạo một successor command nguyên tử:
- **`command_id` mới:** `0195e18d-0002-7a42-9064-69ea49e63bf2`.
- **`desired_version` tăng lên 1 đơn vị:** `"15"`.
- **Giá trị thông số mong muốn giữ nguyên:** `sample_interval_us: 666667` (1.5 Hz).
- Dòng dõi: `origin = "reconciliation"`, `root_command_id = "0195e18d-0001-7a42-9064-69ea49e63bf1"`, `parent_command_id = "0195e18d-0001-7a42-9064-69ea49e63bf1"`.
- Ngân sách tự động của root được cập nhật: `root_auto_successors_used = 1`.
- Lệnh root cũ vẫn giữ nguyên `status = "timeout"`.

##### Bước 2.4: Successor được Gateway áp dụng và xác nhận
- Gateway nhận successor, ghi SQLite receipt $\to$ phát `command_receipt` (backend chuyển successor sang `acknowledged` nếu quan sát được trước khi result đến).
- Gateway gọi sampler apply cấu hình `666667` us $\to$ thành công $\to$ phát `command_result` (`succeeded`).
- Backend nhận result $\to$ chuyển successor sang `succeeded`.
- `twin_states.reported.configuration` cập nhật (kèm `applied_at`):
  `{"sample_interval_us": 666667, "desired_version": "15", "applied_at": "2026-11-18T10:05:01.350000Z"}`.
- Tra cứu successor `GET .../commands/0195e18d-0002-7a42-9064-69ea49e63bf2`:
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "command_id": "0195e18d-0002-7a42-9064-69ea49e63bf2",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "status": "succeeded",
  "desired_version": "15",
  "parameters": {
    "sample_interval_us": 666667
  },
  "attempt_count": 1,
  "acknowledged_at": "2026-11-18T10:05:01.100000Z",
  "completed_at": "2026-11-18T10:05:01.350000Z",
  "origin": "reconciliation",
  "root_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "parent_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "reconcile_blocked": false,
  "root_reconcile_blocked": false,
  "root_auto_successors_used": 1
}
```
*(Lưu ý về precedence: Việc kiểm tra ngân sách diễn ra trước kiểm tra hội tụ. Successor thành công đưa version khớp nhau có thể khiến discovery dừng quét nên không nhất thiết phải hiển thị snapshot decision `converged`. Nếu sau đó gói tin result muộn của root ban đầu đến: backend ghi nhận bằng chứng late result riêng, tuyệt đối không ghi đè cấu hình đã hội tụ).*

---

#### 12.3. Luồng 3: Trường hợp Bị chặn (Blocked) và Hướng dẫn Xử lý Thủ công (Manual Action)

##### Trường hợp 3A: Intent đã bị Hủy (`decision = "blocked_cancelled"`)
Khi người dùng đã chủ động hủy intent bằng `POST .../cancel`, cờ `reconcile_blocked = true` được gán trên root intent.
Khi nhận snapshot, Engine đối soát kiểm tra guard cancel trước và quyết định: `decision = "blocked_cancelled"`. Không successor nào được tạo.

##### Trường hợp 3B: Hết Ngân sách Tự động (`decision = "blocked_budget"`)
Successor duy nhất được phép (`maxAutomaticReconcileSuccessors = 1`) đã phát hành nhưng cũng bị timeout hoặc thất bại.
Engine kiểm tra ngân sách trước kiểm tra hội tụ và quyết định: `decision = "blocked_budget"`.
*Lưu ý phân định:* Hết ngân sách tự động hoàn toàn tách biệt với hủy lệnh; `blocked_budget` **không** đặt `reconcile_blocked = true` hay `root_reconcile_blocked = true`.

Tra cứu lệnh successor khi hết ngân sách:
```http
GET /v1/digital-twins/urn:ngsi-ld:Sensor:gateway_001:sensor_001/commands/0195e18d-0002-7a42-9064-69ea49e63bf2 HTTP/1.1
Host: api.example.com
Authorization: Bearer <VIEWER_OWNER_OPERATOR_JWT>
```

```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "command_id": "0195e18d-0002-7a42-9064-69ea49e63bf2",
  "entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
  "status": "timeout",
  "desired_version": "15",
  "origin": "reconciliation",
  "root_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "parent_command_id": "0195e18d-0001-7a42-9064-69ea49e63bf1",
  "reconcile_blocked": false,
  "root_reconcile_blocked": false,
  "root_auto_successors_used": 1,
  "current_reconciliation": {
    "desired_version": "15",
    "decision": "blocked_budget",
    "last_reconciled_at": "2026-11-18T10:10:00.000000Z"
  }
}
```

##### Trường hợp 3C: Thiết bị Báo lỗi hoặc Store SQLite Hỏng
- Gateway phản hồi snapshot với `configuration_state: "unavailable"` $\to$ Engine ghi nhận `decision = "corrupt_unavailable"`.
- Gateway watermark cao hơn server, hoặc snapshot báo `configured` nhưng thiếu interval, hoặc unconfigured thiếu actual $\to$ Engine ghi nhận `decision = "manual_unconfirmed"`.

##### Hướng dẫn Xử lý Hành động cho Người vận hành / Client:
1. **Kiểm tra tình trạng vật lý:** Kiểm tra kết nối mạng của Gateway, tính sẵn sàng của sensor phần cứng và log lỗi Gateway.
2. **Đọc trạng thái hiện hành:** Gọi `GET /v1/digital-twins/{entity_id}?format=simplified` để xác định `desired_version` và `reported_state` hiện tại; tra cứu chi tiết lệnh qua `GET /v1/digital-twins/{entity_id}/commands/{command_id}` để đối chiếu quyết định đối soát gần nhất.
3. **Mở lại intent mới:** Người dùng hoặc ứng dụng client gửi một request **PATCH mới** tới `/v1/digital-twins/{entity_id}/desired-state`:
   - Phải sử dụng header `Idempotency-Key` mới (không dùng lại key cũ).
   - `expected_desired_version` phải khớp với version hiện hành vừa đọc được (ở ví dụ trên là `"15"`).
   - Khi request PATCH mới được chấp thuận (`202 Accepted`): Server khởi tạo một root intent mới (tăng version lên `"16"`, cấp `command_id` mới, `root_command_id` chính là ID mới này, `auto_successors_used = 0`), đồng thời **xóa sạch trạng thái đối soát của Sensor state (`last_reconciled_at = NULL, last_reconcile_decision = NULL`)** trong cùng transaction. Lưu ý: Intent mới không thay đổi hay xóa cờ trên bản ghi command root lịch sử cũ, mà bắt đầu một lineage hoàn toàn mới!
   - Tuyệt đối không thử gửi lại key cũ hoặc trông đợi hệ thống tự lặp vô hạn.
