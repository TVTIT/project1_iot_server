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
`409 desired_version_overflow`, `413 payload_too_large`,
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
