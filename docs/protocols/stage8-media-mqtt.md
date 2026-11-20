# Hợp đồng MQTT media Stage 8 (v1)

MQTT dùng QoS 1, `retain=false`; envelope JSON tối đa 4.096 byte. Mỗi clip có
`upload_id` ASCII an toàn, ổn định và phải dùng lại khi retry. `gateway_id`
được xác định từ topic; payload có thể bỏ trường này. Nếu có thì phải là chuỗi
không rỗng, đúng gateway trong topic. Parser gán `gateway_id` đã tin cậy vào
message đã decode. ACL/broker vẫn chịu trách nhiệm xác thực quyền.

## Topic và shape

| Chiều | Topic |
|---|---|
| Gateway → backend | `gateways/<gateway_id>/media/up/upload_request` |
| Gateway → backend | `gateways/<gateway_id>/media/up/upload_complete` |
| Backend → Gateway | `gateways/<gateway_id>/media/down/upload_grant` |
| Backend → Gateway | `gateways/<gateway_id>/media/down/upload_result` |

Mọi envelope có `version:1` và `upload_id`. `upload_request` mang
`captured_at` (RFC3339), `content_type:"video/mp4"`, `size_bytes` từ 1 tới đúng
**80.000.000 byte**, và `sha256` gồm 64 ký tự hex. `upload_complete` chỉ cần
các trường chung. Nội dung video tải lên phải là định dạng ISO-BMFF MP4 finalized
không phân mảnh (`video/mp4`, H.264/AVC), chứa các bảng mẫu chuẩn (`stsd`, `stsc`,
`stsz`, `stco`/`co64`) và có ít nhất một mẫu video AVC không rỗng được ánh xạ hợp
lệ vào phạm vi payload của `mdat` thực tế. Khung chứa `mdat` rỗng (kể cả zero-to-EOF)
hoặc bảng mẫu mồ côi không tham chiếu mẫu thực tế trong `mdat` sẽ bị từ chối `INVALID_MP4`.

Để so sánh metadata bất biến khi admission/retry, chuẩn hóa `captured_at` thành
UTC ở độ chính xác microsecond của PostgreSQL (`timestamp(6)`, làm tròn tới
microsecond) và `sha256` thành chữ thường. Parser trả metadata theo dạng chuẩn
này; `size_bytes` và `content_type` giữ nguyên.

`upload_grant` mang `status`. `need_upload` có `media_id`, `upload_url`
(HTTPS signed PUT URL) và `expires_at` (RFC3339); URL cần hostname, path khác
rỗng và query có tham số `token` khác rỗng. Parser giữ nguyên URL/path/query,
không parse rồi serialize lại. `retry_after_completion` mang canonical `media_id`
nhưng không bao giờ mang URL hay expiry: Gateway phải gửi `upload_complete`
trước khi có thể được cấp grant mới.

Các grant status khác: `retry_after_completion`, `pending`, `verifying`,
`ready`, `rejected`, `expired`, `conflict`. `upload_result` dùng `pending`,
`verifying`, `ready`, `rejected`, `expired`, `conflict`; `ready` bắt buộc có
`media_id`. `media_id`, nếu có ở response nào cũng phải là ID an toàn.
`ready`, `rejected`, `expired`, `conflict` không được `retryable:true`.
`conflict` bắt buộc `code:"ID_METADATA_CONFLICT"`.

Grant chỉ cho phép thử PUT, **không phải chấp nhận cuối**. Chỉ
`upload_result:ready` tương quan đúng `upload_id` mới cho phép xóa spool.

## Retry và conflict

- Request cùng `upload_id` và metadata bất biến lặp lại nhận trạng thái hiện
  hành hoặc grant. `need_upload` nghĩa là có thể PUT ngay.
- Nếu PUT có thể đã thành công nhưng phản hồi chưa rõ, Gateway phát
  `upload_complete`. Kết quả `pending`/`verifying` là đang xử lý và không yêu
  cầu gửi clip lại. Nếu đối soát xác định object chưa tồn tại, intent còn hạn,
  backend trả `upload_result` với `status:"pending", code:"OBJECT_MISSING"`
  kèm canonical `media_id` và `retryable:true`.
  Đây là chỉ dẫn gửi lại `upload_request` cùng ID để xin grant mới; không PUT
  cho tới khi nhận grant `need_upload`.
- Backend commits the grant reservation before signing. Its conservative
  `ReservedUntil` is reservation time plus the bounded signing-request timeout,
  configured URL TTL, and in-flight grace. The signer must return the exact
  signed URL expiry; publication is allowed only after an authorized,
  generation-fenced post-sign repository check confirms that expiry is no later
  than the reservation horizon and the intent remains pending/unexpired. The
  database stores neither URL nor query token. Signing/check failure leaves the
  conservative horizon reserved.
- A committed grant reservation marks a possible PUT. Duplicate requests get
  `retry_after_completion` without a URL until `upload_complete` is durably
  recorded and verification reports the object confirmed missing. If a live
  pending intent has no unresolved grant but remaining intent deadline is
  insufficient to fit a full new grant horizon, `upload_grant` returns `pending`
  (`retryable:true`) without a URL and without prematurely expiring the intent;
  only genuinely elapsed deadlines become `expired`. Completion
  transitions a live `pending` intent to `verifying` once, scheduling bounded
  future verification; repeats replay current state without moving the schedule
  or resetting attempts/generation. Terminal states replay unchanged. Expiry
  cleanup is staged no earlier than both the intent deadline and reserved grant
  horizon. A missing-object verification cannot release the grant fence before
  that horizon; workers/scanners/cleanup are a later slice.
- Ví dụ ID bị dùng lại với size/hash khác: `upload_result` status `conflict`,
  code `ID_METADATA_CONFLICT`, `retryable:false`. Đây không phải retry: giữ
  spool nguyên vẹn, báo lỗi và không xóa file.
- `ready`, `rejected`, `expired` là terminal. Lỗi tạm thời được biểu diễn bằng
  `retryable:true` trên trạng thái không-terminal.

Unknown/case-aliased fields, field trùng, null, JSON không phải object, UTF-8
sai, topic/chiều sai, ID không an toàn, gateway không khớp, metadata sai và
envelope quá lớn đều bị từ chối. Log, lỗi và artifacts không được chứa payload
thô, signed URL hoặc query token.
