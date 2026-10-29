# Giao thức WebSocket v1 (WebSocket Telemetry Protocol v1)

**Trạng thái:** Đã triển khai, Local MVP dạng tùy chọn kích hoạt (opt-in); `/v1/ws` bị tắt theo mặc định. P8 có cap hữu hạn cấu hình được `WS_MAX_SESSIONS` 1–10.000 (mặc định 32), logical budget tổng cho session-reference `WS_SESSION_REFERENCE_BYTES` 1–256 MiB (mặc định 8 MiB). Các giới hạn cấu hình không phải nghiệm thu tải/RSS; mặc định recheck interval 5s, sweep budget 2s, concurrency 2. Ring tối đa 32 close observations không đổi. Xem [báo cáo P8](../backend/paper-p8-experiments-and-acceptance.md) về source qualification, pilot và phần còn thiếu. Tài liệu này mô tả hành vi hiện tại của Go/parser/runtime, không phải toàn bộ cam kết nghiệm thu đề xuất cho P7. Đường dẫn là `GET /v1/ws`; giao thức con ứng dụng được thương lượng là `iot.telemetry.v1`.

## Bắt tay, thông tin xác thực và ranh giới ngoại vi (Handshake, credentials and perimeter)

- Client dạng ứng dụng độc lập (native client): Dùng HTTP header `Authorization: Bearer <JWT>` và `Sec-WebSocket-Protocol: iot.telemetry.v1`.
- Client trình duyệt (browser client): Dùng `Sec-WebSocket-Protocol: iot.telemetry.v1, bearer.<JWT>`; subprotocol chứa thông tin xác thực sau khi được xác minh sẽ không được phản hồi lại (echo), hệ thống chỉ thương lượng giao thức con ứng dụng. Không hỗ trợ truyền thông tin xác thực qua chuỗi truy vấn (query-string).
- Cơ chế `auth_request` của Nginx (theo cấu hình P2) thực hiện xác minh qua socket Unix nội bộ; tiến trình Go độc lập xác minh lại thông tin xác thực và trình xử lý (handler) WebSocket sẽ kiểm tra danh tính đã xác minh (verified principal) cùng thời hạn hiệu lực (expiry). Go handler kiểm tra chính xác danh sách Origin được phép: client native được phép không gửi header `Origin`; nếu có gửi, phải có đúng một Origin hợp lệ nằm trong danh sách cho phép. Origin không đại diện cho danh tính (identity). Bản mẫu cấu hình thử nghiệm opt-in không đồng nghĩa hay ngụ ý việc triển khai WSS production.
- Trước khi nâng cấp kết nối (pre-upgrade), các lỗi xác thực HTTP, ranh giới ngoại vi, Origin hoặc tính khả dụng của dịch vụ sẽ trả về mã trạng thái HTTP tương ứng. Sau khi nâng cấp kết nối (post-upgrade), phiên làm việc có thể bị đóng đột ngột (`CloseNow`); các API phía trình duyệt có thể báo mã đóng bất thường `1006`. Không được mặc định coi rằng mọi lỗi phát sinh đều sẽ tạo ra một khung đóng kết nối (close frame) bình thường.

## Bản tin điều khiển từ client (Client control messages)

Bản tin do client gửi lên là văn bản JSON (JSON text), áp dụng cơ chế từ chối nghiêm ngặt các trường lạ không xác định (strict unknown-field rejection). Kích thước khung (frame) tối đa là 4.096 byte; tối đa 16 mục tiêu (targets) cho mỗi yêu cầu và 16 bộ lọc hoạt động (active filters) cho mỗi phiên; mã định danh yêu cầu (`request_id`) tối đa 64 ký tự; giới hạn tần suất tối đa 10 yêu cầu điều khiển/giây cho mỗi phiên. `protocol_version` bắt buộc phải là số nguyên `1`. Trường `action` nhận giá trị `subscribe` hoặc `unsubscribe`; bộ phân tích cú pháp cũng chấp nhận trường `message_type` có giá trị tương ứng để tương thích hành động (action compatibility), nhưng nếu hai trường này không khớp nhau yêu cầu sẽ thất bại.

Mục tiêu (targets) chấp nhận định dạng `{ "gateway_id":"gw" }`, `{ "gateway_id":"gw", "sensor_id":"sensor" }`, hoặc `entity_id` chỉ sử dụng các định danh URN hợp lệ đã qua kiểm tra dạng `urn:ngsi-ld:Gateway:<gateway>` / `urn:ngsi-ld:Sensor:<gateway>:<sensor>`. Nếu các ID tường minh được gửi kèm cùng với một URN, các giá trị này bắt buộc phải nhất quán với nhau. Quyền hạn và sự tồn tại của mục tiêu được ứng dụng kiểm tra trước khi cài đặt bộ lọc; quyền quản trị viên nền tảng (platform admin) không thể bỏ qua quyền thành viên trên Gateway (Gateway membership). Một phản hồi xác nhận thành công (ACK) chỉ biểu thị rằng bộ lọc đã được cấp quyền và được cài đặt, không mang ý nghĩa lịch sử dữ liệu đã được đồng bộ hóa.

```json
{"protocol_version":1,"request_id":"req-1","action":"subscribe","targets":[{"gateway_id":"gw_example","sensor_id":"sensor_temp"}]}
```

```json
{"protocol_version":1,"message_type":"ack","request_id":"req-1","action":"subscribe","targets":[{"gateway_id":"gw_example","sensor_id":"sensor_temp"}]}
```

## Xử lý lỗi và đóng kết nối (Errors and closure)

Đối tượng lỗi tầng ứng dụng (application error object) gồm có: `protocol_version`, `message_type:"error"`, `request_id` (tùy chọn), `error_code`, `error_message`, và `field` (tùy chọn). Các mã lỗi hiện tại do bộ phân tích cú pháp hoặc tầng ứng dụng phát ra gồm: `INVALID_MESSAGE`, `UNSUPPORTED_PROTOCOL`, `MESSAGE_TOO_LARGE`, `INVALID_ACTION`, `INVALID_TARGET`, `TARGET_LIMIT_EXCEEDED`, `RATE_LIMIT_EXCEEDED`, `UNAUTHORIZED`, `NOT_FOUND`, `INTERNAL_ERROR`. Logic phân quyền hiện ánh xạ mọi trường hợp từ chối sang `UNAUTHORIZED`; không được giả định rằng `NOT_FOUND` sẽ được phát ra cho mọi mục tiêu không tồn tại. Các thất bại liên quan đến thông tin xác thực, Origin hoặc bắt tay đều trả về dưới dạng phản hồi HTTP trước khi nâng cấp kết nối, không sử dụng các đối tượng lỗi WebSocket này.

Khi token JWT hết hạn, phiên làm việc sẽ bị đóng dựa trên thời điểm hết hạn đã được xác minh; không hỗ trợ làm mới token trong luồng kết nối (in-band refresh). Tư cách thành viên (membership) được định kỳ tái kiểm tra theo chu kỳ cấu hình 5 giây một lần, với ngân sách quét (sweep budget) toàn cục giới hạn trong 2 giây và mức đồng thời (concurrency) là 2 trong cấu hình hiện hành; nếu bị thu hồi quyền hoặc gặp lỗi truy vấn, hệ thống sẽ đóng kết nối để bảo đảm an toàn (fail-closed). Đây là các ngân sách truy vấn và lập lịch tiến trình, không phải cam kết thu hồi tức thì ngay lập tức. Chu kỳ ping là 20 giây, ngân sách không hoạt động khi chờ pong/read là 45 giây, thời gian chờ ghi (write timeout) là 5 giây. Một client hợp lệ khi rảnh rỗi vẫn phải liên tục gọi API đọc WebSocket của mình để các khung điều khiển/pong được xử lý. Khi hệ thống tắt (shutdown), các socket đã nâng cấp có thể bị đóng ngay lập tức sau khi dừng tiếp nhận phiên và dọn dẹp; tuyệt đối không mặc định rằng sẽ luôn nhận được khung mã `1001`.

## Sự kiện viễn trắc và chuyển giao dữ liệu (Telemetry event and delivery)

Mỗi sự kiện đại diện cho một lô dữ liệu (batch) hợp lệ **vừa được ghi nhận thành công** (newly committed), không phải từng mẫu đơn lẻ. Các trường hợp trùng lặp ở tầng DB/RAM, lưu trữ thất bại hoặc trạng thái lưu trữ không chắc chắn (uncertain persistence) sẽ không tạo ra sự kiện mới. Các đăng ký chồng lấn theo Gateway và Sensor chỉ khớp với một phiên tối đa một lần. Không cam kết thứ tự tuyệt đối (total order), không phát lại bền vững (durable replay), và không có cam kết chuyển giao đúng một lần (exactly-once) qua các lần ngắt kết nối hoặc khởi động lại dịch vụ. Các lô dữ liệu đến muộn (late batches) hoàn toàn có thể xảy ra.

```json
{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw_example:sensor_temp","gateway_id":"gw_example","sensor_id":"sensor_temp","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"9007199254740993","sample_count":2,"measured_at":"2026-10-27T10:00:00.000000Z","sample_interval_us":10000,"samples":[25.4,25.5]}
```

Trường `first_sequence` là một chuỗi JSON dạng số thập phân (decimal JSON string); trường này nhìn chung không thể biểu diễn an toàn dưới dạng kiểu Number của JavaScript. Sự kiện chứa các mẫu dữ liệu thô của lô và không tổng hợp trạng thái bản sao số (Twin) hay cấu hình. Không có hiện tượng giảm tần số lấy mẫu (downsampling) hay cắt ngắn mẫu ở tầng cơ sở dữ liệu. Giới hạn khung (frame cap) là 66.560 byte; phần vỏ bao bọc JSON (envelope JSON) có thể vượt quá giới hạn này ngay cả đối với các tải trọng MQTT hợp lệ do quá trình kết xuất số thực dấu phẩy động (float rendering) có thể làm tăng độ dài ký tự. Khi đó việc chuyển giao sự kiện sẽ xử lý an toàn bằng cách đóng phiên và yêu cầu tái đồng bộ (fail-closed/resynchronizes); dữ liệu đã lưu vào DB và phản hồi ACK ứng dụng vẫn không bị ảnh hưởng. Mặc dù có hàm hỗ trợ thông báo sự kiện quá khổ (oversized-event notice helper), chính sách runtime hiện hành sẽ thực hiện đóng phiên và tái đồng bộ chứ không bảo đảm gửi một khung `gap`; các trường hợp mất mát luồng dữ liệu (tràn hàng đợi phiên hoặc toàn cục, khởi động lại, ngắt kết nối, bản tin quá khổ) **không** bảo đảm sẽ có khung báo lỗi hay khung đóng kết nối sạch sẽ. Hiện tượng tràn hàng đợi ở phiên chậm (slow session queue overflow) sẽ đóng kết nối của riêng viewer đó; các hàng đợi có giới hạn trên từng phiên và toàn cục sẽ cách ly bảo vệ luồng nạp (ingress) và các viewer khác.

## Đồng bộ bù qua REST (REST catch-up)

Truy xuất lịch sử đo lường có giới hạn sau khi kết nối lại hoặc khi nghi ngờ có khoảng trống dữ liệu (gap):

```http
GET /v1/digital-twins/urn:ngsi-ld:Sensor:gw_example:sensor_temp/temporal?attrs=measurement&timerel=between&timeAt=2026-10-27T10:00:00.000000Z&endTimeAt=2026-10-27T10:05:00.000000Z
Accept: application/ld+json
Authorization: Bearer <JWT>
```

API temporal áp dụng các giới hạn hiện hành sẵn có (bao gồm cả `max_points`); bộ kiểm thử (fixture) đã sử dụng giới hạn 256 điểm, đây không phải là giới hạn chung cho toàn máy chủ. Cần phân chia một khoảng thời gian lớn thành các cửa sổ con có giới hạn liền kề nhau. Bộ chuẩn kiểm thử (fixture oracle) sử dụng các cửa sổ con bán mở (half-open subwindows) và mở rộng mốc kết thúc cuối cùng thêm 1 microgiây để bao gồm cả điểm cuối. Bộ dữ liệu thô thực tế có dạng `[value, observedAt]`; việc so sánh giá trị và mốc thời gian được thực hiện ở độ chính xác microgiây, đồng thời giữ nguyên các bản ghi phân biệt có mốc thời gian giống hệt nhau. Dữ liệu lịch sử REST không chứa đầy đủ bộ định danh nguồn gốc sự kiện trực tiếp (provenance tuple), do đó việc hợp nhất theo mốc thời gian không thể chứng minh tính chất exactly-once. Cần làm mới các cửa sổ có giới hạn để bao gồm cả các lô đến muộn; riêng mốc thời gian không phải là con trỏ bền vững (durable cursor). Các đường dẫn cũ đã bị loại bỏ như `/v1/telemetry/history`, `/history`, `/state` không phải là bí danh (alias) của API này.

## Ghi chú lịch sử được bảo lưu (Retained historical note)

Bản thảo P7.2 trước đây từng ghi nhận các hành vi đề xuất trong tương lai và mô tả sai rằng Nginx là bên kiểm tra Origin, cam kết mọi lô 64 KiB hợp lệ đều vừa khung, cũng như ngụ ý về thông báo gap phổ quát và các mã đóng kết nối. Những tuyên bố đó đã bị thay thế hoàn toàn bởi bản hợp đồng giao thức đã được đối chiếu mã nguồn này; các bản ghi thất bại và trạng thái của các lượt chạy trong lịch sử vẫn được bảo lưu trong chứng cứ được bảo vệ và không bị thay đổi phân loại bởi bản hiệu chỉnh giao thức này.
