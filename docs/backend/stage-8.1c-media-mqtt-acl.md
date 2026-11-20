# Stage 8.1c — ACL MQTT media opt-in

Phạm vi này chỉ cấu hình quyền MQTT. Chưa triển khai media runtime, Storage,
Gateway spool hoặc endpoint upload bên ngoài. Compose truyền `MEDIA_UPLOAD_ENABLED`
cho Go backend, mặc định `false`; cờ này không cấu hình Storage. Không bật cờ
trước khi hoàn tất transition có kiểm soát và xác nhận readback chính xác.

## Policy chính xác

Mọi rule dưới đây là allow ở priority 0; bốn default ACL hiện có vẫn deny.

| Principal | Quyền | Topic |
| --- | --- | --- |
| Gateway `<id>` | publish | `gateways/<id>/media/up/#` |
| Gateway `<id>` | receive / subscribe / unsubscribe | `gateways/<id>/media/down/#` |
| `bootstrap_backend` | receive / subscribe / unsubscribe | `gateways/+/media/up/#` |
| `bootstrap_backend` | publish | `gateways/+/media/down/#` |

Profile Gateway hiện hành của Go có **chín rule home** và không được thay thế
bằng một role bốn rule kiểu `gateway_<id>_uplink`:

| Quyền | Topic |
| --- | --- |
| publish | `gateways/<id>/telemetry/#` |
| publish | `gateways/<id>/status` |
| publish | `gateways/<id>/responses/#` |
| receive / subscribe / unsubscribe | `gateways/<id>/acks/#` |
| receive / subscribe / unsubscribe | `gateways/<id>/commands/#` |

Khi media bật, Go thêm đúng bốn rule đã liệt kê ở đầu vào tập chín rule này.
Backend cũng phải có đúng tập Stage 5 hiện hành cộng bốn rule media. Không rule
nào cho phép Gateway khác, đảo chiều topic hoặc truy cập `$CONTROL`.

`mqtt-bootstrap.py` nhận trường JSON opt-in `"media_upload_enabled": true` cho
cả host/container build; bỏ trường này giữ nguyên policy legacy. Bootstrap chỉ
tạo các role vốn có cho manager/backend; Gateway roles được provision qua luồng
credential hiện hữu.

`MEDIA_UPLOAD_ENABLED` là cờ **toàn cục của backend**. Trước khi bật, mọi Gateway
role được quản lý trong profile đó phải khớp chính xác policy Go: chín rule home
và bốn rule media. Nếu chỉ nâng một số role, readback/provisioning Go có thể từ
chối các role còn lại; không bật một phần rồi coi toàn profile là hợp lệ.

## Transition role của store hiện hữu (chỉ operator, không tự chạy)

Không có migration ACL tự động khi startup. Không chạy transition này trên broker
đang phục vụ live traffic. Trong cửa sổ bảo trì, dừng/quiesce backend và các
Gateway MQTT client bị ảnh hưởng, rồi dùng DynSec command capability đã xác thực
với quyền quản trị độc quyền và `scripts/mqtt_backend_acl.py`:

```python
update_existing_backend_media_role(
    request, "bootstrap_backend", exclusive_admin_confirmed=True)
update_existing_gateway_media_role(
    request, "<gateway_id>", exclusive_admin_confirmed=True)
```

`request` là hàm gửi DynSec request có giới hạn và đã xác thực; helper không mở
kết nối, đọc secret, chọn deployment hoặc chạy lệnh live. Gọi Gateway transition
một lần cho từng Gateway trong profile. Trước khi có bất kỳ request nào, Gateway
ID phải qua cùng kiểm tra Go: ASCII `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` và không
được là tên dành riêng `backend_service`.

Backend chỉ chấp nhận policy Stage 5 chính xác hoặc target media chính xác.
Gateway chỉ chấp nhận role `gateway_<id>` được gán riêng cho đúng client và đúng
tập chín rule Go hiện hành, hoặc target media đã đủ 13 rule. Role bốn rule,
`_uplink`, partial/extra ACL, shared role, group membership hay default ACL không
đúng đều bị từ chối trước khi ghi. Client Gateway đang disabled vẫn có thể được
nâng nếu ownership và ACL precheck chính xác; helper giữ nguyên disabled state.

Sau khi thêm đúng bốn ACL, helper đọc lại toàn bộ role và chạy lại ownership,
inventory và default-deny checks. Helper không đổi password/credential, role
assignment, disabled state, default ACL hoặc role control. Nếu có lỗi/interruption
hay readback sai, dừng để kiểm tra trạng thái; tuyệt đối không reset store, đổi
password, tạo lại credential hoặc thử transition trên baseline phỏng đoán.

Giữ cờ ứng dụng tắt trong lúc transition. Xác nhận exact backend readback, exact
13-rule readback cho từng Gateway được quản lý và default-deny readback; sau đó
mới cập nhật cấu hình có chủ đích thành `MEDIA_UPLOAD_ENABLED=true` và khởi động
lại client. Revoke/rotate tiếp tục theo credential lifecycle hiện hành.

## Bằng chứng cục bộ và giới hạn

Từ repository root:

```sh
python3 -m unittest scripts.tests.test_stage8_media_acl
python3 scripts/tests/stage8_media_acl_fixture.py
```

Fixture sở hữu broker container, DynSec volume, TLS certificates và scratch
riêng. Nó kiểm tra exact profile off/on, transition một Gateway disabled mà không
đổi credential/default/control/disabled state, và giữ nguyên luồng telemetry
uplink cùng command downlink legacy. Sau đó kiểm tra media QoS 1 hai chiều,
denied SUBACK cho sai chiều/namespace, không giao chéo Gateway, control isolation
và từ chối xác thực mới sau revoke Gateway A. MQTT 3.x PUBACK chỉ xác nhận
publish packet; không được coi là bằng chứng ACL cho phép giao bản tin. Denial
được kết luận từ SUBACK/receive observation cùng positive controls. Đây không
phải live transition, media runtime/Storage test hay deployment qualification.
