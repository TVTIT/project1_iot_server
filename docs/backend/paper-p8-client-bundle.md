# Bundle client P8 portable (Bước 3)

> **Cập nhật BS.9a (05/11/2026):** Workflow portable hiện dùng thực tế là
> A prepare/provision/export bundle; chuyển thủ công bundle + binary phù hợp OS/
> architecture; B bind → offline preflight bằng binary thật → kiểm tra kết nối
> TLS/REST/WS/MQTT → đợi A `begin` và collector-ready → run → export-results;
> chuyển projection về A để import/verify/finalize. E1–E5 native local pilots
> đã chạy riêng, nhưng physical A/B chưa chạy. B chỉ giữ role credentials; không
> nhận DB/admin/Docker socket/manager credential hoặc private server/CA key.

## Phạm vi

`scripts/paper_p8_bundle.py` cung cấp API `export_bundle(...)` để đóng gói các
tệp input đã được provision/cấp rõ ràng cho client, không có kết nối mạng và
không tự provision danh tính. Đây chưa phải quy trình E1 từ đầu đến cuối: khâu
admin/provisioning vẫn thuộc máy A và trust contract của provisioner. Không
đưa API key admin, JWT admin, `service_role`, thông tin DB, thông tin DynSec
manager, CA/server private key, `.env` deployment, executable hay thành viên
archive tùy ý vào bundle.

## Cấu trúc và giới hạn

- `bundle.json`: schema phiên bản 1, nhận diện run/thí nghiệm/arm/phase, endpoint,
  topology/workload và inventory SHA-256.
- `certs/`: chỉ hai CA public được truyền rõ ràng (`https-ca.pem`, `mqtt-ca.pem`).
- `private/identities/`, `private/gateways/`, `private/gateway-passwords/`:
  identity/token và thông tin Gateway được chọn tường minh; thư mục `0700`, tệp
  `0600`.
- `manifests/`, `gateway-env/`, `execution/run-spec.json`: manifest Go paperload
  hiện hữu và spec executor đã đổi sang đường dẫn tương đối trong bundle.
- `outputs/`: workspace mutable được tạo riêng, không chứa SQLite/outbox/journal
  nguồn. E4 journal được ánh xạ cố định thành `outputs/e4-journal.sqlite`, để các
  phase dùng chung một đích mà không sao chép trạng thái pending từ A.

Exporter chỉ đổi các trường tham chiếu đã allowlist: CA TLS, `identity_files`,
`gateway_identity_files`, `bundle_path`, `e4.journal_path`, các trường đường dẫn
run-spec và các trường CA/password/outbox của Gateway env. Mọi đường dẫn đích là
logic/tương đối; không thay chuỗi tùy ý trong JSON. Nguồn identity, Gateway,
CA và manifest được truyền tường minh; không duyệt thư mục tùy ý để tìm secret.
Hiện Go paperload schema không có trường tham chiếu dataset nên nếu truyền
dataset exporter sẽ từ chối rõ ràng, không đóng gói dữ liệu mồ côi.

Mỗi input bị giới hạn 8 MiB, tổng inventory 32 MiB và tối đa 96 tệp. Bundle chỉ
được tạo tại output leaf chưa tồn tại; parent phải tồn tại và không phải symlink.
JWT đã provision chỉ được giải mã payload để kiểm tra `exp` còn đủ thời gian
wait + workload bị chặn tối đa 7.200 giây. Đây **không** phải xác minh chữ ký,
role hoặc quyền của JWT. Mối liên hệ role/principal và Gateway username được
đối chiếu với metadata provisioner do caller tin cậy cung cấp. Opaque credential
không thể được chứng minh an toàn chỉ bằng cách quét tên khóa.

Checksum chỉ dùng integrity/correlation; không phải chữ ký hay chứng minh nguồn
đáng tin. Việc chuyển bundle an toàn vẫn do operator thực hiện thủ công. Export
không upload hoặc gửi dữ liệu qua mạng. `inspect` xác nhận inventory/checksum:

```sh
python3 scripts/paper_p8_bundle.py inspect /đường/dẫn/tới/bundle
```

API exporter được thiết kế để adapter chuẩn bị E1 sau này gọi trực tiếp bằng các
manifest E1 viewer/telemetry, identity đã provision, CA public, Gateway
credentials/env và run-spec được tạo bởi builder hiện hữu. API cũng nhận
manifest control E3 cùng telemetry/Gateway executor inputs và ánh xạ hai phase
E4 tới cùng một journal output mới mà không sao chép SQLite nguồn. Đây chỉ là
đóng gói đường dẫn; không xác nhận E3/E4 sẵn sàng.

## Bước 4: workspace B

`scripts/paper_p8_client.py` cung cấp các lệnh phía B. Bundle chỉ là template
portable: không đưa trực tiếp `execution/run-spec.json` vào `paper_p8.py`; binder
tiêu thụ contract `path_reference_base=bundle-root`, sao chép inventory đã xác
minh vào `inputs/`, dựng manifest/env/run-spec tuyệt đối trong `runtime/` và
workspace mới, còn `outputs/` dành cho trạng thái mutable. Binder không ghi đè
workspace có sẵn; E4 giữ một journal đích mới chung, chưa chuyển trạng thái pending.

Ví dụ trên máy B (các thư mục output phải là đường dẫn riêng/chưa tồn tại):

```sh
python3 scripts/paper_p8_client.py bind --bundle ./received-inputs \
  --workspace ./runs/session-42
python3 scripts/paper_p8_client.py preflight --workspace ./runs/session-42 \
  --binary ./bin/paperload --gateway-binary ./bin/gateway
python3 scripts/paper_p8_client.py check-connection --workspace ./runs/session-42
python3 scripts/paper_p8_client.py run --workspace ./runs/session-42 \
  --binary ./bin/paperload --gateway-binary ./bin/gateway
python3 scripts/paper_p8_client.py export-results --workspace ./runs/session-42 \
  --output ./results/session-42
```

`preflight` gọi binary thật `paperload --preflight` trên mọi manifest đã bind,
không tạo output/workload, không gọi mạng; validate Gateway env chỉ đọc. Lệnh
`check-connection` dùng TLS xác minh CA + hostname, một REST GET xác thực,
WebSocket authenticated handshake (không subscribe) và MQTT CONNECT/CONNACK/
DISCONNECT; không phát telemetry hay control. Nó không retry lỗi auth. `run`
dùng supervisor hiện có; lỗi giữ lại journal, SQLite, logs và run report nếu có,
không cleanup-to-green. Kết quả an toàn chỉ allowlist JSON, loại bỏ trường ngoài
allowlist và giới hạn file/count/bytes; raw logs, manifests, credentials,
SQLite/journal không export. Private evidence vẫn ở workspace B.

Binder ghi nguồn SHA-256, session/run/phase correlation và kiểm tra lại `exp`
theo transfer wait + workload còn lại. Đây không xác minh chữ ký JWT hoặc nguồn
đáng tin; bundle checksum không phải chữ ký. E4 nhiều phase hiện được bind chung
journal, nhưng supervisor chưa chạy tuần tự phase trên journal dùng chung; chưa
được chạy multi-phase E4 bằng một lệnh.

Contract offline đã được kiểm tra bằng `e1_role_manifests`/`build_e1_execution_plan`
và `e3_pilot_manifests`/`build_e3_execution_plan` hiện có: E1 gồm manifest viewer
64 viewers cùng 2 token identities và telemetry, E3 gồm A/B/control cùng hai
executor riêng sensor/outbox trên một Gateway. Bundle được relocate rồi bind
trước khi chạy binary Go thật. Evidence (inputs, workspace, command/provenance)
được giữ riêng tại `/var/tmp/opencode/p8-step4-preflight-final6/`; cả E1 (2
manifest) và E3 (3 manifest, 2 env) trả `preflight=PASS`, `network=false`,
`writes=false`. JWT chỉ là test-shaped input; không có server hay broker được gọi,
không có workload được chạy. Đây chỉ là kiểm contract/offline compatibility,
không phải remote E1 acceptance.

## Chưa nằm trong Bước 4

Chưa có secure transfer automation, adapter provision/collect/verify E1 từ A,
native E1, remote E1, hay kiểm thử vật lý A/B. Các lệnh trên chỉ cho B client
binding/probe/run/export; không tự provision trên A, không SSH/Docker/admin trên
A. `full_p8_accepted=false`; physical A/B `NOT RUN`. Không dùng hướng dẫn này như
biên nhận rằng Step 5 hay E1 native đã hoàn tất.
