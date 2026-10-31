# Hồ sơ vòng đời mạng riêng P8 (Bước 2)

Hồ sơ opt-in này khởi chạy và giữ một fixture P8 cô lập trên một giao diện
IPv4 riêng. Đây chỉ là mức sẵn sàng mạng/vòng đời: **không** cấp phát hoặc đóng
gói client từ xa, không chạy E1 và không xác nhận bundle/thí nghiệm đã sẵn sàng.
Fixture local mặc định và việc chỉ publish trên loopback được giữ nguyên.

## Cấu hình

Truyền tệp JSON private bằng `--p8-remote-config`. Tệp phải thuộc sở hữu user
hiện tại, là tệp thường không phải symlink, và không cho phép group/world ghi.
Schema là chính xác; thiếu hoặc thêm khóa đều bị từ chối:

```json
{
  "schema_version": 1,
  "bind_ip": "192.168.50.20",
  "advertised_host": "192.168.50.20",
  "https_port": 14443,
  "mqtt_tls_port": 18884,
  "required_san_dns": ["localhost", "nginx", "mosquitto"],
  "required_san_ips": ["127.0.0.1", "192.168.50.20"]
}
```

`bind_ip` và `advertised_host` dạng IP phải là IPv4 RFC1918. Tên DNS chỉ được
chấp nhận dưới `.lan`, `.local`, `.internal` hoặc `.test`. Hai cổng TCP phải
khác nhau và nằm trong 1024–65535. Các mảng SAN phải khớp chính xác tên dịch vụ
nội bộ/loopback cùng host quảng bá và địa chỉ bind; không chấp nhận wildcard.
Chưa hỗ trợ IPv6 hoặc địa chỉ public. Giữ cấu hình này, khóa CA riêng được tạo
và artifact fixture trên máy chủ A; không phát tán chúng.

Sau khi fixture được giữ, quy trình bind/probe/run phía B được mô tả riêng tại
[`paper-p8-client-bundle.md`](paper-p8-client-bundle.md). B chỉ cần HTTPS/Nginx
và MQTT-TLS listener; không mở DB, backend/admin hay Docker socket. Các lệnh B
không thay thế A-side adapter provision/collect/verify hoặc native E1 acceptance.

## Giữ fixture chỉ phục vụ vòng đời mạng

```sh
python3 scripts/tests/native_compose_smoke.py \
  --prepare /var/tmp/opencode/p8-remote-run/fixture \
  --p8-lifecycle-hold \
  --p8-remote-config /var/tmp/opencode/p8-remote-run/remote-config.json \
  --p8-experiment E1 --p8-arm remote-network-only --p8-seed 2026
```

Lifecycle hold không khởi chạy workload E1. Chỉ HTTPS của Nginx (container
target 443) và MQTT-TLS của Mosquitto (target 8883) được publish lên bind IP và
cổng đã cấu hình. DB, backend/admin, MQTT thường/1883 và Docker control socket
không được publish ra host. Startup kiểm tra mapping hiệu lực và mapping runtime
của Docker, rồi bắt tay HTTPS và MQTT-TLS với xác minh CA và hostname quảng bá
được bật. HTTPS readiness probe nội bộ được tách khỏi endpoint quảng bá cho
client B.

Dùng `scripts/paper_p8_session.py status --root <fixture>/private` để xem session
đang được giữ. Kết thúc bằng luồng request/cleanup thông thường của session; giữ
nguyên receipts và artifacts. Không dùng lệnh cleanup Docker diện rộng. Bind
address chỉ giới hạn giao diện được expose; operator phải giới hạn firewall cho
đúng client dự kiến. Hồ sơ này không tự chỉnh firewall/sysctl và không bảo đảm
client B kết nối được.

Kiểm thử vật lý A/B cần một máy riêng; native server smoke hoặc bắt tay TLS
trên chính máy A không thay thế được kiểm thử đó.
