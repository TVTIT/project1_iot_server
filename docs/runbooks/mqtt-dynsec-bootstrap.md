# Bootstrap DynSec offline bằng Python trên host

## Phạm vi và dependency

Image broker được pin Mosquitto 2.0.18 không cung cấp Python. Không chạy
`scripts/mqtt-bootstrap.py` trong image đó. Delivery được hỗ trợ là Python 3
trên Linux host gọi **trực tiếp** binary `mosquitto_ctrl` cùng `libmosquitto`
**2.0.18** trên host. Không truyền chuỗi shell, Docker wrapper hay binary phiên
bản khác vào `--native-tool`.

Prerequisite do operator chuẩn bị riêng: cài native tool từ gói hoặc build
Mosquitto nguồn tin cậy, pin release 2.0.18 và xác minh nguồn/checksum theo quy
trình cung ứng của môi trường. Build phải bao gồm `mosquitto_ctrl`, hỗ trợ
`dynsec init`, cùng các thư viện runtime tương ứng. Binary, thư viện và cấu
hình dynamic loader phải thuộc nguồn quản trị tin cậy; không dùng
`LD_LIBRARY_PATH` hoặc `LD_PRELOAD` để thay thư viện. Script không tải hay build
dependency, không chứng minh provenance bằng việc đọc version banner. Nếu chưa
có dependency này thì **dừng trước khi tạo secret/store**, không đổi pin.

Binary và mọi ancestor phải root-owned, không group/world-writable và không
symlink. Dùng đường dẫn tuyệt đối tới file thật (không phải symlink).
Kiểm tra không đọc stdin, không cần volume, không gọi Docker:

```bash
# Chỉ đặt biến local mới; không source .env hoặc import cấu hình production.
read -r -p 'Absolute trusted mosquitto_ctrl path: ' NATIVE_TOOL
python3 scripts/mqtt-bootstrap.py --check-native-tool --native-tool "$NATIVE_TOOL" </dev/null
```

Exit 0 chỉ xác nhận kiểm tra dependency đã qua, không xác nhận bootstrap,
broker runtime, readiness, JWT hay API. Khi thiếu binary hoặc version sai,
exit khác 0 và không có store được ghi. `dynsec init` còn được kiểm tra khi
bootstrap thực sự chạy; không khẳng định tính năng đó từ banner.

## Đích mới và ownership: prerequisite trước khi ghi

Các bước dưới đây là thủ tục cho lần thực thi **được cấp quyền riêng sau này**,
không phải lệnh cutover hay yêu cầu chạy trên volume hiện có trong task này.

- Chỉ dùng volume mới thuộc operator, chưa gắn với broker đang chạy. Không dùng
  store triển khai hiện hữu, không import static password hoặc legacy state.
- Operator cung cấp đường dẫn host tới thư mục riêng, trống, mode `0700`.
  Thư mục phải có UID/GID host chính xác của broker writer container
  `1883:1883`. Rootful: host `1883:1883`; rootless: xác minh mapping thực tế,
  không đoán từ UID login. Các ancestor phải qua guard của script.
- Không chmod/chown tài nguyên hiện có để vượt qua lỗi guard. Chuẩn bị ownership
  chỉ cho tài nguyên mới được phép tạo. Script không tạo/chown thư mục.
- Chạy bootstrap dưới identity được phép ghi trong thư mục đó và thiết lập
  ownership file mới chính xác. Với rootful, thường cần operator host đặc
  quyền; với rootless, dùng identity phù hợp mapping. Không dùng `sudo` mặc
  định nếu chưa được cấp quyền. Backend chỉ mount directory read-only; reader
  không thay thế identity writer.
- Store đầu ra phải tên `dynamic-security.json`; chỉ chứa native hashes,
  mode `0600`, UID/GID như thư mục. Không mount runtime hay bật API trong bước
  bootstrap. Không seed DB hoặc success/readiness record.

## Sinh cấu hình và secret độc lập vào file bảo vệ

Trong shell riêng không tracing (`set +x`), sau khi dependency đã qua và đích
mới đã được chuẩn bị, đoạn sau hỏi các giá trị không-secret. File cấu hình
được tạo exclusive `0600` trong thư mục `0700` thuộc operator; không in secret.
Không dùng stdout redirect đè file có sẵn. Dùng một thư mục credential riêng,
không phải thư mục store vốn phải trống.

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path
import secrets
import stat

target = Path(input('Absolute new protected input file path: '))
if not target.is_absolute():
    raise SystemExit('Absolute path required')
parent = target.parent
if any(p.is_symlink() for p in (parent, *parent.parents)):
    raise SystemExit('No symlink ancestors allowed')
meta = parent.stat()
if meta.st_uid != os.geteuid() or stat.S_IMODE(meta.st_mode) != 0o700:
    raise SystemExit('Input directory must be operator-owned 0700')
mapping = input('Verified mapping (rootful/rootless): ')
if mapping not in ('rootful', 'rootless'):
    raise SystemExit('Explicit mapping required')
cfg = {
    'store': input('Absolute fresh store path ending in dynamic-security.json: '),
    'manager': input('Manager principal name: '),
    'backend': input('Separate backend principal name: '),
    'manager_password': secrets.token_urlsafe(32),
    'backend_password': secrets.token_urlsafe(32),
    'mapping': mapping,
    'broker_uid': int(input('Verified host writer UID: ')),
    'broker_gid': int(input('Verified host writer GID: ')),
}
if mapping == 'rootless':
    # Enter actual verified mapping rows, e.g. [[container_start, host_start, count]].
    cfg['uid_map'] = json.loads(input('Verified UID mapping rows as JSON: '))
    cfg['gid_map'] = json.loads(input('Verified GID mapping rows as JSON: '))
if Path(cfg['store']).name != 'dynamic-security.json':
    raise SystemExit('Native store filename required')
fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
with os.fdopen(fd, 'w') as stream:
    json.dump(cfg, stream)
    stream.flush()
    os.fsync(stream.fileno())
print('Protected input created; no credentials printed')
PY
```

Đường dẫn credential/store và principal là input operator, không hard-code
deployment. Script bootstrap sẽ kiểm tra strict schema, mapping, principal và
credential trước khi ghi. Không sửa rộng permissions để identity khác đọc
file. Giữ hai secret riêng trong file bảo vệ; việc cấp manager/backend secret
cho fixture hoặc runtime qua protected files là bước riêng được cấp quyền.
Không đưa secret vào argv, shell history, log, report hay `.env` production.

## Lệnh bootstrap thực sự (chỉ sau khi được cấp quyền ghi đích mới)

```bash
read -r -p 'Absolute protected input file path: ' BOOTSTRAP_INPUT
python3 scripts/mqtt-bootstrap.py --native-tool "$NATIVE_TOOL" < "$BOOTSTRAP_INPUT"
```

Lệnh này chạy trên host dưới identity đã xác định ở trên. Shell chỉ truyền
đường dẫn vào argv; JSON và secret đi qua stdin. Hai lần native init chạy trong
thư mục temporary riêng, dùng stdin cho password, không xuất native output.
Publish dùng hard link exclusive, không overwrite. Không retry mù khi có lỗi:
xác minh offline đích mới trước; không xoá store đã tồn tại để chạy lại.

Thông báo `NOT READY` là cố ý. Kiểm tra metadata của file mới mà không dump
nội dung; không có database, MQTT network, JWT, API hay cutover trong thủ tục
này. Việc qualification broker Mosquitto 2.0.18 linux/amd64 và HTTP fixture
với manager/backend hợp lệ vẫn là bước kiểm thử riêng.

---

## Tài liệu liên quan

- Tổng quan kiến trúc DynSec: [docs/dynsec/INDEX.md](../dynsec/INDEX.md)
- Quy trình triển khai và vận hành chi tiết: [docs/dynsec/deployment-and-operations.md](../dynsec/deployment-and-operations.md)
- Giới hạn kỹ thuật và rủi ro: [docs/dynsec/limitations-and-backlog.md](../dynsec/limitations-and-backlog.md)
