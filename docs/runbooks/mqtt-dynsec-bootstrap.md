# Hướng dẫn Bootstrap DynSec Offline cho Mosquitto bằng Python Orchestrator

Tài liệu này hướng dẫn quy trình tạo mới (bootstrap) tệp lưu trữ Dynamic Security (`dynamic-security.json`) lần đầu cho Mosquitto 2.0.18 ở chế độ offline.

Quy trình sử dụng script orchestrator Python 3 trên host (`scripts/mqtt-bootstrap.py`) kết hợp với công cụ gốc `mosquitto_ctrl` chạy bên trong Docker container sử dụng image Mosquitto 2.0.18 đã được ghim cố định (`eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`).

> **Lưu ý quan trọng về phạm vi:**
> - Tài liệu này **chỉ mô tả quy trình thực hiện**; không tự ý chạy lệnh phân bổ tài nguyên Docker, không đọc/ghi `.env`, và không tự ý cutover hay triển khai production trong task này.
> - Phương thức container (**Container Mode**) là phương thức khuyến nghị chính. Host mode (cài đặt `mosquitto_ctrl` trực tiếp trên host) chỉ là tùy chọn phụ được trình bày ở phần Phụ lục.

---

## 1. Nguyên lý hoạt động và mô hình bảo mật

1. **Không cài đặt Python trong image Mosquitto:**
   Image Mosquitto chính thức không có sẵn Python. Script `scripts/mqtt-bootstrap.py` chạy trên Python 3 của máy host và điều phối các container phụ trợ ngắn hạn (ephemeral containers) thông qua Docker CLI để thực thi `mosquitto_ctrl dynsec init` bên trong image.
2. **Yêu cầu image cục bộ đã tải trước (pinned image):**
   Script yêu cầu image Mosquitto phải có sẵn cục bộ với cấu trúc `linux/amd64`, đúng tag/digest ghim cố định. Script chạy với cờ cấm tải mạng (`--pull=never`). Cần lưu ý: script kiểm tra tính hợp lệ cụ thể của image cục bộ này bằng việc so sánh trường `Id` (sha256 digest) và `RepoDigests` theo đúng cấu hình ghim của dự án, chứ không khái quát hóa rằng image config ID luôn trùng với RepoDigest trong mọi trường hợp Docker.
3. **Volume mới và phân quyền danh tính (1883:1883):**
   - Script tạo một Docker named volume hoàn toàn mới để chứa `dynamic-security.json`.
   - Quyền hạn bên trong container bắt buộc là thư mục mode `0700` và tệp mode `0600`, thuộc sở hữu của UID:GID `1883:1883` (danh tính người dùng không đặc quyền của Mosquitto).
   - Danh tính `1883:1883` ở đây là UID/GID trong user namespace của container, không phải là xác nhận về UID trên host khi chạy rootless/userns.
   - Script sử dụng một root container helper ngắn hạn (`--user 0:0`) chỉ nhằm mục đích khởi tạo phân quyền ban đầu cho volume mới này. Operator chạy script thông qua Docker daemon authorization thông thường, không yêu cầu lệnh `sudo` mặc định.
4. **Bảo vệ tài nguyên hiện hữu (Anti-collision):**
   - Script từ chối ghi đè lên volume đã tồn tại (`volume collision`).
   - Tuyệt đối không chạy lệnh `docker compose down -v` làm mất dữ liệu, không bootstrap đè lên store đang hoạt động và không xóa các volume chia sẻ.
5. **Đầu ra an toàn (Safe metadata):**
   - Khi thành công, script in ra cấu trúc JSON chứa tên volume mới (`volume_name`), trạng thái khởi tạo `store_created`, và `runtime_status: NOT_READY`.
   - Trạng thái `NOT_READY` là bình thường và có chủ đích: script chỉ khởi tạo tệp dữ liệu offline, chưa bật cổng ingress, chưa kiểm tra kết nối mạng hay kích hoạt broker runtime.
   - Volume được giữ nguyên có chủ đích trên Docker engine sau khi bootstrap thành công để operator sử dụng cho bước cấu hình tiếp theo.
6. **Bảo mật bí mật (No secret leaks):**
   - Mật khẩu không bao giờ xuất hiện trong đối số dòng lệnh (`argv`), stdout, log hệ thống, hay qua lệnh `source .env`.
   - File cấu hình chứa mật khẩu phải được bảo vệ nghiêm ngặt (chỉ operator sở hữu, phân quyền `0600` trong thư mục `0700`).

---

## 2. Kiểm tra phụ thuộc ban đầu (Dependency Check)

Trước khi tạo secret hay volume, kiểm tra tính sẵn sàng của image Mosquitto cục bộ bằng lệnh:

```bash
python3 scripts/mqtt-bootstrap.py --container --check-native-tool
```

- **Hành vi thực tế:** Lệnh này sẽ kiểm tra metadata của image cục bộ và khởi chạy một container tạm thời (`docker run --rm --network none --pull=never`) để kiểm tra `mosquitto_ctrl --help`.
- **Lưu ý:** Lệnh này **không cấp phát volume hay store nào**, nhưng có tạo và xóa một container công cụ tạm thời.
- **Kết quả kỳ vọng:**
  ```text
  container image dependency verified; no store written; NOT READY
  ```
  Nếu image chưa có sẵn hoặc sai digest/kiến trúc, lệnh sẽ dừng lại với lỗi rõ ràng. Không tự ý chạy `docker pull` nếu chưa có chỉ đạo.

---

## 3. Tạo thông tin xác thực và tệp cấu hình bảo vệ

Operator cần chuẩn bị thông tin đăng nhập cho 2 tài khoản quản trị nội bộ:
- Quản trị viên Dynamic Security: tên đăng nhập phải khớp với biến `MQTT_DYNSEC_MANAGER_USERNAME` trong `.env` (mặc định: `credential_manager`).
- Dịch vụ Go Backend: tên đăng nhập phải khớp với biến `MQTT_USERNAME` trong `.env` (mặc định: `backend_service`).

Hai tài khoản này phải có mật khẩu riêng biệt, độ dài từ 32 đến 128 ký tự in được (ASCII printable, mã 33–126), không trùng nhau.

Chạy đoạn mã Python độc lập dưới đây trong terminal tương tác trên host để tạo thư mục con riêng dưới `/tmp/opencode` (mode `0700`) và tệp cấu hình JSON (mode `0600`). Script hỗ trợ nhập mật khẩu an toàn qua `getpass` (không hiển thị ký tự) hoặc tự động sinh ngẫu nhiên bằng `secrets.token_urlsafe(32)`.

Vì `python3 - <<'PY'` dùng stdin để nhận mã nguồn, các câu hỏi phải đọc từ `/dev/tty`, không dùng `input()`. Nếu không có terminal tương tác, đoạn mã sẽ dừng trước khi tạo file:

```bash
python3 - <<'PY'
import getpass
import json
import os
from pathlib import Path
import secrets
import stat
import tempfile

# Đọc câu trả lời từ terminal, không từ stdin đang chứa mã nguồn.
try:
    tty_in = open('/dev/tty', 'r')
    tty_out = open('/dev/tty', 'w')
except OSError:
    raise SystemExit('Lỗi: Hãy chạy đoạn mã trong terminal tương tác có /dev/tty.')

def ask(prompt):
    tty_out.write(prompt)
    tty_out.flush()
    answer = tty_in.readline()
    if not answer:
        raise SystemExit('Lỗi: Không nhận được câu trả lời từ terminal.')
    return answer.strip()

# 1. Không đổi permissions thư mục dùng chung; tạo thư mục con riêng 0700.
base_dir = Path('/tmp/opencode')
base_dir.mkdir(parents=True, exist_ok=True)
if base_dir.is_symlink():
    raise SystemExit('Lỗi: /tmp/opencode không được là symlink.')
private_dir = Path(tempfile.mkdtemp(prefix='dynsec-credentials-', dir=base_dir))

# 2. Tạo đường dẫn tệp JSON bảo vệ với tên ngẫu nhiên
nonce = secrets.token_hex(8)
config_path = private_dir / f'dynsec-bootstrap-{nonce}.json'

print("=== CẤU HÌNH THÔNG TIN XÁC THỰC BOOTSTRAP DYNSEC ===")
manager_user = ask("Manager username (khớp MQTT_DYNSEC_MANAGER_USERNAME, ví dụ: credential_manager): ")
backend_user = ask("Backend username (khớp MQTT_USERNAME, ví dụ: backend_service): ")

if not manager_user or not backend_user or manager_user == backend_user:
    raise SystemExit("Lỗi: Tên đăng nhập manager và backend phải khác rỗng và không trùng nhau.")

def prompt_password(prompt_text):
    choice = ask(f"Tự sinh mật khẩu ngẫu nhiên cho {prompt_text}? (Y/n): ").lower()
    if choice in ('', 'y', 'yes'):
        return secrets.token_urlsafe(32)
    pwd = getpass.getpass(f"Nhập mật khẩu cho {prompt_text} (32-128 ký tự ASCII): ")
    if not (32 <= len(pwd) <= 128) or any(ord(c) < 33 or ord(c) > 126 for c in pwd):
        raise SystemExit("Lỗi: Mật khẩu phải từ 32 đến 128 ký tự in được (printable ASCII, không chứa khoảng trắng).")
    return pwd

manager_pwd = prompt_password(manager_user)
backend_pwd = prompt_password(backend_user)

if manager_pwd == backend_pwd:
    raise SystemExit("Lỗi: Mật khẩu của manager và backend phải khác nhau.")

cfg = {
    "manager": manager_user,
    "backend": backend_user,
    "manager_password": manager_pwd,
    "backend_password": backend_pwd
}

# 3. Ghi file với cờ O_EXCL và phân quyền 0600
fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
with os.fdopen(fd, 'w') as stream:
    json.dump(cfg, stream)
    stream.flush()
    os.fsync(stream.fileno())

# Chỉ in ra đường dẫn tệp, tuyệt đối không in mật khẩu ra terminal
print(f"\n[OK] Đã tạo tệp cấu hình bảo vệ tại: {config_path.resolve()}")
print("HƯỚNG DẪN TIẾP THEO:")
print("1. Sao chép các mật khẩu trên vào file .env gốc bằng trình soạn thảo cục bộ an toàn:")
print("   - MQTT_DYNSEC_MANAGER_PASSWORD=<mật khẩu manager>")
print("   - MQTT_PASSWORD=<mật khẩu backend>")
print("2. GIỮ LẠI tệp cấu hình này để thực hiện lệnh bootstrap ở Bước 4.")
print("3. Chỉ XÓA tệp cấu hình này sau khi quá trình cấu hình và đối soát hoàn tất.")
PY
```

> **Quy tắc bảo quản secret:**
> - Operator tự chịu trách nhiệm sao chép mật khẩu sang `.env` bằng trình soạn thảo nội bộ bảo vệ. Tuyệt đối không hiển thị mật khẩu bằng `cat` hay `echo`.
> - Không xóa tệp cấu hình tạm trước khi hoàn tất lệnh bootstrap, tránh trường hợp thất bại không thể phục hồi secret. Xóa thủ công tệp tạm này sau khi đã xác minh xong.

---

## 4. Thực thi lệnh Bootstrap (Container Mode)

Sau khi tệp cấu hình đã được tạo và kiểm tra phân quyền an toàn, chạy lệnh bootstrap chỉ định tệp cấu hình:

```bash
read -r -p 'Đường dẫn tệp cấu hình bảo vệ (từ Bước 3): ' BOOTSTRAP_CONFIG
python3 scripts/mqtt-bootstrap.py --container --config "$BOOTSTRAP_CONFIG"
```

*(Hoặc có thể truyền qua stdin: `python3 scripts/mqtt-bootstrap.py --container < "$BOOTSTRAP_CONFIG"`).*

### Kết quả đầu ra kỳ vọng:
Script sẽ in ra siêu dữ liệu an toàn dưới định dạng JSON (không chứa bất kỳ mật khẩu nào):

```json
{
  "status": "store_created",
  "volume_name": "mosquitto_dynsec_a1b2c3d4e5f60718293a4b5c",
  "runtime_status": "NOT_READY",
  "gate": "unchanged",
  "ownership": "1883:1883",
  "mode": "container"
}
```

- Ghi lại giá trị `volume_name` (ví dụ: `mosquitto_dynsec_a1b2c3d4e5f60718293a4b5c`) để sử dụng trong cấu hình Docker Compose.
- Volume này đã chứa `dynamic-security.json` với đầy đủ hash người dùng và các role mặc định (`bootstrap_manager`, `bootstrap_backend`), từ chối toàn bộ truy cập không được cấp phép (`defaultACLAccess = false`).

---

## 5. Cấu hình Docker Compose với Volume mới qua `.env`

Tệp `docker-compose.yml` gốc đã tích hợp sẵn hỗ trợ định danh volume linh hoạt:
```yaml
volumes:
  mosquitto_dynsec:
    name: ${MQTT_DYNSEC_BOOTSTRAP_VOLUME:-mosquitto_dynsec}
```

Do đó, Platform Operator **không cần tạo thêm tệp override cục bộ**, mà chỉ cần cấu hình trực tiếp vào tệp `.env`:

1. Mở tệp `.env` bằng trình soạn thảo an toàn.
2. Thiết lập biến `MQTT_DYNSEC_BOOTSTRAP_VOLUME` bằng giá trị `volume_name` thu được từ Bước 4:
   ```bash
   # Cập nhật trong .env:
   MQTT_DYNSEC_BOOTSTRAP_VOLUME=mosquitto_dynsec_a1b2c3d4e5f60718293a4b5c
   ```
3. Đồng thời xác nhận các biến thông tin đăng nhập trong `.env` đã được điền chính xác:
   - `MQTT_DYNSEC_MANAGER_USERNAME`: Khớp với tên manager đã nhập.
   - `MQTT_DYNSEC_MANAGER_PASSWORD`: Khớp với mật khẩu manager đã nhập.
   - `MQTT_USERNAME`: Khớp với tên backend đã nhập.
   - `MQTT_PASSWORD`: Khớp với mật khẩu backend đã nhập.
   - `MQTT_CREDENTIAL_API_ENABLED=true`: Bật API quản trị credential sau khi store đã sẵn sàng.

---

## 6. Quy trình chuyển đổi và khởi động kiểm tra (Cutover & Verification)

> **CẢNH BÁO QUAN TRỌNG:**
> - Đây là bước chuyển đổi do operator chủ động quyết định và kiểm soát hoàn toàn, **không được tự động kích hoạt**.
> - Chỉ thực hiện bước này khi cơ sở dữ liệu PostgreSQL, các migration và các điều kiện tiên quyết trong `.env` đã thực sự sẵn sàng.
> - Tuyệt đối không chạy lệnh khởi động toàn bộ stack hay các tunnel dịch vụ (`cloudflared`, `rathole`) một cách vô tình khi chưa sẵn sàng.

### Bước 6.1: Dừng các container liên quan
Dừng dịch vụ `backend` và `mosquitto` hiện tại:

```bash
docker compose stop backend mosquitto
```

### Bước 6.2: Khởi tạo lại với volume mới
Tạo lại và khởi động hai dịch vụ (Docker Compose sẽ tự động nhận diện `MQTT_DYNSEC_BOOTSTRAP_VOLUME` từ `.env`):

```bash
docker compose up -d mosquitto backend
```

> **Lưu ý về cờ `--no-deps`:**
> Tránh sử dụng cờ `--no-deps` trong môi trường tiêu chuẩn vì cờ này sẽ bỏ qua việc kiểm tra phụ thuộc (như PostgreSQL, các container chạy migration). Hãy để Docker Compose kiểm tra phụ thuộc bình thường để đảm bảo database và migration tương thích đã hoàn tất.

### Bước 6.3: Xác minh độc lập sau khởi động
1. **Kiểm tra mật khẩu đồng bộ:**
   Đảm bảo mật khẩu của `credential_manager` và `backend_service` trong `.env` khớp với thông tin đã nhập khi bootstrap. Container `backend` chỉ đọc lại cấu hình `.env` khi được recreate.
2. **Kiểm tra TLS và đăng nhập DynSec:**
   Sử dụng client MQTT kiểm tra kết nối TLS tới cổng 8883 với chứng chỉ CA hợp lệ. Kiểm tra danh tính `credential_manager` và `backend_service` có thể xác thực và chỉ được phép publish/subscribe đúng topic quy định.
3. **Kiểm tra readiness của Go Backend:**
   Kiểm tra endpoint sức khỏe của backend:
   ```bash
   curl -s http://127.0.0.1:8080/readyz
   ```
4. **Không in hoặc trích xuất log chứa thông tin nhạy cảm:**
   Tránh chạy `docker compose config` hoặc dump log không lọc vì có thể làm lộ các biến môi trường nhạy cảm.

### Khả năng hoàn tác (Rollback):
Volume cũ vẫn được giữ nguyên trên hệ thống. Trong trường hợp cần hoàn tác, operator chỉ cần gỡ bỏ file override và chạy lại `docker compose up -d mosquitto backend`. Tuy nhiên, nếu volume cũ trước đó chưa từng có tệp store DynSec hợp lệ thì nó cũng sẽ không thể sử dụng cho chế độ Dynamic Security.

---

## 7. Xử lý sự cố thường gặp (Troubleshooting)

1. **Lỗi `pinned image not found locally; pulling is forbidden`:**
   - *Nguyên nhân:* Image `eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9` chưa có sẵn trong Docker cache cục bộ.
   - *Khắc phục:* Tải trước image bằng quy trình cung ứng chuẩn của dự án; script cấm tự động pull.
2. **Lỗi Docker daemon / Docker socket:**
   - *Nguyên nhân:* Người dùng hiện tại không có quyền truy cập Docker socket (`/var/run/docker.sock`) hoặc daemon chưa chạy.
   - *Khắc phục:* Kiểm tra trạng thái Docker daemon và đảm bảo user thuộc nhóm `docker` được cấp quyền.
3. **Lỗi `volume collision`:**
   - *Nguyên nhân:* Tên volume được chỉ định trong cấu hình đã tồn tại trên Docker.
   - *Khắc phục:* Bỏ trường `volume_name` trong file cấu hình để script tự sinh tên volume ngẫu nhiên duy nhất, hoặc chọn một tên volume mới chưa từng tồn tại.
4. **Lỗi quyền truy cập file cấu hình (`configuration file permissions must be exactly 0600`):**
   - *Nguyên nhân:* File cấu hình hoặc thư mục cha vi phạm kiểm tra an toàn (không phải mode 0600, có symlink, hoặc thư mục cha có quyền ghi công khai).
   - *Khắc phục:* Đảm bảo file được tạo bên trong thư mục `0700` thuộc sở hữu của user và file có quyền `0600`.
5. **Môi trường Docker Rootless:**
   - Trong một số môi trường rootless phức tạp, nếu việc ánh xạ quyền (user namespace mapping) không cho phép gắn kết mount hoặc phân quyền, script có thể báo lỗi. Khi đó cần kiểm tra lại cấu hình user namespace của daemon.
6. **Xử lý khi script gặp lỗi một phần (Partial Failure):**
   - Khi gặp sự cố giữa chừng, script sẽ cố gắng dọn dẹp các container tạm thời và volume hỏng. Nếu quá trình dọn dẹp gặp trục trặc, script sẽ in thông báo cảnh báo rõ ràng (kèm theo container ID hoặc volume name).
   - **Tuyệt đối không chạy lại lệnh một cách mù quáng (blind rerun).** Operator phải kiểm tra trạng thái Docker (`docker volume ls`, `docker ps -a`) để đối soát các tài nguyên chưa dọn dẹp trước khi thử lại.

---

## Phụ lục: Khởi tạo trực tiếp trên Host (Host Mode - Tùy chọn)

Nếu môi trường không sử dụng Docker để bootstrap và bắt buộc sử dụng binary `mosquitto_ctrl 2.0.18` cài sẵn trên host:

1. **Yêu cầu:** Binary `/usr/bin/mosquitto_ctrl` và thư viện `libmosquitto` 2.0.18 phải do root sở hữu, không có quyền ghi nhóm/khác và không dùng symlink. Thư mục đích trên host phải trống, phân quyền `0700` và thuộc sở hữu của UID:GID `1883:1883` (hoặc ánh xạ rootless tương ứng).
2. **Kiểm tra binary host:**
   ```bash
   python3 scripts/mqtt-bootstrap.py --check-native-tool --native-tool /usr/bin/mosquitto_ctrl
   ```
3. **Thực thi:**
   Tạo file cấu hình JSON chứa đầy đủ các trường `store`, `manager`, `backend`, `manager_password`, `backend_password`, `mapping`, `broker_uid`, `broker_gid` và thực thi:
   ```bash
   python3 scripts/mqtt-bootstrap.py --native-tool /usr/bin/mosquitto_ctrl < /path/to/protected-config.json
   ```

*Chi tiết về Host Mode được lưu trữ phục vụ tham khảo; chế độ Container Mode ở các mục trên là phương thức chuẩn được hỗ trợ chính thức.*

---

## Tài liệu liên quan

- Tổng quan kiến trúc DynSec: [docs/dynsec/INDEX.md](../dynsec/INDEX.md)
- Quy trình triển khai và vận hành chi tiết: [docs/dynsec/deployment-and-operations.md](../dynsec/deployment-and-operations.md)
- Giới hạn kỹ thuật và rủi ro: [docs/dynsec/limitations-and-backlog.md](../dynsec/limitations-and-backlog.md)
