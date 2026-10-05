# Stage 2 — Task 2.5: Hạ tầng Runtime Credential File Tĩnh cho Mosquitto (Legacy Baseline & Regression)

> **Thông báo Quan trọng về Phạm vi Lịch sử & Kế thừa (Superseded Scope Notice):**
> - **Bản chất tài liệu:** Tài liệu này đặc tả kiến trúc kỹ thuật, cơ chế an toàn và bằng chứng kiểm thử của **Hạ tầng Runtime Credential File Tĩnh (`password_file` + SIGHUP)** thuộc Task 2.5. Phân hệ này còn trong codebase và root Compose để vận hành legacy/kiểm thử hồi quy. Đây **không phải automatic fallback** từ DynSec; chuyển runtime cần cutover được duyệt, không được khôi phục credential cũ để bypass revoke.
> - **Đã được thay thế bởi Task 2.6 & Task 2.7:** Đối với luồng nghiệp vụ quản trị credential động thời gian thực của Gateway, giải pháp chỉnh sửa file tĩnh và phát tín hiệu SIGHUP tại Task 2.5 đã được **thay thế hoàn toàn (superseded)** bởi kiến trúc **Mosquitto Dynamic Security (DynSec) plugin kết hợp cổng Ingress Gate** tại [Sổ tay Vận hành Task 2.6](stage-2-task-2.6-mqtt-credentials.md), [Báo cáo Nghiệm thu Task 2.6](stage-2-task-2.6-acceptance.md) và kế hoạch nghiệm thu tích hợp [Kế hoạch Chi tiết Task 2.7](/media/trung/SSD2-Data/Project1_ET3290/docs/backend_plan/task_2.7_detail_plan.md).
> - **Không đồng nhất chứng cứ đĩa cũ với DynSec:** Các nhân chứng đĩa cục bộ của Task 2.5 (`.op-<opID>/candidate`, `passwd.last-good`, marker `.credential.pending`, `fsync` file và thư mục) là cơ chế đồng bộ riêng của file tĩnh legacy, **tuyệt đối không được diễn giải lại thành bằng chứng vận hành hay chứng cứ an toàn cho DynSec**. DynSec sử dụng kênh quản trị `$CONTROL/dynamic-security/v1`, đối soát trạng thái RAM/snapshot `dynamic-security.json` và hàng rào Ingress Gate `CLOSED/OPEN`.
> - **Hiện trạng Root Compose và Cờ API:** File `docker-compose.yml` gốc của dự án hiện vẫn đang cấu hình dịch vụ Mosquitto gắn với named volume tĩnh legacy (`mosquitto_auth:/mosquitto/auth:ro`, `password_file /mosquitto/auth/passwd`) và **chủ động vô hiệu hóa API quản trị credential** (`MQTT_CREDENTIAL_API_ENABLED: "false"`). Các template cấu hình lifecycle và fixture isolated harness **không đồng nghĩa với việc phê duyệt tự động bật trên môi trường production** (no automatic rollout approval).
> - **Phân định lưu trữ mật khẩu:** PostgreSQL là nguồn thẩm quyền nghiệp vụ duy nhất nhưng **tuyệt đối KHÔNG lưu mật khẩu plaintext hay password hash**. Chuỗi băm mật khẩu native chỉ tồn tại trong file mật khẩu broker (`passwd` đối với legacy static runtime hoặc `dynamic-security.json` đối với DynSec). Không có cơ chế import hash trong suốt (transparent hash import) vào CSDL nghiệp vụ.

---

## 1. Mục tiêu và ranh giới

Tài liệu này ghi nhận kiến trúc, cơ chế bảo mật, quy trình vận hành và kết quả kiểm chứng lịch sử của **Task 2.5: Hạ tầng runtime credential file tĩnh cho Mosquitto**.

### 1.1 Mục tiêu

Xây dựng phân hệ hạ tầng độc lập, an toàn và có khả năng phục hồi khi gặp sự cố (crash-safe) để Go Backend có thể quản lý file mật khẩu tĩnh của Mosquitto trong thời gian thực (runtime):

```text
Yêu cầu thao tác (OpID)
    │
    ├── 1. Bounded admission & File lock (.credential.lock)
    ├── 2. Tạo candidate file trong staging (.op-<opID>/)
    ├── 3. Native utility hashing (mosquitto_passwd qua stdin)
    ├── 4. Kiểm tra toàn vẹn candidate & fsync file
    ├── 5. Ghi intent marker (.credential.pending) & fsync thư mục
    ├── 6. Atomic swap (rename candidate đè lên passwd) & fsync thư mục
    ├── 7. Gửi yêu cầu reload qua private Unix socket tới sidecar
    ├── 8. Sidecar gửi SIGHUP tới Mosquitto PID 1 qua pidfd
    ├── 9. Probe kiểm chứng kết nối MQTT TLS mới (Paho client)
    └── 10. Hoàn tất: Cập nhật last-good, dọn dẹp staging, giải phóng lock
```

### 1.2 Ranh giới nghiệp vụ (Boundaries)

- **Thuộc phạm vi Task 2.5 (Historical Scope)**:
  - Quản lý file mật khẩu runtime trong named volume, loại bỏ việc dùng file tracked trong git làm runtime store.
  - One-shot initializer chuẩn bị quyền hạn thư mục và tài khoản hệ thống nội bộ `backend_service`.
  - Tích hợp công cụ native `mosquitto_passwd` (PBKDF2-SHA512) qua wrapper Go an toàn.
  - Cơ chế đồng bộ (serialization) với file lock `flock`, candidate staging, fsync file & thư mục, atomic rename, và rollback snapshot.
  - Sidecar `mosquitto-reloader` chỉ gửi `SIGHUP` tới broker qua Unix Domain Socket (không dùng Docker socket, không dùng host PID).
  - Probe kiểm chứng xác thực bằng kết nối MQTT TLS 1.2 mới toanh.
  - Bộ kiểm thử tự động (Python harness + Go unit/race/integration) và bổ sung job CI tương ứng.

- **Ranh giới chuyển tiếp và ranh giới ngoài phạm vi (Superseded Boundary)**:
  - Tại thời điểm hoàn thành Task 2.5, chưa mở HTTP REST endpoint nào cho việc provision/rotate/revoke/metadata MQTT credential của Gateway. Các API này đã được triển khai tại **Task 2.6** qua kiến trúc Mosquitto Dynamic Security (DynSec), không dùng file tĩnh.
  - Chưa kết nối thao tác cập nhật file với các bảng PostgreSQL (`gateway_mqtt_credentials`, `gateway_mqtt_credential_events`, `mqtt_credential_maintenance`, `mqtt_credential_recovery`) — các bảng này được quản lý đồng bộ với DynSec tại Task 2.6.
  - Chưa có API quản lý membership, phân quyền người dùng, public signup hay self-claim (thuộc phạm vi bảo vệ tại Task 2.2A, 2.3 và Task 2.7).
  - Chưa triển khai luồng thu thập telemetry, streaming WebSocket hay điều khiển thiết bị Digital Twin.
  - Task 2.5 không sử dụng Mosquitto Dynamic Security plugin; việc chuyển đổi sang DynSec được thực hiện từ Task 2.6 trở đi.
  - Không cam kết duy trì nhiều replica backend cùng ghi một volume hay hỗ trợ nhiều active password cùng lúc cho một Gateway.

---

## 2. Kiến trúc Runtime & Phân quyền cách ly

### 2.1 Cấu trúc Named Volumes

Hệ thống tách biệt hoàn toàn volume chứa dữ liệu xác thực với volume điều khiển tín hiệu:

```text
mosquitto_auth (Named Volume)
├── passwd                   # File mật khẩu chính thức Mosquitto đọc (0600)
├── passwd.last-good         # Snapshot trạng thái đã kiểm chứng thành công gần nhất (0600)
├── .credential.lock         # Inode cố định phục vụ serialization bằng flock (0600)
├── .credential.pending      # Marker ghi nhận intent đang thực hiện (0600)
└── .op-<opID>/              # Thư mục staging riêng biệt cho từng thao tác (0700)
    ├── candidate            # Bản sao đang được chỉnh sửa qua mosquitto_passwd
    └── snapshot             # Bản sao dự phòng để rollback nếu probe thất bại

mosquitto_control (Named Volume)
├── reload.sock              # Unix Domain Socket lắng nghe yêu cầu reload (0600)
└── .reload.lock             # File lock điều phối khởi động và dọn dẹp socket (0600)
```

### 2.2 Ma trận phân quyền và Mount giữa các dịch vụ

| Service | `mosquitto_auth` | `mosquitto_control` | Quyền chạy (UID:GID) | PID Namespace | Network Mode |
|---|---|---|---|---|---|
| `mosquitto-auth-init` | Read-Write | Read-Write | Khởi tạo: `0:0`, sau đó drop về `1883:1883` | Container riêng | `none` |
| `mosquitto` | **Read-Only (`:ro`)** | Không mount | `1883:1883` | Container riêng (PID 1) | `iot_net` |
| `backend` | Read-Write | **Read-Only (`:ro`)** | `1883:1883` | Container riêng | `iot_net` |
| `mosquitto-reloader` | Không mount | Read-Write | `1883:1883` | **`service:mosquitto`** | `none` |

### 2.3 Nguyên tắc bảo mật cốt lõi

1. **Đồng nhất danh tính số (Numeric UID 1883)**:
   - Tất cả các container liên quan (`mosquitto`, `backend`, `mosquitto-reloader`) đều thực thi dưới UID/GID `1883:1883` (tài khoản người dùng mặc định của Mosquitto).
   - Quyền thư mục auth và control được siết chặt ở `0700`, quyền các file và Unix socket ở `0600`.
   - Nhờ cùng UID, backend có thể tạo file mà broker đọc được mà không cần `chown`, đồng thời reloader có thể gửi tín hiệu tới tiến trình Mosquitto mà không cần cấp quyền root hay `CAP_KILL`.

2. **Lý do bắt buộc mount cả thư mục volume**:
   - Single-file bind mount (`/host/passwd:/mosquitto/config/passwd`) giữ nguyên inode tại thời điểm mount. Khi backend thực hiện thay thế nguyên tử bằng `os.Rename`, inode của file thay đổi khiến container Mosquitto không nhìn thấy nội dung mới.
   - Do đó, Mosquitto bắt buộc phải mount cả thư mục volume (`mosquitto_auth:/mosquitto/auth:ro`). Khi nhận tín hiệu SIGHUP, Mosquitto mở lại file `/mosquitto/auth/passwd` theo đường dẫn và thấy ngay inode mới.

3. **Từ chối triệt để Docker Socket và Host PID**:
   - Các container thuộc runtime này không mount Docker socket (`/var/run/docker.sock`), giảm một đường leo thang đặc quyền; điều này không loại bỏ mọi rủi ro container/host.
   - Không chia sẻ PID với máy chủ chủ quản (`pid: host`). Reloader chỉ chia sẻ PID cục bộ với container Mosquitto (`pid: "service:mosquitto"`).
   - Reloader và Initializer được cô lập mạng hoàn toàn (`network_mode: none`).

4. **Tăng cường bảo vệ container (Hardening)**:
   - Các container cấu hình `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`, và `read_only: true`.
   - Vùng nhớ tạm được cung cấp qua `tmpfs` chuyên dụng: `/tmp:rw,noexec,nosuid,size=1m,mode=0700,uid=1883,gid=1883`.

---

## 3. Initializer an toàn (`mosquitto-auth-init`)

Dịch vụ `mosquitto-auth-init` chạy one-shot trước khi Mosquitto và Backend khởi động (`restart: "no"`, Mosquitto `depends_on: { condition: service_completed_successfully }`).

### 3.1 Quy trình Bootstrap quyền hạn

1. Kiểm tra hai thư mục gốc `/mosquitto/auth` và `/mosquitto/control`:
   - Xác nhận đường dẫn thực, không phải symlink.
   - Nếu volume mới tinh thuộc sở hữu `root:root` (do Docker tạo ra): kiểm tra thư mục rỗng, thực hiện `chown 1883:1883` và `chmod 0700`.
   - Nếu volume đã có dữ liệu: bắt buộc phải thuộc `1883:1883` và quyền `0700`. Nếu sai quyền, initializer dừng ngay lập tức.
2. Hạ đặc quyền: Gọi `syscall.Setgroups([]int{})`, `syscall.Setgid(1883)`, `syscall.Setuid(1883)` để từ bỏ hoàn toàn quyền root trước khi xử lý file.

### 3.2 Khởi tạo và bảo vệ tài khoản `backend_service`

1. Đọc biến môi trường `MQTT_USERNAME` (bắt buộc phải là `backend_service`) và `MQTT_PASSWORD` (mật khẩu dịch vụ nội bộ).
2. Chiếm file lock `.credential.lock`.
3. Kiểm tra tính toàn vẹn của volume auth:
   - **Trường hợp volume rỗng**:
     * Tạo candidate file rỗng trong thư mục `.op-initialize`.
     * Gọi native utility `mosquitto_passwd` thêm tài khoản `backend_service`.
     * `fsync` candidate file.
     * Atomic rename thành `/mosquitto/auth/passwd`.
     * Tạo bản sao `/mosquitto/auth/passwd.last-good`.
     * `fsync` thư mục `/mosquitto/auth`.
   - **Trường hợp file `passwd` đã tồn tại**:
     * Đọc và parse cấu trúc file bằng strict parser.
     * Bảo toàn nguyên vẹn mọi tài khoản Gateway hiện có trong file.
     * Kiểm tra hash của tài khoản `backend_service` với mật khẩu trong `MQTT_PASSWORD`. Nếu khớp và `passwd.last-good` đồng nhất, giữ nguyên trạng thái và hoàn tất.
4. **Nguyên tắc Fail-closed**:
   - Nếu file `passwd` bị hỏng, hash của `backend_service` không khớp, `passwd.last-good` bị lệch, hoặc còn sót các file staging bất thường: Initializer báo lỗi rõ ràng và dừng lại với exit code 1 (`ErrRecoveryRequired`).
   - Initializer **tuyệt đối không tự ý ghi đè** hay xóa bỏ file mật khẩu đã có, bảo vệ an toàn cho toàn bộ tài khoản Gateway đã được cấp phát trước đó.

---

## 4. Cơ chế Atomic Persistence & Khóa đồng bộ File Tĩnh

Tất cả các thao tác thay đổi mật khẩu (upsert hoặc remove) trong Go Backend đối với file tĩnh legacy đều đi qua `mqttcredential.Runtime`.

### 4.1 Serialization và Bounded Admission

- **Giới hạn hàng đợi (Admission Control)**: Hệ thống duy trì số lượng request chờ tối đa (`MaxPending`, mặc định 8 slot) thông qua các file đánh dấu `.credential.slot.<index>`. Nếu vượt quá số lượng cho phép, trả về ngay lỗi `ErrRuntimeBusy`.
- **Khóa độc quyền (File Lock)**: Sử dụng hàm hệ thống `flock(unix.LOCK_EX)` trên file `.credential.lock`. Lock được duy trì liên tục xuyên suốt từ lúc bắt đầu đọc file đến khi probe kiểm chứng kết nối MQTT thành công.

### 4.2 Tương tác an toàn với Native Utility `mosquitto_passwd`

Spike đã chứng minh thuật toán băm `sha512-pbkdf2` của Mosquitto có format đặc thù (`$7$...`) không thể thay thế bằng thư viện băm thông thường. Backend sử dụng trực tiếp binary `/usr/bin/mosquitto_passwd` từ official image của Mosquitto:

1. Chạy lệnh: `mosquitto_passwd -H sha512-pbkdf2 <candidate_path> <username>`.
2. **Truyền mật khẩu qua `stdin`**: Gửi 2 dòng chứa mật khẩu kết thúc bằng `\n` vào luồng `stdin` của tiến trình con.
3. **Tuyệt đối không dùng cờ `-b`**: Không bao giờ truyền mật khẩu qua tham số dòng lệnh để tránh lộ mật khẩu trong bảng tiến trình hệ thống (`/proc/<pid>/cmdline`, `ps aux`) hay log hệ điều hành.
4. **Cách ly môi trường thực thi**: Tiến trình con chỉ nhận tập biến môi trường tối thiểu (`PATH`, `HOME`), không thừa kế các biến môi trường nhạy cảm của backend (JWT secret, DB password).
5. **Bảo mật Output**: Bỏ qua prompt từ `stdout`; bắt `stderr` vào buffer giới hạn để phát hiện lỗi kỹ thuật, tuyệt đối không in mật khẩu ra log.

### 4.3 Chu trình Staging, Fsync và Atomic Rename

Để đảm bảo không bao giờ để lại file mật khẩu ở trạng thái dở dang hay hỏng hóc khi hệ thống mất điện/crash:

1. Tạo thư mục staging riêng biệt: `.op-<opID>/` (quyền `0700`).
2. Sao chép nội dung file `passwd` hiện tại sang `.op-<opID>/candidate` và `.op-<opID>/snapshot`.
3. Gọi `mosquitto_passwd` để chỉnh sửa file `candidate`.
4. Parse và kiểm tra toàn vẹn file `candidate`:
   - Xác nhận tài khoản `backend_service` không bị thay đổi.
   - Với thao tác thêm/sửa: xác nhận duy nhất Gateway mục tiêu được cập nhật và hash khớp với mật khẩu mới.
   - Với thao tác xóa: xác nhận duy nhất Gateway mục tiêu bị loại bỏ.
   - Tất cả các tài khoản Gateway khác phải được giữ nguyên vẹn 100%.
5. Gọi `fsync` trên file `candidate`.
6. Gọi `fsync` trên thư mục staging `.op-<opID>/`.
7. Ghi nhận intent marker bền vững: Tạo file `.credential.pending` (chứa `opID\n`) và `fsync` thư mục `/mosquitto/auth`.
8. Thực hiện **Atomic Rename**: `os.Rename(candidate, "/mosquitto/auth/passwd")`.
9. Gọi `fsync` trên thư mục `/mosquitto/auth` để bảo đảm thay đổi cấu trúc thư mục đã ghi xuống đĩa.
10. Gửi yêu cầu reload tới sidecar và thực hiện probe kiểm chứng MQTT mới.
11. Khi probe thành công: Ghi nội dung mới vào `.op-<opID>/last-good`, `os.Rename` đè lên `passwd.last-good`, `fsync` thư mục auth.
12. Xóa marker `.credential.pending`, xóa thư mục staging `.op-<opID>`, và giải phóng file lock.

### 4.4 Cơ chế Phục hồi khi có sự cố (Crash Recovery & Rollback)

- **Lỗi trước bước Rename**: Xóa thư mục staging; file `passwd` chính thức hoàn toàn chưa bị tác động.
- **Lỗi sau bước Rename (Reload lỗi hoặc Probe kiểm chứng thất bại)**:
  - Runtime tự động kích hoạt luồng khôi phục có giới hạn ngân sách thời gian (`RecoveryTimeout`, mặc định 5s).
  - Khôi phục file gốc từ bản sao dự phòng: `rename(snapshot, passwd)` và `fsync` thư mục auth.
  - Gửi lại tín hiệu reload qua Unix socket.
  - Chạy `RecoveryProbe` để xác nhận broker đã khôi phục trạng thái credential trước đó.
- **Phân định rõ ranh giới giữa hai trạng thái sau sự cố**:
  1. **Trạng thái đã rollback có kiểm chứng (Verified Rollback)**:
     - Xảy ra khi quy trình khôi phục snapshot thành công và `RecoveryProbe` xác nhận broker đã quay về trạng thái credential hợp lệ trước đó.
     - Trong trạng thái này, file `passwd` và bộ nhớ broker đã được đưa về trạng thái nhất quán cũ.
  2. **Trạng thái bất định (Uncertain State) khi gặp `ErrRecoveryRequired`**:
     - Xảy ra khi bước rollback thất bại, `RecoveryProbe` thất bại, hoặc quá trình fsync/rename/promote `passwd.last-good` gặp lỗi sau khi file hoặc broker đã bị biến đổi.
     - Runtime **KHÔNG BẢO ĐẢM** được việc rollback đã hoàn tất: file `passwd` và trạng thái trong broker có thể không còn khớp với snapshot cũ, hoặc broker đã nạp credential mới nhưng `passwd.last-good` chưa được promote.
     - Hành vi an toàn của runtime:
       * Giữ nguyên thư mục staging chứa chứng cứ lỗi (`retain = true`).
       * Ghi marker `.credential.pending` với nội dung `recovery-required\n` và `fsync` thư mục.
       * Đánh dấu runtime bị nhiễm độc (`poisoned = true`), trả về lỗi kết hợp `errors.Join(cause, ErrRecoveryRequired)`.
       * Toàn bộ các mutation tiếp theo sẽ bị từ chối (fail-closed) cho đến khi người quản trị kiểm tra và xử lý thủ công.
     - **Ràng buộc đối với tầng gọi/CSDL**: Database **TUYỆT ĐỐI KHÔNG ĐƯỢC** tự ý coi đây là một thất bại thông thường và đánh dấu event là "failed" như thể rollback đã xong, mà bắt buộc phải giữ intent dở dang hoặc chuyển sang trạng thái cần khắc phục (`recovery_needed`) để tiến trình đối soát (reconciler) can thiệp có chủ đích.

---

## 5. Giao thức Reload qua Unix Domain Socket (`mosquitto-reloader`)

Sidecar `mosquitto-reloader` là một tiến trình Go độc lập, chạy cùng PID namespace với container Mosquitto và không kết nối mạng.

### 5.1 Đặc tả giao thức IPC

Giao thức giao tiếp giữa Backend và Reloader qua Unix Domain Socket `/mosquitto/control/reload.sock`:

```text
Backend (Client)                              Reloader (Server)
   │                                                 │
   ├── Kết nối DialContext("unix", reload.sock) ────►│
   │                                                 ├── Kiểm tra SO_PEERCRED (UID == 1883)
   ├── Gửi chuỗi: "reload\n" (7 bytes) ─────────────►│
   ├── Half-close: UnixConn.CloseWrite() ───────────►│
   │                                                 ├── Đọc đúng 7 bytes "reload\n" + EOF
   │                                                 ├── Kiểm tra Mosquitto PID 1
   │                                                 ├── Gửi SIGHUP qua pidfd_send_signal
   │◄── Phản hồi: "signalled\n" (10 bytes) ──────────┤
   ├── Nhận đủ chuỗi phản hồi + EOF                  │
   └── Đóng kết nối                                  └── Đóng kết nối
```

- Bất kỳ byte thừa nào sau chuỗi `reload\n`, hoặc client không thực hiện `CloseWrite`, hoặc timeout: Reloader hủy kết nối và không gửi tín hiệu.
- Reloader giới hạn số lượng kết nối đồng thời (`MQTT_RELOAD_MAX_CONNECTIONS`, mặc định 8) và timeout xử lý (`MQTT_RELOAD_TIMEOUT`, mặc định 2s).

### 5.2 Kiểm tra danh tính Mosquitto PID 1 và gửi tín hiệu qua `pidfd`

Reloader xác minh kỹ lưỡng tiến trình đích trước khi phát tín hiệu:

1. Đọc `/proc/1/comm`: Bắt buộc phải là `mosquitto\n`.
2. Đọc `/proc/1/exe`: Bắt buộc liên kết tượng trưng phải trỏ chính xác về `/usr/sbin/mosquitto`.
3. Đọc `/proc/1/status`: Kiểm tra toàn bộ 4 trường UID (`Real`, `Effective`, `Saved`, `FS`) đều bằng `1883`.
4. **Phát tín hiệu an toàn bằng Linux `pidfd`**:
   - Mở file descriptor đại diện cho tiến trình: `unix.PidfdOpen(1, 0)`.
   - Phát tín hiệu: `unix.PidfdSendSignal(fd, syscall.SIGHUP, nil, 0)`.
   - Cơ chế `pidfd` đảm bảo tín hiệu chỉ được gửi đến đúng thực thể tiến trình mong muốn, loại bỏ hoàn toàn rủi ro PID recycling (tiến trình Mosquitto chết và PID 1 được tái cấp cho một tiến trình khác).

### 5.3 Ý nghĩa thực sự của tín hiệu ACK

- Phản hồi `signalled\n` từ reloader **chỉ mang một ý nghĩa duy nhất**: Tín hiệu `SIGHUP` đã được nhân Linux chuyển giao thành công tới Mosquitto PID 1.
- Nó **hoàn toàn không chứng minh** Mosquitto đã đọc xong file `passwd` mới, cũng không chứng minh Mosquitto đã cập nhật bảng băm trong bộ nhớ.
- Do đó, backend **bắt buộc phải thực hiện fresh connection probe** để xác nhận nghiệp vụ.

---

## 6. Kiểm chứng kết nối MQTT TLS mới (Fresh Connection Probe)

Module `mqttcredential.Probe` sử dụng thư viện `paho.mqtt.golang` để trực tiếp kiểm chứng hiệu lực của thay đổi trên broker Mosquitto qua giao thức MQTTS (port 8883).

### 6.1 Đặc tính của Probe Client

- Kết nối tới `ssl://mosquitto:8883`, xác thực server certificate bằng trust bundle CA (`/mosquitto/config/certs/ca.crt`), yêu cầu TLS 1.2+.
- Mỗi lần probe sinh một Client ID ngẫu nhiên không trùng lặp: `cred-<16 bytes hex ngẫu nhiên>`.
- Cấu hình: `CleanSession = true`, `AutoReconnect = false`, `ConnectRetry = false`.
- Quản lý timeout chặt chẽ: Sử dụng `tls.Dialer.DialContext(ctx)` lồng trong `context.AfterFunc` để đóng kết nối TCP ngay lập tức nếu hết deadline, loại bỏ nguy cơ rò rỉ goroutine chạy nền.

### 6.2 Phân biệt hai loại Probe

1. **Positive Probe (`Login`)**:
   - Sử dụng sau khi thêm mới hoặc xoay vòng mật khẩu Gateway (Upsert/Rotate).
   - Yêu cầu broker trả về `CONNACK` thành công (Return Code 0).
   - Nếu nhận bất kỳ mã lỗi nào hoặc lỗi mạng/timeout: Coi là probe thất bại (`ErrVerificationFailed`).

2. **Negative Probe (`Rejected`)**:
   - Sử dụng sau khi thu hồi mật khẩu Gateway (Revoke) trong các kịch bản kiểm thử có mật khẩu cũ đã biết.
   - Yêu cầu broker phải trả về mã từ chối xác thực rõ ràng:
     * `packets.ErrRefusedBadUsernameOrPassword` (Mã phản hồi MQTT 4).
     * Hoặc `packets.ErrRefusedNotAuthorised` (Mã phản hồi MQTT 5).
   - **Bản chất xác thực**: Negative probe chỉ hợp lệ khi probe bằng **mật khẩu cũ đã biết** của Gateway. Việc thử một mật khẩu ngẫu nhiên không chứng minh được tài khoản đã bị thu hồi vì mật khẩu ngẫu nhiên luôn bị từ chối dù credential cũ còn hiệu lực hay không.
   - **Nguyên tắc cốt tử**: Lỗi mạng (timeout, connection reset, TLS handshake fail, DNS/routing error) **tuyệt đối không được coi là xác thực bị từ chối**. Nếu gặp lỗi mạng trong negative probe, hệ thống coi là probe thất bại (`ErrVerificationFailed`) để kích hoạt rollback, ngăn ngừa việc broker chưa kịp revoke nhưng hệ thống ngỡ là đã revoke do rớt mạng.

---

## 7. Hợp đồng phiên MQTT (MQTT Session Contract)

> **Cảnh báo Kế thừa & Phân biệt Semantics (Superseded Contract Notice):**
> Phần 7 này chỉ mô tả đặc tính kỹ thuật của Mosquitto v2.0.18 khi reload file tĩnh bằng tín hiệu `SIGHUP` trong Task 2.5 legacy. Trong kiến trúc Dynamic Security (DynSec) được lựa chọn tại **Task 2.6** và **Task 2.7**, semantics phiên và thu hồi **hoàn toàn khác biệt**:
> - **Xoay vòng (Rotate):** Diễn ra trong cửa sổ bảo trì toàn cục (**global maintenance**), Ingress Gate chủ động đóng lại (`CloseDrain`), broker được cold restart và verify fresh TLS connection trước khi mở lại cổng. Phiên cũ của mọi Gateway có thể bị ngắt (kể cả Gateway khác); Gateway khác phải reconnect bằng credential không đổi. Hệ thống **không hứa hẹn zero-outage**.
> - **Thu hồi (Revoke):** Native DynSec disable ngắt session mục tiêu; lifecycle kiểm chứng RAM/snapshot và fresh rejection trước khi báo thành công. Đây không phải cam kết thời gian ngắt bằng 0 hoặc bảo đảm cho mọi topology chưa nghiệm thu. Không dùng semantics "chỉ ảnh hưởng kết nối mới" của thời kỳ SIGHUP để nghiệm thu Task 2.6.
> Chi tiết xem tại [docs/backend/stage-2-task-2.6-mqtt-credentials.md](stage-2-task-2.6-mqtt-credentials.md).

### 7.1 Hành vi của Mosquitto khi Reload bằng SIGHUP (Legacy Static-File)

Khi nhận tín hiệu `SIGHUP`, Mosquitto đọc lại file mật khẩu và áp dụng cấu hình bảo mật mặc định (`mosquitto_security_apply_default()`). Trong mã nguồn `v2.0.18`, Mosquitto duyệt qua danh sách client đang kết nối và kiểm tra lại thông tin xác thực đối với username/password. Các client có thông tin không còn hợp lệ trong file mới có thể bị broker đóng kết nối ngay lập tức.

### 7.2 Cam kết lịch sử của hệ thống File Tĩnh (Legacy Contract)

1. **Cam kết kết nối mới**: Sau khi thao tác rotate/revoke trên file tĩnh được probe xác nhận thành công, credential cũ **chắc chắn không thể tạo kết nối MQTT mới**.
2. **Không cam kết trạng thái phiên cũ trong cơ chế SIGHUP**:
   - Phiên kết nối đang tồn tại có thể bị broker ngắt kết nối trên phiên bản Mosquitto hiện tại.
   - Hệ thống file tĩnh **không cam kết giữ phiên cũ** tiếp tục hoạt động.
   - Hệ thống file tĩnh **cũng không cam kết ngắt ngay lập tức mọi phiên đang mở** trong mọi tình huống (ví dụ: trường hợp client đang dở dang gói tin QoS hoặc phiên bản broker thay đổi).
3. **Quy tắc kiểm thử**: Không viết test hay thiết kế API phụ thuộc vào việc "phiên cũ bắt buộc phải sống" hay "phiên cũ bắt buộc phải bị ngắt ngay lập tức" đối với runtime file tĩnh SIGHUP.

---

## 8. Handoff sang Task 2.6 và Liên kết Kế thừa DynSec

Task 2.5 đã hoàn thành toàn bộ nền tảng runtime file tĩnh phục vụ baseline kiểm thử hồi quy. Tuy nhiên, để đáp ứng yêu cầu quản trị credential động an toàn và không downtime cho toàn bộ hệ thống, **Task 2.6 đã chuyển sang sử dụng Mosquitto Dynamic Security (DynSec)** thay cho cơ chế chỉnh sửa file tĩnh SIGHUP.

### 8.1 Phân định Thẩm quyền Nghiệp vụ và Bí mật Broker

Cần phân định rạch ròi giữa Thẩm quyền Nghiệp vụ (PostgreSQL) và Bí mật Xác thực (Broker Store):

1. **PostgreSQL là nguồn thẩm quyền nghiệp vụ duy nhất (Sole Business Authority):**
   - Các bảng `gateway_mqtt_credentials`, `gateway_mqtt_credential_events`, `mqtt_credential_maintenance`, `mqtt_credential_recovery` lưu trữ trạng thái nghiệp vụ, phiên bản (`credential_version`), thế hệ (`generation`), mã thao tác (`operation_id`), actor kiểm toán (`issued_by`), và các mốc thời gian UTC.
   - **PostgreSQL TUYỆT ĐỐI KHÔNG LƯU MẬT KHẨU PLAINTEXT VÀ CŨNG TUYỆT ĐỐI KHÔNG LƯU MQTT PASSWORD HASH**.
   - Bất kỳ nhận định nào cho rằng "CSDL lưu chuỗi hash an toàn của MQTT password" đều là sai lệch so với kiến trúc dự án.
2. **Broker Authentication Store lưu trữ bí mật Native:**
   - Chuỗi băm mật khẩu native (`sha512-pbkdf2` format `$7$...`) là bí mật nội bộ do Mosquitto quản lý, chỉ tồn tại trong `/mosquitto/auth/passwd` (đối với legacy static runtime) hoặc `dynamic-security.json` (đối với DynSec runtime).
   - Tuyệt đối **không có cơ chế import hash trong suốt (transparent hash import)** từ broker vào PostgreSQL hay ngược lại.

**Quy trình Điều phối Hai pha (Two-Phase Coordination) & Phục hồi:**
- **Pha 1 (PostgreSQL Intent)**: Ghi nhận trạng thái dự định thay đổi credential vào bảng `gateway_mqtt_credentials` và `gateway_mqtt_credential_events`.
- **Pha 2 (Runtime Execution)**:
  * Trong Task 2.5 legacy: Gọi `mqttcredential.Runtime` cập nhật file `passwd`, gửi SIGHUP và probe broker.
  * Trong Task 2.6 DynSec: Gọi DynSec adapter gửi mutation payload qua kênh quản trị `$CONTROL/dynamic-security/v1`, đối chiếu snapshot và probe qua listener nội bộ.
- **Pha 3 (PostgreSQL Finalize)**: Cập nhật trạng thái thành công trong PostgreSQL có kiểm soát fence/epoch.
- **Xử lý lỗi và phân định trạng thái phục hồi (`ErrRecoveryRequired` / `recovery_needed`)**:
  * Khi mutation runtime thất bại sau khi broker đã nhận thay đổi hoặc trạng thái bất định, CSDL **TUYỆT ĐỐI KHÔNG ĐƯỢC** tự ý đánh dấu event là "failed" như thể rollback đã hoàn tất. CSDL bắt buộc phải duy trì intent dở dang hoặc chuyển sang trạng thái `recovery_needed` để tiến trình đối soát (reconciler) can thiệp có chủ đích.

### 8.2 Thách thức kiểm chứng Revoke và Giải pháp DynSec tại Task 2.6

- Trong mô hình file tĩnh Task 2.5, Backend không lưu mật khẩu cũ nên không thể thực hiện negative probe (gửi mật khẩu cũ xem broker có từ chối không) khi quản trị viên gọi API Revoke.
- **Nguyên tắc kỹ thuật**: Thử một mật khẩu ngẫu nhiên và nhận phản hồi từ chối **hoàn toàn KHÔNG chứng minh** được tài khoản đã bị thu hồi (gây ra hiện tượng kết luận thành công giả — false-green).
- **Giải pháp DynSec được chọn tại Task 2.6**:
  * Task 2.6 giải quyết triệt để vấn đề này bằng cách gửi lệnh vô hiệu hóa trực tiếp tới DynSec plugin (`disabled=true`), vô hiệu hóa client trong RAM broker ngay lập tức, ngắt kết nối hiện hữu và đọc lại snapshot `dynamic-security.json`.
  * Startup Reconciler của Task 2.6 đối soát danh sách `revoked` từ PostgreSQL và ép trạng thái `disabled=true` trên broker trước khi Ingress Gate mở (`OPEN`).
  * Chi tiết thiết kế và bằng chứng nghiệm thu xem tại [docs/backend/stage-2-task-2.6-mqtt-credentials.md](stage-2-task-2.6-mqtt-credentials.md).

---

## 9. Quy trình vận hành & Kế hoạch Chuyển đổi khỏi Tracked Prototype File

### 9.1 Hiện trạng

File mẫu `config/mosquitto/passwd` hiện vẫn được track trong Git để phục vụ các bài kiểm tra prototype cũ. Trong `docker-compose.yml`, Mosquitto đã được cấu hình chuyển sang đọc từ named volume:

```yaml
volumes:
  - mosquitto_auth:/mosquitto/auth:ro
```

Và `mosquitto.conf` đã trỏ đường dẫn:
```text
password_file /mosquitto/auth/passwd
```

**Lưu ý quan trọng về dữ liệu khởi tạo:** Named volume `mosquitto_auth` khi được container `mosquitto-auth-init` khởi tạo lần đầu chỉ tự động seed duy nhất một tài khoản hệ thống nội bộ là `backend_service`. Initializer **hoàn toàn không tự động import** các tài khoản Gateway từ file prototype tracked trong git (`config/mosquitto/passwd`).

### 9.2 Runbook chuyển đổi lịch sử (Historical Migration Runbook)

Việc chuyển dịch từ file prototype sang store runtime trên môi trường thực tế/production bắt buộc phải tuân thủ quy trình phê duyệt vận hành chặt chẽ theo 5 bước:

1. **Bước 1 — Inventory và backup an toàn**:
   - Rà soát danh sách toàn bộ các Gateway và tài khoản hiện hữu cần duy trì kết nối.
   - Sao lưu dự phòng an toàn dữ liệu, chứng chỉ TLS và các file cấu hình hiện có vào vùng lưu trữ bảo mật (phân quyền `0600`).
   - Tuyệt đối không xuất, in hay ghi các chuỗi băm (hashes) hoặc mật khẩu ra `stdout`, console log, hay hệ thống giám sát tập trung.

2. **Bước 2 — Duyệt maintenance window và chọn phương án migration**:
   - Quản trị viên phê duyệt khung thời gian bảo trì (maintenance window) phù hợp để tránh ảnh hưởng tới luồng thu thập dữ liệu cảm biến.
   - Chọn rõ ràng phương án chuyển đổi được phê duyệt:
     * **Phương án A (Chuyển tiếp credential hiện hữu)**: Thực hiện copy có kiểm soát các dòng credential Gateway hiện tại vào store mới, chạy kiểm tra tính tương thích cú pháp của parser, quyền hạn file (`0600`, UID `1883`), đồng thời bảo đảm tài khoản `backend_service` sử dụng mật khẩu mạnh được sinh mới cho môi trường production.
     * **Phương án B (Provisioning lại có kiểm soát ở Task 2.6 DynSec)**: Khởi tạo volume sạch chỉ với `backend_service`, sau đó sử dụng Admin API của Task 2.6 để provision lại credential mới cho từng Gateway và nạp lại vào thiết bị vật lý qua USB offline.

3. **Bước 3 — Xác minh sau khi nạp store mới**:
   - Khởi động stack dịch vụ và kiểm tra logs khởi tạo của `mosquitto-auth-init` (đảm bảo thoát mã 0, quyền file/thư mục đạt chuẩn `0700`/`0600`).
   - Kiểm tra kết nối MQTT qua TLS (port 8883) của tài khoản `backend_service`.
   - Với các Gateway hiện hữu, kiểm tra xác thực bằng credential mới/chuyển tiếp và xác minh quy tắc phân quyền ACL `%u` đảm bảo topic isolation hoạt động chính xác.

4. **Bước 4 — Quy trình Rollback có kiểm soát**:
   - Nếu quá trình chuyển đổi gặp sự cố không thể khắc phục trong maintenance window, kích hoạt rollback về bản backup đã tạo ở Bước 1.
   - **Tuyệt đối KHÔNG tự động quay về** sử dụng file tracked prototype trong git chứa credential cũ hay các secret mặc định/công khai của prototype.

5. **Bước 5 — Ngừng tracking và xoá/rotate secret prototype**:
   - Sau khi môi trường thực tế đã chạy ổn định trên named volume mới và toàn bộ Gateway được xác minh thành công:
   - Việc ngừng tracking file prototype (`git rm --cached config/mosquitto/passwd` hoặc cập nhật `.gitignore`) và xoá/rotate các prototype credentials công khai phải được thực hiện thông qua một commit riêng biệt kèm biên bản phê duyệt vận hành rõ ràng, không gộp chung với các commit tính năng thông thường.

---

## 10. Bằng chứng kiểm chứng Local & CI (Historical Verification Evidence)

Toàn bộ các yêu cầu của Task 2.5 đã được kiểm chứng độc lập trên môi trường local với kết quả 100% PASS, và CI pipeline trên GitHub Actions đã xác nhận thành công tại mốc baseline lịch sử.

### 10.1 Tổng hợp các lệnh kiểm chứng

```bash
# 1. Chạy toàn bộ 37 Python contract & regression tests
python3 -m unittest discover -s scripts/tests -p 'test_stage2_*.py'

# 2. Chạy test harness tích hợp Mosquitto runtime thật
sh scripts/test-stage2-mosquitto-runtime.sh

# 3. Chạy Go unit tests với Race Detector cho toàn bộ repository
(cd src && go test -race -count=1 ./...)

# 4. Kiểm tra code coverage của các package mới thuộc Task 2.5
(cd src && go test -cover ./internal/mqttcredential ./internal/mosquittoreload ./cmd/mosquitto-auth-init ./cmd/mosquitto-reloader)

# 5. Kiểm tra định dạng và phân tích tĩnh Go
(cd src && go vet ./... && go build ./... && test -z "$(gofmt -l .)")

# 6. Kiểm tra git diff
git diff --check
```

### 10.2 Kết quả kiểm chứng chi tiết

| Hạng mục kiểm tra | Phạm vi & Phương pháp | Kết quả |
|---|---|---|
| **Python Contract Tests** | 37 tests (14 tests runtime trong `test_stage2_mosquitto_runtime.py` + 23 tests auth/provisioning) | **37/37 PASS** |
| **Go Unit & Race Tests** | `src/...` với `-race -count=1` trên Go 1.27.1 | **PASS** (không phát hiện race condition) |
| **Mosquitto Integration** | Docker compose isolated với Mosquitto 2.0.18 thật, TLS 1.2, CA trust bundle, ACL pattern `%u` | **PASS** (kiểm chứng thành công provision, rotate, revoke với mật khẩu cũ đã biết, reload và recovery) |
| **Code Coverage mới** | `src/internal/mqttcredential`<br>`src/internal/mosquittoreload`<br>`src/cmd/mosquitto-reloader` | **84.5%**<br>**90.1%**<br>**84.6%** (Đạt yêu cầu >80% cho feature mới) |
| **CLI Initializer Coverage** | `src/cmd/mosquitto-auth-init` | **~6.5% – 9.4%** instrumented unit coverage (Phần lớn logic cốt lõi về phân quyền root volume, drop UID 1883 và filesystem bootstrap được kiểm chứng toàn diện qua container integration harness) |
| **GitHub Actions CI** | Cả 8 jobs CI trên GitHub Actions đã **PASS** tại commit `674e8db` (bao gồm job `mosquitto-runtime-integration`) | **8/8 JOBS PASS** |

### 10.3 Lưu ý ranh giới thực tế và bảo mật

- **Baseline CI `674e8db`**: Mốc CI PASS 8/8 tại commit `674e8db` là bằng chứng lịch sử trước khi áp dụng các commit sửa lỗi theo review (chặn password placeholder/whitespace tại initializer `fafc565` và thu hẹp mount chứng chỉ backend chỉ đọc CA công khai `d5375ce`). Các commit này đã được kiểm chứng local đầy đủ.
- **Ranh giới Coverage**: Phân định rõ giữa Unit coverage và Feature union coverage: các package nghiệp vụ cốt lõi đều đạt >80%, còn CLI entrypoint initializer có instrumented coverage thấp do phụ thuộc vào đặc quyền Linux container được kiểm chứng bằng integration harness.
- **Không tự phong bảo mật (No Blanket Security Claims)**: Tuyệt đối **không tuyên bố hệ thống production đã hoàn toàn secure** hay **đã hoàn tất rotate secret production** khi chưa có thao tác vận hành thực tế theo runbook chuyển đổi trên môi trường triển khai thật.
- **Ranh giới Nghiệm thu Task 2.7 (Pending E2E Evidence)**:
  * Toàn bộ luồng E2E phối hợp giữa GoTrue Auth, Envoy Gateway, Go Backend, PostgreSQL, DynSec và gated MQTT TLS đang được kiểm chứng trong Task 2.7 với 27 kịch bản (E01–E27) theo [Kế hoạch Chi tiết Task 2.7](/media/trung/SSD2-Data/Project1_ET3290/docs/backend_plan/task_2.7_detail_plan.md).
  * Nghiệm thu Task 2.7 yêu cầu job CI thứ 10 (`stage2-e2e`) xanh trên commit SHA cuối cùng trước khi đóng toàn bộ Milestone Backend Stage 2. Không được ngụy tạo kết quả E2E PASS hoặc SHA verification trước khi job này chạy thành công.
- **Kỷ luật TLS & Bí mật**: Tuyệt đối không hạ chuẩn TLS (bắt buộc TLS 1.2+ với CA trust bundle), không sử dụng mật khẩu fixture làm ví dụ sản xuất, không ghi log plaintext secrets hay bearer tokens.

---

Tài liệu này xác nhận hạ tầng runtime credential file tĩnh của Task 2.5 đã hoàn thành đầy đủ vai trò nền tảng kiểm thử hồi quy (regression baseline) và phục vụ đối chiếu kiến trúc trong quá trình phát triển hệ thống.
