# Kiến trúc & Phạm vi Gate G1: Quản lý Xác thực và Phân quyền Mosquitto (DynSec)

Tài liệu này tổng hợp toàn diện kết quả kỹ thuật từ 6 báo cáo spike thực nghiệm, thiết lập kiến trúc đã thống nhất và xác định phạm vi mới cho Gate G1 (Task 2.6) trong khuôn khổ Đồ án 1 (`Project1_ET3290`).

Kiến trúc thay thế phương án quản lý file tĩnh ban đầu (vốn gặp bế tắc logic kiểm chứng) bằng giải pháp **Mosquitto Dynamic Security (DynSec)** kết hợp **Cổng kiểm soát vòng đời (Lifecycle Ingress Gate)** và nguyên tắc **Fail-Closed**.

---

## 1. Kiến trúc cốt lõi đã phê duyệt (Approved Architecture)

### 1.1. Phân định lưu trữ & Trách nhiệm dữ liệu
- **PostgreSQL:** Là nguồn thẩm quyền lưu trữ bền vững (authoritative business store) cho các quyết định nghiệp vụ (intent), trạng thái công việc (`jobs`), phiên bản (`epoch`) và nhật ký kiểm toán (`audit log`). Tuyệt đối **KHÔNG lưu plaintext password** và **KHÔNG lưu password hash** của Gateway trong cơ sở dữ liệu.
- **Mosquitto Broker RAM:** Là bảng xác thực và phân quyền hoạt động thực tế (actual runtime auth table) để quyết định cho phép hay từ chối kết nối MQTT.
- **DynSec File Snapshot (`dynamic-security.json`):** Là bản lưu snapshot trên đĩa do Mosquitto plugin ghi ra. File này có thể trễ hoặc lệch pha so với RAM nếu thao tác ghi đĩa gặp lỗi (save failure), do đó không được coi là nguồn dữ liệu nghiệp vụ độc lập.

### 1.2. Kênh quản trị & Điều khiển nội bộ
- **Go API (Platform Admin):** Đóng vai trò giao tiếp quản trị nghiệp vụ với PostgreSQL; chỉ quản trị viên nền tảng được ủy quyền mới có quyền tạo tác vụ cấp phát hoặc xoay vòng.
- **Internal MQTT Management Channel:** Tiến trình điều khiển nội bộ gửi lệnh cấu hình DynSec qua topic chuyên dụng `$CONTROL/dynamic-security/v1`. Toàn bộ Gateway và client bên ngoài bị cấm triệt để quyền gửi (publish) hoặc nhận (subscribe) trên namespace này thông qua quy tắc ACL mặc định từ chối (`deny`).

### 1.3. Mô hình kết nối mạng & TLS
- Toàn bộ kết nối MQTT qua mạng ngoài sử dụng MQTT over TLS (cổng tiêu chuẩn 8883 hoặc qua tunnel).
- Xác thực một chiều bằng chứng chỉ server (TLS CA-only): Gateway lưu trữ public CA certificate/trust bundle để xác thực Mosquitto server. Không yêu cầu xác thực hai chiều (mTLS) cho client nhằm giữ độ phức tạp phù hợp với vi điều khiển.

### 1.4. Cổng kiểm soát khởi động an toàn (Startup Ingress Gate)
- **Mặc định ĐÓNG (CLOSED):** Ở mỗi chu kỳ sống mới (startup hoặc container restart), cơ chế kiểm soát đề xuất (đã thử nghiệm qua nonce định danh chu kỳ trong fixture) giữ toàn bộ cổng kết nối Gateway ở trạng thái đóng.
- **Quy trình hòa giải (Replay & Verification):** Tiến trình quản lý đọc danh sách các quyết định thu hồi (`revoked`) bền vững từ PostgreSQL, gửi lệnh vô hiệu hóa (`disableClient`), xác nhận phản hồi `disabled=true` trong RAM qua `getClient`, và đối chiếu trực tiếp file snapshot native.
- **Điều kiện MỞ (OPEN):** Chỉ khi toàn bộ danh sách thu hồi được xác minh thành công và cập nhật trạng thái job (`snapshot_observed`) trên DB, Ingress Gate mới mở cổng cho Gateway kết nối.
- **Xử lý sự cố Fail-Closed khi khởi động:** Nếu DB mất kết nối, dữ liệu bất nhất, hoặc lưu file thất bại khi khởi động, Ingress Gate giữ trạng thái đóng cho đến khi người vận hành can thiệp phục hồi hoặc theo cơ chế thử lại có giới hạn (until operator recovery / bounded retry).
- **Phạm vi bảo vệ cổng:** Mọi tuyến kết nối (kể cả cổng host trực tiếp lẫn Rathole reverse tunnel) bắt buộc phải đi qua cổng này; không tồn tại đường bypass trực tiếp vào Mosquitto listener. Chính sách ngắt kết nối tự động khi mất DB sau khi đã MỞ (post-OPEN DB loss) hiện **chưa chốt (undecided)** và chưa được cài đặt tự động ngắt.
- *Lưu ý về vòng đời:* Cơ chế wrapper/proxy giám sát PID1 hiện là ứng viên kiểm chứng trong fixture (candidate), chưa phải giải pháp production hoàn chỉnh.

### 1.5. Vòng đời thu hồi thông tin xác thực (Revoke Lifecycle)
1. **Commit DB Intent:** Ghi nhận quyết định thu hồi vào PostgreSQL trước khi tác động đến broker.
2. **Thực thi qua DynSec:** Gửi lệnh `disableClient` có gắn mã tương quan (`correlationData`).
3. **Đối chiếu phản hồi:** Nhận phản hồi tương quan từ plugin DynSec (phản hồi logic ứng dụng, không dựa vào MQTT PUBACK).
4. **Kiểm tra đa tầng:** Xác nhận trạng thái RAM (`getClient.disabled=true`) và kiểm tra file snapshot trên đĩa.
5. **Xử lý lỗi bền vững:** Nếu ghi đĩa thất bại, đánh dấu job là `pending recovery`. Tuyệt đối **không thực hiện rollback trạng thái cũ** (tránh kích hoạt lại thông tin bị thu hồi); danh sách thu hồi trên DB duy trì bền vững để cưỡng chế ở lần khởi động kế tiếp.

### 1.6. Quy trình xoay vòng & Cấp phát bảo trì (Maintenance Provision & Rotate)
- **Cửa sổ bảo trì toàn cục (Maintenance Window):** Đóng toàn bộ các tuyến kết nối Gateway và làm sạch (drain) các phiên đang hoạt động.
- **Sinh khóa an toàn:** Mật khẩu mới được sinh bằng CSPRNG hoàn toàn trong bộ nhớ RAM, không ghi ra log hay lưu tạm trên đĩa.
- **Nạp & Kiểm chứng lạnh (Fresh Broker Load & Verify):** Cập nhật plugin DynSec qua lệnh nội bộ (lệnh `setClientPassword` nội bộ truyền mật khẩu mới qua MQTT over TLS đến broker control topic), tái khởi động tiến trình broker mới từ đĩa, kết nối kiểm tra đăng nhập bằng mật khẩu mới trên listener nội bộ trước khi xác nhận.
- **Hoàn tất DB & Xuất mật khẩu 1 lần:** Sau khi xác nhận broker hoạt động tốt với mật khẩu mới, commit trạng thái vào PostgreSQL rồi mới xuất mật khẩu một lần duy nhất (one-time secret) cho người quản trị.
- **Nạp thủ công qua USB:** Người quản trị nạp mật khẩu trực tiếp qua USB vào bộ nhớ mã hóa phần cứng của Gateway. **Tuyệt đối không chuyển giao mật khẩu cho Gateway qua MQTT (no secret delivery to Gateway through MQTT)** và không hỗ trợ hai mật khẩu song song.
- **Tách biệt trạng thái:** Phân định rạch ròi giữa `broker verified` (broker đã nạp thành công) và `device updated` (thiết bị Gateway đã nhận mật khẩu).

### 1.7. Xử lý mất phản hồi khi sự cố (Lost Response Handling)
- Nếu tiến trình hoặc hệ thống gặp sự cố sập (crash) trong lúc thực hiện, hệ thống lúc sập không thể phản hồi API ngay lập tức.
- Khi phục hồi và truy vấn/phát lại sau sự cố (query/replay after recovery), API chỉ trả thông tin metadata (`delivery=unknown`). Bắt buộc thực hiện thao tác xoay vòng mới (rerotate); tuyệt đối không đọc lại mật khẩu từ DB (do không lưu), không rollback về mật khẩu cũ.

### 1.8. Mô hình vận hành đơn giản (Single Broker / Single Controller)
- Kiến trúc phục vụ Đồ án 1 sử dụng một broker và một controller duy nhất do người vận hành quản lý. Chấp nhận gián đoạn kết nối toàn cục có chủ ý trong thời gian bảo trì (explicit global outage), phù hợp nguồn lực Đồ án 1, không cố gắng xây dựng cụm HA tự trị không người trực.

---

## 2. Bảng đối chiếu: Đã kiểm chứng (PASS) vs. Chưa thẩm định / Rào cản (BLOCKED)

| Hạng mục / Kịch bản | Fixture Spike | Đánh giá Thực tế / Rào cản Production |
|---|:---:|---|
| **Cổng khởi động (Restart Ingress Gate)** | **PASS** | Đạt trong fixture Go wrapper (PID1); kiểm chứng nguyên lý fail-closed khi khởi động. |
| **Thu hồi xác thực (Revoke Flow)** | **PASS** | Đạt yêu cầu đối chiếu lệnh tương quan và snapshot; lưu ý file snapshot native **không có bảo đảm fsync chống mất nguồn đột ngột**. |
| **Xoay vòng mật khẩu (Rotate Flow)** | **PASS** | Đạt chu trình drain, sinh CSPRNG trong RAM, cold load verify và nạp giả lập; **DB active không tương đương biên nhận thiết bị (handoff receipt)** sau khi sập. |
| **Quy trình tạo role/client (Provisioning Choreography)** | *Chưa thử nghiệm* | Chuỗi khởi tạo role và cấp quyền ban đầu (từ trạng thái rỗng) chưa được kiểm chứng đầy đủ trong fixture. |
| **Mất kết nối DB sau khi MỞ (DB loss post-OPEN)** | *Chưa đạt / Chưa chốt* | Chưa có cơ chế giám sát rớt kết nối DB (lease-loss fencing) để tự động ngắt kết nối nếu DB sập sau khi đã OPEN. |
| **Rathole thực tế & Tối thiểu đặc quyền** | *Giả lập* | Mới mô phỏng chuyển tiếp TCP qua cổng; chưa kiểm thử binary Rathole thật, chưa phân quyền triệt để UID tiến trình. |
| **Lưu trữ mã hóa trên USB & Phần cứng thật** | *Giả lập* | Mới kiểm chứng qua file local mode 0600; chưa kiểm thử nạp USB vật lý và mã hóa flash/NVS trên bo mạch thật. |
| **Chính sách khởi động không mở mù quáng** | **Scoped PASS** | Đạt trong phạm vi kịch bản thu hồi và xoay vòng; chưa chứng minh trường hợp mất toàn bộ active generation trên hệ thống hoàn chỉnh. |

---

## 3. Phân định phạm vi: Giới hạn đã biết vs. Rào cản Production

### 3.1. Giới hạn đã biết ngoài phạm vi G1 (Known limits out of G1 scope)
- Khả năng chống mất điện đột ngột (power-loss / fsync guarantee) của hệ thống tệp và phần cứng lưu trữ.
- Cơ chế cân bằng tải, phân tán cụm hoặc chuyển vùng tự động (Multi-broker HA / automated clustering).
- Cấp phát mật khẩu động qua môi trường mạng (hệ thống trung thành với mô hình nạp offline qua USB).

### 3.2. Rào cản tối thiểu chặn triển khai Production (Minimum Production Rollout Blockers)
- **Tích hợp quy trình cấp phát (Provisioning Choreography):** Cần hoàn thiện và kiểm chứng chuỗi lệnh tạo role/client hoàn chỉnh từ trạng thái ban đầu, kết hợp nguyên tắc đặc quyền tối thiểu (least privilege) cho các thông tin xác thực nội bộ.
- **Xác định chính sách mất DB sau khi MỞ (Post-OPEN Policy):** Cần định nghĩa rõ ràng phương án xử lý và quản lý tranh chấp đồng thời (concurrency policy) ở mức vừa đủ cho MVP khi mất kết nối DB sau khi đã OPEN (không bắt buộc giải pháp HA/fencing hoàn hảo cho G1).
- **Rủi ro bypass mạng:** Cần cấu hình hạ tầng mạng/Docker để đảm bảo mọi luồng từ Internet hoặc Rathole bắt buộc phải đi qua Ingress Gate trước khi tới Mosquitto.
- *Ghi chú kiến trúc:* Hệ thống không bắt buộc phải trang bị thêm một bộ giám sát ngoài chuyên biệt (external independent supervisor) làm hạ tầng thường trực; việc triển khai production có thể kế thừa mô hình wrapper tối giản đã chứng minh trong fixture.

### 3.3. Giới hạn nghiệm thu phía thiết bị (Device Rollout / Hardware Acceptance Limit)
- Thẩm định vật lý quy trình nạp USB an toàn và khả năng lưu trữ mã hóa phần cứng (flash/NVS) trên các bo mạch mục tiêu (ESP32, Luckfox Pico Plus, TI AM5728) thuộc phạm vi nghiệm thu thiết bị Gateway thực tế, tách biệt với việc triển khai backend G1.

---

## 4. Kết luận & Ranh giới áp dụng cho Đồ án 1

1. **Cho phép tiếp tục triển khai MVP có kiểm soát (Controlled MVP Implementation):**
   - Kết quả từ các spike thực nghiệm đủ độ tin cậy để làm cơ sở thiết kế backend Go và Mosquitto trong khuôn khổ đồ án sinh viên.
   - Luồng nghiệp vụ tuân thủ nghiêm ngặt: Single broker, fail-closed khi khởi động, DynSec quản lý xác thực trong RAM, PostgreSQL quản lý ý định bền vững, nạp mật khẩu thủ công qua USB.
2. **KHÔNG chấp thuận triển khai Production tự động (Not approved for unattended production deployment):**
   - Không được tự động đưa kiến trúc này vào môi trường production không giám sát hoặc tuyên bố "sẵn sàng production" khi chưa giải quyết các rào cản tối thiểu tại mục 3.2.
3. **Tham chiếu tài liệu lịch sử & Cơ sở thực nghiệm:**
   - Sáu narrative report lịch sử đã được thay bằng [hồ sơ harness/evidence hợp nhất](task26-harness-evidence.md): giữ source commit/line references, static witness và RAM-only save-failure phản chứng, fault matrix, commands hiện hành và giới hạn NOT RUN; không dùng commands static/filesystem đã xóa làm runbook.
   - Entrypoint duy nhất: `python3 scripts/tests/task26_credential_spike.py` (DynSec, revoke, startupgate, maintenancerotate). Helper build từ nguồn hiện tại trong fixture mới; bốn Compose topology giữ lại cho đối chứng và lifetime ownership, không phải bốn harness riêng.
   - Fixture PASS không nâng thành full/original G1 PASS: wire-loss/commit ambiguity thật, concurrent writers/post-OPEN fencing, least-privilege PG role, power loss, Rathole binary và USB/encrypted board vẫn chưa kiểm chứng. Không sửa Task 2.5 production runtime/harness.
