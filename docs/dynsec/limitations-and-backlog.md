# Giới Hạn Kỹ Thuật, Rủi Ro Vận Hành Và Danh Mục Backlog

Tài liệu này phân tích chi tiết các giới hạn kỹ thuật (Technical Limitations), đánh giá rủi ro an toàn và thiết lập danh mục công việc tồn đọng (Prioritized Backlog) cho hệ thống Dynamic Security (DynSec) dựa trên hiện trạng mã nguồn và các bằng chứng kiểm thử thực tế.

---

## 1. Phân Tích Chuyên Sâu Các Giới Hạn Kỹ Thuật (Detailed Technical Limitations)

### 1.1 Rủi ro lưu trữ mật khẩu sau Rotate (ROT1 vs ROT2 & Restart-Old-Password Risk) — ƯU TIÊN 1
- **Hiện trạng hiện thực:**
  Mốc Task 2.8a tối giản đã hoàn thiện và kiểm chứng trình tự xoay vòng **ROT1** (`DisableClient -> Disabled Readback -> Exact Role Readback -> SetPassword -> EnableClient -> Fresh TLS Login`). Mật khẩu mới được chứng minh hoạt động thành công trong bộ nhớ RAM của Mosquitto broker đang chạy thông qua kết nối `fresh TLS login` (`FreshPositiveVerified = true`).
- **Giới hạn kỹ thuật:**
  Cơ chế kiểm tra việc ghi chính xác mật khẩu mới xuống đĩa cứng (**ROT2** - `exact persisted-new-password verifier`) đã được hoãn lại. Trình kiểm tra `dynsec_snapshot.go` chỉ kiểm tra việc vô hiệu hóa cho luồng Revoke (`VerifyRevoked`), không giải mã hoặc kiểm tra hash mật khẩu mới trên tệp `dynamic-security.json` sau khi Rotate.
- **Rủi ro vận hành (Restart-Old-Password Risk):**
  Mosquitto DynSec plugin ghi bộ nhớ đệm ra tệp `dynamic-security.json` theo chu kỳ hoặc khi có sự kiện. Nếu tiến trình Mosquitto gặp sự cố đĩa cứng, crash đột ngột, hoặc bị dừng cưỡng bức trước khi kịp flush dữ liệu ra đĩa, khi khởi động lại, Mosquitto có thể nạp lại phiên bản tệp cũ chứa **mật khẩu cũ**.
- **Biện pháp giảm thiểu:**
  Hệ thống nghiêm cấm việc tự động khởi động lại broker sau mỗi lần rotate để che giấu lỗi. Operator phải giám sát log I/O của Mosquitto và kiểm tra sự kiện ghi tệp.

---

### 1.2 Không đảm bảo an toàn mất điện đột ngột (No fsync / Power-Loss Guarantee) — ƯU TIÊN 2
- **Hiện trạng hiện thực:**
  Bộ đọc snapshot `DynSecSnapshotReader` sử dụng syscalls `openat/O_NOFOLLOW`, `fstat` và đọc dữ liệu tệp `dynamic-security.json` ở tầng hệ điều hành.
- **Giới hạn kỹ thuật:**
  Việc đọc snapshot thành công chỉ chứng minh tệp đã được ghi nhận trong filesystem cache của Linux kernel. Cơ chế này **không kiểm tra hoặc ép buộc gọi lệnh `fsync`** xuống bề mặt đĩa vật lý của phần cứng lưu trữ.
- **Rủi ro vận hành:**
  Trong trường hợp mất nguồn điện đột ngột (hard power-loss) trên toàn bộ máy chủ vật lý, dữ liệu trên đĩa có thể bị quay ngược trạng thái (rollback) dù trước đó kiểm tra snapshot đã báo thành công.

---

### 1.3 Cơ chế Reset và Phục hồi còn ở trạng thái chỉ từ chối (Deny-Only Reset) — ƯU TIÊN 3
- **Hiện trạng hiện thực:**
  Endpoint `POST /v1/admin/gateways/{gateway_id}/mqtt-credential/reset` đã được định tuyến tại `src/internal/httpserver/admin_mqtt_credential_handlers.go`, nhưng tại tầng composition `src/cmd/server/credential.go`, thành phần phụ thuộc được gán kiểu `denyOnlyResetPrerequisite{}`.
- **Giới hạn kỹ thuật:**
  Đối với một yêu cầu reset hợp lệ, có đầy đủ thẩm quyền admin, đúng header và đúng payload admissible (`recovery_after_unknown` hoặc `lost_secret_recovery`), hàm xử lý prerequisite luôn fail-closed và trả về mã lỗi HTTP `503 Service Unavailable` (`CodeServiceUnavailable`). Các yêu cầu thiếu token hợp lệ (401), phi admin (403), sai định dạng media type (415), hoặc sai payload/query/header (400) sẽ bị từ chối với mã lỗi tương ứng trước khi tới runtime prerequisite.
- **Hệ quả vận hành:**
  Hệ thống hiện tại chưa cung cấp công cụ tự động phục hồi cho các bản ghi bị rơi vào trạng thái `recovery_needed` hoặc các trường hợp mất mật khẩu một lần (`lost secret`). Mọi trường hợp này đều đòi hỏi Platform Operator phải thực hiện chẩn đoán có giám sát, không có lối tắt khôi phục tự động. Tuyệt đối không mô tả endpoint reset như một công cụ khả dụng.

---

### 1.4 Thiếu hàng rào phân tán và bảo đảm tĩnh lặng Broker (No Distributed Fencing & Broker Quiescence) — ƯU TIÊN 4
- **Hiện trạng hiện thực:**
  Thành phần `credentialOwner` (`src/cmd/server/credential_owner.go`) sử dụng PostgreSQL session advisory lock (`pg_try_advisory_lock` với khóa `0x494f545f43524544`) trên một kết nối CSDL chuyên biệt (hijacked connection).
- **Giới hạn kỹ thuật:**
  Khóa này chỉ có tác dụng loại trừ cạnh tranh giữa các tiến trình Go backend cùng kết nối tới chung một cơ sở dữ liệu PostgreSQL. Nó **không cung cấp cơ chế phân tán hàng rào (no distributed fencing)**:
  1. Không ngăn chặn được các tác nhân bên ngoài tự ý gửi lệnh vào topic quản trị `$CONTROL/dynamic-security/v1` của Mosquitto nếu có được tài khoản Manager.
  2. Không đảm bảo trạng thái tĩnh lặng của broker (no broker quiescence): Các client Gateway vẫn có thể gửi tin nhắn telemetry lên broker trong khi backend đang thực hiện xoay vòng tài khoản.
  3. Nếu kết nối CSDL của backend bị đứt, PostgreSQL tự động giải phóng lock; backend phát hiện và chuyển sang trạng thái dừng xử lý (`Ready() == false`), nhưng không thể hủy các lệnh đang xử lý dở dang trên broker.

---

### 1.5 Không có cơ chế tự động tắt Broker khi Backend hoặc CSDL gặp sự cố — ƯU TIÊN 5
- **Hiện trạng hiện thực:**
  Mosquitto và Go backend là hai tiến trình độc lập trong Docker Compose.
- **Giới hạn kỹ thuật:**
  Hệ thống không cài đặt cơ chế watchdog hay health-lease liên tục để tự động tắt broker Mosquitto khi Go backend crash hoặc mất kết nối tới PostgreSQL.
- **Hệ quả vận hành:**
  Nếu backend dừng hoạt động, broker Mosquitto vẫn tiếp tục nhận kết nối từ Gateway và nhận tin nhắn MQTT trong hàng đợi (nhưng không có backend để lưu vào PostgreSQL TimescaleDB).

---

### 1.6 Ranh giới tương đương bản build Dockerfile gốc (Root Build Parity Caveat) — ƯU TIÊN 6
- **Hiện trạng kiểm thử (Rehearsal Evidence):**
  Các cuộc diễn tập container thực tế (Part 1 Nginx rehearsal và Part 2 Supabase Auth rehearsal) đã kiểm chứng sự phối hợp giữa Nginx container, Supabase Envoy container, GoTrue container, Mosquitto container và backend container.
  Tuy nhiên, trong các bài diễn tập này, dịch vụ Go backend được chạy bằng cách **biên dịch tĩnh nhị phân offline trên host** (`offline host static binary`) rồi mount vào container (`build_scope: offline host static binary mounted read-only`), chưa phải là bản build đa tầng từ `src/Dockerfile` (`target: backend`) trong `docker-compose.yml` gốc.
- **Khoảng trống:** Cần hoàn thành kiểm định quy trình build Docker Compose gốc trước khi triển khai chính thức.

---

### 1.7 Cổng duyệt độc lập và độ bao phủ toàn diện Giai đoạn 2 (Review Gate & Full Stage 2 Gaps) — ƯU TIÊN 7 & 8
- **Cổng duyệt độc lập (Independent Reviewer Gate):** Quy trình nghiệm thu độc lập theo kế hoạch `AGENTS.md` vẫn đang ở trạng thái chờ duyệt.
- **Độ bao phủ kiểm thử tích hợp toàn diện:**
  * Bằng chứng M04 (Cô lập observer) mới chứng minh phiên observer duy trì kết nối và nhận PUBACK trên broker; chưa chứng minh đầu nhận (receiver delivery) correlated hay ghi nhận telemetry vào TimescaleDB.
  * Chưa kiểm chứng toàn bộ ma trận ACL cho toàn bộ các topic nghiệp vụ nâng cao (status, responses, commands).
  * Chưa kiểm chứng quy trình sao lưu và phục hồi dữ liệu từ bản backup thực tế trên môi trường sản xuất.

---

## 2. Các Đính Chính Thực Tế Quan Trọng (Key Factual Corrections)

Đối chiếu với các tài liệu và kế hoạch trước đây, các điểm sau đây cần được hiệu chỉnh chính xác theo hiện trạng mã nguồn:

1. **Xác thực Supabase Auth Thực Tế ĐÃ ĐƯỢC KIỂM CHỨNG (Không còn là Pending):**
   Báo cáo `docs/task28a-supervised-credentials.md` (mục 6.3) trước đây ghi nhận *"Chưa nghiệm thu tầng HTTP và Supabase Auth"*. Nhận định này hiện đã được cập nhật: Bài kiểm thử diễn tập Part 2 (`auth-rehearsal-20261012-63a6a259b0bf.json`) đã khởi chạy container GoTrue Auth v2.196.0 và Envoy v1.39.1 thực tế trong môi trường diễn tập tạm thời (disposable rehearsal fixture), thực hiện đăng nhập bằng mật khẩu thực, nhận JWT thật mang claim `role: "authenticated"` và được Nginx chuyển tiếp tới Go backend thành công. Tuy nhiên, vẫn cần bảo toàn ranh giới cảnh báo: bài diễn tập sử dụng nhị phân tĩnh biên dịch trên host được mount vào container (`offline host static binary mounted read-only`), chưa tương đương với bản build đa tầng từ `src/Dockerfile` (`target: backend`) trong `docker-compose.yml` gốc (host-static vs Dockerfile/root Compose parity caveat).
2. **Nhật ký Nginx KHÔNG tự động che giấu Signed URLs:**
   Không được tuyên bố Nginx tự động che giấu signed URLs hoặc token trong access log. Mặc định Nginx ghi nhận biến `$request` (bao gồm URI và query string đầy đủ).
3. **Role `postgres` trong Cấu Hình Gốc:**
   Trong bài diễn tập Part 2, kịch bản kiểm thử tạo role `postgres NOLOGIN` như một bước chuẩn bị cho migration `20240612123726` của GoTrue. Trong cấu hình gốc `docker-compose.yml`, biến `POSTGRES_USER` mặc định đã là `postgres`, do đó role này đã tồn tại sẵn trong môi trường chuẩn, không cần tạo thủ công trừ khi sử dụng superuser khác.
4. **Giới Hạn Của Kiểm Tra Tĩnh `git diff --check`:**
   Lệnh `git diff --check` chỉ kiểm tra lỗi định dạng khoảng trắng và xung đột merge (whitespace errors & merge conflict markers); tuyệt đối không tuyên bố lệnh này xác thực tính hợp lệ cú pháp ngữ nghĩa (semantic syntax) hay logic của mã nguồn và tài liệu.
5. **Hiện Trạng Triển Khai Quản Trị Danh Mục Và Khởi Tạo Cảm Biến:**
   Cả hai endpoint `GET /v1/admin/gateways` (quản trị danh mục Gateway toàn hệ thống cho Platform Admin) và `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` (khởi tạo cảm biến cho chủ sở hữu Gateway) đều **ĐÃ ĐƯỢC HIỆN THỰC (IMPLEMENTED)** trong mã nguồn; việc hoàn tất nghiệm thu tổng hợp (aggregate acceptance) cho toàn bộ gói tính năng đang trong giai đoạn chờ kết nối nghiệm thu cuối, không phải là tính năng còn thiếu trong mã nguồn.

---

## 3. Danh Mục Tồn Đọng Theo Thứ Tự Ưu Tiên (Prioritized Backlog)

| Thứ Tự | Hạng Mục Công Việc | Mức Độ Rủi Ro / Ảnh Hưởng | Hướng Hiện Thực Đề Xuất |
|:---:|---|---|---|
| **P1** | **Xác thực lưu trữ mật khẩu sau Rotate (ROT2 Verifier)** | Rủi ro nạp lại mật khẩu cũ nếu broker crash sau khi rotate. | Hiện thực bộ đọc phân tích hash mật khẩu mới trên snapshot `dynamic-security.json` trước khi finalize DB cho luồng Rotate. |
| **P2** | **Nghiên cứu cơ chế đảm bảo độ bền phần cứng (`fsync`)** | Mất điện đột ngột có thể làm mất dữ liệu snapshot trên đĩa. | Nghiên cứu cấu hình filesystem/mount flags đồng bộ (sync mount) hoặc cơ chế kích hoạt sync từ host. |
| **P3** | **Hiện thực luồng Supervised Reset khả dụng** | Hiện tại reset hợp lệ bị từ chối fail-closed tại prerequisite 503 (deny-only); thiếu quy trình tĩnh lặng để phục hồi khi gặp sự cố bất định hoặc mất secret. | Xây dựng quy trình `ResetPrerequisite` có giám sát của operator với kiểm tra tĩnh lặng (quiescence) của tiến trình backend quản trị cũ / các đột biến MQTT in-flight và tĩnh lặng luồng quản trị (management path quiescence), thay vì chỉ thuần túy trông đợi Gateway im lặng; không tự ý tạo lệnh recovery mới ngoài quy định. |
| **P4** | **Kiểm định Build Parity trên Dockerfile gốc** | Diễn tập sử dụng nhị phân tĩnh mount từ host, chưa chạy build `src/Dockerfile`. | Chạy kiểm thử tự động với quy trình `docker compose build backend` chính thức của root Compose. |
| **P5** | **Bổ sung kiểm thử End-to-End M04 (Receiver Telemetry Pipeline)** | Observer session chỉ mới chứng minh không bị ngắt; chưa chứng minh nhận tin và lưu vào TimescaleDB. | Viết test case observer subscribe topic dữ liệu và xác nhận dữ liệu được lưu vào bảng `telemetry` trong khi target bị rotate. |
| **P6** | **Hoàn tất rà soát độc lập (Independent Reviewer Gate)** | Điều kiện tiên quyết trước khi nghiệm thu toàn bộ Task 2.8. | Chuyển giao mã nguồn và tài liệu cho reviewer độc lập thẩm định. |
