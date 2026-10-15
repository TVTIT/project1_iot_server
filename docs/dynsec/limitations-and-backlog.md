# Giới Hạn Kỹ Thuật, Rủi Ro Vận Hành Và Danh Mục Backlog

Tài liệu này phân tích chi tiết các giới hạn kỹ thuật (Technical Limitations), đánh giá rủi ro an toàn và thiết lập danh mục công việc tồn đọng (Prioritized Backlog) cho hệ thống Dynamic Security (DynSec) dựa trên hiện trạng mã nguồn và các bằng chứng kiểm thử thực tế.

---

## Cập nhật Task 3.0 — giới hạn chuyển đổi schema telemetry (15/10/2026)

Các kết quả và ưu tiên DynSec lịch sử bên dưới được giữ nguyên; policy hiện hành
theo `AGENTS.md` mục 0/16. Task 3.0 chỉ thử schema/driver, không đóng T01–T12.

| Giới hạn | Ảnh hưởng | Cách xử lý | Khi xem lại |
| --- | --- | --- | --- |
| Đổi telemetry PK/provenance khi columnstore bật không thực hiện trực tiếp với nonconstant default | Cần maintenance và dung lượng decompress; spike 3 rows không ước lượng được thời gian/disk trên dữ liệu thật | Candidate đã PASS decompress → tắt compression → DDL → bật/recompress; dừng writers, chờ compression job đang chạy kết thúc và preflight dung lượng trước migration | Task 3.2 khi viết migration chính thức; xem [báo cáo Task 3.0](../backend/stage-3-task-3.0-baseline-and-contract.md) |
| Provenance lịch sử không có boot/message/topic/range | Không thể suy đoán dedup/ACK cho legacy marker; CHECK provenance chưa thể VALIDATE toàn dữ liệu | Giữ NULL lịch sử, CHECK NOT VALID kiểm new writes, supervised disposition cho legacy marker; không bịa ID hoặc xóa ledger | Task 3.2 và khi có dữ liệu legacy cần replay |
| Compression cảnh báo segment/order cho các cột khóa mới | Hiệu năng trên tập lớn/chunks compressed chưa đo | Giữ warning và layout hiện có trong spike; đánh giá khi migration/query/workload thực sự dùng khóa mới, không gọi warning là correctness PASS trên mọi trường hợp | Task 3.2/3.7 nếu số đo cho thấy cần điều chỉnh |
| SQLite driver mới được chạy amd64, ARMv7 chỉ cross-build | Chưa chứng minh runtime trên board ARM | Giữ exact driver/libc pin, thử SQLite trên board ở checkpoint tích hợp thiết bị; simulator amd64 đã có smoke driver | Task 3.5/tích hợp board |

---

## 1. Phân Tích Chuyên Sâu Các Giới Hạn Kỹ Thuật (Detailed Technical Limitations)

### 1.1 Rủi ro lưu trữ mật khẩu sau Rotate (ROT1 vs ROT2) và Nghiệm thu Độ bền sau Khởi động lại (Restart Persistence Acceptance) — ƯU TIÊN 1
- **Hiện trạng hiện thực và cơ chế bảo vệ đã có trong mã nguồn:**
  * Mốc Task 2.8a tối giản đã hoàn thiện và kiểm chứng trình tự xoay vòng **ROT1** (`DisableClient -> Disabled Readback -> Exact Role Readback -> SetPassword -> EnableClient -> Fresh TLS Login`). Mật khẩu mới được chứng minh hoạt động thành công trong bộ nhớ RAM của Mosquitto broker đang chạy thông qua kết nối `fresh TLS login` (`FreshPositiveVerified = true`).
  * **Cơ chế bảo vệ khi Backend khởi động lại:** Bộ phân loại `SimplifiedStartupClassifier` (`src/internal/mqttcredential/simplified_startup.go`) quét bảng `gateway_mqtt_credential_events` khi Go backend khởi động lại, tìm các thao tác dở dang ở các pha unfinalized (`intent`, `dynsec_mutation`, `snapshot_readback`) và chuyển sang trạng thái `recovery_needed`. Trong phạm vi kiểm thử đơn vị (`src/internal/mqttcredential/simplified_startup_test.go`), **bộ phân loại khởi động đã hiện thực không tự động phát lại (does not auto-replay) các đột biến chưa hoàn tất**, ngăn chặn việc tự ý gửi lại lệnh tới broker hay làm lộ secret; tuy nhiên hành vi này mới được kiểm chứng ở tầng unit classifier, chưa có kiểm chứng E2E sau khởi động lại thực tế.
  * **Cơ chế kiểm tra snapshot đĩa khi Revoke:** Bộ đọc `DynSecSnapshotReader.VerifyRevoked` (`src/internal/mqttcredential/dynsec_snapshot.go`) kiểm tra trực tiếp tệp `dynamic-security.json` ở tầng hệ điều hành, đảm bảo client đã mang cờ `disabled=true` và cấu trúc role/ACL được giữ nguyên trên đĩa trước khi commit CSDL.
  * **Khóa sở hữu phiên:** `credentialOwner` (`src/cmd/server/credential_owner.go`) duy trì session lock độc quyền `0x494f545f43524544` trên một kết nối PostgreSQL chuyên biệt, chuyển ngay sang trạng thái fail-closed nếu mất kết nối CSDL.
- **Giới hạn kỹ thuật và khoảng trống kiểm chứng thực tế (Missing E2E Proof):**
  * **Thiếu bộ kiểm tra ROT2 trên đĩa:** Cơ chế phân tích cú pháp hash mật khẩu mới ghi trên đĩa cứng (**ROT2** - `exact persisted-new-password verifier`) vẫn đang hoãn lại. Trình kiểm tra snapshot chưa giải mã hoặc xác minh hash mật khẩu mới trên tệp `dynamic-security.json` sau khi Rotate.
  * **Rủi ro nạp lại mật khẩu cũ khi Broker khởi động lại (Restart-Old-Password Risk):** Cơ chế và thời điểm ghi bộ nhớ đệm ra tệp `dynamic-security.json` của Mosquitto DynSec plugin trên đĩa chưa được kiểm chứng chi tiết qua mã nguồn ghim (unverified disk persistence timing/semantics). Nếu tiến trình Mosquitto gặp sự cố, crash đột ngột hoặc dừng cưỡng bức trong tình huống dữ liệu chưa được flush bền vững xuống đĩa, khi khởi động lại Mosquitto có thể nạp lại phiên bản tệp cũ chứa **mật khẩu cũ**.
  * **Chưa có kiểm chứng E2E cho ca kiểm thử Khởi động lại (T10 Restart Persistence Acceptance):** Chưa thiết lập kịch bản kiểm thử native end-to-end chứng minh rằng sau khi container Mosquitto bị khởi động lại lạnh (cold restart):
    1. Broker nạp lại store và chấp nhận kết nối fresh TLS với mật khẩu mới đã rotate;
    2. Broker dứt khoát từ chối mật khẩu cũ trước khi rotate;
    3. Broker bảo toàn đầy đủ trạng thái thu hồi (`disabled=true`) và cấu trúc phân quyền ACL (không bị hở quyền hay mất role).
- **Biện pháp giảm thiểu và ranh giới đóng mốc:**
  Hệ thống nghiêm cấm việc tự động khởi động lại broker sau mỗi lần rotate để che giấu lỗi. Hạng mục kiểm chứng độ bền sau khởi động lại (T10) được ghi nhận là một ngoại lệ tồn đọng có chủ đích khi đóng Giai đoạn 2.

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
- **Ranh giới an toàn: Hành vi Fail-Closed / UNKNOWN KHÔNG PHẢI là bằng chứng hoàn tất phục hồi:**
  Hệ thống hiện tại có các cơ chế fail-closed khi gặp sự cố bất định sau đột biến (mutation uncertainty) hoặc commit timeout: đánh dấu tiến trình bị đầu độc (`poisoned = true`), từ chối các thao tác tiếp theo với `CodeRuntimeBusy`, ghi nhận trạng thái `recovery_needed` vào CSDL và giữ kín mật khẩu. **Tuy nhiên, hành vi fail-closed hoặc ghi nhận UNKNOWN / recovery_needed này thuần túy là rào chắn an toàn ngăn ngừa mở rộng sự cố, tuyệt đối không phải là bằng chứng cho thấy quy trình phục hồi đã được giải quyết hay hoàn tất.**
- **Yêu cầu đối với một quy trình Supervised Reset / Recovery khả dụng:**
  Để có thể phục hồi một cách an toàn và có kiểm soát cho các bản ghi bị rơi vào trạng thái bất định hoặc mất mật khẩu một lần (`lost secret`), hệ thống bắt buộc phải đáp ứng đầy đủ 5 tiêu chí:
  1. **Quy trình phục hồi khả dụng (Usable recovery):** Thay thế stub `denyOnlyResetPrerequisite` bằng luồng nghiệp vụ thực thi được dưới sự giám sát trực tiếp của Platform Operator;
  2. **Chứng minh trạng thái tĩnh lặng (Proven quiescence):** Xác nhận chắc chắn broker control channel không còn lệnh in-flight, tiến trình backend quản trị cũ đã dừng hoàn toàn, và các kênh quản trị không bị can thiệp song song;
  3. **Không ghi đè trạng thái cũ (No stale overwrite):** Cơ chế phiên bản hoặc hàng rào ngăn chặn tuyệt đối tình huống một mutation cũ bị trễ đột ngột ghi đè lên trạng thái vừa được reset;
  4. **Bảo toàn lịch sử kiểm toán (Retain audit trail):** Giữ nguyên vẹn toàn bộ chuỗi sự kiện bất biến trong `gateway_mqtt_credential_events`, không xóa hay sửa đổi lịch sử lỗi để phục vụ đối soát;
  5. **Không phát lại mật khẩu (No secret replay):** Tuyệt đối không cấp lại mật khẩu cũ đã mất hoặc mật khẩu thuộc thao tác bất định; chỉ sinh mật khẩu mới ngẫu nhiên qua CSPRNG và phân phối duy nhất một lần qua kênh bảo vệ.
- **Hệ quả vận hành khi đóng Giai đoạn 2:**
  Hệ thống chưa hiện thực và chưa kiểm chứng quy trình Supervised Reset khả dụng thỏa mãn các điều kiện trên. Ca kiểm thử T07 được ghi nhận là ngoại lệ hoãn lại (deferred closeout gap). Platform Operator không được xem endpoint reset là công cụ khả dụng.

---

### 1.4 Thiếu hàng rào phân tán và bảo đảm tĩnh lặng Broker (No Distributed Fencing & Broker Quiescence) — ƯU TIÊN 4
- **Hiện trạng hiện thực:**
  Thành phần `credentialOwner` (`src/cmd/server/credential_owner.go`) sử dụng PostgreSQL session advisory lock (`pg_try_advisory_lock` với khóa `0x494f545f43524544`) trên một kết nối CSDL chuyên biệt (hijacked connection).
- **Giới hạn kỹ thuật:**
  Khóa advisory lock này chỉ có tác dụng loại trừ cạnh tranh cục bộ giữa các tiến trình Go backend cùng kết nối tới chung một cơ sở dữ liệu PostgreSQL. Session advisory lock **tuyệt đối không phải là bằng chứng phân tán hàng rào (distributed fencing) hay bảo đảm tĩnh lặng (quiescence proof)**:
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
- **Khoảng trống và ranh giới:** Bài kiểm thử `native-compose-smoke --full-http` gần đây đã kiểm chứng thành công đường dẫn build gốc `src/Dockerfile` (`target: backend`) cho lát cắt smoke đó. Tuy nhiên, các bài diễn tập đột biến DynSec trước đây (host static binary) không tự động kế thừa bằng chứng tương đương bản build gốc, và dự án chưa kiểm chứng tính tương đương toàn diện của bản build gốc đối với toàn bộ luồng DynSec mutations.

---

### 1.7 Cổng duyệt độc lập, Ma trận ACL Backend và độ bao phủ toàn diện Giai đoạn 2 (Review Gate & Full Stage 2 Gaps) — ƯU TIÊN 7 & 8
- **Cổng duyệt độc lập (Independent Reviewer Gate):** Quy trình nghiệm thu độc lập theo kế hoạch `AGENTS.md` vẫn đang ở trạng thái chờ duyệt.
- **Ranh giới Phân quyền Topic ACL Thực tế (Active Topic ACL Boundaries):**
  * **Thiếu sót cấu hình quyền trên Backend Principal hiện tại:** Trong tệp bootstrap `scripts/mqtt-bootstrap.py` (hàm `compose_store`), tài khoản `backend_service` (role `bootstrap_backend`) **chỉ mới được cấu hình quyền** cho luồng thu thập dữ liệu (telemetry ingress) gồm `gateways/+/telemetry/#` và `gateways/+/acks/#`.
  * **Chưa có quyền gửi lệnh (Downlink Commands):** Backend hiện **chưa có quyền** `publishClientSend` trên `gateways/+/commands/#`. Nếu backend hoặc operator cố phát lệnh điều khiển qua tài khoản này, Mosquitto sẽ từ chối với lỗi quyền truy cập.
  * **Chưa có quyền nhận trạng thái và phản hồi lệnh:** Backend hiện **chưa có quyền** `subscribeLiteral` hay `publishClientReceive` trên `gateways/+/status` hay `gateways/+/responses/#`.
  * **Ràng buộc của `subscribeLiteral` trong Mosquitto DynSec:** Plugin DynSec áp dụng kiểm tra nghiêm ngặt `subscribeLiteral`: chuỗi topic filter trong gói tin `SUBSCRIBE` bắt buộc phải khớp chính xác từng ký tự với chuỗi trong ACL. Khi `backend_service` được cấp `gateways/+/telemetry/#`, client chỉ có thể subscribe đúng chuỗi `gateways/+/telemetry/#`. Mọi nỗ lực subscribe topic cụ thể (ví dụ: `gateways/gw_001/telemetry/abc`) đều bị broker từ chối với lỗi `"All subscription requests were denied."`.
  * **Phân định ranh giới Gateway vs Backend:** Mặc dù trong role của Gateway (`gateway_<id>`) đã có sẵn quyền publish `responses/#`, `status` và subscribe `commands/#`, điều này chỉ nhằm phục vụ sẵn cấu trúc namespace của Gateway; toàn bộ luồng điều khiển downlink (Digital Twin command slice) **chưa thể hoạt động** cho đến khi backend được mở rộng quyền và kiểm thử đồng bộ.
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
| **P1** | **Xác thực lưu trữ mật khẩu sau Rotate (ROT2 Verifier) & Nghiệm thu Khởi động lại (T10)** | Rủi ro nạp lại mật khẩu cũ nếu broker crash sau khi rotate; thiếu bằng chứng E2E cho T10. | Hiện thực bộ đọc phân tích hash mật khẩu mới trên snapshot `dynamic-security.json` trước khi finalize DB; bổ sung test case native khởi động lại Mosquitto container và xác minh kết nối. |
| **P2** | **Nghiên cứu cơ chế đảm bảo độ bền phần cứng (`fsync`)** | Mất điện đột ngột có thể làm mất dữ liệu snapshot trên đĩa. | Nghiên cứu cấu hình filesystem/mount flags đồng bộ (sync mount) hoặc cơ chế kích hoạt sync từ host. |
| **P3** | **Hiện thực luồng Supervised Reset khả dụng với kiểm tra Tĩnh Lặng (T07)** | Hiện tại reset hợp lệ bị từ chối fail-closed tại prerequisite 503 (deny-only); thiếu quy trình tĩnh lặng để phục hồi khi gặp sự cố bất định hoặc mất secret. | Xây dựng quy trình `ResetPrerequisite` có giám sát của operator với kiểm tra tĩnh lặng (quiescence) của tiến trình backend quản trị cũ / các đột biến MQTT in-flight và tĩnh lặng luồng quản trị (management path quiescence), đảm bảo không stale overwrite, retain audit và không secret replay. |
| **P4** | **Kiểm định Build Parity trên Dockerfile gốc cho DynSec (T11)** | Smoke full-HTTP đã build `src/Dockerfile`, nhưng diễn tập DynSec dùng nhị phân tĩnh từ host chưa kế thừa bằng chứng parity. | Kiểm tra tính tương đương của bản build `src/Dockerfile` chính thức trên toàn bộ kịch bản đột biến DynSec. |
| **P5** | **Bổ sung kiểm thử End-to-End Telemetry Ingress & DB-Commit ACK (T01 / M04)** | Observer session chỉ mới chứng minh không bị ngắt; chưa chứng minh nhận tin và lưu vào TimescaleDB qua application-level ACK. | Viết test case client gửi telemetry batch và xác nhận nhận được application DB-commit ACK trên `gateways/<id>/acks/#` sau khi dữ liệu đã commit vào TimescaleDB. |
| **P6** | **Mở rộng và kiểm chứng Ma trận ACL Backend cho Digital Twin Downlink (T02)** | `backend_service` hiện chưa có quyền gửi `commands/#` và chưa thể nhận `responses/#`, `status`; chưa thể triển khai điều khiển thiết bị hai chiều. | Mở rộng role `bootstrap_backend` trong script bootstrap và store để cấp quyền `publishClientSend` trên `gateways/+/commands/#`, `subscribeLiteral` / `publishClientReceive` trên `gateways/+/responses/#` và `gateways/+/status`; thiết lập kiểm thử phân quyền tích hợp. |
| **P7** | **Hoàn tất rà soát độc lập (Independent Reviewer Gate)** | Điều kiện tiên quyết trước khi nghiệm thu toàn bộ Task 2.8. | Chuyển giao mã nguồn và tài liệu cho reviewer độc lập thẩm định. |

---

## 4. Bảng Đối Chiếu Ma Trận Nghiệm Thu Có Giới Hạn T01–T12 (Bounded Acceptance Closeout Mapping)

### 4.1 Bối cảnh đóng mốc Giai đoạn 2 (Stage 2 Pragmatic Closeout Context)
- Theo yêu cầu và định hướng của người dùng nhằm khắc phục chậm trễ tiến độ đồ án, Giai đoạn 2 (Stage 2) được ghi nhận **đóng mốc có giới hạn (bounded scope closeout recorded under user intent)** với các ngoại lệ kỹ thuật và khoảng trống còn lại được ghi nhận minh bạch và khoanh vùng rõ ràng, phục vụ việc chuyển tiếp sang Giai đoạn 3.
- **Tuyên bố ranh giới:** Việc đóng mốc có giới hạn này được thực hiện theo chỉ đạo của người dùng, tuyệt đối **không phải là nghiệm thu hoàn tất toàn diện (not full acceptance), không được phê duyệt bởi bên thứ ba (no third-party approval), và không đồng nghĩa với việc hệ thống đã sẵn sàng vận hành sản xuất (not production-ready)**.
- **Nguồn thẩm quyền tiêu chí nghiệm thu:** Toàn bộ tiêu chí đối chiếu tuân thủ nghiêm ngặt theo mục **0.8 Acceptance hiện hành (T01–T12)** trong tài liệu kế hoạch gốc `docs/backend_plan/task_2.8_plan.md`. Không tự ý bổ sung các mã kiểm thử ngoài danh mục hoặc mở rộng ma trận vô hạn.
- **Bối cảnh CI cục bộ và ranh giới bằng chứng:** Các cải tiến CI cục bộ gần đây đã được tích hợp vào nhánh phát triển (tiêu biểu là commit `b681401` chuẩn hóa thứ tự danh sách mount trong receipt của container helper và bổ sung cờ TCP `-h 127.0.0.1 -p 5432` cho healthcheck PostgreSQL). Bài kiểm thử `native-compose-smoke` với cờ `--full-http` trên fresh volume cục bộ đã hoàn thành PASS (bao quát 14 trường hợp role denial, 9 trường hợp JWT missing/invalid/expired, 16 snapshot denied-write giữ nguyên, và xác minh dọn dẹp sạch sẽ 10 containers, 1 network, 3 volumes). Cần lưu ý rõ: **Kết quả PASS cục bộ này KHÔNG PHẢI là bằng chứng cho thấy DynSec restart persistence (T10), supervised reset (T07) hay luồng telemetry application DB-commit ACK (T01) đã hoàn tất nghiệm thu, và không phản ánh trạng thái remote CI trên GitHub Actions hiện tại đã xanh hoàn toàn.**

### 4.2 Bảng đối chiếu chi tiết 12 ca kiểm thử T01–T12

| Mã | Tên Ca Kiểm Thử | Điều Kiện PASS Chuẩn (Theo `task_2.8_plan.md` Mục 0.8) | Hiện Trạng Hiện Thực & Bằng Chứng Mã Nguồn Hiện Có | Khoảng Trống Còn Lại (Remaining Gaps & Missing Proofs) | Trạng Thái Đóng Giai Đoạn 2 (Stage 2 Disposition) |
|:---:|---|---|---|---|:---:|
| **T01** | **Provision** | Real Go API/lifecycle; fresh TLS login, telemetry publish và application DB-commit ACK. | Đã hoàn thành API Go thực tế, sinh mật khẩu CSPRNG trong RAM, ghi intent CSDL, tạo client/role trên broker, xác nhận `fresh TLS login` thành công (`FreshPositiveVerified`), replay trả metadata không lộ mật khẩu (`TestSimplifiedServiceNativeRotateRevoke`, `TestSimplifiedHTTPNativeRotateRevoke`). | **Khoảng trống nghiệm thu lớn (OPEN proof):** Chưa có bằng chứng kiểm thử luồng nhận tin telemetry tại backend Go và gửi application DB-commit ACK về `gateways/<id>/acks/#` sau khi commit TimescaleDB. Kết nối TLS hay MQTT QoS 1 ACK không thay thế được application-level ACK này. | **BẰNG CHỨNG MỘT PHẦN / CHƯA ĐẠT ĐẦY ĐỦ**<br>*(Provision API/Auth Verified; Telemetry DB-Commit ACK OPEN/DEFERRED)* |
| **T02** | **ACL** | Own publish/subscribe allowed theo contract; cross-Gateway/control denied với protocol/receiver evidence, không coi timeout là denial. | Cấu hình store DynSec đặt `defaultACLAccess = false`. Role của Gateway giới hạn namespace `gateways/<id>/#`. Bằng chứng M04 (`a8_native_rotate_revoke_attempt4`) chứng minh observer gateway duy trì phiên kết nối khi target bị thao tác (observer survival). Script bootstrap kiểm tra từ chối truy cập ngoài namespace. | **Khoảng trống nghiệm thu lớn (OPEN proof):** Observer survival không chứng minh việc cách ly ACL; chưa có bằng chứng ở tầng người nhận (receiver delivery/protocol denial evidence) xác nhận cross-gateway publish bị chặn đến subscriber; backend principal chưa được cấp quyền `commands/#`, `responses/#`, `status`. | **BẰNG CHỨNG MỘT PHẦN / CHƯA ĐẠT ĐẦY ĐỦ**<br>*(Configured Isolation Verified; Receiver Delivery & Downlink ACL OPEN/DEFERRED)* |
| **T03** | **Rotate** | New password fresh login PASS, old password fresh login denied; metadata/version đúng, báo rõ session semantics. | Trình tự ROT1 (`Disable -> Disabled Readback -> Exact Role Readback -> SetPassword -> Enable -> Fresh TLS Login`) đã được kiểm chứng native (`a8_native_rotate_revoke_attempt4`). Mật khẩu mới login TLS thành công; mật khẩu cũ bị từ chối kết nối mới; version CSDL tăng đơn điệu; thông tin metadata phản hồi chính xác. | **Chưa có bộ kiểm tra ROT2 trên đĩa** (`exact persisted-new-password verifier`) để kiểm tra hash mật khẩu mới trên tệp `dynamic-security.json` trước khi finalize CSDL; phụ thuộc vào dữ liệu trong RAM của broker trước khi flush ra đĩa. | **PASS CÓ GIỚI HẠN**<br>*(ROT1 Flow PASS; ROT2 Disk Verifier DEFERRED)* |
| **T04** | **Revoke** | Fresh login denied và session đang mở không tiếp tục MQTT traffic; DB revoked chỉ sau verified outcome. | Đã kiểm chứng native (`a8_native_rotate_revoke_attempt4`): phiên đang mở của target gateway nhận ngay tín hiệu EOF sau `disableClient`; kết nối mới bằng mật khẩu cũ bị từ chối TLS auth; bộ đọc snapshot `VerifyRevoked` kiểm tra trên đĩa client đã mang cờ `disabled=true` và role/ACL không đổi trước khi CSDL commit trạng thái `revoked`. | Logic cốt lõi của luồng Revoke đã hoàn chỉnh và đạt yêu cầu; việc bảo toàn trạng thái revoked sau khi broker khởi động lại thuộc phạm vi kiểm chứng của T10. | **VERIFIED PROTOTYPE**<br>*(Target Disconnect, Denial & Snapshot VerifyRevoked PASS)* |
| **T05** | **Authorization** | Non-admin mutation/reset denied; invalid JWT và DB admin-check outage fail closed. | Đã kiểm chứng chặn đứng yêu cầu phi admin với HTTP 403 trước mutation; chặn JWT không hợp lệ/hết hạn với HTTP 401; rehearsal Part 2 xác nhận Supabase Auth JWT mang claim `role: "authenticated"`; full HTTP smoke kiểm chứng 14 role denial cases và 9 JWT negative cases. | Chưa thiết lập kịch bản native fault injection ngắt kết nối CSDL đột ngột trong lúc đang kiểm tra bảng `platform_admins` (DB admin-check outage fail-closed). | **VERIFIED PROTOTYPE**<br>*(Admin & JWT RBAC PASS; Live DB Outage Fault Injection DEFERRED)* |
| **T06** | **Verification failure** | Không success/secret; phân biệt pre-mutation failure, partial và unknown. | Bộ kiểm thử offline `service_simplified_test.go` đã kiểm chứng việc phân biệt các pha lỗi: lỗi pre-mutation không gọi broker, lỗi trung gian readback kích hoạt đầu độc tiến trình (`poison()`), lỗi commit timeout/finalization trả về `CodeFinalizationPending` và tuyệt đối không bao giờ làm lộ secret. | Chưa thiết lập bài kiểm thử fault injection trực tiếp trong fixture native runner live container (hiện tại mới chứng minh qua unit mock offline). | **VERIFIED PROTOTYPE**<br>*(Offline No-Secret & Poison PASS; Native Fault Injection DEFERRED)* |
| **T07** | **Reset** | Guarded unknown/lost-secret setup bằng real lifecycle; quiescence rồi fresh password verified, no stale overwrite. | Router đã định tuyến `POST /v1/admin/gateways/{id}/mqtt-credential/reset`; parser kiểm tra chặt chẽ reason payload; middleware xử lý đúng thứ tự 401/403/400/415. | **CHƯA ĐẠT (DEFERRED / OPEN):** Runtime prerequisite hiện tại là `denyOnlyResetPrerequisite{}` luôn fail-closed với HTTP 503 (`CodeServiceUnavailable`). Chưa có quy trình phục hồi khả dụng, chưa có kiểm tra tĩnh lặng (quiescence), chưa có hàng rào chống stale overwrite và chưa kiểm chứng khôi phục lost secret. | **DEFERRED EXCEPTION**<br>*(Deny-Only Runtime; Supervised Reset & Quiescence DEFERRED)* |
| **T08** | **Secret isolation** | Không password trong DB/log/audit/argv/durable report; private generated files từ creation. | Mật khẩu sinh bằng CSPRNG trong RAM, không lưu CSDL, không ghi log hệ thống, không in argv/stdout, chỉ trả về một lần duy nhất qua phản hồi đầu tiên (`SecretResult`). Replay chỉ trả metadata (`secret_returned: false`). Tệp bootstrap tạo với mode `0600` trong thư mục `0700`. Các route credential mutation yêu cầu body JSON hợp lệ và cấm hoàn toàn query string; Go backend không ghi log request body hay secret. | Ranh giới kiểm chứng: Chưa có chứng nhận toàn diện không rò rỉ (no unsourced complete no-leak certification); Nginx access log ghi nhận `$request` (chứa URI/query string nếu client vô tình gửi, dù backend từ chối), và bằng chứng bảo vệ bí mật mới nằm trong phạm vi log backend, audit DB và artifacts kiểm thử đã được rà soát. | **VERIFIED PROTOTYPE**<br>*(CSPRNG, RAM-Only & In-Scope No-Leak PASS)* |
| **T09** | **Retry/lost response** | Same-key không duplicate mutation/secret replay; status lookup và controlled new reset đúng contract. | Replay cùng key và cùng actor trả về metadata hiện tại (`secret_returned: false`), không gọi lại broker, không sinh mật khẩu mới; `CheckRevoked` đọc lại trạng thái live khi replay revoke đã hoàn tất. | Lỗi xung đột key binding (`CodeIdempotencyConflict`) mới kiểm chứng qua unit và DB integration test, chưa có native test proof riêng; quy trình reset có kiểm soát sau lost response chưa khả dụng do T07 ở trạng thái deny-only. | **PASS CÓ GIỚI HẠN**<br>*(Idempotent Metadata Replay PASS; Native Mismatch & Reset Recovery DEFERRED)* |
| **T10** | **Restart persistence** | Broker restart giữ credential/ACL mới và revoke; backend restart không replay pending rotation/reset. | `SimplifiedStartupClassifier` quét các bản ghi dở dang khi backend khởi động lại và chuyển sang `recovery_needed`; trong phạm vi unit test (`simplified_startup_test.go`), backend không phát lại mutation dở dang. | **CHƯA ĐẠT (DEFERRED / OPEN):** Tồn tại khoảng trống lớn giữa bảo vệ ở tầng unit classifier và kiểm chứng khởi động lại end-to-end; chưa có bài kiểm thử native E2E chứng minh sau khi Mosquitto khởi động lại lạnh (cold restart): broker chấp nhận mật khẩu mới, dứt khoát từ chối mật khẩu cũ, bảo toàn trạng thái revoked và cấu trúc ACL. | **DEFERRED EXCEPTION**<br>*(Backend Startup Unit Protection PASS; E2E Broker Restart Persistence OPEN/DEFERRED)* |
| **T11** | **Fixture/evidence** | Exact runtime source/binaries/sanitized input manifest và effective topology; owned IDs/receipts, cleanup verified, bounded failure paths. | Bằng chứng thực thi được lưu vết với SHA-256 manifest, digest container cố định. Commit `b681401` chuẩn hóa thứ tự mount receipt; smoke `native-compose-smoke --full-http` xác nhận dọn dẹp 10 containers, 1 network, 3 volumes và đã build từ chính `src/Dockerfile` (`target: backend`). | Bài smoke full-HTTP đã kiểm chứng đường build `src/Dockerfile` cho lát cắt đó; tuy nhiên các bài diễn tập đột biến DynSec trước đây dùng nhị phân tĩnh host không tự động kế thừa bằng chứng tương đương bản build gốc, và chưa có kiểm chứng build parity toàn diện cho toàn bộ luồng DynSec. | **VERIFIED PROTOTYPE**<br>*(Manifest, Fixture & Cleanup PASS; DynSec Root Build Parity DEFERRED)* |
| **T12** | **Journal/failure** | Intent trước broker mutation; DB unavailable/COMMIT unknown không success; startup phát hiện unfinished attempt để admin xử lý. | Luồng nghiệp vụ luôn ghi nhận trạng thái `PhaseIntent` vào bảng `gateway_mqtt_credential_events` trước khi gửi bất kỳ lệnh nào tới broker. Quá trình khởi động phát hiện và chuyển các intent dở dang sang `recovery_needed`. Lỗi commit sau mutation kích hoạt `poison()` và không trả secret. | Chưa thiết lập kịch bản native fault injection ngắt kết nối CSDL đúng thời điểm commit sau khi broker đã mutation trên môi trường live container. | **VERIFIED PROTOTYPE**<br>*(Intent Journal & Startup Scanner PASS; Live Commit Fault Injection DEFERRED)* |
