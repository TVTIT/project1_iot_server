# Tài liệu Client — 01: Tổng quan và Kiến trúc ứng dụng Flutter Web

**Cập nhật:** 2026-10-03  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md`, `docs/backend/stage-2-task-2.2-authentication.md`, `docs/backend/stage-2-task-2.3-authorization.md`, `docs/backend/stage-2-task-2.4-provisioning.md`, `config/nginx/nginx.conf.template`.

---

## 1. Mục tiêu ứng dụng Flutter Web

Ứng dụng **Flutter Web** đóng vai trò là bảng điều khiển trung tâm (Dashboard giám sát và vận hành) cho hệ thống IoT Gateway–Server, chạy trực tiếp trên các trình duyệt hiện đại (Chrome, Edge, Firefox, Safari) và tương thích tốt trên desktop, tablet cũng như mobile browser.

- **Đối tượng sử dụng:**
  - **Người dùng cuối / Chủ sở hữu trạm (`owner`):** Theo dõi trạng thái hoạt động, thông số cảm biến, lịch sử dữ liệu và cấu hình thiết bị thuộc quyền quản lý của mình.
  - **Người vận hành kỹ thuật (`operator`):** Giám sát trạng thái hoạt động tức thời, nhận cảnh báo và thực thi các thao tác vận hành an toàn được cho phép trên trạm được giao.
  - **Người xem (`viewer`):** Giám sát dữ liệu đo và trạng thái trạm ở chế độ chỉ đọc (Read-only).
- **Định hướng nền tảng:**
  - **Ưu tiên Web trước cho đồ án:** Trong giai đoạn phát triển ban đầu và nghiệm thu Đồ án 1, trọng tâm phát triển được đặt vào **Flutter Web**. Lựa chọn này giúp sinh viên dễ dàng triển khai, trình diễn trực quan trên trình duyệt máy tính, kiểm thử nhanh chóng bằng công cụ Developer Tools mà không phụ thuộc vào thiết bị phần cứng di động chuyên biệt hay máy ảo Android Emulator nặng nề.
  - **Bảo toàn khả năng đóng gói sang Android:** Mã nguồn được thiết kế chặt chẽ theo kiến trúc phân tầng (Clean Architecture), tách biệt hoàn toàn giữa tầng giao diện, quản lý trạng thái, tầng nghiệp vụ và tầng giao tiếp dữ liệu. Thiết kế độc lập nền tảng này đảm bảo hệ thống có thể đóng gói (build) sang ứng dụng di động Android ở giai đoạn sau mà không phải thay đổi cấu trúc mã nguồn cốt lõi.
- **Tiêu chí phát triển:**
  - Đơn giản, hoàn chỉnh và có khả năng giải trình kỹ thuật cao (phù hợp với quy mô Đồ án 1 do một sinh viên thực hiện).
  - Tách bạch rõ ràng giữa định danh người dùng (Supabase Auth) và phân quyền nghiệp vụ (Go Backend).

---

## 2. Kiến trúc hệ thống và Luồng mạng (Network Topology)

### 2.1. Sơ đồ tổng thể

Khác với ứng dụng di động native, ứng dụng Flutter Web chạy trong môi trường bảo mật của trình duyệt, chịu sự ràng buộc nghiêm ngặt của **Chính sách cùng nguồn gốc (Same-Origin Policy - SOP)** và cơ chế **Chia sẻ tài nguyên liên nguồn gốc (CORS)**.

Toàn bộ lưu lượng mạng từ Flutter Web đi vào hệ thống thông qua một cổng tiếp nhận duy nhất là **Nginx Reverse Proxy** (ở môi trường Production đứng sau Cloudflare Tunnel, hoặc kết nối trực tiếp trong môi trường phát triển cục bộ).

```text
+-------------------------------------------------------------------------+
|                        Flutter Web Client                               |
|            (Trình duyệt Chrome / Edge / Firefox / Safari)               |
+------------------------------------+------------------------------------+
                                     |
                                     | HTTP / HTTPS / WSS
                                     v
+-------------------------------------------------------------------------+
|                           Nginx Reverse Proxy                           |
|                    (Port 80/443, Quản lý định tuyến)                    |
+---------+----------------+-------------------+----------------+---------+
          |                |                   |                |
          | /              | /auth/v1/*        | /storage/v1/*  | /v1/* & /v1/ws
          v                v                   v                v
+-------------------+  +-------------------+  +---------------+  +-------------------+
|  Flutter Web App  |  |    Supabase       |  |   Supabase    |  |    Go Backend     |
|   (Static Build)  |  |   API Gateway     |  |  API Gateway  |  | (Modular Monolith |
| HTML/JS/CanvasKit |  |  (Envoy / Kong)   |  | (Envoy / Kong)|  |    Port 8080)     |
+-------------------+  +---------+---------+  +-------+-------+  +---------+---------+
                                 |                    |                    |
                                 v                    v                    v
                       +-------------------+  +---------------+  +-------------------+
                       |   Supabase Auth   |  |SupabaseStorage|  | PostgreSQL 16 +   |
                       |     (GoTrue)      |  |(Private Bucket|  |   TimescaleDB     |
                       +-------------------+  +---------------+  +-------------------+
```

### 2.2. Chi tiết phân luồng qua Nginx

Cấu hình định tuyến tại Nginx (`config/nginx/nginx.conf.template`) quy định rõ ranh giới trách nhiệm cho các dịch vụ:

1. **Phục vụ tệp tĩnh Web Dashboard (`/` hoặc `/dashboard/*`):**
   - Nginx phục vụ trực tiếp gói mã nguồn tĩnh của Flutter Web (gồm `index.html`, `main.dart.js`, `flutter.js`, `assets/`, font, CanvasKit WASM binaries).
   - Khi truy cập địa chỉ gốc, trình duyệt tải ứng dụng Web về và thực thi phía Client.
2. **Xác thực người dùng (`/auth/v1/*`):**
   - Nginx chuyển tiếp tới Supabase API Gateway (Envoy/Kong), đích đến là dịch vụ `supabase-auth` (GoTrue).
   - Flutter Web sử dụng `supabase_flutter` SDK để gọi API đăng nhập qua Email/Password, nhận về cặp mã xác thực `access_token` (JWT) và `refresh_token`.
3. **API Nghiệp vụ (`/v1/*`):**
   - Nginx chuyển tiếp trực tiếp tới `backend:8080` (Go Backend).
   - Mọi request nghiệp vụ đều bắt buộc đính kèm header `Authorization: Bearer <access_token>`.
   - Go Backend xác minh chữ ký JWT tại chỗ (HS256) và truy vấn PostgreSQL để kiểm tra quyền truy cập tài nguyên (User–Gateway membership).
4. **Dữ liệu thời gian thực (`/v1/ws` hoặc `/v1/telemetry/ws`):**
   - Nginx nâng cấp kết nối HTTP thành WebSocket (`Upgrade: $http_upgrade`, `Connection: "upgrade"`) và chuyển tới Go Backend.
   - Trình duyệt Web duy trì kết nối WebSocket để nhận dữ liệu đo mới (telemetry) trực tiếp từ server ngay sau khi transaction lưu trữ cơ sở dữ liệu được commit.
5. **Lưu trữ tệp đa phương tiện (`/storage/v1/*`):**
   - Nginx chuyển tiếp tới Supabase API Gateway để vào `supabase-storage`.
   - Lưu trữ ảnh private (ví dụ ảnh chụp hiện trường từ Gateway).
   - Flutter Web không truy cập trực tiếp bằng `service_role` key; việc đọc ảnh private bắt buộc thông qua **Signed Read URL** có thời hạn ngắn do Go Backend cấp sau khi kiểm tra quyền người dùng.

### 2.3. Giải pháp giải quyết bài toán CORS trên Web

Trình duyệt áp dụng chính sách Same-Origin Policy để bảo vệ người dùng. Một request được coi là Cross-Origin khi khác Protocol, Domain hoặc Port so với trang Web đang mở.

1. **Trong môi trường Triển khai Production / Staging (Khuyến nghị chuẩn):**
   - Bản build tĩnh của Flutter Web (`flutter build web --release`) được Nginx phục vụ trực tiếp tại root domain (hoặc sub-path), ví dụ `https://iot.example.com/`.
   - Các API Backend (`/v1/*`), Auth (`/auth/v1/*`) và Storage (`/storage/v1/*`) cũng nằm trên **CÙNG MỘT DOMAIN** thông qua Nginx Reverse Proxy.
   - **Kết quả:** Request từ Flutter Web tới API là **Same-Origin** hoàn toàn, loại bỏ triệt để rủi ro phát sinh lỗi CORS và không cần cấu hình header CORS phức tạp ở backend.
2. **Trong môi trường Phát triển cục bộ (Local Development):**
   - Khi chạy lệnh phát triển `flutter run -d chrome --web-port=3000`, Flutter Web chạy trên máy chủ phát triển cục bộ tại `http://localhost:3000`.
   - Khi ứng dụng gửi request tới Nginx Reverse Proxy tại `http://localhost:80` (hoặc `http://localhost`), đây là request liên cổng (Cross-Origin).
   - **Giải pháp xử lý:**
     - Nginx và Go Backend được cấu hình xử lý preflight request (`OPTIONS`) và trả về các header CORS cần thiết:
       ```http
       Access-Control-Allow-Origin: http://localhost:3000
       Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS
       Access-Control-Allow-Headers: Authorization, Content-Type, Accept, apikey, X-Client-Info
       ```
     - Hoặc có thể cấu hình Nginx cục bộ để proxy cả cổng của Flutter dev server, đưa toàn bộ về chung một origin `http://localhost:80`.

### 2.4. Nguyên tắc bảo mật trên Client

- **Tuyệt đối không nhúng `service_role` key vào mã nguồn Client:** `service_role` key là khóa quản trị kỹ thuật tối cao của Supabase, chỉ được lưu ở môi trường backend nội bộ. Client chỉ được phép giữ `SUPABASE_ANON_KEY`.
- **Lưu trữ phiên an toàn trên trình duyệt:** SDK `supabase_flutter` tự động quản lý phiên và lưu trữ token an toàn trong `localStorage` của trình duyệt. Không lưu trữ thông tin nhạy cảm ở các biến toàn cục không được bảo vệ.
- **JWT hợp lệ không đồng nghĩa với có quyền truy cập:** Token JWT chỉ chứng minh danh tính người dùng (`authenticated`). Mọi quyền đọc/ghi trên từng Gateway đều do Go Backend thẩm định dựa trên bảng `user_gateways` trong cơ sở dữ liệu.
- **Không tự tin cậy dữ liệu phía client:** Mọi tham số như `user_id` không được truyền qua URL query hay request body để đòi quyền. Go Backend lấy `user_id` duy nhất từ claim `sub` của JWT đã xác minh.

---

## 3. Phạm vi chức năng hiện tại (Giai đoạn 2)

Trong Giai đoạn 2 của dự án, ứng dụng Flutter Web tập trung hoàn thiện các tính năng nền tảng sau:

### 3.1. Đăng nhập tập trung (Centralized Authentication)
- Màn hình đăng nhập yêu cầu Email và Password với giao diện đáp ứng (responsive), hiển thị tối ưu trên cả desktop lẫn màn hình di động.
- Gọi trực tiếp API của Supabase Auth qua SDK (`supabase.auth.signInWithPassword(...)`).
- Quản lý phiên làm việc (`access_token` và `refresh_token`), tự động làm mới token trong nền khi gần hết hạn (thời gian sống mặc định 3600 giây).
- Đăng xuất (Sign out): Hủy session tại Supabase Auth và làm sạch dữ liệu phiên trong bộ nhớ trình duyệt.

### 3.2. Danh sách Gateway theo phân quyền (`GET /v1/gateways`)
- Gọi tới Go Backend để lấy danh sách các Gateway mà người dùng hiện tại có quyền truy cập.
- **Contract phản hồi:**
  ```json
  {
    "items": [
      {
        "gateway_id": "gw-lab-01",
        "name": "Trạm quan trắc Phòng Thí Nghiệm",
        "description": "Gateway AM5728 đo rung và nhiệt độ",
        "role": "owner",
        "created_at": "2026-10-02T08:30:00Z"
      }
    ]
  }
  ```
- Hiển thị vai trò của người dùng trên từng Gateway (`owner`, `operator`, `viewer`) dưới dạng nhãn trực quan.
- Trường hợp người dùng chưa được phân quyền Gateway nào: hiển thị danh sách rỗng (`items: []`) kèm thông báo hướng dẫn liên hệ quản trị viên.

### 3.3. Danh sách Sensor theo Gateway (`GET /v1/gateways/{gateway_id}/sensors`)
- Khi người dùng chọn một Gateway cụ thể, ứng dụng điều hướng sang màn hình chi tiết và gọi API lấy danh sách sensor trực thuộc:
- **Contract phản hồi:**
  ```json
  {
    "gateway_id": "gw-lab-01",
    "items": [
      {
        "sensor_id": "sensor-temp-01",
        "name": "Cảm biến nhiệt độ môi trường",
        "unit": "°C",
        "created_at": "2026-10-02T08:35:00Z"
      }
    ]
  }
  ```
- Xử lý mã lỗi chuẩn từ Backend:
  - `401 Unauthorized`: Token hết hạn hoặc không hợp lệ -> Điều hướng người dùng về màn hình đăng nhập.
  - `404 Not Found`: Gateway không tồn tại hoặc người dùng không có quyền truy cập (Backend trả cùng lỗi 404 để bảo toàn tính cô lập dữ liệu).

---

## 4. Các tính năng ngoài phạm vi (Deferred) hoặc chờ giai đoạn sau

Nhằm đảm bảo hoàn thành MVP đúng hạn và đúng định hướng kiến trúc tại `AGENTS.md`, các tính năng sau **không** thuộc phạm vi triển khai của Client ở giai đoạn hiện tại:

1. **Không có chức năng Đăng ký tài khoản công khai (Public Self-Signup Disabled):**
   - Hệ thống áp dụng chính sách quản trị tập trung (`GOTRUE_DISABLE_SIGNUP=true`). Tài khoản người dùng được cấp phát độc quyền bởi Platform Administrator thông qua Supabase Studio hoặc công cụ quản trị nội bộ.
   - Giao diện Client **không có nút "Đăng ký" (Sign Up)**. Không tạo form đăng ký tự do.
2. **Không có tính năng Tự nhận Gateway (No Gateway Self-Claiming):**
   - Người dùng không thể tự nhập mã Gateway để chiếm quyền sở hữu. Quan hệ sở hữu và vận hành (`user_gateways`) do Platform Administrator thiết lập tập trung.
3. **Chưa triển khai lệnh điều khiển hai chiều phức tạp (Device Downlink Control Deferred):**
   - Chưa tích hợp gửi lệnh cấu hình `desired_state` hay các lệnh one-shot (như reboot, capture-image) trên giao diện Client.
   - Toàn bộ cơ chế Digital Twin Command & Transactional Outbox thuộc phạm vi phát triển Backend Giai đoạn 3; Client hiện fail-closed ở chế độ chỉ đọc dữ liệu Gateway/Sensor.
4. **Không phát video trực tiếp (No Live-streaming / No Media Server):**
   - Hệ thống không hỗ trợ WebRTC, RTSP hay HLS streaming.
   - Phân hệ Media chỉ phục vụ ảnh tĩnh chụp theo sự kiện thông qua Supabase Storage.
5. **Không dùng gRPC hay gRPC-Web:**
   - Client giao tiếp thuần túy qua REST (JSON) và WebSocket chuẩn của nền tảng web.

---

## 5. Cấu hình môi trường Client (.env)

### 5.1. Các biến môi trường bắt buộc

Ứng dụng sử dụng compile-time environment variables qua cờ `--dart-define` (hoặc gói cấu hình môi trường) để quản lý cấu hình. File cấu hình `.env` cho Client được thiết lập:

```ini
# Base URL của Go Backend REST API (phải có tiền tố /v1)
# Trong môi trường Local Web, kết nối qua Nginx reverse proxy tại localhost:80
BACKEND_API_BASE_URL=http://localhost/v1

# URL của Supabase API Gateway (phục vụ Auth & Storage)
SUPABASE_URL=http://localhost

# Khóa công khai của Supabase (Anonymous Key - an toàn khi nhúng Client Web)
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# WebSocket URL phục vụ dữ liệu thời gian thực
BACKEND_WS_URL=ws://localhost/v1/ws
```

> **Lưu ý quan trọng về địa chỉ mạng:**  
> Không sử dụng địa chỉ `10.0.2.2` trên Flutter Web. Địa chỉ `10.0.2.2` là alias nội bộ dành riêng cho Android Emulator. Khi chạy trên trình duyệt Web (Chrome/Edge/Firefox), máy khách chính là máy host, do đó địa chỉ trỏ tới Nginx proxy cục bộ là `http://localhost` (cổng 80) hoặc `http://localhost:<nginx_port>`. Khi triển khai Production cùng domain, có thể sử dụng relative URL hoặc domain chính thức.

### 5.2. Cấu hình địa chỉ mạng theo môi trường chạy

| Môi trường chạy | Địa chỉ Nginx / Backend (`BACKEND_API_BASE_URL`) | Địa chỉ Supabase (`SUPABASE_URL`) | Ghi chú kỹ thuật |
|---|---|---|---|
| **Flutter Web Local Dev** | `http://localhost/v1` | `http://localhost` | Web chạy trên trình duyệt qua `flutter run -d chrome`. Trỏ tới Nginx cổng 80. |
| **Flutter Web Production** | `https://iot.example.com/v1` (hoặc `/v1`) | `https://iot.example.com` | Web build tĩnh được phục vụ chung domain qua Nginx. Triệt tiêu hoàn toàn CORS. |
| **Android Emulator (Dự phòng mở rộng)** | `http://10.0.2.2/v1` | `http://10.0.2.2` | Dành riêng cho kịch bản đóng gói Android sau này. `10.0.2.2` trỏ về máy host. |
| **Thiết bị thật / LAN (Dự phòng)** | `http://192.168.1.x/v1` | `http://192.168.1.x` | Thiết bị cầm tay và máy tính cùng chung một lớp mạng Wi-Fi nội bộ. |

---

## 6. Kiến trúc phân tầng mã nguồn Client đề xuất

Để đảm bảo mã nguồn dễ bảo trì, dễ viết kiểm thử đơn vị (Unit Test) và giữ khả năng đóng gói đa nền tảng (Web trước, mở rộng Android sau) mà không phải viết lại code, ứng dụng được tổ chức theo kiến trúc phân tầng chuẩn (**Clean Architecture thực dụng cho Flutter**):

```text
+-------------------------------------------------------------------------+
|                           Presentation Layer                            |
|             (Screens / Pages, Reusable Widgets, Responsive Views)       |
+------------------------------------+------------------------------------+
                                     |
                                     | Dispatches Events / Observes States
                                     v
+-------------------------------------------------------------------------+
|                         State Management Layer                          |
|                     (BLoC / Cubit - flutter_bloc)                       |
+------------------------------------+------------------------------------+
                                     |
                                     | Calls Use Cases / Repositories
                                     v
+-------------------------------------------------------------------------+
|                        Domain & Repository Layer                        |
|       (Entity Models, Repository Interfaces & Implementations)          |
+------------------------------------+------------------------------------+
                                     |
                                     | Invokes Remote Calls / Storage
                                     v
+-------------------------------------------------------------------------+
|                            Data Source Layer                            |
|    (Supabase Client SDK, Go Backend HTTP Client, Web Storage Adapter)   |
+-------------------------------------------------------------------------+
```

### 6.1. Chi tiết các tầng

1. **Presentation Layer (Tầng Giao diện):**
   - **Screens/Pages:** `LoginScreen`, `GatewayListScreen`, `SensorListScreen`. Được thiết kế hỗ trợ Responsive Layout (co giãn tốt theo chiều rộng màn hình từ PC đến điện thoại).
   - **Widgets:** Các thành phần UI dùng lại như `GatewayCard`, `SensorListItem`, `RoleBadge`, `ErrorStateView`, `LoadingIndicator`.
   - Lớp này hoàn toàn không chứa business logic hay gọi HTTP trực tiếp; chỉ phát sự kiện và lắng nghe trạng thái từ BLoC/Cubit để vẽ UI.
2. **State Management Layer (Tầng Quản lý Trạng thái):**
   - Khuyến nghị sử dụng thư viện chuẩn công nghiệp: `flutter_bloc` (hoặc Cubit cho các luồng đơn giản). Hoạt động hoàn toàn độc lập với nền tảng hiển thị (Web hay Mobile).
   - **`AuthBloc`:** Quản lý trạng thái xác thực người dùng:
     - Trạng thái: `AuthInitial`, `AuthLoading`, `Authenticated(user)`, `Unauthenticated`, `AuthFailure(error)`.
   - **`GatewayListCubit`:** Quản lý danh sách Gateway:
     - Trạng thái: `GatewayListLoading`, `GatewayListLoaded(items)`, `GatewayListEmpty`, `GatewayListError(error)`.
   - **`SensorListCubit`:** Quản lý danh sách Sensor theo Gateway:
     - Trạng thái: `SensorListLoading`, `SensorListLoaded(gatewayId, items)`, `SensorListError(error)`.
3. **Domain & Repository Layer (Tầng Nghiệp vụ & Kho dữ liệu):**
   - **Models / Entities:** `UserModel`, `GatewayModel`, `SensorModel`. Các model có phương thức chuyển đổi `fromJson` khớp chính xác với JSON contract của Backend.
   - **`AuthRepository`:** Định nghĩa các hợp đồng nghiệp vụ xác thực (đăng nhập, đăng xuất, lấy session hiện tại).
   - **`GatewayRepository`:** Chịu trách nhiệm lấy danh sách Gateway và danh sách Sensor; chuyển đổi lỗi mạng (Network/HTTP status) thành Domain Exceptions dễ hiểu (`UnauthorizedException`, `ResourceNotFoundException`, `ServerException`).
4. **Data Source Layer (Tầng Nguồn dữ liệu & Tương thích Nền tảng):**
   - **`SupabaseAuthDataSource`:** Đóng gói tương tác với `Supabase.instance.client.auth`. Trên Web, SDK tự động sử dụng trình lưu trữ phiên tích hợp (`localStorage` của trình duyệt) mà không cần cấu hình native phức tạp.
   - **`BackendApiClient`:** Đóng gói HTTP client (sử dụng thư viện `dio` hoặc `http`). Trên Web, các client này tự động chuyển đổi sang các cuộc gọi `fetch`/`XMLHttpRequest` tương thích trình duyệt. Cấu hình tự động chèn header:
     ```http
     Authorization: Bearer <current_supabase_access_token>
     Content-Type: application/json
     Accept: application/json
     ```
   - **`AppStorageService`:** Trừu tượng hóa cơ chế lưu trữ cục bộ. Trên Web, sử dụng Web Storage (`localStorage` / `shared_preferences`); khi chuyển sang Android/iOS, có thể cắm triển khai bằng Keystore / Keychain thông qua cùng một giao diện interface mà không làm thay đổi tầng Domain/Logic.

### 6.2. Cấu trúc thư mục mã nguồn đề xuất (`lib/`)

```text
lib/
├── main.dart                   # Điểm khởi chạy ứng dụng, nạp biến môi trường, cấu hình DI/BlocProviders
├── core/
│   ├── constants/              # Các hằng số API endpoints, storage keys
│   ├── errors/                 # Định nghĩa các Exception và Failure chuẩn
│   ├── network/                # HTTP Client, Auth Interceptor, Network Info
│   ├── storage/                # Trừu tượng hóa LocalStorage / SecureStorage
│   ├── theme/                  # Định nghĩa màu sắc, typography, responsive breakpoints
│   └── utils/                  # Format ngày tháng (RFC3339), validator
├── data/
│   ├── datasources/            # Tầng gọi API / SDK thô
│   │   ├── auth_remote_datasource.dart
│   │   └── backend_api_client.dart
│   ├── models/                 # DTOs và mapper parse JSON
│   │   ├── gateway_model.dart
│   │   └── sensor_model.dart
│   └── repositories/           # Triển khai Repository kết nối Data Source
│       ├── auth_repository_impl.dart
│       └── gateway_repository_impl.dart
├── logic/                      # Tầng State Management (BLoC / Cubit)
│   ├── auth/
│   │   ├── auth_bloc.dart
│   │   ├── auth_event.dart
│   │   └── auth_state.dart
│   ├── gateway_list/
│   │   └── gateway_list_cubit.dart
│   └── sensor_list/
│       └── sensor_list_cubit.dart
└── presentation/               # Tầng giao diện người dùng
    ├── screens/
    │   ├── auth/
    │   │   └── login_screen.dart
    │   ├── gateway/
    │   │   ├── gateway_list_screen.dart
    │   │   └── gateway_detail_screen.dart
    │   └── sensor/
    │       └── sensor_list_screen.dart
    └── widgets/
        ├── common_button.dart
        ├── error_banner.dart
        ├── loading_overlay.dart
        ├── responsive_layout.dart
        └── role_badge.dart
```

---

## 7. Tóm tắt kế hoạch kiểm thử và nghiệm thu Client

1. **Kiểm thử trên trình duyệt (Web Testing):**
   - Thực thi ứng dụng trên Chrome/Edge/Firefox, kiểm tra tab Console và Network trong Developer Tools để xác nhận không phát sinh lỗi script, lỗi tải WASM/CanvasKit hoặc chặn CORS.
2. **Kiểm thử xác thực:** Đăng nhập thành công với tài khoản được Admin cấp trước; thử nghiệm đăng nhập sai mật khẩu; kiểm tra token được lưu trữ an toàn trong Web Storage và tự động đính kèm Bearer token vào các request `/v1/*`.
3. **Kiểm thử đọc dữ liệu phân quyền:**
   - Tài khoản có quyền `owner`/`operator`/`viewer` trên Gateway A: Hiển thị đúng Gateway A và danh sách Sensor của Gateway A.
   - Tài khoản không có quyền trên Gateway B: Không nhìn thấy Gateway B trong danh sách; nếu cố tình request ID của Gateway B thì nhận lỗi `404 Not Found` và ứng dụng hiển thị thông báo lỗi thân thiện.
   - Tài khoản mới chưa gán Gateway: Hiển thị giao diện rỗng một cách rõ ràng, không bị crash hoặc treo màn hình.
4. **Kiểm thử xử lý mất phiên (Token Expiry):** Khi access token hết hạn và không thể refresh, client phải hủy phiên lưu trữ trong trình duyệt và chủ động điều hướng người dùng về màn hình đăng nhập.
5. **Kiểm tra tính tương thích Responsive:** Kiểm tra giao diện co giãn hiển thị tốt ở cả kích thước màn hình máy tính để bàn (Desktop viewport) và kích thước mô phỏng màn hình di động (Mobile viewport).
