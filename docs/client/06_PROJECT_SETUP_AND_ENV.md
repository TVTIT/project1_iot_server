# Tài liệu Client — 06: Khởi tạo Dự án và Quản lý Môi trường Client (Project Setup & Environment)

**Cập nhật:** 2026-10-05
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)
**Tài liệu tham chiếu:**
- `AGENTS.md` (Mục 1, 2, 4, 5.3, 5.5, 6.5, 10, 15, 17, 18)
- `config/nginx/nginx.conf.template` (Định tuyến Nginx thực tế)
- `config/envoy/lds.template.yaml` (Cấu hình Envoy API Gateway)
- `.env.example` (Biến môi trường mẫu phía hạ tầng)
- `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` (Kiến trúc tổng quan)
- `docs/client/02_AUTHENTICATION_AND_SESSION.md` (Xác thực và Phiên làm việc)
- `docs/client/03_USER_GATEWAY_AUTHORIZATION.md` (Phân quyền người dùng và trạm)
- `docs/client/04_REST_API_CLIENT_CONTRACT.md` (Hợp đồng REST API đọc nghiệp vụ)
- `docs/client/05_UPCOMING_FEATURES_ROADMAP.md` (Lộ trình tính năng tương lai)
- `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` (Hợp đồng Platform Admin API)
- `docs/client/08_API_INTEGRATION_AND_STATE_HANDLING.md` (Tích hợp API và Quản lý trạng thái)

---

## 1. Tổng quan và Mục tiêu tài liệu

Tài liệu này đóng vai trò là cẩm nang kỹ thuật thực hành (Implementation & Operations Manual) dành cho việc thiết lập môi trường phát triển, khởi tạo dự án và quản trị biến môi trường cho ứng dụng Client trong hệ thống IoT Gateway–Server.

### 1.1. Định vị nền tảng và Ranh giới kiến trúc
- **Nền tảng mục tiêu MVP:** Theo quy định tại `AGENTS.md` (Mục 1, 2 & 17), dự án do một sinh viên thực hiện và ưu tiên giải pháp nhỏ gọn, hoàn chỉnh và có khả năng giải trình cao nhất. Ứng dụng Client ưu tiên một nền tảng duy nhất là **Flutter di động (ưu tiên Android)** nhằm tối giản hóa sự phức tạp về hiển thị và mạng. Nền tảng Web được định vị phục vụ phát triển, kiểm thử giao diện và dashboard tra cứu nội bộ; đây không phải là sự thay đổi kiến trúc sang Web-first.
- **Tính chất mã nguồn minh họa:** Repository hiện tại **chưa chứa thư mục mã nguồn Flutter chính thức**. Toàn bộ cấu trúc thư mục, mô hình kiến trúc (Clean Architecture / BLoC) và danh mục thư viện trong tài liệu này mang tính chất **tùy chọn minh họa kỹ thuật (illustrative framework options)**. Dự án **không ép buộc** sinh viên phải áp dụng toàn bộ cấu trúc Clean Architecture đa tầng hay BLoC phức tạp; sinh viên có toàn quyền lựa chọn giải pháp quản lý trạng thái tinh gọn, dễ hiểu nhất (như `ChangeNotifier`, `setState` hoặc Cubit đơn giản) để hoàn thành MVP đúng tiến độ.

### 1.2. Mục tiêu kỹ thuật cốt lõi
1. **Phân định ranh giới Base URL và Ghép nối URI:** Làm rõ sự tách biệt tuyệt đối giữa Go Backend REST API base (`/v1`) và Supabase API Gateway root URL, hướng dẫn ghép nối URI tương đối chuẩn xác để tránh lỗi lặp tiền tố (`/v1/v1`) hoặc xóa mất path.
2. **Quy chuẩn an ninh biến môi trường Client:** Khẳng định nguyên tắc bất biến: Mọi cấu hình đóng gói trong Client (Android APK hay Web bundle) đều là **CÔNG KHAI (PUBLIC)**; Client tuyệt đối không phải là kho lưu trữ bí mật.
3. **Khai báo trung thực hạ tầng mạng và CORS:** Khảo sát trực tiếp từ `config/nginx/nginx.conf.template` và `src/internal/httpserver/router.go` để chỉ rõ hiện trạng định tuyến, không ngộ nhận tính năng lưu trữ web tĩnh hay cơ chế CORS OPTIONS đã sẵn sàng trên Go Backend. Loại bỏ hoàn toàn các đề xuất tắt bảo mật trình duyệt hay dùng tiện ích unblocker.
4. **Phân biệt Endpoint Health:** Phân định rõ giữa các endpoint kiểm tra sức khỏe được proxy qua Nginx (`/healthz`, `/v1/health`) và endpoint kiểm tra kết nối trực tiếp nội bộ trên Go backend (`/readyz`).
5. **Chẩn đoán lỗi cấu hình hệ thống:** Hướng dẫn phân tích chính xác các mã trạng thái HTTP chuẩn (`401`, `403`, `404`, `501`, `503`) dựa trên hành vi thực tế của mã nguồn backend.

---

## 2. Yêu cầu môi trường phát triển (Prerequisites)

Để xây dựng và biên dịch ứng dụng Flutter ổn định, máy trạm phát triển cần đáp ứng các tiêu chuẩn công cụ sau:

### 2.1. Bộ công cụ phát triển phần mềm (SDKs)

| Thành phần | Phiên bản khuyến nghị | Yêu cầu tối thiểu | Ghi chú kỹ thuật |
|---|---|---|---|
| **Flutter SDK** | `3.22.x` (stable) | `3.22.0` | Kênh `stable`. Đảm bảo hỗ trợ Dart 3 và hệ thống Gradle / Android SDK hiện đại. |
| **Dart SDK** | `3.4.x` | `3.4.0` | Đi kèm sẵn trong bộ Flutter SDK. Hỗ trợ đầy đủ Null Safety. |
| **Android Toolchain** | Android SDK 34+ | Android SDK 33 | Yêu cầu `cmdline-tools`, Android SDK Platform, Android SDK Build-Tools. |
| **Google Chrome** | Bản mới nhất | Phiên bản 110+ | Phục vụ chạy debug Web và kiểm tra DevTools trong quá trình phát triển. |
| **IDE / Editor** | VS Code / Android Studio | Bản mới nhất | Khuyến nghị VS Code kèm extension `Flutter`, `Dart`. |

Kiểm tra trạng thái môi trường bằng lệnh:
```bash
flutter doctor -v
```
Đảm bảo mục **Android toolchain** và **Chrome** đạt trạng thái sẵn sàng (dấu kiểm xanh).

---

## 3. Khởi tạo cấu trúc dự án Flutter (Illustrative Setup)

### 3.1. Lệnh khởi tạo dự án
Khởi tạo dự án với nền tảng Android (nền tảng cốt lõi theo `AGENTS.md`) và tùy chọn Web (phục vụ phát triển/thử nghiệm):

```bash
flutter create \
  --org com.iotplatform.client \
  --platforms android,web \
  --project-name iot_client_app \
  iot_client_app
```

### 3.2. Cấu trúc thư mục minh họa (Illustrative Clean / Feature-First Option)

Dưới đây là một phương án tổ chức mã nguồn tham khảo theo hướng module hóa nghiệp vụ. Sinh viên có thể thu gọn hoặc tinh giản cấu trúc này tùy theo mức độ phức tạp thực tế của ứng dụng:

```text
iot_client_app/
├── android/                        # Tệp cấu hình Android Native (Gradle, Manifest)
├── web/                            # Tệp cấu hình Web Native (index.html, manifest.json)
├── assets/                         # Thư mục tài nguyên cục bộ
│   ├── .env.example                # Bản mẫu biến môi trường (an toàn, commit lên Git)
│   └── .env                        # Biến môi trường thực tế (ĐÃ BỎ QUA trong .gitignore)
├── lib/
│   ├── core/                       # Thành phần dùng chung toàn ứng dụng
│   │   ├── config/                 # Nạp cấu hình môi trường (.env, AppConfig)
│   │   ├── constants/              # Hằng số giao diện, endpoint, storage keys
│   │   ├── network/                # Cấu hình HTTP Client, Interceptors (Bearer JWT, X-Request-ID)
│   │   ├── router/                 # Cấu hình điều hướng, AuthGuard, AdminGuard
│   │   └── theme/                  # Cấu hình Material 3 Theme
│   ├── features/                   # Phân rã theo chức năng nghiệp vụ
│   │   ├── auth/                   # Xác thực Supabase Auth, quản lý phiên
│   │   ├── gateway/                # Danh mục trạm & chi tiết cảm biến (Stage 2)
│   │   ├── admin/                  # Quản trị nền tảng (Platform Admin - xem doc 07)
│   │   ├── telemetry/              # Biểu đồ lịch sử & Streaming (Tùy chọn Stage 3 - xem doc 05)
│   │   └── digital_twin/           # Quản lý bản sao số & Lệnh (Tùy chọn Stage 4 - xem doc 05)
│   ├── app.dart                    # Widget gốc ứng dụng (MaterialApp)
│   └── main.dart                   # Điểm khởi chạy ứng dụng (Entry Point)
├── pubspec.yaml                    # Khai báo thư viện phụ thuộc và assets
└── .gitignore                      # Danh sách loại trừ Git
```

---

## 4. Danh mục thư viện phụ thuộc (`pubspec.yaml`)

### 4.1. Phân định vai trò và Giai đoạn hỗ trợ của các gói thư viện

Vì repository hiện chưa chứa dự án Flutter chính thức, danh mục gói dưới đây là **tùy chọn khung tham khảo (illustrative framework options)**:

1. **Nhóm thiết yếu cho Giai đoạn 2 (Stage 2 Core Options):** Phục vụ xác thực người dùng qua Supabase, gọi HTTP REST API và điều hướng cơ bản.
2. **Nhóm tùy chọn cho các giai đoạn sau (Stage 3+ Optional):** Biểu đồ chuỗi thời gian, WebSocket và lưu đệm hình ảnh. Các thư viện này chưa có endpoint backend sẵn sàng (các route tương ứng ở Backend hiện trả về `501 Not Implemented`).

| Tên thư viện | Phiên bản khuyến nghị | Vai trò trong ứng dụng Client | Tình trạng hỗ trợ Backend |
|---|---|---|---|
| **`supabase_flutter`** | `^2.5.6` | Xác thực Supabase Auth, quản lý phiên và token refresh. | Đã sẵn sàng (`/auth/v1/*` qua Envoy). |
| **`dio`** (hoặc `http`) | `^5.5.0+1` | HTTP Client gọi Go REST API, gắn Bearer JWT, `X-Request-ID`. | Đã sẵn sàng (`/v1/*` qua Nginx). |
| **`go_router`** | `^14.2.0` | Quản lý điều hướng, phân quyền route (AuthGuard, AdminGuard). | Tùy chọn điều hướng khai báo. |
| **`flutter_bloc`** / Cubit | `^8.1.6` | Quản lý trạng thái ứng dụng. *(Có thể thay bằng `ChangeNotifier`).* | Tùy chọn kiến trúc Client. |
| **`equatable`** | `^2.0.5` | So sánh giá trị đối tượng State, tránh re-render không cần thiết. | Tùy chọn hỗ trợ State. |
| **`flutter_dotenv`** | `^5.1.0` | Nạp biến môi trường từ tệp `assets/.env`. | Đã sẵn sàng. |
| **`intl`** | `^0.19.0` | Định dạng thời gian chuẩn UTC ISO-8601 sang giờ địa phương. | Đã sẵn sàng. |
| **`uuid`** | `^4.4.2` | Khuyến nghị sinh mã UUID cho `X-Request-ID` và `Idempotency-Key`. | Đã sẵn sàng. |
| **`fl_chart`** | `^0.68.0` | Vẽ biểu đồ chuỗi thời gian (Biểu đồ lịch sử / thời gian thực). | **Tùy chọn tương lai (Stage 3)**: Endpoint `/v1/telemetry/history` hiện trả về `501 Not Implemented`. |
| **`web_socket_channel`** | `^3.0.0` | Kết nối WebSocket nhận dữ liệu streaming. | **Tùy chọn tương lai (Stage 3)**: Endpoint `/v1/ws` hiện trả về `501 Not Implemented`. |
| **`cached_network_image`** | `^3.3.1` | Tải và lưu đệm hình ảnh hiện trường từ Supabase Storage. | **Tùy chọn tương lai (Stage 3)**: Luồng ký ảnh chưa được mở trên Go Backend. |

### 4.2. Tệp cấu hình mẫu `pubspec.yaml` (Minh họa)

```yaml
name: iot_client_app
description: "Client Dashboard giam sat va van hanh he thong IoT Gateway-Server (Do an 1)"
publish_to: "none"
version: 1.0.0+1

environment:
  sdk: ">=3.4.0 <4.0.0"
  flutter: ">=3.22.0"

dependencies:
  flutter:
    sdk: flutter

  # --- GIAI DOAN 2: XAC THUC & DIEU HUONG (STAGE 2 CORE) ---
  supabase_flutter: ^2.5.6
  go_router: ^14.2.0

  # --- GIAI DOAN 2: MANG & QUAN LY TRANG THAI ---
  dio: ^5.5.0+1
  # Luu y: Co the su dung ChangeNotifier hoac Cubit tuy nhu cau MVP
  flutter_bloc: ^8.1.6
  equatable: ^2.0.5

  # --- GIAI DOAN 2: CAU HINH & TIEN ICH ---
  flutter_dotenv: ^5.1.0
  intl: ^0.19.0
  uuid: ^4.4.2
  cupertino_icons: ^1.0.8

  # --- GIAI DOAN 3+: TUY CHON TUONG LAI (FUTURE OPTIONAL) ---
  # fl_chart: ^0.68.0
  # web_socket_channel: ^3.0.0
  # cached_network_image: ^3.3.1

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^4.0.0

flutter:
  uses-material-design: true
  assets:
    - assets/.env
```

---

## 5. Quản lý cấu hình biến môi trường (`.env`) trên Client

### 5.1. Quy tắc an ninh cốt lõi: Client Bundle là CÔNG KHAI (PUBLIC)

> ⚠️ **CẢNH BÁO AN NINH TUYỆT ĐỐI:**
> Trong kiến trúc ứng dụng Client (dù là Android APK hay Web JavaScript/WASM), mọi chuỗi ký tự được đóng gói trong mã nguồn hoặc tệp asset đều có thể bị dịch ngược và trích xuất dễ dàng. **Client KHÔNG BAO GIỜ là kho lưu trữ bí mật.**
>
> **DANH MỤC CẤM TUYỆT ĐỐI (NEVER BUNDLE IN CLIENT):**
> 1. `SERVICE_ROLE_KEY`: Khóa siêu quyền của Supabase (cho phép vượt mọi chính sách RLS).
> 2. `JWT_SECRET`: Khóa bí mật dùng để ký và xác thực JWT.
> 3. Mật khẩu cơ sở dữ liệu (`POSTGRES_PASSWORD`, `DATABASE_URL`, `AUTH_DB_PASSWORD`, `STORAGE_DB_PASSWORD`).
> 4. Mật khẩu MQTT Mosquitto (`MQTT_PASSWORD`, `MQTT_DYNSEC_MANAGER_PASSWORD`).
> 5. Khóa riêng tư TLS (`server.key`, `ca.key`).
> 6. Token đường hầm (`CLOUDFLARE_TUNNEL_TOKEN`, Rathole tokens).

### 5.2. Danh mục biến môi trường Client hợp lệ

Client chỉ được phép chứa các thông số công khai phục vụ định tuyến và khóa ẩn danh (Anon Key):

| Tên biến | Kiểu dữ liệu | Bản chất kỹ thuật | Ví dụ Môi trường Cục bộ (Local) | Ví dụ Môi trường Triển khai (Public/Domain) |
|---|---|---|---|---|
| `BACKEND_API_BASE_URL` | String (URL) | **Go REST API base**: Gốc truy cập các endpoint nghiệp vụ và admin của Go Backend. Phải chứa tiền tố `/v1`. | `http://localhost/v1` (Web) hoặc `http://10.0.2.2/v1` (Android Emulator) | `https://api.example.com/v1` |
| `SUPABASE_URL` | String (URL) | **Supabase API Gateway Root URL**: Gốc truy cập Gateway (Envoy/Kong). **TUYỆT ĐỐI KHÔNG chứa `/auth/v1`** vì SDK tự nối đường dẫn này. | `http://localhost` (Web) hoặc `http://10.0.2.2` (Android Emulator) | `https://supabase.example.com` (hoặc `https://api.example.com`) |
| `SUPABASE_ANON_KEY` | String | **Supabase Public Anon Key**: Khóa công khai định danh ứng dụng gọi GoTrue Auth. Không có quyền admin. | `<public_anon_key>` (mô tả an toàn) | Khóa public sinh ra từ hạ tầng |
| `BACKEND_WS_URL` | String (URI) | **Go WebSocket URI**: Địa chỉ kết nối WebSocket dữ liệu thời gian thực. *(Tùy chọn tương lai)*. | `ws://localhost/v1/ws` | `wss://api.example.com/v1/ws` |
| `ENVIRONMENT` | Enum String | Định danh môi trường thực thi (`development`, `staging`, `production`). | `development` | `production` |

### 5.3. Quy tắc Ghép nối URI Tương đối (URI Relative Join Rules)

Thống nhất với quy chuẩn trong `01_OVERVIEW_AND_ARCHITECTURE.md` và `04_REST_API_CLIENT_CONTRACT.md`, `BACKEND_API_BASE_URL` luôn được cấu hình chứa sẵn tiền tố `/v1` (ví dụ `http://localhost/v1` hoặc `https://api.example.com/v1`). Khi gọi API từ Client:

1. **Tránh lỗi lặp tiền tố (Double Prefix):** Không nối thêm `/v1/gateways` vào base URL đã có `/v1`, tránh tạo thành `/v1/v1/gateways` dẫn tới lỗi `404 Not Found`.
2. **Tránh lỗi xóa mất Path (Path Reset Bug trong Dart `Uri.resolve`):**
   - Nếu thực hiện: `Uri.parse("http://localhost/v1").resolve("/gateways")` (có dấu `/` ở đầu), Dart sẽ hiểu là điều hướng về gốc domain và trả về `http://localhost/gateways` (mất `/v1`).
   - **Cách ghép nối đúng:** Luôn dùng đường dẫn tương đối không có dấu gạch chéo đầu dòng:
     ```dart
     // Cách 1: Sử dụng Uri.resolve với đường dẫn tương đối (đảm bảo base URL có dấu gạch chéo cuối)
     final baseUri = Uri.parse(AppConfig.backendApiBaseUrl.endsWith('/')
         ? AppConfig.backendApiBaseUrl
         : '${AppConfig.backendApiBaseUrl}/');
     final gatewaysUri = baseUri.resolve('gateways'); // -> http://localhost/v1/gateways

     // Cách 2: Ghép chuỗi trực tiếp an toàn
     final cleanBase = AppConfig.backendApiBaseUrl.replaceAll(RegExp(r'/+$'), '');
     final url = '$cleanBase/gateways'; // -> http://localhost/v1/gateways
     ```
3. **Cấu hình Supabase SDK Root URL:** Khởi tạo `Supabase.initialize` bằng `SUPABASE_URL` gốc (ví dụ `http://localhost`), **không đưa** `http://localhost/auth/v1` vào tham số `url` vì SDK sẽ tự động gọi tới `http://localhost/auth/v1/auth/v1/...` gây lỗi 404.

### 5.4. Mẫu tệp cấu hình `assets/.env.example`

```ini
# ==============================================================================
# IoT Gateway-Server Platform — Client Environment Configuration Template
# Tệp mẫu an toàn (assets/.env.example) — Sao chép thành assets/.env trước khi chạy
# Tuyệt đối KHÔNG chứa mật khẩu, private key, token hạ tầng hoặc domain thực tế
# ==============================================================================

# 1. Go Backend REST API Base URL (Đầy đủ tiền tố /v1)
# Khi chạy Web cục bộ (qua Nginx cổng 80):
BACKEND_API_BASE_URL=http://localhost/v1
# Khi chạy Android Emulator cục bộ:
# BACKEND_API_BASE_URL=http://10.0.2.2/v1
# Khi chạy môi trường Production:
# BACKEND_API_BASE_URL=https://api.example.com/v1

# 2. Go Backend WebSocket URL (Tùy chọn Stage 3 - Hiện tại backend trả về 501)
BACKEND_WS_URL=ws://localhost/v1/ws
# BACKEND_WS_URL=wss://api.example.com/v1/ws

# 3. Supabase API Gateway Root URL (Dành cho Auth và Storage - KHÔNG kèm /auth/v1 hoặc /v1)
SUPABASE_URL=http://localhost
# SUPABASE_URL=http://10.0.2.2
# SUPABASE_URL=https://supabase.example.com

# 4. Supabase Public Anonymous Key (TUYỆT ĐỐI KHÔNG DÙNG service_role_key)
# Thay the bang public anon key hop le tu ha tang
SUPABASE_ANON_KEY=<public_anon_key>

# 5. Định danh môi trường (development | staging | production)
ENVIRONMENT=development
```

### 5.5. Lớp nạp cấu hình an toàn trong Dart (`AppConfig`)

```dart
import 'package:flutter_dotenv/flutter_dotenv.dart';

class AppConfig {
  static late final String backendApiBaseUrl;
  static late final String backendWsUrl;
  static late final String supabaseUrl;
  static late final String supabaseAnonKey;
  static late final String environment;

  /// Khởi tạo cấu hình và thẩm định các biến bắt buộc (fail-fast)
  static Future<void> initialize({String envPath = 'assets/.env'}) async {
    await dotenv.load(fileName: envPath);

    backendApiBaseUrl = _getRequired('BACKEND_API_BASE_URL');
    supabaseUrl = _getRequired('SUPABASE_URL');
    supabaseAnonKey = _getRequired('SUPABASE_ANON_KEY');

    // WebSocket URL là tùy chọn cho Stage 3 (fallback rỗng nếu chưa cấu hình)
    backendWsUrl = dotenv.get('BACKEND_WS_URL', fallback: '');

    environment = dotenv.get('ENVIRONMENT', fallback: 'development');

    // Thẩm tra tính hợp lệ cơ bản của URL
    _validateUrl(backendApiBaseUrl, 'BACKEND_API_BASE_URL');
    _validateUrl(supabaseUrl, 'SUPABASE_URL');
  }

  static String _getRequired(String key) {
    final value = dotenv.get(key, fallback: '').trim();
    if (value.isEmpty) {
      throw StateError('Thiếu biến môi trường bắt buộc trên Client: $key');
    }
    return value;
  }

  static void _validateUrl(String urlString, String key) {
    final uri = Uri.tryParse(urlString);
    if (uri == null || (!uri.isScheme('http') && !uri.isScheme('https'))) {
      throw FormatError('Biến môi trường $key không phải là HTTP/HTTPS URL hợp lệ: $urlString');
    }
  }

  static bool get isProduction => environment == 'production';
}
```

---

## 6. Ranh giới Mạng, Định tuyến Nginx và Thực trạng CORS

### 6.1. Khảo sát Định tuyến Nginx thực tế (`config/nginx/nginx.conf.template`)

Kiểm tra trực tiếp tệp mẫu Nginx trong repository cho thấy Nginx thực tế chỉ đóng vai trò reverse proxy cho các dịch vụ nội bộ:

```nginx
# Trích xuất từ config/nginx/nginx.conf.template:
location /auth/v1/ {
    proxy_pass $supabase_upstream; # Kong/Envoy:8000
    ...
}
location /storage/v1/ {
    proxy_pass $supabase_upstream; # Kong/Envoy:8000
    ...
}
location ~ ^/v1/(ws|telemetry/ws) {
    proxy_pass http://backend_api; # backend:8080
    ...
}
location /v1/ {
    proxy_pass http://backend_api; # backend:8080
    ...
}
location /healthz {
    return 200 'OK';
}
```

**Hai kết luận kiến trúc quan trọng từ cấu hình thực tế:**
1. **KHÔNG CÓ cấu hình phục vụ Web tĩnh:** Tệp Nginx hoàn toàn không có chỉ thị `root`, `try_files` hay phục vụ thư mục `build/web/`. Việc cho rằng Nginx đã sẵn sàng lưu trữ bản build tĩnh của Flutter Web là **không có căn cứ trong mã nguồn**.
2. **KHÔNG CÓ cấu hình CORS trên Nginx:** Nginx không cấu hình bất kỳ header `Access-Control-Allow-*` nào cho các location `/v1/`.

### 6.2. Phân định Endpoint Kiểm tra Sức khỏe (Proxied vs Backend-Direct)

Cần phân biệt rõ hai cấp độ kiểm tra sức khỏe hệ thống:

| Endpoint | Nơi phục vụ thực tế | Tính chất qua Nginx Reverse Proxy | Mục đích sử dụng |
|---|---|---|---|
| **`/healthz` (Nginx)** | Nginx Container | **Proxied / Phục vụ trực tiếp từ Nginx cổng 80**. Trả về HTTP 200 text `'OK'`. | Kiểm tra trạng thái sống của tiến trình Nginx Reverse Proxy (Liveness). Không phản ánh tình trạng kết nối CSDL của Go backend. |
| **`/v1/health`** | Go Backend (`cmd/server`) | **Proxied qua Nginx** (nằm trong khối `/v1/`). Trả về JSON `{"status":"running","service":"iot-backend","version":"v1"}`. | Client sử dụng để kiểm tra kết nối từ ứng dụng tới Go Backend qua Nginx. |
| **`/readyz`** | Go Backend (`cmd/server`) | **Backend-Direct ONLY (Cổng nội bộ 8080)**. Nginx **KHÔNG PROXY** endpoint này. Trả về `{"status":"ready"}` hoặc 503 khi CSDL ngắt kết nối. | Sử dụng nội bộ cho Docker Compose healthcheck / readiness probe giữa các container. Client gọi qua Nginx sẽ nhận lỗi 404 từ Nginx. |
| **`/healthz` (Backend)** | Go Backend (`cmd/server`) | **Backend-Direct ONLY (Cổng nội bộ 8080)**. Trả về text `'OK'`. | Sử dụng nội bộ cho container liveness probe trực tiếp trên backend. |

### 6.3. Khoảng trống CORS Preflight trên Go Backend (Declared Architecture Gap)

Khi phát triển hoặc chạy ứng dụng trên Trình duyệt Web (Flutter Web) từ một Origin khác (ví dụ `http://localhost:3000`), trình duyệt sẽ thực hiện kiểm tra Preflight bằng HTTP method `OPTIONS` trước khi gửi các request thực tế có header đặc biệt (`Authorization`, `Idempotency-Key`, `Content-Type`).

Khảo sát mã nguồn Go Backend (`src/internal/httpserver/router.go`):
- Go Backend sử dụng framework Gin với cờ:
  ```go
  router.HandleMethodNotAllowed = true
  router.NoMethod(func(c *gin.Context) {
      httpapi.WriteError(c, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
  })
  ```
- Backend **chưa tích hợp CORS Middleware** cho các route nghiệp vụ `/v1/*`.
- **Hệ quả kỹ thuật:** Khi trình duyệt gửi `OPTIONS /v1/gateways`, Go Backend sẽ trả về mã lỗi **`405 Method Not Allowed`** thay vì phản hồi `200/204` kèm các header CORS. Trình duyệt sẽ chặn đứng request ngay tại tầng network.
- *Lưu ý về Envoy:* Tệp cấu hình mẫu Envoy (`config/envoy/lds.template.yaml`) có khai báo bộ lọc CORS cho route Auth/Storage. Tuy nhiên, sự hiện diện của cấu hình mẫu này **không thay thế được việc phải kiểm chứng thực nghiệm (preflight qualification) với Origin, apikey và Authorization thực tế trong từng môi trường triển khai**.

**Quy tắc xử lý an toàn (Tuyệt đối KHÔNG bypass an ninh):**
- **Ưu tiên hàng đầu:** Kiểm thử và vận hành ứng dụng trên nền tảng **Android (Emulator hoặc thiết bị thật)** theo đúng định hướng `AGENTS.md`. Nền tảng di động native không bị ràng buộc bởi chính sách Same-Origin Policy của trình duyệt web và không kích hoạt preflight `OPTIONS`.
- **Khi chạy trên Web Dev:** Sử dụng **controlled same-origin development proxy** (ví dụ cấu hình proxy của máy chủ phát triển để đưa Web và API về cùng một Origin) hoặc thiết lập cấu hình CORS chính thức được phê duyệt kèm kiểm thử preflight đầy đủ. **TUYỆT ĐỐI KHÔNG sử dụng các tiện ích mở rộng tắt bảo mật trình duyệt (CORS unblockers) hay cờ vô hiệu hóa web security.**

### 6.4. Hợp đồng Header `Idempotency-Key` trên Admin API

Theo đặc tả trong `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md` và mã nguồn `src/internal/httpserver/admin_mqtt_credential_request.go`:
- **Chỉ áp dụng cho thao tác thay đổi dữ liệu (Mutation Only):** Header `Idempotency-Key` chỉ bắt buộc đối với các yêu cầu thay đổi trạng thái (`POST` khởi tạo/xoay vòng, `DELETE` thu hồi). Yêu cầu đọc trạng thái (`GET /v1/admin/gateways/{id}/mqtt-credential`) **hoàn toàn không yêu cầu** header này.
- **Quy chuẩn định dạng:** Trình phân tích cú pháp của backend sử dụng `uuid.Parse(v)` và kiểm tra `key != uuid.Nil`. Hợp đồng chấp nhận **bất kỳ chuỗi UUID hợp lệ và khác nil nào (valid non-nil UUID)**, không ép buộc bắt buộc phải là v4 (Client có thể sử dụng UUIDv4 làm thuật toán sinh mã khuyến nghị).
- Nếu trong tương lai các endpoint mutation này được gọi từ Trình duyệt Web, cấu hình CORS preflight sẽ bắt buộc phải đưa `Idempotency-Key` vào danh sách header được phép (`Access-Control-Allow-Headers`).

---

## 7. Hiện trạng các Route và Chẩn đoán Lỗi Cấu hình (HTTP Status Diagnostics)

Bảng phân loại dưới đây tổng hợp các mã trạng thái HTTP chuẩn của hệ thống, giúp nhà phát triển nhanh chóng khoanh vùng nguyên nhân do cấu hình môi trường hay do logic nghiệp vụ:

### 7.1. Bảng chẩn đoán mã lỗi HTTP

| Mã HTTP | Tên chuẩn | Nguyên nhân kỹ thuật tại Backend | Hướng xử lý và Khắc phục trên Client |
|---|---|---|---|
| **`401`** | `Unauthorized` | - Thiếu header `Authorization: Bearer <JWT>`.<br>- JWT hết hạn hoặc sai chữ ký (`JWT_SECRET`).<br>- Lệch cấu hình giữa GoTrue và Go Backend: `GOTRUE_JWT_ISSUER` khác với issuer cấu hình trên Go, hoặc sai `SUPABASE_JWT_AUDIENCE` (mặc định `authenticated`).<br>- **Đặc biệt:** Gọi các endpoint giữ chỗ (`/v1/ws`, `/v1/telemetry/history`) mà không có Bearer token cũng sẽ nhận `401` trước khi chạm tới `501`. | - Kiểm tra trạng thái phiên làm việc (`supabase.auth.currentSession`).<br>- Thực hiện refresh token nếu token đã hết hạn.<br>- Kiểm tra giá trị issuer trong `.env` backend và biến môi trường auth. |
| **`403`** | `Forbidden` | - **Từ chối quyền Quản trị Nền tảng (Platform Admin Privilege Denial):** Người dùng đã xác thực nhưng tài khoản không có bản ghi trong bảng `platform_admins` khi gọi các endpoint `/v1/admin/*`. | - Hiển thị màn hình từ chối quyền quản trị (Admin Access Denied).<br>- Không tự ý retry. Cần liên hệ quản trị viên hạ tầng cấp quyền platform admin qua công cụ quản trị tập trung. |
| **`404`** | `Not Found` | - Gọi vào route **hoàn toàn chưa được khai báo** trong `router.go` (`router.NoRoute`).<br>- **Endpoint đọc 1 trạm:** `GET /v1/gateways/{gateway_id}` hiện chưa được đăng ký trong router và trả về 404.<br>- **Bảo vệ quyền truy cập cảm biến:** Khi gọi `GET /v1/gateways/{gateway_id}/sensors` với trạm không tồn tại hoặc trạm mà người dùng **không được phân quyền** trong `user_gateways`, backend trả về `404 Not Found` (để tránh rò rỉ sự tồn tại và thông tin phân quyền của trạm).<br>- Các route tính năng chưa khai báo: `/v1/digital-twins/{entity_id}`, `/v1/gateways/{id}/media`.<br>- Cấu hình sai base URL (ví dụ thiếu `/v1`). | - Kiểm tra kỹ `BACKEND_API_BASE_URL` (phải kết thúc bằng `/v1`).<br>- Đối chiếu danh mục endpoint khả dụng tại `docs/client/04_REST_API_CLIENT_CONTRACT.md` và `docs/client/07_ADMIN_API_CLIENT_CONTRACT.md`.<br>- Đối với trạm chưa được phân quyền, hướng dẫn người dùng liên hệ quản trị viên gán quyền membership. |
| **`405`** | `Method Not Allowed` | - Sử dụng sai HTTP method đối với endpoint đã đăng ký.<br>- **Khoảng trống CORS Web:** Trình duyệt gửi preflight `OPTIONS` vào các route `/v1/*`. | - Kiểm tra đúng method theo tài liệu API.<br>- Khi chạy Web, xem mục 6.3 để khắc phục bằng same-origin proxy. |
| **`501`** | `Not Implemented` | - Route đã được đăng ký dạng giữ chỗ (authenticated stub) trong `src/internal/httpserver/router.go`: `GET /v1/telemetry/history`, `GET /v1/ws`, `GET /v1/digital-twins`. | - Đây là phản hồi **dự kiến** của hệ thống ở Stage 2. Không cố gắng kết nối WebSocket hoặc truy vấn lịch sử cho đến khi Backend phát hành Stage 3. |
| **`503`** | `Service Unavailable` | - Cơ sở dữ liệu ngắt kết nối (`/readyz` thất bại).<br>- **Cờ hạ tầng bị tắt:** Cờ `MQTT_CREDENTIAL_API_ENABLED=false` trên backend khiến các API `/v1/admin/gateways/{id}/mqtt-credential*` trả về `503` với mã lỗi `credential_runtime_disabled`. | - Xem chi tiết phân tích cờ tính năng tại mục 7.2 dưới đây. |

> ℹ️ **LƯU Ý VỀ PHẢN HỒI DANH MỤC TRẠM (`GET /v1/gateways`):**
> Khi người dùng hợp lệ nhưng chưa được phân quyền trạm nào trong bảng `user_gateways`, endpoint `GET /v1/gateways` trả về mã **`200 OK` với danh sách rỗng `{"items": []}`**, hoàn toàn **KHÔNG PHẢI** là mã lỗi từ chối `403`.

### 7.2. Lưu ý đặc thù về Cờ tính năng Hạ tầng `MQTT_CREDENTIAL_API_ENABLED`

Trong tệp cấu hình `.env.example` của hệ thống:
```ini
MQTT_CREDENTIAL_API_ENABLED=false
```
Mặc định cờ này mang giá trị `false`. Khi cờ này tắt, sau khi đã vượt qua bước xác thực token hợp lệ và kiểm tra quyền platform admin, toàn bộ các yêu cầu quản trị chứng thực MQTT (`GET/POST/DELETE /v1/admin/gateways/{id}/mqtt-credential*`) sẽ nhận phản hồi an toàn:
```json
{
  "error": {
    "code": "credential_runtime_disabled",
    "message": "credential request failed",
    "request_id": "0195e18c-9fc1-7a42-9064-69ea49e63bf2"
  }
}
```
với mã trạng thái HTTP là **`503 Service Unavailable`**.

> 🔒 **NGUYÊN TẮC QUẢN TRỊ NỀN TẢNG (NO CLIENT TOGGLE):**
> 1. **Thứ tự thẩm tra an ninh:** Lỗi 503 `credential_runtime_disabled` **chỉ xảy ra sau khi** yêu cầu đã vượt qua xác thực Bearer JWT (`auth.AuthenticationMiddleware`) và xác minh quyền quản trị nền tảng (`auth.PlatformAdminMiddleware`). Yêu cầu chưa xác thực sẽ nhận `401`, yêu cầu không phải admin sẽ nhận `403`.
> 2. **Client không có quyền bật/tắt (NO TOGGLE):** Cờ này là biến môi trường máy chủ điều khiển quyền truy cập socket/tệp Mosquitto Dynamic Security. Không có bất kỳ tham số, header hay thao tác nào trên Client có thể kích hoạt tính năng này.
> 3. **Yêu cầu Nghiệm thu Triển khai (Rollout Qualification):** Việc kích hoạt cờ `MQTT_CREDENTIAL_API_ENABLED=true` không đơn thuần là sửa cờ và khởi động lại container; việc này đòi hỏi phải áp dụng cấu hình Mosquitto Lifecycle riêng biệt và vượt qua quy trình kiểm thử nghiệm thu cô lập.
> 4. **Phân định thẩm quyền:** Người dùng giữ vai trò `platform_admin` ở tầng nghiệp vụ **hoàn toàn không mặc định có quyền truy cập hạ tầng máy chủ (SSH / container operator)**. Client phải xử lý mã lỗi `503 credential_runtime_disabled` một cách mềm dẻo bằng cách thông báo tính năng quản trị chứng thực đang tạm khóa từ phía hạ tầng.

---

## 8. Hướng dẫn Chạy Thử nghiệm và Đóng gói (Run & Verification)

### 8.1. Chạy Thử nghiệm trên Android Emulator (Nền tảng khuyến nghị)

1. Khởi động Android Emulator từ Android Studio.
2. Cấu hình tệp `assets/.env`:
   ```ini
   BACKEND_API_BASE_URL=http://10.0.2.2/v1
   SUPABASE_URL=http://10.0.2.2
   SUPABASE_ANON_KEY=<public_anon_key>
   ```
   *(Địa chỉ `10.0.2.2` là alias trỏ về `localhost` của máy trạm phát triển từ Android Emulator).*
3. Thực thi lệnh chạy:
   ```bash
   flutter run -d android
   ```

### 8.2. Chạy Thử nghiệm trên Trình duyệt Web (Phục vụ phát triển)

1. Cấu hình tệp `assets/.env`:
   ```ini
   BACKEND_API_BASE_URL=http://localhost/v1
   SUPABASE_URL=http://localhost
   SUPABASE_ANON_KEY=<public_anon_key>
   ```
2. Thực thi lệnh chạy với cổng cố định:
   ```bash
   flutter run -d chrome --web-port=3000
   ```
3. **Lưu ý về CORS:** Khi kiểm thử trên trình duyệt, cần sử dụng controlled same-origin development proxy hoặc cấu hình proxy nội bộ để đưa Client Web về cùng Origin với API; tuyệt đối không tắt kiểm tra an ninh của trình duyệt.

---

## 9. Bảng kiểm tra Hoàn tất Thiết lập (Setup Verification Checklist)

Trước khi tiến hành phát triển tính năng hoặc kiểm thử tích hợp, hãy kiểm tra danh mục sau:

- [ ] **Môi trường SDK:** `flutter doctor -v` đạt chuẩn, Android toolchain sẵn sàng.
- [ ] **Khởi tạo dự án:** Dự án Flutter được tạo và định hướng phát triển tối giản, có khả năng giải trình theo `AGENTS.md`.
- [ ] **Bảo mật Git:** Tệp `assets/.env` đã được thêm vào `.gitignore`, chỉ có `assets/.env.example` được đưa lên Git.
- [ ] **An toàn mã nguồn:** Đã kiểm tra lại toàn bộ mã nguồn Client, cam kết không chứa `service_role_key`, `JWT_SECRET`, database password, hay token hạ tầng.
- [ ] **Tính nhất quán của Base URL:** `BACKEND_API_BASE_URL` trỏ tới `/v1`, `SUPABASE_URL` trỏ tới gốc Gateway không kèm `/auth/v1` hay `/v1`. Ghép nối URI tương đối không gây lỗi lặp tiền tố `/v1/v1` hoặc mất path.
- [ ] **Thư viện phụ thuộc:** Đã phân định gói thư viện Stage 2 cốt lõi và không gọi các hàm WebSocket / Telemetry History khi Backend chưa hỗ trợ.
- [ ] **Kiểm tra kết nối công khai:** Gửi thử nghiệm `GET http://localhost/v1/health` (hoặc `http://10.0.2.2/v1/health`) nhận mã `200 OK` với `{"status":"running"}`.
- [ ] **Xác thực phiên:** Đăng nhập thành công qua Supabase Auth, nhận JWT hợp lệ và truyền đúng qua header `Authorization: Bearer <JWT>`.
- [ ] **Hiểu rõ mã trạng thái:** Đội ngũ phát triển nắm vững ý nghĩa các mã `401`, `403 (admin denial)`, `404 (unregistered route hoặc foreign gateway)`, `501 (stubs)`, và `503 (credential_runtime_disabled)` để hiển thị trạng thái giao diện chính xác.
