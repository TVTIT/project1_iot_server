# Tài liệu Client — 06: Khởi tạo Dự án Flutter Web và Quản lý Môi trường (Project Setup & Environment)

**Cập nhật:** 2026-10-03  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md` (Mục 2, 4, 15, 17), `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md` đến `06_UPCOMING_FEATURES_ROADMAP.md`, `config/nginx/nginx.conf.template`.

---

## 1. Tổng quan và Mục tiêu tài liệu

Tài liệu này đóng vai trò là cẩm nang kỹ thuật thực hành (Implementation & Operations Manual) dành cho việc thiết lập môi trường phát triển, khởi tạo dự án và cấu hình biến môi trường cho ứng dụng **Flutter Web Dashboard** trong hệ thống IoT Gateway–Server.

- **Mục tiêu kỹ thuật:**
  - Hướng dẫn chuẩn hóa môi trường phát triển máy trạm (Workstation), đảm bảo tính nhất quán giữa các phiên bản Flutter SDK, Dart SDK và trình duyệt Web (Google Chrome / Edge).
  - Thiết lập dự án Flutter hỗ trợ nền tảng Web (`--platforms web`), tổ chức mã nguồn theo kiến trúc Clean / Feature-First độc lập nền tảng để dễ dàng đóng gói sang Android ở giai đoạn sau.
  - Khai báo danh mục thư viện phụ thuộc (`pubspec.yaml`) chính thức tương thích hoàn toàn với Flutter Web: `supabase_flutter`, `dio`, `go_router`, `flutter_bloc`, `fl_chart`, `web_socket_channel`, v.v.
  - Xây dựng cơ chế quản lý biến môi trường bảo mật (`.env`), giải quyết bài toán Same-Origin Policy, cấu hình CORS trên Nginx/Backend, và phân định địa chỉ mạng giữa Web Local Development (`http://localhost:80`) và Web Production.
  - Chuẩn hóa quy trình chạy debug, kiểm thử trên trình duyệt và đóng gói bản phát hành tĩnh (Release Web Build: HTML, JS, WASM / CanvasKit) để triển khai lên Nginx.

---

## 2. Yêu cầu môi trường phát triển (Prerequisites)

Để xây dựng và biên dịch ứng dụng Flutter Web ổn định, máy trạm phát triển cần đáp ứng các tiêu chuẩn công cụ sau:

### 2.1. Bộ công cụ phát triển phần mềm (SDKs)

| Thành phần | Phiên bản khuyến nghị | Yêu cầu tối thiểu | Ghi chú kỹ thuật |
|---|---|---|---|
| **Flutter SDK** | `3.22.x` hoặc mới hơn | `3.22.0` | Kênh `stable`. Đảm bảo hỗ trợ đầy đủ Web WASM compilation và CanvasKit renderer. |
| **Dart SDK** | `3.4.x` hoặc mới hơn | `3.4.0` | Đi kèm sẵn trong bộ Flutter SDK. Hỗ trợ đầy đủ Null Safety và Web interop mới. |
| **Google Chrome / Edge**| Phiên bản mới nhất | Phiên bản 110+ | Trình duyệt mục tiêu chính để chạy debug và kiểm tra hiệu năng DevTools. |
| **Trình soạn thảo mã** | VS Code hoặc Android Studio | Bản mới nhất | VS Code kèm extension `Flutter`, `Dart`, `bloc`, `DotENV`. |

Kiểm tra trạng thái môi trường bằng lệnh:
```bash
flutter doctor -v
```
Đảm bảo mục **Chrome - develop for the web** đạt trạng thái sẵn sàng (dấu kiểm xanh). Nếu chưa kích hoạt Web trên Flutter, chạy lệnh:
```bash
flutter config --enable-web
```

---

## 3. Hướng dẫn khởi tạo dự án Flutter Web

### 3.1. Lệnh khởi tạo dự án

Thực hiện lệnh tạo dự án trên terminal tại thư mục mong muốn:

```bash
flutter create \
  --org com.iotplatform.client \
  --platforms web \
  --project-name iot_client_app \
  iot_client_app
```

**Giải thích các cờ (flags):**
- `--org com.iotplatform.client`: Định danh tổ chức dự án.
- `--platforms web`: Khởi tạo thư mục mã nguồn nền tảng `web/` (chứa `index.html`, `manifest.json`, icon web). Khi cần hỗ trợ thêm Android ở giai đoạn sau, chỉ cần chạy bổ sung `flutter create --platforms android .`.

### 3.2. Cấu trúc thư mục mã nguồn chuẩn hóa (Feature-First / Clean)

```text
iot_client_app/
├── web/                            # Tệp cấu hình Web native (index.html, manifest.json)
├── assets/                         # Thư mục tài nguyên cục bộ
│   ├── .env.example                # Bản mẫu biến môi trường (commit lên Git)
│   └── .env                        # Biến môi trường thực tế (ĐÃ BỎ QUA trong .gitignore)
├── lib/
│   ├── core/                       # Thành phần dùng chung toàn ứng dụng
│   │   ├── config/                 # Đọc cấu hình môi trường (.env, AppConfig)
│   │   ├── constants/              # Hằng số giao diện, endpoint, storage keys
│   │   ├── network/                # Cấu hình Dio, Interceptors, WebSocket Client
│   │   ├── router/                 # Cấu hình GoRouter, AppRoutes, Redirect Guard
│   │   ├── theme/                  # Cấu hình Material 3 Theme Web
│   │   └── utils/                  # Định dạng ngày giờ UTC/Local, sinh UUIDv4
│   ├── features/                   # Phân rã theo chức năng nghiệp vụ (Feature-First)
│   │   ├── auth/                   # Xác thực Supabase Auth, quản lý phiên Web
│   │   │   ├── bloc/               # AuthBloc/Cubit, AuthState
│   │   │   ├── data/               # AuthRepository, SupabaseAuthService
│   │   │   └── presentation/       # LoginScreen, AuthGate
│   │   ├── gateway/                # Danh mục trạm & chi tiết cảm biến
│   │   │   ├── cubit/              # GatewayListCubit, SensorListCubit
│   │   │   ├── data/               # GatewayRepository, GatewayRemoteDataSource
│   │   │   ├── models/             # GatewayItemDto, SensorItemDto
│   │   │   └── presentation/       # GatewayListScreen, GatewayDetailScreen, Widgets
│   │   ├── telemetry/              # Biểu đồ lịch sử & WebSocket streaming (Giai đoạn 3)
│   │   ├── media/                  # Xem ảnh hiện trường có chữ ký (Giai đoạn 3)
│   │   └── digital_twin/           # Bảng điều khiển trạm & Twin State (Giai đoạn 4)
│   ├── app.dart                    # Root MaterialApp.router, MultiBlocProvider
│   └── main.dart                   # Điểm khởi chạy ứng dụng (Entry Point, Web URL strategy)
├── pubspec.yaml                    # Quản lý thư viện phụ thuộc và assets
└── .gitignore                      # Danh sách loại trừ Git
```

---

## 4. Danh sách thư viện phụ thuộc chính thức (`pubspec.yaml`)

### 4.1. Bảng phân tích vai trò các gói thư viện trên Flutter Web

| Tên thư viện | Phiên bản khuyến nghị | Vai trò trong hệ thống Client Web | Khả năng tương thích Web |
|---|---|---|---|
| **`supabase_flutter`** | `^2.5.6` | Xác thực Supabase Auth & quản lý phiên. | Tương thích Web 100%. Tự động lưu session vào `window.localStorage`. |
| **`go_router`** | `^14.2.0` | Điều hướng chuẩn Web (URL, Back/Forward, Deep Link). | Thư viện điều hướng số 1 cho Flutter Web, đồng bộ thanh địa chỉ trình duyệt. |
| **`dio`** | `^5.5.0+1` | HTTP Client gọi Go Backend REST API. | Hoạt động trên Web qua `fetch` API / XHR, hỗ trợ gắn Bearer JWT và `X-Request-ID`. |
| **`flutter_bloc`** | `^8.1.6` | Quản lý trạng thái ứng dụng (State Management). | Tách biệt hoàn toàn giao diện Web và logic nghiệp vụ. |
| **`equatable`** | `^2.0.5` | So sánh giá trị đối tượng State. | Giúp BLoC nhận diện sự thay đổi trạng thái, tránh re-render thừa. |
| **`fl_chart`** | `^0.68.0` | Vẽ biểu đồ chuỗi thời gian trên Web. | Hiển thị biểu đồ vector mượt mà trên CanvasKit và WebGL. |
| **`web_socket_channel`** | `^3.0.0` | Kết nối WebSocket thời gian thực (`wss://`). | Tự động sử dụng `HtmlWebSocketChannel` chuẩn của trình duyệt. |
| **`cached_network_image`** | `^3.3.1` | Tải và lưu đệm hình ảnh từ Supabase Storage. | Hỗ trợ hiển thị ảnh với bộ đệm trên trình duyệt. |
| **`flutter_dotenv`** | `^5.1.0` | Quản lý cấu hình biến môi trường. | Đọc các thông số cấu hình từ asset `assets/.env`. |
| **`intl`** | `^0.19.0` | Định dạng thời gian và số học. | Chuyển đổi chuẩn xác thời gian UTC ISO-8601 sang giờ địa phương. |
| **`uuid`** | `^4.4.2` | Sinh mã định danh ngẫu nhiên UUIDv4. | Khởi tạo mã `X-Request-ID` cho từng HTTP request. |

### 4.2. Tệp cấu hình `pubspec.yaml` hoàn chỉnh

```yaml
name: iot_client_app
description: "Web Dashboard giam sat va van hanh he thong IoT Gateway-Server (Do an 1)"
publish_to: "none"
version: 1.0.0+1

environment:
  sdk: ">=3.4.0 <4.0.0"
  flutter: ">=3.22.0"

dependencies:
  flutter:
    sdk: flutter
  flutter_web_plugins:
    sdk: flutter

  # Authentication & Web Session
  supabase_flutter: ^2.5.6

  # Navigation & Web URL Routing
  go_router: ^14.2.0

  # Networking & HTTP Client
  dio: ^5.5.0+1
  web_socket_channel: ^3.0.0

  # State Management & Architecture
  flutter_bloc: ^8.1.6
  equatable: ^2.0.5

  # Charts & Visualization
  fl_chart: ^0.68.0

  # Media & Image Caching
  cached_network_image: ^3.3.1

  # Environment & Utilities
  flutter_dotenv: ^5.1.0
  intl: ^0.19.0
  uuid: ^4.4.2

  # UI Icons & Components
  cupertino_icons: ^1.0.8

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^4.0.0
  bloc_test: ^9.1.7
  mocktail: ^1.0.4

flutter:
  uses-material-design: true

  # Khai bao tap tin tai nguyen cau hinh moi truong
  assets:
    - assets/.env
```

Sau khi cấu hình, thực thi lệnh tải gói:
```bash
flutter pub get
```

---

## 5. Quản lý cấu hình biến môi trường (`.env`) trên Web

### 5.1. Danh mục biến môi trường bắt buộc

| Tên biến | Kiểu dữ liệu | Ý nghĩa kỹ thuật | Ví dụ Web Local Dev | Ví dụ Web Production |
|---|---|---|---|---|
| `BACKEND_API_BASE_URL` | String (URL) | Địa chỉ gốc của Go Backend REST API. | `http://localhost/v1` | `https://api.yourdomain.com/v1` (hoặc `/v1`) |
| `BACKEND_WS_URL` | String (URI) | Địa chỉ kết nối WebSocket tới Go Backend. | `ws://localhost/v1/ws` | `wss://api.yourdomain.com/v1/ws` |
| `SUPABASE_URL` | String (URL) | Địa chỉ gốc tiếp nhận yêu cầu Supabase Auth. | `http://localhost` | `https://supabase.yourdomain.com` |
| `SUPABASE_ANON_KEY` | String (JWT) | Khóa Public Anonymous của dự án Supabase. | `eyJhbGciOiJIUzI1Ni...` | `eyJhbGciOiJIUzI1Ni...` |
| `ENVIRONMENT` | Enum String | Định danh môi trường thực thi. | `development` | `production` |

> **LƯU Ý QUAN TRỌNG VỀ ĐỊA CHỈ MẠNG TRÊN WEB:**  
> - **KHÔNG DÙNG `10.0.2.2`:** Đây là IP nội bộ của Android Emulator. Trình duyệt Web chạy trực tiếp trên máy host nên sử dụng `localhost` (qua Nginx cổng 80).
> - **QUY TẮC BẢO MẬT:** Tuyệt đối không nhúng `service_role` key vào Client Web (mã nguồn JavaScript/Dart trên trình duyệt có thể bị dịch ngược dễ dàng).

### 5.2. Mẫu tệp cấu hình `assets/.env.example`

```ini
# ==============================================================================
# IoT Gateway-Server Platform — Client Environment Configuration Template
# File: assets/.env.example (Sao chep thanh assets/.env truoc khi chay)
# ==============================================================================

# 1. Go Backend REST API Base URL (thong qua Nginx Reverse Proxy)
# Web Local Development (Nginx lang nghe cong 80 tren localhost):
BACKEND_API_BASE_URL=http://localhost/v1
# Web Production (HTTPS domain hoac relative path):
# BACKEND_API_BASE_URL=https://api.yourdomain.com/v1

# 2. Go Backend WebSocket URL
# Web Local Development:
BACKEND_WS_URL=ws://localhost/v1/ws
# Web Production (bat buoc wss:// khi web chay https://):
# BACKEND_WS_URL=wss://api.yourdomain.com/v1/ws

# 3. Supabase API Gateway URL (Auth & Storage thong qua Nginx)
# Web Local Development:
SUPABASE_URL=http://localhost
# Web Production:
# SUPABASE_URL=https://supabase.yourdomain.com

# 4. Supabase Public Anonymous Key (TUYET DOI KHONG DUNG service_role key)
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.placeholder_anon_key_for_client_app

# 5. Environment Identifier (development | staging | production)
ENVIRONMENT=development
```

### 5.3. Lớp nạp cấu hình an toàn trong Dart (`AppConfig`)

```dart
import 'package:flutter_dotenv/flutter_dotenv.dart';

class AppConfig {
  static late final String backendApiBaseUrl;
  static late final String backendWsUrl;
  static late final String supabaseUrl;
  static late final String supabaseAnonKey;
  static late final String environment;

  static Future<void> initialize() async {
    await dotenv.load(fileName: 'assets/.env');

    backendApiBaseUrl = _getRequired('BACKEND_API_BASE_URL');
    backendWsUrl = _getRequired('BACKEND_WS_URL');
    supabaseUrl = _getRequired('SUPABASE_URL');
    supabaseAnonKey = _getRequired('SUPABASE_ANON_KEY');
    environment = dotenv.get('ENVIRONMENT', fallback: 'development');
  }

  static String _getRequired(String key) {
    final value = dotenv.get(key, fallback: '');
    if (value.isEmpty) {
      throw StateError('Thieu bien moi truong bat buoc tren Client: $key');
    }
    return value;
  }

  static bool get isProduction => environment == 'production';
}
```

---

## 6. Lưu ý về CORS và Định tuyến Nginx cho Web

Khi phát triển Web cục bộ, ứng dụng Flutter Web chạy tại `http://localhost:<port>` (ví dụ port 3000 hoặc random), trong khi Nginx và Go Backend chạy tại `http://localhost:80`. Do đó:
1. **Trình duyệt sẽ kích hoạt kiểm tra CORS:** Gửi preflight request `OPTIONS` kèm các header `Authorization`, `X-Request-ID`.
2. **Cấu hình Nginx reverse proxy:** Đảm bảo cấu hình Nginx phản hồi đúng các header CORS:
   ```nginx
   add_header 'Access-Control-Allow-Origin' '*' always;
   add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, PATCH, DELETE, OPTIONS' always;
   add_header 'Access-Control-Allow-Headers' 'Authorization, Content-Type, Accept, X-Request-ID' always;
   add_header 'Access-Control-Expose-Headers' 'X-Request-ID, Cache-Control' always;
   ```
3. **Khi Triển khai Production:** Bản build tĩnh của Flutter Web (HTML/JS/WASM) được đặt ngay trong thư mục tĩnh của Nginx (hoặc cùng domain), triệt tiêu hoàn toàn vấn đề CORS nhờ cùng chung Origin.

---

## 7. Hướng dẫn Chạy Debug và Đóng gói Phát hành (Run & Build)

### 7.1. Chạy Debug trên Trình duyệt Web

Thực thi lệnh chạy trên Google Chrome với một cổng cố định (tiện lợi cho việc cấu hình CORS):

```bash
# Chay tren Google Chrome voi port co dinh 3000
flutter run -d chrome --web-port=3000

# Hoac chay mac dinh (Flutter tu chon port ngau nhien)
flutter run -d chrome
```

**Kỹ thuật gỡ lỗi trên Web:**
- Mở **Chrome DevTools (`F12`)**:
  - Tab **Console**: Xem log ứng dụng Flutter và lỗi Dart runtime.
  - Tab **Network**: Quan sát các request `GET /v1/gateways`, kiểm tra header `Authorization: Bearer ...` và header truy vết `X-Request-ID`.
  - Tab **Application > Local Storage**: Kiểm tra session Supabase được lưu tự động.

### 7.2. Đóng gói Bản phát hành Tĩnh (Build Release Web)

Flutter hỗ trợ 2 bộ render chính cho Web:
1. **CanvasKit (Khuyến nghị cho Dashboard/Charts):** Hiển thị đồ họa và biểu đồ mượt mà, nhất quán từng pixel giữa các trình duyệt.
2. **WASM (WebAssembly):** Tối ưu tốc độ tải và hiệu năng tính toán cao.

Lệnh đóng gói bản release:

```bash
# Biên dịch phiên bản Release tối ưu CanvasKit
flutter build web --release --web-renderer canvaskit

# Hoặc biên dịch tự động tối ưu (Auto-select)
flutter build web --release
```

**Kết quả đầu ra:** Thư mục `build/web/` chứa toàn bộ tệp tĩnh:
- `index.html`
- `main.dart.js`
- `flutter.js`
- `assets/` (chứa `.env` và fonts)
- `canvaskit/`

Chỉ cần sao chép toàn bộ thư mục `build/web/` vào thư mục phục vụ web tĩnh của Nginx (ví dụ `/var/www/html/` hoặc container Nginx) là hệ thống Web Dashboard có thể vận hành ổn định.

---

## 8. Bảng kiểm tra Hoàn tất Thiết lập (Setup Verification Checklist)

- [ ] `flutter doctor -v` đạt chuẩn, Chrome đã sẵn sàng.
- [ ] Dự án Flutter được tạo với nền tảng Web (`--platforms web`).
- [ ] Tệp `pubspec.yaml` đã nạp đầy đủ các gói phụ thuộc tương thích Web (`flutter pub get` thành công).
- [ ] Tệp `assets/.env` đã được sao chép từ `.env.example` và cấu hình trỏ về `http://localhost/v1` (qua Nginx).
- [ ] Tệp `.env` đã được xác nhận nằm trong danh sách loại trừ `.gitignore`.
- [ ] Chạy thử nghiệm thành công `flutter run -d chrome --web-port=3000`.
- [ ] Biên dịch thử nghiệm bản phát hành tĩnh `flutter build web --release` thành công, thư mục `build/web/` được sinh ra hoàn chỉnh.
