# Tài liệu Client — 02: Cơ chế Xác thực và Quản lý phiên làm việc trên Flutter Web (Authentication & Session Management)

**Cập nhật:** 2026-10-03  
**Dự án:** IoT Gateway–Server Platform (Đồ án 1 — ET3290)  
**Tài liệu tham chiếu:** `AGENTS.md`, `docs/client/01_OVERVIEW_AND_ARCHITECTURE.md`, `docs/backend/stage-2-task-2.2-authentication.md`, `docs/backend/stage-2-task-2.2A-centralized-accounts.md`.

---

## 1. Tổng quan cơ chế xác thực và Ranh giới hệ thống

Trong kiến trúc tổng thể của nền tảng IoT Gateway–Server, ứng dụng **Flutter Web** đóng vai trò là Dashboard giám sát và vận hành trung tâm. Cơ chế bảo mật và kiểm soát truy cập được phân định thành hai lớp ranh giới độc lập và rõ ràng:

1. **Ranh giới Định danh người dùng (User Identity Layer) — Supabase Auth (GoTrue):**
   - Đảm nhận toàn bộ chu trình sống của tài khoản người dùng: lưu trữ thông tin đăng nhập, kiểm tra mật khẩu đã hash (bcrypt), cấp phát cặp khóa `access_token` (JSON Web Token - JWT) và `refresh_token`, xử lý làm mới phiên tự động và thu hồi phiên làm việc.
   - Flutter Web tương tác với dịch vụ này thông qua thư viện chính thức `supabase_flutter`.

2. **Ranh giới Phân quyền nghiệp vụ (Business Authorization Layer) — Go Backend:**
   - Go Backend sở hữu toàn bộ các API nghiệp vụ (`/v1/*`), kết nối trực tiếp với PostgreSQL/TimescaleDB.
   - Go Backend **không** gọi ngược lại dịch vụ Supabase Auth trong từng request nhằm tối ưu hiệu năng và triệt tiêu độ trễ mạng thừa. Thay vào đó, backend tự xác minh chữ ký số của JWT tại chỗ (Local HS256 Verification) và bóc tách danh tính người dùng (`sub` claim dạng UUID).
   - Quyền truy cập vào dữ liệu trạm IoT (Gateway, Sensor, Lịch sử đo, WebSocket stream, Media) được Go Backend thẩm định độc lập dựa trên bảng quan hệ `user_gateways` và vai trò tương ứng (`owner`, `operator`, `viewer`).

```text
+-----------------------------------------------------------------------------------+
|                                 Flutter Web Client                                |
|                       (Trình duyệt Chrome / Edge / Firefox)                       |
|                             (supabase_flutter SDK)                                |
+------------------------+----------------------------------+-----------------------+
                         |                                  |
            1. Đăng nhập | /auth/v1/token                   | 3. Gọi Business API
               (Email/Pw)|                                  |    Header Authorization:
                         v                                  |    Bearer <access_token>
+----------------------------------------------------+      |
|           Nginx Reverse Proxy & Envoy              |      |
+------------------------+---------------------------+      |
                         |                                  |
                         v                                  v
+------------------------------------+     +----------------------------------------+
|           Supabase Auth            |     |               Go Backend               |
|              (GoTrue)              |     |           (Modular Monolith)           |
+-----------------+------------------+     +-------------------+--------------------+
                  |                                            |
                  | 2. Cấp Session                             | 4. Local JWT Verify
                  |    - access_token (HS256)                  |    & Check Membership
                  |    - refresh_token                         |    (bảng user_gateways)
                  v                                            v
+------------------------------------+     +----------------------------------------+
|    Web Storage (LocalStorage)      |     |       PostgreSQL / TimescaleDB         |
+------------------------------------+     +----------------------------------------+
```

---

## 2. Chính sách quản lý tài khoản tập trung (Centralized Account Policy — Task 2.2A)

Theo thiết kế kiến trúc và mô hình bảo mật chuẩn tại `AGENTS.md` (Mục 6.5) cùng kết quả kiểm chứng tại `docs/backend/stage-2-task-2.2A-centralized-accounts.md`:

### 2.1. Cấm đăng ký tài khoản tự do (Public Self-Signup Disabled)
- Hệ thống IoT Gateway–Server phục vụ các trạm giám sát chuyên dụng, không phải là ứng dụng mạng xã hội mở. Do đó, việc tự do đăng ký tài khoản bị vô hiệu hóa triệt để từ tầng máy chủ:
  ```dotenv
  # docker-compose.yml / .env của môi trường server
  GOTRUE_DISABLE_SIGNUP=true
  ```
- **Hành vi phía Server:** Nếu bất kỳ client nào (hoặc kẻ tấn công sử dụng Postman/curl) cố tình gửi request `POST /auth/v1/signup`, dịch vụ GoTrue (v2.196.0) sẽ lập tức từ chối và trả về:
  - **Mã HTTP:** `422 Unprocessable Entity`
  - **Payload phản hồi:**
    ```json
    {
      "code": 422,
      "error_code": "signup_disabled",
      "msg": "Signups not allowed for this instance"
    }
    ```
- **Quy tắc triển khai phía Flutter Client:**
  - Tuyệt đối **không** tạo màn hình Đăng ký (`SignUpScreen`), không tạo biểu mẫu nhập liệu đăng ký.
  - Trên màn hình `LoginScreen`, **không** đặt nút bấm hoặc liên kết "Đăng ký tài khoản" ("Create Account" / "Sign Up").
  - Giao diện đăng nhập cần có dòng ghi chú thông tin rõ ràng:
    > *"Hệ thống vận hành trạm quan trắc nội bộ. Tài khoản người dùng được cấp phát tập trung bởi Quản trị viên hệ thống (Platform Administrator)."*
  - **Lưu ý bảo mật:** Việc ẩn nút trên UI chỉ là tối ưu trải nghiệm (UX); cơ chế chặn tại GoTrue ở tầng server mới là chốt chặn bảo mật quyết định.

### 2.2. Quy trình cấp phát tài khoản thực tế
- Người dùng (kể cả chủ trạm `owner` hay kỹ thuật viên `operator`) không thể tự tạo tài khoản.
- Tài khoản được khởi tạo bởi **Platform Administrator** thông qua giao diện quản trị Supabase Studio (được bảo vệ trong mạng nội bộ/VPN) hoặc công cụ quản trị hạ tầng gọi trực tiếp tới Admin API (`POST /auth/v1/admin/users`) với `service_role` key.
- Sau khi tài khoản được tạo, quản trị viên sẽ gán quyền truy cập tương ứng trên các Gateway cụ thể thông qua bảng `user_gateways`. Người dùng nhận thông tin đăng nhập (Email và Mật khẩu tạm thời) từ quản trị viên qua kênh liên lạc bảo mật.

---

## 3. Luồng Đăng nhập (Sign In Flow)

### 3.1. Trình tự thực thi (Sequence Diagram)

```text
User (Web Browser)       Flutter Web UI          AuthService         Nginx / Envoy        GoTrue (Auth)
     |                         |                      |                    |                    |
     |--- Nhập Email/Pw ------>|                      |                    |                    |
     |--- Nhấn Đăng nhập ----->|                      |                    |                    |
     |    (hoặc gõ phím Enter) |-- signInWithPassword>|                    |                    |
     |                         |  (Hiển thị loading)  |-- POST /auth/v1/ ->|                    |
     |                         |                      |   token?grant=pwd  |-- Chuyển tiếp ---->|
     |                         |                      |                    |                    |-- Kiểm tra Hash
     |                         |                      |                    |<-- Trả về 200 OK --|-- Sinh JWT HS256
     |                         |                      |<-- HTTP 200 OK ----|    & Refresh Token
     |                         |                      |    (AuthResponse)  |                    |
     |                         |                      |                    |                    |
     |                         |                      |-- Lưu Session vào Web LocalStorage      |
     |                         |                      |-- Phát AuthChangeEvent.signedIn         |
     |                         |<-- Thành công -------|                    |                    |
     |                         |   (Ẩn loading)       |                    |                    |
     |<-- GoRouter điều hướng--|                      |                    |                    |
     |    tới /gateways        |                      |                    |                    |
```

### 3.2. Triển khai phương thức Đăng nhập bằng `supabase_flutter`

Client sử dụng phương thức chuẩn của SDK Supabase:

```dart
final AuthResponse res = await Supabase.instance.client.auth.signInWithPassword(
  email: email.trim(),
  password: password,
);
final Session? session = res.session;
final User? user = res.user;
```

Khi đăng nhập thành công, SDK nhận về một đối tượng `Session` bao gồm:
- `session.accessToken`: Chuỗi JWT định danh được ký bởi máy chủ.
- `session.refreshToken`: Chuỗi mã bảo mật ngẫu nhiên dùng để xin cấp access token mới khi token cũ hết hạn.
- `session.expiresIn`: Thời gian sống tính bằng giây (mặc định của hệ thống là `3600` giây = 1 giờ).
- `session.user`: Đối tượng chứa thông tin người dùng (`id` là UUID v4 duy nhất, `email`, `role: authenticated`).

### 3.3. Cấu trúc Token JWT theo quy chuẩn của Go Backend (Task 2.2)

Go Backend thẩm định nghiêm ngặt tính hợp lệ của `access_token`. Client cần nắm rõ cấu trúc token này để hiểu hành vi kiểm tra của máy chủ:

#### A. Header
```json
{
  "alg": "HS256",
  "typ": "JWT"
}
```
*Lưu ý:* Go Backend chỉ chấp nhận thuật toán `HS256`. Mọi token sử dụng thuật toán khác (`none`, `RS256`, `HS512`) đều bị từ chối với mã lỗi `401 Unauthorized`.

#### B. Payload (Claims bắt buộc)
```json
{
  "iss": "http://localhost/auth/v1",
  "sub": "b2f67232-a589-40ea-9b97-897db6746cf2",
  "aud": "authenticated",
  "role": "authenticated",
  "email": "operator.station1@example.com",
  "exp": 1790938800,
  "iat": 1790935200
}
```

Bảng đối chiếu kiểm tra của Go Backend Verifier:
| Claim | Yêu cầu của Go Backend | Ý nghĩa / Hành vi kiểm tra |
|---|---|---|
| `sub` | Bắt buộc (UUID v4 hợp lệ) | Định danh duy nhất của người dùng (`auth.users.id`). Trùng khớp với `profiles.id`. Không được chứa chuỗi UUID toàn số 0. |
| `role` | Bằng chính xác `authenticated` | Chứng minh đây là tài khoản người dùng thông thường đã đăng nhập. Go Backend từ chối nếu role là `anon` hoặc `service_role`. |
| `aud` | Bắt buộc chứa `authenticated` | GoTrue tự động đính kèm claim này cho người dùng hợp lệ. |
| `iss` | Khớp chính xác `SUPABASE_JWT_ISSUER` | Khớp với biến môi trường của hệ thống (ví dụ: `http://localhost/auth/v1` trên local hoặc domain staging/production). |
| `exp` | Bắt buộc (Unix timestamp) | Thời điểm hết hạn của token. Go Backend cho phép độ lệch đồng hồ (`clock_skew`) tối đa 30 giây. |
| `iat` | Bắt buộc (Unix timestamp) | Thời điểm phát hành token. Không được nằm trong tương lai (vượt quá độ lệch đồng hồ). |

---

## 4. Quản lý phiên và Tự động làm mới Token (Session & Auto-Refresh trên Web)

### 4.1. Cơ chế lưu trữ phiên (Session Storage) trên Web và Đa nền tảng

Trong quá trình xây dựng ứng dụng Client, cơ chế lưu trữ phiên làm việc (`Session` gồm `access_token` và `refresh_token`) có sự khác biệt cơ bản giữa nền tảng Di động (Mobile) và Web (Browser):

- **Trên Android / iOS (Nền tảng mở rộng sau này):**
  - Thường sử dụng thư viện `flutter_secure_storage` để ghi session vào phân vùng bảo mật phần cứng: **Android KeyStore** (chuẩn mã hóa phần cứng AES-256) hoặc **iOS Keychain**. Điều này ngăn chặn việc trích xuất token khi thiết bị bị can thiệp vật lý hoặc root/jailbreak.
- **Trên Flutter Web (Nền tảng trọng tâm hiện tại):**
  - Môi trường trình duyệt chạy trên Sandbox và không có quyền truy cập trực tiếp vào KeyStore phần cứng của hệ điều hành.
  - Thư viện `supabase_flutter` **mặc định đã tự động hỗ trợ đầy đủ cơ chế lưu trữ phiên vào trình duyệt thông qua Web Storage (`window.localStorage`)**. Khi ứng dụng khởi chạy trên Web, SDK Supabase tự động phát hiện nền tảng và khởi tạo `SharedPreferences` / Web Storage adapter để lưu trữ chuỗi dữ liệu phiên (JSON serialized session).
  - **Lưu ý khi dùng `flutter_secure_storage` trên Web:** Mặc định trên Web, `flutter_secure_storage` sẽ sử dụng Web Cryptography API kết hợp với IndexedDB / LocalStorage để mô phỏng tính năng mã hóa. Tuy nhiên, việc này đòi hỏi cấu hình bổ sung và có thể gặp vấn đề không tương thích trên một số trình duyệt hạn chế Web Crypto hoặc chế độ ẩn danh (Incognito). Do đó, đối với Flutter Web, giải pháp chuẩn và ổn định nhất là **tận dụng trình lưu trữ phiên tích hợp sẵn của `supabase_flutter`** hoặc sử dụng lớp trừu tượng hóa kho lưu trữ có kiểm tra cờ `kIsWeb`.
  - **Vai trò cốt lõi của LocalStorage trên Web:**
    - `localStorage` gắn liền với **Origin** của ứng dụng Web (`protocol://domain:port`).
    - Dữ liệu lưu trong `localStorage` **tồn tại bền vững qua các lần người dùng F5 / reload trang web, đóng tab rồi mở lại, hoặc mở nhiều tab làm việc đồng thời** trên cùng một trình duyệt.
    - Khi người dùng tải lại trang (F5), `supabase_flutter` tự động đọc dữ liệu phiên từ `localStorage`, giải mã chuỗi token, thiết lập lại đối tượng `currentSession`, và phát sự kiện `AuthChangeEvent.initialSession` / `signedIn` vào stream `onAuthStateChange`. Nhờ đó, người dùng không bao giờ bị văng ra màn hình đăng nhập một cách vô lý mỗi khi F5 trang.

### 4.2. Vấn đề bảo mật Session trên Web (Web Session Security)

Chạy trên trình duyệt đặt ứng dụng trước các thách thức bảo mật đặc thù:

1. **Nguy cơ Tấn công XSS (Cross-Site Scripting):**
   - Trên nền tảng Web, rủi ro lớn nhất đối với `localStorage` là tấn công XSS, khi mã độc JavaScript từ bên thứ ba (thư viện ngoài, CDN không kiểm soát hoặc injection) có thể đọc dữ liệu trong storage.
   - **Biện pháp phòng ngừa:**
     - **Tuyệt đối không lưu trữ token trong các biến toàn cục JavaScript không kiểm soát** (như gán vào `window.accessToken` hay biến global không được đóng gói). `supabase_flutter` đóng gói việc quản lý token bên trong bộ nhớ Dart runtime và chỉ tuần tự hóa session xuống Web Storage của chính Origin ứng dụng.
     - Kiểm soát chặt chẽ các gói thư viện phụ thuộc (`pubspec.yaml`), không sử dụng các script JavaScript tùy tiện nhúng vào `web/index.html`.
     - Phục vụ ứng dụng qua HTTPS có cấu hình header bảo mật nghiêm ngặt (`Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`) tại Nginx Reverse Proxy.
2. **Không lưu `service_role` key trên Client:**
   - `service_role` là secret key tối cao có quyền bypass toàn bộ bảo mật của Supabase, chỉ được phép lưu ở môi trường nội bộ Backend.
   - Ứng dụng Flutter Web **chỉ sử dụng duy nhất `SUPABASE_ANON_KEY`**. Bản thân `anon` key là khóa công khai an toàn (safe to be public) vì mọi quyền hạn thực tế được phân định bằng chữ ký JWT của tài khoản người dùng (`role: authenticated`) và được thẩm định độc lập bởi Go Backend.
3. **Chính sách cùng nguồn gốc (Same-Origin Policy - SOP):**
   - Triển khai Flutter Web và Go Backend API trên cùng một domain thông qua Nginx Reverse Proxy giúp bảo toàn phiên trong cùng một Origin an toàn, đồng thời triệt tiêu rủi ro CORS.

### 4.3. Khởi tạo ứng dụng trong `main.dart` trên Flutter Web

Trên Flutter Web, việc khởi tạo `Supabase` được tinh gọn và tận dụng bộ nhớ Web Storage chuẩn:

```dart
// lib/main.dart
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_dotenv/flutter_dotenv.dart';
import 'package:flutter_web_plugins/url_strategy.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:project1_client/app.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // Bật Path URL Strategy cho Web: sử dụng /login, /gateways thay vì /#/login, /#/gateways
  if (kIsWeb) {
    usePathUrlStrategy();
  }

  // Nạp cấu hình môi trường từ .env
  await dotenv.load(fileName: ".env");

  // Khởi tạo Supabase SDK.
  // Trên Web, SDK tự động sử dụng browser LocalStorage để lưu trữ session an toàn và bền vững.
  await Supabase.initialize(
    url: dotenv.env['SUPABASE_URL']!,
    anonKey: dotenv.env['SUPABASE_ANON_KEY']!,
    authOptions: const FlutterAuthClientOptions(
      autoRefreshToken: true, // Tự động làm mới token trong nền trước khi hết hạn
    ),
  );

  runApp(const MyApp());
}
```

> **Ghi chú về hỗ trợ đa nền tảng (Web + Mobile dự phòng):**  
> Nếu sau này mở rộng đóng gói sang Android/iOS mà vẫn muốn dùng chung một codebase, có thể triển khai `AppLocalStorage` kiểm tra `kIsWeb`: nếu là Web thì dùng trình lưu trữ mặc định của SDK, nếu là Mobile thì kích hoạt adapter `FlutterSecureStorage`. Kiến trúc phân tầng này đảm bảo tính độc lập tuyệt đối giữa logic nghiệp vụ và nền tảng runtime.

### 4.4. Lắng nghe trạng thái phiên thời gian thực (`onAuthStateChange`)

Để giao diện người dùng và bộ định tuyến (Router) phản ứng chính xác với các biến đổi trạng thái tài khoản (đăng nhập, phục hồi phiên từ LocalStorage khi F5, hết hạn phiên, đăng xuất), ứng dụng lắng nghe Stream sự kiện của Supabase:

```dart
Supabase.instance.client.auth.onAuthStateChange.listen((AuthState state) {
  final AuthChangeEvent event = state.event;
  final Session? session = state.session;

  switch (event) {
    case AuthChangeEvent.initialSession:
      // SDK vừa đọc xong session từ LocalStorage sau khi người dùng F5 trang
      debugPrint("Đã nạp session ban đầu từ LocalStorage: ${session?.user.email}");
      break;

    case AuthChangeEvent.signedIn:
      // Người dùng vừa đăng nhập thành công hoặc phiên được phục hồi
      debugPrint("Người dùng đã đăng nhập: ${session?.user.email}");
      break;

    case AuthChangeEvent.tokenRefreshed:
      // Token đã được tự động làm mới thành công trong nền qua Nginx
      debugPrint("Access token đã được làm mới: ${session?.accessToken}");
      break;

    case AuthChangeEvent.signedOut:
      // Đăng xuất chủ động hoặc bị thu hồi phiên trên server
      debugPrint("Người dùng đã đăng xuất, LocalStorage đã được xóa sạch.");
      break;

    case AuthChangeEvent.userDeleted:
    case AuthChangeEvent.mfaChallengeVerified:
      break;

    default:
      break;
  }
});
```

### 4.5. Cơ chế tự động làm mới Token (Auto-Refresh) và Xử lý sự cố hết hạn
- **Cơ chế nền:** Nhờ tham số `autoRefreshToken: true`, `supabase_flutter` chạy một bộ đếm nội bộ. Khi `access_token` sắp hết hạn (thông thường trước thời điểm `exp` khoảng 60 giây), SDK tự động gửi request `POST /auth/v1/token?grant_type=refresh_token` qua Nginx/Envoy để lấy cặp `access_token` và `refresh_token` mới, sau đó tự động cập nhật vào `localStorage`.
- **Trường hợp ngoại lệ (Refresh thất bại):**
  - Xảy ra khi: Người dùng bị xóa tài khoản trên hệ thống, phiên bị thu hồi thủ công bởi Quản trị viên, hoặc máy tính mất kết nối mạng liên tục trong thời gian dài vượt quá thời hạn sống của `refresh_token`.
  - Lúc này, GoTrue trả về lỗi `400 Bad Request` hoặc `invalid_grant`.
  - SDK Supabase sẽ tự động kích hoạt sự kiện `AuthChangeEvent.signedOut`, dọn dẹp sạch `localStorage`.
  - Bộ định tuyến (GoRouter) lập tức chuyển hướng người dùng về màn hình đăng nhập (`/login`) và hiển thị thông báo rõ ràng: *"Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."*

---

## 5. Cơ chế đính kèm JWT khi gọi Go Backend API

### 5.1. Định dạng Header chuẩn

Mọi yêu cầu gửi tới Go Backend (`/v1/*`) đều bắt buộc đính kèm `access_token` trong HTTP Header theo định dạng tiêu chuẩn RFC 6750:

```http
GET /v1/gateways HTTP/1.1
Host: api.example.com
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Accept: application/json
```

**Quy tắc bất di bất dịch:**
- Scheme xác thực bắt buộc là `Bearer` (không phân biệt hoa thường, nhưng khuyến nghị viết hoa `Bearer`).
- **Không bao giờ** truyền JWT qua Query String (ví dụ: `?token=...`) hoặc trong JSON Body. Go Backend được cấu hình từ chối triệt để và trả về `401 Unauthorized` nếu token không nằm trong header `Authorization`.

### 5.2. Cấu trúc phản hồi lỗi chuẩn từ Go Backend

Khi request không vượt qua được lớp xác thực, Go Backend trả về phản hồi theo chuẩn cấu trúc lỗi an toàn (`safe error contract` - Task 2.2):

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
WWW-Authenticate: Bearer
X-Request-ID: 7b35f299-e67c-48c5-9276-805c8a4128f0

{
  "error": {
    "code": "unauthorized",
    "message": "authentication required",
    "request_id": "7b35f299-e67c-48c5-9276-805c8a4128f0"
  }
}
```

### 5.3. Xây dựng HTTP Interceptor / API Client Client-side

Nhằm đảm bảo tính trong suốt cho toàn bộ ứng dụng, tạo lớp `ApiClient` tự động chèn header và xử lý thử nghiệm làm mới token khi gặp lỗi `401`:

```dart
// lib/core/network/api_client.dart
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import 'package:supabase_flutter/supabase_flutter.dart';

class ApiClient {
  final String baseUrl;
  final http.Client _httpClient = http.Client();

  ApiClient({required this.baseUrl});

  Future<Map<String, String>> _buildHeaders() async {
    final session = Supabase.instance.client.auth.currentSession;
    final token = session?.accessToken;

    return {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      if (token != null) 'Authorization': 'Bearer $token',
    };
  }

  /// Gửi GET request có kèm cơ chế retry 401 một lần sau khi refresh token
  Future<http.Response> get(String endpoint) async {
    final uri = Uri.parse('$baseUrl$endpoint');
    var headers = await _buildHeaders();

    var response = await _httpClient.get(uri, headers: headers);

    // Nếu gặp lỗi 401: Token có thể vừa hết hạn mà background timer chưa kịp refresh
    if (response.statusCode == 401) {
      debugPrint("Received 401 Unauthorized. Attempting session refresh...");

      try {
        // Thử yêu cầu SDK refresh session ngay lập tức
        final refreshResponse =
            await Supabase.instance.client.auth.refreshSession();

        if (refreshResponse.session != null) {
          debugPrint("Refresh session successful. Retrying original request...");
          // Cập nhật lại header với token mới
          headers = await _buildHeaders();
          response = await _httpClient.get(uri, headers: headers);
        } else {
          _handleAuthExpired();
        }
      } catch (e) {
        debugPrint("Session refresh failed: $e");
        _handleAuthExpired();
      }
    }

    return response;
  }

  void _handleAuthExpired() {
    // Kích hoạt đăng xuất để dọn dẹp state và đá về LoginScreen
    Supabase.instance.client.auth.signOut();
  }
}
```

---

## 6. Luồng Điều hướng, Đồng bộ URL trên Web và Đăng xuất (Web Routing, URL Sync & Sign Out)

### 6.1. Thách thức điều hướng trên Web và sự cần thiết của Declarative Routing (`go_router`)

Trên ứng dụng di động native, việc điều hướng thường dựa vào ngăn xếp màn hình nội bộ (Internal Navigation Stack) thông qua các lệnh mệnh lệnh như `Navigator.push` hay `Navigator.pop`. Tuy nhiên, khi chuyển trọng tâm phát triển sang **Flutter Web**, ứng dụng chạy trong môi trường trình duyệt với các đặc tính tương tác hoàn toàn khác:

1. **Sự hiện diện của thanh địa chỉ (URL Address Bar):**
   - Người dùng có thể quan sát trực tiếp đường dẫn, ví dụ `http://localhost/login`, `http://localhost/gateways` hoặc `http://localhost/gateways/gw-01`.
   - Người dùng có thể sao chép URL, đánh dấu trang (bookmark), hoặc gõ trực tiếp một URL nội bộ vào thanh địa chỉ để truy cập thẳng (Deep Linking).
2. **Thao tác tải lại trang (F5 / Reload):**
   - Người dùng máy tính có thói quen nhấn **F5** hoặc **Ctrl+R** bất cứ lúc nào. Khi đó, toàn bộ ứng dụng Flutter Web được nạp lại từ đầu. Nếu sử dụng `Navigator 1.0` truyền thống, ứng dụng sẽ bị mất ngăn xếp điều hướng, gây lỗi hoặc tự động quay về trang chủ sai lệch.
3. **Phím điều hướng trình duyệt (Back / Forward Buttons):**
   - Người dùng thường xuyên nhấn nút Back hoặc Forward của trình duyệt để quay lại trang trước. Ứng dụng Web phải đồng bộ lịch sử duyệt web (Browser History) với trạng thái giao diện.

**Giải pháp kiến trúc:** Dự án sử dụng thư viện **`go_router`** (dựa trên Navigator 2.0 của Flutter) làm bộ định tuyến chuẩn cho Flutter Web:
- **Đồng bộ hai chiều:** Mọi thay đổi về URL trên trình duyệt sẽ kích hoạt router tương ứng, và ngược lại, mỗi lần chuyển màn hình trong code sẽ cập nhật URL trên thanh địa chỉ.
- **Path URL Strategy:** Kích hoạt `usePathUrlStrategy()` từ `flutter_web_plugins/url_strategy.dart` để loại bỏ dấu `#` (Hash fragment), hiển thị đường dẫn chuẩn hiện đại (ví dụ `/gateways` thay vì `/#/gateways`).
- **Route Guarding với `redirect`:** Tập trung toàn bộ logic kiểm tra đăng nhập vào hàm `redirect` của `GoRouter`, loại bỏ hoàn toàn các đoạn code kiểm tra rải rác ở từng widget.

### 6.2. Cơ chế Bảo vệ Route và Tự động chuyển hướng (`GoRouter.redirect`)

Quy tắc chuyển hướng tự động được đặc tả theo ma trận trạng thái:

| Trạng thái người dùng | URL người dùng cố truy cập | Quyết định của `GoRouter.redirect` | Mục đích & Trải nghiệm |
|---|---|---|---|
| **Chưa đăng nhập** (`Unauthenticated`) | Các trang nội bộ (`/`, `/gateways`, `/gateways/:id`) | Chuyển hướng về `/login` | Bảo vệ tài nguyên nội bộ, buộc người dùng phải xác thực trước. |
| **Đã đăng nhập** (`Authenticated`) | Màn hình đăng nhập (`/login`) | Chuyển hướng về `/gateways` | Không cho phép người dùng đã có phiên hợp lệ quay lại form đăng nhập. |
| **Đang nạp phiên** (`Initial / Loading`) | Bất kỳ URL nào khi vừa F5 | Giữ nguyên và hiển thị Splash/Loading ngắn | Chờ `supabase_flutter` đọc xong `localStorage`, tránh việc redirect nhầm về `/login`. |
| **Đã đăng nhập** (`Authenticated`) | Các trang nội bộ (`/gateways`, `/gateways/:id`) | Cho phép đi tiếp (`null`) | Truy cập dữ liệu bình thường theo phân quyền. |

```text
[Người dùng gõ URL hoặc nhấn F5]
               |
               v
      +-----------------+
      | GoRouter Engine |
      +--------+--------+
               |
               v
   Đang đọc LocalStorage? 
     |                  |
   (Có)               (Không)
     |                  |
     v                  v
[Hiển thị Loading]   Đã có Session hợp lệ?
                       |               |
                     (Có)            (Không)
                       |               |
             Truy cập /login?     Truy cập route nội bộ?
               |          |         |              |
             (Có)       (Không)   (Có)           (Không)
               |          |         |              |
               v          v         v              v
     Redirect về     Cho phép    Redirect về    Cho phép
     /gateways       truy cập    /login         truy cập (/login)
```

### 6.3. Luồng Đăng xuất toàn diện trên Web (Sign Out Flow)

Thao tác đăng xuất trên Web cần đảm bảo tính triệt để, xóa sạch dữ liệu nhạy cảm và ngắt toàn bộ kết nối nền:

1. **Hủy kết nối mạng thời gian thực:** Đóng toàn bộ các kênh kết nối WebSocket đang mở tới `/v1/ws` (nếu có) để tránh rò rỉ kết nối ngầm trên trình duyệt.
2. **Gọi dịch vụ Supabase Auth:** Thực thi `await Supabase.instance.client.auth.signOut()`. Lệnh này gửi request lên GoTrue để hủy bỏ phiên (Revoke Session) trên server.
3. **Xóa sạch dữ liệu cục bộ trong Browser Storage:** `supabase_flutter` tự động xóa hoàn toàn chuỗi token trong `localStorage`.
4. **Giải phóng bộ nhớ ứng dụng (In-memory State):** Reset toàn bộ state trong BLoC / Cubit (như danh sách Gateway, thông tin user).
5. **Kích hoạt tự động chuyển hướng qua GoRouter:** Sự kiện đăng xuất phát ra `AuthChangeEvent.signedOut`, chuyển trạng thái `AuthBloc`/`AuthCubit` sang `Unauthenticated`. Do `GoRouter` lắng nghe trạng thái này thông qua `refreshListenable`, router sẽ tự động chuyển hướng người dùng về `/login` và cập nhật URL trên thanh địa chỉ.

```dart
// lib/core/auth/auth_controller.dart hoặc AuthCubit
Future<void> signOut() async {
  try {
    // 1. Ngắt WebSocket thời gian thực nếu đang kết nối
    // TelemetryWebSocketClient.instance.disconnect();

    // 2. Yêu cầu Supabase hủy phiên trên server và xóa LocalStorage
    await Supabase.instance.client.auth.signOut();

    // 3. Xóa in-memory state của ứng dụng
    // getIt<GatewayListCubit>().resetState();

    // 4. GoRouter sẽ tự động nhận diện state Unauthenticated và redirect về /login
  } catch (error) {
    debugPrint("Lỗi trong quá trình đăng xuất: $error");
  }
}
```

---

## 7. Xử lý các tình huống lỗi và Phản hồi UI/UX

Ứng dụng cần phân biệt rõ ràng các loại lỗi xác thực để cung cấp thông báo chính xác và thân thiện cho người dùng cuối:

| Tình huống / Mã lỗi | Nguyên nhân gốc | Phản hồi giao diện người dùng (UI Message) |
|---|---|---|
| `AuthException: Invalid login credentials` hoặc `invalid_grant` | Người dùng nhập sai Email hoặc Mật khẩu trên màn hình đăng nhập. | *"Email hoặc mật khẩu không chính xác. Vui lòng kiểm tra lại."* |
| `SocketException` / `ClientException` / Timeout | Không có kết nối Internet hoặc Nginx/Server không khả dụng (địa chỉ IP máy chủ sai, chưa bật Nginx hoặc chưa kết nối VPN/Wi-Fi phòng lab). | *"Không thể kết nối đến máy chủ. Vui lòng kiểm tra kết nối mạng hoặc liên hệ quản trị viên."* |
| `AuthException: Email not confirmed` | Tài khoản được tạo nhưng chưa hoàn tất xác thực email (chỉ xảy ra nếu cấu hình `email_confirm: false`). | *"Tài khoản chưa được kích hoạt. Vui lòng liên hệ Quản trị viên hệ thống."* |
| `422 Unprocessable Entity` (`signup_disabled`) | Xảy ra nếu client cố gửi request đăng ký tài khoản tự do. | *"Hệ thống không mở tính năng tự đăng ký. Vui lòng liên hệ Platform Administrator để được cấp tài khoản."* |
| `401 Unauthorized` từ Go Backend | Token JWT hết hạn, chữ ký sai, hoặc token bị thu hồi. | Tự động làm mới phiên trong nền; nếu thất bại: chuyển về Login kèm thông báo: *"Phiên làm việc đã hết hạn. Vui lòng đăng nhập lại."* |
| `403 Forbidden` từ Go Backend | Token hợp lệ nhưng tài khoản không có quyền thao tác trên tài nguyên này (ví dụ: không có trong `platform_admins` hoặc `user_gateways`). | *"Bạn không có quyền thực hiện thao tác này trên hệ thống."* |
| `502 Bad Gateway` / `504 Gateway Timeout` | Nginx đang chạy nhưng container Supabase Auth hoặc Go Backend bị tắt/treo. | *"Dịch vụ máy chủ tạm thời không khả dụng. Vui lòng thử lại sau ít phút."* |

### Nguyên tắc thiết kế UI/UX trên màn hình Đăng nhập (Web-focused):
1. **Trạng thái nút bấm và chống gửi lặp (Debounce):** Khi đang gửi request xác thực, chuyển nút "Đăng nhập" sang trạng thái `CircularProgressIndicator` và vô hiệu hóa (`disable`) các trường nhập liệu để ngăn người dùng nhấn liên tục hoặc spam phím Enter tạo ra nhiều request trùng lặp.
2. **Ẩn/Hiện mật khẩu:** Cung cấp icon con mắt để người dùng có thể kiểm tra chuỗi mật khẩu trước khi gửi, tránh gõ nhầm ký tự đặc biệt.
3. **Hiển thị lỗi nổi bật:** Sử dụng khung thông báo lỗi màu cảnh báo đặt ngay phía trên form nhập liệu để người dùng trên màn hình lớn dễ dàng quan sát.
4. **Tối ưu trải nghiệm Web (Keyboard & Responsive Layout):**
   - **Hỗ trợ phím `Enter`:** Người dùng có thể nhấn phím `Enter` ngay trên trường nhập email hoặc mật khẩu (`onFieldSubmitted`) để gửi form đăng nhập lập tức mà không cần dùng chuột nhấp vào nút.
   - **Bố cục dạng thẻ (Responsive Card):** Trên màn hình Desktop, form đăng nhập được đóng khung trong thẻ `Card` có chiều rộng tối đa (`maxWidth: 440px`), đổ bóng nhẹ, căn giữa màn hình theo cả chiều dọc và chiều ngang. Trên màn hình di động, form tự động co giãn 100% bề ngang.
   - **Tiêu đề trang Web (Page Title):** Cập nhật `Title` của tab trình duyệt thành `"Đăng nhập | IoT Gateway–Server"` để tăng tính chuyên nghiệp.

---

## 8. Mã nguồn mẫu hoàn chỉnh (Reference Implementation)

Dưới đây là mã nguồn mẫu tổ chức theo cấu trúc chuẩn cho dự án Flutter.

### 8.1. `pubspec.yaml` (Các thư viện cần thiết)

```yaml
dependencies:
  flutter:
    sdk: flutter
  flutter_web_plugins:
    sdk: flutter
  supabase_flutter: ^2.8.0
  go_router: ^14.2.0
  flutter_bloc: ^8.1.6
  equatable: ^2.0.5
  flutter_dotenv: ^5.2.1
  http: ^1.2.2
```

### 8.2. Service xác thực (`lib/core/auth/auth_service.dart`)

```dart
import 'package:flutter/foundation.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class AuthService {
  final GoTrueClient _authClient = Supabase.instance.client.auth;

  /// Lấy phiên đăng nhập hiện tại
  Session? get currentSession => _authClient.currentSession;

  /// Lấy thông tin người dùng hiện tại
  User? get currentUser => _authClient.currentUser;

  /// Kiểm tra trạng thái đã đăng nhập hay chưa
  bool get isAuthenticated => currentSession != null;

  /// Đăng nhập bằng Email và Password
  Future<AuthResponse> signIn({
    required String email,
    required String password,
  }) async {
    try {
      final response = await _authClient.signInWithPassword(
        email: email.trim(),
        password: password,
      );
      return response;
    } on AuthException catch (e) {
      debugPrint("Supabase AuthException: ${e.message} (Code: ${e.statusCode})");
      rethrow;
    } catch (e) {
      debugPrint("Unexpected login error: $e");
      rethrow;
    }
  }

  /// Đăng xuất khỏi hệ thống
  Future<void> signOut() async {
    try {
      await _authClient.signOut();
    } catch (e) {
      debugPrint("Error signing out: $e");
      rethrow;
    }
  }

  /// Lấy JWT access token hiện tại
  String? getAccessToken() {
    return _authClient.currentSession?.accessToken;
  }
}
```

### 8.3. Quản lý trạng thái xác thực (`lib/logic/auth/auth_cubit.dart` & `auth_state.dart`)

```dart
// lib/logic/auth/auth_state.dart
import 'package:equatable/equatable.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

abstract class AuthState extends Equatable {
  const AuthState();
  @override
  List<Object?> get props => [];
}

class AuthInitial extends AuthState {}

class AuthLoading extends AuthState {}

class Authenticated extends AuthState {
  final Session session;
  const Authenticated(this.session);

  @override
  List<Object?> get props => [session.accessToken, session.user.id];
}

class Unauthenticated extends AuthState {}

class AuthFailure extends AuthState {
  final String message;
  const AuthFailure(this.message);

  @override
  List<Object?> get props => [message];
}
```

```dart
// lib/logic/auth/auth_cubit.dart
import 'dart:async';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:supabase_flutter/supabase_flutter.dart' as sp;
import 'package:project1_client/core/auth/auth_service.dart';
import 'package:project1_client/logic/auth/auth_state.dart';

class AuthCubit extends Cubit<AuthState> {
  final AuthService _authService;
  StreamSubscription<sp.AuthState>? _authSubscription;

  AuthCubit(this._authService) : super(AuthInitial()) {
    _init();
  }

  void _init() {
    // 1. Kiểm tra session ngay khi khởi tạo (được phục hồi từ LocalStorage nếu có)
    final currentSession = _authService.currentSession;
    if (currentSession != null) {
      emit(Authenticated(currentSession));
    } else {
      emit(Unauthenticated());
    }

    // 2. Lắng nghe thay đổi trạng thái từ Supabase Auth
    _authSubscription = sp.Supabase.instance.client.auth.onAuthStateChange.listen((data) {
      final session = data.session;
      if (session != null) {
        emit(Authenticated(session));
      } else {
        emit(Unauthenticated());
      }
    });
  }

  Future<void> signIn({required String email, required String password}) async {
    emit(AuthLoading());
    try {
      final res = await _authService.signIn(email: email, password: password);
      if (res.session != null) {
        emit(Authenticated(res.session!));
      } else {
        emit(Unauthenticated());
      }
    } on sp.AuthException catch (e) {
      if (e.message.contains("Invalid login credentials") || e.statusCode == "400") {
        emit(const AuthFailure("Email hoặc mật khẩu không chính xác."));
      } else {
        emit(AuthFailure(e.message));
      }
    } catch (e) {
      emit(const AuthFailure("Không thể kết nối đến máy chủ. Vui lòng thử lại sau."));
    }
  }

  Future<void> signOut() async {
    try {
      await _authService.signOut();
      emit(Unauthenticated());
    } catch (e) {
      emit(AuthFailure("Lỗi khi đăng xuất: $e"));
    }
  }

  @override
  Future<void> close() {
    _authSubscription?.cancel();
    return super.close();
  }
}
```

### 8.4. Cấu hình Router với GoRouter (`lib/core/router/app_router.dart`)

```dart
// lib/core/router/app_router.dart
import 'dart:async';
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'package:project1_client/logic/auth/auth_cubit.dart';
import 'package:project1_client/logic/auth/auth_state.dart';
import 'package:project1_client/features/auth/presentation/login_screen.dart';

class AppRouter {
  final AuthCubit authCubit;

  AppRouter({required this.authCubit});

  late final GoRouter router = GoRouter(
    initialLocation: '/gateways',
    // Lắng nghe stream thay đổi trạng thái xác thực để tự động tính toán lại redirect
    refreshListenable: GoRouterRefreshStream(authCubit.stream),
    redirect: (BuildContext context, GoRouterState state) {
      final authState = authCubit.state;
      final bool isAuthenticated = authState is Authenticated;
      final bool isLoggingIn = state.matchedLocation == '/login';

      // 1. Khi đang ở trạng thái khởi tạo (đang nạp session từ LocalStorage lúc vừa F5)
      if (authState is AuthInitial) {
        return null; // Không can thiệp, chờ quá trình nạp session hoàn tất
      }

      // 2. Người dùng CHƯA đăng nhập cố truy cập vào route nội bộ -> tự động chuyển về /login
      if (!isAuthenticated) {
        return isLoggingIn ? null : '/login';
      }

      // 3. Người dùng ĐÃ đăng nhập nhưng gõ /login trên thanh URL -> chuyển về /gateways
      if (isLoggingIn) {
        return '/gateways';
      }

      // 4. Nếu truy cập đường dẫn gốc '/' -> chuyển tiếp về danh sách Gateway
      if (state.matchedLocation == '/') {
        return '/gateways';
      }

      // Cho phép đi tiếp vào route đích
      return null;
    },
    routes: [
      GoRoute(
        path: '/login',
        name: 'login',
        builder: (context, state) => const LoginScreen(),
      ),
      GoRoute(
        path: '/gateways',
        name: 'gateways',
        builder: (context, state) => const Scaffold(
          body: Center(child: Text("Màn hình Danh sách Gateway (Dashboard)")),
        ),
      ),
    ],
    errorBuilder: (context, state) => Scaffold(
      body: Center(
        child: Text("404 - Không tìm thấy đường dẫn: ${state.uri}"),
      ),
    ),
  );
}

/// Helper chuyển đổi Stream sang Listenable cho GoRouter refreshListenable
class GoRouterRefreshStream extends ChangeNotifier {
  late final StreamSubscription<dynamic> _subscription;

  GoRouterRefreshStream(Stream<dynamic> stream) {
    notifyListeners();
    _subscription = stream.asBroadcastStream().listen((_) => notifyListeners());
  }

  @override
  void dispose() {
    _subscription.cancel();
    super.dispose();
  }
}
```

### 8.5. Màn hình Đăng nhập Web (`lib/features/auth/presentation/login_screen.dart`)

```dart
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:project1_client/logic/auth/auth_cubit.dart';
import 'package:project1_client/logic/auth/auth_state.dart';

class LoginScreen extends StatefulWidget {
  const LoginScreen({super.key});

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen> {
  final _formKey = GlobalKey<FormState>();
  final _emailController = TextEditingController();
  final _passwordController = TextEditingController();

  bool _obscurePassword = true;

  @override
  void dispose() {
    _emailController.dispose();
    _passwordController.dispose();
    super.dispose();
  }

  void _submit() {
    if (!_formKey.currentState!.validate()) return;
    context.read<AuthCubit>().signIn(
          email: _emailController.text,
          password: _passwordController.text,
        );
  }

  @override
  Widget build(BuildContext context) {
    return Title(
      title: "Đăng nhập | IoT Gateway–Server",
      color: Colors.blueAccent,
      child: Scaffold(
        backgroundColor: Colors.grey.shade100,
        body: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(24.0),
            child: ConstrainedBox(
              // Giới hạn chiều rộng thẻ đăng nhập trên màn hình Web Desktop
              constraints: const BoxConstraints(maxWidth: 440),
              child: Card(
                elevation: 4,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Padding(
                  padding: const EdgeInsets.all(32.0),
                  child: BlocConsumer<AuthCubit, AuthState>(
                    listener: (context, state) {
                      // GoRouter tự động xử lý redirect sang /gateways khi state là Authenticated
                    },
                    builder: (context, state) {
                      final bool isLoading = state is AuthLoading;
                      final String? errorMessage =
                          state is AuthFailure ? state.message : null;

                      return Form(
                        key: _formKey,
                        child: Column(
                          mainAxisSize: MainAxisSize.min,
                          crossAxisAlignment: CrossAxisAlignment.stretch,
                          children: [
                            const Icon(
                              Icons.sensors_outlined,
                              size: 64,
                              color: Colors.blueAccent,
                            ),
                            const SizedBox(height: 16),
                            const Text(
                              "IoT Gateway–Server Platform",
                              textAlign: TextAlign.center,
                              style: TextStyle(
                                fontSize: 20,
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                            const SizedBox(height: 6),
                            const Text(
                              "Bảng điều khiển Giám sát & Vận hành (Web Dashboard)",
                              textAlign: TextAlign.center,
                              style: TextStyle(color: Colors.grey, fontSize: 13),
                            ),
                            const SizedBox(height: 24),

                            // Khung thông báo lỗi
                            if (errorMessage != null) ...[
                              Container(
                                padding: const EdgeInsets.all(12),
                                decoration: BoxDecoration(
                                  color: Colors.red.shade50,
                                  border: Border.all(color: Colors.red.shade200),
                                  borderRadius: BorderRadius.circular(8),
                                ),
                                child: Row(
                                  children: [
                                    const Icon(Icons.error_outline,
                                        color: Colors.red, size: 20),
                                    const SizedBox(width: 8),
                                    Expanded(
                                      child: Text(
                                        errorMessage,
                                        style: const TextStyle(
                                          color: Colors.red,
                                          fontSize: 13,
                                        ),
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                              const SizedBox(height: 16),
                            ],

                            // Trường nhập Email (hỗ trợ submit bằng Enter)
                            TextFormField(
                              controller: _emailController,
                              keyboardType: TextInputType.emailAddress,
                              enabled: !isLoading,
                              textInputAction: TextInputAction.next,
                              decoration: const InputDecoration(
                                labelText: "Email tài khoản",
                                prefixIcon: Icon(Icons.email_outlined),
                                border: OutlineInputBorder(),
                              ),
                              validator: (val) {
                                if (val == null || val.trim().isEmpty) {
                                  return "Vui lòng nhập địa chỉ email";
                                }
                                if (!val.contains("@")) {
                                  return "Email không đúng định dạng";
                                }
                                return null;
                              },
                            ),
                            const SizedBox(height: 16),

                            // Trường nhập Mật khẩu (hỗ trợ submit bằng Enter)
                            TextFormField(
                              controller: _passwordController,
                              obscureText: _obscurePassword,
                              enabled: !isLoading,
                              textInputAction: TextInputAction.done,
                              onFieldSubmitted: (_) => _submit(),
                              decoration: InputDecoration(
                                labelText: "Mật khẩu",
                                prefixIcon: const Icon(Icons.lock_outline),
                                border: const OutlineInputBorder(),
                                suffixIcon: IconButton(
                                  icon: Icon(
                                    _obscurePassword
                                        ? Icons.visibility_outlined
                                        : Icons.visibility_off_outlined,
                                  ),
                                  onPressed: () {
                                    setState(() {
                                      _obscurePassword = !_obscurePassword;
                                    });
                                  },
                                ),
                              ),
                              validator: (val) {
                                if (val == null || val.isEmpty) {
                                  return "Vui lòng nhập mật khẩu";
                                }
                                return null;
                              },
                            ),
                            const SizedBox(height: 24),

                            // Nút Đăng nhập
                            ElevatedButton(
                              onPressed: isLoading ? null : _submit,
                              style: ElevatedButton.styleFrom(
                                padding:
                                    const EdgeInsets.symmetric(vertical: 16),
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(8),
                                ),
                              ),
                              child: isLoading
                                  ? const SizedBox(
                                      height: 20,
                                      width: 20,
                                      child: CircularProgressIndicator(
                                          strokeWidth: 2),
                                    )
                                  : const Text(
                                      "ĐĂNG NHẬP",
                                      style: TextStyle(
                                        fontSize: 15,
                                        fontWeight: FontWeight.bold,
                                      ),
                                    ),
                            ),
                            const SizedBox(height: 20),

                            // Chú thích chính sách tài khoản tập trung
                            const Text(
                              "Hệ thống vận hành trạm quan trắc nội bộ. "
                              "Tài khoản người dùng được cấp phát tập trung bởi Platform Administrator.",
                              textAlign: TextAlign.center,
                              style: TextStyle(
                                fontSize: 12,
                                color: Colors.grey,
                                fontStyle: FontStyle.italic,
                              ),
                            ),
                          ],
                        ),
                      );
                    },
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
```

---

## 9. Danh mục kiểm tra bảo mật và chất lượng trên Web (Checklist)

Trước khi đóng giai đoạn kiểm thử mô-đun xác thực trên Flutter Web:

- [x] Không tồn tại màn hình hoặc nút bấm "Đăng ký" (`Sign Up`) trong ứng dụng.
- [x] Endpoint `POST /auth/v1/signup` bị từ chối triệt để bởi server (`422 signup_disabled`).
- [x] Khóa `service_role` tuyệt đối không được nhúng vào mã nguồn Flutter hay file cấu hình client. Chỉ dùng `SUPABASE_ANON_KEY`.
- [x] Session được lưu trữ an toàn bằng Web Storage (`localStorage`) trên trình duyệt, không rò rỉ token ra biến JavaScript toàn cục (`window`).
- [x] **Kiểm thử F5 / Reload trang web:** Session được phục hồi nguyên vẹn từ `localStorage`, người dùng tiếp tục làm việc bình thường mà không bị văng về `/login`.
- [x] **Kiểm thử điều hướng URL với `GoRouter`:**
  - Chưa đăng nhập mà gõ URL nội bộ (`/gateways`, `/gateways/gw-01`) -> tự động redirect về `/login`.
  - Đã đăng nhập mà gõ `/login` trên thanh địa chỉ -> tự động redirect về `/gateways`.
  - Sử dụng `usePathUrlStrategy()` hiển thị URL trực quan, không có ký tự `#`.
  - Nút Back / Forward trên trình duyệt hoạt động chính xác theo lịch sử điều hướng.
- [x] Header `Authorization: Bearer <access_token>` được tự động đính kèm trên mọi cuộc gọi API tới Go Backend.
- [x] Xử lý lỗi `401 Unauthorized` từ Go Backend bằng cơ chế thử refresh session trước khi kết luận phiên hết hạn.
- [x] Đăng xuất (`signOut`) thực hiện đầy đủ: hủy session trên máy chủ, xóa `localStorage`, hủy kết nối WebSocket và kích hoạt GoRouter chuyển hướng về `/login`.
- [x] Kiểm thử đăng nhập thành công với tài khoản được Platform Administrator cấp phát tập trung.
