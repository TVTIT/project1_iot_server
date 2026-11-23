# Hợp đồng API cấp phát URL phát lại media Stage 8 (Task 8.6, v1)

**Phạm vi:** Tài liệu hợp đồng REST API cho tính năng cấp phát URL phát lại media (Task 8.6).
Tài liệu này áp dụng cho tầng điều khiển (control plane) cấp grant URL bảo mật từ Supabase Storage v1.74.0.
Tầng dữ liệu (data plane: reverse proxy, streaming, xử lý HTTP `Range` request và phát trực tiếp trên trình duyệt) được nghiệm thu trong Task 8.7.

---

## 1. Điểm cuối và Phân quyền (Route & RBAC)

| Method | Path | Quyền truy cập |
|---|---|---|
| `POST` | `/v1/gateways/{gateway_id}/media/{media_id}/playback-url` | Người dùng xác thực (`role: authenticated`) có tư cách thành viên `owner`, `operator` hoặc `viewer` của Gateway |
| `GET`  | `/v1/gateways/{gateway_id}/media` | Người dùng xác thực (`role: authenticated`) có tư cách thành viên `owner`, `operator` hoặc `viewer` của Gateway |

- **Header bắt buộc:**
  ```http
  Authorization: Bearer <USER_JWT>
  ```
- **Xác thực danh tính và quyền:**
  - Token phải là JWT người dùng do GoTrue cấp (`role: authenticated`). Tuyệt đối **không chấp nhận `service_role` key**.
  - Kiểm tra quyền trực tiếp theo từng request trên bảng `public.user_gateways` ứng với `gateway_id`.
  - Quản trị viên nền tảng (`platform_admins`) nếu không được phân quyền thành viên trong `public.user_gateways` cho Gateway cụ thể cũng sẽ **bị từ chối HTTP 404**.
  - **Cơ chế thu hồi quyền (Revocation Semantics):**
    - Khi một thành viên bị xóa khỏi `public.user_gateways`, các request `POST .../playback-url` và `GET /v1/gateways/{gateway_id}/media` tiếp theo của người dùng này (dù JWT người dùng vẫn còn hạn) sẽ lập tức bị từ chối `404 Not Found`.
    - **Lưu ý về URL đã cấp:** Các signed URL đã được cấp phát trước thời điểm thu hồi vẫn có thể tiếp tục truy cập trực tiếp tại tầng Supabase Storage cho đến khi hết hạn TTL (~120 giây) của token đó, vì token Storage là chữ ký mật mã độc lập không tra cứu lại DB cho mỗi chunk dữ liệu.

---

## 1.1 Hợp đồng Danh sách Media của Gateway (`GET /v1/gateways/{gateway_id}/media`)

- **Điều kiện kích hoạt:** Chỉ khả dụng khi biến cấu hình `MEDIA_PLAYBACK_ENABLED=true`. Khi tắt (`false`), route trả về `404 Not Found` kèm `Cache-Control: no-store` mà không yêu cầu token xác thực.
- **Query Parameters:**
  - `limit` (tùy chọn): Số nguyên từ `1` đến `100`, mặc định là `50`.
  - `cursor` (tùy chọn): Chuỗi opaque token keyset phân trang, ràng buộc chặt chẽ với `gateway_id` và giữ độ chính xác microsecond của PostgreSQL.
  - Mọi query parameter không thuộc danh sách trên, tham số bị lặp lại (ví dụ `limit=10&limit=20`, `cursor=a&cursor=b`), hoặc giá trị rỗng (`limit=`, `cursor=`) đều bị từ chối bằng `400 Bad Request` (`invalid_request`).
- **Nội dung trả về (HTTP 200 OK):**
  - Mảng JSON gốc (root JSON array) chứa các clip video Stage 8 do server quản lý (`managed = true`) và đã qua kiểm định sẵn sàng (`validation_status = 'ready'`) thuộc Gateway này.
  - Nếu Gateway chưa có video sẵn sàng hoặc trang rỗng: trả về mảng rỗng `[]` (tuyệt đối không trả `null`).
  - Mỗi phần tử trong mảng bao gồm chính xác 5 trường công khai sau:
    - `media_id`: Chuỗi UUID canonical dạng chữ thường.
    - `file_name`: Chuỗi `<media_id>.mp4` được server sinh tự động (tuyệt đối không để lộ `storage_path`).
    - `captured_at`: Chuỗi thời gian chuẩn UTC RFC3339 lấy từ cột `captured_at`.
    - `ready_at`: Chuỗi thời gian chuẩn UTC RFC3339 lấy từ cột `last_verification_at` tại thời điểm kiểm tra `ready` (không phải `created_at` và không sáng tạo `uploaded_at`).
    - `size_bytes`: Kích thước thực tế dạng số nguyên lấy từ `actual_size`.
  - Tuyệt đối không gọi Supabase Storage signer, không gọi Storage list API, không trả về URL/token/đường dẫn nội bộ/checksum/mã băm hay trạng thái xác thực nội bộ.
- **Thứ tự sắp xếp và Phân trang (Keyset Pagination):**
  - Danh sách luôn được sắp xếp theo: `captured_at DESC, media_id DESC`.
  - Khi còn trang dữ liệu kế tiếp (số bản ghi truy vấn được > `limit`), response trả về đúng `limit` bản ghi đầu và đính kèm header chuẩn RFC 8288:
    ```http
    Link: </v1/gateways/{gateway_id}/media?limit={limit}&cursor={next_cursor}>; rel="next"
    ```
  - Khi ở trang cuối cùng hoặc danh sách rỗng: không đính kèm header `Link`.
  - Tái sử dụng cursor ở một gateway khác (cross-scope reuse) sẽ bị từ chối `400 Bad Request` (`invalid_request`), ngăn chặn hoàn toàn nguy cơ bypass phân quyền.
- **Header phản hồi:** Mọi phản hồi (kể cả 200, 400, 401, 404, 503) đều đính kèm:
  ```http
  Cache-Control: no-store
  ```

---

## 2. Quy tắc kiểm tra đầu vào và Phản hồi lỗi (Validation & Errors)

1. **Mã lỗi 400 (`invalid_request`):**
   - Trả về khi cú pháp định danh không đúng quy chuẩn:
     - `gateway_id`: Bắt buộc khớp biểu thức chính quy `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` và không được là tên định danh hệ thống dành riêng `backend_service`.
     - `media_id`: Bắt buộc là UUID dạng chuẩn canonical (chuỗi 36 ký tự chữ thường có dấu gạch nối, ví dụ `01234567-89ab-cdef-0123-456789abcdef`, không giới hạn riêng v4) và khác UUID rỗng `00000000-0000-0000-0000-000000000000`.

2. **Mã lỗi 401 (`unauthorized`):**
   - Trả về khi thiếu header `Authorization`, token sai cú pháp, token đã hết hạn hoặc không vượt qua kiểm tra chữ ký xác thực.

3. **Mã lỗi 404 (`not_found`) đồng nhất (Uniform 404):**
   - Để ngăn chặn tấn công dò quét mã định danh (ID enumeration / IDOR) và tránh làm lộ trạng thái lưu trữ của hệ thống, trả về cùng một cấu trúc lỗi cho mọi trường hợp đối tượng không khả dụng hoặc người dùng không có quyền:
     - `gateway_id` không tồn tại hoặc người dùng không có membership.
     - `media_id` không tồn tại trong DB.
     - `media_id` tồn tại nhưng thuộc về một `gateway_id` khác.
     - Clip media đang trong quá trình xử lý hoặc chưa sẵn sàng (`validation_status != 'ready'`, ví dụ `pending` hoặc `verifying`).
     - Clip media thuộc diện legacy unmanaged (`managed != true`).
     - Người dùng đã bị thu hồi quyền truy cập khỏi gateway.
   - **Cấu trúc JSON phản hồi 404:**
     ```json
     {
       "error": {
         "code": "not_found",
         "message": "resource not found"
       }
     }
     ```

4. **Mã lỗi 502 (`bad_gateway`):**
   - Trả về khi dữ liệu đường dẫn lưu trữ trong DB không nhất quán (`ErrPlaybackInconsistentPath`) hoặc dịch vụ Supabase Storage trả về phản hồi không hợp lệ/lỗi ký grant ngoài phạm vi 5xx/429 (`ErrInvalidSignerContract`).

5. **Mã lỗi 503 (`service_unavailable`):**
   - Trả về khi kết nối cơ sở dữ liệu gặp lỗi/quá thời gian chờ hoặc dịch vụ ký Storage bị gián đoạn (timeout, HTTP 5xx, hoặc HTTP 429 - `ErrSignerUnavailable`).

---

## 3. Cache-Control, Thời hạn hiệu lực và Cơ chế khôi phục phát lại

- **Header phản hồi bắt buộc:**
  ```http
  Cache-Control: no-store
  ```
  Header này được middleware áp dụng bắt buộc cho **tất cả** các phản hồi thuộc route này (bao gồm cả mã thành công 200 lẫn các mã lỗi 4xx/5xx) nhằm ngăn chặn việc trình duyệt, proxy trung gian hoặc CDN lưu bộ nhớ đệm URL chứa chữ ký bảo mật.

- **Thời hạn URL (Grant Expiry):**
  - Supabase Storage ký URL với thời hạn mặc định xấp xỉ **120 giây** (`expiresIn: 120`).
  - Phản hồi trả về trường `expires_at` ở định dạng UTC RFC3339 (kết thúc bằng `Z`), tương ứng với trường `exp` trong token chữ ký.
  - Client chỉ nên thực hiện làm mới (refresh) URL có bảo vệ trước khi yêu cầu Play hoặc Seek nếu URL sắp hết hạn, hoặc khi gặp lỗi phát lại do URL hết hạn. Tuyệt đối không thiết lập vòng lặp định thời (interval timer) tự động cấp URL liên tục khi video đang tạm dừng hoặc ở trạng thái nhàn rỗi (idle/paused).

- **Khôi phục vị trí phát (Playback Resume):**
  - Khi client thay thế URL mới cho thẻ video, client cần lắng nghe sự kiện `loadedmetadata` để khôi phục lại vị trí `currentTime` trước đó đúng một lần, tránh việc trình duyệt reset vị trí do gán `currentTime` trước khi metadata được tải.
  - Giao thức **không cam kết phát liền mạch tuyệt đối không độ trễ (no gapless resume)**; trình duyệt có thể có khoảng ngưng ngắn để nạp phân đoạn buffer mới.

---

## 4. Cấu trúc dữ liệu phản hồi thành công (200 OK)

```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Cache-Control: no-store

{
  "url": "https://<MEDIA_PLAYBACK_PUBLIC_ORIGIN>/storage/v1/object/sign/media-video/<media_id>.mp4?token=<storage_signed_token>",
  "expires_at": "2026-11-23T15:30:00Z"
}
```

- **`url`:**
  - URL HTTPS hợp lệ xây dựng dựa trên `MEDIA_PLAYBACK_PUBLIC_ORIGIN`, không chứa thông tin xác thực (`userinfo`) hoặc fragment (`#`).
  - Đường dẫn đối tượng chính xác: `/storage/v1/object/sign/media-video/<media_id>.mp4`.
  - Tham số query duy nhất: `?token=<storage_signed_token>` (token JWT chữ ký 3 phần định dạng base64url).
  - Tuyệt đối không để lộ URL nội bộ của dịch vụ lưu trữ Supabase Storage (`http://supabase-storage:5000`).
- **`expires_at`:**
  - Chuỗi mốc thời gian UTC RFC3339 chuẩn quốc tế (ví dụ: `2026-11-23T15:30:00Z`).

---

## 5. Lưu ý phục vụ dữ liệu và CORS (First-use serving & CORS)

- Khi ứng dụng web (client) được lưu trữ tại một origin khác với origin của API phát lại hoặc cụm lưu trữ media, dịch vụ data plane phải cấu hình đầy đủ các header CORS:
  - `Access-Control-Allow-Origin: <web_origin>`
  - `Access-Control-Allow-Headers: Range, Authorization, ...`
  - `Access-Control-Expose-Headers: Content-Range, Content-Length, Accept-Ranges`
- Việc kiểm thử streaming dữ liệu video qua proxy, xử lý header `Range: bytes=start-end`, hỗ trợ mã trạng thái HTTP 206 Partial Content và tích hợp end-to-end với trình duyệt thuộc phạm vi nghiệm thu của **Task 8.7**.
- Ví dụ client HTML/JS tương thích hoàn chỉnh với hợp đồng này được cung cấp tại `docs/examples/media-playback/`.

---

## 6. Cấu hình bảo mật Backend & Quy trình cấp phát Role Storage (Post-Storage Bootstrap & Rollout)

### Ranh giới quyền hạn tối thiểu (Principle of Least Privilege)
Dịch vụ ký phát lại sử dụng một role cơ sở dữ liệu chuyên biệt: `storage_playback_signer`:
- Thuộc tính role: `NOLOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION`. Role không thể đăng nhập trực tiếp và tuyệt đối không bỏ qua RLS (`NOBYPASSRLS`).
- Quyền truy cập: Chỉ được cấp quyền `SELECT` trên schema `storage` cho hai bảng `storage.objects` và `storage.buckets`. Hoàn toàn không có quyền trên schema `public` (bị thu hồi triệt để qua `REVOKE ALL ON SCHEMA public`).
- Chính sách RLS:
  - Bảng `storage.objects`: áp dụng policy `storage_playback_signer_read_video` với điều kiện nghiêm ngặt `USING (bucket_id = 'media-video')`. Role không thể ký, đọc hay liệt kê bất kỳ bucket nào khác (kể cả `media-images`).
  - Bảng `storage.buckets`: áp dụng policy `storage_playback_signer_read_buckets` với điều kiện `USING (id = 'media-video')`.
- Chuyển role từ Storage: Cấp `GRANT storage_playback_signer TO supabase_storage_admin;` để dịch vụ Supabase Storage (kết nối qua user `supabase_storage_admin`) có thể chuyển role sang `storage_playback_signer` khi ký URL đọc.

### Quy trình triển khai & Bootstrap (Rollout Instructions)
1. **Kiểm tra tiên quyết:** Các bảng `storage.objects` và `storage.buckets` phải được khởi tạo trước khi áp dụng chính sách. File SQL bootstrap kiểm tra điều kiện này và tự động dừng (fail-closed) nếu bảng chưa tồn tại.
2. **Áp dụng chính sách cục bộ / CI:**
   ```sh
   # Cách 1: Chạy script thực thi trực tiếp qua psql
   sh scripts/init-media-playback-policy.sh

   # Cách 2: Sử dụng service opt-in trong Docker Compose (chỉ kích hoạt khi chỉ định đích danh)
   docker compose run --rm stage8-media-playback-bootstrap
   ```
3. **Cấp phát Credential (`MEDIA_PLAYBACK_STORAGE_TOKEN`):**
   - Mint một token JWT HS256 từ `JWT_SECRET` của hệ thống với claim duy nhất:
     ```json
     {
       "role": "storage_playback_signer",
       "iss": "supabase",
       "iat": <epoch>,
       "exp": <epoch_tuong_lai>
     }
     ```
   - Cung cấp token này cho Go backend qua biến môi trường `MEDIA_PLAYBACK_STORAGE_TOKEN`.
   - **Ranh giới Credential độc lập cho Playback:** Scoped signer không nhận broad key (`SERVICE_ROLE_KEY`) lẫn signing secret (`JWT_SECRET`) cho các thao tác ký phát lại. Backend vẫn sử dụng các biến đó cho các thành phần quản lý người dùng (Auth Admin) và tải lên (Upload runtime) theo đúng cấu hình hiện hành, nhưng riêng luồng phát lại (`media_playback`) tuyệt đối không có fallback về `SERVICE_ROLE_KEY` và chỉ sử dụng duy nhất `MEDIA_PLAYBACK_STORAGE_TOKEN`.
   - **Không fallback:** Khi `MEDIA_PLAYBACK_ENABLED=true`, nếu thiếu `MEDIA_PLAYBACK_STORAGE_TOKEN`, backend từ chối khởi động; tuyệt đối không tự động dùng `SERVICE_ROLE_KEY`.
   - **Config Safety Claim Check:** Khi khởi động, backend tự động kiểm tra claim `role` của token. Nếu phát hiện token có role khác `storage_playback_signer` (ví dụ vô tình dùng `service_role` hoặc `anon`), hệ thống từ chối khởi động ngay lập tức.
4. **Quy trình xoay vòng token (Credential Rotation):**
   - Sinh token mới với role `storage_playback_signer`.
   - Cập nhật biến môi trường `MEDIA_PLAYBACK_STORAGE_TOKEN` trong file cấu hình môi trường `.env`.
   - **Tái tạo container backend:** Thực hiện lệnh `docker compose up -d --no-deps backend` để Compose áp dụng biến môi trường mới vào tiến trình backend. Tuyệt đối không dùng lệnh `docker compose restart backend` vì Docker không nạp lại giá trị biến môi trường mới vào container khi chỉ restart. Việc tái tạo container backend hoàn toàn không ảnh hưởng đến database hay Storage API.
