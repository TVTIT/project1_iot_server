# Stage 8.3c — Local signed Storage PUT route

> **Cấu hình hiện hành (23/11/2026):** signed upload đã được gộp vào Nginx
> chung ở `config/nginx/nginx.conf.template`. Chỉ cần đặt
> `MEDIA_UPLOAD_ENABLED=true` để bật backend media và route upload trên Nginx;
> không cần thêm `stage8-media` vào `COMPOSE_PROFILES`. Listener/profile riêng
> bên dưới là đường cũ được giữ để rollback trong giai đoạn chuyển đổi, không
> phải điều kiện để dùng route chung. Chưa kiểm thử Cloudflare/tunnel ngoài.

This is a local, opt-in route slice; it does not publish a hostname or enable
Cloudflare or FRP. The base Nginx template rejects the canonical `media-video`
signed-upload path. The root Compose `stage8-media` profile adds a dedicated
HTTP-only listener on port 80 inside `iot_net`; existing Nginx routes are not
replaced. Public TLS terminates at Cloudflare, which forwards to
`http://stage8-media-upload:80`. The media listener leaves the general Storage
API unavailable.

The `cloudflared` → listener hop is unencrypted and carries signed upload URLs,
which are bearer credentials. Use this only when Docker host/network access is
inside the deployment trust boundary. Browser/device → Cloudflare remains HTTPS.
The local fixture uses a separate test-only TLS edge to model that public HTTPS
termination; it is not part of root Compose or deployment configuration.

The only forwarded path is
`PUT /storage/v1/object/upload/sign/media-video/<lowercase-uuid>.mp4`. Nginx
streams it with an 80 MiB request ceiling and bounded proxy timers. Envoy
requires the private route-key header, strips it and client identity/auth,
apikey, and upsert headers, rewrites only the `/storage/v1` prefix, and retains
the query string for Storage's signed token. All other Storage paths are denied by this media-only Nginx profile; Envoy
also denies other `media-video` routes (`stage8-media-blocked`) after the exact
signed-PUT route, preserving the general `/storage/v1/` route for legacy Storage
operations (such as image write/read to `media-images`). Storage's signed token
retains the no-overwrite constraint.

Signed URL logging protections: Stage 8 Nginx disables access logging and
sends error logs to `/dev/null` at `emerg`; Envoy's access format contains no
path, query, referer, or user agent. The pinned Storage `v1.74.0` logger
serializes request URLs with the `token` query parameter redacted, and the
overlay sets `LOG_LEVEL=fatal` to suppress routine request logs. Fixture
artifacts must contain only sanitized status/timing metadata.

## Bật route trên Nginx chung

Trước khi bật, xác nhận ACL DynSec media và migration 28. Trong `.env`, đặt
`MEDIA_UPLOAD_ENABLED=true`, `MEDIA_UPLOAD_PUBLIC_ORIGIN` bằng HTTPS origin mà
backend và Gateway cùng dùng, và `MEDIA_UPLOAD_ROUTE_KEY` là chuỗi hex ngẫu nhiên
không rỗng, khớp với Envoy. Cùng một cờ Compose bật backend processing và route
Nginx; **không** thêm `stage8-media` vào `COMPOSE_PROFILES` cho luồng này.

Đặt `STORAGE_FILE_SIZE_LIMIT=80000000` và `STORAGE_LOG_LEVEL=fatal`; bootstrap
bucket private `media-video` bằng `scripts/init-media-bucket.py` qua Storage nội
bộ. Backend và Storage dùng chung `MEDIA_UPLOAD_GRANT_TTL_SECONDS` (mặc định
`60`); thay đổi giá trị này không đổi timeout HTTP. Cổng Nginx 80 giữ nguyên.

Profile `stage8-media` hiện chỉ dành cho listener HTTP cũ
`stage8-media-upload:80` trong thời gian giữ đường rollback. Nếu tunnel
`media-upload` còn trỏ tới listener đó, không bỏ profile đang chạy cho tới khi
hoàn tất cutover và xác minh origin mới. Fixture TLS loopback proxy tới Nginx
chung; nó không bật listener cũ hoặc gọi Cloudflare.

Owned native HTTP/Storage fixture: **PASS**, exact cleanup verified. It bootstraps
and reads back the private bucket; obtains the signed URL through the Go client;
uploads over fixture TLS edge → HTTP media listener; confirms direct Storage
read/hash and overwrite denial; exercises wrong path/method, expired-token,
alternate/admin-route denials; and confirms the pinned Storage missing-object and size-cap errors.
Storage v1.74 reports these latter errors with outer HTTP 400 and embedded
`statusCode` 404 (`NoSuchKey`) or 413 (`EntityTooLarge`); the Go client maps only
the exact machine-coded `NoSuchKey` contract to `ErrNotFound`. The cap probe sent
80,000,001 bytes and received the exact `EntityTooLarge` response. Runtime logs
and fixture artifacts passed the signed-token/route-key scan.

After switching the listener to HTTP, the Stage 8 media Python suite passed;
local Gateway E2E passed for a 1,546-byte clip and exact 80,000,000-byte upload,
with matching storage hashes and exact fixture cleanup. Compose model and
service startup validated the root profile, fixture-only TLS edge, Nginx syntax,
and Envoy listener configuration. External Cloudflare/FRP checks remain
**NOT RUN**; no external endpoint or deployment was contacted. The fixture TLS
edge models TLS termination only and is not evidence of a deployed tunnel.
