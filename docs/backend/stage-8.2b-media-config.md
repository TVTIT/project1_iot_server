# Stage 8.2b — Validated media process settings

This slice adds process configuration only. It does not admit intents, start
workers, create a Gateway spool, publish/subscribe media MQTT, access Storage,
or perform HTTP PUTs. `MEDIA_UPLOAD_ENABLED` remains strict opt-in and defaults
to `false`; both backend and Gateway read this same flag. With it off, media
paths and media credentials are not prerequisites.

## Backend bounds

The fixed per-file limit is `mediaprotocol.MaxUploadBytes` (80,000,000 bytes),
not an environment override. Backend environment settings are:

| Variable | Default | Bound / meaning |
| --- | ---: | --- |
| `MEDIA_UPLOAD_MAX_PENDING_PER_GATEWAY` | 1 | 1–8; no greater than global unsettled limit; caps unsettled intents per Gateway (pending/verifying or rejected/expired with unconfirmed cleanup) |
| `MEDIA_UPLOAD_MAX_UNSETTLED_INTENTS` | 8 | 1–64 |
| `MEDIA_UPLOAD_TEMPORARY_BYTES` | 640,000,000 | 80,000,000–5,120,000,000; must cover the global unsettled count times the fixed cap, including terminal objects whose cleanup is unconfirmed |
| `MEDIA_UPLOAD_STORED_BYTES` | 1,600,000,000 | 80,000,000–80,000,000,000; quota budget for ready actual bytes plus active declared reservations |
| `MEDIA_UPLOAD_INTENT_TTL` | 15m | 1ms–24h |
| `MEDIA_UPLOAD_DB_TIMEOUT` | 5s | 1ms–1m |
| `MEDIA_UPLOAD_STORAGE_TIMEOUT` | 30s | 1ms–5m |
| `MEDIA_UPLOAD_VERIFY_WORKERS` / `MEDIA_UPLOAD_VERIFY_BATCH_SIZE` | 1 / 4 | 1–8 workers / 1–64 rows |
| `MEDIA_UPLOAD_VERIFY_SCAN_INTERVAL` | 5s | 1ms–1h |
| `MEDIA_UPLOAD_VERIFY_RETRY_MIN_DELAY` / `MEDIA_UPLOAD_VERIFY_RETRY_MAX_DELAY` | 5s / 1m | 1ms–1h / 1ms–24h; minimum must not exceed maximum |
| `MEDIA_UPLOAD_VERIFY_MAX_ATTEMPTS` | 5 | 1–20 |
| `MEDIA_UPLOAD_CLEANUP_WORKERS` / `MEDIA_UPLOAD_CLEANUP_BATCH_SIZE` | 1 / 4 | 1–4 workers / 1–64 rows |
| `MEDIA_UPLOAD_CLEANUP_SCAN_INTERVAL` | 10s | 1ms–1h |
| `MEDIA_UPLOAD_CLEANUP_RETRY_MIN_DELAY` / `MEDIA_UPLOAD_CLEANUP_RETRY_MAX_DELAY` | 10s / 5m | 1ms–1h / 1ms–24h; minimum must not exceed maximum |
| `MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS` | 5 | 1–20 |

Byte values are parsed as signed 64-bit integers. The stored quota is an
admission budget: downstream admission must count ready measured bytes and
active declared reservations together, releasing a reservation only at its
terminal handling boundary. These settings do not implement that accounting.
The schema/status/cleanup contract remains as described in
[Stage 8.2a](stage-8.2a-media-schema.md).

## Gateway simulator sample

The simulator parses these settings but this slice does not build a spool or
upload client. For an explicitly enabled future media run, provide a finalized
input and a private writable spool path:

```sh
MEDIA_UPLOAD_ENABLED=true \
GATEWAY_MEDIA_INPUT_FILE=/var/lib/gatewaysim/media/finalized.mp4 \
GATEWAY_MEDIA_SPOOL_DIR=/var/lib/gatewaysim/media-spool \
GATEWAY_MEDIA_MAX_FILES=2 \
GATEWAY_MEDIA_MAX_BYTES=160000000 \
GATEWAY_MEDIA_MAX_ATTEMPTS=5 \
GATEWAY_MEDIA_RETRY_MIN_DELAY=5s \
GATEWAY_MEDIA_RETRY_MAX_DELAY=1m \
GATEWAY_MEDIA_UPLOAD_TTL=24h \
GATEWAY_MEDIA_PUT_TIMEOUT=1m \
gateway run
```

The max file count defaults to 2 (bounded 1–100); the 64-bit spool byte budget
defaults to 160,000,000 (bounded to 8 GiB) and must accommodate one fixed-cap
file per configured slot. Attempts are 1–20; retry delays must be ordered;
upload TTL is bounded to 7 days and PUT timeout to 10 minutes. Input and spool
paths must be clean absolute paths. No Storage/backend credentials are parsed
by the Gateway configuration. MQTT's existing CA configuration remains
mandatory for the simulator's existing MQTT transport; no TLS verification
override is introduced.

## Handoff and limits

Compose maps backend-owned limits and keeps the feature disabled by default.
Gateway-only variables are examples for the separate Gateway process and are not
passed into the backend. Storage endpoint, private token and route configuration
remain for their owning slice. No DB code, migration, MQTT media data/config
plumbing, deployment, or runtime behavior is included here.

The values are conservative local-demo process bounds, not measured capacity or
an assurance of disk availability. Downstream slices must enforce the same byte
accounting at admission and storage boundaries; configuration alone is not
enforcement.

## Local Storage opt-in (Stage 8.3b)

The base Compose configuration remains media-off and keeps the Storage-wide
`FILE_SIZE_LIMIT` at 52,428,800 bytes. For a local media fixture only, apply
`docker-compose.stage8-media.yml` after `docker-compose.yml`; it raises the
Storage service cap to 80,000,000 bytes and sets
`UPLOAD_SIGNED_URL_EXPIRATION_TIME` from `MEDIA_UPLOAD_GRANT_TTL_SECONDS`
(default 60 seconds). This is the grant-TTL variable name reserved for the
backend slice; it is not wired into backend runtime by this change.

Before enabling the override, an operator must inventory/audit every existing
Storage bucket and its effective cap. The service-wide cap is raised for all
buckets, and this bootstrap only explicitly keeps `media-images` at 52,428,800
bytes; other legacy buckets may have unknown or unbounded per-bucket policies.
Do not opt in until those buckets are shown to have a suitable per-bucket cap.

From a container on the local Compose network, run
`scripts/init-buckets.sh --media` with fixture-only `SERVICE_ROLE_KEY` and
`STORAGE_INTERNAL_URL` (default `http://supabase-storage:5000`). Media mode
does not source `.env` and does not route through public Nginx/Envoy. The
configured base is the Storage service origin only: direct Storage exposes
`/bucket` routes at its root, while Envoy is the component that rewrites the
external `/storage/v1` prefix. Do not include `/storage/v1` in this direct base.
The bootstrap creates or updates private `media-video` with an exact
80,000,000-byte cap and sole MIME type `video/mp4`, then checks API readback.
It creates the legacy image bucket privately at 52,428,800 bytes or, when it
already exists, only lowers a missing/unbounded or larger limit. Existing
visibility and MIME settings are included explicitly in updates; already-safe
limits are not updated. Incomplete/malformed policy readback fails closed.
Errors include only HTTP status/category, never response bodies, credentials, or
URLs. The legacy no-argument bucket bootstrap remains unchanged.

### Ghi chú cấu hình URL nội bộ

`STORAGE_INTERNAL_URL` là **origin của service Storage**, mặc định
`http://supabase-storage:5000`; script tự nối các route `/bucket/...`. Không
thêm `/storage/v1`: đây là prefix ở tuyến ngoài do Envoy xử lý, không phải
prefix của API trực tiếp trên cổng 5000. URL cấu hình không được chứa userinfo,
query hoặc fragment. Client HTTP bỏ qua proxy từ environment và không theo
redirect để tránh chuyển bearer token sang đích khác.

Native route/API proof against an isolated local Supabase Storage fixture is
deferred to Stage 8.3c; the 8.3b tests exercise bounded API behavior with a
local mock and do not qualify the pinned Storage service end to end.
