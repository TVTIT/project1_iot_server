# Stage 8.3c — Local signed Storage PUT route

This is a local, opt-in route slice; it does not publish a hostname or enable
Cloudflare, FRP, or a deployment profile. The base Nginx template rejects the
canonical `media-video` signed-upload path. The Stage 8 Compose overlay adds a
separate loopback HTTPS listener and requires fixture/operator-provided
`MEDIA_TLS_DIR` and a hex `MEDIA_UPLOAD_ROUTE_KEY`; existing Nginx routes are
not replaced. The media listener leaves the general Storage API unavailable.

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

Owned native HTTP/Storage fixture: **PASS**, exact cleanup verified. It bootstraps
and reads back the private bucket; obtains the signed URL through the Go client;
uploads over CA-verified HTTPS; confirms direct Storage read/hash and overwrite
denial; exercises wrong path/method, expired-token, alternate/admin-route
denials; and confirms the pinned Storage missing-object and size-cap errors.
Storage v1.74 reports these latter errors with outer HTTP 400 and embedded
`statusCode` 404 (`NoSuchKey`) or 413 (`EntityTooLarge`); the Go client maps only
the exact machine-coded `NoSuchKey` contract to `ErrNotFound`. The cap probe sent
80,000,001 bytes and received the exact `EntityTooLarge` response. Runtime logs
and fixture artifacts passed the signed-token/route-key scan.

Targeted checks: Stage 8 media Python tests and
`go test ./internal/mediastorage -count=1` passed; Compose model and service
startup validated the overlay, Nginx syntax, and Envoy listener configuration.
External Cloudflare/FRP cap and timing checks remain **NOT RUN**; no external
endpoint or deployment was contacted. Exact 80,000,000-byte end-to-end upload
remains in Stage 8.5.
