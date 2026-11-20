# Stage 8.5a — local fixture bootstrap prerequisite

Run from the repository root:

```sh
sh scripts/test-stage8-media-storage-http.sh --prepare-stage8-e2e
```

This is a local prerequisite only. It reuses the owned Stage 8 Storage HTTP
fixture and `native_compose_smoke.py` lifecycle. A fresh Compose project is
created with synthetic credentials, pinned Mosquitto TLS, loopback-only
listeners, the backend media profile enabled, and a private `media-video`
Storage bucket. The route listener is started first so its ephemeral loopback
port can be supplied to the backend before the backend starts.

The fixture creates one Auth user, adds that user to the fixture
`platform_admins` table, provisions a Gateway through `POST /v1/digital-twins`,
and provisions its MQTT credential through the ordinary
`POST /v1/admin/gateways/{gateway_id}/mqtt-credential` API. It checks the
credential's `no-store` response and active database status, then proves the
credential can subscribe to that Gateway's media/down grant topic over the
fixture CA-verified MQTTS connection. One-time credentials remain in the
process memory only and are not printed or added to artifacts. The fixture
checks exact cleanup of its Compose containers, volumes, network, and owned
DynSec bootstrap volume.

This prepare-only command does not run the Gateway simulator or upload a video.
It does not contact VPS, FRP, Cloudflare, or any external endpoint.

The opt-in happy-path integration mode shares the same owned fixture lifecycle:

```sh
sh scripts/test-stage8-media-storage-http.sh --stage8-local-e2e
```

It provisions one sensor through the owner API and runs the actual Gateway
executable with the included H.264 sample, loopback MQTTS/HTTPS origins, and
fixture CAs. Acceptance requires one ready managed DB intent and one private
Storage object whose bytes/hash match the source, followed by a ready Gateway
SQLite receipt with its spool copy removed. The simulator is stopped gracefully
only after those checks. This mode is local-only and is not Stage 8.5 acceptance.
