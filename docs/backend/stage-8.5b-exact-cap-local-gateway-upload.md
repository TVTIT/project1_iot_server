# Stage 8.5b — exact-cap local Gateway upload

Run from the repository root:

```sh
sh scripts/test-stage8-media-storage-http.sh --stage8-local-e2e-cap
```

The mode reuses the owned local Stage 8 fixture and Gateway E2E path. It copies
the checked-in valid H.264 MP4 unchanged, then appends one top-level BMFF `free`
box with an 8-byte header, the correct 32-bit box size, and sparse zero-filled
payload to make the source exactly 80,000,000 bytes. It sends no external
requests. Input and Storage readback hashes/counts are streamed in bounded
chunks; file/log inspection does not load the video or an unbounded Gateway
log into memory.

## Result

**PASS — local exact-cap Gateway upload, with exact owned-resource cleanup.**
The actual Gateway completed the MQTT upload request, received its signed grant,
performed the CA-verified HTTPS PUT, and reached backend verification and the
matching MQTT result. The database had exactly one managed ready row; Storage
had exactly one object and the `media-video` bucket was private. Spool release
removed the spool copy while the input source remained unchanged. Graceful
Gateway shutdown and post-stop state checks passed.

| Evidence | Result |
| --- | --- |
| Declared / verified / Storage readback bytes | 80,000,000 each |
| Declared / verified / readback SHA-256 | `1133f8f033b20f3c87f89fb7e8311241ad8e97e64894469dbedfe67ea8120a91` |
| Managed DB rows / private-bucket objects | 1 / 1 |
| Bucket public flag | `false` |
| Spool phase / spool copy | `ready` / removed |
| Source untouched / signed-token log scan | PASS / PASS |
| Gateway-to-ready elapsed time | 11.117 seconds (this run only) |
| Exact cleanup | PASS |

The reused Stage 8.3c local Storage fixture records the real 80,000,001-byte
signed PUT rejection as `EntityTooLarge`; it was not rerun as part of this
slice. Local Gateway admission rejects a source above the fixed cap before it
can be spooled/published, and the backend metadata parser rejects a declared
size above the cap without reading the body. The focused Gateway-spool and
mediaworker tests passed, as did all 12 Stage 8 media-storage Python tests.

This is local evidence only. External Cloudflare/FRP/VPS behavior, aggregate
recovery and ACL regression lanes, and full CI were not run.
