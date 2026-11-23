# Stage 8, Phần 2 — bàn giao kiểm thử local

**Phạm vi:** Gateway simulator → signed PUT → private Supabase Storage →
PostgreSQL `ready`; cấp URL phát qua API theo membership; GET/Range trên
origin HTTPS riêng. `MEDIA_UPLOAD_ENABLED` và `MEDIA_PLAYBACK_ENABLED` mặc định
tắt và độc lập. Nginx playback nghe nội bộ `nginx:8081`; không publish 8081 ra
host. Không thay Cloudflare Tunnel, listener upload rollback hoặc proxy MQTTS.

[Contract API](../protocols/stage8-media-playback-api.md) ·
[HTML mẫu](../examples/media-playback/index.html) ·
[Bằng chứng 8.2R](stage-8.5d-local-regression-handoff.md#82r--phục-hồi-khi-restart-tiến-trình-go-backend) ·
[Mẫu frpc](../../frpc_config/frpc.toml.example).

## Tái lập local

Từ repo root, dùng Docker/Compose local, Go theo `src/go.mod`, Python 3 với `cryptography`,
OpenSSL, `setfacl` và các image đã ghim trong CI. Không `source .env` triển
khai; fixture tự tạo credentials/CA và dùng các cổng loopback động.

```sh
sh scripts/test-stage8-media-storage-http.sh --stage8-local-playback-api
RUN_PARENT=$(mktemp -d /var/tmp/opencode/stage8-playback-gate-XXXXXX)
sh scripts/test-stage8-playback-gate.sh "$RUN_PARENT/run"
```

Lệnh đầu kiểm GoTrue, quyền PostgreSQL và signer Storage thật. Lệnh thứ hai
chạy fixture `--stage8-local-playback`, chỉ đạt khi có upload `ready`, nhận
Range `206` khớp kích thước, các ca quyền đại diện và cleanup chính xác.
Thư mục run mới có `summary.json`. Job `stage8-media-playback-integration`
gọi cùng gate, không cần browser và không dùng endpoint triển khai. Không
tính fixture bị bỏ qua hoặc không có report là PASS.

Để chạy browser, đặt `STAGE8_CHROMIUM_PATH` tới binary có H.264 và
`STAGE8_PLAYWRIGHT_DIR` tới thư mục module `playwright-core` cài sẵn. Cần Node,
FFmpeg với `libx264` và `certutil`; runner không cài công cụ thay bạn.

```sh
sh scripts/test-stage8-media-storage-http.sh --stage8-local-playback-browser
```

Browser dùng NSS profile riêng, import CA fixture và xác minh TLS ở cả hai
origin. Clip H.264 faststart được sinh trong fixture, upload qua Gateway, rồi
phát bằng chính HTML mẫu; không bypass verify TLS hay mock luồng byte video.

## Kết quả đã quan sát

- **8.2R PASS:** backend mới claim generation 2, GET lại object đã upload và
  commit `ready` khi Gateway đang dừng; đúng một PUT. Hai ca media-off đạt.
- **8.6 PASS:** owner/operator/viewer xin URL của cùng clip; nonmember, admin
  không có membership, sai Gateway, legacy/chưa ready và viewer đã revoke nhận
  `404` đồng nhất. Header `no-store`; `expires_at` lấy từ token Storage ký với
  yêu cầu TTL 120 giây. URL đã cấp vẫn có thể đọc đến hạn dù membership bị thu hồi.
- **8.7 PASS:** GET trả đúng hash/MIME; Range `8–23` trả `206`, đúng 16 byte;
  ngoài file `416` không có `Content-Range` trên Storage v1.74.0. ETag khớp và
  cũ ở ca `If-Range` đều trả `206`; không cam kết conditional Range đầy đủ.
  `HEAD` đóng (`405`). Signed GET bị chặn ở origin API. Fixture HTTPS Chromium
  phát clip 45 giây, 13.079.010 byte, tua đến giây 25 ngoài vùng đệm
  `[0; 0,97]`, nhận Range từ byte 7.208.960, tiếp tục tăng thời gian phát và
  giải mã frame. Lượt xác nhận với download giới hạn khoảng 350 KiB/s trong
  Chromium đo `0,94 s` đến khi video phát qua mốc 0,5 s và có frame giải mã;
  `0,46 s` từ lệnh tua đến khi hết trạng thái `seeking`, phát qua giây 25 và
  có frame mới. Đây là thời gian quan sát trong fixture, không phải độ trễ mạng.
- **Hồi quy local:** P2 full; P7 telemetry pilot (8 batch/800 mẫu, không phải
  toàn bộ P7); media-off, Storage ảnh, signed PUT thường, upload đúng
  80.000.000 byte và verifier-timeout đều có lượt PASS. Giữ nguyên FAIL
  lịch sử: P2 từng không lấy được JWKS do Envoy từ chối LDS regex, hai lượt P7
  timeout, verifier-timeout từng bỏ lỡ cửa sổ quan sát. Không tính lại các lượt
  đó là PASS.
- **8.8B phía repo:** TOML/Compose PASS. Route `media-playback` trong mẫu FRP
  tắt mặc định, không đổi MQTTS; CLI `frpc verify` NOT RUN vì thiếu binary/image
  ghim local.
- **8.9 local PASS:** gate dùng chung với CI xác nhận upload `ready`, Range
  `206` khớp byte/kích thước, nonmember/sai Gateway và revoke bị chặn, cleanup
  chính xác. Gate phân biệt FAIL, SKIP và NOT RUN, chỉ xuất summary đã lọc
  khi lane thật sự đạt.

## Post-Storage Bootstrap & Quyền hạn Playback Signer

- **File bootstrap chuẩn duy nhất:** `scripts/post-storage-bootstrap.sql` là nguồn sự thật duy nhất cho việc tạo role `storage_playback_signer` và gán chính sách RLS. Fixture không sao chép SQL inline mà nạp trực tiếp file này.
- **Ranh giới bảo mật:**
  - Role `storage_playback_signer` sở hữu `NOLOGIN`, `NOBYPASSRLS`, `NOINHERIT`, `NOSUPERUSER`.
  - Quyền hạn bị cô lập nghiêm ngặt: chỉ `SELECT` trên `storage.objects` (chỉ cho phép `bucket_id = 'media-video'`) và `storage.buckets` (chỉ cho phép `id = 'media-video'`). Toàn bộ quyền trên `public` bị thu hồi triệt để.
  - Phân quyền chuyển đổi: `GRANT storage_playback_signer TO supabase_storage_admin;`.
- **Thực thi và Rollout:**
  - Script điều phối: `sh scripts/init-media-playback-policy.sh`. Script yêu cầu bảng `storage.objects` và `storage.buckets` phải tồn tại và RLS đã được kích hoạt sẵn bởi Storage v1.74, tự động fail-closed nếu thiếu; toàn bộ script thực thi trong một transaction `BEGIN ... COMMIT` nguyên tử mà không làm thay đổi trạng thái RLS toàn cục của bảng Storage.
  - Docker Compose: Có thể khởi chạy container bootstrap độc lập:
    `docker compose run --rm stage8-media-playback-bootstrap`. Service này sử dụng profile bảo trì riêng biệt (`stage8-media-playback-bootstrap`), không nằm trong profile `stage8-media` để tránh bị chạy DDL tự động khi vận hành rollback upload; đồng thời sử dụng `condition: service_healthy` với `supabase-storage` để tránh tranh chấp (race condition) khi Storage đang chạy migration.
- **Cấp phát & Xoay vòng Credential:**
  - Token `MEDIA_PLAYBACK_STORAGE_TOKEN` được tạo độc lập bằng JWT secret của GoTrue/Storage với claim `{"role": "storage_playback_signer"}`.
  - Scoped signer không nhận broad key (`SERVICE_ROLE_KEY`) hay signing secret cho các thao tác phát lại. Backend Go vẫn dùng các khóa này cho auth và upload như đã cấu hình, nhưng riêng tính năng playback tuyệt đối không có fallback về `SERVICE_ROLE_KEY`.
  - Backend Go kiểm tra claim an toàn khi nạp config: nếu token chứa role khác `storage_playback_signer` (ví dụ `service_role` hay `anon`), hệ thống từ chối khởi động.
  - Khi xoay vòng token, cập nhật biến môi trường `MEDIA_PLAYBACK_STORAGE_TOKEN` và **tái tạo container backend** (`docker compose up -d --no-deps backend`). Không dùng lệnh `docker compose restart` vì Docker Compose không nạp lại giá trị biến môi trường mới vào container khi chỉ restart.
  - Runbook nghiệm thu từ xa (`../docs/backend_plan/stage8_part2_remote_acceptance_runbook.md`) nằm ngoài git repo và không bị chỉnh sửa trong phiên làm việc này.

Các lượt native trên baseline `6994d3eba0f0f34f69922ed7b9c1e0dfda9bdf40`
cùng các diff ở từng mốc, không phải một commit sạch. Chỉ mục report đã lọc
được giữ trong scratch `/var/tmp/opencode/`: `stage8-2r-c-durable-20261123-031706/`,
`stage8-playback-api-JREP5L/`, `stage8-playback-slice-b-verified/`,
`stage8-playback-slice-c-verified/`, `stage8-browser-demo-final-65KAZr/`, `stage8-p2-regression-PWliG9/`,
`stage8-p7-diagnostic-cfe_oa00/`, `stage8-upload-regression-ARfg2w/` và
`stage8-playback-gate-final-20261123-03/`.

**NOT RUN:** GitHub CI từ xa, FRP/NPM/VPS/DNS cho playback, playback qua mạng
ngoài, rollout, AM5728/HIL và capacity/soak. Runbook nghiệm thu remote:
`../docs/backend_plan/stage8_part2_remote_acceptance_runbook.md` tính từ repo root
(ngoài Git). Không bật feature trên môi trường thật trước khi thực hiện runbook.
