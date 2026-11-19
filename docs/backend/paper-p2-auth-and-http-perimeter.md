# P2 — RS256/JWKS và HTTP perimeter

## Tóm tắt bàn giao mức Local MVP (Cập nhật 24/10/2026)

- **Trạng thái & Lượt chạy kiểm chứng tươi:** Bàn giao P2 mức Local MVP hoàn tất sau khi rà soát đầy đủ 9 điều kiện hoàn thành và ghi nhận checkpoint. Lượt chạy native đầy đủ sau reboot `paper-p2-reverify-after-reboot-20261024-220120-30702` đạt **PASS** (`full_p2_accepted: true`, `status: PASS`, run ID `ncs_fcd6c1c6f81a`).
- **Mã nguồn & Artifacts:** Commit `926f642b71bf6729da81209926f25c79a05e6042`, branch `feat/paper-requirement`, `dirty: true` (bảo toàn uncommitted changes). Artifacts an toàn tại `/tmp/opencode/paper-p2-reverify-after-reboot-20261024-220120-30702/artifacts/` (5 files: `run.json`, `metrics.csv`, `resources.csv`, `spike.json`, `startup-summary.json`).
- **Xoay khóa thực tế (Key Rotation):** Auth chuyển signer K1 → K2, overlap công bố đồng thời K1+K2; Storage nhận static public JWKS K1/K2 theo quy trình của profile kiểm thử và được recreate container (K1 đạt 200, K2 đạt 503 → 200 sau 1 retry); tiến trình Go tự nhận K2 không cần khởi động lại (`rotation.go_restarted: false`).
- **Hồi quy Uplink & Temporal:** 40 messages mới / 4.000 samples, DB delta 40/4.000/40, outbox drained (`pending_rows: 0`); replay byte-exact thành công không tăng DB; đọc `/temporal` qua HTTPS bằng user RS256 trả về HTTP 200 với đúng **199 điểm** thực tế theo cửa sổ truy vấn.
- **Dọn dẹp chính xác:** Dọn sạch 19 containers, 1 network, 7 volumes (`exact_cleanup_verified: true`); các tệp private inputs đã được tự động xóa và chỉ lưu giữ các artifacts an toàn (không chứa secret hay credentials).
- **Xác minh kỹ thuật cục bộ P2.8b:** Hoàn tất 8/8 bước PASS trên Go 1.27.3: Go race 5 packages PASS, gofmt clean, go vet clean, golangci-lint 0 issues, go build clean, govulncheck 0 reachable/package findings (1 module-only unimported advisory), Python unittests 336 tests PASS, git diff --check clean. Log bảo vệ tại `/tmp/opencode/p2-p28b-verification-1792855796`.
- **Số liệu độ trễ:** Xem số liệu đo lường tươi và tái tính toán mới tại **mục P2.7** (`client_https` N=16 mean 3,16 ms; Go cumulative verify N=49–50 mean ~0,08–0,11 ms); các số liệu tại mục P2.9 được giữ làm bản nháp lịch sử của run `ncs_60a5e67bb655`.
- **Ranh giới & Giới hạn cốt lõi (chi tiết tại [backlog chung](../dynsec/limitations-and-backlog.md)):**
  - Nghiệm thu cục bộ mức local MVP trên profile cô lập; cấu hình deployment mặc định vẫn là HS256/HTTP.
  - Phản hồi tạm thời 503 được quan sát thấy trong quá trình thực hiện rotation / readiness probe của Storage (không khẳng định nguyên nhân gốc cấp thấp là do tuyến Envoy-to-Storage); cơ chế bounded retry chỉ áp dụng cho các phản hồi transport/readiness tạm thời được nhận diện, tuyệt đối không retry lỗi auth/policy/TLS; không có cam kết zero downtime.
  - Không thu hồi JWT tức thì (không có session blocklist/Redis; token K1 còn hạn theo `exp` cộng clock skew, với 30s thuộc fixture/default P2 và khoảng cấu hình cho phép 0–5 phút, chừng nào K1 còn trong JWKS; Go cache có hard freshness TTL 60s theo paper profile).
  - Thu hồi membership có hiệu lực ở các request mới sau khi transaction thu hồi đã commit do DB được kiểm tra trên mỗi request; không có cơ chế hủy các truy vấn đang thực thi dở dang (in-flight queries).
  - Đường truyền nội bộ giữa Nginx/Go/Envoy là HTTP/Unix trên mạng riêng Docker (trust boundary nội bộ), không phải TLS toàn tuyến.
  - Không tuyên bố toàn bộ dependency graph sạch advisory (tồn tại 1 module-only unimported advisory của `golang.org/x/crypto`).
  - Không tuyên bố cải thiện hiệu năng benchmark hay double verification tăng tốc độ.
  - Hoàn tất bàn giao P2 mức Local MVP: Rà soát tài liệu checklist 9 điều kiện hoàn thành và ghi nhận checkpoint AGENTS.md đã hoàn tất; live rollout nằm ngoài phạm vi P2. Remote CI và kiểm thử phần cứng nhúng (board AM5728 / HIL) tiếp tục giữ trạng thái NOT RUN và không phải là cổng bắt buộc của P2. Thao tác commit/push chưa được yêu cầu và không phải điều kiện hoàn thành task.
  - `STAGE5_ENABLED=false`, Stage 5 giữ mốc tạm dừng sau 5.4; không triển khai P3 hay stage khác.

## P2.0 — Baseline và trust contract

- Trước thay đổi: branch `feat/paper-requirement`, HEAD
  `926f642b71bf6729da81209926f25c79a05e6042`, tracked/untracked worktree sạch.
- Source terminal migration 23; không sửa migrations hoặc `.env` deployment.
- Dùng lại lifecycle/registry/schema checks/quota P0/P1, một backend active.
  P0/P1 giữ HS256 mặc định; cấu hình RSA/perimeter mới opt-in.
- Images hiện có trong cache (chưa chứng minh spike PASS):
  GoTrue `supabase/gotrue:v2.196.0`, Storage `supabase/storage-api:v1.74.0`,
  Envoy `envoyproxy/envoy:v1.39.1`, Nginx `nginx:1.25-alpine`.
  Image IDs thực được ghi theo lượt fixture.

| Token/key | Consumer và đường đi | Contract P2 |
| --- | --- | --- |
| User access token | Client → Nginx → Go business/Unix verify; client → Envoy → Storage | RS256, public RSA verification; Bearer Authorization; không dùng API key làm human principal |
| Anon API key | Client → `/auth/v1/token?grant_type=password` qua Nginx/Envoy | Legacy HS256 trong header `apikey`; không yêu cầu user JWT để login; không cấp quyền business Go |
| Service-role API key | Server → Envoy → `/auth/v1/admin/users`; Storage infrastructure | Legacy HS256, `apikey` và Bearer; chỉ server-side, không đưa client |
| Legacy JWT secret | Auth API-key compatibility và Storage | Không phải RS256 user-verifier secret; giữ riêng consumer cần legacy |
| Auth RSA private signing JWK | Chỉ Auth/private fixture inputs | Một key có `key_ops=sign`; public JWKS không có private/symmetric material |
| Storage public JWKS | Storage user verification | Chỉ public RSA keys; static config cần đồng bộ/restart khi rotation |
| Storage URL signing | Storage signed-URL API | Giữ cơ chế HMAC hiện hữu; không ép RS256 hoặc cấp Auth private key |

User contract: chỉ RS256, `kid` bắt buộc; RSA public key có `alg=RS256`,
`use=sig`, `key_ops=verify`. Issuer khớp cấu hình, audience `authenticated`,
exp/iat bắt buộc hợp lệ, nbf nếu có phải hợp lệ; subject UUID khác nil,
role `authenticated`; token tối đa 16 KiB, clock skew hữu hạn như cấu hình.
Không lấy key URL từ token headers/claims, không fallback HS256 trong mode RSA.

Identity không là authorization: `user_gateways`/`platform_admins` vẫn là nguồn
quyền DB. Auth subrequest không thay guard của request chính. Membership bị xóa
có hiệu lực ở request mới với cùng JWT; không hứa hủy request đang chạy.
Logout/refresh-session revoke không vô hiệu access JWT tức thì trước exp/skew.

## P2.1 — Spike tương thích

Spike đã PASS: login RS256, public JWKS, legacy anon/service-role, Auth Admin
và request user RS256 tới Storage trên các image đang pin.
Spike chỉ kiểm Supabase qua HTTP riêng/local theo topology P0; HTTPS/auth_request
chưa triển khai ở bước này, không gọi là TLS toàn tuyến.

Thiết lập spike:

- `GOTRUE_JWT_KEYS` là JSON array theo `JwtKeysDecoder` tại tag v2.196.0:
  RSA K1 có `key_ops=[sign]`, HMAC compatibility key không kid có
  `key_ops=[verify]` cho legacy API keys. Không có hai signing keys.
- GoTrue tự chọn signing algorithm từ JWK; nhánh ValidMethods lấy từ key set.
  Không bỏ legacy key rồi kỳ vọng service-role HS256 tự tương thích với RSA.
- Storage `JWT_JWKS` nhận public RSA set; giữ `AUTH_JWT_SECRET` và HMAC
  algorithm mặc định cho API keys/URL signing. Không truyền private RSA JWK.
- Python `cryptography` có sẵn dùng sinh RSA và kiểm chữ ký token, không cài
  package/crypto framework mới. Import lazy để P0/P1 không thêm dependency này.
- P2 spike là mode riêng `--paper-p2-spike`, không kết hợp P0/P1; backend vẫn
  HS256 ở spike này, chưa là bằng chứng Go chấp nhận RS256.

Kiểm tra trước spike:

- Guard mới RED: `prepare()` chưa nhận `paper_p2_spike` (2 errors đúng phần thiếu).
- GREEN: 215 tests Python (P2 guards + P0/P1/native + Stage 3 helpers), syntax
  compile và `git diff --check` PASS. Guard mở rộng thêm kiểm private key không
  đi sang service khác và P0/P1 không sinh RSA keys: 3 P2 tests PASS.
- Baseline `go test ./internal/auth ./internal/config ./internal/httpserver
  -count=1`: 3 packages PASS. Chưa thay Go runtime, chưa chạy native P2 hoàn chỉnh.

### Kết quả native P2.1

```sh
python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/paper-p2-spike-20261024-first --paper-p2-spike
```

Exit 0, `PAPER_P2_SPIKE_ACCEPTED`, project `ncs_0a1b4ad4976b`.
`artifacts/spike.json` giữ kết quả an toàn; `full_p2_accepted=false`.

- Auth Admin legacy service-role: 200; login bằng legacy anon: 200.
- User token: RS256, `kid=p2-k1-61f956ecb00036e8`, issuer/audience/role/subject/
  iat/exp đúng; chữ ký kiểm bằng public RSA key PASS.
- JWKS đọc không cần JWT/API key: 200, một public RSA key đúng expected,
  không private/symmetric material.
- Storage service-role list buckets: 200; tạo private bucket fixture: 200;
  user RS256 list empty bucket: 200, `[]`. Đây mới là probe nhận RSA token,
  chưa thay private-object/policy và signed-URL probes bắt buộc ở P2.5.
- Exact cleanup verified: **11 containers, 1 network, 4 volumes**.
  Private inputs của lượt PASS đã xóa; không giữ signing key hay token trong artifact.
- Image IDs thực:
  - GoTrue: `sha256:c0c25187a6b835e65a6f6e6c6b39d090e832d40e6de5186f2c038e0411944232`.
  - Storage: `sha256:f1546fac6d1c7e345428ac904bfaa7be7cecd50a1f549fe1cf38c628a7b15c85`.
  - Envoy: `sha256:57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4`.
  - Nginx: `sha256:516475cc129da42866742567714ddc681e5eed7b9ee0b9e9c015e464b4221a00`.

## P2.2 — Implementation đang có, chưa hoàn thành verification

- Thêm verifier sau interface cũ bằng `keyfunc/v3 v3.8.2` + `jwkset v0.11.3`;
  thư viện yêu cầu `jwt/v5 v5.3.1` (từ v5.3.0), thêm `x/time v0.15.0` cho limiter.
  Không tự viết RSA/JWK decoder hoặc crypto implementation.
- Remote SDK cache/refresh dùng một configured endpoint; response tối đa 64 KiB,
  1–8 keys, public RSA 2048–8192 bits, đúng alg/use/key_ops, kid không trùng;
  reject private/symmetric/certificate URL material trước khi SDK xử lý.
- HTTP timeout mặc định 2 giây, không redirects/proxy env, HTTPS giữ CA/hostname
  verification mặc định; tối đa một network fetch đồng thời. HTTP riêng tới Envoy
  là trust boundary mạng Docker, không TLS toàn tuyến.
- TTL mặc định 60 giây là hard expiry từ lần replace key set hợp lệ; SDK refresh
  mỗi TTL/2, unknown/expired kid có rate limit 5 giây và timeout. Không dùng
  upstream `max-age=600` thay TTL; không giữ cache vô hạn khi refresh lỗi.
- Config mode rõ `hs256|rs256`, mặc định hs256, không fallback; RSA mode không
  đọc/copy legacy secret vào config verifier. Composition fail startup nếu key
  ban đầu không dùng được; chưa chạy composition RSA native.
- RED: tests Auth/Config không compile vì constructor/fields chưa có → GREEN:
  `go test -race ./internal/auth ./internal/config ./internal/httpserver
  ./cmd/server -count=1`: **4 packages PASS**.
- `python3 -m unittest scripts/tests/test_paper_p2.py
  scripts/tests/test_native_compose_smoke.py scripts/tests/test_stage3_uplink_runner.py`:
  **216 tests PASS**.
- Chưa hoàn tất formatting/vet/lint/build sau phần Go mới; chưa chạy full P2.
  Không commit/push. Không tick checklist P2 hoặc coi những bước sau là hoàn tất.

## Điểm dừng trước duyệt — Go vulnerability scan (lịch sử)

`govulncheck ./...` và `govulncheck -show verbose ./...`: **FAIL, exit 3** trên
`go version go1.27.2 linux/amd64`.

- [GO-2026-6809 / CVE-2026-111943](https://pkg.go.dev/vuln/GO-2026-6809),
  published 21/10/2026: bổ sung distrust cho các intermediate certificates
  đã bị compromise trong `crypto/x509.Certificate.Verify`; fixed **Go 1.27.3**.
- Scanner tìm thấy symbol path:
  `internal/mqttcredential/controller_client.go:105` → `tls.Conn.HandshakeContext`
  → `x509.Certificate.Verify`. Đây là đường TLS đã có trước P2; không suy rằng
  CA tự sinh trong fixture đã bị compromise hoặc chứng minh được exploit ở fixture.
- Không có finding ở packages import của dependency mới. Finding module-only
  `GO-2026-5932` thuộc `x/crypto/openpgp` không được import/call; không đánh đồng
  với finding reachable của stdlib hoặc tự nâng dependency không liên quan.
- Toolchain đang pin ở `src/go.mod`, `src/Dockerfile` và `.github/workflows/ci.yml`;
  CI guard tests và docs hiện hành cũng nhắc 1.27.2. Chưa thay các pin này.
- Nâng toolchain cần mở rộng phạm vi ngoài danh sách P2; giữ bản cũ và chấp nhận
  limitation cũng cần người dùng quyết định. **Dừng trước P2.3**, không tự chọn.
- Đề xuất: duyệt nâng patch đồng bộ lên 1.27.3, kiểm lại scanner và affected
  tests/build, rồi tiếp tục P2.2 → P2.9. Không sửa evidence P0/P1 lịch sử.

### Tự đánh giá tại điểm dừng (không phải nghiệm thu)

- Đúng đắn 4/5: spike và tests có kết quả thật; verifier mới chưa qua native.
- Đầy đủ 2/5: P2.3–P2.9 chưa làm do cần quyết định toolchain; không claim hoàn tất.
- Rõ ràng 4/5: phân biệt spike HTTP/HTTPS chưa có, finding reachable/module-only;
  chưa có report cuối về latency/rotation.
- Khả thi 4/5: nêu chính xác phiên bản vá và nơi cần đồng bộ; cần quyền nâng patch.
- Ngắn gọn 4/5: có checkpoint chi tiết để tiếp tục, chưa có bản nghiệm thu ngắn.

Trung bình 3,6/5. Ưu tiên sau duyệt: giải quyết quyết định toolchain; hoàn tất
verification P2.2; mới mở socket/perimeter và native acceptance. Người dùng có
cơ sở coi P2 chưa hoàn tất, đúng với checkpoint này.

## Nâng patch Go sau duyệt (24/10/2026)

Người dùng đã nâng host lên Go 1.27.3 và cho phép đồng bộ Docker/các nơi còn lại,
bao gồm dừng/rebuild backend development. Không nâng Supabase, sửa migrations,
deployment `.env`, hoặc bật Stage 5/RS256 trên development.

- Host xác nhận `go version go1.27.3 linux/amd64`.
- Đổi `src/go.mod`, Docker builder backend/Gateway và SQLite spike dùng khi chạy
  lại, 12 pin setup-go trong CI, 4 assertions CI guards, README/operating guide
  và phiên bản hiện hành trong AGENTS.md ignored. Không sửa evidence lịch sử.
- Guard toolchain mới RED trên pin 1.27.2 → GREEN sau đổi pin. Tổng Python tests
  liên quan: **289 PASS**. `go mod tidy` chỉ chuẩn hóa dependency P2 đang dùng.
- `go vet ./...`, `go build ./...`: PASS. Lint lần đầu báo 2 missing comments
  của exported methods P2; đã sửa, lượt sau **0 issues**.
- `govulncheck ./...` trên Go 1.27.3: exit 0, **0 reachable vulnerabilities**,
  0 package findings; vẫn có 1 module-only advisory không được import/call
  (`x/crypto/openpgp`), không tuyên bố toàn dependency graph không có advisory.
- Lượt `go test -race -count=1 ./...` đầu bị harness timeout 120 giây sau 17
   packages PASS; **không tính là full PASS**. Lượt chạy lại
   `go test -json -race -count=1 -timeout=10m ./...` exit 0: **21 packages PASS**,
   0 failures, 98 opt-in test skip events (không tính SKIP thành native PASS).
   JSON log bảo vệ `/tmp/opencode/go1273-race.jsonl`.
- Build Docker backend và Gateway: PASS. Đã recreate riêng backend development
  bằng `docker compose up -d --no-deps --no-build --pull never backend`, giữ
  `SUPABASE_JWT_MODE=hs256`, `STAGE5_ENABLED=false` qua process override; không
  sửa `.env` hoặc chạy lại init jobs/migrations, DB/broker không bị recreate.
- Container `iot_backend` healthy; copy binary ra để `go version` đọc metadata
  xác nhận **go1.27.3**. Image ID:
  `sha256:12ac62fbd88e58ceefe87e3ce3a3d3d21bab5d6984b94e633ca6d24184b970db`.
  Readiness trực tiếp Go và qua Nginx đều PASS bằng client trong container.
- Cross-build Gateway ARMv7 `CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7` PASS;
  chỉ là compile, chưa chạy board/HIL.
- Native P1 regression Go mới:
  `sh scripts/test-paper-p1.sh --mode baseline /tmp/opencode/go1273-paper-p1-regression`
  exit 0, `PAPER_P1_ACCEPTED`, project `ncs_02dae04bc00e`.
  **40 new messages / 4.000 samples**, DB delta ledger/raw/temporal = 40/4.000/40,
  configuration = 0; 40 Gateway application ACK, 1 replay duplicate không tăng
  dữ liệu, Go ACKPublished = 41, failures = 0. Metrics auth statuses
  401/401/403/200. Exact cleanup verified **15 containers / 1 network / 6 volumes**;
  private inputs đã xóa. Đây là regression mới, không sửa số đo P1 lịch sử.
- Toolchain blocker đã giải quyết trong phạm vi được duyệt. Remote CI, board/HIL
  và full P2 vẫn NOT RUN; socket/HTTPS/rotation P2 chưa triển khai.

## P2.3–P2.4 — Unix listener và profile perimeter (đang xác minh)

- Thêm listener trong cùng binary/process, dùng cùng `TokenVerifier` với business
  HTTP. Router riêng chỉ `GET /internal/auth/verify`, tái sử dụng middleware và
  strict Bearer parsing; 204 khi JWT hợp lệ, 401 khi thiếu/sai, không query quyền.
  Business router TCP không đăng ký endpoint này; authorization vẫn từ DB.
- `AUTH_UNIX_SOCKET_PATH` mặc định rỗng/tắt. Socket 0660, header limit 24 KiB,
  read-header/read/write/idle timeout 2/3/3/10 giây. Shutdown chia budget hiện có;
  verifier đóng sau HTTP/socket, không đóng cache trước khi HTTP drain.
- Không tự xóa stale socket hoặc file/symlink có sẵn. Crash để lại stale socket
  cần operator xác minh và dọn đúng path trước restart; lỗi bind phải fail rõ.
- Profile fixture P2 dùng volume `auth_socket`: init job riêng chỉ chown/chmod
  directory **1883:101 / 2710**; socket kế thừa GID 101 nhờ setgid. Backend 1883
  có quyền tạo/xóa; Nginx worker thực UID/GID 101 chỉ traverse/connect, không có
  quyền ghi directory. Nginx mount read-only, không chia sẻ Auth private key.
  Init job/volume có labels và registry receipts; không sửa permissions shared.
- P2 khởi động Auth/Storage/Envoy trước backend vì initial JWKS fetch fail-closed;
  không gọi request từ Go qua Nginx để tải JWKS (tránh auth loop).
- Template riêng `config/nginx/paper-p2.conf.template`: chỉ publish HTTPS 443
  loopback/ephemeral trong fixture, CA/SAN xác minh thật, cert/key read-only.
  Không expose HTTP, không cần redirect POST login. Không đổi template mặc định.
- Business `/v1/*`, admin/metrics và `/v1/ws` qua `auth_request` tới internal
  `/_p2_auth` → Unix; bỏ body/Content-Length của subrequest, chỉ forward Bearer.
  Request chính tiếp tục verify JWT tại Go. `/v1/ws` vẫn stub 501, không runtime.
- Exceptions: `/auth/v1/*` giữ API-key/GoTrue guards (login/refresh/JWKS không cần
  user JWT), `/storage/v1/*` giữ Storage policy/URL signing, health/readiness mở;
  OPTIONS business chuyển internal named location tới Go CORS validator hiện có,
  không trả 204 cho mọi Origin, không mở signup/Admin/business-data.
- Body bound business/Auth 16 KiB; Storage giữ 50 MiB. Header/body/send timeout
  5/15/15 giây, proxy connect/send/read 2/15/55 giây; auth subrequest 1/3/3 giây.
- Nginx overwrite X-Forwarded-For bằng remote_addr và Proto bằng scheme, bỏ
  identity/role headers; Go tắt trusted proxies (không dùng các header đó làm
  principal). Các hop Envoy/Go riêng vẫn HTTP; chỉ client → Nginx là HTTPS.
- P1 metrics bổ sung aggregate Unix verification và handler duration, cache
  counters; cùng schema version 1 với optional fields. Hai verify observations
  không phải hai business requests; handler duration không là socket round-trip.
- Tests mới RED (thiếu handler/listener/profile/template) → GREEN: Auth/config/
  HTTP/server race **4 packages PASS**, Python related **218 tests PASS**. Guards
  xác nhận TCP không expose internal route và P0/P1 không tự chuyển perimeter.
- Native fixture `paper-p2-perimeter-20261024-first` **FAIL**, exit 1,
  `response_contract_failed` tại phase `paper_p2`. P2.5–P2.9 chưa triển khai tiếp.

### Điểm dừng P2.4 — HTTPS fixture certificate thiếu extensions

- Ba check đã PASS thật: Nginx có auth_request module, `nginx -t`, readback
  UID/GID/socket directory/socket permissions (1883:101/2710 và 1883:101/0660).
  Request HTTPS đầu tiên (Auth Admin create fixture user) thất bại trước HTTP
  response. Chưa xác nhận allow/deny qua Nginx hoặc fail-closed socket native.
- Python 3.13 bật `VERIFY_X509_STRICT` mặc định. Generator mới chỉ tạo CA với
  BasicConstraints và leaf với SAN/BasicConstraints, thiếu AKI/SKI/KeyUsage.
  Đây là lỗi generator certificate của runner, không phải Go RS256 verifier
  hoặc kết luận Supabase không tương thích. Không tắt strict TLS để làm test PASS.
- Chẩn đoán offline bằng certificate của chính lượt FAIL:
  `openssl verify -x509_strict -purpose sslserver -CAfile <fixture-ca> <fixture-cert>`
  exit 2: leaf thiếu Authority Key Identifier; CA thiếu Subject Key Identifier
  và KeyUsage/keyCertSign. Local socketpair TLS handshake với Python default
  verified context tái hiện **verify_code=85, Missing Authority Key Identifier**.
  Không cấp key/token qua argv hoặc in private key trong chẩn đoán.
- `run.json` khởi tạo `https_verified=true` như cờ cấu hình trước request đầu,
  dù status cuối FAIL. Cờ này **không là evidence TLS PASS**; cần sửa runner
  chỉ set true sau verified handshake thành công, tách ý định khỏi kết quả.
- Cleanup **PASS: 12 containers / 1 network / 5 volumes**, đúng resources của
  lượt này; private inputs giữ bảo vệ để chẩn đoán FAIL, không upload/commit.
  Backend development không bị thay cấu hình/recreate ở lượt P2.4 này.
- Đề xuất cần duyệt: chỉ sửa generator CA/leaf đúng KeyUsage/SKI/AKI (và EKU
  serverAuth), thêm regression default strict TLS, sửa cờ verified; giữ verify
  CA/hostname/strict và pin images. Sau đó chạy fixture mới, không tự nâng stack
  hoặc tiếp tục rotation trước khi P2.4 PASS. Artifact FAIL cũ giữ nguyên.
- Race 4 packages và Python 218 tests PASS là kết quả trước native. Formatting
  Go đã chạy trước build; vet/lint/build verification cuối của phần P2.3–P2.4
  và full native P2 chưa hoàn tất, không suy từ upgrade checkpoint trước.

### Vá strict TLS theo duyệt — đang chạy fixture mới

- Không đổi CA/deployment hoặc hạ verification. Generator mới bổ sung critical
  KeyUsage cho CA (`keyCertSign`, `cRLSign`) và leaf (`digitalSignature`,
  `keyEncipherment`), SKI/AKI cho cả hai, leaf EKU serverAuth; giữ SAN, thời hạn
  hai ngày và private key chỉ trong private inputs/Nginx.
- Tests RED tái hiện strict handshake `Missing Authority Key Identifier` và
  `https_verified=true` dù request TLS lỗi. Sau sửa: strict default client PASS
  khi đúng CA/hostname; sai hostname hoặc không trust CA bị từ chối; TLS failure
  giữ status FAIL và `https_verified=false`.
- Tách `https_configured=true` (ý định) và `https_verified=false` ban đầu. Chỉ
  sau response thật từ HTTPS helper tin CA fixture mới set verified=true.
  Artifact FAIL cũ không bị sửa, kể cả cờ bị khởi tạo sai của lượt đầu.
- Python related **220 tests PASS**, syntax compile/diff check PASS.
  `go vet ./...`, `golangci-lint run --timeout=5m` (**0 issues**),
  `go build ./...` PASS tại checkpoint P2.3–P2.4 này.
- Native mới: `paper-p2-perimeter-20261024-strict-tls`, exit 0,
  `PAPER_P2_PERIMETER_ACCEPTED`, project `ncs_7308311fbfc0`.
  HTTPS readiness 200 với CA/hostname verification thật; `https_verified=true`
  sau response. Missing/invalid/service-role/anon business tokens 401; admin 200;
  direct Go missing/invalid 401, valid 200; internal URI 404; WebSocket stub 501;
  preflight hợp lệ 204, denied Origin 403; socket upstream unavailable 500 và
  positive 501 sau restore. Supabase login/JWKS/Admin/Storage probes PASS qua HTTPS.
  Exact cleanup **12 containers / 1 network / 5 volumes**; private inputs xóa.
  Đây là PASS P2.4, không phải full P2 acceptance hoặc rotation evidence.

## P2.5–P2.8 — Implementation/checks bổ sung, chờ native đầy đủ

- Wrapper `scripts/test-paper-p2.sh [NEW_RUN_DIRECTORY]` dùng cùng lifecycle,
  registry, schema/quota, provisioning, TLS/DynSec và 2 Gateway của P1. Workload
  vẫn hữu hạn 40 message/4.000 samples + 1 replay, không benchmark capacity.
- Thêm probe Go admin tạo user thật, login RS256, PATCH profile và đọc lại DB;
  POST oversized phải 413; không sửa business handlers, schema hoặc metadata API.
- Check token expired/signature qua Nginx và direct Go; spoofed identity headers
  không cấp admin; global admin không bypass membership; viewer được grant →
  đọc → revoke → đọc bằng cùng JWT → grant lại → đọc (kỳ vọng 200/404/200).
- Storage probe private bucket/object fixture, select policy chỉ đúng user/object,
  user RS256 đọc được và user khác bị từ chối; tạo/đọc signed URL. Policy chỉ ở
  DB fixture riêng, không thay policy shared. Signed URL/token không vào artifact.
- Rotation: công bố public K1/K2, Auth vẫn một signer K1; đồng bộ static JWKS
  cho Storage; sau đó chỉ chuyển Auth signer sang K2. Auth recreate hai lần,
  Storage một lần; Go không restart. Các replacement containers có tên riêng,
  registry intent/receipts mới; xóa đúng owned ID cũ, không reset receipt để
  chấp nhận container thay thế. DB/broker/backend không recreate trong rotation.
- Check K2 login/Go/Storage và K1 overlap; Auth Admin/service-role/Storage signed
  URL vẫn hoạt động; process marker Go không đổi. Native chưa có kết quả, không
  suy các check này từ SDK. Unit bổ sung: successful refresh loại K1 thực sự
  từ chối K1 và nhận K2; hard TTL+refresh failure test cũ vẫn giữ.
- Reuse P1 CSV thêm fixed groups `unix_auth`, `unix_auth_handler`, `jwks_cache`,
  `client_https`; không label theo kid/user/token. Client đo 16 GET hợp lệ bằng
  monotonic, giữ count/sum/buckets cùng đơn vị giây; snapshot Go là cumulative,
  không chỉ riêng 16 requests. Không cộng p95 hoặc trừ đồng hồ khác process.
  Socket round-trip riêng chưa đo => null; Unix handler không đại diện round-trip.
- Các fixed auth samples gộp vào `metrics.csv` hiện có, không tạo lifecycle,
  verifier artifact hoặc stack đo mới. `run.json` giữ metadata P1 và bổ sung
  issuer/JWKS endpoint, cache TTL, HTTPS/perimeter/rotation checks.
- Native fail-closed check được thêm đối chiếu counter route `/v1/ws` qua direct
  Go: request bị socket failure không được tăng business handler count; login
  không API key phải 401, không nhầm public login/JWKS thành bỏ API-key guard.
- Python **222 tests PASS**; race **5 packages PASS** (Auth/config/HTTP/
  usermanagement/server). Vet/lint/build PASS, lint 0 issues, formatting/diff clean.
- Lượt native đầy đủ: `paper-p2-full-20261024-first`, **FAIL**, exit 1,
  `paper_p2_acceptance_failed`. Không tick checklist P2. Backend development
  vẫn HS256/Stage 5 false, không bị recreate trong lượt này.

### Điểm dừng mới — Storage private read sau rotation

- Project `ncs_26b92ecb5dcd`, artifact `run.json` status FAIL;
  check thất bại **`rotation_storage_new_and_old`**. Dừng theo yêu cầu người dùng,
  chưa tự sửa/rerun hoặc tiếp tục các check sau nó.
- PASS trước điểm dừng: toàn bộ perimeter/HTTPS/internal route/strict auth,
  socket fail-closed (kể cả business counter không tăng), login phải có API key;
  expiry/bad signature qua hai đường HTTP; admin không bypass membership;
  spoof identity header bị từ chối; Go Auth Admin tạo user 201, user login RS256;
  PATCH body/profile DB 200, body limit 413; membership cùng JWT **200/404/200**.
- Storage trước rotation PASS: private bucket/object upload bằng service-role,
  user RS256 đọc object 200 đúng nội dung; user khác bị từ chối 400, không leak
  object; signed URL tạo/đọc 200. Không suy những PASS này sang hậu rotation.
- Rotation đã PASS từng bước: JWKS overlap K1+K2, K1 còn dùng được, login mới
  nhận K2, Go nhận K2 và còn nhận K1. **Chưa nghiệm thu toàn bộ rotation** vì
  ít nhất một lượt Storage private-read với K1/K2 không trả 200.
- Runner hiện dùng `all(...)` cho hai lượt read, không lưu status riêng hoặc
  error code; có short-circuit nên chưa biết lượt nào fail/lượt nào chưa chạy.
  Không kết luận JWT/key policy sai hoặc chỉ là Envoy/DNS warmup khi chưa có
  response cụ thể. Container health readiness chưa chứng minh data route qua
  Envoy đã sẵn sàng sau recreate.
- Đối chiếu private config offline (không print key material): cả overlap/K2
  đều đúng một Auth signer, Storage có 2 public keys không private/symmetric;
  public K1 khớp nguyên bản, public kid set Auth/Storage khớp. Điều này chỉ xác
  nhận generated config, không chứng minh runtime Storage đã nhận/verify đúng.
- NOT RUN sau điểm dừng: signed URL/service-role/Auth Admin hậu rotation,
  process continuity check cuối, 16-request latency samples, uplink/replay/
  temporal workload P2, CSV/resources hoàn chỉnh. P1 regression trước đó không
  thay kết quả P2 này.
- Exact cleanup verified **15 containers / 1 network / 5 volumes**; bao gồm
  3 replacement containers của rotation, chưa tạo Gateway outbox workload.
  Private diagnostics giữ bảo vệ để điều tra, không upload/commit.
- Đề xuất cần duyệt: ghi từng read K1/K2 với status/error category an toàn,
  thêm application-route readback hậu recreate, chạy fixture mới để xác định
  component/response; chỉ retry lỗi readiness/transport tạm thời có timeout,
  không retry/mask lỗi auth/policy và không nới JWT/RLS/TLS. Giữ nguyên FAIL cũ.

### Chẩn đoán Storage theo duyệt — đang chờ lượt mới

- Đã thay short-circuit request generator bằng hai probe riêng K1/K2; ngay cả
  K1 FAIL vẫn thực hiện K2 trước khi quyết định acceptance. Mỗi nhãn có status,
  danh sách attempt hữu hạn, HTTP status và category cố định; không dùng kid
  tùy ý làm metric label hoặc ghi token/API key/object content vào artifact.
- Sau recreate Storage ở bước overlap, readiness qua HTTPS/Nginx/Envoy kiểm
  service-role bucket API và service-role đọc đúng object fixture. Đây chỉ là
  infrastructure readback, không thay test user K1/K2 hoặc nới RLS. Nếu readiness
  FAIL, các probe phụ thuộc tiếp sau giữ NOT_RUN, không chuyển thành PASS.
- Chỉ chờ retry 502/503/504 hoặc Timeout/ConnectionRefused/ConnectionReset/
  RemoteDisconnected: tối đa 20 attempts và deadline 30 giây mỗi probe, timeout
  mỗi request tối đa 2 giây và không vượt budget còn lại. 400/401/403, lỗi TLS,
  500 hoặc HTTP 200 sai nội dung không được retry để che failure.
- Response lỗi chi tiết chỉ lưu file 0600 trong private inputs của fixture,
  không artifact/upload/commit; artifact chỉ fixed category và một allowlist
  Storage error codes. Private inputs lượt PASS xóa như lifecycle cũ; lượt FAIL
  giữ bảo vệ để chẩn đoán. Không đổi Go JWT, Storage RLS, TLS context hay images.
- RED: thiếu helper riêng cho readiness/K1/K2 → GREEN, **225 Python tests PASS**;
  guards gồm transient 503→200, auth/policy/TLS/content fail không retry và K1
  FAIL không short-circuit K2. Syntax compile/diff check PASS.
- Native mới: `paper-p2-full-20261024-storage-diagnostics`, **PASS**, exit 0,
  `PAPER_P2_ACCEPTED`, project `ncs_60a5e67bb655`, `full_p2_accepted=true`.

### Checkpoint dừng theo yêu cầu — kết quả fixture mới

- Storage bucket API readiness: **503 × 4 → 200**, trong retry budget; object
  API readiness bằng service-role **200 ngay lần đầu**, đúng object fixture.
- Read riêng **K1: 200 lần đầu**; **K2: 503 → 200**. Không có auth/policy/TLS
  failure bị retry. Lượt này chứng minh có upstream-unavailable tạm thời trong
  cửa sổ rotation; không có response riêng của lượt FAIL trước nên không gán
  chắc chắn mọi failure lịch sử cùng một nguyên nhân hoặc đổi FAIL cũ thành PASS.
- Rotation PASS: Auth signer K1→K2, K1 overlap, Storage nhận static public K1/K2;
  Auth recreate 2 lần, Storage 1 lần, **Go không restart**. Signed URL,
  service-role operation và Go Auth Admin hậu rotation đều PASS theo runner.
- Uplink/HTTP regression P2: **40 message mới / 4.000 samples**, DB delta
  ledger/raw/temporal = 40/4.000/40, configuration = 0. Replay byte-exact 939 B
  nhận application ACK và không tăng dữ liệu; metrics authorization statuses
  **401/401/403/200**. Temporal read và quyền chéo Gateway PASS.
- Auth/perimeter latency samples và các artifacts `run.json`, `metrics.csv`,
  `resources.csv`, `spike.json`, `startup-summary.json` đã được ghi và tổng hợp
  chi tiết tại mục P2.9; không tự bịa percentile chính xác từ coarse histogram.
- Exact cleanup verified **19 containers / 1 network / 7 volumes**; private
  inputs lượt PASS đã xóa. 226 Python tests PASS trước khi fixture kết thúc;
  không chạy thêm tests/build sau yêu cầu dừng. JWT/RLS/TLS không nới lỏng.
- **Đã dừng.** Không tự chạy lại, sửa code tiếp, commit/push, chuyển P3/Stage 5
  hoặc hoàn tất P2.9/navigation/checklist/AGENTS checkpoint. Các việc bàn giao
  cuối này để lại cho yêu cầu tiếp theo; native PASS không tự đồng nghĩa mọi
  hạng mục tài liệu P2 đã hoàn tất.

### Nghiệm thu tuần tự P2.5a — Bằng chứng tương thích Auth Admin và Storage

Mục này nghiệm thu phạm vi **P2.5a** (các mục 1 đến 4 của P2.5 trong kế hoạch chi tiết `P2_detail_plan.md`): xác nhận tính tương thích của Go Auth Admin API, user login RS256, profile DB, Storage service-role/infrastructure, Storage user RS256 private access, phân quyền RLS, và signed URL. Cơ chế và vòng đời key rotation chi tiết (mục 5 đến 7 của P2.5) thuộc nhiệm vụ tuần tự kế tiếp (P2.5b).

#### 1. Định danh lượt chạy và ngữ cảnh thực thi
- **Mã định danh (Run ID):** `ncs_60a5e67bb655` (lượt chạy native `paper-p2-full-20261024-storage-diagnostics`).
- **Trạng thái:** `status: "PASS"`, `full_p2_accepted: true`, `cleanup_verified: true`.
- **Tài nguyên dọn dẹp chính xác:** 19 containers, 1 network, 7 volumes (toàn bộ private inputs chứa khóa ký và credentials đã được dọn sạch tự động).
- **Mã nguồn và trạng thái git:** Commit `926f642b71bf6729da81209926f25c79a05e6042`, branch `feat/paper-requirement`, `dirty: true` (worktree có uncommitted changes).
- **Ngữ cảnh:** Đây là kết quả fixture native cục bộ trên môi trường dirty shared checkout, **không phải clean committed CI run**, không tuyên bố production readiness hay zero downtime.

#### 2. Đối chiếu yêu cầu và bằng chứng kiểm tra (Requirement-to-Evidence Mapping)
Cần phân định rõ ràng giữa các mã trạng thái được lưu trực tiếp trong artifact và các mã trạng thái suy ra từ assertion thành công trong mã nguồn:
- **Mã HTTP lưu trực tiếp trong artifact (`run.json`):** Chỉ các check `go_auth_admin_create` (201), `business_patch_preserves_body` (200), `storage_private_create` (200), `storage_upload_fixture_object` (200), `storage_rs256_private_read` (200), `storage_private_other_user_denied` (400), `storage_signed_url_create` (200), `storage_signed_url_read` (200), `rotation_storage_k1` (200) và `rotation_storage_k2` (200) có trường `http_status` tồn tại trong artifact.
- **Mã HTTP suy ra từ assertion nguồn (source assertion):** Các check `business_body_limit`, `rotation_legacy_service_survives`, `rotation_auth_admin_survives`, `rotation_signed_url_survives` (cùng các check `rotation_storage_bucket_api_ready`, `rotation_storage_object_api_ready`, `rotation_storage_new_and_old`, `new_user_login_rs256`) **chỉ ghi nhận `status: "PASS"` trong `run.json`, hoàn toàn không có trường `http_status`**. Các giá trị HTTP mong đợi (như 413, 200, 201) được xác lập bởi assertion tường minh trong `scripts/tests/paper_p2_support.py` để quyết định PASS; tuyệt đối không coi hay suy diễn rằng các trường `http_status` này có trong artifact.

| Yêu cầu P2.5 (Mục 1–4) | Tên check trong `run.json` | Kết quả quan sát | Chi tiết hành vi xác thực |
| --- | --- | --- | --- |
| **Go Auth Admin tạo user** (P2.5.2) | `go_auth_admin_create` | `PASS` (`http_status: 201` trong artifact) | Go Admin API (`POST /v1/admin/users`) dùng JWT platform admin RS256 gọi Auth Admin adapter qua Envoy bằng service-role key, tạo user thật thành công và trả về `id`. |
| **Login user mới nhận RS256** (P2.5.2) | `new_user_login_rs256` | `PASS` (chỉ lưu status; source assertion: `alg=="RS256"`) | User mới tạo đăng nhập thành công qua `/auth/v1/token?grant_type=password` với legacy anon key; token nhận được có header `alg="RS256"`. |
| **Cập nhật profile DB** (P2.5.2) | `business_patch_preserves_body` | `PASS` (`http_status: 200` trong artifact) | `PATCH /v1/admin/users/{uid}` cập nhật `full_name`; câu lệnh SQL trực tiếp xác nhận bảng `profiles` ghi nhận đúng `full_name='P2 updated fixture'` cho `uid` tương ứng. |
| **Giới hạn body proxy/perimeter** | `business_body_limit` | `PASS` (chỉ lưu status; source assertion: HTTP 413) | `POST /v1/admin/users` với payload vượt giới hạn (> 16 KiB) bị từ chối bởi giới hạn 16 KiB tại perimeter Nginx. Đây là kiểm tra giới hạn kích thước body ở tầng proxy/perimeter, không phải bằng chứng độc lập cho thấy Go tự thực thi giới hạn body của chính nó; giới hạn của Go được duy trì tách biệt trong mã nguồn. |
| **Storage service-role tạo bucket** (P2.5.3) | `storage_private_create` | `PASS` (`http_status: 200` trong artifact) | Storage chấp nhận legacy service-role key để tạo private bucket tạm `p2-private`. |
| **Storage service-role upload object** (P2.5.3) | `storage_upload_fixture_object` | `PASS` (`http_status: 200` trong artifact) | Storage chấp nhận legacy service-role key để upload một JSON object fixture nhỏ đã biết (`probe.json`). |
| **User RS256 truy cập private object** (P2.5.3) | `storage_rs256_private_read` | `PASS` (`http_status: 200` trong artifact) | User RS256 gửi request `GET /storage/v1/object/authenticated/p2-private/probe.json` (kèm anon key); Storage xác minh chữ ký RS256 qua `JWT_JWKS` và áp dụng RLS policy, trả về nội dung khớp với đối tượng fixture nhỏ đã biết. *Không suy diễn kết quả này từ việc service-role list bucket.* |
| **Từ chối truy cập từ user khác** (P2.5.3) | `storage_private_other_user_denied` | `PASS` (`http_status: 400` trong artifact) | User authenticated khác (token khác `uid`) gửi cùng request bị Storage từ chối với HTTP 400 (thuộc nhóm mã lỗi mong muốn 400/403/404), không rò rỉ nội dung object. |
| **Tạo signed URL** (P2.5.4) | `storage_signed_url_create` | `PASS` (`http_status: 200` trong artifact) | User RS256 tạo signed URL cho private object qua `POST /storage/v1/object/sign/...` với `expiresIn=120`; nhận đường dẫn ký hợp lệ `/object/sign/p2-private/probe.json?...`. |
| **Đọc qua signed URL** (P2.5.4) | `storage_signed_url_read` | `PASS` (`http_status: 200` trong artifact) | Request `GET` trực tiếp tới URL đã ký mà **không cần mang token** (`token=''`) trả về HTTP 200 và nội dung khớp với đối tượng fixture nhỏ đã biết. |
| **Service-role hạ tầng sau rotation** | `rotation_legacy_service_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 200) | Thao tác `GET /storage/v1/bucket` bằng service-role key tiếp tục thành công sau rotation. |
| **Auth Admin sau rotation** | `rotation_auth_admin_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 201) | Go Admin API tiếp tục tạo user mới qua Auth Admin thành công sau khi hoàn tất rotation. |
| **Storage readiness sau rotation** | `rotation_storage_bucket_api_ready`<br>`rotation_storage_object_api_ready` | `PASS`<br>`PASS` (chỉ lưu status; source assertion: readiness đạt 200) | Service-role bucket API đạt 200 sau retry (503 × 4 → 200); object API đạt 200 ngay lần đầu. |
| **Storage đọc private sau rotation** | `rotation_storage_k1`<br>`rotation_storage_k2`<br>`rotation_storage_new_and_old` | `PASS` (`http_status: 200` trong artifact)<br>`PASS` (`http_status: 200` trong artifact)<br>`PASS` (chỉ lưu status; source assertion: pair đạt 200) | Cả token K1 (còn hạn) và token K2 mới đều đọc được private object (K1: 200 lần đầu; K2: 503 → 200); check cặp hoàn tất PASS. |
| **Signed URL sau rotation** | `rotation_signed_url_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 200) | Signed URL đã tạo trước rotation vẫn đọc được nội dung sau rotation (HMAC secret không đổi). |

#### 3. Phân định probe hạ tầng và bảo toàn phạm vi
- **Không mở rộng sang P9 media workflow:** Probe Storage chỉ sử dụng một JSON object fixture nhỏ đã biết (`probe.json`) trong bucket private tạm thời (`p2-private`). Hoàn toàn không thêm endpoint metadata media, không cấp quyền upload cho Gateway, không hỗ trợ multipart/resumable upload và không cấu hình pipeline xử lý media của P9.
- **Không can thiệp RLS dùng chung (Shared RLS):** Policy RLS (`p2_fixture_read`) được tạo độc lập trên đối tượng fixture trong DB của runner (`CREATE POLICY ... USING (bucket_id='p2-private' AND name='probe.json' AND (SELECT auth.uid())=:uid)`). Policy này nằm hoàn toàn trong instance DB tạm của fixture và được dọn dẹp sạch sẽ khi teardown; không sửa đổi các policy RLS mặc định của Supabase hay schema ứng dụng dùng chung.
- **Phân định kiểm tra cấu hình (Configuration) và kết quả runtime (Runtime):**
  - *Kiểm tra cấu hình/preflight có giới hạn (`validate_perimeter`):* Runner thực hiện preflight check đối chiếu cấu hình compose và biến môi trường được tạo cho các container, xác nhận trường private key exponent `d` của RSA signing key chỉ có trong cấu hình môi trường của service `supabase-auth` và không xuất hiện trong cấu hình được sinh cho các service khác (`backend`, `nginx`, `supabase-storage`). Đây là kết quả của kiểm tra tĩnh có giới hạn trong runner, không phải chứng chỉ phần cứng tuyệt đối.
  - *Cấu hình Go:* Chỉ nhận `SUPABASE_JWKS_URL` (lấy public JWKS qua Envoy) và `SUPABASE_JWT_MODE=rs256`; không có `SUPABASE_JWT_SECRET`.
  - *Cấu hình Storage:* Chỉ nhận danh sách public keys tĩnh qua `JWT_JWKS` và secret HMAC legacy (`AUTH_JWT_SECRET`) cho API keys/URL signing; không nhận Auth private RSA key.
  - *Cấu hình Nginx:* Chỉ nhận SSL certificate/key cho HTTPS ngoại vi và mount socket Unix nội bộ; không truy cập key của Auth.
  - *Kết quả runtime (`public_jwks`):* Endpoint `GET /auth/v1/.well-known/jwks.json` trả về HTTP 200 với đúng 1 public RSA key (`key_count: 1`), không chứa bất kỳ trường private RSA nào (`d`, `p`, `q`, `dp`, `dq`, `qi`) hay symmetric secret (`k`).

#### 4. Bảo toàn bằng chứng lịch sử và ghi nhận lỗi tạm thời (Transient 503)
- **Bảo toàn lịch sử thất bại:** Lượt chạy `paper-p2-full-20261024-first` (`ncs_26b92ecb5dcd`) bị `FAIL` tại check `rotation_storage_new_and_old` vẫn được bảo toàn nguyên vẹn là một thất bại lịch sử. Trong lượt chạy đó, runner sử dụng `all(...)` gộp và dừng sớm (short-circuit), không ghi nhận mã lỗi hay HTTP status riêng lẻ cho từng lần đọc K1/K2.
- **Quan sát lỗi tạm thời tại lượt chạy thành công:** Trong quy trình rotation của fixture, `supabase-auth` được recreate hai lần và `supabase-storage` được recreate một lần; **`supabase-envoy` hoàn toàn không bị recreate**. Trong lượt chạy chẩn đoán `ncs_60a5e67bb655`, hệ thống ghi nhận các phản hồi tạm thời `503 upstream_unavailable` xuất hiện trên tuyến đường đi từ Envoy sang Storage trong quá trình rotation (khi Storage được recreate):
  - Bucket API readiness: 4 lần gặp `503` trước khi đạt `200` ở lần thử thứ 5 (trong ngân sách 20 lần thử/30s).
  - Object API readiness: đạt `200` ngay lần đầu.
  - Đọc K1: đạt `200` ngay lần đầu.
  - Đọc K2: lần 1 gặp `503`, lần 2 đạt `200`.
- **Ranh giới suy luận:** Các phản hồi 503 quan sát được trên tuyến Envoy-to-Storage không chứng minh bản thân Envoy bị restart, không khẳng định một nguyên nhân cấp thấp (low-level warmup cause) đã được chứng minh ngoài chính quan sát phản hồi đó, và không xác lập nguyên nhân chính xác của thất bại ở lượt chạy trước đó. Kết quả `FAIL` lịch sử được giữ nguyên vẹn.

#### 5. Hạn chế và kết luận nghiệm thu P2.5a
- **Hạn chế kỹ thuật:**
  - Storage phụ thuộc vào cấu hình static `JWT_JWKS`; khi thay đổi hoặc xoay khóa cần cập nhật cấu hình và recreate/reload container Storage (không tự động cập nhật qua remote JWKS như Go).
  - Signed URL của Storage sử dụng cơ chế HMAC độc lập với access token RS256; rotation RS256 không làm vô hiệu signed URL đã phát khi secret HMAC chưa đổi.
  - Đã quan sát thấy các phản hồi tạm thời 503 trên tuyến đường Envoy-to-Storage trong quá trình rotation khi recreate Storage; hệ thống cần retry budget hữu hạn trong các thao tác vận hành tương tự.
- **Kết luận:** Phạm vi P2.5a đạt tiêu chuẩn nghiệm thu tuần tự (Sequential Acceptance PASS). Các kiểm tra tương thích Auth Admin và Storage đều có bằng chứng runtime và cấu hình xác thực từ artifact an toàn `run.json` cùng các assertions tường minh của runner. Dừng tại đây để bàn giao sang nhiệm vụ tuần tự tiếp theo (P2.5b - Key rotation lifecycle).

### Nghiệm thu tuần tự P2.5b — Vòng đời xoay khóa (Key Rotation) và bộ đệm có giới hạn (Bounded Cache)

Mục này nghiệm thu phạm vi **P2.5b** (các mục 5 đến 7 của P2.5 trong kế hoạch chi tiết `P2_detail_plan.md`): xác nhận vòng đời xoay khóa (key rotation lifecycle), chuyển quyền ký từ K1 sang K2, kiểm tra token trong giai đoạn overlap, sự tồn tại độc lập của tiến trình Go, cơ chế cập nhật tập khóa tĩnh của Storage, chính sách bộ đệm JWKS có giới hạn (bounded cache policy) và bằng chứng kiểm thử đơn vị về việc từ chối các khóa đã hết hạn hoặc bị loại bỏ khi chu kỳ làm mới thất bại. Đây là công việc tuần tự tiếp nối sau khi P2.5a đã nghiệm thu tính tương thích của Auth Admin và Storage.

#### 1. Định danh lượt chạy và ngữ cảnh thực thi
- **Mã định danh (Run ID):** `ncs_60a5e67bb655` (lượt chạy native `paper-p2-full-20261024-storage-diagnostics`).
- **Trạng thái:** `status: "PASS"`, `full_p2_accepted: true`, `cleanup_verified: true`.
- **Dữ liệu vòng đời ghi nhận trong `run.json`:**
  - `rotation.status`: `"PASS"`
  - `rotation.k1`: `"p2-k1-33c88aeafc81531a"`
  - `rotation.k2`: `"p2-k2-9c7af65a0272eea3"`
  - `rotation.overlap`: `true`
  - `rotation.go_restarted`: `false`
  - `rotation.auth_recreates`: `2`
  - `rotation.storage_recreates`: `1`
  - `rotation.static_storage_jwks_updated`: `true`
  - `rotation.cache_ttl_seconds`: `60`
- **Ngữ cảnh:** Kết quả native fixture trên dirty shared worktree (`926f642b71bf6729da81209926f25c79a05e6042`), không phải clean CI; không tuyên bố zero downtime, production readiness, hay thu hồi JWT tức thì.

#### 2. Vòng đời xoay khóa và các bước chuyển dịch (Key Rotation Lifecycle)
Quy trình xoay khóa trong fixture native được thực hiện theo trình tự có kiểm soát nhằm đảm bảo tính liên tục của hệ thống và tuân thủ nguyên tắc chỉ có đúng một signing key tại một thời điểm:
1. **Giai đoạn K1 ban đầu:** Service `supabase-auth` khởi động với khóa RSA K1 (chứa private key `d` để ký token) và secret HMAC legacy. User đăng nhập nhận access token RS256 có `kid=K1`.
2. **Giai đoạn Overlap (K1 + K2) — Công bố khóa chuyển tiếp:**
   - Runner sinh khóa RSA K2 mới. Hàm `rotation_documents` tạo cấu hình chuyển tiếp:
     - `overlap` cho Auth: `[k1 (private signing), k2_public (verify-only), legacy secret]`. Tại bước này, **K1 là signing key duy nhất**; K2 chỉ được công bố public key để xác minh, không có quyền ký.
     - `public` cho Storage: `{'keys': [k1_public, k2_public]}` chứa public key của cả K1 và K2.
   - Recreate dịch vụ lần 1: Runner thực hiện recreate `supabase-auth` (lần 1) với biến `GOTRUE_JWT_KEYS` mang cấu hình `overlap`, và recreate `supabase-storage` (lần 1) với biến `JWT_JWKS` mang cấu hình `public`.
   - **Envoy và Go process hoàn toàn không bị recreate/restart.**
   - Endpoint public JWKS (`GET /auth/v1/.well-known/jwks.json`) qua Envoy được xác minh trả về cả hai khóa `{k1, k2}` (`rotation_overlap_public_keys`).
   - Token K1 cũ vẫn truy cập được Go API thành công (`rotation_overlap_k1`).
3. **Giai đoạn chuyển quyền ký sang K2 (Signing Switch to K2):**
   - Hàm `rotation_documents` tạo cấu hình ký mới:
     - `switched` cho Auth: `[k1_public (verify-only), k2 (private signing), legacy secret]`. Tại bước này, quyền ký được chuyển hoàn toàn sang K2; **K2 là signing key duy nhất**, còn K1 chỉ giữ vai trò public key để verify token cũ còn hạn.
   - Recreate dịch vụ lần 2: Runner chỉ recreate `supabase-auth` (lần 2) với cấu hình `switched`. Dịch vụ `supabase-storage` không cần recreate thêm vì đã nhận đủ public keys ở bước trước; `supabase-envoy` và `backend` tiếp tục giữ nguyên.
4. **Login mới nhận K2 và xác minh tại Go không cần khởi động lại:**
   - User đăng nhập qua `/auth/v1/token?grant_type=password` và nhận token mới (`token2`) có header `kid=K2` (`rotation_new_signer`).
   - Request mang `token2` được gửi tới Go (`GET /v1/digital-twins/...`) và được xác minh thành công (HTTP 200). Bằng chứng native xác lập rằng Go chấp nhận K2 mà không cần khởi động lại, nhưng không chỉ định chính xác liệu request cụ thể đó đã kích hoạt refresh hay worker nền định kỳ đã nạp K2 trước đó; các đường kích hoạt này (refresh khi gặp `kid` mới có giới hạn tốc độ hoặc background refresh theo chu kỳ) được xác lập dưới dạng chính sách trong mã nguồn (source policy), tránh suy diễn một vết nhân quả trên từng request riêng lẻ trong môi trường chạy native.
5. **K1 Overlap Verification tại Go:**
   - Ngay sau khi Go đã refresh và chấp nhận K2, request mang token K1 cũ (`user_token`) tiếp tục được gửi tới Go và được xác minh thành công (`rotation_go_refresh_and_k1_overlap`), chứng minh Go chấp nhận đồng thời cả K1 và K2 trong cửa sổ overlap khi cả hai khóa còn được công bố trong JWKS.
6. **Xác nhận tính nguyên vẹn của tiến trình Go:**
   - Bộ đo `process_instance` từ `/v1/admin/metrics` trước và sau toàn bộ quá trình rotation khớp nhau tuyệt đối (`rotation_same_go_process`), chứng minh Go tự động thích ứng với tập khóa mới qua cơ chế bộ đệm/JWKS mà không cần khởi động lại.

#### 3. Cơ chế Storage và sự độc lập với Go JWKS Cache
- **Cấu hình tĩnh trong profile kiểm thử (Static Key Set):** Trong profile được kiểm thử, Supabase Storage (`v1.74.0`) sử dụng danh sách public keys tĩnh qua biến môi trường `JWT_JWKS` dạng JSON string tại thời điểm khởi động. Báo cáo này không đưa ra khẳng định khái quát rằng Storage v1.74.0 không thể hỗ trợ bất kỳ cơ chế remote JWKS nào trong các ngữ cảnh khác, vì khẳng định rộng đó chưa được xác minh tại đây.
- **Yêu cầu cập nhật cấu hình và recreate riêng biệt:** Để Storage có thể xác minh được token mang khóa mới K2 đồng thời vẫn chấp nhận token cũ K1, runner phải cập nhật tường minh biến `JWT_JWKS` chứa cả hai public keys và thực hiện recreate container Storage (`supabase-storage`).
- **Phân định rõ ràng:** Việc Go tự động làm mới JWKS qua HTTP endpoint hoàn toàn **không** đồng nghĩa hay làm cho Storage tự động cập nhật. Đây là hai cơ chế xác thực độc lập: Go sử dụng client JWKS động có cache, còn Storage dựa trên static configuration injection.

#### 4. Chính sách Hard Freshness và Bounded Cache của Go Verifier
Mã nguồn Go verifier (`src/internal/auth/rs256.go`) hiện thực hóa một cơ chế bộ đệm nghiêm ngặt, kết hợp giữa SDK `jwkset`/`keyfunc` và cấu trúc wrapper `freshJWKS` để ngăn ngừa rủi ro stale key và cạn kiệt tài nguyên:
- **Hard Freshness TTL (60 giây):**
  - Cấu hình paper thiết lập `CacheTTL = 60s`.
  - Wrapper `freshJWKS` lưu giữ mốc thời gian cập nhật cuối cùng `updated`. Trong hàm `KeyRead`, nếu `now().Sub(updated) >= ttl`, verifier lập tức coi khóa đã hết hạn, trả về `jwkset.ErrKeyNotFound` và tăng bộ đếm `misses`.
  - Cơ chế hard expiry này đảm bảo rằng các khóa đã hết hạn không thể tiếp tục được chấp nhận vô hạn khi chu kỳ làm mới thất bại, bất chấp việc upstream JWKS phản hồi header HTTP có giá trị lớn hơn (ví dụ `Cache-Control: public, max-age=600`) hoặc khi endpoint JWKS bị ngắt kết nối; cơ chế này tập trung ngăn chặn việc chấp nhận khóa quá hạn thay vì đảm bảo việc giải phóng vật lý đối tượng khóa trong bộ nhớ.
- **Background Refresh Interval (TTL / 2):** SDK được cấu hình `RefreshInterval: cfg.CacheTTL / 2` (tương đương 30 giây). Cứ sau 30 giây, một worker nền cố gắng cập nhật tập khóa mới từ remote JWKS endpoint.
- **Giới hạn kích thước phản hồi và số lượng khóa (Bounds):**
  - Kích thước tối đa của JWKS payload là 64 KiB (`maxJWKSBytes = 64 * 1024`), kích thước header tối đa 8192 bytes.
  - Số lượng khóa được kiểm soát chặt chẽ trong khoảng từ 1 đến 8 khóa (`len(set.Keys) == 0 || len(set.Keys) > 8` bị từ chối).
  - Khóa phải có `kty="RSA"`, `alg="RS256"`, `use="sig"`, `key_ops=["verify"]`. Cấm tuyệt đối các trường private RSA (`d`, `p`, `q`, v.v.), symmetric key (`k`), hoặc chứng chỉ ngoại vi (`x5u`, `x5c`).
- **Giới hạn đồng thời và Cooldown:**
  - Custom HTTP transport sử dụng kênh `busy chan struct{}, 1` để đảm bảo chỉ có duy nhất 1 request HTTP tải JWKS diễn ra tại một thời điểm.
  - Khi gặp `kid` chưa biết (unknown kid), SDK áp dụng bộ giới hạn tốc độ `rate.NewLimiter(rate.Every(cfg.RefreshCooldown), 1)` với thời gian cooldown từ 5 giây đến 1 phút, ngăn chặn tấn công DoS làm nghẽn hạ tầng xác thực.
  - HTTP timeout cho mỗi lần truy vấn JWKS bị chặn cứng trong khoảng `[100ms, 10s]`.
- **Không fallback thuật toán (No HS256 Fallback):** Parser cấu hình nghiêm ngặt `jwt.WithValidMethods([]string{"RS256"})`. Nếu endpoint JWKS không thể truy cập hoặc gặp lỗi xác thực, hệ thống trả về lỗi và từ chối token, tuyệt đối không tự động fallback sang HS256 hay bất kỳ thuật toán nào khác.
- **Bằng chứng số đo native:** Snapshot metrics tại `after_auth_requests` trong `run.json` ghi nhận: `hits = 95`, `misses = 1`, `refresh_success = 2`, `refresh_failure = 1`.

#### 5. Bằng chứng kiểm thử tập trung (Focused Go Tests)
Để chứng minh các đặc tính mà môi trường native không thực hiện (như xóa bỏ khóa hoặc sập endpoint JWKS kéo dài), các kiểm thử đơn vị có race detector đã được thực thi độc lập:
- **Lệnh kiểm thử đã chạy (thực thi từ `src/`):**
  ```sh
  go test -race ./internal/auth -run '^TestRS256(HardExpiryAndRotation|RemovedKeyCannotSurviveSuccessfulRefresh)$' -count=1
  ```
- **Kết quả thực tế ghi nhận:**
  ```text
  ok  	iot-platform/internal/auth	1.319s
  ```
  (Toàn bộ 2 tests PASS, không có lỗi race condition hay failure).
- **Ý nghĩa kỹ thuật của từng bài test:**
  - `TestRS256HardExpiryAndRotation`: Giả lập endpoint JWKS trả về `max-age=600`, sau đó chuyển sang trạng thái lỗi (HTTP 503). Khi đồng hồ giả lập tiến thêm 61 giây (> hard TTL 60s), request xác thực bằng token K1 bị từ chối với lỗi `ErrInvalidToken`. Bài test chứng minh rằng: **Các khóa đã hết hạn không thể tiếp tục được chấp nhận vô hạn khi chu kỳ làm mới thất bại, bất chấp header Cache-Control từ upstream.**
  - `TestRS256RemovedKeyCannotSurviveSuccessfulRefresh`: Khởi đầu với JWKS chứa `{k1}`, sau đó cập nhật JWKS chỉ còn `{k2}` (K1 bị loại bỏ). Sau khi làm mới thành công (tiến đồng hồ 61s), token ký bằng K1 lập tức bị từ chối (`ErrInvalidToken`), trong khi token ký bằng K2 được chấp nhận thành công. Bài test chứng minh rằng: **Sau một lần refresh thành công mà tập khóa mới không còn K1, K1 lập tức bị vô hiệu hóa và K2 được sử dụng bình thường.**

#### 6. Phân định ranh giới giữa Runtime Overlap và Key Removal
Cần phân biệt ranh giới kỹ thuật rõ ràng giữa lượt chạy native và các bài test đơn vị:
- **Lượt chạy native (`run.json`):** Chỉ kiểm tra kịch bản **overlap (chuyển tiếp đồng thời)**: K1 và K2 cùng tồn tại trong public JWKS của Auth và Storage. Token K1 còn hạn và token K2 mới đều được chấp nhận đồng thời (`rotation.overlap = true`). Lượt chạy native **không** thực hiện thao tác xóa K1 khỏi JWKS.
- **Kiểm thử đơn vị (`rs256_test.go`):** Đảm nhiệm việc chứng minh hành vi khi K1 bị loại bỏ hoàn toàn (`TestRS256RemovedKeyCannotSurviveSuccessfulRefresh`) và khi cache hết hạn trong điều kiện mất kết nối (`TestRS256HardExpiryAndRotation`). Tuyệt đối không suy diễn rằng lượt chạy native đã kiểm tra việc thu hồi hay xóa bỏ K1 khỏi hệ thống runtime.

#### 7. Đối chiếu yêu cầu và bằng chứng kiểm tra (Requirement-to-Evidence Mapping)
Phân định rõ ràng giữa các mã trạng thái được lưu trực tiếp trong artifact và các mã trạng thái suy ra từ assertion thành công trong mã nguồn:
- **Mã HTTP lưu trực tiếp trong artifact (`run.json`):** Chỉ các check `rotation_storage_k1` (`http_status: 200`) và `rotation_storage_k2` (`http_status: 200`) có trường `http_status` tồn tại trong artifact.
- **Mã HTTP suy ra từ assertion nguồn (source assertion):** Các check `rotation_storage_bucket_api_ready`, `rotation_storage_object_api_ready`, `rotation_overlap_public_keys`, `rotation_overlap_k1`, `rotation_new_signer`, `rotation_go_refresh_and_k1_overlap`, `rotation_storage_new_and_old`, `rotation_signed_url_survives`, `rotation_legacy_service_survives`, `rotation_auth_admin_survives`, `rotation_same_go_process` **chỉ ghi nhận `status: "PASS"` trong `run.json`, hoàn toàn không có trường `http_status`**.

| Yêu cầu P2.5 (Mục 5–7) | Tên check trong `run.json` | Kết quả quan sát | Chi tiết hành vi xác thực |
| --- | --- | --- | --- |
| **Công bố K1 + K2 trong JWKS** (P2.5.5) | `rotation_overlap_public_keys` | `PASS` (chỉ lưu status; source assertion: `{k['kid']} == {k1, k2}`) | Endpoint `GET /auth/v1/.well-known/jwks.json` qua Envoy công bố cả hai public keys K1 và K2 trong giai đoạn chuyển tiếp; không có trường private. |
| **Token K1 sống sót trong overlap** (P2.5.5) | `rotation_overlap_k1` | `PASS` (chỉ lưu status; source assertion: HTTP 200) | Request `GET /v1/admin/metrics` mang token K1 cũ vẫn được Go xác minh thành công (HTTP 200) khi cấu hình overlap có hiệu lực. |
| **Chuyển quyền ký sang K2** (P2.5.5) | `rotation_new_signer` | `PASS` (chỉ lưu status; source assertion: `kid == k2`) | User đăng nhập mới nhận access token RS256 có header `kid` khớp chính xác với K2; K2 là signing key duy nhất. |
| **Go refresh và K1 overlap** (P2.5.5) | `rotation_go_refresh_and_k1_overlap` | `PASS` (chỉ lưu status; source assertion: K2 và K1 đều 200) | Token K2 mới được Go xác minh thành công mà không cần restart; token K1 cũ còn hạn tiếp tục được xác minh thành công trên cùng Go process trong cửa sổ overlap. |
| **Storage đọc bằng K1 sau rotation** (P2.5.6) | `rotation_storage_k1` | `PASS` (`http_status: 200` trong artifact) | Token K1 đọc được private object trong Storage sau rotation (đạt 200 ngay lần thử đầu). |
| **Storage đọc bằng K2 sau rotation** (P2.5.6) | `rotation_storage_k2` | `PASS` (`http_status: 200` trong artifact) | Token K2 đọc được private object trong Storage sau rotation (gặp 503 ở attempt 1, đạt 200 ở attempt 2). |
| **Cặp đọc Storage K1 và K2** (P2.5.6) | `rotation_storage_new_and_old` | `PASS` (chỉ lưu status; source assertion: pair đạt 200) | Cả hai token K1 và K2 đều đọc được dữ liệu private tương ứng với RLS policy. |
| **Signed URL sống sót** (P2.5.6) | `rotation_signed_url_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 200) | Signed URL đã tạo trước rotation vẫn đọc được nội dung sau rotation do secret HMAC của Storage không đổi. |
| **Service-role sống sót** (P2.5.6) | `rotation_legacy_service_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 200) | Request `GET /storage/v1/bucket` bằng service-role key tiếp tục thành công sau rotation. |
| **Go Auth Admin sống sót** (P2.5.6) | `rotation_auth_admin_survives` | `PASS` (chỉ lưu status; source assertion: HTTP 201) | Go Admin API tạo user mới thành công qua Auth Admin sau khi hoàn tất toàn bộ chu trình rotation. |
| **Tiến trình Go không đổi** (P2.5.5) | `rotation_same_go_process` | `PASS` (chỉ lưu status; source assertion: instance ID trước = sau) | Process instance ID của backend Go trước và sau rotation khớp nhau; Go không bị restart/recreate. |
| **Hard Expiry & Key Removal** (P2.5.7) | Unit tests trong `rs256_test.go` | `PASS` (`ok iot-platform/internal/auth 1.319s`) | Đã kiểm chứng độc lập: khóa hết hạn trong cache bị từ chối khi refresh lỗi (`TestRS256HardExpiryAndRotation`); khóa bị loại bỏ khỏi JWKS bị từ chối khi refresh thành công (`TestRS256RemovedKeyCannotSurviveSuccessfulRefresh`). |

#### 8. Bảo toàn bằng chứng lịch sử và ghi nhận lỗi tạm thời (Transient 503)
- **Bảo toàn thất bại lịch sử:** Kết quả `FAIL` của lượt chạy `paper-p2-full-20261024-first` (`ncs_26b92ecb5dcd`) tại check `rotation_storage_new_and_old` vẫn được bảo toàn nguyên vẹn trong tài liệu lịch sử.
- **Ghi nhận lỗi tạm thời (Transient 503 upstream_unavailable):** Trong lượt chạy chẩn đoán `ncs_60a5e67bb655`, hệ thống ghi nhận các phản hồi tạm thời `503 upstream_unavailable` trên tuyến Envoy-to-Storage khi container `supabase-storage` được recreate:
  - Bucket API readiness: 4 lần gặp `503` trước khi đạt `200` ở lần thử thứ 5.
  - Storage read token K2: lần 1 gặp `503`, lần 2 đạt `200`.
- **Ranh giới quan sát:** Các phản hồi 503 này là đặc tính tạm thời trong quá trình container khởi động lại và kết nối mạng giữa Envoy và Storage đang được tái lập. Envoy không bị restart; Go process không bị restart. Không suy diễn nguyên nhân cấp thấp nào ngoài các phản hồi thực tế được ghi nhận.

#### 9. Hạn chế kỹ thuật và kết luận nghiệm thu P2.5b
- **Hạn chế kỹ thuật:**
  - **Không thu hồi JWT tức thì (No Instantaneous Revocation):** Hệ thống không có session blocklist phân tán hay Redis. Một access token K1 đã phát vẫn có hiệu lực cho đến khi hết thời hạn `exp` (cộng clock skew) chừng nào K1 còn được giữ lại trong public JWKS của hệ thống.
  - **Phụ thuộc cửa sổ overlap:** Việc vô hiệu hóa token cũ đòi hỏi nhà vận hành phải chủ động loại bỏ K1 khỏi cấu hình JWKS sau khi cửa sổ chuyển tiếp kết thúc.
  - **Cập nhật Storage thủ công:** Do Storage phụ thuộc vào biến môi trường tĩnh `JWT_JWKS`, việc loại bỏ hoàn toàn K1 đòi hỏi phải cấu hình lại và recreate container Storage một lần nữa.
  - **Không cam kết Zero Downtime:** Thao tác recreate các dịch vụ container (`supabase-auth`, `supabase-storage`) tạo ra gián đoạn ngắn hạn thể hiện qua các mã lỗi tạm thời 503.
- **Kết luận:**
  - Phạm vi P2.5b đạt tiêu chuẩn nghiệm thu tuần tự (**Sequential Acceptance PASS**).
  - Toàn bộ các yêu cầu của P2.5 (bao gồm P2.5a và P2.5b) đã được kiểm chứng và hoàn tất với đầy đủ bằng chứng runtime, phân tích mã nguồn và kiểm thử đơn vị độc lập.
  - Dừng tại đây, giữ nguyên trạng thái hệ thống, không tự ý mở rộng sang P2.6 hoặc bất kỳ nhiệm vụ nào tiếp theo.

### Nghiệm thu tuần tự P2.6 — Expiry và thu hồi membership

Mục này nghiệm thu 5 yêu cầu của P2.6 theo kế hoạch `P2_detail_plan.md` từ bằng chứng runner (`ncs_60a5e67bb655`), mã nguồn Go và giới hạn kiến trúc MVP:

| Yêu cầu P2.6 | Check `run.json` | Kết quả quan sát | Chi tiết phân định trạng thái & hành vi |
| --- | --- | --- | --- |
| **Token hết hạn** (P2.6.1) | `expired_perimeter`, `expired_direct` | `PASS` (`http_status: 401` trong artifact) | Token có `exp` quá khứ bị từ chối 401 cả qua Nginx HTTPS và gọi trực tiếp Go container. |
| **Sai chữ ký** (P2.6.1) | `signature_perimeter`, `signature_direct` | `PASS` (`http_status: 401` trong artifact) | Token sai chữ ký bị từ chối 401 ở cả hai đường perimeter và direct. |
| **Admin không bypass** (P2.6.2) | `admin_does_not_bypass_membership` | `PASS` (chỉ lưu status; source assertion: 404) | Platform admin không thuộc `user_gateways` nhận 404 khi đọc sensor, không bypass quyền. |
| **Chặn giả mạo header** (P2.6.2) | `identity_header_spoof_denied` | `PASS` (chỉ lưu status; source assertion: 403) | User thường giả mạo `X-User-ID`/`X-Role` bị 403; Go chỉ tin principal từ JWT Bearer. |
| **Thu hồi membership** (P2.6.3) | `membership_same_token` | `PASS` (`statuses: [200, 404, 200]` trong artifact) | **Cùng 1 JWT còn hạn**: có quyền (200) → admin xóa quyền (204) → bị chặn (404) → gán lại (200) đọc lại được (200). Nonmember trả về 404, không rò rỉ dữ liệu. |

- **Phân định danh tính vs phân quyền DB:** Nginx `auth_request` Unix subrequest chỉ xác thực chữ ký JWT (trả 204), không phân quyền. Router nghiệp vụ Go độc lập kiểm tra `platform_admins` hoặc `user_gateways` qua DB trên mỗi request. Request-scoped context chỉ tồn tại theo vòng đời request; không có bộ đệm kết quả membership dương (cross-request positive cache) giữa các request mới. Việc đối chiếu mã nguồn không thay thế hay chứng minh mọi xen kẽ tương tranh (concurrent interleavings).
- **Hạn chế kỹ thuật & phạm vi:**
  - **Không thu hồi JWT tức thì:** Logout hay revoke refresh-session không hủy access token trước `exp` (kèm clock skew 30s trong fixture P2; cấu hình cho phép 0–5 phút). Phân định rõ: request mới sau khi transaction thu hồi đã commit phải kiểm lại DB và bị từ chối vì DB check mỗi request; thu hồi JWT không có hiệu lực tức thì.
  - **Không blocklist phân tán:** Hệ thống không có Redis, session blocklist hay cơ chế RAM revocation O(1).
  - **Không hủy query đang chạy:** Không có cơ chế hủy (cancellation) các truy vấn đang thực thi dở dang khi membership bị xóa đồng thời tại DB.
- **Kết luận:** Phạm vi P2.6 đạt nghiệm thu tuần tự (**Sequential Acceptance PASS**). Bảo toàn các ghi nhận lỗi lịch sử; không tuyên bố toàn bộ P2 hoàn thành và không mở rộng sang P2.7+.

### Nghiệm thu tuần tự P2.7 — Runner, artifacts và biên đo

Mục này nghiệm thu 9 yêu cầu của P2.7 theo kế hoạch `P2_detail_plan.md` từ cấu trúc runner, kiểm thử tập trung và bộ artifacts mới trên đĩa (`ncs_fcd6c1c6f81a` tại `/tmp/opencode/paper-p2-reverify-after-reboot-20261024-220120-30702/artifacts`). *Lưu ý: Lượt kiểm tra độc lập trước bị gián đoạn do file `/tmp` của lượt cũ mất sau reboot; lượt rerun cô lập mới này thay thế việc kiểm chứng đó mà không làm thay đổi các ghi nhận PASS lịch sử.*

| Tiêu chí P2.7 | Hiện trạng & Bằng chứng thực tế trên đĩa | Chi tiết kiểm chứng & Giới hạn đo lường |
| --- | --- | --- |
| **Runner & Wrapper** (P2.7.1, P2.7.2) | `scripts/test-paper-p2.sh` gọi `native_compose_smoke.py --paper-p2` | Tái sử dụng isolated lifecycle, registry, schema/quota P0/P1. P0/P1 mặc định giữ nguyên HTTP/HS256; `HTTPSEndpoint` xác minh CA thật qua `p2-ca.crt`, không nới lỏng context TLS toàn cục. |
| **Artifacts & Dọn dẹp** (P2.7.6, P2.7.9) | Đủ 5 files: `run.json` (12,6 KB), `metrics.csv` (5,48 MB, 27.941 dòng), `resources.csv`, `spike.json`, `startup-summary.json` | Lượt chạy `ncs_fcd6c1c6f81a` đạt `status: PASS`, `full_p2_accepted: true`, `exact_cleanup_verified: true`, dọn dẹp chính xác: 19 containers, 1 network, 7 volumes. Revision `926f642b...` (`dirty: true`). Bảo toàn schema P1 (`schema_version: 1`), bổ sung metadata P2 (`auth_mode: rs256`, `rotation`, `auth_measurements`). Thuộc tính kế thừa không bị đọc nhầm thành benchmark tối ưu. |
| **Biên đo lường & Tái tính toán** (P2.7.3, P2.7.4, P2.7.7) | Tái tính toán trực tiếp từ `run.json` và `metrics.csv` mới (overflow: 0) | `client_https` ($N=16$, tổng 50,5507 ms, mean 3,1594 ms): 16 mẫu trong `(1,0 ms, 5,0 ms]`, p50 (rank 8), p95 (rank 16), p99 (rank 16) đều thuộc `(1,0 ms, 5,0 ms]`.<br>`business_verify_cumulative` ($N=50$, tổng 4,0554 ms, mean 0,0811 ms): 43 mẫu `<= 0,1 ms`, 7 mẫu `(0,1 ms, 0,5 ms]`; p50 (rank 25) `<= 0,1 ms`; p95 (rank 48) và p99 (rank 50) trong `(0,1 ms, 0,5 ms]`.<br>`unix_verify_cumulative` ($N=49$, tổng 4,3395 ms, mean 0,0886 ms): 35 mẫu `<= 0,1 ms`, 14 mẫu `(0,1 ms, 0,5 ms]`; p50 (rank 25) `<= 0,1 ms`; p95 (rank 47) và p99 (rank 49) trong `(0,1 ms, 0,5 ms]`.<br>`unix_handler_cumulative` ($N=50$, tổng 5,0633 ms, mean 0,1013 ms): 32 mẫu `<= 0,1 ms`, 18 mẫu `(0,1 ms, 0,5 ms]`; p50 (rank 25) `<= 0,1 ms`; p95 (rank 48) và p99 (rank 50) trong `(0,1 ms, 0,5 ms]`.<br>Socket round-trip là null. Double verification tạo 2 quan sát riêng, không phải 2 request nghiệp vụ. Không cộng p95 hay trừ đồng hồ chéo. |
| **Xuất CSV & Nhãn tĩnh** (P2.7.3, P2.7.5) | Header cố định 14 cột, groups chuẩn; không rò rỉ `kid`/token/user | `metrics.csv` xuất các nhóm cố định (`auth`, `unix_auth`, `unix_auth_handler`, `jwks_cache`, `client_https`, `http_*`). Focused test `test_p2_csv_exports_fixed_unix_and_cache_series_without_dynamic_labels` PASS. |
| **Regression Uplink & Temporal** (P2.7.8) | DB delta 40/4000/40, outbox 0, replay ACK, `temporal_points: 199` | 2 Gateway × 20 batches có app ACK, outbox drained (`pending_rows=0`); replay byte-exact thành công, DB không đổi; đọc `/temporal` qua HTTPS bằng RS256 token trả về HTTP 200 với đúng **199 điểm** thực tế theo cửa sổ truy vấn. |

- **Giới hạn đo lường:** Double verification phục vụ an toàn và đo lường, không phải tối ưu thông lượng. Go cumulative samples (49–50 mẫu) phản ánh toàn bộ xác thực từ lúc khởi động, không đại diện riêng cho 16 client requests.
- **Kết luận:** Phạm vi P2.7 đạt nghiệm thu tuần tự (**Sequential Acceptance PASS**) trên bộ dữ liệu tươi `ncs_fcd6c1c6f81a`. Không tuyên bố hoàn thành toàn bộ P2 và không tự chuyển sang P2.8/P2.9.

### Đối chiếu P2.8a — Ma trận nghiệm thu và khoảng trống

Đối chiếu 11 nhóm nghiệm thu P2.8 theo kế hoạch `P2_detail_plan.md` với artifact tươi `ncs_fcd6c1c6f81a` (sau reboot) và kiểm thử hiện hữu:

| Nhóm kiểm tra (P2.8) | Bằng chứng artifact tươi (`run.json`) | Nguồn/Kiểm thử hiện hữu | Phân định trạng thái & Ghi chú |
| --- | --- | --- | --- |
| **1. Login/claims/JWKS** | `new_user_login_rs256` (PASS), `supabase_spike_checks.user_contract` (RS256), `supabase_spike_checks.public_jwks` (200), `direct_valid` (200) | `internal/auth/rs256_test.go` (`TestRS256ContractAndBoundedRefresh`) | Lưu HTTP 200; `alg=="RS256"` & `kid` qua assertion. |
| **2. Token negative** | `expired_perimeter` (401), `expired_direct` (401), nhóm signature (`signature_perimeter`, `signature_direct`) (401), nhóm invalid (`perimeter_invalid`, `direct_invalid`) (401) | `internal/auth/rs256_test.go`, `middleware_test.go`, `verifier_test.go` | Lưu rõ HTTP 401; unit bao phủ missing key/fetch fail. |
| **3. Hai đường HTTP** | nhóm missing/invalid (`perimeter_missing`, `perimeter_invalid`, `direct_missing`, `direct_invalid`) (401), `direct_valid` (200) | `internal/auth/unix_test.go`, `scripts/tests/test_paper_p2.py` | Lưu 401/200; `identity_header_spoof_denied` (403), `internal_hidden` qua assertion. |
| **4. Fail-closed** | `upstream_pause` (PASS), `upstream_fail_closed` (500), `upstream_recovered` (PASS) | `internal/auth/rs256_test.go`, `scripts/tests/test_paper_p2.py` | Lưu HTTP 500; chặn tuyệt đối không vào business handler. |
| **5. Membership/admin** | `membership_same_token` ([200, 404, 200]), `metrics_auth_statuses` ([401, 401, 403, 200]) | `internal/auth/postgres_admin_test.go`, `internal/httpserver` | Lưu mảng status; `admin_does_not_bypass_membership` (404 qua assertion). |
| **6. API-key compat** | `go_auth_admin_create` (201), `business_patch_preserves_body` (200), `supabase_spike_checks.auth_admin_legacy_service` (200) | `internal/usermanagement/auth_adapter_test.go`, `cmd/server` | Lưu 201/200; `perimeter_service`/`anon` bị 401 (lưu trong artifact). |
| **7. Storage** | `storage_private_create` (200), nhóm upload/probe (`storage_upload_fixture_object`) (200), `storage_rs256_private_read` (200) | `scripts/tests/test_paper_p2.py` (probe retry & timeout tests) | Lưu 200; `storage_private_other_user_denied` lưu 400; signed URL lưu 200. |
| **8. Rotation** | `rotation_storage_k1` (200), `rotation_storage_k2` (200), `rotation` dict (k1, k2, overlap) | `internal/auth/rs256_test.go` (hard expiry & key removal) | **Phân định rõ**: Native chứng minh overlap; Unit chứng minh key removal & hard expiry. |
| **9. HTTPS/perimeter** | `https_readiness` (200), `cors_preflight` (204), `https_verified: true`, nhóm business patch/limit (`business_patch_preserves_body`, `business_body_limit`) (200) | `internal/httpserver/cors_test.go`, `scripts/tests/test_paper_p2.py` | Lưu 200/204; `cors_denied`, `business_body_limit` qua assertion. |
| **10. Regression nền** | `db_delta` (40/4000/40), `temporal_points: 199` (tươi), `cleanup_counts` (19/1/7) | `scripts/tests/test_native_compose_smoke.py` | **199 điểm** (đúng artifact, không lấy 200); Stage 5 giữ `false`; P0/P1 tách biệt. |
| **11. Quan sát** | `metrics.csv` (27.941 dòng, 14 cột tĩnh), `auth_measurements` (N=16 client, N=49-50 Go) | `internal/auth/metrics_test.go`, `scripts/tests/test_paper_p2.py` | Double verification tạo 2 quan sát riêng; socket RTT không khả dụng / null (`socket_round_trip_seconds: null`); không cộng p95 chéo. |

- **Kết quả kiểm toán P2.8a:** Ma trận 11 nhóm nghiệm thu đầy đủ bằng chứng, không có blocker nào được phát hiện trong phạm vi kiểm toán có giới hạn (no blocker found in the scoped audit).
- **Khoảng trống xác minh kỹ thuật:** Đã thực thi đầy đủ xác minh cục bộ trong mục P2.8b dưới đây. Không chạy lại native fixture vì `ncs_fcd6c1c6f81a` đã bao phủ đủ.

### Xác minh kỹ thuật P2.8b — Hoàn tất kiểm tra kỹ thuật cục bộ sau kiểm toán

- **Mục đích & Phạm vi:** Thực thi tuần tự các kiểm tra kỹ thuật cục bộ bắt buộc trên môi trường host Go 1.27.3 nhằm đóng khoảng trống bằng chứng sau reboot, tái sử dụng bằng chứng native tươi `ncs_fcd6c1c6f81a`. Không sửa code, không đổi cấu hình, không chạy lại native fixture hay Docker.
- **Toolchain & Ngữ cảnh:**
  - Go toolchain: `go version go1.27.3 linux/amd64` (đạt yêu cầu bắt buộc).
  - Python: `3.13.5`.
  - Git: commit `926f642b71bf6729da81209926f25c79a05e6042`, branch `feat/paper-requirement`, `dirty: true` (bảo toàn uncommitted changes).
  - Native run tái sử dụng: `ncs_fcd6c1c6f81a` tại `/tmp/opencode/paper-p2-reverify-after-reboot-20261024-220120-30702/artifacts` (tồn tại nguyên vẹn, giữ nguyên kết quả).
  - Protected log directory: `/tmp/opencode/p2-p28b-verification-1792855796` (quyền thư mục `0700`, toàn bộ file log `0600`; `umask` được khôi phục `0002` trước khi chạy Go tests).
- **Kết quả kiểm tra tuần tự (8/8 PASS):**
  1. `go test -race ./internal/auth ./internal/config ./internal/httpserver ./internal/usermanagement ./cmd/server -count=1` (chạy từ `src/`): **PASS** (5/5 packages: `internal/auth` 1.62s, `internal/config` 1.04s, `internal/httpserver` 2.13s, `internal/usermanagement` 1.15s, `cmd/server` 1.50s; không data race). Log: `01_go_test.log`.
  2. `gofmt -l .` (chạy từ `src/`, read-only): **PASS** (output rỗng, 0 file vi phạm định dạng). Log: `02_gofmt.log`.
  3. `go vet ./...` (chạy từ `src/`): **PASS** (output rỗng, 0 cảnh báo). Log: `03_govet.log`.
  4. `golangci-lint run --timeout=5m` (chạy từ `src/`): **PASS** (0 issues). Log: `04_golangci_lint.log`.
  5. `go build ./...` (chạy từ `src/`): **PASS** (exit code 0, biên dịch sạch toàn bộ packages). Log: `05_go_build.log`.
  6. `govulncheck ./...` (chạy từ `src/`): **PASS** (0 lỗ hổng ảnh hưởng mã nguồn; 0 lỗ hổng trong packages import; phát hiện 1 advisory ở module phụ thuộc `golang.org/x/crypto` (GO-2026-5932) nhưng không có biểu tượng nào được import hoặc gọi; không có blocker). Log: `06_govulncheck.log`.
  7. Python unittests (chạy từ repo root: `scripts/tests/test_paper_p2.py`, `test_native_compose_smoke.py`, `test_stage3_uplink_runner.py`, `test_ci_go_test_guard.py`, `test_ci_feature_jobs.py`, `test_ci_smoke_jobs.py`, `test_go_toolchain.py`): **PASS** (336 tests, 0 failures, 0 errors, 13.88s). Log: `07_python_unittest.log`.
  8. `git diff --check` (chạy từ repo root): **PASS** (output rỗng, không có lỗi whitespace hay conflict marker). Log: `08_git_diff_check.log`.
- **Phân định giới hạn kiểm thử (NOT RUN vs PASS):**
  - **NOT RUN:** Remote CI trên GitHub Actions và kiểm thử trên phần cứng nhúng thật (board AM5728 / HIL) tiếp tục giữ nguyên trạng thái **NOT RUN**.
  - **Giới hạn phạm vi:** Đây là kết quả xác minh kỹ thuật cục bộ mức MVP (local MVP verification), không phải chứng nhận môi trường production (production certification). Stage 5 tiếp tục giữ `STAGE5_ENABLED=false`.
- **Kết luận nghiệm thu P2.8:** Toàn bộ 8/8 bước kiểm tra kỹ thuật đạt yêu cầu. Phạm vi kiểm toán và xác minh kỹ thuật P2.8 chính thức được chấp nhận (**P2.8 Accepted**). Không đánh dấu hoàn thành toàn bộ P2 và không đánh dấu P2.9 hoàn thành.

## P2.9 — Kết quả native lịch sử và dự thảo độ trễ (Run ncs_60a5e67bb655 — Bản nháp lịch sử)

### 1. Bằng chứng native lịch sử và trạng thái dọn dẹp (Lượt chạy ncs_60a5e67bb655)

*Ghi chú quan trọng:* Tiểu mục này bảo toàn nguyên vẹn số liệu và dự thảo độ trễ từ lượt chạy lịch sử `ncs_60a5e67bb655` (trước reboot). Các tệp `/tmp` của lượt cũ này đã mất sau khi máy khởi động lại. Số liệu đo lường độ trễ tươi và tái tính toán mới từ lượt chạy kiểm chứng độc lập sau reboot (`ncs_fcd6c1c6f81a`) được ghi nhận đầy đủ tại **mục P2.7**.

- Lượt chạy native đầy đủ: `paper-p2-full-20261024-storage-diagnostics`, kết quả **PASS**, exit 0.
- Artifacts an toàn (lịch sử): `/tmp/opencode/paper-p2-full-20261024-storage-diagnostics/artifacts/` (`run.json`, `metrics.csv`, `startup-summary.json`, `resources.csv`, `spike.json`).
- `run.json`: `status: "PASS"`, `run_id: "ncs_60a5e67bb655"`, `mode: "paper_p2"`, `full_p2_accepted: true`, `cleanup_verified: true`.
- `startup-summary.json`: `status: "PAPER_P2_ACCEPTED"`, `project: "ncs_60a5e67bb655"`, `native_executed: true`, `full_p2_accepted: true`, `exact_cleanup_verified: true`.
- Xác nhận dọn dẹp tài nguyên chính xác (`cleanup_counts`): **19 containers, 1 network, 7 volumes**. Private inputs của lượt PASS đã được tự động dọn dẹp; không lưu private key hay user credentials trong artifacts.
- Mã nguồn và trạng thái git: commit `926f642b71bf6729da81209926f25c79a05e6042`, branch `feat/paper-requirement`, `dirty: true` (worktree có uncommitted changes).
- **Giới hạn phạm vi nghiêm ngặt:** Không tuyên bố clean committed revision, production readiness, zero downtime, thu hồi JWT tức thì, full remote CI hay nghiệm thu trên phần cứng thật (board/HIL). Stage 5 vẫn giữ tắt (`STAGE5_ENABLED=false`) và không có công việc P3 nào được triển khai.

### 2. Chẩn đoán Storage readiness và rotation hiện hành

Lượt chạy áp dụng chẩn đoán phân tách rõ từng probe có phân loại trạng thái và retry budget có hạn (tối đa 20 attempts, deadline 30s):
- **Storage bucket API readiness** (service-role): **503 × 4 → 200** (4 attempts đầu gặp `503 upstream_unavailable`, attempt thứ 5 đạt `200 ok` trong retry budget).
- **Storage object API readiness** (service-role): **200 ngay lần đầu** (`ok`).
- **Storage read riêng token K1**: **200 ngay lần đầu** (`ok`, status PASS).
- **Storage read riêng token K2**: **503 → 200** (attempt 1 gặp `503 upstream_unavailable`, attempt 2 đạt `200 ok`, status PASS).
- **Bảo toàn bằng chứng lịch sử:** Quan sát trên xác nhận sự tồn tại của trạng thái tạm thời `upstream_unavailable` từ Envoy/Storage trong cửa sổ recreation khi rotation. Do lượt chạy thất bại trước (`ncs_26b92ecb5dcd`) dùng `all()` gộp hai lượt read và không có trạng thái phân tách riêng, không khẳng định đây là nguyên nhân chính xác duy nhất của lỗi lịch sử. Toàn bộ kết quả và trạng thái FAIL cũ được giữ nguyên vẹn.

### 3. Phân định biên đo lường độ trễ xác thực

Các nhóm đo lường được phân định độc lập, không gộp lẫn:
1. **Client HTTPS requests (`client_https`)**: 16 requests `GET /v1/gateways` liên tiếp có Bearer token hợp lệ gửi qua Nginx HTTPS loopback, đo bằng `time.monotonic()` phía client test trong phase `measure_auth`.
2. **Business JWT verification (`business_verify_cumulative`)**: Số liệu tích lũy trên Go process từ khi khởi động đến snapshot `after_auth_requests` tại tầng HTTP router nghiệp vụ.
3. **Unix JWT verification (`unix_verify_cumulative`)**: Số liệu tích lũy trên Go process tại listener Unix socket (`GET /internal/auth/verify`) phục vụ `auth_request` của Nginx.
4. **Unix auth handler duration (`unix_handler_cumulative`)**: Thời gian xử lý toàn bộ của handler Unix subrequest, ghi nhận qua histogram nội bộ Go.
5. **Socket round-trip (`socket_round_trip_seconds`)**: **null / không khả dụng**. Tuyệt đối không suy đoán từ handler duration và không trừ các mốc thời gian từ các đồng hồ process khác nhau.
6. **JWKS cache counters**: Snapshot tại `after_auth_requests` ghi nhận: `hits = 95`, `misses = 1`, `refresh_success = 2`, `refresh_failure = 1`.

*Lưu ý quan trọng:* Các mẫu tích lũy (cumulative) của Go process (khoảng 49–50 mẫu) đại diện cho toàn bộ các xác thực từ lúc khởi động (bao gồm probe nội bộ, admin probe, và các phase kiểm tra trước đó), không đồng nhất rằng toàn bộ 49–50 mẫu này thuộc về 16 requests client HTTPS nói trên.

### 4. Tổng hợp số đo độ trễ xác thực (Authentication Latency Summary)

- Đơn vị thời gian gốc: giây (s), chuyển đổi hiển thị mili-giây (ms).
- Thang chia bucket (upper bounds cố định, đơn vị giây): `[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]`.
- Quy ước phân vị: Sử dụng quy ước **nearest-rank** ($k = \lceil \frac{P}{100} \times N \rceil$ với phân vị thứ $P$, cỡ mẫu $N$) trên các bucket đếm không tích lũy (non-cumulative counts).
- **Nguyên tắc độ chính xác:** Tuyệt đối không báo cáo giá trị phân vị chính xác từ coarse histogram; chỉ báo cáo khoảng bucket chứa phân vị tương ứng. Nếu phân vị rơi vào overflow, nêu rõ chặn trên không khả dụng (> 10 s).

| Nhóm số đo | Cỡ mẫu ($N$) | Đơn vị | Tổng thời gian | Giá trị trung bình (Mean) | Phân bố bucket thực tế (không tích lũy) | Khoảng chứa p50 | Khoảng chứa p95 | Khoảng chứa p99 | Overflow |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Client HTTPS requests** (`client_https`) | 16 | ms (s) | 48,8562 ms (0,048856227 s) | 3,0535 ms (0,003053514 s) | Bucket 3 `(1,0 ms, 5,0 ms]`: 16 | `(1,0 ms, 5,0 ms]` (rank 8) | `(1,0 ms, 5,0 ms]` (rank 16) | `(1,0 ms, 5,0 ms]` (rank 16) | 0 |
| **Business JWT Verify** (`business_verify_cumulative`) | 50 | ms (s) | 4,0830 ms (0,004083038 s) | 0,0817 ms (0,000081661 s) | Bucket 0 `<= 0,1 ms`: 43<br>Bucket 1 `(0,1 ms, 0,5 ms]`: 7 | `<= 0,1 ms` (rank 25) | `(0,1 ms, 0,5 ms]` (rank 48) | `(0,1 ms, 0,5 ms]` (rank 50) | 0 |
| **Unix JWT Verify** (`unix_verify_cumulative`) | 49 | ms (s) | 5,1854 ms (0,005185440 s) | 0,1058 ms (0,000105825 s) | Bucket 0 `<= 0,1 ms`: 31<br>Bucket 1 `(0,1 ms, 0,5 ms]`: 17<br>Bucket 2 `(0,5 ms, 1,0 ms]`: 1 | `<= 0,1 ms` (rank 25) | `(0,1 ms, 0,5 ms]` (rank 47) | `(0,5 ms, 1,0 ms]` (rank 49) | 0 |
| **Unix Auth Handler** (`unix_handler_cumulative`) | 50 | ms (s) | 5,5259 ms (0,005525860 s) | 0,1105 ms (0,000110517 s) | Bucket 0 `<= 0,1 ms`: 32<br>Bucket 1 `(0,1 ms, 0,5 ms]`: 17<br>Bucket 2 `(0,5 ms, 1,0 ms]`: 1 | `<= 0,1 ms` (rank 25) | `(0,1 ms, 0,5 ms]` (rank 48) | `(0,5 ms, 1,0 ms]` (rank 50) | 0 |

Chi tiết vị trí rank theo quy ước nearest-rank:
- `client_https` ($N=16$): Toàn bộ 16 mẫu đều nằm trong khoảng `(1,0 ms, 5,0 ms]` (tức `(0,001 s, 0,005 s]`), do đó p50 (rank 8), p95 (rank 16), p99 (rank 16) đều thuộc khoảng này.
- `business_verify_cumulative` ($N=50$): 43 mẫu `<= 0,1 ms`, 7 mẫu trong `(0,1 ms, 0,5 ms]`. Rank 25 (p50) $\le 43$ nên thuộc `<= 0,1 ms`; rank 48 (p95) và rank 50 (p99) nằm trong khoảng $43 < rank \le 50$ nên thuộc `(0,1 ms, 0,5 ms]`.
- `unix_verify_cumulative` ($N=49$): 31 mẫu `<= 0,1 ms`, 17 mẫu trong `(0,1 ms, 0,5 ms]`, 1 mẫu trong `(0,5 ms, 1,0 ms]`. Rank 25 (p50) $\le 31$ nên thuộc `<= 0,1 ms`; rank 47 (p95) nằm trong khoảng $31 < rank \le 48$ nên thuộc `(0,1 ms, 0,5 ms]`; rank 49 (p99) là mẫu cuối cùng (rank 49) rơi vào `(0,5 ms, 1,0 ms]`.
- `unix_handler_cumulative` ($N=50$): 32 mẫu `<= 0,1 ms`, 17 mẫu trong `(0,1 ms, 0,5 ms]`, 1 mẫu trong `(0,5 ms, 1,0 ms]`. Rank 25 (p50) $\le 32$ nên thuộc `<= 0,1 ms`; rank 48 (p95) nằm trong khoảng $32 < rank \le 49$ nên thuộc `(0,1 ms, 0,5 ms]`; rank 50 (p99) là mẫu thứ 50 rơi vào `(0,5 ms, 1,0 ms]`.
- Không có mẫu nào rơi vào overflow (`overflow = 0`). Nếu có mẫu rơi vào overflow (> 10 s), chặn trên của khoảng giá trị đó được ghi nhận là không khả dụng.

## Cấu hình và biên tin cậy

| Biến Go | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SUPABASE_JWT_MODE` | `hs256` | Chỉ nhận một thuật toán; không automatic fallback |
| `SUPABASE_JWKS_URL` | rỗng | Bắt buộc trong mode rs256; URL do operator cấu hình, không credentials/query/fragment |
| `SUPABASE_JWKS_CACHE_TTL` | `60s` | Hard freshness, cho phép 1s–1h; refresh nền mỗi TTL/2 |
| `SUPABASE_JWKS_HTTP_TIMEOUT` | `2s` | 100ms–10s; HTTP/key lookup bounded |
| `SUPABASE_JWKS_REFRESH_COOLDOWN` | `5s` | 5s–1m; unknown/expired key refresh rate limit dùng chung, không per-kid map |
| `AUTH_UNIX_SOCKET_PATH` | rỗng/tắt | Profile P2 `/run/iot-auth/auth.sock`; directory/permissions phải chuẩn bị trước |
| `SUPABASE_JWT_CLOCK_SKEW` | `30s` | Giữ giới hạn 0–5m; expiry không là mốc revoke tức thì tuyệt đối |

Issuer phải khớp cấu hình Auth và Go; audience chỉ `authenticated`. P2 fixture
dùng issuer `https://localhost/auth/v1`, JWKS qua Envoy HTTP trong mạng riêng.
Issuer là claim đã pin, không dùng để suy URL tải key. HTTPS JWKS dùng system
CA/hostname verification của Go; fixture không có TLS giữa Go và Envoy.

Config Go chỉ đọc process environment. `.env.example` là mẫu, không được source
deployment `.env` để chạy test. Root Compose mặc định vẫn HS256/HTTP; chỉ đặt
JWT mode không tự tạo signing key, đổi Auth, cấp socket hoặc bật HTTPS. Cần
protected override/profile tương thích như fixture P2; tuyệt đối không bật một
phần rồi nới verifier để che lỗi tương thích.

Auth nhận private signing JWK + legacy verify-only compatibility key; public
JWKS chỉ RSA. Storage nhận **public** static JWKS và legacy HMAC secret riêng cho
API keys/URL signing. Nginx chỉ nhận HTTPS cert/key riêng và socket volume; Go
chỉ nhận public JWKS endpoint. Người có quyền Docker inspect vẫn thuộc trusted
operator vì private configuration đi qua protected Compose/environment; không
publish resolved config, private inputs, login response hoặc signed URL.

## Thứ tự chuyển đổi và rollback (hướng dẫn, chưa rollout live)

### Chuyển đổi có cửa sổ ngắn

1. Backup templates/image IDs và protected config/legacy secret ở nơi kiểm soát
   quyền. Chuẩn bị certificate đúng SAN/trust chain và socket directory/UID/GID.
   Không xóa DB/volume hoặc thay migration để chuyển Auth.
2. Chuẩn bị K1 private signer + legacy API-key verify-only key cho Auth; một
   signing key duy nhất. Chuẩn bị public K1 cho Storage, giữ HMAC URL signing
   và legacy anon/service-role. Không mount Auth private key sang consumer khác.
3. Trong cửa sổ chuyển đổi: reload/recreate Auth và Storage với config mới; kiểm
   Auth Admin bằng service-role, login mới nhận RS256/kid, public JWKS và user
   Storage probe. Đây là thay đổi cấu hình startup, không giả định hot reload.
4. Bật Go `rs256`/JWKS/TTL; JWKS phải reachable trước khi Go khởi động vì initial
   key fetch fail startup. Sau đó bật listener socket và Nginx HTTPS/auth_request.
   Giữ Go business guards; không dùng identity headers thay Bearer/DB checks.
5. Login lại và smoke HTTPS/API/Admin/Storage/uplink; token HS256 user cũ không
   được chấp nhận ở Go rs256. Chưa có dual-mode rollout được nghiệm thu; không
   hứa zero downtime. Giữ Stage 5 false, không rollout control/client tự động.

### Rotation tiếp theo

- Công bố public K1+K2, vẫn đúng một signer K1; đồng bộ Storage static JWKS và
  recreate/reload thành phần đó. Sau khi JWKS/readiness tốt, đổi Auth signer K2,
  giữ K1 verify-only. Go process/cache không cần restart để nhận K2.
- Giữ overlap đủ bao phủ token K1 còn hạn, clock skew và chính sách rollout;
  TTL 60 giây không đồng nghĩa token TTL 60 giây. Loại K1 sau overlap theo quyết
  định vận hành; Go có thể dùng key cached tối đa hard TTL từ lần fetch hợp lệ.
  Storage static JWKS cần cập nhật/restart riêng, không tự theo Go refresh.
- Logout hoặc revoke refresh session không đảm bảo vô hiệu access JWT trước
  exp/skew. Không có session blocklist/Redis/distributed revocation. Thu hồi
  membership kiểm DB ở request mới; không hứa hủy query đã chạy dở.

### Rollback có chủ đích

1. Giữ credentials/config/image đã biết tương thích, không đưa secrets vào argv
   hoặc Git. Tạm ngừng client traffic nếu cần để tránh mixed-mode không được
   hỗ trợ. Không fallback HS256 theo từng request bị RSA verify lỗi.
2. Khôi phục nguyên bộ Auth signing/legacy keys + Storage legacy/public-key set
   tương ứng, Go hs256/secret/issuer/audience, template Nginx cũ và cấu hình
   socket tương ứng; recreate/restart các consumer cần đổi startup config.
3. Login lại rồi kiểm Auth/API/Admin/Storage và uplink. Token RS256 đã phát có
   thể không dùng được sau rollback hs256; thông báo yêu cầu login lại. Không
   rollback DB/migrations/volume hoặc nới TLS/JWT để làm smoke PASS.
