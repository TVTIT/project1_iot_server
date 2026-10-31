# P8 E1 — chuẩn bị, chạy pilot và nhập kết quả

Tài liệu này mô tả **pilot E1 hữu hạn**: A sở hữu fixture và collector; B bind
bundle, kiểm tra kết nối, chạy hai role workload bằng Go, rồi xuất một projection
kết quả đã giới hạn để A xác minh với DB/telemetry thật. `full_p8_accepted` luôn
`false`; đây không phải nghiệm thu P8. E2–E5, collector phân tán và rollout không
nằm trong phạm vi.

## Điều kiện và giới hạn

- A và B dùng các tiến trình/workspace riêng. Same-host smoke dùng advertised
  private interface thật, nhưng **không chứng minh hai máy vật lý**;
  `generator_colocated=true`, `physical_ab=NOT RUN`.
- Cần Go paperload binary đã build cho host chạy B. Bundle không chứa admin,
  DB, manager credential; A không gửi các credential đó cho B.
- Giao bundle sang B và result bundle về A bằng thao tác file đã duyệt; không có
  SSH/control-plane trong công cụ.
- `phase_id` cố định là `pilot` trong slice này. Không sửa phase/session/run IDs
  trong manifest hay result.
- Trên A, `begin` chỉ được ACK khi status đã sang `MEASURING` và
  `collector_ready=true`. `finish-phase` chỉ chuyển sang `VERIFYING` sau khi
  correlation, projection, DB delta, raw telemetry và WS terminal checks chạy.

## A — Terminal 1: serve và giữ fixture foreground

Tạo network config riêng tư theo [`paper-p8-remote-lifecycle.md`](paper-p8-remote-lifecycle.md),
với private IPv4 của A, cổng HTTPS/MQTT-TLS được phép và SAN phù hợp. Không ghi
deployment secrets vào config. Chạy và để tiến trình này mở:

```sh
python3 scripts/paper_p8_server.py serve \
  --root /var/tmp/opencode/p8-e1-run \
  --network-config /path/to/private-network.json \
  --deadline-seconds 900
```

Server dựng profile E1, provision 2 Gateway, 2 user, 4 Sensor, tạo role manifests
và xuất bundle tại `/var/tmp/opencode/p8-e1-run/client-bundle/`. Log foreground:
`/var/tmp/opencode/p8-e1-run/server.log`.

## A — Terminal 2: chờ READY, chuẩn bị nhận kết quả

```sh
python3 scripts/paper_p8_server.py status --root /var/tmp/opencode/p8-e1-run
```

Chỉ tiếp tục khi `preparation_status=E1_PREPARED`, `phase=READY`, và bundle
artifact có `session_id`, `run_id`, `phase=pilot`, digest cùng topology 2/2/4.
Copy bundle thủ công sang B; **đặt nó ở một root riêng**.

A cũng build binary đã duyệt tại
`/var/tmp/opencode/p8-e1-run/fixture/private/bin/paperload`. Chuyển đúng executable
này sang B bằng kênh file đã duyệt (ngoài bundle), giữ nguyên bytes/mode executable
và đối chiếu SHA-256. A kiểm tra hash binary trong result với artifact A; binary
B build độc lập có thể khác bytes và sẽ bị từ chối dù cùng source revision.

```sh
sha256sum /var/tmp/opencode/p8-e1-run/fixture/private/bin/paperload
```

## B — bind, Go preflight và kết nối thật

```sh
python3 scripts/paper_p8_client.py bind \
  --bundle /path/on/B/received/client-bundle \
  --workspace /var/tmp/opencode/p8-e1-workspace

python3 scripts/paper_p8_client.py preflight \
  --workspace /var/tmp/opencode/p8-e1-workspace \
  --binary /path/to/A-approved-paperload

python3 scripts/paper_p8_client.py check-connection \
  --workspace /var/tmp/opencode/p8-e1-workspace
```

Không bắt đầu nếu preflight hoặc kết nối TLS/Auth/WS/MQTT thất bại. Ghi nhận
`session_id` từ A status cho các lệnh operator tiếp theo.

## A — Terminal 2: bắt đầu collector trước workload

```sh
python3 scripts/paper_p8_server.py begin \
  --root /var/tmp/opencode/p8-e1-run --session-id <session_id> \
  --phase READY --phase-id pilot
python3 scripts/paper_p8_server.py status --root /var/tmp/opencode/p8-e1-run
```

Lệnh đầu chỉ publish request. Chưa chạy B cho tới khi status xác nhận
`phase=MEASURING`, `phase_id=pilot`, `collector_ready=true`. Baseline aggregate
và per-target DB counts được chụp trên A trước khi collector start/ACK.

## B — chạy hữu hạn và xuất projection kết quả

```sh
python3 scripts/paper_p8_client.py run \
  --workspace /var/tmp/opencode/p8-e1-workspace \
  --binary /path/to/A-approved-paperload

python3 scripts/paper_p8_client.py export-results \
  --workspace /var/tmp/opencode/p8-e1-workspace \
  --output /var/tmp/opencode/p8-e1-results
```

`run` dùng workload pilot trong manifest đã bind; supervisor phía B có giới hạn
thời gian và giữ evidence nếu runner fail. `export-results` chỉ xuất runner
summary, binary provenance, role workload summaries/counters (viewer dùng ID
pseudonymous `viewer-NNNNN`, không user ID) và tối đa 20 header-hash identity
samples. Nó không xuất raw logs, payloads, credential hay private paths. Không
tự tổng hợp file JSON khác để thay result bundle.

Copy thư mục result đã export bằng thao tác file đã duyệt sang một thư mục private
riêng trên A, ví dụ `/var/tmp/opencode/p8-e1-run/received-results/` (directory
0700; các file regular 0600). Không dùng lại bundle root.

## A — Terminal 2: nhập, xác minh và đóng

```sh
python3 scripts/paper_p8_server.py finish-phase \
  --root /var/tmp/opencode/p8-e1-run --session-id <session_id> \
  --phase MEASURING --phase-id pilot \
  --results /var/tmp/opencode/p8-e1-run/received-results

python3 scripts/paper_p8_server.py status --root /var/tmp/opencode/p8-e1-run
```

Chỉ sau khi status sang `VERIFYING` mới kết thúc phiên. `imported_result` nêu
`validation_valid`, failures và report path; `workload_outcome` là
`VALIDATED` hoặc `VALIDATION_FAILED`. Kết quả sai schema, thiếu role, sai session/
run/phase/binary/manifest provenance, path lạ, symlink hay vượt giới hạn bị từ
chối, vẫn ở `MEASURING` và không đánh dấu PASS. Có thể gửi result hợp lệ khác
hoặc abort.

```sh
python3 scripts/paper_p8_server.py finalize \
  --root /var/tmp/opencode/p8-e1-run --session-id <session_id> \
  --phase VERIFYING
```

Nếu workload hoặc import không thành công, vẫn có thể abort từ phase hiện tại:

```sh
python3 scripts/paper_p8_server.py abort \
  --root /var/tmp/opencode/p8-e1-run --session-id <session_id> \
  --phase <phase-hiện-tại>
```

Đợi Terminal 1 thoát rồi kiểm tra status cuối: `phase=CLOSED`,
`cleanup_outcome=VERIFIED`. JSON report, metrics CSV, resources CSV và startup/run
evidence nằm trong fixture artifacts/private root; chúng ghi source/dirty state,
schema, quota/flags, image IDs/digests, mapping, validation và chỉ số thực tế.
Giữ nguyên artifacts nếu có FAIL. Không chạy `docker compose down`, prune hay
cleanup tổng quát. `full_p8_accepted=false`; kết quả chỉ là same-host pilot nếu
đã chạy trên cùng máy.
