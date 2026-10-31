# P8 E2 — helper A/B pilot (source-qualified)

Hướng dẫn vận hành E2 cho **một profile mỗi fixture**. Chạy tuần tự hai fixture riêng: `samples_per_message=100`, sau đó `1`. Mỗi profile dùng seed 84, 24 unique messages (A=8, B=16), cùng TLS/Auth, provisioning, queue, quota, steady / burst / drain và validation độc lập. Đây là telemetry tổng hợp; không chạy Gateway executor và không chứng minh durable restart.

## Chạy một profile

Đặt `ROOT` riêng tư, mới cho mỗi profile; ví dụ `/var/tmp/opencode/p8-e2-100` rồi `/var/tmp/opencode/p8-e2-1`. Dùng network config riêng đã được kiểm tra. Tiến trình `serve` phải được giữ hoạt động trong lúc operator điều khiển A/B; không đợi nó kết thúc trước khi gửi lệnh.

### A — khởi tạo và chờ collector

```sh
ROOT=/var/tmp/opencode/p8-e2-100
SAMPLES=100                       # lượt kế tiếp: ROOT=.../p8-e2-1; SAMPLES=1
python3 scripts/paper_p8_server.py serve --root "$ROOT/A" \
  --network-config /path/to/private-network.json \
  --experiment E2 --samples-per-message "$SAMPLES" --deadline-seconds 900
```

Trong terminal operator khác, chờ `preparation_status=E2_PREPARED` và `phase=READY`; lấy `session_id` từ status:

```sh
python3 scripts/paper_p8_server.py status --root "$ROOT/A"
```

### B — bind bundle đã relocate, preflight, kết nối và chạy

Sao chép `A/client-bundle/` sang `B/received-bundle/` và đúng binary `A/fixture/private/bin/paperload` sang `B/paperload` qua kênh file đã duyệt. Không rebuild binary độc lập; digest phải khớp A.

```sh
python3 scripts/paper_p8_client.py bind --bundle "$ROOT/B/received-bundle" \
  --workspace "$ROOT/B/workspace"
python3 scripts/paper_p8_client.py preflight --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload"
python3 scripts/paper_p8_client.py check-connection --workspace "$ROOT/B/workspace"
```

Trên A, bắt đầu phase đúng profile; chờ `phase=MEASURING`, phase ID `samples-$SAMPLES` và `collector_ready=true` trước khi B bắt đầu:

```sh
python3 scripts/paper_p8_server.py begin --root "$ROOT/A" \
  --experiment E2 --session-id SESSION --phase READY --phase-id "samples-$SAMPLES"
python3 scripts/paper_p8_server.py status --root "$ROOT/A"
```

Trên B:

```sh
python3 scripts/paper_p8_client.py run --workspace "$ROOT/B/workspace" \
  --binary "$ROOT/B/paperload"
python3 scripts/paper_p8_client.py export-results --workspace "$ROOT/B/workspace" \
  --output "$ROOT/B/exported-results"
```

### A — import, kiểm định và đóng fixture

Chuyển nguyên thư mục export vào `A/received-results/`. Sau đó:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$ROOT/A" \
  --experiment E2 --session-id SESSION --phase MEASURING \
  --phase-id "samples-$SAMPLES" --results "$ROOT/A/received-results"
python3 scripts/paper_p8_server.py status --root "$ROOT/A"
python3 scripts/paper_p8_server.py finalize --root "$ROOT/A" \
  --experiment E2 --session-id SESSION --phase VERIFYING
```

Chỉ xem lượt hoàn tất khi validation hợp lệ, tiến trình `serve` trả mã 0, status cuối là `CLOSED` / `cleanup_outcome=VERIFIED`, và startup report xác nhận cleanup chính xác. Validation, cleanup và exit code là các điều kiện độc lập. Không gộp baseline hoặc kết quả của hai profile.

## Kết quả thực chạy — 2026-10-30

| Profile | Ledger / raw / temporal / configuration | A/B generated = offered unique | App ACK A/B | Pending A/B cuối lượt | Phase overlap | Cleanup | Serve exit |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 samples/message | 24 / 2,400 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 3,414,891 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |
| 1 sample/message | 24 / 24 / 24 / 0 | 8 / 16 | 8 / 16 | 0 / 0 messages, 0 / 0 bytes | 57,792 ns (runner receipt; pipe lag chưa biết) | 12 containers, 1 network, 5 volumes — VERIFIED | 0 |

Counters riêng B không đồng nghĩa với unique: profile 100 có B=41 publish attempts, 25 retries, 41 broker PUBACKs nhưng 16 unique messages / 16 application ACKs. A có 8 attempts / PUBACKs và 8 application ACKs. Cả hai lượt validation là `valid=true`, `failures=[]`; DB delta theo Gateway là A=8, B=16 ledger/temporal, raw=800/1,600 (profile 100) và raw=8/16 (profile 1). Kết quả ingress/collector nằm trong từng report; không dùng để đổi cách đếm message. Không suy diễn latency xuyên process, fairness, capacity hoặc physical A/B.

Test deterministic (bao gồm safe failure-code allowlist): **24 PASS**. Hai module test liên quan chạy cùng nhau — **211 PASS** (`PYTHONPATH=scripts/tests:scripts`); `py_compile` và `git diff --check` PASS.

Artifact roots:

- Profile 100: `/var/tmp/opencode/p8-e2-helper-100-attempt8/`; evidence `e2-helper-cli-smoke.json`, reports trong `A/fixture/artifacts/`.
- Profile 1: `/var/tmp/opencode/p8-e2-helper-1-attempt1/`; evidence `e2-helper-cli-smoke.json`, reports trong `A/fixture/artifacts/`.
- Các thất bại trước đây được giữ nguyên. Attempt 8, native `serve` trả 0 và reports/cleanup hợp lệ; script bao ngoài ghi FAIL sau khi server đóng do lỗi đường dẫn đọc workload trong bước tổng hợp. Evidence PASS riêng được dựng bằng cách đọc lại artifacts đã hoàn thành; không chạy lại hay sửa artifact FAIL lịch sử.

## Giới hạn và trạng thái gate

`full_p8_accepted=false`; `physical_ab=NOT RUN`; cùng máy nên `generator_colocated=true`. E2 chỉ xác nhận pilot synthetic với hai profile đã chạy. Không phải capacity/soak, không chứng minh Gateway outbox, durable restart, production readiness hay physical A/B. E3–E5 vẫn hoãn.
