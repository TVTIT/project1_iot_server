# E4 durable SQLite fault-stop/reopen — operator guide

Đây là lane tùy chọn của E4, được nối sau các phase base đã bật. Hướng dẫn dành
cho lần native proof kế tiếp; **chưa có native/Docker acceptance ở dispatch này**.
Không chạy nếu quota, fixture profile hoặc artifact storage chưa được operator xác
nhận. Không chỉnh SQLite/journal để làm cho proof đạt.

## Mở session

Trên A terminal 1, bật cờ khi khởi tạo. Có thể thêm `--cold-replay` nếu cần; khi
đó thứ tự chính xác là `unique → replay → cold-replay → durable-fault →
durable-reopen`. Không có `--cold-replay` thì hai durable phase theo sau replay.
Các cờ cache-mode/warm/off hiện hữu không đổi nghĩa.

```sh
export A=/absolute/path/to/A-root
python3 scripts/paper_p8_server.py serve --root "$A" \
  --network-config /absolute/path/to/network-config.json \
  --experiment E4 --cache-mode on --durable-reopen
```

Lưu `session_id` do `status` báo thành `$SESSION_ID`. Giữ server foreground hoặc
ghi PID/exit code; không khởi chạy session thứ hai trên cùng root. Hoàn thành và
import UNIQUE/REPLAY (và COLD-REPLAY nếu chọn) theo
`paper-p8-e4-preparation.md`. Mọi bước dưới đây yêu cầu status cho thấy phase trước
đã `VALIDATED`, và `next-phase` đã trả trạng thái `READY` trước `begin`.

## Durable fault — dừng bất thường được dự kiến

A terminal 2: bắt đầu baseline chỉ sau khi phase trước đã validated.

```sh
python3 scripts/paper_p8_server.py next-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase VERIFYING --phase-id durable-fault
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
python3 scripts/paper_p8_server.py begin --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase READY --phase-id durable-fault
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Chỉ khi A là `MEASURING`, B chạy đúng stage cố định:

```sh
python3 scripts/paper_p8_client.py durable-stage --workspace "$B/workspace" \
  --gateway-binary "$B/gateway" --inputs "$B/workspace/runtime/durable-probe.json" \
  --stage fault
python3 scripts/paper_p8_client.py export-durable-results --workspace "$B/workspace" \
  --output "$B/durable-fault-results" --stage durable-fault
```

Gateway child dự kiến thoát với code 1 do fault injection; **đó không phải wrapper
failure**. Wrapper chỉ báo stage thành công khi child đã join, đã có đúng một ACK hợp
lệ bị bỏ qua có chủ đích, và SQLite giữ nguyên một pending batch 100 mẫu. Giữ nguyên
SQLite và outputs. Nếu wrapper trả lỗi, thiếu evidence hoặc pending không đúng, dừng
và abort/recover; tuyệt đối không chạy `reopen` để tự sửa trạng thái.

Copy nguyên `durable-fault-results` vào
`$A/fixture/private/incoming/durable-fault/`. A terminal 2 import sau khi child B
đã dừng; A đọc SQL độc lập và so sánh ledger hash, đúng 100 raw rows, identity và
fingerprint của dataset chính:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase MEASURING --phase-id durable-fault \
  --results "$A/fixture/private/incoming/durable-fault"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Chuyển tiếp chỉ hợp lệ khi A status báo `VALIDATED`/`durable_verified` cho
`durable-fault`. Nếu import hoặc SQL mismatch: giữ pending nguyên trạng, không
chuyển phase.

## Durable reopen — replay nguyên pending state

Chỉ sau xác minh fault thành công, A terminal 2 yêu cầu phase kế tiếp rồi baseline:

```sh
python3 scripts/paper_p8_server.py next-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase VERIFYING --phase-id durable-reopen
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
python3 scripts/paper_p8_server.py begin --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase READY --phase-id durable-reopen
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Khi A xác nhận `MEASURING`, B khởi động lại cùng workspace/SQLite:

```sh
python3 scripts/paper_p8_client.py durable-stage --workspace "$B/workspace" \
  --gateway-binary "$B/gateway" --inputs "$B/workspace/runtime/durable-probe.json" \
  --stage reopen
python3 scripts/paper_p8_client.py export-durable-results --workspace "$B/workspace" \
  --output "$B/durable-reopen-results" --stage durable-reopen
```

Copy nguyên thư mục vào `$A/fixture/private/incoming/durable-reopen/`, rồi A nhập:

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase MEASURING --phase-id durable-reopen \
  --results "$A/fixture/private/incoming/durable-reopen"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

A kiểm tra độc lập payload/ledger, boot/sequence, timestamp và khoảng cách chính xác
10.000 µs cho 100 raw rows; xác nhận pending/quarantine bằng 0, ACK ứng dụng ít
nhất 2, batch mới được tách khỏi batch phục hồi, và main dataset không đổi. Không
được seed hoặc repair pending rows để đạt PASS.

## Finalize, abort và trạng thái evidence

Sau khi phase cuối được `VALIDATED`, A terminal 2 finalize và chờ server thoát:

```sh
python3 scripts/paper_p8_server.py finalize --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase VERIFYING
```

Kiểm tra exit code, final artifact trong thư mục artifacts của session, `exact_cleanup_verified`
và `durable_gateway_reopen.status`. Durable subset không phải full E4/P8 acceptance:
`full_p8_accepted=false`, `conflicts=NOT RUN`, `physical_ab=NOT RUN`. Cleanup sạch
không biến import/validation lỗi thành PASS. Khi bất kỳ bước nào lỗi, giữ outputs,
bundle, SQLite và state; không lặp stage trên cùng workspace, không sửa session JSON,
không `compose down`/prune. Dùng abort/recovery theo receipt hiện hữu.

**Native qualification: NOT RUN.** Lượt triển khai này chỉ chạy offline tests; mọi
native, Docker, DB/broker và HIL claim cần được thiết lập riêng ở lần proof kế tiếp.

## Native attempt 1 — FAIL, giữ nguyên evidence

Ngày 2026-10-30 đã chạy đúng **một** helper gate foreground tại
`/var/tmp/opencode/e4-durable-native-one-20261030` (dưới 1 GiB dữ liệu trong root).
Network bind preflight, bundle bind, binary copy/digest, client preflight/TLS, và
E4 `unique` + `replay` thật đều đạt `VALIDATED`. Khi A xử lý `begin durable-fault`,
callback baseline bị lỗi arity:
`_durable_sensor_counts() missing 1 required positional argument: 'prepared'`.
Do đó không chạy B `durable-stage fault`; intentional process exit, pending SQLite
payload, SQL ledger/raw check, reopen và final durable subset đều **NOT RUN**.
Không có fault proof nào được chấp nhận và không tuyên bố durable PASS.

Orchestrator đã gửi `abort`, A thoát code 1 với workload `ABORTED`, phase history
`[unique, replay]`; fixture cleanup được xác minh chính xác: 12 containers, 1
network, 5 volumes. Không còn server/fixture process hoạt động. Giữ nguyên toàn bộ
attempt, logs, artifacts, bundle và SQLite nếu có tại root nêu trên; không xóa hoặc
retry native trong lần gọi này. `gate-result.json` ghi kết quả FAIL. Lỗi callback đã
được sửa trong source bằng cách truyền `prepared['pg']`; regression offline và
`py_compile`/`git diff --check` chạy lại đạt. Sửa này **không** nâng cấp attempt đã
fail thành PASS. Native proof cần được cấp phép ở một dispatch riêng.

## Native attempt 2 — FAIL (outer acceptance unmerged), giữ nguyên evidence

Sau khi sửa callback arity, native helper đầy đủ đã chạy tại
`/var/tmp/opencode/e4-durable-complete-n3k05y_r`. Cả 4 phases UNIQUE, REPLAY,
DURABLE-FAULT, DURABLE-REOPEN đều đã đạt `VALIDATED`; các kiểm tra SQLite fault-stop
+ resend/drain, SQL/REST/raw checks thành công; session đóng và cleanup verified:
12 containers, 1 network, 5 volumes.

Tuy nhiên, final artifact `p8-e4-on.json` nhận kết quả `FAIL` do tại thời điểm chạy,
outer acceptance `paper_p8_e4_warm_subset_acceptance` và startup summary branch trong
`native_compose_smoke.py` chưa hỗ trợ nhánh durable subset (hardcode `durable_gateway_reopen == 'NOT RUN'`,
phase history không gồm durable phases, và marker/status chưa có durable).

Attempt lịch sử tại `/var/tmp/opencode/e4-durable-complete-n3k05y_r` được giữ nguyên
trạng thái `FAIL`, không chỉnh sửa đè artifact. Logic outer acceptance và startup
summary đã được bổ sung và xác minh offline qua bộ test unit/pipeline. Lượt dispatch
này không chạy native/Docker; claim native PASS đầy đủ cần một lượt native CLI gate
riêng do parent session chạy sau khi code fix đã được tích hợp.

## Native durable accepted — PASS (`/var/tmp/opencode/e4-durable-accepted-w7lfvuih`)

Lượt chạy native gate đầy đủ tiếp theo của parent session đã đạt **PASS** trọn vẹn tại
`/var/tmp/opencode/e4-durable-accepted-w7lfvuih`:
- `phase_history`: `['unique', 'replay', 'durable-fault', 'durable-reopen']`.
- `server_exit_code`: `0`, `cleanup_outcome`: `VERIFIED`, `exact_cleanup_verified`: `true`
  dọn sạch 12 containers, 1 network, 5 volumes.
- Fault stage: intentional exit code 1, `ignored_valid_acks`: 1, SQLite pending batch giữ nguyên
  `payload_sha256`: `04e0f00b7d3047e2852a52527655373c99a30ab177372b8e854b540f2420f45d`,
  `payload_bytes`: 925, SQL committed raw count: 100.
- Reopen stage: exit code 0, drain về 0 pending rows / 0 quarantined rows, `application_acks`: 2.
  Batch phục hồi và batch mới được phân lập chính xác: 100 raw ban đầu giữ nguyên, 100 raw mới của
  auxiliary probe được ghi nhận (`auxiliary_delta`: `{'ledger': 2, 'raw': 200, 'temporal': 2}`).
- `lane_marker`: `PAPER_P8_E4_DURABLE_REOPEN_SUBSET`, `workload_status`: `DURABLE_REOPEN_SUBSET_VALIDATED`.
- Giới hạn: `full_p8_accepted`: `false`, `physical_ab`: `NOT RUN`, `conflicts`: `NOT RUN`.
- Mọi attempt FAIL lịch sử trước đó (`e4-durable-native-one-20261030`, `e4-durable-complete-n3k05y_r`)
  tiếp tục được lưu giữ nguyên vẹn, không đổi nhãn hoặc đánh số lại.
