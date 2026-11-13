# E4 representative conflict rejection — Hướng dẫn vận hành CLI

Tài liệu này hướng dẫn chạy phase tùy chọn **conflicts** của E4 qua mô hình hai máy/tiến trình A (Server/Admin/DB) và B (Client/Gateway/Paperload) qua kết nối TLS bảo mật.

Phase này kiểm tra cơ chế từ chối xung đột có bảo vệ toàn vẹn dữ liệu (persisted guard) tại backend/database với hai ca kiểm thử đại diện:
1. `same_id_changed_payload`: Trùng `message_id`, payload bị sửa đổi -> Backend từ chối với lý do `message_conflict`, không ghi đè DB, không cấp application ACK.
2. `different_first_sequence_overlap`: Khác `message_id`, nhưng dãy sequence bị chồng lấn -> Backend từ chối với lý do `sequence_overlap`, không ghi đè DB, không cấp application ACK.

---

## 1. Mở session tại server A

Khởi chạy server A với cờ `--conflicts`:

```sh
export A=/absolute/path/to/A-root
python3 scripts/paper_p8_server.py serve --root "$A" \
  --network-config /absolute/path/to/network-config.json \
  --experiment E4 --cache-mode on --conflicts --deadline-seconds 900
```

Ghi nhận `$SESSION_ID` từ `status`:

```sh
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Sau khi hoàn thành hai phase cơ sở `unique` và `replay` (đều đạt `VALIDATED` và `phase_history` là `['unique', 'replay']`), tiến hành phase `conflicts`.

---

## 2. Quy trình thực hiện phase conflicts

### Bước 2.1: Client B trích xuất và xuất conflict probe

B đọc bản ghi đầu tiên có 100 mẫu từ journal SQLite thật (`outputs/e4-journal.sqlite`) và sinh 2 payload xung đột schema-valid:

```sh
python3 scripts/paper_p8_client.py export-conflict-probe \
  --workspace "$B/workspace" \
  --output "$B/conflict-probe"
```

Sao chép toàn bộ thư mục `$B/conflict-probe` sang server A tại `$A/fixture/private/incoming/conflict-probe`:

```sh
cp -r "$B/conflict-probe" "$A/fixture/private/incoming/conflict-probe"
chmod 700 "$A/fixture/private/incoming/conflict-probe"
chmod 600 "$A/fixture/private/incoming/conflict-probe"/*.json
```

### Bước 2.2: Server A kích hoạt next-phase và begin

A kiểm tra sự tồn tại của probe, kiểm tra điều kiện tiên quyết DB guard qua câu truy vấn SQL (`SELECT EXISTS (...)`), chụp snapshot fingerprint DB và log backend trước khi cho phép B gửi:

```sh
python3 scripts/paper_p8_server.py next-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase VERIFYING --phase-id conflicts
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4

python3 scripts/paper_p8_server.py begin --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase READY --phase-id conflicts
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

Chờ cho đến khi status của A chuyển sang `phase: MEASURING` và `collector_ready: true`.

### Bước 2.3: Client B thực thi conflict-stage qua TLS

B dùng đúng binary `paperload` đã preflight của session. Helper tạo các journal JSONL riêng, giữ nguyên journal gốc, rồi chạy lại đúng transport MQTT/TLS và bộ đếm workload hiện có. Một replay payload gốc phải có application ACK; mỗi case xung đột chạy trong invocation/kết nối mới và chỉ được ghi nhận là `EXPECTED_REJECTED_NO_ACK` khi workload đo được đúng một offered, 0 application ACK, một pending còn lại, PARTIAL có cleanup sạch, và child đã kết thúc trong giới hạn. Server A vẫn quyết định nghiệm thu sau khi đối chiếu log rejection và SQL guard độc lập.

```sh
python3 scripts/paper_p8_client.py conflict-stage --workspace "$B/workspace" \
  --binary "$PAPERLOAD_BINARY"
```

### Bước 2.4: Client B xuất kết quả conflict-results

B đóng gói bằng chứng `conflicts-proof.json` kèm session binding:

```sh
python3 scripts/paper_p8_client.py export-conflict-results \
  --workspace "$B/workspace" \
  --output "$B/conflict-results"
```

Sao chép sang server A:

```sh
cp -r "$B/conflict-results" "$A/fixture/private/incoming/conflicts"
chmod 700 "$A/fixture/private/incoming/conflicts"
chmod 600 "$A/fixture/private/incoming/conflicts"/*.json
```

### Bước 2.5: Server A xác minh độc lập và kết thúc phase

A đọc bundle kết quả, kiểm tra:
1. `schema_valid`: payload xung đột tuân thủ đúng schema telemetry batch.
2. `database_unchanged`: toàn bộ bảng dữ liệu (`telemetry`, `processed_messages`, `twin_temporal_values`, `twin_states`) không bị biến đổi (khớp chính xác fingerprint trước và sau).
3. Backend log ghi nhận đúng `reason_code` (`message_conflict` hoặc `sequence_overlap`).
4. Không có application ACK nào được cấp ngoài bản tin đối chứng dương.

```sh
python3 scripts/paper_p8_server.py finish-phase --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase MEASURING --phase-id conflicts \
  --results "$A/fixture/private/incoming/conflicts"
python3 scripts/paper_p8_server.py status --root "$A" --experiment E4
```

A xác nhận `workload_outcome: VALIDATED` và `phase_history: ['unique', 'replay', 'conflicts']`.

---

## 3. Finalize và nghiệm thu dọn dẹp

Server A đóng session và dọn dẹp tài nguyên sở hữu:

```sh
python3 scripts/paper_p8_server.py finalize --root "$A" --experiment E4 \
  --session-id "$SESSION_ID" --phase VERIFYING
```

Kiểm tra kết quả trong `$A/fixture/artifacts/p8-e4-on.json`:
- `status`: `PASS`
- `lane_marker`: `PAPER_P8_E4_CONFLICTS_SUBSET`
- `workload_status`: `CONFLICTS_SUBSET_VALIDATED`
- `cleanup_outcome`: `VERIFIED`
- `exact_cleanup_verified`: `true` (dọn sạch 12 containers, 1 network, 5 volumes).
- `conflicts.status`: `PASS` (hai case được xác minh độc lập; B chỉ cung cấp bằng chứng no-ACK, A xác nhận database và rejection log).

---

## 4. Bằng chứng thực tế (Native Evidence)

Lượt chạy native gate tự động khép kín đã hoàn thành thành công tại:
`/var/tmp/opencode/e4-conflicts-native-accepted-one`
- Server exit code: `0`
- Phase history: `['unique', 'replay', 'conflicts']`
- Database delta tổng: `{'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}` (được giữ nguyên vẹn từ baseline, 0 thay đổi trong phase conflicts).
- Số ca xung đột: 2/2 đạt `status: PASS`, `application_ack_count: 0`, `backend_reason_log_delta: 1`.
- Dọn dẹp tài nguyên: 12 containers, 1 network, 5 volumes được dọn dẹp hoàn toàn, không rò rỉ.

### Lưu giữ lịch sử lỗi (Retained Historical FAILs):
- `e4-conflicts-accepted-20261030`: FAIL tại `export-conflict-probe` do truy xuất `KeyError: 'run_id'` trong cấu trúc binding session (danh sách `run_ids`).
- `e4-conflicts-pilot-final`: FAIL tại `conflict-stage` với implementation transport cũ; giữ nguyên FAIL lịch sử.
- Cả hai thư mục lịch sử trên đều được bảo lưu nguyên vẹn bằng chứng lỗi, không bị ghi đè hay đánh số lại.

### Transport follow-up (Go native gate verified)

Review bảo mật loại bỏ việc truyền password trong argv, PTY/debug parsing và phụ thuộc `mosquitto_pub/sub` ở B. Bản sửa dùng binary Go được truyền qua `--binary`, ghi provenance (SHA-256/OS/architecture), tạo journal/manifests dẫn xuất trong `outputs/e4-conflicts/`, preflight từng manifest và giữ file riêng ở quyền 0600. Với manifest E4 nguồn có hai Gateway/targets, mỗi invocation dẫn xuất đúng target của journal và chỉ credential tương ứng. Case overlap từ xa dùng 100 mẫu hợp lệ: message ID mới, `first_sequence` bắt đầu tại sequence gốc + 1 nên dãy vẫn chồng lấn dãy đã commit; case local P5a hai mẫu không đổi. Offline export → bind → conflict-stage với binary Go thật xác nhận cả ba manifest preflight được; chỉ phần chạy mạng được mock.

Native gate mới dùng transport Go đã PASS tại `/var/tmp/opencode/e4-paperload-conflict-gate-20261030-02`. Mỗi case có positive-control `COMPLETE`, 1 application ACK; conflict `PARTIAL`, 0 ACK, 1 pending, 3 publish attempts/PUBACKs, 2 retries, worker joined và return code 4. A xác nhận rejection log delta 3 cho mỗi reason (`message_conflict`, `sequence_overlap`), DB fingerprints/counts không đổi (configuration 0), phase history `unique/replay/conflicts`, server exit 0 và cleanup chính xác 12 containers/1 network/5 volumes. Binary SHA-256 chung cho positive-control và cả hai conflict: `1aa1303bda21fd9e3eee8213a38a144c97134663d358fd98bdb289a6da70ff0b` (linux/x86_64).

FAIL trước đó tại `/var/tmp/opencode/e4-paperload-conflict-gate-20261030-01` được giữ nguyên: `unique/replay` VALIDATED rồi conflict dừng do helper từ chối manifest hai Gateway; cleanup cũng chính xác 12/1/5. PASS cũ tại `/var/tmp/opencode/e4-conflicts-native-accepted-one` là transport Mosquitto/PTY lịch sử, không phải bằng chứng cho transport Go mới. Mọi kết quả giữ `full_p8_accepted: false`, `physical_ab: NOT RUN`, same-host qualification.

---

## 5. Giới hạn phạm vi (Boundary & Limitations)

- Đây là kết quả kiểm thử local MVP tích hợp thực tế trên môi trường Docker đơn máy.
- `full_p8_accepted: false` luôn được áp dụng; không cam kết zero-downtime, không phải đo kiểm tải dung lượng tối đa (capacity soak) hay môi trường phân tán hai máy vật lý thực tế (`physical_ab: NOT RUN`).
- Toàn bộ journal SQLite, file khóa và cấu trúc outbox của Gateway simulator được giữ nguyên vẹn.
