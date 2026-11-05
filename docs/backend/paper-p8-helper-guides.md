# P8 helper CLI operator guides

## Workflow hiện hành (BS.9a, 05/11/2026)

Điều phối thủ công, không phải một lệnh xuyên hai host:

1. **A:** `paper_p8_server.py serve` tạo fixture/session và bundle; `status` chờ
   READY. Chỉ tạo thư mục cha, không tạo trước thư mục lá `--root`.
2. **A → B:** Sao chép bundle và binary `paperload`/`gateway` đúng OS/architecture
   qua kênh được bảo vệ. Không chuyển DB/admin/Docker credentials.
3. **B:** `paper_p8_client.py bind`, `preflight` (binary thật),
   `check-connection`; sau khi A báo `MEASURING` + `collector_ready=true`, chạy
   `run`, rồi `export-results`. Bounded `run` không tự dọn pending/journal thành
   PASS khi lỗi.
4. **B → A:** Chuyển exported projection thủ công; A dùng `finish-phase` để
   validate/import, `next-phase` cho phase kế tiếp nếu có, rồi `finalize` và
   receipt-verified `cleanup` khi phiên đã đóng. `abort` là dừng phiên, không
   xóa bằng chứng/pending để ép kết quả xanh.

Native automated selectors trên một host đã PASS cho các pilot được nêu trong
runbook riêng; điều đó không phải physical A/B qualification. B không cần và
không được cấp DB/admin/Docker access. Cú pháp flags lấy từ `--help` hiện hành,
không suy diễn từ manifest hay ví dụ lịch sử.

- [E1 — WebSocket/MQTT pilot helper gate](paper-p8-e1-remote-gate.md)
- [E2 — shaper/microbatch telemetry A/B pilot helper](paper-p8-e2-remote-gate.md)
- [E3 — profile helper (low-load / mixed; polling / wakeup)](paper-p8-e3-helper-preparation.md)
- [E4 — sequential UNIQUE → WARM REPLAY subset](paper-p8-e4-preparation.md)
- [E4 durable SQLite fault-stop/reopen](paper-p8-e4-durable-reopen.md)
- [E4 representative conflict rejection](paper-p8-e4-conflicts.md)
- [E5 — complete manual A/B helper workflow](paper-p8-e5-preparation.md)

E3 pure helpers reside at `scripts/paper_p8_e3_support.py` (not under
`scripts/tests/`). These are source-qualified local helper workflows only. They do not turn
`full_p8_accepted` true or establish physical A/B.
- E1 selected native 128-viewer pilot passes; 5k/10k are offline contract only.
- E2 native S=100/S=1 pilot is 24 unique total across producer A+B; official rates NOT RUN.
- E3 corrected 3-command low-load/steady polling and wakeup subset passes. Official30 is opt-in offline plan/preflight only; pure helpers are `scripts/paper_p8_e3_support.py`.
- E4 native cache on/off 8 unique + 2 replay passes; official 1,000+100 plan only and real TTL 600s NOT RUN.
- E5 six-arm representative pilot has one repeat; official >=3 independent repeats remain a repeat plan, not capacity evidence.

Physical A/B, capacity soak ($\ge 3$ official repeats), physical hardware sensors, and full P8 acceptance remain unrun. See the [P8 acceptance report](paper-p8-experiments-and-acceptance.md) and [central limitations/backlog](../dynsec/limitations-and-backlog.md) for overall checkpoint status.
