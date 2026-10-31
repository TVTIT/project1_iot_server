# P8 helper CLI operator guides

- [E1 — WebSocket/MQTT pilot helper gate](paper-p8-e1-remote-gate.md)
- [E2 — shaper/microbatch telemetry A/B pilot helper](paper-p8-e2-remote-gate.md)
- [E3 — profile helper (low-load / mixed; polling / wakeup)](paper-p8-e3-helper-preparation.md)
- [E4 — sequential UNIQUE → WARM REPLAY subset](paper-p8-e4-preparation.md)
- [E4 durable SQLite fault-stop/reopen](paper-p8-e4-durable-reopen.md)
- [E4 representative conflict rejection](paper-p8-e4-conflicts.md)
- [E5 — complete manual A/B helper workflow](paper-p8-e5-preparation.md)

These are source-qualified local helper workflows only. They do not turn
`full_p8_accepted` true or establish physical A/B.
- E1 verifies WebSocket viewer density (64 sockets, 2 namespaces) and delivery accounting.
- E2 verifies throughput, latency breakdown, and burst behavior (100 vs 1 sample profiles).
- E3 runs four profiles across low-load and mixed-load background telemetry, testing command life cycles (polling vs wakeup) with command overlap verification and dual SQLite executor reconciliation.
- E4 supports sequential UNIQUE → REPLAY, optional cold restart, durable Gateway SQLite fault/reopen, and representative conflict rejection through dedicated helper options.
- E5 supports the complete multi-phase manual A/B helper workflow (`initial` → `warm_replay` → `control`) with continuous two-viewer monitoring across phases, durable Gateway control executors, post-stop drain with fresh outbox readback, and offline multi-arm comparison export across 1 reference (`optimized`) and 5 ablation arms (`ram_dedup_off`, `fanout_scan`, `shaper_off`, `microbatch_off`, `polling`).

Physical A/B, capacity soak ($\ge 3$ official repeats), physical hardware sensors, and full P8 acceptance remain unrun. See the [P8 acceptance report](paper-p8-experiments-and-acceptance.md) and [central limitations/backlog](../dynsec/limitations-and-backlog.md) for overall checkpoint status.
