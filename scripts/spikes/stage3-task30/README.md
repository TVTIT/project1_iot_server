# Task 3.0 spikes

These are isolated schema/driver experiments, not application migrations or an
MQTT simulator. Results, configuration decisions and limits are recorded in
[the Task 3.0 report](../../../docs/backend/stage-3-task-3.0-baseline-and-contract.md).

From the repository root, with Docker access and the pinned TimescaleDB image:

```sh
python3 scripts/spikes/stage3-task30/ddl_spike.py
```

This reuses `scripts/tests/task28_simplified_schema_runner.py`, bootstraps the
application schema through v19 (no GoTrue), uses tmpfs without published ports,
and verifies cleanup of its own container. `candidate.sql` is deliberately
outside `migrations/` and never writes version 20 to `schema_migrations`.

The nested `sqlite/` module pins the SQLite driver separately from `src/`.
Build/run commands are in the report. Docker/host runs use temporary SQLite
files and remove them on normal completion. ARMv7 is compile-only until run on
that target. No deployment `.env`, credentials or persistent volumes are used.
