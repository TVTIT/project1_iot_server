#!/bin/sh
# Bounded real integration smoke for the Task 3.5 Gateway simulator slice:
# the actual cmd/gateway CLI with its durable SQLite outbox publishes one
# synthetic batch through the pinned Mosquitto broker to the real backend and
# TimescaleDB, then independent SQL verifies the known synthetic envelope, the
# reported-state advance and the preserved desired/Gateway state. Disposable
# fixture; never sources the deployment .env.
#
# Exit codes: 0 = PASS or explicit SKIP (docker daemon/CLI unavailable);
#             1 = FAIL. A run that prints neither the explicit SKIP line nor the
#             terminal PASS line is treated as a failure, so a silently skipped
#             or partial run can never look green.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/tests/stage3_mqtt_ingress_runner.py"

output="$(python3 "$RUNNER" --mode simulator 2>&1)" && status=0 || status=$?
printf '%s\n' "$output"

if [ "$status" -ne 0 ]; then
    echo "FAIL: gateway simulator smoke exited with status $status" >&2
    exit 1
fi

if printf '%s\n' "$output" | grep -q '^SKIP: docker daemon or CLI unavailable$'; then
    echo "SKIP: gateway simulator smoke (docker daemon or CLI unavailable)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 gateway simulator end-to-end smoke'; then
    echo "PASS: gateway simulator smoke"
    exit 0
fi

echo "FAIL: no explicit SKIP and no terminal PASS line were produced" >&2
exit 1
