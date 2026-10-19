#!/bin/sh
# Opt-in workload entrypoint for Task 3.7 (600 batches x 100 samples/batch, 100 Hz).
# Shares the Task 3.6 fixture primitives without modifying the default 9-scenario integration suite.
#
# Exit codes: 0 = PASS or explicit SKIP (docker daemon/CLI unavailable);
#             1 = FAIL. A run that prints neither the explicit SKIP line nor
#             the terminal PASS line fails closed.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/tests/stage3_workload_runner.py"

output="$(python3 "$RUNNER" "$@" 2>&1)" && status=0 || status=$?
printf '%s\n' "$output"

if [ "$status" -ne 0 ]; then
    echo "FAIL: stage3 workload runner exited with status $status" >&2
    exit 1
fi

if printf '%s\n' "$output" | grep -q '^SKIP: docker daemon or CLI unavailable$'; then
    echo "SKIP: stage3 workload (docker daemon or CLI unavailable)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 workload (600 batches verified)$'; then
    echo "PASS: stage3 workload (600 batches verified)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: workload runner preflight and profile verification passed$'; then
    echo "PASS: stage3 workload preflight"
    exit 0
fi

echo "FAIL: no explicit SKIP and no terminal PASS line were produced" >&2
exit 1
