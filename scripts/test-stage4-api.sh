#!/bin/sh
# Bounded real integration entrypoint for Stage 4 API & read/mutation pipeline (Task 4.7):
#   * --scenario vertical  (4.7.3) vertical MQTT commit -> HTTP read -> PATCH -> ingest slice
#   * --scenario contract  (4.7.4) HTTP contract and authorization coverage slice
#   * --scenario queries   (4.7.5) query measurements and compressed history slice
#   * --scenario all       (default) runs all mandatory Task 4.7 slices in sequence
#   * --lifecycle-only     runs isolated fixture lifecycle verification only
#
# It builds the backend and gateway targets from src/Dockerfile, then runs
# the real images through the pinned Mosquitto 2.0.18 broker to TimescaleDB
# with non-superuser iot_backend_app credentials and strict resource cleanup.
#
# Exit codes: 0 = PASS or explicit SKIP (docker daemon/CLI unavailable);
#             1 = FAIL; 2 = FAIL (unknown scenario rejected). A run that prints neither
#             the explicit SKIP line nor the terminal PASS line is a failure.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/tests/stage4_api_runner.py"

output="$(python3 "$RUNNER" "$@" 2>&1)" && status=0 || status=$?
printf '%s\n' "$output"

if [ "$status" -ne 0 ]; then
    echo "FAIL: stage4 api integration exited with status $status" >&2
    exit 1
fi

if printf '%s\n' "$output" | grep -q '^SKIP: docker daemon or CLI unavailable$'; then
    echo "SKIP: stage4 api integration (docker daemon or CLI unavailable)"
    if [ "${STAGE4_REQUIRE_RUN:-0}" = "1" ] || [ "${MANDATORY_CI:-0}" = "1" ] || [ "${CI:-false}" = "true" ]; then
        echo "FAIL: explicit SKIP rejected because execution is mandatory in CI" >&2
        exit 1
    fi
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage4 api integration (all scenarios passed: vertical, contract, queries)$'; then
    echo "PASS: stage4 api integration (all)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage4 api integration (scenario=vertical)'; then
    echo "PASS: stage4 api integration (scenario=vertical)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage4 api integration (scenario=contract)'; then
    echo "PASS: stage4 api integration (scenario=contract)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage4 api integration (scenario=queries)'; then
    echo "PASS: stage4 api integration (scenario=queries)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage4 api lifecycle verified (lifecycle-only)$'; then
    echo "PASS: stage4 api lifecycle (lifecycle-only)"
    exit 0
fi

echo "FAIL: stage4 api integration completed without terminal PASS marker" >&2
exit 1
