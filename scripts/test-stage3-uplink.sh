#!/bin/sh
# Bounded real integration entrypoint for the Task 3.6 slices:
#   * --scenario normal        (3.6.1-3.6.2) two-Gateway normal uplink path
#   * --scenario tls-credential (3.6.3 rows 1-3) TLS/credential denial slice
#   * --scenario acl-control    (3.6.3 rows 4-5, 7) cross-Gateway publish/subscribe
#     and DynSec control-topic denial slice
#   * --scenario ack-fault      (3.6.3 row 6 + 3.6.4) ACK spoof denial and
#     lost-application-ACK byte-exact resend slice
#   * --scenario db-timeout     (3.6.5 steps 1-2) database timeout on Sensor row lock
#     and byte-exact resend recovery slice
#   * --scenario queue-full     (3.6.5 steps 3-6) queue-full pressure and recovery slice
#   * --scenario gateway-reconnect (3.6.6 step 1) ordinary Gateway network disconnect,
#     reconnect, resubscribe, and byte-exact resend slice
#   * --scenario process-restart (3.6.6 steps 2-3) controlled backend restart and
#     Gateway CLI restart slice
#   * --scenario broker-controlled-restart (3.6.7) broker controlled restart with
#     credential and ACL persistence slice
# It builds the backend and gateway targets from src/Dockerfile once, then runs
# the real images through the pinned Mosquitto 2.0.18 broker to TimescaleDB with
# independent SQL verification. Disposable fixture; never sources the deployment
# .env.
#
# The remaining Task 3.6 groups (3.6.5-3.6.7) are declared NOT RUN by the
# runner; this script makes no full-task PASS claim.
#
# Exit codes: 0 = PASS or explicit SKIP (docker daemon/CLI unavailable);
#             1 = FAIL; 2 = FAIL (scenario rejected). A run that prints neither
#             the explicit SKIP line nor the terminal PASS line is a failure, so
#             a silently skipped or partial run can never look green.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/tests/stage3_uplink_runner.py"

output="$(python3 "$RUNNER" "$@" 2>&1)" && status=0 || status=$?
printf '%s\n' "$output"

if [ "$status" -ne 0 ]; then
    echo "FAIL: stage3 uplink integration exited with status $status" >&2
    exit 1
fi

if printf '%s\n' "$output" | grep -q '^SKIP: docker daemon or CLI unavailable$'; then
    echo "SKIP: stage3 uplink integration (docker daemon or CLI unavailable)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (all scenarios passed: normal, tls-credential, acl-control, ack-fault, db-timeout, queue-full, gateway-reconnect, process-restart, broker-controlled-restart)$'; then
    echo "PASS: stage3 uplink integration (all)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=normal) two-Gateway normal path verified$'; then
    echo "PASS: stage3 uplink integration (scenario=normal)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=tls-credential) TLS/credential denial slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=tls-credential)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=acl-control) cross-Gateway/control ACL denial slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=acl-control)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=ack-fault) ACK spoof denial and lost-ACK byte-exact resend slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=ack-fault)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=db-timeout) database timeout and resend recovery slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=db-timeout)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=queue-full) queue-full pressure and recovery slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=queue-full)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=gateway-reconnect) ordinary Gateway disconnect, reconnect, resubscribe, and byte-exact resend slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=gateway-reconnect)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=process-restart) controlled backend restart and Gateway CLI restart slice verified$'; then
    echo "PASS: stage3 uplink integration (scenario=process-restart)"
    exit 0
fi

if printf '%s\n' "$output" | grep -q '^PASS: stage3 uplink integration (scenario=broker-controlled-restart) broker controlled restart with credential and ACL persistence verified$'; then
    echo "PASS: stage3 uplink integration (scenario=broker-controlled-restart)"
    exit 0
fi

echo "FAIL: no explicit SKIP and no terminal PASS line were produced" >&2
exit 1
