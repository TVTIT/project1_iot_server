#!/bin/sh
# Stage 2 Unified E2E Test Runner on an Owned Isolated Stack
# Never sources deployment .env. Runs only in an owned disposable stack.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
LOG_FILE="${STAGE2_E2E_LOG:-/tmp/opencode/task27-isolated-e2e.log}"
export STAGE2_E2E_LOG="$LOG_FILE"
export STAGE2_LOG_VIA_STDOUT_ONLY=1
mkdir -p /tmp/opencode

run_and_tee() {
    status_file="$(mktemp /tmp/opencode/status-XXXXXX)"
    (
        set +e
        "$@"
        echo $? > "$status_file"
    ) 2>&1 | tee -a "$LOG_FILE"
    status="$(cat "$status_file" 2>/dev/null || echo 1)"
    rm -f "$status_file"
    case "$status" in
        ""|*[!0-9]*) status=1 ;;
    esac
    if [ "$status" -ne 0 ]; then
        exit "$status"
    fi
}

if [ "${STAGE2_SKIP_UNIT_GUARDS:-0}" != "1" ]; then
    echo "=== Running Task 2.7 Unit Guards ===" | tee -a "$LOG_FILE"
    run_and_tee python3 -m unittest discover -s "$PROJECT_ROOT/scripts/tests" -p 'test_stage2_e2e.py'
fi

echo "=== Running Task 2.7 Isolated E2E Suite ===" | tee -a "$LOG_FILE"
run_and_tee python3 "$PROJECT_ROOT/scripts/tests/stage2_e2e.py" "$@"
