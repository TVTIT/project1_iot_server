#!/bin/sh
# Reusable local automation gate for Stage 8 media storage playback.
# Enforces require-run assertions, exact cleanup, and safe summary staging.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ "$#" -lt 1 ]; then
    echo "usage: sh scripts/test-stage8-playback-gate.sh <NEW_PRIVATE_DIRECTORY>" >&2
    exit 2
fi

RUN_DIR="$1"

# Refuse existing directory or symlink - must create a new private 0700 leaf
if [ -e "$RUN_DIR" ] || [ -L "$RUN_DIR" ]; then
    echo "Refusing existing or symlink directory: $RUN_DIR" >&2
    exit 2
fi

PARENT_DIR=$(dirname -- "$RUN_DIR")
mkdir -p "$PARENT_DIR"
mkdir -m 0700 "$RUN_DIR"

REPORT_FILE="$RUN_DIR/raw-report.json"
SAFE_REPORT_FILE="$RUN_DIR/summary.json"
RUNNER_LOG="$RUN_DIR/runner.log"

# Real git HEAD only - no zero fallback
CHECKOUT_SHA=$(git -C "$ROOT" rev-parse --verify HEAD 2>/dev/null || true)
if [ -z "$CHECKOUT_SHA" ] || [ ${#CHECKOUT_SHA} -ne 40 ]; then
    echo "Unable to determine real 40-hex git HEAD" >&2
    exit 2
fi

# Touch empty log files with private mode 0600 before execution
(umask 077 && touch "$REPORT_FILE" "$RUNNER_LOG")

set +e
# Stdout produces the sanitized JSON report; stderr is captured in private runner.log
sh "$ROOT/scripts/test-stage8-media-storage-http.sh" --stage8-local-playback > "$REPORT_FILE" 2> "$RUNNER_LOG"
RUNNER_EXIT=$?
set -e

python3 - "$ROOT" "$REPORT_FILE" "$SAFE_REPORT_FILE" "$RUNNER_EXIT" "$CHECKOUT_SHA" <<'PY'
import sys
from pathlib import Path

root, report_path, safe_path, exit_code, checkout_sha = (
    Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
)
sys.path.insert(0, str(root / "scripts" / "tests"))
import stage8_playback_support

stage8_playback_support.validate_and_stage_playback_summary(
    report_path, safe_path, checkout_sha, runner_exit_code=exit_code
)
print("PASS: stage8 playback gate accepted")
PY
