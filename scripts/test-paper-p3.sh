#!/bin/sh
# P3 control on an owned P2 Auth/HTTPS testbed; never deployment credentials.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAGE6=0
if [ "${1:-}" = "--stage6" ]; then
    STAGE6=1
    shift
fi
if [ "$#" -gt 1 ]; then
    echo "usage: sh scripts/test-paper-p3.sh [--stage6] [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
RUN=${1:-/tmp/opencode/paper-p3-$(date +%Y%m%d-%H%M%S)-$$}
STAGE6_ARGS=
if [ "$STAGE6" -eq 1 ]; then
    STAGE6_ARGS=--stage6-p3
fi
set +e
if [ "$STAGE6" -eq 1 ]; then
    python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p3 "$STAGE6_ARGS"
else
    python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p3
fi
RUNNER_STATUS=$?
set -e
python3 - "$ROOT" "$RUN" "$RUNNER_STATUS" "$STAGE6" <<'PY'
import json
from pathlib import Path
import sys

root, run_directory, exit_code, stage6 = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3]), sys.argv[4] == '1'
sys.path.insert(0, str(root / 'scripts' / 'tests'))
import paper_p3_support

summary = run_directory / 'artifacts' / 'startup-summary.json'
try:
    report = json.loads(summary.read_text())
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f'P3 require-run rejected: missing or malformed {summary}: {exc}')
try:
    paper_p3_support.require_paper_p3_run(report, exit_code, require_stage6=stage6)
except ValueError as exc:
    raise SystemExit(f'P3 require-run rejected: {exc}')
print('P3 Stage 6 require-run accepted: native execution, profile, scenario and exact cleanup verified'
      if stage6 else
      'P3 require-run accepted: PAPER_P3_ACCEPTED; native execution and exact cleanup verified')
PY
