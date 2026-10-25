#!/bin/sh
# P3 control on an owned P2 Auth/HTTPS testbed; never deployment credentials.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -gt 1 ]; then
    echo "usage: sh scripts/test-paper-p3.sh [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
RUN=${1:-/tmp/opencode/paper-p3-$(date +%Y%m%d-%H%M%S)-$$}
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p3
