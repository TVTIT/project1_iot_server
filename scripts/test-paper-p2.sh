#!/bin/sh
# Isolated P2 fixture; never reads deployment credentials into the testbed.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -gt 1 ]; then
    echo "usage: sh scripts/test-paper-p2.sh [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
RUN=${1:-/tmp/opencode/paper-p2-$(date +%Y%m%d-%H%M%S)-$$}
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p2
