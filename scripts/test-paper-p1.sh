#!/bin/sh
# Same isolated lifecycle as P0; the mode is not an optimization switch.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if { [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; } || [ "$1" != '--mode' ] || { [ "$#" -eq 4 ] && [ "$4" != '--metrics-off' ]; }; then
    echo 'Usage: sh scripts/test-paper-p1.sh --mode baseline|optimized NEW_RUN_DIRECTORY [--metrics-off]' >&2
    exit 2
fi
MODE=$2
DIRECTORY=$3
shift 3
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --paper-p1 --mode "$MODE" --prepare "$DIRECTORY" "$@"
