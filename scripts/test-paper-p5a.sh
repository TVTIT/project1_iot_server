#!/bin/sh
# Run one isolated finite P5a workload profile.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
    echo "usage: sh scripts/test-paper-p5a.sh <off|on> <100|1> [NEW_RUN_DIRECTORY] [--shutdown-smoke]" >&2
    exit 2
fi
MODE=$1
DATASET=$2
case "$MODE" in
    off|on) ;;
    *) echo "mode must be off or on" >&2; exit 2 ;;
esac
case "$DATASET" in
    100|1) ;;
    *) echo "dataset must be 100 or 1" >&2; exit 2 ;;
esac
if [ "$#" -ge 3 ]; then
    RUN=$3
else
    SCRATCH=/var/tmp/opencode/paper-p5a
    mkdir -p "$SCRATCH"
    RUN=$SCRATCH/run-$(date +%Y%m%d-%H%M%S)-$$
fi
EXTRA=
if [ "$#" -eq 4 ]; then
    if [ "$4" != "--shutdown-smoke" ] || [ "$MODE" != "on" ] || [ "$DATASET" != "1" ]; then
        echo "--shutdown-smoke requires on/1 and a new run directory" >&2
        exit 2
    fi
    EXTRA=--paper-p5a-shutdown-smoke
fi
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p5a --mode "$MODE" --dataset "$DATASET" $EXTRA
