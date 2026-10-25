#!/bin/sh
# Prepare the isolated P4 profile on the P2 HTTPS/Unix topology; workload is pending.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "usage: sh scripts/test-paper-p4.sh <baseline|optimized> [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
MODE=$1
case "$MODE" in
    baseline|optimized) ;;
    *) echo "mode must be baseline or optimized" >&2; exit 2 ;;
esac
if [ "$#" -eq 2 ]; then
    RUN=$2
else
    SCRATCH=/var/tmp/opencode/paper-p4
    mkdir -p "$SCRATCH"
    RUN=$SCRATCH/run-$(date +%Y%m%d-%H%M%S)-$$
fi
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" --paper-p4 --mode "$MODE"
