#!/bin/sh
# Prepare one isolated, comparable P6 profile. Slice A does not execute load.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "usage: sh scripts/test-paper-p6.sh <polling|wake-up> <100|1> [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
MODE=$1
DATASET=$2
case "$MODE" in polling|wake-up) ;; *) echo "mode must be polling or wake-up" >&2; exit 2 ;; esac
case "$DATASET" in 100|1) ;; *) echo "dataset must be 100 or 1" >&2; exit 2 ;; esac
if [ "$#" -eq 3 ]; then
    RUN=$3
else
    SCRATCH=/var/tmp/opencode/paper-p6
    mkdir -p "$SCRATCH"
    RUN=$SCRATCH/prepare-$(date +%Y%m%d-%H%M%S)-$$
fi
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" \
    --paper-p6 --dispatch-mode "$MODE" --dataset "$DATASET"
