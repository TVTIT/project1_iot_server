#!/bin/sh
# Run one bounded P5b cache off/on profile with exact-byte duplicate replay.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "usage: sh scripts/test-paper-p5b-profile.sh <off|on> <100|1> [NEW_RUN_DIRECTORY]" >&2
    exit 2
fi
CACHE_MODE=$1
DATASET=$2
case "$CACHE_MODE" in off|on) ;; *) echo "cache mode must be off or on" >&2; exit 2 ;; esac
case "$DATASET" in 100|1) ;; *) echo "dataset must be 100 or 1" >&2; exit 2 ;; esac
if [ "$#" -eq 3 ]; then
    RUN=$3
else
    SCRATCH=/var/tmp/opencode/paper-p5b
    mkdir -p "$SCRATCH"
    RUN=$SCRATCH/profile-$(date +%Y%m%d-%H%M%S)-$$
fi
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$RUN" \
    --paper-p5b --cache-mode "$CACHE_MODE" --dataset "$DATASET"
