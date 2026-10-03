#!/bin/sh
# Isolated PoC and actual production image targets; never sources deployment .env.
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$ROOT/scripts/tests/stage2_mosquitto_runtime.py"
