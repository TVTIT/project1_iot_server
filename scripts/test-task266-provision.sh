#!/bin/sh
# Owned fixture only. No deployment .env, compose, broker or database is used.
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$ROOT/scripts/tests/task266_provision.py"
