#!/bin/sh
# Thin entrypoint. The runner owns generation, startup, checks and exact cleanup.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -gt 1 ]; then
    echo 'Usage: sh scripts/test-paper-p0.sh [NEW_FIXTURE_DIRECTORY]' >&2
    exit 2
fi
FIXTURE=${1:-/tmp/opencode/paper-p0-$(python3 -c 'import secrets; print(secrets.token_hex(8))')}
exec python3 "$ROOT/scripts/tests/native_compose_smoke.py" --prepare "$FIXTURE" --paper-p0
