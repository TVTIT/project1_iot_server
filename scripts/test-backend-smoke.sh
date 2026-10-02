#!/bin/sh
# Shares the isolated Auth harness; backend always uses iot_backend_app.
set -eu
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$PROJECT_ROOT/scripts/tests/stage2_auth.py" smoke
