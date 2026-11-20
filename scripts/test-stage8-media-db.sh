#!/bin/sh
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$PROJECT_ROOT/scripts/tests/stage8_media_db_fixture.py"
