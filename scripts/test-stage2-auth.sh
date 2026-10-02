#!/bin/sh
# Disposable stack only: requires Docker, Python 3.10+ and the repo Go toolchain.
# Never sources .env. Set STAGE2_BACKEND_IMAGE only to reuse a test image.
set -eu
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$PROJECT_ROOT/scripts/tests/stage2_auth.py" auth
