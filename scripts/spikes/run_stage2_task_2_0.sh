#!/bin/sh
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [ ! -f .env ]; then
    echo ".env is required to exercise the local GoTrue stack." >&2
    exit 1
fi

set -a
# The repository-root .env is the selected local configuration source.
# shellcheck disable=SC1091
. ./.env
set +a

python3 scripts/spikes/jwt_contract.py
sh scripts/spikes/mosquitto_password_reload.sh
sh scripts/spikes/migration_upgrade_probe.sh
