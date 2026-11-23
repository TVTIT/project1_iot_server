#!/bin/sh
# Applies post-Storage compatibility bootstrap for Stage 8 media playback signer role and RLS policy.
# Verifies database connectivity and executes scripts/post-storage-bootstrap.sql.
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SQL_FILE="$SCRIPT_DIR/post-storage-bootstrap.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "ERROR: Missing $SQL_FILE" >&2
    exit 1
fi

PGHOST="${PGHOST:-postgres}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-iot_platform}}"
PGPORT="${PGPORT:-5432}"

if [ -n "${POSTGRES_PASSWORD:-}" ] && [ -z "${PGPASSWORD:-}" ]; then
    export PGPASSWORD="$POSTGRES_PASSWORD"
fi

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" --set ON_ERROR_STOP=1 -f "$SQL_FILE"
echo "Post-Storage media playback policy applied successfully."
