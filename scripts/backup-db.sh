#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${PROJECT_ROOT}/backups"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

# Load configuration from .env if present
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
fi

DB_USER="${POSTGRES_USER:-postgres}"
DB_NAME="${POSTGRES_DB:-iot_platform}"
BACKUP_FILE="${BACKUP_DIR}/${DB_NAME}_${TIMESTAMP}.sql.gz"

mkdir -p "${BACKUP_DIR}"

echo "=== Starting PostgreSQL + TimescaleDB Backup ==="
echo "Target: ${BACKUP_FILE}"

docker exec iot_postgres pg_dump -U "${DB_USER}" "${DB_NAME}" | gzip > "${BACKUP_FILE}"

FILESIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
echo "=== Backup completed successfully! Size: ${FILESIZE} ==="
