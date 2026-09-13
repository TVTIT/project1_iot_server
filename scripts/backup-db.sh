#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${PROJECT_ROOT}/backups"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_FILE="${BACKUP_DIR}/iot_platform_${TIMESTAMP}.sql.gz"

mkdir -p "${BACKUP_DIR}"

echo "=== Starting PostgreSQL + TimescaleDB Backup ==="
echo "Target: ${BACKUP_FILE}"

docker exec iot_postgres pg_dump -U postgres iot_platform | gzip > "${BACKUP_FILE}"

FILESIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
echo "=== Backup completed successfully! Size: ${FILESIZE} ==="
