#!/bin/sh
set -eu

IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
CONTAINER="stage2-migration-spike-$$"
DATABASE="stage2_spike"
PASSWORD="stage2_spike_password"

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run --name "$CONTAINER" \
    -e POSTGRES_USER=spike_admin \
    -e POSTGRES_PASSWORD="$PASSWORD" \
    -e POSTGRES_DB="$DATABASE" \
    -e AUTH_DB_PASSWORD=stage2_auth_password \
    -e STORAGE_DB_PASSWORD=stage2_storage_password \
    -v "$PROJECT_ROOT/migrations:/docker-entrypoint-initdb.d:ro" \
    -d "$IMAGE" >/dev/null

ready=false
for _ in $(seq 1 90); do
    if docker exec "$CONTAINER" psql -U spike_admin -d "$DATABASE" -Atc \
        "SELECT count(*) FROM schema_migrations WHERE version = 9" 2>/dev/null |
        grep -qx 1; then
        ready=true
        break
    fi
    if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" | grep -qx true; then
        docker logs "$CONTAINER"
        exit 1
    fi
    sleep 1
done
[ "$ready" = true ] || { docker logs "$CONTAINER"; exit 1; }

run_probe_migration() {
    docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -U spike_admin -d "$DATABASE" <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(3290, 2);
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 10) THEN
        CREATE TABLE stage2_migration_probe (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        INSERT INTO stage2_migration_probe DEFAULT VALUES;
        INSERT INTO schema_migrations (version, name)
        VALUES (10, 'stage2_upgrade_probe');
    END IF;
END
$$;
COMMIT;
SQL
}

run_probe_migration >/dev/null &
first_pid=$!
run_probe_migration >/dev/null &
second_pid=$!
wait "$first_pid"
wait "$second_pid"
run_probe_migration >/dev/null

docker exec "$CONTAINER" psql -U spike_admin -d "$DATABASE" -Atc \
    "SELECT count(*) FROM schema_migrations WHERE version = 10 AND name = 'stage2_upgrade_probe'" |
    grep -qx 1
docker exec "$CONTAINER" psql -U spike_admin -d "$DATABASE" -Atc \
    "SELECT count(*) FROM stage2_migration_probe" |
    grep -qx 1

echo "Migration upgrade spike passed: version 9 upgraded once under concurrent and repeated runs."
