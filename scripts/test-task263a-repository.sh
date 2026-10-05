#!/bin/sh
# Isolated admission/finalization fixture. Never sources deployment .env or touches its DB.
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
umask 077
TMP="$(mktemp -d /tmp/opencode/task263a.XXXXXX)"
CONTAINER="task263a-$(cat /proc/sys/kernel/random/uuid)"
cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$TMP/init"
chmod 755 "$TMP" "$TMP/init"
for f in "$ROOT"/migrations/00000* "$ROOT"/migrations/000010*; do
    cp "$f" "$TMP/init/"
done
chmod 644 "$TMP/init/"*.sql
chmod 755 "$TMP/init/"*.sh
ADMIN_PASSWORD="$(openssl rand -hex 32)"
BACKEND_PASSWORD="$(openssl rand -hex 32)"
cat > "$TMP/database.env" <<EOF
POSTGRES_USER=fixture_admin
POSTGRES_DB=fixture_admission
POSTGRES_PASSWORD=$ADMIN_PASSWORD
BACKEND_DB_PASSWORD=$BACKEND_PASSWORD
AUTH_DB_PASSWORD=$(openssl rand -hex 32)
STORAGE_DB_PASSWORD=$(openssl rand -hex 32)
EOF
docker run -d --name "$CONTAINER" --env-file "$TMP/database.env" \
    -p 127.0.0.1::5432 -v "$TMP/init:/docker-entrypoint-initdb.d:ro" \
    "$IMAGE" >/dev/null
ready=false
for _ in $(seq 1 90); do
    if docker exec "$CONTAINER" psql -U fixture_admin -d fixture_admission -Atc \
        'SELECT count(*) FROM schema_migrations WHERE version=10' 2>/dev/null | grep -qx 1; then
        ready=true; break
    fi
    sleep 1
done
[ "$ready" = true ] || { echo 'Isolated v10 initialization failed.' >&2; exit 1; }
LEGACY_ACTOR="$(cat /proc/sys/kernel/random/uuid)"
PENDING="fixture_$(cat /proc/sys/kernel/random/uuid)"
TERMINAL="fixture_$(cat /proc/sys/kernel/random/uuid)"
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 >/dev/null <<EOF
INSERT INTO profiles(id) VALUES('$LEGACY_ACTOR');
INSERT INTO gateways(gateway_id,name) VALUES('$PENDING','Legacy pending'),('$TERMINAL','Legacy terminal');
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,completed_at) VALUES
('$(cat /proc/sys/kernel/random/uuid)','$PENDING',NULL,'recover','pending',NULL,NULL),
('$(cat /proc/sys/kernel/random/uuid)','$PENDING',7,'provision','pending','$LEGACY_ACTOR',NULL),
('$(cat /proc/sys/kernel/random/uuid)','$TERMINAL',3,'revoke','succeeded','$LEGACY_ACTOR',now());
INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,revoked_at)
SELECT '$TERMINAL',3,'revoked',operation_id,now() FROM gateway_mqtt_credential_events WHERE gateway_id='$TERMINAL';
EOF
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/migrations/000011_mqtt_credential_operations.up.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000011.sql" >/dev/null
PORT="$(docker port "$CONTAINER" 5432/tcp | sed 's/.*://')"
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/seed-migration-000012-upgrade.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/migrations/000012_mqtt_credential_maintenance.up.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000012-upgrade.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000012.sql" >/dev/null
for sql in scripts/sql/seed-migration-000013-upgrade.sql migrations/000013_mqtt_maintenance_admission.up.sql scripts/sql/verify-migration-000013-upgrade.sql scripts/sql/verify-migration-000013.sql; do
    docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 < "$ROOT/$sql" >/dev/null
done
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/migrations/000014_mqtt_credential_recovery.up.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000014.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/migrations/000015_mqtt_recovery_authority_guards.up.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000015.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/migrations/000016_mqtt_event_authority_guard.up.sql" >/dev/null
docker exec -i "$CONTAINER" psql -U fixture_admin -d fixture_admission -v ON_ERROR_STOP=1 \
    < "$ROOT/scripts/sql/verify-migration-000016.sql" >/dev/null
export TASK263A_APP_DSN="postgres://iot_backend_app:$BACKEND_PASSWORD@127.0.0.1:$PORT/fixture_admission?sslmode=disable"
export TASK263A_ADMIN_DSN="postgres://fixture_admin:$ADMIN_PASSWORD@127.0.0.1:$PORT/fixture_admission?sslmode=disable"
export TASK263A_LEGACY_PENDING="$PENDING" TASK263A_LEGACY_TERMINAL="$TERMINAL"
cd "$ROOT/src"
go test -race -count=1 -coverprofile="$TMP/coverage.out" -v ./internal/mqttcredential -run 'TestPostgres' > "$TMP/tests.log" 2>&1 || {
    cat "$TMP/tests.log"; exit 1;
}
cat "$TMP/tests.log"
if grep -q -- '--- SKIP:' "$TMP/tests.log"; then
    echo 'Integration SKIP is not a fixture PASS.' >&2; exit 1
fi
go tool cover -func="$TMP/coverage.out"
if [ -n "${TASK263_COVERAGE_OUT:-}" ]; then
    cp "$TMP/coverage.out" "$TASK263_COVERAGE_OUT"
fi
go vet ./internal/mqttcredential
