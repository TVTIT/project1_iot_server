#!/bin/sh
set -eu

IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUN_ID="stage2-migrations-$$"
UPGRADE_DB="$RUN_ID-upgrade"
FRESH_DB="$RUN_ID-fresh"
FAILED_DB="$RUN_ID-failed"
BASELINE_DIR="$(mktemp -d)"
INVALID_DIR=""
first_pid=""
second_pid=""
first_bootstrap_pid=""
second_bootstrap_pid=""
ADMIN_PASSWORD="stage2_admin_password"
BACKEND_PASSWORD="stage2_backend_password"

cleanup() {
    for pid in "$first_pid" "$second_pid" "$first_bootstrap_pid" "$second_bootstrap_pid"; do
        if [ -n "$pid" ]; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    docker rm -f "$UPGRADE_DB" "$FRESH_DB" "$FAILED_DB" >/dev/null 2>&1 || true
    rm -rf "$BASELINE_DIR"
    if [ -n "$INVALID_DIR" ]; then
        rm -rf "$INVALID_DIR"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

chmod 755 "$BASELINE_DIR"
for migration in "$PROJECT_ROOT"/migrations/00000[0-9]*; do
    cp "$migration" "$BASELINE_DIR/"
done

start_database() {
    container="$1"
    migration_dir="$2"
    docker run --name "$container" \
        -e POSTGRES_USER=stage2_admin \
        -e POSTGRES_PASSWORD="$ADMIN_PASSWORD" \
        -e POSTGRES_DB=stage2_test \
        -e AUTH_DB_PASSWORD=stage2_auth_password_1234 \
        -e STORAGE_DB_PASSWORD=stage2_storage_password_1234 \
        -e BACKEND_DB_PASSWORD="$BACKEND_PASSWORD" \
        -v "$migration_dir:/docker-entrypoint-initdb.d:ro" \
        -d "$IMAGE" >/dev/null
}

wait_for_version() {
    container="$1"
    version="$2"
    ready=false
    for _ in $(seq 1 90); do
        if docker exec "$container" psql -U stage2_admin -d stage2_test -Atc \
            "SELECT count(*) FROM schema_migrations WHERE version = $version" \
            2>/dev/null | grep -qx 1; then
            ready=true
            break
        fi
        if ! docker inspect -f '{{.State.Running}}' "$container" | grep -qx true; then
            docker logs "$container"
            exit 1
        fi
        sleep 1
    done
    [ "$ready" = true ] || { docker logs "$container"; exit 1; }
}

run_migrations() {
    container="$1"
    docker run --rm --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD="$ADMIN_PASSWORD" \
        -e PGDATABASE=stage2_test \
        -v "$PROJECT_ROOT/migrations:/migrations:ro" \
        -v "$PROJECT_ROOT/scripts/run-migrations.sh:/run-migrations.sh:ro" \
        --entrypoint /bin/sh "$IMAGE" /run-migrations.sh
}

verify_stage2_schema() {
    container="$1"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 \
        -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000010.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 \
        -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000011.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 \
        -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000012.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000013.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000014.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000015.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000016.sql"
    docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "SELECT idempotency_key, previous_status, previous_credential_version, previous_activated_at, previous_revoked_at, phase, ram_applied, snapshot_observed, fresh_positive_verified, delivery_status, legacy_projection FROM gateway_mqtt_credential_events LIMIT 1; SELECT status, credential_version FROM gateway_mqtt_credentials LIMIT 1" >/dev/null

    if docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "CREATE TABLE backend_must_not_create_tables (id integer)" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly has DDL permission." >&2
        exit 1
    fi
    if docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO schema_migrations (version, name) VALUES (999, 'forbidden')" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly modified migration history." >&2
        exit 1
    fi
    if docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "DELETE FROM gateway_mqtt_credential_events" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly deleted credential audit events." >&2
        exit 1
    fi
    docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO gateways (gateway_id, name) VALUES ('gateway_role_probe', 'Role Probe')" \
        >/dev/null
    docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -Atc \
        "SELECT count(*) FROM gateways WHERE gateway_id = 'gateway_role_probe'" | grep -qx 1

    if docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO gateways (gateway_id, name) VALUES ('bad/gateway', 'invalid')" \
        >/dev/null 2>&1; then
        echo "Invalid gateway identifier unexpectedly passed its constraint." >&2
        exit 1
    fi
    if docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO gateways (gateway_id, name) VALUES ('backend_service', 'reserved')" \
        >/dev/null 2>&1; then
        echo "Reserved gateway identifier unexpectedly passed its constraint." >&2
        exit 1
    fi

    bootstrap_user_id="10000000-0000-4000-8000-000000000001"
    competing_user_id="10000000-0000-4000-8000-000000000002"
    docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO profiles (id, full_name) VALUES ('$bootstrap_user_id', 'Stage 2 Admin'), ('$competing_user_id', 'Competing Admin') ON CONFLICT DO NOTHING" \
        >/dev/null
    docker run --rm --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD="$ADMIN_PASSWORD" \
        -e PGDATABASE=stage2_test \
        -e PLATFORM_ADMIN_USER_ID="$bootstrap_user_id" \
        -v "$PROJECT_ROOT/scripts/bootstrap-platform-admin.sh:/bootstrap-platform-admin.sh:ro" \
        --entrypoint /bin/sh "$IMAGE" /bootstrap-platform-admin.sh \
        >/dev/null 2>&1 &
    first_bootstrap_pid=$!
    docker run --rm --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD="$ADMIN_PASSWORD" \
        -e PGDATABASE=stage2_test \
        -e PLATFORM_ADMIN_USER_ID="$competing_user_id" \
        -v "$PROJECT_ROOT/scripts/bootstrap-platform-admin.sh:/bootstrap-platform-admin.sh:ro" \
        --entrypoint /bin/sh "$IMAGE" /bootstrap-platform-admin.sh \
        >/dev/null 2>&1 &
    second_bootstrap_pid=$!
    first_status=0
    second_status=0
    wait "$first_bootstrap_pid" || first_status=$?
    wait "$second_bootstrap_pid" || second_status=$?
    first_bootstrap_pid=""
    second_bootstrap_pid=""
    if [ "$first_status" -eq 0 ] && [ "$second_status" -eq 0 ]; then
        echo "Concurrent bootstrap unexpectedly created two platform admins." >&2
        exit 1
    fi
    docker exec "$container" psql -U stage2_admin -d stage2_test -Atc \
        "SELECT count(*) FROM platform_admins" | grep -qx 1

    selected_admin_id="$(docker exec "$container" psql -U stage2_admin -d stage2_test -Atc \
        "SELECT user_id FROM platform_admins")"
    for _ in 1 2; do
        docker run --rm --network "container:$container" \
            -e PGHOST=127.0.0.1 \
            -e PGUSER=stage2_admin \
            -e PGPASSWORD="$ADMIN_PASSWORD" \
            -e PGDATABASE=stage2_test \
            -e PLATFORM_ADMIN_USER_ID="$selected_admin_id" \
            -v "$PROJECT_ROOT/scripts/bootstrap-platform-admin.sh:/bootstrap-platform-admin.sh:ro" \
            --entrypoint /bin/sh "$IMAGE" /bootstrap-platform-admin.sh \
            >/dev/null
    done
    docker exec "$container" psql -U stage2_admin -d stage2_test -Atc \
        "SELECT count(*) FROM platform_admins WHERE user_id = '$selected_admin_id'" |
        grep -qx 1

    event_id="20000000-0000-4000-8000-000000000001"
    docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$event_id', 'gateway_role_probe', '$selected_admin_id', '$event_id', 1, 'provision', 'pending')" \
        >/dev/null
    docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "UPDATE gateway_mqtt_credential_events SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = true, fresh_positive_verified = true, completed_at = now(), updated_at = now() WHERE operation_id = '$event_id'" \
        >/dev/null
    if docker exec "$container" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "UPDATE gateway_mqtt_credential_events SET status = 'failed', completed_at = now() WHERE operation_id = '$event_id'" \
        >/dev/null 2>&1; then
        echo "Completed credential audit event unexpectedly remained mutable." >&2
        exit 1
    fi
}

# Upgrade an existing version-9 database, including concurrent and repeated runs.
start_database "$UPGRADE_DB" "$BASELINE_DIR"
wait_for_version "$UPGRADE_DB" 9
# Exercise v9 -> v10, then seed the actual legacy state before v10 -> latest.
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000010_stage2_auth_and_provisioning.up.sql" >/dev/null
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/seed-migration-000011-upgrade.sql" >/dev/null
for sql in migrations/000011_mqtt_credential_operations.up.sql scripts/sql/seed-migration-000012-upgrade.sql migrations/000012_mqtt_credential_maintenance.up.sql scripts/sql/verify-migration-000012-upgrade.sql scripts/sql/seed-migration-000013-upgrade.sql migrations/000013_mqtt_maintenance_admission.up.sql scripts/sql/seed-migration-000014-upgrade.sql; do
    docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test < "$PROJECT_ROOT/$sql" >/dev/null
done
for sql in migrations/000014_mqtt_credential_recovery.up.sql scripts/sql/seed-migration-000015-upgrade.sql migrations/000015_mqtt_recovery_authority_guards.up.sql; do
    docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test < "$PROJECT_ROOT/$sql" >/dev/null
done
run_migrations "$UPGRADE_DB" >/dev/null &
first_pid=$!
run_migrations "$UPGRADE_DB" >/dev/null &
second_pid=$!
wait "$first_pid"
wait "$second_pid"
first_pid=""
second_pid=""
run_migrations "$UPGRADE_DB" >/dev/null
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000011-upgrade.sql"
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000013-upgrade.sql"
verify_stage2_schema "$UPGRADE_DB"
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000014-upgrade.sql"
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000015-upgrade.sql"

# A clean database initialized from the complete migration directory must match.
start_database "$FRESH_DB" "$PROJECT_ROOT/migrations"
wait_for_version "$FRESH_DB" 16
verify_stage2_schema "$FRESH_DB"

# A raw repeated migration must also be safe, not merely skipped by the runner.
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000011_mqtt_credential_operations.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000012_mqtt_credential_maintenance.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000013_mqtt_maintenance_admission.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000014_mqtt_credential_recovery.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000015_mqtt_recovery_authority_guards.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000016_mqtt_event_authority_guard.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000013.sql" >/dev/null

# Concurrent app writers: same actor/key on different Gateways, and different
# keys on the same Gateway. Exactly one intent commits in each race.
docker exec "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test -c \
    "INSERT INTO profiles (id, full_name) VALUES ('13000000-0000-4000-8000-000000000001', 'Concurrent actor'); INSERT INTO gateways (gateway_id, name) VALUES ('race11_a', 'Race A'), ('race11_b', 'Race B'), ('race11_c', 'Race C');" >/dev/null
for scenario in key gateway; do
    if [ "$scenario" = key ]; then
        gateway_one=race11_a; gateway_two=race11_b
        key_one=13000000-0000-4000-8000-000000000002; key_two="$key_one"
        op_one=13000000-0000-4000-8000-000000000003; op_two=13000000-0000-4000-8000-000000000004
    else
        gateway_one=race11_c; gateway_two=race11_c
        key_one=13000000-0000-4000-8000-000000000005; key_two=13000000-0000-4000-8000-000000000006
        op_one=13000000-0000-4000-8000-000000000007; op_two=13000000-0000-4000-8000-000000000008
    fi
    docker exec "$FRESH_DB" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 -c \
        "BEGIN; INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$op_one', '$gateway_one', '13000000-0000-4000-8000-000000000001', '$key_one', 1, 'provision', 'pending'); SELECT pg_sleep(1); COMMIT;" >/dev/null 2>&1 &
    first_pid=$!
    docker exec "$FRESH_DB" env PGPASSWORD="$BACKEND_PASSWORD" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 -c \
        "BEGIN; INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$op_two', '$gateway_two', '13000000-0000-4000-8000-000000000001', '$key_two', 1, 'provision', 'pending'); SELECT pg_sleep(1); COMMIT;" >/dev/null 2>&1 &
    second_pid=$!
    first_status=0; second_status=0
    wait "$first_pid" || first_status=$?
    wait "$second_pid" || second_status=$?
    first_pid=""; second_pid=""
    if { [ "$first_status" -eq 0 ] && [ "$second_status" -eq 0 ]; } || \
       { [ "$first_status" -ne 0 ] && [ "$second_status" -ne 0 ]; }; then
        echo "Concurrent $scenario admission did not commit exactly one intent." >&2
        exit 1
    fi
    docker exec "$FRESH_DB" psql -U stage2_admin -d stage2_test -Atc \
        "SELECT count(*) FROM gateway_mqtt_credential_events WHERE operation_id IN ('$op_one', '$op_two')" | grep -qx 1
done

INVALID_DIR="$(mktemp -d)"
chmod 755 "$INVALID_DIR"
cp "$PROJECT_ROOT/migrations/000010_stage2_auth_and_provisioning.up.sql" \
    "$INVALID_DIR/000010_invalid-name.up.sql"
if docker run --rm --network "container:$UPGRADE_DB" \
    -e PGHOST=127.0.0.1 \
    -e PGUSER=stage2_admin \
    -e PGPASSWORD="$ADMIN_PASSWORD" \
    -e PGDATABASE=stage2_test \
    -v "$INVALID_DIR:/migrations:ro" \
    -v "$PROJECT_ROOT/scripts/run-migrations.sh:/run-migrations.sh:ro" \
    --entrypoint /bin/sh "$IMAGE" /run-migrations.sh \
    >/dev/null 2>&1; then
    echo "Migration runner unexpectedly accepted an invalid filename." >&2
    exit 1
fi
rm -rf "$INVALID_DIR"
INVALID_DIR=""

# Failed initialization must stop startup, not continue with an incomplete v11.
INVALID_DIR="$(mktemp -d)"
chmod 755 "$INVALID_DIR"
cp "$PROJECT_ROOT"/migrations/00000* "$INVALID_DIR/"
printf '%s\n' '\set ON_ERROR_STOP on' "DO 'BEGIN RAISE EXCEPTION ''intentional isolated migration failure''; END';" > "$INVALID_DIR/000010_failure.up.sql"
start_database "$FAILED_DB" "$INVALID_DIR"
stopped=false
for _ in $(seq 1 90); do
    if docker inspect -f '{{.State.Running}}' "$FAILED_DB" | grep -qx false; then
        stopped=true
        break
    fi
    sleep 1
done
[ "$stopped" = true ] || { echo 'Failed migration did not stop startup.' >&2; exit 1; }
docker logs "$FAILED_DB" 2>&1 | grep -q 'intentional isolated migration failure'
rm -rf "$INVALID_DIR"
INVALID_DIR=""

echo "Stage 2 migration tests passed for version-9/10/13/14/15 upgrade and clean version-16 install."
