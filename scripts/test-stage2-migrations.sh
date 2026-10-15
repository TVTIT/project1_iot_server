#!/bin/sh
set -eu

IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUN_ID="stage2-migrations-$(cat /proc/sys/kernel/random/uuid)"
UPGRADE_DB="$RUN_ID-upgrade"
FRESH_DB="$RUN_ID-fresh"
FAILED_DB="$RUN_ID-failed"
BASELINE_DIR="$(mktemp -d /tmp/opencode/migrations-baseline.XXXXXX)"
HISTORICAL_DIR="$(mktemp -d /tmp/opencode/migrations-v19.XXXXXX)"
RECEIPT_DIR="$(mktemp -d /tmp/opencode/migrations-receipts.XXXXXX)"
INVALID_DIR=""
first_pid=""
second_pid=""
first_bootstrap_pid=""
second_bootstrap_pid=""
ADMIN_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
BACKEND_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
AUTH_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
STORAGE_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
POSTGRES_PASSWORD="$ADMIN_PASSWORD"
BACKEND_DB_PASSWORD="$BACKEND_PASSWORD"
PGPASSWORD="$ADMIN_PASSWORD"
export POSTGRES_PASSWORD BACKEND_DB_PASSWORD AUTH_DB_PASSWORD STORAGE_DB_PASSWORD PGPASSWORD

# Bound every Docker call as well as the outer CI invocation. timeout resolves
# the external executable, not this shell function.
docker() {
    timeout 60 docker "$@"
}

# Backend secret already lives inside our fixture environment, never in argv.
backend_exec() {
    target="$1"
    shift
    docker exec "$target" sh -c 'export PGPASSWORD="$BACKEND_DB_PASSWORD"; exec "$@"' sh "$@"
}

cleanup() {
    exit_code=$?
    cleanup_status=0
    for pid in "$first_pid" "$second_pid" "$first_bootstrap_pid" "$second_bootstrap_pid"; do
        if [ -n "$pid" ]; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    for receipt in "$RECEIPT_DIR"/*; do
        [ -s "$receipt" ] || continue
        id="$(cat "$receipt")"
        owner="$(timeout 15 docker inspect -f '{{index .Config.Labels "stage2.migration-run"}}' "$id")" || { cleanup_status=1; continue; }
        if [ "$owner" != "$RUN_ID" ]; then
            echo 'Cleanup refused a resource without matching ownership.' >&2
            cleanup_status=1
            continue
        fi
        timeout 30 docker rm -fv "$id" >/dev/null 2>&1 || cleanup_status=1
        if ! remaining="$(timeout 15 docker container ls -aq --no-trunc --filter "id=$id")"; then
            cleanup_status=1
        elif [ -n "$remaining" ]; then
            cleanup_status=1
        else
            echo "Owned container cleanup verified: $id"
        fi
    done
    rm -rf "$RECEIPT_DIR" || cleanup_status=1
    rm -rf "$BASELINE_DIR" || cleanup_status=1
    rm -rf "$HISTORICAL_DIR" || cleanup_status=1
    if [ -n "$INVALID_DIR" ]; then
        rm -rf "$INVALID_DIR" || cleanup_status=1
    fi
    if [ "$cleanup_status" -ne 0 ]; then
        echo "Cleanup encountered errors." >&2
        exit 1
    fi
    exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker image inspect -f '{{.Id}}' "$IMAGE" >/dev/null 2>&1 || {
    echo 'Pinned local migration image unavailable.' >&2
    exit 1
}

# Record the exact fresh container ID before starting it, including transient
# migration/bootstrap clients. No name-based deletion or deployment resources.
owned_run() {
    mode="$1"
    shift
    receipt="$(mktemp "$RECEIPT_DIR/container.XXXXXX")"
    docker create --pull=never --label "stage2.migration-run=$RUN_ID" \
        "$@" > "$receipt"
    id="$(cat "$receipt")"
    echo "Owned container created: $id" >&2
    if [ "$mode" = detached ]; then
        docker start "$id" >/dev/null
    else
        docker start -a "$id"
        status="$(docker inspect -f '{{.State.ExitCode}}' "$id")"
        return "$status"
    fi
}

chmod 755 "$BASELINE_DIR"
for migration in "$PROJECT_ROOT"/migrations/00000[0-9]*; do
    cp "$migration" "$BASELINE_DIR/"
done

# Keep the historical Stage 2 checkpoints pinned to schema 019. Task 3 adds
# migration 020 after those assertions have completed.
chmod 755 "$HISTORICAL_DIR"
for migration in "$PROJECT_ROOT"/migrations/0000*; do
    [ -e "$migration" ] || continue
    filename="${migration##*/}"
    version_text="${filename%%_*}"
    version="$(printf '%s' "$version_text" | sed 's/^0*//')"
    [ -n "$version" ] || version=0
    if [ "$version" -le 19 ]; then
        cp "$migration" "$HISTORICAL_DIR/"
    fi
done

start_database() {
    container="$1"
    migration_dir="$2"
    owned_run detached --name "$container" \
        -e POSTGRES_USER=stage2_admin \
        -e POSTGRES_PASSWORD \
        -e POSTGRES_DB=stage2_test \
        -e AUTH_DB_PASSWORD \
        -e STORAGE_DB_PASSWORD \
        -e BACKEND_DB_PASSWORD \
        -v "$migration_dir:/docker-entrypoint-initdb.d:ro" \
        "$IMAGE" >/dev/null
}

wait_for_version() {
    container="$1"
    version="$2"
    expected_name="${3:-}"
    ready=false
    query="SELECT count(*) FROM schema_migrations WHERE version = $version"
    if [ -n "$expected_name" ]; then
        query="SELECT count(*) FROM schema_migrations WHERE version = $version AND name = '$expected_name'"
    fi
    for _ in $(seq 1 90); do
        if docker exec "$container" sh -c \
            'export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -h 127.0.0.1 -U stage2_admin -d stage2_test -Atc "$1"' \
            sh "$query" \
            2>/dev/null | grep -qx 1; then
            ready=true
            break
        fi
        if ! docker inspect -f '{{.State.Running}}' "$container" | grep -qx true; then
            echo 'Database readiness failed.' >&2
            exit 1
        fi
        sleep 1
    done
    [ "$ready" = true ] || { echo 'Database readiness failed.' >&2; exit 1; }
}

run_migrations() {
    container="$1"
    migration_dir="${2:-$HISTORICAL_DIR}"
    owned_run attached --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD \
        -e PGDATABASE=stage2_test \
        -v "$migration_dir:/migrations:ro" \
        -v "$PROJECT_ROOT/scripts/run-migrations.sh:/run-migrations.sh:ro" \
        --entrypoint /bin/sh "$IMAGE" /run-migrations.sh
}

verify_admission_regressions() {
    container="$1"
    schema="$2"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000011.sql" >/dev/null
    if [ "$schema" = historical ]; then
        mutations="DROP INDEX public.uq_mqtt_operation_unresolved|DROP INDEX public.uq_mqtt_operation_actor_key"
    else
        mutations="ALTER TABLE public.gateway_mqtt_credential_events DISABLE TRIGGER enforce_gateway_mqtt_event_transition|ALTER TABLE public.gateway_mqtt_credential_events DISABLE TRIGGER check_mqtt_replacement_event|DROP FUNCTION public.mqtt_operation_effective(uuid) CASCADE|DROP INDEX public.uq_mqtt_operation_actor_key|UPDATE schema_migrations SET name='incorrect_replacement' WHERE version=17|DELETE FROM schema_migrations WHERE version=17"
    fi
    old_ifs="$IFS"
    IFS='|'
    for mutation in $mutations; do
        # The verifier starts its own transaction; prepend the fault inside that
        # transaction so connection exit rolls back even expected failures.
        sed "/^BEGIN;$/a\\$mutation;" "$PROJECT_ROOT/scripts/sql/verify-migration-000011.sql" > "$BASELINE_DIR/admission-probe.sql"
        if docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
            < "$BASELINE_DIR/admission-probe.sql" > "$BASELINE_DIR/admission-probe.log" 2>&1; then
            echo "Admission verifier accepted missing protection: $mutation" >&2
            exit 1
        fi
        grep -Eq 'Missing (unresolved operation index|unique actor-key index|replacement admission trigger|replacement admission functions)|Unknown replacement migration history' "$BASELINE_DIR/admission-probe.log"
    done
    IFS="$old_ifs"
    echo "Admission regressions passed: $schema"
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
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000018.sql"
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000019.sql"
    backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "SELECT idempotency_key, previous_status, previous_credential_version, previous_activated_at, previous_revoked_at, phase, ram_applied, snapshot_observed, fresh_positive_verified, delivery_status, legacy_projection FROM gateway_mqtt_credential_events LIMIT 1; SELECT status, credential_version FROM gateway_mqtt_credentials LIMIT 1" >/dev/null

    if backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "CREATE TABLE backend_must_not_create_tables (id integer)" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly has DDL permission." >&2
        exit 1
    fi
    if backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO schema_migrations (version, name) VALUES (999, 'forbidden')" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly modified migration history." >&2
        exit 1
    fi
    if backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "DELETE FROM gateway_mqtt_credential_events" \
        >/dev/null 2>&1; then
        echo "Backend database role unexpectedly deleted credential audit events." >&2
        exit 1
    fi
    docker exec "$container" psql -U stage2_admin -d stage2_test -v ON_ERROR_STOP=1 \
        -c "INSERT INTO gateways (gateway_id, name) VALUES ('gateway_role_probe', 'Role Probe')" \
        >/dev/null
    backend_exec "$container" \
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
    owned_run attached --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD \
        -e PGDATABASE=stage2_test \
        -e PLATFORM_ADMIN_USER_ID="$bootstrap_user_id" \
        -v "$PROJECT_ROOT/scripts/bootstrap-platform-admin.sh:/bootstrap-platform-admin.sh:ro" \
        --entrypoint /bin/sh "$IMAGE" /bootstrap-platform-admin.sh \
        >/dev/null 2>&1 &
    first_bootstrap_pid=$!
    owned_run attached --network "container:$container" \
        -e PGHOST=127.0.0.1 \
        -e PGUSER=stage2_admin \
        -e PGPASSWORD \
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
        owned_run attached --network "container:$container" \
            -e PGHOST=127.0.0.1 \
            -e PGUSER=stage2_admin \
            -e PGPASSWORD \
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
    # Deferred inventory guards require a selected projection at the same commit.
    # Keep the default lifecycle contract: its checkpoint is created by the trigger.
    backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "BEGIN;
INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$event_id', 'gateway_role_probe', '$selected_admin_id', '$event_id', 1, 'provision', 'pending');
INSERT INTO gateway_mqtt_credentials (gateway_id, credential_version, status, last_operation_id, changed_by, updated_at)
SELECT gateway_id, credential_version, 'provisioning', operation_id, actor_user_id, created_at FROM gateway_mqtt_credential_events WHERE operation_id = '$event_id';
COMMIT;" \
        >/dev/null
    backend_exec "$container" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 \
        -c "BEGIN;
UPDATE gateway_mqtt_credential_events SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = true, fresh_positive_verified = true, completed_at = now(), updated_at = now() WHERE operation_id = '$event_id';
UPDATE gateway_mqtt_credentials SET status = 'active', activated_at = now(), updated_at = now() WHERE last_operation_id = '$event_id';
UPDATE mqtt_credential_maintenance SET status = 'completed', broker_epoch = repeat('a', 64), completed_at = now(), updated_at = now() WHERE operation_id = '$event_id';
COMMIT;" \
        >/dev/null
    if backend_exec "$container" \
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
for sql in migrations/000011_mqtt_credential_operations.up.sql scripts/sql/seed-migration-000012-upgrade.sql migrations/000012_mqtt_credential_maintenance.up.sql scripts/sql/verify-migration-000012-upgrade.sql scripts/sql/seed-migration-000013-upgrade.sql migrations/000013_mqtt_maintenance_admission.up.sql scripts/sql/verify-migration-000013-upgrade.sql scripts/sql/seed-migration-000014-upgrade.sql; do
    docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test < "$PROJECT_ROOT/$sql" >/dev/null
    if [ "$sql" = migrations/000011_mqtt_credential_operations.up.sql ]; then
        verify_admission_regressions "$UPGRADE_DB" historical
    fi
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
verify_admission_regressions "$UPGRADE_DB" replacement
verify_stage2_schema "$UPGRADE_DB"
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000014-upgrade.sql"
docker exec -i "$UPGRADE_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000015-upgrade.sql"

# A clean database initialized from the complete migration directory must match.
start_database "$FRESH_DB" "$HISTORICAL_DIR"
wait_for_version "$FRESH_DB" 19 mqtt_simplified_journal
verify_admission_regressions "$FRESH_DB" replacement
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
    < "$PROJECT_ROOT/migrations/000017_mqtt_rotation_replacement.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000018_mqtt_guard_search_path.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/migrations/000019_mqtt_simplified_journal.up.sql" >/dev/null
docker exec -i "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test \
    < "$PROJECT_ROOT/scripts/sql/verify-migration-000013.sql" >/dev/null

# Concurrent app writers: same actor/key on different Gateways, and different
# keys on the same Gateway. Exactly one intent commits in each race.
docker exec "$FRESH_DB" psql -v ON_ERROR_STOP=1 -U stage2_admin -d stage2_test -c \
    "INSERT INTO profiles (id, full_name) VALUES ('13000000-0000-4000-8000-000000000001', 'Concurrent actor'); INSERT INTO gateways (gateway_id, name) VALUES ('race11_a', 'Race A'), ('race11_b', 'Race B'), ('race11_c', 'Race C');" >/dev/null
# Prove why the historical intent-only writer fixture is no longer admissible.
# Connection failure rolls the entire transaction back, including its checkpoint.
if backend_exec "$FRESH_DB" \
    psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 -c \
    "BEGIN; INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('13000000-0000-4000-8000-000000000009', 'race11_a', '13000000-0000-4000-8000-000000000001', '13000000-0000-4000-8000-000000000010', 1, 'provision', 'pending'); COMMIT;" \
    > "$BASELINE_DIR/intent-only-probe.log" 2>&1; then
    echo "Intent-only writer unexpectedly committed" >&2
    exit 1
fi
grep '^ERROR:  New intent cannot commit as unassociated audit' "$BASELINE_DIR/intent-only-probe.log"
docker exec "$FRESH_DB" psql -U stage2_admin -d stage2_test -Atc \
    "SELECT (SELECT count(*) FROM gateway_mqtt_credential_events WHERE operation_id='13000000-0000-4000-8000-000000000009') + (SELECT count(*) FROM mqtt_credential_maintenance WHERE operation_id='13000000-0000-4000-8000-000000000009')" | grep -qx 0
echo "Intent-only writer rejection verified"
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
    backend_exec "$FRESH_DB" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 -c \
        "BEGIN; INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$op_one', '$gateway_one', '13000000-0000-4000-8000-000000000001', '$key_one', 1, 'provision', 'pending'); INSERT INTO gateway_mqtt_credentials (gateway_id, credential_version, status, last_operation_id, changed_by, updated_at) SELECT gateway_id, credential_version, 'provisioning', operation_id, actor_user_id, created_at FROM gateway_mqtt_credential_events WHERE operation_id = '$op_one'; SELECT pg_sleep(1); COMMIT;" >/dev/null 2>&1 &
    first_pid=$!
    backend_exec "$FRESH_DB" \
        psql -h 127.0.0.1 -U iot_backend_app -d stage2_test -v ON_ERROR_STOP=1 -c \
        "BEGIN; INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('$op_two', '$gateway_two', '13000000-0000-4000-8000-000000000001', '$key_two', 1, 'provision', 'pending'); INSERT INTO gateway_mqtt_credentials (gateway_id, credential_version, status, last_operation_id, changed_by, updated_at) SELECT gateway_id, credential_version, 'provisioning', operation_id, actor_user_id, created_at FROM gateway_mqtt_credential_events WHERE operation_id = '$op_two'; SELECT pg_sleep(1); COMMIT;" >/dev/null 2>&1 &
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
    echo "Concurrent $scenario writer admission verified: exactly one commit"
done

INVALID_DIR="$(mktemp -d /tmp/opencode/migrations-invalid.XXXXXX)"
chmod 755 "$INVALID_DIR"
cp "$PROJECT_ROOT/migrations/000010_stage2_auth_and_provisioning.up.sql" \
    "$INVALID_DIR/000010_invalid-name.up.sql"
if owned_run attached --network "container:$UPGRADE_DB" \
    -e PGHOST=127.0.0.1 \
    -e PGUSER=stage2_admin \
    -e PGPASSWORD \
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
INVALID_DIR="$(mktemp -d /tmp/opencode/migrations-invalid.XXXXXX)"
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
# Consume logs only inside a bounded filter; never forward initialization logs.
timeout 15 docker container logs "$FAILED_DB" 2>&1 | grep -q 'intentional isolated migration failure'
rm -rf "$INVALID_DIR"
INVALID_DIR=""

# Only after every historical v19 assertion has passed, upgrade both fixtures
# through the existing migration runner and verify v20 schema-only invariants.
run_migrations "$UPGRADE_DB" "$PROJECT_ROOT/migrations" >/dev/null
run_migrations "$FRESH_DB" "$PROJECT_ROOT/migrations" >/dev/null
for container in "$UPGRADE_DB" "$FRESH_DB"; do
    docker exec -i "$container" psql -v ON_ERROR_STOP=1 -v stage3_verify_writes=false \
        -U stage2_admin -d stage2_test \
        < "$PROJECT_ROOT/scripts/sql/verify-migration-000020.sql"
done

echo "Stage 2 historical migration tests passed through version 19; version 20 fresh/upgrade schema verification passed."
