#!/bin/sh
# Isolated PostgreSQL integration tests: never uses the repository's .env.
set -eu
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
RUN_ID="stage2-admin-$(cat /proc/sys/kernel/random/uuid)"
CONTAINER="$RUN_ID"
VOLUME="$RUN_ID-data"
RECEIPT_DIR="$(mktemp -d /var/tmp/opencode/stage2-admin-receipts.XXXXXX)"
chmod 700 "$RECEIPT_DIR"
CID_FILE="$RECEIPT_DIR/container.cid"
volume_created=false
cleanup() {
    original_status=$?
    cleanup_status=0
    if [ -s "$CID_FILE" ]; then
        cid="$(cat "$CID_FILE")"
        if ! printf '%s\n' "$cid" | grep -Eq '^[a-f0-9]{64}$'; then
            cleanup_status=1
        elif ! owner="$(docker inspect -f '{{.Name}} {{index .Config.Labels "io.iot.stage2-admin-run"}}' "$cid")"; then
            cleanup_status=1
        elif [ "$owner" != "/$CONTAINER $RUN_ID" ]; then
            echo 'Admin cleanup refused a foreign container.' >&2
            cleanup_status=1
        else
            docker rm -f -v "$cid" >/dev/null 2>&1 || cleanup_status=1
            if ! remaining="$(docker container ls -aq --no-trunc --filter "id=$cid")"; then
                cleanup_status=1
            elif [ -n "$remaining" ]; then
                cleanup_status=1
            fi
        fi
    fi
    if [ "$volume_created" = true ]; then
        if ! owner="$(docker volume inspect -f '{{index .Labels "io.iot.stage2-admin-run"}}' "$VOLUME")"; then
            cleanup_status=1
        elif [ "$owner" != "$RUN_ID" ]; then
            echo 'Admin cleanup refused a foreign volume.' >&2
            cleanup_status=1
        else
            docker volume rm "$VOLUME" >/dev/null 2>&1 || cleanup_status=1
            if ! remaining="$(docker volume ls -q --filter "name=^$VOLUME$")"; then
                cleanup_status=1
            elif [ -n "$remaining" ]; then
                cleanup_status=1
            fi
        fi
    fi
    if [ "$cleanup_status" -ne 0 ]; then
        echo 'Admin fixture cleanup failed; receipts retained.' >&2
        exit 1
    fi
    echo 'Admin fixture exact container/volume cleanup verified.'
    exit "$original_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Ephemeral credentials are generated for this container; do not print them.
POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
BACKEND_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
AUTH_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
STORAGE_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
POSTGRES_USER=stage2_admin
POSTGRES_DB=stage2_admin_test
export POSTGRES_PASSWORD POSTGRES_USER POSTGRES_DB BACKEND_DB_PASSWORD AUTH_DB_PASSWORD STORAGE_DB_PASSWORD
test -z "$(docker container ls -aq --filter "name=^/$CONTAINER$")"
test -z "$(docker volume ls -q --filter "name=^$VOLUME$")"
volume_created=true
docker volume create --label "io.iot.stage2-admin-run=$RUN_ID" "$VOLUME" >/dev/null
docker create --name "$CONTAINER" --cidfile "$CID_FILE" \
    --label "io.iot.stage2-admin-run=$RUN_ID" \
    -e POSTGRES_PASSWORD -e POSTGRES_USER -e POSTGRES_DB \
    -e BACKEND_DB_PASSWORD -e AUTH_DB_PASSWORD -e STORAGE_DB_PASSWORD \
    -p 127.0.0.1::5432 \
    --mount "type=volume,src=$VOLUME,dst=/var/lib/postgresql/data" \
    -v "$PROJECT_ROOT/migrations:/docker-entrypoint-initdb.d:ro" \
    "$IMAGE" >/dev/null
docker start "$(cat "$CID_FILE")" >/dev/null

ready=false
for _ in $(seq 1 90); do
    if docker logs "$CONTAINER" 2>&1 | grep -F 'PostgreSQL init process complete; ready for start up' >/dev/null &&
       docker exec "$CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
        ready=true
        break
    fi
    if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" | grep -qx true; then
        echo 'Isolated PostgreSQL initialization failed.' >&2
        exit 1
    fi
    sleep 1
done
[ "$ready" = true ] || { echo 'Timed out waiting for isolated PostgreSQL.' >&2; exit 1; }
port="$(docker port "$CONTAINER" 5432/tcp | sed 's/.*://')"
ADMIN_TEST_SETUP_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
ADMIN_TEST_BACKEND_URL="postgres://iot_backend_app:$BACKEND_DB_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
ADMIN_TEST_ISOLATED=1
export ADMIN_TEST_SETUP_URL ADMIN_TEST_BACKEND_URL ADMIN_TEST_ISOLATED
cd "$PROJECT_ROOT/src"
go test -race -count=1 ./internal/auth -run '^TestPostgresAdminIntegration$'
echo 'Isolated platform-admin integration tests passed.'
