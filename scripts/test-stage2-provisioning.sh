#!/bin/sh
# Isolated PostgreSQL only; never source deployment .env or use deployment data.
set -eu
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
CONTAINER="stage2-provisioning-$$"
created=false
cleanup() {
    status=$?
    trap - EXIT
    if [ "$created" = true ]; then
        if ! docker rm -f -v "$CONTAINER" >/dev/null 2>&1; then
            echo 'Failed to remove isolated provisioning container/volumes.' >&2
            [ "$status" -ne 0 ] || status=1
        fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
BACKEND_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
AUTH_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
STORAGE_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
POSTGRES_USER=stage2_provisioning_admin
POSTGRES_DB=stage2_provisioning_test
export POSTGRES_PASSWORD POSTGRES_USER POSTGRES_DB BACKEND_DB_PASSWORD AUTH_DB_PASSWORD STORAGE_DB_PASSWORD
docker run -d --name "$CONTAINER" \
    -e POSTGRES_PASSWORD -e POSTGRES_USER -e POSTGRES_DB \
    -e BACKEND_DB_PASSWORD -e AUTH_DB_PASSWORD -e STORAGE_DB_PASSWORD \
    -p 127.0.0.1::5432 \
    -v "$PROJECT_ROOT/migrations:/docker-entrypoint-initdb.d:ro" \
    "$IMAGE" >/dev/null
created=true
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
PROVISIONING_TEST_SETUP_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
PROVISIONING_TEST_BACKEND_URL="postgres://iot_backend_app:$BACKEND_DB_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
PROVISIONING_TEST_ISOLATED=1
export PROVISIONING_TEST_SETUP_URL PROVISIONING_TEST_BACKEND_URL PROVISIONING_TEST_ISOLATED
cd "$PROJECT_ROOT/src"
# Fixtures share this isolated database; run packages serially so repository
# cleanup/fault injection cannot race HTTP fixtures.
go test -p 1 -v -race -count=1 ${PROVISIONING_TEST_COVERAGE:+-coverprofile="$PROVISIONING_TEST_COVERAGE"} ./internal/gateway ./internal/httpserver -run '^(TestPostgresProvisioningIntegration|TestGatewayProvisioningHTTPIntegration)$'
echo 'Isolated Gateway provisioning integration tests passed.'
