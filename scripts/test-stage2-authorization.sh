#!/bin/sh
# Isolated PostgreSQL authorization tests; never sources deployment .env.
set -eu
PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
IMAGE="timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
SUFFIX="$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
CONTAINER="stage2-authorization-$SUFFIX"
created=false
cleanup() {
    if [ "$created" = true ]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
BACKEND_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
AUTH_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
STORAGE_DB_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
POSTGRES_USER=stage2_admin
POSTGRES_DB=stage2_authorization_test
export POSTGRES_PASSWORD BACKEND_DB_PASSWORD AUTH_DB_PASSWORD STORAGE_DB_PASSWORD POSTGRES_USER POSTGRES_DB
docker create --name "$CONTAINER" \
    -e POSTGRES_PASSWORD -e POSTGRES_USER -e POSTGRES_DB \
    -e BACKEND_DB_PASSWORD -e AUTH_DB_PASSWORD -e STORAGE_DB_PASSWORD \
    -p 127.0.0.1::5432 \
    -v "$PROJECT_ROOT/migrations:/docker-entrypoint-initdb.d:ro" \
    "$IMAGE" >/dev/null
created=true
docker start "$CONTAINER" >/dev/null
ready=false
for _ in $(seq 1 90); do
    if docker logs "$CONTAINER" 2>&1 | grep -F 'PostgreSQL init process complete; ready for start up' >/dev/null &&
       docker exec "$CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
        ready=true
        break
    fi
    if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" | grep -qx true; then
        echo 'Isolated authorization PostgreSQL initialization failed.' >&2
        exit 1
    fi
    sleep 1
done
[ "$ready" = true ] || { echo 'Timed out waiting for isolated authorization PostgreSQL.' >&2; exit 1; }
port="$(docker port "$CONTAINER" 5432/tcp | sed 's/.*://')"
AUTHORIZATION_TEST_SETUP_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
AUTHORIZATION_TEST_BACKEND_URL="postgres://iot_backend_app:$BACKEND_DB_PASSWORD@127.0.0.1:$port/$POSTGRES_DB?sslmode=disable"
AUTHORIZATION_TEST_ISOLATED=1
export AUTHORIZATION_TEST_SETUP_URL AUTHORIZATION_TEST_BACKEND_URL AUTHORIZATION_TEST_ISOLATED
cd "$PROJECT_ROOT/src"
set -- -v -race -count=1 -cover
if [ -n "${AUTHORIZATION_TEST_COVERAGE_FILE:-}" ]; then
    set -- "$@" "-coverprofile=$AUTHORIZATION_TEST_COVERAGE_FILE"
fi
go test "$@" ./internal/gateway ./internal/httpserver -run 'Test(ValidateGatewayID|Repository|Role|ReadService|GatewayList|RouterRejectsMissingGatewayReader|PostgresAuthorizationIntegration|GatewayHTTPAuthorizationIntegration)'
echo 'Isolated Gateway authorization integration tests passed.'
