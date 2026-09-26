#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${AUTH_DB_PASSWORD:?AUTH_DB_PASSWORD is required}"
: "${STORAGE_DB_PASSWORD:?STORAGE_DB_PASSWORD is required}"

case "$AUTH_DB_PASSWORD" in
    *[!A-Za-z0-9._~-]*)
        echo "AUTH_DB_PASSWORD must contain only URL-safe characters" >&2
        exit 1
        ;;
esac
case "$STORAGE_DB_PASSWORD" in
    *[!A-Za-z0-9._~-]*)
        echo "STORAGE_DB_PASSWORD must contain only URL-safe characters" >&2
        exit 1
        ;;
esac

psql --set ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set auth_db_password="$AUTH_DB_PASSWORD" \
    --set storage_db_password="$STORAGE_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE supabase_auth_admin LOGIN PASSWORD %L', :'auth_db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_auth_admin')
\gexec
SELECT format('ALTER ROLE supabase_auth_admin WITH LOGIN PASSWORD %L', :'auth_db_password') \gexec

SELECT format('CREATE ROLE supabase_storage_admin LOGIN PASSWORD %L', :'storage_db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_storage_admin')
\gexec
SELECT format('ALTER ROLE supabase_storage_admin WITH LOGIN PASSWORD %L', :'storage_db_password') \gexec

SELECT 'CREATE ROLE supabase_admin NOLOGIN BYPASSRLS'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin')
\gexec
SQL
