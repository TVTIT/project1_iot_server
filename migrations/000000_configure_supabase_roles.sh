#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${AUTH_DB_PASSWORD:?AUTH_DB_PASSWORD is required}"
: "${STORAGE_DB_PASSWORD:?STORAGE_DB_PASSWORD is required}"
: "${BACKEND_DB_PASSWORD:?BACKEND_DB_PASSWORD is required}"

case "$AUTH_DB_PASSWORD" in
    *[!A-Za-z0-9._~-]*)
        echo "AUTH_DB_PASSWORD must contain only URL-safe characters" >&2
        exit 1
        ;;
esac
if [ "${#AUTH_DB_PASSWORD}" -lt 22 ]; then
    echo "AUTH_DB_PASSWORD must be at least 22 characters" >&2
    exit 1
fi
case "$STORAGE_DB_PASSWORD" in
    *[!A-Za-z0-9._~-]*)
        echo "STORAGE_DB_PASSWORD must contain only URL-safe characters" >&2
        exit 1
        ;;
esac
if [ "${#STORAGE_DB_PASSWORD}" -lt 22 ]; then
    echo "STORAGE_DB_PASSWORD must be at least 22 characters" >&2
    exit 1
fi
case "$BACKEND_DB_PASSWORD" in
    *[!A-Za-z0-9._~-]*)
        echo "BACKEND_DB_PASSWORD must contain only URL-safe characters" >&2
        exit 1
        ;;
esac
if [ "${#BACKEND_DB_PASSWORD}" -lt 22 ]; then
    echo "BACKEND_DB_PASSWORD must be at least 22 characters" >&2
    exit 1
fi

psql --set ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    <<'SQL'
\getenv auth_db_password AUTH_DB_PASSWORD
\getenv storage_db_password STORAGE_DB_PASSWORD
\getenv backend_db_password BACKEND_DB_PASSWORD
BEGIN;
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

SELECT 'CREATE ROLE iot_backend NOLOGIN BYPASSRLS'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iot_backend')
\gexec
ALTER ROLE iot_backend WITH NOLOGIN INHERIT BYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

SELECT format(
    'CREATE ROLE iot_backend_app LOGIN PASSWORD %L IN ROLE iot_backend',
    :'backend_db_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iot_backend_app')
\gexec
SELECT format(
    'ALTER ROLE iot_backend_app WITH LOGIN INHERIT PASSWORD %L BYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION',
    :'backend_db_password'
) \gexec
GRANT iot_backend TO iot_backend_app;
SELECT format(
    'REVOKE CREATE ON DATABASE %I FROM iot_backend, iot_backend_app',
    current_database()
) \gexec

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_auth_members AS membership
        JOIN pg_roles AS member_role ON member_role.oid = membership.member
        JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid
        WHERE (
            member_role.rolname IN ('iot_backend', 'iot_backend_app')
            OR granted_role.rolname IN ('iot_backend', 'iot_backend_app')
        )
          AND NOT (
              member_role.rolname = 'iot_backend_app'
              AND granted_role.rolname = 'iot_backend'
          )
    ) THEN
        RAISE EXCEPTION 'Backend database role has an unexpected role membership';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_database
        WHERE datname = current_database()
          AND datdba IN (
              SELECT oid FROM pg_roles WHERE rolname IN ('iot_backend', 'iot_backend_app')
          )
    ) OR EXISTS (
        SELECT 1 FROM pg_namespace
        WHERE nspowner IN (
            SELECT oid FROM pg_roles WHERE rolname IN ('iot_backend', 'iot_backend_app')
        )
    ) OR EXISTS (
        SELECT 1 FROM pg_class
        WHERE relowner IN (
            SELECT oid FROM pg_roles WHERE rolname IN ('iot_backend', 'iot_backend_app')
        )
    ) OR EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proowner IN (
            SELECT oid FROM pg_roles WHERE rolname IN ('iot_backend', 'iot_backend_app')
        )
    ) THEN
        RAISE EXCEPTION 'Backend database role unexpectedly owns a database object';
    END IF;
END
$$;
COMMIT;
SQL
