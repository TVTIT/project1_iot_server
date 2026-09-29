#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"

MIGRATIONS_DIR="${MIGRATIONS_DIR:-/migrations}"
if [ ! -d "$MIGRATIONS_DIR" ]; then
    echo "Migration directory does not exist: $MIGRATIONS_DIR" >&2
    exit 1
fi

set -- "$MIGRATIONS_DIR"/0*.up.sql
if [ ! -e "$1" ]; then
    echo "No migration files found in $MIGRATIONS_DIR" >&2
    exit 1
fi

MIGRATION_SCRIPT="$(mktemp)"
cleanup() {
    rm -f "$MIGRATION_SCRIPT"
}
trap cleanup EXIT INT TERM

{
    printf '%s\n' '\set ON_ERROR_STOP on'
    printf '%s\n' "SET statement_timeout = '60s';"
    printf '%s\n' 'SELECT pg_advisory_lock(3290, 2);'
    printf '%s\n' 'SET statement_timeout = 0;'
    printf '%s\n' "SELECT to_regclass('public.schema_migrations') IS NOT NULL AS tracking_ready \\gset"
    printf '%s\n' '\if :tracking_ready'
    printf '%s\n' "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 9 AND name = 'migration_tracking_and_sensor_urn') AS baseline_ready \\gset"
    printf '%s\n' '\if :baseline_ready'

    for migration in "$MIGRATIONS_DIR"/0*.up.sql; do
        filename="$(basename "$migration")"
        case "$filename" in
            [0-9][0-9][0-9][0-9][0-9][0-9]_[A-Za-z0-9_]*.up.sql) ;;
            *)
                echo "Invalid migration filename: $filename" >&2
                exit 1
                ;;
        esac
        version_text="${filename%%_*}"
        version="$(printf '%s' "$version_text" | sed 's/^0*//')"
        [ -n "$version" ] || version=0
        if [ "$version" -lt 10 ]; then
            continue
        fi
        name="${filename#*_}"
        name="${name%.up.sql}"
        case "$name" in
            *[!A-Za-z0-9_]*)
                echo "Invalid migration filename: $filename" >&2
                exit 1
                ;;
        esac
        printf '%s\n' "SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $version) AS apply_migration \\gset"
        printf '%s\n' '\if :apply_migration'
        printf '\\echo Applying migration %s\n' "$filename"
        printf "\\i '%s'\n" "$migration"
        printf '%s\n' '\else'
        printf '%s\n' "SELECT name = '$name' AS migration_name_matches FROM schema_migrations WHERE version = $version \\gset"
        printf '%s\n' '\if :migration_name_matches'
        printf '\\echo Migration %s already applied.\n' "$filename"
        printf '%s\n' '\else'
        printf "%s\n" "DO 'BEGIN RAISE EXCEPTION ''Migration version $version has an unexpected recorded name''; END' LANGUAGE plpgsql;"
        printf '%s\n' '\endif'
        printf '%s\n' '\endif'
    done

    printf '%s\n' '\else'
    printf '%s\n' "DO 'BEGIN RAISE EXCEPTION ''Migration baseline version 9 is missing or invalid''; END' LANGUAGE plpgsql;"
    printf '%s\n' '\endif'
    printf '%s\n' '\else'
    printf '%s\n' "DO 'BEGIN RAISE EXCEPTION ''schema_migrations is missing; initialize or baseline the database first''; END' LANGUAGE plpgsql;"
    printf '%s\n' '\endif'
    printf '%s\n' 'SELECT pg_advisory_unlock(3290, 2);'
} >"$MIGRATION_SCRIPT"

psql --set ON_ERROR_STOP=1 --file "$MIGRATION_SCRIPT"
