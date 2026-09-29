#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"
: "${PLATFORM_ADMIN_USER_ID:?PLATFORM_ADMIN_USER_ID is required}"

case "$PLATFORM_ADMIN_USER_ID" in
    ????????-????-????-????-????????????) ;;
    *)
        echo "PLATFORM_ADMIN_USER_ID must use UUID syntax" >&2
        exit 1
        ;;
esac

psql --set ON_ERROR_STOP=1 \
    --set platform_admin_user_id="$PLATFORM_ADMIN_USER_ID" <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(3290, 21);
SELECT EXISTS (
    SELECT 1 FROM profiles WHERE id = :'platform_admin_user_id'::UUID
) AS profile_exists
\gset
\if :profile_exists
SELECT NOT EXISTS (
    SELECT 1 FROM platform_admins
    WHERE user_id <> :'platform_admin_user_id'::UUID
) AS bootstrap_allowed
\gset
\if :bootstrap_allowed
INSERT INTO platform_admins (user_id)
VALUES (:'platform_admin_user_id'::UUID)
ON CONFLICT (user_id) DO NOTHING;
\else
DO $$ BEGIN RAISE EXCEPTION 'Cannot bootstrap an additional platform admin'; END $$;
\endif
\else
DO $$ BEGIN RAISE EXCEPTION 'Cannot grant platform admin: profile does not exist'; END $$;
\endif
COMMIT;
SQL

echo "Platform administrator membership is present for the requested user."
