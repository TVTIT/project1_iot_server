#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"
PGPORT="${PGPORT:-5432}"

ACTOR=""
TARGET_USER=""
GATEWAY=""
ACTION=""
ROLE=""
JOURNAL_FILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        --actor)
            ACTOR="$2"
            shift 2
            ;;
        --target-user)
            TARGET_USER="$2"
            shift 2
            ;;
        --gateway)
            GATEWAY="$2"
            shift 2
            ;;
        --action)
            ACTION="$2"
            shift 2
            ;;
        --role)
            ROLE="$2"
            shift 2
            ;;
        --journal-file)
            JOURNAL_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

ACTOR="${ACTOR:-${ACTOR_USER_ID:-}}"
TARGET_USER="${TARGET_USER:-${TARGET_USER_ID:-}}"
GATEWAY="${GATEWAY:-${GATEWAY_ID:-}}"
ACTION="${ACTION:-${MEMBERSHIP_ACTION:-}}"
ROLE="${ROLE:-${MEMBERSHIP_ROLE:-}}"
JOURNAL_FILE="${JOURNAL_FILE:-${GATEWAY_MEMBERSHIP_JOURNAL_FILE:-}}"

[ -n "$JOURNAL_FILE" ] || { echo "durable journal file is required (--journal-file or GATEWAY_MEMBERSHIP_JOURNAL_FILE)" >&2; exit 1; }
[ -n "$ACTOR" ] || { echo "actor is required (--actor)" >&2; exit 1; }
[ -n "$TARGET_USER" ] || { echo "target user is required (--target-user)" >&2; exit 1; }
[ -n "$GATEWAY" ] || { echo "gateway is required (--gateway)" >&2; exit 1; }
[ -n "$ACTION" ] || { echo "action is required (--action: grant, change, revoke)" >&2; exit 1; }

case "$ACTOR" in
    ????????-????-????-????-????????????) ;;
    *)
        echo "actor must use valid UUID syntax" >&2
        exit 1
        ;;
esac

case "$TARGET_USER" in
    ????????-????-????-????-????????????) ;;
    *)
        echo "target-user must use valid UUID syntax" >&2
        exit 1
        ;;
esac

case "$ACTION" in
    grant|change)
        case "$ROLE" in
            viewer|operator) ;;
            *)
                echo "role must be 'viewer' or 'operator' for $ACTION action (cannot grant/change to owner)" >&2
                exit 1
                ;;
        esac
        ;;
    revoke)
        # Role is not required for revoke, but if supplied must not be owner
        if [ -n "$ROLE" ] && [ "$ROLE" = "owner" ]; then
            echo "cannot revoke owner role via sharing tool" >&2
            exit 1
        fi
        ;;
    *)
        echo "invalid action '$ACTION'; allowed actions are: grant, change, revoke" >&2
        exit 1
        ;;
esac

OP_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
TIMESTAMP="$(python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())')"

# Execute atomic SQL transaction
set +e
SQL_RESULT="$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    --set ON_ERROR_STOP=1 \
    --set actor="$ACTOR" \
    --set target_user="$TARGET_USER" \
    --set gateway_id="$GATEWAY" \
    --set action="$ACTION" \
    --set role="$ROLE" \
    -At <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(3290, 23);

-- 1. Validate actor is platform administrator
SELECT EXISTS (
    SELECT 1 FROM platform_admins WHERE user_id = :'actor'::UUID
) AS actor_is_admin
\gset
\if :actor_is_admin
    -- OK
\else
    DO $$ BEGIN RAISE EXCEPTION 'Actor % is not a platform administrator', :'actor'; END $$;
\endif

-- 2. Validate target user exists in profiles
SELECT EXISTS (
    SELECT 1 FROM profiles WHERE id = :'target_user'::UUID
) AS target_exists
\gset
\if :target_exists
    -- OK
\else
    DO $$ BEGIN RAISE EXCEPTION 'Target user profile % does not exist', :'target_user'; END $$;
\endif

-- 3. Validate gateway exists
SELECT EXISTS (
    SELECT 1 FROM gateways WHERE gateway_id = :'gateway_id'
) AS gateway_exists
\gset
\if :gateway_exists
    -- OK
\else
    DO $$ BEGIN RAISE EXCEPTION 'Gateway % does not exist', :'gateway_id'; END $$;
\endif

-- 4. Check current role and action booleans
SELECT
    COALESCE(
        (SELECT role FROM user_gateways WHERE user_id = :'target_user'::UUID AND gateway_id = :'gateway_id'),
        'NONE'
    ) AS current_role,
    (:'action' = 'grant') AS is_grant,
    (:'action' = 'change') AS is_change,
    (:'action' = 'revoke') AS is_revoke
\gset

SELECT (:'current_role' = 'owner') AS is_owner
\gset

-- Owner protection: never alter or revoke owner
\if :is_owner
    DO $$ BEGIN RAISE EXCEPTION 'Cannot modify or revoke gateway owner membership for % on %', :'target_user', :'gateway_id'; END $$;
\endif

-- 5. Perform requested action
\if :is_grant
    SELECT (:'current_role' = 'NONE') AS can_grant
    \gset
    \if :can_grant
        INSERT INTO user_gateways (user_id, gateway_id, role)
        VALUES (:'target_user'::UUID, :'gateway_id', :'role');
        SELECT 'NONE' AS before_role, :'role' AS after_role;
    \else
        DO $$ BEGIN RAISE EXCEPTION 'Target user % already has membership (%) on %; use change or revoke', :'target_user', :'current_role', :'gateway_id'; END $$;
    \endif
\elif :is_change
    SELECT (:'current_role' <> 'NONE') AS can_change
    \gset
    \if :can_change
        UPDATE user_gateways
        SET role = :'role'
        WHERE user_id = :'target_user'::UUID AND gateway_id = :'gateway_id';
        SELECT :'current_role' AS before_role, :'role' AS after_role;
    \else
        DO $$ BEGIN RAISE EXCEPTION 'Target user % has no membership on %; use grant', :'target_user', :'gateway_id'; END $$;
    \endif
\elif :is_revoke
    SELECT (:'current_role' <> 'NONE') AS can_revoke
    \gset
    \if :can_revoke
        DELETE FROM user_gateways
        WHERE user_id = :'target_user'::UUID AND gateway_id = :'gateway_id';
        SELECT :'current_role' AS before_role, 'NONE' AS after_role;
    \else
        DO $$ BEGIN RAISE EXCEPTION 'Target user % has no membership on % to revoke', :'target_user', :'gateway_id'; END $$;
    \endif
\endif

COMMIT;
SQL
)"
EXIT_CODE=$?
set -e

if [ "$EXIT_CODE" -ne 0 ]; then
    # Transaction was aborted or failed. Record failure in durable journal.
    FAIL_ENTRY="$(python3 -c "
import json
entry = {
    'timestamp': '$TIMESTAMP',
    'operation_id': '$OP_ID',
    'actor': '$ACTOR',
    'target_user': '$TARGET_USER',
    'gateway_id': '$GATEWAY',
    'action': '$ACTION',
    'outcome': 'failed_or_uncertain'
}
print(json.dumps(entry))
")"
    echo "$FAIL_ENTRY" >> "$JOURNAL_FILE"
    echo "psql transaction failed (exit $EXIT_CODE)" >&2
    exit "$EXIT_CODE"
fi

BEFORE_ROLE="$(echo "$SQL_RESULT" | head -n 1 | cut -d'|' -f1)"
AFTER_ROLE="$(echo "$SQL_RESULT" | head -n 1 | cut -d'|' -f2)"

# Emit structured journal record
JOURNAL_ENTRY="$(python3 -c "
import json
entry = {
    'timestamp': '$TIMESTAMP',
    'operation_id': '$OP_ID',
    'actor': '$ACTOR',
    'target_user': '$TARGET_USER',
    'gateway_id': '$GATEWAY',
    'action': '$ACTION',
    'before_role': '$BEFORE_ROLE',
    'after_role': '$AFTER_ROLE',
    'outcome': 'committed'
}
print(json.dumps(entry))
")"

echo "$JOURNAL_ENTRY" >> "$JOURNAL_FILE"
echo "$JOURNAL_ENTRY"
