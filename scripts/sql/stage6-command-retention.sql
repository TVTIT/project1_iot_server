\set ON_ERROR_STOP on

\if :{?retention_mode}
\else
\set retention_mode dry-run
\endif

\if :{?target_database}
\else
\echo 'Retention refused: target_database must explicitly name the connected database.'
SELECT 1 / 0;
\endif

SELECT :'retention_mode' IN ('dry-run', 'apply') AS retention_mode_valid \gset
\if :retention_mode_valid
\else
\echo 'Retention refused: mode must be dry-run or apply.'
SELECT 1 / 0;
\endif

SELECT current_database() = :'target_database'
   AND to_regclass('public.schema_migrations') IS NOT NULL
   AND to_regclass('public.twin_commands') IS NOT NULL
   AND to_regclass('public.twin_command_requests') IS NOT NULL
   AND (SELECT count(*) = 1 FROM public.schema_migrations
        WHERE version = 27 AND name = 'stage6_control_resilience')
   AND (SELECT max(version) = 27 FROM public.schema_migrations)
   AND (SELECT count(*) = 4
        FROM pg_constraint
        WHERE contype = 'f' AND confrelid = 'public.twin_commands'::regclass)
   AND (SELECT count(*) = 1 FROM pg_constraint
        WHERE contype = 'f' AND confrelid = 'public.twin_commands'::regclass
          AND conrelid = 'public.twin_outbox'::regclass AND confdeltype = 'c')
   AND (SELECT count(*) = 1 FROM pg_constraint
        WHERE contype = 'f' AND confrelid = 'public.twin_commands'::regclass
          AND conrelid = 'public.twin_command_requests'::regclass AND confdeltype = 'r')
   AND (SELECT count(*) = 2 FROM pg_constraint
        WHERE contype = 'f' AND confrelid = 'public.twin_commands'::regclass
          AND conrelid = 'public.twin_commands'::regclass AND confdeltype = 'r')
   AS retention_target_schema_valid \gset
\if :retention_target_schema_valid
\else
\echo 'Retention refused: target or schema/foreign-key contract does not match the verified Stage 6 database.'
SELECT 1 / 0;
\endif

SELECT :'retention_mode' = 'apply' AS retention_apply \gset
\if :retention_apply
\if :{?confirm_apply_target}
SELECT :'confirm_apply_target' = current_database()
   AND :'confirm_apply_target' = :'target_database' AS retention_apply_confirmed \gset
\if :retention_apply_confirmed
\else
\echo 'Retention refused: apply requires confirm_apply_target to exactly match the connected database.'
SELECT 1 / 0;
\endif
\else
\echo 'Retention refused: apply requires confirm_apply_target.'
SELECT 1 / 0;
\endif

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE OR REPLACE FUNCTION pg_temp.stage6_retention_json(bytes bytea)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $safe_json$
BEGIN
    RETURN convert_from(bytes, 'UTF8')::jsonb;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END
$safe_json$;
LOCK TABLE public.twin_states IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.twin_commands IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.twin_outbox IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.twin_command_requests IN SHARE ROW EXCLUSIVE MODE;
\set retention_apply true
\ir stage6-command-retention-candidates.sql
CREATE TEMP TABLE retention_batch ON COMMIT DROP AS
SELECT root_id, child_id
FROM jsonb_to_recordset(:'retention_candidates'::jsonb)
    AS candidate(root_id uuid, child_id uuid);

DO $retention$
DECLARE
    expected_roots integer;
    expected_children integer;
    expected_receipts integer;
    affected integer;
BEGIN
    SELECT count(*), count(child_id), count(*)
      INTO expected_roots, expected_children, expected_receipts
      FROM pg_temp.retention_batch;

    DELETE FROM public.twin_command_requests request
    USING pg_temp.retention_batch batch
    WHERE request.command_id = batch.root_id
      AND request.status_code = 202;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> expected_receipts THEN
        RAISE EXCEPTION 'Retention receipt deletion count mismatch';
    END IF;

    DELETE FROM public.twin_commands child
    USING pg_temp.retention_batch batch
    WHERE batch.child_id IS NOT NULL
      AND child.command_id = batch.child_id;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> expected_children THEN
        RAISE EXCEPTION 'Retention successor deletion count mismatch';
    END IF;

    DELETE FROM public.twin_commands root
    USING pg_temp.retention_batch batch
    WHERE root.command_id = batch.root_id;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> expected_roots THEN
        RAISE EXCEPTION 'Retention root deletion count mismatch';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_temp.retention_batch batch
        JOIN public.twin_outbox outbox
          ON outbox.command_id = batch.root_id
          OR outbox.command_id = batch.child_id
    ) OR EXISTS (
        SELECT 1
        FROM pg_temp.retention_batch batch
        JOIN public.twin_command_requests request
          ON request.command_id = batch.root_id
          OR request.command_id = batch.child_id
    ) OR EXISTS (
        SELECT 1
        FROM pg_temp.retention_batch batch
        JOIN public.twin_commands command
          ON command.command_id = batch.root_id
          OR command.command_id = batch.child_id
          OR command.root_command_id = batch.root_id
          OR command.parent_command_id = batch.root_id
    ) THEN
        RAISE EXCEPTION 'Retention left a command lineage association behind';
    END IF;
END
$retention$;

COMMIT;
\echo 'APPLY complete: lineages removed = ' :retention_count ' (maximum 100); re-run dry-run to inspect the next batch.'
\else
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET LOCAL statement_timeout = '10s';
CREATE OR REPLACE FUNCTION pg_temp.stage6_retention_json(bytes bytea)
RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE
AS $safe_json$
BEGIN
    RETURN convert_from(bytes, 'UTF8')::jsonb;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END
$safe_json$;
\set retention_apply false
\ir stage6-command-retention-candidates.sql
\echo 'DRY-RUN: eligible lineages in this bounded batch = ' :retention_count ' (maximum 100); no business rows changed; temporary decoder rolled back.'
ROLLBACK;
SELECT to_regprocedure('pg_temp.stage6_retention_json(bytea)') IS NULL
    AS retention_temp_decoder_gone \gset
\echo 'DRY-RUN: temporary decoder absent after rollback = ' :retention_temp_decoder_gone
\endif
