\set ON_ERROR_STOP on
SELECT current_setting('server_version_num')::integer >= 120000 AS postgres_supported \gset
\if :postgres_supported
\else
    \echo 'Migration 28 verifier requires PostgreSQL 12 or newer'
    \quit 1
\endif

SELECT :'expected_schema_terminal'::integer = 28 AS expected_terminal \gset
\if :expected_terminal
\else
    \echo 'Migration 28 verifier requires expected_schema_terminal=28'
    \quit 1
\endif
SELECT max(version) = 28 AS terminal_verified FROM public.schema_migrations \gset
\if :terminal_verified
\else
    \echo 'Schema history does not end at migration 28'
    \quit 1
\endif

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations
        WHERE version = 28 AND name = 'stage8_media_metadata'
    ) THEN
        RAISE EXCEPTION 'Migration 28 history row is missing';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'media_objects'
          AND column_name IN (
              'managed', 'upload_id', 'captured_at', 'intent_expires_at',
              'declared_sha256', 'verified_sha256', 'actual_media_type',
              'latest_upload_grant_expires_at', 'verification_attempts',
              'verification_generation',
              'next_verification_at', 'last_verification_at',
              'last_verification_error', 'next_reconcile_at', 'terminal_reason',
              'cleanup_scheduled_at', 'cleanup_attempts', 'cleanup_confirmed_at'
          )
        GROUP BY table_name
        HAVING count(*) = 18
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'Migration 28 managed-media columns are incomplete';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'media_objects'
          AND indexname = 'uq_media_objects_gateway_upload'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'media_objects'
          AND indexname = 'uq_media_objects_managed_storage_path'
    ) THEN
        RAISE EXCEPTION 'Migration 28 managed-media uniqueness indexes are missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'media_objects'
          AND indexname = 'idx_media_objects_intent_expiry'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'media_objects'
          AND indexname = 'idx_media_objects_cleanup_due'
    ) THEN
        RAISE EXCEPTION 'Migration 28 bounded expiry/cleanup indexes are missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public' AND tablename = 'media_objects'
          AND policyname = 'media_objects_select_policy' AND roles @> ARRAY['authenticated']::name[]
    ) THEN
        RAISE EXCEPTION 'Migration 28 did not retain membership media SELECT policy';
    END IF;
END
$$;
