-- Post-Storage compatibility bootstrap for Stage 8 media playback.
-- Configures a dedicated, narrowly authorized non-BYPASSRLS role for issuing signed playback URLs.
-- Executed atomically; idempotent and safe for fresh and existing schemas.
--
-- Security Contract:
-- 1. storage_playback_signer must NOT bypass RLS (NOBYPASSRLS) and cannot login directly (NOLOGIN).
-- 2. Granted only SELECT on storage.objects (restricted by RLS to bucket_id = 'media-video')
--    and storage.buckets (restricted to id = 'media-video').
-- 3. Granted to supabase_storage_admin so Storage can set role to storage_playback_signer for read signing.
-- 4. No permissions on application data tables (public schema).
-- 5. Does not mutate global Storage table RLS state; verifies that RLS is already in place as established
--    by pinned Storage, failing closed if unexpected.
-- 6. Token provisioning/rotation: Tokens must be minted from the storage JWT secret with role='storage_playback_signer'
--    and provided via MEDIA_PLAYBACK_STORAGE_TOKEN. The backend holds neither the signing secret nor broad keys
--    for playback operations, and has no fallback.

BEGIN;

DO $$
DECLARE
    objects_rls BOOLEAN;
    buckets_rls BOOLEAN;
BEGIN
    -- Ensure required Storage tables exist; fail closed if Storage schema is uninitialized
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema = 'storage' AND table_name = 'objects'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema = 'storage' AND table_name = 'buckets'
    ) THEN
        RAISE EXCEPTION 'post-storage bootstrap failed: storage schema tables (objects, buckets) do not exist';
    END IF;

    -- Verify expected tables/RLS are already in place for pinned Storage; fail closed without mutating global table state
    SELECT c.relrowsecurity INTO objects_rls
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'storage' AND c.relname = 'objects';

    SELECT c.relrowsecurity INTO buckets_rls
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'storage' AND c.relname = 'buckets';

    IF NOT coalesce(objects_rls, false) OR NOT coalesce(buckets_rls, false) THEN
        RAISE EXCEPTION 'post-storage bootstrap failed: expected RLS to be already active on storage.objects and storage.buckets';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'storage_playback_signer') THEN
        CREATE ROLE storage_playback_signer NOLOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
    ELSE
        ALTER ROLE storage_playback_signer WITH NOLOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END $$;

-- Guard against stale conflicting grants on public schema
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM storage_playback_signer;
REVOKE ALL ON SCHEMA public FROM storage_playback_signer;

-- Revoke all on storage schema then grant minimal exact privileges
REVOKE ALL ON ALL TABLES IN SCHEMA storage FROM storage_playback_signer;
GRANT USAGE ON SCHEMA storage TO storage_playback_signer;
GRANT SELECT ON storage.objects TO storage_playback_signer;
GRANT SELECT ON storage.buckets TO storage_playback_signer;
GRANT storage_playback_signer TO supabase_storage_admin;

-- Idempotently establish exact RLS policies without altering global table RLS state
DROP POLICY IF EXISTS storage_playback_signer_read_video ON storage.objects;
CREATE POLICY storage_playback_signer_read_video ON storage.objects
    FOR SELECT TO storage_playback_signer
    USING (bucket_id = 'media-video');

DROP POLICY IF EXISTS storage_playback_signer_read_buckets ON storage.buckets;
CREATE POLICY storage_playback_signer_read_buckets ON storage.buckets
    FOR SELECT TO storage_playback_signer
    USING (id = 'media-video');

COMMIT;
