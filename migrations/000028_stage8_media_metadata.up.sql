-- Migration 28: durable metadata for managed private-media uploads.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 28 AND name <> 'stage8_media_metadata') THEN
        RAISE EXCEPTION 'Migration 28 has an unexpected name';
    END IF;
END $$;

SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 28) AS apply_stage8_media_metadata \gset
\if :apply_stage8_media_metadata

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 27 AND name = 'stage6_control_resilience') THEN
        RAISE EXCEPTION 'Migration 28 requires predecessor migration 27 stage6_control_resilience';
    END IF;
END $$;

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
LOCK TABLE public.media_objects IN ACCESS EXCLUSIVE MODE;

-- Existing rows remain unmanaged, including their historical validation state.
-- checksum is retained as legacy metadata and is not promoted to verified evidence.
ALTER TABLE public.media_objects
    ADD COLUMN managed boolean NOT NULL DEFAULT false,
    ADD COLUMN upload_id text,
    ADD COLUMN captured_at timestamptz,
    ADD COLUMN intent_expires_at timestamptz,
    ADD COLUMN declared_sha256 text,
    ADD COLUMN verified_sha256 text,
    ADD COLUMN actual_media_type text,
    ADD COLUMN latest_upload_grant_expires_at timestamptz,
    ADD COLUMN upload_grant_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN verification_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN verification_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN next_verification_at timestamptz,
    ADD COLUMN last_verification_at timestamptz,
    ADD COLUMN last_verification_error text,
    ADD COLUMN next_reconcile_at timestamptz,
    ADD COLUMN terminal_reason text,
    ADD COLUMN cleanup_scheduled_at timestamptz,
    ADD COLUMN cleanup_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN next_cleanup_at timestamptz,
    ADD COLUMN last_cleanup_error text,
    ADD COLUMN cleanup_first_deleted_at timestamptz,
    ADD COLUMN cleanup_confirmed_at timestamptz;

ALTER TABLE public.media_objects
    ADD CONSTRAINT chk_media_objects_managed_metadata CHECK (
        managed IS FALSE OR (
            managed IS TRUE
            AND gateway_id IS NOT NULL
            AND upload_id IS NOT NULL
            AND upload_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
            AND storage_path IS NOT NULL AND storage_path <> ''
            AND storage_path = media_id::text || '.mp4'
            AND media_type IS NOT NULL
            AND media_type = 'video/mp4'
            AND created_at IS NOT NULL
            AND expected_size IS NOT NULL AND expected_size BETWEEN 1 AND 80000000
            AND captured_at IS NOT NULL
            AND intent_expires_at IS NOT NULL
            AND declared_sha256 IS NOT NULL
            AND declared_sha256 ~ '^[0-9a-f]{64}$'
            AND validation_status IS NOT NULL
            AND validation_status IN ('pending', 'verifying', 'ready', 'rejected', 'expired')
            AND verification_attempts >= 0
            AND verification_generation >= 0
            AND upload_grant_generation >= 0
            AND cleanup_attempts >= 0
            AND (actual_size IS NULL OR actual_size BETWEEN 0 AND 80000000)
            AND (verified_sha256 IS NULL OR verified_sha256 ~ '^[0-9a-f]{64}$')
            AND (
                validation_status <> 'ready'
                OR (
                    actual_size IS NOT NULL
                    AND actual_size = expected_size
                    AND actual_media_type IS NOT NULL AND actual_media_type <> ''
                    AND actual_media_type = media_type
                    AND verified_sha256 IS NOT NULL
                    AND verified_sha256 = declared_sha256
                )
            )
            AND (validation_status NOT IN ('rejected', 'expired') OR
                 (terminal_reason IS NOT NULL AND terminal_reason <> ''))
            AND (cleanup_scheduled_at IS NULL OR
                 (validation_status IN ('rejected', 'expired')
                  AND cleanup_scheduled_at >= GREATEST(
                      intent_expires_at,
                      COALESCE(latest_upload_grant_expires_at, intent_expires_at))))
            AND (cleanup_confirmed_at IS NULL OR
                 (cleanup_scheduled_at IS NOT NULL
                  AND cleanup_confirmed_at >= cleanup_scheduled_at
                  AND validation_status IN ('rejected', 'expired')))
        )
    ),
    ADD CONSTRAINT chk_media_objects_managed_intent_expiry
        CHECK (managed IS FALSE OR (created_at IS NOT NULL AND intent_expires_at > created_at)),
    ADD CONSTRAINT chk_media_objects_managed_grant_expiry
        CHECK (latest_upload_grant_expires_at IS NULL OR
               (created_at IS NOT NULL AND latest_upload_grant_expires_at > created_at));

CREATE UNIQUE INDEX uq_media_objects_gateway_upload
    ON public.media_objects(gateway_id, upload_id)
    WHERE managed IS TRUE;
CREATE UNIQUE INDEX uq_media_objects_managed_storage_path
    ON public.media_objects(storage_path)
    WHERE managed IS TRUE;
CREATE INDEX idx_media_objects_verification_due
    ON public.media_objects(next_verification_at, created_at, media_id)
    WHERE managed IS TRUE AND validation_status = 'verifying'
      AND next_verification_at IS NOT NULL;
CREATE INDEX idx_media_objects_intent_expiry
    ON public.media_objects(intent_expires_at, created_at, media_id)
    WHERE managed IS TRUE AND validation_status IN ('pending', 'verifying');
CREATE INDEX idx_media_objects_reconciliation_due
    ON public.media_objects(next_reconcile_at, created_at, media_id)
    WHERE managed IS TRUE AND validation_status IN ('pending', 'verifying')
      AND next_reconcile_at IS NOT NULL;
CREATE INDEX idx_media_objects_cleanup_due
    ON public.media_objects(next_cleanup_at, created_at, media_id)
    WHERE managed IS TRUE AND validation_status IN ('rejected', 'expired')
      AND cleanup_confirmed_at IS NULL AND next_cleanup_at IS NOT NULL;

-- Preserve the existing membership SELECT policy and backend write grants. Do not
-- give anonymous/authenticated clients direct metadata write or cleanup authority.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.media_objects FROM PUBLIC, anon, authenticated;

INSERT INTO public.schema_migrations(version, name) VALUES(28, 'stage8_media_metadata');

COMMIT;
\endif
SELECT pg_advisory_unlock(3290, 2);
