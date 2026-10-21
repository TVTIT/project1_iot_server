-- Grant only the backend group the membership DELETE needed by the admin API.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 22) AS apply_gateway_membership_revocation \gset
\if :apply_gateway_membership_revocation
-- Like migration 21, complete this phase after Auth/compat initialization.
SELECT to_regclass('auth.users') IS NOT NULL AS auth_ready \gset
\if :auth_ready
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 21 AND name = 'admin_user_management') THEN
        RAISE EXCEPTION 'Migration 000022 requires version 21 admin_user_management';
    END IF;
END $$;
GRANT DELETE ON TABLE public.user_gateways TO iot_backend;
INSERT INTO public.schema_migrations(version, name) VALUES (22, 'gateway_membership_revocation');
COMMIT;
\else
\echo 'Auth is not initialized; apply migration 000022 after Auth and migration 21.'
\endif
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 22 AND name = 'gateway_membership_revocation') THEN
        RAISE EXCEPTION 'Unexpected migration version 22';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
