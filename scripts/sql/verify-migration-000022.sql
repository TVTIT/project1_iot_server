\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=22 AND name='gateway_membership_revocation')
       OR (SELECT max(version) FROM public.schema_migrations) <> 22 THEN
        RAISE EXCEPTION 'migration 22 tracking is missing or unexpected';
    END IF;
    IF NOT has_table_privilege('iot_backend', 'public.user_gateways', 'DELETE')
       OR NOT has_table_privilege('iot_backend_app', 'public.user_gateways', 'DELETE') THEN
        RAISE EXCEPTION 'backend must inherit membership DELETE';
    END IF;
    IF has_table_privilege('anon', 'public.user_gateways', 'DELETE')
       OR has_table_privilege('authenticated', 'public.user_gateways', 'DELETE') THEN
        RAISE EXCEPTION 'human roles must not gain membership DELETE';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='iot_backend_app' AND rolsuper) THEN
        RAISE EXCEPTION 'runtime principal must not be superuser';
    END IF;
END $$;
COMMIT;
SELECT 'Migration 000022 verification passed.';
