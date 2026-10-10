-- Structural guards resolve persistent truth, never caller/temp relations.
-- ALTER preserves bodies, trigger bindings, ownership and existing ACLs.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;
SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=18) AS apply_guard_search_path \gset
\if :apply_guard_search_path
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=17 AND name='mqtt_rotation_replacement') THEN
        RAISE EXCEPTION 'Migration 000017 is required';
    END IF;
    -- public must remain trusted; fail instead of hiding unsafe CREATE grants.
    IF has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n',n.nspowner))) a
                  WHERE n.nspname='public' AND a.grantee=0 AND a.privilege_type='CREATE') THEN
        RAISE EXCEPTION 'Guard search_path requires trusted public schema';
    END IF;
END $$;
-- Complete v17 trigger/callee closure, including v12 automatic checkpoint.
ALTER FUNCTION public.enforce_gateway_mqtt_event_transition() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.create_mqtt_maintenance_intent() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_maintenance_transition() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_maintenance_admission() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_recovery_transition() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.check_mqtt_recovery_atomicity() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_recovery_authority() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_projection_transition() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.check_mqtt_projection_authority() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.validate_mqtt_projection_authority(text) SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.check_mqtt_event_authority() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.lock_mqtt_replacement_admin(uuid) SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.mqtt_operation_effective(uuid) SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_replacement_disposition() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_replacement_event() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_superseded_checkpoint() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.enforce_mqtt_replacement_no_startup_recovery() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.validate_mqtt_replacement_inventory(text) SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.check_mqtt_replacement_inventory() SET search_path = pg_catalog, public, pg_temp;
INSERT INTO public.schema_migrations(version,name) VALUES(18,'mqtt_guard_search_path');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=18 AND name='mqtt_guard_search_path') THEN
        RAISE EXCEPTION 'Unexpected migration version 18';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
