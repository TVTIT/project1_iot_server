\set ON_ERROR_STOP on
BEGIN;
-- Only these two boundaries are supported. The v19 marker prevents a missing
-- tracking row from masquerading as a valid v18 database.
CREATE FUNCTION pg_temp.verify_guard_tracking() RETURNS void LANGUAGE plpgsql AS $$
DECLARE latest_version bigint;
BEGIN
    SELECT max(version) INTO latest_version FROM public.schema_migrations;
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 18 AND name = 'mqtt_guard_search_path') THEN
        RAISE EXCEPTION 'Migration 000018 is not recorded' USING ERRCODE = 'P0018';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 17 AND name = 'mqtt_rotation_replacement') THEN
        RAISE EXCEPTION 'Migration 000017 is required' USING ERRCODE = 'P0018';
    END IF;
    IF latest_version IS NULL OR latest_version NOT IN (18, 19)
       OR ((latest_version = 19) IS DISTINCT FROM
           (to_regclass('public.mqtt_reset_dispositions') IS NOT NULL))
       OR EXISTS (SELECT 1 FROM schema_migrations WHERE version BETWEEN 11 AND latest_version AND
           name IS DISTINCT FROM CASE version
             WHEN 11 THEN 'mqtt_credential_operations' WHEN 12 THEN 'mqtt_credential_maintenance'
             WHEN 13 THEN 'mqtt_maintenance_admission' WHEN 14 THEN 'mqtt_credential_recovery'
             WHEN 15 THEN 'mqtt_recovery_authority_guards' WHEN 16 THEN 'mqtt_event_authority_guard'
             WHEN 17 THEN 'mqtt_rotation_replacement'
             WHEN 18 THEN 'mqtt_guard_search_path'
             WHEN 19 THEN 'mqtt_simplified_journal' END)
       OR (SELECT count(*) FROM schema_migrations WHERE version BETWEEN 11 AND latest_version) <> latest_version - 10 THEN
        RAISE EXCEPTION 'Unknown or inconsistent credential migration tracking for schema 018' USING ERRCODE = 'P0018';
    END IF;
END $$;
SELECT pg_temp.verify_guard_tracking();

-- Faults run in exception subtransactions: both tracking and schema remain
-- unchanged. Catch only the tracking validator's dedicated SQLSTATE.
DO $$ DECLARE mutation text; latest_version bigint; BEGIN
    SELECT max(version) INTO latest_version FROM public.schema_migrations;
    FOR mutation IN SELECT * FROM (VALUES
        ('UPDATE public.schema_migrations SET name=''incorrect_guard'' WHERE version=18'),
        ('DELETE FROM public.schema_migrations WHERE version=18'),
        ('DELETE FROM public.schema_migrations WHERE version=17'),
        ('UPDATE public.schema_migrations SET name=''incorrect_journal'' WHERE version=19'),
        ('DELETE FROM public.schema_migrations WHERE version=19'),
        ('INSERT INTO public.schema_migrations(version,name) VALUES(20,''unsupported_future'')')
    ) AS faults(statement) LOOP
        IF latest_version = 18 AND mutation LIKE '%version=19' THEN
            CONTINUE;
        END IF;
        BEGIN
            EXECUTE mutation;
            PERFORM pg_temp.verify_guard_tracking();
        EXCEPTION WHEN SQLSTATE 'P0018' THEN
            CONTINUE;
        END;
        RAISE EXCEPTION 'Tracking fault unexpectedly accepted: %', mutation;
    END LOOP;
    PERFORM pg_temp.verify_guard_tracking();
END $$;

DO $$ BEGIN
    -- public schema must remain trusted; fail if unsafe CREATE privileges exist
    IF has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n',n.nspowner))) a
                  WHERE n.nspname='public' AND a.grantee=0 AND a.privilege_type='CREATE') THEN
        RAISE EXCEPTION 'Guard search_path requires trusted public schema';
    END IF;
END $$;

-- Verify all 19 functions: existence, exact proconfig, prosecdef, proowner
DO $$ DECLARE expected record; actual record; BEGIN
    FOR expected IN SELECT * FROM (VALUES
        ('enforce_gateway_mqtt_event_transition()', false),
        ('create_mqtt_maintenance_intent()', false),
        ('enforce_mqtt_maintenance_transition()', false),
        ('enforce_mqtt_maintenance_admission()', false),
        ('enforce_mqtt_recovery_transition()', false),
        ('check_mqtt_recovery_atomicity()', false),
        ('enforce_mqtt_recovery_authority()', false),
        ('enforce_mqtt_projection_transition()', false),
        ('check_mqtt_projection_authority()', false),
        ('validate_mqtt_projection_authority(text)', false),
        ('check_mqtt_event_authority()', false),
        ('lock_mqtt_replacement_admin(uuid)', true),
        ('mqtt_operation_effective(uuid)', false),
        ('enforce_mqtt_replacement_disposition()', false),
        ('enforce_mqtt_replacement_event()', false),
        ('enforce_mqtt_superseded_checkpoint()', false),
        ('enforce_mqtt_replacement_no_startup_recovery()', false),
        ('validate_mqtt_replacement_inventory(text)', false),
        ('check_mqtt_replacement_inventory()', false)
    ) AS e(signature, definer) LOOP
        SELECT prosecdef, proconfig, proowner INTO actual
          FROM pg_proc WHERE oid = to_regprocedure('public.' || expected.signature);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Missing guard function: %', expected.signature;
        END IF;
        IF actual.prosecdef <> expected.definer THEN
            RAISE EXCEPTION 'Incorrect security definer setting for %: expected %, got %',
                expected.signature, expected.definer, actual.prosecdef;
        END IF;
        IF actual.proowner <> (SELECT relowner FROM pg_class WHERE oid='schema_migrations'::regclass) THEN
            RAISE EXCEPTION 'Incorrect owner for %', expected.signature;
        END IF;
        IF actual.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog, public, pg_temp'] THEN
            RAISE EXCEPTION 'Missing or incorrect search_path proconfig for %: got %',
                expected.signature, actual.proconfig;
        END IF;
    END LOOP;
END $$;

-- Verify exact trigger bindings for guard functions
DO $$ DECLARE expected record; BEGIN
    FOR expected IN SELECT * FROM (VALUES
      ('mqtt_rotation_replacements','enforce_mqtt_replacement_disposition','enforce_mqtt_replacement_disposition()',31,false),
      ('gateway_mqtt_credential_events','aa_mqtt_replacement_event','enforce_mqtt_replacement_event()',23,false),
      ('mqtt_credential_maintenance','aa_mqtt_superseded_checkpoint','enforce_mqtt_superseded_checkpoint()',27,false),
      ('mqtt_credential_recovery','aa_mqtt_replacement_no_startup_recovery','enforce_mqtt_replacement_no_startup_recovery()',23,false),
      ('mqtt_rotation_replacements','check_mqtt_replacement_disposition','check_mqtt_replacement_inventory()',5,true),
      ('gateway_mqtt_credential_events','check_mqtt_replacement_event','check_mqtt_replacement_inventory()',21,true),
      ('gateway_mqtt_credentials','check_mqtt_replacement_projection','check_mqtt_replacement_inventory()',21,true),
      ('mqtt_credential_maintenance','check_mqtt_replacement_checkpoint','check_mqtt_replacement_inventory()',29,true),
      ('gateway_mqtt_credential_events','enforce_gateway_mqtt_event_transition','enforce_gateway_mqtt_event_transition()',23,false),
      ('gateway_mqtt_credential_events','enforce_mqtt_maintenance_admission','enforce_mqtt_maintenance_admission()',7,false),
      ('gateway_mqtt_credential_events','create_mqtt_maintenance_intent','create_mqtt_maintenance_intent()',5,false),
      ('gateway_mqtt_credentials','enforce_mqtt_projection_transition','enforce_mqtt_projection_transition()',23,false),
      ('gateway_mqtt_credentials','enforce_mqtt_recovery_authority','enforce_mqtt_recovery_authority()',19,false),
      ('mqtt_credential_maintenance','enforce_mqtt_maintenance_transition','enforce_mqtt_maintenance_transition()',23,false),
      ('mqtt_credential_recovery','enforce_mqtt_recovery_transition','enforce_mqtt_recovery_transition()',23,false),
      ('mqtt_credential_recovery','check_mqtt_recovery_atomicity','check_mqtt_recovery_atomicity()',17,true),
      ('gateway_mqtt_credentials','check_mqtt_projection_authority','check_mqtt_projection_authority()',21,true),
      ('gateway_mqtt_credential_events','check_mqtt_event_authority','check_mqtt_event_authority()',21,true)
    ) AS e(relation,name,signature,kind,deferred) LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass('public.'||expected.relation)
           AND tgname=expected.name AND tgfoid=to_regprocedure('public.'||expected.signature)
           AND tgtype=expected.kind AND tgenabled='O' AND NOT tgisinternal
           AND tgdeferrable=expected.deferred AND tginitdeferred=expected.deferred
           AND (NOT expected.deferred OR tgconstraint<>0) AND tgqual IS NULL
           AND tgnargs=0 AND tgattr=''::int2vector) THEN
            RAISE EXCEPTION 'Missing/corrupt guard trigger binding: % on %', expected.name, expected.relation;
        END IF;
    END LOOP;
END $$;

-- App-role rollback probe: verify that hostile caller search_path is safely
-- superseded by function-local search_path and public truth is enforced.
CREATE FUNCTION pg_temp.must_reject(statement text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE statement;
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN check_violation OR unique_violation OR foreign_key_violation OR raise_exception THEN
        RETURN;
    END;
    RAISE EXCEPTION 'Hostile temp shadowing unexpectedly accepted: %', statement;
END $$;

DO $$ BEGIN
    INSERT INTO profiles(id, full_name) VALUES('18000000-0000-4000-8000-000000000001', 'SearchPath Probe');
    INSERT INTO platform_admins(user_id) VALUES('18000000-0000-4000-8000-000000000001');
    INSERT INTO gateways(gateway_id, name) VALUES('migration18_probe', 'Probe Gateway');
    SET LOCAL ROLE iot_backend_app;
    -- Create temporary table shadowing gateway_mqtt_credentials under caller path
    CREATE TEMP TABLE gateway_mqtt_credentials (LIKE public.gateway_mqtt_credentials INCLUDING ALL) ON COMMIT DROP;
    CREATE TEMP TABLE mqtt_rotation_replacements (LIKE public.mqtt_rotation_replacements INCLUDING ALL) ON COMMIT DROP;
    SET LOCAL search_path = pg_temp, public;
    -- Inserting invalid replacement into public must reject even when temp table exists
    PERFORM pg_temp.must_reject($q$
        INSERT INTO public.mqtt_rotation_replacements (predecessor_id, successor_id, gateway_id)
        VALUES ('18000000-0000-4000-8000-000000000002', '18000000-0000-4000-8000-000000000003', 'migration18_probe')
    $q$);
    RESET ROLE;
END $$;

SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
\echo 'Migration 000018 verification passed.'
