\set ON_ERROR_STOP on
BEGIN;

-- 1. Migration tracking check
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 19 AND name = 'mqtt_simplified_journal') THEN
        RAISE EXCEPTION 'Migration 000019 is not recorded';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 18 AND name = 'mqtt_guard_search_path') THEN
        RAISE EXCEPTION 'Migration 000018 is required';
    END IF;
    IF (SELECT max(version) FROM public.schema_migrations) <> 19
       OR EXISTS (SELECT 1 FROM public.schema_migrations WHERE version BETWEEN 11 AND 19 AND
           name IS DISTINCT FROM CASE version
             WHEN 11 THEN 'mqtt_credential_operations'
             WHEN 12 THEN 'mqtt_credential_maintenance'
             WHEN 13 THEN 'mqtt_maintenance_admission'
             WHEN 14 THEN 'mqtt_credential_recovery'
             WHEN 15 THEN 'mqtt_recovery_authority_guards'
             WHEN 16 THEN 'mqtt_event_authority_guard'
             WHEN 17 THEN 'mqtt_rotation_replacement'
             WHEN 18 THEN 'mqtt_guard_search_path'
             WHEN 19 THEN 'mqtt_simplified_journal' END)
       OR (SELECT count(*) FROM public.schema_migrations WHERE version BETWEEN 11 AND 19) <> 9 THEN
        RAISE EXCEPTION 'Unknown or inconsistent credential migration tracking for schema 019';
    END IF;

    -- public schema must remain trusted
    IF has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
                  WHERE n.nspname = 'public' AND a.grantee = 0 AND a.privilege_type = 'CREATE') THEN
        RAISE EXCEPTION 'Guard search_path requires trusted public schema';
    END IF;
END $$;

-- 2. Verify table mqtt_reset_dispositions exists with correct structure
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'mqtt_reset_dispositions'
    ) THEN
        RAISE EXCEPTION 'Missing table mqtt_reset_dispositions';
    END IF;
END $$;

-- 3. Verify columns and constraints on gateway_mqtt_credential_events
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'gateway_mqtt_credential_events' AND column_name = 'execution_contract'
          AND is_nullable = 'NO' AND column_default = '''lifecycle''::text'
    ) THEN
        RAISE EXCEPTION 'Missing or incorrect execution_contract column on gateway_mqtt_credential_events';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'gateway_mqtt_credential_events' AND column_name = 'reset_predecessor_id'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'gateway_mqtt_credential_events' AND column_name = 'reset_reason'
    ) THEN
        RAISE EXCEPTION 'Missing reset columns on gateway_mqtt_credential_events';
    END IF;

    -- Verify execution_contract value constraint
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.gateway_mqtt_credential_events'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%execution_contract = ANY (%''lifecycle''%''simplified''%)%'
    ) THEN
        RAISE EXCEPTION 'Missing or incorrect execution_contract check constraint';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.gateway_mqtt_credential_events'::regclass AND conname = 'chk_mqtt_reset_identity'
    ) THEN
        RAISE EXCEPTION 'Missing constraint chk_mqtt_reset_identity';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.gateway_mqtt_credential_events'::regclass AND conname = 'chk_mqtt_operation_contract'
    ) THEN
        RAISE EXCEPTION 'Missing constraint chk_mqtt_operation_contract';
    END IF;
END $$;

-- 4. Verify function search_paths and security definer
DO $$ DECLARE
    expected record;
    actual record;
    fdef text;
BEGIN
    FOR expected IN SELECT * FROM (VALUES
        ('lock_mqtt_reset_admin(uuid)', true),
        ('enforce_mqtt_reset_disposition()', false),
        ('enforce_mqtt_reset_event()', false),
        ('validate_mqtt_projection_authority(text)', false),
        ('check_mqtt_replacement_inventory()', false),
        ('validate_mqtt_replacement_inventory(text)', false),
        ('mqtt_operation_effective(uuid)', false),
        ('create_mqtt_maintenance_intent()', false),
        ('enforce_mqtt_maintenance_transition()', false),
        ('enforce_mqtt_recovery_transition()', false)
    ) AS e(signature, definer) LOOP
        SELECT prosecdef, proconfig, proowner INTO actual
          FROM pg_proc WHERE oid = to_regprocedure('public.' || expected.signature);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Missing function: %', expected.signature;
        END IF;
        IF actual.prosecdef <> expected.definer THEN
            RAISE EXCEPTION 'Incorrect security definer setting for %: expected %, got %',
                expected.signature, expected.definer, actual.prosecdef;
        END IF;
        IF actual.proconfig IS NULL OR NOT (actual.proconfig @> ARRAY['search_path=pg_catalog, public, pg_temp']) THEN
            RAISE EXCEPTION 'Missing or incorrect search_path proconfig for %: got %',
                expected.signature, actual.proconfig;
        END IF;
    END LOOP;

    -- Minimal catalog assertion: validate_mqtt_replacement_inventory must retain full structural rotation predicates
    SELECT pg_get_functiondef('public.validate_mqtt_replacement_inventory(text)'::regprocedure) INTO fdef;
    IF fdef NOT LIKE '%LEFT JOIN public.gateway_mqtt_credential_events p ON p.operation_id = d.predecessor_id%'
       OR fdef NOT LIKE '%LEFT JOIN public.gateway_mqtt_credential_events s ON s.operation_id = d.successor_id%'
       OR fdef NOT LIKE '%LEFT JOIN public.mqtt_credential_maintenance pm ON pm.operation_id = p.operation_id%'
       OR fdef NOT LIKE '%LEFT JOIN public.mqtt_credential_maintenance sm ON sm.operation_id = s.operation_id%'
       OR fdef NOT LIKE '%p.legacy_projection OR s.legacy_projection OR p.action <> ''rotate'' OR s.action <> ''rotate''%'
       OR fdef NOT LIKE '%s.replacement_predecessor_id IS DISTINCT FROM p.operation_id OR s.credential_version <= p.credential_version%'
       OR fdef NOT LIKE '%EXISTS (SELECT 1 FROM public.mqtt_credential_recovery WHERE operation_id IN (p.operation_id, s.operation_id))%'
       OR fdef NOT LIKE '%Replacement intent has no disposition%'
       OR fdef NOT LIKE '%Dangling reset disposition%' THEN
        RAISE EXCEPTION 'validate_mqtt_replacement_inventory missing restored 000017 lifecycle rotation predicates';
    END IF;

    -- Minimal catalog assertion: enforce_mqtt_reset_event must enforce reset generation exceeding current and max attempted history under gateway lock
    SELECT pg_get_functiondef('public.enforce_mqtt_reset_event()'::regprocedure) INTO fdef;
    IF fdef NOT LIKE '%PERFORM 1 FROM public.gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE%'
       OR fdef NOT LIKE '%NEW.credential_version <= GREATEST%'
       OR fdef NOT LIKE '%COALESCE((SELECT credential_version FROM public.gateway_mqtt_credentials WHERE gateway_id = NEW.gateway_id), 0)%'
       OR fdef NOT LIKE '%COALESCE((SELECT max(credential_version) FROM public.gateway_mqtt_credential_events WHERE gateway_id = NEW.gateway_id), 0)%'
       OR fdef NOT LIKE '%Reset credential generation must exceed current and attempted history%' THEN
        RAISE EXCEPTION 'enforce_mqtt_reset_event missing reset generation history lock guard';
    END IF;
END $$;

-- 5. Verify trigger bindings for redefined guard functions
DO $$ DECLARE expected record; BEGIN
    FOR expected IN SELECT * FROM (VALUES
        ('gateway_mqtt_credential_events', 'create_mqtt_maintenance_intent', 'create_mqtt_maintenance_intent()', 5, false),
        ('mqtt_credential_maintenance', 'enforce_mqtt_maintenance_transition', 'enforce_mqtt_maintenance_transition()', 23, false),
        ('mqtt_credential_recovery', 'enforce_mqtt_recovery_transition', 'enforce_mqtt_recovery_transition()', 23, false),
        ('mqtt_reset_dispositions', 'enforce_mqtt_reset_disposition', 'enforce_mqtt_reset_disposition()', 31, false),
        ('gateway_mqtt_credential_events', 'aa_mqtt_reset_event', 'enforce_mqtt_reset_event()', 23, false),
        ('mqtt_reset_dispositions', 'check_mqtt_reset_disposition', 'check_mqtt_replacement_inventory()', 5, true)
    ) AS e(relation, name, signature, kind, deferred) LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass('public.' || expected.relation)
           AND tgname = expected.name AND tgfoid = to_regprocedure('public.' || expected.signature)
           AND tgtype = expected.kind AND tgenabled = 'O' AND NOT tgisinternal
           AND tgdeferrable = expected.deferred AND tginitdeferred = expected.deferred
           AND (NOT expected.deferred OR tgconstraint <> 0) AND tgqual IS NULL
           AND tgnargs = 0 AND tgattr = ''::int2vector) THEN
            RAISE EXCEPTION 'Missing/corrupt guard trigger binding: % on %', expected.name, expected.relation;
        END IF;
    END LOOP;
END $$;

-- 6. Verify privileges
DO $$ BEGIN
    IF NOT has_table_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'SELECT')
       OR NOT has_column_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'predecessor_id', 'INSERT')
       OR NOT has_column_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'successor_id', 'INSERT')
       OR NOT has_column_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'gateway_id', 'INSERT')
       OR NOT has_column_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'reason', 'INSERT')
       OR has_column_privilege('iot_backend_app', 'public.mqtt_reset_dispositions', 'disposition', 'INSERT')
       OR has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'reset_predecessor_id', 'UPDATE')
       OR NOT has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'reset_predecessor_id', 'INSERT')
       OR NOT has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'reset_reason', 'INSERT')
       OR has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'reset_reason', 'UPDATE')
       OR NOT has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'execution_contract', 'INSERT')
       OR has_column_privilege('iot_backend_app', 'public.gateway_mqtt_credential_events', 'execution_contract', 'UPDATE')
       OR NOT has_function_privilege('iot_backend_app', 'public.lock_mqtt_reset_admin(uuid)', 'EXECUTE')
       OR has_function_privilege('iot_backend_app', 'public.enforce_mqtt_reset_disposition()', 'EXECUTE')
       OR has_function_privilege('iot_backend_app', 'public.enforce_mqtt_reset_event()', 'EXECUTE')
       OR has_function_privilege('iot_backend_app', 'public.create_mqtt_maintenance_intent()', 'EXECUTE')
       OR has_function_privilege('iot_backend_app', 'public.enforce_mqtt_maintenance_transition()', 'EXECUTE')
       OR EXISTS (
           SELECT 1 FROM pg_roles WHERE rolname IN ('anon', 'authenticated')
           AND has_table_privilege(rolname, 'public.mqtt_reset_dispositions', 'SELECT,INSERT,UPDATE,DELETE')
       ) THEN
        RAISE EXCEPTION 'Invalid reset authority grants';
    END IF;
END $$;

ROLLBACK;
\echo 'Migration 000019 verification passed.'
