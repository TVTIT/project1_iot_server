\set ON_ERROR_STOP on
DO $$
DECLARE role_name text; table_name text;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=23 AND name='sensor_control_intent') THEN
        RAISE EXCEPTION 'Migration 23 exact terminal is missing';
    END IF;
    IF EXISTS(SELECT 1 FROM public.schema_migrations WHERE version>23) THEN
        RAISE EXCEPTION 'Unexpected future schema terminal';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM pg_index WHERE indexrelid='public.uq_twin_commands_active_sensor'::regclass AND indisunique AND indisvalid AND indpred IS NOT NULL)
       OR NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='public.twin_command_requests'::regclass AND contype='p')
       OR NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='public.twin_outbox'::regclass AND conname='uq_twin_outbox_command_id') THEN
        RAISE EXCEPTION 'Active Sensor/receipt/outbox DB protection missing';
    END IF;
    IF EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid IN('public.twin_commands'::regclass,'public.twin_states'::regclass,'public.twin_outbox'::regclass,'public.twin_command_requests'::regclass) AND NOT convalidated) THEN
        RAISE EXCEPTION 'Unvalidated control constraints';
    END IF;
    IF NOT has_table_privilege('iot_backend_app','public.twin_command_requests','SELECT')
       OR NOT has_table_privilege('iot_backend_app','public.twin_command_requests','INSERT')
       OR has_table_privilege('iot_backend_app','public.twin_command_requests','UPDATE')
       OR has_table_privilege('iot_backend_app','public.twin_command_requests','DELETE') THEN
        RAISE EXCEPTION 'Runtime receipt grants are not least privilege';
    END IF;
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
        FOREACH table_name IN ARRAY ARRAY['twin_states','twin_commands','twin_outbox','twin_command_requests'] LOOP
            IF has_table_privilege(role_name,'public.'||table_name,'INSERT')
               OR has_table_privilege(role_name,'public.'||table_name,'UPDATE')
               OR has_table_privilege(role_name,'public.'||table_name,'DELETE') THEN
                RAISE EXCEPTION 'Human direct-write privilege on % for %',table_name,role_name;
            END IF;
        END LOOP;
    END LOOP;
    IF EXISTS(SELECT 1 FROM public.twin_commands c JOIN public.twin_entities e ON e.id=c.entity_id WHERE c.target_entity_type<>e.entity_type) THEN
        RAISE EXCEPTION 'Command target type is inconsistent';
    END IF;
END $$;
\echo 'Migration 000023 verification passed.'
