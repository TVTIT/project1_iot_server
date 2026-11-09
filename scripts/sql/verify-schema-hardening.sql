\set ON_ERROR_STOP on

-- Verify schema metadata and exercise the new constraints without preserving
-- any test rows.
BEGIN;

DO $$
DECLARE
    constraint_count INTEGER;
    retention_count INTEGER;
    compression_count INTEGER;
    index_definition TEXT;
    gateway_entity UUID;
    test_gateway_id TEXT;
    test_command UUID;
    actor_scoped_receipts BOOLEAN := false;
    telemetry_sensor_fk_dropped BOOLEAN := false;
BEGIN
    IF to_regclass('public.schema_migrations') IS NOT NULL THEN
        SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=23 AND name='sensor_control_intent') INTO actor_scoped_receipts;
        SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=25 AND name='drop_telemetry_sensor_fk') INTO telemetry_sensor_fk_dropped;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'twin_entities'
          AND column_name = 'gateway_id'
          AND data_type = 'text'
          AND is_nullable = 'NO'
    ) THEN
        RAISE EXCEPTION 'Verification failed: twin_entities.gateway_id is not TEXT NOT NULL';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'twin_states'
          AND column_name = 'last_desired_by'
          AND data_type = 'uuid'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'Verification failed: twin_states.last_desired_by is not nullable UUID';
    END IF;

    IF telemetry_sensor_fk_dropped THEN
        SELECT count(*)
        INTO constraint_count
        FROM (VALUES
            ('public.twin_entities', 'fk_twin_entities_gateway'),
            ('public.twin_states', 'fk_twin_states_last_desired_by'),
            ('public.processed_messages', 'fk_processed_messages_gateway'),
            ('public.user_gateways', 'chk_user_gateways_role'),
            ('public.twin_commands', 'chk_twin_commands_status'),
            ('public.twin_outbox', 'chk_twin_outbox_status'),
            ('public.twin_temporal_values', 'chk_twin_temporal_exactly_one_value'),
            ('public.twin_outbox', 'uq_twin_outbox_command_id')
        ) AS required(parent_table, constraint_name)
        JOIN pg_constraint c ON c.conrelid = to_regclass(required.parent_table)
                            AND c.conname = required.constraint_name;

        IF constraint_count <> 8 THEN
            RAISE EXCEPTION 'Verification failed: expected 8 hardening constraints with migration 25, found %', constraint_count;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'public.sensors'::regclass
              AND tgname = 'trg_sensors_restrict_telemetry'
        ) THEN
            RAISE EXCEPTION 'Verification failed: trg_sensors_restrict_telemetry trigger is missing on sensors';
        END IF;
    ELSE
        SELECT count(*)
        INTO constraint_count
        FROM (VALUES
            ('public.twin_entities', 'fk_twin_entities_gateway'),
            ('public.twin_states', 'fk_twin_states_last_desired_by'),
            ('public.processed_messages', 'fk_processed_messages_gateway'),
            ('public.telemetry', 'fk_telemetry_sensor'),
            ('public.user_gateways', 'chk_user_gateways_role'),
            ('public.twin_commands', 'chk_twin_commands_status'),
            ('public.twin_outbox', 'chk_twin_outbox_status'),
            ('public.twin_temporal_values', 'chk_twin_temporal_exactly_one_value'),
            ('public.twin_outbox', 'uq_twin_outbox_command_id')
        ) AS required(parent_table, constraint_name)
        JOIN pg_constraint c ON c.conrelid = to_regclass(required.parent_table)
                            AND c.conname = required.constraint_name;

        IF constraint_count <> 9 THEN
            RAISE EXCEPTION 'Verification failed: expected 9 hardening constraints, found %', constraint_count;
        END IF;
    END IF;

    SELECT indexdef
    INTO index_definition
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_twin_commands_idempotency';

    IF index_definition IS NULL OR (position('UNIQUE INDEX' IN index_definition)>0)=actor_scoped_receipts THEN
        RAISE EXCEPTION 'Verification failed: command idempotency index uniqueness does not match schema generation';
    END IF;
    IF actor_scoped_receipts AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('public.twin_command_requests') AND contype='p') THEN
        RAISE EXCEPTION 'Verification failed: actor-scoped receipt uniqueness is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_twin_entities_gateway_type'
    ) THEN
        RAISE EXCEPTION 'Verification failed: twin ownership lookup index is missing';
    END IF;

    IF EXISTS (SELECT 1 FROM twin_entities WHERE gateway_id IS NULL) THEN
        RAISE EXCEPTION 'Verification failed: a Digital Twin has no owning Gateway';
    END IF;

    SELECT count(*)
    INTO retention_count
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention'
      AND hypertable_name IN ('telemetry', 'twin_temporal_values');

    IF retention_count <> 0 THEN
        RAISE EXCEPTION 'Verification failed: retention policies still exist';
    END IF;

    SELECT count(*)
    INTO compression_count
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_name IN ('telemetry', 'twin_temporal_values');

    IF compression_count <> 2 THEN
        RAISE EXCEPTION 'Verification failed: expected 2 compression policies, found %', compression_count;
    END IF;

    gateway_entity := gen_random_uuid();
    test_gateway_id := 'schema_hardening_test_' || replace(gen_random_uuid()::text, '-', '');

    INSERT INTO gateways (gateway_id, name)
    VALUES (test_gateway_id, 'Schema hardening verification Gateway');

    INSERT INTO twin_entities (
        id, entity_id, entity_type, name, gateway_id
    ) VALUES (
        gateway_entity,
        'urn:ngsi-ld:Gateway:' || test_gateway_id,
        'Gateway',
        'Schema hardening verification Gateway',
        test_gateway_id
    );

    INSERT INTO twin_states (entity_id)
    VALUES (gateway_entity);

    BEGIN
        UPDATE twin_states
        SET last_desired_by = gen_random_uuid()
        WHERE entity_id = gateway_entity;
        RAISE EXCEPTION 'Verification failed: unknown desired-state actor was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'processed_messages'
              AND column_name = 'source_topic'
        ) THEN
            EXECUTE 'INSERT INTO processed_messages
                (gateway_id, message_id, payload_hash, source_topic, sensor_id, boot_id,
                 first_sequence, last_sequence, sample_count)
                VALUES (''__missing_gateway'', gen_random_uuid(), ''__constraint_test'',
                        ''gateways/__missing_gateway/telemetry/__constraint_sensor'',
                        ''__constraint_sensor'', gen_random_uuid(), 0, 0, 1)';
        ELSE
            EXECUTE 'INSERT INTO processed_messages (gateway_id, message_id, payload_hash)
                VALUES (''__missing_gateway'', gen_random_uuid(), ''__constraint_test'')';
        END IF;
        RAISE EXCEPTION 'Verification failed: processed message for an unknown Gateway was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    IF NOT telemetry_sensor_fk_dropped THEN
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'telemetry'
                  AND column_name = 'sample_index'
            ) THEN
                EXECUTE 'INSERT INTO telemetry
                    (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
                    VALUES (now(), $1, ''__missing_sensor'', 1, 1, gen_random_uuid(), gen_random_uuid(), 0)'
                    USING test_gateway_id;
            ELSE
                EXECUTE 'INSERT INTO telemetry
                    (measured_at, gateway_id, sensor_id, sequence_num, value)
                    VALUES (now(), $1, ''__missing_sensor'', 1, 1)'
                    USING test_gateway_id;
            END IF;
            RAISE EXCEPTION 'Verification failed: telemetry for an unknown Sensor was accepted';
        EXCEPTION
            WHEN foreign_key_violation THEN NULL;
        END;
    ELSE
        -- Verification of reverse RESTRICT trigger on sensors under migration 25:
        -- Deleting a sensor that has telemetry must raise foreign_key_violation
        INSERT INTO sensors (gateway_id, sensor_id, name)
        VALUES (test_gateway_id, '__test_restrict_sensor', 'Restrict Test Sensor');

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'telemetry'
              AND column_name = 'sample_index'
        ) THEN
            EXECUTE 'INSERT INTO telemetry
                (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
                VALUES (now(), $1, ''__test_restrict_sensor'', 1, 1, gen_random_uuid(), gen_random_uuid(), 0)'
                USING test_gateway_id;
        ELSE
            EXECUTE 'INSERT INTO telemetry
                (measured_at, gateway_id, sensor_id, sequence_num, value)
                VALUES (now(), $1, ''__test_restrict_sensor'', 1, 1)'
                USING test_gateway_id;
        END IF;

        BEGIN
            DELETE FROM sensors WHERE gateway_id = test_gateway_id AND sensor_id = '__test_restrict_sensor';
            RAISE EXCEPTION 'Verification failed: deletion of sensor with telemetry was accepted';
        EXCEPTION
            WHEN foreign_key_violation THEN NULL;
        END;

        DELETE FROM telemetry WHERE gateway_id = test_gateway_id AND sensor_id = '__test_restrict_sensor';
        DELETE FROM sensors WHERE gateway_id = test_gateway_id AND sensor_id = '__test_restrict_sensor';
    END IF;

    BEGIN
        INSERT INTO user_gateways (user_id, gateway_id, role)
        VALUES (gen_random_uuid(), test_gateway_id, '__invalid_role');
        RAISE EXCEPTION 'Verification failed: invalid User-Gateway role was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO twin_temporal_values (
            observed_at, entity_id, property_name
        ) VALUES (
            now(), gateway_entity, '__constraint_test_no_value'
        );
        RAISE EXCEPTION 'Verification failed: temporal row without a value was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO twin_temporal_values (
            observed_at, entity_id, property_name, value_number, value_text
        ) VALUES (
            now(), gateway_entity, '__constraint_test_two_values', 1, 'one'
        );
        RAISE EXCEPTION 'Verification failed: temporal row with two values was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    INSERT INTO twin_temporal_values (
        observed_at, entity_id, property_name, value_boolean
    ) VALUES (
        now(), gateway_entity, '__constraint_test_valid_value', TRUE
    );

    test_command := gen_random_uuid();
    INSERT INTO twin_commands (
        command_id, entity_id, action, status, idempotency_key, expires_at
    ) VALUES (
        test_command,
        gateway_entity,
        '__constraint_test',
        'pending',
        '__constraint_test_key',
        now() + INTERVAL '1 hour'
    );

    BEGIN
        INSERT INTO twin_commands (
            command_id, entity_id, action, status, idempotency_key, expires_at
        ) VALUES (
            gen_random_uuid(),
            gateway_entity,
            '__constraint_test',
            'pending',
            '__constraint_test_key',
            now() + INTERVAL '1 hour'
        );
        IF NOT actor_scoped_receipts THEN
            RAISE EXCEPTION 'Verification failed: duplicate idempotency key was accepted';
        END IF;
    EXCEPTION
        WHEN unique_violation THEN
            IF actor_scoped_receipts THEN RAISE EXCEPTION 'Verification failed: legacy entity/key uniqueness still rejects actor-scoped requests'; END IF;
    END;

    INSERT INTO twin_outbox (command_id, topic, payload, status)
    VALUES (test_command, '__constraint_test', '{}'::jsonb, 'pending');

    BEGIN
        UPDATE twin_outbox
        SET status = '__invalid_status'
        WHERE command_id = test_command;
        RAISE EXCEPTION 'Verification failed: invalid outbox status was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO twin_outbox (command_id, topic, payload, status)
        VALUES (test_command, '__constraint_test', '{}'::jsonb, 'pending');
        RAISE EXCEPTION 'Verification failed: duplicate outbox row was accepted';
    EXCEPTION
        WHEN unique_violation THEN NULL;
    END;

    BEGIN
        UPDATE twin_commands
        SET status = '__invalid_status'
        WHERE command_id = test_command;
        RAISE EXCEPTION 'Verification failed: invalid command status was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        DELETE FROM gateways
        WHERE gateway_id = test_gateway_id;
        RAISE EXCEPTION 'Verification failed: deleting a referenced Gateway was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;
END
$$;

ROLLBACK;

\echo 'Schema hardening verification passed.'
