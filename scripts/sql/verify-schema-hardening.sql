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
    max_wait_seconds INTEGER := 60;
    waited_seconds INTEGER := 0;
    gateway_column_ready BOOLEAN := FALSE;
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'twin_entities'
              AND column_name = 'gateway_id'
              AND data_type = 'text'
              AND is_nullable = 'NO'
        )
        INTO gateway_column_ready;

        EXIT WHEN gateway_column_ready OR waited_seconds >= max_wait_seconds;

        PERFORM pg_sleep(1);
        waited_seconds := waited_seconds + 1;
    END LOOP;

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

    SELECT count(*)
    INTO constraint_count
    FROM pg_constraint
    WHERE conname = ANY (ARRAY[
        'fk_twin_entities_gateway',
        'fk_twin_states_last_desired_by',
        'fk_processed_messages_gateway',
        'fk_telemetry_sensor',
        'chk_user_gateways_role',
        'chk_twin_commands_status',
        'chk_twin_outbox_status',
        'chk_twin_temporal_exactly_one_value',
        'uq_twin_outbox_command_id'
    ]);

    IF constraint_count <> 9 THEN
        RAISE EXCEPTION 'Verification failed: expected 9 hardening constraints, found %', constraint_count;
    END IF;

    SELECT indexdef
    INTO index_definition
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'idx_twin_commands_idempotency';

    IF index_definition IS NULL OR position('UNIQUE INDEX' IN index_definition) = 0 THEN
        RAISE EXCEPTION 'Verification failed: command idempotency index is not unique';
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
    test_gateway_id := '__schema_hardening_test_' || replace(gen_random_uuid()::text, '-', '');

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
        INSERT INTO processed_messages (gateway_id, message_id, payload_hash)
        VALUES ('__missing_gateway', gen_random_uuid(), '__constraint_test');
        RAISE EXCEPTION 'Verification failed: processed message for an unknown Gateway was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO telemetry (
            measured_at, gateway_id, sensor_id, sequence_num, value
        ) VALUES (
            now(), test_gateway_id, '__missing_sensor', 1, 1
        );
        RAISE EXCEPTION 'Verification failed: telemetry for an unknown Sensor was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

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
        RAISE EXCEPTION 'Verification failed: duplicate idempotency key was accepted';
    EXCEPTION
        WHEN unique_violation THEN NULL;
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
