\set ON_ERROR_STOP on

-- Run before 000006-000008 on an existing database. The script must finish
-- without an exception before the hardening migrations are applied.
BEGIN TRANSACTION READ ONLY;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM processed_messages AS message
        LEFT JOIN gateways AS gateway USING (gateway_id)
        WHERE gateway.gateway_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Preflight failed: processed_messages contains unknown Gateways';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM telemetry AS sample
        LEFT JOIN sensors AS sensor
          ON sensor.gateway_id = sample.gateway_id
         AND sensor.sensor_id = sample.sensor_id
        WHERE sensor.sensor_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Preflight failed: telemetry contains unknown Gateway/Sensor pairs';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities AS entity
        LEFT JOIN gateways AS gateway
          ON entity.entity_id = 'urn:ngsi-ld:Gateway:' || gateway.gateway_id
        WHERE entity.entity_type = 'Gateway'
          AND gateway.gateway_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a Gateway twin cannot be mapped to gateways';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE entity_type NOT IN ('Gateway', 'Sensor')
    ) THEN
        RAISE EXCEPTION 'Preflight failed: an entity type needs an explicit Gateway ownership mapping';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities AS child
        WHERE child.entity_type = 'Sensor'
          AND NOT EXISTS (
              SELECT 1
              FROM twin_relationships AS relationship
              JOIN twin_entities AS parent
                ON parent.id = relationship.source_entity_id
              WHERE relationship.target_entity_id = child.id
                AND relationship.relationship_type = 'hasSensor'
                AND parent.entity_type = 'Gateway'
          )
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a Sensor twin has no Gateway hasSensor parent';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities AS child
        JOIN twin_relationships AS relationship
          ON relationship.target_entity_id = child.id
        JOIN twin_entities AS parent
          ON parent.id = relationship.source_entity_id
        WHERE child.entity_type = 'Sensor'
          AND parent.entity_type = 'Gateway'
          AND relationship.relationship_type = 'hasSensor'
        GROUP BY child.id
        HAVING count(DISTINCT parent.id) > 1
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a Sensor twin has multiple Gateway hasSensor parents';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM user_gateways
        WHERE role NOT IN ('owner', 'operator', 'viewer')
    ) THEN
        RAISE EXCEPTION 'Preflight failed: user_gateways contains an unsupported role';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_commands
        WHERE status NOT IN (
            'pending', 'published', 'acknowledged', 'succeeded',
            'failed', 'timeout', 'cancelled'
        )
    ) THEN
        RAISE EXCEPTION 'Preflight failed: twin_commands contains an unsupported status';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_outbox
        WHERE status NOT IN ('pending', 'published', 'failed')
    ) THEN
        RAISE EXCEPTION 'Preflight failed: twin_outbox contains an unsupported status';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_temporal_values
        WHERE num_nonnulls(value_number, value_text, value_boolean) <> 1
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a temporal row does not contain exactly one typed value';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_commands
        WHERE idempotency_key IS NOT NULL
        GROUP BY entity_id, idempotency_key
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Preflight failed: duplicate command idempotency keys exist';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_outbox
        GROUP BY command_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a command has multiple outbox rows';
    END IF;
END
$$;

ROLLBACK;

\echo 'Schema hardening preflight passed.'
