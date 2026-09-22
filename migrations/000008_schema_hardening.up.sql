-- Enforce authorization, ingestion, command, outbox, and temporal invariants.
\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE twin_entities
    ALTER COLUMN gateway_id SET NOT NULL;

ALTER TABLE twin_entities
    ADD CONSTRAINT fk_twin_entities_gateway
    FOREIGN KEY (gateway_id)
    REFERENCES gateways(gateway_id)
    ON DELETE RESTRICT;

CREATE INDEX idx_twin_entities_gateway_type
    ON twin_entities (gateway_id, entity_type);

ALTER TABLE twin_states
    ADD CONSTRAINT fk_twin_states_last_desired_by
    FOREIGN KEY (last_desired_by)
    REFERENCES profiles(id)
    ON DELETE SET NULL;

ALTER TABLE processed_messages
    ADD CONSTRAINT fk_processed_messages_gateway
    FOREIGN KEY (gateway_id)
    REFERENCES gateways(gateway_id)
    ON DELETE RESTRICT;

ALTER TABLE telemetry
    ADD CONSTRAINT fk_telemetry_sensor
    FOREIGN KEY (gateway_id, sensor_id)
    REFERENCES sensors(gateway_id, sensor_id)
    ON DELETE RESTRICT;

ALTER TABLE user_gateways
    ADD CONSTRAINT chk_user_gateways_role
    CHECK (role IN ('owner', 'operator', 'viewer'));

ALTER TABLE twin_commands
    ADD CONSTRAINT chk_twin_commands_status
    CHECK (status IN (
        'pending',
        'published',
        'acknowledged',
        'succeeded',
        'failed',
        'timeout',
        'cancelled'
    ));

ALTER TABLE twin_outbox
    ADD CONSTRAINT chk_twin_outbox_status
    CHECK (status IN ('pending', 'published', 'failed'));

ALTER TABLE twin_temporal_values
    ADD CONSTRAINT chk_twin_temporal_exactly_one_value
    CHECK (num_nonnulls(value_number, value_text, value_boolean) = 1);

DROP INDEX IF EXISTS idx_twin_commands_idempotency;

CREATE UNIQUE INDEX idx_twin_commands_idempotency
    ON twin_commands (entity_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE twin_outbox
    ADD CONSTRAINT uq_twin_outbox_command_id
    UNIQUE (command_id);

-- Retention remains disabled until storage measurements justify a configured
-- policy. Compression policies are intentionally left unchanged.
SELECT remove_retention_policy('telemetry', if_exists => TRUE);
SELECT remove_retention_policy('twin_temporal_values', if_exists => TRUE);

COMMIT;
