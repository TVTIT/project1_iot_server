\set ON_ERROR_STOP on

-- Development/emergency rollback for migrations 000006-000008. Prefer a new
-- forward migration after deployment because backend code may depend on these
-- columns and constraints.
BEGIN;

ALTER TABLE twin_outbox
    DROP CONSTRAINT IF EXISTS uq_twin_outbox_command_id;

DROP INDEX IF EXISTS idx_twin_commands_idempotency;

CREATE INDEX idx_twin_commands_idempotency
    ON twin_commands (entity_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE twin_temporal_values
    DROP CONSTRAINT IF EXISTS chk_twin_temporal_exactly_one_value;

ALTER TABLE twin_outbox
    DROP CONSTRAINT IF EXISTS chk_twin_outbox_status;

ALTER TABLE twin_commands
    DROP CONSTRAINT IF EXISTS chk_twin_commands_status;

ALTER TABLE user_gateways
    DROP CONSTRAINT IF EXISTS chk_user_gateways_role;

ALTER TABLE telemetry
    DROP CONSTRAINT IF EXISTS fk_telemetry_sensor;

ALTER TABLE processed_messages
    DROP CONSTRAINT IF EXISTS fk_processed_messages_gateway;

ALTER TABLE twin_states
    DROP CONSTRAINT IF EXISTS fk_twin_states_last_desired_by;

DROP INDEX IF EXISTS idx_twin_entities_gateway_type;

ALTER TABLE twin_entities
    DROP CONSTRAINT IF EXISTS fk_twin_entities_gateway;

ALTER TABLE twin_states
    DROP COLUMN IF EXISTS last_desired_by;

ALTER TABLE twin_entities
    DROP COLUMN IF EXISTS gateway_id;

COMMIT;

-- Retention is intentionally not restored. Re-enable it only through a new,
-- reviewed policy after storage usage has been measured.
