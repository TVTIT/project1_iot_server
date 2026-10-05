-- Non-secret business checkpoints; never broker authentication or delivery proof.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 12) AS apply_maintenance \gset
\if :apply_maintenance
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=11 AND name='mqtt_credential_operations') THEN
        RAISE EXCEPTION 'Migration 000011 is required';
    END IF;
END $$;
CREATE TABLE mqtt_credential_maintenance (
    operation_id UUID PRIMARY KEY REFERENCES gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT,
    gateway_id TEXT NOT NULL REFERENCES gateways(gateway_id) ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress','completed','recovery_needed')),
    broker_epoch TEXT CHECK (broker_epoch IS NULL OR broker_epoch ~ '^[0-9a-f]{64}$'),
    error_code TEXT CHECK (error_code IS NULL OR error_code IN ('credential_recovery_required','service_unavailable','credential_finalization_pending','credential_verification_unavailable','internal_error','credential_runtime_busy','credential_runtime_disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (updated_at >= created_at),
    CHECK ((status='completed' AND completed_at IS NOT NULL AND broker_epoch IS NOT NULL AND error_code IS NULL AND completed_at >= created_at AND updated_at >= completed_at)
        OR (status IN ('in_progress','recovery_needed') AND completed_at IS NULL))
);
CREATE INDEX idx_mqtt_maintenance_unresolved ON mqtt_credential_maintenance(operation_id) WHERE status <> 'completed';
-- Only unresolved modern v11 intents are backfilled. Legacy projections and
-- historical succeeded/failed events have unknown maintenance, NOT invented proof.
INSERT INTO mqtt_credential_maintenance(operation_id,gateway_id,status,created_at,updated_at)
SELECT operation_id,gateway_id,'recovery_needed',created_at,GREATEST(created_at,now())
FROM gateway_mqtt_credential_events WHERE NOT legacy_projection AND status IN ('pending','recovery_needed');

CREATE FUNCTION enforce_mqtt_maintenance_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.status <> 'in_progress' OR NEW.broker_epoch IS NOT NULL OR NEW.error_code IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE operation_id=NEW.operation_id AND gateway_id=NEW.gateway_id AND NOT legacy_projection AND status='pending') THEN
            RAISE EXCEPTION 'Maintenance requires matching modern intent';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status='completed' THEN RAISE EXCEPTION 'Completed maintenance is immutable'; END IF;
    IF NEW.operation_id <> OLD.operation_id OR NEW.gateway_id <> OLD.gateway_id OR NEW.created_at <> OLD.created_at OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'Maintenance identity is immutable';
    END IF;
    IF NEW.status='completed' AND NOT EXISTS (
        SELECT 1 FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id
        WHERE c.gateway_id=NEW.gateway_id AND c.last_operation_id=NEW.operation_id AND e.status IN ('succeeded','failed')
    ) THEN RAISE EXCEPTION 'Maintenance completion requires current terminal operation'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_maintenance_transition BEFORE INSERT OR UPDATE ON mqtt_credential_maintenance FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_maintenance_transition();

-- Trigger makes every new modern intent + checkpoint atomic, including direct
-- app-role SQL writers. No network work and no cross-Gateway execution mutex.
CREATE FUNCTION create_mqtt_maintenance_intent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO mqtt_credential_maintenance(operation_id,gateway_id) VALUES(NEW.operation_id,NEW.gateway_id);
    RETURN NEW;
END $$;
CREATE TRIGGER create_mqtt_maintenance_intent AFTER INSERT ON gateway_mqtt_credential_events FOR EACH ROW EXECUTE FUNCTION create_mqtt_maintenance_intent();
GRANT SELECT ON mqtt_credential_maintenance TO iot_backend;
GRANT INSERT (operation_id,gateway_id) ON mqtt_credential_maintenance TO iot_backend;
GRANT UPDATE (status,broker_epoch,error_code,updated_at,completed_at) ON mqtt_credential_maintenance TO iot_backend;
INSERT INTO schema_migrations(version,name) VALUES(12,'mqtt_credential_maintenance');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=12 AND name='mqtt_credential_maintenance') THEN RAISE EXCEPTION 'Unexpected migration version 12'; END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
