-- Admission fencing only. Existing contradictory history requires operator recovery.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=13) AS apply_admission \gset
\if :apply_admission
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=12 AND name='mqtt_credential_maintenance') THEN
        RAISE EXCEPTION 'Migration 000012 is required';
    END IF;
END $$;
CREATE FUNCTION enforce_mqtt_maintenance_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Same order as repository admission/completion: Gateway before checkpoint.
    -- v11 still validates live intent/identity and legacy unresolved projections.
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE gateway_id=NEW.gateway_id AND status <> 'completed') THEN
        RAISE EXCEPTION 'Gateway has unresolved credential maintenance';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_maintenance_admission BEFORE INSERT ON gateway_mqtt_credential_events
    FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_maintenance_admission();
INSERT INTO schema_migrations(version,name) VALUES(13,'mqtt_maintenance_admission');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=13 AND name='mqtt_maintenance_admission') THEN
        RAISE EXCEPTION 'Unexpected migration version 13';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
