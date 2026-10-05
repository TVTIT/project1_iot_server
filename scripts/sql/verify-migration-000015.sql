\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=15 AND name='mqtt_recovery_authority_guards') THEN RAISE EXCEPTION 'Missing authority migration'; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='check_mqtt_projection_authority' AND tgdeferrable AND tginitdeferred AND tgenabled='O')
       OR NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='enforce_mqtt_projection_transition' AND tgenabled='O') THEN RAISE EXCEPTION 'Missing authority guards'; END IF;
END $$;
BEGIN;
INSERT INTO gateways(gateway_id,name) VALUES('fixture15_new','Projection denial');
SET LOCAL ROLE iot_backend_app;
DO $$ BEGIN
    BEGIN
        INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,activated_at) VALUES('fixture15_new',1,'active',now());
        RAISE EXCEPTION 'Uncorrelated projection was accepted';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM='Uncorrelated projection was accepted' THEN RAISE; END IF;
    END;
END $$;
ROLLBACK;
