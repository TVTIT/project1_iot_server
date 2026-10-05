\set ON_ERROR_STOP on
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM fixture15_authority f LEFT JOIN gateway_mqtt_credentials c USING(gateway_id) WHERE f.original IS DISTINCT FROM to_jsonb(c))
       OR EXISTS (SELECT 1 FROM fixture15_events f LEFT JOIN gateway_mqtt_credential_events e USING(operation_id) WHERE f.original IS DISTINCT FROM to_jsonb(e))
       OR EXISTS (SELECT 1 FROM fixture15_recovery f LEFT JOIN mqtt_credential_recovery r USING(recovery_id) WHERE f.original IS DISTINCT FROM to_jsonb(r)) THEN
        RAISE EXCEPTION 'Authority upgrade changed historical rows';
    END IF;
END $$;
BEGIN;
SET LOCAL ROLE iot_backend_app;
DO $$ DECLARE g text; BEGIN
    FOR g IN SELECT c.gateway_id FROM gateway_mqtt_credentials c LEFT JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id
        WHERE c.last_operation_id IS NULL OR e.legacy_projection LOOP
        BEGIN
            UPDATE gateway_mqtt_credentials SET credential_version=credential_version+1 WHERE gateway_id=g;
            RAISE EXCEPTION 'Legacy projection mutation accepted';
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM='Legacy projection mutation accepted' THEN RAISE; END IF;
        END;
    END LOOP;
END $$;
ROLLBACK;
