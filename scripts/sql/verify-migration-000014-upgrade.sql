\set ON_ERROR_STOP on
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM fixture14_events f LEFT JOIN gateway_mqtt_credential_events e USING(operation_id)
        WHERE (to_jsonb(e)-'recovery_id') IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture14_maintenance f LEFT JOIN mqtt_credential_maintenance p USING(operation_id)
        WHERE (to_jsonb(p)-'recovery_id') IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture14_authority f LEFT JOIN gateway_mqtt_credentials c USING(gateway_id)
        WHERE to_jsonb(c) IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM mqtt_credential_recovery) THEN
        RAISE EXCEPTION 'v13 upgrade changed historical evidence or fabricated recovery';
    END IF;
END $$;
