\set ON_ERROR_STOP on
DO $$ BEGIN
    IF (SELECT count(*) FROM fixture13_before) <> 2 OR EXISTS (
        SELECT 1 FROM fixture13_before b
        LEFT JOIN gateway_mqtt_credential_events e USING(operation_id)
        LEFT JOIN mqtt_credential_maintenance p USING(operation_id)
        LEFT JOIN gateway_mqtt_credentials c ON c.gateway_id=e.gateway_id
        WHERE b.event IS DISTINCT FROM (to_jsonb(e)-'recovery_id') OR b.checkpoint IS DISTINCT FROM (to_jsonb(p)-'recovery_id') OR b.metadata IS DISTINCT FROM to_jsonb(c)
    ) THEN RAISE EXCEPTION 'v13 modified contradictory v12 history'; END IF;
END $$;
