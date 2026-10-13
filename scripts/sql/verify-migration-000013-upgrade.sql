\set ON_ERROR_STOP on
DO $$ BEGIN
    IF (SELECT count(*) FROM fixture13_before) <> 2 OR EXISTS (
        SELECT 1 FROM fixture13_before b
        LEFT JOIN gateway_mqtt_credential_events e USING(operation_id)
        LEFT JOIN mqtt_credential_maintenance p USING(operation_id)
        LEFT JOIN gateway_mqtt_credentials c ON c.gateway_id=e.gateway_id
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(e) -> key) AS projected_event
            FROM jsonb_object_keys(b.event) AS key
            WHERE key <> 'recovery_id'
        ) pe
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(p) -> key) AS projected_checkpoint
            FROM jsonb_object_keys(b.checkpoint) AS key
            WHERE key <> 'recovery_id'
        ) pp
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(c) -> key) AS projected_metadata
            FROM jsonb_object_keys(b.metadata) AS key
        ) pc
        WHERE (b.event - 'recovery_id') IS DISTINCT FROM pe.projected_event
           OR (b.checkpoint - 'recovery_id') IS DISTINCT FROM pp.projected_checkpoint
           OR b.metadata IS DISTINCT FROM pc.projected_metadata
    ) THEN RAISE EXCEPTION 'v13 modified contradictory v12 history'; END IF;
END $$;
