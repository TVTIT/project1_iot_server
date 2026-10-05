\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=12 AND name='mqtt_credential_maintenance') THEN RAISE EXCEPTION 'Missing maintenance migration'; END IF;
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events e LEFT JOIN mqtt_credential_maintenance p USING(operation_id)
        WHERE NOT e.legacy_projection AND e.status IN ('pending','recovery_needed') AND (p.operation_id IS NULL OR p.gateway_id<>e.gateway_id)) THEN RAISE EXCEPTION 'Missing unresolved checkpoint'; END IF;
    IF EXISTS (SELECT 1 FROM mqtt_credential_maintenance p JOIN gateway_mqtt_credential_events e USING(operation_id) WHERE e.legacy_projection) THEN RAISE EXCEPTION 'Invented legacy maintenance'; END IF;
    IF has_table_privilege('iot_backend_app','mqtt_credential_maintenance','DELETE') OR has_column_privilege('iot_backend_app','mqtt_credential_maintenance','operation_id','UPDATE') OR has_column_privilege('iot_backend_app','mqtt_credential_maintenance','gateway_id','UPDATE') OR has_column_privilege('iot_backend_app','mqtt_credential_maintenance','created_at','UPDATE') THEN RAISE EXCEPTION 'Excess checkpoint privilege'; END IF;
    IF NOT has_table_privilege('iot_backend_app','mqtt_credential_maintenance','SELECT') OR NOT has_column_privilege('iot_backend_app','mqtt_credential_maintenance','status','UPDATE') OR NOT has_column_privilege('iot_backend_app','mqtt_credential_maintenance','operation_id','INSERT') THEN RAISE EXCEPTION 'Missing checkpoint privilege'; END IF;
    IF (SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='mqtt_credential_maintenance' AND column_name<>'recovery_id') <> 8 THEN RAISE EXCEPTION 'Unexpected checkpoint columns'; END IF;
END $$;
