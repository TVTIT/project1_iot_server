\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=14 AND name='mqtt_credential_recovery') THEN RAISE EXCEPTION 'Missing recovery migration'; END IF;
    IF has_table_privilege('iot_backend_app','mqtt_credential_recovery','DELETE')
       OR has_column_privilege('iot_backend_app','mqtt_credential_recovery','origin','UPDATE')
       OR has_column_privilege('iot_backend_app','mqtt_credential_recovery','operation_id','UPDATE')
       OR has_column_privilege('iot_backend_app','mqtt_credential_recovery','attempted_version','UPDATE')
       OR NOT has_column_privilege('iot_backend_app','mqtt_credential_recovery','ram_disabled','UPDATE') THEN
        RAISE EXCEPTION 'Recovery privilege boundary incorrect';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='mqtt_credential_recovery'
        AND column_name ~ '(password|secret|hash|raw_snapshot)') THEN RAISE EXCEPTION 'Recovery contains native credential material'; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='check_mqtt_recovery_atomicity' AND tgdeferrable AND tginitdeferred) THEN
        RAISE EXCEPTION 'Missing deferred atomicity guard';
    END IF;
END $$;
