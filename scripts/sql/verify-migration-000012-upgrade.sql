\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM mqtt_credential_maintenance WHERE gateway_id='fixture12_pending' AND status='recovery_needed' AND broker_epoch IS NULL AND completed_at IS NULL) THEN RAISE EXCEPTION 'v11 pending not safely backfilled'; END IF;
    IF EXISTS(SELECT 1 FROM mqtt_credential_maintenance WHERE gateway_id='fixture12_succeeded') THEN RAISE EXCEPTION 'Historical terminal classified as new maintenance'; END IF;
    IF NOT EXISTS(SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id='fixture12_succeeded' AND status='succeeded' AND delivery_status='unknown') THEN RAISE EXCEPTION 'Terminal history rewritten'; END IF;
END $$;
