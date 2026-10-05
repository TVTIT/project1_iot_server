\set ON_ERROR_STOP on
CREATE TABLE fixture15_authority AS SELECT gateway_id,to_jsonb(c) AS original FROM gateway_mqtt_credentials c;
CREATE TABLE fixture15_events AS SELECT operation_id,to_jsonb(e) AS original FROM gateway_mqtt_credential_events e;
CREATE TABLE fixture15_recovery AS SELECT recovery_id,to_jsonb(r) AS original FROM mqtt_credential_recovery r;
