\set ON_ERROR_STOP on
CREATE TABLE fixture14_events AS SELECT operation_id,to_jsonb(e) AS original FROM gateway_mqtt_credential_events e;
CREATE TABLE fixture14_maintenance AS SELECT operation_id,to_jsonb(p) AS original FROM mqtt_credential_maintenance p;
CREATE TABLE fixture14_authority AS SELECT gateway_id,to_jsonb(c) AS original FROM gateway_mqtt_credentials c;
