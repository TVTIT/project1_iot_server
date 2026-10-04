\set ON_ERROR_STOP on
BEGIN;
INSERT INTO profiles(id) VALUES('15000000-0000-4000-8000-000000000001');
INSERT INTO gateways(gateway_id,name) VALUES('fixture13_terminal','v12 unfinished maintenance');
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key)
VALUES ('15000000-0000-4000-8000-000000000002','fixture13_terminal',1,'provision','pending','15000000-0000-4000-8000-000000000001','15000000-0000-4000-8000-000000000003');
UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now() WHERE gateway_id='fixture13_terminal';
INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,activated_at)
VALUES('fixture13_terminal',1,'active','15000000-0000-4000-8000-000000000002',now());
-- Reproduce the old v12 contradiction; v13 must preserve it for operator recovery.
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,previous_status,previous_credential_version,previous_activated_at)
VALUES ('15000000-0000-4000-8000-000000000004','fixture13_terminal',2,'rotate','pending','15000000-0000-4000-8000-000000000001','15000000-0000-4000-8000-000000000005','active',1,now());
UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now() WHERE operation_id='15000000-0000-4000-8000-000000000004';
UPDATE gateway_mqtt_credentials SET credential_version=2,last_operation_id='15000000-0000-4000-8000-000000000004' WHERE gateway_id='fixture13_terminal';
CREATE TABLE fixture13_before AS SELECT e.operation_id,to_jsonb(e) AS event,to_jsonb(p) AS checkpoint,to_jsonb(c) AS metadata
FROM gateway_mqtt_credential_events e JOIN mqtt_credential_maintenance p USING(operation_id) JOIN gateway_mqtt_credentials c ON c.gateway_id=e.gateway_id WHERE e.gateway_id='fixture13_terminal';
COMMIT;
