\set ON_ERROR_STOP on
INSERT INTO profiles(id) VALUES('14000000-0000-4000-8000-000000000001');
INSERT INTO gateways(gateway_id,name) VALUES('fixture12_pending','Pending v11'),('fixture12_succeeded','Terminal v11');
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key)
VALUES ('14000000-0000-4000-8000-000000000002','fixture12_pending',1,'provision','pending','14000000-0000-4000-8000-000000000001','14000000-0000-4000-8000-000000000004'),
('14000000-0000-4000-8000-000000000003','fixture12_succeeded',1,'provision','pending','14000000-0000-4000-8000-000000000001','14000000-0000-4000-8000-000000000005');
UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now() WHERE gateway_id='fixture12_succeeded';
