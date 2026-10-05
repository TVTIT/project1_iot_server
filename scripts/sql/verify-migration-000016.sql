\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=16 AND name='mqtt_event_authority_guard') THEN RAISE EXCEPTION 'Missing event authority migration'; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='check_mqtt_event_authority' AND tgdeferrable AND tginitdeferred AND tgenabled='O') THEN RAISE EXCEPTION 'Missing deferred event authority guard'; END IF;
END $$;
BEGIN;
INSERT INTO profiles(id) VALUES('16000000-0000-4000-8000-000000000001');
INSERT INTO gateways(gateway_id,name) VALUES('fixture16_current','Event authority');
SET LOCAL ROLE iot_backend_app;
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,actor_user_id,idempotency_key,credential_version,action,status)
VALUES('16000000-0000-4000-8000-000000000002','fixture16_current','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000003',1,'provision','pending');
INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,changed_by,updated_at)
SELECT gateway_id,credential_version,'provisioning',operation_id,actor_user_id,created_at FROM gateway_mqtt_credential_events WHERE gateway_id='fixture16_current';
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;
DO $$ DECLARE mutation text; BEGIN
    FOREACH mutation IN ARRAY ARRAY[
        'status=''succeeded'',phase=''finalize'',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now()',
        'status=''recovery_needed'',phase=''recovery'',updated_at=now()'
    ] LOOP
        BEGIN
            EXECUTE 'UPDATE gateway_mqtt_credential_events SET '||mutation||' WHERE gateway_id=''fixture16_current''';
            SET CONSTRAINTS ALL IMMEDIATE;
            RAISE EXCEPTION 'Event-only authority committed';
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM NOT IN ('Credential projection disagrees with operation evidence','Final projection requires checkpoint') THEN RAISE; END IF;
        END;
        IF NOT EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id='fixture16_current' AND status='pending') THEN RAISE EXCEPTION 'Failed event check changed state'; END IF;
    END LOOP;
    FOREACH mutation IN ARRAY ARRAY['credential_version=2','changed_by=NULL,credential_version=2','updated_at=updated_at+interval ''1 second'''] LOOP
        BEGIN
            EXECUTE 'UPDATE gateway_mqtt_credentials SET '||mutation||' WHERE gateway_id=''fixture16_current''';
            SET CONSTRAINTS ALL IMMEDIATE;
            RAISE EXCEPTION 'Metadata-only authority committed';
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM NOT IN ('Credential projection disagrees with operation evidence','Actor changes require a new intent') THEN RAISE; END IF;
        END;
    END LOOP;
END $$;
-- Metadata-first and event-first updates are both valid at the deferred boundary.
UPDATE gateway_mqtt_credentials SET status='active',activated_at=now(),updated_at=now() WHERE gateway_id='fixture16_current';
UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now() WHERE gateway_id='fixture16_current';
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;
UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),completed_at=now(),updated_at=now() WHERE gateway_id='fixture16_current';
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,actor_user_id,idempotency_key,credential_version,action,status,previous_status,previous_credential_version,previous_activated_at)
SELECT '16000000-0000-4000-8000-000000000004',gateway_id,changed_by,'16000000-0000-4000-8000-000000000005',2,'rotate','pending',status,credential_version,activated_at FROM gateway_mqtt_credentials WHERE gateway_id='fixture16_current';
UPDATE gateway_mqtt_credentials c SET status='rotating',credential_version=2,last_operation_id=e.operation_id,updated_at=e.created_at
FROM gateway_mqtt_credential_events e WHERE c.gateway_id='fixture16_current' AND e.operation_id='16000000-0000-4000-8000-000000000004';
SET CONSTRAINTS ALL IMMEDIATE;
RESET ROLE;
-- Current actor FK nulling validates and superseded terminal audit is skipped.
DELETE FROM profiles WHERE id='16000000-0000-4000-8000-000000000001';
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
