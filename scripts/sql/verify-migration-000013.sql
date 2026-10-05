\set ON_ERROR_STOP on
DO $$ BEGIN
    IF (SELECT count(*) FROM schema_migrations WHERE version=13 AND name='mqtt_maintenance_admission') <> 1 THEN RAISE EXCEPTION 'Missing admission migration'; END IF;
    IF (SELECT count(*) FROM pg_trigger WHERE tgrelid='gateway_mqtt_credential_events'::regclass AND tgname='enforce_mqtt_maintenance_admission' AND tgenabled='O' AND tgtype=7 AND NOT tgisinternal) <> 1 THEN RAISE EXCEPTION 'Missing BEFORE INSERT admission trigger'; END IF;
    IF has_column_privilege('iot_backend_app','gateway_mqtt_credential_events','legacy_projection','INSERT') OR has_column_privilege('iot_backend_app','gateway_mqtt_credential_events','legacy_projection','UPDATE') OR has_column_privilege('iot_backend_app','gateway_mqtt_credential_events','gateway_id','UPDATE') THEN RAISE EXCEPTION 'App can bypass immutable intent identity'; END IF;
END $$;
BEGIN;
INSERT INTO profiles(id) VALUES('16000000-0000-4000-8000-000000000001');
INSERT INTO gateways(gateway_id,name) VALUES('fixture13_probe','Admission probe'),('fixture13_other','Independent probe');
SET LOCAL ROLE iot_backend_app;
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key)
VALUES('16000000-0000-4000-8000-000000000002','fixture13_probe',1,'provision','pending','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000003');
INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,changed_by)
VALUES('fixture13_probe',1,'provisioning','16000000-0000-4000-8000-000000000002','16000000-0000-4000-8000-000000000001');
UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now() WHERE gateway_id='fixture13_probe';
DO $$ DECLARE blocked boolean := false; BEGIN
    BEGIN
        INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key)
        VALUES('16000000-0000-4000-8000-000000000004','fixture13_probe',2,'provision','pending','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000005');
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'Gateway has unresolved credential maintenance' THEN RAISE; END IF;
        blocked := true;
    END;
    IF NOT blocked THEN RAISE EXCEPTION 'App SQL bypassed maintenance admission'; END IF;
END $$;
UPDATE mqtt_credential_maintenance SET status='recovery_needed',error_code='credential_recovery_required',updated_at=now() WHERE gateway_id='fixture13_probe';
DO $$ DECLARE statement text; blocked boolean; BEGIN
    FOREACH statement IN ARRAY ARRAY[
        $q$INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key) VALUES('16000000-0000-4000-8000-000000000004','fixture13_probe',2,'provision','pending','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000005')$q$,
        $q$INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,completed_at,phase) VALUES('16000000-0000-4000-8000-000000000004','fixture13_other',1,'provision','failed','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000005',now(),'finalize')$q$,
        $q$INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,action,status,legacy_projection) VALUES('16000000-0000-4000-8000-000000000004','fixture13_other','recover','pending',true)$q$,
        $q$UPDATE gateway_mqtt_credential_events SET gateway_id='fixture13_other' WHERE gateway_id='fixture13_probe'$q$,
        $q$UPDATE gateway_mqtt_credential_events SET status='pending',completed_at=NULL WHERE gateway_id='fixture13_probe'$q$
    ] LOOP
        blocked := false;
        BEGIN EXECUTE statement;
        EXCEPTION WHEN raise_exception OR insufficient_privilege THEN blocked := true;
        END;
        IF NOT blocked THEN RAISE EXCEPTION 'App bypassed maintenance or event identity guard'; END IF;
    END LOOP;
END $$;
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key)
VALUES('16000000-0000-4000-8000-000000000006','fixture13_other',1,'provision','pending','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000007');
UPDATE gateway_mqtt_credentials c SET status='active',activated_at=e.updated_at,updated_at=e.updated_at
FROM gateway_mqtt_credential_events e WHERE c.gateway_id='fixture13_probe' AND e.operation_id=c.last_operation_id;
UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),error_code=NULL,completed_at=now(),updated_at=now() WHERE gateway_id='fixture13_probe';
DO $$ DECLARE blocked boolean := false; BEGIN
    BEGIN UPDATE mqtt_credential_maintenance SET status='in_progress',completed_at=NULL WHERE gateway_id='fixture13_probe';
    EXCEPTION WHEN raise_exception THEN blocked := true; END;
    IF NOT blocked THEN RAISE EXCEPTION 'App changed completed checkpoint'; END IF;
END $$;
INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,previous_status,previous_credential_version,previous_activated_at)
VALUES('16000000-0000-4000-8000-000000000004','fixture13_probe',2,'rotate','pending','16000000-0000-4000-8000-000000000001','16000000-0000-4000-8000-000000000005','active',1,now());
ROLLBACK;
