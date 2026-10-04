\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 11 AND name = 'mqtt_credential_operations') THEN
        RAISE EXCEPTION 'Migration 000011 is not recorded';
    END IF;
END $$;
CREATE FUNCTION pg_temp.must_reject(statement TEXT) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN check_violation OR unique_violation OR raise_exception OR insufficient_privilege THEN
        RETURN;
    END;
    RAISE EXCEPTION 'Unexpectedly accepted: %', statement;
END $$;
INSERT INTO profiles (id, full_name) VALUES ('11000000-0000-4000-8000-000000000001', 'Migration probe');
INSERT INTO gateways (gateway_id, name) VALUES ('migration11_probe', 'Migration probe'), ('migration11_other', 'Other probe');
INSERT INTO gateway_mqtt_credential_events
    (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status)
VALUES ('11000000-0000-4000-8000-000000000002', 'migration11_probe',
        '11000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000003', 1, 'provision', 'pending');
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM schema_migrations WHERE version=15) THEN
        INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,changed_by)
        VALUES('migration11_probe',1,'provisioning','11000000-0000-4000-8000-000000000002','11000000-0000-4000-8000-000000000001');
    END IF;
END $$;
SELECT pg_temp.must_reject($q$INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('11000000-0000-4000-8000-000000000004', 'migration11_probe', '11000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000005', 2, 'provision', 'pending')$q$);
SELECT pg_temp.must_reject($q$INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status) VALUES ('11000000-0000-4000-8000-000000000004', 'migration11_other', '11000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000003', 2, 'provision', 'pending')$q$);
SELECT pg_temp.must_reject($q$INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, action, status) VALUES ('11000000-0000-4000-8000-000000000004', 'migration11_other', 'recover', 'pending')$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET status = 'snapshot_observed' WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET status = 'succeeded', completed_at = now(), phase = 'finalize' WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET completed_at = now() WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET actor_user_id = NULL WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET legacy_projection = true WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET previous_status = 'active', previous_credential_version = 1 WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET phase = 'snapshot_observed' WHERE gateway_id = 'migration11_probe'$q$);
UPDATE gateway_mqtt_credential_events SET status = 'recovery_needed', phase = 'recovery', ram_applied = true, updated_at = now() WHERE gateway_id = 'migration11_probe';
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET status = 'failed', completed_at = now(), phase = 'finalize' WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET ram_applied = false WHERE gateway_id = 'migration11_probe'$q$);
UPDATE gateway_mqtt_credential_events SET status = 'succeeded', phase = 'finalize', snapshot_observed = true, fresh_positive_verified = true, completed_at = now(), updated_at = now() WHERE gateway_id = 'migration11_probe';
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET snapshot_observed = false WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET delivery_status = 'device_updated' WHERE gateway_id = 'migration11_probe'$q$);
SELECT pg_temp.must_reject($q$UPDATE gateway_mqtt_credential_events SET error_code = 'internal_error' WHERE gateway_id = 'migration11_probe'$q$);
-- Rotate's safe previous snapshot survives a known unchanged failure; failed
-- attempted version cannot be reused by the next mutation.
-- When checked on v12+, model successful maintenance before a new intent.
-- Keep this verifier usable on v11 itself; no fabricated historical backfill.
DO $$ BEGIN
    IF to_regclass('mqtt_credential_maintenance') IS NOT NULL THEN
        IF EXISTS (SELECT 1 FROM schema_migrations WHERE version=15) THEN
            UPDATE gateway_mqtt_credentials c SET status='active',activated_at=e.updated_at,updated_at=e.updated_at
            FROM gateway_mqtt_credential_events e WHERE c.gateway_id='migration11_probe' AND e.operation_id=c.last_operation_id;
        ELSE
            INSERT INTO gateway_mqtt_credentials(gateway_id,credential_version,status,last_operation_id,activated_at)
            VALUES('migration11_probe',1,'active','11000000-0000-4000-8000-000000000002',now());
        END IF;
        EXECUTE $q$UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),completed_at=now(),updated_at=now()
            WHERE operation_id='11000000-0000-4000-8000-000000000002'$q$;
    END IF;
END $$;
INSERT INTO gateway_mqtt_credential_events
    (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
     previous_status, previous_credential_version, previous_activated_at)
VALUES ('11000000-0000-4000-8000-000000000006', 'migration11_probe',
        '11000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000007', 2, 'rotate', 'pending', 'active', 1, now());
UPDATE gateway_mqtt_credential_events SET status = 'failed', phase = 'finalize', completed_at = now(), updated_at = now()
WHERE operation_id = '11000000-0000-4000-8000-000000000006';
SELECT pg_temp.must_reject($q$INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status, previous_status, previous_credential_version, previous_activated_at) VALUES ('11000000-0000-4000-8000-000000000008', 'migration11_probe', '11000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000009', 2, 'rotate', 'pending', 'active', 1, now())$q$);
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE operation_id = '11000000-0000-4000-8000-000000000006' AND previous_status = 'active' AND previous_credential_version = 1 AND previous_activated_at IS NOT NULL AND previous_revoked_at IS NULL) THEN
        RAISE EXCEPTION 'Previous snapshot was lost';
    END IF;
END $$;
DELETE FROM profiles WHERE id = '11000000-0000-4000-8000-000000000001';
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id = 'migration11_probe' AND actor_user_id IS NOT NULL) THEN
        RAISE EXCEPTION 'ON DELETE SET NULL failed';
    END IF;
    IF has_column_privilege('iot_backend_app', 'gateway_mqtt_credential_events', 'actor_user_id', 'UPDATE')
       OR has_column_privilege('iot_backend_app', 'gateway_mqtt_credential_events', 'legacy_projection', 'UPDATE')
       OR has_table_privilege('iot_backend_app', 'gateway_mqtt_credential_events', 'DELETE')
       OR has_table_privilege('iot_backend_app', 'gateway_mqtt_credentials', 'DELETE')
       OR has_table_privilege('iot_backend_app', 'platform_admins', 'UPDATE')
       OR NOT has_column_privilege('iot_backend_app', 'gateway_mqtt_credential_events', 'snapshot_observed', 'UPDATE') THEN
        RAISE EXCEPTION 'Invalid least-privilege grants';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name IN ('gateway_mqtt_credentials', 'gateway_mqtt_credential_events') AND column_name ~ '(password|hash|secret|payload|contents)') THEN
        RAISE EXCEPTION 'Secret-bearing schema';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('anon', 'authenticated') AND (has_table_privilege(rolname, 'gateway_mqtt_credentials', 'SELECT') OR has_table_privilege(rolname, 'gateway_mqtt_credential_events', 'SELECT'))) THEN
        RAISE EXCEPTION 'Public credential access';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = 'uq_mqtt_operation_unresolved'::regclass AND indisunique AND indpred IS NOT NULL)
       OR NOT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = 'uq_mqtt_operation_actor_key'::regclass AND indisunique) THEN
        RAISE EXCEPTION 'Missing unique operation indexes';
    END IF;
END $$;
ROLLBACK;
\echo 'Migration 000011 verification passed.'
