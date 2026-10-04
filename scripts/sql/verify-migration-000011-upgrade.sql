\set ON_ERROR_STOP on
BEGIN;
DO $$ DECLARE expected RECORD; actual JSONB; BEGIN
    FOR expected IN SELECT * FROM migration11_golden LOOP
        CASE expected.kind
        WHEN 'event' THEN
            SELECT jsonb_object_agg(k, to_jsonb(e)->k) INTO actual
            FROM gateway_mqtt_credential_events e, jsonb_object_keys(expected.data) k
            WHERE operation_id = (expected.data->>'operation_id')::uuid;
        WHEN 'credential' THEN SELECT to_jsonb(c) INTO actual FROM gateway_mqtt_credentials c WHERE gateway_id = expected.data->>'gateway_id';
        WHEN 'profile' THEN SELECT to_jsonb(p) INTO actual FROM profiles p WHERE id = (expected.data->>'id')::uuid;
        WHEN 'gateway' THEN SELECT to_jsonb(g) INTO actual FROM gateways g WHERE gateway_id = expected.data->>'gateway_id';
        END CASE;
        IF actual IS DISTINCT FROM expected.data THEN RAISE EXCEPTION 'Legacy data changed: %', expected.kind; END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id LIKE 'legacy11_%'
        AND (NOT legacy_projection OR ram_applied OR snapshot_observed OR fresh_positive_verified OR idempotency_key IS NOT NULL)) THEN
        RAISE EXCEPTION 'Fabricated legacy evidence';
    END IF;
    BEGIN
        INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status)
        VALUES ('12000000-0000-4000-8000-000000000006', 'legacy11_pending', '12000000-0000-4000-8000-000000000001', '12000000-0000-4000-8000-000000000007', 8, 'provision', 'pending');
        RAISE EXCEPTION 'Modern intent bypassed legacy pending';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'Gateway has an unresolved credential operation' THEN RAISE; END IF;
    END;
END $$;
-- Profile deletion also works for legacy terminal and pending audit records.
DELETE FROM profiles WHERE id = '12000000-0000-4000-8000-000000000001';
ROLLBACK;
\echo 'Migration 000011 upgrade preservation passed.'
