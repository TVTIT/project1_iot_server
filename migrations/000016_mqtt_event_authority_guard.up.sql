-- Final-state authority checks on both sides; flags remain adapter assertions,
-- not independent physical broker proof. No historical audit backfill.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=16) AS apply_event_authority \gset
\if :apply_event_authority
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=15 AND name='mqtt_recovery_authority_guards') THEN RAISE EXCEPTION 'Migration 000015 is required'; END IF;
END $$;
CREATE FUNCTION validate_mqtt_projection_authority(target_gateway text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE c gateway_mqtt_credentials; e gateway_mqtt_credential_events; r mqtt_credential_recovery;
    expected_status text; expected_version bigint; activation timestamptz; revocation timestamptz; stamp timestamptz; error text;
BEGIN
    SELECT * INTO c FROM gateway_mqtt_credentials WHERE gateway_id=target_gateway;
    SELECT * INTO e FROM gateway_mqtt_credential_events WHERE operation_id=c.last_operation_id AND gateway_id=c.gateway_id AND NOT legacy_projection;
    IF NOT FOUND THEN RAISE EXCEPTION 'Projection requires matching modern operation'; END IF;
    IF c.changed_by IS DISTINCT FROM e.actor_user_id THEN RAISE EXCEPTION 'Projection actor mismatch'; END IF;
    SELECT * INTO r FROM mqtt_credential_recovery WHERE operation_id=e.operation_id AND status='disabled';
    IF FOUND THEN
        IF NOT EXISTS (SELECT 1 FROM mqtt_credential_maintenance p WHERE p.operation_id=e.operation_id AND p.gateway_id=c.gateway_id
            AND p.status='completed' AND p.recovery_id=r.recovery_id AND p.broker_epoch=r.broker_epoch)
           OR NOT (e.status IN ('succeeded','failed') OR e.recovery_id=r.recovery_id) THEN RAISE EXCEPTION 'Projection requires completed recovery disposition'; END IF;
        expected_status := 'revoked'; expected_version := r.attempted_version;
        activation := CASE WHEN e.status='succeeded' AND e.action<>'revoke' THEN e.updated_at ELSE e.previous_activated_at END;
        revocation := r.completed_at; stamp := r.completed_at; error := 'credential_recovery_required';
    ELSE
        activation := e.previous_activated_at; expected_version := e.credential_version; stamp := e.updated_at; error := e.error_code;
        CASE e.status
        WHEN 'pending' THEN
            expected_status := CASE e.action WHEN 'provision' THEN 'provisioning' WHEN 'rotate' THEN 'rotating' ELSE 'revoking' END;
            error := NULL; stamp := e.created_at;
        WHEN 'succeeded' THEN
            IF NOT e.ram_applied OR NOT e.snapshot_observed OR (e.action<>'revoke' AND NOT e.fresh_positive_verified) THEN RAISE EXCEPTION 'Success requires verified observations'; END IF;
            expected_status := CASE WHEN e.action='revoke' THEN 'revoked' ELSE 'active' END;
            IF e.action='revoke' THEN revocation := e.updated_at; ELSE activation := e.updated_at; END IF;
            error := NULL;
        WHEN 'failed' THEN
            expected_status := COALESCE(e.previous_status,'failed'); expected_version := COALESCE(e.previous_credential_version,e.credential_version); revocation := e.previous_revoked_at;
        WHEN 'recovery_needed' THEN
            expected_status := 'recovery_needed'; expected_version := COALESCE(e.previous_credential_version,e.credential_version); revocation := e.previous_revoked_at;
        ELSE RAISE EXCEPTION 'Unsupported operation projection';
        END CASE;
        IF e.status<>'pending' AND NOT EXISTS (SELECT 1 FROM mqtt_credential_maintenance p WHERE p.operation_id=e.operation_id AND p.gateway_id=c.gateway_id) THEN RAISE EXCEPTION 'Final projection requires checkpoint'; END IF;
    END IF;
    IF ROW(c.status,c.credential_version,c.activated_at,c.revoked_at,c.updated_at,c.last_error_code)
       IS DISTINCT FROM ROW(expected_status,expected_version,activation,revocation,stamp,error) THEN RAISE EXCEPTION 'Credential projection disagrees with operation evidence'; END IF;
END $$;
CREATE OR REPLACE FUNCTION check_mqtt_projection_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
        -- BEFORE guard already proved the nested FK action (depth lost at COMMIT).
        IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL
           AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.changed_by)
           AND (to_jsonb(NEW)-'changed_by') IS NOT DISTINCT FROM (to_jsonb(OLD)-'changed_by') THEN RETURN NEW; END IF;
    END IF;
    PERFORM validate_mqtt_projection_authority(NEW.gateway_id);
    RETURN NEW;
END $$;
CREATE FUNCTION check_mqtt_event_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Validate the CURRENT selected modern event, not superseded/legacy audit.
    -- Unassociated events retain existing policy (no new global orphan policy).
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e
        ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id
        WHERE c.gateway_id=NEW.gateway_id AND e.operation_id=NEW.operation_id AND NOT e.legacy_projection) THEN
        PERFORM validate_mqtt_projection_authority(NEW.gateway_id);
    END IF;
    RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER check_mqtt_event_authority AFTER INSERT OR UPDATE ON gateway_mqtt_credential_events
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_event_authority();
INSERT INTO schema_migrations(version,name) VALUES(16,'mqtt_event_authority_guard');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=16 AND name='mqtt_event_authority_guard') THEN RAISE EXCEPTION 'Unexpected migration version 16'; END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
