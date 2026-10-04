-- Correlation guards; SQL flags are trusted adapter assertions, not broker proof.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=15) AS apply_authority \gset
\if :apply_authority
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=14 AND name='mqtt_credential_recovery') THEN RAISE EXCEPTION 'Migration 000014 is required'; END IF;
END $$;
CREATE OR REPLACE FUNCTION enforce_mqtt_recovery_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r mqtt_credential_recovery;
BEGIN
    IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL AND pg_trigger_depth()>1
       AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.changed_by)
       AND (to_jsonb(NEW)-'changed_by') IS NOT DISTINCT FROM (to_jsonb(OLD)-'changed_by') THEN RETURN NEW; END IF;
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    SELECT * INTO r FROM mqtt_credential_recovery WHERE operation_id=OLD.last_operation_id;
    IF FOUND AND EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=r.operation_id AND status<>'completed') THEN
        IF r.status<>'disabled' OR NEW.last_operation_id IS DISTINCT FROM OLD.last_operation_id
           OR NEW.status<>'revoked' OR NEW.credential_version<>r.attempted_version
           OR NEW.revoked_at IS DISTINCT FROM r.completed_at
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.changed_by IS DISTINCT FROM OLD.changed_by
           OR NEW.last_error_code IS DISTINCT FROM 'credential_recovery_required' THEN RAISE EXCEPTION 'Recovery intent fences current authority'; END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION enforce_mqtt_projection_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE e gateway_mqtt_credential_events; previous gateway_mqtt_credentials;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
        IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL AND pg_trigger_depth()>1
           AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.changed_by)
           AND (to_jsonb(NEW)-'changed_by') IS NOT DISTINCT FROM (to_jsonb(OLD)-'changed_by') THEN RETURN NEW; END IF;
        IF NEW.gateway_id<>OLD.gateway_id THEN RAISE EXCEPTION 'Credential Gateway is immutable'; END IF;
    END IF;
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    SELECT * INTO e FROM gateway_mqtt_credential_events WHERE operation_id=NEW.last_operation_id AND gateway_id=NEW.gateway_id AND NOT legacy_projection;
    IF NOT FOUND THEN RAISE EXCEPTION 'Credential mutation requires modern operation'; END IF;
    IF TG_OP='INSERT' OR NEW.last_operation_id IS DISTINCT FROM OLD.last_operation_id THEN
        IF e.status<>'pending' OR e.phase<>'intent' OR NEW.changed_by IS DISTINCT FROM e.actor_user_id THEN RAISE EXCEPTION 'New projection requires matching intent actor'; END IF;
        IF TG_OP='INSERT' THEN
            SELECT * INTO previous FROM gateway_mqtt_credentials WHERE gateway_id=NEW.gateway_id;
            IF FOUND THEN
                IF ROW(e.previous_status,e.previous_credential_version,e.previous_activated_at,e.previous_revoked_at)
                   IS DISTINCT FROM ROW(previous.status,previous.credential_version,previous.activated_at,previous.revoked_at) THEN RAISE EXCEPTION 'Intent must preserve actual previous projection'; END IF;
            ELSIF e.previous_status IS NOT NULL THEN RAISE EXCEPTION 'Missing previous projection'; END IF;
        ELSIF ROW(e.previous_status,e.previous_credential_version,e.previous_activated_at,e.previous_revoked_at)
            IS DISTINCT FROM ROW(OLD.status,OLD.credential_version,OLD.activated_at,OLD.revoked_at) THEN RAISE EXCEPTION 'Intent must preserve actual previous projection'; END IF;
    ELSIF NEW.changed_by IS DISTINCT FROM OLD.changed_by THEN RAISE EXCEPTION 'Actor changes require a new intent';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_projection_transition BEFORE INSERT OR UPDATE ON gateway_mqtt_credentials FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_projection_transition();
CREATE FUNCTION check_mqtt_projection_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c gateway_mqtt_credentials; e gateway_mqtt_credential_events; r mqtt_credential_recovery;
    expected_status text; expected_version bigint; activation timestamptz; revocation timestamptz; stamp timestamptz; error text;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
        -- BEFORE guard already proved the nested FK action (depth is lost at COMMIT).
        IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL
           AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.changed_by)
           AND (to_jsonb(NEW)-'changed_by') IS NOT DISTINCT FROM (to_jsonb(OLD)-'changed_by') THEN RETURN NEW; END IF;
    END IF;
    SELECT * INTO c FROM gateway_mqtt_credentials WHERE gateway_id=NEW.gateway_id;
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
    RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER check_mqtt_projection_authority AFTER INSERT OR UPDATE ON gateway_mqtt_credentials DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_projection_authority();
INSERT INTO schema_migrations(version,name) VALUES(15,'mqtt_recovery_authority_guards');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=15 AND name='mqtt_recovery_authority_guards') THEN RAISE EXCEPTION 'Unexpected migration version 15'; END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
