-- Internal recovery-disable decisions, not activation or password delivery proof.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=14) AS apply_recovery \gset
\if :apply_recovery
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=13 AND name='mqtt_maintenance_admission') THEN
        RAISE EXCEPTION 'Migration 000013 is required';
    END IF;
END $$;
CREATE TABLE mqtt_credential_recovery (
    recovery_id UUID PRIMARY KEY CHECK (recovery_id <> '00000000-0000-0000-0000-000000000000'),
    operation_id UUID NOT NULL UNIQUE REFERENCES mqtt_credential_maintenance(operation_id) ON DELETE RESTRICT,
    gateway_id TEXT NOT NULL REFERENCES gateways(gateway_id) ON DELETE RESTRICT,
    attempted_version BIGINT NOT NULL CHECK (attempted_version > 0),
    origin TEXT NOT NULL DEFAULT 'startup' CHECK (origin='startup'),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','disabled')),
    broker_epoch TEXT NOT NULL CHECK (broker_epoch ~ '^[0-9a-f]{64}$'),
    ram_disabled BOOLEAN NOT NULL DEFAULT false,
    snapshot_observed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (updated_at >= created_at),
    CHECK ((status='pending' AND completed_at IS NULL AND NOT ram_disabled AND NOT snapshot_observed)
        OR (status='disabled' AND completed_at IS NOT NULL AND completed_at >= created_at AND updated_at >= completed_at AND ram_disabled AND snapshot_observed))
);
CREATE INDEX idx_mqtt_recovery_pending ON mqtt_credential_recovery(recovery_id) WHERE status='pending';
ALTER TABLE gateway_mqtt_credential_events ADD COLUMN recovery_id UUID REFERENCES mqtt_credential_recovery(recovery_id) ON DELETE RESTRICT;
ALTER TABLE mqtt_credential_maintenance ADD COLUMN recovery_id UUID REFERENCES mqtt_credential_recovery(recovery_id) ON DELETE RESTRICT;
ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_recovery_disposition CHECK (recovery_id IS NULL OR (NOT legacy_projection AND status IN ('pending','recovery_needed')));
ALTER TABLE mqtt_credential_maintenance ADD CONSTRAINT chk_mqtt_maintenance_recovery CHECK (recovery_id IS NULL OR status='completed');
DROP INDEX uq_mqtt_operation_unresolved;
CREATE UNIQUE INDEX uq_mqtt_operation_unresolved ON gateway_mqtt_credential_events(gateway_id)
    WHERE NOT legacy_projection AND status IN ('pending','recovery_needed') AND recovery_id IS NULL;

CREATE OR REPLACE FUNCTION enforce_gateway_mqtt_event_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.legacy_projection OR NEW.actor_user_id IS NULL OR NEW.actor_user_id='00000000-0000-0000-0000-000000000000'::uuid
           OR NEW.status<>'pending' OR NEW.phase<>'intent' OR NEW.ram_applied OR NEW.snapshot_observed OR NEW.fresh_positive_verified
           OR NEW.delivery_status<>'unknown' OR NEW.recovery_id IS NOT NULL THEN
            RAISE EXCEPTION 'New operations require a live actor and unverified intent';
        END IF;
        PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
        IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id=NEW.gateway_id AND status IN ('pending','recovery_needed') AND recovery_id IS NULL) THEN
            RAISE EXCEPTION 'Gateway has an unresolved credential operation';
        END IF;
        IF NEW.action IN ('provision','rotate') AND NEW.credential_version <= GREATEST(
            COALESCE((SELECT credential_version FROM gateway_mqtt_credentials WHERE gateway_id=NEW.gateway_id),0),
            COALESCE((SELECT max(credential_version) FROM gateway_mqtt_credential_events WHERE gateway_id=NEW.gateway_id),0)) THEN
            RAISE EXCEPTION 'Credential generation must exceed current and attempted history';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL AND pg_trigger_depth()>1
       AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.actor_user_id)
       AND (to_jsonb(NEW)-'actor_user_id')=(to_jsonb(OLD)-'actor_user_id') THEN RETURN NEW; END IF;
    IF OLD.status IN ('succeeded','failed') OR OLD.recovery_id IS NOT NULL THEN
        RAISE EXCEPTION 'Completed MQTT credential events are immutable';
    END IF;
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    IF NEW.recovery_id IS NOT NULL THEN
        IF (to_jsonb(NEW)-'recovery_id') IS DISTINCT FROM (to_jsonb(OLD)-'recovery_id') OR OLD.legacy_projection
           OR NOT EXISTS (SELECT 1 FROM mqtt_credential_recovery r JOIN gateway_mqtt_credentials c ON c.gateway_id=r.gateway_id
               WHERE r.recovery_id=NEW.recovery_id AND r.operation_id=NEW.operation_id AND r.gateway_id=NEW.gateway_id
                 AND r.attempted_version=NEW.credential_version AND r.status='disabled' AND c.last_operation_id=r.operation_id
                 AND c.status='revoked' AND c.credential_version=r.attempted_version) THEN
            RAISE EXCEPTION 'Recovery disposition requires current verified disable';
        END IF;
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT 1 FROM mqtt_credential_recovery WHERE operation_id=OLD.operation_id) THEN
        RAISE EXCEPTION 'Recovery intent fences ordinary event finalization';
    END IF;
    IF (to_jsonb(NEW)-ARRAY['status','completed_at','error_code','phase','ram_applied','snapshot_observed','fresh_positive_verified','delivery_status','updated_at'])
       IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['status','completed_at','error_code','phase','ram_applied','snapshot_observed','fresh_positive_verified','delivery_status','updated_at']) THEN
        RAISE EXCEPTION 'Credential operation identity and previous state are immutable';
    END IF;
    IF OLD.legacy_projection THEN
        IF OLD.status<>'pending' OR NEW.status NOT IN ('succeeded','failed') OR NEW.ram_applied OR NEW.snapshot_observed OR NEW.fresh_positive_verified
           OR NEW.phase IS DISTINCT FROM OLD.phase OR NEW.delivery_status<>OLD.delivery_status THEN RAISE EXCEPTION 'Invalid historical projection transition'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.updated_at<OLD.updated_at OR (OLD.ram_applied AND NOT NEW.ram_applied) OR (OLD.snapshot_observed AND NOT NEW.snapshot_observed)
       OR (OLD.fresh_positive_verified AND NOT NEW.fresh_positive_verified)
       OR (OLD.status='recovery_needed' AND NEW.status NOT IN ('recovery_needed','succeeded'))
       OR (NEW.status='failed' AND (OLD.ram_applied OR OLD.snapshot_observed OR OLD.fresh_positive_verified)) THEN
        RAISE EXCEPTION 'Invalid credential operation recovery or evidence transition';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION enforce_mqtt_maintenance_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'in_progress' OR NEW.broker_epoch IS NOT NULL OR NEW.error_code IS NOT NULL OR NEW.recovery_id IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE operation_id=NEW.operation_id AND gateway_id=NEW.gateway_id AND NOT legacy_projection AND status='pending') THEN
            RAISE EXCEPTION 'Maintenance requires matching modern intent';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status='completed' THEN RAISE EXCEPTION 'Completed maintenance is immutable'; END IF;
    IF NEW.operation_id<>OLD.operation_id OR NEW.gateway_id<>OLD.gateway_id OR NEW.created_at<>OLD.created_at OR NEW.updated_at<OLD.updated_at THEN
        RAISE EXCEPTION 'Maintenance identity is immutable';
    END IF;
    IF NEW.recovery_id IS NOT NULL THEN
        IF NEW.status<>'completed' OR NOT EXISTS (
            SELECT 1 FROM mqtt_credential_recovery r JOIN gateway_mqtt_credentials c ON c.gateway_id=r.gateway_id
            JOIN gateway_mqtt_credential_events e ON e.operation_id=r.operation_id
            WHERE r.recovery_id=NEW.recovery_id AND r.operation_id=NEW.operation_id AND r.gateway_id=NEW.gateway_id
              AND r.status='disabled' AND r.broker_epoch=NEW.broker_epoch AND c.last_operation_id=r.operation_id
              AND c.status='revoked' AND c.credential_version=r.attempted_version
              AND (e.status IN ('succeeded','failed') OR e.recovery_id=r.recovery_id)) THEN
            RAISE EXCEPTION 'Recovered maintenance requires linked current disable';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM mqtt_credential_recovery WHERE operation_id=NEW.operation_id) THEN
        RAISE EXCEPTION 'Recovery intent forbids ordinary maintenance completion or rebinding';
    ELSIF NEW.status='completed' AND NOT EXISTS (
        SELECT 1 FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id
        WHERE c.gateway_id=NEW.gateway_id AND c.last_operation_id=NEW.operation_id AND e.status IN ('succeeded','failed')) THEN
        RAISE EXCEPTION 'Maintenance completion requires current terminal operation';
    END IF;
    RETURN NEW;
END $$;

CREATE FUNCTION enforce_mqtt_recovery_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'pending' OR NEW.ram_disabled OR NEW.snapshot_observed OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'Recovery requires unverified intent';
        END IF;
        IF EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=NEW.operation_id AND broker_epoch=NEW.broker_epoch) THEN
            RAISE EXCEPTION 'Recovery requires a fresh broker lifetime';
        END IF;
    ELSE
        IF OLD.status='disabled' THEN RAISE EXCEPTION 'Completed recovery is immutable'; END IF;
        IF (to_jsonb(NEW)-ARRAY['status','broker_epoch','ram_disabled','snapshot_observed','updated_at','completed_at']) IS DISTINCT FROM
           (to_jsonb(OLD)-ARRAY['status','broker_epoch','ram_disabled','snapshot_observed','updated_at','completed_at']) OR NEW.updated_at<OLD.updated_at THEN
            RAISE EXCEPTION 'Recovery identity is immutable';
        END IF;
        IF NEW.status='disabled' AND NEW.broker_epoch<>OLD.broker_epoch THEN RAISE EXCEPTION 'Completion must match bound epoch'; END IF;
        IF NEW.broker_epoch<>OLD.broker_epoch AND EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=NEW.operation_id AND broker_epoch=NEW.broker_epoch) THEN
            RAISE EXCEPTION 'Recovery requires a fresh broker lifetime';
        END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id
        JOIN mqtt_credential_maintenance p ON p.operation_id=e.operation_id AND p.gateway_id=e.gateway_id
        WHERE c.gateway_id=NEW.gateway_id AND e.operation_id=NEW.operation_id AND NOT e.legacy_projection AND e.recovery_id IS NULL
          AND e.credential_version=NEW.attempted_version AND p.status<>'completed'
          AND ((e.status='pending' AND c.credential_version=e.credential_version AND c.status=CASE e.action WHEN 'provision' THEN 'provisioning' WHEN 'rotate' THEN 'rotating' ELSE 'revoking' END)
            OR (e.status='recovery_needed' AND c.status='recovery_needed' AND c.credential_version=COALESCE(e.previous_credential_version,e.credential_version))
            OR (e.status='succeeded' AND c.credential_version=e.credential_version AND c.status=CASE WHEN e.action='revoke' THEN 'revoked' ELSE 'active' END)
            OR (e.status='failed' AND c.credential_version=COALESCE(e.previous_credential_version,e.credential_version) AND c.status=COALESCE(e.previous_status,'failed')))) THEN
        RAISE EXCEPTION 'Recovery requires consistent current modern operation and unresolved checkpoint';
    END IF;
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id=NEW.gateway_id AND operation_id<>NEW.operation_id AND status IN ('pending','recovery_needed') AND recovery_id IS NULL)
       OR EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE gateway_id=NEW.gateway_id AND operation_id<>NEW.operation_id AND status<>'completed') THEN
        RAISE EXCEPTION 'Contradictory history requires operator diagnosis';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_recovery_transition BEFORE INSERT OR UPDATE ON mqtt_credential_recovery FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_recovery_transition();

-- Deferred cross-table checks prevent partial direct SQL writes from committing.
-- Observations themselves are trusted internal app-role assertions, like v11;
-- SQL cannot independently attest broker RAM or filesystem observations.
CREATE FUNCTION check_mqtt_recovery_atomicity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM mqtt_credential_recovery r WHERE r.recovery_id=NEW.recovery_id AND r.status='disabled'
        AND NOT EXISTS (SELECT 1 FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id
            JOIN mqtt_credential_maintenance p ON p.operation_id=e.operation_id
            WHERE c.gateway_id=r.gateway_id AND c.last_operation_id=r.operation_id AND c.status='revoked' AND c.credential_version=r.attempted_version
              AND p.status='completed' AND p.recovery_id=r.recovery_id AND p.broker_epoch=r.broker_epoch
              AND (e.status IN ('succeeded','failed') OR e.recovery_id=r.recovery_id))) THEN
        RAISE EXCEPTION 'Recovery completion must atomically revoke and resolve checkpoint and event';
    END IF;
    RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER check_mqtt_recovery_atomicity AFTER UPDATE ON mqtt_credential_recovery DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_recovery_atomicity();
CREATE FUNCTION enforce_mqtt_recovery_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r mqtt_credential_recovery;
BEGIN
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    SELECT * INTO r FROM mqtt_credential_recovery WHERE operation_id=OLD.last_operation_id;
    IF FOUND AND EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=r.operation_id AND status<>'completed') THEN
        IF r.status<>'disabled' OR NEW.last_operation_id IS DISTINCT FROM OLD.last_operation_id
           OR NEW.status<>'revoked' OR NEW.credential_version<>r.attempted_version
           OR NEW.revoked_at IS DISTINCT FROM r.completed_at
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.changed_by IS DISTINCT FROM OLD.changed_by
           OR NEW.last_error_code IS DISTINCT FROM 'credential_recovery_required' THEN
            RAISE EXCEPTION 'Recovery intent fences current authority';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_recovery_authority BEFORE UPDATE ON gateway_mqtt_credentials FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_recovery_authority();
GRANT SELECT ON mqtt_credential_recovery TO iot_backend;
GRANT INSERT (recovery_id,operation_id,gateway_id,attempted_version,broker_epoch) ON mqtt_credential_recovery TO iot_backend;
GRANT UPDATE (status,broker_epoch,ram_disabled,snapshot_observed,updated_at,completed_at) ON mqtt_credential_recovery TO iot_backend;
GRANT UPDATE (recovery_id) ON gateway_mqtt_credential_events,mqtt_credential_maintenance TO iot_backend;
INSERT INTO schema_migrations(version,name) VALUES(14,'mqtt_credential_recovery');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=14 AND name='mqtt_credential_recovery') THEN RAISE EXCEPTION 'Unexpected migration version 14'; END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
