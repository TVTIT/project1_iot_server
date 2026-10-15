-- Narrow manual replacement; no historical audit/checkpoint backfill or secrets.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=17) AS apply_replacement \gset
\if :apply_replacement
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=16 AND name='mqtt_event_authority_guard') THEN RAISE EXCEPTION 'Migration 000016 is required'; END IF;
END $$;

CREATE TABLE mqtt_rotation_replacements (
    predecessor_id uuid PRIMARY KEY REFERENCES gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT,
    successor_id uuid NOT NULL UNIQUE REFERENCES gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    gateway_id text NOT NULL REFERENCES gateways(gateway_id) ON DELETE RESTRICT,
    disposition text NOT NULL DEFAULT 'execution_authority_superseded' CHECK (disposition='execution_authority_superseded'),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (predecessor_id<>successor_id)
);
-- SELECT FOR SHARE needs UPDATE privileges; do not grant admin-table writes to
-- the app. This narrowly scoped owner function only locks an existing grant.
CREATE FUNCTION lock_mqtt_replacement_admin(target uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
    PERFORM 1 FROM public.platform_admins WHERE user_id=target FOR SHARE;
    RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION lock_mqtt_replacement_admin(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lock_mqtt_replacement_admin(uuid) TO iot_backend;
ALTER TABLE gateway_mqtt_credential_events ADD COLUMN replacement_predecessor_id uuid REFERENCES gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT;
ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_replacement_identity CHECK (
    replacement_predecessor_id IS NULL OR (NOT legacy_projection AND action='rotate' AND replacement_predecessor_id<>operation_id AND recovery_id IS NULL)
);
-- Only the manual rotate's previous-state clause changes; every other v11
-- evidence/timestamp/generation constraint is retained verbatim.
DO $$ DECLARE old text; amended text; BEGIN
    SELECT pg_get_constraintdef(oid) INTO old FROM pg_constraint WHERE conrelid='gateway_mqtt_credential_events'::regclass AND conname='chk_mqtt_operation_contract';
    amended := replace(old, '(previous_status = ''active''::text)', '((previous_status = ''active''::text) OR (action = ''rotate''::text AND replacement_predecessor_id IS NOT NULL))');
    IF amended=old THEN RAISE EXCEPTION 'Unexpected previous-state constraint'; END IF;
    ALTER TABLE gateway_mqtt_credential_events DROP CONSTRAINT chk_mqtt_operation_contract;
    EXECUTE 'ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_operation_contract ' || amended;
END $$;

-- Sidecar predicates cannot occur in a partial index. Gateway-serialized BEFORE
-- admission plus deferred both-side inventory checks replace (not remove) its
-- guarantee, and include legacy unresolved rows as before.
DROP INDEX uq_mqtt_operation_unresolved;

CREATE FUNCTION mqtt_operation_effective(target uuid) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
    SELECT NOT EXISTS (SELECT 1 FROM public.mqtt_rotation_replacements WHERE predecessor_id=target)
$$;

-- Preserve v14/v15/v16 ordinary guards. Apply only exact effective predicates to
-- their unresolved inventory, not blanket skip of historical rows.
DO $$ DECLARE name text; definition text; BEGIN
    FOREACH name IN ARRAY ARRAY['enforce_gateway_mqtt_event_transition','enforce_mqtt_maintenance_admission','enforce_mqtt_recovery_transition'] LOOP
        SELECT pg_get_functiondef((('public.'||name||'()')::regprocedure)::oid) INTO definition;
        definition := replace(definition,
          'status IN (''pending'',''recovery_needed'') AND recovery_id IS NULL',
          'status IN (''pending'',''recovery_needed'') AND recovery_id IS NULL AND mqtt_operation_effective(operation_id)');
        definition := replace(definition, 'status <> ''completed''', 'status <> ''completed'' AND mqtt_operation_effective(operation_id)');
        definition := replace(definition, 'status<>''completed''', 'status<>''completed'' AND mqtt_operation_effective(operation_id)');
        EXECUTE definition;
    END LOOP;
END $$;

CREATE FUNCTION enforce_mqtt_replacement_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE e gateway_mqtt_credential_events;
BEGIN
    IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Replacement disposition is immutable'; END IF;
    PERFORM 1 FROM gateways WHERE gateway_id=NEW.gateway_id FOR UPDATE;
    SELECT e0.* INTO e FROM gateway_mqtt_credential_events e0 JOIN gateway_mqtt_credentials c
      ON c.last_operation_id=e0.operation_id AND c.gateway_id=e0.gateway_id
      WHERE e0.operation_id=NEW.predecessor_id AND c.gateway_id=NEW.gateway_id;
    IF NOT FOUND OR e.legacy_projection OR e.action<>'rotate' OR e.recovery_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE operation_id=NEW.successor_id)
       OR EXISTS (SELECT 1 FROM mqtt_credential_recovery WHERE operation_id=e.operation_id)
       OR NOT EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=e.operation_id AND gateway_id=NEW.gateway_id)
       OR EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id=NEW.gateway_id AND operation_id<>e.operation_id AND status IN ('pending','recovery_needed') AND recovery_id IS NULL AND mqtt_operation_effective(operation_id))
       OR EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE gateway_id=NEW.gateway_id AND operation_id<>e.operation_id AND status<>'completed' AND mqtt_operation_effective(operation_id))
       OR EXISTS (SELECT 1 FROM mqtt_credential_recovery WHERE gateway_id=NEW.gateway_id AND status='pending') THEN
        RAISE EXCEPTION 'Replacement requires exact consistent current rotation';
    END IF;
    PERFORM validate_mqtt_projection_authority(NEW.gateway_id);
    RETURN NEW;
END $$;
CREATE TRIGGER enforce_mqtt_replacement_disposition BEFORE INSERT OR UPDATE OR DELETE ON mqtt_rotation_replacements FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_replacement_disposition();

CREATE FUNCTION enforce_mqtt_replacement_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE actor uuid;
BEGIN
    IF TG_OP='UPDATE' THEN
        -- Preserve the narrow nested FK actor-delete exception, no other edits.
        IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL AND pg_trigger_depth()>1
          AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.actor_user_id)
          AND (to_jsonb(NEW)-'actor_user_id')=(to_jsonb(OLD)-'actor_user_id') THEN RETURN NEW; END IF;
        IF NOT mqtt_operation_effective(OLD.operation_id) THEN RAISE EXCEPTION 'Superseded audit is immutable'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.replacement_predecessor_id IS NOT NULL THEN
        IF NOT lock_mqtt_replacement_admin(NEW.actor_user_id) OR NOT EXISTS (SELECT 1 FROM mqtt_rotation_replacements d JOIN gateway_mqtt_credentials c ON c.gateway_id=d.gateway_id
          WHERE d.predecessor_id=NEW.replacement_predecessor_id AND d.successor_id=NEW.operation_id AND d.gateway_id=NEW.gateway_id
            AND c.last_operation_id=d.predecessor_id
            AND ROW(c.status,c.credential_version,c.activated_at,c.revoked_at) IS NOT DISTINCT FROM
                ROW(NEW.previous_status,NEW.previous_credential_version,NEW.previous_activated_at,NEW.previous_revoked_at)) THEN
            RAISE EXCEPTION 'Replacement intent requires exact disposition, previous projection and live admin';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM mqtt_rotation_replacements WHERE successor_id=NEW.operation_id) THEN
        RAISE EXCEPTION 'Disposition requires replacement intent';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER aa_mqtt_replacement_event BEFORE INSERT OR UPDATE ON gateway_mqtt_credential_events FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_replacement_event();
CREATE FUNCTION enforce_mqtt_superseded_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT mqtt_operation_effective(OLD.operation_id) THEN RAISE EXCEPTION 'Superseded checkpoint is immutable'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER aa_mqtt_superseded_checkpoint BEFORE UPDATE OR DELETE ON mqtt_credential_maintenance FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_superseded_checkpoint();
CREATE FUNCTION enforce_mqtt_replacement_no_startup_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT mqtt_operation_effective(NEW.operation_id) OR EXISTS (
        SELECT 1 FROM gateway_mqtt_credential_events WHERE operation_id=NEW.operation_id AND replacement_predecessor_id IS NOT NULL
    ) THEN RAISE EXCEPTION 'Manual replacement cannot enter startup disable recovery'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER aa_mqtt_replacement_no_startup_recovery BEFORE INSERT OR UPDATE ON mqtt_credential_recovery FOR EACH ROW EXECUTE FUNCTION enforce_mqtt_replacement_no_startup_recovery();

CREATE FUNCTION validate_mqtt_replacement_inventory(target text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM mqtt_rotation_replacements d
       LEFT JOIN gateway_mqtt_credential_events p ON p.operation_id=d.predecessor_id
       LEFT JOIN gateway_mqtt_credential_events s ON s.operation_id=d.successor_id
       LEFT JOIN mqtt_credential_maintenance pm ON pm.operation_id=p.operation_id
       LEFT JOIN mqtt_credential_maintenance sm ON sm.operation_id=s.operation_id
       WHERE d.gateway_id=target AND (p.operation_id IS NULL OR s.operation_id IS NULL OR pm.operation_id IS NULL OR sm.operation_id IS NULL
         OR p.gateway_id<>d.gateway_id OR s.gateway_id<>d.gateway_id OR pm.gateway_id<>d.gateway_id OR sm.gateway_id<>d.gateway_id
         OR p.legacy_projection OR s.legacy_projection OR p.action<>'rotate' OR s.action<>'rotate'
         OR s.replacement_predecessor_id IS DISTINCT FROM p.operation_id OR s.credential_version<=p.credential_version
         OR EXISTS (SELECT 1 FROM mqtt_credential_recovery WHERE operation_id IN (p.operation_id,s.operation_id)))) THEN
        RAISE EXCEPTION 'Invalid replacement event/checkpoint/disposition linkage';
    END IF;
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events e WHERE e.gateway_id=target AND e.replacement_predecessor_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM mqtt_rotation_replacements WHERE predecessor_id=e.replacement_predecessor_id AND successor_id=e.operation_id AND gateway_id=target)) THEN
        RAISE EXCEPTION 'Replacement intent has no disposition';
    END IF;
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credentials c WHERE c.gateway_id=target AND NOT mqtt_operation_effective(c.last_operation_id)) THEN
        RAISE EXCEPTION 'Superseded event cannot regain current authority';
    END IF;
    IF (SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id=target AND NOT legacy_projection AND status IN ('pending','recovery_needed') AND recovery_id IS NULL AND mqtt_operation_effective(operation_id))>1 THEN
        RAISE EXCEPTION 'Duplicate effective operation';
    END IF;
    IF EXISTS (SELECT 1 FROM mqtt_credential_maintenance p WHERE p.gateway_id=target AND p.status<>'completed' AND mqtt_operation_effective(p.operation_id)
        AND NOT EXISTS (SELECT 1 FROM gateway_mqtt_credentials WHERE gateway_id=target AND last_operation_id=p.operation_id)) THEN
        RAISE EXCEPTION 'Effective checkpoint requires current authority';
    END IF;
    -- Newly admitted effective intents must be selected current with checkpoint.
    IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events e WHERE e.gateway_id=target AND NOT e.legacy_projection
       AND e.status IN ('pending','recovery_needed') AND e.recovery_id IS NULL AND mqtt_operation_effective(e.operation_id)
       AND (NOT EXISTS (SELECT 1 FROM gateway_mqtt_credentials WHERE gateway_id=target AND last_operation_id=e.operation_id)
         OR NOT EXISTS (SELECT 1 FROM mqtt_credential_maintenance WHERE operation_id=e.operation_id AND gateway_id=target))) THEN
        RAISE EXCEPTION 'Effective intent requires current projection and checkpoint';
    END IF;
    PERFORM validate_mqtt_projection_authority(target);
END $$;
CREATE FUNCTION check_mqtt_replacement_inventory() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF TG_TABLE_NAME='gateway_mqtt_credential_events' THEN
            IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.actor_user_id)
                AND (to_jsonb(NEW)-'actor_user_id')=(to_jsonb(OLD)-'actor_user_id') THEN RETURN NEW; END IF;
        ELSIF TG_TABLE_NAME='gateway_mqtt_credentials' THEN
            IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL
                AND NOT EXISTS (SELECT 1 FROM profiles WHERE id=OLD.changed_by)
                AND (to_jsonb(NEW)-'changed_by')=(to_jsonb(OLD)-'changed_by') THEN RETURN NEW; END IF;
        END IF;
    END IF;
    IF TG_TABLE_NAME='gateway_mqtt_credential_events' AND TG_OP='INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM gateway_mqtt_credentials WHERE gateway_id=NEW.gateway_id AND last_operation_id=NEW.operation_id)
            AND NOT EXISTS (SELECT 1 FROM mqtt_rotation_replacements WHERE predecessor_id=NEW.operation_id AND gateway_id=NEW.gateway_id) THEN
            RAISE EXCEPTION 'New intent cannot commit as unassociated audit';
        END IF;
    END IF;
    PERFORM validate_mqtt_replacement_inventory(COALESCE(NEW.gateway_id,OLD.gateway_id));
    RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER check_mqtt_replacement_disposition AFTER INSERT ON mqtt_rotation_replacements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_replacement_inventory();
CREATE CONSTRAINT TRIGGER check_mqtt_replacement_event AFTER INSERT OR UPDATE ON gateway_mqtt_credential_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_replacement_inventory();
CREATE CONSTRAINT TRIGGER check_mqtt_replacement_projection AFTER INSERT OR UPDATE ON gateway_mqtt_credentials DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_replacement_inventory();
CREATE CONSTRAINT TRIGGER check_mqtt_replacement_checkpoint AFTER INSERT OR UPDATE OR DELETE ON mqtt_credential_maintenance DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_mqtt_replacement_inventory();

GRANT SELECT ON mqtt_rotation_replacements TO iot_backend;
GRANT INSERT (predecessor_id,successor_id,gateway_id) ON mqtt_rotation_replacements TO iot_backend;
GRANT INSERT (replacement_predecessor_id) ON gateway_mqtt_credential_events TO iot_backend;
REVOKE ALL ON FUNCTION validate_mqtt_replacement_inventory(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION validate_mqtt_replacement_inventory(text) TO iot_backend;
INSERT INTO schema_migrations(version,name) VALUES(17,'mqtt_rotation_replacement');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=17 AND name='mqtt_rotation_replacement') THEN RAISE EXCEPTION 'Unexpected migration version 17'; END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
