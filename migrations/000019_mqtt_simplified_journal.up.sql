-- Forward-only schema transition for simplified MQTT credential journal and narrow reset admission.
-- Preserves all existing event, projection, checkpoint, recovery, and replacement data and immutable terminal history.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;
SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 19) AS apply_simplified_journal \gset
\if :apply_simplified_journal
BEGIN;
SET LOCAL lock_timeout = '10s';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 18 AND name = 'mqtt_guard_search_path') THEN
        RAISE EXCEPTION 'Migration 000018 is required';
    END IF;
    -- public schema must remain trusted; fail if unsafe CREATE privileges exist
    IF has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
                  WHERE n.nspname = 'public' AND a.grantee = 0 AND a.privilege_type = 'CREATE') THEN
        RAISE EXCEPTION 'Simplified journal migration requires trusted public schema';
    END IF;
END $$;

-- 1. Create narrow reset dispositions sidecar table for audit-preserving reset links.
CREATE TABLE public.mqtt_reset_dispositions (
    predecessor_id uuid PRIMARY KEY REFERENCES public.gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT,
    successor_id uuid NOT NULL UNIQUE REFERENCES public.gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    gateway_id text NOT NULL REFERENCES public.gateways(gateway_id) ON DELETE RESTRICT,
    reason text NOT NULL CHECK (reason IN ('recovery_after_unknown', 'lost_secret_recovery')),
    disposition text NOT NULL DEFAULT 'reset_superseded' CHECK (disposition = 'reset_superseded'),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (predecessor_id <> successor_id)
);

-- 2. Add reset and execution_contract columns to gateway_mqtt_credential_events.
ALTER TABLE public.gateway_mqtt_credential_events
    ADD COLUMN execution_contract text NOT NULL DEFAULT 'lifecycle' CHECK (execution_contract IN ('lifecycle', 'simplified')),
    ADD COLUMN reset_predecessor_id uuid REFERENCES public.gateway_mqtt_credential_events(operation_id) ON DELETE RESTRICT,
    ADD COLUMN reset_reason text;

-- Add check constraint for reset columns identity and correlation.
ALTER TABLE public.gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_reset_identity CHECK (
    (reset_predecessor_id IS NULL AND reset_reason IS NULL)
    OR (
        NOT legacy_projection
        AND execution_contract = 'simplified'
        AND action = 'reset'
        AND reset_predecessor_id IS NOT NULL
        AND reset_predecessor_id <> operation_id
        AND reset_reason IS NOT NULL
        AND reset_reason IN ('recovery_after_unknown', 'lost_secret_recovery')
        AND recovery_id IS NULL
        AND replacement_predecessor_id IS NULL
    )
);

-- 3. Update table check constraint for action and replace chk_mqtt_operation_contract with explicit complete definition.
ALTER TABLE public.gateway_mqtt_credential_events DROP CONSTRAINT gateway_mqtt_credential_events_action_check;
ALTER TABLE public.gateway_mqtt_credential_events ADD CONSTRAINT gateway_mqtt_credential_events_action_check
    CHECK (action IN ('provision', 'rotate', 'revoke', 'recover', 'reset'));

-- Drop old constraint first.
ALTER TABLE public.gateway_mqtt_credential_events DROP CONSTRAINT chk_mqtt_operation_contract;

ALTER TABLE public.gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_operation_contract CHECK (
    legacy_projection OR (
        operation_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND idempotency_key IS NOT NULL AND idempotency_key <> '00000000-0000-0000-0000-000000000000'::uuid
        AND action IN ('provision', 'rotate', 'revoke', 'reset')
        AND execution_contract IN ('lifecycle', 'simplified')
        AND (action <> 'reset' OR execution_contract = 'simplified')
        AND credential_version IS NOT NULL AND credential_version > 0
        AND phase IS NOT NULL AND phase IN ('intent', 'dynsec_mutation', 'snapshot_readback', 'finalize', 'recovery')
        AND updated_at IS NOT NULL AND updated_at >= created_at
        AND (completed_at IS NULL OR (completed_at >= created_at AND updated_at >= completed_at))
        AND delivery_status IN ('unknown', 'device_updated')
        -- Terminal evidence rules:
        -- Success requires RAM applied and fresh login positive verified (except revoke which verifies absence).
        -- Simplified finalization does not mandate snapshot_observed.
        AND (
            status <> 'succeeded' OR (
                phase = 'finalize' AND ram_applied
                AND (action = 'revoke' OR fresh_positive_verified)
                AND (execution_contract <> 'lifecycle' OR snapshot_observed)
            )
        )
        -- Failed requires no positive observations.
        AND (status <> 'failed' OR (phase = 'finalize' AND NOT ram_applied AND NOT snapshot_observed AND NOT fresh_positive_verified))
        -- Recovery-needed requires recovery phase.
        AND (status <> 'recovery_needed' OR phase = 'recovery')
        -- Previous projection baseline check:
        AND (
            (previous_status IS NULL AND previous_credential_version IS NULL AND previous_activated_at IS NULL AND previous_revoked_at IS NULL)
            OR (previous_status IS NOT NULL AND previous_status IN ('provisioning', 'active', 'rotating', 'revoking', 'revoked', 'failed', 'recovery_needed') AND previous_credential_version IS NOT NULL AND previous_credential_version > 0)
        )
        -- Allowed previous status per action:
        AND (
            (action = 'provision' AND (previous_status IS NULL OR previous_status IN ('revoked', 'failed')))
            OR (action = 'revoke' AND previous_status IS NOT NULL AND previous_status = 'active')
            OR (action = 'rotate' AND (previous_status = 'active' OR replacement_predecessor_id IS NOT NULL))
            OR (action = 'reset' AND reset_predecessor_id IS NOT NULL AND previous_status IS NOT NULL)
        )
        AND (previous_status IS DISTINCT FROM 'active' OR (previous_activated_at IS NOT NULL AND previous_revoked_at IS NULL))
        AND (previous_status IS DISTINCT FROM 'revoked' OR previous_revoked_at IS NOT NULL)
        -- Version monotonicity per action:
        AND (
            previous_credential_version IS NULL
            OR (action = 'revoke' AND credential_version = previous_credential_version)
            OR (action <> 'revoke' AND credential_version > previous_credential_version)
        )
    )
);

-- 4. Update mqtt_operation_effective to exclude predecessor_ids from both replacement and reset sidecars.
CREATE OR REPLACE FUNCTION public.mqtt_operation_effective(target uuid) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT NOT EXISTS (
        SELECT 1 FROM public.mqtt_rotation_replacements WHERE predecessor_id = target
        UNION ALL
        SELECT 1 FROM public.mqtt_reset_dispositions WHERE predecessor_id = target
    )
$$;

-- 5. Helper function for locking platform admin (consistent with lock_mqtt_replacement_admin).
CREATE OR REPLACE FUNCTION public.lock_mqtt_reset_admin(target uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    PERFORM 1 FROM public.platform_admins WHERE user_id = target FOR SHARE;
    RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION public.lock_mqtt_reset_admin(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.lock_mqtt_reset_admin(uuid) TO iot_backend;

-- 6. Trigger functions for reset dispositions and events.
CREATE OR REPLACE FUNCTION public.enforce_mqtt_reset_disposition() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    pred_event public.gateway_mqtt_credential_events;
    curr_cred public.gateway_mqtt_credentials;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Reset disposition is immutable';
    END IF;

    -- Serialize on gateway row
    PERFORM 1 FROM public.gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE;

    -- Find the exact predecessor operation and verify it is currently referenced by gateway_mqtt_credentials
    SELECT * INTO curr_cred FROM public.gateway_mqtt_credentials
      WHERE gateway_id = NEW.gateway_id AND last_operation_id = NEW.predecessor_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Reset requires current predecessor projection';
    END IF;

    SELECT * INTO pred_event FROM public.gateway_mqtt_credential_events
      WHERE operation_id = NEW.predecessor_id AND gateway_id = NEW.gateway_id;
    IF NOT FOUND OR pred_event.legacy_projection OR pred_event.recovery_id IS NOT NULL THEN
        RAISE EXCEPTION 'Reset requires consistent modern predecessor event';
    END IF;

    -- Successor must not already exist as an event prior to disposition insertion
    IF EXISTS (SELECT 1 FROM public.gateway_mqtt_credential_events WHERE operation_id = NEW.successor_id) THEN
        RAISE EXCEPTION 'Reset successor already exists';
    END IF;

    -- Predecessor must not be under recovery table
    IF EXISTS (SELECT 1 FROM public.mqtt_credential_recovery WHERE operation_id = pred_event.operation_id) THEN
        RAISE EXCEPTION 'Reset cannot supersede an active recovery operation';
    END IF;

    -- Reason-specific admission checks:
    IF NEW.reason = 'recovery_after_unknown' THEN
        -- Predecessor must be pending or recovery_needed (unresolved)
        IF pred_event.status NOT IN ('pending', 'recovery_needed') THEN
            RAISE EXCEPTION 'recovery_after_unknown requires unresolved predecessor';
        END IF;
        IF curr_cred.status NOT IN ('provisioning', 'rotating', 'revoking', 'recovery_needed') THEN
            RAISE EXCEPTION 'recovery_after_unknown requires transient or recovery_needed credential';
        END IF;
    ELSIF NEW.reason = 'lost_secret_recovery' THEN
        -- Predecessor must be succeeded password-issuing action (provision or rotate or reset)
        IF pred_event.status <> 'succeeded' OR pred_event.action NOT IN ('provision', 'rotate', 'reset') THEN
            RAISE EXCEPTION 'lost_secret_recovery requires succeeded password-issuing predecessor';
        END IF;
        IF curr_cred.status <> 'active' THEN
            RAISE EXCEPTION 'lost_secret_recovery requires active credential';
        END IF;
    ELSE
        RAISE EXCEPTION 'Invalid reset reason';
    END IF;

    -- There must be no OTHER unresolved effective operations or pending recovery on this gateway
    IF EXISTS (
        SELECT 1 FROM public.gateway_mqtt_credential_events
        WHERE gateway_id = NEW.gateway_id
          AND operation_id <> pred_event.operation_id
          AND status IN ('pending', 'recovery_needed')
          AND recovery_id IS NULL
          AND public.mqtt_operation_effective(operation_id)
    ) OR EXISTS (
        SELECT 1 FROM public.mqtt_credential_recovery
        WHERE gateway_id = NEW.gateway_id AND status = 'pending'
    ) THEN
        RAISE EXCEPTION 'Reset requires absence of other unresolved operations';
    END IF;

    RETURN NEW;
END $$;

CREATE TRIGGER enforce_mqtt_reset_disposition
    BEFORE INSERT OR UPDATE OR DELETE ON public.mqtt_reset_dispositions
    FOR EACH ROW EXECUTE FUNCTION public.enforce_mqtt_reset_disposition();

-- Trigger on gateway_mqtt_credential_events to enforce reset event constraints and execution_contract immutability.
CREATE OR REPLACE FUNCTION public.enforce_mqtt_reset_event() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- Preserve narrow actor-delete exception
        IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL AND pg_trigger_depth() > 1
           AND NOT EXISTS (SELECT 1 FROM public.profiles WHERE id = OLD.actor_user_id)
           AND (to_jsonb(NEW) - 'actor_user_id') = (to_jsonb(OLD) - 'actor_user_id') THEN
            RETURN NEW;
        END IF;
        IF NEW.execution_contract IS DISTINCT FROM OLD.execution_contract THEN
            RAISE EXCEPTION 'Execution contract is immutable';
        END IF;
        IF NOT public.mqtt_operation_effective(OLD.operation_id) THEN
            RAISE EXCEPTION 'Superseded audit is immutable';
        END IF;
        RETURN NEW;
    END IF;

    -- On INSERT:
    IF NEW.action = 'reset' THEN
        IF NEW.execution_contract <> 'simplified' THEN
            RAISE EXCEPTION 'Reset action requires simplified execution contract';
        END IF;
        IF NEW.reset_predecessor_id IS NULL OR NEW.reset_reason IS NULL THEN
            RAISE EXCEPTION 'Reset event requires predecessor id and reason';
        END IF;
        IF NOT public.lock_mqtt_reset_admin(NEW.actor_user_id) THEN
            RAISE EXCEPTION 'Reset event requires platform admin actor';
        END IF;
        PERFORM 1 FROM public.gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE;
        IF NEW.credential_version <= GREATEST(
            COALESCE((SELECT credential_version FROM public.gateway_mqtt_credentials WHERE gateway_id = NEW.gateway_id), 0),
            COALESCE((SELECT max(credential_version) FROM public.gateway_mqtt_credential_events WHERE gateway_id = NEW.gateway_id), 0)
        ) THEN
            RAISE EXCEPTION 'Reset credential generation must exceed current and attempted history';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM public.mqtt_reset_dispositions d
            JOIN public.gateway_mqtt_credentials c ON c.gateway_id = d.gateway_id
            WHERE d.predecessor_id = NEW.reset_predecessor_id
              AND d.successor_id = NEW.operation_id
              AND d.gateway_id = NEW.gateway_id
              AND d.reason = NEW.reset_reason
              AND c.last_operation_id = d.predecessor_id
              AND ROW(c.status, c.credential_version, c.activated_at, c.revoked_at) IS NOT DISTINCT FROM
                  ROW(NEW.previous_status, NEW.previous_credential_version, NEW.previous_activated_at, NEW.previous_revoked_at)
        ) THEN
            RAISE EXCEPTION 'Reset intent requires exact disposition, previous projection, and live admin';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM public.mqtt_reset_dispositions WHERE successor_id = NEW.operation_id) THEN
        RAISE EXCEPTION 'Disposition requires reset intent';
    END IF;

    RETURN NEW;
END $$;

CREATE TRIGGER aa_mqtt_reset_event
    BEFORE INSERT OR UPDATE ON public.gateway_mqtt_credential_events
    FOR EACH ROW EXECUTE FUNCTION public.enforce_mqtt_reset_event();

-- 7. Update validate_mqtt_projection_authority to support simplified operations and reset action.
CREATE OR REPLACE FUNCTION public.validate_mqtt_projection_authority(target_gateway text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    c public.gateway_mqtt_credentials;
    e public.gateway_mqtt_credential_events;
    r public.mqtt_credential_recovery;
    expected_status text;
    expected_version bigint;
    activation timestamptz;
    revocation timestamptz;
    stamp timestamptz;
    error text;
BEGIN
    SELECT * INTO c FROM public.gateway_mqtt_credentials WHERE gateway_id = target_gateway;
    SELECT * INTO e FROM public.gateway_mqtt_credential_events
      WHERE operation_id = c.last_operation_id AND gateway_id = c.gateway_id AND NOT legacy_projection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Projection requires matching modern operation';
    END IF;
    IF c.changed_by IS DISTINCT FROM e.actor_user_id THEN
        RAISE EXCEPTION 'Projection actor mismatch';
    END IF;

    -- Branch by immutable execution_contract
    IF e.execution_contract = 'lifecycle' THEN
        -- Strictly preserve 000018 lifecycle invariants
        SELECT * INTO r FROM public.mqtt_credential_recovery WHERE operation_id = e.operation_id AND status = 'disabled';
        IF FOUND THEN
            IF NOT EXISTS (
                SELECT 1 FROM public.mqtt_credential_maintenance p
                WHERE p.operation_id = e.operation_id AND p.gateway_id = c.gateway_id
                  AND p.status = 'completed' AND p.recovery_id = r.recovery_id AND p.broker_epoch = r.broker_epoch
            ) OR NOT (e.status IN ('succeeded', 'failed') OR e.recovery_id = r.recovery_id) THEN
                RAISE EXCEPTION 'Projection requires completed recovery disposition';
            END IF;
            expected_status := 'revoked';
            expected_version := r.attempted_version;
            activation := CASE WHEN e.status = 'succeeded' AND e.action <> 'revoke' THEN e.updated_at ELSE e.previous_activated_at END;
            revocation := r.completed_at;
            stamp := r.completed_at;
            error := 'credential_recovery_required';
        ELSE
            activation := e.previous_activated_at;
            expected_version := e.credential_version;
            stamp := e.updated_at;
            error := e.error_code;
            CASE e.status
            WHEN 'pending' THEN
                expected_status := CASE e.action
                    WHEN 'provision' THEN 'provisioning'
                    WHEN 'rotate' THEN 'rotating'
                    ELSE 'revoking'
                END;
                error := NULL;
                stamp := e.created_at;
            WHEN 'succeeded' THEN
                IF NOT e.ram_applied OR NOT e.snapshot_observed OR (e.action <> 'revoke' AND NOT e.fresh_positive_verified) THEN
                    RAISE EXCEPTION 'Success requires verified observations';
                END IF;
                expected_status := CASE WHEN e.action = 'revoke' THEN 'revoked' ELSE 'active' END;
                IF e.action = 'revoke' THEN
                    revocation := e.updated_at;
                ELSE
                    activation := e.updated_at;
                END IF;
                error := NULL;
            WHEN 'failed' THEN
                expected_status := COALESCE(e.previous_status, 'failed');
                expected_version := COALESCE(e.previous_credential_version, e.credential_version);
                revocation := e.previous_revoked_at;
            WHEN 'recovery_needed' THEN
                expected_status := 'recovery_needed';
                expected_version := COALESCE(e.previous_credential_version, e.credential_version);
                revocation := e.previous_revoked_at;
            ELSE
                RAISE EXCEPTION 'Unsupported operation projection';
            END CASE;
            IF e.status <> 'pending' AND NOT EXISTS (
                SELECT 1 FROM public.mqtt_credential_maintenance p
                WHERE p.operation_id = e.operation_id AND p.gateway_id = c.gateway_id
            ) THEN
                RAISE EXCEPTION 'Final projection requires checkpoint';
            END IF;
        END IF;
    ELSE
        -- Simplified execution contract: ZERO maintenance rows, no synthetic maintenance/recovery
        IF EXISTS (
            SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = e.operation_id
        ) OR EXISTS (
            SELECT 1 FROM public.mqtt_credential_recovery WHERE operation_id = e.operation_id
        ) THEN
            RAISE EXCEPTION 'Simplified operation forbids maintenance or recovery records';
        END IF;

        activation := e.previous_activated_at;
        expected_version := e.credential_version;
        stamp := e.updated_at;
        error := e.error_code;
        CASE e.status
        WHEN 'pending' THEN
            expected_status := CASE e.action
                WHEN 'provision' THEN 'provisioning'
                WHEN 'rotate' THEN 'rotating'
                WHEN 'reset' THEN 'rotating'
                ELSE 'revoking'
            END;
            error := NULL;
            stamp := e.created_at;
        WHEN 'succeeded' THEN
            IF NOT e.ram_applied OR (e.action <> 'revoke' AND NOT e.fresh_positive_verified) THEN
                RAISE EXCEPTION 'Success requires verified observations';
            END IF;
            expected_status := CASE WHEN e.action = 'revoke' THEN 'revoked' ELSE 'active' END;
            IF e.action = 'revoke' THEN
                revocation := e.updated_at;
            ELSE
                activation := e.updated_at;
            END IF;
            error := NULL;
        WHEN 'failed' THEN
            expected_status := COALESCE(e.previous_status, 'failed');
            expected_version := COALESCE(e.previous_credential_version, e.credential_version);
            revocation := e.previous_revoked_at;
        WHEN 'recovery_needed' THEN
            expected_status := 'recovery_needed';
            expected_version := COALESCE(e.previous_credential_version, e.credential_version);
            revocation := e.previous_revoked_at;
        ELSE
            RAISE EXCEPTION 'Unsupported operation projection';
        END CASE;
    END IF;

    IF ROW(c.status, c.credential_version, c.activated_at, c.revoked_at, c.updated_at, c.last_error_code)
       IS DISTINCT FROM ROW(expected_status, expected_version, activation, revocation, stamp, error) THEN
        RAISE EXCEPTION 'Credential projection disagrees with operation evidence';
    END IF;
END $$;

-- Trigger makes every new modern intent + checkpoint atomic, including direct
-- app-role SQL writers. No network work and no cross-Gateway execution mutex.
-- Simplified operations create zero maintenance rows; lifecycle creates mandatory maintenance.
CREATE OR REPLACE FUNCTION public.create_mqtt_maintenance_intent() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF NEW.execution_contract = 'lifecycle' THEN
        INSERT INTO public.mqtt_credential_maintenance(operation_id, gateway_id)
        VALUES(NEW.operation_id, NEW.gateway_id);
    END IF;
    RETURN NEW;
END $$;

-- Guard direct admission into mqtt_credential_maintenance: block simplified events.
CREATE OR REPLACE FUNCTION public.enforce_mqtt_maintenance_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    ev public.gateway_mqtt_credential_events;
BEGIN
    PERFORM 1 FROM public.gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'in_progress' OR NEW.broker_epoch IS NOT NULL OR NEW.error_code IS NOT NULL OR NEW.recovery_id IS NOT NULL THEN
            RAISE EXCEPTION 'Maintenance requires matching modern intent';
        END IF;
        SELECT * INTO ev FROM public.gateway_mqtt_credential_events
          WHERE operation_id = NEW.operation_id AND gateway_id = NEW.gateway_id AND NOT legacy_projection AND status = 'pending';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Maintenance requires matching modern intent';
        END IF;
        IF ev.execution_contract <> 'lifecycle' THEN
            RAISE EXCEPTION 'Simplified operations cannot have maintenance checkpoints';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'completed' THEN
        RAISE EXCEPTION 'Completed maintenance is immutable';
    END IF;
    IF NEW.operation_id <> OLD.operation_id OR NEW.gateway_id <> OLD.gateway_id OR NEW.created_at <> OLD.created_at OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'Maintenance identity is immutable';
    END IF;
    IF NEW.recovery_id IS NOT NULL THEN
        IF NEW.status <> 'completed' OR NOT EXISTS (
            SELECT 1 FROM public.mqtt_credential_recovery r
            JOIN public.gateway_mqtt_credentials c ON c.gateway_id = r.gateway_id
            JOIN public.gateway_mqtt_credential_events e ON e.operation_id = r.operation_id
            WHERE r.recovery_id = NEW.recovery_id AND r.operation_id = NEW.operation_id AND r.gateway_id = NEW.gateway_id
              AND r.status = 'disabled' AND r.broker_epoch = NEW.broker_epoch AND c.last_operation_id = r.operation_id
              AND c.status = 'revoked' AND c.credential_version = r.attempted_version
              AND (e.status IN ('succeeded', 'failed') OR e.recovery_id = r.recovery_id)
        ) THEN
            RAISE EXCEPTION 'Recovered maintenance requires linked current disable';
        END IF;
    ELSIF EXISTS (SELECT 1 FROM public.mqtt_credential_recovery WHERE operation_id = NEW.operation_id) THEN
        RAISE EXCEPTION 'Recovery intent forbids ordinary maintenance completion or rebinding';
    ELSIF NEW.status = 'completed' AND NOT EXISTS (
        SELECT 1 FROM public.gateway_mqtt_credentials c
        JOIN public.gateway_mqtt_credential_events e ON e.operation_id = c.last_operation_id AND e.gateway_id = c.gateway_id
        WHERE c.gateway_id = NEW.gateway_id AND c.last_operation_id = NEW.operation_id AND e.status IN ('succeeded', 'failed')
    ) THEN
        RAISE EXCEPTION 'Maintenance completion requires current terminal operation';
    END IF;
    RETURN NEW;
END $$;

-- 8. Fix pre-existing ambiguous column reference in enforce_mqtt_recovery_transition
CREATE OR REPLACE FUNCTION public.enforce_mqtt_recovery_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    PERFORM 1 FROM public.gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' OR NEW.ram_disabled OR NEW.snapshot_observed OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'Recovery requires unverified intent';
        END IF;
        IF EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = NEW.operation_id AND broker_epoch = NEW.broker_epoch) THEN
            RAISE EXCEPTION 'Recovery requires a fresh broker lifetime';
        END IF;
    ELSE
        IF OLD.status = 'disabled' THEN RAISE EXCEPTION 'Completed recovery is immutable'; END IF;
        IF (to_jsonb(NEW) - ARRAY['status', 'broker_epoch', 'ram_disabled', 'snapshot_observed', 'updated_at', 'completed_at']) IS DISTINCT FROM
           (to_jsonb(OLD) - ARRAY['status', 'broker_epoch', 'ram_disabled', 'snapshot_observed', 'updated_at', 'completed_at']) OR NEW.updated_at < OLD.updated_at THEN
            RAISE EXCEPTION 'Recovery identity is immutable';
        END IF;
        IF NEW.status = 'disabled' AND NEW.broker_epoch <> OLD.broker_epoch THEN RAISE EXCEPTION 'Completion must match bound epoch'; END IF;
        IF NEW.broker_epoch <> OLD.broker_epoch AND EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = NEW.operation_id AND broker_epoch = NEW.broker_epoch) THEN
            RAISE EXCEPTION 'Recovery requires a fresh broker lifetime';
        END IF;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.gateway_mqtt_credentials c
        JOIN public.gateway_mqtt_credential_events e ON e.operation_id = c.last_operation_id AND e.gateway_id = c.gateway_id
        JOIN public.mqtt_credential_maintenance p ON p.operation_id = e.operation_id AND p.gateway_id = e.gateway_id
        WHERE c.gateway_id = NEW.gateway_id AND e.operation_id = NEW.operation_id AND NOT e.legacy_projection AND e.recovery_id IS NULL
          AND e.credential_version = NEW.attempted_version AND p.status <> 'completed' AND public.mqtt_operation_effective(p.operation_id)
          AND ((e.status = 'pending' AND c.credential_version = e.credential_version AND c.status = CASE e.action WHEN 'provision' THEN 'provisioning' WHEN 'rotate' THEN 'rotating' ELSE 'revoking' END)
            OR (e.status = 'recovery_needed' AND c.status = 'recovery_needed' AND c.credential_version = COALESCE(e.previous_credential_version, e.credential_version))
            OR (e.status = 'succeeded' AND c.credential_version = e.credential_version AND c.status = CASE WHEN e.action = 'revoke' THEN 'revoked' ELSE 'active' END)
            OR (e.status = 'failed' AND c.credential_version = COALESCE(e.previous_credential_version, e.credential_version) AND c.status = COALESCE(e.previous_status, 'failed')))) THEN
        RAISE EXCEPTION 'Recovery requires consistent current modern operation and unresolved checkpoint';
    END IF;
    IF EXISTS (SELECT 1 FROM public.gateway_mqtt_credential_events WHERE gateway_id = NEW.gateway_id AND operation_id <> NEW.operation_id AND status IN ('pending', 'recovery_needed') AND recovery_id IS NULL AND public.mqtt_operation_effective(operation_id))
       OR EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE gateway_id = NEW.gateway_id AND operation_id <> NEW.operation_id AND status <> 'completed' AND public.mqtt_operation_effective(operation_id)) THEN
        RAISE EXCEPTION 'Contradictory history requires operator diagnosis';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION public.check_mqtt_replacement_inventory() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF TG_TABLE_NAME = 'gateway_mqtt_credential_events' THEN
            IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM public.profiles WHERE id = OLD.actor_user_id)
                AND (to_jsonb(NEW) - 'actor_user_id') = (to_jsonb(OLD) - 'actor_user_id') THEN
                RETURN NEW;
            END IF;
        ELSIF TG_TABLE_NAME = 'gateway_mqtt_credentials' THEN
            IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL
                AND NOT EXISTS (SELECT 1 FROM public.profiles WHERE id = OLD.changed_by)
                AND (to_jsonb(NEW) - 'changed_by') = (to_jsonb(OLD) - 'changed_by') THEN
                RETURN NEW;
            END IF;
        END IF;
    END IF;

    IF TG_TABLE_NAME = 'gateway_mqtt_credential_events' AND TG_OP = 'INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM public.gateway_mqtt_credentials WHERE gateway_id = NEW.gateway_id AND last_operation_id = NEW.operation_id)
            AND NOT EXISTS (SELECT 1 FROM public.mqtt_rotation_replacements WHERE predecessor_id = NEW.operation_id AND gateway_id = NEW.gateway_id)
            AND NOT EXISTS (SELECT 1 FROM public.mqtt_reset_dispositions WHERE predecessor_id = NEW.operation_id AND gateway_id = NEW.gateway_id) THEN
            RAISE EXCEPTION 'New intent cannot commit as unassociated audit';
        END IF;
    END IF;

    PERFORM public.validate_mqtt_replacement_inventory(COALESCE(NEW.gateway_id, OLD.gateway_id));
    RETURN NEW;
END $$;

-- Bind deferred trigger on mqtt_reset_dispositions
CREATE CONSTRAINT TRIGGER check_mqtt_reset_disposition
    AFTER INSERT ON public.mqtt_reset_dispositions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_mqtt_replacement_inventory();

-- 9. Update validate_mqtt_replacement_inventory to recognize reset dispositions and operations.
CREATE OR REPLACE FUNCTION public.validate_mqtt_replacement_inventory(target text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.mqtt_rotation_replacements d
       LEFT JOIN public.gateway_mqtt_credential_events p ON p.operation_id = d.predecessor_id
       LEFT JOIN public.gateway_mqtt_credential_events s ON s.operation_id = d.successor_id
       LEFT JOIN public.mqtt_credential_maintenance pm ON pm.operation_id = p.operation_id
       LEFT JOIN public.mqtt_credential_maintenance sm ON sm.operation_id = s.operation_id
       WHERE d.gateway_id = target AND (p.operation_id IS NULL OR s.operation_id IS NULL OR pm.operation_id IS NULL OR sm.operation_id IS NULL
         OR p.gateway_id <> d.gateway_id OR s.gateway_id <> d.gateway_id OR pm.gateway_id <> d.gateway_id OR sm.gateway_id <> d.gateway_id
         OR p.legacy_projection OR s.legacy_projection OR p.action <> 'rotate' OR s.action <> 'rotate'
         OR s.replacement_predecessor_id IS DISTINCT FROM p.operation_id OR s.credential_version <= p.credential_version
         OR EXISTS (SELECT 1 FROM public.mqtt_credential_recovery WHERE operation_id IN (p.operation_id, s.operation_id)))) THEN
        RAISE EXCEPTION 'Invalid replacement event/checkpoint/disposition linkage';
    END IF;

    IF EXISTS (SELECT 1 FROM public.gateway_mqtt_credential_events e WHERE e.gateway_id = target AND e.replacement_predecessor_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM public.mqtt_rotation_replacements WHERE predecessor_id = e.replacement_predecessor_id AND successor_id = e.operation_id AND gateway_id = target)) THEN
        RAISE EXCEPTION 'Replacement intent has no disposition';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.mqtt_reset_dispositions d
        WHERE d.gateway_id = target
          AND (
            NOT EXISTS (SELECT 1 FROM public.gateway_mqtt_credential_events WHERE operation_id = d.predecessor_id AND gateway_id = target)
            OR NOT EXISTS (SELECT 1 FROM public.gateway_mqtt_credential_events WHERE operation_id = d.successor_id AND gateway_id = target)
          )
    ) THEN
        RAISE EXCEPTION 'Dangling reset disposition';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.gateway_mqtt_credentials c
        WHERE c.gateway_id = target AND NOT public.mqtt_operation_effective(c.last_operation_id)
    ) THEN
        RAISE EXCEPTION 'Superseded event cannot regain current authority';
    END IF;

    IF (
        SELECT count(*) FROM public.gateway_mqtt_credential_events
        WHERE gateway_id = target
          AND NOT legacy_projection
          AND status IN ('pending', 'recovery_needed')
          AND recovery_id IS NULL
          AND public.mqtt_operation_effective(operation_id)
    ) > 1 THEN
        RAISE EXCEPTION 'Duplicate effective operation';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.mqtt_credential_maintenance p
        WHERE p.gateway_id = target AND p.status <> 'completed' AND public.mqtt_operation_effective(p.operation_id)
          AND NOT EXISTS (SELECT 1 FROM public.gateway_mqtt_credentials WHERE gateway_id = target AND last_operation_id = p.operation_id)
    ) THEN
        RAISE EXCEPTION 'Effective checkpoint requires current authority';
    END IF;

    -- Newly admitted effective intents must be selected current in gateway_mqtt_credentials.
    -- For lifecycle operations, a maintenance checkpoint row is mandatory.
    -- For simplified operations, no maintenance checkpoint row exists.
    IF EXISTS (
        SELECT 1 FROM public.gateway_mqtt_credential_events e
        WHERE e.gateway_id = target
          AND NOT e.legacy_projection
          AND e.status IN ('pending', 'recovery_needed')
          AND e.recovery_id IS NULL
          AND public.mqtt_operation_effective(e.operation_id)
          AND (
            NOT EXISTS (SELECT 1 FROM public.gateway_mqtt_credentials WHERE gateway_id = target AND last_operation_id = e.operation_id)
            OR (e.execution_contract = 'lifecycle' AND NOT EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = e.operation_id AND gateway_id = target))
          )
    ) THEN
        RAISE EXCEPTION 'Effective intent requires current projection';
    END IF;

    PERFORM public.validate_mqtt_projection_authority(target);
END $$;

-- 10. Least-privilege permissions for iot_backend and iot_backend_app.
GRANT SELECT ON public.mqtt_reset_dispositions TO iot_backend;
GRANT INSERT (predecessor_id, successor_id, gateway_id, reason) ON public.mqtt_reset_dispositions TO iot_backend;
GRANT INSERT (execution_contract, reset_predecessor_id, reset_reason) ON public.gateway_mqtt_credential_events TO iot_backend;
REVOKE ALL ON FUNCTION public.lock_mqtt_reset_admin(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.lock_mqtt_reset_admin(uuid) TO iot_backend;
REVOKE ALL ON FUNCTION public.enforce_mqtt_reset_disposition() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.enforce_mqtt_reset_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.enforce_mqtt_maintenance_transition() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.create_mqtt_maintenance_intent() FROM PUBLIC;

-- 11. Record migration in schema_migrations.
INSERT INTO public.schema_migrations(version, name) VALUES(19, 'mqtt_simplified_journal');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 19 AND name = 'mqtt_simplified_journal') THEN
        RAISE EXCEPTION 'Unexpected migration version 19';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
