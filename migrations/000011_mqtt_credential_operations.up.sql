-- Business intent and safe observations only; never native credentials or secrets.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SELECT NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 11) AS apply_credential_operations \gset
\if :apply_credential_operations
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 10 AND name = 'stage2_auth_and_provisioning') THEN
        RAISE EXCEPTION 'Migration 000010 is required';
    END IF;
END $$;

ALTER TABLE gateway_mqtt_credentials DROP CONSTRAINT gateway_mqtt_credentials_status_check;
ALTER TABLE gateway_mqtt_credentials ADD CONSTRAINT gateway_mqtt_credentials_status_check
    CHECK (status IN ('provisioning', 'active', 'rotating', 'revoking', 'revoked', 'failed', 'recovery_needed'));
ALTER TABLE gateway_mqtt_credentials DROP CONSTRAINT chk_gateway_mqtt_credentials_timestamps;
ALTER TABLE gateway_mqtt_credentials ADD CONSTRAINT chk_gateway_mqtt_credentials_timestamps CHECK (
    (status = 'active' AND activated_at IS NOT NULL AND revoked_at IS NULL)
    OR (status = 'revoked' AND revoked_at IS NOT NULL)
    OR (status IN ('provisioning', 'rotating', 'revoking') AND revoked_at IS NULL)
    OR status IN ('failed', 'recovery_needed')
);

-- Existing rows are explicitly historical projections, not verified DynSec jobs.
-- Multiple old pending rows are preserved unchanged. They ALL block new admission
-- on their Gateway until explicitly reconciled. Only modern jobs enter the unique
-- unresolved index; no arbitrary winner, fabricated failure or legacy data deletion.
ALTER TABLE gateway_mqtt_credential_events
    ADD COLUMN legacy_projection BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN idempotency_key UUID,
    ADD COLUMN previous_status TEXT,
    ADD COLUMN previous_credential_version BIGINT,
    ADD COLUMN previous_activated_at TIMESTAMPTZ,
    ADD COLUMN previous_revoked_at TIMESTAMPTZ,
    ADD COLUMN phase TEXT,
    ADD COLUMN ram_applied BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN snapshot_observed BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN fresh_positive_verified BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN updated_at TIMESTAMPTZ;
-- No trigger fires: existing immutable audit fields are not updated.
ALTER TABLE gateway_mqtt_credential_events ALTER COLUMN legacy_projection SET DEFAULT false;
ALTER TABLE gateway_mqtt_credential_events ALTER COLUMN phase SET DEFAULT 'intent';
ALTER TABLE gateway_mqtt_credential_events ALTER COLUMN updated_at SET DEFAULT now();
ALTER TABLE gateway_mqtt_credential_events DROP CONSTRAINT gateway_mqtt_credential_events_status_check;
ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT gateway_mqtt_credential_events_status_check
    CHECK (status IN ('pending', 'succeeded', 'failed', 'recovery_needed'));
ALTER TABLE gateway_mqtt_credential_events DROP CONSTRAINT chk_gateway_mqtt_credential_events_completion;
ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT chk_gateway_mqtt_credential_events_completion CHECK (
    (status IN ('pending', 'recovery_needed') AND completed_at IS NULL)
    OR (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
);
ALTER TABLE gateway_mqtt_credential_events ADD CONSTRAINT chk_mqtt_operation_contract CHECK (
    legacy_projection OR (
        operation_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND idempotency_key IS NOT NULL AND idempotency_key <> '00000000-0000-0000-0000-000000000000'::uuid
        AND action IN ('provision', 'rotate', 'revoke')
        AND credential_version IS NOT NULL AND credential_version > 0
        AND phase IS NOT NULL AND phase IN ('intent', 'dynsec_mutation', 'snapshot_readback', 'finalize', 'recovery')
        AND updated_at IS NOT NULL AND updated_at >= created_at
        AND (completed_at IS NULL OR (completed_at >= created_at AND updated_at >= completed_at))
        AND delivery_status IN ('unknown', 'device_updated')
        AND (status <> 'succeeded' OR (phase = 'finalize' AND ram_applied AND snapshot_observed AND (action = 'revoke' OR fresh_positive_verified)))
        AND (status <> 'failed' OR (phase = 'finalize' AND NOT ram_applied AND NOT snapshot_observed AND NOT fresh_positive_verified))
        AND (status <> 'recovery_needed' OR phase = 'recovery')
        AND ((previous_status IS NULL AND previous_credential_version IS NULL AND previous_activated_at IS NULL AND previous_revoked_at IS NULL)
            OR (previous_status IS NOT NULL AND previous_status IN ('provisioning', 'active', 'rotating', 'revoking', 'revoked', 'failed', 'recovery_needed') AND previous_credential_version IS NOT NULL AND previous_credential_version > 0))
        AND ((action = 'provision' AND (previous_status IS NULL OR previous_status IN ('revoked', 'failed')))
            OR (action IN ('rotate', 'revoke') AND previous_status IS NOT NULL AND previous_status = 'active'))
        AND (previous_status IS DISTINCT FROM 'active' OR (previous_activated_at IS NOT NULL AND previous_revoked_at IS NULL))
        AND (previous_status IS DISTINCT FROM 'revoked' OR previous_revoked_at IS NOT NULL)
        AND (previous_credential_version IS NULL OR
            (action = 'revoke' AND credential_version = previous_credential_version) OR
            (action <> 'revoke' AND credential_version > previous_credential_version))
    )
);
CREATE UNIQUE INDEX uq_mqtt_operation_actor_key ON gateway_mqtt_credential_events (actor_user_id, idempotency_key)
    WHERE NOT legacy_projection;
CREATE UNIQUE INDEX uq_mqtt_operation_unresolved ON gateway_mqtt_credential_events (gateway_id)
    WHERE NOT legacy_projection AND status IN ('pending', 'recovery_needed');

CREATE OR REPLACE FUNCTION enforce_gateway_mqtt_event_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.legacy_projection OR NEW.actor_user_id IS NULL
           OR NEW.actor_user_id = '00000000-0000-0000-0000-000000000000'::uuid
           OR NEW.status <> 'pending' OR NEW.phase <> 'intent'
           OR NEW.ram_applied OR NEW.snapshot_observed OR NEW.fresh_positive_verified
           OR NEW.delivery_status <> 'unknown' THEN
            RAISE EXCEPTION 'New operations require a live actor and unverified intent';
        END IF;
        -- Serializes admission with legacy unresolved rows, including duplicates.
        PERFORM 1 FROM gateways WHERE gateway_id = NEW.gateway_id FOR UPDATE;
        IF EXISTS (SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id = NEW.gateway_id AND status IN ('pending', 'recovery_needed')) THEN
            RAISE EXCEPTION 'Gateway has an unresolved credential operation';
        END IF;
        -- Failed attempted generations remain reserved in history. Revoke does
        -- not allocate a password generation. Gateway lock closes writer races.
        IF NEW.action IN ('provision', 'rotate') AND NEW.credential_version <= GREATEST(
            COALESCE((SELECT credential_version FROM gateway_mqtt_credentials WHERE gateway_id = NEW.gateway_id), 0),
            COALESCE((SELECT max(credential_version) FROM gateway_mqtt_credential_events WHERE gateway_id = NEW.gateway_id), 0)
        ) THEN
            RAISE EXCEPTION 'Credential generation must exceed current and attempted history';
        END IF;
        RETURN NEW;
    END IF;
    -- Narrow referential action exception. A real profile DELETE is required;
    -- actor UPDATE is not granted to the application. No other field may change.
    IF OLD.actor_user_id IS NOT NULL AND NEW.actor_user_id IS NULL
       AND pg_trigger_depth() > 1
       AND NOT EXISTS (SELECT 1 FROM profiles WHERE id = OLD.actor_user_id)
       AND (to_jsonb(NEW) - 'actor_user_id') = (to_jsonb(OLD) - 'actor_user_id') THEN
        RETURN NEW;
    END IF;
    IF OLD.status IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'Completed MQTT credential events are immutable';
    END IF;
    IF (to_jsonb(NEW) - ARRAY['status', 'completed_at', 'error_code', 'phase', 'ram_applied', 'snapshot_observed', 'fresh_positive_verified', 'delivery_status', 'updated_at'])
       IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['status', 'completed_at', 'error_code', 'phase', 'ram_applied', 'snapshot_observed', 'fresh_positive_verified', 'delivery_status', 'updated_at']) THEN
        RAISE EXCEPTION 'Credential operation identity and previous state are immutable';
    END IF;
    IF OLD.legacy_projection THEN
        -- Keep legacy pending -> terminal semantics, but never invent proof.
        IF OLD.status <> 'pending' OR NEW.status NOT IN ('succeeded', 'failed')
           OR NEW.ram_applied OR NEW.snapshot_observed OR NEW.fresh_positive_verified
           OR NEW.phase IS DISTINCT FROM OLD.phase OR NEW.delivery_status <> OLD.delivery_status THEN
            RAISE EXCEPTION 'Invalid historical projection transition';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.updated_at < OLD.updated_at
       OR (OLD.ram_applied AND NOT NEW.ram_applied)
       OR (OLD.snapshot_observed AND NOT NEW.snapshot_observed)
       OR (OLD.fresh_positive_verified AND NOT NEW.fresh_positive_verified)
       OR (OLD.status = 'recovery_needed' AND NEW.status NOT IN ('recovery_needed', 'succeeded'))
       OR (NEW.status = 'failed' AND (OLD.ram_applied OR OLD.snapshot_observed OR OLD.fresh_positive_verified)) THEN
        RAISE EXCEPTION 'Invalid credential operation recovery or evidence transition';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER enforce_gateway_mqtt_event_transition ON gateway_mqtt_credential_events;
CREATE TRIGGER enforce_gateway_mqtt_event_transition BEFORE INSERT OR UPDATE ON gateway_mqtt_credential_events
    FOR EACH ROW EXECUTE FUNCTION enforce_gateway_mqtt_event_transition();

GRANT UPDATE (status, completed_at, error_code, phase, ram_applied, snapshot_observed,
    fresh_positive_verified, delivery_status, updated_at) ON gateway_mqtt_credential_events TO iot_backend;
-- Discriminator is migration-owned; INSERT cannot fabricate a legacy bypass.
REVOKE INSERT ON gateway_mqtt_credential_events FROM iot_backend, iot_backend_app;
GRANT INSERT (operation_id, gateway_id, credential_version, action, status, actor_user_id,
    request_id, created_at, idempotency_key, previous_status, previous_credential_version,
    previous_activated_at, previous_revoked_at, phase, updated_at)
    ON gateway_mqtt_credential_events TO iot_backend;
INSERT INTO schema_migrations (version, name) VALUES (11, 'mqtt_credential_operations');
COMMIT;
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 11 AND name = 'mqtt_credential_operations') THEN
        RAISE EXCEPTION 'Migration version 11 has an unexpected name';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
