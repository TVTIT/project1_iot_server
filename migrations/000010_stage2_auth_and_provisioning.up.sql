-- Add Stage 2 administration, MQTT credential metadata, identifier constraints,
-- and least-privilege grants for the Go backend database role.
\set ON_ERROR_STOP on

BEGIN;

SET LOCAL lock_timeout = '10s';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM schema_migrations
        WHERE version = 10
          AND name <> 'stage2_auth_and_provisioning'
    ) THEN
        RAISE EXCEPTION 'Migration version 10 has an unexpected name';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iot_backend') THEN
        RAISE EXCEPTION 'Role iot_backend must be provisioned before migration 000010';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gateways
        WHERE gateway_id !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
           OR gateway_id = 'backend_service'
    ) THEN
        RAISE EXCEPTION 'Existing gateway_id violates the Stage 2 identifier contract';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM sensors
        WHERE sensor_id !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
    ) THEN
        RAISE EXCEPTION 'Existing sensor_id violates the Stage 2 identifier contract';
    END IF;
END
$$;

CREATE TABLE platform_admins (
    user_id UUID PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by UUID REFERENCES profiles(id) ON DELETE SET NULL
);

CREATE TABLE gateway_mqtt_credentials (
    gateway_id TEXT PRIMARY KEY REFERENCES gateways(gateway_id) ON DELETE RESTRICT,
    credential_version BIGINT NOT NULL CHECK (credential_version > 0),
    status TEXT NOT NULL CHECK (status IN (
        'provisioning',
        'active',
        'rotating',
        'revoking',
        'revoked',
        'failed'
    )),
    last_operation_id UUID,
    changed_by UUID REFERENCES profiles(id) ON DELETE SET NULL,
    activated_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error_code TEXT,
    CONSTRAINT chk_gateway_mqtt_credentials_timestamps CHECK (
        (status = 'active' AND activated_at IS NOT NULL AND revoked_at IS NULL)
        OR (status = 'revoked' AND revoked_at IS NOT NULL)
        OR (status IN ('provisioning', 'rotating', 'revoking') AND revoked_at IS NULL)
        OR status = 'failed'
    )
);

CREATE TABLE gateway_mqtt_credential_events (
    operation_id UUID PRIMARY KEY,
    gateway_id TEXT NOT NULL REFERENCES gateways(gateway_id) ON DELETE RESTRICT,
    credential_version BIGINT CHECK (credential_version > 0),
    action TEXT NOT NULL CHECK (action IN ('provision', 'rotate', 'revoke', 'recover')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    actor_user_id UUID REFERENCES profiles(id) ON DELETE SET NULL,
    request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    error_code TEXT,
    CONSTRAINT chk_gateway_mqtt_credential_events_completion CHECK (
        (status = 'pending' AND completed_at IS NULL)
        OR (status IN ('succeeded', 'failed') AND completed_at IS NOT NULL)
    ),
    CONSTRAINT uq_gateway_mqtt_events_gateway_operation UNIQUE (gateway_id, operation_id)
);

CREATE FUNCTION enforce_gateway_mqtt_event_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status <> 'pending' THEN
        RAISE EXCEPTION 'Completed MQTT credential events are immutable';
    END IF;
    IF NEW.operation_id <> OLD.operation_id
       OR NEW.gateway_id <> OLD.gateway_id
       OR NEW.credential_version IS DISTINCT FROM OLD.credential_version
       OR NEW.action <> OLD.action
       OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id
       OR NEW.request_id IS DISTINCT FROM OLD.request_id
       OR NEW.created_at <> OLD.created_at
       OR NEW.status NOT IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'Invalid MQTT credential event transition';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER enforce_gateway_mqtt_event_transition
BEFORE UPDATE ON gateway_mqtt_credential_events
FOR EACH ROW EXECUTE FUNCTION enforce_gateway_mqtt_event_transition();

ALTER TABLE gateway_mqtt_credentials
    ADD CONSTRAINT fk_gateway_mqtt_credentials_last_operation
    FOREIGN KEY (gateway_id, last_operation_id)
    REFERENCES gateway_mqtt_credential_events (gateway_id, operation_id)
    ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_user_gateways_gateway_user
    ON user_gateways (gateway_id, user_id);

CREATE INDEX IF NOT EXISTS idx_gateway_mqtt_credential_events_gateway_created
    ON gateway_mqtt_credential_events (gateway_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_platform_admins_granted_by
    ON platform_admins (granted_by) WHERE granted_by IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gateway_mqtt_credentials_changed_by
    ON gateway_mqtt_credentials (changed_by) WHERE changed_by IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gateway_mqtt_credential_events_actor
    ON gateway_mqtt_credential_events (actor_user_id) WHERE actor_user_id IS NOT NULL;

DO $$
BEGIN
    ALTER TABLE gateways DROP CONSTRAINT IF EXISTS chk_gateways_identifier;
    ALTER TABLE gateways
        ADD CONSTRAINT chk_gateways_identifier
        CHECK (
            gateway_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'
            AND gateway_id <> 'backend_service'
        );

    ALTER TABLE sensors DROP CONSTRAINT IF EXISTS chk_sensors_identifier;
    ALTER TABLE sensors
        ADD CONSTRAINT chk_sensors_identifier
        CHECK (sensor_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$');
END
$$;

ALTER TABLE platform_admins ENABLE ROW LEVEL SECURITY;
ALTER TABLE gateway_mqtt_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE gateway_mqtt_credential_events ENABLE ROW LEVEL SECURITY;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM iot_backend;
REVOKE ALL ON SCHEMA public FROM iot_backend_app;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM iot_backend, iot_backend_app;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM iot_backend, iot_backend_app;
GRANT USAGE ON SCHEMA public TO iot_backend;

GRANT SELECT ON TABLE profiles, platform_admins TO iot_backend;
GRANT SELECT, INSERT, UPDATE ON TABLE
    gateways, user_gateways, sensors, media_objects, twin_entities,
    twin_relationships, twin_states, twin_commands, twin_outbox,
    gateway_mqtt_credentials
TO iot_backend;
GRANT SELECT, INSERT ON TABLE
    processed_messages, telemetry, twin_temporal_values,
    gateway_mqtt_credential_events
TO iot_backend;
GRANT UPDATE (status, completed_at, error_code)
    ON gateway_mqtt_credential_events TO iot_backend;

INSERT INTO schema_migrations (version, name)
VALUES (10, 'stage2_auth_and_provisioning')
ON CONFLICT (version) DO NOTHING;

COMMIT;
