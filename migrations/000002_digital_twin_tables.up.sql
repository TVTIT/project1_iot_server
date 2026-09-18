-- 2. Digital Twin Tables & Hypertables (Section 10.4 of AGENTS.md)

-- 1. Twin Entities: Managed Gateways, Sensors, and Controllable Devices
CREATE TABLE IF NOT EXISTS twin_entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id TEXT UNIQUE NOT NULL,      -- Stable URN, e.g. urn:ngsi-ld:Gateway:gateway_001
    entity_type TEXT NOT NULL,            -- Gateway, Sensor, Device
    name TEXT,
    attributes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_twin_entities_type ON twin_entities (entity_type);

-- 2. Twin Relationships: Relationships such as hasSensor, connectedTo, controls, managedBy
CREATE TABLE IF NOT EXISTS twin_relationships (
    source_entity_id UUID NOT NULL REFERENCES twin_entities(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL,
    target_entity_id UUID NOT NULL REFERENCES twin_entities(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_entity_id, relationship_type, target_entity_id)
);

CREATE INDEX IF NOT EXISTS idx_twin_relationships_target ON twin_relationships (target_entity_id, relationship_type);

-- 3. Twin States: Authoritative latest reported and desired states
CREATE TABLE IF NOT EXISTS twin_states (
    entity_id UUID PRIMARY KEY REFERENCES twin_entities(id) ON DELETE CASCADE,
    reported_state JSONB NOT NULL DEFAULT '{}',
    desired_state JSONB NOT NULL DEFAULT '{}',
    reported_version BIGINT NOT NULL DEFAULT 0,
    desired_version BIGINT NOT NULL DEFAULT 0,
    last_reported_at TIMESTAMPTZ,
    last_desired_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4. Twin Commands: Downlink configuration & control actions
CREATE TABLE IF NOT EXISTS twin_commands (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id UUID UNIQUE NOT NULL,
    entity_id UUID NOT NULL REFERENCES twin_entities(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending', -- pending, published, acknowledged, succeeded, failed, timeout, cancelled
    desired_version BIGINT,
    idempotency_key TEXT,
    issued_by UUID REFERENCES profiles(id) ON DELETE SET NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_twin_commands_entity_status ON twin_commands (entity_id, status);
CREATE INDEX IF NOT EXISTS idx_twin_commands_idempotency ON twin_commands (entity_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- 5. Twin Outbox: Transactional Outbox for reliable MQTT publication
CREATE TABLE IF NOT EXISTS twin_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id UUID NOT NULL REFERENCES twin_commands(command_id) ON DELETE CASCADE,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending, published, failed
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_twin_outbox_pending ON twin_outbox (status, next_attempt_at) WHERE status = 'pending';

-- 6. Twin Temporal Values: Append-only property observations & inference history (TimescaleDB Hypertable)
CREATE TABLE IF NOT EXISTS twin_temporal_values (
    observed_at TIMESTAMPTZ NOT NULL,
    entity_id UUID NOT NULL REFERENCES twin_entities(id) ON DELETE CASCADE,
    property_name TEXT NOT NULL,
    value_number DOUBLE PRECISION,
    value_text TEXT,
    value_boolean BOOLEAN,
    metadata JSONB NOT NULL DEFAULT '{}',
    message_id UUID
);

-- Hypertable with 1-day chunk interval
SELECT create_hypertable(
    'twin_temporal_values',
    'observed_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_twin_temporal_entity_prop_time 
ON twin_temporal_values (entity_id, property_name, observed_at DESC);

-- Enable columnar compression
ALTER TABLE twin_temporal_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id, property_name',
    timescaledb.compress_orderby = 'observed_at DESC'
);

SELECT add_compression_policy('twin_temporal_values', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('twin_temporal_values', INTERVAL '30 days', if_not_exists => TRUE);
