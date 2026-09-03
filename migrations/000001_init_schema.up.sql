-- Enable TimescaleDB and UUID extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Human User Profiles (references auth.users if Supabase Auth is enabled, standalone fallback if auth schema not ready)
CREATE SCHEMA IF NOT EXISTS auth;
CREATE TABLE IF NOT EXISTS auth.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    full_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Gateways & Authorization
CREATE TABLE IF NOT EXISTS gateways (
    gateway_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_gateways (
    user_id UUID REFERENCES profiles(id) ON DELETE CASCADE,
    gateway_id TEXT REFERENCES gateways(gateway_id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, gateway_id)
);

-- 3. Sensors
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id TEXT NOT NULL,
    gateway_id TEXT REFERENCES gateways(gateway_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    unit TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (gateway_id, sensor_id)
);

-- 4. Message Deduplication & Outbox Tracking
CREATE TABLE IF NOT EXISTS processed_messages (
    gateway_id TEXT NOT NULL,
    message_id UUID NOT NULL,
    payload_hash TEXT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (gateway_id, message_id)
);

-- 5. Telemetry Time-Series (TimescaleDB Hypertable)
CREATE TABLE IF NOT EXISTS telemetry (
    measured_at TIMESTAMPTZ NOT NULL,
    gateway_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    sequence_num BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (measured_at, gateway_id, sensor_id, sequence_num)
);

-- Turn telemetry into TimescaleDB Hypertable partitioned by measured_at
SELECT create_hypertable('telemetry', 'measured_at', if_not_exists => TRUE);

-- 6. Media Objects (Images/Files uploaded to Supabase Storage)
CREATE TABLE IF NOT EXISTS media_objects (
    media_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_id TEXT REFERENCES gateways(gateway_id) ON DELETE CASCADE,
    storage_path TEXT NOT NULL,
    media_type TEXT NOT NULL,
    expected_size BIGINT,
    actual_size BIGINT,
    checksum TEXT,
    validation_status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);
