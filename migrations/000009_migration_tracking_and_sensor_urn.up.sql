-- Establish migration tracking and make Sensor entity IDs unique per Gateway.
\set ON_ERROR_STOP on

BEGIN;

-- Sensor IDs are unique only inside one Gateway. The public twin entity ID
-- therefore includes both identifiers.
UPDATE twin_entities
SET entity_id = 'urn:ngsi-ld:Sensor:' || gateway_id || ':' ||
                substring(entity_id FROM length('urn:ngsi-ld:Sensor:') + 1),
    updated_at = now()
WHERE entity_type = 'Sensor'
  AND entity_id LIKE 'urn:ngsi-ld:Sensor:%'
  AND strpos(entity_id, 'urn:ngsi-ld:Sensor:' || gateway_id || ':') <> 1;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF to_regclass('public.gateways') IS NULL
       OR to_regclass('public.telemetry') IS NULL
       OR to_regclass('public.twin_entities') IS NULL
       OR to_regclass('public.twin_states') IS NULL
       OR to_regclass('public.twin_commands') IS NULL
       OR to_regclass('public.twin_outbox') IS NULL
       OR to_regclass('public.twin_temporal_values') IS NULL THEN
        RAISE EXCEPTION 'Cannot baseline: a required application table is missing';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')
       OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_twin_entities_gateway')
       OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_twin_commands_status')
       OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_twin_outbox_command_id')
       OR NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_twin_commands_idempotency') THEN
        RAISE EXCEPTION 'Cannot baseline: schema hardening is incomplete';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_auth_admin')
       OR NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_storage_admin') THEN
        RAISE EXCEPTION 'Cannot baseline: Supabase roles are missing';
    END IF;
END
$$;

INSERT INTO schema_migrations (version, name)
VALUES
    (1, 'init_schema'),
    (2, 'digital_twin_tables'),
    (3, 'supabase_roles'),
    (6, 'add_gateway_ownership_columns'),
    (7, 'backfill_twin_gateway_ownership'),
    (8, 'schema_hardening'),
    (9, 'migration_tracking_and_sensor_urn')
ON CONFLICT (version) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM schema_migrations
        WHERE (version, name) NOT IN (
            (1, 'init_schema'),
            (2, 'digital_twin_tables'),
            (3, 'supabase_roles'),
            (4, 'supabase_compat'),
            (6, 'add_gateway_ownership_columns'),
            (7, 'backfill_twin_gateway_ownership'),
            (8, 'schema_hardening'),
            (9, 'migration_tracking_and_sensor_urn')
        )
          AND version BETWEEN 1 AND 9
    ) THEN
        RAISE EXCEPTION 'Migration history contains a version/name mismatch';
    END IF;
END
$$;

-- Version 4 is recorded only by 000004 after all Auth FK, trigger and RLS
-- objects have been created. Version 5 is optional development seed data.

COMMIT;
