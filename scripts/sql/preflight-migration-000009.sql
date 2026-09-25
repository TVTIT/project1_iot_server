\set ON_ERROR_STOP on

BEGIN TRANSACTION READ ONLY;

DO $$
BEGIN
    IF to_regclass('public.schema_migrations') IS NOT NULL THEN
        RAISE EXCEPTION 'Preflight failed: schema_migrations already exists; inspect its versions before applying 000009';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE gateway_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a Digital Twin has no owning Gateway';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE entity_type = 'Sensor'
          AND entity_id NOT LIKE 'urn:ngsi-ld:Sensor:%'
    ) THEN
        RAISE EXCEPTION 'Preflight failed: a Sensor twin has an unsupported entity_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE entity_type = 'Sensor'
        GROUP BY gateway_id,
                 substring(entity_id FROM length('urn:ngsi-ld:Sensor:') + 1)
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Preflight failed: duplicate Sensor twin IDs exist inside one Gateway';
    END IF;
END
$$;

ROLLBACK;

\echo 'Migration 000009 preflight passed.'
