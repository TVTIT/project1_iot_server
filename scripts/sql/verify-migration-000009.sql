\set ON_ERROR_STOP on

BEGIN TRANSACTION READ ONLY;

DO $$
DECLARE
    migration_count INTEGER;
BEGIN
    IF to_regclass('public.schema_migrations') IS NULL THEN
        RAISE EXCEPTION 'Verification failed: schema_migrations does not exist';
    END IF;

    SELECT count(*)
    INTO migration_count
    FROM schema_migrations
    WHERE version = ANY (ARRAY[1, 2, 3, 6, 7, 8, 9]::BIGINT[]);

    IF migration_count <> 7 THEN
        RAISE EXCEPTION 'Verification failed: expected 7 core migration versions, found %', migration_count;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE entity_type = 'Sensor'
          AND strpos(entity_id, 'urn:ngsi-ld:Sensor:' || gateway_id || ':') <> 1
    ) THEN
        RAISE EXCEPTION 'Verification failed: a Sensor twin does not use the Gateway-scoped URN';
    END IF;
END
$$;

ROLLBACK;

\echo 'Migration 000009 verification passed.'
