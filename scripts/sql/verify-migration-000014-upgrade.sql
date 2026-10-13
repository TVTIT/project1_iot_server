\set ON_ERROR_STOP on
BEGIN;
-- Copy real upgraded evidence, without triggers, for rollback-only fault probes.
-- No historical application row is updated or exempted by this verifier.
CREATE TEMP TABLE fixture14_events AS SELECT * FROM public.fixture14_events;
CREATE TEMP TABLE fixture14_maintenance AS SELECT * FROM public.fixture14_maintenance;
CREATE TEMP TABLE fixture14_authority AS SELECT * FROM public.fixture14_authority;
CREATE TEMP TABLE gateway_mqtt_credential_events AS SELECT * FROM public.gateway_mqtt_credential_events;
CREATE TEMP TABLE mqtt_credential_maintenance AS SELECT * FROM public.mqtt_credential_maintenance;
CREATE TEMP TABLE gateway_mqtt_credentials AS SELECT * FROM public.gateway_mqtt_credentials;
CREATE TEMP TABLE mqtt_credential_recovery AS SELECT * FROM public.mqtt_credential_recovery;
CREATE TEMP TABLE fixture14_counts AS
    SELECT (SELECT count(*) FROM fixture14_events) AS events,
           (SELECT count(*) FROM fixture14_maintenance) AS maintenance,
           (SELECT count(*) FROM fixture14_authority) AS authority;

CREATE FUNCTION pg_temp.assert_fixture14_history() RETURNS void
LANGUAGE plpgsql SET search_path = pg_temp, public AS $$ BEGIN
    IF EXISTS (SELECT 1 FROM fixture14_events f LEFT JOIN gateway_mqtt_credential_events e USING(operation_id)
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(e) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key
        ) snapshot
        WHERE e.operation_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original
           OR e.recovery_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM fixture14_maintenance f LEFT JOIN mqtt_credential_maintenance p USING(operation_id)
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(p) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key
        ) snapshot
        WHERE p.operation_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original
           OR p.recovery_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM fixture14_authority f LEFT JOIN gateway_mqtt_credentials c USING(gateway_id)
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(key, to_jsonb(c) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key
        ) snapshot
        WHERE c.gateway_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture14_counts
           WHERE events = 0 OR maintenance = 0 OR authority = 0
              OR events <> (SELECT count(*) FROM fixture14_events)
              OR maintenance <> (SELECT count(*) FROM fixture14_maintenance)
              OR authority <> (SELECT count(*) FROM fixture14_authority))
       OR EXISTS (SELECT 1 FROM mqtt_credential_recovery) THEN
        RAISE EXCEPTION USING ERRCODE = 'P0014',
            MESSAGE = 'v13 upgrade changed historical evidence or fabricated recovery';
    END IF;
END $$;

-- The first call checks copies of the actual upgraded rows. Additive schema
-- columns are allowed, but every original key, identity and recovery is checked.
SELECT pg_temp.assert_fixture14_history();
DO $$
DECLARE
    relation record;
    evidence record;
    original_key text;
BEGIN
    FOR relation IN SELECT * FROM (VALUES
        ('fixture14_events', 'gateway_mqtt_credential_events', 'operation_id'),
        ('fixture14_maintenance', 'mqtt_credential_maintenance', 'operation_id'),
        ('fixture14_authority', 'gateway_mqtt_credentials', 'gateway_id')
    ) AS tables(fixture, current_table, identity_column) LOOP
        FOR evidence IN EXECUTE format('SELECT %I::text AS identity, original FROM %I',
            relation.identity_column, relation.fixture) LOOP
            -- Mutating the expected side tests all historical fields, including
            -- nulls, without requiring schema-specific casts or bypassing guards.
            FOR original_key IN SELECT jsonb_object_keys(evidence.original) LOOP
                BEGIN
                    EXECUTE format('UPDATE %I SET original = jsonb_set(original, ARRAY[$1], $2) WHERE %I::text = $3',
                        relation.fixture, relation.identity_column)
                    USING original_key,
                        CASE WHEN evidence.original -> original_key = '"history_fault"'::jsonb
                             THEN '"different_fault"'::jsonb ELSE '"history_fault"'::jsonb END,
                        evidence.identity;
                    PERFORM pg_temp.assert_fixture14_history();
                    RAISE EXCEPTION 'History fault unexpectedly accepted: %.%', relation.fixture, original_key;
                EXCEPTION WHEN SQLSTATE 'P0014' THEN NULL;
                END;
            END LOOP;
            BEGIN
                EXECUTE format('DELETE FROM %I WHERE %I::text = $1',
                    relation.current_table, relation.identity_column) USING evidence.identity;
                PERFORM pg_temp.assert_fixture14_history();
                RAISE EXCEPTION 'History fault unexpectedly accepted: deleted % row', relation.current_table;
            EXCEPTION WHEN SQLSTATE 'P0014' THEN NULL;
            END;
        END LOOP;
        BEGIN
            EXECUTE format('DELETE FROM %I', relation.fixture);
            PERFORM pg_temp.assert_fixture14_history();
            RAISE EXCEPTION 'History fault unexpectedly accepted: empty % snapshot', relation.fixture;
        EXCEPTION WHEN SQLSTATE 'P0014' THEN NULL;
        END;
    END LOOP;
    BEGIN
        INSERT INTO mqtt_credential_recovery (recovery_id)
        VALUES ('14000000-0000-4000-8000-000000000001');
        PERFORM pg_temp.assert_fixture14_history();
        RAISE EXCEPTION 'History fault unexpectedly accepted: fabricated recovery';
    EXCEPTION WHEN SQLSTATE 'P0014' THEN NULL;
    END;
    PERFORM pg_temp.assert_fixture14_history();
    RAISE NOTICE 'v14 historical snapshot regressions passed';
END $$;
ROLLBACK;
