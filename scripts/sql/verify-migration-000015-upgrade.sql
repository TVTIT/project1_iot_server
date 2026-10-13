\set ON_ERROR_STOP on
BEGIN;
-- Scope event/authority counts to the seeded Gateways: the harness subsequently
-- admits a legitimate role probe on a different Gateway. Recovery stays global.
CREATE TEMP TABLE fixture15_authority AS SELECT * FROM public.fixture15_authority;
CREATE TEMP TABLE fixture15_events AS SELECT * FROM public.fixture15_events;
CREATE TEMP TABLE fixture15_recovery AS SELECT * FROM public.fixture15_recovery;
CREATE TEMP TABLE gateway_mqtt_credentials AS SELECT c.* FROM public.gateway_mqtt_credentials c
    WHERE gateway_id IN (SELECT gateway_id FROM fixture15_authority);
CREATE TEMP TABLE gateway_mqtt_credential_events AS SELECT e.* FROM public.gateway_mqtt_credential_events e
    WHERE gateway_id IN (SELECT original->>'gateway_id' FROM fixture15_events);
CREATE TEMP TABLE mqtt_credential_recovery AS SELECT * FROM public.mqtt_credential_recovery;
CREATE TEMP TABLE fixture15_counts AS SELECT
    (SELECT count(*) FROM fixture15_authority) AS authority,
    (SELECT count(*) FROM fixture15_events) AS events,
    (SELECT count(*) FROM fixture15_recovery) AS recovery;
CREATE FUNCTION pg_temp.assert_fixture15_history() RETURNS void
LANGUAGE plpgsql SET search_path = pg_temp, public AS $$ BEGIN
    IF EXISTS (SELECT 1 FROM fixture15_authority f LEFT JOIN gateway_mqtt_credentials c USING(gateway_id)
        CROSS JOIN LATERAL (SELECT jsonb_object_agg(key, to_jsonb(c) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key) snapshot
        WHERE c.gateway_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture15_events f LEFT JOIN gateway_mqtt_credential_events e USING(operation_id)
        CROSS JOIN LATERAL (SELECT jsonb_object_agg(key, to_jsonb(e) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key) snapshot
        WHERE e.operation_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture15_recovery f LEFT JOIN mqtt_credential_recovery r USING(recovery_id)
        CROSS JOIN LATERAL (SELECT jsonb_object_agg(key, to_jsonb(r) -> key) AS original_columns
            FROM jsonb_object_keys(f.original) AS key) snapshot
        WHERE r.recovery_id IS NULL OR snapshot.original_columns IS DISTINCT FROM f.original)
       OR EXISTS (SELECT 1 FROM fixture15_counts WHERE authority = 0 OR events = 0
           OR authority <> (SELECT count(*) FROM gateway_mqtt_credentials)
           OR events <> (SELECT count(*) FROM gateway_mqtt_credential_events)
           OR recovery <> (SELECT count(*) FROM mqtt_credential_recovery)
           OR authority <> (SELECT count(*) FROM fixture15_authority)
           OR events <> (SELECT count(*) FROM fixture15_events)
           OR recovery <> (SELECT count(*) FROM fixture15_recovery)) THEN
        RAISE EXCEPTION USING ERRCODE = 'P0015', MESSAGE = 'Authority upgrade changed historical rows';
    END IF;
END $$;
-- This checks actual upgraded SQL rows before any fault injection. Emit only
-- column names for additive drift, never historical values or credentials.
SELECT pg_temp.assert_fixture15_history();
DO $$ DECLARE added text; BEGIN
    SELECT string_agg(DISTINCT key, ', ' ORDER BY key) INTO added
    FROM fixture15_events f JOIN gateway_mqtt_credential_events e USING(operation_id),
         LATERAL jsonb_object_keys(to_jsonb(e)) key WHERE NOT f.original ? key;
    RAISE NOTICE 'v15 additive-only drift confirmed: %', added;
END $$;
DO $$ DECLARE relation record; evidence record; original_key text; BEGIN
    FOR relation IN SELECT * FROM (VALUES
        ('fixture15_authority', 'gateway_mqtt_credentials', 'gateway_id'),
        ('fixture15_events', 'gateway_mqtt_credential_events', 'operation_id'),
        ('fixture15_recovery', 'mqtt_credential_recovery', 'recovery_id')
    ) AS tables(fixture, current_table, identity_column) LOOP
        FOR evidence IN EXECUTE format('SELECT %I::text AS identity, original FROM %I',
            relation.identity_column, relation.fixture) LOOP
            FOR original_key IN SELECT jsonb_object_keys(evidence.original) LOOP
                BEGIN
                    EXECUTE format('UPDATE %I SET original = jsonb_set(original, ARRAY[$1], $2) WHERE %I::text = $3',
                        relation.fixture, relation.identity_column)
                    USING original_key, '"history_fault"'::jsonb, evidence.identity;
                    PERFORM pg_temp.assert_fixture15_history();
                    RAISE EXCEPTION 'History fault unexpectedly accepted: original field';
                EXCEPTION WHEN SQLSTATE 'P0015' THEN NULL; END;
            END LOOP;
            BEGIN
                EXECUTE format('DELETE FROM %I WHERE %I::text = $1', relation.current_table, relation.identity_column)
                    USING evidence.identity;
                PERFORM pg_temp.assert_fixture15_history();
                RAISE EXCEPTION 'History fault unexpectedly accepted: missing identity';
            EXCEPTION WHEN SQLSTATE 'P0015' THEN NULL; END;
        END LOOP;
        BEGIN
            EXECUTE format('DELETE FROM %I', relation.fixture);
            -- Empty recovery is valid, so exercise a fabricated recovery instead.
            IF relation.fixture = 'fixture15_recovery' THEN
                INSERT INTO mqtt_credential_recovery (recovery_id) VALUES ('15000000-0000-4000-8000-000000000001');
            END IF;
            PERFORM pg_temp.assert_fixture15_history();
            RAISE EXCEPTION 'History fault unexpectedly accepted: snapshot count';
        EXCEPTION WHEN SQLSTATE 'P0015' THEN NULL; END;
        BEGIN
            EXECUTE format('INSERT INTO %I SELECT * FROM %I LIMIT 1', relation.current_table, relation.current_table);
            IF relation.fixture = 'fixture15_recovery' AND NOT EXISTS (SELECT 1 FROM mqtt_credential_recovery) THEN
                INSERT INTO mqtt_credential_recovery (recovery_id) VALUES ('15000000-0000-4000-8000-000000000001');
            END IF;
            PERFORM pg_temp.assert_fixture15_history();
            RAISE EXCEPTION 'History fault unexpectedly accepted: fabricated authority/recovery';
        EXCEPTION WHEN SQLSTATE 'P0015' THEN NULL; END;
    END LOOP;
    PERFORM pg_temp.assert_fixture15_history();
    RAISE NOTICE 'v15 historical snapshot regressions passed';
END $$;
ROLLBACK;
BEGIN;
SET LOCAL ROLE iot_backend_app;
DO $$ DECLARE g text; BEGIN
    FOR g IN SELECT c.gateway_id FROM gateway_mqtt_credentials c LEFT JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id
        WHERE c.last_operation_id IS NULL OR e.legacy_projection LOOP
        BEGIN
            UPDATE gateway_mqtt_credentials SET credential_version=credential_version+1 WHERE gateway_id=g;
            RAISE EXCEPTION 'Legacy projection mutation accepted';
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM='Legacy projection mutation accepted' THEN RAISE; END IF;
        END;
    END LOOP;
END $$;
ROLLBACK;
