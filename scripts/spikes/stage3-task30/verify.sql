\set ON_ERROR_STOP on
DO $$ BEGIN
 IF EXISTS ((SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM telemetry
             EXCEPT SELECT * FROM spike_before)
            UNION ALL
            (SELECT * FROM spike_before EXCEPT SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM telemetry))
 OR (SELECT count(*) FROM telemetry) <> 3 THEN RAISE EXCEPTION 'legacy data changed'; END IF;
 IF (SELECT count(*) FROM telemetry WHERE boot_id IS NULL AND message_id IS NULL AND sample_index IS NULL) <> 3
 THEN RAISE EXCEPTION 'legacy provenance fabricated'; END IF;
 IF (SELECT count(*) FROM processed_messages WHERE source_topic IS NULL) <> 1
 THEN RAISE EXCEPTION 'legacy ledger changed'; END IF;
END $$;
SET ROLE iot_backend_app;
INSERT INTO telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)
VALUES ('2026-01-01Z','fixture_gw','fixture_sensor',0,30,
 '00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000003',0),
 ('2026-01-01Z','fixture_gw','fixture_sensor',0,31,
 '00000000-0000-4000-8000-000000000004','00000000-0000-4000-8000-000000000005',0);
INSERT INTO processed_messages(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)
VALUES ('fixture_gw','00000000-0000-4000-8000-000000000003',repeat('b',64),
 'gateways/fixture_gw/telemetry/fixture_sensor','fixture_sensor',
 '00000000-0000-4000-8000-000000000002',0,0,1);
DO $$ BEGIN
 BEGIN
  INSERT INTO telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)
  VALUES ('2026-01-01Z','fixture_gw','fixture_sensor',0,30,
   '00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000003',0);
  RAISE EXCEPTION 'duplicate identity allowed';
 EXCEPTION WHEN unique_violation THEN NULL; END;
 BEGIN
  INSERT INTO telemetry(measured_at,gateway_id,sensor_id,sequence_num,value)
  VALUES (now(),'fixture_gw','fixture_sensor',4,1);
  RAISE EXCEPTION 'new NULL provenance allowed';
 EXCEPTION WHEN check_violation THEN NULL; END;
 BEGIN
  INSERT INTO processed_messages(gateway_id,message_id,payload_hash)
  VALUES ('fixture_gw',gen_random_uuid(),repeat('c',64));
  RAISE EXCEPTION 'new legacy ledger allowed';
 EXCEPTION WHEN check_violation THEN NULL; END;
 BEGIN
  CREATE TABLE public.spike_unauthorized(id INT);
  RAISE EXCEPTION 'backend has DDL privileges';
 EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;
RESET ROLE;
DO $$ BEGIN
 IF (SELECT count(*) FROM telemetry) <> 5 THEN RAISE EXCEPTION 'unexpected sample count'; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_telemetry_sensor')
 OR NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='idx_telemetry_gateway_sensor_time')
 OR (SELECT count(*) FROM timescaledb_information.hypertables) <> 2
 OR (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry' AND is_compressed) <> 1
 OR EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name='policy_retention')
 OR EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE hypertable_name='telemetry' AND proc_name='policy_compression' AND NOT scheduled)
 OR (SELECT max(version) FROM schema_migrations) <> 19
 THEN RAISE EXCEPTION 'schema/policy invariant changed'; END IF;
END $$;
SELECT conname, convalidated FROM pg_constraint
WHERE conrelid IN ('telemetry'::regclass,'processed_messages'::regclass) ORDER BY conname;
SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('telemetry','processed_messages');
SELECT chunk_name, is_compressed FROM timescaledb_information.chunks WHERE hypertable_name='telemetry';
SELECT hypertable_size('telemetry') AS hypertable_bytes_after;
