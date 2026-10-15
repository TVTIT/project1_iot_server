\set ON_ERROR_STOP on

INSERT INTO gateways(gateway_id, name) VALUES ('fixture_gw', 'DDL fixture');
INSERT INTO sensors(gateway_id, sensor_id, name, unit)
VALUES ('fixture_gw', 'fixture_sensor', 'DDL sensor', 'degC');
INSERT INTO twin_entities(entity_id, entity_type, gateway_id)
VALUES ('urn:ngsi-ld:Gateway:fixture_gw', 'Gateway', 'fixture_gw'),
       ('urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor', 'Sensor', 'fixture_gw');
INSERT INTO twin_relationships(source_entity_id, relationship_type, target_entity_id)
SELECT parent.id, 'hasSensor', child.id FROM twin_entities parent, twin_entities child
WHERE parent.entity_id='urn:ngsi-ld:Gateway:fixture_gw'
  AND child.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor';
INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities;
INSERT INTO telemetry(measured_at, gateway_id, sensor_id, sequence_num, value)
VALUES (now() - interval '10 days', 'fixture_gw', 'fixture_sensor', 0, 25.1),
       (now() - interval '10 days' + interval '10 milliseconds', 'fixture_gw', 'fixture_sensor', 1, 25.2),
       (now(), 'fixture_gw', 'fixture_sensor', 2, 25.3);
INSERT INTO processed_messages(gateway_id, message_id, payload_hash)
VALUES ('fixture_gw', '00000000-0000-4000-8000-000000000001', repeat('a', 64));

SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM timescaledb_information.chunks
WHERE hypertable_name='telemetry' AND range_end < now() - interval '7 days';
DO $$ BEGIN
  IF (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry' AND is_compressed) <> 1
     OR (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry' AND NOT is_compressed) <> 1 THEN
    RAISE EXCEPTION 'upgrade seed requires one compressed and one uncompressed telemetry chunk';
  END IF;
END $$;

CREATE TABLE stage3_legacy_telemetry_snapshot AS
SELECT measured_at, gateway_id, sensor_id, sequence_num, value FROM telemetry;
CREATE TABLE stage3_legacy_ledger_snapshot AS
SELECT gateway_id, message_id, payload_hash, processed_at FROM processed_messages;
CREATE TABLE stage3_legacy_chunks_snapshot AS
SELECT chunk_schema, chunk_name, is_compressed FROM timescaledb_information.chunks
WHERE hypertable_name='telemetry';
CREATE TABLE stage3_legacy_jobs_snapshot AS
SELECT job_id, scheduled FROM timescaledb_information.jobs
WHERE hypertable_name='telemetry' AND proc_name='policy_compression';
CREATE TABLE stage3_legacy_compression_snapshot AS
SELECT hypertable, segmentby, orderby, compress_interval_length
FROM timescaledb_information.hypertable_compression_settings
WHERE hypertable='public.telemetry'::regclass;
CREATE TABLE stage3_legacy_relation_snapshot AS
SELECT pg_total_relation_size('public.telemetry') AS relation_bytes,
       hypertable_size('public.telemetry') AS hypertable_bytes;
