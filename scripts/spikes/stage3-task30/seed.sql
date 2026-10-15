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
CREATE TABLE spike_before AS SELECT * FROM telemetry;
CREATE TABLE spike_chunks AS SELECT chunk_schema, chunk_name, is_compressed
FROM timescaledb_information.chunks WHERE hypertable_name='telemetry';
SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM timescaledb_information.chunks
WHERE hypertable_name='telemetry' AND range_end < now() - interval '7 days';
DO $$ BEGIN
 IF (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry' AND is_compressed) <> 1
 OR (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry' AND NOT is_compressed) <> 1
 THEN RAISE EXCEPTION 'expected one compressed and one uncompressed chunk'; END IF;
END $$;
SELECT pg_total_relation_size('telemetry') AS parent_relation_bytes_before;
SELECT hypertable_size('telemetry') AS hypertable_bytes_before;
