-- EXPERIMENT ONLY. Not a migration; does not advance schema_migrations.
\set ON_ERROR_STOP on
\timing on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TEMP TABLE spike_compressed_before AS
SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks
WHERE hypertable_name='telemetry' AND is_compressed;
CREATE TEMP TABLE spike_jobs_before AS
SELECT job_id, scheduled FROM timescaledb_information.jobs
WHERE hypertable_name='telemetry' AND proc_name='policy_compression';
SELECT alter_job(job_id, scheduled => false) FROM spike_jobs_before;
SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM spike_compressed_before;
ALTER TABLE telemetry SET (timescaledb.compress=false);
ALTER TABLE telemetry DROP CONSTRAINT telemetry_pkey;
CREATE SEQUENCE telemetry_sample_id_seq AS BIGINT;
ALTER TABLE telemetry ADD COLUMN sample_id BIGINT;
UPDATE telemetry SET sample_id=nextval('telemetry_sample_id_seq');
ALTER TABLE telemetry ALTER COLUMN sample_id SET DEFAULT nextval('telemetry_sample_id_seq');
ALTER TABLE telemetry ALTER COLUMN sample_id SET NOT NULL;
ALTER SEQUENCE telemetry_sample_id_seq OWNED BY telemetry.sample_id;
ALTER TABLE telemetry ADD PRIMARY KEY (measured_at, sample_id);
ALTER TABLE telemetry ADD COLUMN boot_id UUID, ADD COLUMN message_id UUID,
 ADD COLUMN sample_index INTEGER;
-- Historical rows keep unknown provenance. NOT VALID still enforces new writes.
ALTER TABLE telemetry ADD CONSTRAINT telemetry_new_provenance CHECK (
 boot_id IS NOT NULL AND message_id IS NOT NULL AND sample_index IS NOT NULL
 AND sample_index >= 0 AND sequence_num >= 0) NOT VALID;
CREATE UNIQUE INDEX telemetry_message_sample ON telemetry
 (measured_at, gateway_id, message_id, sample_index) WHERE message_id IS NOT NULL;
ALTER TABLE processed_messages ADD COLUMN source_topic TEXT, ADD COLUMN sensor_id TEXT,
 ADD COLUMN boot_id UUID, ADD COLUMN first_sequence BIGINT,
 ADD COLUMN last_sequence BIGINT, ADD COLUMN sample_count INTEGER;
ALTER TABLE processed_messages ADD CONSTRAINT processed_new_provenance CHECK (
 source_topic IS NOT NULL AND sensor_id IS NOT NULL AND boot_id IS NOT NULL
 AND first_sequence IS NOT NULL AND first_sequence >= 0
 AND sample_count IS NOT NULL AND sample_count > 0 AND last_sequence IS NOT NULL
 AND last_sequence >= first_sequence
 AND last_sequence - first_sequence = sample_count::BIGINT - 1) NOT VALID;
CREATE INDEX processed_stream_range ON processed_messages
 (gateway_id, sensor_id, boot_id, first_sequence, last_sequence);
GRANT USAGE ON SEQUENCE telemetry_sample_id_seq TO iot_backend;
ALTER TABLE telemetry SET (
 timescaledb.compress,
 timescaledb.compress_segmentby='gateway_id, sensor_id',
 timescaledb.compress_orderby='measured_at DESC, sequence_num DESC');
SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
FROM spike_compressed_before;
SELECT alter_job(job_id, scheduled => scheduled) FROM spike_jobs_before;
COMMIT;
