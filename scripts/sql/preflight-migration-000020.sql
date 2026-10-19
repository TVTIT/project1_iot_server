-- Read-only checklist before migration 000020. Run with a database role that
-- can inspect Timescale catalogs; this file never changes database state.
\set ON_ERROR_STOP on

SELECT current_setting('server_version') AS postgres_version,
       extversion AS timescaledb_version
FROM pg_extension WHERE extname = 'timescaledb';

SELECT version, name, applied_at
FROM public.schema_migrations
WHERE version IN (19, 20)
ORDER BY version;

SELECT (SELECT count(*) FROM public.telemetry) AS telemetry_rows,
       (SELECT count(*) FROM public.processed_messages) AS processed_message_rows,
       (SELECT count(*) FROM public.telemetry t
         WHERE to_jsonb(t)->>'sample_id' IS NULL) AS missing_sample_id,
       (SELECT count(*) FROM public.telemetry t
         WHERE to_jsonb(t)->>'boot_id' IS NOT NULL
            OR to_jsonb(t)->>'message_id' IS NOT NULL
            OR to_jsonb(t)->>'sample_index' IS NOT NULL) AS partially_or_fully_provenanced_rows;

SELECT table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('telemetry', 'processed_messages')
ORDER BY table_name, ordinal_position;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename IN ('telemetry', 'processed_messages')
ORDER BY tablename, indexname;

SELECT conrelid::regclass AS table_name, conname, contype, convalidated,
       pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid IN ('public.telemetry'::regclass, 'public.processed_messages'::regclass)
ORDER BY conrelid::regclass::text, conname;

SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_name IN ('telemetry', 'twin_temporal_values')
ORDER BY hypertable_name;

SELECT chunk_schema, chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'telemetry'
ORDER BY range_start;

SELECT j.job_id, j.proc_name, j.scheduled, s.job_status, s.last_run_status, s.next_start
FROM timescaledb_information.jobs j
LEFT JOIN timescaledb_information.job_stats s USING (job_id)
WHERE j.hypertable_name = 'telemetry'
ORDER BY j.job_id;

SELECT pg_size_pretty(pg_total_relation_size('public.telemetry')) AS telemetry_parent_size,
       pg_size_pretty(hypertable_size('public.telemetry')) AS telemetry_hypertable_size;

SELECT 'STOP telemetry writers and confirm adequate free disk space for chunk decompression/recompression before applying migration 000020.' AS operator_check;
