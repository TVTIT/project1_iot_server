-- Add server-assigned raw sample identity and provenance required by the
-- Stage 3 telemetry contract. Existing rows keep unknown provenance.
\set ON_ERROR_STOP on

SELECT current_setting('statement_timeout') AS migration20_prior_statement_timeout,
       current_setting('lock_timeout') AS migration20_prior_lock_timeout \gset
SET statement_timeout = '60s';
SET lock_timeout = '10s';
SELECT pg_advisory_lock(3290, 2);

SELECT to_regclass('public.schema_migrations') IS NOT NULL AS tracking_ready \gset
\if :tracking_ready
SELECT EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 20 AND name = 'stage3_telemetry_ingestion') AS already_applied \gset
\if :already_applied
  \echo 'Migration 20 already applied; checking recorded name.'
\else
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 19 AND name = 'mqtt_simplified_journal') THEN
      RAISE EXCEPTION 'Migration 20 requires schema_migrations version 19 mqtt_simplified_journal';
    END IF;
    IF EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 20) THEN
      RAISE EXCEPTION 'Migration version 20 is recorded with an unexpected name';
    END IF;
    IF to_regclass('public.telemetry') IS NULL OR to_regclass('public.processed_messages') IS NULL
       OR NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'telemetry') THEN
      RAISE EXCEPTION 'Migration 20 requires the telemetry and processed_messages schema';
    END IF;
  END $$;

  -- Timescale jobs are external scheduler state. Save exact values and pause
  -- before opening the DDL transaction; a transactional pause is not sufficient.
  CREATE TEMP TABLE migration20_compression_jobs ON COMMIT PRESERVE ROWS AS
    SELECT job_id, scheduled FROM timescaledb_information.jobs
    WHERE hypertable_name = 'telemetry' AND proc_name = 'policy_compression';
  SELECT COALESCE(json_agg(json_build_object('job_id', job_id, 'scheduled', scheduled)
                           ORDER BY job_id)::text, '[]') AS migration20_schedule_snapshot
    FROM migration20_compression_jobs \gset
  \echo 'Migration 20 saved compression schedule snapshot:' :migration20_schedule_snapshot
  \echo 'If this migration fails after pausing, wait for its transaction to end and restore each job with: SELECT alter_job(<job_id>, scheduled => <saved_boolean>);'

  SELECT alter_job(job_id, scheduled => false) FROM migration20_compression_jobs;
  DO $$
  DECLARE deadline timestamptz := clock_timestamp() + interval '30 seconds';
  BEGIN
    WHILE EXISTS (
      SELECT 1 FROM timescaledb_information.job_stats s
      JOIN migration20_compression_jobs j USING (job_id)
      WHERE s.job_status = 'Running'
    ) LOOP
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'Timed out waiting for telemetry compression job to stop';
      END IF;
      PERFORM pg_sleep(0.1);
    END LOOP;
  END $$;

  -- Capture after the scheduler is stopped so a job finishing during the
  -- bounded wait cannot leave a newly compressed chunk outside the snapshot.
  CREATE TEMP TABLE migration20_compressed_chunks ON COMMIT PRESERVE ROWS AS
    SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks
    WHERE hypertable_name = 'telemetry' AND is_compressed;

  BEGIN;
  SET LOCAL lock_timeout = '10s';
  SET LOCAL statement_timeout = '60s';

  SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
    FROM migration20_compressed_chunks;
  ALTER TABLE public.telemetry SET (timescaledb.compress = false);
  ALTER TABLE public.telemetry DROP CONSTRAINT telemetry_pkey;

  CREATE SEQUENCE public.telemetry_sample_id_seq AS BIGINT;
  ALTER TABLE public.telemetry ADD COLUMN sample_id BIGINT;
  UPDATE public.telemetry SET sample_id = nextval('public.telemetry_sample_id_seq');
  ALTER TABLE public.telemetry ALTER COLUMN sample_id SET DEFAULT nextval('public.telemetry_sample_id_seq');
  ALTER TABLE public.telemetry ALTER COLUMN sample_id SET NOT NULL;
  ALTER SEQUENCE public.telemetry_sample_id_seq OWNED BY public.telemetry.sample_id;
  ALTER TABLE public.telemetry ADD CONSTRAINT telemetry_pkey PRIMARY KEY (measured_at, sample_id);

  ALTER TABLE public.telemetry
    ADD COLUMN boot_id UUID,
    ADD COLUMN message_id UUID,
    ADD COLUMN sample_index INTEGER;
  ALTER TABLE public.telemetry ADD CONSTRAINT telemetry_new_provenance CHECK (
    boot_id IS NOT NULL AND message_id IS NOT NULL AND sample_index IS NOT NULL
    AND sample_index >= 0 AND sequence_num >= 0
  ) NOT VALID;
  CREATE UNIQUE INDEX telemetry_message_sample
    ON public.telemetry (measured_at, gateway_id, message_id, sample_index)
    WHERE message_id IS NOT NULL;

  ALTER TABLE public.processed_messages
    ADD COLUMN source_topic TEXT,
    ADD COLUMN sensor_id TEXT,
    ADD COLUMN boot_id UUID,
    ADD COLUMN first_sequence BIGINT,
    ADD COLUMN last_sequence BIGINT,
    ADD COLUMN sample_count INTEGER;
  ALTER TABLE public.processed_messages ADD CONSTRAINT processed_new_provenance CHECK (
    source_topic IS NOT NULL AND sensor_id IS NOT NULL AND boot_id IS NOT NULL
    AND first_sequence IS NOT NULL AND first_sequence >= 0
    AND sample_count IS NOT NULL AND sample_count > 0
    AND last_sequence IS NOT NULL AND last_sequence >= first_sequence
    AND last_sequence - first_sequence = sample_count::BIGINT - 1
  ) NOT VALID;
  CREATE INDEX processed_stream_range
    ON public.processed_messages (gateway_id, sensor_id, boot_id, first_sequence, last_sequence);

  GRANT USAGE ON SEQUENCE public.telemetry_sample_id_seq TO iot_backend;
  ALTER TABLE public.telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'gateway_id, sensor_id',
    timescaledb.compress_orderby = 'measured_at DESC, sequence_num DESC'
  );
  -- Do maintenance restoration before recording the migration. If any of
  -- these steps fail, schema and tracking roll back together; the scheduler
  -- pause remains external and is restored from the printed snapshot.
  SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
    FROM migration20_compressed_chunks;
  SELECT alter_job(job_id, scheduled => scheduled) FROM migration20_compression_jobs;
  INSERT INTO public.schema_migrations(version, name)
    VALUES (20, 'stage3_telemetry_ingestion');
  COMMIT;
\endif
\else
  DO $$ BEGIN RAISE EXCEPTION 'schema_migrations is missing; initialize the database first'; END $$;
\endif

SELECT name = 'stage3_telemetry_ingestion' AS migration20_name_matches
  FROM public.schema_migrations WHERE version = 20 \gset
\if :migration20_name_matches
  \echo 'Migration 20 stage3_telemetry_ingestion is present.'
\else
  DO $$ BEGIN RAISE EXCEPTION 'Migration version 20 has an unexpected name or is absent'; END $$;
\endif
SELECT set_config('statement_timeout', :'migration20_prior_statement_timeout', false),
       set_config('lock_timeout', :'migration20_prior_lock_timeout', false);
SELECT pg_advisory_unlock(3290, 2);
