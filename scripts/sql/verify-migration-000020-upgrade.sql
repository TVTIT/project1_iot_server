\set ON_ERROR_STOP on
DO $$ BEGIN
  IF (SELECT count(*) FROM public.telemetry) <> (SELECT count(*) FROM stage3_legacy_telemetry_snapshot)
     OR EXISTS ((SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM public.telemetry
                 EXCEPT ALL SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM stage3_legacy_telemetry_snapshot)
                UNION ALL
                (SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM stage3_legacy_telemetry_snapshot
                 EXCEPT ALL SELECT measured_at,gateway_id,sensor_id,sequence_num,value FROM public.telemetry)) THEN
    RAISE EXCEPTION 'legacy telemetry fields or multiplicity changed';
  END IF;
  IF (SELECT count(*) FROM public.processed_messages) <> (SELECT count(*) FROM stage3_legacy_ledger_snapshot)
     OR EXISTS ((SELECT gateway_id,message_id,payload_hash,processed_at FROM public.processed_messages
                 EXCEPT ALL SELECT gateway_id,message_id,payload_hash,processed_at FROM stage3_legacy_ledger_snapshot)
                UNION ALL
                (SELECT gateway_id,message_id,payload_hash,processed_at FROM stage3_legacy_ledger_snapshot
                 EXCEPT ALL SELECT gateway_id,message_id,payload_hash,processed_at FROM public.processed_messages)) THEN
    RAISE EXCEPTION 'legacy ledger fields or multiplicity changed';
  END IF;
  IF (SELECT count(*) FROM public.telemetry WHERE sample_id IS NULL) <> 0
     OR (SELECT count(*) FROM public.telemetry WHERE boot_id IS NULL AND message_id IS NULL AND sample_index IS NULL) <> 3
     OR (SELECT count(*) FROM public.processed_messages WHERE source_topic IS NULL AND sensor_id IS NULL AND boot_id IS NULL
          AND first_sequence IS NULL AND last_sequence IS NULL AND sample_count IS NULL) <> 1 THEN
    RAISE EXCEPTION 'legacy surrogate/provenance expectations failed';
  END IF;
  IF EXISTS (
    SELECT 1 FROM stage3_legacy_chunks_snapshot old
    LEFT JOIN timescaledb_information.chunks now USING (chunk_schema,chunk_name)
    WHERE now.chunk_name IS NULL OR old.is_compressed IS DISTINCT FROM now.is_compressed
  ) OR (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='telemetry')
       <> (SELECT count(*) FROM stage3_legacy_chunks_snapshot) THEN
    RAISE EXCEPTION 'telemetry chunk compression state changed';
  END IF;
  IF EXISTS (
    SELECT 1 FROM stage3_legacy_jobs_snapshot old
    LEFT JOIN timescaledb_information.jobs now USING (job_id)
    WHERE now.job_id IS NULL OR old.scheduled IS DISTINCT FROM now.scheduled
  ) THEN
    RAISE EXCEPTION 'compression job schedule was not restored exactly';
  END IF;
  IF EXISTS (
    SELECT 1 FROM stage3_legacy_compression_snapshot old
    FULL JOIN (SELECT * FROM timescaledb_information.hypertable_compression_settings
               WHERE hypertable='public.telemetry'::regclass) current
      ON current.hypertable = old.hypertable
    WHERE old.hypertable IS NULL OR current.hypertable IS NULL
       OR old.segmentby IS DISTINCT FROM current.segmentby
       OR old.orderby IS DISTINCT FROM current.orderby
       OR old.compress_interval_length IS DISTINCT FROM current.compress_interval_length
  ) THEN
    RAISE EXCEPTION 'telemetry compression layout changed';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.telemetry'::regclass AND conname='telemetry_new_provenance' AND convalidated)
     OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.processed_messages'::regclass AND conname='processed_new_provenance' AND convalidated) THEN
    RAISE EXCEPTION 'legacy provenance checks unexpectedly validated';
  END IF;
END $$;
SELECT 'Migration 000020 upgrade data and maintenance verification passed.';
