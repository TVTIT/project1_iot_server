\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE
  pk_def text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                 WHERE version=20 AND name='stage3_telemetry_ingestion')
     OR (SELECT max(version) FROM public.schema_migrations) <> 20 THEN
    RAISE EXCEPTION 'migration 20 tracking is missing or unexpected';
  END IF;
  SELECT pg_get_constraintdef(oid) INTO pk_def FROM pg_constraint
  WHERE conrelid='public.telemetry'::regclass AND contype='p';
  IF pk_def <> 'PRIMARY KEY (measured_at, sample_id)' THEN
    RAISE EXCEPTION 'telemetry primary key mismatch: %', pk_def;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='telemetry'
                 AND column_name='sample_id' AND data_type='bigint' AND is_nullable='NO'
                 AND column_default LIKE 'nextval(%telemetry_sample_id_seq%') THEN
    RAISE EXCEPTION 'sample_id definition mismatch';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='telemetry' AND column_name='boot_id' AND data_type='uuid')
     OR NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='telemetry' AND column_name='message_id' AND data_type='uuid')
     OR NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='telemetry' AND column_name='sample_index' AND data_type='integer') THEN
    RAISE EXCEPTION 'raw provenance columns are missing or have wrong types';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.telemetry'::regclass AND conname='telemetry_new_provenance' AND NOT convalidated)
     OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.processed_messages'::regclass AND conname='processed_new_provenance' AND NOT convalidated) THEN
    RAISE EXCEPTION 'new provenance constraints must exist and remain NOT VALID';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='telemetry' AND indexname='telemetry_message_sample'
                 AND indexdef LIKE '%UNIQUE INDEX telemetry_message_sample%WHERE (message_id IS NOT NULL)%')
     OR NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='processed_messages' AND indexname='processed_stream_range') THEN
    RAISE EXCEPTION 'new telemetry indexes are missing';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.telemetry'::regclass AND conname='fk_telemetry_sensor')
     OR NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='telemetry' AND indexname='idx_telemetry_gateway_sensor_time') THEN
    RAISE EXCEPTION 'historical telemetry FK/index was not preserved';
  END IF;
  IF (SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name IN ('telemetry','twin_temporal_values')) <> 2
     OR EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name='policy_retention') THEN
    RAISE EXCEPTION 'hypertables or disabled retention policy changed';
  END IF;
  IF NOT has_sequence_privilege('iot_backend','public.telemetry_sample_id_seq','USAGE')
     OR NOT has_sequence_privilege('iot_backend_app','public.telemetry_sample_id_seq','USAGE')
     OR has_schema_privilege('iot_backend_app','public','CREATE')
     OR NOT has_table_privilege('iot_backend_app','public.telemetry','SELECT')
     OR NOT has_table_privilege('iot_backend_app','public.telemetry','INSERT')
     OR NOT has_table_privilege('iot_backend_app','public.processed_messages','SELECT')
     OR NOT has_table_privilege('iot_backend_app','public.processed_messages','INSERT') THEN
    RAISE EXCEPTION 'application sequence grant or DDL restriction mismatch';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='iot_backend_app')
     OR NOT pg_has_role('iot_backend_app','iot_backend','MEMBER') THEN
    RAISE EXCEPTION 'application role inheritance is missing';
  END IF;
  IF (SELECT count(*) FROM timescaledb_information.jobs
      WHERE hypertable_name='telemetry' AND proc_name='policy_compression') <> 1
     OR EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name='policy_retention') THEN
    RAISE EXCEPTION 'compression or disabled retention policy mismatch';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertable_compression_settings
                 WHERE hypertable='public.telemetry'::regclass
                   AND segmentby='gateway_id,sensor_id'
                   AND orderby='measured_at DESC,sequence_num DESC') THEN
    RAISE EXCEPTION 'telemetry compression layout changed';
  END IF;
END $$;
COMMIT;

\if :{?stage3_verify_writes}
\else
\set stage3_verify_writes true
\endif
\if :stage3_verify_writes
-- Exercise app-role inserts and check that constraints reject incomplete
-- metadata while permitting reused device sequence/time in another boot.
SET ROLE iot_backend_app;
INSERT INTO public.telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)
VALUES ('2026-01-01 00:00:00+00','fixture_gw','fixture_sensor',7,31,
 '00000000-0000-4000-8000-000000000010','00000000-0000-4000-8000-000000000011',0),
 ('2026-01-01 00:00:00+00','fixture_gw','fixture_sensor',7,32,
 '00000000-0000-4000-8000-000000000012','00000000-0000-4000-8000-000000000013',0);
INSERT INTO public.processed_messages(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)
VALUES ('fixture_gw','00000000-0000-4000-8000-000000000011',repeat('b',64),
 'gateways/fixture_gw/telemetry/fixture_sensor','fixture_sensor',
 '00000000-0000-4000-8000-000000000010',7,7,1);
DO $$ BEGIN
  BEGIN
    INSERT INTO public.telemetry(measured_at,gateway_id,sensor_id,sequence_num,value)
    VALUES ('2026-01-02Z','fixture_gw','fixture_sensor',8,1);
    RAISE EXCEPTION 'new raw row without provenance unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    INSERT INTO public.telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)
    VALUES ('2026-01-01 00:00:00+00','fixture_gw','fixture_sensor',8,1,
      '00000000-0000-4000-8000-000000000010','00000000-0000-4000-8000-000000000011',0);
    RAISE EXCEPTION 'duplicate sample identity unexpectedly succeeded';
  EXCEPTION WHEN unique_violation THEN NULL; END;
  BEGIN
    INSERT INTO public.telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id)
    VALUES ('2026-01-02Z','fixture_gw','fixture_sensor',8,1,
      '00000000-0000-4000-8000-000000000010');
    RAISE EXCEPTION 'partial raw provenance unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    INSERT INTO public.processed_messages(gateway_id,message_id,payload_hash)
    VALUES ('fixture_gw','00000000-0000-4000-8000-000000000014',repeat('c',64));
    RAISE EXCEPTION 'new ledger row without provenance unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    INSERT INTO public.processed_messages(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)
    VALUES ('fixture_gw','00000000-0000-4000-8000-000000000015',repeat('d',64),
      'gateways/fixture_gw/telemetry/fixture_sensor','fixture_sensor',
      '00000000-0000-4000-8000-000000000010',0,1,1);
    RAISE EXCEPTION 'invalid ledger range unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    CREATE TABLE public.stage3_app_must_not_create(id integer);
    RAISE EXCEPTION 'application role unexpectedly created a table';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;
RESET ROLE;

DO $$ BEGIN
  IF (SELECT count(*) FROM public.telemetry WHERE message_id IN (
      '00000000-0000-4000-8000-000000000011','00000000-0000-4000-8000-000000000013')) <> 2
     OR (SELECT count(*) FROM public.processed_messages WHERE message_id='00000000-0000-4000-8000-000000000011') <> 1 THEN
    RAISE EXCEPTION 'app-role valid inserts missing';
  END IF;
END $$;
\else
\echo 'Migration 000020 fixture-specific DML checks skipped by caller.'
\endif
SELECT 'Migration 000020 verification passed.';
