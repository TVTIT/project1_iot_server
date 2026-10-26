-- Tier-2 timestamp index. Legacy rows are backfilled only from a fully
-- matching original sample_index=0; unknown provenance remains NULL.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 24 AND name <> 'telemetry_ledger_timestamp') THEN
        RAISE EXCEPTION 'Migration 24 has an unexpected name';
    END IF;
END $$;
SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 24) AS apply_telemetry_ledger_timestamp \gset
\if :apply_telemetry_ledger_timestamp
SELECT EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 23 AND name = 'sensor_control_intent') AS predecessor_ready \gset
\if :predecessor_ready
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
LOCK TABLE public.processed_messages, public.telemetry IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'public.processed_messages'::regclass
                     AND conname = 'processed_messages_pkey'
                     AND contype = 'p'
                     AND pg_get_constraintdef(oid) = 'PRIMARY KEY (gateway_id, message_id)') THEN
        RAISE EXCEPTION 'Migration 24 requires processed_messages primary key (gateway_id, message_id)';
    END IF;
    IF to_regclass('public.processed_stream_range') IS NULL THEN
        RAISE EXCEPTION 'Migration 24 requires processed_stream_range; no schema changes applied';
    END IF;
END $$;

ALTER TABLE public.processed_messages ADD COLUMN timestamp TIMESTAMPTZ;

-- A known ledger provenance that disagrees with the original sample is a
-- conflict, not a legacy row. Refuse before any timestamp is backfilled.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.processed_messages p
        JOIN public.telemetry t
          ON t.gateway_id = p.gateway_id
         AND t.message_id = p.message_id
         AND t.sample_index = 0
        WHERE (p.sensor_id IS NOT NULL AND p.sensor_id IS DISTINCT FROM t.sensor_id)
           OR (p.boot_id IS NOT NULL AND p.boot_id IS DISTINCT FROM t.boot_id)
           OR (p.first_sequence IS NOT NULL AND p.first_sequence IS DISTINCT FROM t.sequence_num)
           OR t.boot_id IS NULL OR t.sample_index IS NULL OR t.sequence_num IS NULL
    ) THEN
        RAISE EXCEPTION 'Migration 24 conflicting telemetry provenance at sample_index=0; no rows changed';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.processed_messages p
        JOIN public.telemetry t
          ON t.gateway_id = p.gateway_id
         AND t.message_id = p.message_id
         AND t.sample_index = 0
        WHERE p.sensor_id IS NOT NULL AND p.boot_id IS NOT NULL
          AND p.first_sequence IS NOT NULL AND p.source_topic IS NOT NULL
        GROUP BY p.gateway_id, p.message_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Migration 24 ambiguous telemetry sample_index=0 provenance; no rows changed';
    END IF;
END $$;

UPDATE public.processed_messages p
   SET timestamp = t.measured_at
  FROM public.telemetry t
 WHERE t.gateway_id = p.gateway_id
   AND t.message_id = p.message_id
   AND t.sample_index = 0
   AND t.sensor_id = p.sensor_id
   AND t.boot_id = p.boot_id
   AND t.sequence_num = p.first_sequence
   AND p.source_topic IS NOT NULL
   AND p.sensor_id IS NOT NULL
   AND p.boot_id IS NOT NULL
   AND p.first_sequence IS NOT NULL
   AND p.last_sequence IS NOT NULL
   AND p.sample_count IS NOT NULL
   AND p.first_sequence >= 0
   AND p.last_sequence >= p.first_sequence
   AND p.sample_count > 0
   AND p.last_sequence - p.first_sequence = p.sample_count::BIGINT - 1
   AND p.timestamp IS NULL;

DO $$ DECLARE legacy_count BIGINT;
BEGIN
    SELECT count(*) INTO legacy_count FROM public.processed_messages WHERE timestamp IS NULL;
    RAISE NOTICE 'Migration 24 retained % legacy ledger rows with unknown original timestamp (NULL)', legacy_count;
END $$;

CREATE UNIQUE INDEX processed_messages_gateway_sensor_message_timestamp
    ON public.processed_messages (gateway_id, sensor_id, message_id, timestamp);
INSERT INTO public.schema_migrations(version, name)
VALUES (24, 'telemetry_ledger_timestamp');
COMMIT;
\else
\echo 'Migration 24 deferred: apply after migration 23 sensor_control_intent.'
\endif
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 24 AND name = 'telemetry_ledger_timestamp') THEN
        RAISE EXCEPTION 'Unexpected migration version 24';
    END IF;
END $$;
\endif
SELECT pg_advisory_unlock(3290, 2);
