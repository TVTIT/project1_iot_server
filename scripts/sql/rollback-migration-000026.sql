-- Rollback Migration 26: Khôi phục telemetry_pkey và telemetry_message_sample trên bảng telemetry

\if true
BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

-- 1. Kiểm tra an toàn trước khi rollback
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 26 AND name = 'drop_telemetry_redundant_indexes') THEN
        RAISE EXCEPTION 'Migration 26 has not been applied or has an unexpected name';
    END IF;
END $$;

-- 2. Tái lập ràng buộc Primary Key trên hypertable
ALTER TABLE public.telemetry
    ADD CONSTRAINT telemetry_pkey PRIMARY KEY (measured_at, sample_id);

-- 3. Tái lập partial unique index telemetry_message_sample
CREATE UNIQUE INDEX IF NOT EXISTS telemetry_message_sample
    ON public.telemetry (measured_at, gateway_id, message_id, sample_index)
    WHERE message_id IS NOT NULL;

-- 4. Xóa bản ghi migration 26 khỏi schema_migrations
DELETE FROM public.schema_migrations
WHERE version = 26 AND name = 'drop_telemetry_redundant_indexes';

COMMIT;
\endif
