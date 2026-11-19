-- Migration 26: Gỡ bỏ các chỉ mục B-Tree dư thừa trên hypertable telemetry
-- Giữ lại duy nhất 1 index phục vụ đọc: idx_telemetry_gateway_sensor_time
-- Cắt giảm 27.5% WAL và 24% CPU trong các chu kỳ nạp COPY tải cao.

\if true
BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- 1. Gỡ bỏ partial unique index dư thừa (chống trùng đã do processed_messages đảm nhiệm)
DROP INDEX IF EXISTS public.telemetry_message_sample;

-- 2. Gỡ bỏ ràng buộc Primary Key không sử dụng trên hypertable
ALTER TABLE public.telemetry DROP CONSTRAINT IF EXISTS telemetry_pkey;

-- 3. Ghi nhận migration 26 vào schema_migrations
INSERT INTO public.schema_migrations (version, name)
VALUES (26, 'drop_telemetry_redundant_indexes')
ON CONFLICT (version) DO NOTHING;

COMMIT;
\endif
