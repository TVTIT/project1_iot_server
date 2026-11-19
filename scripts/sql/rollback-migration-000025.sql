-- Migration 25 Rollback: Khôi phục khóa ngoại fk_telemetry_sensor trên bảng telemetry
-- và dọn dẹp trigger đảo chiều trên bảng sensors.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 25 AND name = 'drop_telemetry_sensor_fk') THEN
        RAISE EXCEPTION 'Migration 25 has not been applied or has an unexpected name';
    END IF;
END $$;

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE public.telemetry, public.sensors IN ACCESS EXCLUSIVE MODE;

-- 1. Xóa trigger và function bảo vệ trên bảng sensors
DROP TRIGGER IF EXISTS trg_sensors_restrict_telemetry ON public.sensors;
DROP FUNCTION IF EXISTS public.prevent_sensor_delete_or_key_update_if_telemetry_exists();

-- 2. Tái lập khóa ngoại fk_telemetry_sensor trên bảng telemetry
ALTER TABLE public.telemetry
    ADD CONSTRAINT fk_telemetry_sensor
    FOREIGN KEY (gateway_id, sensor_id)
    REFERENCES public.sensors(gateway_id, sensor_id)
    ON DELETE RESTRICT;

-- 3. Xóa bản ghi migration 25 khỏi schema_migrations
DELETE FROM public.schema_migrations WHERE version = 25;

COMMIT;

SELECT pg_advisory_unlock(3290, 2);
