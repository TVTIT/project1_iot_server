-- Migration 25: Drop per-row foreign key trigger on telemetry hypertable
-- and enforce ON DELETE/UPDATE RESTRICT on sensors via a lightweight reverse trigger.
-- Ingestion cost for COPY/INSERT on telemetry: 0.00 ms.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

-- 1. Preflight check: schema_migrations state
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 25 AND name <> 'drop_telemetry_sensor_fk') THEN
        RAISE EXCEPTION 'Migration 25 has an unexpected name';
    END IF;
END $$;

SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 25) AS apply_drop_telemetry_sensor_fk \gset
\if :apply_drop_telemetry_sensor_fk

-- 2. Predecessor gate: Migration 24 must be present and verified
SELECT EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 24 AND name = 'telemetry_ledger_timestamp') AS predecessor_ready \gset
\if :predecessor_ready

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

-- 3. Lock tables in ACCESS EXCLUSIVE mode to alter metadata safely
LOCK TABLE public.telemetry, public.sensors IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN
    IF to_regclass('public.telemetry') IS NULL THEN
        RAISE EXCEPTION 'Migration 25 requires public.telemetry relation';
    END IF;
    IF to_regclass('public.sensors') IS NULL THEN
        RAISE EXCEPTION 'Migration 25 requires public.sensors relation';
    END IF;
END $$;

-- 4. Gỡ bỏ khóa ngoại gây kích hoạt RI_FKey_check_ins trên từng dòng khi COPY vào telemetry.
-- Tính toàn vẹn chiều ghi đã được đảm bảo bởi SELECT ... FOR SHARE trên bảng sensors
-- ngay từ đầu transaction trong prepareTelemetryResourcesWithMetrics.
ALTER TABLE public.telemetry
    DROP CONSTRAINT IF EXISTS fk_telemetry_sensor;

-- 5. Bảo toàn ngữ nghĩa ON DELETE/UPDATE RESTRICT từ phía bảng sensors mà không đặt
-- bất kỳ trigger nào lên bảng telemetry (chi phí khi INSERT/COPY telemetry = 0).
CREATE OR REPLACE FUNCTION public.prevent_sensor_delete_or_key_update_if_telemetry_exists()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    -- Bỏ qua kiểm tra nếu chỉ cập nhật các cột phụ (name, unit, metadata) không đổi khóa chính
    IF TG_OP = 'UPDATE'
       AND OLD.gateway_id = NEW.gateway_id
       AND OLD.sensor_id = NEW.sensor_id THEN
        RETURN NEW;
    END IF;

    -- Tận dụng index idx_telemetry_gateway_sensor_time để seek B-Tree tức thì (< 0.1ms)
    IF EXISTS (
        SELECT 1
        FROM public.telemetry
        WHERE gateway_id = OLD.gateway_id
          AND sensor_id = OLD.sensor_id
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'update or delete on table "sensors" violates restrict constraint on table "telemetry"'
            USING ERRCODE = 'foreign_key_violation',
                  CONSTRAINT = 'fk_telemetry_sensor',
                  TABLE = 'telemetry';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sensors_restrict_telemetry ON public.sensors;
CREATE TRIGGER trg_sensors_restrict_telemetry
    BEFORE DELETE OR UPDATE OF gateway_id, sensor_id ON public.sensors
    FOR EACH ROW
    EXECUTE FUNCTION public.prevent_sensor_delete_or_key_update_if_telemetry_exists();

-- 6. Ghi nhận hoàn thành migration vào bảng theo dõi
INSERT INTO public.schema_migrations(version, name)
VALUES (25, 'drop_telemetry_sensor_fk');

COMMIT;

\else
\echo 'Migration 25 deferred: apply after migration 24 telemetry_ledger_timestamp.'
\endif
\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 25 AND name = 'drop_telemetry_sensor_fk') THEN
        RAISE EXCEPTION 'Unexpected migration version 25';
    END IF;
END $$;
\endif

SELECT pg_advisory_unlock(3290, 2);
