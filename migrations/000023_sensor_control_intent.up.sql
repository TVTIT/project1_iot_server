-- Stage 5.2: durable Sensor intent only. No legacy command is made dispatchable.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=23 AND name<>'sensor_control_intent') THEN
        RAISE EXCEPTION 'Migration 23 has an unexpected name';
    END IF;
END $$;
SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=23) AS apply_sensor_control_intent \gset
\if :apply_sensor_control_intent
-- Match migrations 21/22: initial DB bootstrap precedes Auth initialization.
SELECT to_regclass('auth.users') IS NOT NULL AS auth_ready \gset
\if :auth_ready
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=22 AND name='gateway_membership_revocation') THEN
        RAISE EXCEPTION 'Migration 000023 requires migration 22 gateway_membership_revocation';
    END IF;
END $$;
-- Authoritative preflight runs under DDL locks, not a racy operator-only check.
LOCK TABLE public.twin_entities, public.twin_states, public.twin_commands, public.twin_outbox IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.twin_commands c JOIN public.twin_entities e ON e.id=c.entity_id
               WHERE e.entity_type='Sensor' AND c.status IN ('pending','published','acknowledged')
               GROUP BY c.entity_id HAVING count(*)>1) THEN
        RAISE EXCEPTION 'Stage 5 preflight: multiple active commands for a Sensor; operator resolution required, no rows changed';
    END IF;
    IF EXISTS (SELECT 1 FROM public.twin_commands WHERE desired_version<0 OR expires_at<=issued_at)
       OR EXISTS (SELECT 1 FROM public.twin_states WHERE desired_version<0 OR reported_version<0) THEN
        RAISE EXCEPTION 'Stage 5 preflight: negative version or invalid command deadline; operator resolution required, no rows changed';
    END IF;
END $$;

-- Legacy actions/parameters/payloads (including historical seconds sentinels)
-- remain unchanged and have NULL control_version and NULL payload_bytes.
ALTER TABLE public.twin_entities ADD CONSTRAINT uq_twin_entities_id_type UNIQUE(id,entity_type);
ALTER TABLE public.twin_commands
    ADD COLUMN target_entity_type text,
    ADD COLUMN control_version integer,
    ADD COLUMN requested_frequency_hz text,
    ADD COLUMN result_bytes bytea,
    ADD COLUMN result_hash text,
    ADD COLUMN result_received_at timestamptz,
    ADD COLUMN late_result_bytes bytea,
    ADD COLUMN late_result_hash text,
    ADD COLUMN late_result_received_at timestamptz;
UPDATE public.twin_commands c SET target_entity_type=e.entity_type FROM public.twin_entities e WHERE c.entity_id=e.id;
ALTER TABLE public.twin_commands
    ALTER COLUMN target_entity_type SET NOT NULL,
    ADD CONSTRAINT fk_twin_commands_target_type FOREIGN KEY(entity_id,target_entity_type) REFERENCES public.twin_entities(id,entity_type),
    ADD CONSTRAINT chk_twin_commands_version CHECK(desired_version>=0),
    ADD CONSTRAINT chk_twin_commands_deadline CHECK(expires_at>issued_at),
    ADD CONSTRAINT chk_twin_commands_control CHECK(control_version IS NULL OR COALESCE((
        control_version=1 AND target_entity_type='Sensor' AND action='update_configuration'
        AND desired_version IS NOT NULL AND requested_frequency_hz IS NOT NULL
        AND octet_length(requested_frequency_hz) BETWEEN 1 AND 128
        AND parameters=jsonb_build_object('sample_interval_us',parameters->'sample_interval_us')
        AND jsonb_typeof(parameters->'sample_interval_us')='number'
        AND CASE WHEN parameters->>'sample_interval_us' ~ '^[0-9]+$'
            THEN (parameters->>'sample_interval_us')::numeric BETWEEN 10000 AND 9007199254740992 ELSE false END),false)),
    ADD CONSTRAINT chk_twin_commands_result CHECK (
        (result_bytes IS NULL AND result_hash IS NULL AND result_received_at IS NULL) OR
        (result_bytes IS NOT NULL AND octet_length(result_bytes) BETWEEN 1 AND 8192
         AND result_hash IS NOT NULL AND result_hash=encode(sha256(result_bytes),'hex') AND result_received_at IS NOT NULL)),
    ADD CONSTRAINT chk_twin_commands_late_result CHECK (
        (late_result_bytes IS NULL AND late_result_hash IS NULL AND late_result_received_at IS NULL) OR
        (late_result_bytes IS NOT NULL AND octet_length(late_result_bytes) BETWEEN 1 AND 8192
         AND late_result_hash IS NOT NULL AND late_result_hash=encode(sha256(late_result_bytes),'hex') AND late_result_received_at IS NOT NULL));

CREATE FUNCTION public.twin_command_target_type() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$ BEGIN
    SELECT entity_type INTO NEW.target_entity_type FROM public.twin_entities WHERE id=NEW.entity_id;
    RETURN NEW;
END $$;
CREATE TRIGGER twin_command_target_type BEFORE INSERT OR UPDATE OF entity_id,target_entity_type ON public.twin_commands
FOR EACH ROW EXECUTE FUNCTION public.twin_command_target_type();
REVOKE ALL ON FUNCTION public.twin_command_target_type() FROM PUBLIC;

CREATE UNIQUE INDEX uq_twin_commands_active_sensor ON public.twin_commands(entity_id)
WHERE target_entity_type='Sensor' AND status IN ('pending','published','acknowledged');
CREATE INDEX idx_twin_commands_entity_issued_id ON public.twin_commands(entity_id,issued_at DESC,command_id DESC);
-- Migration 8 actually made this unique (the early handoff described migration 2).
-- Requests now use actor/entity/key scope, including no-op. Preserve old rows/keys.
DROP INDEX public.idx_twin_commands_idempotency;
CREATE INDEX idx_twin_commands_idempotency ON public.twin_commands(entity_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
ALTER TABLE public.twin_states ADD CONSTRAINT chk_twin_states_versions CHECK(desired_version>=0 AND reported_version>=0);

ALTER TABLE public.twin_outbox ADD COLUMN payload_bytes bytea;
ALTER TABLE public.twin_outbox ADD CONSTRAINT chk_twin_outbox_bytes CHECK(payload_bytes IS NULL OR
    (octet_length(payload_bytes) BETWEEN 1 AND 8192 AND convert_from(payload_bytes,'UTF8')::jsonb=payload));
CREATE FUNCTION public.twin_outbox_payload_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE contract integer;
BEGIN
    SELECT control_version INTO contract FROM public.twin_commands WHERE command_id=NEW.command_id;
    IF (contract=1 AND NEW.payload_bytes IS NULL) OR (contract IS NULL AND NEW.payload_bytes IS NOT NULL) THEN
        RAISE EXCEPTION 'Stable payload bytes are required only for validated control intents; legacy payloads are not dispatchable';
    END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.command_id,NEW.topic,NEW.payload,NEW.payload_bytes)
        IS DISTINCT FROM ROW(OLD.command_id,OLD.topic,OLD.payload,OLD.payload_bytes) THEN
        RAISE EXCEPTION 'Outbox command identity and serialized payload are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER twin_outbox_payload_immutable BEFORE INSERT OR UPDATE ON public.twin_outbox
FOR EACH ROW EXECUTE FUNCTION public.twin_outbox_payload_immutable();
REVOKE ALL ON FUNCTION public.twin_outbox_payload_immutable() FROM PUBLIC;

CREATE TABLE public.twin_command_requests (
    actor_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
    entity_id uuid NOT NULL REFERENCES public.twin_entities(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 128 AND idempotency_key COLLATE "C" !~ '[^ -~]'),
    fingerprint text NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
    status_code integer NOT NULL CHECK(status_code IN(200,202)),
    response_bytes bytea NOT NULL CHECK(octet_length(response_bytes) BETWEEN 1 AND 8192),
    command_id uuid REFERENCES public.twin_commands(command_id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL,
    PRIMARY KEY(actor_id,entity_id,idempotency_key),
    CHECK((status_code=200 AND command_id IS NULL) OR (status_code=202 AND command_id IS NOT NULL))
);
ALTER TABLE public.twin_command_requests ENABLE ROW LEVEL SECURITY;
GRANT SELECT,INSERT ON public.twin_command_requests TO iot_backend;
REVOKE ALL ON public.twin_command_requests FROM PUBLIC,anon,authenticated;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON public.twin_states,public.twin_commands,public.twin_outbox FROM PUBLIC,anon,authenticated;

-- Canonical result/late evidence is write-once, bounded to one of each per command.
CREATE FUNCTION public.twin_command_result_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$ BEGIN
    IF (OLD.result_bytes IS NOT NULL AND ROW(NEW.result_bytes,NEW.result_hash,NEW.result_received_at)
          IS DISTINCT FROM ROW(OLD.result_bytes,OLD.result_hash,OLD.result_received_at))
       OR (OLD.late_result_bytes IS NOT NULL AND ROW(NEW.late_result_bytes,NEW.late_result_hash,NEW.late_result_received_at)
          IS DISTINCT FROM ROW(OLD.late_result_bytes,OLD.late_result_hash,OLD.late_result_received_at)) THEN
        RAISE EXCEPTION 'Canonical command result evidence is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER twin_command_result_immutable BEFORE UPDATE ON public.twin_commands
FOR EACH ROW EXECUTE FUNCTION public.twin_command_result_immutable();
REVOKE ALL ON FUNCTION public.twin_command_result_immutable() FROM PUBLIC;

INSERT INTO public.schema_migrations(version,name) VALUES(23,'sensor_control_intent');
COMMIT;
\else
\echo 'Auth is not initialized; apply migration 000023 after Auth and migration 22.'
\endif
\endif
SELECT pg_advisory_unlock(3290, 2);
