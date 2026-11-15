-- Migration 27: Stage 6 control resilience, retry schedule, safe cancellation,
-- receipt evidence, and reconciliation lineage.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.schema_migrations
               WHERE version = 27 AND name <> 'stage6_control_resilience') THEN
        RAISE EXCEPTION 'Migration 27 has an unexpected name';
    END IF;
END $$;

SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 27) AS apply_stage6_control_resilience \gset
\if :apply_stage6_control_resilience

-- Predecessor guard: fail-closed if migration 26 was not applied
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations
                   WHERE version = 26 AND name = 'drop_telemetry_redundant_indexes') THEN
        RAISE EXCEPTION 'Migration 27 requires predecessor migration 26 drop_telemetry_redundant_indexes';
    END IF;
END $$;

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE public.twin_entities, public.twin_states, public.twin_commands, public.twin_outbox IN ACCESS EXCLUSIVE MODE;

-- 1. Add Stage 6 control resilience columns to twin_commands
-- stage6_managed: false for all legacy rows and Stage 5-off inserts (UNKNOWN history)
-- cancelled_by and original_actor_id: immutable actor UUID snapshots without FK to preserve user deletion semantics
ALTER TABLE public.twin_commands
    ADD COLUMN stage6_managed boolean NOT NULL DEFAULT false,
    ADD COLUMN first_reserved_at timestamptz,
    ADD COLUMN retry_stop_reason text,
    ADD COLUMN receipt_bytes bytea,
    ADD COLUMN receipt_hash text,
    ADD COLUMN receipt_received_at timestamptz,
    ADD COLUMN cancelled_at timestamptz,
    ADD COLUMN cancelled_by uuid,
    ADD COLUMN origin text NOT NULL DEFAULT 'client',
    ADD COLUMN root_command_id uuid,
    ADD COLUMN parent_command_id uuid,
    ADD COLUMN original_actor_id uuid,
    ADD COLUMN reconcile_blocked boolean NOT NULL DEFAULT false,
    ADD COLUMN auto_successors_used integer NOT NULL DEFAULT 0;

-- 2. Add reconciliation schedule and decision columns to twin_states
ALTER TABLE public.twin_states
    ADD COLUMN next_query_at timestamptz,
    ADD COLUMN last_reconciled_at timestamptz,
    ADD COLUMN last_reconcile_decision text;

-- 3. Integrity and boundary constraints
ALTER TABLE public.twin_commands
    ADD CONSTRAINT chk_twin_commands_origin
        CHECK (origin IN ('client', 'reconciliation')),
    ADD CONSTRAINT chk_twin_commands_retry_stop_reason
        CHECK (retry_stop_reason IS NULL OR retry_stop_reason IN ('retry_exhausted', 'acknowledged', 'cancelled', 'timeout', 'result_received')),
    ADD CONSTRAINT chk_twin_commands_auto_successors
        CHECK (
            (origin = 'client' AND auto_successors_used IN (0, 1)) OR
            (origin = 'reconciliation' AND auto_successors_used = 0)
        ),
    ADD CONSTRAINT chk_twin_commands_lineage
        CHECK (
            (origin = 'client' AND parent_command_id IS NULL AND (root_command_id IS NULL OR root_command_id = command_id)) OR
            (origin = 'reconciliation' AND parent_command_id IS NOT NULL AND root_command_id IS NOT NULL)
        ),
    ADD CONSTRAINT chk_twin_commands_receipt
        CHECK (
            (receipt_bytes IS NULL AND receipt_hash IS NULL AND receipt_received_at IS NULL) OR
            (receipt_bytes IS NOT NULL AND octet_length(receipt_bytes) BETWEEN 1 AND 8192
             AND receipt_hash IS NOT NULL AND receipt_hash = encode(sha256(receipt_bytes), 'hex')
             AND receipt_received_at IS NOT NULL)
        ),
    ADD CONSTRAINT chk_twin_commands_cancellation
        CHECK (
            (stage6_managed = true AND status = 'cancelled' AND cancelled_at IS NOT NULL) OR
            (stage6_managed = false AND status = 'cancelled') OR
            (status <> 'cancelled' AND cancelled_at IS NULL AND cancelled_by IS NULL)
        ),
    ADD CONSTRAINT chk_twin_commands_reservation
        CHECK (
            (first_reserved_at IS NULL) OR
            (first_reserved_at IS NOT NULL AND attempt_count >= 1)
        ),
    ADD CONSTRAINT fk_twin_commands_root
        FOREIGN KEY (root_command_id) REFERENCES public.twin_commands(command_id) ON DELETE RESTRICT,
    ADD CONSTRAINT fk_twin_commands_parent
        FOREIGN KEY (parent_command_id) REFERENCES public.twin_commands(command_id) ON DELETE RESTRICT;

ALTER TABLE public.twin_states
    ADD CONSTRAINT chk_twin_states_reconcile_decision
        CHECK (last_reconcile_decision IS NULL OR last_reconcile_decision IN (
            'converged', 'successor_created', 'active_busy', 'blocked_cancelled',
            'blocked_budget', 'manual_unconfirmed', 'corrupt_unavailable', 'precondition_unmet'
        ));

-- 4. Immutability trigger for receipt, cancellation, and reservation barrier evidence
CREATE FUNCTION public.twin_command_stage6_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$ BEGIN
    IF (OLD.receipt_bytes IS NOT NULL AND ROW(NEW.receipt_bytes, NEW.receipt_hash, NEW.receipt_received_at)
          IS DISTINCT FROM ROW(OLD.receipt_bytes, OLD.receipt_hash, OLD.receipt_received_at)) THEN
        RAISE EXCEPTION 'Canonical command receipt evidence is immutable';
    END IF;
    IF (OLD.cancelled_at IS NOT NULL AND ROW(NEW.cancelled_at, NEW.cancelled_by)
          IS DISTINCT FROM ROW(OLD.cancelled_at, OLD.cancelled_by)) THEN
        RAISE EXCEPTION 'Command cancellation evidence is immutable';
    END IF;
    IF (OLD.first_reserved_at IS NOT NULL AND NEW.first_reserved_at IS DISTINCT FROM OLD.first_reserved_at) THEN
        RAISE EXCEPTION 'First reservation timestamp is immutable once written';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER twin_command_stage6_immutable BEFORE UPDATE ON public.twin_commands
FOR EACH ROW EXECUTE FUNCTION public.twin_command_stage6_immutable();

REVOKE ALL ON FUNCTION public.twin_command_stage6_immutable() FROM PUBLIC;

-- 5. Supporting indexes for due reconciliation scan and root command tracking
CREATE INDEX idx_twin_states_reconcile_due ON public.twin_states(next_query_at)
WHERE next_query_at IS NOT NULL;

CREATE INDEX idx_twin_commands_root ON public.twin_commands(root_command_id)
WHERE root_command_id IS NOT NULL;

-- 6. Access privileges and RLS enforcement
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON public.twin_states,public.twin_commands FROM PUBLIC,anon,authenticated;

-- 7. Record migration 27 in schema_migrations
INSERT INTO public.schema_migrations(version, name) VALUES(27, 'stage6_control_resilience');

COMMIT;
\endif
SELECT pg_advisory_unlock(3290, 2);
