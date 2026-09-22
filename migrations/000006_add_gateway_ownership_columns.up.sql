-- Add authorization ownership and desired-state audit columns.
-- Columns are nullable here so existing rows can be backfilled safely in 000007.
\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE twin_entities
    ADD COLUMN IF NOT EXISTS gateway_id TEXT;

ALTER TABLE twin_states
    ADD COLUMN IF NOT EXISTS last_desired_by UUID;

COMMIT;
