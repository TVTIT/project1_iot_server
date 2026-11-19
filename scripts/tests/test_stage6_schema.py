"""Focused regression tests for Stage 6 PostgreSQL schema (Migration 27)."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class Stage6SchemaTest(unittest.TestCase):
    def test_migration_27_file_exists_and_contract(self):
        migration_file = ROOT / 'migrations/000027_stage6_control_resilience.up.sql'
        self.assertTrue(migration_file.exists(), "Migration 27 file must exist")
        sql = migration_file.read_text()

        # Advisory lock & safe idempotent guards
        self.assertIn("SELECT pg_advisory_lock(3290, 2);", sql)
        self.assertIn("SELECT pg_advisory_unlock(3290, 2);", sql)
        self.assertIn("version = 27 AND name <> 'stage6_control_resilience'", sql)
        self.assertIn("version = 26 AND name = 'drop_telemetry_redundant_indexes'", sql)
        self.assertIn("INSERT INTO public.schema_migrations(version, name) VALUES(27, 'stage6_control_resilience')", sql)

        # Columns on twin_commands
        self.assertIn("ADD COLUMN stage6_managed boolean NOT NULL DEFAULT false", sql)
        self.assertIn("ADD COLUMN first_reserved_at timestamptz", sql)
        self.assertIn("ADD COLUMN retry_stop_reason text", sql)
        self.assertIn("ADD COLUMN receipt_bytes bytea", sql)
        self.assertIn("ADD COLUMN receipt_hash text", sql)
        self.assertIn("ADD COLUMN receipt_received_at timestamptz", sql)
        self.assertIn("ADD COLUMN cancelled_at timestamptz", sql)
        self.assertIn("ADD COLUMN cancelled_by uuid", sql)
        self.assertIn("ADD COLUMN origin text NOT NULL DEFAULT 'client'", sql)
        self.assertIn("ADD COLUMN root_command_id uuid", sql)
        self.assertIn("ADD COLUMN parent_command_id uuid", sql)
        self.assertIn("ADD COLUMN original_actor_id uuid", sql)
        self.assertIn("ADD COLUMN reconcile_blocked boolean NOT NULL DEFAULT false", sql)
        self.assertIn("ADD COLUMN auto_successors_used integer NOT NULL DEFAULT 0", sql)

        # Columns on twin_states
        self.assertIn("ADD COLUMN next_query_at timestamptz", sql)
        self.assertIn("ADD COLUMN last_reconciled_at timestamptz", sql)
        self.assertIn("ADD COLUMN last_reconcile_decision text", sql)

        # Constraints
        self.assertIn("chk_twin_commands_receipt", sql)
        self.assertIn("chk_twin_commands_cancellation", sql)
        self.assertIn("chk_twin_commands_lineage", sql)
        self.assertIn("chk_twin_commands_origin", sql)
        self.assertIn("chk_twin_commands_retry_stop_reason", sql)
        self.assertIn("chk_twin_states_reconcile_decision", sql)

        # Immutability triggers
        self.assertIn("CREATE FUNCTION public.twin_command_stage6_immutable()", sql)
        self.assertIn("CREATE TRIGGER twin_command_stage6_immutable", sql)
        self.assertIn("REVOKE ALL ON FUNCTION public.twin_command_stage6_immutable() FROM PUBLIC", sql)

        # Indexes
        self.assertIn("CREATE INDEX idx_twin_states_reconcile_due", sql)
        self.assertIn("CREATE INDEX idx_twin_commands_root", sql)

    def test_schema_terminal_guards_match_migration_27(self):
        compat_source = (ROOT / 'scripts/tests/migration_ci_compat.py').read_text()
        self.assertIn("TERMINAL_SCHEMA_VERSION = 27", compat_source)

        runner_source = (ROOT / 'scripts/test-stage2-migrations.sh').read_text()
        self.assertIn("expected_schema_terminal=27", runner_source)
        self.assertIn("versions 20–27 fresh/upgrade schema verification passed", runner_source)


if __name__ == '__main__':
    unittest.main()
