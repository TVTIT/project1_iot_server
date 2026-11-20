"""Focused migration contract checks for Stage 8 managed media metadata."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


class Stage8MediaSchemaTest(unittest.TestCase):
    def test_migration_28_declares_managed_media_contract(self):
        migration = ROOT / "migrations/000028_stage8_media_metadata.up.sql"
        self.assertTrue(migration.exists(), "Migration 28 must exist")
        sql = migration.read_text()
        for required in (
            "pg_advisory_lock(3290, 2)",
            "version = 28 AND name <> 'stage8_media_metadata'",
            "version = 27 AND name = 'stage6_control_resilience'",
            "uq_media_objects_gateway_upload",
            "storage_path",
            "captured_at",
            "intent_expires_at",
            "declared_sha256",
            "verified_sha256",
            "actual_media_type",
            "actual_media_type = media_type",
            "latest_upload_grant_expires_at",
            "verification_generation",
            "upload_grant_generation",
            "cleanup_scheduled_at",
            "intent_expires_at",
            "video/mp4",
            "^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
            "media_id::text || '.mp4'",
            "verification_attempts",
            "next_verification_at",
            "next_reconcile_at",
            "terminal_reason",
            "cleanup_scheduled_at",
            "cleanup_confirmed_at",
            "80000000",
            "ready",
            "schema_migrations(version, name) VALUES(28, 'stage8_media_metadata')",
            "pg_advisory_unlock(3290, 2)",
        ):
            with self.subTest(required=required):
                self.assertIn(required, sql)
        self.assertNotIn("token", sql.lower())
        self.assertNotIn("signed_url", sql.lower())

    def test_migration_28_preserves_legacy_rows_and_managed_only_uniqueness(self):
        sql = (ROOT / "migrations/000028_stage8_media_metadata.up.sql").read_text()
        self.assertIn("ADD COLUMN managed boolean NOT NULL DEFAULT false", sql)
        self.assertIn("WHERE managed", sql)
        self.assertIn("upload_id IS NOT NULL", sql)
        self.assertIn("ADD COLUMN upload_id text", sql)
        self.assertIn("validation_status IN ('pending', 'verifying', 'ready', 'rejected', 'expired')", sql)
        self.assertNotIn("'cleanup_pending'", sql)
        self.assertIn("WHERE managed IS TRUE", sql)
        self.assertIn("validation_status", sql)

    def test_schema_terminal_pins_and_fixture_cover_28(self):
        compat = (ROOT / "scripts/tests/migration_ci_compat.py").read_text()
        compose = (ROOT / "docker-compose.yml").read_text()
        shell = (ROOT / "scripts/test-stage2-migrations.sh").read_text()
        smoke = (ROOT / "scripts/tests/native_compose_smoke.py").read_text()
        self.assertIn("TERMINAL_SCHEMA_VERSION = 28", compat)
        self.assertIn("expected_schema_terminal=28", shell)
        self.assertIn("28:stage8_media_metadata", smoke)
        self.assertIn("000028_stage8_media_metadata.up.sql", compose)
        self.assertIn("verify-migration-000028.sql", shell)
        self.assertIn("migration28 = (ROOT / 'migrations/000028_stage8_media_metadata.up.sql').read_text()", compat)
        self.assertIn("Migration 28 from exact v27 preserved legacy media metadata", compat)
        self.assertIn("clip-A_01", compat)

    def test_next_slice_column_assumptions_are_documented(self):
        docs = ROOT / "docs/backend/stage-8.2a-media-schema.md"
        self.assertTrue(docs.exists(), "Stage 8.2a column/API assumptions are required")
        text = docs.read_text()
        for term in ("upload_id", "declared_sha256", "verified_sha256",
                     "latest_upload_grant_expires_at", "verification_generation",
                     "cleanup_confirmed_at", "legacy rows", "80,000,000",
                     "video/mp4", "cleanup_scheduled_at", "Cleanup must not change"):
            self.assertIn(term, text)


if __name__ == "__main__":
    unittest.main()
