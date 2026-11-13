"""Focused source-contract checks for migration 24; native DB cases live in P5b.1b."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations/000024_telemetry_ledger_timestamp.up.sql"


class Migration24ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text()
        cls.normalized = re.sub(r"\s+", " ", cls.sql.lower())

    def test_additive_nullable_timestamp_and_four_column_unique_index(self):
        self.assertRegex(self.normalized, r"add column timestamp timestamptz(?:\s|;)")
        self.assertNotRegex(self.normalized, r"timestamp timestamptz[^,;]*\bnot null\b")
        self.assertNotRegex(self.normalized, r"timestamp timestamptz[^,;]*\bdefault\b")
        self.assertRegex(self.normalized, r"create unique index (?:if not exists )?processed_messages_gateway_sensor_message_timestamp\s+on public\.processed_messages\s*\(gateway_id,\s*sensor_id,\s*message_id,\s*timestamp\)")

    def test_backfill_uses_validated_sample_zero_and_full_provenance(self):
        self.assertIn("sample_index = 0", self.normalized)
        self.assertIn("t.gateway_id = p.gateway_id", self.normalized)
        self.assertIn("t.message_id = p.message_id", self.normalized)
        for field in ("sensor_id", "boot_id"):
            self.assertIn(f"t.{field} = p.{field}", self.normalized)
        self.assertIn("t.sequence_num = p.first_sequence", self.normalized)
        self.assertIn("set timestamp = t.measured_at", self.normalized)
        self.assertRegex(self.normalized, r"set timestamp\s*=\s*(?:t|r)\.measured_at")

    def test_mismatch_refuses_before_update_and_missing_source_stays_null(self):
        mismatch = self.normalized.find("conflicting telemetry provenance")
        update = self.normalized.find("update public.processed_messages")
        self.assertGreaterEqual(mismatch, 0)
        self.assertGreater(update, mismatch)
        self.assertIn("timestamp is null", self.normalized)
        self.assertRegex(self.normalized, r"raise notice 'migration 24 retained % legacy ledger rows with unknown original timestamp")
        for field in ("source_topic", "last_sequence", "sample_count"):
            self.assertIn(f"p.{field} is not null", self.normalized)

    def test_history_guard_lock_and_bounded_transaction(self):
        self.assertIn("pg_advisory_lock(3290, 2)", self.normalized)
        self.assertIn("lock_timeout = '10s'", self.normalized)
        self.assertIn("statement_timeout = '60s'", self.normalized)
        self.assertIn("begin;", self.normalized)
        self.assertIn("commit;", self.normalized)
        self.assertIn("version = 24", self.normalized)
        self.assertIn("telemetry_ledger_timestamp", self.normalized)
        self.assertIn("version = 23", self.normalized)
        self.assertIn("primary key (gateway_id, message_id)", self.normalized)
        self.assertIn("processed_stream_range", self.normalized)


if __name__ == "__main__":
    unittest.main()
