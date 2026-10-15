"""Migration CI acceptance contracts; no Docker or fixture execution.

These tests enforce safe wiring without silently dropping the old compatibility checks.
Only the migration-check section is inspected; other CI jobs are out of scope.
"""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
IMAGE = (
    "timescale/timescaledb@sha256:"
    "289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
)


class MigrationJobContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        match = re.search(r"^  migration-check:\n.*?(?=^  \S|\Z)", workflow, re.M | re.S)
        if match is None:
            raise AssertionError("migration-check job is required")
        cls.job = match.group()
        cls.harness = (ROOT / "scripts/test-stage2-migrations.sh").read_text()
        cls.compat = (ROOT / "scripts/tests/migration_ci_compat.py").read_text()

    def test_job_runs_migration_unit_and_wiring_contracts(self):
        for suite in ("test_ci_migration19.py", "test_ci_migration_job.py"):
            self.assertIn(suite, self.job)
        self.assertNotIn("continue-on-error", self.job)
        self.assertIn("test_migration_ci_compat.py", self.job)
        self.assertIn("python3 scripts/tests/migration_ci_compat.py", self.job)

    def test_job_has_finite_timeout_and_prepares_harness_directory(self):
        self.assertRegex(self.job, r"timeout-minutes: [1-9][0-9]*")
        self.assertIn("mkdir -p /tmp/opencode", self.job)
        self.assertLess(self.job.index("mkdir -p /tmp/opencode"),
                        self.job.index("sh scripts/test-stage2-migrations.sh"))

    def test_dependency_is_digest_pinned_and_acquired_before_harness(self):
        self.assertIn("docker pull --platform linux/amd64 " + IMAGE, self.job)
        self.assertLess(self.job.index("docker pull"),
                        self.job.index("sh scripts/test-stage2-migrations.sh"))
        self.assertIn("--pull=never", self.harness)

    def test_latest_gate_matches_exact_version_and_name(self):
        self.assertNotIn("WHERE version = 16 AND name", self.job)
        self.assertIn('wait_for_version "$FRESH_DB" 19 mqtt_simplified_journal',
                      self.harness)
        self.assertIn("verify-migration-000019.sql", self.harness)

    def test_hardening_and_core_schema_coverage_is_not_dropped(self):
        for verifier in ("verify-schema-hardening.sql", "verify-migration-000009.sql"):
            self.assertIn(verifier, self.compat)

    def test_supabase_compatibility_and_service_role_logins_are_retained(self):
        combined = self.compat
        for assertion in ("CREATE TABLE auth.users", "000004_supabase_compat.up.sql",
                          "pg_policies", "on_auth_user_created", "profiles_id_fkey",
                          "self.login('supabase_auth_admin'", "self.login('supabase_storage_admin'"):
            self.assertIn(assertion, combined)
        self.assertIn("for _ in range(2):", combined)

    def test_job_and_invoked_harness_do_not_use_fixed_passwords(self):
        combined = self.job + self.harness + self.compat
        self.assertNotRegex(combined, r"(?:PASSWORD|PASSWORDS?)=(?:test_|stage2_)")
        self.assertNotRegex(combined, r'PASSWORD="(?:test_|stage2_)')

    def test_no_raw_logs_dsns_or_fixture_artifact_uploads(self):
        combined = self.job + self.harness + self.compat
        self.assertNotRegex(combined, r"docker logs[^\n]*(?:\n|$)")
        self.assertNotRegex(combined, r"postgres(?:ql)?://")
        self.assertNotIn("upload-artifact", self.job)
        self.assertNotIn(".env", self.job)

    def test_cleanup_is_owned_and_failure_vetoes_success(self):
        self.assertNotRegex(self.job, r"docker rm[^\n]*\|\| true")
        self.assertIn('"stage2.migration-run=$RUN_ID"', self.harness)
        self.assertIn('if [ "$owner" != "$RUN_ID" ]', self.harness)
        self.assertIn('if [ "$cleanup_status" -ne 0 ]; then', self.harness)
        self.assertIn("timeout 30 docker rm -fv", self.harness)


if __name__ == "__main__":
    unittest.main()
