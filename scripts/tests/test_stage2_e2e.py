"""Unit guards for Task 2.7 E2E runner, membership tooling, and upgrade rehearsal."""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))


class MembershipToolGuards(unittest.TestCase):
    script = ROOT / "scripts/manage-gateway-membership.sh"

    def test_script_exists_and_is_file(self):
        self.assertTrue(self.script.is_file(), f"{self.script} must exist")

    def test_missing_environment_variables_fails_closed(self):
        result = subprocess.run(
            ["sh", str(self.script)],
            env={},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PGHOST", result.stderr)

    def test_invalid_actor_uuid_fails_closed(self):
        env = {
            "PGHOST": "127.0.0.1",
            "PGUSER": "test_user",
            "PGDATABASE": "test_db",
            "PGPASSWORD": "test_password",
        }
        result = subprocess.run(
            [
                "sh", str(self.script),
                "--journal-file", "/dev/null",
                "--actor", "not-a-uuid",
                "--target-user", "00000000-0000-4000-8000-000000000002",
                "--gateway", "gw_001",
                "--action", "grant",
                "--role", "viewer",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("UUID", result.stderr)

    def test_invalid_target_uuid_fails_closed(self):
        env = {
            "PGHOST": "127.0.0.1",
            "PGUSER": "test_user",
            "PGDATABASE": "test_db",
            "PGPASSWORD": "test_password",
        }
        result = subprocess.run(
            [
                "sh", str(self.script),
                "--journal-file", "/dev/null",
                "--actor", "00000000-0000-4000-8000-000000000001",
                "--target-user", "invalid-uuid",
                "--gateway", "gw_001",
                "--action", "grant",
                "--role", "viewer",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("UUID", result.stderr)

    def test_disallowed_role_fails_closed(self):
        env = {
            "PGHOST": "127.0.0.1",
            "PGUSER": "test_user",
            "PGDATABASE": "test_db",
            "PGPASSWORD": "test_password",
        }
        # owner role cannot be granted or changed via this sharing tool
        result = subprocess.run(
            [
                "sh", str(self.script),
                "--journal-file", "/dev/null",
                "--actor", "00000000-0000-4000-8000-000000000001",
                "--target-user", "00000000-0000-4000-8000-000000000002",
                "--gateway", "gw_001",
                "--action", "grant",
                "--role", "owner",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("role", result.stderr.lower())

    def test_invalid_action_fails_closed(self):
        env = {
            "PGHOST": "127.0.0.1",
            "PGUSER": "test_user",
            "PGDATABASE": "test_db",
            "PGPASSWORD": "test_password",
        }
        result = subprocess.run(
            [
                "sh", str(self.script),
                "--journal-file", "/dev/null",
                "--actor", "00000000-0000-4000-8000-000000000001",
                "--target-user", "00000000-0000-4000-8000-000000000002",
                "--gateway", "gw_001",
                "--action", "delete_all",
                "--role", "viewer",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("action", result.stderr.lower())


class E2EHarnessUnitGuards(unittest.TestCase):
    def test_harness_imports_and_selectors(self):
        import stage2_e2e
        selectors = stage2_e2e.VALID_SELECTORS
        expected = {
            "accounts/auth",
            "provisioning",
            "memberships/roles",
            "credentials/ACL",
            "rotate/revoke/replay",
            "loss/recovery",
            "restart/fences",
            "upgrade/repeatability",
        }
        self.assertEqual(set(selectors), expected)

    def test_redaction_prevents_leak(self):
        import stage2_e2e
        secret1 = "super_secret_token_12345"
        secret2 = "another_secret_password_67890"
        raw = f"Sending request with token {secret1} and pass {secret2} in url"
        redacted = stage2_e2e.redact(raw, [secret1, secret2])
        self.assertNotIn(secret1, redacted)
        self.assertNotIn(secret2, redacted)
        self.assertIn("[REDACTED]", redacted)

    def test_selector_validation_rejects_unknown_or_empty(self):
        import stage2_e2e
        with self.assertRaises(ValueError):
            stage2_e2e.validate_selectors([])
        with self.assertRaises(ValueError):
            stage2_e2e.validate_selectors(["unknown/selector"])

    def test_scenario_mapping_covers_all_scenarios(self):
        import stage2_e2e
        covered = set()
        for selector in stage2_e2e.VALID_SELECTORS:
            covered.update(stage2_e2e.SELECTOR_SCENARIOS[selector])
        expected = {f"E{i:02d}" for i in range(1, 28)}
        self.assertEqual(covered, expected)

    def test_bounded_probe_times_out(self):
        import stage2_e2e
        import time
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            stage2_e2e.wait_until(lambda: False, timeout_seconds=0.2, poll_interval=0.05, desc="test probe")
        duration = time.monotonic() - start
        self.assertLess(duration, 1.0)

    def test_cleanup_handles_unstarted_cleanly(self):
        import stage2_e2e
        stack = stage2_e2e.IsolatedTestStack()
        # Should not raise exception when cleaning an unstarted stack
        stack.cleanup()
        self.assertFalse(stack.d.exists())

    def test_no_duplicate_scenarios_across_selectors(self):
        import stage2_e2e
        seen = {}
        for selector, scenarios in stage2_e2e.SELECTOR_SCENARIOS.items():
            for sc in scenarios:
                self.assertNotIn(sc, seen, f"Scenario {sc} mapped to multiple selectors: {seen.get(sc)} and {selector}")
                seen[sc] = selector

    def test_test_password_not_hardcoded_in_source(self):
        source = (ROOT / "scripts/tests/stage2_e2e.py").read_text()
        self.assertNotIn('"123" + "456"', source, "Plan password must not be hardcoded in stage2_e2e.py source")
        self.assertNotIn('"123456"', source, "Plan password must not be hardcoded in stage2_e2e.py source")

    def test_false_pass_rejected_when_scenario_fails_or_not_run(self):
        import stage2_e2e
        results = {"E18": ("PASS", ""), "E19": ("PASS", ""), "E20": ("PASS", ""), "E21": ("PASS", ""), "E22": ("NOT RUN", "")}
        status = stage2_e2e.compute_selector_status("rotate/revoke/replay", results)
        self.assertNotEqual(status, "PASS", "Selector with NOT RUN scenario must not be marked PASS")
        self.assertIn("NOT RUN", status)

    def test_cleanup_aggregates_failures_and_raises(self):
        import stage2_e2e
        from unittest import mock
        stack = stage2_e2e.IsolatedTestStack()
        stack.containers.append("container_stuck_in_removal")
        # Mock docker inspect returning returncode 0 (meaning container is still present)
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0, stderr="still running", stdout="{}")
            with self.assertRaises(RuntimeError) as ctx:
                stack.cleanup(verify_all_removed=True)
            self.assertIn("still present", str(ctx.exception))

    def test_cleanup_fails_on_daemon_error(self):
        import stage2_e2e
        from unittest import mock
        stack = stage2_e2e.IsolatedTestStack()
        stack.containers.append("some_container")
        # Mock docker inspect returning nonzero with a daemon error (not "no such container")
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=1, stderr="Cannot connect to the Docker daemon", stdout="")
            with self.assertRaises(RuntimeError) as ctx:
                stack.cleanup(verify_all_removed=True)
            self.assertIn("uncertain", str(ctx.exception))

    def test_secret_scan_detects_short_secret(self):
        import stage2_e2e
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            f.write("Log line with short secret: abc123\n")
            log_path = Path(f.name)
        try:
            with self.assertRaises(RuntimeError):
                stage2_e2e.scan_log_for_secrets(log_path, ["abc123"])
        finally:
            log_path.unlink(missing_ok=True)

    def test_secret_scan_fails_on_missing_log(self):
        import stage2_e2e
        missing = Path("/tmp/non_existent_log_file_12345.log")
        with self.assertRaises(RuntimeError):
            stage2_e2e.scan_log_for_secrets(missing, ["some_secret"])

    def test_e12_admin_routes_matrix_completeness(self):
        import stage2_e2e
        expected_endpoints = {
            ("PUT", "/v1/admin/gateways/gw_test"),
            ("PUT", "/v1/admin/gateways/gw_test/sensors/s1"),
            ("GET", "/v1/admin/gateways/gw_a/mqtt-credential"),
            ("POST", "/v1/admin/gateways/gw_a/mqtt-credential"),
            ("POST", "/v1/admin/gateways/gw_a/mqtt-credential/rotate"),
            ("DELETE", "/v1/admin/gateways/gw_a/mqtt-credential"),
        }
        self.assertTrue(hasattr(stage2_e2e, "ADMIN_ROUTES"), "stage2_e2e must define ADMIN_ROUTES")
        defined = {(m, p) for m, p in stage2_e2e.ADMIN_ROUTES}
        self.assertEqual(defined, expected_endpoints)

    def test_run_specific_logging_configuration(self):
        import stage2_e2e
        custom_log = Path("/tmp/opencode/custom_run_test.log")
        with mock.patch.dict(os.environ, {"STAGE2_E2E_LOG": str(custom_log)}):
            stack = stage2_e2e.IsolatedTestStack()
            self.assertEqual(stack.log_file, custom_log)
            stack.cleanup()

    def test_exit_masking_in_runner_script(self):
        # Test that if a command piped to tee fails, test-stage2-e2e.sh propagates the failure
        # Run test-stage2-e2e.sh with a flag that skips recursive unit guard discovery and uses a probe log
        probe_log = Path("/tmp/opencode/unit_guard_exit_masking_probe.log")
        res = subprocess.run(
            ["sh", str(ROOT / "scripts/test-stage2-e2e.sh"), "invalid/selector/should/fail"],
            env=dict(os.environ, STAGE2_SKIP_UNIT_GUARDS="1", STAGE2_E2E_LOG=str(probe_log)),
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res.returncode, 0, "test-stage2-e2e.sh must propagate non-zero exit codes from python")


class MembershipToolJournalGuards(unittest.TestCase):
    script = ROOT / "scripts/manage-gateway-membership.sh"

    def test_requires_durable_journal_target(self):
        env = {
            "PGHOST": "127.0.0.1",
            "PGUSER": "test_user",
            "PGDATABASE": "test_db",
            "PGPASSWORD": "test_password",
        }
        # Calling without --journal-file or GATEWAY_MEMBERSHIP_JOURNAL_FILE must fail closed
        res = subprocess.run(
            [
                "sh", str(self.script),
                "--actor", "00000000-0000-4000-8000-000000000001",
                "--target-user", "00000000-0000-4000-8000-000000000002",
                "--gateway", "gw_001",
                "--action", "grant",
                "--role", "viewer",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("journal", res.stderr.lower())


if __name__ == "__main__":
    unittest.main()
