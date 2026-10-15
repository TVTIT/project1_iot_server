#!/usr/bin/env python3
"""Unit tests for scripts/ci_go_test_guard.py.

Verifies:
1. Happy path: multi-package and subtest execution with terminal pass
2. Missing required test
3. Skipped required test
4. Failed descendant subtest even when parent test reported pass
5. Package fail terminal event
6. Missing terminal event / truncated input
7. Malformed JSON lines
8. Tests with identical names in different packages
9. Raw Output containing secrets never appears in diagnostics/stdout/stderr
10. CLI exit codes: 0 for success, non-zero (e.g. 1 or 2) for failures/invalid usage
"""

import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

# When ci_go_test_guard is implemented, it should be importable
try:
    from scripts import ci_go_test_guard
except ImportError:
    # Allow fallback if scripts is not a package or run directly
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    try:
        import ci_go_test_guard
    except ImportError:
        ci_go_test_guard = None


class TestCiGoTestGuard(unittest.TestCase):
    def setUp(self):
        if ci_go_test_guard is None:
            self.fail("ci_go_test_guard module could not be imported")

    def _write_jsonl(self, events):
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".jsonl")
        for event in events:
            if isinstance(event, str):
                tmp.write(event + "\n")
            else:
                tmp.write(json.dumps(event) + "\n")
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        return tmp.name

    def test_happy_path_multi_package_and_subtests(self):
        """Happy path: multiple packages, required root tests and subtests all run and pass, with terminal package pass."""
        events = [
            {"Time": "2026-10-13T00:00:00Z", "Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "run", "Package": "pkg/auth", "Test": "TestLogin/ValidUser"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "output", "Package": "pkg/auth", "Test": "TestLogin/ValidUser", "Output": "ok\n"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "pass", "Package": "pkg/auth", "Test": "TestLogin/ValidUser"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "pass", "Package": "pkg/auth"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "run", "Package": "pkg/twin", "Test": "TestReconcile"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "pass", "Package": "pkg/twin", "Test": "TestReconcile"},
            {"Time": "2026-10-13T00:00:00Z", "Action": "pass", "Package": "pkg/twin"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin", "pkg/twin:TestReconcile"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertTrue(success, f"Expected success, got diags: {diags}")

    def test_missing_required_test(self):
        """Fails if a required test was never run."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestOther"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestOther"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestMissing"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("missing" in d.lower() and "TestMissing" in d for d in diags))

    def test_skipped_required_test(self):
        """Fails if a required test was skipped instead of passing."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestSkipped"},
            {"Action": "skip", "Package": "pkg/auth", "Test": "TestSkipped"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestSkipped"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("skip" in d.lower() and "TestSkipped" in d for d in diags))

    def test_failed_descendant_subtest_even_if_parent_passed(self):
        """Fails if a descendant subtest failed or skipped, even if parent test has action pass."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestParent"},
            {"Action": "run", "Package": "pkg/auth", "Test": "TestParent/Child"},
            {"Action": "fail", "Package": "pkg/auth", "Test": "TestParent/Child"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestParent"},  # Anomalous or deceptive parent pass
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestParent"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("subtest" in d.lower() or "fail" in d.lower() for d in diags))

    def test_package_failed(self):
        """Fails if package has terminal action fail."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "fail", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("package" in d.lower() and "fail" in d.lower() for d in diags))

    def test_missing_terminal_or_truncated(self):
        """Fails if package is missing terminal pass (e.g. truncated log or crash)."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            # missing package pass terminal event
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("terminal" in d.lower() or "incomplete" in d.lower() for d in diags))

    def test_malformed_json_lines(self):
        """Fails on malformed / unparseable JSON lines."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            "this is not valid json {{{",
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        self.assertTrue(any("malformed" in d.lower() or "json" in d.lower() for d in diags))

    def test_same_test_name_different_packages(self):
        """Differentiates tests with identical names across different packages."""
        events = [
            {"Action": "run", "Package": "pkg/a", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/a", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/a"},
            {"Action": "run", "Package": "pkg/b", "Test": "TestCommon"},
            {"Action": "fail", "Package": "pkg/b", "Test": "TestCommon"},
            {"Action": "fail", "Package": "pkg/b"},
        ]
        path = self._write_jsonl(events)

        # Require only pkg/a:TestCommon -> should succeed if only checking pkg/a and pkg/b wasn't required
        # Wait, if pkg/b failed, does package failure in pkg/b fail the overall guard?
        # Specification says: "package fail, hoặc thiếu package terminal pass. Exit nonzero nếu bắt buộc không có run+pass, có skip/fail (kể cả descendant subtest của required test), malformed/truncated JSON, package fail, hoặc thiếu package terminal pass."
        # If any required package fails, it definitely fails. If an unrequired package in the log failed, does it fail?
        # Let's test requiring pkg/a:TestCommon when pkg/b also exists. If pkg/b has terminal pass, pkg/a succeeds:
        events_isolated = [
            {"Action": "run", "Package": "pkg/a", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/a", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/a"},
            {"Action": "run", "Package": "pkg/b", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/b", "Test": "TestCommon"},
            {"Action": "pass", "Package": "pkg/b"},
        ]
        path_isolated = self._write_jsonl(events_isolated)
        success_a, _ = ci_go_test_guard.verify_test_log(path_isolated, ["pkg/a:TestCommon"])
        self.assertTrue(success_a)

        # If we require pkg/b:TestCommon with failure in pkg/b
        success_b, diags_b = ci_go_test_guard.verify_test_log(path, ["pkg/b:TestCommon"])
        self.assertFalse(success_b)

    def test_output_secret_not_leaked_in_diagnostics(self):
        """Secrets present in raw 'Output' events must never be printed or leaked into diagnostics."""
        secret = "SUPER_SECRET_TOKEN_XYZ123456"
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestSecret"},
            {"Action": "output", "Package": "pkg/auth", "Test": "TestSecret", "Output": f"token is {secret}\n"},
            {"Action": "fail", "Package": "pkg/auth", "Test": "TestSecret"},
            {"Action": "fail", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestSecret"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success)
        for diag in diags:
            self.assertNotIn(secret, diag, f"Secret leaked in diagnostic: {diag}")

    def test_cached_output_without_run_rejected(self):
        """Reject if test has output/pass from cache but no 'run' event."""
        # Go test -json on cached package might output (cached) or pass without test run event
        events = [
            {"Action": "output", "Package": "pkg/auth", "Output": "=== RUN   TestLogin\n"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},  # No Action: run
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success, "Should fail because test was not explicitly run (Action: run missing)")

    def test_regression_required_toplevel_run_skip_pass_fails(self):
        """(1) Required top-level with run+skip+pass or run+fail+pass must fail even if pass is present."""
        # Case A: run + skip + pass
        events_skip = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestFlaky"},
            {"Action": "skip", "Package": "pkg/auth", "Test": "TestFlaky"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestFlaky"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path_skip = self._write_jsonl(events_skip)
        success_skip, diags_skip = ci_go_test_guard.verify_test_log(path_skip, ["pkg/auth:TestFlaky"])
        self.assertFalse(success_skip, "Should fail when required test has skip action despite pass")
        self.assertTrue(any("skip" in d.lower() for d in diags_skip))

        # Case B: run + fail + pass
        events_fail = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestFlakyFail"},
            {"Action": "fail", "Package": "pkg/auth", "Test": "TestFlakyFail"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestFlakyFail"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path_fail = self._write_jsonl(events_fail)
        success_fail, diags_fail = ci_go_test_guard.verify_test_log(path_fail, ["pkg/auth:TestFlakyFail"])
        self.assertFalse(success_fail, "Should fail when required test has fail action despite pass")
        self.assertTrue(any("fail" in d.lower() for d in diags_fail))

    def test_regression_descendant_run_without_terminal_pass_fails(self):
        """(2) Descendant subtest with run but no terminal pass must fail even if parent/package pass."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestParent"},
            {"Action": "run", "Package": "pkg/auth", "Test": "TestParent/Child"},
            # Notice TestParent/Child was started with 'run' but never finished with 'pass'
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestParent"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        success, diags = ci_go_test_guard.verify_test_log(path, ["pkg/auth:TestParent"])
        self.assertFalse(success, "Should fail if descendant subtest ran but never completed with pass")
        self.assertTrue(any("subtest" in d.lower() and ("pass" in d.lower() or "incomplete" in d.lower()) for d in diags))

    def test_regression_all_seen_packages_must_terminal_pass(self):
        """(3) ALL packages appearing in log must terminal pass, not just req_packages."""
        events = [
            {"Action": "run", "Package": "pkg/req", "Test": "TestReq"},
            {"Action": "pass", "Package": "pkg/req", "Test": "TestReq"},
            {"Action": "pass", "Package": "pkg/req"},
            # Another package pkg/other appeared (ran tests or output), but never got terminal pass
            {"Action": "run", "Package": "pkg/other", "Test": "TestOther"},
            {"Action": "pass", "Package": "pkg/other", "Test": "TestOther"},
            # Missing package terminal pass for pkg/other
        ]
        path = self._write_jsonl(events)
        success, diags = ci_go_test_guard.verify_test_log(path, ["pkg/req:TestReq"])
        self.assertFalse(success, "Should fail if any package in log is missing terminal pass")
        self.assertTrue(any("pkg/other" in d for d in diags))

    def test_regression_invalid_json_types_and_unknown_actions_fail(self):
        """(4) Non-string types for Action/Package/Test or unknown actions must fail safe."""
        # Non-string Action
        events_bad_action = [
            {"Action": 123, "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path_bad_action = self._write_jsonl(events_bad_action)
        success, diags = ci_go_test_guard.verify_test_log(path_bad_action, ["pkg/auth:TestLogin"])
        self.assertFalse(success, "Should fail when event has non-string Action")

        # Unknown action
        events_unknown_action = [
            {"Action": "unexpected_action_xyz", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path_unknown_action = self._write_jsonl(events_unknown_action)
        success, diags = ci_go_test_guard.verify_test_log(path_unknown_action, ["pkg/auth:TestLogin"])
        self.assertFalse(success, "Should fail when event has unknown Action")

    def test_regression_diagnostics_sanitization_and_fixed_messages(self):
        """(5) Diagnostics must not echo raw spec with control chars/newlines/secrets, no raw file path echo."""
        secret_payload = "SECRET_LEAK\nINJECTION\x00_DATA"
        # Invalid require spec containing control chars
        success, diags = ci_go_test_guard.verify_test_log("some_file.jsonl", [f"bad_spec:{secret_payload}"])
        self.assertFalse(success)
        for d in diags:
            self.assertNotIn("SECRET_LEAK", d, "Raw secret or injection in spec must not be echoed")
            self.assertNotIn("\n", d, "Newline in spec must not be echoed into single diagnostic")
            self.assertNotIn("\x00", d, "Null char must not be in diagnostic")

        # File error does not print raw basename if path is non-existent
        success_file, diags_file = ci_go_test_guard.verify_test_log("/var/secrets/sensitive_file.jsonl", ["pkg/auth:TestLogin"])
        self.assertFalse(success_file)
        for d in diags_file:
            self.assertNotIn("sensitive_file.jsonl", d, "Raw file basename should not be printed")

    def test_regression_empty_requires_rejected(self):
        """Empty requires list passed to public verify_test_log must fail."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        success, diags = ci_go_test_guard.verify_test_log(path, [])
        self.assertFalse(success, "Should fail when requires is empty")
        self.assertTrue(any("empty" in d.lower() or "required" in d.lower() for d in diags))

    def test_unicode_subtest_identifiers_allowed(self):
        """Unicode characters in subtest names should be handled correctly and not rejected as malformed."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestUnicode"},
            {"Action": "run", "Package": "pkg/auth", "Test": "TestUnicode/Trường_Hợp_1_Thành_Công"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestUnicode/Trường_Hợp_1_Thành_Công"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestUnicode"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        success, diags = ci_go_test_guard.verify_test_log(path, ["pkg/auth:TestUnicode"])
        self.assertTrue(success, f"Unicode subtest should succeed without errors, got: {diags}")

    def test_cli_execution_and_exit_codes(self):
        """CLI invocation with --input and --require returns 0 on success and non-zero on failure."""
        events_pass = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestCLI"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestCLI"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path_pass = self._write_jsonl(events_pass)

        script_path = Path(__file__).resolve().parent.parent / "ci_go_test_guard.py"

        # Success case
        res_pass = subprocess.run(
            [sys.executable, str(script_path), "--input", path_pass, "--require", "pkg/auth:TestCLI"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res_pass.returncode, 0, f"Expected 0, stderr: {res_pass.stderr}")

        # Failure case: missing test
        res_fail = subprocess.run(
            [sys.executable, str(script_path), "--input", path_pass, "--require", "pkg/auth:TestMissing"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res_fail.returncode, 0, "Expected non-zero exit code for missing test")

        # Failure case: missing input file
        res_nofile = subprocess.run(
            [sys.executable, str(script_path), "--input", "/non/existent/path.jsonl", "--require", "pkg/auth:TestCLI"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res_nofile.returncode, 0, "Expected non-zero exit code for missing file")
        # Ensure raw file path or content does not leak secrets or unhandled trace
        self.assertNotIn("Traceback", res_nofile.stderr)

    def test_default_strict_rejects_no_test_package_skip(self):
        """Strict default behavior: package with [no test files] and skip is rejected without opt-in."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "iot-platform/cmd/gateway"},
            {"Action": "output", "Package": "iot-platform/cmd/gateway", "Output": "?   \tiot-platform/cmd/gateway\t[no test files]\n"},
            {"Action": "skip", "Package": "iot-platform/cmd/gateway"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires)
        self.assertFalse(success, "Strict default must reject skipped package even with [no test files] marker")
        self.assertTrue(any("iot-platform/cmd/gateway" in d for d in diags))

    def test_opted_in_no_test_package_with_valid_evidence_passes(self):
        """Opted-in package with full valid evidence (start, standard marker, skip, 0 tests) passes."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "iot-platform/cmd/gateway"},
            {"Action": "output", "Package": "iot-platform/cmd/gateway", "Output": "?   \tiot-platform/cmd/gateway\t[no test files]\n"},
            {"Action": "skip", "Package": "iot-platform/cmd/gateway"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(
            path, requires, allow_no_test_packages=["iot-platform/cmd/gateway"]
        )
        self.assertTrue(success, f"Expected verification to pass with exact opt-in, got diags: {diags}")

    def test_opted_in_no_test_package_missing_start_rejected(self):
        """Opted-in package missing start event is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            # Missing Action: start
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/empty\t[no test files]\n"},
            {"Action": "skip", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Missing start event must be rejected")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_no_test_package_missing_marker_rejected(self):
        """Opted-in package missing standard marker is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            # Missing Action: output standard [no test files] marker
            {"Action": "skip", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Missing marker output must be rejected")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_no_test_package_wrong_package_marker_rejected(self):
        """Opted-in package with marker for a different package is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            # Output references pkg/other instead of pkg/empty
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/other\t[no test files]\n"},
            {"Action": "skip", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Wrong package marker must be rejected")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_no_test_package_truncated_rejected(self):
        """Opted-in package truncated before terminal skip event is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/empty\t[no test files]\n"},
            # Missing Action: skip
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Truncated log without terminal skip must be rejected")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_no_test_package_fail_after_marker_rejected(self):
        """Opted-in package with failure terminal event is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/empty\t[no test files]\n"},
            {"Action": "fail", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Package failure must never pass")
        self.assertTrue(any("fail" in d.lower() and "pkg/empty" in d for d in diags))

    def test_opted_in_no_test_package_contradictory_pass_after_marker_rejected(self):
        """Opted-in package with contradictory terminal events (both skip and pass) is rejected."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/empty\t[no test files]\n"},
            {"Action": "skip", "Package": "pkg/empty"},
            {"Action": "pass", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Contradictory terminal events must be rejected")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_package_with_required_target_rejected_if_skipped(self):
        """Opted-in package that contains a required test cannot be exempted if it skipped."""
        events = [
            {"Action": "start", "Package": "pkg/empty"},
            {"Action": "output", "Package": "pkg/empty", "Output": "?   \tpkg/empty\t[no test files]\n"},
            {"Action": "skip", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/empty:TestRequired"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Opted-in package cannot bypass required test execution")
        self.assertTrue(any("TestRequired" in d for d in diags))

    def test_opted_in_package_with_real_test_events_not_exempted(self):
        """Opted-in package having real test events cannot be exempted as a no-test package."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            # Real test event present
            {"Action": "run", "Package": "pkg/empty", "Test": "TestReal"},
            {"Action": "skip", "Package": "pkg/empty", "Test": "TestReal"},
            {"Action": "skip", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertFalse(success, "Package with test events must not be exempted as no-test")
        self.assertTrue(any("pkg/empty" in d for d in diags))

    def test_opted_in_package_with_real_tests_passing_succeeds(self):
        """Opted-in package that gains real tests succeeds when tests and package pass."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "pkg/empty"},
            {"Action": "run", "Package": "pkg/empty", "Test": "TestReal"},
            {"Action": "pass", "Package": "pkg/empty", "Test": "TestReal"},
            {"Action": "pass", "Package": "pkg/empty"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=["pkg/empty"])
        self.assertTrue(success, f"Package with passing tests should succeed, diags: {diags}")

    def test_invalid_allow_no_test_package_specifications_rejected(self):
        """Reject wildcards, regex, control characters, empty strings in --allow-no-test-package."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestLogin"},
            {"Action": "pass", "Package": "pkg/auth"},
        ]
        path = self._write_jsonl(events)
        requires = ["pkg/auth:TestLogin"]

        invalid_inputs = [
            "pkg/*",
            "pkg/[a-z]+",
            "",
            "   ",
            "pkg/name\x00extra",
            "../escape",
            "/absolute/pkg",
            "pkg/name?",
            "pkg/(group)",
            "pkg/name^",
        ]
        for inv in invalid_inputs:
            success, diags = ci_go_test_guard.verify_test_log(path, requires, allow_no_test_packages=[inv])
            self.assertFalse(success, f"Invalid spec '{inv}' must be rejected")
            # Diagnostics must be safe fixed messages without leaking raw input or control chars
            for d in diags:
                self.assertNotIn("\x00", d)
                self.assertNotIn("Traceback", d)

    def test_cli_allow_no_test_package_execution(self):
        """CLI invocation with --allow-no-test-package accepts valid no-test package and rejects without it."""
        events = [
            {"Action": "run", "Package": "pkg/auth", "Test": "TestCLI"},
            {"Action": "pass", "Package": "pkg/auth", "Test": "TestCLI"},
            {"Action": "pass", "Package": "pkg/auth"},
            {"Action": "start", "Package": "iot-platform/cmd/gateway"},
            {"Action": "output", "Package": "iot-platform/cmd/gateway", "Output": "?   \tiot-platform/cmd/gateway\t[no test files]\n"},
            {"Action": "skip", "Package": "iot-platform/cmd/gateway"},
        ]
        path = self._write_jsonl(events)
        script_path = Path(__file__).resolve().parent.parent / "ci_go_test_guard.py"

        # Without opt-in -> must fail (non-zero)
        res_fail = subprocess.run(
            [sys.executable, str(script_path), "--input", path, "--require", "pkg/auth:TestCLI"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res_fail.returncode, 0, "Expected non-zero without opt-in")

        # With opt-in -> must succeed (0)
        res_pass = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--input",
                path,
                "--require",
                "pkg/auth:TestCLI",
                "--allow-no-test-package",
                "iot-platform/cmd/gateway",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res_pass.returncode, 0, f"Expected 0 with opt-in, stderr: {res_pass.stderr}")


if __name__ == "__main__":
    unittest.main()
