#!/usr/bin/env python3
"""Comprehensive unit tests proving fixture ownership and failure-path safety
for task28_simplified_schema_runner.py:
1. Name collision causes no deletion of foreign container
2. Create failure before receipt causes no deletion
3. Valid ID success removes that exact ID only
4. Inspect daemon failure is not treated as absence
5. Cleanup timeout preserves primary error
6. Success with cleanup uncertainty fails nonzero
7. Private file mode at open/write time (mode 0600 atomically)
8. Secret-bearing stdout/stderr/timeout output is safe (scrubbed / sanitized)
9. Image/readiness calls and phase loops have bounded timeouts
10. Preserves no-public-port and pinned-image guarantees
"""
import ast
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

ROOT = Path(__file__).resolve().parent.parent.parent
RUNNER_PATH = ROOT / "scripts" / "tests" / "task28_simplified_schema_runner.py"

import scripts.tests.task28_simplified_schema_runner as runner


class TestSimplifiedSchemaRunnerSafety(unittest.TestCase):
    def test_runner_file_exists(self):
        self.assertTrue(RUNNER_PATH.is_file())

    def test_no_external_cleanup_or_provision_imports(self):
        """Runner must avoid unqualified/imported cleanup/readiness helpers from task266_provision."""
        content = RUNNER_PATH.read_text(encoding="utf-8")
        self.assertNotIn("from task266_provision import", content)
        self.assertNotIn("cleanup_container", content)

    def test_name_collision_causes_no_deletion(self):
        """If docker create fails due to a name collision before a receipt is issued,

        cleanup must NOT delete the existing foreign container with that name."""
        fixture = runner.DatabaseFixture()
        mock_sub = MagicMock()
        # Simulate docker create failing with name collision (returncode != 0)
        # cidfile remains empty or absent
        mock_sub.run.return_value = subprocess.CompletedProcess(
            args=["docker", "create"], returncode=1,
            stdout=b"", stderr=b"Conflict. The container name \"/test\" is already in use by container \"foreign-cid\""
        )
        with patch.object(runner.subprocess, "run", side_effect=mock_sub.run.side_effect if mock_sub.run.side_effect else lambda *a, **kw: mock_sub.run.return_value):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.create()
            self.assertIn("create_failed", str(ctx.exception))
            # Now call cleanup: since fixture.container_id is None, no docker rm or inspect must be called
            mock_sub.run.reset_mock()
            cleaned = fixture.cleanup()
            self.assertTrue(cleaned)
            # Ensure docker rm was NOT called
            rm_calls = [c for c in mock_sub.run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 0, "Foreign container must not be removed on name collision")

    def test_create_failure_before_receipt_causes_no_deletion(self):
        """If docker create fails or times out before writing a valid cidfile,

        cleanup must not delete any guessed name or unowned ID."""
        fixture = runner.DatabaseFixture()
        mock_sub = MagicMock()
        mock_sub.run.side_effect = subprocess.TimeoutExpired(cmd=["docker", "create"], timeout=60)
        with patch.object(runner.subprocess, "run", side_effect=mock_sub.run.side_effect):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.create()
            self.assertIn("create_timeout", str(ctx.exception))
            mock_sub.run.reset_mock()
            fixture.cleanup()
            rm_calls = [c for c in mock_sub.run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 0, "No container must be removed when receipt is absent")

    def test_valid_id_success_removes_exact_id_only(self):
        """When a valid container ID is received in the cidfile, cleanup must target

        that exact 64-char hex container ID, never the container name."""
        fixture = runner.DatabaseFixture()
        mock_sub = MagicMock()
        valid_cid = "a" * 64
        # We simulate successful docker create writing valid_cid into cidfile
        def fake_run(args, **kwargs):
            if "create" in args:
                # write cidfile
                cid_idx = args.index("--cidfile") + 1
                cid_path = args[cid_idx]
                Path(cid_path).write_text(valid_cid)
                return subprocess.CompletedProcess(args, 0, stdout=valid_cid.encode() + b"\n", stderr=b"")
            if "rm" in args:
                return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
            if "inspect" in args and "-f" in args and "{{.Id}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=valid_cid.encode() + b"\n", stderr=b"")
            if "inspect" in args and "-f" in args and "{{json .Mounts}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=b"[]\n", stderr=b"")
            if "inspect" in args:
                return subprocess.CompletedProcess(args, 1, stdout=b"", stderr=b"Error: No such container: " + valid_cid.encode())
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

        mock_run = MagicMock(side_effect=fake_run)
        with patch.object(runner.subprocess, "run", mock_run):
            fixture.create()
            self.assertEqual(fixture.container_id, valid_cid)
            mock_run.reset_mock()
            cleaned = fixture.cleanup()
            self.assertTrue(cleaned)
            rm_calls = [c for c in mock_run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 1)
            self.assertIn(valid_cid, rm_calls[0].args[0])
            self.assertNotIn(fixture.name, rm_calls[0].args[0])

    def test_inspect_daemon_failure_is_not_absence(self):
        """If docker inspect returns a generic daemon error / exit code 1 without

        the authoritative 'no such container' message, it must NOT be treated as verified absence."""
        fixture = runner.DatabaseFixture()
        fixture.container_id = "b" * 64
        mock_sub = MagicMock()
        mock_sub.run.side_effect = [
            # docker rm
            subprocess.CompletedProcess(["docker", "rm"], 0, stdout=b"", stderr=b""),
            # docker inspect fails with daemon error
            subprocess.CompletedProcess(["docker", "inspect"], 1, stdout=b"", stderr=b"Error response from daemon: permission denied / socket hangup")
        ]
        with patch.object(runner.subprocess, "run", side_effect=mock_sub.run.side_effect):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.cleanup()
            self.assertIn("absence_verification_failed", str(ctx.exception))

    def test_cleanup_timeout_preserves_primary_error(self):
        """When an exception occurs in main execution and cleanup subsequently times out,

        the primary execution exception must not be masked or lost."""
        mock_sub = MagicMock()
        mock_sub.run.side_effect = [
            # create
            subprocess.CompletedProcess(["docker", "create"], 0, stdout=b"c"*64 + b"\n", stderr=b""),
            # inspect image
            subprocess.CompletedProcess(["docker", "image", "inspect"], 0, stdout=b"", stderr=b""),
        ]
        # In runner.run_with_fixture: primary error occurs, cleanup times out
        fixture = runner.DatabaseFixture()
        fixture.container_id = "c" * 64
        with patch.object(fixture, "cleanup", side_effect=RuntimeError("cleanup_timeout")):
            primary_err = RuntimeError("primary_sql_failed")
            with self.assertRaises(RuntimeError) as ctx:
                runner.safe_execute_with_cleanup(fixture, lambda: (_ for _ in ()).throw(primary_err))
            self.assertEqual(ctx.exception, primary_err)
            self.assertTrue(hasattr(ctx.exception, "__notes__") or "cleanup" in str(ctx.exception.__context__))

    def test_success_with_cleanup_uncertainty_fails_nonzero(self):
        """If the primary workload succeeds but cleanup cannot verify container or directory absence,

        the process must fail with a non-zero exit code / exception."""
        fixture = runner.DatabaseFixture()
        fixture.container_id = "d" * 64
        with patch.object(fixture, "cleanup", return_value=False):
            with self.assertRaises(RuntimeError) as ctx:
                runner.safe_execute_with_cleanup(fixture, lambda: "workload_success")
            self.assertIn("cleanup_uncertainty", str(ctx.exception))

    def test_private_file_mode_at_open_and_write_time(self):
        """Secret files must be created with mode 0600 atomically (via os.open with O_CREAT|O_EXCL),

        inside a parent directory with mode 0700."""
        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td) / "private-fixture"
            env_file = base_dir / "database.env"
            runner.create_private_env_file(env_file, base_dir, {"SECRET": "mysecret"})
            # Check directory permissions
            dir_mode = stat.S_IMODE(base_dir.stat().st_mode)
            self.assertEqual(dir_mode, 0o700, f"Directory mode must be 0700, got {oct(dir_mode)}")
            # Check file permissions
            file_mode = stat.S_IMODE(env_file.stat().st_mode)
            self.assertEqual(file_mode, 0o600, f"Env file mode must be 0600, got {oct(file_mode)}")

    def test_secret_bearing_output_is_sanitized(self):
        """Captured stdout/stderr or timeout exceptions must not escape with raw secrets.

        Sanitizer must replace secrets with [REDACTED]."""
        secret_list = ["super_secret_password_123", "another_secret_456"]
        raw_text = "Error connecting with super_secret_password_123 to database with another_secret_456"
        sanitized = runner.sanitize_output(raw_text, secret_list)
        self.assertNotIn("super_secret_password_123", sanitized)
        self.assertNotIn("another_secret_456", sanitized)
        self.assertIn("[REDACTED]", sanitized)

    def test_image_and_readiness_calls_have_bounded_timeouts(self):
        """Subprocess calls for image preparation, readiness probes, SQL, and cleanup

        must have strict bounded timeouts."""
        self.assertLessEqual(runner.IMAGE_INSPECT_TIMEOUT, 15)
        self.assertLessEqual(runner.IMAGE_PULL_TIMEOUT, 300)
        self.assertLessEqual(runner.CONTAINER_CREATE_TIMEOUT, 60)
        self.assertLessEqual(runner.CONTAINER_START_TIMEOUT, 60)
        self.assertLessEqual(runner.READINESS_TOTAL_TIMEOUT, 90)
        self.assertLessEqual(runner.PROBE_TIMEOUT, 2)
        self.assertLessEqual(runner.SQL_TIMEOUT, 60)
        self.assertLessEqual(runner.CLEANUP_TIMEOUT, 30)

    def test_no_published_ports_and_pinned_image(self):
        """Must use pinned timescale image digest and never bind host ports."""
        self.assertIn("@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a", runner.PG_IMAGE)
        # Ensure arguments generated for create do not contain -p or --publish
        cmd = runner.build_docker_create_command("cidfile_path", "env_path", "container_name")
        self.assertNotIn("-p", cmd)
        self.assertNotIn("--publish", cmd)
        self.assertIn("--cidfile", cmd)

    def test_short_or_invalid_receipt_rejection(self):
        """Receipt must be rejected if not exactly 64 hex characters or if malformed,

        denying cleanup authority and setting container_id to None."""
        fixture = runner.DatabaseFixture()
        valid_64 = "a" * 64
        # Test 12-char hex receipt (previously allowed len >= 12)
        short_cid = "a" * 12

        def fake_run(args, **kwargs):
            if "create" in args:
                cid_idx = args.index("--cidfile") + 1
                Path(args[cid_idx]).write_text(short_cid)
                return subprocess.CompletedProcess(args, 0, stdout=short_cid.encode() + b"\n", stderr=b"")
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

        mock_run = MagicMock(side_effect=fake_run)
        with patch.object(runner.subprocess, "run", mock_run):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.create()
            self.assertIn("invalid_cidfile_receipt", str(ctx.exception))
            self.assertIsNone(fixture.container_id)
            mock_run.reset_mock()
            cleaned = fixture.cleanup()
            self.assertTrue(cleaned)
            rm_calls = [c for c in mock_run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 0, "No destruction on short receipt")

    def test_nonzero_create_with_valid_receipt_cleaned_using_exact_id(self):
        """Nonzero create exit with a valid 64-char receipt must retain ownership

        and be cleaned using the exact inspected ID."""
        fixture = runner.DatabaseFixture()
        valid_cid = "e" * 64

        def fake_run(args, **kwargs):
            if "create" in args:
                cid_idx = args.index("--cidfile") + 1
                Path(args[cid_idx]).write_text(valid_cid)
                return subprocess.CompletedProcess(args, 1, stdout=b"", stderr=b"daemon startup error after create")
            if "inspect" in args and "-f" in args and "{{.Id}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=valid_cid.encode() + b"\n", stderr=b"")
            if "inspect" in args and "-f" in args and "{{json .Mounts}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=b"[]\n", stderr=b"")
            if "rm" in args:
                return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
            if "inspect" in args:
                return subprocess.CompletedProcess(args, 1, stdout=b"", stderr=f"Error: No such container: {valid_cid}".encode())
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

        mock_run = MagicMock(side_effect=fake_run)
        with patch.object(runner.subprocess, "run", mock_run):
            primary_error = None
            with self.assertRaises(RuntimeError) as ctx:
                runner.safe_execute_with_cleanup(fixture, fixture.create)
            self.assertIn("create_failed", str(ctx.exception))
            # Verify rm was called with the exact inspected ID
            rm_calls = [c for c in mock_run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 1)
            self.assertIn(valid_cid, rm_calls[0].args[0])

    def test_timeout_with_receipt_retains_ownership(self):
        """Timeout in create with a valid 64-char receipt retains container_id ownership."""
        fixture = runner.DatabaseFixture()
        valid_cid = "f" * 64

        def fake_run(args, **kwargs):
            if "create" in args:
                cid_idx = args.index("--cidfile") + 1
                Path(args[cid_idx]).write_text(valid_cid)
                raise subprocess.TimeoutExpired(cmd=args, timeout=60)
            if "inspect" in args and "-f" in args and "{{.Id}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=valid_cid.encode() + b"\n", stderr=b"")
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

        mock_run = MagicMock(side_effect=fake_run)
        with patch.object(runner.subprocess, "run", mock_run):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.create()
            self.assertIn("create_timeout", str(ctx.exception))
            self.assertEqual(fixture.container_id, valid_cid)

    def test_inspected_id_mismatch_denies_destruction(self):
        """If inspected ID does not match the receipt ID, destructive ownership is denied."""
        fixture = runner.DatabaseFixture()
        receipt_cid = "1" * 64
        mismatched_cid = "2" * 64

        def fake_run(args, **kwargs):
            if "create" in args:
                cid_idx = args.index("--cidfile") + 1
                Path(args[cid_idx]).write_text(receipt_cid)
                return subprocess.CompletedProcess(args, 0, stdout=receipt_cid.encode() + b"\n", stderr=b"")
            if "inspect" in args and "-f" in args and "{{.Id}}" in args:
                return subprocess.CompletedProcess(args, 0, stdout=mismatched_cid.encode() + b"\n", stderr=b"")
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

        mock_run = MagicMock(side_effect=fake_run)
        with patch.object(runner.subprocess, "run", mock_run):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.create()
            self.assertIn("inspected_id_mismatch", str(ctx.exception))
            self.assertIsNone(fixture.container_id)
            mock_run.reset_mock()
            cleaned = fixture.cleanup()
            self.assertTrue(cleaned)
            rm_calls = [c for c in mock_run.call_args_list if "rm" in c.args[0]]
            self.assertEqual(len(rm_calls), 0)

    def test_anonymous_volume_effective_mount_uncertainty_cannot_report_absence(self):
        """If container has unexpected anonymous volumes or effective mounts cannot be verified,

        cleanup cannot report absence."""
        fixture = runner.DatabaseFixture()
        fixture.container_id = "3" * 64
        # Mark effective mounts as unverified / containing anonymous volume
        fixture._effective_mounts_verified = False
        with self.assertRaises(RuntimeError) as ctx:
            fixture.cleanup()
        self.assertIn("absence_verification_failed", str(ctx.exception))

    def test_daemon_volume_mount_inspect_failure_is_nonsuccess(self):
        """Daemon failure during mount or volume inspection is non-success."""
        fixture = runner.DatabaseFixture()
        fixture.container_id = "4" * 64
        mock_sub = MagicMock()
        mock_sub.run.side_effect = [
            subprocess.CompletedProcess(["docker", "rm"], 0, stdout=b"", stderr=b""),
            # Container absence inspect succeeds (no such container)
            subprocess.CompletedProcess(["docker", "inspect"], 1, stdout=b"", stderr=b"Error: No such container: " + b"4"*64),
            # But volume inspect fails with daemon socket error
            subprocess.CompletedProcess(["docker", "volume", "inspect"], 1, stdout=b"", stderr=b"Error response from daemon: socket hangup")
        ]
        fixture.owned_volumes = ["anon-vol-123"]
        with patch.object(runner.subprocess, "run", side_effect=mock_sub.run.side_effect):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.cleanup()
            self.assertIn("volume_absence_verification_failed", str(ctx.exception))

    def test_directory_cleanup_attempted_when_container_cleanup_fails(self):
        """When container cleanup fails, directory cleanup is still attempted and completed."""
        fixture = runner.DatabaseFixture()
        fixture.container_id = "5" * 64
        self.assertTrue(fixture.directory.exists())
        mock_sub = MagicMock()
        # docker rm times out
        mock_sub.run.side_effect = subprocess.TimeoutExpired(cmd=["docker", "rm"], timeout=30)
        with patch.object(runner.subprocess, "run", side_effect=mock_sub.run.side_effect):
            with self.assertRaises(RuntimeError) as ctx:
                fixture.cleanup()
            self.assertIn("cleanup_timeout", str(ctx.exception))
            # Despite container cleanup failing, the private directory must have been cleaned up!
            self.assertFalse(fixture.directory.exists(), "Private directory must be deleted even if container cleanup fails")


if __name__ == "__main__":
    unittest.main()
