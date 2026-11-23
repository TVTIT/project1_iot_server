#!/usr/bin/env python3
"""Focused unit tests for the Stage 8 playback rely-on-run guard and safe staging."""

import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT / "scripts" / "tests"))
import stage8_playback_support as support
from stage8_playback_support import (
    Stage8PlaybackGuardError,
    require_stage8_playback_run,
    validate_and_stage_playback_summary,
)


def _make_valid_report():
    return {
        "schema_version": 1,
        "status": "PASS",
        "project": "ncs_1234567890ab",
        "exact_cleanup_verified": True,
        "failure_code": None,
        "error": None,
        "checks": {
            "gateway_upload_result_and_durable_verification": {
                "status": "PASS",
                "sqlite_phase": "ready",
                "declared_bytes": 1024,
                "actual_bytes": 1024,
                "declared_sha256": "a" * 64,
                "verified_sha256": "a" * 64,
            },
            "stage8_local_playback": {
                "status": "PASS",
                "reissue_and_revocation_semantics_verified": True,
                "representative_rejections_verified": {
                    "nonmember_denied_404": True,
                    "wrong_gateway_denied_404": True,
                    "revocation_denied_404": True,
                },
                "range_206_verified": {
                    "http_status": 206,
                    "start": 8,
                    "end": 23,
                    "content_range": "bytes 8-23/1024",
                    "bytes_length": 16,
                },
                "gateway_media_list_verified": {
                    "status": "PASS",
                    "pagination_verified": {
                        "pages_traversed": True,
                        "tie_breaking_verified": True,
                        "no_duplicates": True,
                        "link_header_rel_next": True,
                    },
                    "rejections_verified": {
                        "nonmember_denied_404": True,
                        "platform_admin_no_membership_denied_404": True,
                        "missing_gateway_denied_404": True,
                        "revocation_denied_404": True,
                        "cursor_cross_scope_denied_400": True,
                    },
                },
                "storage_playback_signer_boundaries_verified": {
                    "status": "PASS",
                    "restricted_role_verified": True,
                    "public_tables_denied": True,
                    "write_denied_no_side_effects": True,
                    "delete_denied_no_side_effects": True,
                    "other_bucket_denied_existing_proven": True,
                    "bucket_management_denied_no_side_effects": True,
                },
            },
        },
    }


class TestStage8PlaybackGuard(unittest.TestCase):
    """Verify require_stage8_playback_run distinguishes SKIP/FAIL/NOT RUN and enforces assertions."""

    def test_accepts_valid_completed_report(self):
        report = _make_valid_report()
        self.assertTrue(require_stage8_playback_run(report, runner_exit_code=0))

    def test_rejects_missing_report_as_not_run(self):
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(None, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "NOT_RUN")
        self.assertEqual(ctx.exception.code, "report_missing")

    def test_rejects_nonzero_runner_exit_code_as_fail(self):
        report = _make_valid_report()
        for exit_code in (1, 2, 127):
            with self.subTest(exit_code=exit_code):
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=exit_code)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "runner_exit_nonzero")

    def test_rejects_skipped_exit_code_or_status_as_skip(self):
        report = _make_valid_report()
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=77)
        self.assertEqual(ctx.exception.kind, "SKIP")
        self.assertEqual(ctx.exception.code, "runner_exit_skipped")

        report_skip = _make_valid_report()
        report_skip["status"] = "SKIP"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report_skip, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "SKIP")
        self.assertEqual(ctx.exception.code, "status_skipped")

    def test_rejects_not_run_status_as_not_run(self):
        for status_val in ("NOT RUN", "NOT_RUN"):
            with self.subTest(status=status_val):
                report = _make_valid_report()
                report["status"] = status_val
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "NOT_RUN")
                self.assertEqual(ctx.exception.code, "status_not_run")

    def test_rejects_non_pass_status_as_fail(self):
        for bad_status in ("FAIL", "PARTIAL", "UNKNOWN", "", None, 123):
            with self.subTest(status=bad_status):
                report = _make_valid_report()
                report["status"] = bad_status
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "status_not_pass")

    def test_rejects_missing_or_invalid_schema_version(self):
        for bad_ver in (None, 0, 2, "1", True):
            with self.subTest(schema_version=bad_ver):
                report = _make_valid_report()
                report["schema_version"] = bad_ver
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "schema_version_invalid")

    def test_rejects_missing_or_invalid_project(self):
        for bad_project in (None, "", "ncs_", "ncs_123", "ncs_1234567890ag", "custom_project_name", 123):
            with self.subTest(project=bad_project):
                report = _make_valid_report()
                report["project"] = bad_project
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "project_provenance_invalid")

    def test_rejects_malformed_report_not_dict_as_fail(self):
        for malformed in ([], "string", 123, True):
            with self.subTest(malformed=malformed):
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(malformed, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "report_malformed")

    def test_rejects_cleanup_not_true(self):
        for bad_cleanup in (False, None, 1, "true"):
            with self.subTest(cleanup=bad_cleanup):
                report = _make_valid_report()
                report["exact_cleanup_verified"] = bad_cleanup
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.kind, "FAIL")
                self.assertEqual(ctx.exception.code, "exact_cleanup_not_verified")

        report_missing_cleanup = _make_valid_report()
        del report_missing_cleanup["exact_cleanup_verified"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report_missing_cleanup, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "FAIL")
        self.assertEqual(ctx.exception.code, "exact_cleanup_not_verified")

    def test_rejects_report_with_failure_code_or_error(self):
        report_fail = _make_valid_report()
        report_fail["failure_code"] = "fixture_timeout"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report_fail, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "FAIL")
        self.assertEqual(ctx.exception.code, "report_has_failure")

        report_err = _make_valid_report()
        report_err["error"] = "crash"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report_err, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "FAIL")
        self.assertEqual(ctx.exception.code, "report_has_failure")

    def test_rejects_absent_or_invalid_upload_assertions(self):
        report = _make_valid_report()
        del report["checks"]["gateway_upload_result_and_durable_verification"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.kind, "FAIL")
        self.assertEqual(ctx.exception.code, "upload_verification_missing_or_failed")

        report = _make_valid_report()
        report["checks"]["gateway_upload_result_and_durable_verification"]["sqlite_phase"] = "pending"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "upload_ready_phase_missing")

        for bad_bytes in (0, -1, None, "1024"):
            with self.subTest(bad_bytes=bad_bytes):
                report = _make_valid_report()
                report["checks"]["gateway_upload_result_and_durable_verification"]["actual_bytes"] = bad_bytes
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.code, "upload_size_missing_or_invalid")

        for bad_sha in ("", "short", "z" * 64, 123, None):
            with self.subTest(bad_sha=bad_sha):
                report = _make_valid_report()
                report["checks"]["gateway_upload_result_and_durable_verification"]["verified_sha256"] = bad_sha
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.code, "upload_hash_missing_or_invalid")

    def test_rejects_absent_playback_role_revoke_proof(self):
        report = _make_valid_report()
        del report["checks"]["stage8_local_playback"]["reissue_and_revocation_semantics_verified"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "playback_role_revoke_proof_missing")

    def test_rejects_absent_representative_assertions(self):
        report = _make_valid_report()
        del report["checks"]["stage8_local_playback"]["representative_rejections_verified"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "representative_rejections_missing")

        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["representative_rejections_verified"]["nonmember_denied_404"] = False
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "representative_nonmember_denied_missing")

        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["representative_rejections_verified"]["wrong_gateway_denied_404"] = False
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "representative_wrong_gateway_denied_missing")

    def test_rejects_absent_gateway_media_list_proof(self):
        report = _make_valid_report()
        del report["checks"]["stage8_local_playback"]["gateway_media_list_verified"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "gateway_media_list_proof_missing")

        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["gateway_media_list_verified"]["status"] = "FAIL"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "gateway_media_list_proof_missing")

        report = _make_valid_report()
        del report["checks"]["stage8_local_playback"]["gateway_media_list_verified"]["pagination_verified"]["pages_traversed"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "gateway_media_list_pagination_proof_missing")

        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["gateway_media_list_verified"]["rejections_verified"]["revocation_denied_404"] = False
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "gateway_media_list_revocation_proof_missing")

    def test_missing_storage_playback_signer_boundaries_rejected(self):
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["storage_playback_signer_boundaries_verified"] = {
            "status": "PASS",
            "restricted_role_verified": True,
            "public_tables_denied": True,
            "write_denied_no_side_effects": True,
            "delete_denied_no_side_effects": True,
            "other_bucket_denied_existing_proven": True,
            "bucket_management_denied_no_side_effects": True,
        }
        del report["checks"]["stage8_local_playback"]["storage_playback_signer_boundaries_verified"]
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "storage_playback_signer_boundaries_missing")

    def test_incomplete_storage_playback_signer_boundaries_rejected(self):
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["storage_playback_signer_boundaries_verified"] = {
            "status": "PASS",
            "restricted_role_verified": True,
            "public_tables_denied": True,
            "write_denied_no_side_effects": False,  # incomplete!
            "delete_denied_no_side_effects": True,
            "other_bucket_denied_existing_proven": True,
            "bucket_management_denied_no_side_effects": True,
        }
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "storage_playback_signer_boundaries_incomplete")

    def test_rejects_missing_or_non_206_http_status(self):
        for bad_status in (None, 200, 404, 206.0, "206"):
            with self.subTest(bad_status=bad_status):
                report = _make_valid_report()
                report["checks"]["stage8_local_playback"]["range_206_verified"]["http_status"] = bad_status
                with self.assertRaises(Stage8PlaybackGuardError) as ctx:
                    require_stage8_playback_run(report, runner_exit_code=0)
                self.assertEqual(ctx.exception.code, "range_http_status_not_206")

    def test_rejects_inconsistent_range_content_range(self):
        # 1. Byte length mismatch: range 8-23 is 16 bytes, but bytes_length=15
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["range_206_verified"]["bytes_length"] = 15
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "range_content_range_inconsistent")

        # 2. Total mismatch with upload actual_bytes (1024 vs 2048)
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["range_206_verified"]["content_range"] = "bytes 8-23/2048"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "range_content_range_inconsistent")

        # 3. Start > End (bytes 23-8/1024)
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["range_206_verified"]["start"] = 23
        report["checks"]["stage8_local_playback"]["range_206_verified"]["end"] = 8
        report["checks"]["stage8_local_playback"]["range_206_verified"]["content_range"] = "bytes 23-8/1024"
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "range_content_range_inconsistent")

        # 4. Start offset in report mismatch with content_range header
        report = _make_valid_report()
        report["checks"]["stage8_local_playback"]["range_206_verified"]["start"] = 0
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        self.assertEqual(ctx.exception.code, "range_content_range_inconsistent")

    def test_does_not_expose_arbitrary_report_strings_or_runtime_logs(self):
        adversarial_payload = "SECRET_TOKEN_eyJhbGciOiJIUzI1NiJ9_DROP_TABLE_users_<script>alert(1)</script>"
        report = _make_valid_report()
        report["status"] = adversarial_payload
        report["failure_code"] = adversarial_payload
        report["error"] = adversarial_payload
        with self.assertRaises(Stage8PlaybackGuardError) as ctx:
            require_stage8_playback_run(report, runner_exit_code=0)
        err_msg = str(ctx.exception)
        self.assertNotIn(adversarial_payload, err_msg)
        self.assertTrue(err_msg.startswith("FAIL:"))


class TestValidateAndStagePlaybackSummary(unittest.TestCase):
    """Verify safe summary staging writes compact allowlisted JSON with safe permissions."""

    def test_skip_with_missing_or_empty_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            missing = tmp / "missing-report.json"
            dest = tmp / "summary.json"
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(missing, dest, "a" * 40, runner_exit_code=77)
            self.assertIn("SKIP:runner_exit_skipped", str(ctx.exception))

            empty_file = tmp / "empty.json"
            empty_file.write_text("")
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(empty_file, dest, "a" * 40, runner_exit_code=77)
            self.assertIn("SKIP:runner_exit_skipped", str(ctx.exception))

    def test_nonzero_exit_with_missing_or_empty_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            missing = tmp / "missing-report.json"
            dest = tmp / "summary.json"
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(missing, dest, "a" * 40, runner_exit_code=1)
            self.assertIn("FAIL:runner_exit_nonzero", str(ctx.exception))

            empty_file = tmp / "empty.json"
            empty_file.write_text("")
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(empty_file, dest, "a" * 40, runner_exit_code=2)
            self.assertIn("FAIL:runner_exit_nonzero", str(ctx.exception))

    def test_exit_zero_with_missing_or_empty_file_is_not_run(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            missing = tmp / "missing-report.json"
            dest = tmp / "summary.json"
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(missing, dest, "a" * 40, runner_exit_code=0)
            self.assertIn("NOT_RUN:report_missing", str(ctx.exception))

            empty_file = tmp / "empty.json"
            empty_file.write_text("   \n\t")
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(empty_file, dest, "a" * 40, runner_exit_code=0)
            self.assertIn("NOT_RUN:report_missing", str(ctx.exception))

    def test_stages_valid_summary_with_safe_structure_and_permissions(self):
        valid_report = _make_valid_report()
        checkout_sha = "0123456789abcdef0123456789abcdef01234567"
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            source_file = tmp / "report.json"
            safe_file = tmp / "safe" / "summary.json"
            source_file.write_text(json.dumps(valid_report), encoding="utf-8")

            safe_data = validate_and_stage_playback_summary(
                source_file, safe_file, checkout_sha, runner_exit_code=0
            )

            self.assertTrue(safe_file.is_file())
            # Permission check: 0o600
            file_stat = safe_file.stat()
            self.assertEqual(stat.S_IMODE(file_stat.st_mode), 0o600)

            # Structure check: guaranteed allowlisted fields only
            self.assertEqual(safe_data["status"], "PASS")
            self.assertEqual(safe_data["project"], "ncs_1234567890ab")
            self.assertEqual(safe_data["checkout_sha"], checkout_sha)
            self.assertTrue(safe_data["native_executed"])
            self.assertTrue(safe_data["stage8_playback_accepted"])
            self.assertTrue(safe_data["exact_cleanup_verified"])
            self.assertEqual(safe_data["media_ready"]["actual_bytes"], 1024)
            self.assertEqual(safe_data["media_ready"]["verified_sha256"], "a" * 64)
            self.assertEqual(safe_data["media_ready"]["sqlite_phase"], "ready")
            self.assertEqual(safe_data["playback_proof"]["http_status"], 206)
            self.assertTrue(safe_data["playback_proof"]["range_206"])
            self.assertEqual(safe_data["playback_proof"]["content_range"], "bytes 8-23/1024")
            self.assertEqual(safe_data["playback_proof"]["bytes_length"], 16)
            self.assertTrue(safe_data["playback_proof"]["reissue_and_revocation"])
            self.assertTrue(safe_data["playback_proof"]["representative_rejections"]["nonmember_denied_404"])
            self.assertTrue(safe_data["playback_proof"]["representative_rejections"]["wrong_gateway_denied_404"])

    def test_staging_destination_o_excl_refuses_existing_file_without_unlinking(self):
        valid_report = _make_valid_report()
        checkout_sha = "0123456789abcdef0123456789abcdef01234567"
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            source_file = tmp / "report.json"
            source_file.write_text(json.dumps(valid_report), encoding="utf-8")

            existing_dest = tmp / "summary.json"
            existing_dest.write_text("pre-existing-operator-file")

            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(source_file, existing_dest, checkout_sha, runner_exit_code=0)
            self.assertIn("destination already exists", str(ctx.exception))
            # Verify file was NOT modified or unlinked
            self.assertEqual(existing_dest.read_text(), "pre-existing-operator-file")

    def test_staging_destination_refuses_symlink_without_unlinking(self):
        valid_report = _make_valid_report()
        checkout_sha = "0123456789abcdef0123456789abcdef01234567"
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            source_file = tmp / "report.json"
            source_file.write_text(json.dumps(valid_report), encoding="utf-8")

            target_file = tmp / "target.json"
            target_file.write_text("protected-target")
            symlink_dest = tmp / "summary.json"
            symlink_dest.symlink_to(target_file)

            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(source_file, symlink_dest, checkout_sha, runner_exit_code=0)
            self.assertIn("destination already exists or is symlink", str(ctx.exception))
            self.assertTrue(symlink_dest.is_symlink())
            self.assertEqual(target_file.read_text(), "protected-target")

    def test_staging_source_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            real = tmp / "real.json"
            real.write_text(json.dumps(_make_valid_report()), encoding="utf-8")
            symlink = tmp / "symlink.json"
            symlink.symlink_to(real)
            dest = tmp / "summary.json"
            with self.assertRaises(SystemExit) as ctx:
                validate_and_stage_playback_summary(symlink, dest, "a" * 40, runner_exit_code=0)
            self.assertIn("FAIL:report_symlink_rejected", str(ctx.exception))

    def test_rejects_invalid_checkout_sha(self):
        valid_report = _make_valid_report()
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            source_file = tmp / "report.json"
            dest = tmp / "summary.json"
            source_file.write_text(json.dumps(valid_report), encoding="utf-8")
            for bad_sha in ("short", "g" * 40, "0" * 39, "0" * 41):
                with self.subTest(bad_sha=bad_sha):
                    with self.assertRaises(SystemExit) as ctx:
                        validate_and_stage_playback_summary(source_file, dest, bad_sha, runner_exit_code=0)
                    self.assertIn("checkout SHA rejected", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
