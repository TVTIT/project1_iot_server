"""CI smoke and bootstrap job contract regression tests.

Verifies workflow configuration, CLI invocation contracts, pinned dependencies,
safe report validation, prerequisite tools, and execution boundaries for:
- dynsec-bootstrap-tests
- native-compose-smoke
"""

import json
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tests.native_compose_smoke import (
    PENDING,
    SERVICES,
    jwt_http_cases,
    role_http_cases,
)

WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"


class TestCISmokeJobs(unittest.TestCase):
    """Regression suite for CI smoke and bootstrap workflow wiring."""

    @classmethod
    def setUpClass(cls):
        if not WORKFLOW_PATH.exists():
            raise unittest.SkipTest(f"Workflow file not found at {WORKFLOW_PATH}")
        cls.raw_content = WORKFLOW_PATH.read_text(encoding="utf-8")

    def _extract_job_section(self, job_name: str) -> str:
        lines = self.raw_content.splitlines()
        job_lines = []
        in_job = False
        job_indent = None
        for line in lines:
            if not in_job:
                if line.strip().startswith(f"{job_name}:"):
                    in_job = True
                    job_indent = len(line) - len(line.lstrip())
                    job_lines.append(line)
            else:
                current_indent = len(line) - len(line.lstrip())
                if line.strip() and current_indent <= job_indent and not line.strip().startswith("#"):
                    break
                job_lines.append(line)
        return "\n".join(job_lines)

    def test_bootstrap_job_defined_with_bounded_timeout_and_root_cwd(self):
        job = self._extract_job_section("dynsec-bootstrap-tests")
        self.assertTrue(job, "Job dynsec-bootstrap-tests must exist in ci.yml")
        self.assertIn("timeout-minutes: 15", job)
        self.assertIn("working-directory: .", job)

    def test_bootstrap_job_runs_both_regression_suites_before_runner(self):
        job = self._extract_job_section("dynsec-bootstrap-tests")
        self.assertIn("test_task284_bootstrap.py", job)
        self.assertIn("test_dynsec_bootstrap_cli.py", job)
        # Regressions must run before executing the runner
        runner_cmd = "dynsec_bootstrap_cli.py --report"
        self.assertIn(runner_cmd, job)
        self.assertLess(job.index("test_task284_bootstrap.py"), job.index(runner_cmd))
        self.assertLess(job.index("test_dynsec_bootstrap_cli.py"), job.index(runner_cmd))

    def test_bootstrap_job_pinned_dependency_acquisition_and_isolation(self):
        job = self._extract_job_section("dynsec-bootstrap-tests")
        pinned_image = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        self.assertIn(f"docker pull --platform linux/amd64 {pinned_image}", job)
        self.assertLess(job.index("docker pull"), job.index("dynsec_bootstrap_cli.py --report"))

    def test_bootstrap_job_report_contract_and_artifact_upload(self):
        job = self._extract_job_section("dynsec-bootstrap-tests")
        report_env = "DYNSEC_BOOTSTRAP_REPORT: ${{ runner.temp }}/dynsec-bootstrap-${{ github.run_id }}-${{ github.run_attempt }}.json"
        self.assertNotIn(report_env, job)
        self.assertIn("printf 'DYNSEC_BOOTSTRAP_REPORT=%s/dynsec-bootstrap-%s-%s.json\\n'", job)
        self.assertIn('"$RUNNER_TEMP" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT" >> "$GITHUB_ENV"', job)
        self.assertIn('--report "$DYNSEC_BOOTSTRAP_REPORT"', job)
        self.assertIn("path: ${{ runner.temp }}/dynsec-bootstrap-${{ github.run_id }}-${{ github.run_attempt }}.json", job)
        self.assertIn("if-no-files-found: error", job)
        self.assertNotIn("if: always()", job)
        self.assertNotIn("continue-on-error", job)
        self.assertNotIn("--dry-run", job)
        self.assertNotIn("*.json", job)

    def test_report_paths_are_initialized_in_steps_not_job_env(self):
        for job_name, variable, prefix in (
            ("dynsec-native-integration", "DYNSEC_NATIVE_REPORT", "dynsec-native"),
            ("dynsec-bootstrap-tests", "DYNSEC_BOOTSTRAP_REPORT", "dynsec-bootstrap"),
        ):
            with self.subTest(job=job_name):
                job = self._extract_job_section(job_name)
                before_steps = job.split("    steps:", 1)[0]
                self.assertNotIn("runner.temp", before_steps)
                match = re.search(
                    r"      - name: Prepare (?:native|bootstrap) report path\n"
                    r"        run: \|\n((?:          [^\n]*\n)+)", job + "\n"
                )
                self.assertIsNotNone(match, "Report initialization step must exist")
                script = textwrap.dedent(match.group(1))
                self.assertLess(job.index(match.group(0)), job.index('--report "$' + variable + '"'))
                with tempfile.TemporaryDirectory(prefix="ci-report-path-") as temp:
                    env_file = Path(temp) / "github-env"
                    result = subprocess.run(
                        ["/bin/sh", "-eu", "-c", script],
                        env={"RUNNER_TEMP": temp, "GITHUB_RUN_ID": "123",
                             "GITHUB_RUN_ATTEMPT": "2", "GITHUB_ENV": str(env_file)},
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, "")
                    self.assertEqual(env_file.read_text(),
                                     f"{variable}={temp}/{prefix}-123-2.json\n")

    def test_compose_smoke_job_defined_with_bounded_timeout_and_root_cwd(self):
        job = self._extract_job_section("native-compose-smoke")
        self.assertTrue(job, "Job native-compose-smoke must exist in ci.yml")
        self.assertIn("timeout-minutes: 30", job)
        self.assertIn("working-directory: .", job)

    def test_compose_smoke_job_dependencies_and_no_circular_deps(self):
        job = self._extract_job_section("native-compose-smoke")
        # Must depend on smaller gates but not circular
        self.assertIn("needs:", job)
        self.assertIn("lint-and-test", job)
        self.assertIn("migration-check", job)
        self.assertIn("dynsec-native-integration", job)
        self.assertIn("dynsec-bootstrap-tests", job)

    def test_compose_smoke_job_toolchain_prerequisites(self):
        job = self._extract_job_section("native-compose-smoke")
        # Must check Go, Python, Docker, Compose >= 2.24.4, openssl, setfacl/getfacl
        self.assertIn("go-version: '1.27.3'", job)
        self.assertIn("setfacl", job)
        self.assertIn("getfacl", job)
        self.assertIn("openssl", job)
        self.assertIn("docker compose version", job)

    def test_compose_smoke_job_runs_smoke_unit_suite_before_execution(self):
        job = self._extract_job_section("native-compose-smoke")
        self.assertIn("test_native_compose_smoke.py", job)
        smoke_cmd = "native_compose_smoke.py"
        self.assertIn(smoke_cmd, job)
        self.assertLess(job.index("test_native_compose_smoke.py"), job.index(smoke_cmd))

    def test_compose_smoke_job_pinned_dependencies_acquired_in_ci_step(self):
        job = self._extract_job_section("native-compose-smoke")
        for img in (
            "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a",
            "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9",
            "nginx:1.25-alpine",
            "supabase/gotrue:v2.196.0",
            "envoyproxy/envoy:v1.39.1",
        ):
            self.assertIn(f"docker pull --platform linux/amd64 {img}", job)
        runner_invocation = 'native_compose_smoke.py --prepare "$SMOKE_PREPARE_DIR"'
        self.assertLess(job.index("docker pull"), job.index(runner_invocation))

    def test_compose_smoke_job_full_http_and_isolated_dir_per_run(self):
        job = self._extract_job_section("native-compose-smoke")
        self.assertIn("--full-http", job)
        self.assertIn("SMOKE_PREPARE_DIR: /tmp/opencode/native-compose-smoke-${{ github.run_id }}-${{ github.run_attempt }}", job)
        self.assertIn('native_compose_smoke.py --prepare "$SMOKE_PREPARE_DIR"', job)

    def test_compose_smoke_job_report_validation_and_artifact_upload(self):
        job = self._extract_job_section("native-compose-smoke")
        self.assertIn("startup-summary.json", job)
        # Validation of safe report before upload
        self.assertIn("validate_summary", job)
        self.assertIn("path: /tmp/opencode/native-compose-smoke-${{ github.run_id }}-${{ github.run_attempt }}/artifacts/startup-summary.json", job)
        self.assertIn("if-no-files-found: error", job)
        self.assertNotIn("continue-on-error", job)
        self.assertNotIn("prune", job)
        self.assertNotIn(".env", job)

        # Execution and success steps must not have if: always()
        smoke_run_idx = job.find("Run Compose smoke")
        validate_report_idx = job.find("Validate smoke report")
        upload_report_idx = job.find("name: Upload safe report")
        self.assertNotEqual(smoke_run_idx, -1)
        self.assertNotEqual(validate_report_idx, -1)
        self.assertNotEqual(upload_report_idx, -1)

        # Verify success acceptance validator requirements
        self.assertIn('report.get("status") != "RBAC_JWT_HTTP_ACCEPTED"', job)
        self.assertIn('report.get("http_accepted") is not True', job)
        self.assertIn('report.get("exact_cleanup_verified") is not True', job)

        # Stable step ID for smoke execution
        self.assertIn("id: smoke", job)
        self.assertIn('python3 scripts/tests/native_compose_smoke.py --prepare "$SMOKE_PREPARE_DIR" --full-http', job)

        # Steps up to and including Upload safe report must not contain if: always()
        success_section = job[:upload_report_idx + 150]
        self.assertNotIn("if: always()", success_section)

        # Failure diagnostic validation step contract
        self.assertIn("Validate smoke failure report", job)
        self.assertIn("id: failure_diagnostic", job)
        self.assertIn("if: always() && steps.smoke.outcome == 'failure'", job)
        self.assertIn("validate_failure_report", job)
        self.assertIn('"$SMOKE_PREPARE_DIR/artifacts/smoke-failure.json"', job)
        self.assertIn("${{ runner.temp }}/smoke-failure-staging/smoke-failure.json", job)

        # Failure report upload step contract
        self.assertIn("Upload safe failure report", job)
        self.assertIn(
            "if: always() && steps.smoke.outcome == 'failure' && steps.failure_diagnostic.outcome == 'success' && steps.failure_diagnostic.outputs.validated == 'true'",
            job,
        )
        self.assertIn("name: native-compose-smoke-safe-failure-report", job)

    def _extract_smoke_failure_validator_script(self) -> str:
        job = self._extract_job_section("native-compose-smoke")
        step_match = re.search(
            r"- name: Validate smoke failure report\s+id: \S+\s+if: [^\n]+\s+run: \|\s+python3 -c '(.*?)'",
            job,
            re.DOTALL,
        )
        self.assertIsNotNone(step_match, "Validate smoke failure report inline script not found")
        return textwrap.dedent(step_match.group(1)).strip()

    def _run_smoke_failure_validator(
        self,
        report_path: Path,
        staging_path: Path,
        env_vars: dict = None,
    ) -> subprocess.CompletedProcess:
        script = self._extract_smoke_failure_validator_script()
        import os
        env = dict(os.environ)
        if env_vars:
            env.update(env_vars)
        return subprocess.run(
            [sys.executable, "-c", script, str(report_path), str(staging_path)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
            env=env,
        )

    def _valid_smoke_failure_report(self) -> dict:
        return {
            "schema_version": 1,
            "status": "STARTUP_FAILED",
            "phase": "preflight",
            "error_type": "ValueError",
            "error_code": "local_image_unavailable",
            "http_accepted": False,
            "cleanup_verified": False,
            "primary_phase": None,
            "primary_error_code": None,
            "cleanup_step": None,
            "cleanup_error_code": None,
            "resource_kind": None,
        }

    def _valid_cleanup_smoke_failure_report(
        self,
        primary_phase: str | None = "preflight",
        primary_error_code: str | None = "compose_phase_failed",
        cleanup_step: str = "remove_resource",
        cleanup_error_code: str = "unclassified",
        resource_kind: str | None = "container",
    ) -> dict:
        return {
            "schema_version": 1,
            "status": "STARTUP_FAILED",
            "phase": "cleanup",
            "error_type": "ValueError",
            "error_code": "startup_cleanup_unresolved_evidence_retained",
            "http_accepted": False,
            "cleanup_verified": False,
            "primary_phase": primary_phase,
            "primary_error_code": primary_error_code,
            "cleanup_step": cleanup_step,
            "cleanup_error_code": cleanup_error_code,
            "resource_kind": resource_kind,
        }

    def _extract_smoke_summary_validator_script(self) -> str:
        job = self._extract_job_section("native-compose-smoke")
        step_match = re.search(
            r"- name: Validate smoke report\s+run: \|\s+python3 -c '(.*?)' \"\$SMOKE_PREPARE_DIR/artifacts/startup-summary.json\"",
            job,
            re.DOTALL,
        )
        self.assertIsNotNone(step_match, "Validate smoke report inline script not found")
        return textwrap.dedent(step_match.group(1)).strip()

    def _run_smoke_summary_validator(self, report_path: Path) -> subprocess.CompletedProcess:
        script = self._extract_smoke_summary_validator_script()
        return subprocess.run(
            [sys.executable, "-c", script, str(report_path)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )

    def _valid_smoke_summary(self) -> dict:
        return {
            "schema_version": 1,
            "status": "RBAC_JWT_HTTP_ACCEPTED",
            "project": "ncs_0123456789ab",
            "services": list(SERVICES),
            "pending_assertions": list(PENDING),
            "native_executed": True,
            "http_accepted": True,
            "exact_cleanup_verified": True,
            "role_cases": {c["label"]: c["status"] for c in role_http_cases()},
            "jwt_cases": {c["label"]: c["status"] for c in jwt_http_cases()},
            "role_snapshot_comparisons": 10,
            "jwt_snapshot_comparisons": 6,
            "cleanup_counts": {"container": 10, "network": 1, "volume": 3},
            "evidence": {},
        }

    def test_smoke_summary_validator_accepts_valid_summary_and_requires_exact_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            report_file = tmp_path / "artifacts" / "startup-summary.json"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            report_file.write_text(json.dumps(self._valid_smoke_summary()), encoding="utf-8")

            res = self._run_smoke_summary_validator(report_file)
            self.assertEqual(res.returncode, 0, f"Summary validator failed on valid summary: {res.stderr}")
            self.assertIn("Native compose smoke summary validated successfully", res.stdout)

    def test_smoke_summary_validator_rejects_missing_symlink_or_unaccepted(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            missing_file = tmp_path / "artifacts" / "startup-summary.json"
            res = self._run_smoke_summary_validator(missing_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("report file missing or symlink", res.stderr)

            valid_file = tmp_path / "valid-summary.json"
            valid_file.write_text(json.dumps(self._valid_smoke_summary()), encoding="utf-8")
            symlink_file = tmp_path / "symlink-summary.json"
            symlink_file.symlink_to(valid_file)
            res_sym = self._run_smoke_summary_validator(symlink_file)
            self.assertNotEqual(res_sym.returncode, 0)
            self.assertIn("report file missing or symlink", res_sym.stderr)

            base = self._valid_smoke_summary()
            for key, bad_val in (
                ("status", "PREPARED_NOT_EXECUTED"),
                ("http_accepted", False),
                ("exact_cleanup_verified", False),
            ):
                payload = dict(base)
                payload[key] = bad_val
                bad_file = tmp_path / f"bad_{key}.json"
                bad_file.write_text(json.dumps(payload), encoding="utf-8")
                res_bad = self._run_smoke_summary_validator(bad_file)
                self.assertNotEqual(res_bad.returncode, 0, f"Accepted bad {key}={bad_val}")

    def test_smoke_failure_validator_accepts_valid_report_and_stages_exact_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            report_file = tmp_path / "artifacts" / "smoke-failure.json"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            report_file.write_text(json.dumps(self._valid_smoke_failure_report()), encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"
            github_output = tmp_path / "github_output"

            res = self._run_smoke_failure_validator(
                report_file, staging_file, env_vars={"GITHUB_OUTPUT": str(github_output)}
            )
            self.assertEqual(res.returncode, 0, f"Validator failed on valid report: {res.stderr}")
            self.assertIn("Smoke failure diagnostic validated successfully", res.stdout)
            self.assertTrue(staging_file.is_file(), "Staged file must exist")
            staged_data = json.loads(staging_file.read_text(encoding="utf-8"))
            self.assertEqual(staged_data, self._valid_smoke_failure_report())
            self.assertEqual(len(staged_data), 12, "Staged failure report must contain exactly 12 fields")
            self.assertIsNone(staged_data["primary_phase"])
            self.assertIsNone(staged_data["primary_error_code"])
            self.assertIsNone(staged_data["cleanup_step"])
            self.assertIsNone(staged_data["cleanup_error_code"])
            self.assertIsNone(staged_data["resource_kind"])
            raw_text = staging_file.read_text(encoding="utf-8")
            self.assertIn('"primary_phase": null', raw_text)
            self.assertNotIn('"None"', raw_text)
            self.assertTrue(github_output.is_file())
            self.assertIn("validated=true", github_output.read_text(encoding="utf-8"))

    def test_smoke_failure_validator_retains_full_cleanup_context(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            report_file = tmp_path / "artifacts" / "smoke-failure.json"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            cleanup_report = self._valid_cleanup_smoke_failure_report(
                primary_phase="preflight",
                primary_error_code="compose_phase_failed",
                cleanup_step="remove_resource",
                cleanup_error_code="unclassified",
                resource_kind="container",
            )
            report_file.write_text(json.dumps(cleanup_report), encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"
            github_output = tmp_path / "github_output"

            res = self._run_smoke_failure_validator(
                report_file, staging_file, env_vars={"GITHUB_OUTPUT": str(github_output)}
            )
            self.assertEqual(res.returncode, 0, f"Validator failed on cleanup report: {res.stderr}")
            self.assertIn("Smoke failure diagnostic validated successfully", res.stdout)
            self.assertTrue(staging_file.is_file())
            staged_data = json.loads(staging_file.read_text(encoding="utf-8"))
            self.assertEqual(staged_data, cleanup_report)
            self.assertEqual(len(staged_data), 12)
            self.assertEqual(staged_data["primary_phase"], "preflight")
            self.assertEqual(staged_data["primary_error_code"], "compose_phase_failed")
            self.assertEqual(staged_data["cleanup_step"], "remove_resource")
            self.assertEqual(staged_data["cleanup_error_code"], "unclassified")
            self.assertEqual(staged_data["resource_kind"], "container")

    def test_smoke_failure_validator_retains_primary_absent_cleanup_report(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            report_file = tmp_path / "artifacts" / "smoke-failure.json"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            cleanup_report = self._valid_cleanup_smoke_failure_report(
                primary_phase=None,
                primary_error_code=None,
                cleanup_step="check_deadline",
                cleanup_error_code="startup_cleanup_deadline",
                resource_kind=None,
            )
            report_file.write_text(json.dumps(cleanup_report), encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(report_file, staging_file)
            self.assertEqual(res.returncode, 0, f"Validator failed on primary absent cleanup report: {res.stderr}")
            staged_data = json.loads(staging_file.read_text(encoding="utf-8"))
            self.assertEqual(staged_data, cleanup_report)
            self.assertEqual(len(staged_data), 12)
            self.assertIsNone(staged_data["primary_phase"])
            self.assertIsNone(staged_data["primary_error_code"])
            self.assertEqual(staged_data["cleanup_step"], "check_deadline")
            self.assertEqual(staged_data["cleanup_error_code"], "startup_cleanup_deadline")
            self.assertIsNone(staged_data["resource_kind"])
            raw_text = staging_file.read_text(encoding="utf-8")
            self.assertIn('"primary_phase": null', raw_text)
            self.assertIn('"primary_error_code": null', raw_text)
            self.assertIn('"resource_kind": null', raw_text)
            self.assertNotIn('"None"', raw_text)

    def test_smoke_failure_validator_null_semantics_preserved(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            report_file = tmp_path / "artifacts" / "smoke-failure.json"
            report_file.parent.mkdir(parents=True, exist_ok=True)
            report_file.write_text(json.dumps(self._valid_smoke_failure_report()), encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(report_file, staging_file)
            self.assertEqual(res.returncode, 0, f"Validator failed: {res.stderr}")
            raw_text = staging_file.read_text(encoding="utf-8")
            for field in ("primary_phase", "primary_error_code", "cleanup_step", "cleanup_error_code", "resource_kind"):
                self.assertIn(f'"{field}": null', raw_text)
                self.assertNotIn(f'"{field}": "None"', raw_text)
                self.assertNotIn(f'"{field}": false', raw_text)
                self.assertNotIn(f'"{field}": "null"', raw_text)

    def test_smoke_failure_validator_rejects_missing_required_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            # Check deletion of each of 12 fields from non-cleanup report
            base_non_cleanup = self._valid_smoke_failure_report()
            for key in list(base_non_cleanup.keys()):
                incomplete = dict(base_non_cleanup)
                del incomplete[key]
                inc_file = tmp_path / f"missing_non_cl_{key}.json"
                inc_file.write_text(json.dumps(incomplete), encoding="utf-8")
                res = self._run_smoke_failure_validator(inc_file, staging_file)
                self.assertNotEqual(res.returncode, 0, f"Validator unexpectedly accepted missing key {key}")
                self.assertIn("failure report validation failed", res.stderr)
                self.assertFalse(staging_file.exists())

            # Check deletion of each of 12 fields from cleanup report
            base_cleanup = self._valid_cleanup_smoke_failure_report()
            for key in list(base_cleanup.keys()):
                incomplete = dict(base_cleanup)
                del incomplete[key]
                inc_file = tmp_path / f"missing_cl_{key}.json"
                inc_file.write_text(json.dumps(incomplete), encoding="utf-8")
                res = self._run_smoke_failure_validator(inc_file, staging_file)
                self.assertNotEqual(res.returncode, 0, f"Validator unexpectedly accepted missing cleanup key {key}")
                self.assertIn("failure report validation failed", res.stderr)
                self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_wrong_types_for_new_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            base_cleanup = self._valid_cleanup_smoke_failure_report()
            for field in ("primary_phase", "primary_error_code", "cleanup_step", "cleanup_error_code", "resource_kind"):
                for bad_val in (["container"], {"val": "container"}, True, False, 123):
                    payload = dict(base_cleanup)
                    payload[field] = bad_val
                    t_file = tmp_path / f"bad_type_{field}_{type(bad_val).__name__}.json"
                    t_file.write_text(json.dumps(payload), encoding="utf-8")
                    res = self._run_smoke_failure_validator(t_file, staging_file)
                    self.assertNotEqual(res.returncode, 0, f"Validator accepted wrong type for {field}={bad_val}")
                    self.assertIn("failure report validation failed", res.stderr)
                    self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_contradictory_cleanup_context(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            # 1. Non-cleanup report with non-null cleanup fields
            non_cl_base = self._valid_smoke_failure_report()
            for field, bad_val in (
                ("cleanup_step", "remove_resource"),
                ("cleanup_error_code", "unclassified"),
                ("resource_kind", "container"),
                ("primary_phase", "preflight"),
                ("primary_error_code", "compose_phase_failed"),
            ):
                payload = dict(non_cl_base)
                payload[field] = bad_val
                bad_file = tmp_path / f"non_cl_contradict_{field}.json"
                bad_file.write_text(json.dumps(payload), encoding="utf-8")
                res = self._run_smoke_failure_validator(bad_file, staging_file)
                self.assertNotEqual(res.returncode, 0, f"Validator accepted non-cleanup with {field}={bad_val}")
                self.assertIn("failure report validation failed", res.stderr)
                self.assertFalse(staging_file.exists())

            # 2. Cleanup report with contradictory context
            cl_base = self._valid_cleanup_smoke_failure_report()
            for desc, overrides in (
                ("cleanup_verified_true", {"cleanup_verified": True}),
                ("cleanup_step_null", {"cleanup_step": None}),
                ("cleanup_step_unknown", {"cleanup_step": "arbitrary_step"}),
                ("cleanup_error_code_null", {"cleanup_error_code": None}),
                ("cleanup_error_code_unknown", {"cleanup_error_code": "unallowlisted_code"}),
                ("resource_kind_unknown", {"resource_kind": "invalid_kind"}),
                ("resource_kind_nonnull_on_check_deadline", {"cleanup_step": "check_deadline", "resource_kind": "container"}),
                ("resource_kind_nonnull_on_reconcile", {"cleanup_step": "reconcile_child_receipts", "resource_kind": "container"}),
                ("unpaired_primary_missing_code", {"primary_phase": "preflight", "primary_error_code": None}),
                ("unpaired_primary_missing_phase", {"primary_phase": None, "primary_error_code": "compose_phase_failed"}),
                ("invalid_primary_phase", {"primary_phase": "invalid_phase", "primary_error_code": "compose_phase_failed"}),
                ("invalid_primary_error_code", {"primary_phase": "preflight", "primary_error_code": "invalid_code"}),
            ):
                payload = dict(cl_base)
                payload.update(overrides)
                bad_file = tmp_path / f"cl_contradict_{desc}.json"
                bad_file.write_text(json.dumps(payload), encoding="utf-8")
                res = self._run_smoke_failure_validator(bad_file, staging_file)
                self.assertNotEqual(res.returncode, 0, f"Validator accepted contradictory cleanup: {desc}")
                self.assertIn("failure report validation failed", res.stderr)
                self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_handles_missing_report(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            missing_file = tmp_path / "artifacts" / "smoke-failure.json"
            staging_file = tmp_path / "staging" / "smoke-failure.json"
            github_output = tmp_path / "github_output"

            res = self._run_smoke_failure_validator(
                missing_file, staging_file, env_vars={"GITHUB_OUTPUT": str(github_output)}
            )
            self.assertEqual(res.returncode, 0, f"Missing report must not crash step: {res.stderr}")
            self.assertIn("Smoke failure report missing; no diagnostic artifact will be uploaded", res.stderr)
            self.assertFalse(staging_file.exists(), "No staging file must be created on missing source")
            if github_output.exists():
                self.assertNotIn("validated=true", github_output.read_text(encoding="utf-8"))

    def test_smoke_failure_validator_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            valid_file = tmp_path / "valid.json"
            valid_file.write_text(json.dumps(self._valid_smoke_failure_report()), encoding="utf-8")
            symlink_file = tmp_path / "symlink.json"
            symlink_file.symlink_to(valid_file)
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(symlink_file, staging_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("invalid failure report: not a regular file", res.stderr)
            self.assertFalse(staging_file.exists())

            # Broken symlink
            broken_symlink = tmp_path / "broken_symlink.json"
            broken_symlink.symlink_to(tmp_path / "nonexistent.json")
            res_broken = self._run_smoke_failure_validator(broken_symlink, staging_file)
            self.assertNotEqual(res_broken.returncode, 0)
            self.assertIn("invalid failure report: not a regular file", res_broken.stderr)
            self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_oversized_report(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            oversized_file = tmp_path / "oversized.json"
            # > 8192 bytes
            oversized_file.write_text(" " * 8200, encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(oversized_file, staging_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("failure report size out of bounds", res.stderr)
            self.assertFalse(staging_file.exists())

            # Zero bytes
            zero_file = tmp_path / "zero.json"
            zero_file.write_text("", encoding="utf-8")
            res_zero = self._run_smoke_failure_validator(zero_file, staging_file)
            self.assertNotEqual(res_zero.returncode, 0)
            self.assertIn("failure report size out of bounds", res_zero.stderr)
            self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_malformed_json(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bad_json = tmp_path / "bad.json"
            bad_json.write_text("{malformed: true", encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(bad_json, staging_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("invalid failure report JSON", res.stderr)
            self.assertNotIn("malformed", res.stderr)
            self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_unknown_or_extra_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            extra_payload = self._valid_smoke_failure_report()
            extra_payload["unexpected_field"] = "evil"
            extra_file = tmp_path / "extra.json"
            extra_file.write_text(json.dumps(extra_payload), encoding="utf-8")
            staging_file = tmp_path / "staging" / "smoke-failure.json"

            res = self._run_smoke_failure_validator(extra_file, staging_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("failure report validation failed", res.stderr)
            self.assertNotIn("unexpected_field", res.stderr)
            self.assertNotIn("evil", res.stderr)
            self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_secret_bearing_content(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_file = tmp_path / "staging" / "smoke-failure.json"
            secret = "ghp_VERYSECRET_BEARER_TOKEN_9999"

            # Secret in extra field
            secret_payload = self._valid_smoke_failure_report()
            secret_payload["token"] = secret
            sec_file = tmp_path / "secret.json"
            sec_file.write_text(json.dumps(secret_payload), encoding="utf-8")

            res = self._run_smoke_failure_validator(sec_file, staging_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("failure report validation failed", res.stderr)
            self.assertNotIn(secret, res.stderr)
            self.assertNotIn(secret, res.stdout)
            self.assertFalse(staging_file.exists())

            # Secret in new field
            cl_secret = self._valid_cleanup_smoke_failure_report()
            cl_secret["primary_error_code"] = secret
            cl_sec_file = tmp_path / "secret_field.json"
            cl_sec_file.write_text(json.dumps(cl_secret), encoding="utf-8")

            res_cl = self._run_smoke_failure_validator(cl_sec_file, staging_file)
            self.assertNotEqual(res_cl.returncode, 0)
            self.assertIn("failure report validation failed", res_cl.stderr)
            self.assertNotIn(secret, res_cl.stderr)
            self.assertNotIn(secret, res_cl.stdout)
            self.assertFalse(staging_file.exists())

    def test_smoke_failure_validator_rejects_invalid_values_and_types(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_file = tmp_path / "staging" / "smoke-failure.json"
            base = self._valid_smoke_failure_report()

            for key, bad_val in (
                ("status", "SUCCESS"),
                ("status", "RBAC_JWT_HTTP_ACCEPTED"),
                ("schema_version", 2),
                ("schema_version", "1"),
                ("phase", "invalid_phase"),
                ("error_type", "BadType"),
                ("error_code", "bad_code"),
                ("http_accepted", True),
                ("cleanup_verified", "true"),
            ):
                payload = dict(base)
                payload[key] = bad_val
                bad_file = tmp_path / f"bad_{key}.json"
                bad_file.write_text(json.dumps(payload), encoding="utf-8")
                res = self._run_smoke_failure_validator(bad_file, staging_file)
                self.assertNotEqual(res.returncode, 0, f"Validator accepted bad {key}={bad_val}")
                self.assertIn("failure report validation failed", res.stderr)
                self.assertFalse(staging_file.exists())

    def _extract_bootstrap_validator_script(self) -> str:
        job = self._extract_job_section("dynsec-bootstrap-tests")
        step_match = re.search(
            r"- name: Validate bootstrap report\s+run: \|\s+python3 -c '(.*?)' \"\$DYNSEC_BOOTSTRAP_REPORT\"",
            job,
            re.DOTALL,
        )
        self.assertIsNotNone(step_match, "Validate bootstrap report inline script not found")
        return textwrap.dedent(step_match.group(1)).strip()

    def _run_bootstrap_validator(self, report_path: Path) -> subprocess.CompletedProcess:
        script = self._extract_bootstrap_validator_script()
        return subprocess.run(
            [sys.executable, "-c", script, str(report_path)],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )

    def _valid_bootstrap_report(self) -> dict:
        nonce = "0123456789abcdef0123456789abcdef"
        return {
            "status": "success",
            "volume_name": f"mosquitto_dynsec_{nonce[:24]}",
            "runtime_status": "NOT_READY",
            "ownership": "1883:1883",
            "mode": "container",
            "image_ref": "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9",
            "image_digest": "sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9",
            "run_nonce": nonce,
            "assertions_checked": 17,
            "cleaned_up": True,
            "collision_rejected": True,
        }

    def test_bootstrap_validator_accepts_valid_current_report(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_file = Path(tmp_dir) / "report.json"
            report_file.write_text(json.dumps(self._valid_bootstrap_report()), encoding="utf-8")
            res = self._run_bootstrap_validator(report_file)
            self.assertEqual(res.returncode, 0, f"Validator failed on valid report: {res.stderr}")
            self.assertIn("Bootstrap report validated successfully", res.stdout)

    def test_bootstrap_validator_rejects_missing_file_and_symlink(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_file = Path(tmp_dir) / "nonexistent.json"
            res = self._run_bootstrap_validator(missing_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("report file missing or symlink", res.stderr)

            valid_file = Path(tmp_dir) / "valid.json"
            valid_file.write_text(json.dumps(self._valid_bootstrap_report()), encoding="utf-8")
            symlink_file = Path(tmp_dir) / "symlink.json"
            symlink_file.symlink_to(valid_file)
            res = self._run_bootstrap_validator(symlink_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("report file missing or symlink", res.stderr)

    def test_bootstrap_validator_rejects_malformed_json_and_missing_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bad_json = Path(tmp_dir) / "bad.json"
            bad_json.write_text("{malformed_json: true", encoding="utf-8")
            res = self._run_bootstrap_validator(bad_json)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("invalid report JSON", res.stderr)
            self.assertNotIn("malformed_json", res.stderr)

            base = self._valid_bootstrap_report()
            for key in list(base.keys()):
                incomplete = dict(base)
                del incomplete[key]
                inc_file = Path(tmp_dir) / f"missing_{key}.json"
                inc_file.write_text(json.dumps(incomplete), encoding="utf-8")
                res = self._run_bootstrap_validator(inc_file)
                self.assertNotEqual(res.returncode, 0, f"Validator unexpectedly accepted missing key {key}")
                self.assertIn("report schema mismatch", res.stderr)

    def test_bootstrap_validator_rejects_string_booleans(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for field in ("cleaned_up", "collision_rejected"):
                for bad_val in ("true", "True", "false", 1):
                    payload = self._valid_bootstrap_report()
                    payload[field] = bad_val
                    t_file = Path(tmp_dir) / f"{field}_{bad_val}.json"
                    t_file.write_text(json.dumps(payload), encoding="utf-8")
                    res = self._run_bootstrap_validator(t_file)
                    self.assertNotEqual(res.returncode, 0, f"Validator accepted non-boolean {bad_val} for {field}")

    def test_bootstrap_validator_rejects_injected_secrets_and_extra_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            secret_value = "super_secret_plaintext_token_12345"
            payload = self._valid_bootstrap_report()
            payload["secret_token"] = secret_value
            sec_file = Path(tmp_dir) / "injected_secret.json"
            sec_file.write_text(json.dumps(payload), encoding="utf-8")
            res = self._run_bootstrap_validator(sec_file)
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("report schema mismatch", res.stderr)
            self.assertNotIn(secret_value, res.stderr)
            self.assertNotIn(secret_value, res.stdout)

    def test_bootstrap_validator_rejects_unknown_image_or_volume_name(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for key, bad_val in (
                ("image_ref", "eclipse-mosquitto:latest"),
                ("image_digest", "sha256:0000000000000000000000000000000000000000000000000000000000000000"),
                ("volume_name", "mosquitto_dynsec_unknown"),
                ("volume_name", "mosquitto_dynsec_0123456789abcdef01234568"),  # mismatch with nonce
                ("run_nonce", "invalid_non_hex_nonce_length_bad!"),
            ):
                payload = self._valid_bootstrap_report()
                payload[key] = bad_val
                t_file = Path(tmp_dir) / f"{key}_bad.json"
                t_file.write_text(json.dumps(payload), encoding="utf-8")
                res = self._run_bootstrap_validator(t_file)
                self.assertNotEqual(res.returncode, 0, f"Validator accepted bad {key}={bad_val}")

    def test_bootstrap_validator_rejects_invalid_types_modes_and_counts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for key, bad_val in (
                ("mode", "host"),
                ("runtime_status", "READY"),
                ("ownership", "0:0"),
                ("assertions_checked", 16),
                ("assertions_checked", "17"),
                ("assertions_checked", True),
                ("status", "failed"),
            ):
                payload = self._valid_bootstrap_report()
                payload[key] = bad_val
                t_file = Path(tmp_dir) / f"{key}_{bad_val}.json"
                t_file.write_text(json.dumps(payload), encoding="utf-8")
                res = self._run_bootstrap_validator(t_file)
                self.assertNotEqual(res.returncode, 0, f"Validator accepted {key}={bad_val}")

    def test_existing_jobs_preserved(self):
        for name in (
            "stage2-e2e",
            "lint-and-test",
            "cross-compile",
            "migration-check",
            "docker-build",
            "legacy-runtime-image-build",
            "authorization-integration",
            "provisioning-integration",
            "auth-integration",
            "mosquitto-credential-integration",
            "mosquitto-runtime-integration",
            "dynsec-native-integration",
            "dynsec-bootstrap-tests",
            "native-compose-smoke",
        ):
            self.assertTrue(self._extract_job_section(name), f"Existing job {name} must be preserved")


if __name__ == "__main__":
    unittest.main()
