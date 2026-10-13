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
        self.assertIn("go-version: '1.27.1'", job)
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
        self.assertNotIn("if: always()", job)
        self.assertNotIn("continue-on-error", job)
        self.assertNotIn("prune", job)
        self.assertNotIn(".env", job)

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
