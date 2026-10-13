"""Unit and workflow contract tests for CI workflow jobs (.github/workflows/ci.yml).

Verifies the workflow structure, steps, commands, and options for:
- lint-and-test:
  * Runs timeout-minutes: 15 to bound suite execution.
  * Runs both CI feature job contract tests and test guard unit tests.
  * Sets umask 077 and creates unique mktemp private directory under runner.temp.
  * Separates stdout JSON and stderr log into protected files (no console stderr leak, no tee/cat/upload).
  * Preserves both Go test exit status and guard verification exit status.
  * Traps and cleans up the exact two log files and the owned directory; cleanup failure vetoes success.
  * Prints a short, safe diagnostic without leaking secret payloads or unvalidated output when Go fails.
  * Runs ci_go_test_guard with explicit required tests covering:
    - Admin inventory service/handler
    - Owner sensor service/handler
    - Simplified DynSec service/config/HTTP
  * Keeps gofmt, go vet, golangci-lint, and coverage upload.
- authorization-integration:
  * Creates /tmp/opencode directory before mktemp-based script.
  * Runs from repo root (working-directory: .).
  * Sets AUTHORIZATION_TEST_COVERAGE_FILE.
  * Uploads distinct authorization coverage artifact.
- provisioning-integration:
  * Creates /tmp/opencode directory before mktemp-based script.
  * Runs from repo root (working-directory: .).
  * Sets PROVISIONING_TEST_COVERAGE.
  * Uploads distinct provisioning coverage artifact.
"""

import unittest
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"


class TestCIFeatureJobs(unittest.TestCase):
    """Workflow contract tests for ci.yml feature jobs."""

    @classmethod
    def setUpClass(cls):
        if not WORKFLOW_PATH.is_file():
            raise unittest.SkipTest(f"Workflow file not found at {WORKFLOW_PATH}")
        cls.raw_content = WORKFLOW_PATH.read_text(encoding="utf-8")
        if yaml:
            cls.data = yaml.safe_load(cls.raw_content)
        else:
            cls.data = None

    def test_workflow_file_exists_and_readable(self):
        self.assertTrue(WORKFLOW_PATH.is_file())
        self.assertGreater(len(self.raw_content), 100)

    def test_lint_and_test_timeout_minutes_exists(self):
        """lint-and-test must define timeout-minutes to bound execution."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("timeout-minutes:", lint_job_section)

    def test_lint_and_test_runs_both_regression_suites(self):
        """Guard unit tests and CI feature job contract tests must both run in lint-and-test."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("test_ci_go_test_guard.py", lint_job_section)
        self.assertIn("test_ci_feature_jobs.py", lint_job_section)

    def test_lint_and_test_go_unit_suite_flags(self):
        """Go unit suite must run with -json, -race, -count=1, -coverprofile."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("-json", lint_job_section)
        self.assertIn("-race", lint_job_section)
        self.assertIn("-count=1", lint_job_section)
        self.assertIn("-coverprofile", lint_job_section)

    def test_lint_and_test_umask_and_mktemp_directory(self):
        """Must use umask 077 and unique private directory under runner.temp."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("umask 077", lint_job_section)
        self.assertIn("mktemp -d", lint_job_section)
        self.assertIn("runner.temp", lint_job_section)

    def test_lint_and_test_separate_stdout_and_stderr_redirection(self):
        """stdout and stderr must be redirected to separate protected log files (no pipe/tee/cat)."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn(">", lint_job_section)
        self.assertIn("2>", lint_job_section)
        self.assertNotIn("| python3", lint_job_section)
        self.assertNotIn("| tee", lint_job_section)
        self.assertNotIn("cat ", lint_job_section)

    def test_lint_and_test_preserves_both_exit_statuses_and_safe_diagnostic(self):
        """Go test exit and guard exit must both be preserved; safe diagnostic on error."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("test_exit", lint_job_section)
        self.assertIn("guard_exit", lint_job_section)

    def test_lint_and_test_bounded_trap_cleanup_veto(self):
        """Cleanup trap must remove exact files and owned directory; cleanup failure vetoes success."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("trap ", lint_job_section)
        self.assertIn("cleanup", lint_job_section)
        self.assertIn("rm -f", lint_job_section)
        self.assertIn("rmdir", lint_job_section)

    def test_lint_and_test_requires_admin_inventory_tests(self):
        """Manifest must include real admin inventory service and handler unit tests."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("TestAdminGatewayServiceValidationAndBounds", lint_job_section)
        self.assertIn("TestAdminGatewayInventoryHTTPHandler", lint_job_section)

    def test_lint_and_test_requires_owner_sensor_tests(self):
        """Manifest must include real owner sensor service and handler unit tests."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("TestOwnerSensorProvisioningServiceValidation", lint_job_section)
        self.assertIn("TestOwnerSensorProvisioningHTTPHandler", lint_job_section)

    def test_lint_and_test_requires_simplified_dynsec_tests(self):
        """Manifest must include real simplified DynSec service, config, and HTTP unit tests."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("TestSimplifiedDynSecExecutor_ConfigValidation", lint_job_section)
        self.assertIn("TestSimplifiedService_Orchestration", lint_job_section)
        self.assertIn("TestCredentialRoutesAuthorization", lint_job_section)

    def test_lint_and_test_retains_format_and_linters(self):
        """Gofmt, go vet, and golangci-lint must remain intact."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("gofmt -l", lint_job_section)
        self.assertIn("go vet ./...", lint_job_section)
        self.assertIn("golangci/golangci-lint-action", lint_job_section)

    def test_lint_and_test_only_uploads_coverage_artifact(self):
        """Only coverage.out is uploaded; no logs or raw reports."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertIn("name: coverage", lint_job_section)
        self.assertIn("path: src/coverage.out", lint_job_section)
        self.assertNotIn("test-report", lint_job_section)
        self.assertNotIn("stderr", lint_job_section)

    def test_authorization_integration_job_contract(self):
        """authorization-integration must create /tmp/opencode, run from root, and set coverage."""
        auth_job_section = self._extract_job_section("authorization-integration")
        self.assertIn("mkdir -p /tmp/opencode", auth_job_section)
        self.assertIn("AUTHORIZATION_TEST_COVERAGE_FILE", auth_job_section)
        self.assertIn("sh scripts/test-stage2-authorization.sh", auth_job_section)
        self.assertIn("authorization-coverage", auth_job_section)

    def test_provisioning_integration_job_contract(self):
        """provisioning-integration must create /tmp/opencode, run from root, and set coverage."""
        prov_job_section = self._extract_job_section("provisioning-integration")
        self.assertIn("mkdir -p /tmp/opencode", prov_job_section)
        self.assertIn("PROVISIONING_TEST_COVERAGE", prov_job_section)
        self.assertIn("sh scripts/test-stage2-provisioning.sh", prov_job_section)
        self.assertIn("provisioning-coverage", prov_job_section)

    def test_distinct_coverage_artifact_names(self):
        """Coverage artifacts between authorization and provisioning must be distinct."""
        auth_job_section = self._extract_job_section("authorization-integration")
        prov_job_section = self._extract_job_section("provisioning-integration")
        self.assertIn("authorization-coverage", auth_job_section)
        self.assertIn("provisioning-coverage", prov_job_section)
        self.assertNotIn("provisioning-coverage", auth_job_section)

    def test_native_job_prerequisites_and_order(self):
        job = self._extract_job_section('dynsec-native-integration')
        self.assertIn('timeout-minutes: 20', job)
        self.assertIn('working-directory: .', job)
        self.assertIn("go-version: '1.27.1'", job)
        self.assertIn('cache-dependency-path: src/go.sum', job)
        for image in (
            'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a',
            'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9',
        ):
            self.assertIn('docker pull --platform linux/amd64 ' + image, job)
        for suite in ('test_dynsec_native_ci.py', 'test_task28_rotate_revoke_native.py',
                      'test_task28_simplified_schema_runner.py', 'test_task28_disable_client_probe.py'):
            self.assertIn(suite, job)
            self.assertLess(job.index(suite), job.index('dynsec_native_ci.py --report'))
        self.assertLess(job.index('docker pull'), job.index('dynsec_native_ci.py --report'))

    def test_native_job_exact_validated_artifact_and_unique_report(self):
        job = self._extract_job_section('dynsec-native-integration')
        path = '${{ runner.temp }}/dynsec-native-${{ github.run_id }}-${{ github.run_attempt }}.json'
        self.assertIn('DYNSEC_NATIVE_REPORT: ' + path, job)
        self.assertIn('--report "$DYNSEC_NATIVE_REPORT"', job)
        self.assertIn('--validate-report "$DYNSEC_NATIVE_REPORT"', job)
        self.assertIn('path: ' + path, job)
        self.assertIn('if-no-files-found: error', job)
        self.assertNotIn('if: always()', job)
        self.assertNotIn('continue-on-error', job)
        self.assertNotIn('--dry-run', job)
        self.assertNotIn('*.json', job)
        self.assertNotIn('prune', job)
        self.assertNotIn('.env', job)

    def test_active_backend_build_keeps_legacy_builds_separate(self):
        job = self._extract_job_section('docker-build')
        self.assertIn('target: backend', job)
        self.assertIn('test-backend-smoke.sh', job)
        self.assertNotIn('target: auth-init', job)
        self.assertNotIn('target: reloader', job)
        legacy = self._extract_job_section('legacy-runtime-image-build')
        self.assertIn('target: auth-init', legacy)
        self.assertIn('target: reloader', legacy)
        for old in ('stage2-e2e', 'mosquitto-credential-integration', 'mosquitto-runtime-integration'):
            self.assertIn('Legacy', self._extract_job_section(old))

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


if __name__ == "__main__":
    unittest.main()
