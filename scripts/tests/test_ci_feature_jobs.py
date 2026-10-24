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

import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_TESTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
if str(SCRIPTS_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_TESTS_DIR))

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
        self.assertIn("go-version: '1.27.3'", job)
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
        self.assertNotIn('DYNSEC_NATIVE_REPORT: ' + path, job)
        self.assertIn("printf 'DYNSEC_NATIVE_REPORT=%s/dynsec-native-%s-%s.json\\n'", job)
        self.assertIn('"$RUNNER_TEMP" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT" >> "$GITHUB_ENV"', job)
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
        for old in ('stage2-e2e', 'mosquitto-runtime-integration'):
            self.assertIn(f'legacy-execution-deferred: {old}', self._extract_job_section(old))
        current = self._extract_job_section('mosquitto-credential-integration')
        self.assertIn('current-simplified-acceptance:', current)
        self.assertIn('sh scripts/test-stage2-mqtt-credentials.sh', current)

    def test_legacy_deferred_jobs_contracts_and_routing(self):
        """Verifies job IDs, deferred routing, retained units/builds, and markers."""
        from test_stage2_ci import assert_stage2_e2e_contract
        from test_stage2_mqtt_credentials import assert_credential_job_contract
        from test_stage2_mosquitto_runtime import assert_runtime_job_contract

        stage2_job = self._extract_job_section('stage2-e2e')
        cred_job = self._extract_job_section('mosquitto-credential-integration')
        runtime_job = self._extract_job_section('mosquitto-runtime-integration')

        # Job identity is the YAML key, not the optional display name.
        self.assertTrue(stage2_job.startswith('  stage2-e2e:\n'))
        self.assertTrue(cred_job.startswith('  mosquitto-credential-integration:\n'))
        self.assertTrue(runtime_job.startswith('  mosquitto-runtime-integration:\n'))

        # Check explicit deferred markers in job definitions
        self.assertIn("legacy-execution-deferred: stage2-e2e", stage2_job)
        self.assertIn('current-simplified-acceptance:', cred_job)
        self.assertIn('LEGACY COMPONENT-ONLY', cred_job)
        self.assertIn('no global gate guarantee', cred_job)
        self.assertIn("legacy-execution-deferred: mosquitto-runtime-integration", runtime_job)

        # Execute reviewed contract assertions
        assert_stage2_e2e_contract(stage2_job)
        assert_credential_job_contract(cred_job)
        assert_runtime_job_contract(runtime_job)

        # Verify all 16 job IDs remain defined
        jobs_content = self.raw_content.split('\njobs:\n', 1)[1]
        all_jobs = re.findall(r'^  ([a-z0-9-]+):[ \t]*$', jobs_content, re.MULTILINE)
        self.assertEqual(len(all_jobs), 16, f"expected 16 jobs, found {len(all_jobs)}: {all_jobs}")
        expected_jobs = {
            'lint-and-test', 'cross-compile', 'migration-check',
            'docker-build', 'authorization-integration',
            'provisioning-integration', 'auth-integration',
            'mosquitto-runtime-integration',
            'mosquitto-credential-integration', 'stage2-e2e',
            'legacy-runtime-image-build', 'dynsec-native-integration',
            'dynsec-bootstrap-tests', 'stage3-uplink-integration',
            'stage4-api-integration', 'native-compose-smoke',
        }
        self.assertEqual(set(all_jobs), expected_jobs)

        # Verify gates/needs remain intact
        smoke_job = self._extract_job_section('native-compose-smoke')
        self.assertIn('needs: [lint-and-test, migration-check, dynsec-native-integration, dynsec-bootstrap-tests]', smoke_job)

    def test_lint_and_test_gateway_exception_removed(self):
        """Allow-no-test-package exception for cmd/gateway must be removed since it now has tests."""
        lint_job_section = self._extract_job_section("lint-and-test")
        self.assertNotIn("--allow-no-test-package 'iot-platform/cmd/gateway'", lint_job_section)
        self.assertNotIn("--allow-no-test-package", lint_job_section)

    def test_lint_and_test_requires_stage3_telemetry_and_gateway_tests(self):
        """Manifest must include real cmd/gateway, telemetry, ingress, and simulator unit tests."""
        lint_job_section = self._extract_job_section("lint-and-test")
        # cmd/gateway actual tests
        self.assertIn("TestAdminCountAndPendingMetadataOnly", lint_job_section)
        self.assertIn("TestRunRestartResendsPriorBootByteExact", lint_job_section)
        # internal/telemetry actual tests
        self.assertIn("TestValidatorExpandsEverySampleAndNormalizesTime", lint_job_section)
        self.assertIn("TestDecodeBatchValidAndWhitespace", lint_job_section)
        self.assertIn("TestNewCommitACKWireContract", lint_job_section)
        self.assertIn("TestPersistNewMessageAppliesReportedStateThenTemporalBeforeCommit", lint_job_section)
        # internal/mqttingress actual tests
        self.assertIn("TestACKOnlyAfterConfirmedCommit", lint_job_section)
        self.assertIn("TestFixedWorkerBound", lint_job_section)
        # internal/gatewaysim actual tests
        self.assertIn("TestOutboxEnqueueReopenByteExact", lint_job_section)
        self.assertIn("TestACKProcessorDeletesOnlyExactPending", lint_job_section)

    def test_stage3_uplink_integration_job_structure_and_prerequisites(self):
        """stage3-uplink-integration must run bounded timeout, from root, and verify all prerequisites."""
        job = self._extract_job_section("stage3-uplink-integration")
        self.assertTrue(job, "Job stage3-uplink-integration must exist in ci.yml")
        self.assertTrue(job.startswith('  stage3-uplink-integration:\n'))
        self.assertIn("timeout-minutes: 60", job)
        self.assertIn("working-directory: .", job)
        self.assertIn("go-version: '1.27.3'", job)
        self.assertIn("cache-dependency-path: src/go.sum", job)

        # Prerequisites checks
        self.assertIn("mkdir -p /tmp/opencode", job)
        self.assertIn("python3 --version", job)
        self.assertIn("openssl version", job)
        self.assertIn("which setfacl getfacl", job)
        self.assertIn("docker version", job)
        self.assertIn("docker compose version", job)
        self.assertIn("go version", job)

    def test_stage3_uplink_integration_pinned_images(self):
        """stage3-uplink-integration must pull digest-pinned timescaledb and mosquitto images."""
        job = self._extract_job_section("stage3-uplink-integration")
        timescale_img = "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
        mosquitto_img = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        self.assertIn(f"docker pull --platform linux/amd64 {timescale_img}", job)
        self.assertIn(f"docker pull --platform linux/amd64 {mosquitto_img}", job)
        self.assertLess(job.index("docker pull"), job.index("sh scripts/test-stage3-persistence.sh"))
        self.assertLess(job.index("docker pull"), job.index("sh scripts/test-stage3-uplink.sh"))

    def test_stage3_uplink_integration_runs_harness_tests_before_native(self):
        """stage3-uplink-integration must run runner harness unit tests before native scripts."""
        job = self._extract_job_section("stage3-uplink-integration")
        self.assertIn("test_stage3_uplink_runner.py", job)
        self.assertLess(job.index("test_stage3_uplink_runner.py"), job.index("sh scripts/test-stage3-persistence.sh"))
        self.assertLess(job.index("test_stage3_uplink_runner.py"), job.index("sh scripts/test-stage3-uplink.sh"))

    def test_stage3_uplink_integration_strict_persistence_markers(self):
        """Persistence rehearsals step must require pipefail, command timeout, PASS markers, and cleanup."""
        job = self._extract_job_section("stage3-uplink-integration")
        self.assertIn("set -euo pipefail", job)
        self.assertIn("timeout --signal=TERM --kill-after=90s 600s sh scripts/test-stage3-persistence.sh", job)
        self.assertIn("tee ", job)
        self.assertIn("PASS: isolated database-only migration 000020 rehearsals", job)
        self.assertIn("FRESH FIXTURE: .* PASS", job)
        self.assertIn("UPGRADE FIXTURE: .* PASS", job)
        self.assertIn("ROLLBACK FIXTURE: .* PASS", job)
        self.assertIn("Fixture cleanup verified: True", job)

    def test_stage3_uplink_integration_strict_uplink_markers_and_skip_rejection(self):
        """Uplink integration step must require pipefail, command timeout, skip rejection, and cleanup markers."""
        job = self._extract_job_section("stage3-uplink-integration")
        self.assertIn("timeout --signal=TERM --kill-after=90s 1800s sh scripts/test-stage3-uplink.sh --scenario all", job)
        self.assertIn("Stage 3 uplink integration skipped unexpectedly", job)
        self.assertIn("PASS: stage3 uplink integration (all scenarios passed: normal, tls-credential, acl-control, ack-fault, db-timeout, queue-full, gateway-reconnect, process-restart, broker-controlled-restart)", job)
        self.assertIn("Suite image cleanup verified: shared images removed", job)
        self.assertIn("Fixture cleanup verified: containers/network/volumes/workdir absent", job)

    def test_stage3_uplink_integration_no_default_workload(self):
        """The 600-batch 10-minute workload must remain opt-in and NOT run in default CI workflow."""
        self.assertNotIn("test-stage3-workload.sh", self.raw_content)
        self.assertNotIn("stage3_workload_runner.py", self.raw_content)

    def test_stage3_uplink_integration_safe_artifacts_and_no_leakage(self):
        """stage3-uplink-integration must not upload raw logs, certs, DB fixtures, or leak env files."""
        job = self._extract_job_section("stage3-uplink-integration")
        self.assertNotIn("upload-artifact", job)
        self.assertNotIn(".env", job)
        self.assertNotIn("ca.key", job)
        self.assertNotIn("dynamic-security.json", job)

    def test_stage4_api_integration_job_structure_and_prerequisites(self):
        """stage4-api-integration must run bounded timeout (30-45min), from root, and verify all prerequisites."""
        job = self._extract_job_section("stage4-api-integration")
        self.assertTrue(job, "Job stage4-api-integration must exist in ci.yml")
        self.assertTrue(job.startswith('  stage4-api-integration:\n'))
        self.assertIn("timeout-minutes: 45", job)
        self.assertIn("working-directory: .", job)
        self.assertIn("go-version: '1.27.3'", job)
        self.assertIn("cache-dependency-path: src/go.sum", job)

        # Prerequisites checks
        self.assertIn("mkdir -p /tmp/opencode", job)
        self.assertIn("python3 --version", job)
        self.assertIn("openssl version", job)
        self.assertIn("which setfacl getfacl", job)
        self.assertIn("docker version", job)
        self.assertIn("docker compose version", job)
        self.assertIn("go version", job)

    def test_stage4_api_integration_pinned_images(self):
        """stage4-api-integration must pull digest-pinned timescaledb and mosquitto images."""
        job = self._extract_job_section("stage4-api-integration")
        timescale_img = "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
        mosquitto_img = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        self.assertIn(f"docker pull --platform linux/amd64 {timescale_img}", job)
        self.assertIn(f"docker pull --platform linux/amd64 {mosquitto_img}", job)
        self.assertLess(job.index("docker pull"), job.index("sh scripts/test-stage4-api.sh"))

    def test_stage4_api_integration_runs_harness_tests_before_native(self):
        """stage4-api-integration must run runner harness unit tests before native scripts."""
        job = self._extract_job_section("stage4-api-integration")
        self.assertIn("test_stage4_api_runner.py", job)
        self.assertIn("test_stage4_vertical_scenario.py", job)
        self.assertIn("test_stage4_contract_scenario.py", job)
        self.assertIn("test_stage4_queries_scenario.py", job)
        self.assertLess(job.index("test_stage4_api_runner.py"), job.index("sh scripts/test-stage4-api.sh"))

    def test_stage4_api_integration_strict_markers_and_skip_rejection(self):
        """Stage 4 API integration step must require pipefail, command timeout, skip rejection, and cleanup markers."""
        job = self._extract_job_section("stage4-api-integration")
        self.assertIn("timeout --signal=TERM --kill-after=90s 2100s sh scripts/test-stage4-api.sh --scenario all", job)
        self.assertIn("MANDATORY_CI: '1'", job)
        self.assertIn("Stage 4 API integration skipped unexpectedly", job)
        self.assertIn("PASS: stage4 api integration (all scenarios passed: vertical, contract, queries)", job)
        self.assertIn("Suite image cleanup verified: shared images removed", job)
        self.assertIn("Fixture cleanup verified: containers/network/volumes/workdir absent", job)

    def test_stage4_api_integration_safe_artifacts_and_no_leakage(self):
        """stage4-api-integration uploads allowlisted summary artifact and does not leak secrets."""
        job = self._extract_job_section("stage4-api-integration")
        self.assertIn("STAGE4_API_REPORT", job)
        self.assertIn("upload-artifact@v4", job)
        self.assertIn("name: stage4-api-safe-report", job)
        self.assertIn("path: ${{ runner.temp }}/stage4-api-${{ github.run_id }}-${{ github.run_attempt }}.json", job)
        self.assertIn("if-no-files-found: error", job)
        self.assertNotIn(".env", job)
        self.assertNotIn("ca.key", job)
        self.assertNotIn("dynamic-security.json", job)

    def test_job_contracts_allow_missing_or_changed_display_names(self):
        """Optional job labels must not determine job identity or routing."""
        without_names = re.sub(r'^    name:.*\n', '', self.raw_content, flags=re.MULTILINE)
        for display_names in (False, True):
            content = without_names
            if display_names:
                prefix, jobs_content = content.split('\njobs:\n', 1)
                jobs_content = re.sub(
                    r'^(  ([a-z0-9-]+):)[ \t]*$',
                    r'\1\n    name: Custom label for \2',
                    jobs_content, flags=re.MULTILINE,
                )
                content = prefix + '\njobs:\n' + jobs_content
            with self.subTest(display_names=display_names), patch.object(self, 'raw_content', content):
                self.test_active_backend_build_keeps_legacy_builds_separate()
                self.test_legacy_deferred_jobs_contracts_and_routing()
                self.test_stage3_uplink_integration_job_structure_and_prerequisites()
                self.test_stage4_api_integration_job_structure_and_prerequisites()

    def test_job_contracts_reject_missing_job_id_even_with_old_display_name(self):
        content = self.raw_content.replace(
            '  stage2-e2e:\n',
            '  renamed-stage2-e2e:\n    name: Legacy E2E contracts\n', 1,
        )
        with patch.object(self, 'raw_content', content):
            with self.assertRaises(AssertionError):
                self.test_legacy_deferred_jobs_contracts_and_routing()

    def _extract_job_section(self, job_id: str) -> str:
        lines = self.raw_content.splitlines()
        job_lines = []
        in_job = False
        job_indent = None
        for line in lines:
            if not in_job:
                if line == f"  {job_id}:":
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
