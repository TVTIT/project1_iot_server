"""Task 2.6.12 fixture guards; never run side effects here."""
import re
import unittest

import stage2_mqtt_credentials as harness


def extract_job_section(workflow_text: str, job_id: str) -> str:
    match = re.search(r'^  ' + re.escape(job_id) + r':(?:[^\n]*\n)(.*?)(?=^  [a-z0-9-]+:|^\S|\Z)',
                      workflow_text, re.MULTILINE | re.DOTALL)
    if not match:
        raise AssertionError(f"job '{job_id}' not found in workflow")
    return match.group(0)


def assert_credential_job_contract(job: str) -> None:
    marker = 'legacy-execution-deferred: mosquitto-credential-integration'
    if 'continue-on-error' in job:
        raise AssertionError("job must not use continue-on-error")
    if '|| true' in job:
        raise AssertionError("job must not suppress failure with || true")
    if re.search(r'(?m)^\s*if:\s*false\b', job):
        raise AssertionError("job must not disable steps with if: false")

    if "-p 'test_stage2_mqtt_credentials.py'" not in job:
        raise AssertionError("job must retain test_stage2_mqtt_credentials.py unit tests")
    if "-p 'test_task2612_standalone.py'" not in job:
        raise AssertionError("job must retain test_task2612_standalone.py unit tests")
    if 'golangci-lint' not in job:
        raise AssertionError("job must retain golangci-lint check")
    if 'docker build --target backend' not in job or 'docker build --target credential-controller' not in job:
        raise AssertionError("job must retain legacy Docker build command targets (backend and credential-controller)")

    if marker in job:
        run_blocks = re.findall(r'(?m)^\s*run:\s*\|?\n(.*?)(?=\n\s*(?:uses:|name:|env:|with:|if:|id:|\Z))',
                                job, re.DOTALL)
        step_found = False
        for block in run_blocks:
            active_lines = [l for l in block.splitlines() if not l.strip().startswith('#')]
            active_text = '\n'.join(active_lines)
            if marker in active_text and ('GITHUB_STEP_SUMMARY' in active_text or '$GITHUB_STEP_SUMMARY' in active_text):
                if re.search(r'>>\s*["\']?\$?GITHUB_STEP_SUMMARY["\']?', active_text):
                    step_found = True
                    text_lower = active_text.lower()
                    has_omitted = any(k in text_lower for k in ('omitted', 'legacy credential', 'standalone integration'))
                    has_unresolved = any(k in text_lower for k in ('unresolved', 'failure', 'controller gate', 'openverified', 'exit 2'))
                    has_replacement = any(k in text_lower for k in ('replacement', 'dynsec-native-integration', 'native-compose-smoke'))
                    if not (has_omitted and has_unresolved and has_replacement):
                        raise AssertionError("deferred notice must explain omitted native scenario, known unresolved failure, and replacement job reference")
                    break
        if not step_found:
            raise AssertionError("deferred marker must be in an executable notice step writing to $GITHUB_STEP_SUMMARY, not a comment")

        if 'sh scripts/test-stage2-mqtt-credentials.sh' in job:
            raise AssertionError("deferred mode must not execute native script test-stage2-mqtt-credentials.sh")

        if 'task2612-*.cover' in job or 'mosquitto-credential-coverage' in job:
            raise AssertionError("deferred mode must not retain native coverage upload")

        header = job.split('steps:', 1)[0]
        name_match = re.search(r'(?m)^\s*name:\s*(.+)$', header)
        if name_match:
            label = name_match.group(1).strip().strip("'\"")
            if re.search(r'\b(native\s+)?pass\b', label, re.IGNORECASE):
                raise AssertionError(f"misleading job display label claiming pass: {label}")
            if 'Legacy credential checks' not in label and 'Legacy' not in label:
                raise AssertionError(f"job display label must identify legacy credential checks: {label}")
    else:
        if 'sh scripts/test-stage2-mqtt-credentials.sh' not in job:
            raise AssertionError("intact native mode requires sh scripts/test-stage2-mqtt-credentials.sh")
        if 'task2612-*.cover' not in job or 'mosquitto-credential-coverage' not in job:
            raise AssertionError("intact native mode requires coverage upload artifact")


class CredentialHarnessTests(unittest.TestCase):
    def test_selector_requires_actual_pass(self):
        for output in (b'PASS\n', b'--- SKIP: TestCredentialHTTPIsolated\nPASS\n',
                       b'--- PASS: TestOther\nPASS\n'):
            with self.assertRaises(RuntimeError):
                harness.verify_selector(output)
        harness.verify_selector(b'--- PASS: TestCredentialHTTPIsolated (1s)\nPASS\n')

    def test_clean_environment_and_workflow(self):
        self.assertNotIn('DATABASE_URL', harness.ENV)
        workflow = (harness.ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('mosquitto-credential-integration:', workflow)
        self.assertIn('mosquitto-runtime-integration:', workflow)
        job = extract_job_section(workflow, 'mosquitto-credential-integration')
        assert_credential_job_contract(job)

    def test_deferred_workflow_contract_and_mutations(self):
        valid_deferred = '''  mosquitto-credential-integration:
    name: Legacy credential checks
    runs-on: ubuntu-latest
    timeout-minutes: 15
    defaults:
      run:
        working-directory: .
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v7
        with:
          go-version: '1.27.1'
          cache-dependency-path: src/go.sum
      - name: Prepare coverage directory
        run: mkdir -p /tmp/opencode
      - name: Test credential harness
        run: |
          python3 -m unittest discover -s scripts/tests -p 'test_stage2_mqtt_credentials.py'
          python3 -m unittest discover -s scripts/tests -p 'test_task2612_standalone.py'
      - name: Legacy credential execution deferred notice
        run: |
          echo "legacy-execution-deferred: mosquitto-credential-integration" >> "$GITHUB_STEP_SUMMARY"
          echo "legacy-execution-deferred: mosquitto-credential-integration"
          echo "Omitted native scenario: legacy credential lifecycle and controller standalone integration."
          echo "Known unresolved failure: controller gate closed without OpenVerified."
          echo "Current replacement job: dynsec-native-integration and native-compose-smoke."
      - name: Lint credentials
        uses: golangci/golangci-lint-action@v9
        with:
          version: v2.14.0
          working-directory: ./src
          args: --timeout=5m
      - name: Build legacy controller
        run: |
          docker build --target backend -t task2612-backend:ci src
          docker build --target credential-controller -t task2612-controller:ci src
'''
        # 1. Valid deferred candidate must pass
        assert_credential_job_contract(valid_deferred)

        # 2. Silent deletion without deferred notice must fail
        silent_deletion = valid_deferred.replace('      - name: Legacy credential execution deferred notice\n'
                                                 '        run: |\n'
                                                 '          echo "legacy-execution-deferred: mosquitto-credential-integration" >> "$GITHUB_STEP_SUMMARY"\n'
                                                 '          echo "legacy-execution-deferred: mosquitto-credential-integration"\n'
                                                 '          echo "Omitted native scenario: legacy credential lifecycle and controller standalone integration."\n'
                                                 '          echo "Known unresolved failure: controller gate closed without OpenVerified."\n'
                                                 '          echo "Current replacement job: dynsec-native-integration and native-compose-smoke."\n', '')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(silent_deletion)

        # 3. Marker present but unit tests removed must fail
        no_units = valid_deferred.replace("-p 'test_stage2_mqtt_credentials.py'", "-p 'other.py'")
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(no_units)

        # 4. Marker present but lint removed must fail
        no_lint = valid_deferred.replace('uses: golangci/golangci-lint-action@v9', 'run: echo no-lint')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(no_lint)

        # 5. Marker present but docker build removed must fail
        no_build = valid_deferred.replace('docker build --target credential-controller', 'echo no-build')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(no_build)

        # 6. Marker present but fake coverage upload retained must fail
        fake_upload = valid_deferred + '''      - name: Upload coverage
        uses: actions/upload-artifact@v4
        with:
          name: mosquitto-credential-coverage
          path: /tmp/opencode/task2612-*.cover
'''
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(fake_upload)

        # 7. Native command accidentally retained must fail
        retained_native = valid_deferred + '''      - name: Legacy credential tests
        run: sh scripts/test-stage2-mqtt-credentials.sh
'''
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(retained_native)

        # 8. continue-on-error must fail
        with_continue = valid_deferred.replace('    steps:', '    continue-on-error: true\n    steps:')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(with_continue)

        # 9. if: false must fail
        with_if_false = valid_deferred.replace('      - name: Test credential harness', '      - name: Test credential harness\n        if: false')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(with_if_false)

        # 10. || true must fail
        with_or_true = valid_deferred.replace("test_task2612_standalone.py'", "test_task2612_standalone.py' || true")
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(with_or_true)

        # 11. Comment-only marker must fail
        comment_only = valid_deferred.replace('echo "legacy-execution-deferred: mosquitto-credential-integration" >> "$GITHUB_STEP_SUMMARY"',
                                              '# legacy-execution-deferred: mosquitto-credential-integration')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(comment_only)

        # 12. Misleading display label claiming pass must fail
        misleading_label = valid_deferred.replace('name: Legacy credential checks', 'name: Native credential integration PASS')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(misleading_label)

        # 13. Notice missing required explanation must fail
        incomplete_notice = valid_deferred.replace('echo "Current replacement job: dynsec-native-integration and native-compose-smoke."', '')
        with self.assertRaises(AssertionError):
            assert_credential_job_contract(incomplete_notice)

    def test_entrypoint(self):
        source = (harness.ROOT / 'scripts/test-stage2-mqtt-credentials.sh').read_text()
        self.assertIn('stage2_mqtt_credentials.py', source)
        self.assertNotIn('source ', source)
        self.assertNotIn('.env', source)

    def test_common_fixture_retains_faults_cleanup_and_source_build(self):
        source = (harness.ROOT / 'scripts/tests/task266_provision.py').read_text()
        for contract in ('test', '-coverpkg=', 'startup-unreadable',
                         'chmod 000 /security/dynsec.json; chmod 500 /security',
                         "['docker', 'rm', '-f', '-v', owned]",
                          "require(absent, 'owned container cleanup failed')", 'verify_selector(output)',
                         'generated Gateway secret log leak', "'--- PASS: Test'"):
            self.assertIn(contract, source)
        self.assertNotIn("ROOT / '.env'", source)

    def test_pinned_proxy_and_no_bypass(self):
        self.assertIn('@sha256:', harness.NGINX)
        source = (harness.ROOT / 'scripts/tests/stage2_mqtt_credentials.py').read_text()
        self.assertIn("{'8883/tcp', '8884/tcp'}", source)
        self.assertIn("listener 18884 127.0.0.1", source)
        self.assertNotIn('privileged', source)

    def test_standalone_lane_uses_executable_not_test_composition(self):
        source = (harness.ROOT / 'scripts/tests/task2612_standalone.py').read_text()
        for contract in ("'./cmd/server'", "'--entrypoint', '/server'", "'SIGKILL'", "'SIGTERM'",
                         'chmod 500 /security', 'fixture_standalone_reject',
                         'disabled routes without broker', 'caller discard is not transport loss'):
            self.assertIn(contract, source)
        self.assertNotIn('composeCredentials', source)
        self.assertNotIn('client-tests', source)
        self.assertNotIn('InsecureSkipVerify', source)
        self.assertIn("connection.request('GET', '/readyz')", source)
        self.assertIn('stderr=subprocess.STDOUT', source)
        self.assertIn('standalone graceful exit status', source)
        self.assertIn('standalone database secret exclusion', source)
        self.assertIn("if not k.startswith('MQTT_')", source)
        self.assertIn('checker error has no credential mutation', source)
        self.assertIn('valid=1s', (harness.ROOT / 'scripts/tests/stage2_mqtt_credentials.py').read_text())
        self.assertIn('fixture(standalone=True)', (harness.ROOT / 'scripts/tests/stage2_mqtt_credentials.py').read_text())


if __name__ == '__main__':
    unittest.main()
