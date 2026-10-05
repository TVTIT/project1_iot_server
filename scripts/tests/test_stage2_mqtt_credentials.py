"""Task 2.6.12 fixture guards; never run side effects here."""
import unittest

import stage2_mqtt_credentials as harness


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
        self.assertIn('sh scripts/test-stage2-mqtt-credentials.sh', workflow)

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
                         "require(check.returncode != 0", 'verify_selector(output)',
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
