"""Deterministic startup deadline and secret-safe diagnosis regressions."""
import unittest
from unittest.mock import patch, MagicMock

import task2612_standalone as harness


class ReadinessTests(unittest.TestCase):
    def test_http_probe_uses_remaining_budget_and_never_reads_body(self):
        connection = MagicMock()
        connection.getresponse.return_value.status = 200
        with patch.object(harness.http.client, 'HTTPSConnection', return_value=connection) as factory, \
                patch.object(harness.time, 'monotonic', side_effect=[0, .4, .8]):
            self.assertEqual(harness.readiness_status(8443, 'tls', 1), 200)
        factory.assert_called_once_with('localhost', 8443, context='tls', timeout=1)
        self.assertEqual([call.args[0] for call in connection.sock.settimeout.call_args_list],
                         [.6, .19999999999999996])
        connection.getresponse.return_value.read.assert_not_called()
        connection.close.assert_called_once()

    def test_probe_error_closes_connection(self):
        connection = MagicMock()
        connection.connect.side_effect = TimeoutError('withheld')
        with patch.object(harness.http.client, 'HTTPSConnection', return_value=connection):
            with self.assertRaises(TimeoutError):
                harness.readiness_status(8443, None, .1)
        connection.close.assert_called_once()

    def test_deadline_caps_each_probe_and_sleep(self):
        now = [0.0]
        bounds = []

        def probe(timeout):
            bounds.append(timeout)
            now[0] += timeout
            raise TimeoutError('secret network message')

        with patch.object(harness.time, 'monotonic', side_effect=lambda: now[0]), \
                patch.object(harness.time, 'sleep', side_effect=lambda n: now.__setitem__(0, now[0] + n)):
            ready, last = harness.wait_ready(probe, startup_timeout=3)
        self.assertFalse(ready)
        self.assertEqual(last, 'network:TimeoutError')
        self.assertEqual(bounds, [2, .7999999999999998])
        self.assertEqual(now[0], 3)

    def test_status_not_response_body_is_readiness_evidence(self):
        results = iter([502, 503, 200])
        with patch.object(harness.time, 'sleep'):
            self.assertEqual(harness.wait_ready(lambda timeout: next(results)), (True, 'http:200'))

    def test_unready_startup_requires_503_not_proxy_failure_or_success(self):
        results = iter([502, 200, 503])
        with patch.object(harness.time, 'sleep'):
            self.assertEqual(harness.wait_ready(lambda timeout: next(results), expected_status=503),
                             (True, 'http:503'))

    def test_diagnosis_separates_proxy_direct_and_container_state(self):
        def docker(args, **kwargs):
            command = args[1]
            if command == 'run':
                return harness.subprocess.CompletedProcess(args, 0, b'  HTTP/1.1 200 OK\n', b'')
            if command == 'inspect':
                return harness.subprocess.CompletedProcess(args, 0, b'{"Status":"running","Running":true,"ExitCode":0}', b'')
            return harness.subprocess.CompletedProcess(args, 0,
                b'password-value postgres://hidden Bearer token-value token=unknown-secret\n'
                b'-----BEGIN RSA PRIVATE KEY-----\nprivate-material\n-----END RSA PRIVATE KEY-----', b'')

        with patch.object(harness.subprocess, 'run', side_effect=docker):
            report = harness.startup_diagnosis('backend', 'proxy', 'network', 'http:502',
                                               ['password-value', 'token-value'])
        for evidence in ('proxy=http:502', 'direct=http:200', 'running', 'backend logs', 'proxy logs'):
            self.assertIn(evidence, report)
        for secret in ('password-value', 'token-value', 'postgres://hidden', 'unknown-secret', 'private-material'):
            self.assertNotIn(secret, report)

    def test_diagnostic_commands_bounded_and_cleanup_failure_is_fatal(self):
        calls = []

        def docker(args, **kwargs):
            calls.append((args, kwargs))
            if args[1] == 'run':
                raise harness.subprocess.TimeoutExpired(args, 5, output=b'secret response')
            return harness.subprocess.CompletedProcess(args, 1, b'withheld cleanup error')

        with patch.object(harness.subprocess, 'run', side_effect=docker):
            with self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
                harness.startup_diagnosis('backend', 'proxy', 'network', 'network:TimeoutError', [])
        self.assertTrue(all(kwargs['timeout'] == 5 for _, kwargs in calls))
        self.assertIn(harness.IMAGE, calls[0][0])
        self.assertNotIn('-p', calls[0][0])
        self.assertNotIn('--env-file', calls[0][0])
        self.assertEqual(calls[1][0], ['docker', 'rm', '-f', 'backend-readiness'])


class StartupEnvironmentTests(unittest.TestCase):
    def test_environment_contains_required_qualification_and_store_contract(self):
        spike = MagicMock()
        spike.passwords = {'admin': 'admin_pass', 'backend': 'backend_pass'}
        payload = {'AppDSN': 'postgres://user:pass@db:5432/app', 'AdminDSN': 'postgres://admin:pass@db:5432/admin'}
        env = harness.build_standalone_environment(payload, 'test_jwt_secret', spike)

        # Qualification inputs required by src/internal/config/credential.go
        self.assertEqual(env['MQTT_CREDENTIAL_API_ENABLED'], 'true')
        self.assertEqual(env['MQTT_DYNSEC_WRITER_UID'], '1883')
        self.assertEqual(env['MQTT_DYNSEC_IMAGE_REFERENCE'], harness.IMAGE)
        self.assertEqual(env['MQTT_DYNSEC_IMAGE_CONFIG_ID'], harness.CONFIG_ID)
        self.assertEqual(env['MQTT_DYNSEC_PLATFORM'], harness.PLATFORM)
        self.assertEqual(env['MQTT_DYNSEC_JSON_PATH'], '/security/dynsec.json')

        # Pinned qualification constants must match verified release identity
        self.assertEqual(env['MQTT_DYNSEC_IMAGE_REFERENCE'],
                         'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9')
        self.assertEqual(env['MQTT_DYNSEC_IMAGE_CONFIG_ID'],
                         'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4')
        self.assertEqual(env['MQTT_DYNSEC_PLATFORM'], 'linux/amd64')

    def test_disabled_environment_omits_all_mqtt_credential_variables(self):
        spike = MagicMock()
        spike.passwords = {'admin': 'admin_pass', 'backend': 'backend_pass'}
        payload = {'AppDSN': 'postgres://user:pass@db:5432/app', 'AdminDSN': 'postgres://admin:pass@db:5432/admin'}
        env = harness.build_standalone_environment(payload, 'test_jwt_secret', spike, enabled=False)

        mqtt_keys = [k for k in env if k.startswith('MQTT_')]
        self.assertEqual(mqtt_keys, [], f"Disabled environment must omit all MQTT_ variables, got {mqtt_keys}")
        self.assertEqual(env['DATABASE_URL'], payload['AppDSN'])
        self.assertEqual(env['SUPABASE_JWT_SECRET'], 'test_jwt_secret')

    def test_current_environment_has_no_controller_dependency(self):
        spike = MagicMock()
        spike.passwords = {'admin': 'admin_pass', 'backend': 'backend_pass'}
        env = harness.build_standalone_environment({'AppDSN': 'fixture'}, 'jwt', spike, current=True)
        self.assertNotIn('MQTT_CONTROLLER_DIR', env)
        self.assertEqual(env['MQTT_INTERNAL_MANAGEMENT_URL'], 'ssl://broker:8883')
        self.assertEqual(env['MQTT_CREDENTIAL_RUNTIME_ENABLED'], 'false')
        self.assertEqual(env['MQTT_CREDENTIAL_API_ENABLED'], 'true')


if __name__ == '__main__':
    unittest.main()
