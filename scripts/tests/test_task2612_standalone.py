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


if __name__ == '__main__':
    unittest.main()
