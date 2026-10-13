#!/usr/bin/env python3
"""Unit tests for task28_disable_client_probe helper semantics and error handling."""
import json
import socket
import subprocess
import unittest
from unittest.mock import Mock, patch
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).parent))

from task28_disable_client_probe import exact_connack_code, NativeTargetConnection, inspect_resource_presence, inspect_owned_resource
import stage2_mosquitto_runtime as runtime


class TestConsumerFixtureRepair(unittest.TestCase):
    def test_separated_prepare_and_secret_inputs(self):
        from task28_disable_client_probe import DynSecProbeFixture
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            fixture = DynSecProbeFixture(Path(directory), 'test_image', consumer_rehearsal=True)
            def offline(cmd, **kwargs):
                if cmd[0] == 'openssl':
                    for flag in ('-keyout', '-out'):
                        if flag in cmd:
                            Path(cmd[cmd.index(flag) + 1]).write_text('public test placeholder')
                return b''
            with patch('task28_disable_client_probe.runtime.run', side_effect=offline) as run, \
                 patch.object(fixture, '_create_consumer_volumes'), \
                 patch.object(fixture, '_run_setup', side_effect=lambda script, data:
                              offline(['mock', 'setup', script], data=data)) as setup, \
                 patch.object(fixture, '_discover_and_verify'):
                fixture.prepare()
            compose = fixture.compose_file.read_text()
            config = (Path(directory) / 'broker/broker.conf').read_text()
            self.assertIn('plugin_opt_config_file /native-store/dynsec.json', config)
            self.assertIn('native_store:/native-store', compose)
            self.assertIn('public_ca:/public-ca', compose)
            self.assertIn('aliases: [broker]', compose)
            self.assertIn('DNS:broker', (Path(directory) / 'extensions').read_text())
            script = setup.call_args.args[0]
            self.assertIn('chmod 700 /native-store', script)
            self.assertIn('chmod 600 /native-store/dynsec.json', script)
            self.assertIn('chown -R 1883:1883 /native-store', script)
            self.assertIn('cp /fixture/ca.crt /public-ca/ca.crt', script)
            for secret in fixture.passwords.values():
                self.assertNotIn(secret, repr([c.args for c in run.call_args_list]))
                self.assertNotIn(secret, script)
            for role in ('native_store', 'public_ca'):
                fixture.record_verified_receipt('volume', f'{fixture.project}_{role}', role)
            mounts = fixture.consumer_mounts()
            self.assertEqual(len(mounts), 2)
            self.assertTrue(all(m.endswith(':ro') for m in mounts))
            self.assertNotIn('security', repr(mounts))
            self.assertNotIn('server.key', repr(mounts))

    def test_consumer_capture_requires_exact_id_and_labels(self):
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp/opencode'), 'test_image', consumer_rehearsal=True)
        cid = 'a' * 64
        for role in ('backend', 'nginx', 'auth', 'envoy'):
            name = f'{fixture.project}-{role}-1'
            obj = {'Id': cid, 'Name': '/' + name, 'Config': {'Labels': {
                'com.docker.compose.project': fixture.project,
                'com.docker.compose.service': role}}}
            with patch('task28_disable_client_probe.subprocess.run') as run:
                run.return_value = Mock(returncode=0, stdout=json.dumps([obj]).encode())
                self.assertEqual(fixture.capture_consumer(cid, role, name)['status'], 'owned')
                self.assertNotEqual(fixture.capture_consumer(cid[:12], role, name)['status'], 'owned')
                for field, wrong in (('com.docker.compose.project', 'foreign'),
                                     ('com.docker.compose.service', 'foreign')):
                    original = obj['Config']['Labels'][field]
                    obj['Config']['Labels'][field] = wrong
                    run.return_value.stdout = json.dumps([obj]).encode()
                    self.assertNotEqual(fixture.capture_consumer(cid, role, name)['status'], 'owned')
                    obj['Config']['Labels'][field] = original
                obj['Name'] = '/foreign'
                run.return_value.stdout = json.dumps([obj]).encode()
                self.assertNotEqual(fixture.capture_consumer(cid, role, name)['status'], 'owned')
                self.assertNotEqual(fixture.capture_consumer(cid, 'arbitrary', name)['status'], 'owned')

    def test_mounts_without_receipts_are_rejected(self):
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp/opencode'), 'test_image', consumer_rehearsal=True)
        with self.assertRaises(RuntimeError):
            fixture.consumer_mounts()

    def test_partial_volume_creation_is_tracked_and_unknown_vetoes_cleanup(self):
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp/opencode'), 'test_image', consumer_rehearsal=True)
        name = f'{fixture.project}_security'
        with patch('task28_disable_client_probe.inspect_resource_presence') as presence, \
             patch('task28_disable_client_probe.inspect_owned_resource') as owned, \
             patch('task28_disable_client_probe.runtime.run') as run:
            presence.side_effect = [{'status': 'absent'}, {'status': 'unknown'}]
            owned.return_value = {'status': 'owned', 'resource_id': name, 'reason': 'verified'}
            run.return_value = name.encode()
            with self.assertRaises(RuntimeError):
                fixture._create_consumer_volumes()
            self.assertEqual(fixture.owned_volumes, [name])
            self.assertEqual(run.call_count, 1)
        with patch('task28_disable_client_probe.inspect_owned_resource') as owned, \
             patch('task28_disable_client_probe.inspect_resource_presence') as presence, \
             patch('task28_disable_client_probe.subprocess.run') as run:
            owned.return_value = {'status': 'owned', 'resource_id': name, 'reason': 'verified'}
            presence.return_value = {'status': 'unknown'}
            run.return_value = Mock(returncode=0)
            self.assertFalse(fixture.cleanup()['clean'])

    def test_setup_captures_actual_id_before_start_with_no_secret_argv(self):
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp/opencode'), 'test_image', consumer_rehearsal=True)
        cid = 'b' * 64
        with patch('task28_disable_client_probe.runtime.run') as run, \
             patch('task28_disable_client_probe.inspect_owned_resource') as owned, \
             patch.object(fixture, '_discover_and_verify'):
            owned.return_value = {'status': 'owned', 'resource_id': cid}
            def invoke(cmd, **kwargs):
                if cmd[1] == 'start':
                    self.assertIn(cid, fixture.owned_containers)
                    self.assertEqual(kwargs['data'], b'private test input')
                return cid.encode()
            run.side_effect = invoke
            fixture._run_setup('cat >/native-store/dynsec.json', b'private test input')
            self.assertNotIn('private test input', repr([c.args for c in run.call_args_list]))

    def test_backend_store_replacement_uses_separated_path(self):
        from task28_disable_client_probe import DynSecProbeFixture
        import base64
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            fixture = DynSecProbeFixture(Path(directory), 'test_image',
                                        initialize_backend=True, consumer_rehearsal=True)
            for name, username in (('admin', 'admin'), ('backend', 'backend_service')):
                source = {'clients': [{'username': username,
                                       'password': base64.b64encode(b'x' * 64).decode(),
                                       'salt': base64.b64encode(b'x' * 12).decode(),
                                       'iterations': 101}], 'roles': []}
                (Path(directory) / f'{name}-init.json').write_text(json.dumps(source))
            with patch.object(fixture, '_run_setup') as setup:
                fixture._install_backend_store(Path(directory))
            self.assertIn('cat > /native-store/dynsec.json', setup.call_args.args[0])
            store = json.loads(setup.call_args.args[1])
            self.assertEqual(store['clients'][1]['username'], 'backend_service')
            self.assertEqual(len(store['roles'][0]['acls']), 4)


class TestProbeSemantics(unittest.TestCase):
    def test_exact_connack_code_success(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02\x00\x00']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 0)

    def test_exact_connack_code_unauthorized(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02\x00\x05']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 5)

    def test_exact_connack_code_chunks(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02', b'\x00\x04']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 4)

    def test_exact_connack_code_premature_eof(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02', b'']
        with self.assertRaises(EOFError):
            exact_connack_code(mock_sock)

    def test_exact_connack_code_invalid_header(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x30\x02\x00\x00']
        with self.assertRaises(RuntimeError):
            exact_connack_code(mock_sock)

    def test_wait_for_disconnect_eof(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.return_value = b''
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=1.0)
        self.assertTrue(disc)
        self.assertIn('EOF', detail)

    def test_wait_for_disconnect_reset(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.side_effect = ConnectionResetError('Connection reset by peer')
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=1.0)
        self.assertTrue(disc)
        self.assertIn('transport reset', detail)

    def test_wait_for_disconnect_timeout(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.side_effect = socket.timeout()
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=0.1)
        self.assertFalse(disc)
        self.assertIn('timeout', detail)


class TestInspectResourcePresence(unittest.TestCase):
    @patch('task28_disable_client_probe.subprocess.run')
    def test_inspect_rc0_exists_no_daemon_health_probe(self, mock_run):
        mock_run.return_value = Mock(returncode=0, stdout=b'[{}]', stderr=b'')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res, {'status': 'exists', 'kind': 'container', 'resource_id': 'test-c1'})
        self.assertEqual(mock_run.call_count, 1)
        args, kwargs = mock_run.call_args
        self.assertEqual(args[0], ['docker', 'inspect', '--type=container', 'test-c1'])
        from stage2_mosquitto_runtime import ENV
        self.assertEqual(kwargs['env'], ENV)
        self.assertAlmostEqual(kwargs['timeout'], 5.0, places=2)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_exact_not_found_with_healthy_daemon_is_absent(self, mock_run):
        # 1. inspect call -> rc=1, exact not-found stderr
        # 2. docker info call -> rc=0, valid daemon ID
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n'),
            Mock(returncode=0, stdout=b'TEST-DAEMON-ID-123\n', stderr=b'')
        ]
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res, {'status': 'absent', 'kind': 'container', 'resource_id': 'test-c1'})
        self.assertEqual(mock_run.call_count, 2)
        info_args, info_kwargs = mock_run.call_args_list[1]
        self.assertEqual(info_args[0], ['docker', 'info', '--format', '{{.ID}}'])
        from stage2_mosquitto_runtime import ENV
        self.assertEqual(info_kwargs['env'], ENV)
        self.assertTrue(info_kwargs['timeout'] > 0)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_exact_not_found_volume_and_network_absent(self, mock_run):
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such volume: test-vol1\n'),
            Mock(returncode=0, stdout=b'DAEMON-ID\n', stderr=b''),
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such network: test-net1\n'),
            Mock(returncode=0, stdout=b'DAEMON-ID\n', stderr=b''),
        ]
        res_vol = inspect_resource_presence('volume', 'test-vol1', timeout_sec=5.0)
        self.assertEqual(res_vol['status'], 'absent')
        res_net = inspect_resource_presence('network', 'test-net1', timeout_sec=5.0)
        self.assertEqual(res_net['status'], 'absent')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_different_id_or_unanchored_or_mixed_diagnostics_unknown(self, mock_run):
        # Different ID in error message
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: other-c1\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

        # Generic 404 substring without exact anchor
        mock_run.reset_mock()
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'404 page not found\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

        # Mixed diagnostics (e.g., daemon socket warning plus not found)
        mock_run.reset_mock()
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'warning: connection reset\nError: No such container: test-c1\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_docker29_daemon_error_responses_with_healthy_daemon_are_absent(self, mock_run):
        # Container
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: No such container: test-c1\n'),
            Mock(returncode=0, stdout=b'TEST-DAEMON-ID-123\n', stderr=b'')
        ]
        res_c = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res_c, {'status': 'absent', 'kind': 'container', 'resource_id': 'test-c1'})

        # Volume
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: get test-vol1: no such volume\n'),
            Mock(returncode=0, stdout=b'TEST-DAEMON-ID-123\n', stderr=b'')
        ]
        res_v = inspect_resource_presence('volume', 'test-vol1', timeout_sec=5.0)
        self.assertEqual(res_v, {'status': 'absent', 'kind': 'volume', 'resource_id': 'test-vol1'})

        # Network
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: network test-net1 not found\n'),
            Mock(returncode=0, stdout=b'TEST-DAEMON-ID-123\n', stderr=b'')
        ]
        res_n = inspect_resource_presence('network', 'test-net1', timeout_sec=5.0)
        self.assertEqual(res_n, {'status': 'absent', 'kind': 'network', 'resource_id': 'test-net1'})

    @patch('task28_disable_client_probe.subprocess.run')
    def test_docker29_wrong_id_or_mixed_diagnostics_unknown(self, mock_run):
        # Wrong container ID
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: No such container: other-c1\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

        # Wrong volume ID
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: get other-vol: no such volume\n')
        res = inspect_resource_presence('volume', 'test-vol1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

        # Wrong network ID
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Error response from daemon: network other-net not found\n')
        res = inspect_resource_presence('network', 'test-net1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_daemon_or_permission_failure_is_unknown(self, mock_run):
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Cannot connect to the Docker daemon at unix:///var/run/docker.sock\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

        mock_run.reset_mock()
        mock_run.return_value = Mock(returncode=126, stdout=b'', stderr=b'permission denied\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_inspect_timeout_or_oserror_is_unknown(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=['docker', 'inspect'], timeout=2.0)
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=2.0)
        self.assertEqual(res['status'], 'unknown')

        mock_run.side_effect = OSError('execvp failed')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=2.0)
        self.assertEqual(res['status'], 'unknown')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_candidate_not_found_plus_daemon_health_failure_is_unknown(self, mock_run):
        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n'),
            Mock(returncode=1, stdout=b'', stderr=b'daemon down\n')
        ]
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n'),
            subprocess.TimeoutExpired(cmd=['docker', 'info'], timeout=2.0)
        ]
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n'),
            Mock(returncode=0, stdout=b'  \n', stderr=b'')
        ]
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

        mock_run.side_effect = [
            Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n'),
            Mock(returncode=0, stdout=b'line1\nline2\n', stderr=b'')
        ]
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_inspect_exit_code_must_be_exactly_one_for_absence_candidate(self, mock_run):
        non_one_codes = [126, 127, 2, -9, 255]
        for code in non_one_codes:
            mock_run.reset_mock()
            mock_run.return_value = Mock(returncode=code, stdout=b'', stderr=b'Error: No such container: test-c1\n')
            res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
            self.assertEqual(res['status'], 'unknown')
            self.assertEqual(mock_run.call_count, 1)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_resource_id_syntax_validation_and_no_repr_leakage(self, mock_run):
        class ExplodingStr:
            def __str__(self):
                raise RuntimeError('attacker repr explosion')

        invalid_ids = [
            '-leading-hyphen',
            '--option-shaped',
            'has whitespace',
            'has\nnewline',
            'has\x00null',
            'a' * 256,
            '',
            '   ',
            None,
            12345,
            ExplodingStr(),
        ]
        for bad_id in invalid_ids:
            mock_run.reset_mock()
            res = inspect_resource_presence('container', bad_id, timeout_sec=5.0)
            self.assertEqual(res['status'], 'unknown')
            self.assertEqual(res['kind'], 'container')
            if not isinstance(bad_id, str) or bad_id != 'test-c1':
                self.assertEqual(res['resource_id'], '')
            self.assertEqual(mock_run.call_count, 0)

        invalid_kinds = [
            'invalid_kind',
            ExplodingStr(),
            None,
            123,
            '-container',
        ]
        for bad_kind in invalid_kinds:
            mock_run.reset_mock()
            res = inspect_resource_presence(bad_kind, 'valid-id-123', timeout_sec=5.0)
            self.assertEqual(res['status'], 'unknown')
            self.assertEqual(res['kind'], '')
            self.assertEqual(mock_run.call_count, 0)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_valid_docker_id_and_name_syntax_accepted(self, mock_run):
        valid_ids = [
            'a',
            '0',
            '7f2c45f70a764e288a377793d7d85b04',
            'my-container.name_1',
            'a' * 255,
            'volume_123.backup-test',
        ]
        for vid in valid_ids:
            mock_run.reset_mock()
            mock_run.return_value = Mock(returncode=0, stdout=b'[{}]', stderr=b'')
            res = inspect_resource_presence('container', vid, timeout_sec=5.0)
            self.assertEqual(res['status'], 'exists')
            self.assertEqual(res['kind'], 'container')
            self.assertEqual(res['resource_id'], vid)
            self.assertEqual(mock_run.call_count, 1)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_invalid_inputs_or_budgets_zero_subprocess(self, mock_run):
        invalid_cases = [
            ('invalid_kind', 'id1', 5.0),
            ('container', '', 5.0),
            ('container', '   ', 5.0),
            ('container', 'id1', 0),
            ('container', 'id1', -1.0),
            ('container', 'id1', float('inf')),
            ('container', 'id1', float('nan')),
            ('container', 'id1', 'not_a_number'),
            (None, 'id1', 5.0),
        ]
        for kind, rid, timeout in invalid_cases:
            res = inspect_resource_presence(kind, rid, timeout_sec=timeout)
            self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 0)

    @patch('task28_disable_client_probe.time.monotonic')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_total_deadline_respected_and_exhausted_budget_skips_health(self, mock_run, mock_time):
        mock_time.side_effect = [100.0, 100.0, 105.0]
        mock_run.return_value = Mock(returncode=1, stdout=b'', stderr=b'Error: No such container: test-c1\n')
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=5.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(mock_run.call_count, 1)

    def test_sanitized_status_no_raw_strings(self):
        res = inspect_resource_presence('container', 'test-c1', timeout_sec=-1)
        self.assertEqual(set(res.keys()), {'status', 'kind', 'resource_id'})
        self.assertIn(res['status'], {'exists', 'absent', 'unknown'})


class TestInspectOwnedResource(unittest.TestCase):
    @patch('task28_disable_client_probe.subprocess.run')
    def test_broker_container_owned_success(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/dynsec_probe_1234567890ab-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'dynsec_probe_1234567890ab',
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'dynsec_probe_1234567890ab', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res, {
            'kind': 'container',
            'resource_id': cid,
            'status': 'owned',
            'reason': 'verified'
        })
        self.assertEqual(mock_run.call_count, 1)
        args, kwargs = mock_run.call_args
        self.assertEqual(args[0], ['docker', 'inspect', '--type=container', cid])
        from stage2_mosquitto_runtime import ENV
        self.assertEqual(kwargs['env'], ENV)
        self.assertAlmostEqual(kwargs['timeout'], 5.0, places=2)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_setup_container_owned_success_explicit_name_only(self, mock_run):
        cid = 'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789'
        payload = [{
            'Id': cid,
            'Name': '/dynsec_probe_proj-setup',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'dynsec_probe_proj',
                    'com.docker.compose.service': 'setup',
                    'com.docker.compose.oneoff': 'True',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res_default = inspect_owned_resource(
            'container', cid, 'dynsec_probe_proj', expected_role='setup', timeout_sec=5.0
        )
        self.assertEqual(res_default['status'], 'not_owned')
        self.assertEqual(res_default['reason'], 'name_mismatch')

        res_explicit = inspect_owned_resource(
            'container', cid, 'dynsec_probe_proj', expected_role='setup',
            expected_name='dynsec_probe_proj-setup', timeout_sec=5.0
        )
        self.assertEqual(res_explicit, {
            'kind': 'container',
            'resource_id': cid,
            'status': 'owned',
            'reason': 'verified'
        })

    @patch('task28_disable_client_probe.subprocess.run')
    def test_network_owned_success(self, mock_run):
        nid = '11223344556677889900aabbccddeeff0011223344556677889900aabbccddee'
        payload = [{
            'Id': nid,
            'Name': 'dynsec_probe_proj_default',
            'Labels': {
                'com.docker.compose.project': 'dynsec_probe_proj',
                'com.docker.compose.network': 'default',
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'network', nid, 'dynsec_probe_proj', expected_role='default', timeout_sec=5.0
        )
        self.assertEqual(res, {
            'kind': 'network',
            'resource_id': nid,
            'status': 'owned',
            'reason': 'verified'
        })

    @patch('task28_disable_client_probe.subprocess.run')
    def test_volume_owned_success(self, mock_run):
        vname = 'dynsec_probe_proj_security'
        payload = [{
            'Name': vname,
            'Labels': {
                'com.docker.compose.project': 'dynsec_probe_proj',
                'com.docker.compose.volume': 'security',
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'volume', vname, 'dynsec_probe_proj', expected_role='security', timeout_sec=5.0
        )
        self.assertEqual(res, {
            'kind': 'volume',
            'resource_id': vname,
            'status': 'owned',
            'reason': 'verified'
        })

    @patch('task28_disable_client_probe.subprocess.run')
    def test_foreign_project_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/foreign_project-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'foreign_project',
                    'com.docker.compose.service': 'broker',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'label_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_wrong_role_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/my_project-setup-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'my_project',
                    'com.docker.compose.service': 'setup',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'role_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_wrong_name_or_arbitrary_same_project_container_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/my_project-attacker-evil',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'my_project',
                    'com.docker.compose.service': 'broker',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'name_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_oneoff_semantics_mismatch_and_type_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/my_project-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'my_project',
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'True',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'oneoff_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_mismatched_full_id_or_candidate_id_rejected(self, mock_run):
        cid1 = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        cid2 = 'fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210'
        payload = [{
            'Id': cid2,
            'Name': '/my_project-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'my_project',
                    'com.docker.compose.service': 'broker',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid1, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'id_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_non_full_64hex_id_rejected_for_container_and_network(self, mock_run):
        payload = [{
            'Id': '0123456789ab',
            'Name': '/my_project-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'my_project',
                    'com.docker.compose.service': 'broker',
                }
            }
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', '0123456789ab', 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'schema_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_absent_labels_or_schema_field_missing_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        payload = [{
            'Id': cid,
            'Name': '/my_project-broker-1',
            'Config': {}
        }]
        mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
        res = inspect_owned_resource(
            'container', cid, 'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res['status'], 'not_owned')
        self.assertEqual(res['reason'], 'schema_mismatch')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_malformed_or_duplicate_json_keys_rejected(self, mock_run):
        dup_json = b'[{"Id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "Id": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"}]'
        mock_run.return_value = Mock(returncode=0, stdout=dup_json, stderr=b'')
        res_dup = inspect_owned_resource(
            'container', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            'my_project', expected_role='broker', timeout_sec=5.0
        )
        self.assertEqual(res_dup['status'], 'unknown')
        self.assertEqual(res_dup['reason'], 'duplicate_json_keys')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_timeout_or_oserror_returns_unknown_no_leakage(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=['docker', 'inspect'], timeout=2.0)
        res_to = inspect_owned_resource(
            'container', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            'my_project', expected_role='broker', timeout_sec=2.0
        )
        self.assertEqual(res_to['status'], 'unknown')
        self.assertEqual(res_to['reason'], 'timeout')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_invalid_arguments_zero_subprocess(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        res = inspect_owned_resource('container', cid, 'my_proj', 'broker', -1.0)
        self.assertEqual(res['status'], 'unknown')
        self.assertEqual(res['reason'], 'invalid_parameters')
        self.assertEqual(mock_run.call_count, 0)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_auth_and_envoy_container_roles_supported(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        for role in ('auth', 'envoy'):
            name = f'my_project-{role}-1'
            payload = [{
                'Id': cid,
                'Name': '/' + name,
                'Config': {
                    'Labels': {
                        'com.docker.compose.project': 'my_project',
                        'com.docker.compose.service': role,
                        'com.docker.compose.oneoff': 'False',
                    }
                }
            }]
            mock_run.return_value = Mock(returncode=0, stdout=json.dumps(payload).encode(), stderr=b'')
            res = inspect_owned_resource(
                'container', cid, 'my_project', expected_role=role, expected_name=name, timeout_sec=5.0
            )
            self.assertEqual(res, {
                'kind': 'container',
                'resource_id': cid,
                'status': 'owned',
                'reason': 'verified'
            })

            # Missing expected_name is rejected
            res_no_name = inspect_owned_resource(
                'container', cid, 'my_project', expected_role=role, expected_name=None, timeout_sec=5.0
            )
            self.assertEqual(res_no_name['status'], 'unknown')
            self.assertEqual(res_no_name['reason'], 'invalid_parameters')

            # Short ID is rejected
            res_short = inspect_owned_resource(
                'container', cid[:12], 'my_project', expected_role=role, expected_name=name, timeout_sec=5.0
            )
            self.assertEqual(res_short['status'], 'unknown')
            self.assertEqual(res_short['reason'], 'invalid_parameters')

    @patch('task28_disable_client_probe.subprocess.run')
    def test_unknown_role_remains_rejected(self, mock_run):
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        for bad_role in ('arbitrary', 'postgres', 'storage', 'admin', 'root'):
            res = inspect_owned_resource(
                'container', cid, 'my_project', expected_role=bad_role, expected_name=f'my_project-{bad_role}-1'
            )
            self.assertEqual(res['status'], 'unknown')
            self.assertEqual(res['reason'], 'invalid_parameters')
        self.assertEqual(mock_run.call_count, 0)


class TestC1cExactTeardownRed(unittest.TestCase):
    """C1c tests: exact-owned-resource teardown, receipts, finite shared deadline, no broad fallback."""

    def test_no_compose_down_or_prune_or_broad_cleanup_in_script(self):
        """1. Verify scripts/tests/task28_disable_client_probe.py contains no 'down', '-v', 'prune'."""
        with open(Path(__file__).parent / 'task28_disable_client_probe.py', 'r', encoding='utf-8') as f:
            src = f.read()
        self.assertNotIn("'down'", src)
        self.assertNotIn('"down"', src)
        self.assertNotIn("'prune'", src)
        self.assertNotIn('"prune"', src)

    def test_list_append_alone_cannot_authorize_deletion(self):
        """2. Merely appending to owned_* without verified receipts cannot authorize delete."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        fixture.owned_containers = ['fake_candidate_cid']
        fixture.verified_receipts = []

        with patch('task28_disable_client_probe.subprocess.run') as mock_run:
            receipt = fixture.cleanup()
            self.assertEqual(mock_run.call_count, 0)
            self.assertFalse(receipt['clean'])
            self.assertIn({'kind': 'container', 'resource_id': 'fake_candidate_cid', 'reason': 'no_capture_receipt'},
                          receipt['cleanup_unknown'])

    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_exact_owned_receipt_predelete_verification_and_exact_id_delete(self, mock_subproc, mock_inspect, mock_presence):
        """Exact owned receipt -> pre-delete identity/labels verification then only exact-ID delete, explicit env/timeouts."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        vid = f'{fixture.project}_security'
        nid = '11223344556677889900aabbccddeeff0011223344556677889900aabbccddee'

        fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')
        fixture.record_verified_receipt('volume', vid, 'security', vid)
        fixture.record_verified_receipt('network', nid, 'default', f'{fixture.project}_default')

        mock_inspect.side_effect = lambda **kwargs: {'status': 'owned', 'resource_id': kwargs.get('candidate_id'), 'reason': 'verified'}
        mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
        mock_presence.return_value = {'status': 'absent', 'kind': 'any', 'resource_id': 'any'}

        receipt = fixture.cleanup(cleanup_deadline_sec=20.0)
        self.assertTrue(receipt['clean'])

        # Two inspect_owned_resource calls for container (before stop and before rm) + network + volume = 4 calls
        container_inspect_calls = [c for c in mock_inspect.call_args_list if c[1].get('kind') == 'container']
        self.assertEqual(len(container_inspect_calls), 2)
        for c in container_inspect_calls:
            self.assertEqual(c[1]['candidate_id'], cid)
            self.assertEqual(c[1]['expected_project'], fixture.project)
            self.assertEqual(c[1]['expected_role'], 'broker')
            self.assertTrue(0 < c[1]['timeout_sec'] <= 5.0)

        calls = [c[0][0] for c in mock_subproc.call_args_list]
        self.assertIn(['docker', 'stop', '-t', '2', cid], calls)
        self.assertIn(['docker', 'rm', '-f', cid], calls)
        self.assertIn(['docker', 'network', 'rm', nid], calls)
        self.assertIn(['docker', 'volume', 'rm', '-f', vid], calls)

        for c in mock_subproc.call_args_list:
            self.assertEqual(c[1]['env'], fixture.env)
            self.assertTrue(c[1]['timeout'] > 0 and c[1]['timeout'] <= 5.0)

    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_consumer_cleanup_removes_auth_and_envoy_before_shared_resources(self, mock_subproc, mock_inspect, mock_presence):
        """Consumers (auth, envoy, backend, nginx) are stopped/removed before networks and volumes."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image', consumer_rehearsal=True)

        auth_cid = '1' * 64
        envoy_cid = '2' * 64
        backend_cid = '3' * 64
        nginx_cid = '4' * 64
        vid = f'{fixture.project}_security'
        nid = '5' * 64

        fixture.record_verified_receipt('container', auth_cid, 'auth', f'{fixture.project}-auth-1')
        fixture.record_verified_receipt('container', envoy_cid, 'envoy', f'{fixture.project}-envoy-1')
        fixture.record_verified_receipt('container', backend_cid, 'backend', f'{fixture.project}-backend-1')
        fixture.record_verified_receipt('container', nginx_cid, 'nginx', f'{fixture.project}-nginx-1')
        fixture.record_verified_receipt('volume', vid, 'security', vid)
        fixture.record_verified_receipt('network', nid, 'default', f'{fixture.project}_default')

        mock_inspect.side_effect = lambda **kwargs: {'status': 'owned', 'resource_id': kwargs.get('candidate_id'), 'reason': 'verified'}
        mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
        mock_presence.return_value = {'status': 'absent', 'kind': 'any', 'resource_id': 'any'}

        receipt = fixture.cleanup(cleanup_deadline_sec=30.0)
        self.assertTrue(receipt['clean'])

        calls = [c[0][0] for c in mock_subproc.call_args_list]

        # Verify all 4 containers stopped and removed
        for cid in (auth_cid, envoy_cid, backend_cid, nginx_cid):
            self.assertIn(['docker', 'stop', '-t', '2', cid], calls)
            self.assertIn(['docker', 'rm', '-f', cid], calls)

        # Container removals must occur before network and volume removals
        last_container_rm_idx = max(
            calls.index(['docker', 'rm', '-f', cid])
            for cid in (auth_cid, envoy_cid, backend_cid, nginx_cid)
        )
        net_rm_idx = calls.index(['docker', 'network', 'rm', nid])
        vol_rm_idx = calls.index(['docker', 'volume', 'rm', '-f', vid])

        self.assertLess(last_container_rm_idx, net_rm_idx)
        self.assertLess(last_container_rm_idx, vol_rm_idx)

    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_cleanup_uncertainty_never_becomes_verified_absence(self, mock_subproc, mock_inspect, mock_presence):
        """Cleanup uncertainty (e.g. timeout during container rm or presence check unknown) never becomes clean=True."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image', consumer_rehearsal=True)

        auth_cid = 'a' * 64
        fixture.record_verified_receipt('container', auth_cid, 'auth', f'{fixture.project}-auth-1')

        mock_inspect.return_value = {'status': 'owned', 'resource_id': auth_cid, 'reason': 'verified'}
        mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
        # Presence check returns unknown
        mock_presence.return_value = {'status': 'unknown', 'kind': 'container', 'resource_id': auth_cid}

        receipt = fixture.cleanup(cleanup_deadline_sec=20.0)
        self.assertFalse(receipt['clean'])
        self.assertIn({'kind': 'container', 'resource_id': auth_cid, 'reason': 'presence_unknown'},
                      receipt['cleanup_unknown'])

    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_foreign_or_replaced_identity_zero_destructive_operations(self, mock_subproc, mock_inspect):
        """Foreign/replaced identity/project/role/name -> ZERO destructive operations for that resource, clean=False."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')

        mock_inspect.return_value = {'status': 'not_owned', 'resource_id': cid, 'reason': 'role_mismatch'}

        receipt = fixture.cleanup()
        self.assertFalse(receipt['clean'])
        self.assertEqual(mock_subproc.call_count, 0)
        self.assertIn({'kind': 'container', 'resource_id': cid, 'reason': 'role_mismatch'},
                      receipt['cleanup_unknown'])

    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_stop_rm_timeout_or_error_is_unresolved(self, mock_subproc, mock_inspect, mock_presence):
        """Stop/rm/inspect timeout/error, remaining resource -> unresolved, no false clean."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')

        mock_inspect.return_value = {'status': 'owned', 'resource_id': cid, 'reason': 'verified'}
        mock_subproc.side_effect = subprocess.TimeoutExpired(cmd=['docker', 'stop'], timeout=2.0)

        receipt = fixture.cleanup()
        self.assertFalse(receipt['clean'])
        self.assertIn({'kind': 'container', 'resource_id': cid, 'reason': 'stop_timeout'},
                      receipt['cleanup_unknown'])

    @patch('task28_disable_client_probe.time.monotonic')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_expired_total_budget_exhaustion_skips_subprocess_and_clean_false(self, mock_subproc, mock_inspect, mock_time):
        """Expired total budget -> safe unknown and clean=False, no further subprocess."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')

        mock_time.side_effect = [100.0, 150.0, 150.0, 150.0]

        receipt = fixture.cleanup(cleanup_deadline_sec=10.0)
        self.assertFalse(receipt['clean'])
        self.assertEqual(mock_inspect.call_count, 0)
        self.assertEqual(mock_subproc.call_count, 0)
        self.assertIn({'kind': 'container', 'resource_id': cid, 'reason': 'deadline_exhausted'},
                      receipt['cleanup_unknown'])

    @patch('task28_disable_client_probe.subprocess.run')
    def test_discovery_service_filter_distinguishes_broker_and_setup(self, mock_subproc):
        """Discovery uses expected service label filter to distinguish broker and surviving setup containers."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')

        fixture._discover_and_verify('container', expected_role='broker', expected_name=f'{fixture.project}-broker-1')
        self.assertEqual(mock_subproc.call_count, 1)
        cmd = mock_subproc.call_args[0][0]
        self.assertIn(f'label=com.docker.compose.project={fixture.project}', cmd)
        self.assertIn('label=com.docker.compose.service=broker', cmd)

    @patch('task28_disable_client_probe.subprocess.run')
    def test_candidate_count_cap_sticky_unknown_on_excess(self, mock_subproc):
        """Excess discovery candidates (> MAX_CANDIDATES e.g. 8) marks unknown without multiplying subprocess calls."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')

        lines = [f'{i:064x}' for i in range(12)]
        mock_subproc.return_value = Mock(returncode=0, stdout=('\n'.join(lines) + '\n').encode(), stderr=b'')

        fixture._discover_and_verify('container', expected_role='broker', expected_name=f'{fixture.project}-broker-1')
        self.assertEqual(len(fixture.owned_containers), 0)
        self.assertIn({'kind': 'container', 'resource_id': '', 'reason': 'candidate_count_exceeded'},
                      fixture.ownership_unknown)
        self.assertEqual(mock_subproc.call_count, 1)

    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_partial_failure_captures_and_cleans_surviving_owned_setup_and_volume(self, mock_subproc, mock_inspect, mock_presence):
        """Partial failure captures and cleans surviving owned setup + volume; mixed setup/broker filters correct."""
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid_setup = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        vid = f'{fixture.project}_security'

        # Surviving setup container and volume recorded during failed/partial prepare
        fixture.record_verified_receipt('container', cid_setup, 'setup', fixture.setup_name)
        fixture.record_verified_receipt('volume', vid, 'security', vid)

        mock_inspect.side_effect = lambda **kwargs: {'status': 'owned', 'resource_id': kwargs.get('candidate_id'), 'reason': 'verified'}
        mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
        mock_presence.return_value = {'status': 'absent', 'kind': 'any', 'resource_id': 'any'}

        receipt = fixture.cleanup(cleanup_deadline_sec=15.0)
        self.assertTrue(receipt['clean'])

        calls = [c[0][0] for c in mock_subproc.call_args_list]
        self.assertIn(['docker', 'stop', '-t', '2', cid_setup], calls)
        self.assertIn(['docker', 'rm', '-f', cid_setup], calls)
        self.assertIn(['docker', 'volume', 'rm', '-f', vid], calls)

    @patch('task28_disable_client_probe.subprocess.run')
    @patch('task28_disable_client_probe.inspect_resource_presence')
    @patch('task28_disable_client_probe.inspect_owned_resource')
    def test_invalid_cleanup_budget_zero_docker_calls_and_clean_false(self, mock_inspect, mock_presence, mock_subproc):
        """Invalid cleanup_deadline_sec (bool, non-number, <=0, nan, inf) -> clean=False, invalid_budget uncertainty, 0 docker/inspect calls."""
        from task28_disable_client_probe import DynSecProbeFixture
        import math

        invalid_budgets = [
            True, False,
            "30", None, [10], {"sec": 5},
            0, 0.0, -1, -0.001, -100.0,
            float('nan'), float('inf'), float('-inf')
        ]

        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        for invalid_budget in invalid_budgets:
            mock_subproc.reset_mock()
            mock_presence.reset_mock()
            mock_inspect.reset_mock()
            mock_inspect.return_value = {'status': 'owned', 'resource_id': cid, 'reason': 'verified'}

            fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
            fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')

            # Create a mock socket to ensure tracked sockets are closed without docker calls
            mock_socket = Mock()
            fixture.open_connections.append(mock_socket)

            receipt = fixture.cleanup(cleanup_deadline_sec=invalid_budget)

            self.assertFalse(receipt['clean'], f"Failed clean=False for budget {invalid_budget!r}")
            self.assertEqual(mock_subproc.call_count, 0, f"Subprocess called for budget {invalid_budget!r}")
            self.assertEqual(mock_inspect.call_count, 0, f"Inspect called for budget {invalid_budget!r}")
            self.assertEqual(mock_presence.call_count, 0, f"Presence inspect called for budget {invalid_budget!r}")
            self.assertEqual(mock_socket.close.call_count, 1)
            self.assertEqual(len(fixture.open_connections), 0)

            # Must record invalid_budget uncertainty
            reasons = [u.get('reason') for u in receipt.get('cleanup_unknown', [])]
            self.assertIn('invalid_budget', reasons, f"invalid_budget reason missing for {invalid_budget!r}")

    @patch('task28_disable_client_probe.inspect_owned_resource')
    @patch('task28_disable_client_probe.subprocess.run')
    def test_reinspect_before_rm_foreign_or_mismatched_id_prevents_rm(self, mock_subproc, mock_inspect):
        """First ownership check owned (stop occurs), second check foreign/unknown/mismatched ID -> stop occurs, rm must not."""
        from task28_disable_client_probe import DynSecProbeFixture
        cid = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'

        failure_scenarios = [
            {'status': 'not_owned', 'resource_id': cid, 'reason': 'role_mismatch'},
            {'status': 'unknown', 'resource_id': cid, 'reason': 'timeout'},
            {'status': 'owned', 'resource_id': 'fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210', 'reason': 'verified'},  # mismatched ID
        ]

        for fail_spec in failure_scenarios:
            mock_subproc.reset_mock()
            mock_inspect.reset_mock()

            fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
            fixture.record_verified_receipt('container', cid, 'broker', f'{fixture.project}-broker-1')

            # First inspect returns owned with exact cid, second inspect returns fail_spec
            mock_inspect.side_effect = [
                {'status': 'owned', 'resource_id': cid, 'reason': 'verified'},
                fail_spec
            ]
            mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')

            receipt = fixture.cleanup(cleanup_deadline_sec=20.0)

            self.assertFalse(receipt['clean'])
            calls = [c[0][0] for c in mock_subproc.call_args_list]
            self.assertIn(['docker', 'stop', '-t', '2', cid], calls)
            for call in calls:
                self.assertNotIn('rm', call)

            # Inspect must have been called twice for the container
            container_inspects = [c for c in mock_inspect.call_args_list if c[1].get('kind') == 'container']
            self.assertEqual(len(container_inspects), 2)

            expected_reason = fail_spec['reason'] if fail_spec['status'] != 'owned' else 'id_mismatch'
            reasons = [u.get('reason') for u in receipt.get('cleanup_unknown', [])]
            self.assertIn(expected_reason, reasons)


class TestC1CoverageRestoration(unittest.TestCase):
    """C1 coverage restoration: discover_and_verify, lifecycle capture, and cleanup invariants."""

    def test_discover_and_verify_real_inspect_owned_resource_with_mocked_subprocess_json(self):
        """1. _discover_and_verify uses real inspect_owned_resource with mocked subprocess JSON;
        records full container/network IDs or exact volume name and qualified receipt, duplicate candidates deduplicated.
        """
        from task28_disable_client_probe import DynSecProbeFixture
        import json

        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid_full = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
        cid_short = cid_full[:12]
        vol_name = f'{fixture.project}_security'
        net_full = 'aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899'

        container_inspect_json = json.dumps([{
            'Id': cid_full,
            'Name': f'/{fixture.project}-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': fixture.project,
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False'
                }
            }
        }]).encode('utf-8')

        volume_inspect_json = json.dumps([{
            'Name': vol_name,
            'Labels': {
                'com.docker.compose.project': fixture.project,
                'com.docker.compose.volume': 'security'
            }
        }]).encode('utf-8')

        network_inspect_json = json.dumps([{
            'Id': net_full,
            'Name': f'{fixture.project}_default',
            'Labels': {
                'com.docker.compose.project': fixture.project,
                'com.docker.compose.network': 'default'
            }
        }]).encode('utf-8')

        # Test container discovery with duplicate candidate IDs in docker ps output
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:2] == ['docker', 'ps']:
                    # Return duplicates of short candidate id
                    return Mock(returncode=0, stdout=f'{cid_short}\n{cid_short}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    return Mock(returncode=0, stdout=container_inspect_json, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            fixture._discover_and_verify(
                'container',
                expected_role='broker',
                expected_name=f'{fixture.project}-broker-1'
            )

            # Assert duplicate was deduplicated: docker inspect called once
            inspect_calls = [c for c in mock_subproc.call_args_list if c[0][0][0:2] == ['docker', 'inspect']]
            self.assertEqual(len(inspect_calls), 1)
            # Full 64-char ID recorded, not short prefix
            self.assertEqual(fixture.owned_containers, [cid_full])
            self.assertEqual(len(fixture.verified_receipts), 1)
            self.assertEqual(fixture.verified_receipts[0]['resource_id'], cid_full)
            self.assertEqual(fixture.verified_receipts[0]['kind'], 'container')
            self.assertEqual(fixture.verified_receipts[0]['expected_role'], 'broker')

        # Test volume discovery with duplicate volume names
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'volume', 'ls']:
                    return Mock(returncode=0, stdout=f'{vol_name}\n{vol_name}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    return Mock(returncode=0, stdout=volume_inspect_json, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            fixture._discover_and_verify(
                'volume',
                expected_role='security',
                expected_name=vol_name
            )

            inspect_calls = [c for c in mock_subproc.call_args_list if c[0][0][0:2] == ['docker', 'inspect']]
            self.assertEqual(len(inspect_calls), 1)
            self.assertEqual(fixture.owned_volumes, [vol_name])
            vol_receipt = [r for r in fixture.verified_receipts if r['kind'] == 'volume']
            self.assertEqual(len(vol_receipt), 1)
            self.assertEqual(vol_receipt[0]['resource_id'], vol_name)

        # Test network discovery
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'network', 'ls']:
                    return Mock(returncode=0, stdout=f'{net_full[:12]}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    return Mock(returncode=0, stdout=network_inspect_json, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            fixture._discover_and_verify(
                'network',
                expected_role='default',
                expected_name=f'{fixture.project}_default'
            )

            self.assertEqual(fixture.owned_networks, [net_full])
            net_receipt = [r for r in fixture.verified_receipts if r['kind'] == 'network']
            self.assertEqual(len(net_receipt), 1)
            self.assertEqual(net_receipt[0]['resource_id'], net_full)

    def test_wrong_name_foreign_wrong_role_candidate_never_adds_receipt_and_sets_sticky_unknown(self):
        """2. Wrong-name/foreign/wrong-role candidate never adds receipt and sets sticky ownership_unknown;
        subsequent owned candidate cannot clear unknown.
        """
        from task28_disable_client_probe import DynSecProbeFixture
        import json

        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid_foreign = '1111111111111111111111111111111111111111111111111111111111111111'
        cid_valid = '2222222222222222222222222222222222222222222222222222222222222222'

        foreign_inspect_json = json.dumps([{
            'Id': cid_foreign,
            'Name': '/foreign-container',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': 'foreign_project',
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False'
                }
            }
        }]).encode('utf-8')

        valid_inspect_json = json.dumps([{
            'Id': cid_valid,
            'Name': f'/{fixture.project}-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': fixture.project,
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False'
                }
            }
        }]).encode('utf-8')

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:2] == ['docker', 'ps']:
                    # First foreign candidate, then valid candidate
                    return Mock(returncode=0, stdout=f'{cid_foreign}\n{cid_valid}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    if cid_foreign in cmd:
                        return Mock(returncode=0, stdout=foreign_inspect_json, stderr=b'')
                    elif cid_valid in cmd:
                        return Mock(returncode=0, stdout=valid_inspect_json, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            fixture._discover_and_verify(
                'container',
                expected_role='broker',
                expected_name=f'{fixture.project}-broker-1'
            )

            # Foreign candidate never added to owned or receipts
            self.assertNotIn(cid_foreign, fixture.owned_containers)
            receipt_ids = [r['resource_id'] for r in fixture.verified_receipts]
            self.assertNotIn(cid_foreign, receipt_ids)

            # Foreign candidate created sticky ownership_unknown
            foreign_unknown = [u for u in fixture.ownership_unknown if u['resource_id'] == cid_foreign]
            self.assertTrue(len(foreign_unknown) > 0)
            self.assertEqual(foreign_unknown[0]['reason'], 'label_mismatch')

            # Valid candidate was recorded, but could NOT clear ownership_unknown
            self.assertIn(cid_valid, fixture.owned_containers)
            self.assertIn(cid_valid, receipt_ids)
            self.assertTrue(len(fixture.ownership_unknown) > 0)

            # When cleanup runs, clean MUST be False due to sticky ownership_unknown
            cleanup_receipt = fixture.cleanup()
            self.assertFalse(cleanup_receipt['clean'])
            self.assertTrue(len(cleanup_receipt['ownership_unknown']) > 0)

    def test_discovery_timeout_nonzero_malformed_and_shared_deadline_exhaustion_produce_unknown_no_unbounded_calls(self):
        """3. Discovery timeout/nonzero/malformed candidate and shared deadline exhaustion produce unknown,
        no unbounded extra calls.
        """
        from task28_disable_client_probe import DynSecProbeFixture

        # Case A: nonzero returncode from discovery command
        fixture1 = DynSecProbeFixture(Path('/tmp'), 'test_image')
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            mock_subproc.return_value = Mock(returncode=1, stdout=b'', stderr=b'error')
            fixture1._discover_and_verify('container', expected_role='broker')
            self.assertEqual(mock_subproc.call_count, 1)
            reasons = [u['reason'] for u in fixture1.ownership_unknown]
            self.assertIn('discovery_failed', reasons)
            self.assertEqual(len(fixture1.verified_receipts), 0)

        # Case B: timeout during discovery command
        fixture2 = DynSecProbeFixture(Path('/tmp'), 'test_image')
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            mock_subproc.side_effect = subprocess.TimeoutExpired(cmd=['docker', 'ps'], timeout=1.0)
            fixture2._discover_and_verify('container', expected_role='broker')
            self.assertEqual(mock_subproc.call_count, 1)
            reasons = [u['reason'] for u in fixture2.ownership_unknown]
            self.assertIn('discovery_timeout', reasons)

        # Case C: malformed candidate ID
        fixture3 = DynSecProbeFixture(Path('/tmp'), 'test_image')
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            mock_subproc.return_value = Mock(returncode=0, stdout=b'not safe@id!\n', stderr=b'')
            fixture3._discover_and_verify('container', expected_role='broker')
            # Inspect should NOT be called for malformed candidate ID
            self.assertEqual(mock_subproc.call_count, 1)
            reasons = [u['reason'] for u in fixture3.ownership_unknown]
            self.assertIn('malformed_candidate_id', reasons)

        # Case D: shared deadline exhaustion stops inspection calls
        fixture4 = DynSecProbeFixture(Path('/tmp'), 'test_image')
        cid1 = '1111111111111111111111111111111111111111111111111111111111111111'
        cid2 = '2222222222222222222222222222222222222222222222222222222222222222'
        cid3 = '3333333333333333333333333333333333333333333333333333333333333333'
        with patch('task28_disable_client_probe.time.monotonic') as mock_time, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc:
            # Time progression:
            # 1. start deadline = 100 + 5 = 105
            # 2. rem check before ps: 100.1 (rem = 4.9 > 0)
            # 3. candidate 1 rem check: 101.0 (rem = 4.0 > 0)
            # 4. candidate 2 rem check: 106.0 (rem = -1.0 <= 0 -> discovery_timeout)
            # 5. candidate 3 rem check: 106.0 (rem = -1.0 <= 0 -> discovery_timeout)
            mock_time.side_effect = [100.0, 100.1, 101.0, 106.0, 106.0]
            mock_subproc.side_effect = [
                # docker ps returns 3 candidates
                Mock(returncode=0, stdout=f'{cid1}\n{cid2}\n{cid3}\n'.encode('utf-8'), stderr=b''),
                # inspect for cid1 returns inspect failure
                Mock(returncode=1, stdout=b'', stderr=b''),
            ]
            fixture4._discover_and_verify('container', expected_role='broker', discovery_deadline_sec=5.0)

            # Subprocess calls: 1 for ps, 1 for inspect cid1, 0 for cid2 and cid3 (exhausted)
            self.assertEqual(mock_subproc.call_count, 2)
            timeout_entries = [u for u in fixture4.ownership_unknown if u['reason'] == 'discovery_timeout']
            self.assertEqual(len(timeout_entries), 2)
            timeout_cands = {u['resource_id'] for u in timeout_entries}
            self.assertEqual(timeout_cands, {cid2, cid3})

    def test_prepare_setup_allocation_raises_finally_invokes_capture_for_setup_and_volume(self):
        """4. prepare setup allocation raises after creation: actual prepare finally invokes capture
        for setup and volume (patch OpenSSL/runtime.run/file-copy to avoid secrets/native;
        use TemporaryDirectory under /tmp/opencode and harmless fixture file contents).
        At least one path records real qualified capture via mocked inspect JSON. Do not seed receipts beforehand.
        """
        from task28_disable_client_probe import DynSecProbeFixture
        import tempfile
        import json

        base_tmp = Path('/tmp/opencode')
        base_tmp.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=base_tmp) as tmpdir:
            fix_dir = Path(tmpdir)
            fixture = DynSecProbeFixture(fix_dir, 'test_image')
            # Verify no receipts seeded beforehand
            self.assertEqual(len(fixture.verified_receipts), 0)
            self.assertEqual(len(fixture.owned_containers), 0)
            self.assertEqual(len(fixture.owned_volumes), 0)

            # Populate fake COMPOSE_BASE in place
            with patch.object(Path, 'read_text', return_value=(
                'services:\n'
                '  broker:\n'
                '    volumes:\n'
                '      - ${DYNSEC_FIXTURE:?protected fixture required}/broker:/fixture:ro\n'
                '  setup:\n'
                '    image: eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9\n'
            )):
                setup_cid = '3333333333333333333333333333333333333333333333333333333333333333'
                vol_name = f'{fixture.project}_security'

                setup_inspect_json = json.dumps([{
                    'Id': setup_cid,
                    'Name': f'/{fixture.setup_name}',
                    'Config': {
                        'Labels': {
                            'com.docker.compose.project': fixture.project,
                            'com.docker.compose.service': 'setup',
                            'com.docker.compose.oneoff': 'true'
                        }
                    }
                }]).encode('utf-8')

                vol_inspect_json = json.dumps([{
                    'Name': vol_name,
                    'Labels': {
                        'com.docker.compose.project': fixture.project,
                        'com.docker.compose.volume': 'security'
                    }
                }]).encode('utf-8')

                with patch('task28_disable_client_probe.runtime.run') as mock_run, \
                     patch('task28_disable_client_probe.shutil.copyfile'), \
                     patch('task28_disable_client_probe.subprocess.run') as mock_subproc:

                    # runtime.run will succeed for openssl calls, but raise for setup run command
                    def runtime_side_effect(cmd, *args, **kwargs):
                        if 'openssl' in cmd:
                            # write dummy files if needed
                            for c in cmd:
                                if c.endswith(('.crt', '.key', '.csr')):
                                    Path(c).write_text('dummy')
                            return b''
                        if 'run' in cmd and '--name' in cmd:
                            raise RuntimeError("setup container allocation / execution failure")
                        return b''

                    mock_run.side_effect = runtime_side_effect

                    # Subprocess calls for discovery and inspect
                    def subproc_side_effect(cmd, *args, **kwargs):
                        if cmd[0:3] == ['docker', 'volume', 'ls']:
                            return Mock(returncode=0, stdout=f'{vol_name}\n'.encode('utf-8'), stderr=b'')
                        elif cmd[0:2] == ['docker', 'ps']:
                            return Mock(returncode=0, stdout=f'{setup_cid}\n'.encode('utf-8'), stderr=b'')
                        elif cmd[0:2] == ['docker', 'inspect']:
                            if '--type=volume' in cmd:
                                return Mock(returncode=0, stdout=vol_inspect_json, stderr=b'')
                            elif '--type=container' in cmd:
                                return Mock(returncode=0, stdout=setup_inspect_json, stderr=b'')
                        return Mock(returncode=1, stdout=b'', stderr=b'')

                    mock_subproc.side_effect = subproc_side_effect

                    # prepare() must raise RuntimeError from setup run, but finally block must capture volume & setup container
                    with self.assertRaises(RuntimeError) as ctx:
                        fixture.prepare()

                    self.assertIn("setup container allocation / execution failure", str(ctx.exception))

                    # Verify that real qualified capture was recorded via mocked inspect JSON
                    self.assertIn(vol_name, fixture.owned_volumes)
                    self.assertIn(setup_cid, fixture.owned_containers)
                    receipt_kinds = [r['kind'] for r in fixture.verified_receipts]
                    self.assertIn('volume', receipt_kinds)
                    self.assertIn('container', receipt_kinds)

    def test_start_up_failure_still_invokes_capture_before_reraising_and_readiness_failure_does_not_bypass(self):
        """5. start up failure still invokes capture before re-raising; port/readiness failure after up
        does not bypass capture. Assert call order capture precedes port/readiness, finite discovery calls.
        """
        from task28_disable_client_probe import DynSecProbeFixture
        import json

        fixture = DynSecProbeFixture(Path('/tmp'), 'test_image')
        broker_cid = '4444444444444444444444444444444444444444444444444444444444444444'
        net_id = '5555555555555555555555555555555555555555555555555555555555555555'

        broker_inspect_json = json.dumps([{
            'Id': broker_cid,
            'Name': f'/{fixture.project}-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': fixture.project,
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False'
                }
            }
        }]).encode('utf-8')

        net_inspect_json = json.dumps([{
            'Id': net_id,
            'Name': f'{fixture.project}_default',
            'Labels': {
                'com.docker.compose.project': fixture.project,
                'com.docker.compose.network': 'default'
            }
        }]).encode('utf-8')

        # Scenario 1: `compose up` itself raises error
        with patch('task28_disable_client_probe.runtime.run') as mock_run, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc:

            mock_run.side_effect = RuntimeError("docker compose up failed partially")

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:2] == ['docker', 'ps']:
                    return Mock(returncode=0, stdout=f'{broker_cid}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:3] == ['docker', 'network', 'ls']:
                    return Mock(returncode=0, stdout=f'{net_id}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    if '--type=container' in cmd:
                        return Mock(returncode=0, stdout=broker_inspect_json, stderr=b'')
                    elif '--type=network' in cmd:
                        return Mock(returncode=0, stdout=net_inspect_json, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            with self.assertRaises(RuntimeError) as ctx:
                fixture.start()
            self.assertIn("docker compose up failed partially", str(ctx.exception))

            # Broker and network must still be captured despite up error
            self.assertIn(broker_cid, fixture.owned_containers)
            self.assertIn(net_id, fixture.owned_networks)

        # Scenario 2: `compose up` succeeds, but port/readiness fails -> capture precedes port/readiness
        fixture2 = DynSecProbeFixture(Path('/tmp'), 'test_image')
        broker_inspect_json2 = json.dumps([{
            'Id': broker_cid,
            'Name': f'/{fixture2.project}-broker-1',
            'Config': {
                'Labels': {
                    'com.docker.compose.project': fixture2.project,
                    'com.docker.compose.service': 'broker',
                    'com.docker.compose.oneoff': 'False'
                }
            }
        }]).encode('utf-8')

        net_inspect_json2 = json.dumps([{
            'Id': net_id,
            'Name': f'{fixture2.project}_default',
            'Labels': {
                'com.docker.compose.project': fixture2.project,
                'com.docker.compose.network': 'default'
            }
        }]).encode('utf-8')

        call_events = []

        with patch('task28_disable_client_probe.runtime.run') as mock_run, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc:

            def run_side_effect(cmd, *args, **kwargs):
                if 'up' in cmd:
                    call_events.append('up')
                    return b''
                if 'port' in cmd:
                    call_events.append('port')
                    raise RuntimeError("port mapping lookup failed")
                return b''

            mock_run.side_effect = run_side_effect

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:2] == ['docker', 'ps']:
                    call_events.append('discover_container')
                    return Mock(returncode=0, stdout=f'{broker_cid}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:3] == ['docker', 'network', 'ls']:
                    call_events.append('discover_network')
                    return Mock(returncode=0, stdout=f'{net_id}\n'.encode('utf-8'), stderr=b'')
                elif cmd[0:2] == ['docker', 'inspect']:
                    if '--type=container' in cmd:
                        call_events.append('inspect_container')
                        return Mock(returncode=0, stdout=broker_inspect_json2, stderr=b'')
                    elif '--type=network' in cmd:
                        call_events.append('inspect_network')
                        return Mock(returncode=0, stdout=net_inspect_json2, stderr=b'')
                return Mock(returncode=1, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect

            with self.assertRaises(RuntimeError) as ctx:
                fixture2.start()
            self.assertIn("port mapping lookup failed", str(ctx.exception))

            # Assert order: up -> discovery/inspect -> port
            self.assertIn('up', call_events)
            self.assertIn('inspect_container', call_events)
            self.assertIn('inspect_network', call_events)
            self.assertIn('port', call_events)

            idx_inspect = max(call_events.index('inspect_container'), call_events.index('inspect_network'))
            idx_port = call_events.index('port')
            self.assertLess(idx_inspect, idx_port, "Discovery/inspect must precede port resolution")

            self.assertIn(broker_cid, fixture2.owned_containers)
            self.assertIn(net_id, fixture2.owned_networks)

    def test_cleanup_verified_resource_still_exists_or_unknown_or_sticky_error_cannot_become_clean(self):
        """6. cleanup verified resource still exists -> corresponding remaining list and clean=False;
        inspection unknown -> clean=False; sticky operation error cannot become clean even if later absence.
        """
        from task28_disable_client_probe import DynSecProbeFixture

        cid = '6666666666666666666666666666666666666666666666666666666666666666'
        vid = 'dynsec_probe_test_security'
        nid = '7777777777777777777777777777777777777777777777777777777777777777'

        # Sub-case A: Resource still exists upon post-cleanup presence inspection
        fixture_a = DynSecProbeFixture(Path('/tmp'), 'test_image')
        fixture_a.record_verified_receipt('container', cid, 'broker', f'{fixture_a.project}-broker-1')

        with patch('task28_disable_client_probe.inspect_owned_resource') as mock_inspect, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.inspect_resource_presence') as mock_presence:

            mock_inspect.return_value = {'status': 'owned', 'resource_id': cid, 'reason': 'verified'}
            mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
            # Resource still exists!
            mock_presence.return_value = {'status': 'exists', 'kind': 'container', 'resource_id': cid}

            receipt_a = fixture_a.cleanup()
            self.assertFalse(receipt_a['clean'])
            self.assertIn(cid, receipt_a['containers_remaining'])

        # Sub-case B: Post-cleanup presence inspection returns unknown
        fixture_b = DynSecProbeFixture(Path('/tmp'), 'test_image')
        fixture_b.record_verified_receipt('volume', vid, 'security', vid)

        with patch('task28_disable_client_probe.inspect_owned_resource') as mock_inspect, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.inspect_resource_presence') as mock_presence:

            mock_inspect.return_value = {'status': 'owned', 'resource_id': vid, 'reason': 'verified'}
            mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
            # Presence is unknown (e.g. timeout or inspection error)
            mock_presence.return_value = {'status': 'unknown', 'kind': 'volume', 'resource_id': vid}

            receipt_b = fixture_b.cleanup()
            self.assertFalse(receipt_b['clean'])
            unknown_reasons = [u['reason'] for u in receipt_b['cleanup_unknown']]
            self.assertIn('presence_unknown', unknown_reasons)

        # Sub-case C: Sticky operation error (e.g. rm_failed) cannot become clean even if later presence is absent
        fixture_c = DynSecProbeFixture(Path('/tmp'), 'test_image')
        fixture_c.record_verified_receipt('container', cid, 'broker', f'{fixture_c.project}-broker-1')

        with patch('task28_disable_client_probe.inspect_owned_resource') as mock_inspect, \
             patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.inspect_resource_presence') as mock_presence:

            mock_inspect.return_value = {'status': 'owned', 'resource_id': cid, 'reason': 'verified'}
            # docker stop succeeds (rc=0), but docker rm fails (rc=1)
            def subproc_side_effect(cmd, *args, **kwargs):
                if 'rm' in cmd:
                    return Mock(returncode=1, stdout=b'', stderr=b'rm failed')
                return Mock(returncode=0, stdout=b'', stderr=b'')

            mock_subproc.side_effect = subproc_side_effect
            # Even if presence check later claims absent
            mock_presence.return_value = {'status': 'absent', 'kind': 'container', 'resource_id': cid}

            receipt_c = fixture_c.cleanup()
            self.assertFalse(receipt_c['clean'])
            unknown_reasons = [u['reason'] for u in receipt_c['cleanup_unknown']]
            self.assertIn('rm_failed', unknown_reasons)

        # Sub-case D: Pre-existing sticky ownership_unknown on fixture prevents clean=True even if everything absent
        fixture_d = DynSecProbeFixture(Path('/tmp'), 'test_image')
        fixture_d.ownership_unknown.append({'kind': 'container', 'resource_id': '', 'reason': 'discovery_failed'})

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.inspect_resource_presence') as mock_presence:

            mock_presence.return_value = {'status': 'absent', 'kind': 'any', 'resource_id': 'any'}

            receipt_d = fixture_d.cleanup()
            self.assertFalse(receipt_d['clean'])
            self.assertTrue(len(receipt_d['ownership_unknown']) > 0)


class TestC1dHostFixtureCleanup(unittest.TestCase):
    """Unit tests for C1d: verified removal of unique host fixture root."""

    def setUp(self):
        import os
        self.os = os
        self.test_base = Path('/tmp/opencode')
        self.test_base.mkdir(parents=True, exist_ok=True)

    def test_create_verified_host_fixture_dir_rejects_symlink_base_and_ancestor(self):
        """tmp_base itself a symlink and an intermediate ancestor symlink -> creation rejected, no fixture created."""
        from task28_disable_client_probe import create_verified_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            real_base = Path(tmpdir) / 'real_dir'
            real_base.mkdir(0o700)

            # 1. tmp_base itself is a symlink
            symlink_base = Path(tmpdir) / 'symlink_base'
            self.os.symlink(real_base, symlink_base)

            with self.assertRaises(ValueError) as ctx:
                create_verified_host_fixture_dir(symlink_base)
            self.assertIn("symlink", str(ctx.exception).lower())
            self.assertEqual(len(list(real_base.iterdir())), 0)

            # 2. Intermediate ancestor is a symlink: ancestor_symlink / child_dir
            ancestor_symlink = Path(tmpdir) / 'ancestor_link'
            self.os.symlink(real_base, ancestor_symlink)
            child_base = ancestor_symlink / 'child_dir'
            (real_base / 'child_dir').mkdir(0o700)

            with self.assertRaises(ValueError) as ctx:
                create_verified_host_fixture_dir(child_base)
            self.assertIn("symlink", str(ctx.exception).lower())
            self.assertEqual(len(list((real_base / 'child_dir').iterdir())), 0)

    def test_verified_remove_host_fixture_dir_rejects_parent_substitution_and_symlink(self):
        """After valid creation, parent substituted with symlink/replaced directory -> deletion rejected, not verified absent."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)

            # Case A: parent replaced by symlink pointing to alt_dir
            sym_parent = tmp_base / 'sym_parent'
            sym_parent.mkdir(0o700)
            sym_fix, sym_receipt = create_verified_host_fixture_dir(sym_parent)
            self.assertTrue(sym_fix.exists())

            alt_dir = tmp_base / 'alt_dir'
            alt_dir.mkdir(0o700)
            target_in_alt = alt_dir / sym_receipt['name']
            target_in_alt.mkdir(0o700)

            # Retain original sym_parent at an owned alternate location while substituting with symlink
            retained_sym_parent = tmp_base / 'retained_sym_parent'
            self.os.rename(sym_parent, retained_sym_parent)
            self.os.symlink(alt_dir, sym_parent)

            with patch('task28_disable_client_probe.shutil.rmtree') as mock_rmtree:
                res = verified_remove_host_fixture_dir(sym_receipt)
                self.assertEqual(res['status'], 'rejected')
                self.assertFalse(res['verified'])
                self.assertEqual(mock_rmtree.call_count, 0)
                # Ensure target in alt_dir was never touched or deleted
                self.assertTrue(target_in_alt.exists())

            # Cleanup symlink
            sym_parent.unlink()

            # Case B: parent replaced by different directory with guaranteed different inode
            # Rename the original parent to an owned alternate location while retaining it,
            # then create the replacement directory at the original path.
            sub_dir = tmp_base / 'sub'
            sub_dir.mkdir(0o700)
            sub_fix, sub_receipt = create_verified_host_fixture_dir(sub_dir)
            self.assertTrue(sub_fix.exists())

            retained_orig_parent = tmp_base / 'retained_orig_sub'
            self.os.rename(sub_dir, retained_orig_parent)

            # Replacement directory created at the original path while original is retained
            sub_dir.mkdir(0o700)

            # Assert original and replacement st_dev/st_ino identity differs while both exist
            orig_stat = self.os.lstat(str(retained_orig_parent))
            repl_stat = self.os.lstat(str(sub_dir))
            self.assertTrue((orig_stat.st_dev, orig_stat.st_ino) != (repl_stat.st_dev, repl_stat.st_ino))
            self.assertNotEqual(orig_stat.st_ino, repl_stat.st_ino)
            self.assertEqual(sub_receipt['parent_dev'], orig_stat.st_dev)
            self.assertEqual(sub_receipt['parent_ino'], orig_stat.st_ino)

            # Even if an entity creates a target inside the replaced parent with matching fixture name
            fake_target = sub_dir / sub_receipt['name']
            fake_target.mkdir(0o700)

            with patch('task28_disable_client_probe.shutil.rmtree') as mock_rmtree:
                res = verified_remove_host_fixture_dir(sub_receipt)
                self.assertEqual(res['status'], 'rejected')
                self.assertFalse(res['verified'])
                self.assertEqual(res['reason'], 'parent_identity_mismatch')
                self.assertEqual(mock_rmtree.call_count, 0)
                self.assertTrue(fake_target.exists())

            # Even if target is absent in the replaced parent, parent identity mismatch must reject (never already_absent)
            fake_target.rmdir()
            self.assertFalse(fake_target.exists())
            with patch('task28_disable_client_probe.shutil.rmtree') as mock_rmtree:
                res_absent = verified_remove_host_fixture_dir(sub_receipt)
                self.assertEqual(res_absent['status'], 'rejected')
                self.assertFalse(res_absent['verified'])
                self.assertEqual(res_absent['reason'], 'parent_identity_mismatch')
                self.assertEqual(mock_rmtree.call_count, 0)

            # Clean up replacement sub_dir
            sub_dir.rmdir()

            # Case C: Focused counterexamples
            # 1) Correct parent plus genuinely absent target returns already_absent
            # Restore original parent back to its recorded path
            self.os.rename(retained_orig_parent, sub_dir)
            fix_in_orig = sub_dir / sub_receipt['name']
            fix_in_orig.rmdir()
            self.assertFalse(fix_in_orig.exists())
            res_genuinely_absent = verified_remove_host_fixture_dir(sub_receipt)
            self.assertEqual(res_genuinely_absent['status'], 'already_absent')
            self.assertTrue(res_genuinely_absent['verified'])
            self.assertEqual(res_genuinely_absent['reason'], 'verified_absent')

            # 2) Inaccessible/unknown parent metadata never returns verified cleanup (fails closed)
            real_lstat = self.os.lstat

            def _inaccessible_parent_lstat(path_arg, *args, **kwargs):
                if str(path_arg) == str(sub_dir):
                    raise PermissionError("Inaccessible parent metadata")
                return real_lstat(path_arg, *args, **kwargs)

            with patch('task28_disable_client_probe.os.lstat', side_effect=_inaccessible_parent_lstat):
                res_inaccessible = verified_remove_host_fixture_dir(sub_receipt)
                self.assertEqual(res_inaccessible['status'], 'rejected')
                self.assertFalse(res_inaccessible['verified'])
                self.assertEqual(res_inaccessible['reason'], 'parent_validation_failed')

            # Parent path missing completely on disk
            sub_dir.rmdir()
            res_missing = verified_remove_host_fixture_dir(sub_receipt)
            self.assertEqual(res_missing['status'], 'rejected')
            self.assertFalse(res_missing['verified'])
            self.assertEqual(res_missing['reason'], 'parent_validation_failed')

            # Clean up remaining owned fixtures
            target_in_alt.rmdir()
            alt_dir.rmdir()
            retained_sym_fix = retained_sym_parent / sym_receipt['name']
            retained_sym_fix.rmdir()
            retained_sym_parent.rmdir()

    def test_verified_remove_host_fixture_dir_malformed_receipt_validation(self):
        """Malformed receipt path/parent/name nonstrings, noninteger dev/ino/uid/mode, invalid name, traversal -> stable invalid receipt."""
        from task28_disable_client_probe import verified_remove_host_fixture_dir

        base_valid = {
            'status': 'created',
            'path': '/tmp/opencode/test/fixture_1234abcd',
            'parent': '/tmp/opencode/test',
            'name': 'fixture_1234abcd',
            'dev': 100,
            'ino': 200,
            'uid': self.os.getuid(),
            'mode': 0o700,
            'parent_dev': 100,
            'parent_ino': 199,
            'parent_uid': self.os.getuid(),
        }

        # Non-string path/parent/name
        for field in ['path', 'parent', 'name']:
            r = dict(base_valid, **{field: 12345})
            res = verified_remove_host_fixture_dir(r)
            self.assertEqual(res['status'], 'invalid_receipt')
            self.assertFalse(res['verified'])

        # Non-integer dev, ino, uid, mode
        for field in ['dev', 'ino', 'uid', 'mode', 'parent_dev', 'parent_ino', 'parent_uid']:
            for bad_val in ['string_val', 12.34, None, True]:
                r = dict(base_valid, **{field: bad_val})
                res = verified_remove_host_fixture_dir(r)
                self.assertEqual(res['status'], 'invalid_receipt')
                self.assertFalse(res['verified'])

        # Name not fixture_<8 hex>
        for bad_name in ['fixture_XYZ', 'fixture_1234abc', 'fixture_1234abcde', 'fixture_1234ABCD', 'not_fixture_1234abcd', '../fixture_1234abcd']:
            r = dict(base_valid, name=bad_name, path=f'/tmp/opencode/test/{bad_name}')
            res = verified_remove_host_fixture_dir(r)
            self.assertEqual(res['status'], 'invalid_receipt')
            self.assertFalse(res['verified'])

        # Relative path / path traversal
        for bad_path in ['relative/fixture_1234abcd', '/tmp/opencode/test/../other/fixture_1234abcd']:
            r = dict(base_valid, path=bad_path)
            res = verified_remove_host_fixture_dir(r)
            self.assertEqual(res['status'], 'invalid_receipt')
            self.assertFalse(res['verified'])

    def test_authorized_new_private_fixture_root_removed_and_explicit_absence_proof(self):
        """1. Authorized new private fixture root -> removed and explicit absence proof."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)
            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)
            self.assertTrue(fix_dir.is_dir())
            self.assertEqual(receipt['status'], 'created')

            res = verified_remove_host_fixture_dir(receipt)
            self.assertEqual(res['status'], 'removed')
            self.assertEqual(res['reason'], 'verified_absent')
            self.assertTrue(res['verified'])
            self.assertFalse(fix_dir.exists())

    def test_rmtree_permission_error_or_os_error_unverified_root_remains(self):
        """2. rmtree PermissionError/OSError -> unverified, root remains."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)
            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)
            (fix_dir / 'child.txt').write_text('content')

            with patch('task28_disable_client_probe.shutil.rmtree') as mock_rmtree:
                mock_rmtree.side_effect = PermissionError("Permission denied")
                res = verified_remove_host_fixture_dir(receipt)
                self.assertEqual(res['status'], 'unverified')
                self.assertEqual(res['reason'], 'rmtree_failed')
                self.assertFalse(res['verified'])
                self.assertTrue(fix_dir.exists())

    def test_rmtree_returns_but_root_remains_unverified(self):
        """3. rmtree returns but root remains -> unverified."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)
            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)

            with patch('task28_disable_client_probe.shutil.rmtree') as mock_rmtree:
                # Mock rmtree doing nothing (not deleting root)
                mock_rmtree.return_value = None
                res = verified_remove_host_fixture_dir(receipt)
                self.assertEqual(res['status'], 'unverified')
                self.assertEqual(res['reason'], 'root_still_present')
                self.assertFalse(res['verified'])
                self.assertTrue(fix_dir.exists())

    def test_valid_receipt_and_root_already_absent_verified_absent(self):
        """4. Valid receipt + root already absent -> absent verified."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)
            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)
            # Remove directory directly before verified removal
            fix_dir.rmdir()
            self.assertFalse(fix_dir.exists())

            res = verified_remove_host_fixture_dir(receipt)
            self.assertEqual(res['status'], 'already_absent')
            self.assertEqual(res['reason'], 'verified_absent')
            self.assertTrue(res['verified'])

    def test_missing_invalid_receipt_wrong_identity_or_replaced_root_symlink_zero_deletion(self):
        """5. Missing/invalid receipt, wrong inode/uid/mode/parent/name, replaced root/symlink -> ZERO deletion."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        # None receipt
        res_none = verified_remove_host_fixture_dir(None)
        self.assertEqual(res_none['status'], 'invalid_receipt')
        self.assertFalse(res_none['verified'])

        # Empty or malformed receipt
        res_malformed = verified_remove_host_fixture_dir({'status': 'other'})
        self.assertEqual(res_malformed['status'], 'invalid_receipt')
        self.assertFalse(res_malformed['verified'])

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)
            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)

            # Altered inode
            bad_inode_receipt = dict(receipt, ino=receipt['ino'] + 999999)
            res = verified_remove_host_fixture_dir(bad_inode_receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'inode_mismatch')
            self.assertFalse(res['verified'])
            self.assertTrue(fix_dir.exists())

            # Altered dev
            bad_dev_receipt = dict(receipt, dev=receipt['dev'] + 999999)
            res = verified_remove_host_fixture_dir(bad_dev_receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'device_mismatch')
            self.assertFalse(res['verified'])
            self.assertTrue(fix_dir.exists())

            # Altered UID
            bad_uid_receipt = dict(receipt, uid=receipt['uid'] + 1)
            res = verified_remove_host_fixture_dir(bad_uid_receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'uid_mismatch')
            self.assertFalse(res['verified'])
            self.assertTrue(fix_dir.exists())

            # Altered mode
            bad_mode_receipt = dict(receipt, mode=0o777)
            res = verified_remove_host_fixture_dir(bad_mode_receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'mode_mismatch')
            self.assertFalse(res['verified'])
            self.assertTrue(fix_dir.exists())

            # Altered parent
            bad_parent_receipt = dict(receipt, parent=str(tmp_base / 'other_parent'))
            res = verified_remove_host_fixture_dir(bad_parent_receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'parent_mismatch')
            self.assertFalse(res['verified'])
            self.assertTrue(fix_dir.exists())

            # Replaced with symlink: remove original, replace with symlink to tmp_base
            fix_dir.rmdir()
            self.os.symlink(tmp_base, fix_dir)
            res = verified_remove_host_fixture_dir(receipt)
            self.assertEqual(res['status'], 'rejected')
            self.assertEqual(res['reason'], 'symlink_root_rejected')
            self.assertFalse(res['verified'])
            # Clean up symlink so TemporaryDirectory can exit
            fix_dir.unlink()

    def test_symlink_child_outside_fixture_survives_untouched(self):
        """6. Symlink child must not cause deletion of outside target."""
        from task28_disable_client_probe import create_verified_host_fixture_dir, verified_remove_host_fixture_dir
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir, \
             tempfile.TemporaryDirectory(dir=self.test_base) as outside_tmpdir:
            tmp_base = Path(tmpdir)
            outside_dir = Path(outside_tmpdir)
            outside_target = outside_dir / 'canary.txt'
            outside_target.write_text('survive!')

            fix_dir, receipt = create_verified_host_fixture_dir(tmp_base)
            link_child = fix_dir / 'symlink_to_outside'
            self.os.symlink(outside_target, link_child)

            res = verified_remove_host_fixture_dir(receipt)
            self.assertEqual(res['status'], 'removed')
            self.assertTrue(res['verified'])
            self.assertFalse(fix_dir.exists())
            # Outside target must survive untouched!
            self.assertTrue(outside_target.exists())
            self.assertEqual(outside_target.read_text(), 'survive!')

    def test_run_capability_probe_docker_cleanup_exception_still_attempts_host_cleanup_and_reports_uncertainty(self):
        """7. Docker cleanup exception still attempts host cleanup and reports overall uncertainty."""
        from task28_disable_client_probe import run_capability_probe
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)

            with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
                 patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
                 patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

                mock_subproc.return_value = Mock(returncode=0, stdout='sha256:' + 'a' * 64 + '\n', stderr='')
                mock_fixture = Mock()
                mock_fixture_cls.return_value = mock_fixture
                mock_fixture.prepare.side_effect = RuntimeError("prepare exploded")
                # Docker cleanup raises exception
                mock_fixture.cleanup.side_effect = RuntimeError("docker daemon disconnected")

                mock_host_rm.return_value = {
                    'status': 'removed',
                    'verified': True,
                    'reason': 'verified_absent'
                }

                results = run_capability_probe(runtime.IMAGE, tmp_base, test_persistence=False)
                self.assertEqual(results['failure_stage'], 'step1_init_and_admin')
                self.assertEqual(results['failure_code'], 'runtime_error')
                self.assertIs(results['aggregate_pass'], False)

                # Assert verified_remove_host_fixture_dir was called despite fixture.cleanup() raising
                self.assertEqual(mock_host_rm.call_count, 1)

    def test_run_capability_probe_host_unknown_prevents_overall_clean(self):
        """8. Docker clean plus host unknown must not be called clean (preserves separate and overall status)."""
        from task28_disable_client_probe import run_capability_probe
        import tempfile

        with tempfile.TemporaryDirectory(dir=self.test_base) as tmpdir:
            tmp_base = Path(tmpdir)

            with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
                 patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
                 patch('task28_disable_client_probe.MQTT'), \
                 patch('task28_disable_client_probe.NativeTargetConnection') as mock_native_conn, \
                 patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
                 patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

                dummy_id = 'sha256:' + 'a' * 64
                cid_full = 'b' * 64

                def subproc_side_effect(cmd, *args, **kwargs):
                    if cmd[0:3] == ['docker', 'image', 'inspect']:
                        return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                    if cmd[0:2] == ['docker', 'exec']:
                        return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                    if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                        return Mock(returncode=0, stdout=json.dumps([{
                            'Id': cid_full,
                            'Name': f'/{tmp_base.name}-broker-1',
                            'Config': {
                                'Labels': {
                                    'com.docker.compose.project': tmp_base.name,
                                    'com.docker.compose.service': 'broker',
                                    'com.docker.compose.oneoff': 'False',
                                }
                            }
                        }]).encode('utf-8'), stderr=b'')
                    return Mock(returncode=0, stdout='', stderr='')

                mock_subproc.side_effect = subproc_side_effect
                mock_fixture = Mock()
                mock_fixture.port = 18883
                mock_fixture.d = tmp_base
                mock_fixture.project = tmp_base.name
                mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
                mock_fixture.open_connections = []
                mock_fixture.verified_receipts = [
                    {
                        'kind': 'container',
                        'resource_id': cid_full,
                        'expected_project': tmp_base.name,
                        'expected_role': 'broker',
                        'expected_name': f'{tmp_base.name}-broker-1',
                    }
                ]
                mock_fixture.cleanup.return_value = {
                    'clean': True,
                    'containers_remaining': [],
                    'volumes_remaining': [],
                    'networks_remaining': [],
                    'cleanup_unknown': [],
                    'ownership_unknown': [],
                }
                mock_fixture_cls.return_value = mock_fixture

                mock_target_conn = Mock()
                mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)
                mock_target_conn.message.return_value = ('probe/target/dev1', b'msg')
                mock_native_conn.return_value = mock_target_conn

                # Host cleanup failed / unknown
                mock_host_rm.return_value = {
                    'status': 'unverified',
                    'verified': False,
                    'reason': 'rmtree_failed'
                }

                mock_admin = Mock()
                mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

                def mqtt_side_effect(spike, user, pw):
                    return mock_admin

                with patch('task28_disable_client_probe.MQTT', side_effect=mqtt_side_effect):
                    def message_side_effect():
                        # Returns echo topic and message
                        return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

                    def pub_side_effect(top, msg):
                        mock_target_conn.last_pub_topic = top
                        mock_target_conn.last_pub_msg = msg

                    mock_target_conn.message.side_effect = message_side_effect
                    mock_target_conn.publish.side_effect = pub_side_effect

                    results = run_capability_probe(runtime.IMAGE, tmp_base, test_persistence=False)

                receipt = results['cleanup_receipt']
                self.assertIsNotNone(receipt)
                self.assertTrue(receipt['docker_clean'])
                self.assertFalse(receipt['host_directory_cleanup']['verified'])
                # Docker clean + host unknown MUST NOT be clean!
                self.assertFalse(receipt['clean'])



class TestC1eAggregateProbeOutcome(unittest.TestCase):
    """C1e Unit Tests: Truthful probe aggregate outcome, safe stage accounting, no raw exceptions."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_base = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_compute_probe_outcomes_pure_combinations(self):
        """Pure aggregate combinations of core/restart/docker-clean/host-verified and missing/wrong-type fields."""
        from task28_disable_client_probe import compute_probe_outcomes

        base_res = {
            'step1_init_and_admin': True,
            'step2_create_client_and_role': True,
            'step3_dual_session_functional': True,
            'step4_disable_command_and_readback': True,
            'step5_active_sessions_disconnected': True,
            'step5_observer_unaffected': True,
            'step6_same_password_rejected': True,
            'step7_persistence_check': True,
            'cleanup_receipt': {
                'clean': True,
                'docker_clean': True,
                'host_directory_cleanup': {'verified': True, 'status': 'removed'}
            }
        }

        # 1. Full clean pass
        c, a = compute_probe_outcomes(base_res)
        self.assertIs(c, True)
        self.assertIs(a, True)

        # 2. One core step False -> core False, aggregate False
        for step in ('step1_init_and_admin', 'step2_create_client_and_role',
                      'step3_dual_session_functional', 'step4_disable_command_and_readback',
                      'step5_active_sessions_disconnected', 'step5_observer_unaffected',
                      'step6_same_password_rejected'):
            res = dict(base_res)
            res[step] = False
            c, a = compute_probe_outcomes(res)
            self.assertIs(c, False)
            self.assertIs(a, False)

        # 3. Wrong type on core step (truthy integer 1 or non-empty string) -> strictly False
        res = dict(base_res)
        res['step6_same_password_rejected'] = 1
        c, a = compute_probe_outcomes(res)
        self.assertIs(c, False)
        self.assertIs(a, False)

        # 4. Core passes, but restart False -> core True, aggregate False
        res = dict(base_res)
        res['step7_persistence_check'] = False
        c, a = compute_probe_outcomes(res)
        self.assertIs(c, True)
        self.assertIs(a, False)

        # 5. Missing / wrong type on step7
        for val in (None, 1, 'True', []):
            res = dict(base_res)
            res['step7_persistence_check'] = val
            c, a = compute_probe_outcomes(res)
            self.assertIs(c, True)
            self.assertIs(a, False)

        # 6. Cleanup receipts: docker_clean False -> aggregate False
        res = dict(base_res)
        res['cleanup_receipt'] = {
            'clean': False,
            'docker_clean': False,
            'host_directory_cleanup': {'verified': True}
        }
        c, a = compute_probe_outcomes(res)
        self.assertIs(c, True)
        self.assertIs(a, False)

        # 7. Cleanup receipts: host_directory_cleanup verified False -> aggregate False
        res = dict(base_res)
        res['cleanup_receipt'] = {
            'clean': False,
            'docker_clean': True,
            'host_directory_cleanup': {'verified': False}
        }
        c, a = compute_probe_outcomes(res)
        self.assertIs(c, True)
        self.assertIs(a, False)

        # 8. Missing cleanup_receipt or non-dict -> aggregate False
        for val in (None, {}, 'clean', False):
            res = dict(base_res)
            res['cleanup_receipt'] = val
            c, a = compute_probe_outcomes(res)
            self.assertIs(c, True)
            self.assertIs(a, False)

    def test_restart_skipped_leaves_aggregate_false_even_if_core_true(self):
        """restart skipped (test_persistence=False) -> aggregate false though core diagnostic may be true."""
        from task28_disable_client_probe import run_capability_probe

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'b' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{self.tmp_base.name}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': self.tmp_base.name,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = self.tmp_base.name
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': self.tmp_base.name,
                    'expected_role': 'broker',
                    'expected_name': f'{self.tmp_base.name}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)
            mock_target_conn.last_pub_topic = 'probe/target/dev1'
            mock_target_conn.last_pub_msg = b'ping'

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=False)

            self.assertIs(results['pass_all_core'], True)
            self.assertIs(results['step7_persistence_check'], False)
            self.assertEqual(results['persistence_stage'], 'skipped')
            self.assertIs(results['aggregate_pass'], False)
            self.assertIs(results['pass_all_required'], False)
            self.assertIsNone(results['failure_stage'])
            self.assertIsNone(results['failure_code'])

    def test_failure_after_core_before_restart_finalization_preserves_core_true_aggregate_false(self):
        """Failure during restart preserves core True but sets aggregate False, failure_stage recorded, cleanups run."""
        from task28_disable_client_probe import run_capability_probe

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'b' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{self.tmp_base.name}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': self.tmp_base.name,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = self.tmp_base.name
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': self.tmp_base.name,
                    'expected_role': 'broker',
                    'expected_name': f'{self.tmp_base.name}-broker-1',
                }
            ]
            mock_fixture.restart.side_effect = RuntimeError("restart container failed to restart")
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['pass_all_core'], True)
            self.assertIs(results['step7_persistence_check'], False)
            self.assertEqual(results['persistence_stage'], 'failed')
            self.assertIs(results['aggregate_pass'], False)
            self.assertIs(results['pass_all_required'], False)
            self.assertEqual(results['failure_stage'], 'step7_persistence_check')
            self.assertEqual(results['failure_code'], 'runtime_error')
            self.assertEqual(mock_fixture.cleanup.call_count, 1)
            self.assertEqual(mock_host_rm.call_count, 1)

    def test_setup_failure_records_failure_stage_and_no_fabricated_later_success_and_no_secret_leak(self):
        """Setup exception catches safely: failure_stage recorded, no fabricated steps, raw secret sentinel not in JSON."""
        from task28_disable_client_probe import run_capability_probe

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            mock_subproc.return_value = Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
            mock_fixture = Mock()
            secret_sentinel = "SECRET_TOKEN_DO_NOT_LEAK_xyz123"
            mock_fixture.prepare.side_effect = RuntimeError(f"prepare exploded with password {secret_sentinel}")
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertEqual(results['failure_stage'], 'step1_init_and_admin')
            self.assertEqual(results['failure_code'], 'runtime_error')
            self.assertIs(results['step1_init_and_admin'], False)
            self.assertIs(results['step2_create_client_and_role'], False)
            self.assertIs(results['step3_dual_session_functional'], False)
            self.assertIs(results['step4_disable_command_and_readback'], False)
            self.assertIs(results['step5_active_sessions_disconnected'], False)
            self.assertIs(results['step6_same_password_rejected'], False)
            self.assertIs(results['step7_persistence_check'], False)
            self.assertIs(results['pass_all_core'], False)
            self.assertIs(results['aggregate_pass'], False)
            self.assertIs(results['pass_all_required'], False)

            # Dump JSON and verify secret sentinel never appears
            serialized = json.dumps(results)
            self.assertNotIn(secret_sentinel, serialized)
            self.assertEqual(mock_fixture.cleanup.call_count, 1)
            self.assertEqual(mock_host_rm.call_count, 1)

    def test_docker_and_host_cleanup_failures_independently_veto_aggregate(self):
        """Docker and host cleanup failures independently veto aggregate_pass even when core and restart passed."""
        from task28_disable_client_probe import run_capability_probe

        dummy_id = 'sha256:' + 'a' * 64
        cid_full = 'b' * 64

        def subproc_side_effect(cmd, *args, **kwargs):
            if cmd[0:3] == ['docker', 'image', 'inspect']:
                return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
            if cmd[0:2] == ['docker', 'exec']:
                return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
            if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                return Mock(returncode=0, stdout=json.dumps([{
                    'Id': cid_full,
                    'Name': f'/{self.tmp_base.name}-broker-1',
                    'Config': {
                        'Labels': {
                            'com.docker.compose.project': self.tmp_base.name,
                            'com.docker.compose.service': 'broker',
                            'com.docker.compose.oneoff': 'False',
                        }
                    }
                }]).encode('utf-8'), stderr=b'')
            return Mock(returncode=0, stdout='', stderr='')

        # Case A: Docker cleanup unverified/not clean
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            mock_subproc.side_effect = subproc_side_effect
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = self.tmp_base.name
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': self.tmp_base.name,
                    'expected_role': 'broker',
                    'expected_name': f'{self.tmp_base.name}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': False,
                'containers_remaining': ['c1'],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['pass_all_core'], True)
            self.assertIs(results['step7_persistence_check'], True)
            self.assertIs(results['cleanup_receipt']['docker_clean'], False)
            self.assertIs(results['cleanup_receipt']['clean'], False)
            self.assertIs(results['aggregate_pass'], False)
            self.assertIs(results['pass_all_required'], False)

        # Case B: Host cleanup unverified/not clean
        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            mock_subproc.side_effect = subproc_side_effect
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = self.tmp_base.name
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': self.tmp_base.name,
                    'expected_role': 'broker',
                    'expected_name': f'{self.tmp_base.name}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'unverified',
                'verified': False,
                'reason': 'rmtree_failed'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['pass_all_core'], True)
            self.assertIs(results['step7_persistence_check'], True)
            self.assertIs(results['cleanup_receipt']['host_directory_cleanup']['verified'], False)
            self.assertIs(results['cleanup_receipt']['clean'], False)
            self.assertIs(results['aggregate_pass'], False)
            self.assertIs(results['pass_all_required'], False)

    def test_full_successful_mocked_sequence_yields_aggregate_pass_true(self):
        """Full successful mocked sequence => aggregate true, evidence explicitly mock-only."""
        from task28_disable_client_probe import run_capability_probe

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'b' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{self.tmp_base.name}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': self.tmp_base.name,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = self.tmp_base.name
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': self.tmp_base.name,
                    'expected_role': 'broker',
                    'expected_name': f'{self.tmp_base.name}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['pass_all_core'], True)
            self.assertIs(results['step7_persistence_check'], True)
            self.assertEqual(results['persistence_stage'], 'succeeded')
            self.assertIs(results['aggregate_pass'], True)
            self.assertIs(results['pass_all_required'], True)
            self.assertIsNone(results['failure_stage'])
            self.assertIsNone(results['failure_code'])
            self.assertTrue(results['cleanup_receipt']['clean'])


class TestC1PreflightCorrections(unittest.TestCase):
    """C1 Preflight and Broker Version Verification Tests."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_base = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_unsupported_latest_or_mismatched_image_zero_subprocess_and_safe_preflight_failure(self):
        """Unsupported/latest/mismatched image reference causes immediate preflight failure with zero subprocesses."""
        from task28_disable_client_probe import run_capability_probe

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.create_verified_host_fixture_dir') as mock_host_create, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls:

            for bad_image in ('latest', 'eclipse-mosquitto:latest', 'eclipse-mosquitto:2.0.18', 'unpinned:tag', ''):
                mock_subproc.reset_mock()
                mock_host_create.reset_mock()
                mock_fixture_cls.reset_mock()

                results = run_capability_probe(bad_image, self.tmp_base, test_persistence=True)
                self.assertEqual(results['failure_stage'], 'preflight')
                self.assertEqual(results['failure_code'], 'invalid_image_reference')
                self.assertIs(results['aggregate_pass'], False)
                self.assertIs(results['pass_all_core'], False)
                self.assertIs(results['pass_all_required'], False)
                self.assertEqual(mock_subproc.call_count, 0)
                self.assertEqual(mock_host_create.call_count, 0)
                self.assertEqual(mock_fixture_cls.call_count, 0)

    def test_image_inspect_timeout_error_malformed_zero_fixture_allocation(self):
        """Image inspect timeout, non-zero returncode, or malformed config ID triggers safe preflight failure and zero allocation."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.create_verified_host_fixture_dir') as mock_host_create, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls:

            # 1. Non-zero returncode
            mock_subproc.return_value = Mock(returncode=1, stdout='', stderr='')
            results = run_capability_probe(runtime.IMAGE, self.tmp_base)
            self.assertEqual(results['failure_stage'], 'preflight')
            self.assertEqual(results['failure_code'], 'image_inspect_failed')
            self.assertIs(results['aggregate_pass'], False)
            self.assertEqual(mock_host_create.call_count, 0)
            self.assertEqual(mock_fixture_cls.call_count, 0)

            # 2. Timeout
            mock_subproc.side_effect = subprocess.TimeoutExpired(cmd=['docker', 'image', 'inspect'], timeout=5.0)
            results = run_capability_probe(runtime.IMAGE, self.tmp_base)
            self.assertEqual(results['failure_stage'], 'preflight')
            self.assertEqual(results['failure_code'], 'image_inspect_timeout')
            self.assertIs(results['aggregate_pass'], False)
            self.assertEqual(mock_host_create.call_count, 0)

            # 3. Malformed image ID (not sha256:<64hex>)
            mock_subproc.side_effect = None
            mock_subproc.return_value = Mock(returncode=0, stdout='not_a_valid_sha256_id\n', stderr='')
            results = run_capability_probe(runtime.IMAGE, self.tmp_base)
            self.assertEqual(results['failure_stage'], 'preflight')
            self.assertEqual(results['failure_code'], 'image_inspect_malformed')
            self.assertIs(results['aggregate_pass'], False)
            self.assertEqual(mock_host_create.call_count, 0)

    def test_no_docker_run_anywhere_in_probe_orchestration(self):
        """Verification that docker run is never invoked anywhere during capability probe."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'b' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{mock_fixture.project}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': mock_fixture.project,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect

            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = 'dynsec_probe_1234567890ab'
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': mock_fixture.project,
                    'expected_role': 'broker',
                    'expected_name': f'{mock_fixture.project}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['aggregate_pass'], True)
            # Ensure docker run was never called in any subprocess.run invocation
            for c in mock_subproc.call_args_list:
                cmd = c[0][0]
                self.assertFalse(cmd[0:2] == ['docker', 'run'], f"docker run was called: {cmd}")

    def test_version_check_uses_exact_verified_broker_id_via_exec(self):
        """Version check runs via docker exec with exact owned container ID and runtime.ENV."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'c' * 64

            exec_calls = []

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    exec_calls.append((cmd, kwargs))
                    return Mock(returncode=0, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{mock_fixture.project}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': mock_fixture.project,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect

            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = 'dynsec_probe_testproj'
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': mock_fixture.project,
                    'expected_role': 'broker',
                    'expected_name': f'{mock_fixture.project}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['aggregate_pass'], True)
            self.assertEqual(len(exec_calls), 1)
            cmd, kw = exec_calls[0]
            self.assertEqual(cmd, ['docker', 'exec', cid_full, 'mosquitto', '-h'])
            self.assertEqual(kw.get('env'), runtime.ENV)
            self.assertEqual(results['broker_version'], 'mosquitto version 2.0.18')

    def test_missing_receipt_or_revalidation_failure_or_version_mismatch_fails_and_cleans_up(self):
        """Missing receipt, revalidation failure, exec timeout, or version mismatch yields safe failure and runs cleanup."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'd' * 64

            # Subtest A: Missing broker container receipt
            mock_subproc.side_effect = None
            mock_subproc.return_value = Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = 'dynsec_probe_testproj'
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = []  # No container receipt!
            mock_fixture.cleanup.return_value = {'clean': True, 'containers_remaining': [], 'volumes_remaining': [], 'networks_remaining': [], 'cleanup_unknown': [], 'ownership_unknown': []}
            mock_fixture_cls.return_value = mock_fixture

            mock_host_rm.return_value = {'status': 'removed', 'verified': True, 'reason': 'verified_absent'}

            results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)
            self.assertEqual(results['failure_stage'], 'step1_init_and_admin')
            self.assertEqual(results['failure_code'], 'broker_receipt_missing')
            self.assertIs(results['aggregate_pass'], False)
            self.assertEqual(mock_fixture.cleanup.call_count, 1)
            self.assertEqual(mock_host_rm.call_count, 1)

            # Subtest B: Version mismatch
            mock_fixture.reset_mock()
            mock_host_rm.reset_mock()
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': mock_fixture.project,
                    'expected_role': 'broker',
                    'expected_name': f'{mock_fixture.project}-broker-1',
                }
            ]

            def subproc_mismatch(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=0, stdout='mosquitto version 1.6.9\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{mock_fixture.project}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': mock_fixture.project,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_mismatch
            results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)
            self.assertEqual(results['failure_stage'], 'step1_init_and_admin')
            self.assertEqual(results['failure_code'], 'broker_version_mismatch')
            self.assertIs(results['aggregate_pass'], False)
            self.assertEqual(mock_fixture.cleanup.call_count, 1)
            self.assertEqual(mock_host_rm.call_count, 1)

    def test_version_check_exit3_with_expected_version_succeeds(self):
        """mosquitto -h with returncode 3 and anchored version string succeeds."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.probe_raw_connack', return_value=0x05), \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'c' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    # Native mosquitto -h exits with code 3
                    return Mock(returncode=3, stdout='mosquitto version 2.0.18\nmosquitto is an MQTT v3.1.1/v5.0 broker.\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{mock_fixture.project}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': mock_fixture.project,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect

            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = 'dynsec_probe_testproj'
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': mock_fixture.project,
                    'expected_role': 'broker',
                    'expected_name': f'{mock_fixture.project}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_target_conn = Mock()
            mock_target_conn.wait_for_disconnect.return_value = (True, 'eof', 0.1)

            def message_side_effect():
                return (mock_target_conn.last_pub_topic, mock_target_conn.last_pub_msg)

            def pub_side_effect(top, msg):
                mock_target_conn.last_pub_topic = top
                mock_target_conn.last_pub_msg = msg

            mock_target_conn.message.side_effect = message_side_effect
            mock_target_conn.publish.side_effect = pub_side_effect

            mock_admin = Mock()
            mock_admin.request.side_effect = lambda req, **kw: {'client': {'disabled': True}} if req == 'getClient' else {}

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            with patch('task28_disable_client_probe.MQTT', return_value=mock_admin), \
                 patch('task28_disable_client_probe.NativeTargetConnection', return_value=mock_target_conn):

                results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)

            self.assertIs(results['aggregate_pass'], True)
            self.assertEqual(results['broker_version'], 'mosquitto version 2.0.18')
            self.assertIs(results['step1_init_and_admin'], True)

    def test_version_check_unexpected_returncode_fails(self):
        """mosquitto -h with unexpected returncode (e.g. 1) fails with broker_version_failed."""
        from task28_disable_client_probe import run_capability_probe
        import stage2_mosquitto_runtime as runtime

        with patch('task28_disable_client_probe.subprocess.run') as mock_subproc, \
             patch('task28_disable_client_probe.DynSecProbeFixture') as mock_fixture_cls, \
             patch('task28_disable_client_probe.verified_remove_host_fixture_dir') as mock_host_rm:

            dummy_id = 'sha256:' + 'a' * 64
            cid_full = 'c' * 64

            def subproc_side_effect(cmd, *args, **kwargs):
                if cmd[0:3] == ['docker', 'image', 'inspect']:
                    return Mock(returncode=0, stdout=f'{dummy_id}\n', stderr='')
                if cmd[0:2] == ['docker', 'exec']:
                    return Mock(returncode=1, stdout='mosquitto version 2.0.18\n', stderr='')
                if cmd[0:3] == ['docker', 'inspect', '--type=container']:
                    return Mock(returncode=0, stdout=json.dumps([{
                        'Id': cid_full,
                        'Name': f'/{mock_fixture.project}-broker-1',
                        'Config': {
                            'Labels': {
                                'com.docker.compose.project': mock_fixture.project,
                                'com.docker.compose.service': 'broker',
                                'com.docker.compose.oneoff': 'False',
                            }
                        }
                    }]).encode('utf-8'), stderr=b'')
                return Mock(returncode=0, stdout='', stderr='')

            mock_subproc.side_effect = subproc_side_effect

            mock_fixture = Mock()
            mock_fixture.port = 18883
            mock_fixture.d = self.tmp_base
            mock_fixture.project = 'dynsec_probe_testproj'
            mock_fixture.passwords = {'admin': 'p1', 'target': 'p2', 'observer': 'p3'}
            mock_fixture.open_connections = []
            mock_fixture.verified_receipts = [
                {
                    'kind': 'container',
                    'resource_id': cid_full,
                    'expected_project': mock_fixture.project,
                    'expected_role': 'broker',
                    'expected_name': f'{mock_fixture.project}-broker-1',
                }
            ]
            mock_fixture.cleanup.return_value = {
                'clean': True,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [],
                'ownership_unknown': [],
            }
            mock_fixture_cls.return_value = mock_fixture

            mock_host_rm.return_value = {
                'status': 'removed',
                'verified': True,
                'reason': 'verified_absent'
            }

            results = run_capability_probe(runtime.IMAGE, self.tmp_base, test_persistence=True)
            self.assertEqual(results['failure_stage'], 'step1_init_and_admin')
            self.assertEqual(results['failure_code'], 'broker_version_failed')
            self.assertIs(results['aggregate_pass'], False)

    def test_fixture_prepare_mount_replacement_and_setup_script_contract(self):
        """Fixture prepare replaces broker bind mount with named volume security:/fixture:ro exactly once and configures setup script."""
        from task28_disable_client_probe import DynSecProbeFixture
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            fix_dir = Path(tmpdir)
            fixture = DynSecProbeFixture(fix_dir, 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9')

            run_calls = []
            def fake_runtime_run(cmd, *args, **kwargs):
                run_calls.append((cmd, kwargs))
                if 'openssl' in cmd:
                    for c in cmd:
                        if c.endswith(('.crt', '.key', '.csr')):
                            Path(c).write_text('dummy')
                return b''

            with patch('task28_disable_client_probe.runtime.run', side_effect=fake_runtime_run), \
                 patch('task28_disable_client_probe.subprocess.run') as mock_subproc:

                mock_subproc.return_value = Mock(returncode=0, stdout=b'', stderr=b'')
                fixture.prepare()

            # 1. Inspect generated compose file
            compose_text = fixture.compose_file.read_text()
            self.assertIn('security:/fixture:ro', compose_text)
            self.assertNotIn('${DYNSEC_FIXTURE:?protected fixture required}/broker:/fixture:ro', compose_text)
            # Setup service still retains the host bind mount to read prepared files
            self.assertIn('${DYNSEC_FIXTURE:?protected fixture required}/broker:/fixture', compose_text)

            # 2. Inspect setup script executed via compose run
            setup_runs = [c for c in run_calls if 'run' in c[0] and fixture.setup_name in c[0]]
            self.assertEqual(len(setup_runs), 1)
            cmd, kw = setup_runs[0]
            script_idx = cmd.index('-ec') + 1
            script = cmd[script_idx]
            self.assertNotIn('backend_service', script)
            self.assertEqual(kw['data'], ((fixture.passwords['admin'] + '\n') * 2).encode())

            # Setup script copies exact files into /security
            self.assertIn('cp /fixture/broker.conf /fixture/ca.crt /fixture/server.crt /fixture/server.key /security/', script)
            self.assertIn('chmod 600 /security/broker.conf /security/ca.crt /security/server.crt /security/server.key', script)
            # Chowns ONLY /security, never /fixture
            self.assertIn('chown -R 1883:1883 /security', script)
            self.assertNotIn('/fixture', script.split('chown')[1])
            self.assertNotIn('chown -R 1883:1883 /security /fixture', script)

    def test_fixture_prepare_malformed_mount_template_raises(self):
        """Malformed COMPOSE_BASE missing broker fixture mount template raises safely rather than falling back silently."""
        from task28_disable_client_probe import DynSecProbeFixture
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            fix_dir = Path(tmpdir)
            fixture = DynSecProbeFixture(fix_dir, 'test-image')

            # Mock COMPOSE_BASE with read_text returning missing template mount
            mock_base = Mock()
            mock_base.read_text.return_value = 'services:\n  broker:\n    image: test-image\n'
            with patch('task28_disable_client_probe.COMPOSE_BASE', mock_base):
                with self.assertRaises(RuntimeError) as ctx:
                    fixture.prepare()
                self.assertIn('expected broker fixture mount template must occur exactly once', str(ctx.exception))


class TestOptionalBackendInitialization(unittest.TestCase):
    def test_default_fixture_has_no_backend_secret(self):
        from task28_disable_client_probe import DynSecProbeFixture
        fixture = DynSecProbeFixture(Path('/tmp/opencode'), 'test-image')
        self.assertEqual(set(fixture.passwords), {'admin', 'target', 'observer'})

    def test_backend_native_initialization_is_private_and_before_launch(self):
        import tempfile
        import json
        import base64
        from task28_disable_client_probe import DynSecProbeFixture
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            fixture = DynSecProbeFixture(Path(directory), 'test-image', initialize_backend=True)
            self.assertNotEqual(fixture.passwords['admin'], fixture.passwords['backend_service'])
            calls = []

            def fake_run(cmd, **kwargs):
                calls.append((cmd, kwargs))
                if cmd[0] == 'openssl':
                    for arg in cmd:
                        if arg.endswith(('.crt', '.key', '.csr')):
                            Path(arg).write_text('dummy')
                elif 'run' in cmd and 'backend-init.json' in cmd[-1]:
                    for name, username in (('admin', 'admin'), ('backend', 'backend_service')):
                        source = {'clients': [{'username': username,
                                  'password': base64.b64encode(b'h' * 64).decode(),
                                  'salt': base64.b64encode(b's' * 12).decode(),
                                  'iterations': 101, 'roles': [{'rolename': 'admin'}]}],
                                  'roles': [{'rolename': 'admin', 'acls': []}]}
                        path = fixture.d / 'broker' / (name + '-init.json')
                        path.write_text(json.dumps(source))
                        path.chmod(0o600)
                elif 'port' in cmd:
                    return b'127.0.0.1:12345'
                return b''

            with patch('task28_disable_client_probe.runtime.run', side_effect=fake_run), \
                 patch.object(fixture, '_discover_and_verify'), \
                 patch('task28_disable_client_probe.runtime.mqtt_connection'):
                fixture.prepare()
                fixture.start()
            setups = [(cmd, kw) for cmd, kw in calls if 'run' in cmd]
            self.assertEqual(len(setups), 2)
            store = json.loads(setups[1][1]['data'])
            backend = store['clients'][1]
            self.assertEqual(backend['username'], 'backend_service')
            self.assertEqual(backend['roles'], [{'rolename': 'bootstrap_backend'}])
            self.assertEqual(store['defaultACLAccess'], {kind: False for kind in
                             ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')})
            self.assertEqual(store['roles'][0], {'rolename': 'admin', 'acls': []})
            self.assertEqual(store['roles'][1], {'rolename': 'bootstrap_backend', 'acls': [
                {'acltype': kind, 'topic': topic, 'allow': True, 'priority': 0}
                for kind, topic in [('publishClientReceive', 'gateways/+/telemetry/#'),
                                   ('subscribeLiteral', 'gateways/+/telemetry/#'),
                                   ('unsubscribeLiteral', 'gateways/+/telemetry/#'),
                                   ('publishClientSend', 'gateways/+/acks/#')]]})
            self.assertEqual(setups[0][1]['data'], ''.join(
                (fixture.passwords[key] + '\n') * 2
                for key in ('admin', 'backend_service')).encode())
            self.assertIn('chmod 600', setups[0][0][-1])
            self.assertIn('chown -R 1883:1883 /security', setups[1][0][-1])
            self.assertFalse(list((fixture.d / 'broker').glob('*-init.json')))
            launch_index = next(i for i, (cmd, _) in enumerate(calls) if 'up' in cmd)
            self.assertLess(calls.index(setups[1]), launch_index)
            for secret in fixture.passwords.values():
                self.assertNotIn(secret, repr([cmd for cmd, _ in calls]))
                self.assertNotIn(secret, json.dumps(store))
                self.assertNotIn(secret, json.dumps(fixture.verified_receipts))


if __name__ == '__main__':
    unittest.main()
