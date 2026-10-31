import base64
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
import os
import tempfile
import time
import unittest
import contextlib
import io
from pathlib import Path
from unittest import mock

from scripts import paper_p8_client as client
from scripts.tests.test_paper_p8_bundle import PortableBundleTests, token, write


class FragmentedSocket:
    def __init__(self, response=b''):
        self.response = response
        self.sent = bytearray()
        self.writes = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def settimeout(self, _):
        pass

    def sendall(self, value):
        self.writes.append(value)
        self.sent.extend(value)

    def recv(self, size):
        value = self.response[:min(size, 3)]
        self.response = self.response[len(value):]
        return value


class IdentityAndProbeTests(unittest.TestCase):
    def test_conflict_transport_has_no_legacy_external_client_path(self):
        self.assertFalse(hasattr(client, '_run_b_conflicts'))

    def test_bound_e4_conflict_manifests_preflight_with_real_binary(self):
        binary = os.environ.get('P8_TEST_PAPERLOAD')
        if not binary:
            self.skipTest('set P8_TEST_PAPERLOAD to a real paperload binary for offline preflight')
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        telemetry = dict(fixture.telemetry)
        telemetry.update({'experiment': 'E4', 'arm': 'baseline', 'durability_mode': 'synthetic-load',
            'retry_interval_ms': 500, 'publish_timeout_ms': 1000,
            'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'targets': [{'gateway_id': 'gw-1', 'sensor_id': 'sensor-1'},
                        {'gateway_id': 'gw-2', 'sensor_id': 'sensor-2'}],
            'gateway_identity_files': {'gw-1': '/A/private/gw-1.json',
                                       'gw-2': '/A/private/gw-2.json'},
            'e4': {'mode': 'unique', 'journal_path': 'outputs/e4-journal.sqlite',
                   'max_messages': 20, 'max_bytes': 1 << 20}})
        unique_path = write(fixture.src / 'unique.manifest.json', telemetry)
        write(fixture.src / 'gw-1.json', {'username': 'gw-1', 'password': 'gw-1-password'})
        gateway_env_1 = json.loads((fixture.src / 'gateway-env.json').read_text())
        gateway_env_1['GATEWAY_MQTT_USERNAME'] = 'gw-1'
        gateway_env_1['GATEWAY_MQTT_PASSWORD'] = 'gw-1-password'
        write(fixture.src / 'gateway-env.json', gateway_env_1)
        write(fixture.src / 'gw-2.json', {'username': 'gw-2', 'password': 'gw-2-password'})
        write(fixture.src / 'gateway-env-2.json', {'GATEWAY_CONTROL_ENABLED': 'true',
            'GATEWAY_ID': 'gw-2', 'SENSOR_ID': 'sensor-2',
            'GATEWAY_MQTT_BROKER_URL': 'mqtts://broker.test:18884',
            'GATEWAY_MQTT_CLIENT_ID': 'client-2', 'GATEWAY_MQTT_USERNAME': 'gw-2',
            'GATEWAY_MQTT_PASSWORD': 'gw-2-password', 'GATEWAY_MQTT_CA_FILE': '/A/private/ca.pem',
            'GATEWAY_OUTBOX_PATH': '/A/private/outbox-2.sqlite'})
        spec_path = write(fixture.src / 'e4-run-spec.json', {'paperload': [
            {'manifest': str(unique_path), 'output_dir': 'outputs/unique',
             'role': 'telemetry', 'ready_timeout_seconds': 30}],
            'gateways': [{'env_file': 'gateway-env.json', 'role': 'durable-telemetry'},
                         {'env_file': 'gateway-env-2.json', 'role': 'durable-telemetry'}],
            'stop_timeout_seconds': 30})
        roles = {'roles': {'telemetry': {'role': 'telemetry', 'principals': []}},
                 'gateways': {'gw-1': {'role': 'gateway', 'gateway_id': 'gw-1', 'username': 'gw-1'},
                              'gw-2': {'role': 'gateway', 'gateway_id': 'gw-2', 'username': 'gw-2'}}}
        exported = fixture.export(experiment='E4', phase='unique', manifests={'unique': unique_path},
            identity_files={'telemetry': fixture.src / 'telemetry.json'}, role_metadata=roles,
            gateway_identity_files={'gw-1': fixture.src / 'gw-1.json', 'gw-2': fixture.src / 'gw-2.json'},
            gateway_env_files={'gw-1': fixture.src / 'gateway-env.json',
                               'gw-2': fixture.src / 'gateway-env-2.json'},
            run_spec=spec_path, workload={'experiment': 'E4', 'arm': 'baseline',
                'run_id': 'run-1', 'profile': 'pilot', 'phase': 'unique',
                'samples_per_message': 100, 'measure_seconds': 1})
        workspace = fixture.root / 'B' / 'workspace'
        workspace.parent.mkdir(mode=0o700)
        client.paper_p8_bundle.bind_bundle(bundle_root=Path(exported['bundle_root']), workspace_leaf=workspace)
        bound_spec = json.loads((workspace / 'runtime/execution/run-spec.json').read_text())
        job = bound_spec['paperload'][0]
        self.assertEqual(job['role'], 'telemetry')
        self.assertEqual(Path(job['manifest']).name, 'unique.json')
        source_manifest_path = Path(job['manifest'])
        source_manifest_bytes = source_manifest_path.read_bytes()
        source_manifest = json.loads(source_manifest_bytes)
        self.assertEqual(len(source_manifest['targets']), 2)
        self.assertEqual(set(source_manifest['gateway_identity_files']), {'gw-1', 'gw-2'})

        # Build the exact client-side probe from a private journal, then run every
        # generated manifest through the real Go preflight. Only network runs are mocked.
        (workspace / 'outputs').mkdir(mode=0o700, exist_ok=True)
        original = {'protocol_version': 1, 'message_id': '550e8400-e29b-41d4-a716-446655440000',
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-1', 'sensor_id': 'sensor-1',
            'boot_id': 'boot-1', 'first_sequence': 10, 'sample_count': 100,
            'measured_at': '2026-10-28T00:00:00Z', 'sample_interval_us': 10000,
            'samples': [float(i) for i in range(100)]}
        payload = json.dumps(original, separators=(',', ':')).encode()
        record = {'topic': 'gateways/gw-1/telemetry/sensor-1', 'gateway_id': 'gw-1',
            'sensor_id': 'sensor-1', 'message_id': original['message_id'], 'boot_id': 'boot-1',
            'first_sequence': '10', 'sample_count': 100, 'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'payload': base64.b64encode(payload).decode()}
        source_journal_path = workspace / 'outputs/e4-journal.sqlite'
        write(source_journal_path, json.dumps(record) + '\n')
        source_journal_bytes = source_journal_path.read_bytes()
        write(workspace / 'runtime/e4-phase-state.json', {'unique_validated': True})
        cases = client._export_conflict_probe(workspace, fixture.root / 'probe')
        probe = json.loads((fixture.root / 'probe/conflict-probe.json').read_text())
        conflict_cases = probe['cases']
        source_manifest = client._private_json(source_manifest_path)
        with self.assertRaisesRegex(ValueError, 'p8_e4_conflict_target_missing'):
            client._select_bound_conflict_target(source_manifest, source_manifest_path, workspace,
                'gw-absent', 'sensor-1')
        wrong_credential_path = workspace / 'outputs/wrong-credential.json'
        write(wrong_credential_path, {'username': 'not-gw-1', 'password': 'test-password'})
        wrong_source_manifest = json.loads(json.dumps(source_manifest))
        wrong_source_manifest['gateway_identity_files']['gw-1'] = str(wrong_credential_path)
        with self.assertRaisesRegex(ValueError, 'p8_e4_conflict_credential_rejected'):
            client._select_bound_conflict_target(wrong_source_manifest, source_manifest_path,
                workspace, 'gw-1', 'sensor-1')
        launches = []

        def run_network_only_mock(argv, **kwargs):
            if argv[-1] != '--run':
                return real_run(argv, **kwargs)
            manifest_path = Path(argv[argv.index('--manifest') + 1])
            manifest = json.loads(manifest_path.read_text())
            journal_bytes = Path(manifest['e4']['journal_path']).read_bytes()
            row = json.loads(journal_bytes)
            body = base64.b64decode(row['payload'])
            message = json.loads(body)
            launches.append((manifest, len(journal_bytes), len(body)))
            output_dir = Path(argv[argv.index('--output-dir') + 1]); output_dir.mkdir(mode=0o700)
            positive = manifest['run_id'].endswith('positive-control')
            workload = {'status': 'COMPLETE' if positive else 'PARTIAL',
                'telemetry': {'replay_offered': 1, 'replay_application_acks': 1 if positive else 0,
                    'pending_messages': 0 if positive else 1, 'publish_attempts': 1,
                    'broker_pubacks': 1, 'retries': 0 if positive else 1},
                'phase_events': [{'event': 'workers_joined'}], 'cleanup_errors': []}
            write(output_dir / 'workload.json', workload)
            return subprocess.CompletedProcess(argv, 0 if positive else 4)

        real_run = subprocess.run
        with mock.patch.object(client.subprocess, 'run', side_effect=run_network_only_mock):
            result = client._run_conflict_stage(workspace, binary=binary)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual([item[0]['samples_per_message'] for item in launches], [100, 100, 100])
        self.assertEqual(source_manifest_path.read_bytes(), source_manifest_bytes)
        self.assertEqual(source_journal_path.read_bytes(), source_journal_bytes)
        self.assertEqual(source_manifest['targets'], json.loads(source_manifest_path.read_text())['targets'])
        self.assertTrue(all(item[0]['targets'] == [{'gateway_id': 'gw-1', 'sensor_id': 'sensor-1'}]
                            and set(item[0]['gateway_identity_files']) == {'gw-1'} for item in launches))
        self.assertTrue(all(item[0]['topology']['gateways'] == 1
                            and item[0]['limits']['workers'] == 1
                            and item[0]['limits']['connections'] == 1
                            and item[0]['limits']['pending_messages'] == 1 for item in launches))
        for label in ('positive-control', 'same_id_changed_payload',
                      'different_first_sequence_overlap'):
            row = json.loads((workspace / 'outputs/e4-conflicts' / label / 'journal.jsonl').read_text())
            self.assertEqual((row['gateway_id'], row['sensor_id']), ('gw-1', 'sensor-1'))
            self.assertEqual(row['topic'], 'gateways/gw-1/telemetry/sensor-1')
        self.assertTrue(all(item[0]['e4']['max_bytes'] == item[1] for item in launches))
        overlap_manifest = workspace / 'outputs/e4-conflicts/different_first_sequence_overlap/manifest.json'
        self.assertEqual(json.loads(overlap_manifest.read_text())['samples_per_message'], 100)
        overlap_message = json.loads(base64.b64decode(
            json.loads((workspace / 'runtime/e4-conflicts.json').read_text())['cases']
                ['different_first_sequence_overlap']['raw_payload_b64']))
        self.assertNotEqual(overlap_message['message_id'], original['message_id'])
        self.assertEqual(overlap_message['first_sequence'], original['first_sequence'] + 1)
        self.assertEqual(overlap_message['sample_count'], 100)
        self.assertLess(overlap_message['first_sequence'], original['first_sequence'] + original['sample_count'])
        self.assertEqual(cases['cases'], 2)

    def test_durable_helpers_load_without_test_pythonpath(self):
        repo = Path(__file__).resolve().parents[2]
        env = {key: value for key, value in os.environ.items() if key != 'PYTHONPATH'}
        code = ("from scripts import paper_p8_client as client; "
                "assert all(callable(fn) for fn in client._durable_helpers())")
        result = subprocess.run([sys.executable, '-c', code], cwd=repo, env=env,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_e3_command_projection_keeps_reconciliation_fields_and_drops_private_fields(self):
        projected = client._safe_workload({'status': 'COMPLETE', 'experiment': 'E3', 'arm': 'control',
            'commands': [{'classification': 'succeeded', 'idempotency_key': 'a' * 36,
                'command_id': 'cmd-1', 'sensor_id': 'sensor-1', 'gateway_id': 'gw-1',
                'expected_desired_version': '4', 'desired_version': '5', 'frequency_hz': 0.5,
                'sample_interval_us': 2000000, 'http_status': 202, 'client_round_trip_ms': 12,
                'access_token': 'must-not-export'}]}, role='control')
        row = projected['commands'][0]
        self.assertEqual(row['command_id'], 'cmd-1')
        self.assertEqual(row['sample_interval_us'], 2000000)
        self.assertEqual(row['http_status'], 202)
        self.assertNotIn('access_token', row)

    def test_e3_safe_phase_projection_accepts_exact_control_child_label(self):
        events = client._safe_phase_events([{'child': 'paperload-control:control.manifest.json',
            'event': 'request_begin', 'elapsed_ns': 10,
            'clock_qualification': 'runner observed pipe receipt; observation lag unknown',
            'command_ordinal': 1}])
        self.assertEqual(events[0]['child'], 'paperload-control:control.manifest.json')

    def test_e5_safe_phase_events_accepts_initial_and_warm_replay_child_labels(self):
        accepted_children = [
            'paperload-telemetry:initial.json',
            'paperload-telemetry:initial.manifest.json',
            'paperload-telemetry:warm_replay.json',
            'paperload-telemetry:warm_replay.manifest.json',
            'paperload-telemetry:initial-replay.manifest.json',
            'paperload-telemetry:initial-replay.json',
            'paperload-telemetry:warm-replay.manifest.json',
            'paperload-telemetry:warm-replay.json',
            'paperload-telemetry:unique.json',
            'paperload-telemetry:replay.json',
            'paperload-telemetry:cold-replay.json',
            'paperload-telemetry:A.json',
            'paperload-telemetry:B.json',
            'paperload-viewer:viewer.json',
            'paperload-viewer:viewer.manifest.json',
            'paperload-control:control.json',
            'paperload-control:control.manifest.json',
        ]
        for child in accepted_children:
            events = client._safe_phase_events([{'child': child,
                'event': 'measure_begin', 'elapsed_ns': 100,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local'}])
            self.assertEqual(events[0]['child'], child)

    def test_safe_phase_events_rejects_malformed_and_mismatched_child_labels(self):
        rejected_children = [
            'paperload-telemetry:control.json',
            'paperload-telemetry:viewer.json',
            'paperload-viewer:initial.json',
            'paperload-viewer:control.json',
            'paperload-control:initial.json',
            'paperload-control:viewer.json',
            'paperload-telemetry:../initial.json',
            'paperload-telemetry:/etc/passwd',
            'paperload-telemetry:initial',
            'paperload-telemetry:initial.yaml',
            'http://evil.com/exploit.json',
            'admin:secret_token_123',
            'paperload-unknown:initial.json',
            'arbitrary_child_label.json',
        ]
        for child in rejected_children:
            with self.assertRaisesRegex(ValueError, 'result phase event schema rejected'):
                client._safe_phase_events([{'child': child,
                    'event': 'measure_begin', 'elapsed_ns': 100,
                    'clock_qualification': 'runner observed pipe receipt; observation lag unknown'}])

    def test_e3_control_workload_phase_qualification_is_bounded_and_preserved(self):
        result = client._safe_workload({'status': 'COMPLETE', 'experiment': 'E3',
            'arm': 'control', 'phase_observation_status':
                'observed; maximum 9 command ordinals sampled; process-local clock'}, role='control')
        self.assertEqual(result['phase_observation_status'],
            'observed; maximum 9 command ordinals sampled; process-local clock')

    def test_viewer_map_probes_all_bounded_principals_for_rest_and_ws(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        workspace = fixture.root / 'B'
        workspace.mkdir(mode=0o700)
        bound = workspace / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=bound)
        spec = bound / 'runtime/execution/run-spec.json'
        with mock.patch.object(client, '_https_get') as rest, \
                mock.patch.object(client, '_websocket_handshake') as ws, \
                mock.patch.object(client, '_mqtt_connect_probe') as mqtt:
            result = client._check_connection(bound, spec)
        self.assertEqual(rest.call_count, 1)
        self.assertEqual(ws.call_count, 1)
        self.assertEqual(mqtt.call_count, 2)
        self.assertEqual(result['mqtt_connections_checked'], 2)
        manifest = json.loads(Path(json.loads(spec.read_text())['paperload'][0]['manifest']).read_text())
        tokens = client._tokens(manifest, 'viewer')
        self.assertEqual(len(tokens), 2)

    def test_websocket_handshake_accepts_fragmented_headers_and_closes_without_subscribe(self):
        client_key = base64.b64encode(b'nonce-test').decode()
        nonce_accept = base64.b64encode(hashlib.sha1(
            (client_key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        response = (f'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                    f'Connection: keep-alive, Upgrade\r\nSec-WebSocket-Protocol: iot.telemetry.v1\r\n'
                    f'Sec-WebSocket-Accept: {nonce_accept}\r\n\r\n').encode()
        stream = FragmentedSocket(response)
        context = mock.Mock()
        context.wrap_socket.side_effect = lambda raw, server_hostname: raw
        manifest = {'tls': {'https_endpoint': 'https://example.test', 'https_ca_file': '/ca'},
                    'identity_files': {'viewer': '/identity'}}
        with mock.patch.object(client, '_tls_context', return_value=context), \
                mock.patch.object(client, '_tokens', return_value=['header.payload.signature']), \
                mock.patch.object(client.socket, 'create_connection', return_value=stream), \
                mock.patch.object(client.os, 'urandom', return_value=b'nonce-test'):
            client._websocket_handshake(manifest, 'viewer')
        self.assertTrue(stream.closed)
        request = bytes(stream.sent)
        self.assertIn(b'Sec-WebSocket-Protocol: iot.telemetry.v1', request)
        self.assertIn(b'Authorization: Bearer header.payload.signature', request)
        self.assertNotIn(b'Subscribe', request)

    def test_websocket_handshake_rejects_wrong_accept(self):
        response = (b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                    b'Connection: Upgrade\r\nSec-WebSocket-Protocol: iot.telemetry.v1\r\n'
                    b'Sec-WebSocket-Accept: wrong\r\n\r\n')
        stream = FragmentedSocket(response)
        context = mock.Mock()
        context.wrap_socket.side_effect = lambda raw, server_hostname: raw
        manifest = {'tls': {'https_endpoint': 'https://example.test', 'https_ca_file': '/ca'},
                    'identity_files': {'viewer': '/identity'}}
        with mock.patch.object(client, '_tls_context', return_value=context), \
                mock.patch.object(client, '_tokens', return_value=['header.payload.signature']), \
                mock.patch.object(client.socket, 'create_connection', return_value=stream):
            with self.assertRaisesRegex(ValueError, 'authenticated WebSocket handshake rejected'):
                client._websocket_handshake(manifest, 'viewer')
        self.assertTrue(stream.closed)

    def test_rest_probe_checks_every_token_and_does_not_follow_redirects(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        parent = fixture.root / 'B'; parent.mkdir(mode=0o700)
        workspace = parent / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=workspace)
        spec = json.loads((workspace / 'runtime/execution/run-spec.json').read_text())
        manifest = json.loads(Path(spec['paperload'][0]['manifest']).read_text())
        response = mock.MagicMock()
        response.status = 200
        response.geturl.return_value = 'https://fixture.test:14443/v1/me/permissions'
        response.__enter__.return_value = response
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(client, '_tls_context', return_value=mock.Mock()), \
                mock.patch.object(client.urllib.request, 'build_opener', return_value=opener):
            client._https_get(manifest, 'viewer')
        self.assertEqual(opener.open.call_count, 2)
        self.assertTrue(all(call.args[0].get_header('Authorization').startswith('Bearer ')
                            for call in opener.open.call_args_list))

    def test_mqtt_read_exact_handles_fragmentation_and_invalid_token_map_is_rejected(self):
        stream = FragmentedSocket(b'\x20\x02\x00\x00')
        self.assertEqual(client._read_exact(stream, 4), b'\x20\x02\x00\x00')
        path = Path(tempfile.mkdtemp()) / 'identity.json'
        path.write_text(json.dumps({'viewer': 'not-a-jwt'}))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'role identity token map rejected'):
            client._tokens({'identity_files': {'viewer': str(path)}}, 'viewer')
        ca = Path(tempfile.mkdtemp(prefix='p8-bad-ca-', dir='/var/tmp/opencode')) / 'ca.pem'
        ca.write_text('not a PEM certificate')
        with self.assertRaises((OSError, ValueError)):
            client.ssl.create_default_context(cafile=str(ca))

    def test_mqtt_probe_is_connect_connack_disconnect_only(self):
        stream = FragmentedSocket(b'\x20\x02\x00\x00')
        context = mock.Mock()
        context.wrap_socket.side_effect = lambda raw, server_hostname: raw
        with mock.patch.object(client.ssl, 'create_default_context', return_value=context), \
                mock.patch.object(client.socket, 'create_connection', return_value=stream):
            client._mqtt_connect_probe(endpoint='mqtts://broker.example.test:8883',
                ca_file='/ca.pem', server_name='broker.example.test',
                gateway={'username': 'user', 'password': 'password'})
        packets = bytes(stream.sent)
        self.assertEqual(packets[-2:], b'\xe0\x00')
        self.assertEqual(packets[0], 0x10)
        self.assertEqual(stream.writes[-1], b'\xe0\x00')
        self.assertEqual(len(stream.writes), 2)
        self.assertTrue(stream.closed)

    def test_e3_probe_checks_telemetry_credentials_and_each_same_gateway_executor_config(self):
        root = Path(tempfile.mkdtemp(prefix='p8-client-e3-', dir='/var/tmp/opencode'))
        root.chmod(0o700)
        (root / 'private').mkdir(mode=0o700)
        manifests = []
        for index, gateway_id in enumerate(('gw-a', 'gw-b')):
            credential = root / 'private' / f'{gateway_id}.json'
            credential.write_text(json.dumps({'username': gateway_id, 'password': f'pw-{gateway_id}'}))
            credential.chmod(0o600)
            manifest = root / 'private' / f'arm-{index}.json'
            manifest.write_text(json.dumps({'roles': ['telemetry'],
                'tls': {'mqtt_tls_endpoint': f'mqtts://manifest-{index}.test:8883',
                        'mqtt_tls_ca_file': str(root / 'ca.pem')},
                'gateway_identity_files': {gateway_id: str(credential)}}))
            manifest.chmod(0o600)
            manifests.append(str(manifest))
        (root / 'ca.pem').write_text('ca'); (root / 'ca.pem').chmod(0o600)
        gateways = []
        for index, sensor in enumerate(('control-1', 'control-2')):
            password = root / 'private' / f'password-{index}.txt'
            password.write_text('same-gateway-password'); password.chmod(0o600)
            env = root / 'private' / f'executor-{index}.json'
            env.write_text(json.dumps({'GATEWAY_MQTT_PASSWORD_FILE': str(password),
                'GATEWAY_MQTT_USERNAME': 'gw-a', 'GATEWAY_MQTT_BROKER_URL': 'ssl://executor.test:8884',
                'GATEWAY_MQTT_CA_FILE': str(root / 'ca.pem'),
                'GATEWAY_MQTT_SERVER_NAME': 'mqtt.example.test', 'GATEWAY_MQTT_CLIENT_ID': f'client-{index}',
                'GATEWAY_ID': 'gw-a', 'SENSOR_ID': sensor}))
            env.chmod(0o600)
            gateways.append({'env_file': str(env), 'role': 'executor'})
        spec = root / 'spec.json'
        spec.write_text(json.dumps({'paperload': [{'manifest': path} for path in manifests],
                                    'gateways': gateways}))
        spec.chmod(0o600)
        with mock.patch.object(client, '_mqtt_connect_probe') as probe:
            result = client._check_connection(root, spec)
        self.assertEqual(result['mqtt_connections_checked'], 4)
        endpoints = [call.kwargs['endpoint'] for call in probe.call_args_list]
        self.assertIn('ssl://executor.test:8884', endpoints)
        self.assertIn('mqtts://manifest-0.test:8883', endpoints)
        self.assertIn('mqtts://manifest-1.test:8883', endpoints)
        executor_checks = [call for call in probe.call_args_list
                            if call.kwargs['endpoint'] == 'ssl://executor.test:8884']
        self.assertEqual(len(executor_checks), 2)
        self.assertTrue(all(call.kwargs['server_name'] == 'mqtt.example.test'
                            for call in executor_checks))


class ResultProjectionTests(unittest.TestCase):
    def test_preflight_uses_all_bound_manifests_without_creating_workload_dirs(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        parent = fixture.root / 'B'; parent.mkdir(mode=0o700)
        workspace = parent / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=workspace)
        binary = parent / 'paperload'; binary.write_text('test binary'); binary.chmod(0o700)
        with mock.patch.object(client.paper_p8, 'preflight', return_value=mock.Mock(stdout='ok')) as preflight:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                status = client.main(['preflight', '--workspace', str(workspace), '--binary', str(binary)])
        self.assertEqual(status, 0)
        self.assertEqual(preflight.call_count, 2)
        self.assertTrue(all(not Path(call.args[2]).exists()
                            for call in preflight.call_args_list))

    def test_export_preserves_role_counters_separates_roles_and_redacts_paths(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        parent = fixture.root / 'B'
        parent.mkdir(mode=0o700)
        workspace = parent / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=workspace)
        spec = json.loads((workspace / 'runtime/execution/run-spec.json').read_text())
        for job in spec['paperload']:
            output = Path(job['output_dir'])
            output.mkdir(mode=0o700)
            role = job['role']
            value = {'status': 'COMPLETE', 'experiment': 'E1', 'arm': 'baseline',
                     'run_id': 'run-1', 'started_at': '2026-10-30T00:00:00Z',
                     'finished_at': '2026-10-30T00:00:01Z',
                     'telemetry': {'generated_unique': 20, 'offered_unique': 20,
                                   'application_acks': 20, 'pending_messages': 0,
                                   'pending_bytes': 0},
                     'phase_events': [{'event': 'measure_end', 'elapsed_ns': 10,
                                       'counters': {'offered_unique': 20}}],
                      'primary_error': '/private/host/path secret'}
            value['implementation_status'] = ('P8.4c viewer-only client; no control/durable/server orchestration'
                if role == 'viewer' else 'P8.4b telemetry synthetic-load; no durable restart assurance')
            if role == 'viewer':
                value.update({'connected_distinct_viewers': 64,
                              'viewers': [{'id': 'viewer-00000', 'events': 2,
                                            'unique_events': 2, 'duplicates': 0, 'bytes': 300}],
                              'identity_samples': [{'viewer_id':'viewer-001','gateway_id':'gw-a',
                                'sensor_id':'s-1','message_id':'m-1','boot_id':'b-1',
                                'first_sequence':'1','sample_count':100,'header_sha256':'c'*64}]})
            else:
                value['phase_observation_status'] = 'observed_process_local_elapsed; runner_pipe_observation_lag_unknown'
                value['identity_samples'] = [{'viewer_id':'','gateway_id':'gw-a',
                    'sensor_id':'s-1','message_id':'m-1','boot_id':'b-1',
                    'first_sequence':'1','sample_count':100,'header_sha256':'d'*64}]
            path = output / 'workload.json'
            path.write_text(json.dumps(value))
            path.chmod(0o600)
        runner = workspace / 'outputs/run.json'
        runner.write_text(json.dumps({'status': 'COMPLETE', 'children': [{'name': '/private/path'}],
                                      'resource_sampling': {'status': 'not_collected_in_P8.4'}}))
        runner.chmod(0o600)
        provenance = workspace / 'runtime/runner-provenance.json'
        provenance.write_text(json.dumps({'binary_sha256': 'a' * 64,
                                          'binary_os': 'linux', 'binary_arch': 'amd64'}))
        provenance.chmod(0o600)
        out = fixture.root / 'safe-results'
        client._export_results(workspace, out)
        summary = json.loads((out / 'results.json').read_text())
        self.assertEqual(len({row['path'] for row in summary['files']}), len(summary['files']))
        self.assertEqual(set(summary['bound_manifest_sha256']), {'viewer','telemetry'})
        self.assertTrue(all(re.fullmatch(r'[0-9a-f]{64}', digest) for digest in
                            summary['bound_gateway_environment_sha256'].values()))
        self.assertRegex(summary['bound_run_spec_sha256'], r'^[0-9a-f]{64}$')
        viewer = json.loads((out / 'results/workloads/viewer/workload.json').read_text())
        self.assertEqual(viewer['telemetry']['offered_unique'], 20)
        self.assertEqual(viewer['viewer_summary_by_close_reason']['unspecified']['viewers'], 1)
        self.assertEqual(viewer['viewers'][0]['id'], 'viewer-00000')
        self.assertEqual(viewer['identity_samples'][0]['header_sha256'], 'c'*64)
        telemetry = json.loads((out / 'results/workloads/telemetry/workload.json').read_text())
        self.assertEqual(telemetry['identity_samples'][0]['viewer_id'], '')
        self.assertEqual(telemetry['phase_observation_status'],
                         'observed_process_local_elapsed; runner_pipe_observation_lag_unknown')
        self.assertNotIn('private-user', (out / 'results/workloads/viewer/workload.json').read_text())
        exported_text = '\n'.join(path.read_text() for path in out.rglob('*.json'))
        self.assertNotIn('/private', exported_text)

    def test_e4_phase_selection_keeps_viewer_and_exactly_one_telemetry_writer(self):
        jobs = [
            {'role': 'telemetry', 'manifest': '/b/manifests/unique.json', 'output_dir': '/b/outputs/unique'},
            {'role': 'telemetry', 'manifest': '/b/manifests/replay.json', 'output_dir': '/b/outputs/replay'},
            {'role': 'viewer', 'manifest': '/b/manifests/viewer.json', 'output_dir': '/b/outputs/viewer'},
        ]
        selected = client._select_e4_phase_jobs(jobs, 'unique')
        self.assertEqual({Path(item['manifest']).stem for item in selected}, {'unique', 'viewer'})
        self.assertEqual(len([item for item in selected if item['role'] == 'telemetry']), 1)
        with self.assertRaisesRegex(ValueError, 'unique_phase_not_validated'):
            client._select_e4_phase_jobs(jobs, 'replay')
        with self.assertRaisesRegex(ValueError, 'phase_rejected'):
            client._select_e4_phase_jobs(jobs, 'warm')

    def test_e4_cold_phase_is_opt_in_and_selects_cold_manifest_only(self):
        jobs = [
            {'role': 'telemetry', 'manifest': '/b/manifests/unique.json'},
            {'role': 'telemetry', 'manifest': '/b/manifests/replay.json'},
            {'role': 'telemetry', 'manifest': '/b/manifests/cold-replay.json'},
            {'role': 'viewer', 'manifest': '/b/manifests/viewer.json'},
        ]
        with self.assertRaisesRegex(ValueError, 'unique_phase_not_validated'):
            client._select_e4_phase_jobs(jobs, 'cold-replay')
        selected = client._select_e4_phase_jobs(jobs, 'cold-replay', replay_validated=True)
        self.assertEqual({Path(item['manifest']).stem for item in selected}, {'cold-replay', 'viewer'})
        with self.assertRaisesRegex(ValueError, 'E4 phase input rejected'):
            client._select_e4_phase_jobs(jobs[:-2] + jobs[-1:], 'cold-replay', replay_validated=True)

    def test_e4_safe_projection_preserves_phase_proof_without_private_material(self):
        payload = {'status': 'COMPLETE', 'experiment': 'E4', 'run_id': 'e4-unique',
            'telemetry': {'generated_unique': 20, 'application_acks': 20},
            'e4': {'mode': 'unique', 'journal_entry_count': 20,
                'safe_header_samples': [{'topic': 'gateways/gw-a/telemetry/sensor-a', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
                    'message_id': 'm-1', 'boot_id': 'boot-1', 'first_sequence': '1',
                    'sample_count': 100, 'payload_sha256': 'a' * 64}]},
            'primary_error': '/private/secrets'}
        projected = client._safe_workload(payload, role='telemetry')
        self.assertEqual(projected['e4']['mode'], 'unique')
        self.assertEqual(projected['e4']['safe_header_samples'][0]['payload_sha256'], 'a' * 64)
        self.assertNotIn('primary_error', projected)
        with self.assertRaisesRegex(ValueError, 'E4 proof schema rejected'):
            client._safe_workload({**payload, 'e4': {'mode': 'unique', 'journal_path': '/private/x'}},
                                  role='telemetry')

    def test_e4_workspace_lock_rejects_concurrent_phase_runner(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); (root / 'runtime').mkdir(mode=0o700)
            first = client._e4_phase_lock(root)
            try:
                with self.assertRaisesRegex(ValueError, 'already active'):
                    client._e4_phase_lock(root)
            finally:
                import os
                os.close(first)
            second = client._e4_phase_lock(root)
            import os
            os.close(second)

    def test_e4_phase_result_requires_real_viewer_subscription_and_event_evidence(self):
        telemetry = {'status': 'COMPLETE', 'experiment': 'E4', 'e4': {
            'mode': 'unique', 'journal_entry_count': 20, 'safe_header_samples': [{} for _ in range(10)]},
            'telemetry': {'generated_unique': 20, 'application_acks': 20,
                'ignored_valid_acks': 1, 'retries': 1, 'pending_messages': 0}}
        viewer = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20}
        client._validate_e4_phase_result('unique', telemetry, viewer)
        viewer['subscribed_distinct_viewers'] = 0
        with self.assertRaisesRegex(ValueError, 'viewer proof rejected'):
            client._validate_e4_phase_result('unique', telemetry, viewer)

    def test_e4_safe_projection_keeps_named_viewers_and_phase_observations(self):
        workload = {'status': 'COMPLETE', 'experiment': 'E1',
            'viewers': [{'id': 'e4-viewer-a', 'unique_events': 10, 'duplicates': 0,
                         'close_reason': 'measurement_complete'}],
            'phase_events': [{'child': 'paperload-telemetry:unique.json',
                'event': 'measure_begin', 'elapsed_ns': 123,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local'}]}
        projected = client._safe_workload(workload, role='viewer')
        self.assertEqual(projected['viewers'][0]['id'], 'e4-viewer-a')
        events = client._safe_phase_events(workload['phase_events'])
        self.assertEqual(events[0]['child'], 'paperload-telemetry:unique.json')

    def test_server_accepts_begin_request_and_rejects_finish_before_collector_ack(self):
        from scripts import paper_p8_server
        with self.assertRaises(FileNotFoundError):
            paper_p8_server.main(['begin', '--root', '/nope', '--session-id', 'x',
                                  '--phase', 'READY', '--phase-id', 'p', '--experiment', 'E3'])
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        from scripts.tests import paper_p8_supervisor
        private = fixture.root / 'fixture/private'
        private.mkdir(mode=0o700, parents=True)
        state = paper_p8_supervisor.initialize(private, experiment='E1', arm='baseline',
            seed=1, deadline_seconds=10, receipts=[], preparation_status='E1_PREPARED')
        root = fixture.root
        with self.assertRaisesRegex(ValueError, 'request_finish_transition_invalid'):
            paper_p8_server.main(['finish-phase', '--root', str(root), '--session-id', state['session_id'],
                                  '--phase', 'READY', '--phase-id', 'pilot', '--results', '/nope'])
        paper_p8_server.main(['begin', '--root', str(root), '--session-id', state['session_id'],
                              '--phase', 'READY', '--phase-id', 'pilot'])
        request = json.loads((private / 'request.json').read_text())
        self.assertEqual(request['command'], 'begin')
        self.assertEqual(request['phase_id'], 'pilot')

    def test_server_publishes_e3_begin_for_collector_import_slice(self):
        from scripts import paper_p8_server
        from scripts.tests import paper_p8_supervisor
        fixture=PortableBundleTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        private=fixture.root/'fixture/private'; private.mkdir(mode=0o700,parents=True)
        state=paper_p8_supervisor.initialize(private,experiment='E3',arm='polling',seed=89,
            deadline_seconds=300,receipts=[],preparation_status='E3_PREPARED')
        paper_p8_server.main(['begin','--root',str(fixture.root),'--experiment','E3',
            '--session-id',state['session_id'],'--phase','READY','--phase-id','e3-low_load-polling'])
        request=json.loads((private/'request.json').read_text())
        self.assertEqual(request['command'],'begin')
        self.assertEqual(request['phase_id'],'e3-low_load-polling')

    def test_server_begin_accepts_explicit_e2_prepared_profile_only(self):
        from scripts import paper_p8_server
        from scripts.tests import paper_p8_supervisor
        fixture = PortableBundleTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        private = fixture.root / 'fixture/private'; private.mkdir(mode=0o700, parents=True)
        state = paper_p8_supervisor.initialize(private, experiment='E2', arm='E2-pilot',
            seed=84, deadline_seconds=10, receipts=[], preparation_status='E2_PREPARED')
        paper_p8_server.main(['begin', '--root', str(fixture.root), '--session-id', state['session_id'],
            '--phase', 'READY', '--phase-id', 'samples-100', '--experiment', 'E2'])
        request = json.loads((private / 'request.json').read_text())
        self.assertEqual(request['phase_id'], 'samples-100')

    def test_run_failure_keeps_partial_report_and_pending_evidence(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        parent = fixture.root / 'B'
        parent.mkdir(mode=0o700)
        workspace = parent / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=workspace)
        binary = parent / 'paperload'; binary.write_text('binary'); binary.chmod(0o700)
        pending = workspace / 'outputs/outbox/pending.sqlite'
        with sqlite3.connect(pending) as database:
            database.execute('CREATE TABLE pending (message_id TEXT PRIMARY KEY)')
            database.execute('INSERT INTO pending VALUES (?)', ('keep-me',))
        pending.chmod(0o600)

        def failed_runner(*, run_artifact, **kwargs):
            report = {'status': 'PARTIAL', 'cleanup_errors': ['fixture failure']}
            Path(run_artifact).write_text(json.dumps(report))
            Path(run_artifact).chmod(0o600)
            return 'PARTIAL', report

        stderr = io.StringIO()
        with mock.patch.object(client.paper_p8, 'run_supervised', side_effect=failed_runner), \
                contextlib.redirect_stderr(stderr):
            status = client.main(['run', '--workspace', str(workspace), '--binary', str(binary)])
        self.assertEqual(status, 1)
        with sqlite3.connect(pending) as database:
            self.assertEqual(database.execute('SELECT message_id FROM pending').fetchone(), ('keep-me',))
        self.assertTrue((workspace / 'outputs/run.json').is_file())
        self.assertTrue((workspace / 'runtime/runner-provenance.json').is_file())

    def test_workspace_symlink_is_rejected(self):
        fixture = PortableBundleTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        source = Path(fixture.export()['bundle_root'])
        parent = fixture.root / 'B'; parent.mkdir(mode=0o700)
        workspace = parent / 'workspace'
        client.paper_p8_bundle.bind_bundle(bundle_root=source, workspace_leaf=workspace)
        link = parent / 'workspace-link'; link.symlink_to(workspace, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'workspace symlink path rejected'):
            client._workspace(link)
        trusted = workspace / 'inputs/certs/https-ca.pem'
        trusted.write_text('mutated CA')
        trusted.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'bound input provenance mismatch'):
            client._workspace(workspace)

    def test_conflict_helpers_load_without_test_pythonpath(self):
        repo = Path(__file__).resolve().parents[2]
        env = {key: value for key, value in os.environ.items() if key != 'PYTHONPATH'}
        code = ("from scripts import paper_p8_client as client; "
                "e4, p5a = client._conflict_helpers(); "
                "assert callable(e4.generate_e4_conflict_payloads) and callable(p5a.conflict_payloads)")
        result = subprocess.run([sys.executable, '-c', code], cwd=repo, env=env,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_export_conflict_probe_and_conflict_stage_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ws = root / 'workspace'
            ws.mkdir(mode=0o700, parents=True)
            runtime = ws / 'runtime'
            runtime.mkdir(mode=0o700, parents=True)
            outputs = ws / 'outputs'
            outputs.mkdir(mode=0o700, parents=True)
            inputs = ws / 'inputs'
            inputs.mkdir(mode=0o700, parents=True)
            (inputs / 'certs').mkdir(mode=0o700, parents=True)
            (inputs / 'private/gateways').mkdir(mode=0o700, parents=True)

            ca_file = inputs / 'certs/mqtt-ca.pem'
            ca_file.write_text('ca-content')
            ca_file.chmod(0o600)

            gw_cred = inputs / 'private/gateways/gw-1.json'
            gw_cred.write_text(json.dumps({'username': 'gw-1', 'password': 'secret-pass'}))
            gw_cred.chmod(0o600)

            # Run spec
            (runtime / 'execution').mkdir(mode=0o700, parents=True)
            spec_file = runtime / 'execution/run-spec.json'
            spec_file.write_text('{}')
            spec_file.chmod(0o600)
            spec_hash = hashlib.sha256(b'{}').hexdigest()

            # Binding
            binding = {
                'schema_version': 1,
                'session': {
                    'experiment': 'E4',
                    'session_id': 'b' * 32,
                    'run_id': 'run-e4-1',
                    'endpoints': {'mqtt_tls': 'mqtts://127.0.0.1:18883'}
                },
                'source_bundle_sha256': 'c' * 64,
                'source_inventory': [
                    {'path': 'certs/mqtt-ca.pem', 'sha256': hashlib.sha256(b'ca-content').hexdigest()},
                    {'path': 'private/gateways/gw-1.json', 'sha256': hashlib.sha256(gw_cred.read_bytes()).hexdigest()}
                ],
                'bound_manifest_sha256': {},
                'bound_gateway_environment_sha256': {},
                'bound_run_spec_sha256': spec_hash,
                'elapsed_transfer_wait_seconds': 0,
                'required_token_lifetime_seconds': 3600
            }
            binding_path = runtime / 'binding.json'
            binding_path.write_text(json.dumps(binding))
            binding_path.chmod(0o600)

            # Phase state: unique completed
            phase_state = {
                'unique_validated': True,
                'last_completed_phase': 'replay'
            }
            state_path = runtime / 'e4-phase-state.json'
            state_path.write_text(json.dumps(phase_state))
            state_path.chmod(0o600)

            # Journal
            mid = '550e8400-e29b-41d4-a716-446655440000'
            payload_dict = {
                'protocol_version': 1, 'message_id': mid, 'message_type': 'telemetry_batch',
                'gateway_id': 'gw-1', 'sensor_id': 'sensor-1', 'boot_id': 'boot-1',
                'first_sequence': 10, 'sample_count': 100, 'measured_at': '2026-10-28T00:00:00Z',
                'sample_interval_us': 10000, 'samples': [float(i) for i in range(100)]
            }
            raw_payload = json.dumps(payload_dict, separators=(',', ':')).encode()
            digest = hashlib.sha256(raw_payload).hexdigest()
            record = {
                'topic': 'gateways/gw-1/telemetry/sensor-1',
                'gateway_id': 'gw-1', 'sensor_id': 'sensor-1', 'message_id': mid,
                'boot_id': 'boot-1', 'first_sequence': '10', 'sample_count': 100,
                'payload_sha256': digest,
                'payload': base64.b64encode(raw_payload).decode('ascii')
            }
            journal = outputs / 'e4-journal.sqlite'
            journal.write_text(json.dumps(record) + '\n')
            journal.chmod(0o600)

            # 1. Export conflict probe
            probe_out = root / 'probe-exported'
            res = client._export_conflict_probe(ws, probe_out)
            self.assertEqual(res['cases'], 2)
            self.assertTrue((probe_out / 'conflict-probe.json').is_file())
            self.assertTrue((probe_out / 'results.json').is_file())

            # 2. Run conflict stage with mock transport
            def mock_transport(gw, sensor, pwd, orig_pay, cases):
                self.assertEqual(gw, 'gw-1')
                self.assertEqual(sensor, 'sensor-1')
                self.assertEqual(pwd, 'secret-pass')
                self.assertEqual(orig_pay, raw_payload)
                self.assertEqual(set(cases), {'same_id_changed_payload', 'different_first_sequence_overlap'})
                return {
                    name: {
                        'message_id': cases[name]['message_id'],
                        'payload_sha256': cases[name]['payload_sha256'],
                        'schema_valid': True,
                        'positive_control_ack_count': 1,
                        'conflict_ack_count': 0,
                        'raw_payload_b64': cases[name]['raw_payload_b64'],
                        'status': 'PASS'
                    }
                    for name in cases
                }

            stage_res = client._run_conflict_stage(ws, transport=mock_transport)
            self.assertEqual(stage_res['status'], 'PASS')
            self.assertTrue((runtime / 'e4-conflicts.json').is_file())

            # 3. Export conflict results
            results_out = root / 'results-exported'
            exp_res = client._export_conflict_results(ws, results_out)
            self.assertEqual(exp_res['phase'], 'conflicts')
            self.assertTrue((results_out / 'conflicts-proof.json').is_file())
            self.assertTrue((results_out / 'results.json').is_file())
            results_summary = json.loads((results_out / 'results.json').read_text())
            self.assertEqual(results_summary['phase'], 'conflicts')
            self.assertEqual(results_summary['proof']['path'], 'conflicts-proof.json')
