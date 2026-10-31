"""Self-contained unit tests for E5 remote A adapter and phase validation."""
import base64
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

_HERE = Path(__file__).resolve().parent
for _path in (str(_HERE), str(_HERE.parent)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import paper_p8_bundle
import paper_p8_e5_remote as remote
import paper_p8_e5_support as e5
import paper_p8_e4_support as e4
import paper_p8_remote as remote_network
from scripts import paper_p8_client as client
from scripts import paper_p8_e5 as e5_driver
from native_compose_smoke import paper_p8_e5_acceptance


def _make_test_jwt(sub, exp=None):
    if exp is None:
        exp = int(time.time()) + 7200
    header = base64.urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').decode().rstrip('=')
    payload = base64.urlsafe_b64encode(json.dumps({'sub': sub, 'exp': exp}).encode()).decode().rstrip('=')
    return f'{header}.{payload}.sig'


def _make_sample_dataset(dest_path):
    """Use the production E5 builder, including valid base64 MQTT payloads."""
    p = Path(dest_path)
    p.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    p.parent.chmod(0o700)
    built = e5.build_dataset(seed=813, sensor_ids=['sensor_stage3_uplink_shared', 'sensor_stage3_uplink_shared'],
                             anchor='2026-10-25T00:00:00Z', path=p)
    raw = p.read_bytes()
    rows = [json.loads(line) for line in raw.splitlines()]
    return raw, built['sha256'], [row['message_id'] for row in rows]


def _make_sample_context(dataset_path, dataset_bytes, dataset_sha256, command_keys=None):
    if command_keys is None:
        command_keys = e5.command_idempotency_keys(813)
    return {
        'dataset_path': str(dataset_path),
        'sha256': dataset_sha256,
        'bytes': dataset_bytes,
        'seed': 813,
        'anchor': '2026-10-25T00:00:00Z',
        'sensor_ids': ['sensor_stage3_uplink_shared', 'sensor_stage3_uplink_shared'],
        'gateway_ids': ['p8-e5-a', 'p8-e5-b'],
        'command_keys': command_keys,
        'viewer_subscriptions': [
            {'viewer_id': 'e5-viewer-a', 'user_id': 'user-a', 'gateway_id': 'p8-e5-a', 'sensor_id': 'sensor_stage3_uplink_shared'},
            {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b'},
            {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b', 'sensor_id': 'sensor_stage3_uplink_shared'},
        ],
    }


def _sample_admin_metrics(process_instance='proc-alpha', writes=0, events=0, drops=0, dedup_hits=0):
    return {
        'process_instance': process_instance,
        'ingress': {
            'Received': writes,
            'microbatch': {'groups': 1, 'members': 2, 'max_members': 2},
            'ShaperRateLimited': 0,
            'ShaperPassed': writes,
        },
        'ram_dedup': {
            'lookup_hit_total': dedup_hits,
            'database_calls_avoided_total': dedup_hits,
        },
        'command_dispatch': {
            'notification_signals_accepted': 4,
            'puback': 4,
            'scans': 4,
        },
        'websocket_metrics': {
            'availability': 'available',
            'subscriptions': 2,
            'submissions': events,
            'new_events': events,
            'fanout_indexed_selections': events,
            'fanout_scan_evaluations': 0,
            'telemetry_writes_completed': writes,
            'deliveries_enqueued': events,
            'drops': drops,
            'global_queued_work': 0,
            'global_queued_bytes': 0,
        },
    }


def _make_replay_projection(count, headers):
    return {
        'status': 'COMPLETE',
        'cleanup_errors': [],
        'e4': {
            'mode': 'replay',
            'journal_entry_count': count,
            'replayed_messages': count,
            'safe_header_samples': headers,
        },
        'telemetry': {
            'generated_unique': 0,
            'offered_unique': 0,
            'replay_offered': count,
            'replay_application_acks': count,
            'broker_pubacks': count + 2,
            'publish_attempts': count + 2,
            'retries': 2,
            'pending_messages': 0,
            'pending_bytes': 0,
        },
    }


def _verified_b_lifecycle():
    return {
        'source_verified': True,
        'binary_verified': True,
        'combined_binary_sha256': 'a' * 64,
        'paperload_binary_sha256': 'd' * 64,
        'gateway_binary_sha256': 'e' * 64,
        'paperload_binary_verified': True,
        'gateway_binary_verified': True,
        'combined_binary_verified': True,
        'actual_stop_confirmed': True,
        'runner_stopped': True,
        'processes_stopped': True,
        'measurement_windows': {'initial': 1, 'warm_replay': 2, 'control': 1},
        'idle_transfer_windows': [{'between': 'initial/warm_replay', 'excluded': True}],
    }


def _viewer_correlation():
    return {'session_id': 'session-a', 'run_id': 'run-a', 'binding_digest': 'b' * 64,
            'manifest_sha256': 'c' * 64, 'binary_sha256': 'd' * 64}


def _viewer_lifecycle(correlation=None):
    return {
        'schema_version': 1,
        'clock': 'viewer_holder_local_elapsed_seconds',
        'correlation': correlation or _viewer_correlation(),
        'ready_elapsed_seconds': 1.0,
        'checkpoints': [
            {'phase': phase, 'boundary': boundary, 'elapsed_seconds': elapsed,
             'child_alive': True, 'ready_verified': True}
            for (phase, boundary), elapsed in zip(remote._VIEWER_BOUNDARIES, (2, 4, 6, 8, 10, 12))
        ],
        'stop_elapsed_seconds': 14.0,
        'joined': True,
    }


def _viewer_stop_result(projection):
    projection.update({
        'status': 'COMPLETE',
        'planned_stop': True,
        'planned_stop_reason': 'stopped_by_operator',
        'connection_attempts': 2,
        'connected_distinct_viewers': 2,
        'subscribed_distinct_viewers': 2,
        'reconnections': 0,
    })
    return projection


def _header_samples(dataset_path, ids):
    rows = [json.loads(line) for line in Path(dataset_path).read_text().splitlines()]
    return [{key: row[key] for key in ('message_id', 'gateway_id', 'sensor_id', 'boot_id',
                                      'first_sequence', 'sample_count', 'payload_sha256')}
            for row in rows if row['message_id'] in set(ids)]


class FakeEndpoint:
    def __init__(self, private_dir):
        self.url = 'https://fixture-a.lan:14443'
        self.ca_file = Path(private_dir) / 'certs/ca.crt'


class FakeRegistry:
    def resolve(self, container_name):
        return {
            'NetworkSettings': {
                'Ports': {
                    '8883/tcp': [{'HostIp': '192.168.50.20', 'HostPort': '18884'}],
                    '443/tcp': [{'HostIp': '192.168.50.20', 'HostPort': '14443'}],
                }
            }
        }


class FakeSmoke:
    paper_p8_e5_acceptance = staticmethod(paper_p8_e5_acceptance)

    def __init__(self, root_dir):
        self.ROOT = Path(root_dir)

    def prepare_paper_workload(self, fixture, registry, endpoint, containers, images, state, gateway_ids):
        pg = 'fake-pg'
        quota = {'cpus': 2, 'memory_bytes': 4294967296}
        effective = {
            'services': {
                'backend': {
                    'environment': dict(e5.e5_arm_profile('optimized'))
                }
            }
        }
        resources = {'cpu': 1.0, 'memory_bytes': 1024 * 1024 * 100}
        storage = {'status': 'healthy'}
        tokens = {
            'admin': _make_test_jwt('admin'),
            'a': _make_test_jwt('user-a'),
            'b': _make_test_jwt('user-b'),
        }
        specs = [
            SimpleNamespace(gateway_id='p8-e5-a', sensor_id='sensor_stage3_uplink_shared', sensor_unit='degC',
                            sensor_urn='urn:ngsi-ld:Sensor:p8-e5-a:sensor_stage3_uplink_shared'),
            SimpleNamespace(gateway_id='p8-e5-b', sensor_id='sensor_stage3_uplink_shared', sensor_unit='m/s',
                            sensor_urn='urn:ngsi-ld:Sensor:p8-e5-b:sensor_stage3_uplink_shared'),
        ]
        passwords = ['pass-a', 'pass-b']
        return pg, quota, effective, resources, storage, tokens, specs, passwords, None

    def db_psql(self, registry, pg, sql, params=None):
        if 'schema_migrations' in sql:
            return '24:telemetry_ledger_timestamp'
        if 'SELECT count(*) FROM twin_command_requests WHERE idempotency_key=' in sql:
            return '0'
        if 'SELECT json_build_object' in sql and 'processed_messages' in sql:
            return json.dumps({
                'ledger': 0,
                'raw': 0,
                'temporal': 0,
                'configuration': 0,
            })
        if 'SELECT md5(concat_ws' in sql:
            return 'fingerprint-default'
        if 'SELECT json_build_object' in sql and 'twin_command_requests' in sql:
            key = params.get('key') if params else 'fake-key'
            return json.dumps({
                'idempotency_key': key,
                'status_code': 202,
                'command_id': str(uuid.uuid4()),
                'status': 'succeeded',
                'result_hash': 'sha256:' + 'a' * 64,
                'desired_version': '1',
                'sample_interval_us': 100000,
            })
        return ''

    def http_request(self, endpoint, method, path, token=None, body=None, timeout=None):
        if path == '/v1/admin/metrics':
            return 200, {}, _sample_admin_metrics()
        if method == 'PUT' and '/sensors/' in path:
            sid = path.split('/')[-1]
            gw = path.split('/')[3]
            return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:{gw}:{sid}'}
        if method == 'GET' and '/digital-twins/' in path:
            return 200, {}, {
                'desired_version': '1',
                'reported_state': {
                    'configuration': {'sample_interval_us': 100000, 'sensor_unit': 'degC'}
                }
            }
        return 404, {}, {}

    def paper_p0_stats(self, registry, containers, tag):
        return {'tag': tag, 'stats': 'ok'}

    def paper_p8_remote_support(self):
        return remote_network


class PaperP8E5RemoteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.private = self.root / 'private'
        self.private.mkdir(mode=0o700)
        self.certs = self.private / 'certs'
        self.certs.mkdir(mode=0o700)
        (self.certs / 'ca.crt').write_text('FAKE CA CERT')
        (self.certs / 'ca.crt').chmod(0o600)

        self.repeat_root = self.private / 'repeats'
        self.repeat_root.mkdir(mode=0o700)
        self.repeat_root.chmod(0o700)
        self.repeat_dir, self.context_file = e5_driver.prepare_repeat(self.repeat_root, 1, 813)
        self.context_dict = json.loads(self.context_file.read_text())
        self.dataset_file = Path(self.context_dict['dataset_path'])
        self.dataset_raw = self.dataset_file.read_bytes()
        self.dataset_sha256 = self.context_dict['sha256']
        self.msg_ids = [json.loads(line)['message_id'] for line in self.dataset_raw.splitlines()]

        self.smoke = FakeSmoke(_HERE.parent.parent)
        self.fixture = SimpleNamespace(
            private=self.private,
            project='test-project',
        )
        self.endpoint = FakeEndpoint(self.private)
        self.registry = FakeRegistry()
        self.containers = {'mosquitto': 'fake-mosquitto', 'nginx': 'fake-nginx'}
        self.images = {
            'backend': 'sha256:' + 'b' * 64,
            'gateway': 'sha256:' + 'c' * 64,
        }
        self.state = {}
        self.remote_profile = remote_network.validate_config({
            'schema_version': 1,
            'bind_ip': '192.168.50.20',
            'advertised_host': 'fixture-a.lan',
            'https_port': 14443,
            'mqtt_tls_port': 18884,
            'required_san_dns': ['fixture-a.lan', 'localhost', 'mosquitto', 'nginx'],
            'required_san_ips': ['127.0.0.1', '192.168.50.20'],
        })

    def _initial_headers(self):
        records = [json.loads(line) for line in self.dataset_raw.splitlines()]
        return [
            {
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
                'topic': f"gateways/{r['gateway_id']}/telemetry/{r['sensor_id']}",
            }
            for r in records
        ]

    def _viewer_identity_samples(self):
        records = [json.loads(line) for line in self.dataset_raw.splitlines()]
        r_a = records[0]
        r_b = records[1]
        def mk_sample(vid, r):
            canonical = json.dumps({
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
            }, separators=(',', ':')).encode('utf-8')
            return {
                'viewer_id': vid,
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
                'header_sha256': hashlib.sha256(canonical).hexdigest(),
            }
        return [mk_sample('e5-viewer-a', r_a), mk_sample('e5-viewer-b', r_b)]

    def test_prepare_validates_context_and_dataset_without_launching_processes(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        self.assertEqual(prepared['arm'], 'optimized')
        self.assertEqual(prepared['journal_sha256'], self.dataset_sha256)
        self.assertTrue(Path(prepared['dataset_path']).is_file())
        self.assertIn('portable_inputs', prepared)
        self.assertIn('manifests', prepared['portable_inputs'])
        self.assertEqual(prepared['completed_phases'], set())
        # Confirm no processes were spawned or tracked in prepared
        self.assertNotIn('procs', prepared)

    def test_remote_prepare_uses_validated_advertised_tls_server_name(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
            remote_profile=self.remote_profile,
        )
        for key in ('initial', 'warm_replay', 'viewer'):
            manifest = json.loads(Path(prepared['manifests'][key]).read_text())
            self.assertEqual(manifest['tls']['mqtt_tls_server_name'], 'fixture-a.lan')
            self.assertEqual(manifest['tls']['mqtt_tls_endpoint'], self.remote_profile['advertised_mqtt_tls'])
            self.assertEqual(manifest['tls']['https_endpoint'], self.remote_profile['advertised_https'])
            self.assertTrue(manifest['tls'].get('https_ca_file'))
            self.assertTrue(manifest['tls'].get('mqtt_tls_ca_file'))

        control_manifest = json.loads(Path(prepared['manifests']['control']).read_text())
        self.assertEqual(control_manifest['tls']['https_endpoint'], self.remote_profile['advertised_https'])
        self.assertTrue(control_manifest['tls'].get('https_ca_file'))
        self.assertFalse(control_manifest['tls'].get('mqtt_tls_endpoint'))
        self.assertFalse(control_manifest['tls'].get('mqtt_tls_server_name'))
        self.assertFalse(control_manifest['tls'].get('mqtt_tls_ca_file'))

        for env_path in prepared['manifests']['gateway_env']:
            env = json.loads(Path(env_path).read_text())
            self.assertEqual(env['GATEWAY_MQTT_SERVER_NAME'], 'fixture-a.lan')
            self.assertEqual(env['GATEWAY_MQTT_BROKER_URL'], 'ssl://fixture-a.lan:18884')

    def test_remote_prepare_control_manifest_omits_mqtt_fields_for_paperload_preflight(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
            remote_profile=self.remote_profile,
        )
        ctrl = json.loads(Path(prepared['manifests']['control']).read_text())
        tls = ctrl.get('tls', {})
        self.assertTrue(tls.get('https_endpoint'))
        self.assertTrue(tls.get('https_ca_file'))
        self.assertFalse(tls.get('mqtt_tls_endpoint'), f'control has mqtt_tls_endpoint: {tls}')
        self.assertFalse(tls.get('mqtt_tls_server_name'), f'control has mqtt_tls_server_name: {tls}')
        self.assertFalse(tls.get('mqtt_tls_ca_file'), f'control has mqtt_tls_ca_file: {tls}')

    def test_local_prepare_control_manifest_omits_mqtt_fields_for_paperload_preflight(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
            remote_profile=None,
        )
        ctrl = json.loads(Path(prepared['manifests']['control']).read_text())
        tls = ctrl.get('tls', {})
        self.assertTrue(tls.get('https_endpoint'))
        self.assertTrue(tls.get('https_ca_file'))
        self.assertFalse(tls.get('mqtt_tls_endpoint'), f'local control has mqtt_tls_endpoint: {tls}')
        self.assertFalse(tls.get('mqtt_tls_server_name'), f'local control has mqtt_tls_server_name: {tls}')
        self.assertFalse(tls.get('mqtt_tls_ca_file'), f'local control has mqtt_tls_ca_file: {tls}')

    def test_prepare_and_export_bundle_preserves_https_only_control_manifest_on_b(self):
        import scripts.paper_p8_bundle as bundle
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
            remote_profile=self.remote_profile,
        )
        bundle_dir = self.root / 'client-bundle'
        session_id = 'a' * 32
        exported = remote.export_prepared_bundle(
            prepared, output_leaf=str(bundle_dir), session_id=session_id,
        )
        self.assertGreater(exported['file_count'], 0)

        # Inspect exported bundle control manifest
        bundle_ctrl = json.loads((bundle_dir / 'manifests/control.json').read_text())
        b_tls = bundle_ctrl.get('tls', {})
        self.assertEqual(b_tls.get('https_endpoint'), self.remote_profile['advertised_https'])
        self.assertEqual(b_tls.get('https_ca_file'), 'certs/https-ca.pem')
        self.assertFalse(b_tls.get('mqtt_tls_endpoint'))
        self.assertFalse(b_tls.get('mqtt_tls_server_name'))
        self.assertFalse(b_tls.get('mqtt_tls_ca_file'))

        # Bind to B workspace and verify bound runtime control manifest
        ws_dir = self.root / 'b-workspace'
        bound = bundle.bind_bundle(
            bundle_root=bundle_dir, workspace_leaf=ws_dir, elapsed_transfer_wait_seconds=2,
        )
        self.assertEqual(bound['session']['session_id'], session_id)

        bound_ctrl = json.loads((ws_dir / 'runtime/manifests/control.json').read_text())
        runtime_tls = bound_ctrl.get('tls', {})
        self.assertEqual(runtime_tls.get('https_endpoint'), self.remote_profile['advertised_https'])
        self.assertEqual(runtime_tls.get('https_ca_file'), str((ws_dir / 'inputs/certs/https-ca.pem').resolve()))
        self.assertFalse(runtime_tls.get('mqtt_tls_endpoint'))
        self.assertFalse(runtime_tls.get('mqtt_tls_server_name'))
        self.assertFalse(runtime_tls.get('mqtt_tls_ca_file'))

    def test_prepare_rejects_invalid_arm_or_reference_rules(self):
        with self.assertRaisesRegex(ValueError, 'e5_arm_rejected'):
            remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='unknown_arm', context_path=self.context_file,
            )
        # Optimized with reference path must be rejected
        with self.assertRaisesRegex(ValueError, 'paper_p8_e5_reference_rejected'):
            remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='optimized', context_path=self.context_file,
                reference_path='/some/reference.json',
            )
        # Ablation without reference path must be rejected
        with self.assertRaisesRegex(ValueError, 'e5_reference_required'):
            remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='shaper_off', context_path=self.context_file,
                reference_path=None,
            )

    def test_prepare_rejects_hash_mismatch_or_non_private_context(self):
        bad_context = self.private / 'bad_context.json'
        bad_dict = dict(self.context_dict, sha256='0' * 64)
        bad_context.write_text(json.dumps(bad_dict))
        bad_context.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'e5_dataset_context_hash_mismatch'):
            remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='optimized', context_path=bad_context,
            )

    def test_begin_remote_phase_sequencing_and_snapshots(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        # Cannot start warm_replay or control before initial
        with self.assertRaisesRegex(ValueError, 'initial_required_before_warm_replay'):
            remote.begin_remote_phase(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, phase='warm_replay',
            )

        # Successfully begin initial
        snap = remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='initial',
        )
        self.assertEqual(snap['phase'], 'initial')
        self.assertIn('admin_before', snap)
        self.assertIn('db_before', snap)

        # Cannot begin initial again
        prepared['completed_phases'].add('initial')
        with self.assertRaisesRegex(ValueError, 'e5_phase_already_completed: initial'):
            remote.begin_remote_phase(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, phase='initial',
            )

    def test_finish_remote_initial_validates_20_acks_and_exact_headers(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='initial',
        )

        b_proj = _make_replay_projection(20, _header_samples(self.dataset_file, self.msg_ids))

        # Mockcounts so delta is exactly ledger 20, raw 2000, temporal 20, config 0
        counts_before = [
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': 'fp-0'},
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': 'fp-0'},
        ]
        counts_after = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        prepared['phase_snapshots']['initial']['db_before'] = counts_before

        orig_counts = remote.e4._counts
        orig_verify_raw = remote.e4.verify_pilot_raw
        orig_admin = remote.e4._support._admin_metrics
        try:
            remote.e4._counts = lambda s, r, pg, spec: counts_after[0] if spec.gateway_id == 'p8-e5-a' else counts_after[1]
            remote.e4.verify_pilot_raw = lambda s, r, pg, spec, message_ids: {'verified_exact': True, 'count': 1000}
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics(writes=20, events=20)

            res = remote.finish_remote_initial(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, b_proj,
            )
            self.assertEqual(res['phase'], 'initial')
            self.assertEqual(res['database_delta'], {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0})
            self.assertIn('initial', prepared['completed_phases'])
        finally:
            remote.e4._counts = orig_counts
            remote.e4.verify_pilot_raw = orig_verify_raw
            remote.e4._support._admin_metrics = orig_admin

    def test_finish_remote_initial_rejects_header_mismatch_or_non_unique(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='initial',
        )

        # 20 headers but not unique
        b_proj = _make_replay_projection(20, [{'message_id': 'msg-000'}] * 20)
        with self.assertRaisesRegex(ValueError, 'e5_initial_headers_not_unique'):
            remote.finish_remote_initial(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, b_proj,
            )

    def test_finish_remote_warm_replay_enforces_fingerprint_unchanged_and_no_new_ws(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].add('initial')
        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        prepared['phase_results']['initial'] = {'db_after': initial_db}

        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='warm_replay',
        )
        prepared['phase_snapshots']['warm_replay']['db_before'] = initial_db
        prepared['phase_snapshots']['warm_replay']['admin_before'] = _sample_admin_metrics(writes=20, events=20, dedup_hits=0)

        b_proj = _make_replay_projection(10, _header_samples(self.dataset_file, self.msg_ids[:10]))

        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            # Case 1: Success - db unchanged, no new events
            remote.e4._counts = lambda s, r, pg, spec: initial_db[0] if spec.gateway_id == 'p8-e5-a' else initial_db[1]
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics(writes=20, events=20, dedup_hits=10)

            res = remote.finish_remote_warm_replay(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, b_proj,
            )
            self.assertEqual(res['phase'], 'warm_replay')
            self.assertEqual(res['database_delta'], {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0})
            self.assertEqual(res['ws_window']['new_events'], 0)
        finally:
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_finish_remote_warm_replay_rejects_changed_fingerprint(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].add('initial')
        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        prepared['phase_results']['initial'] = {'db_after': initial_db}

        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='warm_replay',
        )

        b_proj = _make_replay_projection(10, _header_samples(self.dataset_file, self.msg_ids[:10]))

        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            # DB changed with new ledger rows!
            changed_db = [
                {'ledger': 15, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-CHANGED'},
                initial_db[1],
            ]
            remote.e4._counts = lambda s, r, pg, spec: changed_db[0] if spec.gateway_id == 'p8-e5-a' else changed_db[1]
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics()

            with self.assertRaisesRegex(ValueError, 'e5_warm_replay_changed_main_fingerprint'):
                remote.finish_remote_warm_replay(
                    self.smoke, self.fixture, self.endpoint, self.registry,
                    self.containers, prepared, b_proj,
                )
        finally:
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_finish_remote_control_verifies_succeeded_commands_receipts_and_configs(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay'])
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='control',
        )

        b_proj = {
            'status': 'COMPLETE',
            'commands': [
                {'idempotency_key': k, 'classification': 'succeeded'}
                for k in self.context_dict['command_keys']
            ],
            'executors': [
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
            ],
        }

        control_db = {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'}
        prepared['phase_snapshots']['control']['db_before'] = [control_db, control_db]
        prepared['phase_results']['initial'] = {'db_after': [control_db, control_db]}

        orig_verify_control = remote._verify_control_receipts
        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            remote._verify_control_receipts = lambda summaries, cmds, receipts, configs, sensors: True
            remote.e4._counts = lambda s, r, pg, spec: control_db
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics()

            res = remote.finish_remote_control(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, prepared, b_proj,
            )
            self.assertEqual(res['phase'], 'control')
            self.assertEqual(len(res['pg_receipts']), 4)
            self.assertEqual(len(res['rest_configs']), 2)
            self.assertIn('control', prepared['completed_phases'])
        finally:
            remote._verify_control_receipts = orig_verify_control
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_validate_continuous_viewer_verifies_events_and_readiness(self):
        initial_res = {
            'b_projection': {
                'e4': {'safe_header_samples': self._initial_headers()}
            }
        }
        viewer_proj = _viewer_stop_result({
            'status': 'COMPLETE',
            'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2,
            'receiving_distinct_viewers': 2,
            'unique_events': 20,
            'duplicates': 0,
            'wrong_namespace': 0,
            'viewers': [
                {'id': 'e5-viewer-a', 'unique_events': 10},
                {'id': 'e5-viewer-b', 'unique_events': 10},
            ],
            'identity_samples': self._viewer_identity_samples(),
        })
        init_ws = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
        warm_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        control_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        end_metrics = _sample_admin_metrics()

        res = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection={'ready_before_load': True, 'connected_distinct_viewers': 2,
                                  'subscribed_distinct_viewers': 2},
            lifecycle_proof=_viewer_lifecycle(), expected_correlation=_viewer_correlation(),
        )
        self.assertTrue(res['valid'])
        self.assertEqual(res['failures'], [])
        self.assertTrue(res['validation']['two_viewer_readiness'])
        self.assertEqual(res['validation']['lifecycle_clock'], 'viewer_holder_local_elapsed_seconds')
        self.assertEqual([w['duration_seconds'] for w in res['validation']['idle_windows_described']], [2.0, 2.0])

    def test_validate_continuous_viewer_accepts_two_sample_viewer_result_matching_go_contract(self):
        records = [json.loads(line) for line in self.dataset_raw.splitlines()]
        initial_headers = []
        for r in records:
            initial_headers.append({
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
                'topic': f"gateways/{r['gateway_id']}/telemetry/{r['sensor_id']}",
            })
        initial_res = {'b_projection': {'e4': {'safe_header_samples': initial_headers}}}

        # Go paperload viewer emits exactly one sample per viewer (2 total for 2 viewers)
        r_a = records[0]
        r_b = records[1]
        def mk_sample(vid, r):
            canonical = json.dumps({
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
            }, separators=(',', ':')).encode('utf-8')
            return {
                'viewer_id': vid,
                'gateway_id': r['gateway_id'],
                'sensor_id': r['sensor_id'],
                'message_id': r['message_id'],
                'boot_id': r['boot_id'],
                'first_sequence': str(r['first_sequence']),
                'sample_count': int(r['sample_count']),
                'header_sha256': hashlib.sha256(canonical).hexdigest(),
            }
        two_samples = [mk_sample('e5-viewer-a', r_a), mk_sample('e5-viewer-b', r_b)]

        viewer_proj = _viewer_stop_result({
            'status': 'COMPLETE',
            'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2,
            'receiving_distinct_viewers': 2,
            'unique_events': 20,
            'duplicates': 0,
            'wrong_namespace': 0,
            'viewers': [
                {'id': 'e5-viewer-a', 'unique_events': 10},
                {'id': 'e5-viewer-b', 'unique_events': 10},
            ],
            'identity_samples': two_samples,
        })
        init_ws = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
        warm_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        control_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        end_metrics = _sample_admin_metrics()

        res = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection={'ready_before_load': True, 'connected_distinct_viewers': 2,
                                  'subscribed_distinct_viewers': 2},
            lifecycle_proof=_viewer_lifecycle(), expected_correlation=_viewer_correlation(),
        )
        self.assertTrue(res['valid'], f"Expected valid, got failures: {res['failures']}")
        self.assertEqual(res['failures'], [])
        self.assertEqual(res['validation']['sampled_identity_provenance_qualification'],
                         'bounded_sample_one_per_viewer; not_exhaustive_event_enumeration')

        # Negative checks:
        ready = {'ready_before_load': True, 'connected_distinct_viewers': 2, 'subscribed_distinct_viewers': 2}
        proof = _viewer_lifecycle()
        corr = _viewer_correlation()

        # 1. Incomplete samples (only 1 viewer sampled)
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, identity_samples=[two_samples[0]]), self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_samples_incomplete_for_viewers', res['failures'])

        # 2. Duplicate sample for same viewer
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, identity_samples=[two_samples[0], two_samples[0]]), self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_sample_duplicate_per_viewer', res['failures'])

        # 3. Unknown viewer
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, identity_samples=[dict(two_samples[0], viewer_id='viewer-unknown'), two_samples[1]]),
            self.context_dict, initial_res, init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_sample_unknown_viewer', res['failures'])

        # 4. Mutated composite identity
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, identity_samples=[dict(two_samples[0], sensor_id='sensor-mutated'), two_samples[1]]),
            self.context_dict, initial_res, init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_sample_header_composite_mismatch', res['failures'])

        # 5. Mutated header hash
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, identity_samples=[dict(two_samples[0], header_sha256='0' * 64), two_samples[1]]),
            self.context_dict, initial_res, init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_sample_header_hash_mismatch', res['failures'])

        # 6. Aggregate count failure (19 unique events instead of 20)
        res = remote.validate_continuous_viewer(
            dict(viewer_proj, unique_events=19), self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr)
        self.assertFalse(res['valid'])
        self.assertIn('viewer_event_accounting', res['failures'])
        initial = {'b_projection': {'e4': {'safe_header_samples': self._initial_headers()}}}
        viewer = _viewer_stop_result({
            'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [{'id': 'e5-viewer-a', 'unique_events': 10}, {'id': 'e5-viewer-b', 'unique_events': 10}],
            'identity_samples': self._viewer_identity_samples(),
        })
        ws20 = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
        ws0 = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        ready = {'ready_before_load': True, 'connected_distinct_viewers': 2, 'subscribed_distinct_viewers': 2}
        args = (viewer, self.context_dict, initial, ws20, ws0, ws0, _sample_admin_metrics())
        good = _viewer_lifecycle()
        cases = [
            (None, 'viewer_lifecycle_proof_missing'),
            (dict(good, checkpoints=[*good['checkpoints'][:2],
                                    *[dict(p, child_alive=False) for p in good['checkpoints'][2:]]]),
             'viewer_lifecycle_child_not_ready'),
            (dict(good, checkpoints=good['checkpoints'][:2]), 'viewer_lifecycle_checkpoints_invalid'),
            (dict(good, checkpoints=[good['checkpoints'][1], *good['checkpoints'][1:]]), 'viewer_lifecycle_checkpoint_order_invalid'),
            (dict(good, checkpoints=[dict(good['checkpoints'][0], child_alive=False), *good['checkpoints'][1:]]), 'viewer_lifecycle_child_not_ready'),
            (dict(good, checkpoints=[dict(good['checkpoints'][0], elapsed_seconds=float('nan')), *good['checkpoints'][1:]]), 'viewer_lifecycle_clock_invalid'),
            (dict(good, correlation=dict(good['correlation'], run_id='other')), 'viewer_lifecycle_correlation_mismatch'),
            (dict(good, stop_elapsed_seconds=11), 'viewer_lifecycle_clock_order_invalid'),
        ]
        for proof, expected_failure in cases:
            with self.subTest(failure=expected_failure):
                result = remote.validate_continuous_viewer(
                    *args, readiness_projection=ready, lifecycle_proof=proof,
                    expected_correlation=_viewer_correlation())
                self.assertFalse(result['valid'])
                self.assertIn(expected_failure, result['failures'])
        churn = dict(viewer, reconnections=1)
        result = remote.validate_continuous_viewer(
            churn, *args[1:], readiness_projection=ready,
            lifecycle_proof=good, expected_correlation=_viewer_correlation())
        self.assertIn('viewer_planned_stop_invalid', result['failures'])
        cancelled = dict(viewer, planned_stop=False, planned_stop_reason='cancelled')
        result = remote.validate_continuous_viewer(
            cancelled, *args[1:], readiness_projection=ready,
            lifecycle_proof=good, expected_correlation=_viewer_correlation())
        self.assertIn('viewer_planned_stop_invalid', result['failures'])
        missing_reason = dict(viewer)
        del missing_reason['planned_stop_reason']
        result = remote.validate_continuous_viewer(
            missing_reason, *args[1:], readiness_projection=ready,
            lifecycle_proof=good, expected_correlation=_viewer_correlation())
        self.assertIn('viewer_planned_stop_invalid', result['failures'])
        with_error = dict(viewer, primary_error='ws_connection_failed')
        result = remote.validate_continuous_viewer(
            with_error, *args[1:], readiness_projection=ready,
            lifecycle_proof=good, expected_correlation=_viewer_correlation())
        self.assertIn('viewer_planned_stop_invalid', result['failures'])
        with_cleanup_error = dict(viewer, cleanup_errors=['child_cleanup_failed'])
        result = remote.validate_continuous_viewer(
            with_cleanup_error, *args[1:], readiness_projection=ready,
            lifecycle_proof=good, expected_correlation=_viewer_correlation())
        self.assertIn('viewer_planned_stop_invalid', result['failures'])

    def test_validate_continuous_viewer_rejects_incomplete_readiness_or_events(self):
        initial_res = {
            'b_projection': {
                'e4': {'safe_header_samples': [{'message_id': mid} for mid in self.msg_ids]}
            }
        }
        # Only 1 receiving viewer
        viewer_proj = {
            'status': 'COMPLETE',
            'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2,
            'receiving_distinct_viewers': 1,
            'unique_events': 19,
            'duplicates': 1,
            'wrong_namespace': 0,
            'viewers': [
                {'id': 'e5-viewer-a', 'unique_events': 10},
                {'id': 'e5-viewer-b', 'unique_events': 9},
            ],
            'identity_samples': [{'message_id': mid} for mid in self.msg_ids[:19]],
        }
        init_ws = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
        warm_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        control_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        end_metrics = _sample_admin_metrics()

        res = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, control_ws, end_metrics,
        )
        self.assertFalse(res['valid'])
        self.assertIn('viewer_readiness', res['failures'])
        self.assertIn('viewer_event_accounting', res['failures'])

    def test_finalize_and_native_acceptance_require_viewer_lifecycle_and_b_provenance(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay', 'control'])

        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        phase_results = {
            'initial': {
                'admin_before': _sample_admin_metrics(writes=0, events=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}] * 2,
                'db_after': initial_db,
                'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                'raw_proof': [{'verified_exact': True, 'count': 1000}, {'verified_exact': True, 'count': 1000}],
                'ingress_delta': {'available': True, 'delta': {'Received': 20}},
                'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'e4': {'mode': 'replay', 'journal_entry_count': 20, 'replayed_messages': 20, 'safe_header_samples': self._initial_headers()},
                    'telemetry': {'broker_pubacks': 20, 'pending_messages': 0},
                },
            },
            'warm_replay': {
                'admin_before': _sample_admin_metrics(writes=20, events=20, dedup_hits=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20, dedup_hits=10),
                'db_before': initial_db,
                'db_after': initial_db,
                'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                'ingress_delta': {'available': True, 'delta': {'Received': 0}},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'e4': {'mode': 'replay', 'journal_entry_count': 10, 'replayed_messages': 10, 'safe_header_samples': self._initial_headers()[:10]},
                    'telemetry': {'broker_pubacks': 10, 'pending_messages': 0},
                },
            },
            'control': {
                'admin_before': _sample_admin_metrics(writes=20, events=20),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': initial_db,
                'db_after': initial_db,
                'pg_receipts': {k: {'status': 'succeeded', 'result_hash': 'abc'} for k in self.context_dict['command_keys']},
                'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
                'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4, 'verified_configurations': 2},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
                },
            },
        }

        viewer = _viewer_stop_result({
            'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [{'id': 'e5-viewer-a', 'unique_events': 10}, {'id': 'e5-viewer-b', 'unique_events': 10}],
            'identity_samples': self._viewer_identity_samples(),
        })
        initial_ws = phase_results['initial']['ws_window']
        zero_ws = phase_results['warm_replay']['ws_window']
        continuous_viewer = remote.validate_continuous_viewer(
            viewer, self.context_dict, phase_results['initial'], initial_ws, zero_ws,
            phase_results['control']['ws_window'], _sample_admin_metrics(),
            readiness_projection={'ready_before_load': True, 'connected_distinct_viewers': 2,
                                  'subscribed_distinct_viewers': 2},
            lifecycle_proof=_viewer_lifecycle(), expected_correlation=_viewer_correlation())
        self.assertTrue(continuous_viewer['valid'])
        b_declared = _verified_b_lifecycle()
        b_declared.update({'source_verified': True, 'binary_verified': True,
                           'combined_binary_sha256': 'a' * 64,
                           'paperload_binary_sha256': 'd' * 64,
                           'gateway_binary_sha256': 'e' * 64,
                           'paperload_binary_verified': True,
                           'gateway_binary_verified': True,
                           'combined_binary_verified': True})

        report = remote.finalize(
            self.smoke, self.fixture, prepared,
            phase_results, continuous_viewer,
            b_declared=b_declared, cleanup_verified=True,
            expected_correlation=_viewer_correlation(),
        )
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
        self.assertFalse(report['full_p8_accepted'])
        self.assertEqual(report['physical_ab'], 'NOT RUN')
        self.assertTrue(report['cleanup_verified'])
        self.assertEqual(report['metadata']['idle_transfer_windows'][0]['clock'],
                         'viewer_holder_local_elapsed_seconds')
        self.assertEqual(report['metadata']['paperload_binary_sha256'], 'd' * 64)
        self.assertEqual(report['metadata']['gateway_binary_sha256'], 'e' * 64)
        self.assertEqual(report['metadata']['combined_binary_sha256'], 'a' * 64)

        # Verify against native_compose_smoke acceptance validator
        acceptance_ok = paper_p8_e5_acceptance(report, {'arm': 'optimized'}, cleanup_verified=True)
        self.assertTrue(acceptance_ok, report)

        missing_provenance = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, continuous_viewer,
            cleanup_verified=True, expected_correlation=_viewer_correlation())
        self.assertEqual(missing_provenance['status'], 'WORKLOAD_COMPLETE_PENDING_REMOTE_PROVENANCE')
        self.assertIn('remote_lifecycle_proof_missing', missing_provenance['validation']['failures'])
        mismatched_binary = dict(b_declared, paperload_binary_sha256='f' * 64)
        mismatch_report = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, continuous_viewer,
            b_declared=mismatched_binary, cleanup_verified=True,
            expected_correlation=_viewer_correlation())
        self.assertEqual(mismatch_report['status'], 'FAIL')
        missing_paperload_hash = dict(b_declared)
        missing_paperload_hash.pop('paperload_binary_sha256')
        missing_report = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, continuous_viewer,
            b_declared=missing_paperload_hash, cleanup_verified=True,
            expected_correlation=_viewer_correlation())
        self.assertEqual(missing_report['status'], 'FAIL')
        swapped_correlation = dict(_viewer_correlation(), binary_sha256='a' * 64)
        swapped_report = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, continuous_viewer,
            b_declared=b_declared, cleanup_verified=True,
            expected_correlation=swapped_correlation)
        self.assertEqual(swapped_report['status'], 'FAIL')
        unverified_gateway = dict(b_declared, gateway_binary_verified=False)
        unverified_report = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, continuous_viewer,
            b_declared=unverified_gateway, cleanup_verified=True,
            expected_correlation=_viewer_correlation())
        self.assertEqual(unverified_report['status'], 'FAIL')

    def test_finalize_fails_when_proof_missing_even_with_cleanup_true(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        # Empty phase results (missing proof)
        continuous_viewer = {'viewer': {}, 'validation': {'valid': False, 'failures': ['viewer_incomplete']}}
        report = remote.finalize(
            self.smoke, self.fixture, prepared,
            {}, continuous_viewer,
            cleanup_verified=True,
        )
        self.assertEqual(report['status'], 'FAIL')
        self.assertFalse(report['validation']['valid'])
        acceptance_ok = paper_p8_e5_acceptance(report, {'arm': 'optimized'}, cleanup_verified=True)
        self.assertFalse(acceptance_ok)

    def test_ablation_prepare_with_valid_optimized_reference(self):
        # 1. First build a valid optimized report using finalize
        prepared_opt = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared_opt['completed_phases'].update(['initial', 'warm_replay', 'control'])

        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        phase_results = {
            'initial': {
                'admin_before': _sample_admin_metrics(writes=0, events=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}] * 2,
                'db_after': initial_db,
                'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                'raw_proof': [{'verified_exact': True, 'count': 1000}, {'verified_exact': True, 'count': 1000}],
                'ingress_delta': {'available': True, 'delta': {'Received': 20}},
                'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 20, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids]},
                    'telemetry': {'broker_pubacks': 20, 'pending_messages': 0},
                },
            },
            'warm_replay': {
                'admin_before': _sample_admin_metrics(writes=20, events=20, dedup_hits=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20, dedup_hits=10),
                'db_before': initial_db,
                'db_after': initial_db,
                'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                'ingress_delta': {'available': True, 'delta': {'Received': 0}},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 10, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids[:10]]},
                    'telemetry': {'broker_pubacks': 10, 'pending_messages': 0},
                },
            },
            'control': {
                'admin_before': _sample_admin_metrics(writes=20, events=20),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': initial_db,
                'db_after': initial_db,
                'pg_receipts': {k: {'status': 'succeeded', 'result_hash': 'abc'} for k in self.context_dict['command_keys']},
                'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
                'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4, 'verified_configurations': 2},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
                },
            },
        }
        continuous_viewer = {
            'viewer': {
                'status': 'COMPLETE',
                'connected_distinct_viewers': 2,
                'subscribed_distinct_viewers': 2,
                'receiving_distinct_viewers': 2,
                'unique_events': 20,
            },
            'validation': {
                'valid': True,
                'failures': [],
                'two_viewer_readiness': True,
            },
        }
        opt_report = remote.finalize(
            self.smoke, self.fixture, prepared_opt,
            phase_results, continuous_viewer,
            cleanup_verified=True,
        )
        self.assertEqual(opt_report['status'], 'FAIL')

        ref_file = self.private / 'p8-e5-optimized.json'
        ref_file.write_text(json.dumps(opt_report))
        ref_file.chmod(0o600)

        # 2. Mock prepare_paper_workload to return shaper_off effective profile
        orig_prepare_wl = self.smoke.prepare_paper_workload
        try:
            shaper_off_profile = dict(e5.e5_arm_profile('shaper_off'))
            def _shaper_off_wl(*args, **kwargs):
                pg, quota, effective, resources, storage, tokens, specs, passwords, _ = orig_prepare_wl(*args, **kwargs)
                effective['services']['backend']['environment'] = shaper_off_profile
                return pg, quota, effective, resources, storage, tokens, specs, passwords, None
            self.smoke.prepare_paper_workload = _shaper_off_wl

            with self.assertRaisesRegex(ValueError, 'e5_reference_artifact_rejected'):
                remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='shaper_off', context_path=self.context_file,
                reference_path=ref_file,
                )
        finally:
            self.smoke.prepare_paper_workload = orig_prepare_wl

    def test_ablation_prepare_rejects_reference_image_mismatch(self):
        # 1. First build a valid optimized report using finalize
        prepared_opt = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared_opt['completed_phases'].update(['initial', 'warm_replay', 'control'])

        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        phase_results = {
            'initial': {
                'admin_before': _sample_admin_metrics(writes=0, events=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}] * 2,
                'db_after': initial_db,
                'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                'raw_proof': [{'verified_exact': True, 'count': 1000}, {'verified_exact': True, 'count': 1000}],
                'ingress_delta': {'available': True, 'delta': {'Received': 20}},
                'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 20, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids]},
                    'telemetry': {'broker_pubacks': 20, 'pending_messages': 0},
                },
            },
            'warm_replay': {
                'admin_before': _sample_admin_metrics(writes=20, events=20, dedup_hits=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20, dedup_hits=10),
                'db_before': initial_db,
                'db_after': initial_db,
                'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                'ingress_delta': {'available': True, 'delta': {'Received': 0}},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 10, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids[:10]]},
                    'telemetry': {'broker_pubacks': 10, 'pending_messages': 0},
                },
            },
            'control': {
                'admin_before': _sample_admin_metrics(writes=20, events=20),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': initial_db,
                'db_after': initial_db,
                'pg_receipts': {k: {'status': 'succeeded', 'result_hash': 'abc'} for k in self.context_dict['command_keys']},
                'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
                'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4, 'verified_configurations': 2},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
                },
            },
        }
        continuous_viewer = {
            'viewer': {
                'status': 'COMPLETE',
                'connected_distinct_viewers': 2,
                'subscribed_distinct_viewers': 2,
                'receiving_distinct_viewers': 2,
                'unique_events': 20,
            },
            'validation': {
                'valid': True,
                'failures': [],
                'two_viewer_readiness': True,
            },
        }
        opt_report = remote.finalize(
            self.smoke, self.fixture, prepared_opt,
            phase_results, continuous_viewer,
            cleanup_verified=True,
        )
        self.assertEqual(opt_report['status'], 'FAIL')

        # Change backend image ID to a different valid hex
        mismatch_report = copy.deepcopy(opt_report)
        mismatch_report['image_ids']['backend'] = 'sha256:' + 'a' * 64
        mismatch_report['metadata']['image_ids']['backend'] = 'sha256:' + 'a' * 64

        ref_file = self.private / 'p8-e5-mismatch-images.json'
        ref_file.write_text(json.dumps(mismatch_report))
        ref_file.chmod(0o600)

        with self.assertRaisesRegex(ValueError, 'e5_reference_artifact_rejected'):
            remote.prepare(
                self.smoke, self.fixture, self.endpoint, self.registry,
                self.containers, self.images, self.state,
                arm='shaper_off', context_path=self.context_file,
                reference_path=ref_file,
            )

    def test_finalize_fanout_readiness_stays_inconclusive_when_viewer_readiness_not_proven(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay', 'control'])

        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        phase_results = {
            'initial': {
                'admin_before': _sample_admin_metrics(writes=0, events=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}] * 2,
                'db_after': initial_db,
                'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                'raw_proof': [{'verified_exact': True, 'count': 1000}, {'verified_exact': True, 'count': 1000}],
                'ingress_delta': {'available': True, 'delta': {'Received': 20}},
                'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 20, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids]},
                    'telemetry': {'broker_pubacks': 20, 'pending_messages': 0},
                },
            },
            'warm_replay': {
                'admin_before': _sample_admin_metrics(writes=20, events=20, dedup_hits=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20, dedup_hits=10),
                'db_before': initial_db,
                'db_after': initial_db,
                'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                'ingress_delta': {'available': True, 'delta': {'Received': 0}},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'e4': {'mode': 'replay', 'replayed_messages': 10, 'safe_header_samples': [{'message_id': m} for m in self.msg_ids[:10]]},
                    'telemetry': {'broker_pubacks': 10, 'pending_messages': 0},
                },
            },
            'control': {
                'admin_before': _sample_admin_metrics(writes=20, events=20),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': initial_db,
                'db_after': initial_db,
                'pg_receipts': {k: {'status': 'succeeded', 'result_hash': 'abc'} for k in self.context_dict['command_keys']},
                'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
                'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4, 'verified_configurations': 2},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
                },
            },
        }
        # Viewer readiness is NOT proven
        continuous_viewer = {
            'viewer': {'status': 'COMPLETE'},
            'validation': {
                'valid': True,
                'failures': [],
                'two_viewer_readiness': False,
            },
        }
        report = remote.finalize(
            self.smoke, self.fixture, prepared,
            phase_results, continuous_viewer,
            cleanup_verified=True,
        )
        # Mechanism activation must be INCONCLUSIVE because fanout readiness is unverified
        self.assertEqual(report['mechanisms']['activation_status'], 'INCONCLUSIVE')
        self.assertEqual(report['mechanisms']['fanout'].get('status'), 'INCONCLUSIVE')
        self.assertEqual(report['mechanisms']['fanout'].get('reason'), 'two_viewer_readiness_unverified')

    def test_finish_remote_control_rejects_pending_sqlite_rows(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay'])
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='control',
        )

        b_proj = {
            'status': 'COMPLETE',
            'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
            'executors': [
                {'pending_outbox': 1, 'receipts': [], 'configuration': {}},
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
            ],
        }

        control_db = {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'}
        prepared['phase_snapshots']['control']['db_before'] = [control_db, control_db]

        orig_verify_control = remote._verify_control_receipts
        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            remote._verify_control_receipts = lambda summaries, cmds, receipts, configs, sensors: True
            remote.e4._counts = lambda s, r, pg, spec: control_db
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics()

            with self.assertRaisesRegex(ValueError, 'e5_control_sqlite_executor_has_pending_rows'):
                remote.finish_remote_control(
                    self.smoke, self.fixture, self.endpoint, self.registry,
                    self.containers, prepared, b_proj,
                )
        finally:
            remote._verify_control_receipts = orig_verify_control
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_finish_remote_control_rejects_missing_pg_receipt(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay'])
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='control',
        )

        b_proj = {
            'status': 'COMPLETE',
            'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
            'executors': [
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
            ],
        }

        control_db = {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'}
        prepared['phase_snapshots']['control']['db_before'] = [control_db, control_db]

        orig_db_psql = self.smoke.db_psql
        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            # db_psql returns empty string for twin_command_requests
            def _empty_psql(registry, pg, sql, params=None):
                if 'SELECT json_build_object' in sql and 'twin_command_requests' in sql:
                    return ''
                return orig_db_psql(registry, pg, sql, params)
            self.smoke.db_psql = _empty_psql

            remote.e4._counts = lambda s, r, pg, spec: control_db
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics()

            with self.assertRaisesRegex(ValueError, 'e5_command_receipt_missing'):
                remote.finish_remote_control(
                    self.smoke, self.fixture, self.endpoint, self.registry,
                    self.containers, prepared, b_proj,
                )
        finally:
            self.smoke.db_psql = orig_db_psql
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_validate_continuous_viewer_accepts_control_global_new_events_and_rejects_auxiliary_deliveries(self):
        initial_res = {
            'b_projection': {
                'e4': {'safe_header_samples': self._initial_headers()}
            }
        }
        viewer_proj = _viewer_stop_result({
            'status': 'COMPLETE',
            'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2,
            'receiving_distinct_viewers': 2,
            'unique_events': 20,
            'duplicates': 0,
            'wrong_namespace': 0,
            'viewers': [
                {'id': 'e5-viewer-a', 'unique_events': 10},
                {'id': 'e5-viewer-b', 'unique_events': 10},
            ],
            'identity_samples': self._viewer_identity_samples(),
        })
        init_ws = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
        warm_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        control_ws_global = {'submissions': 4, 'new_events': 4, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
        end_metrics = _sample_admin_metrics()
        ready = {'ready_before_load': True, 'connected_distinct_viewers': 2, 'subscribed_distinct_viewers': 2}
        proof = _viewer_lifecycle()
        corr = _viewer_correlation()

        # Positive: global new_events=4 during control is preserved and passes
        res = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, control_ws_global, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertTrue(res['valid'])
        self.assertEqual(res['failures'], [])
        self.assertEqual(res['validation'].get('control_global_new_events'), 4)
        self.assertEqual(res['validation'].get('control_ws_window_scope'), 'process_global')

        # Negative: auxiliary event delivery enqueued to viewer during control fails
        bad_control_deliv = dict(control_ws_global, deliveries_enqueued=1)
        res_deliv = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, bad_control_deliv, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_deliv['valid'])
        self.assertIn('viewer_ws_control_window', res_deliv['failures'])

        # Negative: auxiliary event telemetry write completed to viewer during control fails
        bad_control_writes = dict(control_ws_global, telemetry_writes_completed=1)
        res_writes = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, bad_control_writes, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_writes['valid'])
        self.assertIn('viewer_ws_control_window', res_writes['failures'])

        # Negative: drops during control fails
        bad_control_drops = dict(control_ws_global, drops=1)
        res_drops = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, bad_control_drops, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_drops['valid'])
        self.assertIn('viewer_ws_control_window', res_drops['failures'])

        # Negative: missing counters in control window fails
        for missing_key in ('new_events', 'telemetry_writes_completed', 'deliveries_enqueued', 'drops'):
            bad_missing = dict(control_ws_global)
            del bad_missing[missing_key]
            res_missing = remote.validate_continuous_viewer(
                viewer_proj, self.context_dict, initial_res,
                init_ws, warm_ws, bad_missing, end_metrics,
                readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
            )
            self.assertFalse(res_missing['valid'])
            self.assertIn('viewer_ws_control_window', res_missing['failures'])

        # Negative: control window None fails
        res_none = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, warm_ws, None, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_none['valid'])
        self.assertIn('viewer_ws_control_window', res_none['failures'])

        # Negative: initial window mismatch not weakened
        bad_init = dict(init_ws, new_events=19)
        res_init = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            bad_init, warm_ws, control_ws_global, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_init['valid'])
        self.assertIn('viewer_ws_initial_window', res_init['failures'])

        # Negative: warm window mismatch not weakened
        bad_warm = dict(warm_ws, new_events=1)
        res_warm = remote.validate_continuous_viewer(
            viewer_proj, self.context_dict, initial_res,
            init_ws, bad_warm, control_ws_global, end_metrics,
            readiness_projection=ready, lifecycle_proof=proof, expected_correlation=corr,
        )
        self.assertFalse(res_warm['valid'])
        self.assertIn('viewer_ws_warm_window', res_warm['failures'])

    def test_finish_remote_control_and_finalize_reject_changed_main_fingerprint(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        prepared['completed_phases'].update(['initial', 'warm_replay'])
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='control',
        )

        b_proj = {
            'status': 'COMPLETE',
            'commands': [
                {'idempotency_key': k, 'classification': 'succeeded'}
                for k in self.context_dict['command_keys']
            ],
            'executors': [
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
                {'pending_outbox': 0, 'receipts': [], 'configuration': {}},
            ],
        }

        control_db = {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'}
        prepared['phase_snapshots']['control']['db_before'] = [control_db, control_db]
        prepared['phase_results']['initial'] = {'db_after': [control_db, control_db]}

        orig_verify_control = remote._verify_control_receipts
        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            remote._verify_control_receipts = lambda summaries, cmds, receipts, configs, sensors: True
            # Simulate unexpected mutation of main dataset during control phase!
            changed_db = {'ledger': 11, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-MUTATED'}
            remote.e4._counts = lambda s, r, pg, spec: changed_db
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics()

            with self.assertRaisesRegex(ValueError, 'e5_control_main_dataset_changed'):
                remote.finish_remote_control(
                    self.smoke, self.fixture, self.endpoint, self.registry,
                    self.containers, prepared, b_proj,
                )
        finally:
            remote._verify_control_receipts = orig_verify_control
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

        # Test that finalize also catches changed main dataset fingerprint in control_res
        initial_db = [
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'},
            {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-2'},
        ]
        phase_results = {
            'initial': {
                'admin_before': _sample_admin_metrics(writes=0, events=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20),
                'db_before': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}] * 2,
                'db_after': initial_db,
                'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                'raw_proof': [{'verified_exact': True, 'count': 1000}, {'verified_exact': True, 'count': 1000}],
                'ingress_delta': {'available': True, 'delta': {'Received': 20}},
                'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'e4': {'mode': 'replay', 'journal_entry_count': 20, 'replayed_messages': 20, 'safe_header_samples': self._initial_headers()},
                    'telemetry': {'broker_pubacks': 20, 'pending_messages': 0},
                },
            },
            'warm_replay': {
                'admin_before': _sample_admin_metrics(writes=20, events=20, dedup_hits=0),
                'admin_after': _sample_admin_metrics(writes=20, events=20, dedup_hits=10),
                'db_before': initial_db,
                'db_after': initial_db,
                'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                'ingress_delta': {'available': True, 'delta': {'Received': 0}},
                'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'e4': {'mode': 'replay', 'journal_entry_count': 10, 'replayed_messages': 10, 'safe_header_samples': self._initial_headers()[:10]},
                    'telemetry': {'broker_pubacks': 10, 'pending_messages': 0},
                },
            },
            'control': {
                'admin_before': _sample_admin_metrics(writes=20, events=20),
                'admin_after': _sample_admin_metrics(writes=20, events=24),
                'db_before': initial_db,
                'db_after': [
                    {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-MUTATED'},
                    initial_db[1],
                ],
                'pg_receipts': {k: {'status': 'succeeded', 'result_hash': 'abc'} for k in self.context_dict['command_keys']},
                'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
                'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4, 'verified_configurations': 2},
                'ws_window': {'new_events': 4, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0},
                'b_projection': {
                    'status': 'COMPLETE', 'cleanup_errors': [],
                    'commands': [{'idempotency_key': k, 'classification': 'succeeded'} for k in self.context_dict['command_keys']],
                },
            },
        }
        viewer = _viewer_stop_result({
            'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [{'id': 'e5-viewer-a', 'unique_events': 10}, {'id': 'e5-viewer-b', 'unique_events': 10}],
            'identity_samples': self._viewer_identity_samples(),
        })
        cv = remote.validate_continuous_viewer(
            viewer, self.context_dict, phase_results['initial'],
            phase_results['initial']['ws_window'], phase_results['warm_replay']['ws_window'],
            phase_results['control']['ws_window'], _sample_admin_metrics(),
            readiness_projection={'ready_before_load': True, 'connected_distinct_viewers': 2, 'subscribed_distinct_viewers': 2},
            lifecycle_proof=_viewer_lifecycle(), expected_correlation=_viewer_correlation(),
        )
        prepared['completed_phases'].update(['initial', 'warm_replay', 'control'])
        b_declared = _verified_b_lifecycle()
        rep = remote.finalize(
            self.smoke, self.fixture, prepared, phase_results, cv,
            b_declared=b_declared, cleanup_verified=True, expected_correlation=_viewer_correlation(),
        )
        self.assertEqual(rep['status'], 'FAIL')
        self.assertIn('control_changed_main_fingerprint', rep['validation']['failures'])

    def test_finish_remote_initial_detects_server_restart(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        remote.begin_remote_phase(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, prepared, phase='initial',
        )

        b_proj = _make_replay_projection(20, _header_samples(self.dataset_file, self.msg_ids))

        counts_before = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': 'fp-0'}] * 2
        prepared['phase_snapshots']['initial']['db_before'] = counts_before

        orig_counts = remote.e4._counts
        orig_admin = remote.e4._support._admin_metrics
        try:
            remote.e4._counts = lambda s, r, pg, spec: {'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0, 'fingerprint': 'fp-1'}
            # Process restarted!
            remote.e4._support._admin_metrics = lambda s, ep, tok: _sample_admin_metrics(process_instance='proc-NEW-RESTARTED')

            with self.assertRaisesRegex(ValueError, 'e5_server_process_restarted_during_initial'):
                remote.finish_remote_initial(
                    self.smoke, self.fixture, self.endpoint, self.registry,
                    self.containers, prepared, b_proj,
                )
        finally:
            remote.e4._counts = orig_counts
            remote.e4._support._admin_metrics = orig_admin

    def test_export_prepared_bundle_bind_and_preflight_flow(self):
        prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, self.state,
            arm='optimized', context_path=self.context_file,
        )
        bundle_dir = Path(self.private) / 'exported-bundle'
        export_result = remote.export_prepared_bundle(
            prepared, output_leaf=bundle_dir, deadline_seconds=900,
        )
        self.assertTrue((bundle_dir / 'bundle.json').is_file())
        metadata = json.loads((bundle_dir / 'bundle.json').read_text())
        self.assertEqual(metadata['session']['experiment'], 'E5')
        self.assertEqual(metadata['session']['arm'], 'optimized')
        self.assertIn('contexts/e5-context.json', [row['path'] for row in metadata['inputs']])
        self.assertIn('datasets/dataset.jsonl', [row['path'] for row in metadata['inputs']])
        self.assertIn('execution/e5-phase-plan.json', [row['path'] for row in metadata['inputs']])

        # Bind into workspace
        workspace_dir = Path(self.private) / 'bound-workspace'
        bind_result = paper_p8_bundle.bind_bundle(bundle_root=bundle_dir, workspace_leaf=workspace_dir)
        self.assertEqual(bind_result['manifest_count'], 4)
        self.assertEqual(bind_result['executor_count'], 2)
        self.assertTrue(Path(bind_result['e5_context']).is_file())
        self.assertTrue(Path(bind_result['e5_dataset']).is_file())
        self.assertTrue(Path(bind_result['e5_phase_plan']).is_file())

        # Validate workspace with client._workspace
        root = client._workspace(workspace_dir)
        self.assertEqual(root, workspace_dir.resolve())

        # Client preflight runs all 4 child manifests and validates 2 gateways without network or writing files
        fake_binary = Path(self.private) / 'fake-paperload'
        fake_binary.write_text('#!/bin/sh\nexit 0\n')
        fake_binary.chmod(0o700)
        with patch.object(client.paper_p8, 'preflight', return_value=Mock(stdout='ok')) as mock_preflight:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = client.main(['preflight', '--workspace', str(workspace_dir), '--binary', str(fake_binary)])
            self.assertEqual(code, 0)
            self.assertEqual(mock_preflight.call_count, 4)

        # Ensure no output directories or sqlite files created by preflight
        self.assertEqual(list((workspace_dir / 'outputs/outbox').iterdir()), [])
        run_spec = json.loads((workspace_dir / 'runtime/execution/run-spec.json').read_text())
        self.assertTrue(all(not (workspace_dir / job['output_dir']).exists() for job in run_spec['paperload']))

        # Negative: client run requires explicit phase
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = client.main(['run', '--workspace', str(workspace_dir), '--binary', str(fake_binary)])
        self.assertEqual(code, 1)
        self.assertIn('E5 run requires explicit --phase initial|warm_replay|control', stderr.getvalue())

        # Negative: client export requires explicit phase
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = client.main(['export-results', '--workspace', str(workspace_dir), '--output', str(Path(self.private) / 'out')])
        self.assertEqual(code, 1)
        self.assertIn('E5 export requires explicit --phase initial|warm_replay|control', stderr.getvalue())

        # Negative: tampered context
        tampered_ws = Path(self.private) / 'tampered-context-ws'
        paper_p8_bundle.bind_bundle(bundle_root=bundle_dir, workspace_leaf=tampered_ws)
        (tampered_ws / 'runtime/e5-context.json').write_text(json.dumps({'tampered': True}))
        with self.assertRaisesRegex(ValueError, 'bound e5 context provenance mismatch'):
            client._workspace(tampered_ws)

        # Negative: tampered phase plan
        tampered_plan_ws = Path(self.private) / 'tampered-plan-ws'
        paper_p8_bundle.bind_bundle(bundle_root=bundle_dir, workspace_leaf=tampered_plan_ws)
        (tampered_plan_ws / 'runtime/e5-phase-plan.json').write_text(json.dumps({'tampered': True}))
        with self.assertRaisesRegex(ValueError, 'bound e5 phase plan provenance mismatch'):
            client._workspace(tampered_plan_ws)

        # Negative: tampered dataset
        tampered_ds_ws = Path(self.private) / 'tampered-ds-ws'
        paper_p8_bundle.bind_bundle(bundle_root=bundle_dir, workspace_leaf=tampered_ds_ws)
        (tampered_ds_ws / 'inputs/datasets/dataset.jsonl').chmod(0o600)
        (tampered_ds_ws / 'inputs/datasets/dataset.jsonl').write_bytes(b'mutated dataset\n')
        with self.assertRaisesRegex(ValueError, 'bound (input provenance mismatch|e5 dataset provenance mismatch)'):
            client._workspace(tampered_ds_ws)


if __name__ == '__main__':
    unittest.main()
