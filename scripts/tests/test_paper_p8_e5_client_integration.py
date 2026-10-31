"""Unit tests for E5 B-side client workflow: e5-viewers, run --phase, e5-stop-viewers, and export-results."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

_HERE = Path(__file__).resolve().parent
for _path in (str(_HERE), str(_HERE.parent)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import paper_p8_bundle
from scripts import paper_p8_client as client
from scripts import paper_p8_e5 as e5_driver
from scripts import paper_p8_e5_client as holder
from scripts.tests import paper_p8_e5_remote as e5_remote
from scripts.tests import paper_p8_e5_fixture as e5_fixture
from scripts.tests import paper_p8_e5_import as e5_import
import base64


def _jwt(sub):
    header = base64.urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').decode().rstrip('=')
    payload = base64.urlsafe_b64encode(json.dumps({'sub': sub, 'exp': int(time.time()) + 7200}).encode()).decode().rstrip('=')
    return f'{header}.{payload}.sig'


class FakeSmoke:
    def __init__(self, root):
        self.ROOT = Path(root)

    def http_request(self, endpoint, method, path, token=None, body=None, **kwargs):
        if method == 'PUT' and '/sensors/' in path:
            urn = 'urn:ngsi-ld:Sensor:' + path.split('/')[3] + ':' + path.split('/')[5]
            return 201, {}, {'entity_id': urn}
        if method == 'GET' and '/v1/digital-twins/' in path:
            return 200, {}, {'desired_version': '1', 'desired_state': {'desired_version': '1'}}
        return 200, {}, {}


class E5ClientIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='p8-e5-client-test-')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)

        # Setup A side files
        self.host_a = self.base / 'A'
        self.private_a = self.host_a / 'private'
        self.private_a.mkdir(parents=True, mode=0o700)
        self.repeats_dir = self.private_a / 'repeats'
        self.repeat_dir, self.context_path = e5_driver.prepare_repeat(self.repeats_dir, 1, 813)
        self.context = json.loads(self.context_path.read_text())

        # Public CA fixtures
        self.certs_dir = self.private_a / 'certs'
        self.certs_dir.mkdir(mode=0o700)
        self.https_ca = self.certs_dir / 'https-ca.pem'
        self.mqtt_ca = self.certs_dir / 'mqtt-ca.pem'
        self.https_ca.write_text('https ca\n')
        self.mqtt_ca.write_text('mqtt ca\n')
        self.https_ca.chmod(0o600)
        self.mqtt_ca.chmod(0o600)

        self.endpoint = SimpleNamespace(url='https://fixture-a.lan:14443', ca_file=self.https_ca)
        self.fixture_a = SimpleNamespace(private=self.private_a, project='e5-proj')
        self.specs = [
            SimpleNamespace(gateway_id='p8-e5-a', sensor_id=self.context['sensor_ids'][0], sensor_unit='degC'),
            SimpleNamespace(gateway_id='p8-e5-b', sensor_id=self.context['sensor_ids'][1], sensor_unit='m/s'),
        ]
        self.passwords = ['pass-a', 'pass-b']
        self.tokens = {'a': _jwt('user-a'), 'b': _jwt('user-b'), 'admin': _jwt('admin')}

        self.prep_root = self.private_a / 'prepared'
        self.prep_root.mkdir(mode=0o700)
        self.prepared_inputs = e5_fixture.prepare_inputs(
            smoke=FakeSmoke(self.base),
            fixture=self.fixture_a,
            endpoint=self.endpoint,
            specs=self.specs,
            passwords=self.passwords,
            tokens=self.tokens,
            mqtt_broker_url='ssl://fixture-a.lan:18884',
            mqtt_manifest_endpoint='mqtts://fixture-a.lan:18884',
            mqtt_ca_file=self.mqtt_ca,
            journal_path=Path(self.context['dataset_path']),
            journal_context={k: self.context[k] for k in ('sha256', 'bytes', 'gateway_ids', 'sensor_ids', 'seed', 'anchor')},
            seed=self.context['seed'],
            anchor=self.context['anchor'],
            root=self.prep_root,
            command_keys=self.context['command_keys'],
            viewer_subscriptions=self.context['viewer_subscriptions'],
        )

        self.prepared = {
            'arm': 'optimized',
            'session_id': 'e5-session-001',
            'manifests': self.prepared_inputs['manifests'],
            'workload': self.prepared_inputs['workload'],
            'context': self.context,
            'root': str(self.prep_root),
            'https_ca_file': str(self.https_ca),
            'mqtt_ca_file': str(self.mqtt_ca),
        }

        # Export bundle
        self.bundle_dir = self.base / 'bundle'
        e5_remote.export_prepared_bundle(self.prepared, output_leaf=self.bundle_dir, deadline_seconds=900)
        bundle_meta = json.loads((self.bundle_dir / 'bundle.json').read_text())
        self.prepared['bundle_sha256'] = hashlib.sha256((self.bundle_dir / 'bundle.json').read_bytes()).hexdigest()
        self.prepared['bundle_inventory'] = [{'path': r['path'], 'sha256': r['sha256']}
                                             for r in bundle_meta['inputs']]
        self.prepared['bundle_session'] = bundle_meta['session']

        # B side workspace
        self.host_b = self.base / 'B'
        self.host_b.mkdir(parents=True, mode=0o700)
        self.workspace = self.host_b / 'workspace'
        paper_p8_bundle.bind_bundle(bundle_root=self.bundle_dir, workspace_leaf=self.workspace)

        # Executables on B
        self.bin_dir = self.host_b / 'bin'
        self.bin_dir.mkdir(mode=0o700)
        self.paperload_bin = self.bin_dir / 'paperload'
        self.paperload_bin.write_text('#!/bin/sh\nexit 0\n')
        self.paperload_bin.chmod(0o700)

        gateway_script = '''#!/bin/sh
if [ "$1" = "count" ]; then
    python3 -c "import sqlite3, os; conn=sqlite3.connect(os.environ['GATEWAY_OUTBOX_PATH']); c=conn.execute(\\"SELECT count(*) FROM outbox WHERE status='pending'\\").fetchone()[0]; print(f'pending_rows={c} pending_bytes=0 quarantined_rows=0 quarantined_bytes=0')"
elif [ "$1" = "run" ]; then
    python3 -c "import sqlite3, os; conn=sqlite3.connect(os.environ['GATEWAY_OUTBOX_PATH']); conn.execute(\\"UPDATE outbox SET status='delivered' WHERE status='pending'\\"); conn.commit(); print('run summary: completed=true pending_rows=0 app_acknowledged=1')"
else
    exit 0
fi
'''
        self.gateway_bin = self.bin_dir / 'gateway'
        self.gateway_bin.write_text(gateway_script)
        self.gateway_bin.chmod(0o700)

    def test_e5_client_phase_execution_and_export_lifecycle(self):
        # 1. Start e5-viewers (mocking run_viewer_holder)
        mock_holder_return = {'state': 'STOPPED', 'result': {'status': 'COMPLETE'}}
        with patch.object(holder, 'run_viewer_holder', return_value=mock_holder_return) as mock_holder:
            code = client.main(['e5-viewers', '--workspace', str(self.workspace),
                                '--binary', str(self.paperload_bin),
                                '--gateway-binary', str(self.gateway_bin)])
            self.assertEqual(code, 0)
            mock_holder.assert_called_once()

        # Check provenance recorded
        prov_path = self.workspace / 'runtime/e5-viewer-provenance.json'
        self.assertTrue(prov_path.is_file())
        prov = json.loads(prov_path.read_text())
        self.assertEqual(prov['session_id'], 'e5-session-001')
        self.assertEqual(prov['paperload_binary_sha256'], hashlib.sha256(self.paperload_bin.read_bytes()).hexdigest())
        self.assertEqual(prov['gateway_binary_sha256'], hashlib.sha256(self.gateway_bin.read_bytes()).hexdigest())

        # Setup active holder simulation. Exactly one owner (the registered
        # cleanup) closes this descriptor; the test body only releases the
        # flock so the fd number can never be reused behind a stale close.
        control_dir = self.workspace / 'runtime/e5-holder-control'
        lock_fd = os.open(control_dir / 'viewer-holder.lock', os.O_RDWR | os.O_CREAT, 0o600)
        import fcntl
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.addCleanup(os.close, lock_fd)

        # Initial holder status
        checkpoints = []
        status_file = control_dir / 'status.json'
        status_file.write_text(json.dumps({
            'schema_version': 1, 'state': 'READY',
            'correlation': prov['correlation'], 'ready_verified': True,
            'owner_token': 'test-owner-token',
            'next_checkpoint_index': 0,
            'checkpoints': checkpoints,
        }))
        status_file.chmod(0o600)

        # Helper to simulate realistic paperload workload.json
        raw_lines = Path(self.context['dataset_path']).read_text().splitlines()
        dataset_records = [json.loads(l) for l in raw_lines]

        def _bound_identity(label, keys=('experiment', 'arm', 'profile', 'mode', 'run_id')):
            manifest = json.loads((self.workspace / f'runtime/manifests/{label}.json').read_text())
            return {key: manifest[key] for key in keys}

        def _mock_run_supervised(*args, **kwargs):
            artifact = Path(kwargs['run_artifact'])
            artifact.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            gateways = kwargs.get('gateways') or []
            jobs = kwargs.get('jobs') or []
            events = []
            children = []
            for job in jobs:
                child_role = 'paperload-telemetry' if 'initial' in job['manifest'] or 'warm' in job['manifest'] else 'paperload-control'
                child_name = f"{child_role}:{Path(job['manifest']).name}"
                children.append({
                    'role': child_role,
                    'name': Path(job['manifest']).name,
                    'status': 'stopped',
                    'ready': False,
                    'pid': 12345,
                    'exit_code': 0,
                })
                events.append({
                    'child': child_name,
                    'event': 'measure_begin',
                    'child_elapsed_ns': 10000000,
                    'observed_at_runner_monotonic_ns': 1000000000,
                    'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
                    'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 1, 'drain_seconds': 12, 'cooldown_seconds': 0},
                    'counters': {'application_acks': 0, 'publish_attempts': 0, 'pending_messages': 0, 'pending_bytes': 0},
                })
                events.append({
                    'child': child_name,
                    'event': 'measure_end',
                    'child_elapsed_ns': 20000000,
                    'observed_at_runner_monotonic_ns': 2000000000,
                    'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
                    'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 1, 'drain_seconds': 12, 'cooldown_seconds': 0},
                    'counters': {'application_acks': 20 if 'initial' in job['manifest'] else 10 if 'warm' in job['manifest'] else 4,
                                 'publish_attempts': 20 if 'initial' in job['manifest'] else 10 if 'warm' in job['manifest'] else 4,
                                 'pending_messages': 0, 'pending_bytes': 0},
                })
            is_control = any('control' in j.get('manifest', '') for j in jobs)
            if is_control:
                pending_readback = [
                    {'path_hash': hashlib.sha256(b'outbox-0').hexdigest(),
                     'rows': None, 'status': 'readback', 'pending_rows': 1, 'pending_bytes': 378,
                     'quarantined_rows': 0, 'quarantined_bytes': 0},
                    {'path_hash': hashlib.sha256(b'outbox-1').hexdigest(),
                     'rows': None, 'status': 'readback', 'pending_rows': 0, 'pending_bytes': 0,
                     'quarantined_rows': 0, 'quarantined_bytes': 0},
                ]
                runner_status = 'PARTIAL'
            else:
                pending_readback = [
                    {'path_hash': hashlib.sha256(f'outbox-{index}'.encode()).hexdigest(),
                     'rows': None, 'status': 'readback', 'pending_rows': 0, 'pending_bytes': 0,
                     'quarantined_rows': 0, 'quarantined_bytes': 0}
                    for index in range(len(gateways))]
                runner_status = 'COMPLETE'

            artifact.write_text(json.dumps({
                'status': runner_status,
                'implementation_status': 'P8.4 bounded child lifecycle; not native acceptance',
                'started_at_unix': 1.0, 'finished_at_unix': 2.0,
                'actual_stop_confirmed': True,
                'durable_gateway_process_count': len(gateways),
                'durable_gateway_count': len(gateways),
                'durable_sensor_connections': 0,
                'durable_sensor_count': 0,
                'timer_driven_gateway_workload': False,
                'primary_error': None,
                'cleanup_errors': [],
                'children': children,
                'phase_observations': {
                    'status': 'observed',
                    'clock_qualification': 'runner monotonic receipt times are descriptive; child process elapsed clocks are not comparable across children; pipe observation lag unknown',
                    'events': events,
                    'errors': [],
                },
                'readiness': [{'child': f'gateway-{index + 1}', 'ready': True}
                              for index in range(len(gateways))],
                'pending_outbox_readback': pending_readback,
            }))
            artifact.chmod(0o600)
            # Write workload results using the actual Go result schemas: the
            # role comes from the bound manifest; RunResult/ControlResult/
            # ViewerResult never emit a "roles" field.
            for job in kwargs['jobs']:
                out = Path(job['output_dir'])
                out.mkdir(parents=True, mode=0o700, exist_ok=True)
                workload_file = out / 'workload.json'
                if 'initial' in job['manifest']:
                    samples = [{k: r[k] for k in ('gateway_id', 'sensor_id', 'boot_id', 'first_sequence',
                                                  'sample_count', 'payload_sha256', 'message_id')} for r in dataset_records]
                    for s in samples:
                        s['topic'] = f"gateways/{s['gateway_id']}/telemetry/{s['sensor_id']}"
                    workload_file.write_text(json.dumps({
                        **_bound_identity('initial'),
                        'status': 'COMPLETE',
                        'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                        'durability_mode': 'synthetic-load',
                        'e4': {'mode': 'replay', 'journal_entry_count': 20,
                               'sampling_qualification': 'synthetic_load_only; not SQLite/durable assurance',
                               'safe_header_samples': samples},
                        'telemetry': {'broker_pubacks': 20, 'application_acks': 0, 'publish_attempts': 20,
                                      'retries': 0, 'pending_messages': 0, 'pending_bytes': 0,
                                      'replay_application_acks': 20},
                    }))
                elif 'warm' in job['manifest']:
                    samples = [{k: r[k] for k in ('gateway_id', 'sensor_id', 'boot_id', 'first_sequence',
                                                  'sample_count', 'payload_sha256', 'message_id')} for r in dataset_records[:10]]
                    for s in samples:
                        s['topic'] = f"gateways/{s['gateway_id']}/telemetry/{s['sensor_id']}"
                    workload_file.write_text(json.dumps({
                        **_bound_identity('warm_replay'),
                        'status': 'COMPLETE',
                        'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                        'durability_mode': 'synthetic-load',
                        'e4': {'mode': 'replay', 'journal_entry_count': 10,
                               'sampling_qualification': 'synthetic_load_only; not SQLite/durable assurance',
                               'safe_header_samples': samples},
                        'telemetry': {'broker_pubacks': 10, 'application_acks': 0, 'publish_attempts': 10,
                                      'retries': 0, 'pending_messages': 0, 'pending_bytes': 0,
                                      'replay_application_acks': 10},
                    }))
                elif 'control' in job['manifest']:
                    backend_cmd_ids = [
                        'dad89ea1-5993-4e35-9b91-2916556ec22c',
                        '9631fc93-fd2f-4d0f-986f-acf3853f3dc3',
                        'da2d2f21-7c17-4b42-b825-7457b9d4df9b',
                        'b102b3f8-e43f-48c2-866d-c22e1a265f57',
                    ]
                    manifest_ctrl = json.loads(Path(job['manifest']).read_text())
                    cmds = []
                    for i, m_cmd in enumerate(manifest_ctrl.get('commands', [])):
                        cmds.append({
                            'gateway_id': m_cmd['gateway_id'],
                            'sensor_id': m_cmd['sensor_id'],
                            'actor_label': m_cmd.get('actor_label', 'e3-operator'),
                            'frequency_hz': m_cmd['frequency_hz'],
                            'expected_desired_version': str(m_cmd['expected_desired_version']),
                            'idempotency_key': m_cmd['idempotency_key'],
                            'command_id': backend_cmd_ids[i],
                            'classification': 'succeeded',
                            'terminal_status': 'succeeded',
                            'http_status': 202,
                        })
                    workload_file.write_text(json.dumps({
                        **_bound_identity('control', keys=('experiment', 'arm', 'run_id')),
                        'status': 'COMPLETE',
                        'implementation_status': 'P8.4.10 HTTPS REST control producer only; external durable simulator required',
                        'planned': 4, 'sent': 4, 'skipped': 0, 'pending': 0,
                        'commands': cmds,
                    }))
                workload_file.chmod(0o600)
            return runner_status, json.loads(artifact.read_text())

        def _fake_request_phase(cd, corr, phase, boundary, **kw):
            st_data = json.loads(status_file.read_text())
            cps = st_data.setdefault('checkpoints', [])
            cps.append({'phase': phase, 'boundary': boundary, 'elapsed_seconds': len(cps) * 2.0,
                        'child_alive': True, 'ready_verified': True})
            st_data['state'] = 'PHASE'
            st_data['next_checkpoint_index'] = len(cps)
            st_data['owner_token'] = st_data.get('owner_token', 'test-owner-token')
            status_file.write_text(json.dumps(st_data))
            status_file.chmod(0o600)
            return {'status': 'ACK'}

        with patch.object(client.paper_p8, 'run_supervised', side_effect=_mock_run_supervised), \
             patch.object(holder, 'request_viewer_phase', side_effect=_fake_request_phase) as mock_phase_req:

            # 2. Run phase initial
            code = client.main(['run', '--workspace', str(self.workspace),
                                '--binary', str(self.paperload_bin),
                                '--phase', 'initial'])
            self.assertEqual(code, 0)
            self.assertEqual(mock_phase_req.call_count, 2)  # begin and end

            # Verify phase state
            pstate = json.loads((self.workspace / 'runtime/e5-phase-state.json').read_text())
            self.assertIn('initial', pstate['completed_phases'])

            # 3. Export phase initial
            out_initial = self.base / 'export-initial'
            code = client.main(['export-results', '--workspace', str(self.workspace),
                                '--phase', 'initial', '--output', str(out_initial)])
            self.assertEqual(code, 0)
            self.assertTrue((out_initial / 'results.json').is_file())
            initial_res = json.loads((out_initial / 'results.json').read_text())
            self.assertEqual(initial_res['status'], 'COMPLETE')
            self.assertEqual(initial_res['e4']['journal_entry_count'], 20)

            # Transfer to A and import into A-side supervisor
            in_root = self.private_a / 'incoming'
            in_root.mkdir(mode=0o700, exist_ok=True)
            in_root.chmod(0o700)
            in_initial = in_root / 'initial'
            in_initial.mkdir(mode=0o700, exist_ok=True)
            in_initial.chmod(0o700)
            shutil.copyfile(out_initial / 'results.json', in_initial / 'results.json')
            shutil.copyfile(out_initial / 'resources-generator.csv', in_initial / 'resources-generator.csv')
            (in_initial / 'results.json').chmod(0o600)
            (in_initial / 'resources-generator.csv').chmod(0o600)
            imported_initial = e5_import.import_phase_result(self.prepared, self.private_a, phase='initial')
            self.assertEqual(imported_initial['projection']['status'], 'COMPLETE')

            # 4. Run phase warm_replay
            code = client.main(['run', '--workspace', str(self.workspace),
                                '--binary', str(self.paperload_bin),
                                '--phase', 'warm_replay'])
            self.assertEqual(code, 0)

            # 5. Export phase warm_replay
            out_warm = self.base / 'export-warm'
            code = client.main(['export-results', '--workspace', str(self.workspace),
                                '--phase', 'warm_replay', '--output', str(out_warm)])
            self.assertEqual(code, 0)
            warm_res = json.loads((out_warm / 'results.json').read_text())
            self.assertEqual(warm_res['e4']['journal_entry_count'], 10)

            # Transfer to A and import into A-side supervisor
            in_warm = in_root / 'warm_replay'
            in_warm.mkdir(mode=0o700, exist_ok=True)
            in_warm.chmod(0o700)
            shutil.copyfile(out_warm / 'results.json', in_warm / 'results.json')
            shutil.copyfile(out_warm / 'resources-generator.csv', in_warm / 'resources-generator.csv')
            (in_warm / 'results.json').chmod(0o600)
            (in_warm / 'resources-generator.csv').chmod(0o600)
            imported_warm = e5_import.import_phase_result(self.prepared, self.private_a, phase='warm_replay')
            self.assertEqual(imported_warm['projection']['status'], 'COMPLETE')

            # Setup SQLite executor databases before control phase using realistic Gateway simulator schema:
            # command_receipts stores backend command_id and status='completed'; result_bytes stores status='succeeded'
            backend_cmd_ids = [
                'dad89ea1-5993-4e35-9b91-2916556ec22c',
                '9631fc93-fd2f-4d0f-986f-acf3853f3dc3',
                'da2d2f21-7c17-4b42-b825-7457b9d4df9b',
                'b102b3f8-e43f-48c2-866d-c22e1a265f57',
            ]
            outbox_dir = self.workspace / 'outputs/outbox'
            outbox_dir.mkdir(parents=True, exist_ok=True)
            manifest_ctrl = json.loads((self.workspace / 'runtime/manifests/control.json').read_text())
            m_cmds = manifest_ctrl['commands']
            # Compute expected versions from manifest commands
            v_a0 = int(m_cmds[0]['expected_desired_version']) + 1
            v_b0 = int(m_cmds[1]['expected_desired_version']) + 1
            v_a1 = int(m_cmds[2]['expected_desired_version']) + 1
            v_b1 = int(m_cmds[3]['expected_desired_version']) + 1

            executor_receipt_data = [
                # Executor 1 (sensor p8-e5-command-a, commands 0 and 2)
                [
                    (backend_cmd_ids[0], 'hash-0', v_a0, 666667, 'completed',
                     json.dumps({'command_id': backend_cmd_ids[0], 'desired_version': str(v_a0),
                                 'status': 'succeeded', 'reported_state': {'sample_interval_us': 666667}}).encode()),
                    (backend_cmd_ids[2], 'hash-2', v_a1, 500000, 'completed',
                     json.dumps({'command_id': backend_cmd_ids[2], 'desired_version': str(v_a1),
                                 'status': 'succeeded', 'reported_state': {'sample_interval_us': 500000}}).encode()),
                ],
                # Executor 2 (sensor p8-e5-command-b, commands 1 and 3)
                [
                    (backend_cmd_ids[1], 'hash-1', v_b0, 2000000, 'completed',
                     json.dumps({'command_id': backend_cmd_ids[1], 'desired_version': str(v_b0),
                                 'status': 'succeeded', 'reported_state': {'sample_interval_us': 2000000}}).encode()),
                    (backend_cmd_ids[3], 'hash-3', v_b1, 1333333, 'completed',
                     json.dumps({'command_id': backend_cmd_ids[3], 'desired_version': str(v_b1),
                                 'status': 'succeeded', 'reported_state': {'sample_interval_us': 1333333}}).encode()),
                ],
            ]
            final_configs = [
                (str(v_a1), 500000),
                (str(v_b1), 1333333),
            ]
            for idx in (1, 2):
                db_path = outbox_dir / f'executor-{idx}.sqlite'
                with sqlite3.connect(db_path) as conn:
                    conn.execute('CREATE TABLE sensor_configuration (id INTEGER PRIMARY KEY, desired_version TEXT, sample_interval_us INTEGER, applied_at TEXT);')
                    conn.execute('INSERT INTO sensor_configuration VALUES (1, ?, ?, "2026-01-01T00:00:00Z");', final_configs[idx - 1])
                    conn.execute('CREATE TABLE command_receipts (command_id TEXT, command_hash TEXT, desired_version INTEGER, sample_interval_us INTEGER, status TEXT, result_bytes BLOB);')
                    conn.execute("CREATE TABLE outbox (id INTEGER PRIMARY KEY, status TEXT);")
                    if idx == 1:
                        conn.execute("INSERT INTO outbox VALUES (1, 'pending');")
                    for row in executor_receipt_data[idx - 1]:
                        conn.execute("INSERT INTO command_receipts VALUES (?, ?, ?, ?, ?, ?);", row)

            # 6. Run phase control
            code = client.main(['run', '--workspace', str(self.workspace),
                                '--binary', str(self.paperload_bin),
                                '--gateway-binary', str(self.gateway_bin),
                                '--phase', 'control'])
            self.assertEqual(code, 0)

            # 7. Stop viewers
            code = client.main(['e5-stop-viewers', '--workspace', str(self.workspace)])
            self.assertEqual(code, 0)

            # Release holder lock (without closing: cleanup still owns the fd)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lifecycle = {
                'schema_version': 1,
                'clock': 'viewer_holder_local_elapsed_seconds',
                'correlation': prov['correlation'],
                'ready_elapsed_seconds': 1.0,
                'checkpoints': [
                    {'phase': phase, 'boundary': b, 'elapsed_seconds': sec, 'child_alive': True, 'ready_verified': True}
                    for (phase, b), sec in zip(
                        [('initial', 'begin'), ('initial', 'end'),
                         ('warm_replay', 'begin'), ('warm_replay', 'end'),
                         ('control', 'begin'), ('control', 'end')],
                        (2, 4, 6, 8, 10, 12),
                    )
                ],
                'stop_elapsed_seconds': 14.0,
                'joined': True,
            }
            viewer_workload = {
                **_bound_identity('viewer'),
                'status': 'COMPLETE',
                'implementation_status': 'P8.4c viewer-only client; no control/durable/server orchestration',
                'planned_stop': True,
                'planned_stop_reason': 'stopped_by_operator',
                'latency_available': False,
                'latency_unavailable_reason': 'publisher_origin_not_available_in_viewer_role',
                'connection_attempts': 2,
                'connected_distinct_viewers': 2,
                'subscribed_distinct_viewers': 2,
                'receiving_distinct_viewers': 2,
                'reconnections': 0,
                'unique_events': 20,
                'duplicates': 0,
                'wrong_namespace': 0,
                'close_reasons': {'planned_stop': 2},
                'viewers': [
                    {'id': 'e5-viewer-a', 'close_reason': 'planned_stop', 'events': 10, 'unique_events': 10,
                     'duplicates': 0, 'bytes': 100, 'wrong_namespace': 0},
                    {'id': 'e5-viewer-b', 'close_reason': 'planned_stop', 'events': 10, 'unique_events': 10,
                     'duplicates': 0, 'bytes': 100, 'wrong_namespace': 0},
                ],
                'identity_samples': [
                    {
                        'viewer_id': 'e5-viewer-a',
                        'gateway_id': dataset_records[0]['gateway_id'],
                        'sensor_id': dataset_records[0]['sensor_id'],
                        'message_id': dataset_records[0]['message_id'],
                        'boot_id': dataset_records[0]['boot_id'],
                        'first_sequence': str(dataset_records[0]['first_sequence']),
                        'sample_count': 100,
                        'header_sha256': hashlib.sha256(json.dumps({
                            'gateway_id': dataset_records[0]['gateway_id'],
                            'sensor_id': dataset_records[0]['sensor_id'],
                            'message_id': dataset_records[0]['message_id'],
                            'boot_id': dataset_records[0]['boot_id'],
                            'first_sequence': str(dataset_records[0]['first_sequence']),
                            'sample_count': 100,
                        }, separators=(',', ':')).encode('utf-8')).hexdigest(),
                    },
                    {
                        'viewer_id': 'e5-viewer-b',
                        'gateway_id': dataset_records[1]['gateway_id'],
                        'sensor_id': dataset_records[1]['sensor_id'],
                        'message_id': dataset_records[1]['message_id'],
                        'boot_id': dataset_records[1]['boot_id'],
                        'first_sequence': str(dataset_records[1]['first_sequence']),
                        'sample_count': 100,
                        'header_sha256': hashlib.sha256(json.dumps({
                            'gateway_id': dataset_records[1]['gateway_id'],
                            'sensor_id': dataset_records[1]['sensor_id'],
                            'message_id': dataset_records[1]['message_id'],
                            'boot_id': dataset_records[1]['boot_id'],
                            'first_sequence': str(dataset_records[1]['first_sequence']),
                            'sample_count': 100,
                        }, separators=(',', ':')).encode('utf-8')).hexdigest(),
                    },
                ],
            }
            holder_res_file = control_dir / 'holder-result.json'
            holder_res_file.write_text(json.dumps({
                'schema_version': 1, 'correlation': prov['correlation'],
                'lifecycle': lifecycle, 'workload': viewer_workload,
            }))
            holder_res_file.chmod(0o600)

            # 8. Export phase control
            out_control = self.base / 'export-control'
            code = client.main(['export-results', '--workspace', str(self.workspace),
                                '--phase', 'control', '--output', str(out_control)])
            self.assertEqual(code, 0)
            self.assertTrue((out_control / 'results.json').is_file())
            control_res = json.loads((out_control / 'results.json').read_text())
            self.assertEqual(control_res['status'], 'COMPLETE')
            self.assertEqual(len(control_res['commands']), 4)
            self.assertEqual(len(control_res['executors']), 2)
            self.assertEqual(control_res['viewer']['planned_stop_reason'], 'stopped_by_operator')
            self.assertTrue(control_res['b_declared']['actual_stop_confirmed'])

            # Verify the raw control run artifact preserved the historical pre-drain pending readback
            # while recording the verified fresh zero readback and post-stop drain qualification.
            raw_control = json.loads((self.workspace / 'outputs/e5-phases/control/run.json').read_text())
            self.assertEqual(raw_control['status'], 'COMPLETE')
            self.assertEqual(raw_control['pre_drain_pending_outbox_readback'][0]['pending_rows'], 1)
            self.assertEqual(raw_control['pre_drain_pending_outbox_readback'][0]['pending_bytes'], 378)
            self.assertEqual(raw_control['pending_outbox_readback'][0]['pending_rows'], 0)
            self.assertEqual(raw_control['pending_outbox_readback'][0]['pending_bytes'], 0)
            self.assertEqual(raw_control['pending_outbox_readback'][1]['pending_rows'], 0)
            self.assertEqual(raw_control['pending_outbox_readback'][1]['pending_bytes'], 0)
            self.assertEqual(raw_control['post_stop_drain'][0]['drain'], 'verified')
            self.assertEqual(raw_control['post_stop_drain'][0]['pending_before'], 1)
            self.assertEqual(raw_control['post_stop_drain'][0]['pending_after'], 0)
            self.assertEqual(raw_control['post_stop_drain'][1]['drain'], 'not_needed')
            self.assertIn('sequential executor reopen', raw_control.get('post_stop_drain_qualification', ''))

            # Verify exported runner reflects fresh zero readback without leaking pre_drain raw field
            self.assertEqual(control_res['runner']['pending_outbox_readback'][0]['pending_rows'], 0)
            self.assertEqual(control_res['runner']['pending_outbox_readback'][0]['pending_bytes'], 0)
            self.assertEqual(control_res['runner']['pending_outbox_readback'][1]['pending_rows'], 0)
            self.assertEqual(control_res['runner']['pending_outbox_readback'][1]['pending_bytes'], 0)
            self.assertNotIn('pre_drain_pending_outbox_readback', control_res['runner'])

            # Transfer to A and import into A-side supervisor
            in_control = in_root / 'control'
            in_control.mkdir(mode=0o700, exist_ok=True)
            in_control.chmod(0o700)
            shutil.copyfile(out_control / 'results.json', in_control / 'results.json')
            shutil.copyfile(out_control / 'resources-generator.csv', in_control / 'resources-generator.csv')
            (in_control / 'results.json').chmod(0o600)
            (in_control / 'resources-generator.csv').chmod(0o600)
            imported_control = e5_import.import_phase_result(self.prepared, self.private_a, phase='control')
            self.assertEqual(imported_control['projection']['status'], 'COMPLETE')

            # Exercise existing A E3 projection + _verify_control_receipts with the actual control export
            import paper_p8_e3_fixture as e3_fixture
            sqlite_summaries = e3_fixture._e3_executor_receipt_projection(control_res['executors'])
            self.assertEqual(len(sqlite_summaries), 2)
            self.assertEqual([s['completed_receipts_count'] for s in sqlite_summaries], [2, 2])
            self.assertEqual([s['pending_outbox'] for s in sqlite_summaries], [0, 0])

            # Reconcile using backend command IDs and actual receipt result hashes
            receipts_by_cid = {r['command_id']: r for ex in control_res['executors'] for r in ex['receipts']}
            pg_receipts = {}
            for i, cmd in enumerate(m_cmds):
                cid = backend_cmd_ids[i]
                r = receipts_by_cid[cid]
                pg_receipts[cmd['idempotency_key']] = {
                    'idempotency_key': cmd['idempotency_key'],
                    'command_id': cid,
                    'status_code': 202,
                    'status': 'succeeded',
                    'result_hash': r['result']['result_sha256'],
                    'desired_version': r['desired_version'],
                    'sample_interval_us': r['sample_interval_us'],
                }
            rest_configurations = {
                'p8-e5-command-a': {'desired_version': str(v_a1), 'sample_interval_us': 500000},
                'p8-e5-command-b': {'desired_version': str(v_b1), 'sample_interval_us': 1333333},
            }
            command_sensors = ['p8-e5-command-a', 'p8-e5-command-b']
            self.assertTrue(e3_fixture._verify_control_receipts(
                sqlite_summaries, m_cmds, pg_receipts, rest_configurations, command_sensors))

            # Verify continuous viewer validation accepts the exported B viewer result
            init_ws = {'new_events': 20, 'telemetry_writes_completed': 20, 'deliveries_enqueued': 20, 'drops': 0}
            warm_ws = {'new_events': 0, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
            control_ws = {'new_events': 4, 'telemetry_writes_completed': 0, 'deliveries_enqueued': 0, 'drops': 0}
            end_metrics = {'websocket_metrics': {'global_queued_work': 0, 'global_queued_bytes': 0}}
            cv_res = e5_remote.validate_continuous_viewer(
                control_res['viewer'], self.context, {'b_projection': initial_res},
                init_ws, warm_ws, control_ws, end_metrics,
                readiness_projection=control_res['readiness_projection'],
                lifecycle_proof=control_res['lifecycle'],
                expected_correlation=prov['correlation'],
            )
            self.assertTrue(cv_res['valid'])
            self.assertEqual(cv_res['failures'], [])

            # Raw results follow the actual Go structs: no result-side role field.
            self.assertNotIn('roles', control_res['viewer'])
            self.assertNotIn('roles', initial_res['telemetry'])
            self.assertNotIn('roles', initial_res['e4'])
            self.assertTrue(control_res['runner']['actual_stop_confirmed'])
            self.assertTrue(control_res['joined_proof']['executors_joined'])

            # --- negative proofs: raw runner and identity must be real ---
            control_run = self.workspace / 'outputs/e5-phases/control/run.json'
            control_workload = self.workspace / 'outputs/e5-phases/control/workload/workload.json'
            initial_workload = self.workspace / 'outputs/e5-phases/initial/workload/workload.json'
            holder_result = control_dir / 'holder-result.json'
            originals = {path: path.read_text() for path in
                         (control_run, control_workload, initial_workload, holder_result)}

            def _mutate(path, update):
                data = json.loads(path.read_text())
                update(data)
                path.write_text(json.dumps(data))
                path.chmod(0o600)

            def _expect_reject(label, path, update, phase, pattern):
                _mutate(path, update)
                try:
                    with self.assertRaisesRegex(ValueError, pattern):
                        client._export_e5_results(self.workspace, self.base / label, phase)
                finally:
                    path.write_text(originals[path])
                    path.chmod(0o600)

            def _drop(key):
                return lambda data: data.pop(key, None)

            def _set(key, value):
                return lambda data: data.__setitem__(key, value)

            _expect_reject('neg-stop-false', control_run,
                           _set('actual_stop_confirmed', False), 'control',
                           'runner_stop_unconfirmed_control')
            _expect_reject('neg-stop-missing', control_run,
                           _drop('actual_stop_confirmed'), 'control',
                           'runner_proof_missing_control')
            _expect_reject('neg-cleanup-error', control_run,
                           _set('cleanup_errors', ['gateway-1 count exited 1']), 'control',
                           'runner_cleanup_errors_control')
            _expect_reject('neg-primary-error', control_run,
                           _set('primary_error', 'PaperloadExitError'), 'control',
                           'runner_primary_error_control')
            _expect_reject('neg-control-run-id', control_workload,
                           _set('run_id', 'e5-proj-wrong'), 'control',
                           'control_child_workload_invalid')
            _expect_reject('neg-initial-experiment', initial_workload,
                           _set('experiment', 'E1'), 'initial',
                           'initial_child_workload_invalid')
            _expect_reject('neg-initial-run-id', initial_workload,
                           _set('run_id', 'e5-proj-wrong'), 'initial',
                           'initial_child_workload_invalid')
            _expect_reject('neg-initial-ack', initial_workload,
                           lambda data: data['telemetry'].__setitem__('replay_application_acks', 19),
                           'initial', 'initial_telemetry_accounting_invalid')
            _expect_reject('neg-initial-puback-only', initial_workload,
                           lambda data: data['telemetry'].pop('replay_application_acks', None),
                           'initial', 'initial_telemetry_accounting_invalid')
            _expect_reject('neg-drain-pending', control_run,
                           lambda data: data['pending_outbox_readback'][0].__setitem__('pending_rows', 1),
                           'control', 'runner_drain_unconfirmed_control')
            _expect_reject('neg-viewer-cleanup', holder_result,
                           lambda data: data['workload'].__setitem__('cleanup_errors', ['child_not_joined']),
                           'control', 'e5_viewer_cleanup_errors')

    def test_e5_client_negative_cases(self):
        # 1. e5-viewers without gateway binary fails
        with self.assertRaisesRegex(ValueError, 'gateway_binary_required'):
            client._run_e5_viewers(self.workspace, binary=self.paperload_bin, gateway_binary=None)

        # 2. e5-viewers with deadline exceeding declared fails
        with self.assertRaisesRegex(ValueError, 'exceeds declared lifecycle deadline'):
            client._run_e5_viewers(self.workspace, binary=self.paperload_bin,
                                   gateway_binary=self.gateway_bin, deadline_seconds=1200)

        # 3. First e5-viewers setup succeeds, duplicate setup fails and preserves original provenance
        mock_holder_return = {'state': 'STOPPED', 'result': {'status': 'COMPLETE'}}
        with patch.object(holder, 'run_viewer_holder', return_value=mock_holder_return):
            res = client._run_e5_viewers(self.workspace, binary=self.paperload_bin,
                                         gateway_binary=self.gateway_bin, deadline_seconds=900)
            self.assertEqual(res['state'], 'STOPPED')

        prov_path = self.workspace / 'runtime/e5-viewer-provenance.json'
        original_prov_bytes = prov_path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'e5_viewer_provenance_already_exists'):
            client._run_e5_viewers(self.workspace, binary=self.paperload_bin,
                                   gateway_binary=self.gateway_bin, deadline_seconds=900)
        self.assertEqual(prov_path.read_bytes(), original_prov_bytes)

        # 4. Run without holder running fails
        with self.assertRaisesRegex(ValueError, 'e5_viewer_holder_not_running'):
            client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)

        # 5. Run with mismatched paperload binary fails
        other_bin = self.bin_dir / 'other-paperload'
        other_bin.write_text('#!/bin/sh\nexit 1\n')
        other_bin.chmod(0o700)
        with self.assertRaisesRegex(ValueError, 'paperload_binary_provenance_mismatch'):
            client._run_e5_phase(self.workspace, 'initial', binary=other_bin)

        # 6. Run control without gateway binary fails
        with self.assertRaisesRegex(ValueError, 'E5 control phase requires --gateway-binary'):
            client._run_e5_phase(self.workspace, 'control', binary=self.paperload_bin)

        # 7. Run warm_replay before initial fails
        with self.assertRaisesRegex(ValueError, 'E5 initial required before warm_replay'):
            client._run_e5_phase(self.workspace, 'warm_replay', binary=self.paperload_bin)

        # 8. Stop viewers before control phase completed fails
        with self.assertRaisesRegex(ValueError, 'e5_stop_viewers_requires_control_phase_completed'):
            client._stop_e5_viewers(self.workspace)

        # 9. Export phase not completed fails
        with self.assertRaisesRegex(ValueError, 'E5 phase initial not completed'):
            client._export_e5_results(self.workspace, self.base / 'bad-exp', 'initial')

        # 10. Phase state session/binding tampering fails
        pstate_path = self.workspace / 'runtime/e5-phase-state.json'
        orig_pstate = pstate_path.read_text() if pstate_path.exists() else None
        pstate_path.write_text(json.dumps({
            'schema_version': 1, 'experiment': 'E5', 'session_id': 'tampered-session',
            'binding_digest': 'wrong-digest', 'completed_phases': [], 'phase_history': [],
            'last_completed_phase': None,
        }))
        pstate_path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'e5_phase_state_binding_mismatch'):
            client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)
        if orig_pstate:
            pstate_path.write_text(orig_pstate)
            pstate_path.chmod(0o600)
        else:
            pstate_path.unlink(missing_ok=True)

        # 11. Resource sample validation rejects non-finite numbers and extra fields
        bad_sample = {'sample_index': 0, 'timestamp': '2026-01-01T00:00:00Z', 'elapsed_seconds': float('nan')}
        phase_dir = self.workspace / 'outputs/e5-phases/initial'
        phase_dir.mkdir(parents=True, mode=0o700)
        res_json = phase_dir / 'resources-generator.json'
        res_json.write_text(json.dumps({'samples': [bad_sample]}))
        res_json.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'resource_sample_non_finite_rejected'):
            client._sanitize_and_write_resource_samples(phase_dir, self.base / 'res-out')

        bad_fields_sample = {'sample_index': 0, 'malicious_extra_field': 'attack'}
        res_json.write_text(json.dumps({'samples': [bad_fields_sample]}))
        res_json.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'resource_sample_fields_rejected'):
            client._sanitize_and_write_resource_samples(phase_dir, self.base / 'res-out')

        # 12. SQLite outbox path escape rejected
        fake_spec = {'gateways': [
            {'env_file': str(self.workspace / 'runtime/gateway-env/executor-1.json')},
            {'env_file': str(self.workspace / 'runtime/gateway-env/executor-2.json')},
        ]}
        valid_cmds = [
            {'idempotency_key': k, 'command_id': f'cmd-{i}', 'classification': 'succeeded', 'terminal_status': 'succeeded'}
            for i, k in enumerate(self.context['command_keys'])
        ]
        env_file = self.workspace / 'runtime/gateway-env/executor-1.json'
        orig_env = env_file.read_text()
        env_data = json.loads(orig_env)
        env_data['GATEWAY_OUTBOX_PATH'] = '/etc/passwd'
        env_file.write_text(json.dumps(env_data))
        env_file.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'executor_database_path_escape_rejected'):
            client._project_sqlite_executors(self.workspace, fake_spec, self.context['command_keys'], valid_cmds)
        env_file.write_text(orig_env)
        env_file.chmod(0o600)

        # 13. Holder readiness state validation rejects non-READY/PHASE states, mismatched correlation and unverified readiness
        control_dir = self.workspace / 'runtime/e5-holder-control'
        lock_fd = os.open(control_dir / 'viewer-holder.lock', os.O_RDWR | os.O_CREAT, 0o600)
        import fcntl
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.addCleanup(os.close, lock_fd)
        prov = json.loads(prov_path.read_text())
        status_file = control_dir / 'status.json'

        # Non-READY/PHASE states are rejected
        for bad_state in ('STOPPED', 'FAILED', 'STARTING', 'UNKNOWN'):
            status_file.write_text(json.dumps({
                'schema_version': 1, 'state': bad_state,
                'correlation': prov['correlation'], 'ready_verified': True,
                'owner_token': 'tok', 'next_checkpoint_index': 0, 'checkpoints': [],
            }))
            status_file.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'e5_viewer_holder_not_ready'):
                client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)

        # Mismatched correlation is rejected
        bad_corr = dict(prov['correlation'], binary_sha256='0' * 64)
        status_file.write_text(json.dumps({
            'schema_version': 1, 'state': 'READY',
            'correlation': bad_corr, 'ready_verified': True,
            'owner_token': 'tok', 'next_checkpoint_index': 0, 'checkpoints': [],
        }))
        status_file.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'e5_viewer_holder_not_ready'):
            client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)

        # ready_verified not True is rejected
        status_file.write_text(json.dumps({
            'schema_version': 1, 'state': 'READY',
            'correlation': prov['correlation'], 'ready_verified': False,
            'owner_token': 'tok', 'next_checkpoint_index': 0, 'checkpoints': [],
        }))
        status_file.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'e5_viewer_holder_ready_unverified'):
            client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)

        # Checkpoint failure prevents producer launch
        status_file.write_text(json.dumps({
            'schema_version': 1, 'state': 'READY',
            'correlation': prov['correlation'], 'ready_verified': True,
            'owner_token': 'tok', 'next_checkpoint_index': 0, 'checkpoints': [],
        }))
        status_file.chmod(0o600)
        supervised_called = []
        def _mock_supervised_fail(*a, **kw):
            supervised_called.append(True)
            return 'COMPLETE', {}
        with patch.object(holder, 'request_viewer_phase', side_effect=holder.HolderError('phase_ack_timeout')), \
             patch.object(client.paper_p8, 'run_supervised', side_effect=_mock_supervised_fail):
            with self.assertRaises(holder.HolderError):
                client._run_e5_phase(self.workspace, 'initial', binary=self.paperload_bin)
            self.assertEqual(supervised_called, [], 'run_supervised must not be called if begin checkpoint fails')

        # 14. SQLite projector negative cases: command count mismatch, foreign/duplicate mapping, wrong row/business status, result mismatch
        declared = self.context['command_keys']
        # 14a. Count mismatch
        with self.assertRaisesRegex(ValueError, 'e5_control_commands_count_mismatch'):
            client._project_sqlite_executors(self.workspace, fake_spec, declared, valid_cmds[:3])
        # 14b. Foreign idempotency key in commands
        foreign_cmds = [dict(c) for c in valid_cmds]
        foreign_cmds[0]['idempotency_key'] = 'foreign-key-not-in-declared'
        with self.assertRaisesRegex(ValueError, 'e5_control_idempotency_key_invalid'):
            client._project_sqlite_executors(self.workspace, fake_spec, declared, foreign_cmds)
        # 14c. Duplicate idempotency key in commands
        dup_cmds = [dict(c) for c in valid_cmds]
        dup_cmds[1]['idempotency_key'] = dup_cmds[0]['idempotency_key']
        with self.assertRaisesRegex(ValueError, 'e5_control_idempotency_key_invalid'):
            client._project_sqlite_executors(self.workspace, fake_spec, declared, dup_cmds)
        # 14d. Duplicate backend command_id in commands
        dup_cid_cmds = [dict(c) for c in valid_cmds]
        dup_cid_cmds[1]['command_id'] = dup_cid_cmds[0]['command_id']
        with self.assertRaisesRegex(ValueError, 'e5_control_backend_command_id_invalid'):
            client._project_sqlite_executors(self.workspace, fake_spec, declared, dup_cid_cmds)
        # 14e. Command not succeeded terminal status
        unverified_cmds = [dict(c) for c in valid_cmds]
        unverified_cmds[0]['terminal_status'] = 'failed'
        with self.assertRaisesRegex(ValueError, 'e5_control_command_unverified'):
            client._project_sqlite_executors(self.workspace, fake_spec, declared, unverified_cmds)

        # 15. Fresh executor outbox readback negatives and drain promotion safety
        gateways = fake_spec['gateways']
        # 15a. Nonzero pending rows in fresh count returns None
        bad_count_bin = self.bin_dir / 'bad-count-nonzero-rows'
        bad_count_bin.write_text('#!/bin/sh\n[ "$1" = "count" ] && echo "pending_rows=1 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0" || exit 0\n')
        bad_count_bin.chmod(0o700)
        self.assertIsNone(client._fresh_executor_outbox_readback(self.workspace, gateways, bad_count_bin))

        # 15b. Nonzero pending bytes in fresh count returns None
        bad_bytes_bin = self.bin_dir / 'bad-count-nonzero-bytes'
        bad_bytes_bin.write_text('#!/bin/sh\n[ "$1" = "count" ] && echo "pending_rows=0 pending_bytes=128 quarantined_rows=0 quarantined_bytes=0" || exit 0\n')
        bad_bytes_bin.chmod(0o700)
        self.assertIsNone(client._fresh_executor_outbox_readback(self.workspace, gateways, bad_bytes_bin))

        # 15c. Nonzero quarantine in fresh count returns None
        bad_q_bin = self.bin_dir / 'bad-count-nonzero-q'
        bad_q_bin.write_text('#!/bin/sh\n[ "$1" = "count" ] && echo "pending_rows=0 pending_bytes=0 quarantined_rows=1 quarantined_bytes=100" || exit 0\n')
        bad_q_bin.chmod(0o700)
        self.assertIsNone(client._fresh_executor_outbox_readback(self.workspace, gateways, bad_q_bin))

        # 15d. Malformed count output returns None
        bad_malformed_bin = self.bin_dir / 'bad-count-malformed'
        bad_malformed_bin.write_text('#!/bin/sh\n[ "$1" = "count" ] && echo "garbage output" || exit 0\n')
        bad_malformed_bin.chmod(0o700)
        self.assertIsNone(client._fresh_executor_outbox_readback(self.workspace, gateways, bad_malformed_bin))

        # 15e. Count exit failure returns None
        bad_fail_bin = self.bin_dir / 'bad-count-fail'
        bad_fail_bin.write_text('#!/bin/sh\n[ "$1" = "count" ] && exit 1 || exit 0\n')
        bad_fail_bin.chmod(0o700)
        self.assertIsNone(client._fresh_executor_outbox_readback(self.workspace, gateways, bad_fail_bin))

        # 15f. Ineligible post-stop drain when actual_stop_confirmed is False
        ineligible_artifact = {
            'status': 'PARTIAL', 'actual_stop_confirmed': False, 'primary_error': None, 'cleanup_errors': [],
            'children': [{'status': 'stopped'}, {'status': 'stopped'}],
            'pending_outbox_readback': [
                {'status': 'readback', 'pending_rows': 1, 'quarantined_rows': 0},
                {'status': 'readback', 'pending_rows': 0, 'quarantined_rows': 0},
            ],
        }
        self.assertIsNone(client._complete_e3_poststop_drain(
            self.workspace, fake_spec, ineligible_artifact, self.gateway_bin, expected_jobs=None))

        # 15g. Ineligible post-stop drain when primary_error is present
        ineligible_err_artifact = {
            'status': 'PARTIAL', 'actual_stop_confirmed': True, 'primary_error': 'timeout', 'cleanup_errors': [],
            'children': [{'status': 'stopped'}, {'status': 'stopped'}],
            'pending_outbox_readback': [
                {'status': 'readback', 'pending_rows': 1, 'quarantined_rows': 0},
                {'status': 'readback', 'pending_rows': 0, 'quarantined_rows': 0},
            ],
        }
        self.assertIsNone(client._complete_e3_poststop_drain(
            self.workspace, fake_spec, ineligible_err_artifact, self.gateway_bin, expected_jobs=None))

        # 15h. Ineligible post-stop drain when cleanup_errors is not empty
        ineligible_cleanup_artifact = {
            'status': 'PARTIAL', 'actual_stop_confirmed': True, 'primary_error': None,
            'cleanup_errors': ['child_failed'],
            'children': [{'status': 'stopped'}, {'status': 'stopped'}],
            'pending_outbox_readback': [
                {'status': 'readback', 'pending_rows': 1, 'quarantined_rows': 0},
                {'status': 'readback', 'pending_rows': 0, 'quarantined_rows': 0},
            ],
        }
        self.assertIsNone(client._complete_e3_poststop_drain(
            self.workspace, fake_spec, ineligible_cleanup_artifact, self.gateway_bin, expected_jobs=None))


if __name__ == '__main__':
    unittest.main()
