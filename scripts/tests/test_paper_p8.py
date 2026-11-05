import subprocess
import tempfile
import unittest
from pathlib import Path
import io
import json
import os
import sqlite3
from unittest import mock

import paper_p8
import math


class PaperP8EntrypointTests(unittest.TestCase):
    def test_local_resource_preflight_reports_actual_and_unavailable_host_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / 'manifest.json'
            manifest.write_text(json.dumps({'roles':['viewer'], 'topology':{'viewers':2},
                                            'limits':{'connections':3}}))
            report = paper_p8.local_resource_preflight([{'manifest':str(manifest), 'role':'viewer'}])
        self.assertEqual(report['scope'], 'B_runner_process_and_host_readonly')
        self.assertEqual(report['roles'][0]['estimated_fds'], 71)
        self.assertIn('ephemeral_port_range_count', report)

    def test_phase_prefix_validation_and_safe_capture(self):
        event = {'event':'measure_begin','elapsed_ns':7,'configured_phases':{},
                 'counters':{key:0 for key in paper_p8.PHASE_COUNTERS}}
        pipe = io.StringIO('P8_PHASE ' + json.dumps(event) + '\nP8_PHASE {bad}\npassword=private\n')
        captured, errors, lines = [], [], []
        paper_p8._pump_lines(pipe, lines, __import__('threading').Event(), 'paperload-telemetry:x', captured, errors)
        self.assertEqual(len(captured), 1)
        self.assertIn('observed_at_runner_monotonic_ns', captured[0])
        self.assertEqual(errors, ['phase_line_invalid_or_over_limit'])
        self.assertNotIn('private', repr(lines))

    def test_phase_prefix_rejects_nonfinite_malformed_oversize_and_count(self):
        valid = {'event':'measure_begin','elapsed_ns':1,'configured_phases':{},
                 'counters':{key:0 for key in paper_p8.PHASE_COUNTERS}}
        bad = dict(valid, elapsed_ns=float('nan'))
        lines = ['P8_PHASE ' + json.dumps(bad)] + ['P8_PHASE ' + json.dumps(valid)] * 17
        lines.append('P8_PHASE ' + ('x' * 4100))
        captured, errors = [], []
        paper_p8._pump_lines(io.StringIO('\n'.join(lines)+'\n'), [], __import__('threading').Event(), 'child', captured, errors)
        self.assertEqual(len(captured), 16)
        self.assertTrue(errors)

    def test_control_phase_role_has_no_telemetry_counter_contract(self):
        event = {'event':'request_begin','elapsed_ns':12,'command_ordinal':2,
                 'clock_qualification':'process-local elapsed from RunControl origin; not server latency'}
        captured, errors = [], []
        paper_p8._pump_lines(io.StringIO('P8_PHASE '+json.dumps(event)+'\n'), [],
                             __import__('threading').Event(), 'paperload-control:pilot', captured, errors)
        self.assertEqual(errors, [])
        self.assertEqual(captured[0]['command_ordinal'], 2)
        self.assertIn('observed_at_runner_monotonic_ns', captured[0])
        bad = dict(event, command_ordinal=True)
        captured, errors = [], []
        paper_p8._pump_lines(io.StringIO('P8_PHASE '+json.dumps(bad)+'\n'), [],
                             __import__('threading').Event(), 'paperload-control:pilot', captured, errors)
        self.assertFalse(captured)
        self.assertTrue(errors)

    def paths(self, directory):
        root = Path(directory)
        binary, manifest = root / 'paper load', root / 'manifest file.json'
        binary.touch(); manifest.write_text('{}')
        return str(binary), str(manifest), str(root / 'output dir')

    def test_invokes_binary_with_path_objects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = root / 'paper load', root / 'manifest file.json'
            binary.touch(); manifest.write_text('{}')
            output = root / 'output dir'
            with mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, 'ok', '')) as run:
                result = paper_p8.invoke_paperload(binary, manifest, output, official=True, timeout=8)
            self.assertEqual(result.stdout, 'ok')
            args, _ = run.call_args
            self.assertEqual(args[0], [str(binary.resolve()), '--manifest', str(manifest.resolve()),
                                       '--output-dir', str(output.resolve()), '--preflight', '--official-opt-in'])

    def test_invokes_binary_with_argv_no_shell_and_bounded_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, manifest, output = self.paths(directory)
            with mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, 'ok', '')) as run:
                result = paper_p8.invoke_paperload(binary, manifest, output, official=True, timeout=8)
        self.assertEqual(result.stdout, 'ok')
        args, kwargs = run.call_args
        self.assertEqual(args[0], [str(Path(binary).resolve()), '--manifest', str(Path(manifest).resolve()),
                                   '--output-dir', str(Path(output).resolve()), '--preflight', '--official-opt-in'])
        self.assertFalse(kwargs['shell'])
        self.assertEqual(kwargs['timeout'], 8)

    def test_classifies_timeout_and_does_not_include_child_secret_output(self):
        error = subprocess.TimeoutExpired(['paperload'], 3, output='password=secret')
        with tempfile.TemporaryDirectory() as directory:
            binary, manifest, output = self.paths(directory)
            with mock.patch.object(paper_p8.subprocess, 'run', side_effect=error):
                with self.assertRaisesRegex(paper_p8.PaperloadTimeout, 'timed out') as raised:
                    paper_p8.invoke_paperload(binary, manifest, output, timeout=3)
        self.assertNotIn('secret', str(raised.exception))

    def test_classifies_oserror_and_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, manifest, output = self.paths(directory)
            with mock.patch.object(paper_p8.subprocess, 'run', side_effect=OSError('private path')):
                with self.assertRaisesRegex(paper_p8.PaperloadLaunchError, 'could not be started'):
                    paper_p8.invoke_paperload(binary, manifest, output)
            with mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 2, '', 'invalid')):
                with self.assertRaisesRegex(paper_p8.PaperloadExitError, 'exited with code 2'):
                    paper_p8.invoke_paperload(binary, manifest, output)

    def test_paths_required_and_flags_cannot_be_forwarded_arbitrarily(self):
        with self.assertRaises(paper_p8.PaperloadInputError):
            paper_p8.invoke_paperload('/does-not-exist', '', 'out')

    def test_gateway_password_file_is_loaded_only_into_runtime_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); root.chmod(0o700)
            password = root / 'password'; password.write_text('tls-secret-value'); password.chmod(0o600)
            env_path = root / 'gateway.json'
            config = {'GATEWAY_CONTROL_ENABLED':'true', 'GATEWAY_ID':'gw', 'SENSOR_ID':'sensor',
                'GATEWAY_MQTT_BROKER_URL':'ssl://broker.example.test:8883',
                'GATEWAY_MQTT_CLIENT_ID':'client', 'GATEWAY_MQTT_USERNAME':'gw',
                'GATEWAY_MQTT_PASSWORD_FILE':str(password), 'GATEWAY_MQTT_CA_FILE':str(root/'ca.pem'),
                'GATEWAY_OUTBOX_PATH':str(root/'outbox.sqlite')}
            env_path.write_text(json.dumps(config)); env_path.chmod(0o600)
            loaded = paper_p8._gateway_environment(str(env_path))
            self.assertEqual(loaded['GATEWAY_MQTT_PASSWORD'], 'tls-secret-value')
            self.assertNotIn('GATEWAY_MQTT_PASSWORD', json.loads(env_path.read_text()))


class FakeChild:
    next_pid = 100

    def __init__(self, argv, *, ready=True, terminate_error=False, wait_timeout=False,
                 artifact=None, exit_before_ready=False, events=None, signal_on_wait=False):
        FakeChild.next_pid += 1
        self.pid = FakeChild.next_pid
        self.argv = argv
        self.returncode = 1 if exit_before_ready else None
        output = 'gateway simulator running gateway=g sensor=s boot_id=b\n' if ready else ''
        if '--run' in argv:
            counters = {key: 0 for key in paper_p8.PHASE_COUNTERS}
            for name in ('warmup_begin','warmup_end','measure_begin','measure_end','burst_begin',
                         'burst_end','generation_stopped','drain_begin','drain_end','workers_joined'):
                output += 'P8_PHASE ' + json.dumps({'event':name,'elapsed_ns':len(output),
                    'configured_phases':{},'counters':counters}) + '\n'
        self.stdout = io.StringIO(output)
        self.terminate_error = terminate_error
        self.wait_timeout = wait_timeout
        self.artifact = artifact
        self.signal_on_wait = signal_on_wait
        self.events = events if events is not None else []

    def poll(self):
        return self.returncode

    def terminate(self):
        self.events.append(('terminate', self.argv[-1]))
        if self.terminate_error:
            raise OSError('private detail')
        self.returncode = -15

    def kill(self):
        self.events.append(('kill', self.argv[-1]))
        self.returncode = -9

    def wait(self, timeout=None):
        self.events.append(('wait', self.argv[-1]))
        if self.signal_on_wait:
            self.signal_on_wait = False
            import signal
            signal.raise_signal(signal.SIGTERM)
        if self.wait_timeout:
            raise subprocess.TimeoutExpired(self.argv, timeout)
        if self.artifact:
            self.artifact.parent.mkdir(parents=True, exist_ok=True)
            self.artifact.write_text('{"status":"COMPLETE"}')
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class PaperP8LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir='/var/tmp/opencode')
        self.root = Path(self.temp.name)
        self.binary = self.root / 'paperload'
        self.gateway = self.root / 'gateway'
        self.manifest = self.root / 'manifest.json'
        self.outbox = self.root / 'persistent.sqlite'
        self.env_file = self.root / 'gateway-env.json'
        self.artifact = self.root / 'run.json'
        self.binary.touch(); self.gateway.touch()
        self.manifest.write_text(json.dumps({'limits': {'wall_seconds': 1}}))
        self.outbox.write_bytes(b'pending bytes must stay')
        values = {
            'GATEWAY_CONTROL_ENABLED': 'true', 'GATEWAY_ID': 'gw-real', 'SENSOR_ID': 'sensor-real',
            'GATEWAY_MQTT_BROKER_URL': 'ssl://broker:8883', 'GATEWAY_MQTT_CLIENT_ID': 'client-unique',
            'GATEWAY_MQTT_USERNAME': 'gw-real', 'GATEWAY_MQTT_PASSWORD': 'private',
            'GATEWAY_MQTT_CA_FILE': '/protected/ca.pem', 'GATEWAY_OUTBOX_PATH': str(self.outbox),
        }
        self.env_file.write_text(json.dumps(values)); self.env_file.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def run_with(self, fake_factory, *, jobs=(), gateways=None):
        return paper_p8.run_supervised(
            binary=str(self.binary), gateway_binary=str(self.gateway), jobs=jobs,
            gateways=gateways if gateways is not None else [{'env_file': str(self.env_file), 'role': 'executor', 'ready_timeout_seconds': .05}],
            stop_timeout=30, run_artifact=str(self.artifact), popen=fake_factory)

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '', ''))
    def test_official_opt_in_reaches_actual_control_child_argv(self, _count):
        children = []
        def factory(argv, **kwargs):
            output = Path(argv[argv.index('--output-dir') + 1])
            child = FakeChild(argv, artifact=output / 'workload.json')
            children.append(child)
            return child
        job = {'manifest':str(self.manifest), 'output_dir':str(self.root/'control'), 'role':'control'}
        paper_p8.run_supervised(binary=str(self.binary), jobs=[job], gateways=[], stop_timeout=30,
            run_artifact=str(self.artifact), popen=factory, official=True)
        self.assertIn('--official-opt-in', children[0].argv)

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=1 pending_bytes=22 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_readiness_timeout_stops_child_and_preserves_outbox(self, _count):
        children = []
        def factory(argv, **kwargs):
            child = FakeChild(argv, ready=False); children.append(child); return child
        status, report = self.run_with(factory)
        self.assertEqual(status, 'PARTIAL')
        self.assertIn('readiness timed out', report['primary_error'])
        self.assertEqual(children[0].returncode, -15)
        self.assertTrue(self.outbox.exists())

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_early_exit_before_ready_is_primary_failure(self, _count):
        children = []
        def factory(argv, **kwargs):
            child = FakeChild(argv, exit_before_ready=True); children.append(child); return child
        status, report = self.run_with(factory)
        self.assertEqual(status, 'PARTIAL')
        self.assertIn('exited before readiness', report['primary_error'])
        self.assertTrue(children)

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_producers_run_after_readiness_and_stop_before_gateway(self, _count):
        events, children = [], []
        workload_dir = self.root / 'workload'
        def factory(argv, **kwargs):
            is_gateway = argv[-1] == 'run'
            child = FakeChild(argv, artifact=None if is_gateway else workload_dir / 'workload.json', events=events)
            children.append(child)
            return child
        job = {'manifest': str(self.manifest), 'output_dir': str(workload_dir)}
        status, report = self.run_with(factory, jobs=[job])
        self.assertEqual(status, 'COMPLETE', report)
        terminations = [event[1] for event in events if event[0] == 'terminate']
        self.assertEqual(terminations, ['run'])
        self.assertEqual([child.argv[-1] for child in children], ['run', '--run'])
        self.assertTrue(report['actual_stop_confirmed'])

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=1 pending_bytes=22 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_storage_guard_real_threshold_stops_and_flushes_owned_child(self, _count):
        child = []
        workload = self.root / 'guard-output' / 'workload.json'
        def factory(argv, **kwargs):
            item = FakeChild(argv, artifact=workload)
            child.append(item)
            return item
        status, report = paper_p8.run_supervised(binary=str(self.binary), jobs=[
            {'manifest':str(self.manifest), 'output_dir':str(self.root/'guard-output')}],
            gateways=[], stop_timeout=30, run_artifact=str(self.artifact), popen=factory,
            disk_guard={'growth_threshold_bytes':1, 'minimum_free_bytes':0,
                        'allowed_root':str(self.root), 'interval_seconds':.01})
        self.assertEqual(status, 'PARTIAL')
        self.assertIn('owned storage guard stopped workload', report['primary_error'])
        self.assertTrue(report['actual_stop_confirmed'])
        self.assertEqual(report['storage_guard']['stop_reason'], 'owned_storage_growth_threshold')
        self.assertEqual(report['pending_outbox_readback'], [])
        self.assertTrue(workload.is_file())
        self.assertIsNotNone(child[0].returncode)

    def test_storage_guard_threshold_reads_real_pending_sqlite_after_child_join(self):
        message_id = 'a1111111-1111-4111-8111-111111111111'
        payload = b'{"sensor":"sensor-real","samples":[1,2,3]}'
        self.outbox.unlink()
        with sqlite3.connect(self.outbox) as db:
            db.executescript('''
                CREATE TABLE outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL UNIQUE,
                    gateway_id TEXT NOT NULL, sensor_id TEXT NOT NULL, boot_id TEXT NOT NULL,
                    topic TEXT NOT NULL, payload BLOB NOT NULL, byte_size INTEGER NOT NULL,
                    first_sequence INTEGER NOT NULL, sample_count INTEGER NOT NULL,
                    sample_interval_us INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','quarantined')),
                    reason TEXT, enqueued_at TEXT NOT NULL
                );
                INSERT INTO outbox (message_id,gateway_id,sensor_id,boot_id,topic,payload,byte_size,
                    first_sequence,sample_count,sample_interval_us,status,enqueued_at)
                VALUES ('a1111111-1111-4111-8111-111111111111','gw-real','sensor-real','boot',
                    'telemetry','',0,1,1,10000,'pending','2026-11-05T00:00:00Z');
            ''')
            db.execute('UPDATE outbox SET payload=?, byte_size=? WHERE message_id=?',
                       (payload, len(payload), message_id))

        children = []
        workload = self.root / 'guard-output' / 'workload.json'

        class GrowingChild(FakeChild):
            def __init__(self, argv, **kwargs):
                super().__init__(argv, **kwargs)
                self.first_wait = True

            def wait(self, timeout=None):
                if self.argv[-1] == '--run' and self.first_wait:
                    self.first_wait = False
                    (self.root / 'growth.bin').write_bytes(b'x' * 128)
                    raise subprocess.TimeoutExpired(self.argv, timeout)
                return super().wait(timeout)

            def terminate(self):
                super().terminate()
                if self.argv[-1] == '--run':
                    self.artifact.parent.mkdir(parents=True, exist_ok=True)
                    self.artifact.write_text('{"status":"FLUSHED_ON_STOP"}')

        def factory(argv, **kwargs):
            child = GrowingChild(argv, artifact=workload)
            child.root = self.root
            children.append(child)
            return child

        def run_command(argv, **kwargs):
            if argv[-1] == '--preflight':
                return subprocess.CompletedProcess(argv, 0, '', '')
            with sqlite3.connect(self.outbox) as db:
                rows, byte_count = db.execute(
                    "SELECT count(*), coalesce(sum(byte_size),0) FROM outbox WHERE status='pending'"
                ).fetchone()
                quarantined, quarantine_bytes = db.execute(
                    "SELECT count(*), coalesce(sum(byte_size),0) FROM outbox WHERE status='quarantined'"
                ).fetchone()
            output = (f'pending_rows={rows} pending_bytes={byte_count} '
                      f'quarantined_rows={quarantined} quarantined_bytes={quarantine_bytes}\n')
            return subprocess.CompletedProcess(argv, 0, output, '')

        with mock.patch.object(paper_p8.subprocess, 'run', side_effect=run_command):
            status, report = paper_p8.run_supervised(binary=str(self.binary), gateway_binary=str(self.gateway),
                jobs=[{'manifest':str(self.manifest), 'output_dir':str(self.root/'guard-output')}],
                gateways=[{'env_file':str(self.env_file), 'role':'executor', 'ready_timeout_seconds':.05}],
                stop_timeout=30, run_artifact=str(self.artifact), popen=factory,
                disk_guard={'growth_threshold_bytes':1, 'minimum_free_bytes':0,
                            'allowed_root':str(self.root.parent), 'interval_seconds':.01})

        self.assertEqual(status, 'PARTIAL')
        self.assertIn('owned storage guard stopped workload', report['primary_error'])
        self.assertEqual(report['storage_guard']['stop_reason'], 'owned_storage_growth_threshold')
        self.assertTrue(report['actual_stop_confirmed'])
        self.assertTrue(all(child.returncode is not None for child in children))
        self.assertTrue(workload.is_file(), 'owned child artifact must flush during join')
        self.assertEqual(report['pending_outbox_readback'][0]['status'], 'readback')
        self.assertEqual(report['pending_outbox_readback'][0]['pending_rows'], 1)
        self.assertEqual(report['pending_outbox_readback'][0]['pending_bytes'], len(payload))
        with sqlite3.connect(self.outbox) as db:
            row = db.execute('SELECT message_id,payload,byte_size,status FROM outbox').fetchone()
        self.assertEqual(row, (message_id, payload, len(payload), 'pending'))
        self.assertEqual(report['cleanup_errors'], [])

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '', ''))
    def test_run_supervised_launches_viewer_before_telemetry_even_if_input_reversed(self, _count):
        launch_order=[]
        viewer_out=self.root/'viewer'; telemetry_out=self.root/'telemetry'
        viewer_manifest=self.root/'viewer.json'; telemetry_manifest=self.root/'telemetry.json'
        viewer_manifest.write_text(json.dumps({'roles':['viewer'],'profile':'pilot','topology':{'viewers':64},'limits':{'wall_seconds':1}}))
        telemetry_manifest.write_text(json.dumps({'roles':['telemetry'],'profile':'pilot','limits':{'wall_seconds':1}}))
        def factory(argv, **kwargs):
            role=Path(argv[argv.index('--manifest')+1]).stem
            launch_order.append(role)
            output=Path(argv[argv.index('--output-dir')+1])
            if role == 'viewer':
                output.mkdir(parents=True,exist_ok=True)
                ready=output/'ready.json'
                ready.write_text(json.dumps({'status':'ready','connected':64,'subscribed':64,'principals':2,'mode':'healthy','monotonic_origin_qualification':'process local; not comparable across processes'}))
                ready.chmod(0o600)
            return FakeChild(argv, artifact=output/'workload.json')
        status, report=paper_p8.run_supervised(binary=str(self.binary), jobs=[
            {'manifest':str(telemetry_manifest),'output_dir':str(telemetry_out),'role':'telemetry'},
            {'manifest':str(viewer_manifest),'output_dir':str(viewer_out),'role':'viewer'}],
            gateways=[],stop_timeout=30,run_artifact=str(self.artifact),popen=factory)
        self.assertEqual(status,'COMPLETE',report)
        self.assertEqual(launch_order,['viewer','telemetry'])

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_sigterm_uses_idempotent_producer_then_gateway_drain(self, _count):
        events = []
        workload_dir = self.root / 'signal-workload'
        def factory(argv, **kwargs):
            is_gateway = argv[-1] == 'run'
            return FakeChild(argv, artifact=None if is_gateway else workload_dir / 'workload.json',
                             signal_on_wait=not is_gateway, events=events)
        job = {'manifest': str(self.manifest), 'output_dir': str(workload_dir)}
        status, report = self.run_with(factory, jobs=[job])
        self.assertEqual(status, 'PARTIAL')
        self.assertIn('KeyboardInterrupt', report['primary_error'])
        self.assertEqual([event[1] for event in events if event[0] == 'terminate'], ['--run', 'run'])

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_oserror_and_timeout_during_cleanup_do_not_skip_later_children(self, _count):
        events, children = [], []
        def factory(argv, **kwargs):
            child = FakeChild(argv, terminate_error=True, wait_timeout=True, events=events)
            children.append(child); return child
        second_env = self.root / 'gateway-env-second.json'
        second_values = json.loads(self.env_file.read_text())
        second_values['GATEWAY_MQTT_CLIENT_ID'] = 'client-unique-2'
        second_values['SENSOR_ID'] = 'sensor-second'
        second_values['GATEWAY_OUTBOX_PATH'] = str(self.root / 'second.sqlite')
        Path(second_values['GATEWAY_OUTBOX_PATH']).write_bytes(b'pending')
        second_env.write_text(json.dumps(second_values)); second_env.chmod(0o600)
        gateways = [{'env_file': str(self.env_file), 'role': 'executor', 'ready_timeout_seconds': .05},
                    {'env_file': str(second_env), 'role': 'executor', 'ready_timeout_seconds': .05}]
        status, report = self.run_with(factory, gateways=gateways)
        self.assertEqual(status, 'PARTIAL')
        self.assertGreaterEqual(len([event for event in events if event[0] == 'terminate']), 2)
        self.assertTrue(any('terminate: OSError' in err for err in report['cleanup_errors']))
        self.assertTrue(any('join timed out' in err for err in report['cleanup_errors']))

    @mock.patch.object(paper_p8.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0,
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n', ''))
    def test_missing_or_malformed_workload_artifact_is_failure(self, _count):
        for malformed in (False, True):
            output = self.root / ('malformed' if malformed else 'no-output')
            def factory(argv, **kwargs):
                child = FakeChild(argv)
                if argv[0] == str(self.binary):
                    if malformed:
                        child.artifact = output / 'workload.json'
                        child.wait = lambda timeout=None: (output.mkdir(parents=True, exist_ok=True),
                                                           child.artifact.write_text('{')) and 0
                    else:
                        child.artifact = None
                return child
            job = {'manifest': str(self.manifest), 'output_dir': str(output)}
            status, report = self.run_with(factory, jobs=[job], gateways=[])
            self.assertEqual(status, 'PARTIAL')
            self.assertIn('artifact is missing', report['primary_error'])

    def test_credential_file_must_be_private_and_allowlisted(self):
        self.env_file.chmod(0o644)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'private regular'):
            paper_p8._gateway_environment(str(self.env_file))


class PaperP8SpecTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        from datetime import datetime, timezone
        from paper_p8_e2_support import build_e2_equal_samples_context, e2_equal_samples_manifests
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {'mode': 'remote', 'publish_timeout_ms': 3000, 'tls': {}}
        targets = [{'gateway_id': 'gw-1', 'sensor_id': 's-1'}, {'gateway_id': 'gw-2', 'sensor_id': 's-2'}]
        gw_files = {'gw-1': 'g1', 'gw-2': 'g2'}
        tele_files = {'A': 't1', 'B': 't2'}
        all_manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)
        pair = all_manifests['profiles']['100']
        self.manifest_e2_a = self.root / 'e2_a.json'
        self.manifest_e2_a.write_text(json.dumps(pair['A']))
        self.manifest_e2_b = self.root / 'e2_b.json'
        self.manifest_e2_b.write_text(json.dumps(pair['B']))

        self.manifest_legacy = self.root / 'legacy.json'
        self.manifest_legacy.write_text(json.dumps({
            'experiment': 'E1', 'profile': 'pilot', 'roles': ['telemetry'],
            'limits': {'wall_seconds': 30}
        }))

    def tearDown(self):
        self.temp.cleanup()

    def _write_spec(self, paperload_jobs, stop_timeout=None, gateways=None):
        spec_file = self.root / 'run-spec.json'
        spec = {'paperload': paperload_jobs}
        if stop_timeout is not None:
            spec['stop_timeout_seconds'] = stop_timeout
        if gateways is not None:
            spec['gateways'] = gateways
        spec_file.write_text(json.dumps(spec))
        return spec_file

    def test_e2_equal_samples_allows_stop_timeout_up_to_300(self):
        # Realistic 90s spec generated by bundle / e2_support
        jobs = [
            {'manifest': str(self.manifest_e2_a), 'output_dir': str(self.root / 'out_a')},
            {'manifest': str(self.manifest_e2_b), 'output_dir': str(self.root / 'out_b')},
        ]
        spec_90 = self._write_spec(jobs, stop_timeout=90)
        jobs_res, gws_res, stop_timeout, _ = paper_p8._read_spec(str(spec_90), None, None)
        self.assertEqual(stop_timeout, 90.0)
        self.assertEqual(len(jobs_res), 2)

        spec_300 = self._write_spec(jobs, stop_timeout=300)
        _, _, stop_timeout_300, _ = paper_p8._read_spec(str(spec_300), None, None)
        self.assertEqual(stop_timeout_300, 300.0)

    def test_e2_equal_samples_rejects_stop_timeout_over_300(self):
        jobs = [
            {'manifest': str(self.manifest_e2_a), 'output_dir': str(self.root / 'out_a')},
            {'manifest': str(self.manifest_e2_b), 'output_dir': str(self.root / 'out_b')},
        ]
        spec_over = self._write_spec(jobs, stop_timeout=300.5)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'stop timeout must be between 0 and 300 seconds'):
            paper_p8._read_spec(str(spec_over), None, None)

    def test_legacy_workload_retains_cap_60(self):
        jobs = [{'manifest': str(self.manifest_legacy), 'output_dir': str(self.root / 'out_leg')}]
        spec_90 = self._write_spec(jobs, stop_timeout=90)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'stop timeout must be between 0 and 60 seconds'):
            paper_p8._read_spec(str(spec_90), None, None)

        spec_61 = self._write_spec(jobs, stop_timeout=61)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'stop timeout must be between 0 and 60 seconds'):
            paper_p8._read_spec(str(spec_61), None, None)

        spec_60 = self._write_spec(jobs, stop_timeout=60)
        _, _, timeout_60, _ = paper_p8._read_spec(str(spec_60), None, None)
        self.assertEqual(timeout_60, 60.0)

        spec_30 = self._write_spec(jobs, stop_timeout=30)
        _, _, timeout_30, _ = paper_p8._read_spec(str(spec_30), None, None)
        self.assertEqual(timeout_30, 30.0)

    def test_mixed_workload_retains_cap_60(self):
        # 1 E2 job + 1 legacy job
        jobs = [
            {'manifest': str(self.manifest_e2_a), 'output_dir': str(self.root / 'out_a')},
            {'manifest': str(self.manifest_legacy), 'output_dir': str(self.root / 'out_leg')},
        ]
        spec_90 = self._write_spec(jobs, stop_timeout=90)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'stop timeout must be between 0 and 60 seconds'):
            paper_p8._read_spec(str(spec_90), None, None)

    def test_e2_equal_samples_with_gateways_retains_cap_60(self):
        env_file = self.root / 'gw.json'
        env_file.write_text('{"GATEWAY_ID":"gw"}')
        jobs = [{'manifest': str(self.manifest_e2_a), 'output_dir': str(self.root / 'out_a')}]
        gateways = [{'env_file': str(env_file), 'role': 'executor'}]
        spec_90 = self._write_spec(jobs, stop_timeout=90, gateways=gateways)
        with self.assertRaisesRegex(paper_p8.PaperloadInputError, 'stop timeout must be between 0 and 60 seconds'):
            paper_p8._read_spec(str(spec_90), None, None)

    def test_rejects_booleans_nan_inf_and_nonpositive(self):
        jobs_e2 = [{'manifest': str(self.manifest_e2_a), 'output_dir': str(self.root / 'out_a')}]
        jobs_leg = [{'manifest': str(self.manifest_legacy), 'output_dir': str(self.root / 'out_leg')}]
        for jobs, cap in ((jobs_e2, 300), (jobs_leg, 60)):
            for bad_val in (True, False, 0, -1, -0.5, float('nan'), float('inf'), float('-inf')):
                spec = self._write_spec(jobs, stop_timeout=bad_val)
                with self.assertRaisesRegex(paper_p8.PaperloadInputError, f'stop timeout must be between 0 and {cap} seconds'):
                    paper_p8._read_spec(str(spec), None, None)


if __name__ == '__main__':
    unittest.main()
