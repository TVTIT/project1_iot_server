import subprocess
import tempfile
import unittest
from pathlib import Path
import io
import json
import os
from unittest import mock

import paper_p8
import math


class PaperP8EntrypointTests(unittest.TestCase):
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
        self.temp = tempfile.TemporaryDirectory()
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


if __name__ == '__main__':
    unittest.main()
