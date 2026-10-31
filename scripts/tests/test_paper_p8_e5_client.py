"""E5 B holder tests with the real E5 fixture builder and offline Go preflight."""
import hashlib
import json
import os
import fcntl
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

HERE = Path(__file__).resolve()
for parent in (str(HERE.parent), str(HERE.parents[1])):
    if parent not in sys.path:
        sys.path.insert(0, parent)
import paper_p8_e5_client as holder
import paper_p8_e5_support as e5
import paper_p8_e5_fixture as e5_fixture


class FakeSampler:
    instances = []
    def __init__(self, **kwargs): self.kwargs, self.stopped = kwargs, False; FakeSampler.instances.append(self)
    def start(self): return self
    def stop(self): self.stopped = True
    def report(self): return {'summary': {'total_samples': 1, 'available_samples': 1}, 'samples': []}
    def write_csv(self, path): Path(path).write_text('sample\n', encoding='utf-8')


class FakeChild:
    current = None
    def __init__(self, argv, **kwargs):
        self.argv, self.returncode, self.pid = argv, None, 876543
        FakeChild.current = self
        self.output = Path(argv[argv.index('--output-dir') + 1])
        assert not self.output.exists(), 'holder must not create Go output leaf'
        self.output.mkdir(mode=0o700)
        ready = {'status': 'ready', 'connected': 2, 'subscribed': 2, 'principals': 2,
                 'mode': 'healthy', 'monotonic_origin_qualification': 'process-local'}
        path = self.output / 'ready.json'; path.write_text(json.dumps(ready)); path.chmod(0o600)
    def poll(self): return self.returncode
    def wait(self, timeout=None):
        if self.returncode is None: self.returncode = 0
        return self.returncode
    def terminate(self): self.returncode = -signal.SIGTERM
    def kill(self): self.returncode = -signal.SIGKILL


def _prepare_actual_e1_viewer(root: Path):
    """Call the existing E5 input builder; only its provisioning REST call is mocked."""
    root.mkdir(mode=0o700); root.chmod(0o700)
    source = root / 'source'; source.mkdir(mode=0o700)
    manifest_root = root / 'inputs'; manifest_root.mkdir(mode=0o700)
    sensor_ids = ['e5-main-a', 'e5-main-b']
    anchor, seed = '2026-10-29T00:00:00Z', 805
    dataset_path = source / 'dataset.jsonl'
    built = e5.build_dataset(seed=seed, sensor_ids=sensor_ids, anchor=anchor, path=dataset_path)
    gateways = list(e5_fixture.GATEWAYS)
    context = {'sha256': built['sha256'], 'bytes': built['bytes'], 'gateway_ids': gateways,
               'sensor_ids': sensor_ids, 'seed': seed, 'anchor': anchor}
    subscriptions = [
        {'viewer_id': 'e5-viewer-a', 'user_id': 'user-a', 'gateway_id': gateways[0], 'sensor_id': sensor_ids[0]},
        {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': gateways[1]},
        {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': gateways[1], 'sensor_id': sensor_ids[1]},
    ]
    class Smoke:
        @staticmethod
        def http_request(endpoint, method, route, **kwargs):
            if method == 'PUT':
                sid = route.rsplit('/', 1)[-1]
                return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:{gateways[0]}:{sid}'}
            return 200, {}, {'desired_version': '1'}
    endpoint = SimpleNamespace(url='https://localhost', ca_file=str(root / 'ca.pem'))
    specs = [SimpleNamespace(gateway_id=gateways[0], sensor_id=sensor_ids[0], sensor_unit='C'),
             SimpleNamespace(gateway_id=gateways[1], sensor_id=sensor_ids[1], sensor_unit='C')]
    prepared = e5_fixture.prepare_inputs(smoke=Smoke, fixture=SimpleNamespace(project='e5-holder-test'),
        endpoint=endpoint, specs=specs, passwords=['pw-a', 'pw-b'], tokens={'a': 'jwt-a', 'b': 'jwt-b'},
        mqtt_broker_url='mqtts://localhost:8883', mqtt_manifest_endpoint='mqtts://localhost:8883',
        mqtt_ca_file=str(root / 'mqtt-ca.pem'), journal_path=dataset_path, journal_context=context,
        seed=seed, anchor=anchor, root=manifest_root, command_keys=e5.command_idempotency_keys(seed),
        viewer_subscriptions=subscriptions)
    return Path(prepared['manifests']['viewer'])


class HolderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix='e5-holder-go-', dir='/var/tmp/opencode')
        cls.scratch_root = Path(cls.scratch.name); cls.scratch_root.chmod(0o700)
        go = shutil.which('go')
        if not go:
            raise unittest.SkipTest('Go unavailable; offline real-preflight check skipped')
        cls.binary = cls.scratch_root / 'paperload'
        built = subprocess.run([go, 'build', '-o', str(cls.binary), './cmd/paperload'],
                               cwd=HERE.parents[2] / 'src', capture_output=True, text=True, timeout=300)
        if built.returncode:
            raise RuntimeError('paperload_build_failed: ' + built.stderr[-1500:])
        cls.binary.chmod(0o700)
        cls.manifest = _prepare_actual_e1_viewer(cls.scratch_root / 'fixture')

    @classmethod
    def tearDownClass(cls): cls.scratch.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='e5-holder-test-', dir='/var/tmp/opencode')
        self.root = Path(self.temp.name); self.root.chmod(0o700)
        self.output, self.control = self.root / 'output', self.root / 'control'
        self.control.mkdir(mode=0o700)
        manifest = json.loads(self.manifest.read_text())
        self.data = manifest
        self.correlation = {'session_id': 'e5-session-1', 'run_id': manifest['run_id'],
            'binding_digest': 'a' * 64, 'manifest_sha256': holder._digest(self.manifest),
            'binary_sha256': holder._digest(self.binary)}
        FakeSampler.instances.clear()

    def tearDown(self): self.temp.cleanup()

    def _planned_result(self, path):
        result = {'status': 'COMPLETE', 'experiment': self.data['experiment'], 'arm': self.data['arm'],
            'profile': self.data['profile'], 'mode': self.data['mode'], 'run_id': self.data['run_id'],
            'planned_stop': True, 'planned_stop_reason': 'stopped_by_operator',
            'connection_attempts': 2, 'socket_count': 2, 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2, 'principal_count': 2,
            'reconnections': 0, 'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0}
        path.write_text(json.dumps(result)); path.chmod(0o600)

    def _install_child(self):
        def kill(pid, sig):
            self.assertEqual((pid, sig), (876543, signal.SIGUSR1))
            self._planned_result(self.output / 'workload.json')
            FakeChild.current.returncode = 0
        return mock.patch.object(holder.subprocess, 'run', return_value=SimpleNamespace(returncode=0)), \
            mock.patch.object(holder.subprocess, 'Popen', FakeChild), \
            mock.patch.object(holder.os, 'kill', kill), mock.patch.object(holder, 'GeneratorResourceSampler', FakeSampler)

    def test_real_go_preflight_actual_built_e1_viewer_manifest(self):
        manifest = json.loads(self.manifest.read_text())
        self.assertEqual(manifest['experiment'], 'E1')
        self.assertEqual(manifest['roles'], ['viewer'])
        result = subprocess.run([str(self.binary), '--manifest', str(self.manifest), '--preflight'],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('PREFLIGHT_ONLY_NOT_RUN', result.stdout)

    def _run_and_drive(self, phases=holder.PHASE_SEQUENCE, result_events=20, deadline=10,
                       premature_complete_after_initial=False):
        preflight_patch, child_popen, kill_patch, sampler_patch = self._install_child()
        if result_events != 20:
            old = self._planned_result
            def custom(path):
                old(path)
                data = json.loads(path.read_text()); data['unique_events'] = result_events
                path.write_text(json.dumps(data)); path.chmod(0o600)
            self._planned_result = custom
        holder_errors = []
        def drive():
            try:
                limit = time.monotonic() + 8
                while not (self.control / 'status.json').exists():
                    if time.monotonic() > limit: raise AssertionError('holder did not become ready')
                    time.sleep(.005)
                while holder.read_viewer_holder_status(self.control).get('state') != 'READY':
                    if time.monotonic() > limit: raise AssertionError('holder did not become ready')
                    time.sleep(.005)
                for phase, boundary in phases:
                    holder.request_viewer_phase(self.control, self.correlation, phase, boundary, ack_timeout_seconds=4)
                try:
                    holder.request_viewer_stop(self.control, self.correlation)
                except holder.HolderError:
                    if not premature_complete_after_initial:
                        raise
                    # Even a forged/early COMPLETE with all 20 events cannot replace
                    # the missing lifecycle boundaries or authorize a planned stop.
                    self._planned_result(self.output / 'workload.json')
                    FakeChild.current.returncode = 0
            except BaseException as exc:
                holder_errors.append(exc)
        with preflight_patch, child_popen, kill_patch, sampler_patch:
            driver = threading.Thread(target=drive)
            driver.start()
            try:
                with mock.patch.object(holder, 'GeneratorResourceSampler', FakeSampler):
                    output = holder.run_viewer_holder(self.binary, self.manifest, self.output, self.control,
                                                      self.correlation, deadline, poll_seconds=.01)
            except BaseException:
                driver.join(timeout=5)
                if holder_errors: raise holder_errors[0]
                raise
            driver.join(timeout=5)
        if driver.is_alive(): self.fail('phase request driver did not finish')
        if holder_errors: raise holder_errors[0]
        return output

    def test_six_phase_checkpoints_and_planned_stop_proof(self):
        result = self._run_and_drive()
        proof = json.loads((self.control / 'lifecycle.json').read_text())
        self.assertEqual(proof['schema_version'], 1)
        self.assertEqual(proof['clock'], 'viewer_holder_local_elapsed_seconds')
        self.assertEqual([(c['phase'], c['boundary']) for c in proof['checkpoints']], list(holder.PHASE_SEQUENCE))
        times = [proof['ready_elapsed_seconds']] + [c['elapsed_seconds'] for c in proof['checkpoints']] + [proof['stop_elapsed_seconds']]
        self.assertEqual(times, sorted(times)); self.assertTrue(proof['joined'])
        self.assertEqual(result['result']['experiment'], 'E1')
        complete = json.loads((self.control / 'holder-result.json').read_text())
        self.assertEqual(complete['correlation'], self.correlation)
        self.assertEqual(complete['binary_provenance']['sha256'], self.correlation['binary_sha256'])
        status = holder.read_viewer_holder_status(self.control)
        self.assertEqual(status['state'], 'STOPPED'); self.assertTrue(status['joined'])
        self.assertEqual(FakeChild.current.argv[-1], '--viewer-stop-on-sigusr1')
        self.assertTrue(self.output.exists())
        self.assertTrue((self.output / 'resources-generator.csv').is_file())
        resources = json.loads((self.control / 'resources-generator.json').read_text())
        self.assertEqual(resources['scope'], 'viewer_child_process_tree')
        self.assertTrue(resources['includes_idle_and_manual_transfer_time'])
        self.assertTrue(FakeSampler.instances[-1].stopped)

    def test_actual_twenty_event_result_after_initial_only_is_not_success(self):
        with self.assertRaisesRegex(holder.HolderError, 'child_exited_before_join'):
            self._run_and_drive(holder.PHASE_SEQUENCE[:2], result_events=20, deadline=5,
                                premature_complete_after_initial=True)
        self.assertEqual(holder.read_viewer_holder_status(self.control)['state'], 'FAILED')
        result = json.loads((self.output / 'workload.json').read_text())
        self.assertEqual(result['unique_events'], 20)
        self.assertTrue(result['planned_stop'])
        self.assertFalse((self.control / 'lifecycle.json').exists())

    def test_wrong_order_and_stale_holder_proof_rejected(self):
        with self.assertRaisesRegex(holder.HolderError, 'holder_status_missing'):
            holder.request_viewer_phase(self.control, self.correlation, 'initial', 'begin')
        lock_path = self.control / 'viewer-holder.lock'
        lock_path.touch(mode=0o600)
        status = {'schema_version': 1, 'state': 'READY', 'correlation': self.correlation,
                  'owner_token': 'stale-owner', 'next_checkpoint_index': 0, 'checkpoints': []}
        (self.control / 'status.json').write_text(json.dumps(status)); (self.control / 'status.json').chmod(0o600)
        with self.assertRaisesRegex(holder.HolderError, 'holder_not_active'):
            holder.request_viewer_phase(self.control, self.correlation, 'initial', 'begin')

    def test_wrong_phase_or_session_and_missing_boundary_rejected(self):
        with self.assertRaises(TypeError):
            holder.request_viewer_phase(self.control, self.correlation, 'initial')
        fd = os.open(self.control / 'viewer-holder.lock', os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (self.control / 'status.json').write_text(json.dumps({'state': 'READY', 'correlation': self.correlation,
            'owner_token': 'owner', 'next_checkpoint_index': 0, 'checkpoints': []})); (self.control / 'status.json').chmod(0o600)
        try:
            with self.assertRaisesRegex(holder.HolderError, 'holder_not_ready_or_correlation_mismatch'):
                holder.request_viewer_phase(self.control, {**self.correlation, 'session_id': 'wrong'}, 'initial', 'begin')
            with self.assertRaisesRegex(holder.HolderError, 'phase_boundary_order_invalid'):
                holder.request_viewer_phase(self.control, self.correlation, 'warm_replay', 'begin')
        finally: os.close(fd)

    def test_stale_ready_output_is_rejected_before_child_launch(self):
        self.output.mkdir(mode=0o700)
        stale = self.output / 'ready.json'; stale.write_text(json.dumps({'status': 'ready'})); stale.chmod(0o600)
        with self.assertRaisesRegex(holder.HolderError, 'output_directory_not_fresh'):
            holder.run_viewer_holder(self.binary, self.manifest, self.output, self.control,
                                     self.correlation, 5)
        self.assertEqual(json.loads(stale.read_text()), {'status': 'ready'})

    def test_second_holder_cannot_acquire_control_lock(self):
        lock_fd = os.open(self.control / 'viewer-holder.lock', os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with self.assertRaisesRegex(holder.HolderError, 'holder_already_active'):
                holder.run_viewer_holder(self.binary, self.manifest, self.output, self.control,
                                         self.correlation, 5)
        finally:
            os.close(lock_fd)

    def test_atomic_stop_request_is_invisible_until_fully_written_and_duplicate_rejected(self):
        fd = os.open(self.control / 'viewer-holder.lock', os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status = {'state': 'PHASE', 'correlation': self.correlation, 'owner_token': 'owner',
            'next_checkpoint_index': 6, 'checkpoints': [{'phase': p, 'boundary': b} for p, b in holder.PHASE_SEQUENCE]}
        path = self.control / 'status.json'; path.write_text(json.dumps(status)); path.chmod(0o600)
        reached_link, release_link = threading.Event(), threading.Event()
        real_link = holder.os.link
        def paused_link(*args, **kwargs):
            reached_link.set()
            if not release_link.wait(3): raise AssertionError('test did not release atomic link')
            return real_link(*args, **kwargs)
        outcomes = []
        with mock.patch.object(holder.os, 'link', paused_link):
            thread = threading.Thread(target=lambda: outcomes.append(holder.request_viewer_stop(self.control, self.correlation)))
            thread.start()
            self.assertTrue(reached_link.wait(2))
            self.assertFalse((self.control / 'stop-request.json').exists())
            release_link.set(); thread.join(3)
        self.assertFalse(thread.is_alive()); self.assertEqual(len(outcomes), 1)
        with self.assertRaisesRegex(holder.HolderError, 'stop_request_exists'):
            holder.request_viewer_stop(self.control, self.correlation)
        os.close(fd)

    def test_output_leaf_is_absent_at_launch_and_credentials_never_enter_argv(self):
        preflight_patch, child_popen, kill_patch, sampler_patch = self._install_child()
        with preflight_patch, child_popen, kill_patch, sampler_patch:
            # Real preflight has its own offline test; fake child proves launch args/leaf timing.
            with self.assertRaises(holder.HolderError):
                holder.run_viewer_holder(self.binary, self.manifest, self.output, self.control,
                                         self.correlation, 1, poll_seconds=.01)
        self.assertNotIn('jwt-a', json.dumps(FakeChild.current.argv))
        self.assertEqual(FakeChild.current.argv[-1], '--viewer-stop-on-sigusr1')
        self.assertEqual(holder.read_viewer_holder_status(self.control)['failure_code'], 'holder_deadline_expired')

    def test_child_failure_without_output_preserves_failure_and_unavailable_resources(self):
        class MissingOutputChild:
            pid = 876544
            def __init__(self, argv, **kwargs):
                self.argv = argv; self.returncode = 7
                assert not Path(argv[argv.index('--output-dir') + 1]).exists()
            def poll(self): return self.returncode
            def wait(self, timeout=None): return self.returncode
        with mock.patch.object(holder.subprocess, 'run', return_value=SimpleNamespace(returncode=0)), \
                mock.patch.object(holder.subprocess, 'Popen', MissingOutputChild), \
                mock.patch.object(holder, 'GeneratorResourceSampler', FakeSampler):
            with self.assertRaisesRegex(holder.HolderError, 'child_exited_before_join'):
                holder.run_viewer_holder(self.binary, self.manifest, self.output, self.control,
                                         self.correlation, 5)
        self.assertEqual(holder.read_viewer_holder_status(self.control)['state'], 'FAILED')
        self.assertFalse(self.output.exists())
        proof = json.loads((self.control / 'resources-generator.json').read_text())
        self.assertFalse(proof['available']); self.assertEqual(proof['reason'], 'HolderError')


if __name__ == '__main__': unittest.main()
