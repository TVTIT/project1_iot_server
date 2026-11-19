"""A-side E5 integration: bounded import, phase lifecycle, collection and final report.

Only independent DB/REST/network children are mocked. The real B export schema
is produced locally with the same typed projection keys the client writes.
"""
import hashlib
import inspect
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import create_autospec, patch

_HERE = Path(__file__).resolve().parent
for _path in (str(_HERE), str(_HERE.parent)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import paper_p8_e5_import as e5_import
import paper_p8_e5_remote as remote
from scripts import paper_p8_e5 as e5_driver
from scripts.tests import native_compose_smoke as smoke
from scripts.tests.test_paper_p8_e5_remote import (
    FakeEndpoint,
    FakeRegistry,
    FakeSmoke,
    _sample_admin_metrics,
)

PHASES = ('initial', 'warm_replay', 'control')
SESSION_ID = 'e5-session-001'
PHASE_RUN_ID = 'e5-proj-e5'


def _write_private(path, value, *, raw=None):
    path = Path(path)
    data = raw if raw is not None else (
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode() + b'\n')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(data)
    return path


def _canonical(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class E5AIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.private = self.root / 'private'
        self.private.mkdir(mode=0o700)
        certs = self.private / 'certs'
        certs.mkdir(mode=0o700)
        (certs / 'ca.crt').write_text('FAKE CA CERT')
        (certs / 'ca.crt').chmod(0o600)
        self.artifacts = self.private / 'artifacts'
        self.artifacts.mkdir(mode=0o700)

        self.smoke = FakeSmoke(_HERE.parent.parent)
        self.fixture = SimpleNamespace(
            private=self.private, artifacts=self.artifacts,
            project='ncs_0123456789ab', paper_p8_e5='optimized')
        self.endpoint = FakeEndpoint(self.private)
        self.registry = FakeRegistry()
        self.containers = {'mosquitto': 'fake-mosquitto', 'nginx': 'fake-nginx'}
        self.images = {'backend': 'sha256:' + 'b' * 64, 'gateway': 'sha256:' + 'c' * 64}

        repeat_root = self.private / 'repeats'
        repeat_root.mkdir(mode=0o700)
        repeat_root.chmod(0o700)
        _, self.context_file = e5_driver.prepare_repeat(repeat_root, 1, 813)
        self.context = json.loads(self.context_file.read_text())
        self.dataset = Path(self.context['dataset_path'])
        self.msg_ids = [json.loads(line)['message_id']
                        for line in self.dataset.read_text().splitlines() if line.strip()]
        self.dataset_records = [json.loads(line)
                                for line in self.dataset.read_text().splitlines() if line.strip()]
        self.prepared = remote.prepare(
            self.smoke, self.fixture, self.endpoint, self.registry,
            self.containers, self.images, {}, arm='optimized',
            context_path=self.context_file)
        self.prepared['session_id'] = SESSION_ID
        self.prepared['phase_id'] = 'initial'
        self.prepared['run_id'] = PHASE_RUN_ID
        self.prepared['bundle_session'] = {
            'experiment': 'E5', 'arm': 'optimized', 'session_id': SESSION_ID,
            'run_ids': [PHASE_RUN_ID]}
        self.prepared['bundle_inventory'] = [{'path': 'datasets/dataset.jsonl',
                                              'sha256': self.context['sha256']}]
        self.prepared['bundle_sha256'] = 'a' * 64
        self.binding = self._binding()

    # -- fixture builders -------------------------------------------------
    def _binding(self):
        manifests = self.prepared['manifests']
        environments = {f'executor-{index + 1}': hashlib.sha256(Path(path).read_bytes()).hexdigest()
                        for index, path in enumerate(manifests['gateway_env'])}
        return {
            'schema_version': 1,
            'session': self.prepared['bundle_session'],
            'source_bundle_sha256': self.prepared['bundle_sha256'],
            'source_inventory': self.prepared['bundle_inventory'],
            'bound_manifest_sha256': {
                label: hashlib.sha256(Path(manifests[label]).read_bytes()).hexdigest()
                for label in ('initial', 'warm_replay', 'viewer', 'control')},
            'bound_gateway_environment_sha256': environments,
            'bound_run_spec_sha256': '1' * 64,
            'elapsed_transfer_wait_seconds': 0.0,
            'required_token_lifetime_seconds': 0.0,
            'e5': {'context_sha256': self.context['sha256'],
                   'phase_plan_sha256': '2' * 64,
                   'dataset_sha256': self.context['sha256']},
            'immutable_root': 'inputs/',
            'mutable_root': 'outputs/',
        }

    def _initial_headers(self):
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
            for r in self.dataset_records
        ]

    def _viewer_identity_samples(self):
        r_a = self.dataset_records[0]
        r_b = self.dataset_records[1]
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

    def _projection(self, phase):
        if phase in ('initial', 'warm_replay'):
            return {
                'status': 'COMPLETE', 'cleanup_errors': [],
                'e4': {'mode': 'replay', 'journal_entry_count': 20 if phase == 'initial' else 10,
                       'safe_header_samples': self._initial_headers() if phase == 'initial' else self._initial_headers()[:10]},
                'telemetry': {'broker_pubacks': 20 if phase == 'initial' else 10,
                              'pending_messages': 0},
                'holder_evidence': {},
            }
        return {
            'status': 'COMPLETE', 'cleanup_errors': [],
            'commands': [{'idempotency_key': key, 'classification': 'succeeded'}
                         for key in self.context['command_keys']],
            'executors': [], 'sqlite_executors': [],
            'joined_proof': {}, 'viewer': {}, 'lifecycle': {},
            'lifecycle_proof': None, 'readiness_projection': None,
            'b_declared': None,
        }

    def _runner(self, phase):
        return {
            'status': 'COMPLETE', 'actual_stop_confirmed': True,
            'timer_driven_gateway_workload': False,
            'started_at_unix': 1.0, 'finished_at_unix': 2.0,
            'durable_gateway_process_count': 0, 'durable_gateway_count': 0,
            'durable_sensor_connections': 0, 'durable_sensor_count': 0,
            'readiness': [{'child': 'gateway-1', 'ready': True}],
            'pending_outbox_readback': [
                {'path_hash': 'f' * 64, 'rows': None, 'status': 'readback',
                 'pending_rows': 0, 'pending_bytes': 0}],
            'phase_observations': {'status': 'observed', 'events': []},
            'resource_sampling': {'status': 'not_collected_in_P8.4'},
        }

    def _results(self, phase, projection=None):
        binding = self.binding
        run_id = json.loads(Path(self.prepared['manifests'][phase]).read_text())['run_id']
        projection = projection if projection is not None else self._projection(phase)
        summary = {
            'schema_version': 1, 'experiment': 'E5', 'arm': 'optimized',
            'phase': phase, 'session_id': SESSION_ID, 'run_id': run_id,
            'status': projection['status'],
            'source_bundle_sha256': binding['source_bundle_sha256'],
            'source_inventory': binding['source_inventory'],
            'bound_manifest_sha256': binding['bound_manifest_sha256'],
            'bound_gateway_environment_sha256': binding['bound_gateway_environment_sha256'],
            'bound_run_spec_sha256': binding['bound_run_spec_sha256'],
            'bound_e5_context_sha256': binding['e5']['context_sha256'],
            'bound_e5_phase_plan_sha256': binding['e5']['phase_plan_sha256'],
            'bound_dataset_sha256': binding['e5']['dataset_sha256'],
            'binding': binding, 'binding_sha256': _canonical(binding),
            'runner': self._runner(phase),
            'resource_samples': [],
            'projection': projection,
            'full_p8_accepted': False, 'physical_ab': 'NOT RUN',
            **projection,
        }
        return summary

    def _copy_b_results(self, phase, summary=None, raw=None):
        directory = self.private / 'incoming' / phase
        if not directory.exists():
            (self.private / 'incoming').mkdir(mode=0o700, exist_ok=True)
            directory.mkdir(mode=0o700)
        directory.chmod(0o700)
        summary = summary if summary is not None else self._results(phase)
        path = _write_private(directory / 'results.json', summary, raw=raw)
        return directory, path

    # -- import ----------------------------------------------------------
    def test_import_writes_pinned_receipt_and_rejects_content_changes(self):
        for phase in PHASES:
            directory, path = self._copy_b_results(phase)
            loaded = e5_import.import_phase_result(self.prepared, self.private, phase=phase)
            self.assertEqual(loaded['phase'], phase)
            self.assertEqual(loaded['run_id'], json.loads(
                Path(self.prepared['manifests'][phase]).read_text())['run_id'])
            self.assertEqual(loaded['binding_sha256'], _canonical(self.binding))
            self.assertEqual(loaded['provenance'], 'a_owned_import_of_operator_copied_b_export')
            receipt = e5_import.read_receipt(self.private, phase)
            self.assertEqual(receipt['pinned'], loaded['pinned'])
            with self.assertRaisesRegex(ValueError, 'e5_import_receipt_exists'):
                e5_import.import_phase_result(self.prepared, self.private, phase=phase)
            # no private-file mutation is needed between phases
            self.assertTrue(path.is_file())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_import_rejects_wrong_identity_source_and_digest(self):
        cases = {
            'e5_import_session_mismatch': {'session_id': 'e5-other'},
            'e5_import_phase_mismatch': {'phase': 'warm_replay'},
            'e5_import_run_id_mismatch': {'run_id': 'e5-other'},
            'e5_import_source_bundle_mismatch': {'source_bundle_sha256': 'b' * 64},
            'e5_import_binding_digest_mismatch': {'binding_sha256': 'c' * 64},
            'e5_import_acceptance_flags_rejected': {'full_p8_accepted': True},
        }
        for index, (code, update) in enumerate(cases.items()):
            with self.subTest(code=code):
                sub = self.private / f'case-{index}'
                sub.mkdir(mode=0o700)
                summary = self._results('initial')
                summary.update(update)
                incoming = sub / 'incoming'
                incoming.mkdir(mode=0o700)
                incoming.chmod(0o700)
                directory = incoming / 'initial'
                directory.mkdir(mode=0o700)
                directory.chmod(0o700)
                sub.chmod(0o700)
                _write_private(directory / 'results.json', summary)
                with self.assertRaisesRegex(ValueError, code):
                    e5_import.import_phase_result(self.prepared, sub, phase='initial')

    def test_import_rejects_unknown_files_symlinks_and_run_id_source_drift(self):
        # Unknown extra file in the copied phase directory.
        directory, _ = self._copy_b_results('initial')
        _write_private(directory / 'extra.txt', None, raw=b'x')
        with self.assertRaisesRegex(ValueError, 'e5_import_incoming_files_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')
        (directory / 'extra.txt').unlink()
        directory.chmod(0o700)

        # Results path may not be a symlink, a non-private file or oversized.
        target = self.private / 'real-results.json'
        _write_private(target, self._results('initial'))
        (directory / 'results.json').unlink()
        (directory / 'results.json').symlink_to(target.name)
        with self.assertRaisesRegex(ValueError, 'e5_import_results_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')
        (directory / 'results.json').unlink()
        directory.chmod(0o700)

        raw = json.dumps(self._results('initial'), sort_keys=True).encode()
        _write_private(directory / 'results.json', None, raw=b'{"a":' + b'1' * (8 << 20))
        with self.assertRaisesRegex(ValueError, 'e5_import_results_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')
        (directory / 'results.json').unlink()
        directory.chmod(0o700)

        # Duplicate keys and non-finite numbers are rejected by strict parsing.
        for label, payload in (('duplicate', b'{"schema_version":1,"schema_version":1}'),
                               ('nonfinite', b'{"schema_version":1,"x":NaN}')):
            with self.subTest(label=label):
                _write_private(directory / 'results.json', None, raw=payload)
                with self.assertRaisesRegex(ValueError, 'e5_import_results_rejected'):
                    e5_import.import_phase_result(self.prepared, self.private, phase='initial')
                (directory / 'results.json').unlink()
                directory.chmod(0o700)

        # A later phase that re-declares the pinned binding is refused.
        _, _ = self._copy_b_results('initial')
        e5_import.import_phase_result(self.prepared, self.private, phase='initial')
        changed = self._results('warm_replay')
        changed['binding'] = dict(self.binding, required_token_lifetime_seconds=30.0)
        changed['binding_sha256'] = _canonical(changed['binding'])
        self._copy_b_results('warm_replay', changed)
        with self.assertRaisesRegex(ValueError, 'e5_import_pin_mismatch_warm_replay'):
            e5_import.import_phase_result(self.prepared, self.private, phase='warm_replay')

    def test_import_rejects_unconfirmed_runner_and_pending_control(self):
        summary = self._results('control')
        summary['runner']['actual_stop_confirmed'] = False
        self._copy_b_results('control', summary)
        with self.assertRaisesRegex(ValueError, 'e5_import_runner_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='control')

        (self.private / 'incoming' / 'control' / 'results.json').unlink()
        summary = self._results('control')
        summary['runner']['pending_outbox_readback'][0]['pending_rows'] = 3
        self._copy_b_results('control', summary)
        with self.assertRaisesRegex(ValueError, 'e5_import_runner_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='control')

    def test_import_validates_runner_timer_driven_gateway_workload_field(self):
        # Accepts boolean True or False
        for val in (True, False):
            summary = self._results('initial')
            summary['runner']['timer_driven_gateway_workload'] = val
            self._copy_b_results('initial', summary)
            res = e5_import.import_phase_result(self.prepared, self.private, phase='initial')
            self.assertEqual(res['projection']['status'], 'COMPLETE')
            (self.private / 'incoming' / 'initial' / 'results.json').unlink()
            (self.private / 'e5-imports' / 'initial.json').unlink()

        # Rejects non-boolean types: strings, 0/1, null, list
        for bad_val in ('true', 'false', 1, 0, None, [True]):
            summary = self._results('initial')
            summary['runner']['timer_driven_gateway_workload'] = bad_val
            self._copy_b_results('initial', summary)
            with self.assertRaisesRegex(ValueError, 'e5_import_runner_rejected'):
                e5_import.import_phase_result(self.prepared, self.private, phase='initial')
            (self.private / 'incoming' / 'initial' / 'results.json').unlink()

        # Rejects arbitrary unknown field in runner
        summary = self._results('initial')
        summary['runner']['arbitrary_unknown_key'] = 'attack'
        self._copy_b_results('initial', summary)
        with self.assertRaisesRegex(ValueError, 'e5_import_runner_rejected'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')
        (self.private / 'incoming' / 'initial' / 'results.json').unlink()

    def test_import_rejects_non_complete_projection_and_cleanup_errors(self):
        summary = self._results('initial')
        summary['status'] = summary['projection']['status'] = 'PARTIAL'
        self._copy_b_results('initial', summary)
        with self.assertRaisesRegex(ValueError, 'e5_workload_incomplete_initial'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')

        (self.private / 'incoming' / 'initial' / 'results.json').unlink()
        summary = self._results('initial')
        summary['cleanup_errors'] = summary['projection']['cleanup_errors'] = ['stray']
        self._copy_b_results('initial', summary)
        with self.assertRaisesRegex(ValueError, 'e5_import_cleanup_errors_initial'):
            e5_import.import_phase_result(self.prepared, self.private, phase='initial')

    def test_read_context_seed_bounds_private_context(self):
        self.assertEqual(e5_import.read_context_seed(self.context_file), 813)
        malformed = self.private / 'malformed.json'
        _write_private(malformed, {'seed': 'not-an-int'})
        with self.assertRaisesRegex(ValueError, 'server_context_seed_rejected'):
            e5_import.read_context_seed(malformed)
        link = self.private / 'linked.json'
        link.symlink_to(self.context_file.name)
        with self.assertRaisesRegex(ValueError, 'server_context_rejected'):
            e5_import.read_context_seed(link)

    # -- phase lifecycle -------------------------------------------------
    def test_begin_phase_collects_a_side_metrics(self):
        snapshot = smoke.paper_p8_e5_begin_phase(
            self.smoke, self.fixture, self.endpoint, self.registry, self.containers,
            self.prepared, 'initial')
        self.assertEqual(snapshot['phase'], 'initial')
        self.assertIn('initial', self.prepared['phase_snapshots'])
        self.assertIn('initial', self.prepared['remote_collectors'])
        with self.assertRaisesRegex(ValueError, 'initial_required_before_warm_replay'):
            smoke.paper_p8_e5_begin_phase(
                self.smoke, self.fixture, self.endpoint, self.registry, self.containers,
                self.prepared, 'warm_replay')

    def test_finish_phase_imports_and_writes_bounded_artifacts(self):
        directory, results_path = self._copy_b_results('initial')
        with patch.object(remote, 'finish_remote_initial',
                          return_value={'phase': 'initial', 'db_after': [],
                                        'ws_window': {}, 'collector_metrics': [],
                                        'resource_samples': [],
                                        'collector_generator_samples': [],
                                        'collector_errors': []}):
            outcome = smoke.paper_p8_e5_finish_phase(
                self.smoke, self.fixture, self.endpoint, self.registry, self.containers,
                self.prepared, str(results_path), self.artifacts, 'initial')
        self.assertEqual(outcome['phase_id'], 'initial')
        self.assertEqual(outcome['workload_outcome'], 'VALIDATED')
        self.assertTrue(outcome['validation_valid'])
        receipt = e5_import.read_receipt(self.private, 'initial')
        self.assertEqual(receipt['results_sha256'], outcome['results_sha256'])
        artifact = self.artifacts / 'p8-e5-optimized-initial-phase.json'
        self.assertTrue(artifact.is_file())
        data = json.loads(artifact.read_text())
        self.assertEqual(data['phase'], 'initial')
        self.assertEqual(data['experiment'], 'E5')
        self.assertFalse(data['full_p8_accepted'])
        self.assertEqual(data['physical_ab'], 'NOT RUN')
        self.assertTrue((self.artifacts / 'p8-e5-optimized-initial-server-metrics.csv').is_file())
        self.assertTrue((self.artifacts / 'p8-e5-optimized-initial-resources.csv').is_file())

        # A results path outside the copied incoming phase is never opened.
        outside = self.artifacts / 'results.json'
        _write_private(outside, self._results('initial'))
        with self.assertRaisesRegex(ValueError, 'e5_result_path_mismatch'):
            smoke.paper_p8_e5_finish_phase(
                self.smoke, self.fixture, self.endpoint, self.registry, self.containers,
                self.prepared, str(outside), self.artifacts, 'initial')

    # -- final report ----------------------------------------------------
    def test_export_bundle_stages_owner_only_ca_sources(self):
        import scripts.paper_p8_bundle as bundle
        root = self.root / 'e5-prep'
        root.mkdir(mode=0o700)
        certs = root / 'certs'
        certs.mkdir(mode=0o700)
        https_ca = certs / 'https-ca.pem'
        mqtt_ca = certs / 'mqtt-ca.pem'
        for path in (https_ca, mqtt_ca):
            path.write_bytes(b'-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n')
            path.chmod(0o644)
        identities = root / 'identities'
        identities.mkdir(mode=0o700)
        viewer_identity = identities / 'viewer.json'
        telemetry_identity = identities / 'telemetry.json'
        credential = identities / 'gateway-a.json'
        _write_private(viewer_identity, {'user-a': 'a.b.c'})
        _write_private(telemetry_identity, {'p8-e5-a': 'd.e.f'})
        _write_private(credential, {'username': 'p8-e5-a', 'password': 'secret'})
        manifests = {}
        for label, role, identity in (
                ('initial', 'telemetry', telemetry_identity),
                ('warm_replay', 'telemetry', telemetry_identity),
                ('viewer', 'viewer', viewer_identity),
                ('control', 'control', viewer_identity)):
            manifest = root / f'{label}.manifest.json'
            _write_private(manifest, {
                'roles': [role],
                'identity_files': {role: str(identity)},
                'gateway_identity_files': {'p8-e5-a': str(credential)},
            })
            manifests[label] = str(manifest)
        env = identities / 'executor-1.json'
        _write_private(env, {'GATEWAY_ID': 'p8-e5-a'})
        manifests['gateway_env'] = [str(env)]
        manifests['run_spec'] = str(root / 'run-spec.json')
        prepared = {
            'root': str(root), 'arm': 'optimized', 'manifests': manifests,
            'https_ca_file': str(https_ca), 'mqtt_ca_file': str(mqtt_ca),
            'context': {'dataset_path': str(root / 'dataset.jsonl')}, 'workload': {},
        }
        captured = {}

        def fake_export(**kwargs):
            captured.update(kwargs)
            return {'file_count': 0, 'total_bytes': 0}

        with patch.object(bundle, 'export_bundle', side_effect=fake_export):
            result = remote.export_prepared_bundle(
                prepared, output_leaf=str(root / 'bundle'), session_id='0' * 32)
        self.assertEqual(result['file_count'], 0)
        staged = captured['ca_files']
        self.assertEqual(set(staged), {'https', 'mqtt'})
        staged_root = root / 'portable-ca-inputs'
        self.assertEqual(stat.S_IMODE(staged_root.stat().st_mode), 0o700)
        for name, source in staged.items():
            self.assertEqual(source.parent, staged_root)
            info = source.stat()
            self.assertTrue(stat.S_ISREG(info.st_mode))
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
            source_bytes = bundle._source(source)
            self.assertEqual(source_bytes,
                             (https_ca if name == 'https' else mqtt_ca).read_bytes())
            self.assertNotEqual(source, https_ca if name == 'https' else mqtt_ca)

    def test_final_report_partial_run_is_incomplete_never_not_run(self):
        final_state = {'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN',
                       'phase_history': ['initial'], 'cleanup_outcome': 'VERIFIED'}
        report = smoke.paper_p8_e5_finalize_report(
            self.smoke, self.fixture, self.prepared, final_state, cleanup_verified=True)
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(report['workload_status'], 'INCOMPLETE')
        self.assertEqual(report['reason'], 'e5_incomplete_phase_history')
        self.assertFalse(report['full_p8_accepted'])
        self.assertEqual(report['physical_ab'], 'NOT RUN')
        artifact = json.loads(
            (self.artifacts / 'p8-e5-optimized.json').read_text())
        self.assertEqual(artifact['status'], 'FAIL')

    def _accepted_phase_results(self):
        """Build the phase evidence a validated, fully-run E5 session leaves."""
        for phase in PHASES:
            self._copy_b_results(phase)
            e5_import.import_phase_result(self.prepared, self.private, phase=phase)
        # The trusted importer only advances history after a validated phase, so
        # the finalize fixture must carry the same completed-phase and raw-proof
        # evidence a real run produces.
        self.prepared['completed_phases'] = set(PHASES)
        initial_db = [{'fingerprint': 'fp-0'}]
        initial = {
            'admin_before': _sample_admin_metrics(), 'admin_after': _sample_admin_metrics(writes=20, events=20),
            'db_after': initial_db,
            'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            'raw_proof': [{'verified_exact': True}, {'verified_exact': True}],
            'ws_window': {'new_events': 20, 'telemetry_writes_completed': 20,
                          'deliveries_enqueued': 20, 'drops': 0},
            'ingress_delta': {}, 'b_projection': self._projection('initial'),
        }
        warm = {
            'admin_before': _sample_admin_metrics(writes=20, events=20),
            'admin_after': _sample_admin_metrics(writes=20, events=20),
            'db_after': initial_db,
            'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
            'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0,
                          'deliveries_enqueued': 0, 'drops': 0},
            'ingress_delta': {}, 'b_projection': self._projection('warm_replay'),
        }
        control = {
            'admin_before': _sample_admin_metrics(writes=20, events=20),
            'admin_after': _sample_admin_metrics(writes=20, events=20),
            'db_after': initial_db,
            'pg_receipts': {key: {'status': 'succeeded', 'result_hash': 'abc'}
                            for key in self.context['command_keys']},
            'rest_configs': {'p8-e5-command-a': {}, 'p8-e5-command-b': {}},
            'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4,
                                       'verified_configurations': 2},
            'ws_window': {'new_events': 0, 'telemetry_writes_completed': 0,
                          'deliveries_enqueued': 0, 'drops': 0},
            'b_projection': self._projection('control'),
        }
        phase_results = {'initial': initial, 'warm_replay': warm, 'control': control}
        self.prepared['phase_results'] = phase_results

        source = remote.e4._support._source_fingerprint(Path(self.smoke.ROOT))
        binding_sha = _canonical(self.binding)
        manifests = self.prepared['manifests']
        viewer_manifest_sha = hashlib.sha256(Path(manifests['viewer']).read_bytes()).hexdigest()
        viewer_run_id = json.loads(Path(manifests['viewer']).read_text())['run_id']
        correlation = {
            'session_id': SESSION_ID, 'run_id': viewer_run_id,
            'binding_digest': binding_sha, 'manifest_sha256': viewer_manifest_sha,
            'binary_sha256': 'd' * 64,
        }
        viewer_projection = {
            'status': 'COMPLETE', 'planned_stop': True,
            'planned_stop_reason': 'stopped_by_operator', 'connection_attempts': 2,
            'connected_distinct_viewers': 2, 'subscribed_distinct_viewers': 2,
            'receiving_distinct_viewers': 2, 'unique_events': 20, 'duplicates': 0,
            'wrong_namespace': 0, 'reconnections': 0, 'primary_error': None,
            'cleanup_errors': [],
            'viewers': [{'id': row['viewer_id'], 'unique_events': 10}
                        for row in self.context['viewer_subscriptions']],
            'identity_samples': self._viewer_identity_samples(),
        }
        lifecycle = {
            'schema_version': 1, 'clock': 'viewer_holder_local_elapsed_seconds',
            'correlation': correlation, 'ready_elapsed_seconds': 0.5,
            'checkpoints': [
                {'phase': phase, 'boundary': boundary, 'elapsed_seconds': value,
                 'child_alive': True, 'ready_verified': True}
                for value, (phase, boundary) in zip(
                    (1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
                    (('initial', 'begin'), ('initial', 'end'),
                     ('warm_replay', 'begin'), ('warm_replay', 'end'),
                     ('control', 'begin'), ('control', 'end')))],
            'stop_elapsed_seconds': 7.0, 'joined': True,
        }
        control['b_projection'].update({
            'viewer': viewer_projection, 'lifecycle': lifecycle,
            'lifecycle_proof': lifecycle,
            'readiness_projection': {'ready_before_load': True,
                                     'connected_distinct_viewers': 2,
                                     'subscribed_distinct_viewers': 2},
            'b_declared': {
                'paperload_binary_sha256': 'd' * 64,
                'gateway_binary_sha256': 'e' * 64,
                'combined_binary_sha256': 'f' * 64,
                'source_fingerprint': {key: source.get(key) for key in
                                       ('revision', 'branch', 'dirty', 'dirty_source_sha256')},
                'b_os': 'linux', 'b_arch': 'amd64',
                'declared_measurement_windows': {phase: 10.0 for phase in PHASES},
                'observed_measurement_windows': {phase: 9.5 for phase in PHASES},
                'idle_transfer_windows': [
                    {'between': 'initial/warm_replay',
                     'clock': 'viewer_holder_local_elapsed_seconds',
                     'start_elapsed_seconds': 2.0, 'end_elapsed_seconds': 3.0,
                     'duration_seconds': 1.0, 'excluded': True},
                    {'between': 'warm_replay/control',
                     'clock': 'viewer_holder_local_elapsed_seconds',
                     'start_elapsed_seconds': 4.0, 'end_elapsed_seconds': 5.0,
                     'duration_seconds': 1.0, 'excluded': True}],
                'actual_stop_confirmed': True, 'runner_stopped': True,
                'processes_stopped': True,
            },
        })
        return phase_results

    def _accepted_final_state(self):
        return {'phase': 'CLOSED', 'workload_outcome': 'VALIDATED',
                'phase_history': list(PHASES), 'cleanup_outcome': 'VERIFIED'}

    def _finalize_accepted(self, *, cleanup_verified=True):
        with patch.object(remote.e4, 'verify_pilot_raw',
                          return_value={'verified_exact': True}):
            return smoke.paper_p8_e5_finalize_report(
                self.smoke, self.fixture, self.prepared,
                self._accepted_final_state(), cleanup_verified=cleanup_verified)

    def test_final_report_builds_accepted_report_with_viewer_and_b_declared(self):
        self._accepted_phase_results()
        report = self._finalize_accepted()
        self.assertEqual(report['status'], 'PASS', report.get('validation'))
        self.assertEqual(report['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
        self.assertFalse(report['full_p8_accepted'])
        self.assertEqual(report['physical_ab'], 'NOT RUN')
        self.assertEqual(report['phase_history'], list(PHASES))
        self.assertEqual(report['executed_phases'], list(PHASES))
        self.assertEqual(report['metadata']['b_declared']['provenance'],
                         'operator_declared_b_measured_facts')
        self.assertIs(report['metadata']['b_declared']['remote_attestation'], False)
        self.assertTrue(report['metadata']['resource_cohort'])
        self.assertEqual(set(report['collector_evidence']), set(PHASES))
        self.assertTrue(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True))
        artifact = json.loads((self.artifacts / 'p8-e5-optimized.json').read_text())
        self.assertEqual(artifact['status'], 'PASS')
        self.assertEqual(artifact['cleanup_verified'], True)

    def test_final_report_keeps_cleanup_pending_and_never_accepts(self):
        self._accepted_phase_results()
        report = self._finalize_accepted(cleanup_verified=False)
        self.assertEqual(report['status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
        self.assertIs(report['cleanup_verified'], False)
        self.assertFalse(report['full_p8_accepted'])
        self.assertFalse(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, False))
        artifact = json.loads((self.artifacts / 'p8-e5-optimized.json').read_text())
        self.assertEqual(artifact['status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')

    def test_final_report_rejects_changed_main_database_fingerprint(self):
        phase_results = self._accepted_phase_results()
        phase_results['control']['db_after'] = [{'fingerprint': 'fp-1'}]
        report = self._finalize_accepted()
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(report['workload_status'], 'FAIL')
        self.assertIn('control_changed_main_fingerprint',
                      report['validation']['failures'])
        # A changed fingerprint invalidates raw proof and must block acceptance.
        self.assertFalse(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True))

    def test_final_report_rejects_changed_warm_replay_fingerprint(self):
        phase_results = self._accepted_phase_results()
        phase_results['warm_replay']['db_after'] = [{'fingerprint': 'fp-1'}]
        report = self._finalize_accepted()
        self.assertEqual(report['status'], 'FAIL')
        self.assertIn('warm_replay_changed_main_fingerprint',
                      report['validation']['failures'])

    def test_post_run_preparation_only_reports_not_run(self):
        ok, outcome = smoke.paper_p8_e5_post_run_status(
            {'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN'},
            {'status': 'PREPARATION_ONLY'})
        self.assertTrue(ok)
        self.assertEqual(outcome, 'NOT_RUN')
        # A collector that still claims readiness cannot pass the preparation lane.
        ok, _ = smoke.paper_p8_e5_post_run_status(
            {'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN', 'collector_ready': True},
            {'status': 'PREPARATION_ONLY'})
        self.assertFalse(ok)

    def test_post_run_validated_session_passes_only_with_cleanup(self):
        self._accepted_phase_results()
        evidence = self._finalize_accepted()
        session = {'phase': 'CLOSED', 'workload_outcome': 'VALIDATED',
                   'cleanup_outcome': 'VERIFIED', 'phase_history': list(PHASES)}
        ok, outcome = smoke.paper_p8_e5_post_run_status(session, evidence)
        self.assertTrue(ok)
        self.assertEqual(outcome, 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
        # Missing registry cleanup flips the same evidence to a failing gate.
        ok, _ = smoke.paper_p8_e5_post_run_status(
            dict(session, cleanup_outcome='PENDING'), evidence)
        self.assertFalse(ok)

    def test_post_run_partial_session_stays_incomplete_never_not_run(self):
        final_state = {'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN',
                       'phase_history': ['initial'], 'cleanup_outcome': 'VERIFIED'}
        evidence = smoke.paper_p8_e5_finalize_report(
            self.smoke, self.fixture, self.prepared, final_state, cleanup_verified=True)
        self.assertEqual(evidence['status'], 'FAIL')
        ok, outcome = smoke.paper_p8_e5_post_run_status(final_state, evidence)
        self.assertFalse(ok)
        self.assertEqual(outcome, 'INCOMPLETE')

    def test_post_run_status_rejects_non_mapping_evidence(self):
        for session, evidence in (({}, []), (None, {}), ({}, None)):
            with self.subTest(session=session, evidence=evidence):
                with self.assertRaisesRegex(ValueError, 'e5_post_run_evidence_rejected'):
                    smoke.paper_p8_e5_post_run_status(session, evidence)

    def test_hold_callbacks_required_includes_every_prepare_hold_lane(self):
        names = ('p8_e1_prepare_hold', 'p8_e2_prepare_hold', 'p8_e3_prepare_hold',
                 'p8_e4_prepare_hold', 'p8_e5_prepare_hold')
        self.assertFalse(smoke.paper_p8_hold_callbacks_required(SimpleNamespace()))
        for name in names:
            with self.subTest(flag=name):
                self.assertTrue(smoke.paper_p8_hold_callbacks_required(
                    SimpleNamespace(**{name: True})))

    def test_native_callback_signatures_match_hold_contract(self):
        # ``native_compose_smoke.main`` installs these helpers into the hold loop,
        # so their real signatures are the contract between the CLI and the
        # bounded importer/collector. Autospec pins arity and keyword-only names.
        contracts = {
            'paper_p8_e5_begin_phase': (
                ['smoke', 'fixture', 'endpoint', 'registry', 'containers',
                 'prepared', 'phase'], []),
            'paper_p8_e5_finish_phase': (
                ['smoke', 'fixture', 'endpoint', 'registry', 'containers',
                 'prepared', 'results_path', 'artifacts', 'phase'], []),
            'paper_p8_e5_finalize_report': (
                ['smoke', 'fixture', 'prepared', 'final_state'], ['cleanup_verified']),
        }
        for name, (positional, keyword_only) in contracts.items():
            with self.subTest(callback=name):
                target = getattr(smoke, name)
                signature = inspect.signature(target)
                self.assertEqual(
                    [param.name for param in signature.parameters.values()
                     if param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD],
                    positional)
                self.assertEqual(
                    [param.name for param in signature.parameters.values()
                     if param.kind is inspect.Parameter.KEYWORD_ONLY],
                    keyword_only)
                mock = create_autospec(target)
                mock(*[object() for _ in positional],
                     **{flag: True for flag in keyword_only})
                mock.assert_called_once()
                with self.assertRaises(TypeError):
                    mock(*[object() for _ in positional + ['extra']])


if __name__ == '__main__':
    unittest.main()
