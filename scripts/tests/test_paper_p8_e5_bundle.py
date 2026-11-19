"""Portable E5 bundle round-trip built from the production repeat/input builders."""
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'scripts' / 'tests'))

import paper_p8_bundle as bundle
import paper_p8_e5_fixture as e5_fixture
import paper_p8_e5_support as e5
from scripts import paper_p8_e5 as e5_driver


def _jwt(exp):
    enc = lambda obj: base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip('=')
    return enc({'alg': 'HS256'}) + '.' + enc({'exp': exp}) + '.signature'


class FakeSmoke:
    def http_request(self, endpoint, method, path, token=None, body=None, **kwargs):
        if method == 'PUT':
            gateway, sensor = path.split('/')[3], path.split('/')[-1]
            return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:{gateway}:{sensor}'}
        if method == 'GET':
            return 200, {}, {'desired_version': '0'}
        return 404, {}, {}


class PortableE5BundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='p8-e5-bundle-', dir='/var/tmp/opencode')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.source = self.base / 'A'
        self.source.mkdir(mode=0o700)
        self.source.chmod(0o700)
        self.private = self.source / 'private'
        self.private.mkdir(mode=0o700)
        self.private.chmod(0o700)
        self.repeat_root = self.private / 'repeats'
        self.repeat_root.mkdir(mode=0o700)
        self.repeat_root.chmod(0o700)
        self.repeat_dir, self.context_path = e5_driver.prepare_repeat(self.repeat_root, 1, 813)
        self.context = json.loads(self.context_path.read_text())
        self.exp = int(time.time()) + 3600
        self.ca_dir = self.private / 'certs'
        self.ca_dir.mkdir(mode=0o700)
        self.https_ca = self.ca_dir / 'https-ca.pem'
        self.mqtt_ca = self.ca_dir / 'mqtt-ca.pem'
        self.https_ca.write_text('public https CA fixture\n')
        self.mqtt_ca.write_text('public mqtt CA fixture\n')
        self.https_ca.chmod(0o600)
        self.mqtt_ca.chmod(0o600)
        self.fixture = SimpleNamespace(private=self.private, project='portable-e5')
        self.endpoint = SimpleNamespace(url='https://fixture-a.lan:14443', ca_file=self.https_ca)
        self.specs = [
            SimpleNamespace(gateway_id='p8-e5-a', sensor_id=self.context['sensor_ids'][0], sensor_unit='degC'),
            SimpleNamespace(gateway_id='p8-e5-b', sensor_id=self.context['sensor_ids'][1], sensor_unit='m/s'),
        ]
        self.passwords = ['gateway-password-a', 'gateway-password-b']
        self.tokens = {'a': _jwt(self.exp), 'b': _jwt(self.exp), 'admin': _jwt(self.exp)}
        self.input_root = self.private / 'prepared'
        self.input_root.mkdir(mode=0o700)
        self.input_root.chmod(0o700)
        self.prepared = e5_fixture.prepare_inputs(
            smoke=FakeSmoke(), fixture=self.fixture, endpoint=self.endpoint,
            specs=self.specs, passwords=self.passwords, tokens=self.tokens,
            mqtt_broker_url='ssl://fixture-a.lan:18884',
            mqtt_manifest_endpoint='mqtts://fixture-a.lan:18884',
            mqtt_ca_file=self.mqtt_ca, journal_path=self.context['dataset_path'],
            journal_context={k: self.context[k] for k in ('sha256','bytes','gateway_ids','sensor_ids','seed','anchor')},
            seed=self.context['seed'], anchor=self.context['anchor'], root=self.input_root,
            command_keys=self.context['command_keys'],
            viewer_subscriptions=self.context['viewer_subscriptions'],
        )

    def _export_args(self, output_leaf):
        manifests = {name: Path(path) for name, path in self.prepared['manifests'].items()
                     if name in ('initial','warm_replay','viewer','control')}
        identities = {}
        role_metadata = {'roles': {}, 'gateways': {}}
        for label, path in manifests.items():
            manifest = json.loads(path.read_text())
            role = manifest['roles'][0]
            source = Path(next(iter(manifest['identity_files'].values())))
            identity_key = f'{label}:{role}'
            identities[identity_key] = source
            principals = list(json.loads(source.read_text()))
            role_metadata['roles'][identity_key] = {'role': role, 'principals': principals}

        gateway_files = {}
        for manifest_path in manifests.values():
            manifest = json.loads(manifest_path.read_text())
            gateway_files.update({gid: Path(path) for gid, path in manifest.get('gateway_identity_files', {}).items()})
        for gid, path in gateway_files.items():
            username = json.loads(path.read_text())['username']
            role_metadata['gateways'][gid] = {'role': 'gateway', 'gateway_id': gid, 'username': username}
        env_files = {f'executor-{i+1}': Path(path)
                     for i, path in enumerate(self.prepared['manifests']['gateway_env'])}
        return {
            'output_leaf': output_leaf,
            'session_id': 'e5-session-813', 'experiment': 'E5', 'arm': 'optimized', 'phase': 'repeat-01',
            'manifests': manifests, 'identity_files': identities,
            'gateway_identity_files': gateway_files, 'gateway_env_files': env_files,
            'ca_files': {'https': self.https_ca, 'mqtt': self.mqtt_ca},
            'role_metadata': role_metadata, 'wait_seconds': 10, 'workload_seconds': 60,
            'topology': {'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 2, 'viewers': 2},
            'workload': self.prepared['workload'],
            'run_spec': Path(self.prepared['manifests']['run_spec']),
            'datasets': {'shared_journal': Path(self.context['dataset_path'])},
            'e5_context': self.context,
            'e5_lifecycle': {'mode': 'continuous', 'viewer_count': 2,
                'phases': ['initial','warm_replay','control'], 'deadline_seconds': 900,
                'idle_transfer_excluded': True},
        }

    def test_representative_selection_reaches_manifests_and_portable_bundle(self):
        repeat_dir, context_path = e5_driver.prepare_repeat(
            self.repeat_root, 2, 814, 'representative')
        context = json.loads(context_path.read_text())
        alternate_root = self.private / 'representative-prepared'
        alternate_root.mkdir(mode=0o700)
        self.prepared = e5_fixture.prepare_inputs(
            smoke=FakeSmoke(), fixture=self.fixture, endpoint=self.endpoint,
            specs=self.specs, passwords=self.passwords, tokens=self.tokens,
            mqtt_broker_url='ssl://fixture-a.lan:18884',
            mqtt_manifest_endpoint='mqtts://fixture-a.lan:18884', mqtt_ca_file=self.mqtt_ca,
            journal_path=context['dataset_path'],
            journal_context={k: context[k] for k in ('sha256','bytes','gateway_ids','sensor_ids',
                'seed','anchor','workload_selection_sha256')},
            seed=context['seed'], anchor=context['anchor'], root=alternate_root,
            command_keys=context['command_keys'], viewer_subscriptions=context['viewer_subscriptions'],
            workload_selection=context['workload_selection'])
        self.context = context
        initial = json.loads(Path(self.prepared['manifests']['initial']).read_text())
        warm = json.loads(Path(self.prepared['manifests']['warm_replay']).read_text())
        self.assertEqual(initial['profile'], 'pilot')
        self.assertEqual(warm['profile'], 'pilot')
        control = json.loads(Path(self.prepared['manifests']['control']).read_text())
        self.assertEqual(control['profile'], 'pilot')
        self.assertEqual(initial['e4']['replay_count'], 8)
        self.assertEqual(warm['e4']['replay_count'], 2)
        self.assertEqual(self.prepared['expected']['main_messages'], 8)
        result = bundle.export_bundle(**self._export_args(self.base / 'A' / 'representative-bundle'))
        root = Path(result['bundle_root'])
        self.assertTrue(bundle.inspect_bundle(root)['valid'])
        metadata = json.loads((root / 'bundle.json').read_text())
        self.assertEqual(metadata['session']['profile'], 'representative')
        self.assertEqual(metadata['workload']['telemetry']['messages'], 8)
        self.assertEqual(metadata['workload']['schedule']['warm_replay_prefix'], 2)

    def test_export_relocate_inspect_bind_preserves_immutable_inputs_and_role_contracts(self):
        source_journal = Path(self.context['dataset_path']).read_bytes()
        result = bundle.export_bundle(**self._export_args(self.base / 'A' / 'bundle'))
        bundle_root = Path(result['bundle_root'])
        copied = self.base / 'B' / 'elsewhere' / 'portable'
        copied.parent.mkdir(parents=True)
        shutil.copytree(bundle_root, copied)
        self.assertTrue(bundle.inspect_bundle(copied)['valid'])
        meta = json.loads((copied / 'bundle.json').read_text())
        self.assertEqual(meta['session']['experiment'], 'E5')
        self.assertEqual(meta['e5_dataset_ref'], 'datasets/dataset.jsonl')
        self.assertNotIn(str(self.source), (copied / 'bundle.json').read_text())
        self.assertNotIn(self.tokens['admin'], (copied / 'bundle.json').read_text())
        self.assertNotIn(self.passwords[0], (copied / 'bundle.json').read_text())
        self.assertNotIn(self.passwords[1], (copied / 'bundle.json').read_text())
        for env_path in (copied / 'gateway-env').glob('*.json'):
            self.assertNotIn('GATEWAY_MQTT_PASSWORD', json.loads(env_path.read_text()))
        self.assertEqual((copied / 'datasets/dataset.jsonl').read_bytes(), source_journal)

        for label, expected_exp, expected_role in (
                ('initial','E4','telemetry'), ('warm_replay','E4','telemetry'),
                ('viewer','E1','viewer'), ('control','E3','control')):
            manifest = json.loads((copied / f'manifests/{label}.json').read_text())
            self.assertEqual(manifest['experiment'], expected_exp)
            self.assertEqual(manifest['roles'], [expected_role])
        viewer_manifest = json.loads((copied / 'manifests/viewer.json').read_text())
        self.assertEqual(viewer_manifest['phases']['measure_seconds'], 900)
        self.assertGreaterEqual(viewer_manifest['limits']['wall_seconds'], 900)
        self.assertNotEqual(viewer_manifest['phases']['measure_seconds'], 40)
        for label in ('initial','warm_replay'):
            m = json.loads((copied / f'manifests/{label}.json').read_text())
            self.assertEqual(m['e4']['journal_path'], 'datasets/dataset.jsonl')
        self.assertEqual((copied / 'datasets/dataset.jsonl').read_bytes(), source_journal)

        workspace = self.base / 'B' / 'workspace'
        bind = bundle.bind_bundle(bundle_root=copied, workspace_leaf=workspace)
        bound_dataset = Path(bind['e5_dataset'])
        self.assertTrue(bound_dataset.is_relative_to(workspace))
        self.assertEqual(bound_dataset.read_bytes(), source_journal)
        self.assertEqual(hashlib.sha256(bound_dataset.read_bytes()).hexdigest(), self.context['sha256'])
        self.assertEqual(bound_dataset.stat().st_mode & 0o777, 0o600)
        self.assertEqual(bound_dataset.parent.stat().st_mode & 0o777, 0o700)
        self.assertTrue(Path(bind['e5_context']).is_file())
        self.assertTrue(Path(bind['e5_phase_plan']).is_file())
        plan = json.loads(Path(bind['e5_phase_plan']).read_text())
        self.assertEqual(plan['phases'], ['initial','warm_replay','control'])
        self.assertEqual(plan['viewer_lifecycle']['mode'], 'continuous')
        spec = json.loads(Path(bind['e5_run_spec']).read_text())
        self.assertEqual({Path(row['manifest']).stem for row in spec['paperload']},
                         {'initial','warm_replay','viewer','control'})
        for label in ('initial','warm_replay'):
            manifest = json.loads((workspace / f'runtime/manifests/{label}.json').read_text())
            self.assertEqual(manifest['e4']['journal_path'], bind['e5_dataset'])
        control_manifest = json.loads((workspace / 'runtime/manifests/control.json').read_text())
        c_tls = control_manifest.get('tls', {})
        self.assertTrue(c_tls.get('https_endpoint'))
        self.assertTrue(c_tls.get('https_ca_file'))
        self.assertNotIn('mqtt_tls_endpoint', c_tls)
        self.assertNotIn('mqtt_tls_ca_file', c_tls)
        self.assertNotIn('mqtt_tls_server_name', c_tls)
        for env_path in (workspace / 'runtime/gateway-env').glob('*.json'):
            env = json.loads(env_path.read_text())
            self.assertFalse(Path(env['GATEWAY_OUTBOX_PATH']).exists())
        self.assertEqual((copied / 'datasets/dataset.jsonl').read_bytes(), source_journal)

    def test_bound_workspace_rejects_tampered_dataset_via_client_verification(self):
        import scripts.paper_p8_client as client
        result = bundle.export_bundle(**self._export_args(self.base / 'A' / 'bundle-tamper'))
        copied = self.base / 'B' / 'portable-tamper'
        copied.parent.mkdir(parents=True)
        shutil.copytree(Path(result['bundle_root']), copied)
        workspace = self.base / 'B' / 'workspace-tamper'
        bind = bundle.bind_bundle(bundle_root=copied, workspace_leaf=workspace)
        bound_dataset = Path(bind['e5_dataset'])

        # Untampered workspace passes client verification
        self.assertEqual(client._workspace(workspace), workspace)

        # Tampering with bound dataset content fails digest verification immediately
        orig_bytes = bound_dataset.read_bytes()
        bound_dataset.write_bytes(orig_bytes[:-1] + b'X')
        with self.assertRaisesRegex(ValueError, 'bound input provenance mismatch'):
            client._workspace(workspace)

    def test_export_rejects_wrong_phase_plan_and_dataset_mutation(self):
        bad = self._export_args(self.base / 'A' / 'bad-phase')
        bad['e5_lifecycle'] = dict(bad['e5_lifecycle'], phases=['initial','control'])
        with self.assertRaisesRegex(ValueError, 'bundle_e5_lifecycle_rejected'):
            bundle.export_bundle(**bad)
        bad = self._export_args(self.base / 'A' / 'bad-dataset')
        bad['datasets'] = {'shared_journal': self.mqtt_ca}
        with self.assertRaisesRegex(ValueError, 'bundle_e5_dataset_context_mismatch'):
            bundle.export_bundle(**bad)

    def test_export_rejects_child_experiment_mismatch_and_sqlite_journal(self):
        args = self._export_args(self.base / 'A' / 'bad-child')
        manifest = Path(args['manifests']['initial'])
        original = json.loads(manifest.read_text())
        manifest.write_text(json.dumps(dict(original, experiment='E5')))
        manifest.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'bundle_manifest_identity_mismatch'):
            bundle.export_bundle(**args)
        manifest.write_text(json.dumps(original))
        manifest.chmod(0o600)

        args = self._export_args(self.base / 'A' / 'sqlite-as-journal')
        sqlite_file = self.private / 'wrong.sqlite'
        sqlite_file.write_bytes(b'SQLite format 3\x00' + b'x' * 64)
        sqlite_file.chmod(0o600)
        args['datasets'] = {'shared_journal': sqlite_file}
        with self.assertRaisesRegex(ValueError, 'bundle_e5_dataset_context_mismatch'):
            bundle.export_bundle(**args)

    def test_export_rejects_journal_path_escape_and_symlinked_dataset(self):
        args = self._export_args(self.base / 'A' / 'escape')
        path = Path(args['manifests']['initial'])
        original = json.loads(path.read_text())
        manifest = json.loads(path.read_text())
        manifest['e4']['journal_path'] = '/etc/passwd'
        path.write_text(json.dumps(manifest))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'bundle_e5_replay_manifest_rejected'):
            bundle.export_bundle(**args)

        path.write_text(json.dumps(original))
        path.chmod(0o600)
        symlink = self.private / 'dataset-symlink.jsonl'
        symlink.symlink_to(self.context['dataset_path'])
        args = self._export_args(self.base / 'A' / 'symlink-dataset')
        args['datasets'] = {'shared_journal': symlink}
        with self.assertRaisesRegex(ValueError, 'bundle_source_symlink_rejected'):
            bundle.export_bundle(**args)

    def test_inspect_rejects_tampered_dataset_context_and_phase_plan(self):
        result = bundle.export_bundle(**self._export_args(self.base / 'A' / 'bundle'))
        root = Path(result['bundle_root'])
        dataset = root / 'datasets/dataset.jsonl'
        dataset.write_bytes(dataset.read_bytes() + b'\n')
        with self.assertRaisesRegex(ValueError, 'bundle_checksum_mismatch'):
            bundle.inspect_bundle(root)

    def test_inspect_rejects_context_tampering(self):
        result = bundle.export_bundle(**self._export_args(self.base / 'A' / 'bundle-context'))
        root = Path(result['bundle_root'])
        path = root / 'contexts/e5-context.json'
        value = json.loads(path.read_text())
        value['seed'] += 1
        path.write_text(json.dumps(value))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'bundle_checksum_mismatch'):
            bundle.inspect_bundle(root)


if __name__ == '__main__':
    unittest.main()
