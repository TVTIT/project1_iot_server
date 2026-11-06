import json
import hashlib
import platform
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.tests.paper_p8_e1_support import (
    mixed_e1_mapping, role_artifacts, safe_effective_flags, validate_e1_result,
    _drain_websocket_metrics, websocket_metrics_window, e1_role_manifests,
    export_prepared_bundle,
    _load_result_bundle,
)
from scripts.tests.paper_p8_support import (
    e1_expected_deliveries, MAX_MANIFEST_BYTES,
)


class E1PilotSupportTests(unittest.TestCase):
    def setUp(self):
        self.targets = [{'gateway_id': g, 'sensor_id': f's{i}'}
                        for g in ('g-a', 'g-b') for i in range(2)]

    def test_mixed_mapping_recomputes_recipients_and_keeps_user_gateway_scope(self):
        mapping = mixed_e1_mapping(self.targets)
        self.assertEqual(len(mapping['viewers']), 64)
        self.assertEqual(set(mapping['expected_recipients_by_target']),
                         {'g-a\0s0', 'g-a\0s1', 'g-b\0s0', 'g-b\0s1'})
        self.assertEqual(mapping['expected_deliveries_by_target'],
                         {key: len(ids) * 5 for key, ids in mapping['expected_recipients_by_target'].items()})
        for row in mapping['viewers']:
            gateway = 'g-a' if row['user_id'] == 'user-000' else 'g-b'
            self.assertTrue(all(f['gateway_id'] == gateway for f in row['filters']))

    def test_parameterized_mapping_preserves_socket_principal_and_recipient_counts(self):
        mapping = mixed_e1_mapping(self.targets, viewers=128, principals=4,
                                   density='overlap', messages_per_target=7)
        self.assertEqual(mapping['counts']['sockets'], 128)
        self.assertEqual(mapping['counts']['principals'], 4)
        self.assertEqual(len({row['user_id'] for row in mapping['viewers']}), 4)
        self.assertEqual(set(mapping['expected_deliveries_by_target'].values()),
                         {len(ids) * 7 for ids in mapping['expected_recipients_by_target'].values()})
        for target, recipients in mapping['expected_recipients_by_target'].items():
            self.assertEqual(len(recipients), len(set(recipients)), target)
        self.assertTrue(all(all(f['gateway_id'] == ('g-a' if row['user_id'] in ('user-000','user-002') else 'g-b')
                                for f in row['filters']) for row in mapping['viewers']))

    def test_sparse_dense_and_overlap_recipient_sets_are_unique_and_authorized(self):
        counts = {}
        for density in ('sparse', 'dense', 'overlap'):
            mapping = mixed_e1_mapping(self.targets, viewers=128, principals=2, density=density)
            counts[density] = sum(map(len, mapping['expected_recipients_by_target'].values()))
            for row in mapping['viewers']:
                gateway = 'g-a' if row['user_id'] == 'user-000' else 'g-b'
                self.assertTrue(all(f['gateway_id'] == gateway for f in row['filters']))
            self.assertTrue(all(len(ids) == len(set(ids)) for ids in mapping['expected_recipients_by_target'].values()))
        self.assertLess(counts['sparse'], counts['dense'])
        self.assertGreater(counts['overlap'], counts['sparse'])

    def test_official_mapping_serialization_fits_bounded_manifest_and_result_caps(self):
        import json
        from scripts import paper_p8_bundle, paper_p8_client
        for viewers in (5000, 10000):
            mapping = mixed_e1_mapping(self.targets, viewers=viewers, principals=2,
                density='dense', seed=86, messages_per_target=5)
            subscriptions = [
                {'viewer_id': row['viewer_id'], 'user_id': row['user_id'], **filter_row}
                for row in mapping['viewers'] for filter_row in row['filters']
            ]
            viewer_manifest, telemetry_manifest = e1_role_manifests({
                'schema_version': 1, 'profile': 'official', 'arm': 'reference',
                'mode': 'remote', 'run_id': f'e1-{viewers}', 'seed': 86,
                'topology': {'gateways': 2, 'sensors_per_gateway': 2,
                    'users': 2, 'viewers': viewers}, 'targets': self.targets,
                'traffic': {'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': 0},
                'tls': {'https_endpoint': 'https://example.invalid',
                    'mqtt_tls_endpoint': 'mqtts://example.invalid'},
                'gateway_identity_files': {},
            }, subscriptions=subscriptions, viewer_identity='viewer.json',
                telemetry_identity='telemetry.json', gateway_refs={}, root='bundle')
            mapping_bytes = json.dumps(mapping, sort_keys=True, separators=(',', ':')).encode()
            manifest_bytes = json.dumps(viewer_manifest, sort_keys=True, separators=(',', ':')).encode()
            projection = {'seed': mapping['seed'], 'counts': mapping['counts'],
                'expected_recipients_by_target': mapping['expected_recipients_by_target']}
            result = {'profile': 'official', 'mapping': projection,
                'expected_deliveries_by_target': e1_expected_deliveries(mapping,
                    {key: 5 for key in mapping['expected_recipients_by_target']})}
            projection_bytes = json.dumps(projection, sort_keys=True, separators=(',', ':')).encode()
            result_bytes = json.dumps(result, sort_keys=True, separators=(',', ':')).encode()
            self.assertLessEqual(len(mapping_bytes), MAX_MANIFEST_BYTES)
            self.assertLessEqual(len(mapping_bytes), paper_p8_bundle.MAX_FILE_BYTES)
            self.assertLessEqual(len(manifest_bytes), MAX_MANIFEST_BYTES)
            self.assertLessEqual(len(projection_bytes), paper_p8_client.MAX_RESULT_FILE_BYTES)
            self.assertLessEqual(len(result_bytes), paper_p8_client.MAX_RESULT_FILE_BYTES)
            self.assertEqual(len(mapping['viewers']), viewers)
            self.assertEqual(viewer_manifest['limits']['connections'], viewers)
            self.assertLessEqual(viewer_manifest['limits']['workers'], 32)

    def test_e1_role_manifests_keep_synthetic_load_on_telemetry_only(self):
        viewer, telemetry = e1_role_manifests(
            {'schema_version': 1, 'durability_mode': 'synthetic-load'},
            subscriptions=[], viewer_identity='viewer.json', telemetry_identity='telemetry.json',
            gateway_refs={}, root='run')
        self.assertNotIn('durability_mode', viewer)
        self.assertEqual(viewer['viewer'], {'mode': 'healthy'})
        self.assertEqual(telemetry['durability_mode'], 'synthetic-load')

    def test_role_manifest_budgets_follow_selected_mapping_without_buffer_scaling(self):
        mapping = mixed_e1_mapping(self.targets, viewers=512, principals=2,
                                   density='sparse', messages_per_target=9)
        viewer, telemetry = e1_role_manifests(
            {'schema_version': 1, 'topology': {'viewers': 512}},
            subscriptions=[], viewer_identity='viewer.json', telemetry_identity='telemetry.json',
            gateway_refs={}, root='run', mapping=mapping,
            phases={'warmup_seconds': 2, 'measure_seconds': 20, 'drain_seconds': 10, 'cooldown_seconds': 1})
        self.assertEqual(viewer['limits']['connections'], 512)
        self.assertEqual(viewer['topology']['viewers'], 512)
        self.assertEqual(viewer['topology']['users'], 2)
        self.assertLessEqual(viewer['limits']['workers'], 32)
        self.assertEqual(viewer['limits']['pending_messages'], 64)
        self.assertEqual(viewer['phases']['measure_seconds'], 20)
        self.assertEqual(telemetry['phases']['measure_seconds'], 9)

    def test_official_large_rung_default_wall_and_readiness_budgets_cover_ramp(self):
        for viewers, wall in ((2000, 130), (5000, 190), (10000, 210)):
            with self.subTest(viewers=viewers):
                mapping = mixed_e1_mapping(self.targets, viewers=viewers, principals=2,
                    density='dense', messages_per_target=5)
                viewer, _ = e1_role_manifests({'schema_version': 1,
                    'topology': {'viewers': viewers}}, subscriptions=[],
                    viewer_identity='viewer.json', telemetry_identity='telemetry.json',
                    gateway_refs={}, root='run', mapping=mapping,
                    phases={'warmup_seconds': 0, 'measure_seconds': 14,
                            'drain_seconds': 8, 'cooldown_seconds': 0})
                self.assertEqual(viewer['traffic']['ramp_seconds'], min(120, viewers // 50))
                self.assertEqual(viewer['limits']['wall_seconds'], wall)

    def test_e1_timing_budget_validation_and_boundaries(self):
        from scripts.paper_p8_bundle import e1_timing_budget
        # Exact large pairs
        p2000 = e1_timing_budget(2000)
        self.assertEqual(p2000['ready_timeout_seconds'], 100)
        self.assertEqual(p2000['wall_seconds'], 130)
        p5000 = e1_timing_budget(5000)
        self.assertEqual(p5000['ready_timeout_seconds'], 160)
        self.assertEqual(p5000['wall_seconds'], 190)
        p10000 = e1_timing_budget(10000)
        self.assertEqual(p10000['ready_timeout_seconds'], 180)
        self.assertEqual(p10000['wall_seconds'], 210)

        # Pilot accommodates declared ramp
        p_pilot = e1_timing_budget(64, phases={'measure_seconds': 14, 'drain_seconds': 8}, declared_ramp=25)
        self.assertEqual(p_pilot['ramp_seconds'], 25)
        self.assertGreaterEqual(p_pilot['ready_timeout_seconds'], 25)
        self.assertGreaterEqual(p_pilot['wall_seconds'], 25)

        # 1000 viewer case: count-based ramp=20, default phases=22, wall reserves ramp/setup before phases
        p1000 = e1_timing_budget(1000)
        self.assertEqual(p1000['ramp_seconds'], 20)
        self.assertEqual(p1000['ready_timeout_seconds'], 22)
        self.assertEqual(p1000['wall_seconds'], 52)
        self.assertGreaterEqual(p1000['wall_seconds'], 20 + 22 + 8)

        # Small count
        p_small = e1_timing_budget(64)
        self.assertEqual(p_small['ramp_seconds'], 1)
        self.assertEqual(p_small['ready_timeout_seconds'], 22)
        self.assertEqual(p_small['wall_seconds'], 52)

        # Explicit ramp0
        p_ramp0 = e1_timing_budget(1000, declared_ramp=0)
        self.assertEqual(p_ramp0['ramp_seconds'], 0)
        self.assertEqual(p_ramp0['ready_timeout_seconds'], 22)
        self.assertEqual(p_ramp0['wall_seconds'], 52)

        p_small_ramp0 = e1_timing_budget(64, declared_ramp=0)
        self.assertEqual(p_small_ramp0['ramp_seconds'], 0)
        self.assertEqual(p_small_ramp0['ready_timeout_seconds'], 22)
        self.assertEqual(p_small_ramp0['wall_seconds'], 52)

        # Differing phases
        diff_phases = {'warmup_seconds': 5, 'measure_seconds': 20, 'drain_seconds': 10, 'cooldown_seconds': 5}
        p_diff = e1_timing_budget(1000, phases=diff_phases)
        self.assertEqual(p_diff['ramp_seconds'], 20)
        self.assertEqual(p_diff['ready_timeout_seconds'], 35)
        self.assertEqual(p_diff['wall_seconds'], 83)

        # Invalid viewers
        for bad_v in (0, -1, True, False, '2000', 1.5):
            with self.assertRaises(ValueError):
                e1_timing_budget(bad_v)

        # Invalid phases
        for bad_p in ({'measure_seconds': -1}, {'measure_seconds': True}, 'not-dict'):
            with self.assertRaises(ValueError):
                e1_timing_budget(64, phases=bad_p)

        # Invalid ramp
        for bad_r in (-1, True, 'ramp'):
            with self.assertRaises(ValueError):
                e1_timing_budget(64, declared_ramp=bad_r)

    def test_e1_role_manifests_generate_finite_viewer_receive_budgets(self):
        for viewer_count in (2000, 5000, 10000):
            with self.subTest(viewers=viewer_count):
                mapping = mixed_e1_mapping(self.targets, viewers=viewer_count, principals=2,
                    density='mixed', messages_per_target=5)
                viewer, _ = e1_role_manifests({'schema_version': 1,
                    'topology': {'viewers': viewer_count}}, subscriptions=[],
                    viewer_identity='viewer.json', telemetry_identity='telemetry.json',
                    gateway_refs={}, root='run', mapping=mapping,
                    phases={'warmup_seconds': 0, 'measure_seconds': 14,
                            'drain_seconds': 8, 'cooldown_seconds': 0})
                settings = viewer['viewer']
                total_deliveries = sum(mapping['expected_deliveries_by_target'].values())
                self.assertEqual(settings['max_messages'], total_deliveries)
                self.assertEqual(settings['max_bytes'], total_deliveries * 8192)
                self.assertLessEqual(settings['max_messages'], 10_000_000)
                self.assertLessEqual(settings['max_bytes'], 1 << 30)

    def test_e1_viewer_receive_budget_rejects_over_cap_and_invalid_counts(self):
        mapping = mixed_e1_mapping(self.targets, viewers=2000, principals=2,
            density='mixed', messages_per_target=5)
        args = dict(subscriptions=[], viewer_identity='viewer.json',
            telemetry_identity='telemetry.json', gateway_refs={}, root='run', mapping=mapping)
        mapping['expected_deliveries_by_target'] = {'target': 10_000_001}
        with self.assertRaisesRegex(ValueError, 'max_messages'):
            e1_role_manifests({'schema_version': 1, 'topology': {'viewers': 2000}}, **args)
        mapping['expected_deliveries_by_target'] = {'target': 131_073}
        with self.assertRaisesRegex(ValueError, 'max_bytes'):
            e1_role_manifests({'schema_version': 1, 'topology': {'viewers': 2000}}, **args)
        mapping['expected_deliveries_by_target'] = {'target': True}
        with self.assertRaisesRegex(ValueError, 'expected_deliveries'):
            e1_role_manifests({'schema_version': 1, 'topology': {'viewers': 2000}}, **args)

    def test_remote_collector_uses_session_lifetime_budget_not_wall_plus_ten(self):
        from unittest import mock
        from scripts.tests import paper_p8_e1_support as e1
        prepared={'pg':'pg','targets_db':[],'tokens':{'admin':'opaque'}}
        smoke=mock.Mock()
        smoke.paper_p1_count_db.return_value={}
        smoke.paper_p0_stats.return_value={}
        collector=mock.Mock(); collector.resources=[{}]
        with mock.patch.object(e1,'_admin_metrics',return_value={}), \
             mock.patch.object(e1,'BoundedCollector',return_value=collector) as factory:
            e1.begin_remote_collection(smoke,None,None,None,None,prepared,phase_id='test',
                collector_lifetime_budget_seconds=900, workload_lifecycle_seconds=240)
        self.assertEqual(factory.call_args.kwargs['deadline_seconds'],900)

    def test_remaining_collector_budget_accounts_for_delayed_copy_and_reserve(self):
        from scripts.tests.paper_p8_e1_support import remaining_collector_budget
        # 30s cleanup reserve is separate; the 60s import/copy allowance is
        # included in the required post-begin window.
        self.assertEqual(remaining_collector_budget(1000, 240, clock=lambda:100),870)
        self.assertEqual(remaining_collector_budget(1000, 240, clock=lambda:370),600)
        with self.assertRaisesRegex(ValueError, 'collector_session_budget_insufficient'):
            remaining_collector_budget(1000, 240, clock=lambda:701)
        with self.assertRaisesRegex(ValueError, 'collector_session_budget_insufficient'):
            remaining_collector_budget(100, 240, clock=lambda:70)

    def test_e1_lifecycle_rejects_non_dict_role_manifest(self):
        from scripts.paper_p8_bundle import _derive_e1_lifecycle
        with self.assertRaisesRegex(ValueError, 'bundle_e1_lifecycle_rejected'):
            _derive_e1_lifecycle({'viewer': [], 'telemetry': {'roles':['telemetry'],
                'limits':{'wall_seconds':30}}})

    def test_begin_remote_collection_rejects_window_shorter_than_lifecycle(self):
        from unittest import mock
        from scripts.tests import paper_p8_e1_support as e1
        prepared={'pg':'pg','targets_db':[],'tokens':{'admin':'opaque'}}
        with mock.patch.object(e1,'_admin_metrics',return_value={}), \
             mock.patch.object(e1,'BoundedCollector') as factory:
            with self.assertRaisesRegex(ValueError, 'collector_session_budget_insufficient'):
                e1.begin_remote_collection(mock.Mock(),None,None,None,None,prepared,
                    phase_id='test', collector_lifetime_budget_seconds=100,
                    workload_lifecycle_seconds=90)
        factory.assert_not_called()

    def test_remote_export_passes_exact_prepared_roles_and_session_correlation(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); root.chmod(0o700)
            viewer = root / 'viewer.json'; viewer.write_text('{"user-000":"jwt","user-001":"jwt"}')
            telemetry = root / 'telemetry.json'; telemetry.write_text('{}')
            gateway = root / 'gw.json'; gateway.write_text('{"username":"gw-a","password":"private"}')
            run_spec = root / 'run-spec.json'
            viewer_manifest = root / 'viewer.manifest.json'
            telemetry_manifest = root / 'telemetry.manifest.json'
            viewer_manifest.write_text(json.dumps({'roles':['viewer'],'limits':{'wall_seconds':210}}))
            telemetry_manifest.write_text(json.dumps({'roles':['telemetry'],'limits':{'wall_seconds':30}}))
            viewer_manifest.chmod(0o600); telemetry_manifest.chmod(0o600)
            run_spec.write_text(json.dumps({'paperload': [
                {'role':'viewer','manifest': str(viewer_manifest)},
                {'role':'telemetry','manifest': str(telemetry_manifest)}]}))
            ca_https = root / 'https-ca.pem'; ca_https.write_text('https-ca')
            ca_mqtt = root / 'mqtt-ca.pem'; ca_mqtt.write_text('mqtt-ca')
            prepared = {'viewer_identity': viewer, 'telemetry_identity': telemetry,
                'gateway_refs': {'gw-a': gateway}, 'plan': {'run_spec': str(run_spec)},
                'common': {'arm': 'e1-pilot', 'topology': {'gateways': 2,
                    'sensors_per_gateway': 2, 'users': 2, 'viewers': 64}},
                'targets': self.targets, 'root': root, 'session_id': 'a' * 32, 'phase': 'pilot',
                'https_ca_file': ca_https, 'mqtt_ca_file': ca_mqtt}
            with mock.patch('scripts.paper_p8_bundle.export_bundle', return_value={'bundle_root':'fresh'}) as export:
                result = export_prepared_bundle(prepared, output_leaf=root/'bundle')
            kwargs = export.call_args.kwargs
            self.assertEqual(result, {'bundle_root':'fresh'})
            self.assertEqual(kwargs['session_id'], 'a' * 32)
            self.assertEqual(kwargs['arm'], 'e1-pilot')
            self.assertEqual(kwargs['phase'], 'pilot')
            self.assertEqual(kwargs['role_metadata']['roles']['viewer']['principals'], ['user-000','user-001'])
            self.assertEqual(kwargs['role_metadata']['roles']['telemetry']['principals'], [])
            self.assertNotIn('admin', json.dumps(kwargs['role_metadata']))

    def _result_bundle(self, root, *, session_id='a'*32, role='telemetry', unsafe_path=None,
                       bound_gateway_environment=None, profile='pilot', session_profile=None):
        root.mkdir(mode=0o700)
        binary = b'paperload-binary'
        digest = hashlib.sha256(binary).hexdigest()
        payloads = {
            'results/runner/run.json': {'status':'COMPLETE'},
            'results/runner/provenance.json': {'binary_sha256':digest,
                'binary_os':sys.platform,'binary_arch':platform.machine()},
            'results/workloads/viewer/workload.json': {'status':'COMPLETE','experiment':'E1',
                'arm':'e1-pilot','run_id':'p8e1-'+('a'*32),'mode':'remote','profile':profile},
            'results/workloads/telemetry/workload.json': {'status':'COMPLETE','experiment':'E2',
                'arm':'e1-pilot','run_id':'p8e1-'+('a'*32),'mode':'remote','profile':profile},
        }
        rows=[]
        for rel, value in payloads.items():
            data=json.dumps(value,separators=(',',':')).encode()
            path=root/rel; path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            path.write_bytes(data); path.chmod(0o600)
            rows.append({'path':rel,'size_bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        summary={'schema_version':1,'full_p8_accepted':False,'physical_ab':'NOT RUN',
            'session':{'session_id':session_id,'run_ids':['p8e1-'+'a'*32],
                'experiment':'E1','arm':'e1-pilot','phase':'pilot'},
            'source_bundle_sha256':'b'*64,'runner_status':'COMPLETE',
            'expected_workload_roles':['telemetry','viewer'],
            'present_workload_roles':['telemetry','viewer'],'missing_workload_roles':[],
            'source_inventory':[], 'bound_manifest_sha256':{'viewer':'1'*64,'telemetry':'2'*64},
            'bound_gateway_environment_sha256':bound_gateway_environment or {}, 'bound_run_spec_sha256':'3'*64,
            'binary':payloads['results/runner/provenance.json'],
            'files':rows}
        if unsafe_path:
            summary['files'][0]['path']=unsafe_path
        if session_profile is not None:
            summary['session']['profile'] = session_profile
        data=json.dumps(summary).encode()
        (root/'results.json').write_bytes(data); (root/'results.json').chmod(0o600)
        prepared={'session_id':'a'*32,'run_id':'p8e1-'+'a'*32,
            'common':{'arm':'e1-pilot'},'profile':profile,'bundle_sha256':'b'*64,'bundle_inventory':[],
            'gateway_refs':{'gw-a':'/unused/a','gw-b':'/unused/b'}}
        return prepared,digest

    def test_imported_session_accepts_selected_profile_and_legacy_shape(self):
        for profile in ('pilot', 'official'):
            for include_profile in (False, True):
                with self.subTest(profile=profile, include_profile=include_profile), \
                        tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as directory:
                    root = Path(directory) / 'results'
                    prepared, binary = self._result_bundle(root, profile=profile,
                        session_profile=profile if include_profile else None)
                    # Older prepared metadata stores the profile in common instead.
                    if profile == 'pilot':
                        prepared['common']['profile'] = prepared.pop('profile')
                    loaded = _load_result_bundle(root, prepared, phase_id='pilot', binary_sha256=binary)
                    self.assertEqual(loaded['viewer']['profile'], profile)
                    self.assertEqual(loaded['telemetry']['profile'], profile)

    def test_imported_profile_session_rejects_mismatch_and_unknown_fields(self):
        changes = (
            {'profile': 'pilot'}, {'profile': None}, {'profile': 'unsupported'},
            {'session_id': 'c' * 32}, {'run_ids': ['another-run']},
            {'phase': 'another-phase'}, {'arm': 'another-arm'},
            {'experiment': 'E3'}, {'unexpected': True},
        )
        for change in changes:
            with self.subTest(change=change), tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as directory:
                root = Path(directory) / 'results'
                prepared, binary = self._result_bundle(root, profile='official', session_profile='official')
                summary_path = root / 'results.json'
                summary = json.loads(summary_path.read_text())
                summary['session'].update(change)
                summary_path.write_text(json.dumps(summary))
                with self.assertRaisesRegex(ValueError, 'result_bundle_correlation_rejected'):
                    _load_result_bundle(root, prepared, phase_id='pilot', binary_sha256=binary)

    def test_imported_result_requires_correlation_all_roles_and_digest_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'results'; prepared,binary=self._result_bundle(root)
            loaded=_load_result_bundle(root,prepared,phase_id='pilot',binary_sha256=binary)
            self.assertEqual(set(loaded),{'viewer','telemetry','_result_status','runner','binary_provenance'})
        for kwargs, code in (({'session_id':'c'*32},'result_bundle_correlation_rejected'),
                             ({'unsafe_path':'../outside'},'result_inventory_rejected'),
                             ({'bound_gateway_environment':{'unexpected':'4'*64}},
                              'result_manifest_provenance_rejected')):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)/'results'; prepared,binary=self._result_bundle(root,**kwargs)
                with self.assertRaisesRegex(ValueError,code):
                    _load_result_bundle(root,prepared,phase_id='pilot',binary_sha256=binary)

    def test_imported_result_rejects_symlink_and_oversized_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'results'; prepared,binary=self._result_bundle(root)
            target=root/'results/runner/run.json'
            data=target.read_bytes(); target.unlink(); outside=Path(directory)/'outside.json'
            outside.write_bytes(data); outside.chmod(0o600); target.symlink_to(outside)
            with self.assertRaisesRegex(ValueError,'result_path_symlink_rejected'):
                _load_result_bundle(root,prepared,phase_id='pilot',binary_sha256=binary)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'results'; prepared,binary=self._result_bundle(root)
            summary=json.loads((root/'results.json').read_text())
            summary['files'][0]['size_bytes']=9<<20
            (root/'results.json').write_text(json.dumps(summary)); (root/'results.json').chmod(0o600)
            with self.assertRaisesRegex(ValueError,'result_inventory_rejected'):
                _load_result_bundle(root,prepared,phase_id='pilot',binary_sha256=binary)

    def valid_inputs(self, *, viewers=32):
        mapping = mixed_e1_mapping(self.targets, viewers=viewers, principals=2, messages_per_target=5)
        viewers = [{'id': r['viewer_id'], 'unique_events': sum(5 for ids in mapping['expected_recipients_by_target'].values()
            if r['viewer_id'] in ids), 'duplicates': 0, 'wrong_namespace': 0} for r in mapping['viewers']]
        expected_unique = sum(mapping['messages_per_target'].values())
        return (mapping, {'status': 'COMPLETE', 'samples_per_message': 100, 'telemetry': {'generated_unique': expected_unique,
            'offered_unique': expected_unique, 'publish_attempts': expected_unique, 'broker_pubacks': expected_unique,
            'application_acks': expected_unique, 'pending_messages': 0, 'pending_bytes': 0}},
            {'status': 'COMPLETE','connected_distinct_viewers': mapping['counts']['sockets'],'subscribed_distinct_viewers': mapping['counts']['sockets'],
             'receiving_distinct_viewers': mapping['counts']['receiving_viewers'],'duplicates': 0,'wrong_namespace': 0,'viewers': viewers,
             'identity_samples': [{'viewer_id': mapping['viewers'][0]['viewer_id'],
                'gateway_id': mapping['viewers'][0]['filters'][0]['gateway_id'],
                'sensor_id': mapping['viewers'][0]['filters'][0].get('sensor_id','s0'),
                'message_id':'producer-0','boot_id':'boot-0','first_sequence':'0','sample_count':100},
                {'viewer_id': mapping['viewers'][0]['viewer_id'],
                'gateway_id': mapping['viewers'][0]['filters'][0]['gateway_id'],
                'sensor_id': mapping['viewers'][0]['filters'][0].get('sensor_id','s0'),
                'message_id':'producer-0','boot_id':'boot-0','first_sequence':'0','sample_count':100}]})

    def test_result_validator_accepts_reconciled_pilot(self):
        mapping, workload, sessions = self.valid_inputs()
        headers = [f'producer-{i}' for i in range(20)]
        database={'ledger':20,'raw':2000,'temporal':20,'configuration':0}
        raw=[{'verified_exact':True,'ledger_count':5,'raw_identity_value_mismatches':0}]*4
        result = validate_e1_result(workload, mapping, database_delta=database,
            raw_verification=raw, viewer_sessions=sessions,
            sampled_headers=sessions['identity_samples'], expected_headers={
                'producer-0':{'message_id':'producer-0','gateway_id':self.targets[0]['gateway_id'],
                'sensor_id':self.targets[0]['sensor_id'],'boot_id':'boot-0','first_sequence':'0','sample_count':100}})
        self.assertTrue(result['valid'],result['failures'])

    def test_large_raw_verification_accepts_exact_or_sample_without_weakening_checks(self):
        mapping, workload, sessions = self.valid_inputs(viewers=128)
        headers = {'producer-0': {key: value for key, value in sessions['identity_samples'][0].items()
                                 if key != 'viewer_id'}}
        cases = (
            ({'verified_exact': True, 'verified_sample': False}, True),
            ({'verified_exact': False, 'verified_sample': True}, True),
            ({'verified_exact': False, 'verified_sample': False}, False),
            ({}, False),
            ({'verified_exact': True, 'raw_identity_value_mismatches': 1}, False),
            ({'verified_exact': True, 'ledger_count': 4}, False),
            ({'verified_exact': True, 'ledger_count': 6}, False),
        )
        for changes, expected_valid in cases:
            with self.subTest(changes=changes):
                raw = [{'ledger_count': 5, 'raw_identity_value_mismatches': 0, **changes}
                       for _ in self.targets]
                result = validate_e1_result(workload, mapping,
                    database_delta={'ledger':20,'raw':2000,'temporal':20,'configuration':0},
                    raw_verification=raw, viewer_sessions=sessions,
                    sampled_headers=sessions['identity_samples'], expected_headers=headers)
                self.assertEqual(result['valid'], expected_valid, result['failures'])
                self.assertEqual('raw_identity' in result['failures'], not expected_valid)

    def test_remote_finish_above_64_viewers_accepts_actual_exact_raw_verification(self):
        from types import SimpleNamespace
        from unittest import mock
        from scripts.tests import paper_p8_e1_support as e1

        for viewer_count in (128, 5000):
            with self.subTest(viewers=viewer_count), \
                    tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as directory:
                mapping, telemetry, viewer = self.valid_inputs(viewers=viewer_count)
                telemetry['identity_samples'] = [{key: value
                    for key, value in viewer['identity_samples'][0].items() if key != 'viewer_id'}]
                counts = {'ledger':5,'raw':500,'temporal':5,'configuration':0}
                total = {'ledger':20,'raw':2000,'temporal':20,'configuration':0}
                specs = [SimpleNamespace(**target,
                    sensor_urn=f"urn:ngsi-ld:Sensor:{target['gateway_id']}:{target['sensor_id']}")
                    for target in self.targets]
                collector = mock.Mock(errors=[], metrics=[], resources=[])
                prepared = {'pg':'pg','tokens':{'admin':'opaque'}, 'targets_db':specs,
                    'mapping':mapping, 'bundle_sha256':'b'*64, 'remote_collector':collector,
                    'remote_baseline':{'phase_id':'pilot','metrics':{},
                        'database':dict.fromkeys(total, 0),
                        'sensors':[dict(target, counts=dict.fromkeys(counts, 0))
                                   for target in self.targets]}}
                smoke = mock.Mock()
                smoke.paper_p1_count_db.return_value = total
                smoke.paper_p0_stats.return_value = {}

                def db_psql(registry, pg, query, params):
                    if "json_build_object('ledger'" in query:
                        return json.dumps(counts)
                    if 'json_agg' in query:
                        return json.dumps([{'message_id':f'producer-{i}',
                            'first_sequence':str(i*100), 'last_sequence':str(i*100+99),
                            'sample_count':100} for i in range(5)])
                    if 'SELECT count(*) FROM processed_messages p' in query:
                        return '0'
                    self.fail('unexpected raw-verification query')

                smoke.db_psql.side_effect = db_psql
                binary = Path(directory)/'paperload'
                binary.write_bytes(b'paperload-binary')
                imported = {'telemetry':telemetry, 'viewer':viewer, 'runner':{},
                    '_result_status':dict.fromkeys(('runner','viewer','telemetry'), 'COMPLETE')}
                # Only external result/metrics IO is mocked. Keep the actual DB
                # verification helper and final validator wired together.
                with mock.patch.object(e1, '_load_result_bundle', return_value=imported), \
                     mock.patch.object(e1, '_drain_websocket_metrics', return_value={}), \
                     mock.patch.object(e1, 'websocket_metrics_window',
                                       return_value={'availability':'available'}):
                    result = e1.finish_remote_collection(smoke, None, None, None, [],
                        prepared, Path(directory)/'results', phase_id='pilot',
                        paperload_binary=binary)
                self.assertTrue(result['validation']['valid'], result['validation']['failures'])
                self.assertEqual(result['workload_outcome'], 'VALIDATED')
                self.assertEqual(len(result['raw_verification']), len(self.targets))
                for raw in result['raw_verification']:
                    self.assertTrue(raw['verified_exact'])
                    self.assertFalse(raw['verified_sample'])
                    self.assertEqual(raw['raw_identity_value_mismatches'], 0)
                collector.stop.assert_called_once_with(None)

    def test_small_pilot_still_requires_exact_raw_verification(self):
        mapping, workload, sessions = self.valid_inputs()
        headers = {'producer-0': {key: value for key, value in sessions['identity_samples'][0].items()
                                 if key != 'viewer_id'}}
        result = validate_e1_result(workload, mapping,
            database_delta={'ledger':20,'raw':2000,'temporal':20,'configuration':0},
            raw_verification=[{'verified_exact':False,'verified_sample':True,
                'ledger_count':5,'raw_identity_value_mismatches':0} for _ in self.targets],
            viewer_sessions=sessions, sampled_headers=sessions['identity_samples'],
            expected_headers=headers)
        self.assertEqual(result, {'valid':False,'failures':['raw_identity']})

    def test_result_validator_accepts_selected_128_viewer_sampled_raw_and_bounded_headers(self):
        mapping = mixed_e1_mapping(self.targets, viewers=128, principals=2,
                                   density='mixed', messages_per_target=2)
        viewers = [{'id': row['viewer_id'], 'unique_events': sum(
            mapping['messages_per_target'][key] for key, ids in mapping['expected_recipients_by_target'].items()
            if row['viewer_id'] in ids), 'duplicates': 0, 'wrong_namespace': 0}
            for row in mapping['viewers']]
        headers = {'producer-0': {'message_id':'producer-0','gateway_id':'g-a','sensor_id':'s0',
            'boot_id':'boot-0','first_sequence':'0','sample_count':100}}
        sessions = {'status':'COMPLETE','connected_distinct_viewers':128,
            'subscribed_distinct_viewers':128,'receiving_distinct_viewers':128,
            'duplicates':0,'wrong_namespace':0,'viewers':viewers,
            'identity_samples':[{'viewer_id':'viewer-00000','message_id':'producer-0',
                'gateway_id':'g-a','sensor_id':'s0','boot_id':'boot-0','first_sequence':'0',
                'sample_count':100} for _ in range(20)]}
        expected_unique = sum(mapping['messages_per_target'].values())
        result = validate_e1_result({'status':'COMPLETE','samples_per_message':100,
            'telemetry':{'generated_unique':expected_unique,'offered_unique':expected_unique,
                'publish_attempts':expected_unique,'broker_pubacks':expected_unique,
                'application_acks':expected_unique,'pending_messages':0,'pending_bytes':0}},
            mapping, database_delta={'ledger':expected_unique,'raw':expected_unique*100,
                'temporal':expected_unique,'configuration':0},
            raw_verification=[{'verified_sample':True,'ledger_count':2,
                'raw_identity_value_mismatches':0} for _ in range(4)],
            viewer_sessions=sessions,sampled_headers=sessions['identity_samples'],
            expected_headers=headers)
        self.assertTrue(result['valid'], result['failures'])

    def test_result_validator_accepts_native_viewer_rows_without_per_viewer_namespace_counter(self):
        mapping, workload, sessions = self.valid_inputs()
        for row in sessions['viewers']:
            row.pop('wrong_namespace')
        result = validate_e1_result(workload, mapping,
            database_delta={'ledger':20,'raw':2000,'temporal':20,'configuration':0},
            raw_verification=[{'verified_exact':True,'ledger_count':5,'raw_identity_value_mismatches':0}]*4,
            viewer_sessions=sessions, sampled_headers=sessions['identity_samples'],
            expected_headers={'producer-0':{'message_id':'producer-0',
                'gateway_id':self.targets[0]['gateway_id'],'sensor_id':self.targets[0]['sensor_id'],
                'boot_id':'boot-0','first_sequence':'0','sample_count':100}})
        self.assertTrue(result['valid'], result['failures'])

    def test_result_validator_reports_independent_reconciliation_failures(self):
        mapping, workload, sessions = self.valid_inputs()
        sessions['viewers'][0]['unique_events'] += 1
        result = validate_e1_result(workload, mapping, database_delta={'ledger': 19, 'raw': 2000, 'temporal': 20},
            raw_verification=[{'verified_exact': False}] * 4, viewer_sessions=sessions,
            sampled_headers=['wrong'], expected_headers=[f'producer-{i}' for i in range(20)])
        self.assertFalse(result['valid'])
        self.assertTrue({'viewer_delivery_counts','database_delta','raw_identity',
            'websocket_producer_headers'} <= set(result['failures']))

    def test_result_validator_rejects_incomplete_delivery_even_when_actuals_agree(self):
        mapping, workload, sessions = self.valid_inputs()
        sessions['viewers'][0]['unique_events'] -= 1
        result = validate_e1_result(workload, mapping,
            database_delta={'ledger':20,'raw':2000,'temporal':20,'configuration':0},
            raw_verification=[{'verified_exact':True,'ledger_count':5,'raw_identity_value_mismatches':0}]*4,
            viewer_sessions=sessions, sampled_headers=['producer-0'], expected_headers=['producer-0'])
        self.assertFalse(result['valid'])
        self.assertIn('viewer_delivery_counts', result['failures'])

    def test_role_artifact_validation_uses_role_output_paths_and_requires_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            for role in ('viewer','telemetry'):
                path=Path(tmp)/'plan'/f'{role}-output'/'workload.json'
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps({'status':'COMPLETE'}))
            self.assertEqual(set(role_artifacts(tmp)), {'viewer','telemetry'})
            (Path(tmp)/'plan/viewer-output/workload.json').write_text('{"status":"PENDING"}')
            with self.assertRaisesRegex(ValueError,'viewer_artifact_incomplete'):
                role_artifacts(tmp)

    def test_effective_report_allowlist_does_not_serialize_secrets(self):
        effective={'services':{'backend':{'environment':{'WEBSOCKET_ENABLED':'true',
            'WS_MAX_SESSIONS':'64','POSTGRES_PASSWORD':'never-report',
            'JWT_SECRET':'never-report-either','ADMIN_TOKEN':'secret'}}}}
        rendered=json.dumps(safe_effective_flags(effective))
        self.assertIn('WS_MAX_SESSIONS',rendered)
        for secret in ('never-report','JWT_SECRET','ADMIN_TOKEN'):
            self.assertNotIn(secret,rendered)

    def test_drain_returns_only_known_zero_terminal_snapshot(self):
        snapshot={'websocket_metrics': dict.fromkeys((
            'sessions','subscriptions','session_reference_bytes','global_queued_work',
            'global_queued_bytes','session_queued_in_flight_bytes'), 0)}
        self.assertIs(_drain_websocket_metrics(lambda: snapshot), snapshot)

    def test_drain_rejects_nonzero_missing_and_timed_out_metrics(self):
        nonzero={'websocket_metrics': {'sessions': 1}}
        with self.assertRaisesRegex(ValueError, 'websocket_drain_rejected'):
            _drain_websocket_metrics(lambda: nonzero, clock=lambda: 0, sleep=lambda _: None,
                                     deadline_seconds=0, max_scrapes=1)
        with self.assertRaisesRegex(ValueError, 'websocket_drain_rejected'):
            _drain_websocket_metrics(lambda: {}, clock=lambda: 0, sleep=lambda _: None,
                                     deadline_seconds=0, max_scrapes=1)
        now=[0.0]
        def still_busy():
            now[0] += 1
            return {'websocket_metrics': dict.fromkeys((
                'sessions','subscriptions','session_reference_bytes','global_queued_work',
                'global_queued_bytes','session_queued_in_flight_bytes'), 1)}
        with self.assertRaisesRegex(ValueError, 'websocket_drain_rejected'):
            _drain_websocket_metrics(still_busy, clock=lambda: now[0], sleep=lambda _: None,
                                     deadline_seconds=1, max_scrapes=20)

    def test_websocket_window_checks_same_instance_counters_and_histograms(self):
        keys=('fanout_indexed_selections','fanout_scan_evaluations','deliveries_enqueued',
              'telemetry_writes_completed','drops','slow_closes','aggregate_capacity_closes',
              'recheck_sessions_examined','recheck_distinct_keys','recheck_queries_started',
              'recheck_timeouts')
        before={'process_instance':'same','websocket_metrics':dict.fromkeys(keys, 0)}
        after={'process_instance':'same','websocket_metrics':dict.fromkeys(keys, 0)}
        after['websocket_metrics'].update(fanout_mode='indexed', fanout_indexed_selections=20,
            deliveries_enqueued=50, telemetry_writes_completed=50)
        called=[]
        def histogram(*args):
            called.append(args[2:])
            return {'availability':'available','count':0}
        result=websocket_metrics_window(before, after, histogram, 50)
        self.assertEqual(result['availability'], 'available', result)
        self.assertEqual(len(called), 4)
        changed=dict(after, process_instance='other')
        self.assertEqual(websocket_metrics_window(before, changed, histogram, 50)['reason'],
                         'process_instance_changed')


if __name__ == '__main__':
    unittest.main()
