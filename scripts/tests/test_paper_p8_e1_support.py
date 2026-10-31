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

    def test_e1_role_manifests_keep_synthetic_load_on_telemetry_only(self):
        viewer, telemetry = e1_role_manifests(
            {'schema_version': 1, 'durability_mode': 'synthetic-load'},
            subscriptions=[], viewer_identity='viewer.json', telemetry_identity='telemetry.json',
            gateway_refs={}, root='run')
        self.assertNotIn('durability_mode', viewer)
        self.assertEqual(viewer['viewer'], {'mode': 'healthy'})
        self.assertEqual(telemetry['durability_mode'], 'synthetic-load')

    def test_remote_export_passes_exact_prepared_roles_and_session_correlation(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); root.chmod(0o700)
            viewer = root / 'viewer.json'; viewer.write_text('{"user-000":"jwt","user-001":"jwt"}')
            telemetry = root / 'telemetry.json'; telemetry.write_text('{}')
            gateway = root / 'gw.json'; gateway.write_text('{"username":"gw-a","password":"private"}')
            run_spec = root / 'run-spec.json'
            run_spec.write_text(json.dumps({'paperload': [
                {'manifest': str(root/'viewer.manifest.json')},
                {'manifest': str(root/'telemetry.manifest.json')}]}))
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
                       bound_gateway_environment=None):
        root.mkdir(mode=0o700)
        binary = b'paperload-binary'
        digest = hashlib.sha256(binary).hexdigest()
        payloads = {
            'results/runner/run.json': {'status':'COMPLETE'},
            'results/runner/provenance.json': {'binary_sha256':digest,
                'binary_os':sys.platform,'binary_arch':platform.machine()},
            'results/workloads/viewer/workload.json': {'status':'COMPLETE','experiment':'E1',
                'arm':'e1-pilot','run_id':'p8e1-'+('a'*32),'mode':'remote','profile':'pilot'},
            'results/workloads/telemetry/workload.json': {'status':'COMPLETE','experiment':'E2',
                'arm':'e1-pilot','run_id':'p8e1-'+('a'*32),'mode':'remote','profile':'pilot'},
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
        data=json.dumps(summary).encode()
        (root/'results.json').write_bytes(data); (root/'results.json').chmod(0o600)
        prepared={'session_id':'a'*32,'run_id':'p8e1-'+'a'*32,
            'common':{'arm':'e1-pilot'},'bundle_sha256':'b'*64,'bundle_inventory':[],
            'gateway_refs':{'gw-a':'/unused/a','gw-b':'/unused/b'}}
        return prepared,digest

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

    def valid_inputs(self):
        mapping = mixed_e1_mapping(self.targets)
        viewers = [{'id': r['viewer_id'], 'unique_events': sum(5 for ids in mapping['expected_recipients_by_target'].values()
            if r['viewer_id'] in ids), 'duplicates': 0, 'wrong_namespace': 0} for r in mapping['viewers']]
        return (mapping, {'status': 'COMPLETE', 'telemetry': {'generated_unique': 20,
            'offered_unique': 20, 'publish_attempts': 20, 'broker_pubacks': 20,
            'application_acks': 20, 'pending_messages': 0, 'pending_bytes': 0}},
            {'status': 'COMPLETE','connected_distinct_viewers': 64,'subscribed_distinct_viewers': 64,
             'receiving_distinct_viewers': 64,'duplicates': 0,'wrong_namespace': 0,'viewers': viewers,
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
