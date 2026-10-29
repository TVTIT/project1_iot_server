import json
import tempfile
import unittest
from pathlib import Path

from scripts.tests.paper_p8_e1_support import (
    mixed_e1_mapping, role_artifacts, safe_effective_flags, validate_e1_result,
    _drain_websocket_metrics, websocket_metrics_window, e1_role_manifests,
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
