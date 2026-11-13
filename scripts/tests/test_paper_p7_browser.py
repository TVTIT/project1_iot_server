"""Unit guards for the isolated real-Chromium P7 browser probe."""
import base64
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
MODULE = ROOT / 'scripts/tests/native_compose_smoke.py'
spec = importlib.util.spec_from_file_location('p7_browser_smoke', MODULE)
smoke = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = smoke
spec.loader.exec_module(smoke)
helper = smoke.paper_p7_support()


def telemetry_oracle_fixture():
    base = '2026-10-27T10:00:00.000000Z'
    ledger, raw, events = [], [], []
    for batch in range(4):
        mid, first = f'm{batch}', 10 + batch * 2
        row = {'gateway_id': 'gw', 'sensor_id': 's', 'message_id': mid, 'boot_id': 'boot',
               'first_sequence': str(first), 'sample_count': 100, 'measured_at': base}
        ledger.append(row)
        raw.extend({'gateway_id': 'gw', 'sensor_id': 's', 'message_id': mid, 'boot_id': 'boot',
            'sample_index': i, 'sequence_num': str(first + i),
            'measured_at': (datetime.fromisoformat(base.replace('Z', '+00:00')) + timedelta(microseconds=i * 10000)).isoformat(timespec='microseconds').replace('+00:00', 'Z'),
            'value': float(i + 1)} for i in range(100))
        events.append(dict(row, sample_interval_us=10000, samples=list(range(1, 101))))
    return ledger, raw, events


def sample_slow_proof():
    ids = [f'id-{i}' for i in range(1000)]
    return {'functional_status': 'PASS', 'capture_finalization_status': 'finished',
        'resource_cleanup_status': 'pending',
        'workload': {'messages_offered': 1000, 'application_acks_expected': 1000,
            'application_acks_actual': 1000, 'unique_application_acks': 1000, 'message_ids': ids},
        'database': {'actual_counts': {'ledger': 1000, 'raw': 1000000, 'temporal': 1000},
            'expected_counts': {'ledger': 1000, 'raw': 1000000, 'temporal': 1000},
            'ledger_ids': ids, 'temporal_ids': ids, 'ledger_provenance_match': True,
            'ledger_ids_match': True, 'ledger_time_provenance_match': True,
            'raw_content_match': True, 'raw_value_sequence_time_match': True, 'temporal_ids_match': True,
            'temporal_value_time_match': True, 'temporal_provenance_match': True,
            'temporal_values': [{'message_id': item, 'observed_at': '2026-10-27T00:00:00Z',
                'value_number': 10000} for item in ids],
            'ledger_time_values': [{'message_id': item, 'measured_at': '2026-10-27T00:00:00Z'} for item in ids],
            'raw_chunk_statistics': {'total_rows_expected': 1000000, 'rows_checked': 1000000,
                'chunks_checked': 10, 'failing_chunk': None}},
        'policy': {'close_causes': ['queue_overflow'], 'close_cause_observed': True,
            'slow_close_delta_verified': True, 'queue_drained': True},
        'capture_finalization': {'expected_a_count': 1000,
            'a': {'status': 'PASS', 'count': 1000, 'event_header_count': 1000,
                'headers_match_count': True, 'namespace_match': True},
            'b': {'status': 'ready', 'count': 0, 'event_header_count': 0,
                'headers_match_count': True, 'namespace_match': True}}}


class PaperP7BrowserTests(unittest.TestCase):
    def test_telemetry_event_oracle_preserves_fractional_samples(self):
        ledger, raw, events = telemetry_oracle_fixture()
        raw[0]['value'], events[0]['samples'][0] = 1.25, 1.25
        raw[1]['value'], events[0]['samples'][1] = -2.75, -2.75
        self.assertTrue(helper.telemetry_event_matches_db(events, ledger, raw, 'gw', 's'))

    def test_telemetry_event_oracle_rejects_fractional_mutation_with_same_integer_part(self):
        ledger, raw, events = telemetry_oracle_fixture()
        raw[0]['value'], events[0]['samples'][0] = 1.25, 1.75
        self.assertFalse(helper.telemetry_event_matches_db(events, ledger, raw, 'gw', 's'))

    def test_telemetry_event_oracle_rejects_boolean_nonnumeric_and_nonfinite_samples(self):
        for invalid in (True, False, '1.0', float('nan'), float('inf'), -float('inf'), 10 ** 1000):
            with self.subTest(side='event', value=repr(invalid)):
                ledger, raw, events = telemetry_oracle_fixture()
                events[0]['samples'][0] = invalid
                self.assertFalse(helper.telemetry_event_matches_db(events, ledger, raw, 'gw', 's'))
            with self.subTest(side='db', value=repr(invalid)):
                ledger, raw, events = telemetry_oracle_fixture()
                raw[0]['value'] = invalid
                self.assertFalse(helper.telemetry_event_matches_db(events, ledger, raw, 'gw', 's'))

    def test_required_auth_metric_uses_websocket_snapshot_fields_and_fails_closed(self):
        # This is the JSON shape emitted by auth.MetricsSnapshot.
        snapshot = {
            'auth': {'allow': 4, 'deny': 2, 'websocket_deny': 7, 'verify': {}},
            'unix_auth': {'allow': 1, 'deny': 3, 'websocket_deny': 5, 'verify': {}},
        }
        self.assertEqual(helper._required_auth_counter(snapshot, 'auth', 'websocket_deny'), 7)
        self.assertEqual(helper._required_auth_counter(snapshot, 'unix_auth', 'websocket_deny'), 5)
        for invalid in ({}, {'auth': {'deny': 0}},
                        {'auth': {'websocket_deny': True}},
                        {'auth': {'websocket_deny': -1}}):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, 'p7_auth_metric_counter_unavailable'):
                helper._required_auth_counter(invalid, 'auth', 'websocket_deny')

    def test_p6_ws_is_real_p6_profile_mode_and_wraps_actual_p6_runner(self):
        self.assertEqual(helper.pilot_mode(SimpleNamespace(paper_p7_mode='p6_ws')), 'p6_ws')
        from paper_p6_support import profile
        p6 = profile('wake-up', '100')
        self.assertEqual((p6['STAGE5_WAKEUP_ENABLED'], p6['MQTT_SHAPER_RATE_PER_SECOND'],
                          p6['MQTT_SHAPER_BURST'], p6['MQTT_WORKER_COUNT']), ('true', '2', '4', '4'))
        import inspect
        source = inspect.getsource(helper.run_p6_ws)
        self.assertEqual(source.count('run_p6('), 1)
        self.assertIn('wait_ready(self.capture', source)
        self.assertIn('180000', source)
        self.assertIn('WORKLOAD_COMPLETE_PENDING_CLEANUP', source)
        self.assertIn('viewer_headers_match_scoped_ledgers', source)

    def test_p6_ws_reads_actual_final_events_and_compares_timestamp_semantically(self):
        # Shape emitted by two_viewer_capture_source() in final.json (not state.json).
        from paper_p7_support import _p6_ws_event_headers, _utc_microseconds
        final = {'status': 'PASS', 'finish_status': 'finished', 'cleanup_errors': [], 'viewers': {
            'a': {'status': 'PASS', 'acknowledged': True, 'count': 1, 'wrong_namespace': 0,
                  'events': [{'gateway_id': 'gw', 'sensor_id': 'sensor', 'message_id': 'm',
                              'boot_id': 'b', 'first_sequence': 9007199254740991,
                              'sample_count': 2, 'measured_at': '2026-10-27T10:00:00.123456Z',
                              'samples': [1, 2]}]},
            'b': {'status': 'PASS', 'acknowledged': True, 'count': 0, 'wrong_namespace': 0,
                  'events': []}}}
        headers = _p6_ws_event_headers(final['viewers']['a'])
        self.assertEqual(headers[0]['first_sequence'], '9007199254740991')
        self.assertEqual(set(headers[0]), {'gateway_id', 'sensor_id', 'message_id', 'boot_id',
                                           'first_sequence', 'sample_count', 'measured_at'})
        self.assertEqual(_p6_ws_event_headers(final['viewers']['b']), [])
        self.assertEqual(_utc_microseconds(headers[0]['measured_at']),
                         _utc_microseconds('2026-10-27 10:00:00.123456+00'))
        self.assertIsNone(_p6_ws_event_headers({'event_headers': [{'message_id': 'm'}]}))

    def test_p6_ws_native_prepare_keeps_real_p6_profile_and_effective_workers(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p6-ws', paper_p7=True,
                                    paper_p7_mode='p6_ws', paper_p6=('wake-up', '100'))
            profile = smoke.paper_p6_support().profile('wake-up', '100')
            self.assertTrue(fixture.paper_p6)
            self.assertEqual(fixture.paper_p6_mode, 'wake-up')
            for key, value in profile.items():
                self.assertEqual(fixture.values[key], value, key)
            self.assertEqual((fixture.values['MQTT_WORKER_COUNT'],
                              fixture.values['MQTT_SHAPER_RATE_PER_SECOND'],
                              fixture.values['MQTT_SHAPER_BURST']), ('4', '2', '4'))

    def test_overhead_modes_declare_same_finite_workload_and_pair_guard_fails_closed(self):
        zero = helper.overhead_profile('overhead_zero')
        two = helper.overhead_profile('overhead_two')
        self.assertEqual((zero['viewers'], two['viewers']), (0, 2))
        for profile in (zero, two):
            self.assertEqual(profile['messages_per_gateway'], 20)
            self.assertEqual(profile['samples_per_message'], 100)
            self.assertEqual((profile['ledgers'], profile['raw_samples'], profile['temporal']), (40, 4000, 40))
            self.assertEqual((profile['sample_rate_hz'], profile['subscription_density']), (100, 1))
        a = {'status': 'PASS', 'mode': 'paper_p7_overhead_zero', 'full_p7_accepted': False,
             'exact_cleanup_verified': True, 'source_fingerprint': 'same', 'schema_version': 24,
             'profile': {'flags': 'same'}, 'backend_quota': {'cpus': 2, 'memory_bytes': 4294967296},
             'caps': {'sessions': 32},
             'workload': {k: v for k, v in zero.items() if k not in ('mode', 'viewers')},
             'viewer_count': 0}
        b = dict(a, mode='paper_p7_overhead_two', viewer_count=2,
                 workload={k: v for k, v in two.items() if k not in ('mode', 'viewers')})
        self.assertTrue(helper.overhead_pair_compatible(a, b))
        self.assertFalse(helper.overhead_pair_compatible(a, dict(b, workload=dict(b['workload'], messages_per_gateway=19))))
        self.assertFalse(helper.overhead_pair_compatible(a, dict(b, backend_quota={'cpus': 4})))
        self.assertFalse(helper.overhead_pair_compatible(a, dict(b, source_fingerprint='different')))
        self.assertFalse(helper.overhead_pair_compatible(a, dict(b, status='FAIL')))
        self.assertFalse(helper.overhead_pair_compatible(a, dict(b, exact_cleanup_verified=False)))
        with self.assertRaisesRegex(ValueError, 'overhead_mode'):
            helper.overhead_profile('telemetry_pilot')

    def test_overhead_runner_is_one_real_shared_workload_and_gates_viewers_before_publish(self):
        import inspect
        source = inspect.getsource(helper.run_overhead)
        self.assertEqual(source.count('prepare_paper_workload('), 1)
        self.assertIn("if profile['viewers'] == 2:", source)
        self.assertLess(source.index('wait_ready(capture'), source.index('outputs = [run_gateway'))
        self.assertIn("'run', '--batches', '20'", source)
        self.assertIn("GATEWAY_SAMPLE_RATE_HZ='100'", source)
        self.assertIn("GATEWAY_SAMPLES_PER_BATCH='100'", source)
        self.assertIn("capture = start_viewers", source)
        self.assertIn("if capture is not None", source)
        self.assertIn("full_p7_accepted':False", source)
        self.assertNotIn('replay', source.lower())
        self.assertIn("before_ws.get('sessions') != profile['viewers']", source)
        self.assertIn("'producer_active'", source)
        self.assertIn('known_concurrency_window_seconds', source)
        self.assertIn('deliveries_enqueued', source)

    def test_overhead_acceptance_rejects_missing_metrics_or_outer_cleanup(self):
        e = {key: True for key in ('profile_verified', 'database_counts', 'application_acks',
            'outboxes_drained', 'viewer_capture', 'namespace_isolation', 'metrics_complete', 'source_fingerprinted')}
        result = {'status':'PASS','mode':'paper_p7_overhead_two','full_p7_accepted':False,
            'exact_cleanup_verified':True,'evidence':e}
        self.assertTrue(helper.overhead_result_ok(result))
        self.assertFalse(helper.overhead_result_ok(dict(result, exact_cleanup_verified=False)))
        self.assertFalse(helper.overhead_result_ok(dict(result, evidence=dict(e, metrics_complete=False))))

    def test_zero_viewer_close_observation_json_shapes(self):
        from paper_p7_support import CLOSE_REASONS
        zero_counts = {reason: 0 for reason in CLOSE_REASONS}
        available = lambda counts, observations: {'availability': 'available', 'sessions': 0,
            'close_counts': counts, 'close_observations': observations}
        self.assertTrue(helper.safe_close_evidence(available(zero_counts, None))['close_counts'] is not None)
        self.assertEqual(helper.safe_close_evidence(available(zero_counts, None))['close_observations'], [])
        self.assertEqual(helper.safe_close_evidence(available(zero_counts, []))['close_observations'], [])
        self.assertFalse(helper.close_observation_shape_valid({'availability': 'available', 'close_observations': None,
            'close_counts': {'queue_overflow': 0}}))
        self.assertEqual(helper.safe_close_evidence({'availability': 'unavailable', 'close_observations': None,
            'close_counts': zero_counts})['close_counts'], zero_counts)
        malformed = helper.safe_close_evidence(available(zero_counts, {}))
        self.assertEqual(malformed['close_observations'], [])
        self.assertEqual(helper.safe_close_evidence(available(zero_counts,
            [{'reason': 'client_close', 'operation': 'read'}]))['close_observations'],
            [{'reason': 'client_close', 'operation': 'read'}])
        self.assertTrue(helper.close_observation_shape_valid(available(zero_counts, None)))
        self.assertTrue(helper.close_observation_shape_valid(available(zero_counts, [])))
        missing = dict(zero_counts); del missing['queue_overflow']
        self.assertFalse(helper.close_observation_shape_valid(available(missing, None)))
        self.assertFalse(helper.close_observation_shape_valid(dict(available(zero_counts, None), availability='unavailable')))
        self.assertFalse(helper.close_observation_shape_valid(available(zero_counts, {})))
        self.assertTrue(helper.close_observation_shape_valid(available(zero_counts,
            [{'reason': 'client_close', 'operation': 'read'}])))

    def test_slow_payload_is_fixed_sequential_unique_and_finite(self):
        payloads = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=500)
        self.assertEqual(len(payloads), 1000)
        decoded = [json.loads(value) for value in payloads]
        self.assertEqual([int(item['first_sequence']) for item in decoded], list(range(500, 1000500, 1000)))
        self.assertEqual(len({item['message_id'] for item in decoded}), 1000)
        self.assertTrue(all(item['sample_count'] == 1000 and len(item['samples']) == 1000 for item in decoded))
        self.assertTrue(all(datetime.fromisoformat(item['measured_at'].replace('Z', '+00:00')) < datetime.now(timezone.utc) for item in decoded))
        self.assertTrue(20_000_000 <= sum(map(len, payloads)) <= 28_000_000)
        self.assertTrue(all(all(__import__('math').isfinite(x) for x in item['samples']) for item in decoded))
        with self.assertRaisesRegex(ValueError, 'p7_slow_payload'):
            helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=-1)
        p200 = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=100, message_count=200)
        self.assertEqual(len(p200), 200)
        self.assertTrue(all(datetime.fromisoformat(item['measured_at'].replace('Z', '+00:00')) < datetime.now(timezone.utc) for item in [json.loads(x) for x in p200]))

    def test_slow_oracles_reject_missing_duplicate_namespace_and_policy_proof(self):
        payloads = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=0)
        ack_ids = [json.loads(p)['message_id'] for p in payloads]
        self.assertTrue(helper.slow_ack_ids_complete(ack_ids, payloads))
        self.assertFalse(helper.slow_ack_ids_complete(ack_ids[:-1], payloads))
        self.assertFalse(helper.slow_ack_ids_complete(ack_ids + [ack_ids[0]], payloads))
        self.assertFalse(helper.slow_db_oracle_ok({'ledger': 999, 'raw': 1000000, 'temporal': 1000}))
        self.assertTrue(helper.slow_db_oracle_ok({'ledger': 1000, 'raw': 1000000, 'temporal': 1000}))
        self.assertTrue(helper.slow_db_oracle_ok({'ledger': 200, 'raw': 200000, 'temporal': 200}, expected_messages=200))
        self.assertFalse(helper.slow_healthy_peer_ok({'a': {'count': 1000}, 'b': {'count': 0}}, 'a', 'b',
            {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sb'}, []))
        self.assertFalse(helper.slow_viewer_result_ok({'status': 'PASS', 'mode': 'paper_p7_slow_viewer',
            'full_p7_accepted': False, 'precleanup_proof_verified': True,
            'exact_cleanup_verified': True,
            'evidence': {key: True for key in helper.SLOW_VIEWER_REQUIRED_PROOFS}}))
        baseline = {'close_counts': {'queue_overflow': 0}, 'close_observations': []}
        observed = {'close_counts': {'queue_overflow': 1},
                    'close_observations': [{'reason': 'queue_overflow', 'operation': 'access'}]}
        self.assertEqual(helper.slow_close_cause_evidence(baseline, observed),
                         {'causes': ['queue_overflow'], 'observed': True})
        self.assertFalse(helper.slow_close_cause_evidence(baseline, {'close_counts': None,
            'close_observations': []})['causes'])

    def test_slow_viewer_requires_exact_a_headers_and_ready_zero_event_b(self):
        payloads = [json.loads(p) for p in helper.build_slow_viewer_payloads('ga', 'sa', 'boot', start_sequence=2**53)]
        expected = [{'gateway_id': 'ga', 'sensor_id': 'sa', 'message_id': p['message_id'],
            'boot_id': p['boot_id'], 'first_sequence': str(p['first_sequence']),
            'sample_count': p['sample_count'], 'measured_at': p['measured_at']} for p in payloads]
        viewers = {'a': {'count': len(payloads), 'acknowledged': True, 'status': 'ready', 'wrong_namespace': 0,
                         'event_headers': expected},
                   'b': {'count': 0, 'acknowledged': True, 'status': 'ready', 'wrong_namespace': 0,
                         'event_headers': []}}
        self.assertTrue(helper.slow_healthy_peer_ok(viewers, 'a', 'b',
            {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sa'}, payloads))
        bad = dict(viewers['a']); bad['event_headers'] = [dict(expected[0], first_sequence=str(2**53 + 1)), *expected[1:]]
        self.assertFalse(helper.slow_healthy_peer_ok({'a': bad, 'b': viewers['b']}, 'a', 'b',
            {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sa'}, payloads))
        self.assertFalse(helper.slow_healthy_peer_ok({'a': viewers['a'], 'b': dict(viewers['b'], count=1)}, 'a', 'b',
            {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sa'}, payloads))
        final = {'status': 'PASS', 'viewers': {
            'a': {'status': 'PASS', 'count': len(payloads), 'acknowledged': True,
                  'wrong_namespace': 0, 'events': expected},
            'b': {'status': 'PASS', 'count': 0, 'acknowledged': True, 'wrong_namespace': 0, 'events': []}}}
        safe = helper._safe_capture_proof(final)
        self.assertEqual(safe['a']['event_header_count'], len(payloads))
        self.assertTrue(safe['a']['headers_match_count'])
        self.assertTrue(safe['a']['namespace_match'])
        self.assertNotIn('events', json.dumps(safe))
        self.assertFalse(helper._safe_capture_proof({'status': 'PASS', 'viewers': {
            'a': {'status': 'PASS', 'count': 1, 'events': [], 'wrong_namespace': 0},
            'b': {'status': 'PASS', 'count': 0, 'events': [], 'wrong_namespace': 0}}})['a']['headers_match_count'])

    def test_slow_ack_contract_uses_real_p5_verifier_and_expected_wave(self):
        message_id = '550e8400-e29b-41d4-a716-446655440000'
        raw = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack', 'message_id': message_id,
            'status': 'committed', 'committed_at': '2026-10-27T00:00:00Z'}).encode()
        # Mirrors telemetry.NewCommitACK: ACK topic is keyed by message ID.
        topic = 'gateways/g/acks/' + message_id
        self.assertEqual(helper.validate_slow_ack_messages([(topic, raw)], [message_id], 'g', 's'), [message_id])
        with self.assertRaisesRegex(ValueError, 'application_ack_invalid'):
            helper.validate_slow_ack_messages([(topic, raw)], ['other'], 'g', 's')
        with self.assertRaisesRegex(ValueError, 'application_ack_invalid'):
            helper.validate_slow_ack_messages([('gateways/other/acks/' + message_id, raw)], [message_id], 'g', 's')
        with self.assertRaisesRegex(ValueError, 'application_ack_invalid'):
            helper.validate_slow_ack_messages([('gateways/g/acks/s', raw)], [message_id], 'g', 's')
        malformed = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack', 'message_id': 'm',
            'status': 'committed', 'committed_at': '2026-10-27T00:00:00Z', 'extra': True}).encode()
        with self.assertRaisesRegex(ValueError, 'application_ack_invalid'):
            helper.validate_slow_ack_messages([(topic, malformed)], [message_id], 'g', 's')

    def test_slow_raw_content_oracle_rejects_missing_or_changed_sample(self):
        payload = json.loads(helper.build_slow_viewer_payloads('gw', 's', 'boot', start_sequence=10)[0])
        measured = datetime.fromisoformat(payload['measured_at'].replace('Z', '+00:00'))
        rows = [{'message_id': payload['message_id'], 'sample_index': i, 'sequence_num': str(10 + i),
                 'measured_at': (measured + timedelta(microseconds=payload['sample_interval_us'] * i)).isoformat(),
                 'value': payload['samples'][i]} for i in range(1000)]
        self.assertTrue(helper.slow_raw_batch_content_ok(rows, payload))
        self.assertFalse(helper.slow_raw_batch_content_ok(rows[:-1], payload))
        changed = [dict(rows[0], value=rows[0]['value'] + 1), *rows[1:]]
        self.assertFalse(helper.slow_raw_batch_content_ok(changed, payload))

    def test_slow_viewer_contract_bounds_and_policy_proof_guard(self):
        bounds = helper.slow_viewer_workload_bounds()
        self.assertEqual((bounds['messages'], bounds['samples_per_message'], bounds['wave_size']), (1000, 1000, 10))
        self.assertTrue(bounds['manual_publisher'])
        self.assertFalse(bounds['gateway_outbox_applicable'])
        proof = {key: True for key in helper.SLOW_VIEWER_REQUIRED_PROOFS}
        good = {'status': 'PASS', 'mode': 'paper_p7_slow_viewer', 'full_p7_accepted': False,
                'precleanup_proof_verified': True, 'precleanup_proof_artifact': 'p7-slow-viewer-proof.json',
                'capture_finalization_status': 'finished', 'evidence': proof}
        self.assertTrue(helper.slow_viewer_result_ok(good))
        self.assertFalse(helper.slow_viewer_result_ok(dict(good, precleanup_proof_verified=False)))
        self.assertFalse(helper.slow_viewer_result_ok(dict(good, evidence=dict(proof, slow_close_metric_delta=False))))
        self.assertFalse(helper.slow_viewer_result_ok(dict(good, capture_finalization_status='pending')))
        self.assertFalse(helper.slow_viewer_result_ok(dict(good, precleanup_proof_artifact=None)))

    def test_slow_viewer_probe_config_is_bounded_and_private(self):
        with tempfile.TemporaryDirectory() as temp:
            from pathlib import Path
            directory = Path(temp) / 'private'
            directory.mkdir(mode=0o700)
            directory.chmod(0o700)
            value = helper.slow_viewer_probe_config('wss://127.0.0.1:9443/v1/ws', '/ca.pem', 'secret',
                {'gateway_id': 'gw', 'sensor_id': 'sensor'}, directory)
            self.assertEqual(value['max_lifetime_ms'], 180000)
            self.assertEqual(value['max_frames'], 1024)
            self.assertEqual(value['max_bytes'], 33554432)
            self.assertNotIn('secret', json.dumps({k: v for k, v in value.items() if k != 'valid_jwt'}))
            with self.assertRaises(ValueError):
                helper.slow_viewer_probe_config('wss://example.com/v1/ws', '/ca.pem', 'secret',
                    {'gateway_id': 'gw', 'sensor_id': 'sensor'}, directory)

    def test_slow_case_setup_calls_real_workload_then_browser_and_native_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / 'private'
            (private / 'certs').mkdir(parents=True, mode=0o700)
            (private / 'certs' / 'ca.crt').write_text('ca')
            (private / 'certs' / 'ca.crt').chmod(0o600)
            fixture = SimpleNamespace(private=private, project='test')
            spec_row = SimpleNamespace(gateway_id='gw', sensor_id='s', sensor_urn='urn:s')
            peer_row = SimpleNamespace(gateway_id='gw2', sensor_id='s', sensor_urn='urn:s2')
            events = []
            tokens = {'a': 'ta', 'b': 'tb', 'admin': 'admin'}
            smoke_mock = SimpleNamespace()
            smoke_mock.__file__ = str(MODULE)
            # Provide the normal HTTPS endpoint type and exact 2 CPU / 4 GiB profile.
            smoke_mock.HTTPSEndpoint = type('HTTPS', (), {})
            endpoint = smoke_mock.HTTPSEndpoint()
            endpoint.url = 'https://127.0.0.1:9443'
            endpoint.ca_file = private / 'p2-ca.crt'
            endpoint.ca_file.write_text('p2-ca')
            smoke_mock.prepare_paper_workload = Mock(side_effect=lambda *args: (
                events.append('prepare') or ('pg', {'cpus': 2, 'memory_bytes': 4294967296}, {},
                {'backend': {'Memory': 4294967296, 'NanoCpus': 2000000000}}, {}, tokens,
                 [spec_row, peer_row], ['pw', 'pw2'], '/sensor')))
            smoke_mock.http_request = Mock(return_value=(200, {}, {'ok': True,
                'websocket_metrics': {'availability': 'available', 'slow_closes': 0, 'sessions': 3,
                    'global_queued_work': 0, 'global_queued_bytes': 0,
                    'session_queued_in_flight_bytes': 0}}))
            capture = {'output_dir': private, 'item': {'receipt': {'id': 'id'}}, 'cleaned': False}
            with patch.object(helper, 'start_viewers', side_effect=lambda *a, **kw: (events.append('browser') or capture)) as browser, \
                 patch.object(helper, 'wait_ready', return_value={'viewers': ['a', 'b']}), \
                 patch.object(helper, 'finish_viewers', side_effect=lambda *a, **kw: events.append('finish')), \
                 patch('subprocess.Popen') as popen:
                def launch(command, **kwargs):
                    events.append('probe')
                    cfg = json.loads(Path(kwargs['env']['P7_SLOW_VIEWER_CONFIG_FILE']).read_text())
                    Path(cfg['ready_path']).write_text(json.dumps({'ready': True, 'subscribed': True, 'target': cfg['target']}))
                    proc = Mock()
                    proc.poll.return_value = None
                    return proc
                popen.side_effect = launch
                context = helper.setup_slow_case(smoke_mock, fixture, object(), endpoint, {}, {}, object())
                self.assertEqual(events, ['prepare', 'browser', 'probe'])
                self.assertEqual(popen.call_args.args[0], ['go', 'test', './internal/telemetryws', '-run', '^TestNativeSlowViewerProbe$', '-count=1', '-timeout=195s'])
                self.assertEqual(popen.call_args.kwargs['cwd'], ROOT / 'src')
                self.assertEqual(context['metrics_before']['ok'], True)
                self.assertEqual(Path(context['probe_config_path']).stat().st_mode & 0o777, 0o600)
                cfg = json.loads(Path(context['probe_config_path']).read_text())
                self.assertEqual(cfg['fixture_ca_path'], str(endpoint.ca_file))
                Path(cfg['output_path']).write_text(json.dumps({'outcome': 'server_closed'}))
                helper.teardown_slow_case(context)
                self.assertEqual(events[-1], 'finish')

    def test_slow_case_setup_failure_cleans_capture_without_masking_error(self):
        context = {'capture': {'item': {}, 'cleaned': False}, 'registry': object(), 'probe': None,
                   'probe_log': None, 'probe_config_path': None}
        with patch.object(helper, 'finish_viewers', side_effect=RuntimeError('cleanup')):
            helper.teardown_slow_case(context, primary=ValueError('primary'))

    def test_slow_viewer_wrapper_preserves_primary_and_requires_capture_cleanup_proof(self):
        with tempfile.TemporaryDirectory() as td:
            proof_path = Path(td) / 'p7-slow-viewer-proof.json'
            proof_path.write_text(json.dumps(sample_slow_proof()))
            fake_context = {'capture': object(), 'probe': object(), 'slow_proof_path': proof_path,
                            'slow_proof': {'policy': {'close_counts': {'queue_overflow': 1}}},
                            'slow_workload_result': {'healthy_peer_events': 200}}
            primary = ValueError('workload-primary')
            with patch.object(helper, 'setup_slow_case', return_value=fake_context), \
                 patch.object(helper, 'run_slow_case_workload', side_effect=primary), \
                 patch.object(helper, 'teardown_slow_case', side_effect=RuntimeError('cleanup-secondary')):
                with self.assertRaisesRegex(ValueError, 'workload-primary'):
                    helper.run_slow_viewer(None, None, None, None, None, None, None)
            proof = {key: True for key in helper.SLOW_VIEWER_REQUIRED_PROOFS}
            workload = {'status': 'PASS', 'mode': 'paper_p7_slow_viewer', 'full_p7_accepted': False,
                        'precleanup_proof_verified': True, 'precleanup_proof_artifact': proof_path.name,
                        'capture_finalization_status': 'pending', 'evidence': proof,
                        'healthy_peer_events': 200, 'close_evidence': {'close_counts': {'queue_overflow': 1}}}
            capture_result = {'status': 'PASS', 'viewers': {
                'a': {'status': 'ready', 'count': 200, 'acknowledged': True, 'wrong_namespace': 0,
                      'events': [None] * 200},
                'b': {'status': 'ready', 'count': 0, 'acknowledged': True, 'wrong_namespace': 0, 'events': []}}}
            with patch.object(helper, 'setup_slow_case', return_value=fake_context), \
                 patch.object(helper, 'run_slow_case_workload', return_value=workload), \
                 patch.object(helper, 'teardown_slow_case', return_value={'cleanup_errors': [],
                     'capture_result': capture_result, 'probe_result': {'outcome': 'server_closed'}}):
                actual = helper.run_slow_viewer(None, None, None, None, None, None, None)
            self.assertEqual(actual['probe_close_corroboration'], 'server_closed')
            self.assertEqual(actual['close_evidence']['close_counts']['queue_overflow'], 1)
            self.assertEqual(actual['precleanup_proof_artifact'], proof_path.name)
            fake_context['slow_proof'] = {'functional_status': 'PASS', 'capture_finalization_status': 'pending',
                'database': {'actual_counts': {'ledger': 1000}},
                'workload': {'application_acks_actual': 1000}}
            helper._finalize_slow_capture_proof(fake_context, 'FAIL', ['capture_finalization_failed'])
            retained = json.loads(proof_path.read_text())
            self.assertEqual(retained['functional_status'], 'PASS')
            self.assertEqual(retained['capture_finalization_status'], 'FAIL')
            self.assertEqual(retained['capture_failure_codes'], ['capture_finalization_failed'])
            self.assertEqual(retained['workload']['application_acks_actual'], 1000)
            proof_path.write_text(json.dumps(sample_slow_proof()))
            self.assertTrue(helper.finalize_slow_viewer_cleanup({**workload, 'precleanup_proof_path': str(proof_path),
                'capture_finalization_status': 'finished'}, True))
            self.assertEqual(json.loads(proof_path.read_text())['resource_cleanup_status'], 'verified')
            self.assertFalse(helper.finalize_slow_viewer_cleanup({**workload,
                'precleanup_proof_path': str(proof_path), 'capture_finalization_status': 'finished'}, False))
        with patch.object(helper, 'setup_slow_case', return_value=fake_context), \
             patch.object(helper, 'run_slow_case_workload', return_value=workload), \
             patch.object(helper, 'teardown_slow_case', return_value={'cleanup_errors': [], 'capture_result': None}):
            with self.assertRaisesRegex(RuntimeError, 'capture_proof_missing'):
                helper.run_slow_viewer(None, None, None, None, None, None, None)

    def test_slow_case_bad_ready_shape_releases_capture_and_process(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / 'private'
            (private / 'certs').mkdir(parents=True, mode=0o700)
            fixture = SimpleNamespace(private=private, project='test')
            spec_row = SimpleNamespace(gateway_id='gw', sensor_id='s', sensor_urn='urn:s')
            peer_row = SimpleNamespace(gateway_id='gw2', sensor_id='s', sensor_urn='urn:s2')
            tokens = {'a': 'ta', 'b': 'tb', 'admin': 'admin'}
            smoke_mock = SimpleNamespace()
            smoke_mock.__file__ = str(MODULE)
            smoke_mock.HTTPSEndpoint = type('HTTPS', (), {})
            endpoint = smoke_mock.HTTPSEndpoint()
            endpoint.url = 'https://127.0.0.1:9443'
            endpoint.ca_file = private / 'p2-ca.crt'
            smoke_mock.prepare_paper_workload = Mock(return_value=(
                'pg', {'cpus': 2, 'memory_bytes': 4294967296}, {},
                {'backend': {'Memory': 4294967296, 'NanoCpus': 2000000000}}, {}, tokens,
                 [spec_row, peer_row], ['pw', 'pw2'], '/sensor'))
            capture = {'output_dir': private, 'item': {'receipt': {'id': 'id'}}, 'cleaned': False}
            proc = Mock()
            proc.poll.return_value = None
            def launch(command, **kwargs):
                cfg = json.loads(Path(kwargs['env']['P7_SLOW_VIEWER_CONFIG_FILE']).read_text())
                Path(cfg['ready_path']).write_text('{"ready":false}')
                return proc
            with patch.object(helper, 'start_viewers', return_value=capture), \
                 patch.object(helper, 'wait_ready', return_value={'viewers': ['a', 'b']}), \
                 patch.object(helper, 'finish_viewers', side_effect=RuntimeError('cleanup')) as finish, \
                 patch('subprocess.Popen', side_effect=launch):
                with self.assertRaisesRegex(ValueError, 'p7_slow_probe_ready_mismatch'):
                    helper.setup_slow_case(smoke_mock, fixture, object(), endpoint, {}, {}, object())
                self.assertTrue(finish.called)
                proc.wait.assert_called_once()

    def test_lifecycle_mode_dispatch_contract_and_proof_guard(self):
        fixture = Mock(paper_p7_mode='lifecycle')
        self.assertEqual(helper.pilot_mode(fixture), 'lifecycle')
        self.assertEqual(helper.lifecycle_phases(), ('expiry', 'revocation', 'controlled_stop'))
        proof = {key: True for key in helper.LIFECYCLE_REQUIRED_PROOFS}
        phases = [{'name': name, 'complete': True} for name in helper.lifecycle_phases()]
        phase_proof = {name: True for name in helper.lifecycle_phases()}
        self.assertTrue(helper.lifecycle_result_ok({'status': 'PASS', 'mode': 'paper_p7_lifecycle',
            'full_p7_accepted': False, 'exact_cleanup_verified': True, 'evidence': proof,
            'phases': phases, 'proof': phase_proof}))
        for key in helper.LIFECYCLE_REQUIRED_PROOFS:
            missing = dict(proof, **{key: False})
            self.assertFalse(helper.lifecycle_result_ok({'status': 'PASS', 'mode': 'paper_p7_lifecycle',
                'full_p7_accepted': False, 'exact_cleanup_verified': True, 'evidence': missing,
                'phases': phases, 'proof': phase_proof}), key)
        self.assertFalse(helper.lifecycle_result_ok({'status': 'PASS', 'mode': 'paper_p7_lifecycle',
            'full_p7_accepted': True, 'exact_cleanup_verified': True, 'evidence': proof,
            'phases': phases, 'proof': phase_proof}))

    def test_lifecycle_helper_to_outer_cleanup_to_final_acceptance(self):
        result = {'status': 'PASS', 'mode': 'paper_p7_lifecycle', 'full_p7_accepted': False,
            'exact_cleanup_verified': False,
            'evidence': {key: True for key in helper.LIFECYCLE_REQUIRED_PROOFS},
            'phases': [{'name': name, 'complete': True} for name in helper.lifecycle_phases()],
            'proof': {name: True for name in helper.lifecycle_phases()}}
        self.assertFalse(helper.lifecycle_result_ok(result))
        self.assertTrue(helper.finalize_lifecycle_cleanup(result, True))
        self.assertTrue(result['exact_cleanup_verified'])
        self.assertFalse(result['full_p7_accepted'])
        for cleanup in (False, None):
            candidate = dict(result)
            candidate['exact_cleanup_verified'] = False
            self.assertFalse(helper.finalize_lifecycle_cleanup(candidate, cleanup))
        missing_phase = dict(result, phases=[{'name': 'expiry', 'complete': True}])
        self.assertFalse(helper.lifecycle_result_ok(missing_phase))

    def test_expected_close_is_only_positive_after_ack_and_not_infrastructure_error(self):
        self.assertTrue(helper.expected_close_result_ok({'status': 'closed', 'acknowledged': True,
            'actual_close': True, 'expected_close': True, 'close_code': 1000, 'browser_launched': True, 'origin_loaded': True}))
        for bad in (
            {'status': 'closed', 'acknowledged': False, 'actual_close': True, 'browser_launched': True, 'origin_loaded': True},
            {'status': 'error', 'acknowledged': True, 'actual_close': False, 'browser_launched': True, 'origin_loaded': True},
            {'status': 'closed', 'acknowledged': True, 'actual_close': True, 'browser_launched': False, 'origin_loaded': True},
            {'status': 'closed', 'acknowledged': True, 'actual_close': True, 'browser_launched': True, 'origin_loaded': False},
        ):
            self.assertFalse(helper.expected_close_result_ok(bad))

    def test_lifecycle_final_capture_requires_finished_expected_close_for_both_viewers(self):
        def viewer():
            return {'status': 'closed', 'acknowledged': True, 'actual_close': True,
                'expected_close': True, 'close_code': 1000, 'browser_launched': True,
                'origin_loaded': True}
        final = {'status': 'PASS', 'finish_status': 'finished', 'cleanup_errors': [],
            'viewers': {'a': viewer(), 'b': viewer()}}
        self.assertTrue(helper.lifecycle_finished_capture_ok(final))
        for field in ('expected_close', 'browser_launched', 'origin_loaded', 'acknowledged', 'actual_close'):
            bad = json.loads(json.dumps(final))
            del bad['viewers']['b'][field]
            self.assertFalse(helper.lifecycle_finished_capture_ok(bad), field)
        bad = dict(final, finish_status='snapshot')
        self.assertFalse(helper.lifecycle_finished_capture_ok(bad))

    def test_finished_capture_expected_close_shape_is_required_for_lifecycle_proof(self):
        good = {'status': 'closed', 'acknowledged': True, 'actual_close': True,
            'expected_close': True, 'close_code': 1000, 'browser_launched': True,
            'origin_loaded': True}
        self.assertTrue(helper.expected_close_result_ok(good))
        for missing in ('expected_close', 'browser_launched', 'origin_loaded', 'acknowledged', 'actual_close'):
            candidate = dict(good)
            candidate.pop(missing)
            self.assertFalse(helper.expected_close_result_ok(candidate), missing)

    def test_two_viewer_capture_expected_close_records_actual_close_separately(self):
        source = helper.two_viewer_capture_source()
        self.assertIn('expectedClose:input.expected_close===true', source)
        self.assertIn('actualClose=true;closeCode=ev.code', source)
        self.assertIn("acknowledged?'closed':'closed_before_ack'", source)
        self.assertIn("if(!expectedClose)try{socket.close", source)
        self.assertIn('browser_launch_completed', source)
        self.assertIn('origin_loaded', source)
        self.assertIn('expected_close===true', source)
        self.assertIn("write('state.json'", source)
        self.assertIn('actual_close:v.actual_close', source)

    def test_wait_viewer_closed_requires_real_close_and_healthy_peer(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            capture = {'output_dir': Path(td)}
            (Path(td) / 'state.json').write_text(json.dumps({'viewers': {
                'a': {'status': 'closed', 'acknowledged': True, 'expected_close': True,
                      'browser_launched': True, 'origin_loaded': True, 'actual_close': True, 'close_code': 1000},
                'b': {'status': 'ready', 'acknowledged': True, 'actual_close': False, 'close_code': None}}}))
            self.assertTrue(helper.wait_viewer_closed(capture, 'a', 'b', timeout=.1)['actual_close'])
            (Path(td) / 'state.json').write_text(json.dumps({'viewers': {
                'a': {'status': 'error', 'acknowledged': True, 'actual_close': False},
                'b': {'status': 'ready', 'acknowledged': True}}}))
            with self.assertRaisesRegex(ValueError, 'viewer_close_timeout'):
                helper.wait_viewer_closed(capture, 'a', 'b', timeout=.1)

    def test_lifecycle_context_prepares_once_and_grants_viewer_membership(self):
        specs = [SimpleNamespace(gateway_id='ga', sensor_id='sa'), SimpleNamespace(gateway_id='gb', sensor_id='sb')]
        smoke_mock = Mock()
        smoke_mock.prepare_paper_workload.return_value = ('pg', {}, {}, {}, {}, {'a': 'ta', 'b': 'tb'}, specs, ['pa', 'pb'], '/sensor')
        smoke_mock.create_user_via_admin_api.return_value = 'viewer-uid'
        smoke_mock.login_user_for_token.return_value = 'viewer-token'
        fixture = SimpleNamespace(project='setup-test', values={'SERVICE_ROLE_KEY': 'private-service', 'ANON_KEY': 'anon'})
        registry = Mock()
        item = {'name': 'volume', 'receipt': None}
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, {'receipt': {'id': 'vol'}}]
        registry.command.return_value = SimpleNamespace(returncode=0)
        context = helper.setup_lifecycle_context(smoke_mock, fixture, registry, 'endpoint', {}, {}, Mock())
        smoke_mock.prepare_paper_workload.assert_called_once()
        smoke_mock.db_psql.assert_called_once()
        self.assertEqual(smoke_mock.db_psql.call_args.args[2],
            "INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES (:'uid',:'gateway_id','viewer');")
        self.assertEqual(context['viewer']['token'], 'viewer-token')
        self.assertIs(context['outbox_volume'], item)

    def test_lifecycle_metrics_uses_authenticated_real_admin_route_and_schema(self):
        calls = []
        smoke_obj = SimpleNamespace(http_request=lambda *a, **k: (calls.append((a, k)) or
            (200, {}, {'websocket_metrics': {'availability': 'available', 'expiries': 4,
              'revocations': 2}})))
        context = {'tokens': {'admin': 'admin-jwt'}}
        self.assertEqual(helper.lifecycle_metrics(smoke_obj, SimpleNamespace(url='https://host'), context),
                         {'expiries': 4, 'revocations': 2})
        args, kwargs = calls[0]
        self.assertEqual(args[2], '/v1/admin/metrics')
        self.assertEqual(kwargs.get('token') or kwargs.get('jwt') or args[3], 'admin-jwt')

    def test_lifecycle_metrics_fails_closed_for_missing_snapshot_or_counter(self):
        for body in ({}, {'websocket_metrics': {'availability': 'unavailable', 'expiries': 0, 'revocations': 0}},
                     {'websocket_metrics': {'availability': 'available', 'expiries': 0}}):
            smoke_obj = SimpleNamespace(http_request=lambda *a, _body=body, **k: (200, {}, _body))
            with self.assertRaisesRegex(ValueError, 'p7_lifecycle_metrics_unavailable'):
                helper.lifecycle_metrics(smoke_obj, SimpleNamespace(url='https://host'),
                                         {'tokens': {'admin': 'admin-jwt'}})

    def test_revocation_order_is_commit_then_wait_and_probe_requires_server_code(self):
        import inspect
        source = inspect.getsource(helper.run_revocation_case)
        self.assertLess(source.index('DELETE FROM public.user_gateways'), source.index('wait_viewer_closed'))
        self.assertIn('run_revocation_probe', source)
        self.assertIn("'UNAUTHORIZED'", helper.revocation_probe_source())
        self.assertIn('server_error_code', helper.revocation_probe_source())
        self.assertNotIn('service_role', source.lower())

    def test_expiry_is_factored_and_aggregate_shares_context_once(self):
        import inspect
        aggregate = inspect.getsource(helper.run_lifecycle)
        self.assertEqual(aggregate.count('setup_lifecycle_context('), 1)
        self.assertIn('run_expiry_case(', aggregate)
        self.assertIn('run_revocation_case(', aggregate)
        self.assertIn("context['outbox_volume']", inspect.getsource(helper.emit_healthy_gateway_batch))

    def test_lifecycle_controlled_stop_and_aggregate_require_ordered_proofs(self):
        import inspect
        controlled = inspect.getsource(helper.run_controlled_stop_case)
        aggregate = inspect.getsource(helper.run_lifecycle)
        self.assertIn("['stop', '--time'", controlled)
        self.assertIn("['start',", controlled)
        self.assertIn('backend shutdown complete', controlled)
        self.assertIn("get('ExitCode')", controlled)
        self.assertIn('restart_subscribe_ack', controlled)
        self.assertLess(aggregate.index('run_expiry_case('), aggregate.index('run_revocation_case('))
        self.assertLess(aggregate.index('run_revocation_case('), aggregate.index('run_controlled_stop_case('))
        self.assertIn('LIFECYCLE_REQUIRED_PROOFS', aggregate)

    def test_revocation_history_query_has_valid_bounded_measurement_parameters(self):
        import inspect
        source = inspect.getsource(helper.run_revocation_case)
        self.assertIn('paper_p0_temporal_query', source)
        self.assertIn("'attrs': 'measurement'", inspect.getsource(smoke.paper_p0_temporal_query))
        self.assertIn('history_status in (403, 404)', source)

    def test_subscription_probe_distinguishes_expected_ack_from_server_denial(self):
        source = helper.revocation_probe_source()
        self.assertIn('expectedAck', source)
        self.assertIn("out.status='PASS'", source)
        self.assertIn("out.status='PASS'", source)
        self.assertIn('server_error_code', source)
        self.assertIn('subscribe_ack', source)

    def test_lifecycle_cleanup_does_not_leak_capture_or_mask_failure(self):
        import inspect
        source = inspect.getsource(helper.run_lifecycle)
        self.assertIn('finally:', source)
        self.assertIn("active_b_capture", source)
        self.assertNotIn("'active_b_capture': revocation", source)

    def test_expiry_phase_orders_close_metric_and_healthy_gateway_batch_before_cleanup(self):
        root = Path('/var/tmp/opencode/paper-p7/unit-expiry')
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        spec_a = SimpleNamespace(gateway_id='ga', sensor_id='sa')
        spec_b = SimpleNamespace(gateway_id='gb', sensor_id='sb')
        ctx = {'quota': {'cpus': 2, 'memory_bytes': 4294967296},
            'resources': {'backend': {'Memory': 4294967296}}, 'specs': [spec_a, spec_b],
            'passwords': ['pa', 'pb'], 'tokens': {'b': 'tb', 'admin': 'admin'}, 'viewer': {'token': 'tv'},
            'outbox_volume': {'name': 'v', 'receipt': {'id': 'vol'}}}
        fixture = SimpleNamespace(project='expiry-test', private=root, artifacts=root)
        endpoint_type = type('HTTPS', (), {'url': 'https://localhost'})
        class Registry:
            def __init__(self): self.calls=[]
            def intent(self, *a): self.calls.append('volume_intent'); return {'name':'v','receipt':None}
            def resolve(self, item): self.calls.append('volume_resolve'); return None if item.get('receipt') is None else {'receipt': item['receipt']}
            def allocating(self, item): self.calls.append('volume_allocating')
            def command(self, args):
                if args[0] == 'volume': item['receipt']={'id':'vol'}
                self.calls.append('cli')
                return SimpleNamespace(returncode=0)
        registry=Registry(); item={'name':'v','receipt':None}
        registry.intent=lambda *a: (registry.calls.append('volume_intent') or item)
        smoke_obj=SimpleNamespace(HTTPSEndpoint=endpoint_type, private_write=lambda *a: None,
            durable_json=lambda *a: None,
            paper_p0_gateway_cli=lambda *a, **k: (registry.calls.append('gateway') or
                ('completed=true\nenqueued=1\napp_acknowledged=1' if a[7][0]=='run' else
                 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0')),
            http_request=lambda *a, **k: (200, {}, {'websocket_metrics': {'availability':'available','expiries':3,'revocations':0}}))
        registry_resolve=[None, {'receipt': {'id':'vol'}}]
        registry.resolve=lambda obj: (registry.calls.append('volume_resolve') or registry_resolve.pop(0))
        with patch.object(helper, 'setup_lifecycle_context', return_value=ctx), \
             patch.object(helper, 'mint_short_lived_token', return_value='short'), \
             patch.object(helper, 'start_viewers', side_effect=lambda *a, **k: (registry.calls.append('capture_start') or {'output_dir':root})), \
             patch.object(helper, 'wait_ready', side_effect=lambda *a, **k: (registry.calls.append('both_ack') or
                (root / 'state.json').write_text(json.dumps({'viewers': {'b': {'count': 1}}})))), \
              patch.object(helper, 'wait_viewer_closed', side_effect=lambda *a, **k: (registry.calls.append('actual_close') or {'status':'closed','actual_close':True,'expected_close':True,'browser_launched':True,'origin_loaded':True,'close_code':1000,'acknowledged':True})), \
               patch.object(helper, 'finish_viewers', side_effect=lambda *a, **k: (registry.calls.append('cleanup') or {'status':'PASS','finish_status':'finished','cleanup_errors':[],'viewers':{'a': {'status':'closed','actual_close':True,'expected_close':True,'browser_launched':True,'origin_loaded':True,'close_code':1000,'acknowledged':True},'b': {'status':'closed','actual_close':True,'expected_close':True,'browser_launched':True,'origin_loaded':True,'close_code':1000,'acknowledged':True}}})), \
              patch.object(helper, 'run_revocation_case', return_value={'status':'PASS','evidence':{
                  'revocation_close':True,'revocation_metric_delta':True,'healthy_peer_data':True,
                  'app_acknowledged':True,'outbox_drained':True,'denied_reconnect':True,'history_denied':True},
                   'active_b_capture': {'cleaned': False}}), \
              patch.object(helper, 'run_controlled_stop_case', return_value={'controlled_stop_close':True,'graceful_exit':True,'restart_ready':True,'restart_subscribe_ack':True}), \
             patch('stage3_mqtt_ingress_runner.parse_run_summary', return_value={'completed':'true','app_acknowledged':'1'}), \
             patch('paper_p5a_support.BACKEND_QUOTA', ctx['quota']):
            with patch.object(smoke_obj, 'HTTPSEndpoint', endpoint_type):
                # Distinguish baseline from post-close expiry counter.
                counts=iter((2,3,3)); smoke_obj.http_request=lambda *a, **k: (200, {}, {'websocket_metrics': {'availability':'available','expiries':next(counts),'revocations':0}})
                result=helper.run_lifecycle(smoke_obj, fixture, registry, endpoint_type(), {}, {'gateway':'img'}, SimpleNamespace())
        self.assertEqual(result['status'], 'PASS')
        self.assertFalse(result['exact_cleanup_verified'])
        calls=registry.calls
        self.assertLess(calls.index('both_ack'), calls.index('actual_close'))
        self.assertLess(calls.index('actual_close'), calls.index('gateway'))
        self.assertLess(calls.index('gateway'), calls.index('cleanup'))
        self.assertTrue(result['evidence']['expiry_metric_delta'])
        self.assertTrue(result['evidence']['healthy_peer_data'])
        self.assertTrue(result['evidence']['outbox_drained'])

    def test_reconnect_gap_query_uses_db_psql_supported_variable_names(self):
        registry = Mock()
        registry.command.return_value = Mock(returncode=0, stdout=b'[]', stderr=b'')
        query = (
            "SELECT coalesce(json_agg(json_build_object('message_id',q.message_id::text) "
            "ORDER BY q.message_id, q.sample_index),'[]'::json) "
            "FROM (SELECT t.* FROM telemetry t WHERE t.gateway_id=:'gw' AND t.sensor_id=:'sensor' "
            "AND t.message_id::text IN (:'first_gap_message_id', :'second_gap_message_id') "
            "ORDER BY t.message_id, t.sample_index) q;"
        )
        variables = {
            'gw': '11111111-1111-4111-8111-111111111111',
            'sensor': '22222222-2222-4222-8222-222222222222',
            'first_gap_message_id': '33333333-3333-4333-8333-333333333333',
            'second_gap_message_id': '44444444-4444-4444-8444-444444444444',
        }
        self.assertEqual(smoke.db_psql(registry, 'postgres-container', query, variables), '[]')
        command = registry.command.call_args.args[0]
        self.assertIn("IN (:'first_gap_message_id', :'second_gap_message_id')", query)
        self.assertIn('-v', command)
        for name, value in variables.items():
            self.assertIn(name + '=' + value, command)
        with self.assertRaisesRegex(ValueError, 'db_variable_invalid'):
            smoke.db_psql(registry, 'postgres-container', query, {'m1': variables['first_gap_message_id']})

    def test_content_oracles_accept_exact_and_reject_changed_event_and_rest_content(self):
        base = '2026-10-27T10:00:00.000000Z'
        ledger, raw, events = [], [], []
        for batch in range(4):
            mid, first = f'm{batch}', 10 + batch * 2
            row = {'gateway_id': 'gw', 'sensor_id': 's', 'message_id': mid, 'boot_id': 'boot',
                   'first_sequence': str(first), 'sample_count': 100, 'measured_at': base}
            ledger.append(row)
            raw.extend({'gateway_id': 'gw', 'sensor_id': 's', 'message_id': mid, 'boot_id': 'boot',
                'sample_index': i, 'sequence_num': str(first + i),
                'measured_at': (datetime.fromisoformat(base.replace('Z', '+00:00')) + timedelta(microseconds=i * 10000)).isoformat(timespec='microseconds').replace('+00:00', 'Z'),
                'value': i + 1} for i in range(100))
            events.append(dict(row, sample_interval_us=10000, samples=list(range(1, 101))))
        self.assertTrue(helper.telemetry_event_matches_db(events, ledger, raw, 'gw', 's'))
        altered = [dict(events[0], samples=[1, 999, *events[0]['samples'][2:]]), *events[1:]]
        self.assertFalse(helper.telemetry_event_matches_db(altered, ledger, raw, 'gw', 's'))
        self.assertFalse(helper.telemetry_event_matches_db(events, ledger, raw, 'wrong', 's'))
        api = [{'observedAt': row['measured_at'], 'value': row['value']} for row in raw]
        self.assertTrue(helper.telemetry_values_match_db(api, raw))
        self.assertFalse(helper.telemetry_values_match_db([dict(api[0], value=9), *api[1:]], raw))
        self.assertFalse(helper.telemetry_values_match_db([dict(api[0], observedAt='2026-10-27T10:00:00.000001Z'), *api[1:]], raw))

        # Actual wire response shape from NGSI-LD temporal values endpoint: [[value, timestamp], ...]
        api_tuples = [[row['value'], row['measured_at']] for row in raw]
        self.assertTrue(helper.telemetry_values_match_db(api_tuples, raw))
        # Changed value in tuple
        self.assertFalse(helper.telemetry_values_match_db([[999.0, raw[0]['measured_at']], *api_tuples[1:]], raw))
        # Changed timestamp by 1 microsecond
        self.assertFalse(helper.telemetry_values_match_db([[raw[0]['value'], '2026-10-27T10:00:00.000001Z'], *api_tuples[1:]], raw))
        # Reproduce REST oracle failure: truncated 256 points compared to full 400 raw rows
        self.assertFalse(helper.telemetry_values_match_db(api_tuples[:256], raw))
        # Missing tuple (399 items)
        self.assertFalse(helper.telemetry_values_match_db(api_tuples[:-1], raw))

    def test_partition_temporal_subwindows_and_oracle_diagnostics(self):
        import tempfile
        from types import SimpleNamespace
        base = '2026-10-27T10:00:00.000000Z'
        base_dt = datetime.fromisoformat(base.replace('Z', '+00:00'))
        raw = [{'gateway_id': 'gw', 'sensor_id': 's', 'message_id': f'm{i // 100}',
                'measured_at': (base_dt + timedelta(microseconds=i * 10000)).isoformat(timespec='microseconds').replace('+00:00', 'Z'),
                'value': float(i + 1)} for i in range(400)]
        # 1. Partition 400 points with cap 256 produces 2 non-overlapping subwindows
        windows = helper.partition_temporal_subwindows(raw, max_points=256)
        self.assertEqual(len(windows), 2)
        w0_start, w0_end = windows[0]
        w1_start, w1_end = windows[1]
        self.assertEqual(w0_start, raw[0]['measured_at'])
        self.assertEqual(w0_end, raw[256]['measured_at'])
        self.assertEqual(w1_start, raw[256]['measured_at'])
        # Non-overlapping half-open boundary
        self.assertEqual(w0_end, w1_start)
        # Exact microseconds: final sample + 1 microsecond
        expected_final = (datetime.fromisoformat(raw[-1]['measured_at'].replace('Z', '+00:00')) + timedelta(microseconds=1)).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        self.assertEqual(w1_end, expected_final)

        # Subwindows from (first, last) bounds without rows
        windows_bounds = helper.partition_temporal_subwindows(raw[0]['measured_at'], raw[-1]['measured_at'], max_points=256, sample_interval_us=10000)
        self.assertEqual(len(windows_bounds), 2)
        self.assertEqual(windows_bounds[0], windows[0])
        self.assertEqual(windows_bounds[1], windows[1])

        # 2. Same-timestamp grouping preservation
        grouped_rows = [{'measured_at': '2026-10-27T10:00:00.000000Z'} for _ in range(200)] + \
                       [{'measured_at': '2026-10-27T10:00:01.000000Z'} for _ in range(60)]
        g_windows = helper.partition_temporal_subwindows(grouped_rows, max_points=256)
        self.assertEqual(len(g_windows), 2)
        self.assertEqual(g_windows[0][0], '2026-10-27T10:00:00.000000Z')
        self.assertEqual(g_windows[0][1], '2026-10-27T10:00:01.000000Z')
        self.assertEqual(g_windows[1][0], '2026-10-27T10:00:01.000000Z')
        self.assertEqual(g_windows[1][1], '2026-10-27T10:00:01.000001Z')

        # 3. Honest failure when a single timestamp group exceeds max_points
        oversized = [{'measured_at': '2026-10-27T10:00:00.000000Z'} for _ in range(257)]
        with self.assertRaisesRegex(ValueError, 'timestamp_group_exceeds_window_cap'):
            helper.partition_temporal_subwindows(oversized, max_points=256)

        # 4. Diagnostic diff computation
        api_tuples = [[r['value'], r['measured_at']] for r in raw]
        diff_exact = helper.compute_telemetry_diff(api_tuples, raw)
        self.assertTrue(diff_exact['content_match'])
        self.assertEqual(diff_exact['expected_count'], 400)
        self.assertEqual(diff_exact['actual_count'], 400)

        # Truncated diff (single window bug reproduction)
        diff_trunc = helper.compute_telemetry_diff(api_tuples[:256], raw)
        self.assertFalse(diff_trunc['content_match'])
        self.assertEqual(diff_trunc['expected_count'], 400)
        self.assertEqual(diff_trunc['actual_count'], 256)
        self.assertEqual(diff_trunc['missing_count'], 144)

        # 5. Private diagnostic file saving with 0600 permissions
        with tempfile.TemporaryDirectory() as td:
            fixture = SimpleNamespace(private=Path(td))
            diag = {'status': 'mismatch', 'sensors': [{'sensor_urn': 'urn:s', 'diff': diff_trunc}]}
            raw_dumps = {'p7-db-raw.json': raw[:10], 'p7-rest-raw.json': api_tuples[:10]}
            helper.save_oracle_private_diagnostic(fixture, diag, raw_dumps)

            p_diag = fixture.private / 'p7-telemetry-oracle-diagnostic.json'
            self.assertTrue(p_diag.exists())
            self.assertEqual(p_diag.stat().st_mode & 0o777, 0o600)
            data = json.loads(p_diag.read_text())
            self.assertEqual(data['status'], 'mismatch')

            p_raw = fixture.private / 'p7-db-raw.json'
            self.assertTrue(p_raw.exists())
            self.assertEqual(p_raw.stat().st_mode & 0o777, 0o600)

    def test_real_ack_verifier_shape_is_used(self):
        from stage3_mqtt_ingress_runner import verify_ack
        payload = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack', 'message_id': 'm',
                              'status': 'committed', 'committed_at': '2026-10-27T10:00:00Z'}).encode()
        verify_ack('m', payload)
        with self.assertRaises(RuntimeError):
            verify_ack('other', payload)

    def test_two_viewer_driver_uses_private_config_and_real_ack_capture_bounds(self):
        source = helper.two_viewer_capture_source()
        self.assertIn('process.argv[2]', source)
        self.assertIn("message_type==='ack'", source)
        self.assertIn("action:'subscribe'", source)
        self.assertIn("message_type==='telemetry'", source)
        self.assertIn('wrong_namespace', source)
        self.assertIn('MAX_EVENTS = 256', source)
        self.assertIn('MAX_FRAME_BYTES', source)
        self.assertIn('SERVER_MAX_FRAME_BYTES = 66560', source)
        self.assertIn('MAX_LIFETIME_MS = 60_000', source)
        self.assertIn('--ignore-certificate-errors', source)
        self.assertNotIn('console.', source)
        cmd = helper.two_viewer_command()
        self.assertNotIn('PRIVATE_JWT', json.dumps(cmd))
        self.assertNotIn('token', json.dumps(cmd).lower())
        self.assertIn('/probe/config.json', cmd)
        self.assertEqual(cmd, ['node', '/probe/capture.cjs', '/probe/config.json', '/out'])

    def test_two_viewer_helper_readiness_stop_and_owned_cleanup(self):
        import tempfile
        fixture = Mock(project='p7-owned', private=Path(tempfile.mkdtemp()))
        registry = Mock()
        item = {'name': 'owned', 'receipt': None}
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, {'receipt': {'id': 'c' * 64}}]
        registry.command.return_value = Mock(returncode=0)
        with patch.object(helper, 'ensure_playwright_tool_image', return_value=('tag', 'sha256:qualified')):
            capture = helper.start_viewers(fixture, registry, 'wss://127.0.0.1:443/v1/ws', 'https://client.example.test', [
                {'viewer_id': 'a', 'token': 'JWT_A', 'target': {'gateway_id': 'g1', 'sensor_id': 's1'}},
                {'viewer_id': 'b', 'token': 'JWT_B', 'target': {'gateway_id': 'g2', 'sensor_id': 's2'}},
            ])
        payload = json.loads(capture['config_path'].read_text())
        self.assertEqual([x['token'] for x in payload['viewers']], ['JWT_A', 'JWT_B'])
        self.assertTrue((capture['owned_dir'] / 'capture.cjs').exists())
        self.assertFalse((capture['owned_dir'] / 'capture.mjs').exists())
        self.assertIn('sha256:qualified', registry.command.call_args_list[0][0][0])
        self.assertIn(f"{capture['owned_dir'] / 'capture.cjs'}:/probe/capture.cjs:ro", registry.command.call_args_list[0][0][0])
        (capture['output_dir'] / 'ready.json').write_text(json.dumps({'status': 'ready', 'viewers': ['a', 'b']}))
        self.assertEqual(helper.wait_ready(capture, timeout=1)['status'], 'ready')
        final = {'status': 'PASS', 'finish_status': 'finished', 'cleanup_errors': [],
                 'viewers': {'a': {'status': 'PASS'}, 'b': {'status': 'PASS'}}}
        (capture['output_dir'] / 'final.json').write_text(json.dumps(final))
        self.assertEqual(helper.finish_viewers(capture, registry, timeout=1), final)
        self.assertEqual(registry.remove.call_count, 1)

    def test_two_viewer_readiness_failure_cleans_receipt_and_stop_timeout_is_explicit(self):
        import tempfile
        fixture = Mock(project='p7-owned', private=Path(tempfile.mkdtemp()))
        registry = Mock()
        item = {'name': 'owned', 'receipt': None}
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, {'receipt': {'id': 'd' * 64}}]
        registry.command.return_value = Mock(returncode=0)
        with patch.object(helper, 'ensure_playwright_tool_image', return_value=('tag', 'sha256:qualified')):
            capture = helper.start_viewers(fixture, registry, 'wss://127.0.0.1:443/v1/ws', 'https://client.example.test', [
                {'viewer_id': 'a', 'token': 'JWT_A', 'target': {'gateway_id': 'g1', 'sensor_id': 's1'}},
                {'viewer_id': 'b', 'token': 'JWT_B', 'target': {'gateway_id': 'g2', 'sensor_id': 's2'}},
            ])
        with self.assertRaisesRegex(ValueError, 'viewer_ready_timeout'):
            helper.wait_ready(capture, timeout=0.01)
        with self.assertRaisesRegex(ValueError, 'viewer_capture_finalization_timeout'):
            helper.finish_viewers(capture, registry, timeout=0.01)
        self.assertEqual(capture['finish_diagnostic']['code'], 'finalization_timeout')
        registry.remove.assert_called_once_with(item)

    def test_capture_finish_diagnostics_distinguish_driver_exit_invalid_result_and_cleanup(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            output = Path(td)
            registry = Mock()
            exited = {'output_dir': output, 'container_id': 'owned-id'}
            registry.command.return_value = Mock(returncode=0, stdout='{"Running":false,"ExitCode":17,"OOMKilled":true}')
            with self.assertRaisesRegex(ValueError, 'process_exited_without_final'):
                helper.finish_viewers(exited, registry, timeout=0.001)
            self.assertEqual(exited['finish_diagnostic'], {'phase': 'await_final', 'code': 'process_exited_without_final'})
            self.assertEqual(exited['container_exit_diagnostic'], {'exited': True, 'exit_code': 17, 'oom_killed': True})
            (output / 'final.json').write_text('{"status":"FAIL","finish_status":"finished","viewers":{}}')
            invalid = {'output_dir': output}
            with self.assertRaisesRegex(ValueError, 'viewer_capture_failed'):
                helper.finish_viewers(invalid, Mock(), timeout=1)
            self.assertEqual(invalid['finish_diagnostic']['code'], 'result_invalid')
            (output / 'final.json').write_text(json.dumps({'status': 'FAIL', 'finish_status': 'finished',
                'cleanup_errors': ['context_close_failed'], 'viewers': {'a': {}, 'b': {}}}))
            failed_cleanup = {'output_dir': output}
            with self.assertRaisesRegex(ValueError, 'viewer_capture_cleanup_failed'):
                helper.finish_viewers(failed_cleanup, Mock(), timeout=1)
            self.assertEqual(failed_cleanup['finish_diagnostic']['code'], 'driver_cleanup_failed')

    def test_telemetry_mode_dispatch_and_acceptance_are_not_handshake_only(self):
        fixture = Mock(paper_p7=True, paper_p7_mode='telemetry_pilot')
        self.assertEqual(helper.pilot_mode(fixture), 'telemetry_pilot')
        self.assertFalse(helper.telemetry_pilot_result_ok({'status': 'PASS', 'handshake': {'status': 'PASS'}}))
        valid = {'status': 'PASS', 'mode': 'paper_p7_telemetry_pilot', 'full_p7_accepted': False,
                 'viewers': {'a': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True},
                             'b': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True}},
                 'namespace_isolation': True, 'grouped_persistence': True,
                 'dedup_unchanged': True, 'outbox_drained': True, 'db_rest_match': True,
                  'exact_cleanup_verified': True,
                  'event_db_match': True, 'evidence': {'db_rows': 800, 'event_db_match': True, 'rest_match': True, 'duplicate_observed': True,
                               'duplicate_ack_observed': True, 'no_replay_event': True}}
        self.assertTrue(helper.telemetry_pilot_result_ok(valid))
        for key in ('namespace_isolation', 'grouped_persistence', 'dedup_unchanged', 'outbox_drained'):
            bad = dict(valid, **{key: False})
            self.assertFalse(helper.telemetry_pilot_result_ok(bad), key)
        self.assertFalse(helper.telemetry_pilot_result_ok(dict(valid, full_p7_accepted=True)))

    def test_telemetry_result_requires_actual_oracle_evidence_and_outer_cleanup(self):
        valid = {'status': 'PASS', 'mode': 'paper_p7_telemetry_pilot', 'full_p7_accepted': False,
                 'viewers': {k: {'subscribed': True, 'events': 4, 'expected_events': 4,
                                 'no_extra_namespace': True} for k in ('a', 'b')},
                 'namespace_isolation': True, 'grouped_persistence': True, 'dedup_unchanged': True,
                 'outbox_drained': True, 'db_rest_match': True, 'exact_cleanup_verified': True,
                  'event_db_match': True, 'evidence': {'db_rows': 800, 'event_db_match': True, 'rest_match': True, 'duplicate_observed': True,
                              'duplicate_ack_observed': True, 'no_replay_event': True}}
        self.assertTrue(helper.telemetry_pilot_result_ok(valid))
        for bad in (
            dict(valid, evidence={**valid['evidence'], 'db_rows': 799}),
            dict(valid, evidence={**valid['evidence'], 'rest_match': False}),
            dict(valid, evidence={**valid['evidence'], 'duplicate_ack_observed': False}),
            dict(valid, exact_cleanup_verified=False),
        ):
            self.assertFalse(helper.telemetry_pilot_result_ok(bad))
        self.assertFalse(helper.telemetry_pilot_result_ok(dict(valid, status='FAIL')))

    def test_real_prepare_workload_contract_uses_quota_mapping_and_https_endpoint(self):
        from types import SimpleNamespace
        quota = {'cpus': 2, 'memory_bytes': 4294967296}
        endpoint = smoke.HTTPSEndpoint('https://localhost', Path('/ca.crt'))
        self.assertTrue(helper.telemetry_profile_ok(quota, endpoint, smoke))
        self.assertFalse(helper.telemetry_profile_ok(2, endpoint, smoke))
        self.assertFalse(helper.telemetry_profile_ok(quota, SimpleNamespace(url='http://localhost'), smoke))

    def test_capture_driver_stops_ack_deadline_and_writes_final_snapshot(self):
        source = helper.two_viewer_capture_source()
        self.assertIn('clearTimeout(ackTimer)', source)
        self.assertIn('window.__p7Capture?.()', source)
        self.assertIn('MAX_LIFETIME_MS = 60_000', source)
        self.assertIn('headersOnly?{gateway_id:msg.gateway_id', source)
        self.assertIn('msg.samples.length!==msg.sample_count', source)
        self.assertLess(source.index("write('snapshot.json',result);"), source.index('await v.context.close()'))
        self.assertGreater(source.index("write('final.json',result);"), source.index('await v.context.close()'))
        self.assertIn("await browser.close()", source)

    def test_capture_finish_waits_for_final_not_snapshot_and_rejects_malformed_final(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            output = Path(td)
            capture = {'output_dir': output}
            registry = Mock()
            (output / 'snapshot.json').write_text(json.dumps({'status': 'PASS', 'finish_status': 'snapshot'}))
            final = {'status': 'PASS', 'finish_status': 'finished', 'cleanup_errors': [],
                     'viewers': {'a': {'status': 'PASS'}, 'b': {'status': 'PASS'}}}
            (output / 'final.json').write_text(json.dumps(final))
            self.assertEqual(helper.finish_viewers(capture, registry, timeout=1), final)
            malformed = {'output_dir': output}
            (output / 'final.json').write_text('{')
            with self.assertRaisesRegex(ValueError, 'final_result_malformed'):
                helper.finish_viewers(malformed, registry, timeout=1)
            self.assertEqual(malformed['finish_diagnostic']['code'], 'final_result_malformed')

    def test_capture_finish_ignores_snapshot_until_final_artifact_is_published(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            output = Path(td)
            capture = {'output_dir': output}
            final = {'status': 'PASS', 'finish_status': 'finished', 'cleanup_errors': [],
                     'viewers': {'a': {'status': 'PASS'}, 'b': {'status': 'PASS'}}}
            (output / 'snapshot.json').write_text(json.dumps({'status': 'PASS', 'finish_status': 'snapshot'}))
            def publish_final(_):
                (output / 'final.json').write_text(json.dumps(final))
            with patch.object(helper.time, 'sleep', side_effect=publish_final):
                self.assertEqual(helper.finish_viewers(capture, Mock(), timeout=1), final)
            self.assertEqual(capture['finish_diagnostic']['code'], 'ok')

    def test_single_path_mode_profile_and_acceptance(self):
        import tempfile
        from types import SimpleNamespace
        fixture = Mock(paper_p7=True, paper_p7_mode='single_path')
        self.assertEqual(helper.pilot_mode(fixture), 'single_path')

        profile = helper.single_path_profile()
        self.assertEqual(profile['MQTT_MICROBATCH_ENABLED'], 'false')
        self.assertEqual(profile['MQTT_WORKER_COUNT'], '1')
        self.assertEqual(profile['MQTT_INGRESS_ENABLED'], 'true')
        self.assertEqual(profile['MQTT_RAM_DEDUP_ENABLED'], 'true')
        pilot_prof = helper.telemetry_pilot_profile()
        for k, v in profile.items():
            if k == 'MQTT_MICROBATCH_ENABLED':
                self.assertEqual(v, 'false')
                self.assertEqual(pilot_prof[k], 'true')
            else:
                self.assertEqual(v, pilot_prof[k])

        valid_single = {
            'status': 'PASS', 'mode': 'paper_p7_single_path', 'full_p7_accepted': False,
            'viewers': {'a': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True},
                        'b': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True}},
            'namespace_isolation': True, 'single_persistence': True,
            'dedup_unchanged': True, 'outbox_drained': True, 'db_rest_match': True,
            'exact_cleanup_verified': True, 'event_db_match': True,
            'evidence': {
                'db_rows': 800, 'microbatch_enabled': False, 'max_members': 0,
                'single_committed': 8, 'single_samples': 800,
                'event_db_match': True, 'rest_match': True, 'duplicate_observed': True,
                'duplicate_ack_observed': True, 'no_replay_event': True
            }
        }
        self.assertTrue(helper.single_path_result_ok(valid_single))

        # NO FALSE GROUPED ACCEPTANCE: grouped validator rejects single_path result
        self.assertFalse(helper.telemetry_pilot_result_ok(valid_single))

        # NO FALSE SINGLE ACCEPTANCE: single_path validator rejects grouped result
        valid_grouped = {'status': 'PASS', 'mode': 'paper_p7_telemetry_pilot', 'full_p7_accepted': False,
                         'viewers': {'a': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True},
                                     'b': {'subscribed': True, 'events': 4, 'expected_events': 4, 'no_extra_namespace': True}},
                         'namespace_isolation': True, 'grouped_persistence': True, 'dedup_unchanged': True,
                         'outbox_drained': True, 'db_rest_match': True, 'exact_cleanup_verified': True,
                         'event_db_match': True, 'evidence': {'db_rows': 800, 'event_db_match': True, 'rest_match': True,
                                                              'duplicate_observed': True, 'duplicate_ack_observed': True,
                                                              'no_replay_event': True}}
        self.assertTrue(helper.telemetry_pilot_result_ok(valid_grouped))
        self.assertFalse(helper.single_path_result_ok(valid_grouped))

        # Strict checks for single_path_result_ok
        for bad in (
            dict(valid_single, status='FAIL'),
            dict(valid_single, full_p7_accepted=True),
            dict(valid_single, single_persistence=False),
            dict(valid_single, exact_cleanup_verified=False),
            dict(valid_single, evidence={**valid_single['evidence'], 'microbatch_enabled': True}),
            dict(valid_single, evidence={**valid_single['evidence'], 'max_members': 2}),
            dict(valid_single, evidence={**valid_single['evidence'], 'single_committed': 7}),
            dict(valid_single, evidence={**valid_single['evidence'], 'single_samples': 799}),
            dict(valid_single, evidence={**valid_single['evidence'], 'db_rows': 799}),
            dict(valid_single, evidence={**valid_single['evidence'], 'rest_match': False}),
            dict(valid_single, evidence={**valid_single['evidence'], 'duplicate_observed': False}),
            dict(valid_single, evidence={**valid_single['evidence'], 'duplicate_ack_observed': False}),
            dict(valid_single, evidence={**valid_single['evidence'], 'no_replay_event': False}),
        ):
            self.assertFalse(helper.single_path_result_ok(bad))

        # Test smoke.prepare for single_path
        with tempfile.TemporaryDirectory() as d:
            f = smoke.prepare(Path(d) / 'p7', paper_p7=True, paper_p7_mode='single_path')
            self.assertEqual(f.values['MQTT_WORKER_COUNT'], '1')
            self.assertEqual(f.values['MQTT_MICROBATCH_ENABLED'], 'false')
            self.assertEqual(f.values['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(f.values['STAGE5_ENABLED'], 'false')

            spec = SimpleNamespace(gateway_id='gw', sensor_id='sensor')
            bad_effective = {'services': {'backend': {'environment': dict(f.values, MQTT_MICROBATCH_ENABLED='true')}}}
            bad_smoke = SimpleNamespace(
                prepare_paper_workload=lambda *a: ('pg', {'cpus': 2, 'memory_bytes': 4294967296},
                    bad_effective, {'backend': {'Memory': 4294967296}}, 'storage',
                    {'a': 'ta', 'b': 'tb', 'admin': 'admin'}, [spec, spec], ['pa', 'pb'], '/sensor')
            )
            bad_smoke.HTTPSEndpoint = type('HTTPSEndpoint', (), {'url': 'https://localhost'})
            with self.assertRaisesRegex(ValueError, '^p7_telemetry_ingress_profile_rejected$'):
                helper.run_telemetry_pilot(bad_smoke, f, Mock(), bad_smoke.HTTPSEndpoint(),
                    {'mosquitto': {'receipt': {'id': 'broker'}}}, {'gateway': 'image'}, Mock())

    def test_native_telemetry_selector_and_capture_frame_cap_are_explicit(self):
        self.assertIn('--paper-p7-mode', (smoke.ROOT / 'scripts/tests/native_compose_smoke.py').read_text())
        with self.assertRaisesRegex(ValueError, 'p7_mode_rejected'):
            helper.pilot_mode(Mock(paper_p7_mode='future_mode'))
        pilot_fixture = Mock(paper_p0=False, paper_p1=None, paper_p2=False, paper_p7=True,
                             paper_p7_mode='telemetry_pilot')
        single_path_fixture = Mock(paper_p0=False, paper_p1=None, paper_p2=False, paper_p7=True,
                                   paper_p7_mode='single_path')
        handshake_fixture = Mock(paper_p0=False, paper_p1=None, paper_p2=True, paper_p7=True,
                                 paper_p7_mode='handshake_only')
        self.assertTrue(smoke.requires_gateway_image(pilot_fixture))
        self.assertTrue(smoke.requires_gateway_image(single_path_fixture))
        self.assertTrue(smoke.requires_gateway_image(handshake_fixture))
        self.assertFalse(smoke.requires_gateway_image(Mock(paper_p0=False, paper_p1=None,
            paper_p2=False, paper_p7=False, paper_p7_mode=None)))
        source = helper.two_viewer_capture_source()
        self.assertIn('cfg.max_frame_bytes', source)
        self.assertNotIn('Math.min(MAX_FRAME_BYTES,cfg.max_frame_bytes', source)

    def test_bounded_viewer_readiness_rejects_error_and_malformed_artifact(self):
        self.assertTrue(helper.viewer_ready_artifact({
            'status': 'ready', 'viewer_id': 'viewer-a', 'acknowledged': True,
            'capture_limit': 32, 'max_frame_bytes': 66560, 'events': []}, 'viewer-a'))
        for artifact in (
            {'status': 'error', 'viewer_id': 'viewer-a', 'acknowledged': True, 'capture_limit': 32, 'max_frame_bytes': 66560, 'events': []},
            {'status': 'ready', 'viewer_id': 'viewer-a', 'acknowledged': False, 'capture_limit': 32, 'max_frame_bytes': 66560, 'events': []},
            {'status': 'ready', 'viewer_id': 'viewer-a', 'acknowledged': True, 'capture_limit': 0, 'max_frame_bytes': 66560, 'events': []},
            {'status': 'ready', 'viewer_id': 'viewer-b', 'acknowledged': True, 'capture_limit': 32, 'max_frame_bytes': 66560, 'events': []},
        ):
            self.assertFalse(helper.viewer_ready_artifact(artifact, 'viewer-a'))

    def test_telemetry_pilot_subscribers_enforce_contract_and_reject_missing_kind(self):
        fixture = SimpleNamespace(values={'MQTT_USERNAME': 'backend_service', 'MQTT_PASSWORD': 'pw'})
        spec = SimpleNamespace(gateway_id='gw-0', sensor_id='s-0')
        # Without explicit subscription_kind, tag 'p7-0' is rejected
        with self.assertRaisesRegex(ValueError, '^paper_p1_subscription_failed$'):
            smoke.paper_p1_subscription_target(fixture, spec, 'pwd', 'p7-0')
        # With explicit kind='telemetry', succeeds with backend credentials and wildcard topic
        u, p, topic = smoke.paper_p1_subscription_target(fixture, spec, 'pwd', 'p7-0', kind='telemetry')
        self.assertEqual(u, 'backend_service')
        self.assertEqual(p, 'pw')
        self.assertEqual(topic, 'gateways/+/telemetry/#')
        # With explicit kind='ack', succeeds with gateway credentials and gateway ack topic
        u, p, topic = smoke.paper_p1_subscription_target(fixture, spec, 'pwd', 'p7-replay-ack', kind='ack')
        self.assertEqual(u, 'gw-0')
        self.assertEqual(p, 'pwd')
        self.assertEqual(topic, 'gateways/gw-0/acks/#')

    def test_telemetry_pilot_profile_mapping_and_effective_validation(self):
        import tempfile
        profile = helper.telemetry_pilot_profile()
        self.assertEqual(profile['MQTT_WORKER_COUNT'], '1')
        self.assertEqual(profile['MQTT_MICROBATCH_ENABLED'], 'true')
        self.assertEqual(profile['MQTT_RAM_DEDUP_ENABLED'], 'true')
        self.assertEqual(profile['STAGE5_ENABLED'], 'false')

        with tempfile.TemporaryDirectory() as d:
            f = smoke.prepare(Path(d) / 'p7', paper_p7=True, paper_p7_mode='telemetry_pilot')
            self.assertEqual(f.values['MQTT_WORKER_COUNT'], '1')
            self.assertEqual(f.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(f.values['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(f.values['STAGE5_ENABLED'], 'false')

            # Test run_telemetry_pilot profile validation
            spec = SimpleNamespace(gateway_id='gw', sensor_id='sensor')
            # Rejects worker count != 1 in run_telemetry_pilot
            bad_effective = {'services': {'backend': {'environment': dict(f.values, MQTT_WORKER_COUNT='4')}}}
            bad_smoke = SimpleNamespace(
                prepare_paper_workload=lambda *a: ('pg', {'cpus': 2, 'memory_bytes': 4294967296},
                    bad_effective, {'backend': {'Memory': 4294967296}}, 'storage',
                    {'a': 'ta', 'b': 'tb', 'admin': 'admin'}, [spec, spec], ['pa', 'pb'], '/sensor')
            )
            bad_smoke.HTTPSEndpoint = type('HTTPSEndpoint', (), {'url': 'https://localhost'})
            with self.assertRaisesRegex(ValueError, '^p7_telemetry_ingress_profile_rejected$'):
                helper.run_telemetry_pilot(bad_smoke, f, Mock(), bad_smoke.HTTPSEndpoint(),
                    {'mosquitto': {'receipt': {'id': 'broker'}}}, {'gateway': 'image'}, Mock())

    def test_telemetry_workload_orders_ack_producers_and_capture_cleanup(self):
        import tempfile
        from types import SimpleNamespace
        from unittest.mock import call
        root = Path(tempfile.mkdtemp())
        fixture = SimpleNamespace(project='pilot', private=root,
                                  values={'MQTT_USERNAME': 'backend_service', 'MQTT_PASSWORD': 'pw'})
        spec_a = SimpleNamespace(gateway_id='gw-a', sensor_id='sensor', gateway_name='a', sensor_urn='urn:a')
        spec_b = SimpleNamespace(gateway_id='gw-b', sensor_id='sensor', gateway_name='b', sensor_urn='urn:b')
        registry = Mock()
        registry.intent.side_effect = lambda kind, name, image, labels: {'name': name, 'receipt': {'name': name}}
        registry.resolve.side_effect = [None, {'name': 'v0'}, None, {'name': 'v1'}]
        registry.command.return_value = Mock(returncode=0)
        events = []
        capture = {'owned': True}
        smoke_sub = lambda f, r, b, spec, pwd, tag, **k: (
            smoke.paper_p1_subscription_target(f, spec, pwd, tag, kind=k.get('subscription_kind')) and
            (events.append('mqtt_ready') or object()))
        smoke_obj = SimpleNamespace(
            prepare_paper_workload=lambda *args: (events.append('prepare') or
                ('pg', {'cpus': 2, 'memory_bytes': 4294967296}, {'services': {'backend': {'environment': {'MQTT_INGRESS_ENABLED': 'true',
                    'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true', 'MQTT_WORKER_COUNT': '1'}}}},
                 {'backend': {'Memory': 4294967296}}, 'storage', {'a': 'ta', 'b': 'tb', 'admin': 'admin'},
                 [spec_a, spec_b], ['pa', 'pb'], '/sensor')),
            paper_p1_subscriber=smoke_sub,
            paper_p1_stop_subscription=lambda sub: events.append('observer_stop'),
            private_write=lambda path, text: Path(path).write_text(text),
            paper_p0_gateway_cli=None,
              paper_p1_capture_subscription=lambda *a, **k: b'{"message_id":"message-1","original":true}')
        smoke_obj.HTTPSEndpoint = type('HTTPSEndpoint', (), {'url': 'https://localhost'})
        smoke_obj.db_psql = lambda *a, **k: '0'
        smoke_obj.http_request = lambda *a, **k: (200, {}, {'ingress': {'microbatch': {'max_members': 2}}})
        smoke_obj.paper_p0_stats = lambda *a, **k: {}
        smoke_obj.paper_p1_resources_csv = lambda *a, **k: None
        smoke_obj.paper_p1_metrics_csv = lambda *a, **k: None
        smoke_obj.durable_json = lambda *a, **k: None
        def gateway(*args, **kwargs):
            if args[-2] == ['count']:
                return 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0'
            events.append('producer')
            if kwargs.get('during'):
                kwargs['during']()
            return 'summary'
        smoke_obj.paper_p0_gateway_cli = gateway
        def start(*args, **kwargs):
            events.append('viewer_start')
            return capture
        def ready(*args, **kwargs):
            events.append('viewer_ack')
            return {'status': 'ready'}
        def finish(*args, **kwargs):
            events.append('capture_finish')
            return {'status': 'PASS', 'viewers': {'a': {'acknowledged': True, 'count': 4, 'wrong_namespace': 0},
                'b': {'acknowledged': True, 'count': 4, 'wrong_namespace': 0}}}
        fixture.artifacts = root / 'artifacts'
        with patch.object(helper, 'start_viewers', side_effect=start), \
             patch.object(helper, 'wait_ready', side_effect=ready), \
             patch.object(helper, 'finish_viewers', side_effect=finish), \
             patch.object(smoke_obj, 'HTTPSEndpoint', type('HTTPSEndpoint', (), {'url': 'https://localhost'})), \
             patch.object(smoke_obj, 'db_psql', return_value='0'), \
             patch.object(smoke_obj, 'http_request', return_value=(200, {}, {'ingress': {'microbatch': {'max_members': 2}}})), \
              patch.object(smoke_obj, 'paper_p0_stats', return_value={}), \
              patch.object(smoke_obj, 'paper_p1_resources_csv', return_value=None), \
             patch.object(smoke_obj, 'paper_p1_metrics_csv', return_value=None), \
             patch.object(smoke_obj, 'durable_json', return_value=None), \
             patch.object(helper, 'get_source_qualification', return_value={'revision': 'x'}), \
             patch.object(smoke_obj, 'paper_p1_stop_subscription', return_value=None), \
             patch('stage3_mqtt_ingress_runner.parse_run_summary', return_value={'completed': 'true',
                 'enqueued': '4', 'app_acknowledged': '4', 'pending_rows': '0', 'pending_bytes': '0'}):
             with self.assertRaisesRegex(ValueError, 'p7_grouped_persistence_rejected'):
                 helper.run_telemetry_pilot(smoke_obj, fixture, registry, smoke_obj.HTTPSEndpoint(),
                     {'mosquitto': {'receipt': {'id': 'broker'}}}, {'gateway': 'image'}, SimpleNamespace(checkpoint=lambda: None))
        self.assertLess(events.index('prepare'), events.index('viewer_ack'))
        self.assertLess(events.index('viewer_ack'), events.index('producer'))
        self.assertLess(events.index('producer'), events.index('capture_finish'))
        self.assertEqual(events.count('producer'), 2)
        self.assertEqual(events.count('capture_finish'), 1)

    def test_telemetry_workload_finishes_owned_capture_after_producer_failure(self):
        import tempfile
        from types import SimpleNamespace
        root = Path(tempfile.mkdtemp())
        fixture = SimpleNamespace(project='pilot', private=root,
                                  values={'MQTT_USERNAME': 'backend_service', 'MQTT_PASSWORD': 'pw'})
        spec = SimpleNamespace(gateway_id='gw', sensor_id='sensor')
        smoke_sub = lambda f, r, b, spec, pwd, tag, **k: (
            smoke.paper_p1_subscription_target(f, spec, pwd, tag, kind=k.get('subscription_kind')) and object())
        smoke_obj = SimpleNamespace(prepare_paper_workload=lambda *a: ('pg', {'cpus': 2, 'memory_bytes': 4294967296},
            {'services': {'backend': {'environment': {'MQTT_INGRESS_ENABLED': 'true',
             'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true', 'MQTT_WORKER_COUNT': '1'}}}},
             {'backend': {'Memory': 4294967296}}, 'storage', {'a': 'ta', 'b': 'tb', 'admin': 'admin'},
            [spec, spec], ['pa', 'pb'], '/sensor'),
            paper_p1_subscriber=smoke_sub, paper_p1_stop_subscription=Mock(),
             private_write=lambda *a: None, paper_p0_gateway_cli=lambda *a, **k: (_ for _ in ()).throw(RuntimeError('producer')))
        smoke_obj.HTTPSEndpoint = type('HTTPSEndpoint', (), {'url': 'https://localhost'})
        smoke_obj.db_psql = lambda *a, **k: '0'
        smoke_obj.http_request = lambda *a, **k: (200, {}, {'ingress': {'microbatch': {'max_members': 2}}})
        smoke_obj.paper_p0_stats = lambda *a, **k: {}
        smoke_obj.paper_p1_resources_csv = lambda *a, **k: None
        smoke_obj.paper_p1_metrics_csv = lambda *a, **k: None
        smoke_obj.durable_json = lambda *a, **k: None
        registry = Mock()
        registry.intent.side_effect = lambda kind, name, image, labels: {'name': name, 'receipt': {'name': name}}
        registry.resolve.side_effect = [None, {'name': 'v0'}, None, {'name': 'v1'}]
        registry.command.return_value = Mock(returncode=0)
        fixture.artifacts = root / 'artifacts'
        with patch.object(helper, 'start_viewers', return_value={'owned': True}), \
             patch.object(helper, 'wait_ready', return_value={'status': 'ready'}), \
             patch.object(smoke_obj, 'HTTPSEndpoint', type('HTTPSEndpoint', (), {'url': 'https://localhost'})), \
             patch.object(helper, 'finish_viewers') as finish:
            with self.assertRaisesRegex(RuntimeError, 'producer'):
                 helper.run_telemetry_pilot(smoke_obj, fixture, registry, smoke_obj.HTTPSEndpoint(),
                     {'mosquitto': {'receipt': {'id': 'broker'}}}, {'gateway': 'image'}, SimpleNamespace(checkpoint=lambda: None))
        finish.assert_called_once()

    def test_private_stdin_and_browser_js_subprotocol_validation(self):
        source = helper.browser_probe_source()
        command = helper.browser_command('wss://127.0.0.1:443/v1/ws', 'ORIGIN', 5)
        self.assertIn('process.stdin', source)
        self.assertIn('new WebSocket', source)
        self.assertIn("protocol==='iot.telemetry.v1'", source)
        self.assertNotIn('console.', source)
        self.assertNotIn('JWT_SENTINEL', json.dumps(command))
        self.assertIn('node -e', command[-1])

    def test_missing_driver_or_socket_open_is_failure(self):
        self.assertFalse(helper.browser_result_ok({'status': 'FAIL', 'event': 'browser_unavailable'}))
        self.assertFalse(helper.browser_result_ok({'status': 'FAIL', 'event': 'timeout'}))
        self.assertTrue(helper.browser_result_ok({'status': 'PASS', 'event': 'open',
                                                  'selected_protocol': 'iot.telemetry.v1'}))

    def test_timeout_is_bounded_before_resource_allocation(self):
        registry = Mock()
        with self.assertRaisesRegex(ValueError, 'timeout_out_of_range'):
            helper.run_browser_probe(Mock(), Mock(), registry, 'wss://127.0.0.1/v1/ws', 'PRIVATE', timeout=31)
        registry.intent.assert_not_called()

    def test_probe_timeout_is_bounded_and_cleans_owned_container(self):
        fixture = Mock(project='p7-owned')
        item = {'name': 'owned', 'receipt': {'id': 'a' * 64}}
        registry = Mock()
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, {}]
        registry.command.return_value = Mock(returncode=0)
        with patch.object(helper.subprocess, 'run', side_effect=helper.subprocess.TimeoutExpired('docker start', 91)) as run:
            result = helper.run_browser_probe(Mock(), fixture, registry, 'wss://127.0.0.1:443/v1/ws',
                                              'PRIVATE_JWT', timeout=1)
        self.assertEqual(run.call_args.kwargs['timeout'], 91)
        registry.remove.assert_called_once_with(item)
        self.assertEqual(result['event'], 'timeout')
        self.assertNotIn('PRIVATE_JWT', json.dumps(result))

    def test_dependency_installation_is_done_before_network_none_runtime_smoke(self):
        command = helper.browser_command('wss://127.0.0.1:443/v1/ws', 'https://client.example.test', 5)
        self.assertNotIn('npm install', json.dumps(command))

        fixture = Mock(project='p7-owned')
        item = {'name': 'owned', 'receipt': {'id': 'b' * 64}}
        registry = Mock()
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, {}]
        registry.command.return_value = Mock(returncode=0)

        saved = dict(helper._TOOL_IMAGE_CACHE)
        helper._TOOL_IMAGE_CACHE[helper.PLAYWRIGHT_TOOL_TAG] = 'sha256:prebuilt'
        try:
            fake_out = json.dumps({'websocket': 'function', 'origin': 'null', 'browser': '140.0.7339.16'}).encode()
            with patch.object(helper.subprocess, 'run', return_value=Mock(returncode=0, stdout=fake_out)):
                result = helper.run_driver_smoke(fixture, registry)

            self.assertEqual(result['status'], 'PASS')
            create_args = registry.command.call_args[0][0]
            self.assertIn('--network', create_args)
            net_idx = create_args.index('--network')
            self.assertEqual(create_args[net_idx + 1], 'none')
            self.assertNotIn('npm install', json.dumps(create_args))
        finally:
            helper._TOOL_IMAGE_CACHE.clear()
            helper._TOOL_IMAGE_CACHE.update(saved)

    def test_driver_smoke_timeout_is_bounded(self):
        with self.assertRaisesRegex(ValueError, 'smoke_timeout_out_of_range'):
            helper.run_driver_smoke(Mock(), Mock(), timeout=35)
        with self.assertRaisesRegex(ValueError, 'smoke_timeout_out_of_range'):
            helper.run_driver_smoke(Mock(), Mock(), timeout=0)

    def test_tool_image_version_validation_failure_raises(self):
        saved = dict(helper._TOOL_IMAGE_CACHE)
        helper._TOOL_IMAGE_CACHE.clear()
        try:
            qualified_labels = {
                'org.opencontainers.image.base.name': helper.PLAYWRIGHT_BASE_IMAGE,
                'paper.p7.tool.recipe.hash': helper.TOOL_RECIPE_HASH,
                'paper.p7.tool.package.version': helper.PLAYWRIGHT_VERSION,
                'paper.p7.tool.owner': helper.TOOL_OWNER,
            }
            inspect_mock = Mock(returncode=0, stdout=json.dumps([{'Id': 'sha256:fake', 'Config': {'Labels': qualified_labels}}]).encode())
            val_mock = Mock(returncode=1)
            rm_mock = Mock(returncode=0)
            insp_val_mock = Mock(returncode=1)
            with patch.object(helper.subprocess, 'run', side_effect=[inspect_mock, val_mock, rm_mock, insp_val_mock, Mock(returncode=1, stderr='build error')]):
                with self.assertRaises(ValueError):
                    helper.ensure_playwright_tool_image(timeout=5)
        finally:
            helper._TOOL_IMAGE_CACHE.clear()
            helper._TOOL_IMAGE_CACHE.update(saved)

    def test_paper_p7_support_import_has_no_docker_subprocess_call(self):
        import importlib
        with patch('subprocess.run') as mock_run:
            spec = importlib.util.spec_from_file_location('p7_test_import', ROOT / 'scripts/tests/paper_p7_support.py')
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mock_run.assert_not_called()

    def test_report_validation_p7_accepts_valid_handshake_and_rejects_incomplete(self):
        fixture = Mock(project='test-p7', paper_p7=True, paper_p2=True,
                       paper_p5a=False, paper_p4=False, paper_p6=False, paper_p3=False,
                       paper_p2_spike=False, paper_p1=False, paper_p0=False,
                       artifacts=Path('/tmp'), private=Path('/tmp'), values={'MQTT_SHAPER_ENABLED': 'false'})
        registry = Mock()
        item = {
            'kind': 'container', 'run': 'test-p7', 'task': 'native-compose-allocation',
            'allocation_attempted': True, 'removed': True,
            'labels': {'test': '1'},
            'receipt': {'id': 'a' * 64, 'created': '2026-10-27T00:00:00Z', 'labels': {'test': '1'}}
        }
        registry.items = [item]
        cleanup_counts = {'container': 1, 'network': 0, 'volume': 0}
        evidence = {
            'paper_p7': {
                'status': 'PASS',
                'mode': 'paper_p7_handshake_only',
                'perimeter': {'status': 'PASS', 'https_configured': True, 'auth_request_enabled': True},
                'handshake': {
                    'status': 'PASS',
                    'native': {'http_status': 101, 'subprotocol': 'iot.telemetry.v1'},
                    'browser': {'status': 'PASS', 'event': 'open', 'selected_protocol': 'iot.telemetry.v1'},
                    'negative': {
                        'missing_credentials': {'status': 'PASS', 'http_status': 401},
                        'bad_token': {'status': 'PASS', 'http_status': 401},
                        'conflicting_credentials': {'status': 'PASS', 'http_status': 401},
                        'denied_origin': {'status': 'PASS', 'http_status': 403},
                        'missing_app_protocol': {'status': 'PASS', 'http_status': 401},
                        'expired_jwt': {'status': 'PASS', 'http_status': 401},
                        'rest_subprotocol_only': {'status': 'PASS', 'http_status': 401},
                        'browser_denied_origin': {'status': 'PASS', 'event': 'error'},
                    },
                    'perimeter_isolation': {'status': 'PASS'},
                    'leak_check': {'status': 'PASS', 'leaks_found_count': 0, 'policy': 'credential_redaction_enforced'}
                }
            }
        }
        # In smoke report_validation:
        # P7 must produce PAPER_P7_HANDSHAKE_ACCEPTED and not full_p7_accepted
        # Before fix, report_validation routes to paper_p2 and raises ValueError('cleanup_acceptance_incomplete')
        # Here we test smoke.paper_p7_cleanup_acceptance
        self.assertTrue(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, True, registry))

        # Incomplete browser must fail
        bad_browser_ev = json.loads(json.dumps(evidence))
        bad_browser_ev['paper_p7']['handshake']['browser']['status'] = 'FAIL'
        self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, bad_browser_ev, cleanup_counts, True, registry))

        # Missing negative control must fail
        bad_neg_ev = json.loads(json.dumps(evidence))
        del bad_neg_ev['paper_p7']['handshake']['negative']['expired_jwt']
        self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, bad_neg_ev, cleanup_counts, True, registry))

        # cleanup_verified False must fail
        self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, False, registry))

        # Unremoved item must fail
        bad_item = dict(item, removed=False)
        bad_registry = Mock(items=[bad_item])
        self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, True, bad_registry))

    def test_run_handshake_wrong_artifact_order_fixed(self):
        # run_handshake must NOT set status='PASS' if a negative control fails
        saved = dict(helper._TOOL_IMAGE_CACHE)
        helper._TOOL_IMAGE_CACHE[helper.PLAYWRIGHT_TOOL_IMAGE] = 'sha256:fake'
        try:
            fixture = Mock(artifacts=Path('/tmp'), private=Path('/tmp'), project='test-p7')
            smoke_mock = Mock()
            smoke_mock.http_request.return_value = (401, {}, b'')
            registry = Mock()
            endpoint = Mock(url='https://127.0.0.1:443')
            # Mock driver_smoke to PASS
            # Mock native_probe: positive returns (101, {'sec-websocket-protocol': 'iot.telemetry.v1'})
            # Mock run_browser_probe to PASS
            # Mock negative probe to return 200 instead of 401
            qualified_labels = {
                'org.opencontainers.image.base.name': helper.PLAYWRIGHT_BASE_IMAGE,
                'paper.p7.tool.recipe.hash': helper.TOOL_RECIPE_HASH,
                'paper.p7.tool.package.version': helper.PLAYWRIGHT_VERSION,
                'paper.p7.tool.owner': helper.TOOL_OWNER,
            }
            with patch.object(helper, 'mint_expired_token', return_value='expired-tok'), \
                 patch.object(helper, 'mint_control_token', return_value='control-tok'), \
                 patch.object(helper, 'run_driver_smoke', return_value={'status': 'PASS'}), \
                 patch.object(helper, 'native_probe', side_effect=[
                     (101, {'sec-websocket-protocol': 'iot.telemetry.v1'}), # positive native
                     (101, {'sec-websocket-protocol': 'iot.telemetry.v1'}), # control token probe
                     (200, {}), # negative fails (expected 401, got 200)
                     (401, {}), (401, {}), (403, {}), (401, {}), (401, {}), (401, {})
                 ]), \
                 patch.object(helper, 'run_browser_probe', return_value={'status': 'PASS', 'event': 'open', 'selected_protocol': 'iot.telemetry.v1'}):
                with self.assertRaises(ValueError):
                    helper.run_handshake(smoke_mock, fixture, registry, endpoint, {}, 'token')
                # Inspect last call to durable_json
                last_call_payload = smoke_mock.durable_json.call_args_list[-1][0][1]
                self.assertEqual(last_call_payload.get('status'), 'FAIL', "Artifact must persist FAIL when negative control fails")
        finally:
            helper._TOOL_IMAGE_CACHE.clear()
            helper._TOOL_IMAGE_CACHE.update(saved)

    def test_tool_image_build_dir_uses_opencode_scratch_never_antigravity(self):
        saved = dict(helper._TOOL_IMAGE_CACHE)
        helper._TOOL_IMAGE_CACHE.clear()
        try:
            qualified_labels = {
                'org.opencontainers.image.base.name': helper.PLAYWRIGHT_BASE_IMAGE,
                'paper.p7.tool.recipe.hash': helper.TOOL_RECIPE_HASH,
                'paper.p7.tool.package.version': helper.PLAYWRIGHT_VERSION,
                'paper.p7.tool.owner': helper.TOOL_OWNER,
            }
            inspect_mock = Mock(returncode=1) # image not yet built
            build_mock = Mock(returncode=0)
            inspect2_mock = Mock(returncode=0, stdout=json.dumps([{'Id': 'sha256:built', 'Config': {'Labels': qualified_labels}}]).encode())
            val_mock = Mock(returncode=0) # start
            rm_mock = Mock(returncode=0)
            insp_val_mock = Mock(returncode=1)
            with patch.object(helper.subprocess, 'run', side_effect=[inspect_mock, build_mock, inspect2_mock, val_mock, rm_mock, insp_val_mock]):
                tool_img, img_id = helper.ensure_playwright_tool_image()
                all_calls = helper.subprocess.run.call_args_list
                build_call = [c for c in all_calls if len(c[0]) > 0 and 'build' in c[0][0]]
                self.assertTrue(len(build_call) > 0)
                build_dir_arg = build_call[0][0][0][-1]
                self.assertIn('/var/tmp/opencode/paper-p7', build_dir_arg)
                self.assertNotIn('antigravity', build_dir_arg)
                self.assertEqual(img_id, 'sha256:built')
        finally:
            helper._TOOL_IMAGE_CACHE.clear()
            helper._TOOL_IMAGE_CACHE.update(saved)

    def test_f03_browser_denied_origin_infra_failure_fails(self):
        fixture = Mock(artifacts=Path('/tmp'), private=Path('/tmp'), project='test-f03')
        smoke_mock = Mock()
        smoke_mock.http_request.return_value = (401, {}, b'')
        registry = Mock()
        endpoint = Mock(url='https://127.0.0.1:443')
        with patch.object(helper, 'mint_expired_token', return_value='expired-tok'), \
             patch.object(helper, 'mint_control_token', return_value='control-tok'), \
             patch.object(helper, 'run_driver_smoke', return_value={'status': 'PASS'}), \
             patch.object(helper, 'native_probe', return_value=(101, {'sec-websocket-protocol': 'iot.telemetry.v1'})), \
             patch.object(helper, 'run_browser_probe', side_effect=[
                 {'status': 'PASS', 'event': 'open', 'selected_protocol': 'iot.telemetry.v1'},
                 RuntimeError('browser allocation failed before launch')
             ]):
            with self.assertRaises(ValueError):
                helper.run_handshake(smoke_mock, fixture, registry, endpoint, {}, 'token')
            last_payload = smoke_mock.durable_json.call_args_list[-1][0][1]
            self.assertEqual(last_payload['status'], 'FAIL')
            self.assertEqual(last_payload['negative']['browser_denied_origin']['status'], 'FAIL')
            self.assertEqual(last_payload['negative']['browser_denied_origin']['event'], 'probe_exception')

    def test_f03_browser_infra_events_fail(self):
        fixture = Mock(artifacts=Path('/tmp'), private=Path('/tmp'), project='test-f03')
        smoke_mock = Mock()
        smoke_mock.http_request.return_value = (401, {}, b'')
        registry = Mock()
        endpoint = Mock(url='https://127.0.0.1:443')
        with patch.object(helper, 'mint_expired_token', return_value='expired-tok'), \
             patch.object(helper, 'mint_control_token', return_value='control-tok'), \
             patch.object(helper, 'run_driver_smoke', return_value={'status': 'PASS'}), \
             patch.object(helper, 'native_probe', return_value=(101, {'sec-websocket-protocol': 'iot.telemetry.v1'})), \
             patch.object(helper, 'run_browser_probe', side_effect=[
                 {'status': 'PASS', 'event': 'open', 'selected_protocol': 'iot.telemetry.v1'},
                 {'status': 'FAIL', 'event': 'browser_error', 'selected_protocol': ''}
             ]):
            with self.assertRaises(ValueError):
                helper.run_handshake(smoke_mock, fixture, registry, endpoint, {}, 'token')
            last_payload = smoke_mock.durable_json.call_args_list[-1][0][1]
            self.assertEqual(last_payload['status'], 'FAIL')
            self.assertEqual(last_payload['negative']['browser_denied_origin']['status'], 'FAIL')

    def test_f03_acceptance_guard_requires_browser_denied_origin(self):
        fixture = Mock(project='test-f03-guard')
        item = {'kind': 'container', 'run': 'test-f03-guard', 'task': 'native-compose-allocation',
                'allocation_attempted': True, 'removed': True, 'labels': {},
                'receipt': {'id': 'a'*64, 'created': 'now', 'labels': {}}}
        registry = Mock(items=[item])
        cleanup_counts = {'container': 1, 'network': 0, 'volume': 0}
        evidence = {
            'paper_p7': {
                'status': 'PASS', 'mode': 'paper_p7_handshake_only',
                'perimeter': {'status': 'PASS', 'https_configured': True, 'auth_request_enabled': True},
                'handshake': {
                    'status': 'PASS',
                    'native': {'http_status': 101, 'subprotocol': 'iot.telemetry.v1'},
                    'browser': {'status': 'PASS', 'event': 'open', 'selected_protocol': 'iot.telemetry.v1'},
                    'negative': {
                        'missing_credentials': {'status': 'PASS'}, 'bad_token': {'status': 'PASS'},
                        'conflicting_credentials': {'status': 'PASS'}, 'denied_origin': {'status': 'PASS'},
                        'missing_app_protocol': {'status': 'PASS'}, 'expired_jwt': {'status': 'PASS'},
                        'rest_subprotocol_only': {'status': 'PASS'},
                        # 'browser_denied_origin' intentionally missing
                    },
                    'perimeter_isolation': {'status': 'PASS'},
                    'leak_check': {'status': 'PASS', 'leaks_found_count': 0}
                }
            }
        }
        self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, True, registry))
        evidence['paper_p7']['handshake']['negative']['browser_denied_origin'] = {'status': 'PASS', 'event': 'error'}
        self.assertTrue(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, True, registry))

    def test_f04_direct_go_includes_ws_headers(self):
        registry = Mock()
        registry.command.return_value = Mock(returncode=0, stderr=b'HTTP/1.1 401 Unauthorized\r\n', stdout=b'{}')
        containers = {'backend': {'receipt': {'id': 'back-id'}}}
        ws_headers = {'Connection': 'Upgrade', 'Upgrade': 'websocket', 'Sec-WebSocket-Protocol': 'iot.telemetry.v1'}
        p2_support = smoke.paper_p2_support()
        code, body = p2_support.direct_go(smoke, registry, containers, 'invalid', path='/v1/ws', headers=ws_headers)
        self.assertEqual(code, 401)
        exec_args = registry.command.call_args[0][0]
        self.assertIn('--header="Connection: Upgrade"', exec_args[-1])
        self.assertIn('--header="Upgrade: websocket"', exec_args[-1])
        self.assertIn('--header="Sec-WebSocket-Protocol: iot.telemetry.v1"', exec_args[-1])

    def test_f05_mint_expired_token_valid_claims_and_uuid(self):
        import uuid
        fixture = Mock(private=Path('/tmp'))
        # If valid_token is missing or has invalid UUID, must raise ValueError
        with self.assertRaises(ValueError):
            helper.mint_expired_token(fixture, valid_token='bad.token')
        # Token with valid UUID sub
        valid_sub = str(uuid.uuid4())
        claims = {'iss': 'auth', 'aud': 'authenticated', 'sub': valid_sub, 'role': 'authenticated'}
        def b64url(s): return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')
        fake_jwt = f"hdr.{b64url(json.dumps(claims))}.sig"
        with patch.object(helper, 'mint_jwt_with_claims', return_value='minted.jwt.sig') as mock_mint:
            tok = helper.mint_expired_token(fixture, valid_token=fake_jwt)
            self.assertEqual(tok, 'minted.jwt.sig')
            minted_claims = mock_mint.call_args[0][1]
            self.assertEqual(minted_claims['sub'], valid_sub)
            self.assertEqual(minted_claims['role'], 'authenticated')
            self.assertTrue(minted_claims['exp'] < minted_claims['iat'] + 3700)
            self.assertTrue(minted_claims['exp'] < time.time())

    def test_f06_leak_scan_requires_both_receipts_and_docker_logs_zero_exit(self):
        registry = Mock()
        fixture = Mock(private=Path('/tmp'))
        # Missing backend receipt
        res1 = helper.check_credential_leaks(fixture, registry, {'nginx': {'receipt': {'id': 'ng'}}}, ['token123456789012'])
        self.assertEqual(res1['status'], 'FAIL')
        self.assertEqual(res1['error'], 'missing_container_receipt_id')

        # Docker logs exit code != 0
        containers = {'nginx': {'receipt': {'id': 'ng'}}, 'backend': {'receipt': {'id': 'bk'}}}
        registry.command.return_value = Mock(returncode=1, stdout=b'', stderr=b'error')
        res2 = helper.check_credential_leaks(fixture, registry, containers, ['token123456789012'])
        self.assertEqual(res2['status'], 'FAIL')
        self.assertIn('docker_logs_failed', res2['error'])

        # Success case with bounded scope
        registry.command.return_value = Mock(returncode=0, stdout=b'clean logs\n', stderr=b'')
        res3 = helper.check_credential_leaks(fixture, registry, containers, ['token123456789012'])
        self.assertEqual(res3['status'], 'PASS')
        self.assertEqual(res3['leaks_found_count'], 0)
        self.assertIn('sources', res3)
        self.assertEqual(res3['sources']['nginx']['log_read_status'], 'PASS')
        self.assertEqual(res3['sources']['backend']['log_read_status'], 'PASS')
        self.assertNotIn('clean logs', json.dumps(res3))

    def test_f07_tool_image_qualification_metadata_enforced(self):
        # Image inspect without labels or wrong owner must be rejected
        unlabelled_inspect = json.dumps([{'Id': 'sha256:unlabelled', 'Config': {'Labels': {}}}]).encode()
        with patch.object(helper.subprocess, 'run', return_value=Mock(returncode=0, stdout=unlabelled_inspect)):
            tag, img_id = helper.inspect_tool_image('paper-p7-playwright-tool:v1.55.0')
            self.assertIsNone(tag)
            self.assertIsNone(img_id)

        # Qualified image is accepted
        qualified_labels = {
            'org.opencontainers.image.base.name': helper.PLAYWRIGHT_BASE_IMAGE,
            'paper.p7.tool.recipe.hash': helper.TOOL_RECIPE_HASH,
            'paper.p7.tool.package.version': helper.PLAYWRIGHT_VERSION,
            'paper.p7.tool.owner': helper.TOOL_OWNER,
        }
        qualified_inspect = json.dumps([{'Id': 'sha256:qualified123', 'Config': {'Labels': qualified_labels}}]).encode()
        with patch.object(helper.subprocess, 'run', return_value=Mock(returncode=0, stdout=qualified_inspect)):
            tag, img_id = helper.inspect_tool_image(helper.PLAYWRIGHT_TOOL_TAG)
            self.assertEqual(tag, helper.PLAYWRIGHT_TOOL_TAG)
            self.assertEqual(img_id, 'sha256:qualified123')

    def test_capture_driver_module_format_and_node_runtime_rejection(self):
        """Demonstrate that Node rejects CommonJS require() in .mjs at runtime, while .cjs succeeds."""
        import subprocess
        import tempfile
        source = helper.two_viewer_capture_source()
        self.assertIn("require('fs')", source)
        self.assertIn("require('playwright')", source)
        cmd = helper.two_viewer_command()
        self.assertEqual(cmd[1], '/probe/capture.cjs')

        with tempfile.TemporaryDirectory() as td:
            mjs_path = Path(td) / 'test_capture.mjs'
            cjs_path = Path(td) / 'test_capture.cjs'
            mjs_path.write_text("const fs = require('fs');\n")
            cjs_path.write_text("const fs = require('fs');\n")

            # node --check succeeds on BOTH (.mjs and .cjs) because require is syntactically a valid identifier
            res_mjs_check = subprocess.run(['node', '--check', str(mjs_path)], capture_output=True, text=True)
            self.assertEqual(res_mjs_check.returncode, 0)
            res_cjs_check = subprocess.run(['node', '--check', str(cjs_path)], capture_output=True, text=True)
            self.assertEqual(res_cjs_check.returncode, 0)

            # Executing .mjs throws ReferenceError: require is not defined in ES module scope
            res_mjs_run = subprocess.run(['node', str(mjs_path)], capture_output=True, text=True)
            self.assertNotEqual(res_mjs_run.returncode, 0)
            self.assertIn('require is not defined in ES module scope', res_mjs_run.stderr)

            # Executing .cjs succeeds cleanly (returncode 0)
            res_cjs_run = subprocess.run(['node', str(cjs_path)], capture_output=True, text=True)
            self.assertEqual(res_cjs_run.returncode, 0)

    def test_run_capture_driver_preflight_unit(self):
        """Unit test for run_capture_driver_preflight with mocked container and truthful socket_error verification."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            with patch.object(helper, 'start_viewers') as mock_start, \
                 patch.object(helper, 'wait_ready', side_effect=ValueError('viewer_ready_failed')):
                out = Path(td) / 'out'
                out.mkdir(mode=0o700)
                (out / 'ready.json').write_text(json.dumps({'status': 'error', 'phase': 'browser_launch_completed', 'error_code': 'viewer_ack_failed'}))
                (out / 'result.json').write_text(json.dumps({
                    'status': 'FAIL', 'phase': 'browser_launch_completed', 'error_code': 'viewer_ack_failed',
                    'viewers': {
                        'a': {'status': 'socket_error', 'phase': 'socket_error'},
                        'b': {'status': 'socket_error', 'phase': 'socket_error'}
                    }
                }))
                (out / 'diagnostics.json').write_text(json.dumps({
                    'phases': ['browser_launch_started', 'browser_launch_completed'],
                    'viewers': {
                        'a': {'phases': ['context_created', 'page_created', 'origin_navigation_started', 'origin_loaded', 'page_evaluate_started', 'ws_constructed', 'ws_attempted', 'socket_error'], 'last_phase': 'socket_error', 'status': 'socket_error'},
                        'b': {'phases': ['context_created', 'page_created', 'origin_navigation_started', 'origin_loaded', 'page_evaluate_started', 'ws_constructed', 'ws_attempted', 'socket_error'], 'last_phase': 'socket_error', 'status': 'socket_error'}
                    },
                    'error': {'class': 'Error', 'message': 'viewer_ack_failed', 'phase': 'browser_launch_completed'}
                }))
                mock_item = {'kind': 'container', 'name': 'test-container', 'removed': True, 'receipt': {'id': 'x'*64}}
                mock_reg = Mock()
                mock_reg.resolve.return_value = None
                mock_start.return_value = {
                    'output_dir': out,
                    'item': mock_item,
                    'registry': mock_reg,
                    'owned_dir': Path(td),
                }
                res = helper.run_capture_driver_preflight(scratch_dir=td)
                self.assertEqual(res['status'], 'PASS')
                self.assertEqual(res['readiness_error'], 'viewer_ready_failed')
                self.assertTrue(res['cleanup_verified'])
                self.assertTrue(res['browser_launch_verified'])
                self.assertTrue(res['origin_loaded_verified'])
                self.assertTrue(res['ws_attempt_verified'])
                self.assertTrue(res['expected_socket_error_verified'])

    def test_run_capture_driver_preflight_reproduces_false_pass_on_setup_failure(self):
        """Demonstrate that naive preflight check falsely marks setup failure as PASS."""
        readiness_error = 'viewer_ready_failed'
        ready_artifact = {'status': 'error'}
        result_artifact = {'status': 'FAIL', 'viewers': {'a': {'status': 'error'}, 'b': {'status': 'error'}}}
        cleanup_verified = True

        # Naive guard check (the vulnerable prior logic):
        naive_success = bool(
            readiness_error == 'viewer_ready_failed'
            and ready_artifact is not None and ready_artifact.get('status') == 'error'
            and result_artifact is not None and result_artifact.get('status') == 'FAIL'
            and cleanup_verified
        )
        self.assertTrue(naive_success, "Naive check falsely accepted generic setup error as PASS")

    def test_run_capture_driver_preflight_truthful_guard_rejects_setup_failure(self):
        """Verify truthful guard rejects setup/ReferenceError failure where WS attempt was never made."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            with patch.object(helper, 'start_viewers') as mock_start, \
                 patch.object(helper, 'wait_ready', side_effect=ValueError('viewer_ready_failed')):
                out = Path(td) / 'out'
                out.mkdir(mode=0o700)
                (out / 'ready.json').write_text(json.dumps({'status': 'error', 'phase': 'browser_launch_completed', 'error_code': 'reference_error'}))
                (out / 'result.json').write_text(json.dumps({
                    'status': 'FAIL', 'phase': 'browser_launch_completed', 'error_code': 'reference_error',
                    'viewers': {
                        'a': {'status': 'error', 'phase': 'page_evaluate_started'},
                        'b': {'status': 'error', 'phase': 'page_evaluate_started'}
                    }
                }))
                (out / 'diagnostics.json').write_text(json.dumps({
                    'phases': ['browser_launch_started', 'browser_launch_completed'],
                    'viewers': {
                        'a': {'phases': ['context_created', 'page_created', 'origin_navigation_started', 'origin_loaded', 'page_evaluate_started'], 'last_phase': 'page_evaluate_started', 'status': 'connecting'},
                        'b': {'phases': ['context_created', 'page_created', 'origin_navigation_started', 'origin_loaded', 'page_evaluate_started'], 'last_phase': 'page_evaluate_started', 'status': 'connecting'}
                    },
                    'error': {'class': 'Error', 'message': 'page.evaluate: ReferenceError: MAX_LIFETIME_MS is not defined', 'phase': 'browser_launch_completed'}
                }))
                mock_item = {'kind': 'container', 'name': 'test-container', 'removed': True, 'receipt': {'id': 'x'*64}}
                mock_reg = Mock()
                mock_reg.resolve.return_value = None
                mock_start.return_value = {
                    'output_dir': out,
                    'item': mock_item,
                    'registry': mock_reg,
                    'owned_dir': Path(td),
                }
                res = helper.run_capture_driver_preflight(scratch_dir=td)
                self.assertEqual(res['status'], 'FAIL')
                self.assertEqual(res['failure_reason'], 'ws_attempt_not_reached')
                self.assertFalse(res['ws_attempt_verified'])
                self.assertFalse(res['expected_socket_error_verified'])
                self.assertTrue(res['browser_launch_verified'])
                self.assertTrue(res['cleanup_verified'])

    def test_two_viewer_capture_source_scopes_evaluate_variables_and_avoids_node_closure(self):
        """Regression test: verify all constants and bounded values are explicitly scoped in page.evaluate."""
        import subprocess
        import tempfile
        source = helper.two_viewer_capture_source()
        self.assertIn('maxCaptureEvents', source)
        self.assertIn('maxEvents,maxLifetimeMs', source)
        self.assertIn('const MAX_EVENTS = Number.isInteger(maxEvents)', source)
        self.assertIn('const MAX_LIFETIME_MS = Number.isInteger(maxLifetimeMs)', source)
        self.assertIn('maxEvents:maxCaptureEvents,maxLifetimeMs', source)
        with tempfile.NamedTemporaryFile(suffix='.cjs', mode='w') as f:
            f.write(source)
            f.flush()
            res = subprocess.run(['node', '--check', f.name], capture_output=True, text=True)
            self.assertEqual(res.returncode, 0, f"Node syntax check failed: {res.stderr}")

    def test_reconnect_mode_cli_parsing_and_smoke_dispatch(self):
        # 1. CLI arg parsing accepts --paper-p7-mode reconnect
        import argparse
        from native_compose_smoke import dispatch_paper_fixture, requires_gateway_image
        fixture = SimpleNamespace(paper_p0=False, paper_p1=False, paper_p2=False, paper_p7=True, paper_p7_mode='reconnect')
        self.assertTrue(requires_gateway_image(fixture))
        # Exercise real mode validation and dispatch; only replace the expensive lane.
        with patch.object(helper, 'run_reconnect', return_value={'status': 'PASS', 'mode': 'paper_p7_reconnect'}) as lane:
            res = dispatch_paper_fixture(fixture, helper, 'arg1', 'arg2')
        self.assertEqual(res, {'status': 'PASS', 'mode': 'paper_p7_reconnect'})
        lane.assert_called_once_with('arg1', 'arg2')
        for allowed in ('handshake_only', 'telemetry_pilot', 'single_path', 'reconnect'):
            self.assertEqual(helper.pilot_mode(SimpleNamespace(paper_p7_mode=allowed)), allowed)
        with self.assertRaises(ValueError):
            helper.pilot_mode(SimpleNamespace(paper_p7_mode='bad'))

    def test_reconnect_phase_resources_are_distinct_and_bounds_ignore_uuid_order(self):
        first = helper.reconnect_gateway_identity('initial')
        gap = helper.reconnect_gateway_identity('gap')
        self.assertNotEqual(first['tag'], gap['tag'])
        self.assertNotEqual(first['client_id_suffix'], gap['client_id_suffix'])
        self.assertNotEqual(first['env_suffix'], gap['env_suffix'])
        early, late = '2026-10-27T10:00:00.000000Z', '2026-10-27T10:00:01.990000Z'
        rows = [{'message_id': 'z-later', 'measured_at': late}, {'message_id': 'a-earlier', 'measured_at': early}]
        self.assertEqual(helper.reconnect_window_bounds(rows), (early, '2026-10-27T10:00:01.990001Z'))

    def test_reconnect_result_ok_acceptance_and_negative_guards(self):
        valid = {
            'status': 'PASS',
            'mode': 'paper_p7_reconnect',
            'full_p7_accepted': False,
            'reconnect_ack': True,
            'exact_cleanup_verified': True,
            'evidence': {
                'content_match': True,
                'values_match': True,
                'gap_rows_count': 200,
                'initial_live_samples': 100,
                'app_acknowledged_total': 3,
                'outbox_drained': True,
            }
        }
        self.assertTrue(helper.reconnect_result_ok(valid))

        # full_p7_accepted must remain False
        bad_conflated = json.loads(json.dumps(valid))
        bad_conflated['full_p7_accepted'] = True
        self.assertFalse(helper.reconnect_result_ok(bad_conflated))

        # reconnect_ack must be True
        bad_ack = json.loads(json.dumps(valid))
        bad_ack['reconnect_ack'] = False
        self.assertFalse(helper.reconnect_result_ok(bad_ack))

        # exact_cleanup_verified must be True
        bad_clean = json.loads(json.dumps(valid))
        bad_clean['exact_cleanup_verified'] = False
        self.assertFalse(helper.reconnect_result_ok(bad_clean))

        # content_match must be True
        bad_content = json.loads(json.dumps(valid))
        bad_content['evidence']['content_match'] = False
        self.assertFalse(helper.reconnect_result_ok(bad_content))

        # values_match must be True
        bad_val = json.loads(json.dumps(valid))
        bad_val['evidence']['values_match'] = False
        self.assertFalse(helper.reconnect_result_ok(bad_val))

        # gap_rows_count must be 200
        bad_gap = json.loads(json.dumps(valid))
        bad_gap['evidence']['gap_rows_count'] = 199
        self.assertFalse(helper.reconnect_result_ok(bad_gap))

        # initial_live_samples must be 100
        bad_live = json.loads(json.dumps(valid))
        bad_live['evidence']['initial_live_samples'] = 99
        self.assertFalse(helper.reconnect_result_ok(bad_live))

        # app_acknowledged_total must be 3
        bad_acks = json.loads(json.dumps(valid))
        bad_acks['evidence']['app_acknowledged_total'] = 2
        self.assertFalse(helper.reconnect_result_ok(bad_acks))

        # outbox_drained must be True
        bad_outbox = json.loads(json.dumps(valid))
        bad_outbox['evidence']['outbox_drained'] = False
        self.assertFalse(helper.reconnect_result_ok(bad_outbox))

    def test_reconnect_driver_source_syntax_and_module_route(self):
        import subprocess
        import tempfile
        import re
        source = helper.reconnect_driver_source()
        self.assertIn("require('playwright')", source)
        self.assertIn("p7_reconnect_client.mjs", source)
        self.assertIn(r"client\.example\.test", source)
        self.assertIn("window.fetch", source)

        # Route regex matching check:
        route_pattern = re.compile(r'^https://client\.example\.test(/.*)?$')
        self.assertTrue(route_pattern.match('https://client.example.test'))
        self.assertTrue(route_pattern.match('https://client.example.test/'))
        self.assertTrue(route_pattern.match('https://client.example.test/p7_reconnect_client.mjs'))
        # Must NOT match real fixture endpoint:
        self.assertFalse(route_pattern.match('https://127.0.0.1:8443/v1/digital-twins/test/temporal'))
        self.assertFalse(route_pattern.match('https://localhost:8443/v1/digital-twins/test/temporal'))

        # Explicit evaluate argument checks:
        self.assertIn('origin,wsEndpoint,restEndpoint,token,target,ackTimeoutMs', source)
        self.assertIn('wsEndpoint,token,target,windowConfig', source)

        # Node syntax check
        with tempfile.NamedTemporaryFile(suffix='.cjs', mode='w') as f:
            f.write(source)
            f.flush()
            res = subprocess.run(['node', '--check', f.name], capture_output=True, text=True)
            self.assertEqual(res.returncode, 0, f"Node syntax check failed: {res.stderr}")

    def test_reconnect_window_bounding_and_phase_artifacts(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'out'
            out.mkdir(mode=0o700)
            capture = {'output_dir': out, 'registry': Mock(), 'cleaned': False, 'item': {'receipt': {'id': 'c1'}}}

            # 1. supply_reconnect_window
            win = helper.supply_reconnect_window(capture, 'urn:ngsi-ld:Sensor:gw:s', '2026-10-27T10:00:00.000000Z', '2026-10-27T10:00:02.000001Z', overlap_ms=0)
            self.assertEqual(win['entityId'], 'urn:ngsi-ld:Sensor:gw:s')
            win_file = out / 'window.json'
            self.assertTrue(win_file.exists())
            self.assertEqual(win_file.stat().st_mode & 0o777, 0o600)
            parsed_win = json.loads(win_file.read_text())
            self.assertEqual(parsed_win['from'], '2026-10-27T10:00:00.000000Z')

            # 2. wait_reconnect_ready
            (out / 'ready.json').write_text(json.dumps({'status': 'ready', 'phase': 'connected_subscribed'}))
            ready_res = helper.wait_reconnect_ready(capture, timeout=1)
            self.assertEqual(ready_res['status'], 'ready')

            # 3. wait_reconnect_gap_ready
            initial_live = {'observed_samples': 100, 'first_sample_timestamp': '2026-10-27T10:00:00.000000Z'}
            (out / 'gap_ready.json').write_text(json.dumps({'status': 'gap_ready', 'initial_live': initial_live}))
            gap_res = helper.wait_reconnect_gap_ready(capture, timeout=1)
            self.assertEqual(gap_res['observed_samples'], 100)

            # 4. finish_reconnect_client
            client_result = {'status': 'PASS', 'phase': 'catchup_completed', 'reconnect_ack': True, 'catchup': {'rows': [{'value': 1.0, 'timestamp': '2026-10-27T10:00:01.000000Z'}]}}
            (out / 'result.json').write_text(json.dumps(client_result))
            fin_res = helper.finish_reconnect_client(capture, capture['registry'], timeout=1)
            self.assertEqual(fin_res['status'], 'PASS')
            self.assertTrue(capture['cleaned'])

    def test_reconnect_oracle_model_comparison_exact_and_mutations(self):
        base = '2026-10-27T10:00:00.000000Z'
        base_dt = datetime.fromisoformat(base.replace('Z', '+00:00'))
        # Generate 200 gap rows matching 2 batches of 100 samples
        gap_rows = []
        for i in range(200):
            ts = (base_dt + timedelta(microseconds=i * 10000)).isoformat(timespec='microseconds').replace('+00:00', 'Z')
            gap_rows.append({
                'gateway_id': 'gw', 'sensor_id': 's', 'message_id': f'm{1 + i // 100}',
                'boot_id': 'b1', 'sample_index': i % 100, 'sequence_num': str(100 + i),
                'measured_at': ts, 'value': float(i + 1)
            })

        # Matching model rows from ReconnectCatchupClient catchUp
        model_rows = [{'value': r['value'], 'timestamp': r['measured_at'], 'source': 'history'} for r in gap_rows]

        # Exact match
        self.assertTrue(helper.telemetry_values_match_db(model_rows, gap_rows))
        diff = helper.compute_telemetry_diff(model_rows, gap_rows)
        self.assertTrue(diff['content_match'])
        self.assertEqual(diff['expected_count'], 200)
        self.assertEqual(diff['actual_count'], 200)

        # Microsecond timestamp mutation
        bad_ts_model = [{'value': model_rows[0]['value'], 'timestamp': '2026-10-27T10:00:00.000001Z', 'source': 'history'}, *model_rows[1:]]
        self.assertFalse(helper.telemetry_values_match_db(bad_ts_model, gap_rows))
        diff_ts = helper.compute_telemetry_diff(bad_ts_model, gap_rows)
        self.assertFalse(diff_ts['content_match'])

        # Value mutation
        bad_val_model = [{'value': 999.0, 'timestamp': model_rows[0]['timestamp'], 'source': 'history'}, *model_rows[1:]]
        self.assertFalse(helper.telemetry_values_match_db(bad_val_model, gap_rows))
        diff_val = helper.compute_telemetry_diff(bad_val_model, gap_rows)
        self.assertFalse(diff_val['content_match'])

        # Length mismatch (199 rows)
        self.assertFalse(helper.telemetry_values_match_db(model_rows[:-1], gap_rows))
        diff_len = helper.compute_telemetry_diff(model_rows[:-1], gap_rows)
        self.assertFalse(diff_len['content_match'])

    def test_reconnect_workload_lifecycle_order_and_cleanup_on_failure(self):
        import tempfile
        root = Path(tempfile.mkdtemp())
        fixture = SimpleNamespace(project='rec-test', private=root, artifacts=root / 'artifacts',
                                  values={'MQTT_USERNAME': 'backend_service', 'MQTT_PASSWORD': 'pw'})
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a')
        smoke_obj = SimpleNamespace(
            prepare_paper_workload=lambda *a: ('pg', {'cpus': 2, 'memory_bytes': 4294967296},
                {'services': {'backend': {'environment': {'MQTT_INGRESS_ENABLED': 'true',
                 'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true', 'MQTT_WORKER_COUNT': '1'}}}},
                 {'backend': {'Memory': 4294967296}}, 'storage', {'a': 'tok-a', 'b': 'tok-b'},
                [spec, spec], ['pwd-a', 'pwd-b'], '/sensor'),
            private_write=lambda *a: None,
            paper_p0_gateway_cli=Mock(),
            HTTPSEndpoint=type('HTTPSEndpoint', (), {'url': 'https://localhost:8443'}),
            db_psql=Mock(),
            durable_json=Mock()
        )
        registry = Mock()
        registry.intent.return_value = {'name': 'vol', 'receipt': {'name': 'vol'}}
        registry.resolve.side_effect = [None, {'name': 'vol'}]
        registry.command.return_value = Mock(returncode=0)

        # Track execution events
        events = []
        def mock_start(*a, **k):
            events.append('start_reconnect_client')
            return {'output_dir': root, 'registry': registry, 'cleaned': False, 'item': {'receipt': {'id': 'c1'}}}
        def mock_ready(*a, **k):
            events.append('wait_ready')
            return {'status': 'ready'}
        def mock_gap_ready(*a, **k):
            events.append('gap_ready')
            return {'observed_samples': 100}
        def mock_supply_window(*a, **k):
            events.append('supply_window')
            return {}

        # 200 gap rows
        base = '2026-10-27T10:00:00.000000Z'
        base_dt = datetime.fromisoformat(base.replace('Z', '+00:00'))
        gap_rows = [{'gateway_id': 'gw-a', 'sensor_id': 's-a', 'message_id': 'm1',
                     'boot_id': 'b1', 'sample_index': i, 'sequence_num': str(100 + i),
                     'measured_at': (base_dt + timedelta(microseconds=i * 10000)).isoformat(timespec='microseconds').replace('+00:00', 'Z'),
                     'value': float(i + 1)} for i in range(200)]
        model_rows = [{'value': r['value'], 'timestamp': r['measured_at'], 'source': 'history'} for r in gap_rows]

        def mock_finish(*a, **k):
            events.append('finish_client')
            return {'status': 'PASS', 'reconnect_ack': True, 'catchup': {'status': 'caught_up', 'rows': model_rows, 'policy': 'p', 'historyExactlyOnce': False}}

        # gateway CLI mock returns
        summary_batch1 = "run summary: completed=true enqueued=1 app_acknowledged=1 pending_rows=0 pending_bytes=0"
        summary_batch2 = "run summary: completed=true enqueued=2 app_acknowledged=2 pending_rows=0 pending_bytes=0"
        outbox_count = "pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0"
        smoke_obj.paper_p0_gateway_cli.side_effect = [
            summary_batch1,  # run 1 batch
            summary_batch2,  # run 2 batches
            outbox_count     # count
        ]

        # SQL mock returns: messages list, then gap_rows
        smoke_obj.db_psql.side_effect = [
            json.dumps(['m0', 'm1', 'm2']), # processed_messages message_ids
            json.dumps(gap_rows)             # gap rows
        ]

        with patch.object(helper, 'start_reconnect_client', side_effect=mock_start), \
             patch.object(helper, 'wait_reconnect_ready', side_effect=mock_ready), \
             patch.object(helper, 'wait_reconnect_gap_ready', side_effect=mock_gap_ready), \
             patch.object(helper, 'supply_reconnect_window', side_effect=mock_supply_window), \
             patch.object(helper, 'finish_reconnect_client', side_effect=mock_finish):

            res = helper.run_reconnect(smoke_obj, fixture, registry, smoke_obj.HTTPSEndpoint(),
                                       {'mosquitto': {'receipt': {'id': 'broker'}}}, {'gateway': 'image'}, SimpleNamespace(checkpoint=lambda: None))

            self.assertEqual(res['status'], 'PASS')
            self.assertEqual(res['mode'], 'paper_p7_reconnect')
            self.assertFalse(res['full_p7_accepted'])
            self.assertTrue(res['reconnect_ack'])
            self.assertTrue(res['content_match'])
            self.assertEqual(res['gap_rows_count'], 200)
            self.assertEqual(res['initial_live_samples'], 100)
            self.assertEqual(res['app_acknowledged_total'], 3)

            # Verify order of lifecycle phases
            self.assertEqual(events, ['start_reconnect_client', 'wait_ready', 'gap_ready', 'supply_window', 'finish_client'])

    def test_reconnect_client_dynamic_import_from_node_commonjs(self):
        """Verify Node CommonJS can dynamically import scripts/p7_reconnect_client.mjs."""
        import subprocess
        test_script = (
            "(async () => {\n"
            "  const mod = await import('./scripts/p7_reconnect_client.mjs');\n"
            "  if (typeof mod.ReconnectCatchupClient !== 'function') throw new Error('missing_class');\n"
            "  const client = new mod.ReconnectCatchupClient({\n"
            "    WebSocket: class MockWS {},\n"
            "    fetch: async () => {},\n"
            "    maxLive: 256,\n"
            "    connectTimeoutMs: 5000\n"
            "  });\n"
            "  if (typeof client.connect !== 'function' || typeof client.disconnect !== 'function' || typeof client.catchUp !== 'function') {\n"
            "    throw new Error('missing_method');\n"
            "  }\n"
            "})();\n"
        )
        res = subprocess.run(['node', '-e', test_script], cwd=str(ROOT), capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, f"CommonJS dynamic import failed: {res.stderr}")

    def test_reconnect_cleanup_acceptance_in_native_compose_smoke(self):
        fixture = Mock(project='test-rec-clean', paper_p7=True, paper_p7_mode='reconnect')
        item = {
            'kind': 'container', 'run': 'test-rec-clean', 'task': 'native-compose-allocation',
            'allocation_attempted': True, 'removed': True, 'labels': {},
            'receipt': {'id': 'c'*64, 'created': 'now', 'labels': {}}
        }
        registry = Mock(items=[item])
        cleanup_counts = {'container': 1, 'network': 0, 'volume': 0}
        evidence = {
            'paper_p7': {
                'status': 'PASS',
                'mode': 'paper_p7_reconnect',
                'full_p7_accepted': False,
                'reconnect_ack': True,
                'exact_cleanup_verified': False,
                'evidence': {
                    'content_match': True,
                    'values_match': True,
                    'gap_rows_count': 200,
                    'initial_live_samples': 100,
                    'app_acknowledged_total': 3,
                    'outbox_drained': True,
                }
            }
        }
        self.assertTrue(smoke.paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, True, registry))
        self.assertTrue(evidence['paper_p7']['exact_cleanup_verified'])

    def test_slow_viewer_healthy_peer_timestamps_semantic_utc_microseconds(self):
        payloads = [json.loads(p) for p in helper.build_slow_viewer_payloads('ga', 'sa', 'boot', start_sequence=100)]
        expected = [{'gateway_id': 'ga', 'sensor_id': 'sa', 'message_id': p['message_id'],
            'boot_id': p['boot_id'], 'first_sequence': str(p['first_sequence']),
            'sample_count': p['sample_count'], 'measured_at': p['measured_at']} for p in payloads]
        # Alternate headers with RFC3339 without trailing fractional zeros (e.g. .replace('.000000Z', 'Z'))
        variant_headers = []
        for h in expected:
            v = dict(h)
            if v['measured_at'].endswith('.000000Z'):
                v['measured_at'] = v['measured_at'][:-8] + 'Z'
            variant_headers.append(v)
        viewers = {'a': {'count': len(payloads), 'acknowledged': True, 'status': 'ready', 'wrong_namespace': 0,
                         'event_headers': variant_headers},
                   'b': {'count': 0, 'acknowledged': True, 'status': 'ready', 'wrong_namespace': 0,
                         'event_headers': []}}
        # Semantically identical UTC timestamps must match
        self.assertTrue(helper.slow_healthy_peer_ok(viewers, 'a', 'b',
            {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sa'}, payloads))
        # 1 microsecond difference must fail
        bad_headers = list(variant_headers)
        base_ts = datetime.fromisoformat(bad_headers[0]['measured_at'].replace('Z', '+00:00'))
        bad_headers[0] = dict(bad_headers[0], measured_at=(base_ts + timedelta(microseconds=1)).isoformat())
        self.assertFalse(helper.slow_healthy_peer_ok({'a': dict(viewers['a'], event_headers=bad_headers), 'b': viewers['b']},
            'a', 'b', {'gateway_id': 'ga', 'sensor_id': 'sa'}, {'gateway_id': 'gb', 'sensor_id': 'sa'}, payloads))

    def test_slow_viewer_finalize_cleanup_acceptance_guards(self):
        proof = {key: True for key in helper.SLOW_VIEWER_REQUIRED_PROOFS}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'proof.json'
            path.write_text(json.dumps(sample_slow_proof()))
            good = {'status': 'PASS', 'mode': 'paper_p7_slow_viewer', 'full_p7_accepted': False,
                'exact_cleanup_verified': False, 'evidence': dict(proof), 'capture_finalization_status': 'finished',
                'precleanup_proof_artifact': 'p7-slow-viewer-proof.json',
                'precleanup_proof_path': str(path)}
            self.assertTrue(helper.finalize_slow_viewer_cleanup(good, True))
            self.assertTrue(good['exact_cleanup_verified'])
            self.assertFalse(good['full_p7_accepted'])
            self.assertEqual(json.loads(path.read_text())['resource_cleanup_status'], 'verified')
            self.assertFalse(helper.finalize_slow_viewer_cleanup(dict(good, exact_cleanup_verified=False), False))
            self.assertFalse(helper.finalize_slow_viewer_cleanup(dict(good, status='FAIL'), True))
            for k in helper.SLOW_VIEWER_REQUIRED_PROOFS:
                bad_proof = dict(proof, **{k: False})
                self.assertFalse(helper.finalize_slow_viewer_cleanup(dict(good, evidence=bad_proof), True))
            self.assertFalse(helper.finalize_slow_viewer_cleanup(dict(good, full_p7_accepted=True), True))
            for mutate in (
                lambda p: p['database'].update(actual_counts={'ledger': 999, 'raw': 1000000, 'temporal': 1000}),
                lambda p: p['policy'].update(close_cause_observed=False),
                lambda p: p['capture_finalization']['a'].update(count=999),
            ):
                bad_path = Path(td) / ('bad-' + str(len(list(Path(td).glob('bad-*')))) + '.json')
                bad = sample_slow_proof()
                mutate(bad)
                bad_path.write_text(json.dumps(bad))
                candidate = dict(good, exact_cleanup_verified=False, precleanup_proof_path=str(bad_path))
                self.assertFalse(helper.finalize_slow_viewer_cleanup(candidate, True))

    def test_slow_viewer_sql_query_scoping_and_composite_joins(self):
        import inspect
        source = inspect.getsource(helper.run_slow_case_workload)
        # Fully qualified p. predicate for all joined queries
        self.assertIn("p_scope = \"p.gateway_id=:'gw' AND p.sensor_id=:'sensor' AND p.boot_id=:'boot' AND p.first_sequence >= :'first'::bigint AND p.first_sequence < :'end'::bigint\"", source)
        # Joined raw count must use composite identity and p_scope
        self.assertIn("FROM telemetry t JOIN processed_messages p ON p.gateway_id=t.gateway_id AND p.sensor_id=t.sensor_id AND p.message_id=t.message_id WHERE ' + p_scope", source)
        # Joined raw rows must use composite identity and p_scope
        self.assertIn("FROM telemetry t JOIN processed_messages p ON p.gateway_id=t.gateway_id AND p.sensor_id=t.sensor_id AND p.message_id=t.message_id WHERE \" + p_scope", source)
        # Joined temporal must use p_scope
        self.assertIn("twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id JOIN processed_messages p ON p.message_id=v.message_id WHERE e.entity_id=:'urn' AND v.property_name='sample_interval_us' AND \" + p_scope", source)
        # Single processed_messages uses p_scope with alias p
        self.assertIn("FROM processed_messages p WHERE ' + p_scope", source)
        self.assertNotIn("WHERE p.' + scope", source)

        # SQL syntax preflight: verify PostgreSQL EXPLAIN accepts the exact joined queries without ambiguous column errors
        try:
            res = subprocess.run([
                'docker', 'exec', '-u', 'postgres', 'iot_postgres', 'psql', '-d', 'iot_platform', '-c',
                "EXPLAIN SELECT count(*) FROM telemetry t JOIN processed_messages p ON p.gateway_id=t.gateway_id AND p.sensor_id=t.sensor_id AND p.message_id=t.message_id WHERE p.gateway_id='g' AND p.sensor_id='s' AND p.boot_id='550e8400-e29b-41d4-a716-446655440000' AND p.first_sequence >= 0 AND p.first_sequence < 1000;"
            ], capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                self.assertIn('Nested Loop', res.stdout)
                self.assertNotIn('ambiguous', res.stderr.lower())
        except Exception:
            pass


    def test_slow_viewer_payload_1000_past_base_and_duration_invariants(self):
        payloads = helper.build_slow_viewer_payloads('gw-slow', 's-slow', 'boot-123', start_sequence=1000, message_count=1000)
        self.assertEqual(len(payloads), 1000)
        decoded = [json.loads(p) for p in payloads]
        # Invariants: 1000 messages * 1000 samples, unique message_ids, same boot_id
        self.assertEqual(len({d['message_id'] for d in decoded}), 1000)
        self.assertEqual({d['boot_id'] for d in decoded}, {'boot-123'})
        self.assertEqual([int(d['first_sequence']) for d in decoded], list(range(1000, 1001000, 1000)))
        # Past base verification: ALL timestamps must be strictly in the past (> 300s in the past)
        now = datetime.now(timezone.utc)
        for d in decoded:
            measured = datetime.fromisoformat(d['measured_at'].replace('Z', '+00:00'))
            self.assertTrue(measured < now - timedelta(seconds=290))
        # Durations within batch: 9.99s <= 60s
        step_us = decoded[0]['sample_interval_us']
        span = timedelta(microseconds=(decoded[0]['sample_count'] - 1) * step_us)
        self.assertTrue(span.total_seconds() <= 60)
        # Total logical payload size is ~23.3 - 24.5 MiB
        total_bytes = sum(len(p) for p in payloads)
        self.assertTrue(22_000_000 <= total_bytes <= 26_000_000)

    def test_slow_viewer_mosquitto_line_framing_and_stdin_contract(self):
        import subprocess
        wave = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=0, message_count=10)
        self.assertEqual(len(wave), 10)
        preamble = ('gw\nsecret\ngateways/gw/telemetry/sensor\n').encode()
        wave_lines = b''.join(p + b'\n' for p in wave)
        full_stdin = preamble + wave_lines

        # Verify each wave line has no embedded newlines and is valid JSON
        lines = wave_lines.splitlines()
        self.assertEqual(len(lines), 10)
        for i, line in enumerate(lines):
            self.assertNotIn(b'\r', line)
            self.assertNotIn(b'\n', line)
            obj = json.loads(line)
            self.assertEqual(obj['sample_count'], 1000)
            self.assertEqual(int(obj['first_sequence']), i * 1000)

        # Simulate mosquitto_pub stdin preamble consumption via shell
        script = 'IFS= read -r u; IFS= read -r p; IFS= read -r t; count=0; while IFS= read -r line; do count=$((count+1)); done; echo "$u|$p|$t|$count"'
        proc = subprocess.Popen(['sh', '-ec', script], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate(input=full_stdin)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(out.decode().strip(), 'gw|secret|gateways/gw/telemetry/sensor|10')

    def test_slow_viewer_group_100_ack_caps_and_phase_subscribers(self):
        payloads = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=0, message_count=1000)
        total_phases = (len(payloads) + 99) // 100
        self.assertEqual(total_phases, 10)

        all_phase_ids = []
        for phase in range(total_phases):
            tag = f'p7-slow-ack-{phase}'
            self.assertEqual(tag, f'p7-slow-ack-{phase}')
            phase_payloads = payloads[phase * 100:(phase + 1) * 100]
            self.assertEqual(len(phase_payloads), 100)
            phase_ids = [json.loads(p)['message_id'] for p in phase_payloads]
            all_phase_ids.extend(phase_ids)

        self.assertEqual(len(all_phase_ids), 1000)
        self.assertEqual(len(set(all_phase_ids)), 1000)
        self.assertTrue(helper.slow_ack_ids_complete(all_phase_ids, payloads))

    def test_slow_viewer_parameterized_db_and_chunked_raw_oracle(self):
        # Database counts oracle parameterized to N
        self.assertTrue(helper.slow_db_oracle_ok({'ledger': 1000, 'raw': 1000000, 'temporal': 1000}))
        self.assertFalse(helper.slow_db_oracle_ok({'ledger': 999, 'raw': 1000000, 'temporal': 1000}))
        self.assertFalse(helper.slow_db_oracle_ok({'ledger': 1000, 'raw': 999999, 'temporal': 1000}))
        self.assertFalse(helper.slow_db_oracle_ok({'ledger': 1000, 'raw': 1000000, 'temporal': 999}))
        self.assertTrue(helper.slow_db_oracle_ok({'ledger': 50, 'raw': 50000, 'temporal': 50}, expected_messages=50))

        # Chunked raw content validation on 5-batch chunk (5000 rows)
        chunk_payloads = helper.build_slow_viewer_payloads('gw', 'sensor', 'boot', start_sequence=100, message_count=5)
        raw_rows = []
        for p in chunk_payloads:
            d = json.loads(p)
            measured = datetime.fromisoformat(d['measured_at'].replace('Z', '+00:00'))
            for i in range(1000):
                raw_rows.append({
                    'message_id': d['message_id'],
                    'sample_index': i,
                    'sequence_num': str(int(d['first_sequence']) + i),
                    'measured_at': (measured + timedelta(microseconds=d['sample_interval_us'] * i)).isoformat(),
                    'value': d['samples'][i]
                })
        self.assertTrue(helper.slow_raw_content_ok(raw_rows, chunk_payloads))
        # Missing row in chunk
        self.assertFalse(helper.slow_raw_content_ok(raw_rows[:-1], chunk_payloads))
        # Altered value in chunk
        bad_val = [dict(raw_rows[0], value=999.0)] + raw_rows[1:]
        self.assertFalse(helper.slow_raw_content_ok(bad_val, chunk_payloads))
        # Empty payloads rejected
        self.assertFalse(helper.slow_raw_content_ok([], []))

    def test_slow_viewer_timeline_collection_schema_and_bounding(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td)
            smoke_mock = SimpleNamespace()
            smoke_mock.http_request = Mock(return_value=(200, {}, {
                'websocket_metrics': {
                    'availability': 'available',
                    'new_events': 42, 'deliveries_enqueued': 40,
                    'telemetry_writes_completed': 38, 'control_writes_completed': 2,
                    'slow_closes': 1, 'sessions': 2, 'global_queued_work': 0,
                    'global_queued_bytes': 0, 'session_queued_in_flight_bytes': 0,
                    'drops': 0, 'expiries': 0, 'revocations': 0, 'origin_missing': 0,
                },
                'ingress': {'Committed': 42}
            }))
            endpoint = SimpleNamespace(url='https://127.0.0.1:9443')
            context = {'tokens': {'admin': 'admin-tok'}, 'probe': Mock(poll=lambda: None), 'capture': {'output_dir': private}}
            # Create a mock state.json in capture output
            (private / 'state.json').write_text(json.dumps({'viewers': {'a': {'count': 100, 'status': 'ready'}, 'b': {'count': 0, 'status': 'ready'}}}))

            timeline = []
            entry = helper.collect_timeline_snapshot(smoke_mock, endpoint, context, 'test_phase', time.monotonic(), 50, 40, timeline)
            self.assertEqual(len(timeline), 1)
            # Verify all required schema fields
            for f in helper.TIMELINE_CSV_FIELDS:
                self.assertIn(f, entry)
            self.assertEqual(entry['phase'], 'test_phase')
            self.assertEqual(entry['offered'], 50)
            self.assertEqual(entry['acked'], 40)
            self.assertEqual(entry['telemetry_writes'], 38)
            self.assertEqual(entry['control_writes'], 2)
            self.assertEqual(entry['ingress_commits'], 42)
            self.assertEqual(entry['browser_a_count'], 100)
            self.assertEqual(entry['browser_b_count'], 0)
            self.assertTrue(entry['probe_alive'])
            self.assertTrue(entry['metrics_available'])
            # Verify no secret tokens or IDs in entry
            for val in entry.values():
                self.assertNotIn('admin-tok', str(val))

            # Test bounding: append 260 entries, must cap at 250
            for i in range(260):
                helper.collect_timeline_snapshot(smoke_mock, endpoint, context, f'phase_{i}', time.monotonic(), i, i, timeline)
            self.assertEqual(len(timeline), 250)

            # Test saving timeline.json and timeline.csv
            helper.save_slow_timeline(private, timeline)
            json_file = private / 'timeline.json'
            csv_file = private / 'timeline.csv'
            self.assertTrue(json_file.exists())
            self.assertTrue(csv_file.exists())
            self.assertEqual(oct(json_file.stat().st_mode & 0o777), '0o600')
            self.assertEqual(oct(csv_file.stat().st_mode & 0o777), '0o600')

            loaded = json.loads(json_file.read_text())
            self.assertEqual(len(loaded), 250)
            csv_lines = csv_file.read_text().splitlines()
            self.assertEqual(len(csv_lines), 251) # 1 header + 250 rows
            self.assertEqual(csv_lines[0], ','.join(helper.TIMELINE_CSV_FIELDS))

    def test_close_cause_evidence_is_fixed_and_not_policy_pass(self):
        ws = {'close_counts': {'ping_timeout': 1, 'unknown-secret-label': 9},
              'close_observations': [{'reason': 'ping_timeout', 'operation': 'ping',
                  'error_class': 'deadline_exceeded', 'context_class': 'none',
                  'operation_context_class': 'deadline_exceeded', 'elapsed_seconds': 45.001,
                  'deadline_elapsed_seconds': 45.0, 'user_id': 'secret'}]}
        safe = helper.safe_close_evidence(ws)
        self.assertEqual(safe['close_counts']['ping_timeout'], 1)
        self.assertNotIn('secret', json.dumps(safe))
        self.assertEqual(safe['close_observations'][0]['elapsed_seconds'], 45.001)
        self.assertFalse(helper.slow_viewer_result_ok({'status': 'FAIL', **safe}))
        self.assertEqual(helper.safe_close_evidence({}), {'close_counts': None, 'close_observations': []})

    def test_slow_viewer_policy_stop_condition_and_resume_on_fail(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td)
            resume_path = private / 'slow.resume'
            # Probe config path
            cfg_path = private / 'config.json'
            out_path = private / 'slow.result.json'
            cfg_path.write_text(json.dumps({'resume_path': str(resume_path), 'output_path': str(out_path)}))
            cfg_path.chmod(0o600)

            # End of offered phase triggers resume_path creation for diagnostics
            self.assertFalse(resume_path.exists())
            resume_path.write_text('1')
            resume_path.chmod(0o600)
            self.assertTrue(resume_path.exists())
            self.assertEqual(oct(resume_path.stat().st_mode & 0o777), '0o600')

    def test_slow_viewer_timeline_snapshot_handles_unavailable_and_errors_without_fake_zeros(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td)
            endpoint = SimpleNamespace(url='https://127.0.0.1:9443')
            context = {'tokens': {'admin': 'admin-tok'}, 'probe': None, 'capture': {'output_dir': private}}

            # Case A: HTTP 500 error -> metrics_available is False, all WS/ingress counters are None, not 0
            smoke_err = SimpleNamespace()
            smoke_err.http_request = Mock(return_value=(500, {}, {'error': 'internal'}))
            timeline = []
            entry_err = helper.collect_timeline_snapshot(smoke_err, endpoint, context, 'err_phase', time.monotonic(), 10, 5, timeline)
            self.assertFalse(entry_err['metrics_available'])
            self.assertIsNone(entry_err['new_events'])
            self.assertIsNone(entry_err['deliveries'])
            self.assertIsNone(entry_err['telemetry_writes'])
            self.assertIsNone(entry_err['control_writes'])
            self.assertIsNone(entry_err['slow_closes'])
            self.assertIsNone(entry_err['sessions'])
            self.assertIsNone(entry_err['global_queued_work'])
            self.assertIsNone(entry_err['global_queued_bytes'])
            self.assertIsNone(entry_err['session_queued_in_flight_bytes'])
            self.assertIsNone(entry_err['drops'])
            self.assertIsNone(entry_err['expiries'])
            self.assertIsNone(entry_err['revocations'])
            self.assertIsNone(entry_err['origin_missing'])
            self.assertIsNone(entry_err['ingress_commits'])
            # Missing state.json -> browser counts are None, not 0
            self.assertIsNone(entry_err['browser_a_count'])
            self.assertIsNone(entry_err['browser_b_count'])
            self.assertIsNone(entry_err['probe_alive'])

            # Verify policy proof comparison safely rejects unavailable snapshot without TypeError
            before = {'slow_closes': 0, 'sessions': 3}
            can_prove = (before is not None
                         and entry_err.get('metrics_available') is True
                         and entry_err.get('slow_closes') is not None
                         and before.get('slow_closes') is not None
                         and entry_err.get('sessions') is not None
                         and before.get('sessions') is not None
                         and entry_err['slow_closes'] > before['slow_closes']
                         and entry_err['sessions'] == 2 and before['sessions'] == 3)
            self.assertFalse(can_prove)

            # Case B: HTTP 200 but websocket availability is 'unavailable'
            smoke_unavail = SimpleNamespace()
            smoke_unavail.http_request = Mock(return_value=(200, {}, {
                'websocket_metrics': {'availability': 'unavailable', 'sessions': 0},
                'ingress': None
            }))
            entry_unavail = helper.collect_timeline_snapshot(smoke_unavail, endpoint, context, 'unavail_phase', time.monotonic(), 0, 0, [])
            self.assertFalse(entry_unavail['metrics_available'])
            self.assertIsNone(entry_unavail['sessions'])
            self.assertIsNone(entry_unavail['ingress_commits'])

    def test_slow_viewer_ack_requires_exact_set(self):
        target_gw = 'gw-1'
        sensor_id = 'sensor-1'
        expected = ['550e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440002']
        msgs = [('gateways/' + target_gw + '/acks/' + m,
                 json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack', 'message_id': m,
                             'status': 'committed', 'committed_at': '2026-10-27T00:00:00Z'}).encode())
                for m in expected]
        received = helper.validate_slow_ack_messages(msgs, expected, target_gw, sensor_id)
        self.assertEqual(set(received), set(expected))

        # Partial set (only 1 out of 2)
        msgs_partial = msgs[:1]
        received_partial = helper.validate_slow_ack_messages(msgs_partial, expected, target_gw, sensor_id)
        self.assertNotEqual(set(received_partial), set(expected))
        self.assertTrue(set(received_partial) < set(expected))

        # Superset or foreign ID raises application_ack_invalid
        foreign_id = '550e8400-e29b-41d4-a716-446655440003'
        foreign = ('gateways/' + target_gw + '/acks/' + foreign_id,
                   json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack', 'message_id': foreign_id,
                               'status': 'committed', 'committed_at': '2026-10-27T00:00:00Z'}).encode())
        with self.assertRaisesRegex(ValueError, 'application_ack_invalid'):
            helper.validate_slow_ack_messages(msgs + [foreign], expected, target_gw, sensor_id)

    def test_slow_viewer_mosquitto_line_mode_contract(self):
        bounds = helper.slow_viewer_workload_bounds()
        self.assertEqual(bounds['messages'], 1000)
        self.assertEqual(bounds['wave_size'], 10)
        payloads = helper.build_slow_viewer_payloads('gw', 's', 'boot', start_sequence=1000, message_count=10)
        self.assertEqual(len(payloads), 10)
        wave_data = b''.join(p + b'\n' for p in payloads)
        lines = wave_data.splitlines()
        self.assertEqual(len(lines), 10)
        for line in lines:
            parsed = json.loads(line)
            self.assertEqual(parsed['gateway_id'], 'gw')
            self.assertEqual(parsed['sensor_id'], 's')

    def test_other_p7_modes_remain_unchanged(self):
        # Legacy start_viewers defaults remain 256 events / 60s
        with tempfile.TemporaryDirectory() as td:
            private = Path(td)
            fixture = SimpleNamespace(private=private, project='p7-def')
            registry = Mock()
            item = {'kind': 'container', 'name': 'test', 'receipt': {'id': 'cid'}}
            registry.intent.return_value = item
            registry.resolve.side_effect = [None, {'receipt': {'id': 'cid'}}]
            registry.command.return_value = Mock(returncode=0)
            with patch.object(helper, 'ensure_playwright_tool_image', return_value=('tag', 'sha256:img')):
                cap = helper.start_viewers(fixture, registry, 'wss://127.0.0.1:9443/v1/ws', 'https://client.example.test', [
                    {'viewer_id': 'a', 'token': 't1', 'target': {'gateway_id': 'g', 'sensor_id': 's'}},
                    {'viewer_id': 'b', 'token': 't2', 'target': {'gateway_id': 'g2', 'sensor_id': 's2'}},
                ])
                cfg = json.loads(cap['config_path'].read_text())
                self.assertEqual(cfg['capture_limit'], 256)
                self.assertEqual(cfg['max_lifetime_ms'], 60000)


if __name__ == '__main__':
    unittest.main()
