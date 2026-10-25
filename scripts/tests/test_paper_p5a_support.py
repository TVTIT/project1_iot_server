import json
import re
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs

import paper_p5a_support as p5a
import native_compose_smoke as smoke


class PaperP5aWorkloadContractTests(unittest.TestCase):
    def test_shutdown_smoke_isolated_to_on_one_profile_and_uses_bounded_wait(self):
        ordinary = p5a.profile('on', '1')
        shutdown = p5a.profile('on', '1', shutdown_smoke=True)
        self.assertEqual(ordinary['MQTT_MICROBATCH_MAX_WAIT'], '1s')
        self.assertEqual(shutdown['MQTT_MICROBATCH_MAX_WAIT'], '2s')
        self.assertEqual({k: v for k, v in ordinary.items() if k != 'MQTT_MICROBATCH_MAX_WAIT'},
                         {k: v for k, v in shutdown.items() if k != 'MQTT_MICROBATCH_MAX_WAIT'})
        self.assertTrue(p5a.run_metadata('on', '1', shutdown_smoke=True)['shutdown_smoke'])
        self.assertNotIn('shutdown_smoke', p5a.run_metadata('on', '1'))
        with self.assertRaisesRegex(ValueError, '^paper_p5a_shutdown_profile_rejected$'):
            p5a.profile('off', '1', shutdown_smoke=True)
        with self.assertRaisesRegex(ValueError, '^paper_p5a_shutdown_profile_rejected$'):
            p5a.profile('on', '100', shutdown_smoke=True)

    def test_observers_fit_real_bounded_helper_and_keep_telemetry_principal(self):
        observers = p5a.observer_specs(p5a.workload_config('100'))
        self.assertEqual(len(observers), 2)
        self.assertEqual(len({item['tag'] for item in observers}), 2)
        self.assertTrue(all(item['kind'] == 'telemetry' for item in observers))
        self.assertTrue(all(1 <= item['timeout'] <= 90 for item in observers))
        self.assertTrue(all(item['count'] == 24 for item in observers))
        self.assertTrue(all(re.fullmatch(r'[a-z0-9-]{1,32}', item['tag']) for item in observers))
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p1='baseline')
            spec = type('Spec', (), {'gateway_id': 'gw_p5a'})()
            for item in observers:
                username, password, topic = smoke.paper_p1_subscription_target(
                    fixture, spec, 'gateway-password', item['tag'], kind=item['kind'])
                self.assertEqual((username, password, topic),
                    (fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'],
                     'gateways/+/telemetry/#'))
                command = smoke.paper_p1_subscription_command(
                    'broker', item['tag'], topic, message_count=item['count'],
                    message_timeout=item['timeout'])
                self.assertIn('-i p1-observer-' + item['tag'] + '-', command[6])
                self.assertIn('-W ' + str(item['timeout']), command[6])
            with self.assertRaisesRegex(ValueError, '^paper_p1_subscription_failed$'):
                smoke.paper_p1_subscriber(
                    fixture, None, 'broker', spec, 'pw', 'telemetry',
                    message_count=12, message_timeout=150)

    def test_history_request_uses_measured_window_and_backend_query_contract(self):
        first = '2026-10-25T10:00:00.123456Z'
        last = '2026-10-25T10:00:11.123456Z'
        path = p5a.temporal_read_path(smoke, 'urn:ngsi-ld:Sensor:gw:sensor', first, last, 1200)
        self.assertIn('/v1/digital-twins/urn:ngsi-ld:Sensor:gw:sensor/temporal?', path)
        query = parse_qs(path.split('?', 1)[1])
        self.assertEqual(query['attrs'], ['measurement'])
        self.assertEqual(query['max_points'], ['1200'])
        self.assertEqual(query['timerel'], ['between'])
        self.assertEqual(query['timeAt'], [first])
        self.assertEqual(query['endTimeAt'], [last])
        self.assertTrue(p5a.temporal_count_matches({'measurement': {'values': [[1], [2]]}}, 2))
        self.assertFalse(p5a.temporal_count_matches({'measurement': {'values': [[1]]}}, 2))

    def test_temporal_read_windows_partition_actual_bounds_under_raw_window_cap(self):
        first = '2026-10-25T10:00:00.123456+00:00'
        last = '2026-10-25T10:00:11.123456+00:00'
        windows = p5a.temporal_read_windows(first, last)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][0].isoformat(), '2026-10-25T10:00:00.123456+00:00')
        self.assertEqual(windows[0][1], windows[1][0])
        self.assertTrue(all(end - start <= p5a.TEMPORAL_RAW_WINDOW
                            for start, end in windows))
        self.assertEqual(windows[-1][1].isoformat(), '2026-10-25T10:00:11.123457+00:00')
        path = p5a.temporal_read_path(smoke, 'urn:ngsi-ld:Sensor:gw:sensor',
                                      *windows[0], 1200)
        query = parse_qs(path.split('?', 1)[1])
        self.assertEqual(query['timeAt'], ['2026-10-25T10:00:00.123456Z'])
        self.assertEqual(query['endTimeAt'], ['2026-10-25T10:00:09.123456Z'])

    def test_history_point_counts_use_raw_samples_and_bounded_explicit_limits(self):
        for dataset, expected, limit in (('100', 1200, 1200), ('1', 12, 100)):
            config = p5a.workload_config(dataset)
            self.assertEqual(p5a.expected_measurement_points(config), expected)
            self.assertEqual(p5a.history_max_points(config), limit)
            self.assertGreaterEqual(limit, expected)
            self.assertLessEqual(limit, 2000)

    def test_read_diagnostics_are_safe_and_persisted_before_failure(self):
        run_data = {'history_reads': []}
        persisted = []

        def persist():
            persisted.append(json.loads(json.dumps(run_data)))

        result = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 503, {'private': 'must-not-leak'},
            200, {'measurement': {'values': [[1], [2]]}}, expected_points=1200,
            max_points=1200, first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:11Z',
            query_windows=[{'timeAt': '2026-10-25T10:00:00Z', 'http_status': 200}])
        self.assertFalse(result['accepted'])
        with self.assertRaisesRegex(ValueError, '^paper_p5a_read_regression_failed$'):
            p5a.persist_history_read_diagnostic(run_data, result, persist)
        self.assertEqual(len(persisted), 1)
        saved = json.dumps(persisted[0])
        self.assertIn('http_status', saved)
        self.assertIn('expected_points', saved)
        self.assertIn('actual_points', saved)
        self.assertIn('max_points', saved)
        self.assertIn('2026-10-25T10:00:00Z', saved)
        self.assertIn('"windows"', saved)
        self.assertNotIn('must-not-leak', saved)
        self.assertNotIn('private', saved)

    def test_history_diagnostics_classify_malformed_and_error_bodies_safely(self):
        malformed = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 200, [], 200, {}, expected_points=12,
            max_points=100, first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:01Z')
        error = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 200,
            {'id': 'urn:ngsi-ld:Sensor:gw:sensor', 'reported_state': {'value': 1}, 'reported_version': '1'},
            400, {'error': {'message': 'private'}},
            expected_points=12, max_points=100,
            first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:01Z')
        self.assertEqual(malformed['reason'], 'twin_body_malformed')
        self.assertEqual(malformed['actual_points'], None)
        self.assertEqual(error['reason'], 'temporal_http_error')
        self.assertEqual(error['actual_points'], None)
        self.assertNotIn('private', json.dumps([malformed, error]))

    def test_measurement_export_parses_real_ack_summary_without_conflating_windows(self):
        output = ('measurement summary: samples=12 p50_ms=4.12 p95_ms=9.87 min_ms=1.01 '
                  'max_ms=12.34 first_publishes=12 retry_attempts=0 missing_origins=0 '
                  'overflow_samples=0 gen_duration_ms=100.00 publish_drain_duration_ms=45.00 '
                  'total_duration_ms=145.00')
        measurement = p5a.gateway_measurement(output)
        self.assertEqual(measurement['status'], 'available')
        self.assertEqual(measurement['samples'], 12)
        self.assertEqual(measurement['p95_ms'], 9.87)
        self.assertEqual(measurement['publish_drain_duration_ms'], 45.0)
        self.assertNotEqual(measurement['gen_duration_ms'], measurement['publish_drain_duration_ms'])
        self.assertEqual(measurement['reported_resolution_ms'], 0.01)
        self.assertIsNone(measurement['histogram_resolution_ms'])
        self.assertEqual(p5a.gateway_measurement_args(p5a.workload_config('100')),
                         ['--measure', '--measure-capacity', '12'])
        missing = p5a.gateway_measurement('no measurement summary')
        self.assertEqual(missing, {'status': 'unavailable', 'reason': 'summary_missing'})

    def test_finite_workload_is_identical_between_modes_and_has_real_group_members(self):
        off = p5a.workload_config('100')
        on = p5a.workload_config('100')
        self.assertEqual(off, on)
        self.assertEqual(off['gateway_count'], 2)
        self.assertEqual(off['messages_per_gateway'], 12)
        self.assertEqual(off['offered_unique_messages'], 24)
        self.assertEqual(off['offered_unique_samples'], 2400)
        self.assertGreaterEqual(off['messages_per_group_target'], 2)
        self.assertLessEqual(off['run_deadline_seconds'], 180)
        one = p5a.workload_config('1')
        self.assertEqual(one['offered_unique_messages'], off['offered_unique_messages'])
        self.assertEqual(one['offered_unique_samples'], 24)

    def test_accounting_requires_exact_drain_and_reconciles_offered_data(self):
        config = p5a.workload_config('100')
        p5a.validate_accounting(config, {
            'offered_unique': 24, 'committed_unique': 24, 'raw_samples': 2400,
            'temporal': 24, 'application_acks': 24, 'outbox_rows': 0,
            'outbox_bytes': 0, 'payload_budget_bytes': 0,
        })
        with self.assertRaisesRegex(ValueError, '^paper_p5a_accounting_rejected$'):
            p5a.validate_accounting(config, {
                'offered_unique': 24, 'committed_unique': 24, 'raw_samples': 2399,
                'temporal': 24, 'application_acks': 24, 'outbox_rows': 0,
                'outbox_bytes': 0, 'payload_budget_bytes': 0,
            })

    def test_acceptance_requires_runtime_groups_replay_acl_reads_and_cleanup(self):
        required = p5a.REQUIRED_CHECKS
        checks = {name: {'status': 'PASS'} for name in required}
        checks['group_metrics_observed'] = {'status': 'PASS', 'groups': 1, 'max_members': 2}
        self.assertTrue(p5a.acceptance_ready(checks, 'on'))
        checks['group_metrics_observed'] = {'status': 'FAIL', 'groups': 1, 'max_members': 1}
        self.assertFalse(p5a.acceptance_ready(checks, 'on'))
        self.assertFalse(p5a.acceptance_ready(
            {name: {'status': 'PASS'} for name in required - {'duplicate_replay_unchanged'}}, 'off'))

    def test_metadata_and_csv_are_secret_and_payload_safe(self):
        metadata = p5a.run_metadata('off', '1')
        self.assertEqual(metadata['offered_pattern']['status'], 'configured_not_run')
        safe = json.dumps(p5a.safe_artifact({'payload': 'private-bytes', 'password': 'hidden', 'count': 1}))
        self.assertNotIn('private-bytes', safe)
        self.assertNotIn('hidden', safe)
        self.assertIn('"count": 1', safe)


if __name__ == '__main__':
    unittest.main()
