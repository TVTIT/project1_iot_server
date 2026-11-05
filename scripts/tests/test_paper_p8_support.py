import subprocess
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import paper_p8_support as p8
import paper_p8_slow_support as slow


class PaperP8FixtureSupportTests(unittest.TestCase):
    def setUp(self):
        self.targets = [
            {'group': 'A', 'target': {'gateway_id': 'gw-a', 'sensor_id': 'sensor-a'}},
            {'group': 'B', 'target': {'gateway_id': 'gw-b', 'sensor_id': 'sensor-b'}},
        ]
        self.sessions = p8.session_specs(self.targets)

    def test_profile_is_opt_in_and_disposable_with_default_deployment_flags_false(self):
        profile = p8.profile()
        self.assertEqual(profile['WS_MAX_SESSIONS'], '64')
        self.assertEqual(profile['WEBSOCKET_ENABLED'], 'true')
        for flag in ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED',
                     'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
            self.assertEqual(profile[flag], 'false')

    def test_expected_counts_split_namespaces_and_overlap_does_not_multiply_delivery(self):
        self.assertEqual(len(self.sessions), 64)
        self.assertEqual(sum(s['group'] == 'A' for s in self.sessions), 32)
        self.assertEqual(sum(s['group'] == 'B' for s in self.sessions), 32)
        self.assertTrue(any(len(s['filters']) == 2 for s in self.sessions))
        self.assertEqual(p8.expected_deliveries(self.sessions, batches_a=1, batches_b=2),
                         {'A': 32, 'B': 64})

    def test_validates_healthy_group_after_revoke_and_delivery_totals(self):
        viewers = [{'id': s['id'], 'group': s['group'], 'unique_events': 1 if s['group'] == 'A' else 2,
                    'duplicates': 0, 'unexpected_namespace': 0, 'closed': s['group'] == 'A'}
                   for s in self.sessions]
        state = {'viewers': viewers, 'revoke_complete': True, 'cap_plus_one_denied': True,
                 'replacement_ready': True, 'duplicates': 0,
                 'unexpected_namespace': 0}
        self.assertEqual(p8.validate_state(state, sessions=self.sessions,
                                           before_a=1, before_b=1, after_b=1),
                         {'expected': {'A': 32, 'B': 64}, 'actual': {'A': 32, 'B': 64}})
        viewers[-1]['closed'] = True
        with self.assertRaisesRegex(ValueError, 'healthy_namespace'):
            p8.validate_state(state, sessions=self.sessions, before_a=1, before_b=1, after_b=1)

    def test_rejects_invalid_namespace_routing(self):
        with self.assertRaisesRegex(ValueError, 'namespace_targets'):
            p8.session_specs(self.targets[:1])

    def test_owned_child_stop_uses_bounded_interrupt_then_terminate(self):
        proc = mock.Mock()
        proc.pid = 321
        proc.poll.side_effect = [None, None]
        proc.wait.side_effect = [subprocess.TimeoutExpired('child', 1), None]
        with mock.patch.object(p8.os, 'killpg') as killpg:
            p8.stop_child(proc, timeout=1)
        self.assertEqual([call.args[1] for call in killpg.call_args_list],
                         [p8.signal.SIGINT, p8.signal.SIGTERM])

    def test_stop_oserror_is_reportable_not_swallowed(self):
        proc = mock.Mock()
        proc.pid = 321
        proc.poll.return_value = None
        with mock.patch.object(p8.os, 'killpg', side_effect=OSError('signal failed')):
            with self.assertRaises(OSError):
                p8.stop_child(proc)

    def test_finite_resource_estimates_and_unavailable_values_are_not_zero(self):
        limits = {'soft': 256, 'hard': 512}
        result = p8.resource_estimate(limits, ephemeral_range=(32768, 60999),
                                      pilot_viewers=64, proposed_rungs=(128, 256, 10000))
        self.assertEqual(result['generator']['required_fds']['pilot64'], 192)
        self.assertEqual(result['generator']['required_fds']['proposed_rungs'],
                         {'128': 320, '256': 576, '10000': 20064})
        self.assertEqual(result['ephemeral_ports']['range'], [32768, 60999])
        self.assertEqual(result['generator_os_fd_limits']['reason'], 'system_file_limits_unavailable')
        unavailable = p8.resource_estimate(None, ephemeral_range=None, pilot_viewers=64,
                                           proposed_rungs=(128,))
        self.assertIsNone(unavailable['generator']['available'])
        self.assertIn('reason', unavailable['generator'])
        with self.assertRaisesRegex(ValueError, 'finite_budget'):
            p8.resource_estimate(limits, ephemeral_range=None, pilot_viewers=10001,
                                 proposed_rungs=())

    def test_bundle_separates_public_metadata_and_private_identity_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = p8.prepare_client_bundle(root, run_id='pilot-1',
                endpoints={'https': 'https://fixture.example.test',
                           'mqtt_tls': 'mqtts://broker.example.test:8883'},
                topology={'gateways': 2, 'sensors': 2, 'viewers': 64},
                public_cas={'https': b'https ca', 'mqtt': b'mqtt ca'},
                targets=[{'gateway_id': 'gw-a', 'sensor_id': 's-a'}],
                workload={'profile': 'pilot64'},
                identities={'role_jwts': {'viewer': 'jwt-secret'},
                            'gateways': {'gw-a': {'client_id': 'gw-a', 'username': 'user',
                                                   'password': 'mqtt-secret'}}})
            public = Path(bundle['public_manifest']).read_text()
            self.assertNotIn('jwt-secret', public)
            self.assertNotIn('mqtt-secret', public)
            self.assertNotIn('admin', public.lower())
            for private_file in bundle['private_files'].values():
                self.assertEqual(oct(os.stat(private_file).st_mode & 0o777), '0o600')
            jwt_private = Path(bundle['private_files']['role_jwts.json']).read_text()
            gateway_private = Path(bundle['private_files']['gateways/gw-a.json']).read_text()
            self.assertIn('jwt-secret', jwt_private)
            self.assertIn('mqtt-secret', gateway_private)
            self.assertNotEqual(bundle['ca_sha256']['https'], bundle['ca_sha256']['mqtt'])

    def test_transport_mapping_is_opt_in_loopback_and_owned_tls_service_only(self):
        self.assertIsNone(p8.validate_mqtt_mapping(None, owned_service='mosquitto'))
        self.assertEqual(p8.validate_mqtt_mapping({'host_ip': '127.0.0.1', 'published': 18884,
                           'target': 8883, 'service': 'mosquitto'}, owned_service='mosquitto'), 18884)
        for mapping in ({'host_ip': '0.0.0.0', 'published': 18884, 'target': 8883, 'service': 'mosquitto'},
                        {'host_ip': '127.0.0.1', 'published': 1883, 'target': 1883, 'service': 'mosquitto'},
                        {'host_ip': '127.0.0.1', 'published': 18884, 'target': 8883, 'service': 'postgres'}):
            with self.assertRaisesRegex(ValueError, 'mqtt_mapping_rejected'):
                p8.validate_mqtt_mapping(mapping, owned_service='mosquitto')

    def test_dynamic_fixture_mapping_and_host_readback_fail_closed(self):
        p8.validate_dynamic_mqtt_mapping({'host_ip': '127.0.0.1', 'published': '0',
            'target': 8883, 'service': 'mosquitto'}, owned_service='mosquitto')
        self.assertEqual(p8.validate_mqtt_readback({'host_ip': '127.0.0.1',
            'published': 18884, 'target': 8883, 'service': 'mosquitto'}, owned_service='mosquitto'), 18884)
        for value in ({'host_ip': '0.0.0.0', 'published': '0', 'target': 8883, 'service': 'mosquitto'},
                      {'host_ip': '127.0.0.1', 'published': '0', 'target': 1883, 'service': 'mosquitto'},
                      {'host_ip': '127.0.0.1', 'published': '0', 'target': 8883, 'service': 'postgres'}):
            with self.assertRaisesRegex(ValueError, 'mqtt_mapping_rejected'):
                p8.validate_dynamic_mqtt_mapping(value, owned_service='mosquitto')

    def test_fixture_preflight_accounts_for_both_nginx_websocket_connections(self):
        result = p8.fixture_process_preflight(worker_processes=2, worker_connections=160,
            process_fd_limits={'soft': 1024, 'hard': 1024}, viewers=64,
            backend_fd_availability={'soft': 1024, 'open': 12},
            broker_fd_availability={'soft': 1024, 'open': 12})
        self.assertEqual(result['nginx']['required_connections_per_worker'], 80)
        self.assertTrue(result['nginx']['fits'])
        self.assertIsNone(result['process_fd_limits'].get('available'))
        below = p8.fixture_process_preflight(worker_processes=1, worker_connections=159,
            process_fd_limits=None, viewers=64)
        self.assertFalse(below['nginx']['fits'])
        self.assertIsNone(below['backend_fd_availability']['available'])
        self.assertIsNone(p8.parse_process_limits('no proc limit data'))
        self.assertEqual(p8.parse_process_limits('Max open files            64                  128                 files'),
                         {'soft': 64, 'hard': 128})

    def test_lifecycle_budget_rejects_short_deadline_and_allows_two_hour_workload(self):
        with self.assertRaisesRegex(ValueError, 'lifecycle_deadline_insufficient'):
            p8.lifecycle_budget(workload_seconds=7200, session_deadline_seconds=8000)
        budget = p8.lifecycle_budget(workload_seconds=7200, session_deadline_seconds=8100)
        self.assertEqual(budget['required_session_seconds'], 8100)
        self.assertEqual(budget['workload_seconds'], 7200)

    def test_fixture_fd_estimate_uses_actual_roles_and_reserve(self):
        result = p8.fixture_process_preflight(worker_processes=2, worker_connections=248,
            process_fd_limits={'soft': 1024, 'hard': 1024}, viewers=100,
            role_connections={'viewer': 100, 'telemetry': 16}, reserve_connections=32)
        self.assertEqual(result['nginx']['required_connections_per_worker'], 124)
        self.assertTrue(result['nginx']['fits'])

    def test_owned_fixture_readback_queries_only_nginx_backend_and_broker(self):
        def fake(service, command):
            if command[:2] == ['nginx', '-T']:
                return 'worker_processes 2;\nevents {\n worker_connections 160;\n}'
            if command[0] == 'cat':
                return 'Max open files 1024 2048 files'
            return '10\n'
        result = p8.read_owned_fixture_resources(fake)
        self.assertEqual(result['nginx']['worker_processes'], 2)
        self.assertEqual(result['nginx']['worker_connections'], 160)
        self.assertTrue(result['nginx']['fits'])
        self.assertEqual(result['backend_fd_availability']['available'], 1014)
        self.assertEqual(result['backend_fd_availability']['hard'], 2048)
        empty = p8.read_owned_fixture_resources(lambda service, command:
            'worker_processes 1;\nworker_connections 1024;' if command[:2] == ['nginx', '-T'] else
            ('Max open files 1024 2048 files' if command[0] == 'cat' else ''))
        self.assertIsNone(empty['broker_fd_availability']['available'])
        self.assertIn('reason', empty['broker_fd_availability'])

    def test_bundle_rejects_db_and_admin_identity_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            kwargs = dict(run_id='pilot', endpoints={'https': 'https://a.test',
                'mqtt_tls': 'mqtts://b.test:8883'}, topology={}, public_cas={'https': b'a', 'mqtt': b'b'},
                targets=[], workload={}, identities={'role_jwts': {'viewer': 'jwt'}, 'gateways': {}})
            kwargs['endpoints'] = {'https': 'https://a.test', 'mqtt_tls': 'mqtts://b.test:5432'}
            with self.assertRaisesRegex(ValueError, 'bundle_endpoint_rejected'):
                p8.prepare_client_bundle(Path(directory) / 'endpoint', **kwargs)
            kwargs['endpoints'] = {'https': 'https://a.test', 'mqtt_tls': 'mqtts://b.test:8883'}
            kwargs['identities'] = {'role_jwts': {'viewer': 'jwt'}, 'gateway': {'admin': 'secret'}}
            with self.assertRaisesRegex(ValueError, 'bundle_identity_rejected'):
                p8.prepare_client_bundle(Path(directory) / 'identity', **kwargs)
            kwargs['identities'] = {'role_jwts': {'viewer': 'jwt'}, 'gateways': {}}
            kwargs['workload'] = {'token': 'must-not-serialize'}
            with self.assertRaisesRegex(ValueError, 'bundle_public_data_rejected'):
                p8.prepare_client_bundle(Path(directory) / 'public', **kwargs)

    def test_owned_stop_signal_is_idempotent_and_rejects_nonprivate_existing_path(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'stop'
            p8._touch_private(marker)
            p8._touch_private(marker)
            marker.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'control_file'):
                p8._touch_private(marker)

    def test_metrics_window_rejects_missing_or_changed_process_and_counter_reset(self):
        hist = lambda count, buckets: {'count': count, 'overflow': 0,
            'upper_seconds': [1.0, 2.0], 'buckets': buckets}
        before = {'process_instance': 'one', 'availability': {'ingress': 'available'},
                  'ingress': {'h': hist(3, [2, 1])}}
        after = {'process_instance': 'one', 'availability': {'ingress': 'available'},
                 'ingress': {'h': hist(5, [3, 2])}}
        def helper(a, b, group, name):
            old, new = a[group][name], b[group][name]
            delta = [n - o for o, n in zip(old['buckets'], new['buckets'])]
            count, overflow = new['count'] - old['count'], new['overflow'] - old['overflow']
            if count < 0 or overflow < 0 or any(x < 0 for x in delta) or sum(delta) + overflow != count:
                return {'availability': 'unavailable'}
            return {'availability': 'available', 'count': count, 'overflow': overflow}
        self.assertEqual(p8.metrics_window(None, after, 'ingress', 'h', helper)['reason'], 'scrape_missing')
        changed = dict(after, process_instance='two')
        self.assertEqual(p8.metrics_window(before, changed, 'ingress', 'h', helper)['reason'],
                         'process_instance_changed')
        reset = dict(after, ingress={'h': hist(1, [1, 0])})
        result = p8.metrics_window(before, reset, 'ingress', 'h', helper)
        self.assertEqual(result['availability'], 'unavailable')

    def test_histogram_overflow_is_reported_as_unbounded_percentile(self):
        before = {'process_instance': 'same', 'availability': {'ingress': 'available'},
                  'ingress': {'h': {'count': 0, 'overflow': 0, 'upper_seconds': [1.0], 'buckets': [0]}}}
        after = {'process_instance': 'same', 'availability': {'ingress': 'available'},
                 'ingress': {'h': {'count': 2, 'overflow': 1, 'upper_seconds': [1.0], 'buckets': [1]}}}
        def histogram(a, b, group, name):
            old, new = a[group][name], b[group][name]
            if sum(new['buckets']) + new['overflow'] != new['count']:
                return {'availability': 'unavailable'}
            in_range = new['buckets'][0] - old['buckets'][0]
            return {'availability': 'available', 'count': new['count'] - old['count'],
                    'overflow': new['overflow'] - old['overflow'],
                    'p99_seconds': 'infinity' if in_range < new['count'] else 1.0}
        result = p8.metrics_window(before, after, 'ingress', 'h', histogram)
        self.assertEqual(result['p99_seconds'], 'infinity')

    def test_composite_sensor_scope_delta_does_not_collapse_same_sensor_ids(self):
        before = [{'gateway_id': 'gw-a', 'sensor_id': 'same', 'counts': {'ledger': 1, 'raw': 2, 'temporal': 1}},
                  {'gateway_id': 'gw-b', 'sensor_id': 'same', 'counts': {'ledger': 3, 'raw': 7, 'temporal': 3}}]
        after = [{'gateway_id': 'gw-a', 'sensor_id': 'same', 'counts': {'ledger': 2, 'raw': 3, 'temporal': 2}},
                 {'gateway_id': 'gw-b', 'sensor_id': 'same', 'counts': {'ledger': 5, 'raw': 9, 'temporal': 5}}]
        delta = p8.sensor_scoped_delta(before, after)
        self.assertEqual(delta['available'], {'gw-a/same': {'ledger': 1, 'raw': 1, 'temporal': 1},
                                              'gw-b/same': {'ledger': 2, 'raw': 2, 'temporal': 2}})
        self.assertIsNone(p8.sensor_scoped_delta(before, after[:1])['available'])
        # Reject duplicate keys
        duplicate = before + [{'gateway_id': 'gw-a', 'sensor_id': 'same', 'counts': {'ledger': 1, 'raw': 2, 'temporal': 1}}]
        self.assertEqual(p8.sensor_scoped_delta(duplicate, after)['reason'], 'duplicate_composite_sensor_key')
        # Reject missing counter field
        missing_field = [{'gateway_id': 'gw-a', 'sensor_id': 'same', 'counts': {'raw': 2, 'temporal': 1}},
                         {'gateway_id': 'gw-b', 'sensor_id': 'same', 'counts': {'ledger': 3, 'raw': 7, 'temporal': 3}}]
        self.assertIn('missing_counter_field', p8.sensor_scoped_delta(missing_field, after)['reason'])
        # Reject counter reset / negative delta
        reset = [{'gateway_id': 'gw-a', 'sensor_id': 'same', 'counts': {'ledger': 0, 'raw': 1, 'temporal': 0}},
                 {'gateway_id': 'gw-b', 'sensor_id': 'same', 'counts': {'ledger': 5, 'raw': 9, 'temporal': 5}}]
        self.assertIn('counter_reset', p8.sensor_scoped_delta(before, reset)['reason'])

    def test_calculate_ingress_delta_validates_same_instance_and_monotonicity(self):
        counters = lambda received, committed, samples: {
            'Received': received, 'ReceivedBytes': 100, 'Accepted': received, 'AcceptedBytes': 100,
            'RejectedNotRunning': 0, 'RejectedStopping': 0, 'RejectedRetained': 0,
            'RejectedOversized': 0, 'RejectedInvalidTopic': 0, 'QueueFull': 0,
            'ByteBudgetFull': 0, 'ShaperPassed': 0, 'ShaperRateLimited': 0,
            'ShaperStateCapacity': 0, 'Processed': received, 'ValidationRejected': 0,
            'PersistFailures': 0, 'CommitUnconfirmed': 0, 'IdentityMismatch': 0,
            'ACKBuildFailures': 0, 'Committed': committed, 'DuplicateCommitted': 0,
            'SamplesNew': samples, 'ACKPublished': committed, 'ACKPublishFailures': 0,
        }
        b = {'process_instance': 'pid-1', 'ingress': counters(0, 0, 0)}
        a = {'process_instance': 'pid-1', 'ingress': counters(20, 20, 2000)}
        delta = p8.calculate_ingress_delta(b, a)
        self.assertTrue(delta['available'])
        self.assertEqual(delta['delta']['Received'], 20)
        self.assertEqual(delta['delta']['Accepted'], 20)
        self.assertEqual(delta['delta']['Committed'], 20)
        self.assertEqual(delta['delta']['SamplesNew'], 2000)
        self.assertEqual(delta['delta']['ACKPublished'], 20)
        # Missing scrape
        self.assertEqual(p8.calculate_ingress_delta(None, a)['reason'], 'scrape_missing')
        # Process instance changed
        self.assertEqual(p8.calculate_ingress_delta(b, dict(a, process_instance='pid-2'))['reason'], 'process_instance_changed')
        # Process instance missing
        self.assertEqual(p8.calculate_ingress_delta(b, {'ingress': counters(20, 20, 2000)})['reason'], 'process_instance_missing')
        # Counter reset
        self.assertIn('counter_reset', p8.calculate_ingress_delta(b, {'process_instance': 'pid-1', 'ingress': counters(-1, 20, 2000)})['reason'])

    def test_assemble_e2_report_assembles_cleanly_without_unbound_local_error(self):
        report = p8.assemble_e2_report(
            success=True, source_revision='rev123', dirty_worktree=False,
            schema='24:telemetry_ledger_timestamp', quota={'cpus': 2, 'memory_bytes': 4294967296},
            image_ids={'backend': 'sha256:1', 'mosquitto': 'sha256:2', 'gateway': 'sha256:3'},
            paperload_source_sha256='source-hash', paperload_sha256='bin-hash',
            manifest_sha256='manifest-hash', workload_path=Path('/tmp/workload.json'),
            effective_flags={'WEBSOCKET_ENABLED': 'false'},
            endpoints={'https': 'https://127.0.0.1:443', 'mqtt_tls': 'mqtts://127.0.0.1:8883'},
            counters={'generated_unique': 20, 'offered_unique': 20, 'publish_attempts': 20,
                      'broker_pubacks': 20, 'application_acks': 20, 'pending_messages': 0, 'pending_bytes': 0},
            database_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            database_by_gateway={'gw-a': {'ledger': 10, 'raw': 1000, 'temporal': 10}},
            scoped_delta={'available': {'gw-a/s-a': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            pilot_raw_verification=[{'verified_exact': True, 'ledger_count': 10}],
            manifest={'limits': {'workers': 2}, 'topology': {'gateways': 2}, 'phases': {'measure_seconds': 4}, 'traffic': {'rate_per_second': 5}},
            collector_metrics=[{'phase': 'workload_start', 'snapshot': {}}],
            collector_resources=[],
            collector_errors=[],
            ingress_delta={'available': True, 'delta': {'Received': 20, 'Accepted': 20, 'Committed': 20, 'DuplicateCommitted': 0, 'SamplesNew': 2000, 'ACKPublished': 20, 'RejectedNotRunning': 0, 'RejectedStopping': 0, 'RejectedRetained': 0, 'RejectedOversized': 0, 'RejectedInvalidTopic': 0, 'QueueFull': 0, 'ByteBudgetFull': 0, 'ShaperRateLimited': 0}},
            histogram_windows={},
        )
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['metadata']['source_sha256'], 'source-hash')
        self.assertEqual(report['metadata']['binary_sha256'], 'bin-hash')
        self.assertEqual(report['accounting']['backend_observed']['available'], 20)
        self.assertEqual(report['accounting']['new_commit']['available'], 20)
        self.assertEqual(report['accounting']['raw_rows']['available'], 2000)
        self.assertEqual(report['accounting']['ack_published']['available'], 20)
        self.assertEqual(report['accounting']['outbox_deleted']['reason'], 'not_applicable_synthetic_workload')

    def test_collector_records_scrape_and_resource_exceptions_and_stops(self):
        collector = p8.BoundedCollector(scrape=lambda: (_ for _ in ()).throw(OSError()),
            interval=0.1, max_snapshots=2)
        collector.start(None)
        collector.stop(None)
        self.assertEqual(collector.metrics[0]['error'], 'scrape_OSError')
        self.assertLessEqual(len(collector.metrics), 2)
        self.assertFalse(collector._thread.is_alive())
        self.assertEqual(collector.generator_samples[0]['reason'], 'generator_process_unavailable')

    def test_collector_keeps_baseline_terminal_and_rejects_process_set_reset(self):
        collector = p8.BoundedCollector(scrape=lambda: {'process_instance': 'backend-1'},
            interval=0.1, max_snapshots=3, baseline_metrics={'process_instance': 'backend-1'})
        proc = {'available': True, 'rss_bytes': 10, 'cpu_ticks': 20, 'process_count': 1,
                'processes': {(1, 10): {'ticks': 20}}}
        changed = {'available': True, 'rss_bytes': 10, 'cpu_ticks': 25, 'process_count': 1,
                   'processes': {(1, 10): {'ticks': 20}, (2, 11): {'ticks': 5}}}
        with mock.patch.object(p8, 'process_tree_resources', side_effect=[proc, changed]):
            collector.start(1)
            collector.stop(1)
        self.assertEqual(collector.metrics[0]['phase'], 'workload_start')
        self.assertEqual(collector.metrics[-1]['phase'], 'terminal')
        self.assertTrue(any(sample.get('reason') == 'generator_process_set_changed'
                            for sample in collector.generator_samples))


class PaperP8SlowViewerTests(unittest.TestCase):
    def test_finite_budgets_and_explicit_three_peer_qualification(self):
        self.assertEqual(slow.expected_counts(), {'A': 400, 'B': 2,
                                                  'raw_A': 400000, 'raw_B': 2000})
        self.assertEqual(slow.LIFETIME_MS, 150000)
        self.assertEqual(slow.WAVE_SIZE, 10)
        self.assertEqual(slow.profile()['WS_MAX_SESSIONS'], '64')
        self.assertEqual(slow.profile()['WEBSOCKET_ENABLED'], 'true')
        self.assertEqual({key: slow.profile()[key] for key in
                          ('WS_HEARTBEAT_INTERVAL', 'WS_PONG_TIMEOUT', 'WS_WRITE_TIMEOUT')},
                         {'WS_HEARTBEAT_INTERVAL': '2s', 'WS_PONG_TIMEOUT': '4s',
                          'WS_WRITE_TIMEOUT': '1s'})
        for flag in ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED',
                     'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
            self.assertEqual(slow.profile()[flag], 'false')
        with self.assertRaisesRegex(ValueError, 'finite_budget'):
            slow.expected_counts(batches_a=401)

    def test_validates_exact_healthy_namespace_counts(self):
        state = {'events': 202, 'failure': '', 'peers': [
            {'role': 'A', 'events': 200, 'unique_events': 200, 'duplicates': 0,
             'unexpected_namespace': 0, 'closed': False},
            {'role': 'B', 'events': 2, 'unique_events': 2, 'duplicates': 0,
             'unexpected_namespace': 0, 'closed': False}]}
        self.assertEqual(slow.validate_peer_state(state, batches_a=200)['actual'], {'A': 200, 'B': 2})
        state['peers'][0]['unique_events'] = 199
        with self.assertRaisesRegex(ValueError, 'healthy_peer_counts'):
            slow.validate_peer_state(state, batches_a=200)

    def test_stop_child_keeps_oserror_visible(self):
        proc = mock.Mock()
        proc.pid = 10
        proc.poll.return_value = None
        with mock.patch.object(slow.os, 'killpg', side_effect=OSError('cannot signal')):
            with self.assertRaises(OSError):
                slow.stop_child(proc)


if __name__ == '__main__':
    unittest.main()
