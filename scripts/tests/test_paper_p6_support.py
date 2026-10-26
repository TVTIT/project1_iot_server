import unittest
import subprocess
import sys
import tempfile
import ast
import io
from unittest.mock import patch
from types import SimpleNamespace
from pathlib import Path

import paper_p6_support as p6


class PaperP6PreparationTests(unittest.TestCase):
    def test_cli_modes_and_dataset_are_explicit(self):
        for mode in ('polling', 'wake-up'):
            for dataset in ('100', '1'):
                self.assertEqual(p6.profile(mode, dataset)['STAGE5_WAKEUP_ENABLED'],
                                 'true' if mode == 'wake-up' else 'false')
        for args in (('unknown', '100'), ('polling', '10')):
            with self.assertRaises(ValueError):
                p6.profile(*args)

    def test_cli_rejects_incomplete_and_mixed_profile_selectors_before_prepare(self):
        cli = Path(__file__).with_name('native_compose_smoke.py')
        base = [sys.executable, str(cli), '--prepare', '/var/tmp/opencode/paper-p6/never-created']
        invalid = [
            ['--paper-p6'],
            ['--paper-p6', '--dispatch-mode', 'polling'],
            ['--paper-p6', '--dispatch-mode', 'polling', '--dataset', '100', '--paper-p3'],
            ['--dispatch-mode', 'polling'],
            ['--p6-recovery-smoke'],
            ['--paper-p6', '--dispatch-mode', 'polling', '--dataset', '100', '--p6-recovery-smoke'],
        ]
        for selector in invalid:
            result = subprocess.run(base + selector, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertNotIn('never-created', result.stdout)

    def test_recovery_sequence_requires_transport_loss_restart_and_identical_intent(self):
        self.assertTrue(p6.recovery_evidence_valid({'native_executed': True,
            'initial_command_succeeded': True, 'transport_not_ready': True,
            'pending_before_restart': True, 'pending_after_restart': True,
            'stable_command_id': True, 'stable_payload_sha256': True,
            'process_marker_changed': True, 'reconnected_ready': True,
            'backend_restarted_with_broker_healthy': True,
            'recovered_command_succeeded': True, 'durable_receipt_evidence': True,
            'missing_origin': True, 'latency_samples_missing': True,
            'config_rest_db_match': True, 'telemetry_outboxes_drained': True,
            'exact_cleanup_verified': True,
        }))
        for key in ('transport_not_ready', 'pending_after_restart', 'stable_payload_sha256',
                    'process_marker_changed', 'backend_restarted_with_broker_healthy',
                    'durable_receipt_evidence', 'missing_origin',
                    'latency_samples_missing', 'config_rest_db_match', 'telemetry_outboxes_drained',
                    'exact_cleanup_verified'):
            self.assertFalse(p6.recovery_evidence_valid({key: False}))
        self.assertFalse(p6.recovery_evidence_valid({'native_executed': True, 'single_apply': True}))

    def test_recovery_requires_backend_restart_with_broker_healthy_and_readiness(self):
        timeline = [
            {'event': 'broker_started', 'broker_running': True},
            {'event': 'backend_restarted', 'broker_running': True},
            {'event': 'ready', 'http_status': 200},
        ]
        self.assertTrue(p6.recovery_restart_order_valid(timeline))
        self.assertFalse(p6.recovery_restart_order_valid([
            {'event': 'backend_restarted', 'broker_running': False},
            {'event': 'broker_started', 'broker_running': True},
            {'event': 'ready', 'http_status': 200},
        ]))
        self.assertFalse(p6.recovery_restart_order_valid([
            {'event': 'broker_started', 'broker_running': True},
            {'event': 'backend_restarted', 'broker_running': False},
            {'event': 'ready', 'http_status': 200},
        ]))

    def test_recovered_old_origin_metrics_use_serialized_json_names_and_no_fake_zero(self):
        before = {'process_instance': 'new-process', 'command_timeline': {
            'd_dispatch': {'count': 0}, 'd_e2e_succeeded': {'count': 0},
            'pending_origins': 0, 'missing_origin': 0}}
        after = {'process_instance': 'new-process', 'command_timeline': {
            'd_dispatch': {'count': 0}, 'd_e2e_succeeded': {'count': 0},
            'pending_origins': 1, 'missing_origin': 0}}
        self.assertEqual(p6.recovered_origin_metrics(before, after), {
            'dispatch_count_before': 0, 'dispatch_count_after': 0,
            'succeeded_count_before': 0, 'succeeded_count_after': 0,
            'dispatch_delta': 0, 'succeeded_delta': 0, 'missing_origin': True,
            'latency_samples_missing': True})
        # A zero count means no latency sample exists; no synthetic latency=0 is emitted.
        self.assertNotIn('latency_seconds', p6.recovered_origin_metrics(before, after))
        for malformed in (None, {}, {'command_timeline': {}},
                          {'command_timeline': {'d_dispatch': {'count': 0}}}):
            with self.assertRaises(ValueError):
                p6.recovered_origin_metrics(before, malformed)
        positive_e2e = {'process_instance': 'new-process', 'command_timeline': {
            'd_dispatch': {'count': 0}, 'd_e2e_succeeded': {'count': 1},
            'pending_origins': 0, 'missing_origin': 1}}
        with self.assertRaises(ValueError):
            p6.recovered_origin_metrics(before, positive_e2e)

    def test_readiness_diagnostic_redacts_credential_like_fields(self):
        diagnostic = p6.readiness_diagnostic_body({
            'message': 'broker unavailable password=never-print token:do-not-print'})
        self.assertIn('broker unavailable', diagnostic)
        self.assertNotIn('never-print', diagnostic)
        self.assertNotIn('do-not-print', diagnostic)
        lines = p6.safe_transport_log_excerpt(
            'info msg="mqtt reconnect" password=never-print\n'
            'info msg="ordinary request" path=/private')
        self.assertEqual(len(lines), 1)
        self.assertNotIn('never-print', lines[0])
        self.assertNotIn('ordinary request', str(lines))
        probe = p6.safe_direct_probe(SimpleNamespace(returncode=0,
            stdout=b'HTTP/1.1 200 OK\nOK\n', stderr=b''))
        self.assertEqual(probe, {'exit_code': 0, 'http_status': 200, 'body_excerpt': 'OK'})

    def test_readiness_snapshot_handles_http_request_status_headers_body(self):
        smoke = SimpleNamespace(http_request=lambda *args, **kwargs: (503, {'header': 'ignored'},
                                                                       {'error': 'not ready'}))
        self.assertEqual(p6.readiness_snapshot(smoke, 'https://fixture.invalid'),
                         (503, {'error': 'not ready'}))

    def test_arms_have_locked_common_configuration(self):
        baseline, wakeup = p6.profile('polling', '100'), p6.profile('wake-up', '100')
        self.assertTrue(p6.profiles_differ_only_by_wakeup(baseline, wakeup))
        self.assertEqual(baseline['STAGE5_PUBLISH_QUEUE_CAPACITY'], '32')
        self.assertEqual(baseline['STAGE5_SCAN_BATCH'], '32')
        self.assertEqual(baseline['STAGE5_PUBLISH_WORKERS'], '1')
        self.assertEqual(baseline['DATABASE_MAX_CONNS'], '12')
        self.assertEqual(baseline['MQTT_MICROBATCH_MAX_WAIT'], '1s')
        self.assertEqual(baseline['STAGE5_COMMAND_TTL'], '60s')
        self.assertTrue(all(baseline[k] == 'true' for k in
                            ('STAGE5_ENABLED', 'MQTT_SHAPER_ENABLED',
                             'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED')))

    def test_metadata_is_prepared_not_execution_evidence(self):
        for mode in ('polling', 'wake-up'):
            metadata = p6.run_metadata(mode, '100')
            self.assertEqual(metadata['status'], 'PREPARED_NOT_EXECUTED')
            self.assertFalse(metadata['native_executed'])
            self.assertEqual(metadata['acceptance']['status'], 'NOT_RUN')
            self.assertEqual(metadata['workload_specification']['status'], 'configured_not_run')
            self.assertEqual(metadata['database_schema_terminal'], '24:telemetry_ledger_timestamp')
            self.assertNotIn('password', str(metadata).lower())

    def test_workload_spec_has_real_overlap_and_safe_command_semantics(self):
        spec = p6.workload_spec('100')
        self.assertTrue(spec['phases_seconds']['steady_command_overlap_required'])
        self.assertTrue(spec['phases_seconds']['burst_command_overlap_required'])
        self.assertTrue(spec['command_stream']['one_active_command_per_sensor'])
        self.assertTrue(spec['accounting']['telemetry_exact_counts_required'])
        self.assertTrue(spec['accounting']['command_sensor_telemetry_excluded'])
        self.assertTrue(p6.workload_spec('1')['telemetry']['representative_single_sample_profile'])

    def test_workload_topology_uses_two_gateways_and_four_isolated_sensors(self):
        full = p6.workload_spec('100')
        single = p6.workload_spec('1')
        self.assertEqual(full['gateway_count'], 2)
        self.assertEqual(full['gateway_roles'], {'gw-a': 'steady_telemetry_and_commands',
                                                  'gw-b': 'steady_and_burst_telemetry'})
        self.assertEqual(full['sensors']['total'], 4)
        self.assertEqual(full['sensors']['telemetry_ids'], ['existing_gw_a_sensor',
                                                            'existing_gw_b_sensor'])
        self.assertEqual(full['sensors']['command_sensors_per_gateway']['gw-a'], 2)
        self.assertEqual(full['telemetry']['sample_rate_hz'], 100)
        self.assertEqual(single['telemetry']['sample_rate_hz'], 1)
        self.assertTrue(full['accounting']['command_sensors_excluded_from_telemetry'])
        self.assertEqual(full['phases_seconds']['steady_messages_by_gateway'], {'gw-a': 60, 'gw-b': 12})
        self.assertEqual(full['phases_seconds']['burst_messages'], 24)
        self.assertEqual(full['command_stream']['target_sampling_frequency_hz_sequence'], [1, 2, 4])

    def test_command_sensor_environment_is_private_and_uses_distinct_sensor_control_identity(self):
        base = {'GATEWAY_ID': 'gw-a', 'SENSOR_ID': 'telemetry', 'GATEWAY_SAMPLE_RATE_HZ': '100',
                'GATEWAY_SAMPLES_PER_BATCH': '100', 'GATEWAY_MQTT_CLIENT_ID': 'base'}
        env = p6.command_simulator_environment(base, 'command-1', 'run-p6-command-1')
        self.assertEqual(env['SENSOR_ID'], 'command-1')
        self.assertEqual(env['GATEWAY_CONTROL_ENABLED'], 'true')
        self.assertEqual(env['GATEWAY_MQTT_CLIENT_ID'], 'run-p6-command-1')
        self.assertEqual(env['GATEWAY_SAMPLE_RATE_HZ'], '100')
        self.assertEqual(env['GATEWAY_SAMPLES_PER_BATCH'], '100')
        self.assertNotIn('GATEWAY_MQTT_PASSWORD', str(env))

    def test_setup_provisions_two_owned_command_sensors_and_independent_simulator_volumes(self):
        class Registry:
            def __init__(self):
                self.items, self.created = [], {}
            def intent(self, kind, name, image, labels):
                item = {'kind': kind, 'name': name, 'labels': labels}
                self.items.append(item)
                return item
            def resolve(self, item):
                return self.created.get(item['name'])
            def allocating(self, item):
                item['allocation_attempted'] = True
            def command(self, args):
                self.created[args[-1]] = {'Name': args[-1]}
                return SimpleNamespace(returncode=0)

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = SimpleNamespace(project='p6unit', private=root)
            registry, calls, written = Registry(), [], {}
            class Smoke:
                @staticmethod
                def http_request(*args, **kwargs):
                    calls.append((args, kwargs))
                    sensor = args[2].rsplit('/', 1)[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:gateway-a:{sensor}'}
                @staticmethod
                def private_write(path, contents):
                    written[str(path)] = contents
                    Path(path).write_text(contents)
            controls = p6.provision_command_simulators(
                Smoke, fixture, registry, 'https://fixture.invalid', {'a': 'jwt'},
                SimpleNamespace(gateway_id='gateway-a', sensor_id='telemetry', sensor_unit='C'),
                'fixture-password', SimpleNamespace(checkpoint=lambda: None), dataset='1')
            self.assertEqual(len(calls), 2)
            self.assertEqual([item['sensor_id'] for item in controls],
                             ['p6-command-1', 'p6-command-2'])
            self.assertEqual(len({item['volume']['name'] for item in controls}), 2)
            self.assertTrue(all(item['volume']['labels']['test.run'] == 'p6unit' for item in controls))
            self.assertEqual(len(written), 2)
            for item in controls:
                env = dict(line.split('=', 1) for line in written[str(item['env_path'])].splitlines())
                self.assertEqual(env['SENSOR_ID'], item['sensor_id'])
                self.assertEqual(env['GATEWAY_SAMPLE_RATE_HZ'], '1')
                self.assertEqual(env['GATEWAY_SAMPLES_PER_BATCH'], '1')
                self.assertEqual(env['GATEWAY_CONTROL_ENABLED'], 'true')
                self.assertIn('ssl://mosquitto:8883', env['GATEWAY_MQTT_BROKER_URL'])
                self.assertNotEqual(env['GATEWAY_MQTT_CLIENT_ID'], 'gateway-a_sim')

    def test_acceptance_requires_real_overlap_accounting_schema_and_cleanup(self):
        evidence = {'native_executed': True, 'command_overlap_verified': True,
                    'telemetry_accounting_exact': True, 'schema_24_verified': True,
                    'exact_cleanup_verified': True}
        self.assertTrue(p6.execution_acceptance_ready(evidence))
        for key in ('native_executed', 'command_overlap_verified', 'telemetry_accounting_exact',
                    'schema_24_verified', 'exact_cleanup_verified'):
            self.assertFalse(p6.execution_acceptance_ready({**evidence, key: False}))
        self.assertFalse(p6.execution_acceptance_ready({'status': 'PASS'}))

    def test_overlap_requires_observed_generation_windows_not_container_lifetimes(self):
        self.assertTrue(p6.intervals_overlap(
            {'generation_start': 10, 'generation_end': 20},
            {'generation_start': 19, 'generation_end': 21}))
        self.assertFalse(p6.intervals_overlap(
            {'generation_start': 10, 'generation_end': 15},
            {'generation_start': 16, 'generation_end': 21}))
        self.assertFalse(p6.intervals_overlap({'container_start': 10, 'container_end': 30},
                                              {'container_start': 12, 'container_end': 18}))

    def test_telemetry_accounting_requires_exact_sensor_isolated_counts(self):
        row = {'sensor_id': 'telemetry-a', 'offered': 3, 'generated': 3, 'raw': 300,
               'samples_expected': 300, 'ledger': 3, 'temporal': 3, 'acknowledged': 3}
        self.assertTrue(p6.validate_telemetry_accounting([row, {**row, 'sensor_id': 'telemetry-b'}]))
        # Application ACK totals are reported as observed counters, not inferred
        # from DB commits; the command-only simulator can ACK prior outbox rows.
        self.assertTrue(p6.validate_telemetry_accounting(
            [row, {**row, 'sensor_id': 'command-b', 'raw': 200}],
            require_sample_match=False))
        for field, value in (('generated', 2), ('raw', 299), ('raw_samples', 299), ('ledger', 2),
                             ('temporal', 2), ('acknowledged', 2)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                p6.validate_telemetry_accounting([row, {**row, 'sensor_id': 'telemetry-b', field: value}])

    def test_command_sensor_ack_mismatch_is_rejected(self):
        row = {'sensor_id': 'command-a', 'offered': 3, 'generated': 3, 'raw': 17,
               'samples_expected': None, 'ledger': 3, 'temporal': 3, 'acknowledged': 2}
        with self.assertRaises(ValueError):
            p6.validate_telemetry_accounting([row, {**row, 'sensor_id': 'command-b'}],
                                             require_sample_match=False)

    def test_metric_counter_deltas_require_same_process_and_monotonic_counters(self):
        before = {'process_instance': 'p', 'ingress': {'Committed': 2, 'ShaperRateLimited': 1}}
        after = {'process_instance': 'p', 'ingress': {'Committed': 5, 'ShaperRateLimited': 4}}
        self.assertEqual(p6.counter_deltas(before, after, ('Committed', 'ShaperRateLimited')),
                         {'Committed': 3, 'ShaperRateLimited': 3})
        for invalid in ({**after, 'process_instance': 'other'},
                        {'process_instance': 'p', 'ingress': {'Committed': 1, 'ShaperRateLimited': 4}}):
            with self.assertRaises(ValueError):
                p6.counter_deltas(before, invalid, ('Committed', 'ShaperRateLimited'))

    def test_exact_aggregate_accounting_keeps_message_and_sample_dimensions_separate(self):
        db = {'ledger': 100, 'temporal': 100, 'raw': 10_000, 'raw_samples': 10_000}
        ingress = {'Committed': 100, 'SamplesNew': 10_000, 'process_instance': 'p'}
        cli = {'offered': 100, 'application_acks': 100}
        self.assertTrue(p6.validate_aggregate_accounting(db, ingress, cli))
        for bad_db, bad_ingress, bad_cli in (
                ({**db, 'raw': 199}, ingress, cli),
                (db, {**ingress, 'SamplesNew': 199}, cli),
                (db, ingress, {**cli, 'offered': 1}),
                (db, ingress, {**cli, 'application_acks': 1}),
                (db, {**ingress, 'process_instance': ''}, cli)):
            with self.subTest(db=bad_db, ingress=bad_ingress, cli=bad_cli), self.assertRaises(ValueError):
                p6.validate_aggregate_accounting(bad_db, bad_ingress, bad_cli)

    def test_nested_command_lifecycle_waits_for_subscriptions_and_stops_both_in_finally(self):
        class Registry:
            def __init__(self):
                self.items, self.events = [], []
            def resolve(self, item):
                if item.get('kind') == 'volume':
                    return {'Name': item['name']}
                return item.get('object')
            def command(self, args):
                self.events.append(tuple(args))
                if args[0] == 'logs':
                    return SimpleNamespace(returncode=0, stderr=(
                        b'event=subscribed epoch=1 filter=gateways/gw-a/acks/#'))
                if args[0] == 'kill':
                    target = next(x for x in self.items if x['object']['Id'] == args[-1])
                    target['object']['State']['Running'] = False
                    return SimpleNamespace(returncode=0)
                raise AssertionError(args)

        class Smoke:
            def __init__(self, registry, fail=False):
                self.registry, self.fail = registry, fail
                self.calls = []
            def paper_p0_gateway_cli(self, fixture, registry, image, volume, env, ca, tag, args,
                                     state, during=None, timeout=45):
                self.calls.append((tag, tuple(args), timeout))
                container = {'kind': 'container', 'name': fixture.project + '-gateway-' + tag,
                    'object': {'Id': tag, 'State': {'Running': True}}}
                registry.items.append(container)
                if args == ['count']:
                    container['object']['State']['Running'] = False
                    return 'pending_rows=1 pending_bytes=100 quarantined_rows=0 quarantined_bytes=0'
                if during:
                    during()
                if self.fail:
                    raise RuntimeError('callback failure')
                container['object']['State']['Running'] = False
                return ('run summary: enqueued=2 publish_attempts=1 app_acknowledged=1 '
                    'pending_rows=1 pending_bytes=100 quarantined_rows=0 quarantined_bytes=0 completed=false')

        for fail, early, stop_fail in ((False, False, False), (True, False, False),
                                      (False, True, False), (True, True, True)):
            registry = Registry()
            smoke = Smoke(registry, fail)
            controls = [{'sensor_id': f's{i}', 'volume': {'kind': 'volume', 'name': f'vol{i}'},
                         'tag': f'command-{i}', 'env_path': Path(f'/private/{i}'), 'gateway_id': 'gw-a'}
                        for i in (1, 2)]
            fixture = SimpleNamespace(project='runx', private=Path('/private'))
            state = SimpleNamespace(checkpoint=lambda: None)
            callback_calls = []
            original_command = registry.command
            def command(args):
                result = original_command(args)
                if stop_fail and args[0] == 'kill':
                    raise OSError('stop diagnostic failure')
                return result
            registry.command = command
            def callback(stop=None):
                callback_calls.append('called')
                if early:
                    # Represents the command/config verification boundary while A is live.
                    callback_calls.append('configuration-verified')
                    if fail:
                        try:
                            raise RuntimeError('primary workload failure')
                        finally:
                            stop()
                    stop()
                    stop()
                    self.assertFalse(any(x['object']['State']['Running'] for x in registry.items))
                    callback_calls.append('a-returned')
                if fail:
                    raise RuntimeError('callback failure')
                return 'commands-done'
            if fail:
                with self.assertRaisesRegex(RuntimeError, 'primary workload failure' if early else 'callback failure'):
                    p6.run_command_simulators(smoke, fixture, registry, {'gateway': 'image'},
                                              controls, state, callback, comparison_stop_hook=early)
            else:
                result = p6.run_command_simulators(smoke, fixture, registry, {'gateway': 'image'},
                                                   controls, state, callback, comparison_stop_hook=early)
                self.assertEqual(result['callback_result'], 'commands-done')
                self.assertEqual(len(result['summaries']), 2)
                self.assertFalse(any(row['completed'] for row in result['summaries'].values()))
                self.assertEqual(result['post_stop_outbox_counts']['s1']['pending_rows'], 1)
                self.assertEqual(result['summaries']['primary']['enqueued'], 2)
                self.assertEqual(result['summaries']['primary']['app_acknowledged'], 1)
                self.assertEqual(result['summaries']['primary']['pending_bytes'], 100)
            kill_ids = [call[-1] for call in registry.events if call[0] == 'kill']
            self.assertEqual(set(kill_ids), {'command-1', 'command-2'}, (fail, kill_ids))
            self.assertEqual(len(kill_ids), 2)
            self.assertEqual(callback_calls, ['called'] +
                (['configuration-verified'] + ([] if fail else ['a-returned']) if early else []))

    def test_sensor_baselines_keep_gateway_identity_and_require_baseline(self):
        specs = [SimpleNamespace(gateway_id=gw, sensor_id='shared') for gw in ('a', 'b')]
        fields = ('ledger', 'raw', 'raw_samples', 'temporal')
        before = {('a', 'shared'): dict.fromkeys(fields, 2),
                  ('b', 'shared'): dict.fromkeys(fields, 5)}
        deltas = [p6.sensor_count_delta(spec, dict.fromkeys(fields, end), before)
                  for spec, end in zip(specs, (12, 15))]
        self.assertEqual([row['ledger'] for row in deltas], [10, 10])
        self.assertEqual(sum(row['ledger'] for row in deltas), 20)
        with self.assertRaises(KeyError):
            p6.sensor_count_delta(specs[0], dict.fromkeys(fields, 12), {})

    def test_command_helper_classifies_exact_error_envelope(self):
        for body, expected in (({'error': {'code': 'command_in_progress'}}, 'busy'),
                ({'error': {'code': 'desired_version_conflict'}}, 'rejected'),
                ({'error': {'code': 'idempotency_conflict'}}, 'rejected'),
                ({'code': 'active_busy'}, 'rejected'), ({'error': None}, 'rejected'),
                ({'error': []}, 'rejected'), (None, 'rejected')):
            with self.subTest(body=body):
                smoke = SimpleNamespace(http_request=lambda *a, **k: (409, {}, body))
                result = p6.issue_and_confirm_command(smoke, 'https://fixture', 'jwt', None,
                    None, 'urn:s', 4, prior_response={'desired_version': '0'})
                self.assertEqual(result['classification'], expected)
                self.assertEqual(result['counts']['busy'], int(expected == 'busy'))
                self.assertEqual(result['counts']['rejected'], int(expected == 'rejected'))

    def test_command_helper_uses_current_version_and_classifies_noop(self):
        calls = []
        class Smoke:
            @staticmethod
            def http_request(endpoint, method, path, **kwargs):
                calls.append((method, path, kwargs))
                if method == 'GET':
                    return 200, {}, {'desired_version': '8'}
                return 200, {}, {'code': 'no_op'}
        result = p6.issue_and_confirm_command(Smoke, 'https://fixture', 'jwt', None, None,
                                              'urn:ngsi-ld:Sensor:gw:s', 4)
        self.assertEqual(result['classification'], 'noop')
        self.assertEqual(result['counts']['noop'], 1)
        self.assertEqual(calls[-1][2]['body']['expected_desired_version'], '8')
        self.assertNotIn('jwt', str(result))

    def test_command_helper_confirms_requested_new_frequency_and_version(self):
        calls = []
        config = {'desired_version': '9', 'sample_interval_us': 500000}
        class Smoke:
            @staticmethod
            def db_psql(registry, pg, query, variables):
                return '{"desired_version":"9","sample_interval_us":500000}'
            @staticmethod
            def http_request(endpoint, method, path, **kwargs):
                calls.append((method, path, kwargs))
                if method == 'GET' and path.endswith('?format=simplified'):
                    return 200, {}, {'reported_state': {'configuration': config}}
                if method == 'GET':
                    return 200, {}, {'status': 'succeeded'}
                return 202, {}, {'status_url': '/command/status', 'desired_version': '9',
                                 'sample_interval_us': 500000}
        class Registry:
            @staticmethod
            def command(args):
                return SimpleNamespace(returncode=0, stdout=b'{"desired_version":"9","sample_interval_us":500000}')
        result = p6.issue_and_confirm_command(Smoke, 'https://fixture', 'jwt', Registry(), object(),
            'urn:ngsi-ld:Sensor:gw:s', 2, prior_response={'desired_version': '8'})
        admission = next(item for item in calls if item[0] == 'PATCH')
        self.assertEqual(admission[2]['body']['expected_desired_version'], '8')
        self.assertNotEqual(admission[2]['headers']['Idempotency-Key'], '')
        self.assertEqual(result['classification'], 'succeeded')
        self.assertEqual(result['desired_version'], '9')
        self.assertTrue(result['reported_configuration_verified'])

    def test_command_sequence_changes_frequency_and_carries_confirmed_version(self):
        observed = []
        def issue(smoke, endpoint, token, registry, pg, urn, hz, *, prior_response=None, timeout=35):
            observed.append((hz, prior_response))
            return {'classification': 'succeeded', 'desired_version': str(len(observed)),
                    'counts': {'accepted': 1, 'succeeded': 1}}
        with patch.object(p6, 'issue_and_confirm_command', side_effect=issue):
            result = p6.issue_command_sequence(None, None, None, None, None, 'urn:s',
                                               [1, 2, 4])
        self.assertEqual([item[0] for item in observed], [1, 2, 4])
        self.assertIsNone(observed[0][1])
        self.assertEqual(observed[1][1]['desired_version'], '1')
        self.assertEqual(observed[2][1]['desired_version'], '2')
        self.assertEqual(result['counts']['succeeded'], 3)
        with self.assertRaises(ValueError):
            p6.issue_command_sequence(None, None, None, None, None, 'urn:s', [2, 2.0])

    def test_p6_progress_call_sites_conform_to_keyword_only_signature(self):
        source = Path(p6.__file__).read_text()
        tree = ast.parse(source, filename=str(p6.__file__))
        violations = []
        calls_found = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'p6_progress':
                calls_found += 1
                # p6_progress signature: (event, *, tag='workload', count=None)
                # At most 1 positional argument (event) is allowed.
                if len(node.args) > 1:
                    violations.append((node.lineno, len(node.args), [ast.unparse(a) for a in node.args]))
                for kw in node.keywords:
                    if kw.arg not in ('tag', 'count'):
                        violations.append((node.lineno, f'invalid keyword {kw.arg}'))
        self.assertGreater(calls_found, 0)
        self.assertEqual(violations, [], f'Found invalid positional/keyword p6_progress call sites: {violations}')

    def test_measured_workload_progress_logging_exercises_all_phases_and_drains(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            artifacts_dir = temp_path / 'artifacts'
            private_dir = temp_path / 'private'
            artifacts_dir.mkdir()
            private_dir.mkdir()
            fixture = SimpleNamespace(
                project='test-p6-log',
                artifacts=artifacts_dir,
                private=private_dir,
                paper_p6_mode='wake-up',
                paper_p6_dataset='100',
                values=p6.profile('wake-up', '100')
            )

            class MockRegistry:
                def __init__(self):
                    self.items = []
                    self.created = {}
                def intent(self, kind, name, image, labels):
                    item = {'kind': kind, 'name': name, 'labels': labels}
                    self.items.append(item)
                    return item
                def resolve(self, item):
                    return self.created.get(item.get('name'))
                def allocating(self, item):
                    pass
                def command(self, args):
                    name = args[-1]
                    self.created[name] = {'State': {'Running': False, 'ExitCode': 0}, 'Name': name}
                    return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')

            class MockSmoke:
                @staticmethod
                def paper_p4_support():
                    return SimpleNamespace(safe_artifact=lambda x: x)
                @staticmethod
                def prepare_paper_workload(fix, reg, ep, cont, img, st):
                    pg = object()
                    quota = {}
                    effective = {}
                    resources = {}
                    storage = {}
                    tokens = {'admin': 'tok', 'a': 'tok'}
                    specs = [
                        SimpleNamespace(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:a', sensor_unit='C'),
                        SimpleNamespace(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:b', sensor_unit='C')
                    ]
                    passwords = ['pass-a', 'pass-b']
                    return pg, quota, effective, resources, storage, tokens, specs, passwords, None
                @staticmethod
                def durable_json(path, data):
                    pass
                @staticmethod
                def db_psql(registry, pg, query, variables):
                    return '{"ledger":0,"raw":0,"raw_samples":0,"temporal":0}'
                @staticmethod
                def http_request(endpoint, method, path, **kwargs):
                    if method == 'PUT' and '/sensors/' in path:
                        sensor = path.rsplit('/', 1)[-1]
                        return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:gw-a:{sensor}'}
                    return 200, {}, {
                        'command_admission': {},
                        'command_dispatch': {},
                        'command_timeline': {},
                        'command_result': {}
                    }
                @staticmethod
                def paper_p1_metrics_csv(path, rows):
                    pass
                @staticmethod
                def paper_p0_stats(registry, containers, phase):
                    return {}
                @staticmethod
                def paper_p1_resources_csv(path, rows, resources):
                    pass
                @staticmethod
                def private_write(path, contents):
                    Path(path).write_text(contents)
                @staticmethod
                def paper_p0_gateway_cli(fix, reg, img, vol, env, ca, tag, args, state, during=None, **kw):
                    if during:
                        during()
                    if args == ['count']:
                        return 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0'
                    return ('run summary: enqueued=1 publish_attempts=1 app_acknowledged=1 '
                            'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true')
                @staticmethod
                def paper_p1_histogram_window(before, after, group, name):
                    return {}

            state = SimpleNamespace(checkpoint=lambda: None)
            captured_stdout = io.StringIO()

            events = []
            def command_run(smk, fix, reg, img, cs, st, cb, **kw):
                self.assertTrue(kw['comparison_stop_hook'])
                def stop():
                    events.append('stop')
                result = cb(stop)
                events.append('a-returned')
                return {'summaries': {}, 'callback_result': result}
            def command_sequence(*args, **kwargs):
                events.append('configuration-verified')
                return {'classification': 'succeeded', 'counts': {'succeeded': 3}}
            with patch('sys.stdout', captured_stdout), \
                 patch.object(p6, 'run_command_simulators', side_effect=command_run), \
                 patch.object(p6, 'issue_command_sequence', side_effect=command_sequence):
                try:
                    p6.run(MockSmoke(), fixture, 'https://mock', MockRegistry(), {}, {'gateway': 'gw:test'}, state)
                except TypeError as e:
                    # Reraise TypeError so that positional argument errors fail the test
                    raise e
                except Exception:
                    # Downstream accounting expectation error after logging paths complete
                    pass

            self.assertEqual(events, ['configuration-verified', 'configuration-verified', 'stop', 'a-returned'])
            output = captured_stdout.getvalue()
            for expected_event in (
                'phase_enter', 'b_steady_start', 'command_sequence_start',
                'command_sequence_finished', 'b_burst_start', 'b_phases_finished',
                'phase_exit', 'drain_start', 'drain_end'
            ):
                self.assertIn(f'event={expected_event}', output, f'Missing progress event: {expected_event}')


if __name__ == '__main__':
    unittest.main()
