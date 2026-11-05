import base64
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

import paper_p8_comparison as comparison
import paper_p8_e5_support as e5
import paper_p8_e5_fixture as e5_fixture
from test_paper_p8_comparison import sample_report


class DatasetTests(unittest.TestCase):
    def test_dataset_is_repeatably_byte_identical_scoped_and_valid(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            a, b = root / 'a.jsonl', root / 'b.jsonl'
            one = e5.build_dataset(seed=91, sensor_ids=['shared-sensor', 'shared-sensor'],
                anchor='2026-10-28T00:00:00Z', path=a)
            two = e5.build_dataset(seed=91, sensor_ids=['shared-sensor', 'shared-sensor'],
                anchor='2026-10-28T00:00:00Z', path=b)
            self.assertEqual(a.read_bytes(), b.read_bytes())
            self.assertEqual(one['sha256'], two['sha256'])
            self.assertEqual(os.stat(a).st_mode & 0o777, 0o600)
            lines = a.read_text().splitlines()
            self.assertEqual(len(lines), 20)
            for index, line in enumerate(lines):
                row = json.loads(line)
                payload = base64.b64decode(row['payload'], validate=True)
                self.assertEqual(hashlib.sha256(payload).hexdigest(), row['payload_sha256'])
                body = json.loads(payload)
                self.assertEqual(body['gateway_id'], f'p8-e5-{"a" if index % 2 == 0 else "b"}')
                self.assertEqual(body['sensor_id'], 'shared-sensor')
                self.assertEqual(body['sample_count'], 100)
                self.assertEqual(body['sample_interval_us'], 10000)
                self.assertEqual(row['first_sequence'], str((index // 2) * 100 + 1))
                self.assertEqual(body['first_sequence'], (index // 2) * 100 + 1)
                self.assertEqual(len(body['message_id']), 36)
                self.assertEqual(body['message_id'][14], '4')
            bodies = [json.loads(base64.b64decode(json.loads(row)['payload'])) for row in lines]
            self.assertEqual(len({body['message_id'] for body in bodies}), 20)
            for gateway in ('p8-e5-a', 'p8-e5-b'):
                self.assertEqual(len({body['boot_id'] for body in bodies if body['gateway_id'] == gateway}), 1)

    def test_dataset_refuses_shared_or_nonprivate_parent_without_chmod(self):
        with tempfile.TemporaryDirectory() as td:
            parent = Path(td) / 'shared'
            parent.mkdir(mode=0o755)
            parent.chmod(0o755)
            with self.assertRaises(ValueError):
                e5.build_dataset(seed=1, sensor_ids=['a', 'a'], anchor='2026-10-28T00:00:00Z', path=parent/'x')
            self.assertEqual(os.stat(parent).st_mode & 0o777, 0o755)

    def test_dataset_rejects_invalid_ids_anchor_and_existing_output(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ValueError):
                e5.build_dataset(seed=1, sensor_ids=['bad/id', 'sensor-b'], anchor='2026-10-28T00:00:00Z', path=Path(td)/'x')
            with self.assertRaises(ValueError):
                e5.build_dataset(seed=1, sensor_ids=['a', 'b'], anchor='2026-10-28T00:00:00', path=Path(td)/'x')
            p = Path(td)/'x'
            e5.build_dataset(seed=1, sensor_ids=['a', 'b'], anchor='2026-10-28T00:00:00Z', path=p)
            with self.assertRaises(FileExistsError):
                e5.build_dataset(seed=1, sensor_ids=['a', 'b'], anchor='2026-10-28T00:00:00Z', path=p)


class ProfileAndAdapterTests(unittest.TestCase):
    def test_control_timeline_uses_real_histogram_and_counter_schema(self):
        def snapshot(count, total, missing=0, pending=0, process='proc-a'):
            hist = {'count': count, 'sum_seconds': total, 'buckets': [count, 0],
                    'upper_seconds': [0.5, 1.0], 'overflow': 0}
            return {'process_instance': process, 'command_timeline': {
                'd_dispatch': hist, 'd_e2e_succeeded': hist,
                'missing_origin': missing, 'missing_dispatch': 0, 'missing_result': 0,
                'overflow': 0, 'pending_origins': pending}}
        result = e5.control_timeline_window(snapshot(0, 0), snapshot(4, 2.0), expected_commands=4)
        self.assertEqual(result['status'], 'AVAILABLE')
        self.assertEqual(result['d_dispatch']['count'], 4)
        self.assertEqual(result['d_dispatch']['sum_seconds'], 2.0)
        self.assertEqual(result['missing_origin'], 0)
        self.assertTrue(result['count_matches_expected'])
        pending = e5.control_timeline_window(snapshot(0, 0), snapshot(0, 0, pending=1), expected_commands=0)
        self.assertFalse(pending['count_matches_expected'])

    def test_control_timeline_rejects_wrong_layer_restart_reset_and_corruption(self):
        hist = {'count': 0, 'sum_seconds': 0.0, 'buckets': [0],
                'upper_seconds': [1.0], 'overflow': 0}
        before = {'process_instance': 'p', 'command_timeline': {
            'd_dispatch': hist, 'd_e2e_succeeded': hist, 'missing_origin': 0,
            'missing_dispatch': 0, 'missing_result': 0, 'overflow': 0, 'pending_origins': 0}}
        after = json.loads(json.dumps(before))
        self.assertEqual(e5.control_timeline_window({'snapshot': before}, {'snapshot': before}, expected_commands=0)['status'], 'INCONCLUSIVE')
        after['process_instance'] = 'new'
        self.assertEqual(e5.control_timeline_window(before, after, expected_commands=0)['status'], 'INCONCLUSIVE')
        after = json.loads(json.dumps(before)); after['command_timeline']['d_dispatch']['count'] = -1
        self.assertEqual(e5.control_timeline_window(before, after, expected_commands=0)['status'], 'INCONCLUSIVE')
        after = json.loads(json.dumps(before)); after['command_timeline']['d_dispatch']['buckets'] = [1]
        self.assertEqual(e5.control_timeline_window(before, after, expected_commands=0)['status'], 'INCONCLUSIVE')

    def test_control_timeline_no_command_window_is_available_zero_delta(self):
        hist = {'count': 0, 'sum_seconds': 0.0, 'buckets': [0],
                'upper_seconds': [1.0], 'overflow': 0}
        snap = {'process_instance': 'p', 'command_timeline': {
            'd_dispatch': hist, 'd_e2e_succeeded': hist, 'missing_origin': 0,
            'missing_dispatch': 0, 'missing_result': 0, 'overflow': 0, 'pending_origins': 0}}
        self.assertEqual(e5.control_timeline_window(snap, snap, expected_commands=0)['status'], 'AVAILABLE')

    def test_all_profiles_reserve_combined_pool_budget_with_only_mechanism_variance(self):
        # Config reserves dispatch + result + publisher + ingress workers, then
        # one extra connection when credential management is enabled: 8 + 3 = 11.
        expected = e5.e5_arm_profile('optimized')
        self.assertEqual(expected['MQTT_WORKER_COUNT'], '4')
        self.assertEqual(expected['STAGE5_PUBLISH_WORKERS'], '1')
        self.assertEqual(expected['STAGE5_RESULT_WORKERS'], '2')
        self.assertEqual(expected['DATABASE_MAX_CONNS'], '11')
        for arm in e5.ARMS:
            profile = e5.e5_arm_profile(arm)
            self.assertEqual(profile['DATABASE_MAX_CONNS'], '11', arm)
            self.assertEqual(profile['MQTT_WORKER_COUNT'], '4', arm)
            self.assertEqual({key for key in profile if profile[key] != expected[key]},
                {key for key in comparison.get_expected_arm_flags(arm)
                 if comparison.get_expected_arm_flags(arm)[key] !=
                    comparison.get_expected_arm_flags('optimized')[key]})

    def test_all_profiles_have_exactly_one_mechanism_difference_and_fixed_safety(self):
        ref = e5.e5_arm_profile('optimized')
        for arm in e5.ARMS[1:]:
            flags = e5.e5_arm_profile(arm)
            self.assertEqual({k: v for k, v in flags.items() if flags[k] != ref[k]},
                             {k: v for k, v in comparison.get_expected_arm_flags(arm).items()
                              if v != comparison.get_expected_arm_flags('optimized')[k]})
            for key in ('STAGE5_ENABLED', 'WEBSOCKET_ENABLED'):
                self.assertEqual(flags[key], 'true')
            self.assertEqual(flags['MQTT_SHAPER_RATE_PER_SECOND'], '2')
            self.assertEqual(flags['WS_MAX_SESSIONS'], '32')
            self.assertEqual(flags['WS_SESSION_REFERENCE_BYTES'], str(8 << 20))

    def test_actual_counter_delta_and_missing_reset_inconclusive(self):
        before = {'process_instance': 'p', 'ingress': {'microbatch': {'groups': 2, 'members': 2, 'max_members': 1},
            'ShaperRateLimited': 0, 'ShaperPassed': 0},
            'ram_dedup': {'lookup_hit_total': 0, 'database_calls_avoided_total': 0},
            'command_dispatch': {'notification_signals_accepted': 0, 'puback': 0, 'scans': 0},
            'websocket_metrics': {'availability': 'available', 'subscriptions': 2, 'fanout_indexed_selections': 0,
                'fanout_scan_evaluations': 0, 'telemetry_writes_completed': 0}}
        after = json.loads(json.dumps(before))
        after['ingress']['microbatch'].update(groups=3, members=5, max_members=3)
        after['ram_dedup'].update(lookup_hit_total=4, database_calls_avoided_total=4)
        after['command_dispatch'].update(notification_signals_accepted=4, puback=4, scans=4)
        after['websocket_metrics'].update(fanout_indexed_selections=5, telemetry_writes_completed=5)
        metrics = e5.adapt_mechanism_counters(before, after, arm='optimized', verified_commands=4,
            viewer_readiness=[True, True])
        self.assertEqual(metrics['microbatch']['groups'], 1)
        self.assertEqual(metrics['microbatch']['multi_member_groups_lower_bound'], 1)
        self.assertEqual(metrics['microbatch']['max_members'], 3)
        self.assertEqual(metrics['ram_dedup']['lookup_hit_total'], 4)
        self.assertEqual(metrics['command']['notification_signals_accepted'], 4)
        self.assertEqual(metrics['command']['puback'], 4)
        self.assertEqual(metrics['fanout']['subscriptions_ready'], 2)
        self.assertEqual(metrics['fanout']['writes'], 5)
        missing = e5.adapt_mechanism_counters(before, {}, arm='optimized', verified_commands=4)
        self.assertEqual(missing['activation_status'], 'INCONCLUSIVE')
        after['process_instance'] = 'restarted'
        self.assertEqual(e5.adapt_mechanism_counters(before, after, arm='optimized')['activation_status'], 'INCONCLUSIVE')
        after['process_instance'] = 'p'
        before['ram_dedup']['lookup_hit_total'] = 3
        after['ram_dedup']['lookup_hit_total'] = 0
        self.assertEqual(e5.adapt_mechanism_counters(before, after, arm='optimized')['activation_status'], 'INCONCLUSIVE')

    def test_workload_has_separate_command_targets_and_key_declarations(self):
        manifest = e5.workload_manifest(journal_sha256='a'*64, journal_bytes=1234,
            command_ids=['key-a','key-b','key-c','key-d'], sensor_ids=['shared','shared'],
            viewer_targets=['viewer-a','viewer-b'], anchor='2026-10-28T00:00:00Z')
        self.assertEqual(manifest['journal_bytes'], 1234)
        self.assertEqual(manifest['commands']['idempotency_keys'], ['key-a','key-b','key-c','key-d'])
        self.assertEqual(manifest['commands']['command_sensor_ids'], ['p8-e5-command-a','p8-e5-command-b'])
        self.assertNotEqual(manifest['commands']['command_sensor_ids'], manifest['telemetry']['sensor_ids'])
        self.assertEqual(e5.command_idempotency_keys(17), e5.command_idempotency_keys(17))
        self.assertEqual(len(set(e5.command_idempotency_keys(17))), 4)
        self.assertTrue(all(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key)
                            for key in e5.command_idempotency_keys(17)))

    def test_run_plans_share_workload_seed_per_repeat_independent_of_order(self):
        arms = list(e5.ARMS)
        one = comparison.plan_run_order(arms, 3, 73)
        two = comparison.plan_run_order(arms, 3, 73)
        self.assertEqual(one['plan'], two['plan'])
        for rep in range(3):
            entries = [x for x in one['plan'] if x['repeat_index'] == rep]
            self.assertEqual(len({x['run_seed'] for x in entries}), 1)
            self.assertEqual(len({x['run_id'] for x in entries}), len(arms))
        self.assertEqual({x['run_seed'] for x in one['plan'] if x['repeat_index'] == 0}, {73})


class PrepareInputsTests(unittest.TestCase):
    def test_prepare_inputs_builds_four_real_preflight_manifests_and_private_context(self):
        class Smoke:
            def __init__(self): self.calls = []
            def http_request(self, endpoint, method, path, *, token, body=None):
                self.calls.append((method, path, token, body))
                if method == 'PUT':
                    return 201, {}, {'entity_id': path.rsplit('/', 1)[-1] and
                        f"urn:ngsi-ld:Sensor:p8-e5-a:{path.rsplit('/', 1)[-1]}"}
                return 200, {}, {'desired_version': '0'}

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / 'run'; root.mkdir(mode=0o700); root.chmod(0o700)
            source_dir = Path(td) / 'source'; source_dir.mkdir(mode=0o700); source_dir.chmod(0o700)
            source = source_dir / 'dataset.jsonl'
            dataset = e5.build_dataset(seed=91, sensor_ids=['shared-sensor', 'shared-sensor'],
                anchor='2026-10-28T00:00:00Z', path=source)
            specs = [SimpleNamespace(gateway_id=gw, sensor_id='shared-sensor', sensor_unit='C')
                     for gw in ('p8-e5-a', 'p8-e5-b')]
            endpoint = SimpleNamespace(url='https://localhost:8443', ca_file=Path(td)/'https-ca.pem')
            fixture = SimpleNamespace(project='fixture')
            smoke = Smoke()
            subscriptions = [
                {'viewer_id': 'e5-viewer-a', 'user_id': 'user-a', 'gateway_id': 'p8-e5-a', 'sensor_id': 'shared-sensor'},
                {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b'},
                {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b', 'sensor_id': 'shared-sensor'},
            ]
            result = e5_fixture.prepare_inputs(smoke=smoke, fixture=fixture, endpoint=endpoint,
                specs=specs, passwords=['pw-a', 'pw-b'], tokens={'a': 'token-a', 'b': 'token-b'},
                mqtt_broker_url='mqtts://localhost:8883', mqtt_manifest_endpoint='mqtts://localhost:8883',
                mqtt_ca_file=Path(td)/'mqtt-ca.pem', journal_path=source,
                journal_context={**dataset, 'anchor': '2026-10-28T00:00:00Z', 'seed': 91},
             seed=91, anchor='2026-10-28T00:00:00Z', root=root,
             command_keys=e5.command_idempotency_keys(91), viewer_subscriptions=subscriptions,
             command_versions={'p8-e5-command-a': '0', 'p8-e5-command-b': '0'},
             workload_selection=e5.workload_preset('representative'))
            manifests = {name: json.loads(Path(result['manifests'][key]).read_text())
                for name, key in (('initial', 'initial'), ('warm', 'warm_replay'),
                                  ('viewer', 'viewer'), ('control', 'control'))}
            self.assertEqual(manifests['initial']['e4']['replay_count'], 8)
            self.assertEqual(manifests['warm']['e4']['replay_count'], 2)
            self.assertEqual(manifests['viewer']['schema_version'], 1)
            for name in ('initial', 'warm', 'viewer', 'control'):
                self.assertEqual(manifests[name]['schema_version'], 1, name)
            for name in ('initial', 'warm'):
                manifest = manifests[name]
                traffic, phases = manifest['traffic'], manifest['phases']
                offered = traffic['rate_per_second'] * phases['measure_seconds'] + traffic['burst_messages']
                self.assertEqual(offered, manifest['e4']['replay_count'], name)
            self.assertEqual(manifests['warm']['traffic'],
                             {'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': 0})
            self.assertEqual(manifests['initial']['traffic'],
                             {'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': 7})
            self.assertEqual(manifests['warm']['e4']['journal_path'], manifests['initial']['e4']['journal_path'])
            self.assertEqual(manifests['initial']['experiment'], 'E4')
            self.assertEqual(manifests['control']['experiment'], 'E3')
            self.assertEqual(manifests['control']['commands'][0]['actor_label'], 'e3-operator')
            self.assertEqual(len(manifests['viewer']['subscriptions']), 3)
            self.assertEqual(set(json.loads(Path(manifests['viewer']['identity_files']['viewer']).read_text())),
                             {'user-a', 'user-b'})
            self.assertEqual(result['expected'], {'main_messages': 8, 'raw_samples': 800, 'temporal': 8,
                'initial_schedule': {'rate_per_second': 1, 'measure_seconds': 1, 'burst_messages': 7},
                'viewers': 2, 'commands': 4})
            self.assertEqual(result['workload']['schedule']['initial_replay'],
                {'rate_per_second': 1, 'measure_seconds': 1, 'burst_messages': 7})
            self.assertEqual(len(smoke.calls), 4)
            for path in [*result['manifests'].values()]:
                if isinstance(path, list):
                    for item in path: self.assertEqual(Path(item).stat().st_mode & 0o777, 0o600)
                elif path.endswith('.json'): self.assertEqual(Path(path).stat().st_mode & 0o777, 0o600)
            binary = os.environ.get('P8_TEST_PAPERLOAD')
            if binary:
                failures = []
                for key in ('initial', 'warm_replay', 'viewer', 'control'):
                    output = root / (key + '-preflight')
                    completed = subprocess.run([binary, '--manifest', result['manifests'][key],
                        '--output-dir', str(output), '--preflight'], text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
                    if completed.returncode != 0:
                        failures.append(f"{key}: {completed.stdout} {completed.stderr}; manifest="
                                        f"{Path(result['manifests'][key]).read_text()}")
                self.assertFalse(failures, '\n'.join(failures))


class StagedRuntimeGateTests(unittest.TestCase):
    def test_replay_accounting_uses_actual_replay_ack_field(self):
        workload = {'mode': 'replay', 'journal_entry_count': 20}
        telemetry = {'generated_unique': 0, 'offered_unique': 0,
            'replay_offered': 20, 'replay_application_acks': 20,
            'pending_messages': 0, 'pending_bytes': 0}
        self.assertTrue(e5.replay_accounting_matches(workload, telemetry, 20))
        telemetry['replay_application_acks'] = 19
        self.assertFalse(e5.replay_accounting_matches(workload, telemetry, 20))

    def test_phase_shutdown_uses_existing_two_error_cleanup_contract(self):
        from unittest.mock import Mock
        collector = Mock()
        proc = Mock(pid=123)
        proc.poll.return_value = 0
        self.assertEqual(e5_fixture._stop_collector_and_process(collector, proc), (None, None))
        collector.stop.assert_called_once_with(123)
        proc.terminate.assert_not_called()

    def test_actual_runtime_accounting_and_viewer_shape_rejects_missing_or_changed_proof(self):
        workload = {'e4': {'mode': 'replay', 'journal_entry_count': 20,
                           'safe_header_samples': [{'message_id': f'id-{i}'} for i in range(20)]},
            'telemetry': {'generated_unique': 0, 'offered_unique': 0,
                'replay_offered': 20, 'replay_application_acks': 20,
                'application_acks': 20, 'pending_messages': 0, 'pending_bytes': 0}}
        context = {'viewer_subscriptions': [{'viewer_id': 'viewer-one'}, {'viewer_id': 'viewer-two'}]}
        viewer = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [{'id': 'viewer-one', 'unique_events': 10},
                        {'id': 'viewer-two', 'unique_events': 10}],
            'identity_samples': [{'message_id': 'id-1'}]}
        ws_initial = {'submissions': 20, 'new_events': 20, 'deliveries_enqueued': 20,
            'telemetry_writes_completed': 20, 'drops': 0}
        ws_warm = {key: (0 if key != 'drops' else 0) for key in ws_initial}
        end = {'websocket_metrics': {'global_queued_work': 0, 'global_queued_bytes': 0}}
        valid = e5_fixture._validate_e5_viewer(viewer, context, workload,
            dict(ws_initial), dict(ws_warm), end)
        self.assertTrue(valid['valid'], valid['failures'])
        missing = e5_fixture._validate_e5_viewer({}, context, workload,
            dict(ws_initial), dict(ws_warm), end)
        changed = dict(viewer, identity_samples=[{'message_id': 'not-in-journal'}])
        fingerprint = e5_fixture._validate_e5_viewer(changed, context, workload,
            dict(ws_initial), dict(ws_warm), end)
        self.assertFalse(missing['valid'])
        self.assertFalse(fingerprint['valid'])

    def test_runtime_rejects_unpaired_or_unknown_arm_before_fixture_use(self):
        with self.assertRaisesRegex(ValueError, 'e5_arm_rejected'):
            e5_fixture.run(None, None, None, None, None, None, None,
                           'unused-paperload', 'unused-gateway', 'not-an-arm', '/unused/context')

    def test_post_stop_drain_accepts_only_clean_partial_with_complete_output(self):
        from paper_p8_e3_fixture import _eligible_post_stop_drain
        artifact = {'status': 'PARTIAL', 'primary_error': None, 'cleanup_errors': [],
            'phase_observations': {'errors': []}, 'actual_stop_confirmed': True,
            'children': [{'status': 'complete'}]}
        output = [{'status': 'COMPLETE'}]
        executor = [{'pending_outbox': 1}]
        self.assertTrue(_eligible_post_stop_drain(artifact, output, executor))
        for key, value in (('actual_stop_confirmed', False), ('cleanup_errors', ['failure']),
                           ('primary_error', 'failure')):
            rejected = dict(artifact, **{key: value})
            self.assertFalse(_eligible_post_stop_drain(rejected, output, executor))
        self.assertFalse(_eligible_post_stop_drain(artifact,
            [{'status': 'FAILED'}], executor))

    def test_run_uses_fixture_private_root_and_persists_safe_failure_from_whole_entrypoint(self):
        class Fixture:
            project = 'whole-run'
            def __init__(self, root):
                self.private = root / 'private'; self.private.mkdir(mode=0o700)
                self.artifacts = root / 'artifacts'; self.artifacts.mkdir(mode=0o700)
        class Smoke:
            ROOT = Path.cwd()
            @staticmethod
            def durable_json(path, value):
                Path(path).write_text(json.dumps(value))

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fixture = Fixture(root)
            context = root / 'shared-context.json'
            context.write_text('{}'); context.chmod(0o600)
            report = e5_fixture.run(Smoke(), fixture, None, None, None, None, None,
                'paperload', 'gateway', 'optimized', context)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual(Path(report.get('private_root', fixture.private / 'p8-e5')).parent, fixture.private)
            self.assertEqual(report['primary_error']['exception'], 'ValueError')
            self.assertEqual(report['primary_error']['phase'], 'preflight')
            self.assertEqual(report['primary_error']['code'], 'e5_dataset_context_invalid')
            saved = json.loads((fixture.private / 'p8-e5-result.json').read_text())
            self.assertEqual(saved['primary_error'], report['primary_error'])

    def test_whole_run_unknown_error_does_not_disclose_unsafe_message(self):
        class Fixture:
            project = 'whole-run-unsafe'
            def __init__(self, root):
                self.private = root / 'private'; self.private.mkdir(mode=0o700)
                self.artifacts = root / 'artifacts'; self.artifacts.mkdir(mode=0o700)
        class Smoke:
            ROOT = Path.cwd()
            def prepare_paper_workload(self, *args, **kwargs):
                raise RuntimeError('password=do-not-leak; SQL SELECT secret')
            @staticmethod
            def durable_json(path, value):
                Path(path).write_text(json.dumps(value))

        with tempfile.TemporaryDirectory() as td:
            root = Path(td); fixture = Fixture(root)
            dataset_dir = root / 'dataset'; dataset_dir.mkdir(mode=0o700)
            dataset = e5.build_dataset(seed=7, sensor_ids=['a', 'b'],
                anchor='2026-10-28T00:00:00Z', path=dataset_dir / 'journal.jsonl')
            context = root / 'context.json'
            selection = e5.workload_preset('pilot')
            context.write_text(json.dumps({**dataset, 'dataset_path': str(dataset_dir / 'journal.jsonl'),
                'seed': 7, 'anchor': '2026-10-28T00:00:00Z', 'sensor_ids': ['a', 'b'],
                'gateway_ids': list(e5_fixture.GATEWAYS), 'command_keys': e5.command_idempotency_keys(7),
                'viewer_subscriptions': [], 'workload_selection': selection,
                'workload_selection_sha256': e5.selection_sha256(selection)}))
            context.chmod(0o600)
            report = e5_fixture.run(Smoke(), fixture, None, None, None, None, None,
                'paperload', 'gateway', 'optimized', context)
            encoded = json.dumps(report['primary_error'])
            self.assertEqual(report['primary_error']['phase'], 'preflight')
            self.assertEqual(report['primary_error']['code'], None)
            self.assertEqual(report['primary_error']['location']['file'], 'test_paper_p8_e5_support.py')
            self.assertNotIn('do-not-leak', encoded)
            self.assertNotIn('SELECT secret', encoded)


if __name__ == '__main__':
    unittest.main()
