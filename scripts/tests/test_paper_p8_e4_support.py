import importlib.util
import hashlib
import base64
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('e4', Path(__file__).with_name('paper_p8_e4_support.py'))
e4 = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(e4)


def sample(mode='on'):
    unique = {'status': 'COMPLETE', 'telemetry': {
        'generated_unique': 20, 'offered_unique': 20, 'application_acks': 20,
        'pending_messages': 0, 'pending_bytes': 0, 'ignored_valid_acks': 1,
        'retries': 1, 'broker_pubacks': 21},
        'e4': {'mode': 'unique', 'journal_entry_count': 20,
               'safe_header_samples': ['header-' + str(i) for i in range(20)]}}
    replay = {'status': 'COMPLETE', 'telemetry': {
        'generated_unique': 0, 'offered_unique': 0, 'replay_offered': 10,
        'replay_application_acks': 10, 'application_acks': 0,
        'pending_messages': 0, 'pending_bytes': 0, 'broker_pubacks': 10},
        'e4': {'mode': 'replay', 'journal_entry_count': 10,
               'safe_header_samples': ['header-' + str(i) for i in range(10)]}}
    cache = ({'warm_lookup_hits': 10, 'repository_calls_avoided': 10,
              'database_duplicates': 0, 'confirmed_new_rows': 0, 'copy_calls': 0,
              'transaction_commits': 0, 'attempts': 0} if mode == 'on'
             else {'database_duplicates': None, 'confirmed_new_rows': 0, 'copy_calls': 0,
                   'transaction_commits': 10, 'attempts': 10})
    def window(delta):
        before = {k: 0 for k in ('Committed', 'SamplesNew', 'DuplicateCommitted', 'ACKPublished')}
        after = {k: before[k] + delta.get(k, 0) for k in before}
        return {'available': True, 'before': before, 'after': after, 'delta': delta, 'cache': dict(cache)}
    uw, rw = window({'Committed': 20, 'SamplesNew': 2000,
        'DuplicateCommitted': (0 if mode == 'on' else 1), 'ACKPublished': 21}), window({'Committed': 0,
        'SamplesNew': 0, 'DuplicateCommitted': 10, 'ACKPublished': 10})
    fingerprints = [{'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0,
                     'fingerprint': 'a' * 32}] * 2
    rw['database_proof'] = {'unique_ack': fingerprints, 'replay_end': fingerprints,
                            'exact_equal': True}
    return unique, replay, uw, rw


class E4SupportTests(unittest.TestCase):
    def test_two_viewer_manifest_is_e1_viewer_only_and_bounded(self):
        common = {'topology': {'gateways': 2}, 'durability_mode': 'synthetic-load',
                  'gateway_identity_files': {'gw': 'secret'}}
        manifest = e4.e4_viewer_manifest(common, subscriptions=[{}, {}],
                                         viewer_identity='/private/viewer.json', root='/private')
        self.assertEqual(manifest['experiment'], 'E1')
        self.assertEqual(manifest['roles'], ['viewer'])
        self.assertEqual(manifest['limits']['connections'], 2)
        self.assertEqual(manifest['limits']['workers'], 2)
        self.assertNotIn('durability_mode', manifest)
        self.assertNotIn('gateway_identity_files', manifest)
        self.assertNotIn('telemetry', manifest['identity_files'])
        with self.assertRaises(ValueError):
            e4.e4_viewer_manifest(common, subscriptions=[{}], viewer_identity='/private/v.json', root='/private')

    def test_ws_proof_uses_real_viewer_result_and_server_metric_fields(self):
        counters = {'submissions': 20, 'new_events': 20, 'deliveries_enqueued': 20,
                    'telemetry_writes_completed': 20, 'drops': 0}
        peer = {'unique_events': 10, 'duplicates': 0, 'close_reason': 'measurement_complete',
                'id': 'e4-viewer-a'}
        state = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [peer, {**peer, 'id': 'e4-viewer-b'}]}
        good = e4.validate_e4_ws_proof(unique_delta=counters,
            replay_delta={'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
                'telemetry_writes_completed': 0, 'drops': 0}, state=state)
        self.assertEqual(good['status'], 'PASS')
        bad = e4.validate_e4_ws_proof(unique_delta=counters,
            replay_delta={'submissions': 1, 'new_events': 1, 'deliveries_enqueued': 0,
                'telemetry_writes_completed': 0, 'drops': 0}, state=state)
        self.assertIn('replay_ws_submissions', bad['failures'])

    def test_viewer_runtime_preflights_launches_waits_and_reads_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, output, log = root / 'viewer.json', root / 'out', root / 'viewer.log'
            manifest.write_text('{}')
            proc = Mock()
            proc.poll.return_value = None
            order = []
            with patch('scripts.paper_p8.invoke_paperload', side_effect=lambda *a, **k: order.append('preflight')) as preflight, \
                    patch('scripts.paper_p8._wait_viewer_ready', side_effect=lambda *a: order.append('ready')) as ready, \
                    patch.object(e4.subprocess, 'Popen', side_effect=lambda *a, **k: (order.append('launch'), proc)[1]) as popen:
                started = e4.start_e4_viewer(paperload_binary='/bin/true', manifest_path=manifest,
                    output_dir=output, log_path=log)
            self.assertIs(started, proc)
            preflight.assert_called_once()
            ready.assert_called_once_with(proc, output, 2, 15)
            self.assertEqual(popen.call_args.args[0][-1], '--run')
            self.assertEqual(order, ['preflight', 'launch', 'ready'])
            self.assertTrue(log.exists())
            (output).mkdir()
            (output / 'workload.json').write_text(json.dumps({'status': 'COMPLETE'}))
            proc.poll.return_value = 0; proc.returncode = 0
            self.assertEqual(e4.finish_e4_viewer(proc, output)['status'], 'COMPLETE')

    def test_viewer_readiness_error_joins_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'viewer.json'; manifest.write_text('{}')
            proc = Mock(); proc.poll.return_value = None
            with patch('scripts.paper_p8.invoke_paperload'), \
                    patch('scripts.paper_p8._wait_viewer_ready', side_effect=RuntimeError('no ready')), \
                    patch.object(e4.subprocess, 'Popen', return_value=proc):
                with self.assertRaisesRegex(RuntimeError, 'no ready'):
                    e4.start_e4_viewer(paperload_binary='/bin/true', manifest_path=manifest,
                        output_dir=root / 'out', log_path=root / 'viewer.log')
            proc.terminate.assert_called_once()
            proc.wait.assert_called_once_with(timeout=2)

    def validate(self, mode='on'):
        unique, replay, uw, rw = sample(mode)
        return e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode=mode)

    def test_actual_runresult_shape_and_warm_replay_accounting(self):
        result = self.validate()
        self.assertTrue(result['valid'], result['failures'])

    def test_cache_off_requires_actual_database_duplicate_path(self):
        self.assertTrue(self.validate('off')['valid'])
        unique, replay, uw, rw = sample('off')
        rw['cache']['attempts'] = 0
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='off')
        self.assertIn('cache_off_database_path_unproven', result['failures'])

    def test_wrong_cache_evidence_is_rejected(self):
        unique, replay, uw, rw = sample('off')
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('cache_warm_hit_unproven', result['failures'])

    def test_missing_counters_and_counter_reset_fail_closed(self):
        unique, replay, uw, rw = sample()
        del replay['telemetry']['replay_offered']
        uw['available'] = None
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {}}, raw_checks=[], unique_window=uw,
            replay_window=rw, cache_mode='on')
        self.assertIn('replay_replay_offered', result['failures'])
        self.assertIn('unique_counter_window_unavailable', result['failures'])

    def test_empty_snapshot_is_not_equal_database_proof(self):
        unique, replay, uw, rw = sample()
        rw['database_proof'] = {'unique_ack': {}, 'replay_end': {}, 'exact_equal': True}
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('replay_database_fingerprint_changed', result['failures'])

    def test_cache_on_e4_runtime_lane_replaces_then_runs_cold_replay(self):
        sentinel = {'replacement': True}
        replace, replay = Mock(return_value=sentinel), Mock(return_value={'status': 'PASS'})
        with unittest.mock.patch.object(e4._cold, 'replace_owned_backend', replace), \
             unittest.mock.patch.object(e4, '_run_cold_replay', replay):
            result = e4._execute_cold_lane('smoke', 'fixture', 'endpoint', 'registry',
                'containers', 'binary', 'root', 'manifest', 'journal', 'pg', 'tokens',
                'specs', 'old-process', {'warm': True}, 'state', {})
        self.assertEqual(result, {'status': 'PASS'})
        replace.assert_called_once_with('smoke', 'fixture', 'registry', 'containers', 'state')
        self.assertEqual(replay.call_args.args[14], sentinel)

    def test_replay_headers_must_match_unique_journal_prefix(self):
        unique, replay, uw, rw = sample()
        replay['e4']['safe_header_samples'] = ['different'] * 10
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('replay_journal_content_mismatch', result['failures'])

    def test_cache_on_accepts_zero_unique_db_duplicates_but_requires_replay_no_db_work(self):
        unique, replay, uw, rw = sample()
        uw['delta']['DuplicateCommitted'] = 0
        self.assertTrue(self.validate('on')['valid'])
        rw['cache']['copy_calls'] = 1
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('cache_on_database_work_unexpected', result['failures'])

    def test_effective_flags_require_boolean_strings_and_normalize_ram_mode(self):
        env = {'WEBSOCKET_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
               'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'false',
               'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true',
               'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
               'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
        self.assertEqual(e4._flag_profile({'services': {'backend': {'environment': env}}}, 'on'), env)
        env.pop('STAGE5_ENABLED')
        with self.assertRaises(ValueError):
            e4._flag_profile({'services': {'backend': {'environment': env}}}, 'on')

    def test_actual_admin_toplevel_groups_and_counter_reset(self):
        before = {'process_instance': 'backend-1', 'ram_dedup': {'lookup_hit_total': 4, 'lookup_miss_total': 2,
                                'database_calls_avoided_total': 3},
                  'persistence': {'begin_attempts': 8, 'confirmed_commits': 5,
                                  'copy_calls': 2, 'confirmed_new_rows': 200}}
        after = {'process_instance': 'backend-1', 'ram_dedup': {'lookup_hit_total': 7, 'lookup_miss_total': 3,
                               'database_calls_avoided_total': 6},
                 'persistence': {'begin_attempts': 8, 'confirmed_commits': 5,
                                 'copy_calls': 2, 'confirmed_new_rows': 200}}
        window = e4._cache_counter_window(before, after)
        self.assertEqual(window['warm_lookup_hits'], 3)
        self.assertEqual(window['repository_calls_avoided'], 3)
        self.assertEqual(window['attempts'], 0)
        after['ram_dedup']['lookup_hit_total'] = 1
        self.assertIsNone(e4._cache_counter_window(before, after)['warm_lookup_hits'])
        after['process_instance'] = 'backend-2'
        self.assertIsNone(e4._cache_counter_window(before, after))
        self.assertIsNone(e4._cache_counter_window(before, {}) )

    def test_counts_with_fingerprint_only_delta_numeric_database_counts(self):
        before = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': 'a' * 32}]
        after = [{'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0, 'fingerprint': 'b' * 32}]
        self.assertEqual(e4._database_delta(before, after),
                         {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0})

    def test_collector_stop_error_does_not_skip_process_stop(self):
        collector = Mock()
        collector.stop.side_effect = RuntimeError('scrape close failed')
        proc = Mock(pid=123)
        proc.poll.return_value = None
        error = e4._stop_collector_and_process(collector, proc)
        self.assertIsInstance(error, RuntimeError)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=2)

    def test_workload_and_cleanup_failure_are_not_accepted(self):
        unique, replay, uw, rw = sample()
        unique['cleanup_errors'] = ['mqtt_transport_stop_failed']
        failed = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('unique_cleanup_failure', failed['failures'])
        unique['cleanup_errors'] = []
        unique['status'] = 'PARTIAL'
        failed = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('unique_result_incomplete', failed['failures'])

    def test_conflict_requires_schema_guard_backend_reason_zero_ack_and_unchanged_db(self):
        good = e4.validate_representative_conflict(schema_valid=True, guard_precondition=True,
            rejection_reason='sequence_overlap', expected_reason='sequence_overlap', ack_count=0,
            before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertEqual(good['status'], 'PASS')
        missing_guard = e4.validate_representative_conflict(schema_valid=True,
            guard_precondition=False, rejection_reason=None, expected_reason='sequence_overlap',
            ack_count=0, before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertIn('conflict_guard_precondition_missing', missing_guard['failures'])
        self.assertIn('conflict_backend_rejection_unproven', missing_guard['failures'])
        ack_only = e4.validate_representative_conflict(schema_valid=True,
            guard_precondition=True, rejection_reason=None, expected_reason='sequence_overlap',
            ack_count=0, before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertNotEqual(ack_only['status'], 'PASS')

    def test_representative_conflicts_publish_real_journal_bytes_and_stop_observer(self):
        from types import SimpleNamespace
        from datetime import datetime, timezone
        from unittest.mock import Mock

        import paper_p5a_support as p5a
        message_id = 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3'
        payload = json.dumps({'protocol_version': 1, 'message_id': message_id,
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
            'boot_id': '2c285db6-eef6-4ca0-b613-9f1663f1d95a', 'first_sequence': 42,
            'sample_count': 100, 'measured_at': '2026-10-26T10:00:00Z',
            'sample_interval_us': 10000, 'samples': [21.5 + i / 100 for i in range(100)]},
            separators=(',', ':')).encode()
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / 'dataset.jsonl'
            journal.write_text(json.dumps({'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
                'message_id': message_id, 'first_sequence': '42', 'sample_count': 100,
                'payload_sha256': hashlib.sha256(payload).hexdigest(),
                'payload': base64.b64encode(payload).decode()}) + '\n')
            spec = SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a', sensor_urn='urn:gw-a:sensor-a')
            fixture = SimpleNamespace(private=Path(tmp))
            registry = Mock()
            registry.command.side_effect = [
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'reason_code=message_conflict reason_code=sequence_overlap', stderr=b''),
            ] * 2
            smoke = Mock()
            smoke.db_psql.side_effect = lambda *args: (
                't' if 'SELECT EXISTS' in args[2] else
                json.dumps({'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0})
                if 'SELECT json_build_object' in args[2] else 'a' * 32)
            ack = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack',
                'message_id': message_id, 'status': 'committed',
                'committed_at': datetime.now(timezone.utc).isoformat()}).encode()
            observer_log = Path(tmp) / 'observer.log'
            observer_log.write_bytes(b'')
            observer = (Mock(), Mock(), observer_log, None)
            observer[0].poll.return_value = None
            smoke.paper_p1_subscriber.return_value = observer
            smoke.paper_p1_capture_subscription.side_effect = [ack, None, ack, None]
            def observed_test_messages(_data):
                calls = smoke.paper_p1_capture_subscription.call_args_list[-2:]
                base = len(smoke.paper_p1_capture_subscription.call_args_list) - 2
                return [(call.args[1], b'') for index, call in enumerate(calls, base)
                        if call.args[1].startswith('gateways/gw-a/acks/') and
                           [ack, None, ack, None][index] is not None]
            smoke.paper_p1_subscription_messages.side_effect = observed_test_messages
            smoke.paper_p1_stop_subscription.return_value = None
            containers = {'mosquitto': {'receipt': {'id': 'broker-id'}},
                          'backend': {'receipt': {'id': 'backend-id'}}}
            with patch.object(p5a, 'conflict_payloads', wraps=p5a.conflict_payloads):
                result = e4.run_representative_conflicts(smoke, fixture, registry, containers,
                    'pg-id', [spec], ['gateway-secret'], journal)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(set(result['cases']), {'same_id_changed_payload', 'different_first_sequence_overlap'})
        self.assertEqual(smoke.paper_p1_subscriber.call_count, 2)
        self.assertEqual(smoke.paper_p1_stop_subscription.call_count, 2)
        calls = smoke.paper_p1_capture_subscription.call_args_list
        self.assertEqual([call.kwargs.get('after_count') for call in calls], [None, 1, None, 0])
        publish_calls = [call for call in registry.command.call_args_list
                         if call.args[0][0] == 'exec' and call.kwargs.get('input')]
        self.assertEqual(len(publish_calls), 4)
        self.assertTrue(publish_calls[0].kwargs['input'].endswith(payload))
        self.assertTrue(publish_calls[2].kwargs['input'].endswith(payload))

    def test_representative_conflict_missing_backend_reason_fails_even_without_ack(self):
        result = e4.validate_representative_conflict(schema_valid=True, guard_precondition=True,
            rejection_reason=None, expected_reason='sequence_overlap', ack_count=0,
            before_db={'fingerprint': 'a' * 32}, after_db={'fingerprint': 'a' * 32})
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('conflict_backend_rejection_unproven', result['failures'])

    def test_durable_ignore_and_reopen_requires_exact_pending_identity_and_join(self):
        before = {'payload_sha256': 'abc', 'payload_bytes': 12, 'header': {'v': 1},
                  'pending_messages': 1, 'ack_deleted': 0, 'message_id': 'id',
                  'sequence': 7, 'interval_us': 10000}
        replay = {'status': 'COMPLETE', 'old_payload_sha256': 'abc', 'old_message_id': 'id',
                  'old_sequence': 7, 'old_interval_us': 10000, 'old_boot_id': 'boot',
                  'old_raw_count': 100, 'old_sample_count': 100,
                  'old_ledger_fingerprint': 'fp', 'generated': 1,
                  'application_acks': 2}
        before['header']['boot_id'] = 'boot'; before['ledger_fingerprint'] = 'fp'
        args = dict(intentional_stop=True, ignored_valid_acks=1, ack_deleted=0, before=before,
                    committed_payload_sha256='abc', replay=replay, final_pending=0,
                    final_quarantine=0, joined=True,
                    main_dataset_counts={'unchanged': True, 'before': {'fingerprint': 'x'}, 'after': {'fingerprint': 'x'}})
        result = e4.validate_durable_reopen(**args)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(result['extra_generated'], 1)
        self.assertEqual(result['extra_acks'], 2)
        args['joined'] = False
        self.assertIn('durable_reopen_process_not_joined', e4.validate_durable_reopen(**args)['failures'])
        args['joined'] = True
        args['replay'] = dict(replay, old_sequence=8)
        self.assertIn('durable_replay_identity_changed', e4.validate_durable_reopen(**args)['failures'])

    def run_probe(self, *, mismatch=None, unexpected_fault=False, timeout=False):
        from types import SimpleNamespace
        message_id = 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3'
        payload = json.dumps({'protocol_version': 1, 'message_id': message_id, 'message_type': 'telemetry_batch',
            'gateway_id': 'gw', 'sensor_id': 'p8-e4-durable', 'boot_id': 'boot-a',
            'first_sequence': 11, 'sample_count': 100, 'measured_at': '2026-10-28T00:00:00+00:00',
            'sample_interval_us': 10000, 'samples': list(range(100))}, separators=(',', ':')).encode()
        digest = hashlib.sha256(payload).hexdigest()
        identity = {'hash': digest, 'ledger_fingerprint': 'ledger-fp', 'raw': 100,
                    'boot': 'boot-a', 'first_sequence': 11, 'interval_us': 10000,
                    'sample_count': 100}
        if mismatch:
            identity[mismatch] = 99 if isinstance(identity[mismatch], int) else 'wrong'
        aux_reads = iter(({'ledger': 0, 'raw': 0, 'temporal': 0},
                          {'ledger': 2, 'raw': 200, 'temporal': 2}))

        class Smoke:
            ROOT = Path.cwd()
            def http_request(self, *args, **kwargs):
                return 201, {}, {'entity_id': 'urn:ngsi-ld:Sensor:gw:p8-e4-durable'}
            def private_write(self, path, text): Path(path).write_text(text)
            def process_env(self, fixture): return {}
            def db_psql(self, registry, pg, sql, params):
                if "'ledger',(SELECT count(*)" in sql:
                    return json.dumps(next(aux_reads))
                if "'last_sequence'" in sql:
                    return json.dumps({'hash': digest, 'ledger_fingerprint': 'ledger-fp',
                        'raw': identity.get('raw'), 'boot': 'boot-a', 'first_sequence': 11,
                        'last_sequence': 110, 'sample_count': 100})
                if "'ledger_fingerprint'" in sql and 'p.payload_hash' in sql:
                    return json.dumps(identity)
                if 'json_agg(json_build_object' in sql:
                    from datetime import datetime, timedelta, timezone
                    start = datetime(2026, 10, 28, tzinfo=timezone.utc)
                    return json.dumps([{'sequence': 11 + i,
                        'measured_at': (start + timedelta(microseconds=10000 * i)).isoformat()}
                        for i in range(100)])
                return json.dumps({'hash': digest, 'ledger_fingerprint': 'ledger-fp', 'raw': 100,
                    'boot': 'boot-a', 'first_sequence': 11, 'last_sequence': 110, 'sample_count': 100})

        class Proc:
            calls = 0
            instances = []
            def __init__(self, args, **kwargs):
                self.assert_env = kwargs['env']
                if self.assert_env.get('GATEWAY_CONTROL_ENABLED') != 'false' or 'CONTROL_ENABLED' in self.assert_env:
                    raise AssertionError('wrong gateway control environment key')
                type(self).calls += 1; self.phase = 'fault' if type(self).calls == 1 else 'reopen'
                self.stdout = kwargs['stdout']; self.returncode = None
                self.wait_calls = 0
                type(self).instances.append(self)
                self.dbfile = Path(root) / 'durable.sqlite'
                if self.phase == 'fault':
                    db = sqlite3.connect(self.dbfile)
                    db.execute('CREATE TABLE outbox (message_id TEXT,boot_id TEXT,gateway_id TEXT,sensor_id TEXT,first_sequence INTEGER,sample_interval_us INTEGER,sample_count INTEGER,payload BLOB,byte_size INTEGER,status TEXT)')
                    db.execute('INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?,?,?)',
                        (message_id,'boot-a','gw','p8-e4-durable',11,10000,100,payload,len(payload),'pending'))
                    db.commit(); db.close()
                self.pid = 7
            def wait(self, timeout=None):
                self.wait_calls += 1
                if self.phase == 'fault' and os.environ.get('_durable_unexpected') == 'True':
                    self.returncode = 8
                    self.stdout.write(b'SUMMARY fault\n'); return 8
                if timeout and os.environ.get('_durable_timeout') == 'True' and self.returncode is None:
                    raise __import__('subprocess').TimeoutExpired('gateway', timeout)
                if self.phase == 'reopen':
                    db = sqlite3.connect(self.dbfile); db.execute("UPDATE outbox SET status='acked'"); db.commit(); db.close()
                self.returncode = 1 if self.phase == 'fault' else 0
                self.stdout.write(('SUMMARY ' + self.phase + '\n').encode()); self.stdout.flush()
                return self.returncode
            def poll(self): return self.returncode
            def terminate(self): self.returncode = -15
            def kill(self): self.returncode = -9

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            smoke = Smoke()
            fixture = SimpleNamespace(private=root / 'private')
            fixture.private.mkdir()
            spec = SimpleNamespace(gateway_id='gw', sensor_unit='C')
            summaries = {'fault': {'intentional_fault_stop': 'true', 'ignored_valid_acks': '1',
                'completed': 'false', 'app_acknowledged': '0', 'pending_rows': '1'},
                'reopen': {'completed': 'true', 'enqueued': '1', 'app_acknowledged': '2'}}
            module = SimpleNamespace(gateway_env=lambda *a: {}, parse_run_summary=lambda text: summaries['fault' if 'fault' in text else 'reopen'])
            with patch.dict('sys.modules', {'stage3_uplink_runner': module}), \
                 patch.object(e4.subprocess, 'Popen', Proc), \
                 patch.dict('os.environ', {'_durable_unexpected': str(unexpected_fault), '_durable_timeout': str(timeout)}):
                if unexpected_fault:
                    with self.assertRaisesRegex(ValueError, 'intentional_stop_unproven'):
                        e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                            'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')
                    return None
                if timeout:
                    with self.assertRaisesRegex(ValueError, 'fault_timeout'):
                        e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                            'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')
                    self.assertGreaterEqual(Proc.instances[0].wait_calls, 2)
                    self.assertIsNotNone(Proc.instances[0].poll())
                    return None
                return e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                    'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')

    def test_durable_runtime_probe_orders_both_processes_and_reads_back_after_reopen(self):
        result = self.run_probe()
        self.assertEqual(result['status'], 'PASS', result['failures'])
        self.assertEqual(result['process_join_states'], [True, True])
        self.assertEqual(result['aux_dataset_counts']['delta'], {'ledger': 2, 'raw': 200, 'temporal': 2})

    def test_durable_runtime_probe_refuses_unexpected_nonzero_fault_exit(self):
        self.run_probe(unexpected_fault=True)

    def test_durable_runtime_probe_refuses_post_reopen_hash_or_raw_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'post_reopen_backend_readback_mismatch'):
            self.run_probe(mismatch='raw')

    def test_durable_runtime_probe_timeout_joins_child(self):
        self.run_probe(timeout=True)


if __name__ == '__main__':
    unittest.main()
