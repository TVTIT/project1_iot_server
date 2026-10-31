"""Preparation and preflight regression tests; these tests never contact Docker."""
import base64
from contextlib import ExitStack
import dataclasses
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

MODULE = Path(__file__).with_name("native_compose_smoke.py")
spec = importlib.util.spec_from_file_location("native_compose_smoke", MODULE)
smoke = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = smoke
spec.loader.exec_module(smoke)


class PreparationTests(unittest.TestCase):
    def test_e2_helper_import_without_pythonpath_from_outside_repository(self):
        import subprocess
        import textwrap

        code = textwrap.dedent('''
            import importlib.util
            import json
            from pathlib import Path
            import sys
            from unittest.mock import patch

            native_path = Path(sys.argv[1])
            sys.path.insert(0, str(native_path.parent))
            name = 'native_compose_smoke' if sys.argv[2] == 'native' else 'paper_p8_e2_support'
            path = native_path if sys.argv[2] == 'native' else native_path.with_name('paper_p8_e2_support.py')
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            with patch('subprocess.run', side_effect=AssertionError('unexpected external command')), \
                 patch('subprocess.Popen', side_effect=AssertionError('unexpected child process')):
                spec.loader.exec_module(module)
                helper = module.paper_p8_e2_support() if sys.argv[2] == 'native' else module
                context = helper.build_e2_equal_samples_context()
                helper.validate_e2_equal_samples_context(context)
                print(json.dumps({
                    'messages_1': helper.calculate_e2_equal_samples_expected_counts(context, 1)['total_messages'],
                    'messages_100': helper.calculate_e2_equal_samples_expected_counts(context, 100)['total_messages'],
                    'legacy_messages': sum(helper.PILOT_MESSAGES.values()),
                    'client_loaded': callable(helper.paper_p8_client._safe_equal_samples),
                    'bundle_loaded': callable(helper.paper_p8_client.paper_p8_bundle.bind_bundle),
                }))
        ''')
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        with tempfile.TemporaryDirectory(prefix='p8-import-', dir='/var/tmp/opencode') as directory:
            for entrypoint in ('native', 'helper'):
                with self.subTest(entrypoint=entrypoint):
                    result = subprocess.run(
                        [sys.executable, '-I', '-c', code, str(MODULE.resolve()), entrypoint],
                        cwd=directory, env=env, capture_output=True, text=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(json.loads(result.stdout), {
                        'messages_1': 6000, 'messages_100': 60, 'legacy_messages': 24,
                        'client_loaded': True, 'bundle_loaded': True,
                    })

    def test_e4_warm_acceptance_reads_nested_phase_workload_counters(self):
        report = {
            'status': 'PASS', 'lane_marker': 'PAPER_P8_E4_WARM_SUBSET',
            'workload_status': 'WARM_SUBSET_VALIDATED',
            'phase_history': ['unique', 'replay'], 'full_p8_accepted': False,
            'coldrestart': 'NOT RUN', 'durable_gateway_reopen': 'NOT RUN',
            'conflicts': 'NOT RUN', 'physical_ab': 'NOT RUN',
            'journal_sha256': 'c' * 64,
            'journal_continuity': True, 'replay_journal_prefix_validated': True,
            'generator_colocated': True, 'runner_stopped': True, 'cleanup_verified': True,
            'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            'validation': {'valid': True, 'failures': []},
            'cache_window': {'warm_lookup_hits': 10, 'repository_calls_avoided': 10},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'new_ws_event_proof': {'status': 'PASS'},
            'workloads': {
                'unique': {'validation_valid': True, 'telemetry': {
                    'experiment': 'E4', 'telemetry': {'generated_unique': 20,
                        'application_acks': 20, 'ignored_valid_acks': 1, 'pending_messages': 0}}},
                'replay': {'validation_valid': True, 'telemetry': {
                    'experiment': 'E4', 'telemetry': {'replay_offered': 10,
                        'replay_application_acks': 10}}}}}
        self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, True))
        report['workloads']['replay']['telemetry']['telemetry']['replay_application_acks'] = 9
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(report, True))
        report['workloads']['replay']['telemetry']['telemetry']['replay_application_acks'] = 10
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(report, False))

    def test_e4_cache_off_acceptance_requires_real_database_duplicate_window(self):
        report = {
            'status': 'PASS', 'lane_marker': 'PAPER_P8_E4_CACHE_OFF_SUBSET',
            'cache_mode': 'off', 'workload_status': 'CACHE_OFF_SUBSET_VALIDATED',
            'phase_history': ['unique', 'replay'], 'full_p8_accepted': False,
            'coldrestart': 'NOT APPLICABLE', 'durable_gateway_reopen': 'NOT RUN',
            'conflicts': 'NOT RUN', 'physical_ab': 'NOT RUN', 'generator_colocated': True,
            'runner_stopped': True, 'cleanup_verified': True,
            'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            'validation': {'valid': True, 'failures': []},
            'cache_window': {'attempts': 10, 'transaction_commits': 10,
                'database_duplicates': 10, 'confirmed_new_rows': 0, 'copy_calls': 0,
                'database_fingerprint_unchanged': True},
            'counter_windows': {'replay': {'database_proof': {'exact_equal': True,
                'unique_ack': [{'fingerprint': 'a' * 32}, {'fingerprint': 'b' * 32}],
                'replay_end': [{'fingerprint': 'a' * 32}, {'fingerprint': 'b' * 32}]}}},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'new_ws_event_proof': {'status': 'PASS', 'replay': {
                'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
                'telemetry_writes_completed': 0, 'drops': 0}},
            'journal_sha256': 'a' * 64,
            'journal_continuity': True, 'replay_journal_prefix_validated': True,
            'workloads': {
                'unique': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'generated_unique': 20, 'application_acks': 20, 'ignored_valid_acks': 1,
                    'pending_messages': 0}}},
                'replay': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'replay_offered': 10, 'replay_application_acks': 10}}}}}
        self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, True))
        for key, value in (('database_duplicates', 0), ('transaction_commits', None),
                           ('copy_calls', 1), ('confirmed_new_rows', 1)):
            bad = json.loads(json.dumps(report))
            bad['cache_window'][key] = value
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), key)
        bad = json.loads(json.dumps(report)); bad['cache_window'].pop('attempts')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))
        bad = json.loads(json.dumps(report))
        bad['counter_windows']['replay']['database_proof']['replay_end'][0]['fingerprint'] = 'd' * 32
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

    def _make_durable_report_fixture(self):
        msg_id = '38267d8c-e556-4722-99d8-8951fc8ef607'
        payload_sha = '42181cd469ab3c5226a27ce4c17cfd929b3c558e965a17fd0e46ae5658202a46'
        binding_sha = '459c393fc4ec651b6e4d3b106f16420f3a1bd09b7b89a18c418550293ea59a6d'
        pending = {
            'boot_id': '07511757-0f33-4db7-ac7a-1035be018577', 'gateway_id': 'gw_p0_a_49007a3f',
            'sensor_id': 'p8-e4-durable', 'sequence': 0, 'sample_count': 100, 'interval_us': 10000,
            'message_id': msg_id, 'payload_bytes': 925, 'payload_sha256': payload_sha,
            'header': {
                'boot_id': '07511757-0f33-4db7-ac7a-1035be018577', 'first_sequence': 0,
                'gateway_id': 'gw_p0_a_49007a3f', 'sensor_id': 'p8-e4-durable',
                'measured_at': '2026-10-30T10:53:45.315242Z', 'sample_count': 100,
                'sample_interval_us': 10000
            }
        }
        fault_record = {
            'status': 'PASS', 'phase': 'durable-fault', 'pending': json.loads(json.dumps(pending)),
            'sql': {
                'status': 'PASS', 'message_id': msg_id, 'payload_sha256': payload_sha,
                'ledger_fingerprint': '33230e6bbe497b567bb24f745720edb7', 'raw_count': 100
            },
            'database_before': {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': '8' * 32},
            'database_after': {'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0, 'fingerprint': 'c' * 32},
            'database_delta': {'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0},
            'main_dataset_unchanged': True, 'binding_sha256': binding_sha
        }
        reopen_record = {
            'status': 'PASS', 'phase': 'durable-reopen', 'pending_before': json.loads(json.dumps(pending)),
            'sql': {
                'status': 'PASS', 'raw_count': 100, 'main_dataset_unchanged': True,
                'first_sequence': 0, 'last_sequence': 99,
                'first_timestamp': '2026-10-30T10:53:45.315242+00:00',
                'last_timestamp': '2026-10-30T10:53:46.305242+00:00',
                'interval_us': 10000,
                'auxiliary_delta': {'ledger': 2, 'raw': 200, 'temporal': 2}
            },
            'database_before': {'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0, 'fingerprint': 'c' * 32},
            'database_after': {'ledger': 2, 'raw': 200, 'temporal': 2, 'configuration': 0, 'fingerprint': '9' * 32},
            'database_delta': {'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0},
            'application_acks': 2, 'pending_rows': 0, 'quarantined_rows': 0, 'joined': True,
            'main_dataset_unchanged': True, 'binding_sha256': binding_sha
        }
        return {
            'schema_version': 1, 'status': 'PASS', 'experiment': 'E4', 'arm': 'warm', 'cache_mode': 'on',
            'workload_status': 'DURABLE_REOPEN_SUBSET_VALIDATED',
            'lane_marker': 'PAPER_P8_E4_DURABLE_REOPEN_SUBSET',
            'phase_history': ['unique', 'replay', 'durable-fault', 'durable-reopen'],
            'full_p8_accepted': False, 'coldrestart': 'NOT RUN',
            'durable_gateway_reopen': {
                'status': 'PASS', 'fault': fault_record, 'reopen': reopen_record
            },
            'conflicts': 'NOT RUN', 'physical_ab': 'NOT RUN', 'generator_colocated': True,
            'runner_stopped': True, 'cleanup_verified': True,
            'journal_sha256': 'a' * 64, 'journal_continuity': True,
            'replay_journal_prefix_validated': True,
            'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            'counter_windows': {'replay': {'database_proof': {'exact_equal': True}}},
            'validation': {'valid': True, 'failures': []},
            'cache_window': {'warm_lookup_hits': 10, 'repository_calls_avoided': 10},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'new_ws_event_proof': {'status': 'PASS'},
            'workloads': {
                'unique': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'generated_unique': 20, 'application_acks': 20, 'ignored_valid_acks': 1,
                    'pending_messages': 0}}},
                'replay': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'replay_offered': 10, 'replay_application_acks': 10}}}}}

    def test_e4_durable_reopen_acceptance_and_rejections(self):
        report = self._make_durable_report_fixture()
        self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, True))

        # 1. Bad cleanup
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(report, False))

        # 2. Wrong stage delta vs total:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['database_delta']['raw'] = 99
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['database_delta']['raw'] = 101
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['sql']['auxiliary_delta']['raw'] = 300
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 3. Hash changed / mismatch:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['binding_sha256'] = 'f' * 64
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['sql']['payload_sha256'] = 'e' * 64
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['pending_before']['message_id'] = 'different-id'
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 4. Pending > 0 or quarantined > 0:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['pending_rows'] = 1
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['quarantined_rows'] = 1
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 5. Missing SQL:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen'].pop('sql')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault'].pop('sql')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 6. Wrong phase history:
        bad = json.loads(json.dumps(report))
        bad['phase_history'] = ['unique', 'replay', 'durable-fault']
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 7. Missing base proof:
        bad = json.loads(json.dumps(report))
        bad['cache_window']['warm_lookup_hits'] = 9
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['raw_verification'][0]['verified_exact'] = False
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['database_delta']['raw'] = 1900
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['workloads']['unique']['telemetry']['telemetry']['application_acks'] = 19
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 8. Missing / invalid ledger_fingerprint:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['sql'].pop('ledger_fingerprint')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['sql']['ledger_fingerprint'] = ''
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['sql']['ledger_fingerprint'] = 'not-a-valid-hex-fingerprint!'
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['sql']['ledger_fingerprint'] = 'a' * 31
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 9. Missing / invalid / mismatched header:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['pending'].pop('header')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['pending_before'].pop('header')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['pending_before']['header']['gateway_id'] = 'gw-different'
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        for key, bad_val in (
            ('gateway_id', 'different_gw'),
            ('sensor_id', 'different_sensor'),
            ('boot_id', 'different_boot'),
            ('first_sequence', 999),
            ('sample_count', 50),
            ('sample_interval_us', 5000),
            ('measured_at', 'not-a-timestamp'),
        ):
            bad = json.loads(json.dumps(report))
            bad['durable_gateway_reopen']['fault']['pending']['header'][key] = bad_val
            bad['durable_gateway_reopen']['reopen']['pending_before']['header'][key] = bad_val
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), key)

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['pending']['header'].pop('measured_at')
        bad['durable_gateway_reopen']['reopen']['pending_before']['header'].pop('measured_at')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 10. Missing / invalid / mismatched database snapshots:
        for phase, snap_key in (('fault', 'database_before'), ('fault', 'database_after'),
                                ('reopen', 'database_before'), ('reopen', 'database_after')):
            bad = json.loads(json.dumps(report))
            bad['durable_gateway_reopen'][phase].pop(snap_key)
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), f'missing {phase}.{snap_key}')

            bad = json.loads(json.dumps(report))
            bad['durable_gateway_reopen'][phase][snap_key]['raw'] = 'not-an-int'
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), f'non-int count {phase}.{snap_key}')

            bad = json.loads(json.dumps(report))
            bad['durable_gateway_reopen'][phase][snap_key]['fingerprint'] = 'bad-fp'
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), f'bad fp {phase}.{snap_key}')

            bad = json.loads(json.dumps(report))
            bad['durable_gateway_reopen'][phase][snap_key].pop('fingerprint')
            self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True), f'missing fp {phase}.{snap_key}')

        # 11. Continuity mismatch (fault.database_after != reopen.database_before):
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['database_before']['raw'] = 105
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['database_before']['fingerprint'] = 'e' * 32
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 12. Snapshot stage deltas must be 1/100/1/0:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['fault']['database_before']['raw'] = 10
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['database_after']['raw'] = 250
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 13. Cumulative reopen.after minus fault.before must be 2/200/2/0:
        bad = json.loads(json.dumps(report))
        bad['durable_gateway_reopen']['reopen']['database_after']['temporal'] = 3
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

    def test_e4_startup_summary_branch(self):
        fixture = type('Fixture', (), {
            'paper_p8_e4': 'on', 'paper_p8_e4_cache_mode': 'on', 'project': 'test-project'})()
        report = self._make_durable_report_fixture()

        # Valid durable subset:
        summary = smoke.paper_p8_e4_startup_summary(
            fixture, report, accepted=True, cleanup_verified=True, cleanup_counts={},
            artifact_name='p8-e4-on.json')
        self.assertEqual(summary['status'], 'PAPER_P8_E4_DURABLE_REOPEN_SUBSET_VALIDATED')
        self.assertEqual(summary['lane'], 'PAPER_P8_E4_DURABLE_REOPEN_SUBSET')
        self.assertEqual(summary['workload_outcome'], 'DURABLE_REOPEN_SUBSET_VALIDATED')
        self.assertEqual(summary['durable_gateway_reopen'], 'PASS')
        self.assertFalse(summary['full_p8_accepted'])

        # Failed durable subset remains failed even with cleanup_verified=True:
        summary_fail = smoke.paper_p8_e4_startup_summary(
            fixture, report, accepted=False, cleanup_verified=True, cleanup_counts={},
            artifact_name='p8-e4-on.json')
        self.assertEqual(summary_fail['status'], 'FAIL')
        self.assertEqual(summary_fail['workload_outcome'], 'DURABLE_REOPEN_SUBSET_VALIDATED')

        # Warm subset behavior unchanged:
        warm_rep = {'workload_status': 'WARM_SUBSET_VALIDATED', 'coldrestart': 'NOT RUN',
                    'durable_gateway_reopen': 'NOT RUN'}
        warm_summary = smoke.paper_p8_e4_startup_summary(
            fixture, warm_rep, accepted=True, cleanup_verified=True, cleanup_counts={},
            artifact_name='p8-e4-on.json')
        self.assertEqual(warm_summary['status'], 'PAPER_P8_E4_WARM_SUBSET_VALIDATED')
        self.assertEqual(warm_summary['lane'], 'PAPER_P8_E4_WARM_SUBSET')
        self.assertEqual(warm_summary['durable_gateway_reopen'], 'NOT RUN')

        # Cache-off subset behavior unchanged:
        off_fix = type('Fixture', (), {
            'paper_p8_e4': 'off', 'paper_p8_e4_cache_mode': 'off', 'project': 'test-project'})()
        off_rep = {'workload_status': 'CACHE_OFF_SUBSET_VALIDATED', 'coldrestart': 'NOT APPLICABLE',
                   'durable_gateway_reopen': 'NOT RUN'}
        off_summary = smoke.paper_p8_e4_startup_summary(
            off_fix, off_rep, accepted=True, cleanup_verified=True, cleanup_counts={},
            artifact_name='p8-e4-off.json')
        self.assertEqual(off_summary['status'], 'PAPER_P8_E4_CACHE_OFF_SUBSET_VALIDATED')
        self.assertEqual(off_summary['lane'], 'PAPER_P8_E4_CACHE_OFF_SUBSET')
        self.assertEqual(off_summary['durable_gateway_reopen'], 'NOT RUN')

    def _make_conflicts_report_fixture(self):
        cases = {
            'same_id_changed_payload': {
                'status': 'PASS',
                'schema_valid': True,
                'positive_control_ack_count': 1,
                'conflict_ack_count': 0,
                'database_unchanged': True,
                'expected_rejection_reason': 'message_conflict',
                'observed_rejection_reason': 'message_conflict',
                'guard_precondition': {'matched': True}
            },
            'different_first_sequence_overlap': {
                'status': 'PASS',
                'schema_valid': True,
                'positive_control_ack_count': 1,
                'conflict_ack_count': 0,
                'database_unchanged': True,
                'expected_rejection_reason': 'sequence_overlap',
                'observed_rejection_reason': 'sequence_overlap',
                'guard_precondition': {'matched': True}
            }
        }
        return {
            'schema_version': 1, 'status': 'PASS', 'experiment': 'E4', 'arm': 'warm', 'cache_mode': 'on',
            'workload_status': 'CONFLICTS_SUBSET_VALIDATED',
            'lane_marker': 'PAPER_P8_E4_CONFLICTS_SUBSET',
            'phase_history': ['unique', 'replay', 'conflicts'],
            'full_p8_accepted': False, 'coldrestart': 'NOT RUN',
            'durable_gateway_reopen': 'NOT RUN',
            'conflicts': {'status': 'PASS', 'cases': cases},
            'physical_ab': 'NOT RUN', 'generator_colocated': True,
            'runner_stopped': True, 'cleanup_verified': True,
            'journal_sha256': 'a' * 64, 'journal_continuity': True,
            'replay_journal_prefix_validated': True,
            'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            'counter_windows': {'replay': {'database_proof': {'exact_equal': True}}},
            'validation': {'valid': True, 'failures': []},
            'cache_window': {'warm_lookup_hits': 10, 'repository_calls_avoided': 10},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'new_ws_event_proof': {'status': 'PASS'},
            'workloads': {
                'unique': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'generated_unique': 20, 'application_acks': 20, 'ignored_valid_acks': 1,
                    'pending_messages': 0}}},
                'replay': {'validation_valid': True, 'telemetry': {'telemetry': {
                    'replay_offered': 10, 'replay_application_acks': 10}}}}}

    def test_e4_conflicts_acceptance_and_rejections(self):
        report = self._make_conflicts_report_fixture()
        self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, True))

        # 1. Cleanup not verified
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(report, False))

        # 2. Conflict ACK observed
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['same_id_changed_payload']['conflict_ack_count'] = 1
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 3. Missing positive control ACK
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['same_id_changed_payload']['positive_control_ack_count'] = 0
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 4. Schema invalid
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['different_first_sequence_overlap']['schema_valid'] = False
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 5. Observed rejection reason mismatch
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['same_id_changed_payload']['observed_rejection_reason'] = 'wrong_reason'
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 6. Database changed
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['different_first_sequence_overlap']['database_unchanged'] = False
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 7. Missing required case
        bad = json.loads(json.dumps(report))
        del bad['conflicts']['cases']['different_first_sequence_overlap']
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 8. Guard precondition not matched
        bad = json.loads(json.dumps(report))
        bad['conflicts']['cases']['same_id_changed_payload']['guard_precondition']['matched'] = False
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad, True))

        # 9. Startup summary
        fixture = type('Fixture', (), {'paper_p8_e4': 'on', 'paper_p8_e4_cache_mode': 'on', 'project': 'test-project'})()
        summary = smoke.paper_p8_e4_startup_summary(fixture, report, accepted=True, cleanup_verified=True, cleanup_counts={}, artifact_name='p8-e4-on.json')
        self.assertEqual(summary['status'], 'PAPER_P8_E4_CONFLICTS_SUBSET_VALIDATED')
        self.assertEqual(summary['lane'], 'PAPER_P8_E4_CONFLICTS_SUBSET')
        self.assertEqual(summary['conflicts'], 'PASS')
        self.assertFalse(summary['full_p8_accepted'])

    def test_e3_hold_uses_requested_arm_for_runtime_flags(self):
        for arm in ('polling', 'wakeup'):
            self.assertEqual(smoke.paper_p8_e3_selector(type('Args', (), {
                'p8_e3_prepare_hold': True, 'p8_arm': arm, 'paper_p8_e3': None})()), arm)
        self.assertEqual(smoke.paper_p8_e3_selector(type('Args', (), {
            'p8_e3_prepare_hold': False, 'p8_arm': None, 'paper_p8_e3': 'wakeup'})()), 'wakeup')
    def test_e3_lifecycle_exit_accepts_only_unstarted_preparation_or_validated_workload(self):
        self.assertTrue(smoke.paper_p8_e3_lifecycle_outcome_accepted({
            'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN'}))
        self.assertTrue(smoke.paper_p8_e3_lifecycle_outcome_accepted({
            'phase': 'CLOSED', 'workload_outcome': 'VALIDATED',
            'imported_result': {'validation_valid': True}}))
        for state in (
                {'phase': 'CLOSED', 'workload_outcome': 'ABORTED', 'collector_ready': True},
                {'phase': 'CLOSED', 'workload_outcome': 'VALIDATION_FAILED',
                 'imported_result': {'validation_valid': False}},
                {'phase': 'CLOSED', 'workload_outcome': 'NOT_RUN', 'collector_ready': True},
                {'phase': 'CLOSED', 'workload_outcome': 'VALIDATED',
                 'imported_result': {'validation_valid': False}}):
            self.assertFalse(smoke.paper_p8_e3_lifecycle_outcome_accepted(state))

    def test_e2_prepare_only_report_uses_e2_artifacts_and_does_not_require_workload_when_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = type('Fixture', (), {'paper_p8_e2':False, 'paper_p8_e1':False,
                'project':'e2-test', 'private':root/'private', 'artifacts':root/'artifacts'})()
            fixture.private.mkdir()
            fixture.artifacts.mkdir()
            (fixture.private/'session.json').write_text(json.dumps({'experiment':'E2','phase_id':'samples-100'}))
            (fixture.artifacts/'p8-e2-pilot.json').write_text(json.dumps({'status':'PREPARATION_ONLY'}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='NOT_RUN',
                cleanup_verified=True, cleanup_counts={'container':12,'network':1,'volume':5})
            self.assertEqual(report['status'], 'PAPER_P8_E2_PREPARATION_ONLY')
            self.assertEqual(report['artifact'], 'p8-e2-pilot.json')
            self.assertFalse(report['p8_e2_workload_verified'])
            self.assertFalse((fixture.artifacts/'p8-e1-pilot.json').exists())

    def test_e2_prepare_only_report_selects_validated_sample_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = type('Fixture', (), {'paper_p8_e2':False, 'project':'e2-test',
                'private':root/'private', 'artifacts':root/'artifacts'})()
            fixture.private.mkdir(); fixture.artifacts.mkdir()
            (fixture.private/'session.json').write_text(json.dumps({'experiment':'E2','phase_id':'samples-1'}))
            (fixture.artifacts/'p8-e2-samples-1-remote-result.json').write_text(json.dumps(
                {'status':'PASS','validation':{'valid':True}}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='VALIDATED',
                cleanup_verified=True, cleanup_counts={})
            self.assertEqual(report['status'], 'PAPER_P8_E2_WORKLOAD_VERIFIED')
            self.assertEqual(report['artifact'], 'p8-e2-samples-1-remote-result.json')
            self.assertTrue(report['p8_e2_workload_verified'])

    def test_e1_prepare_only_report_keeps_e1_artifact_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = type('Fixture', (), {'paper_p8_e1':True, 'paper_p8_e2':False,
                'project':'e1-test', 'private':root/'private', 'artifacts':root/'artifacts'})()
            fixture.private.mkdir(); fixture.artifacts.mkdir()
            (fixture.artifacts/'p8-e1-pilot.json').write_text(json.dumps({'status':'PREPARATION_ONLY'}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='NOT_RUN',
                cleanup_verified=True, cleanup_counts={})
            self.assertEqual(report['status'], 'PAPER_P8_E1_PREPARATION_ONLY')
            self.assertEqual(report['artifact'], 'p8-e1-remote-result.json')

    def test_prepare_only_run_status_uses_session_experiment_and_separates_failure_states(self):
        self.assertEqual(smoke.paper_p8_prepare_only_run_status('E2', failed=False,
            cleanup_verified=True, remote_outcome='VALIDATED'), 'E2_WORKLOAD_VERIFICATION_PASS')
        self.assertEqual(smoke.paper_p8_prepare_only_run_status('E2', failed=False,
            cleanup_verified=True, remote_outcome='NOT_RUN'), 'E2_PREPARATION_ONLY_CLEANUP')
        self.assertEqual(smoke.paper_p8_prepare_only_run_status('E2', failed=False,
            cleanup_verified=False, remote_outcome='VALIDATED'), 'FAIL')
        self.assertEqual(smoke.paper_p8_prepare_only_run_status('E2', failed=False,
            cleanup_verified=True, remote_outcome='VALIDATION_FAILED'), 'E2_WORKLOAD_VALIDATION_FAILED')

    def test_e3_prepare_report_selects_low_load_artifact_and_keeps_workload_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = type('Fixture', (), {'paper_p8_e3': 'polling',
                'paper_p8_e3_workload': 'low_load', 'project': 'e3-test',
                'private': root/'private', 'artifacts': root/'artifacts'})()
            fixture.private.mkdir(); fixture.artifacts.mkdir()
            (fixture.private/'session.json').write_text(json.dumps({'experiment':'E3',
                'phase_id':'e3-low_load-polling', 'workload_outcome':'NOT_RUN'}))
            (fixture.artifacts/'p8-e3-polling-low-load.json').write_text(json.dumps({
                'status':'PREPARATION_ONLY', 'workload_status':'NOT_RUN',
                'experiment':'E3', 'phase_id':'e3-low_load-polling', 'full_p8_accepted':False}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='NOT_RUN',
                cleanup_verified=True, cleanup_counts={'container':12,'network':1,'volume':5})
            self.assertEqual(report['status'], 'PAPER_P8_E3_PREPARATION_ONLY')
            self.assertEqual(report['artifact'], 'p8-e3-polling-low-load.json')
            self.assertFalse(report['p8_e3_workload_verified'])
            self.assertEqual(smoke.paper_p8_prepare_only_run_status('E3', failed=False,
                cleanup_verified=True, remote_outcome='NOT_RUN'), 'E3_PREPARATION_ONLY_CLEANUP')

    def test_e3_prepare_report_uses_imported_result_for_validated_workload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = type('Fixture', (), {'paper_p8_e3': 'polling',
                'paper_p8_e3_workload': 'low_load', 'project': 'e3-test',
                'private': root/'private', 'artifacts': root/'artifacts'})()
            fixture.private.mkdir(); fixture.artifacts.mkdir()
            (fixture.private/'session.json').write_text(json.dumps({'experiment':'E3',
                'phase_id':'e3-low_load-polling', 'workload_outcome':'VALIDATED'}))
            (fixture.artifacts/'p8-e3-polling-low-load.json').write_text(json.dumps(
                {'status':'PREPARATION_ONLY'}))
            result_path = fixture.artifacts/'p8-e3-polling-low-load-remote-result.json'
            result_path.write_text(json.dumps({'status':'PASS','validation':{'valid':True}}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='VALIDATED',
                cleanup_verified=True, cleanup_counts={'container':12,'network':1,'volume':5})
            self.assertEqual(report['status'], 'PAPER_P8_E3_WORKLOAD_VERIFIED')
            self.assertEqual(report['artifact'], result_path.name)
            self.assertTrue(report['p8_e3_workload_verified'])

    def test_private_csv_writer_creates_mode_0600_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'metrics.csv'
            smoke.paper_p8_write_private_csv(path, ('phase', 'value'),
                                             [('workload_start', '{"count":1}')])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_text(), 'phase,value\nworkload_start,"{""count"":1}"\n')
            with self.assertRaises(FileExistsError):
                smoke.paper_p8_write_private_csv(path, ('phase',), [])

    def test_p8_lifecycle_rejects_unrelated_low_load_selector_before_prepare(self):
        with patch.object(sys, 'argv', ['native_compose_smoke.py', '--prepare', '/tmp/not-created',
                                      '--p8-lifecycle-hold', '--paper-p8-e3-low-load']):
            with self.assertRaises(SystemExit) as raised:
                smoke.main()
        self.assertEqual(raised.exception.code, 2)

    def test_p8_e1_selector_builds_opt_in_websocket_profile_and_reuses_load_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'fixture', paper_p8_e1=True)
            self.assertTrue(fixture.paper_p8_load)
            self.assertTrue(fixture.paper_p8_e1)
            self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'true')
            self.assertEqual(fixture.values['WS_MAX_SESSIONS'], '64')
            for key, expected in {'WS_HEARTBEAT_INTERVAL': '2s', 'WS_PONG_TIMEOUT': '4s',
                                  'WS_WRITE_TIMEOUT': '1s', 'WS_RECHECK_INTERVAL': '5s',
                                  'WS_RECHECK_SWEEP_BUDGET': '2s', 'WS_RECHECK_CONCURRENCY': '2',
                                  'WS_FANOUT_MODE': 'indexed'}.items():
                self.assertEqual(fixture.values[key], expected)
            self.assertIn('target: 8883', fixture.override.read_text())
            self.assertEqual(fixture.private.joinpath('bin').exists(), False)
            self.assertTrue(fixture.paper_p2)
            self.assertFalse(any(fixture.values[k] == 'true' for k in
                ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED',
                 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED')))

    def test_p8_e1_rejects_mixed_probe_and_e2_selector(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'paper_p8_mode_rejected'):
                smoke.prepare(Path(directory) / 'mixed', paper_p8_e1=True, paper_p8_sessions=True)

    def test_p8_e2_selector_uses_isolated_shaper_microbatch_profile_with_ws_off(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'e2', paper_p8_e2=True)
            self.assertTrue(fixture.paper_p8_load)
            self.assertTrue(fixture.paper_p8_e2)
            self.assertFalse(fixture.paper_p8_e1)
            self.assertTrue(fixture.paper_p2)
            self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'false')
            for key, value in {'MQTT_SHAPER_ENABLED': 'true', 'MQTT_SHAPER_RATE_PER_SECOND': '2',
                               'MQTT_SHAPER_BURST': '4', 'MQTT_MICROBATCH_ENABLED': 'true',
                               'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
                               'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
                               'MQTT_MICROBATCH_MAX_WAIT': '50ms', 'MQTT_RAM_DEDUP_ENABLED': 'false',
                               'STAGE5_ENABLED': 'false', 'STAGE5_WAKEUP_ENABLED': 'false'}.items():
                self.assertEqual(fixture.values[key], value)
            with self.assertRaisesRegex(ValueError, 'paper_p8_mode_rejected'):
                smoke.prepare(Path(directory) / 'mixed', paper_p8_e2=True, paper_p8_load=True)
            with self.assertRaisesRegex(ValueError, 'paper_p8_mode_rejected'):
                smoke.prepare(Path(directory) / 'sessions', paper_p8_e2=True, paper_p8_sessions=True)

    def test_p8_e2_cli_rejects_other_p8_lanes(self):
        for extra in ('--paper-p8-e1', '--paper-p8-sessions', '--paper-p8-slow-viewer', '--paper-p8-load'):
            with self.subTest(extra=extra), patch.object(sys, 'argv',
                    ['native_compose_smoke.py', '--prepare', '/tmp/not-created', '--paper-p8-e2', extra]):
                with self.assertRaises(SystemExit) as raised:
                    smoke.main()
                self.assertEqual(raised.exception.code, 2)

    def test_p8_e1_acceptance_requires_complete_workload_and_exact_owned_cleanup(self):
        fixture = type('Fixture', (), {'project': 'run'})()
        item = {'kind': 'container', 'run': 'run', 'task': 'native-compose-allocation',
                'allocation_attempted': True, 'removed': True,
                'receipt': {'id': 'a' * 64, 'created': 'created', 'labels': {}}}
        registry = type('Registry', (), {'items': [item]})()
        counts = {'container': 1, 'network': 0, 'volume': 0}
        complete = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'sockets': 64,
                    'workload': {'telemetry': {'generated_unique': 20}},
                    'validation': {'valid': True, 'failures': []}, 'full_p8_accepted': False}
        self.assertTrue(smoke.paper_p8_e1_cleanup_acceptance(fixture, complete, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e1_cleanup_acceptance(fixture, {}, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e1_cleanup_acceptance(fixture,
            {**complete, 'validation': {'valid': False}}, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e1_cleanup_acceptance(fixture, complete, counts, False, registry))

    def test_p8_load_dispatch_selects_e1_helper_without_changing_e2_route(self):
        sentinel = object()
        helper = Mock(run=Mock(return_value=sentinel))
        e2 = Mock(run_e2_telemetry100=Mock(return_value=sentinel))
        with patch.object(smoke, 'paper_p8_e1_support', return_value=helper), \
             patch.object(smoke, 'paper_p8_support', return_value=e2):
            e1_fixture = type('Fixture', (), {'paper_p8_e1': True})()
            e2_fixture = type('Fixture', (), {'paper_p8_e1': False})()
            args = (smoke, e1_fixture, 'endpoint', 'registry', {}, {}, 'state', 'binary')
            result = smoke.dispatch_paper_p8_load(e1_fixture, smoke, 'endpoint', 'registry', {}, {}, 'state', 'binary')
            self.assertIs(result, sentinel)
            helper.run.assert_called_once_with(*args)
            e2.run_e2_telemetry100.assert_not_called()
            smoke.dispatch_paper_p8_load(e2_fixture, smoke, 'endpoint', 'registry', {}, {}, 'state', 'binary')
            e2.run_e2_telemetry100.assert_called_once_with(smoke, e2_fixture, 'endpoint', 'registry', {}, {}, 'state', 'binary')

    def test_p8_e2_dispatch_uses_e2_helper(self):
        sentinel = object()
        helper = Mock(run=Mock(return_value=sentinel))
        with patch.object(smoke, 'paper_p8_e2_support', return_value=helper):
            fixture = type('Fixture', (), {'paper_p8_e2': True, 'paper_p8_e1': False})()
            args = (smoke, fixture, 'endpoint', 'registry', {}, {}, 'state', 'binary')
            self.assertIs(smoke.dispatch_paper_p8_load(fixture, args[0], *args[2:]), sentinel)
            helper.run.assert_called_once_with(*args)

    def test_p8_e2_acceptance_requires_both_valid_profiles_and_exact_cleanup(self):
        fixture = type('Fixture', (), {'project': 'run'})()
        item = {'kind': 'container', 'run': 'run', 'task': 'native-compose-allocation',
                'allocation_attempted': True, 'removed': True,
                'receipt': {'id': 'a' * 64, 'created': 'created', 'labels': {}}}
        registry = type('Registry', (), {'items': [item]})()
        counts = {'container': 1, 'network': 0, 'volume': 0}
        def profile(samples):
            return {'samples_per_message': samples, 'status': 'PASS',
                    'validation': {'valid': True, 'failures': []},
                    'workloads': {arm: {'telemetry': {'generated_unique': n,
                        'application_acks': n, 'pending_messages': 0}} for arm, n in (('A', 8), ('B', 16))}}
        evidence = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'full_p8_accepted': False,
                    'profiles': [profile(100), profile(1)]}
        self.assertTrue(smoke.paper_p8_e2_cleanup_acceptance(fixture, evidence, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e2_cleanup_acceptance(fixture, {**evidence, 'profiles': [profile(100)]}, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e2_cleanup_acceptance(fixture, {**evidence, 'profiles': [profile(100), {**profile(1), 'status': 'FAIL'}]}, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e2_cleanup_acceptance(fixture, {**evidence, 'profiles': [profile(100), {**profile(1), 'validation': {}}]}, counts, True, registry))
        self.assertFalse(smoke.paper_p8_e2_cleanup_acceptance(fixture, evidence, counts, False, registry))
        self.assertTrue(smoke.paper_p8_e2_report_acceptance({**evidence, 'status': 'PASS',
            'workload_status': evidence['status'], 'cleanup_verified': True}, True))
        self.assertFalse(smoke.paper_p8_e2_report_acceptance({**evidence, 'status': 'PASS',
            'workload_status': evidence['status'], 'cleanup_verified': False}, True))

    def test_p8_load_selector_isolated_and_adds_only_loopback_mqtt_tls_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'fixture', paper_p8_load=True)
            self.assertTrue(fixture.paper_p2)
            self.assertTrue(fixture.paper_p8_load)
            for key in ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED',
                        'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
                self.assertEqual(fixture.values[key], 'false')
            override = fixture.override.read_text()
            self.assertIn('published: "0"', override)
            self.assertIn('host_ip: "127.0.0.1"', override)
            self.assertIn('target: 8883', override)
            with self.assertRaisesRegex(ValueError, 'paper_p8_mode_rejected'):
                smoke.prepare(Path(directory) / 'second', paper_p8_load=True, paper_p8_sessions=True)

    def test_gateway_cli_captures_owned_diagnostics_before_callback_error(self):
        writes = []
        class Registry:
            def __init__(self):
                self.item = {'name': 'run-gateway-p6-a-steady', 'receipt': {'id': 'owned-id'}}
                self.obj = {'Id': 'owned-id', 'State': {'Running': True, 'Status': 'running'}}
                self.created = False
                self.commands = []
            def intent(self, *args): return self.item
            def resolve(self, item): return self.obj if self.created else None
            def save(self, item): pass
            def allocating(self, item): pass
            def command(self, args):
                self.commands.append(args)
                if args[0] == 'create':
                    self.created = True
                    return Mock(returncode=0, stdout=b'owned-id')
                if args[0] == 'start': return Mock(returncode=0)
                if args[0] == 'logs':
                    return Mock(returncode=0, stdout=b'bounded log', stderr=b'')
                raise AssertionError(args)
        fixture = type('Fixture', (), {'project': 'run', 'private': Path('/private')})()
        registry = Registry()
        error = RuntimeError('original callback error')
        with patch.object(smoke, 'private_write', side_effect=lambda path, data: writes.append((path, data))):
            with self.assertRaises(RuntimeError) as raised:
                smoke.paper_p0_gateway_cli(fixture, registry, 'image', {'name': 'vol'},
                    Path('/secret/env'), Path('/private/ca'), 'p6-a-steady', ['run'],
                    type('State', (), {'checkpoint': lambda self: None})(),
                    during=lambda: (_ for _ in ()).throw(error))
        self.assertIs(raised.exception, error)
        self.assertTrue(any('diagnostic.log' in str(path) and data == 'bounded log'
                            for path, data in writes))
        self.assertTrue(any('diagnostic.json' in str(path) and 'callback_exception' in data
                            for path, data in writes))
        self.assertIn(['logs', '--tail', '1000', 'owned-id'], registry.commands)

    def test_gateway_cli_diagnostic_failure_does_not_mask_timeout(self):
        class Registry:
            def __init__(self):
                self.item = {'name': 'run-gateway-p6-a-steady', 'receipt': {'id': 'owned-id'}}
                self.obj = {'Id': 'owned-id', 'State': {'Running': True, 'Status': 'running'}}
                self.created = False
            def intent(self, *args): return self.item
            def resolve(self, item): return self.obj if self.created else None
            def save(self, item): pass
            def allocating(self, item): pass
            def command(self, args):
                if args[0] == 'create':
                    self.created = True
                    return Mock(returncode=0, stdout=b'owned-id')
                if args[0] == 'start': return Mock(returncode=0)
                raise AssertionError(args)
        fixture = type('Fixture', (), {'project': 'run', 'private': Path('/private')})()
        with patch.object(smoke, 'private_write', side_effect=OSError('disk failure')), \
             patch.object(smoke.time, 'monotonic', side_effect=[0.0, 2.0, 2.0, 2.0]):
            with self.assertRaisesRegex(ValueError, 'paper_p0_gateway_failed'):
                smoke.paper_p0_gateway_cli(fixture, Registry(), 'image', {'name': 'vol'},
                    Path('/secret/env'), Path('/private/ca'), 'p6-a-steady', ['run'],
                    type('State', (), {'checkpoint': lambda self: None})(), timeout=1)

    def test_startup_migration_failure_blocks_next_phase(self):
        with self.assertRaisesRegex(ValueError, 'migration_nonzero'):
            smoke.startup_state('application-migrations', {'Running': False, 'ExitCode': 1})

    def test_startup_health_never_accepts_running_alone(self):
        self.assertFalse(smoke.startup_state('postgres', {'Running': True}))
        self.assertTrue(smoke.startup_state('postgres', {'Running': True, 'Health': {'Status': 'healthy'}}))
        with self.assertRaisesRegex(ValueError, 'health_failed'):
            smoke.startup_state('postgres', {'Running': True, 'Health': {'Status': 'unhealthy'}})

    def test_startup_http_gate_remains_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'native_execution_deferred'):
            smoke.execute_native(None)

    def test_runtime_receipt_rejects_mount_security_and_extra_network(self):
        import copy
        expected = {'mounts': [], 'tmpfs': {}, 'user': '1883:1883', 'readonly': True,
                    'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'], 'network': 'owned'}
        obj = {'Config': {'User': '1883:1883'}, 'Mounts': [],
               'HostConfig': {'ReadonlyRootfs': True, 'CapDrop': ['ALL'], 'SecurityOpt': ['no-new-privileges:true']},
               'NetworkSettings': {'Networks': {'owned': {}}}}
        smoke.verify_runtime_receipt(obj, expected)
        for key, value in [('Privileged', True), ('Tmpfs', {'/unknown': 'rw'}), ('CapDrop', [])]:
            bad = copy.deepcopy(obj)
            bad['HostConfig'][key] = value
            with self.assertRaisesRegex(ValueError, 'runtime_receipt_rejected'):
                smoke.verify_runtime_receipt(bad, expected)
        obj['NetworkSettings']['Networks']['unowned'] = {}
        with self.assertRaisesRegex(ValueError, 'runtime_receipt_rejected'):
            smoke.verify_runtime_receipt(obj, expected)

    def test_startup_jobs_created_or_running_are_not_complete(self):
        for state in ({'Status': 'created', 'ExitCode': 0}, {'Running': True, 'ExitCode': 0}):
            self.assertFalse(smoke.startup_state('application-migrations', state))

    def test_runtime_wrong_image_label_and_cleanup_daemon_failure_refused(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            runner = Mock()
            registry = smoke.ResourceRegistry(fixture, runner)
            item = registry.intent('container', fixture.project + '-fixture', 'sha256:expected', {'test.run': fixture.project})
            obj = {'Id': 'a' * 64, 'Name': '/' + item['name'], 'Image': 'sha256:wrong',
                   'Created': '2026-10-13T00:00:00Z', 'Config': {'Labels': {'test.run': fixture.project}}}
            runner.return_value = Mock(returncode=0, stdout=json.dumps([obj]).encode(), stderr=b'')
            with self.assertRaisesRegex(ValueError, 'resource_ownership_refused'):
                registry.resolve(item)
            obj['Image'] = item['image']
            obj['Config']['Labels']['test.run'] = 'foreign'
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, 'resource_ownership_refused'):
                registry.resolve(item)
            runner.return_value = Mock(returncode=1, stderr=b'daemon unavailable')
            with self.assertRaisesRegex(ValueError, 'resource_inspect_unresolved'):
                registry.remove(item)


    def prepare(self, directory):
        return smoke.prepare(Path(directory) / "fixture")

    def test_private_and_safe_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for path in (fixture.base, fixture.private, fixture.artifacts):
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertEqual(fixture.env_file.stat().st_mode & 0o777, 0o600)
            self.assertFalse(list(fixture.artifacts.iterdir()))

    def test_unique_projects_and_fresh_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            a = smoke.prepare(Path(directory) / "a")
            b = smoke.prepare(Path(directory) / "b")
            self.assertNotEqual(a.project, b.project)
            self.assertIn(a.project, a.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"])

    def test_existing_directory_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            with self.assertRaises(FileExistsError):
                self.prepare(directory)

    def test_no_process_or_deployment_env_leak(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "POSTGRES_PASSWORD": "deployment-secret", "COMPOSE_FILE": "evil.yml",
                "DOCKER_HOST": "tcp://deployment", "HOME": "/deployment"}):
            fixture = self.prepare(directory)
            self.assertNotIn("deployment-secret", fixture.env_file.read_text())
            env = smoke.process_env(fixture)
            self.assertEqual(set(env), {"PATH", "HOME", "DOCKER_CONFIG", "COMPOSE_DISABLE_ENV_FILE"})
            self.assertEqual(env["COMPOSE_DISABLE_ENV_FILE"], "1")
            self.assertNotEqual(env["HOME"], "/deployment")

    def test_e5_go_builds_use_trimpath_only_for_e5_and_keep_owned_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            for is_e5 in (False, True):
                private = Path(directory) / ('e5' if is_e5 else 'default')
                private.mkdir()
                (private / 'bin').mkdir()
                fixture = type('Fixture', (), {'private': private, 'paper_p8_e5': is_e5})()
                calls = []

                def build(command, **kwargs):
                    calls.append((command, kwargs))
                    Path(command[command.index('-o') + 1]).write_bytes(b'binary')
                    return type('Result', (), {'returncode': 0})()

                with patch.object(smoke.subprocess, 'run', side_effect=build):
                    paperload = smoke.build_paperload(fixture, '/tool/go')
                    gateway = smoke.build_paper_p8_e3_gateway(fixture, '/tool/go')

                expected_flags = ['-trimpath'] if is_e5 else []
                self.assertEqual([('-trimpath' in call[0]) for call in calls], [is_e5, is_e5])
                self.assertEqual(calls[0][0], ['/tool/go', 'build', *expected_flags, '-o', str(paperload), './cmd/paperload'])
                self.assertEqual(calls[1][0], ['/tool/go', 'build', *expected_flags, '-o', str(gateway), './cmd/gateway'])
                for _, kwargs in calls:
                    self.assertEqual(kwargs['cwd'], smoke.ROOT / 'src')
                    self.assertEqual(kwargs['env'], smoke.process_env(fixture))
                    self.assertEqual(kwargs['check'], False)
                self.assertTrue((private / 'paperload-build.log').is_file())
                self.assertTrue((private / 'gateway-build.log').is_file())

    def test_all_root_interpolation_inputs_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            smoke.require_inputs(smoke.ROOT_COMPOSE.read_text(), fixture.values)
            with self.assertRaisesRegex(ValueError, "missing_interpolation"):
                smoke.require_inputs("${NEW_REQUIRED:-production}", fixture.values)

    def test_task34_telemetry_ingress_defaults_explicit(self):
        # Task 3.4 added telemetry/MQTT-ingress keys to the root compose; the
        # fixture must carry explicit safe local defaults mirroring the compose
        # `:-` fallbacks so prepare() never fails closed with a missing key.
        expected = {
            "MQTT_INGRESS_ENABLED": "false", "MQTT_CONNECT_TIMEOUT": "5s",
            "MQTT_SUBSCRIBE_TIMEOUT": "3s", "MQTT_PUBLISH_TIMEOUT": "3s",
            "MQTT_RECONNECT_MAX_INTERVAL": "30s",
            "TELEMETRY_MAX_SAMPLES_PER_MESSAGE": "1000",
            "TELEMETRY_MAX_BATCH_DURATION": "60s",
            "TELEMETRY_MAX_FUTURE_SKEW": "2m", "TELEMETRY_DB_TIMEOUT": "5s",
        }
        for key, value in expected.items():
            self.assertEqual(smoke.DEFAULTS.get(key), value, key)
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for key, value in expected.items():
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", fixture.env_file.read_text())

    def test_p4_shaper_interpolations_are_private_synthetic_defaults(self):
        expected = {
            "MQTT_SHAPER_ENABLED": "false",
            "MQTT_SHAPER_RATE_PER_SECOND": "2",
            "MQTT_SHAPER_BURST": "4",
            "MQTT_SHAPER_MAX_GATEWAYS": "4096",
            "MQTT_SHAPER_IDLE_RETENTION": "10m",
            "MQTT_INGRESS_BUFFERED_BYTES": "16777216",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                key: "deployment-value-must-not-leak" for key in expected}):
            fixture = self.prepare(directory)
            env_text = fixture.env_file.read_text()
            for key, value in expected.items():
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", env_text)
            self.assertNotIn("deployment-value-must-not-leak", env_text)

    def test_p5a_microbatch_defaults_are_explicit_and_default_off(self):
        expected = {
            "MQTT_MICROBATCH_ENABLED": "false",
            "MQTT_MICROBATCH_MAX_MESSAGES": "32",
            "MQTT_MICROBATCH_MAX_ROWS": "500",
            "MQTT_MICROBATCH_MAX_PAYLOAD_BYTES": "4194304",
            "MQTT_MICROBATCH_MAX_WAIT": "1s",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                key: "deployment-value-must-not-leak" for key in expected}):
            fixture = self.prepare(directory)
            env_text = fixture.env_file.read_text()
            for key, value in expected.items():
                self.assertEqual(smoke.DEFAULTS.get(key), value, key)
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", env_text)
            self.assertNotIn("deployment-value-must-not-leak", env_text)

    def test_p5b_ram_dedup_defaults_are_explicit_and_default_off(self):
        expected = {
            "MQTT_RAM_DEDUP_ENABLED": "false",
            "MQTT_RAM_DEDUP_MAX_ENTRIES": "100000",
            "MQTT_RAM_DEDUP_MAX_BYTES": "67108864",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                key: "deployment-value-must-not-leak" for key in expected}):
            fixture = self.prepare(directory)
            env_text = fixture.env_file.read_text()
            for key, value in expected.items():
                self.assertEqual(smoke.DEFAULTS.get(key), value, key)
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", env_text)
            self.assertNotIn("deployment-value-must-not-leak", env_text)

    def test_p4_broker_configs_keep_packet_overhead_and_control_bounds(self):
        root_config = smoke.ROOT / "config/mosquitto/mosquitto.conf"
        text = root_config.read_text()
        settings = {name: int(value) for name, value in re.findall(
            r"(?m)^(max_packet_size|message_size_limit|max_inflight_messages|"
            r"max_inflight_bytes|max_queued_messages|max_queued_bytes) (\d+)\s*$", text)}
        self.assertEqual(settings, {
            "max_packet_size": 2097152,
            "message_size_limit": 1048576,
            "max_inflight_messages": 20,
            "max_inflight_bytes": 1048576,
            "max_queued_messages": 256,
            "max_queued_bytes": 16777216,
        })
        # Leave at least 1 KiB for topic, fixed/variable headers and properties
        # above the supported 1 MiB publish payload ceiling.
        self.assertGreaterEqual(settings["max_packet_size"], settings["message_size_limit"] + 1024)
        self.assertEqual(smoke.DEFAULTS["STAGE5_PAYLOAD_BYTES"], "8192")
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            generated = (fixture.private / "mosquitto.conf").read_text()
            self.assertEqual(generated, text)
            self.assertIn("require_certificate false", generated)
            self.assertIn("plugin /usr/lib/mosquitto_dynamic_security.so", generated)
            self.assertIn("allow_anonymous false", generated)

    def test_user_management_and_auth_admin_defaults_explicit(self):
        expected = {
            "AUTH_ADMIN_BASE_URL": "http://supabase-envoy:8000",
            "AUTH_ADMIN_TIMEOUT": "10s",
            "AUTH_ADMIN_MAX_BODY_BYTES": "16384",
            "USER_LIST_MAX_USERS": "1000",
            "USER_LIST_MAX_MEMBERSHIPS": "10000",
            "USER_LIST_MAX_RESPONSE_BYTES": "2097152",
            "SUPABASE_SERVICE_ROLE_KEY": "",
        }
        for key, value in expected.items():
            self.assertEqual(smoke.DEFAULTS.get(key), value, key)
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for key, value in expected.items():
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", fixture.env_file.read_text())

    def test_dynsec_qualification_defaults_match_verified_config_identity(self):
        root = Path(__file__).resolve().parents[2]
        env_text = (root / ".env.example").read_text()
        compose_text = smoke.ROOT_COMPOSE.read_text()
        image = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        config_id = "sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4"
        self.assertRegex(env_text, rf"(?m)^MQTT_DYNSEC_IMAGE_REFERENCE={re.escape(image)}$")
        self.assertRegex(env_text, rf"(?m)^MQTT_DYNSEC_IMAGE_CONFIG_ID={re.escape(config_id)}$")
        self.assertRegex(env_text, r"(?m)^MQTT_DYNSEC_PLATFORM=linux/amd64$")
        self.assertRegex(compose_text, rf"(?m)^\s+MQTT_DYNSEC_IMAGE_REFERENCE: \$\{{MQTT_DYNSEC_IMAGE_REFERENCE:-{re.escape(image)}\}}$")
        self.assertRegex(compose_text, rf"(?m)^\s+MQTT_DYNSEC_IMAGE_CONFIG_ID: \$\{{MQTT_DYNSEC_IMAGE_CONFIG_ID:-{re.escape(config_id)}\}}$")
        self.assertRegex(compose_text, r"(?m)^\s+MQTT_DYNSEC_PLATFORM: \$\{MQTT_DYNSEC_PLATFORM:-linux/amd64\}$")

    def test_prepared_fixture_contains_verified_dynsec_qualification_identity(self):
        image = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        config_id = "sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4"
        platform = "linux/amd64"
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "MQTT_DYNSEC_IMAGE_CONFIG_ID": "sha256:deployment-leak",
                "MQTT_DYNSEC_PLATFORM": "linux/arm64"}):
            fixture = self.prepare(directory)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_IMAGE_REFERENCE"], image)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_IMAGE_CONFIG_ID"], config_id)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_PLATFORM"], platform)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_REFERENCE"], image)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], config_id)
            self.assertEqual(fixture.values["MQTT_DYNSEC_PLATFORM"], platform)
            env_content = fixture.env_file.read_text()
            self.assertIn(f"MQTT_DYNSEC_IMAGE_REFERENCE={image}\n", env_content)
            self.assertIn(f"MQTT_DYNSEC_IMAGE_CONFIG_ID={config_id}\n", env_content)
            self.assertIn(f"MQTT_DYNSEC_PLATFORM={platform}\n", env_content)
            self.assertNotIn("deployment-leak", env_content)
            # Consistency with production bootstrap and CLI qualification constants
            bootstrap_spec = importlib.util.spec_from_file_location("mqtt_bootstrap", smoke.ROOT / "scripts/mqtt-bootstrap.py")
            bootstrap_mod = importlib.util.module_from_spec(bootstrap_spec)
            bootstrap_spec.loader.exec_module(bootstrap_mod)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], getattr(bootstrap_mod, "PINNED_CONFIG_ID"))
            cli_spec = importlib.util.spec_from_file_location("dynsec_bootstrap_cli", smoke.ROOT / "scripts/tests/dynsec_bootstrap_cli.py")
            cli_mod = importlib.util.module_from_spec(cli_spec)
            cli_spec.loader.exec_module(cli_mod)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], getattr(cli_mod, "PINNED_CONFIG_ID"))

    def _verify_root_postgres_healthcheck(self, compose_text):
        pg_match = re.search(r"(?ms)^  postgres:\n(.*?)(?=\n  [a-z0-9_-]+:|\Z)", compose_text)
        self.assertIsNotNone(pg_match, "postgres service definition missing in root compose")
        pg_block = pg_match.group(1)

        hc_match = re.search(r"(?ms)^\s+healthcheck:\n(.*?)(?=\n    [a-z_]+:|\n  #|\Z)", pg_block)
        self.assertIsNotNone(hc_match, "healthcheck missing from postgres service in root compose")
        hc_block = hc_match.group(1)

        tm = re.search(r'test:\s*\[\s*"CMD-SHELL",\s*"([^"]+)"\s*\]', hc_block)
        self.assertIsNotNone(tm, "CMD-SHELL test command missing from postgres healthcheck")
        cmd_str = tm.group(1)

        # Validate explicit TCP host 127.0.0.1 and port 5432 transport
        tokens = cmd_str.split()
        self.assertEqual(tokens[0], "pg_isready", "healthcheck must execute pg_isready")
        self.assertIn("-h", tokens, "healthcheck command missing explicit -h flag")
        self.assertEqual(tokens[tokens.index("-h") + 1], "127.0.0.1", "healthcheck must target 127.0.0.1")
        self.assertIn("-p", tokens, "healthcheck command missing explicit -p flag")
        self.assertEqual(tokens[tokens.index("-p") + 1], "5432", "healthcheck must target port 5432")

        # Validate preserved user and db interpolation and safe defaults
        self.assertIn("-U ${POSTGRES_USER:-postgres}", cmd_str)
        self.assertIn("-d ${POSTGRES_DB:-iot_platform}", cmd_str)

        # Validate preserved healthcheck budgets and invariants
        self.assertRegex(hc_block, r"(?m)^\s+interval:\s*10s$")
        self.assertRegex(hc_block, r"(?m)^\s+timeout:\s*5s$")
        self.assertRegex(hc_block, r"(?m)^\s+retries:\s*5$")
        self.assertNotIn("start_period", hc_block)
        self.assertNotIn("sleep", cmd_str)

        # Validate localhost-only port mapping and service isolation preserved
        self.assertRegex(pg_block, r'(?m)^\s+-\s*"127\.0\.0\.1:5432:5432"$')
        return cmd_str

    def test_root_compose_postgres_healthcheck_explicit_tcp_transport_and_budgets(self):
        compose_text = smoke.ROOT_COMPOSE.read_text()
        self._verify_root_postgres_healthcheck(compose_text)

    def test_root_compose_postgres_healthcheck_regression_rejects_socket_only(self):
        real_text = smoke.ROOT_COMPOSE.read_text()
        socket_only_text = real_text.replace(
            "pg_isready -h 127.0.0.1 -p 5432 -U",
            "pg_isready -U",
        )
        self.assertNotEqual(real_text, socket_only_text)
        with self.assertRaises(AssertionError):
            self._verify_root_postgres_healthcheck(socket_only_text)
        mock_compose = Mock()
        mock_compose.read_text.return_value = socket_only_text
        with patch.object(smoke, "ROOT_COMPOSE", mock_compose):
            with self.assertRaises(AssertionError):
                self._verify_root_postgres_healthcheck(smoke.ROOT_COMPOSE.read_text())

    def test_smoke_override_inherits_root_postgres_healthcheck(self):
        """Verify smoke override template does not redefine or remove postgres healthcheck.

        Note: This asserts source template inheritance intent only (the override file does not
        touch healthcheck, intending Compose to inherit root compose definition).
        Actual effective configuration merge must be verified in the pending native run.
        """
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            override_text = fixture.override.read_text()
            pg_match = re.search(r"(?ms)^  postgres:\n(.*?)(?=\n  [a-z0-9_-]+:|\Z)", override_text)
            self.assertIsNotNone(pg_match, "postgres service missing in generated override")
            self.assertNotIn("healthcheck", pg_match.group(1), "smoke override must not replace root healthcheck")


    def test_compose_command_is_explicit_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            command = smoke.compose_command(fixture, "up")
            self.assertIn("--env-file", command)
            self.assertIn(str(fixture.env_file), command)
            self.assertEqual(command[-len(smoke.SERVICES):], list(smoke.SERVICES))
            self.assertNotIn("rathole", command)
            self.assertNotIn("supabase-storage", command)
            self.assertNotIn("cloudflared", command)
            self.assertIn("--no-deps", command)
            self.assertGreater(smoke.BUDGETS["up"], 0)
            self.assertLessEqual(max(smoke.BUDGETS.values()), 600)
            with self.assertRaises(ValueError):
                smoke.compose_command(fixture, "logs")

    def test_override_isolates_names_ports_mounts_and_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            text = fixture.override.read_text()
            self.assertEqual(text.count("container_name: !reset null"), len(smoke.SERVICES))
            self.assertEqual(text.count("ports: !override"), len(smoke.SERVICES))
            self.assertIn('host_ip: "127.0.0.1"', text)
            self.assertIn('published: "0"', text)
            self.assertIn("external: true", text)
            self.assertIn("target: backend", text)
            self.assertNotIn("/app/server", text)
            self.assertNotIn("storage_data", text)
            self.assertIn("service_completed_successfully", text)
            self.assertIn("io.iot.native-smoke.run", text)

    def test_supported_compose_version(self):
        smoke.require_compose_version("2.24.4")
        smoke.require_compose_version("v2.40.1")
        for value in ("2.24.3", "1.29.2", "unknown", "2.24"):
            with self.assertRaises(ValueError):
                smoke.require_compose_version(value)

    def test_bootstrap_hook_contract_and_collision(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            result = {"volume": fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"],
                      "store_created": True, "readiness": "NOT_READY", "fresh_volume": True}
            smoke.verify_bootstrap_result(fixture, result)
            for key, value in (("fresh_volume", False), ("store_created", False),
                               ("readiness", "READY"), ("volume", "deployment")):
                with self.assertRaises(ValueError):
                    smoke.verify_bootstrap_result(fixture, dict(result, **{key: value}))

    def test_no_native_execution_even_with_claimed_gate(self):
        with tempfile.TemporaryDirectory() as directory, patch("subprocess.run") as run:
            fixture = self.prepare(directory)
            for gate in (None, "approved", "bootstrap-green"):
                with self.assertRaisesRegex(RuntimeError, "native_execution_deferred"):
                    smoke.execute_native(fixture, gate)
            run.assert_not_called()

    def test_summary_rejects_raw_logs_and_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            report = smoke.summary(fixture)
            self.assertEqual(report["status"], "PREPARED_NOT_EXECUTED")
            smoke.validate_summary(report)
            for extra in ("logs", "inspect", "password", "stdout", "access_token"):
                with self.assertRaises(ValueError):
                    smoke.validate_summary(dict(report, **{extra: "secret"}))
            with self.assertRaises(ValueError):
                smoke.validate_summary(dict(report, status="PASS"))


class PaperP1Tests(unittest.TestCase):
    def test_p1_subscribers_use_existing_literal_filters_and_separate_principals(self):
        spec = Mock(gateway_id='gateway_a')
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p1='baseline')
            username, password, topic = smoke.paper_p1_subscription_target(fixture, spec, 'gateway-password', 'telemetry')
            self.assertEqual((username, password, topic), (fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'], 'gateways/+/telemetry/#'))
            self.assertNotEqual(username, fixture.values['MQTT_DYNSEC_MANAGER_USERNAME'])
            self.assertEqual(smoke.paper_p1_subscription_target(fixture, spec, 'gateway-password', 'ack'),
                             ('gateway_a', 'gateway-password', 'gateways/gateway_a/acks/#'))
            with self.assertRaises(ValueError):
                smoke.paper_p1_subscription_target(fixture, spec, 'gateway-password', 'control')

    def test_p1_suback_denied_is_never_readiness(self):
        self.assertFalse(smoke.paper_p1_suback_ready(b'Client received CONNACK (0)'))
        self.assertTrue(smoke.paper_p1_suback_ready(b'Client received SUBACK\nSubscribed (mid: 1): 1\n'))
        for code in (0, 128):
            with self.assertRaisesRegex(ValueError, '^paper_p1_subscription_failed$'):
                smoke.paper_p1_suback_ready(f'Client received SUBACK\nSubscribed (mid: 1): {code}\n'.encode())

    def test_p1_subscription_parser_correlates_topics_and_payloads(self):
        log = (b"Client received PUBLISH (d0, q1, r0, m1, 'gateways/a/telemetry/s', ... (len 7))\n"
               b'{"message_id":"one"}\n'
               b"Client received PUBLISH (d0, q1, r0, m2, 'gateways/b/telemetry/s', ... (len 7))\n"
               b'{"message_id":"two"}\n')
        self.assertEqual(smoke.paper_p1_subscription_messages(log), [
            ('gateways/a/telemetry/s', b'{"message_id":"one"}'),
            ('gateways/b/telemetry/s', b'{"message_id":"two"}')])

    def test_p1_subscriber_credentials_are_sent_on_stdin_not_host_argv(self):
        class FakeProcess:
            def __init__(self, stdout):
                import io
                self.stdin = io.BytesIO()
                self.stdout = stdout
                self.returncode = None
                self.stopped = False
            def poll(self):
                return self.returncode
            def terminate(self):
                self.stopped = True
                self.returncode = 0
            def wait(self, timeout=None):
                return self.returncode

        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p1='baseline')
            calls = []
            def popen(args, **kwargs):
                calls.append(args)
                kwargs['stdout'].write(b'OBSERVER_READY\nClient received SUBACK\nSubscribed (mid: 1): 1\n')
                kwargs['stdout'].flush()
                return FakeProcess(kwargs['stdout'])
            with patch.object(smoke.subprocess, 'Popen', side_effect=popen):
                subscription = smoke.paper_p1_subscriber(fixture, Mock(), 'broker-id',
                    Mock(gateway_id='gw_a'), 'sensitive-password', 'ack')
                smoke.paper_p1_stop_subscription(subscription)
            rendered = repr(calls[0])
            self.assertNotIn('sensitive-password', rendered)
            self.assertNotIn(fixture.values['MQTT_PASSWORD'], rendered)
            self.assertIn('gateways/gw_a/acks/#', rendered)

    def test_p1_subscriber_uses_echo_disabled_tty_and_distinct_client_id(self):
        command = smoke.paper_p1_subscription_command(
            'broker-id', 'telemetry', 'gateways/+/telemetry/#',
            message_count=2, message_timeout=30)
        self.assertEqual(command[:4], ['docker', 'exec', '-it', 'broker-id'])
        self.assertIn('stty -echo', command[6])
        self.assertLess(command[6].index('stty -echo'), command[6].index('OBSERVER_READY'))
        self.assertLess(command[6].index('OBSERVER_READY'), command[6].index('read -r password'))
        self.assertIn('-i p1-observer-telemetry-', command[6])
        self.assertIn('"$user"', command[6])
        self.assertIn('"$password"', command[6])
        self.assertNotIn('backend_service', command[6])
        self.assertNotIn('smoke_backend', command[6])
        self.assertIn('gateways/+/telemetry/#', command[6])

    def test_p1_pty_handshake_sends_secrets_only_after_ready_without_echo(self):
        import subprocess
        import termios
        script = (
            "import os,select,sys,termios,time\n"
            "a=termios.tcgetattr(0); a[3] &= ~termios.ECHO; termios.tcsetattr(0,termios.TCSANOW,a)\n"
            "time.sleep(0.15)\n"
            "if select.select([0],[],[],0)[0]: print('PREMATURE_INPUT',flush=True)\n"
            "print('OBSERVER_READY',flush=True)\n"
            "u=sys.stdin.readline().rstrip('\\n'); p=sys.stdin.readline().rstrip('\\n')\n"
            "print('CREDENTIALS_RECEIVED',flush=True)\n")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'observer.log'
            path.touch(mode=0o600)
            with path.open('wb') as stream:
                master_fd, slave_fd = smoke.paper_p1_open_echo_disabled_pty()
                proc = None
                try:
                    self.assertFalse(termios.tcgetattr(slave_fd)[3] & termios.ECHO)
                    proc = subprocess.Popen([sys.executable, '-c', script], stdin=slave_fd,
                                             stdout=stream, stderr=subprocess.STDOUT)
                    os.close(slave_fd)
                    slave_fd = -1
                    smoke.paper_p1_send_credentials_after_ready(
                        proc, path, master_fd, 'observer-user', 'observer-password', timeout=3)
                    self.assertEqual(proc.wait(timeout=3), 0)
                    stream.flush()
                    output = path.read_bytes()
                    self.assertIn(b'OBSERVER_READY', output)
                    self.assertIn(b'CREDENTIALS_RECEIVED', output)
                    self.assertNotIn(b'PREMATURE_INPUT', output)
                    self.assertNotIn(b'observer-user', output)
                    self.assertNotIn(b'observer-password', output)
                finally:
                    if proc is not None and proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=3)
                    if slave_fd >= 0:
                        os.close(slave_fd)
                    os.close(master_fd)

    def test_p1_resource_export_marks_unknown_gateway_limits_unavailable(self):
        import csv
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'resources.csv'
            smoke.paper_p1_resources_csv(target, [{'phase': 'burst', 'started_at': 't0', 'finished_at': 't1',
                'services': {'backend': {'CPUPerc': '1.0%', 'MemUsage': '2MiB / 4GiB', 'MemPerc': '1%'},
                             'gateway-p4-b': {'CPUPerc': '2.0%', 'MemUsage': '3MiB / 4GiB', 'MemPerc': '2%'}}}],
                {'backend': {'NanoCpus': 2000000000, 'Memory': 4294967296}, 'gateway-p4-b': {}})
            with target.open() as stream:
                rows = {row['service']: row for row in csv.DictReader(stream)}
            self.assertEqual(rows['backend']['cpu_limit_nanocpus'], '2000000000')
            self.assertEqual(rows['gateway-p4-b']['cpu_limit_nanocpus'], '')
            self.assertEqual(rows['gateway-p4-b']['memory_limit_bytes'], '')

    def test_p1_failed_cleanup_cannot_leave_success_run_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p1='baseline')
            smoke.durable_json(fixture.artifacts / 'run.json', {'schema_version': 1, 'status': 'PASS'})
            smoke.record_failure_report(fixture, 'cleanup', ValueError('startup_cleanup_unresolved_evidence_retained'),
                                        cleanup_verified=False, primary_phase='paper_p1',
                                        primary_error_code='paper_p1_gateway_failed')
            report = json.loads((fixture.artifacts / 'run.json').read_text())
            self.assertEqual(report['status'], 'FAIL')
            self.assertFalse(report['cleanup_verified'])
            self.assertEqual(report['error'], 'startup_cleanup_unresolved_evidence_retained')

    def test_p1_metrics_off_preserves_p0_profile_and_equivalent_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'off', paper_p1='baseline', metrics_enabled=False)
            self.assertEqual(fixture.values['METRICS_ENABLED'], 'false')
            model = smoke.dry_effective_config(fixture)
            self.assertEqual(model['services']['backend']['environment']['METRICS_ENABLED'], 'false')

    def test_p1_multiple_batches_keep_shared_last_batch_assertions(self):
        with patch.object(sys, 'path', [str(MODULE.parent), *sys.path]):
            from stage3_uplink_runner import build_gateway_assert_sql, default_specs, simulator_envelope
            base_envelope = simulator_envelope()
        from dataclasses import replace
        env = replace(base_envelope, message_id='00000000-0000-4000-8000-000000000001',
                      first_sequence=1900, expected_reported_version=20)
        sql = build_gateway_assert_sql(default_specs()[0], env, 'p1_last_batch', expected_messages=20)
        self.assertIn('IF ledger <> 20', sql)
        self.assertIn("AND message_id='00000000-0000-4000-8000-000000000001'::uuid", sql)
        self.assertIn('telemetry rows inconsistent', sql)

    def test_p1_rates_require_same_process_and_no_counter_reset(self):
        a = {'process_instance': 'a', 'elapsed_seconds': 1, 'ingress': {'Received': 2, 'Committed': 1, 'SamplesNew': 100}}
        b = {'process_instance': 'a', 'elapsed_seconds': 3, 'ingress': {'Received': 6, 'Committed': 2, 'SamplesNew': 200}}
        self.assertEqual(smoke.paper_p1_rates(a, b), {'received_delivery_msg_per_s': 2, 'new_committed_msg_per_s': 0.5, 'new_committed_samples_per_s': 50})
        for bad in (dict(b, process_instance='b'), dict(b, elapsed_seconds=1), dict(b, ingress={'Received': 0, 'Committed': 0, 'SamplesNew': 0})):
            self.assertTrue(all(v is None for v in smoke.paper_p1_rates(a, bad).values()))
        self.assertTrue(all(v is None for v in smoke.paper_p1_rates(None, b).values()))

    def test_p4_metrics_csv_exports_fixed_ingress_budget_and_shaper_fields(self):
        import csv
        sample = {'process_instance': 'a', 'elapsed_seconds': 1, 'observed_at': '2026-10-25T10:00:00Z',
                  'ingress': {'BufferedPayloadBytes': 12, 'BufferedPayloadCapacityBytes': 100,
                              'BufferedPayloadHighWaterBytes': 80, 'ByteBudgetFull': 2,
                              'ShaperEnabled': True, 'ShaperEntries': 3, 'ShaperCapacity': 10,
                              'ShaperPassed': 7, 'ShaperRateLimited': 4, 'ShaperStateCapacity': 1,
                              'GatewayID': 'PRIVATE_SENTINEL', 'payload': 'PRIVATE_SENTINEL'}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'shaped', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
            actual = {(r['metric'], r['value'], r['availability']) for r in rows if r['group'] == 'ingress'}
            expected = {('BufferedPayloadBytes', '12', 'available'),
                        ('BufferedPayloadCapacityBytes', '100', 'available'),
                        ('BufferedPayloadHighWaterBytes', '80', 'available'),
                        ('ByteBudgetFull', '2', 'available'),
                        ('ShaperEnabled', 'True', 'available'),
                        ('ShaperEntries', '3', 'available'), ('ShaperCapacity', '10', 'available'),
                        ('ShaperPassed', '7', 'available'), ('ShaperRateLimited', '4', 'available'),
                        ('ShaperStateCapacity', '1', 'available')}
            self.assertTrue(expected.issubset(actual))
            self.assertNotIn('PRIVATE_SENTINEL', target.read_text())

    def test_p4_metrics_csv_marks_missing_or_null_fixed_fields_unavailable(self):
        import csv
        sample = {'process_instance': 'a', 'elapsed_seconds': 1, 'observed_at': '2026-10-25T10:00:00Z',
                  'ingress': {'BufferedPayloadBytes': None, 'ShaperPassed': 0}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'old', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
            fields = {r['metric']: r for r in rows if r['group'] == 'ingress'}
            for name in ('BufferedPayloadBytes', 'BufferedPayloadCapacityBytes', 'ShaperEnabled'):
                self.assertEqual(fields[name]['availability'], 'unavailable')
                self.assertEqual(fields[name]['value'], '')
            self.assertEqual(fields['ShaperPassed']['availability'], 'available')
            self.assertEqual(fields['ShaperPassed']['value'], '0')

    def test_p7_websocket_metrics_csv_uses_fixed_rows_and_units(self):
        import csv
        histogram = {'count': 1, 'sum_seconds': 0.25, 'overflow': 0,
                     'upper_seconds': [0.1], 'buckets': [0]}
        ws = {'availability': 'available', 'sessions': 2, 'subscriptions': 3,
              'global_queued_work': 1, 'global_queued_bytes': 2048,
              'session_queued_in_flight_bytes': 512, 'submissions': 1,
              'new_events': 1, 'build_failures': 0, 'deliveries_enqueued': 2,
              'telemetry_writes_completed': 2, 'control_writes_completed': 1,
              'drops': 0, 'slow_closes': 0, 'auth_denials': 0, 'origin_denials': 0,
              'expiries': 0, 'revocations': 0, 'origin_missing': 0,
              'confirmed_to_write_seconds': histogram, 'identity': 'PRIVATE_SENTINEL'}
        sample = {'process_instance': 'a', 'elapsed_seconds': 1, 'observed_at': '2026-10-25T10:00:00Z',
                  'availability': {'websocket': 'available'}, 'websocket_metrics': ws}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'ws', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
        fields = {(r['metric'], r['unit']): r for r in rows if r['group'] == 'websocket'}
        self.assertEqual(fields[('global_queued_bytes', 'bytes')]['value'], '2048')
        self.assertEqual(fields[('telemetry_writes_completed', 'count')]['value'], '2')
        self.assertEqual(fields[('confirmed_to_write_seconds.sum', 'seconds')]['value'], '0.25')
        self.assertNotIn('PRIVATE_SENTINEL', str(rows))

    def test_p7_websocket_metrics_csv_preserves_disabled_availability(self):
        import csv
        sample = {'process_instance': 'a', 'elapsed_seconds': 1, 'observed_at': '2026-10-25T10:00:00Z',
                  'availability': {'websocket': 'disabled'}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'disabled', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
        self.assertTrue(all(r['availability'] == 'disabled' and r['value'] == ''
                            for r in rows if r['group'] == 'websocket'))

    def test_p5a_metrics_csv_exports_actual_persistence_and_microbatch_operations(self):
        import csv
        h = {'count': 2, 'sum_seconds': 0.03, 'overflow': 0,
             'upper_seconds': [0.01, 0.1], 'buckets': [1, 2]}
        sample = {'process_instance': 'instance-a', 'elapsed_seconds': 10,
                  'observed_at': '2026-10-25T10:00:00Z',
                  'availability': {'persistence': 'available', 'group_persistence': 'available',
                                   'batch_wait': 'available'},
                  'persistence': {'single_attempts': 5, 'group_attempts': 3, 'group_members': 8,
                                  'group_rows': 800, 'fallback_attempts': 1, 'begin_attempts': 9,
                                  'confirmed_commits': 8, 'confirmed_rollbacks': 1,
                                  'copy_calls': 4, 'copy_returned_rows': 700, 'confirmed_new_rows': 650},
                  'ingress': {'Received': 8, 'SamplesNew': 650,
                              'microbatch': {'enabled': True, 'max_messages': 32, 'max_rows': 500,
                                  'max_payload_bytes': 4194304, 'max_wait_ns': 1000000000,
                                  'groups': 3, 'members': 8, 'rows': 800, 'payload_bytes': 4200,
                                  'flush_messages': 2, 'flush_rows': 1, 'flush_bytes': 0,
                                  'flush_timer': 0, 'flush_stop': 0,
                                  'standalone_oversized_members': 0},
                              'Durations': {'batch_wait': h, 'group_persistence': h}}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'after', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
        values = {(r['group'], r['metric']): (r['value'], r['availability']) for r in rows}
        self.assertEqual(values[('persistence', 'copy_calls')], ('4', 'available'))
        self.assertEqual(values[('persistence', 'copy_returned_rows')], ('700', 'available'))
        self.assertEqual(values[('persistence', 'group_members')], ('8', 'available'))
        self.assertEqual(values[('ingress_microbatch', 'flush_messages')], ('2', 'available'))
        self.assertEqual(values[('ingress_microbatch', 'max_rows')], ('500', 'available'))
        self.assertEqual(values[('ingress_durations', 'batch_wait.count')], ('2', 'available'))
        self.assertEqual(values[('ingress_durations', 'group_persistence.sum')], ('0.03', 'available'))
        # Operation counts are not inferred from group rows or target row limits.
        self.assertEqual(values[('persistence', 'copy_calls')][0], '4')

    def test_p5a_metrics_missing_sources_are_unavailable_and_keep_instance_delta_rules(self):
        import csv
        a = {'process_instance': 'same', 'elapsed_seconds': 1, 'observed_at': 't1',
             'persistence': {'copy_calls': 2}, 'ingress': {'microbatch': {'groups': 1}}}
        b = {'process_instance': 'same', 'elapsed_seconds': 3, 'observed_at': 't2',
             'persistence': {'copy_calls': 5}, 'ingress': {'microbatch': {'groups': 3}}}
        missing = {'process_instance': 'same', 'elapsed_seconds': 4, 'observed_at': 't3', 'ingress': {}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'a', 'snapshot': a},
                                                {'phase': 'b', 'snapshot': b},
                                                {'phase': 'missing', 'snapshot': missing}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
        copy_rows = [r for r in rows if r['group'] == 'persistence' and r['metric'] == 'copy_calls']
        self.assertEqual([(r['value'], r['delta']) for r in copy_rows[:2]], [('2', ''), ('5', '3')])
        missing_fields = {(r['group'], r['metric']): r for r in rows if r['availability'] == 'unavailable'}
        for key in (('persistence', 'copy_calls'), ('ingress_microbatch', 'groups'),
                    ('ingress_microbatch', 'enabled'), ('ingress_durations', 'batch_wait.count')):
            self.assertEqual(missing_fields[key]['value'], '')

    def test_p1_metrics_csv_exports_fixed_ram_dedup_fields_with_truthful_availability(self):
        import csv
        snapshot = {'process_instance': 'cache-instance', 'elapsed_seconds': 12,
                    'observed_at': '2026-10-26T10:00:00Z',
                    'availability': {'ram_dedup': 'available'},
                    'ram_dedup': {
                        'lookup_hit_total': 4, 'lookup_miss_total': 8,
                        'lookup_expired_total': 1, 'lookup_metadata_mismatch_total': 2,
                        'lookup_disabled_bypass_total': 0, 'publication_stored_total': 7,
                        'publication_existing_total': 2, 'publication_metadata_mismatch_total': 1,
                        'publication_full_total': 3, 'publication_invalid_skip_total': 4,
                        'publication_disabled_bypass_total': 5, 'capacity_bypass_total': 6,
                        'rotations_total': 2, 'database_calls_avoided_total': 4,
                        'entries': 7, 'logical_bytes': 896, 'current_generation_entries': 5,
                        'previous_generation_entries': 2, 'enabled': True, 'ttl_seconds': 600,
                        'max_entries': 1000, 'max_bytes': 65536,
                        'unexpected_private_label': 'must_not_export'},
                    'ingress': {}}
        disabled = {'process_instance': 'cache-instance', 'elapsed_seconds': 13,
                    'observed_at': '2026-10-26T10:00:01Z',
                    'availability': {'ram_dedup': 'disabled'},
                    'ram_dedup': {'enabled': False}, 'ingress': {}}
        unavailable = {'process_instance': 'cache-instance', 'elapsed_seconds': 14,
                      'observed_at': '2026-10-26T10:00:02Z',
                      'availability': {'ram_dedup': 'unavailable'}, 'ingress': {}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [
                {'phase': 'warm', 'snapshot': snapshot},
                {'phase': 'disabled', 'snapshot': disabled},
                {'phase': 'missing', 'snapshot': unavailable}])
            with target.open() as stream:
                reader = csv.DictReader(stream)
                header = reader.fieldnames
                rows = list(reader)
        values = {(r['phase'], r['metric']): r for r in rows if r['group'] == 'ram_dedup'}
        self.assertEqual(values[('warm', 'lookup_hit_total')]['value'], '4')
        self.assertEqual(values[('warm', 'lookup_hit_total')]['unit'], 'operations')
        self.assertEqual(values[('warm', 'logical_bytes')]['unit'], 'bytes')
        self.assertEqual(values[('warm', 'ttl_seconds')]['unit'], 'seconds')
        self.assertEqual(values[('warm', 'database_calls_avoided_total')]['value'], '4')
        self.assertEqual(values[('warm', 'database_calls_avoided_total')]['delta'], '')
        self.assertEqual(values[('disabled', 'lookup_hit_total')]['availability'], 'disabled')
        self.assertEqual(values[('disabled', 'max_bytes')]['availability'], 'unavailable')
        self.assertEqual(values[('missing', 'lookup_hit_total')]['availability'], 'unavailable')
        self.assertEqual(values[('missing', 'lookup_hit_total')]['value'], '')
        self.assertNotIn('unexpected_private_label', {r['metric'] for r in rows})
        self.assertEqual(header, ['schema_version', 'timestamp_utc', 'elapsed_seconds', 'process_instance',
                                  'phase', 'group', 'metric', 'kind', 'unit', 'value', 'delta',
                                  'rate_per_s', 'availability', 'error'])

    def test_p1_cache_export_preserves_explicit_unavailable_with_present_snapshot(self):
        import csv
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                snapshot = {
                    'process_instance': 'same', 'elapsed_seconds': 1,
                    'observed_at': '2026-10-26T10:00:00Z',
                    'availability': {'ram_dedup': 'unavailable'},
                    'ram_dedup': {'enabled': enabled, 'lookup_hit_total': 0},
                    'ingress': {},
                }
                target = Path(directory) / 'metrics.csv'
                smoke.paper_p1_metrics_csv(target, [{'phase': 'unavailable', 'snapshot': snapshot}])
                with target.open() as stream:
                    rows = [row for row in csv.DictReader(stream) if row['group'] == 'ram_dedup']
                hit = next(row for row in rows if row['metric'] == 'lookup_hit_total')
                self.assertEqual(hit['availability'], 'unavailable')
                self.assertEqual(hit['value'], '')

    def test_p5a_metadata_has_explicit_cache_off_and_schema24_defaults(self):
        support = smoke.paper_p5a_support()
        metadata = support.run_metadata('off', '100')
        self.assertEqual(metadata['database_schema_version'], 24)
        self.assertEqual(metadata['ram_cache'], {
            'enabled': False, 'ttl_seconds': 600, 'max_entries': 100000,
            'max_bytes': 67108864, 'status': 'configured_not_run'})

    def test_p5a_profiles_only_change_microbatch_and_keep_cache_disabled(self):
        support = smoke.paper_p5a_support()
        for dataset, samples in (('100', 100), ('1', 1)):
            off = support.profile('off', dataset)
            on = support.profile('on', dataset)
            self.assertEqual(off['MQTT_MICROBATCH_ENABLED'], 'false')
            self.assertEqual(on['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual({k: v for k, v in off.items() if k != 'MQTT_MICROBATCH_ENABLED'},
                             {k: v for k, v in on.items() if k != 'MQTT_MICROBATCH_ENABLED'})
            for mode in ('off', 'on'):
                meta = support.run_metadata(mode, dataset,
                    backend_quota={'cpus': 2, 'memory_bytes': 4294967296},
                    effective_environment=support.profile(mode, dataset))
                self.assertEqual(meta['dataset']['samples_per_message'], samples)
                self.assertEqual(meta['effective_profile'], support.profile(mode, dataset))
                self.assertEqual(meta['backend_quota'], {'cpus': 2, 'memory_bytes': 4294967296})
                self.assertEqual(meta['offered_pattern']['status'], 'configured_not_run')
                self.assertEqual(meta['auth_tls_acl'], {
                    'auth_mode': 'rs256', 'https': True, 'mqtt_tls': True, 'acl_enforced': True})
                self.assertEqual(meta['ram_cache'], {
                    'enabled': False, 'ttl_seconds': 600, 'max_entries': 100000,
                    'max_bytes': 67108864, 'status': 'configured_not_run'})
                self.assertEqual(meta['mode'], mode)
                self.assertEqual(meta['effective_profile_source'], 'validated_compose_environment')
                self.assertEqual(meta['effective_profile']['MQTT_MICROBATCH_ENABLED'],
                                 'true' if mode == 'on' else 'false')
        with self.assertRaises(ValueError):
            support.profile('invalid', '100')
        with self.assertRaises(ValueError):
            support.profile('off', '10')

    def test_p4_rate_delta_is_same_process_positive_interval_and_not_shaper_accepted(self):
        a = {'process_instance': 'a', 'elapsed_seconds': 2,
             'ingress': {'Received': 4, 'Committed': 1, 'SamplesNew': 10, 'ShaperPassed': 8}}
        b = {'process_instance': 'a', 'elapsed_seconds': 4,
             'ingress': {'Received': 8, 'Committed': 3, 'SamplesNew': 30, 'ShaperPassed': 20}}
        self.assertEqual(smoke.paper_p1_rates(a, b), {
            'received_delivery_msg_per_s': 2, 'new_committed_msg_per_s': 1, 'new_committed_samples_per_s': 10})
        for bad in (dict(b, process_instance='b'), dict(b, elapsed_seconds=2)):
            self.assertTrue(all(value is None for value in smoke.paper_p1_rates(a, bad).values()))

    def test_p1_artifacts_allowlist_and_missing_is_not_zero(self):
        import csv
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            sample = {'process_instance': 'a', 'elapsed_seconds': 1, 'observed_at': '2026-10-24T10:00:00Z',
                      'ingress': {'Received': 3, 'SamplesNew': 100, 'token': 'PRIVATE_SENTINEL'},
                      'availability': {'command': 'disabled', 'result': 'not_implemented'}, 'password': 'PRIVATE_SENTINEL'}
            smoke.paper_p1_metrics_csv(target, [{'phase': 'before', 'snapshot': sample}, {'phase': 'running', 'snapshot': None, 'error': 'scrape_failed'}])
            self.assertNotIn('PRIVATE_SENTINEL', target.read_text())
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
            missing = [r for r in rows if r['error'] == 'scrape_failed']
            self.assertTrue(missing)
            self.assertTrue(all(r['value'] == '' for r in missing))
            unavailable = [r for r in rows if r['availability'] == 'not_implemented']
            self.assertTrue(unavailable)
            self.assertTrue(all(r['value'] == '' for r in unavailable))

    def test_p3_control_csv_reuses_fixed_groups_without_identity_labels(self):
        import csv
        hist = {'count': 3, 'sum_seconds': 0.04, 'overflow': 0,
                'upper_seconds': [0.1], 'buckets': [3]}
        sample = {'process_instance': 'a', 'elapsed_seconds': 1,
                  'observed_at': '2026-10-25T10:00:00Z',
                  'command_result': {'accepted': 3, 'db_error': 1, 'processing': hist,
                                     'command_id': 'PRIVATE_SENTINEL'},
                  'command_timeline': {'d_dispatch': hist, 'd_e2e_succeeded': hist,
                                       'missing_result': 1, 'capacity': 1024,
                                       'command_id': 'PRIVATE_SENTINEL'}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(target, [{'phase': 'control', 'snapshot': sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertTrue(any(r['group'] == 'command_timeline' and
                                r['metric'] == 'd_dispatch.count' and r['value'] == '3'
                                for r in rows))
            self.assertTrue(any(r['group'] == 'command_result' and
                                r['metric'] == 'db_error' and r['value'] == '1'
                                for r in rows))
            self.assertNotIn('PRIVATE_SENTINEL', target.read_text())

    def test_p6_control_csv_exports_fixed_dispatch_and_result_decomposition(self):
        import csv
        hist = {'count': 3, 'sum_seconds': 0.06, 'overflow': 0,
                'upper_seconds': [0.01, 0.1], 'buckets': [1, 2]}
        sample = {'process_instance': 'p', 'elapsed_seconds': 4, 'observed_at': 'now',
                  'availability': {'command': 'available', 'dispatch_e2e': 'available', 'result': 'available'},
                  'command_admission': {'accepted': 2, 'admission': hist},
                  'command_dispatch': {'live_publish_workers': 2, 'published_mark_successful_calls': 5,
                                       'queue_depth': 3, 'queue_capacity': 32, 'in_flight': 4,
                                       'notification_signals_accepted': 7,
                                       'notification_signals_coalesced': 2, 'prepare_db_recheck': hist,
                                       'published_mark': hist},
                  'command_result': {'queue_wait': hist, 'received_to_confirmed_commit': hist,
                                     'accepted': 1},
                  'command_timeline': {'commit_to_first_discovery': hist,
                                       'commit_to_first_enqueue': hist,
                                       'missing_commit_to_discovery': 2,
                                       'missing_commit_to_enqueue': 3}}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'metrics.csv'
            next_sample = dict(sample, elapsed_seconds=5, observed_at='later',
                               command_dispatch=dict(sample['command_dispatch'],
                                                     prepare_db_recheck={
                                                         'count': 5, 'sum_seconds': 0.1, 'overflow': 1,
                                                         'upper_seconds': [0.01, 0.1], 'buckets': [2, 2]}))
            smoke.paper_p1_metrics_csv(target, [{'phase': 'control', 'snapshot': sample},
                                                {'phase': 'control', 'snapshot': next_sample}])
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
        indexed = {(r['group'], r['metric']): r for r in rows}
        for group, metric, unit in (
                ('command_dispatch', 'live_publish_workers', 'count'),
                ('command_dispatch', 'published_mark_successful_calls', 'count'),
                ('command_dispatch', 'queue_depth', 'count'),
                ('command_dispatch', 'queue_capacity', 'count'),
                ('command_dispatch', 'in_flight', 'count'),
                ('command_dispatch', 'notification_signals_coalesced', 'count'),
                ('command_dispatch', 'prepare_db_recheck.count', 'count'),
                ('command_dispatch', 'published_mark.count', 'count'),
                ('command_result', 'queue_wait.count', 'count'),
                ('command_result', 'received_to_confirmed_commit.count', 'count'),
                ('command_timeline', 'commit_to_first_discovery.count', 'count'),
                ('command_timeline', 'commit_to_first_enqueue.count', 'count'),
                ('command_timeline', 'missing_commit_to_discovery', 'count')):
            self.assertEqual(indexed[(group, metric)]['unit'], unit)
            self.assertEqual(indexed[(group, metric)]['availability'], 'available')
        self.assertEqual(indexed[('command_result', 'received_to_confirmed_commit.sum')]['unit'], 'seconds')
        self.assertEqual(indexed[('command_dispatch', 'prepare_db_recheck.window.p95_seconds')]['value'], 'infinity')
        self.assertEqual(indexed[('command_dispatch', 'prepare_db_recheck.window.percentile_method')]['value'],
                         'upper_bucket_approximation')

    def test_p6_histogram_window_delta_and_percentiles_are_process_local_and_bucket_bounded(self):
        hist1 = {'count': 4, 'sum_seconds': 0.2, 'overflow': 0,
                 'upper_seconds': [0.01, 0.1, 1.0], 'buckets': [1, 2, 1]}
        hist2 = {'count': 7, 'sum_seconds': 0.8, 'overflow': 1,
                 'upper_seconds': [0.01, 0.1, 1.0], 'buckets': [2, 3, 1]}
        before = {'process_instance': 'same', 'availability': {'dispatch_e2e': 'available'},
                  'command_dispatch': {'prepare_db_recheck': hist1}}
        after = {'process_instance': 'same', 'availability': {'dispatch_e2e': 'available'},
                 'command_dispatch': {'prepare_db_recheck': hist2}}
        window = smoke.paper_p1_histogram_window(before, after, 'command_dispatch', 'prepare_db_recheck')
        self.assertEqual(window['availability'], 'available')
        self.assertEqual(window['count'], 3)
        self.assertEqual(window['overflow'], 1)
        self.assertEqual(window['buckets'], [1, 1, 0])
        self.assertEqual(window['p50_seconds'], 0.1)
        self.assertEqual(window['p95_seconds'], 'infinity')
        self.assertEqual(window['percentile_method'], 'upper_bucket_approximation')
        for bad_before, bad_after in (
                (before, dict(after, process_instance='restarted')),
                (None, after),
                (before, {'process_instance': 'same', 'availability': {'dispatch_e2e': 'available'},
                          'command_dispatch': {'prepare_db_recheck': dict(hist2, count=3)}}),
                (before, {'process_instance': 'same', 'availability': {'dispatch_e2e': 'unavailable'},
                          'command_dispatch': {'prepare_db_recheck': hist2}})):
            self.assertNotEqual(smoke.paper_p1_histogram_window(
                bad_before, bad_after, 'command_dispatch', 'prepare_db_recheck')['availability'], 'available')

    def test_p6_histogram_window_marks_disabled_missing_and_non_cumulative_bucket_data(self):
        hist = {'count': 2, 'sum_seconds': 0.3, 'overflow': 0,
                'upper_seconds': [0.01, 0.1, 1.0], 'buckets': [1, 0, 1]}
        disabled = {'process_instance': 'p', 'availability': {'dispatch_e2e': 'disabled'}}
        available = {'process_instance': 'p', 'availability': {'dispatch_e2e': 'available'},
                     'command_dispatch': {'prepare_db_recheck': hist}}
        self.assertEqual(smoke.paper_p1_histogram_window(
            disabled, available, 'command_dispatch', 'prepare_db_recheck')['availability'], 'disabled')
        invalid = dict(hist, buckets=[2, 1, 0])
        corrupt = {'process_instance': 'p', 'availability': {'dispatch_e2e': 'available'},
                   'command_dispatch': {'prepare_db_recheck': invalid}}
        self.assertEqual(smoke.paper_p1_histogram_window(
            available, corrupt, 'command_dispatch', 'prepare_db_recheck')['availability'], 'unavailable')

    def test_p3_profile_reuses_p2_but_enables_only_isolated_control(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p3', paper_p3=True)
            self.assertTrue(fixture.paper_p3)
            self.assertTrue(fixture.paper_p2)
            self.assertEqual(fixture.values['STAGE5_ENABLED'], 'true')
            self.assertEqual(fixture.values['STAGE5_RESULT_WORKERS'], '2')
            self.assertEqual(fixture.values['STAGE5_WAKEUP_ENABLED'], 'false')
            self.assertEqual(fixture.values['STAGE5_PUBLISH_QUEUE_CAPACITY'], '32')
            self.assertEqual(fixture.values['DATABASE_MAX_CONNS'], '12')
            model = smoke.dry_effective_config(fixture)
            self.assertEqual(model['services']['backend']['environment']['STAGE5_ENABLED'], 'true')
            self.assertEqual(model['services']['backend']['environment']['STAGE5_WAKEUP_ENABLED'], 'false')
            self.assertEqual(model['services']['backend']['environment']['STAGE5_PUBLISH_QUEUE_CAPACITY'], '32')
            self.assertEqual(model['services']['backend']['environment']['SUPABASE_JWT_MODE'], 'rs256')
            ordinary = smoke.prepare(Path(directory) / 'p2', paper_p2=True)
            self.assertEqual(ordinary.values['STAGE5_ENABLED'], 'false')

    def test_p3_microbatch_variant_is_explicit_opt_in_and_keeps_default_off(self):
        with tempfile.TemporaryDirectory() as directory:
            ordinary = smoke.prepare(Path(directory) / 'ordinary', paper_p3=True)
            variant = smoke.prepare(Path(directory) / 'microbatch', paper_p3=True,
                                    paper_p3_microbatch=True)
            self.assertFalse(ordinary.paper_p3_microbatch)
            self.assertEqual(ordinary.values['MQTT_MICROBATCH_ENABLED'], 'false')
            self.assertTrue(variant.paper_p3_microbatch)
            self.assertTrue(variant.paper_p3)
            self.assertTrue(variant.paper_p2)
            self.assertEqual(variant.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(variant.values['STAGE5_ENABLED'], 'true')
            model = smoke.dry_effective_config(variant)
            env = model['services']['backend']['environment']
            self.assertEqual(env['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(env['METRICS_ENABLED'], 'true')

    def test_p3_microbatch_variant_rejects_other_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, '^paper_p3_microbatch_mode_rejected$'):
                smoke.prepare(Path(directory) / 'bad', paper_p3_microbatch=True)

    def test_p5b_control_variant_requires_p5a_control_and_keeps_schema24_cache_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, '^paper_p5b_control_requires_p5a_control$'):
                smoke.prepare(Path(directory) / 'bad', paper_p3=True, paper_p5b_control_regression=True)
            fixture = smoke.prepare(Path(directory) / 'good', paper_p3=True, paper_p3_microbatch=True,
                                    paper_p5b_control_regression=True)
            self.assertTrue(fixture.paper_p5b_control_regression)
            self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_ENABLED'], 'true')
            model = smoke.dry_effective_config(fixture)
            env = model['services']['backend']['environment']
            self.assertEqual(env['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(env['MQTT_RAM_DEDUP_MAX_ENTRIES'], '100000')
            self.assertEqual(env['MQTT_RAM_DEDUP_MAX_BYTES'], '67108864')
            self.assertIn('000024_telemetry_ledger_timestamp.up.sql',
                          model['services']['supabase-compat-migration']['command'][-1])

    def test_p5b_control_cli_requires_both_explicit_regression_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            captured = {}
            def startup(fixture):
                captured['fixture'] = fixture
                return {'status': 'P3_FIXTURE_ONLY'}
            argv = ['native_compose_smoke.py', '--prepare', str(Path(directory) / 'cli'),
                    '--paper-p3', '--p5a-control-regression', '--p5b-control-regression']
            with patch.object(sys, 'argv', argv), patch.object(smoke, 'startup_only', side_effect=startup), \
                    patch.object(smoke, 'is_fixture_verified', return_value=True), patch.object(smoke.shutil, 'rmtree'):
                self.assertEqual(smoke.main(), 0)
            self.assertTrue(captured['fixture'].paper_p5b_control_regression)
            self.assertIn('--p5b-control-regression', smoke.cli_arguments())

    def test_p3_microbatch_cli_routes_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            captured = {}
            def startup(fixture):
                captured['fixture'] = fixture
                return {'status': 'P3_FIXTURE_ONLY'}
            argv = ['native_compose_smoke.py', '--prepare', str(Path(directory) / 'cli'),
                    '--paper-p3', '--p5a-control-regression']
            with patch.object(sys, 'argv', argv), patch.object(smoke, 'startup_only', side_effect=startup), \
                    patch.object(smoke, 'is_fixture_verified', return_value=True), patch.object(smoke.shutil, 'rmtree'):
                self.assertEqual(smoke.main(), 0)
            fixture = captured['fixture']
            self.assertTrue(fixture.paper_p3_microbatch)
            self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertIn('--p5a-control-regression', smoke.cli_arguments())

    def test_p3_microbatch_acceptance_label_requires_regression_and_exact_cleanup(self):
        evidence = {'full_p3_accepted': True, 'microbatch_control_regression_accepted': True}
        good = smoke.paper_p3_acceptance_report('fixture', evidence, True, True,
                                                {'container': 1, 'network': 1, 'volume': 1})
        self.assertEqual(good['status'], 'PAPER_P5A_CONTROL_REGRESSION_ACCEPTED')
        self.assertEqual(good['effective_optimizations'], ['mqtt_microbatch'])
        self.assertTrue(good['exact_cleanup_verified'])
        for cleanup, candidate in ((False, evidence), (True, {'full_p3_accepted': True,
                                                               'microbatch_control_regression_accepted': False})):
            report = smoke.paper_p3_acceptance_report('fixture', candidate, cleanup, True)
            self.assertEqual(report['status'], 'FAIL')
            if not cleanup:
                self.assertFalse(report['exact_cleanup_verified'])

    def test_p5b_control_report_is_narrow_and_requires_exact_cleanup(self):
        evidence = {'full_p3_accepted': True, 'microbatch_control_regression_accepted': True}
        report = smoke.paper_p3_acceptance_report('fixture', evidence, True, True, {}, True)
        self.assertEqual(report['status'], 'PAPER_P5B_CONTROL_PRESERVATION_ACCEPTED')
        self.assertTrue(report['ram_dedup_enabled'])
        self.assertFalse(report.get('full_p5b_accepted', False))
        self.assertEqual(smoke.paper_p3_acceptance_report('fixture', evidence, False, True, {}, True)['status'], 'FAIL')

    def test_p4_profiles_are_comparable_and_use_real_shaper_switch(self):
        expected = {
            'MQTT_SHAPER_RATE_PER_SECOND': '2', 'MQTT_SHAPER_BURST': '4',
            'MQTT_SHAPER_MAX_GATEWAYS': '4096', 'MQTT_SHAPER_IDLE_RETENTION': '10m',
            'MQTT_INGRESS_BUFFERED_BYTES': '16777216', 'MQTT_QUEUE_CAPACITY': '256',
            'MQTT_MAX_PAYLOAD_BYTES': '65536', 'MQTT_WORKER_COUNT': '4',
            'DATABASE_MAX_CONNS': '10', 'STAGE5_ENABLED': 'false', 'METRICS_ENABLED': 'true',
        }
        with tempfile.TemporaryDirectory() as directory:
            baseline = smoke.prepare(Path(directory) / 'baseline', paper_p4='baseline')
            optimized = smoke.prepare(Path(directory) / 'optimized', paper_p4='optimized')
            for fixture, enabled in ((baseline, 'false'), (optimized, 'true')):
                self.assertTrue(fixture.paper_p4)
                self.assertEqual(fixture.paper_p4_mode, 'baseline' if enabled == 'false' else 'optimized')
                self.assertTrue(fixture.paper_p2)
                self.assertEqual(fixture.values['MQTT_SHAPER_ENABLED'], enabled)
                self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'false')
                for key, value in expected.items():
                    self.assertEqual(fixture.values[key], value, key)
                self.assertIn('auth_socket:', (fixture.private / 'compose.p2.yml').read_text())
                self.assertIn(str(smoke.PAPER_P0_TEMPLATE), smoke.compose_command(fixture, 'config'))
                self.assertIn(str(fixture.private / 'compose.p2.yml'), smoke.compose_command(fixture, 'config'))
                effective = {'services': {'backend': {'environment': dict(fixture.values)}}}
                self.assertTrue(smoke.paper_p4_support().validate_effective_profile(fixture, effective))
                for key in smoke.paper_p4_support().profile(fixture.paper_p4_mode):
                    tampered = {'services': {'backend': {'environment': dict(fixture.values)}}}
                    tampered['services']['backend']['environment'][key] = 'tampered'
                    with self.assertRaisesRegex(ValueError, '^paper_p4_profile_rejected$'):
                        smoke.paper_p4_support().validate_effective_profile(fixture, tampered)
            self.assertNotEqual(baseline.values['MQTT_SHAPER_ENABLED'], optimized.values['MQTT_SHAPER_ENABLED'])

    def test_p4_override_does_not_replace_root_metrics_environment_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p4', paper_p4='baseline')
            backend = re.search(r'(?ms)^  backend:\n(.*?)(?=^  [a-z0-9_-]+:|^volumes:)',
                                fixture.override.read_text())
            self.assertIsNotNone(backend)
            self.assertNotIn('METRICS_ENABLED', backend.group(1))
            self.assertIn('METRICS_ENABLED: ${METRICS_ENABLED:-false}', smoke.ROOT_COMPOSE.read_text())

    def test_p4_rejects_incompatible_or_metrics_off_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            for kwargs in (
                {'paper_p4': 'optimized', 'metrics_enabled': False},
                {'paper_p4': 'invalid'},
                {'paper_p4': 'baseline', 'paper_p3': True},
                {'paper_p4': 'baseline', 'paper_p1': 'baseline'},
            ):
                with self.assertRaises(ValueError):
                    smoke.prepare(Path(directory) / ('mode-' + str(len(kwargs)) + str(kwargs)), **kwargs)

    def test_p4_lifecycle_requires_fixture_and_cleanup_veto(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p4', paper_p4='baseline')
            with self.assertRaisesRegex(ValueError, '^paper_p4_fixture_required$'):
                smoke.paper_p4_support().run(smoke, fixture, None, None, None, None, None)
            labels = {'io.iot.native-smoke.run': fixture.project}
            def owned_item(kind, identity):
                return {'kind': kind, 'name': identity, 'run': fixture.project,
                        'task': 'native-compose-allocation', 'allocation_attempted': True,
                        'removed': True, 'labels': labels,
                        'receipt': {'id': identity if kind == 'volume' else 'a' * 64,
                                    'created': '2026-10-25T08:00:00Z', 'labels': dict(labels)}}
            registry = Mock(items=[owned_item('container', 'backend'),
                                   owned_item('network', 'network'),
                                   owned_item('volume', fixture.project + '_store')])
            counts = {'container': 1, 'network': 1, 'volume': 1}
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                                               {'container': 0, 'network': 0, 'volume': 0}, True,
                                                               Mock(items=[])))
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True}, counts, True,
                                                               Mock(items=[owned_item('container', 'backend')])))
            missing_receipt = owned_item('container', 'backend')
            missing_receipt['receipt'] = None
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                                               counts, True, Mock(items=[missing_receipt])))
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True}, counts, False, registry))
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': False}, counts, True, registry))
            self.assertTrue(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True}, counts, True, registry))
            registry.items[-1]['removed'] = False
            self.assertFalse(smoke.paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True}, counts, True, registry))

    def test_p7_slow_viewer_mode_is_profiled_dispatched_and_proof_gated(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'slow', paper_p7=True, paper_p7_mode='slow_viewer')
            self.assertTrue(fixture.paper_p2)
            self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(smoke.paper_p7_support().pilot_mode(fixture), 'slow_viewer')
            labels = {'io.iot.native-smoke.run': fixture.project}
            item = {'kind': 'volume', 'name': fixture.project + '_store', 'run': fixture.project,
                    'task': 'native-compose-allocation', 'allocation_attempted': True, 'removed': True,
                    'labels': labels, 'receipt': {'id': fixture.project + '_store',
                    'created': '2026-10-27T08:00:00Z', 'labels': dict(labels)}}
            registry = Mock(items=[item])
            proof_path = fixture.artifacts / 'p7-slow-viewer-proof.json'
            ids = [f'id-{i}' for i in range(1000)]
            proof_path.write_text(json.dumps({'functional_status': 'PASS',
                'capture_finalization_status': 'finished', 'resource_cleanup_status': 'pending',
                'workload': {'messages_offered': 1000, 'application_acks_expected': 1000,
                    'application_acks_actual': 1000, 'unique_application_acks': 1000, 'message_ids': ids},
                'database': {'actual_counts': {'ledger': 1000, 'raw': 1000000, 'temporal': 1000},
                    'expected_counts': {'ledger': 1000, 'raw': 1000000, 'temporal': 1000},
                    'ledger_ids': ids, 'temporal_ids': ids, 'ledger_provenance_match': True,
                    'ledger_ids_match': True, 'ledger_time_provenance_match': True,
                    'raw_content_match': True, 'raw_value_sequence_time_match': True,
                    'temporal_ids_match': True, 'temporal_value_time_match': True,
                    'temporal_provenance_match': True, 'temporal_values': [
                        {'message_id': item, 'observed_at': '2026-10-27T00:00:00Z', 'value_number': 10000}
                        for item in ids], 'ledger_time_values': [
                        {'message_id': item, 'measured_at': '2026-10-27T00:00:00Z'} for item in ids],
                    'raw_chunk_statistics': {'total_rows_expected': 1000000, 'rows_checked': 1000000,
                        'chunks_checked': 10, 'failing_chunk': None}},
                'policy': {'close_causes': ['queue_overflow'], 'close_cause_observed': True,
                    'slow_close_delta_verified': True, 'queue_drained': True},
                'capture_finalization': {'expected_a_count': 1000,
                    'a': {'status': 'PASS', 'count': 1000, 'event_header_count': 1000,
                        'headers_match_count': True, 'namespace_match': True},
                    'b': {'status': 'ready', 'count': 0, 'event_header_count': 0,
                        'headers_match_count': True, 'namespace_match': True}}}))
            good = {'status': 'PASS', 'mode': 'paper_p7_slow_viewer', 'full_p7_accepted': False,
                    'exact_cleanup_verified': False, 'evidence': {key: True for key in
                    smoke.paper_p7_support().SLOW_VIEWER_REQUIRED_PROOFS},
                    'capture_finalization_status': 'finished',
                    'precleanup_proof_artifact': 'p7-slow-viewer-proof.json',
                    'precleanup_proof_path': str(proof_path)}
            self.assertTrue(smoke.paper_p7_cleanup_acceptance(fixture, {'paper_p7': good},
                {'container': 0, 'network': 0, 'volume': 1}, True, registry))
            bad = dict(good, evidence=dict(good['evidence'], database_counts=False))
            self.assertFalse(smoke.paper_p7_cleanup_acceptance(fixture, {'paper_p7': bad},
                {'container': 0, 'network': 0, 'volume': 1}, True, registry))

    def test_p5a_workload_acceptance_is_separate_and_exact_cleanup_gated(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p5a', paper_p5a=('on', '1'))
            labels = {'io.iot.native-smoke.run': fixture.project}
            item = {'kind': 'volume', 'name': fixture.project + '_outbox', 'run': fixture.project,
                    'task': 'native-compose-allocation', 'allocation_attempted': True, 'removed': True,
                    'labels': labels, 'receipt': {'id': fixture.project + '_outbox',
                    'created': '2026-10-25T08:00:00Z', 'labels': dict(labels)}}
            registry = Mock(items=[item])
            counts = {'container': 0, 'network': 0, 'volume': 1}
            self.assertFalse(smoke.paper_p5a_cleanup_acceptance(fixture,
                {'full_p5a_workload_accepted': False}, counts, True, registry))
            self.assertFalse(smoke.paper_p5a_cleanup_acceptance(fixture,
                {'full_p5a_workload_accepted': True}, counts, False, registry))
            self.assertTrue(smoke.paper_p5a_cleanup_acceptance(fixture,
                {'full_p5a_workload_accepted': True}, counts, True, registry))

    def test_p4_cli_dispatch_and_wrapper_are_explicit(self):
        self.assertIn('--paper-p4', smoke.cli_arguments())
        wrapper = (smoke.ROOT / 'scripts/test-paper-p4.sh').read_text()
        self.assertIn('native_compose_smoke.py', wrapper)
        self.assertIn('--paper-p4', wrapper)

    def test_p4_cli_passes_mode_only_to_p4_and_never_runs_workload(self):
        with tempfile.TemporaryDirectory() as directory:
            captured = {}
            def startup(fixture):
                captured['fixture'] = fixture
                return {'status': 'P4_FIXTURE_ONLY'}
            argv = ['native_compose_smoke.py', '--prepare', str(Path(directory) / 'p4'),
                    '--paper-p4', '--mode', 'optimized']
            with patch.object(sys, 'argv', argv), patch.object(smoke, 'startup_only', side_effect=startup), \
                    patch.object(smoke, 'is_fixture_verified', return_value=True), patch.object(smoke.shutil, 'rmtree'):
                self.assertEqual(smoke.main(), 0)
            fixture = captured['fixture']
            self.assertTrue(fixture.paper_p4)
            self.assertIsNone(fixture.paper_p1)
            self.assertEqual(fixture.paper_p4_mode, 'optimized')
            self.assertEqual(fixture.values['MQTT_SHAPER_ENABLED'], 'true')

    def test_p5a_prepares_off_on_profiles_for_both_datasets_without_execution_claim(self):
        support = smoke.paper_p5a_support()
        with tempfile.TemporaryDirectory() as directory:
            for mode in ('off', 'on'):
                for dataset in ('100', '1'):
                    fixture = smoke.prepare(Path(directory) / f'{mode}-{dataset}',
                                            paper_p5a=(mode, dataset))
                    self.assertTrue(fixture.paper_p5a)
                    self.assertEqual((fixture.paper_p5a_mode, fixture.paper_p5a_dataset), (mode, dataset))
                    self.assertTrue(fixture.paper_p2)
                    self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'true' if mode == 'on' else 'false')
                    self.assertEqual(fixture.values['MQTT_SHAPER_ENABLED'], 'false')
                    self.assertEqual(fixture.values['STAGE5_ENABLED'], 'false')
                    self.assertEqual(fixture.values['DATABASE_MAX_CONNS'], '10')
                    self.assertEqual(fixture.values['MQTT_WORKER_COUNT'], '4')
                    self.assertEqual(fixture.values['MQTT_QUEUE_CAPACITY'], '256')
                    self.assertEqual(fixture.values['SUPABASE_JWT_MODE'], 'rs256')
                    support.validate_effective_profile(fixture.values, mode, dataset)
                    env = dict(fixture.values)
                    env['MQTT_MICROBATCH_ENABLED'] = 'false' if mode == 'on' else 'true'
                    with self.assertRaisesRegex(ValueError, '^paper_p5a_profile_rejected$'):
                        support.validate_effective_profile(env, mode, dataset)
                    metadata = support.run_metadata(mode, dataset,
                        effective_environment=fixture.values)
                    self.assertEqual(metadata['offered_pattern']['status'], 'configured_not_run')
                    self.assertEqual(metadata['backend_quota'], {'cpus': 2, 'memory_bytes': 4294967296})
                    self.assertNotIn('password', json.dumps(metadata).lower())
                    self.assertNotIn('secret', json.dumps(metadata).lower())
                    artifact = json.loads((fixture.artifacts / 'run.json').read_text())
                    self.assertEqual(artifact['status'], 'PREPARED_NOT_EXECUTED')
                    self.assertIs(artifact['native_executed'], False)
                    self.assertNotIn('full_p5a_accepted', artifact)

    def test_p5a_rejects_conflicting_or_invalid_fixture_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            for kwargs in (
                {'paper_p5a': ('off', '100'), 'paper_p3': True},
                {'paper_p5a': ('on', '1'), 'paper_p4': 'baseline'},
                {'paper_p5a': ('on', '10')},
                {'paper_p5a': ('invalid', '100')},
                {'paper_p5a': ('off', '100'), 'metrics_enabled': False},
            ):
                with self.assertRaises(ValueError):
                    smoke.prepare(Path(directory) / (str(len(kwargs)) + str(kwargs)), **kwargs)

    def test_p5a_dispatch_is_explicitly_pending_and_cli_routes_only_p5a(self):
        support = smoke.paper_p5a_support()
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p5a', paper_p5a=('on', '1'))
            with self.assertRaisesRegex(ValueError, '^paper_p5a_fixture_required$'):
                support.run(smoke, fixture, None, None, None, None, None)
            captured = {}
            def startup(value):
                captured['fixture'] = value
                return {'status': 'P5A_FIXTURE_ONLY', 'native_executed': False}
            argv = ['native_compose_smoke.py', '--prepare', str(Path(directory) / 'cli'),
                    '--paper-p5a', '--mode', 'off', '--dataset', '100']
            with patch.object(sys, 'argv', argv), patch.object(smoke, 'startup_only', side_effect=startup), \
                    patch.object(smoke, 'is_fixture_verified', return_value=True), patch.object(smoke.shutil, 'rmtree'):
                self.assertEqual(smoke.main(), 0)
            self.assertTrue(captured['fixture'].paper_p5a)
            self.assertEqual(captured['fixture'].paper_p5a_dataset, '100')
            self.assertIn('--paper-p5a', smoke.cli_arguments())
            wrapper = (smoke.ROOT / 'scripts/test-paper-p5a.sh').read_text()
            self.assertIn('--paper-p5a', wrapper)

    def test_p5b_cli_selects_cache_mode_without_running_native_workload(self):
        support = smoke.paper_p5a_support()
        with tempfile.TemporaryDirectory() as directory:
            captured = {}
            def startup(fixture):
                captured['fixture'] = fixture
                return {'status': 'P5B_PROFILE_FIXTURE_ONLY', 'native_executed': False}
            argv = ['native_compose_smoke.py', '--prepare', str(Path(directory) / 'p5b'),
                    '--paper-p5b', '--cache-mode', 'off', '--dataset', '100']
            with patch.object(sys, 'argv', argv), patch.object(smoke, 'startup_only', side_effect=startup), \
                    patch.object(smoke, 'is_fixture_verified', return_value=True), patch.object(smoke.shutil, 'rmtree'):
                self.assertEqual(smoke.main(), 0)
            fixture = captured['fixture']
            self.assertTrue(fixture.paper_p5b)
            self.assertEqual((fixture.paper_p5b_cache_mode, fixture.paper_p5a_dataset), ('off', '100'))
            self.assertEqual(fixture.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_ENABLED'], 'false')
            artifact = json.loads((fixture.artifacts / 'run.json').read_text())
            self.assertEqual(artifact['comparison']['cache_mode'], 'off')
            self.assertEqual(artifact['phases']['duplicate_replay']['status'], 'configured_not_run')
            self.assertFalse(artifact['native_executed'])
            self.assertIn('--paper-p5b', smoke.cli_arguments())

    def test_p5b_prepared_arms_use_same_schema24_profile_except_ram_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            for dataset in ('100', '1'):
                prepared = {}
                for mode in ('off', 'on'):
                    fixture = smoke.prepare(Path(directory) / f'{mode}-{dataset}',
                                            paper_p5b=(mode, dataset))
                    prepared[mode] = fixture
                    artifact = json.loads((fixture.artifacts / 'run.json').read_text())
                    self.assertEqual(artifact['database_schema_version'], 24)
                    self.assertEqual(artifact['comparison']['microbatch_mode'], 'on')
                    self.assertEqual(artifact['effective_profile']['MQTT_MICROBATCH_ENABLED'], 'true')
                    self.assertEqual(artifact['effective_profile']['MQTT_RAM_DEDUP_ENABLED'],
                                     'true' if mode == 'on' else 'false')
                    self.assertEqual(fixture.values['MQTT_SHAPER_ENABLED'], 'false')
                    self.assertEqual(fixture.values['DATABASE_MAX_CONNS'], '10')
                    self.assertEqual(fixture.values['MQTT_WORKER_COUNT'], '4')
                    self.assertEqual(artifact['phases']['new_messages']['status'], 'configured_not_run')
                    self.assertEqual(artifact['phases']['duplicate_replay']['offered_attempts'], 24)
                    self.assertIsInstance(artifact['source']['source_revision'], str)
                    self.assertIsInstance(artifact['source']['dirty_worktree'], bool)
                    self.assertEqual(artifact['source']['image_manifest']['status'], 'captured_after_image_build')
                off_profile = smoke.paper_p5a_support().p5b_profile('off', dataset)
                on_profile = smoke.paper_p5a_support().p5b_profile('on', dataset)
                self.assertEqual({k: v for k, v in off_profile.items() if k != 'MQTT_RAM_DEDUP_ENABLED'},
                                 {k: v for k, v in on_profile.items() if k != 'MQTT_RAM_DEDUP_ENABLED'})

    def test_p5b_cli_rejects_missing_or_conflicting_cache_selector(self):
        cases = (
            ['--paper-p5b', '--dataset', '1'],
            ['--cache-mode', 'on', '--dataset', '1'],
            ['--paper-p5b', '--cache-mode', 'on', '--dataset', '1', '--paper-p5a', '--mode', 'on'],
        )
        for extra in cases:
            with self.subTest(extra=extra), patch.object(sys, 'argv', ['native_compose_smoke.py',
                    '--prepare', '/tmp/unused-p5b', *extra]), \
                    self.assertRaises(SystemExit):
                smoke.main()

    def test_p5b_cache_flag_selects_only_the_schema24_p5a_on_one_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p5b', paper_p5a=('on', '1'),
                                    paper_p5b_cache_smoke=True)
            self.assertTrue(fixture.paper_p5b_cache_smoke)
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_MAX_ENTRIES'], '100000')
            self.assertEqual(fixture.values['MQTT_RAM_DEDUP_MAX_BYTES'], '67108864')
            metadata = smoke.paper_p5a_support().run_metadata('on', '1', cache_smoke=True,
                                                               effective_environment=fixture.values)
            self.assertEqual(metadata['database_schema_version'], 24)
            self.assertEqual(metadata['cache_smoke_contract']['status'],
                             'configured_not_run')
            self.assertEqual(metadata['cache_smoke_contract']['phases'],
                             ['warm_from_new_commit', 'exact_replay', 'representative_conflicts',
                              'cold_exact_replay_after_controlled_backend_restart'])
            for profile in (('off', '1'), ('on', '100')):
                with self.assertRaisesRegex(ValueError, '^paper_p5b_cache_profile_rejected$'):
                    smoke.prepare(Path(directory) / ('bad' + profile[0] + profile[1]),
                                  paper_p5a=profile, paper_p5b_cache_smoke=True)

    def test_p1_profile_reuses_p0_resources_but_keeps_its_own_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            for mode in ('baseline', 'optimized'):
                fixture = smoke.prepare(Path(directory) / mode, paper_p1=mode)
                self.assertEqual(fixture.paper_p1, mode)
                self.assertFalse(fixture.paper_p0)
                self.assertIn('supabase-storage', smoke.fixture_services(fixture))
                self.assertIn(str(smoke.PAPER_P0_TEMPLATE), smoke.compose_command(fixture, 'config'))
                self.assertEqual(fixture.values['METRICS_ENABLED'], 'true')
                self.assertEqual(fixture.values['STAGE5_ENABLED'], 'false')
                smoke.dry_effective_config(fixture)
            for mode in ('invalid', 'BASELINE'):
                with self.assertRaises(ValueError):
                    smoke.prepare(Path(directory) / mode, paper_p1=mode)
            with self.assertRaises(ValueError):
                smoke.prepare(Path(directory) / 'both', paper_p0=True, paper_p1='baseline')


class PaperP0Tests(unittest.TestCase):
    def test_p0_git_trust_is_per_command_read_only_and_keeps_private_env(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p0=True)
            runner = Mock(return_value=Mock(returncode=0, stdout=b'a' * 40 + b'\n', stderr=b''))
            self.assertEqual(smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD', runner=runner), 'a' * 40)
            self.assertEqual(runner.call_args.args[0], [
                'git', '--no-optional-locks', '-c', 'safe.directory=' + str(smoke.ROOT),
                '-C', str(smoke.ROOT), 'rev-parse', 'HEAD'])
            self.assertEqual(runner.call_args.kwargs['env'], smoke.process_env(fixture))
            self.assertTrue(runner.call_args.kwargs['capture_output'])
            self.assertEqual(runner.call_args.kwargs['timeout'], 10)
            for args in (('config', '--global', '--add', 'safe.directory', '*'), ('reset', '--hard')):
                with self.assertRaisesRegex(ValueError, 'paper_p0_metadata_failed'):
                    smoke.paper_p0_git(fixture, *args, runner=runner)
            self.assertEqual(runner.call_count, 1)

    def test_p0_git_failure_does_not_echo_raw_stderr(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p0=True)
            runner = Mock(return_value=Mock(returncode=128, stdout=b'', stderr=b'private diagnostic sentinel'))
            with self.assertRaisesRegex(ValueError, '^paper_p0_metadata_failed$'):
                smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD', runner=runner)

    def test_p0_git_real_metadata_works_with_private_home(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p0=True)
            self.assertRegex(smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'), r'^[0-9a-f]{40}$')
            self.assertTrue(smoke.paper_p0_git(fixture, 'branch', '--show-current'))
            self.assertIsInstance(smoke.paper_p0_git(fixture, 'status', '--porcelain'), str)

    def test_p0_temporal_query_uses_backend_utc_z_contract(self):
        from urllib.parse import parse_qs
        query = parse_qs(smoke.paper_p0_temporal_query(
            '2026-10-24T10:00:00.123456+00:00',
            '2026-10-24T10:00:01.113456+00:00'))
        self.assertEqual(query, {
            'attrs': ['measurement'], 'timerel': ['between'],
            'timeAt': ['2026-10-24T09:59:59.123456Z'],
            'endTimeAt': ['2026-10-24T10:00:02.113456Z']})
        # Same instant, different input offset: still send canonical UTC Z.
        other = parse_qs(smoke.paper_p0_temporal_query(
            '2026-10-24T17:00:00.123456+07:00',
            '2026-10-24T17:00:01.113456+07:00'))
        self.assertEqual(other, query)

    def test_p0_temporal_query_rejects_ambiguous_naive_time(self):
        with self.assertRaisesRegex(ValueError, 'paper_p0_read_failed'):
            smoke.paper_p0_temporal_query('2026-10-24T10:00:00', '2026-10-24T10:00:01')

    def test_effective_p0_model_rejects_flag_quota_mount_and_port_drift(self):
        import copy
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p0=True)
            model = smoke.dry_effective_config(fixture)
            self.assertEqual(len(model['services']), 10)
            self.assertEqual(str(model['services']['backend']['mem_limit']), '4294967296')
            self.assertEqual(model['services']['supabase-storage']['volumes'][0]['source'], 'storage_data')
            for key, value in [('MQTT_INGRESS_ENABLED', 'false'), ('STAGE5_ENABLED', 'true'),
                               ('DATABASE_MAX_CONNS', '5'), ('MQTT_MAX_PAYLOAD_BYTES', '1048576'),
                               ('MQTT_CREDENTIAL_RUNTIME_ENABLED', 'true')]:
                bad = copy.deepcopy(model)
                bad['services']['backend']['environment'][key] = value
                with self.assertRaisesRegex(ValueError, 'paper_p0_profile_rejected'):
                    smoke.validate_effective_model(fixture, bad)
            for key, value in [('cpus', 0), ('mem_limit', 4000000000)]:
                bad = copy.deepcopy(model)
                bad['services']['backend'][key] = value
                with self.assertRaisesRegex(ValueError, 'paper_p0_profile_rejected'):
                    smoke.validate_effective_model(fixture, bad)
            bad = copy.deepcopy(model)
            bad['services']['supabase-storage']['volumes'][0]['source'] = 'postgres_data'
            with self.assertRaisesRegex(ValueError, 'effective_mounts_rejected'):
                smoke.validate_effective_model(fixture, bad)
            bad = copy.deepcopy(model)
            bad['services']['mosquitto']['ports'] = [{'target': 8883, 'published': '8883'}]
            with self.assertRaisesRegex(ValueError, 'effective_ports_rejected'):
                smoke.validate_effective_model(fixture, bad)
            bad = copy.deepcopy(model)
            bad['volumes']['storage_data']['name'] = 'deployment_storage_data'
            with self.assertRaisesRegex(ValueError, 'effective_names_rejected'):
                smoke.validate_effective_model(fixture, bad)

    def test_effective_e3_polling_profile_accepts_required_stage5_control(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p8_e3='polling')
            model = smoke.dry_effective_config(fixture)
            self.assertEqual(model['services']['backend']['environment']['STAGE5_ENABLED'], 'true')
            self.assertEqual(model['services']['backend']['environment']['DATABASE_MAX_CONNS'], '12')
            smoke.validate_effective_model(fixture, model)

    def test_effective_p6_profile_accepts_stage5_pool_and_metrics_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p6=('polling', '100'))
            model = smoke.dry_effective_config(fixture)
            environment = model['services']['backend']['environment']
            self.assertEqual(environment['STAGE5_ENABLED'], 'true')
            self.assertEqual(environment['DATABASE_MAX_CONNS'], '12')
            self.assertEqual(environment['METRICS_ENABLED'], 'true')

    def test_p0_partial_evidence_cannot_be_accepted(self):
        with self.assertRaisesRegex(ValueError, 'paper_p0_acceptance_incomplete'):
            smoke.validate_paper_p0_evidence({'checks': {'raw_samples': 200}})

    def test_p0_profile_is_opt_in_and_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            old = smoke.prepare(Path(directory) / 'old')
            p0 = smoke.prepare(Path(directory) / 'paper', paper_p0=True)
            self.assertEqual(old.values['MQTT_INGRESS_ENABLED'], 'false')
            self.assertEqual(old.values['DATABASE_MAX_CONNS'], '5')
            self.assertNotIn('supabase-storage', smoke.fixture_services(old))
            self.assertIn('supabase-storage', smoke.fixture_services(p0))
            for key, value in {'MQTT_INGRESS_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
                               'DATABASE_MAX_CONNS': '10', 'MQTT_QUEUE_CAPACITY': '256',
                               'MQTT_WORKER_COUNT': '4', 'MQTT_MAX_PAYLOAD_BYTES': '65536'}.items():
                self.assertEqual(p0.values[key], value)
            self.assertNotEqual(old.project, p0.project)
            self.assertNotEqual(old.values['JWT_SECRET'], p0.values['JWT_SECRET'])
            text = p0.override.read_text()
            self.assertNotIn('supabase-storage: !reset', text)
            self.assertEqual(text.count('container_name: !reset null'), 10)
            command = smoke.compose_command(p0, 'up')
            self.assertIn(str(smoke.PAPER_P0_TEMPLATE), command)
            self.assertEqual(command[-10:], list(smoke.fixture_services(p0)))
            self.assertNotIn('rathole', command)
            self.assertNotIn('cloudflared', command)

    def test_p0_quota_requires_effective_runtime_limit(self):
        for host in ({'NanoCpus': 2000000000, 'Memory': 4294967296},
                     {'NanoCpus': 0, 'CpuQuota': 200000, 'CpuPeriod': 100000,
                      'Memory': 4294967296}):
            self.assertEqual(smoke.paper_p0_quota(host), {'cpus': 2, 'memory_bytes': 4294967296})
        for host in ({'NanoCpus': 0, 'Memory': 4294967296},
                     {'NanoCpus': 2000000000, 'Memory': 4000000000}):
            with self.assertRaisesRegex(ValueError, 'paper_p0_quota_rejected'):
                smoke.paper_p0_quota(host)

    def test_p0_mode_exposed_and_fragment_has_no_public_ports_or_secrets(self):
        self.assertIn('--paper-p0', smoke.cli_arguments())
        text = smoke.PAPER_P0_TEMPLATE.read_text()
        self.assertIn('cpus: 2', text)
        self.assertIn('mem_limit: 4294967296', text)
        self.assertIn('storage_data:', text)
        self.assertNotIn('ports:', text)
        self.assertNotIn('password', text.lower())


class SchemaHistoryTests(unittest.TestCase):
    """Focused regression: the smoke must accept only the exact supported schema.

    The pinned terminal is ``EXPECTED_SCHEMA_TERMINAL`` (current migration 24);
    stale, missing, renamed, extra, or failed histories must all fail closed.
    """

    def test_current_schema_history_matches_pinned_terminal(self):
        expected = smoke.expected_schema_history()
        self.assertIn("24:telemetry_ledger_timestamp", expected)
        self.assertTrue(expected.endswith(smoke.EXPECTED_SCHEMA_TERMINAL))
        self.assertTrue(smoke.schema_history_verified(0, expected, expected))

    def test_historical_schema23_context_remains_explicitly_supported(self):
        expected = smoke.expected_schema_history()
        history23 = ",".join(row for row in expected.split(",")
                              if int(row.split(":", 1)[0]) <= 23)
        self.assertTrue(history23.endswith("23:sensor_control_intent"))
        self.assertTrue(smoke.schema_history_verified(
            0, history23, history23, terminal="23:sensor_control_intent"))
        # The default verifier remains pinned to current terminal 24.
        self.assertFalse(smoke.schema_history_verified(0, history23, history23))

    def test_stale_schema21_history_rejected(self):
        expected = smoke.expected_schema_history()
        stale = expected.removesuffix(",24:telemetry_ledger_timestamp")
        self.assertFalse(smoke.schema_history_verified(0, stale, expected))

    def test_missing_terminal_migration_rejected(self):
        expected = smoke.expected_schema_history()
        missing = ",".join(p for p in expected.split(",") if p != "24:telemetry_ledger_timestamp")
        self.assertFalse(smoke.schema_history_verified(0, missing, expected))

    def test_wrong_terminal_name_rejected(self):
        expected = smoke.expected_schema_history()
        wrong = expected.replace("24:telemetry_ledger_timestamp", "24:unexpected_name")
        self.assertFalse(smoke.schema_history_verified(0, wrong, expected))

    def test_extra_migration_row_rejected(self):
        expected = smoke.expected_schema_history()
        self.assertFalse(smoke.schema_history_verified(0, expected + ",24:future_migration", expected))

    def test_failed_migration_returncode_rejected(self):
        expected = smoke.expected_schema_history()
        self.assertFalse(smoke.schema_history_verified(1, expected, expected))

    def test_unpinned_future_terminal_rejected(self):
        # Guards the pin itself: a history that matches its own files but no
        # longer ends at the pinned terminal is rejected, so adding a supported
        # migration forces an explicit terminal update instead of passing.
        expected = smoke.expected_schema_history()
        future = expected.replace("24:telemetry_ledger_timestamp", "25:future_migration")
        self.assertFalse(smoke.schema_history_verified(0, future, future))


def schema_query_result(expected, query):
    # Pre-Auth startup stops at 20; both 21 and 22 run in the compat phase.
    if 'WHERE version <= 20' in query:
        return ','.join(row for row in expected.split(',') if int(row.split(':')[0]) <= 20)
    return expected


class ReceiptTests(unittest.TestCase):
    def receipt(self, kind="container"):
        return smoke.Receipt(kind, "a" * 64 if kind != "volume" else "ncs_123456789abc_store",
                             "ncs_123456789abc", "store" if kind == "volume" else "backend", "2026-10-13T00:00:00Z")

    def test_receipt_is_immutable(self):
        receipt = self.receipt()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            receipt.role = "nginx"

    def test_wrong_ownership_identity_and_timestamp_refused(self):
        receipt = self.receipt()
        good = smoke.receipt_metadata(receipt)
        for key in good:
            with self.assertRaises(ValueError):
                smoke.cleanup_commands((receipt,), lambda _: dict(good, **{key: "wrong"}))

    def test_cleanup_veto_and_verify_all_before_commands(self):
        receipt = self.receipt()
        inspect = Mock(side_effect=smoke.receipt_metadata)
        with self.assertRaises(ValueError):
            smoke.cleanup_commands((receipt,), inspect, veto=True)
        inspect.assert_not_called()
        commands = smoke.cleanup_commands((receipt,), inspect)
        self.assertEqual(commands, [["docker", "container", "rm", "--force", receipt.identity]])
        self.assertFalse(any("prune" in c or "down" in c for c in commands))
        with self.assertRaises(ValueError):
            smoke.cleanup_commands((receipt, receipt), inspect)

    def test_volume_requires_unique_name_and_created_stamp(self):
        receipt = self.receipt("volume")
        self.assertEqual(smoke.cleanup_commands((receipt,), smoke.receipt_metadata)[0],
                         ["docker", "volume", "rm", receipt.identity])
        for identity in ("postgres_data", "ncs_123456789abc_other", "*"):
            with self.assertRaises(ValueError):
                smoke.cleanup_commands((dataclasses.replace(receipt, identity=identity),),
                                       smoke.receipt_metadata)


class AssertionTests(unittest.TestCase):
    def test_role_cases_cover_admin_and_each_denied_sensor_target(self):
        cases = smoke.role_http_cases()
        self.assertEqual(len(cases), 14)
        self.assertEqual(sum(case["method"] == "GET" for case in cases), 4)
        self.assertEqual(sum(case["method"] == "PUT" for case in cases), 10)
        self.assertEqual({case["sensor_kind"] for case in cases if case["method"] == "PUT"},
                         {"new", "existing"})
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            report = smoke.role_http_summary(fixture, {
                "positive_http": {"admin_inventory_status": 200, "owner_sensor_create_status": 201,
                                  "owner_sensor_retry_status": 200, "db_guarantees_verified": True},
                "role_http": {"role_cases": {c["label"]: c["status"] for c in cases},
                              "role_snapshot_comparisons": 10}},
                {"container": 10, "network": 1, "volume": 3})
            smoke.validate_summary(report)
            self.assertFalse(report["http_accepted"])
            self.assertEqual(set(report["evidence"]), {"positive_http_statuses", "role_case_count", "role_denials_verified"})

    def test_role_executor_checks_snapshot_around_each_denied_write(self):
        self.assertTrue(callable(smoke.run_role_http_slice))
        self.assertIn("--role-http-only", smoke.cli_arguments())
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            token_map = {r: r for r in ("platform_admin", "owner", "operator", "viewer", "nonmember", "cross_gateway_owner")}
            with patch.object(smoke, "provision_role_identities", return_value=token_map), \
                 patch.object(smoke, "denial_snapshot", return_value={"sensors": [], "twin_entities": [], "twin_relationships": [], "twin_states": []}) as snapshot, \
                 patch.object(smoke, "http_request", return_value=(403, {}, {"error": {"code": "forbidden", "message": "access denied"}})):
                # Supply 404 responses for the three not-found identities.
                def request(*args, **kwargs):
                    token = kwargs["token"]
                    status = 404 if args[2] != "/v1/admin/gateways" and token in {"nonmember", "cross_gateway_owner", "platform_admin"} else 403
                    return status, {}, {"error": {"code": "not_found", "message": "resource not found"}} if status == 404 else {"error": {"code": "forbidden", "message": "access denied"}}
                with patch.object(smoke, "http_request", side_effect=request):
                    result = smoke.run_role_http_slice(fixture, object(), "http://127.0.0.1", "pg")
            self.assertEqual(len(result["role_cases"]), 14)
            self.assertEqual(snapshot.call_count, 20)

    def test_http_request_timeout_and_headers(self):
        with self.assertRaises(ValueError):
            smoke.http_request("http://127.0.0.1:1", "GET", "/test", timeout=0.01)

    def test_assert_safe_secret_leak_prevention(self):
        smoke.assert_safe_data({"ok": True, "count": 1})
        for bad in [{"token": "abc"}, {"password": "123"}, {"secret_key": "x"}, {"jwt": "ey..."}]:
            with self.assertRaisesRegex(ValueError, "secret_response_rejected"):
                smoke.assert_safe_data(bad)

    def test_positive_http_summary_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            rep = smoke.positive_summary(fixture, evidence={
                "admin_inventory_status": 200,
                "owner_sensor_create_status": 201,
                "owner_sensor_retry_status": 200,
            })
            rep["backend_image_id"] = "sha256:mock"
            rep["phase"] = "positive_http"
            self.assertEqual(rep["status"], "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED")
            self.assertFalse(rep["http_accepted"])
            smoke.validate_summary(rep)

    def test_inventory_allowlist(self):
        smoke.assert_response("inventory", 200, {"items": [], "next_cursor": None})
        smoke.assert_response("inventory", 200, {"items": [{"gateway_id": "fixture", "name": "Fixture",
                              "description": None, "created_at": None}], "next_cursor": None})
        with self.assertRaises(ValueError):
            smoke.assert_response("inventory", 200, {"items": [{"password_hash": "secret"}], "next_cursor": None})

    def test_owner_sensor_supported_fields_only(self):
        body = {"gateway_id": "fixture", "sensor_id": "sample", "name": "Sample",
                "unit": None, "entity_id": "urn:ngsi-ld:Sensor:fixture:sample", "created_at": None}
        smoke.assert_response("sensor_create", 201, body)
        smoke.assert_response("sensor_retry", 200, body)
        with self.assertRaises(ValueError):
            smoke.assert_response("sensor_create", 201, dict(body, desired_state={}))

    def test_denials_and_login_are_not_saved_as_artifacts(self):
        smoke.assert_response("forbidden", 403, {"error": {"code": "forbidden", "message": "access denied"}})
        smoke.assert_response("not_found", 404, {"error": {"code": "not_found", "message": "resource not found"}})
        smoke.assert_response("unauthorized", 401, {})
        with self.assertRaises(ValueError):
            smoke.assert_response("forbidden", 200, {})
        self.assertIsNone(smoke.assert_login(200, {"access_token": "private", "token_type": "bearer"}))
        with self.assertRaises(ValueError):
            smoke.assert_login(200, {"access_token": ""})

    def test_pending_routes_and_no_admin_owner_bypass(self):
        nonmember = [c for c in smoke.ACCEPTANCE if c[3] == "nonmember" and c[1] == "PUT"]
        self.assertEqual(nonmember[0][4], 404)
        admin = [c for c in smoke.ACCEPTANCE if c[3] == "admin_without_membership"]
        self.assertEqual(admin[0][4], 404)
        self.assertEqual(set(smoke.SENSOR_PAYLOAD), {"name", "unit"})

    def test_stage5_intent_flag_defaults_off_in_owned_fixture(self):
        self.assertEqual(smoke.DEFAULTS["STAGE5_ENABLED"], "false")
        self.assertEqual(smoke.DEFAULTS["STAGE5_REQUEST_TIMEOUT"], "5s")
        self.assertEqual(smoke.DEFAULTS["STAGE5_COMMAND_TTL"], "60s")
        self.assertEqual(smoke.DEFAULTS["STAGE5_MAX_PENDING_OUTBOX"], "10000")
        self.assertEqual(smoke.DEFAULTS["STAGE5_MAX_RECEIPTS"], "100000")

    def test_denial_snapshot_preserves_full_rows_not_just_counts(self):
        before = {"sensors": [{"gateway_id": "gw", "sensor_id": "s", "name": "old", "unit": "C"}],
                  "twin_entities": [{"id": "id", "entity_id": "urn:ngsi-ld:Sensor:gw:s", "attributes": {"unit": "C"}}],
                  "twin_relationships": [{"source": "g", "type": "hasSensor", "target": "id"}],
                  "twin_states": [{"entity_id": "id", "reported_state": {"unit": "C"}, "desired_state": {}}]}
        self.assertTrue(smoke.assert_unchanged(before, before))
        changed = json.loads(json.dumps(before))
        changed["sensors"][0]["name"] = "tampered"
        with self.assertRaisesRegex(ValueError, "denial_mutated_database"):
            smoke.assert_unchanged(before, changed)

    def test_full_rbac_report_cannot_accept_partial_cases(self):
        with self.assertRaisesRegex(ValueError, "acceptance_incomplete"):
            smoke.acceptance_summary(None, {}, {"container": 10, "network": 1, "volume": 3})

    def test_jwt_denial_cases_cover_all_conditions_and_targets(self):
        cases = smoke.jwt_http_cases()
        self.assertEqual(len(cases), 9)
        self.assertEqual(sum(case["method"] == "GET" for case in cases), 3)
        self.assertEqual(sum(case["method"] == "PUT" for case in cases), 6)
        self.assertEqual({c["condition"] for c in cases}, {"missing", "invalid", "expired"})
        self.assertEqual({c["sensor_kind"] for c in cases if c["method"] == "PUT"}, {"new", "existing"})
        for c in cases:
            self.assertEqual(c["status"], 401)
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            tokens = smoke.generate_jwt_denial_tokens(fixture)
            self.assertIn("missing", tokens)
            self.assertIn("invalid", tokens)
            self.assertIn("expired", tokens)
            self.assertEqual(tokens["missing"], "")
            # Expired token claims check
            parts = tokens["expired"].split(".")
            self.assertEqual(len(parts), 3)
            padding = b"=" * (-len(parts[1]) % 4)
            claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
            self.assertEqual(claims["role"], "authenticated")
            self.assertEqual(claims["aud"], "authenticated")
            self.assertEqual(claims["iss"], fixture.values["GOTRUE_JWT_ISSUER"])
            self.assertEqual(claims["sub"], "00000000-0000-4000-8000-000000000001")
            header = json.loads(base64.urlsafe_b64decode(parts[0] + "=" * (-len(parts[0]) % 4)))
            self.assertEqual(header["alg"], "HS256")
            self.assertTrue(claims["exp"] < time.time() - 3600)
            # Invalid token is malformed / wrong signature
            self.assertTrue(len(tokens["invalid"].split(".")) in (1, 3))

    def test_jwt_denial_executor_verifies_snapshots_and_rejections(self):
        self.assertTrue(callable(smoke.run_jwt_denial_slice))
        self.assertIn("--full-http", smoke.cli_arguments())
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            empty_snap = {"sensors": [], "twin_entities": [], "twin_relationships": [], "twin_states": []}
            with patch.object(smoke, "denial_snapshot", return_value=empty_snap) as snapshot, \
                 patch.object(smoke, "http_request", return_value=(401, {}, {"error": {"code": "unauthorized", "message": "authentication required"}})):
                result = smoke.run_jwt_denial_slice(fixture, object(), "http://127.0.0.1", "pg", "gw_test", "sensor_test")
            self.assertEqual(len(result["jwt_cases"]), 9)
            self.assertEqual(result["jwt_case_count"], 9)
            self.assertTrue(result["jwt_denials_verified"])
            self.assertEqual(snapshot.call_count, 12)  # 6 PUTs * 2 (before/after)
            self.assertEqual(result["jwt_snapshot_comparisons"], 6)

    def test_full_http_acceptance_summary_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            role_cases = {c["label"]: c["status"] for c in smoke.role_http_cases()}
            jwt_cases = {c["label"]: c["status"] for c in smoke.jwt_http_cases()}
            evidence = {
                "positive_http": {
                    "admin_inventory_status": 200,
                    "owner_sensor_create_status": 201,
                    "owner_sensor_retry_status": 200,
                    "db_guarantees_verified": True,
                },
                "role_http": {
                    "role_cases": role_cases,
                    "role_snapshot_comparisons": 10,
                },
                "jwt_http": {
                    "jwt_cases": jwt_cases,
                    "jwt_snapshot_comparisons": 6,
                },
            }
            cleanup_counts = {"container": 10, "network": 1, "volume": 3}
            report = smoke.full_http_summary(fixture, evidence, cleanup_counts)
            report["backend_image_id"] = "sha256:mock"
            report["phase"] = "full_http"
            self.assertEqual(report["status"], "RBAC_JWT_HTTP_ACCEPTED")
            self.assertTrue(report["http_accepted"])
            self.assertTrue(report["exact_cleanup_verified"])
            smoke.validate_summary(report)

            # Incomplete cases rejected
            incomplete_ev = dict(evidence)
            incomplete_ev["jwt_http"] = {"jwt_cases": {k: v for k, v in list(jwt_cases.items())[:-1]}, "jwt_snapshot_comparisons": 5}
            with self.assertRaisesRegex(ValueError, "jwt_acceptance_incomplete"):
                smoke.full_http_summary(fixture, incomplete_ev, cleanup_counts)


class ModelTests(unittest.TestCase):
    def test_envoy_probe_requires_model_and_no_foreign_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            with patch.object(smoke, "dry_effective_config", side_effect=ValueError("blocked")):
                with self.assertRaisesRegex(ValueError, "blocked"):
                    smoke.probe_envoy_toolchain(fixture)

    def test_build_requires_model_before_real_dockerfile(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            runner = Mock()
            with patch.object(smoke, "dry_effective_config", side_effect=ValueError("blocked")):
                with self.assertRaisesRegex(ValueError, "blocked"):
                    smoke.build_native_image(fixture, runner=runner)
            runner.assert_not_called()

    def test_model_rejects_extra_services_before_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            with self.assertRaisesRegex(ValueError, "effective_services_rejected"):
                smoke.validate_effective_model(fixture, {"services": {"rathole": {}}})

    def test_root_topology_removed_explicitly_not_hidden_by_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            text = fixture.override.read_text()
            for name in ("rathole", "supabase-storage", "cloudflared"):
                self.assertIn("  " + name + ": !reset null", text)
            self.assertIn("\nvolumes: !override\n", text)
            self.assertIn("\nnetworks: !override\n", text)
            self.assertIn("  mosquitto:\n", text)
            self.assertIn("    volumes: !override\n", text)

    def test_dry_config_uses_private_capture_and_finite_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(side_effect=[type("Result", (), {"returncode": 0, "stdout": b"2.24.4\n"})(),
                                      type("Result", (), {"returncode": 0, "stdout": b'{"services":{}}'})()])
            with self.assertRaisesRegex(ValueError, "effective_services_rejected"):
                smoke.dry_effective_config(fixture, runner=runner)
            self.assertEqual(runner.call_count, 2)
            for call in runner.call_args_list:
                self.assertGreater(call.kwargs["timeout"], 0)
                self.assertEqual(call.kwargs["env"], smoke.process_env(fixture))
            self.assertEqual((fixture.private / "effective.json").stat().st_mode & 0o777, 0o600)

    def test_rendered_compose_backend_environment_carries_nonsecret_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            fixture = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(fixture)
            backend_env = model["services"]["backend"].get("environment", {})
            self.assertEqual(backend_env.get("AUTH_ADMIN_BASE_URL"), "http://supabase-envoy:8000")
            self.assertEqual(backend_env.get("AUTH_ADMIN_TIMEOUT"), "10s")
            self.assertEqual(backend_env.get("AUTH_ADMIN_MAX_BODY_BYTES"), "16384")
            self.assertEqual(backend_env.get("USER_LIST_MAX_USERS"), "1000")
            self.assertEqual(backend_env.get("USER_LIST_MAX_MEMBERSHIPS"), "10000")
            self.assertEqual(backend_env.get("USER_LIST_MAX_RESPONSE_BYTES"), "2097152")


class AllocationTests(unittest.TestCase):
    def test_readback_mount_metadata_rejects_anonymous_image_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'fixture')
            obj = {'HostConfig': {'Tmpfs': {p: 'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883' for p in ('/mosquitto/data', '/mosquitto/log')}},
                   'Mounts': [{'Type': 'volume', 'Name': f.project + '_store', 'Destination': '/store', 'RW': False},
                              {'Type': 'bind', 'Source': str(f.private / 'certs'), 'Destination': '/certs', 'RW': False}]}
            volume = {'name': f.project + '_store'}
            self.assertTrue(smoke.verify_readback_mounts(f, volume, obj))
            obj['Mounts'].append({'Type': 'volume', 'Name': 'unowned', 'Destination': '/mosquitto/data', 'RW': True})
            with self.assertRaisesRegex(ValueError, 'mounts_rejected'):
                smoke.verify_readback_mounts(f, volume, obj)

    def test_readback_uses_real_open_not_busybox_mode_only_access_test(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'fixture')
            registry = Mock()
            item = {'name': f.project + '-reader', 'image': 'sha256:' + 'a' * 64}
            registry.intent.return_value = item
            registry.resolve.side_effect = [None, {'Id': 'b' * 64}, {'State': {'Running': False, 'ExitCode': 1}}]
            registry.command.side_effect = [subprocess.CompletedProcess([], 0, ('b' * 64).encode(), b''), subprocess.CompletedProcess([], 1, b'', b'phase=server_key')]
            with patch.object(smoke, 'verify_readback_mounts'), self.assertRaises(ValueError):
                registry.resolve.side_effect = [None, {'Id': 'b' * 64, 'Mounts': [], 'HostConfig': {'Tmpfs': {}}}, {'State': {'Running': False, 'ExitCode': 1}}]
                smoke.allocation_readback(f, registry, {'image': item['image'], 'name': f.project + '_store'})
            script = registry.command.call_args_list[0].args[0][-1]
            self.assertIn('cat /certs/server.key >/dev/null', script)
            self.assertNotIn('test -r', script)

    def test_helper_disables_implicit_image_volumes_and_retains_failure_output(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = Mock()
            item = {"name": f.project + "-reader", "image": "sha256:" + "a" * 64}
            registry.intent.return_value = item
            registry.resolve.side_effect = [None, {"Id": "b" * 64}, {"State": {"Running": False, "ExitCode": 1}}]
            registry.command.side_effect = [subprocess.CompletedProcess([], 0, ("b" * 64).encode(), b""), subprocess.CompletedProcess([], 1, b"private-output", b"private-error")]
            registry.resolve.side_effect = [None, {"Id": "b" * 64, 'Mounts': [], 'HostConfig': {'Tmpfs': {}}}, {"State": {"Running": False, "ExitCode": 1}}]
            with patch.object(smoke, 'verify_readback_mounts'), self.assertRaisesRegex(ValueError, "readback_failed"):
                smoke.allocation_readback(f, registry, {"image": item["image"], "name": f.project + "_store"})
            command = registry.command.call_args_list[0].args[0]
            self.assertIn("/mosquitto/data:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883", command)
            self.assertIn("/mosquitto/log:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883", command)
            self.assertEqual((f.private / "readback-output.json").stat().st_mode & 0o777, 0o600)
            self.assertFalse(list(f.artifacts.iterdir()))

    def test_cancellation_reconciles_before_cleanup_and_restores_handlers(self):
        import signal
        lifecycle = smoke.bootstrap_lifecycle()
        before = signal.getsignal(signal.SIGTERM)
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            events = []
            def allocate(fixture, registry, runner):
                item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
                registry.allocating(item)
                raise lifecycle.RunnerInterrupted(signal.SIGTERM)
            with patch.object(smoke, "bootstrap_lifecycle", return_value=lifecycle), patch.object(smoke, "generate_fixture_tls"), patch.object(smoke, "allocate_bootstrap_store", side_effect=allocate), patch.object(lifecycle, "reconcile_child_receipts", side_effect=lambda *a: events.append("reconcile")), patch.object(smoke.ResourceRegistry, "remove", side_effect=lambda *a: events.append("remove")):
                with self.assertRaises(lifecycle.RunnerInterrupted):
                    smoke.allocation_roundtrip(f)
            self.assertEqual(events, ["reconcile", "remove"])
            self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def test_unjoined_child_vetoes_cleanup_and_retains_inputs(self):
        lifecycle = smoke.bootstrap_lifecycle()
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            smoke.private_write(f.private / "bootstrap.json", "protected")
            with patch.object(smoke, "bootstrap_lifecycle", return_value=lifecycle), patch.object(smoke, "generate_fixture_tls"), patch.object(smoke, "allocate_bootstrap_store", side_effect=lifecycle.IncompleteCleanup("unjoined")), patch.object(smoke.ResourceRegistry, "remove") as remove:
                with self.assertRaisesRegex(ValueError, "child_unresolved"):
                    smoke.allocation_roundtrip(f)
                remove.assert_not_called()
            self.assertTrue((f.private / "bootstrap.json").exists())

    def test_allocation_attempt_is_durable(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = smoke.ResourceRegistry(f, Mock())
            item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
            registry.allocating(item)
            self.assertTrue(json.loads(item["path"].read_text())["allocation_attempted"])

    def test_actual_tls_fields_and_protected_keys(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            smoke.generate_fixture_tls(f)
            certs = f.private / "certs"
            text = subprocess.check_output(["openssl", "x509", "-in", str(certs / "server.crt"), "-noout", "-text"])
            self.assertIn(b"DNS:mosquitto", text)
            self.assertIn(b"DNS:localhost", text)
            self.assertEqual((f.private / "ca.key").stat().st_mode & 0o777, 0o600)
            self.assertEqual((certs / "server.key").stat().st_mode & 0o007, 0)
            self.assertFalse((certs / "ca.key").exists())
            self.assertNotEqual((f.private / "ca.key").read_bytes(), (certs / "server.key").read_bytes())
            subprocess.check_call(["openssl", "verify", "-CAfile", str(certs / "ca.crt"), str(certs / "server.crt")], stdout=subprocess.DEVNULL)
            acl = subprocess.check_output(["getfacl", "-cp", str(certs / "server.key")])
            self.assertIn(b"user:1883:r--", acl)

    def test_registry_intent_is_durable_before_reply_and_unknown_is_not_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(side_effect=OSError("lost reply"))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", f.project + "-reader", "sha256:" + "a" * 64, {"test.run": f.project})
            self.assertEqual(item["path"].stat().st_mode & 0o777, 0o600)
            with self.assertRaisesRegex(ValueError, "resource_inspect_unresolved"):
                registry.resolve(item)
            self.assertTrue(item["path"].exists())

    def test_volume_replacement_refused_before_delete(self):
        import json
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = {"Name": f.project + "_store", "CreatedAt": "2026-10-13T01:00:00Z", "Labels": {"test.run": f.project}, "Driver": "local", "Options": None}
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("volume", obj["Name"], "sha256:" + "a" * 64, obj["Labels"])
            registry.resolve(item)
            obj["CreatedAt"] = "2026-10-13T02:00:00Z"
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.remove(item)
            self.assertFalse(any("rm" in call.args[0] for call in runner.call_args_list))

    def test_absence_requires_explicit_daemon_not_found(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(return_value=subprocess.CompletedProcess([], 1, b"", b"daemon unavailable"))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
            with self.assertRaisesRegex(ValueError, "resource_inspect_unresolved"):
                registry.resolve(item)
            runner.return_value.stderr = b"Error: No such volume: fixture"
            self.assertIsNone(registry.resolve(item))

    def test_helper_immutable_identity_and_creation_receipt(self):
        import json
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = {"Id": "b" * 64, "Name": "/" + f.project + "-reader", "Created": "2026-10-13T01:00:00Z", "Image": "sha256:" + "a" * 64, "Config": {"Labels": {"test.run": f.project}}}
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            obj["Id"] = "c" * 64
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.remove(item)

    def _create_helper_container_obj(self, f):
        return {
            "Id": "b" * 64,
            "Name": "/" + f.project + "-reader",
            "Created": "2026-10-13T01:00:00Z",
            "Image": "sha256:" + "a" * 64,
            "Config": {"Labels": {"test.run": f.project}},
            "HostConfig": {
                "Tmpfs": {
                    "/mosquitto/data": "rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883",
                    "/mosquitto/log": "rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883",
                }
            },
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": str(f.private / "certs"),
                    "Destination": "/certs",
                    "Mode": "ro",
                    "RW": False,
                    "Propagation": "rprivate",
                },
                {
                    "Type": "volume",
                    "Name": "test_volume",
                    "Source": "/var/lib/docker/volumes/test_volume/_data",
                    "Destination": "/store",
                    "Driver": "local",
                    "Mode": "ro",
                    "RW": False,
                    "Propagation": "",
                },
            ],
        }

    def test_helper_mount_canonicalization_and_reorder_acceptance(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock()
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b"")
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])

            # Initial resolve
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Reordered inspect Mounts: volume first, then bind
            reordered_obj = copy.deepcopy(obj)
            reordered_obj["Mounts"] = list(reversed(obj["Mounts"]))
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([reordered_obj]).encode(), b"")

            resolved = registry.resolve(item)
            self.assertEqual(resolved["Id"], obj["Id"])

            # Resolve / remove symmetry after reorder
            removed_state = {"removed": False}
            def fake_runner(args, **kwargs):
                if "rm" in args:
                    removed_state["removed"] = True
                    return subprocess.CompletedProcess(args, 0, b"", b"")
                if "inspect" in args:
                    if removed_state["removed"]:
                        return subprocess.CompletedProcess(args, 1, b"", b"Error: No such container")
                    return subprocess.CompletedProcess(args, 0, json.dumps([reordered_obj]).encode(), b"")
                return subprocess.CompletedProcess(args, 0, json.dumps([reordered_obj]).encode(), b"")

            runner.side_effect = fake_runner
            registry.remove(item)
            self.assertTrue(item.get("removed", False))

    def test_helper_mount_attribute_mutations_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # 1. Volume Name mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Name"] = "mutated_volume_name"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 2. Volume Source path mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Source"] = "/var/lib/docker/volumes/other_volume/_data"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 3. Bind Source path mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Source"] = str(f.private / "other_certs")
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 4. Driver mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Driver"] = "nfs"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 5. Mode mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Mode"] = "rw"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 6. Propagation mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Propagation"] = "rshared"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 7. Type mutation (volume -> bind)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Type"] = "bind"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 8. RW flip (False -> True)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["RW"] = True
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

    def test_helper_mount_destinations_and_counts_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Destination mutated (/store -> /other_store)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Destination"] = "/other_store"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_destinations_mismatch"):
                registry.resolve(item)

            # Extra mount added
            mut_obj = copy.deepcopy(obj)
            extra = copy.deepcopy(obj["Mounts"][0])
            extra["Destination"] = "/extra"
            mut_obj["Mounts"].append(extra)
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_count_mismatch"):
                registry.resolve(item)

            # Missing mount
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = mut_obj["Mounts"][:1]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_count_mismatch"):
                registry.resolve(item)

    def test_helper_mount_malformed_and_duplicate_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Duplicate destination
            mut_obj = copy.deepcopy(obj)
            dup = copy.deepcopy(obj["Mounts"][0])
            mut_obj["Mounts"] = [obj["Mounts"][0], dup]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Wrong-type RW: string "false", int 0, int 1, None
            for bad_rw in ("false", 0, 1, None, []):
                mut_obj = copy.deepcopy(obj)
                mut_obj["Mounts"][0]["RW"] = bad_rw
                runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
                with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                    registry.resolve(item)

            # Mounts not a list
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = {"Destination": "/certs"}
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Mount element not a dict
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = ["/certs", "/store"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Destination missing or not a string
            for bad_dest in (123, None, "", ["/certs"]):
                mut_obj = copy.deepcopy(obj)
                mut_obj["Mounts"][0]["Destination"] = bad_dest
                runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
                with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                    registry.resolve(item)

            # Tmpfs not a dict
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"] = ["/mosquitto/data"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Tmpfs options not a string
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"] = 123
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

    def test_helper_tmpfs_options_and_entries_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Tmpfs options changed
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"] = "rw,size=2m"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

            # Tmpfs entry added
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/extra"] = "rw,size=1m"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

            # Tmpfs entry removed
            mut_obj = copy.deepcopy(obj)
            del mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

    def test_helper_identity_protection_retained(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Container Id mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Id"] = "f" * 64
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.resolve(item)

            # Container Created mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Created"] = "2026-10-14T12:00:00Z"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.resolve(item)

            # Container Labels mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Config"]["Labels"] = {"test.run": "wrong_project"}
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_ownership_refused"):
                registry.resolve(item)

            # Container Image mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Image"] = "sha256:" + "9" * 64
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_ownership_refused"):
                registry.resolve(item)

    def test_helper_receipt_json_roundtrip_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            receipt = smoke.canonicalize_helper_mounts(obj)

            # Must contain only JSON-native lists, dicts, primitives (no tuples)
            self.assertIsInstance(receipt["mounts"], list)
            self.assertIsInstance(receipt["tmpfs"], dict)
            for m in receipt["mounts"]:
                self.assertIsInstance(m, dict)
                self.assertIsInstance(m["RW"], bool)
                self.assertIsInstance(m["Destination"], str)
            for k, v in receipt["tmpfs"].items():
                self.assertIsInstance(k, str)
                self.assertIsInstance(v, str)

            # JSON roundtrip stability
            dumped = json.dumps(receipt)
            loaded = json.loads(dumped)
            self.assertEqual(loaded, receipt)

            # Re-canonicalization produces identical receipt
            re_canonical = smoke.canonicalize_helper_mounts(loaded)
            self.assertEqual(re_canonical, receipt)

    def test_helper_mount_mismatch_codes_survive_safe_report_and_diagnostic(self):
        new_codes = [
            "helper_mount_count_mismatch",
            "helper_mount_destinations_mismatch",
            "helper_mount_attributes_mismatch",
            "helper_tmpfs_mismatch",
            "helper_mount_receipt_invalid",
        ]
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            for code in new_codes:
                self.assertIn(code, smoke.ALLOWED_FAILURE_ERROR_CODES)
                rep = smoke.record_failure_report(f, phase="bootstrap", exc=ValueError(code), cleanup_verified=True)
                self.assertEqual(rep["error_code"], code)
                smoke.validate_failure_report(rep)

                diag = smoke.FailureDiagnostic(rep, cause=ValueError(code))
                self.assertEqual(str(diag), code)

            # Verify secrets do not leak into error_code
            secret = f.values["MQTT_PASSWORD"]
            leak_rep = smoke.record_failure_report(f, phase="bootstrap", exc=ValueError(f"leak_{secret}"), cleanup_verified=True)
            self.assertEqual(leak_rep["error_code"], "unclassified")
            self.assertNotIn(secret, json.dumps(leak_rep))


    def test_probe_startup_envoy_and_nginx_contracts(self):
        registry = Mock()
        running_obj = {"Id": "c" * 64, "State": {"Status": "running", "Running": True}}
        dead_obj = {"Id": "c" * 64, "State": {"Status": "dead", "Running": False}}
        stopped_obj = {"Id": "c" * 64, "State": {"Status": "created", "Running": False}}

        # Container exited or dead raises health_failed
        with self.assertRaisesRegex(ValueError, "health_failed"):
            smoke.probe_startup(registry, "supabase-envoy", dead_obj)

        # Container not running returns False without executing commands
        self.assertFalse(smoke.probe_startup(registry, "supabase-envoy", stopped_obj))
        registry.command.assert_not_called()

        # Envoy probe command: must use /bin/bash /dev/tcp, bounded timeout, no curl/wget
        registry.command.return_value = type("Res", (), {"returncode": 0})()
        self.assertTrue(smoke.probe_startup(registry, "supabase-envoy", running_obj))
        envoy_cmd = registry.command.call_args_list[0].args[0]
        self.assertEqual(envoy_cmd[:3], ["exec", "c" * 64, "/bin/bash"])
        script = envoy_cmd[4]
        self.assertIn("/dev/tcp/127.0.0.1/9901", script)
        self.assertIn("GET /ready HTTP/1.1", script)
        self.assertIn("HTTP/1.1 200 OK", script)
        self.assertIn("-t 2", script)  # bounded read timeout
        self.assertNotIn("curl", script)
        self.assertNotIn("wget", script)
        self.assertNotIn("token", script.lower())
        self.assertNotIn("secret", script.lower())

        # Envoy probe failure: non-zero return code returns False (timeout, connection failure, wrong status)
        registry.command.return_value = type("Res", (), {"returncode": 1})()
        self.assertFalse(smoke.probe_startup(registry, "supabase-envoy", running_obj))

        # Nginx probe command: checks /readyz endpoint, returncode 0 -> True, 1 -> False
        registry.command.reset_mock()
        registry.command.return_value = type("Res", (), {"returncode": 0})()
        self.assertTrue(smoke.probe_startup(registry, "nginx", running_obj))
        nginx_cmd = registry.command.call_args_list[0].args[0]
        self.assertEqual(nginx_cmd[:3], ["exec", "c" * 64, "wget"])
        self.assertIn("http://127.0.0.1/readyz", nginx_cmd)
        self.assertIn("-q", nginx_cmd)
        self.assertIn("-O", nginx_cmd)
        self.assertIn("/dev/null", nginx_cmd)

        # Nginx probe failure: non-zero return code returns False
        registry.command.return_value = type("Res", (), {"returncode": 1})()
        self.assertFalse(smoke.probe_startup(registry, "nginx", running_obj))

    def test_startup_phases_envoy_readiness_failure_stops_before_nginx(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            model = {"networks": {"iot_net": {"name": f.project + "_iot_net", "labels": {}}},
                     "volumes": {},
                     "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}}
            image_inspect = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()

            registry = smoke.ResourceRegistry(f, Mock(side_effect=lambda args: type("Res", (), {
                "returncode": 0, "stdout": image_inspect, "stderr": b""})()))

            started_services = []

            def fake_run(cmd, **kwargs):
                if "up" in cmd:
                    srv = cmd[-1]
                    started_services.append(srv)
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            # Mock probe_startup to fail when name == "supabase-envoy"
            def fake_probe(reg, name, obj):
                if name == "supabase-envoy":
                    return False
                return True

            # Mock startup_state to always return True for jobs / healthy services
            fake_state = lambda name, st: True

            with patch.object(smoke, "dry_effective_config", return_value=model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store"), \
                 patch.object(smoke, "allocation_readback"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", side_effect=fake_state), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch("subprocess.run", side_effect=fake_run):

                # Give registry.resolve None initially for collision check, then running container
                running_state = {"Status": "running", "Running": True, "ExitCode": 0}
                running_obj = {"Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"}}
                resolve_calls = 0
                def fake_resolve(item):
                    nonlocal resolve_calls
                    resolve_calls += 1
                    # During initial collision checks, resolve returns None
                    if resolve_calls <= len(smoke.SERVICES) + 1:
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": image_inspect, "stderr": b""})()

                with patch.object(registry, "resolve", side_effect=fake_resolve), \
                     patch.object(registry, "remove"), \
                     patch.object(registry, "command", side_effect=fake_cmd):
                    with self.assertRaisesRegex(ValueError, "phase_health_timeout"):
                        smoke.startup_only(f, registry=registry)

            # supabase-envoy was started, but nginx was NEVER started because envoy failed readiness
            self.assertIn("supabase-envoy", started_services)
            self.assertNotIn("nginx", started_services)

    def test_run_positive_http_slice_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = Mock()

            # Mock psql responses
            def fake_psql(cmd):
                sql = cmd[-1]
                if "SELECT count(*)" in sql:
                    return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            registry.command.side_effect = fake_psql

            # Mock HTTP calls
            def fake_http(base_url, method, path, *, token="", api_key="", headers=None, body=None, timeout=10.0):
                if path == "/auth/v1/admin/users":
                    return 200, {}, {"id": "11111111-2222-3333-4444-555555555555"}
                if path.startswith("/auth/v1/token"):
                    # Return valid standard JWT format with authenticated role
                    claims = base64.urlsafe_b64encode(json.dumps({"role": "authenticated", "aud": "authenticated"}).encode()).decode().rstrip("=")
                    tok = f"eyJhbGciOiJIUzI1NiJ9.{claims}.sig"
                    return 200, {}, {"access_token": tok, "token_type": "bearer"}
                if path == "/v1/admin/gateways":
                    # Extract gateway_id from query/body or provide matching item
                    gw_id = [arg for arg in fake_psql.last_sql if "INSERT INTO public.gateways" in arg] if hasattr(fake_psql, "last_sql") else []
                    return 200, {}, {"items": [{"gateway_id": "test_gw", "name": "test_name", "description": None, "created_at": None}], "next_cursor": None}
                if "/sensors/" in path:
                    status = 201 if getattr(fake_http, "sensor_called", False) is False else 200
                    fake_http.sensor_called = True
                    parts = path.split("/")
                    gw, sensor = parts[3], parts[5]
                    return status, {}, {
                        "gateway_id": gw, "sensor_id": sensor, "name": body["name"],
                        "unit": body["unit"], "entity_id": f"urn:ngsi-ld:Sensor:{gw}:{sensor}",
                        "created_at": None
                    }
                return 404, {}, {}

            def intercept_psql(cmd):
                sql = cmd[-1]
                if "INSERT INTO public.gateways" in sql:
                    # capture gw_id and gw_name
                    m = re.search(r"INSERT INTO public\.gateways \(gateway_id, name, description\) VALUES \('([^']+)', '([^']+)'", sql)
                    if m:
                        fake_http.seeded_gw_id = m.group(1)
                        fake_http.seeded_gw_name = m.group(2)
                if "SELECT count(*)" in sql:
                    return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            registry.command.side_effect = intercept_psql

            def updated_fake_http(base_url, method, path, *, token="", api_key="", headers=None, body=None, timeout=10.0):
                if path == "/auth/v1/admin/users":
                    return 200, {}, {"id": "11111111-2222-3333-4444-555555555555"}
                if path.startswith("/auth/v1/token"):
                    claims = base64.urlsafe_b64encode(json.dumps({"role": "authenticated", "aud": "authenticated"}).encode()).decode().rstrip("=")
                    tok = f"eyJhbGciOiJIUzI1NiJ9.{claims}.sig"
                    return 200, {}, {"access_token": tok, "token_type": "bearer"}
                if path == "/v1/admin/gateways":
                    return 200, {}, {"items": [{"gateway_id": getattr(fake_http, "seeded_gw_id", "gw_1"), "name": getattr(fake_http, "seeded_gw_name", "Positive Smoke"), "description": None, "created_at": None}], "next_cursor": None}
                if "/sensors/" in path:
                    status = 201 if getattr(fake_http, "sensor_called", False) is False else 200
                    fake_http.sensor_called = True
                    parts = path.split("/")
                    gw, sensor = parts[3], parts[5]
                    return status, {}, {
                        "gateway_id": gw, "sensor_id": sensor, "name": body["name"],
                        "unit": body["unit"], "entity_id": f"urn:ngsi-ld:Sensor:{gw}:{sensor}",
                        "created_at": None
                    }
                return 404, {}, {}

            with patch.object(smoke, "http_request", side_effect=updated_fake_http):
                ev = smoke.run_positive_http_slice(f, registry, "http://127.0.0.1:8080", "cid_postgres")
                self.assertEqual(ev["admin_inventory_status"], 200)
                self.assertEqual(ev["owner_sensor_create_status"], 201)
                self.assertEqual(ev["owner_sensor_retry_status"], 200)
                self.assertTrue(ev["db_guarantees_verified"])

    def test_startup_with_positive_http_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            model = {"networks": {"iot_net": {"name": f.project + "_iot_net", "labels": {}}},
                     "volumes": {},
                     "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}}
            image_inspect = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()

            registry = smoke.ResourceRegistry(f, Mock(side_effect=lambda args: type("Res", (), {
                "returncode": 0, "stdout": image_inspect, "stderr": b""})()))

            def fake_state(name, st):
                return True

            def fake_probe(reg, name, obj):
                return True

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": image_inspect, "stderr": b""})()

            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            resolve_calls = 0
            def fake_resolve(item):
                nonlocal resolve_calls
                resolve_calls += 1
                if resolve_calls <= len(smoke.SERVICES) + 1:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            mock_lifecycle = smoke.bootstrap_lifecycle()
            with patch.object(smoke, "dry_effective_config", return_value=model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store"), \
                 patch.object(smoke, "allocation_readback"), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", side_effect=fake_state), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch.object(smoke, "run_positive_http_slice", return_value={"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}), \
                 patch.object(registry, "resolve", side_effect=fake_resolve), \
                 patch.object(registry, "remove"), \
                 patch.object(registry, "command", side_effect=fake_cmd), \
                 patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()):

                rep = smoke.startup_only(f, registry=registry, positive_http=True)
                self.assertEqual(rep["status"], "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED")
                self.assertFalse(rep["http_accepted"])
                self.assertTrue(rep["exact_cleanup_verified"])
                self.assertEqual(rep["positive_verifications"]["admin_inventory"], True)
                smoke.validate_summary(rep)

    def test_safe_failure_report_schema_and_allowlists(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            for code in ("paper_p5b_cold_replay_reconciliation_failed",
                         "paper_p5b_functional_smoke_failed",
                         "p8_prepare_report_artifact_missing",
                         "p8_prepare_report_validation_missing"):
                self.assertIn(code, smoke.ALLOWED_FAILURE_ERROR_CODES)
                self.assertEqual(smoke.sanitize_failure_code(ValueError(code)), code)
                report = smoke.record_failure_report(f, phase="paper_p5a",
                    exc=ValueError(code), cleanup_verified=True)
                self.assertEqual(report["error_code"], code)
            valid_report = {
                "schema_version": 1,
                "status": "STARTUP_FAILED",
                "phase": "preflight",
                "error_type": "ValueError",
                "error_code": "local_image_unavailable",
                "http_accepted": False,
                "cleanup_verified": False,
                "primary_phase": None,
                "primary_error_code": None,
                "cleanup_step": None,
                "cleanup_error_code": None,
                "resource_kind": None,
            }
            smoke.validate_failure_report(valid_report)
            smoke.record_failure_report(f, phase="preflight", exc=ValueError("local_image_unavailable"), cleanup_verified=False)
            failure_path = f.artifacts / "smoke-failure.json"
            self.assertTrue(failure_path.exists())
            loaded = json.loads(failure_path.read_text())
            smoke.validate_failure_report(loaded)
            self.assertEqual(loaded["error_code"], "local_image_unavailable")
            self.assertFalse(loaded["http_accepted"])
            self.assertFalse(loaded["cleanup_verified"])

            # Null semantics: when phase is not cleanup, all 5 new fields must be None
            for key in ("primary_phase", "primary_error_code", "cleanup_step", "cleanup_error_code", "resource_kind"):
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, **{key: "not_none"}))

            # When phase is cleanup:
            cleanup_report = {
                "schema_version": 1,
                "status": "STARTUP_FAILED",
                "phase": "cleanup",
                "error_type": "ValueError",
                "error_code": "startup_cleanup_unresolved_evidence_retained",
                "http_accepted": False,
                "cleanup_verified": False,
                "primary_phase": "bootstrap",
                "primary_error_code": "bootstrap_contract_rejected",
                "cleanup_step": "remove_resource",
                "cleanup_error_code": "resource_cleanup_unresolved",
                "resource_kind": "container",
            }
            smoke.validate_failure_report(cleanup_report)

            # cleanup_verified must be False for cleanup phase
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_verified=True))

            # cleanup_step must be in ALLOWED_CLEANUP_STEPS
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="unauthorized_step"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step=None))

            # cleanup_error_code must be in ALLOWED_FAILURE_ERROR_CODES
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_error_code="evil_code"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_error_code=None))

            # resource_kind must be in ALLOWED_RESOURCE_KINDS or None
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, resource_kind="unknown_kind"))
            smoke.validate_failure_report(dict(cleanup_report, resource_kind=None))

            # primary_phase and primary_error_code must either both be set or both be None
            smoke.validate_failure_report(dict(cleanup_report, primary_phase=None, primary_error_code=None))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, primary_phase="bootstrap", primary_error_code=None))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, primary_phase=None, primary_error_code="bootstrap_contract_rejected"))

            # resource_kind may be non-null only when cleanup_step is remove_resource
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="check_deadline", resource_kind="container"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="reconcile_child_receipts", resource_kind="container"))
            smoke.validate_failure_report(dict(cleanup_report, cleanup_step="check_deadline", resource_kind=None))
            smoke.validate_failure_report(dict(cleanup_report, cleanup_step="reconcile_child_receipts", resource_kind=None))

            # Each nullable string field rejects wrong types (list, dict, bool, int, etc.) with safe ValueError
            for field in ("cleanup_step", "cleanup_error_code", "resource_kind", "primary_phase", "primary_error_code"):
                for bad_val in ([1, 2], {"bad": "type"}, True, False, 123, 1.5):
                    with self.assertRaises(ValueError):
                        smoke.validate_failure_report(dict(cleanup_report, **{field: bad_val}))

            # Test invalid schemas: extra field, wrong types, unknown values, bool-as-int
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, extra="unexpected"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=2))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, status="SUCCESS"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, phase="unknown_phase"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, error_type="EvilException"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, error_code="evil_code"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, http_accepted=True))
            # bool-as-int: 1 or 0 instead of True/False
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, http_accepted=0))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, cleanup_verified=1))
            # strict schema_version true and 1.0 rejection
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=True))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=1.0))
            # wrong unhashable phase/type/code/status values rejected predictably with ValueError, not TypeError
            for unhashable in ([1, 2], {"bad": "type"}):
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, phase=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, error_type=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, error_code=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, status=unhashable))

    def test_adversarial_exception_secret_leak_prevention(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            # Adversarial exception whose text is a plausible lowercase secret
            secret_exc = ValueError("super_secret_token_12345")
            report = smoke.record_failure_report(f, phase="positive_http", exc=secret_exc, cleanup_verified=False)
            self.assertEqual(report["error_code"], "unclassified")
            self.assertEqual(report["error_type"], "ValueError")
            self.assertNotIn("super_secret_token_12345", json.dumps(report))

            # Attacker-defined exception subclass with evil __str__
            class AttackerExploitError(ValueError):
                def __init__(self):
                    self.called = False
                def __str__(self):
                    self.called = True
                    raise RuntimeError("exploit __str__ invoked")
            custom_exc = AttackerExploitError()
            report2 = smoke.record_failure_report(f, phase="positive_http", exc=custom_exc, cleanup_verified=False)
            self.assertEqual(report2["error_type"], "Exception")
            self.assertEqual(report2["error_code"], "unclassified")
            self.assertFalse(custom_exc.called)

            # Class named ValueError that is not builtin
            class FakeValueError(Exception):
                pass
            FakeValueError.__name__ = "ValueError"
            fake_exc = FakeValueError("fake")
            report3 = smoke.record_failure_report(f, phase="positive_http", exc=fake_exc, cleanup_verified=False)
            self.assertEqual(report3["error_type"], "Exception")
            self.assertEqual(report3["error_code"], "unclassified")

    def test_unsafe_prepare_path_does_not_write_anything(self):
        with tempfile.TemporaryDirectory() as directory:
            unsafe_dir = Path(directory) / "unsafe symlink dir"
            os.symlink(directory, unsafe_dir)
            with self.assertRaises(ValueError):
                smoke.prepare(unsafe_dir / "fixture")
            self.assertFalse((unsafe_dir / "fixture").exists())

    def test_main_prepare_failure_emits_safe_json_stdout_without_writes(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            unsafe_dir = Path(directory) / "unsafe_symlink_dir"
            os.symlink(directory, unsafe_dir)
            prep_target = unsafe_dir / "fixture"
            stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_target)]), \
                 patch("sys.stdout", stdout_buf), patch("sys.stderr", stderr_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            self.assertEqual(stderr_buf.getvalue(), "")
            out_str = stdout_buf.getvalue().strip()
            rep = json.loads(out_str)
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "prepare")
            self.assertEqual(rep["error_code"], "unsafe_fixture_path")
            self.assertFalse(rep["cleanup_verified"])
            self.assertFalse(prep_target.exists())

    def test_main_preflight_dry_config_failure_propagation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertEqual(rep["error_code"], "compose_preflight_failed")
            self.assertFalse(rep["cleanup_verified"])
            # Dedicated artifact matches stdout
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_bootstrap_failure_preserves_phase_and_cleanup_true(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError("bootstrap_contract_rejected")), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "bootstrap")
            self.assertEqual(rep["error_code"], "bootstrap_contract_rejected")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_service_envoy_readiness_failure_preserves_envoy_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()

            def fake_probe(reg, name, obj):
                if name == "supabase-envoy":
                    return False
                return True

            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=fake_dry), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "allocation_readback", return_value=True), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", return_value=True), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()), \
                 patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve), \
                 patch.object(smoke.ResourceRegistry, "remove"), \
                 patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "supabase-envoy")
            self.assertEqual(rep["error_code"], "phase_health_timeout")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_http_phases_preservation(self):
        import io
        for flag, failing_phase, helper_to_fail in (
            ("--positive-http-only", "positive_http", "run_positive_http_slice"),
            ("--role-http-only", "role_http", "run_role_http_slice"),
            ("--full-http", "jwt_http", "run_jwt_denial_slice"),
        ):
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                def fake_dry(fixture, **kwargs):
                    return {
                        "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                        "volumes": {},
                        "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                    }
                img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
                mock_lifecycle = smoke.bootstrap_lifecycle()
                def fake_alloc(fixture, registry, lifecycle):
                    item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                    registry.allocating(item)
                    return item

                running_obj = {
                    "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                    "receipt": {"id": "cid"},
                    "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
                }
                def fake_resolve(item):
                    if not item.get("allocation_attempted"):
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

                dummy_positive = {"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}
                dummy_role = {"role_cases": {c["label"]: c["status"] for c in smoke.role_http_cases()}, "role_snapshot_comparisons": 10, "gateway_id": "gw", "existing_sensor": "s"}

                patches = [
                    patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), flag]),
                    patch.object(smoke, "dry_effective_config", side_effect=fake_dry),
                    patch.object(smoke, "build_native_image"),
                    patch.object(smoke, "generate_fixture_tls"),
                    patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc),
                    patch.object(smoke, "allocation_readback", return_value=True),
                    patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle),
                    patch.object(mock_lifecycle, "reconcile_child_receipts"),
                    patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)),
                    patch.object(smoke, "startup_state", return_value=True),
                    patch.object(smoke, "probe_startup", return_value=True),
                    patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()),
                    patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve),
                    patch.object(smoke.ResourceRegistry, "remove"),
                    patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd),
                ]
                if helper_to_fail == "run_positive_http_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", side_effect=ValueError("response_contract_failed")))
                elif helper_to_fail == "run_role_http_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", return_value=dummy_positive))
                    patches.append(patch.object(smoke, "run_role_http_slice", side_effect=ValueError("role_case_inventory_owner_contract_failed")))
                elif helper_to_fail == "run_jwt_denial_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", return_value=dummy_positive))
                    patches.append(patch.object(smoke, "run_role_http_slice", return_value=dummy_role))
                    patches.append(patch.object(smoke, "run_jwt_denial_slice", side_effect=ValueError("response_contract_failed")))

                import contextlib
                stdout_buf = io.StringIO()
                patches.append(patch("sys.stdout", stdout_buf))

                with contextlib.ExitStack() as stack:
                    for p in patches:
                        stack.enter_context(p)
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                rep = json.loads(stdout_buf.getvalue().strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["phase"], failing_phase)
                self.assertTrue(rep["cleanup_verified"])
                art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
                self.assertEqual(rep, art)

    def test_main_cleanup_failure_overrides_primary_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("cleanup_daemon_dead")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)
            self.assertEqual(rep["primary_phase"], "bootstrap")
            self.assertEqual(rep["primary_error_code"], "bootstrap_contract_rejected")
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "unclassified")
            self.assertIsNone(rep["resource_kind"])

    def test_cleanup_failure_reconcile_child_receipts_with_incomplete_cleanup(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=mock_model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=mock_lifecycle.IncompleteCleanup("invalid recovery receipt")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["primary_phase"], "bootstrap")
            self.assertEqual(rep["primary_error_code"], "bootstrap_contract_rejected")
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "child_cleanup_unresolved")
            self.assertIsNone(rep["resource_kind"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_resource_removal_container_and_kinds(self):
        import io, contextlib
        for target_kind in ("container", "network", "volume"):
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                def fake_dry(fixture, **kwargs):
                    return {
                        "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                        "volumes": {},
                        "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                    }
                img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
                mock_lifecycle = smoke.bootstrap_lifecycle()
                def fake_alloc(fixture, registry, lifecycle):
                    item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                    registry.allocating(item)
                    return item

                running_obj = {
                    "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                    "receipt": {"id": "cid"},
                    "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
                }
                def fake_resolve(item):
                    if not item.get("allocation_attempted"):
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

                def fake_remove(self, item):
                    if item.get("kind") == target_kind:
                        raise ValueError("resource_cleanup_unresolved")
                    item["removed"] = True

                stdout_buf = io.StringIO()
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                    stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                    stack.enter_context(patch.object(smoke, "build_native_image"))
                    stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                    stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                    stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                    stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                    stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                    stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                    stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                    stack.enter_context(patch.object(smoke, "probe_startup", side_effect=lambda reg, name, obj: False if name == "supabase-envoy" else True))
                    stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "remove", fake_remove))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                    stack.enter_context(patch("sys.stdout", stdout_buf))
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                rep = json.loads(stdout_buf.getvalue().strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["phase"], "cleanup")
                self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
                self.assertFalse(rep["cleanup_verified"])
                self.assertEqual(rep["primary_phase"], "supabase-envoy")
                self.assertEqual(rep["primary_error_code"], "phase_health_timeout")
                self.assertEqual(rep["cleanup_step"], "remove_resource")
                self.assertEqual(rep["cleanup_error_code"], "resource_cleanup_unresolved")
                self.assertEqual(rep["resource_kind"], target_kind)
                art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
                self.assertEqual(rep, art)

    def test_cleanup_failure_resource_ownership_refusal_network(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            def fake_remove(self, item):
                if item.get("kind") == "network":
                    raise ValueError("resource_ownership_refused")
                item["removed"] = True

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove", fake_remove))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertIsNone(rep["primary_phase"])
            self.assertIsNone(rep["primary_error_code"])
            self.assertEqual(rep["cleanup_step"], "remove_resource")
            self.assertEqual(rep["cleanup_error_code"], "resource_ownership_refused")
            self.assertEqual(rep["resource_kind"], "network")
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_deadline_with_preceding_http_failure(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            real_monotonic = time.monotonic
            call_count = [0]
            def fake_monotonic():
                call_count[0] += 1
                if call_count[0] > 10:
                    return real_monotonic() + 100000.0
                return real_monotonic()

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--positive-http-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke, "run_positive_http_slice", side_effect=ValueError("response_contract_failed")))
                stack.enter_context(patch("time.monotonic", side_effect=fake_monotonic))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["primary_phase"], "positive_http")
            self.assertEqual(rep["primary_error_code"], "response_contract_failed")
            self.assertEqual(rep["cleanup_step"], "check_deadline")
            self.assertEqual(rep["cleanup_error_code"], "startup_cleanup_deadline")
            self.assertIsNone(rep["resource_kind"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_unknown_secret_bearing_exception_redacted(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=mock_model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("leak_secret_key_abcdef12345")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "unclassified")
            self.assertNotIn("leak_secret_key_abcdef12345", json.dumps(rep))
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)
            self.assertNotIn("leak_secret_key_abcdef12345", json.dumps(art))

    def test_main_report_validation_failure_preserves_its_own_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            dummy_summary = {"status": "RBAC_JWT_HTTP_ACCEPTED", "exact_cleanup_verified": True}
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--full-http"]), \
                 patch.object(smoke, "startup_only", return_value=dummy_summary), \
                 patch.object(smoke, "validate_summary", side_effect=ValueError("unsafe_summary")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertEqual(rep["error_code"], "unsafe_summary")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_secret_bearing_service_error_sanitized(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            secret_leak = "password_super_secret_12345"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError(secret_leak)), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(secret_leak, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["error_code"], "unclassified")
            self.assertTrue(rep["cleanup_verified"])

    def test_main_replaced_artifact_directory_symlink_refuses_writes(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            # First prepare properly
            fixture = smoke.prepare(prep_dir)
            # Now replace artifacts directory with a newly created dir (different inode) or symlink
            import shutil
            shutil.rmtree(fixture.artifacts)
            os.symlink(directory, fixture.artifacts)
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "prepare", return_value=fixture), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            # Artifact was NOT written through symlink
            self.assertFalse((fixture.artifacts / "smoke-failure.json").exists())

    def test_main_artifact_persistence_failure_emits_json_and_omits_artifact(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch.object(smoke, "durable_json", side_effect=OSError("disk read-only")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertFalse((prep_dir / "artifacts" / "smoke-failure.json").exists())

    def test_cleanup_failure_reports_unresolved_not_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            # If cleanup failed, cleanup_verified must be false
            report = smoke.record_failure_report(f, phase="cleanup", exc=ValueError("startup_cleanup_unresolved_evidence_retained"), cleanup_verified=False)
            self.assertEqual(report["phase"], "cleanup")
            self.assertFalse(report["cleanup_verified"])
            self.assertEqual(report["error_code"], "startup_cleanup_unresolved_evidence_retained")

    def test_main_attacker_smoke_diagnostic_attribute_not_printed(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            attacker_secret = "secret_leak_in_smoke_diag_12345"
            exc = RuntimeError("attacker_controlled")
            exc._smoke_diagnostic = {"token": attacker_secret, "status": "EXPLOITED"}
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "startup_only", side_effect=exc), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(attacker_secret, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertEqual(rep["error_code"], "unclassified")

    def test_main_hostile_getattribute_for_smoke_diagnostic_not_invoked(self):
        import io
        class HostileException(RuntimeError):
            def __getattribute__(self, name):
                if name == "_smoke_diagnostic":
                    raise AssertionError("hostile attribute accessed")
                return super().__getattribute__(name)

        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "startup_only", side_effect=HostileException("boom")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")

    def test_main_malformed_failure_diagnostic_fails_closed(self):
        import io
        secret_text = "super_secret_token_123"
        malformed_reports = [
            # Extra fields
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False, "extra_leak": secret_text},
            # Secret-bearing values
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": secret_text,
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": None, "primary_error_code": None,
             "cleanup_step": None, "cleanup_error_code": None,
             "resource_kind": None},
            # Malformed new fields with wrong types / secret payload
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "cleanup",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": [secret_text], "primary_error_code": None,
             "cleanup_step": {"bad": secret_text}, "cleanup_error_code": [1, 2],
             "resource_kind": 123},
            # Context contradiction carrying secret in resource_kind
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "cleanup",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": None, "primary_error_code": None,
             "cleanup_step": "check_deadline", "cleanup_error_code": "resource_cleanup_unresolved",
             "resource_kind": "container"},
            # False success
            {"schema_version": 1, "status": "RBAC_JWT_HTTP_ACCEPTED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": True, "cleanup_verified": True},
        ]
        for bad_rep in malformed_reports:
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                stdout_buf = io.StringIO()
                diag = smoke.FailureDiagnostic(bad_rep)
                with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir)]), \
                     patch.object(smoke, "summary", side_effect=diag), \
                     patch("sys.stdout", stdout_buf):
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                raw_out = stdout_buf.getvalue()
                self.assertNotIn(secret_text, raw_out)
                rep = json.loads(raw_out.strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["status"], "STARTUP_FAILED")
                self.assertFalse(rep["http_accepted"])

    def test_main_custom_report_object_repr_str_not_invoked(self):
        import io
        secret_repr = "custom_repr_secret_token_abc"
        class HostileReport:
            def __repr__(self):
                return secret_repr
            def __str__(self):
                return secret_repr

        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            diag = smoke.FailureDiagnostic(HostileReport())
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir)]), \
                 patch.object(smoke, "summary", side_effect=diag), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(secret_repr, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)

    def test_full_http_summary_failure_classified_as_report_validation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            f = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(f)
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            started = [False]
            def fake_resolve(item):
                if not started[0]:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            def fake_subp(*args, **kwargs):
                started[0] = True
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if 'psql' in cmd:
                    if 'SELECT string_agg' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': schema_query_result(expected_schema, cmd[-1]).encode(), 'stderr': b''})()
                    if 'profiles_id_fkey' in cmd[-1] or 'on_auth_user_created' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': b'1\n', 'stderr': b''})()
                return type('Res', (), {'returncode': 0, 'stdout': img_out, 'stderr': b''})()

            stdout_buf = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--full-http"]))
                stack.enter_context(patch.object(smoke, "prepare", return_value=f))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store"))
                stack.enter_context(patch.object(smoke, "allocation_readback"))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch.object(smoke, "run_positive_http_slice", return_value={"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}))
                stack.enter_context(patch.object(smoke, "run_role_http_slice", return_value={"gateway_id": "gw1", "existing_sensor": "s1", "role_cases": {c["label"]: c["status"] for c in smoke.role_http_cases()}, "role_snapshot_comparisons": 10}))
                stack.enter_context(patch.object(smoke, "run_jwt_denial_slice", return_value={"jwt_cases": {c["label"]: c["status"] for c in smoke.jwt_http_cases()}, "jwt_snapshot_comparisons": 6}))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch("subprocess.run", side_effect=fake_subp))
                stack.enter_context(patch.object(smoke, "full_http_summary", side_effect=ValueError("cleanup_acceptance_incomplete")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertEqual(rep["error_code"], "cleanup_acceptance_incomplete")
            self.assertTrue(rep["cleanup_verified"])

    def test_durable_startup_summary_write_failure_classified_as_report_validation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            f = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(f)
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            started = [False]
            def fake_resolve(item):
                if not started[0]:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            def fake_subp(*args, **kwargs):
                started[0] = True
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if 'psql' in cmd:
                    if 'SELECT string_agg' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': schema_query_result(expected_schema, cmd[-1]).encode(), 'stderr': b''})()
                    if 'profiles_id_fkey' in cmd[-1] or 'on_auth_user_created' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': b'1\n', 'stderr': b''})()
                return type('Res', (), {'returncode': 0, 'stdout': img_out, 'stderr': b''})()

            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                if str(path).endswith("startup-summary.json") and data.get("status") == "STARTUP_VERIFIED_NOT_HTTP_ACCEPTED":
                    raise OSError("disk_write_error")
                return real_durable_json(path, data)

            stdout_buf = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "prepare", return_value=f))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store"))
                stack.enter_context(patch.object(smoke, "allocation_readback"))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch("subprocess.run", side_effect=fake_subp))
                stack.enter_context(patch.object(smoke, "durable_json", side_effect=selective_durable_json))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertTrue(rep["cleanup_verified"])

    def test_private_diagnostic_write_failure_does_not_suppress_primary_failure(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                # Fail private startup-failure.json write
                if str(path).endswith("startup-failure.json"):
                    raise OSError("private_diagnostic_write_failed")
                return real_durable_json(path, data)

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError("bootstrap_contract_rejected")), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "durable_json", side_effect=selective_durable_json), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            # Primary controlled phase must be bootstrap, not preflight
            self.assertEqual(rep["phase"], "bootstrap")
            self.assertEqual(rep["error_code"], "bootstrap_contract_rejected")
            self.assertTrue(rep["cleanup_verified"])

    def test_private_diagnostic_write_failure_does_not_suppress_cleanup_failure(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                # Fail cleanup startup-summary.json write
                if str(path).endswith("startup-summary.json") and data.get("status") == "CLEANUP_UNRESOLVED":
                    raise OSError("cleanup_summary_write_failed")
                return real_durable_json(path, data)

            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.project + "_store", "img1", {})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("cleanup_daemon_dead")), \
                 patch.object(smoke, "durable_json", side_effect=selective_durable_json), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])


class P8ServerAndSmokeE2EqualSamplesTests(unittest.TestCase):
    def setUp(self):
        from scripts import paper_p8_server
        self.server = paper_p8_server
        from scripts.tests.paper_p8_e2_support import (
            build_e2_equal_samples_context,
            validate_e2_equal_samples_context,
        )
        self.build_ctx = build_e2_equal_samples_context
        self.validate_ctx = validate_e2_equal_samples_context

    def test_e2_context_command_creates_private_valid_context_default(self):
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / 'e2-context.json'
            ret = self.server.main(['e2-context', '--output', str(output)])
            self.assertEqual(ret, 0)
            self.assertTrue(output.is_file())
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            ctx = json.loads(output.read_text())
            validated = self.validate_ctx(ctx, check_freshness=False)
            self.assertEqual(validated['total_samples'], 6000)
            self.assertEqual(validated['sample_rate'], 100)
            self.assertEqual(validated['seed'], 84)
            self.assertEqual(validated['duration_seconds'], 60)
            self.assertEqual(validated['workload_type'], 'equal_samples')

    def test_e2_context_command_custom_values(self):
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / 'e2-custom-context.json'
            ret = self.server.main([
                'e2-context', '--output', str(output),
                '--total-samples', '12000',
                '--sample-rate', '200',
                '--seed', '42'
            ])
            self.assertEqual(ret, 0)
            ctx = json.loads(output.read_text())
            validated = self.validate_ctx(ctx, check_freshness=False)
            self.assertEqual(validated['total_samples'], 12000)
            self.assertEqual(validated['sample_rate'], 200)
            self.assertEqual(validated['seed'], 42)
            self.assertEqual(validated['duration_seconds'], 60)

    def test_e2_context_command_rejects_existing_output_and_symlinks(self):
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / 'exists.json'
            output.write_text('{}')
            with self.assertRaises(ValueError):
                self.server.main(['e2-context', '--output', str(output)])

            sym_dir = Path(td) / 'symdir'
            sym_dir.symlink_to(Path(td))
            with self.assertRaises(ValueError):
                self.server.main(['e2-context', '--output', str(sym_dir / 'out.json')])

    def _valid_net_config(self, td):
        net_config = Path(td) / 'net.json'
        net_config.write_text(json.dumps({
            'schema_version': 1, 'bind_ip': '10.0.0.1', 'advertised_host': '10.0.0.1',
            'https_port': 8443, 'mqtt_tls_port': 8883,
            'required_san_dns': ['localhost', 'nginx', 'mosquitto'],
            'required_san_ips': ['127.0.0.1', '10.0.0.1'],
        }))
        net_config.chmod(0o600)
        return net_config

    def test_serve_e2_workload_omitted_is_steady_burst(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / 'server'
            net_config = self._valid_net_config(td)
            captured_cmd = []
            with patch.object(self.server.remote, 'preflight_bind'), \
                 patch('subprocess.Popen') as mock_popen:
                mock_proc = Mock(wait=Mock(return_value=0))
                def fake_popen(cmd, **kwargs):
                    captured_cmd.extend(cmd)
                    return mock_proc
                mock_popen.side_effect = fake_popen
                ret = self.server.main(['serve', '--root', str(root), '--network-config', str(net_config),
                                        '--experiment', 'E2', '--samples-per-message', '100'])
                self.assertEqual(ret, 0)
                self.assertIn('--p8-e2-workload', captured_cmd)
                idx = captured_cmd.index('--p8-e2-workload')
                self.assertEqual(captured_cmd[idx + 1], 'steady_burst')

            # steady_burst rejects --context and --reference
            with self.assertRaisesRegex(ValueError, 'E2 steady_burst does not accept --context or --reference'):
                self.server.main(['serve', '--root', str(Path(td) / 's2'), '--network-config', str(net_config),
                                  '--experiment', 'E2', '--samples-per-message', '100',
                                  '--workload', 'steady_burst', '--context', str(Path(td) / 'any.json')])

    def test_serve_e2_workload_equal_samples_requires_context(self):
        with tempfile.TemporaryDirectory() as td:
            net_config = self._valid_net_config(td)

            # Missing --context
            with self.assertRaisesRegex(ValueError, 'E2 equal_samples requires --context'):
                self.server.main(['serve', '--root', str(Path(td) / 'server_missing'), '--network-config', str(net_config),
                                  '--experiment', 'E2', '--workload', 'equal_samples',
                                  '--samples-per-message', '100'])

            # Valid context
            ctx_path = Path(td) / 'valid_ctx.json'
            ctx = self.build_ctx(total_samples=6000, sample_rate=100, seed=84)
            ctx_path.write_text(json.dumps(ctx))
            ctx_path.chmod(0o600)

            captured_cmd = []
            with patch.object(self.server.remote, 'preflight_bind'), \
                 patch('subprocess.Popen') as mock_popen:
                mock_proc = Mock(wait=Mock(return_value=0))
                def fake_popen(cmd, **kwargs):
                    captured_cmd.extend(cmd)
                    return mock_proc
                mock_popen.side_effect = fake_popen
                ret = self.server.main(['serve', '--root', str(Path(td) / 'server_valid'), '--network-config', str(net_config),
                                        '--experiment', 'E2', '--workload', 'equal_samples',
                                        '--samples-per-message', '100',
                                        '--context', str(ctx_path)])
                self.assertEqual(ret, 0)
                self.assertIn('--p8-e2-workload', captured_cmd)
                idx = captured_cmd.index('--p8-e2-workload')
                self.assertEqual(captured_cmd[idx + 1], 'equal_samples')
                self.assertIn('--p8-e2-context', captured_cmd)
                c_idx = captured_cmd.index('--p8-e2-context')
                self.assertEqual(captured_cmd[c_idx + 1], str(ctx_path))

    def test_serve_rejects_mixing_and_unrelated_flags(self):
        with tempfile.TemporaryDirectory() as td:
            net_config = self._valid_net_config(td)

            # E1 with --workload rejected
            with self.assertRaises(ValueError):
                self.server.main(['serve', '--root', str(Path(td) / 's1'), '--network-config', str(net_config),
                                  '--experiment', 'E1', '--workload', 'equal_samples'])

            # E4 with --workload rejected
            with self.assertRaises(ValueError):
                self.server.main(['serve', '--root', str(Path(td) / 's2'), '--network-config', str(net_config),
                                  '--experiment', 'E4', '--workload', 'equal_samples'])

            # E3 with equal_samples rejected
            with self.assertRaises(ValueError):
                self.server.main(['serve', '--root', str(Path(td) / 's3'), '--network-config', str(net_config),
                                  '--experiment', 'E3', '--dispatch-mode', 'polling',
                                  '--workload', 'equal_samples'])

            # E2 with --arm rejected
            with self.assertRaises(ValueError):
                self.server.main(['serve', '--root', str(Path(td) / 's4'), '--network-config', str(net_config),
                                  '--experiment', 'E2', '--samples-per-message', '100',
                                  '--arm', 'optimized'])

    def test_smoke_e2_fixture_equal_samples_disables_only_shaper(self):
        with tempfile.TemporaryDirectory() as td:
            ctx_path = Path(td) / 'context.json'
            ctx = self.build_ctx(total_samples=6000, sample_rate=100, seed=84)
            ctx_path.write_text(json.dumps(ctx))
            ctx_path.chmod(0o600)

            fixture_steady = smoke.prepare(Path(td) / 'f_steady', paper_p8_e2=True,
                                           paper_p8_e2_workload='steady_burst')
            self.assertEqual(fixture_steady.values['MQTT_SHAPER_ENABLED'], 'true')
            self.assertEqual(fixture_steady.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(fixture_steady.values['MQTT_MICROBATCH_MAX_WAIT'], '50ms')

            fixture_equal = smoke.prepare(Path(td) / 'f_equal', paper_p8_e2=True,
                                          paper_p8_e2_workload='equal_samples',
                                          paper_p8_e2_context=str(ctx_path))
            self.assertEqual(fixture_equal.values['MQTT_SHAPER_ENABLED'], 'false')
            self.assertEqual(fixture_equal.values['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(fixture_equal.values['MQTT_MICROBATCH_MAX_MESSAGES'], '32')
            self.assertEqual(fixture_equal.values['MQTT_MICROBATCH_MAX_ROWS'], '500')
            self.assertEqual(fixture_equal.values['MQTT_MICROBATCH_MAX_PAYLOAD_BYTES'], '4194304')
            self.assertEqual(fixture_equal.values['MQTT_MICROBATCH_MAX_WAIT'], '50ms')
            self.assertEqual(fixture_equal.values['MQTT_RAM_DEDUP_ENABLED'], 'false')
            self.assertEqual(fixture_equal.values['STAGE5_ENABLED'], 'false')
            self.assertEqual(fixture_equal.values['STAGE5_WAKEUP_ENABLED'], 'false')
            self.assertEqual(fixture_equal.values['WEBSOCKET_ENABLED'], 'false')

    def test_smoke_e2_prepare_only_report_for_equal_samples(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fixture = type('Fixture', (), {'paper_p8_e2': True, 'paper_p8_e2_workload': 'equal_samples',
                'project': 'e2-test', 'private': root / 'private', 'artifacts': root / 'artifacts'})()
            fixture.private.mkdir()
            fixture.artifacts.mkdir()
            (fixture.private / 'session.json').write_text(json.dumps(
                {'experiment': 'E2', 'phase_id': 'equal-samples-100', 'workload': 'equal_samples'}))
            (fixture.artifacts / 'p8-e2-equal-samples-100-remote-result.json').write_text(json.dumps(
                {'status': 'PASS', 'validation': {'valid': True}}))
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='VALIDATED',
                cleanup_verified=True, cleanup_counts={})
            self.assertEqual(report['status'], 'PAPER_P8_E2_WORKLOAD_VERIFIED')
            self.assertEqual(report['artifact'], 'p8-e2-equal-samples-100-remote-result.json')
            self.assertTrue(report['p8_e2_workload_verified'])

    def test_paper_p8_e2_profiles_valid_for_equal_samples(self):
        # Equal samples profile with equal steady counts (e.g. 30 messages each for S=100)
        profile_100 = {
            'samples_per_message': 100,
            'status': 'PASS',
            'validation': {'valid': True, 'failures': []},
            'workloads': {
                'A': {'telemetry': {'generated_unique': 30, 'application_acks': 30, 'pending_messages': 0}},
                'B': {'telemetry': {'generated_unique': 30, 'application_acks': 30, 'pending_messages': 0}},
            }
        }
        profile_1 = {
            'samples_per_message': 1,
            'status': 'PASS',
            'validation': {'valid': True, 'failures': []},
            'workloads': {
                'A': {'telemetry': {'generated_unique': 3000, 'application_acks': 3000, 'pending_messages': 0}},
                'B': {'telemetry': {'generated_unique': 3000, 'application_acks': 3000, 'pending_messages': 0}},
            }
        }
        evidence = {
            'workload_type': 'equal_samples',
            'profiles': [profile_100, profile_1]
        }
        self.assertTrue(smoke.paper_p8_e2_profiles_valid(evidence))


if __name__ == "__main__":
    unittest.main()
