#!/usr/bin/env python3
"""Unit tests for P8.5c mechanism-arm, comparability, and accounting helpers."""
import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import unittest

import paper_p8_comparison as p8c


def sample_report(
    *,
    arm='optimized',
    source_revision='1' * 40,
    dirty_tree_hash='a' * 64,
    binary_sha256='b' * 64,
    paperload_source_sha256='c' * 64,
    image_ids=None,
    schema='24:telemetry_ledger_timestamp',
    security_mode='jwt_hs256_dynsec_tls',
    quota=None,
    workers=2,
    durability='durable_sqlite_postgres',
    topology=None,
    seed=42,
    phase_seconds=None,
    cohort=None,
    workload=None,
    effective_flags=None,
    counters=None,
    ingress_delta=None,
    microbatch_metrics=None,
    ram_dedup_metrics=None,
    command_metrics=None,
    fanout_metrics=None,
    raw_rows=2000,
):
    if image_ids is None:
        image_ids = {'backend': 'sha256:' + 'd' * 64, 'mosquitto': 'sha256:' + 'e' * 64}
    if quota is None:
        quota = {'cpus': 2, 'memory_bytes': 4294967296, 'queue_capacity': 1000}
    if topology is None:
        topology = {'gateways': 2, 'sensors_per_gateway': 1, 'viewers': 4}
    if phase_seconds is None:
        phase_seconds = {'warmup': 2, 'measure': 10, 'drain': 5}
    if cohort is None:
        cohort = ['gw-1:sensor-1', 'gw-2:sensor-1']
    if workload is None:
        workload = {
            'workload_type': 'p8_telemetry_stream',
            'traffic': {'rate_per_second': 10, 'samples_per_message': 100},
            'topology': copy.deepcopy(topology),
            'phases': copy.deepcopy(phase_seconds),
            'seed': seed,
            'limits': {'workers': workers, 'queue_capacity': 1000},
            'run_id': f'run-{arm}-123',
            'endpoints': {'https': 'https://127.0.0.1:443', 'mqtt': 'mqtts://127.0.0.1:8883'},
        }
    if effective_flags is None:
        effective_flags = p8c.get_expected_arm_flags(arm)
    if counters is None:
        counters = {'generated_unique': 20, 'offered_unique': 20, 'publish_attempts': 20}
    if ingress_delta is None:
        ingress_delta = {
            'available': True,
            'delta': {
                'Received': 20, 'Accepted': 20, 'Committed': 20,
                'DuplicateCommitted': 0, 'SamplesNew': raw_rows,
                'ShaperRateLimited': 5 if arm == 'optimized' else 0,
                'ShaperPassed': 20,
            }
        }
    if microbatch_metrics is None:
        microbatch_metrics = {
            'groups': 5,
            'members': 20,
            'max_members': 4,
            'copy_operations': 6,
            'committed_messages': 20,
        }
        if ram_dedup_metrics is None:
            ram_dedup_metrics = {
            'lookup_hit_total': 3 if arm == 'optimized' else 0,
            'ram_misses': 17,
        }
    if command_metrics is None:
        command_metrics = {
            'fresh_commands_dispatched': 4,
            'notification_signals_accepted': 4 if arm == 'optimized' else 0,
            'puback': 4,
            'scans': 5,
        }
    if fanout_metrics is None:
        fanout_metrics = {
            'subscriptions_ready': 4,
            'writes': 20,
            'scan_evaluations': 0 if arm == 'optimized' else 80,
            'indexed_selections': 20 if arm == 'optimized' else 0,
            'readiness_verified': True,
        }

    return {
        'status': 'PASS',
        'cleanup_verified': True,
        'arm': arm,
        'metadata': {
            'source_revision': source_revision,
            'dirty_tree_hash': dirty_tree_hash,
            'dirty_worktree': True,
            'binary_sha256': binary_sha256,
            'paperload_source_sha256': paperload_source_sha256,
            'image_ids': copy.deepcopy(image_ids),
            'schema': schema,
            'security_mode': security_mode,
            'quota': copy.deepcopy(quota),
            'workers': workers,
            'durability': durability,
            'topology': copy.deepcopy(topology),
            'seed': seed,
            'phase_seconds': copy.deepcopy(phase_seconds),
            'cohort': copy.deepcopy(cohort),
            'workload_sha256': (p8c.normalized_workload_fingerprint(workload).removeprefix('sha256:')
                                if workload else 'f' * 64),
            'workload': copy.deepcopy(workload),
            'effective_flags': copy.deepcopy(effective_flags),
        },
        'accounting': {
            'counters': copy.deepcopy(counters),
            'ingress_delta': copy.deepcopy(ingress_delta),
            'raw_rows': {'available': raw_rows},
        },
        'mechanism_metrics': {
            'microbatch': copy.deepcopy(microbatch_metrics),
            'ram_dedup': copy.deepcopy(ram_dedup_metrics),
            'command': copy.deepcopy(command_metrics),
            'fanout': copy.deepcopy(fanout_metrics),
        },
    }


class ArmDefinitionsTests(unittest.TestCase):
    def test_optimized_reference_flags(self):
        flags = p8c.get_expected_arm_flags('optimized')
        self.assertEqual(flags['MQTT_SHAPER_ENABLED'], 'true')
        self.assertEqual(flags['MQTT_MICROBATCH_ENABLED'], 'true')
        self.assertEqual(flags['MQTT_RAM_DEDUP_ENABLED'], 'true')
        self.assertEqual(flags['STAGE5_WAKEUP_ENABLED'], 'true')
        self.assertEqual(flags['WS_FANOUT_MODE'], 'indexed')

    def test_each_ablation_arm_differs_by_exactly_one_flag(self):
        ref = p8c.get_expected_arm_flags('optimized')
        ablation_arms = ('shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan')
        expected_diffs = {
            'shaper_off': ('MQTT_SHAPER_ENABLED', 'false'),
            'microbatch_off': ('MQTT_MICROBATCH_ENABLED', 'false'),
            'ram_dedup_off': ('MQTT_RAM_DEDUP_ENABLED', 'false'),
            'polling': ('STAGE5_WAKEUP_ENABLED', 'false'),
            'fanout_scan': ('WS_FANOUT_MODE', 'scan'),
        }
        for arm in ablation_arms:
            flags = p8c.get_expected_arm_flags(arm)
            diffs = {k: flags[k] for k in ref if flags[k] != ref[k]}
            self.assertEqual(len(diffs), 1, f'Arm {arm} must differ by exactly 1 flag')
            diff_key, diff_val = expected_diffs[arm]
            self.assertIn(diff_key, diffs)
            self.assertEqual(diffs[diff_key], diff_val)

    def test_unknown_arm_rejected(self):
        with self.assertRaises(ValueError):
            p8c.get_expected_arm_flags('unknown_arm')


class MechanismDiffTests(unittest.TestCase):
    def test_valid_single_difference(self):
        ref = p8c.get_expected_arm_flags('optimized')
        arm = p8c.get_expected_arm_flags('shaper_off')
        res = p8c.validate_mechanism_diff(ref, arm, 'shaper_off')
        self.assertTrue(res['valid'])
        self.assertEqual(res['differing_flag'], 'MQTT_SHAPER_ENABLED')

    def test_identical_paths_rejected(self):
        ref = p8c.get_expected_arm_flags('optimized')
        res = p8c.validate_mechanism_diff(ref, ref, 'shaper_off')
        self.assertFalse(res['valid'])
        self.assertEqual(res['reason'], 'two_identical_paths_rejected')

    def test_multiple_differences_rejected(self):
        ref = p8c.get_expected_arm_flags('optimized')
        arm = dict(ref, MQTT_SHAPER_ENABLED='false', MQTT_MICROBATCH_ENABLED='false')
        res = p8c.validate_mechanism_diff(ref, arm, 'shaper_off')
        self.assertFalse(res['valid'])
        self.assertEqual(res['reason'], 'multiple_mechanism_differences_rejected')

    def test_unexpected_difference_rejected(self):
        ref = p8c.get_expected_arm_flags('optimized')
        arm = p8c.get_expected_arm_flags('microbatch_off')
        res = p8c.validate_mechanism_diff(ref, arm, 'shaper_off')
        self.assertFalse(res['valid'])
        self.assertEqual(res['reason'], 'unexpected_mechanism_difference')

    def test_missing_flag_not_false(self):
        ref = p8c.get_expected_arm_flags('optimized')
        arm = p8c.get_expected_arm_flags('shaper_off')
        del arm['MQTT_RAM_DEDUP_ENABLED']
        self.assertFalse(p8c.validate_mechanism_diff(ref, arm, 'shaper_off')['valid'])


class WorkloadNormalizationTests(unittest.TestCase):
    def test_normalizes_run_id_endpoints_timestamps(self):
        w1 = {
            'workload_type': 'telemetry',
            'traffic': {'rate': 10},
            'seed': 42,
            'run_id': 'run-1111',
            'created_at': '2026-10-28T10:00:00Z',
            'endpoints': {'https': 'https://localhost:443'},
        }
        w2 = {
            'workload_type': 'telemetry',
            'traffic': {'rate': 10},
            'seed': 42,
            'run_id': 'run-9999',
            'created_at': '2026-10-28T12:00:00Z',
            'endpoints': {'https': 'https://10.0.0.1:8443'},
        }
        f1 = p8c.normalized_workload_fingerprint(w1)
        f2 = p8c.normalized_workload_fingerprint(w2)
        self.assertEqual(f1, f2)
        self.assertTrue(f1.startswith('sha256:'))

    def test_content_difference_produces_different_fingerprint(self):
        w1 = {'workload_type': 'telemetry', 'traffic': {'rate': 10}, 'seed': 42}
        w2 = {'workload_type': 'telemetry', 'traffic': {'rate': 20}, 'seed': 42}
        f1 = p8c.normalized_workload_fingerprint(w1)
        f2 = p8c.normalized_workload_fingerprint(w2)
        self.assertNotEqual(f1, f2)

    def test_nested_semantic_timestamp_and_port_preserved(self):
        a = {'schedule': [{'timestamp': 10, 'payload': {'port': 7, 'client_id': 'x'}}]}
        b = {'schedule': [{'timestamp': 11, 'payload': {'port': 7, 'client_id': 'x'}}]}
        self.assertNotEqual(p8c.normalized_workload_fingerprint(a), p8c.normalized_workload_fingerprint(b))


class RunComparabilityTests(unittest.TestCase):
    def test_comparable_pair_accepted(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='shaper_off')
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertTrue(res['comparable'])
        self.assertEqual(res['reason'], None)
        self.assertIn('normalized_workload_fingerprint', res)

    def test_changed_source_revision_rejected(self):
        ref = sample_report(arm='optimized', source_revision='1' * 40)
        arm = sample_report(arm='shaper_off', source_revision='2' * 40)
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'source_revision_mismatch')

    def test_changed_prebuilt_journal_digest_rejected(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        arm['metadata']['workload_sha256'] = '0' * 64
        self.assertEqual(p8c.validate_run_comparability(ref, arm, 'shaper_off')['reason'],
                         'workload_sha256_mismatch')

    def test_workload_hash_must_match_actual_normalized_workload(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        ref['metadata']['workload']['journal_sha256'] = '0' * 64
        arm['metadata']['workload']['journal_sha256'] = '0' * 64
        self.assertEqual(p8c.validate_run_comparability(ref, arm, 'shaper_off')['reason'],
                         'workload_sha256_content_mismatch')

    def test_changed_binary_rejected(self):
        ref = sample_report(arm='optimized', binary_sha256='b' * 64)
        arm = sample_report(arm='shaper_off', binary_sha256='a' * 64)
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'binary_fingerprint_mismatch')

    def test_unlike_dirty_trees_rejected(self):
        ref = sample_report(arm='optimized', dirty_tree_hash='a' * 64)
        arm = sample_report(arm='shaper_off', dirty_tree_hash='b' * 64)
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'dirty_worktree_mismatch')

    def test_changed_security_mode_rejected(self):
        ref = sample_report(arm='optimized', security_mode='jwt_hs256_dynsec_tls')
        arm = sample_report(arm='shaper_off', security_mode='no_auth')
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'security_mode_mismatch')

    def test_changed_quota_rejected(self):
        ref = sample_report(arm='optimized', quota={'cpus': 2, 'memory_bytes': 4294967296})
        arm = sample_report(arm='shaper_off', quota={'cpus': 4, 'memory_bytes': 8589934592})
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'quota_mismatch')

    def test_synthetic_vs_durable_rejected(self):
        ref = sample_report(arm='optimized', durability='durable_sqlite_postgres')
        arm = sample_report(arm='shaper_off', durability='synthetic_mock')
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'durability_mismatch')

    def test_missing_fingerprint_rejected(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='shaper_off')
        del arm['metadata']['paperload_source_sha256']
        res = p8c.validate_run_comparability(ref, arm, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertIn(res['reason'], ('missing_fingerprint_paperload_source_sha256',
                                      'unavailable_fingerprint_paperload_source_sha256'))

    def test_none_fingerprint_rejected(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        arm['metadata']['binary_sha256'] = None
        self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])

    def test_dirty_boolean_without_hash_rejected(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        for run in (ref, arm):
            run['metadata'].pop('dirty_tree_hash')
            run['metadata']['dirty_worktree'] = True
        self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])

    def test_empty_workload_rejected(self):
        ref, arm = sample_report(arm='optimized', workload={}), sample_report(arm='shaper_off', workload={})
        self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])

    def test_nonpass_or_unverified_cleanup_rejected(self):
        for status in ('FAILED', 'PARTIAL'):
            ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
            ref['status'] = status
            self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        ref['cleanup_verified'] = False
        self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])

    def test_extra_effective_flag_difference_rejected(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        ref['metadata']['effective_flags']['STAGE5_ENABLED'] = 'true'
        arm['metadata']['effective_flags']['STAGE5_ENABLED'] = 'false'
        self.assertFalse(p8c.validate_run_comparability(ref, arm, 'shaper_off')['comparable'])

    def test_current_p85a_report_shape_not_qualified_as_optimized_reference(self):
        path = Path('/var/tmp/opencode/p8/p85a-run-20261028-102501/fixture/artifacts/run.json')
        if not path.is_file():
            self.skipTest('provided safe P8.5a artifact unavailable')
        report = json.loads(path.read_text())
        self.assertFalse(p8c.validate_run_comparability(report, report, 'shaper_off')['comparable'])


class MechanismActivationTests(unittest.TestCase):
    def test_shaper_active(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='shaper_off')
        res = p8c.check_mechanism_activation('shaper_off', ref, arm)
        self.assertTrue(res['eligible'])
        self.assertEqual(res['status'], 'ELIGIBLE')

    def test_shaper_inactive_no_rate_limiting(self):
        ref = sample_report(arm='optimized')
        ref['accounting']['ingress_delta']['delta']['ShaperRateLimited'] = 0
        arm = sample_report(arm='shaper_off')
        res = p8c.check_mechanism_activation('shaper_off', ref, arm)
        self.assertFalse(res['eligible'])
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'mechanism_inactive_no_shaper_rate_limiting')

    def test_microbatch_active(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='microbatch_off')
        res = p8c.check_mechanism_activation('microbatch_off', ref, arm)
        self.assertTrue(res['eligible'])
        self.assertEqual(res['status'], 'ELIGIBLE')

    def test_microbatch_inactive_no_multi_member_groups(self):
        ref = sample_report(arm='optimized')
        ref['mechanism_metrics']['microbatch'].update(groups=5, members=5, max_members=1)
        ref['mechanism_metrics']['microbatch']['copy_operations'] = 20
        ref['mechanism_metrics']['microbatch']['committed_messages'] = 20
        arm = sample_report(arm='microbatch_off')
        res = p8c.check_mechanism_activation('microbatch_off', ref, arm)
        self.assertFalse(res['eligible'])
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'mechanism_inactive_no_multi_member_batches')

    def test_ram_dedup_active(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='ram_dedup_off')
        res = p8c.check_mechanism_activation('ram_dedup_off', ref, arm)
        self.assertTrue(res['eligible'])
        self.assertEqual(res['status'], 'ELIGIBLE')

    def test_ram_dedup_inactive_zero_hits(self):
        ref = sample_report(arm='optimized')
        ref['mechanism_metrics']['ram_dedup']['lookup_hit_total'] = 0
        arm = sample_report(arm='ram_dedup_off')
        res = p8c.check_mechanism_activation('ram_dedup_off', ref, arm)
        self.assertFalse(res['eligible'])
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'mechanism_inactive_no_warm_ram_hits')

    def test_polling_wakeup_active(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='polling')
        res = p8c.check_mechanism_activation('polling', ref, arm)
        self.assertTrue(res['eligible'])
        self.assertEqual(res['status'], 'ELIGIBLE')

    def test_polling_wakeup_inactive_zero_wakeups(self):
        ref = sample_report(arm='optimized')
        ref['mechanism_metrics']['command']['notification_signals_accepted'] = 0
        arm = sample_report(arm='polling')
        res = p8c.check_mechanism_activation('polling', ref, arm)
        self.assertFalse(res['eligible'])
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'mechanism_inactive_no_commands_or_wakeups')

    def test_fanout_scan_active(self):
        ref = sample_report(arm='optimized')
        arm = sample_report(arm='fanout_scan')
        res = p8c.check_mechanism_activation('fanout_scan', ref, arm)
        self.assertTrue(res['eligible'])
        self.assertEqual(res['status'], 'ELIGIBLE')

    def test_fanout_scan_inactive_zero_deliveries(self):
        ref = sample_report(arm='optimized')
        ref['mechanism_metrics']['fanout']['writes'] = 0
        arm = sample_report(arm='fanout_scan')
        res = p8c.check_mechanism_activation('fanout_scan', ref, arm)
        self.assertFalse(res['eligible'])
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'mechanism_inactive_no_fanout_deliveries')

    def test_fanout_requires_both_real_selection_path_counters(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='fanout_scan')
        del ref['mechanism_metrics']['fanout']['indexed_selections']
        self.assertEqual(p8c.check_mechanism_activation('fanout_scan', ref, arm)['status'], 'INCONCLUSIVE')

    def test_missing_shaper_counter_is_unavailable_not_zero(self):
        ref, arm = sample_report(arm='optimized'), sample_report(arm='shaper_off')
        del ref['accounting']['ingress_delta']['delta']['ShaperRateLimited']
        result = p8c.check_mechanism_activation('shaper_off', ref, arm)
        self.assertFalse(result['eligible'])
        self.assertIn('unavailable', result['reason'])


class GammaRedAndRetentionTests(unittest.TestCase):
    def test_gamma_red_nominal(self):
        res = p8c.calculate_gamma_red(100, 80, metric_name='db_tx_count')
        self.assertTrue(res['available'])
        self.assertAlmostEqual(res['gamma_red_percent'], 20.0)
        self.assertEqual(res['metric'], 'db_tx_count')
        self.assertFalse(res['normalized'])

    def test_gamma_red_reference_zero_unavailable(self):
        res = p8c.calculate_gamma_red(0, 0, metric_name='db_tx_count')
        self.assertFalse(res['available'])
        self.assertEqual(res['reason'], 'reference_zero_or_negative')

    def test_gamma_red_reference_negative_unavailable(self):
        res = p8c.calculate_gamma_red(-10, 5, metric_name='db_tx_count')
        self.assertFalse(res['available'])
        self.assertEqual(res['reason'], 'reference_zero_or_negative')

    def test_gamma_red_unique_normalized(self):
        # ref: 200 ops for 100 unique = 2.0 ops/msg
        # opt: 150 ops for 100 unique = 1.5 ops/msg
        # gamma: (2.0 - 1.5) / 2.0 = 25%
        res = p8c.calculate_gamma_red(
            200, 150, metric_name='db_copy_calls',
            unique_messages_ref=100, unique_messages_opt=100,
        )
        self.assertTrue(res['available'])
        self.assertTrue(res['normalized'])
        self.assertAlmostEqual(res['gamma_red_percent'], 25.0)

    def test_gamma_red_unique_normalized_invalid_unique_count(self):
        res = p8c.calculate_gamma_red(
            200, 150, metric_name='db_copy_calls',
            unique_messages_ref=0, unique_messages_opt=100,
        )
        self.assertFalse(res['available'])
        self.assertEqual(res['reason'], 'unique_messages_zero_or_negative')

    def test_raw_retention_verified(self):
        res = p8c.verify_raw_retention(2000, 2000, content_provenance={
            'verified': True, 'reference_digest': 'sha256:x', 'optimized_digest': 'sha256:x'})
        self.assertTrue(res['retention_verified'])
        self.assertEqual(res['reason'], None)

    def test_raw_retention_mismatch(self):
        res = p8c.verify_raw_retention(2000, 1990)
        self.assertFalse(res['retention_verified'])
        self.assertEqual(res['reason'], 'raw_retention_mismatch')

    def test_raw_count_alone_does_not_verify_content(self):
        self.assertFalse(p8c.verify_raw_retention(2000, 2000)['retention_verified'])


class StatisticalAggregationTests(unittest.TestCase):
    def test_per_run_median_and_range(self):
        runs = [
            {'run_id': f'r{i}', 'status': 'PASS', 'cleanup_verified': True,
             'compatibility_key': 'same', 'p99_ms': p, 'tx_count': tx}
            for i, (p, tx) in enumerate(((12.0, 100), (15.0, 110), (11.0, 95)))
        ]
        agg = p8c.aggregate_independent_repeats(runs, ['p99_ms', 'tx_count'])
        self.assertTrue(agg['available'])
        self.assertEqual(agg['count'], 3)
        # Median of [11.0, 12.0, 15.0] is 12.0
        self.assertAlmostEqual(agg['metrics']['p99_ms']['median'], 12.0)
        self.assertEqual(agg['metrics']['p99_ms']['min'], 11.0)
        self.assertEqual(agg['metrics']['p99_ms']['max'], 15.0)
        self.assertEqual(agg['metrics']['p99_ms']['range'], 4.0)

        # Median of [95, 100, 110] is 100
        self.assertAlmostEqual(agg['metrics']['tx_count']['median'], 100)
        self.assertEqual(agg['metrics']['tx_count']['range'], 15)

        # Verify notice on synthetic CI and merged percentiles
        self.assertIn('aggregation_note', agg)
        self.assertFalse(agg.get('synthetic_ci_computed', False))

    def test_empty_runs_handled(self):
        agg = p8c.aggregate_independent_repeats([], ['p99_ms'])
        self.assertFalse(agg['available'])
        self.assertEqual(agg['reason'], 'no_runs_provided')

    def test_repeat_aggregation_requires_run_identity_and_success(self):
        runs = [{'run_id': 'r1', 'p99_ms': 12.0}, {'run_id': 'r2', 'p99_ms': 15.0}]
        self.assertFalse(p8c.aggregate_independent_repeats(runs, ['p99_ms'])['available'])

    def test_repeat_aggregation_rejects_nonfinite_and_bool_values(self):
        runs = [
            {'run_id': 'r1', 'status': 'PASS', 'cleanup_verified': True, 'compatibility_key': 'x', 'p99_ms': float('nan')},
            {'run_id': 'r2', 'status': 'PASS', 'cleanup_verified': True, 'compatibility_key': 'x', 'p99_ms': True},
        ]
        agg = p8c.aggregate_independent_repeats(runs, ['p99_ms'])
        self.assertFalse(agg['available'])

    def test_gamma_rejects_bool_nan_inf_and_negative(self):
        for invalid in (True, float('nan'), float('inf'), -1, None):
            self.assertFalse(p8c.calculate_gamma_red(invalid, 1, metric_name='x')['available'])


class RunOrderPlannerTests(unittest.TestCase):
    def test_deterministic_order_with_seed(self):
        arms = ['optimized', 'shaper_off', 'microbatch_off']
        plan1 = p8c.plan_run_order(arms, repeats=3, seed=42)
        plan2 = p8c.plan_run_order(arms, repeats=3, seed=42)
        self.assertEqual(plan1['plan'], plan2['plan'])
        self.assertEqual(len(plan1['plan']), 9)
        self.assertFalse(plan1['executed'])
        self.assertTrue(plan1['official_repeats_gte_3'])

    def test_different_seeds_vary_order(self):
        arms = ['optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling']
        plan1 = p8c.plan_run_order(arms, repeats=3, seed=42)
        plan2 = p8c.plan_run_order(arms, repeats=3, seed=999)
        self.assertNotEqual(
            [item['arm'] for item in plan1['plan']],
            [item['arm'] for item in plan2['plan']],
        )

    def test_invalid_parameters_rejected(self):
        with self.assertRaises(ValueError):
            p8c.plan_run_order([], repeats=1, seed=42)
        with self.assertRaises(ValueError):
            p8c.plan_run_order(['optimized'], repeats=0, seed=42)
        with self.assertRaises(ValueError):
            p8c.plan_run_order(['invalid_arm'], repeats=1, seed=42)


def sample_e5_report(
    arm: str,
    *,
    seed: int = 999,
    gateway_binary_sha256: str = '9' * 64,
    paperload_binary_sha256: str = '8' * 64,
    combined_binary_sha256: str = '7' * 64,
    process_instance: str = 'proc-uuid-fixed-1234',
    initial_copy_before: int = 5,
    initial_copy_after: int = 25,
    warm_att_before: int = 16,
    warm_att_after: int = 26,
    process_mismatch: bool = False,
    counter_reset: bool = False,
    include_extra_secrets: bool = False,
    image_ids: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    rep = sample_report(arm=arm, seed=seed)
    rep['experiment'] = 'E5'
    rep['full_p8_accepted'] = False
    rep['cleanup_verified'] = True
    rep['exact_cleanup_verified'] = True
    rep['validation'] = {'valid': True}
    rep['source_revision'] = rep['metadata']['source_revision']
    rep['source_fingerprint'] = {
        'revision': rep['metadata']['source_revision'],
        'branch': 'feat/test',
        'dirty': True,
        'dirty_source_sha256': rep['metadata']['dirty_tree_hash'],
    }
    rep['paperload_binary_sha256'] = paperload_binary_sha256
    rep['gateway_binary_sha256'] = gateway_binary_sha256
    rep['metadata']['binary_sha256'] = combined_binary_sha256
    if image_ids is not None:
        rep['image_ids'] = copy.deepcopy(image_ids)
        rep['metadata']['image_ids'] = copy.deepcopy(image_ids)
    else:
        rep['image_ids'] = copy.deepcopy(rep['metadata']['image_ids'])
    rep['journal_sha256'] = 'e' * 64
    rep['effective_flags'] = p8c.get_expected_arm_flags(arm)
    rep['metadata']['effective_flags'] = rep['effective_flags']
    rep['quota'] = rep['metadata']['quota']
    rep['schema'] = rep['metadata']['schema']

    cleanup_counts = {'container': 12, 'network': 1, 'volume': 5}
    if include_extra_secrets:
        cleanup_counts['secret_key'] = 'leaked_secret_val'
        cleanup_counts['admin_password'] = 123456
        rep['metadata']['image_ids']['injected_secret'] = 'not_a_sha256_hash'
    rep['cleanup_counts'] = cleanup_counts

    db_delta_init = {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}
    if include_extra_secrets:
        db_delta_init['sql_password'] = 'secret_pass'
    db_delta_warm = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
    rep['phases'] = {
        'initial': {'status': 'PASS', 'database_delta': db_delta_init},
        'warm_replay': {'status': 'PASS', 'database_delta': db_delta_warm},
    }

    rep['viewer'] = {
        'result': {
            'connected_distinct_viewers': 2,
            'unique_events': 20,
            'duplicates': 0,
            'wrong_namespace': 0,
        }
    }

    fanout_metrics = {
        'subscriptions_ready': 4,
        'writes': 20,
        'scan_evaluations': 0 if arm == 'optimized' else 80,
        'indexed_selections': 20 if arm == 'optimized' else 0,
    }
    rep['mechanisms'] = {
        'microbatch': {'groups': 5, 'members': 20},
        'ram_dedup': {
            'lookup_hit_total': 3 if arm == 'optimized' else 0,
            'database_calls_avoided_total': 3 if arm == 'optimized' else 0,
        },
        'shaper': {'rate_limited': 5 if arm == 'optimized' else 0},
        'fanout': fanout_metrics,
        'command': {
            'notification_signals_accepted': 4,
            'puback': 4,
            'scans': 20,
            'fresh_commands_dispatched': 4,
            'timeline_window': {'status': 'INCONCLUSIVE', 'reason': 'counter_field_missing_events'},
        },
    }

    init_after_copy = (initial_copy_before + 16) if arm == 'optimized' else initial_copy_after
    if counter_reset:
        init_after_copy = initial_copy_before - 2

    proc_after_init = (process_instance + '_mismatch') if process_mismatch else process_instance

    pers_init_before = {
        'single_attempts': 0, 'group_attempts': 0, 'group_members': 0, 'group_rows': 0, 'group_payload_bytes': 0,
        'fallback_attempts': 0, 'begin_attempts': initial_copy_before, 'begin_succeeded': initial_copy_before,
        'confirmed_commits': initial_copy_before, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
        'commit_unconfirmed': 0, 'copy_calls': initial_copy_before, 'copy_returned_rows': initial_copy_before * 100,
        'confirmed_new_rows': initial_copy_before * 100,
    }
    pers_init_after = {
        'single_attempts': 0, 'group_attempts': 16, 'group_members': 20, 'group_rows': 2000, 'group_payload_bytes': 21172,
        'fallback_attempts': 0, 'begin_attempts': init_after_copy, 'begin_succeeded': init_after_copy,
        'confirmed_commits': init_after_copy, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
        'commit_unconfirmed': 0, 'copy_calls': init_after_copy, 'copy_returned_rows': init_after_copy * 100,
        'confirmed_new_rows': init_after_copy * 100,
    }

    warm_after_att = (warm_att_before + 0) if arm == 'optimized' else warm_att_after
    pers_warm_before = {
        'single_attempts': 0, 'group_attempts': 16, 'group_members': 20, 'group_rows': 2000, 'group_payload_bytes': 21172,
        'fallback_attempts': 0, 'begin_attempts': warm_att_before, 'begin_succeeded': warm_att_before,
        'confirmed_commits': warm_att_before, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
        'commit_unconfirmed': 0, 'copy_calls': 16, 'copy_returned_rows': 2000, 'confirmed_new_rows': 2000,
    }
    pers_warm_after = {
        'single_attempts': 0, 'group_attempts': 16, 'group_members': 20, 'group_rows': 2000, 'group_payload_bytes': 21172,
        'fallback_attempts': 0, 'begin_attempts': warm_after_att, 'begin_succeeded': warm_after_att,
        'confirmed_commits': warm_after_att, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
        'commit_unconfirmed': 0, 'copy_calls': 16, 'copy_returned_rows': 2000, 'confirmed_new_rows': 2000,
    }

    rep['phase_windows'] = {
        'initial': {
            'admin_before': {'process_instance': process_instance, 'persistence': pers_init_before},
            'admin_after': {'process_instance': proc_after_init, 'persistence': pers_init_after},
            'ingress_delta': {'delta': {'Committed': 20, 'DuplicateCommitted': 0, 'SamplesNew': 2000}},
        },
        'warm_replay': {
            'admin_before': {'process_instance': process_instance, 'persistence': pers_warm_before},
            'admin_after': {'process_instance': process_instance, 'persistence': pers_warm_after},
            'ingress_delta': {'delta': {'Committed': 0, 'DuplicateCommitted': 0, 'SamplesNew': 0}},
        },
    }
    rep['accounting'] = {
        'ingress_delta': rep['phase_windows']['initial']['ingress_delta'],
        'imported_input': 20, 'warm_replay': 10,
        'command_receipts_verified': 4,
        'command_configurations_verified': 2,
    }
    rep['control_run'] = {'status': 'COMPLETE'}
    return rep


def sample_remote_e5_report(
    arm: str,
    *,
    seed: int = 805,
    paperload_binary_sha256: str = '8' * 64,
    gateway_binary_sha256: str = '9' * 64,
    combined_binary_sha256: str = '7' * 64,
    declared_windows: Optional[Dict[str, float]] = None,
    observed_windows: Optional[Dict[str, float]] = None,
    contradictory_canonical_binary: Optional[str] = None,
    fanout_readiness_verified: bool = True,
) -> Dict[str, Any]:
    rep = sample_e5_report(
        arm,
        seed=seed,
        gateway_binary_sha256=gateway_binary_sha256,
        paperload_binary_sha256=paperload_binary_sha256,
        combined_binary_sha256=combined_binary_sha256,
    )
    rep.pop('paperload_binary_sha256', None)
    rep.pop('gateway_binary_sha256', None)
    rep['metadata'].pop('binary_sha256', None)
    if contradictory_canonical_binary:
        rep['metadata']['binary_sha256'] = contradictory_canonical_binary

    rep['metadata']['paperload_binary_sha256'] = paperload_binary_sha256
    rep['metadata']['gateway_binary_sha256'] = gateway_binary_sha256
    rep['metadata']['combined_binary_sha256'] = combined_binary_sha256

    if declared_windows is None:
        declared_windows = {'initial': 1.0, 'warm_replay': 2.0, 'control': 8.0}
    if observed_windows is None:
        variance = 0.0 if arm == 'optimized' else 0.05
        observed_windows = {
            'initial': 4.137 + variance,
            'warm_replay': 2.083 + variance,
            'control': 12.72 + variance,
        }

    rep['metadata']['b_declared'] = {
        'declared_measurement_windows': declared_windows,
        'observed_measurement_windows': observed_windows,
        'paperload_binary_sha256': paperload_binary_sha256,
        'gateway_binary_sha256': gateway_binary_sha256,
        'combined_binary_sha256': combined_binary_sha256,
    }
    rep['metadata']['phase_seconds'] = observed_windows
    if fanout_readiness_verified:
        rep['mechanisms']['fanout']['readiness_verified'] = True
        rep['mechanism_metrics']['fanout']['readiness_verified'] = True
    rep['accounting']['ingress_delta']['delta']['ShaperRateLimited'] = 24 if arm == 'optimized' else 0
    rep['accounting']['ingress_delta']['delta']['ShaperPassed'] = 20
    rep['mechanisms']['microbatch']['max_members'] = 2 if arm == 'optimized' else 0
    rep['mechanism_metrics']['microbatch']['max_members'] = 2 if arm == 'optimized' else 0

    return rep


class CliInterfaceTests(unittest.TestCase):
    def test_cli_preflight_arm(self):
        import io
        import contextlib
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            ret = p8c.main(['preflight-arm', '--arm', 'optimized'])
        self.assertEqual(ret, 0)
        self.assertIn('"arm": "optimized"', f.getvalue())

    def test_cli_plan(self):
        import io
        import contextlib
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            ret = p8c.main(['plan', '--arms', 'optimized,shaper_off', '--repeats', '2', '--seed', '10'])
        self.assertEqual(ret, 0)
        self.assertIn('"master_seed": 10', f.getvalue())

    def test_cli_compare_success_and_failure(self):
        import tempfile
        import io
        import contextlib
        with tempfile.TemporaryDirectory() as td:
            ref_path = Path(td) / 'ref.json'
            arm_path = Path(td) / 'arm.json'
            ref_data = sample_report(arm='optimized')
            arm_data = sample_report(arm='shaper_off')
            ref_path.write_text(json.dumps(ref_data))
            arm_path.write_text(json.dumps(arm_data))

            f_ok = io.StringIO()
            with contextlib.redirect_stdout(f_ok):
                ret = p8c.main(['compare', '--ref', str(ref_path), '--arm-file', str(arm_path), '--expected-arm', 'shaper_off'])
            self.assertEqual(ret, 0)
            self.assertIn('"status": "COMPARABLE"', f_ok.getvalue())

            # Mismatched expected arm
            f_fail = io.StringIO()
            with contextlib.redirect_stdout(f_fail):
                ret_fail = p8c.main(['compare', '--ref', str(ref_path), '--arm-file', str(arm_path), '--expected-arm', 'microbatch_off'])
            self.assertEqual(ret_fail, 1)
            self.assertIn('"status": "REJECTED"', f_fail.getvalue())

    def test_e5_summary_self_contained_fixtures_and_csv_roundtrip(self):
        import tempfile
        import csv
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                r = sample_e5_report(
                    arm,
                    seed=999,
                    initial_copy_before=5,
                    initial_copy_after=25,
                    warm_att_before=16,
                    warm_att_after=26,
                )
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            summary = p8c.build_e5_summary(paths)
            self.assertEqual(summary['status'], 'PILOT_RUNS_COMPLETE_QUALIFIED')
            self.assertFalse(summary['full_p8_accepted'])
            # Derived seed from metadata, not 805
            self.assertEqual(summary['source_recorded_by_runs']['seed'], 999)
            # Context SHA is unavailable honestly when not supplied in raw schema
            self.assertIsNone(summary['source_recorded_by_runs']['context_sha256'])
            self.assertEqual(summary['source_recorded_by_runs']['context_sha256_status'], 'UNAVAILABLE')
            self.assertEqual(summary['source_recorded_by_runs']['context_sha256_provenance'], 'unavailable_not_in_raw_schema')

            # Command timeline status is INCONCLUSIVE, no false zero claims
            for row in summary['runs']:
                self.assertEqual(row['command_timeline_status'], 'INCONCLUSIVE')
                self.assertEqual(row['command_timeline_reason'], 'counter_field_missing_events')
                self.assertIsNone(row['timeline_dispatch_count'])
                self.assertIsNone(row['timeline_e2e_count'])

            # Microbatch pair: measured deltas (25-5=20 vs 21-5=16 => 20.0%)
            micro = next(p for p in summary['pairs'] if p['arm'] == 'microbatch_off')
            self.assertEqual(micro['ablation_copy_calls'], 20)
            self.assertEqual(micro['optimized_copy_calls'], 16)
            self.assertTrue(micro['gamma_copy_calls']['available'])
            self.assertAlmostEqual(micro['gamma_copy_calls']['gamma_red_percent'], 20.0)
            self.assertEqual(micro['ablation_confirmed_commits'], 20)
            self.assertEqual(micro['optimized_confirmed_commits'], 16)
            self.assertTrue(micro['gamma_confirmed_commits']['available'])
            self.assertAlmostEqual(micro['gamma_confirmed_commits']['gamma_red_percent'], 20.0)
            self.assertEqual(micro['ablation_begin_attempts'], 20)
            self.assertEqual(micro['optimized_begin_attempts'], 16)
            self.assertTrue(micro['gamma_begin_attempts']['available'])
            self.assertAlmostEqual(micro['gamma_begin_attempts']['gamma_red_percent'], 20.0)

            # RAM warm replay: measured deltas (26-16=10 vs 16-16=0 => 100.0%)
            ram = next(p for p in summary['pairs'] if p['arm'] == 'ram_dedup_off')
            self.assertEqual(ram['ablation_warm_attempts'], 10)
            self.assertEqual(ram['optimized_warm_attempts'], 0)
            self.assertTrue(ram['gamma_warm_attempts']['available'])
            self.assertAlmostEqual(ram['gamma_warm_attempts']['gamma_red_percent'], 100.0)
            self.assertEqual(ram['ablation_warm_confirmed_commits'], 10)
            self.assertEqual(ram['optimized_warm_confirmed_commits'], 0)
            self.assertTrue(ram['gamma_warm_commits']['available'])
            self.assertAlmostEqual(ram['gamma_warm_commits']['gamma_red_percent'], 100.0)

            # Fanout activation INCONCLUSIVE (readiness_verified absent)
            fan = next(p for p in summary['pairs'] if p['arm'] == 'fanout_scan')
            self.assertEqual(fan['activation_status'], 'INCONCLUSIVE')

            # CSV roundtrip export
            out_dir = td_path / 'export'
            p8c.export_e5_summary(paths, out_dir)
            self.assertTrue((out_dir / 'e5-summary.json').is_file())
            self.assertTrue((out_dir / 'e5-runs.csv').is_file())
            self.assertTrue((out_dir / 'e5-pairs.csv').is_file())

            # Read back e5-pairs.csv
            with (out_dir / 'e5-pairs.csv').open(newline='') as f:
                reader = csv.DictReader(f)
                pair_rows = list(reader)
            self.assertEqual(len(pair_rows), 5)
            # Verify microbatch row in CSV retains 20/16/20%
            csv_micro = next(r for r in pair_rows if r['arm'] == 'microbatch_off')
            self.assertEqual(int(csv_micro['ablation_copy_calls']), 20)
            self.assertEqual(int(csv_micro['optimized_copy_calls']), 16)
            g_copy_data = json.loads(csv_micro['gamma_copy_calls'])
            self.assertTrue(g_copy_data['available'])
            self.assertAlmostEqual(g_copy_data['gamma_red_percent'], 20.0)

            # Verify RAM row in CSV retains 10/0/100%
            csv_ram = next(r for r in pair_rows if r['arm'] == 'ram_dedup_off')
            self.assertEqual(int(csv_ram['ablation_warm_attempts']), 10)
            self.assertEqual(int(csv_ram['optimized_warm_attempts']), 0)
            g_warm_data = json.loads(csv_ram['gamma_warm_attempts'])
            self.assertTrue(g_warm_data['available'])
            self.assertAlmostEqual(g_warm_data['gamma_red_percent'], 100.0)

            # Check no private path leaks
            encoded = (out_dir / 'e5-summary.json').read_text()
            for denied in ('message_id', 'payload', 'jwt', '/var/tmp/opencode/p8/', 'endpoint'):
                self.assertNotIn(denied, encoded)

    def test_e5_summary_nested_secret_field_exclusion(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                r = sample_e5_report(arm, include_extra_secrets=True)
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            summary = p8c.build_e5_summary(paths)
            for run_row in summary['runs']:
                self.assertNotIn('secret_key', run_row['cleanup_counts'])
                self.assertNotIn('admin_password', run_row['cleanup_counts'])
                self.assertNotIn('injected_secret', run_row['image_ids'])
                self.assertNotIn('sql_password', run_row['initial_persistence_delta'])

    def test_e5_summary_process_instance_mismatch_unavailable(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                r = sample_e5_report(arm, process_mismatch=(arm == 'microbatch_off'))
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            summary = p8c.build_e5_summary(paths)
            micro = next(p for p in summary['pairs'] if p['arm'] == 'microbatch_off')
            self.assertFalse(micro['gamma_copy_calls']['available'])
            self.assertEqual(micro['gamma_copy_calls']['reason'], 'process_instance_mismatch_or_missing')

    def test_e5_summary_counter_reset_unavailable(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                r = sample_e5_report(arm, counter_reset=(arm == 'microbatch_off'))
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            summary = p8c.build_e5_summary(paths)
            micro = next(p for p in summary['pairs'] if p['arm'] == 'microbatch_off')
            self.assertFalse(micro['gamma_copy_calls']['available'])
            self.assertEqual(micro['gamma_copy_calls']['reason'], 'counter_reset')

    def test_e5_summary_gateway_mismatch_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                gw_hash = ('0' * 64) if arm == 'shaper_off' else ('9' * 64)
                r = sample_e5_report(arm, gateway_binary_sha256=gw_hash)
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            with self.assertRaises(ValueError) as ctx:
                p8c.build_e5_summary(paths)
            self.assertIn('gateway_binary_sha256_mismatch', str(ctx.exception))

    def test_e5_summary_combined_binary_mismatch_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paths = {}
            for arm in p8c.ARM_NAMES:
                bin_hash = ('0' * 64) if arm == 'shaper_off' else ('7' * 64)
                r = sample_e5_report(arm, combined_binary_sha256=bin_hash)
                p = td_path / f'p8-e5-{arm}.json'
                p.write_text(json.dumps(r))
                paths[arm] = p

            with self.assertRaises(ValueError) as ctx:
                p8c.build_e5_summary(paths)
            self.assertIn('binary_fingerprint_mismatch', str(ctx.exception))

    def test_e5_summary_preserves_input_bytes_and_safe_pair_values(self):
        import tempfile
        import hashlib
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name)
        paths = {arm: base / f'{arm}.json' for arm in p8c.ARM_NAMES}
        for arm, path in paths.items():
            path.write_text(json.dumps(sample_e5_report(arm)))
        before = {a: hashlib.sha256(p.read_bytes()).hexdigest() for a,p in paths.items()}
        summary = p8c.build_e5_summary(paths)
        self.assertEqual(summary['status'], 'PILOT_RUNS_COMPLETE_QUALIFIED')
        self.assertFalse(summary['full_p8_accepted'])
        fan = next(p for p in summary['pairs'] if p['arm'] == 'fanout_scan')
        self.assertEqual(fan['activation_status'], 'INCONCLUSIVE')
        self.assertEqual(fan['activation_reason'], 'fanout_viewer_readiness_unverified')
        micro = next(p for p in summary['pairs'] if p['arm'] == 'microbatch_off')
        self.assertAlmostEqual(micro['gamma_copy_calls']['gamma_red_percent'], 20.0)
        self.assertEqual(micro['raw_samples_retained_ablation'], 2000)
        ram = next(p for p in summary['pairs'] if p['arm'] == 'ram_dedup_off')
        self.assertAlmostEqual(ram['gamma_warm_transactions']['gamma_red_percent'], 100.0)
        self.assertEqual(ram['raw_samples_retained_ablation'], 0)
        self.assertTrue(all(hashlib.sha256(p.read_bytes()).hexdigest() == before[a] for a,p in paths.items()))
        encoded = json.dumps(summary)
        for denied in ('message_id', 'payload', 'jwt', '/var/tmp/opencode/p8/', 'endpoint'):
            self.assertNotIn(denied, encoded)
        with tempfile.TemporaryDirectory() as td:
            p8c.export_e5_summary(paths, td)
            self.assertTrue((Path(td) / 'e5-summary.json').is_file())

    def test_e5_summary_fails_closed_on_missing_arm(self):
        with self.assertRaises(ValueError):
            p8c.build_e5_summary({'optimized':'missing.json'})

    def test_remote_e5_summary_and_cli_lifecycle(self):
        import tempfile
        import io
        import contextlib
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        td_path = Path(td.name)
        paths = {}
        for arm in p8c.ARM_NAMES:
            r = sample_remote_e5_report(arm, seed=805)
            p = td_path / f'p8-e5-{arm}.json'
            p.write_text(json.dumps(r))
            paths[arm] = p

        # 1. Verify validate_run_comparability on all 5 pairs
        opt_report = json.loads(paths['optimized'].read_text())
        for arm in p8c.ARM_NAMES[1:]:
            arm_report = json.loads(paths[arm].read_text())
            comp = p8c.validate_run_comparability(opt_report, arm_report, arm)
            self.assertTrue(comp['comparable'], f'{arm} comparability failed: {comp.get("reason")}')
            self.assertIsNone(comp['reason'])
            act = p8c.check_mechanism_activation(arm, opt_report, arm_report)
            self.assertEqual(act['status'], 'ELIGIBLE', f'{arm} activation failed: {act}')

        # 2. Test CLI compare
        f_cmp = io.StringIO()
        with contextlib.redirect_stdout(f_cmp):
            rc = p8c.main([
                'compare',
                '--ref', str(paths['optimized']),
                '--arm-file', str(paths['shaper_off']),
                '--expected-arm', 'shaper_off',
            ])
        self.assertEqual(rc, 0)
        self.assertIn('"status": "COMPARABLE"', f_cmp.getvalue())

        # 3. Test build_e5_summary
        summary = p8c.build_e5_summary(paths, private_context='2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175')
        self.assertEqual(summary['status'], 'PILOT_RUNS_COMPLETE_QUALIFIED')
        self.assertEqual(summary['run_count'], 6)
        self.assertEqual(len(summary['pairs']), 5)
        for pair in summary['pairs']:
            self.assertEqual(pair['comparability'], 'COMPARABLE')
            self.assertEqual(pair['activation_status'], 'ELIGIBLE')

        # 4. Test CLI e5-summary export
        out_dir = td_path / 'summary_export'
        f_sum = io.StringIO()
        with contextlib.redirect_stdout(f_sum):
            rc = p8c.main([
                'e5-summary',
                '--optimized', str(paths['optimized']),
                '--shaper-off', str(paths['shaper_off']),
                '--microbatch-off', str(paths['microbatch_off']),
                '--ram-dedup-off', str(paths['ram_dedup_off']),
                '--polling', str(paths['polling']),
                '--fanout-scan', str(paths['fanout_scan']),
                '--output-dir', str(out_dir),
                '--private-context', '2411c01b9ea1be35ddb44ee449b311e8460f99cbdfe01fa2d41a335bd1500175',
            ])
        self.assertEqual(rc, 0)
        self.assertTrue((out_dir / 'e5-summary.json').is_file())
        self.assertTrue((out_dir / 'e5-runs.csv').is_file())
        self.assertTrue((out_dir / 'e5-pairs.csv').is_file())

    def test_remote_e5_negative_cases(self):
        # 1. Missing constituent binary fails (one-sided or both missing)
        ref = sample_remote_e5_report('optimized')
        arm = sample_remote_e5_report('shaper_off')
        arm_no_pl = copy.deepcopy(arm)
        arm_no_pl['metadata'].pop('paperload_binary_sha256')
        res = p8c.validate_run_comparability(ref, arm_no_pl, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'missing_fingerprint_paperload_binary_sha256')

        ref_no_both = copy.deepcopy(ref)
        arm_no_both = copy.deepcopy(arm)
        ref_no_both['metadata'].pop('paperload_binary_sha256', None)
        ref_no_both['metadata'].pop('gateway_binary_sha256', None)
        arm_no_both['metadata'].pop('paperload_binary_sha256', None)
        arm_no_both['metadata'].pop('gateway_binary_sha256', None)
        res_both_missing = p8c.validate_run_comparability(ref_no_both, arm_no_both, 'shaper_off')
        self.assertFalse(res_both_missing['comparable'])
        self.assertEqual(res_both_missing['reason'], 'missing_fingerprint_paperload_binary_sha256')

        ref_no_gw = copy.deepcopy(ref)
        arm_no_gw = copy.deepcopy(arm)
        ref_no_gw['metadata'].pop('gateway_binary_sha256', None)
        arm_no_gw['metadata'].pop('gateway_binary_sha256', None)
        res_gw_missing = p8c.validate_run_comparability(ref_no_gw, arm_no_gw, 'shaper_off')
        self.assertFalse(res_gw_missing['comparable'])
        self.assertEqual(res_gw_missing['reason'], 'missing_fingerprint_gateway_binary_sha256')

        # 2. Mismatched constituent binary fails
        arm_bad_gw = copy.deepcopy(arm)
        arm_bad_gw['metadata']['gateway_binary_sha256'] = '0' * 64
        res = p8c.validate_run_comparability(ref, arm_bad_gw, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'gateway_binary_sha256_mismatch')

        # 3. Contradictory combined alias fails
        arm_contra = sample_remote_e5_report('shaper_off', contradictory_canonical_binary='1' * 64)
        res = p8c.validate_run_comparability(ref, arm_contra, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'contradictory_binary_fingerprint')

        # 4. Changed declared measurement window fails
        arm_diff_decl = sample_remote_e5_report('shaper_off', declared_windows={'initial': 2.0, 'warm_replay': 2.0, 'control': 8.0})
        res = p8c.validate_run_comparability(ref, arm_diff_decl, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'window_duration_mismatch')

        # 5. Invalid declared measurement window formats fail (non-finite, bool, partial, extra keys, negative)
        arm_nan = sample_remote_e5_report('shaper_off', declared_windows={'initial': float('nan'), 'warm_replay': 2.0, 'control': 8.0})
        res = p8c.validate_run_comparability(ref, arm_nan, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertEqual(res['reason'], 'invalid_fingerprint_phase_seconds')

        ref_bool = copy.deepcopy(ref)
        arm_bool = copy.deepcopy(arm)
        ref_bool['metadata']['b_declared']['declared_measurement_windows'] = {'initial': True, 'warm_replay': 2.0, 'control': 8.0}
        arm_bool['metadata']['b_declared']['declared_measurement_windows'] = {'initial': 1.0, 'warm_replay': 2.0, 'control': 8.0}
        res_bool = p8c.validate_run_comparability(ref_bool, arm_bool, 'shaper_off')
        self.assertFalse(res_bool['comparable'])
        self.assertEqual(res_bool['reason'], 'invalid_fingerprint_phase_seconds')

        ref_part = copy.deepcopy(ref)
        arm_part = copy.deepcopy(arm)
        ref_part['metadata']['b_declared']['declared_measurement_windows'] = {'initial': 1.0, 'control': 8.0}
        arm_part['metadata']['b_declared']['declared_measurement_windows'] = {'initial': 1.0, 'control': 8.0}
        res_part = p8c.validate_run_comparability(ref_part, arm_part, 'shaper_off')
        self.assertFalse(res_part['comparable'])
        self.assertEqual(res_part['reason'], 'invalid_fingerprint_phase_seconds')

        ref_extra = copy.deepcopy(ref)
        arm_extra = copy.deepcopy(arm)
        ref_extra['metadata']['b_declared']['declared_measurement_windows'] = {'initial': 1.0, 'warm_replay': 2.0, 'control': 8.0, 'extra': 1.0}
        arm_extra['metadata']['b_declared']['declared_measurement_windows'] = {'initial': 1.0, 'warm_replay': 2.0, 'control': 8.0, 'extra': 1.0}
        res_extra = p8c.validate_run_comparability(ref_extra, arm_extra, 'shaper_off')
        self.assertFalse(res_extra['comparable'])
        self.assertEqual(res_extra['reason'], 'invalid_fingerprint_phase_seconds')

        ref_neg = copy.deepcopy(ref)
        arm_neg = copy.deepcopy(arm)
        ref_neg['metadata']['b_declared']['declared_measurement_windows'] = {'initial': -1.0, 'warm_replay': 2.0, 'control': 8.0}
        arm_neg['metadata']['b_declared']['declared_measurement_windows'] = {'initial': -1.0, 'warm_replay': 2.0, 'control': 8.0}
        res_neg = p8c.validate_run_comparability(ref_neg, arm_neg, 'shaper_off')
        self.assertFalse(res_neg['comparable'])
        self.assertEqual(res_neg['reason'], 'invalid_fingerprint_phase_seconds')

        # 6. Differing observed durations are accepted while declared windows match
        arm_varied_obs = sample_remote_e5_report('shaper_off', observed_windows={'initial': 4.999, 'warm_replay': 3.123, 'control': 15.001})
        res = p8c.validate_run_comparability(ref, arm_varied_obs, 'shaper_off')
        self.assertTrue(res['comparable'])
        self.assertIsNone(res['reason'])

        # 7. Wrong one-factor delta fails
        arm_wrong_flag = copy.deepcopy(arm)
        arm_wrong_flag['metadata']['effective_flags']['MQTT_SHAPER_ENABLED'] = 'true'
        res = p8c.validate_run_comparability(ref, arm_wrong_flag, 'shaper_off')
        self.assertFalse(res['comparable'])
        self.assertIn(res['reason'], ('ablation_flags_invalid', 'ablation_diff_mismatch'))


if __name__ == '__main__':
    unittest.main()
