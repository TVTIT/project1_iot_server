"""Tests for ES.6b: compare-e2 read-only comparison."""
import hashlib
import json
import math
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts.tests import paper_p8_e2_support as e2
from scripts import paper_p8_server as server


def _make_context(anchor_dt=None):
    if anchor_dt is None:
        anchor_dt = datetime(2026, 10, 31, 0, 0, 0, tzinfo=timezone.utc)
    return e2.build_e2_equal_samples_context(
        total_samples=6000,
        sample_rate=100,
        seed=84,
        timestamp_anchor=anchor_dt.isoformat().replace('+00:00', 'Z'),
        max_context_age_seconds=86400,
        now_utc=anchor_dt + timedelta(seconds=60),
    )


def _make_report(td_path, *, samples_per_message, context=None,
                 binary_sha256=None, image_ids=None,
                 status='PASS', cleanup_verified=True, full_p8_accepted=False,
                 effective_flags=None, quota=None, logical_topology=None,
                 workers=None, source_fingerprint=None, schema=None,
                 clean_comparison=True, contamination_reasons=None,
                 normalized_metrics=None, validation_failures=None):
    if context is None:
        context = _make_context()
    if binary_sha256 is None:
        binary_sha256 = 'b' * 64
    if image_ids is None:
        image_ids = {
            'backend': 'sha256:' + '1' * 64,
            'gateway': 'sha256:' + '2' * 64,
            'postgres': 'sha256:' + '3' * 64,
            'mosquitto': 'sha256:' + '4' * 64,
            'nginx': 'sha256:' + '5' * 64,
        }
    if effective_flags is None:
        effective_flags = {
            'STAGE5_ENABLED': False,
            'STAGE5_WAKEUP_ENABLED': False,
            'WEBSOCKET_ENABLED': False,
            'MQTT_SHAPER_ENABLED': False,
            'MQTT_MICROBATCH_ENABLED': True,
            'MQTT_RAM_DEDUP_ENABLED': False,
        }
    if quota is None:
        quota = {'rate_per_second': 2, 'burst': 4}
    if logical_topology is None:
        logical_topology = {'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}
    if workers is None:
        workers = {'workers': 2, 'connections': 1, 'pending_messages': 64}
    if source_fingerprint is None:
        source_fingerprint = {
            'revision': 'r' * 40,
            'branch': 'feat/paper-requirement',
            'dirty': True,
            'dirty_source_sha256': 'd' * 64,
        }
    if schema is None:
        schema = '24:telemetry_ledger_timestamp'
    if contamination_reasons is None:
        contamination_reasons = []
    if validation_failures is None:
        validation_failures = []
    if normalized_metrics is None:
        if samples_per_message == 1:
            normalized_metrics = {
                'actual_samples': 6000,
                'bytes_per_sample': 100.0,
                'db_transactions_per_sample': 1.0,
                'active_cpu_seconds_per_sample': 0.002,
                'lifecycle_cpu_seconds_per_sample': 0.005,
                'active_cpu_available': True,
                'active_cpu_unavailable_reason': None,
                'lifecycle_cpu_available': True,
                'lifecycle_cpu_unavailable_reason': None,
                'generator_active_cpu_available': True,
                'generator_active_cpu_unavailable_reason': None,
                'generator_active_cpu_seconds_per_sample': 0.0004,
                'generator_lifecycle_cpu_available': True,
                'generator_lifecycle_cpu_unavailable_reason': None,
                'generator_lifecycle_cpu_seconds_per_sample': 0.0008,
            }
        else:
            normalized_metrics = {
                'actual_samples': 6000,
                'bytes_per_sample': 50.0,
                'db_transactions_per_sample': 0.01,
                'active_cpu_seconds_per_sample': 0.001,
                'lifecycle_cpu_seconds_per_sample': 0.003,
                'active_cpu_available': True,
                'active_cpu_unavailable_reason': None,
                'lifecycle_cpu_available': True,
                'lifecycle_cpu_unavailable_reason': None,
                'generator_active_cpu_available': True,
                'generator_active_cpu_unavailable_reason': None,
                'generator_active_cpu_seconds_per_sample': 0.0002,
                'generator_lifecycle_cpu_available': True,
                'generator_lifecycle_cpu_unavailable_reason': None,
                'generator_lifecycle_cpu_seconds_per_sample': 0.0004,
            }

    expected_counts = e2.calculate_e2_equal_samples_expected_counts(context, samples_per_message)
    expected_msgs_per_role = expected_counts['per_role']['A']['messages']

    try:
        p_bytes = int(normalized_metrics.get('bytes_per_sample', 100) * 3000)
    except Exception:
        p_bytes = 300000

    actual_counters = {
        'A': {
            'generated_unique': expected_msgs_per_role,
            'offered_unique': expected_msgs_per_role,
            'application_acks': expected_msgs_per_role,
            'pending_messages': 0,
            'pending_bytes': 0,
            'payload_bytes': p_bytes,
        },
        'B': {
            'generated_unique': expected_msgs_per_role,
            'offered_unique': expected_msgs_per_role,
            'application_acks': expected_msgs_per_role,
            'pending_messages': 0,
            'pending_bytes': 0,
            'payload_bytes': p_bytes,
        },
    }

    raw_ver = {
        'A': {'raw_identity_value_mismatches': 0, 'timestamp_mismatches': 0, 'sequence_gaps_or_overlaps': 0, 'provenance_mismatches': 0},
        'B': {'raw_identity_value_mismatches': 0, 'timestamp_mismatches': 0, 'sequence_gaps_or_overlaps': 0, 'provenance_mismatches': 0},
    }

    is_valid = (status == 'PASS' and len(validation_failures) == 0)

    report = {
        'schema_version': 1,
        'status': status,
        'experiment': 'E2',
        'workload': 'equal_samples',
        'workload_type': 'equal_samples',
        'samples_per_message': samples_per_message,
        'context': context,
        'context_sha256': context['context_sha256'],
        'seed': context['seed'],
        'source_fingerprint': source_fingerprint,
        'schema': schema,
        'image_ids': image_ids,
        'images': {k: {'id': v} for k, v in image_ids.items()},
        'binary_sha256': binary_sha256,
        'paperload_binary_sha256': binary_sha256,
        'binary_path': 'bin/paperload',
        'effective_flags': effective_flags,
        'quota': quota,
        'logical_topology': logical_topology,
        'workers': workers,
        'cleanup_verified': cleanup_verified,
        'exact_cleanup_verified': cleanup_verified,
        'cleanup_provenance': {'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': {'containers': 5}},
        'full_p8_accepted': full_p8_accepted,
        'validation': {
            'valid': is_valid,
            'failures': validation_failures,
            'clean_comparison': clean_comparison and not contamination_reasons,
            'contamination_reasons': list(contamination_reasons),
        },
        'workload_outcome': 'VALIDATED' if is_valid else 'VALIDATION_FAILED',
        'expected_counts': expected_counts,
        'actual_counters': actual_counters,
        'raw_verification': raw_ver,
        'measurement_quality': {
            'availability': 'available',
            'normalized_metrics': normalized_metrics,
        },
        'normalized_metrics': normalized_metrics,
        'clean_comparison': clean_comparison and not contamination_reasons,
        'contamination_reasons': list(contamination_reasons),
    }

    out_file = td_path / f'p8-e2-equal-samples-{samples_per_message}-remote-result.json'
    out_file.write_text(json.dumps(report, indent=2), encoding='utf-8')
    out_file.chmod(0o600)
    return out_file, report


class E2CompareTests(unittest.TestCase):
    def test_compare_valid_reports_both_orders(self):
        """Valid reports in both orders produce identical ratios and comparable verdict."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, r1 = _make_report(td_path, samples_per_message=1)
            f100, r100 = _make_report(td_path, samples_per_message=100)

            # Order 1: (1, 100)
            res1 = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res1['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res1['comparable'])
            self.assertEqual(res1['mismatch_reasons'], [])
            self.assertFalse(res1['workload_contamination']['contaminated'])
            self.assertTrue(res1['workload_contamination']['clean_comparison'])
            self.assertNotIn('winner', res1)
            self.assertFalse(res1['qualifications']['full_p8_accepted'])

            ratios1 = res1['ratios_100_over_1']
            self.assertEqual(ratios1['bytes_per_sample']['ratio'], 0.5)
            self.assertEqual(ratios1['db_transactions_per_sample']['ratio'], 0.01)
            self.assertEqual(ratios1['active_cpu_seconds_per_sample']['ratio'], 0.5)
            self.assertEqual(ratios1['lifecycle_cpu_seconds_per_sample']['ratio'], 0.6)

            # Order 2: (100, 1)
            res2 = e2.compare_e2_reports(r100, r1)
            self.assertEqual(res2['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res2['comparable'])
            self.assertEqual(res2['mismatch_reasons'], [])
            self.assertEqual(res2['ratios_100_over_1']['bytes_per_sample']['ratio'], 0.5)
            self.assertEqual(res2['ratios_100_over_1']['db_transactions_per_sample']['ratio'], 0.01)

    def test_compare_mismatch_binary(self):
        """Binary SHA256 mismatch yields NOT_COMPARABLE with exact reason."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1, binary_sha256='a' * 64)
            _, r100 = _make_report(td_path, samples_per_message=100, binary_sha256='b' * 64)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('binary_sha256_mismatch' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_image(self):
        """Docker image ID mismatch yields NOT_COMPARABLE with exact service name."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            imgs_a = {'backend': 'sha256:' + '1' * 64, 'gateway': 'sha256:' + '2' * 64,
                      'postgres': 'sha256:' + '3' * 64, 'mosquitto': 'sha256:' + '4' * 64, 'nginx': 'sha256:' + '5' * 64}
            imgs_b = dict(imgs_a)
            imgs_b['backend'] = 'sha256:' + '9' * 64
            _, r1 = _make_report(td_path, samples_per_message=1, image_ids=imgs_a)
            _, r100 = _make_report(td_path, samples_per_message=100, image_ids=imgs_b)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('image_id_mismatch:backend' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_context(self):
        """Context duration_seconds mismatch yields NOT_COMPARABLE with field name."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1)
            _, r100 = _make_report(td_path, samples_per_message=100)
            r100['context']['duration_seconds'] = 120

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('context' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_flags(self):
        """Effective flags mismatch yields NOT_COMPARABLE with flag name."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f_a = {'STAGE5_ENABLED': False, 'STAGE5_WAKEUP_ENABLED': False, 'WEBSOCKET_ENABLED': False,
                   'MQTT_SHAPER_ENABLED': False, 'MQTT_MICROBATCH_ENABLED': True, 'MQTT_RAM_DEDUP_ENABLED': False}
            f_b = dict(f_a)
            f_b['MQTT_MICROBATCH_ENABLED'] = False
            _, r1 = _make_report(td_path, samples_per_message=1, effective_flags=f_a)
            _, r100 = _make_report(td_path, samples_per_message=100, effective_flags=f_b)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('effective_flags_mismatch:MQTT_MICROBATCH_ENABLED' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_seed(self):
        """Seed mismatch yields NOT_COMPARABLE with seed_mismatch."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            c1 = _make_context()
            c2 = dict(c1)
            c2['seed'] = 999
            c2['context_sha256'] = e2.calculate_e2_equal_samples_context_sha256(c2)
            _, r1 = _make_report(td_path, samples_per_message=1, context=c1)
            _, r100 = _make_report(td_path, samples_per_message=100, context=c2)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('seed_mismatch' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_source(self):
        """Source fingerprint mismatch yields NOT_COMPARABLE with revision."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            s_a = {'revision': 'a' * 40, 'branch': 'feat/x', 'dirty': True, 'dirty_source_sha256': 'd' * 64}
            s_b = {'revision': 'b' * 40, 'branch': 'feat/x', 'dirty': True, 'dirty_source_sha256': 'd' * 64}
            _, r1 = _make_report(td_path, samples_per_message=1, source_fingerprint=s_a)
            _, r100 = _make_report(td_path, samples_per_message=100, source_fingerprint=s_b)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('source_fingerprint_mismatch:revision' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_schema(self):
        """Schema mismatch yields NOT_COMPARABLE."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1, schema='24:telemetry_ledger_timestamp')
            _, r100 = _make_report(td_path, samples_per_message=100, schema='23:something_else')

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('schema_mismatch' in r or 'schema_invalid' in r for r in res['mismatch_reasons']))

    def test_compare_mismatch_limits_and_topology(self):
        """Topology and workers mismatches yield NOT_COMPARABLE."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            t_a = {'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}
            t_b = dict(t_a, sensors=3)
            _, r1 = _make_report(td_path, samples_per_message=1, logical_topology=t_a)
            _, r100 = _make_report(td_path, samples_per_message=100, logical_topology=t_b)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('topology_mismatch:sensors' in r for r in res['mismatch_reasons']))

    def test_compare_missing_required_fields_not_equal_safe(self):
        """Missing keys never equal-safe by both returning None."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1)
            _, r100 = _make_report(td_path, samples_per_message=100)

            # Strip effective_flags from BOTH reports
            del r1['effective_flags']
            del r100['effective_flags']

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'NOT_COMPARABLE')
            self.assertFalse(res['comparable'])
            self.assertTrue(any('missing_required_field:effective_flags' in r for r in res['mismatch_reasons']))

    def test_compare_functional_failure(self):
        """Functional validation failure in a profile flags functional validity failure."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1, status='FAIL', validation_failures=['ingress_Committed'])
            _, r100 = _make_report(td_path, samples_per_message=100)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'FUNCTIONAL_FAILURE')
            self.assertFalse(res['functional_validity']['profile_1']['valid'])
            self.assertTrue(res['functional_validity']['profile_100']['valid'])

    def test_compare_cleanup_not_verified(self):
        """Unverified cleanup flags NOT_COMPARABLE or CLEANUP_NOT_VERIFIED."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1)
            _, r100 = _make_report(td_path, samples_per_message=100, cleanup_verified=False)
            r100['exact_cleanup_verified'] = False
            r100['cleanup_verified'] = False

            res = e2.compare_e2_reports(r1, r100)
            self.assertIn(res['status'], ('CLEANUP_NOT_VERIFIED', 'NOT_COMPARABLE'))
            self.assertFalse(res['comparable'])
            self.assertFalse(res['cleanup']['profile_100_verified'])

    def test_compare_contaminated_no_headline_speedup(self):
        """Contaminated run must have clean_comparison=False and no headline speedup."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            _, r1 = _make_report(td_path, samples_per_message=1,
                                 clean_comparison=False, contamination_reasons=['ingress_queue_full:3'])
            _, r100 = _make_report(td_path, samples_per_message=100)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'CONTAMINATED')
            self.assertTrue(res['workload_contamination']['contaminated'])
            self.assertFalse(res['workload_contamination']['clean_comparison'])
            self.assertNotIn('winner', res)
            self.assertIn('headline_speedup', res)
            self.assertFalse(res['headline_speedup']['available'])
            self.assertEqual(res['headline_speedup']['reason'], 'contaminated_run_no_headline_speedup')

    def test_compare_missing_cpu_preserves_payload_and_db(self):
        """Missing CPU metrics do not invalidate payload/DB transactions ratios."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            norm_1 = {
                'actual_samples': 6000,
                'bytes_per_sample': 100.0,
                'db_transactions_per_sample': 1.0,
                'active_cpu_seconds_per_sample': None,
                'active_cpu_available': False,
                'active_cpu_unavailable_reason': 'active_cpu_counters_unavailable',
                'lifecycle_cpu_seconds_per_sample': None,
                'lifecycle_cpu_available': False,
                'lifecycle_cpu_unavailable_reason': 'lifecycle_cpu_counters_unavailable',
            }
            norm_100 = {
                'actual_samples': 6000,
                'bytes_per_sample': 50.0,
                'db_transactions_per_sample': 0.01,
                'active_cpu_seconds_per_sample': None,
                'active_cpu_available': False,
                'active_cpu_unavailable_reason': 'active_cpu_counters_unavailable',
                'lifecycle_cpu_seconds_per_sample': None,
                'lifecycle_cpu_available': False,
                'lifecycle_cpu_unavailable_reason': 'lifecycle_cpu_counters_unavailable',
            }
            _, r1 = _make_report(td_path, samples_per_message=1, normalized_metrics=norm_1)
            _, r100 = _make_report(td_path, samples_per_message=100, normalized_metrics=norm_100)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res['comparable'])

            ratios = res['ratios_100_over_1']
            self.assertTrue(ratios['bytes_per_sample']['available'])
            self.assertEqual(ratios['bytes_per_sample']['ratio'], 0.5)

            self.assertTrue(ratios['db_transactions_per_sample']['available'])
            self.assertEqual(ratios['db_transactions_per_sample']['ratio'], 0.01)

            self.assertFalse(ratios['active_cpu_seconds_per_sample']['available'])
            self.assertIsNotNone(ratios['active_cpu_seconds_per_sample']['unavailable_reason'])

    def test_compare_zero_denominator(self):
        """Zero denominator yields unavailable ratio without ZeroDivisionError."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            norm_1 = {'actual_samples': 6000, 'bytes_per_sample': 0, 'db_transactions_per_sample': 1.0}
            norm_100 = {'actual_samples': 6000, 'bytes_per_sample': 50.0, 'db_transactions_per_sample': 0.01}
            _, r1 = _make_report(td_path, samples_per_message=1, normalized_metrics=norm_1)
            _, r100 = _make_report(td_path, samples_per_message=100, normalized_metrics=norm_100)

            res = e2.compare_e2_reports(r1, r100)
            ratio_bytes = res['ratios_100_over_1']['bytes_per_sample']
            self.assertFalse(ratio_bytes['available'])
            self.assertEqual(ratio_bytes['unavailable_reason'], 'zero_denominator')
            self.assertIsNone(ratio_bytes['ratio'])

    def test_compare_invalid_numeric_types(self):
        """NaN, Inf, negative, bool, and string are rejected for ratios."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            for bad_val, expected_reason in [
                (float('nan'), 'nan_or_inf'),
                (float('inf'), 'nan_or_inf'),
                (-10.0, 'negative_value'),
                (True, 'invalid_numeric_type_bool'),
                ("100", 'non_numeric_type'),
            ]:
                norm_1 = {'actual_samples': 6000, 'bytes_per_sample': bad_val, 'db_transactions_per_sample': 1.0}
                norm_100 = {'actual_samples': 6000, 'bytes_per_sample': 50.0, 'db_transactions_per_sample': 0.01}
                _, r1 = _make_report(td_path, samples_per_message=1, normalized_metrics=norm_1)
                _, r100 = _make_report(td_path, samples_per_message=100, normalized_metrics=norm_100)

                res = e2.compare_e2_reports(r1, r100)
                r_entry = res['ratios_100_over_1']['bytes_per_sample']
                self.assertFalse(r_entry['available'], f"Failed for {bad_val}")
                self.assertIn(expected_reason, r_entry['unavailable_reason'], f"Failed for {bad_val}")

    def test_compare_expired_historical_context_succeeds(self):
        """Historical context does not fail validation due to elapsed age."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            old_anchor = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            ctx = _make_context(old_anchor)
            _, r1 = _make_report(td_path, samples_per_message=1, context=ctx)
            _, r100 = _make_report(td_path, samples_per_message=100, context=ctx)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res['comparable'])

    def test_server_cli_output_collision_rejected(self):
        """Server compare-e2 rejects if destination output path already exists."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, _ = _make_report(td_path, samples_per_message=1)
            f100, _ = _make_report(td_path, samples_per_message=100)
            existing_out = td_path / 'existing.json'
            existing_out.write_text('{}')
            existing_out.chmod(0o600)

            with self.assertRaises(ValueError) as ctx:
                server.main(['compare-e2', '--report-1', str(f1), '--report-2', str(f100), '--output', str(existing_out)])
            self.assertIn('server_output_file_exists', str(ctx.exception))

    def test_server_cli_read_only_source_reports_unmodified(self):
        """Server compare-e2 strictly reads reports without modifying them."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, _ = _make_report(td_path, samples_per_message=1)
            f100, _ = _make_report(td_path, samples_per_message=100)
            h1_before = hashlib.sha256(f1.read_bytes()).hexdigest()
            h100_before = hashlib.sha256(f100.read_bytes()).hexdigest()

            out_file = td_path / 'comparison.json'
            rc = server.main(['compare-e2', '--report-1', str(f1), '--report-2', str(f100), '--output', str(out_file)])
            self.assertEqual(rc, 0)

            h1_after = hashlib.sha256(f1.read_bytes()).hexdigest()
            h100_after = hashlib.sha256(f100.read_bytes()).hexdigest()
            self.assertEqual(h1_before, h1_after)
            self.assertEqual(h100_before, h100_after)

            self.assertTrue(out_file.is_file())
            st = out_file.stat()
            self.assertEqual(st.st_mode & 0o077, 0)
            comp = json.loads(out_file.read_text(encoding='utf-8'))
            self.assertEqual(comp['status'], 'COMPARABLE_CLEAN')

    def test_server_cli_either_order_and_positional(self):
        """CLI works with either input order and positional arguments."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, _ = _make_report(td_path, samples_per_message=1)
            f100, _ = _make_report(td_path, samples_per_message=100)

            out_1 = td_path / 'comp_order_1.json'
            rc1 = server.main(['compare-e2', str(f100), str(f1), '--output', str(out_1)])
            self.assertEqual(rc1, 0)
            data1 = json.loads(out_1.read_text(encoding='utf-8'))
            self.assertEqual(data1['status'], 'COMPARABLE_CLEAN')
            self.assertEqual(data1['ratios_100_over_1']['bytes_per_sample']['ratio'], 0.5)

            out_2 = td_path / 'comp_order_2.json'
            rc2 = server.main(['compare-e2', '--report-1', str(f1), '--report-2', str(f100), '--output', str(out_2)])
            self.assertEqual(rc2, 0)
            data2 = json.loads(out_2.read_text(encoding='utf-8'))
            self.assertEqual(data2['status'], 'COMPARABLE_CLEAN')
            self.assertEqual(data2['ratios_100_over_1']['bytes_per_sample']['ratio'], 0.5)

    def test_compare_asymmetric_contamination_reasons(self):
        """Profile 100 retains its own contamination reasons, not profile 1's."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            reasons_1 = ['clean_comparison_false', 'A_retries_observed:11', 'B_retries_observed:10']
            reasons_100 = ['clean_comparison_false', 'A_retries_observed:1', 'B_retries_observed:1']
            f1, r1 = _make_report(td_path, samples_per_message=1, clean_comparison=False, contamination_reasons=reasons_1)
            f100, r100 = _make_report(td_path, samples_per_message=100, clean_comparison=False, contamination_reasons=reasons_100)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['workload_contamination']['profile_1']['contamination_reasons'], reasons_1)
            self.assertEqual(res['workload_contamination']['profile_100']['contamination_reasons'], reasons_100)

    def test_compare_per_metric_ratio_respects_evidence_not_numeric_presence(self):
        """Ratio must be unavailable if per-metric evidence is inconclusive, despite numbers."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, r1 = _make_report(td_path, samples_per_message=1)
            f100, r100 = _make_report(td_path, samples_per_message=100)

            # Invalidate db_transactions in profile 1
            r1['measurement_quality'] = {
                'per_metric': {
                    'db_transactions': {'status': 'INCONCLUSIVE', 'availability': 'inconclusive', 'reason': 'db_transactions_unavailable'},
                    'payload_bytes': {'status': 'AVAILABLE', 'availability': 'available', 'reason': None},
                }
            }
            r1['normalized_metrics']['db_transactions_available'] = False

            res = e2.compare_e2_reports(r1, r100)
            self.assertFalse(res['ratios_100_over_1']['db_transactions_per_sample']['available'])
            self.assertEqual(res['per_metric_quality']['db_transactions_per_sample']['status'], 'INCONCLUSIVE')

    def test_compare_headline_speedup_blocked_on_functional_invalid_or_unclean(self):
        """Headline speedup must be unavailable when either run is functionally invalid."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, r1 = _make_report(td_path, samples_per_message=1, validation_failures=['test_failure'])
            f100, r100 = _make_report(td_path, samples_per_message=100)

            res = e2.compare_e2_reports(r1, r100)
            self.assertFalse(res['headline_speedup']['available'])
            self.assertEqual(res['headline_speedup']['reason'], 'functional_invalid_no_headline_speedup')

    def test_compare_missing_raw_verification_role_fails(self):
        """Missing role B in raw_verification must fail functional validity."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, r1 = _make_report(td_path, samples_per_message=1)
            f100, r100 = _make_report(td_path, samples_per_message=100)

            # Remove role B from raw_verification in r1
            r1['raw_verification'] = {'A': {'raw_identity_value_mismatches': 0, 'timestamp_mismatches': 0, 'sequence_gaps_or_overlaps': 0, 'provenance_mismatches': 0}}

            res = e2.compare_e2_reports(r1, r100)
            self.assertFalse(res['functional_validity']['profile_1']['valid'])
            self.assertIn('raw_verification_missing_role_B', res['functional_validity']['profile_1']['failures'])

    def test_compare_missing_pending_counters_fails_no_silent_default(self):
        """Missing pending_messages in actual_counters must fail rather than default to 0."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            f1, r1 = _make_report(td_path, samples_per_message=1)
            f100, r100 = _make_report(td_path, samples_per_message=100)

            # Omit pending_messages in role A
            del r1['actual_counters']['A']['pending_messages']

            res = e2.compare_e2_reports(r1, r100)
            self.assertFalse(res['functional_validity']['profile_1']['valid'])
            self.assertIn('missing_pending_messages_role_A', res['functional_validity']['profile_1']['failures'])



if __name__ == '__main__':
    unittest.main()
