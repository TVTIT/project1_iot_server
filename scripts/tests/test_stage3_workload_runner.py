#!/usr/bin/env python3
"""Focused Unit Tests for Task 3.7.1-3.7.4 Workload Runner and Measurement Infrastructure.

Tests strict summary parsing, metric calculations, profile invariants, SQL assertion queries,
resource sampling bounds, and fail-closed shell wrapper behavior without executing the 600-batch workload.
"""

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import stage3_workload_runner as W
from stage3_uplink_runner import GatewaySpec, SENSOR_UNIT


class WorkloadSummaryParsingTests(unittest.TestCase):
    def test_parse_measurement_summary_complete(self):
        line = (
            "measurement summary: samples=600 p50_ms=12.34 p95_ms=25.67 min_ms=5.10 max_ms=89.00 "
            "first_publishes=600 retry_attempts=3 missing_origins=0 overflow_samples=0 "
            "gen_duration_ms=599120.00 publish_drain_duration_ms=599850.50 total_duration_ms=600100.00\n"
        )
        res = W.parse_measurement_summary(line)
        self.assertEqual(res['samples'], 600)
        self.assertEqual(res['first_publishes'], 600)
        self.assertEqual(res['retry_attempts'], 3)
        self.assertEqual(res['missing_origins'], 0)
        self.assertEqual(res['overflow_samples'], 0)
        self.assertAlmostEqual(res['p50_ms'], 12.34)
        self.assertAlmostEqual(res['p95_ms'], 25.67)
        self.assertAlmostEqual(res['min_ms'], 5.10)
        self.assertAlmostEqual(res['max_ms'], 89.00)
        self.assertAlmostEqual(res['gen_duration_ms'], 599120.0)
        self.assertAlmostEqual(res['publish_drain_duration_ms'], 599850.5)
        self.assertAlmostEqual(res['total_duration_ms'], 600100.0)

    def test_parse_measurement_summary_none_percentiles(self):
        line = (
            "measurement summary: samples=0 p50_ms=none p95_ms=none min_ms=none max_ms=none "
            "first_publishes=0 retry_attempts=0 missing_origins=0 overflow_samples=0 "
            "gen_duration_ms=0.00 publish_drain_duration_ms=0.00 total_duration_ms=10.00\n"
        )
        res = W.parse_measurement_summary(line)
        self.assertEqual(res['samples'], 0)
        self.assertEqual(res['p50_ms'], 'none')
        self.assertEqual(res['p95_ms'], 'none')
        self.assertEqual(res['first_publishes'], 0)

    def test_parse_measurement_summary_missing_metric(self):
        # Missing overflow_samples
        line = (
            "measurement summary: samples=600 p50_ms=12.34 p95_ms=25.67 min_ms=5.10 max_ms=89.00 "
            "first_publishes=600 retry_attempts=0 missing_origins=0 "
            "gen_duration_ms=599120.00 publish_drain_duration_ms=599850.50 total_duration_ms=600100.00\n"
        )
        self.assertEqual(W.parse_measurement_summary(line), {})

    def test_parse_measurement_summary_malformed_float(self):
        line = "measurement summary: samples=600 p50_ms=not_a_float p95_ms=25.67 ...\n"
        self.assertEqual(W.parse_measurement_summary(line), {})

    def test_parse_measurement_summary_absent(self):
        self.assertEqual(W.parse_measurement_summary("random logs without marker"), {})


class ThroughputCalculationTests(unittest.TestCase):
    def test_calculate_throughputs_distinct_denominators(self):
        windows = {
            'first_to_last_enqueue_window': 599.0,
            'run_start_to_last_enqueue_window': 600.0,
            'publish_to_drain_window': 601.5,
            'runner_total_window': 610.0,
        }
        res = W.calculate_throughputs(600, 60000, windows)

        # 600 / 599 = 1.002 b/s, 60000 / 599 = 100.167 s/s
        self.assertAlmostEqual(res['first_to_last_enqueue_window']['batches_per_sec'], 1.002, places=3)
        self.assertAlmostEqual(res['first_to_last_enqueue_window']['samples_per_sec'], 100.167, places=3)

        # 600 / 600 = 1.0 b/s, 60000 / 600 = 100.0 s/s
        self.assertAlmostEqual(res['run_start_to_last_enqueue_window']['batches_per_sec'], 1.0, places=3)
        self.assertAlmostEqual(res['run_start_to_last_enqueue_window']['samples_per_sec'], 100.0, places=3)

        # 600 / 601.5 = 0.998 b/s
        self.assertAlmostEqual(res['publish_to_drain_window']['batches_per_sec'], 0.998, places=3)

        # 600 / 610.0 = 0.984 b/s
        self.assertAlmostEqual(res['runner_total_window']['batches_per_sec'], 0.984, places=3)


class StorageMeasurementTests(unittest.TestCase):
    def test_parse_bytes_unit(self):
        self.assertEqual(W.parse_bytes_unit('1024b'), 1024)
        self.assertEqual(W.parse_bytes_unit('10KiB'), 10 * 1024)
        self.assertEqual(W.parse_bytes_unit('12.5MiB'), int(12.5 * 1024 * 1024))
        self.assertEqual(W.parse_bytes_unit('2GiB'), 2 * 1024 * 1024 * 1024)
        self.assertEqual(W.parse_bytes_unit('invalid'), 0)

    def test_compute_storage_deltas(self):
        b = W.StorageSnapshot(
            telemetry_hypertable_bytes=10000,
            telemetry_chunks=1,
            temporal_hypertable_bytes=5000,
            temporal_chunks=1,
            ledger_table_bytes=2000,
            ledger_indexes_bytes=1000,
            ledger_total_bytes=3000,
            database_bytes=5000000,
            sqlite_file_bytes=4096,
        )
        a = W.StorageSnapshot(
            telemetry_hypertable_bytes=80000,
            telemetry_chunks=4,
            temporal_hypertable_bytes=15000,
            temporal_chunks=2,
            ledger_table_bytes=8000,
            ledger_indexes_bytes=4000,
            ledger_total_bytes=12000,
            database_bytes=5200000,
            sqlite_file_bytes=8192,
        )
        d = W.compute_storage_deltas(b, a)
        self.assertEqual(d['telemetry_hypertable_bytes_delta'], 70000)
        self.assertEqual(d['telemetry_chunks_delta'], 3)
        self.assertEqual(d['temporal_hypertable_bytes_delta'], 10000)
        self.assertEqual(d['temporal_chunks_delta'], 1)
        self.assertEqual(d['ledger_table_bytes_delta'], 6000)
        self.assertEqual(d['ledger_indexes_bytes_delta'], 3000)
        self.assertEqual(d['ledger_total_bytes_delta'], 9000)
        self.assertEqual(d['database_bytes_delta'], 200000)
        self.assertEqual(d['sqlite_file_bytes_delta'], 4096)


class ResourceSamplerTests(unittest.TestCase):
    def test_resource_sampler_bounded_collection_and_summary(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'outbox.db'
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE outbox (id INTEGER PRIMARY KEY, status TEXT, byte_size INTEGER)")
            conn.execute("INSERT INTO outbox VALUES (1, 'pending', 150)")
            conn.execute("INSERT INTO outbox VALUES (2, 'pending', 250)")
            conn.execute("INSERT INTO outbox VALUES (3, 'quarantined', 400)")
            conn.commit()
            conn.close()

            cids = {'backend': 'cid_backend_123', 'gateway': 'cid_gateway_456'}
            sampler = W.ResourceSampler(cids, db_path, poll_interval=0.05, max_samples=10)

            # Mock _sample_containers to avoid real docker calls
            sampler._sample_containers = MagicMock(return_value={
                'backend': {'cpu_pct': 10.5, 'mem_bytes': 50 * 1024 * 1024},
                'gateway': {'cpu_pct': 4.2, 'mem_bytes': 20 * 1024 * 1024},
            })

            sampler.start()
            import time
            time.sleep(0.2)
            sampler.stop()

            sum_data = sampler.summary()
            self.assertGreater(sum_data['sample_count'], 0)
            self.assertIn('backend', sum_data['containers'])
            self.assertIn('gateway', sum_data['containers'])
            self.assertEqual(sum_data['containers']['backend']['cpu_max_pct'], 10.5)
            self.assertEqual(sum_data['containers']['gateway']['cpu_max_pct'], 4.2)
            self.assertGreater(sum_data['sqlite_trend_sample_count'], 0)
            last_sq = sum_data['sqlite_trend_last']
            self.assertEqual(last_sq['pending_rows'], 2)
            self.assertEqual(last_sq['pending_bytes'], 400)
            self.assertEqual(last_sq['quarantined_rows'], 1)
            self.assertEqual(last_sq['in_flight'], 'unavailable')
            self.assertEqual(sum_data['backlog_high_water_rows'], 2)
            self.assertEqual(sum_data['backlog_high_water_bytes'], 400)
            self.assertEqual(sum_data['in_flight'], 'unavailable')


class WorkloadSqlAssertionTests(unittest.TestCase):
    def test_workload_assert_sql_uses_actual_schema_names_and_invariants(self):
        spec = GatewaySpec(
            gateway_id='gw_workload_test',
            gateway_name='Stage 3 Workload Gateway Test',
            sensor_name='Stage 3 Workload Sensor Test',
            gateway_reported={'firmware_version': '1.0.0'},
            gateway_desired={'reboot_requested': False},
            gateway_reported_version=1,
            gateway_desired_version=1,
            sensor_desired={'sampling_interval_seconds': 5},
            sensor_desired_version=1,
        )
        sql = W.build_workload_assert_sql(spec, 'TEST_MARKER')

        # Must use ACTUAL schema names
        self.assertIn('public.processed_messages', sql)
        self.assertIn('public.telemetry', sql)
        self.assertIn('public.twin_temporal_values', sql)
        self.assertIn('public.twin_states', sql)
        self.assertIn('public.twin_entities', sql)

        # Must NOT use uncommitted or wrong names
        self.assertNotIn('telemetry_ledger', sql)

        # Invariant values
        self.assertIn(str(W.WORKLOAD_TARGET_BATCHES), sql)
        self.assertIn(str(W.WORKLOAD_TOTAL_SAMPLES), sql)
        self.assertIn(str(W.WORKLOAD_SAMPLES_PER_BATCH), sql)
        self.assertIn(str(W.WORKLOAD_SAMPLE_INTERVAL_US), sql)

        # Contiguity check with lag(last_sequence)
        self.assertIn('lag(last_sequence) OVER (ORDER BY first_sequence)', sql)

        # Synthetic value formula check
        self.assertIn('sequence_num % 2000', sql)
        self.assertIn('TEST_MARKER', sql)


class WorkloadGatewayEnvTests(unittest.TestCase):
    def test_workload_gateway_env_profile(self):
        env = W.workload_gateway_env('gw_test', 'secret_pwd', 'sensor_test')
        self.assertEqual(env['GATEWAY_ID'], 'gw_test')
        self.assertEqual(env['SENSOR_ID'], 'sensor_test')
        self.assertEqual(env['GATEWAY_MAX_INFLIGHT'], '8')
        self.assertEqual(env['GATEWAY_ACK_TIMEOUT'], '5s')
        self.assertEqual(env['GATEWAY_RETRY_MIN_DELAY'], '1s')
        self.assertEqual(env['GATEWAY_RETRY_MAX_DELAY'], '30s')
        self.assertEqual(env['GATEWAY_OUTBOX_MAX_ROWS'], '5000')
        self.assertEqual(env['GATEWAY_OUTBOX_MAX_BYTES'], '67108864')
        self.assertEqual(env['GATEWAY_SAMPLES_PER_BATCH'], '100')
        self.assertEqual(env['GATEWAY_SAMPLE_INTERVAL_US'], '10000')
        self.assertEqual(env['GATEWAY_BATCH_SPAN'], '1s')


class WorkloadEntrypointContractTests(unittest.TestCase):
    def test_dry_run_preflight_without_execute(self):
        res = W.main([])
        self.assertEqual(res, 0)

    def test_shell_script_preflight_runs_and_passes(self):
        proc = subprocess.run(
            ['sh', str(ROOT / 'scripts/test-stage3-workload.sh')],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.assertEqual(proc.returncode, 0, f"stdout: {proc.stdout}\nstderr: {proc.stderr}")
        self.assertIn("PASS: stage3 workload preflight", proc.stdout)

    def test_report_allowlist_no_secrets(self):
        prov = W.get_git_provenance()
        env = W.get_host_environment()
        report_str = json.dumps({'prov': prov, 'env': env})
        for forbidden in ('password', 'token', 'secret', 'dsn', 'private_key'):
            self.assertNotIn(forbidden, report_str.lower())


if __name__ == '__main__':
    unittest.main()
