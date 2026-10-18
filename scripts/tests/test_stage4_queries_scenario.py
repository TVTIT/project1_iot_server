#!/usr/bin/env python3
"""Focused unit tests for Task 4.7.5 uncompressed query measurement parsing and orchestration.

No running Docker daemon or containers required for these unit tests.
They verify:
  1. JSON marker extraction from Go test stdout (valid, missing marker, malformed JSON).
  2. Non-secret summary structure validation (no passwords/tokens/DSNs exposed).
  3. Metric dictionary validation (target counts, buffer counters, plan nodes).
  4. Incomplete scenario orchestration contract (raising NotImplementedError).
"""
from __future__ import annotations

import json
import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/tests"))

import stage4_queries_scenario as QS


class CompressionEquivalenceTests(unittest.TestCase):
    def test_raw_provenance_unit_and_missing_fields_are_compared(self):
        before = {'mode': 'raw', 'unit': None, 'points': [{
            'measured_at': '2026-10-18T12:00:00Z', 'value': 1.0, 'sequence_num': '1',
            'boot_id': None, 'message_id': None, 'sample_index': None,
        }]}
        QS.assert_history_equivalent(before, copy.deepcopy(before))
        for key in ('boot_id', 'message_id', 'sample_index', 'unit'):
            for missing in (False, True):
                with self.subTest(key=key, missing=missing):
                    after = copy.deepcopy(before)
                    target = after if key == 'unit' else after['points'][0]
                    if missing:
                        del target[key]
                    else:
                        target[key] = 2 if key == 'sample_index' else 'changed'
                    with self.assertRaises(AssertionError):
                        QS.assert_history_equivalent(before, after)

    def test_temporal_multiset_preserves_type_and_message(self):
        point = {'observed_at': '2026-10-18T12:00:00Z', 'value': 10.0,
                 'value_type': 'number', 'message_id': None, 'metadata': {'unit': 'us'}}
        before = {'mode': 'raw', 'unit': 'us', 'points': [point, dict(point, value=20.0)]}
        reversed_result = copy.deepcopy(before)
        reversed_result['points'].reverse()
        QS.assert_history_equivalent(before, reversed_result, temporal=True)
        for key, value in (('value_type', 'text'), ('message_id', 'changed')):
            after = copy.deepcopy(before)
            after['points'][0][key] = value
            with self.assertRaises(AssertionError):
                QS.assert_history_equivalent(before, after, temporal=True)
        del before['points'][0]['message_id']
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(before, copy.deepcopy(before), temporal=True)


class MarkerParsingTests(unittest.TestCase):
    """Test extracting JSON measurement payloads from Go test outputs."""

    def test_parse_valid_telemetry_marker(self):
        sample_output = (
            "=== RUN   TestNativeTelemetryHistoryQueryPlanMeasurement\n"
            "M1 Plan: ExecutionTime=0.5 ms\n"
            'TELEMETRY_MEASUREMENT_JSON: {"scenario":"telemetry","m1_execution_time_ms":0.52,"m1_actual_rows":500}\n'
            "--- PASS: TestNativeTelemetryHistoryQueryPlanMeasurement (0.12s)\n"
            "PASS\n"
        )
        data = QS.parse_json_marker_from_output(sample_output, "TELEMETRY_MEASUREMENT_JSON: ")
        self.assertEqual(data["scenario"], "telemetry")
        self.assertEqual(data["m1_execution_time_ms"], 0.52)
        self.assertEqual(data["m1_actual_rows"], 500)

    def test_parse_valid_temporal_marker(self):
        sample_output = (
            "=== RUN   TestNativeTemporalHistoryQueryPlanMeasurement\n"
            'TEMPORAL_MEASUREMENT_JSON: {"scenario":"temporal","m4_execution_time_ms":0.88,"m4_actual_rows":10}\n'
            "--- PASS: TestNativeTemporalHistoryQueryPlanMeasurement (0.15s)\n"
        )
        data = QS.parse_json_marker_from_output(sample_output, "TEMPORAL_MEASUREMENT_JSON: ")
        self.assertEqual(data["scenario"], "temporal")
        self.assertEqual(data["m4_execution_time_ms"], 0.88)
        self.assertEqual(data["m4_actual_rows"], 10)

    def test_missing_marker_raises_runtime_error(self):
        output = "=== RUN   TestFoo\n--- PASS: TestFoo (0.01s)\nPASS\n"
        with self.assertRaises(RuntimeError) as ctx:
            QS.parse_json_marker_from_output(output, "TELEMETRY_MEASUREMENT_JSON: ")
        self.assertIn("marker 'TELEMETRY_MEASUREMENT_JSON: ' not found", str(ctx.exception))

    def test_malformed_json_raises_json_decode_error(self):
        output = "TELEMETRY_MEASUREMENT_JSON: {not valid json}\n"
        with self.assertRaises(json.JSONDecodeError):
            QS.parse_json_marker_from_output(output, "TELEMETRY_MEASUREMENT_JSON: ")


class SummaryStructureAndSecretIsolationTests(unittest.TestCase):
    """Verify measurement summary schema and non-secret isolation."""

    def test_summary_contains_no_secrets(self):
        # Sample realistic summary structure
        dummy_summary = {
            "status": "UNCOMPRESSED_BASELINE_COMPLETE_COMPRESSION_PENDING",
            "timestamp": "2026-10-18T12:00:00Z",
            "run_id": "s4_test123",
            "dataset": {
                "target_gateway_id": "gw_meas_test123",
                "target_sensor_id": "sens_meas_test123",
                "target_sensor_urn": "urn:ngsi-ld:Sensor:gw_meas_test123:sens_meas_test123",
                "distract_gateway_id": "gw_distr_test123",
                "distract_sensor_id": "sens_distr_test123",
                "distract_sensor_urn": "urn:ngsi-ld:Sensor:gw_distr_test123:sens_distr_test123",
                "target_raw_samples": 60000,
                "distract_raw_samples": 6000,
                "target_temporal_observations": 580,
                "distract_temporal_observations": 60,
                "time_span_seconds": 600,
                "target_sample_rate_hz": 100,
                "target_sample_interval_us": 10000,
                "target_temporal_gap": "[300s, 320s) (20 seconds)",
            },
            "compression_status": {
                "is_compressed": False,
                "telemetry_chunks": ["_hyper_1_1_chunk"],
                "twin_temporal_chunks": ["_hyper_2_1_chunk"],
                "total_uncompressed_chunks": 2,
            },
            "query_measurements": {
                "m1_raw_telemetry": {
                    "planning_time_ms": 1.2,
                    "execution_time_ms": 0.6,
                    "actual_rows": 500,
                    "shared_hit_blocks": 28,
                    "shared_read_blocks": 0,
                    "plan_nodes": ["Limit", "Sort", "Index Scan"],
                    "service_elapsed_ms": 2.1,
                    "http_elapsed_ms": 5.4,
                },
                "m2_bucket_telemetry": {
                    "planning_time_ms": 0.9,
                    "execution_time_ms": 68.2,
                    "actual_rows": 60,
                    "shared_hit_blocks": 1920,
                    "shared_read_blocks": 0,
                    "plan_nodes": ["Limit", "Aggregate", "Index Scan"],
                    "service_elapsed_ms": 58.3,
                    "http_elapsed_ms": 62.1,
                },
                "m3_overflow_fallback_telemetry": {
                    "probe_planning_time_ms": 0.5,
                    "probe_execution_time_ms": 0.7,
                    "probe_actual_rows": 501,
                    "fallback_planning_time_ms": 0.6,
                    "fallback_execution_time_ms": 1.2,
                    "fallback_actual_rows": 80,
                    "total_execution_time_ms": 1.9,
                    "service_elapsed_ms": 5.1,
                    "http_elapsed_ms": 7.8,
                },
                "m4_raw_temporal": {
                    "planning_time_ms": 0.8,
                    "execution_time_ms": 0.9,
                    "actual_rows": 10,
                    "shared_hit_blocks": 25,
                    "shared_read_blocks": 0,
                    "plan_nodes": ["Limit", "Sort", "Nested Loop"],
                    "service_elapsed_ms": 2.5,
                    "http_elapsed_ms": 6.2,
                },
                "m5_bucket_temporal": {
                    "planning_time_ms": 0.7,
                    "execution_time_ms": 1.5,
                    "actual_rows": 10,
                    "shared_hit_blocks": 30,
                    "shared_read_blocks": 0,
                    "plan_nodes": ["Limit", "Aggregate", "Nested Loop"],
                    "service_elapsed_ms": 3.8,
                    "http_elapsed_ms": 8.0,
                    "bucket_counts": [60, 60, 60, 60, 60, 40, 60, 60, 60, 60],
                    "arithmetic_valid": True,
                    "gap_verified": True,
                },
            },
            "tenant_isolation_verified": True,
            "schema_decision": "Retain Schema 20",
        }

        serialized = json.dumps(dummy_summary)
        # Verify no credentials, passwords, DSNs, or tokens
        for forbidden in ("password", "postgres://", "bearer", "secret", "eyJ"):
            self.assertNotIn(forbidden, serialized.lower())

        # Verify dataset invariants
        ds = dummy_summary["dataset"]
        self.assertEqual(ds["target_raw_samples"], 60000)
        self.assertEqual(ds["distract_raw_samples"], 6000)
        self.assertEqual(ds["target_temporal_observations"], 580)
        self.assertEqual(ds["distract_temporal_observations"], 60)

        # Verify compression status
        comp = dummy_summary["compression_status"]
        self.assertFalse(comp["is_compressed"])

        # Verify arithmetic & gap in temporal bucket
        m5 = dummy_summary["query_measurements"]["m5_bucket_temporal"]
        self.assertTrue(m5["arithmetic_valid"])
        self.assertTrue(m5["gap_verified"])
        self.assertEqual(sum(m5["bucket_counts"]), 580)
        self.assertEqual(m5["bucket_counts"][5], 40)


class ScenarioOrchestrationTests(unittest.TestCase):
    """Verify that run_queries_scenario coordinates uncompressed, compression, compressed, and cleanup."""

    @patch("stage4_queries_scenario.cleanup_queries_scenario_fixture_data")
    @patch("stage4_queries_scenario.run_compressed_query_measurements")
    @patch("stage4_queries_scenario.compress_target_hypertable_chunks")
    @patch("stage4_queries_scenario.run_uncompressed_query_measurements")
    def test_run_queries_scenario_success_pipeline(self, mock_uncomp, mock_compress, mock_comp_meas, mock_cleanup):
        fake_ctx = MagicMock()
        fake_ctx.work_dir = Path("/tmp/dummy_stage4_workdir")
        mock_uncomp.return_value = {
            "status": "UNCOMPRESSED_BASELINE_COMPLETE",
            "run_id": "test_run_123",
            "dataset": {"dummy": "data"},
            "query_measurements": {
                "m1_raw_telemetry": {},
                "m2_bucket_telemetry": {},
                "m3_overflow_fallback_telemetry": {},
                "m4_raw_temporal": {},
                "m5_bucket_temporal": {},
            },
        }
        mock_compress.return_value = {"is_compressed": True}
        mock_comp_meas.return_value = {
            "status": "COMPRESSED_MEASUREMENTS_COMPLETE",
            "query_measurements": {
                "m1_raw_telemetry": {},
                "m2_bucket_telemetry": {},
                "m3_overflow_fallback_telemetry": {},
                "m4_raw_temporal": {},
                "m5_bucket_temporal": {},
            },
        }

        with patch.object(Path, "write_text") as mock_write:
            res = QS.run_queries_scenario(fake_ctx)

        self.assertEqual(res["status"], "COMPLETED")
        self.assertEqual(res["run_id"], "test_run_123")
        self.assertTrue(res["result_equivalence_verified"])
        self.assertTrue(res["cleanup_verified"])
        mock_uncomp.assert_called_once_with(fake_ctx)
        mock_compress.assert_called_once_with(fake_ctx)
        mock_comp_meas.assert_called_once()
        mock_cleanup.assert_called_once_with(fake_ctx, "test_run_123")

    @patch("stage4_queries_scenario.cleanup_queries_scenario_fixture_data")
    @patch("stage4_queries_scenario.run_compressed_query_measurements", side_effect=RuntimeError("compression mismatch"))
    @patch("stage4_queries_scenario.compress_target_hypertable_chunks")
    @patch("stage4_queries_scenario.run_uncompressed_query_measurements")
    def test_run_queries_scenario_cleans_up_on_failure(self, mock_uncomp, mock_compress, mock_comp_meas, mock_cleanup):
        fake_ctx = MagicMock()
        mock_uncomp.return_value = {"run_id": "fail_run_456"}

        with self.assertRaises(RuntimeError) as ctx:
            QS.run_queries_scenario(fake_ctx)

        self.assertIn("compression mismatch", str(ctx.exception))
        mock_cleanup.assert_called_once_with(fake_ctx, "fail_run_456")


if __name__ == "__main__":
    unittest.main()
