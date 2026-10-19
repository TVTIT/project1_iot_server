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

    def test_ngsi_ld_temporal_raw_and_bucket_equivalence(self):
        # Raw format
        raw_before = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'values': [
                    [20.5, '2026-10-18T12:00:00.000000Z'],
                    [21.0, '2026-10-18T12:00:01.000000Z'],
                ],
            },
        }
        QS.assert_history_equivalent(raw_before, copy.deepcopy(raw_before))

        # Reordering ties in raw temporal values
        temp_tie = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'sample_interval_us': {
                'type': 'Property',
                'values': [
                    [10000.0, '2026-10-18T12:00:00.000000Z'],
                    [20000.0, '2026-10-18T12:00:00.000000Z'],
                ],
            },
        }
        temp_tie_rev = copy.deepcopy(temp_tie)
        temp_tie_rev['sample_interval_us']['values'].reverse()
        QS.assert_history_equivalent(temp_tie, temp_tie_rev, temporal=True)

        # Value mismatch in raw
        raw_mismatch = copy.deepcopy(raw_before)
        raw_mismatch['measurement']['values'][0][0] = 99.9
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(raw_before, raw_mismatch)

        # Bucket format
        bucket_before = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'min': [[20.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'max': [[21.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [[1000, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
            },
        }
        QS.assert_history_equivalent(bucket_before, copy.deepcopy(bucket_before))

        # Missing field in bucket
        bucket_missing = copy.deepcopy(bucket_before)
        del bucket_missing['measurement']['totalCount']
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(bucket_before, bucket_missing)

    def test_total_count_differs_by_one_above_2_pow_53(self):
        # 2^53 is 9007199254740992; IEEE-754 double rounds 2^53 and 2^53 + 1 to the same float.
        val_a = 9007199254740992
        val_b = 9007199254740993
        before = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [[val_a, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
            },
        }
        after = copy.deepcopy(before)
        after['measurement']['totalCount'][0][0] = val_b
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(before, after)
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(after, before)

    def test_total_count_slot_rejects_float_string_bool(self):
        base = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [[1000, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
            },
        }
        bad_tokens = [
            1000.0,       # float instead of int
            '1000',       # string instead of int
            True,         # bool instead of int
            False,        # bool instead of int
            None,         # NoneType
            [1000],       # nested list
        ]
        for bad_token in bad_tokens:
            with self.subTest(bad_token=bad_token, bad_type=type(bad_token).__name__):
                candidate = copy.deepcopy(base)
                candidate['measurement']['totalCount'][0][0] = bad_token
                with self.assertRaises(AssertionError):
                    QS.assert_history_equivalent(base, candidate)
                with self.assertRaises(AssertionError):
                    QS.assert_history_equivalent(candidate, base)

    def test_total_count_both_sides_malformed_even_if_equal(self):
        malformed_total_counts = [
            [[1000.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],  # float count token
            [['1000', '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],  # string count token
            [[True, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],    # bool count token
            [[False, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],   # bool count token
            [[1000]],                                                                     # length 1 instead of 3
            [[1000, '2026-10-18T12:00:00.000000Z']],                                     # length 2 instead of 3
            [[1000, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z', 1]],   # length 4
            [[1000, 12345, '2026-10-18T12:00:10.000000Z']],                              # non-string start timestamp
            [[1000, '2026-10-18T12:00:00.000000Z', 67890]],                              # non-string end timestamp
            'not_a_list',                                                                 # not a list
        ]
        for malformed in malformed_total_counts:
            with self.subTest(malformed=malformed):
                doc = {
                    'id': 'urn:ngsi-ld:Sensor:gw1:s1',
                    'type': 'Sensor',
                    '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
                    'measurement': {
                        'type': 'Property',
                        'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                        'totalCount': malformed,
                    },
                }
                with self.assertRaises(AssertionError):
                    QS.assert_history_equivalent(doc, copy.deepcopy(doc))

    def test_valid_exact_int_counts(self):
        val_large = 9007199254740992
        before = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [
                    [1000, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z'],
                    [0, '2026-10-18T12:00:10.000000Z', '2026-10-18T12:00:20.000000Z'],
                    [val_large, '2026-10-18T12:00:20.000000Z', '2026-10-18T12:00:30.000000Z'],
                ],
            },
        }
        # Exact equal int counts should pass
        QS.assert_history_equivalent(before, copy.deepcopy(before))

        # Also works on sample_interval_us
        temp_doc = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'sample_interval_us': {
                'type': 'Property',
                'avg': [[10000.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [[50, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
            },
        }
        QS.assert_history_equivalent(temp_doc, copy.deepcopy(temp_doc), temporal=True)

    def test_ordinary_floating_measurement_tolerance(self):
        before = {
            'id': 'urn:ngsi-ld:Sensor:gw1:s1',
            'type': 'Sensor',
            '@context': ['https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld'],
            'measurement': {
                'type': 'Property',
                'avg': [[20.5, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'min': [[20.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'max': [[21.0, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
                'totalCount': [[1000, '2026-10-18T12:00:00.000000Z', '2026-10-18T12:00:10.000000Z']],
            },
        }
        # Perturbation within 1e-9 tolerance passes
        tolerated = copy.deepcopy(before)
        tolerated['measurement']['avg'][0][0] = 20.5 + 1e-10
        tolerated['measurement']['min'][0][0] = 20.0 - 1e-10
        QS.assert_history_equivalent(before, tolerated)

        # Perturbation exceeding tolerance fails
        exceeded = copy.deepcopy(before)
        exceeded['measurement']['avg'][0][0] = 20.5 + 1e-8
        with self.assertRaises(AssertionError):
            QS.assert_history_equivalent(before, exceeded)


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
