#!/usr/bin/env python3
"""Focused unit tests for Task 4.7.3 vertical scenario module.

No running Docker daemon or containers required for these unit tests.
They verify:
  1. Callback registration of 'vertical' scenario in stage4_api_runner.
  2. Synthetic value formula fidelity (mirrors internal/gatewaysim/batch.go).
  3. JSON query wrapping and parsing helpers.
  4. Snapshot invariants comparison (detecting mutations).
"""
from __future__ import annotations

import json
import copy
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import stage4_api_runner as R  # noqa: E402
import stage4_vertical_scenario as V  # noqa: E402


class VerticalScenarioUnitTests(unittest.TestCase):
    """Verify vertical scenario registration and unit helpers."""

    def test_vertical_scenario_is_registered_in_runner(self):
        self.assertIn('vertical', R.SCENARIO_CALLBACKS)
        self.assertEqual(R.SCENARIO_CALLBACKS['vertical'], V.run_vertical_scenario)

    def test_gateway_timeout_removes_registered_container(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = MagicMock(work_dir=Path(tmp))
            spec = R.GatewayFixtureSpec('gw_timeout')
            ctx.gateway_passwords = {spec.gateway_id: 'fixture-secret'}
            with patch.object(V, 'run_gateway_container', side_effect=subprocess.TimeoutExpired('docker', 1)) as run, \
                 patch.object(V, 'container_absent', return_value=False), \
                 patch.object(V, 'remove_container', create=True) as remove:
                with self.assertRaises(subprocess.TimeoutExpired):
                    V.run_sim_batches(ctx, spec, 1, Path(tmp) / 'outbox')
            name = run.call_args.kwargs['container_name']
            self.assertTrue(name.startswith('stage4-api-gateway-'))
            ctx.registry.register.assert_called_once_with('gateway simulator', 'container', name)
            remove.assert_called_once_with(name)

    def test_queries_is_registered_in_runner(self):
        self.assertIn('contract', R.SCENARIO_CALLBACKS)
        self.assertIn('queries', R.SCENARIO_CALLBACKS)

    def test_synthetic_value_formula(self):
        # seq 0 -> 20.00
        self.assertAlmostEqual(V.synthetic_value(0), 20.00, places=4)
        # seq 1 -> 20.01
        self.assertAlmostEqual(V.synthetic_value(1), 20.01, places=4)
        # seq 99 -> 20.99
        self.assertAlmostEqual(V.synthetic_value(99), 20.99, places=4)
        # seq 100 -> 21.00
        self.assertAlmostEqual(V.synthetic_value(100), 21.00, places=4)
        # seq 1000 (peak) -> 30.00
        self.assertAlmostEqual(V.synthetic_value(1000), 30.00, places=4)
        # seq 2000 (cycle complete) -> 20.00
        self.assertAlmostEqual(V.synthetic_value(2000), 20.00, places=4)

    def test_full_content_detects_same_count_mutation_and_added_sensor(self):
        spec = R.GatewayFixtureSpec('gw_a')
        before = {'telemetry': [{'value': 20, 'measured_at': 't'}],
                  'sensors': [{'gateway_id': 'gw_a', 'sensor_id': spec.sensor_id, 'name': 'old'}],
                  'twin_entities': [{'entity_id': spec.sensor_urn, 'name': 'old', 'updated_at': 't'}]}
        after = copy.deepcopy(before)
        after['telemetry'][0]['value'] = 21
        with self.assertRaises(AssertionError):
            V.assert_patch_preserves_content(before, after, spec)
        after = copy.deepcopy(before)
        after['sensors'].append({'gateway_id': 'gw_a', 'sensor_id': 'unexpected', 'name': 'new'})
        with self.assertRaises(AssertionError):
            V.assert_content_unchanged(before, after)
        after = copy.deepcopy(before)
        after['sensors'][0]['name'] = 'new'
        after['twin_entities'][0].update(name='new', updated_at='later')
        V.assert_patch_preserves_content(before, after, spec)

    def test_owned_snapshot_selects_full_rows_and_all_gateway_sensors(self):
        with patch.object(V, 'query_json_row', return_value={}) as query:
            V.snapshot_owned_content(MagicMock(), ['gw_a'])
        sql = query.call_args.args[1]
        self.assertIn('jsonb_agg(to_jsonb(r)', sql)
        self.assertIn("FROM public.sensors r WHERE gateway_id IN ('gw_a')", sql)
        self.assertIn('FROM public.twin_states', sql)
        self.assertIn('FROM public.twin_temporal_values', sql)

    def test_query_json_row_parses_marker(self):
        fake_fixture = MagicMock()
        fake_fixture.sql.return_value = (0, b'NOTICE:  JSON_ROW {"count": 42, "status": "ok"}\nDO')
        result = V.query_json_row(fake_fixture, "SELECT 42 AS count, 'ok' AS status")
        self.assertEqual(result, {'count': 42, 'status': 'ok'})

    def test_query_json_row_raises_on_missing_marker(self):
        fake_fixture = MagicMock()
        fake_fixture.sql.return_value = (0, b'NOTICE:  something else\nDO')
        with self.assertRaises(RuntimeError) as ctx:
            V.query_json_row(fake_fixture, "SELECT 1")
        self.assertIn('JSON_ROW marker not found', str(ctx.exception))

    def test_query_json_rows_parses_marker(self):
        fake_fixture = MagicMock()
        fake_fixture.sql.return_value = (0, b'NOTICE:  JSON_ROWS [{"id": 1}, {"id": 2}]\nDO')
        result = V.query_json_rows(fake_fixture, "SELECT 1 AS id UNION SELECT 2 AS id")
        self.assertEqual(result, [{'id': 1}, {'id': 2}])

    def test_snapshot_invariants_query_structure(self):
        fake_fixture = MagicMock()
        fake_fixture.sql.return_value = (0, b'NOTICE:  JSON_ROW {"telem_a_count": 200, "gw_a_attributes": {}}\nDO')
        spec_a = R.GatewayFixtureSpec('gw_a')
        spec_b = R.GatewayFixtureSpec('gw_b')
        snap = V.snapshot_invariants(fake_fixture, spec_a, spec_b)
        self.assertEqual(snap['telem_a_count'], 200)
        self.assertEqual(snap['gw_a_attributes'], {})
        # Verify SQL sent contains table checks
        executed = fake_fixture.sql.call_args[0][0].decode('utf-8')
        self.assertIn('public.telemetry', executed)
        self.assertIn('public.processed_messages', executed)
        self.assertIn('public.twin_states', executed)
        self.assertIn('public.twin_temporal_values', executed)


if __name__ == '__main__':
    unittest.main()
