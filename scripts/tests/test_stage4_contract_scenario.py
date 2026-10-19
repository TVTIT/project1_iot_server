#!/usr/bin/env python3
"""Focused unit tests for Task 4.7.4 contract scenario module.

No running Docker daemon or containers required for these unit tests.
They verify:
  1. Callback registration of 'contract' scenario in stage4_api_runner.
  2. Error envelope assertion utility correctness (status, code, message, request_id, headers).
  3. Error envelope assertion failure modes (mismatched status, bad code, missing fields, bad headers).
  4. Denial snapshot query structure and parameter substitution.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import stage4_api_runner as R  # noqa: E402
import stage4_contract_scenario as C  # noqa: E402


class MockHTTPResponse:
    def __init__(self, status: int, headers: dict[str, str], json_data: dict | None):
        self.status = status
        self.headers = headers
        self.json = json_data


class ContractScenarioUnitTests(unittest.TestCase):
    """Verify contract scenario registration and assertion helpers."""

    def test_contract_scenario_is_registered_in_runner(self):
        self.assertIn('contract', R.SCENARIO_CALLBACKS)
        self.assertEqual(R.SCENARIO_CALLBACKS['contract'], C.run_contract_scenario)

    def test_assert_error_envelope_valid(self):
        resp = MockHTTPResponse(
            status=400,
            headers={
                'content-type': 'application/json; charset=utf-8',
                'cache-control': 'no-store',
            },
            json_data={
                'error': {
                    'code': 'raw_limit_exceeded',
                    'message': 'raw window exceeds limit',
                    'request_id': 'req-test-123',
                }
            },
        )
        err_obj = C.assert_error_envelope(resp, 400, 'raw_limit_exceeded', 'test label')
        self.assertEqual(err_obj['code'], 'raw_limit_exceeded')
        self.assertEqual(err_obj['request_id'], 'req-test-123')

    def test_assert_error_envelope_status_mismatch(self):
        resp = MockHTTPResponse(
            status=404,
            headers={'content-type': 'application/json', 'cache-control': 'no-store'},
            json_data={'error': {'code': 'not_found', 'message': 'not found', 'request_id': 'r1'}},
        )
        with self.assertRaises(AssertionError) as ctx:
            C.assert_error_envelope(resp, 400, 'not_found', 'status check')
        self.assertIn('expected HTTP 400, got 404', str(ctx.exception))

    def test_assert_error_envelope_code_mismatch(self):
        resp = MockHTTPResponse(
            status=400,
            headers={'content-type': 'application/json', 'cache-control': 'no-store'},
            json_data={'error': {'code': 'wrong_code', 'message': 'bad', 'request_id': 'r1'}},
        )
        with self.assertRaises(AssertionError) as ctx:
            C.assert_error_envelope(resp, 400, 'expected_code', 'code check')
        self.assertIn("expected error code 'expected_code', got 'wrong_code'", str(ctx.exception))

    def test_assert_error_envelope_missing_cache_control(self):
        resp = MockHTTPResponse(
            status=400,
            headers={'content-type': 'application/json'},
            json_data={'error': {'code': 'bad', 'message': 'bad', 'request_id': 'r1'}},
        )
        with self.assertRaises(AssertionError) as ctx:
            C.assert_error_envelope(resp, 400, 'bad', 'header check')
        self.assertIn('expected Cache-Control: no-store', str(ctx.exception))

    def test_assert_error_envelope_missing_request_id(self):
        resp = MockHTTPResponse(
            status=400,
            headers={'content-type': 'application/json', 'cache-control': 'no-store'},
            json_data={'error': {'code': 'bad', 'message': 'bad'}},
        )
        with self.assertRaises(AssertionError) as ctx:
            C.assert_error_envelope(resp, 400, 'bad', 'req id check')
        self.assertIn('missing request_id', str(ctx.exception))

    def test_snapshot_denial_state_query_construction(self):
        fake_fixture = MagicMock()
        fake_fixture.sql.return_value = (0, b'NOTICE:  JSON_ROW {"gw_name": "Test"}\nDO')
        snap = C.snapshot_denial_state(
            fake_fixture,
            'gw_test',
            'urn:ngsi-ld:Gateway:gw_test',
            'sens_test',
            'urn:ngsi-ld:Sensor:gw_test:sens_test',
        )
        self.assertEqual(snap, {'gw_name': 'Test', 'owned_content': {'gw_name': 'Test'}})
        self.assertEqual(fake_fixture.sql.call_count, 2)
        call_args = fake_fixture.sql.call_args_list[0].args[0].decode('utf-8')
        self.assertIn("gateway_id = 'gw_test'", call_args)
        self.assertIn("sensor_id = 'sens_test'", call_args)
        content_sql = fake_fixture.sql.call_args_list[1].args[0].decode('utf-8')
        self.assertIn('FROM public.sensors r WHERE', content_sql)
        self.assertIn('FROM public.twin_entities r WHERE', content_sql)

    def test_parse_rel_next_uri(self):
        # Single link with rel="next"
        self.assertEqual(
            C.parse_rel_next_uri('</v1/digital-twins?limit=1&cursor=cur123>; rel="next"'),
            '/v1/digital-twins?limit=1&cursor=cur123',
        )
        # Multiple links (prev, next)
        self.assertEqual(
            C.parse_rel_next_uri('</v1/prev>; rel="prev", </v1/next?c=1>; rel="next"'),
            '/v1/next?c=1',
        )
        # Case insensitive and whitespace
        self.assertEqual(
            C.parse_rel_next_uri('  </v1/next?c=2>  ;  REL="NEXT"  '),
            '/v1/next?c=2',
        )
        # Unquoted rel=next
        self.assertEqual(
            C.parse_rel_next_uri('</v1/next?c=3>; rel=next'),
            '/v1/next?c=3',
        )
        # Missing rel=next
        self.assertIsNone(C.parse_rel_next_uri('</v1/prev>; rel="prev"'))
        # None or empty
        self.assertIsNone(C.parse_rel_next_uri(None))
        self.assertIsNone(C.parse_rel_next_uri(''))

    def test_validate_next_uri_contract_valid(self):
        next_uri = '/v1/digital-twins?gateway_id=gw1&limit=1&cursor=base64cursor'
        cursor = C.validate_next_uri_contract(
            next_uri,
            expected_path='/v1/digital-twins',
            expected_params={'gateway_id': 'gw1', 'limit': '1'},
            cursor_param_name='cursor',
        )
        self.assertEqual(cursor, 'base64cursor')

    def test_validate_next_uri_contract_rejects_non_relative(self):
        next_uri = 'http://attacker.invalid/v1/digital-twins?gateway_id=gw1&limit=1&cursor=c1'
        with self.assertRaises(AssertionError) as ctx:
            C.validate_next_uri_contract(
                next_uri,
                expected_path='/v1/digital-twins',
                expected_params={'gateway_id': 'gw1', 'limit': '1'},
                cursor_param_name='cursor',
            )
        self.assertIn('rel="next" URI must be relative', str(ctx.exception))

    def test_validate_next_uri_contract_rejects_path_mismatch(self):
        next_uri = '/v1/other-endpoint?limit=1&cursor=c1'
        with self.assertRaises(AssertionError) as ctx:
            C.validate_next_uri_contract(
                next_uri,
                expected_path='/v1/digital-twins',
                expected_params={'limit': '1'},
                cursor_param_name='cursor',
            )
        self.assertIn('path mismatch', str(ctx.exception))

    def test_validate_next_uri_contract_rejects_missing_or_mismatched_params(self):
        # Missing limit
        next_uri = '/v1/digital-twins?gateway_id=gw1&cursor=c1'
        with self.assertRaises(AssertionError) as ctx:
            C.validate_next_uri_contract(
                next_uri,
                expected_path='/v1/digital-twins',
                expected_params={'gateway_id': 'gw1', 'limit': '1'},
                cursor_param_name='cursor',
            )
        self.assertIn('parameter limit mismatch', str(ctx.exception))

    def test_validate_next_uri_contract_rejects_missing_cursor(self):
        next_uri = '/v1/digital-twins?gateway_id=gw1&limit=1'
        with self.assertRaises(AssertionError) as ctx:
            C.validate_next_uri_contract(
                next_uri,
                expected_path='/v1/digital-twins',
                expected_params={'gateway_id': 'gw1', 'limit': '1'},
                cursor_param_name='cursor',
            )
        self.assertIn('missing cursor parameter', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
