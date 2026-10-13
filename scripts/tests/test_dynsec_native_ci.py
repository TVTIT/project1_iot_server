"""Unit tests for DynSec native CI supervisor (Slice 4b) - Review Follow-up Regressions.

TDD tests verifying:
- Strict literal True for raw success, aggregate success, executed, and every cleanup flag in BOTH
  cleanup and aggregate dicts. Rejects integers, boolean strings ('false', 'true', 1, 0), and contradictions.
- Require exact expected aggregate verdict 'SUCCESS' and native status 'PASS'; reject nonempty failure_code
  even if success is True. Missing/contradictory structures fail closed.
- Values allowlisting / sanitization: status/verdict/failure_code/lane restricted to explicit known safe enums.
  Injected secret strings in ANY of these fields are redacted or mapped to safe codes, never appearing in reports.
- Report writer: pre-validates report path BEFORE allocating native fixtures / running lanes; fails if path
  exists or is a dangling/valid symlink; uses atomic hard linking / O_CREAT|O_EXCL style no-overwrite;
  no raw path in exception strings.
- Fixed DYNSEC typo in verdict output.
- Signal handling honesty: single-shot signal triggering prevents repeated re-raising during cleanup unwinding;
  handler restoration errors fail closed and are not silently masked as full success.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

_SCRIPTS_TESTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_TESTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_TESTS_DIR)

import dynsec_native_ci as supervisor


def _make_passing_lane_result(lane_name: str) -> dict:
    return {
        'success': True,
        'allocated': True,
        'failure_stage': None,
        'failure_code': None,
        'native_test_result': {
            'status': 'PASS',
            'executed': True,
            'returncode': 0,
            'summary': f'{lane_name}_passed',
        },
        'cleanup': {
            'pg_cleanup_verified': True,
            'broker_cleanup_verified': True,
            'host_cleanup_verified': True,
        },
        'aggregate': {
            'success': True,
            'verdict': 'SUCCESS',
            'native_status': 'PASS',
            'pg_cleanup_verified': True,
            'broker_cleanup_verified': True,
            'host_cleanup_verified': True,
        },
    }


class TestDynSecNativeCISupervisor(unittest.TestCase):

    def test_artifact_schema_rejects_skip_cleanup_and_extra_values(self):
        import copy
        report = supervisor.build_safe_summary(
            _make_passing_lane_result('service'), _make_passing_lane_result('http'),
            True, 'SUCCESS', None, None)
        supervisor.validate_success_report(report)
        for field, value in (('status', 'SKIP'), ('executed', 1),
                             ('host_cleanup_verified', False), ('secret', 'not-allowlisted')):
            bad = copy.deepcopy(report)
            bad['lanes']['http'][field] = value
            with self.assertRaises(ValueError):
                supervisor.validate_success_report(bad)
        report['raw_logs'] = 'not-allowlisted'
        with self.assertRaises(ValueError):
            supervisor.validate_success_report(report)

    def test_dry_run_writes_safe_failure_report(self):
        report = self.tmp_dir / 'dry.json'
        result = supervisor.run_dynsec_native_ci(report_path=report, dry_run=True)
        self.assertFalse(result['success'])
        self.assertEqual(json.loads(report.read_text()), result)

    def test_artifact_cli_missing_or_invalid_report_fails_without_running_lanes(self):
        report = self.tmp_dir / 'missing.json'
        for content in (None, '{"success": true}', 'not-json'):
            if content is not None:
                report.write_text(content)
            with patch('sys.argv', ['dynsec_native_ci.py', '--validate-report', str(report)]), \
                    patch('dynsec_native_ci.run_native_rotate_revoke_suite') as runner, \
                    patch('builtins.print') as output:
                with self.assertRaises(SystemExit) as exited:
                    supervisor.main()
                self.assertEqual(exited.exception.code, 1)
                runner.assert_not_called()
                output.assert_called_once_with('DYNSEC_NATIVE_CI_VERDICT: SUPERVISOR_ERROR')

    def setUp(self):
        self.tmp_dir = Path('/tmp/opencode/test_dynsec_ci_' + os.urandom(6).hex())
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # 1. Success test: both service and HTTP runners PASS sequentially
    @patch('dynsec_native_ci.run_http_native_suite')
    @patch('dynsec_native_ci.run_native_rotate_revoke_suite')
    def test_both_lanes_pass_sequentially_produces_success_report(
        self, mock_service_runner, mock_http_runner
    ):
        mock_service_runner.return_value = _make_passing_lane_result('service')
        mock_http_runner.return_value = _make_passing_lane_result('http')

        report_path = self.tmp_dir / 'report.json'
        res = supervisor.run_dynsec_native_ci(report_path=report_path, dry_run=False)

        self.assertTrue(res['success'])
        self.assertEqual(res['verdict'], supervisor.VERDICT_SUCCESS)
        self.assertIsNone(res['failure_code'])

        mock_service_runner.assert_called_once_with(dry_run=False)
        mock_http_runner.assert_called_once_with(dry_run=False)

        # Check report was written
        self.assertTrue(report_path.exists())
        data = json.loads(report_path.read_text())
        self.assertTrue(data['success'])
        self.assertEqual(data['verdict'], supervisor.VERDICT_SUCCESS)
        self.assertIn('lanes', data)
        self.assertEqual(data['lanes']['service']['verdict'], 'SUCCESS')
        self.assertEqual(data['lanes']['http']['verdict'], 'SUCCESS')

    # 2. Strict literal boolean checking (Item 1):
    # Reject ints, bool strings ('false', 'true', 1, 0), and contradictions (cleanup False + aggregate True)
    def test_validate_lane_result_strict_literal_true_and_rejections(self):
        # Base valid result
        valid = _make_passing_lane_result('service')
        ok, code, _ = supervisor.validate_lane_result(valid, 'service')
        self.assertTrue(ok)
        self.assertEqual(code, '')

        # 2a. Integers 1 / 0 must be rejected (is not True)
        int_res = _make_passing_lane_result('service')
        int_res['success'] = 1
        ok, code, _ = supervisor.validate_lane_result(int_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'invalid_boolean_type')

        int_res2 = _make_passing_lane_result('service')
        int_res2['cleanup']['pg_cleanup_verified'] = 1
        ok, code, _ = supervisor.validate_lane_result(int_res2, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'invalid_boolean_type')

        # 2b. Strings 'true' / 'false' must be rejected
        str_res = _make_passing_lane_result('service')
        str_res['cleanup']['host_cleanup_verified'] = 'true'
        ok, code, _ = supervisor.validate_lane_result(str_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'invalid_boolean_type')

        # 2c. Contradiction: cleanup flag False, aggregate flag True
        contra_res = _make_passing_lane_result('service')
        contra_res['cleanup']['pg_cleanup_verified'] = False
        contra_res['aggregate']['pg_cleanup_verified'] = True
        ok, code, _ = supervisor.validate_lane_result(contra_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'cleanup_verification_failed')

        # 2d. Contradiction: cleanup flag True, aggregate flag False
        contra_res2 = _make_passing_lane_result('service')
        contra_res2['cleanup']['pg_cleanup_verified'] = True
        contra_res2['aggregate']['pg_cleanup_verified'] = False
        ok, code, _ = supervisor.validate_lane_result(contra_res2, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'cleanup_verification_failed')

        # 2e. Nonempty failure_code even if success is True must fail closed
        bad_code_res = _make_passing_lane_result('service')
        bad_code_res['failure_code'] = 'some_residual_error'
        ok, code, _ = supervisor.validate_lane_result(bad_code_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'unexpected_failure_code_on_success')

        # 2f. Aggregate verdict not literal 'SUCCESS' or native status not literal 'PASS'
        bad_verdict_res = _make_passing_lane_result('service')
        bad_verdict_res['aggregate']['verdict'] = 'PASS_WITH_WARNING'
        ok, code, _ = supervisor.validate_lane_result(bad_verdict_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'invalid_aggregate_verdict')

        bad_status_res = _make_passing_lane_result('service')
        bad_status_res['native_test_result']['status'] = 'PASSED'
        ok, code, _ = supervisor.validate_lane_result(bad_status_res, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'invalid_native_status')

        # 2g. Missing cleanup or aggregate dictionary fails closed
        missing_agg = _make_passing_lane_result('service')
        del missing_agg['aggregate']
        ok, code, _ = supervisor.validate_lane_result(missing_agg, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'malformed_lane_result')

        missing_cleanup = _make_passing_lane_result('service')
        del missing_cleanup['cleanup']
        ok, code, _ = supervisor.validate_lane_result(missing_cleanup, 'service')
        self.assertFalse(ok)
        self.assertEqual(code, 'malformed_lane_result')

    # 3. Secret sanitization across ALL dictionary VALUES (Item 2)
    def test_values_allowlist_sanitizes_injected_secrets_in_all_fields(self):
        injected_secret = "SECRET_TOKEN_eyJhbGciOi_password_leak_12345"

        polluted_service = {
            'success': False,
            'allocated': True,
            'failure_stage': injected_secret,
            'failure_code': f"leak_{injected_secret}",
            'native_test_result': {
                'status': f"STATUS_{injected_secret}",
                'executed': False,
                'returncode': 1,
                'summary': injected_secret,
            },
            'cleanup': {
                'pg_cleanup_verified': False,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            },
            'aggregate': {
                'success': False,
                'verdict': f"VERDICT_{injected_secret}",
                'native_status': f"NATIVE_{injected_secret}",
                'pg_cleanup_verified': False,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            },
        }

        # Validate lane directly
        _, code, summary = supervisor.validate_lane_result(polluted_service, 'service')
        self.assertNotIn(injected_secret, str(code))
        self.assertNotIn(injected_secret, json.dumps(summary))

        # Build safe summary with injected secret in failure_code, failure_lane, and unknown lane_name
        summary_all = supervisor.build_safe_summary(
            service_result=polluted_service,
            http_result=None,
            supervisor_success=False,
            supervisor_verdict=f"VERDICT_{injected_secret}",
            failure_code=f"CODE_{injected_secret}",
            failure_lane=f"LANE_{injected_secret}",
        )
        serialized = json.dumps(summary_all)
        self.assertNotIn(injected_secret, serialized)
        # Ensure values fall back to safe allowlisted enums
        self.assertEqual(summary_all['verdict'], supervisor.VERDICT_MALFORMED)
        self.assertEqual(summary_all['failure_code'], 'sanitized_failure_code')
        self.assertEqual(summary_all['failure_lane'], 'unknown_lane')

    # 4. Report path pre-validation BEFORE running lanes (Item 3)
    @patch('dynsec_native_ci.run_native_rotate_revoke_suite')
    def test_report_path_pre_validation_fails_before_lane_execution(self, mock_service):
        # 4a. Existing target file
        existing_file = self.tmp_dir / 'existing.json'
        existing_file.write_text('{}')

        with self.assertRaises(FileExistsError) as cm:
            supervisor.run_dynsec_native_ci(report_path=existing_file, dry_run=False)
        # Must fail before running any lane!
        mock_service.assert_not_called()
        self.assertNotIn(str(existing_file), str(cm.exception))  # No raw path in exception text

        # 4b. Dangling symlink as target
        dangling_symlink = self.tmp_dir / 'dangling_link.json'
        dangling_symlink.symlink_to(self.tmp_dir / 'nonexistent_target.json')

        with self.assertRaises(FileExistsError) as cm2:
            supervisor.run_dynsec_native_ci(report_path=dangling_symlink, dry_run=False)
        mock_service.assert_not_called()
        self.assertNotIn(str(dangling_symlink), str(cm2.exception))

        # 4c. Valid symlink pointing to an existing file
        target_file = self.tmp_dir / 'target.json'
        target_file.write_text('{"real": 1}')
        valid_symlink = self.tmp_dir / 'valid_link.json'
        valid_symlink.symlink_to(target_file)

        with self.assertRaises(FileExistsError):
            supervisor.run_dynsec_native_ci(report_path=valid_symlink, dry_run=False)
        mock_service.assert_not_called()

    # 5. Atomic link/no-overwrite writing without TOCTOU (Item 3)
    def test_write_safe_report_atomic_no_overwrite(self):
        report_file = self.tmp_dir / 'out_report.json'
        safe_data = {'success': True, 'verdict': 'SUCCESS'}

        # First write succeeds
        supervisor.write_safe_report(report_file, safe_data)
        self.assertTrue(report_file.exists())

        # Second write to the exact same path fails FileExistsError
        with self.assertRaises(FileExistsError):
            supervisor.write_safe_report(report_file, safe_data)

    # 6. Service failure blocks HTTP runner
    @patch('dynsec_native_ci.run_http_native_suite')
    @patch('dynsec_native_ci.run_native_rotate_revoke_suite')
    def test_service_lane_failure_blocks_http_allocation(
        self, mock_service_runner, mock_http_runner
    ):
        mock_service_runner.return_value = {
            'success': False,
            'allocated': True,
            'failure_stage': 'compile',
            'failure_code': 'compilation_failed',
            'native_test_result': {'status': 'FAIL', 'executed': False},
            'cleanup': {
                'pg_cleanup_verified': True,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            },
            'aggregate': {
                'success': False,
                'verdict': 'NATIVE_TEST_FAILED',
                'native_status': 'FAIL',
                'pg_cleanup_verified': True,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            },
        }

        report_path = self.tmp_dir / 'report.json'
        res = supervisor.run_dynsec_native_ci(report_path=report_path, dry_run=False)

        self.assertFalse(res['success'])
        self.assertEqual(res['failure_lane'], 'service')
        self.assertEqual(res['failure_code'], 'compilation_failed')
        mock_http_runner.assert_not_called()

        self.assertTrue(report_path.exists())
        data = json.loads(report_path.read_text())
        self.assertFalse(data['success'])
        self.assertEqual(data['lanes']['http']['verdict'], 'SKIPPED')

    # 7. Native SKIP or dry-run fails supervisor
    @patch('dynsec_native_ci.run_http_native_suite')
    @patch('dynsec_native_ci.run_native_rotate_revoke_suite')
    def test_dry_run_or_skip_fails_supervisor(
        self, mock_service_runner, mock_http_runner
    ):
        mock_service_runner.return_value = {
            'success': False,
            'allocated': False,
            'failure_stage': None,
            'failure_code': 'dry_run_complete',
            'aggregate': {
                'success': False,
                'verdict': 'DRY_RUN_ONLY',
                'native_status': 'DRY_RUN',
                'pg_cleanup_verified': True,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            },
        }

        res = supervisor.run_dynsec_native_ci(report_path=None, dry_run=True)
        self.assertFalse(res['success'])
        self.assertEqual(res['verdict'], supervisor.VERDICT_DRY_RUN_REJECTED)
        self.assertEqual(res['failure_code'], 'dry_run_or_skip_not_permitted')
        mock_http_runner.assert_not_called()

    # 8. Signal handler honesty: first signal unwinds try/finally, subsequent signals do not repeatedly raise (Item 4)
    def test_signal_handling_first_signal_only_and_restoration(self):
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        cleanup_ran = {'service': False, 'signals_received_in_cleanup': 0}

        def fake_service_runner(dry_run=False):
            try:
                # Trigger first signal
                os.kill(os.getpid(), signal.SIGINT)
            finally:
                cleanup_ran['service'] = True
                # Trigger a second signal during teardown: should NOT raise again
                try:
                    os.kill(os.getpid(), signal.SIGINT)
                except supervisor.SupervisorInterruptedError:
                    cleanup_ran['signals_received_in_cleanup'] += 1
            return _make_passing_lane_result('service')

        with patch('dynsec_native_ci.run_native_rotate_revoke_suite', side_effect=fake_service_runner):
            res = supervisor.run_dynsec_native_ci(report_path=None, dry_run=False)

        self.assertFalse(res['success'])
        self.assertEqual(res['verdict'], supervisor.VERDICT_INTERRUPTED)
        self.assertEqual(res['failure_code'], 'interrupted_by_signal')

        # Cleanup ran
        self.assertTrue(cleanup_ran['service'])
        # Teardown was not re-interrupted by the second signal
        self.assertEqual(cleanup_ran['signals_received_in_cleanup'], 0)

        # Handlers restored
        self.assertEqual(signal.getsignal(signal.SIGINT), original_sigint)
        self.assertEqual(signal.getsignal(signal.SIGTERM), original_sigterm)

    # 9. Signal handler restoration error causes fail closed
    @patch('signal.signal')
    @patch('dynsec_native_ci.run_http_native_suite')
    @patch('dynsec_native_ci.run_native_rotate_revoke_suite')
    def test_signal_restoration_error_fails_closed(
        self, mock_service, mock_http, mock_signal
    ):
        mock_service.return_value = _make_passing_lane_result('service')
        mock_http.return_value = _make_passing_lane_result('http')

        # Simulate error during signal handler restoration
        def signal_side_effect(sig, handler):
            if handler != supervisor._sig_handler and handler != supervisor.SupervisorInterruptedError:
                raise OSError("signal restoration failed")
            return MagicMock()

        # Let registration succeed, restoration fail
        call_count = [0]
        def custom_signal(sig, handler):
            call_count[0] += 1
            if call_count[0] > 2:  # restoration calls
                raise OSError("simulated restoration error")
            return MagicMock()

        mock_signal.side_effect = custom_signal

        res = supervisor.run_dynsec_native_ci(report_path=None, dry_run=False)
        self.assertFalse(res['success'])
        self.assertEqual(res['verdict'], supervisor.VERDICT_RESTORATION_ERROR)
        self.assertEqual(res['failure_code'], 'signal_handler_restoration_failed')

    # 10. CLI prints DYNSEC typo-free verdict (Item 4)
    @patch('sys.argv', ['dynsec_native_ci.py', '--dry-run'])
    def test_cli_output_spells_dynsec_correctly(self):
        with patch('builtins.print') as mock_print:
            with self.assertRaises(SystemExit) as cm:
                supervisor.main()
            self.assertNotEqual(cm.exception.code, 0)
            mock_print.assert_called_once()
            output = mock_print.call_args[0][0]
            self.assertTrue(output.startswith('DYNSEC_NATIVE_CI_VERDICT: '))
            self.assertNotIn('DYNDEC', output)


if __name__ == '__main__':
    unittest.main()
