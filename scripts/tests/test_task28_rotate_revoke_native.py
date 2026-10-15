"""Unit tests for task28_rotate_revoke_native runner.

Offline tests only:
- noimage/missingreceipt -> noallocation/nativeexec
- stdin only secrets, exact test selector/env/timeout and no host PG ports
- migration selection only up 0..19, no historical regression/red/reapply runs
- PASS/SKIP/failed native classification; cleanup failure veto
- partial setup/test exception still invokes owned cleanup in safe order
"""
import unittest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
import json

import scripts.tests.task28_rotate_revoke_native as runner


class TestTask28RotateRevokeNative(unittest.TestCase):

    def test_preflight_missing_image_or_inspect_failure_blocks_allocation_and_dryrun(self):
        """noimage/missingreceipt -> noallocation/nativeexec; dryrun labels dryrun only and compile cwd/target verified."""
        with patch('scripts.tests.task28_rotate_revoke_native.inspect_local_image') as mock_insp:
            mock_insp.return_value = (False, None, "image_not_found")
            res = runner.run_native_rotate_revoke_suite(dry_run=False)
            self.assertFalse(res['success'])
            self.assertEqual(res['failure_stage'], 'preflight')
            self.assertEqual(res['failure_code'], 'image_not_found')
            self.assertFalse(res.get('allocated', False))

        # Test dry-run labels dryrun only, not nativePASS
        with patch('scripts.tests.task28_rotate_revoke_native.inspect_local_image') as mock_insp:
            mock_insp.return_value = (True, {}, None)
            res_dry = runner.run_native_rotate_revoke_suite(dry_run=True)
            self.assertFalse(res_dry['success'])
            self.assertEqual(res_dry.get('aggregate', {}).get('verdict'), 'DRY_RUN_ONLY')
            self.assertEqual(res_dry.get('failure_code'), 'dry_run_complete')

        # Test Go test binary compile runs in ROOT/'src' targeting './internal/mqttcredential'
        with patch('subprocess.run') as mock_run:
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_run.return_value = mock_proc
            out_p = Path('/tmp/fake.test')
            ok, err = runner.compile_go_test_binary(out_p)
            self.assertTrue(ok)
            self.assertIsNone(err)
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            call_cmd = mock_run.call_args[0][0]
            self.assertEqual(call_kwargs['cwd'], runner.ROOT / 'src')
            self.assertEqual(call_cmd[-1], './internal/mqttcredential')

            # Failure must return safe code without raw stderr string
            mock_proc.returncode = 1
            mock_proc.stderr = b"raw stderr sensitive leak details"
            ok, err = runner.compile_go_test_binary(out_p)
            self.assertFalse(ok)
            self.assertEqual(err, 'compilation_failed')
            self.assertNotIn('sensitive', err)

    def test_stdin_only_secrets_foreign_or_missing_receipt_zero_exec(self):
        """stdin only secrets, exact test selector/env/timeout and foreign/missing receipt blocks exec."""
        payload_data = runner.build_native_test_input(
            pg_alias="task28a-pg",
            pg_password="pg-secret-pass-12345",
            manager_username="admin",
            manager_password="broker-admin-pass-5678",
            run_marker="runmarker12345",
            project_binding="dynsec_probe_1234567890ab",
        )
        # Verify JSON conforms to strict Go contract
        self.assertEqual(payload_data['test_pg_dsn'], 'postgres://fixture_admin:pg-secret-pass-12345@task28a-pg:5432/fixture_service?sslmode=disable')
        self.assertEqual(payload_data['broker_url'], 'ssl://localhost:8883')
        self.assertEqual(payload_data['ca_path'], '/security/ca.crt')
        self.assertEqual(payload_data['store_path'], '/security/dynsec.json')
        self.assertEqual(payload_data['writer_uid'], 1883)
        self.assertEqual(payload_data['project_binding'], 'dynsec_probe_1234567890ab')

        cmd = runner.build_native_exec_cmd("broker_container_id_123")
        expected_cmd = [
            'docker', 'exec', '-i', '-u1883:1883',
            '-e', 'TASK28A_NATIVE_ISOLATED=1',
            'broker_container_id_123',
            '/security/task28a-native.test',
            '-test.run', '^TestSimplifiedServiceNativeRotateRevoke$',
            '-test.v',
            '-test.timeout', '60s',
        ]
        self.assertEqual(cmd, expected_cmd)

        # Unit mock missing/foreign receipt blocks actual orchestration (zero exec)
        with patch('scripts.tests.task28_rotate_revoke_native.inspect_local_image', return_value=(True, {}, None)), \
             patch('scripts.tests.task28_rotate_revoke_native.create_verified_host_fixture_dir', return_value=(Path('/tmp/fake'), {'status': 'created'})), \
             patch('scripts.tests.task28_rotate_revoke_native.compile_go_test_binary', return_value=(True, None)), \
             patch('scripts.tests.task28_rotate_revoke_native.DatabaseFixture') as mock_db_cls, \
             patch('scripts.tests.task28_rotate_revoke_native.DynSecProbeFixture') as mock_broker_cls, \
             patch('scripts.tests.task28_rotate_revoke_native.inspect_owned_resource') as mock_insp_owned, \
             patch('scripts.tests.task28_rotate_revoke_native.get_ordered_migrations', return_value=[]), \
             patch('subprocess.run') as mock_subproc:

            mock_insp_owned.return_value = {'status': 'owned', 'resource_id': 'broker_cid_123'}

            mock_pg = MagicMock()
            mock_pg.container_id = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
            mock_pg.passwords = ['pgpass']
            mock_pg._load_and_verify_receipt.return_value = 'different_cid_than_original'
            mock_db_cls.return_value = mock_pg

            mock_broker = MagicMock()
            mock_broker.project = 'dynsec_probe_1234567890ab'
            mock_broker.passwords = {'admin': 'adminpass'}
            mock_broker.verified_receipts = [
                {'kind': 'container', 'resource_id': 'broker_cid_123', 'expected_role': 'broker', 'expected_project': 'dynsec_probe_1234567890ab', 'expected_name': 'dynsec_probe_1234567890ab-broker-1'},
                {'kind': 'network', 'resource_id': 'net_123', 'expected_role': 'default', 'expected_project': 'dynsec_probe_1234567890ab', 'expected_name': 'dynsec_probe_1234567890ab_default'},
            ]
            mock_broker_cls.return_value = mock_broker

            # Foreign PG receipt -> zero network/CP/exec
            res = runner.run_native_rotate_revoke_suite(dry_run=False)
            self.assertFalse(res['success'])
            self.assertEqual(res['failure_stage'], 'reverify_receipts')
            self.assertEqual(res['failure_code'], 'pg_receipt_mismatch')
            # Verify ZERO docker network connect / cp / exec happened
            self.assertEqual(mock_subproc.call_count, 0)

            # Test missing broker receipt also blocks with zero exec
            mock_broker.verified_receipts = []
            res_missing = runner.run_native_rotate_revoke_suite(dry_run=False)
            self.assertFalse(res_missing['success'])
            self.assertEqual(res_missing['failure_stage'], 'reverify_receipts')
            self.assertEqual(res_missing['failure_code'], 'missing_broker_receipt')
            self.assertEqual(mock_subproc.call_count, 0)

    def test_migration_selection_only_up_0_to_19(self):
        """migration selection only up 0..19, no historical regression/red/reapply runs."""
        mock_fixture = MagicMock()
        migrations = runner.get_ordered_migrations(runner.ROOT / 'migrations')
        
        # Verify migration 000000 is .sh, 000001..000019 are .up.sql
        self.assertTrue(len(migrations) >= 20)
        self.assertEqual(migrations[0].name, '000000_configure_supabase_roles.sh')
        for m in migrations[1:]:
            self.assertTrue(m.name.endswith('.up.sql'))
            idx = int(m.name[:6])
            self.assertTrue(1 <= idx <= 19)

        # No down or regression sql in list
        for m in migrations:
            self.assertNotIn('.down.', m.name)
            self.assertNotIn('regression', m.name)

    def test_native_outcome_classification(self):
        """PASS/SKIP/failed native classification; cleanup failure veto."""
        # Case 1: PASS output with exit 0
        pass_out = b"=== RUN   TestSimplifiedServiceNativeRotateRevoke\n--- PASS: TestSimplifiedServiceNativeRotateRevoke (1.23s)\nPASS\n"
        cls1 = runner.classify_native_test_output(0, pass_out)
        self.assertEqual(cls1['status'], 'PASS')
        self.assertTrue(cls1['executed'])
        self.assertEqual(cls1['summary'], 'test_passed')

        # Case 2: SKIP output with exit 0
        skip_out = b"=== RUN   TestSimplifiedServiceNativeRotateRevoke\n--- SKIP: TestSimplifiedServiceNativeRotateRevoke (0.00s)\n    service_simplified_native_test.go:340: skipping native isolated test\nPASS\n"
        cls2 = runner.classify_native_test_output(0, skip_out)
        self.assertEqual(cls2['status'], 'SKIP')
        self.assertFalse(cls2['executed'])
        self.assertEqual(cls2['summary'], 'test_skipped')

        # Case 3: FAIL output with exit 1 after RUN marker
        fail_out = b"=== RUN   TestSimplifiedServiceNativeRotateRevoke\n--- FAIL: TestSimplifiedServiceNativeRotateRevoke (0.50s)\nFAIL\n"
        cls3 = runner.classify_native_test_output(1, fail_out)
        self.assertEqual(cls3['status'], 'FAIL')
        self.assertTrue(cls3['executed'])
        self.assertEqual(cls3['summary'], 'test_failed')

        # Case 4: Launch failure (exit 126, no RUN marker) -> executed=False, launch_permission_denied
        cls4 = runner.classify_native_test_output(126, b"", b"OCI runtime exec failed: exec: \"/security/task28a-native.test\": permission denied: unknown")
        self.assertEqual(cls4['status'], 'FAIL')
        self.assertFalse(cls4['executed'])
        self.assertEqual(cls4['returncode'], 126)
        self.assertEqual(cls4['summary'], 'launch_permission_denied')
        self.assertEqual(cls4['launch_error'], 'launch_permission_denied')

        # Case 5: Launch failure (exit 127, no RUN marker) -> executed=False, launch_not_found
        cls5 = runner.classify_native_test_output(127, b"", b"sh: /security/task28a-native.test: not found")
        self.assertEqual(cls5['status'], 'FAIL')
        self.assertFalse(cls5['executed'])
        self.assertEqual(cls5['returncode'], 127)
        self.assertEqual(cls5['summary'], 'launch_not_found')
        self.assertEqual(cls5['launch_error'], 'launch_not_found')

        # Cleanup failure veto: even if native PASS, if cleanup fails, aggregate is NOT success
        agg = runner.evaluate_aggregate_result(native_status='PASS', pg_cleanup_verified=True, broker_cleanup_verified=False, host_cleanup_verified=True)
        self.assertFalse(agg['success'])
        self.assertEqual(agg['verdict'], 'CLEANUP_FAILURE_VETO')

        # Clean PASS:
        agg_pass = runner.evaluate_aggregate_result(native_status='PASS', pg_cleanup_verified=True, broker_cleanup_verified=True, host_cleanup_verified=True)
        self.assertTrue(agg_pass['success'])
        self.assertEqual(agg_pass['verdict'], 'SUCCESS')

    def test_partial_setup_or_test_exception_cleans_up_in_safe_order(self):
        """partial setup/test exception still invokes owned cleanup in safe order, sentinel not leaked."""
        cleanup_calls = []

        mock_pg = MagicMock()
        def pg_clean():
            cleanup_calls.append('pg_cleanup')
            return True
        mock_pg.cleanup.side_effect = pg_clean

        mock_broker = MagicMock()
        def broker_clean(*args, **kwargs):
            cleanup_calls.append('broker_cleanup')
            return {'clean': True}
        mock_broker.cleanup.side_effect = broker_clean

        host_receipt = {'status': 'created'}
        def host_clean(r):
            cleanup_calls.append('host_cleanup')
            return {'status': 'already_absent', 'verified': True}

        # 1. Execute safe teardown helper with host already_absent verified True
        clean_res = runner.execute_safe_teardown(
            pg_fixture=mock_pg,
            broker_fixture=mock_broker,
            host_receipt=host_receipt,
            host_cleaner=host_clean,
        )

        self.assertEqual(cleanup_calls, ['pg_cleanup', 'broker_cleanup', 'host_cleanup'])
        self.assertTrue(clean_res['pg_cleanup_verified'])
        self.assertTrue(clean_res['broker_cleanup_verified'])
        self.assertTrue(clean_res['host_cleanup_verified'])

        # 2. Check exception handling: sentinel exception string must NOT appear in output dict
        mock_pg_err = MagicMock()
        mock_pg_err.cleanup.side_effect = RuntimeError("SECRET_DB_PASSWORD_SENTINEL_LEAK")
        mock_broker_err = MagicMock()
        mock_broker_err.cleanup.side_effect = RuntimeError("SECRET_BROKER_KEY_SENTINEL_LEAK")
        def host_clean_err(r):
            raise RuntimeError("SECRET_HOST_PATH_SENTINEL_LEAK")

        err_clean_res = runner.execute_safe_teardown(
            pg_fixture=mock_pg_err,
            broker_fixture=mock_broker_err,
            host_receipt=host_receipt,
            host_cleaner=host_clean_err,
        )
        self.assertFalse(err_clean_res['pg_cleanup_verified'])
        self.assertFalse(err_clean_res['broker_cleanup_verified'])
        self.assertFalse(err_clean_res['host_cleanup_verified'])
        res_json = json.dumps(err_clean_res)
        self.assertNotIn('SECRET_DB_PASSWORD_SENTINEL_LEAK', res_json)
        self.assertNotIn('SECRET_BROKER_KEY_SENTINEL_LEAK', res_json)
        self.assertNotIn('SECRET_HOST_PATH_SENTINEL_LEAK', res_json)

        # 3. Main orchestration exception caught, aggregate computed, sentinel never leaked
        with patch('scripts.tests.task28_rotate_revoke_native.inspect_local_image', return_value=(True, {}, None)), \
             patch('scripts.tests.task28_rotate_revoke_native.create_verified_host_fixture_dir', return_value=(Path('/tmp/fake'), {'status': 'created'})), \
             patch('scripts.tests.task28_rotate_revoke_native.compile_go_test_binary', side_effect=RuntimeError("SENTINEL_UNEXPECTED_SETUP_FAILURE")):

            orch_res = runner.run_native_rotate_revoke_suite(dry_run=False)
            self.assertFalse(orch_res['success'])
            self.assertEqual(orch_res['failure_stage'], 'orchestration_error')
            self.assertEqual(orch_res['failure_code'], 'orchestration_exception')
            self.assertIn('aggregate', orch_res)
            self.assertIn('cleanup', orch_res)
            orch_json = json.dumps(orch_res)
            self.assertNotIn('SENTINEL_UNEXPECTED_SETUP_FAILURE', orch_json)

    def test_streamed_binary_transfer_command_and_execution_contract(self):
        """Assert streamed transfer uses exact tracked ID/u1883:1883/constant destination/stdin file/finite timeout.

        docker cp no longer used for binary (roles.sh PG copy remains legitimate);
        transfer failure stops native exec and triggers existing safe cleanup order.
        """
        # 1. build_native_transfer_cmd builds constant command with tracked cid and -u1883:1883
        transfer_cmd = runner.build_native_transfer_cmd("broker_cid_tracked_456")
        expected_cmd = [
            'docker', 'exec', '-i', '-u1883:1883',
            'broker_cid_tracked_456',
            'sh', '-c', 'cat > /security/task28a-native.test && chmod 0700 /security/task28a-native.test'
        ]
        self.assertEqual(transfer_cmd, expected_cmd)

        # 2. transfer_binary_to_broker streams through stdin file handle with finite timeout (e.g. 30s)
        with patch('subprocess.run') as mock_run:
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_run.return_value = mock_proc

            fake_bin = Path('/tmp/fake_bin.test')
            with patch('builtins.open', unittest.mock.mock_open(read_data=b'binary_content')) as mock_file:
                ok, err = runner.transfer_binary_to_broker("broker_cid_tracked_456", fake_bin)
                self.assertTrue(ok)
                self.assertIsNone(err)
                mock_run.assert_called_once()
                call_args, call_kwargs = mock_run.call_args
                self.assertEqual(call_args[0], expected_cmd)
                self.assertEqual(call_kwargs['timeout'], 30)
                # Verify stdin is passed as file handle (not input=bytes in argv/RAM)
                self.assertIn('stdin', call_kwargs)
                self.assertNotIn('input', call_kwargs)

            # Failure path returns sanitized stage/code without leaking raw stderr
            # Test each classified failure code
            cases = [
                (1, b"cat: write error: Permission denied to /security with token 9999", 'failed_to_transfer_binary_rc_1_transfer_permission_denied'),
                (1, b"cat: write error: Read-only file system with token 9999", 'failed_to_transfer_binary_rc_1_transfer_readonly_filesystem'),
                (1, b"sh: /security: No such file or directory with token 9999", 'failed_to_transfer_binary_rc_1_transfer_missing_path'),
                (2, b"cat: generic unknown write error with token 9999", 'failed_to_transfer_binary_rc_2_transfer_failed'),
            ]
            for rc, stderr_bytes, expected_code in cases:
                mock_proc.returncode = rc
                mock_proc.stderr = stderr_bytes
                with patch('builtins.open', unittest.mock.mock_open(read_data=b'binary_content')):
                    ok, err = runner.transfer_binary_to_broker("broker_cid_tracked_456", fake_bin)
                    self.assertFalse(ok)
                    self.assertEqual(err, expected_code)
                    self.assertNotIn('token 9999', err)
                    self.assertNotIn('sensitive', err)

        # 3. In run_native_rotate_revoke_suite, verify transfer failure stops native exec and triggers safe cleanup order
        with patch('scripts.tests.task28_rotate_revoke_native.inspect_local_image', return_value=(True, {}, None)), \
             patch('scripts.tests.task28_rotate_revoke_native.create_verified_host_fixture_dir', return_value=(Path('/tmp/fake'), {'status': 'created'})), \
             patch('scripts.tests.task28_rotate_revoke_native.compile_go_test_binary', return_value=(True, None)), \
             patch('scripts.tests.task28_rotate_revoke_native.DatabaseFixture') as mock_db_cls, \
             patch('scripts.tests.task28_rotate_revoke_native.DynSecProbeFixture') as mock_broker_cls, \
             patch('scripts.tests.task28_rotate_revoke_native.inspect_owned_resource') as mock_insp_owned, \
             patch('scripts.tests.task28_rotate_revoke_native.get_ordered_migrations', return_value=[]), \
             patch('scripts.tests.task28_rotate_revoke_native.transfer_binary_to_broker', return_value=(False, 'failed_to_transfer_binary_rc_1_transfer_permission_denied')) as mock_transfer, \
             patch('scripts.tests.task28_rotate_revoke_native.execute_safe_teardown') as mock_teardown, \
             patch('subprocess.run') as mock_subproc:

            mock_insp_owned.return_value = {'status': 'owned', 'resource_id': 'broker_cid_123'}

            mock_pg = MagicMock()
            mock_pg.container_id = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
            mock_pg.passwords = ['pgpass']
            mock_pg._load_and_verify_receipt.return_value = mock_pg.container_id
            mock_db_cls.return_value = mock_pg

            mock_broker = MagicMock()
            mock_broker.project = 'dynsec_probe_1234567890ab'
            mock_broker.passwords = {'admin': 'adminpass'}
            mock_broker.verified_receipts = [
                {'kind': 'container', 'resource_id': 'broker_cid_123', 'expected_role': 'broker', 'expected_project': 'dynsec_probe_1234567890ab', 'expected_name': 'dynsec_probe_1234567890ab-broker-1'},
                {'kind': 'network', 'resource_id': 'net_123', 'expected_role': 'default', 'expected_project': 'dynsec_probe_1234567890ab', 'expected_name': 'dynsec_probe_1234567890ab_default'},
            ]
            mock_broker_cls.return_value = mock_broker

            mock_teardown.return_value = {
                'pg_cleanup_verified': True,
                'broker_cleanup_verified': True,
                'host_cleanup_verified': True,
            }

            # subprocess.run for network connect succeeds
            mock_connect_proc = MagicMock()
            mock_connect_proc.returncode = 0
            mock_subproc.return_value = mock_connect_proc

            res = runner.run_native_rotate_revoke_suite(dry_run=False)

            self.assertFalse(res['success'])
            self.assertEqual(res['failure_stage'], 'binary_transfer')
            self.assertEqual(res['failure_code'], 'failed_to_transfer_binary_rc_1_transfer_permission_denied')
            # Verify transfer was called
            mock_transfer.assert_called_once()
            # Verify native exec was NEVER called
            self.assertNotIn('native_test_result', res)
            # Verify safe cleanup was called
            mock_teardown.assert_called_once()
            # Verify docker cp was never called for binary
            for call_item in mock_subproc.call_args_list:
                args = call_item[0][0]
                self.assertFalse(len(args) >= 2 and args[0] == 'docker' and args[1] == 'cp' and '/security/task28a-native.test' in args[-1])


if __name__ == '__main__':
    unittest.main()
