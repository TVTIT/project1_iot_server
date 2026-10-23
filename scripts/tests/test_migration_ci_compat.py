"""Focused fixture security and compatibility contracts (no Docker)."""
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


class HarnessSecurity(unittest.TestCase):
    def test_credentials_are_random_and_not_argument_values(self):
        text = (ROOT / 'scripts/test-stage2-migrations.sh').read_text()
        self.assertIn('secrets.token_hex(32)', text)
        for value in ('stage2_admin_password', 'stage2_backend_password',
                      'stage2_auth_password_1234', 'stage2_storage_password_1234',
                      '-e PGPASSWORD="$ADMIN_PASSWORD"', 'env PGPASSWORD="$BACKEND_PASSWORD"'):
            self.assertNotIn(value, text)

    def test_readiness_does_not_print_raw_logs(self):
        text = (ROOT / 'scripts/test-stage2-migrations.sh').read_text()
        self.assertNotIn('docker logs', text)
        self.assertIn('Database readiness failed.', text)


class CompatFixture(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('compat', ROOT / 'scripts/tests/migration_ci_compat.py')
        self.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.m)
        self.f = self.m.Fixture()
        self.f.id = 'a' * 64

    def test_false_readiness_is_not_success(self):
        with patch.object(self.f, 'sql', return_value='0'), patch.object(self.m.time, 'sleep'):
            with self.assertRaises(self.m.Failure):
                self.f.wait_ready(attempts=2)

    def test_missing_assertion_fails(self):
        with patch.object(self.f, 'sql', return_value='0'):
            with self.assertRaises(self.m.Failure):
                self.f.expect('SELECT count(*) FROM pg_policies', '6')

    def test_wrong_ownership_never_removes(self):
        with patch.object(self.f, 'command', return_value='other') as command:
            with self.assertRaises(self.m.Failure):
                self.f.cleanup()
            self.assertEqual(command.call_count, 1)

    def test_daemon_error_is_not_absence(self):
        with patch.object(self.f, 'command', side_effect=[self.f.nonce, '', self.m.Failure('daemon')]):
            with self.assertRaises(self.m.Failure):
                self.f.cleanup()

    def test_cleanup_remaining_id_vetoes(self):
        with patch.object(self.f, 'command', side_effect=[self.f.nonce, '', self.f.id]):
            with self.assertRaises(self.m.Failure):
                self.f.cleanup()

    def test_remove_failure_vetoes(self):
        with patch.object(self.f, 'command', side_effect=[self.f.nonce, self.m.Failure('remove')]):
            with self.assertRaises(self.m.Failure):
                self.f.cleanup()

    def test_role_login_failure_is_fatal(self):
        with patch.object(self.f, 'sql', side_effect=self.m.Failure('login')):
            with self.assertRaises(self.m.Failure):
                self.f.login('supabase_auth_admin', 'secret')

    def test_subprocess_failure_never_leaks(self):
        with patch.object(self.m.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, 'secret', 'secret')) as run:
            with self.assertRaises(self.m.Failure) as caught:
                self.f.command(['docker', 'version'], input='secret')
            self.assertNotIn('secret', str(caught.exception))
            self.assertGreater(run.call_args.kwargs['timeout'], 0)

    def test_exact_coverage_and_order(self):
        with patch.object(self.f, 'sql', return_value='') as sql, \
             patch.object(self.f, 'expect') as expect, patch.object(self.f, 'login') as login:
            self.f.verify()
        inputs = [call.args[0] for call in sql.call_args_list]
        for name in ('verify-schema-hardening.sql', 'verify-migration-000009.sql'):
            self.assertIn((ROOT / 'scripts/sql' / name).read_text(), inputs)
        compat = (ROOT / 'migrations/000004_supabase_compat.up.sql').read_text()
        self.assertEqual(inputs.count(compat), 2)
        self.assertTrue(inputs[2].startswith('CREATE TABLE auth.users'))
        queries = ' '.join(call.args[0] for call in expect.call_args_list)
        for required in ('pg_policies', 'on_auth_user_created', 'profiles_id_fkey', 'supabase_compat'):
            self.assertIn(required, queries)
        self.assertEqual([c.args[0] for c in login.call_args_list], ['supabase_auth_admin', 'supabase_storage_admin', 'iot_backend_app'])
        migration22 = (ROOT / 'migrations/000022_gateway_membership_revocation.up.sql').read_text()
        self.assertEqual(inputs.count(migration22), 2)
        self.assertIn((ROOT / 'scripts/sql/verify-migration-000022.sql').read_text(), inputs)
        migration23 = (ROOT / 'migrations/000023_sensor_control_intent.up.sql').read_text()
        self.assertEqual(inputs.count(migration23), 2)
        self.assertIn((ROOT / 'scripts/sql/verify-migration-000023.sql').read_text(), inputs)
        self.assertIn("DELETE FROM public.user_gateways WHERE false;", inputs)

    def test_login_secret_only_protected_stdin(self):
        with patch.object(self.f, 'command', return_value='supabase_auth_admin') as command:
            self.f.login('supabase_auth_admin', 'secret')
        args, kwargs = command.call_args
        self.assertNotIn('secret', ' '.join(args[0]))
        self.assertIn('secret', kwargs['input'])
        self.assertIn('127.0.0.1', args[0])

    def test_image_preflight_fails_before_create(self):
        self.f.id = None
        with patch.object(self.f, 'command', side_effect=self.m.Failure('image')) as command:
            with self.assertRaises(self.m.Failure):
                self.f.start()
        self.assertEqual(command.call_count, 1)
        self.assertIsNone(self.f.id)
        self.assertIn('inspect', command.call_args.args[0])

    def test_create_secrets_only_stdin_and_exact_receipt(self):
        self.f.id = None
        with patch.object(self.f, 'command', side_effect=['image', 'b' * 64, '']) as command:
            self.f.start()
        create = command.call_args_list[1]
        argv = ' '.join(create.args[0])
        self.assertIn('--pull=never', argv)
        for secret in self.f.passwords.values():
            self.assertNotIn(secret, argv)
            self.assertIn(secret, create.kwargs['input'])
        self.assertEqual(self.f.id, 'b' * 64)

    def test_cleanup_failure_vetoes_main_success(self):
        with patch.object(self.m, 'Fixture', return_value=self.f), \
             patch.object(self.f, 'start'), patch.object(self.f, 'wait_ready'), \
             patch.object(self.f, 'verify'), \
             patch.object(self.f, 'cleanup', side_effect=self.m.Failure('cleanup')):
            self.assertEqual(self.m.main(), 1)


if __name__ == '__main__':
    unittest.main()
