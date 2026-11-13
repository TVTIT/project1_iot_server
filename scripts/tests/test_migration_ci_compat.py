"""Focused fixture security and compatibility contracts (no Docker)."""
import importlib.util
from pathlib import Path
import re
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


class HarnessSecurity(unittest.TestCase):
    def test_current_stage2_runner_passes_latest_terminal_to_all_verifiers(self):
        terminal = max(int(path.name[:6]) for path in (ROOT / 'migrations').glob('*.up.sql'))
        source = (ROOT / 'scripts/test-stage2-migrations.sh').read_text()
        pins = [int(value) for value in re.findall(r'-v expected_schema_terminal=(\d+)', source)]
        self.assertEqual(pins, [terminal] * 3)

    def test_migration23_verifier_defaults_to_history_but_checks_requested_terminal(self):
        source = (ROOT / 'scripts/sql/verify-migration-000023.sql').read_text()
        self.assertIn(r'\if :{?expected_schema_terminal}', source)
        self.assertIn(r'\set expected_schema_terminal 23', source)
        self.assertIn('max(version) = :expected_schema_terminal', source)
        self.assertIn(r'\if :terminal_verified', source)
        self.assertIn('unexpected terminal schema', source)
        self.assertIn("version=23 AND name='sensor_control_intent'", source)
        self.assertNotRegex(source, r'version\s*>\s*23')

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
        migration24 = (ROOT / 'migrations/000024_telemetry_ledger_timestamp.up.sql').read_text()
        seen_migration24 = False
        def sql_stub(query, **kwargs):
            nonlocal seen_migration24
            if query == migration24 and not seen_migration24:
                seen_migration24 = True
                raise self.m.Failure('ERROR: Migration 24 conflicting telemetry provenance')
            return ''
        with patch.object(self.f, 'sql', side_effect=sql_stub) as sql, \
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
        terminal = max(int(path.name[:6]) for path in (ROOT / 'migrations').glob('*.up.sql'))
        for version in (21, 22, 23):
            verifier = next((ROOT / 'scripts/sql').glob(f'verify-migration-{version:06d}.sql')).read_text()
            calls = [call for call in sql.call_args_list if call.args[0] == verifier]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0].kwargs.get('expected_schema_terminal'), terminal)

    def test_sql_passes_explicit_terminal_to_psql(self):
        with patch.object(self.f, 'command', return_value='verified') as command:
            self.assertEqual(self.f.sql('SELECT 1;', expected_schema_terminal=26), 'verified')
        args, kwargs = command.call_args
        self.assertIn('expected_schema_terminal=26', args[0])
        self.assertEqual(kwargs['input'], 'SELECT 1;\n')

    def test_migration24_fixture_covers_rollback_backfill_and_reapply(self):
        source = (ROOT / 'scripts/tests/migration_ci_compat.py').read_text()
        compose = (ROOT / 'docker-compose.yml').read_text()
        self.assertIn("migration24 = (ROOT / 'migrations/000024_telemetry_ledger_timestamp.up.sql').read_text()", source)
        self.assertIn("self.sql(migration24)\n        self.sql(migration24)", source)
        self.assertIn("conflicting telemetry provenance", source)
        self.assertIn("telemetry_ledger_timestamp", source)
        self.assertIn("000024_telemetry_ledger_timestamp.up.sql", compose)
        self.assertIn("000025_drop_telemetry_sensor_fk.up.sql", compose)
        self.assertLess(compose.index('000023_sensor_control_intent.up.sql'),
                        compose.index('000024_telemetry_ledger_timestamp.up.sql'))
        self.assertLess(compose.index('000024_telemetry_ledger_timestamp.up.sql'),
                        compose.index('000025_drop_telemetry_sensor_fk.up.sql'))

    def test_migration26_fixture_covers_redundant_index_removal(self):
        source = (ROOT / 'scripts/tests/migration_ci_compat.py').read_text()
        compose = (ROOT / 'docker-compose.yml').read_text()
        self.assertIn("migration26 = (ROOT / 'migrations/000026_drop_telemetry_redundant_indexes.up.sql').read_text()", source)
        self.assertIn("self.sql(migration26)", source)
        self.assertIn("drop_telemetry_redundant_indexes", source)
        self.assertIn("000026_drop_telemetry_redundant_indexes.up.sql", compose)
        self.assertLess(compose.index('000025_drop_telemetry_sensor_fk.up.sql'),
                        compose.index('000026_drop_telemetry_redundant_indexes.up.sql'))


    def test_stage3_writer_fixtures_apply_post_auth_schema24_before_writer(self):
        source = (ROOT / 'scripts/tests/stage3_persistence_runner.py').read_text()
        helper = source[source.index('def apply_post_auth_compatibility'):source.index('def run_normal_fixture')]
        self.assertIn("fixture.sql(b\"CREATE SCHEMA IF NOT EXISTS auth", helper)
        for version, name in ((21, 'admin_user_management'), (22, 'gateway_membership_revocation'),
                              (23, 'sensor_control_intent'), (24, 'telemetry_ledger_timestamp'),
                              (25, 'drop_telemetry_sensor_fk'), (26, 'drop_telemetry_redundant_indexes')):
            self.assertIn(f"({version}, '{name}')", helper)
        self.assertIn("expected = (b'21:admin_user_management'", helper)
        for fixture in ('run_normal_fixture', 'run_group_fixture'):
            body = source[source.index(f'def {fixture}'):]
            self.assertLess(body.index('apply_post_auth_compatibility(fixture)'), body.index('run_go_integration('))

    def test_native_writer_cases_assert_schema24_timestamp(self):
        source = (ROOT / 'src/internal/telemetry/postgres_integration_test.go').read_text()
        self.assertIn('sample_count=100 AND timestamp=$4', source)
        self.assertIn('sample_count,timestamp)::text FROM processed_messages', source)

    def test_stage4_live_telemetry_writer_fixture_uses_schema24(self):
        runner = (ROOT / 'scripts/tests/stage4_api_runner.py').read_text()
        scenario = (ROOT / 'scripts/tests/stage4_vertical_scenario.py').read_text()
        self.assertIn('TERMINAL_SCHEMA_VERSION = 26', runner)
        self.assertIn('run_existing_migration_runner(fixture, TERMINAL_SCHEMA_VERSION)', runner)
        self.assertIn("producer.publish_qos1(cap_topic, cap_payload)", scenario)

    def test_stage3_live_writer_fixture_upgrades_to_schema24(self):
        for filename, seed_call in (
                ('stage3_mqtt_ingress_runner.py', 'seed_graph(fixture)'),
                ('stage3_workload_runner.py', 'seed_graph(fixture,'),
                ('stage3_uplink_runner.py', 'seed_graph(fixture,')):
            source = (ROOT / 'scripts/tests' / filename).read_text()
            self.assertIn('apply_post_auth_compatibility', source, filename)
            fixture_start = source.index("run_existing_migration_runner(fixture, 20)")
            self.assertLess(source.index('apply_post_auth_compatibility(fixture)', fixture_start),
                            source.index(seed_call, fixture_start), filename)

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
