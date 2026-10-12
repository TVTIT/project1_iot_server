"""Unit tests for task28_supabase_auth_rehearsal.py.

Verifies offline regressions, token handling, gateway routes, signup denial,
receipt ownership gates, secret handling, partial cleanup order and unknown vetoes.
"""
from __future__ import annotations
import ast
import base64
import hashlib
import json
import stat
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.tests import task28_supabase_auth_rehearsal as r


class SupabaseAuthRehearsalOfflineTests(unittest.TestCase):
    def test_minimal_auth_prerequisite_and_compatibility_gate(self):
        pg = MagicMock()
        r.bootstrap_auth_prerequisites(pg)
        pg.sql.assert_called_once_with(
            b'CREATE ROLE postgres NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE '
            b'NOINHERIT NOREPLICATION NOBYPASSRLS;', expect=True)
        pg.reset_mock()
        for readiness in ({}, {'ready': False}):
            with self.assertRaises(RuntimeError):
                r.apply_auth_compatibility(pg, readiness)
        pg.sql.assert_not_called()
        r.apply_auth_compatibility(pg, {'ready': True})
        pg.sql.assert_called_once_with(
            (r.ROOT / 'migrations/000004_supabase_compat.up.sql').read_bytes(), expect=True)

    def test_pop_filename_and_undefined_static_role(self):
        text = ('error executing migrations/20240612123726_enable_rls_update_grants.up.sql, '
                'sql: SECRET_SENTINEL_SQL: ERROR: role "postgres" does not exist (SQLSTATE 42704)')
        detail = r.migration_failure_details(text)
        self.assertEqual(detail, {
            'migration_identifier': '20240612123726',
            'migration_label': '20240612123726_enable_rls_update_grants.up.sql',
            'sqlstate': '42704', 'category': 'undefined_object',
            'object_type': 'role', 'object_name': 'postgres'})
        self.assertNotIn('SECRET_SENTINEL', json.dumps(detail))

    def test_undefined_objects_do_not_leak_or_invent_identifiers(self):
        for kind in ('role', 'schema', 'table', 'type', 'function', 'extension'):
            for name in ('SECRET_SENTINEL', 'postgres SECRET_SENTINEL', 'postgres"evil', 'postgres.secret'):
                text = f'migration failed: {kind} "{name}" does not exist (SQLSTATE 42704)'
                detail = r.migration_failure_details(text)
                self.assertEqual(detail['category'], 'undefined_object')
                if '"' not in name:
                    self.assertEqual(detail['object_type'], kind)
                else:
                    self.assertNotIn('object_type', detail)
                self.assertNotIn('object_name', detail)
                self.assertNotIn('SECRET_SENTINEL', json.dumps(detail))
        for text in ('migration failed: role postgres does not exist (SQLSTATE 42704)',
                     'migration failed: SELECT role "postgres"; (SQLSTATE 42704)'):
            self.assertNotIn('object_name', r.migration_failure_details(text))
        detail = r.migration_failure_details(
            'error executing 20240612123726_SECRET_SENTINEL.up.sql, sql: hidden (SQLSTATE 42704)')
        self.assertEqual(detail['migration_identifier'], '20240612123726')
        self.assertNotIn('migration_label', detail)

    def test_nested_migration_fatal_safe_details(self):
        secret = 'SECRET_SENTINEL_PASSWORD_DSN_JWT'
        nested = {'msg': 'running db migrations', 'error': {
            'message': 'error executing migration 00_init_auth_schema.up.sql: '
                       'must be owner of table schema_migrations (SQLSTATE 42501)',
            'sql': secret, 'dsn': secret}}
        raw = json.dumps({'level': 'fatal', 'msg': json.dumps(nested), 'token': secret}).encode()
        self.assertEqual(r.classify_auth_fatal(raw), {
            'fatal_category': 'permission_denied', 'migration_diagnostic': {
                'migration_identifier': '00', 'sqlstate': '42501',
                'category': 'object_ownership', 'object_name': 'schema_migrations'}})
        self.assertNotIn(secret, json.dumps(r.classify_auth_fatal(raw)))

    def test_migration_diagnostic_explicit_codes_and_unknown_not_guessed(self):
        cases = [
            ('migration 20221208132122: column version does not exist (SQLSTATE 42703)',
             '20221208132122', '42703', 'missing_relation_or_column'),
            ('migration 00: permission denied for schema auth (SQLSTATE 42501)',
             '00', '42501', 'schema_privileges'),
            ('migration 00: relation already exists (SQLSTATE 42P07)',
             '00', '42P07', 'duplicate_object'),
            ('migration 00: syntax error (SQLSTATE 42601)', '00', '42601', 'invalid_syntax'),
            ('migration 00: extension is not available (SQLSTATE 0A000)',
             '00', '0A000', 'unsupported_extension'),
            ('migration failed SECRET_SENTINEL 123456 arbitrary.sql SQLSTATE INVALID',
             None, None, 'unknown'),
        ]
        for text, identifier, code, category in cases:
            with self.subTest(category=category):
                out = r.classify_auth_fatal(json.dumps({'level': 'fatal', 'error': {'message': text}}).encode())
                detail = out['migration_diagnostic']
                self.assertEqual(detail['migration_identifier'], identifier)
                self.assertEqual(detail['sqlstate'], code)
                self.assertEqual(detail['category'], category)
                self.assertLessEqual(set(detail), {'migration_identifier', 'sqlstate', 'category', 'object_name'})
                self.assertNotIn('SECRET_SENTINEL', json.dumps(out))

    def test_untrusted_fields_cannot_supply_diagnostic_details(self):
        raw = json.dumps({'level': 'fatal', 'msg': 'migration failed',
                          'token': 'migration 123456 SQLSTATE 42501',
                          'sql': 'must be owner of table schema_migrations'}).encode()
        self.assertEqual(r.classify_auth_fatal(raw)['migration_diagnostic'], {
            'migration_identifier': None, 'sqlstate': None, 'category': 'unknown'})

    def test_fatal_classifier_never_returns_secret_text(self):
        sentinel = 'SECRET_SENTINEL_PASSWORD_DSN_JWT'
        for message, category in [
            ('permission denied for schema auth (SQLSTATE 42501)', 'permission_denied'),
            ('password authentication failed', 'db_authentication'),
            ('relation missing does not exist (SQLSTATE 42P01)', 'missing_relation_or_column'),
            ('migration failed', 'migration_failure'),
            ('required key GOTRUE_JWT_SECRET missing value', 'missing_setting'),
            ('invalid configuration', 'invalid_config'),
            ('bind: address already in use', 'binding'),
            ('connection refused', 'connection_failure'),
            ('unrecognized failure', 'unknown'),
        ]:
            with self.subTest(category=category):
                raw = json.dumps({'level': 'fatal', 'msg': message + ' ' + sentinel}).encode()
                self.assertEqual(r.classify_auth_fatal(raw)['fatal_category'], category)
                self.assertNotIn(sentinel, json.dumps(r.classify_auth_fatal(raw)))
        self.assertEqual(r.classify_auth_fatal(b'{"level":"info","msg":"migration failed"}'),
                         {'fatal_category': 'unknown'})

    def test_auth_log_collection_requires_verified_exact_ownership(self):
        for receipt in (None, self.receipt('backend'), self.receipt('auth') | {'resource_id': 'b' * 64}):
            with patch.object(r, 'docker') as docker_mock:
                with self.assertRaises(RuntimeError):
                    r.auth_exit_diagnostic('a' * 64, receipt)
                docker_mock.assert_not_called()
        with patch.object(r, 'inspect_owned_resource', return_value={'status': 'unknown'}), \
             patch.object(r, 'docker') as docker_mock:
            with self.assertRaises(RuntimeError):
                r.auth_exit_diagnostic('a' * 64, self.receipt('auth'))
            docker_mock.assert_not_called()

    def test_owned_exit_diagnostic_and_early_stop_retained(self):
        lifecycle = {'status': 'exited', 'exit_code': 1}
        with patch.object(r, 'require_consumer_owned'), \
             patch.object(r, 'inspect_container_lifecycle', return_value=lifecycle), \
             patch.object(r, 'docker', return_value=b'{"level":"fatal","msg":"migration failed SECRET"}') as logs:
            diagnostic = r.auth_exit_diagnostic('a' * 64, self.receipt('auth'))
            self.assertEqual({k: diagnostic[k] for k in ('status', 'exit_code', 'fatal_category')},
                             lifecycle | {'fatal_category': 'migration_failure'})
            logs.assert_called_once_with(['logs', '--tail', '100', 'a' * 64], timeout=5, capture_stderr=True)
        with patch.object(r, 'inspect_container_lifecycle', return_value=lifecycle), \
             patch.object(r, 'auth_exit_diagnostic', return_value=lifecycle | {'fatal_category': 'unknown'}), \
             patch.object(r, 'http_call') as http, patch.object(r.time, 'sleep') as sleep:
            result = r.probe_auth_gateway_readiness(80, 'secret', 'a' * 64,
                                                     auth_receipt=self.receipt('auth'))
            self.assertFalse(result['ready'])
            self.assertEqual(result['auth_container']['exit_code'], 1)
            http.assert_not_called()
            sleep.assert_not_called()
        clean = {role + '_cleanup_verified': True for role in
                 ('nginx', 'backend', 'envoy', 'auth', 'pg', 'broker', 'host')}
        self.assertEqual(r.aggregate('FAIL', clean)['verdict'], 'NOT_PASSED')
        clean['auth_cleanup_verified'] = False
        self.assertEqual(r.aggregate('FAIL', clean)['verdict'], 'CLEANUP_FAILURE_VETO')

    def receipt(self, role: str = 'backend') -> dict:
        return {
            'resource_id': 'a' * 64,
            'expected_project': 'p',
            'expected_role': role,
            'expected_name': f'p-{role}-1'
        }

    def test_missing_or_mismatched_ownership_blocks_start(self):
        for owned in ({'status': 'unknown'}, {'status': 'owned', 'resource_id': 'b' * 64}):
            with patch.object(r, 'inspect_owned_resource', return_value=owned), \
                 patch.object(r.subprocess, 'run') as run:
                with self.assertRaises(RuntimeError):
                    r.start_container('a' * 64, self.receipt('auth'))
                run.assert_not_called()

    def test_consumer_ownership_requires_exact_receipt(self):
        with patch.object(r, 'inspect_owned_resource', return_value={'status': 'owned', 'resource_id': 'a' * 64}):
            r.require_consumer_owned('a' * 64, self.receipt('envoy'))
        with patch.object(r, 'inspect_owned_resource', return_value={'status': 'not_owned'}):
            with self.assertRaises(RuntimeError):
                r.require_consumer_owned('a' * 64, self.receipt('envoy'))

    def test_auth_and_envoy_image_constants_match_repository(self):
        self.assertEqual(r.GOTRUE_IMAGE, 'supabase/gotrue:v2.196.0')
        self.assertEqual(r.ENVOY_IMAGE, 'envoyproxy/envoy:v1.39.1')
        self.assertEqual(r.NGINX_IMAGE, 'nginx:1.25-alpine')
        self.assertEqual(r.BACKEND_EXECUTABLE, '/usr/sbin/mosquitto')
        self.assertEqual(r.ISSUER, 'http://localhost/auth/v1')

    def test_dry_run_has_zero_allocations_and_never_reports_pass(self):
        with patch.object(r, 'inspect_local_image', return_value=(True, {}, None)), \
                 patch.object(r, 'create_verified_host_fixture_dir') as create_host:
            res = r.run_rehearsal_suite(dry_run=True, bounded_native=False)
            create_host.assert_not_called()
            self.assertFalse(res['allocated'])
            self.assertFalse(res['success'])
            self.assertEqual(res['failure_code'], 'dry_run_only')
            self.assertNotEqual(res['aggregate']['verdict'], 'PASS')

    def test_preflight_image_failure_blocks_without_allocation(self):
        with patch.object(r, 'inspect_local_image', side_effect=[
            (True, {}, None), (True, {}, None), (True, {}, None), (False, None, 'auth_missing'), (True, {}, None)
        ]), patch.object(r, 'create_verified_host_fixture_dir') as create_host:
            res = r.run_rehearsal_suite(dry_run=False, bounded_native=False)
            create_host.assert_not_called()
            self.assertFalse(res['allocated'])
            self.assertEqual(res['failure_stage'], 'preflight')
            self.assertEqual(res['failure_code'], 'auth_image_unavailable')
            self.assertFalse(res['success'])

    def test_signup_disabled_assertion_requires_semantic_error(self):
        r.assert_signup_disabled(422, {"error_code": "signup_disabled"})
        for status, body in [
            (200, {}),
            (403, {}),
            (502, {}),
            (422, {"error_code": "invalid_credentials"}),
            (422, "not a dict")
        ]:
            with self.subTest(status=status, body=body):
                with self.assertRaises(RuntimeError):
                    r.assert_signup_disabled(status, body)

    def test_semantic_rejection_classifier(self):
        self.assertEqual(r.classify_semantic_rejection(422, {"error_code": "signup_disabled"}, "application/json"), "signup_disabled")
        self.assertEqual(r.classify_semantic_rejection(401, {"message": "Invalid API key"}, "application/json"), "invalid_api_key")
        self.assertEqual(r.classify_semantic_rejection(401, {"code": "unauthorized"}, "application/json"), "unauthorized")
        self.assertEqual(r.classify_semantic_rejection(401, None, "text/plain"), "unauthorized")
        self.assertEqual(r.classify_semantic_rejection(404, {"msg": "Not found"}, "application/json"), "route_not_found")
        self.assertEqual(r.classify_semantic_rejection(502, None, "text/html"), "upstream_unavailable")
        self.assertEqual(r.classify_semantic_rejection(500, {"error": "internal"}, "application/json"), "unknown")

    def test_http_call_handles_non_json_cleanly(self):
        fake_resp = MagicMock()
        fake_resp.read.return_value = b"Unauthorized"
        fake_resp.status = 401
        fake_resp.getheaders.return_value = [('Content-Type', 'text/plain')]
        with patch('http.client.HTTPConnection') as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn.getresponse.return_value = fake_resp
            mock_conn_cls.return_value = mock_conn
            status, headers, body = r.http_call(80, 'POST', '/auth/v1/signup', api_key='anon')
            self.assertEqual(status, 401)
            self.assertEqual(body, 'non_json')
            self.assertEqual(r.classify_semantic_rejection(status, body, headers.get('Content-Type', '')), 'unauthorized')

    def test_create_user_via_admin_api_validates_uuid(self):
        uid = "11111111-2222-4333-8444-555555555555"
        with patch.object(r, 'http_call', return_value=(200, {}, {"id": uid})):
            out = r.create_user_via_admin_api(8080, "service-key", "a@example.invalid", "pass")
            self.assertEqual(out, uid)
        for status, body in [(403, {"id": uid}), (200, {"id": "not-a-uuid"}), (200, {})]:
            with self.subTest(status=status, body=body), \
                 patch.object(r, 'http_call', return_value=(status, {}, body)):
                with self.assertRaises(RuntimeError):
                    r.create_user_via_admin_api(8080, "service-key", "a@example.invalid", "pass")

    def test_login_user_for_token_checks_tokens(self):
        with patch.object(r, 'http_call', return_value=(200, {}, {"access_token": "acc", "refresh_token": "ref"})):
            acc, ref = r.login_user_for_token(8080, "anon-key", "a@example.invalid", "pass")
            self.assertEqual(acc, "acc")
            self.assertEqual(ref, "ref")
        for status, body in [(400, {}), (200, {"access_token": "acc"}), (200, {"refresh_token": "ref"})]:
            with self.subTest(status=status, body=body), \
                 patch.object(r, 'http_call', return_value=(status, {}, body)):
                with self.assertRaises(RuntimeError):
                    r.login_user_for_token(8080, "anon-key", "a@example.invalid", "pass")

    def test_bootstrap_token_generation_format(self):
        secret = "very_long_test_secret_for_supabase_32_bytes_min"
        tok = r.make_bootstrap_token(secret, {"role": "service_role", "sub": "admin"})
        parts = tok.split('.')
        self.assertEqual(len(parts), 3)
        padding = b'=' * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
        self.assertEqual(claims["role"], "service_role")
        self.assertEqual(claims["sub"], "admin")

    def test_teardown_order_and_cleanup_veto(self):
        order = []
        broker, pg = MagicMock(), MagicMock()
        pg.container_id = None
        pg.cidfile.exists.return_value = False
        pg.cleanup.side_effect = lambda: order.append('pg') or True
        broker.cleanup.side_effect = lambda: order.append('broker') or {'clean': True}
        consumers = {
            'backend': {'resource_id': '1' * 64, 'expected_project': 'p', 'expected_role': 'backend', 'expected_name': 'p-backend-1'},
            'nginx': {'resource_id': '2' * 64, 'expected_project': 'p', 'expected_role': 'nginx', 'expected_name': 'p-nginx-1'},
            'envoy': {'resource_id': '3' * 64, 'expected_project': 'p', 'expected_role': 'envoy', 'expected_name': 'p-envoy-1'},
            'auth': {'resource_id': '4' * 64, 'expected_project': 'p', 'expected_role': 'auth', 'expected_name': 'p-auth-1'},
        }
        with patch.object(r, 'remove_consumer', side_effect=lambda x: order.append(x['expected_role']) or True), \
             patch.object(r, 'verified_remove_host_fixture_dir', side_effect=lambda x: order.append('host') or {'verified': True}):
            clean = r.teardown(consumers, pg, broker, Path('/tmp/opencode/fake'))
        self.assertEqual(order, ['nginx', 'backend', 'envoy', 'auth', 'pg', 'broker', 'host'])
        self.assertTrue(r.aggregate('PASS', clean)['success'])

        # Unknown or failed cleanup vetoes PASS
        clean['auth_cleanup_verified'] = False
        self.assertFalse(r.aggregate('PASS', clean)['success'])
        self.assertEqual(r.aggregate('PASS', clean)['verdict'], 'CLEANUP_FAILURE_VETO')

    def test_unknown_consumer_vetoes_dependencies(self):
        broker, pg = MagicMock(), MagicMock()
        pg.container_id = None
        pg.cidfile.exists.return_value = False
        consumers = {'auth': {'resource_id': 'a' * 64}}
        with patch.object(r, 'remove_consumer', return_value=False), \
             patch.object(r, 'verified_remove_host_fixture_dir') as host:
            res = r.teardown(consumers, pg, broker, Path('/tmp/opencode/fake'))
        self.assertFalse(res['auth_cleanup_verified'])
        pg.cleanup.assert_not_called()
        broker.cleanup.assert_not_called()
        host.assert_not_called()

    def test_consumer_removal_validates_presence_and_ownership(self):
        receipt = self.receipt('auth')
        with patch.object(r, 'inspect_resource_presence', return_value={'status': 'absent'}):
            self.assertTrue(r.remove_consumer(receipt))
        with patch.object(r, 'inspect_resource_presence', side_effect=[{'status': 'exists'}, {'status': 'absent'}]), \
             patch.object(r, 'inspect_owned_resource', return_value={'status': 'owned', 'resource_id': 'a' * 64}), \
             patch.object(r, 'docker') as docker_mock:
            self.assertTrue(r.remove_consumer(receipt))
            docker_mock.assert_called_with(['rm', '-f', 'a' * 64])

    def test_source_ast_no_forbidden_strings(self):
        code = Path(r.__file__).read_text()
        ast.parse(code)
        for forbidden in ('httptest', '18080', 'shared_fixture_scope_blocker', 'IMPLEMENTATION_SLICE_COMPLETE'):
            self.assertNotIn(forbidden, code)

    def test_auth_smoke_flow_full_execution(self):
        broker = MagicMock()
        broker.port = 8883
        broker.d = Path('/tmp/fake')
        backend_cid = 'b' * 64
        pg = MagicMock()
        pg.sql.side_effect = [
            (0, b""),  # grant sql
            (0, b"1\n"),  # journal before rotate
            (0, b"1\n"),  # journal after rotate
            (0, b"2\n"),  # journal after rotate
        ]
        observations = {}
        admin_uid = "11111111-1111-4111-8111-111111111111"
        owner_uid = "22222222-2222-4222-8222-222222222222"
        jwt_secret = "test_secret_must_be_thirty_two_bytes_long_min"
        now = int(r.time.time())
        admin_tok = r.make_bootstrap_token(jwt_secret, {"sub": admin_uid, "iss": r.ISSUER, "aud": "authenticated", "role": "authenticated", "iat": now, "exp": now + 600})
        owner_tok = r.make_bootstrap_token(jwt_secret, {"sub": owner_uid, "iss": r.ISSUER, "aud": "authenticated", "role": "authenticated", "iat": now, "exp": now + 600})

        with patch.object(r, 'http_call') as mock_http, \
             patch.object(r, 'probe_raw_connack', side_effect=[0, 0, 0, 5, 5]) as mock_probe, \
             patch.object(r, 'NativeTargetConnection') as mock_conn_cls, \
             patch.object(r, 'native_store') as mock_store:

            # Mock conn
            active_conn = MagicMock()
            active_conn.wait_for_disconnect.return_value = (True, 'EOF (server closed TLS)', 0.1)
            mock_conn_cls.return_value = active_conn

            # Mock native store
            mock_store.return_value = {
                'clients': [{'username': 'gw_test', 'disabled': True, 'roles': ['gateway_gw_test']}]
            }

            # Setup http_call responses in sequence:
            # 1. signup denied (422)
            # 2. create admin user (200)
            # 3. create owner user (200)
            # 4. login admin (200)
            # 5. login owner (200)
            # 6. invalid token rotate -> 401
            # 7. provision admin -> 201 (no-store)
            # 8. get credential before -> 200 (no-store)
            # 9. owner rotate -> 403
            # 10. owner delete -> 403
            # 11. get credential after -> 200 (no-store)
            # 12. admin rotate -> 200 (no-store)
            # 13. replay rotate -> 200 (no-store)
            # 14. admin delete -> 200 (no-store)
            no_store_headers = {'Cache-Control': 'no-store', 'Pragma': 'no-cache'}
            mock_http.side_effect = [
                (422, {}, {"error_code": "signup_disabled"}),
                (200, {}, {"id": admin_uid}),
                (200, {}, {"id": owner_uid}),
                (200, {}, {"access_token": admin_tok, "refresh_token": "r1"}),
                (200, {}, {"access_token": owner_tok, "refresh_token": "r2"}),
                (401, {}, {"error": "unauthorized"}),
                (201, no_store_headers, {"password": "x" * 24, "secret_returned": True}),
                (200, no_store_headers, {"status": "active"}),
                (403, {}, {"error": "forbidden"}),
                (403, {}, {"error": "forbidden"}),
                (200, no_store_headers, {"status": "active"}),
                (200, no_store_headers, {"password": "y" * 24, "credential_version": 2}),
                (200, no_store_headers, {"credential_version": 2}),
                (200, no_store_headers, {"status": "revoked"}),
            ]

            r.auth_smoke_flow(
                80, broker, backend_cid, pg,
                "anon-key", "service-key", jwt_secret,
                "gw_test", observations
            )

            self.assertTrue(observations['checks']['public_signup_disabled'])
            self.assertTrue(observations['checks']['admin_user_created'])
            self.assertTrue(observations['checks']['owner_user_created'])
            self.assertTrue(observations['checks']['authorization_granted'])
            self.assertTrue(observations['checks']['tokens_valid_unmodified'])
            self.assertTrue(observations['checks']['rotate_session_closed'])
            self.assertTrue(observations['checks']['revoke_session_closed'])
            self.assertTrue(observations['checks']['native_disabled'])

            calls = mock_http.call_args_list
            for index in (0, 3, 4):
                self.assertEqual(calls[index].kwargs['api_key'], 'anon-key')
            for index in (1, 2):
                self.assertEqual(calls[index].kwargs['api_key'], 'service-key')
                self.assertEqual(calls[index].kwargs['token'], 'service-key')
            for index in (5, 6, 8, 9, 11, 12, 13):
                kwargs = calls[index].kwargs
                self.assertNotIn('api_key', kwargs)
                self.assertEqual(str(r.uuid.UUID(kwargs['idempotency_key'])), kwargs['idempotency_key'])
                self.assertIsNone(kwargs.get('body'))
            self.assertEqual(calls[5].kwargs['token'], 'invalid-token')
            for index in (8, 9):
                self.assertEqual(calls[index].kwargs['token'], owner_tok)
            for index in (6, 11, 12, 13):
                self.assertEqual(calls[index].kwargs['token'], admin_tok)
            self.assertEqual(calls[11].kwargs['idempotency_key'], calls[12].kwargs['idempotency_key'])

    def test_http_header_names_are_separate(self):
        connection = MagicMock()
        connection.getresponse.return_value.read.return_value = b'{}'
        connection.getresponse.return_value.status = 200
        connection.getresponse.return_value.getheaders.return_value = []
        with patch.object(r.http.client, 'HTTPConnection', return_value=connection):
            r.http_call(80, 'POST', '/credential', token='human-placeholder',
                        idempotency_key='11111111-1111-4111-8111-111111111111')
            headers = connection.request.call_args.kwargs['headers']
            self.assertEqual(set(headers), {'Content-Type', 'Authorization', 'Idempotency-Key'})
            self.assertEqual(headers['Authorization'], 'Bearer human-placeholder')
            self.assertEqual(headers['Idempotency-Key'], '11111111-1111-4111-8111-111111111111')
            self.assertIsNone(connection.request.call_args.kwargs['body'])
            r.http_call(80, 'POST', '/auth/v1/admin/users', token='service-placeholder',
                        api_key='api-placeholder', body={})
            headers = connection.request.call_args.kwargs['headers']
            self.assertEqual(set(headers), {'Content-Type', 'Authorization', 'apikey'})
            self.assertEqual(headers['apikey'], 'api-placeholder')

    def test_probe_auth_gateway_transient_503_to_success(self):
        """Focused test: transient 503 is retried until genuine health success."""
        calls = []
        def mock_call(port, method, path, token='', api_key='', body=None, timeout=2.0):
            calls.append((method, path, api_key))
            if len(calls) < 3:
                return (503, {'Content-Type': 'text/plain'}, 'upstream_unavailable')
            return (200, {'Content-Type': 'application/json'}, {'version': '2.196.0', 'name': 'GoTrue'})

        with patch.object(r, 'http_call', side_effect=mock_call), \
             patch.object(r, 'inspect_container_lifecycle', return_value={'status': 'running', 'exit_code': 0}), \
             patch.object(r.time, 'sleep'):
            res = r.probe_auth_gateway_readiness(80, 'anon-key', 'a' * 64, timeout_sec=10.0, per_call_timeout=1.0)
            self.assertTrue(res['ready'])
            self.assertEqual(res['status'], 200)
            self.assertEqual(res['category'], 'ready')
            self.assertEqual(res['attempts'], 3)
            # Ensure every probe went to the safe GET /auth/v1/health path with the key
            for method, path, key in calls:
                self.assertEqual(method, 'GET')
                self.assertEqual(path, '/auth/v1/health')
                self.assertEqual(key, 'anon-key')

    def test_probe_auth_gateway_permanent_503_stops_smoke(self):
        """Focused test: permanent 503 exhausts deadline and stops smoke before mutation."""
        mock_resp = (503, {'Content-Type': 'text/plain'}, 'upstream_unavailable')
        with patch.object(r, 'http_call', return_value=mock_resp), \
             patch.object(r, 'inspect_container_lifecycle', return_value={'status': 'running', 'exit_code': 0}), \
             patch.object(r.time, 'sleep'):
            res = r.probe_auth_gateway_readiness(80, 'anon-key', 'a' * 64, timeout_sec=2.0, per_call_timeout=0.5)
            self.assertFalse(res['ready'])
            self.assertEqual(res['status'], 503)
            self.assertEqual(res['category'], 'upstream_unavailable')
            self.assertEqual(res['auth_container'], {'status': 'running', 'exit_code': 0})

    def test_probe_auth_gateway_wrong_response_200_fails(self):
        """Focused test: arbitrary non-health 200 or 200 from another service fails semantic check."""
        for wrong_body in [
            {},  # empty json
            'non_json',  # raw text or html
            {'other_service': True},  # wrong payload
            {'status': 'ok'},  # missing expected health schema fields
        ]:
            with self.subTest(wrong_body=wrong_body):
                mock_resp = (200, {'Content-Type': 'application/json'}, wrong_body)
                with patch.object(r, 'http_call', return_value=mock_resp), \
                     patch.object(r, 'inspect_container_lifecycle', return_value={'status': 'running', 'exit_code': 0}), \
                     patch.object(r.time, 'sleep'):
                    res = r.probe_auth_gateway_readiness(80, 'anon-key', 'a' * 64, timeout_sec=1.0, per_call_timeout=0.2)
                    self.assertFalse(res['ready'])
                    self.assertNotEqual(res['category'], 'ready')

    def test_probe_auth_gateway_no_mutations_or_secrets_in_diagnostics(self):
        """Focused test: readiness probes are strictly read-only and leak no secrets/tokens in diagnostics."""
        calls = []
        secret_key = 'super_secret_anon_key_string_must_not_leak'
        def mock_call(port, method, path, token='', api_key='', body=None, timeout=2.0):
            calls.append((method, path, body))
            return (503, {'Content-Type': 'text/plain'}, 'upstream_unavailable')

        with patch.object(r, 'http_call', side_effect=mock_call), \
             patch.object(r, 'inspect_container_lifecycle', return_value={'status': 'exited', 'exit_code': 137}), \
             patch.object(r.time, 'sleep'):
            res = r.probe_auth_gateway_readiness(80, secret_key, 'a' * 64, timeout_sec=1.5, per_call_timeout=0.5)
            self.assertFalse(res['ready'])
            # Ensure no mutations (all GET, no body)
            for method, path, body in calls:
                self.assertEqual(method, 'GET')
                self.assertIsNone(body)
            # Ensure safe numeric status/category observations, no raw bodies, keys or tokens in res
            res_str = json.dumps(res)
            self.assertNotIn(secret_key, res_str)
            self.assertNotIn('upstream_unavailable', res.get('auth_container', {}).values())
            self.assertEqual(res['auth_container'], {'status': 'exited', 'exit_code': 137})


if __name__ == '__main__':
    unittest.main()
