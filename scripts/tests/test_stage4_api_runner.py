#!/usr/bin/env python3
"""Focused unit tests for the Task 4.7 Stage 4 API runner and fixture lifecycle.

No running Docker daemon or containers required for these unit tests.
They verify:
  1. Finite selector resolution and unknown selector strictness.
  2. Incomplete scenario handling (fail-closed with clear error, never placeholder PASS).
  3. All vs. partial scenario semantics.
  4. ResourceRegistry tracking and cleanup failure propagation.
  5. Ephemeral HS256 JWT token generation, expiry, and signature tampering detection.
  6. Real HTTP client request framing, header handling, and response decoding.
  7. User fixture definitions and SQL generation (admin in platform_admins, no entity graphs).
  8. Non-secret metadata collection (no credentials/tokens/DSNs exposed).
  9. Secret sanitization across outputs.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import io
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import stage4_api_runner as R  # noqa: E402


class SelectorAndScenarioContractTests(unittest.TestCase):
    """Verify scenario strictness, selectors, and incomplete enforcement."""

    def test_supported_selectors_resolve(self):
        self.assertEqual(R.resolve_scenario('vertical'), 'vertical')
        self.assertEqual(R.resolve_scenario('contract'), 'contract')
        self.assertEqual(R.resolve_scenario('queries'), 'queries')
        self.assertEqual(R.resolve_scenario('all'), 'all')

class SchemaEvidenceTests(unittest.TestCase):
    def test_unverified_metadata_does_not_claim_schema(self):
        self.assertIsNone(R.collect_metadata()['terminal_schema'])

    def test_schema_skip_or_history_drift_is_rejected(self):
        expected = R.expected_stage4_schema_history()
        for history in (expected[:-1], expected + ['22:unexpected'],
                        [row.replace('admin_user_management', 'wrong') for row in expected]):
            fixture = MagicMock()
            fixture.sql.return_value = (0, ('STAGE4_SCHEMA=' + json.dumps(history) + '\n').encode())
            with self.assertRaisesRegex(RuntimeError, 'schema'):
                R.verify_terminal_schema(fixture)

    def test_schema_is_read_from_database(self):
        fixture = MagicMock()
        fixture.sql.return_value = (0, ('STAGE4_SCHEMA=' + json.dumps(R.expected_stage4_schema_history()) + '\n').encode())
        self.assertEqual(R.verify_terminal_schema(fixture), 21)
        self.assertIn(b'schema_migrations', fixture.sql.call_args.args[0])

    def test_schema_read_errors_cannot_claim_success(self):
        for response in ((1, b''), (0, b'STAGE4_SCHEMA=invalid\n'), (0, b'')):
            fixture = MagicMock()
            fixture.sql.return_value = response
            with self.assertRaisesRegex(RuntimeError, 'schema'):
                R.verify_terminal_schema(fixture)

    def test_scenario_result_uses_verified_fixture_metadata(self):
        ctx, cleanup = MagicMock(), MagicMock()
        ctx.metadata = {'terminal_schema': 21}
        with patch.object(R, 'create_stage4_fixture', return_value=(ctx, cleanup)), \
             patch.object(R, 'run_scenario', return_value={'status': 'PASS'}):
            result = R.run_single_scenario_fixture('contract', 'backend', 'gateway')
        self.assertEqual(result['_verified_schema'], 21)
        cleanup.assert_called_once()


class InterruptedSuiteTests(unittest.TestCase):
    def test_signals_never_publish_success(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            for phase in ('build', 'setup', 'scenario'):
                with self.subTest(signal=signum, phase=phase), tempfile.TemporaryDirectory() as tmp:
                    handlers = {}
                    def install(sig, handler):
                        handlers[sig] = handler
                    def interrupt(*args, **kwargs):
                        handlers[signum](signum, None)
                    output = io.StringIO()
                    cleanup = MagicMock()
                    with patch.object(R, 'docker_available', return_value=True), \
                         patch.object(R.signal, 'signal', side_effect=install), \
                         patch.object(R, 'build_image', side_effect=interrupt if phase == 'build' else None, return_value='sha256:shared'), \
                         patch.object(R, 'collect_metadata', return_value={'head': 'fixture'}), \
                         patch.object(R, 'create_stage4_fixture', side_effect=interrupt if phase == 'setup' else None, return_value=(MagicMock(), cleanup)), \
                         patch.object(R, 'run_scenario', side_effect=interrupt), \
                         patch.object(R, 'docker') as docker, \
                         patch.object(R, 'image_absent', return_value=True), \
                         patch('sys.stdout', output), patch('sys.stderr', io.StringIO()):
                        report = Path(tmp) / 'summary.json'
                        code = R.main(['--scenario', 'vertical', '--report', str(report)])
                    self.assertNotEqual(code, 0)
                    self.assertNotIn('PASS: stage4 api integration', output.getvalue())
                    self.assertFalse(report.exists())
                    if phase == 'scenario':
                        cleanup.assert_called_once()
                    for call in docker.call_args_list:
                        self.assertNotIn('sha256:shared', call.args[0])

    def test_signal_during_actual_fixture_setup_runs_registered_cleanup(self):
        fixture = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / 'owned-fixture'
            work.mkdir()
            with patch.object(R.tempfile, 'mkdtemp', return_value=str(work)), \
                 patch.object(R, 'generate_certs'), patch.object(R, 'docker'), \
                 patch.object(R, 'DatabaseFixture', return_value=fixture), \
                 patch.object(R, 'bootstrap_to_9', side_effect=R.SuiteInterrupted(signal.SIGTERM)), \
                 patch.object(R.ResourceRegistry, 'still_present', return_value=[]), \
                 patch('sys.stdout', io.StringIO()) as output:
                with self.assertRaises(R.SuiteInterrupted):
                    R.create_stage4_fixture('backend', 'gateway')
            fixture.cleanup.assert_called_once()
            self.assertFalse(work.exists())
            self.assertNotIn('PASS:', output.getvalue())

    def test_image_cleanup_removes_only_owned_tags(self):
        with patch.object(R, 'docker_available', return_value=True), \
             patch.object(R, 'build_image', return_value='sha256:shared'), \
             patch.object(R, 'collect_metadata', return_value={'head': 'fixture'}), \
             patch.object(R, 'run_single_scenario_fixture', return_value={}), \
             patch.object(R, 'docker') as docker, \
             patch.object(R, 'image_absent', side_effect=[False, False, True, True]), \
             patch('sys.stdout', io.StringIO()):
            self.assertEqual(R.main(['--scenario', 'vertical']), 0)
        commands = [c.args[0] for c in docker.call_args_list]
        self.assertEqual(len(commands), 2)
        for command in commands:
            self.assertEqual(command[:2], ['image', 'rm'])
            self.assertTrue(command[2].startswith('stage4-api-'))
            self.assertNotIn('-f', command)


class SelectorValidationTests(unittest.TestCase):

    def test_unknown_selector_fails_closed(self):
        with self.assertRaises(ValueError) as ctx:
            R.resolve_scenario('unknown_selector')
        self.assertIn('unknown scenario selector', str(ctx.exception))

        with self.assertRaises(ValueError):
            R.resolve_scenario('')

        with self.assertRaises(ValueError):
            R.resolve_scenario('telemetry')

    def test_mandatory_scenarios_are_the_three_planned_slices(self):
        self.assertEqual(R.MANDATORY_SCENARIOS, ('vertical', 'contract', 'queries'))
        self.assertEqual(set(R.SCENARIO_DESCRIPTIONS.keys()), {'vertical', 'contract', 'queries'})

    def test_unregistered_scenario_raises_not_implemented(self):
        # Unimplemented scenarios must fail clearly with NotImplementedError.
        prev = R.SCENARIO_CALLBACKS.pop('queries', None)
        try:
            fake_ctx = MagicMock()
            with self.assertRaises(NotImplementedError) as ctx:
                R.run_scenario('queries', fake_ctx)
            self.assertIn("scenario 'queries' is not yet implemented", str(ctx.exception))
        finally:
            if prev is not None:
                R.SCENARIO_CALLBACKS['queries'] = prev

    def test_cannot_register_unknown_scenario_callback(self):
        with self.assertRaises(ValueError):
            R.register_scenario_callback('nonexistent', lambda ctx: None)

    def test_registered_scenario_callback_invokes(self):
        called = []
        prev = R.SCENARIO_CALLBACKS.get('contract')
        try:
            R.register_scenario_callback('contract', lambda ctx: called.append('contract_called'))
            fake_ctx = MagicMock()
            R.run_scenario('contract', fake_ctx)
            self.assertEqual(called, ['contract_called'])
        finally:
            if prev is not None:
                R.SCENARIO_CALLBACKS['contract'] = prev
            else:
                R.SCENARIO_CALLBACKS.pop('contract', None)


class AllVsPartialSemanticsTests(unittest.TestCase):
    """Verify partial runs do not claim full-task PASS."""

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_partial_scenario_does_not_print_all_marker(self, mock_run, mock_build, mock_docker):
        captured_out = []
        with patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)):
            code = R.main(['--scenario', 'vertical'])
        self.assertEqual(code, 0)
        combined = ''.join(captured_out)
        self.assertIn('PASS: stage4 api integration (scenario=vertical)', combined)
        self.assertNotIn('all scenarios passed', combined)

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_all_scenario_runs_all_three_and_prints_all_marker(self, mock_run, mock_build, mock_docker):
        captured_out = []
        with patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)):
            code = R.main(['--scenario', 'all'])
        self.assertEqual(code, 0)
        self.assertEqual(mock_run.call_count, 3)
        combined = ''.join(captured_out)
        self.assertIn('PASS: stage4 api integration (all scenarios passed: vertical, contract, queries)', combined)

    @patch('stage4_api_runner.docker_available', return_value=True)
    def test_unknown_scenario_cli_returns_exit_2(self, mock_docker):
        captured_err = []
        with patch('sys.stderr.write', side_effect=lambda s: captured_err.append(s)):
            code = R.main(['--scenario', 'invalid_xyz'])
        self.assertEqual(code, 2)
        combined_err = ''.join(captured_err)
        self.assertIn('FAIL: unknown scenario selector', combined_err)

    @patch('stage4_api_runner.docker_available', return_value=False)
    @patch.dict(os.environ, {'CI': 'false', 'MANDATORY_CI': '0', 'STAGE4_REQUIRE_RUN': '0'})
    def test_docker_unavailable_prints_skip_and_exits_0(self, mock_docker):
        captured_out = []
        with patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)):
            code = R.main([])
        self.assertEqual(code, 0)
        combined = ''.join(captured_out)
        self.assertIn('SKIP: docker daemon or CLI unavailable', combined)

    @patch('stage4_api_runner.docker_available', return_value=False)
    def test_each_mandatory_environment_rejects_skip(self, mock_docker):
        for variable, enabled in (('CI', 'true'), ('MANDATORY_CI', '1'), ('STAGE4_REQUIRE_RUN', '1')):
            with self.subTest(variable=variable), \
                 patch.dict(os.environ, {'CI': 'false', 'MANDATORY_CI': '0', 'STAGE4_REQUIRE_RUN': '0', variable: enabled}), \
                 patch('sys.stdout', io.StringIO()) as output, patch('sys.stderr', io.StringIO()):
                self.assertEqual(R.main([]), 1)
                self.assertNotIn('SKIP:', output.getvalue())

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.create_stage4_fixture')
    def test_incomplete_scenario_cli_fails_with_exit_1(self, mock_create, mock_build, mock_docker):
        # Scenario without registered callback fails closed with exit 1.
        prev = R.SCENARIO_CALLBACKS.pop('queries', None)
        try:
            mock_ctx = MagicMock()
            mock_cleanup = MagicMock()
            mock_create.return_value = (mock_ctx, mock_cleanup)
            captured_err = []
            with patch('sys.stderr.write', side_effect=lambda s: captured_err.append(s)):
                code = R.main(['--scenario', 'queries'])
            self.assertEqual(code, 1)
            combined_err = ''.join(captured_err)
            self.assertIn('FAIL: stage4 api integration incomplete', combined_err)
            self.assertIn("scenario 'queries' is not yet implemented", combined_err)
            self.assertTrue(mock_cleanup.called)
        finally:
            if prev is not None:
                R.SCENARIO_CALLBACKS['queries'] = prev

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture', side_effect=RuntimeError('simulated assertion failure'))
    def test_scenario_failure_returns_exit_1(self, mock_run, mock_build, mock_docker):
        captured_err = []
        with patch('sys.stderr.write', side_effect=lambda s: captured_err.append(s)):
            code = R.main(['--scenario', 'vertical'])
        self.assertEqual(code, 1)
        combined_err = ''.join(captured_err)
        self.assertIn('FAIL: scenario vertical failed: simulated assertion failure', combined_err)


class ResourceRegistryAndCleanupTests(unittest.TestCase):
    """Verify resource registry tracking and cleanup failure propagation."""

    def test_still_present_filters_absent_resources(self):
        reg = R.ResourceRegistry()
        reg.register('backend container', 'container', 'cid_1')
        reg.register('network', 'network', 'net_1')
        reg.register('dynsec volume', 'volume', 'vol_1')

        present_set = {'cid_1'}
        # absent_fn returns True if the resource is absent, False if still present
        still_present = reg.still_present(lambda kind, ident: ident not in present_set)
        self.assertEqual(still_present, ['backend container'])

    def test_still_present_empty_when_all_absent(self):
        reg = R.ResourceRegistry()
        reg.register('workdir', 'workdir', '/tmp/nonexistent_dir')
        self.assertEqual(reg.still_present(lambda kind, ident: True), [])

    def test_unknown_resource_kind_raises_value_error(self):
        with self.assertRaises(ValueError):
            R.absent('unknown_kind', 'id_xyz')


class EphemeralJwtTests(unittest.TestCase):
    """Verify HS256 JWT creation, claims structure, and cryptographic signature."""

    def test_jwt_header_and_claims_structure(self):
        secret = 'test_secret_32_bytes_minimum_length_required'
        user_id = '12345678-1234-5678-1234-567812345678'
        token = R.make_jwt(secret, user_id, issuer='https://test.issuer', audience='authenticated', role='authenticated')

        parts = token.split('.')
        self.assertEqual(len(parts), 3)

        # Decode header
        header = json.loads(base64.urlsafe_b64decode(parts[0] + '==').decode())
        self.assertEqual(header, {'alg': 'HS256', 'typ': 'JWT'})

        # Decode payload
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + '==').decode())
        self.assertEqual(payload['sub'], user_id)
        self.assertEqual(payload['iss'], 'https://test.issuer')
        self.assertEqual(payload['aud'], 'authenticated')
        self.assertEqual(payload['role'], 'authenticated')
        self.assertIn('iat', payload)
        self.assertIn('exp', payload)
        self.assertGreater(payload['exp'], payload['iat'])

    def test_jwt_signature_verification_and_tamper_detection(self):
        secret = 'secure_ephemeral_random_secret_48_bytes'
        user_id = '87654321-4321-8765-4321-876543218765'
        token = R.make_jwt(secret, user_id)
        parts = token.split('.')

        msg = f'{parts[0]}.{parts[1]}'
        expected_sig = base64.urlsafe_b64encode(
            hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).rstrip(b'=').decode()

        self.assertEqual(parts[2], expected_sig)

        # Tampered message must not match signature
        tampered_msg = f'{parts[0]}.{parts[1]}extra'
        tampered_sig = base64.urlsafe_b64encode(
            hmac.new(secret.encode(), tampered_msg.encode(), hashlib.sha256).digest()
        ).rstrip(b'=').decode()
        self.assertNotEqual(expected_sig, tampered_sig)

        # Wrong secret must not match signature
        wrong_secret_sig = base64.urlsafe_b64encode(
            hmac.new(b'wrong_secret', msg.encode(), hashlib.sha256).digest()
        ).rstrip(b'=').decode()
        self.assertNotEqual(expected_sig, wrong_secret_sig)


class UserFixtureAndSqlSeedingTests(unittest.TestCase):
    """Verify user fixture setup and SQL generation (no entity graphs)."""

    def test_default_fixture_users_has_all_five_roles(self):
        users = R.default_fixture_users('test')
        self.assertEqual(len(users.all_users), 5)
        self.assertEqual(users.admin.role_label, 'admin')
        self.assertEqual(users.owner.role_label, 'owner')
        self.assertEqual(users.operator.role_label, 'operator')
        self.assertEqual(users.viewer.role_label, 'viewer')
        self.assertEqual(users.user_b.role_label, 'user_b')

    def test_user_seed_sql_contains_only_admin_in_platform_admins(self):
        users = R.default_fixture_users('s4')
        sql = R.build_user_seed_sql(users, has_auth_users=True)

        admin_uid = R.format_user_id_as_uuid(users.admin.user_id)
        owner_uid = R.format_user_id_as_uuid(users.owner.user_id)

        # Platform admins table must contain only admin_uid
        self.assertIn(f"INSERT INTO public.platform_admins (user_id)\n        VALUES ('{admin_uid}')", sql)
        self.assertNotIn(f"VALUES ('{owner_uid}')", sql.split('platform_admins')[1])

        # Entity tables must NEVER be seeded here!
        self.assertNotIn('twin_entities', sql)
        self.assertNotIn('twin_states', sql)
        self.assertNotIn('sensors', sql)
        self.assertNotIn('gateways', sql)

    def test_format_user_id_as_uuid(self):
        hex32 = '11223344556677889900aabbccddeeff'
        self.assertEqual(R.format_user_id_as_uuid(hex32), '11223344-5566-7788-9900-aabbccddeeff')
        already_uuid = '11223344-5566-7788-9900-aabbccddeeff'
        self.assertEqual(R.format_user_id_as_uuid(already_uuid), already_uuid)


class HttpClientUtilityTests(unittest.TestCase):
    """Verify the real HTTP client helper framing."""

    def test_http_response_dataclass(self):
        resp = R.HTTPResponse(200, {'content-type': 'application/json'}, b'{"status":"ok"}', {'status': 'ok'})
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.json, {'status': 'ok'})
        self.assertEqual(resp.body, b'{"status":"ok"}')

    @patch('http.client.HTTPConnection')
    def test_request_headers_and_auth_token(self, mock_conn_cls):
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.getheaders.return_value = [('Content-Type', 'application/json')]
        mock_resp.read.return_value = b'{"items":[]}'
        mock_conn.getresponse.return_value = mock_resp

        client = R.Stage4HttpClient(8080)
        token = 'fake.jwt.token'
        resp = client.get('/v1/digital-twins', token=token, params={'limit': 10})

        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.json, {'items': []})

        # Verify request framing
        call_args = mock_conn.request.call_args
        method, full_path = call_args[0]
        headers = call_args[1]['headers']

        self.assertEqual(method, 'GET')
        self.assertEqual(full_path, '/v1/digital-twins?limit=10')
        self.assertEqual(headers.get('Authorization'), f'Bearer {token}')


class MetadataAndSanitizationTests(unittest.TestCase):
    """Verify metadata collection and secret masking."""

    def test_collect_metadata_contains_no_sensitive_fields(self):
        meta = R.collect_metadata(backend_image='sha256:backend_test', gateway_image='sha256:gateway_test')
        self.assertIn('head', meta)
        self.assertIn('backend_image', meta)
        self.assertIn('gateway_image', meta)
        self.assertIn('mosquitto_image', meta)
        self.assertIn('timescaledb_image', meta)
        self.assertIn('terminal_schema', meta)

        meta_json = json.dumps(meta).lower()
        for forbidden in ('password', 'token', 'secret', 'key', 'private', 'postgres://'):
            self.assertNotIn(forbidden, meta_json)

    def test_sanitize_masks_registered_secrets(self):
        secret1 = 'super_secret_password_xyz'
        secret2 = 'another_jwt_token_12345'
        R.SECRETS.extend([secret1, secret2])
        try:
            output = f"Connecting with password={secret1} and bearer={secret2}"
            cleaned = R.sanitize(output)
            self.assertNotIn(secret1, cleaned)
            self.assertNotIn(secret2, cleaned)
            self.assertIn('[REDACTED]', cleaned)
        finally:
            R.SECRETS.clear()


class BackendEnvAndGatewaySpecsTests(unittest.TestCase):
    """Verify backend environment composing and gateway specs isolation."""

    def test_backend_env_uses_iot_backend_app_and_stage4_defaults(self):
        fake_fixture = MagicMock()
        fake_fixture.passwords = ['admin_pwd', 'app_pwd_secret', 'auth_pwd']
        env = R.backend_env(
            fake_fixture,
            jwt_secret='jwt_sec',
            backend_password='be_pwd',
            jwt_issuer='https://test.issuer',
            jwt_audience='authenticated',
        )
        self.assertIn('iot_backend_app:app_pwd_secret@database:5432/fixture_service', env['DATABASE_URL'])
        self.assertEqual(env['SUPABASE_JWT_SECRET'], 'jwt_sec')
        self.assertEqual(env['SUPABASE_JWT_ISSUER'], 'https://test.issuer')
        self.assertEqual(env['SUPABASE_JWT_AUDIENCE'], 'authenticated')
        self.assertEqual(env['MQTT_INGRESS_ENABLED'], 'true')
        self.assertEqual(env['MQTT_USERNAME'], 'bootstrap_backend')
        self.assertEqual(env['MQTT_PASSWORD'], 'be_pwd')
        self.assertEqual(env['MQTT_TLS_CA_FILE'], '/certs/ca.crt')

        # Stage 4 bounds defaults
        self.assertEqual(env['HISTORY_QUERY_TIMEOUT'], '5s')
        self.assertEqual(env['HISTORY_MAX_RANGE'], '744h')
        self.assertEqual(env['TWIN_LIST_DEFAULT_LIMIT'], '50')
        self.assertEqual(env['TWIN_RELATIONSHIP_DEFAULT_LIMIT'], '50')

    def test_default_gateway_specs_distinct_and_well_formed(self):
        specs = R.default_gateway_specs('beef12')
        self.assertEqual(len(specs), 2)
        a, b = specs[0], specs[1]

        self.assertNotEqual(a.gateway_id, b.gateway_id)
        self.assertEqual(a.gateway_urn, f'urn:ngsi-ld:Gateway:{a.gateway_id}')
        self.assertEqual(b.gateway_urn, f'urn:ngsi-ld:Gateway:{b.gateway_id}')
        self.assertIn(a.gateway_id, a.telemetry_topic)
        self.assertIn(b.gateway_id, b.telemetry_topic)
        self.assertNotEqual(a.telemetry_topic, b.telemetry_topic)

    def test_fixture_context_assign_membership(self):
        mock_db = MagicMock()
        ctx = R.Stage4FixtureContext(
            work_dir=Path('/tmp'),
            net_name='net',
            db_fixture=mock_db,
            db_setup_url='pg_setup',
            db_app_url='pg_app',
            broker_cid='b_cid',
            broker_port=8883,
            backend_cid='be_cid',
            backend_port=8080,
            http=MagicMock(),
            users=R.default_fixture_users('t'),
            tokens={},
            gateways=[],
            gateway_passwords={},
            backend_image='be_img',
            gateway_image='gw_img',
            ca_path=Path('/tmp/ca.crt'),
            registry=R.ResourceRegistry(),
            metadata={},
            jwt_secret='sec',
            jwt_issuer='iss',
            jwt_audience='aud',
        )

        test_uid = '11223344-5566-7788-9900-aabbccddeeff'
        ctx.assign_membership(test_uid, 'gw_test', 'operator')

        self.assertEqual(mock_db.sql.call_count, 1)
        executed_sql = mock_db.sql.call_args[0][0].decode('utf-8')
        self.assertIn('INSERT INTO public.user_gateways', executed_sql)
        self.assertIn(test_uid, executed_sql)
        self.assertIn('gw_test', executed_sql)
        self.assertIn("'operator'", executed_sql)


class ReportAndCleanupFailureGuardTests(unittest.TestCase):
    """Verify cleanup failure propagation, report allowlist, and mandatory execution guards."""

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_cleanup_failure_prevents_terminal_pass_marker_and_fails(self, mock_run, mock_build, mock_docker):
        """When suite image cleanup is incomplete, main() must fail (exit 1) and never print PASS."""
        captured_out = []
        captured_err = []
        with patch.object(R.ResourceRegistry, 'still_present', return_value=['backend image']), \
             patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)), \
             patch('sys.stderr.write', side_effect=lambda s: captured_err.append(s)):
            code = R.main(['--scenario', 'all'])

        self.assertEqual(code, 1)
        combined_out = ''.join(captured_out)
        combined_err = ''.join(captured_err)

        # PASS marker must NEVER be printed if cleanup failed!
        self.assertNotIn('all scenarios passed', combined_out)
        self.assertNotIn('PASS: stage4 api integration', combined_out)
        self.assertIn('FAIL: suite image cleanup incomplete: still present: backend image', combined_err)

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_safe_compact_report_generation_and_allowlist(self, mock_run, mock_build, mock_docker):
        """When --report is passed and all scenarios pass, an allowlisted report must be generated."""
        mock_run.return_value = {'sample_result': 123}
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_file = Path(tmp_dir) / 'test_report.json'
            captured_out = []
            with patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)):
                code = R.main(['--scenario', 'all', '--report', str(report_file)])

            self.assertEqual(code, 0)
            self.assertTrue(report_file.is_file())

            data = json.loads(report_file.read_text(encoding='utf-8'))
            self.assertEqual(data['status'], 'PASS')
            self.assertEqual(data['task'], '4.7.6')
            self.assertEqual(data['profiles']['selector'], 'all')
            self.assertEqual(data['checks']['all_mandatory_passed'], True)
            self.assertEqual(data['cleanup']['suite_images_removed'], True)
            self.assertEqual(data['cleanup']['fixture_cleanup_verified'], True)

            # All top-level keys must be in SAFE_SUMMARY_KEYS
            self.assertTrue(set(data.keys()).issubset(R.SAFE_SUMMARY_KEYS))

            # Non-secret assertions: no credentials, URLs, or keys in serialized artifact
            raw_text = report_file.read_text(encoding='utf-8')
            for forbidden in ('BEGIN PRIVATE KEY', 'postgres://', 'Bearer '):
                self.assertNotIn(forbidden, raw_text)

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_safe_compact_report_partial_scenario_status_and_compression_counts(self, mock_run, mock_build, mock_docker):
        """When partial scenario runs or queries result contains compression_status, summary reflects all-only semantics and real counts."""
        mock_run.return_value = {
            'dataset': {'target_sample_count': 60000, 'target_temporal_count': 600},
            'compression_status': {
                'uncompressed_chunks_before': 2,
                'compressed_chunks_after': 2,
                'target_chunks': ['_hyper_1_1_chunk', '_hyper_2_2_chunk'],
                'is_compressed': True,
            },
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_file = Path(tmp_dir) / 'partial_report.json'
            with patch('sys.stdout.write'):
                code = R.main(['--scenario', 'queries', '--report', str(report_file)])

            self.assertEqual(code, 0)
            data = json.loads(report_file.read_text(encoding='utf-8'))
            self.assertEqual(data['status'], 'PARTIAL')
            self.assertEqual(data['checks']['all_mandatory_passed'], False)
            self.assertEqual(data['checks']['queries'], 'PASS')
            self.assertEqual(data['checks']['vertical'], 'NOT_RUN')
            self.assertEqual(data['checks']['contract'], 'NOT_RUN')
            self.assertEqual(data['counts']['queries']['compressed_chunks_count'], 2)
            self.assertEqual(data['counts']['queries']['is_compressed'], True)

    def test_safe_compact_report_rejects_forbidden_secrets(self):
        """write_safe_compact_report must raise ValueError if forbidden patterns appear."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_file = Path(tmp_dir) / 'forbidden.json'
            with self.assertRaises(ValueError) as ctx:
                R.write_safe_compact_report(report_file, {
                    'task': '4.7.6',
                    'status': 'PASS',
                    'revision': 'leak BEGIN PRIVATE KEY here',
                })
            self.assertIn('BEGIN PRIVATE KEY', str(ctx.exception))
            self.assertFalse(report_file.exists())

    def test_pre_validate_report_path_rejects_symlinks(self):
        """pre_validate_report_path must reject symlinks to prevent TOCTOU redirection."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            real_file = Path(tmp_dir) / 'real.json'
            real_file.write_text('{}')
            symlink_file = Path(tmp_dir) / 'symlink.json'
            symlink_file.symlink_to(real_file)

            with self.assertRaises(FileExistsError):
                R.pre_validate_report_path(symlink_file)

    @patch('stage4_api_runner.docker_available', return_value=False)
    def test_mandatory_ci_rejects_skip_when_docker_unavailable(self, mock_docker):
        """When require_run is set, missing Docker must fail closed with exit 1 rather than skip."""
        captured_out = []
        captured_err = []
        with patch('sys.stdout.write', side_effect=lambda s: captured_out.append(s)), \
             patch('sys.stderr.write', side_effect=lambda s: captured_err.append(s)):
            code = R.main(['--require-run'])

        self.assertEqual(code, 1)
        combined_out = ''.join(captured_out)
        combined_err = ''.join(captured_err)
        self.assertNotIn('SKIP:', combined_out)
        self.assertIn('FAIL: docker daemon or CLI unavailable and execution is mandatory', combined_err)

    @patch('stage4_api_runner.docker_available', return_value=True)
    @patch('stage4_api_runner.build_image', return_value='sha256:dummyimage123456')
    @patch('stage4_api_runner.run_single_scenario_fixture')
    def test_cleanup_failure_prevents_report_writing(self, mock_run, mock_build, mock_docker):
        """When image cleanup fails, the report file must NOT be written."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_file = Path(tmp_dir) / 'unwritten_report.json'
            with patch.object(R.ResourceRegistry, 'still_present', return_value=['gateway image']):
                code = R.main(['--scenario', 'all', '--report', str(report_file)])

            self.assertEqual(code, 1)
            self.assertFalse(report_file.exists())


if __name__ == '__main__':
    unittest.main()
