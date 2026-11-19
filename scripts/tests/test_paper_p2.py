"""Small P2 spike guards; no Docker required."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import socket
import ssl
import threading
from unittest.mock import Mock, patch

MODULE = Path(__file__).with_name('native_compose_smoke.py')
spec = importlib.util.spec_from_file_location('p2_smoke', MODULE)
smoke = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = smoke
spec.loader.exec_module(smoke)


class PaperP2Tests(unittest.TestCase):
    def test_fixture_dispatch_routes_p7_only_to_handshake_helper(self):
        p7 = Mock(paper_p7=True, paper_p2=True)
        p2 = Mock(paper_p7=False, paper_p2=True)
        helper = Mock()
        helper.run.return_value = {'status': 'PASS'}
        helper.run_full.return_value = {'status': 'P2'}
        self.assertEqual(smoke.dispatch_paper_fixture(p7, helper, 'args'), {'status': 'PASS'})
        helper.run.assert_called_once_with('args')
        helper.run_full.assert_not_called()
        helper.reset_mock()
        self.assertEqual(smoke.dispatch_paper_fixture(p2, helper, 'args'), {'status': 'P2'})
        helper.run_full.assert_called_once_with('args')
        helper.run.assert_not_called()

    def test_upgrade_probe_uses_one_buffered_reader_and_closes_all_statuses(self):
        helper = smoke.paper_p7_support()
        for status in (101, 401, 403):
            client, peer = socket.socketpair()
            peer.sendall(f'HTTP/1.1 {status} Result\r\nX-Test: yes\r\n\r\n'.encode())
            context = Mock()
            context.wrap_socket.return_value = client
            with patch.object(helper.ssl, 'create_default_context', return_value=context), \
                    patch.object(helper.socket, 'create_connection', return_value=client):
                code, fields = helper.native_probe('https://localhost:443', 'fixture-ca', 'token')
            self.assertEqual(code, status)
            self.assertEqual(fields['x-test'], 'yes')
            self.assertEqual(client.fileno(), -1)
            peer.close()

    def test_upgrade_probe_bounds_headers_and_closes_on_eof_or_timeout(self):
        helper = smoke.paper_p7_support()
        for payload in (b'HTTP/1.1 101 OK\r\nX: ' + b'a' * 200 + b'\r\n',
                        b'HTTP/1.1 101 OK\r\nX: y\r\n'):
            client, peer = socket.socketpair()
            peer.sendall(payload)
            context = Mock()
            context.wrap_socket.return_value = client
            with patch.object(helper.ssl, 'create_default_context', return_value=context), \
                    patch.object(helper.socket, 'create_connection', return_value=client):
                with self.assertRaises((ValueError, OSError)):
                    helper.native_probe('https://localhost:443', 'fixture-ca', 'token')
            self.assertEqual(client.fileno(), -1)
            peer.close()

    def test_upgrade_probe_parse_exception_releases_reader_and_socket(self):
        helper = smoke.paper_p7_support()
        client, peer = socket.socketpair()
        peer.sendall(b'not-http\r\n\r\n')
        reader = Mock()
        reader.readline.side_effect = [b'not-http\r\n']
        context = Mock()
        context.wrap_socket.return_value = client
        with patch.object(helper.ssl, 'create_default_context', return_value=context), \
                patch.object(helper.socket, 'create_connection', return_value=client), \
                patch.object(helper.socket.socket, 'makefile', return_value=reader):
            with self.assertRaises(ValueError):
                helper.native_probe('https://localhost:443', 'fixture-ca', 'token')
        reader.close.assert_called_once()
        self.assertEqual(client.fileno(), -1)
        peer.close()

    def test_storage_probe_retries_only_transient_status_and_keeps_safe_diagnostics(self):
        helper = smoke.paper_p2_support()
        record = {}
        request = Mock(side_effect=[(503, {}, 'PRIVATE_SENTINEL'), (200, {}, [])])
        with patch.object(helper.time, 'sleep'):
            ok = helper.storage_api_probe(request, '/storage/v1/bucket', 'PRIVATE_TOKEN', 'PRIVATE_KEY',
                'bucket_list', record, lambda: None, Mock())
        self.assertTrue(ok)
        self.assertEqual([r['http_status'] for r in record['attempts']], [503, 200])
        self.assertEqual(record['status'], 'PASS')
        self.assertNotIn('PRIVATE', json.dumps(record))

    def test_storage_probe_never_retries_auth_policy_or_tls_failures(self):
        helper = smoke.paper_p2_support()
        for response in [(401, {}, {'message': 'PRIVATE'}), (403, {}, {'error': 'PRIVATE'}),
                         (400, {}, {'message': 'PRIVATE'}), (200, {}, {'unexpected': True}),
                         ValueError('http_request_failed: SSLCertVerificationError')]:
            with self.subTest(response_type=type(response).__name__):
                request = Mock(side_effect=response) if isinstance(response, Exception) else Mock(return_value=response)
                record = {}
                self.assertFalse(helper.storage_api_probe(request, '/object', 'TOKEN', 'KEY',
                    'fixture_object', record, lambda: None, Mock()))
                self.assertEqual(request.call_count, 1)
                self.assertEqual(record['status'], 'FAIL')
                self.assertNotIn('PRIVATE', json.dumps(record))

    def test_storage_pair_probes_both_keys_even_if_k1_fails(self):
        helper = smoke.paper_p2_support()
        request = Mock(side_effect=[(401, {}, {}), (200, {}, {'probe': 'p2-fixture'})])
        records = {}
        result = helper.storage_pair_probe(request, '/object', 'K1_TOKEN', 'K2_TOKEN', 'KEY',
            records, lambda: None, Mock())
        self.assertFalse(result)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(records['K1']['status'], 'FAIL')
        self.assertEqual(records['K2']['status'], 'PASS')

    def test_storage_readiness_attempts_and_request_timeouts_are_bounded(self):
        helper = smoke.paper_p2_support()
        request = Mock(return_value=(503, {}, {}))
        record = {}
        with patch.object(helper.time, 'sleep'):
            self.assertFalse(helper.storage_api_probe(request, '/bucket', 'TOKEN', 'KEY',
                'bucket_list', record, lambda: None, Mock()))
        self.assertEqual(request.call_count, 20)
        self.assertEqual(len(record['attempts']), 20)
        self.assertTrue(all(0 < call.kwargs['timeout'] <= 2 for call in request.call_args_list))

    def test_rotation_documents_keep_exactly_one_signer_and_public_storage_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'rotation', paper_p2=True)
            k1 = json.loads((f.private / 'p2-signing-keys.json').read_text())
            k2, public, _ = smoke.paper_p2_support().rsa_jwk('k2-test')
            overlap, switched, storage = smoke.paper_p2_support().rotation_documents(k1, k2, public)
            self.assertEqual([k['kid'] for k in overlap if 'sign' in k['key_ops']], [k1[0]['kid']])
            self.assertEqual([k['kid'] for k in switched if 'sign' in k['key_ops']], [k2['kid']])
            self.assertEqual(len(storage['keys']), 2)
            self.assertTrue(all(not set(k) & {'d', 'p', 'q', 'dp', 'dq', 'qi', 'k'} for k in storage['keys']))
            self.assertEqual(k1[0]['key_ops'], ['sign'])

    def test_p2_csv_exports_fixed_unix_and_cache_series_without_dynamic_labels(self):
        import csv
        hist = {'upper_seconds': [0.1], 'buckets': [2], 'count': 2, 'sum_seconds': 0.02, 'overflow': 0}
        sample = {'process_instance': 'p2', 'elapsed_seconds': 1, 'observed_at': '2026-10-24T00:00:00Z',
                  'unix_auth': {'allow': 2, 'deny': 1, 'verify': hist}, 'unix_auth_handler': hist,
                  'jwks_cache': {'hits': 2, 'misses': 1, 'refresh_success': 1, 'refresh_failure': 0, 'kid': 'PRIVATE'},
                  'client_https': hist}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'metrics.csv'
            smoke.paper_p1_metrics_csv(path, [{'phase': 'p2', 'snapshot': sample}])
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
            groups = {r['group'] for r in rows}
            self.assertTrue({'unix_auth', 'unix_auth_handler', 'jwks_cache', 'client_https'} <= groups)
            self.assertNotIn('PRIVATE', path.read_text())

    def test_generated_https_certificates_pass_default_strict_tls_and_deny_wrong_trust(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'tls', paper_p2=True)
            server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server.load_cert_chain(f.private / 'p2-tls/server.crt', f.private / 'p2-tls/server.key')
            for hostname, trust_fixture, allowed in [('localhost', True, True),
                                                     ('wrong.example.test', True, False),
                                                     ('localhost', False, False)]:
                with self.subTest(hostname=hostname, trust_fixture=trust_fixture):
                    client = ssl.create_default_context(cafile=str(f.private / 'p2-ca.crt')) if trust_fixture else ssl.create_default_context()
                    client.verify_flags |= ssl.VERIFY_X509_STRICT
                    a, b = socket.socketpair()
                    a.settimeout(3)
                    b.settimeout(3)
                    def serve():
                        try:
                            with server.wrap_socket(a, server_side=True):
                                pass
                        except ssl.SSLError:
                            pass
                        finally:
                            a.close()
                    thread = threading.Thread(target=serve)
                    thread.start()
                    try:
                        if allowed:
                            with client.wrap_socket(b, server_hostname=hostname):
                                pass
                        else:
                            with self.assertRaises(ssl.SSLCertVerificationError):
                                with client.wrap_socket(b, server_hostname=hostname):
                                    pass
                    finally:
                        b.close()
                        thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())

    def test_tls_failure_never_sets_https_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'failure', paper_p2=True)
            registry = Mock()
            registry.command.return_value = Mock(returncode=0,
                stderr=b'--with-http_auth_request_module',
                stdout=b'uid=101(nginx)\n1883:101:2710\n1883:101:660\n')
            with patch.object(smoke, 'http_request', side_effect=ValueError('http_request_failed: SSLCertVerificationError')):
                with self.assertRaises(ValueError):
                    smoke.paper_p2_support().run_perimeter(smoke, f, registry,
                        smoke.HTTPSEndpoint('https://localhost', f.private / 'p2-ca.crt'),
                        {'nginx': {'receipt': {'id': 'fixture'}}},
                        {k: 'fixture-image' for k in ('supabase-auth', 'supabase-storage', 'supabase-envoy', 'nginx')}, None)
            run = json.loads((f.artifacts / 'run.json').read_text())
            self.assertEqual(run['status'], 'FAIL')
            self.assertFalse(run['https_verified'])

    def test_p2_profile_has_https_socket_and_bootstrap_order(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'full', paper_p2=True)
            m = smoke.dry_effective_config(f)
            services = smoke.fixture_services(f)
            self.assertLess(services.index('supabase-envoy'), services.index('backend'))
            self.assertLess(services.index('auth-socket-init'), services.index('backend'))
            self.assertEqual(m['services']['backend']['environment']['SUPABASE_JWT_MODE'], 'rs256')
            self.assertNotIn('SUPABASE_JWT_SECRET', m['services']['backend']['environment'])
            self.assertEqual(m['services']['nginx']['ports'][0]['target'], 443)
            self.assertEqual(m['services']['backend']['environment']['AUTH_UNIX_SOCKET_PATH'], '/run/iot-auth/auth.sock')
            self.assertEqual(m['services']['backend']['environment']['WEBSOCKET_ENABLED'], 'false')

    def test_p7_mode_alone_enables_ws_and_rejects_p2_mode_mix(self):
        with tempfile.TemporaryDirectory() as directory:
            p2 = smoke.prepare(Path(directory) / 'p2', paper_p2=True)
            p7 = smoke.prepare(Path(directory) / 'p7', paper_p7=True)
            self.assertFalse(p2.paper_p7)
            self.assertEqual(p2.values['WEBSOCKET_ENABLED'], 'false')
            self.assertTrue(p7.paper_p7)
            self.assertEqual(p7.values['WEBSOCKET_ENABLED'], 'true')
            self.assertEqual(smoke.fixture_services(p7), smoke.fixture_services(p2))
            with self.assertRaisesRegex(ValueError, 'paper_p7_mode_rejected'):
                smoke.prepare(Path(directory) / 'mixed', paper_p7=True, paper_p2=True)

    def test_p8_slow_selector_reuses_p2_overlay_with_websocket_only_in_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p8-slow', paper_p8_slow=True)
            self.assertTrue(fixture.paper_p8_slow)
            self.assertTrue(fixture.paper_p2)
            self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'true')
            overlay = (fixture.private / 'compose.p2.yml').read_text()
            self.assertIn("WEBSOCKET_ENABLED: 'true'", overlay)
            self.assertNotIn("\n        WEBSOCKET_ENABLED:", overlay)
            self.assertIn("WS_HEARTBEAT_INTERVAL: '2s'", overlay)
            self.assertIn("WS_PONG_TIMEOUT: '4s'", overlay)
            self.assertIn("WS_WRITE_TIMEOUT: '1s'", overlay)
            self.assertEqual(fixture.values['WS_MAX_SESSIONS'], '64')
            for key in ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED',
                        'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
                self.assertEqual(fixture.values[key], 'false')
            with self.assertRaisesRegex(ValueError, 'paper_p8_mode_rejected'):
                smoke.prepare(Path(directory) / 'mixed-p8', paper_p8_sessions=True, paper_p8_slow=True)

    def test_p8_e4_enables_websocket_only_in_its_private_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p8-e4', paper_p8_e4='on')
            self.assertTrue(fixture.paper_p2)
            self.assertTrue(fixture.paper_p8_e4)
            self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'true')
            self.assertEqual(fixture.values['METRICS_ENABLED'], 'true')
            overlay = (fixture.private / 'compose.p2.yml').read_text()
            self.assertIn("WEBSOCKET_ENABLED: 'true'", overlay)
            default = (smoke.ROOT / 'docker-compose.yml').read_text()
            self.assertIn('WEBSOCKET_ENABLED: ${WEBSOCKET_ENABLED:-false}', default)

    def test_nginx_perimeter_template_is_separate_and_fail_closed(self):
        text = (smoke.ROOT / 'config/nginx/paper-p2.conf.template').read_text()
        for required in ('listen 443 ssl', 'auth_request /_p2_auth;', 'auth_request /_p2_ws_auth;',
                         'proxy_pass http://auth_verifier/internal/auth/verify/ws;',
                         'proxy_set_header Sec-WebSocket-Protocol $http_sec_websocket_protocol;', 'internal;',
                         'unix:/run/iot-auth/auth.sock', 'proxy_pass_request_body off;',
                         'client_max_body_size 16k;', 'client_max_body_size 50m;',
                         'proxy_set_header X-Forwarded-For $remote_addr;'):
            self.assertIn(required, text)
        self.assertNotIn('satisfy any', text)
        self.assertNotIn('auth_request', (smoke.ROOT / 'config/nginx/nginx.conf.template').read_text())

    def test_p7_ws_overlay_is_opt_in_and_uses_private_unix_verifier(self):
        base = (smoke.ROOT / 'docker-compose.yml').read_text()
        overlay = (smoke.ROOT / 'docker-compose.p7-ws.yml').read_text()
        template = (smoke.ROOT / 'config/nginx/paper-p7-ws.conf.template').read_text()
        default_nginx = (smoke.ROOT / 'config/nginx/nginx.conf.template').read_text()
        self.assertIn('WEBSOCKET_ENABLED: ${WEBSOCKET_ENABLED:-false}', base)
        self.assertIn('AUTH_UNIX_SOCKET_PATH: ${AUTH_UNIX_SOCKET_PATH:-}', base)
        self.assertNotIn('auth_socket', base)
        self.assertIn('auth_socket_p7:/run/iot-auth', overlay)
        self.assertIn('auth_socket_p7:/run/iot-auth:ro', overlay)
        self.assertIn('group_add:', overlay)
        self.assertIn('chmod 2710 /run/iot-auth', overlay)
        self.assertIn('cap_add: [ CHOWN, FOWNER, FSETID ]', overlay)
        self.assertIn("WEBSOCKET_ENABLED: 'true'", overlay)
        self.assertIn("AUTH_UNIX_SOCKET_PATH: /run/iot-auth/auth.sock", overlay)
        self.assertIn('location = /v1/ws', template)
        self.assertIn('auth_request /_p7_ws_auth;', template)
        self.assertIn('proxy_pass http://auth_verifier/internal/auth/verify/ws;', template)
        self.assertIn('proxy_set_header Sec-WebSocket-Protocol $http_sec_websocket_protocol;', template)
        self.assertIn('unix:/run/iot-auth/auth.sock', template)
        self.assertIn('internal;', template)
        self.assertNotIn('/v1/telemetry/ws', template)
        self.assertNotIn('/v1/telemetry/ws', default_nginx)
        self.assertIn('location = /v1/ws {\n        return 404;', default_nginx)
        self.assertNotIn('auth_request', default_nginx)
        self.assertNotIn('proxy_pass http://backend_api;', template.split('location = /v1/ws', 1)[1].split('location /v1/', 1)[0].split('auth_request', 1)[0])
        for identity_header in ('X-User-ID', 'X-Authenticated-User', 'X-Role', 'X-Forwarded-User', 'X-Forwarded-Roles', 'Forwarded'):
            self.assertIn(f'proxy_set_header {identity_header} "";', template)

    def test_spike_is_opt_in_with_storage_and_private_auth_keys_only(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'p2', paper_p2_spike=True)
            self.assertIn('supabase-storage', smoke.fixture_services(fixture))
            self.assertFalse(fixture.paper_p0)
            self.assertIsNone(fixture.paper_p1)
            self.assertEqual(fixture.values['STAGE5_ENABLED'], 'false')
            self.assertNotIn('GOTRUE_JWT_KEYS', fixture.env_file.read_text())
            keys = json.loads((fixture.private / 'p2-signing-keys.json').read_text())
            self.assertEqual(sum('sign' in k['key_ops'] for k in keys), 1)
            self.assertEqual(keys[0]['alg'], 'RS256')
            public = json.loads((fixture.private / 'p2-public-jwks.json').read_text())
            self.assertEqual(len(public['keys']), 1)
            self.assertFalse(set(public['keys'][0]) & {'d', 'p', 'q', 'dp', 'dq', 'qi', 'k'})
            self.assertEqual((fixture.private / 'p2-signing-keys.json').stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(fixture.artifacts.iterdir()), [])
            model = smoke.dry_effective_config(fixture)
            self.assertEqual(json.loads(model['services']['supabase-auth']['environment']['GOTRUE_JWT_KEYS']), keys)
            self.assertEqual(json.loads(model['services']['supabase-storage']['environment']['JWT_JWKS']), public)
            for service, config in model['services'].items():
                if service != 'supabase-auth':
                    self.assertNotIn(keys[0]['d'], json.dumps(config))
            self.assertEqual(model['services']['backend']['cpus'], 2)

    def test_old_modes_do_not_generate_or_configure_rsa_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            for name, kwargs in (('p0', {'paper_p0': True}), ('p1', {'paper_p1': 'baseline'})):
                fixture = smoke.prepare(Path(directory) / name, **kwargs)
                self.assertFalse(fixture.paper_p2_spike)
                self.assertFalse((fixture.private / 'p2-signing-keys.json').exists())
                self.assertNotIn('compose.p2.yml', ' '.join(smoke.compose_command(fixture, 'config')))

    def test_spike_cannot_be_combined_with_p0_or_p1(self):
        with tempfile.TemporaryDirectory() as directory:
            for options in ({'paper_p0': True}, {'paper_p1': 'baseline'}):
                with self.assertRaises(ValueError):
                    smoke.prepare(Path(directory) / 'p2', paper_p2_spike=True, **options)


if __name__ == '__main__':
    unittest.main()
