"""Offline safety regressions; these are not native container proof."""
import ast
import hashlib
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from scripts.tests import task28_nginx_compose_rehearsal as r

class RehearsalTests(unittest.TestCase):
    def receipt(self):
        return dict(resource_id='a' * 64, expected_project='p', expected_role='backend', expected_name='p-backend-1')

    def test_missing_or_mismatched_ownership_blocks_inspect_and_start(self):
        for owned in ({'status': 'unknown'}, {'status': 'owned', 'resource_id': 'b' * 64}):
            with patch.object(r, 'inspect_owned_resource', return_value=owned), patch.object(r, 'docker') as docker, patch.object(r.subprocess, 'run') as run:
                with self.assertRaises(RuntimeError):
                    r.backend_start_metadata(self.receipt(), Path('/tmp/opencode/backend-server'), 'a' * 64)
                with self.assertRaises(RuntimeError):
                    r.start_container('a' * 64, self.receipt())
                docker.assert_not_called()
                run.assert_not_called()
        with patch.object(r.subprocess, 'run') as run:
            with self.assertRaises(RuntimeError):
                r.start_container('a' * 64)
            run.assert_not_called()

    def test_metadata_failure_is_unknown_not_proof(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            p = Path(d) / 'backend-server'
            p.write_bytes(b'not ELF private-secret')
            with self.assertRaisesRegex(RuntimeError, '^binary_metadata_invalid$'):
                r.binary_metadata(p)
        clean = {role + '_cleanup_verified': True for role in ('nginx', 'backend', 'pg', 'broker', 'host')}
        self.assertFalse(r.aggregate('UNKNOWN', clean)['success'])

    def test_metadata_allowlist_rejects_secret_fields(self):
        data = dict(entrypoint=[r.BACKEND_EXECUTABLE], cmd=None, user='1883:1883', readonly_root=True,
                    apparmor='docker-default', userns='', cap_add=None, cap_drop=['ALL'],
                    security_opt=['no-new-privileges:true'], binary_source='/tmp/opencode/backend-server',
                    binary_destination=r.BACKEND_EXECUTABLE, binary_rw=False)
        safe = r.sanitize_backend_start_metadata(data, Path(data['binary_source']))
        self.assertNotIn('binary_source', safe)
        data['Env'] = ['SECRET=private-secret']
        with self.assertRaisesRegex(RuntimeError, '^backend_metadata_invalid$'):
            r.sanitize_backend_start_metadata(data, Path('/tmp/opencode/backend-server'))
        data.pop('Env')
        data['cmd'] = ['private-secret']
        with self.assertRaises(RuntimeError):
            r.sanitize_backend_start_metadata(data, Path('/tmp/opencode/backend-server'))

    def test_findmnt_is_exact_target_and_flags_only(self):
        with patch.object(r.subprocess, 'run', return_value=MagicMock(returncode=0, stdout=b'rw,noatime,inode64,private-secret\n')) as run:
            flags = r.binary_mount_flags(Path('/tmp/opencode/backend-server'))
        self.assertEqual(flags, ['rw', 'noatime', 'inode64'])
        self.assertEqual(run.call_args.args[0], ['findmnt', '--target', '/tmp/opencode/backend-server', '--noheadings', '--output', 'OPTIONS'])

    def test_regular_elf_header_metadata_and_hash_without_content(self):
        header = bytearray(64)
        header[:6] = b'\x7fELF\x02\x01'
        struct.pack_into('<HH', header, 16, 2, 62)
        struct.pack_into('<Q', header, 32, 64)
        struct.pack_into('<HH', header, 54, 56, 1)
        program = bytearray(56)
        struct.pack_into('<I', program, 0, 1)
        content = bytes(header + program) + b'private-secret'
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            p = Path(d) / 'backend-server'
            p.write_bytes(content)
            p.chmod(0o555)
            meta = r.binary_metadata(p)
            link = Path(d) / 'link'
            link.symlink_to(p)
            with self.assertRaises(RuntimeError):
                r.binary_metadata(link)
        self.assertEqual(meta['mode'], '0555')
        self.assertEqual(meta['sha256'], hashlib.sha256(content).hexdigest())
        self.assertTrue(meta['elf']['static_format'])
        self.assertNotIn('private-secret', str(meta))

    def test_start_diagnostics_are_allowlisted_and_never_echo_secrets(self):
        examples = [
            (b'error mounting: mkdir /app: read-only file system secret-token', 'missing_mount_destination'),
            (b'exec: permission denied secret-token', 'permission_denied'),
            (b'exec: no such file or directory secret-token', 'no_such_executable'),
            (b'read-only file system secret-token', 'readonly_rootfs'),
            (b'exec format error secret-token', 'exec_format'),
            (b'unknown secret-token postgres://private', 'generic_start_failure'),
        ]
        for stderr, code in examples:
            with self.subTest(code=code):
                diagnostic = r.classify_start_failure(1, stderr)
                self.assertEqual(diagnostic, {'exit_code': 1, 'code': code})
                self.assertNotIn('secret', str(diagnostic))

    def test_start_failure_keeps_only_safe_diagnostic(self):
        process = MagicMock(returncode=1, stderr=b'exec format error private-secret')
        with patch.object(r, 'require_consumer_owned'), patch.object(r.subprocess, 'run', return_value=process):
            with self.assertRaises(r.ContainerStartFailure) as raised:
                r.start_container('a' * 64, self.receipt())
        self.assertEqual(str(raised.exception), 'container_start_failed')
        self.assertEqual(raised.exception.diagnostic, {'exit_code': 1, 'code': 'exec_format'})

    def test_runtime_binary_uses_existing_executable_destination(self):
        self.assertEqual(r.BACKEND_EXECUTABLE, '/usr/sbin/mosquitto')
        code = Path(r.__file__).read_text()
        self.assertIn("'--entrypoint', BACKEND_EXECUTABLE", code)
        self.assertIn('target={BACKEND_EXECUTABLE},readonly', code)
        self.assertNotIn('target=/app/server', code)

    def test_production_credential_requests_have_empty_body(self):
        connection = MagicMock()
        connection.getresponse.return_value.status = 201
        connection.getresponse.return_value.read.return_value = b'{}'
        connection.getresponse.return_value.getheaders.return_value = []
        with patch.object(r.http.client, 'HTTPConnection', return_value=connection):
            r.request(12345, 'POST', '/v1/admin/gateways/test/mqtt-credential', 'token', 'key')
        self.assertIsNone(connection.request.call_args.kwargs['body'])

    def test_private_first_open_and_exclusive(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            p = Path(d) / 'config'
            r.private_write(p, 'secret')
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                r.private_write(p, 'overwrite')

    def test_actual_config_budgets_role_and_paths(self):
        cfg = r.backend_environment('dbpass', 'managerpass', 'backendpass', 'jwtsecret')
        self.assertIn('iot_backend_app:', cfg['DATABASE_URL'])
        self.assertEqual(cfg['MQTT_DYNSEC_JSON_PATH'], '/native-store/dynsec.json')
        self.assertEqual(cfg['MQTT_TLS_CA_FILE'], '/public-ca/ca.crt')
        self.assertGreater(int(cfg['HTTP_WRITE_TIMEOUT'][:-1]), 47)
        self.assertGreater(int(cfg['SHUTDOWN_TIMEOUT'][:-1]), 45)

    def test_offline_build_contract(self):
        with patch.object(r.subprocess, 'run') as run:
            run.return_value.returncode = 0
            self.assertTrue(r.compile_backend_binary(Path('/tmp/opencode/test'))[0])
        env = run.call_args.kwargs['env']
        for k, v in {'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOTOOLCHAIN': 'local', 'CGO_ENABLED': '0', 'GOOS': 'linux', 'GOARCH': 'amd64'}.items():
            self.assertEqual(env[k], v)

    def test_actual_setup_build_follows_all_broker_preparation(self):
        tree = ast.parse(Path(r.__file__).read_text())
        setup = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'run_rehearsal_suite')
        calls = [n for n in ast.walk(setup) if isinstance(n, ast.Call)]
        build = next(n.lineno for n in calls if isinstance(n.func, ast.Name) and n.func.id == 'compile_backend_binary')
        preparations = [n.lineno for n in calls if isinstance(n.func, ast.Attribute) and n.func.attr in ('prepare', 'start') and isinstance(n.func.value, ast.Name) and n.func.value.id == 'broker']
        self.assertEqual(len(preparations), 2)
        self.assertLess(max(preparations), build)
        create_stage = next(n.lineno for n in ast.walk(setup) if isinstance(n, ast.Assign) and isinstance(n.value, ast.BinOp) and isinstance(n.value.right, ast.Constant) and n.value.right.value == '_create')
        self.assertLess(build, create_stage)
        guard = next(n for n in calls if isinstance(n.func, ast.Name) and n.func.id == 'backend_start_metadata')
        self.assertEqual(len(guard.args), 3)

    def test_prestart_binary_mode_and_expected_hash_fail_closed(self):
        data = dict(entrypoint=[r.BACKEND_EXECUTABLE], cmd=None, user='1883:1883', readonly_root=True,
                    apparmor='docker-default', userns='', cap_add=None, cap_drop=['ALL'],
                    security_opt=['no-new-privileges:true'], binary_source='/tmp/opencode/backend-server',
                    binary_destination=r.BACKEND_EXECUTABLE, binary_rw=False)
        raw = dict(data, state='created', binary_mount=[{'source': data['binary_source'], 'destination': r.BACKEND_EXECUTABLE, 'rw': False}])
        for key in ('binary_source', 'binary_destination', 'binary_rw'):
            raw.pop(key)
        import json
        for mode, digest, accepted in [('0555', 'a' * 64, True), ('0600', 'a' * 64, False), ('0755', 'a' * 64, False), ('0555', 'b' * 64, False)]:
            with self.subTest(mode=mode, digest=digest), patch.object(r, 'require_consumer_owned'), patch.object(r, 'docker', return_value=json.dumps(raw)), patch.object(r, 'binary_metadata', return_value={'file_type': 'regular', 'mode': mode, 'sha256': digest}), patch.object(r, 'binary_mount_flags', return_value=['rw']), patch.object(r, 'start_container') as start:
                if accepted:
                    r.backend_start_metadata(self.receipt(), Path(data['binary_source']), 'a' * 64)
                else:
                    with self.assertRaisesRegex(RuntimeError, '^backend_binary_contract_failed$'):
                        r.backend_start_metadata(self.receipt(), Path(data['binary_source']), 'a' * 64)
                start.assert_not_called()

    def test_absence_unknown_never_authorizes_cleanup(self):
        receipt = {'resource_id': 'a' * 64, 'expected_project': 'dynsec_probe_123456789abc', 'expected_role': 'backend', 'expected_name': 'fixture-backend'}
        with patch.object(r, 'inspect_resource_presence', return_value={'status': 'unknown'}), patch.object(r, 'inspect_owned_resource', return_value={'status': 'unknown'}), patch.object(r, 'docker') as docker:
            self.assertFalse(r.remove_consumer(receipt))
        docker.assert_not_called()

    def test_full_id_reverified_and_authoritative_absence(self):
        receipt = {'resource_id': 'a' * 64, 'expected_project': 'dynsec_probe_123456789abc', 'expected_role': 'backend', 'expected_name': 'fixture-backend'}
        with patch.object(r, 'inspect_owned_resource', return_value={'status': 'owned', 'resource_id': 'a' * 64}) as inspect, patch.object(r, 'inspect_resource_presence', side_effect=[{'status': 'exists'}, {'status': 'absent'}]), patch.object(r, 'docker') as docker:
            self.assertTrue(r.remove_consumer(receipt))
        self.assertEqual(inspect.call_count, 1)
        self.assertEqual(docker.call_args.args[0], ['rm', '-f', 'a' * 64])

    def test_unknown_consumer_vetoes_dependencies(self):
        broker, pg = MagicMock(), MagicMock()
        pg.container_id = None
        pg.cidfile.exists.return_value = False
        with patch.object(r, 'remove_consumer', return_value=False), patch.object(r, 'verified_remove_host_fixture_dir') as host:
            result = r.teardown({'backend': {'resource_id': 'a' * 64}}, pg, broker, {})
        self.assertFalse(result['backend_cleanup_verified'])
        pg.cleanup.assert_not_called()
        broker.cleanup.assert_not_called()
        host.assert_not_called()

    def test_order_and_cleanup_veto(self):
        order = []
        broker, pg = MagicMock(), MagicMock()
        pg.container_id = None
        pg.cidfile.exists.return_value = False
        pg.cleanup.side_effect = lambda: order.append('pg') or True
        broker.cleanup.side_effect = lambda: order.append('broker') or {'clean': True}
        with patch.object(r, 'remove_consumer', side_effect=lambda x: order.append(x['role']) or True), patch.object(r, 'verified_remove_host_fixture_dir', side_effect=lambda x: order.append('host') or {'verified': True}):
            clean = r.teardown({'backend': {'role': 'backend', 'resource_id': 'a' * 64}, 'nginx': {'role': 'nginx', 'resource_id': 'b' * 64}}, pg, broker, {})
        self.assertEqual(order, ['nginx', 'backend', 'pg', 'broker', 'host'])
        self.assertTrue(r.aggregate('PASS', clean)['success'])
        clean['host_cleanup_verified'] = False
        self.assertFalse(r.aggregate('PASS', clean)['success'])

    def test_native_preflight_blocks_without_allocation(self):
        with patch.object(r, 'inspect_local_image', return_value=(False, None, 'image_not_found')), patch.object(r, 'create_verified_host_fixture_dir') as allocate:
            result = r.run_rehearsal_suite()
        allocate.assert_not_called()
        self.assertEqual(result['failure_stage'], 'preflight')
        self.assertFalse(result['success'])

    def test_metadata_rejects_secret_volume(self):
        safe = {'user': '1883:1883', 'mounts': [{'Destination': '/native-store', 'RW': False, 'Name': 'p_native_store'}, {'Destination': '/public-ca', 'RW': False, 'Name': 'p_public_ca'}, {'Destination': '/usr/sbin/mosquitto', 'RW': False}], 'networks': ['p_default']}
        self.assertTrue(r.validate_backend_metadata(safe, 'p'))
        safe['mounts'].append({'Destination': '/security', 'RW': False})
        self.assertFalse(r.validate_backend_metadata(safe, 'p'))

    def test_source_ast(self):
        code = Path(r.__file__).read_text()
        ast.parse(code)
        for prohibited in ('httptest', '18080', 'shared_fixture_scope_blocker', 'IMPLEMENTATION_SLICE_COMPLETE'):
            self.assertNotIn(prohibited, code)

    def test_consumer_absence_is_retired_before_shared_cleanup(self):
        broker = MagicMock()
        broker.owned_containers = ['a' * 64, 'b' * 64]
        broker.cleanup.return_value = {'clean': True}
        with patch.object(r, 'remove_consumer', return_value=True):
            r.teardown({'backend': {'resource_id': 'a' * 64}}, None, broker, None)
        self.assertEqual(broker.owned_containers, ['b' * 64])

if __name__ == '__main__':
    unittest.main()
