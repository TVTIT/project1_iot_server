#!/usr/bin/env python3
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import stage2_mosquitto_runtime as runtime


class Contracts(unittest.TestCase):
    def parse_yaml_service_volumes(self, yaml_text, service_name):
        lines = yaml_text.splitlines()
        in_services = False
        in_service = False
        in_volumes = False
        volume_blocks = []
        current_block = []

        for line in lines:
            raw_indent = len(line) - len(line.lstrip(' '))
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue

            if not in_services:
                if stripped == 'services:' and raw_indent == 0:
                    in_services = True
                continue

            if in_services and not in_service:
                if stripped == f'{service_name}:' and raw_indent == 2:
                    in_service = True
                elif raw_indent == 0:
                    break
                continue

            if in_service:
                if not in_volumes:
                    if raw_indent <= 2 and stripped.endswith(':'):
                        break
                    if stripped == 'volumes:' and raw_indent == 4:
                        in_volumes = True
                    continue
                if in_volumes:
                    if raw_indent <= 4:
                        break
                    if line.lstrip().startswith('- '):
                        if current_block:
                            volume_blocks.append('\n'.join(current_block))
                            current_block = []
                        current_block.append(line)
                    elif current_block:
                        current_block.append(line)

        if current_block:
            volume_blocks.append('\n'.join(current_block))
        return volume_blocks

    def assert_source_yaml_ca_only(self, yaml_source, backend='backend', private_services=()):
        if isinstance(yaml_source, (str, Path)) and '\n' not in str(yaml_source):
            yaml_text = Path(yaml_source).read_text()
            filepath = str(yaml_source)
        else:
            yaml_text = str(yaml_source)
            filepath = '<memory>'

        vols = self.parse_yaml_service_volumes(yaml_text, backend)
        ca_vols = [v for v in vols if 'ca.crt' in v]
        self.assertEqual(len(ca_vols), 1, f'{filepath}: expected 1 CA mount in {backend}')
        ca = ca_vols[0]
        self.assertTrue(re.search(r'type:\s*bind\b', ca), f'{filepath}: CA mount must be type bind')
        self.assertTrue(re.search(r'read_only:\s*true\b', ca), f'{filepath}: CA mount must be read_only: true')
        self.assertTrue(re.search(r'bind:\s*\n\s*create_host_path:\s*false\b', ca),
                        f'{filepath}: CA mount must explicitly declare bind: create_host_path: false')
        self.assertFalse(re.search(r'create_host_path:\s*true\b', ca),
                         f'{filepath}: CA mount must not set create_host_path: true')

        for v in vols:
            self.assertFalse('.key' in v, f'{filepath}: key mount forbidden in {backend}: {v}')
            self.assertFalse(re.search(r'/certs(?!/ca\.crt)', v),
                             f'{filepath}: broad certs mount forbidden in {backend}: {v}')

        for name in private_services:
            p_vols = self.parse_yaml_service_volumes(yaml_text, name)
            for v in p_vols:
                self.assertFalse(re.search(r'/mosquitto/config\b', v),
                                 f'{filepath}: broad /mosquitto/config forbidden in {name}: {v}')
                self.assertFalse('.key' in v, f'{filepath}: key mount forbidden in {name}: {v}')
                self.assertFalse(re.search(r'/certs\b', v),
                                 f'{filepath}: certs mount forbidden in {name}: {v}')

    def assert_ca_only(self, config, backend, private_services):
        service = config['services'][backend]
        target = service['environment']['MQTT_TLS_CA_FILE']
        mounts = service['volumes']
        ca = [m for m in mounts if m['target'] == target]
        self.assertEqual(len(ca), 1, 'CA must be an individual file mount')
        self.assertTrue(ca[0].get('read_only'), 'CA must be read-only')
        self.assertTrue(ca[0]['source'].endswith('/ca.crt'))
        self.assertEqual(ca[0]['type'], 'bind')
        bind_opts = ca[0].get('bind') or {}
        self.assertFalse(bind_opts.get('create_host_path', False),
                         'CA bind mount must not enable create_host_path')
        for mount in mounts:
            self.assertFalse(mount['source'].endswith('.key'))
            self.assertFalse(mount['source'].endswith('/certs'))
            self.assertFalse(mount['target'].endswith('.key'))
            self.assertFalse(target.startswith(mount['target'].rstrip('/') + '/'),
                             'broad certificate/config mount forbidden')
        for name in private_services:
            for mount in config['services'][name].get('volumes', []):
                self.assertFalse(mount['target'].startswith('/mosquitto/config'))
                self.assertFalse(mount['source'].endswith(('.key', '/certs')))

    def test_actual_production_compose_ca_only(self):
        import json
        # No inherited deployment environment, implicit .env, or printed config.
        env = dict(runtime.ENV, COMPOSE_DISABLE_ENV_FILE='1')
        for key in ('MQTT_PASSWORD', 'POSTGRES_PASSWORD', 'AUTH_DB_PASSWORD',
                    'STORAGE_DB_PASSWORD', 'BACKEND_DB_PASSWORD', 'JWT_SECRET',
                    'DATABASE_URL', 'GOTRUE_JWT_ISSUER', 'SUPABASE_JWT_AUDIENCE'):
            env[key] = 'isolated-test-placeholder'
        env['MQTT_TLS_CA_SOURCE'] = '/tmp/opencode/public-fixture/ca.crt'
        env['MQTT_TLS_CA_FILE'] = '/mqtt-public/ca.crt'
        self.assert_source_yaml_ca_only(runtime.ROOT / 'docker-compose.yml', 'backend',
                                       ('mosquitto-auth-init', 'mosquitto-reloader'))
        config = json.loads(runtime.run(['docker', 'compose', '--env-file', '/dev/null',
                                        '-f', str(runtime.ROOT / 'docker-compose.yml'),
                                        'config', '--format', 'json'], env=env, capture=True))
        self.assert_ca_only(config, 'backend', ('mosquitto-auth-init', 'mosquitto-reloader'))
        ca = next(m for m in config['services']['backend']['volumes']
                  if m['target'] == '/mqtt-public/ca.crt')
        self.assertEqual(ca['source'], env['MQTT_TLS_CA_SOURCE'])
        del env['MQTT_TLS_CA_SOURCE'], env['MQTT_TLS_CA_FILE']
        defaults = json.loads(runtime.run(['docker', 'compose', '--env-file', '/dev/null',
                                          '-f', str(runtime.ROOT / 'docker-compose.yml'),
                                          'config', '--format', 'json'], env=env, capture=True))
        self.assert_ca_only(defaults, 'backend', ('mosquitto-auth-init', 'mosquitto-reloader'))
        self.assertEqual(defaults['services']['backend']['environment']['MQTT_TLS_CA_FILE'],
                         '/mosquitto/config/certs/ca.crt')

    def test_fixture_compose_ca_only(self):
        import json
        self.assert_source_yaml_ca_only(runtime.COMPOSE, 'backend', ('initializer', 'reloader'))
        poc = runtime.PoC('/tmp/opencode/public-fixture')
        config = json.loads(poc.command('config', '--format', 'json', capture=True))
        self.assert_ca_only(config, 'backend', ('initializer', 'reloader'))

    def test_ca_mount_normalized_json_semantics_and_restrictions(self):
        def make_config(ca_bind=None, has_bind=True, read_only=True, ca_source='/etc/ca.crt',
                        ca_type='bind', extra_backend_volumes=None, private_volumes=None):
            mount = {
                'type': ca_type,
                'source': ca_source,
                'target': '/mosquitto/config/certs/ca.crt',
                'read_only': read_only,
            }
            if has_bind:
                mount['bind'] = {} if ca_bind is None else ca_bind
            backend_vols = [mount] + (extra_backend_volumes or [])
            return {
                'services': {
                    'backend': {
                        'environment': {'MQTT_TLS_CA_FILE': '/mosquitto/config/certs/ca.crt'},
                        'volumes': backend_vols,
                    },
                    'private_svc': {'volumes': private_volumes or []},
                }
            }

        # Explicit create_host_path: false must pass
        cfg_explicit_false = make_config(ca_bind={'create_host_path': False})
        self.assert_ca_only(cfg_explicit_false, 'backend', ('private_svc',))

        # Omitted false canonical Compose output (empty bind dict or missing bind key) must pass
        cfg_omitted_dict = make_config(ca_bind={})
        self.assert_ca_only(cfg_omitted_dict, 'backend', ('private_svc',))
        cfg_omitted_key = make_config(has_bind=False)
        self.assert_ca_only(cfg_omitted_key, 'backend', ('private_svc',))

        # Explicit true must be rejected
        cfg_explicit_true = make_config(ca_bind={'create_host_path': True})
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_explicit_true, 'backend', ('private_svc',))

        # read_only must be true
        cfg_ro = make_config(read_only=False)
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_ro, 'backend', ('private_svc',))

        # type must be bind
        cfg_type = make_config(ca_type='volume')
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_type, 'backend', ('private_svc',))

        # source must end in /ca.crt
        cfg_src = make_config(ca_source='/etc/other.crt')
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_src, 'backend', ('private_svc',))

        # backend mounting private key forbidden
        cfg_key = make_config(extra_backend_volumes=[{'source': '/tmp/server.key', 'target': '/app/server.key'}])
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_key, 'backend', ('private_svc',))

        # backend mounting /certs directory forbidden
        cfg_certs = make_config(extra_backend_volumes=[{'source': '/tmp/certs', 'target': '/app/certs'}])
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_certs, 'backend', ('private_svc',))

        # private service mounting /mosquitto/config forbidden
        cfg_priv_cfg = make_config(private_volumes=[{'target': '/mosquitto/config/passwords', 'source': '/tmp/p'}])
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_priv_cfg, 'backend', ('private_svc',))

        # private service mounting .key forbidden
        cfg_priv_key = make_config(private_volumes=[{'target': '/app/key', 'source': '/tmp/server.key'}])
        with self.assertRaises(AssertionError):
            self.assert_ca_only(cfg_priv_key, 'backend', ('private_svc',))

    def test_source_yaml_ca_mount_contract_guards(self):
        # Both actual source YAML files must pass
        self.assert_source_yaml_ca_only(runtime.ROOT / 'docker-compose.yml', 'backend',
                                       ('mosquitto-auth-init', 'mosquitto-reloader'))
        self.assert_source_yaml_ca_only(runtime.COMPOSE, 'backend', ('initializer', 'reloader'))

        good_yaml = '''services:
  backend:
    volumes:
      - auth:/mosquitto/auth
      - type: bind
        source: ./config/mosquitto/certs/ca.crt
        target: /mosquitto/config/certs/ca.crt
        read_only: true
        bind:
          create_host_path: false
  private_svc:
    volumes:
      - auth:/mosquitto/auth
'''
        self.assert_source_yaml_ca_only(good_yaml, 'backend', ('private_svc',))

        # Missing create_host_path: false must be rejected
        bad_missing_create_host = good_yaml.replace('          create_host_path: false', '')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_missing_create_host, 'backend', ('private_svc',))

        # Explicit create_host_path: true must be rejected
        bad_true_create_host = good_yaml.replace('create_host_path: false', 'create_host_path: true')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_true_create_host, 'backend', ('private_svc',))

        # read_only: true missing/false must be rejected
        bad_ro = good_yaml.replace('read_only: true', 'read_only: false')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_ro, 'backend', ('private_svc',))

        # non-bind type must be rejected
        bad_type = good_yaml.replace('type: bind', 'type: volume')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_type, 'backend', ('private_svc',))

        # mounting private key in backend forbidden
        bad_backend_key = good_yaml.replace('  private_svc:', '      - ./server.key:/mosquitto/key\n  private_svc:')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_backend_key, 'backend', ('private_svc',))

        # mounting /certs in backend forbidden
        bad_backend_certs = good_yaml.replace('  private_svc:', '      - ./certs:/mosquitto/certs\n  private_svc:')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_backend_certs, 'backend', ('private_svc',))

        # private service mounting /mosquitto/config forbidden
        bad_priv_cfg = good_yaml.replace('  private_svc:\n    volumes:\n      - auth:/mosquitto/auth\n',
                                         '  private_svc:\n    volumes:\n      - /host/config:/mosquitto/config\n')
        with self.assertRaises(AssertionError):
            self.assert_source_yaml_ca_only(bad_priv_cfg, 'backend', ('private_svc',))

    def test_cleanup_owned_scope_partial_build_and_recreated_resources(self):
        poc = runtime.PoC('/tmp/opencode')
        commands = []
        def fake_run(argv, **kwargs):
            commands.append(argv)
            if 'reference=' + poc.owned_images[0] in argv and '--format' in argv:
                return (poc.owned_images[0] + '\n').encode()
            if 'ls' in argv and any(v.startswith('label=') for v in argv):
                # Registry before down sees resources created/recreated after
                # the initial snapshot; verification after down sees none.
                return b'new-owned-id\n' if len([c for c in commands if c[:2] == argv[:2]]) == 1 else b''
            return b''
        with patch.object(runtime, 'run', side_effect=fake_run), patch.object(poc, 'command') as command:
            poc.cleanup()
        command.assert_called_once_with('down', '-v', '--remove-orphans', timeout=60)
        self.assertEqual(poc.owned['container'], ['new-owned-id'])
        self.assertIn(['docker', 'image', 'rm', poc.owned_images[0]], commands)
        for argv in commands:
            self.assertNotIn('prune', argv)
            self.assertNotIn('-f', argv)
            if 'label=com.docker.compose.project=' + poc.project in argv:
                self.assertIn('--filter', argv)

    def test_cleanup_failures_are_fatal(self):
        for failure in ('down', 'remaining', 'image-remove'):
            poc = runtime.PoC('/tmp/opencode')
            def fake_run(argv, failure=failure, **kwargs):
                if failure == 'remaining' and 'ls' in argv and 'container' in argv:
                    return b'owned-leftover\n'
                if failure == 'image-remove':
                    if '--format' in argv: return b'owned-tag\n'
                    if 'rm' in argv: raise RuntimeError('subprocess failed')
                return b''
            with patch.object(runtime, 'run', side_effect=fake_run), patch.object(poc, 'command', side_effect=RuntimeError('down failed') if failure == 'down' else None), self.assertRaises(RuntimeError):
                poc.cleanup()

    def test_cleanup_rejects_foreign_resource_names(self):
        poc = runtime.PoC('/tmp/opencode')
        poc.project = 'user-deployment'
        with patch.object(runtime, 'run') as run, patch.object(poc, 'command') as command, self.assertRaises(RuntimeError):
            poc.cleanup()
        run.assert_not_called()
        command.assert_not_called()

    def test_fault_selectors_no_skip_and_output_secrecy(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            poc = runtime.PoC(directory)
            fixture = {'Old': 'private-old', 'New': 'private-new', 'Backend': 'private-backend', 'BPassword': 'private-b'}
            proof = ('--- PASS: TestProductionRuntimeFaultIntegration\n'
                     'production adapter phase completed: fault-probe\nadapter_exit=0\n')
            with patch.object(poc, 'exec', return_value=(proof + 'private-new').encode()), self.assertRaisesRegex(RuntimeError, 'secret detected'):
                poc.adapter_phase(fixture, 'fault-probe')
            runtime.assert_adapter_output(proof, 'fault-probe', 'TestProductionRuntimeFaultIntegration')
            with self.assertRaises(RuntimeError):
                runtime.assert_adapter_output(proof + '--- SKIP:', 'fault-probe', 'TestProductionRuntimeFaultIntegration')

    def test_adapter_proof_cannot_skip_or_select_nothing(self):
        valid = ('--- PASS: TestProductionRuntimeIntegration (1s)\n'
                 'production adapter phase completed: mutate\nadapter_exit=0\n')
        runtime.assert_adapter_output(valid, 'mutate')
        for output in ('PASS', valid + '--- SKIP: other',
                       valid.replace('mutate', 'persist'),
                       valid.replace('--- PASS:', '--- FAIL:'),
                       valid.replace('adapter_exit=0', 'adapter_exit=1')):
            with self.assertRaises(RuntimeError):
                runtime.assert_adapter_output(output, 'mutate')

    def test_adapter_fixture_only_stdin_and_coverage_not_store(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            poc = runtime.PoC(directory)
            poc.project = Path(directory).name
            fixture = {'Old': 'old-private-secret', 'New': 'new-private-secret'}
            proof = (b'--- PASS: TestProductionRuntimeIntegration\n'
                     b'production adapter phase completed: mutate\nadapter_exit=0\n')
            with patch.object(poc, 'exec', side_effect=[proof, b'mode: set\n']) as execute:
                poc.adapter_phase(fixture, 'mutate')
            args, kwargs = execute.call_args_list[0]
            self.assertNotIn('private-secret', repr(args))
            self.assertIn(b'private-secret', kwargs['data'])
            self.assertIn('MQTT_RUNTIME_INTEGRATION=1', repr(args))
            artifact = Path('/tmp/opencode') / (poc.project + '-adapter-mutate.cover')
            self.assertEqual(artifact.read_bytes(), b'mode: set\n')
            artifact.unlink()

    def test_production_reloader_protocol_helper_half_closes(self):
        self.assertIn('CloseWrite()', runtime.HELPER)
        self.assertIn('io.ReadAll(c)', runtime.HELPER)
        self.assertNotIn('syscall.Kill', runtime.HELPER)

    def test_connack_requires_exact_auth_rejection(self):
        runtime.assert_connack(b'\x20\x02\x00\x05', False)
        for packet in (b'', b'\x20\x02\x00\x00', b'\x20\x02\x00\x03'):
            with self.assertRaises(RuntimeError):
                runtime.assert_connack(packet, False)

    def test_success_requires_exact_connack(self):
        runtime.assert_connack(b'\x20\x02\x00\x00', True)
        with self.assertRaises(RuntimeError):
            runtime.assert_connack(b'', True)

    def test_backend_protected(self):
        with self.assertRaises(ValueError):
            runtime.validate_gateway('backend_service')
        with self.assertRaises(ValueError):
            runtime.validate_gateway('bad:entry')
        with self.assertRaises(ValueError):
            runtime.validate_gateway('_bad')
        runtime.validate_gateway('gw_test')

    def test_native_tool_secrecy(self):
        with patch.object(runtime.subprocess, 'run') as run:
            run.return_value.returncode = 0
            runtime.run(['docker', 'exec', '-i', 'owned', runtime.TOOL,
                         '-H', 'sha512-pbkdf2', '/mosquitto/data/candidate',
                         'gw_test'], b'test-secret\ntest-secret\n')
            args, kwargs = run.call_args
            self.assertNotIn('test-secret', repr(args))
            self.assertNotIn('test-secret', repr(kwargs['env']))
            self.assertEqual(kwargs['input'], b'test-secret\ntest-secret\n')
            self.assertEqual(kwargs['stdout'], runtime.subprocess.DEVNULL)

    def test_command_errors_suppressed(self):
        with patch.object(runtime.subprocess, 'run') as run:
            run.return_value.returncode = 1
            with self.assertRaisesRegex(RuntimeError, '^subprocess failed$'):
                runtime.run(['false'])

    def test_connack_network_error_is_not_auth_denial(self):
        with patch.object(runtime.ssl, 'create_default_context'), \
                patch.object(runtime.socket, 'create_connection', side_effect=TimeoutError), self.assertRaises(TimeoutError):
            runtime.mqtt_connection(1, '/missing-ca', 'test', 'password', False)


class WorkflowGuards(unittest.TestCase):
    def test_ci_keeps_baseline_and_runs_real_runtime(self):
        workflow = (runtime.ROOT / '.github/workflows/ci.yml').read_text()
        import re
        jobs = re.findall(r'^  ([a-z-]+):\n    runs-on:', workflow, re.MULTILINE)
        self.assertEqual(set(jobs), {'lint-and-test', 'cross-compile', 'migration-check',
                                   'docker-build', 'authorization-integration',
                                   'provisioning-integration', 'auth-integration',
                                   'mosquitto-runtime-integration'})
        job = workflow.split('  mosquitto-runtime-integration:', 1)[1]
        self.assertIn('sh scripts/test-stage2-mosquitto-runtime.sh', job)
        self.assertIn("-p 'test_stage2_mosquitto_runtime.py'", job)
        self.assertIn('go-version:', job)
        self.assertNotIn('continue-on-error', job)
        self.assertNotIn('|| true', job)
        for target in ('auth-init', 'reloader', 'backend'):
            self.assertIn('target: ' + target, workflow)


if __name__ == '__main__':
    unittest.main()
