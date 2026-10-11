#!/usr/bin/env python3
"""Fresh offline bootstrap only; native lane explicitly opted in."""
import importlib.util
import io
import json
import os
from pathlib import Path
import secrets
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).parents[1] / 'mqtt-bootstrap.py'


def load():
    spec = importlib.util.spec_from_file_location('mqtt_bootstrap', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_management_denied(test, backend, manager, username):
    """State oracle shared by restrictive and deliberately permissive fixtures."""
    backend.publish('$CONTROL/dynamic-security/v1', json.dumps({'commands': [
        {'command': 'disableClient', 'username': username}]}).encode())
    # Same TCP/TLS stream, QoS0 non-retained PUBLISH then allowed SUBSCRIBE.
    # Pinned Mosquitto2.0.18 processes the write ACL/control callback inline
    # before the next packet; the helper waits for this subscription's exact
    # SUBACK under its 2s socket timeout. This is a processing barrier, NOT an
    # authorization, command-success, persistence or DB-commit acknowledgement.
    backend.subscribe('gateways/+/telemetry/#')
    test.assertFalse(manager.request('getClient', username=username)['client'].get('disabled', False),
                     'backend gained DynSec management access')


class ManagementOrderingTests(unittest.TestCase):
    def test_same_connection_barrier_precedes_independent_readback(self):
        events = []
        backend = mock.Mock()
        manager = mock.Mock()
        backend.publish.side_effect = lambda *a: events.append('publish')
        backend.subscribe.side_effect = lambda *a: events.append('backend SUBACK')
        manager.request.side_effect = lambda *a, **kw: (events.append('manager readback') or
                                                       {'client': {'disabled': False}})
        assert_management_denied(self, backend, manager, 'ordering_witness')
        self.assertEqual(events, ['publish', 'backend SUBACK', 'manager readback'])
        backend.subscribe.assert_called_once_with('gateways/+/telemetry/#')

    def test_old_oracle_gap_and_permissive_negative_control(self):
        # Deterministic synthetic broker scheduling: send queues the mutation;
        # processing the next packet on this connection applies it first.
        state = {'disabled': False, 'queued': False}
        backend = mock.Mock()
        manager = mock.Mock()
        backend.publish.side_effect = lambda *a: state.update(queued=True)
        backend.subscribe.side_effect = lambda *a: state.update(disabled=state['queued'])
        manager.request.side_effect = lambda *a, **kw: {'client': {'disabled': state['disabled']}}
        backend.publish('$CONTROL/dynamic-security/v1', b'synthetic command')
        self.assertFalse(manager.request('getClient')['client']['disabled'],
                         'legacy unsynchronized oracle false-passes queued mutation')
        with self.assertRaisesRegex(AssertionError, 'backend gained DynSec management access'):
            assert_management_denied(self, backend, manager, 'ordering_witness')
        self.assertTrue(state['disabled'], 'negative control must really change witness state')

    def test_barrier_failure_prevents_readback(self):
        backend = mock.Mock()
        manager = mock.Mock()
        manager.request.return_value = {'client': {'disabled': False}}
        backend.subscribe.side_effect = TimeoutError('bounded barrier failure')
        with self.assertRaises(TimeoutError):
            assert_management_denied(self, backend, manager, 'ordering_witness')
        manager.request.assert_not_called()


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.m = load()
        # /tmp/opencode is group-writable; strict ancestry guard must reject it.
        self.tmp = tempfile.TemporaryDirectory(prefix='task284_')
        self.root = Path(self.tmp.name)
        self.store = self.root / 'store.json'
        self.config = dict(store=str(self.store), manager='fixture_manager',
                           backend='fixture_backend', manager_password=secrets.token_urlsafe(32),
                           backend_password=secrets.token_urlsafe(32), mapping='rootful',
                           broker_uid=1883, broker_gid=1883)

    def tearDown(self):
        self.tmp.cleanup()

    def test_credentials_and_strict_input(self):
        self.m.parse_config(io.StringIO(json.dumps(self.config)))
        for key, value in [('manager', 'fixture_backend'), ('manager', '../x'),
                           ('backend_password', ''), ('manager_password', 'a\nb'),
                           ('backend_password', self.config['manager_password']),
                           ('mapping', 'guess'), ('broker_uid', 0), ('extra', True)]:
            with self.subTest(key=key):
                cfg = dict(self.config, **{key: value})
                with self.assertRaises(self.m.BootstrapError):
                    self.m.parse_config(io.StringIO(json.dumps(cfg)))
        for text in ['{', '[]', '{"store":"x","store":"y"}', '{}']:
            with self.assertRaises(self.m.BootstrapError):
                self.m.parse_config(io.StringIO(text))

    def test_rootless_requires_explicit_mapping(self):
        cfg = dict(self.config, mapping='rootless', broker_uid=os.getuid(), broker_gid=os.getgid())
        with self.assertRaises(self.m.BootstrapError):
            self.m.parse_config(io.StringIO(json.dumps(cfg)))
        cfg.update(uid_map=[[1883, os.getuid(), 1]], gid_map=[[1883, os.getgid(), 1]])
        self.m.parse_config(io.StringIO(json.dumps(cfg)))

    def test_native_init_password_stdin_not_argv_and_no_output(self):
        runner = mock.Mock(return_value=mock.Mock(returncode=0))
        self.m.native_init('/trusted/mosquitto_ctrl', self.store, 'fixture_manager',
                           self.config['manager_password'], runner=runner)
        args, kwargs = runner.call_args
        self.assertTrue(args[0] == ['/trusted/mosquitto_ctrl', 'dynsec', 'init',
                                   str(self.store), 'fixture_manager'], 'unexpected native argv')
        self.assertTrue(kwargs['input'] == (self.config['manager_password'] + '\n').encode() * 2,
                        'unexpected stdin framing')
        self.assertEqual(kwargs['stdout'], self.m.subprocess.DEVNULL)
        self.assertEqual(kwargs['stderr'], self.m.subprocess.DEVNULL)

    def test_acl_exact_default_deny_no_bootstrap_admin(self):
        clients = [{'username': name, 'password': 'opaque-native-hash', 'salt': 'native-salt',
                    'iterations': 101, 'roles': [{'rolename': 'admin'}]}
                   for name in ('fixture_manager', 'fixture_backend')]
        store = self.m.compose_store(*[{'clients': [c]} for c in clients])
        self.assertEqual(len(store['clients']), 2)
        self.assertTrue(all(not v for v in store['defaultACLAccess'].values()))
        roles = {r['rolename']: r['acls'] for r in store['roles']}
        self.assertEqual(set(roles), {'bootstrap_manager', 'bootstrap_backend'})
        self.assertEqual({a['topic'] for a in roles['bootstrap_manager']},
                         {'$CONTROL/dynamic-security/v1', '$CONTROL/dynamic-security/v1/response'})
        self.assertEqual({a['topic'] for a in roles['bootstrap_backend']},
                         {'gateways/+/telemetry/#', 'gateways/+/acks/#'})
        self.assertNotIn('admin', json.dumps(store))
        self.assertEqual(store['clients'][0]['password'], clients[0]['password'])

    def test_guard_rejects_existing_unsafe_and_symlink_paths(self):
        for kind in ('existing', 'permission', 'symlink', 'ancestor', 'nonempty'):
            with self.subTest(kind=kind):
                d = self.root / kind
                d.mkdir(mode=0o700)
                dst = d / 'store.json'
                if kind == 'existing':
                    dst.write_text('do not overwrite')
                elif kind == 'permission':
                    d.chmod(0o755)
                elif kind == 'symlink':
                    dst.symlink_to(self.store)
                elif kind == 'ancestor':
                    alias = self.root / 'alias'
                    alias.symlink_to(d)
                    dst = alias / 'store.json'
                else:
                    (d / 'unrelated').write_text('keep')
                with self.assertRaises(self.m.BootstrapError):
                    self.m.guard_directory(dst, os.getuid(), os.getgid())

    def test_atomic_publish_no_overwrite_and_failure_cleanup(self):
        fd = self.m.guard_directory(self.store, os.getuid(), os.getgid())
        try:
            self.m.publish(fd, 'store.json', b'{"complete":true}', os.getuid(), os.getgid())
            self.assertEqual(self.store.stat().st_mode & 0o777, 0o600)
            before = self.store.read_bytes()
            with self.assertRaises(self.m.BootstrapError):
                self.m.publish(fd, 'store.json', b'overwrite', os.getuid(), os.getgid())
            self.assertEqual(self.store.read_bytes(), before)
        finally:
            os.close(fd)
        self.assertEqual([p.name for p in self.root.iterdir()], ['store.json'])

    def test_cannot_establish_owner_fails_without_chmod_fallback(self):
        fd = self.m.guard_directory(self.store, os.getuid(), os.getgid())
        try:
            with mock.patch.object(self.m.os, 'fchown', side_effect=PermissionError):
                with self.assertRaises(self.m.BootstrapError):
                    self.m.publish(fd, 'store.json', b'{}', 1883, 1883)
        finally:
            os.close(fd)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_real_bootstrap_rejects_wrong_owner_before_native_mutation(self):
        with mock.patch.object(self.m, 'native_init') as init:
            with self.assertRaises(self.m.BootstrapError):
                self.m.bootstrap(self.config, '/usr/bin/mosquitto_ctrl')
            init.assert_not_called()

    def test_native_error_sanitized(self):
        runner = mock.Mock(side_effect=self.m.subprocess.TimeoutExpired('native', 15))
        with self.assertRaisesRegex(self.m.BootstrapError, 'timed out'):
            self.m.native_init('native', self.store, 'manager', self.config['manager_password'], runner=runner)

    def test_native_bad_schema_and_identity_rejected(self):
        for source in ('[]', '{}', '{"clients":[]}', '{"clients":[null]}',
                       '{"clients":[{"username":"wrong"}]}',
                       '{"clients":[{"username":"fixture_manager"}]}'):
            def bad_init(tool, path, user, password):
                path.write_text(source)
            with self.subTest(source=source):
                with self.assertRaises(self.m.BootstrapError):
                    self.m.build_store(self.config, 'unused', initializer=bad_init)

    def test_native_return_code_failure(self):
        with self.assertRaises(self.m.BootstrapError):
            self.m.native_init('native', self.store, 'manager', self.config['manager_password'],
                               runner=mock.Mock(return_value=mock.Mock(returncode=1)))

    def test_config_size_bool_mapping_and_traversal(self):
        with self.assertRaises(self.m.BootstrapError):
            self.m.parse_config(io.StringIO(' ' * 16385))
        for rows in (None, [], [[0, 0, True]], [[1883, 0, 1]], [[0, 1000, 2000], [1, 3000, 2000]]):
            with self.assertRaises(self.m.BootstrapError):
                self.m.mapped_id(rows)
        with self.assertRaises(self.m.BootstrapError):
            self.m.guard_directory(self.root / '..' / 'store.json', os.getuid(), os.getgid())

    def test_bootstrap_orchestration_and_wrong_version(self):
        cfg = dict(self.config, mapping='rootless', broker_uid=os.getuid(), broker_gid=os.getgid(),
                   uid_map=[[1883, os.getuid(), 1]], gid_map=[[1883, os.getgid(), 1]])
        # Trusted installed binary path; version/build mocked in this unit lane.
        with mock.patch.object(self.m.subprocess, 'run', return_value=mock.Mock(stdout=b'wrong')):
            with self.assertRaises(self.m.BootstrapError):
                self.m.bootstrap(cfg, '/usr/bin/true')
        with mock.patch.object(self.m.subprocess, 'run', return_value=mock.Mock(
                stdout=b'mosquitto_ctrl version 2.0.18 running on libmosquitto 2.0.18.')):
            with mock.patch.object(self.m, 'build_store', return_value=b'{}'):
                self.m.bootstrap(cfg, '/usr/bin/true')
        self.assertEqual(self.store.read_bytes(), b'{}')


# No skipped tests presented as native proof: define the opt-in live lane only
# when requested. It uses the existing digest-pinned disposable Spike fixture.
if os.environ.get('TASK284_NATIVE') == '1':
    class NativeBootstrapTests(unittest.TestCase):
        def test_native_hashes_tls_roles_and_cleanup(self):
            import subprocess
            from task26_dynsec_cases import Spike
            from task26_harness_helpers import CONTROL, RESPONSE
            from stage2_mosquitto_runtime import IMAGE, ENV, mqtt_connection
            m = load()
            with tempfile.TemporaryDirectory(dir='/tmp/opencode', prefix='task284-native-') as d:
                spike = Spike(Path(d))
                cfg = dict(manager='manager', backend='backend',
                           manager_password=spike.passwords['manager'],
                           backend_password=spike.passwords['backend'])

                def initialize(tool, destination, username, password):
                    # Protected bind contains generated test credentials/hashes
                    # only. Nothing is emitted to stdout, stderr or argv.
                    command = ['docker', 'run', '--rm', '-i', '--network', 'none',
                               '--user', f'{os.getuid()}:{os.getgid()}',
                               '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                               '--mount', f'type=bind,src={destination.parent},dst=/native',
                               '--entrypoint', 'mosquitto_ctrl', IMAGE,
                               'dynsec', 'init', '/native/' + destination.name, username]
                    result = subprocess.run(command, input=((password + '\n') * 2).encode(),
                                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                            timeout=30, env=ENV)
                    self.assertEqual(result.returncode, 0, 'native init failed')
                    destination.chmod(0o600)

                store = m.build_store(cfg, 'fixture-native', initializer=initialize)
                self.assertFalse(any(p.encode() in store for p in cfg.values() if len(p) > 30))
                setup = spike.setup

                def fresh_setup(script, data=None):
                    if 'mosquitto_ctrl dynsec init' in script:
                        return setup('umask 077; mkdir -p /security; chmod 700 /security; '
                                     'test ! -e /security/dynsec.json; cat > /security/dynsec.json; '
                                     'chmod 600 /security/dynsec.json; '
                                     'chown -R 1883:1883 /security /fixture', store)
                    return setup(script, data)

                spike.setup = fresh_setup
                try:
                    spike.prepare()
                    # start() normally probes the fixture's bootstrap admin;
                    # here the store has only our two fresh principals.
                    original_probe = spike.probe
                    spike.probe = lambda user, *a, **kw: original_probe('manager' if user == 'admin' else user, *a, **kw)
                    spike.start()
                    spike.probe = original_probe
                    self.assertEqual(spike.setup('stat -c "%u:%g:%a" /security/dynsec.json').strip(), b'1883:1883:600')
                    self.assertEqual(spike.setup('stat -c "%u:%g:%a" /security').strip(), b'1883:1883:700')
                    spike.probe('manager')
                    spike.probe('backend')
                    spike.probe('admin', accepted=False)
                    spike.probe('backend', spike.passwords['new'], accepted=False)
                    anonymous = mqtt_connection(spike.port, Path(d) / 'ca.crt', '', '', False)
                    anonymous.close()
                    manager = spike.connect('manager')
                    manager.subscribe(RESPONSE)
                    self.assertEqual(len(manager.request('listClients')['clients']), 2)
                    manager.subscribe('gateways/+/telemetry/#', allowed=False)
                    backend = spike.connect('backend')
                    backend.subscribe('gateways/+/telemetry/#')
                    backend.subscribe(RESPONSE, allowed=False)
                    backend.subscribe('#', allowed=False)
                    backend.subscribe('gateways/+/responses/#', allowed=False)
                    backend.subscribe('gateways/+/status', allowed=False)
                    backend.subscribe('gateways/+/commands/#', allowed=False)
                    backend.subscribe('unrelated/#', allowed=False)
                    spike.delivery(manager, backend, 'gateways/fixture_A/telemetry/sample', False)
                    # Disposable witness has NO password and cannot authenticate.
                    # Test injection never makes bootstrap create Gateway clients.
                    manager.request('createRole', rolename='witness')
                    for kind, topic in [('publishClientSend', 'gateways/+/telemetry/#'),
                                        ('publishClientReceive', 'gateways/+/acks/#'),
                                        ('subscribeLiteral', 'gateways/+/acks/#'),
                                        ('publishClientReceive', 'unrelated/#'),
                                        ('subscribeLiteral', 'unrelated/#')]:
                        manager.request('addRoleACL', rolename='witness', acltype=kind,
                                        topic=topic, allow=True)
                    # Native role changes disconnect the changed principal.
                    with self.assertRaises((EOFError, ConnectionResetError)):
                        manager.request('addClientRole', username='manager', rolename='witness')
                    manager = spike.connect('manager')
                    manager.subscribe(RESPONSE)
                    manager.subscribe('gateways/+/acks/#')
                    manager.subscribe('unrelated/#')
                    for gateway in ('fixture_A', 'fixture_B'):
                        spike.delivery(manager, backend, f'gateways/{gateway}/telemetry/sample', True)
                        spike.delivery(backend, manager, f'gateways/{gateway}/acks/message', True)
                    spike.delivery(backend, backend, 'gateways/fixture_A/telemetry/sample', False)
                    spike.delivery(backend, manager, 'unrelated/test', False)
                    assert_management_denied(self, backend, manager, 'manager')
                    # Oracle sensitivity, never a production ACL change: grant
                    # only control publish temporarily in this owned fixture.
                    # The victim is uncredentialed/disposable, NOT the manager.
                    manager.request('createClient', username='ordering_witness')
                    assert_management_denied(self, backend, manager, 'ordering_witness')
                    manager.request('createRole', rolename='ordering_control_publish')
                    manager.request('addRoleACL', rolename='ordering_control_publish',
                                    acltype='publishClientSend', topic=CONTROL, allow=True)
                    manager.request('addClientRole', username='backend', rolename='ordering_control_publish')
                    backend.close()  # role change drains old backend session
                    backend = spike.connect('backend')
                    backend.subscribe(RESPONSE, allowed=False)
                    backend.subscribe('#', allowed=False)
                    with self.assertRaisesRegex(AssertionError, 'backend gained DynSec management access'):
                        assert_management_denied(self, backend, manager, 'ordering_witness')
                    self.assertTrue(manager.request('getClient', username='ordering_witness')['client']['disabled'],
                                    'permissive native negative control did not mutate witness')
                    print('PASS native negative control: synchronized denial assertion rejected changed witness', flush=True)
                    manager.request('removeClientRole', username='backend', rolename='ordering_control_publish')
                    backend.close()
                    backend = spike.connect('backend')
                    manager.request('deleteRole', rolename='ordering_control_publish')
                    manager.request('enableClient', username='ordering_witness')
                    assert_management_denied(self, backend, manager, 'ordering_witness')
                    backend.subscribe(RESPONSE, allowed=False)
                    backend.subscribe('unrelated/#', allowed=False)
                    manager.request('deleteClient', username='ordering_witness')
                    print('PASS native restrictive ACL: synchronized management publish denied; witness/role removed', flush=True)
                    with self.assertRaises((EOFError, ConnectionResetError)):
                        manager.request('removeClientRole', username='manager', rolename='witness')
                    manager = spike.connect('manager')
                    manager.subscribe(RESPONSE)
                    manager.request('deleteRole', rolename='witness')
                    # Required controller lifecycle mutation/readback rights.
                    manager.request('createClient', username='uncredentialed_witness')
                    manager.request('getRole', rolename='bootstrap_backend')
                    manager.request('disableClient', username='uncredentialed_witness')
                    self.assertTrue(manager.request('getClient', username='uncredentialed_witness')['client']['disabled'])
                    manager.request('deleteClient', username='uncredentialed_witness')
                    self.assertEqual(len(manager.request('listClients')['clients']), 2)
                    spike.probe = lambda user, *a, **kw: original_probe('manager' if user == 'admin' else user, *a, **kw)
                    spike.restart()
                    spike.probe = original_probe
                    spike.probe('backend')
                    spike.probe('admin', accepted=False)
                    manager = spike.connect('manager')
                    manager.subscribe(RESPONSE)
                    self.assertEqual(len(manager.request('listClients')['clients']), 2)
                    logs = spike.command('logs', '--no-color', 'broker', capture=True)
                    self.assertFalse(any(p.encode() in logs for p in spike.passwords.values()))
                    self.assertNotIn(b'PRIVATE KEY', logs)
                    self.assertNotIn(b'"password"', logs)
                finally:
                    try:
                        # Only the unique fixture broker bind was chowned by
                        # Spike.prepare; restore its test owner before removal.
                        if (Path(d) / 'broker').exists():
                            spike.setup(f'chown -R {os.getuid()}:{os.getgid()} /fixture')
                    finally:
                        spike.cleanup()
            self.assertFalse(Path(d).exists(), 'generated fixture files remain')


if __name__ == '__main__':
    unittest.main()
