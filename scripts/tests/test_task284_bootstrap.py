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
import base64

SCRIPT = Path(__file__).parents[1] / 'mqtt-bootstrap.py'


def load():
    spec = importlib.util.spec_from_file_location('mqtt_bootstrap', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ParentRecoveryContract(unittest.TestCase):
    def test_every_helper_masks_image_volumes_before_intent_and_create(self):
        module = load()
        for kind, user in [('native-init', '1000:1000'), ('ownership', '0:0'),
                           ('writer', '1883:1883'), ('reader', '1883:1883')]:
            tracker = []
            def runner(cmd, **kwargs):
                expected = {p: 'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=' + user.split(':')[0] + ',gid=' + user.split(':')[1]
                            for p in ('/mosquitto/data', '/mosquitto/log')}
                self.assertEqual(tracker[0]['tmpfs'], expected)
                self.assertEqual(dict(cmd[i + 1].split(':', 1) for i, v in enumerate(cmd) if v == '--tmpfs'), expected)
                self.assertEqual(json.loads(Path(tracker[0]['receipt']).read_text())['tmpfs'], expected)
                return mock.Mock(returncode=1)
            with self.assertRaises(module.BootstrapError):
                module.allocate_helper(['docker', 'create', '--name', 'mqtt-boot-test', '--user', user,
                    '--mount', 'type=volume,src=fixture,dst=/store', '--entrypoint', '/bin/sh',
                    'sha256:' + module.PINNED_DIGEST], tracker, kind, runner=runner)

    def test_image_declared_anonymous_volume_counterexample_is_refused(self):
        module = load()
        item = {'name': 'mqtt-boot-test', 'nonce': 'expected', 'image': 'sha256:' + module.PINNED_DIGEST,
                'user': '1883:1883', 'mounts': [], 'tmpfs': {'/mosquitto/data': 'mask', '/mosquitto/log': 'mask'}}
        rec = {'Id': 'a' * 64, 'Name': '/' + item['name'], 'Image': item['image'],
               'Config': {'Labels': {'bootstrap.nonce': item['nonce']}, 'User': item['user']},
               'HostConfig': {'Mounts': [], 'Tmpfs': item['tmpfs']},
               'Mounts': [{'Type': 'volume', 'Name': 'unowned', 'Destination': '/mosquitto/data'}]}
        with self.assertRaises(module.BootstrapError):
            module.resolve_helper(item, runner=lambda *a, **k: mock.Mock(returncode=0, stdout=json.dumps([rec]).encode()))

    def test_intent_is_private_and_inside_supplied_recovery_directory(self):
        module = load()
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            module._recovery_parent = directory
            item = {'kind': 'volume', 'name': 'isolated-test', 'labels': {}}
            path = Path(module.save_intent(item))
            self.assertEqual(path.parent.parent, Path(directory))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
            self.assertEqual(json.loads(path.read_text()), item)
            module.discard_intent({'receipt': str(path)})


def with_allocation_receipts(runner):
    """Model Docker ownership metadata in the existing lifecycle fixtures."""
    receipts = {}
    removed_volumes = set()

    def wrapped(cmd, **kwargs):
        if cmd[:3] == ['docker', 'volume', 'inspect'] and cmd[-1] in removed_volumes:
            return mock.Mock(returncode=1, stdout=b'[]', stderr=b'no such volume')
        result = runner(cmd, **kwargs)
        if cmd[:2] == ['docker', 'create'] and result.returncode == 0:
            cid = result.stdout.decode().strip()
            mount = cmd[cmd.index('--mount') + 1]
            fields = dict(p.split('=', 1) for p in mount.split(',') if '=' in p)
            labels = dict(cmd[i + 1].split('=', 1) for i, v in enumerate(cmd) if v == '--label')
            img = cmd[cmd.index('--entrypoint') + 2]
            rec = {'Id': cid, 'Name': '/' + cmd[cmd.index('--name') + 1],
                   'Image': img,
                   'Config': {'User': cmd[cmd.index('--user') + 1], 'Labels': labels},
                   'Mounts': [{'Type': fields['type'], 'Name': fields['src'], 'Source': fields['src'],
                               'Destination': fields['dst'], 'RW': 'readonly' not in mount.split(',')}],
                   'HostConfig': {'Tmpfs': dict(cmd[i + 1].split(':', 1) for i, v in enumerate(cmd) if v == '--tmpfs'),
                       'Mounts': [{'Type': fields['type'], 'Source': fields['src'],
                       'Target': fields['dst'], 'ReadOnly': 'readonly' in mount.split(',')}]}}
            if img.lower() == ('sha256:' + load().PINNED_DIGEST).lower():
                rec['ImageManifestDescriptor'] = {
                    'digest': load().PINNED_PLATFORM_MANIFEST_DIGEST,
                    'mediaType': 'application/vnd.oci.image.manifest.v1+json'
                }
            receipts[cid] = rec
            receipts[rec['Name'][1:]] = rec
        if cmd[:3] == ['docker', 'container', 'inspect'] and result.returncode == 0:
            rec = receipts.get(cmd[-1])
            if rec:
                original = json.loads(result.stdout)[0]
                result.stdout = json.dumps([{**original, **rec}]).encode()
        if cmd[:2] == ['docker', 'rm'] and result.returncode == 0:
            rec = receipts.get(cmd[-1])
            if rec:
                receipts.pop(rec['Name'][1:], None)
                receipts.pop(rec['Id'], None)
        if cmd[:3] == ['docker', 'container', 'inspect'] and cmd[-1].startswith('mqtt-boot-') and cmd[-1] not in receipts:
            return mock.Mock(returncode=1, stdout=b'[]', stderr=b'no such container')
        if cmd[:3] == ['docker', 'volume', 'rm'] and result.returncode == 0:
            removed_volumes.add(cmd[-1])
        return result
    return wrapped


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


class CancellationOwnershipBoundaryTests(unittest.TestCase):
    """Release blockers: ambiguous Docker allocation must retain ownership receipts."""

    def test_lost_reply_recovered_by_nonce_and_exact_id(self):
        module = load()
        tracker = []
        deleted = []

        def lost(cmd, **kwargs):
            raise module.subprocess.TimeoutExpired(cmd, 10)

        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            with self.assertRaises(module.subprocess.TimeoutExpired):
                module.container_native_init('sha256:' + module.PINNED_DIGEST,
                    Path(directory) / 'manager.json', 'manager', 'unused input', tracker, runner=lost)
            item = tracker[0]
            receipt_path = Path(item['receipt'])
            self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('unused input', receipt_path.read_text())
            cid = 'a' * 64

            def recover(cmd, **kwargs):
                if cmd[1] == 'rm':
                    deleted.append(cmd[-1])
                    return mock.Mock(returncode=0)
                if deleted:
                    return mock.Mock(returncode=1, stderr=b'no such container')
                rec = {'Id': cid, 'Name': '/' + item['name'], 'Image': item['image'],
                       'Config': {'Labels': {'bootstrap.nonce': item['nonce']}, 'User': item['user']},
                       'ImageManifestDescriptor': {
                           'digest': module.PINNED_PLATFORM_MANIFEST_DIGEST,
                           'mediaType': 'application/vnd.oci.image.manifest.v1+json'
                       },
                       'Mounts': [{'Type': m['Type'], 'Name': m['Source'], 'Source': m['Source'], 'Destination': m['Target'], 'RW': not m['ReadOnly']} for m in item['mounts']],
                       'HostConfig': {'Mounts': item['mounts'], 'Tmpfs': item['tmpfs']}}
                return mock.Mock(returncode=0, stdout=json.dumps([rec]).encode())

            self.assertEqual(module.cleanup_containers(tracker, runner=recover), [])
            self.assertEqual(deleted, [cid])
            self.assertFalse(receipt_path.exists())

    def test_foreign_receipt_and_daemon_error_never_authorize_delete(self):
        module = load()
        for foreign in (True, False):
            tracker = [{'name': 'mqtt-boot-test', 'nonce': 'expected',
                        'image': 'sha256:' + module.PINNED_DIGEST,
                        'user': '1883:1883', 'mounts': []}]
            calls = []

            def runner(cmd, **kwargs):
                calls.append(cmd)
                if not foreign:
                    return mock.Mock(returncode=1, stderr=b'daemon unavailable')
                rec = {'Id': 'a' * 64, 'Name': '/mqtt-boot-test', 'Image': tracker[0]['image'],
                       'Config': {'Labels': {'bootstrap.nonce': 'foreign'}, 'User': '1883:1883'},
                       'HostConfig': {'Mounts': []}}
                return mock.Mock(returncode=0, stdout=json.dumps([rec]).encode())

            self.assertEqual(module.cleanup_containers(tracker, runner=runner), ['mqtt-boot-test'])
            self.assertFalse(any(cmd[1] == 'rm' for cmd in calls))
            self.assertEqual(len(tracker), 1)

    def test_remove_reply_without_authoritative_absence_retains_intent(self):
        module = load()
        tracker = [{'id': 'a' * 64, 'name': 'mqtt-boot-test', 'nonce': 'expected',
                    'image': 'sha256:' + module.PINNED_DIGEST,
                    'user': '1883:1883', 'mounts': []}]
        removed = []
        def runner(cmd, **kwargs):
            if cmd[1] == 'rm':
                removed.append(cmd[-1])
                return mock.Mock(returncode=0)
            if removed:
                return mock.Mock(returncode=1, stderr=b'daemon unavailable')
            item = tracker[0]
            rec = {'Id': item['id'], 'Name': '/' + item['name'], 'Image': item['image'],
                   'Config': {'Labels': {'bootstrap.nonce': item['nonce']}, 'User': item['user']},
                   'ImageManifestDescriptor': {
                       'digest': module.PINNED_PLATFORM_MANIFEST_DIGEST,
                       'mediaType': 'application/vnd.oci.image.manifest.v1+json'
                   },
                   'HostConfig': {'Mounts': []}}
            return mock.Mock(returncode=0, stdout=json.dumps([rec]).encode())
        self.assertEqual(module.cleanup_containers(tracker, runner=runner), ['a' * 64])
        self.assertEqual(len(tracker), 1)

    def test_image_preflight_does_not_allocate_untracked_container(self):
        module = load()
        calls = []
        def runner(cmd, **kwargs):
            calls.append(cmd)
            rec = {'Architecture': 'amd64', 'Os': 'linux',
                   'RepoDigests': [module.PINNED_IMAGE], 'Id': module.PINNED_CONFIG_ID}
            return mock.Mock(returncode=0, stdout=json.dumps([rec]).encode())
        module.verify_image(module.PINNED_IMAGE, runner=runner)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:3], ['docker', 'image', 'inspect'])

    def test_signal_during_allocation_waits_for_receipt_boundary(self):
        module = load()
        tracker = []
        calls = []
        def runner(cmd, **kwargs):
            calls.append(cmd)
            if cmd[1] == 'create':
                module.request_cancellation(module.signal.SIGTERM, None)
                return mock.Mock(returncode=0, stdout=('a' * 64).encode())
            item = tracker[0]
            rec = {'Id': 'a' * 64, 'Name': '/' + item['name'], 'Image': item['image'],
                   'Config': {'Labels': {'bootstrap.nonce': item['nonce']}, 'User': item['user']},
                   'ImageManifestDescriptor': {
                       'digest': module.PINNED_PLATFORM_MANIFEST_DIGEST,
                       'mediaType': 'application/vnd.oci.image.manifest.v1+json'
                   },
                    'Mounts': [{'Type': m['Type'], 'Name': m['Source'], 'Source': m['Source'], 'Destination': m['Target'], 'RW': not m['ReadOnly']} for m in item['mounts']],
                    'HostConfig': {'Mounts': item['mounts'], 'Tmpfs': item['tmpfs']}}
            return mock.Mock(returncode=0, stdout=json.dumps([rec]).encode())
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            with self.assertRaisesRegex(module.BootstrapError, 'interrupted'):
                module.container_native_init('sha256:' + module.PINNED_DIGEST,
                    Path(directory) / 'manager.json', 'manager', 'unused', tracker, runner=runner)
        self.assertEqual(tracker[0]['id'], 'a' * 64)
        self.assertEqual([cmd[1] for cmd in calls], ['create', 'container'])

    def test_helper_create_timeout_retains_recovery_receipt(self):
        module = load()
        tracker = []

        def runner(cmd, **kwargs):
            # The daemon can allocate the helper before the CLI loses its reply.
            raise module.subprocess.TimeoutExpired(cmd, kwargs['timeout'])

        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            with self.assertRaises(module.subprocess.TimeoutExpired):
                module.container_native_init(
                    'sha256:' + module.PINNED_DIGEST,
                    Path(directory) / 'manager.json', 'manager', 'unused test input',
                    tracker, runner=runner)
        self.assertTrue(tracker, 'Allocation intent must survive a lost create reply')

    def test_helpers_have_nonce_label_before_allocation(self):
        module = load()
        commands = []

        def runner(cmd, **kwargs):
            commands.append(cmd)
            return mock.Mock(returncode=1, stdout=b'', stderr=b'')

        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            with self.assertRaises(module.BootstrapError):
                module.container_native_init(
                    'sha256:' + module.PINNED_DIGEST,
                    Path(directory) / 'manager.json', 'manager', 'unused test input',
                    [], runner=runner)
        create = commands[0]
        labels = [create[index + 1] for index, value in enumerate(create)
                  if value == '--label']
        self.assertTrue(any(value.startswith('bootstrap.nonce=') for value in labels),
                        'A random helper name alone is not an ownership receipt')

    def test_volume_verification_failure_enters_cleanup_boundary(self):
        module = load()
        calls = []
        cfg = {'volume_name': 'cancellation_boundary_test'}

        def runner(cmd, **kwargs):
            calls.append(cmd)
            return mock.Mock(returncode=0, stdout=cfg['volume_name'].encode(), stderr=b'')

        with mock.patch.object(module, 'verify_image', return_value='sha256:' + module.PINNED_DIGEST), \
                mock.patch.object(module, 'check_volume_absent'), \
                mock.patch.object(module, 'verify_volume_receipt',
                                  side_effect=module.BootstrapError('receipt unavailable')) as receipt:
            with self.assertRaises(module.BootstrapError):
                module.bootstrap_container(cfg, runner=runner)
        self.assertGreaterEqual(receipt.call_count, 2,
                                'Failed verification after allocation must enter recovery')


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
        self.container_config = dict(manager='fixture_manager',
                                     backend='fixture_backend',
                                     manager_password=secrets.token_urlsafe(32),
                                     backend_password=secrets.token_urlsafe(32),
                                     volume_name='mosquitto_dynsec_test_vol',
                                     labels={'app': 'test'})

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

    def test_container_mode_config_validation(self):
        # Valid config
        cfg = self.m.parse_config(io.StringIO(json.dumps(self.container_config)), container_mode=True)
        self.assertEqual(cfg['volume_name'], 'mosquitto_dynsec_test_vol')

        # Invalid field combinations
        for key, value in [('manager', 'fixture_backend'),
                           ('manager_password', 'short'),
                           ('backend_password', self.container_config['manager_password']),
                           ('volume_name', '../bad_name'),
                           ('labels', 'not_a_dict'),
                           ('unexpected_field', True)]:
            with self.subTest(key=key):
                bad = dict(self.container_config, **{key: value})
                with self.assertRaises(self.m.BootstrapError):
                    self.m.parse_config(io.StringIO(json.dumps(bad)), container_mode=True)

    def test_verify_image_checks_pin_os_arch_and_help(self):
        def fake_inspect(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                data = [{
                    'Architecture': 'amd64',
                    'Os': 'linux',
                    'RepoDigests': [self.m.PINNED_IMAGE],
                    'Id': self.m.PINNED_CONFIG_ID
                }]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif cmd[:3] == ['docker', 'run', '--rm']:
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        self.m.verify_image(self.m.PINNED_IMAGE, runner=fake_inspect)

        # Failure: Wrong architecture
        def bad_arch(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                data = [{'Architecture': 'arm64', 'Os': 'linux', 'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(self.m.BootstrapError, 'linux/amd64 required'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=bad_arch)

        # Failure: Wrong OS
        def bad_os(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                data = [{'Architecture': 'amd64', 'Os': 'windows', 'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(self.m.BootstrapError, 'linux/amd64 required'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=bad_os)

        # Failure: Missing / wrong RepoDigests with correct config ID
        def bad_digest(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': ['eclipse-mosquitto@sha256:000000'],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(self.m.BootstrapError, 'image RepoDigests exact pin mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=bad_digest)

        # Failure: RepoDigests is not a list (dict, str, null, int)
        for bad_rd in ({self.m.PINNED_IMAGE: 'value'}, self.m.PINNED_IMAGE, None, 12345):
            with self.subTest(repodigests=bad_rd):
                def non_list_rd(cmd, **kwargs):
                    if cmd[:3] == ['docker', 'image', 'inspect']:
                        data = [{'Architecture': 'amd64', 'Os': 'linux',
                                 'RepoDigests': bad_rd,
                                 'Id': self.m.PINNED_CONFIG_ID}]
                        return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
                    return mock.Mock(returncode=0)
                with self.assertRaisesRegex(self.m.BootstrapError, 'image RepoDigests exact pin mismatch'):
                    self.m.verify_image(self.m.PINNED_IMAGE, runner=non_list_rd)

        # Failure: Missing / malformed inspect metadata
        def empty_inspect(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                return mock.Mock(returncode=0, stdout=b'[]', stderr=b'')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(self.m.BootstrapError, 'invalid image inspect metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=empty_inspect)

        def malformed_id_inspect(cmd, **kwargs):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                data = [{'Architecture': 'amd64', 'Os': 'linux', 'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': 'not-a-valid-sha256'}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(self.m.BootstrapError, 'image Id immutable digest mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=malformed_id_inspect)

    def test_container_bootstrap_partial_failure_teardown(self):
        created_vol = []
        rm_vol = []
        vol_created = False
        saved_labels = {}

        def mock_runner(cmd, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels
            if c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run' and '--entrypoint' in cmd:
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                sub = cmd[2]
                if sub == 'inspect':
                    if vol_created:
                        rec = [{'Name': self.container_config['volume_name'],
                                'Labels': saved_labels}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'', stderr=b'no such volume')
                elif sub == 'create':
                    vol_created = True
                    created_vol.append(cmd[-1])
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=cmd[-1].encode(), stderr=b'')
                elif sub == 'rm':
                    rm_vol.append(cmd[-1])
                    return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'create':
                # Fail at root helper container creation
                return mock.Mock(returncode=1, stdout=b'', stderr=b'cannot create container')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            with self.assertRaises(self.m.BootstrapError):
                self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))

        # Verify volume was torn down upon failure
        self.assertEqual(created_vol, [self.container_config['volume_name']])
        self.assertEqual(rm_vol, [self.container_config['volume_name']])

    def test_container_bootstrap_no_overwrite_existing_file(self):
        # If writer encounters existing file, it exits with error and does not overwrite
        vol_created = False
        saved_labels = {}
        started_count = 0
        rm_cids = set()

        created_cids = []
        def mock_runner(cmd, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels, started_count, rm_cids, created_cids
            if c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run' and '--entrypoint' in cmd:
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                if cmd[2] == 'inspect':
                    if vol_created:
                        rec = [{'Name': self.container_config['volume_name'],
                                'Labels': saved_labels}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'', stderr=b'no such volume')
                elif cmd[2] == 'create':
                    vol_created = True
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'create':
                cid = f'{len(created_cids):064x}'
                created_cids.append(cid)
                return mock.Mock(returncode=0, stdout=cid.encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'container' and cmd[2] == 'inspect':
                cid = cmd[3]
                if cid in rm_cids:
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: No such container')
                code = 0 if started_count <= 1 else 2
                return mock.Mock(returncode=0, stdout=f'[{{"State": {{"ExitCode": {code}}}}}]'.encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'rm':
                cid = cmd[-1]
                rm_cids.add(cid)
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'start':
                started_count += 1
                if started_count == 1:
                    # Root helper
                    return mock.Mock(returncode=0, stdout=b'', stderr=b'')
                # Simulate writer returning code 2 (exit 2 when file already exists)
                return mock.Mock(returncode=2, stdout=b'', stderr=b'file exists')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            with self.assertRaisesRegex(self.m.BootstrapError, 'no overwrite'):
                self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))

    def test_container_bootstrap_mocked_lifecycle_success_and_cleanup(self):
        created_cmds = []
        started_cmds = []
        rm_cmds = []
        rm_cids = set()

        def mock_runner(cmd, input=None, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            if c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run' and '--entrypoint' in cmd:
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                sub = cmd[2]
                if sub == 'inspect':
                    # Return not found for absent check, or receipt for verify
                    if 'created_ok' in created_cmds:
                        rec = [{'Name': self.container_config['volume_name'],
                                'Labels': self.saved_labels}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'', stderr=b'no such volume')
                elif sub == 'create':
                    created_cmds.append('created_ok')
                    self.saved_labels = {}
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            self.saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'create':
                cid = f'{len(created_cmds):064x}'
                created_cmds.append(cid)
                return mock.Mock(returncode=0, stdout=cid.encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'container' and cmd[2] == 'inspect':
                cid = cmd[3]
                if cid in rm_cids:
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: No such container')
                return mock.Mock(returncode=0, stdout=b'[{"State": {"ExitCode": 0}}]', stderr=b'')
            elif c0 == 'docker' and c1 == 'start':
                started_cmds.append(cmd)
                # Reader verification container returns stats + json
                if len(started_cmds) >= 3:
                    full_store = self.m.compose_store(
                        {'clients': [{'username': self.container_config['manager'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]},
                        {'clients': [{'username': self.container_config['backend'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]}
                    )
                    store_bytes = json.dumps(full_store).encode()
                    out = b'1883:1883:700\n1883:1883:600\n' + store_bytes
                    return mock.Mock(returncode=0, stdout=out, stderr=b'')
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'rm':
                rm_cmds.append(cmd)
                rm_cids.add(cmd[-1])
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                # Write minimal native client json
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            vol = self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))
            self.assertEqual(vol, self.container_config['volume_name'])
            # Verify no secret leaked into any executed command argv
            for cmd in created_cmds + started_cmds + rm_cmds:
                if isinstance(cmd, list):
                    for arg in cmd:
                        self.assertNotIn(self.container_config['manager_password'], str(arg))
                        self.assertNotIn(self.container_config['backend_password'], str(arg))

    def test_volume_absence_checks_no_such_volume_and_daemon_errors(self):
        # 1. Normal absent case: returns 1 with 'no such volume' in stderr
        def mock_absent(cmd, **kw):
            return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error response from daemon: get vol: no such volume\n')
        self.m.check_volume_absent('vol', runner=mock_absent)

        # 2. Collision case: returncode 0
        def mock_exists(cmd, **kw):
            return mock.Mock(returncode=0, stdout=b'[{"Name": "vol"}]', stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'volume collision'):
            self.m.check_volume_absent('vol', runner=mock_exists)

        # 3. Daemon error / timeout / unexpected stderr: nonzero but no "no such volume"
        for bad_err in [b'connection refused', b'i/o timeout', b'permission denied', b'']:
            with self.subTest(err=bad_err):
                def mock_err(cmd, **kw):
                    return mock.Mock(returncode=1, stdout=b'', stderr=bad_err)
                with self.assertRaisesRegex(self.m.BootstrapError, 'presence check failed'):
                    self.m.check_volume_absent('vol', runner=mock_err)

    def test_label_validation_rejects_reserved_and_invalid_labels(self):
        # Reject reserved labels
        for rk in ['bootstrap.managed', 'bootstrap.nonce', 'com.docker.compose.volume']:
            with self.subTest(key=rk):
                bad_cfg = dict(self.container_config, labels={rk: 'custom'})
                with self.assertRaisesRegex(self.m.BootstrapError, 'reserved label key'):
                    self.m.parse_config(io.StringIO(json.dumps(bad_cfg)), container_mode=True)

        # Reject non-string, length > 256, or invalid format
        for bad_k, bad_v in [('bad key with space', 'val'), ('k'*129, 'val'), ('key', 'v'*257), ('key', 123)]:
            with self.subTest(k=bad_k, v=bad_v):
                bad_cfg = dict(self.container_config, labels={bad_k: bad_v})
                with self.assertRaises(self.m.BootstrapError):
                    self.m.parse_config(io.StringIO(json.dumps(bad_cfg)), container_mode=True)

    def test_verify_image_accepts_verified_tuple_and_rejects_index_as_config(self):
        # Independently verified literal inspect metadata
        verified_tuple_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4'
        }]

        def mock_verified_tuple(cmd, **kw):
            if cmd[1] == 'image':
                return mock.Mock(returncode=0, stdout=json.dumps(verified_tuple_inspect).encode(), stderr=b'')
            return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')

        # Real verified inspect tuple accepted
        res = self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_verified_tuple)
        self.assertEqual(res, 'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4')

        # Rejects root index digest as Id when Descriptor metadata is missing
        index_as_config_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:' + self.m.PINNED_DIGEST
        }]

        def mock_index_as_config(cmd, **kw):
            if cmd[1] == 'image':
                return mock.Mock(returncode=0, stdout=json.dumps(index_as_config_inspect).encode(), stderr=b'')
            return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')

        with self.assertRaisesRegex(self.m.BootstrapError, 'missing or malformed root index descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_index_as_config)

    def test_verify_image_exact_repodigests_pin_and_immutable_id(self):
        # Exact match succeeds
        def mock_ok(cmd, **kw):
            if cmd[1] == 'image':
                data = [{
                    'Architecture': 'amd64',
                    'Os': 'linux',
                    'RepoDigests': [self.m.PINNED_IMAGE],
                    'Id': self.m.PINNED_CONFIG_ID
                }]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
        img_id = self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_ok)
        self.assertEqual(img_id, self.m.PINNED_CONFIG_ID)

        # Substring digest spoof in RepoDigests is rejected
        def mock_spoof_repo(cmd, **kw):
            if cmd[1] == 'image':
                data = [{
                    'Architecture': 'amd64',
                    'Os': 'linux',
                    'RepoDigests': ['attacker/' + self.m.PINNED_IMAGE],
                    'Id': self.m.PINNED_CONFIG_ID
                }]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'RepoDigests exact pin mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_spoof_repo)

        # Mismatched immutable Id is rejected
        def mock_bad_id(cmd, **kw):
            if cmd[1] == 'image':
                data = [{
                    'Architecture': 'amd64',
                    'Os': 'linux',
                    'RepoDigests': [self.m.PINNED_IMAGE],
                    'Id': 'sha256:' + '0'*64
                }]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'Id immutable digest mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_bad_id)

    def test_verify_image_containerd_root_index_store_accepted(self):
        root_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:' + self.m.PINNED_DIGEST,
            'Descriptor': {
                'digest': 'sha256:' + self.m.PINNED_DIGEST,
                'mediaType': 'application/vnd.oci.image.index.v1+json'
            }
        }]
        platform_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'Id': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
            'Descriptor': {
                'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
                'mediaType': 'application/vnd.oci.image.manifest.v1+json'
            }
        }]

        def mock_containerd(cmd, **kw):
            if cmd[:3] == ['docker', 'image', 'inspect']:
                if '--platform' in cmd:
                    self.assertEqual(cmd, ['docker', 'image', 'inspect', '--platform', 'linux/amd64', self.m.PINNED_IMAGE])
                    return mock.Mock(returncode=0, stdout=json.dumps(platform_inspect).encode(), stderr=b'')
                return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
            return mock.Mock(returncode=1)

        res = self.m.verify_image(self.m.PINNED_IMAGE, runner=mock_containerd)
        self.assertEqual(res, 'sha256:' + self.m.PINNED_DIGEST)

    def test_verify_image_classic_store_with_valid_and_invalid_descriptor(self):
        # Valid classic descriptor with exact supported pair (config digest with OCI image config mediaType)
        valid_desc_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {
                'digest': self.m.PINNED_CONFIG_ID,
                'mediaType': 'application/vnd.oci.image.config.v1+json'
            }
        }]
        res = self.m.verify_image(self.m.PINNED_IMAGE,
                                  runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(valid_desc_inspect).encode()))
        self.assertEqual(res, self.m.PINNED_CONFIG_ID)

        # Valid classic inspect with NO Descriptor present (descriptor is None / omitted)
        no_desc_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID
        }]
        res_no_desc = self.m.verify_image(self.m.PINNED_IMAGE,
                                          runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(no_desc_inspect).encode()))
        self.assertEqual(res_no_desc, self.m.PINNED_CONFIG_ID)

        # Rejection: config d259 with unknown mediaType
        unknown_mt_desc = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {
                'digest': self.m.PINNED_CONFIG_ID,
                'mediaType': 'application/vnd.unknown.custom.type.v1+json'
            }
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'unsupported image descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(unknown_mt_desc).encode()))

        # Rejection: valid manifest f438 digest with swapped mediaType (e.g. index mediaType)
        swapped_mt_desc = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {
                'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
                'mediaType': 'application/vnd.oci.image.index.v1+json'
            }
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'unsupported image descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(swapped_mt_desc).encode()))

        # Rejection: valid root d12c digest with config mediaType (swapped)
        swapped_root_desc = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {
                'digest': 'sha256:' + self.m.PINNED_DIGEST,
                'mediaType': 'application/vnd.oci.image.config.v1+json'
            }
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'unsupported image descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(swapped_root_desc).encode()))

        # Malformed descriptor (not a dict: string, int, empty dict)
        bad_desc1 = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': 'not-a-dict'
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'malformed image descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(bad_desc1).encode()))

        # Empty dict descriptor
        empty_desc = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {}
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'malformed image descriptor metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(empty_desc).encode()))

        # Contradictory descriptor digest
        bad_desc2 = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': self.m.PINNED_CONFIG_ID,
            'Descriptor': {
                'digest': 'sha256:' + 'e'*64,
                'mediaType': 'application/vnd.oci.image.config.v1+json'
            }
        }]
        with self.assertRaisesRegex(self.m.BootstrapError, 'contradictory image descriptor digest'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps(bad_desc2).encode()))

    def test_verify_image_containerd_rejects_spoofed_or_mismatched_descriptor(self):
        base_root = {
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:' + self.m.PINNED_DIGEST,
            'Descriptor': {
                'digest': 'sha256:' + self.m.PINNED_DIGEST,
                'mediaType': 'application/vnd.oci.image.index.v1+json'
            }
        }
        # Wrong root descriptor digest
        wrong_root_digest = dict(base_root, Descriptor={'digest': 'sha256:' + 'a'*64, 'mediaType': 'application/vnd.oci.image.index.v1+json'})
        with self.assertRaisesRegex(self.m.BootstrapError, 'root index descriptor mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([wrong_root_digest]).encode()))

        # Wrong root descriptor mediaType
        wrong_root_mt = dict(base_root, Descriptor={'digest': 'sha256:' + self.m.PINNED_DIGEST, 'mediaType': 'application/vnd.docker.distribution.manifest.v2+json'})
        with self.assertRaisesRegex(self.m.BootstrapError, 'root index descriptor mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE,
                                runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([wrong_root_mt]).encode()))

    def test_verify_image_containerd_second_inspect_failure_and_mismatches(self):
        root_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:' + self.m.PINNED_DIGEST,
            'Descriptor': {
                'digest': 'sha256:' + self.m.PINNED_DIGEST,
                'mediaType': 'application/vnd.oci.image.index.v1+json'
            }
        }]

        # 1. Platform inspect command failure
        def fail_platform(cmd, **kw):
            if '--platform' in cmd:
                return mock.Mock(returncode=1, stdout=b'', stderr=b'unsupported platform')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'platform-specific image inspect failed'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=fail_platform)

        # 2. Platform inspect timeout
        def timeout_platform(cmd, **kw):
            if '--platform' in cmd:
                raise self.m.subprocess.TimeoutExpired(cmd, 10)
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'platform-specific image inspect failed or timed out'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=timeout_platform)

        # 3. Platform inspect returns multiple records
        def multi_platform(cmd, **kw):
            if '--platform' in cmd:
                return mock.Mock(returncode=0, stdout=json.dumps([{}, {}]).encode(), stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'invalid platform-specific image inspect metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=multi_platform)

        # 3b. Platform inspect returns malformed JSON or duplicate keys
        def malformed_json_platform(cmd, **kw):
            if '--platform' in cmd:
                return mock.Mock(returncode=0, stdout=b'{"not_a_list": true}', stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'invalid platform-specific image inspect metadata'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=malformed_json_platform)

        def dup_keys_platform(cmd, **kw):
            if '--platform' in cmd:
                return mock.Mock(returncode=0, stdout=b'[{"Id": "sha256:1", "Id": "sha256:2"}]', stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'duplicate JSON field'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=dup_keys_platform)

        # 4. Platform inspect Os / Architecture mismatch
        def arch_mismatch(cmd, **kw):
            if '--platform' in cmd:
                return mock.Mock(returncode=0, stdout=json.dumps([{'Architecture': 'arm64', 'Os': 'linux'}]).encode(), stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'platform-specific image inspect os/architecture mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=arch_mismatch)

        # 5. Platform inspect Descriptor digest mismatch
        def desc_mismatch(cmd, **kw):
            if '--platform' in cmd:
                plat_rec = {
                    'Architecture': 'amd64', 'Os': 'linux',
                    'Id': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
                    'Descriptor': {'digest': 'sha256:' + 'b'*64, 'mediaType': 'application/vnd.oci.image.manifest.v1+json'}
                }
                return mock.Mock(returncode=0, stdout=json.dumps([plat_rec]).encode(), stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'platform-specific image descriptor mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=desc_mismatch)

        # 6. Platform inspect Id mismatch
        def id_mismatch(cmd, **kw):
            if '--platform' in cmd:
                plat_rec = {
                    'Architecture': 'amd64', 'Os': 'linux',
                    'Id': 'sha256:' + 'c'*64,
                    'Descriptor': {'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST, 'mediaType': 'application/vnd.oci.image.manifest.v1+json'}
                }
                return mock.Mock(returncode=0, stdout=json.dumps([plat_rec]).encode(), stderr=b'')
            return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
        with self.assertRaisesRegex(self.m.BootstrapError, 'platform-specific image Id mismatch'):
            self.m.verify_image(self.m.PINNED_IMAGE, runner=id_mismatch)

    def test_helper_receipt_root_index_image_manifest_descriptor(self):
        # Container inspect receipt checking under root-index execution
        root_img = 'sha256:' + self.m.PINNED_DIGEST
        item = {'name': 'test-helper', 'image': root_img, 'nonce': 'nonce123',
                'user': '1883:1883', 'mounts': []}
        base_rec = {
            'Id': 'd' * 64,
            'Name': '/test-helper',
            'Image': root_img,
            'Config': {'Labels': {'bootstrap.nonce': 'nonce123'}, 'User': '1883:1883'},
            'HostConfig': {'Mounts': []}
        }

        # 1. Accepted when ImageManifestDescriptor matches pinned platform manifest
        valid_rec = dict(base_rec, ImageManifestDescriptor={
            'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
            'mediaType': 'application/vnd.oci.image.manifest.v1+json'
        })
        cid = self.m.resolve_helper(item, runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([valid_rec]).encode()))
        self.assertEqual(cid, 'd' * 64)

        # 2. Rejected when ImageManifestDescriptor is missing
        with self.assertRaisesRegex(self.m.BootstrapError, 'missing container image manifest descriptor'):
            self.m.resolve_helper(item, runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([base_rec]).encode()))

        # 3. Rejected when ImageManifestDescriptor has mismatched digest
        bad_digest_rec = dict(base_rec, ImageManifestDescriptor={
            'digest': 'sha256:' + 'e' * 64,
            'mediaType': 'application/vnd.oci.image.manifest.v1+json'
        })
        with self.assertRaisesRegex(self.m.BootstrapError, 'container image manifest descriptor mismatch'):
            self.m.resolve_helper(item, runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([bad_digest_rec]).encode()))

        # 4. Rejected when ImageManifestDescriptor has wrong mediaType
        bad_mt_rec = dict(base_rec, ImageManifestDescriptor={
            'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
            'mediaType': 'application/vnd.docker.distribution.manifest.v2+json'
        })
        with self.assertRaisesRegex(self.m.BootstrapError, 'container image manifest descriptor mismatch'):
            self.m.resolve_helper(item, runner=lambda cmd, **kw: mock.Mock(returncode=0, stdout=json.dumps([bad_mt_rec]).encode()))

    def test_container_bootstrap_containerd_lifecycle_success(self):
        created_cmds = []
        started_cmds = []
        rm_cmds = []
        rm_cids = set()
        vol_created = False
        saved_labels = {}

        root_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.m.PINNED_IMAGE],
            'Id': 'sha256:' + self.m.PINNED_DIGEST,
            'Descriptor': {
                'digest': 'sha256:' + self.m.PINNED_DIGEST,
                'mediaType': 'application/vnd.oci.image.index.v1+json'
            }
        }]
        platform_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'Id': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
            'Descriptor': {
                'digest': self.m.PINNED_PLATFORM_MANIFEST_DIGEST,
                'mediaType': 'application/vnd.oci.image.manifest.v1+json'
            }
        }]

        def mock_runner(cmd, input=None, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels
            if c0 == 'docker' and c1 == 'image':
                if '--platform' in cmd:
                    return mock.Mock(returncode=0, stdout=json.dumps(platform_inspect).encode(), stderr=b'')
                return mock.Mock(returncode=0, stdout=json.dumps(root_inspect).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                sub = cmd[2]
                if sub == 'inspect':
                    if vol_created:
                        rec = [{'Name': self.container_config['volume_name'], 'Labels': saved_labels}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'', stderr=b'no such volume')
                elif sub == 'create':
                    vol_created = True
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'create':
                cid = f'{len(created_cmds):064x}'
                created_cmds.append(cid)
                return mock.Mock(returncode=0, stdout=cid.encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'container' and cmd[2] == 'inspect':
                cid = cmd[3]
                if cid in rm_cids:
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: No such container')
                return mock.Mock(returncode=0, stdout=b'[{"State": {"ExitCode": 0}}]', stderr=b'')
            elif c0 == 'docker' and c1 == 'start':
                started_cmds.append(cmd)
                if len(started_cmds) >= 3:
                    full_store = self.m.compose_store(
                        {'clients': [{'username': self.container_config['manager'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]},
                        {'clients': [{'username': self.container_config['backend'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]}
                    )
                    store_bytes = json.dumps(full_store).encode()
                    out = b'1883:1883:700\n1883:1883:600\n' + store_bytes
                    return mock.Mock(returncode=0, stdout=out, stderr=b'')
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'rm':
                rm_cmds.append(cmd)
                rm_cids.add(cmd[-1])
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            vol = self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))
            self.assertEqual(vol, self.container_config['volume_name'])

    def test_root_helper_enforces_user_0_0_and_pull_never(self):
        created_cmds = []
        vol_created = False
        saved_labels = {}
        def mock_runner(cmd, **kw):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels
            if c0 == 'docker' and c1 == 'create':
                created_cmds.append(cmd)
                # fail after capturing root helper create command
                return mock.Mock(returncode=1, stdout=b'', stderr=b'stop here')
            elif c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run':
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                if cmd[2] == 'inspect':
                    if vol_created:
                        return mock.Mock(returncode=0, stdout=json.dumps([{'Name': self.container_config['volume_name'], 'Labels': saved_labels}]).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: no such volume')
                elif cmd[2] == 'create':
                    vol_created = True
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            with self.assertRaises(self.m.BootstrapError):
                self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))

        # Inspect root helper create cmd
        root_cmd = [c for c in created_cmds if any('mqtt-boot-root' in str(a) for a in c)][0]
        self.assertIn('--pull=never', root_cmd)
        self.assertIn('--user', root_cmd)
        user_idx = root_cmd.index('--user')
        self.assertEqual(root_cmd[user_idx + 1], '0:0')
        # Ensure image passed was immutable ID
        self.assertIn(self.m.PINNED_CONFIG_ID, root_cmd)

    def test_inspect_exit_code_detects_container_failure_despite_start_rc_zero(self):
        vol_created = False
        saved_labels = {}
        rm_cids = set()
        def mock_runner(cmd, **kw):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels
            if c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run':
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                if cmd[2] == 'inspect':
                    if vol_created:
                        return mock.Mock(returncode=0, stdout=json.dumps([{'Name': self.container_config['volume_name'], 'Labels': saved_labels}]).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: no such volume')
                elif cmd[2] == 'create':
                    vol_created = True
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'create':
                return mock.Mock(returncode=0, stdout=b'a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90', stderr=b'')
            elif c0 == 'docker' and c1 == 'start':
                # start -a exits 0 (e.g. docker daemon detached or swallowed)
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'container' and cmd[2] == 'inspect':
                cid = cmd[3]
                if cid in rm_cids:
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: No such container')
                return mock.Mock(returncode=0, stdout=b'[{"State": {"ExitCode": 1}}]', stderr=b'')
            elif c0 == 'docker' and c1 == 'rm':
                cid = cmd[-1]
                rm_cids.add(cid)
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            with self.assertRaisesRegex(self.m.BootstrapError, 'root ownership helper execution failed'):
                self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))

    def test_cleanup_failure_vetoes_success_and_retains_store(self):
        vol_created = False
        saved_labels = {}
        started_cmds = []

        def mock_runner(cmd, input=None, **kw):
            c0, c1 = cmd[0], cmd[1]
            nonlocal vol_created, saved_labels
            if c0 == 'docker' and c1 == 'image':
                data = [{'Architecture': 'amd64', 'Os': 'linux',
                         'RepoDigests': [self.m.PINNED_IMAGE],
                         'Id': self.m.PINNED_CONFIG_ID}]
                return mock.Mock(returncode=0, stdout=json.dumps(data).encode(), stderr=b'')
            elif c0 == 'docker' and c1 == 'run':
                return mock.Mock(returncode=1, stdout=self.m.EXPECTED_VERSION_BANNER, stderr=b'')
            elif c0 == 'docker' and c1 == 'volume':
                if cmd[2] == 'inspect':
                    if vol_created:
                        return mock.Mock(returncode=0, stdout=json.dumps([{'Name': self.container_config['volume_name'], 'Labels': saved_labels}]).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stdout=b'[]', stderr=b'Error: no such volume')
                elif cmd[2] == 'create':
                    vol_created = True
                    for i, a in enumerate(cmd):
                        if a == '--label' and i + 1 < len(cmd):
                            k, v = cmd[i+1].split('=', 1)
                            saved_labels[k] = v
                    return mock.Mock(returncode=0, stdout=self.container_config['volume_name'].encode(), stderr=b'')
                elif cmd[2] == 'rm':
                    # Must NOT be called when volume retained!
                    raise AssertionError('volume rm should not be called when store is retained!')
            elif c0 == 'docker' and c1 == 'create':
                return mock.Mock(returncode=0, stdout=b'a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90', stderr=b'')
            elif c0 == 'docker' and c1 == 'container' and cmd[2] == 'inspect':
                return mock.Mock(returncode=0, stdout=b'[{"State": {"ExitCode": 0}}]', stderr=b'')
            elif c0 == 'docker' and c1 == 'start':
                started_cmds.append(cmd)
                if len(started_cmds) >= 3:
                    full_store = self.m.compose_store(
                        {'clients': [{'username': self.container_config['manager'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]},
                        {'clients': [{'username': self.container_config['backend'],
                                      'password': base64.b64encode(b'A'*64).decode(),
                                      'salt': base64.b64encode(b'B'*12).decode(),
                                      'iterations': 101, 'roles': []}]}
                    )
                    out = b'1883:1883:700\n1883:1883:600\n' + json.dumps(full_store).encode()
                    return mock.Mock(returncode=0, stdout=out, stderr=b'')
                return mock.Mock(returncode=0, stdout=b'', stderr=b'')
            elif c0 == 'docker' and c1 == 'rm':
                # rm fails: container still exists
                return mock.Mock(returncode=1, stdout=b'', stderr=b'cannot rm container')
            return mock.Mock(returncode=0, stdout=b'', stderr=b'')

        with mock.patch.object(self.m, 'container_native_init') as mock_cinit:
            def fake_cinit(image, path, user, pwd, tracker, runner):
                c = {'username': user, 'password': base64.b64encode(b'A'*64).decode(),
                     'salt': base64.b64encode(b'B'*12).decode(), 'iterations': 101, 'roles': []}
                path.write_text(json.dumps({'clients': [c]}))
            mock_cinit.side_effect = fake_cinit

            # With remove_and_verify_container failing during cleanup in reader or steps,
            # verify it raises cleanup failure veto
            with self.assertRaises(self.m.BootstrapError):
                self.m.bootstrap_container(self.container_config, image_ref=self.m.PINNED_IMAGE, runner=with_allocation_receipts(mock_runner))

    def test_config_file_security_checks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_p = Path(tmp_dir)
            cfg_file = tmp_p / 'test.json'
            cfg_file.write_text('{}')

            # 1. Non-0600 mode rejected
            cfg_file.chmod(0o644)
            with self.assertRaisesRegex(self.m.BootstrapError, '0600'):
                self.m.check_config_file_security(cfg_file)

            # 2. 0600 valid file passes
            cfg_file.chmod(0o600)
            self.m.check_config_file_security(cfg_file)

            # 3. Symlink rejected
            sym = tmp_p / 'symlink.json'
            sym.symlink_to(cfg_file)
            with self.assertRaisesRegex(self.m.BootstrapError, 'symlink'):
                self.m.check_config_file_security(sym)


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
