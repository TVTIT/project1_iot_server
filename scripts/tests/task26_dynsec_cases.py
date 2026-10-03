#!/usr/bin/env python3
"""Bounded native MQTT/TLS DynSec investigation, never a production adapter.

Assertions precede execution. Reuses the existing native TLS CONNECT oracle;
small test-only MQTT framing avoids adding a dependency. No secret in argv,
retained requests, debug logging, production credential or production mount.
"""
import concurrent.futures
import json
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import time
import uuid

from stage2_mosquitto_runtime import ENV, mqtt_connection, run

from task26_harness_helpers import CONTROL, RESPONSE, MQTT, require, text
COMPOSE = Path(__file__).with_name('compose.task260-dynsec.yml')


class Spike:
    def __init__(self, directory):
        self.d = Path(directory)
        self.project = 'dynsec_spike_' + uuid.uuid4().hex[:12]
        self.env = dict(ENV, DYNSEC_FIXTURE=str(self.d), COMPOSE_DISABLE_ENV_FILE='1')
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-p',
                        self.project, '-f', str(COMPOSE)]
        self.passwords = {key: secrets.token_urlsafe(32)
                          for key in ('admin', 'manager', 'backend', 'A', 'B', 'new')}
        self.connections = []
        self.artifacts = self.d / 'artifacts'
        self.artifacts.mkdir(mode=0o700)

    def command(self, *args, **kwargs):
        return run(self.compose + list(args), env=self.env, **kwargs)

    def setup(self, script, data=None):
        return self.command('run', '--rm', '-T', 'setup', '-ec', script, data=data,
                            capture=True)

    def connect(self, user, password=None):
        connection = MQTT(self, user, password or self.passwords[user])
        self.connections.append(connection)
        return connection

    def probe(self, user, password=None, accepted=True):
        mqtt_connection(self.port, self.d / 'ca.crt', user,
                        password or self.passwords[user], accepted).close()

    def prepare(self):
        # Isolated port, identities and credentials never target deployment.
        print('isolated fixture; no deployment .env access', flush=True)
        broker = self.d / 'broker'
        broker.mkdir(mode=0o700)
        for args in (
            ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=isolated-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
             '-keyout', 'ca.key', '-out', 'ca.crt'],
            ['req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
             '-keyout', 'server.key', '-out', 'server.csr'],
            ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=wrong-ca', '-keyout', 'wrong-ca.key', '-out', 'wrong-ca.crt'],
        ):
            args = [str(self.d / a) if a.endswith(('.key', '.crt', '.csr')) else a for a in args]
            run(['openssl'] + args)
        (self.d / 'extensions').write_text('subjectAltName=DNS:localhost\n'
            'extendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n')
        run(['openssl', 'x509', '-req', '-in', str(self.d / 'server.csr'),
             '-CA', str(self.d / 'ca.crt'), '-CAkey', str(self.d / 'ca.key'),
             '-CAcreateserial', '-days', '1', '-extfile', str(self.d / 'extensions'),
             '-out', str(self.d / 'server.crt')])
        for name in ('server.key', 'server.crt', 'ca.crt'):
            shutil.copyfile(self.d / name, broker / name)
        (broker / 'broker.conf').write_text('listener 8883\nallow_anonymous false\n'
            'cafile /fixture/ca.crt\ncertfile /fixture/server.crt\nkeyfile /fixture/server.key\n'
            'plugin /usr/lib/mosquitto_dynamic_security.so\n'
            'plugin_opt_config_file /security/dynsec.json\n'
            'log_dest stdout\nlog_type error\nlog_type warning\n')
        for p in self.d.iterdir():
            if p.is_file():
                p.chmod(0o600)
        # Offline v2.0 init: stdin password prompt, never argv or v2.1 autobootstrap.
        password = self.passwords['admin']
        self.setup('umask 077; mkdir -p /security; chmod 700 /security; '
                   'mosquitto_ctrl dynsec init /security/dynsec.json admin >/dev/null; '
                   'chmod 600 /security/dynsec.json; chown -R 1883:1883 /security /fixture',
                   (password + '\n' + password + '\n').encode())
        print('PASS plugin available; offline stdin bootstrap; protected named volume', flush=True)

    def start(self):
        self.command('up', '-d', 'broker')
        self.port = int(self.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
        deadline = time.monotonic() + 10
        while True:
            try:
                self.probe('admin')
                break
            except (OSError, RuntimeError):
                require(time.monotonic() < deadline, 'broker startup timeout')
                time.sleep(.1)

    def restart(self):
        self.command('kill', '-s', 'SIGKILL', 'broker')
        self.start()
        print('broker SIGKILL/restart completed', flush=True)

    def role(self, control, name, acls):
        control.request('createRole', rolename=name)
        for kind, topic in acls:
            control.request('addRoleACL', rolename=name, acltype=kind,
                            topic=topic, allow=True, priority=1)

    def provision(self, control, user, role):
        # 2.0.18 createClient does NOT consume a disabled field. Omit password
        # and roles: native pw.valid=false rejects all CONNECTs until setPassword.
        control.request('createClient', username=user)
        self.probe(user, accepted=False)
        control.request('disableClient', username=user)
        control.request('setClientPassword', username=user, password=self.passwords[user])
        control.request('addClientRole', username=user, rolename=role)
        self.probe(user, accepted=False)
        control.request('enableClient', username=user)
        self.probe(user)

    def delivery(self, publisher, subscriber, topic, allowed):
        payload = uuid.uuid4().hex.encode()  # non-secret oracle
        publisher.publish(topic, payload)
        if allowed:
            received_topic, received = subscriber.message()
            require(received_topic == topic and received == payload, 'allowed delivery failed')
        else:
            subscriber.s.settimeout(.25)
            try:
                subscriber.message()
            except socket.timeout:
                pass
            else:
                raise RuntimeError('denied publish delivered')
            finally:
                subscriber.s.settimeout(2)

    def disconnected(self, connection):
        connection.s.settimeout(2)
        try:
            require(connection.s.recv(1) == b'', 'active session not disconnected')
        except (ConnectionResetError, ssl.SSLEOFError):
            pass

    def tests(self):
        admin = self.connect('admin')
        admin.subscribe(RESPONSE)
        admin.request('setDefaultACLAccess', acls=[{'acltype': key, 'allow': False}
            for key in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')])
        defaults = admin.request('getDefaultACLAccess')
        require(all(not acl['allow'] for acl in defaults['acls']), 'default deny readback')
        self.role(admin, 'management', [('publishClientSend', CONTROL),
                  ('publishClientReceive', RESPONSE), ('subscribeLiteral', RESPONSE)])
        self.provision(admin, 'manager', 'management')
        # Remove alltopics bootstrap role from admin itself. Manager becomes
        # control principal; telemetry backend never receives this credential.
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        manager.request('removeClientRole', username='admin', rolename='admin')
        manager.request('deleteRole', rolename='admin')
        manager.subscribe('#', allowed=False)
        self.role(manager, 'telemetry', [(kind, 'gateways/+/' + suffix)
            for suffix in ('telemetry/#', 'responses/#', 'status')
            for kind in ('publishClientReceive', 'subscribePattern')] +
            [(kind, 'gateways/+/' + suffix + '/#')
             for suffix in ('acks', 'commands') for kind in ('publishClientSend',)])
        self.provision(manager, 'backend', 'telemetry')
        for user in ('A', 'B'):
            self.role(manager, 'gateway_' + user,
                [('publishClientSend', 'gateways/' + user + '/' + suffix)
                 for suffix in ('telemetry/#', 'responses/#', 'status')] +
                [(kind, 'gateways/' + user + '/' + suffix + '/#')
                 for suffix in ('acks', 'commands')
                 for kind in ('publishClientReceive', 'subscribePattern')])
            self.provision(manager, user, 'gateway_' + user)
        print('PASS createClient no-password/no-role -> disable -> setPassword/role -> enable; no login window', flush=True)
        backend = self.connect('backend')
        backend.subscribe('gateways/+/telemetry/#')
        backend.subscribe('gateways/+/responses/#')
        backend.subscribe('gateways/+/status')
        a, b = self.connect('A'), self.connect('B')
        self.delivery(a, backend, 'gateways/A/telemetry/test', True)
        self.delivery(a, backend, 'gateways/B/telemetry/test', False)
        self.delivery(b, backend, 'gateways/B/telemetry/test', True)
        self.delivery(a, backend, 'gateways/A/responses/test', True)
        self.delivery(a, backend, 'gateways/A/status', True)
        self.delivery(a, backend, 'gateways/B/responses/test', False)
        self.delivery(a, backend, 'gateways/B/status', False)
        for connection, other in ((a, 'B'), (b, 'A')):
            connection.subscribe('gateways/' + other + '/commands/#', allowed=False)
            connection.subscribe(CONTROL + '/#', allowed=False)
            connection.subscribe('gateways/' + ('A' if other == 'B' else 'B') + '/acks/#')
            connection.subscribe('gateways/' + ('A' if other == 'B' else 'B') + '/commands/#')
        self.delivery(backend, a, 'gateways/A/commands/test', True)
        self.delivery(backend, a, 'gateways/A/acks/test', True)
        self.delivery(a, a, 'gateways/A/commands/test', False)
        # Gateway attempts management mutation. A fresh correlated readback
        # proves B was not disabled; no PUBACK inference.
        a.publish(CONTROL, json.dumps({'commands': [{'command': 'disableClient',
                   'username': 'B'}]}).encode())
        require(not manager.request('getClient', username='B')['client'].get('disabled', False),
                'gateway gained control access')
        backend.subscribe(RESPONSE, allowed=False)
        backend.publish(CONTROL, b'{"commands":[{"command":"disableClient","username":"B"}]}')
        require(not manager.request('getClient', username='B')['client'].get('disabled', False),
                'telemetry backend gained control access')
        manager.subscribe('gateways/+/telemetry/#', allowed=False)
        self.delivery(manager, backend, 'gateways/A/telemetry/test', False)
        print('PASS literal per-Gateway ACL isolation, send/receive, control denied; separate restricted manager', flush=True)
        try:
            manager.request('setClientPassword', username='A', password=self.passwords['new'], discard=True)
        except TimeoutError:
            pass
        else:
            raise RuntimeError('lost rotate response not injected')
        self.disconnected(a)
        self.probe('A', accepted=False)
        self.probe('A', self.passwords['new'])
        # Same supplied password on retry, never regenerate during uncertainty.
        manager.request('setClientPassword', username='A', password=self.passwords['new'])
        self.probe('A', self.passwords['new'])
        a = self.connect('A', self.passwords['new'])
        print('PASS rotation new login/old rejection; observed active A disconnected', flush=True)
        try:
            manager.request('disableClient', username='A', discard=True)
        except TimeoutError:
            pass
        else:
            raise RuntimeError('lost-response injection not triggered')
        require(manager.request('getClient', username='A')['client']['disabled'], 'disable readback')
        self.disconnected(a)
        self.probe('A', self.passwords['new'], accepted=False)
        manager.request('disableClient', username='A')
        manager.request('createClient', username='B', error=True)
        try:
            manager.request('createClient', username='pending_fixture', discard=True)
        except TimeoutError:
            pass
        else:
            raise RuntimeError('lost create response not injected')
        created = manager.request('getClient', username='pending_fixture')['client']
        require(created['username'] == 'pending_fixture' and not created.get('roles'),
                'lost create readback')
        manager.request('createClient', username='pending_fixture', error=True)
        manager.request('deleteClient', username='pending_fixture')
        self.probe('B')
        self.delivery(b, backend, 'gateways/B/telemetry/test', True)
        print('PASS lost application response after disable; correlated readback/retry; B/backend unaffected', flush=True)
        # Bounded concurrent requests use separate connections and random IDs.
        def query(_):
            c = self.connect('manager')
            try:
                c.subscribe(RESPONSE)
                require(c.request('getClient', username='B')['client']['username'] == 'B',
                        'concurrent matched result')
            finally:
                c.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(query, range(4)))
        idle = self.connect('manager')
        idle.subscribe(RESPONSE)
        idle.s.settimeout(.25)
        try:
            idle.message()
        except socket.timeout:
            pass
        else:
            raise RuntimeError('retained control response present')
        idle.close()
        # Ordinary request timeout: no invented response from a transport receipt.
        backend.publish(CONTROL, b'{"commands":[{"command":"getClient","username":"B"}]}')
        backend.s.settimeout(.25)
        try:
            backend.message()
        except socket.timeout:
            pass
        else:
            raise RuntimeError('unauthorized control response received')
        backend.s.settimeout(2)
        print('PASS bounded concurrency correlated replies, no retained response, unauthorized request timeout', flush=True)
        for ca, hostname in ((self.d / 'wrong-ca.crt', 'localhost'),
                             (self.d / 'ca.crt', 'wrong-host.invalid')):
            ctx = ssl.create_default_context(cafile=str(ca))
            raw = socket.create_connection(('127.0.0.1', self.port), 2)
            try:
                ctx.wrap_socket(raw, server_hostname=hostname)
            except ssl.SSLCertVerificationError:
                pass
            else:
                raise RuntimeError('TLS trust/hostname bypass')
            finally:
                raw.close()
        print('PASS wrong CA and hostname reject', flush=True)
        self.restart()
        self.probe('A', self.passwords['new'], accepted=False)
        self.probe('B')
        self.probe('backend')
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        require(manager.request('getClient', username='A')['client']['disabled'], 'restart disabled readback')
        manager.request('enableClient', username='A')
        self.probe('A', self.passwords['new'])
        self.probe('A', accepted=False)
        manager.request('disableClient', username='A')
        b, backend = self.connect('B'), self.connect('backend')
        backend.subscribe('gateways/+/telemetry/#')
        self.delivery(b, backend, 'gateways/B/telemetry/test', True)
        b.subscribe('gateways/A/commands/#', allowed=False)
        b.subscribe(CONTROL + '/#', allowed=False)
        print('PASS SIGKILL/restart retains clients/roles/rotated password/disabled state/ACL', flush=True)
        # Actual safe permission fault: owned directory mode 0500, broker UID1883.
        # File stays readable; .new create/rename must fail. No query is durable proof.
        self.setup('chmod 500 /security; test "$(stat -c %a /security)" = 500')
        manager.request('disableClient', username='B')
        require(manager.request('getClient', username='B')['client']['disabled'], 'fault live disable')
        self.disconnected(b)
        self.probe('B', accepted=False)
        logs = self.command('logs', '--no-color', 'broker', capture=True)
        require(b'File is not writable' in logs, 'actual persistence fault missing')
        self.restart()  # no successful save allowed between mutation and crash
        self.probe('B')
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        require(not manager.request('getClient', username='B')['client'].get('disabled', False),
                'expected restart rollback not observed')
        self.setup('chmod 700 /security')
        print('CONFIRMED BLOCKER: disable success + live disabled + disconnect, save failed; SIGKILL rolls B back to enabled', flush=True)
        print('adoption=NO-GO under current durable-success/G1 policy; not production migration', flush=True)

    def scan(self):
        logs = self.command('logs', '--no-color', 'broker', capture=True)
        require(not any(p.encode() in logs for p in self.passwords.values()), 'secret in broker log')
        require(b'PRIVATE KEY' not in logs and b'"password"' not in logs, 'sensitive broker log')
        path = self.artifacts / 'broker-redacted.log'
        path.write_bytes(logs)
        path.chmod(0o600)
        for artifact in self.artifacts.rglob('*'):
            if artifact.is_file():
                require(not any(p.encode() in artifact.read_bytes()
                                for p in self.passwords.values()), 'secret in test artifact')
        transcript = self.transcript
        if transcript.exists():
            require(not any(p.encode() in transcript.read_bytes()
                            for p in self.passwords.values()), 'secret in stdout/stderr transcript')
        print('PASS broker log secret scan; no debug logging/retained requests', flush=True)

    def cleanup(self):
        for connection in self.connections:
            connection.close()
        self.command('down', '-v', '--remove-orphans', timeout=30)
        for resource in ('container', 'network', 'volume'):
            noun = 'ps' if resource == 'container' else 'ls'
            output = run(['docker', resource, noun, '-q', '--filter',
                'label=com.docker.compose.project=' + self.project], capture=True)
            require(not output.strip(), 'owned resource cleanup incomplete')
        print('PASS owned cleanup verified project=' + self.project, flush=True)


def unit_tests():
    """Test-first assertions for response matching and packet security bounds."""
    import unittest
    from unittest.mock import Mock, patch

    class FramingTests(unittest.TestCase):
        def client(self):
            c = object.__new__(MQTT)
            c.s = Mock()
            c.pid = 0
            return c

        def test_publish_never_retains(self):
            c = self.client()
            c.publish(CONTROL, b'{}')
            self.assertEqual(c.s.sendall.call_args[0][0][0], 0x30)

        def test_bound_before_send(self):
            c = self.client()
            with self.assertRaises(RuntimeError):
                c.publish(CONTROL, b'x' * 16385)
            c.s.sendall.assert_not_called()

        def test_wrong_correlation_cannot_complete(self):
            c = self.client()
            c.message = Mock(return_value=(RESPONSE, json.dumps({'responses': [
                {'command': 'disableClient', 'correlationData': 'other'}]}).encode()))
            with self.assertRaises(TimeoutError):
                c.request('disableClient', username='A')
            self.assertEqual(c.message.call_count, 16)

        def test_matched_error_is_not_success(self):
            c = self.client()
            c.message = Mock(return_value=(RESPONSE, json.dumps({'responses': [
                {'command': 'disableClient', 'correlationData': 'fixed',
                 'error': 'Client not found'}]}).encode()))
            with patch.object(uuid, 'uuid4', return_value=Mock(hex='fixed')):
                with self.assertRaises(RuntimeError):
                    c.request('disableClient', username='A')

        def test_lost_reply_is_uncertain(self):
            c = self.client()
            c.message = Mock(return_value=(RESPONSE, json.dumps({'responses': [
                {'command': 'disableClient', 'correlationData': 'fixed'}]}).encode()))
            with patch.object(uuid, 'uuid4', return_value=Mock(hex='fixed')):
                with self.assertRaises(TimeoutError):
                    c.request('disableClient', username='A', discard=True)

        def test_retained_reply_rejected(self):
            c = self.client()
            c.packet = Mock(return_value=(0x31, text(RESPONSE) + b'{}'))
            with self.assertRaises(RuntimeError):
                c.message()

    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(FramingTests))
    require(result.wasSuccessful(), 'unit tests failed')
