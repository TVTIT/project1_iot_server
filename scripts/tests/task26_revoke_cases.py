#!/usr/bin/env python3
"""Bounded real-PG revoke proof; restart gate deliberately BLOCKED, not deployed.

The unguarded listener is ONLY the controlled negative environment. No .env is
read. Shared framing and owned fixture lifecycle are independent of old scripts.
"""
from pathlib import Path
import re
import secrets
import shutil
import socket
import ssl
import time
import uuid

from stage2_mosquitto_runtime import run
from task26_dynsec_cases import Spike
from task26_harness_helpers import RESPONSE, CONTROL, require, build_helper

COMPOSE = Path(__file__).with_name('compose.task260-revoke-reconcile.yml')


def intent_sql(operation, gateway):
    # SQL goes on stdin; restrictive fixture identities prevent interpolation.
    if not re.fullmatch('[a-f0-9]{32}', operation) or gateway not in ('A', 'B'):
        raise ValueError('invalid fixture identity')
    return f"""BEGIN;
SELECT gateway FROM authority WHERE gateway='{gateway}' FOR UPDATE;
INSERT INTO jobs(operation,gateway,status) VALUES ('{operation}','{gateway}','pending')
ON CONFLICT(operation) DO NOTHING;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM jobs WHERE operation='{operation}' AND gateway='{gateway}')
THEN RAISE EXCEPTION 'identity conflict'; END IF;
END $$;
UPDATE authority SET revoked=true,epoch=epoch+1
WHERE gateway='{gateway}' AND NOT revoked;
COMMIT;"""


def resolve_commit(status):
    # Missing row in a new connection is not, by itself, proof that an old
    # in-flight COMMIT cannot finish. Admission must remain closed until resolved.
    if status is None:
        return 'unknown'
    if status == '':
        return 'absent_observed'
    if status in ('pending', 'snapshot_observed'):
        return 'committed'
    raise ValueError('invalid outcome')


def retry_delays():
    return tuple(min(.4, .05 * 2**i) + secrets.randbelow(20) / 1000 for i in range(3))


class RevokeSpike(Spike):
    def __init__(self, directory):
        super().__init__(directory)
        self.project = 'revoke_reconcile_' + uuid.uuid4().hex[:12]
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-p',
                        self.project, '-f', str(COMPOSE)]

    def sql(self, sql):
        return self.command('exec', '-T', 'db', 'psql', '-X', '-qAt', '-v',
                            'ON_ERROR_STOP=1', '-U', 'postgres', data=sql.encode(),
                            capture=True, timeout=10).decode().strip()

    def prepare(self):
        # No root .env access, no production mounts or keys. Password file is
        # ephemeral protected DB bootstrap only, never a report artifact.
        (self.d / 'db-password').write_text(secrets.token_urlsafe(32))
        (self.d / 'db-password').chmod(0o600)
        build_helper('snapshot', self.d)
        (self.d / 'snapshot').chmod(0o555)
        broker = self.d / 'broker'
        broker.mkdir(mode=0o700)
        for name in ('ca', 'wrong-ca'):
            run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                 '-days', '1', '-subj', '/CN=fixture-' + name,
                 '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
                 '-keyout', str(self.d / (name + '.key')),
                 '-out', str(self.d / (name + '.crt'))])
        run(['openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
             '-keyout', str(self.d / 'server.key'), '-out', str(self.d / 'server.csr')])
        (self.d / 'extensions').write_text('subjectAltName=DNS:localhost\n'
            'extendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n')
        run(['openssl', 'x509', '-req', '-in', str(self.d / 'server.csr'),
             '-CA', str(self.d / 'ca.crt'), '-CAkey', str(self.d / 'ca.key'),
             '-CAcreateserial', '-days', '1', '-extfile', str(self.d / 'extensions'),
             '-out', str(self.d / 'server.crt')])
        for name in ('server.key', 'server.crt', 'ca.crt'):
            shutil.copyfile(self.d / name, broker / name)
            (broker / name).chmod(0o600)
        (broker / 'broker.conf').write_text('listener 8883\nallow_anonymous false\n'
            'cafile /fixture/ca.crt\ncertfile /fixture/server.crt\nkeyfile /fixture/server.key\n'
            'plugin /usr/lib/mosquitto_dynamic_security.so\n'
            'plugin_opt_config_file /security/dynsec.json\n'
            'log_dest stdout\nlog_type error\nlog_type warning\n')
        p = self.passwords['admin']
        self.setup('umask 077; mkdir -p /security; chmod 700 /security; '
            'mosquitto_ctrl dynsec init /security/dynsec.json admin >/dev/null; '
            'chmod 600 /security/dynsec.json; chown -R 1883:1883 /security /fixture',
            (p + '\n' + p + '\n').encode())
        self.command('up', '-d', 'db')
        deadline = time.monotonic() + 15
        # Require completion of initdb's temporary-server lifecycle, not SELECT
        # against its short-lived bootstrap socket.
        while True:
            logs = self.command('logs', '--no-color', 'db', capture=True)
            if b'init process complete; ready for start up' in logs:
                break
            require(time.monotonic() < deadline, 'DB init bound')
            time.sleep(.2)
        while True:
            try:
                self.sql('SELECT 1;')
                break
            except RuntimeError:
                require(time.monotonic() < deadline, 'DB startup bound')
                time.sleep(.2)
        self.sql(Path(__file__).with_name('task260_revoke_reconcile_fixture.sql').read_text())

    def snapshot(self, gateway):
        require(gateway in ('A', 'B'), 'snapshot target bound')
        # Only helper accesses full native JSON. stdout has only a boolean.
        raw = self.setup('/snapshot ' + gateway).strip()
        require(raw in (b'true', b'false'), 'snapshot safe output')
        return raw == b'true'

    def job(self, operation):
        require(bool(re.fullmatch('[a-f0-9]{32}', operation)), 'job identity')
        return self.sql("SELECT status FROM jobs WHERE operation='" + operation + "';")

    def apply(self, manager, operation, gateway, discard=False):
        require(resolve_commit(self.job(operation)) == 'committed', 'no confirmed intent')
        require(self.sql("SELECT gateway FROM jobs WHERE operation='" + operation + "';") == gateway,
                'intent target mismatch')
        manager.request('disableClient', username=gateway, discard=discard)
        require(manager.request('getClient', username=gateway)['client']['disabled'] is True,
                'RAM disabled proof')
        self.probe(gateway, accepted=False)
        if not self.snapshot(gateway):
            print('WARNING RAM-only disable; file old; durable decision retained; pending', flush=True)
            return False
        self.sql("UPDATE jobs SET status='snapshot_observed',attempts=attempts+1 "
                 "WHERE operation='" + operation + "';")
        print('N: persisted snapshot observed, NOT guaranteed power-loss safe', flush=True)
        return True

    def tests(self):
        # These assertions were specified before the first actual run.
        op = uuid.uuid4().hex
        statement = intent_sql(op, 'A')
        self.sql(statement.replace('COMMIT;', 'ROLLBACK;'))
        require(self.job(op) == '' and self.sql("SELECT revoked FROM authority WHERE gateway='A';") == 'f',
                'rollback left partial intent')
        self.sql(statement)
        self.sql(statement)
        try:
            self.sql(intent_sql(op, 'B'))
        except RuntimeError:
            require(self.sql("SELECT revoked FROM authority WHERE gateway='B';") == 'f',
                    'identity conflict changed non-target')
        else:
            raise RuntimeError('operation reused across targets')
        require(self.sql('SELECT count(*) FROM jobs;') == '1', 'replay created logical jobs')
        # Confirm actual PostgreSQL unique constraint rolls back second operation.
        try:
            self.sql(intent_sql(uuid.uuid4().hex, 'A'))
        except RuntimeError:
            pass
        else:
            raise RuntimeError('unique job constraint missing')
        # Lost COMMIT response seam: actual COMMIT, discard receipt, NEW psql connection.
        require(resolve_commit(self.job(op)) == 'committed', 'new connection outcome resolution')
        print('PASS real PG rollback/commit/replay/unique job; lost receipt seam queried anew', flush=True)
        admin = self.connect('admin')
        admin.subscribe(RESPONSE)
        admin.request('setDefaultACLAccess', acls=[{'acltype': key, 'allow': False}
            for key in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')])
        self.role(admin, 'management', [('publishClientSend', CONTROL),
                  ('publishClientReceive', RESPONSE), ('subscribeLiteral', RESPONSE)])
        self.provision(admin, 'manager', 'management')
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        manager.request('removeClientRole', username='admin', rolename='admin')
        manager.request('deleteRole', rolename='admin')
        for user in ('A', 'B'):
            self.role(manager, 'gateway_' + user, [('publishClientSend', 'gateways/' + user + '/telemetry/#')])
            self.provision(manager, user, 'gateway_' + user)
        a = self.connect('A')
        a.subscribe(CONTROL + '/#', allowed=False)
        a.publish(CONTROL, b'{"commands":[{"command":"disableClient","username":"B"}]}')
        require(not manager.request('getClient', username='B')['client'].get('disabled', False),
                'Gateway gained control')
        # Actual permission failure: native save fails while plugin reports success.
        self.setup('chmod 500 /security')
        require(not self.apply(manager, op, 'A'), 'fault unexpectedly persisted')
        self.disconnected(a)
        self.probe('B')
        for ca, hostname in ((self.d / 'wrong-ca.crt', 'localhost'),
                             (self.d / 'ca.crt', 'wrong-host.invalid')):
            raw = socket.create_connection(('127.0.0.1', self.port), 2)
            try:
                ssl.create_default_context(cafile=str(ca)).wrap_socket(raw, server_hostname=hostname)
            except ssl.SSLCertVerificationError:
                pass
            else:
                raise RuntimeError('TLS trust/hostname bypass')
            finally:
                raw.close()
        print('PASS wrong CA/hostname rejection; not interpreted as auth rejection', flush=True)
        require(self.job(op) == 'pending', 'fault cleared pending')
        logs = self.command('logs', '--no-color', 'broker', capture=True)
        require(b'File is not writable' in logs, 'real save fault absent')
        self.restart()
        self.probe('A')
        require(self.sql("SELECT revoked FROM authority WHERE gateway='A';") == 't',
                'durable decision lost after crash')
        print('DANGER controlled UNGUARDED restart: old credential accepted despite DB revoked', flush=True)
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        self.setup('chmod 700 /security')
        # Lost application response AFTER correlated reply, not wire packet loss.
        try:
            self.apply(manager, op, 'A', discard=True)
        except TimeoutError:
            pass
        else:
            raise RuntimeError('lost MQTT response seam not triggered')
        require(self.job(op) == 'pending', 'lost response cleared intent')
        require(manager.request('getClient', username='A')['client']['disabled'], 'lost reply readback')
        for delay in retry_delays():
            time.sleep(delay)
            if self.apply(manager, op, 'A'):
                break
        require(self.job(op) == 'snapshot_observed', 'bounded retry failed')
        self.probe('B')
        require(self.sql('SELECT count(*) FROM jobs;') == '1', 'retry duplicated logical jobs')
        for _ in range(2):
            self.restart()
            self.probe('A', accepted=False)
            self.probe('B')
            require(self.snapshot('A'), 'snapshot restart lost')
        print('PASS operator repair/retry, repeated writable restarts; B healthy', flush=True)
        # Actual DB offline query is unknown, never not-performed.
        self.command('stop', '-t', '1', 'db')
        try:
            self.job(op)
        except RuntimeError:
            require(resolve_commit(None) == 'unknown', 'DB outage inferred no revoke')
        else:
            raise RuntimeError('DB offline fault not injected')
        print('PASS actual DB offline classification unknown; gate behavior NOT RUN', flush=True)
        self.scan()
        transcript = self.transcript.read_bytes()
        require(not any(p.encode() in transcript for p in self.passwords.values()),
                'secret in current transcript')
