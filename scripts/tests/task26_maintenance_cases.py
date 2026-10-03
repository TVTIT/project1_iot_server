#!/usr/bin/env python3
"""Disposable maintenance rotate proof. Host owns broker lifecycle and both routes.

No deployment API, automatic device delivery, rollback or secret replay. Local
host processes/Docker operator are trusted; public CA only in TLS client probes.
All broker starts are explicit and private, regardless of DB active metadata.
"""
import json
import os
from pathlib import Path
import secrets
import selectors
import socket
import subprocess
import threading
import time
import uuid

from stage2_mosquitto_runtime import IMAGE, mqtt_connection, run
from task26_harness_helpers import RESPONSE, CONTROL, require
from task26_revoke_cases import RevokeSpike

COMPOSE = Path(__file__).with_name('compose.task260-usb-rotate.yml')


def release_allowed(status, live_proof):
    return status == 'active' and live_proof


def replay(status):
    return {'broker': status, 'delivery': 'unknown',
            'action': 'manual_rerotate_if_secret_lost'}


class Proxy:
    """Two disposable routes; close ACK joins all bounded sockets/workers."""
    def __init__(self, upstream):
        self.upstream = upstream
        self.listener = None
        self.sockets = []
        self.workers = []
        self.lock = threading.Lock()

    def open(self):
        require(self.listener is None, 'double open')
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(16)
        listener.settimeout(.1)
        self.listener = listener
        self.port = listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, args=(listener,))
        self.thread.start()

    def accept(self, listener):
        while self.listener is listener:
            try:
                client, _ = listener.accept()
            except (socket.timeout, OSError):
                continue
            with self.lock:
                if self.listener is not listener or len(self.workers) >= 32:
                    client.close()
                    continue
                self.sockets.append(client)
                worker = threading.Thread(target=self.forward, args=(client,))
                self.workers.append(worker)
                worker.start()

    def forward(self, client):
        upstream = None
        try:
            upstream = socket.create_connection(('127.0.0.1', self.upstream), 1)
            with self.lock:
                self.sockets.append(upstream)
            for s in (client, upstream):
                s.setblocking(False)
            with selectors.DefaultSelector() as poll:
                poll.register(client, selectors.EVENT_READ, upstream)
                poll.register(upstream, selectors.EVENT_READ, client)
                deadline = time.monotonic() + 15
                while self.listener is not None and time.monotonic() < deadline:
                    for key, _ in poll.select(.1):
                        data = key.fileobj.recv(65536)
                        if not data:
                            return
                        key.data.settimeout(1)
                        key.data.sendall(data)
                        key.data.setblocking(False)
        except (OSError, ValueError):
            pass
        finally:
            client.close()
            if upstream:
                upstream.close()

    def close(self):
        listener, self.listener = self.listener, None
        if listener:
            listener.close()
            self.thread.join(2)
            require(not self.thread.is_alive(), 'route close accept bound')
        with self.lock:
            for s in self.sockets:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()
        for worker in self.workers:
            worker.join(2)
            require(not worker.is_alive(), 'route drain bound')
        self.sockets.clear()
        self.workers.clear()


class RotateSpike(RevokeSpike):
    def __init__(self, directory):
        super().__init__(directory)
        self.project = 'usb_rotate_' + uuid.uuid4().hex[:12]
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-p',
                        self.project, '-f', str(COMPOSE)]
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            self.port = s.getsockname()[1]
        self.routes = [Proxy(self.port), Proxy(self.port)]
        self.lease = None
        self.phase = 'bootstrap'
        self.samples = []
        self.stop = threading.Event()
        self.released = 0
        self.all_secrets = list(self.passwords.values())

    def prepare(self):
        super().prepare()
        self.setup('chmod 700 /fixture')
        # Restore host ownership only to edit owned generated configuration.
        run(['docker', 'run', '--rm', '--network', 'none', '-v',
             str(self.d / 'broker') + ':/owned', '--entrypoint', '/bin/sh', IMAGE,
             '-ec', 'chown -R ' + str(os.getuid()) + ':' + str(os.getgid()) + ' /owned'])
        conf = self.d / 'broker/broker.conf'
        conf.write_text(conf.read_text().replace('listener 8883',
                        f'listener {self.port} 127.0.0.1'))
        self.setup('chown -R 1883:1883 /fixture; chmod 600 /fixture/*')
        self.sql(Path(__file__).with_name('task260_usb_rotate_fixture.sql').read_text())

    def start_private(self):
        self.command('up', '-d', 'broker')
        deadline = time.monotonic() + 10
        while True:
            try:
                return self.connect('manager' if hasattr(self, 'manager_ready') else 'admin')
            except (OSError, RuntimeError, EOFError):
                require(time.monotonic() < deadline, 'private broker startup bound')
                time.sleep(.1)

    def identity(self):
        cid = self.command('ps', '-q', 'broker', capture=True).decode().strip()
        return run(['docker', 'inspect', '-f', '{{.State.Pid}}:{{.State.StartedAt}}', cid],
                   capture=True).decode().strip()

    def fresh(self):
        before = self.identity()
        self.command('kill', '-s', 'SIGKILL', 'broker')
        manager = self.start_private()
        require(self.identity() != before, 'fresh child identity unchanged')
        manager.subscribe(RESPONSE)
        return manager

    def closed(self):
        for route in self.routes:
            if hasattr(route, 'port'):
                try:
                    socket.create_connection(('127.0.0.1', route.port), .2).close()
                except OSError:
                    continue
                raise RuntimeError('old open proxy bypass')

    def racing(self):
        # Outcome is explicitly TCP_CLOSED vs TLS/MQTT denial vs authenticated.
        # Sampling is evidence, not a mathematical assertion over all packets.
        while not self.stop.is_set():
            for index, route in enumerate(self.routes):
                phase = self.phase
                try:
                    socket.create_connection(('127.0.0.1', route.port), .2).close()
                except OSError:
                    outcome = 'TCP_CLOSED'
                else:
                    try:
                        mqtt_connection(route.port, self.d / 'ca.crt', 'A', self.passwords['A']).close()
                    except (OSError, RuntimeError, EOFError):
                        outcome = 'TLS_OR_MQTT_DENIED'
                    else:
                        outcome = 'AUTHENTICATED'
                self.samples.append((phase, index, outcome))
            self.stop.wait(.02)

    def acquire(self):
        # Dedicated real PostgreSQL SESSION advisory lock. All fixture writers
        # must hold this one lease. No password inherited by broker subprocess.
        self.lease = subprocess.Popen(self.compose + ['exec', '-T', 'db', 'psql',
            '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres'],
            env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        self.lease.stdin.write(b"SELECT CASE WHEN pg_try_advisory_lock(260003) THEN 'owned' ELSE 'busy' END;\n")
        self.lease.stdin.flush()
        with selectors.DefaultSelector() as poll:
            poll.register(self.lease.stdout, selectors.EVENT_READ)
            require(bool(poll.select(5)), 'maintenance lock timeout')
            require(self.lease.stdout.readline().strip() == b'owned', 'maintenance busy')

    def owned(self):
        require(self.lease is not None and self.lease.poll() is None, 'lease lost')
        require(self.sql('SELECT pg_try_advisory_lock(260003);') == 'f', 'lease no longer held')

    def status(self, op):
        require(len(op) == 32 and all(c in '0123456789abcdef' for c in op), 'op identity')
        return self.sql(f"SELECT status FROM rotate_jobs WHERE operation='{op}' AND gateway='A';")

    def rotate(self, fault=''):
        self.owned()
        op = uuid.uuid4().hex
        self.phase = fault or 'healthy-maintenance'
        # Intent committed first. Receipt queried in a NEW connection, even when
        # intentionally discarded. Missing/DB-down never means rolled back.
        self.sql(f"""BEGIN;
        UPDATE rotate_authority SET epoch=epoch+1,admission='maintenance' WHERE gateway='A';
        INSERT INTO rotate_jobs(operation,gateway,epoch,status)
        SELECT '{op}',gateway,epoch,'pending' FROM rotate_authority WHERE gateway='A';
        COMMIT;""")
        require(self.status(op) == 'pending', 'intent confirmation identity')
        for route in self.routes:
            route.close()
        self.closed()
        # Candidate not generated until exclusive intent and route-close ACK.
        candidate = secrets.token_urlsafe(32)
        self.all_secrets.append(candidate)
        self.passwords['new'] = candidate
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        manager.request('disableClient', username='A')
        if fault == 'save-failure':
            self.setup('chmod 500 /security')
        manager.request('setClientPassword', username='A', password=candidate)
        require(manager.request('getClient', username='A')['client']['disabled'], 'RAM target enabled')
        if fault == 'before-probe':
            self.command('kill', '-s', 'SIGKILL', 'broker')
            self.closed()
            return op, None
        manager = self.fresh()
        self.closed()
        manager.request('enableClient', username='A')
        try:
            self.probe('A', candidate)
        except (OSError, RuntimeError, EOFError):
            require(fault == 'save-failure', 'fresh positive failed unexpectedly')
            self.probe('A', self.passwords['A'])  # independent old oracle
            manager.request('disableClient', username='A')
            self.sql(f"UPDATE rotate_jobs SET status='recovery_needed' WHERE operation='{op}';")
            require(self.status(op) == 'recovery_needed', 'failure finalized active')
            self.closed()
            print('PASS native RAM success/save failure: fresh new FAIL, old internal ACCEPT, external TCP CLOSED')
            return op, None
        self.probe('A', self.passwords['A'], accepted=False)
        self.probe('B')
        manager.request('disableClient', username='A')
        require(self.snapshot('A'), 'disabled persisted boundary missing')
        # Second cold load verifies disabled boundary; final enable is private,
        # and every future harness startup still begins with CLOSED proxies.
        manager = self.fresh()
        require(manager.request('getClient', username='A')['client']['disabled'], 'cold disabled lost')
        manager.request('enableClient', username='A')
        self.probe('A', candidate)
        proof_identity = self.identity()
        self.sql(f"UPDATE rotate_jobs SET status='verified' WHERE operation='{op}';")
        if fault in ('after-verify', 'finalize-outage'):
            if fault == 'after-verify':
                self.command('kill', '-s', 'SIGKILL', 'broker')
            else:
                self.command('stop', '-t', '1', 'db')
                try:
                    self.status(op)
                except RuntimeError:
                    pass
                else:
                    raise RuntimeError('DB outage absent')
            self.closed()
            return op, None
        self.owned()
        self.sql(f"""BEGIN;
        UPDATE rotate_jobs SET status='active' WHERE operation='{op}' AND status='verified';
        UPDATE rotate_authority SET admission='active' WHERE gateway='A'
          AND epoch=(SELECT epoch FROM rotate_jobs WHERE operation='{op}'); COMMIT;""")
        # Lost receipt seam deliberately uses NEW connection to query identity.
        confirmed = self.status(op)
        require(release_allowed(confirmed, self.identity() == proof_identity), 'release without live finalized proof')
        if fault == 'after-finalize':
            self.command('kill', '-s', 'SIGKILL', 'broker')
            self.closed()
            require('password' not in json.dumps(replay(confirmed)), 'secret replay')
            return op, None
        # Only fixture memory return represents one-time admin response. No file
        # handoff, simulator auto-delivery or API is implemented.
        self.released += 1
        return op, candidate

    def recover_private(self):
        for route in self.routes:
            route.close()
        self.setup('chmod 700 /security')
        manager = self.start_private()
        manager.subscribe(RESPONSE)
        # Explicit conservative maintenance-only policy, NOT ordinary production
        # active-password startup admission. New operation must reprove candidate.
        manager.request('disableClient', username='A')
        self.sql("UPDATE rotate_authority SET admission='recovery_needed' WHERE gateway='A';")
        self.closed()
        return manager

    def tests(self):
        admin = self.start_private()
        admin.subscribe(RESPONSE)
        admin.request('setDefaultACLAccess', acls=[{'acltype': key, 'allow': False}
            for key in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')])
        self.role(admin, 'management', [('publishClientSend', CONTROL),
            ('publishClientReceive', RESPONSE), ('subscribeLiteral', RESPONSE)])
        self.provision(admin, 'manager', 'management')
        self.manager_ready = True
        manager = self.connect('manager')
        manager.subscribe(RESPONSE)
        manager.request('removeClientRole', username='admin', rolename='admin')
        manager.request('deleteRole', rolename='admin')
        for user in ('A', 'B', 'backend'):
            self.role(manager, 'gateway_' + user, [('publishClientSend', 'gateways/' + user + '/telemetry/#')])
            self.provision(manager, user, 'gateway_' + user)
        self.acquire()
        require(self.sql('SELECT pg_try_advisory_lock(260003);') == 'f', 'concurrent rotate/revoke not rejected')
        print('PASS real PG single-writer advisory ownership; competing rotate/revoke lease rejected')
        for route in self.routes:
            route.open()
        external = mqtt_connection(self.routes[0].port, self.d / 'ca.crt', 'A', self.passwords['A'])
        self.probe('B')
        op, secret = self.rotate()
        require(secret is not None and self.released == 1, 'healthy secret withheld')
        external.settimeout(2)
        require(external.recv(1) == b'', 'existing external connection not drained')
        external.close()
        for route in self.routes:
            route.open()
            mqtt_connection(route.port, self.d / 'ca.crt', 'A', secret).close()
            mqtt_connection(route.port, self.d / 'ca.crt', 'A', self.passwords['A'], False).close()
            mqtt_connection(route.port, self.d / 'ca.crt', 'B', self.passwords['B']).close()
        # Disposable admin tool manually writes a protected fixture device file.
        # This is neither actual USB nor encryption nor automatic password delivery.
        device = self.d / 'device-credential'
        device.write_text(secret)
        device.chmod(0o600)
        mqtt_connection(self.routes[0].port, self.d / 'ca.crt', 'A', device.read_text()).close()
        self.sql(f"UPDATE rotate_jobs SET delivery='device_updated' WHERE operation='{op}';")
        require(self.sql(f"SELECT delivery FROM rotate_jobs WHERE operation='{op}';") == 'device_updated', 'handoff missing')
        device.unlink()
        secret = None
        self.passwords['A'] = self.passwords['new']
        print('PASS healthy fresh changed process/new positive/old negative/B healthy; manual fixture load + reconnect separate device_updated')
        gateway = self.connect('B')
        gateway.subscribe(CONTROL + '/#', allowed=False)
        gateway.publish(CONTROL, b'{"commands":[{"command":"disableClient","username":"A"}]}')
        time.sleep(.1)
        self.probe('A')
        print('PASS Gateway control ACL denied; separate manager and telemetry principals')
        # After OPEN child dies: explicit host supervisor closes both proxies.
        # No autonomous replacement exists. Detection latency is NOT qualified.
        self.command('kill', '-s', 'SIGKILL', 'broker')
        for route in self.routes:
            route.close()
        self.closed()
        print('PASS child SIGKILL after OPEN => explicit controller close ACK; no autonomous replacement')
        self.phase = 'recovery-private'
        thread = threading.Thread(target=self.racing)
        thread.start()
        try:
            for fault in ('save-failure', 'before-probe', 'after-verify', 'after-finalize', 'finalize-outage'):
                self.recover_private()
                before = self.released
                op, value = self.rotate(fault)
                require(value is None and self.released == before, 'fault returned secret')
                if fault != 'finalize-outage':
                    require(self.status(op) == {'before-probe': 'pending', 'after-verify': 'verified',
                        'after-finalize': 'active', 'save-failure': 'recovery_needed'}[fault], 'crash classification')
                print('PASS fault=' + fault + ' external=CLOSED secret_returned=false')
                time.sleep(.1)
                if fault == 'finalize-outage':
                    break
        finally:
            self.stop.set()
            thread.join(5)
            require(not thread.is_alive(), 'sampling shutdown bound')
        summary = {}
        for phase, route, outcome in self.samples:
            require(outcome != 'AUTHENTICATED', 'external old credential sample accepted')
            key = phase + ':route' + str(route) + ':' + outcome
            summary[key] = summary.get(key, 0) + 1
        require(all(any(key.startswith(fault + ':') for key in summary) for fault in
            ('save-failure', 'before-probe', 'after-verify', 'after-finalize', 'finalize-outage')), 'sampling phase absent')
        (self.artifacts / 'observations.json').write_text(json.dumps(summary, sort_keys=True))
        print('PASS continuous two-route old CONNECT samples: ' + json.dumps(summary, sort_keys=True))
        require(self.released == 1, 'secret replay counter')
        logs = self.command('logs', '--no-color', capture=True)
        require(b'File is not writable' in logs, 'native save failure evidence absent')
        transcript = self.transcript.read_bytes()
        self.all_secrets.append((self.d / 'db-password').read_text())
        for raw in (logs, transcript):
            require(not any(p.encode() in raw for p in self.all_secrets), 'secret log leak')
        require(b'"password"' not in logs and b'"salt"' not in logs, 'hash-bearing log')
        (self.artifacts / 'safe-container.log').write_bytes(logs)

    def cleanup(self):
        for route in self.routes:
            route.close()
        if self.lease:
            self.lease.stdin.close()
            try:
                self.lease.wait(3)
            except subprocess.TimeoutExpired:
                self.lease.kill()
                self.lease.wait(3)
            self.lease.stdout.close()
        super().cleanup()
