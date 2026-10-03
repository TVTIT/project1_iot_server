#!/usr/bin/env python3
"""Test-first bounded isolated lifecycle gate acceptance; no deployment adoption."""
import json
import os
from pathlib import Path
import shutil
import socket
import threading
import time
import uuid

from stage2_mosquitto_runtime import IMAGE, mqtt_connection, run
from task26_harness_helpers import CONTROL, RESPONSE, require, build_helper
from task26_revoke_cases import RevokeSpike, intent_sql

COMPOSE = Path(__file__).with_name('compose.task260-ingress-gate.yml')


class GateSpike(RevokeSpike):
    def __init__(self, directory):
        super().__init__(directory)
        self.project = 'ingress_gate_' + uuid.uuid4().hex[:12]
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-p',
                        self.project, '-f', str(COMPOSE)]
        self.stop_loop = threading.Event()
        self.phase = 'not-started'
        self.observations = []
        self.log_baseline = 0
        for key in ('GATE_PORT', 'TUNNEL_PORT'):
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                self.env[key] = str(reservation.getsockname()[1])

    def port_of(self, service, port):
        return int(self.command('port', service, str(port), capture=True).decode().strip().rsplit(':', 1)[1])

    def prepare(self):
        super().prepare()
        run(['docker', 'run', '--rm', '--network', 'none', '-v',
             str(self.d / 'broker') + ':/owned', '--entrypoint', '/bin/sh', IMAGE,
             '-ec', 'chown -R ' + str(os.getuid()) + ':' + str(os.getgid()) + ' /owned'])
        build_helper('gate', self.d)
        (self.d / 'gate').chmod(0o555)
        shutil.copyfile(self.d / 'db-password', self.d / 'broker/db-password')
        (self.d / 'broker/manager-password').write_text(self.passwords['manager'])
        conf = (self.d / 'broker/broker.conf').read_text()
        (self.d / 'broker/ungated.conf').write_text(conf)
        (self.d / 'broker/broker.conf').write_text(conf.replace('listener 8883', 'listener 18884 127.0.0.1'))
        self.setup('chown -R 1883:1883 /fixture; chmod 600 /fixture/*')

    def bootstrap_negative(self):
        self.command('up', '-d', 'ungated')
        self.port = self.port_of('ungated', 8883)
        deadline = time.monotonic() + 10
        while True:
            try:
                admin = self.connect('admin')
                break
            except (OSError, RuntimeError):
                require(time.monotonic() < deadline, 'negative startup bound')
                time.sleep(.1)
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
        # Only native store inside owned volume; never a hash-bearing artifact.
        self.setup('cp /security/dynsec.json /security/old.json; chmod 600 /security/old.json')
        self.operation = uuid.uuid4().hex
        self.sql(intent_sql(self.operation, 'A'))
        self.probe('A')
        require(not self.snapshot('A'), 'negative store must remain enabled')
        print('PASS negative control: durable PG revoke + old enabled native store accepts A', flush=True)
        self.command('kill', '-s', 'SIGKILL', 'ungated')
        self.command('rm', '-f', 'ungated')

    def old_store(self):
        self.setup('chmod 700 /security; cp /security/old.json /security/dynsec.json; '
                   'chmod 600 /security/dynsec.json; chown 1883:1883 /security/dynsec.json')

    def start_gate(self, fault=''):
        self.env['GATE_FAULT'] = fault
        self.command('up', '-d', '--force-recreate', 'broker')
        self.port = int(self.env['GATE_PORT'])
        self.tunnel_port = int(self.env['TUNNEL_PORT'])
        self.log_baseline = 0

    def restart_existing(self):
        self.log_baseline = len(self.command('logs', '--no-color', 'broker', capture=True))
        self.command('start', 'broker')

    def wait_log(self, marker, bound=20):
        deadline = time.monotonic() + bound
        while time.monotonic() < deadline:
            logs = self.command('logs', '--no-color', 'broker', capture=True)[self.log_baseline:]
            if marker.encode() in logs:
                return logs
            time.sleep(.1)
        raise RuntimeError('gate log bound: ' + marker)

    def racing(self):
        while not self.stop_loop.is_set():
            for port in (getattr(self, 'port', 0), getattr(self, 'tunnel_port', 0)):
                if not port:
                    continue
                phase = self.phase
                accepted = False
                try:
                    mqtt_connection(port, self.d / 'ca.crt', 'A', self.passwords['A']).close()
                    accepted = True
                except (OSError, RuntimeError, EOFError):
                    pass
                self.observations.append((phase, accepted))
            time.sleep(.01)

    def closed(self):
        for port in (self.port, self.tunnel_port):
            try:
                mqtt_connection(port, self.d / 'ca.crt', 'B', self.passwords['B']).close()
            except (OSError, RuntimeError, EOFError):
                pass
            else:
                raise RuntimeError('healthy Gateway admitted while closed')

    def tests(self):
        self.bootstrap_negative()
        # Create container once to fix both random host mappings BEFORE racing
        # begins. Later restarts preserve mappings; recreation updates mappings.
        self.env['GATE_FAULT'] = 'after-query'
        self.command('create', 'broker')
        self.port = int(self.env['GATE_PORT'])
        self.tunnel_port = int(self.env['TUNNEL_PORT'])
        thread = threading.Thread(target=self.racing, daemon=True)
        self.phase = 'mid-reconcile'
        thread.start()
        try:
            self.command('start', 'broker')
            self.wait_log('QUERY_VERIFIED')
            self.closed()
            self.command('kill', '-s', 'SIGKILL', 'broker')
            require(self.job(self.operation) == 'pending', 'query-before-crash lost pending intent')
            self.old_store()
            # Same retained stale state cannot authorize a new lifetime; no ready
            # files are consumed. Old nonce tested by Go unit tests as well.
            self.phase = 'restart-verified'
            self.start_gate()
            first = self.wait_log('OPEN').decode().split('OPEN ')[-1].strip()
            self.probe('A', accepted=False)
            self.probe('B')
            original = self.port
            self.port = self.tunnel_port
            self.probe('A', accepted=False)
            self.probe('B')
            self.port = original
            b = self.connect('B')
            b.subscribe(CONTROL + '/#', allowed=False)
            b.publish(CONTROL, b'{"commands":[{"command":"enableClient","username":"A"}]}')
            time.sleep(.2)
            self.probe('A', accepted=False)
            require(self.snapshot('A') and self.job(self.operation) == 'snapshot_observed', 'readback job mismatch')
            print('PASS host TLS + tunnel simulation + healthy B + control ACL + durable job', flush=True)
            # Gateway network probes container IP, not loopback: even healthy B
            # cannot CONNECT to the private management listener.
            cid = self.command('ps', '-q', 'broker', capture=True).decode().strip()
            address = run(['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', cid], capture=True).decode().strip()
            for port in (18884, 8883):
                if port == 8883:
                    continue
                try:
                    socket.create_connection((address, port), .5).close()
                except OSError:
                    pass
                else:
                    raise RuntimeError('management reachable from external network')
            print('PASS external Gateway management TCP network denial (loopback only)', flush=True)
            # Separate unprivileged container on the same bridge: more hostile
            # than a segregated Gateway network. Must not reach management even
            # with old credentials, since it cannot establish TCP at all.
            run(['docker', 'run', '--rm', '--network', self.project + '_management',
                 '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                 '--entrypoint', '/bin/sh', IMAGE, '-ec',
                 'if nc -z -w 1 broker 18884; then exit 1; fi; '
                 'nc -z -w 1 broker 8883'])
            print('PASS independent same-bridge Gateway TCP denied management; public proxy reachable', flush=True)
            # Actual independent child SIGKILL while old credential CONNECT loop
            # is running. No autonomous replacement exists; old open proxy may
            # briefly accept TCP but only forwards to dead child, never new store.
            self.phase = 'child-SIGKILL'
            self.log_baseline = len(self.command('logs', '--no-color', 'broker', capture=True))
            self.command('exec', '-T', 'broker', 'sh', '-ec', 'kill -9 $(pidof mosquitto)')
            self.wait_log('CLOSED child-death')
            self.closed()
            for index in range(2):
                self.old_store()
                self.phase = 'wrapper-restart-' + str(index)
                self.restart_existing()
                current = self.wait_log('OPEN').decode().split('OPEN ')[-1].strip()
                require(current != first, 'nonce inherited across lifetime')
                self.probe('A', accepted=False)
                self.probe('B')
                self.command('kill', '-s', 'SIGKILL', 'broker')
                self.closed()
                first = current
            # Store fault must NOT mark snapshot_observed on this attempt.
            self.sql("UPDATE jobs SET status='pending' WHERE gateway='A';")
            self.old_store()
            self.setup('chmod 500 /security')
            self.phase = 'save-fault'
            self.restart_existing()
            self.wait_log('BLOCKED reconcile')
            self.closed()
            require(self.job(self.operation) == 'pending' and not self.snapshot('A'), 'save fault erased intent')
            print('PASS save fault => closed, pending, old enabled file', flush=True)
            self.old_store()
            self.phase = 'DB-offline'
            self.command('stop', '-t', '1', 'db')
            self.restart_existing()
            self.wait_log('BLOCKED reconcile')
            self.closed()
            self.command('up', '-d', 'db')
            deadline = time.monotonic() + 10
            while True:
                try:
                    self.sql('SELECT 1;')
                    break
                except RuntimeError:
                    require(time.monotonic() < deadline, 'DB recovery bound')
                    time.sleep(.2)
            self.phase = 'authority-corrupt'
            self.sql("DELETE FROM jobs WHERE gateway='A';")
            self.restart_existing()
            self.wait_log('BLOCKED reconcile')
            self.closed()
            self.sql(intent_sql(self.operation, 'A'))
            self.phase = 'controller-absent'
            self.command('kill', '-s', 'SIGKILL', 'broker')
            self.closed()
            time.sleep(.5)
            self.phase = 'final-recovery'
            self.restart_existing()
            self.wait_log('OPEN')
            self.probe('A', accepted=False)
            self.probe('B')
            time.sleep(.5)
        finally:
            self.stop_loop.set()
            thread.join(6)
            require(not thread.is_alive(), 'race probe shutdown bound')
        phases = {}
        for phase, accepted in self.observations:
            require(not accepted, 'continuous old credential accepted: ' + phase)
            phases[phase] = phases.get(phase, 0) + 1
        require(all(phases.get(p, 0) > 0 for p in ('mid-reconcile', 'child-SIGKILL', 'DB-offline', 'save-fault', 'authority-corrupt', 'controller-absent', 'final-recovery')), 'missing racing phase')
        print('PASS continuous external old-secret CONNECT attempts: ' + json.dumps(phases, sort_keys=True), flush=True)
        (self.artifacts / 'observations.json').write_text(json.dumps(phases, sort_keys=True))
        # Collect secret-free broker logs only after scanning exact secrets and
        # native password/hash JSON field names. Never store full native JSON.
        logs = self.command('logs', '--no-color', capture=True)
        transcript = self.transcript.read_bytes()
        for raw in (logs, transcript):
            require(not any(p.encode() in raw for p in self.passwords.values()), 'secret log leak')
            require((self.d / 'db-password').read_bytes() not in raw, 'DB secret log leak')
            require(b'"password"' not in raw and b'"salt"' not in raw, 'native JSON log leak')
        (self.artifacts / 'safe-container.log').write_bytes(logs)
