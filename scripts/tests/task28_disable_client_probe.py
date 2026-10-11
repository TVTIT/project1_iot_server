#!/usr/bin/env python3
"""Task 2.8 Native capability probe: DynSec disableClient disconnect and reconnect denial.

Runs isolated tests on the pinned/current Mosquitto Docker image first.
Only falls back to eclipse-mosquitto:latest if the capability test fails on a functioning broker.
Strict fixture boundaries, sanitized metadata, RAM-only credentials, and exact-ID cleanup.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import struct
import subprocess
import time
import uuid

import stage2_mosquitto_runtime as runtime
from task26_harness_helpers import CONTROL, RESPONSE, MQTT, require, text

COMPOSE_BASE = Path(__file__).with_name('compose.task260-dynsec.yml')


def exact_connack_code(sock: socket.socket) -> int:
    """Read exact 4-byte MQTT 3.1.1 CONNACK and return return_code."""
    buf = bytearray()
    while len(buf) < 4:
        chunk = sock.recv(4 - len(buf))
        if not chunk:
            raise EOFError(f'connection closed after reading {len(buf)} bytes of CONNACK')
        buf.extend(chunk)
    require(len(buf) == 4 and buf[0] == 0x20 and buf[1] == 0x02, 'invalid CONNACK header')
    return buf[3]


class NativeTargetConnection:
    """Bounded MQTT 3.1.1 client with explicit ClientID, keepalive, and disconnect observation."""
    def __init__(self, port: int, ca_file: Path, user: str, password: str, client_id: str, keepalive: int = 60):
        self.port = port
        self.ca_file = ca_file
        self.user = user
        self.password = password
        self.client_id = client_id
        self.keepalive = keepalive
        self.pid = 0

        context = ssl.create_default_context(cafile=str(self.ca_file))
        raw_sock = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        self.sock = context.wrap_socket(raw_sock, server_hostname='localhost')
        self.sock.settimeout(5)

        body = (text('MQTT') +
                struct.pack('!BBH', 4, 0xC2, self.keepalive) +
                text(self.client_id) +
                text(self.user) +
                text(self.password))

        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.sock.sendall(bytes([0x10]) + remaining + body)
        code = exact_connack_code(self.sock)
        require(code == 0x00, f'target connect refused with code {code}')

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def send(self, header: int, body: bytes):
        require(len(body) <= 16384, 'payload size bound')
        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.sock.sendall(bytes([header]) + remaining + body)

    def exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise EOFError('connection closed')
            data.extend(chunk)
        return bytes(data)

    def packet(self) -> tuple[int, bytes]:
        header = self.exact(1)[0]
        size = 0
        for i in range(4):
            digit = self.exact(1)[0]
            size += (digit & 127) << (7 * i)
            if not (digit & 128):
                require(size <= 65536, 'response size bound')
                return header, self.exact(size)
        raise RuntimeError('invalid frame')

    def subscribe(self, topic: str, allowed: bool = True):
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self.send(0x82, pid + text(topic) + b'\x00')
        header, body = self.packet()
        require(header == 0x90 and body == pid + (b'\x00' if allowed else b'\x80'),
                'exact SUBACK mismatch')

    def publish(self, topic: str, payload: bytes):
        self.send(0x30, text(topic) + payload)

    def message(self) -> tuple[str, bytes]:
        header, body = self.packet()
        require(header == 0x30, 'unexpected/retained response frame')
        n = struct.unpack('!H', body[:2])[0]
        return body[2:2 + n].decode(), body[2 + n:]

    def ping(self):
        self.send(0xC0, b'')
        header, body = self.packet()
        require(header == 0xD0 and len(body) == 0, 'PINGRESP mismatch')

    def wait_for_disconnect(self, timeout_sec: float = 3.0) -> tuple[bool, str, float]:
        """Observes actual EOF or connection reset within timeout_sec."""
        start = time.monotonic()
        self.sock.settimeout(timeout_sec)
        try:
            data = self.sock.recv(1024)
            elapsed = time.monotonic() - start
            if len(data) == 0:
                return True, 'EOF (server closed TLS)', elapsed
            return False, f'unexpected data received: {len(data)} bytes', elapsed
        except (ConnectionResetError, ssl.SSLEOFError) as e:
            elapsed = time.monotonic() - start
            return True, f'transport reset ({type(e).__name__})', elapsed
        except socket.timeout:
            elapsed = time.monotonic() - start
            return False, 'timeout (connection remained open)', elapsed
        except Exception as e:
            elapsed = time.monotonic() - start
            return False, f'unexpected error ({type(e).__name__})', elapsed
        finally:
            self.sock.settimeout(5)


def probe_raw_connack(port: int, ca_file: Path, user: str, password: str, client_id: str) -> int:
    """Attempt a fresh TLS CONNECT and return the exact CONNACK return code."""
    context = ssl.create_default_context(cafile=str(ca_file))
    raw_sock = socket.create_connection(('127.0.0.1', port), timeout=5)
    wrapped = context.wrap_socket(raw_sock, server_hostname='localhost')
    wrapped.settimeout(5)
    try:
        body = (text('MQTT') +
                struct.pack('!BBH', 4, 0xC2, 60) +
                text(client_id) +
                text(user) +
                text(password))

        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        wrapped.sendall(bytes([0x10]) + remaining + body)
        code = exact_connack_code(wrapped)
        return code
    finally:
        wrapped.close()


class DynSecProbeFixture:
    """Manages an isolated Compose fixture with exact-ID resource tracking."""
    def __init__(self, base_dir: Path, image_ref: str):
        self.d = base_dir.resolve()
        self.image_ref = image_ref
        self.project = 'dynsec_probe_' + uuid.uuid4().hex[:12]
        self.env = dict(runtime.ENV, DYNSEC_FIXTURE=str(self.d), COMPOSE_DISABLE_ENV_FILE='1')
        self.compose_file = self.d / 'compose.probe.yml'
        self.passwords = {
            'admin': secrets.token_urlsafe(32),
            'target': secrets.token_urlsafe(32),
            'observer': secrets.token_urlsafe(32),
        }
        self.port: int | None = None
        self.open_connections: list[NativeTargetConnection | MQTT] = []

        # Exact receipts captured during lifecycle
        self.owned_containers: list[str] = []
        self.owned_volumes: list[str] = []
        self.owned_networks: list[str] = []

    def _compose_cmd(self, *args: str) -> list[str]:
        return ['docker', 'compose', '--env-file', '/dev/null', '-p',
                self.project, '-f', str(self.compose_file)] + list(args)

    def prepare(self):
        require(self.d.is_dir(), 'fixture directory must exist')
        broker_dir = self.d / 'broker'
        broker_dir.mkdir(mode=0o700, exist_ok=True)

        # Generate custom compose file with specified image_ref
        content = COMPOSE_BASE.read_text()
        content = content.replace('image: eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9',
                                  f'image: {self.image_ref}')
        self.compose_file.write_text(content)
        self.compose_file.chmod(0o600)

        # Generate CA and server certificates
        for args in (
            ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=isolated-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
             '-keyout', 'ca.key', '-out', 'ca.crt'],
            ['req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
             '-keyout', 'server.key', '-out', 'server.csr'],
        ):
            fixed_args = [str(self.d / a) if a.endswith(('.key', '.crt', '.csr')) else a for a in args]
            runtime.run(['openssl'] + fixed_args)

        (self.d / 'extensions').write_text('subjectAltName=DNS:localhost\n'
                                           'extendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n')
        runtime.run(['openssl', 'x509', '-req', '-in', str(self.d / 'server.csr'),
                     '-CA', str(self.d / 'ca.crt'), '-CAkey', str(self.d / 'ca.key'),
                     '-CAcreateserial', '-days', '1', '-extfile', str(self.d / 'extensions'),
                     '-out', str(self.d / 'server.crt')])

        for name in ('server.key', 'server.crt', 'ca.crt'):
            shutil.copyfile(self.d / name, broker_dir / name)

        (broker_dir / 'broker.conf').write_text(
            'listener 8883\nallow_anonymous false\n'
            'cafile /fixture/ca.crt\ncertfile /fixture/server.crt\nkeyfile /fixture/server.key\n'
            'plugin /usr/lib/mosquitto_dynamic_security.so\n'
            'plugin_opt_config_file /security/dynsec.json\n'
            'log_dest stdout\nlog_type error\nlog_type warning\n'
        )

        for p in self.d.iterdir():
            if p.is_file():
                p.chmod(0o600)

        # Offline dynsec init via setup service with stdin password
        admin_pw = self.passwords['admin']
        script = ('umask 077; mkdir -p /security; chmod 700 /security; '
                  'mosquitto_ctrl dynsec init /security/dynsec.json admin >/dev/null; '
                  'chmod 600 /security/dynsec.json; chown -R 1883:1883 /security /fixture')
        stdin_data = (admin_pw + '\n' + admin_pw + '\n').encode()

        setup_cmd = self._compose_cmd('run', '--rm', '-T', 'setup', '-ec', script)
        runtime.run(setup_cmd, data=stdin_data, env=self.env)

        # Capture owned volume after setup run
        v_out = runtime.run(['docker', 'volume', 'ls', '-q', '--filter',
                             f'label=com.docker.compose.project={self.project}'], capture=True)
        for v in v_out.decode().splitlines():
            v = v.strip()
            if v and v not in self.owned_volumes:
                self.owned_volumes.append(v)

    def start(self):
        up_cmd = self._compose_cmd('up', '-d', 'broker')
        runtime.run(up_cmd, env=self.env)

        port_cmd = self._compose_cmd('port', 'broker', '8883')
        port_raw = runtime.run(port_cmd, env=self.env, capture=True).decode().strip()
        self.port = int(port_raw.rsplit(':', 1)[1])

        # Capture running container ID
        c_out = runtime.run(['docker', 'ps', '-q', '--filter',
                             f'label=com.docker.compose.project={self.project}'], capture=True)
        for c in c_out.decode().splitlines():
            c = c.strip()
            if c and c not in self.owned_containers:
                self.owned_containers.append(c)

        # Capture network ID
        n_out = runtime.run(['docker', 'network', 'ls', '-q', '--filter',
                             f'label=com.docker.compose.project={self.project}'], capture=True)
        for n in n_out.decode().splitlines():
            n = n.strip()
            if n and n not in self.owned_networks:
                self.owned_networks.append(n)

        # Wait for broker readiness with admin probe
        deadline = time.monotonic() + 10
        ready = False
        while time.monotonic() < deadline:
            try:
                conn = runtime.mqtt_connection(self.port, self.d / 'ca.crt', 'admin', self.passwords['admin'], accepted=True)
                conn.close()
                ready = True
                break
            except (OSError, RuntimeError):
                time.sleep(0.1)
        require(ready, 'broker startup readiness timeout')

    def restart(self):
        # Controlled restart: kill broker with SIGTERM/stop then start again
        self._compose_cmd('stop', 'broker')
        runtime.run(self._compose_cmd('stop', 'broker'), env=self.env)
        self.start()

    def cleanup(self) -> dict:
        """Exact-receipt teardown with post-cleanup inspection."""
        receipt = {
            'containers_before': list(self.owned_containers),
            'volumes_before': list(self.owned_volumes),
            'networks_before': list(self.owned_networks),
            'containers_remaining': [],
            'volumes_remaining': [],
            'networks_remaining': [],
            'clean': False
        }

        # 1. Close open sockets
        for conn in self.open_connections:
            try:
                conn.close()
            except Exception:
                pass
        self.open_connections.clear()

        # 2. Stop and remove owned containers by exact ID
        for cid in self.owned_containers:
            try:
                subprocess.run(['docker', 'stop', '-t', '2', cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.run(['docker', 'rm', '-f', cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

        # 3. Down the compose project to ensure network/volume detached cleanly
        try:
            down_cmd = self._compose_cmd('down', '-v')
            subprocess.run(down_cmd, env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

        # 4. Remove owned volumes by exact name/ID
        for vid in self.owned_volumes:
            try:
                subprocess.run(['docker', 'volume', 'rm', '-f', vid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

        # 5. Remove owned networks by exact name/ID
        for nid in self.owned_networks:
            try:
                subprocess.run(['docker', 'network', 'rm', nid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

        # 6. Verify exact resources are gone via exact docker inspect
        for cid in self.owned_containers:
            res = subprocess.run(['docker', 'inspect', '--type=container', cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if res.returncode == 0:
                receipt['containers_remaining'].append(cid)

        for vid in self.owned_volumes:
            res = subprocess.run(['docker', 'inspect', '--type=volume', vid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if res.returncode == 0:
                receipt['volumes_remaining'].append(vid)

        for nid in self.owned_networks:
            res = subprocess.run(['docker', 'inspect', '--type=network', nid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if res.returncode == 0:
                receipt['networks_remaining'].append(nid)

        receipt['clean'] = (
            len(receipt['containers_remaining']) == 0 and
            len(receipt['volumes_remaining']) == 0 and
            len(receipt['networks_remaining']) == 0
        )
        return receipt


def run_capability_probe(image_ref: str, tmp_base: Path, test_persistence: bool = True) -> dict:
    """Executes the exact native capability test sequence for a given image reference."""
    results = {
        'image_ref': image_ref,
        'image_id': None,
        'broker_version': None,
        'step1_init_and_admin': False,
        'step2_create_client_and_role': False,
        'step3_dual_session_functional': False,
        'step4_disable_command_and_readback': False,
        'step5_active_sessions_disconnected': False,
        'step5_disconnect_details': {},
        'step5_observer_unaffected': False,
        'step6_same_password_rejected': False,
        'step6_connack_code': None,
        'step7_persistence_check': False,
        'cleanup_receipt': None,
        'pass_all_core': False
    }

    # Inspect image ID
    insp = subprocess.run(['docker', 'inspect', image_ref, '--format', '{{.Id}}'],
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    if insp.returncode == 0:
        results['image_id'] = insp.stdout.strip()

    # Get broker version string from image
    v_proc = subprocess.run(['docker', 'run', '--rm', image_ref, 'mosquitto', '-h'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in v_proc.stdout.splitlines():
        if 'mosquitto version' in line:
            results['broker_version'] = line.strip()
            break

    fixture_dir = tmp_base / f'fixture_{uuid.uuid4().hex[:8]}'
    fixture_dir.mkdir(parents=True, mode=0o700)
    fixture = DynSecProbeFixture(fixture_dir, image_ref)

    try:
        # Step 1: Bootstrap & Start
        fixture.prepare()
        fixture.start()
        results['step1_init_and_admin'] = True

        # Helper wrapper for admin MQTT control
        class FakeSpike:
            def __init__(self, port, d):
                self.port = port
                self.d = d

        fake_spike = FakeSpike(fixture.port, fixture.d)
        admin = MQTT(fake_spike, 'admin', fixture.passwords['admin'])
        fixture.open_connections.append(admin)
        admin.subscribe(RESPONSE)

        # Step 2: Create target client, observer client, and roles
        # Create roles for target and observer
        admin.request('createRole', rolename='target_role')
        admin.request('addRoleACL', rolename='target_role', acltype='publishClientSend', topic='probe/target/#', allow=True, priority=1)
        admin.request('addRoleACL', rolename='target_role', acltype='publishClientReceive', topic='probe/target/#', allow=True, priority=1)
        admin.request('addRoleACL', rolename='target_role', acltype='subscribePattern', topic='probe/target/#', allow=True, priority=1)

        admin.request('createRole', rolename='observer_role')
        admin.request('addRoleACL', rolename='observer_role', acltype='publishClientSend', topic='probe/observer/#', allow=True, priority=1)
        admin.request('addRoleACL', rolename='observer_role', acltype='publishClientReceive', topic='probe/observer/#', allow=True, priority=1)
        admin.request('addRoleACL', rolename='observer_role', acltype='subscribePattern', topic='probe/observer/#', allow=True, priority=1)

        # Create target client: createClient -> setClientPassword -> addClientRole
        target_user = 'target_client'
        target_pw = fixture.passwords['target']
        admin.request('createClient', username=target_user)
        admin.request('setClientPassword', username=target_user, password=target_pw)
        admin.request('addClientRole', username=target_user, rolename='target_role')

        # Create observer client
        observer_user = 'observer_client'
        observer_pw = fixture.passwords['observer']
        admin.request('createClient', username=observer_user)
        admin.request('setClientPassword', username=observer_user, password=observer_pw)
        admin.request('addClientRole', username=observer_user, rolename='observer_role')

        # Connect observer
        observer = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', observer_user, observer_pw, 'obs_1')
        fixture.open_connections.append(observer)
        observer.subscribe('probe/observer/echo')

        # Connect TWO target devices with different client IDs sharing target_user/target_pw
        target_dev1 = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_1')
        target_dev2 = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_2')
        fixture.open_connections.append(target_dev1)
        fixture.open_connections.append(target_dev2)
        results['step2_create_client_and_role'] = True

        # Step 3: Prove both target connections function immediately before disable
        target_dev1.subscribe('probe/target/dev1')
        target_dev2.subscribe('probe/target/dev2')

        # Bidirectional message exchanges
        msg1 = b'ping_from_dev1_' + uuid.uuid4().hex[:8].encode()
        target_dev1.publish('probe/target/dev1', msg1)
        top1, rcv1 = target_dev1.message()
        require(top1 == 'probe/target/dev1' and rcv1 == msg1, 'target 1 echo delivery failed')

        msg2 = b'ping_from_dev2_' + uuid.uuid4().hex[:8].encode()
        target_dev2.publish('probe/target/dev2', msg2)
        top2, rcv2 = target_dev2.message()
        require(top2 == 'probe/target/dev2' and rcv2 == msg2, 'target 2 echo delivery failed')

        # Keep alive check
        target_dev1.ping()
        target_dev2.ping()
        results['step3_dual_session_functional'] = True

        # Step 4: Send ONE disableClient command and read back
        admin.request('disableClient', username=target_user)
        client_info = admin.request('getClient', username=target_user)
        require(client_info.get('client', {}).get('disabled') is True, 'getClient disabled field not true')
        results['step4_disable_command_and_readback'] = True

        # Step 5: Require actual EOF / connection reset on both already-open target connections
        disc1, detail1, elapsed1 = target_dev1.wait_for_disconnect(timeout_sec=3.0)
        disc2, detail2, elapsed2 = target_dev2.wait_for_disconnect(timeout_sec=3.0)
        results['step5_disconnect_details'] = {
            'dev1': {'disconnected': disc1, 'detail': detail1, 'elapsed_sec': round(elapsed1, 4)},
            'dev2': {'disconnected': disc2, 'detail': detail2, 'elapsed_sec': round(elapsed2, 4)},
        }
        require(disc1 and disc2, f'target connections not disconnected: dev1={disc1}, dev2={disc2}')
        results['step5_active_sessions_disconnected'] = True

        # Verify observer is unaffected and exchanges a fresh payload
        obs_msg = b'obs_payload_' + uuid.uuid4().hex[:8].encode()
        observer.publish('probe/observer/echo', obs_msg)
        obs_top, obs_rcv = observer.message()
        require(obs_top == 'probe/observer/echo' and obs_rcv == obs_msg, 'observer communication failed after target disable')
        results['step5_observer_unaffected'] = True

        # Step 6: Attempt fresh TLS CONNECT with SAME valid password
        connack_code = probe_raw_connack(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_fresh')
        results['step6_connack_code'] = connack_code
        # In MQTT 3.1.1, return code 0x04 = Bad user name or password, 0x05 = Not authorized.
        # Dynsec returns 0x05 or 0x04 for disabled/unauthorized client.
        require(connack_code in (0x04, 0x05), f'expected rejection CONNACK (0x04 or 0x05), got 0x{connack_code:02x}')
        results['step6_same_password_rejected'] = True

        # Core test passed!
        results['pass_all_core'] = True

        # Step 7: Optional persistence check
        if test_persistence:
            fixture.restart()
            fake_spike_after = FakeSpike(fixture.port, fixture.d)
            # Verify admin still connects and can read disabled status
            admin_after = MQTT(fake_spike_after, 'admin', fixture.passwords['admin'])
            fixture.open_connections.append(admin_after)
            admin_after.subscribe(RESPONSE)
            client_info_after = admin_after.request('getClient', username=target_user)
            require(client_info_after.get('client', {}).get('disabled') is True, 'restart: disabled not preserved')

            # Verify same-password rejection persists
            code_after = probe_raw_connack(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_after_restart')
            require(code_after in (0x04, 0x05), f'restart: expected rejection CONNACK, got 0x{code_after:02x}')

            # Verify observer still connects with its password
            obs_after = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', observer_user, observer_pw, 'obs_after')
            fixture.open_connections.append(obs_after)
            obs_after.subscribe('probe/observer/echo')
            obs_after.publish('probe/observer/echo', b'after_restart_msg')
            top_a, rcv_a = obs_after.message()
            require(top_a == 'probe/observer/echo' and rcv_a == b'after_restart_msg', 'observer failed after restart')

            results['step7_persistence_check'] = True

    finally:
        results['cleanup_receipt'] = fixture.cleanup()
        # Clean local temporary fixture files
        try:
            shutil.rmtree(fixture_dir, ignore_errors=True)
        except Exception:
            pass

    return results
