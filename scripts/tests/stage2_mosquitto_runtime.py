#!/usr/bin/env python3
"""Disposable PoC plus production Go adapter TLS/ACL integration.

Only test secrets, stdin native hashing, fresh TLS MQTT 3.1.1 CONNACK probes.
No deployment .env, Docker socket mounts, host PID, or privileged containers.
"""
from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = Path(__file__).with_name('compose.mosquitto-runtime.yml')
IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
TOOL = '/usr/bin/mosquitto_passwd'
AUTH = '/mosquitto/auth'
CONTROL = '/mosquitto/control'
ENV = {key: os.environ[key] for key in ('PATH', 'HOME', 'DOCKER_HOST',
       'DOCKER_CONTEXT', 'XDG_RUNTIME_DIR') if key in os.environ}

# Minimal test-only helper compiled locally; nothing installed in pinned image.
HELPER = r'''package main
import("bufio";"io";"net";"os";"time")
func check(e error){if e!=nil {panic("PoC helper failed")}}
func main(){
 path:="/mosquitto/control/reload.sock"
 switch os.Args[1] {
 case "reload":
  c,e:=net.DialTimeout("unix",path,2*time.Second);check(e);defer c.Close()
  c.SetDeadline(time.Now().Add(2*time.Second));_,e=c.Write([]byte("reload\n"));check(e)
  check(c.(*net.UnixConn).CloseWrite())
  s,e:=bufio.NewReader(c).ReadString('\n');check(e)
  if s!="signalled\n" {panic("no signal ACK")}
  b,e:=io.ReadAll(c);check(e);if len(b)!=0 {panic("unexpected trailing response")}
 case "swap":
  f,e:=os.OpenFile("/mosquitto/auth/candidate",os.O_RDWR,0);check(e)
  check(f.Sync());check(f.Close())
  check(os.Rename("/mosquitto/auth/candidate","/mosquitto/auth/passwd"))
  d,e:=os.Open("/mosquitto/auth");check(e);check(d.Sync());check(d.Close())
 }
}'''


def run(argv, data=None, *, capture=False, env=None, timeout=30):
    """Never echo subprocess output/errors; credentials only travel via stdin."""
    result = subprocess.run(argv, input=data, env=ENV if env is None else env,
                            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError('subprocess failed')
    return result.stdout if capture else b''


def validate_gateway(username):
    if username == 'backend_service' or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', username):
        raise ValueError('protected or invalid gateway principal')


def assert_connack(packet, accepted):
    expected = b'\x20\x02\x00' + (b'\x00' if accepted else b'\x05')
    if packet != expected:
        raise RuntimeError('expected exact MQTT authentication CONNACK')


def mqtt_connection(port, ca, username, password, accepted=True):
    # create_default_context verifies CA AND localhost SAN; no insecure fallback.
    context = ssl.create_default_context(cafile=str(ca))
    connection = context.wrap_socket(socket.create_connection(('127.0.0.1', port), 2),
                                     server_hostname='localhost')
    connection.settimeout(2)
    def text(value):
        value = value.encode()
        return struct.pack('!H', len(value)) + value
    body = text('MQTT') + b'\x04\xc2\x00\x05' + text('poc_' + uuid.uuid4().hex)
    body += text(username) + text(password)
    length = len(body)
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        encoded.append(byte | (128 if length else 0))
        if not length:
            break
    try:
        connection.sendall(b'\x10' + encoded + body)
        packet = b''
        while len(packet) < 4:
            part = connection.recv(4 - len(packet))
            if not part:
                break
            packet += part
        assert_connack(packet, accepted)
        return connection
    except BaseException:
        connection.close()
        raise


class PoC:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.project = 'mqtt_poc_' + uuid.uuid4().hex[:12]
        self.env = dict(ENV, POC_HELPER_DIR=str(directory), COMPOSE_DISABLE_ENV_FILE='1')
        self.env.update(POC_BACKEND_PASSWORD=secrets.token_urlsafe(32),
                        POC_DATABASE_PASSWORD=secrets.token_hex(32),
                        POC_JWT_SECRET=secrets.token_urlsafe(48),
                        POC_BACKEND_IMAGE=self.project + ':backend',
                        POC_INIT_IMAGE=self.project + ':init',
                        POC_RELOADER_IMAGE=self.project + ':reloader')
        self.compose = ['docker', 'compose', '--env-file', '/dev/null', '-p',
                        self.project, '-f', str(COMPOSE)]
        self.owned = {}
        # This registry is scoped to generated tags/project labels, never user
        # deployment names. Query again at cleanup to cover partial startup and
        # recreated containers rather than relying on stale initial IDs.
        self.owned_images = tuple(self.env[key] for key in
                                  ('POC_BACKEND_IMAGE', 'POC_INIT_IMAGE', 'POC_RELOADER_IMAGE'))
        self.test_secrets = [self.env[key] for key in
                             ('POC_BACKEND_PASSWORD', 'POC_DATABASE_PASSWORD', 'POC_JWT_SECRET')]

    def command(self, *args, **kwargs):
        return run(self.compose + list(args), env=self.env, **kwargs)

    def exec(self, service, *args, data=None, capture=False):
        return self.command('exec', '-T', service, *args, data=data, capture=capture)

    def write(self, path, contents, mode='600'):
        self.exec('writer', '/bin/sh', '-c',
                  f'umask 077; cat > {path}; chmod {mode} {path}', data=contents)

    def tool(self, username, password):
        if username != 'backend_service':
            validate_gateway(username)
        self.exec('writer', TOOL, '-H', 'sha512-pbkdf2', AUTH + '/candidate',
                  username, data=(password + '\n' + password + '\n').encode())

    def probe(self, username, password, accepted=True):
        mqtt_connection(self.port, self.directory / 'ca.crt', username, password, accepted).close()

    def wait_probe(self, username, password):
        deadline = time.monotonic() + 10
        while True:
            try:
                self.probe(username, password)
                return
            except (OSError, RuntimeError) as error:
                if time.monotonic() > deadline:
                    raise RuntimeError('fresh TLS login did not confirm reload: ' +
                                       type(error).__name__ +
                                       (': ' + error.verify_message if isinstance(error, ssl.SSLCertVerificationError) else '')) from None
                time.sleep(.1)

    def swap_reload(self):
        self.exec('writer', '/poc/helper', 'swap')
        self.exec('writer', '/poc/helper', 'reload')

    def check_security(self, service):
        cid = self.command('ps', '-q', service, capture=True).decode().strip()
        inspected = json.loads(run(['docker', 'inspect', cid], capture=True))[0]
        host = inspected['HostConfig']
        mounts = inspected['Mounts']
        if (inspected['Config']['User'] != '1883:1883' or host['Privileged']
                or host['PidMode'] == 'host' or host['CapDrop'] != ['ALL']
                or 'no-new-privileges:true' not in host['SecurityOpt']
                or any('docker.sock' in m['Source'] or 'docker.sock' in m['Destination']
                       for m in mounts)):
            raise RuntimeError('container security assumptions failed')
        auth = [m for m in mounts if m['Destination'] == AUTH]
        control = [m for m in mounts if m['Destination'] == CONTROL]
        if service == 'reloader':
            if auth or host['NetworkMode'] != 'none' or not host['PidMode'].startswith('container:'):
                raise RuntimeError('sidecar isolation failed')
            if any(item.split('=', 1)[0] in ('MQTT_PASSWORD', 'DATABASE_URL', 'SUPABASE_JWT_SECRET')
                   for item in inspected['Config']['Env']):
                raise RuntimeError('sidecar inherited application secret')
        if service in ('writer', 'broker', 'backend') and (
                len(auth) != 1 or auth[0]['RW'] != (service != 'broker')):
            raise RuntimeError('auth mount permissions failed')
        if service in ('writer', 'backend') and (len(control) != 1 or control[0]['RW']):
            raise RuntimeError('backend control mount must be read-only')

    def adapter_phase(self, fixture, phase):
        fixture = dict(fixture, Phase=phase)
        test = 'TestProductionRuntimeFaultIntegration' if phase.startswith('fault-') else 'TestProductionRuntimeIntegration'
        output = self.exec('writer', '/bin/sh', '-c',
                           'env MQTT_RUNTIME_INTEGRATION=1 /poc/runtime.test "$@"; rc=$?; printf "adapter_exit=%s\\n" "$rc"; exit 0', 'adapter',
                           '-test.run=^' + test + '$',
                           '-test.v', '-test.timeout=60s',
                           '-test.coverprofile=/tmp/adapter-' + phase + '.cover',
                           data=json.dumps(fixture).encode(), capture=True).decode()
        # A green process with a skipped/mis-selected test is not evidence.
        # Test code logs only fixed categories (never fixture/password contents).
        if any(fixture[key] in output for key in ('Old', 'New', 'BPassword', 'Backend')
               if fixture.get(key)) or re.search(r'\$7\$\d+\$', output):
            raise RuntimeError('secret detected in adapter diagnostics')
        print(output, end='', flush=True)
        assert_adapter_output(output, phase, test)
        artifact = Path('/tmp/opencode') / (self.project + '-adapter-' + phase + '.cover')
        artifact.write_bytes(self.exec('writer', 'cat', '/tmp/adapter-' + phase + '.cover', capture=True))
        artifact.chmod(0o600)
        print('coverage_artifact=' + str(artifact), flush=True)

    def adapter_integration(self):
        print('stage=production-Go-adapter-integration (not manual native PoC)', flush=True)
        fixture = {'A': 'gw_' + uuid.uuid4().hex[:16], 'B': 'gw_' + uuid.uuid4().hex[:16],
                       'Old': secrets.token_urlsafe(32), 'New': secrets.token_urlsafe(32),
                       'BPassword': secrets.token_urlsafe(32), 'Backend': self.env['POC_BACKEND_PASSWORD']}
        self.test_secrets.extend(fixture[key] for key in ('Old', 'New', 'BPassword'))
        self.adapter_phase(fixture, 'mutate')
        final = self.exec('writer', 'cat', AUTH + '/passwd', capture=True)
        self.command('run', '--rm', 'initializer')
        if final != self.exec('writer', 'cat', AUTH + '/passwd', capture=True):
            raise RuntimeError('initializer mutated completed adapter state')
        # Recreate broker PID namespace and sidecar; backend fresh startup uses
        # CheckRuntime plus a fresh protected-principal TLS probe, no API writes.
        self.command('stop', 'backend', 'reloader', 'broker')
        self.command('up', '-d', '--force-recreate', 'broker', 'reloader')
        self.port = int(self.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
        self.wait_probe('backend_service', fixture['Backend'])
        deadline = time.monotonic() + 10
        while True:
            try:
                self.exec('writer', '/poc/helper', 'reload')
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('recreated sidecar unavailable') from None
                time.sleep(.1)
        self.command('up', '-d', '--force-recreate', 'backend')
        deadline = time.monotonic() + 15
        while True:
            try:
                self.exec('writer', 'wget', '-q', '-O', '/dev/null', 'http://backend:8080/readyz')
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('recreated backend runtime startup failed') from None
                time.sleep(.1)
        self.adapter_phase(fixture, 'persist')
        if final != self.exec('writer', 'cat', AUTH + '/passwd', capture=True):
            raise RuntimeError('recreation changed completed credential state')
        print('GoAdapterE2E=PASS create-A/B rotate revoke TLS ACL delivery restart CheckRuntime init-noop protected-hash', flush=True)
        self.adapter_phase(fixture, 'fault-probe')
        self.adapter_phase(fixture, 'fault-enospc')
        self.adapter_phase(fixture, 'fault-lost-ack')
        for phase, service in (('fault-killed-reloader', 'reloader'), ('fault-offline', 'broker')):
            snapshot = self.exec('writer', 'cat', AUTH + '/passwd', capture=True)
            self.command('kill', '-s', 'SIGKILL', service)
            self.adapter_phase(fixture, phase)
            # Explicit TEST-ONLY intervention after the failed adapter retained
            # evidence. Never startup rollback: independently prove the exact
            # old file and operation-specific broker login before clearing it.
            if snapshot != self.exec('writer', 'cat', AUTH + '/passwd', capture=True):
                raise RuntimeError('fault rollback snapshot differs')
            if service == 'broker':
                self.command('up', '-d', '--force-recreate', 'broker', 'reloader')
                self.port = int(self.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
            else:
                self.command('up', '-d', '--force-recreate', 'reloader')
            self.wait_probe(fixture['B'], fixture['BPassword'])
            self.probe(fixture['B'], fixture['New'], False)
            self.exec('writer', 'rm', '-r', AUTH + '/.op-fault', AUTH + '/.credential.pending')
            self.adapter_phase(fixture, 'persist')
        print('GoAdapterFaults=PASS actual-killed-reloader actual-offline-broker real-probe-rollback lost-ACK-injected-after-real-signal', flush=True)

    def execute(self):
        d = self.directory
        # Only the non-secret helper needs to be readable through the bind mount.
        d.chmod(0o755)
        (d / 'helper.go').write_text(HELPER)
        print('stage=build-helper', flush=True)
        run(['go', 'build', '-o', str(d / 'helper'), str(d / 'helper.go')],
            env=dict(ENV, CGO_ENABLED='0'), timeout=120)
        (d / 'helper').chmod(0o755)
        run(['go', '-C', str(ROOT / 'src'), 'test', '-c', '-cover',
             '-o', str(d / 'runtime.test'), './internal/mqttcredential'],
            env=dict(ENV, CGO_ENABLED='0'), timeout=120)
        (d / 'runtime.test').chmod(0o755)
        for args in (
            ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=isolated-poc-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
             '-keyout', 'ca.key', '-out', 'ca.crt'],
            ['req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
             '-keyout', 'server.key', '-out', 'server.csr'],
        ):
            # Absolute output paths avoid changing the caller's working directory.
            for index, value in enumerate(args):
                if value in ('ca.key', 'ca.crt', 'server.key', 'server.csr'):
                    args[index] = str(d / value)
            run(['openssl'] + args)
        (d / 'extensions').write_text('subjectAltName=DNS:localhost,DNS:broker\nextendedKeyUsage=serverAuth\n'
                                    'basicConstraints=critical,CA:FALSE\n'
                                    'keyUsage=critical,digitalSignature,keyEncipherment\n')
        run(['openssl', 'x509', '-req', '-in', str(d / 'server.csr'), '-CA', str(d / 'ca.crt'),
             '-CAkey', str(d / 'ca.key'), '-CAcreateserial', '-days', '1',
              '-extfile', str(d / 'extensions'), '-out', str(d / 'server.crt')])
        run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=wrong-isolated-ca', '-keyout', str(d / 'wrong-ca.key'),
             '-out', str(d / 'wrong-ca.crt')])
        for path in d.iterdir():
            if path.name not in ('helper', 'runtime.test'):
                path.chmod(0o600)
        print('stage=build-production-targets', flush=True)
        self.command('build', 'writer', 'initializer', 'reloader', timeout=300)
        # Prove rejection at the initializer, not merely at backend startup.
        # This isolated override models the production completed-init dependency.
        override = d / 'initializer-dependency.yml'
        override.write_text('services:\n  broker:\n    depends_on:\n      initializer:\n        condition: service_completed_successfully\n')
        original_password = self.env['POC_BACKEND_PASSWORD']
        self.compose.extend(['-f', str(override)])
        try:
            for password in ('replace_with_backend_mqtt_password', ' \t '):
                self.env['POC_BACKEND_PASSWORD'] = password
                try:
                    self.command('up', '-d', 'broker')
                except RuntimeError:
                    pass
                else:
                    raise RuntimeError('unsafe initializer password allowed broker startup')
                cid = self.command('ps', '-aq', 'initializer', capture=True).decode().strip()
                state = json.loads(run(['docker', 'inspect', cid], capture=True))[0]['State']
                if state['Running'] or state['ExitCode'] != 1:
                    raise RuntimeError('initializer did not reject unsafe password')
                logs = subprocess.run(['docker', 'logs', cid], env=ENV,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      timeout=30, check=True)
                log = logs.stdout + logs.stderr
                if b'auth initialization failed' not in log or password.encode() in log:
                    raise RuntimeError('initializer rejection missing or exposed credential')
                broker = self.command('ps', '-aq', 'broker', capture=True).decode().strip()
                if broker and json.loads(run(['docker', 'inspect', broker], capture=True))[0]['State']['Running']:
                    raise RuntimeError('broker started after initializer rejection')
                self.command('run', '--rm', '--entrypoint', '/bin/sh', 'initializer',
                             '-c', 'test ! -e /mosquitto/auth/passwd; test ! -e /mosquitto/auth/passwd.last-good')
                self.command('rm', '-f', 'initializer', 'broker')
        finally:
            self.env['POC_BACKEND_PASSWORD'] = original_password
            del self.compose[-2:]
        print('initializer_unsafe_password=denied,no-store,broker-blocked', flush=True)
        self.command('run', '--rm', 'initializer')
        self.command('run', '--rm', 'initializer')
        print('stage=start-writer', flush=True)
        self.command('up', '-d', 'writer')
        self.exec('writer', '/bin/sh', '-c',
                  'test "$(command -v mosquitto_passwd)" = /usr/bin/mosquitto_passwd; '
                  'grep -q "VERSION_ID=3.18.9" /etc/os-release; '
                  '/usr/sbin/mosquitto -h | grep -q "mosquitto version 2.0.18"')
        print('runtime_base=Alpine-3.18.9 broker=2.0.18 tool_path_verified=true')
        self.exec('writer', 'chmod', '700', AUTH, '/mosquitto/config')
        self.write('/mosquitto/config/mosquitto.conf', b'''listener 8883
allow_anonymous false
password_file /mosquitto/auth/passwd
acl_file /mosquitto/config/acl
cafile /mosquitto/config/ca.crt
certfile /mosquitto/config/server.crt
keyfile /mosquitto/config/server.key
persistence false
log_dest stdout
''')
        self.write('/mosquitto/config/acl', (ROOT / 'config/mosquitto/acl').read_bytes())
        for name in ('ca.crt', 'server.crt', 'server.key', 'wrong-ca.crt'):
            self.write('/mosquitto/config/' + name, (d / name).read_bytes())
        backend = self.env['POC_BACKEND_PASSWORD']
        old, new = (secrets.token_urlsafe(32) for _ in range(2))
        self.test_secrets.extend((old, new))
        gateway = 'gw_' + uuid.uuid4().hex[:16]
        print('stage=native-tool', flush=True)
        self.exec('writer', '/bin/sh', '-c', f'cp {AUTH}/passwd {AUTH}/candidate')
        backend_line = self.exec('writer', 'cat', AUTH + '/candidate', capture=True)
        self.tool(gateway, old)
        entries = self.exec('writer', 'cat', AUTH + '/candidate', capture=True)
        # A mismatched confirmation must fail without changing candidate content.
        try:
            self.exec('writer', TOOL, '-H', 'sha512-pbkdf2', AUTH + '/candidate',
                      gateway, data=b'confirmation-one\nconfirmation-two\n')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('mismatched native confirmation did not fail')
        if self.exec('writer', 'cat', AUTH + '/candidate', capture=True) != entries:
            raise RuntimeError('failed utility changed candidate')
        if not all(re.fullmatch(rb'[\w-]+:\$7\$101\$[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+',
                               line) for line in entries.splitlines()):
            raise RuntimeError('unexpected native PBKDF2 format')
        listing = self.exec('writer', 'ls', '-A', AUTH, capture=True).decode().splitlines()
        print('native_tool=/usr/bin/mosquitto_passwd hash=$7$101$ backup_files=' +
              ','.join(sorted(listing)))
        self.exec('writer', '/poc/helper', 'swap')
        self.exec('writer', '/bin/sh', '-c', f'cp {AUTH}/passwd {AUTH}/passwd.last-good; rm -f {AUTH}/candidate.backup.*')
        preserved = self.exec('writer', 'cat', AUTH + '/passwd', capture=True)
        self.command('run', '--rm', 'initializer')
        if self.exec('writer', 'cat', AUTH + '/passwd', capture=True) != preserved:
            raise RuntimeError('initializer altered Gateway entries')
        saved = self.env['POC_BACKEND_PASSWORD']
        self.env['POC_BACKEND_PASSWORD'] = secrets.token_urlsafe(32)
        self.test_secrets.append(self.env['POC_BACKEND_PASSWORD'])
        try:
            self.command('run', '--rm', 'initializer')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('initializer silently rotated backend')
        finally:
            self.env['POC_BACKEND_PASSWORD'] = saved
        if self.exec('writer', 'cat', AUTH + '/passwd', capture=True) != preserved:
            raise RuntimeError('failed initializer altered store')
        self.write(AUTH + '/.credential.pending', b'recovery-required\n')
        try:
            self.command('run', '--rm', 'initializer')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('initializer ignored recovery marker')
        self.exec('writer', 'rm', AUTH + '/.credential.pending')
        self.write(AUTH + '/passwd', b'corrupt\n')
        try:
            self.command('run', '--rm', 'initializer')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('initializer reset corrupt file')
        if self.exec('writer', 'cat', AUTH + '/passwd', capture=True) != b'corrupt\n':
            raise RuntimeError('initializer modified corrupt evidence')
        self.write(AUTH + '/passwd', preserved)
        self.exec('writer', 'chmod', '755', AUTH)
        try:
            self.command('run', '--rm', 'initializer')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('initializer silently repaired unsafe permissions')
        self.exec('writer', 'chmod', '700', AUTH)
        try:
            self.command('run', '--rm', '--user', '1884:1884', 'initializer')
        except RuntimeError:
            pass
        else:
            raise RuntimeError('initializer accepted wrong UID')
        print('stage=start-broker-reloader', flush=True)
        self.command('up', '-d', 'broker', 'reloader')
        for service in ('writer', 'broker', 'reloader'):
            cid = self.command('ps', '-q', service, capture=True).decode().strip()
            self.owned[service] = cid
        print('owned_project=' + self.project + ' container_ids=' + json.dumps(self.owned))
        info = json.loads(run(['docker', 'inspect', self.owned['broker']], capture=True))[0]
        if (info['Config']['User'] != '1883:1883' or info['Path'] != '/usr/sbin/mosquitto'
                or info['HostConfig']['CapDrop'] != ['ALL']):
            raise RuntimeError('broker UID/entrypoint/capability assumption failed')
        for service, cid in self.owned.items():
            self.check_security(service)
            inspected = json.loads(run(['docker', 'inspect', cid], capture=True))[0]
            host = inspected['HostConfig']
            if (inspected['Config']['User'] != '1883:1883' or host['Privileged']
                    or host['CapDrop'] != ['ALL'] or
                    'no-new-privileges:true' not in host['SecurityOpt']):
                raise RuntimeError('container security assumptions failed')
            auth_mount = [m for m in inspected['Mounts'] if m['Destination'] == AUTH]
            if service in ('writer', 'broker') and (
                    len(auth_mount) != 1 or auth_mount[0]['RW'] != (service == 'writer')):
                raise RuntimeError('auth mount permissions failed')
            if service == 'reloader' and (host['NetworkMode'] != 'none' or
                                          not host['PidMode'].startswith('container:')):
                raise RuntimeError('sidecar namespace assumptions failed')
        self.exec('broker', '/bin/sh', '-c',
                  'test "$(cat /proc/1/comm)" = mosquitto; test "$(id -u)" = 1883')
        self.exec('reloader', '/bin/sh', '-c',
                  'test "$(cat /proc/1/comm)" = mosquitto; test "$(id -u)" = 1883')
        self.port = int(self.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
        self.wait_probe(gateway, old)
        identity = self.exec('broker', 'cat', '/proc/1/stat', capture=True).split()[21]
        self.probe('backend_service', backend)
        self.probe(gateway, 'wrong_' + secrets.token_hex(8), False)
        # Invalid hostname must fail TLS, not be classified as auth rejection.
        context = ssl.create_default_context(cafile=str(d / 'ca.crt'))
        try:
            with socket.create_connection(('127.0.0.1', self.port), 2) as raw:
                context.wrap_socket(raw, server_hostname='wrong.invalid')
        except ssl.SSLCertVerificationError:
            pass
        else:
            raise RuntimeError('hostname verification did not fail')
        session = mqtt_connection(self.port, d / 'ca.crt', gateway, old)
        self.exec('writer', '/bin/sh', '-c',
                  f'umask 077; cp {AUTH}/passwd {AUTH}/candidate')
        before = self.exec('broker', 'stat', '-c', '%i', AUTH + '/passwd', capture=True)
        self.tool(gateway, new)
        changed = self.exec('writer', 'cat', AUTH + '/candidate', capture=True)
        if backend_line.strip() not in changed.splitlines():
            raise RuntimeError('backend principal changed')
        # Wait for socket readiness without considering readiness proof of reload.
        deadline = time.monotonic() + 5
        while True:
            try:
                self.exec('writer', 'test', '-S', CONTROL + '/reload.sock')
                break
            except RuntimeError:
                if time.monotonic() > deadline:
                    raise RuntimeError('test socket unavailable') from None
                time.sleep(.1)
        self.swap_reload()
        if self.exec('broker', 'cat', '/proc/1/stat', capture=True).split()[21] != identity:
            raise RuntimeError('signal restarted broker')
        after = self.exec('broker', 'stat', '-c', '%i', AUTH + '/passwd', capture=True)
        if before == after or self.exec('broker', 'cat', AUTH + '/passwd', capture=True) != changed:
            raise RuntimeError('directory rename not visible to broker')
        self.wait_probe(gateway, new)
        self.probe(gateway, old, False)
        self.probe('backend_service', backend)
        session.settimeout(1)
        try:
            session.sendall(b'\xc0\x00')
            existing = session.recv(2) == b'\xd0\x00'
        except OSError:
            existing = False
        finally:
            session.close()
        print('rotation_kept_existing_session=' + str(existing).lower())
        # Native backup artifacts are observed but never used as rollback authority.
        self.exec('writer', '/bin/sh', '-c', f'umask 077; cp {AUTH}/passwd {AUTH}/candidate')
        self.exec('writer', TOOL, '-D', AUTH + '/candidate', gateway)
        self.swap_reload()
        self.probe('backend_service', backend)
        self.probe(gateway, new, False)
        self.exec('writer', '/bin/sh', '-c', f'cp {AUTH}/passwd {AUTH}/passwd.last-good; rm -f {AUTH}/candidate.backup.*')
        self.command('run', '--rm', 'initializer')
        self.command('restart', 'reloader')
        time.sleep(.5)
        self.exec('writer', '/poc/helper', 'reload')
        self.command('restart', 'broker')
        self.port = int(self.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
        self.wait_probe('backend_service', backend)
        # Real production server image, isolated DB, no business mutations.
        # Broker restart replaces its PID namespace; recreate its sidecar.
        self.command('up', '-d', '--force-recreate', 'reloader')
        time.sleep(.5)
        self.command('up', '-d', 'backend')
        deadline = time.monotonic() + 15
        while True:
            try:
                self.exec('writer', 'wget', '-q', '-O', '/dev/null', 'http://backend:8080/readyz')
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    logs = self.command('logs', '--no-color', 'backend', capture=True).decode()
                    # Print only fixed startup error categories, never raw logs.
                    for category in ('load configuration', 'open database', 'check MQTT credential runtime',
                                     'credential persistence failed', 'credential recovery required',
                                     'credential reload unavailable', 'credential verification failed',
                                     'password utility failed', 'invalid credential input', 'backend listening'):
                        if category in logs:
                            print('backend_diagnostic=' + category, flush=True)
                    raise RuntimeError('enabled backend startup did not become ready') from None
                time.sleep(.2)
        self.check_security('backend')
        self.command('stop', 'backend')
        for settings in (
                ['-e', 'MQTT_PASSWORD=wrong_' + secrets.token_hex(8)],
                ['-e', 'MQTT_RELOAD_SOCKET=/mosquitto/control/absent.sock']):
            output = self.command('run', '--rm', '--no-deps', *settings,
                                  '--entrypoint', '/bin/sh', 'backend', '-c',
                                  '/app/server 2>&1; rc=$?; printf "server_exit=%s\\n" "$rc"',
                                  timeout=20, capture=True).decode()
            if ('server_exit=1\n' not in output or 'check MQTT credential runtime' not in output
                    or 'backend listening' in output):
                raise RuntimeError('invalid enabled startup did not fail at runtime check')
            if any(secret in output for secret in self.test_secrets):
                raise RuntimeError('secret detected in negative startup diagnostics')
        print('enabled_backend=fresh-TLS-ready; wrong-password=no-HTTP; absent-socket=no-HTTP')
        print('actual_initializer=cold-empty,idempotent,preserved-gateway,mismatch-denied,'
              'corrupt-denied,pending-denied,wrong-uid-denied,restart-preserved; '
              'production_pidfd=permitted-default-seccomp-capdropALL; '
              'reload_signal_kept_process_starttime=true; fresh_TLS_verified=true')
        permissions = self.exec('writer', 'stat', '-c', '%u:%g %a', AUTH,
                                AUTH + '/passwd', CONTROL, CONTROL + '/reload.sock', capture=True)
        if permissions.decode().splitlines() != ['1883:1883 700', '1883:1883 600',
                                                 '1883:1883 700', '1883:1883 600']:
            raise RuntimeError('unexpected volume/socket permissions')
        print('PASS pinned image; UID1883 PID1; RO directory rename; native stdin; '
              'network-none shared-PID signal via RO cross-network Unix socket; '
               'fresh TLS rotation/revoke authentication CONNACK; protected backend')
        self.adapter_integration()
        logs = self.command('logs', '--no-color', capture=True).decode()
        if any(secret in logs for secret in self.test_secrets) or re.search(r'\$7\$\d+\$[A-Za-z0-9+/=]+\$', logs):
            raise RuntimeError('secret detected in owned container logs')
        print('secret_scan=PASS owned-container-logs fixture-stdin adapter-output; utility-child-env=LC_ALL-only (initializer own env intentionally contains backend secret)', flush=True)

    def cleanup(self):
        # Only this unique Compose project's resources; errors are fatal.
        if not re.fullmatch(r'mqtt_poc_[a-f0-9]{12}', self.project):
            raise RuntimeError('invalid cleanup ownership project')
        for kind in ('container', 'volume', 'network'):
            self.owned[kind] = run(['docker', kind, 'ls', '--filter',
                                   'label=com.docker.compose.project=' + self.project,
                                   '-q'], capture=True).decode().splitlines()
        self.command('down', '-v', '--remove-orphans', timeout=60)
        for kind in ('container', 'volume', 'network'):
            argv = ['docker', kind, 'ls', '--filter',
                    'label=com.docker.compose.project=' + self.project, '-q']
            if run(argv, capture=True).strip():
                raise RuntimeError('owned resources remain after cleanup')
        # A failed build may not have created every tag. Missing tags are not
        # cleanup errors; failure to remove an existing owned tag still is.
        for tag in self.owned_images:
            if not tag.startswith(self.project + ':'):
                raise RuntimeError('invalid cleanup image ownership')
            if run(['docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}}',
                    '--filter', 'reference=' + tag], capture=True).strip():
                run(['docker', 'image', 'rm', tag])
                if run(['docker', 'image', 'ls', '-q', '--filter', 'reference=' + tag], capture=True).strip():
                    raise RuntimeError('owned image remains after cleanup')
        print('cleanup_verified project=' + self.project + ' volumes_removed=true')


def assert_adapter_output(output, phase, test='TestProductionRuntimeIntegration'):
    if ('adapter_exit=0\n' not in output or '--- SKIP:' in output or '--- FAIL:' in output or '--- PASS: ' + test not in output
            or 'production adapter phase completed: ' + phase not in output):
        raise RuntimeError('production integration missing, skipped or incomplete')


def main():
    os.umask(0o077)
    def interrupted(*_):
        raise RuntimeError('PoC interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    directory = tempfile.mkdtemp(prefix='mqtt-runtime-', dir='/tmp/opencode')
    poc = PoC(directory)
    try:
        poc.execute()
    finally:
        try:
            poc.cleanup()
        finally:
            shutil.rmtree(directory)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:  # noqa: BLE001 -- CLI boundary; diagnostics are sanitized below.
        # Safe errors only; never dump subprocess output, credentials or hashes.
        print('FAIL ' + type(error).__name__ + ': ' + str(error))
        raise SystemExit(1)
