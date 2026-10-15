#!/usr/bin/env python3
"""One isolated standalone backend/Nginx rehearsal, offline builds, verified cleanup.

Uses root Nginx template and selected runtime image. Host-built static binary is
NOT Dockerfile-build/full root-image parity. JWT is a random verifier seam, not
Supabase login. No resolved Compose, environment, secrets or snapshots emitted.
"""
from __future__ import annotations
import base64
import hashlib
import hmac
import http.client
import json
import os
import re
import secrets
import signal
import stat
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stage2_mosquitto_runtime as runtime
from task28_disable_client_probe import (
    DynSecProbeFixture, NativeTargetConnection, probe_raw_connack,
    create_verified_host_fixture_dir, verified_remove_host_fixture_dir,
    inspect_owned_resource, inspect_resource_presence,
)
from task28_simplified_schema_runner import DatabaseFixture, PG_IMAGE

ROOT = Path(__file__).resolve().parents[2]
MOSQUITTO_IMAGE = runtime.IMAGE
NGINX_IMAGE = 'nginx:1.25-alpine'
# Offline image-layer inspection proves this existing executable and its parents
# are 0755. /app is absent in the base (Dockerfile WORKDIR is not reproduced).
# Overlay only in the disposable backend, never the broker container.
BACKEND_EXECUTABLE = '/usr/sbin/mosquitto'
ENV = dict(runtime.ENV, COMPOSE_DISABLE_ENV_FILE='1')


def private_write(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(content)


def docker(args, data=None, timeout=15):
    p = subprocess.run(['docker'] + args, input=data, env=ENV,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if p.returncode:
        raise RuntimeError('docker_operation_failed')
    return p.stdout


def classify_start_failure(exit_code, stderr):
    # Raw daemon/OCI text is examined in RAM only, never retained in evidence.
    text = stderr.lower()
    if b'mkdir' in text and b'read-only file system' in text:
        code = 'missing_mount_destination'
    elif b'permission denied' in text:
        code = 'permission_denied'
    elif b'exec' in text and b'no such file or directory' in text:
        code = 'no_such_executable'
    elif b'read-only file system' in text:
        code = 'readonly_rootfs'
    elif b'exec format error' in text:
        code = 'exec_format'
    else:
        code = 'generic_start_failure'
    return {'exit_code': int(exit_code), 'code': code}


class ContainerStartFailure(RuntimeError):
    def __init__(self, diagnostic):
        super().__init__('container_start_failed')
        self.diagnostic = diagnostic


def require_consumer_owned(cid, receipt):
    if (not receipt or not re.fullmatch('[a-f0-9]{64}', cid) or
            receipt.get('resource_id') != cid):
        raise RuntimeError('consumer_not_owned')
    owned = inspect_owned_resource('container', cid, receipt['expected_project'],
        receipt['expected_role'], expected_name=receipt['expected_name'])
    if owned.get('status') != 'owned' or owned.get('resource_id') != cid:
        raise RuntimeError('consumer_not_owned')


def start_container(cid, receipt=None):
    require_consumer_owned(cid, receipt)
    p = subprocess.run(['docker', 'start', cid], env=ENV, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, timeout=15)
    if p.returncode:
        diagnostic = classify_start_failure(p.returncode, p.stderr)
        # Do not retain the CompletedProcess or its raw streams in the exception.
        del p
        raise ContainerStartFailure(diagnostic)
    return {'exit_code': 0, 'code': 'started'}


def binary_metadata(path):
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode):
        raise RuntimeError('binary_metadata_invalid')
    with path.open('rb') as stream:
        header = stream.read(64)
        if (len(header) != 64 or header[:6] != b'\x7fELF\x02\x01' or
                struct.unpack_from('<H', header, 18)[0] != 62):
            raise RuntimeError('binary_metadata_invalid')
        offset = struct.unpack_from('<Q', header, 32)[0]
        entry_size, count = struct.unpack_from('<HH', header, 54)
        if entry_size != 56 or count < 1 or count > 128 or offset + count * entry_size > st.st_size:
            raise RuntimeError('binary_metadata_invalid')
        stream.seek(offset)
        types = [struct.unpack_from('<I', stream.read(entry_size))[0] for _ in range(count)]
    return {'file_type': 'regular', 'mode': format(stat.S_IMODE(st.st_mode), '04o'),
            'uid': st.st_uid, 'gid': st.st_gid, 'size': st.st_size,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'elf': {'class': 'ELF64', 'byte_order': 'little', 'target': 'x86_64',
                    'type': struct.unpack_from('<H', header, 16)[0],
                    'has_interpreter': 3 in types, 'has_dynamic_segment': 2 in types,
                    'static_format': 3 not in types and 2 not in types}}


def binary_mount_flags(path):
    p = subprocess.run(['findmnt', '--target', str(path), '--noheadings', '--output', 'OPTIONS'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
    if p.returncode or len(p.stdout.splitlines()) != 1:
        raise RuntimeError('binary_mount_metadata_failed')
    allowed = {'rw', 'ro', 'exec', 'noexec', 'suid', 'nosuid', 'dev', 'nodev',
               'noatime', 'relatime', 'strictatime', 'nodiratime', 'inode64', 'seclabel'}
    flags = [f for f in p.stdout.decode('ascii').strip().split(',') if f in allowed]
    if not ({'rw', 'ro'} & set(flags)):
        raise RuntimeError('binary_mount_metadata_failed')
    return flags


def sanitize_backend_start_metadata(data, binary):
    keys = {'entrypoint', 'cmd', 'user', 'readonly_root', 'apparmor', 'userns',
            'cap_add', 'cap_drop', 'security_opt', 'binary_source', 'binary_destination', 'binary_rw'}
    if (set(data) != keys or data['entrypoint'] != [BACKEND_EXECUTABLE] or
            data['cmd'] not in (None, [], [BACKEND_EXECUTABLE, '-c', '/mosquitto/config/mosquitto.conf']) or
            data['user'] != '1883:1883' or data['readonly_root'] is not True or
            data['apparmor'] not in ('', 'docker-default') or data['userns'] != '' or
            data['cap_add'] not in (None, []) or data['cap_drop'] != ['ALL'] or
            data['security_opt'] != ['no-new-privileges:true'] or
            data['binary_source'] != str(binary) or data['binary_destination'] != BACKEND_EXECUTABLE or
            data['binary_rw'] is not False):
        raise RuntimeError('backend_metadata_invalid')
    return {k: v for k, v in data.items() if k != 'binary_source'} | {'binary_source_matches_fixture': True}


def backend_start_metadata(receipt, binary, expected_sha256):
    cid = receipt.get('resource_id', '')
    require_consumer_owned(cid, receipt)
    # Select only these fields on the owned backend, never Env or whole inspect.
    fields = {'state': '.State.Status', 'entrypoint': '.Config.Entrypoint', 'cmd': '.Config.Cmd', 'user': '.Config.User',
              'readonly_root': '.HostConfig.ReadonlyRootfs', 'apparmor': '.AppArmorProfile',
              'userns': '.HostConfig.UsernsMode', 'cap_add': '.HostConfig.CapAdd',
              'cap_drop': '.HostConfig.CapDrop', 'security_opt': '.HostConfig.SecurityOpt'}
    parts = ['"' + k + '":{{json ' + v + '}}' for k, v in fields.items()]
    template = '{' + ','.join(parts) + ',"binary_mount":['
    template += '{{range .Mounts}}{{if eq .Destination "' + BACKEND_EXECUTABLE + '"}}'
    template += '{"source":{{json .Source}},"destination":{{json .Destination}},"rw":{{json .RW}}}'
    template += '{{end}}{{end}}]}'
    data = json.loads(docker(['inspect', '-f', template, cid]))
    if data.pop('state', None) != 'created':
        raise RuntimeError('backend_metadata_invalid')
    mounts = data.pop('binary_mount')
    if len(mounts) != 1 or set(mounts[0]) != {'source', 'destination', 'rw'}:
        raise RuntimeError('backend_metadata_invalid')
    data.update({'binary_' + k: v for k, v in mounts[0].items()})
    safe = sanitize_backend_start_metadata(data, binary)
    safe['effective_source_metadata'] = binary_metadata(binary)
    source = safe['effective_source_metadata']
    if (source['file_type'] != 'regular' or source['mode'] != '0555' or
            source['sha256'] != expected_sha256):
        raise RuntimeError('backend_binary_contract_failed')
    safe['effective_source_mount_flags'] = binary_mount_flags(binary)
    return safe


def inspect_local_image(image_ref, timeout=10):
    try:
        # Select only non-secret metadata; never inspect container environment.
        p = subprocess.run(['docker', 'image', 'inspect', '-f', '{{json .Config.Volumes}}', image_ref],
                           env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        if p.returncode:
            return False, None, 'image_unavailable'
        return True, json.loads(p.stdout), None
    except Exception:
        return False, None, 'image_inspect_error'


def compile_backend_binary(out_path, timeout=120):
    env = dict(os.environ, CGO_ENABLED='0', GOOS='linux', GOARCH='amd64',
               GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local')
    try:
        p = subprocess.run(['go', 'build', '-o', str(out_path), './cmd/server'],
                           cwd=ROOT / 'src', env=env, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, timeout=timeout)
        return p.returncode == 0, None if p.returncode == 0 else 'offline_build_failed'
    except Exception:
        return False, 'offline_build_unavailable'


def backend_environment(db_password, manager_password, backend_password, jwt_secret):
    # config.Load defaults are used except required prerequisites and these narrow
    # fixture overrides. 55s matches root Compose budgets (defaults 30s/10s fail).
    return dict(SERVER_ENV='production', SERVER_PORT='8080',
        DATABASE_URL=f'postgres://iot_backend_app:{db_password}@task28a-pg:5432/fixture_service?sslmode=disable',
        DATABASE_MAX_CONNS='5', DATABASE_MIN_CONNS='1', DATABASE_CONNECT_TIMEOUT='5s',
        HTTP_WRITE_TIMEOUT='55s', SHUTDOWN_TIMEOUT='55s',
        MQTT_CREDENTIAL_RUNTIME_ENABLED='false', MQTT_CREDENTIAL_API_ENABLED='true',
        INGRESS_GATE_ENABLED='true', MQTT_PASSWORD=backend_password,
        MQTT_INTERNAL_MANAGEMENT_URL='ssl://broker:8883', MQTT_DYNSEC_MANAGER_USERNAME='admin',
        MQTT_DYNSEC_MANAGER_PASSWORD=manager_password, MQTT_TLS_CA_FILE='/public-ca/ca.crt',
        MQTT_DYNSEC_JSON_PATH='/native-store/dynsec.json', MQTT_DYNSEC_WRITER_UID='1883',
        MQTT_DYNSEC_IMAGE_REFERENCE=MOSQUITTO_IMAGE,
        MQTT_DYNSEC_IMAGE_CONFIG_ID=MOSQUITTO_IMAGE.split('@')[1], MQTT_DYNSEC_PLATFORM='linux/amd64',
        SUPABASE_JWT_SECRET=jwt_secret, SUPABASE_JWT_ISSUER='http://supabase-auth:9999',
        SUPABASE_JWT_AUDIENCE='authenticated')


def generate_fixture_jwt(secret, user):
    def enc(v):
        return base64.urlsafe_b64encode(v).rstrip(b'=')
    now = int(time.time())
    payload = dict(sub=user, aud='authenticated', iss='http://supabase-auth:9999',
                   role='authenticated', iat=now, exp=now + 600)
    body = enc(b'{"alg":"HS256","typ":"JWT"}') + b'.' + enc(json.dumps(payload).encode())
    return (body + b'.' + enc(hmac.new(secret.encode(), body, hashlib.sha256).digest())).decode()


def effective_metadata(cid):
    def field(template):
        return json.loads(docker(['inspect', '-f', template, cid]))
    mounts = field('{{json .Mounts}}')
    return {'user': field('{{json .Config.User}}'),
            'mounts': [{k: m[k] for k in ('Destination', 'RW', 'Name', 'Type') if k in m} for m in mounts],
            'networks': sorted(field('{{json .NetworkSettings.Networks}}'))}


def validate_backend_metadata(meta, project):
    mounts = {m['Destination']: m for m in meta['mounts'] if m.get('Type') != 'tmpfs'}
    if any(m.get('Type') == 'tmpfs' and m['Destination'] not in
           {'/tmp', '/mosquitto/data', '/mosquitto/log', '/mosquitto/config'} for m in meta['mounts']):
        return False
    return (meta['user'] == '1883:1883' and
            set(mounts) == {BACKEND_EXECUTABLE, '/native-store', '/public-ca'} and
            all(m.get('RW') is False for m in mounts.values()) and
            mounts['/native-store'].get('Name') == project + '_native_store' and
            mounts['/public-ca'].get('Name') == project + '_public_ca' and
            meta['networks'] == [project + '_default'])


def remove_consumer(receipt):
    cid = receipt.get('resource_id', '')
    if not re.fullmatch('[a-f0-9]{64}', cid):
        return False
    try:
        state = inspect_resource_presence('container', cid)['status']
        if state == 'absent':
            return True
        owned = inspect_owned_resource('container', cid, receipt['expected_project'],
                    receipt['expected_role'], expected_name=receipt['expected_name'])
        if owned['status'] != 'owned' or owned['resource_id'] != cid:
            return False
        docker(['rm', '-f', cid])
        return inspect_resource_presence('container', cid)['status'] == 'absent'
    except Exception:
        return False


def teardown(consumers, pg, broker, host):
    result = {r + '_cleanup_verified': False for r in ('nginx', 'backend', 'pg', 'broker', 'host')}
    for role in ('nginx', 'backend'):
        result[role + '_cleanup_verified'] = role not in consumers or remove_consumer(consumers[role])
        if result[role + '_cleanup_verified'] and role in consumers and broker is not None:
            # Retain capture receipts for evidence; retire only verified absent IDs
            # from the shared helper's deletion queue (it refuses absent as owned).
            cid = consumers[role]['resource_id']
            if cid in broker.owned_containers:
                broker.owned_containers.remove(cid)
    if not all(result[r + '_cleanup_verified'] for r in ('nginx', 'backend')):
        return result
    try:
        if pg is None:
            result['pg_cleanup_verified'] = True
        else:
            cid = pg.container_id
            if cid and pg._load_and_verify_receipt() != cid:
                return result
            if not cid and pg.cidfile.exists():
                cid = pg._load_and_verify_receipt()
                if not cid:
                    return result
            result['pg_cleanup_verified'] = pg.cleanup() is True
            if cid:
                result['pg_cleanup_verified'] = (result['pg_cleanup_verified'] and
                    inspect_resource_presence('container', cid)['status'] == 'absent')
    except Exception:
        pass
    if not result['pg_cleanup_verified']:
        return result
    try:
        detail = {'clean': True} if broker is None else broker.cleanup()
        result['broker_detail'] = detail
        result['broker_cleanup_verified'] = detail.get('clean') is True
    except Exception:
        pass
    if not result['broker_cleanup_verified']:
        return result
    try:
        result['host_cleanup_verified'] = host is None or verified_remove_host_fixture_dir(host).get('verified') is True
    except Exception:
        pass
    return result


def aggregate(smoke, cleanup):
    clean = all(cleanup.get(r + '_cleanup_verified') is True for r in ('nginx', 'backend', 'pg', 'broker', 'host'))
    return {'success': smoke == 'PASS' and clean,
            'verdict': 'CLEANUP_FAILURE_VETO' if not clean else ('PASS' if smoke == 'PASS' else 'NOT_PASSED'),
            'smoke_status': smoke}


def request(port, method, path, token='', key=''):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=50)
    try:
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
        if key:
            headers['Idempotency-Key'] = key
        conn.request(method, path, body=None, headers=headers)
        response = conn.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise RuntimeError('http_body_too_large')
        return response.status, dict(response.getheaders()), json.loads(raw) if raw else {}
    finally:
        conn.close()


def native_store(cid):
    # Private in-memory observation only. Never return this from the runner.
    return json.loads(docker(['exec', '-u', '1883:1883', cid, 'cat', '/native-store/dynsec.json']))


def smoke_flow(port, broker, backend_cid, pg, admin, owner, gateway, observations):
    endpoint = '/v1/admin/gateways/' + gateway + '/mqtt-credential'
    checks = observations['checks'] = {}
    def check(name, ok):
        checks[name] = bool(ok)
        if not ok:
            raise RuntimeError('smoke_assertion_failed')
    def call(method, suffix, token, expected, key=None):
        status, headers, body = request(port, method, endpoint + suffix, token,
                                        str(uuid.uuid4()) if key is None else key)
        observations.setdefault('http_statuses', []).append(status)
        check(method.lower() + '_' + suffix.replace('/', '_') + '_' + str(expected) + '_' + str(len(checks)), status == expected)
        if expected in (200, 201):
            check('no_store_' + str(len(checks)), headers.get('Cache-Control') == 'no-store' and headers.get('Pragma') == 'no-cache')
        return body
    def login(password, code):
        actual = probe_raw_connack(broker.port, broker.d / 'ca.crt', gateway, password, 'probe_' + uuid.uuid4().hex)
        observations.setdefault('connack_codes', []).append(actual)
        check('connack_' + str(len(checks)), actual == code)
    def session(password):
        conn = NativeTargetConnection(broker.port, broker.d / 'ca.crt', gateway, password, 'session_' + uuid.uuid4().hex)
        broker.open_connections.append(conn)
        return conn
    def journal():
        _, raw = pg.sql(f"SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id='{gateway}';".encode())
        numbers = re.findall(rb'^\s*(\d+)\s*$', raw, re.M)
        if len(numbers) != 1:
            raise RuntimeError('journal_observation_invalid')
        return int(numbers[0])
    call('POST', '/rotate', 'invalid-token', 401)
    provision = call('POST', '', admin, 201)
    old = provision.get('password')
    check('provision_secret', isinstance(old, str) and len(old) >= 22 and provision.get('secret_returned') is True)
    login(old, 0)
    active = session(old)
    before = call('GET', '', admin, 200, '')
    count, baseline = journal(), native_store(backend_cid)
    call('POST', '/rotate', owner, 403)
    call('DELETE', '', owner, 403)
    after = call('GET', '', admin, 200, '')
    check('owner_baseline_intact', before == after and count == journal() and baseline == native_store(backend_cid))
    login(old, 0)
    key = str(uuid.uuid4())
    rotated = call('POST', '/rotate', admin, 200, key)
    new = rotated.get('password')
    check('rotate_secret_and_version', isinstance(new, str) and len(new) >= 22 and new != old and rotated.get('credential_version') == 2)
    login(new, 0)
    login(old, 5)
    dropped, kind, elapsed = active.wait_for_disconnect(5)
    observations['rotate_disconnect'] = {'observed': dropped, 'kind': kind, 'elapsed_seconds': round(elapsed, 3)}
    check('rotate_session_closed', dropped)
    replay = call('POST', '/rotate', admin, 200, key)
    check('metadata_only_replay', not replay.get('password') and replay.get('secret_returned') is not True and replay.get('credential_version') == 2)
    active_new = session(new)
    revoked = call('DELETE', '', admin, 200)
    check('revoke_metadata_only', not revoked.get('password') and revoked.get('secret_returned') is not True and revoked.get('status') == 'revoked')
    login(new, 5)
    dropped, kind, elapsed = active_new.wait_for_disconnect(5)
    observations['revoke_disconnect'] = {'observed': dropped, 'kind': kind, 'elapsed_seconds': round(elapsed, 3)}
    check('revoke_session_closed', dropped)
    store = native_store(backend_cid)
    clients = [c for c in store['clients'] if c['username'] == gateway]
    check('native_disabled', len(clients) == 1 and clients[0].get('disabled') is True)
    # Successful production revoke itself also invokes protected VerifyRevoked.
    check('native_roles_unchanged', clients[0].get('roles') == next(c for c in baseline['clients'] if c['username'] == gateway).get('roles'))
    role = 'gateway_' + gateway
    roles = [r for r in store['roles'] if r['rolename'] == role]
    wanted = {(kind, 'gateways/' + gateway + '/' + topic, 0, True) for kind, topic in (
        ('publishClientSend', 'telemetry/#'), ('publishClientSend', 'status'),
        ('publishClientSend', 'responses/#'), ('publishClientReceive', 'acks/#'),
        ('publishClientReceive', 'commands/#'), ('subscribeLiteral', 'acks/#'),
        ('subscribeLiteral', 'commands/#'), ('unsubscribeLiteral', 'acks/#'),
        ('unsubscribeLiteral', 'commands/#'))}
    check('native_exact_gateway_acl', len(roles) == 1 and len(roles[0]['acls']) == len(wanted) and
          {(a['acltype'], a['topic'], a['priority'], a['allow']) for a in roles[0]['acls']} == wanted)


def run_rehearsal_suite(dry_run=False, bounded_native=False):
    result = {'success': False, 'allocated': False, 'failure_stage': None, 'failure_code': None,
              'build_scope': 'offline host static binary mounted read-only; NOT Dockerfile-build/full root-image parity',
              'auth_scope': 'random HS256 verifier seam; NOT Supabase login', 'observations': {}}
    result['root_cause'] = 'UNKNOWN'
    pg = broker = host = None
    consumers = {}
    smoke = 'NOT_EXECUTED'
    stage = 'preflight'
    old_handler = None
    if bounded_native:
        def budget_expired(signum, frame):
            raise RuntimeError('outer_budget_exceeded')
        old_handler = signal.signal(signal.SIGALRM, budget_expired)
        signal.alarm(210)
    try:
        image_volumes = {}
        for role, image in [('pg', PG_IMAGE), ('broker', MOSQUITTO_IMAGE), ('nginx', NGINX_IMAGE)]:
            ok, vols, error = inspect_local_image(image)
            if not ok:
                raise RuntimeError(role + '_image_unavailable')
            image_volumes[role] = vols or {}
        if dry_run:
            result['failure_code'] = 'dry_run_only'
        else:
            d, host = create_verified_host_fixture_dir(Path('/tmp/opencode'))
            result['allocated'] = True
            stage = 'database'
            pg = DatabaseFixture()
            pg.tmpfs_mounts = sorted(set(image_volumes['pg']) | {'/var/lib/postgresql/data'})
            pg.create()
            pg.start()
            pg.wait_ready()
            stage = 'migrations'
            migrations = sorted(p for p in (ROOT / 'migrations').iterdir() if
                (p.name == '000000_configure_supabase_roles.sh' or
                 re.match(r'^0000(?:0[1-9]|1[0-9])_.*\.up\.sql$', p.name)))
            if len(migrations) != 20:
                raise RuntimeError('migration_set_invalid')
            for p in migrations:
                if p.suffix == '.sh':
                    docker(['exec', '-i', pg.container_id, 'sh'], p.read_bytes())
                else:
                    pg.sql(p.read_bytes())
            admin_id, owner_id = str(uuid.uuid4()), str(uuid.uuid4())
            gateway = 'gw_' + secrets.token_hex(6)
            pg.sql(f"INSERT INTO profiles(id,full_name) VALUES ('{admin_id}','Fixture Admin'),('{owner_id}','Fixture Owner'); INSERT INTO platform_admins(user_id) VALUES ('{admin_id}'); INSERT INTO gateways(gateway_id,name) VALUES ('{gateway}','Fixture Gateway'); INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ('{owner_id}','{gateway}','owner');".encode())
            stage = 'broker'
            broker = DynSecProbeFixture(d, MOSQUITTO_IMAGE, initialize_backend=True, consumer_rehearsal=True)
            broker.prepare()
            broker.start()
            # Shared broker preparation makes root fixture files private (0600).
            # Build only after it finishes; no preparation may follow this build.
            stage = 'offline_build'
            binary = d / 'backend-server'
            ok, error = compile_backend_binary(binary)
            if not ok:
                raise RuntimeError(error)
            binary.chmod(0o555)
            result['observations']['compiled_binary'] = binary_metadata(binary)
            result['observations']['binary_mount_flags'] = binary_mount_flags(binary)
            result['binary_sha256'] = result['observations']['compiled_binary']['sha256']
            stage = 'broker_network'
            net = next(r for r in broker.verified_receipts if r['kind'] == 'network')
            if inspect_owned_resource('network', net['resource_id'], broker.project, 'default')['status'] != 'owned':
                raise RuntimeError('network_not_owned')
            if pg._load_and_verify_receipt() != pg.container_id:
                raise RuntimeError('pg_not_owned')
            docker(['network', 'connect', '--alias', 'task28a-pg', net['resource_id'], pg.container_id])
            secret = secrets.token_urlsafe(32)
            envfile = d / 'backend.env'
            cfg = backend_environment(pg.passwords[1], broker.passwords['admin'], broker.passwords['backend_service'], secret)
            private_write(envfile, ''.join(k + '=' + v + '\n' for k, v in cfg.items()))
            # Docker create with exact cidfile: partial creation is retained even on timeout.
            for role in ('backend', 'nginx'):
                stage = role + '_create'
                name = broker.project + '-' + role + '-1'
                cidfile = d / (role + '.cid')
                args = ['create', '--pull=never', '--name', name, '--cidfile', str(cidfile),
                        '--label', 'com.docker.compose.project=' + broker.project,
                        '--label', 'com.docker.compose.service=' + role,
                        '--label', 'com.docker.compose.oneoff=false', '--network', net['resource_id']]
                if role == 'backend':
                    args += ['--network-alias', 'backend', '--user', '1883:1883', '--read-only',
                             '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                             '--tmpfs', '/tmp:rw,noexec,nosuid,size=1m,mode=0700,uid=1883,gid=1883',
                              '--env-file', str(envfile), '--mount', f'type=bind,source={binary},target={BACKEND_EXECUTABLE},readonly']
                    for mount in broker.consumer_mounts():
                        args += ['-v', mount]
                    for dest in image_volumes['broker']:
                        args += ['--tmpfs', dest + ':rw,noexec,nosuid,size=1m']
                    args += ['--entrypoint', BACKEND_EXECUTABLE, MOSQUITTO_IMAGE]
                else:
                    args += ['-p', '127.0.0.1::80', '-e', 'API_DOMAIN=localhost',
                             '--mount', f'type=bind,source={ROOT / "config/nginx/nginx.conf.template"},target=/etc/nginx/templates/default.conf.template,readonly']
                    for dest in image_volumes['nginx']:
                        args += ['--tmpfs', dest + ':rw']
                    args += [NGINX_IMAGE]
                consumers[role] = {'resource_id': '', 'expected_project': broker.project, 'expected_role': role, 'expected_name': name}
                try:
                    docker(args, timeout=20)
                finally:
                    if cidfile.is_file() and not cidfile.is_symlink():
                        cid = cidfile.read_text().strip()
                        consumers[role]['resource_id'] = cid
                        captured = broker.capture_consumer(cid, role, name)
                        if captured['status'] != 'owned' or captured['resource_id'] != cid:
                            raise RuntimeError('consumer_capture_failed')
                cid = consumers[role]['resource_id']
                if role == 'backend':
                    stage = 'backend_prestart_metadata'
                    result['observations']['backend_prestart_metadata'] = backend_start_metadata(consumers[role], binary, result['binary_sha256'])
                    meta = effective_metadata(cid)
                    result['observations']['backend_metadata'] = meta
                    if not validate_backend_metadata(meta, broker.project):
                        raise RuntimeError('backend_mount_contract_failed')
                    # Recheck after other metadata helpers, immediately before start.
                    source = binary_metadata(binary)
                    if source['mode'] != '0555' or source['sha256'] != result['binary_sha256']:
                        raise RuntimeError('backend_binary_contract_failed')
                stage = role + '_start'
                result['observations'][role + '_start'] = start_container(cid, consumers[role])
            backend_cid, nginx_cid = consumers['backend']['resource_id'], consumers['nginx']['resource_id']
            stage = 'effective_metadata'
            meta = effective_metadata(backend_cid)
            result['observations']['backend_metadata'] = meta
            # Selected runtime declares /mosquitto/data/log/config volumes overridden tmpfs;
            # tmpfs is not in Docker .Mounts on supported daemon versions.
            if not validate_backend_metadata(meta, broker.project):
                raise RuntimeError('backend_mount_contract_failed')
            result['observations']['nginx_metadata'] = effective_metadata(nginx_cid)
            public = docker(['exec', '-u', '1883:1883', backend_cid, 'sh', '-c',
                'test "$(ls -A /public-ca)" = ca.crt && test ! -e /security && test ! -e /var/run/docker.sock && stat -c "%u:%g:%a" /native-store /native-store/dynsec.json']).decode().splitlines()
            result['observations']['protected_store_stat'] = public
            if public != ['1883:1883:700', '1883:1883:600']:
                raise RuntimeError('protected_store_permissions_failed')
            portdata = json.loads(docker(['inspect', '-f', '{{json .NetworkSettings.Ports}}', nginx_cid]))
            ports = portdata['80/tcp']
            if len(ports) != 1 or ports[0]['HostIp'] != '127.0.0.1':
                raise RuntimeError('http_binding_unsafe')
            port = int(ports[0]['HostPort'])
            result['observations']['http_binding'] = {'host': '127.0.0.1', 'port': port}
            stage = 'standalone_readiness'
            deadline = time.monotonic() + 20
            ready = False
            while time.monotonic() < deadline:
                try:
                    status, _, body = request(port, 'GET', '/readyz')
                    ready = status == 200
                    if ready:
                        break
                except (OSError, ValueError, http.client.HTTPException):
                    pass
                time.sleep(0.2)
            result['observations']['readiness_200'] = ready
            if not ready:
                raise RuntimeError('standalone_readiness_timeout')
            stage = 'smoke'
            smoke = 'FAIL'
            smoke_flow(port, broker, backend_cid, pg, generate_fixture_jwt(secret, admin_id),
                       generate_fixture_jwt(secret, owner_id), gateway, result['observations'])
            smoke = 'PASS'
    except Exception as exc:
        result['failure_stage'] = stage
        if isinstance(exc, ContainerStartFailure):
            result['observations']['start_diagnostic'] = exc.diagnostic
        allowed = {'pg_image_unavailable', 'broker_image_unavailable', 'nginx_image_unavailable',
                   'offline_build_failed', 'offline_build_unavailable', 'migration_set_invalid',
                   'network_not_owned', 'pg_not_owned', 'consumer_capture_failed',
                    'backend_mount_contract_failed', 'backend_binary_contract_failed', 'protected_store_permissions_failed',
                   'http_binding_unsafe', 'standalone_readiness_timeout', 'smoke_assertion_failed'}
        code = str(exc) if isinstance(exc, RuntimeError) else ''
        result['failure_code'] = code if code in allowed else stage + '_failed'
    finally:
        if bounded_native:
            signal.alarm(80)  # Total lifecycle budget <= 290s; unknown cleanup vetoes PASS.
        try:
            clean = teardown(consumers, pg, broker, host)
        except Exception:
            clean = {r + '_cleanup_verified': False for r in ('nginx', 'backend', 'pg', 'broker', 'host')}
        finally:
            if bounded_native:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        result['cleanup'] = clean
        result['aggregate'] = aggregate(smoke, clean)
        result['success'] = result['aggregate']['success']
    return result


def source_identity():
    files = [Path(__file__), Path(__file__).with_name('task28_disable_client_probe.py'),
             Path(__file__).with_name('task28_simplified_schema_runner.py'),
             Path(__file__).with_name('stage2_mosquitto_runtime.py'),
             Path(__file__).with_name('compose.task260-dynsec.yml'),
             Path(__file__).with_name('test_task28_nginx_compose_rehearsal.py'),
             ROOT / 'src/go.mod', ROOT / 'src/go.sum',
             ROOT / 'docker-compose.yml', ROOT / 'config/nginx/nginx.conf.template', ROOT / 'src/Dockerfile']
    files += sorted((ROOT / 'src').rglob('*.go'))
    files += sorted((ROOT / 'migrations').glob('*'))
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}


if __name__ == '__main__':
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, env=ENV,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5).stdout.decode().strip()
    identity = source_identity()
    out = run_rehearsal_suite(bounded_native=True)
    out['head'], out['source_sha256'] = head, identity
    evidence = Path('/home/trung/.opencode/plan/task28-evidence/simplified') / ('nginx-rehearsal-' + time.strftime('%Y%m%d') + '-' + secrets.token_hex(6) + '.json')
    private_write(evidence, json.dumps(out, indent=2) + '\n')
    print(json.dumps({'verdict': out['aggregate']['verdict'], 'failure_stage': out['failure_stage'],
                      'failure_code': out['failure_code'], 'evidence': str(evidence)}))
    sys.exit(0 if out['success'] else 1)
