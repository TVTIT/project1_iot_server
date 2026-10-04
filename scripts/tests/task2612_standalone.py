#!/usr/bin/env python3
"""Actual cmd/server process lane. No test handler or production fault switch."""
import base64
import hashlib
import hmac
import http.client
import json
import secrets
import socket
import ssl
import subprocess
import time
import uuid

from stage2_mosquitto_runtime import ENV, ROOT, run
from task265b_lifecycle import IMAGE
from task26_harness_helpers import require


def acceptance(directory, spike, database, backend, proxy, network, mounts, payload):
    run(['go', '-C', str(ROOT / 'src'), 'build', '-o', str(directory / 'server'), './cmd/server'],
        env=dict(ENV, CGO_ENABLED='0'), timeout=120)
    (directory / 'server').chmod(0o755)
    jwt_secret = secrets.token_urlsafe(48)
    actor = str(uuid.uuid4())
    gateway = 'fixture_' + secrets.token_hex(4)
    base = '/v1/admin/gateways/' + gateway + '/mqtt-credential'
    observed_secrets = []

    def sql(statement):
        result = subprocess.run(['docker', 'exec', '-i', database, 'psql', '-U', 'fixture_admin',
                                 '-d', 'fixture_service', '-v', 'ON_ERROR_STOP=1', '-At'],
                                input=statement.encode(), env=ENV, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=30)
        require(result.returncode == 0, 'trusted standalone fixture SQL')
        return result.stdout.decode().strip()

    sql(f"INSERT INTO profiles(id) VALUES('{actor}'); INSERT INTO platform_admins(user_id) VALUES('{actor}'); "
        f"INSERT INTO gateways(gateway_id,name) VALUES('{gateway}','standalone fixture');")
    environment = dict(DATABASE_URL=payload['AppDSN'], SUPABASE_JWT_SECRET=jwt_secret,
                       SUPABASE_JWT_ISSUER='https://fixture.invalid/auth/v1',
                       SUPABASE_JWT_AUDIENCE='authenticated', SERVER_ENV='production',
                       HTTP_WRITE_TIMEOUT='60s', SHUTDOWN_TIMEOUT='50s',
                       MQTT_CREDENTIAL_API_ENABLED='true', MQTT_DYNSEC_MANAGER_USERNAME='admin',
                       MQTT_DYNSEC_MANAGER_PASSWORD=spike.passwords['admin'],
                       MQTT_PASSWORD=spike.passwords['backend'], MQTT_INTERNAL_MANAGEMENT_URL='ssl://localhost:18884',
                       MQTT_TLS_CA_FILE='/ca.crt', MQTT_DYNSEC_JSON_PATH='/security/dynsec.json',
                       MQTT_CONTROLLER_DIR='/control', MQTT_CREDENTIAL_RECONCILE_TIMEOUT='30s')
    env_file = directory / 'server.env'

    def start(enabled=True):
        run(['docker', 'rm', '-f', backend], capture=True) if exists() else None
        values = environment if enabled else {k: v for k, v in environment.items() if not k.startswith('MQTT_')}
        env_file.write_text(''.join(k + '=' + v + '\n' for k, v in values.items()))
        env_file.chmod(0o600)
        args = ['docker', 'run', '-d', '--name', backend, '--network', network,
                '--user', '1883:1883', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                '--env-file', str(env_file), '--mount', f'type=bind,src={directory}/server,dst=/server,readonly']
        if enabled:
            args += ['--mount', f'type=bind,src={directory}/ca.crt,dst=/ca.crt,readonly',
                     '--mount', 'type=volume,src=' + mounts['/control'] + ',dst=/control',
                     '--mount', 'type=volume,src=' + mounts['/security'] + ',dst=/security,readonly']
        run(args + ['--entrypoint', '/server', IMAGE], capture=True)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            try:
                status, _, _ = request('GET', '/readyz', '', '')
                if status == 200:
                    print('PASS standalone startup ' + ('enabled' if enabled else 'disabled without credential mounts/config'), flush=True)
                    return
            except (OSError, ValueError):
                pass
            time.sleep(.2)
        logs = subprocess.run(['docker', 'logs', backend], env=ENV, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, check=True, timeout=10).stdout
        for value in [jwt_secret, payload['AppDSN'], payload['AdminDSN'], *spike.passwords.values(), *observed_secrets]:
            logs = logs.replace(value.encode(), b'[REDACTED]')
        print(logs.decode(), flush=True)
        raise RuntimeError('standalone startup failed')

    def exists():
        return subprocess.run(['docker', 'inspect', backend], env=ENV, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0

    port = int(run(['docker', 'port', proxy, '8443/tcp'], capture=True).decode().strip().rsplit(':', 1)[1])
    tls = ssl.create_default_context(cafile=str(directory / 'ca.crt'))

    def token(user=actor, role='authenticated'):
        encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b'=')
        claims = dict(sub=user, role=role, iss=environment['SUPABASE_JWT_ISSUER'], aud='authenticated',
                      exp=int(time.time()) + 300, iat=int(time.time()), is_admin=True)
        message = encode(b'{"alg":"HS256","typ":"JWT"}') + b'.' + encode(json.dumps(claims).encode())
        return (message + b'.' + encode(hmac.new(jwt_secret.encode(), message, hashlib.sha256).digest())).decode()

    def request(method, path, bearer=None, key=None):
        connection = http.client.HTTPSConnection('localhost', port, context=tls, timeout=50)
        headers = {}
        if bearer:
            headers['Authorization'] = 'Bearer ' + bearer
        if key:
            headers['Idempotency-Key'] = key
        try:
            connection.request(method, path, headers=headers)
            response = connection.getresponse()
            body = json.loads(response.read())
            return response.status, dict(response.getheaders()), body
        finally:
            connection.close()

    def call(method, path=base, want=200, bearer=None, key=None):
        status, headers, body = request(method, path, token() if bearer is None else bearer,
                                        str(uuid.uuid4()) if key is None else key)
        require(status == want, f'standalone {method} expected {want}, got {status} (body withheld)')
        require(headers.get('Cache-Control') == 'no-store' and headers.get('Pragma') == 'no-cache', 'standalone no-store')
        if body.get('password'):
            observed_secrets.append(body['password'])
        return body

    def gate(opened):
        broker_port = int(spike.command('port', 'broker', '8883', capture=True).decode().strip().rsplit(':', 1)[1])
        try:
            with socket.create_connection(('localhost', broker_port), timeout=2) as raw:
                with tls.wrap_socket(raw, server_hostname='localhost'):
                    actual = True
        except OSError:
            actual = False
        require(actual == opened, 'standalone gated TLS OPEN/CLOSED')

    def stop(crash=False):
        start_time = time.monotonic()
        run(['docker', 'kill', '-s', 'SIGKILL' if crash else 'SIGTERM', backend], capture=True)
        run(['docker', 'wait', backend], capture=True, timeout=55)
        require(time.monotonic() - start_time < 55, 'bounded standalone shutdown')
        logs = subprocess.run(['docker', 'logs', backend], env=ENV, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, check=True, timeout=10).stdout
        require(not any(v.encode() in logs for v in [jwt_secret, *spike.passwords.values(), *observed_secrets]), 'standalone log secrecy')
        if not crash:
            require(b'backend shutdown complete' in logs, 'actual graceful shutdown executed')
            state = json.loads(run(['docker', 'inspect', backend], capture=True))[0]['State']
            require(state['ExitCode'] == 0, 'standalone graceful exit status')

    def count():
        return sql('SELECT count(*) FROM gateway_mqtt_credential_events;')

    start()
    gate(True)
    broker_name = spike.command('ps', '-q', 'broker', capture=True).decode().strip()
    broker_name = json.loads(run(['docker', 'inspect', broker_name], capture=True))[0]['Name'].lstrip('/')
    for port_number in ('18884', '1883'):
        result = subprocess.run(['docker', 'run', '--rm', '--network', network, '--entrypoint', '/bin/sh',
                                 IMAGE, '-ec', f'nc -z -w 2 {broker_name} {port_number}'], env=ENV,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        require(result.returncode != 0, 'standalone bridge cannot bypass gated listener')
    routes = [('GET', base), ('POST', base), ('POST', base + '/rotate'), ('DELETE', base)]
    before = count()
    for role in ('owner', 'operator', 'viewer', 'non-member'):
        user = str(uuid.uuid4())
        sql(f"INSERT INTO profiles(id) VALUES('{user}');")
        if role != 'non-member':
            sql(f"INSERT INTO user_gateways(user_id,gateway_id,role) VALUES('{user}','{gateway}','{role}');")
        for method, path in routes:
            call(method, path, 403, token(user))
    for method, path in routes:
        for bearer in ('', 'invalid', token(role='service_role')):
            call(method, path, 401, bearer)
    require(count() == before, 'denials have no mutation')
    key = str(uuid.uuid4())
    first = call('POST', want=201, key=key)
    require(first.get('secret_returned') is True and len(first.get('password', '')) == 43, 'standalone provision secret')
    before = count()
    replay = call('POST', want=201, key=key)
    require(replay.get('password') is None and replay.get('secret_returned') is False and count() == before, 'standalone replay no second secret/mutation')
    call('GET')
    rotated = call('POST', base + '/rotate')
    require(rotated.get('credential_version') == 2, 'standalone rotation generation')
    require(call('DELETE').get('status') == 'revoked', 'standalone revoke')
    print('PASS standalone four routes, real JWT/admin SQL, role denials, provision/rotate/revoke, replay and no-store', flush=True)

    # Actual per-request authorization query error. No global DB-loss policy.
    sql('REVOKE SELECT ON platform_admins FROM iot_backend;')
    before = count()
    for method, path in routes:
        call(method, path, 503)
    sql('GRANT SELECT ON platform_admins TO iot_backend;')
    require(count() == before, 'checker error has no credential mutation')
    print('PASS standalone per-request database checker fail-closed', flush=True)

    # Native filesystem save uncertainty through the real HTTP boundary.
    spike.setup('chmod 500 /security')
    try:
        require(call('POST', want=503).get('password') is None, 'save fault withheld secret')
        gate(False)
    finally:
        spike.setup('chmod 700 /security')
    stop()
    gate(False)
    start()
    call('GET')
    print('PASS standalone native save uncertainty HTTP 503/CLOSED and process restart recovery', flush=True)

    spike.setup('chmod 000 /security/dynsec.json; chmod 500 /security')
    try:
        require(call('POST', want=503).get('password') is None, 'snapshot uncertainty withheld secret')
        gate(False)
    finally:
        spike.setup('chmod 700 /security; chmod 600 /security/dynsec.json')
    stop()
    start()
    print('PASS standalone protected snapshot uncertainty HTTP 503/CLOSED and restart recovery', flush=True)

    sql("CREATE FUNCTION fixture_standalone_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='active' THEN RAISE EXCEPTION 'fixture finalization fault'; END IF; RETURN NEW; END $$; "
        'CREATE TRIGGER fixture_standalone_reject BEFORE UPDATE ON gateway_mqtt_credentials FOR EACH ROW EXECUTE FUNCTION fixture_standalone_reject();')
    require(call('POST', want=503).get('password') is None, 'SQL finalization withheld secret')
    gate(False)
    require(int(sql("SELECT count(*) FROM gateway_mqtt_credential_events WHERE status IN ('pending','recovery_needed');")) > 0, 'unresolved intent retained')
    stop(crash=True)
    sql('DROP TRIGGER fixture_standalone_reject ON gateway_mqtt_credentials; DROP FUNCTION fixture_standalone_reject();')
    start()
    require(call('GET').get('status') == 'revoked', 'crash restart revoked disposition')
    call('POST', want=201)
    print('PASS standalone SQL finalization error, SIGKILL/restart recovery, deliberate fresh-key provision', flush=True)

    # Stop the owned manager/controller, not a fake dependency implementation.
    spike.command('stop', 'broker')
    require(call('POST', base + '/rotate', 503).get('password') is None, 'unavailable manager withheld secret')
    stop()
    spike.command('up', '-d', 'broker')
    spike.command('exec', '-T', 'broker', '/bin/sh', '-ec',
                  'i=0; until test -S /control/control.sock; do '
                  'i=$((i+1)); test "$i" -lt 100; sleep 0.05; done')
    start()
    stop()
    gate(False)
    print('PASS standalone broker/controller unavailable HTTP 503; bounded SIGTERM shutdown CLOSED', flush=True)
    spike.command('stop', 'broker')
    start(enabled=False)
    for method, path in routes:
        call(method, path, 503)
    stop()
    print('PASS standalone disabled routes without broker, CA, snapshot or controller dependencies', flush=True)
    for owned in (proxy,):
        logs = subprocess.run(['docker', 'logs', owned], env=ENV, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, check=True, timeout=10).stdout
        require(not any(v.encode() in logs for v in [jwt_secret, *spike.passwords.values(), *observed_secrets]), 'standalone proxy log secrecy')
    rows = sql('SELECT row_to_json(e)::text FROM gateway_mqtt_credential_events e '
               'UNION ALL SELECT row_to_json(m)::text FROM gateway_mqtt_credentials m;')
    require(not any(value in rows for value in observed_secrets), 'standalone database secret exclusion')
    print('NOT RUN optional committed HTTP transport-response loss; caller discard is not transport loss', flush=True)
