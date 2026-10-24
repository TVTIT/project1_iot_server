"""P2 compatibility probes; allocation and cleanup belong to native_compose_smoke."""
import base64
import json
import secrets
import time
from datetime import datetime, timedelta, timezone
import ipaddress
import re
import subprocess


def b64url(data):
    return base64.urlsafe_b64encode(data).decode().rstrip('=')


def rsa_jwk(kid):
    # Fixture generation only; runtime key decoding remains in the JWKS SDK.
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private.private_numbers()
    def integer(value):
        return b64url(value.to_bytes((value.bit_length() + 7) // 8, 'big'))
    public = dict(kty='RSA', alg='RS256', kid=kid,
                  use='sig', key_ops=['verify'], n=integer(numbers.public_numbers.n),
                  e=integer(numbers.public_numbers.e))
    signing = dict(public, key_ops=['sign'], d=integer(numbers.d), p=integer(numbers.p),
                   q=integer(numbers.q), dp=integer(numbers.dmp1), dq=integer(numbers.dmq1),
                   qi=integer(numbers.iqmp))
    pem = private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption()).decode()
    return signing, public, pem


def prepare_spike(smoke, fixture):
    from cryptography.hazmat.primitives import serialization
    signing, public, pem = rsa_jwk('p2-k1-' + secrets.token_hex(8))
    # Legacy API keys have no kid. This verify-only compatibility key belongs to
    # Auth alone; it must never appear in the public JWKS or Go RSA verifier.
    compatibility = dict(kty='oct', alg='HS256', kid='', use='sig', key_ops=['verify'],
                         k=b64url(fixture.values['JWT_SECRET'].encode()))
    keys, jwks = [signing, compatibility], {'keys': [public]}
    smoke.private_write(fixture.private / 'p2-signing-keys.json', json.dumps(keys))
    smoke.private_write(fixture.private / 'p2-public-jwks.json', json.dumps(jwks))
    smoke.private_write(fixture.private / 'p2-k1.pem', pem)
    private = serialization.load_pem_private_key(pem.encode(), password=None)
    smoke.private_write(fixture.private / 'p2-verify.pem', private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode())
    # Override is itself a protected private input. No signing material in argv,
    # generic env-file, source template, artifact or non-Auth service environment.
    stanzas = ('services:\n  supabase-auth:\n    environment:\n      GOTRUE_JWT_KEYS: '
               + json.dumps(json.dumps(keys))
               + '\n  supabase-storage:\n    environment:\n      JWT_JWKS: '
               + json.dumps(json.dumps(jwks)) + '\n')
    smoke.private_write(fixture.private / 'compose.p2.yml', stanzas)


def run_spike(smoke, fixture, registry, endpoint, containers, images):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    evidence = {'schema_version': 1, 'status': 'FAIL', 'full_p2_accepted': False,
                 'transport': 'https-verified' if fixture.paper_p2 else 'http-local-spike-only', 'checks': {},
                'image_ids': {key: images[key] for key in (
                    'supabase-auth', 'supabase-storage', 'supabase-envoy', 'nginx')}}
    def check(name, success, **metadata):
        evidence['checks'][name] = dict(status='PASS' if success else 'FAIL', **metadata)
        smoke.durable_json(fixture.artifacts / 'spike.json', evidence)
        if not success:
            raise ValueError('paper_p2_spike_failed')

    service = fixture.values['SERVICE_ROLE_KEY']
    email, password = 'p2-spike@example.test', secrets.token_hex(20)
    status, _, body = smoke.http_request(endpoint, 'POST', '/auth/v1/admin/users',
        token=service, api_key=service, body={'email': email, 'password': password, 'email_confirm': True})
    check('auth_admin_legacy_service', status == 200 and isinstance(body, dict) and bool(body.get('id')),
          http_status=status)
    uid = body['id']
    status, _, body = smoke.http_request(endpoint, 'POST', '/auth/v1/token?grant_type=password',
        api_key=fixture.values['ANON_KEY'], body={'email': email, 'password': password})
    smoke.private_write(fixture.private / 'p2-login-response.json', json.dumps(body))
    check('login_legacy_anon', status == 200 and isinstance(body, dict) and bool(body.get('access_token')),
          http_status=status)
    token = body['access_token']
    parts = token.split('.')
    def decode(part):
        return base64.urlsafe_b64decode(part + '=' * (-len(part) % 4))
    header, claims = json.loads(decode(parts[0])), json.loads(decode(parts[1]))
    jwks_expected = json.loads((fixture.private / 'p2-public-jwks.json').read_text())
    check('user_contract', header.get('alg') == 'RS256'
          and header.get('kid') == jwks_expected['keys'][0]['kid']
          and claims.get('iss') == fixture.values['GOTRUE_JWT_ISSUER']
          and claims.get('aud') == 'authenticated' and claims.get('sub') == uid
          and claims.get('role') == 'authenticated'
          and claims.get('iat', 0) <= time.time() < claims.get('exp', 0),
          alg=header.get('alg'), kid=header.get('kid'))
    key = serialization.load_pem_public_key((fixture.private / 'p2-verify.pem').read_bytes())
    try:
        key.verify(decode(parts[2]), (parts[0] + '.' + parts[1]).encode(), padding.PKCS1v15(), hashes.SHA256())
        valid = True
    except Exception:
        valid = False
    check('signature', valid)
    status, _, jwks = smoke.http_request(endpoint, 'GET', '/auth/v1/.well-known/jwks.json')
    keys = jwks.get('keys', []) if isinstance(jwks, dict) else []
    check('public_jwks', status == 200 and len(keys) == 1
          and keys[0] == jwks_expected['keys'][0]
          and not any(set(k) & {'d', 'p', 'q', 'dp', 'dq', 'qi', 'oth', 'k'} for k in keys),
          http_status=status, key_count=len(keys))
    status, _, _ = smoke.http_request(endpoint, 'GET', '/storage/v1/bucket', token=service, api_key=service)
    check('storage_legacy_service', status == 200, http_status=status)
    status, _, _ = smoke.http_request(endpoint, 'POST', '/storage/v1/bucket', token=service, api_key=service,
        body={'id': 'p2-spike', 'name': 'p2-spike', 'public': False})
    check('storage_private_bucket', status == 200, http_status=status)
    status, _, body = smoke.http_request(endpoint, 'POST', '/storage/v1/object/list/p2-spike',
        token=token, api_key=fixture.values['ANON_KEY'], body={'prefix': '', 'limit': 1})
    check('storage_rs256_user', status == 200 and body == [], http_status=status)
    evidence['status'] = 'PASS'
    smoke.durable_json(fixture.artifacts / 'spike.json', evidence)
    return evidence


def prepare_perimeter(smoke, fixture):
    from cryptography import x509
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
    tls = fixture.private / 'p2-tls'
    tls.mkdir(mode=0o700)
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'P2 fixture CA')])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(ca_key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
          .not_valid_after(now + timedelta(days=2)).add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
          .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False, key_encipherment=False,
              data_encipherment=False, key_agreement=False, key_cert_sign=True, crl_sign=True,
              encipher_only=False, decipher_only=False), True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
          .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False)
          .sign(ca_key, hashes.SHA256()))
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')]))
            .issuer_name(name).public_key(server_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=2))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=True,
                data_encipherment=False, key_agreement=False, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False), True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(server_key.public_key()), False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'), x509.DNSName('nginx'),
                x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), False).sign(ca_key, hashes.SHA256()))
    smoke.private_write(tls / 'server.key', server_key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode())
    smoke.private_write(tls / 'server.crt', cert.public_bytes(serialization.Encoding.PEM).decode())
    smoke.private_write(fixture.private / 'p2-ca.crt', ca.public_bytes(serialization.Encoding.PEM).decode())
    path = fixture.private / 'compose.p2.yml'
    text = path.read_text()
    text += f'''  backend:
    environment:
      SUPABASE_JWT_SECRET: !reset null
    volumes:
      - auth_socket:/run/iot-auth
    depends_on:
      supabase-envoy:
        condition: service_started
      auth-socket-init:
        condition: service_completed_successfully
  nginx:
    ports: !override
      - target: 443
        published: "0"
        host_ip: "127.0.0.1"
        protocol: tcp
    volumes: !override
      - {smoke.ROOT / 'config/nginx/paper-p2.conf.template'}:/etc/nginx/templates/default.conf.template:ro
      - {tls}:/etc/nginx/p2-tls:ro
      - auth_socket:/run/iot-auth:ro
  auth-socket-init:
    image: {smoke.DEFAULTS['MQTT_DYNSEC_IMAGE_REFERENCE']}
    restart: 'no'
    user: '0:0'
    read_only: true
    entrypoint: ['/bin/sh']
    command: ['-ec', 'chown 1883:101 /run/iot-auth && chmod 2710 /run/iot-auth']
    tmpfs: ['/mosquitto/data:rw,noexec,nosuid,nodev,size=1m,mode=0700', '/mosquitto/log:rw,noexec,nosuid,nodev,size=1m,mode=0700']
    volumes:
      - auth_socket:/run/iot-auth
    networks: [iot_net]
    labels:
      io.iot.native-smoke.run: {fixture.project}
      io.iot.native-smoke.role: auth-socket-init
      test.task: native-compose-startup
      test.run: {fixture.project}
volumes:
  auth_socket:
    labels:
      io.iot.native-smoke.run: {fixture.project}
      io.iot.native-smoke.role: auth_socket
'''
    # This is an existing protected, fixture-owned input, not a source template.
    smoke.durable_json(fixture.private / 'p2-profile.json', {'https': True, 'socket_gid': 101})
    with path.open('w') as stream:
        stream.write(text)


def validate_perimeter(fixture, services):
    env = services['backend']['environment']
    if (env.get('SUPABASE_JWT_MODE') != 'rs256' or 'SUPABASE_JWT_SECRET' in env
            or env.get('AUTH_UNIX_SOCKET_PATH') != '/run/iot-auth/auth.sock'
            or env.get('SUPABASE_JWKS_CACHE_TTL') != '60s'):
        raise ValueError('paper_p2_perimeter_failed')
    signing = json.loads((fixture.private / 'p2-signing-keys.json').read_text())
    for role, service in services.items():
        if role != 'supabase-auth' and signing[0]['d'] in json.dumps(service):
            raise ValueError('paper_p2_perimeter_failed')


def direct_go(smoke, registry, containers, token, path='/v1/admin/metrics'):
    # Token stays on stdin, never a docker/wget command-line argument.
    result = registry.command(['exec', '-i', containers['backend']['receipt']['id'], '/bin/sh', '-ec',
        'IFS= read -r token; wget -S -T 5 -O - --header="Authorization: Bearer $token" "http://127.0.0.1:8080' + path + '"'],
        input=(token + '\n').encode())
    codes = re.findall(rb'HTTP/1\.[01] (\d{3})', result.stderr)
    if not codes:
        raise ValueError('paper_p2_perimeter_failed')
    try:
        body = json.loads(result.stdout)
    except ValueError:
        body = None
    return int(codes[-1]), body


def run_perimeter(smoke, fixture, registry, endpoint, containers, images, state):
    run = {'schema_version': 1, 'status': 'FAIL', 'full_p2_accepted': False, 'auth_mode': 'rs256',
           'https_configured': True, 'https_verified': False,
           'auth_request_enabled': True, 'cache_ttl_seconds': 60, 'checks': {}}
    def check(name, success, **metadata):
        run['checks'][name] = dict(status='PASS' if success else 'FAIL', **metadata)
        smoke.durable_json(fixture.artifacts / 'run.json', run)
        if not success:
            raise ValueError('paper_p2_perimeter_failed')
    cid = containers['nginx']['receipt']['id']
    result = registry.command(['exec', cid, 'nginx', '-V'])
    check('nginx_module', result.returncode == 0 and b'--with-http_auth_request_module' in result.stderr)
    result = registry.command(['exec', cid, 'nginx', '-t'])
    check('nginx_config', result.returncode == 0)
    result = registry.command(['exec', cid, 'sh', '-ec', 'id nginx; stat -c "%u:%g:%a" /run/iot-auth /run/iot-auth/auth.sock'])
    check('socket_worker_permissions', result.returncode == 0 and b'uid=101(nginx)' in result.stdout
          and b'1883:101:2710' in result.stdout and b'1883:101:660' in result.stdout)
    if not isinstance(endpoint, smoke.HTTPSEndpoint):
        raise ValueError('paper_p2_perimeter_failed')
    # The verified client has returned a real response; configuration intent alone
    # is not evidence that a TLS handshake passed CA/hostname verification.
    status = smoke.http_request(endpoint, 'GET', '/readyz')[0]
    run['https_verified'] = True
    check('https_readiness', status == 200, http_status=status)
    spike = run_spike(smoke, fixture, registry, endpoint, containers, images)
    run['supabase_spike_checks'] = spike['checks']
    login = json.loads((fixture.private / 'p2-login-response.json').read_text())
    token, uid = login['access_token'], login['user']['id']
    smoke.db_psql(registry, containers['postgres']['receipt']['id'],
        "INSERT INTO platform_admins(user_id) VALUES (:'uid');", {'uid': uid})
    for name, value, expected in [('missing', '', 401), ('invalid', 'invalid', 401), ('admin', token, 200),
                                  ('service', fixture.values['SERVICE_ROLE_KEY'], 401), ('anon', fixture.values['ANON_KEY'], 401)]:
        status = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=value)[0]
        check('perimeter_' + name, status == expected, http_status=status)
    for path in ('/_p2_auth', '/internal/auth/verify'):
        check('hidden_' + path.rsplit('/', 1)[-1], smoke.http_request(endpoint, 'GET', path, token=token)[0] == 404)
    for name, value, expected in [('missing', '', 401), ('invalid', 'invalid', 401), ('valid', token, 200)]:
        status, _ = direct_go(smoke, registry, containers, value)
        check('direct_' + name, status == expected, http_status=status)
    check('direct_internal_hidden', direct_go(smoke, registry, containers, token, '/internal/auth/verify')[0] == 404)
    check('websocket_stub', smoke.http_request(endpoint, 'GET', '/v1/ws', token=token)[0] == 501)
    status = smoke.http_request(endpoint, 'OPTIONS', '/v1/gateways', headers={'Origin': 'https://client.example.test',
        'Access-Control-Request-Method': 'GET', 'Access-Control-Request-Headers': 'authorization'})[0]
    check('cors_preflight', status == 204, http_status=status)
    check('cors_denied', smoke.http_request(endpoint, 'OPTIONS', '/v1/gateways', headers={
        'Origin': 'https://denied.example.test', 'Access-Control-Request-Method': 'GET'})[0] == 403)
    check('login_requires_api_key', smoke.http_request(endpoint, 'POST', '/auth/v1/token?grant_type=password',
        body={'email': 'not-a-user@example.test', 'password': 'not-a-password'})[0] == 401)
    _, metrics_before_pause = direct_go(smoke, registry, containers, token)
    ws_before = next(r['duration']['count'] for r in metrics_before_pause['http'] if r['route'] == 'GET /v1/ws')
    backend_id = containers['backend']['receipt']['id']
    result = registry.command(['exec', backend_id, 'mv', '/run/iot-auth/auth.sock', '/run/iot-auth/auth.paused'])
    check('upstream_pause', result.returncode == 0)
    try:
        status = smoke.http_request(endpoint, 'GET', '/v1/ws', token=token)[0]
        check('upstream_fail_closed', status == 500, http_status=status)
        _, metrics_during_pause = direct_go(smoke, registry, containers, token)
        ws_during = next(r['duration']['count'] for r in metrics_during_pause['http'] if r['route'] == 'GET /v1/ws')
        check('upstream_failure_never_enters_business', ws_before == ws_during)
    finally:
        result = registry.command(['exec', backend_id, 'mv', '/run/iot-auth/auth.paused', '/run/iot-auth/auth.sock'])
        check('upstream_restore', result.returncode == 0)
    check('upstream_recovered', smoke.http_request(endpoint, 'GET', '/v1/ws', token=token)[0] == 501)
    run['status'] = 'PASS'
    smoke.durable_json(fixture.artifacts / 'run.json', run)
    return run


def rotation_documents(k1_keys, k2, k2_public):
    k1 = k1_keys[0]
    k1_public = {k: v for k, v in k1.items() if k not in ('d', 'p', 'q', 'dp', 'dq', 'qi')}
    k1_public['key_ops'] = ['verify']
    legacy = k1_keys[1]
    return ([k1, k2_public, legacy], [k1_public, k2, legacy], {'keys': [k1_public, k2_public]})


def replace_owned_services(smoke, fixture, registry, containers, state, phase, keys, public, roles):
    names = {'supabase-auth': fixture.project + '-auth-' + phase,
             'supabase-storage': fixture.project + '-storage-overlap'}
    fragment = fixture.private / ('compose.rotation-' + phase + '.yml')
    smoke.private_write(fragment, 'services:\n  supabase-auth:\n    container_name: ' + names['supabase-auth']
        + '\n    environment:\n      GOTRUE_JWT_KEYS: ' + json.dumps(json.dumps(keys))
        + '\n  supabase-storage:\n    container_name: ' + names['supabase-storage']
        + '\n    environment:\n      JWT_JWKS: ' + json.dumps(json.dumps(public)) + '\n')
    for role in roles:
        old = containers[role]
        registry.remove(old)  # Exact owned ID; never reset a receipt to accept a replacement.
        item = registry.intent('container', names[role], old['image'], old['labels'])
        item['expectation'] = old['expectation']
        registry.save(item)
        if registry.resolve(item) is not None:
            raise ValueError('resource_collision')
        registry.allocating(item)
        containers[role] = item
    command = smoke.compose_command(fixture, 'config')[:-3] + ['-f', str(fragment), 'up', '--detach',
        '--no-deps', '--no-build', '--pull', 'never', *roles]
    result = subprocess.run(command, capture_output=True, env=smoke.process_env(fixture), timeout=smoke.BUDGETS['up'])
    smoke.private_write(fixture.private / ('rotation-' + phase + '.log'),
                        (result.stdout + result.stderr).decode(errors='replace'))
    for role in roles:
        registry.resolve(containers[role])
    if result.returncode:
        raise ValueError('paper_p2_rotation_failed')
    deadline = time.monotonic() + 45
    for role in roles:
        while True:
            state.checkpoint()
            obj = registry.resolve(containers[role])
            if obj and smoke.startup_state(role, obj['State']):
                break
            if time.monotonic() >= deadline:
                raise ValueError('paper_p2_rotation_failed')
            time.sleep(0.5)


def sign_fixture_token(fixture, claims, kid):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    key = serialization.load_pem_private_key((fixture.private / 'p2-k1.pem').read_bytes(), password=None)
    head = b64url(json.dumps({'alg': 'RS256', 'typ': 'JWT', 'kid': kid}).encode())
    body = b64url(json.dumps(claims).encode())
    raw = (head + '.' + body).encode()
    return raw.decode() + '.' + b64url(key.sign(raw, padding.PKCS1v15(), hashes.SHA256()))


def decode_token(raw):
    return [json.loads(base64.urlsafe_b64decode(p + '=' * (-len(p) % 4))) for p in raw.split('.')[:2]]


def storage_api_probe(request, path, token, api_key, expected, record, persist, save_private):
    """Bounded API readback; never retry auth/policy, TLS or content failures."""
    record.update(status='FAIL', attempts=[])
    deadline = time.monotonic() + 30
    for attempt in range(1, 21):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        body, status, retry = None, None, False
        try:
            status, _, body = request('GET', path, token=token, api_key=api_key, timeout=min(2, remaining))
            matched = isinstance(body, list) if expected == 'bucket_list' else body == {'probe': 'p2-fixture'}
            success = status == 200 and matched
            retry = status in (502, 503, 504)
            category = ('ok' if success else 'unexpected_content' if status == 200 else
                        'auth_denied' if status == 401 else 'policy_denied' if status == 403 else
                        'upstream_unavailable' if retry else 'storage_request_failed')
        except ValueError as exc:
            success = False
            code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else ''
            retry = code in {'http_request_failed: TimeoutError', 'http_request_failed: ConnectionRefusedError',
                             'http_request_failed: ConnectionResetError', 'http_request_failed: RemoteDisconnected'}
            category = 'transport_unavailable' if retry else 'tls_validation_failed' if code in {
                'http_request_failed: SSLCertVerificationError', 'http_request_failed: SSLError'} else 'request_failed'
        entry = {'attempt': attempt, 'http_status': status, 'category': category}
        # Error text can contain server internals. Only fixed codes enter artifacts.
        if isinstance(body, dict):
            code = body.get('code', body.get('error'))
            if isinstance(code, str) and code in {'AccessDenied', 'InvalidJWT', 'InvalidToken', 'InvalidSignature',
                    'NoSuchKey', 'NoSuchBucket', 'InternalError', 'DatabaseError', 'JWTExpired'}:
                entry['storage_code'] = code
        if not success:
            save_private(body, attempt)
        record['attempts'].append(entry)
        record['status'] = 'PASS' if success else 'FAIL'
        persist()
        if success or not retry:
            return success
        if time.monotonic() >= deadline:
            break
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    return False


def storage_pair_probe(request, path, token1, token2, api_key, records, persist, save_private):
    results = []
    for label, token in (('K1', token1), ('K2', token2)):
        record = records.setdefault(label, {})
        results.append(storage_api_probe(request, path, token, api_key, 'fixture_object', record,
                       persist, lambda body, attempt: save_private(label, body, attempt)))
    return all(results)  # Both actual reads have already run, even when K1 failed.


def run_full(smoke, fixture, registry, endpoint, containers, images, state):
    perimeter = run_perimeter(smoke, fixture, registry, endpoint, containers, images, state)
    perimeter.update(full_p2_accepted=False, mode='paper_p2', issuer=fixture.values['GOTRUE_JWT_ISSUER'],
                     jwks_endpoint=fixture.values['SUPABASE_JWKS_URL'], socket_round_trip_seconds=None)

    def checks(pg, tokens, specs, sensor_path, run):
        def check(name, success, **metadata):
            run['checks'][name] = dict(status='PASS' if success else 'FAIL', **metadata)
            smoke.durable_json(fixture.artifacts / 'run.json', run)
            if not success:
                raise ValueError('paper_p2_acceptance_failed')
        def request(method, path, token=tokens['admin'], **kwargs):
            return smoke.http_request(endpoint, method, path, token=token, **kwargs)

        # Negative user claims are signed with the fixture's real K1, not a mock verifier.
        header, claims = decode_token(tokens['admin'])
        expired = dict(claims, iat=int(time.time()) - 180, exp=int(time.time()) - 120)
        invalid_signature = tokens['admin'].rsplit('.', 1)[0] + '.' + b64url(b'wrong-signature')
        for name, bad in [('expired', sign_fixture_token(fixture, expired, header['kid'])),
                          ('signature', invalid_signature)]:
            for route in ('perimeter', 'direct'):
                status = request('GET', '/v1/gateways', token=bad)[0] if route == 'perimeter' else direct_go(smoke, registry, containers, bad, '/v1/gateways')[0]
                check(name + '_' + route, status == 401, http_status=status)
        check('admin_does_not_bypass_membership', request('GET', sensor_path)[0] == 404)
        check('identity_header_spoof_denied', request('GET', '/v1/admin/metrics', token=tokens['a'],
            headers={'X-User-ID': claims['sub'], 'X-Role': 'service_role', 'X-Forwarded-User': claims['sub']})[0] == 403)

        email, password = 'p2-created-' + secrets.token_hex(6) + '@example.test', secrets.token_urlsafe(24)
        status, _, body = request('POST', '/v1/admin/users', body={'full_name': 'P2 fixture user', 'email': email, 'password': password})
        check('go_auth_admin_create', status == 201 and isinstance(body, dict) and bool(body.get('id')), http_status=status)
        uid = body['id']
        user_token = smoke.login_user_for_token(endpoint, fixture.values['ANON_KEY'], email, password)
        check('new_user_login_rs256', decode_token(user_token)[0].get('alg') == 'RS256')
        status, _, _ = request('PATCH', '/v1/admin/users/' + uid, body={'full_name': 'P2 updated fixture'})
        check('business_patch_preserves_body', status == 200 and smoke.db_psql(registry, pg,
            "SELECT count(*) FROM profiles WHERE id=:'uid'::uuid AND full_name='P2 updated fixture';", {'uid': uid}).strip() == '1', http_status=status)
        check('business_body_limit', request('POST', '/v1/admin/users', body={'full_name': 'x' * 17000})[0] == 413)
        member_path = '/v1/admin/gateways/' + specs[0].gateway_id + '/users/' + uid
        check('membership_assign', request('PUT', member_path, body={'role': 'viewer'})[0] == 200)
        bracket = [request('GET', sensor_path, token=user_token)[0]]
        check('membership_remove', request('DELETE', member_path)[0] == 204)
        bracket.append(request('GET', sensor_path, token=user_token)[0])
        check('membership_reassign', request('PUT', member_path, body={'role': 'viewer'})[0] == 200)
        bracket.append(request('GET', sensor_path, token=user_token)[0])
        check('membership_same_token', bracket == [200, 404, 200], statuses=bracket)

        service, anon = fixture.values['SERVICE_ROLE_KEY'], fixture.values['ANON_KEY']
        bucket, object_path = 'p2-private', 'probe.json'
        status = request('POST', '/storage/v1/bucket', token=service, api_key=service,
                         body={'id': bucket, 'name': bucket, 'public': False})[0]
        check('storage_private_create', status == 200, http_status=status)
        status = request('POST', '/storage/v1/object/' + bucket + '/' + object_path,
                         token=service, api_key=service, body={'probe': 'p2-fixture'})[0]
        check('storage_upload_fixture_object', status == 200, http_status=status)
        smoke.db_psql(registry, pg, """CREATE POLICY p2_fixture_read ON storage.objects FOR SELECT TO authenticated
USING (bucket_id='p2-private' AND name='probe.json' AND (SELECT auth.uid())=:'uid'::uuid);""", {'uid': uid})
        object_url = '/storage/v1/object/authenticated/' + bucket + '/' + object_path
        status, _, obj = request('GET', object_url, token=user_token, api_key=anon)
        check('storage_rs256_private_read', status == 200 and obj == {'probe': 'p2-fixture'}, http_status=status)
        status, _, obj = request('GET', object_url, token=tokens['a'], api_key=anon)
        check('storage_private_other_user_denied', status in (400, 403, 404) and obj != {'probe': 'p2-fixture'}, http_status=status)
        status, _, signed = request('POST', '/storage/v1/object/sign/' + bucket + '/' + object_path,
                                    token=user_token, api_key=anon, body={'expiresIn': 120})
        signed_path = signed.get('signedURL', '') if isinstance(signed, dict) else ''
        check('storage_signed_url_create', status == 200 and signed_path.startswith('/object/sign/p2-private/probe.json?'), http_status=status)
        signed_status, _, signed_obj = request('GET', '/storage/v1' + signed_path, token='')
        check('storage_signed_url_read', signed_status == 200 and signed_obj == {'probe': 'p2-fixture'}, http_status=signed_status)

        before = request('GET', '/v1/admin/metrics')[2]
        key1 = json.loads((fixture.private / 'p2-signing-keys.json').read_text())
        k2, public2, _ = rsa_jwk('p2-k2-' + secrets.token_hex(8))
        overlap, switched, public = rotation_documents(key1, k2, public2)
        replace_owned_services(smoke, fixture, registry, containers, state, 'overlap', overlap, public,
                               ('supabase-auth', 'supabase-storage'))
        diagnostics = {'readiness_bucket_api': {'status': 'NOT_RUN'},
                       'readiness_object_api': {'status': 'NOT_RUN'},
                       'K1': {'status': 'NOT_RUN'}, 'K2': {'status': 'NOT_RUN'}}
        run['storage_rotation_diagnostics'] = diagnostics
        def persist_storage():
            state.checkpoint()
            smoke.durable_json(fixture.artifacts / 'run.json', run)
        def save_storage(label, body, attempt):
            smoke.private_write(fixture.private / ('storage-' + label + '-' + str(attempt) + '.json'), json.dumps(body))
        persist_storage()
        ready_bucket = storage_api_probe(request, '/storage/v1/bucket', service, service, 'bucket_list',
            diagnostics['readiness_bucket_api'], persist_storage,
            lambda body, attempt: save_storage('readiness-bucket', body, attempt))
        check('rotation_storage_bucket_api_ready', ready_bucket)
        ready_object = storage_api_probe(request, object_url, service, service, 'fixture_object',
            diagnostics['readiness_object_api'], persist_storage,
            lambda body, attempt: save_storage('readiness-object', body, attempt))
        check('rotation_storage_object_api_ready', ready_object)
        def wait_request(method, path, expected, **kwargs):
            deadline = time.monotonic() + 30
            while True:
                state.checkpoint()
                response = request(method, path, **kwargs)
                if response[0] == expected:
                    return response
                if time.monotonic() >= deadline:
                    raise ValueError('paper_p2_rotation_failed')
                time.sleep(0.5)
        _, _, jwks = wait_request('GET', '/auth/v1/.well-known/jwks.json', 200, token='')
        check('rotation_overlap_public_keys', {k['kid'] for k in jwks.get('keys', [])} == {key1[0]['kid'], k2['kid']})
        check('rotation_overlap_k1', request('GET', '/v1/admin/metrics')[0] == 200)
        replace_owned_services(smoke, fixture, registry, containers, state, 'k2', switched, public, ('supabase-auth',))
        _, _, login = wait_request('POST', '/auth/v1/token?grant_type=password', 200, token='',
            api_key=anon, body={'email': email, 'password': password})
        token2 = login.get('access_token', '')
        check('rotation_new_signer', bool(token2) and decode_token(token2)[0].get('kid') == k2['kid'])
        wait_request('GET', sensor_path, 200, token=token2)
        check('rotation_go_refresh_and_k1_overlap', request('GET', sensor_path, token=user_token)[0] == 200)
        pair_ok = storage_pair_probe(request, object_url, user_token, token2, anon,
                                    diagnostics, persist_storage, save_storage)
        for label in ('K1', 'K2'):
            run['checks']['rotation_storage_' + label.lower()] = {
                'status': diagnostics[label]['status'], 'http_status': diagnostics[label]['attempts'][-1]['http_status']}
        check('rotation_storage_new_and_old', pair_ok)
        check('rotation_signed_url_survives', request('GET', '/storage/v1' + signed_path, token='')[0] == 200)
        check('rotation_legacy_service_survives', request('GET', '/storage/v1/bucket', token=service, api_key=service)[0] == 200)
        check('rotation_auth_admin_survives', request('POST', '/v1/admin/users', body={
            'full_name': 'P2 post rotation', 'email': 'p2-after-' + secrets.token_hex(6) + '@example.test',
            'password': secrets.token_urlsafe(24)})[0] == 201)
        after = request('GET', '/v1/admin/metrics')[2]
        check('rotation_same_go_process', before['process_instance'] == after['process_instance'])
        run['rotation'] = {'status': 'PASS', 'k1': key1[0]['kid'], 'k2': k2['kid'], 'overlap': True,
                           'go_restarted': False, 'auth_recreates': 2, 'storage_recreates': 1,
                           'static_storage_jwks_updated': True, 'cache_ttl_seconds': 60}
        measured, auth_samples = measure_auth(smoke, endpoint, tokens['admin'])
        run['auth_measurements'] = measured
        smoke.durable_json(fixture.artifacts / 'run.json', run)
        return auth_samples

    result = smoke.run_paper_p1(fixture, registry, endpoint, containers, images, state,
                                 extra=perimeter, before_workload=checks)
    result['full_p2_accepted'] = True
    smoke.durable_json(fixture.artifacts / 'run.json', result)
    return result


def measure_auth(smoke, endpoint, token):
    status, _, before = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=token)
    if status != 200:
        raise ValueError('paper_p2_acceptance_failed')
    bounds = before['auth']['verify']['upper_seconds']
    client = {'upper_seconds': bounds, 'buckets': [0] * len(bounds), 'count': 0, 'sum_seconds': 0, 'overflow': 0}
    for _ in range(16):
        started = time.monotonic()
        status = smoke.http_request(endpoint, 'GET', '/v1/gateways', token=token)[0]
        elapsed = time.monotonic() - started
        if status != 200:
            raise ValueError('paper_p2_acceptance_failed')
        client['count'] += 1
        client['sum_seconds'] += elapsed
        for i, upper in enumerate(bounds):
            if elapsed <= upper:
                client['buckets'][i] += 1
                break
        else:
            client['overflow'] += 1
    after = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=token)[2]
    after['client_https'] = client
    measured = {'requests': 16, 'client_https': client, 'business_verify_cumulative': after['auth']['verify'],
            'unix_verify_cumulative': after['unix_auth']['verify'], 'unix_handler_cumulative': after['unix_auth_handler'],
            'cache_counters': after['jwks_cache'], 'socket_round_trip_seconds': None}
    return measured, [{'phase': 'before_auth_requests', 'snapshot': before},
                      {'phase': 'after_auth_requests', 'snapshot': after}]
