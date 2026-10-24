#!/usr/bin/env python3
"""Task 5.4 only: extend the existing owned Stage 3 broker+DB fixture lifecycle.

No Stage 5 E2E, Gateway scheduler, result receiver, live discovery or deployment.
"""
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import shlex
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request

from stage3_mqtt_ingress_runner import (
    ROOT, MQTT, RESPONSE, _Endpoint, connect_manager, container_logs, docker, docker_create,
    mapped_port, remove_container, run, wait_http, wait_tcp, write_private,
)

sys.path.insert(0, str(ROOT / 'scripts'))
from mqtt_backend_acl import ACLUpdateError, expected_acls, role_acls, update_existing_backend_role


def connect(port, work, user, password):
    return MQTT(_Endpoint(port, work), user, password)


def quiet(client):
    client.s.settimeout(.15)
    try:
        client.packet()
    except socket.timeout:
        return
    finally:
        client.s.settimeout(2)
    raise RuntimeError('denied namespace delivered a packet')


def acl_proof(port, work, manager_password, backend_password, gateway_password):
    manager = connect_manager(port, work, manager_password)
    manager.subscribe(RESPONSE)
    try:
        # Rehearse an EXISTING store with original Stage 3 four-permission role.
        for kind, topic, _, _ in sorted(expected_acls(True) - expected_acls(False)):
            manager.request('removeRoleACL', rolename='bootstrap_backend', acltype=kind, topic=topic)
        if role_acls(manager.request('getRole', rolename='bootstrap_backend')) != expected_acls(False):
            raise RuntimeError('existing-store precondition mismatch')
        print('PASS existing-store original exact ACL readback', flush=True)
        # Snapshot persistence is read by the caller separately; hashes never print.
        return manager
    except BaseException:
        manager.close()
        raise


def exact_native_acls(manager, port, work, backend_password, gateway_password):
    backend = connect(port, work, 'bootstrap_backend', backend_password)
    gateway = connect(port, work, 'stage54_gateway', gateway_password)
    other = connect(port, work, 'stage54_other', gateway_password)
    try:
        backend.subscribe('gateways/+/responses/#')
        backend.subscribe('gateways/+/telemetry/#')
        # Real UNSUBACK then resubscribe the exact filter.
        backend.pid += 1
        pid = struct.pack('!H', backend.pid)
        from task26_harness_helpers import text
        backend.send(0xa2, pid + text('gateways/+/responses/#'))
        if backend.packet() != (0xb0, pid):
            raise RuntimeError('responses UNSUBACK mismatch')
        backend.subscribe('gateways/+/responses/#')
        for topic in ('#', 'gateways/#', 'gateways/+/commands/#',
                      'gateways/stage54_gateway/responses/#', RESPONSE):
            backend.subscribe(topic, allowed=False)
        gateway.subscribe('gateways/stage54_gateway/commands/#')
        gateway.subscribe('gateways/stage54_gateway/acks/#')
        other.subscribe('gateways/stage54_other/commands/#')
        gateway.subscribe('gateways/stage54_other/commands/#', allowed=False)
        gateway.subscribe('gateways/+/commands/#', allowed=False)
        own = 'gateways/stage54_gateway/commands/native_acl'
        backend.publish(own, b'positive-command')
        if gateway.message() != (own, b'positive-command'):
            raise RuntimeError('command publish positive missing')
        quiet(other)
        response = 'gateways/stage54_gateway/responses/native_acl'
        gateway.publish(response, b'positive-response')
        if backend.message() != (response, b'positive-response'):
            raise RuntimeError('response receive positive missing')
        gateway.publish('gateways/stage54_other/responses/spoof', b'denied')
        quiet(backend)
        gateway.publish(own, b'denied-gateway-command')
        quiet(gateway)
        gateway.publish('gateways/stage54_gateway/acks/spoof', b'denied-ack')
        quiet(gateway)
        backend.publish('gateways/stage54_gateway/acks/positive', b'positive-ack')
        if gateway.message()[1] != b'positive-ack':
            raise RuntimeError('ACK positive missing')
        backend.publish(response, b'denied-backend-response')
        quiet(backend)
        backend.publish('$CONTROL/dynamic-security/v1', b'{"commands":[{"command":"getRole","rolename":"bootstrap_backend","correlationData":"denied"}]}')
        quiet(manager)
        manager.request('getRole', rolename='bootstrap_backend')  # positive control, still alive
        print('PASS native exact ACL send/receive/subscribe/unsubscribe positive/negative controls', flush=True)
    finally:
        backend.close()
        gateway.close()
        other.close()


def exercise(fixture, work, broker, port, manager_password, backend_password, gateway_password, server, network):
    # Same native binary/container pattern used by Task 5.2/5.3.
    binary = work / 'control-mqtt.test'
    subprocess.run(['go', 'test', '-cover', '-c', '-o', str(binary), './internal/digitaltwin'],
                   cwd=ROOT / 'src', env={**os.environ, 'CGO_ENABLED': '0'}, check=True, timeout=180)
    docker(['cp', str(binary), fixture.container_id + ':/tmp/control-mqtt.test'])
    docker(['cp', str(work / 'ca.crt'), fixture.container_id + ':/tmp/task54-ca.crt'])
    manager = acl_proof(port, work, manager_password, backend_password, gateway_password)
    try:
        manager.request('createClient', username='stage54_shadow', password=gateway_password)
        manager.request('addClientRole', username='stage54_shadow', rolename='bootstrap_backend')
        try:
            update_existing_backend_role(manager.request, 'bootstrap_backend', exclusive_admin_confirmed=True)
        except ACLUpdateError:
            pass
        else:
            raise RuntimeError('shared existing-store role was not refused')
        if role_acls(manager.request('getRole', rolename='bootstrap_backend')) != expected_acls(False):
            raise RuntimeError('refused shared role changed ACLs')
        manager.request('removeClientRole', username='stage54_shadow', rolename='bootstrap_backend')
        manager.request('deleteClient', username='stage54_shadow')
        print('PASS native operator shared-role refusal with unchanged ACL readback', flush=True)
        before = json.loads(docker(['exec', broker, 'cat', '/mosquitto/dynsec/dynamic-security.json']))
        update_existing_backend_role(manager.request, 'bootstrap_backend', exclusive_admin_confirmed=True)
        # Verify RAM and persisted content, including unchanged credential hashes.
        after = json.loads(docker(['exec', broker, 'cat', '/mosquitto/dynsec/dynamic-security.json']))
        for store in (before, after):
            for role in store['roles']:
                if role['rolename'] == 'bootstrap_backend':
                    role.pop('acls')
        if before != after:
            raise RuntimeError('ACL upgrade changed credentials/identity/other roles/defaults')
        update_existing_backend_role(manager.request, 'bootstrap_backend', exclusive_admin_confirmed=True)
        print('PASS isolated existing-store operator update/readback/idempotence and credential/store preservation', flush=True)
        for user in ('stage54_gateway', 'stage54_other'):
            manager.request('createRole', rolename='gateway_' + user, acls=[
                {'acltype': kind, 'topic': f'gateways/{user}/{suffix}/#', 'allow': True, 'priority': 0}
                for suffix in ('commands', 'acks') for kind in ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')]
                + [{'acltype': 'publishClientSend', 'topic': f'gateways/{user}/{suffix}/#', 'allow': True, 'priority': 0} for suffix in ('responses', 'telemetry')])
            manager.request('createClient', username=user, password=gateway_password)
            manager.request('addClientRole', username=user, rolename='gateway_' + user)
        exact_native_acls(manager, port, work, backend_password, gateway_password)
    finally:
        manager.close()
    docker(['stop', '-t', '5', broker])
    phase_path = '/tmp/task54-phase'

    def phase(expected):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            result = docker(['exec', fixture.container_id, 'sh', '-c', 'if test -f /tmp/task54-phase; then cat /tmp/task54-phase; fi'])
            if result.decode().strip() == expected:
                return
            time.sleep(.05)
        raise RuntimeError('native phase missing: ' + expected)

    script = 'set -e\nexport CONTROL_TEST_ISOLATED=1 CONTROL_MQTT_ISOLATED=1\n'
    script += 'export CONTROL_TEST_SETUP_URL="postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"\n'
    script += 'export CONTROL_TEST_BACKEND_URL="postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"\n'
    for key, value in {'CONTROL_MQTT_MANAGER_PASSWORD': manager_password, 'CONTROL_MQTT_BACKEND_PASSWORD': backend_password,
                       'CONTROL_MQTT_GATEWAY_PASSWORD': gateway_password}.items():
        script += 'export ' + key + '=' + shlex.quote(value) + '\n'
    script += 'exec /tmp/control-mqtt.test -test.v -test.timeout=100s -test.coverprofile=/tmp/control-mqtt.cover -test.run "^TestNativeMQTTControlDurableLifecycle$"\n'
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(run, ['docker', 'exec', '-i', fixture.container_id, 'sh'], input_data=script.encode(), timeout=120)
        try:
            phase('intent_committed_offline')
            docker(['start', broker]); port = mapped_port(broker, '8883/tcp'); wait_tcp(port, 15)
            phase('restart_with_extra_acl')
            manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
            manager.request('addRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='#', allow=True, priority=0)
            manager.close()
            docker(['restart', '-t', '5', broker]); port = mapped_port(broker, '8883/tcp'); wait_tcp(port, 15)
            docker(['exec', fixture.container_id, 'sh', '-c', 'printf extra_acl_live > /tmp/task54-action'])
            phase('extra_acl_rejected')
            manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
            manager.request('removeRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='#'); manager.close()
            phase('reconnected_verified')
            manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
            manager.request('removeRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='gateways/+/commands/#'); manager.close()
            docker(['restart', '-t', '5', broker]); port = mapped_port(broker, '8883/tcp'); wait_tcp(port, 15)
            docker(['exec', fixture.container_id, 'sh', '-c', 'printf missing_acl_live > /tmp/task54-action'])
            phase('missing_acl_rejected')
            manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
            manager.request('addRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='gateways/+/commands/#', allow=True, priority=0); manager.close()
            phase('complete')
        finally:
            output = result.result().decode()
            print(output, flush=True)
    if '--- PASS: TestNativeMQTTControlDurableLifecycle (' not in output or '--- SKIP:' in output:
        raise RuntimeError('native MQTT test did not pass without skips')
    coverage = os.environ.get('CONTROL_MQTT_COVERAGE_FILE')
    if coverage:
        Path(coverage).write_bytes(docker(['exec', fixture.container_id, 'cat', '/tmp/control-mqtt.cover']))
    print('PASS Task 5.4 native DB+Mosquitto required lifecycle proof; no result ingestion', flush=True)

    # Composition/lifecycle only: no HTTP->Gateway result flow or Stage 5 E2E.
    env = {
        'DATABASE_URL': f'postgres://iot_backend_app:{fixture.passwords[1]}@database:5432/fixture_service?sslmode=disable',
        'SUPABASE_JWT_SECRET': 'isolated-stage54-jwt-secret-01234567890123456789',
        'SUPABASE_JWT_ISSUER': 'http://auth.invalid/auth/v1', 'SUPABASE_JWT_AUDIENCE': 'authenticated',
        'SERVER_ENV': 'smoke', 'MQTT_CREDENTIAL_API_ENABLED': 'false', 'MQTT_CREDENTIAL_RUNTIME_ENABLED': 'false',
        'STAGE5_ENABLED': 'true', 'MQTT_INGRESS_ENABLED': 'true', 'MQTT_WORKER_COUNT': '2',
        'MQTT_BROKER_URL': 'ssl://broker:8883', 'MQTT_CLIENT_ID': 'stage54-ingress',
        'STAGE5_MQTT_CLIENT_ID': 'stage54-command', 'MQTT_USERNAME': 'bootstrap_backend',
        'MQTT_PASSWORD': backend_password, 'MQTT_TLS_CA_FILE': '/certs/ca.crt',
        'MQTT_INTERNAL_MANAGEMENT_URL': 'ssl://broker:8883', 'MQTT_DYNSEC_MANAGER_USERNAME': 'bootstrap_manager',
        'MQTT_DYNSEC_MANAGER_PASSWORD': manager_password, 'DATABASE_MAX_CONNS': '10', 'SHUTDOWN_TIMEOUT': '10s',
        'STAGE5_RECONNECT_MAX_INTERVAL': '100ms',
    }
    env_file = work / 'stage54-backend.env'
    write_private(env_file, ''.join(f'{k}={v}\n' for k, v in env.items()).encode())
    backend = None
    try:
        backend = docker_create([
            '--user', '1883:1883', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--read-only',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m', '--network', network,
            '--publish', '127.0.0.1::8080', '--env-file', str(env_file),
            '--mount', f'type=bind,src={work / "public" / "ca.crt"},dst=/certs/ca.crt,readonly',
            '--mount', f'type=bind,src={server},dst=/app/server,readonly',
            '--entrypoint', '/app/server',
            'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'])
        docker(['start', backend])
        http_port = mapped_port(backend, '8080/tcp')
        wait_http(http_port, '/readyz', 20)
        wait_http(http_port, '/healthz', 10)

        def not_ready():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    urllib.request.urlopen(f'http://127.0.0.1:{http_port}/readyz', timeout=1).close()
                except urllib.error.HTTPError as error:
                    if error.code == 503:
                        return
                    raise
                time.sleep(.05)
            raise RuntimeError('enabled Stage 5 was incorrectly ready')

        docker(['stop', '-t', '5', broker]); not_ready()
        docker(['start', broker]); port = mapped_port(broker, '8883/tcp'); wait_tcp(port, 15)
        wait_http(http_port, '/readyz', 20)
        manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
        manager.request('addRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='#', allow=True, priority=0); manager.close()
        docker(['restart', '-t', '5', broker]); port = mapped_port(broker, '8883/tcp'); wait_tcp(port, 15)
        not_ready(); wait_http(http_port, '/healthz', 5)
        manager = connect_manager(port, work, manager_password); manager.subscribe(RESPONSE)
        manager.request('removeRoleACL', rolename='bootstrap_backend', acltype='publishClientSend', topic='#'); manager.close()
        wait_http(http_port, '/readyz', 20)
        start = time.monotonic()
        docker(['stop', '-t', '12', backend], timeout=20)
        code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend]).decode().strip()
        logs = container_logs(backend)
        if code != '0' or time.monotonic() - start > 10 or 'backend shutdown complete' not in logs or 'backend shutdown incomplete' in logs:
            raise RuntimeError('combined HTTP/ingress/control shutdown failed')
        print('PASS native composed /readyz ACL/reconnect, /healthz, independent disabled credential API and shared shutdown budget', flush=True)
    finally:
        if backend:
            remove_container(backend)


if __name__ == '__main__':
    from stage3_mqtt_ingress_runner import main
    if main(['--mode', 'control']) != 0:
        raise SystemExit(1)
