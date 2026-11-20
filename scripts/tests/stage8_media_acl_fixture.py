#!/usr/bin/env python3
"""Owned local Mosquitto proof for the opt-in Stage 8 media ACL profile."""
import json
from pathlib import Path
import secrets
import shutil
import struct
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'scripts/tests'))
import mqtt_backend_acl as acl  # noqa: E402
import stage3_mqtt_ingress_runner as stage3  # noqa: E402
from stage3_mqtt_ingress_runner import (  # noqa: E402
    MOSQUITTO_IMAGE, _Endpoint, docker, docker_create, generate_certs, mapped_port,
    docker_available, remove_container, run, volume_absent, wait_tcp, write_private,
)
from task26_harness_helpers import MQTT  # noqa: E402


def client(port, work, username, password):
    return MQTT(_Endpoint(port, work), username, password)


def sub_denied(mqtt, topic):
    mqtt.pid += 1
    pid = struct.pack('!H', mqtt.pid)
    from stage3_mqtt_ingress_runner import mqtt_text
    mqtt.send(0x82, pid + mqtt_text(topic) + b'\x01')
    header, body = mqtt.packet()
    if header != 0x90 or body != pid + b'\x80':
        raise RuntimeError(f'expected denied SUBACK for {topic}')


def publish_qos1(mqtt, topic, payload):
    mqtt.pid += 1
    pid = struct.pack('!H', mqtt.pid)
    from stage3_mqtt_ingress_runner import mqtt_text
    mqtt.send(0x32, mqtt_text(topic) + pid + payload)
    header, body = mqtt.packet()
    if header != 0x40 or body != pid:
        raise RuntimeError('QoS1 PUBACK mismatch')


def subscribe_qos1(mqtt, topic):
    mqtt.pid += 1
    pid = struct.pack('!H', mqtt.pid)
    from stage3_mqtt_ingress_runner import mqtt_text
    mqtt.send(0x82, pid + mqtt_text(topic) + b'\x01')
    header, body = mqtt.packet()
    if header != 0x90 or body != pid + b'\x01':
        raise RuntimeError(f'QoS1 SUBACK mismatch: {topic}')


def no_delivery(mqtt, label):
    mqtt.s.settimeout(.5)
    try:
        mqtt.packet()
    except (TimeoutError, OSError):
        return
    finally:
        mqtt.s.settimeout(2)
    raise RuntimeError(f'unexpected delivery: {label}')


def receive_qos1(mqtt):
    header, body = mqtt.packet()
    if header & 0xF0 != 0x30 or (header >> 1) & 0x03 != 1:
        raise RuntimeError('expected QoS1 PUBLISH delivery')
    topic_len = struct.unpack('!H', body[:2])[0]
    topic = body[2:2 + topic_len].decode()
    packet_id = body[2 + topic_len:4 + topic_len]
    mqtt.send(0x40, packet_id)
    return topic, body[4 + topic_len:]


def exact_role(manager, name, expected):
    got = acl.role_acls(manager.request('getRole', rolename=name), name)
    if got != expected:
        raise RuntimeError(f'exact ACL readback mismatch: {name}')


def credential_snapshot(broker):
    store = json.loads(docker([
        'exec', broker, 'cat', '/mosquitto/dynsec/dynamic-security.json',
    ]))
    return {
        c['username']: {key: c.get(key) for key in ('password', 'salt', 'iterations')}
        for c in store['clients']
    }


def run_fixture():
    if not docker_available():
        print('SKIP: Docker daemon/CLI unavailable')
        return 0
    base = Path('/var/tmp/opencode')
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='stage8-media-acl-', dir=base))
    work.chmod(0o700)
    manager_pw, backend_pw = secrets.token_urlsafe(40), secrets.token_urlsafe(40)
    gateway_pw = {g: secrets.token_urlsafe(40) for g in ('media_a', 'media_b')}
    stage3.SECRETS.extend([manager_pw, backend_pw, *gateway_pw.values()])
    volume = f'stage8_media_dynsec_{secrets.token_hex(8)}'
    cid = None
    primary = None
    try:
        generate_certs(work)
        cfg = work / 'bootstrap.json'
        write_private(cfg, json.dumps({
            'manager': 'bootstrap_manager', 'backend': 'bootstrap_backend',
            'manager_password': manager_pw, 'backend_password': backend_pw,
            'volume_name': volume, 'media_upload_enabled': False,
        }).encode())
        try:
            out = run([sys.executable, str(ROOT / 'scripts/mqtt-bootstrap.py'),
                       '--container', '--config', str(cfg)], timeout=240)
        finally:
            cfg.unlink(missing_ok=True)
        receipt = json.loads(out)
        if receipt.get('status') != 'store_created' or receipt.get('volume_name') != volume:
            raise RuntimeError('owned DynSec bootstrap did not return expected volume')
        cid = docker_create([
            '--name', 'stage8-media-acl-' + secrets.token_hex(6), '--user', '1883:1883',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--read-only',
            '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=16m',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--publish', '127.0.0.1::8883',
            '--mount', f'type=bind,src={work / "broker"},dst=/mosquitto/config,readonly',
            '--mount', f'type=volume,src={volume},dst=/mosquitto/dynsec',
            '--entrypoint', '/usr/sbin/mosquitto', MOSQUITTO_IMAGE,
            '-c', '/mosquitto/config/mosquitto.conf',
        ])
        docker(['start', cid], timeout=45)
        port = mapped_port(cid, '8883/tcp')
        wait_tcp(port, 45)
        manager = client(port, work, 'bootstrap_manager', manager_pw)
        gateway_ids = ('media_a', 'media_b')
        try:
            manager.subscribe('$CONTROL/dynamic-security/v1/response')
            # New bootstrap defaults to the exact non-media Stage 5 role policy.
            exact_role(manager, 'bootstrap_backend', acl.expected_acls(True))
            for gateway in gateway_ids:
                role = f'gateway_{gateway}'
                manager.request('createClient', username=gateway, password=gateway_pw[gateway])
                manager.request('createRole', rolename=role, acls=[
                    {'acltype': k, 'topic': t, 'allow': a, 'priority': p}
                    for k, t, a, p in acl.expected_gateway_acls(gateway)])
                manager.request('addClientRole', username=gateway, rolename=role)
                acl.verify_existing_gateway_media_role(manager.request, gateway, False)
            # A disabled but exactly owned role may be upgraded without re-enabling it.
            manager.request('disableClient', username='media_a')
            defaults_before = manager.request('getDefaultACLAccess')['acls']
            credentials_before = credential_snapshot(cid)
            control_before = acl.role_acls(manager.request(
                'getRole', rolename='bootstrap_manager'), 'bootstrap_manager')
            # Explicit operator path: only exact owned old role shapes may transition.
            acl.update_existing_backend_media_role(
                manager.request, 'bootstrap_backend', exclusive_admin_confirmed=True)
            for gateway in gateway_ids:
                acl.update_existing_gateway_media_role(
                    manager.request, gateway, exclusive_admin_confirmed=True)
            exact_role(manager, 'bootstrap_backend', acl.expected_backend_media_acls())
            for gateway in gateway_ids:
                acl.verify_existing_gateway_media_role(manager.request, gateway, True)
            revoked = manager.request('getClient', username='media_a')['client']
            if revoked.get('disabled') is not True:
                raise RuntimeError('media ACL transition changed disabled Gateway state')
            if manager.request('getDefaultACLAccess')['acls'] != defaults_before:
                raise RuntimeError('media ACL transition changed default ACL policy')
            if credential_snapshot(cid) != credentials_before:
                raise RuntimeError('media ACL transition changed credential material')
            if acl.role_acls(manager.request(
                    'getRole', rolename='bootstrap_manager'), 'bootstrap_manager') != control_before:
                raise RuntimeError('media ACL transition changed control role')
            # Continue positive MQTT proof only after an explicit fixture-only re-enable.
            manager.request('enableClient', username='media_a')
        finally:
            manager.close()

        backend = client(port, work, 'bootstrap_backend', backend_pw)
        a = client(port, work, 'media_a', gateway_pw['media_a'])
        b = client(port, work, 'media_b', gateway_pw['media_b'])
        try:
            subscribe_qos1(a, 'gateways/media_a/commands/#')
            subscribe_qos1(backend, 'gateways/+/telemetry/#')
            # Existing telemetry uplink and command downlink remain available.
            telemetry = 'gateways/media_a/telemetry/sensor_1'
            publish_qos1(a, telemetry, b'legacy-telemetry')
            if receive_qos1(backend) != (telemetry, b'legacy-telemetry'):
                raise RuntimeError('legacy telemetry control path broken')
            command = 'gateways/media_a/commands/legacy-control'
            publish_qos1(backend, command, b'legacy-command')
            if receive_qos1(a) != (command, b'legacy-command'):
                raise RuntimeError('legacy command control path broken')
            no_delivery(b, 'legacy command cross-Gateway')
            subscribe_qos1(a, 'gateways/media_a/media/down/#')
            subscribe_qos1(b, 'gateways/media_b/media/down/#')
            subscribe_qos1(backend, 'gateways/+/media/up/#')
            # Correct QoS1 uplink: backend observes the payload, independently of PUBACK.
            up_a = 'gateways/media_a/media/up/upload_request'
            publish_qos1(a, up_a, b'{"id":"a"}')
            if receive_qos1(backend) != (up_a, b'{"id":"a"}'):
                raise RuntimeError('correct Gateway A uplink not received')
            # Correct QoS1 downlink and Gateway isolation.
            down_a = 'gateways/media_a/media/down/upload_grant'
            publish_qos1(backend, down_a, b'grant-a')
            if receive_qos1(a) != (down_a, b'grant-a'):
                raise RuntimeError('correct Gateway A downlink not received')
            no_delivery(b, 'Gateway B cross-delivery')
            # Wrong direction and other Gateway filters are denied at SUBACK; a
            # broker PUBACK is not interpreted as evidence that an ACL-denied publish arrived.
            sub_denied(a, 'gateways/media_a/media/up/#')
            sub_denied(a, 'gateways/media_b/media/down/#')
            sub_denied(a, '$CONTROL/dynamic-security/v1/response')
            sub_denied(backend, 'gateways/+/media/down/#')
            sub_denied(backend, '$CONTROL/dynamic-security/v1/#')
            publish_qos1(backend, 'gateways/media_b/media/down/upload_grant', b'grant-b')
            if receive_qos1(b) != ('gateways/media_b/media/down/upload_grant', b'grant-b'):
                raise RuntimeError('Gateway B downlink positive control missing')
            publish_qos1(a, 'gateways/media_a/media/down/upload_grant', b'wrong-direction')
            no_delivery(a, 'Gateway publish to its downlink')
            # Unauthorized Gateway B downlink publish can receive MQTT 3.x PUBACK,
            # so assert lack of delivery to A after an independent positive control.
            publish_qos1(b, 'gateways/media_b/media/up/upload_request', b'{"id":"b"}')
            if receive_qos1(backend)[0] != 'gateways/media_b/media/up/upload_request':
                raise RuntimeError('Gateway B uplink positive control missing')
            publish_qos1(b, 'gateways/media_b/media/down/upload_grant', b'wrong-direction')
            no_delivery(b, 'Gateway B publish to its own downlink')
            no_delivery(a, 'unauthorized Gateway B downlink')
            # Revoke A, then prove fresh auth denial (not just a timeout on an old socket).
            control = client(port, work, 'bootstrap_manager', manager_pw)
            control.subscribe('$CONTROL/dynamic-security/v1/response')
            control.request('disableClient', username='media_a')
            control.close()
            a.close()
            try:
                client(port, work, 'media_a', gateway_pw['media_a'])
            except Exception:
                pass
            else:
                raise RuntimeError('revoked Gateway A credential still authenticated')
            print('PASS: exact off/on readback; QoS1 media up/down; wrong-direction, cross-Gateway, control and revoked-A denials')
        finally:
            for mqtt in (backend, a, b):
                try:
                    mqtt.close()
                except Exception:
                    pass
        return 0
    except Exception as exc:
        primary = exc
        raise
    finally:
        cleanup_errors = []
        if cid:
            try:
                remove_container(cid)
            except Exception as exc:
                cleanup_errors.append('container')
        try:
            docker(['volume', 'rm', volume], timeout=30)
            if not volume_absent(volume):
                cleanup_errors.append('volume still exists')
        except Exception:
            cleanup_errors.append('volume')
        shutil.rmtree(work, ignore_errors=True)
        if work.exists():
            cleanup_errors.append('scratch directory still exists')
        if cleanup_errors:
            print('FAIL: owned cleanup incomplete: ' + ', '.join(cleanup_errors), flush=True)
            if primary is None:
                raise RuntimeError('owned cleanup incomplete')
        else:
            print('PASS: owned broker container, DynSec volume and private scratch cleaned', flush=True)


if __name__ == '__main__':
    raise SystemExit(run_fixture())
