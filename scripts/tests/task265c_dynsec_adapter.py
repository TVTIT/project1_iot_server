#!/usr/bin/env python3
"""Isolated actual native adapter; trusted OPEN seam is NOT a DB service test."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time

from stage2_mosquitto_runtime import ENV, ROOT, run
from task26_dynsec_cases import Spike
from task26_harness_helpers import require
from task265b_lifecycle import IMAGE


def main():
    directory = Path(tempfile.mkdtemp(prefix='task265c-', dir='/tmp/opencode'))
    directory.chmod(0o700)
    spike = Spike(directory)
    spike.compose[-1] = str(Path(__file__).with_name('compose.task265b-lifecycle.yml'))
    try:
        spike.prepare()
        run(['go', '-C', str(ROOT / 'src'), 'build', '-o', str(directory / 'controller'),
             './cmd/mosquitto-credential-controller'], env=dict(ENV, CGO_ENABLED='0'))
        run(['go', '-C', str(ROOT / 'src'), 'test', '-c', '-cover', '-o', str(directory / 'client-tests'),
             './internal/mqttcredential'], env=dict(ENV, CGO_ENABLED='0'))
        (directory / 'controller').chmod(0o755)
        (directory / 'client-tests').chmod(0o755)
        (directory / 'ca.crt').chmod(0o644)
        spike.setup("sed -i 's/listener 8883/listener 18884 127.0.0.1/' /fixture/broker.conf; "
                    'mkdir -p /control; chmod 700 /control; chown 1883:1883 /control')
        config = {'Lifecycle': {'Executable': '/usr/sbin/mosquitto', 'Args': ['-c', '/fixture/broker.conf'],
                  'PublicAddresses': ['0.0.0.0:8883', '0.0.0.0:8884'], 'BrokerAddress': '127.0.0.1:18884',
                  'MaxConnections': 8, 'DialTimeout': 1000000000, 'StopTimeout': 2000000000},
                  'Controller': {'ControlDir': '/control', 'UID': 1883, 'Timeout': 10000000000,
                  'MaxFrameBytes': 2048, 'MaxInflight': 8, 'ManagementAddress': '127.0.0.1:18884'}}
        spike.setup('cat > /fixture/startup.json; chmod 600 /fixture/startup.json; chown 1883:1883 /fixture/startup.json', json.dumps(config).encode())
        spike.command('up', '-d', 'broker')
        container = spike.command('ps', '-q', 'broker', capture=True).decode().strip()
        inspected = json.loads(run(['docker', 'inspect', container], capture=True))[0]
        mounts = {m['Destination']: m['Name'] for m in inspected['Mounts'] if m['Type'] == 'volume'}
        artifacts = directory / 'artifacts'
        artifacts.mkdir(mode=0o777, exist_ok=True)
        artifacts.chmod(0o777)
        payload = dict(Password=spike.passwords['admin'], NewPassword=secrets.token_urlsafe(32),
                       OldPassword=secrets.token_urlsafe(32))
        args = ['docker', 'run', '--rm', '-i', '--network', next(iter(inspected['NetworkSettings']['Networks'])),
                '--user', '1883:1883', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                '--mount', 'type=bind,src=' + str(directory / 'client-tests') + ',dst=/client-tests,readonly',
                '--mount', 'type=bind,src=' + str(directory / 'ca.crt') + ',dst=/ca.crt,readonly',
                '--mount', 'type=volume,src=' + mounts['/control'] + ',dst=/control',
                '--mount', 'type=volume,src=' + mounts['/security'] + ',dst=/security,readonly',
                '--mount', 'type=bind,src=' + str(artifacts) + ',dst=/artifacts',
                '-e', 'TASK265C_ISOLATED=1', '-e', 'GOCOVERDIR=/artifacts', '-e', 'TMPDIR=/artifacts', '--entrypoint', '/client-tests', IMAGE,
                '-test.run=^TestDynSecAdapterPinnedBroker$', '-test.v', '-test.coverprofile=/artifacts/adapter.cover']
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=ENV)
        p.stdin.write(json.dumps(payload).encode())
        p.stdin.close()
        deadline = time.monotonic() + 60
        for name, action in (
            ('revoke-snapshot', lambda: spike.setup('chmod 000 /security/dynsec.json')),
            ('revoke-restore', lambda: spike.setup('chmod 600 /security/dynsec.json')),
            ('fault', lambda: spike.setup('chmod 500 /security')),
            ('controller-disconnect', lambda: spike.command('stop', 'broker')),
        ):
            while time.monotonic() < deadline and not (artifacts / (name + '-ready')).exists() and p.poll() is None:
                time.sleep(.05)
            if (artifacts / (name + '-ready')).exists():
                action()
                (artifacts / (name + '-armed')).touch()
        output = p.stdout.read()
        p.wait(timeout=30)
        require(not any(v.encode() in output for v in [*spike.passwords.values(), *payload.values(), 'native-save-fault-fixture-secret', 'unrelated-fixture-oracle-secret']), 'secret output')
        print(output.decode(), end='', flush=True)
        require(p.returncode == 0 and all(marker in output for marker in (
            b'PASS actual native save fault', b'PASS actual revoke snapshot permission failure',
            b'PASS actual controller disconnect', b'PASS actual adapter role send/receive/subscribe',
        )), 'actual adapter test failed/skipped')
    finally:
        spike.setup(f'chown -R {os.getuid()}:{os.getgid()} /fixture; chmod 700 /fixture')
        spike.cleanup()
        for entry in directory.iterdir():
            if entry.name == 'artifacts':
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        print('owned redacted evidence=' + str(directory), flush=True)


if __name__ == '__main__':
    main()
