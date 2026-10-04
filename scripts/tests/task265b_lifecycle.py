#!/usr/bin/env python3
"""New lifecycle PID1 executable + private backend namespace, pinned broker.

Never accesses deployment .env. Owned unique Compose project and paths only.
Secrets sent on stdin, no CA private key or auth-store mount in backend runner.
"""
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time

from stage2_mosquitto_runtime import ENV, ROOT, run
from task26_dynsec_cases import Spike
from task26_harness_helpers import require

IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'

def main():
    directory = Path(tempfile.mkdtemp(prefix='task265b-', dir='/tmp/opencode'))
    directory.chmod(0o700)
    spike = Spike(directory)
    spike.compose[-1] = str(Path(__file__).with_name('compose.task265b-lifecycle.yml'))
    (directory / 'owner.json').write_text(json.dumps({'project': spike.project, 'fixture': str(directory), 'scope': 'task265b only'}) + '\n')
    binary = directory / 'client-tests'
    try:
        spike.prepare()
        run(['go', '-C', str(ROOT / 'src'), 'build', '-o', str(directory / 'controller'),
             './cmd/mosquitto-credential-controller'], env=dict(ENV, CGO_ENABLED='0'))
        run(['go', '-C', str(ROOT / 'src'), 'test', '-c', '-o', str(binary),
             './internal/mqttcredential'], env=dict(ENV, CGO_ENABLED='0'))
        (directory / 'controller').chmod(0o755)
        binary.chmod(0o755)
        # Numeric loopback management; two fixture paths simulate direct/tunnel.
        spike.setup("sed -i 's/listener 8883/listener 18884 127.0.0.1/' /fixture/broker.conf; "
                    'mkdir -p /control; chmod 700 /control; chown 1883:1883 /control')
        config = {'Lifecycle': {'Executable': '/usr/sbin/mosquitto', 'Args': ['-c', '/fixture/broker.conf'],
                    'PublicAddresses': ['0.0.0.0:8883', '0.0.0.0:8884'], 'BrokerAddress': '127.0.0.1:18884',
                    'MaxConnections': 8, 'DialTimeout': 1000000000, 'StopTimeout': 2000000000},
                  'Controller': {'ControlDir': '/control', 'UID': 1883, 'Timeout': 10000000000,
                    'MaxFrameBytes': 2048, 'MaxInflight': 8, 'ManagementAddress': '127.0.0.1:18884'}}
        # Broker dir already owned by fixture UID; write through isolated setup.
        spike.setup('cat > /fixture/startup.json; chmod 600 /fixture/startup.json; chown 1883:1883 /fixture/startup.json', json.dumps(config).encode())
        spike.command('up', '-d', 'broker')
        container = spike.command('ps', '-q', 'broker', capture=True).decode().strip()
        inspected = json.loads(run(['docker', 'inspect', container], capture=True))[0]
        control = next(m['Name'] for m in inspected['Mounts'] if m['Destination'] == '/control')
        network = next(iter(inspected['NetworkSettings']['Networks']))
        old_epoch = ''
        # Own backend network namespace, not container:<broker> loopback sharing.
        def test(mode, uid='1883:1883', mount=True):
            args = ['docker', 'run', '--rm', '-i', '--network', network, '--user', uid,
                    '--mount', 'type=bind,src=' + str(binary) + ',dst=/client-tests,readonly',
                    '--mount', 'type=bind,src=' + str(directory / 'ca.crt') + ',dst=/ca.crt,readonly',
                    '--mount', 'type=bind,src=' + str(directory / 'wrong-ca.crt') + ',dst=/wrong-ca.crt,readonly']
            if mount:
                args += ['--mount', 'type=volume,src=' + control + ',dst=/control']
            args += ['--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                     '-e', 'TASK265B_ISOLATED=1', '--entrypoint', '/client-tests', IMAGE,
                     '-test.run=^TestControllerPinnedBroker$', '-test.v']
            if uid == '0:0':
                # Bypass directory DAC only to exercise actual SO_PEERCRED
                # rejection, rather than relying solely on mount permissions.
                args[args.index('--entrypoint'):args.index('--entrypoint')] = ['--cap-add=DAC_OVERRIDE']
            payload = dict(ControlDir='/control', CAPath='/ca.crt', WrongCAPath='/wrong-ca.crt',
                           Password=spike.passwords['admin'], NewPassword=secrets.token_urlsafe(32),
                           Mode=mode, OldEpoch=old_epoch, Public=['broker:8883', 'broker:8884'])
            completed = subprocess.run(args, input=json.dumps(payload).encode(), stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, env=ENV, timeout=60)
            require(not any(p.encode() in completed.stdout for p in [*spike.passwords.values(), payload['NewPassword']]), 'secret output')
            print(completed.stdout.decode(), end='', flush=True)
            require(completed.returncode == 0, 'new lifecycle integration failed')
            match = re.search(r'fixture epoch=([a-f0-9]{64})', completed.stdout.decode())
            return match.group(1) if match else ''
        # Public certificates must be readable by the fixture backend UID.
        (directory / 'ca.crt').chmod(0o644)
        (directory / 'wrong-ca.crt').chmod(0o644)
        test('unauthorized', uid='1884:1884')
        test('unauthorized', uid='0:0')
        test('unauthorized', mount=False)
        old_epoch = test('normal')
        require(bool(old_epoch), 'missing fixture epoch')
        proc = spike.command('exec', '-T', 'broker', '/bin/sh', '-ec',
                             'cat /proc/1/task/*/children', capture=True).decode().strip().split()
        require(len(proc) == 1, 'wrapper must own exactly one child')
        keys = spike.command('exec', '-T', 'broker', '/bin/sh', '-ec',
                'tr "\\000" "\\n" < /proc/' + proc[0] + '/environ | cut -d= -f1', capture=True).decode().split()
        require('PGPASSWORD' not in keys and 'JWT_SECRET' not in keys, 'child inherited backend secrets')
        spike.command('exec', '-T', 'broker', '/bin/sh', '-ec', 'kill -KILL ' + proc[0])
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = json.loads(run(['docker', 'inspect', container], capture=True))[0]['State']
            if not state['Running']:
                break
            time.sleep(.1)
        require(not state['Running'] and state['ExitCode'] != 0, 'child death did not fail wrapper closed')
        require(json.loads(run(['docker', 'inspect', container], capture=True))[0]['RestartCount'] == 0, 'autonomous respawn')
        spike.command('up', '-d', 'broker')
        test('fresh')
        spike.command('stop', 'broker')
        spike.setup("printf 'invalid_configuration_directive\\n' > /fixture/broker.conf")
        spike.command('up', '-d', 'broker')
        failed_container = spike.command('ps', '-a', '-q', 'broker', capture=True).decode().strip()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = json.loads(run(['docker', 'inspect', failed_container], capture=True))[0]['State']
            if not state['Running']:
                break
            time.sleep(.1)
        require(not state['Running'] and state['ExitCode'] != 0, 'bad broker config did not fail closed')
        print('PASS new PID1 wrapper, SIGKILL fail-closed/no-respawn, restart CLOSED, private UID/mount boundary, environment sanitation', flush=True)
    finally:
        spike.setup(f'chown -R {os.getuid()}:{os.getgid()} /fixture; chmod 700 /fixture')
        spike.cleanup()
        for entry in directory.iterdir():
            if entry.name in ('artifacts', 'owner.json'):
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        print('owned evidence=' + str(directory), flush=True)

if __name__ == '__main__':
    main()
