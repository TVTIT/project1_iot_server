#!/usr/bin/env python3
"""Isolated new Go client/native reader verification; no .env or deployment.

Reuse the consolidated pinned broker, old independent oracles and owned cleanup.
New test executable is freshly compiled and run inside the broker network
namespace with the native store mounted read-only. Only stdin carries secrets.
"""
import json
import os
from pathlib import Path
import secrets
import tempfile
import subprocess
import shutil

from stage2_mosquitto_runtime import ENV, ROOT, run
from task26_dynsec_cases import Spike
from task26_harness_helpers import require

IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'

def main():
    directory = Path(tempfile.mkdtemp(prefix='task265a-', dir='/tmp/opencode'))
    directory.chmod(0o700)
    spike = Spike(directory)
    (directory / 'owner.json').write_text(json.dumps({
        'project': spike.project, 'fixture': str(directory),
        'compose': str(Path(__file__).with_name('compose.task260-dynsec.yml')),
        'scope': 'task265a fixture only',
    }) + '\n')
    (directory / 'owner.json').chmod(0o600)
    spike.transcript = directory / 'artifacts' / 'transcript.log'
    binary = directory / 'client-tests'
    coverage = directory / 'coverage'
    coverage.mkdir(mode=0o700)
    try:
        spike.prepare()
        run(['go', '-C', str(ROOT / 'src'), 'test', '-c', '-cover', '-o', str(binary),
             './internal/mqttcredential'], env=dict(ENV, CGO_ENABLED='0'), timeout=120)
        binary.chmod(0o755)
        spike.start()
        spike.tests()  # Retain old ACL/TLS/rotate/revoke/permission-fault oracles.
        container = spike.command('ps', '-q', 'broker', capture=True).decode().strip()
        inspected = json.loads(run(['docker', 'inspect', container], capture=True))[0]
        volume = next(m['Name'] for m in inspected['Mounts'] if m['Destination'] == '/security')
        def go_test(mode):
            payload = dict(URL='ssl://localhost:8883', CAPath='/fixture/ca.crt',
                           WrongCAPath='/wrong-ca.crt',
                           SnapshotPath='/security/dynsec.json',
                           ManagerPassword=spike.passwords['manager'],
                           OldPassword=spike.passwords['new'], BPassword=spike.passwords['B'],
                           NewPassword=secrets.token_urlsafe(32), Mode=mode)
            completed = subprocess.run(['docker', 'run', '--rm', '-i', '--network', 'container:' + container,
                         '--user', '0:0',
                         '--mount', 'type=volume,src=' + volume + ',dst=/security,readonly',
                         '--mount', 'type=bind,src=' + str(directory / 'broker') + ',dst=/fixture,readonly',
                         '--mount', 'type=bind,src=' + str(binary) + ',dst=/client-tests,readonly',
                         '--mount', 'type=bind,src=' + str(directory / 'wrong-ca.crt') + ',dst=/wrong-ca.crt,readonly',
                         '--mount', 'type=bind,src=' + str(coverage) + ',dst=/coverage',
                         '--read-only', '--cap-drop=ALL', '--cap-add=DAC_OVERRIDE',
                         '--security-opt=no-new-privileges:true', '-e', 'TASK265A_ISOLATED=1',
                         '--entrypoint', '/client-tests', IMAGE,
                         '-test.run=^TestDynSecPinnedBroker$', '-test.v', '-test.gocoverdir=/coverage'],
                         input=json.dumps(payload).encode(), stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, env=ENV, timeout=60)
            result = completed.stdout
            require(not any(p.encode() in result for p in
                            [*spike.passwords.values(), payload['NewPassword']]), 'secret in test output')
            print(result.decode(), end='', flush=True)
            require(completed.returncode == 0, 'Go client fixture failed')
        go_test('normal')
        spike.setup('chmod 500 /security')
        go_test('fault')
        spike.probe('B', accepted=False)
        spike.restart()
        spike.probe('B')
        print('PASS new client RAM success + old native snapshot + ungated restart accepts old B', flush=True)
        spike.setup('chmod 700 /security')
        spike.scan()
    finally:
        spike.setup(f'chown -R {os.getuid()}:{os.getgid()} /fixture; chmod 700 /fixture')
        # Coverage contains no credentials; restore host ownership explicitly.
        run(['docker', 'run', '--rm', '--network', 'none', '--user', '0:0',
             '--mount', 'type=bind,src=' + str(coverage) + ',dst=/coverage',
             '--entrypoint', '/bin/sh', IMAGE, '-ec',
             f'chown -R {os.getuid()}:{os.getgid()} /coverage'], env=ENV)
        spike.cleanup()
        # Remove only ephemeral material this run owns; preserve safe coverage
        # and redacted artifacts for review. No .env, global prune or secrets.
        for entry in directory.iterdir():
            if entry.name in ('coverage', 'artifacts', 'owner.json'):
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        print('owned evidence=' + str(directory), flush=True)

if __name__ == '__main__':
    main()
