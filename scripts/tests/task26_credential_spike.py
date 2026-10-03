#!/usr/bin/env python3
"""Single public entrypoint for isolated credential evidence, NOT production G1."""
import argparse
import contextlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile

from stage2_mosquitto_runtime import IMAGE, run
from task26_dynsec_cases import Spike, unit_tests
from task26_revoke_cases import RevokeSpike
from task26_startup_cases import GateSpike
from task26_maintenance_cases import RotateSpike

SCENARIOS = {'dynsec': Spike, 'revoke': RevokeSpike,
             'startupgate': GateSpike, 'maintenancerotate': RotateSpike}


class Tee:
    def __init__(self, console, transcript):
        self.console, self.transcript = console, transcript

    def write(self, text):
        self.console.write(text)
        self.transcript.write(text)
        self.flush()
        return len(text)

    def flush(self):
        self.console.flush()
        self.transcript.flush()


def execute_scenario(name, output):
    artifacts = output / name
    artifacts.mkdir(mode=0o700)
    with tempfile.TemporaryDirectory(prefix=name + '-', dir=output) as directory:
        spike = SCENARIOS[name](directory)
        spike.artifacts = artifacts
        spike.transcript = artifacts / 'transcript.log'
        registry = artifacts / 'owner.json'
        registry.write_text(json.dumps({'project': spike.project, 'fixture': directory,
            'compose': spike.compose[-1], 'cleanup':
            'DYNSEC_FIXTURE=<fixture> COMPOSE_DISABLE_ENV_FILE=1 docker compose '
            '--env-file /dev/null -p <project> -f <compose> down -v --remove-orphans'}))
        with spike.transcript.open('x') as transcript:
            with contextlib.redirect_stdout(Tee(sys.stdout, transcript)), \
                    contextlib.redirect_stderr(Tee(sys.stderr, transcript)):
                try:
                    spike.prepare()
                    if name in ('dynsec', 'revoke'):
                        spike.start()
                    if os.environ.get('TASK26_FAIL_AFTER_PREPARE') == name:
                        raise RuntimeError('injected owned preparation failure')
                    spike.tests()
                    if name == 'dynsec':
                        spike.scan()
                    print('PASS scenario=' + name + '; scoped evidence only', flush=True)
                finally:
                    try:
                        spike.cleanup()
                    finally:
                        run(['docker', 'run', '--rm', '--network', 'none', '-v',
                             directory + ':/owned', '--entrypoint', '/bin/sh', IMAGE,
                             '-ec', 'chown -R ' + str(os.getuid()) + ':' +
                             str(os.getgid()) + ' /owned'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=['all', *SCENARIOS], default='all')
    parser.add_argument('--artifact-root', type=Path,
                        default=Path(tempfile.gettempdir()) / 'opencode')
    parser.add_argument('--unit', action='store_true')
    args = parser.parse_args()
    if args.unit:
        unit_tests()
        return
    os.umask(0o077)
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    args.artifact_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix='task26-run-', dir=args.artifact_root)).resolve()
    print('artifacts=' + str(output), flush=True)
    for name in SCENARIOS if args.scenario == 'all' else [args.scenario]:
        execute_scenario(name, output)
    print('Fixture scenarios PASS; full/original G1 BLOCKED; production NOT RUN', flush=True)


if __name__ == '__main__':
    main()
