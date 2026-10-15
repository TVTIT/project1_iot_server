#!/usr/bin/env python3
"""Real app-role PostgreSQL latest schema + pinned broker PID1/private IPC proof."""
import json
import hashlib
import os
from pathlib import Path
import secrets
import re
import shutil
import subprocess
import tempfile
import time

from stage2_mosquitto_runtime import ENV, ROOT, run
from task26_dynsec_cases import Spike
from task26_harness_helpers import require
from task265b_lifecycle import IMAGE

PG_IMAGE = 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a'
IMAGE_TIMEOUT = 300
CONTAINER_TIMEOUT = 60
DATABASE_TIMEOUT = 90
PROBE_TIMEOUT = 2


def prepare_database_image():
    """Inspect the exact digest; only an absent local image needs a pull."""
    try:
        inspected = subprocess.run(['docker', 'image', 'inspect', PG_IMAGE], env=ENV,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=10)
        if inspected.returncode == 0:
            return
        if b'no such image: ' + PG_IMAGE.encode() not in inspected.stderr.lower():
            raise RuntimeError('image_inspect_failed')
    except subprocess.TimeoutExpired:
        raise RuntimeError('image_inspect_timeout') from None
    except OSError:
        raise RuntimeError('image_inspect_failed') from None
    try:
        run(['docker', 'pull', PG_IMAGE], timeout=IMAGE_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise RuntimeError('image_pull_timeout') from None
    except (RuntimeError, OSError):
        raise RuntimeError('image_pull_failed') from None


def start_database(database, network, env_file):
    try:
        run(['docker', 'run', '--pull=never', '-d', '--name', database,
             '--network', network, '--env-file', str(env_file), PG_IMAGE],
            capture=True, timeout=CONTAINER_TIMEOUT)
    except subprocess.TimeoutExpired:
        # The daemon may already have created the named container. The caller's
        # finally always removes that owned name, even without a returned ID.
        raise RuntimeError('container_start_timeout') from None
    except (RuntimeError, OSError):
        raise RuntimeError('container_start_failed') from None


def wait_database(database, timeout=DATABASE_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError('database_not_ready')
        try:
            # TCP excludes the entrypoint's temporary Unix-only server.
            result = subprocess.run(
                ['docker', 'exec', database, 'pg_isready', '-h', '127.0.0.1',
                 '-U', 'fixture_admin', '-d', 'fixture_service'], env=ENV,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=min(PROBE_TIMEOUT, remaining))
            if result.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            pass
        except OSError:
            raise RuntimeError('database_probe_failed') from None
        remaining = deadline - time.monotonic()
        if remaining > 0:
            try:
                # Only status, never Env, logs, or initialization SQL.
                state = subprocess.run(
                    ['docker', 'inspect', '-f', '{{.State.Status}}', database],
                    env=ENV, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    timeout=min(PROBE_TIMEOUT, remaining))
                if state.returncode == 0 and state.stdout.strip() in (b'exited', b'dead', b'removing'):
                    raise RuntimeError('database_not_ready:' + state.stdout.strip().decode('ascii'))
            except subprocess.TimeoutExpired:
                pass
            except OSError:
                raise RuntimeError('database_probe_failed') from None
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(1, remaining))


def cleanup_container(owned):
    """Absence is success only with Docker's specific not-found evidence."""
    try:
        removed = subprocess.run(['docker', 'rm', '-f', '-v', owned], env=ENV,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
        check = subprocess.run(['docker', 'inspect', owned], env=ENV,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=10)
        absent = check.returncode != 0 and (
            b'no such container: ' + owned.encode() in check.stderr.lower() or
            b'no such object: ' + owned.encode() in check.stderr.lower())
        require(absent, 'owned container cleanup failed')
        require(removed.returncode == 0 or
                b'no such container: ' + owned.encode() in (removed.stderr or b'').lower(),
                'owned container cleanup failed')
        # rm may report not-found for an uncreated/already removed container;
        # a successful absence check is the authoritative cleanup postcondition.
    except (subprocess.TimeoutExpired, OSError):
        raise RuntimeError('owned container cleanup failed') from None


def main(rotate=False, revoke=False, startup=False, composition=False, http=False, standalone=False):
    task = 'task2612' if http or standalone else ('task2611' if composition else ('task269' if startup else ('task268' if revoke else ('task267' if rotate else 'task266'))))
    directory = Path(tempfile.mkdtemp(prefix=task + '-', dir='/tmp/opencode'))
    directory.chmod(0o700)
    spike = Spike(directory)
    spike.compose[-1] = str(Path(__file__).with_name('compose.task265b-lifecycle.yml'))
    database = 'task266-pg-' + secrets.token_hex(8)
    test_container = 'task266-service-' + secrets.token_hex(8)
    proxy = 'task2612-nginx-' + secrets.token_hex(8)
    try:
        spike.prepare()
        run(['go', '-C', str(ROOT / 'src'), 'build', '-o', str(directory / 'controller'),
             './cmd/mosquitto-credential-controller'], env=dict(ENV, CGO_ENABLED='0'))
        run(['go', '-C', str(ROOT / 'src'), 'test', '-c', '-cover', '-coverpkg=./cmd/server,./internal/mqttcredential,./internal/httpserver,./internal/auth,./internal/gateway', '-o', str(directory / 'client-tests'),
             './cmd/server' if composition or http else './internal/mqttcredential'], env=dict(ENV, CGO_ENABLED='0'))
        for name in ('controller', 'client-tests'):
            (directory / name).chmod(0o755)
        (directory / 'ca.crt').chmod(0o644)
        spike.setup("sed -i 's/listener 8883/listener 18884 127.0.0.1/' /fixture/broker.conf; "
                    'mkdir -p /control; chmod 700 /control; chown 1883:1883 /control')
        config = {'Lifecycle': {'Executable': '/usr/sbin/mosquitto', 'Args': ['-c', '/fixture/broker.conf'],
                  'PublicAddresses': ['0.0.0.0:8883', '0.0.0.0:8884'], 'BrokerAddress': '127.0.0.1:18884',
                  'MaxConnections': 8, 'DialTimeout': 1000000000, 'StopTimeout': 2000000000},
                  'Controller': {'ControlDir': '/control', 'UID': 1883, 'Timeout': 10000000000,
                  'MaxFrameBytes': 2048, 'MaxInflight': 8, 'ManagementAddress': '127.0.0.1:18884'}}
        spike.setup('cat > /fixture/startup.json; chmod 600 /fixture/startup.json; chown 1883:1883 /fixture/startup.json', json.dumps(config).encode())
        if composition or http or standalone:
            # Offline native test-only bootstrap; never write plaintext/hash to DB.
            password = spike.passwords['backend']
            spike.setup('umask 077; mosquitto_ctrl dynsec init /security/backend.json backend_service >/dev/null',
                        (password + '\n' + password + '\n').encode())
            original = json.loads(spike.setup('cat /security/dynsec.json'))
            backend = json.loads(spike.setup('cat /security/backend.json'))
            original['clients'].extend(backend['clients'])
            spike.setup('cat > /security/dynsec.json; chmod 600 /security/dynsec.json; chown 1883:1883 /security/dynsec.json; rm /security/backend.json',
                        json.dumps(original).encode())
        spike.command('up', '-d', 'broker')
        container = spike.command('ps', '-q', 'broker', capture=True).decode().strip()
        inspected = json.loads(run(['docker', 'inspect', container], capture=True))[0]
        mounts = {m['Destination']: m['Name'] for m in inspected['Mounts'] if m['Type'] == 'volume'}
        network = next(iter(inspected['NetworkSettings']['Networks']))
        if http or standalone:
            from stage2_mqtt_credentials import start_proxy, qualify_network
            qualify_network(spike, inspected)
            start_proxy(directory, network, proxy, test_container, publish=standalone)
        admin_password, app_password = secrets.token_hex(32), secrets.token_hex(32)
        env_file = directory / 'database.env'
        env_file.write_text('POSTGRES_USER=fixture_admin\nPOSTGRES_DB=fixture_service\nPOSTGRES_PASSWORD=' + admin_password + '\nBACKEND_DB_PASSWORD=' + app_password + '\nAUTH_DB_PASSWORD=' + secrets.token_hex(32) + '\nSTORAGE_DB_PASSWORD=' + secrets.token_hex(32) + '\n')
        env_file.chmod(0o600)
        prepare_database_image()
        start_database(database, network, env_file)
        wait_database(database)
        # Shell initialization uses container env, never secret command arguments.
        for migration in sorted((ROOT / 'migrations').iterdir()):
            if not migration.name.startswith('0000') or (not migration.name.endswith('.sql') and not migration.name.endswith('.sh')):
                continue
            if migration.suffix == '.sh':
                args = ['docker', 'exec', '-i', database, 'sh', '-s']
            else:
                args = ['docker', 'exec', '-i', database, 'psql', '-U', 'fixture_admin', '-d', 'fixture_service', '-v', 'ON_ERROR_STOP=1']
            try:
                if migration.suffix == '.sh':
                    script = directory / 'roles.sh'
                    script.write_bytes(migration.read_bytes())
                    run(['docker', 'cp', str(script), database + ':/roles.sh'])
                    run(['docker', 'exec', database, 'sh', '/roles.sh'], capture=True)
                else:
                    result = subprocess.run(args, input=migration.read_bytes(), env=ENV,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
                    if result.returncode:
                        error = result.stderr.decode()
                        require(not any(v in error for v in (admin_password, app_password)), 'migration secret scan')
                        print(error, flush=True)
                        raise RuntimeError('SQL migration failed')
            except RuntimeError:
                raise RuntimeError('isolated migration failed: ' + migration.name) from None
        for version in (13, 14, 15, 16):
            verifier = ROOT / 'scripts' / 'sql' / f'verify-migration-{version:06d}.sql'
            result = subprocess.run(['docker', 'exec', '-i', database, 'psql', '-U', 'fixture_admin',
                                     '-d', 'fixture_service', '-v', 'ON_ERROR_STOP=1'],
                                    input=verifier.read_bytes(), env=ENV, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=30)
            require(result.returncode == 0, f'version {version} schema verifier')
        artifacts = directory / 'artifacts'
        artifacts.mkdir(mode=0o777, exist_ok=True)
        artifacts.chmod(0o777)
        payload = dict(Password=spike.passwords['admin'],
                       Broker=inspected['Name'].lstrip('/'),
                       AppDSN=f'postgres://iot_backend_app:{app_password}@{database}:5432/fixture_service?sslmode=disable',
                       AdminDSN=f'postgres://fixture_admin:{admin_password}@{database}:5432/fixture_service?sslmode=disable')
        if standalone:
            from task2612_standalone import acceptance
            acceptance(directory, spike, database, test_container, proxy, network, mounts, payload)
            return
        if composition:
            payload = dict(Password=spike.passwords['admin'], BackendPassword=spike.passwords['backend'], AppDSN=payload['AppDSN'])
        if http:
            payload.update(BackendPassword=spike.passwords['backend'], Proxy=proxy,
                           Broker=inspected['Name'].lstrip('/'), JWTSecret=secrets.token_urlsafe(48))
        args = ['docker', 'run', '--name', test_container, '--rm', '-i', '--network', network, '--user', '1883:1883', '--read-only',
                '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
                '--mount', f'type=bind,src={directory}/client-tests,dst=/client-tests,readonly',
                '--mount', f'type=bind,src={directory}/ca.crt,dst=/ca.crt,readonly',
                '--mount', 'type=volume,src=' + mounts['/control'] + ',dst=/control',
                '--mount', 'type=volume,src=' + mounts['/security'] + ',dst=/security,readonly',
                '--mount', f'type=bind,src={artifacts},dst=/artifacts',
                '-e', 'TASK2612_ISOLATED=1' if http else ('TASK2611_ISOLATED=1' if composition else 'TASK266_ISOLATED=1'), '-e', 'TMPDIR=/artifacts', '--entrypoint', '/client-tests', IMAGE,
                '-test.run=^TestCredentialHTTPIsolated$' if http else ('-test.run=^TestCredentialCompositionIsolated$' if composition else '-test.run=^Test' + ('Startup' if startup else ('Revoke' if revoke else ('Rotate' if rotate else 'Provision'))) + 'ServicePinnedBrokerPostgres$'), '-test.v', '-test.coverprofile=/artifacts/service.cover']
        p = subprocess.Popen(args, env=ENV, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.stdin.write(json.dumps(payload).encode())
        p.stdin.close()
        deadline = time.monotonic() + 180
        actions = (
            ('save-fault', lambda: spike.setup('chmod 500 /security')),
            ('save-restore', lambda: spike.setup('chmod 700 /security')),
            ('controller-disconnect', lambda: spike.command('stop', 'broker')),
        )
        def kill_restart():
            before = json.loads(run(['docker', 'inspect', container], capture=True))[0]['State']['StartedAt']
            spike.command('kill', '-s', 'SIGKILL', 'broker')
            state = json.loads(run(['docker', 'inspect', container], capture=True))[0]
            require(not state['State']['Running'] and state['RestartCount'] == 0, 'SIGKILL no autonomous respawn')
            spike.command('up', '-d', 'broker')
            state = json.loads(run(['docker', 'inspect', container], capture=True))[0]
            require(state['State']['Running'] and state['State']['StartedAt'] != before, 'new PID1 required')
            spike.command('exec', '-T', 'broker', '/bin/sh', '-ec',
                          'i=0; until test -S /control/control.sock; do '
                          'i=$((i+1)); test "$i" -lt 100; sleep 0.05; done')
            # Go independently verifies the new epoch, CLOSED and stale receipt.
        revoke_actions = (('revoke-kill-open', kill_restart),
                          ('revoke-kill-execute', kill_restart), *actions[:2])
        startup_actions = (
            ('startup-kill-pending', kill_restart),
            # disableClient saves by rename: file-only chmod is replaced by its
            # save and does not hold a native unreadable-snapshot fault.
            ('startup-unreadable', lambda: spike.setup('chmod 000 /security/dynsec.json; chmod 500 /security')),
            ('startup-readable', lambda: spike.setup('chmod 700 /security; chmod 600 /security/dynsec.json')),
            ('startup-save-fault', lambda: spike.setup('chmod 500 /security')),
            ('startup-save-restore', lambda: spike.setup('chmod 700 /security')),
        )
        for name, action in (() if composition or http else (startup_actions if startup else (revoke_actions if revoke else actions))):
            while time.monotonic() < deadline and not (artifacts / (name + '-ready')).exists() and p.poll() is None:
                time.sleep(.05)
            if (artifacts / (name + '-ready')).exists():
                action()
                (artifacts / (name + '-armed')).touch()
        p.wait(timeout=60)
        output = p.stdout.read()
        require(not any(v.encode() in output for v in [*spike.passwords.values(), admin_password, app_password]), 'secret output scan')
        print(output.decode(), end='', flush=True)
        if http:
            from stage2_mqtt_credentials import verify_selector
            fingerprints_file = artifacts / 'secret-fingerprints.json'
            fingerprints = json.loads(fingerprints_file.read_text()) if fingerprints_file.exists() else []
            for owned in (proxy, container):
                logs = run(['docker', 'logs', owned], capture=True)
                require(not any(v.encode() in logs for v in [*spike.passwords.values(), admin_password, app_password, payload['JWTSecret']]), 'fixture log secrecy')
                candidates = re.findall(rb'[A-Za-z0-9_-]{43}', logs + output)
                require(not any(hashlib.sha256(v).hexdigest() in fingerprints for v in candidates), 'generated Gateway secret log leak')
                if owned == proxy and p.returncode:
                    print(logs.decode(), flush=True)
                    print(run(['docker', 'inspect', '-f', '{{json .State}}', proxy], capture=True).decode(), flush=True)
            verify_selector(output)
            require(bool(fingerprints), 'generated secret secrecy report executed')
        marker = b'PASS real HTTP PostgreSQL broker' if http else (b'PASS isolated composition' if composition else (b'PASS real v14 startup' if startup else b'PASS real v13 app-role'))
        selector = b'--- PASS: TestCredentialHTTPIsolated (' if http else (b'--- PASS: TestCredentialCompositionIsolated (' if composition else b'--- PASS: Test' + ('Startup' if startup else ('Revoke' if revoke else ('Rotate' if rotate else 'Provision'))).encode() + b'ServicePinnedBrokerPostgres (')
        require(p.returncode == 0 and b'--- SKIP:' not in output and marker in output and selector in output, 'real service test failed/skipped/zero selector')
        coverage_out = os.environ.get(task.upper() + '_COVERAGE_OUT')
        if coverage_out:
            shutil.copyfile(artifacts / 'service.cover', coverage_out)
    finally:
        cleanup_failed = False
        for owned in (test_container + '-readiness', test_container, database, proxy):
            try:
                cleanup_container(owned)
            except RuntimeError:
                cleanup_failed = True
        spike.setup(f'chown -R {os.getuid()}:{os.getgid()} /fixture; chmod 700 /fixture')
        spike.cleanup()
        shutil.rmtree(directory)
        require(not cleanup_failed, 'owned container cleanup failed')


if __name__ == '__main__':
    main()
