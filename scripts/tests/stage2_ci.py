#!/usr/bin/env python3
"""Bounded CI supervisor; only newly derived allowlisted reports are artifacts."""
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
LABEL = 'io.iot.stage2-ci-run'
SELECTORS = ['accounts/auth', 'provisioning', 'memberships/roles', 'credentials/ACL',
             'rotate/revoke/replay', 'loss/recovery', 'restart/fences', 'upgrade/repeatability']
SCENARIOS = {f'E{i:02}': 'PASS' for i in range(1, 28)}
BROKER = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
IMAGES = [BROKER, 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a',
          'supabase/gotrue:v2.196.0', 'envoyproxy/envoy:v1.39.1', 'nginx:1.25-alpine']


def observed(text):
    # Capture unknown statuses/identifiers too, so they cannot disappear from gates.
    scenarios = re.findall(r'SCENARIO (\S+): ([^\n]+)', text)
    allowed = r'(PASS|FAIL|BLOCKED|NOT RUN|INCOMPLETE|PARTIAL \(NOT RUN\))(?:\s|$)'
    scenarios = [(key, match.group(1) if (match := re.match(allowed, value)) else 'UNKNOWN')
                 for key, value in scenarios]
    selectors = re.findall(r"^Selector '([^']+)': ([^\n]+)$", text, re.MULTILINE)
    return scenarios, selectors


def complete(text, code):
    scenarios, selectors = observed(text)
    return (code == 0 and len(scenarios) == 27 and dict(scenarios) == SCENARIOS and
            len(selectors) == 8 and dict(selectors) == dict.fromkeys(SELECTORS, 'PASS') and
            text.count('STAGE 2 E2E EXECUTION SUMMARY') == 1 and
            'Post-cleanup known-secret scan passed' in text and
            'Traceback (most recent call last)' not in text)


def evidence(text, code):
    scenarios, selectors = observed(text)
    statuses = {'PASS', 'FAIL', 'BLOCKED', 'NOT RUN', 'INCOMPLETE', 'PARTIAL (NOT RUN)'}
    return {'harness_exit': code, 'complete': complete(text, code),
            'scenarios': {key: status for key, status in scenarios if key in SCENARIOS and status in statuses},
            'expected_selectors': SELECTORS,
            'observed_selectors': {key: status for key, status in selectors
                                   if key in SELECTORS and status in statuses}}


def canonical(path):
    path = Path(path)
    if not path.is_absolute() or path != path.resolve():
        raise RuntimeError('invalid canonical path')
    for item in (path, *path.parents):
        if item.is_symlink():
            raise RuntimeError('symlink boundary')
    return path


def validate_owner(manifest):
    """Validate the entire ownership contract before even querying Docker."""
    manifest = canonical(manifest)
    base = manifest.parent
    identity = canonical(base / 'identity')
    run_id = identity.read_text().strip()
    if not re.fullmatch('[a-f0-9]{32}', run_id):
        raise RuntimeError('invalid identity')
    owner = json.loads(manifest.read_text())
    if set(owner) != {'run_id', 'directory', 'prefix', 'project'}:
        raise RuntimeError('invalid manifest schema')
    if owner['run_id'] != run_id or owner['prefix'] != 'e2e_' + run_id[:8]:
        raise RuntimeError('foreign ownership')
    if canonical(owner['directory']) != base / 'fixture':
        raise RuntimeError('foreign fixture')
    project = owner['project']
    if project is not None and not re.fullmatch(r'dynsec_spike_[a-f0-9]{12}', project):
        raise RuntimeError('invalid project')
    return owner


def docker(args):
    return subprocess.run(['docker', *args], check=True, capture_output=True,
                          text=True, timeout=30).stdout


def owned_resources(owner):
    """Inspect all candidates before deletion; labels AND exact names must match."""
    resources = []
    direct = {owner['prefix'] + '_' + suffix for suffix in
              ('pg', 'gotrue', 'envoy', 'nginx', 'backend', 'loss_https', 'net')}
    project = owner['project']
    for kind, listing in [('container', ['ps', '-aq', '--no-trunc']),
                          ('volume', ['volume', 'ls', '-q']), ('network', ['network', 'ls', '-q', '--no-trunc'])]:
        candidates = docker([*listing, '--filter', 'label=' + LABEL + '=' + owner['run_id']]).split()
        for candidate in candidates:
            data = json.loads(docker([kind, 'inspect', candidate]))
            if len(data) != 1:
                raise RuntimeError('invalid inspection')
            item = data[0]
            name = item['Name'].removeprefix('/')
            labels = item.get('Config', {}).get('Labels') if kind == 'container' else item.get('Labels')
            labels = labels or {}
            if labels.get(LABEL) != owner['run_id']:
                raise RuntimeError('foreign run label')
            composed = labels.get('com.docker.compose.project')
            if composed:
                if composed != project or not project:
                    raise RuntimeError('foreign project label')
                pattern = (re.escape(project) + r'-(?:broker-1|setup-run-[a-f0-9]+)' if kind == 'container'
                           else re.escape(project) + (r'_(?:security|control)' if kind == 'volume' else r'_default'))
                if not re.fullmatch(pattern, name):
                    raise RuntimeError('foreign compose name')
            elif (name not in direct and not (kind == 'container' and
                  re.fullmatch(re.escape(owner['prefix']) + r'_script_[a-f0-9]{8}', name))) or kind == 'volume':
                raise RuntimeError('foreign exact name')
            identifier = item.get('Id', item.get('ID')) if kind != 'volume' else name
            if kind != 'volume' and not re.fullmatch('[a-f0-9]{64}', identifier or ''):
                raise RuntimeError('invalid inspected ID')
            resources.append((kind, identifier))
    return resources


def fallback(manifest):
    owner = validate_owner(manifest)
    resources = owned_resources(owner)  # no mutations until every inspection passes
    for kind, identifier in resources:
        docker(['rm', '-f', '-v', identifier] if kind == 'container' else [kind, 'rm', identifier])
    if owned_resources(owner):
        raise RuntimeError('owned resources remain')
    directory = canonical(owner['directory'])
    shutil.rmtree(directory, ignore_errors=True)
    if directory.exists():
        # No glob crossing mounts, no symlink traversal, only this canonical run subtree.
        docker(['run', '--rm', '--network', 'none', '--cap-drop=ALL', '--cap-add=DAC_OVERRIDE',
                '--security-opt=no-new-privileges:true', '--read-only', '--mount',
                f'type=bind,src={directory},dst=/owned', '--entrypoint', '/bin/sh', BROKER,
                '-ec', 'rm -rf /owned/* /owned/.[!.]* /owned/..?*'])
        directory.rmdir()


def metadata():
    def git(*args):
        try:
            return subprocess.run(['git', *args], cwd=ROOT, check=True, capture_output=True,
                                  text=True, timeout=5).stdout.strip()
        except Exception:
            return None
    head = git('rev-parse', 'HEAD')
    dirty = git('status', '--porcelain', '--untracked-files=all', '--', '.github/workflows/ci.yml',
                'scripts', 'src', 'migrations', 'config')
    event = os.environ.get('GITHUB_SHA', '')
    result = {'head': head if re.fullmatch('[a-f0-9]{40}', head or '') else 'unavailable',
              'relevant_worktree_dirty': dirty != '' if dirty is not None else 'unavailable',
              'execution_context': 'github_actions' if os.environ.get('GITHUB_ACTIONS') == 'true' else 'local',
              'event_sha': event if re.fullmatch('[a-f0-9]{40}', event) else 'unavailable', 'images': {}}
    for image in IMAGES:
        try:
            value = docker(['image', 'inspect', '-f', '{{.Id}}', image]).strip()
            result['images'][image] = value if re.fullmatch('sha256:[a-f0-9]{64}', value) else 'unavailable'
        except subprocess.TimeoutExpired:
            result['images'][image] = 'inspection_timeout'
        except Exception:
            result['images'][image] = 'unavailable'
    return result


def publish(base, report, password):
    safe = canonical(base / 'safe')
    safe.mkdir(mode=0o700, exist_ok=True)
    safe.chmod(0o700)
    serialized = json.dumps(report, indent=2) + '\n'
    if password in serialized or re.search(r'Bearer|PRIVATE KEY|eyJ', serialized):
        raise RuntimeError('artifact rejected')
    temporary = safe / 'report.tmp'
    with temporary.open('x') as handle:
        temporary.chmod(0o600)
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, safe / 'report.json')
    print(serialized, end='')


def supervise(base, command=None, timeout=1500, grace=60):
    base = canonical(base)
    base.mkdir(mode=0o700, parents=True, exist_ok=False)
    base.chmod(0o700)
    run_id = secrets.token_hex(16)
    (base / 'identity').write_text(run_id)
    (base / 'identity').chmod(0o600)
    password = secrets.token_urlsafe(36) + 'Aa1!'
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        print('::add-mask::' + password, flush=True)
    env = dict(os.environ, STAGE2_TEST_PASSWORD=password, STAGE2_CI_RUN_ID=run_id,
               STAGE2_CI_FIXTURE=str(base / 'fixture'), STAGE2_E2E_LOG=str(base / 'private.log'),
               STAGE2_CI_OWNER=str(base / 'owner.json'), STAGE2_SKIP_UNIT_GUARDS='0')
    # Prelaunch manifest makes even early import/exec failures safely cleanable.
    (base / 'owner.json').write_text(json.dumps({'run_id': run_id, 'directory': str(base / 'fixture'),
                                               'prefix': 'e2e_' + run_id[:8], 'project': None}))
    (base / 'owner.json').chmod(0o600)
    child = None
    code = 1
    cleanup_ok = False
    started = time.monotonic()
    previous = {}
    def interrupted(sig, frame):
        raise InterruptedError(sig)
    def terminate():
        if child is None:
            return
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        # Reap/kill any surviving descendants even when the leader exited first.
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=grace)
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, interrupted)
        with (base / 'private-console.log').open('x') as capture:
            (base / 'private-console.log').chmod(0o600)
            child = subprocess.Popen(command or ['sh', 'scripts/test-stage2-e2e.sh'], cwd=ROOT,
                                     env=env, stdout=capture, stderr=subprocess.STDOUT,
                                     start_new_session=True, umask=0o022)
            try:
                code = child.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, InterruptedError) as error:
                for sig in previous:
                    signal.signal(sig, signal.SIG_IGN)
                terminate()
                code = 124 if isinstance(error, subprocess.TimeoutExpired) else 128 + error.args[0]
    except Exception:
        code = 1
        terminate()
    finally:
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        try:
            fallback(base / 'owner.json')
            cleanup_ok = True
        except Exception:
            pass  # No exception strings or arbitrary diagnostics cross artifact boundary.
        text = ''
        try:
            text = (base / 'private-console.log').read_text(errors='replace')
        except Exception:
            pass
        report = evidence(text, code)
        report.update(cleanup_verified=cleanup_ok, seconds=round(time.monotonic() - started, 2))
        try:
            report['provenance'] = metadata()
        except Exception:
            report['provenance'] = {'head': 'unavailable', 'relevant_worktree_dirty': 'unavailable',
                                    'event_sha': 'unavailable', 'execution_context': 'unavailable'}
        try:
            publish(base, report, password)
        except Exception:
            code = 1
            print('Safe report unavailable', flush=True)
        for name in ('private.log', 'private-console.log'):
            (base / name).unlink(missing_ok=True)
        if cleanup_ok:
            (base / 'owner.json').unlink(missing_ok=True)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return code if code > 0 else (128 - code if code < 0 else
                                 (0 if report['complete'] and cleanup_ok else 1))


def main():
    base = Path(os.environ['STAGE2_CI_DIR'])
    if sys.argv[1:] == ['--cleanup']:
        for manifest in base.glob('run-*/owner.json'):
            fallback(manifest)
            manifest.unlink()
        for name in ('private.log', 'private-console.log'):
            for capture in base.glob('run-*/' + name):
                canonical(capture).unlink()
        return 0
    return supervise(base)


if __name__ == '__main__':
    sys.exit(main())
