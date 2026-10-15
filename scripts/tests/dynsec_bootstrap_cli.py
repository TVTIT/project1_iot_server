#!/usr/bin/env python3
"""Reusable Native DynSec Bootstrap CLI Runner and Verifier.

Executes scripts/mqtt-bootstrap.py in container mode with hardened safeguards:
- Strict isolation, unique run nonces, private temp configs (0600 file in 0700 dir)
- Random credentials generated at runtime (not passed via argv)
- Pinned immutable Mosquitto image verification (never pulls)
- Preflight authoritative volume absence check
- Receipt label verification (including task, run, and managed labels)
- Readback verification via pinned immutable helper container (log-driver none, network none, readonly)
  verifying UID/GID (1883:1883), permissions (0700 dir, 0600 file), default deny,
  and exact expected roles and ACL literals (bootstrap_manager and bootstrap_backend)
- Client identity verification: usernames, roles, no groups, disabled=False, valid native hash schema
- Idempotency / collision check (re-running same volume name must fail and preserve store digest)
- Authoritative cleanup of own helper containers and own volume with ownership verification
  (absence check requires explicit Docker 404 / no such object; errors veto success)
- Partial failure cleanup resilience: cleans owned leftover volume even if bootstrap fails
- Secret scanning on all captured output; fixed diagnostic errors
- Atomic report publication rejecting overwrite and symlinks.
- SIGINT/SIGTERM unwind in process; repeated signals are suppressed during cleanup.
   Native children have finite execution/grace/join budgets. Escalation or unknown
   ownership vetoes success and retains protected inputs and recovery receipts.
   SIGKILL and power loss cannot guarantee complete cleanup.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from contextlib import contextmanager

PINNED_IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
# The registry pin is the OCI index; Docker inspect Id depends on the image store.
PINNED_DIGEST = 'd12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
PINNED_CONFIG_ID = 'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4'
PINNED_PLATFORM_MANIFEST_DIGEST = 'sha256:f43889926d948c1146751bce701373b71c16a81e5de9b2986b7589221fa4d9e9'
BOOTSTRAP_SCRIPT = Path(__file__).resolve().parents[1] / 'mqtt-bootstrap.py'
CHILD_EXECUTION_SECONDS = 45
CHILD_GRACE_SECONDS = 120

EXPECTED_CONTROL_TOPIC = '$CONTROL/dynamic-security/v1'
EXPECTED_RESPONSE_TOPIC = '$CONTROL/dynamic-security/v1/response'
EXPECTED_TELEMETRY_PATTERN = 'gateways/+/telemetry/#'
EXPECTED_ACKS_PATTERN = 'gateways/+/acks/#'

EXPECTED_MANAGER_ROLE = 'bootstrap_manager'
EXPECTED_BACKEND_ROLE = 'bootstrap_backend'

SAFE_REPORT_KEYS = frozenset({
    'status',
    'volume_name',
    'runtime_status',
    'ownership',
    'mode',
    'image_ref',
    'image_digest',
    'run_nonce',
    'assertions_checked',
    'cleaned_up',
    'collision_rejected',
})


class RunnerError(Exception):
    """Safe runner error containing fixed diagnostics without sensitive data."""


class RunnerInterrupted(BaseException):
    """Cancellation must bypass generic Exception recovery."""

    def __init__(self, signum):
        self.signum = signum
        super().__init__('execution interrupted')


class IncompleteCleanup(RunnerError):
    """Inputs and receipts must remain protected for offline recovery."""


_cancellation = None
_NATIVE_RUN = subprocess.run


class CancellationState:
    def __init__(self):
        self.signum = None
        self.deferred = 0
        self.unwinding = False

    def handle(self, signum, frame):
        if self.signum is not None:
            return
        self.signum = signum
        if not self.deferred and not self.unwinding:
            raise RunnerInterrupted(signum)

    def checkpoint(self):
        if self.signum is not None and not self.unwinding:
            raise RunnerInterrupted(self.signum)


@contextmanager
def cancellation_scope():
    global _cancellation
    previous = _cancellation
    state = CancellationState()
    saved = {}
    _cancellation = state
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            saved[signum] = signal.getsignal(signum)
            signal.signal(signum, state.handle)
        yield state
    finally:
        state.unwinding = True
        failed = False
        for signum, handler in saved.items():
            try:
                signal.signal(signum, handler)
            except Exception:
                failed = True
        _cancellation = previous
        if failed:
            raise RunnerError('signal handler restoration failed') from None


@contextmanager
def defer_cancellation():
    state = _cancellation
    if state is not None:
        state.deferred += 1
    try:
        yield
    finally:
        if state is not None:
            state.deferred -= 1


def run_bootstrap_command(cmd, timeout=45, runner=subprocess.run, grace=120, secrets_to_scan=()):
    if any(not math.isfinite(value) or value <= 0 for value in (timeout, grace)):
        raise RunnerError('child budgets must be finite and positive')
    # Mock runners are synchronous controlled boundaries, not native children.
    if runner is not _NATIVE_RUN:
        return run_cmd(cmd, timeout=timeout, runner=runner)
    # Private files avoid pipe deadlocks and descendant-held output pipes.
    with defer_cancellation():
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            try:
                child = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
                                         start_new_session=True,
                                         env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
            except Exception:
                raise RunnerError('bootstrap child execution failed') from None
            deadline = time.monotonic() + timeout
            timed_out = False
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or (_cancellation and _cancellation.signum):
                    timed_out = remaining <= 0
                    try:
                        # The CLI flag handler must survive to finish Docker receipts.
                        # Do not interrupt its docker subprocess: signal exact CLI first.
                        child.send_signal(signal.SIGTERM)
                        child.wait(timeout=grace)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait(timeout=10)
                        except Exception:
                            raise IncompleteCleanup('INCOMPLETE_CLEANUP: child join unresolved; inputs retained') from None
                        raise IncompleteCleanup('INCOMPLETE_CLEANUP: escalated child; recovery receipts retained')
                    except Exception:
                        raise IncompleteCleanup('INCOMPLETE_CLEANUP: child termination unresolved; inputs retained') from None
                    break
                try:
                    child.wait(timeout=min(0.1, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
                except Exception:
                    raise IncompleteCleanup('INCOMPLETE_CLEANUP: child wait unresolved; inputs retained') from None
            stdout.seek(0)
            stderr.seek(0)
            res = subprocess.CompletedProcess(cmd, child.returncode, stdout.read(), stderr.read())
            try:
                os.killpg(child.pid, 0)
            except ProcessLookupError:
                pass
            except OSError:
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: child process group ownership unresolved') from None
            else:
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: child descendants still alive; inputs retained')
            scan_output_for_secrets(res.stdout, res.stderr, secrets_to_scan)
            if b'incomplete' in res.stderr.lower() or b'offline verification' in res.stderr.lower():
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: child reported unresolved ownership; receipts retained')
    if _cancellation is not None:
        _cancellation.checkpoint()
    if timed_out:
        raise RunnerError('bootstrap child deadline exceeded')
    return res


def run_cmd(cmd, input_data=None, timeout=30, runner=subprocess.run):
    try:
        res = runner(
            cmd,
            input=input_data,
            capture_output=True,
            timeout=timeout,
            check=False,
            env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}
        )
        return res
    except subprocess.TimeoutExpired:
        raise RunnerError('command timed out') from None
    except RunnerError:
        raise
    except Exception:
        raise RunnerError('command execution failed') from None


def scan_output_for_secrets(stdout_bytes, stderr_bytes, secrets_to_scan):
    """Scans captured process streams for plaintext secrets."""
    combined = (stdout_bytes or b'') + b' ' + (stderr_bytes or b'')
    for sec in secrets_to_scan:
        if sec and sec.encode('utf-8') in combined:
            raise RunnerError('plaintext password detected in process output')


def verify_pinned_image(image_ref=PINNED_IMAGE, runner=subprocess.run):
    # Keep the CLI aligned with the production bootstrap's offline descriptor verifier.
    try:
        spec = importlib.util.spec_from_file_location('mqtt_bootstrap_image_verifier', BOOTSTRAP_SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_image(image_ref, runner=runner)
    except Exception as exc:
        # Bootstrap verifier exceptions contain fixed diagnostics only; do not expose runner output.
        message = str(exc)
        safe_messages = {
            'pinned image not found locally; pulling is forbidden', 'invalid image inspect metadata',
            'image architecture or OS mismatch; linux/amd64 required', 'image RepoDigests exact pin mismatch',
            'image Id immutable digest mismatch', 'malformed image descriptor metadata',
            'contradictory image descriptor digest', 'unsupported image descriptor metadata',
            'missing or malformed root index descriptor metadata', 'root index descriptor mismatch',
            'platform-specific image inspect failed or timed out', 'platform-specific image inspect failed',
            'invalid platform-specific image inspect metadata', 'platform-specific image inspect os/architecture mismatch',
            'missing or malformed platform image descriptor metadata', 'platform-specific image descriptor mismatch',
            'platform-specific image Id mismatch', 'image verification failed or timed out', 'invalid JSON',
        }
        if message == 'invalid JSON':
            raise RunnerError('failed to parse image inspect output') from None
        if message not in safe_messages:
            message = 'image verification failed'
        raise RunnerError(message) from None


def check_volume_absent(volume_name, runner=subprocess.run):
    res = run_cmd(['docker', 'volume', 'inspect', volume_name], timeout=10, runner=runner)
    if res.returncode == 0:
        raise RunnerError(f'volume collision; volume {volume_name} already exists')
    err = res.stderr.decode('utf-8', errors='replace').lower()
    if 'no such volume' not in err:
        raise RunnerError('volume presence check failed: unexpected daemon response')


def verify_volume_receipt(volume_name, expected_labels, runner=subprocess.run):
    res = run_cmd(['docker', 'volume', 'inspect', volume_name], timeout=10, runner=runner)
    if res.returncode != 0:
        raise RunnerError('created volume inspect failed')
    try:
        data = json.loads(res.stdout.decode('utf-8'))
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise RunnerError('invalid volume inspect metadata')
        inspect = data[0]
        if inspect.get('Name') != volume_name:
            raise RunnerError('volume receipt name mismatch')
        labels = inspect.get('Labels') or {}
        for k, v in expected_labels.items():
            if labels.get(k) != v:
                raise RunnerError(f'volume receipt label mismatch for {k}')
        return inspect
    except json.JSONDecodeError:
        raise RunnerError('failed to parse volume inspect output') from None


def get_container_baseline(runner=subprocess.run):
    res = run_cmd(['docker', 'ps', '-a', '--format', '{{.ID}} {{.Names}}'], timeout=10, runner=runner)
    if res.returncode != 0:
        raise RunnerError('failed to inspect container baseline')
    containers = {}
    for line in res.stdout.decode('utf-8', errors='replace').splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            containers[parts[0]] = parts[1]
    return containers


def execute_bootstrap(config_path, image_ref=PINNED_IMAGE, timeout=45, secrets_to_scan=None, runner=subprocess.run):
    cmd = [
        sys.executable,
        str(BOOTSTRAP_SCRIPT),
        '--container',
        '--image', image_ref,
        '--config', str(config_path)
    ]
    if runner is _NATIVE_RUN:
        cmd.extend(['--recovery-dir', str(Path(config_path).parent)])
    res = run_bootstrap_command(cmd, timeout=timeout, runner=runner,
                                grace=CHILD_GRACE_SECONDS, secrets_to_scan=secrets_to_scan or ())
    if secrets_to_scan:
        scan_output_for_secrets(res.stdout, res.stderr, secrets_to_scan)
    if res.returncode != 0:
        raise RunnerError('bootstrap execution failed')
    try:
        output = json.loads(res.stdout.decode('utf-8'))
        if not isinstance(output, dict):
            raise RunnerError('bootstrap output must be a JSON object')
        return output
    except json.JSONDecodeError:
        raise RunnerError('failed to parse bootstrap JSON output') from None


READBACK_TMPFS = {p: 'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883'
                  for p in ('/mosquitto/data', '/mosquitto/log')}


def verify_readback_masks(obj, volume_name):
    mounts = obj.get('Mounts', [])
    physical = [m for m in mounts if m.get('Type') != 'tmpfs']
    if (obj.get('HostConfig', {}).get('Tmpfs') != READBACK_TMPFS
            or len(physical) != 1 or physical[0].get('Type') != 'volume'
            or physical[0].get('Name') != volume_name
            or physical[0].get('Destination') != '/mosquitto/dynsec'
            or physical[0].get('RW') is not False
            or any(m.get('Type') == 'tmpfs' and m.get('Destination') not in READBACK_TMPFS for m in mounts)):
        raise RunnerError('helper mount receipt mismatch')


def readback_and_verify_store(volume_name, image_id, expected_manager, expected_backend,
                              run_nonce=None, task_label=None, runner=subprocess.run):
    """Readback via pinned immutable helper container.

    Log-driver: none, network: none, readonly mount.
    Verifies:
    - uid/gid and modes (1883:1883:700 dir, 1883:1883:600 file)
    - default deny ACL access
    - exact manager and backend usernames and roles
    - no groups, disabled=False, valid native hash schema
    - exact expected roles and ACL literals
    - authoritative container absence upon cleanup
    """
    helper_nonce = secrets.token_hex(8)
    helper_name = f'dynsec-readback-helper-{helper_nonce}'
    task_lbl = task_label or 'dynsec-readback-helper'
    run_lbl = run_nonce or helper_nonce
    reader_script = (
        'set -e\n'
        'stat -c "%u:%g:%a" /mosquitto/dynsec /mosquitto/dynsec/dynamic-security.json\n'
        'cat /mosquitto/dynsec/dynamic-security.json\n'
    )
    cmd_create = [
        'docker', 'create', '--pull=never', '--name', helper_name,
        '--network', 'none', '--log-driver', 'none',
        '--user', '1883:1883',
        '--label', f'test.task={task_lbl}',
        '--label', f'test.run={run_lbl}',
        '--mount', f'type=volume,src={volume_name},dst=/mosquitto/dynsec,readonly',
        '--entrypoint', '/bin/sh', image_id,
        '-c', reader_script
    ]
    masks = [arg for path, options in READBACK_TMPFS.items() for arg in ('--tmpfs', path + ':' + options)]
    cmd_create[2:2] = masks
    # Keep the exact create receipt available before delivering cancellation.
    with defer_cancellation():
        res_create = run_cmd(cmd_create, timeout=10, runner=runner)
        if res_create.returncode != 0:
            raise RunnerError('failed to create readback helper container')
        cid = res_create.stdout.decode('utf-8', errors='replace').strip()
        if not re.fullmatch(r'^[0-9a-fA-F]{64}$', cid):
            raise RunnerError('invalid helper container ID')

    content = None
    helper_verified = False
    helper_foreign = False
    try:
        if _cancellation is not None:
            _cancellation.checkpoint()
        res_start = run_cmd(['docker', 'start', '-a', cid], timeout=15, runner=runner)
        if res_start.returncode != 0:
            raise RunnerError('failed to execute readback helper container')

        # Inspect helper exit state independently of docker start
        res_insp = run_cmd(['docker', 'container', 'inspect', cid], timeout=10, runner=runner)
        if res_insp.returncode != 0:
            raise RunnerError('failed to inspect readback helper container')
        try:
            insp_data = json.loads(res_insp.stdout.decode('utf-8'))[0]
            state = insp_data.get('State') or {}
            exit_code = state.get('ExitCode')
            # Verify helper ownership and labels before removal
            cfg_labels = (insp_data.get('Config') or {}).get('Labels') or {}
            if (insp_data.get('Id') != cid or insp_data.get('Image') != image_id
                    or cfg_labels.get('test.task') != task_lbl
                    or cfg_labels.get('test.run') != run_lbl):
                helper_foreign = True
                raise RunnerError('helper container receipt ownership mismatch')
            if image_id == 'sha256:' + PINNED_DIGEST:
                descriptor = insp_data.get('ImageManifestDescriptor')
                if (not isinstance(descriptor, dict)
                        or descriptor.get('digest') != PINNED_PLATFORM_MANIFEST_DIGEST
                        or descriptor.get('mediaType') != 'application/vnd.oci.image.manifest.v1+json'):
                    helper_foreign = True
                    raise RunnerError('helper container image manifest descriptor mismatch')
            helper_verified = True
            verify_readback_masks(insp_data, volume_name)
            if exit_code != 0:
                raise RunnerError('helper container execution failed with non-zero exit code')
        except (json.JSONDecodeError, IndexError, KeyError):
            raise RunnerError('failed to parse helper container inspect output') from None

        lines = res_start.stdout.splitlines()
        if len(lines) < 3:
            raise RunnerError('incomplete readback helper output')
        dir_stat = lines[0].decode('utf-8', errors='replace').strip()
        file_stat = lines[1].decode('utf-8', errors='replace').strip()
        if dir_stat != '1883:1883:700' or file_stat != '1883:1883:600':
            raise RunnerError('store readback permission verification failed')
        raw_bytes = b'\n'.join(lines[2:]).strip()
        store_data = json.loads(raw_bytes.decode('utf-8'))
        content = (store_data, raw_bytes)
    finally:
        # Only exact returned IDs with matching receipt labels may be removed.
        with defer_cancellation():
            if not helper_verified and not helper_foreign:
                receipt = run_cmd(['docker', 'container', 'inspect', cid], timeout=10, runner=runner)
                try:
                    data = json.loads(receipt.stdout.decode('utf-8'))
                    obj = data[0]
                    cfg_labels = (obj.get('Config') or {}).get('Labels') or {}
                    helper_verified = (receipt.returncode == 0 and len(data) == 1
                                       and obj.get('Id') == cid and obj.get('Image') == image_id
                                       and cfg_labels.get('test.task') == task_lbl
                                       and cfg_labels.get('test.run') == run_lbl)
                    if helper_verified and image_id == 'sha256:' + PINNED_DIGEST:
                        descriptor = obj.get('ImageManifestDescriptor')
                        helper_verified = (isinstance(descriptor, dict)
                                           and descriptor.get('digest') == PINNED_PLATFORM_MANIFEST_DIGEST
                                           and descriptor.get('mediaType') == 'application/vnd.oci.image.manifest.v1+json')
                except Exception:
                    raise RunnerError('helper container cleanup receipt failed') from None
                if not helper_verified:
                    raise RunnerError('helper container receipt ownership mismatch')
                verify_readback_masks(obj, volume_name)
            if helper_verified:
                rm_res = run_cmd(['docker', 'rm', '-f', cid], timeout=10, runner=runner)
                if rm_res.returncode != 0:
                    raise RunnerError('helper container removal failed')
                insp_res = run_cmd(['docker', 'container', 'inspect', cid], timeout=10, runner=runner)
                if insp_res.returncode == 0:
                    raise RunnerError('helper container still exists after cleanup')
                err_msg = insp_res.stderr.decode('utf-8', errors='replace').lower()
                if 'no such container' not in err_msg:
                    raise RunnerError('helper container absence verification failed')

    store, store_raw = content
    # Verify default deny
    default_acl = store.get('defaultACLAccess')
    if not isinstance(default_acl, dict):
        raise RunnerError('defaultACLAccess missing or invalid')
    for perm in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe'):
        if default_acl.get(perm) is not False:
            raise RunnerError(f'defaultACLAccess for {perm} is not denied')

    # Verify clients: exactly manager and backend
    clients = store.get('clients')
    if not isinstance(clients, list) or len(clients) != 2:
        raise RunnerError('store must contain exactly 2 clients')

    client_map = {}
    for c in clients:
        if not isinstance(c, dict):
            raise RunnerError('client must be an object')
        uname = c.get('username')
        if not isinstance(uname, str):
            raise RunnerError('client username must be a string')
        client_map[uname] = c

    if set(client_map.keys()) != {expected_manager, expected_backend}:
        raise RunnerError('store client identities do not match expected principals')

    # Verify manager client details
    mgr_c = client_map[expected_manager]
    mgr_roles = mgr_c.get('roles')
    if mgr_roles != [{'rolename': EXPECTED_MANAGER_ROLE}]:
        raise RunnerError('manager client role mismatch')
    if mgr_c.get('groups') not in (None, []):
        raise RunnerError('client groups must be empty')
    if mgr_c.get('disabled') not in (None, False):
        raise RunnerError('client disabled flag must be absent or False')

    # Verify backend client details
    be_c = client_map[expected_backend]
    be_roles = be_c.get('roles')
    if be_roles != [{'rolename': EXPECTED_BACKEND_ROLE}]:
        raise RunnerError('backend client role mismatch')
    if be_c.get('groups') not in (None, []):
        raise RunnerError('client groups must be empty')
    if be_c.get('disabled') not in (None, False):
        raise RunnerError('client disabled flag must be absent or False')

    # Verify native hash schema for both
    for u, c in client_map.items():
        try:
            pw = c.get('password')
            salt = c.get('salt')
            it = c.get('iterations')
            if not isinstance(pw, str) or len(base64.b64decode(pw, validate=True)) != 64:
                raise ValueError()
            if not isinstance(salt, str) or len(base64.b64decode(salt, validate=True)) != 12:
                raise ValueError()
            if not isinstance(it, int) or it <= 0:
                raise ValueError()
        except Exception:
            raise RunnerError('client native hash schema invalid') from None

    # Verify roles and expected ACL literals
    roles = store.get('roles')
    if not isinstance(roles, list) or len(roles) != 2:
        raise RunnerError('store must contain exactly 2 roles')
    role_map = {r.get('rolename'): r.get('acls') for r in roles if isinstance(r, dict)}
    if set(role_map.keys()) != {EXPECTED_MANAGER_ROLE, EXPECTED_BACKEND_ROLE}:
        raise RunnerError('store role names do not match expected manager/backend roles')

    # Check manager role ACL literals
    manager_acls = role_map[EXPECTED_MANAGER_ROLE]
    if not isinstance(manager_acls, list):
        raise RunnerError('manager role ACLs must be a list')
    expected_mgr_acls = [
        {'acltype': 'publishClientSend', 'topic': EXPECTED_CONTROL_TOPIC, 'allow': True, 'priority': 0},
        {'acltype': 'publishClientReceive', 'topic': EXPECTED_RESPONSE_TOPIC, 'allow': True, 'priority': 0},
        {'acltype': 'subscribeLiteral', 'topic': EXPECTED_RESPONSE_TOPIC, 'allow': True, 'priority': 0},
        {'acltype': 'unsubscribeLiteral', 'topic': EXPECTED_RESPONSE_TOPIC, 'allow': True, 'priority': 0},
    ]
    if manager_acls != expected_mgr_acls:
        raise RunnerError('manager role ACL literals mismatch')

    # Check backend role ACL literals
    backend_acls = role_map[EXPECTED_BACKEND_ROLE]
    if not isinstance(backend_acls, list):
        raise RunnerError('backend role ACLs must be a list')
    expected_backend_acls = [
        {'acltype': 'publishClientReceive', 'topic': EXPECTED_TELEMETRY_PATTERN, 'allow': True, 'priority': 0},
        {'acltype': 'subscribeLiteral', 'topic': EXPECTED_TELEMETRY_PATTERN, 'allow': True, 'priority': 0},
        {'acltype': 'unsubscribeLiteral', 'topic': EXPECTED_TELEMETRY_PATTERN, 'allow': True, 'priority': 0},
        {'acltype': 'publishClientSend', 'topic': EXPECTED_ACKS_PATTERN, 'allow': True, 'priority': 0},
    ]
    if backend_acls != expected_backend_acls:
        raise RunnerError('backend role ACL literals mismatch')

    digest = hashlib.sha256(store_raw).hexdigest()
    return digest


def verify_collision_and_immutability(config_path, expected_digest, volume_name, image_id,
                                      expected_manager, expected_backend,
                                      secrets_to_scan=None, run_nonce=None, task_label=None,
                                      runner=subprocess.run):
    """Invoke CLI with same volume name; must fail and store bytes/digest must remain unchanged."""
    cmd = [
        sys.executable,
        str(BOOTSTRAP_SCRIPT),
        '--container',
        '--image', PINNED_IMAGE,
        '--config', str(config_path)
    ]
    if runner is _NATIVE_RUN:
        cmd.extend(['--recovery-dir', str(Path(config_path).parent)])
    res = run_bootstrap_command(cmd, timeout=CHILD_EXECUTION_SECONDS, runner=runner,
                                grace=CHILD_GRACE_SECONDS, secrets_to_scan=secrets_to_scan or ())
    if secrets_to_scan:
        scan_output_for_secrets(res.stdout, res.stderr, secrets_to_scan)
    if res.returncode == 0:
        raise RunnerError('collision execution unexpectedly succeeded')

    after_digest = readback_and_verify_store(
        volume_name, image_id, expected_manager, expected_backend,
        run_nonce=run_nonce, task_label=task_label, runner=runner
    )
    if after_digest != expected_digest:
        raise RunnerError('volume store digest modified during collision attempt')


def cleanup_volume(volume_name, expected_labels, runner=subprocess.run):
    """Removes own volume ONLY after authoritative ownership verification.

    Absence verification is authoritative (requires explicit Docker 404); failure vetoes success.
    """
    verify_volume_receipt(volume_name, expected_labels, runner=runner)
    res_rm = run_cmd(['docker', 'volume', 'rm', volume_name], timeout=10, runner=runner)
    if res_rm.returncode != 0:
        raise RunnerError('failed to remove volume')
    res_check = run_cmd(['docker', 'volume', 'inspect', volume_name], timeout=10, runner=runner)
    if res_check.returncode == 0:
        raise RunnerError('volume still exists after removal')
    err_msg = res_check.stderr.decode('utf-8', errors='replace').lower()
    if 'no such volume' not in err_msg:
        raise RunnerError('volume absence verification failed')


def reconcile_child_receipts(directory, volume_name, labels, runner=subprocess.run):
    """Only receipts within this run's private directory are recovery authority.

    Use the production ownership validator, not a name prefix. Missing/invalid
    receipts after escalation remain an explicit offline-recovery boundary.
    """
    spec = importlib.util.spec_from_file_location('bootstrap_recovery', BOOTSTRAP_SCRIPT)
    production = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(production)
    for path in Path(directory).glob('*/recovery.json'):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise IncompleteCleanup('INCOMPLETE_CLEANUP: invalid recovery receipt')
        item = production.strict_json(path.read_text())
        if item.get('kind') == 'volume':
            if item.get('name') != volume_name or any(item.get('labels', {}).get(k) != v for k, v in labels.items()):
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: volume intent mismatch')
            labels['bootstrap.nonce'] = item['labels']['bootstrap.nonce']
        elif item.get('kind') in ('native-init', 'ownership', 'writer', 'reader'):
            if item.get('image') != 'sha256:' + PINNED_DIGEST:
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: helper image intent mismatch')
            item['receipt'] = str(path)
            if production.cleanup_containers([item], runner=runner):
                raise IncompleteCleanup('INCOMPLETE_CLEANUP: helper recovery unresolved')
        else:
            # Aggregate failure receipts can refer to protected staging elsewhere.
            raise IncompleteCleanup('INCOMPLETE_CLEANUP: protected staging recovery required')


def verify_transient_containers_baseline(baseline, runner=subprocess.run):
    current = get_container_baseline(runner=runner)
    new_containers = set(current.keys()) - set(baseline.keys())
    if any(name.startswith('mqtt-boot-') for name in (current.get(cid, '') for cid in new_containers)):
        raise RunnerError('transient bootstrap containers leaked into daemon state')


def publish_safe_report(report_path, report_data):
    """Publishes safe JSON verification report atomically; rejects overwrite and symlinks."""
    target_path = Path(report_path)
    if target_path.is_symlink() or target_path.exists():
        raise RunnerError('report path already exists; overwriting is forbidden')

    target_dir = target_path.parent
    if not target_dir.exists():
        raise RunnerError('report parent directory does not exist')

    safe_payload = {k: report_data[k] for k in sorted(report_data.keys()) if k in SAFE_REPORT_KEYS}
    serialized = json.dumps(safe_payload, indent=2).encode('utf-8')

    temp_path = target_dir / f'.report_tmp_{secrets.token_hex(8)}'
    try:
        fd = os.open(str(temp_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as f:
            f.write(serialized)
            f.flush()
            os.fsync(fd)
        os.link(str(temp_path), str(target_path))
    except FileExistsError:
        raise RunnerError('report target appeared concurrently; overwriting is forbidden') from None
    except OSError:
        raise RunnerError('failed to atomically publish report') from None
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            raise RunnerError('failed to cleanup temporary report file') from None


def run_pipeline(report_path=None, runner=subprocess.run):
    with cancellation_scope() as state:
        report = _run_pipeline(runner=runner)
        state.checkpoint()
    # Handler restoration is a prerequisite for publishing success.
    if report_path is not None:
        publish_safe_report(report_path, report)
    return report


def _run_pipeline(report_path=None, runner=subprocess.run):
    """Main execution orchestrating all verification steps."""
    assertions_checked = 0
    run_nonce = secrets.token_hex(16)
    volume_name = f'mosquitto_dynsec_{run_nonce[:24]}'
    task_label = 'dynsec-bootstrap-test'

    labels = {
        'com.docker.compose.volume': 'mosquitto_dynsec',
        'bootstrap.managed': 'true',
        'test.task': task_label,
        'test.run': run_nonce,
    }

    # Step 1: Baseline containers and verify pinned image
    baseline_containers = get_container_baseline(runner=runner)
    image_id = verify_pinned_image(PINNED_IMAGE, runner=runner)
    assertions_checked += 1

    # Step 2: Preflight authoritative volume absence check
    check_volume_absent(volume_name, runner=runner)
    assertions_checked += 1

    # Step 3: Write protected configuration (0600 file in 0700 private temp dir under /tmp/opencode)
    temp_dir = None
    config_file = None
    cleanup_errors = []
    cleaned_up = False
    pipeline_failed_exc = None
    store_digest = None

    try:
        parent_tmp = '/tmp/opencode' if os.path.exists('/tmp/opencode') else None
        temp_dir = tempfile.mkdtemp(prefix='dynsec_run_', dir=parent_tmp)
        os.chmod(temp_dir, 0o700)
        config_file = Path(temp_dir) / 'config.json'

        manager_user = f'mgr_{secrets.token_hex(4)}'
        backend_user = f'be_{secrets.token_hex(4)}'
        manager_pwd = secrets.token_urlsafe(32)
        backend_pwd = secrets.token_urlsafe(32)
        secrets_to_scan = [manager_pwd, backend_pwd]

        config_data = {
            'manager': manager_user,
            'backend': backend_user,
            'manager_password': manager_pwd,
            'backend_password': backend_pwd,
            'volume_name': volume_name,
            'labels': {
                'test.task': task_label,
                'test.run': run_nonce,
            }
        }

        fd = os.open(str(config_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(config_data, f)

        # Step 4: Execute bootstrap via CLI
        boot_out = execute_bootstrap(
            config_file, image_ref=PINNED_IMAGE, timeout=CHILD_EXECUTION_SECONDS,
            secrets_to_scan=secrets_to_scan, runner=runner
        )
        assertions_checked += 1

        if boot_out.get('status') != 'store_created':
            raise RunnerError('bootstrap status is not store_created')
        if boot_out.get('volume_name') != volume_name:
            raise RunnerError('bootstrap volume_name mismatch')
        if boot_out.get('runtime_status') != 'NOT_READY':
            raise RunnerError('bootstrap runtime_status is not NOT_READY')
        assertions_checked += 3

        # Verify volume ownership and receipt labels, capturing bootstrap.nonce
        vol_receipt = verify_volume_receipt(volume_name, labels, runner=runner)
        boot_nonce = (vol_receipt.get('Labels') or {}).get('bootstrap.nonce')
        if not boot_nonce:
            raise RunnerError('bootstrap.nonce label missing from volume receipt')
        labels['bootstrap.nonce'] = boot_nonce
        assertions_checked += 1

        # Step 5: Readback verification via helper container
        store_digest = readback_and_verify_store(
            volume_name, image_id, manager_user, backend_user,
            run_nonce=run_nonce, task_label=task_label, runner=runner
        )
        assertions_checked += 6  # perms, default deny, identities, roles, literals, hash schema

        # Step 6: Collision check
        verify_collision_and_immutability(
            config_file, store_digest, volume_name, image_id,
            manager_user, backend_user,
            secrets_to_scan=secrets_to_scan, run_nonce=run_nonce, task_label=task_label,
            runner=runner
        )
        assertions_checked += 2

    except BaseException as exc:
        pipeline_failed_exc = exc

    finally:
        if _cancellation is not None:
            _cancellation.unwinding = True
        # An unresolved/live child can still use inputs and mounted volumes.
        if isinstance(pipeline_failed_exc, IncompleteCleanup):
            raise IncompleteCleanup(f'INCOMPLETE_CLEANUP: protected inputs and receipts retained at {temp_dir}')
        if runner is _NATIVE_RUN and temp_dir:
            try:
                reconcile_child_receipts(temp_dir, volume_name, labels, runner=runner)
            except Exception:
                raise IncompleteCleanup(f'INCOMPLETE_CLEANUP: recovery unresolved; protected inputs at {temp_dir}') from None

        # Action B: Authoritative cleanup of created or leftover volume
        try:
            res_insp = run_cmd(['docker', 'volume', 'inspect', volume_name], timeout=10, runner=runner)
            if res_insp.returncode == 0:
                # Volume exists; verify ownership and labels before removal
                try:
                    insp_v = json.loads(res_insp.stdout.decode('utf-8'))[0]
                    v_labels = insp_v.get('Labels') or {}
                    for lk, lv in labels.items():
                        if v_labels.get(lk) != lv:
                            raise RunnerError('foreign leftover volume label mismatch')
                    # Owned volume confirmed; remove it
                    cleanup_volume(volume_name, labels, runner=runner)
                    cleaned_up = True
                    assertions_checked += 1
                except Exception:
                    cleanup_errors.append('volume cleanup failed')
            else:
                err_str = res_insp.stderr.decode('utf-8', errors='replace').lower()
                if 'no such volume' in err_str:
                    # Cleanly absent
                    cleaned_up = True
                else:
                    cleanup_errors.append('volume presence inspect failed during cleanup')
        except Exception:
            cleanup_errors.append('volume cleanup inspect error')

        # Action C: Transient containers baseline check
        try:
            verify_transient_containers_baseline(baseline_containers, runner=runner)
            assertions_checked += 1
        except Exception:
            cleanup_errors.append('transient container cleanup failed')

        # Inputs are removed only after exact resource cleanup has succeeded.
        if not cleanup_errors:
            try:
                if runner is _NATIVE_RUN and temp_dir:
                    for receipt in Path(temp_dir).glob('*/recovery.json'):
                        receipt.unlink()
                        receipt.parent.rmdir()
                if config_file is not None and config_file.exists():
                    config_file.unlink()
                if temp_dir is not None and os.path.exists(temp_dir):
                    os.rmdir(temp_dir)
            except OSError:
                cleanup_errors.append('temporary configuration cleanup failed')

    # If any pipeline error or cleanup error occurred, fail closed
    if cleanup_errors:
        raise RunnerError(f'INCOMPLETE_CLEANUP: cleanup failure: {", ".join(cleanup_errors)}; protected inputs at {temp_dir}')
    if pipeline_failed_exc is not None:
        raise pipeline_failed_exc
    if _cancellation is not None and _cancellation.signum is not None:
        raise RunnerInterrupted(_cancellation.signum)

    report = {
        'status': 'success',
        'volume_name': volume_name,
        'runtime_status': 'NOT_READY',
        'ownership': '1883:1883',
        'mode': 'container',
        'image_ref': PINNED_IMAGE,
        'image_digest': 'sha256:' + PINNED_DIGEST,
        'run_nonce': run_nonce,
        'assertions_checked': assertions_checked,
        'cleaned_up': cleaned_up,
        'collision_rejected': True,
    }

    if report_path is not None:
        publish_safe_report(report_path, report)

    return report


def main():
    global CHILD_EXECUTION_SECONDS, CHILD_GRACE_SECONDS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, default=None, help='path to write safe JSON verification report')
    parser.add_argument('--child-execution-seconds', type=float, default=45)
    parser.add_argument('--child-grace-seconds', type=float, default=120)
    args = parser.parse_args()
    if any(not math.isfinite(v) or v <= 0 for v in (args.child_execution_seconds, args.child_grace_seconds)):
        parser.error('child budgets must be finite and positive')
    CHILD_EXECUTION_SECONDS = args.child_execution_seconds
    CHILD_GRACE_SECONDS = args.child_grace_seconds

    try:
        report = run_pipeline(report_path=args.report)
        print(json.dumps(report, indent=2))
        return 0
    except RunnerError as e:
        print(f'RunnerError: {e}', file=sys.stderr)
        return 1
    except RunnerInterrupted as exc:
        message = ('execution interrupted (SIGTERM)' if exc.signum == signal.SIGTERM
                   else 'execution interrupted (SIGINT)')
        print(f'RunnerError: {message}', file=sys.stderr)
        return 1
    except Exception:
        print('RunnerError: unexpected failure', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
