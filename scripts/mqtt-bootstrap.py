#!/usr/bin/env python3
"""Offline fresh Mosquitto 2.0.18 DynSec store. Never opens the ingress gate.

Supports two modes:
1. Container mode (recommended): Runs inside the verified pinned local Mosquitto
   2.0.18 image (linux/amd64). Provisions a fresh, owned Docker named volume
   holding dynamic-security.json with 1883:1883 permissions (0700 dir, 0600 file).
   Outputs the safe new volume name.
2. Host mode: Run with a trusted, root-owned host Mosquitto 2.0.18 mosquitto_ctrl.
   The containing directory must already be empty, 0700 and owned by mapped UID/GID.

Read one strict JSON object from protected stdin or protected file.
No deployment discovery, legacy import, directory chown outside owned volume,
or DB access occurs.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import uuid

PINNED_IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
PINNED_DIGEST = 'd12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
EXPECTED_VERSION_BANNER = b'mosquitto_ctrl version 2.0.18 running on libmosquitto 2.0.18.'
SAFE_IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$')
SAFE_HEX_64_PATTERN = re.compile(r'^[0-9a-fA-F]{64}$')
SAFE_LABEL_KEY_PATTERN = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9_.-]{0,126}[a-zA-Z0-9])?$')
RESERVED_LABEL_KEYS = frozenset({'bootstrap.managed', 'bootstrap.nonce', 'com.docker.compose.volume'})
MAX_LABEL_KEY_LEN = 128
MAX_LABEL_VAL_LEN = 256
MAX_TOTAL_LABELS = 32


class BootstrapError(Exception):
    """Safe error: never contains credential/input/native output."""


_cancelled = False
_cleaning = False
_recovery_parent = '/tmp/opencode'


def request_cancellation(signum, frame):
    global _cancelled
    _cancelled = True


def cancellation_checkpoint():
    if _cancelled and not _cleaning:
        raise BootstrapError('bootstrap interrupted; cleanup required')


def save_intent(item):
    directory = tempfile.mkdtemp(dir=_recovery_parent, prefix='mqtt-bootstrap-intent-')
    path = Path(directory) / 'recovery.json'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(item, stream)
        stream.flush()
        os.fsync(stream.fileno())
    return str(path)


def discard_intent(item):
    path = item.get('receipt')
    if path:
        os.unlink(path)
        os.rmdir(str(Path(path).parent))


def strict_json(text):
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise BootstrapError('duplicate JSON field')
            obj[key] = value
        return obj
    try:
        return json.loads(text, object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, TypeError):
        raise BootstrapError('invalid JSON') from None


def mapped_id(rows):
    if not isinstance(rows, list) or not rows:
        raise BootstrapError('rootless mapping required')
    result = None
    ranges = []
    for row in rows:
        if (not isinstance(row, list) or len(row) != 3 or
                any(type(v) is not int or v < 0 for v in row) or row[2] < 1):
            raise BootstrapError('invalid mapping')
        if any(row[0] < start + count and start < row[0] + row[2] for start, count in ranges):
            raise BootstrapError('overlapping mapping')
        ranges.append((row[0], row[2]))
        if row[0] <= 1883 < row[0] + row[2]:
            if result is not None:
                raise BootstrapError('ambiguous mapping')
            result = row[1] + 1883 - row[0]
    if result is None or result == 0:
        raise BootstrapError('broker mapping absent or privileged')
    return result


def parse_config(stream, container_mode=False):
    text = stream.read(16385)
    if len(text) > 16384:
        raise BootstrapError('configuration too large')
    cfg = strict_json(text)
    if not isinstance(cfg, dict):
        raise BootstrapError('configuration must be a JSON object')

    if container_mode:
        required = {'manager', 'backend', 'manager_password', 'backend_password'}
        optional = {'volume_name', 'labels'}
        if not required <= cfg.keys() or cfg.keys() - required - optional:
            raise BootstrapError('configuration fields missing or unknown')
        if 'volume_name' in cfg:
            vol = cfg['volume_name']
            if not isinstance(vol, str) or not SAFE_IDENTIFIER_PATTERN.fullmatch(vol):
                raise BootstrapError('invalid volume_name')
        if 'labels' in cfg:
            labels = cfg['labels']
            if not isinstance(labels, dict) or len(labels) > MAX_TOTAL_LABELS:
                raise BootstrapError('labels must be a JSON object within size limits')
            for k, v in labels.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise BootstrapError('label keys and values must be strings')
                if len(k) > MAX_LABEL_KEY_LEN or len(v) > MAX_LABEL_VAL_LEN:
                    raise BootstrapError('label length exceeded')
                if not SAFE_LABEL_KEY_PATTERN.fullmatch(k):
                    raise BootstrapError('invalid label key format')
                if k in RESERVED_LABEL_KEYS:
                    raise BootstrapError(f'reserved label key rejected: {k}')
    else:
        required = {'store', 'manager', 'backend', 'manager_password', 'backend_password',
                    'mapping', 'broker_uid', 'broker_gid'}
        if not required <= cfg.keys() or cfg.keys() - required - {'uid_map', 'gid_map'}:
            raise BootstrapError('configuration fields missing or unknown')
        if not isinstance(cfg['store'], str) or not Path(cfg['store']).is_absolute() or '\x00' in cfg['store']:
            raise BootstrapError('absolute store path required')
        if cfg['mapping'] == 'rootful':
            ids = (1883, 1883)
            if 'uid_map' in cfg or 'gid_map' in cfg:
                raise BootstrapError('unexpected rootful map')
        elif cfg['mapping'] == 'rootless':
            ids = (mapped_id(cfg.get('uid_map')), mapped_id(cfg.get('gid_map')))
        else:
            raise BootstrapError('explicit ownership mapping required')
        if any(type(cfg[k]) is not int for k in ('broker_uid', 'broker_gid')) or ids != (cfg['broker_uid'], cfg['broker_gid']):
            raise BootstrapError('ownership does not match broker 1883 mapping')

    for key in ('manager', 'backend'):
        if not isinstance(cfg[key], str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', cfg[key]):
            raise BootstrapError('invalid principal')
    if cfg['manager'] == cfg['backend']:
        raise BootstrapError('principals must differ')
    for key in ('manager_password', 'backend_password'):
        value = cfg[key]
        if not isinstance(value, str) or not 32 <= len(value) <= 128 or any(not 33 <= ord(c) <= 126 for c in value):
            raise BootstrapError('invalid credential; use independently generated random secrets')
    if cfg['manager_password'] == cfg['backend_password']:
        raise BootstrapError('credentials must differ')
    return cfg


def guard_directory(path, uid, gid):
    """Pin every ancestor with O_NOFOLLOW; never follow a destination symlink."""
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or path.name in ('', '.', '..'):
        raise BootstrapError('unsafe store path')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parent.parts[1:]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
            meta = os.fstat(fd)
            sticky_root = meta.st_uid == 0 and bool(meta.st_mode & stat.S_ISVTX)
            if meta.st_uid not in (0, os.geteuid(), uid) or (meta.st_mode & 0o022 and not sticky_root):
                raise BootstrapError('unsafe ancestor ownership or permissions')
        meta = os.fstat(fd)
        if (meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)) != (uid, gid, 0o700):
            raise BootstrapError('guarded directory must be mapped broker-owned 0700')
        if os.listdir(fd):
            raise BootstrapError('guarded directory must be empty; no overwrite or import')
        return fd
    except (OSError, BootstrapError):
        os.close(fd)
        raise BootstrapError('unsafe or inaccessible guarded directory') from None


def native_init(tool, destination, username, password, runner=subprocess.run):
    try:
        result = runner([tool, 'dynsec', 'init', str(destination), username],
                        input=((password + '\n') * 2).encode(), stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL, timeout=15, check=False,
                        env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if result.returncode:
            raise BootstrapError('native initialization failed')
    except (OSError, subprocess.SubprocessError):
        raise BootstrapError('native initialization unavailable or timed out') from None


def acl(kind, topic):
    return {'acltype': kind, 'topic': topic, 'allow': True, 'priority': 0}


def compose_store(manager, backend):
    """Preserve native credentials; replace broad init ACLs with exact roles."""
    clients = []
    for source, role in ((manager, 'bootstrap_manager'), (backend, 'bootstrap_backend')):
        if not isinstance(source, dict) or len(source.get('clients', [])) != 1:
            raise BootstrapError('unexpected native client schema')
        client = dict(source['clients'][0])
        if not {'username', 'password', 'salt', 'iterations', 'roles'} <= client.keys():
            raise BootstrapError('native credential fields missing')
        client['roles'] = [{'rolename': role}]
        clients.append(client)
    control = '$CONTROL/dynamic-security/v1'
    response = control + '/response'
    management = [acl('publishClientSend', control)] + [acl(k, response) for k in
                  ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')]
    telemetry = [acl(k, 'gateways/+/telemetry/#') for k in
                 ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')]
    telemetry.append(acl('publishClientSend', 'gateways/+/acks/#'))
    return {'clients': clients, 'roles': [
        {'rolename': 'bootstrap_manager', 'acls': management},
        {'rolename': 'bootstrap_backend', 'acls': telemetry}],
        'defaultACLAccess': {k: False for k in
                            ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')}}


def build_store(cfg, tool, initializer=native_init):
    with tempfile.TemporaryDirectory(prefix='mqtt-bootstrap-') as directory:
        sources = []
        for name in ('manager', 'backend'):
            path = Path(directory) / (name + '.json')
            initializer(tool, path, cfg[name], cfg[name + '_password'])
            with path.open() as stream:
                source = strict_json(stream.read(65537))
            if not isinstance(source, dict) or not isinstance(source.get('clients'), list) or len(source['clients']) != 1 or not isinstance(source['clients'][0], dict):
                raise BootstrapError('unexpected native client schema')
            client = source['clients'][0]
            if client.get('username') != cfg[name]:
                raise BootstrapError('native principal mismatch')
            try:
                if (len(base64.b64decode(client['password'], validate=True)) != 64 or
                        len(base64.b64decode(client['salt'], validate=True)) != 12 or
                        type(client['iterations']) is not int or client['iterations'] <= 0):
                    raise ValueError()
            except (KeyError, ValueError, TypeError):
                raise BootstrapError('unexpected native hash schema') from None
            sources.append(source)
        return json.dumps(compose_store(*sources), separators=(',', ':')).encode()


def publish(directory_fd, name, data, uid, gid):
    temporary = '.bootstrap-' + uuid.uuid4().hex
    fd = None
    try:
        if os.listdir(directory_fd):
            raise BootstrapError('guarded directory no longer empty')
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory_fd)
        os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb', closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(fd)
        meta = os.fstat(fd)
        if (meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)) != (uid, gid, 0o600):
            raise BootstrapError('cannot establish protected broker ownership')
        if os.listdir(directory_fd) != [temporary]:
            raise BootstrapError('guarded directory changed during initialization')
        os.link(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd,
                follow_symlinks=False)
        os.unlink(temporary, dir_fd=directory_fd)
        os.fsync(directory_fd)
    except PermissionError:
        raise BootstrapError('cannot establish mapped broker ownership; no chmod fallback') from None
    except OSError:
        raise BootstrapError('publication failed; verify directory offline, never overwrite/retry blindly') from None
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


def verify_native_tool(tool):
    executable = Path(tool)
    try:
        meta = executable.stat()
    except OSError:
        raise BootstrapError('native tool missing or inaccessible; install trusted Mosquitto 2.0.18 on host') from None
    if not executable.is_absolute() or executable.is_symlink() or not stat.S_ISREG(meta.st_mode) or meta.st_uid != 0 or meta.st_mode & 0o022:
        raise BootstrapError('trusted root-owned native executable required')
    for ancestor in executable.parents:
        meta = ancestor.lstat()
        if ancestor.is_symlink() or meta.st_uid != 0 or meta.st_mode & 0o022:
            raise BootstrapError('unsafe native executable ancestor')
    version = subprocess.run([str(executable), '--help'], capture_output=True,
                             timeout=5, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    if EXPECTED_VERSION_BANNER not in version.stdout:
        raise BootstrapError('Mosquitto/libmosquitto 2.0.18 required')
    return str(executable)


def bootstrap(cfg, tool):
    directory_fd = guard_directory(Path(cfg['store']), cfg['broker_uid'], cfg['broker_gid'])
    try:
        executable = verify_native_tool(tool)
        old_umask = os.umask(0o077)
        try:
            data = build_store(cfg, str(executable))
            publish(directory_fd, Path(cfg['store']).name, data, cfg['broker_uid'], cfg['broker_gid'])
        finally:
            os.umask(old_umask)
    finally:
        os.close(directory_fd)


# --- Container Bootstrap Mode ---

def run_docker(cmd, input_data=None, timeout=30, capture=True, runner=subprocess.run):
    try:
        res = runner(cmd, input=input_data, capture_output=capture,
                     timeout=timeout, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res.returncode != 0:
            raise BootstrapError('docker command failed')
        return res
    except (subprocess.TimeoutExpired, BootstrapError):
        raise
    except Exception:
        raise BootstrapError('docker command error') from None


def verify_image(image_ref, runner=subprocess.run):
    try:
        res = runner(['docker', 'image', 'inspect', image_ref], capture_output=True,
                     timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res.returncode != 0:
            raise BootstrapError('pinned image not found locally; pulling is forbidden')
        data = strict_json(res.stdout.decode())
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise BootstrapError('invalid image inspect metadata')
        inspect = data[0]
        arch = inspect.get('Architecture')
        os_name = inspect.get('Os')
        if arch != 'amd64' or os_name != 'linux':
            raise BootstrapError('image architecture or OS mismatch; linux/amd64 required')

        digests = inspect.get('RepoDigests', [])
        if not any(d == PINNED_IMAGE for d in digests):
            raise BootstrapError('image RepoDigests exact pin mismatch')

        raw_id = inspect.get('Id', '')
        if raw_id.startswith('sha256:'):
            raw_id = raw_id[7:]
        if not SAFE_HEX_64_PATTERN.fullmatch(raw_id) or raw_id.lower() != PINNED_DIGEST.lower():
            raise BootstrapError('image Id immutable digest mismatch')

        # The exact immutable pin is the dependency receipt. Do not allocate an
        # untracked container merely to repeat the pinned version banner check.
        return 'sha256:' + raw_id
    except BootstrapError:
        raise
    except (subprocess.SubprocessError, OSError):
        raise BootstrapError('image verification failed or timed out') from None


def check_volume_absent(volume_name, runner=subprocess.run):
    try:
        res = runner(['docker', 'volume', 'inspect', volume_name], capture_output=True,
                     timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res.returncode == 0:
            raise BootstrapError(f'volume collision; volume {volume_name} already exists')
        err = res.stderr.decode()
        if 'no such volume' not in err.lower():
            raise BootstrapError('volume presence check failed: daemon returned unexpected error or timeout')
    except BootstrapError:
        raise
    except (subprocess.SubprocessError, OSError):
        raise BootstrapError('volume presence check failed') from None


def verify_volume_receipt(volume_name, expected_labels, runner=subprocess.run):
    try:
        res = runner(['docker', 'volume', 'inspect', volume_name], capture_output=True,
                     timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res.returncode != 0:
            raise BootstrapError('created volume inspect failed')
        data = strict_json(res.stdout.decode())
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise BootstrapError('invalid volume inspect metadata')
        inspect = data[0]
        if inspect.get('Name') != volume_name:
            raise BootstrapError('volume receipt name mismatch')
        labels = inspect.get('Labels') or {}
        for k, v in expected_labels.items():
            if labels.get(k) != v:
                raise BootstrapError('volume receipt label mismatch')
        return inspect
    except BootstrapError:
        raise
    except (subprocess.SubprocessError, OSError):
        raise BootstrapError('volume verification failed') from None


def inspect_container_state(cid, runner=subprocess.run):
    try:
        res = runner(['docker', 'container', 'inspect', cid], capture_output=True,
                     timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res.returncode != 0:
            raise BootstrapError('container inspect failed')
        data = strict_json(res.stdout.decode())
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise BootstrapError('invalid container inspect metadata')
        state = data[0].get('State') or {}
        exit_code = state.get('ExitCode')
        if type(exit_code) is not int:
            raise BootstrapError('container inspect ExitCode invalid')
        return state, exit_code
    except BootstrapError:
        raise
    except (subprocess.SubprocessError, OSError):
        raise BootstrapError('container inspect error') from None


def remove_and_verify_container(cid, runner=subprocess.run):
    try:
        rm_res = runner(['docker', 'rm', cid], capture_output=True, timeout=10, check=False,
                        env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if rm_res.returncode != 0:
            runner(['docker', 'rm', '-f', cid], capture_output=True, timeout=10, check=False,
                   env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        insp_res = runner(['docker', 'container', 'inspect', cid], capture_output=True,
                           timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if insp_res.returncode == 0:
            return False
        err = insp_res.stderr.decode().lower()
        if 'no such container' in err:
            return True
        return False
    except (subprocess.SubprocessError, OSError):
        return False


def cleanup_containers(tracker, runner=subprocess.run):
    uncleaned = []
    for item in list(tracker):
        try:
            cid = resolve_helper(item, runner=runner)
            cleaned = cid is None or remove_and_verify_container(cid, runner=runner)
            if cleaned:
                discard_intent(item)
        except (BootstrapError, OSError, subprocess.SubprocessError):
            cid = item.get('name', 'unresolved')
            cleaned = False
        if cleaned:
            if item in tracker:
                tracker.remove(item)
        else:
            uncleaned.append(cid)
    return uncleaned


def resolve_helper(item, runner=subprocess.run):
    """Resolve only an exact intent name; daemon errors are not absence."""
    res = runner(['docker', 'container', 'inspect', item.get('id') or item['name']],
                 capture_output=True, timeout=10, check=False,
                 env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    if res.returncode:
        if 'no such container' in res.stderr.decode().lower():
            return None
        raise BootstrapError('helper receipt unavailable; incomplete cleanup')
    rows = strict_json(res.stdout.decode())
    if not isinstance(rows, list) or len(rows) != 1:
        raise BootstrapError('helper receipt invalid')
    rec = rows[0]
    cid = rec.get('Id', '')
    config = rec.get('Config') or {}
    mounts = rec.get('HostConfig', {}).get('Mounts', [])
    if not isinstance(mounts, list) or any(not isinstance(m, dict) for m in mounts):
        raise BootstrapError('helper mount receipt invalid')
    mounts = [{key: m.get(key, False if key == 'ReadOnly' else None)
                for key in ('Type', 'Source', 'Target', 'ReadOnly')} for m in mounts]
    if 'tmpfs' in item:
        actual = rec.get('Mounts', [])
        expected = {m['Target']: (m['Type'], m['Source'], m['ReadOnly']) for m in item['mounts']}
        # Docker reports tmpfs in HostConfig.Tmpfs; some versions also include
        # it in Mounts. All physical bind/volume mounts must be intentional.
        physical = {m.get('Destination'): (m.get('Type'), m.get('Name') if m.get('Type') == 'volume' else m.get('Source'),
                    not m.get('RW')) for m in actual if m.get('Type') != 'tmpfs'}
        if (rec.get('HostConfig', {}).get('Tmpfs') != item['tmpfs'] or physical != expected
                or len(actual) != len({m.get('Destination') for m in actual})
                or any(m.get('Type') == 'tmpfs' and m.get('Destination') not in item['tmpfs'] for m in actual)):
            raise BootstrapError('helper anonymous volume or tmpfs receipt mismatch; incomplete cleanup')
    if (not SAFE_HEX_64_PATTERN.fullmatch(cid) or
            rec.get('Name') != '/' + item['name'] or
            rec.get('Image') != item['image'] or
            config.get('Labels', {}).get('bootstrap.nonce') != item['nonce'] or
            config.get('User') != item['user'] or
            mounts != item['mounts'] or
            (item.get('id') and item['id'] != cid)):
        raise BootstrapError('helper ownership mismatch; incomplete cleanup')
    item['id'] = cid
    return cid


def allocate_helper(cmd, tracker, kind, runner=subprocess.run):
    """Record intent before create; keep it even when the reply is lost."""
    nonce = secrets.token_hex(32)
    mount = cmd[cmd.index('--mount') + 1]
    fields = dict(part.split('=', 1) for part in mount.split(',') if '=' in part)
    item = {'name': cmd[cmd.index('--name') + 1], 'nonce': nonce,
            'image': cmd[cmd.index('--entrypoint') + 2], 'kind': kind,
            'user': cmd[cmd.index('--user') + 1],
            'mounts': [{'Type': fields['type'], 'Source': fields['src'],
                         'Target': fields['dst'], 'ReadOnly': 'readonly' in mount.split(',')}]}
    uid, gid = item['user'].split(':')
    item['tmpfs'] = {path: f'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid={uid},gid={gid}'
                     for path in ('/mosquitto/data', '/mosquitto/log')}
    # Mask every image VOLUME at the common allocation boundary, including init.
    # Persist the expected masks before create; never delete anonymous volumes.
    for path, options in item['tmpfs'].items():
        cmd[2:2] = ['--tmpfs', path + ':' + options]
    cancellation_checkpoint()
    item['receipt'] = save_intent(item)
    tracker.append(item)
    cmd[2:2] = ['--label', 'bootstrap.nonce=' + nonce]
    res = runner(cmd, capture_output=True, timeout=10, check=False,
                 env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    if res.returncode:
        raise BootstrapError('helper creation failed; recovery required')
    cid = res.stdout.decode().strip()
    if not SAFE_HEX_64_PATTERN.fullmatch(cid):
        raise BootstrapError('invalid helper create reply; recovery required')
    item['id'] = cid
    if resolve_helper(item, runner=runner) != cid:
        raise BootstrapError('allocated helper absent')
    cancellation_checkpoint()
    return item, cid


def container_native_init(image_id, destination, username, password, tracker, runner=subprocess.run):
    c_nonce = secrets.token_hex(8)
    c_name = f'mqtt-boot-init-{c_nonce}'
    work_dir = destination.parent
    file_name = destination.name

    create_cmd = ['docker', 'create', '--pull=never', '-i', '--name', c_name, '--network', 'none', '--log-driver', 'none',
                  '--user', f'{os.getuid()}:{os.getgid()}',
                  '--mount', f'type=bind,src={work_dir},dst=/work',
                  '--entrypoint', 'mosquitto_ctrl', image_id,
                  'dynsec', 'init', f'/work/{file_name}', username]
    tracker_item, cid = allocate_helper(create_cmd, tracker, 'native-init', runner=runner)

    start_cmd = ['docker', 'start', '-a', '-i', cid]
    res_start = runner(start_cmd, input=((password + '\n') * 2).encode(),
                       capture_output=True, timeout=15, check=False,
                       env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})

    state, exit_code = inspect_container_state(cid, runner=runner)

    if cleanup_containers(tracker, runner=runner):
        raise BootstrapError('init container cleanup could not verify absence')
    if tracker_item in tracker:
        tracker.remove(tracker_item)

    if res_start.returncode != 0 or exit_code != 0:
        raise BootstrapError('container native init start failed')


def check_config_file_security(config_path):
    path_str = str(config_path)
    if os.path.islink(path_str):
        raise BootstrapError('configuration file cannot be a symlink')
    try:
        st = os.lstat(path_str)
    except OSError:
        raise BootstrapError('cannot stat configuration file') from None
    if not stat.S_ISREG(st.st_mode):
        raise BootstrapError('configuration must be a regular file')
    if (st.st_mode & 0o777) != 0o600:
        raise BootstrapError('configuration file permissions must be exactly 0600')
    if st.st_uid != os.geteuid():
        raise BootstrapError('configuration file must be owned by current user')

    parent = os.path.dirname(os.path.abspath(path_str))
    try:
        pst = os.stat(parent)
    except OSError:
        raise BootstrapError('cannot stat configuration parent directory') from None
    sticky = bool(pst.st_mode & stat.S_ISVTX)
    if pst.st_uid not in (0, os.geteuid()):
        raise BootstrapError('configuration parent directory must be owned by root or current user')
    if (pst.st_mode & 0o002) and not (pst.st_uid == 0 and sticky):
        raise BootstrapError('configuration parent directory cannot be world-writable without sticky bit')


def bootstrap_container(cfg, image_ref=PINNED_IMAGE, runner=subprocess.run):
    global _cleaning
    _cleaning = False
    verified_image_id = verify_image(image_ref, runner=runner)

    volume_name = cfg.get('volume_name') or f'mosquitto_dynsec_{secrets.token_hex(12)}'
    check_volume_absent(volume_name, runner=runner)

    boot_nonce = secrets.token_hex(16)
    labels = {
        'com.docker.compose.volume': 'mosquitto_dynsec',
        'bootstrap.managed': 'true',
        'bootstrap.nonce': boot_nonce
    }
    if 'labels' in cfg:
        labels.update(cfg['labels'])

    create_vol_cmd = ['docker', 'volume', 'create']
    for k, v in labels.items():
        create_vol_cmd.extend(['--label', f'{k}={v}'])
    create_vol_cmd.append(volume_name)

    container_tracker = []
    volume_retained = False
    staging_cleaned = False
    staging_path = None
    volume_intent = {'kind': 'volume', 'name': volume_name, 'labels': labels,
                     'image': verified_image_id}
    cancellation_checkpoint()
    volume_intent['receipt'] = save_intent(volume_intent)

    try:
        res_vol = runner(create_vol_cmd, capture_output=True, timeout=10, check=False,
                         env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        if res_vol.returncode != 0 or res_vol.stdout.decode().strip() != volume_name:
            raise BootstrapError('volume creation failed')
        verify_volume_receipt(volume_name, labels, runner=runner)
        cancellation_checkpoint()
        # Step 1: Generate native hashes in protected host temporary directory
        staging_dir_parent = '/tmp/opencode' if os.path.exists('/tmp/opencode') else None
        old_umask = os.umask(0o077)
        try:
            # No automatic finalizer may unlink inputs still mounted by helpers.
            staging_path = tempfile.mkdtemp(dir=staging_dir_parent, prefix='mqtt-bootstrap-')
            os.chmod(staging_path, 0o700)
            sources = []
            for name in ('manager', 'backend'):
                path = Path(staging_path) / (name + '.json')
                container_native_init(verified_image_id, path, cfg[name], cfg[name + '_password'],
                                      container_tracker, runner=runner)
                with path.open() as stream:
                    source = strict_json(stream.read(65537))
                if not isinstance(source, dict) or not isinstance(source.get('clients'), list) or len(source['clients']) != 1 or not isinstance(source['clients'][0], dict):
                    raise BootstrapError('unexpected native client schema')
                client = source['clients'][0]
                if client.get('username') != cfg[name]:
                    raise BootstrapError('native principal mismatch')
                try:
                    if (len(base64.b64decode(client['password'], validate=True)) != 64 or
                            len(base64.b64decode(client['salt'], validate=True)) != 12 or
                            type(client['iterations']) is not int or client['iterations'] <= 0):
                        raise ValueError()
                except (KeyError, ValueError, TypeError):
                    raise BootstrapError('unexpected native hash schema') from None
                sources.append(source)

            composed_dict = compose_store(*sources)
            composed_bytes = json.dumps(composed_dict, separators=(',', ':')).encode()
        finally:
            os.umask(old_umask)

        # Ensure container tracker is empty before staging directory cleanup
        if container_tracker:
            raise BootstrapError('active init containers remain before staging cleanup')
        shutil.rmtree(staging_path)
        staging_cleaned = True

        # Re-verify volume receipt before Step 2
        verify_volume_receipt(volume_name, labels, runner=runner)

        # Step 2: Root helper container to verify empty dir and chown 1883:1883 / chmod 0700
        c_root_nonce = secrets.token_hex(8)
        c_root_name = f'mqtt-boot-root-{c_root_nonce}'
        root_script = (
            'set -e\n'
            'entries=$(ls -A /mosquitto/dynsec)\n'
            'if [ -n "$entries" ]; then\n'
            '  echo "non-empty directory; foreign replacement rejected" >&2\n'
            '  exit 1\n'
            'fi\n'
            'mkdir -p /mosquitto/dynsec\n'
            'chown 1883:1883 /mosquitto/dynsec\n'
            'chmod 0700 /mosquitto/dynsec\n'
        )
        cmd_root = ['docker', 'create', '--pull=never', '--user', '0:0', '--name', c_root_name,
                    '--network', 'none', '--log-driver', 'none',
                    '--mount', f'type=volume,src={volume_name},dst=/mosquitto/dynsec',
                    '--entrypoint', '/bin/sh', verified_image_id,
                    '-c', root_script]
        item_root, c_root_id = allocate_helper(cmd_root, container_tracker, 'ownership', runner=runner)

        res_sroot = runner(['docker', 'start', '-a', c_root_id], capture_output=True, timeout=15, check=False,
                           env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        state_root, exit_root = inspect_container_state(c_root_id, runner=runner)
        if cleanup_containers(container_tracker, runner=runner):
            raise BootstrapError('root helper container cleanup could not verify absence')
        if item_root in container_tracker:
            container_tracker.remove(item_root)
        if res_sroot.returncode != 0 or exit_root != 0:
            raise BootstrapError('root ownership helper execution failed')

        # Re-verify volume receipt before Step 3
        verify_volume_receipt(volume_name, labels, runner=runner)

        # Step 3: Writer container (1883:1883) publishes file via stdin, temp link, chmod 0600
        c_writer_nonce = secrets.token_hex(8)
        c_writer_name = f'mqtt-boot-writer-{c_writer_nonce}'
        writer_script = (
            'set -e\n'
            'umask 077\n'
            'dir_meta=$(stat -c "%u:%g:%a" /mosquitto/dynsec)\n'
            'if [ "$dir_meta" != "1883:1883:700" ]; then exit 1; fi\n'
            'if [ -e /mosquitto/dynsec/dynamic-security.json ]; then exit 2; fi\n'
            'tmp="/mosquitto/dynsec/.bootstrap-$1"\n'
            'set -C\n'
            'cat > "$tmp"\n'
            'chmod 0600 "$tmp"\n'
            'ln "$tmp" /mosquitto/dynsec/dynamic-security.json\n'
            'rm -f "$tmp"\n'
            'file_meta=$(stat -c "%u:%g:%a" /mosquitto/dynsec/dynamic-security.json)\n'
            'if [ "$file_meta" != "1883:1883:600" ]; then exit 3; fi\n'
        )
        cmd_writer = ['docker', 'create', '--pull=never', '-i', '--name', c_writer_name,
                      '--network', 'none', '--log-driver', 'none',
                      '--user', '1883:1883',
                      '--mount', f'type=volume,src={volume_name},dst=/mosquitto/dynsec',
                      '--entrypoint', '/bin/sh', verified_image_id,
                      '-c', writer_script, 'sh', c_writer_nonce]
        item_writer, c_writer_id = allocate_helper(cmd_writer, container_tracker, 'writer', runner=runner)

        res_swriter = runner(['docker', 'start', '-a', '-i', c_writer_id], input=composed_bytes,
                             capture_output=True, timeout=15, check=False,
                             env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        state_writer, exit_writer = inspect_container_state(c_writer_id, runner=runner)
        if cleanup_containers(container_tracker, runner=runner):
            raise BootstrapError('writer container cleanup could not verify absence')
        if item_writer in container_tracker:
            container_tracker.remove(item_writer)
        if res_swriter.returncode != 0 or exit_writer != 0:
            raise BootstrapError('writer container execution failed; no overwrite or improper permissions')

        # Re-verify volume receipt before Step 4
        verify_volume_receipt(volume_name, labels, runner=runner)

        # Step 4: Verification reader container (1883:1883) readonly mount checks exact schema
        c_reader_nonce = secrets.token_hex(8)
        c_reader_name = f'mqtt-boot-reader-{c_reader_nonce}'
        reader_script = (
            'set -e\n'
            'stat -c "%u:%g:%a" /mosquitto/dynsec /mosquitto/dynsec/dynamic-security.json\n'
            'cat /mosquitto/dynsec/dynamic-security.json\n'
        )
        cmd_reader = ['docker', 'create', '--pull=never', '--name', c_reader_name,
                      '--network', 'none', '--log-driver', 'none',
                      '--user', '1883:1883',
                      '--mount', f'type=volume,src={volume_name},dst=/mosquitto/dynsec,readonly',
                      '--entrypoint', '/bin/sh', verified_image_id,
                      '-c', reader_script]
        item_reader, c_reader_id = allocate_helper(cmd_reader, container_tracker, 'reader', runner=runner)

        res_sreader = runner(['docker', 'start', '-a', c_reader_id], capture_output=True,
                             timeout=15, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
        state_reader, exit_reader = inspect_container_state(c_reader_id, runner=runner)
        if cleanup_containers(container_tracker, runner=runner):
            raise BootstrapError('reader container cleanup could not verify absence')
        if item_reader in container_tracker:
            container_tracker.remove(item_reader)
        if res_sreader.returncode != 0 or exit_reader != 0:
            raise BootstrapError('reader verification failed')

        lines = res_sreader.stdout.splitlines()
        if len(lines) < 3:
            raise BootstrapError('incomplete reader output')
        dir_stat = lines[0].decode().strip()
        file_stat = lines[1].decode().strip()
        content = b'\n'.join(lines[2:]).strip()

        if dir_stat != '1883:1883:700' or file_stat != '1883:1883:600':
            raise BootstrapError('verified container permissions do not match 1883:1883 0700/0600')

        verified_store = strict_json(content.decode())
        if verified_store != composed_dict:
            raise BootstrapError('verified store content does not match composed store exactly')

        volume_retained = True
        cancellation_checkpoint()
        discard_intent(volume_intent)
        return volume_name
    finally:
        _cleaning = True
        uncleaned_cids = cleanup_containers(container_tracker, runner=runner)
        if uncleaned_cids:
            recovery_dir = staging_path if staging_path and not staging_cleaned else tempfile.mkdtemp(
                dir=_recovery_parent, prefix='mqtt-bootstrap-recovery-')
            receipt_path = Path(recovery_dir) / 'recovery.json'
            fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump({'helpers': container_tracker, 'volume': volume_name,
                           'labels': labels}, stream)
            raise BootstrapError(f'incomplete cleanup; protected recovery receipt: {receipt_path}')
        if staging_path and not staging_cleaned:
            shutil.rmtree(staging_path)
        if not volume_retained:
            # Safe teardown of failed/partial newly created volume (never use -f)
            try:
                rec = verify_volume_receipt(volume_name, labels, runner=runner)
                if rec.get('Name') == volume_name:
                    res = runner(['docker', 'volume', 'rm', volume_name], capture_output=True,
                                 timeout=10, check=False, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
                    if res.returncode:
                        raise BootstrapError('volume removal failed')
                    check_volume_absent(volume_name, runner=runner)
                    discard_intent(volume_intent)
            except Exception:
                recovery_dir = tempfile.mkdtemp(dir=_recovery_parent, prefix='mqtt-bootstrap-recovery-')
                receipt_path = Path(recovery_dir) / 'recovery.json'
                fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'w') as stream:
                    json.dump({'volume': volume_name, 'labels': labels}, stream)
                raise BootstrapError(f'incomplete volume cleanup; protected recovery receipt: {receipt_path}') from None
        _cleaning = False
        cancellation_checkpoint()


def main():
    global _recovery_parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', action='store_true',
                        help='run bootstrap in pinned Mosquitto container targeting a fresh named volume')
    parser.add_argument('--image', default=PINNED_IMAGE,
                        help=f'pinned Mosquitto Docker image reference (default: {PINNED_IMAGE})')
    parser.add_argument('--config', type=Path, default=None,
                        help='path to protected JSON configuration file (default: stdin)')
    parser.add_argument('--recovery-dir', type=Path, default=None,
                        help='existing private 0700 directory for allocation recovery receipts')
    parser.add_argument('--native-tool', default='/usr/bin/mosquitto_ctrl',
                        help='path to native mosquitto_ctrl (host mode only)')
    parser.add_argument('--check-native-tool', action='store_true',
                        help='verify dependency only (host binary or container image); no store written')
    args = parser.parse_args()

    if args.recovery_dir is not None:
        info = args.recovery_dir.lstat()
        if (not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700
                or info.st_uid != os.getuid()):
            raise BootstrapError('recovery directory must be owned and private 0700')
        _recovery_parent = str(args.recovery_dir.resolve())

    if args.config is not None:
        check_config_file_security(args.config)

    try:
        if args.container:
            if args.check_native_tool:
                verify_image(args.image)
                print('container image dependency verified; no store written; NOT READY')
                return 0
            if args.config is not None:
                try:
                    stream = args.config.open('r')
                except OSError:
                    print('cannot open configuration file', file=sys.stderr)
                    return 1
            else:
                stream = sys.stdin
            try:
                cfg = parse_config(stream, container_mode=True)
            finally:
                if args.config is not None:
                    stream.close()
            vol_name = bootstrap_container(cfg, image_ref=args.image)
            output = {
                'status': 'store_created',
                'volume_name': vol_name,
                'runtime_status': 'NOT_READY',
                'gate': 'unchanged',
                'ownership': '1883:1883',
                'mode': 'container'
            }
            print(json.dumps(output, indent=2))
            return 0
        else:
            if args.check_native_tool:
                verify_native_tool(args.native_tool)
                print('native tool dependency verified; no store written; NOT READY')
                return 0
            if args.config is not None:
                try:
                    stream = args.config.open('r')
                except OSError:
                    print('cannot open configuration file', file=sys.stderr)
                    return 1
            else:
                stream = sys.stdin
            try:
                cfg = parse_config(stream, container_mode=False)
            finally:
                if args.config is not None:
                    stream.close()
            bootstrap(cfg, args.native_tool)
            print('fresh protected DynSec store published; gate unchanged; NOT READY')
            return 0
    except BootstrapError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, subprocess.SubprocessError):
        print('bootstrap refused/failed; store and ownership require offline verification', file=sys.stderr)
        return 1


if __name__ == '__main__':
    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        for signum in previous:
            signal.signal(signum, request_cancellation)
        sys.exit(main())
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
