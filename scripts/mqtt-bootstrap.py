#!/usr/bin/env python3
"""Offline fresh Mosquitto 2.0.18 DynSec store. Never opens the ingress gate.

Read one strict JSON object from protected stdin (not a shell command argument).
Run with a trusted, installed Mosquitto 2.0.18 mosquitto_ctrl. The containing
directory must already be empty, 0700 and owned by the mapped broker UID/GID.
Rootless uid_map/gid_map are operator-verified Docker mappings, not guesses.
No deployment discovery, legacy import, directory chown, or DB access occurs.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import uuid


class BootstrapError(Exception):
    """Safe error: never contains credential/input/native output."""


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


def parse_config(stream):
    text = stream.read(16385)
    if len(text) > 16384:
        raise BootstrapError('configuration too large')
    cfg = strict_json(text)
    required = {'store', 'manager', 'backend', 'manager_password', 'backend_password',
                'mapping', 'broker_uid', 'broker_gid'}
    if not isinstance(cfg, dict) or not required <= cfg.keys() or cfg.keys() - required - {'uid_map', 'gid_map'}:
        raise BootstrapError('configuration fields missing or unknown')
    for key in ('manager', 'backend'):
        if not isinstance(cfg[key], str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', cfg[key]):
            raise BootstrapError('invalid principal')
    if cfg['manager'] == cfg['backend']:
        raise BootstrapError('principals must differ')
    for key in ('manager_password', 'backend_password'):
        value = cfg[key]
        # Native get_password has a 200-byte buffer. Restrict to printable ASCII
        # so stdin framing and native truncation cannot change the secret.
        if not isinstance(value, str) or not 32 <= len(value) <= 128 or any(not 33 <= ord(c) <= 126 for c in value):
            raise BootstrapError('invalid credential; use independently generated random secrets')
    if cfg['manager_password'] == cfg['backend_password']:
        raise BootstrapError('credentials must differ')
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
            # Root-owned sticky shared temp is safe with a private child.
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
    # Private temporary native files; never export a hash through stdout.
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
                # Shape only, NOT a homemade password verifier/hash generator.
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
        # Hard-link publication is atomic and unlike rename cannot overwrite.
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
    """Check the installed binary without reading credentials or touching a store."""
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
    if b'mosquitto_ctrl version 2.0.18 running on libmosquitto 2.0.18.' not in version.stdout:
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-tool', default='/usr/bin/mosquitto_ctrl')
    parser.add_argument('--check-native-tool', action='store_true',
                        help='verify installed dependency only; no stdin or store access')
    args = parser.parse_args()
    try:
        if args.check_native_tool:
            verify_native_tool(args.native_tool)
            print('native tool dependency verified; no store written; NOT READY')
            return 0
        bootstrap(parse_config(sys.stdin), args.native_tool)
    except BootstrapError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, subprocess.SubprocessError):
        print('bootstrap refused/failed; store and ownership require offline verification', file=sys.stderr)
        return 1
    print('fresh protected DynSec store published; gate unchanged; NOT READY')
    return 0


if __name__ == '__main__':
    sys.exit(main())
