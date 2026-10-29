"""Narrow, fixture-owned PostgreSQL dataset backup/restore primitives for P8.13.

This is a database-only restore: PostgreSQL roles plus Auth, broker, keys and
socket dependencies are retained separately by the enclosing disposable fixture.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import tempfile
import time


MAX_DUMP_BYTES = 64 * 1024 * 1024
DUMP_TIMEOUT_SECONDS = 120
RESTORE_TIMEOUT_SECONDS = 180
SOURCE_DATABASE = 'smoke'
SNAPSHOT_TABLES = (
    'user_gateways', 'gateways', 'sensors', 'twin_entities', 'twin_relationships',
    'twin_states', 'twin_commands', 'twin_outbox', 'twin_command_requests', 'processed_messages',
    'telemetry', 'twin_temporal_values',
)


def _validate_owned_container(registry, item):
    if (not isinstance(item, dict) or item.get('kind') != 'container'
            or not item.get('receipt') or not item.get('receipt', {}).get('id')):
        raise ValueError('recovery_container_not_owned')
    current = registry.resolve(item)
    if current is None or item.get('receipt') is None:
        raise ValueError('recovery_container_not_owned')
    return item['receipt']['id']


def _private_root(fixture):
    root = Path(fixture.private)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('recovery_private_directory_invalid')
    st = root.stat(follow_symlinks=False)
    if st.st_uid != os.getuid() or st.st_mode & 0o777 != 0o700:
        raise ValueError('recovery_private_directory_invalid')
    return root.resolve(strict=True)


def _archive_path(fixture, path):
    root = _private_root(fixture)
    candidate = Path(path)
    if candidate.is_symlink() or candidate.parent.resolve(strict=True) != root:
        raise ValueError('recovery_archive_path_invalid')
    return candidate


def _target_database(run_id):
    if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', run_id):
        raise ValueError('recovery_run_id_invalid')
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    name = 'p8_restore_' + suffix
    if not re.fullmatch(r'[a-z][a-z0-9_]{1,62}', name):
        raise ValueError('recovery_target_name_invalid')
    return name


def _psql(registry, container_id, database, sql):
    if database != SOURCE_DATABASE and not re.fullmatch(r'p8_restore_[a-f0-9]{20}', database):
        raise ValueError('recovery_database_name_invalid')
    result = registry.command(['exec', '-i', container_id, 'psql', '-U', 'postgres',
                               '-d', database, '-v', 'ON_ERROR_STOP=1', '-At', '-c', sql])
    if result.returncode:
        raise ValueError('recovery_psql_failed')
    return result.stdout.decode(errors='strict').strip()


def _psql_variables(registry, container_id, database, sql, variables):
    """Run psql with validated variables in an explicitly selected fixture DB."""
    if database != SOURCE_DATABASE and not re.fullmatch(r'p8_restore_[a-f0-9]{20}', database):
        raise ValueError('recovery_database_name_invalid')
    args = ['exec', '-i', container_id, 'psql', '-U', 'postgres', '-d', database,
            '-v', 'ON_ERROR_STOP=1', '-At']
    for key, value in (variables or {}).items():
        if (not re.fullmatch(r'[a-z_]+', key) or not isinstance(value, str)
                or len(value) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in value)):
            raise ValueError('recovery_psql_variable_invalid')
        args += ['-v', key + '=' + value]
    result = registry.command(args + ['-f', '-'], input=sql.encode())
    if result.returncode:
        raise ValueError('recovery_psql_failed')
    return result.stdout.decode(errors='strict').strip()


def _write_bounded_dump(args, archive, *, timeout, popen=subprocess.Popen, diagnostic_path=None):
    """Stream child stdout to an already-exclusive file; never buffer the archive."""
    if diagnostic_path is None:
        diagnostic = tempfile.TemporaryFile(mode='w+b')
    else:
        diagnostic_fd = os.open(diagnostic_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                getattr(os, 'O_NOFOLLOW', 0), 0o600)
        diagnostic = os.fdopen(diagnostic_fd, 'wb')
    try:
        proc = popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=diagnostic,
                     close_fds=True)
    except BaseException:
        diagnostic.close()
        raise
    deadline = time.monotonic() + timeout
    size = 0
    digest = hashlib.sha256()
    selector = None
    completed = False
    try:
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                proc.wait()
                raise ValueError('recovery_dump_timeout')
            events = selector.select(min(remaining, 1.0))
            if not events:
                if proc.poll() is not None:
                    break
                continue
            chunk = os.read(proc.stdout.fileno(), min(1024 * 1024, MAX_DUMP_BYTES + 1 - size))
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_DUMP_BYTES:
                proc.kill()
                proc.wait()
                raise ValueError('recovery_dump_too_large')
            archive.write(chunk)
            digest.update(chunk)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            proc.kill(); proc.wait()
            raise ValueError('recovery_dump_timeout')
        try:
            code = proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait()
            raise ValueError('recovery_dump_timeout') from None
        if code:
            raise ValueError('recovery_dump_failed')
        completed = True
    finally:
        if selector is not None:
            selector.close()
        if proc.stdout:
            proc.stdout.close()
        if not completed and proc.poll() is None:
            proc.kill()
            proc.wait()
        diagnostic.close()
    if size == 0:
        raise ValueError('recovery_dump_empty')
    return {'size_bytes': size, 'sha256': digest.hexdigest()}


def backup_database(registry, fixture, source_item, archive_path, *, timeout=DUMP_TIMEOUT_SECONDS,
                    popen=subprocess.Popen):
    """Dump only the owned fixture's `smoke` DB into a private exclusive archive."""
    container_id = _validate_owned_container(registry, source_item)
    path = _archive_path(fixture, archive_path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        if os.fstat(fd).st_uid != os.getuid():
            raise ValueError('recovery_archive_owner_invalid')
        with os.fdopen(fd, 'wb') as archive:
            fd = -1
            result = _write_bounded_dump(
                ['docker', 'exec', container_id, 'pg_dump', '--format=custom', '--no-password',
                 '-U', 'postgres', '--dbname=' + SOURCE_DATABASE], archive, timeout=timeout, popen=popen,
                diagnostic_path=path.with_name(path.name + '.pg_dump.stderr'))
            archive.flush(); os.fsync(archive.fileno())
        return {**result, 'status': 'backed_up'}
    finally:
        if fd >= 0:
            os.close(fd)


def snapshot_database(registry, source_item, database=SOURCE_DATABASE):
    """Return counts and content digests only; no row values or credentials escape."""
    container_id = _validate_owned_container(registry, source_item)
    if database != SOURCE_DATABASE and not re.fullmatch(r'p8_restore_[a-f0-9]{20}', database):
        raise ValueError('recovery_database_name_invalid')
    rows = {}
    for table in SNAPSHOT_TABLES:
        # Table identifiers are the module's fixed allowlist; data is only hashed in SQL.
        value = _psql(registry, container_id, database,
                      "SELECT json_build_object('count',count(*),'hash',"
                      "md5(coalesce(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),''))) "
                      f'FROM public.{table} t;')
        try:
            row = json.loads(value)
        except (ValueError, TypeError):
            raise ValueError('recovery_snapshot_invalid') from None
        if (not isinstance(row, dict) or set(row) != {'count', 'hash'}
                or isinstance(row.get('count'), bool) or not isinstance(row.get('count'), int)
                or row['count'] < 0 or not isinstance(row.get('hash'), str)
                or not re.fullmatch(r'[0-9a-f]{32}', row['hash'])):
            raise ValueError('recovery_snapshot_invalid')
        rows[table] = row
    return {'database': database, 'tables': rows}


def assert_snapshot_equal(source, restored):
    """Compare private count/hash snapshots without returning any row content."""
    def valid(snapshot):
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get('tables'), dict):
            return False
        tables = snapshot['tables']
        required_nonempty = {'user_gateways', 'gateways', 'sensors', 'twin_entities',
                             'twin_states', 'twin_commands', 'twin_command_requests',
                             'telemetry', 'twin_temporal_values'}
        return (set(tables) == set(SNAPSHOT_TABLES) and all(
            isinstance(row, dict) and set(row) == {'count', 'hash'}
            and isinstance(row.get('count'), int) and not isinstance(row.get('count'), bool)
            and row['count'] >= 0 and isinstance(row.get('hash'), str)
            and re.fullmatch(r'[0-9a-f]{32}', row['hash']) for row in tables.values())
            and all(tables[name]['count'] > 0 for name in required_nonempty))
    if (not valid(source) or not valid(restored)
            or source['tables'] != restored['tables']):
        raise ValueError('recovery_snapshot_mismatch')
    return True


def restore_database(registry, fixture, source_item, archive_path, run_id, *, timeout=RESTORE_TIMEOUT_SECONDS,
                     popen=subprocess.Popen):
    """Restore a verified fixture dump to a new DB in the same owned cluster."""
    container_id = _validate_owned_container(registry, source_item)
    path = _archive_path(fixture, archive_path)
    target = _target_database(run_id)
    if target == SOURCE_DATABASE:
        raise ValueError('recovery_source_target_collision')
    if _psql(registry, container_id, SOURCE_DATABASE,
             "SELECT current_database() = 'smoke';") != 't':
        raise ValueError('recovery_source_database_mismatch')
    exists = _psql(registry, container_id, SOURCE_DATABASE,
                   "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='" + target + "');")
    if exists != 'f':
        raise ValueError('recovery_target_collision')
    actual = _psql(registry, container_id, SOURCE_DATABASE,
                   "SELECT current_database();")
    if actual != SOURCE_DATABASE:
        raise ValueError('recovery_source_database_mismatch')
    _psql(registry, container_id, SOURCE_DATABASE,
          'CREATE DATABASE ' + target + ' TEMPLATE template0 OWNER postgres;')
    _psql(registry, container_id, target,
          'CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();')
    try:
        archive_fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    except OSError:
        raise ValueError('recovery_archive_invalid') from None
    st = os.fstat(archive_fd)
    if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077
            or not 0 < st.st_size <= MAX_DUMP_BYTES):
        os.close(archive_fd)
        raise ValueError('recovery_archive_invalid')
    with os.fdopen(archive_fd, 'rb') as archive:
        diagnostic_path = path.with_name(path.name + '.pg_restore.stderr')
        diagnostic_fd = os.open(diagnostic_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                getattr(os, 'O_NOFOLLOW', 0), 0o600)
        diagnostic = os.fdopen(diagnostic_fd, 'wb')
        proc = popen(['docker', 'exec', '-i', container_id, 'pg_restore', '--exit-on-error',
                      '-U', 'postgres', '--dbname=' + target, '--no-password'], stdin=subprocess.PIPE,
                     stdout=subprocess.DEVNULL, stderr=diagnostic, close_fds=True)
        try:
            dump = archive.read(MAX_DUMP_BYTES + 1)
            if len(dump) != st.st_size:
                proc.kill(); proc.communicate()
                raise ValueError('recovery_archive_changed')
            _, _ = proc.communicate(input=dump, timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.communicate()
            raise ValueError('recovery_restore_timeout') from None
        except BaseException:
            if proc.poll() is None:
                proc.kill()
            proc.communicate()
            raise
        finally:
            diagnostic.close()
        if proc.returncode:
            raise ValueError('recovery_restore_failed')
    _psql(registry, container_id, target, 'SELECT timescaledb_post_restore();')
    if _psql(registry, container_id, target, 'SELECT current_database();') != target:
        raise ValueError('recovery_target_database_mismatch')
    schema = _psql(registry, container_id, target,
                   'SELECT max(version)::text FROM public.schema_migrations;')
    extension = _psql(registry, container_id, target,
                      "SELECT extversion FROM pg_extension WHERE extname='timescaledb';")
    if schema != '24' or not extension:
        raise ValueError('recovery_schema_or_extension_invalid')
    return {'status': 'restored', 'target_database': target,
            'schema_version': int(schema), 'timescaledb_ready': True}
