"""Small, read-only PostgreSQL snapshot for an isolated E2 fixture."""
import json
import math
import subprocess
import time
from datetime import datetime, timezone


MAX_OUTPUT_BYTES = 65536
MAX_ACTIVITY = 32
MAX_BLOCKERS = 16
_COUNTERS = {
    'database': ('numbackends', 'xact_commit', 'xact_rollback', 'blks_read', 'blks_hit',
                 'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted',
                 'conflicts', 'deadlocks', 'temp_files', 'temp_bytes', 'blk_read_time',
                 'blk_write_time'),
    'wal': ('wal_records', 'wal_fpi', 'wal_bytes', 'wal_buffers_full', 'wal_write', 'wal_sync',
            'wal_write_time', 'wal_sync_time'),
    'io': ('reads', 'read_time', 'writes', 'write_time', 'writebacks', 'writeback_time',
           'extends', 'extend_time', 'hits', 'evictions', 'reuses', 'fsyncs', 'fsync_time'),
}
_TEXT_LIMITS = {'state': 32, 'backend_type': 64, 'wait_event_type': 64, 'wait_event': 64,
                'object': 32, 'context': 32, 'reason': 40}


class _CommandOnlyRegistry:
    """Add psql quiet mode and keep db_psql from persisting raw stderr."""
    def __init__(self, registry):
        self._command = registry.command

    def command(self, args, **kwargs):
        args = list(args)
        if 'psql' in args and '-q' not in args:
            args.insert(args.index('-At') + 1, '-q')
        return self._command(args, **kwargs)


def _numeric(value, *, nullable=False):
    if value is None and nullable:
        return None
    if type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 1e100:
        raise ValueError('diagnostics_payload_rejected')
    return value


def _text(value, limit):
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ValueError('diagnostics_payload_rejected')
    return value


def _timestamp(value):
    return _text(value, 64)


def _activity(rows):
    if not isinstance(rows, list) or len(rows) > 256:
        raise ValueError('diagnostics_payload_rejected')
    result = []
    for row in rows[:MAX_ACTIVITY]:
        if not isinstance(row, dict):
            raise ValueError('diagnostics_payload_rejected')
        pid = row.get('pid')
        if type(pid) is not int or pid < 0:
            raise ValueError('diagnostics_payload_rejected')
        blockers = row.get('blocking_pids')
        if not isinstance(blockers, list) or len(blockers) > 256:
            raise ValueError('diagnostics_payload_rejected')
        if any(type(p) is not int or p < 0 for p in blockers):
            raise ValueError('diagnostics_payload_rejected')
        result.append({'pid': pid, 'blocking_pids': [_numeric(p) for p in blockers[:MAX_BLOCKERS]],
                       'state': _text(row.get('state'), 32),
                       'backend_type': _text(row.get('backend_type'), 64),
                       'wait_event_type': _text(row.get('wait_event_type'), 64),
                       'wait_event': _text(row.get('wait_event'), 64),
                       'xact_age_seconds': _numeric(row.get('xact_age_seconds'), nullable=True),
                       'query_age_seconds': _numeric(row.get('query_age_seconds'), nullable=True)})
    return result


def _stats(raw, section, *, rows=False):
    if not isinstance(raw, dict) or type(raw.get('available')) is not bool:
        raise ValueError('diagnostics_payload_rejected')
    scope = _text(raw.get('scope'), 16)
    if scope not in ('cluster', 'database'):
        raise ValueError('diagnostics_payload_rejected')
    if raw['available'] is False:
        reason = _text(raw.get('reason'), 40)
        if reason != 'view_unsupported':
            raise ValueError('diagnostics_payload_rejected')
        return {'available': False, 'reason': reason, 'scope': scope}
    data = raw.get('rows' if rows else 'stats')
    if rows:
        if not isinstance(data, list) or len(data) > 64:
            raise ValueError('diagnostics_payload_rejected')
        clean = []
        for row in data:
            if not isinstance(row, dict):
                raise ValueError('diagnostics_payload_rejected')
            clean.append({key: (_text(row.get(key), _TEXT_LIMITS[key]) if key in _TEXT_LIMITS
                                else None if key.endswith('_time') and not raw.get('track_io_timing')
                                else _numeric(row.get(key), nullable=True))
                          for key in ('backend_type', 'object', 'context') + _COUNTERS['io']})
    else:
        if not isinstance(data, dict):
            raise ValueError('diagnostics_payload_rejected')
        timing_enabled = raw.get('track_wal_io_timing') if section == 'wal' else True
        clean = {key: (None if key.endswith('_time') and not timing_enabled else
                       _numeric(data.get(key), nullable=(key.endswith('_time'))))
                 for key in _COUNTERS[section]}
    result = {'available': True, 'scope': scope, 'stats' if not rows else 'rows': clean}
    if section in ('wal', 'io'):
        result['stats_reset'] = _timestamp(raw.get('stats_reset'))
    return result


_SQL = r"""BEGIN READ ONLY;
SET LOCAL statement_timeout = '1500ms';
SET LOCAL lock_timeout = '500ms';
SELECT json_build_object(
 'server_version_num', current_setting('server_version_num')::integer,
 'postmaster_start_time', (SELECT to_char(pg_postmaster_start_time() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
 'stats_reset', (SELECT to_char(stats_reset AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') FROM pg_stat_database WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())),
 'settings', json_build_object('track_activities', current_setting('track_activities')::boolean,
   'track_counts', current_setting('track_counts')::boolean,
   'track_io_timing', current_setting('track_io_timing')::boolean,
   'track_wal_io_timing', current_setting('track_wal_io_timing')::boolean),
 'activity', coalesce((SELECT json_agg(a) FROM (SELECT pid, (pg_blocking_pids(pid))[1:16] AS blocking_pids,
   state, backend_type, wait_event_type, wait_event,
   extract(epoch FROM clock_timestamp()-xact_start) AS xact_age_seconds,
   extract(epoch FROM clock_timestamp()-query_start) AS query_age_seconds
   FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND pid<>pg_backend_pid() ORDER BY pid LIMIT 32) a), '[]'::json),
 'database', (SELECT json_build_object('numbackends', numbackends, 'xact_commit', xact_commit,
   'xact_rollback', xact_rollback, 'blks_read', blks_read, 'blks_hit', blks_hit,
   'tup_returned', tup_returned, 'tup_fetched', tup_fetched, 'tup_inserted', tup_inserted,
   'tup_updated', tup_updated, 'tup_deleted', tup_deleted, 'conflicts', conflicts,
   'deadlocks', deadlocks, 'temp_files', temp_files, 'temp_bytes', temp_bytes,
   'blk_read_time', CASE WHEN current_setting('track_io_timing')::boolean THEN blk_read_time END,
   'blk_write_time', CASE WHEN current_setting('track_io_timing')::boolean THEN blk_write_time END)
   FROM pg_stat_database WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())),
 'wal', (SELECT CASE WHEN to_regclass('pg_stat_wal') IS NULL THEN
   json_build_object('available',false,'reason','view_unsupported','scope','cluster') ELSE
   json_build_object('available',true,'scope','cluster','stats',(SELECT json_build_object(
    'wal_records',wal_records,'wal_fpi',wal_fpi,'wal_bytes',wal_bytes,'wal_buffers_full',wal_buffers_full,
    'wal_write',wal_write,'wal_sync',wal_sync,
    'wal_write_time',CASE WHEN current_setting('track_wal_io_timing')::boolean THEN wal_write_time END,
    'wal_sync_time',CASE WHEN current_setting('track_wal_io_timing')::boolean THEN wal_sync_time END) FROM pg_stat_wal),
    'stats_reset',(SELECT to_char(stats_reset AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') FROM pg_stat_wal)) END),
 'io', (SELECT CASE WHEN to_regclass('pg_stat_io') IS NULL THEN
   json_build_object('available',false,'reason','view_unsupported','scope','cluster') ELSE
   json_build_object('available',true,'scope','cluster','rows',(SELECT json_agg(r) FROM
    (SELECT backend_type, object, context, sum(reads) reads,
     sum(CASE WHEN current_setting('track_io_timing')::boolean THEN read_time END) read_time,
     sum(writes) writes,
     sum(CASE WHEN current_setting('track_io_timing')::boolean THEN write_time END) write_time,
     sum(writebacks) writebacks,
     sum(CASE WHEN current_setting('track_io_timing')::boolean THEN writeback_time END) writeback_time,
     sum(extends) extends,
     sum(CASE WHEN current_setting('track_io_timing')::boolean THEN extend_time END) extend_time,
     sum(hits) hits, sum(evictions) evictions, sum(reuses) reuses, sum(fsyncs) fsyncs,
     sum(CASE WHEN current_setting('track_io_timing')::boolean THEN fsync_time END) fsync_time
     FROM pg_stat_io GROUP BY backend_type,object,context
     ORDER BY backend_type,object,context LIMIT 64) r),
      'stats_reset',(SELECT to_char(max(stats_reset) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') FROM pg_stat_io)) END));
COMMIT;"""


def read_postgres_diagnostics(smoke, registry, postgres_cid, *, timeout=2.0):
    """Read bounded diagnostics from the isolated fixture's pinned PostgreSQL 16 image only."""
    if (not isinstance(postgres_cid, str) or not postgres_cid or len(postgres_cid) > 128
            or type(timeout) not in (int, float) or not math.isfinite(timeout)
            or timeout <= 0 or timeout > 2.0):
        raise ValueError('diagnostics_arguments_invalid')
    start_utc = datetime.now(timezone.utc).isoformat()
    start_mono = time.monotonic()
    try:
        raw = smoke.db_psql(_CommandOnlyRegistry(registry), postgres_cid, _SQL,
                            timeout=timeout)
    except (TimeoutError, subprocess.TimeoutExpired):
        raise ValueError('diagnostics_timeout') from None
    except Exception:
        raise ValueError('diagnostics_query_failed') from None
    end_mono = time.monotonic()
    end_utc = datetime.now(timezone.utc).isoformat()
    if not isinstance(raw, str) or len(raw.encode('utf-8', errors='replace')) > MAX_OUTPUT_BYTES:
        raise ValueError('diagnostics_payload_rejected')
    try:
        data = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(data, dict):
            raise ValueError
        version = _numeric(data.get('server_version_num'))
        if type(version) is not int or not 160000 <= version < 170000:
            raise ValueError
        settings = data.get('settings')
        if not isinstance(settings, dict) or any(type(settings.get(k)) is not bool for k in
               ('track_activities', 'track_counts', 'track_io_timing', 'track_wal_io_timing')):
            raise ValueError
        database = data.get('database')
        if not isinstance(database, dict):
            raise ValueError
        database_clean = {key: _numeric(database.get(key), nullable=key.endswith('_time'))
                          for key in _COUNTERS['database']}
        result = {
            'server_version_num': version,
            'postmaster_start_time': _timestamp(data.get('postmaster_start_time')),
            'stats_reset': _timestamp(data.get('stats_reset')),
            'timing': {key: settings[key] for key in
                       ('track_activities', 'track_counts', 'track_io_timing',
                        'track_wal_io_timing')},
            'io_timing': ({'available': True} if settings['track_io_timing'] else
                          {'available': False, 'reason': 'track_io_timing_disabled'}),
            'wal_io_timing': ({'available': True} if settings['track_wal_io_timing'] else
                              {'available': False, 'reason': 'track_wal_io_timing_disabled'}),
            'sample': {'start_utc': start_utc, 'end_utc': end_utc,
                       'start_monotonic_seconds': start_mono, 'end_monotonic_seconds': end_mono},
            'activity': _activity(data.get('activity')),
            'database': database_clean,
            'wal': _stats(data.get('wal') | {
                'track_wal_io_timing': settings['track_wal_io_timing']}, 'wal'),
            'io': _stats(data.get('io') | {'track_io_timing': settings['track_io_timing']},
                         'io', rows=True),
            'postgresql_scope': 'pinned_fixture_postgresql_16_only',
            'wal_scope_note': 'pg_stat_wal is cluster-wide, not telemetry-only',
            'io_scope_note': 'pg_stat_io is cluster-wide, not telemetry-only',
            'contention_note': 'pg_blocking_pids is sampled evidence; absence is not proof of no contention',
        }
        return result
    except (TypeError, ValueError, OverflowError, KeyError):
        raise ValueError('diagnostics_payload_rejected') from None
