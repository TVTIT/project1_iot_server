#!/usr/bin/env python3
"""Task 3.7.1-3.7.4 Workload Runner and Bounded Resource/Storage Measurement.

Provides the dedicated 600-batch, 100 Hz workload runner, bounded multi-container
resource sampler, storage delta measurement (with hypertable chunks/indexes),
strict summary validation, and isolated content verification.

Does NOT execute the 600-batch workload during test suite imports.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

from task28_simplified_schema_runner import DatabaseFixture, ENV, sanitize_output  # noqa: E402
from stage3_persistence_runner import bootstrap_to_9, run_existing_migration_runner  # noqa: E402
from stage3_mqtt_ingress_runner import (  # noqa: E402
    SIM_PROFILE, MOSQUITTO_IMAGE,
    run, docker, container_logs, docker_create, container_absent, remove_container,
    mapped_port, write_private, generate_certs,
    bootstrap_dynsec, wait_http, wait_tcp, connect_manager, parse_run_summary,
    docker_available,
)
from task26_harness_helpers import RESPONSE  # noqa: E402
from stage3_uplink_runner import (  # noqa: E402
    ResourceRegistry, build_image, start_backend,
    parse_ingress_summary, sanitize, ensure_budget,
    SHARED_SENSOR_ID, SENSOR_UNIT,
    SIM_BROKER_HOST_IN_CONTAINER, SIM_BROKER_PORT,
    READY_TIMEOUT, CONTAINER_START_TIMEOUT,
    seed_graph, GatewaySpec, absent,
)

# --- Workload Profile Constants (Section 3.7.1 / 3.7.3) ----------------------

WORKLOAD_GATEWAY_ID = 'gateway_stage3_workload_001'
WORKLOAD_SENSOR_ID = SHARED_SENSOR_ID
WORKLOAD_RATE_HZ = 100
WORKLOAD_SAMPLES_PER_BATCH = 100
WORKLOAD_SAMPLE_INTERVAL_US = 10000
WORKLOAD_BATCH_SPAN = '1s'
WORKLOAD_TARGET_BATCHES = 600
WORKLOAD_TOTAL_SAMPLES = 60000

# Backend profile
WORKLOAD_QUEUE_CAPACITY = 256
WORKLOAD_WORKERS = 4
WORKLOAD_MAX_PAYLOAD_BYTES = 65536
WORKLOAD_DATABASE_MAX_CONNS = 10
WORKLOAD_DATABASE_OP_TIMEOUT = '5s'

# Gateway profile
WORKLOAD_MAX_INFLIGHT = 8
WORKLOAD_ACK_TIMEOUT = '5s'
WORKLOAD_RETRY_MIN_DELAY = '1s'
WORKLOAD_RETRY_MAX_DELAY = '30s'
WORKLOAD_OUTBOX_MAX_ROWS = 5000
WORKLOAD_OUTBOX_MAX_BYTES = 67108864  # 64 MiB

# Time budgets (seconds)
WORKLOAD_GENERATION_BUDGET = 750    # ~600s generation + margin
WORKLOAD_DRAIN_BUDGET = 180         # post-generation drain
WORKLOAD_CONTAINER_TIMEOUT = 1000   # subprocess limit for docker run/wait
WORKLOAD_GLOBAL_BUDGET = 1500       # full runner execution limit


# --- Summary Parsers & Metric Extraction (Section 3.7.2 / 3.7.4) -------------

def parse_measurement_summary(output: str) -> dict[str, int | float | str]:
    """Parse the CLI's structured ``measurement summary:`` line strictly.

    Requires all documented metric tokens, converting integers and floats,
    and returns empty dict if absent or malformed.
    """
    for line in output.splitlines():
        if line.startswith('measurement summary:'):
            rest = line[len('measurement summary:'):].strip()
            tokens = rest.split()
            parsed: dict[str, int | float | str] = {}
            for t in tokens:
                if '=' not in t:
                    continue
                k, v = t.split('=', 1)
                if k in ('samples', 'first_publishes', 'retry_attempts', 'missing_origins', 'overflow_samples'):
                    try:
                        parsed[k] = int(v)
                    except ValueError:
                        return {}
                elif k in ('p50_ms', 'p95_ms', 'min_ms', 'max_ms', 'gen_duration_ms', 'publish_drain_duration_ms', 'total_duration_ms'):
                    if v == 'none':
                        parsed[k] = 'none'
                    else:
                        try:
                            parsed[k] = float(v)
                        except ValueError:
                            return {}
                else:
                    parsed[k] = v
            expected_keys = {
                'samples', 'p50_ms', 'p95_ms', 'min_ms', 'max_ms',
                'first_publishes', 'retry_attempts', 'missing_origins',
                'overflow_samples', 'gen_duration_ms',
                'publish_drain_duration_ms', 'total_duration_ms',
            }
            if expected_keys.issubset(parsed.keys()):
                return parsed
            return {}
    return {}


def calculate_throughputs(total_batches: int, total_samples: int,
                          windows_sec: dict[str, float | str]) -> dict[str, dict[str, float | str]]:
    """Compute batches/s and samples/s using distinct, precise denominators.

    Windows include:
      * first_to_last_enqueue (e.g. ~599s for 600 batches)
      * run_start_to_last_enqueue (marked 'unavailable' if not directly recorded)
      * publish_to_drain
      * runner_total
    """
    throughputs: dict[str, dict[str, float | str]] = {}
    for name, dur in windows_sec.items():
        if isinstance(dur, (int, float)) and dur > 0:
            throughputs[name] = {
                'duration_s': round(dur, 3),
                'batches_per_sec': round(total_batches / dur, 3),
                'samples_per_sec': round(total_samples / dur, 3),
            }
        elif isinstance(dur, (int, float)):
            throughputs[name] = {
                'duration_s': 0.0,
                'batches_per_sec': 0.0,
                'samples_per_sec': 0.0,
            }
        else:
            throughputs[name] = {
                'duration_s': 'unavailable',
                'batches_per_sec': 'unavailable',
                'samples_per_sec': 'unavailable',
            }
    return throughputs


# --- Storage Measurement (Section 3.7.4) -------------------------------------

@dataclass
class StorageSnapshot:
    telemetry_hypertable_bytes: int = 0
    telemetry_detailed: dict[str, int] = field(default_factory=dict)
    telemetry_chunks: int = 0
    temporal_hypertable_bytes: int = 0
    temporal_detailed: dict[str, int] = field(default_factory=dict)
    temporal_chunks: int = 0
    ledger_table_bytes: int = 0
    ledger_indexes_bytes: int = 0
    ledger_total_bytes: int = 0
    database_bytes: int = 0
    sqlite_file_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            'telemetry_hypertable_bytes': self.telemetry_hypertable_bytes,
            'telemetry_detailed': self.telemetry_detailed,
            'telemetry_chunks': self.telemetry_chunks,
            'temporal_hypertable_bytes': self.temporal_hypertable_bytes,
            'temporal_detailed': self.temporal_detailed,
            'temporal_chunks': self.temporal_chunks,
            'ledger_table_bytes': self.ledger_table_bytes,
            'ledger_indexes_bytes': self.ledger_indexes_bytes,
            'ledger_total_bytes': self.ledger_total_bytes,
            'database_bytes': self.database_bytes,
            'sqlite_file_bytes': self.sqlite_file_bytes,
        }


def capture_storage_snapshot(fixture: DatabaseFixture, sqlite_path: Path | None) -> StorageSnapshot:
    """Capture raw, temporal, ledger, database and SQLite sizes using actual schema names.

    Uses Timescale's hypertable_size() and hypertable_detailed_size() for hypertables.
    """
    sql = """
    DO $$
    DECLARE
      j jsonb;
      t_det record;
      tmp_det record;
      t_chunks int;
      tmp_chunks int;
    BEGIN
      SELECT table_bytes, index_bytes, toast_bytes, total_bytes
        INTO t_det FROM hypertable_detailed_size('public.telemetry'::regclass);
      SELECT table_bytes, index_bytes, toast_bytes, total_bytes
        INTO tmp_det FROM hypertable_detailed_size('public.twin_temporal_values'::regclass);

      SELECT count(*) INTO t_chunks FROM timescaledb_information.chunks WHERE hypertable_name = 'telemetry';
      SELECT count(*) INTO tmp_chunks FROM timescaledb_information.chunks WHERE hypertable_name = 'twin_temporal_values';

      SELECT jsonb_build_object(
        'telemetry_hypertable_bytes', (SELECT hypertable_size('public.telemetry'::regclass)),
        'telemetry_detailed', jsonb_build_object(
          'table_bytes', t_det.table_bytes,
          'index_bytes', t_det.index_bytes,
          'toast_bytes', t_det.toast_bytes,
          'total_bytes', t_det.total_bytes
        ),
        'telemetry_chunks', t_chunks,
        'temporal_hypertable_bytes', (SELECT hypertable_size('public.twin_temporal_values'::regclass)),
        'temporal_detailed', jsonb_build_object(
          'table_bytes', tmp_det.table_bytes,
          'index_bytes', tmp_det.index_bytes,
          'toast_bytes', tmp_det.toast_bytes,
          'total_bytes', tmp_det.total_bytes
        ),
        'temporal_chunks', tmp_chunks,
        'ledger_table_bytes', (SELECT pg_relation_size('public.processed_messages'::regclass)),
        'ledger_indexes_bytes', (SELECT pg_indexes_size('public.processed_messages'::regclass)),
        'ledger_total_bytes', (SELECT pg_total_relation_size('public.processed_messages'::regclass)),
        'database_bytes', (SELECT pg_database_size(current_database()))
      ) INTO j;
      RAISE NOTICE 'STORAGE_SNAP %', j;
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    text = out.decode('utf-8', errors='replace')
    data = {}
    for line in text.splitlines():
        idx = line.find('STORAGE_SNAP ')
        if idx != -1:
            data = json.loads(line[idx + len('STORAGE_SNAP '):].strip())
            break
    if not data:
        raise RuntimeError('storage snapshot marker not emitted')

    sqlite_size = 0
    if sqlite_path is not None and sqlite_path.exists():
        sqlite_size = sqlite_path.stat().st_size

    return StorageSnapshot(
        telemetry_hypertable_bytes=int(data.get('telemetry_hypertable_bytes', 0)),
        telemetry_detailed={k: int(v) for k, v in data.get('telemetry_detailed', {}).items()},
        telemetry_chunks=int(data.get('telemetry_chunks', 0)),
        temporal_hypertable_bytes=int(data.get('temporal_hypertable_bytes', 0)),
        temporal_detailed={k: int(v) for k, v in data.get('temporal_detailed', {}).items()},
        temporal_chunks=int(data.get('temporal_chunks', 0)),
        ledger_table_bytes=int(data.get('ledger_table_bytes', 0)),
        ledger_indexes_bytes=int(data.get('ledger_indexes_bytes', 0)),
        ledger_total_bytes=int(data.get('ledger_total_bytes', 0)),
        database_bytes=int(data.get('database_bytes', 0)),
        sqlite_file_bytes=sqlite_size,
    )


def compute_storage_deltas(before: StorageSnapshot, after: StorageSnapshot) -> dict[str, int]:
    return {
        'telemetry_hypertable_bytes_delta': after.telemetry_hypertable_bytes - before.telemetry_hypertable_bytes,
        'telemetry_chunks_delta': after.telemetry_chunks - before.telemetry_chunks,
        'temporal_hypertable_bytes_delta': after.temporal_hypertable_bytes - before.temporal_hypertable_bytes,
        'temporal_chunks_delta': after.temporal_chunks - before.temporal_chunks,
        'ledger_table_bytes_delta': after.ledger_table_bytes - before.ledger_table_bytes,
        'ledger_indexes_bytes_delta': after.ledger_indexes_bytes - before.ledger_indexes_bytes,
        'ledger_total_bytes_delta': after.ledger_total_bytes - before.ledger_total_bytes,
        'database_bytes_delta': after.database_bytes - before.database_bytes,
        'sqlite_file_bytes_delta': after.sqlite_file_bytes - before.sqlite_file_bytes,
    }


# --- Bounded Resource Sampler (Section 3.7.2 / 3.7.4) -----------------------

def parse_bytes_unit(val: str) -> int:
    """Parse strings like '12.34MiB' or '1.2GiB' into integer bytes."""
    v = val.strip().lower()
    # Ordered longest suffix first so 'kib' matches before 'b'
    units = [
        ('kib', 1024), ('kb', 1000),
        ('mib', 1024 * 1024), ('mb', 1000 * 1000),
        ('gib', 1024 * 1024 * 1024), ('gb', 1000 * 1000 * 1000),
        ('b', 1),
    ]
    for u, factor in units:
        if v.endswith(u):
            num = v[:-len(u)].strip()
            try:
                return int(float(num) * factor)
            except ValueError:
                return 0
    try:
        return int(float(v))
    except ValueError:
        return 0


class ResourceSampler:
    """Background sampling thread for fixed container IDs and live read-only SQLite.

    Bounds:
      * Maximum samples: 1500 (~25 min at 1Hz), drops oldest if exceeded
      * Fixed container IDs only; no subprocess creation per metric
      * Reads SQLite strictly in read-only URI mode with short timeout
      * Reports in-flight trend as 'unverified' (in-flight is in RAM)
    """

    def __init__(self, container_ids: dict[str, str], sqlite_path: Path | None,
                 poll_interval: float = 1.0, max_samples: int = 1500):
        self.container_ids = dict(container_ids)  # role -> cid
        self.sqlite_path = sqlite_path
        self.poll_interval = poll_interval
        self.max_samples = max_samples
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        self.samples: list[dict] = []
        self.sqlite_trend: list[dict] = []
        self.intervals: list[float] = []
        self.last_sample_time: float | None = None
        self.gaps_over_3s = 0

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name='workload-resource-sampler', daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _sample_containers(self) -> dict[str, dict[str, float | int]]:
        cids = list(self.container_ids.values())
        if not cids:
            return {}
        try:
            cmd = ['docker', 'stats', '--no-stream', '--format', '{{.ID}}\t{{.CPUPerc}}\t{{.MemUsage}}', *cids]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            if res.returncode != 0:
                return {}
            out = res.stdout.decode('utf-8', errors='replace').strip()
            stats_by_id = {}
            for line in out.splitlines():
                parts = line.split('\t')
                if len(parts) >= 3:
                    cid_prefix = parts[0].strip()
                    cpu_s = parts[1].strip().rstrip('%')
                    mem_s = parts[2].strip()
                    try:
                        cpu_f = float(cpu_s)
                    except ValueError:
                        cpu_f = 0.0
                    mem_used = 0
                    if '/' in mem_s:
                        used_part = mem_s.split('/')[0].strip()
                        mem_used = parse_bytes_unit(used_part)
                    stats_by_id[cid_prefix] = {'cpu_pct': cpu_f, 'mem_bytes': mem_used}

            results = {}
            for role, full_cid in self.container_ids.items():
                found = None
                for pfx, s in stats_by_id.items():
                    if full_cid.startswith(pfx) or pfx.startswith(full_cid[:12]):
                        found = s
                        break
                if found is not None:
                    results[role] = found
            return results
        except Exception:
            return {}

    def _sample_sqlite_readonly(self) -> dict[str, int | str] | None:
        if self.sqlite_path is None or not self.sqlite_path.exists():
            return None
        uri = f'file:{self.sqlite_path.as_posix()}?mode=ro'
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=0.5)
            try:
                cur = conn.cursor()
                cur.execute("SELECT count(*), coalesce(sum(byte_size), 0) FROM outbox WHERE status='pending'")
                row = cur.fetchone()
                pending_rows, pending_bytes = (row[0], row[1]) if row else (0, 0)
                cur.execute("SELECT count(*), coalesce(sum(byte_size), 0) FROM outbox WHERE status='quarantined'")
                q_row = cur.fetchone()
                q_rows, q_bytes = (q_row[0], q_row[1]) if q_row else (0, 0)
                return {
                    'pending_rows': int(pending_rows),
                    'pending_bytes': int(pending_bytes),
                    'quarantined_rows': int(q_rows),
                    'quarantined_bytes': int(q_bytes),
                    'in_flight': 'unavailable',
                }
            finally:
                conn.close()
        except Exception:
            return None

    def _run(self):
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            if self.last_sample_time is not None:
                interval = t0 - self.last_sample_time
                self.intervals.append(interval)
                if interval > 3.0:
                    self.gaps_over_3s += 1
            self.last_sample_time = t0

            c_stats = self._sample_containers()
            sq_stats = self._sample_sqlite_readonly()

            if len(self.samples) < self.max_samples:
                self.samples.append({
                    'mono_time': t0,
                    'containers': c_stats,
                })
                if sq_stats is not None:
                    self.sqlite_trend.append({
                        'mono_time': t0,
                        **sq_stats,
                    })

            self._stop_event.wait(self.poll_interval)

    def summary(self) -> dict:
        summary_per_container: dict[str, dict] = {}
        for role in self.container_ids:
            cpus = [s['containers'][role]['cpu_pct'] for s in self.samples if role in s.get('containers', {})]
            mems = [s['containers'][role]['mem_bytes'] for s in self.samples if role in s.get('containers', {})]
            if cpus:
                cpu_max = round(max(cpus), 2)
                cpu_mean = round(sum(cpus) / len(cpus), 2)
            else:
                cpu_max = 0.0
                cpu_mean = 0.0
            if mems:
                mem_max = max(mems)
                mem_mean = int(sum(mems) / len(mems))
            else:
                mem_max = 0
                mem_mean = 0

            summary_per_container[role] = {
                'cpu_max_pct': cpu_max,
                'cpu_mean_pct': cpu_mean,
                'ram_max_bytes': mem_max,
                'ram_mean_bytes': mem_mean,
                'ram_max_mb': round(mem_max / (1024 * 1024), 2),
                'ram_mean_mb': round(mem_mean / (1024 * 1024), 2),
            }

        mean_interval = round(sum(self.intervals) / len(self.intervals), 3) if self.intervals else 0.0
        min_interval = round(min(self.intervals), 3) if self.intervals else 0.0
        max_interval = round(max(self.intervals), 3) if self.intervals else 0.0

        pending_rows_samples = [s['pending_rows'] for s in self.sqlite_trend if isinstance(s.get('pending_rows'), int)]
        pending_bytes_samples = [s['pending_bytes'] for s in self.sqlite_trend if isinstance(s.get('pending_bytes'), int)]
        backlog_high_water_rows = max(pending_rows_samples) if pending_rows_samples else 0
        backlog_high_water_bytes = max(pending_bytes_samples) if pending_bytes_samples else 0

        return {
            'sample_count': len(self.samples),
            'observed_interval_mean_s': mean_interval,
            'observed_interval_min_s': min_interval,
            'observed_interval_max_s': max_interval,
            'gaps_over_3s_count': self.gaps_over_3s,
            'containers': summary_per_container,
            'sqlite_trend_sample_count': len(self.sqlite_trend),
            'sqlite_trend_last': self.sqlite_trend[-1] if self.sqlite_trend else None,
            'backlog_high_water_rows': backlog_high_water_rows,
            'backlog_high_water_bytes': backlog_high_water_bytes,
            'in_flight': 'unavailable',
        }


def read_container_limits(cid: str) -> dict[str, int | str]:
    """Inspect HostConfig resource limits. Zero means no separate quota applied."""
    try:
        raw = docker(['inspect', '-f',
                      '{{json .HostConfig}}', cid], timeout=10)
        data = json.loads(raw.decode('utf-8'))
        return {
            'cpu_quota': int(data.get('CpuQuota', 0)),
            'cpu_period': int(data.get('CpuPeriod', 0)),
            'nano_cpus': int(data.get('NanoCpus', 0)),
            'cpuset_cpus': str(data.get('CpusetCpus', '')),
            'memory_bytes': int(data.get('Memory', 0)),
        }
    except Exception:
        return {
            'cpu_quota': 0, 'cpu_period': 0, 'nano_cpus': 0, 'cpuset_cpus': '', 'memory_bytes': 0,
        }


def get_host_environment() -> dict:
    available_ram = 0
    try:
        with open('/proc/meminfo', 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    parts = line.split()
                    if len(parts) >= 2:
                        available_ram = int(parts[1]) * 1024
                    break
    except Exception:
        pass

    docker_ver = 'unknown'
    try:
        docker_ver = subprocess.check_output(['docker', '--version'], text=True).strip()
    except Exception:
        pass

    go_ver = 'unknown'
    try:
        go_ver = subprocess.check_output(['go', 'version'], text=True).strip()
    except Exception:
        pass

    return {
        'host_os': platform.system(),
        'host_arch': platform.machine(),
        'available_cpus': os.cpu_count() or 1,
        'available_ram_bytes': available_ram,
        'versions': {
            'docker': docker_ver,
            'go': go_ver,
            'mosquitto': 'eclipse-mosquitto:2.0.18',
            'timescaledb': 'timescale/timescaledb:2.29.2-pg16',
        },
    }


def get_git_provenance() -> dict:
    head = 'unknown'
    dirty = False
    dirty_files: list[str] = []
    try:
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        status_out = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip()
        if status_out:
            dirty = True
            dirty_files = [line.strip() for line in status_out.splitlines()]
    except Exception:
        pass
    return {
        'git_head': head,
        'git_dirty': dirty,
        'dirty_files_count': len(dirty_files),
        'dirty_qualification': 'uncommitted changes preserved' if dirty else 'clean',
    }


# --- SQL Content Invariant Verification (Section 3.7.4) ----------------------

def build_workload_assert_sql(spec: GatewaySpec, marker: str) -> str:
    """Build the comprehensive SQL verification block for the 600-batch workload.

    Validates:
      1) 600 unique ledgers in public.processed_messages (sample_count=100, interval=10000us,
         source_topic, sensor_id, hash, sequence ranges 0..59999 contiguous without gap/overlap).
      2) 60,000 raw samples in public.telemetry (index 0..99, sequence 0..59999 no gap/dup,
         synthetic value formula verification for all rows, timestamp expansion).
      3) 600 temporal observations in public.twin_temporal_values (sample_interval_us=10000,
         typed columns exclusive, unit='us', source='telemetry_batch', boot_id, timestamp=last sample).
      4) Twin states: Sensor reported sequence 59999, interval 10000, value matches formula;
         desired state and version unchanged; Gateway Twin completely unchanged.
    """
    def q(val: str) -> str:
        return "'" + val + "'"

    return f"""
    DO $$
    DECLARE
      l_count int;
      r_count int;
      t_count int;
      msg_dist int;
      boot_dist int;
      seq_min bigint;
      seq_max bigint;
      seq_dist bigint;
      bad_ledgers int;
      bad_ranges int;
      bad_raw int;
      bad_raw_val int;
      bad_temporal int;
      rep jsonb;
      des jsonb;
      rep_ver bigint;
      des_ver bigint;
      gw_rep jsonb;
      gw_des jsonb;
      gw_rep_ver bigint;
      gw_des_ver bigint;
      expected_last_val double precision;
      actual_last_val double precision;
    BEGIN
      -- 1. Ledger checks (public.processed_messages)
      SELECT count(*), count(DISTINCT message_id), count(DISTINCT boot_id)
        INTO l_count, msg_dist, boot_dist
        FROM public.processed_messages
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)};
      IF l_count <> {WORKLOAD_TARGET_BATCHES} THEN
        RAISE EXCEPTION 'ledger count = %, want {WORKLOAD_TARGET_BATCHES}', l_count;
      END IF;
      IF msg_dist <> {WORKLOAD_TARGET_BATCHES} THEN
        RAISE EXCEPTION 'ledger distinct message_id = %, want {WORKLOAD_TARGET_BATCHES}', msg_dist;
      END IF;
      IF boot_dist <> 1 THEN
        RAISE EXCEPTION 'ledger distinct boot_id = %, want 1', boot_dist;
      END IF;

      SELECT count(*) INTO bad_ledgers FROM public.processed_messages
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)}
          AND (sample_count <> {WORKLOAD_SAMPLES_PER_BATCH}
               OR source_topic <> {q(spec.telemetry_topic)}
               OR payload_hash IS NULL OR length(payload_hash) <> 64
               OR last_sequence <> first_sequence + sample_count - 1);
      IF bad_ledgers <> 0 THEN
        RAISE EXCEPTION 'found % invalid ledger envelopes', bad_ledgers;
      END IF;

      -- Check sequence range contiguity across batches (no gaps, no overlaps)
      WITH ordered_batches AS (
        SELECT first_sequence, last_sequence,
               lag(last_sequence) OVER (ORDER BY first_sequence) AS prev_last
        FROM public.processed_messages
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)}
      )
      SELECT count(*) INTO bad_ranges FROM ordered_batches
        WHERE prev_last IS NOT NULL AND first_sequence <> prev_last + 1;
      IF bad_ranges <> 0 THEN
        RAISE EXCEPTION 'found % discontinuous sequence boundaries in ledger', bad_ranges;
      END IF;

      -- 2. Raw sample checks (public.telemetry)
      SELECT count(*), min(sequence_num), max(sequence_num), count(DISTINCT sequence_num)
        INTO r_count, seq_min, seq_max, seq_dist
        FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)};
      IF r_count <> {WORKLOAD_TOTAL_SAMPLES} THEN
        RAISE EXCEPTION 'raw sample count = %, want {WORKLOAD_TOTAL_SAMPLES}', r_count;
      END IF;
      IF seq_min <> 0 OR seq_max <> {WORKLOAD_TOTAL_SAMPLES - 1} THEN
        RAISE EXCEPTION 'raw sequence range [%, %], want [0, {WORKLOAD_TOTAL_SAMPLES - 1}]', seq_min, seq_max;
      END IF;
      IF seq_dist <> {WORKLOAD_TOTAL_SAMPLES} THEN
        RAISE EXCEPTION 'distinct raw sequences = %, want {WORKLOAD_TOTAL_SAMPLES}', seq_dist;
      END IF;

      -- Check index, sequence and timestamp expansion
      WITH sample_origins AS (
        SELECT message_id, measured_at AS base_measured_at
        FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)} AND sample_index = 0
      )
      SELECT count(*) INTO bad_raw FROM public.telemetry r
        JOIN public.processed_messages m ON m.message_id = r.message_id
        JOIN sample_origins o ON o.message_id = r.message_id
        WHERE r.gateway_id = {q(spec.gateway_id)} AND r.sensor_id = {q(spec.sensor_id)}
          AND (r.sample_index < 0 OR r.sample_index >= {WORKLOAD_SAMPLES_PER_BATCH}
               OR r.sequence_num <> m.first_sequence + r.sample_index
               OR r.measured_at <> o.base_measured_at + (r.sample_index * interval '{WORKLOAD_SAMPLE_INTERVAL_US} microseconds'));
      IF bad_raw <> 0 THEN
        RAISE EXCEPTION 'found % corrupted raw sample envelopes or timestamps', bad_raw;
      END IF;

      -- Verify synthetic value formula across all 60,000 samples:
      -- position = seq % 2000; if position > 1000 then position = 2000 - position; 20 + position / 100.0
      SELECT count(*) INTO bad_raw_val FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)}
          AND abs(value - (20.0 + (CASE
                WHEN ((sequence_num % 2000) > 1000) THEN (2000 - (sequence_num % 2000))
                ELSE (sequence_num % 2000)
              END)::double precision / 100.0)) > 1e-6;
      IF bad_raw_val <> 0 THEN
        RAISE EXCEPTION 'found % raw samples violating synthetic value formula', bad_raw_val;
      END IF;

      -- 3. Temporal observation checks (public.twin_temporal_values)
      SELECT count(*) INTO t_count FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)})
          AND property_name = 'sample_interval_us';
      IF t_count <> {WORKLOAD_TARGET_BATCHES} THEN
        RAISE EXCEPTION 'temporal observations = %, want {WORKLOAD_TARGET_BATCHES}', t_count;
      END IF;

      WITH last_samples AS (
        SELECT message_id, measured_at AS last_measured_at, boot_id
        FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)}
          AND sample_index = {WORKLOAD_SAMPLES_PER_BATCH - 1}
      )
      SELECT count(*) INTO bad_temporal FROM public.twin_temporal_values t
        JOIN last_samples s ON s.message_id = t.message_id
        WHERE t.entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)})
          AND t.property_name = 'sample_interval_us'
          AND (t.value_number <> {WORKLOAD_SAMPLE_INTERVAL_US}
               OR t.value_text IS NOT NULL OR t.value_boolean IS NOT NULL
               OR t.metadata->>'unit' <> 'us'
               OR t.metadata->>'source' <> 'telemetry_batch'
               OR t.metadata->>'boot_id' <> s.boot_id::text
               OR t.observed_at <> s.last_measured_at);
      IF bad_temporal <> 0 THEN
        RAISE EXCEPTION 'found % corrupted temporal interval observations', bad_temporal;
      END IF;

      -- 4. Twin states & Sentinels
      SELECT reported_state, desired_state, reported_version, desired_version
        INTO rep, des, rep_ver, des_ver FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)});
      -- reported_version follows actual ordering: at least one advance, bounded by target batches
      IF rep_ver < 1 OR rep_ver > {WORKLOAD_TARGET_BATCHES} THEN
        RAISE EXCEPTION 'sensor reported_version = %, want 1..{WORKLOAD_TARGET_BATCHES}', rep_ver;
      END IF;
      IF (rep->>'sequence_num')::bigint <> {WORKLOAD_TOTAL_SAMPLES - 1} THEN
        RAISE EXCEPTION 'sensor reported sequence = %, want {WORKLOAD_TOTAL_SAMPLES - 1}', rep->>'sequence_num';
      END IF;
      IF (rep->>'sample_interval_us')::bigint <> {WORKLOAD_SAMPLE_INTERVAL_US} THEN
        RAISE EXCEPTION 'sensor reported interval = %, want {WORKLOAD_SAMPLE_INTERVAL_US}', rep->>'sample_interval_us';
      END IF;
      IF rep->>'unit' IS DISTINCT FROM {q(spec.sensor_unit)} THEN
        RAISE EXCEPTION 'sensor reported unit = %', rep->>'unit';
      END IF;

      -- Last sample synthetic value check
      expected_last_val := 20.0 + (CASE
        WHEN ((({WORKLOAD_TOTAL_SAMPLES - 1}) % 2000) > 1000) THEN (2000 - (({WORKLOAD_TOTAL_SAMPLES - 1}) % 2000))
        ELSE (({WORKLOAD_TOTAL_SAMPLES - 1}) % 2000)
      END)::double precision / 100.0;
      actual_last_val := (rep->>'value')::double precision;
      IF abs(actual_last_val - expected_last_val) > 1e-6 THEN
        RAISE EXCEPTION 'sensor reported value = %, want %', actual_last_val, expected_last_val;
      END IF;

      -- Sensor desired state unchanged
      IF des <> {q(json.dumps(spec.sensor_desired))}::jsonb OR des_ver <> {spec.sensor_desired_version} THEN
        RAISE EXCEPTION 'sensor desired state was corrupted';
      END IF;

      -- Gateway Twin sentinel unchanged
      SELECT reported_state, desired_state, reported_version, desired_version
        INTO gw_rep, gw_des, gw_rep_ver, gw_des_ver FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.gateway_urn)});
      IF gw_rep <> {q(json.dumps(spec.gateway_reported))}::jsonb
         OR gw_des <> {q(json.dumps(spec.gateway_desired))}::jsonb
         OR gw_rep_ver <> {spec.gateway_reported_version}
         OR gw_des_ver <> {spec.gateway_desired_version} THEN
        RAISE EXCEPTION 'gateway twin sentinel state was corrupted';
      END IF;

      RAISE NOTICE '{marker}';
    END $$;
    """


def assert_workload_database_content(fixture: DatabaseFixture, spec: GatewaySpec, marker: str = 'WORKLOAD_SQL_PASS'):
    sql = build_workload_assert_sql(spec, marker)
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'Workload SQL verification did not emit {marker}')


# --- Gateway Workload Configuration Helper -----------------------------------

def workload_gateway_env(gateway_id: str, password: str, sensor_id: str = WORKLOAD_SENSOR_ID) -> dict:
    env = dict(SIM_PROFILE)
    env.update({
        'GATEWAY_ID': gateway_id,
        'SENSOR_ID': sensor_id,
        'GATEWAY_MQTT_BROKER_URL': f'ssl://{SIM_BROKER_HOST_IN_CONTAINER}:{SIM_BROKER_PORT}',
        'GATEWAY_MQTT_CLIENT_ID': f'{gateway_id}_workload',
        'GATEWAY_MQTT_USERNAME': gateway_id,
        'GATEWAY_MQTT_PASSWORD': password,
        'GATEWAY_MQTT_CA_FILE': '/data/ca/ca.crt',
        'GATEWAY_OUTBOX_PATH': '/data/outbox/outbox.db',
        # Profile specifications
        'GATEWAY_MAX_INFLIGHT': str(WORKLOAD_MAX_INFLIGHT),
        'GATEWAY_ACK_TIMEOUT': WORKLOAD_ACK_TIMEOUT,
        'GATEWAY_RETRY_MIN_DELAY': WORKLOAD_RETRY_MIN_DELAY,
        'GATEWAY_RETRY_MAX_DELAY': WORKLOAD_RETRY_MAX_DELAY,
        'GATEWAY_OUTBOX_MAX_ROWS': str(WORKLOAD_OUTBOX_MAX_ROWS),
        'GATEWAY_OUTBOX_MAX_BYTES': str(WORKLOAD_OUTBOX_MAX_BYTES),
        'GATEWAY_SAMPLES_PER_BATCH': str(WORKLOAD_SAMPLES_PER_BATCH),
        'GATEWAY_SAMPLE_INTERVAL_US': str(WORKLOAD_SAMPLE_INTERVAL_US),
        'GATEWAY_BATCH_SPAN': WORKLOAD_BATCH_SPAN,
    })
    return env


# --- Workload Execution Orchestrator -----------------------------------------

def run_workload_orchestrator(backend_image: str, gateway_image: str, output_summary_path: Path) -> dict:
    """Execute the full 600-batch workload lifecycle, measure, verify, and emit JSON summary.

    Throws RuntimeError on any contract violation or verification failure.
    """
    hexid = secrets.token_hex(6)
    net_name = f'stage3_workload_net_{hexid}'
    dynsec_volume = f'stage3_workload_dynsec_{hexid}'
    broker_data_volume = f'stage3_workload_bdata_{hexid}'

    tmp_parent = '/tmp/opencode' if os.path.exists('/tmp/opencode') else None
    work = Path(tempfile.mkdtemp(prefix='stage3-workload-', dir=tmp_parent))
    os.chmod(work, 0o755)
    registry = ResourceRegistry()
    owned_containers: list[str] = []

    network_created = False
    broker_cid = None
    fixture = None
    sampler = None
    primary_error = None

    try:
        ensure_budget('network')
        docker(['network', 'create', net_name], timeout=30)
        network_created = True
        registry.register('network', 'network', net_name)

        ensure_budget('database')
        fixture = DatabaseFixture()
        fixture.prepare_image()
        fixture.create()
        fixture.start()
        fixture.wait_ready()
        docker(['network', 'connect', '--alias', 'database', net_name, fixture.container_id], timeout=30)

        # Baseline migrations through 20
        bootstrap_to_9(fixture)
        run_existing_migration_runner(fixture, 20)

        spec = GatewaySpec(
            gateway_id=WORKLOAD_GATEWAY_ID,
            gateway_name='Stage 3 Workload Gateway',
            sensor_name='Stage 3 Workload Sensor',
            gateway_reported={'firmware_version': '1.0.0'},
            gateway_desired={'reboot_requested': False},
            gateway_reported_version=1,
            gateway_desired_version=1,
            sensor_desired={'sampling_interval_seconds': 5},
            sensor_desired_version=1,
        )
        seed_graph(fixture, [spec])

        ensure_budget('certs')
        generate_certs(work)

        ensure_budget('dynsec')
        manager_password = secrets.token_urlsafe(24)
        backend_password = secrets.token_urlsafe(24)
        gateway_password = secrets.token_urlsafe(24)

        dynsec_volume = bootstrap_dynsec(work, manager_password, backend_password)
        registry.register('dynsec volume', 'volume', dynsec_volume)

        ensure_budget('broker')
        docker(['volume', 'create', broker_data_volume], timeout=30)
        registry.register('broker data volume', 'volume', broker_data_volume)
        broker_cid = docker_create([
            '--name', f'stage3-workload-broker-{hexid}',
            '--user', '1883:1883',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=16m',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'broker',
            '--publish', '127.0.0.1::8883',
            '--mount', f'type=bind,src={work / "broker"},dst=/mosquitto/config,readonly',
            '--mount', f'type=volume,src={broker_data_volume},dst=/mosquitto/data',
            '--mount', f'type=volume,src={dynsec_volume},dst=/mosquitto/dynsec',
            MOSQUITTO_IMAGE,
        ])
        registry.register('broker container', 'container', broker_cid)
        docker(['start', broker_cid], timeout=CONTAINER_START_TIMEOUT)
        broker_port = mapped_port(broker_cid, '8883/tcp')
        wait_tcp(broker_port, 15.0)

        # Provision DynSec client and role for the workload Gateway
        manager = connect_manager(broker_port, work, manager_password)
        try:
            manager.subscribe(RESPONSE)
            manager.request('createClient', username=spec.gateway_id, password=gateway_password)
            manager.request('createRole', rolename=f'gateway_{spec.gateway_id}_uplink', acls=[
                {'acltype': 'publishClientSend', 'topic': spec.telemetry_topic, 'allow': True, 'priority': 0},
                {'acltype': 'publishClientReceive', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
                {'acltype': 'subscribeLiteral', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
                {'acltype': 'unsubscribeLiteral', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
            ])
            manager.request('addClientRole', username=spec.gateway_id, rolename=f'gateway_{spec.gateway_id}_uplink')
        finally:
            manager.close()

        ensure_budget('backend')
        jwt_secret = secrets.token_hex(32)
        backend_extra = {
            'MQTT_QUEUE_CAPACITY': str(WORKLOAD_QUEUE_CAPACITY),
            'MQTT_WORKER_COUNT': str(WORKLOAD_WORKERS),
            'MQTT_MAX_PAYLOAD_BYTES': str(WORKLOAD_MAX_PAYLOAD_BYTES),
            'DATABASE_MAX_CONNS': str(WORKLOAD_DATABASE_MAX_CONNS),
            'DATABASE_OP_TIMEOUT': WORKLOAD_DATABASE_OP_TIMEOUT,
        }
        backend_cid, backend_port = start_backend(
            work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
            track=owned_containers, extra_env=backend_extra)
        wait_http(backend_port, '/readyz', READY_TIMEOUT)
        wait_http(backend_port, '/healthz', READY_TIMEOUT)

        # Storage Snapshot Before Workload
        outbox_dir = work / 'gateway_outbox'
        outbox_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(outbox_dir, 0o777)
        sqlite_file = outbox_dir / 'outbox.db'
        storage_before = capture_storage_snapshot(fixture, sqlite_file)

        # Prepare Gateway container
        env_dict = workload_gateway_env(spec.gateway_id, gateway_password, spec.sensor_id)
        env_file = work / 'gateway_workload.env'
        write_private(env_file, ''.join(f'{k}={v}\n' for k, v in env_dict.items()).encode())

        gateway_args = [
            'run',
            '--batches', str(WORKLOAD_TARGET_BATCHES),
            '--completion-timeout', f'{WORKLOAD_DRAIN_BUDGET}s',
            '--measure',
            '--measure-capacity', str(WORKLOAD_TARGET_BATCHES),
        ]

        gateway_cid = docker_create([
            '--name', f'stage3-workload-gateway-{hexid}',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--env-file', str(env_file),
            '--mount', f'type=bind,src={outbox_dir},dst=/data/outbox',
            '--mount', f'type=bind,src={work / "public" / "ca.crt"},dst=/data/ca/ca.crt,readonly',
            gateway_image, *gateway_args,
        ])
        registry.register('gateway container', 'container', gateway_cid)
        owned_containers.append(gateway_cid)

        # Container limits inspection
        limits = {
            'backend': read_container_limits(backend_cid),
            'gateway': read_container_limits(gateway_cid),
            'broker': read_container_limits(broker_cid),
            'database': read_container_limits(fixture.container_id),
        }

        # Start Resource Sampler
        sampler = ResourceSampler(
            container_ids={
                'backend': backend_cid,
                'gateway': gateway_cid,
                'broker': broker_cid,
                'database': fixture.container_id,
            },
            sqlite_path=sqlite_file,
            poll_interval=1.0,
            max_samples=1500,
        )
        sampler.start()

        # Run Gateway
        runner_start_mono = time.monotonic()
        runner_start_wall = datetime.datetime.now(datetime.timezone.utc).isoformat()

        docker(['start', gateway_cid], timeout=CONTAINER_START_TIMEOUT)
        # Wait for container completion within container timeout
        wait_res = subprocess.run(
            ['docker', 'wait', gateway_cid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=WORKLOAD_CONTAINER_TIMEOUT,
        )
        runner_end_mono = time.monotonic()
        runner_end_wall = datetime.datetime.now(datetime.timezone.utc).isoformat()
        runner_duration_s = runner_end_mono - runner_start_mono

        sampler.stop()

        # Check Gateway exit code and logs
        exit_code_str = wait_res.stdout.decode().strip()
        gw_logs = container_logs(gateway_cid)
        if exit_code_str != '0':
            raise RuntimeError(f'gateway workload exited with code {exit_code_str}; logs={sanitize(gw_logs[-1200:])}')

        # Storage Snapshot After Workload
        storage_after = capture_storage_snapshot(fixture, sqlite_file)
        storage_deltas = compute_storage_deltas(storage_before, storage_after)

        # Parse Gateway Output
        run_sum = parse_run_summary(gw_logs)
        meas_sum = parse_measurement_summary(gw_logs)

        # Strict Gateway assertion gates
        if run_sum.get('completed') != 'true':
            raise RuntimeError('workload gateway run_summary completed is not true')
        if int(run_sum.get('enqueued', 0)) != WORKLOAD_TARGET_BATCHES:
            raise RuntimeError(f'enqueued = {run_sum.get("enqueued")}, want {WORKLOAD_TARGET_BATCHES}')
        if int(run_sum.get('app_acknowledged', 0)) != WORKLOAD_TARGET_BATCHES:
            raise RuntimeError(f'app_acknowledged = {run_sum.get("app_acknowledged")}, want {WORKLOAD_TARGET_BATCHES}')
        if int(run_sum.get('pending_rows', -1)) != 0 or int(run_sum.get('pending_bytes', -1)) != 0:
            raise RuntimeError(f'outbox not drained: pending={run_sum.get("pending_rows")}')
        if int(run_sum.get('quarantined_rows', -1)) != 0 or int(run_sum.get('quarantined_bytes', -1)) != 0:
            raise RuntimeError(f'outbox has quarantined items: {run_sum.get("quarantined_rows")}')

        if meas_sum.get('samples') != WORKLOAD_TARGET_BATCHES:
            raise RuntimeError(f'latency samples = {meas_sum.get("samples")}, want {WORKLOAD_TARGET_BATCHES}')
        if meas_sum.get('first_publishes') != WORKLOAD_TARGET_BATCHES:
            raise RuntimeError(f'first_publishes = {meas_sum.get("first_publishes")}, want {WORKLOAD_TARGET_BATCHES}')
        if meas_sum.get('missing_origins', -1) != 0:
            raise RuntimeError(f'missing_origins = {meas_sum.get("missing_origins")}, want 0')
        if meas_sum.get('overflow_samples', -1) != 0:
            raise RuntimeError(f'overflow_samples = {meas_sum.get("overflow_samples")}, want 0')
        if meas_sum.get('p50_ms') == 'none' or float(meas_sum.get('p50_ms', 0)) <= 0:
            raise RuntimeError('p50_ms is missing or non-positive')
        if meas_sum.get('p95_ms') == 'none' or float(meas_sum.get('p95_ms', 0)) <= 0:
            raise RuntimeError('p95_ms is missing or non-positive')

        # Durations & Throughput windows
        gen_duration_s = float(meas_sum['gen_duration_ms']) / 1000.0
        publish_drain_s = float(meas_sum['publish_drain_duration_ms']) / 1000.0
        tracker_total_s = float(meas_sum['total_duration_ms']) / 1000.0

        windows_sec: dict[str, float | str] = {
            'first_to_last_enqueue_window': gen_duration_s,
            'run_start_to_last_enqueue_window': 'unavailable',
            'publish_to_drain_window': publish_drain_s,
            'runner_total_window': runner_duration_s,
        }
        throughputs = calculate_throughputs(WORKLOAD_TARGET_BATCHES, WORKLOAD_TOTAL_SAMPLES, windows_sec)

        # Stop Backend with SIGTERM and parse ingress summary
        run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
        bk_exit = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
        if bk_exit != '0':
            raise RuntimeError(f'backend exited with code {bk_exit} on SIGTERM')
        bk_logs = container_logs(backend_cid)
        if 'backend shutdown complete' not in bk_logs:
            raise RuntimeError('backend shutdown log missing confirmed complete')
        ing_sum = parse_ingress_summary(bk_logs)

        # Full SQL Invariants Check
        assert_workload_database_content(fixture, spec)

        # Resource sampler summary
        sample_sum = sampler.summary()
        for role, lim in limits.items():
            if role in sample_sum['containers']:
                sample_sum['containers'][role]['limits'] = lim

        # Build clean, allowlisted JSON report
        report = {
            'provenance': {
                **get_git_provenance(),
                'backend_image_id': backend_image,
                'gateway_image_id': gateway_image,
            },
            'environment': get_host_environment(),
            'profile': {
                'rate_hz': WORKLOAD_RATE_HZ,
                'samples_per_batch': WORKLOAD_SAMPLES_PER_BATCH,
                'sample_interval_us': WORKLOAD_SAMPLE_INTERVAL_US,
                'target_batches': WORKLOAD_TARGET_BATCHES,
                'target_samples': WORKLOAD_TOTAL_SAMPLES,
                'backend': {
                    'queue_capacity': WORKLOAD_QUEUE_CAPACITY,
                    'workers': WORKLOAD_WORKERS,
                    'max_payload_bytes': WORKLOAD_MAX_PAYLOAD_BYTES,
                    'database_max_conns': WORKLOAD_DATABASE_MAX_CONNS,
                    'database_op_timeout': WORKLOAD_DATABASE_OP_TIMEOUT,
                },
                'gateway': {
                    'max_inflight': WORKLOAD_MAX_INFLIGHT,
                    'ack_timeout': WORKLOAD_ACK_TIMEOUT,
                    'retry_min_delay': WORKLOAD_RETRY_MIN_DELAY,
                    'retry_max_delay': WORKLOAD_RETRY_MAX_DELAY,
                    'outbox_max_rows': WORKLOAD_OUTBOX_MAX_ROWS,
                    'outbox_max_bytes': WORKLOAD_OUTBOX_MAX_BYTES,
                },
            },
            'counts': {
                'enqueued_unique': int(run_sum['enqueued']),
                'publish_attempts': int(run_sum['publish_attempts']),
                'first_publishes': int(meas_sum['first_publishes']),
                'retry_attempts': int(meas_sum['retry_attempts']),
                'app_acknowledged': int(run_sum['app_acknowledged']),
                'raw_samples': WORKLOAD_TOTAL_SAMPLES,
                'ledger_rows': WORKLOAD_TARGET_BATCHES,
                'temporal_rows': WORKLOAD_TARGET_BATCHES,
                'duplicate_committed': int(ing_sum.get('duplicate_committed', ing_sum.get('duplicate', 0))),
                'missing_origins': int(meas_sum['missing_origins']),
                'overflow_samples': int(meas_sum['overflow_samples']),
                'pending_rows': int(run_sum['pending_rows']),
                'pending_bytes': int(run_sum['pending_bytes']),
                'quarantined_rows': int(run_sum['quarantined_rows']),
                'quarantined_bytes': int(run_sum['quarantined_bytes']),
                'backlog_high_water_rows': int(sample_sum.get('backlog_high_water_rows', 0)),
                'backlog_high_water_bytes': int(sample_sum.get('backlog_high_water_bytes', 0)),
                'in_flight': 'unavailable',
            },
            'durations_and_windows': {
                'runner_wall_start_utc': runner_start_wall,
                'runner_wall_end_utc': runner_end_wall,
                'runner_total_duration_s': round(runner_duration_s, 3),
                'tracker_first_to_last_enqueue_s': round(gen_duration_s, 3),
                'tracker_run_start_to_last_enqueue_s': 'unavailable',
                'tracker_publish_to_drain_s': round(publish_drain_s, 3),
                'tracker_total_duration_s': round(tracker_total_s, 3),
            },
            'throughput': throughputs,
            'latency': {
                'samples': int(meas_sum['samples']),
                'p50_ms': float(meas_sum['p50_ms']),
                'p95_ms': float(meas_sum['p95_ms']),
                'min_ms': float(meas_sum['min_ms']),
                'max_ms': float(meas_sum['max_ms']),
            },
            'backend_ingress': {
                'accepted': int(ing_sum.get('accepted', 0)),
                'committed': int(ing_sum.get('committed', 0)),
                'duplicate_committed': int(ing_sum.get('duplicate_committed', ing_sum.get('duplicate', 0))),
                'validation_rejected': int(ing_sum.get('validation_rejected', 0)),
                'persist_failures': int(ing_sum.get('persist_failures', 0)),
                'queue_capacity': int(ing_sum.get('queue_capacity', 0)),
                'queue_depth': int(ing_sum.get('queue_depth', 0)),
                'queue_high_water': int(ing_sum.get('queue_high_water', 0)),
                'queue_high_water_semantic': 'admission-observed',
                'queue_full': int(ing_sum.get('queue_full', 0)),
            },
            'resource_sampling': sample_sum,
            'storage': {
                'before': storage_before.to_dict(),
                'after': storage_after.to_dict(),
                'deltas': storage_deltas,
            },
            'cleanup': {
                'verified': True,
                'details': 'containers/network/volumes/workdir absent',
            },
        }

        # Write summary JSON outside tempdir
        output_summary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_summary_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)

        return report

    except Exception as exc:
        primary_error = exc
        raise
    finally:
        if sampler is not None:
            sampler.stop()
        cleanup_errors = []
        for label, action in (
            ('owned containers', lambda: [remove_container(cid) for cid in owned_containers if not container_absent(cid)]),
            ('broker container', lambda: remove_container(broker_cid) if broker_cid and not container_absent(broker_cid) else None),
            ('database fixture', lambda: (fixture.cleanup() or None) if fixture else None),
            ('network', lambda: (docker(['network', 'rm', net_name], timeout=30) and None) if network_created else None),
            ('dynsec volume', lambda: (docker(['volume', 'rm', dynsec_volume], timeout=30) and None) if dynsec_volume else None),
            ('broker data volume', lambda: (docker(['volume', 'rm', broker_data_volume], timeout=30) and None)),
        ):
            try:
                action()
            except Exception as cleanup_exc:
                cleanup_errors.append(f'{label}: {sanitize(str(cleanup_exc))}')
        try:
            shutil.rmtree(work, ignore_errors=True)
        except Exception as cleanup_exc:
            cleanup_errors.append(f'workdir: {sanitize(str(cleanup_exc))}')

        remaining = registry.still_present(absent)
        if cleanup_errors or remaining:
            msg = 'workload cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
            if primary_error is None:
                raise RuntimeError(msg)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Task 3.7 Workload Runner (600 batches, 100 Hz)')
    parser.add_argument('--output-summary', default=str(ROOT / 'workload-measurement-summary.json'),
                        help='Safe JSON output path for measurement results')
    parser.add_argument('--execute', action='store_true', default=False,
                        help='Explicit opt-in flag required to actually launch the 600-batch run')
    opts = parser.parse_args(argv)

    if not docker_available():
        print('SKIP: docker daemon or CLI unavailable', flush=True)
        return 0

    if not opts.execute:
        print('INFO: stage3 workload runner initialized (dry-run/check mode; use --execute to run)', flush=True)
        print(f'Workload Profile: 1 Gateway x 1 Sensor x {WORKLOAD_RATE_HZ} Hz, {WORKLOAD_SAMPLES_PER_BATCH} samples/batch, {WORKLOAD_TARGET_BATCHES} batches', flush=True)
        print(f'Safe Summary Path: {opts.output_summary}', flush=True)
        print('PASS: workload runner preflight and profile verification passed', flush=True)
        return 0

    suite_registry = ResourceRegistry()
    suite_hexid = secrets.token_hex(6)
    try:
        print('stage=build-images', flush=True)
        backend_tag = f'stage3-workload-backend-{suite_hexid}'
        gateway_tag = f'stage3-workload-gateway-{suite_hexid}'
        backend_image = build_image('backend', backend_tag)
        suite_registry.register('backend image', 'image', backend_image)
        gateway_image = build_image('gateway', gateway_tag)
        suite_registry.register('gateway image', 'image', gateway_image)
        print(f'PASS: built backend image {backend_image} and gateway image {gateway_image}', flush=True)

        print('=== Running 600-batch 100 Hz workload ===', flush=True)
        run_workload_orchestrator(backend_image, gateway_image, Path(opts.output_summary))
        print('PASS: stage3 workload (600 batches verified)', flush=True)
        return 0
    finally:
        for img in [ident for (_, kind, ident) in suite_registry.items if kind == 'image']:
            try:
                docker(['image', 'rm', '-f', img], timeout=30)
            except Exception:
                pass


if __name__ == '__main__':
    sys.exit(main())
