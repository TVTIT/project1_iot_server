#!/usr/bin/env python3
"""Client-side P8 workspace lifecycle; never provisions or manages host A."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import socket
import sqlite3
import subprocess
import ssl
import struct
import sys
import time
import base64
import platform
import secrets
import re
import fcntl
import resource
import urllib.parse
import urllib.error
import urllib.error
import urllib.request

try:
    from scripts import paper_p8, paper_p8_bundle, paper_p8_e5_client
    from scripts.paper_p8_resources import GeneratorResourceSampler, write_generator_resources_csv
except ModuleNotFoundError:
    import paper_p8
    import paper_p8_bundle
    import paper_p8_e5_client
    from paper_p8_resources import GeneratorResourceSampler, write_generator_resources_csv

MAX_RESULT_FILES = 128
MAX_RESULT_FILE_BYTES = 8 << 20
MAX_RESULT_TOTAL_BYTES = 32 << 20
SAFE_RESULT_KEYS = {
    'status', 'implementation_status', 'experiment', 'arm', 'profile', 'mode',
    'run_id', 'started_at', 'finished_at', 'configured_phases', 'telemetry',
    'durable_gateway_count', 'commands', 'planned', 'sent', 'skipped', 'pending',
    'server_origin_latency', 'phase_observation_status', 'phase_events',
    'connection_attempts', 'connected_distinct_viewers', 'reconnections',
    'subscribed_distinct_viewers', 'receiving_distinct_viewers', 'received_messages',
    'received_bytes', 'duplicates', 'gaps', 'sequence_inversions', 'clock_skew',
    'pending_rows', 'pending_bytes', 'quarantined_rows', 'quarantined_bytes',
    'readiness', 'outbox_counts', 'phase_errors', 'cleanup_errors',
    'session', 'source_bundle_sha256', 'source_inventory', 'bound_manifest_sha256',
    'bound_gateway_environment_sha256', 'elapsed_transfer_wait_seconds',
    'required_token_lifetime_seconds', 'immutable_root', 'mutable_root',
    'binary_sha256', 'binary_os', 'binary_arch',
    'planned_stop', 'planned_stop_reason',
    'executor_receipts',
    'path', 'sha256', 'size_bytes',
}
MAX_VIEWER_PROBE_IDENTITIES = 64
MAX_RESULT_WALK = 128


def _private_json(path, max_bytes=1 << 20):
    path = Path(path)
    _reject_symlink_chain(path)
    st = path.lstat()
    if path.is_symlink() or not path.is_file() or st.st_mode & 0o077 or st.st_size > max_bytes:
        raise ValueError('private input rejected')
    return json.loads(path.read_text(encoding='utf-8'))


def _safe_number_map(value):
    if not isinstance(value, dict) or len(value) > 128:
        raise ValueError('result counter schema rejected')
    if any(not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', key)
           or type(item) not in (int, float, bool)
           for key, item in value.items()):
        raise ValueError('result counter schema rejected')
    return value


def _safe_phase_events(value):
    if not isinstance(value, list) or len(value) > 3100:
        raise ValueError('result phase event schema rejected')
    result = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError('result phase event schema rejected')
        event = {key: item[key] for key in ('child', 'event', 'elapsed_ns', 'child_elapsed_ns', 'observed_at_runner_monotonic_ns',
                                             'command_ordinal', 'clock_qualification') if key in item}
        if 'child' in event and (not isinstance(event['child'], str) or len(event['child']) > 160
                or not re.fullmatch(r'(?:paperload-telemetry:(?:[A-B]|telemetry|unique|replay|cold-replay|initial|warm_replay|initial-replay|warm-replay|samples-(?:1|100)|equal-samples-(?:1|100))(?:\.manifest)?\.json|paperload-viewer:viewer(?:\.manifest)?\.json|paperload-control:control(?:\.manifest)?\.json)', event['child'])):
            raise ValueError('result phase event schema rejected')
        for key in ('event', 'clock_qualification'):
            if key in event and (not isinstance(event[key], str) or len(event[key]) > 120
                                 or not re.fullmatch(r'[A-Za-z0-9_.; -]+', event[key])):
                raise ValueError('result phase event schema rejected')
        for key in ('elapsed_ns', 'observed_at_runner_monotonic_ns', 'command_ordinal'):
            if key in event and type(event[key]) is not int:
                raise ValueError('result phase event schema rejected')
        if 'child_elapsed_ns' in event and type(event['child_elapsed_ns']) is not int:
            raise ValueError('result phase event schema rejected')
        if 'child_elapsed_ns' in event and 'elapsed_ns' not in event:
            event['elapsed_ns'] = event.pop('child_elapsed_ns')
        else:
            event.pop('child_elapsed_ns', None)
        for key in ('configured_phases', 'counters'):
            if key in item:
                event[key] = _safe_number_map(item[key]) if key == 'configured_phases' else _safe_counter_tree(item[key])
        result.append(event)
    return result


def _safe_counter_tree(value, depth=0):
    if depth > 3 or not isinstance(value, dict) or len(value) > 128:
        raise ValueError('result counter schema rejected')
    output = {}
    for key, item in value.items():
        if not isinstance(key, str) or len(key) > 80 or not re.fullmatch(r'[a-z0-9_]+', key):
            raise ValueError('result counter schema rejected')
        if type(item) in (int, float, bool):
            output[key] = item
        elif isinstance(item, dict):
            output[key] = _safe_counter_tree(item, depth + 1)
        elif key.endswith('_unavailable_reason') or key.endswith('_qualification'):
            if (not isinstance(item, str) or len(item) > 160
                    or '/' in item or '\\' in item):
                raise ValueError('result qualification field rejected')
            output[key] = item
        else:
            raise ValueError('result counter schema rejected')
    return output


EQUAL_SAMPLES_FIELDS = (
    'context_sha256', 'workload_type', 'logical_producer',
    'total_samples', 'sample_rate', 'duration_seconds', 'producer_sample_rate',
    'sample_interval_us', 'samples_per_message', 'expected_samples', 'expected_messages',
    'generated_samples', 'generated_messages', 'offered_unique_samples', 'offered_unique_messages',
    'application_acks', 'offered_unique_bytes', 'attempted_bytes',
    'schedule_misses', 'scheduling_lag_max_ns', 'completed_on_time', 'sampling_qualification'
)


def _safe_equal_samples(value, *, role=None):
    if not isinstance(value, dict) or set(value) != set(EQUAL_SAMPLES_FIELDS):
        raise ValueError('equal_samples schema rejected')
    safe = {}
    context_sha256 = value['context_sha256']
    if not isinstance(context_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', context_sha256):
        raise ValueError('equal_samples context sha256 rejected')
    safe['context_sha256'] = context_sha256

    workload_type = value['workload_type']
    if workload_type != 'equal_samples':
        raise ValueError('equal_samples workload type rejected')
    safe['workload_type'] = workload_type

    logical_producer = value['logical_producer']
    if logical_producer not in ('A', 'B'):
        raise ValueError('equal_samples logical producer rejected')
    if role in ('A', 'B') and logical_producer != role:
        raise ValueError('equal_samples logical producer mismatch')
    safe['logical_producer'] = logical_producer

    total_samples = value['total_samples']
    if type(total_samples) is not int or not 0 < total_samples <= 60000:
        raise ValueError('equal_samples total_samples rejected')
    safe['total_samples'] = total_samples

    sample_rate = value['sample_rate']
    if type(sample_rate) is not int or not 2 <= sample_rate <= 200:
        raise ValueError('equal_samples sample_rate rejected')
    safe['sample_rate'] = sample_rate

    duration_seconds = value['duration_seconds']
    if type(duration_seconds) is not int or not 10 <= duration_seconds <= 120:
        raise ValueError('equal_samples duration_seconds rejected')
    safe['duration_seconds'] = duration_seconds

    producer_sample_rate = value['producer_sample_rate']
    if type(producer_sample_rate) is not int or not 1 <= producer_sample_rate <= 100:
        raise ValueError('equal_samples producer_sample_rate rejected')
    safe['producer_sample_rate'] = producer_sample_rate

    sample_interval_us = value['sample_interval_us']
    if type(sample_interval_us) is not int or not 0 < sample_interval_us <= (1 << 53):
        raise ValueError('equal_samples sample_interval_us rejected')
    safe['sample_interval_us'] = sample_interval_us

    samples_per_message = value['samples_per_message']
    if type(samples_per_message) is not int or samples_per_message not in (1, 100):
        raise ValueError('equal_samples samples_per_message rejected')
    safe['samples_per_message'] = samples_per_message

    expected_samples = value['expected_samples']
    if type(expected_samples) is not int or not 0 < expected_samples <= 60000:
        raise ValueError('equal_samples expected_samples rejected')
    safe['expected_samples'] = expected_samples

    expected_messages = value['expected_messages']
    if type(expected_messages) is not int or not 0 < expected_messages <= 60000:
        raise ValueError('equal_samples expected_messages rejected')
    safe['expected_messages'] = expected_messages

    for key in ('generated_samples', 'generated_messages', 'offered_unique_samples', 'offered_unique_messages',
                'application_acks', 'offered_unique_bytes', 'attempted_bytes',
                'schedule_misses', 'scheduling_lag_max_ns'):
        val = value[key]
        if type(val) is not int or not 0 <= val <= (1 << 53):
            raise ValueError(f'equal_samples {key} counter rejected')
        safe[key] = val

    completed_on_time = value['completed_on_time']
    if type(completed_on_time) is not bool:
        raise ValueError('equal_samples completed_on_time rejected')
    safe['completed_on_time'] = completed_on_time

    sampling_qualification = value['sampling_qualification']
    if (not isinstance(sampling_qualification, str) or len(sampling_qualification) > 256
            or '/' in sampling_qualification or '\\' in sampling_qualification
            or sampling_qualification not in (
                'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only',
            )):
        raise ValueError('equal_samples sampling_qualification rejected')
    safe['sampling_qualification'] = sampling_qualification

    return safe


def _safe_workload(value, *, role=None):
    if not isinstance(value, dict) or value.get('status') not in ('COMPLETE', 'PARTIAL', 'FAILED'):
        raise ValueError('workload status/schema rejected')
    safe = {}
    scalar_fields = ('status', 'implementation_status', 'experiment', 'arm', 'profile', 'mode',
                     'run_id', 'started_at', 'finished_at', 'durability_mode',
                     'identity_sampling_qualification', 'phase_observation_status',
                     'latency_available', 'latency_unavailable_reason', 'planned_stop', 'planned_stop_reason')
    for key in scalar_fields:
        item = value.get(key)
        if item is None:
            continue
        if key == 'status' and item not in ('COMPLETE', 'PARTIAL', 'FAILED'):
            raise ValueError('workload status rejected')
        if key == 'experiment' and item not in ('E1', 'E2', 'E3', 'E4', 'E5'):
            raise ValueError('workload experiment rejected')
        if key == 'profile' and item not in ('pilot', 'official'):
            raise ValueError('workload profile rejected')
        if key == 'mode' and item not in ('local', 'remote'):
            raise ValueError('workload mode rejected')
        if key == 'phase_observation_status' and item not in (
                'observed', 'unavailable', 'not_applicable',
                'observed_process_local_elapsed; runner_pipe_observation_lag_unknown',
                'observed; maximum 9 command ordinals sampled; process-local clock',
                'observed; all command ordinals; process-local clock'):
            raise ValueError('workload phase status rejected')
        if key == 'durability_mode' and item not in ('synthetic-load', 'durable', ''):
            raise ValueError('workload durability mode rejected')
        if key == 'implementation_status' and item not in (
                'P8.4b telemetry synthetic-load; no durable restart assurance',
                'P8.4c viewer-only client; no control/durable/server orchestration',
                'P8.4.10 HTTPS REST control producer only; external durable simulator required'):
            raise ValueError('workload implementation field rejected')
        if key == 'identity_sampling_qualification' and item not in (
                'header_sha256_only; maximum 20 globally; not every payload; not a full-content audit',
                'header_sha256_only; max 20 globally and 1 per viewer; not a full-payload audit'):
            raise ValueError('workload identity qualification rejected')
        if key in ('status', 'experiment', 'arm', 'profile', 'mode', 'durability_mode',
                   'implementation_status', 'identity_sampling_qualification', 'phase_observation_status',
                   'latency_unavailable_reason', 'planned_stop_reason'):
            path_character_rejected = (key not in ('implementation_status', 'identity_sampling_qualification',
                'phase_observation_status') and ('/' in item or '\\' in item)) if isinstance(item, str) else False
            if not isinstance(item, str) or len(item) > 160 or path_character_rejected:
                raise ValueError('workload scalar field rejected')
        elif key == 'run_id':
            if not isinstance(item, str) or len(item) > 128 or not item.replace('-', '').replace('_', '').replace('.', '').isalnum():
                raise ValueError('workload run id rejected')
        elif key in ('started_at', 'finished_at'):
            if not isinstance(item, str) or len(item) > 64 or '/' in item or '\\' in item:
                raise ValueError('workload timestamp rejected')
        elif key in ('latency_available', 'planned_stop') and type(item) is not bool:
            raise ValueError('workload scalar field rejected')
        elif key == 'latency_unavailable_reason' and item not in ('publisher_origin_not_available_in_viewer_role', ''):
            raise ValueError('workload latency qualification rejected')
        safe[key] = item
    for key in ('telemetry', 'configured_phases', 'viewer'):
        if key in value:
            safe[key] = _safe_counter_tree(value[key])
    for key in ('phase_events',):
        if key in value:
            safe[key] = _safe_phase_events(value[key])
    if 'close_reasons' in value:
        safe['close_reasons'] = _safe_number_map(value['close_reasons'])
    if 'e4' in value:
        proof = value['e4']
        if (role != 'telemetry' or not isinstance(proof, dict)
                or set(proof) - {'mode', 'journal_entry_count', 'safe_header_samples',
                                 'sampling_qualification', 'ignored_valid_acks',
                                 'replay_application_acks'}):
            raise ValueError('E4 proof schema rejected')
        if proof.get('mode') not in ('unique', 'replay') or type(proof.get('journal_entry_count')) is not int:
            raise ValueError('E4 proof schema rejected')
        safe_proof = {'mode': proof['mode'], 'journal_entry_count': proof['journal_entry_count']}
        for key in ('ignored_valid_acks', 'replay_application_acks'):
            if key in proof:
                if type(proof[key]) is not int or proof[key] < 0:
                    raise ValueError('E4 proof schema rejected')
                safe_proof[key] = proof[key]
        if 'sampling_qualification' in proof:
            if proof['sampling_qualification'] not in (
                    'header_sha256_only; maximum 20 globally; not every payload; not a full-content audit',
                    'synthetic_load_only; not SQLite/durable assurance', ''):
                raise ValueError('E4 proof schema rejected')
            safe_proof['sampling_qualification'] = proof['sampling_qualification']
        headers = proof.get('safe_header_samples', [])
        if not isinstance(headers, list) or len(headers) > 20:
            raise ValueError('E4 proof schema rejected')
        safe_proof['safe_header_samples'] = []
        fields = ('topic', 'gateway_id', 'sensor_id', 'message_id', 'boot_id',
                  'first_sequence', 'sample_count', 'payload_sha256')
        for header in headers:
            if not isinstance(header, dict) or set(header) != set(fields):
                raise ValueError('E4 proof schema rejected')
            row = {key: header[key] for key in fields}
            if any(not isinstance(row[key], str) or not row[key] or len(row[key]) > 256
                   or '/' in row[key] or '\\' in row[key]
                   for key in fields if key not in ('sample_count', 'topic')):
                raise ValueError('E4 proof schema rejected')
            if (type(row['sample_count']) is not int or not 0 < row['sample_count'] <= 10000
                    or not re.fullmatch(r'[0-9a-f]{64}', row['payload_sha256'])
                    or row['topic'] != f"gateways/{row['gateway_id']}/telemetry/{row['sensor_id']}"):
                raise ValueError('E4 proof schema rejected')
            safe_proof['safe_header_samples'].append(row)
        safe['e4'] = safe_proof
    if 'identity_samples' in value:
        samples = value['identity_samples']
        if not isinstance(samples, list) or len(samples) > 20:
            raise ValueError('identity sample bounds rejected')
        safe['identity_samples'] = []
        fields = ('viewer_id', 'gateway_id', 'sensor_id', 'message_id', 'boot_id',
                  'first_sequence', 'sample_count', 'header_sha256')
        for sample in samples:
            if not isinstance(sample, dict) or set(sample) - set(fields):
                raise ValueError('identity sample schema rejected')
            row = {key: sample.get(key) for key in fields}
            for key in fields[:6]:
                empty_telemetry_viewer = key == 'viewer_id' and role == 'telemetry' and row[key] == ''
                if not isinstance(row[key], str) or (not row[key] and not empty_telemetry_viewer) or len(row[key]) > 160 \
                        or '/' in row[key] or '\\' in row[key]:
                    raise ValueError('identity sample field rejected')
            if (type(row['sample_count']) is not int or not 0 < row['sample_count'] <= 10000
                    or not isinstance(row['header_sha256'], str)
                    or not re.fullmatch(r'[0-9a-f]{64}', row['header_sha256'])):
                raise ValueError('identity sample digest/counter rejected')
            safe['identity_samples'].append(row)
    if 'viewers' in value:
        viewers = value['viewers']
        if not isinstance(viewers, list) or len(viewers) > 10000:
            raise ValueError('viewer result bound rejected')
        aggregate = {}
        safe_viewers = []
        for row in viewers:
            if not isinstance(row, dict):
                raise ValueError('viewer result schema rejected')
            viewer_id = row.get('id')
            if (not isinstance(viewer_id, str)
                    or not re.fullmatch(r'(?:viewer-[0-9]{5}|e4-viewer-[ab]|e5-viewer-[ab])', viewer_id)):
                raise ValueError('viewer result identity rejected')
            reason = row.get('close_reason', 'unspecified') or 'unspecified'
            if not isinstance(reason, str) or len(reason) > 80 or '/' in reason or '\\' in reason:
                raise ValueError('viewer close reason rejected')
            counts = aggregate.setdefault(reason, {'viewers': 0, 'events': 0, 'unique_events': 0,
                                                    'duplicates': 0, 'bytes': 0})
            counts['viewers'] += 1
            projected = {'id': viewer_id, 'close_reason': reason}
            for field in ('events', 'unique_events', 'duplicates', 'bytes'):
                number = row.get(field, 0)
                if type(number) is not int or number < 0:
                    raise ValueError('viewer result counters rejected')
                counts[field] += number
                projected[field] = number
            wrong_namespace = row.get('wrong_namespace', 0)
            if type(wrong_namespace) is not int or wrong_namespace < 0:
                raise ValueError('viewer result counters rejected')
            projected['wrong_namespace'] = wrong_namespace
            safe_viewers.append(projected)
        safe['viewer_summary_by_close_reason'] = aggregate
        safe['viewers'] = safe_viewers
    if 'commands' in value:
        commands = value['commands']
        if not isinstance(commands, list) or len(commands) > 10000:
            raise ValueError('command result bound rejected')
        keep = ('classification', 'admission_classification', 'freshness_qualification',
                'terminal_status', 'http_status', 'client_round_trip_ms', 'round_trip_measurement',
                'idempotency_key', 'command_id', 'sensor_id', 'gateway_id',
                'expected_desired_version', 'desired_version', 'frequency_hz',
                'sample_interval_us')
        safe['commands'] = []
        for row in commands:
            if not isinstance(row, dict):
                raise ValueError('command result schema rejected')
            projected = {key: row[key] for key in keep if key in row}
            for key, item in projected.items():
                if key == 'frequency_hz':
                    if type(item) not in (int, float) or not 0 < item <= 100:
                        raise ValueError('command frequency rejected')
                    projected[key] = str(item)
                    continue
                if key in ('sample_interval_us',) and type(item) is int and 0 < item <= (1 << 53):
                    continue
                if key == 'http_status' and type(item) is int and 100 <= item <= 599:
                    continue
                if key == 'client_round_trip_ms' and type(item) is int and 0 <= item <= 600000:
                    continue
                if not isinstance(item, str) or len(item) > 100:
                    raise ValueError('command result field rejected')
            for key in ('http_status', 'client_round_trip_ms'):
                if key in projected and type(projected[key]) is not int:
                    raise ValueError('command result counter rejected')
            safe['commands'].append(projected)
    if 'equal_samples' in value:
        eq = value['equal_samples']
        if eq is not None:
            safe['equal_samples'] = _safe_equal_samples(eq, role=role)
    for key in ('planned', 'sent', 'skipped', 'pending', 'connection_attempts',
                'connected_distinct_viewers', 'reconnections', 'subscribed_distinct_viewers',
                'receiving_distinct_viewers', 'unique_events', 'duplicates', 'wrong_namespace',
                'messages', 'bytes', 'socket_count', 'principal_count', 'effective_identity_window_per_viewer',
                'durable_gateway_count'):
        if key in value:
            if type(value[key]) is not int or value[key] < 0:
                raise ValueError('workload counter rejected')
            safe[key] = value[key]
    return safe


def _safe_runner(value):
    if not isinstance(value, dict) or value.get('status') not in ('COMPLETE', 'PARTIAL', 'FAILED'):
        raise ValueError('runner status/schema rejected')
    result = {'status': value['status']}
    for key in ('started_at_unix', 'finished_at_unix', 'durable_gateway_process_count',
                'durable_gateway_count', 'durable_sensor_connections', 'durable_sensor_count',
                'actual_stop_confirmed', 'timer_driven_gateway_workload'):
        item = value.get(key)
        if type(item) in (int, float, bool):
            result[key] = item
    for key in ('readiness', 'pending_outbox_readback'):
        rows = value.get(key)
        if isinstance(rows, list) and len(rows) <= 16:
            result[key] = []
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError('runner child result schema rejected')
                result[key].append({k: row[k] for k in (
                    'role', 'status', 'ready', 'exit_code', 'pending_rows', 'pending_bytes',
                    'quarantined_rows', 'quarantined_bytes') if k in row and
                    type(row[k]) in (str, int, bool)})
    observations = value.get('phase_observations')
    if isinstance(observations, dict):
        result['phase_observations'] = {
            'status': observations.get('status') if observations.get('status') in ('observed', 'unavailable', 'not_applicable') else 'unavailable',
            'events': _safe_phase_events(observations.get('events', [])),
        }
    resource = value.get('resource_sampling')
    if isinstance(resource, dict):
        result['resource_sampling'] = {
            'status': resource.get('status') if resource.get('status') in
            ('collected', 'not_collected_in_P8.4', 'unavailable') else 'unavailable',
        }
        for key, item in resource.items():
            if key != 'status' and type(item) in (int, float, bool):
                result['resource_sampling'][key] = item
    return result


def _safe_generator_resources(value):
    if not isinstance(value, dict):
        raise ValueError('generator_resources_schema_rejected')
    if value.get('schema_version') != 1:
        raise ValueError('generator_resources_version_rejected')
    if value.get('role') != 'generator_B':
        raise ValueError('generator_resources_role_rejected')
    if value.get('cpu_normalization') not in ('host_percent', 'core_percent'):
        raise ValueError('generator_resources_cpu_norm_rejected')

    host_cpus = value.get('host_cpus')
    if type(host_cpus) is not int or host_cpus <= 0:
        raise ValueError('generator_resources_host_cpus_rejected')
    clk_tck = value.get('clk_tck')
    if type(clk_tck) is not int or clk_tck <= 0:
        raise ValueError('generator_resources_clk_tck_rejected')

    safe = {
        'schema_version': 1,
        'role': 'generator_B',
        'cpu_normalization': value['cpu_normalization'],
        'cpu_normalization_description': str(value.get('cpu_normalization_description', ''))[:200],
        'host_cpus': host_cpus,
        'clk_tck': clk_tck,
    }

    scopes = value.get('scopes')
    if not isinstance(scopes, dict):
        raise ValueError('generator_resources_scopes_rejected')
    allowed_scope_keys = {'process_tree_scope', 'fd_scope', 'rss_scope', 'cpu_scope'}
    safe['scopes'] = {}
    for k, v in scopes.items():
        if k not in allowed_scope_keys or not isinstance(v, str) or len(v) > 200:
            raise ValueError('generator_resources_scope_field_rejected')
        safe['scopes'][k] = v

    units = value.get('units')
    if not isinstance(units, dict):
        raise ValueError('generator_resources_units_rejected')
    allowed_unit_keys = {'rss_bytes', 'cpu_percent', 'elapsed_seconds', 'interval_seconds',
                         'delta_ticks', 'process_count', 'current_fd_count'}
    safe['units'] = {}
    for k, v in units.items():
        if k not in allowed_unit_keys or not isinstance(v, str) or len(v) > 200:
            raise ValueError('generator_resources_unit_field_rejected')
        safe['units'][k] = v

    clock = value.get('clock')
    if not isinstance(clock, dict):
        raise ValueError('generator_resources_clock_rejected')
    mono_start = clock.get('monotonic_start')
    if mono_start is not None and (type(mono_start) not in (int, float) or isinstance(mono_start, bool) or not math.isfinite(mono_start)):
        raise ValueError('generator_resources_clock_mono_rejected')
    interval = clock.get('sample_interval_seconds')
    if type(interval) not in (int, float) or isinstance(interval, bool) or not math.isfinite(interval) or interval <= 0:
        raise ValueError('generator_resources_clock_interval_rejected')
    deadline = clock.get('deadline_seconds')
    if type(deadline) not in (int, float) or isinstance(deadline, bool) or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError('generator_resources_clock_deadline_rejected')
    max_snaps = clock.get('max_snapshots')
    if type(max_snaps) is not int or max_snaps <= 0 or max_snaps > 128:
        raise ValueError('generator_resources_clock_max_snaps_rejected')

    safe['clock'] = {
        'monotonic_start': float(mono_start) if mono_start is not None else None,
        'sample_interval_seconds': float(interval),
        'deadline_seconds': float(deadline),
        'max_snapshots': max_snaps,
    }

    summary = value.get('summary')
    if not isinstance(summary, dict):
        raise ValueError('generator_resources_summary_rejected')
    for num_field in ('total_samples', 'available_samples', 'unavailable_samples', 'cpu_available_samples'):
        v = summary.get(num_field)
        if type(v) is not int or v < 0:
            raise ValueError(f'generator_resources_summary_{num_field}_rejected')
    max_rss = summary.get('max_sample_rss_bytes')
    if max_rss is not None and (type(max_rss) is not int or max_rss < 0):
        raise ValueError('generator_resources_summary_max_rss_rejected')

    missing_reasons = summary.get('missing_reasons', [])
    if not isinstance(missing_reasons, list) or any(not isinstance(r, str) or len(r) > 100 for r in missing_reasons):
        raise ValueError('generator_resources_summary_missing_reasons_rejected')
    errors = summary.get('errors', [])
    if not isinstance(errors, list) or any(not isinstance(e, str) or len(e) > 100 for e in errors):
        raise ValueError('generator_resources_summary_errors_rejected')

    safe['summary'] = {
        'total_samples': summary['total_samples'],
        'available_samples': summary['available_samples'],
        'unavailable_samples': summary['unavailable_samples'],
        'cpu_available_samples': summary['cpu_available_samples'],
        'max_sample_rss_bytes': max_rss,
        'max_sample_rss_qualification': str(summary.get('max_sample_rss_qualification', ''))[:200],
        'fd_scope': str(summary.get('fd_scope', ''))[:100],
        'missing_reasons': sorted(missing_reasons),
        'errors': list(errors),
    }

    raw_samples = value.get('samples', [])
    if not isinstance(raw_samples, list) or len(raw_samples) > 128:
        raise ValueError('generator_resources_samples_count_rejected')
    allowed_sample_keys = {'sample_index', 'timestamp', 'elapsed_seconds', 'interval_seconds',
                    'available', 'cpu_percent', 'delta_ticks', 'rss_bytes', 'process_count',
                    'current_fd_count', 'reason', 'phase'}
    sanitized_samples = []
    for s in raw_samples:
        if not isinstance(s, dict) or set(s) - allowed_sample_keys:
            raise ValueError('generator_resources_sample_fields_rejected')
        sample = {k: s[k] for k in allowed_sample_keys if k in s}
        for num_key in ('elapsed_seconds', 'interval_seconds', 'cpu_percent', 'delta_ticks',
                        'rss_bytes', 'process_count', 'current_fd_count', 'sample_index'):
            v = sample.get(num_key)
            if v is not None:
                if type(v) not in (int, float) or isinstance(v, bool) or not math.isfinite(v):
                    raise ValueError('generator_resources_sample_non_finite_rejected')
        if sample.get('available') is not None and type(sample['available']) is not bool:
            raise ValueError('generator_resources_sample_available_rejected')
        if sample.get('reason') is not None and (not isinstance(sample['reason'], str) or len(sample['reason']) > 100):
            raise ValueError('generator_resources_sample_reason_rejected')
        if sample.get('phase') is not None and sample['phase'] not in ('sample', 'terminal'):
            raise ValueError('generator_resources_sample_phase_rejected')
        sanitized_samples.append(sample)

    safe['samples'] = sanitized_samples
    return safe


def _write_new(path, data):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise ValueError('output already exists')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def _workspace(workspace):
    supplied = Path(workspace).absolute()
    if any(part.is_symlink() for part in (supplied, *supplied.parents)):
        raise ValueError('workspace symlink path rejected')
    root = supplied.resolve(strict=True)
    if not root.is_dir() or not (root / 'runtime/binding.json').is_file():
        raise ValueError('bound workspace required')
    binding = _private_json(root / 'runtime/binding.json')
    source_inventory = binding.get('source_inventory')
    if (not isinstance(source_inventory, list) or not 1 <= len(source_inventory) <= 96
            or not re.fullmatch(r'[0-9a-f]{64}', binding.get('source_bundle_sha256', ''))):
        raise ValueError('binding provenance metadata rejected')
    seen_inputs = set()
    for row in source_inventory:
        rel = paper_p8_bundle._safe_ref(row.get('path'))
        if rel in seen_inputs or not re.fullmatch(r'[0-9a-f]{64}', row.get('sha256', '')):
            raise ValueError('binding input inventory rejected')
        seen_inputs.add(rel)
        path = root / 'inputs' / rel
        _reject_symlink_chain(path)
        data = _bounded_read(path, paper_p8_bundle.MAX_FILE_BYTES)
        if hashlib.sha256(data).hexdigest() != row.get('sha256'):
            raise ValueError('bound input provenance mismatch')
    bound_manifests = binding.get('bound_manifest_sha256')
    bound_envs = binding.get('bound_gateway_environment_sha256')
    if not isinstance(bound_manifests, dict) or not isinstance(bound_envs, dict):
        raise ValueError('binding runtime inventory rejected')
    for label, expected in bound_manifests.items():
        if not paper_p8_bundle.SAFE_ID.fullmatch(label) or not re.fullmatch(r'[0-9a-f]{64}', expected):
            raise ValueError('bound manifest inventory rejected')
        path = root / 'runtime/manifests' / f'{label}.json'
        if hashlib.sha256(_bounded_read(path, paper_p8_bundle.MAX_FILE_BYTES)).hexdigest() != expected:
            raise ValueError('bound manifest provenance mismatch')
    for label, expected in bound_envs.items():
        if not paper_p8_bundle.SAFE_ID.fullmatch(label) or not re.fullmatch(r'[0-9a-f]{64}', expected):
            raise ValueError('bound Gateway inventory rejected')
        path = root / 'runtime/gateway-env' / f'{label}.json'
        if hashlib.sha256(_bounded_read(path, 65536)).hexdigest() != expected:
            raise ValueError('bound Gateway provenance mismatch')
    durable_hash = binding.get('bound_durable_probe_sha256')
    if durable_hash is not None:
        if (not re.fullmatch(r'[0-9a-f]{64}', durable_hash)
                or hashlib.sha256(_bounded_read(root / 'runtime/durable-probe.json', 65536)).hexdigest() != durable_hash):
            raise ValueError('bound durable probe provenance mismatch')
    if binding.get('session', {}).get('experiment') == 'E5':
        e5 = binding.get('e5')
        if not isinstance(e5, dict):
            raise ValueError('bound e5 metadata missing')
        for key in ('context_sha256', 'phase_plan_sha256', 'dataset_sha256'):
            if not isinstance(e5.get(key), str) or not re.fullmatch(r'[0-9a-f]{64}', e5[key]):
                raise ValueError(f'bound e5 {key} rejected')
        ctx_path = root / 'runtime/e5-context.json'
        _reject_symlink_chain(ctx_path)
        ctx_data = _bounded_read(ctx_path, 65536)
        if hashlib.sha256(ctx_data).hexdigest() != e5['context_sha256']:
            raise ValueError('bound e5 context provenance mismatch')
        plan_path = root / 'runtime/e5-phase-plan.json'
        _reject_symlink_chain(plan_path)
        plan_data = _bounded_read(plan_path, 65536)
        if hashlib.sha256(plan_data).hexdigest() != e5['phase_plan_sha256']:
            raise ValueError('bound e5 phase plan provenance mismatch')
        dataset_path = root / 'inputs/datasets/dataset.jsonl'
        _reject_symlink_chain(dataset_path)
        dataset_data = _bounded_read(dataset_path, paper_p8_bundle.MAX_FILE_BYTES)
        if hashlib.sha256(dataset_data).hexdigest() != e5['dataset_sha256']:
            raise ValueError('bound e5 dataset provenance mismatch')
        metadata = _verified_bundle_metadata(root, binding)
        if metadata.get('session', {}).get('experiment') != 'E5':
            raise ValueError('bundle metadata experiment mismatch')
    spec_path = root / 'runtime/execution/run-spec.json'
    spec_data = _bounded_read(spec_path, 1 << 20)
    if hashlib.sha256(spec_data).hexdigest() != binding.get('bound_run_spec_sha256'):
        raise ValueError('bound run spec provenance mismatch')
    spec = json.loads(spec_data)
    for job in spec.get('paperload', []):
        manifest_path = Path(job.get('manifest', '')).absolute()
        if manifest_path.is_symlink() or not manifest_path.is_relative_to(root / 'runtime/manifests'):
            raise ValueError('bound manifest path rejected')
        if not Path(job.get('output_dir', '')).absolute().is_relative_to(root / 'outputs'):
            raise ValueError('bound workload output path rejected')
    for gateway in spec.get('gateways', []):
        env_path = Path(gateway.get('env_file', '')).absolute()
        if env_path.is_symlink() or not env_path.is_relative_to(root / 'runtime/gateway-env'):
            raise ValueError('bound Gateway environment path rejected')
    return root


def _verified_bundle_metadata(root, binding):
    """Read the actual bound bundle metadata and tie it to binding/input inventory."""
    path = root / 'runtime/bundle-metadata.json'
    _reject_symlink_chain(path)
    data = _bounded_read(path, 1 << 20)
    if hashlib.sha256(data).hexdigest() != binding.get('source_bundle_sha256'):
        raise ValueError('bound bundle metadata provenance mismatch')
    try:
        metadata = json.loads(data)
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError('bound bundle metadata rejected') from None
    if (not isinstance(metadata, dict) or metadata.get('schema_version') != 1
            or metadata.get('session') != binding.get('session')
            or not isinstance(metadata.get('workload'), dict)
            or not isinstance(metadata.get('inputs'), list)):
        raise ValueError('bound bundle metadata rejected')
    expected_inventory = [{'path': row.get('path'), 'sha256': row.get('sha256')}
                          for row in metadata['inputs'] if isinstance(row, dict)]
    if expected_inventory != binding.get('source_inventory'):
        raise ValueError('bound bundle metadata inventory mismatch')
    return metadata


def _validate_e4_phase_eligibility(root, binding, jobs, state, phase):
    """Fail closed on E4 sequencing and opt-in metadata from the verified bundle."""
    if phase == 'cold-replay':
        metadata = _verified_bundle_metadata(root, binding)
        workload = metadata.get('workload', {})
        labels = {Path(job.get('manifest', '')).stem for job in jobs if isinstance(job, dict)}
        if (state.get('last_completed_phase') != 'replay'
                or metadata.get('session', {}).get('experiment') != 'E4'
                or workload.get('experiment') != 'E4'
                or workload.get('cache_mode') != 'on'
                or 'cold-replay' not in labels):
            raise ValueError('E4 cold replay phase is not enabled or not next')
        return True
    if phase == 'expiry-replay':
        metadata = _verified_bundle_metadata(root, binding)
        workload = metadata.get('workload', {})
        labels = {Path(job.get('manifest', '')).stem for job in jobs if isinstance(job, dict)}
        if (state.get('last_completed_phase') != 'replay'
                or workload.get('experiment') != 'E4'
                or workload.get('cache_mode') != 'on'
                or workload.get('expiry_replay_enabled') is not True
                or 'expiry-replay' not in labels):
            raise ValueError('E4 expiry replay phase is not enabled or not next')
        return True
    if phase == 'replay' and state.get('last_completed_phase') != 'unique':
        raise ValueError('E4 replay phase is not next')
    if phase == 'unique' and state.get('last_completed_phase') is not None:
        raise ValueError('E4 unique phase already recorded')
    return True


def _reject_symlink_chain(path):
    current = Path(path).absolute()
    if any(part.is_symlink() for part in (current, *current.parents)):
        raise ValueError('symlink path rejected')


def _bounded_read(path, limit):
    path = Path(path)
    _reject_symlink_chain(path)
    st = path.lstat()
    if not path.is_file() or st.st_size > limit or st.st_mode & 0o077:
        raise ValueError('bounded private file rejected')
    with path.open('rb') as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError('bounded file limit exceeded')
    return data


def _file_sha256(path, max_bytes=256 << 20):
    path = Path(path).absolute()
    _reject_symlink_chain(path)
    st = path.lstat()
    if not path.is_file() or st.st_size > max_bytes:
        raise ValueError('binary file bound/path rejected')
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_endpoints(workspace, run_spec):
    spec = _private_json(run_spec)
    manifests = []
    for job in spec['paperload']:
        manifest = _private_json(job['manifest'], paper_p8_bundle.MAX_MANIFEST_BYTES)
        manifests.append(manifest)
    return manifests, spec


def _tls_context(manifest, ca_field):
    ca_file = manifest['tls'][ca_field]
    context = ssl.create_default_context(cafile=ca_file)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _tokens(manifest, role):
    identities = _private_json(manifest['identity_files'][role])
    if not isinstance(identities, dict) or not identities or len(identities) > MAX_VIEWER_PROBE_IDENTITIES:
        raise ValueError('bounded role identity map rejected')
    tokens = []
    for principal, token in sorted(identities.items()):
        if (not isinstance(principal, str) or not principal or not isinstance(token, str)
                or len(token) > 8192 or token.count('.') != 2):
            raise ValueError('role identity token map rejected')
        parts = token.split('.')
        if not all(parts) or any(not re.fullmatch(r'[A-Za-z0-9_-]+', part) for part in parts):
            raise ValueError('role identity token map rejected')
        try:
            decoded = [base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)) for part in parts[:2]]
            header, payload = (json.loads(item) for item in decoded)
        except (ValueError, UnicodeError, json.JSONDecodeError):
            raise ValueError('role identity token map rejected') from None
        if (not isinstance(header, dict) or not isinstance(payload, dict)
                or type(payload.get('exp')) not in (int, float)
                or not time.time() < payload['exp'] < time.time() + 86400 * 366):
            raise ValueError('role identity token map rejected')
        tokens.append(token)
    return tokens


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _https_get(manifest, role):
    endpoint = manifest['tls']['https_endpoint'].rstrip('/')
    origin = urllib.parse.urlsplit(endpoint)
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=_tls_context(manifest, 'https_ca_file')),
        _NoRedirect())
    for token in _tokens(manifest, role):
        request = urllib.request.Request(endpoint + '/v1/me/permissions',
                                         headers={'Authorization': 'Bearer ' + token})
        try:
            with opener.open(request, timeout=10) as response:
                final = urllib.parse.urlsplit(response.geturl())
                if (final.scheme != origin.scheme or final.hostname != origin.hostname
                        or final.port != origin.port or not 200 <= response.status < 300):
                    raise ValueError('REST authenticated read rejected')
        except urllib.error.HTTPError as exc:
            raise ValueError(f'REST probe rejected: HTTP {exc.code}') from None


def _websocket_handshake(manifest, role):
    endpoint = __import__('urllib.parse', fromlist=['urlsplit']).urlsplit(
        manifest['tls']['https_endpoint'])
    context = _tls_context(manifest, 'https_ca_file')
    port = endpoint.port or 443
    nonce = base64.b64encode(os.urandom(16)).decode('ascii')
    for token in _tokens(manifest, role):
        nonce = base64.b64encode(os.urandom(16)).decode('ascii')
        request = (
            'GET /v1/ws HTTP/1.1\r\n'
            f'Host: {endpoint.hostname}:{port}\r\n'
            'Connection: Upgrade\r\n'
            'Upgrade: websocket\r\n'
            'Sec-WebSocket-Version: 13\r\n'
            f'Sec-WebSocket-Key: {nonce}\r\n'
            'Sec-WebSocket-Protocol: iot.telemetry.v1\r\n'
            f'Authorization: Bearer {token}\r\n\r\n'
        ).encode('ascii')
        with socket.create_connection((endpoint.hostname, port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname=endpoint.hostname) as stream:
                stream.settimeout(10)
                stream.sendall(request)
                response = bytearray()
                while b'\r\n\r\n' not in response:
                    chunk = stream.recv(min(1024, 8192 - len(response)))
                    if not chunk or len(response) + len(chunk) > 8192:
                        raise ValueError('bounded WebSocket handshake response rejected')
                    response.extend(chunk)
                head = bytes(response).split(b'\r\n\r\n', 1)[0]
                lines = head.decode('iso-8859-1').split('\r\n')
                status = lines[0].split()
                headers = {}
                for line in lines[1:]:
                    key, sep, value = line.partition(':')
                    if not sep or key.lower() in headers:
                        raise ValueError('WebSocket handshake headers rejected')
                    headers[key.lower()] = value.strip()
                expected_accept = base64.b64encode(hashlib.sha1(
                    (nonce + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
                connection_tokens = {part.strip().lower() for part in
                                     headers.get('connection', '').split(',')}
                if (len(status) < 2 or status[1] != '101'
                        or headers.get('upgrade', '').lower() != 'websocket'
                        or 'upgrade' not in connection_tokens
                        or headers.get('sec-websocket-protocol') != 'iot.telemetry.v1'
                        or not secrets.compare_digest(headers.get('sec-websocket-accept', ''), expected_accept)):
                    raise ValueError('authenticated WebSocket handshake rejected')


def _mqtt_connect_probe(*, endpoint, ca_file, server_name, gateway):
    parsed = urllib.parse.urlsplit(endpoint)
    # P8 manifests use mqtts://; the existing Gateway client contract uses
    # Paho's ssl:// spelling for the same CA/hostname-verified TLS transport.
    if parsed.scheme not in ('mqtts', 'ssl') or not parsed.hostname or parsed.username or parsed.password:
        # Report only validation predicates; never echo endpoint, host or credentials.
        raise ValueError('MQTT TLS endpoint rejected: scheme=%s hostname=%s userinfo=%s' %
                         (parsed.scheme in ('mqtts', 'ssl'), bool(parsed.hostname),
                          bool(parsed.username or parsed.password)))
    port = parsed.port or 8883
    context = ssl.create_default_context(cafile=ca_file)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    with socket.create_connection((parsed.hostname, port), timeout=10) as raw:
        with context.wrap_socket(raw, server_hostname=server_name or parsed.hostname) as stream:
            username = gateway['username'].encode()
            password = gateway['password'].encode()
            if (not username or not password or len(username) > 65535
                    or len(password) > 65535):
                raise ValueError('MQTT probe credential bounds rejected')
            client_id = ('p8-probe-' + secrets.token_hex(12)).encode()
            payload = (struct.pack('!H', len(client_id)) + client_id + struct.pack('!H', len(username))
                       + username + struct.pack('!H', len(password)) + password)
            variable = b'\x00\x04MQTT\x04\xc2\x00\x0a'
            remaining = len(variable) + len(payload)
            encoded = bytearray()
            while True:
                digit = remaining % 128
                remaining //= 128
                encoded.append(digit | (0x80 if remaining else 0))
                if not remaining:
                    break
            stream.sendall(b'\x10' + bytes(encoded) + variable + payload)
            response = _read_exact(stream, 4)
            if response[0] != 0x20 or response[1] != 2 or response[2] & 0xfe or response[3] != 0:
                raise ValueError('MQTT authenticated CONNECT rejected')
            stream.sendall(b'\xe0\x00')


def _read_exact(stream, count):
    result = bytearray()
    while len(result) < count:
        chunk = stream.recv(count - len(result))
        if not chunk:
            raise ValueError('MQTT response ended early')
        result.extend(chunk)
    return bytes(result)


def _check_connection(workspace, run_spec):
    manifests, spec = _manifest_endpoints(workspace, run_spec)
    roles = {manifest['roles'][0] for manifest in manifests}
    rest_roles = [role for role in ('viewer', 'control') if role in roles]
    ws_checked = False
    for manifest in manifests:
        for role in rest_roles:
            if role in manifest['identity_files']:
                _https_get(manifest, role)
                if role == 'viewer':
                    _websocket_handshake(manifest, role)
                    ws_checked = True
                break
    mqtt_probes = {}
    for manifest in manifests:
        tls = manifest.get('tls', {})
        endpoint = tls.get('mqtt_tls_endpoint')
        ca_file = tls.get('mqtt_tls_ca_file')
        if not endpoint or not ca_file or manifest.get('roles') != ['telemetry']:
            continue
        for gateway_id, credential_ref in manifest.get('gateway_identity_files', {}).items():
            credential = _private_json(credential_ref)
            if (not isinstance(credential, dict) or set(credential) != {'username', 'password'}
                    or not all(isinstance(credential.get(field), str) and credential[field]
                               for field in ('username', 'password'))):
                raise ValueError('Gateway credential input rejected')
            key = (endpoint, credential.get('username'), hashlib.sha256(
                credential.get('password', '').encode()).hexdigest(), 'manifest', gateway_id)
            mqtt_probes[key] = {
                'endpoint': endpoint, 'ca_file': ca_file,
                'server_name': tls.get('mqtt_tls_server_name'),
                'gateway': credential, 'gateway_id': gateway_id,
            }
    for executor_index, gateway in enumerate(spec.get('gateways', [])):
        env = _private_json(gateway['env_file'])
        password_file = Path(env['GATEWAY_MQTT_PASSWORD_FILE'])
        password_stat = password_file.lstat()
        if (password_file.is_symlink() or not password_file.is_file()
                or password_stat.st_mode & 0o077 or password_stat.st_size > 4096):
            raise ValueError('Gateway password file rejected')
        password = password_file.read_text(encoding='utf-8')
        username = env['GATEWAY_MQTT_USERNAME']
        endpoint = env['GATEWAY_MQTT_BROKER_URL']
        ca_file = env['GATEWAY_MQTT_CA_FILE']
        server_name = env.get('GATEWAY_MQTT_SERVER_NAME')
        key = (endpoint, username, hashlib.sha256(password.encode()).hexdigest(),
               'executor', executor_index, env.get('GATEWAY_MQTT_CLIENT_ID'))
        mqtt_probes[key] = {'endpoint': endpoint, 'ca_file': ca_file,
                            'server_name': server_name,
                            'gateway': {'username': username, 'password': password}}
    if len(mqtt_probes) > 16:
        raise ValueError('MQTT connection probe identity bound exceeded')
    for probe in mqtt_probes.values():
        _mqtt_connect_probe(endpoint=probe['endpoint'], ca_file=probe['ca_file'],
                            server_name=probe['server_name'], gateway=probe['gateway'])
    return {'https_authenticated_get': bool(rest_roles), 'mqtt_connect_disconnect': bool(mqtt_probes),
            'websocket_authenticated_handshake': ws_checked,
            'mqtt_connections_checked': len(mqtt_probes),
            'telemetry_or_command_published': False}


def _export_results(workspace, output):
    root = _workspace(workspace)
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise ValueError('result output already exists')
    _reject_symlink_chain(output.absolute())
    binding = _private_json(root / 'runtime/binding.json')
    run_spec = _private_json(root / 'runtime/execution/run-spec.json')
    phase = None
    if binding.get('session', {}).get('experiment') == 'E4':
        phase_state = _private_json(root / 'runtime/e4-phase-state.json')
        phase = phase_state.get('last_completed_phase')
        if phase not in ('unique', 'replay', 'cold-replay', 'expiry-replay'):
            raise ValueError('E4 result phase unavailable')
        files = [(f'phases/{phase}/runner/run.json',
                  root / 'outputs/e4-phases' / phase / 'run.json', 'runner')]
        provenance_path = root / 'runtime/e4-phases' / phase / 'provenance.json'
        files.append((f'phases/{phase}/runner/provenance.json', provenance_path, 'provenance'))
    else:
        files = [('runner/run.json', root / 'outputs/run.json', 'runner')]
        files.append(('runner/provenance.json', root / 'runtime/runner-provenance.json', 'provenance'))
        gen_res_path = root / 'outputs/resources-generator.json'
        if gen_res_path.is_file():
            files.append(('runner/resources-generator.json', gen_res_path, 'generator_resources'))
    for job in run_spec.get('paperload', []):
        role = job.get('role')
        if role not in ('viewer', 'telemetry', 'control'):
            raise ValueError('result role rejected')
        label = Path(job['manifest']).stem
        result_role = label if binding['session'].get('experiment') in ('E2', 'E3', 'E4') else role
        if result_role not in ('A', 'B', 'viewer', 'telemetry', 'control', 'unique', 'replay', 'cold-replay', 'expiry-replay'):
            raise ValueError('result role rejected')
        output_dir = (root / 'outputs/e4-phases' / phase / Path(label).stem
                      if phase and label in (phase, 'viewer') else Path(job['output_dir']).absolute())
        if output_dir.is_symlink() or not output_dir.is_relative_to(root / 'outputs'):
            raise ValueError('workload result path outside workspace')
        result_path = (f'phases/{phase}/workloads/{result_role}/workload.json' if phase
                       else f'workloads/{result_role}/workload.json')
        files.append((result_path, output_dir / 'workload.json', role))
    if binding['session'].get('experiment') == 'E3':
        executor_rows = []
        for index, job in enumerate(run_spec.get('gateways', [])):
            env_path = Path(job.get('env_file', '')).absolute()
            if env_path.is_symlink() or not env_path.is_relative_to(root / 'runtime'):
                raise ValueError('executor environment path outside workspace')
            env = _private_json(env_path)
            db_path = Path(env.get('GATEWAY_OUTBOX_PATH', '')).absolute()
            if db_path.is_symlink() or not db_path.resolve().is_relative_to((root / 'outputs').resolve()):
                raise ValueError('executor database path outside workspace')
            if not db_path.is_file():
                raise ValueError('executor database missing')
            import sqlite3
            try:
                with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as conn:
                    config = conn.execute('SELECT desired_version,sample_interval_us,applied_at FROM sensor_configuration WHERE id=1').fetchone()
                    receipts = conn.execute('SELECT command_id,command_hash,desired_version,sample_interval_us,status,result_bytes FROM command_receipts ORDER BY desired_version').fetchall()
                    pending = conn.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
            except sqlite3.Error:
                raise ValueError('executor receipt database schema rejected') from None
            projected = []
            for command_id, command_hash, version, interval, status, result_bytes in receipts:
                result = None
                if result_bytes is not None:
                    raw = bytes(result_bytes)
                    if len(raw) > 8192:
                        raise ValueError('executor receipt result too large')
                    try:
                        parsed = json.loads(raw)
                    except (UnicodeError, json.JSONDecodeError):
                        raise ValueError('executor receipt result invalid') from None
                    report = parsed.get('reported_state') if isinstance(parsed, dict) else None
                    result = {'command_id': parsed.get('command_id'), 'desired_version': str(parsed.get('desired_version')),
                              'status': parsed.get('status'), 'sample_interval_us': report.get('sample_interval_us') if isinstance(report, dict) else None,
                              'result_sha256': hashlib.sha256(raw).hexdigest()}
                projected.append({'command_id': command_id, 'command_hash': command_hash,
                                  'desired_version': str(version), 'sample_interval_us': interval,
                                  'status': status, 'result': result})
            executor_rows.append({'executor_index': index, 'configuration': ({'desired_version': str(config[0]),
                'sample_interval_us': config[1], 'applied_at': config[2]} if config else None),
                'receipts': projected, 'pending_outbox': pending})
        if len(executor_rows) != 2:
            raise ValueError('E3 requires exactly two executor databases')
        files.append(('runner/executor-receipts.json', None, 'executor_receipts', executor_rows))
    if len(files) > MAX_RESULT_FILES:
        raise ValueError('result inventory bounds rejected')
    entries = []
    total_bytes = 0
    for entry in files:
        rel, path, kind = entry[:3]
        if kind == 'executor_receipts':
            data = json.dumps(entry[3], sort_keys=True, separators=(',', ':')).encode()
            entries.append((rel, data))
            total_bytes += len(data)
            continue
        if not path.exists():
            continue
        _reject_symlink_chain(path)
        st = path.lstat()
        if not path.is_file() or st.st_size > MAX_RESULT_FILE_BYTES:
            raise ValueError('result file bound/path rejected')
        total_bytes += st.st_size
        if total_bytes > MAX_RESULT_TOTAL_BYTES:
            raise ValueError('result total size limit exceeded')
        data = _bounded_read(path, MAX_RESULT_FILE_BYTES)
        try:
            parsed = json.loads(data)
        except (UnicodeError, json.JSONDecodeError):
            raise ValueError('unsafe result format rejected') from None
        if kind == 'provenance':
            if (not isinstance(parsed, dict) or set(parsed) != {'binary_sha256', 'binary_os', 'binary_arch'}
                    or not re.fullmatch(r'[0-9a-f]{64}', parsed.get('binary_sha256', ''))):
                raise ValueError('runner provenance rejected')
            safe = parsed
        elif kind == 'runner':
            safe = _safe_runner(parsed)
        elif kind == 'generator_resources':
            safe = _safe_generator_resources(parsed)
        else:
            safe = _safe_workload(parsed, role=kind)
        entries.append((rel, json.dumps(safe, sort_keys=True, separators=(',', ':')).encode()))
    present_roles = {(rel.split('/')[3] if phase else rel.split('/')[1])
                     for rel, _ in entries if '/workloads/' in rel or rel.startswith('workloads/')}
    expected_roles = ({phase, 'viewer'} if phase else
                      {Path(job['manifest']).stem for job in run_spec.get('paperload', [])}
                      if binding['session'].get('experiment') in ('E2', 'E3')
                      else {job['role'] for job in run_spec.get('paperload', [])})
    expected_runner = f'phases/{phase}/runner/run.json' if phase else 'runner/run.json'
    if expected_runner not in {rel for rel, _ in entries}:
        raise ValueError('runner result artifact is required before export')
    if not entries or len(entries) > MAX_RESULT_FILES or sum(len(data) for _, data in entries) > MAX_RESULT_TOTAL_BYTES:
        raise ValueError('result inventory bounds rejected')
    output.mkdir(mode=0o700, parents=True)
    os.chmod(output, 0o700)
    try:
        inventory = []
        for rel, data in entries:
            target_rel = 'results/' + rel
            _write_new(output / target_rel, data)
            inventory.append({'path': target_rel, 'size_bytes': len(data),
                              'sha256': hashlib.sha256(data).hexdigest()})
        summary = {'schema_version': 1, 'session': binding['session'],
                    'source_bundle_sha256': binding['source_bundle_sha256'],
                    'source_inventory': binding['source_inventory'],
                    'bound_manifest_sha256': binding['bound_manifest_sha256'],
                    'bound_gateway_environment_sha256': binding['bound_gateway_environment_sha256'],
                    'bound_run_spec_sha256': binding['bound_run_spec_sha256'],
                    'binary': next((json.loads(data) for rel, data in entries
                                    if rel.endswith('/runner/provenance.json') or rel == 'runner/provenance.json'), {}), 'files': inventory,
                    'runner_status': next((json.loads(data).get('status') for rel, data in entries
                                           if rel.endswith('/runner/run.json') or rel == 'runner/run.json'), 'unknown'),
                   'expected_workload_roles': sorted(expected_roles),
                   'present_workload_roles': sorted(present_roles),
                    'missing_workload_roles': sorted(expected_roles - present_roles),
                     **({'phase': phase, 'phase_state_sha256': hashlib.sha256(
                         _bounded_read(_phase_state_path(root), 65536)).hexdigest(),
                         'journal_sha256': _private_json(_phase_state_path(root)).get('last_journal_sha256'),
                          'phase_run_id': next((_private_json(Path(job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES).get('run_id')
                              for job in run_spec.get('paperload', [])
                              if Path(job['manifest']).stem == phase), None)}
                        if phase else {}),
                   'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
        _write_new(output / 'results.json', json.dumps(summary, sort_keys=True).encode())
    except BaseException:
        shutil.rmtree(output)
        raise
    return {'result_bundle': str(output), 'files': len(entries)}


def _export_durable_results(workspace, output, stage):
    if stage not in ('durable-fault', 'durable-reopen'):
        raise ValueError('durable_result_stage_rejected')
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    probe_path = root / 'runtime/durable-probe.json'
    if (binding.get('session', {}).get('experiment') != 'E4'
            or not binding.get('bound_durable_probe_sha256')
            or hashlib.sha256(_bounded_read(probe_path, 65536)).hexdigest()
                != binding['bound_durable_probe_sha256']):
        raise ValueError('durable_result_binding_missing')
    label = stage.removeprefix('durable-')
    proof_path = root / 'runtime' / f'e4-durable-{label}.json'
    proof = _private_json(proof_path)
    expected = ('INTENTIONAL_FAULT_STOP' if label == 'fault' else 'DRAINED')
    if proof.get('stage') != label or proof.get('classification') != expected or proof.get('joined') is not True:
        raise ValueError('durable_result_proof_incomplete')
    data = json.dumps(proof, sort_keys=True, separators=(',', ':')).encode()
    if len(data) > 65536:
        raise ValueError('durable_result_size_limit')
    if output.exists() or output.is_symlink():
        raise ValueError('output already exists')
    output.mkdir(mode=0o700, parents=True)
    os.chmod(output, 0o700)
    try:
        _write_new(output / 'durable-proof.json', data)
        binding_projection = {key: binding[key] for key in ('schema_version','session',
            'source_bundle_sha256','source_inventory','bound_manifest_sha256',
            'bound_gateway_environment_sha256','bound_run_spec_sha256',
            'bound_durable_probe_sha256','elapsed_transfer_wait_seconds',
            'required_token_lifetime_seconds') if key in binding}
        projection_bytes = json.dumps(binding_projection, sort_keys=True,
            separators=(',', ':')).encode()
        summary = {'schema_version': 1, 'experiment': 'E4', 'phase': stage,
            'session_id': binding['session']['session_id'],
            'run_id': _private_json(probe_path)['run_id'],
            'source_bundle_sha256': binding['source_bundle_sha256'],
            'source_inventory': binding['source_inventory'],
            'bound_durable_probe_sha256': binding['bound_durable_probe_sha256'],
            'binding': binding_projection,
            'binding_sha256': hashlib.sha256(projection_bytes).hexdigest(),
            'proof': {'path': 'durable-proof.json', 'size_bytes': len(data),
                      'sha256': hashlib.sha256(data).hexdigest()},
            'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
        _write_new(output / 'results.json', json.dumps(summary, sort_keys=True).encode())
    except BaseException:
        shutil.rmtree(output)
        raise
    return {'result_bundle': str(output), 'stage': stage, 'files': 1}


def _conflict_helpers():
    """Load repository helpers for standalone CLI use without a test PYTHONPATH."""
    tests_path = str(Path(__file__).resolve().parent / 'tests')
    if tests_path not in sys.path:
        sys.path.insert(0, tests_path)
    import paper_p8_e4_support as e4
    import paper_p5a_support as p5a
    return e4, p5a


def _select_bound_conflict_target(base, base_path, root, gw_id, sensor_id):
    """Derive one authorized target and its verified bound credential from E4's source manifest."""
    source_targets = base.get('targets')
    selected_target = {'gateway_id': gw_id, 'sensor_id': sensor_id}
    if (not isinstance(source_targets, list) or selected_target not in source_targets
            or any(not isinstance(target, dict) or set(target) != {'gateway_id', 'sensor_id'}
                   for target in source_targets)):
        raise ValueError('p8_e4_conflict_target_missing')
    def bind_ref(value):
        if not isinstance(value, str) or not value:
            raise ValueError('p8_e4_conflict_bound_file_reference_rejected')
        path = Path(value)
        path = path if path.is_absolute() else base_path.parent / path
        _reject_symlink_chain(path)
        path = path.resolve(strict=True)
        if not path.is_relative_to(root.resolve()) or path.is_symlink() or not path.is_file():
            raise ValueError('p8_e4_conflict_bound_file_reference_rejected')
        return str(path)

    source_credentials = base.get('gateway_identity_files')
    if not isinstance(source_credentials, dict) or gw_id not in source_credentials:
        raise ValueError('p8_e4_conflict_credential_rejected')
    selected_credential_path = bind_ref(source_credentials[gw_id])
    selected_credential = _private_json(Path(selected_credential_path))
    if (not isinstance(selected_credential, dict)
            or selected_credential.get('username') != gw_id
            or not isinstance(selected_credential.get('password'), str)
            or not selected_credential['password']):
        raise ValueError('p8_e4_conflict_credential_rejected')
    return selected_target, selected_credential_path


def _run_b_conflicts_paperload(root, gw_id, sensor_id, original_payload, cases_info, binary):
    """Use the pinned paperload executable and its existing bounded MQTT/TLS replay."""
    _clean_env, preflight = paper_p8._clean_env, paper_p8.preflight
    e4, _ = _conflict_helpers()
    root = _workspace(root)
    jobs, _, _, _ = paper_p8._read_spec(str(root / 'runtime/execution/run-spec.json'), None, None)
    job = next((item for item in jobs if item['role'] == 'telemetry'
                and Path(item['manifest']).name == 'unique.json'), None)
    if job is None:
        raise ValueError('p8_e4_conflict_bound_manifest_missing')
    base_path = Path(job['manifest'])
    base = _private_json(base_path)
    executable = Path(binary).absolute()
    if (base.get('experiment') != 'E4' or base.get('roles') != ['telemetry']
            or executable.is_symlink() or not executable.is_file()):
        raise ValueError('p8_e4_conflict_inputs_rejected')
    binary_hash = _file_sha256(executable)
    if not re.fullmatch(r'[0-9a-f]{64}', binary_hash):
        raise ValueError('p8_e4_conflict_binary_provenance_rejected')
    selected_target, selected_credential_path = _select_bound_conflict_target(
        base, base_path, root, gw_id, sensor_id)

    def bind_ref(value):
        if not isinstance(value, str) or not value:
            raise ValueError('p8_e4_conflict_bound_file_reference_rejected')
        path = Path(value)
        path = path if path.is_absolute() else base_path.parent / path
        _reject_symlink_chain(path)
        path = path.resolve(strict=True)
        if not path.is_relative_to(root.resolve()) or path.is_symlink() or not path.is_file():
            raise ValueError('p8_e4_conflict_bound_file_reference_rejected')
        return str(path)

    header, journal_payload = e4.extract_e4_journal_first_record(root / 'outputs/e4-journal.sqlite')
    if (journal_payload != original_payload
            or (header['gateway_id'], header['sensor_id']) != (gw_id, sensor_id)):
        raise ValueError('p8_e4_conflict_source_journal_mismatch')
    result_root = root / 'outputs/e4-conflicts'
    result_root.mkdir(mode=0o700, exist_ok=False)
    cases = [('positive-control', original_payload, True)] + [
        (name, base64.b64decode(cases_info[name]['raw_payload_b64'], validate=True), False)
        for name in ('same_id_changed_payload', 'different_first_sequence_overlap')]
    outcomes = {}
    for label, payload, positive in cases:
        msg = json.loads(payload)
        directory = result_root / label
        directory.mkdir(mode=0o700)
        journal = directory / 'journal.jsonl'
        record = {'topic': f'gateways/{gw_id}/telemetry/{sensor_id}', 'gateway_id': gw_id,
            'sensor_id': sensor_id, 'message_id': msg['message_id'], 'boot_id': msg['boot_id'],
            'first_sequence': str(msg['first_sequence']), 'sample_count': msg['sample_count'],
            'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'payload': base64.b64encode(payload).decode('ascii')}
        journal_bytes = json.dumps(record, separators=(',', ':')).encode() + b'\n'
        _write_new(journal, journal_bytes)
        manifest = json.loads(json.dumps(base))
        if (not manifest.get('tls', {}).get('mqtt_tls_endpoint')
                or not manifest.get('tls', {}).get('mqtt_tls_ca_file')):
            raise ValueError('p8_e4_conflict_bound_identity_rejected')
        manifest['targets'] = [selected_target]
        manifest['topology'] = dict(manifest['topology'], gateways=1, sensors_per_gateway=1)
        manifest['gateway_identity_files'] = {gw_id: selected_credential_path}
        manifest['limits'] = dict(manifest['limits'], workers=1, connections=1,
                                  pending_messages=1)
        manifest['identity_files'] = {key: bind_ref(value)
                                      for key, value in manifest['identity_files'].items()}
        manifest['tls']['https_ca_file'] = bind_ref(manifest['tls']['https_ca_file'])
        manifest['tls']['mqtt_tls_ca_file'] = bind_ref(manifest['tls']['mqtt_tls_ca_file'])
        manifest['bundle_path'] = str(root.resolve())
        manifest['samples_per_message'] = msg['sample_count']
        manifest.update(run_id=f"{base['run_id']}-e4-{label}",
            phases={'warmup_seconds': 0, 'measure_seconds': 1, 'drain_seconds': 4, 'cooldown_seconds': 0},
            traffic={'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': 0},
            limits=dict(base['limits'], workers=1, connections=1, retries=2, wall_seconds=8, pending_messages=1,
                        pending_bytes=max(65536, len(payload) * 4)),
            e4={'mode': 'replay', 'journal_path': str(journal), 'max_messages': 1,
                'max_bytes': len(journal_bytes), 'replay_count': 1})
        manifest_path, output = directory / 'manifest.json', directory / 'workload'
        _write_new(manifest_path, json.dumps(manifest, sort_keys=True).encode())
        preflight(str(executable), str(manifest_path), str(output))
        stdout_path, stderr_path = directory / 'stdout.log', directory / 'stderr.log'
        start = time.monotonic()
        try:
            def limit_child_output():
                resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 65536))
            with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
                os.chmod(stdout_path, 0o600); os.chmod(stderr_path, 0o600)
                proc = subprocess.run([str(executable), '--manifest', str(manifest_path),
                    '--output-dir', str(output), '--run'], cwd=root, env=_clean_env(),
                    stdout=stdout, stderr=stderr, timeout=12, check=False,
                    preexec_fn=limit_child_output)
        except subprocess.TimeoutExpired:
            raise ValueError('p8_e4_conflict_paperload_timeout') from None
        except OSError:
            raise ValueError('p8_e4_conflict_paperload_launch_failed') from None
        elapsed = time.monotonic() - start
        if max(stdout_path.stat().st_size, stderr_path.stat().st_size) > 65536:
            raise ValueError('p8_e4_conflict_output_limit')
        workload_path = output / 'workload.json'
        if workload_path.is_symlink() or not workload_path.is_file():
            raise ValueError('p8_e4_conflict_workload_missing')
        workload = _private_json(workload_path)
        counts = workload.get('telemetry', {})
        offered, acks, pending = (counts.get('replay_offered'), counts.get('replay_application_acks'),
                                  counts.get('pending_messages'))
        published, attempts, retries = (counts.get('broker_pubacks'), counts.get('publish_attempts'),
                                         counts.get('retries'))
        phase_events = workload.get('phase_events', [])
        joined_event = any(isinstance(event, dict) and event.get('event') == 'workers_joined'
                           for event in phase_events)
        if (type(offered) is not int or type(acks) is not int or type(pending) is not int
                or type(published) is not int or type(attempts) is not int or type(retries) is not int
                or offered != 1 or elapsed > 12 or not joined_event
                or workload.get('cleanup_errors') not in (None, [])):
            raise ValueError('p8_e4_conflict_workload_counters_rejected')
        if positive:
            valid = (proc.returncode == 0 and workload.get('status') == 'COMPLETE' and acks == 1
                     and pending == 0 and attempts >= 1 and published >= 1)
        else:
            valid = (proc.returncode == 4 and workload.get('status') == 'PARTIAL' and acks == 0
                     and pending == 1 and workload.get('primary_error') is None
                     and attempts >= 1 and published >= 1 and 1 <= retries <= 2)
        if not valid:
            raise ValueError('p8_e4_conflict_transport_result_rejected')
        outcomes[label] = {'returncode': proc.returncode, 'status': workload.get('status'),
            'replay_offered': offered, 'replay_application_acks': acks, 'pending_messages': pending,
            'publish_attempts': attempts, 'broker_pubacks': published, 'retries': retries,
            'joined': joined_event, 'observation_seconds': round(elapsed, 3), 'binary_sha256': binary_hash,
            'binary_os': sys.platform, 'binary_arch': platform.machine(),
            'workload_sha256': _file_sha256(workload_path)}
    return {name: {'message_id': cases_info[name]['message_id'],
        'payload_sha256': cases_info[name]['payload_sha256'], 'schema_valid': True,
        'positive_control_ack_count': 1, 'conflict_ack_count': 0,
        'raw_payload_b64': cases_info[name]['raw_payload_b64'],
        'status': 'EXPECTED_REJECTED_NO_ACK', 'positive_control': outcomes['positive-control'],
        'transport': outcomes[name]} for name in ('same_id_changed_payload', 'different_first_sequence_overlap')}


def _export_conflict_probe(workspace, output):
    e4, _ = _conflict_helpers()
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E4':
        raise ValueError('conflict_probe_is_e4_only')
    state = _private_json(_phase_state_path(root))
    if not state.get('unique_validated'):
        raise ValueError('conflict_probe_requires_validated_unique')
    journal_path = root / 'outputs/e4-journal.sqlite'
    header, original_payload = e4.extract_e4_journal_first_record(journal_path)
    cases = e4.generate_e4_conflict_payloads(original_payload,
        gateway_id=header['gateway_id'], sensor_id=header['sensor_id'])
    manifest_file = root / 'inputs/manifests/unique.json'
    if manifest_file.is_file():
        manifest = _private_json(manifest_file, paper_p8_bundle.MAX_MANIFEST_BYTES)
        mqtt_endpoint = manifest.get('tls', {}).get('mqtt_tls_endpoint')
    else:
        mqtt_endpoint = binding['session'].get('endpoints', {}).get('mqtt_tls', '')

    run_id = binding['session'].get('run_id') or (binding['session'].get('run_ids') and binding['session']['run_ids'][0]) or binding['session']['session_id']
    probe_data = {
        'schema_version': 1,
        'experiment': 'E4',
        'phase': 'conflicts',
        'session_id': binding['session']['session_id'],
        'run_id': run_id,
        'mqtt_endpoint': mqtt_endpoint,
        'original_header': header,
        'original_payload_sha256': hashlib.sha256(original_payload).hexdigest(),
        'original_payload_b64': base64.b64encode(original_payload).decode('ascii'),
        'cases': {
            name: {
                'message_id': json.loads(case_bytes)['message_id'],
                'payload_sha256': hashlib.sha256(case_bytes).hexdigest(),
                'raw_payload_b64': base64.b64encode(case_bytes).decode('ascii')
            }
            for name, case_bytes in cases.items()
        }
    }
    probe_path = root / 'runtime/conflict-probe.json'
    _replace_private_json(probe_path, probe_data)

    data = json.dumps(probe_data, sort_keys=True, separators=(',', ':')).encode()
    if output.exists() or output.is_symlink():
        raise ValueError('output already exists')
    output.mkdir(mode=0o700, parents=True)
    os.chmod(output, 0o700)
    try:
        _write_new(output / 'conflict-probe.json', data)
        binding_projection = {key: binding[key] for key in ('schema_version','session',
            'source_bundle_sha256','source_inventory','bound_manifest_sha256',
            'bound_gateway_environment_sha256','bound_run_spec_sha256',
            'bound_durable_probe_sha256','elapsed_transfer_wait_seconds',
            'required_token_lifetime_seconds') if key in binding}
        projection_bytes = json.dumps(binding_projection, sort_keys=True,
            separators=(',', ':')).encode()
        summary = {'schema_version': 1, 'experiment': 'E4', 'phase': 'conflicts',
            'session_id': binding['session']['session_id'],
            'run_id': run_id,
            'source_bundle_sha256': binding['source_bundle_sha256'],
            'source_inventory': binding['source_inventory'],
            'binding': binding_projection,
            'binding_sha256': hashlib.sha256(projection_bytes).hexdigest(),
            'proof': {'path': 'conflict-probe.json', 'size_bytes': len(data),
                      'sha256': hashlib.sha256(data).hexdigest()},
            'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
        _write_new(output / 'results.json', json.dumps(summary, sort_keys=True).encode())
    except BaseException:
        shutil.rmtree(output)
        raise
    return {'conflict_probe': str(output), 'cases': len(cases)}


def _run_conflict_stage(workspace, *, binary=None, transport=None):
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E4':
        raise ValueError('conflict_stage_is_e4_only')
    probe_path = root / 'runtime/conflict-probe.json'
    if not probe_path.is_file():
        raise ValueError('conflict_probe_missing')
    probe = _private_json(probe_path)
    result_path = root / 'runtime/e4-conflicts.json'
    if result_path.exists() or result_path.is_symlink():
        raise ValueError('conflict_stage_result_already_exists')
    lock_fd = _e4_phase_lock(root)
    try:
        header = probe['original_header']
        gw_id = header['gateway_id']
        sensor_id = header['sensor_id']
        original_payload = base64.b64decode(probe['original_payload_b64'])

        if transport is not None:
            cred_file = root / f'inputs/private/gateways/{gw_id}.json'
            password = _private_json(cred_file)['password']
            cases_results = transport(gw_id, sensor_id, password, original_payload, probe['cases'])
        else:
            if binary is None:
                raise ValueError('p8_e4_conflict_binary_required')
            cases_results = _run_b_conflicts_paperload(root, gw_id, sensor_id,
                original_payload, probe['cases'], binary)

        proof = {
            'status': 'PASS',
            'phase': 'conflicts',
            'cases': cases_results,
            'original_header': header,
            'original_payload_sha256': probe['original_payload_sha256'],
            'joined': True
        }
        _write_new(result_path, json.dumps(proof, sort_keys=True).encode())
    finally:
        os.close(lock_fd)
    return {'status': 'PASS', 'cases': list(cases_results.keys())}


def _export_conflict_results(workspace, output):
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E4':
        raise ValueError('conflict_result_is_e4_only')
    proof_path = root / 'runtime/e4-conflicts.json'
    proof = _private_json(proof_path)
    if proof.get('status') != 'PASS' or proof.get('joined') is not True:
        raise ValueError('conflict_result_proof_incomplete')
    data = json.dumps(proof, sort_keys=True, separators=(',', ':')).encode()
    if len(data) > 131072:
        raise ValueError('conflict_result_size_limit')
    if output.exists() or output.is_symlink():
        raise ValueError('output already exists')
    output.mkdir(mode=0o700, parents=True)
    os.chmod(output, 0o700)
    try:
        _write_new(output / 'conflicts-proof.json', data)
        binding_projection = {key: binding[key] for key in ('schema_version','session',
            'source_bundle_sha256','source_inventory','bound_manifest_sha256',
            'bound_gateway_environment_sha256','bound_run_spec_sha256',
            'bound_durable_probe_sha256','elapsed_transfer_wait_seconds',
            'required_token_lifetime_seconds') if key in binding}
        projection_bytes = json.dumps(binding_projection, sort_keys=True,
            separators=(',', ':')).encode()
        run_id = binding['session'].get('run_id') or (binding['session'].get('run_ids') and binding['session']['run_ids'][0]) or binding['session']['session_id']
        summary = {'schema_version': 1, 'experiment': 'E4', 'phase': 'conflicts',
            'session_id': binding['session']['session_id'],
            'run_id': run_id,
            'source_bundle_sha256': binding['source_bundle_sha256'],
            'source_inventory': binding['source_inventory'],
            'binding': binding_projection,
            'binding_sha256': hashlib.sha256(projection_bytes).hexdigest(),
            'proof': {'path': 'conflicts-proof.json', 'size_bytes': len(data),
                      'sha256': hashlib.sha256(data).hexdigest()},
            'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
        _write_new(output / 'results.json', json.dumps(summary, sort_keys=True).encode())
    except BaseException:
        shutil.rmtree(output)
        raise
    return {'result_bundle': str(output), 'phase': 'conflicts', 'files': 1}


def _complete_e3_poststop_drain(root, spec, artifact, gateway_binary, *, expected_jobs=3):
    """Bounded reopen of E3 executors when clean producer completion leaves queued ACK rows."""
    if (artifact.get('status') != 'PARTIAL' or artifact.get('primary_error') is not None
            or artifact.get('cleanup_errors') != [] or artifact.get('actual_stop_confirmed') is not True):
        return None
    children = artifact.get('children')
    counts = artifact.get('pending_outbox_readback')
    if (not isinstance(children, list) or not children
            or any(not isinstance(row, dict) or row.get('status') not in ('complete', 'stopped', 'stopped_after_kill') for row in children)
            or not isinstance(counts, list) or len(counts) != 2
            or any(not isinstance(row, dict) or row.get('status') != 'readback' or row.get('quarantined_rows') != 0 for row in counts)
            or not any(isinstance(row, dict) and type(row.get('pending_rows')) is int and row['pending_rows'] > 0 for row in counts)):
        return None
    jobs = spec.get('paperload', [])
    if expected_jobs is not None and len(jobs) != expected_jobs:
        return None
    for job in jobs:
        if expected_jobs is None and job.get('role') != 'control':
            continue
        result_path = Path(job['output_dir']) / 'workload.json'
        if result_path.is_symlink() or not result_path.is_file():
            return None
        result = json.loads(result_path.read_text(encoding='utf-8'))
        if result.get('status') != 'COMPLETE':
            return None
    if len(spec.get('gateways', [])) != 2:
        return None
    from paper_p8 import _gateway_environment, _clean_env, _safe_path
    tests_path = str(Path(__file__).resolve().parent / 'tests')
    if tests_path not in sys.path:
        sys.path.insert(0, tests_path)
    from stage3_uplink_runner import parse_run_summary
    reports = []
    for index, gateway in enumerate(spec['gateways']):
        env_path = Path(gateway['env_file'])
        env = _gateway_environment(str(env_path))
        db_path = Path(env['GATEWAY_OUTBOX_PATH']).absolute()
        if (db_path.is_symlink() or not db_path.is_file()
                or not db_path.resolve().is_relative_to(root.resolve())):
            raise ValueError('e3_executor_database_path_rejected')
        uri = f'file:{db_path}?mode=ro'
        try:
            with sqlite3.connect(uri, uri=True) as conn:
                config_before = conn.execute('SELECT desired_version,sample_interval_us FROM sensor_configuration WHERE id=1').fetchone()
                pending_before = conn.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
        except sqlite3.Error:
            raise ValueError('e3_executor_database_read_failed') from None
        if pending_before == 0:
            reports.append({'executor': index + 1, 'drain': 'not_needed', 'pending_before': 0, 'pending_after': 0})
            continue
        if pending_before > 500:
            raise ValueError('e3_pending_outbox_bound_exceeded')
        env['GATEWAY_MQTT_CLIENT_ID'] += '_p8_e3_poststop_drain'
        try:
            result = subprocess.run([_safe_path(gateway_binary, must_exist=True),
                                     'run', '--batches', '1', '--completion-timeout', '30s'],
                cwd=root, env=_clean_env(env), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=45, check=False)
        except subprocess.TimeoutExpired:
            raise ValueError('e3_poststop_drain_timeout') from None
        except OSError:
            raise ValueError('e3_poststop_drain_launch_failed') from None
        summary = parse_run_summary(result.stdout)
        try:
            with sqlite3.connect(uri, uri=True) as conn:
                config_after = conn.execute('SELECT desired_version,sample_interval_us FROM sensor_configuration WHERE id=1').fetchone()
                pending_after = conn.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
        except sqlite3.Error:
            raise ValueError('e3_executor_database_read_failed') from None
        if (result.returncode != 0 or summary.get('completed') != 'true'
                or int(summary.get('app_acknowledged', '-1')) < pending_before
                or int(summary.get('pending_rows', '-1')) != 0 or pending_after != 0
                or config_after != config_before):
            raise ValueError('e3_poststop_drain_verification_failed')
        reports.append({'executor': index + 1, 'drain': 'verified', 'pending_before': pending_before,
                        'app_acknowledged': int(summary['app_acknowledged']), 'pending_after': pending_after,
                        'configuration_unchanged': True})
    return reports if any(row.get('drain') == 'verified' for row in reports) else None


def _fresh_executor_outbox_readback(root, gateways, gateway_binary, timeout=10):
    """Obtain a fresh read-only bounded outbox readback for declared executors using Gateway CLI."""
    if not isinstance(gateways, list) or len(gateways) != 2:
        return None
    from paper_p8 import _gateway_environment, _clean_env, _safe_path
    fresh_counts = []
    outputs_root = (Path(root) / 'outputs').resolve()
    for idx, gateway in enumerate(gateways):
        env_path = Path(gateway.get('env_file', ''))
        if not env_path.is_file():
            return None
        env = _gateway_environment(str(env_path))
        raw_db = env.get('GATEWAY_OUTBOX_PATH', '')
        if not raw_db:
            return None
        db_path = Path(raw_db).absolute()
        if (db_path.is_symlink() or not db_path.is_file()
                or not db_path.resolve().is_relative_to(outputs_root)):
            return None
        try:
            res = subprocess.run([_safe_path(gateway_binary, must_exist=True), 'count'],
                                 shell=False, env=_clean_env(env), capture_output=True,
                                 text=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if res.returncode != 0:
            return None
        found = dict(re.findall(r'(pending_rows|pending_bytes|quarantined_rows|quarantined_bytes)=(\d+)', res.stdout))
        if len(found) != 4:
            return None
        p_rows = int(found['pending_rows'])
        p_bytes = int(found['pending_bytes'])
        q_rows = int(found['quarantined_rows'])
        q_bytes = int(found['quarantined_bytes'])
        if p_rows != 0 or p_bytes != 0 or q_rows != 0 or q_bytes != 0:
            return None
        fresh_counts.append({
            'path_hash': hashlib.sha256(str(db_path).encode()).hexdigest(),
            'rows': None,
            'status': 'readback',
            'pending_rows': p_rows,
            'pending_bytes': p_bytes,
            'quarantined_rows': q_rows,
            'quarantined_bytes': q_bytes,
        })
    if len(fresh_counts) != 2:
        return None
    return fresh_counts


def _replace_private_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.tmp-' + secrets.token_hex(8))
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, sort_keys=True, separators=(',', ':'))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    except BaseException:
        try: temp.unlink()
        except OSError: pass
        raise


def _phase_state_path(root):
    return root / 'runtime/e4-phase-state.json'


def _select_e4_phase_jobs(jobs, phase, *, replay_validated=False):
    if phase not in ('unique', 'replay', 'cold-replay', 'expiry-replay'):
        raise ValueError('phase_rejected')
    if phase in ('replay', 'cold-replay', 'expiry-replay') and not replay_validated:
        raise ValueError('unique_phase_not_validated')
    if not isinstance(jobs, list):
        raise ValueError('E4 phase input rejected')
    by_label = {}
    for job in jobs:
        label = Path(job.get('manifest', '')).stem if isinstance(job, dict) else ''
        if label in by_label:
            raise ValueError('E4 phase input rejected')
        by_label[label] = job
    expected_labels = {'unique', 'replay', 'viewer'}
    if 'cold-replay' in by_label:
        expected_labels.add('cold-replay')
    if 'expiry-replay' in by_label:
        expected_labels.add('expiry-replay')
    if set(by_label) != expected_labels:
        raise ValueError('E4 phase input rejected')
    if phase not in by_label:
        raise ValueError('E4 phase input rejected')
    selected = [by_label[phase], by_label['viewer']]
    if ([item.get('role') for item in selected].count('telemetry') != 1
            or [item.get('role') for item in selected].count('viewer') != 1):
        raise ValueError('E4 phase role rejected')
    return selected


def _validate_e4_phase_result(phase, telemetry, viewer):
    if (not isinstance(telemetry, dict) or telemetry.get('experiment') != 'E4'
            or not isinstance(telemetry.get('e4'), dict)
            or telemetry['e4'].get('mode') != phase):
        raise ValueError('E4 telemetry phase result rejected')
    counts = telemetry.get('telemetry', {})
    if phase == 'unique':
        if (telemetry['e4'].get('journal_entry_count') != 20
                or counts.get('generated_unique') != 20 or counts.get('application_acks') != 20
                or counts.get('ignored_valid_acks') != 1 or counts.get('retries', 0) < 1
                or len(telemetry['e4'].get('safe_header_samples', [])) < 10):
            raise ValueError('E4 unique phase result rejected')
        if (viewer.get('status') != 'COMPLETE' or viewer.get('subscribed_distinct_viewers') != 2
                or viewer.get('connected_distinct_viewers') != 2
                or viewer.get('receiving_distinct_viewers') != 2 or viewer.get('unique_events', 0) < 1):
            raise ValueError('E4 unique viewer proof rejected')
    else:
        if (telemetry['e4'].get('journal_entry_count') != 10
                or counts.get('replay_offered') != 10
                or counts.get('replay_application_acks') != 10
                or len(telemetry['e4'].get('safe_header_samples', [])) != 10):
            raise ValueError('E4 replay phase result rejected')
        if (viewer.get('status') != 'COMPLETE' or viewer.get('subscribed_distinct_viewers') != 2
                or viewer.get('connected_distinct_viewers') != 2
                or viewer.get('unique_events', 0) != 0):
            raise ValueError('E4 replay viewer proof rejected')


def _e4_phase_lock(root):
    path = root / 'runtime/e4-run.lock'
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    os.fchmod(fd, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise ValueError('E4 workspace run already active') from None
    return fd


def _durable_helpers():
    """Load repository helpers for standalone CLI use without a test PYTHONPATH."""
    tests_path = str(Path(__file__).resolve().parent / 'tests')
    if tests_path not in sys.path:
        sys.path.insert(0, tests_path)
    from paper_p8_e4_durable_client import validate_inputs, run_stage
    from stage3_uplink_runner import gateway_env
    return validate_inputs, run_stage, gateway_env


def _jwt_expiry_seconds(token):
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        raw = parts[1]
        raw += '=' * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(raw.encode('ascii')))
        exp = payload.get('exp')
        return float(exp) if exp is not None else None
    except Exception:
        return None


def _sanitize_and_write_resource_samples(phase_dir, output_dir):
    json_path = phase_dir / 'resources-generator.json'
    if not json_path.is_file():
        return []
    _reject_symlink_chain(json_path)
    data = _private_json(json_path)
    if not isinstance(data, dict):
        raise ValueError('resource_data_rejected')
    # E5 viewer resources are wrapped with availability/scope metadata; the
    # sampler's bounded report (not wrapper paths or process internals) owns samples.
    report = data.get('report', data)
    if not isinstance(report, dict):
        raise ValueError('resource_report_rejected')
    raw_samples = report.get('samples', [])
    if not isinstance(raw_samples, list) or len(raw_samples) > 128:
        raise ValueError('resource_samples_count_rejected')
    allowed_keys = {'sample_index', 'timestamp', 'elapsed_seconds', 'interval_seconds',
                    'available', 'cpu_percent', 'delta_ticks', 'rss_bytes', 'process_count',
                    'current_fd_count', 'reason', 'phase'}
    sanitized = []
    for s in raw_samples:
        if not isinstance(s, dict) or set(s) - allowed_keys:
            raise ValueError('resource_sample_fields_rejected')
        sample = {k: s[k] for k in allowed_keys if k in s}
        for num_key in ('elapsed_seconds', 'interval_seconds', 'cpu_percent', 'delta_ticks',
                        'rss_bytes', 'process_count', 'current_fd_count', 'sample_index'):
            v = sample.get(num_key)
            if v is not None:
                if type(v) not in (int, float) or isinstance(v, bool) or not math.isfinite(v):
                    raise ValueError('resource_sample_non_finite_rejected')
        if sample.get('available') is not None and type(sample['available']) is not bool:
            raise ValueError('resource_sample_available_rejected')
        if sample.get('phase') is not None and sample['phase'] not in ('sample', 'terminal'):
            raise ValueError('resource_sample_phase_rejected')
        sanitized.append(sample)
    write_generator_resources_csv({'samples': sanitized}, output_dir / 'resources-generator.csv')
    return sanitized


def _project_sqlite_executors(root, run_spec, declared_keys, commands):
    gateways = run_spec.get('gateways', [])
    if len(gateways) != 2:
        raise ValueError('e5_control_sqlite_executors_count_mismatch')
    if not isinstance(commands, list) or len(commands) != len(declared_keys):
        raise ValueError('e5_control_commands_count_mismatch')

    # Build and validate exact 1:1 mapping between declared idempotency keys and backend command IDs
    declared_set = set(declared_keys)
    if len(declared_set) != len(declared_keys):
        raise ValueError('e5_declared_keys_duplicate')
    command_by_id = {}
    seen_idempotency_keys = set()
    for cmd in commands:
        if not isinstance(cmd, dict):
            raise ValueError('e5_control_command_schema_invalid')
        idem = cmd.get('idempotency_key')
        cmd_id = cmd.get('command_id')
        if not isinstance(idem, str) or idem not in declared_set or idem in seen_idempotency_keys:
            raise ValueError('e5_control_idempotency_key_invalid')
        if not isinstance(cmd_id, str) or not paper_p8_bundle.SAFE_ID.fullmatch(cmd_id) or cmd_id in command_by_id:
            raise ValueError('e5_control_backend_command_id_invalid')
        if cmd.get('classification') != 'succeeded' or cmd.get('terminal_status') != 'succeeded':
            raise ValueError('e5_control_command_unverified')
        seen_idempotency_keys.add(idem)
        command_by_id[cmd_id] = cmd

    if seen_idempotency_keys != declared_set:
        raise ValueError('e5_control_declared_keys_incomplete')

    outputs_root = (root / 'outputs').resolve()
    executor_rows = []
    total_receipts = 0
    seen_receipt_command_ids = set()
    for index, job in enumerate(gateways):
        env_path = Path(job.get('env_file', '')).absolute()
        _reject_symlink_chain(env_path)
        if not env_path.resolve().is_relative_to((root / 'runtime').resolve()):
            raise ValueError('executor_environment_path_escape_rejected')
        env = _private_json(env_path)
        raw_db = env.get('GATEWAY_OUTBOX_PATH', '')
        if not raw_db:
            raise ValueError('gateway_outbox_path_missing')
        db_path = Path(raw_db).absolute()
        _reject_symlink_chain(db_path)
        resolved_db = db_path.resolve(strict=True)
        if not resolved_db.is_relative_to(outputs_root) or resolved_db.is_symlink() or not resolved_db.is_file():
            raise ValueError('executor_database_path_escape_rejected')
        if resolved_db.stat().st_size > 16 << 20:
            raise ValueError('executor_database_too_large')
        try:
            with sqlite3.connect(f'file:{resolved_db}?mode=ro', uri=True) as conn:
                config_row = conn.execute(
                    'SELECT desired_version, sample_interval_us, applied_at FROM sensor_configuration WHERE id=1'
                ).fetchone()
                receipt_rows = conn.execute(
                    'SELECT command_id, command_hash, desired_version, sample_interval_us, status, result_bytes '
                    'FROM command_receipts ORDER BY desired_version LIMIT 8'
                ).fetchall()
                pending_count = conn.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
        except sqlite3.Error as exc:
            raise ValueError('executor_receipt_database_schema_rejected') from exc

        if not config_row or any(v is None for v in config_row):
            raise ValueError('executor_configuration_missing')
        if pending_count != 0:
            raise ValueError('e5_control_sqlite_executor_has_pending_rows')
        if not (1 <= len(receipt_rows) <= 4):
            raise ValueError('executor_receipts_count_invalid')
        total_receipts += len(receipt_rows)

        projected_receipts = []
        for cmd_id, cmd_hash, version, interval, st, result_bytes in receipt_rows:
            if not isinstance(cmd_id, str) or cmd_id not in command_by_id or cmd_id in seen_receipt_command_ids:
                raise ValueError('executor_command_id_not_declared')
            seen_receipt_command_ids.add(cmd_id)
            if st != 'completed':
                raise ValueError('executor_receipt_status_unverified')
            if result_bytes is None:
                raise ValueError('executor_receipt_result_missing')
            raw = bytes(result_bytes)
            if not (1 <= len(raw) <= 8192):
                raise ValueError('executor_receipt_result_too_large')
            try:
                parsed = json.loads(raw)
            except (ValueError, UnicodeDecodeError) as exc:
                raise ValueError('executor_receipt_result_invalid_json') from exc
            if not isinstance(parsed, dict) or parsed.get('status') != 'succeeded':
                raise ValueError('executor_receipt_business_status_unverified')
            if parsed.get('command_id') != cmd_id:
                raise ValueError('executor_receipt_result_command_id_mismatch')
            if str(parsed.get('desired_version')) != str(version):
                raise ValueError('executor_receipt_result_version_mismatch')
            report = parsed.get('reported_state')
            if not isinstance(report, dict) or report.get('sample_interval_us') != interval:
                raise ValueError('executor_receipt_result_interval_mismatch')

            cmd = command_by_id[cmd_id]
            expected_version = str(int(cmd['expected_desired_version']) + 1)
            if str(version) != expected_version:
                raise ValueError('executor_receipt_expected_version_mismatch')

            res_dict = {
                'command_id': parsed.get('command_id'),
                'desired_version': str(parsed.get('desired_version')),
                'status': parsed.get('status'),
                'sample_interval_us': report.get('sample_interval_us'),
                'result_sha256': hashlib.sha256(raw).hexdigest(),
            }
            projected_receipts.append({
                'command_id': cmd_id,
                'command_hash': cmd_hash,
                'desired_version': str(version),
                'sample_interval_us': interval,
                'status': st,
                'result': res_dict,
            })
        executor_rows.append({
            'executor_index': index,
            'configuration': {
                'desired_version': str(config_row[0]),
                'sample_interval_us': config_row[1],
                'applied_at': config_row[2],
            },
            'receipts': projected_receipts,
            'pending_outbox': pending_count,
        })
    if total_receipts != 4 or len(seen_receipt_command_ids) != 4:
        raise ValueError('total_executor_receipts_count_mismatch')
    return executor_rows


def _e5_phase_lock(root):
    path = root / 'runtime/e5-phase.lock'
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        raise ValueError('E5 phase already locked by another runner')


def _local_source_fingerprint():
    try:
        from scripts.tests.paper_p8_support import _source_fingerprint
        return _source_fingerprint(Path(__file__).resolve().parent.parent)
    except Exception:
        return {'revision': 'local-b', 'dirty': False, 'valid': True}


def _run_e5_viewers(workspace, binary, gateway_binary=None, deadline_seconds=None, poll_seconds=0.1):
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E5':
        raise ValueError('e5_viewers_requires_e5_workspace')

    if not binary:
        raise ValueError('paperload_binary_required')
    if not gateway_binary:
        raise ValueError('gateway_binary_required')

    paperload_path = Path(binary).absolute()
    if paperload_path.is_symlink() or not paperload_path.is_file() or not os.access(paperload_path, os.X_OK):
        raise ValueError('paperload_binary_rejected')
    paperload_sha = _file_sha256(paperload_path)

    gateway_path = Path(gateway_binary).absolute()
    if gateway_path.is_symlink() or not gateway_path.is_file() or not os.access(gateway_path, os.X_OK):
        raise ValueError('gateway_binary_rejected')
    gateway_sha = _file_sha256(gateway_path)
    combined_sha = hashlib.sha256(paperload_path.read_bytes() + gateway_path.read_bytes()).hexdigest()

    spec = _private_json(root / 'runtime/execution/run-spec.json')
    viewer_job = next((j for j in spec.get('paperload', []) if j.get('role') == 'viewer'), None)
    if not viewer_job:
        raise ValueError('viewer_job_missing')
    viewer_manifest_path = Path(viewer_job['manifest']).absolute()
    viewer_manifest = _private_json(viewer_manifest_path, paper_p8_bundle.MAX_MANIFEST_BYTES)
    viewer_manifest_sha = _file_sha256(viewer_manifest_path)
    viewer_run_id = viewer_manifest.get('run_id')
    if not viewer_run_id:
        raise ValueError('viewer_run_id_missing')

    # Canonical trusted binding projection
    binding_projection = {key: binding[key] for key in (
        'schema_version', 'session', 'source_bundle_sha256', 'source_inventory',
        'bound_manifest_sha256', 'bound_gateway_environment_sha256', 'bound_run_spec_sha256',
        'elapsed_transfer_wait_seconds', 'required_token_lifetime_seconds', 'e5',
        'immutable_root', 'mutable_root') if key in binding}
    projection_bytes = json.dumps(binding_projection, sort_keys=True, separators=(',', ':')).encode()
    binding_digest = hashlib.sha256(projection_bytes).hexdigest()

    correlation = {
        'session_id': binding['session']['session_id'],
        'run_id': viewer_run_id,
        'binding_digest': binding_digest,
        'manifest_sha256': viewer_manifest_sha,
        'binary_sha256': paperload_sha,
    }

    phase_plan = _private_json(root / 'runtime/e5-phase-plan.json')
    configured_deadline = phase_plan.get('viewer_lifecycle', {}).get('deadline_seconds', 900)
    if deadline_seconds is not None:
        if deadline_seconds > configured_deadline:
            raise ValueError(f'deadline_seconds ({deadline_seconds}) exceeds declared lifecycle deadline ({configured_deadline})')
        effective_deadline = int(deadline_seconds)
    else:
        effective_deadline = configured_deadline
    if not 1 <= effective_deadline <= 3600:
        raise ValueError('deadline_invalid')

    # Verify token lifetime and forbid admin token on B before launch
    viewer_ident_rel = viewer_manifest.get('identity_files', {}).get('viewer')
    if not viewer_ident_rel:
        raise ValueError('viewer_identity_file_missing')
    viewer_ident_path = root / 'runtime' / viewer_ident_rel
    if not viewer_ident_path.is_file():
        viewer_ident_path = root / 'inputs' / viewer_ident_rel
    _reject_symlink_chain(viewer_ident_path)
    viewer_tokens = _private_json(viewer_ident_path)
    now = time.time()
    for uid, tok in viewer_tokens.items():
        if uid == 'admin' or 'admin' in uid:
            raise ValueError('admin_token_forbidden_on_b')
        exp = _jwt_expiry_seconds(tok)
        if exp is not None and exp - now < effective_deadline:
            raise ValueError(f'viewer token for {uid} near expiry (remaining {exp - now:.1f}s < deadline {effective_deadline}s)')

    control_dir = root / 'runtime/e5-holder-control'
    control_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    control_dir.chmod(0o700)
    output_dir = root / 'outputs/e5-viewer'
    output_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output_dir.exists():
        raise ValueError('viewer_output_dir_already_exists')

    # Provenance must be exclusive/new; never delete or overwrite existing provenance
    prov_path = root / 'runtime/e5-viewer-provenance.json'
    if prov_path.exists() or prov_path.is_symlink():
        raise ValueError('e5_viewer_provenance_already_exists')

    source_fp = _local_source_fingerprint()
    provenance = {
        'schema_version': 1,
        'session_id': binding['session']['session_id'],
        'run_id': viewer_run_id,
        'binding_digest': binding_digest,
        'manifest_sha256': viewer_manifest_sha,
        'paperload_binary_sha256': paperload_sha,
        'gateway_binary_sha256': gateway_sha,
        'combined_binary_sha256': combined_sha,
        'b_os': sys.platform,
        'b_arch': platform.machine(),
        'source_fingerprint': source_fp,
        'control_dir': str(control_dir),
        'output_dir': str(output_dir),
        'correlation': correlation,
    }
    _write_new(prov_path, json.dumps(provenance, sort_keys=True).encode())

    # Verify bundle metadata
    _verified_bundle_metadata(root, binding)

    return paper_p8_e5_client.run_viewer_holder(
        binary=paperload_path,
        manifest_path=viewer_manifest_path,
        output_dir=output_dir,
        control_dir=control_dir,
        correlation=correlation,
        deadline_seconds=effective_deadline,
        poll_seconds=poll_seconds if poll_seconds else 0.1,
    )


def _stop_e5_viewers(workspace):
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E5':
        raise ValueError('e5_stop_viewers_requires_e5_workspace')

    state_path = root / 'runtime/e5-phase-state.json'
    state = _private_json(state_path) if state_path.exists() else {}
    if 'control' not in state.get('completed_phases', []):
        raise ValueError('e5_stop_viewers_requires_control_phase_completed')

    prov_path = root / 'runtime/e5-viewer-provenance.json'
    if not prov_path.is_file():
        raise ValueError('e5_viewer_provenance_missing')
    provenance = _private_json(prov_path)
    control_dir = Path(provenance['control_dir'])
    correlation = provenance['correlation']

    paper_p8_e5_client.request_viewer_stop(control_dir, correlation)
    return {'status': 'STOP_REQUESTED', 'correlation': correlation}


def _run_e5_phase(workspace, phase, binary, gateway_binary=None):
    if phase not in ('initial', 'warm_replay', 'control'):
        raise ValueError('E5 run requires explicit --phase initial|warm_replay|control')
    root = _workspace(workspace)
    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E5':
        raise ValueError('e5_run_requires_e5_workspace')

    paperload_path = Path(binary).absolute()
    if paperload_path.is_symlink() or not paperload_path.is_file() or not os.access(paperload_path, os.X_OK):
        raise ValueError('paperload_binary_rejected')

    prov_path = root / 'runtime/e5-viewer-provenance.json'
    if not prov_path.is_file():
        raise ValueError('e5_viewer_provenance_missing')
    provenance = _private_json(prov_path)
    if _file_sha256(paperload_path) != provenance.get('paperload_binary_sha256'):
        raise ValueError('paperload_binary_provenance_mismatch')
    if gateway_binary:
        gw_path = Path(gateway_binary).absolute()
        if gw_path.is_symlink() or not gw_path.is_file() or not os.access(gw_path, os.X_OK):
            raise ValueError('gateway_binary_rejected')
        if provenance.get('gateway_binary_sha256') and _file_sha256(gw_path) != provenance['gateway_binary_sha256']:
            raise ValueError('gateway_binary_provenance_mismatch')
    elif phase == 'control':
        raise ValueError('E5 control phase requires --gateway-binary')

    # Source provenance check
    if _local_source_fingerprint() != provenance.get('source_fingerprint'):
        raise ValueError('source_provenance_mismatch')

    binding_projection = {key: binding[key] for key in (
        'schema_version', 'session', 'source_bundle_sha256', 'source_inventory',
        'bound_manifest_sha256', 'bound_gateway_environment_sha256', 'bound_run_spec_sha256',
        'elapsed_transfer_wait_seconds', 'required_token_lifetime_seconds', 'e5',
        'immutable_root', 'mutable_root') if key in binding}
    projection_bytes = json.dumps(binding_projection, sort_keys=True, separators=(',', ':')).encode()
    binding_digest = hashlib.sha256(projection_bytes).hexdigest()

    if provenance.get('session_id') != binding['session']['session_id']:
        raise ValueError('e5_viewer_provenance_session_mismatch')
    if provenance.get('binding_digest') != binding_digest:
        raise ValueError('e5_viewer_provenance_binding_mismatch')

    lock_fd = _e5_phase_lock(root)
    try:
        state_path = root / 'runtime/e5-phase-state.json'
        state = _private_json(state_path) if state_path.exists() else {
            'schema_version': 1,
            'experiment': 'E5',
            'session_id': binding['session']['session_id'],
            'binding_digest': binding_digest,
            'completed_phases': [],
            'phase_history': [],
            'last_completed_phase': None,
        }
        if state.get('session_id') != binding['session']['session_id'] or state.get('binding_digest') != binding_digest:
            raise ValueError('e5_phase_state_binding_mismatch')

        completed = state.get('completed_phases', [])
        if phase == 'initial':
            if 'initial' in completed:
                raise ValueError('E5 phase initial already completed')
        elif phase == 'warm_replay':
            if 'initial' not in completed:
                raise ValueError('E5 initial required before warm_replay')
            if 'warm_replay' in completed:
                raise ValueError('E5 phase warm_replay already completed')
        elif phase == 'control':
            if 'initial' not in completed or 'warm_replay' not in completed:
                raise ValueError('E5 initial and warm_replay required before control')
            if 'control' in completed:
                raise ValueError('E5 phase control already completed')

        control_dir = Path(provenance['control_dir'])
        if not (control_dir / 'viewer-holder.lock').is_file() or not paper_p8_e5_client._holder_lock_is_active(control_dir):
            raise ValueError('e5_viewer_holder_not_running')
        h_status = paper_p8_e5_client.read_viewer_holder_status(control_dir)
        if h_status.get('state') not in ('READY', 'PHASE') or h_status.get('correlation') != provenance['correlation']:
            raise ValueError('e5_viewer_holder_not_ready')
        if h_status.get('ready_verified') is not True:
            raise ValueError('e5_viewer_holder_ready_unverified')

        # Begin checkpoint
        paper_p8_e5_client.request_viewer_phase(
            control_dir, provenance['correlation'], phase, 'begin', ack_timeout_seconds=5)

        spec = _private_json(root / 'runtime/execution/run-spec.json')
        phase_dir = root / 'outputs/e5-phases' / phase
        phase_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        phase_dir.chmod(0o700)
        phase_artifact = phase_dir / 'run.json'

        if phase == 'initial':
            job = next((j for j in spec.get('paperload', [])
                        if Path(j['manifest']).stem in ('initial', 'initial-replay')), None)
            if not job:
                raise ValueError('initial_manifest_missing')
            selected_jobs = [dict(job, output_dir=str(phase_dir / 'workload'))]
            phase_gateways = []
        elif phase == 'warm_replay':
            job = next((j for j in spec.get('paperload', [])
                        if Path(j['manifest']).stem in ('warm_replay', 'warm-replay')), None)
            if not job:
                raise ValueError('warm_replay_manifest_missing')
            selected_jobs = [dict(job, output_dir=str(phase_dir / 'workload'))]
            phase_gateways = []
        else:
            job = next((j for j in spec.get('paperload', [])
                        if Path(j['manifest']).stem in ('control', 'control.manifest')), None)
            if not job:
                raise ValueError('control_manifest_missing')
            selected_jobs = [dict(job, output_dir=str(phase_dir / 'workload'))]
            phase_gateways = list(spec.get('gateways', []))

        # Sample resources around supervised execution
        sampler = GeneratorResourceSampler(interval=0.1)
        sampler.start()
        try:
            run_status, report = paper_p8.run_supervised(
                binary=str(paperload_path),
                gateway_binary=str(gateway_binary) if gateway_binary else None,
                jobs=selected_jobs,
                gateways=phase_gateways,
                stop_timeout=spec.get('stop_timeout_seconds', 30),
                run_artifact=str(phase_artifact),
                disk_guard=spec.get('disk_guard'),
            )
        finally:
            resource_report = sampler.stop()
        sampler.write_csv(phase_dir / 'resources-generator.csv')
        _write_new(phase_dir / 'resources-generator.json', json.dumps(resource_report, sort_keys=True).encode())

        if run_status == 'PARTIAL' and phase == 'control':
            drained = _complete_e3_poststop_drain(root, dict(spec, paperload=selected_jobs), report, gateway_binary, expected_jobs=None)
            if drained is not None:
                fresh_counts = _fresh_executor_outbox_readback(root, spec.get('gateways', []), gateway_binary)
                if fresh_counts is not None:
                    report['pre_drain_pending_outbox_readback'] = list(report.get('pending_outbox_readback') or [])
                    report['pending_outbox_readback'] = fresh_counts
                    report['post_stop_drain'] = drained
                    report['post_stop_drain_qualification'] = (
                        'sequential executor reopen after all supervised children joined; '
                        'drain batches excluded from measured producer output')
                    report['status'] = 'COMPLETE'
                    _replace_private_json(phase_artifact, report)
                    run_status = 'COMPLETE'
                else:
                    report['cleanup_errors'] = list(report.get('cleanup_errors') or [])
                    report['cleanup_errors'].append('post_stop_drain_readback_failed')
                    _replace_private_json(phase_artifact, report)

        if run_status != 'COMPLETE':
            state['last_failed_phase'] = phase
            state['last_failure_reason'] = f'supervised_runner_incomplete_{run_status}'
            _replace_private_json(state_path, state)
            raise RuntimeError(f'E5 phase {phase} runner incomplete: {run_status}')

        # End checkpoint
        paper_p8_e5_client.request_viewer_phase(
            control_dir, provenance['correlation'], phase, 'end', ack_timeout_seconds=5)

        state['completed_phases'].append(phase)
        state['phase_history'].append(phase)
        state['last_completed_phase'] = phase
        _replace_private_json(state_path, state)

        return {
            'status': 'COMPLETE',
            'phase': phase,
            'artifact': f'outputs/e5-phases/{phase}/run.json',
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN',
        }
    finally:
        os.close(lock_fd)


def _export_e5_results(workspace, output, phase):
    root = _workspace(workspace)
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise ValueError('result output already exists')
    _reject_symlink_chain(output.absolute())

    binding = _private_json(root / 'runtime/binding.json')
    if binding.get('session', {}).get('experiment') != 'E5':
        raise ValueError('E5 export requires E5 workspace')

    binding_projection = {key: binding[key] for key in (
        'schema_version', 'session', 'source_bundle_sha256', 'source_inventory',
        'bound_manifest_sha256', 'bound_gateway_environment_sha256', 'bound_run_spec_sha256',
        'elapsed_transfer_wait_seconds', 'required_token_lifetime_seconds', 'e5',
        'immutable_root', 'mutable_root') if key in binding}
    projection_bytes = json.dumps(binding_projection, sort_keys=True, separators=(',', ':')).encode()
    binding_digest = hashlib.sha256(projection_bytes).hexdigest()

    state_path = root / 'runtime/e5-phase-state.json'
    if not state_path.is_file():
        raise ValueError(f'E5 phase {phase} not completed')
    state = _private_json(state_path)
    if state.get('session_id') != binding['session']['session_id'] or state.get('binding_digest') != binding_digest:
        raise ValueError('e5_phase_state_binding_mismatch')
    if phase not in state.get('completed_phases', []):
        raise ValueError(f'E5 phase {phase} not completed')

    prov_path = root / 'runtime/e5-viewer-provenance.json'
    if not prov_path.is_file():
        raise ValueError('e5_viewer_provenance_missing')
    provenance = _private_json(prov_path)
    control_dir = Path(provenance['control_dir'])
    correlation = provenance['correlation']

    phase_dir = root / 'outputs/e5-phases' / phase
    run_artifact = phase_dir / 'run.json'
    if not run_artifact.is_file():
        raise ValueError(f'E5 phase {phase} run artifact missing')
    raw_runner = _private_json(run_artifact)
    if not isinstance(raw_runner, dict):
        raise ValueError(f'runner_report_rejected_{phase}')
    # Validate the raw supervised runner before sanitizing: a missing or false
    # proof field must reject instead of defaulting to success.
    for proof_key in ('status', 'actual_stop_confirmed', 'primary_error', 'cleanup_errors',
                      'readiness', 'pending_outbox_readback'):
        if proof_key not in raw_runner:
            raise ValueError(f'runner_proof_missing_{phase}')
    if raw_runner['status'] != 'COMPLETE':
        raise ValueError(f'runner_status_incomplete_{phase}')
    if raw_runner['actual_stop_confirmed'] is not True:
        raise ValueError(f'runner_stop_unconfirmed_{phase}')
    if raw_runner['primary_error']:
        raise ValueError(f'runner_primary_error_{phase}')
    if not isinstance(raw_runner['cleanup_errors'], list) or raw_runner['cleanup_errors']:
        raise ValueError(f'runner_cleanup_errors_{phase}')
    readiness_rows = raw_runner['readiness']
    if not isinstance(readiness_rows, list) or any(
            not isinstance(row, dict) or row.get('ready') is not True for row in readiness_rows):
        raise ValueError(f'runner_readiness_unconfirmed_{phase}')
    readback_rows = raw_runner['pending_outbox_readback']
    if not isinstance(readback_rows, list) or any(not isinstance(row, dict) for row in readback_rows):
        raise ValueError(f'runner_drain_evidence_invalid_{phase}')
    if phase == 'control':
        # The control phase owns durable executors: readiness and drained outbox
        # readback are required actual evidence, not optional defaults.
        if not readiness_rows:
            raise ValueError('runner_readiness_unconfirmed_control')
        if not readback_rows:
            raise ValueError('runner_drain_missing_control')
        if any(row.get('status') != 'readback' or row.get('pending_rows') != 0 for row in readback_rows):
            raise ValueError('runner_drain_unconfirmed_control')

    runner_report = _safe_runner(raw_runner)
    runner_stopped = True
    cleanup_errors = []

    run_spec = _private_json(root / 'runtime/execution/run-spec.json')
    declared_keys = _private_json(root / 'runtime/e5-context.json').get('command_keys', [])

    holder_status = paper_p8_e5_client.read_viewer_holder_status(control_dir)
    holder_ready = (holder_status.get('ready_verified') is True)

    if phase == 'initial':
        job = next((j for j in run_spec.get('paperload', [])
                    if Path(j['manifest']).stem in ('initial', 'initial-replay')), None)
        if not job:
            raise ValueError('initial_job_missing')
        if job.get('role') != 'telemetry':
            raise ValueError('initial_job_role_invalid')
        job_manifest = _private_json(Path(job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
        if job_manifest.get('roles') != ['telemetry']:
            raise ValueError('initial_manifest_role_invalid')
        phase_run_id = job_manifest.get('run_id') or binding['session']['session_id']
        workload_path = phase_dir / 'workload' / 'workload.json'
        if not workload_path.is_file():
            raise ValueError('initial_workload_missing')
        workload_raw = _private_json(workload_path)
        if (workload_raw.get('experiment') != job_manifest.get('experiment')
                or workload_raw.get('arm') != job_manifest.get('arm')
                or workload_raw.get('profile') != job_manifest.get('profile')
                or workload_raw.get('mode') != job_manifest.get('mode')
                or workload_raw.get('run_id') != job_manifest.get('run_id')
                or workload_raw.get('status') != 'COMPLETE'):
            raise ValueError('initial_child_workload_invalid')
        if workload_raw.get('primary_error') or workload_raw.get('cleanup_errors'):
            raise ValueError('initial_child_errors_present')
        e4_info = workload_raw.get('e4')
        if (not isinstance(e4_info, dict) or e4_info.get('mode') != 'replay'
                or e4_info.get('journal_entry_count') != 20 or len(e4_info.get('safe_header_samples', [])) != 20):
            raise ValueError('initial_replay_contract_invalid')
        telemetry_info = workload_raw.get('telemetry')
        if (not isinstance(telemetry_info, dict) or telemetry_info.get('pending_messages') != 0
                or telemetry_info.get('broker_pubacks', 0) < 20
                or telemetry_info.get('replay_application_acks', 0) != 20):
            raise ValueError('initial_telemetry_accounting_invalid')

        telemetry_workload = _safe_workload(workload_raw, role='telemetry')
        checkpoints = [cp for cp in holder_status.get('checkpoints', []) if cp.get('phase') == 'initial']
        has_begin = any(cp.get('boundary') == 'begin' and cp.get('child_alive') is True for cp in checkpoints)
        has_end = any(cp.get('boundary') == 'end' and cp.get('child_alive') is True for cp in checkpoints)
        if not (has_begin and has_end):
            raise ValueError('initial_holder_checkpoints_missing')

        phase_status = 'COMPLETE' if (runner_stopped and holder_ready and not cleanup_errors) else 'FAILED'
        holder_evidence = {
            'ready_verified': holder_ready,
            'checkpoints': checkpoints,
        }
        phase_projection = {
            'status': phase_status,
            'cleanup_errors': cleanup_errors,
            'e4': telemetry_workload.get('e4'),
            'telemetry': telemetry_workload.get('telemetry'),
            'holder_evidence': holder_evidence,
        }
    elif phase == 'warm_replay':
        job = next((j for j in run_spec.get('paperload', [])
                    if Path(j['manifest']).stem in ('warm_replay', 'warm-replay')), None)
        if not job:
            raise ValueError('warm_replay_job_missing')
        if job.get('role') != 'telemetry':
            raise ValueError('warm_replay_job_role_invalid')
        job_manifest = _private_json(Path(job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
        if job_manifest.get('roles') != ['telemetry']:
            raise ValueError('warm_replay_manifest_role_invalid')
        phase_run_id = job_manifest.get('run_id') or binding['session']['session_id']
        workload_path = phase_dir / 'workload' / 'workload.json'
        if not workload_path.is_file():
            raise ValueError('warm_replay_workload_missing')
        workload_raw = _private_json(workload_path)
        if (workload_raw.get('experiment') != job_manifest.get('experiment')
                or workload_raw.get('arm') != job_manifest.get('arm')
                or workload_raw.get('profile') != job_manifest.get('profile')
                or workload_raw.get('mode') != job_manifest.get('mode')
                or workload_raw.get('run_id') != job_manifest.get('run_id')
                or workload_raw.get('status') != 'COMPLETE'):
            raise ValueError('warm_replay_child_workload_invalid')
        if workload_raw.get('primary_error') or workload_raw.get('cleanup_errors'):
            raise ValueError('warm_replay_child_errors_present')
        e4_info = workload_raw.get('e4')
        if (not isinstance(e4_info, dict) or e4_info.get('mode') != 'replay'
                or e4_info.get('journal_entry_count') != 10 or len(e4_info.get('safe_header_samples', [])) != 10):
            raise ValueError('warm_replay_contract_invalid')
        telemetry_info = workload_raw.get('telemetry')
        if (not isinstance(telemetry_info, dict) or telemetry_info.get('pending_messages') != 0
                or telemetry_info.get('broker_pubacks', 0) < 10
                or telemetry_info.get('replay_application_acks', 0) != 10):
            raise ValueError('warm_replay_telemetry_accounting_invalid')

        telemetry_workload = _safe_workload(workload_raw, role='telemetry')
        checkpoints = [cp for cp in holder_status.get('checkpoints', []) if cp.get('phase') == 'warm_replay']
        has_begin = any(cp.get('boundary') == 'begin' and cp.get('child_alive') is True for cp in checkpoints)
        has_end = any(cp.get('boundary') == 'end' and cp.get('child_alive') is True for cp in checkpoints)
        if not (has_begin and has_end):
            raise ValueError('warm_replay_holder_checkpoints_missing')

        phase_status = 'COMPLETE' if (runner_stopped and not cleanup_errors) else 'FAILED'
        holder_evidence = {
            'checkpoints': checkpoints,
        }
        phase_projection = {
            'status': phase_status,
            'cleanup_errors': cleanup_errors,
            'e4': telemetry_workload.get('e4'),
            'telemetry': telemetry_workload.get('telemetry'),
            'holder_evidence': holder_evidence,
        }
    else:  # control
        job = next((j for j in run_spec.get('paperload', [])
                    if Path(j['manifest']).stem in ('control', 'control.manifest')), None)
        if not job:
            raise ValueError('control_job_missing')
        if job.get('role') != 'control':
            raise ValueError('control_job_role_invalid')
        job_manifest = _private_json(Path(job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
        if job_manifest.get('roles') != ['control']:
            raise ValueError('control_manifest_role_invalid')
        phase_run_id = job_manifest.get('run_id') or binding['session']['session_id']
        workload_path = phase_dir / 'workload' / 'workload.json'
        if not workload_path.is_file():
            raise ValueError('control_workload_missing')
        control_raw = _private_json(workload_path)
        # ControlResult emits experiment/arm/run_id only: it has no profile or
        # mode field, so those must not be required or invented here.
        if (control_raw.get('experiment') != job_manifest.get('experiment')
                or control_raw.get('arm') != job_manifest.get('arm')
                or control_raw.get('run_id') != job_manifest.get('run_id')
                or control_raw.get('status') != 'COMPLETE'):
            raise ValueError('control_child_workload_invalid')
        if control_raw.get('primary_error'):
            raise ValueError('control_child_primary_error')
        cmds = control_raw.get('commands', [])
        if len(cmds) != 4 or any(
                not isinstance(c, dict) or c.get('classification') != 'succeeded'
                or c.get('terminal_status') != 'succeeded' for c in cmds):
            raise ValueError('control_commands_unverified')
        control_workload = _safe_workload(control_raw, role='control')

        deadline = time.monotonic() + 10.0
        holder_res_path = control_dir / 'holder-result.json'
        while time.monotonic() < deadline:
            if not paper_p8_e5_client._holder_lock_is_active(control_dir) and holder_res_path.is_file():
                break
            time.sleep(0.05)
        if paper_p8_e5_client._holder_lock_is_active(control_dir) or not holder_res_path.is_file():
            raise ValueError('e5_viewer_holder_not_stopped')

        holder_res = _private_json(holder_res_path)
        lifecycle = holder_res.get('lifecycle', {})
        viewer_raw = holder_res.get('workload', {})
        viewer_job = next((j for j in run_spec.get('paperload', []) if j.get('role') == 'viewer'), None)
        if not viewer_job:
            raise ValueError('viewer_job_missing')
        viewer_manifest = _private_json(Path(viewer_job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
        if viewer_manifest.get('roles') != ['viewer']:
            raise ValueError('viewer_manifest_role_invalid')
        # ViewerResult emits experiment/arm/profile/mode/run_id; the trusted
        # manifest (not a result-side roles field) declares the role.
        if (viewer_raw.get('experiment') != viewer_manifest.get('experiment')
                or viewer_raw.get('arm') != viewer_manifest.get('arm')
                or viewer_raw.get('profile') != viewer_manifest.get('profile')
                or viewer_raw.get('mode') != viewer_manifest.get('mode')
                or viewer_raw.get('run_id') != viewer_manifest.get('run_id')
                or viewer_raw.get('status') != 'COMPLETE' or viewer_raw.get('planned_stop') is not True
                or viewer_raw.get('planned_stop_reason') != 'stopped_by_operator'
                or viewer_raw.get('connection_attempts') != 2 or viewer_raw.get('connected_distinct_viewers') != 2
                or viewer_raw.get('subscribed_distinct_viewers') != 2 or viewer_raw.get('receiving_distinct_viewers') != 2
                or viewer_raw.get('reconnections') != 0 or viewer_raw.get('unique_events') != 20
                or viewer_raw.get('duplicates') != 0 or viewer_raw.get('wrong_namespace') != 0
                or viewer_raw.get('primary_error')):
            raise ValueError('e5_viewer_planned_stop_invalid')

        viewer_workload = _safe_workload(viewer_raw, role='viewer')
        viewer_cleanups = viewer_raw.get('cleanup_errors', [])
        if not isinstance(viewer_cleanups, list):
            raise ValueError('e5_viewer_cleanup_evidence_invalid')
        if viewer_cleanups:
            # Never re-export raw derived error strings; reject the viewer proof.
            raise ValueError('e5_viewer_cleanup_errors')

        # Safe projection of SQLite executors with path confinement and bounding
        executor_rows = _project_sqlite_executors(root, run_spec, declared_keys, control_workload.get('commands', []))

        # Readiness projection from actual evidence
        readiness_projection = {
            'ready_before_load': (lifecycle.get('ready_elapsed_seconds') is not None and holder_status.get('ready_verified') is True),
            'connected_distinct_viewers': viewer_workload.get('connected_distinct_viewers', 0),
            'subscribed_distinct_viewers': viewer_workload.get('subscribed_distinct_viewers', 0),
        }

        # Measurement windows
        declared_windows = {
            'initial': _private_json(root / 'runtime/manifests/initial.json', paper_p8_bundle.MAX_MANIFEST_BYTES).get('phases', {}).get('measure_seconds'),
            'warm_replay': _private_json(root / 'runtime/manifests/warm_replay.json', paper_p8_bundle.MAX_MANIFEST_BYTES).get('phases', {}).get('measure_seconds'),
            'control': _private_json(root / 'runtime/manifests/control.json', paper_p8_bundle.MAX_MANIFEST_BYTES).get('phases', {}).get('measure_seconds'),
        }
        observed_windows = {}
        for ph in ('initial', 'warm_replay', 'control'):
            rf = root / f'outputs/e5-phases/{ph}/run.json'
            if rf.is_file():
                rj = _private_json(rf)
                if rj.get('started_at_unix') and rj.get('finished_at_unix'):
                    observed_windows[ph] = round(rj['finished_at_unix'] - rj['started_at_unix'], 3)

        cps = lifecycle.get('checkpoints', [])
        values = [cp.get('elapsed_seconds') for cp in cps] if isinstance(cps, list) else []
        idle_windows = []
        if len(values) == 6 and all(v is not None for v in values):
            idle_windows = [
                {'between': 'initial/warm_replay', 'clock': lifecycle.get('clock'),
                 'start_elapsed_seconds': values[1], 'end_elapsed_seconds': values[2],
                 'duration_seconds': values[2] - values[1], 'excluded': True},
                {'between': 'warm_replay/control', 'clock': lifecycle.get('clock'),
                 'start_elapsed_seconds': values[3], 'end_elapsed_seconds': values[4],
                 'duration_seconds': values[4] - values[3], 'excluded': True},
            ]

        actual_stop_confirmed = bool(viewer_workload.get('planned_stop') is True and not paper_p8_e5_client._holder_lock_is_active(control_dir))
        processes_stopped = bool(actual_stop_confirmed and runner_stopped)
        declared_gateways = run_spec.get('gateways', [])
        pending_outbox_total = sum(e['pending_outbox'] for e in executor_rows)
        executors_joined = bool(
            declared_gateways
            and len(readback_rows) == len(declared_gateways)
            and all(row.get('status') == 'readback' and row.get('pending_rows') == 0
                    for row in readback_rows)
            and all(e['pending_outbox'] == 0 for e in executor_rows))

        b_declared = {
            'paperload_binary_sha256': provenance['paperload_binary_sha256'],
            'gateway_binary_sha256': provenance['gateway_binary_sha256'],
            'combined_binary_sha256': provenance['combined_binary_sha256'],
            'source_fingerprint': provenance['source_fingerprint'],
            'b_os': provenance['b_os'],
            'b_arch': provenance['b_arch'],
            'declared_measurement_windows': declared_windows,
            'observed_measurement_windows': observed_windows,
            'idle_transfer_windows': idle_windows,
            'actual_stop_confirmed': actual_stop_confirmed,
            'runner_stopped': runner_stopped,
            'processes_stopped': processes_stopped,
        }

        phase_status = 'COMPLETE' if (runner_stopped and viewer_workload.get('status') == 'COMPLETE' and not cleanup_errors) else 'FAILED'
        phase_projection = {
            'status': phase_status,
            'cleanup_errors': cleanup_errors,
            'commands': control_workload.get('commands', []),
            'executors': executor_rows,
            'sqlite_executors': executor_rows,
            'joined_proof': {'executors_joined': executors_joined, 'pending_outbox_total': pending_outbox_total},
            'viewer': viewer_workload,
            'lifecycle': lifecycle,
            'lifecycle_proof': lifecycle,
            'readiness_projection': readiness_projection,
            'b_declared': b_declared,
        }

    # Write output directory and regenerate resources-generator.csv safely
    output.mkdir(mode=0o700, parents=True)
    output.chmod(0o700)
    resource_samples = _sanitize_and_write_resource_samples(phase_dir, output)

    summary = {
        'schema_version': 1,
        'experiment': 'E5',
        'arm': binding['session']['arm'],
        'phase': phase,
        'session_id': binding['session']['session_id'],
        'run_id': phase_run_id,
        'status': phase_projection.get('status', 'COMPLETE'),
        'source_bundle_sha256': binding['source_bundle_sha256'],
        'source_inventory': binding['source_inventory'],
        'bound_manifest_sha256': binding['bound_manifest_sha256'],
        'bound_gateway_environment_sha256': binding['bound_gateway_environment_sha256'],
        'bound_run_spec_sha256': binding['bound_run_spec_sha256'],
        'bound_e5_context_sha256': binding['e5']['context_sha256'],
        'bound_e5_phase_plan_sha256': binding['e5']['phase_plan_sha256'],
        'bound_dataset_sha256': binding['e5']['dataset_sha256'],
        'binding': binding_projection,
        'binding_sha256': binding_digest,
        'runner': runner_report,
        'resource_samples': resource_samples,
        'projection': phase_projection,
        **phase_projection,
        'full_p8_accepted': False,
        'physical_ab': 'NOT RUN',
    }
    _write_new(output / 'results.json', json.dumps(summary, sort_keys=True, indent=2).encode())
    return {
        'status': phase_projection.get('status', 'COMPLETE'),
        'phase': phase,
        'output': str(output),
        'results': str(output / 'results.json'),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    bind = commands.add_parser('bind')
    bind.add_argument('--bundle', required=True)
    bind.add_argument('--workspace', required=True)
    bind.add_argument('--elapsed-transfer-wait-seconds', type=float, default=0)
    preflight = commands.add_parser('preflight')
    preflight.add_argument('--workspace', required=True)
    preflight.add_argument('--binary', required=True)
    preflight.add_argument('--gateway-binary')
    connection = commands.add_parser('check-connection')
    connection.add_argument('--workspace', required=True)
    run = commands.add_parser('run')
    run.add_argument('--workspace', required=True)
    run.add_argument('--binary', required=True)
    run.add_argument('--gateway-binary')
    run.add_argument('--phase', choices=('unique', 'replay', 'cold-replay', 'expiry-replay', 'initial', 'warm_replay', 'control'))
    export = commands.add_parser('export-results')
    export.add_argument('--workspace', required=True)
    export.add_argument('--output', required=True)
    export.add_argument('--phase', choices=('unique', 'replay', 'cold-replay', 'expiry-replay', 'initial', 'warm_replay', 'control'))
    e5_viewers = commands.add_parser('e5-viewers', help='start foreground E5 continuous viewer holder')
    e5_viewers.add_argument('--workspace', required=True)
    e5_viewers.add_argument('--binary', required=True)
    e5_viewers.add_argument('--gateway-binary')
    e5_viewers.add_argument('--deadline-seconds', type=int)
    e5_viewers.add_argument('--poll-seconds', type=float, default=0.1)
    e5_stop = commands.add_parser('e5-stop-viewers', help='request planned stop for E5 continuous viewer holder')
    e5_stop.add_argument('--workspace', required=True)
    durable_export = commands.add_parser('export-durable-results')
    durable_export.add_argument('--workspace', required=True)
    durable_export.add_argument('--output', required=True)
    durable_export.add_argument('--stage', choices=('durable-fault', 'durable-reopen'), required=True)
    durable = commands.add_parser('durable-stage', help='run one fixed E4 durable SQLite probe stage')
    durable.add_argument('--workspace', required=True)
    durable.add_argument('--gateway-binary', required=True)
    durable.add_argument('--inputs', required=True)
    durable.add_argument('--stage', choices=('fault', 'reopen'), required=True)
    export_conflict_probe = commands.add_parser('export-conflict-probe',
        help='extract initial journal candidate and export conflict probe')
    export_conflict_probe.add_argument('--workspace', required=True)
    export_conflict_probe.add_argument('--output', required=True)
    conflict_stage = commands.add_parser('conflict-stage',
        help='run the bounded E4 conflicts workload with paperload')
    conflict_stage.add_argument('--workspace', required=True)
    conflict_stage.add_argument('--binary', required=True)
    export_conflict_results = commands.add_parser('export-conflict-results',
        help='export bounded conflict proof and results projection')
    export_conflict_results.add_argument('--workspace', required=True)
    export_conflict_results.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'bind':
            result = paper_p8_bundle.bind_bundle(bundle_root=args.bundle,
                workspace_leaf=args.workspace,
                elapsed_transfer_wait_seconds=args.elapsed_transfer_wait_seconds)
        elif args.command == 'e5-viewers':
            result = _run_e5_viewers(args.workspace, binary=args.binary, gateway_binary=args.gateway_binary,
                                     deadline_seconds=args.deadline_seconds, poll_seconds=args.poll_seconds)
        elif args.command == 'e5-stop-viewers':
            result = _stop_e5_viewers(args.workspace)
        elif args.command in ('preflight', 'run'):
            root = _workspace(args.workspace)
            spec_path = root / 'runtime/execution/run-spec.json'
            run_spec = _private_json(spec_path)
            jobs, gateways, stop_timeout, _ = paper_p8._read_spec(str(spec_path), None, None)
            if args.command == 'preflight':
                binding = _private_json(root / 'runtime/binding.json')
                official_opt_in = (binding.get('session', {}).get('experiment') in ('E1', 'E3')
                    and binding.get('session', {}).get('profile') == 'official')
                for gateway in gateways:
                    paper_p8._gateway_environment(gateway['env_file'])
                for job in jobs:
                    paper_p8.preflight(args.binary, job['manifest'], job['output_dir'],
                                       official=official_opt_in)
                result = {'preflight': 'PASS', 'network': False, 'writes': False,
                          'manifests': len(jobs), 'gateways_validated': len(gateways)}
            else:
                binding = _private_json(root / 'runtime/binding.json')
                if binding.get('session', {}).get('experiment') == 'E5':
                    if not args.phase or args.phase not in ('initial', 'warm_replay', 'control'):
                        raise ValueError('E5 run requires explicit --phase initial|warm_replay|control')
                    result = _run_e5_phase(args.workspace, args.phase, binary=args.binary, gateway_binary=args.gateway_binary)
                    print(json.dumps(result, sort_keys=True))
                    return 0
                e4 = binding.get('session', {}).get('experiment') == 'E4'
                if e4 != (args.phase is not None):
                    raise ValueError('E4 run requires explicit --phase; phase is E4-only')
                binary_path = Path(args.binary).absolute()
                if binary_path.is_symlink() or not binary_path.is_file():
                    raise ValueError('paperload binary rejected')
                provenance = {'binary_sha256': _file_sha256(binary_path),
                              'binary_os': sys.platform, 'binary_arch': platform.machine()}
                lock_fd = _e4_phase_lock(root) if e4 else None
                try:
                    if e4:
                        state_path = _phase_state_path(root)
                        state = _private_json(state_path) if state_path.exists() else {
                            'schema_version': 1, 'experiment': 'E4',
                            'session_id': binding.get('session', {}).get('session_id'),
                            'run_ids': binding.get('session', {}).get('run_ids'),
                            'last_completed_phase': None}
                        if (state.get('session_id') != binding.get('session', {}).get('session_id')
                                or state.get('run_ids') != binding.get('session', {}).get('run_ids')):
                            raise ValueError('E4 phase state binding mismatch')
                        _validate_e4_phase_eligibility(root, binding, jobs, state, args.phase)
                        selected = _select_e4_phase_jobs(jobs, args.phase,
                            replay_validated=state.get('unique_validated') is True)
                        manifest_values = []
                        for job in selected:
                            manifest = _private_json(Path(job['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
                            if manifest.get('experiment') != ('E1' if job['role'] == 'viewer' else 'E4'):
                                raise ValueError('E4 phase manifest provenance mismatch')
                            manifest_values.append(manifest)
                        telemetry_manifest = next(m for m in manifest_values if m.get('roles') == ['telemetry'])
                        expected_e4_mode = 'replay' if args.phase in ('cold-replay', 'expiry-replay') else args.phase
                        if telemetry_manifest.get('e4', {}).get('mode') != expected_e4_mode:
                            raise ValueError('E4 phase manifest mode mismatch')
                        journal_limit = telemetry_manifest.get('e4', {}).get('max_bytes')
                        if type(journal_limit) is not int or not 1 <= journal_limit <= (64 << 20):
                            raise ValueError('E4 journal byte budget rejected')
                        if shutil.disk_usage(root).free < journal_limit * 2 + (8 << 20):
                            raise ValueError('E4 output and journal disk budget insufficient')
                        journal_paths = {m.get('e4', {}).get('journal_path') for m in
                             (_private_json(Path(j['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES) for j in jobs if j.get('role') == 'telemetry')}
                        if len(journal_paths) != 1 or not next(iter(journal_paths)):
                            raise ValueError('E4 phase journal reference mismatch')
                        journal_path = Path(next(iter(journal_paths)))
                        _reject_symlink_chain(journal_path)
                        journal_path = journal_path.resolve(strict=False)
                        if journal_path.is_symlink() or not journal_path.is_relative_to((root / 'outputs').resolve()):
                            raise ValueError('E4 journal path outside workspace')
                        if args.phase in ('replay', 'cold-replay', 'expiry-replay') and not journal_path.is_file():
                            raise ValueError('E4 unique journal missing')
                        journal_before = _file_sha256(journal_path) if journal_path.is_file() else None
                        if args.phase in ('replay', 'cold-replay', 'expiry-replay') and journal_before != state.get('unique_journal_sha256'):
                            raise ValueError('E4 journal changed before replay')
                        phase_root = root / 'outputs/e4-phases' / args.phase
                        phase_resumed = phase_root.exists()
                        if phase_resumed:
                            _reject_symlink_chain(phase_root)
                            phase_stat = phase_root.lstat()
                            if (not phase_root.is_dir() or phase_stat.st_uid != os.getuid()
                                    or phase_stat.st_mode & 0o077):
                                raise ValueError('E4 existing phase directory rejected')
                        else:
                            phase_root.mkdir(mode=0o700, parents=True, exist_ok=False)
                        selected = [dict(job, output_dir=str(phase_root / Path(job['manifest']).stem))
                                    for job in selected]
                        artifact = phase_root / 'run.json'
                        provenance_path = root / 'runtime/e4-phases' / args.phase / 'provenance.json'
                        if phase_resumed:
                            prior_runner = _private_json(artifact)
                            prior_provenance = _private_json(provenance_path)
                            if prior_runner.get('status') != 'COMPLETE' or prior_provenance != provenance:
                                raise ValueError('E4 existing run is not resumable')
                            for job in selected:
                                output_dir = Path(job['output_dir'])
                                _reject_symlink_chain(output_dir)
                                if (not output_dir.is_dir() or output_dir.stat().st_uid != os.getuid()
                                        or output_dir.stat().st_mode & 0o077):
                                    raise ValueError('E4 existing workload output rejected')
                        else:
                            provenance_path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
                            _write_new(provenance_path, json.dumps(provenance, sort_keys=True).encode())
                    else:
                        selected = jobs
                        artifact = root / 'outputs/run.json'
                        if artifact.exists():
                            raise ValueError('run artifact already exists')
                        _write_new(root / 'runtime/runner-provenance.json',
                                   json.dumps(provenance, sort_keys=True).encode())
                    sampler = None
                    is_equal_samples_e2 = (
                        not e4
                        and binding.get('session', {}).get('experiment') == 'E2'
                        and (
                            binding.get('session', {}).get('workload') == 'equal_samples'
                            or str(binding.get('session', {}).get('phase', '')).startswith('equal-samples-')
                            or any(
                                isinstance(_private_json(Path(j['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES), dict)
                                and 'equal_samples' in _private_json(Path(j['manifest']), paper_p8_bundle.MAX_MANIFEST_BYTES)
                                for j in selected
                                if Path(j.get('manifest', '')).is_file()
                            )
                        )
                    )
                    if is_equal_samples_e2:
                        sampler = GeneratorResourceSampler(interval=1.0)
                        sampler.start()
                    try:
                        if e4 and phase_resumed:
                            status, report = 'COMPLETE', _private_json(artifact)
                        else:
                            official_opt_in = (binding.get('session', {}).get('experiment') in ('E1', 'E3')
                                and binding.get('session', {}).get('profile') == 'official')
                            status, report = paper_p8.run_supervised(binary=args.binary,
                                gateway_binary=args.gateway_binary, jobs=selected, gateways=gateways,
                                stop_timeout=stop_timeout, run_artifact=str(artifact),
                                disk_guard=run_spec.get('disk_guard'), official=official_opt_in)
                    finally:
                        if sampler is not None:
                            gen_report = sampler.stop()
                            sampler.write_csv(root / 'outputs/resources-generator.csv')
                            _write_new(
                                root / 'outputs/resources-generator.json',
                                json.dumps(gen_report, sort_keys=True).encode(),
                            )
                    if e4:
                        if status != 'COMPLETE':
                            state['last_failed_phase'] = args.phase
                            state['last_failure_reason'] = 'supervised_runner_incomplete'
                            _replace_private_json(state_path, state)
                            raise RuntimeError('supervised runner incomplete; journal and phase outputs retained')
                        telemetry_output = Path(selected[0]['output_dir']) / 'workload.json'
                        viewer_job = next(job for job in selected if job['role'] == 'viewer')
                        viewer_output = Path(viewer_job['output_dir']) / 'workload.json'
                        telemetry = _safe_workload(json.loads(telemetry_output.read_text()), role='telemetry')
                        viewer = _safe_workload(json.loads(viewer_output.read_text()), role='viewer')
                        validation_phase = 'replay' if args.phase in ('cold-replay', 'expiry-replay') else args.phase
                        _validate_e4_phase_result(validation_phase, telemetry, viewer)
                        if (args.phase in ('replay', 'cold-replay', 'expiry-replay') and telemetry['e4']['safe_header_samples']
                                != state.get('unique_header_samples')):
                            raise ValueError('E4 replay journal headers changed')
                        if not journal_path.is_file() or journal_path.stat().st_size > journal_limit:
                            raise ValueError('E4 journal artifact missing or over bound')
                        journal_after = _file_sha256(journal_path)
                        if args.phase in ('replay', 'cold-replay', 'expiry-replay') and journal_after != journal_before:
                            raise ValueError('E4 replay changed journal bytes')
                        state.update(last_completed_phase=args.phase,
                            unique_validated=(args.phase == 'unique' or state.get('unique_validated') is True),
                            unique_journal_sha256=(journal_after if args.phase == 'unique'
                                else state.get('unique_journal_sha256')),
                        unique_header_samples=(telemetry['e4']['safe_header_samples'][:10]
                                if args.phase == 'unique' else state.get('unique_header_samples')),
                            last_journal_sha256=journal_after,
                            last_phase_workload_sha256=hashlib.sha256(telemetry_output.read_bytes()).hexdigest(),
                            last_phase_viewer_sha256=hashlib.sha256(viewer_output.read_bytes()).hexdigest())
                        _replace_private_json(state_path, state)
                finally:
                    if lock_fd is not None:
                        os.close(lock_fd)
                binding = _private_json(root / 'runtime/binding.json')
                if status == 'PARTIAL' and binding.get('session', {}).get('experiment') == 'E3':
                    run_evidence = _private_json(artifact)
                    drained = _complete_e3_poststop_drain(root, _private_json(spec_path),
                                                         run_evidence, args.gateway_binary)
                    if drained is not None:
                        run_evidence['status'] = 'COMPLETE'
                        run_evidence['post_stop_drain'] = drained
                        run_evidence['post_stop_drain_qualification'] = (
                            'sequential executor reopen after all supervised children joined; '
                            'drain batches excluded from measured producer output')
                        _replace_private_json(artifact, run_evidence)
                        status = 'COMPLETE'
                result = {'status': status, 'artifact': (f'outputs/e4-phases/{args.phase}/run.json'
                          if e4 else 'outputs/run.json'),
                          'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
                if status != 'COMPLETE':
                    raise RuntimeError('supervised runner did not complete; workspace evidence retained')
        elif args.command == 'durable-stage':
            root = _workspace(args.workspace)
            binding = _private_json(root / 'runtime/binding.json')
            if binding.get('session', {}).get('experiment') != 'E4':
                raise ValueError('durable_stage_is_e4_only')
            validate_inputs, run_stage, gateway_env = _durable_helpers()
            lock_fd = _e4_phase_lock(root)
            try:
                expected_inputs = root / 'runtime/durable-probe.json'
                if Path(args.inputs).absolute() != expected_inputs.absolute():
                    raise ValueError('durable inputs must use verified bundle binding')
                raw_inputs = _private_json(expected_inputs)
                inputs = validate_inputs(raw_inputs, workspace=root,
                    stage=args.stage, binding=binding)
                # Use the repository's Gateway environment defaults, then override only
                # the explicitly provisioned E4 data-plane inputs.
                password = json.loads(Path(inputs['password_file']).read_text(encoding='utf-8'))['password']
                env = gateway_env(inputs['gateway_id'], password)
                env.update({'GATEWAY_CONTROL_ENABLED': 'false',
                    'GATEWAY_MQTT_BROKER_URL': inputs['mqtt_endpoint'],
                    'GATEWAY_MQTT_CA_FILE': inputs['ca_file'],
                    'GATEWAY_MQTT_SERVER_NAME': inputs['mqtt_server_name'],
                    'GATEWAY_OUTBOX_PATH': inputs['outbox_path']})
                result = run_stage(gateway_binary=args.gateway_binary, inputs=inputs,
                                   phase=args.stage, env_base=env)
                result_path = root / 'runtime' / f'e4-durable-{args.stage}.json'
                if result_path.exists() or result_path.is_symlink():
                    raise ValueError('durable_stage_result_already_exists')
                _write_new(result_path, json.dumps(result, sort_keys=True).encode())
            finally:
                os.close(lock_fd)
        elif args.command == 'export-durable-results':
            result = _export_durable_results(args.workspace, Path(args.output), args.stage)
        elif args.command == 'export-conflict-probe':
            result = _export_conflict_probe(args.workspace, Path(args.output))
        elif args.command == 'conflict-stage':
            result = _run_conflict_stage(args.workspace, binary=args.binary)
        elif args.command == 'export-conflict-results':
            result = _export_conflict_results(args.workspace, Path(args.output))
        elif args.command == 'check-connection':
            root = _workspace(args.workspace)
            result = _check_connection(root, root / 'runtime/execution/run-spec.json')
        else:
            root = _workspace(args.workspace)
            binding = _private_json(root / 'runtime/binding.json')
            if binding.get('session', {}).get('experiment') == 'E5':
                if not args.phase or args.phase not in ('initial', 'warm_replay', 'control'):
                    raise ValueError('E5 export requires explicit --phase initial|warm_replay|control')
                result = _export_e5_results(args.workspace, args.output, args.phase)
            elif binding.get('session', {}).get('experiment') == 'E4':
                if args.phase not in ('unique', 'replay', 'cold-replay', 'expiry-replay'):
                    raise ValueError('E4 export requires explicit --phase; phase is E4-only')
                lock_fd = _e4_phase_lock(root)
                try:
                    state = _private_json(_phase_state_path(root))
                    if state.get('last_completed_phase') != args.phase:
                        raise ValueError('E4 result phase not completed')
                    result = _export_results(root, args.output)
                finally:
                    if lock_fd is not None:
                        os.close(lock_fd)
            else:
                result = _export_results(root, args.output)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ValueError, OSError, RuntimeError, paper_p8.PaperloadInputError,
            paper_p8.PaperloadTimeout, paper_p8.PaperloadLaunchError,
            paper_p8.PaperloadExitError) as exc:
        print(f'P8 client error: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
