"""Finite P5a MQTT/Gateway workload on native_compose_smoke's owned fixture."""

import hashlib
import json
import os
import re
import subprocess
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

PROFILE_COMMON = {
    'MQTT_INGRESS_ENABLED': 'true',
    'MQTT_QUEUE_CAPACITY': '256',
    'MQTT_MAX_PAYLOAD_BYTES': '65536',
    'MQTT_WORKER_COUNT': '4',
    'DATABASE_MAX_CONNS': '10',
    'STAGE5_ENABLED': 'false',
    'METRICS_ENABLED': 'true',
    'MQTT_SHAPER_ENABLED': 'false',
    'MQTT_SHAPER_RATE_PER_SECOND': '2',
    'MQTT_SHAPER_BURST': '4',
    'MQTT_SHAPER_MAX_GATEWAYS': '4096',
    'MQTT_SHAPER_IDLE_RETENTION': '10m',
    'MQTT_INGRESS_BUFFERED_BYTES': '16777216',
    'MQTT_MICROBATCH_MAX_MESSAGES': '32',
    'MQTT_MICROBATCH_MAX_ROWS': '500',
    'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
    'MQTT_MICROBATCH_MAX_WAIT': '1s',
    'MQTT_RAM_DEDUP_ENABLED': 'false',
    'MQTT_RAM_DEDUP_MAX_ENTRIES': '100000',
    'MQTT_RAM_DEDUP_MAX_BYTES': '67108864',
    'SUPABASE_JWT_MODE': 'rs256',
    'SUPABASE_JWKS_URL': 'http://supabase-envoy:8000/auth/v1/.well-known/jwks.json',
    'AUTH_UNIX_SOCKET_PATH': '/run/iot-auth/auth.sock',
}

BACKEND_QUOTA = {'cpus': 2, 'memory_bytes': 4294967296}
REQUIRED_CHECKS = {
    'workload_accounting', 'ledger_raw_provenance', 'temporal_per_message',
    'current_state_and_versions', 'application_acks_and_empty_outboxes',
    'duplicate_replay_unchanged', 'wrong_namespace_denied', 'https_reads',
    'rs256_https_mqtt_tls_acl', 'payload_budget_drained', 'group_metrics_observed',
    'transaction_copy_metrics_observed', 'exporters_written',
}
DATASET_PROFILES = {
    '100': {'name': '100-samples-per-message', 'samples_per_message': 100},
    '1': {'name': '1-sample-per-message', 'samples_per_message': 1},
}

# Keep these query bodies shared with the real workload and the PostgreSQL
# regression. The psql variables are bound by the existing db_psql helper.
PER_GATEWAY_SUMMARY_SQL = """SELECT coalesce(json_agg(json_build_object(
  'gateway_id', gateway_id, 'messages', messages, 'samples', samples) ORDER BY gateway_id),'[]'::json)
  FROM (
    SELECT gateway_id, count(*) AS messages, sum(sample_count) AS samples
    FROM processed_messages WHERE gateway_id IN (:'a',:'b') GROUP BY gateway_id
  ) AS grouped;"""

PER_TEMPORAL_SUMMARY_SQL = """SELECT coalesce(json_agg(json_build_object(
  'entity_id', entity_id, 'observations', observations) ORDER BY entity_id),'[]'::json)
  FROM (
    SELECT e.entity_id, count(*) AS observations
    FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
    WHERE e.entity_id IN (:'ua',:'ub') GROUP BY e.entity_id
  ) AS grouped;"""

# Keep the P5b state/version read shared across warm, conflict and cold snapshots.
# twin_states.entity_id is the internal twin_entities.id UUID; entity_id on
# twin_entities is the external URN text used by the fixture variables.
VERSION_SNAPSHOT_SQL = """SELECT s.reported_version::text
  FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
  WHERE e.entity_id=:'urn';"""
STATE_FINGERPRINT_SQL = """SELECT to_jsonb(s)::text FROM twin_states s
  JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn'"""


def profile(mode, dataset, *, shutdown_smoke=False, cache_smoke=False):
    """Return an effective backend profile; off/on differ only in the feature flag."""
    if mode not in ('off', 'on'):
        raise ValueError('paper_p5a_mode_rejected')
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p5a_dataset_rejected')
    if shutdown_smoke and (mode != 'on' or dataset != '1'):
        raise ValueError('paper_p5a_shutdown_profile_rejected')
    if cache_smoke and (mode != 'on' or dataset != '1' or shutdown_smoke):
        raise ValueError('paper_p5b_cache_profile_rejected')
    values = {**PROFILE_COMMON, 'MQTT_MICROBATCH_ENABLED': 'true' if mode == 'on' else 'false'}
    if cache_smoke:
        values['MQTT_RAM_DEDUP_ENABLED'] = 'true'
    if shutdown_smoke:
        # Isolated functional smoke only: leave time to observe the partial group
        # before SIGTERM while retaining the existing 10s shared shutdown budget.
        values['MQTT_MICROBATCH_MAX_WAIT'] = '2s'
    return values


def p5b_profile(cache_mode, dataset):
    """Fixed P5a microbatch-on workload with only the independent RAM cache flag varied."""
    if cache_mode not in ('off', 'on'):
        raise ValueError('paper_p5b_cache_mode_rejected')
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p5b_dataset_rejected')
    values = profile('on', dataset)
    values['MQTT_RAM_DEDUP_ENABLED'] = 'true' if cache_mode == 'on' else 'false'
    return values


def p5b_run_metadata(cache_mode, dataset, *, backend_quota=BACKEND_QUOTA, effective_environment=None,
                     source_revision='captured_at_execution', dirty_worktree='captured_at_execution'):
    """Comparable schema-24 profile description; workload phases are not run by preparation."""
    effective = p5b_profile(cache_mode, dataset)
    if backend_quota != BACKEND_QUOTA:
        raise ValueError('paper_p5b_quota_rejected')
    source = 'fixture_profile'
    if effective_environment is not None:
        if any(str(effective_environment.get(key)).lower() != value for key, value in effective.items()):
            raise ValueError('paper_p5b_profile_rejected')
        source = 'validated_compose_environment'
    return {
        'database_schema_version': 24,
        'comparison': {'experiment': 'p5b_ram_dedup', 'cache_mode': cache_mode,
                       'microbatch_mode': 'on', 'cache_flag': 'MQTT_RAM_DEDUP_ENABLED',
                       'only_profile_difference': 'MQTT_RAM_DEDUP_ENABLED'},
        'dataset': dict(DATASET_PROFILES[dataset]),
        'effective_profile': effective,
        'effective_profile_source': source,
        'backend_quota': dict(BACKEND_QUOTA),
        'shaper': {key: effective[key] for key in PROFILE_COMMON
                   if key.startswith('MQTT_SHAPER_') or key == 'MQTT_INGRESS_BUFFERED_BYTES'},
        'auth_tls_acl': {'auth_mode': 'rs256', 'https': True, 'mqtt_tls': True, 'acl_enforced': True},
        'ram_cache': {'enabled': cache_mode == 'on', 'ttl_seconds': 600,
                      'max_entries': int(effective['MQTT_RAM_DEDUP_MAX_ENTRIES']),
                      'max_bytes': int(effective['MQTT_RAM_DEDUP_MAX_BYTES']),
                      'status': 'configured_not_run'},
        'phases': {
            'new_messages': {'status': 'configured_not_run', 'source': 'finite P5a Gateway dataset',
                             'offered_unique_messages': 24,
                             'offered_unique_samples': int(workload_config(dataset)['offered_unique_samples']),
                             'gateway_application_ack_timing': 'Gateway generation plus publish/drain, as P5a'},
            'duplicate_replay': {'status': 'configured_not_run',
                                 'source': 'saved original Gateway bytes, manual TLS/DynSec publish',
                                 'attempts_per_gateway': 12, 'gateway_count': 2, 'offered_attempts': 24,
                                 'denominator': '24 duplicate deliveries',
                                 'pattern': 'same captured original message repeated 12 times per Gateway',
                                 'ack_timing': 'manual replay ACK window, separate from Gateway generation ACK timing',
                                 'outbox_readback': 'not_applicable_manual_replay'},
            'drain': {'status': 'configured_not_run', 'scope': 'original Gateway generated workload outboxes'},
        },
        'source': {'source_revision': source_revision, 'dirty_worktree': dirty_worktree,
                   'image_manifest': {'status': 'captured_after_image_build', 'images': None}},
        'offered_pattern': {'status': 'configured_not_run', 'comparison_rule': 'identical_between_cache_off_and_on'},
    }


def validate_effective_profile(environment, mode, dataset, *, shutdown_smoke=False, cache_smoke=False):
    expected = profile(mode, dataset, shutdown_smoke=shutdown_smoke, cache_smoke=cache_smoke)
    if any(str(environment.get(key)).lower() != value for key, value in expected.items()):
        raise ValueError('paper_p5a_profile_rejected')
    return True


def run_metadata(mode, dataset, *, backend_quota=BACKEND_QUOTA, effective_environment=None,
                 shutdown_smoke=False, cache_smoke=False):
    """Safe run.json fields for a future workload; does not execute or claim a run."""
    effective = profile(mode, dataset, shutdown_smoke=shutdown_smoke, cache_smoke=cache_smoke)
    if backend_quota != BACKEND_QUOTA:
        raise ValueError('paper_p5a_quota_rejected')
    source = 'fixture_profile'
    if effective_environment is not None:
        validate_effective_profile(effective_environment, mode, dataset, shutdown_smoke=shutdown_smoke,
                                   cache_smoke=cache_smoke)
        source = 'validated_compose_environment'
    metadata = {
        'database_schema_version': 24,
        'mode': mode,
        'dataset': dict(DATASET_PROFILES[dataset]),
        'effective_profile': effective,
        'effective_profile_source': source,
        'backend_quota': dict(BACKEND_QUOTA),
        'shaper': {key: effective[key] for key in PROFILE_COMMON if key.startswith('MQTT_SHAPER_')
                   or key == 'MQTT_INGRESS_BUFFERED_BYTES'},
        'auth_tls_acl': {'auth_mode': 'rs256', 'https': True, 'mqtt_tls': True, 'acl_enforced': True},
        'offered_pattern': {'status': 'configured_not_run', 'comparison_rule': 'identical_between_off_and_on'},
        'ram_cache': {
            'enabled': effective['MQTT_RAM_DEDUP_ENABLED'] == 'true',
            'ttl_seconds': 600,
            'max_entries': int(effective['MQTT_RAM_DEDUP_MAX_ENTRIES']),
            'max_bytes': int(effective['MQTT_RAM_DEDUP_MAX_BYTES']),
            'status': 'configured_not_run',
        },
    }
    if shutdown_smoke:
        metadata['shutdown_smoke'] = True
    if cache_smoke:
        metadata['cache_smoke'] = True
        metadata['cache_smoke_contract'] = {
            'phases': ['warm_from_new_commit', 'exact_replay', 'representative_conflicts',
                       'cold_exact_replay_after_controlled_backend_restart'],
            'manual_replay_outbox_readback': 'not_applicable; Gateway-generated unique workload separately verifies drain',
            'status': 'configured_not_run',
            'pending': ['native_execution_and_exact_cleanup'],
        }
    return metadata


def cache_smoke_phases():
    return [
        {'name': 'warm', 'source': 'new committed Gateway telemetry delivery'},
        {'name': 'replay', 'source': 'saved byte-exact payload over fixture TLS/ACL ingress'},
        {'name': 'cold_replay', 'source': 'same payload after controlled backend replacement'},
    ]


def cache_smoke_evidence_ready(evidence):
    """Strict shape gate for bounded native phase evidence; no inferred ACK/outbox claims."""
    if not isinstance(evidence, dict):
        return False
    warm, replay, cold = (evidence.get(name) for name in ('warm', 'replay', 'cold_replay'))
    return (isinstance(warm, dict) and warm.get('new_committed') is True
            and isinstance(replay, dict) and replay.get('payload_sha256_matches') is True
            and replay.get('cache_hit_delta', 0) > 0
            and replay.get('repository_calls_avoided_delta', 0) > 0
            and replay.get('application_ack') is True and replay.get('database_unchanged') is True
            and replay.get('outbox_readback') == 'not_applicable_manual_replay'
            and isinstance(cold, dict) and cold.get('process_identity_changed') is True
            and cold.get('cache_entries_before') == 0 and cold.get('cache_entries_after') == 0
            and cold.get('cache_miss_delta', 0) > 0 and cold.get('db_duplicate_delta', 0) > 0
            and cold.get('database_work_observed') is True and cold.get('cache_hit_delta') == 0
            and cold.get('application_ack') is True and cold.get('database_unchanged') is True)


def cache_smoke_acceptance_ready(evidence):
    """Functional marker requires warm replay and all bounded conflict cases."""
    phase = evidence.get('warm_replay') if isinstance(evidence, dict) else None
    conflicts = evidence.get('conflicts') if isinstance(evidence, dict) else None
    cold = evidence.get('cold_replay') if isinstance(evidence, dict) else None
    return (isinstance(phase, dict) and phase.get('status') == 'PASS'
            and phase.get('process_identity_same') is True
            and phase.get('payload_sha256_matches') is True
            and phase.get('application_ack') is True
            and phase.get('database_unchanged') is True
            and type(phase.get('cache_hit_delta')) is int and phase['cache_hit_delta'] > 0
            and type(phase.get('database_calls_avoided_delta')) is int
            and phase['database_calls_avoided_delta'] > 0
            and type(phase.get('duplicate_committed_delta')) is int
            and phase['duplicate_committed_delta'] > 0
            and isinstance(conflicts, dict) and conflicts.get('status') == 'PASS'
            and set(conflicts.get('cases', {})) == {'same_id_changed_payload',
                'same_id_moved_sensor_time_key', 'same_ram_key_different_id',
                'different_first_sequence_overlap'}
            and all(item.get('status') == 'PASS' for item in conflicts['cases'].values())
            and isinstance(cold, dict) and cold.get('status') == 'PASS'
            and cold.get('process_identity_changed') is True and cold.get('cache_entries_before') == 0
            and cold.get('cache_entries_after') == 0
            and cold.get('cache_miss_delta', 0) > 0 and cold.get('cache_hit_delta') == 0
            and cold.get('db_duplicate_delta', 0) > 0 and cold.get('database_work_observed') is True
            and cold.get('application_ack') is True and cold.get('database_unchanged') is True
            and evidence.get('exact_cleanup_verified') is True)


def replace_backend_for_cold_cache(smoke, fixture, registry, containers, state):
    """Gracefully replace only this fixture's backend, keeping its exact cache profile."""
    old = containers['backend']
    name = fixture.project + '-backend-cache-cold'
    values = profile('on', '1', cache_smoke=True)
    env_yaml = ''.join('      ' + key + ': ' + json.dumps(value) + '\n' for key, value in values.items())
    fragment = fixture.private / 'compose.cache-cold.yml'
    smoke.private_write(fragment, 'services:\n  backend:\n    container_name: ' + name
                        + '\n    environment:\n' + env_yaml)
    if registry.command(['stop', '--time', '20', old['receipt']['id']]).returncode:
        raise ValueError('paper_p5b_backend_restart_failed')
    registry.remove(old)
    item = registry.intent('container', name, old['image'], old['labels'])
    item['expectation'] = old['expectation']
    registry.save(item)
    if registry.resolve(item) is not None:
        raise ValueError('resource_collision')
    registry.allocating(item)
    containers['backend'] = item
    command = smoke.compose_command(fixture, 'config')[:-3] + ['-f', str(fragment), 'up', '--detach',
        '--no-deps', '--no-build', '--pull', 'never', 'backend']
    result = subprocess.run(command, capture_output=True, env=smoke.process_env(fixture),
                            timeout=smoke.BUDGETS['up'], check=False)
    smoke.private_write(fixture.private / 'cache-cold-replacement.log',
                        (result.stdout + result.stderr).decode(errors='replace'))
    obj = registry.resolve(item)
    if result.returncode or obj is None:
        raise ValueError('paper_p5b_backend_restart_failed')
    deadline = time.monotonic() + 45
    try:
        while not smoke.startup_state('backend', obj['State']):
            state.checkpoint()
            if time.monotonic() >= deadline:
                raise ValueError('paper_p5b_backend_restart_failed')
            time.sleep(0.5)
            obj = registry.resolve(item)
            if obj is None:
                raise ValueError('paper_p5b_backend_restart_failed')
        env = obj['Config']['Env']
        if any(key + '=' + value not in env for key, value in values.items()):
            raise ValueError('paper_p5b_backend_profile_mismatch')
        if registry.command(['exec', containers['nginx']['receipt']['id'], 'nginx', '-s', 'reload']).returncode:
            raise ValueError('paper_p5b_backend_restart_failed')
    except BaseException:
        if obj is not None:
            logs = registry.command(['logs', '--tail', '50', obj['Id']])
            smoke.private_write(fixture.private / 'cache-cold-startup-failure.log',
                                (logs.stdout + logs.stderr).decode(errors='replace'))
            smoke.durable_json(fixture.private / 'cache-cold-startup-state.json', obj['State'])
        raise
    return obj


def cold_replay_evidence(before_metrics, after_metrics, before_db, after_db, *, selected,
                         ack_topic, ack_payload, old_process_identity):
    """Require a fresh-process cache miss, real DB duplicate work and exact application ACK."""
    from stage3_mqtt_ingress_runner import verify_ack
    reasons = []
    identity_changed = (isinstance(before_metrics, dict)
                        and before_metrics.get('process_instance') != old_process_identity)
    if not identity_changed:
        reasons.append('process_identity_not_changed')
    if (not isinstance(before_metrics, dict) or not isinstance(after_metrics, dict)
            or before_metrics.get('process_instance') != after_metrics.get('process_instance')):
        reasons.append('metrics_window_crossed_process_restart')
    cache_before = before_metrics.get('ram_dedup') if isinstance(before_metrics, dict) else None
    cache_after = after_metrics.get('ram_dedup') if isinstance(after_metrics, dict) else None
    entries = cache_before.get('entries') if isinstance(cache_before, dict) else None
    entries_after = cache_after.get('entries') if isinstance(cache_after, dict) else None
    miss_delta = hit_delta = duplicate_delta = begin_delta = commit_delta = None
    new_rows_delta = copy_delta = committed_delta = samples_new_delta = None
    if type(entries) is not int or entries != 0:
        reasons.append('replacement_cache_not_empty')
    if type(entries_after) is not int or entries_after != 0:
        reasons.append('database_duplicate_populated_cache')
    if not isinstance(cache_before, dict) or not isinstance(cache_after, dict):
        reasons.append('cache_metrics_unavailable')
    else:
        for field in ('lookup_miss_total', 'lookup_hit_total'):
            left, right = cache_before.get(field), cache_after.get(field)
            if type(left) is not int or type(right) is not int or right < left:
                reasons.append('cache_counter_unavailable_or_reset:' + field)
            elif field == 'lookup_miss_total':
                miss_delta = right - left
            else:
                hit_delta = right - left
    old_i = before_metrics.get('ingress') if isinstance(before_metrics, dict) else None
    new_i = after_metrics.get('ingress') if isinstance(after_metrics, dict) else None
    if not isinstance(old_i, dict) or not isinstance(new_i, dict):
        reasons.append('ingress_metrics_unavailable')
    else:
        left, right = old_i.get('DuplicateCommitted'), new_i.get('DuplicateCommitted')
        if type(left) is int and type(right) is int and right >= left:
            duplicate_delta = right - left
        else:
            reasons.append('duplicate_metric_unavailable')
    old_p = before_metrics.get('persistence') if isinstance(before_metrics, dict) else None
    new_p = after_metrics.get('persistence') if isinstance(after_metrics, dict) else None
    if not isinstance(old_p, dict) or not isinstance(new_p, dict):
        reasons.append('persistence_metrics_unavailable')
    else:
        left, right = old_p.get('begin_attempts'), new_p.get('begin_attempts')
        if type(left) is int and type(right) is int and right > left:
            begin_delta = right - left
        else:
            reasons.append('database_transaction_not_observed')
        left, right = old_p.get('confirmed_commits'), new_p.get('confirmed_commits')
        if type(left) is int and type(right) is int and right >= left:
            commit_delta = right - left
            if commit_delta <= 0:
                reasons.append('confirmed_database_commit_not_observed')
        else:
            reasons.append('persistence_metric_unavailable:confirmed_commits')
        for field, result_key in (('confirmed_new_rows', 'confirmed_new_rows_delta'),
                                  ('copy_calls', 'copy_calls_delta')):
            left, right = old_p.get(field), new_p.get(field)
            delta = right - left if type(left) is int and type(right) is int else None
            if field == 'confirmed_new_rows':
                new_rows_delta = delta
            else:
                copy_delta = delta
            if delta != 0:
                reasons.append('new_persistence_work:' + field)
        stored_before = cache_before.get('publication_stored_total') if isinstance(cache_before, dict) else None
        stored_after = cache_after.get('publication_stored_total') if isinstance(cache_after, dict) else None
        if type(stored_before) is not int or stored_before != stored_after:
            reasons.append('database_duplicate_published_cache_proof')
    if not isinstance(old_i, dict) or not isinstance(new_i, dict):
        reasons.append('ingress_new_data_metrics_unavailable')
    else:
        for field, result_key in (('Committed', 'ingress_committed_delta'),
                                  ('SamplesNew', 'ingress_samples_new_delta')):
            left, right = old_i.get(field), new_i.get(field)
            delta = right - left if type(left) is int and type(right) is int else None
            if field == 'Committed':
                committed_delta = delta
            else:
                samples_new_delta = delta
            if delta != 0:
                reasons.append('new_ingress_work:' + field)
    unchanged = before_db == after_db
    if not unchanged:
        reasons.append('database_changed')
    ack_valid = False
    try:
        if not isinstance(ack_payload, bytes):
            raise ValueError('ack_missing')
        ack_json = json.loads(ack_payload.decode('utf-8'))
        verify_ack(selected['message_id'], ack_payload)
        ack_valid = (ack_topic == 'gateways/' + selected['gateway_id'] + '/acks/' + selected['message_id']
                     and ack_json.get('status') == 'committed')
        if not ack_valid:
            reasons.append('ack_identity_or_outcome_mismatch')
    except (KeyError, TypeError, ValueError, RuntimeError, UnicodeError):
        reasons.append('ack_invalid_or_missing')
    payload_match = selected.get('payload_sha256') == selected.get('original_bytes_sha256')
    if not payload_match:
        reasons.append('payload_provenance_mismatch')
    if miss_delta is None or miss_delta <= 0:
        reasons.append('cache_miss_not_observed')
    if hit_delta != 0:
        reasons.append('cold_replay_hit_cache')
    if duplicate_delta is None or duplicate_delta <= 0:
        reasons.append('database_duplicate_not_observed')
    return {'status': 'PASS' if not reasons else 'FAIL', 'reasons': reasons,
        'process_identity_changed': identity_changed, 'cache_entries_before': entries,
        'cache_entries_after': entries_after,
        'cache_miss_delta': miss_delta, 'cache_hit_delta': hit_delta,
        'db_duplicate_delta': duplicate_delta, 'begin_attempts_delta': begin_delta,
        'confirmed_commits_delta': commit_delta, 'confirmed_new_rows_delta': new_rows_delta,
        'copy_calls_delta': copy_delta, 'ingress_committed_delta': committed_delta,
        'ingress_samples_new_delta': samples_new_delta,
        'database_work_observed': (begin_delta is not None and begin_delta > 0
                                   and commit_delta is not None and commit_delta > 0),
        'application_ack': ack_valid, 'database_unchanged': unchanged,
        'payload_sha256_matches': payload_match}


def warm_replay_evidence(before_metrics, after_metrics, before_db, after_db, *, selected,
                         ack_topic, ack_payload):
    """Evaluate one real, byte-exact replay without inferring absent counters."""
    from stage3_mqtt_ingress_runner import verify_ack

    reasons = []
    metric_fields = ('lookup_hit_total', 'database_calls_avoided_total')
    before_cache = before_metrics.get('ram_dedup') if isinstance(before_metrics, dict) else None
    after_cache = after_metrics.get('ram_dedup') if isinstance(after_metrics, dict) else None
    deltas = {}
    if (not isinstance(before_cache, dict) or not isinstance(after_cache, dict)
            or before_metrics.get('process_instance') != after_metrics.get('process_instance')):
        reasons.append('metrics_unavailable_or_process_changed')
    else:
        for field in metric_fields:
            left, right = before_cache.get(field), after_cache.get(field)
            if type(left) is not int or type(right) is not int or right < left:
                reasons.append('metric_unavailable_or_reset:' + field)
            else:
                deltas[field] = right - left
                if right <= left:
                    reasons.append('metric_not_incremented:' + field)
    duplicate_delta = None
    if isinstance(before_metrics, dict) and isinstance(after_metrics, dict):
        before_ingress = before_metrics.get('ingress')
        after_ingress = after_metrics.get('ingress')
        if isinstance(before_ingress, dict) and isinstance(after_ingress, dict):
            left, right = before_ingress.get('DuplicateCommitted'), after_ingress.get('DuplicateCommitted')
            if type(left) is int and type(right) is int and right >= left:
                duplicate_delta = right - left
            else:
                reasons.append('duplicate_committed_metric_unavailable')
        else:
            reasons.append('ingress_metrics_unavailable')
    if duplicate_delta is None or duplicate_delta <= 0:
        reasons.append('duplicate_committed_not_observed')
    persistence_fields = ('begin_attempts', 'confirmed_commits', 'copy_calls', 'confirmed_new_rows')
    before_persist = before_metrics.get('persistence') if isinstance(before_metrics, dict) else None
    after_persist = after_metrics.get('persistence') if isinstance(after_metrics, dict) else None
    persistence_delta = {}
    if not isinstance(before_persist, dict) or not isinstance(after_persist, dict):
        reasons.append('persistence_metrics_unavailable')
    else:
        for field in persistence_fields:
            left, right = before_persist.get(field), after_persist.get(field)
            if type(left) is not int or type(right) is not int:
                reasons.append('persistence_metric_unavailable:' + field)
            else:
                persistence_delta[field] = right - left
                if right != left:
                    reasons.append('database_work_changed:' + field)
    db_unchanged = before_db == after_db
    if not db_unchanged:
        reasons.append('database_changed')
    payload_match = (isinstance(selected, dict)
                     and selected.get('payload_sha256') == selected.get('original_bytes_sha256'))
    if not payload_match:
        reasons.append('payload_provenance_mismatch')
    ack_valid = False
    ack_status = None
    try:
        if not isinstance(ack_payload, bytes):
            raise ValueError('ack_missing')
        ack_json = json.loads(ack_payload.decode('utf-8'))
        verify_ack(selected['message_id'], ack_payload)
        ack_valid = ack_topic == 'gateways/' + selected['gateway_id'] + '/acks/' + selected['message_id']
        ack_status = ack_json.get('status')
        if not ack_valid:
            reasons.append('ack_identity_mismatch')
        # Existing protocol uses committed for both first commit and DuplicateCommitted.
        if ack_status != 'committed':
            reasons.append('ack_outcome_mismatch')
    except (KeyError, TypeError, ValueError, RuntimeError, UnicodeError):
        reasons.append('ack_invalid_or_missing')
    return {
        'status': 'PASS' if not reasons else 'FAIL', 'reasons': reasons,
        'process_identity_same': (isinstance(before_metrics, dict) and isinstance(after_metrics, dict)
                                  and before_metrics.get('process_instance') == after_metrics.get('process_instance')),
        'cache_hit_delta': deltas.get('lookup_hit_total'),
        'database_calls_avoided_delta': deltas.get('database_calls_avoided_total'),
        'duplicate_committed_delta': duplicate_delta,
        'persistence_counter_deltas': persistence_delta,
        'database_unchanged': db_unchanged, 'payload_sha256_matches': payload_match,
        'message_id': selected.get('message_id') if isinstance(selected, dict) else None,
        'topic': selected.get('topic') if isinstance(selected, dict) else None,
        'payload_sha256': selected.get('payload_sha256') if isinstance(selected, dict) else None,
        'application_ack': ack_valid, 'ack_status': ack_status,
        'ack_provenance': 'manual_fixture_replay_application_ack',
        'outbox_readback': 'not_applicable_manual_replay',
    }


def duplicate_replay_evidence(*, mode, offered_attempts, received, acked, before_metrics,
                              after_metrics, before_db, after_db, exact_bytes, fresh_acks=None):
    """Reconcile the finite manual duplicate window without inventing absent metrics."""
    reasons = []
    if mode not in ('off', 'on'):
        reasons.append('cache_mode_unavailable')
    if offered_attempts != 24:
        reasons.append('offered_attempt_count_mismatch')
    if received != offered_attempts:
        reasons.append('received_count_mismatch')
    if acked != offered_attempts:
        reasons.append('ack_count_mismatch')
    if fresh_acks != offered_attempts:
        reasons.append('fresh_application_ack_count_mismatch')
    if not exact_bytes:
        reasons.append('captured_payload_bytes_changed')
    if (not isinstance(before_metrics, dict) or not isinstance(after_metrics, dict)
            or before_metrics.get('process_instance') != after_metrics.get('process_instance')):
        reasons.append('process_snapshot_unavailable_or_changed')

    def delta(section, field):
        before = before_metrics.get(section, {}) if isinstance(before_metrics, dict) else {}
        after = after_metrics.get(section, {}) if isinstance(after_metrics, dict) else {}
        left, right = before.get(field), after.get(field)
        if type(left) is not int or type(right) is not int or right < left:
            reasons.append('metric_unavailable_or_reset:' + section + '.' + field)
            return None
        return right - left

    duplicate_delta = delta('ingress', 'DuplicateCommitted')
    committed_delta = delta('ingress', 'Committed')
    admitted_delta = delta('ingress', 'Processed')
    samples_delta = delta('ingress', 'SamplesNew')
    ack_published_delta = delta('ingress', 'ACKPublished')
    begin_delta = delta('persistence', 'begin_attempts')
    commit_delta = delta('persistence', 'confirmed_commits')
    copy_delta = delta('persistence', 'copy_calls')
    rows_delta = delta('persistence', 'confirmed_new_rows')
    hit_delta = delta('ram_dedup', 'lookup_hit_total')
    avoided_delta = delta('ram_dedup', 'database_calls_avoided_total')
    if duplicate_delta != offered_attempts:
        reasons.append('database_duplicate_count_mismatch')
    if committed_delta != 0:
        reasons.append('unexpected_new_ingress_commits')
    if samples_delta != 0:
        reasons.append('unexpected_new_samples')
    if ack_published_delta != offered_attempts:
        reasons.append('application_ack_publish_count_mismatch')
    db_duplicate_delta = (duplicate_delta - hit_delta
                          if duplicate_delta is not None and hit_delta is not None else None)
    if db_duplicate_delta is None or db_duplicate_delta < 0:
        reasons.append('db_duplicate_count_unavailable_or_invalid')
    if copy_delta != 0 or rows_delta != 0:
        reasons.append('new_commit_or_copy_during_duplicate_window')
    if mode == 'off':
        if hit_delta != 0 or avoided_delta != 0:
            reasons.append('cache_work_observed_while_disabled')
        if (begin_delta is None or begin_delta <= 0 or begin_delta > db_duplicate_delta
                or commit_delta is None or commit_delta <= 0 or commit_delta > begin_delta):
            reasons.append('database_duplicate_transaction_work_not_observed')
        if db_duplicate_delta != offered_attempts:
            reasons.append('db_duplicate_outcome_count_mismatch')
    elif mode == 'on':
        if hit_delta != offered_attempts or avoided_delta != offered_attempts:
            reasons.append('warm_cache_hits_or_calls_avoided_not_observed')
        if begin_delta is None or begin_delta != 0 or commit_delta != 0:
            reasons.append('duplicate_window_db_work_not_reduced')
    if before_db != after_db:
        reasons.append('persisted_data_changed')
    return {
        'status': 'PASS' if not reasons else 'FAIL', 'reasons': reasons,
        'cache_mode': mode, 'offered_attempts': offered_attempts,
        'received': received, 'application_acks': acked,
        'fresh_application_acks': fresh_acks,
        'admitted': admitted_delta, 'ingress_commits': committed_delta,
        'new_samples': samples_delta,
        'admission_denominator': offered_attempts,
        'retries': 0, 'retry_basis': 'manual publisher has no automatic retry loop',
        'denominators': {'offered': offered_attempts, 'received': received,
                         'cache_hits': hit_delta, 'application_acks': acked},
        'ingress_duplicate_outcome_delta': duplicate_delta,
        'database_duplicate_delta': db_duplicate_delta,
        'cache_hit_delta': hit_delta, 'database_calls_avoided_delta': avoided_delta,
        'persistence_deltas': {'begin_attempts': begin_delta,
            'confirmed_commits': commit_delta, 'copy_calls': copy_delta,
            'confirmed_new_rows': rows_delta},
        'database_unchanged': before_db == after_db, 'exact_captured_bytes': bool(exact_bytes),
        'manual_outbox_readback': 'not_applicable_manual_replay',
        'ack_latency_scope': 'manual_fixture end-to-end including CLI and observer cost',
    }


def persist_duplicate_replay_evidence(run_data, evidence, persist):
    """Persist only safe measurement fields before the caller raises on FAIL."""
    run_data['duplicate_replay'] = safe_artifact(evidence)
    persist()
    return run_data['duplicate_replay']


def conflict_guard_query(case_name):
    """Return the exact DB precondition query for one guarded conflict case."""
    queries = {
        'same_id_changed_payload': ("SELECT EXISTS (SELECT 1 FROM processed_messages "
            "WHERE gateway_id=:'gw' AND message_id=:'mid'::uuid AND payload_hash <> :'hash');"),
        'same_id_moved_sensor_time_key': ("SELECT EXISTS (SELECT 1 FROM processed_messages "
            "WHERE gateway_id=:'gw' AND message_id=:'mid'::uuid AND "
            "(sensor_id <> :'sensor' OR timestamp <> :'timestamp'::timestamptz));"),
        'same_ram_key_different_id': ("SELECT EXISTS (SELECT 1 FROM processed_messages "
            "WHERE gateway_id=:'gw' AND sensor_id=:'sensor' AND boot_id=:'boot'::uuid "
            "AND first_sequence <= :'last'::bigint AND last_sequence >= :'first'::bigint "
            "AND message_id <> :'mid'::uuid);"),
        'different_first_sequence_overlap': ("SELECT EXISTS (SELECT 1 FROM processed_messages "
            "WHERE gateway_id=:'gw' AND sensor_id=:'sensor' AND boot_id=:'boot'::uuid "
            "AND first_sequence <= :'last'::bigint AND last_sequence >= :'first'::bigint "
            "AND message_id <> :'mid'::uuid);"),
    }
    try:
        return queries[case_name]
    except KeyError:
        raise ValueError('paper_p5b_unknown_conflict_guard') from None


def conflict_rejection_reasons():
    # The repository inserts a fresh message ID before checking sequence ranges;
    # an overlapping range therefore reports ReasonSequenceOverlap.
    return {
        'same_id_changed_payload': 'message_conflict',
        'same_id_moved_sensor_time_key': 'message_conflict',
        'same_ram_key_different_id': 'sequence_overlap',
        'different_first_sequence_overlap': 'sequence_overlap',
    }


def provision_conflict_sensor(smoke, endpoint, target, tokens, run_data, persist):
    moved_sensor_id = target.sensor_id + '-conflict'
    status = smoke.http_request(endpoint, 'PUT',
        f'/v1/gateways/{target.gateway_id}/sensors/{moved_sensor_id}', token=tokens['b'],
        body={'name': 'Cache conflict sensor', 'unit': target.sensor_unit})[0]
    run_data['conflict_sensor_provision'] = {'status': status}
    persist()
    # The P5b profile may have created this Sensor in initial fixture setup;
    # the owner endpoint returns 200 for that idempotent existing resource.
    if status not in (200, 201):
        raise ValueError('paper_p5b_conflict_sensor_provision_failed')
    return moved_sensor_id


def overlap_setup_payload(replay_payload, *, first_sequence, measured_at):
    import uuid

    source = json.loads(replay_payload)
    source.update(message_id=str(uuid.uuid4()), first_sequence=first_sequence,
                  sample_count=2, measured_at=measured_at)
    source['samples'] = [float(source['samples'][0]), float(source['samples'][0]) + 0.01]
    return json.dumps(source, separators=(',', ':')).encode()


def conflict_payloads(original_payload, *, gateway_id, original_sensor_id, conflict_sensor_id, now):
    """Derive four schema-valid guarded-conflict payloads from one real delivery."""
    import uuid

    try:
        original = json.loads(original_payload.decode('utf-8'))
        fields = {'protocol_version', 'message_id', 'message_type', 'gateway_id', 'sensor_id',
                  'boot_id', 'first_sequence', 'sample_count', 'measured_at',
                  'sample_interval_us', 'samples'}
        if (not isinstance(original, dict) or set(original) != fields
                or original['protocol_version'] != 1 or original['message_type'] != 'telemetry_batch'
                or original['gateway_id'] != gateway_id or original['sensor_id'] != original_sensor_id
                or not isinstance(original['samples'], list) or not original['samples']
                or len(original['samples']) != original['sample_count']):
            raise ValueError
        now_dt = datetime.fromisoformat(now.replace('Z', '+00:00')).astimezone(timezone.utc)
        base_dt = datetime.fromisoformat(original['measured_at'].replace('Z', '+00:00')).astimezone(timezone.utc)
        if base_dt >= now_dt:
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError('paper_p5b_conflict_source_rejected') from None

    def encode(item):
        if (item['sample_count'] != len(item['samples']) or item['sample_count'] < 1
                or item['first_sequence'] < 0 or item['measured_at'] >= now_dt.isoformat()):
            raise ValueError('paper_p5b_conflict_mutation_rejected')
        return json.dumps(item, separators=(',', ':')).encode('utf-8')

    same_id_payload = dict(original)
    same_id_payload['samples'] = list(original['samples'])
    same_id_payload['samples'][0] = float(same_id_payload['samples'][0]) + 0.01

    moved = dict(original)
    moved.update(sensor_id=conflict_sensor_id, first_sequence=original['first_sequence'] + 200,
                 measured_at=(now_dt - timedelta(minutes=1)).isoformat().replace('+00:00', 'Z'))

    same_key = dict(original)
    same_key['message_id'] = str(uuid.uuid4())

    # Caller supplies an independently committed, two-sample functional message.
    # Its range must start above zero so the overlap can use a distinct cache key.
    overlap = dict(original)
    overlap['message_id'] = str(uuid.uuid4())
    if original['sample_count'] < 2 or original['first_sequence'] <= 0:
        raise ValueError('paper_p5b_overlap_source_not_independent')
    overlap['first_sequence'] = original['first_sequence'] - 1
    overlap['sample_count'] = 2
    overlap['samples'] = [float(original['samples'][0]) - 0.01,
                          float(original['samples'][1]) - 0.01]
    overlap['measured_at'] = (now_dt - timedelta(minutes=2)).isoformat().replace('+00:00', 'Z')

    cases = {
        'same_id_changed_payload': encode(same_id_payload),
        'same_id_moved_sensor_time_key': encode(moved),
        'same_ram_key_different_id': encode(same_key),
        'different_first_sequence_overlap': encode(overlap),
    }
    from types import SimpleNamespace
    from stage3_uplink_runner import probe_batch_is_valid
    for name, payload in cases.items():
        target_sensor = conflict_sensor_id if name == 'same_id_moved_sensor_time_key' else original_sensor_id
        count = 2 if name == 'different_first_sequence_overlap' else original['sample_count']
        if not probe_batch_is_valid(payload, SimpleNamespace(gateway_id=gateway_id, sensor_id=target_sensor),
                                    expected_samples=count):
            raise ValueError('paper_p5b_conflict_mutation_rejected')
    return cases


def conflict_guard_variables(conflict_message, *, gateway_id, conflict_id, conflict_bytes):
    """Build precheck parameters from the batch envelope and canonical payload."""
    return {'gw': gateway_id, 'mid': conflict_id,
        'hash': hashlib.sha256(conflict_bytes).hexdigest(),
        'sensor': conflict_message['sensor_id'], 'boot': conflict_message['boot_id'],
        'timestamp': conflict_message['measured_at'],
        'first': str(conflict_message['first_sequence']),
        'last': str(conflict_message['first_sequence'] + conflict_message['sample_count'] - 1)}


def conflict_evidence(before_metrics, after_metrics, before_db, after_db, *,
                      expected_lookup, ack_payload, guard_reason, guard_precondition,
                      rejection_reason, expected_rejection_reason):
    """Require DB guard failure and stable state; missing ACK alone is not proof."""
    reasons = []
    if (not isinstance(before_metrics, dict) or not isinstance(after_metrics, dict)
            or before_metrics.get('process_instance') != after_metrics.get('process_instance')):
        reasons.append('metrics_unavailable_or_process_changed')
    before_ingress = before_metrics.get('ingress', {}) if isinstance(before_metrics, dict) else {}
    after_ingress = after_metrics.get('ingress', {}) if isinstance(after_metrics, dict) else {}
    for field in ('Committed', 'DuplicateCommitted', 'SamplesNew', 'ACKPublished', 'ACKPublishFailures',
                  'ValidationRejected', 'ShaperRateLimited'):
        if type(before_ingress.get(field)) is not int or after_ingress.get(field) != before_ingress.get(field):
            reasons.append('unexpected_ingress_delta:' + field)
    if (type(before_ingress.get('Received')) is not int
            or after_ingress.get('Received') != before_ingress.get('Received') + 1):
        reasons.append('delivery_not_observed')
    for field in ('InFlight', 'QueueDepth'):
        if before_ingress.get(field) != 0 or after_ingress.get(field) != 0:
            reasons.append('writers_not_quiescent:' + field)
    failure_delta = after_ingress.get('PersistFailures', -1) - before_ingress.get('PersistFailures', -1)
    if type(failure_delta) is not int or failure_delta != 1:
        reasons.append('db_guard_rejection_not_observed')
    cache_fields = {'metadata_mismatch': 'lookup_metadata_mismatch_total', 'miss': 'lookup_miss_total'}
    cache_field = cache_fields.get(expected_lookup)
    before_cache = before_metrics.get('ram_dedup', {}) if isinstance(before_metrics, dict) else {}
    after_cache = after_metrics.get('ram_dedup', {}) if isinstance(after_metrics, dict) else {}
    if (cache_field is None or type(before_cache.get(cache_field)) is not int
            or type(after_cache.get(cache_field)) is not int
            or after_cache[cache_field] - before_cache[cache_field] != 1):
        reasons.append('expected_cache_guard_not_observed')
    if after_cache.get('lookup_hit_total') != before_cache.get('lookup_hit_total'):
        reasons.append('cache_hit_observed')
    if after_cache.get('database_calls_avoided_total') != before_cache.get('database_calls_avoided_total'):
        reasons.append('repository_call_avoided')
    if before_db != after_db:
        reasons.append('database_changed')
    if not isinstance(guard_reason, str) or not guard_reason:
        reasons.append('guard_error_classification_missing')
    if rejection_reason != expected_rejection_reason:
        reasons.append('structured_rejection_reason_missing_or_unexpected')
    if not isinstance(guard_precondition, dict) or guard_precondition.get('matched') is not True:
        reasons.append('intended_database_guard_precondition_missing')
    return {'status': 'PASS' if not reasons and ack_payload is None else 'FAIL',
            'reasons': reasons + (['application_ack_observed'] if ack_payload is not None else []),
            'application_ack': ack_payload is not None, 'guard_reason': guard_reason,
            'guard_precondition': guard_precondition,
            'database_unchanged': before_db == after_db,
            'cache_lookup_kind': expected_lookup, 'persist_failure_delta': failure_delta}


def workload_config(dataset):
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p5a_dataset_rejected')
    count = 12
    samples = DATASET_PROFILES[dataset]['samples_per_message']
    config = {
        'gateway_count': 2,
        'messages_per_gateway': count,
        'samples_per_message': samples,
        'messages_per_group_target': 4,
        'steady_sample_rate_hz': 100,
        'run_deadline_seconds': 150,
        'drain_deadline_seconds': 90,
        'comparison': 'same gateway order, message count, sample rate, values, limits and retries in off/on',
        'generation': 'finite Gateway simulator --batches; measured separately from drain/commit',
        'retry_delivery': 'real Gateway SQLite outbox and application commit ACK',
    }
    config['offered_unique_messages'] = config['gateway_count'] * count
    config['offered_unique_samples'] = config['offered_unique_messages'] * samples
    return config


def observer_specs(config):
    """Unique observer identities with a bounded lifetime, selecting telemetry explicitly."""
    return [
        {'tag': 'p5a-telemetry-a', 'kind': 'telemetry',
         'count': config['offered_unique_messages'], 'timeout': 90},
        {'tag': 'p5a-telemetry-b', 'kind': 'telemetry',
         'count': config['offered_unique_messages'], 'timeout': 90},
    ]


def gateway_measurement_args(config):
    return ['--measure', '--measure-capacity', str(config['messages_per_gateway'])]


TEMPORAL_RAW_WINDOW = timedelta(seconds=9)


def temporal_read_windows(first, last):
    """Partition exact telemetry bounds into raw windows below the fixture's 10s cap."""
    try:
        start = datetime.fromisoformat(first.replace('Z', '+00:00'))
        final = datetime.fromisoformat(last.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        raise ValueError('paper_p5a_read_failed') from None
    if start.utcoffset() is None or final.utcoffset() is None or final < start:
        raise ValueError('paper_p5a_read_failed')
    # Temporal ranges are [from, to); one microsecond includes the last stored
    # timestamp without widening beyond the bounded raw-query window.
    end_exclusive = final + timedelta(microseconds=1)
    windows = []
    cursor = start
    while cursor < end_exclusive:
        end = min(cursor + TEMPORAL_RAW_WINDOW, end_exclusive)
        windows.append((cursor, end))
        cursor = end
    return windows


def _utc_z(value):
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def temporal_read_path(smoke, sensor_urn, first, last, max_points):
    if not isinstance(sensor_urn, str) or not re.fullmatch(r'urn:ngsi-ld:Sensor:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+', sensor_urn):
        raise ValueError('paper_p5a_read_failed')
    if type(max_points) is not int or not 100 <= max_points <= 2000:
        raise ValueError('paper_p5a_read_failed')
    del smoke  # Keep the existing helper call shape; this endpoint uses exact split bounds.
    query = urllib.parse.urlencode({
        'attrs': 'measurement', 'timerel': 'between',
        'timeAt': first if isinstance(first, str) else _utc_z(first),
        'endTimeAt': last if isinstance(last, str) else _utc_z(last),
        'max_points': str(max_points),
    })
    return ('/v1/digital-twins/' + urllib.parse.quote(sensor_urn, safe=':') + '/temporal?'
            + query)


def temporal_count_matches(body, expected):
    values = body.get('measurement', {}).get('values', []) if isinstance(body, dict) else []
    return isinstance(values, list) and len(values) == expected


def expected_measurement_points(config):
    return config['messages_per_gateway'] * config['samples_per_message']


def history_max_points(config):
    # The API's minimum is 100; never request more than the contract hard cap.
    return min(2000, max(100, expected_measurement_points(config)))


def history_read_diagnostic(sensor_urn, twin_status, twin_body, temporal_status,
                            temporal_body, *, expected_points, max_points, first, last,
                            query_windows=None):
    twin_shape_ok = isinstance(twin_body, dict)
    reported_state = twin_body.get('reported_state') if twin_shape_ok else None
    reported_version = twin_body.get('reported_version') if twin_shape_ok else None
    twin_state_ok = (twin_shape_ok and twin_body.get('id') == sensor_urn
                     and isinstance(reported_state, dict) and bool(reported_state)
                     and isinstance(reported_version, str) and reported_version.isdigit()
                     and int(reported_version) > 0)
    values = None
    if isinstance(temporal_body, dict):
        measurement = temporal_body.get('measurement')
        if isinstance(measurement, dict) and isinstance(measurement.get('values'), list):
            values = measurement['values']
    actual_points = len(values) if values is not None else None
    reason = None
    if twin_status != 200:
        reason = 'twin_http_error'
    elif not twin_shape_ok:
        reason = 'twin_body_malformed'
    elif not twin_state_ok:
        reason = 'twin_state_mismatch'
    elif temporal_status != 200:
        reason = 'temporal_http_error'
    elif actual_points is None:
        reason = 'temporal_body_malformed'
    elif actual_points != expected_points:
        reason = 'temporal_point_count_mismatch'
    return {
        'sensor_urn': sensor_urn,
        'twin_http_status': twin_status,
        'twin_state_valid': twin_state_ok,
        'temporal_http_status': temporal_status,
        'expected_points': expected_points,
        'actual_points': actual_points,
        'query': {'attrs': 'measurement', 'max_points': max_points,
                  'window_source': 'telemetry.measured_at_min_max',
                  'first_measured_at': first, 'last_measured_at': last,
                  'range_semantics': '[timeAt,endTimeAt); last sample included with 1us end extension',
                  'time_format': 'UTC-Z', 'windows': query_windows or []},
        'reason': reason,
        'accepted': reason is None,
    }


def persist_history_read_diagnostic(run_data, diagnostic, persist):
    run_data.setdefault('history_reads', []).append(safe_artifact(diagnostic))
    persist()
    if not diagnostic['accepted']:
        raise ValueError('paper_p5a_read_regression_failed')


def gateway_measurement(output):
    """Preserve actual bounded ACK summary; never substitute window durations for latency."""
    from stage3_workload_runner import parse_measurement_summary

    parsed = parse_measurement_summary(output)
    if not parsed:
        reason = 'summary_missing' if 'measurement summary:' not in output else 'summary_malformed'
        return {'status': 'unavailable', 'reason': reason}
    keys = ('samples', 'p50_ms', 'p95_ms', 'min_ms', 'max_ms', 'first_publishes',
            'retry_attempts', 'missing_origins', 'overflow_samples', 'gen_duration_ms',
            'publish_drain_duration_ms', 'total_duration_ms')
    result = {'status': 'available', **{key: parsed[key] for key in keys}}
    # ACK durations are bounded exact samples, not a histogram; CLI percentiles
    # are rendered to two decimal places in milliseconds.
    result['histogram_resolution_ms'] = None
    result['reported_resolution_ms'] = 0.01 if parsed['samples'] and parsed['p50_ms'] != 'none' else None
    result['resolution_basis'] = 'no histogram; duration samples, CLI rendering rounds to 0.01 ms'
    return result


def validate_accounting(config, accounting):
    expected = {
        'offered_unique': config['offered_unique_messages'],
        'committed_unique': config['offered_unique_messages'],
        'raw_samples': config['offered_unique_samples'],
        'temporal': config['offered_unique_messages'],
        'application_acks': config['offered_unique_messages'],
        'outbox_rows': 0, 'outbox_bytes': 0, 'payload_budget_bytes': 0,
    }
    if any(type(accounting.get(key)) is not int or accounting[key] != value
           for key, value in expected.items()):
        raise ValueError('paper_p5a_accounting_rejected')
    return True


def acceptance_ready(checks, mode):
    if mode not in ('off', 'on') or not REQUIRED_CHECKS.issubset(checks):
        return False
    if any(checks[name].get('status') != 'PASS' for name in REQUIRED_CHECKS):
        return False
    group = checks['group_metrics_observed']
    if mode == 'on' and (group.get('max_members', 0) < 2 or group.get('groups', 0) < 1):
        return False
    if mode == 'off' and group.get('microbatch_enabled') is not False:
        return False
    return True


def safe_artifact(value):
    if isinstance(value, dict):
        return {key: safe_artifact(item) for key, item in value.items()
                if key.lower() not in {'password', 'token', 'secret', 'payload', 'payload_bytes', 'raw_payload'}
                and not key.lower().endswith(('_password', '_token', '_secret'))}
    if isinstance(value, list):
        return [safe_artifact(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ValueError('paper_p5a_artifact_rejected')


def run(smoke, fixture, endpoint, registry, containers, images, lifecycle_state):
    """Run a finite two-Gateway workload; every success is read back from runtime."""
    if not getattr(fixture, 'paper_p5a', False):
        raise ValueError('paper_p5a_fixture_required')
    if any(value is None for value in (endpoint, registry, containers, images, lifecycle_state)):
        raise ValueError('paper_p5a_fixture_required')

    import stage3_uplink_runner as uplink

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, lifecycle_state))
    mode, dataset = fixture.paper_p5a_mode, fixture.paper_p5a_dataset
    config = workload_config(dataset)
    shutdown_smoke = bool(getattr(fixture, 'paper_p5a_shutdown', False))
    cache_smoke = bool(getattr(fixture, 'paper_p5b_cache_smoke', False))
    effective_environment = effective.get('services', {}).get('backend', {}).get('environment', {})
    if getattr(fixture, 'paper_p5b', False):
        profile_data = p5b_run_metadata(fixture.paper_p5b_cache_mode, dataset,
                                        effective_environment=effective_environment)
    else:
        profile_data = run_metadata(mode, dataset, shutdown_smoke=shutdown_smoke,
                                    effective_environment=effective_environment, cache_smoke=cache_smoke)
    if quota != BACKEND_QUOTA or not isinstance(endpoint, smoke.HTTPSEndpoint):
        raise ValueError('paper_p5a_profile_rejected')
    if len(specs) < 2 or len(passwords) < 2:
        raise ValueError('paper_p5a_fixture_required')

    run_data = {
        'schema_version': 1, 'run_id': fixture.project, 'status': 'FAIL',
        'mode': mode, 'dataset': profile_data['dataset'], 'native_executed': True,
        'revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'branch': smoke.paper_p0_git(fixture, 'branch', '--show-current'),
        'dirty': bool(smoke.paper_p0_git(fixture, 'status', '--porcelain')),
        'auth_mode': 'rs256', 'https_verified': True, 'backend_quota': quota,
        'service_resources': resources,
        'images': {name: images[name] for name in smoke.fixture_services(fixture)},
        'gateway_image_id': images['gateway'], 'effective_profile': profile_data['effective_profile'],
        'effective_profile_source': profile_data['effective_profile_source'],
        'shaper': profile_data['shaper'], 'auth_tls_acl': profile_data['auth_tls_acl'],
        'storage_status': storage_status, 'workload': config, 'phases': [], 'checks': {},
        'history_reads': [], 'database_schema_version': profile_data['database_schema_version'],
        'offered_pattern': {'status': 'same_finite_gateway_cli_pattern_for_both_modes',
                            'cadence': "Gateway simulator configured sample-rate controls generation; no synthetic future samples"},
        'cleanup_verified': False, 'ram_cache': profile_data['ram_cache'],
    }
    metrics, resource_rows = [], []
    started = time.monotonic()

    def persist():
        smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))

    def check(name, success, **detail):
        run_data['checks'][name] = {'status': 'PASS' if success else 'FAIL', **safe_artifact(detail)}
        persist()
        if not success:
            raise ValueError('paper_p5a_acceptance_failed')

    def sql(query, **variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def scrape(phase):
        lifecycle_state.checkpoint()
        status, _, snapshot = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        if status != 200 or not isinstance(snapshot, dict) or not isinstance(snapshot.get('ingress'), dict):
            raise ValueError('paper_p5a_metrics_unavailable')
        sample = {'phase': phase, 'observed_monotonic': time.monotonic(), 'snapshot': snapshot}
        metrics.append(sample)
        smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', metrics)
        resource = smoke.paper_p0_stats(registry, containers, phase)
        resource_rows.append(resource)
        smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', resource_rows, resources)
        return sample

    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    volumes = []
    for index in range(2):
        volume = registry.intent('volume', fixture.project + '_outbox_p5a_' + str(index), None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, volume['name']])
        if result.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')
        volumes.append(volume)

    def gateway(index, tag, *, during=None):
        spec = specs[index]
        env = uplink.gateway_env(spec.gateway_id, passwords[index])
        env.update(GATEWAY_MQTT_CLIENT_ID=fixture.project + '_p5a_' + str(index) + '_' + tag,
                   GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_SAMPLE_RATE_HZ=str(config['steady_sample_rate_hz']),
                   GATEWAY_SAMPLES_PER_BATCH=str(config['samples_per_message']))
        env_path = fixture.private / ('gateway-p5a-' + str(index) + '-' + tag + '.env')
        smoke.private_write(env_path, ''.join(key + '=' + value + '\n' for key, value in env.items()))
        return smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[index], env_path,
            fixture.private / 'certs/ca.crt', 'p5a-' + str(index) + '-' + tag,
            ['run', '--batches', str(config['messages_per_gateway']), '--completion-timeout', '80s',
             *gateway_measurement_args(config)],
            lifecycle_state, during=during, timeout=90)

    def gateway_count(index):
        spec = specs[index]
        env_path = fixture.private / ('gateway-p5a-' + str(index) + '-count.env')
        smoke.private_write(env_path, 'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
        output = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[index], env_path,
            fixture.private / 'certs/ca.crt', 'p5a-' + str(index) + '-count', ['count'],
            lifecycle_state, timeout=30)
        return output.strip()

    observers = []
    try:
        before = scrape('before_generation')
        for index, observer_spec in enumerate(observer_specs(config)):
            observers.append(smoke.paper_p1_subscriber(fixture, registry,
                containers['mosquitto']['receipt']['id'], specs[index], passwords[index], observer_spec['tag'],
                message_count=observer_spec['count'], message_timeout=observer_spec['timeout'],
                subscription_kind=observer_spec['kind']))
        run_data['phases'].append({'name': 'generation_start', 'observed_monotonic': before['observed_monotonic']})
        second_output = []
        gateway_windows = {}

        def concurrent_second_gateway():
            gateway_windows['second_start'] = time.monotonic()
            second_output.append(gateway(1, 'generation'))
            gateway_windows['second_end'] = time.monotonic()

        gateway_windows['first_start'] = time.monotonic()
        first_output = gateway(0, 'generation', during=concurrent_second_gateway)
        gateway_windows['first_end'] = time.monotonic()
        summaries = []
        ack_measurements = []
        for index, output in enumerate((first_output, second_output[0] if second_output else '')):
            from stage3_mqtt_ingress_runner import parse_run_summary
            summary = parse_run_summary(output)
            if (summary.get('completed') != 'true'
                    or int(summary.get('enqueued', '-1')) != config['messages_per_gateway']
                    or int(summary.get('app_acknowledged', '-1')) != config['messages_per_gateway']
                    or summary.get('pending_rows') != '0' or summary.get('pending_bytes') != '0'):
                raise ValueError('paper_p5a_gateway_summary_rejected')
            summaries.append(summary)
            measurement = gateway_measurement(output)
            ack_measurements.append(measurement)
            run_data['phases'].append({'name': 'gateway_' + str(index) + '_generation_and_ack',
                'start_monotonic': gateway_windows['first_start' if index == 0 else 'second_start'],
                'end_monotonic': gateway_windows['first_end' if index == 0 else 'second_end'],
                'duration_seconds': gateway_windows[('first_end' if index == 0 else 'second_end')]
                    - gateway_windows[('first_start' if index == 0 else 'second_start')],
                'enqueued': int(summary['enqueued']),
                'application_acknowledged': int(summary['app_acknowledged']),
                'pending_rows_at_cli_exit': int(summary['pending_rows']),
                'pending_bytes_at_cli_exit': int(summary['pending_bytes']),
                'ack_measurement': measurement})
        outbox_counts = [gateway_count(index) for index in range(2)]
        captured = []
        for index, spec in enumerate(specs[:2]):
            payload = smoke.paper_p1_capture_subscription(observers[index],
                'gateways/' + spec.gateway_id + '/telemetry/' + spec.sensor_id, timeout=10)
            if not isinstance(payload, bytes):
                raise ValueError('paper_p5a_observer_capture_failed')
            parsed = json.loads(payload)
            if not isinstance(parsed.get('message_id'), str):
                raise ValueError('paper_p5a_observer_capture_failed')
            captured.append((payload, parsed['message_id']))
        drained = scrape('drain_commit_end')
        ingress = drained['snapshot']['ingress']
        persistence = drained['snapshot'].get('persistence')
        if ingress.get('BufferedPayloadBytes') != 0:
            raise ValueError('paper_p5a_accounting_rejected')
        run_data['phases'].append({'name': 'generation_and_drain_complete',
            'observed_monotonic': drained['observed_monotonic'],
            'generation_plus_drain_seconds': drained['observed_monotonic'] - before['observed_monotonic'],
            'generation_window': 'Gateway generation and application-ACKing CLI run windows',
            'drain_commit_window': 'after final Gateway summary through final metrics sample'})

        ids = [specs[i].gateway_id for i in range(2)]
        ledgers = int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id IN (:'a',:'b');",
                          a=ids[0], b=ids[1]))
        raw = int(sql("SELECT count(*) FROM telemetry WHERE gateway_id IN (:'a',:'b');", a=ids[0], b=ids[1]))
        temporal = int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id IN (:'ua',:'ub');",
            ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        per_gateway = json.loads(sql(PER_GATEWAY_SUMMARY_SQL, a=ids[0], b=ids[1]))
        per_temporal = json.loads(sql(PER_TEMPORAL_SUMMARY_SQL,
          ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        mismatch = int(sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
          AND (p.sample_count <> (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
            OR p.first_sequence <> (SELECT min(sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
            OR p.last_sequence <> (SELECT max(sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id));""", a=ids[0], b=ids[1]))
        unique_values = int(sql("""SELECT count(*) FROM telemetry t JOIN processed_messages p USING(gateway_id,message_id)
          WHERE t.gateway_id IN (:'a',:'b') AND (t.boot_id <> p.boot_id OR t.sample_index <> t.sequence_num-p.first_sequence);""", a=ids[0], b=ids[1]))
        timestamp_mismatch = int(sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
          AND (SELECT max(t.measured_at)-min(t.measured_at) FROM telemetry t
               WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
              <> (p.sample_count-1)*10000*interval '1 microsecond';""", a=ids[0], b=ids[1]))
        raw_values = json.loads(sql("""SELECT coalesce(json_agg(json_build_array(sequence_num,value)
          ORDER BY gateway_id,message_id,sample_index),'[]'::json) FROM telemetry WHERE gateway_id IN (:'a',:'b');""",
          a=ids[0], b=ids[1]))
        value_mismatch = sum(abs(float(value) - uplink.synthetic_value(int(sequence))) > 1e-9
                             for sequence, value in raw_values)
        expected = config['offered_unique_messages']
        expected_samples = config['messages_per_gateway'] * config['samples_per_message']
        check('workload_accounting', ledgers == expected and raw == config['offered_unique_samples']
              and len(per_gateway) == 2 and all(row['messages'] == config['messages_per_gateway']
              and row['samples'] == expected_samples for row in per_gateway),
              offered_unique=expected, committed_unique=ledgers, raw_samples=raw,
              per_gateway=per_gateway)
        check('ledger_raw_provenance', mismatch == 0 and unique_values == 0
              and timestamp_mismatch == 0 and value_mismatch == 0,
              aggregate_mismatch=mismatch, provenance_mismatch=unique_values,
              timestamp_mismatch=timestamp_mismatch, synthetic_value_mismatch=value_mismatch)
        check('temporal_per_message', temporal == expected and len(per_temporal) == 2
              and all(row['observations'] == config['messages_per_gateway'] for row in per_temporal),
              temporal=temporal, per_sensor=per_temporal)
        states = json.loads(sql("""SELECT coalesce(json_agg(json_build_object(
          'reported_version',s.reported_version,'reported_state',s.reported_state) ORDER BY e.entity_id),'[]'::json)
          FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id IN (:'ua',:'ub');""",
          ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        check('current_state_and_versions', len(states) == 2 and all(
              row.get('reported_version', 0) > 0 and isinstance(row.get('reported_state'), dict)
              and bool(row['reported_state']) for row in states), state_rows=len(states),
              reported_versions=[row.get('reported_version') for row in states],
              reported_state_keys=[sorted(row.get('reported_state', {}).keys()) for row in states])
        ack_total = sum(int(item['app_acknowledged']) for item in summaries)
        empty_count = 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0'
        check('application_acks_and_empty_outboxes', ack_total == expected and all(
            item['pending_rows'] == '0' and item['pending_bytes'] == '0' for item in summaries)
            and all(value == empty_count for value in outbox_counts),
            application_acks=ack_total, outbox_count_readbacks=outbox_counts)

        micro = ingress.get('microbatch', {})
        check('group_metrics_observed', isinstance(micro, dict)
              and micro.get('enabled') is (mode == 'on')
              and (mode != 'on' or (micro.get('groups', 0) > 0 and micro.get('max_members', 0) >= 2)),
              microbatch_enabled=micro.get('enabled'), groups=micro.get('groups', 0),
              max_members=micro.get('max_members', 0), microbatch=micro)
        persistence_fields = ('single_attempts', 'group_attempts', 'fallback_attempts',
            'begin_attempts', 'begin_succeeded', 'confirmed_commits', 'rollback_attempts',
            'confirmed_rollbacks', 'commit_unconfirmed', 'copy_calls',
            'copy_returned_rows', 'confirmed_new_rows')
        check('transaction_copy_metrics_observed', isinstance(persistence, dict)
              and all(type(persistence.get(key)) is int and persistence[key] >= 0
                      for key in persistence_fields)
              and (persistence.get('group_attempts', 0) > 0 if mode == 'on'
                   else persistence.get('single_attempts', 0) > 0),
              persistence=persistence)
        check('payload_budget_drained', ingress.get('BufferedPayloadBytes') == 0,
              buffered_payload_bytes=ingress.get('BufferedPayloadBytes'))

        # Read the existing HTTPS APIs under the provisioned gateway identity.
        for index, spec in enumerate(specs[:2]):
            twin_status, _, twin_body = smoke.http_request(endpoint, 'GET', '/v1/digital-twins/' + spec.sensor_urn
                + '?format=simplified', token=tokens['a' if index == 0 else 'b'])
            times = json.loads(sql("SELECT json_build_object('first',min(measured_at),'last',max(measured_at)) "
                "FROM telemetry WHERE gateway_id=:'gw';", gw=spec.gateway_id))
            if not times.get('first') or not times.get('last'):
                raise ValueError('paper_p5a_read_regression_failed')
            max_points = history_max_points(config)
            expected_points = expected_measurement_points(config)
            query_windows = []
            combined_values = []
            all_windows_well_formed = True
            window_statuses = []
            for window_start, window_end in temporal_read_windows(times['first'], times['last']):
                history_path = temporal_read_path(smoke, spec.sensor_urn,
                    window_start, window_end, max_points)
                window_status, _, window_body = smoke.http_request(endpoint, 'GET', history_path,
                    token=tokens['a' if index == 0 else 'b'])
                measurement = window_body.get('measurement') if isinstance(window_body, dict) else None
                values = measurement.get('values') if isinstance(measurement, dict) else None
                if not isinstance(values, list):
                    all_windows_well_formed = False
                    actual_window_points = None
                else:
                    combined_values.extend(values)
                    actual_window_points = len(values)
                query_windows.append({'timeAt': _utc_z(window_start),
                    'endTimeAt': _utc_z(window_end), 'http_status': window_status,
                    'actual_points': actual_window_points})
                window_statuses.append(window_status)
            temporal_status = next((status for status in window_statuses if status != 200), 200)
            temporal_body = ({'measurement': {'values': combined_values}}
                             if all_windows_well_formed else {})
            diagnostic = history_read_diagnostic(spec.sensor_urn, twin_status, twin_body,
                temporal_status, temporal_body, expected_points=expected_points,
                max_points=max_points, first=times['first'], last=times['last'],
                query_windows=query_windows)
            persist_history_read_diagnostic(run_data, diagnostic, persist)
        check('https_reads', True, twin_and_temporal_status=200,
              temporal_points_per_sensor=expected_measurement_points(config),
              temporal_point_unit='raw measurement samples per Sensor',
              temporal_max_points=history_max_points(config),
              temporal_window_source='actual telemetry measured_at min/max partitioned below the 10-second raw-window bound')
        jwt_valid = all(smoke.paper_p2_support().decode_token(token)[0].get('alg') == 'RS256'
                        for token in tokens.values())
        check('rs256_https_mqtt_tls_acl', run_data['https_verified'] and jwt_valid
              and profile_data['auth_tls_acl']['acl_enforced'],
              auth='RS256', https=True, mqtt_tls=True, acl=True)

        def fingerprint(gateway_id, sensor_urn):
            return json.loads(sql("""SELECT json_build_object(
              'ledger', md5(coalesce((SELECT string_agg(to_jsonb(p)::text,'' ORDER BY p.message_id)
                FROM processed_messages p WHERE p.gateway_id=:'gw'),'')),
              'raw', md5(coalesce((SELECT string_agg(to_jsonb(t)::text,'' ORDER BY t.message_id,t.sample_index)
                FROM telemetry t WHERE t.gateway_id=:'gw'),'')),
              'temporal', md5(coalesce((SELECT string_agg(to_jsonb(v)::text,'' ORDER BY v.message_id,v.property_name)
                FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                WHERE e.entity_id=:'urn'),'')),
              'state', (""" + STATE_FINGERPRINT_SQL + "));", gw=gateway_id, urn=sensor_urn))

        replay_payload, selected_message_id = captured[1]
        replay_path = fixture.private / 'p5a-exact-replay-payload.bin'
        replay_fd = os.open(replay_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(replay_fd, 'wb') as replay_file:
            replay_file.write(replay_payload)
            replay_file.flush()
            os.fsync(replay_file.fileno())
        replay_payload = replay_path.read_bytes()
        target = specs[1]
        before_fp = fingerprint(target.gateway_id, target.sensor_urn)
        before_metrics = scrape('before_exact_replay')
        broker_id = containers['mosquitto']['receipt']['id']
        backend_id = containers['backend']['receipt']['id']
        replay_topic = 'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id
        ack_topic = 'gateways/' + target.gateway_id + '/acks/' + selected_message_id
        cache_ack_observer = None
        if cache_smoke:
            cache_ack_observer = smoke.paper_p1_subscriber(fixture, registry, broker_id,
                target, passwords[1], 'p5b-warm-replay-ack', message_count=1,
                message_timeout=12, subscription_kind='ack')
            observers.append(cache_ack_observer)
            before_metrics = scrape('p5b_warm_replay_before')
            before_db = {'fingerprint': fingerprint(target.gateway_id, target.sensor_urn),
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn';", urn=target.sensor_urn)),
                'version': sql(VERSION_SNAPSHOT_SQL, urn=target.sensor_urn)}
            run_data['warm_replay'] = {'status': 'IN_PROGRESS', 'process_identity': before_metrics['snapshot'].get('process_instance'),
                'schema_version': profile_data['database_schema_version'], 'cache_ttl_seconds': 600,
                'window_start_monotonic': before_metrics['observed_monotonic'],
                'selected_message_id': selected_message_id, 'topic': replay_topic,
                'payload_sha256': hashlib.sha256(replay_payload).hexdigest(),
                'payload_provenance': 'captured_actual_gateway_new_delivery',
                'outbox_readback': 'not_applicable_manual_replay'}
            persist()
        replay = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(target.gateway_id + '\n' + passwords[1] + '\n' +
             replay_topic + '\n').encode() + replay_payload)
        if replay.returncode:
            raise ValueError('paper_p5a_duplicate_replay_failed')
        replay_after = None
        replay_ack_payload = None
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            lifecycle_state.checkpoint()
            candidate = scrape('exact_replay_observed')
            old = before_metrics['snapshot']['ingress'].get('DuplicateCommitted')
            new = candidate['snapshot']['ingress'].get('DuplicateCommitted')
            old_ack = before_metrics['snapshot']['ingress'].get('ACKPublished')
            new_ack = candidate['snapshot']['ingress'].get('ACKPublished')
            if (isinstance(old, int) and isinstance(new, int) and new > old
                    and isinstance(old_ack, int) and isinstance(new_ack, int) and new_ack > old_ack):
                replay_after = candidate
            if cache_ack_observer is not None and replay_ack_payload is None:
                replay_ack_payload = smoke.paper_p1_capture_subscription(cache_ack_observer, ack_topic, timeout=0.1)
            if replay_after is not None and (cache_ack_observer is None or replay_ack_payload is not None):
                break
            time.sleep(0.25)
        unchanged = before_fp == fingerprint(target.gateway_id, target.sensor_urn)
        if cache_smoke:
            after_db = {'fingerprint': fingerprint(target.gateway_id, target.sensor_urn),
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn';", urn=target.sensor_urn)),
                'version': sql(VERSION_SNAPSHOT_SQL, urn=target.sensor_urn)}
            if replay_after is None:
                replay_after = scrape('p5b_warm_replay_final_window')
            warm_evidence = warm_replay_evidence(before_metrics['snapshot'], replay_after['snapshot'],
                before_db, after_db, selected={'message_id': selected_message_id,
                'gateway_id': target.gateway_id, 'topic': replay_topic,
                'payload_sha256': hashlib.sha256(replay_payload).hexdigest(),
                'original_bytes_sha256': hashlib.sha256(captured[1][0]).hexdigest()},
                ack_topic=ack_topic, ack_payload=replay_ack_payload)
            warm_evidence.update(window_start_monotonic=before_metrics['observed_monotonic'],
                window_end_monotonic=replay_after['observed_monotonic'],
                db_before=before_db, db_after=after_db,
                process_identity=before_metrics['snapshot'].get('process_instance'),
                schema_version=profile_data['database_schema_version'], cache_ttl_seconds=600)
            run_data['warm_replay'] = safe_artifact(warm_evidence)
            persist()
            if cache_ack_observer is not None:
                smoke.paper_p1_stop_subscription(cache_ack_observer)
                observers.remove(cache_ack_observer)
            # Four bounded DB-guard cases use the same warm Gateway principal,
            # schema-validated payload source, TLS/ACL publisher and ACK observer.
            # Provision the moved-Sensor destination through the normal owner API;
            # this setup is outside the protected target's per-conflict snapshots.
            moved_sensor_id = provision_conflict_sensor(smoke, endpoint, target, tokens, run_data, persist)

            # Seed a genuinely committed two-sample range via authorized MQTT and
            # application ACK. This functional fixture message is not benchmark
            # workload and is accounted separately before conflict snapshots.
            from types import SimpleNamespace
            overlap_setup_bytes = overlap_setup_payload(replay_payload,
                first_sequence=int(sql("SELECT COALESCE(max(last_sequence),0)+10000 FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor';",
                    gw=target.gateway_id, sensor=target.sensor_id)),
                measured_at=(datetime.now(timezone.utc)-timedelta(minutes=3)).isoformat().replace('+00:00', 'Z'))
            overlap_source = json.loads(overlap_setup_bytes)
            from stage3_uplink_runner import probe_batch_is_valid
            if not probe_batch_is_valid(overlap_setup_bytes,
                    SimpleNamespace(gateway_id=target.gateway_id, sensor_id=target.sensor_id), expected_samples=2):
                raise ValueError('paper_p5b_overlap_setup_invalid')
            setup_topic = 'gateways/' + target.gateway_id + '/acks/' + overlap_source['message_id']
            setup_observer = smoke.paper_p1_subscriber(fixture, registry, broker_id, target,
                passwords[1], 'p5b-overlap-setup-ack', message_count=1, message_timeout=8,
                subscription_kind='ack')
            observers.append(setup_observer)
            setup_before_db = {'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id))}
            setup_publish = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
                'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                input=(target.gateway_id + '\n' + passwords[1] + '\n'
                       + 'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id + '\n').encode()
                      + overlap_setup_bytes)
            setup_ack = smoke.paper_p1_capture_subscription(setup_observer, setup_topic, timeout=8) if setup_publish.returncode == 0 else None
            smoke.paper_p1_stop_subscription(setup_observer)
            observers.remove(setup_observer)
            setup_after_db = {'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id))}
            run_data['conflict_overlap_setup'] = {'traffic_source': 'manual_authorized_functional_fixture_not_benchmark',
                'message_id': overlap_source['message_id'], 'payload_sha256': hashlib.sha256(overlap_setup_bytes).hexdigest(),
                'ack_observed': setup_ack is not None, 'db_before': setup_before_db, 'db_after': setup_after_db}
            persist()
            if setup_publish.returncode or setup_ack is None or setup_after_db['ledger'] != setup_before_db['ledger'] + 1 or setup_after_db['raw'] != setup_before_db['raw'] + 2:
                raise ValueError('paper_p5b_overlap_setup_not_committed_and_acked')
            conflict_payload_map = conflict_payloads(overlap_setup_bytes,
                gateway_id=target.gateway_id, original_sensor_id=target.sensor_id,
                conflict_sensor_id=moved_sensor_id,
                now=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))
            expected_lookup = {
                'same_id_changed_payload': 'metadata_mismatch',
                'same_id_moved_sensor_time_key': 'miss',
                'same_ram_key_different_id': 'metadata_mismatch',
                'different_first_sequence_overlap': 'miss',
            }
            expected_rejection_reason = conflict_rejection_reasons()
            conflict_cases = {}
            def conflict_metric_diagnostic(snapshot):
                ingress_fields = ('Received', 'PersistFailures', 'Committed', 'DuplicateCommitted',
                    'SamplesNew', 'ACKPublished', 'ACKPublishFailures', 'ValidationRejected',
                    'ShaperRateLimited', 'InFlight', 'QueueDepth')
                cache_fields = ('lookup_hit_total', 'lookup_miss_total',
                    'lookup_metadata_mismatch_total', 'database_calls_avoided_total')
                ingress_metrics = snapshot.get('ingress', {}) if isinstance(snapshot, dict) else {}
                cache_metrics = snapshot.get('ram_dedup', {}) if isinstance(snapshot, dict) else {}
                return {'process_instance': snapshot.get('process_instance') if isinstance(snapshot, dict) else None,
                    'ingress': {name: ingress_metrics.get(name) for name in ingress_fields},
                    'ram_dedup': {name: cache_metrics.get(name) for name in cache_fields}}
            for case_name, conflict_bytes in conflict_payload_map.items():
                conflict_message = json.loads(conflict_bytes)
                conflict_id = conflict_message['message_id']
                conflict_topic = 'gateways/' + target.gateway_id + '/acks/' + conflict_id
                conflict_telemetry_topic = ('gateways/' + target.gateway_id + '/telemetry/'
                    + conflict_message['sensor_id'])
                observer = smoke.paper_p1_subscriber(fixture, registry, broker_id,
                    target, passwords[1], 'p5b-conflict-' + case_name.replace('_', '-')[:20],
                    message_count=1, message_timeout=4, subscription_kind='ack')
                observers.append(observer)

                def db_state():
                    return {'fingerprint': fingerprint(target.gateway_id, target.sensor_urn),
                        'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                        'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                        'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn';", urn=target.sensor_urn)),
                        'version': sql(VERSION_SNAPSHOT_SQL, urn=target.sensor_urn)}

                conflict_before_db = db_state()
                # A generic PersistFailures increment is not enough: prove the
                # persisted ledger contains the exact ID/provenance or overlap
                # precondition this case is intended to exercise.
                guard_kind = ('message_id_payload_mismatch' if case_name == 'same_id_changed_payload'
                    else 'message_id_provenance_mismatch' if case_name == 'same_id_moved_sensor_time_key'
                    else 'sequence_overlap')
                guard_vars = conflict_guard_variables(conflict_message,
                    gateway_id=target.gateway_id, conflict_id=conflict_id, conflict_bytes=conflict_bytes)
                try:
                    guard_matched = sql(conflict_guard_query(case_name), **guard_vars).lower() == 't'
                except ValueError as exc:
                    safe_reason = str(exc) if re.fullmatch(r'[a-z0-9_]{1,96}', str(exc)) else 'query_failure_unclassified'
                    run_data.setdefault('conflicts', {})[case_name] = {
                        'status': 'DB_QUERY_FAILED', 'reason_code': safe_reason,
                        'guard_reason': guard_kind}
                    persist()
                    raise ValueError('paper_p5b_conflict_precheck_query_failed') from None
                guard_precondition = {'kind': guard_kind, 'matched': guard_matched}
                if not guard_matched:
                    run_data.setdefault('conflicts', {})[case_name] = {
                        'status': 'PRECONDITION_REJECTED',
                        'reason_code': 'conflict_precheck_not_matched',
                        'guard_reason': guard_kind, 'guard_precondition': guard_precondition}
                    persist()
                    raise ValueError('paper_p5b_conflict_precondition_not_matched')
                conflict_before = scrape('p5b_conflict_' + case_name + '_before')
                pre_log_result = registry.command(['logs', '--tail', '500', backend_id])
                pre_backend_log = (pre_log_result.stdout + pre_log_result.stderr).decode(errors='replace')
                start = time.monotonic()
                conflict_publish = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
                    'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                    'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                    '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                    input=(target.gateway_id + '\n' + passwords[1] + '\n' + conflict_telemetry_topic + '\n').encode()
                          + conflict_bytes)
                conflict_ack = None
                try:
                    if conflict_publish.returncode == 0:
                        conflict_ack = smoke.paper_p1_capture_subscription(observer, conflict_topic, timeout=4)
                finally:
                    smoke.paper_p1_stop_subscription(observer)
                    observers.remove(observer)
                conflict_after = scrape('p5b_conflict_' + case_name + '_after')
                conflict_after_db = db_state()
                log_result = registry.command(['logs', '--tail', '500', backend_id])
                backend_log = (log_result.stdout + log_result.stderr).decode(errors='replace')
                reason_code = expected_rejection_reason[case_name]
                reason_markers = ('reason_code=' + reason_code, '"reason_code":"' + reason_code + '"')
                reason_count_before = sum(pre_backend_log.count(marker) for marker in reason_markers)
                reason_count_after = sum(backend_log.count(marker) for marker in reason_markers)
                rejection_reason = reason_code if reason_count_after > reason_count_before else None
                selected_hash = hashlib.sha256(conflict_bytes).hexdigest()
                # Preserve only identifiers, hashes, counters and DB fingerprints.
                diagnostic = {'status': 'PENDING_ASSERTION', 'message_id': conflict_id,
                    'payload_sha256': selected_hash, 'payload_bytes': len(conflict_bytes),
                    'expected_lookup': expected_lookup[case_name],
                    'guard_precondition': guard_precondition,
                    'publish_returncode': conflict_publish.returncode,
                    'expected_rejection_reason': expected_rejection_reason[case_name],
                    'observed_rejection_reason': rejection_reason,
                    'elapsed_seconds': time.monotonic() - start,
                    'db_before': conflict_before_db, 'db_after': conflict_after_db,
                    'metrics_before': conflict_metric_diagnostic(conflict_before['snapshot']),
                    'metrics_after': conflict_metric_diagnostic(conflict_after['snapshot']),
                    'ack_observed': conflict_ack is not None}
                run_data.setdefault('conflicts', {})[case_name] = diagnostic
                persist()
                if conflict_publish.returncode:
                    raise ValueError('paper_p5b_conflict_publish_failed')
                case_evidence = conflict_evidence(
                    conflict_before['snapshot'], conflict_after['snapshot'],
                    conflict_before_db, conflict_after_db,
                    expected_lookup=expected_lookup[case_name], ack_payload=conflict_ack,
                    guard_reason=guard_kind, guard_precondition=guard_precondition,
                    rejection_reason=rejection_reason,
                    expected_rejection_reason=expected_rejection_reason[case_name])
                case_evidence.update(message_id=conflict_id, payload_sha256=selected_hash,
                    payload_bytes=len(conflict_bytes),
                    window_start_monotonic=conflict_before['observed_monotonic'],
                    window_end_monotonic=conflict_after['observed_monotonic'])
                run_data['conflicts'][case_name].update(safe_artifact(case_evidence))
                persist()
                if case_evidence['status'] != 'PASS':
                    raise ValueError('paper_p5b_conflict_reconciliation_failed')
                conflict_cases[case_name] = case_evidence
            run_data['conflict_assertions'] = {'status': 'PASS', 'cases': conflict_cases,
                'overlap_fixture': 'two samples; first_sequence one before the actual committed one-sample range',
                'ack_absence_is_supplementary': True}
            run_data['cache_smoke_evidence'] = {'warm_replay': run_data.get('warm_replay'),
                'conflicts': run_data['conflict_assertions']}
            persist()
            # Restart the owned backend only after the warm and conflict proof.
            # The persistent DB and broker stay in place; the replacement has
            # the same exact profile and starts with a fresh process/cache.
            old_process = conflict_after['snapshot'].get('process_instance')
            run_data['backend_replacement'] = {'status': 'IN_PROGRESS',
                'old_process_identity': old_process,
                'same_schema_version': profile_data['database_schema_version'],
                'same_effective_profile': True, 'same_database_and_broker': True}
            persist()
            replacement = replace_backend_for_cold_cache(smoke, fixture, registry, containers,
                                                         lifecycle_state)
            run_data['backend_replacement'] = {'status': 'REPLACED',
                'old_process_identity': old_process, 'replacement_container': replacement.get('Name'),
                'same_schema_version': profile_data['database_schema_version'],
                'same_effective_profile': True, 'same_database_and_broker': True}
            cold_observer = smoke.paper_p1_subscriber(fixture, registry, broker_id, target,
                passwords[1], 'p5b-cold-replay-ack', message_count=1, message_timeout=12,
                subscription_kind='ack')
            observers.append(cold_observer)
            cold_before = scrape('p5b_cold_replay_new_process_baseline')
            cold_cache = cold_before['snapshot'].get('ram_dedup', {})
            cold_before_db = {'fingerprint': fingerprint(target.gateway_id, target.sensor_urn),
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn';", urn=target.sensor_urn)),
                'version': sql(VERSION_SNAPSHOT_SQL, urn=target.sensor_urn)}
            run_data['cold_replay'] = {'status': 'IN_PROGRESS',
                'old_process_identity': old_process,
                'process_identity': cold_before['snapshot'].get('process_instance'),
                'window_start_monotonic': cold_before['observed_monotonic'],
                'cache_entries_before': cold_cache.get('entries'), 'db_before': cold_before_db,
                'message_id': selected_message_id, 'topic': replay_topic,
                'payload_sha256': hashlib.sha256(replay_payload).hexdigest(),
                'payload_provenance': 'same_saved_original_gateway_delivery',
                'effective_profile_preserved': True,
                'baseline_valid': (cold_before['snapshot'].get('process_instance') != old_process
                    and cold_cache.get('enabled') is True and cold_cache.get('entries') == 0)}
            persist()
            if not run_data['cold_replay']['baseline_valid']:
                raise ValueError('paper_p5b_cold_baseline_rejected')
            cold_publish = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
                'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                input=(target.gateway_id + '\n' + passwords[1] + '\n' + replay_topic + '\n').encode()
                      + replay_payload)
            if cold_publish.returncode:
                raise ValueError('paper_p5b_cold_replay_publish_failed')
            cold_ack = None
            cold_after = None
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                lifecycle_state.checkpoint()
                candidate = scrape('p5b_cold_replay_observed')
                if cold_ack is None:
                    cold_ack = smoke.paper_p1_capture_subscription(cold_observer, ack_topic, timeout=0.1)
                ci = candidate['snapshot'].get('ingress', {})
                bi = cold_before['snapshot'].get('ingress', {})
                if (type(ci.get('DuplicateCommitted')) is int and type(bi.get('DuplicateCommitted')) is int
                        and ci['DuplicateCommitted'] > bi['DuplicateCommitted']
                        and cold_ack is not None):
                    cold_after = candidate
                    break
                time.sleep(0.25)
            smoke.paper_p1_stop_subscription(cold_observer)
            observers.remove(cold_observer)
            cold_after_db = {'fingerprint': fingerprint(target.gateway_id, target.sensor_urn),
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id=:'gw';", gw=target.gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn';", urn=target.sensor_urn)),
                'version': sql(VERSION_SNAPSHOT_SQL, urn=target.sensor_urn)}
            if cold_after is None:
                cold_after = scrape('p5b_cold_replay_final_window')
            cold_evidence = cold_replay_evidence(cold_before['snapshot'], cold_after['snapshot'],
                cold_before_db, cold_after_db,
                selected={'message_id': selected_message_id, 'gateway_id': target.gateway_id,
                    'payload_sha256': hashlib.sha256(replay_payload).hexdigest(),
                    'original_bytes_sha256': hashlib.sha256(captured[1][0]).hexdigest()},
                ack_topic=ack_topic, ack_payload=cold_ack, old_process_identity=old_process)
            cold_evidence.update(window_start_monotonic=cold_before['observed_monotonic'],
                window_end_monotonic=cold_after['observed_monotonic'], db_before=cold_before_db,
                db_after=cold_after_db, process_identity=cold_before['snapshot'].get('process_instance'),
                old_process_identity=old_process)
            run_data['cold_replay'] = safe_artifact(cold_evidence)
            run_data['cache_smoke_evidence'] = {'warm_replay': run_data.get('warm_replay'),
                'conflicts': run_data['conflict_assertions'], 'cold_replay': run_data['cold_replay']}
            run_data['p5b_cold_replay_status'] = cold_evidence['status']
            persist()
            if cold_evidence['status'] != 'PASS':
                raise ValueError('paper_p5b_cold_replay_reconciliation_failed')
        check('duplicate_replay_unchanged', replay_after is not None and unchanged,
              duplicate_metric_observed=replay_after is not None, database_unchanged=unchanged,
              payload_sha256=hashlib.sha256(replay_payload).hexdigest(),
              manual_replay_not_counted_as_gateway_attempt=True)

        if getattr(fixture, 'paper_p5b', False):
            # Comparable P5b phase: fixed 12 exact repeats for each of the two
            # real Gateway deliveries, only after both writers/outboxes drained.
            from stage3_mqtt_ingress_runner import verify_ack
            replay_observers = []
            replay_records = []
            for index, spec in enumerate(specs[:2]):
                ack_observer = smoke.paper_p1_subscriber(fixture, registry, broker_id, spec,
                    passwords[index], 'p5b-dup-ack-' + str(index), message_count=12,
                    message_timeout=60, subscription_kind='ack')
                replay_observers.append(ack_observer)
            p5b_before = scrape('p5b_duplicate_replay_before')
            replay_db_before = {'fingerprint': [fingerprint(s.gateway_id, s.sensor_urn) for s in specs[:2]],
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id IN (:'a',:'b');",
                                  a=specs[0].gateway_id, b=specs[1].gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id IN (:'a',:'b');",
                               a=specs[0].gateway_id, b=specs[1].gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id IN (:'ua',:'ub');",
                                    ua=specs[0].sensor_urn, ub=specs[1].sensor_urn)),
                'versions': [sql(VERSION_SNAPSHOT_SQL, urn=s.sensor_urn) for s in specs[:2]]}
            phase_start = time.monotonic()
            try:
                for index, spec in enumerate(specs[:2]):
                    exact_payload, message_id = captured[index]
                    topic = 'gateways/' + spec.gateway_id + '/telemetry/' + spec.sensor_id
                    ack_topic_for_message = 'gateways/' + spec.gateway_id + '/acks/' + message_id
                    for ordinal in range(12):
                        lifecycle_state.checkpoint()
                        ack_boundary = smoke.paper_p1_subscription_match_count(
                            replay_observers[index], ack_topic_for_message)
                        publish_start = time.monotonic()
                        result = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
                            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                            input=(spec.gateway_id + '\n' + passwords[index] + '\n' + topic + '\n').encode()
                                  + exact_payload)
                        if result.returncode:
                            replay_records.append({'gateway_index': index, 'ordinal': ordinal,
                                'publish_returncode': result.returncode, 'ack_valid': False,
                                'elapsed_seconds': time.monotonic() - publish_start})
                            break
                        ack = smoke.paper_p1_capture_subscription(replay_observers[index],
                            ack_topic_for_message, timeout=5, after_count=ack_boundary)
                        ack_observed_at = time.monotonic() if isinstance(ack, bytes) else None
                        valid_ack = False
                        try:
                            if isinstance(ack, bytes):
                                verify_ack(message_id, ack)
                                ack_doc = json.loads(ack.decode('utf-8'))
                                valid_ack = (ack_doc.get('status') == 'committed'
                                    and ack_topic_for_message == 'gateways/' + spec.gateway_id + '/acks/' + message_id)
                        except (ValueError, TypeError, UnicodeError, RuntimeError):
                            valid_ack = False
                        replay_records.append({'gateway_index': index, 'ordinal': ordinal,
                            'publish_returncode': result.returncode, 'ack_valid': valid_ack,
                            'elapsed_seconds': time.monotonic() - publish_start,
                            'ack_match_ordinal': ack_boundary if isinstance(ack, bytes) else None,
                            'ack_observed_monotonic': ack_observed_at,
                            'payload_sha256': hashlib.sha256(exact_payload).hexdigest(),
                            'timing_scope': 'manual fixture publish to observed application ACK; includes CLI and observer cost'})
                        if not isinstance(ack, bytes):
                            # Do not let a late ACK for this publish be attributed to
                            # the next delivery's freshly established boundary.
                            break
                # Drain all accepted deliveries and workers before taking the ending
                # snapshot. This is bounded; failure diagnostics are still persisted.
                replay_drain = {'complete': False, 'deadline_seconds': 20}
                drain_deadline = time.monotonic() + replay_drain['deadline_seconds']
                latest_replay_snapshot = None
                while time.monotonic() < drain_deadline:
                    lifecycle_state.checkpoint()
                    candidate = scrape('p5b_duplicate_replay_drain')
                    latest_replay_snapshot = candidate['snapshot']
                    current = latest_replay_snapshot.get('ingress', {})
                    start = p5b_before['snapshot'].get('ingress', {})
                    deltas_ready = all(type(current.get(field)) is int
                        and type(start.get(field)) is int
                        and current[field] - start[field] == 24
                        for field in ('Processed', 'DuplicateCommitted', 'ACKPublished'))
                    queues_empty = all(current.get(field) == 0 for field in
                                       ('QueueDepth', 'InFlight', 'BufferedPayloadBytes'))
                    if deltas_ready and queues_empty:
                        replay_drain.update(complete=True,
                            observed_monotonic=candidate['observed_monotonic'],
                            ingress=current)
                        break
                    time.sleep(0.1)
                if not replay_drain['complete']:
                    replay_drain.update(observed_monotonic=time.monotonic(),
                        ingress=(latest_replay_snapshot or {}).get('ingress', {}),
                        reason='expected_delivery_totals_or_zero_queue_state_not_observed')
                p5b_after = scrape('p5b_duplicate_replay_after')
            finally:
                for replay_observer in replay_observers:
                    smoke.paper_p1_stop_subscription(replay_observer)
                    if replay_observer in observers:
                        observers.remove(replay_observer)
            replay_db_after = {'fingerprint': [fingerprint(s.gateway_id, s.sensor_urn) for s in specs[:2]],
                'ledger': int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id IN (:'a',:'b');",
                                  a=specs[0].gateway_id, b=specs[1].gateway_id)),
                'raw': int(sql("SELECT count(*) FROM telemetry WHERE gateway_id IN (:'a',:'b');",
                               a=specs[0].gateway_id, b=specs[1].gateway_id)),
                'temporal': int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id IN (:'ua',:'ub');",
                                    ua=specs[0].sensor_urn, ub=specs[1].sensor_urn)),
                'versions': [sql(VERSION_SNAPSHOT_SQL, urn=s.sensor_urn) for s in specs[:2]]}
            before_snapshot, after_snapshot = p5b_before['snapshot'], p5b_after['snapshot']
            before_ingress, after_ingress = before_snapshot.get('ingress', {}), after_snapshot.get('ingress', {})
            received_delta = (after_ingress.get('Received') - before_ingress.get('Received')
                              if type(after_ingress.get('Received')) is int and type(before_ingress.get('Received')) is int else -1)
            attempts = len(replay_records)
            acked = sum(record.get('ack_valid') is True for record in replay_records)
            fresh_ack_count = sum(record.get('ack_valid') is True
                                  and type(record.get('ack_match_ordinal')) is int
                                  for record in replay_records)
            evidence = duplicate_replay_evidence(mode=fixture.paper_p5b_cache_mode,
                offered_attempts=attempts, received=received_delta, acked=acked,
                fresh_acks=fresh_ack_count,
                before_metrics={**before_snapshot, 'persistence': before_snapshot.get('persistence'),
                                'ram_dedup': before_snapshot.get('ram_dedup')},
                after_metrics={**after_snapshot, 'persistence': after_snapshot.get('persistence'),
                               'ram_dedup': after_snapshot.get('ram_dedup')},
                before_db=replay_db_before, after_db=replay_db_after,
                exact_bytes=(all(isinstance(captured[i][0], bytes) and captured[i][0]
                    and all(record.get('payload_sha256') == hashlib.sha256(captured[i][0]).hexdigest()
                            for record in replay_records if record.get('gateway_index') == i)
                    for i in range(2))))
            evidence.update(window_start_monotonic=phase_start,
                window_end_monotonic=p5b_after['observed_monotonic'], db_before=replay_db_before,
                db_after=replay_db_after, per_publish_ack_spans=[r.get('elapsed_seconds') for r in replay_records],
                offered_pattern='12 exact original-byte republishes per Gateway, sequential and same in both cache arms',
                drain=replay_drain,
                retries=0, retry_basis='manual publisher does not retry; failed publishes are recorded as failures',
                rate_limit_delta=(after_ingress.get('ShaperRateLimited', 0) - before_ingress.get('ShaperRateLimited', 0)
                    if type(after_ingress.get('ShaperRateLimited')) is int and type(before_ingress.get('ShaperRateLimited')) is int else None),
                actual_cache_snapshot=after_snapshot.get('ram_dedup'))
            persist_duplicate_replay_evidence(run_data, evidence, persist)
            run_data['duplicate_replay']['attempt_records'] = safe_artifact(replay_records)
            run_data['duplicate_replay']['drain'] = safe_artifact(replay_drain)
            run_data['duplicate_replay']['new_phase_metrics'] = safe_artifact(drained['snapshot'].get('persistence'))
            run_data['duplicate_replay']['new_phase_grouping'] = safe_artifact(drained['snapshot'].get('ingress', {}).get('microbatch'))
            persist()
            if evidence['status'] != 'PASS' or not replay_drain['complete']:
                raise ValueError('paper_p5b_duplicate_replay_reconciliation_failed')

        import stage3_uplink_runner as uplink
        logs_before = uplink.container_logs(broker_id)
        before_denial_fp = fingerprint(target.gateway_id, target.sensor_urn)
        deny_payload, _ = captured[0]
        denied = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(specs[0].gateway_id + '\n' + passwords[0] + '\n' +
                   'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id + '\n').encode()
                  + deny_payload)
        logs_after = uplink.wait_denied_publish(broker_id, logs_before,
            'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id,
            uplink.PROBE_DENY_SETTLE)
        denied_observed = uplink.denied_publish_logged(logs_before, logs_after,
            'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id)
        denial_unchanged = before_denial_fp == fingerprint(target.gateway_id, target.sensor_urn)
        check('wrong_namespace_denied', denied_observed and denial_unchanged,
              broker_audit_denial=denied_observed, target_database_unchanged=denial_unchanged,
              client_exit_code=denied.returncode,
              payload_sha256=hashlib.sha256(deny_payload).hexdigest())
        check('exporters_written', (fixture.artifacts / 'metrics.csv').is_file()
              and (fixture.artifacts / 'resources.csv').is_file(),
              metrics_csv=True, resources_csv=True)

        if shutdown_smoke:
            # Separate functional smoke: all finite Gateway writers are done.
            # Admit one fresh TLS/DynSec message, prove it is validated and held
            # in an underfilled group, then signal the real backend container.
            from stage3_mqtt_ingress_runner import verify_ack
            from stage3_uplink_runner import build_probe_batch

            primary = specs[0]
            ack_observer = smoke.paper_p1_subscriber(fixture, registry,
                containers['mosquitto']['receipt']['id'], primary, passwords[0],
                'p5a-shutdown-ack', message_count=1, message_timeout=8, subscription_kind='ack')
            shutdown_id = str(__import__('uuid').uuid4())
            sample = json.loads(sql("""SELECT json_build_object(
              'boot_id',boot_id,'sequence',coalesce(max(sequence_num),0)+1)
              FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor' GROUP BY boot_id
              ORDER BY max(sequence_num) DESC LIMIT 1;""", gw=primary.gateway_id, sensor=primary.sensor_id))
            measured = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
            topic = 'gateways/' + primary.gateway_id + '/telemetry/' + primary.sensor_id
            ack_topic = 'gateways/' + primary.gateway_id + '/acks/' + shutdown_id
            payload = build_probe_batch(primary, shutdown_id, sample['boot_id'], int(sample['sequence']),
                sample_count=1, sample_interval_us=10000, measured_at=measured)
            before = scrape('before_shutdown_partial_group')['snapshot']['ingress']
            pub_result = registry.command(['exec', '-i', containers['mosquitto']['receipt']['id'],
                'sh', '-ec', 'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                input=(primary.gateway_id + '\n' + passwords[0] + '\n' + topic + '\n').encode() + payload)
            if pub_result.returncode:
                smoke.paper_p1_stop_subscription(ack_observer)
                raise ValueError('paper_p5a_shutdown_publish_failed')
            publish_at = time.monotonic()
            msg_count = "SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id';"
            pending_snapshot = None
            deadline = publish_at + 1.5
            try:
                while time.monotonic() < deadline:
                    candidate = scrape('shutdown_partial_group_observed')['snapshot']['ingress']
                    ledger_before_stop = int(sql(msg_count, gw=primary.gateway_id, id=shutdown_id))
                    if (candidate.get('Processed', 0) > before.get('Processed', 0)
                            and candidate.get('BufferedPayloadBytes', 0) > 0
                            and candidate.get('ValidationRejected', 0) == before.get('ValidationRejected', 0)
                            and ledger_before_stop == 0
                            and time.monotonic() - publish_at < 2.0):
                        pending_snapshot = candidate
                        break
                    time.sleep(0.1)
                if pending_snapshot is None:
                    raise ValueError('paper_p5a_shutdown_partial_group_unproven')
                shutdown_started = time.monotonic()
                signalled = registry.command(['kill', '--signal', 'TERM', backend_id])
                if signalled.returncode:
                    raise ValueError('paper_p5a_shutdown_signal_failed')
                common_budget = int(effective.get('services', {}).get('backend', {})
                                    .get('environment', {}).get('SHUTDOWN_TIMEOUT', '10s').rstrip('s'))
                stop_deadline = shutdown_started + common_budget
                exit_code = None
                ack_payload = None
                while time.monotonic() < stop_deadline:
                    inspect = registry.command(['inspect', '--format', '{{.State.Running}} {{.State.ExitCode}}', backend_id])
                    if inspect.returncode == 0:
                        state = inspect.stdout.decode().strip().split()
                        if len(state) == 2 and state[0] == 'false':
                            exit_code = int(state[1])
                    if ack_payload is None:
                        ack_payload = smoke.paper_p1_capture_subscription(ack_observer, ack_topic, timeout=0.2)
                    if exit_code is not None and ack_payload is not None:
                        break
                    time.sleep(0.1)
                if exit_code != 0 or ack_payload is None:
                    raise ValueError('paper_p5a_shutdown_deadline_failed')
                verify_ack(shutdown_id, ack_payload)
                persisted = json.loads(sql("""SELECT json_build_object(
                  'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'temporal',(SELECT count(*) FROM twin_temporal_values WHERE message_id=:'id'));""",
                  gw=primary.gateway_id, id=shutdown_id))
                if persisted != {'ledger': 1, 'raw': 1, 'temporal': 1}:
                    raise ValueError('paper_p5a_shutdown_persistence_failed')
                log_result = registry.command(['logs', backend_id])
                summary = (log_result.stdout + log_result.stderr).decode(errors='replace')
                if not re.search(r'buffered_payload_bytes=0(?:\s|$)', summary):
                    raise ValueError('paper_p5a_shutdown_bytes_not_drained')
                check('controlled_shutdown_partial_group', True,
                    max_wait_seconds=2, members_before_signal=1, validated_before_signal=True,
                    ledger_absent_before_signal=True, buffered_payload_bytes_before_signal=
                        pending_snapshot.get('BufferedPayloadBytes'), timer_elapsed_before_signal=False,
                    signal='SIGTERM', backend_exit_code=exit_code,
                    shutdown_elapsed_seconds=time.monotonic() - shutdown_started,
                    common_shutdown_budget_seconds=common_budget, persisted=persisted,
                    application_ack_verified=True, final_buffered_payload_bytes=0)
                run_data['shutdown_smoke'] = {'status': 'PASS', 'message_id': shutdown_id,
                    'ack_topic': ack_topic, 'payload_sha256': hashlib.sha256(payload).hexdigest(),
                    'group_members': 1, 'timer_target_seconds': 2, 'backend_exit_code': exit_code,
                    'shutdown_elapsed_seconds': time.monotonic() - shutdown_started,
                    'shutdown_budget_seconds': common_budget, 'persisted': persisted,
                    'final_buffered_payload_bytes': 0}
            finally:
                smoke.paper_p1_stop_subscription(ack_observer)

        accounting = {'offered_unique': expected, 'committed_unique': ledgers,
            'raw_samples': raw, 'temporal': temporal, 'application_acks': ack_total,
            'outbox_rows': 0, 'outbox_bytes': 0, 'payload_budget_bytes': ingress['BufferedPayloadBytes']}
        validate_accounting(config, accounting)
        if time.monotonic() - started > config['run_deadline_seconds']:
            raise ValueError('paper_p5a_deadline_exceeded')
        run_data.update(accounting=accounting, backend_metrics=ingress, status='PASS')
        run_data['backend_snapshot'] = drained['snapshot']
        run_data['gateway_ack_measurements'] = ack_measurements
        gateway_attempts = sum(int(row.get('publish_attempts', '0')) for row in summaries)
        run_data['gateway_attempt_accounting'] = {
            'publish_attempts': gateway_attempts,
            'unique_messages': expected,
            'retries': max(0, gateway_attempts - expected),
            'basis': 'sum of actual Gateway CLI publish_attempts minus generated unique messages',
            'pending_backlog_at_final_cli_exit': 0,
        }
        run_data['full_p5a_workload_accepted'] = acceptance_ready(run_data['checks'], mode)
        if shutdown_smoke:
            run_data['full_p5a_workload_accepted'] = (run_data['full_p5a_workload_accepted']
                and run_data.get('shutdown_smoke', {}).get('status') == 'PASS')
        if not run_data['full_p5a_workload_accepted']:
            raise ValueError('paper_p5a_acceptance_failed')
        if cache_smoke:
            run_data['p5b_cache_functional_accepted'] = False
            if run_data.get('p5b_cold_replay_status') != 'PASS':
                run_data['status'] = 'FAIL'
                persist()
                raise ValueError('paper_p5b_functional_smoke_failed')
        persist()
        return run_data
    except BaseException:
        persist()
        raise
    finally:
        for observer in observers:
            smoke.paper_p1_stop_subscription(observer)
