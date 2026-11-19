"""Small, fail-closed helpers for E4's fresh-backend replay window."""
import json
import subprocess
import time
from pathlib import Path

def replacement_fragment(name):
    if not isinstance(name, str) or not name or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in name):
        raise ValueError('p8_e4_restart_name_rejected')
    # Deliberately no environment, image, mount, network, or security overrides.
    return 'services:\n  backend:\n    container_name: ' + name + '\n'


def normalize_host_config(hc):
    if not isinstance(hc, dict):
        return hc
    norm = dict(hc)
    for key in ('CapAdd', 'CapDrop'):
        if isinstance(norm.get(key), list):
            norm[key] = sorted(norm[key])
    binds = norm.get('Binds')
    if isinstance(binds, list):
        destinations = [item.split(':', 2)[1] if isinstance(item, str) and ':' in item else None
                        for item in binds]
        # Reordering is harmless only when every bind has a known unique target.
        if all(destinations) and len(set(destinations)) == len(destinations):
            norm['Binds'] = sorted(binds)
    return norm


def verify_replacement(old, new, *, expected_image, expected_environment,
                       expected_mounts, expected_host_config):
    """Check observed replacement preserves exact runtime settings and identity changes."""
    if not isinstance(old, dict) or not isinstance(new, dict):
        raise ValueError('p8_e4_restart_readback_missing')
    if old.get('Id') == new.get('Id') or old.get('Id') is None or new.get('Id') is None:
        raise ValueError('p8_e4_restart_identity_not_changed')
    if new.get('Image') != expected_image:
        raise ValueError('p8_e4_restart_image_changed')
    config = new.get('Config') or {}
    if sorted(config.get('Env') or []) != sorted(expected_environment):
        raise ValueError('p8_e4_restart_environment_changed')
    if sorted((new.get('Mounts') or []), key=lambda x: x.get('Destination', '')) != sorted(expected_mounts, key=lambda x: x.get('Destination', '')):
        raise ValueError('p8_e4_restart_mounts_changed')
    if normalize_host_config(new.get('HostConfig')) != normalize_host_config(expected_host_config):
        raise ValueError('p8_e4_restart_host_config_changed')
    return {'old_id': old['Id'], 'new_id': new['Id'], 'image': expected_image,
            'environment_unchanged': True, 'mounts_unchanged': True,
            'host_config_unchanged': True}


def cold_window_evidence(old_metrics, before, after, *, before_db, after_db,
                         application_ack_count, offered_count=10):
    """Validate independent process windows; DB duplicates are not ingress outcomes."""
    reasons = []
    old_id = old_metrics.get('process_instance') if isinstance(old_metrics, dict) else None
    process = before.get('process_instance') if isinstance(before, dict) else None
    if not old_id or not process or process == old_id:
        reasons.append('replacement_process_identity_not_changed')
    if not process or (after or {}).get('process_instance') != process:
        reasons.append('cold_window_crossed_process_restart')
    cache0 = (before or {}).get('ram_dedup') or {}
    cache1 = (after or {}).get('ram_dedup') or {}
    if cache0.get('entries') != 0:
        reasons.append('replacement_cache_not_empty')
    def delta(group0, group1, key):
        a, b = group0.get(key), group1.get(key)
        return b - a if type(a) is int and type(b) is int and a >= 0 and b >= a else None
    miss = delta(cache0, cache1, 'lookup_miss_total')
    hit = delta(cache0, cache1, 'lookup_hit_total')
    if miss is None or miss <= 0:
        reasons.append('cold_lookup_miss_not_observed')
    if hit is None or hit != 0:
        reasons.append('cold_lookup_hit_observed')
    p0, p1 = (before or {}).get('persistence') or {}, (after or {}).get('persistence') or {}
    deltas = {key: delta(p0, p1, key) for key in
              ('begin_attempts', 'confirmed_commits', 'confirmed_new_rows', 'copy_calls')}
    if any(type(value) is not int for value in deltas.values()) or deltas['begin_attempts'] <= 0 or deltas['confirmed_commits'] <= 0:
        reasons.append('cold_database_duplicate_transaction_not_observed')
    if deltas['confirmed_new_rows'] != 0 or deltas['copy_calls'] != 0:
        reasons.append('cold_replay_persisted_new_data')
    i0, i1 = (before or {}).get('ingress') or {}, (after or {}).get('ingress') or {}
    committed = delta(i0, i1, 'Committed')
    samples = delta(i0, i1, 'SamplesNew')
    if committed is None or committed != 0 or samples is None or samples != 0:
        reasons.append('cold_replay_ingress_new_data')
    if application_ack_count != offered_count:
        reasons.append('cold_replay_application_ack_count')
    if not before_db or not after_db or before_db != after_db:
        reasons.append('cold_replay_database_fingerprint_changed')
    return {'status': 'PASS' if not reasons else 'FAIL', 'reasons': reasons,
            'old_process_instance': old_id, 'process_instance': process,
            'cache_entries_before': cache0.get('entries'), 'cache_miss_delta': miss,
            'cache_hit_delta': hit, 'persistence_delta': deltas,
            'ingress_committed_delta': committed, 'ingress_samples_new_delta': samples,
            'application_ack_count': application_ack_count,
            'database_unchanged': before_db == after_db}


def replace_owned_backend(smoke, fixture, registry, containers, state):
    """Replace only the fixture-owned backend, without changing its Compose profile."""
    old = containers['backend']
    old_obj = registry.resolve(old)
    if old_obj is None:
        raise ValueError('p8_e4_backend_restart_failed')
    name = fixture.project + '-backend-e4-cold'
    fragment = fixture.private / 'compose.e4-cold.yml'
    smoke.private_write(fragment, replacement_fragment(name))
    if registry.command(['stop', '--time', '20', old['receipt']['id']]).returncode:
        raise ValueError('p8_e4_backend_restart_failed')
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
    smoke.private_write(fixture.private / 'e4-cold-replacement.log',
                        (result.stdout + result.stderr).decode(errors='replace'))
    obj = registry.resolve(item)
    if result.returncode or obj is None:
        raise ValueError('p8_e4_backend_restart_failed')
    deadline = time.monotonic() + 45
    try:
        while not smoke.startup_state('backend', obj['State']):
            state.checkpoint()
            if time.monotonic() >= deadline:
                raise ValueError('p8_e4_backend_restart_failed')
            time.sleep(.5)
            obj = registry.resolve(item)
            if obj is None:
                raise ValueError('p8_e4_backend_restart_failed')
        expected_env = old_obj['Config']['Env']
        expected_mounts = old_obj['Mounts']
        expected_host = old_obj['HostConfig']
        evidence = verify_replacement(old_obj, obj, expected_image=old['image'],
            expected_environment=expected_env, expected_mounts=expected_mounts,
            expected_host_config=expected_host)
        if registry.command(['exec', containers['nginx']['receipt']['id'], 'nginx', '-s', 'reload']).returncode:
            raise ValueError('p8_e4_backend_restart_failed')
    except BaseException:
        if obj is not None:
            logs = registry.command(['logs', '--tail', '50', obj['Id']])
            smoke.private_write(fixture.private / 'e4-cold-startup-failure.log',
                                (logs.stdout + logs.stderr).decode(errors='replace'))
        raise
    return {'container': obj, 'replacement': evidence}


def conflict_no_ack_evidence(before_db, after_db, *, guard_precondition,
                             rejection_reason, expected_reason, ack_count):
    """Missing ACK is accepted only alongside a matched DB guard and rejection."""
    reasons = []
    if not isinstance(guard_precondition, dict) or guard_precondition.get('matched') is not True:
        reasons.append('conflict_database_guard_not_matched')
    if not rejection_reason or rejection_reason != expected_reason:
        reasons.append('conflict_rejection_reason_unproven')
    if type(ack_count) is not int or ack_count != 0:
        reasons.append('conflict_application_ack_observed_or_unbounded')
    if before_db != after_db:
        reasons.append('conflict_changed_database_state')
    return {'status': 'PASS' if not reasons else 'FAIL', 'reasons': reasons,
            'guard_precondition': guard_precondition, 'rejection_reason': rejection_reason,
            'application_ack_count': ack_count, 'database_unchanged': before_db == after_db}
