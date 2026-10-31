#!/usr/bin/env python3
"""P8.5c mechanism-arm, comparability, and accounting helpers.

Defines the optimized reference and one-factor ablation arms, validates run
comparability invariants, evaluates mechanism activation eligibility from actual
counter deltas, computes Gamma_red and independent repeat aggregation, and
provides a seeded sequential run-order planner.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import random
import math
import re
import statistics
import sys
import csv
import os
from typing import Any, Dict, List, Optional, Tuple, Union


ARM_NAMES = (
    'optimized',
    'shaper_off',
    'microbatch_off',
    'ram_dedup_off',
    'polling',
    'fanout_scan',
)

MECHANISM_FLAGS = (
    'STAGE5_ENABLED',
    'WEBSOCKET_ENABLED',
    'MQTT_SHAPER_ENABLED',
    'MQTT_MICROBATCH_ENABLED',
    'MQTT_RAM_DEDUP_ENABLED',
    'STAGE5_WAKEUP_ENABLED',
    'WS_FANOUT_MODE',
)

OPTIMIZED_REFERENCE_FLAGS = {
    'STAGE5_ENABLED': 'true',
    'WEBSOCKET_ENABLED': 'true',
    'MQTT_SHAPER_ENABLED': 'true',
    'MQTT_MICROBATCH_ENABLED': 'true',
    'MQTT_RAM_DEDUP_ENABLED': 'true',
    'STAGE5_WAKEUP_ENABLED': 'true',
    'WS_FANOUT_MODE': 'indexed',
}

ABLATION_DIFFS = {
    'shaper_off': ('MQTT_SHAPER_ENABLED', 'false'),
    'microbatch_off': ('MQTT_MICROBATCH_ENABLED', 'false'),
    'ram_dedup_off': ('MQTT_RAM_DEDUP_ENABLED', 'false'),
    'polling': ('STAGE5_WAKEUP_ENABLED', 'false'),
    'fanout_scan': ('WS_FANOUT_MODE', 'scan'),
}

NORMALIZATION_PATHS = (
    ('run_id',), ('created_at',), ('endpoints',), ('generated_fixture_paths',), ('temp_dir',),
    ('gateway_env_path',), ('fixture_paths',),
)


def canonicalize_flag_value(val: Any) -> str:
    """Normalize boolean and string flag values to lower-case canonical strings."""
    if isinstance(val, bool):
        return 'true' if val else 'false'
    if val is None:
        raise ValueError('effective_flag_missing')
    s = str(val).strip().lower()
    return s


def get_expected_arm_flags(arm_name: str) -> Dict[str, str]:
    """Return the expected effective flags for a recognized arm."""
    if arm_name not in ARM_NAMES:
        raise ValueError(f'unknown_arm: {arm_name}')
    flags = copy.deepcopy(OPTIMIZED_REFERENCE_FLAGS)
    if arm_name in ABLATION_DIFFS:
        k, v = ABLATION_DIFFS[arm_name]
        flags[k] = v
    return flags


def validate_arm_flags(arm_name: str, effective_flags: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that effective_flags match the exact intended flags for arm_name."""
    if not isinstance(effective_flags, dict):
        return {'valid': False, 'reason': 'effective_flags_not_dict'}
    try:
        expected = get_expected_arm_flags(arm_name)
    except ValueError as exc:
        return {'valid': False, 'reason': str(exc)}

    missing = [k for k in MECHANISM_FLAGS if k not in effective_flags or effective_flags[k] is None]
    if missing:
        return {'valid': False, 'reason': 'effective_flag_missing', 'missing': missing}
    canonical = {k: canonicalize_flag_value(effective_flags[k]) for k in MECHANISM_FLAGS}
    mismatches = {k: {'expected': expected[k], 'actual': canonical.get(k)}
                  for k in MECHANISM_FLAGS if canonical.get(k) != expected[k]}
    if mismatches:
        return {'valid': False, 'reason': 'flag_mismatch', 'mismatches': mismatches}
    return {'valid': True, 'arm': arm_name, 'effective_flags': canonical}


def validate_mechanism_diff(ref_flags: Dict[str, Any], arm_flags: Dict[str, Any],
                            expected_arm: str) -> Dict[str, Any]:
    """Validate that arm_flags differs from ref_flags by exactly one intended flag."""
    if expected_arm not in ABLATION_DIFFS:
        raise ValueError(f'expected_arm_must_be_ablation_arm: {expected_arm}')

    if not isinstance(ref_flags, dict) or not isinstance(arm_flags, dict):
        return {'valid': False, 'reason': 'effective_flags_not_dict'}
    if set(ref_flags) != set(arm_flags):
        return {'valid': False, 'reason': 'effective_flag_set_mismatch'}
    if any(v is None for v in (*ref_flags.values(), *arm_flags.values())):
        return {'valid': False, 'reason': 'effective_flag_missing'}
    canon_ref = {k: canonicalize_flag_value(v) for k, v in ref_flags.items()}
    canon_arm = {k: canonicalize_flag_value(v) for k, v in arm_flags.items()}

    diffs = {k: {'ref': canon_ref[k], 'arm': canon_arm[k]}
             for k in canon_ref if canon_ref[k] != canon_arm[k]}

    if not diffs:
        return {'valid': False, 'reason': 'two_identical_paths_rejected'}
    if len(diffs) > 1:
        return {'valid': False, 'reason': 'multiple_mechanism_differences_rejected', 'diffs': diffs}

    differing_flag = next(iter(diffs.keys()))
    expected_flag, expected_val = ABLATION_DIFFS[expected_arm]

    if differing_flag != expected_flag or canon_arm[differing_flag] != expected_val:
        return {
            'valid': False,
            'reason': 'unexpected_mechanism_difference',
            'differing_flag': differing_flag,
            'expected_flag': expected_flag,
        }

    return {'valid': True, 'differing_flag': differing_flag, 'expected_arm': expected_arm}


def _remove_path(obj: Dict[str, Any], path: Tuple[str, ...]) -> None:
    """Remove only an explicitly declared top-level metadata path."""
    if len(path) == 1:
        obj.pop(path[0], None)


def normalized_workload_fingerprint(manifest_or_workload: Dict[str, Any]) -> str:
    """Compute a canonical SHA-256 fingerprint for a workload after stripping volatile fields."""
    if not isinstance(manifest_or_workload, dict):
        raise ValueError('workload_must_be_dict')
    if not manifest_or_workload:
        raise ValueError('workload_empty')
    normalized = copy.deepcopy(manifest_or_workload)
    for path in NORMALIZATION_PATHS:
        _remove_path(normalized, path)
    encoded = json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return f'sha256:{hashlib.sha256(encoded).hexdigest()}'


def validate_run_comparability(report_ref: Dict[str, Any], report_arm: Dict[str, Any],
                               expected_arm: str) -> Dict[str, Any]:
    """Validate comparability invariants between reference and ablation run reports."""
    if not isinstance(report_ref, dict) or not isinstance(report_arm, dict):
        return {'comparable': False, 'reason': 'invalid_report_shape'}

    meta_ref = _comparison_metadata(report_ref)
    meta_arm = _comparison_metadata(report_arm)
    for report in (report_ref, report_arm):
        if report.get('status') != 'PASS':
            return {'comparable': False, 'reason': 'run_status_not_pass'}
        cleanup = report.get('cleanup_verified', report.get('metadata', {}).get('cleanup', {}).get('fixture_cleanup_verified'))
        if cleanup is not True:
            return {'comparable': False, 'reason': 'cleanup_unverified'}

    if meta_ref.get('contradictory_binary_alias') or meta_arm.get('contradictory_binary_alias'):
        return {'comparable': False, 'reason': 'contradictory_binary_fingerprint'}

    required_fingerprints = (
        'source_revision',
        'binary_sha256',
        'paperload_source_sha256',
        'image_ids',
        'schema',
        'security_mode',
        'quota',
        'workers',
        'durability',
        'topology',
        'seed',
        'phase_seconds',
        'cohort',
        'workload_sha256',
    )
    for fp in required_fingerprints:
        if fp not in meta_ref or fp not in meta_arm:
            return {'comparable': False, 'reason': f'missing_fingerprint_{fp}'}
        for value in (meta_ref[fp], meta_arm[fp]):
            if value is None or value == '' or value == {} or value == []:
                return {'comparable': False, 'reason': f'unavailable_fingerprint_{fp}'}
    for key in ('quota', 'topology', 'phase_seconds', 'cohort', 'image_ids'):
        if not isinstance(meta_ref[key], (dict, list)) or not isinstance(meta_arm[key], type(meta_ref[key])):
            return {'comparable': False, 'reason': f'invalid_fingerprint_{key}'}
    if not re.fullmatch(r'[0-9a-f]{40,64}', str(meta_ref['source_revision'])):
        return {'comparable': False, 'reason': 'source_revision_fingerprint_invalid'}
    for key in ('binary_sha256', 'paperload_source_sha256'):
        if not re.fullmatch(r'[0-9a-f]{64}', str(meta_ref[key])) or not re.fullmatch(r'[0-9a-f]{64}', str(meta_arm[key])):
            return {'comparable': False, 'reason': f'{key}_fingerprint_invalid'}
    for meta in (meta_ref, meta_arm):
        if not re.fullmatch(r'[0-9a-f]{64}', str(meta['workload_sha256'])):
            return {'comparable': False, 'reason': 'workload_sha256_fingerprint_invalid'}
    for image_map in (meta_ref['image_ids'], meta_arm['image_ids']):
        if not image_map or any(not isinstance(v, str) or not v.strip() for v in image_map.values()):
            return {'comparable': False, 'reason': 'image_fingerprint_invalid'}

    # Constituent client binaries must be valid and match
    is_e5 = (
        report_ref.get('experiment') == 'E5'
        or report_arm.get('experiment') == 'E5'
        or (report_ref.get('metadata') or {}).get('experiment') == 'E5'
        or (report_arm.get('metadata') or {}).get('experiment') == 'E5'
        or (report_ref.get('metadata') or {}).get('b_declared') is not None
        or (report_arm.get('metadata') or {}).get('b_declared') is not None
    )
    if is_e5:
        for key in ('paperload_binary_sha256', 'gateway_binary_sha256'):
            v_ref = meta_ref.get(key)
            v_arm = meta_arm.get(key)
            if not v_ref or not v_arm:
                return {'comparable': False, 'reason': f'missing_fingerprint_{key}'}
            if v_ref != v_arm:
                return {'comparable': False, 'reason': f'{key}_mismatch'}
            if not isinstance(v_ref, str) or not re.fullmatch(r'[0-9a-f]{64}', str(v_ref)):
                return {'comparable': False, 'reason': f'{key}_invalid'}
    else:
        for key in ('paperload_binary_sha256', 'gateway_binary_sha256'):
            v_ref = meta_ref.get(key)
            v_arm = meta_arm.get(key)
            if v_ref is not None or v_arm is not None:
                if not v_ref or not v_arm or v_ref != v_arm:
                    return {'comparable': False, 'reason': f'{key}_mismatch'}
                if not isinstance(v_ref, str) or not re.fullmatch(r'[0-9a-f]{64}', str(v_ref)):
                    return {'comparable': False, 'reason': f'{key}_invalid'}

    if meta_ref['source_revision'] != meta_arm['source_revision']:
        return {'comparable': False, 'reason': 'source_revision_mismatch'}

    # Dirty worktree validation: both must match (both clean or exact same dirty diff hash)
    dirty_ref = meta_ref.get('dirty_tree_hash')
    dirty_arm = meta_arm.get('dirty_tree_hash')
    for meta, dirty_hash in ((meta_ref, dirty_ref), (meta_arm, dirty_arm)):
        dirty = meta.get('dirty_worktree')
        revision = meta.get('source_revision')
        if dirty is True:
            if not isinstance(dirty_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', dirty_hash):
                return {'comparable': False, 'reason': 'dirty_tree_fingerprint_missing'}
        elif dirty is False:
            if not isinstance(revision, str) or not re.fullmatch(r'[0-9a-f]{40,64}', revision):
                return {'comparable': False, 'reason': 'clean_revision_fingerprint_missing'}
        else:
            return {'comparable': False, 'reason': 'dirty_worktree_state_missing'}
    if dirty_ref != dirty_arm:
        if meta_ref.get('dirty_worktree') is not False or meta_arm.get('dirty_worktree') is not False:
            return {'comparable': False, 'reason': 'dirty_worktree_mismatch'}

    if meta_ref['binary_sha256'] != meta_arm['binary_sha256']:
        return {'comparable': False, 'reason': 'binary_fingerprint_mismatch'}
    if meta_ref['paperload_source_sha256'] != meta_arm['paperload_source_sha256']:
        return {'comparable': False, 'reason': 'binary_fingerprint_mismatch'}
    if meta_ref['image_ids'] != meta_arm['image_ids']:
        return {'comparable': False, 'reason': 'binary_fingerprint_mismatch'}

    if meta_ref['schema'] != meta_arm['schema']:
        return {'comparable': False, 'reason': 'schema_mismatch'}
    if meta_ref['security_mode'] != meta_arm['security_mode']:
        return {'comparable': False, 'reason': 'security_mode_mismatch'}
    if meta_ref['quota'] != meta_arm['quota']:
        return {'comparable': False, 'reason': 'quota_mismatch'}
    if meta_ref['workers'] != meta_arm['workers']:
        return {'comparable': False, 'reason': 'worker_pool_mismatch'}
    if meta_ref['durability'] != meta_arm['durability']:
        return {'comparable': False, 'reason': 'durability_mismatch'}
    if meta_ref['topology'] != meta_arm['topology']:
        return {'comparable': False, 'reason': 'topology_mismatch'}
    if meta_ref['seed'] != meta_arm['seed']:
        return {'comparable': False, 'reason': 'seed_mismatch'}
    if meta_ref['phase_seconds'] != meta_arm['phase_seconds']:
        return {'comparable': False, 'reason': 'window_duration_mismatch'}
    if meta_ref['cohort'] != meta_arm['cohort']:
        return {'comparable': False, 'reason': 'cohort_mismatch'}
    if meta_ref['workload_sha256'] != meta_arm['workload_sha256']:
        return {'comparable': False, 'reason': 'workload_sha256_mismatch'}

    # Workload normalization
    w_ref = meta_ref.get('workload')
    w_arm = meta_arm.get('workload')
    if not isinstance(w_ref, dict) or not w_ref or not isinstance(w_arm, dict) or not w_arm:
        return {'comparable': False, 'reason': 'workload_missing_or_empty'}
    fp_ref = normalized_workload_fingerprint(w_ref)
    fp_arm = normalized_workload_fingerprint(w_arm)
    expected_workload_ref = fp_ref.removeprefix('sha256:')
    expected_workload_arm = fp_arm.removeprefix('sha256:')
    if meta_ref['workload_sha256'] != expected_workload_ref or meta_arm['workload_sha256'] != expected_workload_arm:
        return {'comparable': False, 'reason': 'workload_sha256_content_mismatch'}
    if fp_ref != fp_arm:
        return {'comparable': False, 'reason': 'workload_fingerprint_mismatch'}

    # Mechanism difference validation
    flags_ref = meta_ref.get('effective_flags', {})
    flags_arm = meta_arm.get('effective_flags', {})
    arm_ref_check = validate_arm_flags('optimized', flags_ref)
    arm_actual_check = validate_arm_flags(expected_arm, flags_arm)
    if not arm_ref_check['valid']:
        return {'comparable': False, 'reason': 'optimized_reference_flags_invalid', 'detail': arm_ref_check['reason']}
    if not arm_actual_check['valid']:
        return {'comparable': False, 'reason': 'ablation_flags_invalid', 'detail': arm_actual_check['reason']}
    diff_res = validate_mechanism_diff(flags_ref, flags_arm, expected_arm)
    if not diff_res['valid']:
        return {'comparable': False, 'reason': diff_res['reason']}

    return {
        'comparable': True,
        'reason': None,
        'expected_arm': expected_arm,
        'differing_flag': diff_res['differing_flag'],
        'normalized_workload_fingerprint': fp_ref,
    }


def _comparison_metadata(report: Dict[str, Any]) -> Dict[str, Any]:
    """Read comparison metadata from new reports or map known P8.5a field names.

    The legacy P8.5a schema lacks workload content and dirty-tree hash, so it
    remains unqualified rather than having those facts inferred.
    """
    meta = report.get('metadata')
    if not isinstance(meta, dict):
        meta = {}
    mapped = dict(meta)

    # Detect contradictory canonical binary vs combined alias if both provided
    can_bin = meta.get('binary_sha256') or report.get('binary_sha256')
    comb_bin = meta.get('combined_binary_sha256') or report.get('combined_binary_sha256')
    if (
        can_bin and comb_bin
        and isinstance(can_bin, str) and isinstance(comb_bin, str)
        and can_bin != comb_bin
    ):
        mapped['contradictory_binary_alias'] = True

    resolved_binary = can_bin or comb_bin

    # Remote E5 reports provide b_declared with declared_measurement_windows
    b_decl = meta.get('b_declared')
    has_remote_e5_declared = isinstance(b_decl, dict) and 'declared_measurement_windows' in b_decl
    if has_remote_e5_declared:
        decl_win = b_decl['declared_measurement_windows']
        mapped['observed_phase_seconds'] = meta.get('phase_seconds') or b_decl.get('observed_measurement_windows')
        expected_phases = {'initial', 'warm_replay', 'control'}
        if (
            isinstance(decl_win, dict)
            and set(decl_win.keys()) == expected_phases
            and all(type(v) is not bool and isinstance(v, (int, float)) and math.isfinite(v) and v > 0 for v in decl_win.values())
        ):
            mapped['phase_seconds'] = decl_win
        else:
            mapped['phase_seconds'] = 'INVALID_DECLARED_WINDOWS'

    aliases = {
        'source_revision': report.get('source_revision') or (report.get('source_fingerprint') or {}).get('revision'),
        'binary_sha256': resolved_binary,
        'paperload_binary_sha256': meta.get('paperload_binary_sha256') or report.get('paperload_binary_sha256'),
        'gateway_binary_sha256': meta.get('gateway_binary_sha256') or report.get('gateway_binary_sha256'),
        'combined_binary_sha256': comb_bin,
        'paperload_source_sha256': report.get('paperload_source_sha256', meta.get('source_sha256')),
        'image_ids': report.get('image_ids', meta.get('image_ids')),
        'schema': report.get('schema_terminal', meta.get('actual_schema', report.get('schema'))),
        'quota': report.get('backend_quota', meta.get('backend_quota', report.get('quota'))),
        'security_mode': meta.get('tls', {}).get('mode') if isinstance(meta.get('tls'), dict) else (meta.get('security_mode') or report.get('security_mode')),
        'workers': meta.get('caps', {}).get('workers') if isinstance(meta.get('caps'), dict) else (meta.get('workers') or report.get('workers')),
        'topology': meta.get('topology') or report.get('topology'),
        'seed': meta.get('seed') or (report.get('context') or {}).get('seed'),
        'phase_seconds': None if has_remote_e5_declared else meta.get('phases', meta.get('sampling', {}).get('effective_window')),
        'cohort': meta.get('cohort') or report.get('cohort'),
        'dirty_tree_hash': report.get('dirty_tree_hash') or (report.get('source_fingerprint') or {}).get('dirty_source_sha256'),
        'dirty_worktree': report.get('dirty_worktree') or (report.get('source_fingerprint') or {}).get('dirty'),
        'workload': meta.get('workload') or report.get('workload'),
        'effective_flags': report.get('effective_flags', meta.get('effective_flags')),
    }
    for key, value in aliases.items():
        if key not in mapped or mapped[key] is None:
            mapped[key] = value
    return mapped


def check_mechanism_activation(arm_name: str, report_ref: Dict[str, Any],
                               report_arm: Dict[str, Any]) -> Dict[str, Any]:
    """Verify that the mechanism under test was active during the run.

    Missing or inactive mechanisms return INCONCLUSIVE rather than a fabricated
    or zero improvement percentage.
    """
    if arm_name not in ABLATION_DIFFS:
        raise ValueError(f'arm_must_be_ablation_arm: {arm_name}')

    acc_ref = report_ref.get('accounting', {})
    mech_ref = report_ref.get('mechanism_metrics', {})
    ingress = acc_ref.get('ingress_delta', report_ref.get('ingress_delta', {}))
    ing_ref = ingress.get('delta', {}) if isinstance(ingress, dict) else {}

    def count(obj, field, reason):
        if not isinstance(obj, dict) or field not in obj:
            return None, {'eligible': False, 'status': 'INCONCLUSIVE', 'reason': f'unavailable_{reason}_{field}'}
        value = obj[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            return None, {'eligible': False, 'status': 'INCONCLUSIVE', 'reason': f'unavailable_invalid_{reason}_{field}'}
        return value, None

    if arm_name == 'shaper_off':
        shaper_limited, unavailable = count(ing_ref, 'ShaperRateLimited', 'shaper_counter')
        if unavailable:
            return unavailable
        shaper_passed, _ = count(ing_ref, 'ShaperPassed', 'shaper_counter')
        if shaper_limited <= 0:
            return {
                'eligible': False,
                'status': 'INCONCLUSIVE',
                'reason': 'mechanism_inactive_no_shaper_rate_limiting',
                'shaper_rate_limited': shaper_limited,
                'shaper_passed': shaper_passed,
            }
        return {'eligible': True, 'status': 'ELIGIBLE', 'reason': None}

    if arm_name == 'microbatch_off':
        mb = mech_ref.get('microbatch', {})
        groups, unavailable = count(mb, 'groups', 'microbatch_counter')
        if unavailable:
            return unavailable
        members, unavailable = count(mb, 'members', 'microbatch_counter')
        if unavailable:
            return unavailable
        maximum, unavailable = count(mb, 'max_members', 'microbatch_counter')
        if unavailable:
            return unavailable
        multi_member = 1 if groups > 0 and members > groups and maximum >= 2 else 0
        if multi_member <= 0:
            return {
                'eligible': False,
                'status': 'INCONCLUSIVE',
                'reason': 'mechanism_inactive_no_multi_member_batches',
                'multi_member_groups': multi_member,
            }
        return {'eligible': True, 'status': 'ELIGIBLE', 'reason': None,
                'multi_member_groups_lower_bound': 1,
                'provenance': 'members_exceed_groups_and_max_members_at_least_two'}

    if arm_name == 'ram_dedup_off':
        rd = mech_ref.get('ram_dedup', {})
        ram_hits, unavailable = count(rd, 'lookup_hit_total', 'ram_dedup_counter')
        if unavailable:
            return unavailable
        if ram_hits <= 0:
            return {
                'eligible': False,
                'status': 'INCONCLUSIVE',
                'reason': 'mechanism_inactive_no_warm_ram_hits',
                'ram_hits': ram_hits,
            }
        return {'eligible': True, 'status': 'ELIGIBLE', 'reason': None}

    if arm_name == 'polling':
        cmd = mech_ref.get('command', {})
        dispatched, unavailable = count(cmd, 'fresh_commands_dispatched', 'wakeup_counter')
        if unavailable:
            return unavailable
        wakeups, unavailable = count(cmd, 'notification_signals_accepted', 'wakeup_counter')
        if unavailable:
            return unavailable
        pubacks, unavailable = count(cmd, 'puback', 'wakeup_counter')
        if unavailable:
            return unavailable
        polling_cycles, unavailable = count(report_arm.get('mechanism_metrics', {}).get('command', {}), 'scans', 'polling_arm_counter')
        if unavailable:
            return unavailable
        if dispatched != 4 or wakeups <= 0 or pubacks <= 0 or polling_cycles <= 0:
            return {
                'eligible': False,
                'status': 'INCONCLUSIVE',
                'reason': 'mechanism_inactive_no_commands_or_wakeups',
                'commands_dispatched': dispatched,
                'wakeups_observed': wakeups,
                'pubacks': pubacks,
                'polling_cycles': polling_cycles,
            }
        return {'eligible': True, 'status': 'ELIGIBLE', 'reason': None}

    if arm_name == 'fanout_scan':
        fo = mech_ref.get('fanout', {})
        subs, unavailable = count(fo, 'subscriptions_ready', 'fanout_counter')
        if unavailable:
            return unavailable
        delivered, unavailable = count(fo, 'writes', 'fanout_counter')
        if unavailable:
            return unavailable
        scan_eval, unavailable = count(report_arm.get('mechanism_metrics', {}).get('fanout', {}), 'scan_evaluations', 'fanout_arm_counter')
        if unavailable:
            return unavailable
        index_select, unavailable = count(fo, 'indexed_selections', 'fanout_reference_counter')
        if unavailable:
            return unavailable
        if fo.get('readiness_verified') is not True:
            return {'eligible': False, 'status': 'INCONCLUSIVE', 'reason': 'fanout_viewer_readiness_unverified'}
        if subs < 2 or delivered <= 0 or scan_eval <= 0 or index_select <= 0:
            return {
                'eligible': False,
                'status': 'INCONCLUSIVE',
                'reason': 'mechanism_inactive_no_fanout_deliveries',
                'active_subscriptions': subs,
                'events_delivered': delivered,
                'scan_evaluations': scan_eval,
                'indexed_selections': index_select,
            }
        return {'eligible': True, 'status': 'ELIGIBLE', 'reason': None}

    return {'eligible': False, 'status': 'INCONCLUSIVE', 'reason': f'unhandled_arm_{arm_name}'}


def calculate_gamma_red(c_reference: Union[int, float], c_optimized: Union[int, float],
                        *, metric_name: str,
                        unique_messages_ref: Optional[int] = None,
                        unique_messages_opt: Optional[int] = None) -> Dict[str, Any]:
    """Calculate percentage reduction Gamma_red with named metric and safe reference check.

    Gamma_red = 100.0 * (C_reference - C_optimized) / C_reference
    If reference is zero or negative, returns unavailable.
    """
    def valid_number(value):
        return (not isinstance(value, bool) and isinstance(value, (int, float))
                and math.isfinite(value) and value >= 0)
    if (isinstance(c_reference, bool) or not isinstance(c_reference, (int, float))
            or not math.isfinite(c_reference)):
        return {'available': False, 'reason': 'invalid_metric_value', 'metric': metric_name}
    if c_reference <= 0:
        return {'available': False, 'reason': 'reference_zero_or_negative', 'metric': metric_name}
    if not valid_number(c_optimized):
        return {'available': False, 'reason': 'invalid_metric_value', 'metric': metric_name}
    if not isinstance(metric_name, str) or not metric_name.strip():
        return {'available': False, 'reason': 'metric_name_missing'}
    if unique_messages_ref is not None or unique_messages_opt is not None:
        if (unique_messages_ref is None or unique_messages_opt is None
                or isinstance(unique_messages_ref, bool) or isinstance(unique_messages_opt, bool)
                or not isinstance(unique_messages_ref, int) or not isinstance(unique_messages_opt, int)
                or unique_messages_ref <= 0 or unique_messages_opt <= 0):
            return {'available': False, 'reason': 'unique_messages_zero_or_negative', 'metric': metric_name}
        ref_norm = float(c_reference) / unique_messages_ref
        opt_norm = float(c_optimized) / unique_messages_opt
        if ref_norm <= 0:
            return {'available': False, 'reason': 'normalized_reference_zero_or_negative', 'metric': metric_name}
        gamma = 100.0 * (ref_norm - opt_norm) / ref_norm
        return {
            'available': True,
            'gamma_red_percent': gamma,
            'metric': metric_name,
            'normalized': True,
            'c_ref_raw': c_reference,
            'c_opt_raw': c_optimized,
            'c_ref_norm': ref_norm,
            'c_opt_norm': opt_norm,
        }

    gamma = 100.0 * (float(c_reference) - float(c_optimized)) / float(c_reference)
    return {
        'available': True,
        'gamma_red_percent': gamma,
        'metric': metric_name,
        'normalized': False,
        'c_ref': c_reference,
        'c_opt': c_optimized,
    }


def verify_raw_retention(raw_ref: Any, raw_opt: Any, *, content_provenance: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Verify that raw rows and content count match across arms.

    Raw retention is a separate count/content guarantee, never reduced operation count.
    """
    if (isinstance(raw_ref, int) and not isinstance(raw_ref, bool) and raw_ref >= 0
            and isinstance(raw_opt, int) and not isinstance(raw_opt, bool) and raw_opt >= 0
            and raw_ref == raw_opt and isinstance(content_provenance, dict)
            and content_provenance.get('verified') is True
            and content_provenance.get('reference_digest')
            and content_provenance.get('optimized_digest')
            and content_provenance['reference_digest'] == content_provenance['optimized_digest']):
        return {
            'retention_verified': True,
            'reason': None,
            'raw_ref': raw_ref,
            'raw_opt': raw_opt,
        }
    return {
        'retention_verified': False,
        'reason': 'raw_retention_mismatch' if raw_ref != raw_opt else 'raw_content_provenance_missing_or_mismatched',
        'raw_ref': raw_ref,
        'raw_opt': raw_opt,
    }


def aggregate_independent_repeats(runs_data: List[Dict[str, Any]],
                                  metric_keys: List[str]) -> Dict[str, Any]:
    """Aggregate declared independent repeats by computing per-run first, then median and range.

    Prohibits synthetic confidence intervals and merging individual run percentiles.
    """
    if not runs_data:
        return {'available': False, 'reason': 'no_runs_provided'}

    run_ids = []
    compatibility = []
    for run in runs_data:
        if not isinstance(run, dict):
            return {'available': False, 'reason': 'invalid_run_record'}
        run_id = run.get('run_id')
        if not isinstance(run_id, str) or not run_id.strip():
            return {'available': False, 'reason': 'run_id_missing'}
        if run.get('status') != 'PASS' or run.get('cleanup_verified') is not True:
            return {'available': False, 'reason': 'run_failed_or_cleanup_unverified'}
        compat = run.get('compatibility_key')
        if not isinstance(compat, str) or not compat.strip():
            return {'available': False, 'reason': 'source_compatibility_missing'}
        run_ids.append(run_id)
        compatibility.append(compat)
    if len(set(run_ids)) != len(run_ids):
        return {'available': False, 'reason': 'run_id_duplicate'}
    if len(set(compatibility)) != 1:
        return {'available': False, 'reason': 'source_compatibility_mismatch'}

    metrics_summary = {}
    for key in metric_keys:
        values = []
        for r in runs_data:
            val = r.get(key)
            if (isinstance(val, bool) or not isinstance(val, (int, float))
                    or not math.isfinite(val) or val < 0):
                return {'available': False, 'reason': f'invalid_metric_value_{key}'}
            values.append(val)
        if not values:
            metrics_summary[key] = {'available': False, 'reason': 'no_numeric_values'}
            continue

        med = statistics.median(values)
        val_min = min(values)
        val_max = max(values)
        val_range = val_max - val_min
        metrics_summary[key] = {
            'available': True,
            'count': len(values),
            'median': med,
            'min': val_min,
            'max': val_max,
            'range': val_range,
        }

    return {
        'available': True,
        'count': len(runs_data),
        'metrics': metrics_summary,
        'synthetic_ci_computed': False,
        'aggregation_note': (
            'Per-run values computed first; reporting median and range across independent runs. '
            'Synthetic confidence intervals and merged percentile distributions are intentionally prohibited.'
        ),
    }


def plan_run_order(arms: List[str], repeats: int, seed: int,
                   *, include_cooldown: bool = True) -> Dict[str, Any]:
    """Generate a deterministic pseudo-random sequential execution plan.

    Does NOT execute the matrix automatically.
    """
    if not arms or any(a not in ARM_NAMES for a in arms):
        raise ValueError(f'invalid_arms: {arms}')
    if repeats < 1:
        raise ValueError(f'repeats_must_be_positive: {repeats}')

    # Generate sequential repetitions: list of (arm, repeat_idx)
    items = []
    for rep in range(repeats):
        for arm in arms:
            items.append((arm, rep))

    # Shuffle deterministically using seed
    rng = random.Random(seed)
    rng.shuffle(items)

    plan = []
    for idx, (arm, rep) in enumerate(items):
        run_seed = seed + rep
        plan.append({
            'order': idx,
            'arm': arm,
            'repeat_index': rep,
            'run_seed': run_seed,
            'run_id': f'repeat-{rep + 1}-{arm}',
            'cooldown_required': include_cooldown,
        })

    return {
        'plan': plan,
        'arms': list(arms),
        'repeats': repeats,
        'master_seed': seed,
        'executed': False,
        'official_repeats_gte_3': repeats >= 3,
        'note': 'sequential run-order plan only; MUST NOT execute automatically',
    }


CLEANUP_COUNT_KEYS = frozenset({'container', 'containers', 'network', 'networks', 'volume', 'volumes'})
DATABASE_DELTA_KEYS = frozenset({'ledger', 'raw', 'temporal', 'configuration'})
IMAGE_ID_KEYS = frozenset({
    'application-migrations', 'auth-socket-init', 'mosquitto', 'nginx', 'postgres',
    'supabase-auth', 'supabase-compat-migration', 'supabase-envoy',
    'supabase-role-provisioner', 'supabase-storage', 'backend', 'gateway',
})


def _sanitize_cleanup_counts(val: Any) -> Dict[str, int]:
    if not isinstance(val, dict):
        return {}
    sanitized = {}
    for k, v in val.items():
        if k in CLEANUP_COUNT_KEYS and type(v) is int and v >= 0:
            sanitized[k] = v
    return sanitized


def _sanitize_image_ids(val: Any) -> Dict[str, str]:
    if not isinstance(val, dict):
        return {}
    sanitized = {}
    for k, v in val.items():
        if (k in IMAGE_ID_KEYS
                and isinstance(v, str) and re.fullmatch(r'^sha256:[0-9a-f]{64}$', v)):
            sanitized[k] = v
    return sanitized


def _sanitize_database_delta(val: Any) -> Dict[str, int]:
    if not isinstance(val, dict):
        return {}
    sanitized = {}
    for k, v in val.items():
        if k in DATABASE_DELTA_KEYS and type(v) is int and v >= 0:
            sanitized[k] = v
    return sanitized


def _safe_persistence_delta(
    report: Dict[str, Any], phase: str, metric: str
) -> Tuple[Optional[int], Optional[int], Optional[int], str, Optional[str]]:
    windows = (report.get('phase_windows') or {}).get(phase) or {}
    before_snap = windows.get('admin_before') or {}
    after_snap = windows.get('admin_after') or {}
    proc_before = before_snap.get('process_instance')
    proc_after = after_snap.get('process_instance')
    if (
        not isinstance(proc_before, str)
        or not isinstance(proc_after, str)
        or not proc_before.strip()
        or proc_before != proc_after
    ):
        return None, None, None, 'UNAVAILABLE', 'process_instance_mismatch_or_missing'
    before_pers = before_snap.get('persistence') or {}
    after_pers = after_snap.get('persistence') or {}
    b = before_pers.get(metric)
    a = after_pers.get(metric)
    if type(b) is not int or b < 0 or type(a) is not int or a < 0:
        return None, None, None, 'UNAVAILABLE', 'counter_missing_or_invalid'
    if a < b:
        return b, a, None, 'UNAVAILABLE', 'counter_reset'
    return b, a, a - b, 'AVAILABLE', None


def _e5_adapter(report: Dict[str, Any]) -> Dict[str, Any]:
    """Map frozen E5 schema to the existing strict pair/activation helpers."""
    meta = dict(report.get('metadata') or {})
    can_bin = meta.get('binary_sha256') or report.get('binary_sha256')
    comb_bin = meta.get('combined_binary_sha256') or report.get('combined_binary_sha256')
    if can_bin and comb_bin and isinstance(can_bin, str) and isinstance(comb_bin, str) and can_bin != comb_bin:
        meta['contradictory_binary_alias'] = True

    combined_binary = can_bin or comb_bin

    b_decl = meta.get('b_declared')
    decl_win = None
    has_remote_e5_declared = isinstance(b_decl, dict) and 'declared_measurement_windows' in b_decl
    if has_remote_e5_declared:
        dw = b_decl['declared_measurement_windows']
        expected_phases = {'initial', 'warm_replay', 'control'}
        if (
            isinstance(dw, dict)
            and set(dw.keys()) == expected_phases
            and all(type(v) is not bool and isinstance(v, (int, float)) and math.isfinite(v) and v > 0 for v in dw.values())
        ):
            decl_win = dw
        else:
            decl_win = 'INVALID_DECLARED_WINDOWS'

    meta.update({
        'source_revision': report.get('source_revision') or meta.get('source_revision') or (report.get('source_fingerprint') or {}).get('revision'),
        'binary_sha256': combined_binary,
        'paperload_binary_sha256': meta.get('paperload_binary_sha256') or report.get('paperload_binary_sha256'),
        'gateway_binary_sha256': meta.get('gateway_binary_sha256') or report.get('gateway_binary_sha256'),
        'combined_binary_sha256': comb_bin or can_bin,
        'paperload_source_sha256': meta.get('paperload_source_sha256'),
        'image_ids': report.get('image_ids') or meta.get('image_ids'),
        'schema': report.get('schema') or meta.get('schema'),
        'security_mode': json.dumps(meta.get('security_mode'), sort_keys=True) if isinstance(meta.get('security_mode'), dict) else (meta.get('security_mode') or 'jwt_hs256_dynsec_tls'),
        'workload_sha256': report.get('workload_sha256') or meta.get('workload_sha256'),
        'workload': report.get('workload') or meta.get('workload'),
        'effective_flags': report.get('effective_flags') or meta.get('effective_flags'),
        'dirty_worktree': (report.get('source_fingerprint') or {}).get('dirty', meta.get('dirty_worktree')),
        'dirty_tree_hash': (report.get('source_fingerprint') or {}).get('dirty_source_sha256', meta.get('dirty_tree_hash')),
        'topology': meta.get('topology'),
        'seed': meta.get('seed'),
        'phase_seconds': decl_win if has_remote_e5_declared else meta.get('phase_seconds'),
        'observed_phase_seconds': meta.get('phase_seconds') or (b_decl.get('observed_measurement_windows') if isinstance(b_decl, dict) else None),
        'cohort': meta.get('cohort'),
        'quota': report.get('quota') or meta.get('quota'),
        'workers': meta.get('workers'),
        'durability': meta.get('durability'),
    })
    return {
        'status': report.get('status'),
        'cleanup_verified': report.get('cleanup_verified', report.get('exact_cleanup_verified')),
        'exact_cleanup_verified': report.get('exact_cleanup_verified', report.get('cleanup_verified')),
        'metadata': meta,
        'accounting': report.get('accounting'),
        'mechanism_metrics': report.get('mechanisms') or report.get('mechanism_metrics'),
        'mechanisms': report.get('mechanisms') or report.get('mechanism_metrics'),
    }


def _e5_num(obj, key):
    v = obj.get(key) if isinstance(obj, dict) else None
    return v if type(v) is int and v >= 0 else None


def build_e5_summary(
    arm_paths: Dict[str, Union[str, Path]],
    *,
    private_context: Optional[str] = None,
) -> Dict[str, Any]:
    """Build safe one-repeat E5 run/pair exports; raw report trees are never copied."""
    if set(arm_paths) != set(ARM_NAMES):
        raise ValueError('exactly_six_arm_paths_required')
    reports, raw_hashes = {}, {}
    for arm in ARM_NAMES:
        path = Path(arm_paths[arm])
        if not path.is_file():
            raise ValueError('missing_report_' + arm)
        raw = path.read_bytes()
        report = json.loads(raw)
        if report.get('experiment') != 'E5' or report.get('arm') != arm:
            raise ValueError('report_arm_mismatch_' + arm)
        if (
            report.get('status') != 'PASS'
            or report.get('full_p8_accepted') is not False
            or report.get('cleanup_verified') is not True
            or report.get('exact_cleanup_verified') is not True
            or (report.get('validation') or {}).get('valid') is not True
        ):
            raise ValueError('report_not_qualified_pass_' + arm)
        reports[arm] = report
        raw_hashes[arm] = hashlib.sha256(raw).hexdigest()

    adapted = {a: _e5_adapter(r) for a, r in reports.items()}
    paired = []
    opt_r = reports['optimized']
    opt_adapted = adapted['optimized']
    for arm in ARM_NAMES[1:]:
        arm_r = reports[arm]
        arm_adapted = adapted[arm]
        # Strict comparability check using validate_run_comparability on original reports
        comp = validate_run_comparability(opt_r, arm_r, arm)
        if not comp.get('comparable'):
            raise ValueError('pair_noncomparable_' + arm + ':' + str(comp.get('reason')))
        # Strict individual Gateway and paperload binary check
        gw_arm = arm_r.get('gateway_binary_sha256') or arm_r.get('metadata', {}).get('gateway_binary_sha256')
        gw_opt = opt_r.get('gateway_binary_sha256') or opt_r.get('metadata', {}).get('gateway_binary_sha256')
        if not gw_arm or not gw_opt or gw_arm != gw_opt:
            raise ValueError('pair_noncomparable_' + arm + ':gateway_binary_sha256_mismatch')
        pl_arm = arm_r.get('paperload_binary_sha256') or arm_r.get('metadata', {}).get('paperload_binary_sha256')
        pl_opt = opt_r.get('paperload_binary_sha256') or opt_r.get('metadata', {}).get('paperload_binary_sha256')
        if not pl_arm or not pl_opt or pl_arm != pl_opt:
            raise ValueError('pair_noncomparable_' + arm + ':paperload_binary_sha256_mismatch')
        activation = check_mechanism_activation(arm, opt_adapted, arm_adapted)
        paired.append({'arm': arm, 'comparability': comp, 'activation': activation})

    def persistence_window(report, phase):
        result = {}
        for key in ('begin_attempts', 'confirmed_commits', 'copy_calls', 'copy_returned_rows'):
            b, a, delta, status, reason = _safe_persistence_delta(report, phase, key)
            result[key] = {
                'before': b,
                'after': a,
                'delta': delta,
                'status': status,
                'reason': reason,
            }
        return result

    run_rows = []
    for arm in ARM_NAMES:
        r = reports[arm]
        phases = r.get('phases') or {}
        initial = phases.get('initial') or {}
        warm = phases.get('warm_replay') or {}
        d = r.get('accounting') or {}
        delta = (d.get('ingress_delta') or {}).get('delta') or {}
        viewer = (r.get('viewer') or {}).get('result') or {}
        mechanisms = r.get('mechanisms') or r.get('mechanism_metrics') or {}
        mm = mechanisms.get('microbatch') or {}
        ram = mechanisms.get('ram_dedup') or {}

        # Command-window timeline inspection: do not use initial snapshot before commands
        cmd_window = (mechanisms.get('command') or {}).get('timeline_window')
        if isinstance(cmd_window, dict) and cmd_window.get('status') == 'AVAILABLE':
            disp_cnt = _e5_num(cmd_window.get('d_dispatch'), 'count')
            e2e_cnt = _e5_num(cmd_window.get('d_e2e_succeeded'), 'count')
            cmd_status = 'AVAILABLE'
            cmd_reason = None
        else:
            cmd_status = (cmd_window.get('status') if isinstance(cmd_window, dict) else None) or 'UNAVAILABLE'
            cmd_reason = (cmd_window.get('reason') if isinstance(cmd_window, dict) else None) or 'command_window_timeline_not_recorded'
            disp_cnt = None
            e2e_cnt = None

        flags = r.get('effective_flags') or r.get('metadata', {}).get('effective_flags') or {}
        run_rows.append({
            'arm': arm,
            'status': r['status'],
            'full_p8_accepted': False,
            'cleanup_verified': r['cleanup_verified'],
            'exact_cleanup_verified': r['exact_cleanup_verified'],
            'cleanup_counts': _sanitize_cleanup_counts(r.get('cleanup_counts') or r.get('metadata', {}).get('cleanup_counts')),
            'flags': {k: v for k, v in flags.items() if k in MECHANISM_FLAGS},
            'source_revision': r.get('source_revision') or r.get('metadata', {}).get('source_revision'),
            'source_dirty_sha256': (r.get('source_fingerprint') or {}).get('dirty_source_sha256') or r.get('metadata', {}).get('dirty_tree_hash'),
            'paperload_binary_sha256': r.get('paperload_binary_sha256') or r.get('metadata', {}).get('paperload_binary_sha256'),
            'gateway_binary_sha256': r.get('gateway_binary_sha256') or r.get('metadata', {}).get('gateway_binary_sha256'),
            'image_ids': _sanitize_image_ids(r.get('image_ids') or r.get('metadata', {}).get('image_ids')),
            'journal_sha256': r.get('journal_sha256'),
            'workload_sha256': r.get('workload_sha256'),
            'raw_report_sha256': raw_hashes[arm],
            'logical_artifact_id': f'p8-e5-{arm}-{raw_hashes[arm][:16]}',
            'control_runner_status': (r.get('control_run') or {}).get('status'),
            'initial_phase_status': initial.get('status'),
            'warm_phase_status': warm.get('status'),
            'initial_persistence_delta': _sanitize_database_delta(initial.get('database_delta')),
            'warm_persistence_delta': _sanitize_database_delta(warm.get('database_delta')),
            'initial_repository_counters': persistence_window(r, 'initial'),
            'warm_repository_counters': persistence_window(r, 'warm_replay'),
            'committed': _e5_num(delta, 'Committed'),
            'duplicate_committed': _e5_num(delta, 'DuplicateCommitted'),
            'samples_new': _e5_num(delta, 'SamplesNew'),
            'warm_replay': _e5_num(d, 'warm_replay'),
            'receipts_verified': _e5_num(d, 'command_receipts_verified'),
            'configurations_verified': _e5_num(d, 'command_configurations_verified'),
            'viewer_count': _e5_num(viewer, 'connected_distinct_viewers'),
            'viewer_unique_events': _e5_num(viewer, 'unique_events'),
            'viewer_duplicates': _e5_num(viewer, 'duplicates'),
            'viewer_wrong_namespace': _e5_num(viewer, 'wrong_namespace'),
            'microbatch_groups': _e5_num(mm, 'groups'),
            'microbatch_members': _e5_num(mm, 'members'),
            'ram_lookup_hits': _e5_num(ram, 'lookup_hit_total'),
            'ram_db_calls_avoided': _e5_num(ram, 'database_calls_avoided_total'),
            'shaper_rate_limited': _e5_num((mechanisms.get('shaper') or {}), 'rate_limited'),
            'fanout_indexed_selections': _e5_num((mechanisms.get('fanout') or {}), 'indexed_selections'),
            'fanout_scan_evaluations': _e5_num((mechanisms.get('fanout') or {}), 'scan_evaluations'),
            'command_timeline_status': cmd_status,
            'command_timeline_reason': cmd_reason,
            'timeline_dispatch_count': disp_cnt,
            'timeline_e2e_count': e2e_cnt,
        })

    pairs = []
    for arm in ARM_NAMES[1:]:
        r, o = reports[arm], reports['optimized']
        record = next(p for p in paired if p['arm'] == arm)
        row = {
            'arm': arm,
            'reference_arm': 'optimized',
            'comparison_arm': arm,
            'comparability': 'COMPARABLE',
            'activation_status': record['activation']['status'],
            'activation_reason': record['activation'].get('reason'),
            'window': 'initial+warm_replay as stated per metric',
            'denominator': 'same single pilot; no repeat aggregation',
        }
        if arm == 'microbatch_off':
            b_ref_copy, a_ref_copy, d_ref_copy, s_ref_copy, r_ref_copy = _safe_persistence_delta(r, 'initial', 'copy_calls')
            b_opt_copy, a_opt_copy, d_opt_copy, s_opt_copy, r_opt_copy = _safe_persistence_delta(o, 'initial', 'copy_calls')
            b_ref_comm, a_ref_comm, d_ref_comm, s_ref_comm, r_ref_comm = _safe_persistence_delta(r, 'initial', 'confirmed_commits')
            b_opt_comm, a_opt_comm, d_opt_comm, s_opt_comm, r_opt_comm = _safe_persistence_delta(o, 'initial', 'confirmed_commits')
            b_ref_att, a_ref_att, d_ref_att, s_ref_att, r_ref_att = _safe_persistence_delta(r, 'initial', 'begin_attempts')
            b_opt_att, a_opt_att, d_opt_att, s_opt_att, r_opt_att = _safe_persistence_delta(o, 'initial', 'begin_attempts')

            gamma_copy = (
                calculate_gamma_red(d_ref_copy, d_opt_copy, metric_name='initial_repository_copy_calls')
                if (d_ref_copy is not None and d_opt_copy is not None)
                else {'available': False, 'reason': r_ref_copy or r_opt_copy or 'counter_unavailable'}
            )
            gamma_commit = (
                calculate_gamma_red(d_ref_comm, d_opt_comm, metric_name='initial_confirmed_commits')
                if (d_ref_comm is not None and d_opt_comm is not None)
                else {'available': False, 'reason': r_ref_comm or r_opt_comm or 'counter_unavailable'}
            )
            gamma_attempts = (
                calculate_gamma_red(d_ref_att, d_opt_att, metric_name='initial_begin_attempts')
                if (d_ref_att is not None and d_opt_att is not None)
                else {'available': False, 'reason': r_ref_att or r_opt_att or 'counter_unavailable'}
            )

            row.update({
                'window': 'initial import phase',
                'provenance': 'phase_windows.initial admin_before/admin_after persistence deltas with matching process_instance; C_reference=microbatch_off; C_optimized=optimized',
                'ablation_copy_calls': d_ref_copy,
                'optimized_copy_calls': d_opt_copy,
                'ablation_confirmed_commits': d_ref_comm,
                'optimized_confirmed_commits': d_opt_comm,
                'ablation_begin_attempts': d_ref_att,
                'optimized_begin_attempts': d_opt_att,
                'raw_samples_retained_ablation': _sanitize_database_delta(r.get('phases', {}).get('initial', {}).get('database_delta')).get('raw'),
                'raw_samples_retained_optimized': _sanitize_database_delta(o.get('phases', {}).get('initial', {}).get('database_delta')).get('raw'),
                'gamma_copy_calls': gamma_copy,
                'gamma_confirmed_commits': gamma_commit,
                'gamma_begin_attempts': gamma_attempts,
                'interpretation_limit': '20% applies only to these initial operation counters, not all SQL/storage/RSS/latency/throughput',
            })
        elif arm == 'ram_dedup_off':
            b_ref_att, a_ref_att, d_ref_att, s_ref_att, r_ref_att = _safe_persistence_delta(r, 'warm_replay', 'begin_attempts')
            b_opt_att, a_opt_att, d_opt_att, s_opt_att, r_opt_att = _safe_persistence_delta(o, 'warm_replay', 'begin_attempts')
            b_ref_comm, a_ref_comm, d_ref_comm, s_ref_comm, r_ref_comm = _safe_persistence_delta(r, 'warm_replay', 'confirmed_commits')
            b_opt_comm, a_opt_comm, d_opt_comm, s_opt_comm, r_opt_comm = _safe_persistence_delta(o, 'warm_replay', 'confirmed_commits')

            gamma_attempts = (
                calculate_gamma_red(d_ref_att, d_opt_att, metric_name='warm_begin_attempts')
                if (d_ref_att is not None and d_opt_att is not None)
                else {'available': False, 'reason': r_ref_att or r_opt_att or 'counter_unavailable'}
            )
            gamma_commits = (
                calculate_gamma_red(d_ref_comm, d_opt_comm, metric_name='warm_confirmed_commits')
                if (d_ref_comm is not None and d_opt_comm is not None)
                else {'available': False, 'reason': r_ref_comm or r_opt_comm or 'counter_unavailable'}
            )

            row.update({
                'window': 'warm replay phase',
                'provenance': 'phase_windows.warm_replay admin_before/admin_after persistence deltas (begin_attempts as attempts, confirmed_commits as commits) with matching process_instance; C_reference=ram_dedup_off; C_optimized=optimized; RAM lookup hits separately reported',
                'ablation_warm_attempts': d_ref_att,
                'optimized_warm_attempts': d_opt_att,
                'gamma_warm_attempts': gamma_attempts,
                'ablation_warm_confirmed_commits': d_ref_comm,
                'optimized_warm_confirmed_commits': d_opt_comm,
                'gamma_warm_commits': gamma_commits,
                'ablation_warm_transactions': d_ref_att,
                'optimized_warm_transactions': d_opt_att,
                'gamma_warm_transactions': gamma_attempts,
                'ablation_warm_db_calls_avoided': _e5_num((r.get('mechanisms') or r.get('mechanism_metrics') or {}).get('ram_dedup'), 'database_calls_avoided_total'),
                'optimized_warm_db_calls_avoided': _e5_num((o.get('mechanisms') or o.get('mechanism_metrics') or {}).get('ram_dedup'), 'database_calls_avoided_total'),
                'raw_samples_retained_ablation': _sanitize_database_delta(r.get('phases', {}).get('warm_replay', {}).get('database_delta')).get('raw'),
                'raw_samples_retained_optimized': _sanitize_database_delta(o.get('phases', {}).get('warm_replay', {}).get('database_delta')).get('raw'),
                'interpretation_limit': '100% only warm repository/transaction work reflected by this counter; not all SQL/storage/RSS/latency/throughput',
            })
        else:
            row.update({
                'window': None,
                'provenance': 'no defined matching benefit metric in this report',
                'improvement_percent': None,
                'metric_status': 'UNAVAILABLE',
            })
        if arm == 'fanout_scan':
            def fanout_evidence(report):
                fm = (report.get('mechanisms') or report.get('mechanism_metrics') or {}).get('fanout', {})
                vr = report.get('viewer', {}).get('result', {})
                return {
                    'subscriptions_ready_counter': _e5_num(fm, 'subscriptions_ready'),
                    'viewer_count': _e5_num(vr, 'connected_distinct_viewers'),
                    'viewer_events': _e5_num(vr, 'unique_events'),
                    'duplicates': _e5_num(vr, 'duplicates'),
                    'wrong_namespace': _e5_num(vr, 'wrong_namespace'),
                    'indexed_selections': _e5_num(fm, 'indexed_selections'),
                    'scan_evaluations': _e5_num(fm, 'scan_evaluations'),
                }
            row['raw_activation_evidence'] = {
                'optimized': fanout_evidence(o),
                'fanout_scan': fanout_evidence(r),
                'conservative_helper_result': record['activation'],
            }
        pairs.append(row)

    opt_meta = reports['optimized'].get('metadata', {})
    seed = opt_meta.get('seed')
    if type(seed) is not int or seed < 0:
        raise ValueError('invalid_or_missing_seed_in_metadata')

    if private_context and isinstance(private_context, str) and re.fullmatch(r'[0-9a-f]{64}', private_context):
        ctx_sha = private_context
        ctx_prov = 'explicit_validated_private_context'
        ctx_status = 'AVAILABLE'
    else:
        ctx_sha = None
        ctx_prov = 'unavailable_not_in_raw_schema'
        ctx_status = 'UNAVAILABLE'

    return {
        'schema': 'p8-e5-safe-summary-v1',
        'status': 'PILOT_RUNS_COMPLETE_QUALIFIED',
        'full_p8_accepted': False,
        'source_recorded_by_runs': {
            'revision': run_rows[0]['source_revision'],
            'dirty_sha256': run_rows[0]['source_dirty_sha256'],
            'context_sha256': ctx_sha,
            'context_sha256_status': ctx_status,
            'context_sha256_provenance': ctx_prov,
            'journal_sha256': run_rows[0]['journal_sha256'],
            'workload_sha256': run_rows[0]['workload_sha256'],
            'seed': seed,
        },
        'actual_binary_and_images_comparable': True,
        'run_count': len(ARM_NAMES),
        'independent_repeats': 1,
        'official_repeats_gte_3': False,
        'runs': run_rows,
        'pairs': pairs,
        'qualifications': [
            'Native workloads ran; whole-run happy mocked integration was NOT RUN.',
            'Command-window timeline is INCONCLUSIVE (counter_field_missing_events); command latency is INCONCLUSIVE and no zero latency or false zero sample counts are claimed.',
            'Resource snapshots are sparse and colocated; not capacity evidence.',
            'Fanout raw counters/viewer proof are reported alongside conservative helper result; adapter readiness_verified was absent, so activation stays INCONCLUSIVE.',
            'This newly generated summary code was not the source executed by these frozen runs.',
        ],
    }


def export_e5_summary(
    arm_paths: Dict[str, Union[str, Path]],
    output_dir: Union[str, Path],
    *,
    private_context: Optional[str] = None,
) -> Dict[str, Any]:
    summary = build_e5_summary(arm_paths, private_context=private_context)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'e5-summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    for filename, rows in (('e5-runs.csv', summary['runs']), ('e5-pairs.csv', summary['pairs'])):
        if not rows:
            continue
        fields = []
        seen = set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
        with (out / filename).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else ('' if v is None else v)
                    for k, v in row.items()
                })
    return summary


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='P8.5c mechanism-arm, comparability, and accounting helper.',
    )
    sub = parser.add_subparsers(dest='command')

    p_compare = sub.add_parser('compare', help='Validate comparability between two run reports.')
    p_compare.add_argument('--ref', required=True, help='Path to reference JSON report.')
    p_compare.add_argument('--arm-file', required=True, help='Path to ablation arm JSON report.')
    p_compare.add_argument('--expected-arm', required=True, choices=ARM_NAMES[1:],
                           help='Expected ablation arm name.')

    p_plan = sub.add_parser('plan', help='Generate seeded sequential run-order plan.')
    p_plan.add_argument('--arms', required=True,
                        help='Comma-separated arm names (e.g. optimized,shaper_off).')
    p_plan.add_argument('--repeats', type=int, default=3, help='Number of independent repeats.')
    p_plan.add_argument('--seed', type=int, default=42, help='Master RNG seed.')

    p_e5 = sub.add_parser('e5-summary', help='Export safe per-run and paired summary for six frozen E5 pilots.')
    for arm in ARM_NAMES:
        p_e5.add_argument('--' + arm.replace('_', '-'), required=True, dest='path_' + arm, help=arm + ' frozen report JSON')
    p_e5.add_argument('--output-dir', required=True, help='Summary export directory')
    p_e5.add_argument('--private-context', default=None, help='Validated private context SHA-256')

    p_preflight = sub.add_parser('preflight-arm', help='Show expected flags for an arm.')
    p_preflight.add_argument('--arm', required=True, choices=ARM_NAMES, help='Arm name.')

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)

    if args.command == 'preflight-arm':
        flags = get_expected_arm_flags(args.arm)
        print(json.dumps({'arm': args.arm, 'expected_flags': flags}, indent=2))
        return 0

    if args.command == 'e5-summary':
        try:
            arm_paths = {arm: getattr(args, 'path_' + arm) for arm in ARM_NAMES}
            summary = export_e5_summary(arm_paths, args.output_dir, private_context=getattr(args, 'private_context', None))
            print(json.dumps({'status': summary['status'], 'run_count': summary['run_count'],
                              'output_dir': str(args.output_dir)}, indent=2))
            return 0
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({'status': 'REJECTED', 'error': str(exc)}), file=sys.stderr)
            return 1

    if args.command == 'plan':
        arms_list = [a.strip() for a in args.arms.split(',') if a.strip()]
        try:
            plan = plan_run_order(arms_list, args.repeats, args.seed)
            print(json.dumps(plan, indent=2))
            return 0
        except ValueError as exc:
            print(json.dumps({'error': str(exc)}), file=sys.stderr)
            return 1

    if args.command == 'compare':
        ref_path = Path(args.ref)
        arm_path = Path(args.arm_file)
        if not ref_path.is_file() or not arm_path.is_file():
            print(json.dumps({'error': 'input_files_must_exist'}), file=sys.stderr)
            return 1
        with open(ref_path, encoding='utf-8') as f:
            ref_data = json.load(f)
        with open(arm_path, encoding='utf-8') as f:
            arm_data = json.load(f)

        comp = validate_run_comparability(ref_data, arm_data, args.expected_arm)
        if not comp['comparable']:
            print(json.dumps({'status': 'REJECTED', 'comparability': comp}, indent=2))
            return 1

        act = check_mechanism_activation(args.expected_arm, ref_data, arm_data)
        out = {
            'status': 'COMPARABLE',
            'comparability': comp,
            'mechanism_activation': act,
        }
        print(json.dumps(out, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
