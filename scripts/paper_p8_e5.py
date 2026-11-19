#!/usr/bin/env python3
"""Prepare paired E5 private inputs; native execution is explicit and sequential."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'tests'))
import paper_p8_e5_support as e5
from stage3_uplink_runner import default_specs


def private_root(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    path.chmod(0o700)
    st = path.lstat()
    if path.is_symlink() or st.st_uid != os.getuid() or st.st_mode & 0o777 != 0o700:
        raise ValueError('private_root_rejected')
    return path


def write_private(path, value):
    data = json.dumps(value, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())


def _stop_child_group(proc, grace):
    cleanup = {'result': 'already_exited'}
    def group_exists():
        try:
            os.killpg(proc.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def wait_group_gone():
        deadline = time.monotonic() + grace
        while group_exists() and time.monotonic() < deadline:
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        return not group_exists()

    if proc.poll() is not None and not group_exists():
        return cleanup
    try:
        os.killpg(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        if wait_group_gone():
            cleanup['result'] = 'joined_after_interrupt'
            return cleanup
    except subprocess.TimeoutExpired:
        cleanup['interrupt'] = 'grace_expired'
    except ProcessLookupError:
        pass
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        if wait_group_gone():
            cleanup['result'] = 'joined_after_terminate'
            return cleanup
    except subprocess.TimeoutExpired:
        cleanup['terminate'] = 'grace_expired'
    except ProcessLookupError:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=grace)
        cleanup['result'] = 'joined_after_kill'
    except subprocess.TimeoutExpired:
        cleanup['result'] = 'unresolved'
    return cleanup


def _supervise_child(command, *, cwd, stdout, timeout, cleanup_grace=10):
    proc = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=subprocess.STDOUT,
                            start_new_session=True)
    try:
        return {'returncode': proc.wait(timeout=timeout), 'reason': None,
                'cleanup': {'result': 'normal_exit'}}
    except subprocess.TimeoutExpired:
        return {'returncode': proc.poll(), 'reason': 'timeout',
                'cleanup': _stop_child_group(proc, cleanup_grace)}
    except KeyboardInterrupt:
        return {'returncode': proc.poll(), 'reason': 'interrupted',
                'cleanup': _stop_child_group(proc, cleanup_grace)}


def _private_file(path):
    path = Path(path)
    st = path.lstat()
    if path.is_symlink() or not path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o777 != 0o600:
        raise ValueError('private_context_file_rejected')
    return path


def load_context(path, seed, preset=None):
    context_path = _private_file(path)
    context = json.loads(context_path.read_text())
    if context.get('seed') != seed:
        raise ValueError('context_seed_mismatch')
    dataset_path = _private_file(context['dataset_path'])
    raw = dataset_path.read_bytes()
    if len(raw) != context.get('bytes') or hashlib.sha256(raw).hexdigest() != context.get('sha256'):
        raise ValueError('context_dataset_fingerprint_mismatch')
    selection = context.get('workload_selection')
    records = [json.loads(line) for line in raw.splitlines() if line]
    if (not context.get('anchor') or len(context.get('command_keys', [])) != 4
            or not isinstance(selection, dict)
            or context.get('workload_selection_sha256') != e5.selection_sha256(selection)
            or len(records) != selection['telemetry_messages']
            or any(row.get('sample_count') != selection['samples_per_message'] for row in records)
            or len(context.get('command_keys', [])) != selection['command_count']
            or preset is not None and selection.get('profile') != preset):
        raise ValueError('context_contract_invalid')
    return context_path


def prepare_repeat(root, repeat, seed, preset='pilot'):
    leaf = private_root(root / f'repeat-{repeat:02d}')
    dataset_dir = private_root(leaf / 'shared')
    specs = default_specs()
    sensor_ids = [specs[0].sensor_id, specs[1].sensor_id]
    anchor = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    selection = e5.workload_preset(preset)
    dataset = e5.build_dataset(seed=seed, sensor_ids=sensor_ids, anchor=anchor,
        path=dataset_dir / 'dataset.jsonl', message_count=selection['telemetry_messages'],
        samples_per_message=selection['samples_per_message'])
    keys = e5.command_idempotency_keys(seed)
    subscriptions = [
        {'viewer_id': 'e5-viewer-a', 'user_id': 'user-a', 'gateway_id': 'p8-e5-a', 'sensor_id': sensor_ids[0]},
        {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b'},
        {'viewer_id': 'e5-viewer-b', 'user_id': 'user-b', 'gateway_id': 'p8-e5-b', 'sensor_id': sensor_ids[1]},
    ]
    # Ensure the overlap is a distinct sensor target; default_specs intentionally
    # shares a Sensor ID, so the third target uses the named second Gateway's E5 identity.
    context = {'dataset_path': dataset['path'], 'sha256': dataset['sha256'], 'bytes': dataset['bytes'],
        'seed': seed, 'anchor': anchor, 'gateway_ids': ['p8-e5-a', 'p8-e5-b'],
        'sensor_ids': sensor_ids, 'command_keys': keys, 'viewer_subscriptions': subscriptions,
        'workload_selection': selection,
        'workload_selection_sha256': e5.selection_sha256(selection)}
    context_path = leaf / 'context.json'
    write_private(context_path, context)
    return leaf, context_path


def _validate_reference(path, context_path, seed):
    """Reject unusable external references before creating a run root."""
    artifact_path = _private_file(path)
    report = json.loads(artifact_path.read_text())
    context = json.loads(_private_file(context_path).read_text())
    if (report.get('status') != 'PASS' or report.get('arm') != 'optimized'
            or report.get('cleanup_verified') is not True
            or report.get('full_p8_accepted') is not False):
        raise ValueError('reference_must_be_successful_optimized_cleaned_pilot')
    reference_context = report.get('context')
    if (not isinstance(reference_context, dict)
            or any(reference_context.get(key) != context.get(key) for key in
                ('sha256', 'seed', 'anchor', 'workload_selection_sha256'))
            or context.get('seed') != seed):
        raise ValueError('reference_context_seed_mismatch')
    images = report.get('image_ids') or (report.get('metadata') or {}).get('image_ids')
    if not isinstance(images, dict) or any(not isinstance(images.get(key), str) or not images[key]
                                            for key in ('backend', 'gateway')):
        raise ValueError('reference_image_ids_missing')
    return artifact_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True, help='New private scratch root outside the Git tree')
    parser.add_argument('--mode', choices=('pilot', 'official'), default='pilot')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--seed', type=int, default=805)
    parser.add_argument('--workload-preset', choices=tuple(e5.WORKLOAD_PRESETS), default='pilot')
    parser.add_argument('--context', type=Path, help='Reuse an existing private single-pilot context unchanged')
    parser.add_argument('--reference', type=Path, help='Successful optimized E5 artifact for ablation image reuse')
    parser.add_argument('--arm', choices=e5.ARMS, help='Single-arm debugging pilot')
    parser.add_argument('--arms', nargs='+', choices=e5.ARMS, help='Bounded sequential arm subset')
    parser.add_argument('--child-timeout-seconds', type=int, default=3900,
        help='Finite per-arm lifecycle timeout (default covers 3600s viewer wall plus 300s lifecycle overhead)')
    parser.add_argument('--run', action='store_true', help='Explicitly execute native Docker fixtures')
    parser.add_argument('--prepare-only', action='store_true', help='Prepare context only (default)')
    args = parser.parse_args(argv)
    if args.repeat < 1 or (args.mode == 'pilot' and args.repeat != 1):
        parser.error('pilot mode requires --repeat 1')
    if args.mode == 'official' and args.repeat < 3:
        parser.error('official repeats require --repeat >= 3')
    if args.context and (args.mode != 'pilot' or args.repeat != 1):
        parser.error('--context is restricted to one pilot repeat')
    if args.arm and args.arms:
        parser.error('choose --arm or --arms')
    if args.run and args.prepare_only:
        parser.error('--run and --prepare-only conflict')
    selected = [args.arm] if args.arm else args.arms or []
    if args.reference and (not args.run or not selected or 'optimized' in selected):
        parser.error('--reference is for an --run ablation-only pilot; optimized matrix arms produce their own references')
    if args.run and any(a != 'optimized' for a in selected) and not args.reference and 'optimized' not in selected:
        parser.error('ablation-only runs require --context and matching --reference')
    if args.run and not selected:
        parser.error('--run requires --arm or --arms; all arms are never implicit')
    if not 1 <= args.child_timeout_seconds <= 7200:
        parser.error('--child-timeout-seconds must be 1..7200')
    if args.run and any(a != 'optimized' for a in selected) and 'optimized' not in selected and not args.context:
        parser.error('ablation-only runs require reusable --context matching the reference')
    root = args.root.resolve()
    if ROOT == root or ROOT in root.parents:
        parser.error('scratch root must not be inside the Git tree')
    if args.reference:
        try:
            if not args.context:
                raise ValueError('ablation_only_reference_requires_reusable_context')
            _validate_reference(args.reference, args.context, args.seed)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            parser.error(str(exc))
    private_root(root)
    order = list(selected)
    matrix = 'optimized' in selected and any(a != 'optimized' for a in selected)
    summary = {'status': 'PREPARED', 'mode': args.mode, 'repeat': args.repeat,
        'order': order, 'native_runs': [],
        'timeout_budget': {'child_timeout_seconds': args.child_timeout_seconds,
            'cleanup_overhead_seconds_per_arm': 60,
            'declared_total_seconds': (args.child_timeout_seconds + 60) * len(selected) * args.repeat},
        'execution_policy': {'matrix': 'optimized reference first, then seeded ablation order within each repeat',
            'between_arm_cooldown_seconds': 0, 'cooldown_executed': False,
            'profile': 'bounded selected E5 preset; --mode official schedules independent repeats only',
            'workload_selection': 'the private per-repeat context is authoritative; mode does not change preset size'}}
    for repeat in range(args.repeat):
        repeat_seed = args.seed + repeat
        if matrix:
            ablations = [arm for arm in selected if arm != 'optimized']
            plan = e5.comparison.plan_run_order(ablations, 1, repeat_seed, include_cooldown=False)
            repeat_order = ['optimized'] + [item['arm'] for item in plan['plan']]
            plan['plan'] = [{'order': index, 'arm': arm, 'repeat_index': repeat,
                'run_seed': repeat_seed, 'run_id': f'repeat-{repeat + 1}-{arm}',
                'cooldown_required': False} for index, arm in enumerate(repeat_order)]
            plan['note'] = 'optimized reference first; seeded sequential ablation order; no cooldown executed'
        else:
            plan = e5.comparison.plan_run_order(order, 1, repeat_seed) if order else {'plan': [], 'executed': False}
            repeat_order = [item['arm'] for item in plan['plan']]
        summary.setdefault('plans', []).append(plan)
        if args.context:
            context_path = load_context(args.context, repeat_seed, args.workload_preset)
            repeat_root = private_root(root / f'repeat-{repeat + 1:02d}')
        else:
            repeat_root, context_path = prepare_repeat(root, repeat + 1, repeat_seed, args.workload_preset)
        summary.setdefault('contexts', []).append(str(context_path))
        summary.setdefault('workload_selections', []).append(
            json.loads(context_path.read_text())['workload_selection'])
        if not args.run:
            continue
        for arm in repeat_order:
            arm_root = private_root(repeat_root / ('arm-' + arm))
            log_path = arm_root / 'native.log'
            command = [sys.executable, str(ROOT / 'scripts/tests/native_compose_smoke.py'),
                '--prepare', str(arm_root / 'fixture'), '--paper-p8-e5', arm,
                '--paper-p8-e5-context', str(context_path)]
            reference_path = args.reference
            if matrix and arm != 'optimized':
                reference_path = repeat_root / 'arm-optimized' / 'fixture' / 'artifacts' / 'p8-e5-optimized.json'
            if reference_path and arm != 'optimized':
                command += ['--paper-p8-e5-reference', str(reference_path)]
            with log_path.open('xb') as log:
                os.chmod(log_path, 0o600)
                result = _supervise_child(command, cwd=ROOT, stdout=log,
                    timeout=args.child_timeout_seconds)
                if result['reason'] == 'timeout':
                    run = {'arm': arm, 'repeat': repeat + 1, 'argv': command,
                        'exit_code': result['returncode'], 'log': str(log_path), 'outcome': 'TIMEOUT',
                        'cleanup': result['cleanup']}
                    summary['native_runs'].append(run)
                    summary.update({'status': 'PARTIAL', 'primary_failure': {
                        'reason': 'child_timeout', 'arm': arm, 'repeat': repeat + 1,
                        'timeout_seconds': args.child_timeout_seconds}})
                    _attach_arm_artifact(run, arm_root, arm)
                    run['cleanup_verified'] = run['cleanup_verified'] is True and result['cleanup']['result'] in (
                        'joined_after_interrupt', 'already_exited')
                    write_private(root / 'driver-run.json', summary)
                    print(json.dumps(summary, sort_keys=True))
                    return 124
                if result['reason'] == 'interrupted':
                    run = {'arm': arm, 'repeat': repeat + 1, 'argv': command,
                        'exit_code': result['returncode'], 'log': str(log_path), 'outcome': 'INTERRUPTED',
                        'cleanup': result['cleanup']}
                    summary['native_runs'].append(run)
                    summary.update({'status': 'PARTIAL', 'primary_failure': {
                        'reason': 'interrupted', 'arm': arm, 'repeat': repeat + 1}})
                    _attach_arm_artifact(run, arm_root, arm)
                    run['cleanup_verified'] = run['cleanup_verified'] is True and result['cleanup']['result'] in (
                        'joined_after_interrupt', 'already_exited')
                    write_private(root / 'driver-run.json', summary)
                    print(json.dumps(summary, sort_keys=True))
                    return 130
            summary['native_runs'].append({'arm': arm, 'repeat': repeat + 1,
                'argv': command, 'exit_code': result['returncode'], 'log': str(log_path)})
            artifact = _attach_arm_artifact(summary['native_runs'][-1], arm_root, arm)
            if (result['returncode'] or artifact.get('status') != 'PASS' or
                    artifact.get('cleanup_verified') is not True or artifact.get('full_p8_accepted') is not False):
                summary['status'] = 'FAIL'
                summary['primary_failure'] = {'reason': 'child_failed_or_artifact_unqualified',
                    'arm': arm, 'repeat': repeat + 1, 'exit_code': result['returncode']}
                write_private(root / 'driver-run.json', summary)
                return 1
    if args.run:
        summary['status'] = 'RUNS_COMPLETE'
    write_private(root / 'driver-run.json', summary)
    print(json.dumps(summary, sort_keys=True))
    return 0


def _attach_arm_artifact(run, arm_root, arm):
    artifact_path = arm_root / 'fixture' / 'artifacts' / f'p8-e5-{arm}.json'
    try:
        artifact = json.loads(artifact_path.read_text())
    except (OSError, json.JSONDecodeError):
        artifact = {}
    run.update({'artifact': str(artifact_path), 'artifact_status': artifact.get('status'),
        'cleanup_verified': artifact.get('cleanup_verified'),
        'full_p8_accepted': artifact.get('full_p8_accepted')})
    return artifact


if __name__ == '__main__':
    raise SystemExit(main())
