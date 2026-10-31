#!/usr/bin/env python3
"""Foreground local-file operator interface for bounded P8 E1/E2 fixtures."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'tests'))
import paper_p8_remote as remote
import paper_p8_supervisor as supervisor
import paper_p8_e5_import as e5_import


def _private_root(path, create=False):
    path = Path(path).absolute()
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError('server_path_symlink_rejected')
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
    stat = path.lstat()
    if not path.is_dir() or stat.st_uid != os.getuid() or stat.st_mode & 0o077:
        raise ValueError('server_root_rejected')
    return path


def _state_root(root):
    root = _private_root(root)
    fixture = root / 'fixture'
    private = fixture / 'private'
    return private


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    serve = sub.add_parser('serve', help='start an owned P8 fixture and hold until operator finalizes')
    serve.add_argument('--root', type=Path, required=True)
    serve.add_argument('--network-config', type=Path, required=True)
    serve.add_argument('--deadline-seconds', type=int, default=900)
    serve.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    serve.add_argument('--arm', choices=('optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan'))
    serve.add_argument('--context', type=Path)
    serve.add_argument('--reference', type=Path)
    serve.add_argument('--samples-per-message', choices=('100', '1'))
    serve.add_argument('--dispatch-mode', choices=('polling', 'wakeup'))
    serve.add_argument('--workload', choices=('low_load', 'mixed'))
    serve.add_argument('--cache-mode', choices=('on', 'off'), default='on')
    serve.add_argument('--cold-replay', action='store_true',
                       help='include the optional cache-on fresh-backend E4 replay phase')
    serve.add_argument('--durable-reopen', action='store_true',
                       help='include the optional sequential durable Gateway SQLite fault/reopen phases')
    serve.add_argument('--conflicts', action='store_true',
                       help='include the optional E4 representative conflict rejection phase')
    status = sub.add_parser('status')
    status.add_argument('--root', type=Path, required=True)
    status.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    for name in ('begin', 'finish-phase', 'next-phase', 'finalize', 'abort'):
        command = sub.add_parser(name)
        command.add_argument('--root', type=Path, required=True)
        command.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
        command.add_argument('--session-id', required=True)
        command.add_argument('--phase', required=True)
        if name in ('begin', 'finish-phase', 'next-phase'):
            command.add_argument('--phase-id', required=True)
        if name == 'finish-phase':
            command.add_argument('--results', type=Path, required=True)
    cleanup = sub.add_parser('cleanup', help='receipt-verified stale fixture cleanup')
    cleanup.add_argument('--root', type=Path, required=True)
    cleanup.add_argument('--project', required=True)
    cleanup.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    args = parser.parse_args(argv)
    if getattr(args, 'experiment', 'E1') not in ('E1', 'E2', 'E3', 'E4', 'E5'):
        parser.error('unsupported experiment')
    if args.command == 'serve':
        root = _private_root(args.root, create=True)
        if not 60 <= args.deadline_seconds <= 3600:
            raise ValueError('P8 deadline must be 60..3600 seconds')
        if args.experiment == 'E5':
            if args.arm is None:
                raise ValueError('E5 requires explicit --arm')
            if args.context is None:
                raise ValueError('E5 requires --context')
            if args.arm == 'optimized' and args.reference is not None:
                raise ValueError('E5 optimized arm does not accept --reference')
            if args.arm != 'optimized' and args.reference is None:
                raise ValueError('E5 ablation arms require --reference')
            if (args.samples_per_message is not None or args.dispatch_mode is not None
                    or args.workload is not None or args.cache_mode != 'on'
                    or args.cold_replay or args.durable_reopen or args.conflicts):
                raise ValueError('E1-E4 options are rejected for E5')
        if args.experiment != 'E5' and (args.arm is not None or args.context is not None or args.reference is not None):
            raise ValueError('E5 options require --experiment E5')
        if (args.experiment == 'E2') != (args.samples_per_message is not None):
            raise ValueError('E2 requires one explicit --samples-per-message profile')
        if args.experiment == 'E3' and (args.dispatch_mode is None or args.workload is None):
            raise ValueError('E3 requires explicit --dispatch-mode and --workload')
        if args.experiment != 'E3' and (args.dispatch_mode is not None or args.workload is not None):
            raise ValueError('E3 profile options require --experiment E3')
        if args.experiment != 'E4' and args.experiment != 'E5' and args.cache_mode != 'on':
            raise ValueError('--cache-mode requires --experiment E4')
        if args.experiment == 'E4' and args.samples_per_message is not None:
            raise ValueError('E4 does not accept --samples-per-message')
        if args.cold_replay and (args.experiment != 'E4' or args.cache_mode != 'on'):
            raise ValueError('--cold-replay requires --experiment E4 --cache-mode on')
        if args.durable_reopen and args.experiment != 'E4':
            raise ValueError('--durable-reopen requires --experiment E4')
        if args.conflicts and args.experiment != 'E4':
            raise ValueError('--conflicts requires --experiment E4')
        profile = remote.load_config(args.network_config)
        remote.preflight_bind(profile)
        log = root / 'server.log'
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        hold_flag = {'E1':'--p8-e1-prepare-hold','E2':'--p8-e2-prepare-hold',
                     'E3':'--p8-e3-prepare-hold','E4':'--p8-e4-prepare-hold',
                     'E5':'--p8-e5-prepare-hold'}[args.experiment]
        bundle_flag = {'E1':'--p8-e1-bundle-root','E2':'--p8-e2-bundle-root',
                        'E3':'--p8-e3-bundle-root','E4':'--p8-e4-bundle-root',
                        'E5':'--p8-e5-bundle-root'}[args.experiment]
        arm = (args.arm if args.experiment == 'E5' else
               args.dispatch_mode if args.experiment == 'E3' else
               'e1-pilot' if args.experiment == 'E1' else
               'warm' if args.experiment == 'E4' else 'E2-pilot')
        e5_seed = e5_import.read_context_seed(args.context) if args.experiment == 'E5' else None
        command = [sys.executable, str(ROOT / 'scripts/tests/native_compose_smoke.py'),
                   '--prepare', str(root / 'fixture'), '--p8-lifecycle-hold',
                   hold_flag, bundle_flag, str(root / 'client-bundle'),
                   '--p8-experiment', args.experiment,
                    '--p8-arm', arm,
                    '--p8-seed', (str(e5_seed) if e5_seed is not None else
                                  '86' if args.experiment == 'E1'
                                  else '89' if args.experiment in ('E3', 'E4') else '84'),
                   '--p8-deadline-seconds', str(args.deadline_seconds),
                   '--p8-remote-config', str(args.network_config)]
        if args.experiment == 'E2':
            command += ['--p8-e2-samples-per-message', args.samples_per_message]
        if args.experiment == 'E3':
            command += ['--p8-e3-workload', args.workload]
        if args.experiment == 'E4':
            command += ['--p8-e4-cache-mode', args.cache_mode]
            if args.cold_replay:
                command += ['--p8-e4-cold-replay']
            if args.durable_reopen:
                command += ['--p8-e4-durable-reopen']
            if args.conflicts:
                command += ['--p8-e4-conflicts']
        if args.experiment == 'E5':
            command += ['--p8-e5-context', str(args.context)]
            if args.reference is not None:
                command += ['--p8-e5-reference', str(args.reference)]
        with os.fdopen(fd, 'wb') as output:
            proc = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
                                    close_fds=True, start_new_session=False)
            try:
                code = proc.wait()
            except KeyboardInterrupt:
                proc.send_signal(signal.SIGINT)
                code = proc.wait(timeout=120)
        print(json.dumps({'server_exit_code': code, 'log': str(log),
                          'state': str(_state_root(root) / 'session.json')}))
        return code
    if args.command == 'status':
        state = supervisor.read_state(_state_root(args.root))
        print(json.dumps({key: state.get(key) for key in (
            'session_id', 'experiment', 'arm', 'phase', 'preparation_status',
            'deadline_at', 'endpoint_metadata', 'artifacts', 'workload_outcome',
            'cleanup_outcome', 'project', 'phase_id', 'collector_ready',
            'imported_result', 'phase_history', 'cold_replay_enabled',
            'e4_phase_plan', 'e5_phase_plan', 'durable_verified',
            'replacement_proof', 'last_request_error') if key in state}, sort_keys=True))
        return 0
    if args.command == 'cleanup':
        state = supervisor.read_state(_state_root(args.root))
        print(json.dumps(supervisor.recover_cleanup(_state_root(args.root), project=args.project), sort_keys=True))
        return 0
    state_root = _state_root(args.root)
    state = supervisor.read_state(state_root)
    if state.get('experiment') == 'E5' and args.command in supervisor.E5_PHASE_COMMANDS:
        error = supervisor.e5_phase_transition_error(state, args.command, args.phase_id)
        if error:
            raise ValueError(error)
    if args.command == 'next-phase' and state.get('experiment') not in ('E4', 'E5'):
        raise ValueError('next_phase_is_e4_only')
    if state.get('experiment') == 'E4' and args.command in ('begin', 'finish-phase', 'next-phase'):
        if args.command == 'begin' and (state.get('phase') != 'READY'
                or args.phase_id not in state.get('e4_phase_plan', [])):
            raise ValueError('e4_begin_phase_transition_invalid')
        if args.command == 'finish-phase' and (state.get('phase') != 'MEASURING'
                or args.phase_id != state.get('phase_id')):
            raise ValueError('e4_finish_phase_transition_invalid')
        if args.command == 'next-phase' and (state.get('phase') != 'VERIFYING'
                or args.phase_id not in ('replay', 'cold-replay','durable-fault','durable-reopen','conflicts')
                or state.get('workload_outcome') != 'VALIDATED'
                or not isinstance(state.get('imported_result'), dict)
                or state['imported_result'].get('validation_valid') is not True
                or args.phase_id == 'replay' and (state.get('phase_id') != 'unique'
                    or state.get('phase_history') != ['unique'])
                or args.phase_id == 'cold-replay' and (state.get('phase_id') != 'replay'
                    or state.get('cold_replay_enabled') is not True
                    or state.get('phase_history') != ['unique', 'replay'])
                or not supervisor._e4_next_phase_matches(state, args.phase_id)):
            raise ValueError('e4_next_phase_requires_validated_unique')
    if state.get('session_id') != args.session_id or state.get('phase') != args.phase:
        raise ValueError('request_session_or_phase_mismatch')
    if args.command == 'begin' and state.get('experiment') != 'E4' and (state.get('phase') != 'READY'
            or state.get('preparation_status') != f"{state.get('experiment')}_PREPARED"):
        raise ValueError('request_begin_transition_invalid')
    if args.command == 'finish-phase' and state.get('experiment') != 'E4' and (state.get('phase') != 'MEASURING'
            or state.get('phase_id') != args.phase_id):
        raise ValueError('request_finish_transition_invalid')
    if args.command in ('finalize', 'abort') and state.get('phase') not in ('READY', 'MEASURING', 'VERIFYING'):
        raise ValueError('request_finalization_transition_invalid')
    request = {'session_id': args.session_id, 'phase': args.phase, 'command': args.command}
    if args.command in ('begin', 'finish-phase', 'next-phase'):
        request['phase_id'] = args.phase_id
    if args.command == 'finish-phase':
        request['results'] = str(args.results.absolute())
    supervisor.write_request(state_root, request)
    print(json.dumps({'request_published': True, 'command': args.command,
                      'acknowledgement': 'check status for phase transition and collector/import outcome'}))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        print(f'P8 server error: {exc}', file=sys.stderr)
        raise SystemExit(2)
