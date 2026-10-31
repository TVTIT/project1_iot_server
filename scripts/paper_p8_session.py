#!/usr/bin/env python3
"""Local-only controls for the P8 lifecycle hold; not an E1 load client."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'tests'))
import paper_p8_supervisor as supervisor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    status = sub.add_parser('status')
    status.add_argument('--root', type=Path, required=True)
    request = sub.add_parser('request')
    request.add_argument('--root', type=Path, required=True)
    request.add_argument('--command', dest='operation', choices=('begin', 'finish-phase', 'finalize', 'abort'), required=True)
    request.add_argument('--session-id', required=True)
    request.add_argument('--phase', choices=('READY', 'MEASURING', 'VERIFYING'), required=True)
    request.add_argument('--phase-id')
    request.add_argument('--results', type=Path)
    recovery = sub.add_parser('cleanup')
    recovery.add_argument('--root', type=Path, required=True)
    recovery.add_argument('--project', required=True)
    args = parser.parse_args()
    if args.action == 'status':
        result = supervisor.read_state(args.root)
    elif args.action == 'request':
        state = supervisor.read_state(args.root)
        if state.get('phase') != args.phase or state.get('session_id') != args.session_id:
            raise ValueError('request_session_or_phase_mismatch')
        request = {'session_id': args.session_id, 'command': args.operation, 'phase': args.phase}
        if args.operation in ('begin', 'finish-phase'):
            if not args.phase_id:
                raise ValueError('request_phase_id_required')
            request['phase_id'] = args.phase_id
        if args.operation == 'finish-phase':
            if args.results is None:
                raise ValueError('request_results_required')
            request['results'] = str(args.results.absolute())
        supervisor.write_request(args.root, request)
        result = {'request_published': True}
    else:
        result = supervisor.recover_cleanup(args.root, project=args.project)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
