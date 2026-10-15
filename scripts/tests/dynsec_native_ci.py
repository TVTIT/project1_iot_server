#!/usr/bin/env python3
"""Standard-library native CI supervisor for DynSec service and HTTP test lanes (Slice 4b).

Runs task28_rotate_revoke_native and task28_http_credential_native sequentially in-process
so Python signal handling can initiate runner try/finally teardown. This is best effort,
not a proof of bounded child cancellation or orphan-free forced termination.

Constraints & Safety Guarantees:
- In-process execution: Does NOT launch runners as detached subprocesses.
- Deep validation: Requires literal `True` (is True, not just truthy) for raw success,
  aggregate success, executed, and every cleanup flag in BOTH cleanup and aggregate dictionaries.
  Rejects integer, string booleans, and any contradiction.
- Exact expected statuses: Requires exact aggregate verdict 'SUCCESS' and native status 'PASS'.
  Rejects nonempty failure_code even if success claims True.
- Strict value allowlisting: Sanitizes all emitted dictionary values (status, verdict, failure_code,
  lane) against explicit known enums or fixed safe fallback codes. Secret strings or arbitrary input
  cannot cross into reports or output.
- Atomic report writing: Pre-validates report target before running any lane; rejects existing files,
  symlinks, or dangling symlinks without following them or leaking paths in exceptions. Writes to a
  private 0600 temp file and links atomically via `os.link` (fail if target exists).
- Signal safety: Temporary SIGINT/SIGTERM handlers raise `SupervisorInterruptedError` (inheriting from
  BaseException, NOT Exception) on the FIRST signal only, then ignore subsequent signals during teardown
  so that runner try/finally blocks can complete without interruption.
- Restoration integrity: Any failure or error restoring signal handlers causes fail-closed (returns error).
- Limitation notice: SIGKILL/OOM/runner shutdown cannot guarantee cleanup. A signal during a
  subprocess wait does not establish bounded cancellation of the Docker CLI child.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Ensure scripts/tests directory is on sys.path for sibling imports
_SCRIPTS_TESTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_TESTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_TESTS_DIR)

from task28_rotate_revoke_native import run_native_rotate_revoke_suite
from task28_http_credential_native import run_http_native_suite


VERDICT_SUCCESS = 'SUCCESS'
VERDICT_SERVICE_FAILED = 'SERVICE_LANE_FAILED'
VERDICT_HTTP_FAILED = 'HTTP_LANE_FAILED'
VERDICT_CLEANUP_FAILURE_VETO = 'CLEANUP_FAILURE_VETO'
VERDICT_DRY_RUN_REJECTED = 'DRY_RUN_REJECTED'
VERDICT_MALFORMED = 'MALFORMED_LANE_RESULT'
VERDICT_INTERRUPTED = 'INTERRUPTED'
VERDICT_RESTORATION_ERROR = 'SIGNAL_RESTORATION_ERROR'

# Safe allowed enums for values
KNOWN_VERDICTS = {
    VERDICT_SUCCESS,
    VERDICT_SERVICE_FAILED,
    VERDICT_HTTP_FAILED,
    VERDICT_CLEANUP_FAILURE_VETO,
    VERDICT_DRY_RUN_REJECTED,
    VERDICT_MALFORMED,
    VERDICT_INTERRUPTED,
    VERDICT_RESTORATION_ERROR,
    'SKIPPED',
    'UNKNOWN',
    'NATIVE_TEST_FAILED',
    'DRY_RUN_ONLY',
}

KNOWN_STATUSES = {
    'PASS',
    'FAIL',
    'NOT_RUN',
    'DRY_RUN',
    'SKIP',
    'SKIPPED',
    'UNKNOWN',
}

KNOWN_FAILURE_CODES = {
    'compilation_failed',
    'compilation_timeout',
    'compilation_os_error',
    'image_not_found',
    'image_inspect_failed',
    'image_inspect_timeout',
    'image_inspect_malformed',
    'dry_run_complete',
    'dry_run_or_skip_not_permitted',
    'cleanup_verification_failed',
    'native_not_executed',
    'native_test_failed',
    'invalid_boolean_type',
    'invalid_aggregate_verdict',
    'invalid_native_status',
    'unexpected_failure_code_on_success',
    'malformed_lane_result',
    'interrupted_by_signal',
    'unexpected_base_exception',
    'signal_handler_restoration_failed',
    'sanitized_failure_code',
}

KNOWN_LANES = {'service', 'http'}
NOTICE = ('SIGINT/SIGTERM initiate best-effort in-process teardown; child cancellation '
          'and forced termination (SIGKILL/OOM or runner shutdown) cannot guarantee cleanup.')


def validate_success_report(data: Any) -> None:
    """Reject missing, failed or non-allowlisted evidence before artifact upload."""
    keys = {'success', 'verdict', 'failure_code', 'failure_lane', 'elapsed_seconds', 'lanes', 'notice'}
    if not isinstance(data, dict) or set(data) != keys:
        raise ValueError('Invalid report schema')
    elapsed = data['elapsed_seconds']
    if (data['success'] is not True or data['verdict'] != 'SUCCESS'
            or data['failure_code'] is not None or data['failure_lane'] is not None
            or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0
            or data['notice'] != NOTICE):
        raise ValueError('Report is not validated native success')
    lanes = data['lanes']
    if not isinstance(lanes, dict) or set(lanes) != KNOWN_LANES:
        raise ValueError('Invalid report lanes')
    for name in KNOWN_LANES:
        expected = dict(lane=name, verdict='SUCCESS', status='PASS', executed=True,
                        pg_cleanup_verified=True, broker_cleanup_verified=True,
                        host_cleanup_verified=True, failure_code=None)
        lane = lanes[name]
        if not isinstance(lane, dict) or set(lane) != set(expected):
            raise ValueError('Invalid lane schema')
        if any(type(lane[k]) is not type(v) or lane[k] != v for k, v in expected.items()):
            raise ValueError('Lane did not execute and clean up successfully')


class SupervisorInterruptedError(BaseException):
    """Raised on SIGINT or SIGTERM during supervisor execution.

    Inherits from BaseException (not Exception) so runner generic `except Exception:`
    blocks do not catch or suppress it, allowing runner `finally:` cleanup to unwind cleanly.
    """
    def __init__(self, signum: int) -> None:
        super().__init__(f"Supervisor interrupted by signal {signum}")
        self.signum = signum


class DryRunRejected(Exception):
    """Internal control flow; still restore handlers and write failure evidence."""


# Internal reference to handler for mock checking
_sig_handler = None


def sanitize_enum_value(val: Any, allowed: set[str], fallback: str) -> str:
    """Sanitize arbitrary string into an allowed safe enum or fallback."""
    if isinstance(val, str) and val in allowed:
        return val
    return fallback


def validate_lane_result(raw_result: Any, lane_name: str) -> Tuple[bool, str, Dict[str, Any]]:
    """Validate structured result from a lane runner with strict type checking.

    Returns (is_valid_success, failure_code, safe_lane_summary).
    """
    safe_lane = sanitize_enum_value(lane_name, KNOWN_LANES, 'unknown_lane')

    if not isinstance(raw_result, dict):
        return False, 'malformed_lane_result', {
            'lane': safe_lane,
            'verdict': 'MALFORMED',
            'status': 'UNKNOWN',
            'executed': False,
            'pg_cleanup_verified': False,
            'broker_cleanup_verified': False,
            'host_cleanup_verified': False,
            'failure_code': 'malformed_lane_result',
        }

    agg = raw_result.get('aggregate')
    cleanup = raw_result.get('cleanup')
    native_test = raw_result.get('native_test_result')

    # Both aggregate and cleanup must be present dictionaries
    if not isinstance(agg, dict) or not isinstance(cleanup, dict):
        return False, 'malformed_lane_result', {
            'lane': safe_lane,
            'verdict': 'MALFORMED',
            'status': 'UNKNOWN',
            'executed': False,
            'pg_cleanup_verified': False,
            'broker_cleanup_verified': False,
            'host_cleanup_verified': False,
            'failure_code': 'malformed_lane_result',
        }

    if not isinstance(native_test, dict):
        native_test = {}

    # Strict literal boolean check helper
    def is_literal_bool(v: Any) -> bool:
        return type(v) is bool

    # 1. Check raw success and aggregate success boolean types
    raw_success = raw_result.get('success')
    agg_success = agg.get('success')
    if not is_literal_bool(raw_success) or not is_literal_bool(agg_success):
        return False, 'invalid_boolean_type', {
            'lane': safe_lane,
            'verdict': 'MALFORMED',
            'status': 'UNKNOWN',
            'executed': False,
            'pg_cleanup_verified': False,
            'broker_cleanup_verified': False,
            'host_cleanup_verified': False,
            'failure_code': 'invalid_boolean_type',
        }

    # 2. Check each cleanup flag in BOTH cleanup and aggregate
    for key in ('pg_cleanup_verified', 'broker_cleanup_verified', 'host_cleanup_verified'):
        c_val = cleanup.get(key)
        a_val = agg.get(key)
        if not is_literal_bool(c_val) or not is_literal_bool(a_val):
            return False, 'invalid_boolean_type', {
                'lane': safe_lane,
                'verdict': 'MALFORMED',
                'status': 'UNKNOWN',
                'executed': False,
                'pg_cleanup_verified': False,
                'broker_cleanup_verified': False,
                'host_cleanup_verified': False,
                'failure_code': 'invalid_boolean_type',
            }

    # 3. Check executed boolean type
    executed_val = native_test.get('executed')
    if not is_literal_bool(executed_val):
        executed = False
    else:
        executed = executed_val is True

    # Check cleanup flags match and are literally True
    c_pg = cleanup.get('pg_cleanup_verified') is True
    a_pg = agg.get('pg_cleanup_verified') is True
    c_broker = cleanup.get('broker_cleanup_verified') is True
    a_broker = agg.get('broker_cleanup_verified') is True
    c_host = cleanup.get('host_cleanup_verified') is True
    a_host = agg.get('host_cleanup_verified') is True

    pg_clean = c_pg and a_pg
    broker_clean = c_broker and a_broker
    host_clean = c_host and a_host
    cleanup_ok = (
        (cleanup.get('pg_cleanup_verified') == agg.get('pg_cleanup_verified') == True) and  # noqa: E712
        (cleanup.get('broker_cleanup_verified') == agg.get('broker_cleanup_verified') == True) and  # noqa: E712
        (cleanup.get('host_cleanup_verified') == agg.get('host_cleanup_verified') == True)  # noqa: E712
    )

    raw_status = native_test.get('status')
    raw_agg_status = agg.get('native_status')
    status_str = raw_status if isinstance(raw_status, str) else (raw_agg_status if isinstance(raw_agg_status, str) else 'UNKNOWN')
    safe_status = sanitize_enum_value(status_str, KNOWN_STATUSES, 'UNKNOWN')

    raw_agg_verdict = agg.get('verdict')
    safe_verdict = sanitize_enum_value(raw_agg_verdict if isinstance(raw_agg_verdict, str) else '', KNOWN_VERDICTS, 'UNKNOWN')

    raw_code = raw_result.get('failure_code')
    safe_code = sanitize_enum_value(raw_code if isinstance(raw_code, str) else '', KNOWN_FAILURE_CODES, 'sanitized_failure_code') if raw_code else None

    lane_summary = {
        'lane': safe_lane,
        'verdict': safe_verdict,
        'status': safe_status,
        'executed': executed,
        'pg_cleanup_verified': pg_clean,
        'broker_cleanup_verified': broker_clean,
        'host_cleanup_verified': host_clean,
        'failure_code': safe_code,
    }

    # Dry-run or SKIP check
    if safe_status in ('DRY_RUN', 'SKIP', 'SKIPPED') or raw_code == 'dry_run_complete':
        lane_summary['failure_code'] = 'dry_run_or_skip_not_permitted'
        return False, 'dry_run_or_skip_not_permitted', lane_summary

    # Cleanup verification check
    if not cleanup_ok:
        lane_summary['failure_code'] = 'cleanup_verification_failed'
        return False, 'cleanup_verification_failed', lane_summary

    # If lane is not successful, prefer explicit failure code
    if not (raw_success is True and agg_success is True):
        if safe_code:
            return False, safe_code, lane_summary
        lane_summary['failure_code'] = 'native_test_failed'
        return False, 'native_test_failed', lane_summary

    # Lane claims success: check for unexpected failure_code
    if raw_code:
        lane_summary['failure_code'] = 'unexpected_failure_code_on_success'
        return False, 'unexpected_failure_code_on_success', lane_summary

    # Check exact expected aggregate verdict 'SUCCESS'
    if raw_agg_verdict != 'SUCCESS':
        lane_summary['failure_code'] = 'invalid_aggregate_verdict'
        return False, 'invalid_aggregate_verdict', lane_summary

    # Check exact expected native status 'PASS'
    if raw_status != 'PASS' or raw_agg_status != 'PASS':
        lane_summary['failure_code'] = 'invalid_native_status'
        return False, 'invalid_native_status', lane_summary

    # Check executed is literally True
    if executed is not True:
        lane_summary['failure_code'] = 'native_not_executed'
        return False, 'native_not_executed', lane_summary

    return True, '', lane_summary


def build_safe_summary(
    service_result: Optional[Dict[str, Any]],
    http_result: Optional[Dict[str, Any]],
    supervisor_success: bool,
    supervisor_verdict: str,
    failure_code: Optional[str],
    failure_lane: Optional[str],
    elapsed_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Construct an explicitly allowlisted safe summary dictionary."""
    safe_verdict = sanitize_enum_value(supervisor_verdict, KNOWN_VERDICTS, VERDICT_MALFORMED)
    safe_failure_code = sanitize_enum_value(failure_code, KNOWN_FAILURE_CODES, 'sanitized_failure_code') if failure_code else None
    safe_failure_lane = sanitize_enum_value(failure_lane, KNOWN_LANES, 'unknown_lane') if failure_lane else None

    _, _, service_summary = validate_lane_result(service_result, 'service') if service_result else (False, '', {
        'lane': 'service',
        'verdict': 'SKIPPED',
        'status': 'NOT_RUN',
        'executed': False,
        'pg_cleanup_verified': False,
        'broker_cleanup_verified': False,
        'host_cleanup_verified': False,
        'failure_code': None,
    })

    _, _, http_summary = validate_lane_result(http_result, 'http') if http_result else (False, '', {
        'lane': 'http',
        'verdict': 'SKIPPED',
        'status': 'NOT_RUN',
        'executed': False,
        'pg_cleanup_verified': False,
        'broker_cleanup_verified': False,
        'host_cleanup_verified': False,
        'failure_code': None,
    })

    return {
        'success': bool(supervisor_success is True),
        'verdict': safe_verdict,
        'failure_code': safe_failure_code,
        'failure_lane': safe_failure_lane,
        'elapsed_seconds': round(elapsed_seconds, 3),
        'lanes': {
            'service': service_summary,
            'http': http_summary,
        },
        'notice': NOTICE,
    }


def pre_validate_report_path(report_path: Path) -> Path:
    """Pre-validate target report path before allocating resources or running lanes.

    Rejects pre-existing files, symlinks, dangling symlinks, or directories.
    Does not leak raw file paths in exception messages.
    """
    target = report_path.expanduser()

    # Check for symlink existence or dangling symlink without dereferencing
    if target.is_symlink():
        raise FileExistsError("Report target is a symlink, rejected for security")

    # Check if target exists on filesystem
    if target.exists():
        raise FileExistsError("Report target already exists, overwrite rejected")

    # Ensure parent directory exists or can be created
    parent = target.parent
    if parent.is_symlink():
        raise FileExistsError("Report target parent directory is a symlink, rejected")

    try:
        parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        raise OSError("Failed to prepare report destination directory")

    return target


def write_safe_report(report_path: Path, safe_data: Dict[str, Any]) -> None:
    """Atomically write safe report JSON using private 0600 temp file and os.link.

    Fails without overwriting if the target already exists or is created concurrently.
    Does not leak raw file paths in exception messages.
    """
    target = pre_validate_report_path(report_path)
    serialized = json.dumps(safe_data, indent=2) + '\n'

    parent = target.parent
    # Create private temp file with 0600 permissions
    fd, tmp_str = tempfile.mkstemp(prefix='.report_tmp_', dir=parent)
    tmp_path = Path(tmp_str)
    try:
        try:
            with open(fd, 'w', encoding='utf-8') as f:
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            raise OSError("Failed to write temporary report file")

        # Atomic link: os.link creates a hard link and fails with FileExistsError
        # if the destination file already exists (preventing TOCTOU overwrite).
        try:
            os.link(tmp_path, target)
        except FileExistsError:
            raise FileExistsError("Report target already exists, overwrite rejected")
        except Exception:
            raise OSError("Failed to link atomic report file")
    finally:
        # Clean up temporary link source
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                raise OSError("Failed to remove temporary report file")


def run_dynsec_native_ci(
    report_path: Optional[Path] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Run DynSec native CI lanes in process with signal trapping and safe reporting."""
    global _sig_handler

    # Step 0: Pre-validate report path before allocating any resource
    if report_path is not None:
        pre_validate_report_path(report_path)

    start_time = time.monotonic()

    service_result: Optional[Dict[str, Any]] = None
    http_result: Optional[Dict[str, Any]] = None

    supervisor_success = False
    supervisor_verdict = VERDICT_MALFORMED
    failure_code: Optional[str] = None
    failure_lane: Optional[str] = None

    previous_handlers: Dict[int, Any] = {}
    signal_received = False

    def handler(signum: int, frame: Any) -> None:
        nonlocal signal_received
        if not signal_received:
            signal_received = True
            # For subsequent signals during teardown, ignore them so cleanup completes
            for s in (signal.SIGINT, signal.SIGTERM):
                try:
                    signal.signal(s, signal.SIG_IGN)
                except Exception:
                    pass
            raise SupervisorInterruptedError(signum)

    _sig_handler = handler

    restoration_error = False

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.signal(sig, handler)

        if dry_run:
            supervisor_success = False
            supervisor_verdict = VERDICT_DRY_RUN_REJECTED
            failure_code = 'dry_run_or_skip_not_permitted'
            raise DryRunRejected()

        # Step 1: Execute Service native suite
        service_result = run_native_rotate_revoke_suite(dry_run=False)
        svc_ok, svc_code, svc_summary = validate_lane_result(service_result, 'service')
        if not svc_ok:
            supervisor_success = False
            failure_lane = 'service'
            failure_code = svc_code
            if svc_code == 'cleanup_verification_failed':
                supervisor_verdict = VERDICT_CLEANUP_FAILURE_VETO
            elif svc_code == 'dry_run_or_skip_not_permitted':
                supervisor_verdict = VERDICT_DRY_RUN_REJECTED
            else:
                supervisor_verdict = VERDICT_SERVICE_FAILED
        else:
            # Step 2: Execute HTTP native suite only if service lane succeeded
            http_result = run_http_native_suite(dry_run=False)
            http_ok, http_code, http_summary = validate_lane_result(http_result, 'http')
            if not http_ok:
                supervisor_success = False
                failure_lane = 'http'
                failure_code = http_code
                if http_code == 'cleanup_verification_failed':
                    supervisor_verdict = VERDICT_CLEANUP_FAILURE_VETO
                elif http_code == 'dry_run_or_skip_not_permitted':
                    supervisor_verdict = VERDICT_DRY_RUN_REJECTED
                else:
                    supervisor_verdict = VERDICT_HTTP_FAILED
            else:
                supervisor_success = True
                supervisor_verdict = VERDICT_SUCCESS
                failure_code = None
                failure_lane = None

    except DryRunRejected:
        pass
    except SupervisorInterruptedError:
        supervisor_success = False
        supervisor_verdict = VERDICT_INTERRUPTED
        failure_code = 'interrupted_by_signal'
    except BaseException:
        supervisor_success = False
        supervisor_verdict = VERDICT_MALFORMED
        failure_code = 'unexpected_base_exception'
    finally:
        # Always restore prior signal handlers; failure to restore must fail closed
        for sig, old_handler in previous_handlers.items():
            try:
                signal.signal(sig, old_handler)
            except Exception:
                restoration_error = True

        if restoration_error:
            supervisor_success = False
            supervisor_verdict = VERDICT_RESTORATION_ERROR
            failure_code = 'signal_handler_restoration_failed'

    elapsed = time.monotonic() - start_time

    safe_summary = build_safe_summary(
        service_result=service_result,
        http_result=http_result,
        supervisor_success=supervisor_success,
        supervisor_verdict=supervisor_verdict,
        failure_code=failure_code,
        failure_lane=failure_lane,
        elapsed_seconds=elapsed,
    )

    if report_path:
        write_safe_report(report_path, safe_summary)

    return safe_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="DynSec native CI in-process supervisor (Slice 4b)")
    parser.add_argument('--report', type=str, default=None, help="Path to write safe report JSON")
    parser.add_argument('--dry-run', action='store_true', help="Perform dry run check (fails CI by design)")
    parser.add_argument('--validate-report', type=Path, help='Validate exact safe SUCCESS artifact without running lanes')
    args = parser.parse_args()

    report_p = Path(args.report) if args.report else None
    try:
        if args.validate_report is not None:
            if args.report or args.dry_run or args.validate_report.is_symlink():
                raise ValueError('Invalid validation mode')
            validate_success_report(json.loads(args.validate_report.read_text(encoding='utf-8')))
            print('DYNSEC_NATIVE_CI_VERDICT: VALIDATED_SUCCESS')
            return
        result = run_dynsec_native_ci(report_path=report_p, dry_run=args.dry_run)
        verdict = result.get('verdict', 'UNKNOWN')
        # Print concise verdict without sensitive details, spelling DYNSEC correctly
        print(f"DYNSEC_NATIVE_CI_VERDICT: {verdict}")
        if not result.get('success', False):
            sys.exit(1)
        sys.exit(0)
    except Exception:
        print("DYNSEC_NATIVE_CI_VERDICT: SUPERVISOR_ERROR")
        sys.exit(1)


if __name__ == '__main__':
    main()
