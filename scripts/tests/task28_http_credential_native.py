#!/usr/bin/env python3
"""A8c HTTP native runner for ONE Go testcase (TestSimplifiedHTTPNativeRotateRevoke).

Offline source/unit testable runner with strict boundaries:
- Finite timeouts, private root under /tmp/opencode
- Precheck local pinned images (no pull/latest)
- Compile single cmd/server test binary with CGO_ENABLED=0 GOOS=linux GOARCH=amd64
- Apply ONLY ordered 000000 roles.sh and 000001..000019 .up.sql on own fixture
- Network attach own PG to broker network with alias task28a-pg
- Initialize backend in DynSecProbeFixture (initialize_backend=True)
- Transfer binary as 1883:1883 to /security/task28a-http-native.test with verified ownership
- Execute native HTTP test ONCE via docker exec with stdin JSON only (no argv secrets)
- Safe sequential teardown: OWN PG first, then broker fixture, then verified host root
"""
from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

_SCRIPTS_TESTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_TESTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_TESTS_DIR)

from task28_disable_client_probe import (
    DynSecProbeFixture,
    create_verified_host_fixture_dir,
    verified_remove_host_fixture_dir,
    inspect_owned_resource,
)
import stage2_mosquitto_runtime as runtime
from task28_simplified_schema_runner import (
    DatabaseFixture,
    PG_IMAGE,
)

ROOT = Path(__file__).resolve().parents[2]
MOSQUITTO_IMAGE = runtime.IMAGE

ENV = {k: os.environ[k] for k in ('PATH', 'HOME', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'XDG_RUNTIME_DIR') if k in os.environ}

SAFE_RUN_MARKER_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{8,64}$')
SAFE_PROJECT_BINDING_PATTERN = re.compile(r'^dynsec_probe_[a-f0-9]{12}$')


def inspect_local_image(image_ref: str, timeout: float = 10.0) -> tuple[bool, dict | None, str | None]:
    """Inspect local image without pulling."""
    try:
        proc = subprocess.run(
            ['docker', 'image', 'inspect', image_ref],
            env=ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if proc.returncode != 0:
            err = proc.stderr.decode('utf-8', errors='replace').lower()
            if 'no such image' in err or 'not found' in err:
                return False, None, 'image_not_found'
            return False, None, 'image_inspect_failed'

        raw = json.loads(proc.stdout.decode('utf-8'))
        if not raw or not isinstance(raw, list):
            return False, None, 'image_inspect_malformed'

        config_vols = raw[0].get('Config', {}).get('Volumes')
        return True, config_vols, None
    except subprocess.TimeoutExpired:
        return False, None, 'image_inspect_timeout'
    except Exception as e:
        return False, None, f'image_inspect_error: {e}'


def get_ordered_migrations(migrations_dir: Path) -> list[Path]:
    """Select ONLY 000000_configure_supabase_roles.sh and 000001..000019 .up.sql."""
    ordered = []
    for p in sorted(migrations_dir.iterdir()):
        name = p.name
        if not name[:6].isdigit():
            continue
        idx = int(name[:6])
        if idx > 19:
            continue
        if idx == 0 and name.endswith('.sh'):
            ordered.append(p)
        elif 1 <= idx <= 19 and name.endswith('.up.sql'):
            ordered.append(p)
    return ordered


def build_http_native_test_input(
    pg_alias: str,
    pg_password: str,
    manager_username: str,
    manager_password: str,
    backend_password: str,
    run_marker: str,
    project_binding: str,
) -> dict[str, Any]:
    """Build exact JSON input payload matching credential_http_native_test.go contract."""
    dsn = f"postgres://fixture_admin:{pg_password}@{pg_alias}:5432/fixture_service?sslmode=disable"
    return {
        "test_pg_dsn": dsn,
        "manager_username": manager_username,
        "manager_password": manager_password,
        "backend_password": backend_password,
        "broker_url": "ssl://localhost:8883",
        "ca_path": "/security/ca.crt",
        "store_path": "/security/dynsec.json",
        "writer_uid": 1883,
        "run_marker": run_marker,
        "project_binding": project_binding,
    }


def build_http_native_transfer_cmd(broker_container_id: str) -> list[str]:
    """Build docker exec command to stream HTTP native Go test binary into broker /security volume."""
    return [
        'docker', 'exec', '-i', '-u1883:1883',
        broker_container_id,
        'sh', '-c', 'cat > /security/task28a-http-native.test && chmod 0700 /security/task28a-http-native.test'
    ]


def classify_transfer_failure(returncode: int, stderr: bytes = b'') -> str:
    """Classify transfer failure using numeric return code and sanitized whitelist."""
    err_lower = stderr.decode('utf-8', errors='replace').lower()
    if 'permission denied' in err_lower:
        diag = 'transfer_permission_denied'
    elif 'read-only' in err_lower or 'readonly' in err_lower:
        diag = 'transfer_readonly_filesystem'
    elif 'not found' in err_lower or 'no such file' in err_lower:
        diag = 'transfer_missing_path'
    else:
        diag = 'transfer_failed'

    return f"failed_to_transfer_binary_rc_{returncode}_{diag}"


def transfer_binary_to_broker(broker_container_id: str, binary_path: Path) -> tuple[bool, str | None]:
    """Stream binary file to broker /security volume via docker exec stdin with finite timeout."""
    cmd = build_http_native_transfer_cmd(broker_container_id)
    try:
        with open(binary_path, 'rb') as f:
            proc = subprocess.run(
                cmd,
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=ENV,
                timeout=30,
            )
        if proc.returncode != 0:
            diag = classify_transfer_failure(proc.returncode, proc.stderr)
            return False, diag
        return True, None
    except subprocess.TimeoutExpired:
        return False, 'transfer_timeout'
    except Exception:
        return False, 'transfer_exception'


def build_http_native_exec_cmd(broker_container_id: str) -> list[str]:
    """Build docker exec command to execute HTTP native Go test in already tracked broker."""
    return [
        'docker', 'exec', '-i', '-u1883:1883',
        '-e', 'TASK28_HTTP_NATIVE_ISOLATED=1',
        broker_container_id,
        '/security/task28a-http-native.test',
        '-test.run', '^TestSimplifiedHTTPNativeRotateRevoke$',
        '-test.v',
        '-test.timeout', '90s',
    ]


def classify_http_native_test_output(returncode: int, stdout: bytes, stderr: bytes = b'') -> dict[str, Any]:
    """Classify Go test run output in memory without exposing raw stderr."""
    combined = stdout + stderr
    has_run = b'=== RUN   TestSimplifiedHTTPNativeRotateRevoke' in combined

    if not has_run:
        err_lower = stderr.decode('utf-8', errors='replace').lower()
        if 'permission denied' in err_lower:
            diag = 'launch_permission_denied'
        elif 'exec format error' in err_lower:
            diag = 'launch_exec_format_error'
        elif 'not found' in err_lower or 'no such file' in err_lower:
            diag = 'launch_not_found'
        else:
            diag = 'launch_failed'

        summary = diag if returncode in (126, 127) else 'launch_failed'
        return {
            'status': 'FAIL',
            'executed': False,
            'returncode': returncode,
            'summary': summary,
            'launch_error': diag,
        }

    if b'--- SKIP: TestSimplifiedHTTPNativeRotateRevoke' in combined:
        return {'status': 'SKIP', 'executed': False, 'returncode': returncode, 'summary': 'test_skipped'}

    if returncode == 0 and b'--- PASS: TestSimplifiedHTTPNativeRotateRevoke' in combined:
        return {'status': 'PASS', 'executed': True, 'returncode': returncode, 'summary': 'test_passed'}

    return {'status': 'FAIL', 'executed': True, 'returncode': returncode, 'summary': 'test_failed'}


def evaluate_aggregate_result(
    native_status: str,
    pg_cleanup_verified: bool,
    broker_cleanup_verified: bool,
    host_cleanup_verified: bool,
) -> dict[str, Any]:
    """Evaluate aggregate verdict with cleanup veto."""
    cleanup_ok = pg_cleanup_verified and broker_cleanup_verified and host_cleanup_verified
    if not cleanup_ok:
        return {
            'success': False,
            'verdict': 'CLEANUP_FAILURE_VETO',
            'native_status': native_status,
            'pg_cleanup_verified': pg_cleanup_verified,
            'broker_cleanup_verified': broker_cleanup_verified,
            'host_cleanup_verified': host_cleanup_verified,
        }
    if native_status == 'PASS':
        return {
            'success': True,
            'verdict': 'SUCCESS',
            'native_status': native_status,
            'pg_cleanup_verified': True,
            'broker_cleanup_verified': True,
            'host_cleanup_verified': True,
        }
    return {
        'success': False,
        'verdict': f'NATIVE_{native_status}',
        'native_status': native_status,
        'pg_cleanup_verified': True,
        'broker_cleanup_verified': True,
        'host_cleanup_verified': True,
    }


def execute_safe_teardown(
    pg_fixture: DatabaseFixture | None,
    broker_fixture: DynSecProbeFixture | None,
    host_receipt: dict | None,
    host_cleaner: Callable[[dict | None], dict] = verified_remove_host_fixture_dir,
) -> dict[str, Any]:
    """Execute teardown in safe order: own PG first, then broker fixture, then host root."""
    results: dict[str, Any] = {
        'pg_cleanup_verified': False,
        'broker_cleanup_verified': False,
        'host_cleanup_verified': False,
    }

    if pg_fixture is not None:
        try:
            pg_res = pg_fixture.cleanup()
            results['pg_cleanup_verified'] = (pg_res is True)
        except Exception:
            results['pg_cleanup_verified'] = False
            results['pg_cleanup_error'] = 'pg_cleanup_failed'

    if broker_fixture is not None:
        try:
            b_res = broker_fixture.cleanup()
            results['broker_cleanup_verified'] = (isinstance(b_res, dict) and b_res.get('clean') is True)
        except Exception:
            results['broker_cleanup_verified'] = False
            results['broker_cleanup_error'] = 'broker_cleanup_failed'

    if host_receipt is not None:
        try:
            h_res = host_cleaner(host_receipt)
            results['host_cleanup_verified'] = (isinstance(h_res, dict) and h_res.get('verified') is True)
        except Exception:
            results['host_cleanup_verified'] = False
            results['host_cleanup_error'] = 'host_cleanup_failed'

    return results


def compile_http_test_binary(out_path: Path, timeout: float = 120.0) -> tuple[bool, str | None]:
    """Compile single Go package test binary with CGO_ENABLED=0 GOOS=linux GOARCH=amd64."""
    cmd = [
        'go', 'test',
        '-c',
        '-o', str(out_path),
        './cmd/server',
    ]
    compile_env = dict(os.environ)
    compile_env.update({
        'CGO_ENABLED': '0',
        'GOOS': 'linux',
        'GOARCH': 'amd64',
    })
    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT / 'src',
            env=compile_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if proc.returncode != 0:
            return False, "compilation_failed"
        return True, None
    except subprocess.TimeoutExpired:
        return False, "compilation_timeout"
    except Exception:
        return False, "compilation_os_error"


def run_http_native_suite(dry_run: bool = False) -> dict[str, Any]:
    """Orchestration of A8c HTTP native test runner."""
    res: dict[str, Any] = {
        'success': False,
        'allocated': False,
        'failure_stage': None,
        'failure_code': None,
    }

    ok_pg, pg_vols, pg_err = inspect_local_image(PG_IMAGE)
    if not ok_pg:
        res['failure_stage'] = 'preflight'
        res['failure_code'] = pg_err or 'pg_image_missing'
        return res

    ok_broker, _, broker_err = inspect_local_image(MOSQUITTO_IMAGE)
    if not ok_broker:
        res['failure_stage'] = 'preflight'
        res['failure_code'] = broker_err or 'broker_image_missing'
        return res

    if dry_run:
        res['success'] = False
        res['failure_code'] = 'dry_run_complete'
        res['aggregate'] = {
            'success': False,
            'verdict': 'DRY_RUN_ONLY',
            'native_status': 'DRY_RUN',
            'pg_cleanup_verified': True,
            'broker_cleanup_verified': True,
            'host_cleanup_verified': True,
        }
        return res

    tmp_base = Path('/tmp/opencode')
    tmp_base.mkdir(parents=True, exist_ok=True)

    fixture_dir, host_receipt = create_verified_host_fixture_dir(tmp_base)
    res['allocated'] = True

    pg_fixture: DatabaseFixture | None = None
    broker_fixture: DynSecProbeFixture | None = None
    test_binary_path = fixture_dir / 'task28a-http-native.test'

    try:
        ok_compile, compile_err = compile_http_test_binary(test_binary_path)
        if not ok_compile:
            res['failure_stage'] = 'compile'
            res['failure_code'] = compile_err
            return res

        pg_fixture = DatabaseFixture()
        if isinstance(pg_vols, dict) and pg_vols:
            pg_fixture.tmpfs_mounts = list(pg_vols.keys())
        pg_fixture.create()
        original_pg_cid = pg_fixture.container_id
        pg_fixture.start()
        pg_fixture.wait_ready()

        migrations = get_ordered_migrations(ROOT / 'migrations')
        for m in migrations:
            if m.suffix == '.sh':
                subprocess.run(
                    ['docker', 'cp', str(m), f"{pg_fixture.container_id}:/roles.sh"],
                    env=ENV, check=True, stdout=subprocess.DEVNULL, timeout=30
                )
                subprocess.run(
                    ['docker', 'exec', pg_fixture.container_id, 'sh', '/roles.sh'],
                    env=ENV, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30
                )
            elif m.suffix == '.sql':
                pg_fixture.sql(m.read_bytes())

        # Setup broker fixture with backend bootstrap opt-in
        broker_fixture = DynSecProbeFixture(fixture_dir, MOSQUITTO_IMAGE, initialize_backend=True)
        broker_fixture.prepare()
        broker_fixture.start()

        # Re-verify broker container receipt
        broker_receipt = None
        for r in broker_fixture.verified_receipts:
            if (
                r.get('kind') == 'container' and
                r.get('expected_role') == 'broker' and
                r.get('expected_project') == broker_fixture.project and
                r.get('expected_name') == f'{broker_fixture.project}-broker-1'
            ):
                broker_receipt = r
                break

        if not broker_receipt:
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'missing_broker_receipt'
            return res

        broker_cid = broker_receipt['resource_id']
        v_broker = inspect_owned_resource(
            kind='container',
            candidate_id=broker_cid,
            expected_project=broker_receipt['expected_project'],
            expected_role=broker_receipt['expected_role'],
            timeout_sec=5.0,
            expected_name=broker_receipt['expected_name'],
        )
        if v_broker.get('status') != 'owned':
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'broker_ownership_mismatch'
            return res

        # Re-verify broker network receipt
        network_receipt = None
        for r in broker_fixture.verified_receipts:
            if (
                r.get('kind') == 'network' and
                r.get('expected_role') == 'default' and
                r.get('expected_project') == broker_fixture.project
            ):
                network_receipt = r
                break

        if not network_receipt:
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'missing_network_receipt'
            return res

        broker_net = network_receipt['resource_id']
        v_net = inspect_owned_resource(
            kind='network',
            candidate_id=broker_net,
            expected_project=network_receipt['expected_project'],
            expected_role=network_receipt['expected_role'],
            timeout_sec=5.0,
        )
        if v_net.get('status') != 'owned':
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'network_ownership_mismatch'
            return res

        # Re-verify PG container receipt
        verified_pg_cid = None
        try:
            verified_pg_cid = pg_fixture._load_and_verify_receipt()
        except Exception:
            verified_pg_cid = None

        if not verified_pg_cid or verified_pg_cid != original_pg_cid:
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'pg_receipt_mismatch'
            return res

        # Attach PG container to broker network with alias task28a-pg
        connect_proc = subprocess.run(
            ['docker', 'network', 'connect', '--alias', 'task28a-pg', broker_net, pg_fixture.container_id],
            env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15
        )
        if connect_proc.returncode != 0:
            res['failure_stage'] = 'network_connect'
            res['failure_code'] = 'failed_to_attach_pg_to_broker_network'
            return res

        transfer_ok, transfer_err = transfer_binary_to_broker(broker_cid, test_binary_path)
        if not transfer_ok:
            res['failure_stage'] = 'binary_transfer'
            res['failure_code'] = transfer_err or 'failed_to_transfer_binary_to_broker'
            return res

        v_broker_pre_exec = inspect_owned_resource(
            kind='container',
            candidate_id=broker_cid,
            expected_project=broker_receipt['expected_project'],
            expected_role=broker_receipt['expected_role'],
            timeout_sec=5.0,
            expected_name=broker_receipt['expected_name'],
        )
        if v_broker_pre_exec.get('status') != 'owned':
            res['failure_stage'] = 'reverify_receipts'
            res['failure_code'] = 'broker_not_owned_before_exec'
            return res

        run_marker = secrets.token_hex(8)
        stdin_payload = build_http_native_test_input(
            pg_alias='task28a-pg',
            pg_password=pg_fixture.passwords[0],
            manager_username='admin',
            manager_password=broker_fixture.passwords['admin'],
            backend_password=broker_fixture.passwords['backend_service'],
            run_marker=run_marker,
            project_binding=broker_fixture.project,
        )
        stdin_bytes = json.dumps(stdin_payload).encode('utf-8')

        exec_cmd = build_http_native_exec_cmd(broker_cid)
        exec_proc = subprocess.run(
            exec_cmd,
            input=stdin_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ENV,
            timeout=100,
        )

        test_result = classify_http_native_test_output(exec_proc.returncode, exec_proc.stdout, exec_proc.stderr)
        res['native_test_result'] = test_result

    except Exception:
        res['failure_stage'] = 'orchestration_error'
        res['failure_code'] = 'orchestration_exception'
    finally:
        clean_res = execute_safe_teardown(
            pg_fixture=pg_fixture,
            broker_fixture=broker_fixture,
            host_receipt=host_receipt,
        )
        res['cleanup'] = clean_res

    native_status = res.get('native_test_result', {}).get('status', 'FAIL')
    agg = evaluate_aggregate_result(
        native_status=native_status,
        pg_cleanup_verified=clean_res.get('pg_cleanup_verified', False),
        broker_cleanup_verified=clean_res.get('broker_cleanup_verified', False),
        host_cleanup_verified=clean_res.get('host_cleanup_verified', False),
    )
    res['aggregate'] = agg
    res['success'] = agg['success']
    return res


if __name__ == '__main__':
    out = run_http_native_suite(dry_run=False)
    print(f"VERDICT: {out.get('aggregate', {}).get('verdict', out.get('failure_code'))}")
    sys.exit(0 if out['success'] else 1)
