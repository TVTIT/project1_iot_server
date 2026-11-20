#!/usr/bin/env python3
"""Required-run PostgreSQL proof for Stage 8.2c/d managed-media transitions."""
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
sys.path.insert(0, str(ROOT / 'scripts'))

import stage3_persistence_runner as stage3  # noqa: E402
import stage5_intent_fixture as stage5  # noqa: E402

REQUIRED = (
    'TestPostgresAdmissionReplayConflictAndAuthorization',
    'TestPostgresAdmissionSerializesQuotaAcrossGateways',
    'TestPostgresAdmissionDeferredCommitFailureReturnsNoIntent',
    'TestPostgresAdmissionUnsettledQuotaFencingAcrossLifecycle',
    'TestPostgresGrantReservationFenceAndConfirmation',
    'TestPostgresGrantDenialAndTerminalReplay',
    'TestPostgresGrantDuplicatePreservesLivePendingIntent',
    'TestPostgresCompleteExpiredSchedulesSafeCleanup',
    'TestPostgresVerifierClaimFencingReadyReplayMissingReplayAndRetry',
    'TestPostgresExpiryAndCleanupClaimsPreserveTerminalAndExcludeReady',
)


def compile_test(directory):
    binary = Path(directory) / 'media.test'
    env = dict(stage3.ENV)
    for key in ('GOCACHE', 'GOMODCACHE', 'GOPROXY'):
        if key in os.environ:
            env[key] = os.environ[key]
    env['CGO_ENABLED'] = '0'
    env.setdefault('GOCACHE', str(Path(directory) / 'gocache'))
    proc = subprocess.run(
        ['go', 'test', '-c', '-o', str(binary), './internal/media'],
        cwd=ROOT / 'src', env=env, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=180,
    )
    if proc.returncode:
        raise RuntimeError('media Go test binary build failed: ' +
                           (proc.stdout + proc.stderr).decode(errors='replace')[-4000:])
    return binary


def run_media_tests(fixture, binary):
    stage3.docker_cp(fixture, binary, '/tmp/media.test')
    output = stage3.docker_exec(fixture, [
        'sh', '-c',
        'export MEDIA_TEST_ISOLATED=1 '
        'MEDIA_TEST_SETUP_URL="postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable" '
        'MEDIA_TEST_BACKEND_URL="postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"; '
        "exec /tmp/media.test -test.v -test.timeout 120s -test.run '^TestPostgres(Admission|Grant|Complete|Verifier|ExpiryAndCleanup)'",
    ], timeout=150)
    text = output.decode(errors='replace')
    if '--- SKIP:' in text or any(
        f'=== RUN   {name}\n' not in text or f'--- PASS: {name} (' not in text
        for name in REQUIRED
    ):
        raise RuntimeError('media native require-run did not execute every required test: ' + text[-6000:])
    print(text, flush=True)
    print(f'Stage 8.4c native require-run: {len(REQUIRED)} required PostgreSQL tests PASS, no skips', flush=True)


def run_fixture(binary):
    fixture = stage3.new_fixture()
    primary_error = None
    try:
        stage5.migrate(fixture, 28)
        _, terminal = fixture.sql(b"SELECT version || ':' || name FROM public.schema_migrations ORDER BY version DESC LIMIT 1")
        if b'28:stage8_media_metadata' not in terminal:
            raise RuntimeError(f'fixture did not reach exact migration 28 terminal: {terminal!r}')
        print('Exact schema terminal 28:stage8_media_metadata verified', flush=True)
        run_media_tests(fixture, binary)
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        stage3.finish_fixture(fixture, primary_error)


def main():
    scratch = Path('/var/tmp/opencode/stage8-media-db')
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(scratch, 0o700)
    with tempfile.TemporaryDirectory(prefix='required-run-', dir=scratch) as work:
        os.chmod(work, 0o700)
        run_fixture(compile_test(work))


if __name__ == '__main__':
    main()
