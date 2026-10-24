#!/usr/bin/env python3
"""Task 5.3 HTTP + PostgreSQL only, using the owned existing native fixture.

No deployment environment, broker, Gateway, broad cleanup or Stage 5 E2E lane.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile

from stage5_intent_fixture import migrate, verify
from stage3_persistence_runner import (
    ROOT, docker_cp, docker_exec, finish_fixture, new_fixture,
)


def main():
    with tempfile.TemporaryDirectory(prefix='task53-http-', dir='/tmp/opencode') as directory:
        binary = Path(directory) / 'server.test'
        subprocess.run(['go', 'test', '-coverpkg=./cmd/server,./internal/httpserver,./internal/digitaltwin,./internal/config',
                        '-c', '-o', str(binary), './cmd/server'],
                       cwd=ROOT / 'src', env={**os.environ, 'CGO_ENABLED': '0'},
                       check=True, timeout=180)
        fixture = new_fixture()
        primary = None
        try:
            migrate(fixture, 23)
            verify(fixture)
            docker_cp(fixture, binary, '/tmp/server.test')
            output = docker_exec(fixture, ['sh', '-c',
                'export CONTROL_TEST_ISOLATED=1 '
                'CONTROL_TEST_SETUP_URL="postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable" '
                'CONTROL_TEST_BACKEND_URL="postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"; '
                'exec /tmp/server.test -test.v -test.timeout 180s -test.coverprofile=/tmp/http.cover '
                '-test.run "^TestNativeControlHTTP"'], timeout=210)
            text = output.decode()
            required = re.findall(r'^func (TestNativeControlHTTP\w+)\(',
                (ROOT / 'src/cmd/server/stage5_native_http_test.go').read_text(), re.M)
            if not required or '--- SKIP:' in text or any(
                    f'=== RUN   {name}\n' not in text or f'--- PASS: {name} (' not in text
                    for name in required):
                raise RuntimeError('native HTTP tests did not all execute and pass')
            print(text, flush=True)
            print(f'Task 5.3 native HTTP require-run: {len(required)} PASS, no skips', flush=True)
            coverage = os.environ.get('CONTROL_HTTP_COVERAGE_FILE')
            if coverage:
                Path(coverage).write_bytes(docker_exec(fixture, ['cat', '/tmp/http.cover']))
        except Exception as exc:
            primary = exc
            raise
        finally:
            finish_fixture(fixture, primary)


if __name__ == '__main__':
    main()
