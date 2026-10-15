#!/usr/bin/env python3
"""Task 3.0 SQL experiment only; reuses the existing disposable PG fixture."""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
from task28_simplified_schema_runner import DatabaseFixture, ENV  # noqa: E402
import subprocess  # noqa: E402

HERE = Path(__file__).resolve().parent


def main():
    fixture = DatabaseFixture()
    try:
        fixture.prepare_image()
        fixture.create()
        fixture.start()
        fixture.wait_ready()
        # Same bootstrap order as the existing simplified-schema native runner.
        for path in sorted((ROOT / 'migrations').iterdir()):
            if path.suffix == '.sh':
                subprocess.run(['docker', 'cp', str(path),
                                f'{fixture.container_id}:/roles.sh'],
                               env=ENV, check=True, capture_output=True, timeout=30)
                result = subprocess.run(['docker', 'exec', fixture.container_id,
                                         'sh', '/roles.sh'], env=ENV,
                                        capture_output=True, timeout=30)
                if result.returncode:
                    raise RuntimeError('fixture role bootstrap failed')
            elif path.suffix == '.sql' and int(path.name[:6]) <= 19:
                fixture.sql(path.read_bytes())
        fixture.sql((ROOT / 'scripts/sql/verify-migration-000019.sql').read_bytes())
        _, out = fixture.sql(b"SELECT version(), extversion FROM pg_extension WHERE extname='timescaledb'; SELECT version, name FROM schema_migrations ORDER BY version;")
        print(out.decode(), flush=True)
        _, out = fixture.sql((HERE / 'seed.sql').read_bytes())
        print('SEED:', out.decode(), flush=True)
        # Deliberately attempt nonconstant-default DDL with a compressed chunk.
        fixture.sql(b"CREATE SEQUENCE telemetry_sequence_probe;")
        _, out = fixture.sql(b"BEGIN; ALTER TABLE telemetry ADD COLUMN direct_probe BIGINT DEFAULT nextval('telemetry_sequence_probe'); ROLLBACK;", expect=False)
        if b'non-constant default expression' not in out:
            raise RuntimeError('compressed DDL failed for an unexpected reason')
        print('DIRECT COMPRESSED DDL REJECTED:', out.decode(), flush=True)
        fixture.sql(b"DROP SEQUENCE telemetry_sequence_probe;")
        started = time.monotonic()
        _, out = fixture.sql((HERE / 'candidate.sql').read_bytes())
        print('CANDIDATE:', out.decode(), flush=True)
        print(f'DDL elapsed seconds: {time.monotonic() - started:.3f}', flush=True)
        _, out = fixture.sql((HERE / 'verify.sql').read_bytes())
        print(out.decode(), flush=True)
        print('PASS: DDL spike assertions', flush=True)
    finally:
        print(f'Cleanup verified: {fixture.cleanup()}', flush=True)


if __name__ == '__main__':
    main()
