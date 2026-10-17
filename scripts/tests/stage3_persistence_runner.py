#!/usr/bin/env python3
"""Database-only migration 020 rehearsal on isolated pinned Timescale fixtures."""
import subprocess
import sys
import os
import tempfile
import shutil
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
from task28_simplified_schema_runner import DatabaseFixture, ENV, sanitize_output  # noqa: E402

IMAGE = 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a'
MIGRATIONS = ROOT / 'migrations'
SQL = ROOT / 'scripts/sql'


def docker_exec(fixture, args, *, input_data=None, timeout=60, expect=True):
    proc = subprocess.run(['docker', 'exec', '-i', fixture.container_id, *args],
                          env=ENV, input=input_data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    output = proc.stdout + proc.stderr
    if any(secret.encode() in output for secret in fixture.passwords):
        raise RuntimeError('fixture command output contained a secret')
    if (proc.returncode == 0) != expect:
        clean = sanitize_output(output.decode('utf-8', errors='replace'), fixture.passwords)
        raise RuntimeError(f'fixture command failed (expected success={expect}): {clean}')
    return output


def docker_cp(fixture, source, destination):
    proc = subprocess.run(['docker', 'cp', str(source), f'{fixture.container_id}:{destination}'],
                          env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    output = proc.stdout + proc.stderr
    if any(secret.encode() in output for secret in fixture.passwords):
        raise RuntimeError('docker copy output contained a secret')
    if proc.returncode:
        clean = sanitize_output(output.decode('utf-8', errors='replace'), fixture.passwords)
        raise RuntimeError(f'fixture copy failed: {clean}')
    return output


def compile_integration_binary(directory):
    binary = Path(directory) / 'telemetry.test'
    env = dict(ENV)
    for key in ('GOCACHE', 'GOMODCACHE', 'GOPROXY'):
        if key in os.environ:
            env[key] = os.environ[key]
    env.update({'CGO_ENABLED': '0'})
    env.setdefault('GOCACHE', str(Path(directory) / 'gocache'))
    proc = subprocess.run(['go', 'test', '-c', '-o', str(binary), './internal/telemetry'],
                          cwd=ROOT / 'src', env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=180)
    if proc.returncode:
        raise RuntimeError('telemetry integration test compile failed: ' +
                           (proc.stdout + proc.stderr).decode('utf-8', errors='replace')[-4000:])
    return binary


def run_go_integration(fixture, binary, *, late_chunk=False, legacy=False, legacy_history=False):
    docker_cp(fixture, binary, '/tmp/telemetry.test')
    test_run = '^TestNativeTelemetryHistoryUpgradedLegacyProvenance$' if legacy_history else '^TestPostgresTelemetryIntegration$'
    command = [
        'sh', '-c',
        'export TELEMETRY_TEST_ISOLATED=1 ' +
        ('TELEMETRY_TEST_LATE_CHUNK=1 ' if late_chunk else '') +
        ('TELEMETRY_TEST_LEGACY=1 ' if legacy else '') +
        ('TELEMETRY_TEST_LEGACY_HISTORY=1 ' if legacy_history else '') +
        'TELEMETRY_TEST_SETUP_URL="postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable" '
        'TELEMETRY_TEST_BACKEND_URL="postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"; '
        f'exec /tmp/telemetry.test -test.v -test.run \'{test_run}\'',
    ]
    output = docker_exec(fixture, command, timeout=90)
    marker = b'INTEGRATION CASE PASS:' if late_chunk or legacy or legacy_history else b'INTEGRATION CASES PASS:'
    test_func = b'TestNativeTelemetryHistoryUpgradedLegacyProvenance' if legacy_history else b'TestPostgresTelemetryIntegration'
    if b'=== RUN   ' + test_func not in output or marker not in output or b'--- PASS: ' + test_func not in output:
        clean = sanitize_output(output.decode('utf-8', errors='replace'), fixture.passwords)
        raise RuntimeError(f'Go PostgreSQL integration did not report its required pass marker: {clean}')
    print(output.decode('utf-8', errors='replace'), flush=True)


def bootstrap_to_9(fixture):
    roles = MIGRATIONS / '000000_configure_supabase_roles.sh'
    docker_cp(fixture, roles, '/tmp/task32-roles.sh')
    docker_exec(fixture, ['sh', '/tmp/task32-roles.sh'])
    for path in sorted(MIGRATIONS.glob('*.up.sql')):
        version = int(path.name[:6])
        if version > 9 or version in (4, 5):
            continue
        fixture.sql(path.read_bytes())
    # 000004 is conditional on auth.users; 000005 is optional development seed.
    fixture.sql((MIGRATIONS / '000004_supabase_compat.up.sql').read_bytes())


def run_existing_migration_runner(fixture, max_version, *, expect=True):
    runner = ROOT / 'scripts/run-migrations.sh'
    docker_cp(fixture, runner, '/tmp/task32-run-migrations.sh')
    docker_exec(fixture, ['mkdir', '-p', '/tmp/task32-migrations'])
    for path in sorted(MIGRATIONS.glob('*.up.sql')):
        version = int(path.name[:6])
        if 10 <= version <= max_version:
            docker_cp(fixture, path, f'/tmp/task32-migrations/{path.name}')
    cmd = [
        'sh', '-c',
        'export PGHOST=127.0.0.1 PGUSER=fixture_admin PGDATABASE=fixture_service '
        'PGPASSWORD="$POSTGRES_PASSWORD" MIGRATIONS_DIR=/tmp/task32-migrations; '
        'exec sh /tmp/task32-run-migrations.sh',
    ]
    return docker_exec(fixture, cmd, timeout=180, expect=expect)


def new_fixture():
    fixture = DatabaseFixture()
    try:
        fixture.prepare_image()
        fixture.create()
        fixture.start()
        fixture.wait_ready()
    except Exception as exc:
        finish_fixture(fixture, exc)
        raise
    return fixture


def apply_base_19(fixture):
    bootstrap_to_9(fixture)
    run_existing_migration_runner(fixture, 19)
    _, out = fixture.sql((SQL / 'verify-migration-000019.sql').read_bytes())
    if b'Migration 000019 verification passed.' not in out:
        raise RuntimeError('migration 019 verifier did not pass at checkpoint')


def seed_upgrade(fixture):
    fixture.sql((SQL / 'seed-migration-000020-upgrade.sql').read_bytes())


def apply_20(fixture):
    started = time.monotonic()
    out = run_existing_migration_runner(fixture, 20)
    elapsed = time.monotonic() - started
    if b'Applying migration 000020_stage3_telemetry_ingestion.up.sql' not in out:
        raise RuntimeError('existing migration runner did not apply migration 020')
    return elapsed, out


def remember_replay_state(fixture):
    fixture.sql(b"""
      CREATE TABLE stage3_replay_snapshot AS
      SELECT (SELECT count(*) FROM telemetry) AS telemetry_count,
             (SELECT max(sample_id) FROM telemetry) AS max_sample_id,
             (SELECT count(*) FROM processed_messages) AS ledger_count;
      CREATE TABLE stage3_replay_sequence_snapshot AS
      SELECT last_value, is_called FROM telemetry_sample_id_seq;
      CREATE TABLE stage3_replay_jobs_snapshot AS
      SELECT job_id, scheduled FROM timescaledb_information.jobs
      WHERE hypertable_name='telemetry' AND proc_name='policy_compression';
    """)


def verify_replay_state(fixture):
    fixture.sql(b"""
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM stage3_replay_snapshot s WHERE
          s.telemetry_count <> (SELECT count(*) FROM telemetry) OR
          s.max_sample_id IS DISTINCT FROM (SELECT max(sample_id) FROM telemetry) OR
          s.ledger_count <> (SELECT count(*) FROM processed_messages) OR
          ROW((SELECT last_value FROM telemetry_sample_id_seq), (SELECT is_called FROM telemetry_sample_id_seq))
            IS DISTINCT FROM
          ROW((SELECT last_value FROM stage3_replay_sequence_snapshot), (SELECT is_called FROM stage3_replay_sequence_snapshot))) THEN
          RAISE EXCEPTION 'migration replay changed row counts or sequence state';
        END IF;
        IF EXISTS (SELECT 1 FROM stage3_replay_jobs_snapshot old
                   LEFT JOIN timescaledb_information.jobs current USING(job_id)
                   WHERE current.job_id IS NULL OR old.scheduled IS DISTINCT FROM current.scheduled) THEN
          RAISE EXCEPTION 'migration replay changed scheduler state';
        END IF;
      END $$;
    """)


def finish_fixture(fixture, primary_error=None):
    try:
        cleaned = fixture.cleanup()
        if not cleaned:
            raise RuntimeError('fixture cleanup absence was not verified')
        print('Fixture cleanup verified: True', flush=True)
    except Exception as cleanup_error:
        if primary_error is not None:
            if hasattr(primary_error, 'add_note'):
                primary_error.add_note(f'Fixture cleanup failed: {cleanup_error}')
            return
        raise


def common_graph_seed(fixture):
    fixture.sql(b"""
      INSERT INTO gateways(gateway_id,name) VALUES ('fixture_gw','fresh DDL fixture');
      INSERT INTO sensors(gateway_id,sensor_id,name,unit) VALUES ('fixture_gw','fixture_sensor','DDL sensor','degC');
      INSERT INTO twin_entities(entity_id,entity_type,gateway_id)
        VALUES ('urn:ngsi-ld:Gateway:fixture_gw','Gateway','fixture_gw'),
               ('urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor','Sensor','fixture_gw');
      INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id)
        SELECT p.id,'hasSensor',c.id FROM twin_entities p,twin_entities c
        WHERE p.entity_id='urn:ngsi-ld:Gateway:fixture_gw'
          AND c.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor';
      INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities;
    """)


def run_normal_fixture(integration_binary):
    fixture = new_fixture()
    primary_error = None
    try:
        bootstrap_to_9(fixture)
        elapsed, _ = apply_20(fixture)
        common_graph_seed(fixture)
        run_go_integration(fixture, integration_binary)
        _, out = fixture.sql((SQL / 'verify-migration-000020.sql').read_bytes())
        if b'Migration 000020 verification passed.' not in out:
            raise RuntimeError('fresh migration verifier did not pass')
        remember_replay_state(fixture)
        fixture.sql((MIGRATIONS / '000020_stage3_telemetry_ingestion.up.sql').read_bytes())
        verify_replay_state(fixture)
        _, metrics = fixture.sql(b"""
          SELECT version(), extversion FROM pg_extension WHERE extname='timescaledb';
          SELECT count(*) AS raw_count, count(sample_id) AS sample_ids, min(sample_id), max(sample_id) FROM telemetry;
          SELECT count(*) AS ledger_count FROM processed_messages;
          SELECT count(*) AS hypertables FROM timescaledb_information.hypertables
           WHERE hypertable_name IN ('telemetry','twin_temporal_values');
          SELECT pg_total_relation_size('telemetry'), hypertable_size('telemetry');
        """)
        print('FRESH FIXTURE: migration20, app grants, constraint/index cases, and direct replay PASS', flush=True)
        print(f'FRESH DDL runner elapsed: {elapsed:.3f}s', flush=True)
        print(metrics.decode('utf-8', errors='replace'), flush=True)
        # The runner's applied-version branch must also be safe and idempotent.
        run_existing_migration_runner(fixture, 20)
        verify_replay_state(fixture)
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        finish_fixture(fixture, primary_error)


def run_upgrade_fixture(integration_binary):
    fixture = new_fixture()
    primary_error = None
    try:
        apply_base_19(fixture)
        seed_upgrade(fixture)
        _, preflight = fixture.sql((SQL / 'preflight-migration-000020.sql').read_bytes())
        elapsed, _ = apply_20(fixture)
        _, legacy = fixture.sql((SQL / 'verify-migration-000020-upgrade.sql').read_bytes())
        if b'Migration 000020 upgrade data and maintenance verification passed.' not in legacy:
            raise RuntimeError('upgrade legacy snapshot verifier did not pass')
        _, verifier = fixture.sql((SQL / 'verify-migration-000020.sql').read_bytes())
        if b'Migration 000020 verification passed.' not in verifier:
            raise RuntimeError('upgrade schema and role verifier did not pass')
        run_go_integration(fixture, integration_binary, late_chunk=True)
        run_go_integration(fixture, integration_binary, legacy=True)
        run_go_integration(fixture, integration_binary, legacy_history=True)
        remember_replay_state(fixture)
        fixture.sql((MIGRATIONS / '000020_stage3_telemetry_ingestion.up.sql').read_bytes())
        verify_replay_state(fixture)
        _, metrics = fixture.sql(b"""
          SELECT version(), extversion FROM pg_extension WHERE extname='timescaledb';
          SELECT count(*) AS raw_count, count(sample_id) AS sample_ids, min(sample_id), max(sample_id) FROM telemetry;
          SELECT count(*) AS ledger_count FROM processed_messages;
          SELECT count(*) AS compressed_chunks FROM timescaledb_information.chunks
           WHERE hypertable_name='telemetry' AND is_compressed;
          SELECT (SELECT relation_bytes FROM stage3_legacy_relation_snapshot) AS relation_bytes_before,
                 pg_total_relation_size('telemetry') AS relation_bytes_after,
                 (SELECT hypertable_bytes FROM stage3_legacy_relation_snapshot) AS hypertable_bytes_before,
                 hypertable_size('telemetry') AS hypertable_bytes_after;
        """)
        _, postflight = fixture.sql((SQL / 'preflight-migration-000020.sql').read_bytes())
        print('UPGRADE FIXTURE: verifier19 checkpoint, v20 data preservation, policies, grants, and replay PASS', flush=True)
        print(f'Upgrade DDL runner elapsed: {elapsed:.3f}s', flush=True)
        print('READ-ONLY PREFLIGHT v19:', preflight.decode('utf-8', errors='replace'), flush=True)
        print('READ-ONLY PREFLIGHT v20:', postflight.decode('utf-8', errors='replace'), flush=True)
        print(metrics.decode('utf-8', errors='replace'), flush=True)
        run_existing_migration_runner(fixture, 20)
        verify_replay_state(fixture)
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        finish_fixture(fixture, primary_error)


def run_rollback_fixture():
    fixture = new_fixture()
    primary_error = None
    try:
        apply_base_19(fixture)
        seed_upgrade(fixture)
        fixture.sql(b'CREATE SEQUENCE public.telemetry_sample_id_seq AS BIGINT;')
        result = run_existing_migration_runner(fixture, 20, expect=False)
        # The expected duplicate sequence forces a real SQL error after the
        # migration paused compression and entered its DDL transaction.
        if b'telemetry_sample_id_seq' not in result:
            raise RuntimeError('fixture SQL failure did not hit the intended DDL point')
        fixture.sql(b"""
          DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=19 AND name='mqtt_simplified_journal')
               OR EXISTS (SELECT 1 FROM schema_migrations WHERE version=20)
               OR EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='telemetry' AND column_name='sample_id')
               OR (SELECT count(*) FROM telemetry) <> (SELECT count(*) FROM stage3_legacy_telemetry_snapshot)
               OR (SELECT count(*) FROM processed_messages) <> (SELECT count(*) FROM stage3_legacy_ledger_snapshot) THEN
              RAISE EXCEPTION 'failed migration did not roll back schema, data, and tracking';
            END IF;
            IF EXISTS (SELECT 1 FROM timescaledb_information.jobs j JOIN stage3_legacy_jobs_snapshot old USING(job_id)
                       WHERE j.scheduled IS DISTINCT FROM false) THEN
              RAISE EXCEPTION 'failure path did not leave the compression scheduler paused for operator restoration';
            END IF;
          END $$;
        """)
        # DDL rollback restores the database transaction but the scheduler
        # pause was autocommitted. Perform the documented operator restoration.
        fixture.sql(b"SELECT alter_job(job_id, scheduled => scheduled) FROM stage3_legacy_jobs_snapshot;")
        fixture.sql(b"""
          DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM timescaledb_information.jobs j JOIN stage3_legacy_jobs_snapshot old USING(job_id)
                       WHERE j.scheduled IS DISTINCT FROM old.scheduled) THEN
              RAISE EXCEPTION 'operator scheduler restoration failed';
            END IF;
          END $$;
        """)
        print('ROLLBACK FIXTURE: injected duplicate-sequence DDL failure rolled back schema/data/tracking; operator job restore PASS', flush=True)
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        finish_fixture(fixture, primary_error)


def main():
    parser = argparse.ArgumentParser(description='Database-only migration 020 rehearsal on isolated pinned Timescale fixtures.')
    parser.add_argument('--fixture', choices=['all', 'normal', 'upgrade', 'rollback'], default='all',
                        help='Fixture to run (default: all)')
    args = parser.parse_args()

    print(f'Pinned image: {IMAGE}', flush=True)
    work = tempfile.mkdtemp(prefix='task32-testbin-', dir='/tmp')
    os.chmod(work, 0o700)
    try:
        binary = compile_integration_binary(work)
        if args.fixture in ('all', 'normal'):
            run_normal_fixture(binary)
        if args.fixture in ('all', 'upgrade'):
            run_upgrade_fixture(binary)
        if args.fixture in ('all', 'rollback'):
            run_rollback_fixture()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if args.fixture == 'all':
        print('PASS: isolated database-only migration 000020 rehearsals', flush=True)
    else:
        print(f'PASS: isolated database-only migration 000020 rehearsal ({args.fixture})', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr, flush=True)
        raise
