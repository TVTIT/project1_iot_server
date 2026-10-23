#!/usr/bin/env python3
"""Task 5.2 PostgreSQL-only rehearsal. No broker, HTTP, deployment or CI lane.

Uses the existing pinned DatabaseFixture lifecycle and migration runner. Every
container is owned by its verified cidfile, has no published ports, and is removed
with authoritative absence verification even on failure. No deployment DSN is read.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile

from stage3_persistence_runner import (
    ROOT, bootstrap_to_9, common_graph_seed, docker_cp, docker_exec,
    finish_fixture, new_fixture, run_existing_migration_runner,
)


def migrate(fixture, version):
    bootstrap_to_9(fixture)
    fixture.sql(b"""CREATE TABLE auth.users (
        id uuid PRIMARY KEY, email text, aud text, role text,
        raw_app_meta_data jsonb NOT NULL DEFAULT '{}',
        raw_user_meta_data jsonb NOT NULL DEFAULT '{}',
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz);
    """)
    fixture.sql((ROOT / 'migrations/000004_supabase_compat.up.sql').read_bytes())
    run_existing_migration_runner(fixture, version)


def verify(fixture):
    for name in ('verify-schema-hardening.sql', 'verify-migration-000022.sql',
                 'verify-migration-000023.sql'):
        _, output = fixture.sql(b'\x5cset expected_schema_terminal 23\n' + (ROOT / 'scripts/sql' / name).read_bytes())
        print(output.decode(), flush=True)


def native(fixture, binary):
    docker_cp(fixture, binary, '/tmp/control.test')
    output = docker_exec(fixture, ['sh', '-c',
        'export CONTROL_TEST_ISOLATED=1 '
        'CONTROL_TEST_SETUP_URL="postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable" '
        'CONTROL_TEST_BACKEND_URL="postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable"; '
        'exec /tmp/control.test -test.v -test.timeout 120s -test.coverprofile=/tmp/control.cover -test.run "^TestNativeControl"'], timeout=150)
    text = output.decode()
    required = re.findall(r'^func (TestNativeControl\w+)\(',
        (ROOT / 'src/internal/digitaltwin/postgres_control_integration_test.go').read_text(), re.M)
    if not required or '--- SKIP:' in text or any(
            f'=== RUN   {name}\n' not in text or f'--- PASS: {name} (' not in text
            for name in required):
        raise RuntimeError('native control tests did not all execute and pass')
    print(text, flush=True)
    print(f'Task 5.2 native require-run: {len(required)} top-level tests PASS, no skips', flush=True)
    coverage = os.environ.get('CONTROL_TEST_COVERAGE_FILE')
    if coverage:
        Path(coverage).write_bytes(docker_exec(fixture, ['cat', '/tmp/control.cover']))


def seed_legacy(fixture):
    common_graph_seed(fixture)
    fixture.sql(b"""
        INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at)
        SELECT gen_random_uuid(),id,'update_configuration','{"sampling_interval_seconds":5}',
               'acknowledged',1,'2026-01-01Z','2026-01-02Z'
        FROM twin_entities WHERE entity_type='Sensor';
        INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at)
        SELECT gen_random_uuid(),id,'legacy_unsupported','{"sentinel":true}',
               s.status,NULL,'2026-01-01Z','2026-01-02Z'
        FROM twin_entities CROSS JOIN (VALUES('pending'),('published'),('cancelled')) s(status)
        WHERE entity_type='Gateway';
        INSERT INTO twin_outbox(command_id,topic,payload)
        SELECT command_id,'fixture/legacy','{"protocol_version":1,"message_type":"command"}' FROM twin_commands;
        UPDATE twin_states SET desired_state='{"sample_interval_us":10000000,"preserve":true}',
           reported_state='{"value":42,"configuration":{"sample_interval_us":20000,"desired_version":"9007199254740993"}}';
        CREATE TABLE stage52_commands_snapshot AS SELECT command_id,to_jsonb(c) AS original FROM twin_commands c;
        CREATE TABLE stage52_outbox_snapshot AS SELECT command_id,to_jsonb(o) AS original FROM twin_outbox o;
        CREATE TABLE stage52_states_snapshot AS SELECT entity_id,to_jsonb(s) AS original FROM twin_states s;
    """)


def preservation(fixture):
    fixture.sql(b"""DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM stage52_commands_snapshot old FULL JOIN twin_commands c USING(command_id)
            WHERE old.original IS DISTINCT FROM to_jsonb(c)-ARRAY['target_entity_type','control_version','requested_frequency_hz',
                'result_bytes','result_hash','result_received_at','late_result_bytes','late_result_hash','late_result_received_at'])
          OR EXISTS(SELECT 1 FROM stage52_outbox_snapshot old FULL JOIN twin_outbox o USING(command_id)
            WHERE old.original IS DISTINCT FROM to_jsonb(o)-'payload_bytes')
          OR EXISTS(SELECT 1 FROM stage52_states_snapshot old FULL JOIN twin_states s USING(entity_id)
            WHERE old.original IS DISTINCT FROM to_jsonb(s)) THEN
            RAISE EXCEPTION 'Migration changed legacy rows/state';
        END IF;
        IF EXISTS(SELECT 1 FROM twin_commands WHERE control_version IS NOT NULL)
          OR EXISTS(SELECT 1 FROM twin_outbox WHERE payload_bytes IS NOT NULL) THEN
            RAISE EXCEPTION 'Migration relabeled or made legacy sentinels dispatchable';
        END IF;
    END $$;""")


def scenario(kind, binary):
    fixture = new_fixture()
    primary = None
    try:
        if kind == 'fresh':
            migrate(fixture, 23)
            verify(fixture)
            native(fixture, binary)
            run_existing_migration_runner(fixture, 23)
            verify(fixture)
        else:
            migrate(fixture, 22)
            seed_legacy(fixture)
            if kind == 'upgrade':
                fixture.sql((ROOT / 'scripts/sql/preflight-migration-000023.sql').read_bytes())
                run_existing_migration_runner(fixture, 23)
                preservation(fixture)
                verify(fixture)
                fixture.sql((ROOT / 'migrations/000023_sensor_control_intent.up.sql').read_bytes())
                preservation(fixture)
            else:
                if kind == 'active-conflict':
                    fixture.sql(b"""INSERT INTO twin_commands(command_id,entity_id,action,status,issued_at,expires_at)
                        SELECT gen_random_uuid(),id,'legacy_fixture','pending','2026-01-01Z','2026-01-02Z'
                        FROM twin_entities WHERE entity_type='Sensor';""")
                    expected = b'multiple active commands for a Sensor'
                elif kind == 'version-conflict':
                    fixture.sql(b"UPDATE twin_commands SET desired_version=-1 WHERE status='acknowledged';")
                    expected = b'negative version or invalid command deadline'
                else:
                    fixture.sql(b"UPDATE twin_commands SET expires_at=issued_at WHERE status='acknowledged';")
                    expected = b'negative version or invalid command deadline'
                fixture.sql(b'CREATE TABLE stage52_conflict_snapshot AS SELECT command_id,to_jsonb(c) AS original FROM twin_commands c;')
                _, output = fixture.sql((ROOT / 'scripts/sql/preflight-migration-000023.sql').read_bytes(), expect=False)
                if expected not in output:
                    raise RuntimeError('operator preflight did not explain conflict')
                output = run_existing_migration_runner(fixture, 23, expect=False)
                if expected not in output:
                    raise RuntimeError('locked migration preflight did not explain conflict')
                fixture.sql(b"""DO $$ BEGIN
                    IF EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=23)
                       OR to_regclass('public.twin_command_requests') IS NOT NULL
                       OR EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='twin_outbox' AND column_name='payload_bytes')
                       OR EXISTS(SELECT 1 FROM stage52_conflict_snapshot old FULL JOIN twin_commands c USING(command_id)
                                 WHERE old.original IS DISTINCT FROM to_jsonb(c)) THEN
                        RAISE EXCEPTION 'Rejected migration changed data or schema';
                    END IF;
                END $$;""")
                print(f'Expected preflight refusal: {expected.decode()}; data/schema unchanged', flush=True)
        print(f'TASK52 {kind}: PASS', flush=True)
    except Exception as exc:
        primary = exc
        raise
    finally:
        finish_fixture(fixture, primary)


def main():
    with tempfile.TemporaryDirectory(prefix='task52-control-', dir='/tmp/opencode') as directory:
        binary = Path(directory) / 'control.test'
        subprocess.run(['go', 'test', '-cover', '-c', '-o', str(binary), './internal/digitaltwin'],
                       cwd=ROOT / 'src', env={**os.environ, 'CGO_ENABLED': '0'}, check=True, timeout=180)
        for kind in ('fresh', 'upgrade', 'active-conflict', 'version-conflict', 'deadline-conflict'):
            scenario(kind, binary)


if __name__ == '__main__':
    main()
