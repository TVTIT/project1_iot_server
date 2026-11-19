#!/usr/bin/env python3
"""Real PostgreSQL regression for the two P5a workload summary queries."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import paper_p5a_support as p5a  # noqa: E402
import stage3_persistence_runner as stage3  # noqa: E402


OLD_GATEWAY_SQL = """SELECT coalesce(json_agg(json_build_object(
  'gateway_id',gateway_id,'messages',count(*),'samples',sum(sample_count)) ORDER BY gateway_id),'[]'::json)
  FROM processed_messages WHERE gateway_id IN (:'a',:'b') GROUP BY gateway_id;"""
OLD_TEMPORAL_SQL = """SELECT coalesce(json_agg(json_build_object(
  'entity_id',e.entity_id,'observations',count(*)) ORDER BY e.entity_id),'[]'::json)
  FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
  WHERE e.entity_id IN (:'ua',:'ub') GROUP BY e.entity_id;"""


def run_query(fixture, query: str, variables: dict[str, str], *, expect_success: bool = True):
    args = ['psql', '-X', '-A', '-t', '-U', 'fixture_admin', '-d', 'fixture_service',
            '-v', 'ON_ERROR_STOP=1']
    for key, value in variables.items():
        args.extend(['-v', f'{key}={value}'])
    proc = stage3.subprocess.run(
        ['docker', 'exec', '-i', fixture.container_id, *args],
        env=stage3.ENV, input=query.encode(), stdout=stage3.subprocess.PIPE,
        stderr=stage3.subprocess.PIPE, timeout=30)
    output = proc.stdout + proc.stderr
    if any(secret.encode() in output for secret in fixture.passwords):
        raise RuntimeError('p5a_sql_regression_secret_output_rejected')
    if (proc.returncode == 0) != expect_success:
        clean = stage3.sanitize_output(output.decode('utf-8', errors='replace'), fixture.passwords)
        raise RuntimeError(f'p5a_sql_query_outcome_unexpected: {clean}')
    return proc.returncode, output.decode('utf-8', errors='replace').strip()


def run():
    fixture = stage3.new_fixture()
    primary_error = None
    try:
        stage3.bootstrap_to_9(fixture)
        stage3.apply_20(fixture)
        fixture.sql(b"""
          INSERT INTO gateways(gateway_id,name) VALUES
            ('p5a_sql_gateway_a','query fixture A'),
            ('p5a_sql_gateway_b','query fixture B');
          INSERT INTO processed_messages(
            gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,
            first_sequence,last_sequence,sample_count)
          VALUES
            ('p5a_sql_gateway_a','00000000-0000-4000-8000-000000000001','hash-a1','gateways/p5a_sql_gateway_a/telemetry/sensor','sensor','00000000-0000-4000-8000-000000000010',0,1,2),
            ('p5a_sql_gateway_a','00000000-0000-4000-8000-000000000002','hash-a2','gateways/p5a_sql_gateway_a/telemetry/sensor','sensor','00000000-0000-4000-8000-000000000010',2,2,1),
            ('p5a_sql_gateway_b','00000000-0000-4000-8000-000000000003','hash-b1','gateways/p5a_sql_gateway_b/telemetry/sensor','sensor','00000000-0000-4000-8000-000000000020',0,3,4);
          INSERT INTO twin_entities(entity_id,entity_type,gateway_id)
          VALUES ('urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor','Sensor','p5a_sql_gateway_a'),
                 ('urn:ngsi-ld:Sensor:p5a_sql_gateway_b:sensor','Sensor','p5a_sql_gateway_b');
          INSERT INTO twin_temporal_values(observed_at,entity_id,property_name,value_number)
          SELECT now(), id, 'measurement', 1 FROM twin_entities
          WHERE entity_id='urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor'
          UNION ALL SELECT now(), id, 'measurement', 2 FROM twin_entities
          WHERE entity_id='urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor'
           UNION ALL SELECT now(), id, 'measurement', 3 FROM twin_entities
           WHERE entity_id='urn:ngsi-ld:Sensor:p5a_sql_gateway_b:sensor';
          INSERT INTO twin_states(entity_id,reported_version)
          SELECT id, 17 FROM twin_entities
          WHERE entity_id='urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor';
          ALTER TABLE processed_messages ADD COLUMN timestamp timestamptz;
          UPDATE processed_messages SET timestamp='2026-01-02T03:04:05Z'
          WHERE gateway_id='p5a_sql_gateway_a'
            AND message_id='00000000-0000-4000-8000-000000000001';
         """)

        # Preserve the failure evidence: each pre-fix statement must fail at
        # PostgreSQL's nested-aggregate validation, not an unrelated SQL error.
        _, red_gateway = run_query(fixture, OLD_GATEWAY_SQL,
            {'a': 'p5a_sql_gateway_a', 'b': 'p5a_sql_gateway_b'}, expect_success=False)
        if 'aggregate function calls cannot be nested' not in red_gateway:
            raise RuntimeError('p5a_sql_gateway_red_cause_unexpected: ' + red_gateway)
        print('RED VERIFIED: old per-Gateway query rejected: aggregate function calls cannot be nested', flush=True)
        _, red_temporal = run_query(fixture, OLD_TEMPORAL_SQL,
            {'ua': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor',
             'ub': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_b:sensor'}, expect_success=False)
        if 'aggregate function calls cannot be nested' not in red_temporal:
            raise RuntimeError('p5a_sql_temporal_red_cause_unexpected: ' + red_temporal)
        print('RED VERIFIED: old per-temporal query rejected: aggregate function calls cannot be nested', flush=True)

        _, gateway_output = run_query(fixture, p5a.PER_GATEWAY_SUMMARY_SQL,
            {'a': 'p5a_sql_gateway_a', 'b': 'p5a_sql_gateway_b'})
        gateway_rows = json.loads(gateway_output)
        expected_gateways = [
            {'gateway_id': 'p5a_sql_gateway_a', 'messages': 2, 'samples': 3},
            {'gateway_id': 'p5a_sql_gateway_b', 'messages': 1, 'samples': 4},
        ]
        if gateway_rows != expected_gateways:
            raise RuntimeError(f'p5a_sql_gateway_result_mismatch: {gateway_rows!r}')
        _, temporal_output = run_query(fixture, p5a.PER_TEMPORAL_SUMMARY_SQL,
            {'ua': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor',
             'ub': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_b:sensor'})
        temporal_rows = json.loads(temporal_output)
        expected_temporal = [
            {'entity_id': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor', 'observations': 2},
            {'entity_id': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_b:sensor', 'observations': 1},
        ]
        if temporal_rows != expected_temporal:
            raise RuntimeError(f'p5a_sql_temporal_result_mismatch: {temporal_rows!r}')

        _, empty_gateway = run_query(fixture, p5a.PER_GATEWAY_SUMMARY_SQL,
            {'a': 'p5a_sql_absent_a', 'b': 'p5a_sql_absent_b'})
        _, empty_temporal = run_query(fixture, p5a.PER_TEMPORAL_SUMMARY_SQL,
            {'ua': 'urn:ngsi-ld:Sensor:absent:a', 'ub': 'urn:ngsi-ld:Sensor:absent:b'})
        if json.loads(empty_gateway) != [] or json.loads(empty_temporal) != []:
            raise RuntimeError('p5a_sql_empty_result_not_empty_array')

        version_code, version_output = run_query(fixture, p5a.VERSION_SNAPSHOT_SQL,
            {'urn': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor'})
        if version_code or version_output != '17':
            raise RuntimeError('p5b_version_snapshot_result_mismatch')
        _, state_output = run_query(fixture, p5a.STATE_FINGERPRINT_SQL,
            {'urn': 'urn:ngsi-ld:Sensor:p5a_sql_gateway_a:sensor'})
        if json.loads(state_output).get('reported_version') != 17:
            raise RuntimeError('p5b_state_fingerprint_result_mismatch')

        guard_vars = {
            'gw': 'p5a_sql_gateway_a', 'mid': '00000000-0000-4000-8000-000000000001',
            'hash': 'different-hash', 'sensor': 'moved-sensor',
            'boot': '00000000-0000-4000-8000-000000000010',
            'timestamp': '2026-01-02T03:04:06Z', 'first': '0', 'last': '1',
        }
        for case_name in ('same_id_changed_payload', 'same_id_moved_sensor_time_key',
                          'same_ram_key_different_id', 'different_first_sequence_overlap'):
            variables = dict(guard_vars)
            if case_name in ('same_ram_key_different_id', 'different_first_sequence_overlap'):
                variables['mid'] = '00000000-0000-4000-8000-000000000002'
                variables['sensor'] = 'sensor'
            _, guard_output = run_query(fixture, p5a.conflict_guard_query(case_name), variables)
            if guard_output != 't':
                raise RuntimeError('p5b_conflict_guard_result_mismatch:' + case_name)
        print('PASS P5A_POSTGRES_SQL_REGRESSION: summaries plus P5b UUID/text version join and four conflict-precheck queries', flush=True)
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        stage3.finish_fixture(fixture, primary_error)


if __name__ == '__main__':
    run()
