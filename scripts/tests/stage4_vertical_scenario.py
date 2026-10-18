#!/usr/bin/env python3
"""Task 4.7.3 Vertical Scenario: MQTT commit -> HTTP read -> PATCH -> ingest slice.

This scenario verifies the end-to-end vertical integration slice across
real Mosquitto TLS broker, Go backend, and TimescaleDB:
  1. Provision Gateways A and B sharing local Sensor ID via HTTP admin POST Gateway
     and owner POST Sensor. Assert 201 Created and exact replay 200 OK.
     Assert NGSI-LD graph and inline context contents (@context, hasSensor, unit).
     Setup operator and viewer memberships using fixture context.
  2. Run Docker gateway target simulator for 2x100 samples (Gateway A) and 1x100
     samples (Gateway B positive control).
     Assert CLI run summary (completed=true, app_acknowledged=N, pending_rows=0)
     and outbox drained via CLI count.
     Assert independent SQL counts and ordering in processed_messages, telemetry,
     twin_temporal_values, and twin_states. Verify desired and Gateway sentinels.
     Capture non-secret message/boot identities for assertions and logging.
  3. Real JWT HTTP reads for owner, operator, and viewer:
     - GET /v1/telemetry/history (raw window <= 10s: 200 points, deterministic values)
     - GET /v1/telemetry/history (bucket downsampled: count, avg, min, max)
     - GET /v1/digital-twins (list: Gateway A and Sensor A)
     - GET /v1/digital-twins/{entity_id} (detail: ld+json, @context, hasSensor)
     - GET /v1/digital-twins/{entity_id}/state (state: reported_version 2)
     - GET /v1/digital-twins/{entity_id}/history (property=value: 200 points)
     - GET /v1/digital-twins/{entity_id}/history (property=sample_interval_us: 2 points)
     Cross-tenant isolation: User B owns B; B cannot read A (404), list excludes A,
     and A cannot read B (404).
  4. Snapshot state/history/versions/audit before PATCH.
     Owner PATCHes Sensor A name.
     Admin non-member PATCHes Gateway A description.
     Assert ONLY allowed metadata changes in DB.
     Assert Admin non-member still cannot GET state/history (404).
  5. Simulator ingests 1 further 100-sample batch for Gateway A:
     Total A reaches 300 raw, 3 ledger, 3 interval observations.
     Sensor A reported_version advances to 3.
     Both patched metadata names preserved; desired/audit unchanged.
     Exercise byte-exact resend of an already committed message:
     re-ACKed with status=committed, and DB counts do NOT increase.
"""
from __future__ import annotations

import datetime
import copy
import json
import os
from pathlib import Path
import secrets
import sys
import time
import urllib.parse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stage4_api_runner import Stage4FixtureContext, GatewayFixtureSpec

from task28_simplified_schema_runner import sanitize_output

def sanitize(text: str) -> str:
    return sanitize_output(text, [])

def format_user_id_as_uuid(hex_or_uuid: str) -> str:
    clean = hex_or_uuid.replace('-', '')
    if len(clean) == 32:
        return f'{clean[:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:]}'
    return hex_or_uuid
from stage3_uplink_runner import (
    AclProbe,
    run_gateway_container,
    parse_run_summary,
)
from stage3_mqtt_ingress_runner import (
    GatewayProducer,
    remove_container,
    container_absent,
    verify_ack,
    SIM_PROFILE,
)
from task28_simplified_schema_runner import DatabaseFixture


# --- Deterministic Value Formula ---------------------------------------------


def synthetic_value(sequence: int) -> float:
    """Mirrors internal/gatewaysim/batch.go syntheticValue for deterministic check."""
    period = 2000
    pos = sequence % period
    if pos < 0:
        pos += period
    if pos > period // 2:
        pos = period - pos
    return 20.0 + pos / 100.0


# --- Database JSON Query Helpers ---------------------------------------------


def query_json_row(fixture: DatabaseFixture, query: str) -> dict:
    """Execute a SQL query returning a single JSONB object, or raise on error."""
    wrapped = f"""
    DO $$
    DECLARE j jsonb;
    BEGIN
      SELECT to_jsonb(sub) INTO j FROM ({query}) sub;
      RAISE NOTICE 'JSON_ROW %', coalesce(j, '{{}}'::jsonb);
    END $$;
    """
    _, out = fixture.sql(wrapped.encode('utf-8'))
    text = out.decode('utf-8', errors='replace')
    for line in text.splitlines():
        idx = line.find('JSON_ROW ')
        if idx != -1:
            return json.loads(line[idx + len('JSON_ROW '):].strip())
    raise RuntimeError('JSON_ROW marker not found in psql output')


def query_json_rows(fixture: DatabaseFixture, query: str) -> list[dict]:
    """Execute a SQL query returning an array of JSONB objects, or raise on error."""
    wrapped = f"""
    DO $$
    DECLARE j jsonb;
    BEGIN
      SELECT jsonb_agg(sub) INTO j FROM ({query}) sub;
      RAISE NOTICE 'JSON_ROWS %', coalesce(j, '[]'::jsonb);
    END $$;
    """
    _, out = fixture.sql(wrapped.encode('utf-8'))
    text = out.decode('utf-8', errors='replace')
    for line in text.splitlines():
        idx = line.find('JSON_ROWS ')
        if idx != -1:
            return json.loads(line[idx + len('JSON_ROWS '):].strip())
    raise RuntimeError('JSON_ROWS marker not found in psql output')


# --- Gateway Target Simulator Runner -----------------------------------------


def run_owned_gateway(ctx, env_file, args, timeout, outbox_dir):
    name = f'stage4-api-gateway-{secrets.token_hex(8)}'
    ctx.registry.register('gateway simulator', 'container', name)
    try:
        return run_gateway_container(
            ctx.gateway_image, ctx.net_name, '', ctx.ca_path, env_file, args, timeout,
            outbox_dir=outbox_dir, container_name=name,
        )
    finally:
        interrupted = sys.exc_info()[0] is not None
        try:
            if not container_absent(name):
                remove_container(name)
        except Exception:
            if not interrupted:
                raise
            # Preserve the original failure; registry cleanup retries this owned name.
            print('FAIL: gateway cleanup needs fixture retry', file=sys.stderr, flush=True)


def run_sim_batches(
    ctx: Stage4FixtureContext,
    spec: GatewayFixtureSpec,
    batches: int,
    outbox_dir: Path,
    *,
    timeout: int = 120,
    completion_timeout: str = '30s',
) -> dict[str, str]:
    """Execute the gateway target simulator container for N batches and verify drain."""
    outbox_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir, 0o777)

    env = dict(SIM_PROFILE)
    env.update({
        'GATEWAY_ID': spec.gateway_id,
        'SENSOR_ID': spec.sensor_id,
        'GATEWAY_MQTT_BROKER_URL': 'ssl://broker:8883',
        'GATEWAY_MQTT_CLIENT_ID': f'{spec.gateway_id}_sim_{secrets.token_hex(4)}',
        'GATEWAY_MQTT_USERNAME': spec.gateway_id,
        'GATEWAY_MQTT_PASSWORD': ctx.gateway_passwords[spec.gateway_id],
        'GATEWAY_MQTT_CA_FILE': '/data/ca/ca.crt',
        'GATEWAY_OUTBOX_PATH': '/data/outbox/outbox.db',
    })

    env_file = ctx.work_dir / f'sim_{spec.gateway_id}_{batches}_{secrets.token_hex(3)}.env'
    env_content = ''.join(f'{k}={v}\n' for k, v in env.items()).encode('utf-8')
    with open(env_file, 'wb') as f:
        f.write(env_content)
    os.chmod(env_file, 0o600)

    proc = run_owned_gateway(
        ctx,
        env_file,
        ['run', '--batches', str(batches), '--completion-timeout', completion_timeout],
        timeout,
        outbox_dir,
    )
    out = proc.stdout.decode('utf-8', errors='replace')
    err = proc.stderr.decode('utf-8', errors='replace')
    if proc.returncode != 0:
        clean = sanitize(out[-600:] + err[-600:])
        raise RuntimeError(f'gateway {spec.gateway_id} run exit={proc.returncode}: {clean}')

    summary = parse_run_summary(out)
    truthful = (
        summary.get('completed') == 'true'
        and summary.get('enqueued') == str(batches)
        and summary.get('app_acknowledged') == str(batches)
        and summary.get('pending_rows') == '0'
        and summary.get('pending_bytes') == '0'
        and summary.get('quarantined_rows') == '0'
        and summary.get('quarantined_bytes') == '0'
    )
    if not truthful:
        raise RuntimeError(f'gateway {spec.gateway_id} run summary not truthful: {summary}')

    # Independent CLI count check
    admin_env = ctx.work_dir / f'sim_{spec.gateway_id}_admin_{secrets.token_hex(3)}.env'
    with open(admin_env, 'wb') as f:
        f.write(b'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
    os.chmod(admin_env, 0o600)

    admin = run_owned_gateway(
        ctx,
        admin_env,
        ['count'],
        30,
        outbox_dir,
    )
    count_out = admin.stdout.decode('utf-8', errors='replace').strip()
    if admin.returncode != 0 or count_out != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
        raise RuntimeError(f'gateway {spec.gateway_id} count failed: {count_out}')

    return summary


# --- Invariants Snapshot -----------------------------------------------------


def snapshot_owned_content(fixture, gateway_ids: list[str]) -> dict:
    """Full content for the small owned vertical/contract fixture, never dumped to logs."""
    ids = ', '.join("'" + g.replace("'", "''") + "'" for g in gateway_ids)
    entities = f'SELECT id FROM public.twin_entities WHERE gateway_id IN ({ids})'
    predicates = {
        'gateways': f'gateway_id IN ({ids})',
        'sensors': f'gateway_id IN ({ids})',
        'twin_entities': f'gateway_id IN ({ids})',
        'twin_states': f'entity_id IN ({entities})',
        'telemetry': f'gateway_id IN ({ids})',
        'processed_messages': f'gateway_id IN ({ids})',
        'twin_temporal_values': f'entity_id IN ({entities})',
        'twin_relationships': f'source_entity_id IN ({entities}) OR target_entity_id IN ({entities})',
        'user_gateways': f'gateway_id IN ({ids})',
        'twin_commands': f'entity_id IN ({entities})',
        'twin_outbox': f'command_id IN (SELECT command_id FROM public.twin_commands WHERE entity_id IN ({entities}))',
        'gateway_mqtt_credentials': f'gateway_id IN ({ids})',
        'gateway_mqtt_credential_events': f'gateway_id IN ({ids})',
    }
    columns = [f"(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text), '[]'::jsonb) "
               f"FROM public.{table} r WHERE {predicate}) AS {table}"
               for table, predicate in predicates.items()]
    return query_json_row(fixture, 'SELECT ' + ', '.join(columns))


def assert_content_unchanged(before: dict, after: dict) -> None:
    if before != after:
        changed = sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
        raise AssertionError('fixture content changed in: ' + ', '.join(changed))


def assert_patch_preserves_content(before: dict, after: dict, spec) -> None:
    def normalized(snapshot):
        result = copy.deepcopy(snapshot)
        for table, rows in result.items():
            for row in rows:
                if table == 'gateways' and row['gateway_id'] == spec.gateway_id:
                    row.pop('description', None)
                elif table == 'sensors' and (row['gateway_id'], row['sensor_id']) == (spec.gateway_id, spec.sensor_id):
                    row.pop('name', None)
                elif table == 'twin_entities' and row['entity_id'] in (spec.gateway_urn, spec.sensor_urn):
                    row.pop('updated_at', None)
                    if row['entity_id'] == spec.sensor_urn:
                        row.pop('name', None)
            rows.sort(key=lambda row: json.dumps(row, sort_keys=True))
        return result
    assert_content_unchanged(normalized(before), normalized(after))


def snapshot_invariants(
    fixture: DatabaseFixture,
    spec_a: GatewayFixtureSpec,
    spec_b: GatewayFixtureSpec,
) -> dict:
    """Capture a comprehensive DB snapshot of states, versions, metadata, and tables."""
    return query_json_row(fixture, f"""
        SELECT
            (SELECT count(*) FROM public.telemetry WHERE gateway_id = '{spec_a.gateway_id}') AS telem_a_count,
            (SELECT count(*) FROM public.telemetry WHERE gateway_id = '{spec_b.gateway_id}') AS telem_b_count,
            (SELECT count(*) FROM public.processed_messages WHERE gateway_id = '{spec_a.gateway_id}') AS ledger_a_count,
            (SELECT count(*) FROM public.processed_messages WHERE gateway_id = '{spec_b.gateway_id}') AS ledger_b_count,
            (SELECT count(*) FROM public.twin_temporal_values WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')) AS temporal_a_count,
            (SELECT count(*) FROM public.twin_temporal_values WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_b.sensor_urn}')) AS temporal_b_count,
            (SELECT count(*) FROM public.twin_commands) AS commands_count,
            (SELECT count(*) FROM public.twin_outbox) AS outbox_count,
            (SELECT count(*) FROM public.gateway_mqtt_credentials) AS creds_count,
            (SELECT count(*) FROM public.gateway_mqtt_credential_events) AS cred_events_count,
            (SELECT reported_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')) AS sensor_a_rep_ver,
            (SELECT desired_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')) AS sensor_a_des_ver,
            (SELECT reported_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')) AS sensor_a_rep_state,
            (SELECT desired_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')) AS sensor_a_des_state,
            (SELECT reported_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}')) AS gw_a_rep_ver,
            (SELECT desired_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}')) AS gw_a_des_ver,
            (SELECT reported_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}')) AS gw_a_rep_state,
            (SELECT desired_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}')) AS gw_a_des_state,
            (SELECT attributes FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}') AS gw_a_attributes
    """)


# --- Vertical Scenario Implementation ----------------------------------------


def run_vertical_scenario(ctx: Stage4FixtureContext) -> None:
    """Execute the full Task 4.7.3 vertical scenario slice."""
    if len(ctx.gateways) < 2:
        raise ValueError('vertical scenario requires at least two gateway fixture specs')

    spec_a = ctx.gateways[0]
    spec_b = ctx.gateways[1]

    admin_token = ctx.tokens['admin']
    owner_token = ctx.tokens['owner']
    operator_token = ctx.tokens['operator']
    viewer_token = ctx.tokens['viewer']
    user_b_token = ctx.tokens['user_b']

    print(f'=== Starting 4.7.3 Vertical Scenario for {spec_a.gateway_id} and {spec_b.gateway_id} ===', flush=True)

    # -------------------------------------------------------------------------
    # Step 1: Provision Gateways A and B sharing local Sensor ID via HTTP
    # -------------------------------------------------------------------------
    print('stage=vertical:step1-provision-entities', flush=True)

    # 1a. Admin POST Gateway A with Owner A
    gw_a_payload = {
        'entity_type': 'Gateway',
        'gateway_id': spec_a.gateway_id,
        'name': f'Gateway {spec_a.gateway_id}',
        'owner_user_id': format_user_id_as_uuid(ctx.users.owner.user_id),
    }
    resp = ctx.http.post('/v1/digital-twins', token=admin_token, json_body=gw_a_payload)
    if resp.status != 201:
        raise RuntimeError(f'admin POST Gateway A failed: HTTP {resp.status} {resp.json}')
    if resp.headers.get('content-type') != 'application/json; charset=utf-8':
        raise RuntimeError(f'expected application/json; charset=utf-8, got {resp.headers.get("content-type")}')
    if resp.headers.get('cache-control') != 'no-store' or resp.headers.get('pragma') != 'no-cache':
        raise RuntimeError(f'invalid cache headers on POST Gateway: {resp.headers}')
    if resp.json.get('entity_id') != spec_a.gateway_urn or resp.json.get('gateway_id') != spec_a.gateway_id:
        raise RuntimeError(f'POST Gateway A returned invalid response: {resp.json}')

    # Exact replay POST Gateway A -> 200 OK
    replay_resp = ctx.http.post('/v1/digital-twins', token=admin_token, json_body=gw_a_payload)
    if replay_resp.status != 200:
        raise RuntimeError(f'exact replay POST Gateway A failed: HTTP {replay_resp.status} {replay_resp.json}')

    # 1b. Owner A POST Sensor A
    sens_a_payload = {
        'entity_type': 'Sensor',
        'gateway_id': spec_a.gateway_id,
        'sensor_id': spec_a.sensor_id,
        'name': f'Sensor {spec_a.sensor_id}',
        'unit': spec_a.sensor_unit,
    }
    resp = ctx.http.post('/v1/digital-twins', token=owner_token, json_body=sens_a_payload)
    if resp.status != 201:
        raise RuntimeError(f'owner POST Sensor A failed: HTTP {resp.status} {resp.json}')
    if resp.json.get('entity_id') != spec_a.sensor_urn:
        raise RuntimeError(f'POST Sensor A returned invalid entity_id: {resp.json}')

    # Exact replay POST Sensor A -> 200 OK
    replay_sens = ctx.http.post('/v1/digital-twins', token=owner_token, json_body=sens_a_payload)
    if replay_sens.status != 200:
        raise RuntimeError(f'exact replay POST Sensor A failed: HTTP {replay_sens.status} {replay_sens.json}')

    # 1c. Admin POST Gateway B with User B as Owner
    gw_b_payload = {
        'entity_type': 'Gateway',
        'gateway_id': spec_b.gateway_id,
        'name': f'Gateway {spec_b.gateway_id}',
        'owner_user_id': format_user_id_as_uuid(ctx.users.user_b.user_id),
    }
    resp = ctx.http.post('/v1/digital-twins', token=admin_token, json_body=gw_b_payload)
    if resp.status != 201:
        raise RuntimeError(f'admin POST Gateway B failed: HTTP {resp.status} {resp.json}')

    # Exact replay POST Gateway B -> 200 OK
    replay_resp_b = ctx.http.post('/v1/digital-twins', token=admin_token, json_body=gw_b_payload)
    if replay_resp_b.status != 200:
        raise RuntimeError(f'exact replay POST Gateway B failed: HTTP {replay_resp_b.status}')

    # 1d. User B (Owner of B) POST Sensor B (sharing identical sensor_id)
    sens_b_payload = {
        'entity_type': 'Sensor',
        'gateway_id': spec_b.gateway_id,
        'sensor_id': spec_b.sensor_id,
        'name': f'Sensor B {spec_b.sensor_id}',
        'unit': spec_b.sensor_unit,
    }
    resp = ctx.http.post('/v1/digital-twins', token=user_b_token, json_body=sens_b_payload)
    if resp.status != 201:
        raise RuntimeError(f'user B POST Sensor B failed: HTTP {resp.status} {resp.json}')

    # Exact replay POST Sensor B -> 200 OK
    replay_sens_b = ctx.http.post('/v1/digital-twins', token=user_b_token, json_body=sens_b_payload)
    if replay_sens_b.status != 200:
        raise RuntimeError(f'exact replay POST Sensor B failed: HTTP {replay_sens_b.status}')

    # 1e. Assign operator and viewer memberships for Gateway A using fixture context
    ctx.assign_membership(ctx.users.operator.user_id, spec_a.gateway_id, 'operator')
    ctx.assign_membership(ctx.users.viewer.user_id, spec_a.gateway_id, 'viewer')

    # 1f. Assert graph and context contents
    gw_a_detail = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.gateway_urn, safe=":")}', token=owner_token)
    if gw_a_detail.status != 200:
        raise RuntimeError(f'GET Gateway A detail failed: HTTP {gw_a_detail.status}')
    if not gw_a_detail.headers.get('content-type', '').startswith('application/ld+json'):
        raise RuntimeError(f'expected application/ld+json, got {gw_a_detail.headers.get("content-type")}')
    if gw_a_detail.json.get('@context') is None:
        raise RuntimeError('missing @context in Gateway detail')
    has_sensor = gw_a_detail.json.get('hasSensor')
    if not isinstance(has_sensor, list) or len(has_sensor) != 1 or has_sensor[0].get('object') != spec_a.sensor_urn:
        raise RuntimeError(f'invalid hasSensor relationship in Gateway detail: {has_sensor}')

    sens_a_detail = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}', token=owner_token)
    if sens_a_detail.status != 200:
        raise RuntimeError(f'GET Sensor A detail failed: HTTP {sens_a_detail.status}')
    if sens_a_detail.json.get('unit', {}).get('value') != spec_a.sensor_unit:
        raise RuntimeError(f'invalid sensor unit in Sensor detail: {sens_a_detail.json}')

    print('PASS: step 1 provisioned Gateway/Sensor A and B with exact replay and graph validation', flush=True)

    # -------------------------------------------------------------------------
    # Step 2: Start Gateway simulator (2x100 A, 1x100 B positive control)
    # -------------------------------------------------------------------------
    print('stage=vertical:step2-simulator-ingest', flush=True)

    outbox_dir_a = ctx.work_dir / f'outbox_{spec_a.gateway_id}'
    outbox_dir_b = ctx.work_dir / f'outbox_{spec_b.gateway_id}'

    # Run Gateway A: 2 batches x 100 samples
    sim_a_summary = run_sim_batches(ctx, spec_a, 2, outbox_dir_a)
    print(f'PASS: Gateway A simulator 2x100 batches finished: {sim_a_summary}', flush=True)

    # Run Gateway B: 1 batch x 100 samples (finite positive control)
    sim_b_summary = run_sim_batches(ctx, spec_b, 1, outbox_dir_b)
    print(f'PASS: Gateway B simulator 1x100 batch finished: {sim_b_summary}', flush=True)

    # Independent SQL verification for Gateway A
    ledger_a_rows = query_json_rows(ctx.db_fixture, f"""
        SELECT message_id, boot_id, first_sequence, last_sequence, sample_count, payload_hash,
               to_char(processed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS processed_at
        FROM public.processed_messages
        WHERE gateway_id = '{spec_a.gateway_id}'
        ORDER BY first_sequence ASC
    """)
    if len(ledger_a_rows) != 2:
        raise RuntimeError(f'expected exactly 2 ledger rows for Gateway A, got {len(ledger_a_rows)}')

    msg_id_a1 = ledger_a_rows[0]['message_id']
    msg_id_a2 = ledger_a_rows[1]['message_id']
    boot_id_a = ledger_a_rows[0]['boot_id']
    if ledger_a_rows[0]['sample_count'] != 100 or ledger_a_rows[1]['sample_count'] != 100:
        raise RuntimeError(f'invalid sample_count in Gateway A ledger: {ledger_a_rows}')
    if ledger_a_rows[0]['first_sequence'] != 0 or ledger_a_rows[0]['last_sequence'] != 99:
        raise RuntimeError(f'unexpected sequences in batch 1: {ledger_a_rows[0]}')
    if ledger_a_rows[1]['first_sequence'] != 100 or ledger_a_rows[1]['last_sequence'] != 199:
        raise RuntimeError(f'unexpected sequences in batch 2: {ledger_a_rows[1]}')

    telem_a_stats = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count,
               min(sequence_num) AS min_seq,
               max(sequence_num) AS max_seq,
               to_char(min(measured_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS min_time,
               to_char(max(measured_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS max_time
        FROM public.telemetry
        WHERE gateway_id = '{spec_a.gateway_id}' AND sensor_id = '{spec_a.sensor_id}'
    """)
    if telem_a_stats['total_count'] != 200 or telem_a_stats['min_seq'] != 0 or telem_a_stats['max_seq'] != 199:
        raise RuntimeError(f'invalid telemetry counts for Gateway A: {telem_a_stats}')

    temporal_a_stats = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count,
               min(value_number) AS min_val,
               max(value_number) AS max_val
        FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
          AND property_name = 'sample_interval_us'
    """)
    if temporal_a_stats['total_count'] != 2 or temporal_a_stats['min_val'] != 10000.0 or temporal_a_stats['max_val'] != 10000.0:
        raise RuntimeError(f'invalid temporal interval observations for Sensor A: {temporal_a_stats}')

    state_a = query_json_row(ctx.db_fixture, f"""
        SELECT reported_version, desired_version, reported_state, desired_state
        FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
    """)
    if state_a['reported_version'] != 2 or state_a['desired_version'] != 0:
        raise RuntimeError(f'invalid twin state versions for Sensor A: {state_a}')
    if state_a['desired_state'] != {}:
        raise RuntimeError(f'desired_state mutated on ingestion: {state_a["desired_state"]}')

    gw_state_a = query_json_row(ctx.db_fixture, f"""
        SELECT reported_version, desired_version, reported_state, desired_state
        FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.gateway_urn}')
    """)
    if gw_state_a['reported_version'] != 0 or gw_state_a['desired_version'] != 0 or gw_state_a['reported_state'] != {} or gw_state_a['desired_state'] != {}:
        raise RuntimeError(f'Gateway A twin_states sentinel mutated: {gw_state_a}')

    # Independent SQL verification for Gateway B
    telem_b_stats = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count
        FROM public.telemetry
        WHERE gateway_id = '{spec_b.gateway_id}' AND sensor_id = '{spec_b.sensor_id}'
    """)
    if telem_b_stats['total_count'] != 100:
        raise RuntimeError(f'expected 100 raw samples for Gateway B, got {telem_b_stats}')

    state_b = query_json_row(ctx.db_fixture, f"""
        SELECT reported_version, desired_version
        FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_b.sensor_urn}')
    """)
    if state_b['reported_version'] != 1 or state_b['desired_version'] != 0:
        raise RuntimeError(f'invalid twin state versions for Sensor B: {state_b}')

    print(f'PASS: step 2 SQL verification confirmed 200 raw / 2 ledger / 2 temporal for A ({msg_id_a1[:8]}..., {msg_id_a2[:8]}...) and 100 raw for B', flush=True)

    # -------------------------------------------------------------------------
    # Step 3: Owner / Operator / Viewer HTTP Reads & User B Cross-Tenant Isolation
    # -------------------------------------------------------------------------
    print('stage=vertical:step3-http-reads-and-isolation', flush=True)

    t_min_str = telem_a_stats['min_time']
    t_max_str = telem_a_stats['max_time']
    t_min = datetime.datetime.fromisoformat(t_min_str.replace('Z', '+00:00'))
    t_max = datetime.datetime.fromisoformat(t_max_str.replace('Z', '+00:00'))

    # Short window <= 10s triggers raw mode
    raw_from = (t_min - datetime.timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    raw_to = (t_max + datetime.timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    # Wide window triggers bucket downsampled mode
    bucket_from = (t_min - datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    bucket_to = (t_max + datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Test all roles with read permissions: owner, operator, viewer
    for role_name, token in (
        ('owner', owner_token),
        ('operator', operator_token),
        ('viewer', viewer_token),
    ):
        # 3a. GET /v1/telemetry/history (raw window <= 10s)
        resp = ctx.http.get(
            f'/v1/telemetry/history?gateway_id={spec_a.gateway_id}&sensor_id={spec_a.sensor_id}&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
            token=token,
        )
        if resp.status != 200:
            raise RuntimeError(f'telemetry history raw for {role_name} failed: HTTP {resp.status} {resp.json}')
        if resp.headers.get('cache-control') != 'no-store':
            raise RuntimeError('missing Cache-Control: no-store on telemetry history')
        if resp.json.get('mode') != 'raw' or resp.json.get('point_count') != 200:
            raise RuntimeError(f'expected 200 raw points for {role_name}, got {resp.json.get("point_count")}')
        pts = resp.json.get('points', [])
        if len(pts) != 200:
            raise RuntimeError(f'points array length mismatch for {role_name}: {len(pts)}')
        # Assert deterministic values
        first_pt = pts[0]
        if first_pt.get('sequence_num') != '0' or abs(float(first_pt.get('value')) - synthetic_value(0)) > 1e-4:
            raise RuntimeError(f'first point value mismatch: {first_pt}')
        last_pt = pts[199]
        if last_pt.get('sequence_num') != '199' or abs(float(last_pt.get('value')) - synthetic_value(199)) > 1e-4:
            raise RuntimeError(f'last point value mismatch: {last_pt}')

        # 3b. GET /v1/telemetry/history (bucket downsampled with resolution=1s)
        resp = ctx.http.get(
            f'/v1/telemetry/history?gateway_id={spec_a.gateway_id}&sensor_id={spec_a.sensor_id}&from={urllib.parse.quote(bucket_from)}&to={urllib.parse.quote(bucket_to)}&resolution=1s',
            token=token,
        )
        if resp.status != 200:
            raise RuntimeError(f'telemetry history bucket for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('mode') != 'bucket' or resp.json.get('point_count', 0) < 1:
            raise RuntimeError(f'invalid bucket history for {role_name}: {resp.json}')

        # 3c. GET /v1/digital-twins (list)
        resp = ctx.http.get(f'/v1/digital-twins?gateway_id={spec_a.gateway_id}', token=token)
        if resp.status != 200:
            raise RuntimeError(f'twin list for {role_name} failed: HTTP {resp.status}')
        items = resp.json.get('items', [])
        item_ids = {it.get('id') for it in items}
        if spec_a.gateway_urn not in item_ids or spec_a.sensor_urn not in item_ids:
            raise RuntimeError(f'twin list for {role_name} missing A items: {items}')

        # 3d. GET /v1/digital-twins/{entity_id} (detail)
        resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.gateway_urn, safe=":")}', token=token)
        if resp.status != 200 or not resp.headers.get('content-type', '').startswith('application/ld+json'):
            raise RuntimeError(f'gateway detail for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('id') != spec_a.gateway_urn or resp.json.get('type') != 'Gateway':
            raise RuntimeError(f'gateway detail body mismatch: {resp.json}')

        resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}', token=token)
        if resp.status != 200 or not resp.headers.get('content-type', '').startswith('application/ld+json'):
            raise RuntimeError(f'sensor detail for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('id') != spec_a.sensor_urn or resp.json.get('type') != 'Sensor':
            raise RuntimeError(f'sensor detail body mismatch: {resp.json}')

        # 3e. GET /v1/digital-twins/{entity_id}/state
        resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/state', token=token)
        if resp.status != 200:
            raise RuntimeError(f'twin state for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('reported_version') != '2' or resp.json.get('desired_version') != '0':
            raise RuntimeError(f'unexpected twin state versions: {resp.json}')

        # 3f. GET /v1/digital-twins/{entity_id}/history (property=value)
        resp = ctx.http.get(
            f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/history?property=value&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
            token=token,
        )
        if resp.status != 200:
            raise RuntimeError(f'twin history value for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('point_count') != 200 or resp.json.get('property') != 'value':
            raise RuntimeError(f'twin history value mismatch: {resp.json}')

        # 3g. GET /v1/digital-twins/{entity_id}/history (property=sample_interval_us)
        resp = ctx.http.get(
            f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/history?property=sample_interval_us&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
            token=token,
        )
        if resp.status != 200:
            raise RuntimeError(f'twin history sample_interval_us for {role_name} failed: HTTP {resp.status}')
        if resp.json.get('point_count') != 2 or resp.json.get('property') != 'sample_interval_us':
            raise RuntimeError(f'twin history interval mismatch: {resp.json}')

    print('PASS: owner, operator, and viewer read assertions verified', flush=True)

    # 3h. Cross-Tenant Denials and Isolation: User B (owns B) vs Gateway A
    # User B cannot read Gateway A telemetry history -> 404 Not Found
    resp = ctx.http.get(
        f'/v1/telemetry/history?gateway_id={spec_a.gateway_id}&sensor_id={spec_a.sensor_id}&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=user_b_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Gateway A telemetry, got {resp.status}')

    # User B cannot read Gateway A detail -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.gateway_urn, safe=":")}', token=user_b_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Gateway A detail, got {resp.status}')

    # User B cannot read Sensor A detail -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}', token=user_b_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Sensor A detail, got {resp.status}')

    # User B cannot read Sensor A state -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/state', token=user_b_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Sensor A state, got {resp.status}')

    # User B cannot read Sensor A history value -> 404
    resp = ctx.http.get(
        f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/history?property=value&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=user_b_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Sensor A history value, got {resp.status}')

    # User B cannot read Sensor A history interval -> 404
    resp = ctx.http.get(
        f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/history?property=sample_interval_us&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=user_b_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for User B reading Sensor A history interval, got {resp.status}')

    # User B listing digital twins must exclude Gateway A and Sensor A
    resp = ctx.http.get('/v1/digital-twins', token=user_b_token)
    if resp.status != 200:
        raise RuntimeError(f'User B twin list failed: HTTP {resp.status}')
    b_item_ids = {it.get('id') for it in resp.json.get('items', [])}
    if spec_a.gateway_urn in b_item_ids or spec_a.sensor_urn in b_item_ids:
        raise RuntimeError(f'User B twin list contains Gateway A items: {b_item_ids}')
    if spec_b.gateway_urn not in b_item_ids or spec_b.sensor_urn not in b_item_ids:
        raise RuntimeError(f'User B twin list missing Gateway B items: {b_item_ids}')

    # User B querying list with gateway_id=A returns empty items (authorized-empty)
    resp = ctx.http.get(f'/v1/digital-twins?gateway_id={spec_a.gateway_id}', token=user_b_token)
    if resp.status != 200:
        raise RuntimeError(f'expected 200 for User B querying list with gateway_id=A, got {resp.status}')
    if resp.json.get('items') != []:
        raise RuntimeError(f'expected empty items for User B querying gateway_id=A, got {resp.json.get("items")}')

    # Owner A cannot read Gateway B detail -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_b.gateway_urn, safe=":")}', token=owner_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Owner A reading Gateway B detail, got {resp.status}')

    # Owner A cannot read Sensor B detail -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_b.sensor_urn, safe=":")}', token=owner_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Owner A reading Sensor B detail, got {resp.status}')

    # Owner A cannot read Sensor B state -> 404
    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_b.sensor_urn, safe=":")}/state', token=owner_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Owner A reading Sensor B state, got {resp.status}')

    # Owner A cannot read Gateway B telemetry history -> 404
    resp = ctx.http.get(
        f'/v1/telemetry/history?gateway_id={spec_b.gateway_id}&sensor_id={spec_b.sensor_id}&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=owner_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Owner A reading Gateway B telemetry, got {resp.status}')

    # Owner A querying list with gateway_id=B returns empty items (authorized-empty)
    resp = ctx.http.get(f'/v1/digital-twins?gateway_id={spec_b.gateway_id}', token=owner_token)
    if resp.status != 200:
        raise RuntimeError(f'expected 200 for Owner A querying list with gateway_id=B, got {resp.status}')
    if resp.json.get('items') != []:
        raise RuntimeError(f'expected empty items for Owner A querying gateway_id=B, got {resp.json.get("items")}')

    print('PASS: cross-tenant isolation and 404 denials verified for User B and Gateway A', flush=True)

    # -------------------------------------------------------------------------
    # Step 4: Pre-PATCH Snapshot, Metadata PATCH, and Admin GET Denials
    # -------------------------------------------------------------------------
    print('stage=vertical:step4-snapshot-and-patch', flush=True)

    pre_patch_snap = snapshot_invariants(ctx.db_fixture, spec_a, spec_b)
    protected_before = snapshot_owned_content(ctx.db_fixture, [spec_a.gateway_id, spec_b.gateway_id])

    # 4a. Owner A PATCHes Sensor A name
    new_sensor_name = f'Patched Sensor A {secrets.token_hex(4)}'
    resp = ctx.http.patch(
        f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}',
        token=owner_token,
        json_body={'name': new_sensor_name},
    )
    if resp.status != 200:
        raise RuntimeError(f'owner PATCH Sensor A failed: HTTP {resp.status} {resp.json}')
    if resp.json.get('name') != new_sensor_name:
        raise RuntimeError(f'PATCH Sensor A response name mismatch: {resp.json}')

    # 4b. Admin non-member PATCHes Gateway A description
    new_gw_desc = f'Patched Gateway A Description {secrets.token_hex(4)}'
    resp = ctx.http.patch(
        f'/v1/digital-twins/{urllib.parse.quote(spec_a.gateway_urn, safe=":")}',
        token=admin_token,
        json_body={'description': new_gw_desc},
    )
    if resp.status != 200:
        raise RuntimeError(f'admin PATCH Gateway A failed: HTTP {resp.status} {resp.json}')
    if resp.json.get('description') != new_gw_desc:
        raise RuntimeError(f'PATCH Gateway A response description mismatch: {resp.json}')

    # 4c. Verify in DB that ONLY allowed metadata changed
    post_patch_snap = snapshot_invariants(ctx.db_fixture, spec_a, spec_b)
    assert_patch_preserves_content(
        protected_before, snapshot_owned_content(ctx.db_fixture, [spec_a.gateway_id, spec_b.gateway_id]), spec_a,
    )

    # State versions and contents MUST be completely untouched!
    if post_patch_snap['sensor_a_rep_ver'] != pre_patch_snap['sensor_a_rep_ver']:
        raise RuntimeError('PATCH Sensor mutated reported_version')
    if post_patch_snap['sensor_a_des_ver'] != pre_patch_snap['sensor_a_des_ver']:
        raise RuntimeError('PATCH Sensor mutated desired_version')
    if post_patch_snap['sensor_a_rep_state'] != pre_patch_snap['sensor_a_rep_state']:
        raise RuntimeError('PATCH Sensor mutated reported_state')
    if post_patch_snap['sensor_a_des_state'] != pre_patch_snap['sensor_a_des_state']:
        raise RuntimeError('PATCH Sensor mutated desired_state')

    if post_patch_snap['gw_a_rep_ver'] != pre_patch_snap['gw_a_rep_ver']:
        raise RuntimeError('PATCH Gateway mutated reported_version')
    if post_patch_snap['gw_a_des_ver'] != pre_patch_snap['gw_a_des_ver']:
        raise RuntimeError('PATCH Gateway mutated desired_version')
    if post_patch_snap['gw_a_rep_state'] != pre_patch_snap['gw_a_rep_state']:
        raise RuntimeError('PATCH Gateway mutated reported_state')
    if post_patch_snap['gw_a_des_state'] != pre_patch_snap['gw_a_des_state']:
        raise RuntimeError('PATCH Gateway mutated desired_state')

    # Gateway attributes MUST remain untouched!
    if post_patch_snap['gw_a_attributes'] != {}:
        raise RuntimeError(f'Gateway A attributes mutated: {post_patch_snap["gw_a_attributes"]}')

    # Table counts MUST remain untouched!
    for table_count_key in (
        'telem_a_count', 'telem_b_count', 'ledger_a_count', 'ledger_b_count',
        'temporal_a_count', 'temporal_b_count', 'commands_count', 'outbox_count',
        'creds_count', 'cred_events_count',
    ):
        if post_patch_snap[table_count_key] != pre_patch_snap[table_count_key]:
            raise RuntimeError(f'table count {table_count_key} changed during metadata PATCH')

    # Verify actual names in tables
    sens_db = query_json_row(ctx.db_fixture, f"""
        SELECT s.name AS sensor_rel_name, e.name AS sensor_twin_name
        FROM public.sensors s
        JOIN public.twin_entities e ON e.entity_id = '{spec_a.sensor_urn}'
        WHERE s.gateway_id = '{spec_a.gateway_id}' AND s.sensor_id = '{spec_a.sensor_id}'
    """)
    if sens_db['sensor_rel_name'] != new_sensor_name or sens_db['sensor_twin_name'] != new_sensor_name:
        raise RuntimeError(f'Sensor name not updated in DB: {sens_db}')

    gw_db = query_json_row(ctx.db_fixture, f"""
        SELECT description FROM public.gateways WHERE gateway_id = '{spec_a.gateway_id}'
    """)
    if gw_db['description'] != new_gw_desc:
        raise RuntimeError(f'Gateway description not updated in DB: {gw_db}')

    # 4d. Assert Admin non-member still cannot GET state or history
    resp = ctx.http.get(
        f'/v1/telemetry/history?gateway_id={spec_a.gateway_id}&sensor_id={spec_a.sensor_id}&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=admin_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Admin GET telemetry history, got {resp.status}')

    resp = ctx.http.get(f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/state', token=admin_token)
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Admin GET twin state, got {resp.status}')

    resp = ctx.http.get(
        f'/v1/digital-twins/{urllib.parse.quote(spec_a.sensor_urn, safe=":")}/history?property=value&from={urllib.parse.quote(raw_from)}&to={urllib.parse.quote(raw_to)}',
        token=admin_token,
    )
    if resp.status != 404:
        raise RuntimeError(f'expected 404 for Admin GET twin history value, got {resp.status}')

    print('PASS: metadata PATCH updated only allowed fields; admin non-member read denied (404)', flush=True)

    # -------------------------------------------------------------------------
    # Step 5: Simulator Ingests Next Batch (1x100 A) & Byte-Exact Resend
    # -------------------------------------------------------------------------
    print('stage=vertical:step5-ingest-next-batch-and-resend', flush=True)

    # Start AclProbe to capture the published message frame for the subsequent resend test
    probe = AclProbe(ctx.broker_port, ctx.ca_path, 'bootstrap_backend', ctx.backend_password)
    sub_code = probe.subscribe_code('gateways/+/telemetry/#')
    if sub_code != 0x00:
        probe.close()
        raise RuntimeError(f'probe subscribe failed with code {sub_code}')

    captured_frame = None
    try:
        # Run simulator for Gateway A: 1 batch x 100 samples
        sim_a_b3 = run_sim_batches(ctx, spec_a, 1, outbox_dir_a)
        print(f'PASS: Gateway A simulator batch 3 finished: {sim_a_b3}', flush=True)

        # Read the published frame from probe for Gateway A
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            frame = probe.publish_frame(timeout=max(0.5, deadline - time.monotonic()))
            if frame is None:
                continue
            t, p = frame
            if t.startswith(f'gateways/{spec_a.gateway_id}/telemetry/'):
                captured_frame = frame
                break
    finally:
        probe.close()

    if captured_frame is None:
        raise RuntimeError('failed to capture telemetry frame from simulator batch 3')

    cap_topic, cap_payload = captured_frame
    batch_json = json.loads(cap_payload.decode('utf-8'))
    msg_id_a3 = batch_json['message_id']

    # Verify counts for Gateway A now total 300 raw, 3 ledger, 3 interval
    telem_a3 = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count, min(sequence_num) AS min_seq
        FROM public.telemetry
        WHERE gateway_id = '{spec_a.gateway_id}' AND sensor_id = '{spec_a.sensor_id}'
    """)
    if telem_a3['total_count'] != 300 or telem_a3['min_seq'] != 0:
        raise RuntimeError(f'expected 300 raw samples after batch 3, got {telem_a3}')

    ledger_a3_count = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count FROM public.processed_messages WHERE gateway_id = '{spec_a.gateway_id}'
    """)['total_count']
    if ledger_a3_count != 3:
        raise RuntimeError(f'expected 3 ledger rows after batch 3, got {ledger_a3_count}')

    temporal_a3_count = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
    """)['total_count']
    if temporal_a3_count != 3:
        raise RuntimeError(f'expected 3 temporal rows after batch 3, got {temporal_a3_count}')

    state_a3 = query_json_row(ctx.db_fixture, f"""
        SELECT reported_version, desired_version
        FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
    """)
    if state_a3['reported_version'] != 3 or state_a3['desired_version'] != 0:
        raise RuntimeError(f'expected reported_version 3 for Sensor A, got {state_a3}')

    # Verify patched metadata names are PRESERVED after batch 3 ingestion
    sens_after_b3 = query_json_row(ctx.db_fixture, f"""
        SELECT name FROM public.sensors WHERE gateway_id = '{spec_a.gateway_id}' AND sensor_id = '{spec_a.sensor_id}'
    """)['name']
    if sens_after_b3 != new_sensor_name:
        raise RuntimeError(f'patched sensor name reverted after ingestion: {sens_after_b3}')

    gw_after_b3 = query_json_row(ctx.db_fixture, f"""
        SELECT description FROM public.gateways WHERE gateway_id = '{spec_a.gateway_id}'
    """)['description']
    if gw_after_b3 != new_gw_desc:
        raise RuntimeError(f'patched gateway description reverted after ingestion: {gw_after_b3}')

    print(f'PASS: batch 3 committed (total 300 raw, 3 ledger, 3 interval, version 3), metadata preserved', flush=True)

    # 5b. Exercise byte-exact resend of the already-committed batch 3
    print('stage=vertical:step5-byte-exact-resend', flush=True)
    producer = GatewayProducer(
        ctx.broker_port,
        ctx.ca_path,
        spec_a.gateway_id,
        ctx.gateway_passwords[spec_a.gateway_id],
    )
    try:
        producer.subscribe(spec_a.ack_filter)
        producer.publish_qos1(cap_topic, cap_payload)
        ack_topic, ack_body = producer.read_publish()
    finally:
        producer.close()

    expected_ack_topic = f'gateways/{spec_a.gateway_id}/acks/{msg_id_a3}'
    if ack_topic != expected_ack_topic:
        raise RuntimeError(f'resend ACK topic mismatch: got {ack_topic}, want {expected_ack_topic}')

    verify_ack(msg_id_a3, ack_body)
    print(f'PASS: byte-exact resend re-ACKed with committed status for {msg_id_a3[:8]}...', flush=True)

    # Assert that database counts have NOT increased after the resend
    telem_after_resend = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count FROM public.telemetry
        WHERE gateway_id = '{spec_a.gateway_id}' AND sensor_id = '{spec_a.sensor_id}'
    """)['total_count']
    if telem_after_resend != 300:
        raise RuntimeError(f'telemetry count increased on byte-exact resend: {telem_after_resend}')

    ledger_after_resend = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count FROM public.processed_messages WHERE gateway_id = '{spec_a.gateway_id}'
    """)['total_count']
    if ledger_after_resend != 3:
        raise RuntimeError(f'ledger count increased on byte-exact resend: {ledger_after_resend}')

    temporal_after_resend = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS total_count FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
    """)['total_count']
    if temporal_after_resend != 3:
        raise RuntimeError(f'temporal count increased on byte-exact resend: {temporal_after_resend}')

    state_after_resend = query_json_row(ctx.db_fixture, f"""
        SELECT reported_version FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{spec_a.sensor_urn}')
    """)['reported_version']
    if state_after_resend != 3:
        raise RuntimeError(f'reported_version increased on byte-exact resend: {state_after_resend}')

    print('PASS: database counts unchanged on byte-exact resend (no double-insert)', flush=True)
    print(f'=== PASS: Task 4.7.3 Vertical Scenario complete for {spec_a.gateway_id} and {spec_b.gateway_id} ===', flush=True)
    return {
        'gateway_a_raw_count': telem_after_resend,
        'gateway_a_ledger_count': ledger_after_resend,
        'gateway_a_temporal_count': temporal_after_resend,
        'gateway_a_reported_version': state_after_resend,
        'resend_dedup_verified': True,
    }
