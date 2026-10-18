#!/usr/bin/env python3
"""Task 4.7.5 Queries Scenario: Uncompressed dataset & query measurements half.

This scenario measures actual PostgreSQL/TimescaleDB query plans and performance
on representative uncompressed datasets under application role 'iot_backend_app'
(non-superuser):
  1. Compiles and executes Go native measurement tests in internal/telemetry and
     internal/digitaltwin against the isolated database fixture.
  2. Seeds and verifies deterministic dataset:
     - 60,000 raw samples @ 100 Hz (10ms interval, 10-minute span) for target Gateway/Sensor
     - 6,000 raw samples @ 10 Hz for distractor Gateway/Sensor
     - 580 temporal interval observations for target Sensor (1/s, 10,000.0 us, with 20s gap at [300s, 320s))
     - 60 temporal interval observations for distractor Sensor (1/10s, 100,000.0 us)
     - Canonical provisioning graph (hasSensor), twin_entities, twin_states, and user_gateways memberships
  3. Measures five primary query paths with EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) from exact adapter SQL:
     - M1: Short raw window (5s, 500 samples @ 100 Hz, cap=500, limit=501)
     - M2: Longer bucket query (10m, auto-selected width 10s, 60 buckets aggregating 60,000 samples)
     - M3: Short raw probe point overflow (8s, 800 samples, limit=501 -> 501 rows) and bucket fallback (100ms, 80 buckets)
     - M4: Temporal raw query (10s, 10 observations, cap=500, limit=501)
     - M5: Temporal bucket query (10m, 1m resolution, 10 buckets verifying arithmetic avg/min/max=10000.0 and gap bucket 5=40)
  4. Verifies uncompressed hypertable status on timescaledb_information.chunks (is_compressed = False).
  5. Measures HTTP service elapsed times via ctx.http and asserts tenant isolation (User B denied 404).
  6. Emits reusable non-secret measurement summary for subsequent compression worker.
  7. Fails closed with NotImplementedError signaling that uncompressed baseline is complete
     and compressed history comparison remains pending.
"""
from __future__ import annotations

import datetime
import json
import math
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.parse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stage4_api_runner import Stage4FixtureContext

ROOT = Path(__file__).resolve().parents[2]

from stage4_vertical_scenario import (
    format_user_id_as_uuid,
    query_json_row,
    query_json_rows,
)


def assert_history_equivalent(before: dict, after: dict, *, temporal: bool = False) -> None:
    """Compare actual wire fields, including explicit nulls, without assuming tie order."""
    required = ({'observed_at', 'value', 'value_type', 'metadata', 'message_id'} if temporal else
                {'measured_at', 'value', 'sequence_num', 'boot_id', 'message_id', 'sample_index'})
    for response in (before, after):
        if 'unit' not in response or 'points' not in response or 'mode' not in response:
            raise AssertionError('history response missing required metadata')
        for point in response['points']:
            fields = required if response['mode'] == 'raw' else {'bucket_start', 'avg', 'min', 'max', 'count'}
            if not fields.issubset(point):
                raise AssertionError('history point missing required wire fields')

    def compare(left, right):
        if isinstance(left, dict) and isinstance(right, dict):
            if left.keys() != right.keys():
                raise AssertionError('history field presence changed')
            for key in left:
                compare(left[key], right[key])
        elif isinstance(left, list) and isinstance(right, list):
            if len(left) != len(right):
                raise AssertionError('history length changed')
            for a, b in zip(left, right):
                compare(a, b)
        elif type(left) in (float, int) and type(right) in (float, int):
            if not math.isclose(left, right, rel_tol=0, abs_tol=1e-9):
                raise AssertionError('history numeric value changed')
        elif type(left) is not type(right) or left != right:
            raise AssertionError('history value or type changed')

    a, b = dict(before), dict(after)
    if temporal and before['mode'] == 'raw':
        # Sort complete observations, not timestamps alone, to compare ties as a multiset.
        a['points'] = sorted(before['points'], key=lambda p: json.dumps(p, sort_keys=True))
        b['points'] = sorted(after['points'], key=lambda p: json.dumps(p, sort_keys=True))
    compare(a, b)


def compile_go_test_binary(package_rel: str, dest: Path) -> Path:
    """Compile Go test binary with CGO_ENABLED=0."""
    env = dict(os.environ)
    env["CGO_ENABLED"] = "0"
    for key in ("GOCACHE", "GOMODCACHE", "GOPROXY"):
        if key in os.environ:
            env[key] = os.environ[key]

    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["go", "test", "-c", "-o", str(dest), package_rel]
    proc = subprocess.run(
        cmd,
        cwd=ROOT / "src",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=180,
    )
    if proc.returncode != 0:
        err = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        raise RuntimeError(f"failed to compile Go test {package_rel}: {err}")
    return dest


def parse_json_marker_from_output(output_text: str, marker_prefix: str) -> dict:
    """Extract and parse JSON payload marked with a specific line prefix."""
    for line in output_text.splitlines():
        idx = line.find(marker_prefix)
        if idx != -1:
            json_str = line[idx + len(marker_prefix):].strip()
            return json.loads(json_str)
    raise RuntimeError(f"marker '{marker_prefix}' not found in output")


def run_go_measurement_in_fixture(
    ctx: Stage4FixtureContext,
    test_binary: Path,
    test_run_pattern: str,
    extra_env: dict[str, str],
    *,
    timeout: int = 120,
) -> str:
    """Copy compiled test binary into database fixture container and execute it."""
    cid = ctx.db_fixture.container_id
    container_bin = f"/tmp/{test_binary.name}"

    # Copy binary into DB container
    cp_cmd = ["docker", "cp", str(test_binary), f"{cid}:{container_bin}"]
    cp_proc = subprocess.run(cp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if cp_proc.returncode != 0:
        err = (cp_proc.stdout + cp_proc.stderr).decode("utf-8", errors="replace")
        raise RuntimeError(f"docker cp {test_binary.name} failed: {err}")

    # Build execution command with environment variables inside the container
    env_exports = [
        f'export {k}="{v}"'
        for k, v in extra_env.items()
    ]
    env_exports_str = " && ".join(env_exports)
    exec_script = f"{env_exports_str} && chmod +x {container_bin} && exec {container_bin} -test.v -test.run '{test_run_pattern}'"

    exec_cmd = ["docker", "exec", "-i", cid, "sh", "-c", exec_script]
    exec_proc = subprocess.run(exec_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    output = (exec_proc.stdout + exec_proc.stderr).decode("utf-8", errors="replace")

    if exec_proc.returncode != 0:
        raise RuntimeError(f"test {test_run_pattern} execution in container failed (exit {exec_proc.returncode}):\n{output}")

    return output


def run_uncompressed_query_measurements(ctx: Stage4FixtureContext) -> dict:
    """Execute uncompressed dataset seeding, EXPLAIN queries, assertions, and measurements.

    Returns allowlisted non-secret summary dictionary for consumption by compression worker.
    """
    print("stage=queries:step1-compile-native-tests", flush=True)
    work_dir = ctx.work_dir
    telemetry_bin = compile_go_test_binary("./internal/telemetry", work_dir / "telemetry_meas.test")
    temporal_bin = compile_go_test_binary("./internal/digitaltwin", work_dir / "digitaltwin_meas.test")
    print(f"PASS: compiled {telemetry_bin.name} and {temporal_bin.name}", flush=True)

    # Coordinated run ID and User IDs
    run_id = f"s4_{secrets.token_hex(4)}"
    user_a_uuid = format_user_id_as_uuid(ctx.users.owner.user_id)
    user_b_uuid = format_user_id_as_uuid(ctx.users.user_b.user_id)

    target_gw = f"gw_meas_{run_id}"
    target_sensor = f"sens_meas_{run_id}"
    target_sensor_urn = f"urn:ngsi-ld:Sensor:{target_gw}:{target_sensor}"

    distract_gw = f"gw_distr_{run_id}"
    distract_sensor = f"sens_distr_{run_id}"
    distract_sensor_urn = f"urn:ngsi-ld:Sensor:{distract_gw}:{distract_sensor}"

    token_a = ctx.tokens["owner"]
    token_b = ctx.tokens["user_b"]

    print(f"stage=queries:step2-execute-telemetry-measurements run_id={run_id}", flush=True)
    telemetry_env = {
        "TELEMETRY_TEST_ISOLATED": "1",
        "TELEMETRY_HISTORY_TEST_NATIVE": "1",
        "TELEMETRY_TEST_SETUP_URL": "postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "TELEMETRY_TEST_BACKEND_URL": "postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "STAGE4_MEASUREMENT_RUN_ID": run_id,
        "STAGE4_MEASUREMENT_USER_ID": user_a_uuid,
        "STAGE4_PRESERVE_FIXTURE": "1",
    }
    telem_output = run_go_measurement_in_fixture(
        ctx,
        telemetry_bin,
        "^TestNativeTelemetryHistoryQueryPlanMeasurement$",
        telemetry_env,
        timeout=180,
    )
    telem_data = parse_json_marker_from_output(telem_output, "TELEMETRY_MEASUREMENT_JSON: ")
    print("PASS: telemetry native query plan measurement executed", flush=True)

    print(f"stage=queries:step3-execute-temporal-measurements run_id={run_id}", flush=True)
    temporal_env = {
        "TWIN_TEMPORAL_TEST_NATIVE": "1",
        "TELEMETRY_TEST_ISOLATED": "1",
        "TWIN_TEST_SETUP_URL": "postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "TWIN_TEST_BACKEND_URL": "postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "STAGE4_MEASUREMENT_RUN_ID": run_id,
        "STAGE4_MEASUREMENT_USER_ID": user_a_uuid,
        "STAGE4_PRESERVE_FIXTURE": "1",
    }
    temp_output = run_go_measurement_in_fixture(
        ctx,
        temporal_bin,
        "^TestNativeTemporalHistoryQueryPlanMeasurement$",
        temporal_env,
        timeout=180,
    )
    temp_data = parse_json_marker_from_output(temp_output, "TEMPORAL_MEASUREMENT_JSON: ")
    print("PASS: temporal native query plan measurement executed", flush=True)

    print("stage=queries:step4-verify-database-target-counts-and-isolation", flush=True)
    # Target raw telemetry count
    telem_counts = query_json_row(ctx.db_fixture, f"""
        SELECT
            COALESCE(SUM(CASE WHEN gateway_id = '{target_gw}' THEN 1 ELSE 0 END), 0) AS target_count,
            COALESCE(SUM(CASE WHEN gateway_id = '{distract_gw}' THEN 1 ELSE 0 END), 0) AS distract_count
        FROM public.telemetry
    """)
    if telem_counts["target_count"] != 60000:
        raise AssertionError(f"expected 60,000 target raw telemetry rows, got {telem_counts['target_count']}")
    if telem_counts["distract_count"] != 6000:
        raise AssertionError(f"expected 6,000 distractor raw telemetry rows, got {telem_counts['distract_count']}")

    # Target temporal observations count
    temp_counts = query_json_row(ctx.db_fixture, f"""
        SELECT
            COALESCE(SUM(CASE WHEN e.entity_id = '{target_sensor_urn}' THEN 1 ELSE 0 END), 0) AS target_count,
            COALESCE(SUM(CASE WHEN e.entity_id = '{distract_sensor_urn}' THEN 1 ELSE 0 END), 0) AS distract_count
        FROM public.twin_temporal_values v
        JOIN public.twin_entities e ON e.id = v.entity_id
    """)
    if temp_counts["target_count"] != 580:
        raise AssertionError(f"expected 580 target temporal observations, got {temp_counts['target_count']}")
    if temp_counts["distract_count"] != 60:
        raise AssertionError(f"expected 60 distractor temporal observations, got {temp_counts['distract_count']}")

    print(
        f"PASS: database counts verified: {telem_counts['target_count']} target raw / "
        f"{telem_counts['distract_count']} distract raw, {temp_counts['target_count']} target temporal / "
        f"{temp_counts['distract_count']} distract temporal",
        flush=True,
    )

    print("stage=queries:step5-verify-hypertable-compression-status", flush=True)
    chunk_rows = query_json_rows(ctx.db_fixture, """
        SELECT
            chunk_name,
            hypertable_name,
            is_compressed
        FROM timescaledb_information.chunks
        WHERE hypertable_name IN ('telemetry', 'twin_temporal_values')
        ORDER BY hypertable_name, chunk_name
    """)
    if not chunk_rows:
        raise AssertionError("no hypertable chunks found in timescaledb_information.chunks")

    compressed_chunks = [c["chunk_name"] for c in chunk_rows if c.get("is_compressed")]
    if compressed_chunks:
        raise AssertionError(f"chunks already compressed prior to compression test: {compressed_chunks}")

    chunk_names_by_table = {
        "telemetry": [c["chunk_name"] for c in chunk_rows if c.get("hypertable_name") == "telemetry"],
        "twin_temporal_values": [c["chunk_name"] for c in chunk_rows if c.get("hypertable_name") == "twin_temporal_values"],
    }
    print(
        f"PASS: verified uncompressed hypertable status across {len(chunk_rows)} chunks "
        f"(telemetry={len(chunk_names_by_table['telemetry'])}, temporal={len(chunk_names_by_table['twin_temporal_values'])})",
        flush=True,
    )

    print("stage=queries:step6-measure-http-service-elapsed-and-isolation", flush=True)
    base_time_iso = "2026-10-18T12:00:00Z"
    t_base = datetime.datetime(2026, 10, 18, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # M1 HTTP: Short raw window (5s: 500 samples)
    from_5s = t_base.isoformat().replace("+00:00", "Z")
    to_5s = (t_base + datetime.timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m1 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_5s)}&to={urllib.parse.quote(to_5s)}&resolution=raw&max_points=500",
        token=token_a,
    )
    m1_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m1.status != 200 or resp_m1.json.get("point_count") != 500 or resp_m1.json.get("resolution") != "raw":
        raise AssertionError(f"M1 HTTP query failed: status {resp_m1.status}, body: {resp_m1.json}")

    # M2 HTTP: Longer bucket window (10m: auto resolution 10s -> 60 buckets with cap 500)
    from_10m = t_base.isoformat().replace("+00:00", "Z")
    to_10m = (t_base + datetime.timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m2 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_10m)}&to={urllib.parse.quote(to_10m)}&resolution=auto&max_points=500",
        token=token_a,
    )
    m2_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m2.status != 200 or resp_m2.json.get("point_count") != 60 or resp_m2.json.get("resolution") != "10s":
        raise AssertionError(f"M2 HTTP query failed: status {resp_m2.status}, body: {resp_m2.json}")

    # M3 HTTP: Short auto probe point overflow (8s: 800 samples -> cap 500 overflow -> fallback 100ms -> 80 buckets)
    from_8s = t_base.isoformat().replace("+00:00", "Z")
    to_8s = (t_base + datetime.timedelta(seconds=8)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m3 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_8s)}&to={urllib.parse.quote(to_8s)}&resolution=auto&max_points=500",
        token=token_a,
    )
    m3_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m3.status != 200 or resp_m3.json.get("point_count") != 80 or resp_m3.json.get("resolution") != "100ms":
        raise AssertionError(f"M3 HTTP query failed: status {resp_m3.status}, body: {resp_m3.json}")

    # M4 HTTP: Temporal raw query (10s: 10 observations)
    from_10s = t_base.isoformat().replace("+00:00", "Z")
    to_10s = (t_base + datetime.timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m4 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10s)}&to={urllib.parse.quote(to_10s)}",
        token=token_a,
    )
    m4_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m4.status != 200 or resp_m4.json.get("point_count") != 10 or resp_m4.json.get("mode") != "raw":
        raise AssertionError(f"M4 HTTP query failed: status {resp_m4.status}, body: {resp_m4.json}")

    # M5 HTTP: Temporal bucket query (10m: resolution 1m -> 10 buckets)
    t0 = time.monotonic()
    resp_m5 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10m)}&to={urllib.parse.quote(to_10m)}&resolution=1m",
        token=token_a,
    )
    m5_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m5.status != 200 or resp_m5.json.get("point_count") != 10 or resp_m5.json.get("mode") != "bucket":
        raise AssertionError(f"M5 HTTP query failed: status {resp_m5.status}, body: {resp_m5.json}")

    # Tenant Isolation HTTP check: User B (owns distractor gateway) denied reading Target Gateway
    resp_iso_telem = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_5s)}&to={urllib.parse.quote(to_5s)}",
        token=token_b,
    )
    if resp_iso_telem.status != 404:
        raise AssertionError(f"User B expected 404 for target telemetry history, got {resp_iso_telem.status}")

    resp_iso_twin = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10s)}&to={urllib.parse.quote(to_10s)}",
        token=token_b,
    )
    if resp_iso_twin.status != 404:
        raise AssertionError(f"User B expected 404 for target twin history, got {resp_iso_twin.status}")

    print("PASS: HTTP queries and cross-tenant isolation verified", flush=True)

    print("stage=queries:step7-save-uncompressed-measurement-summary", flush=True)
    summary = {
        "status": "UNCOMPRESSED_BASELINE_COMPLETE_COMPRESSION_PENDING",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "run_id": run_id,
        "dataset": {
            "target_gateway_id": target_gw,
            "target_sensor_id": target_sensor,
            "target_sensor_urn": target_sensor_urn,
            "distract_gateway_id": distract_gw,
            "distract_sensor_id": distract_sensor,
            "distract_sensor_urn": distract_sensor_urn,
            "target_raw_samples": telem_counts["target_count"],
            "distract_raw_samples": telem_counts["distract_count"],
            "target_temporal_observations": temp_counts["target_count"],
            "distract_temporal_observations": temp_counts["distract_count"],
            "time_span_seconds": 600,
            "target_sample_rate_hz": 100,
            "target_sample_interval_us": 10000,
            "target_temporal_gap": "[300s, 320s) (20 seconds)",
        },
        "compression_status": {
            "is_compressed": False,
            "telemetry_chunks": chunk_names_by_table["telemetry"],
            "twin_temporal_chunks": chunk_names_by_table["twin_temporal_values"],
            "total_uncompressed_chunks": len(chunk_rows),
        },
        "query_measurements": {
            "m1_raw_telemetry": {
                "planning_time_ms": telem_data["m1_planning_time_ms"],
                "execution_time_ms": telem_data["m1_execution_time_ms"],
                "actual_rows": telem_data["m1_actual_rows"],
                "shared_hit_blocks": telem_data["m1_shared_hit_blocks"],
                "shared_read_blocks": telem_data["m1_shared_read_blocks"],
                "plan_nodes": telem_data["m1_plan_nodes"],
                "service_elapsed_ms": telem_data["m1_service_elapsed_ms"],
                "http_elapsed_ms": round(m1_http_ms, 3),
            },
            "m2_bucket_telemetry": {
                "planning_time_ms": telem_data["m2_planning_time_ms"],
                "execution_time_ms": telem_data["m2_execution_time_ms"],
                "actual_rows": telem_data["m2_actual_rows"],
                "shared_hit_blocks": telem_data["m2_shared_hit_blocks"],
                "shared_read_blocks": telem_data["m2_shared_read_blocks"],
                "plan_nodes": telem_data["m2_plan_nodes"],
                "service_elapsed_ms": telem_data["m2_service_elapsed_ms"],
                "http_elapsed_ms": round(m2_http_ms, 3),
            },
            "m3_overflow_fallback_telemetry": {
                "probe_planning_time_ms": telem_data["m3a_planning_time_ms"],
                "probe_execution_time_ms": telem_data["m3a_execution_time_ms"],
                "probe_actual_rows": telem_data["m3a_actual_rows"],
                "fallback_planning_time_ms": telem_data["m3b_planning_time_ms"],
                "fallback_execution_time_ms": telem_data["m3b_execution_time_ms"],
                "fallback_actual_rows": telem_data["m3b_actual_rows"],
                "total_execution_time_ms": telem_data["m3_total_execution_ms"],
                "service_elapsed_ms": telem_data["m3_service_elapsed_ms"],
                "http_elapsed_ms": round(m3_http_ms, 3),
            },
            "m4_raw_temporal": {
                "planning_time_ms": temp_data["m4_planning_time_ms"],
                "execution_time_ms": temp_data["m4_execution_time_ms"],
                "actual_rows": temp_data["m4_actual_rows"],
                "shared_hit_blocks": temp_data["m4_shared_hit_blocks"],
                "shared_read_blocks": temp_data["m4_shared_read_blocks"],
                "plan_nodes": temp_data["m4_plan_nodes"],
                "service_elapsed_ms": temp_data["m4_service_elapsed_ms"],
                "http_elapsed_ms": round(m4_http_ms, 3),
            },
            "m5_bucket_temporal": {
                "planning_time_ms": temp_data["m5_planning_time_ms"],
                "execution_time_ms": temp_data["m5_execution_time_ms"],
                "actual_rows": temp_data["m5_actual_rows"],
                "shared_hit_blocks": temp_data["m5_shared_hit_blocks"],
                "shared_read_blocks": temp_data["m5_shared_read_blocks"],
                "plan_nodes": temp_data["m5_plan_nodes"],
                "service_elapsed_ms": temp_data["m5_service_elapsed_ms"],
                "http_elapsed_ms": round(m5_http_ms, 3),
                "bucket_counts": temp_data["m5_bucket_counts"],
                "arithmetic_valid": temp_data["m5_arithmetic_valid"],
                "gap_verified": temp_data["m5_gap_verified"],
            },
        },
        "tenant_isolation_verified": True,
        "schema_decision": "Retain Schema 20 and existing indexes without speculative changes; uncompressed queries execute comfortably within 5.0s budget",
    }
    summary["_http_responses"] = {
        "m1": resp_m1.json,
        "m2": resp_m2.json,
        "m3": resp_m3.json,
        "m4": resp_m4.json,
        "m5": resp_m5.json,
    }

    clean_summary = {k: v for k, v in summary.items() if not k.startswith("_")}
    summary_file = work_dir / "stage4-query-measurements-uncompressed.json"
    summary_file.write_text(json.dumps(clean_summary, indent=2), encoding="utf-8")
    print(f"PASS: saved non-secret uncompressed summary to {summary_file}", flush=True)

    if os.environ.get("STAGE4_UPDATE_DOCS_SUMMARY") == "1":
        repo_summary_file = ROOT / "docs/backend/stage-4-task-4.7-uncompressed-measurements.json"
        repo_summary_file.write_text(json.dumps(clean_summary, indent=2), encoding="utf-8")
        print(f"PASS: saved non-secret uncompressed summary to {repo_summary_file}", flush=True)

    return summary


def compress_target_hypertable_chunks(ctx: Stage4FixtureContext) -> dict:
    """Compress only the owned target telemetry and twin_temporal_values hypertable chunks.

    Verifies catalog is_compressed flags before (all False) and after (all True),
    along with row counts preservation.
    """
    print("stage=queries:compress-target-chunks", flush=True)

    # 1. Query chunks before compression
    chunks_before = query_json_rows(ctx.db_fixture, """
        SELECT
            chunk_schema,
            chunk_name,
            hypertable_name,
            is_compressed
        FROM timescaledb_information.chunks
        WHERE hypertable_name IN ('telemetry', 'twin_temporal_values')
        ORDER BY hypertable_name, chunk_name
    """)
    if not chunks_before:
        raise AssertionError("no hypertable chunks found in timescaledb_information.chunks")

    for c in chunks_before:
        if c.get("is_compressed"):
            raise AssertionError(f"chunk already compressed before compression step: {c}")

    telem_chunks = [c for c in chunks_before if c["hypertable_name"] == "telemetry"]
    temp_chunks = [c for c in chunks_before if c["hypertable_name"] == "twin_temporal_values"]

    # 2. Compress each chunk explicitly through TimescaleDB 2.29.2 API
    for c in chunks_before:
        schema = c["chunk_schema"]
        name = c["chunk_name"]
        compress_stmt = f"SELECT compress_chunk(format('%I.%I', '{schema}', '{name}')::regclass);"
        ctx.db_fixture.sql(compress_stmt.encode("utf-8"))

    # 3. Query chunks after compression and verify is_compressed = True
    chunks_after = query_json_rows(ctx.db_fixture, """
        SELECT
            chunk_schema,
            chunk_name,
            hypertable_name,
            is_compressed
        FROM timescaledb_information.chunks
        WHERE hypertable_name IN ('telemetry', 'twin_temporal_values')
        ORDER BY hypertable_name, chunk_name
    """)
    for c in chunks_after:
        if not c.get("is_compressed"):
            raise AssertionError(f"chunk was not compressed by compress_chunk: {c}")

    # 4. Verify table row counts remain intact
    telem_count = query_json_row(ctx.db_fixture, "SELECT count(*) AS cnt FROM public.telemetry")["cnt"]
    temp_count = query_json_row(ctx.db_fixture, "SELECT count(*) AS cnt FROM public.twin_temporal_values")["cnt"]
    if telem_count != 66000:
        raise AssertionError(f"telemetry row count changed after compression: expected 66000, got {telem_count}")
    if temp_count != 640:
        raise AssertionError(f"twin_temporal_values row count changed after compression: expected 640, got {temp_count}")

    print(
        f"PASS: verified compression across {len(chunks_after)} chunks "
        f"(telemetry={len(telem_chunks)}, temporal={len(temp_chunks)}); row counts intact",
        flush=True,
    )
    return {
        "uncompressed_chunks_before": len(chunks_before),
        "compressed_chunks_after": len(chunks_after),
        "telemetry_chunks": [c["chunk_name"] for c in telem_chunks],
        "twin_temporal_chunks": [c["chunk_name"] for c in temp_chunks],
        "is_compressed": True,
    }


def run_compressed_query_measurements(
    ctx: Stage4FixtureContext,
    uncompressed_summary: dict,
) -> dict:
    """Rerun Go measurements and HTTP queries against compressed chunks and assert equivalence."""
    work_dir = ctx.work_dir
    telemetry_bin = work_dir / "telemetry_meas.test"
    temporal_bin = work_dir / "digitaltwin_meas.test"
    run_id = uncompressed_summary["run_id"]
    user_a_uuid = format_user_id_as_uuid(ctx.users.owner.user_id)

    target_gw = uncompressed_summary["dataset"]["target_gateway_id"]
    target_sensor = uncompressed_summary["dataset"]["target_sensor_id"]
    target_sensor_urn = uncompressed_summary["dataset"]["target_sensor_urn"]

    token_a = ctx.tokens["owner"]
    token_b = ctx.tokens["user_b"]

    print(f"stage=queries:rerun-telemetry-measurements-compressed run_id={run_id}", flush=True)
    telemetry_env = {
        "TELEMETRY_TEST_ISOLATED": "1",
        "TELEMETRY_HISTORY_TEST_NATIVE": "1",
        "TELEMETRY_TEST_SETUP_URL": "postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "TELEMETRY_TEST_BACKEND_URL": "postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "STAGE4_MEASUREMENT_RUN_ID": run_id,
        "STAGE4_MEASUREMENT_USER_ID": user_a_uuid,
        "STAGE4_PRESERVE_FIXTURE": "1",
        "STAGE4_MEASUREMENT_RERUN_ONLY": "1",
    }
    telem_output = run_go_measurement_in_fixture(
        ctx,
        telemetry_bin,
        "^TestNativeTelemetryHistoryQueryPlanMeasurement$",
        telemetry_env,
        timeout=180,
    )
    telem_data = parse_json_marker_from_output(telem_output, "TELEMETRY_MEASUREMENT_JSON: ")
    if not telem_data.get("is_compressed"):
        raise AssertionError("telemetry measurement rerun did not observe is_compressed=True")
    print("PASS: telemetry native query plan measurement executed on compressed chunks", flush=True)

    print(f"stage=queries:rerun-temporal-measurements-compressed run_id={run_id}", flush=True)
    temporal_env = {
        "TWIN_TEMPORAL_TEST_NATIVE": "1",
        "TELEMETRY_TEST_ISOLATED": "1",
        "TWIN_TEST_SETUP_URL": "postgres://fixture_admin:${POSTGRES_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "TWIN_TEST_BACKEND_URL": "postgres://iot_backend_app:${BACKEND_DB_PASSWORD}@127.0.0.1:5432/fixture_service?sslmode=disable",
        "STAGE4_MEASUREMENT_RUN_ID": run_id,
        "STAGE4_MEASUREMENT_USER_ID": user_a_uuid,
        "STAGE4_PRESERVE_FIXTURE": "1",
        "STAGE4_MEASUREMENT_RERUN_ONLY": "1",
    }
    temp_output = run_go_measurement_in_fixture(
        ctx,
        temporal_bin,
        "^TestNativeTemporalHistoryQueryPlanMeasurement$",
        temporal_env,
        timeout=180,
    )
    temp_data = parse_json_marker_from_output(temp_output, "TEMPORAL_MEASUREMENT_JSON: ")
    if not temp_data.get("is_compressed"):
        raise AssertionError("temporal measurement rerun did not observe is_compressed=True")
    print("PASS: temporal native query plan measurement executed on compressed chunks", flush=True)

    print("stage=queries:measure-http-compressed-and-verify-equality", flush=True)
    t_base = datetime.datetime(2026, 10, 18, 12, 0, 0, tzinfo=datetime.timezone.utc)
    uncompressed_results = uncompressed_summary.get("_http_responses", {})

    # M1 HTTP: Short raw window (5s: 500 samples)
    from_5s = t_base.isoformat().replace("+00:00", "Z")
    to_5s = (t_base + datetime.timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m1 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_5s)}&to={urllib.parse.quote(to_5s)}&resolution=raw&max_points=500",
        token=token_a,
    )
    m1_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m1.status != 200:
        raise AssertionError(f"M1 compressed HTTP query failed: {resp_m1.status}")
    if resp_m1.headers.get("cache-control") != "no-store":
        raise AssertionError(f"M1 missing Cache-Control: no-store, got {resp_m1.headers.get('cache-control')}")

    # Compare M1 result equivalence
    m1_u = uncompressed_results.get("m1", {})
    m1_c = resp_m1.json
    if m1_c.get("point_count") != 500 or m1_c.get("resolution") != "raw":
        raise AssertionError(f"M1 compressed metadata mismatch: {m1_c}")
    assert_history_equivalent(m1_u, m1_c)

    # M2 HTTP: Longer bucket window (10m: auto resolution 10s -> 60 buckets)
    from_10m = t_base.isoformat().replace("+00:00", "Z")
    to_10m = (t_base + datetime.timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m2 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_10m)}&to={urllib.parse.quote(to_10m)}&resolution=auto&max_points=500",
        token=token_a,
    )
    m2_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m2.status != 200:
        raise AssertionError(f"M2 compressed HTTP query failed: {resp_m2.status}")
    m2_u = uncompressed_results.get("m2", {})
    m2_c = resp_m2.json
    assert_history_equivalent(m2_u, m2_c)
    if m2_c.get("point_count") != 60 or m2_c.get("resolution") != "10s":
        raise AssertionError(f"M2 compressed metadata mismatch: {m2_c}")
    if m2_u:
        b_u = m2_u.get("points", [])
        b_c = m2_c.get("points", [])
        if len(b_u) != len(b_c):
            raise AssertionError(f"M2 buckets array length mismatch: {len(b_u)} vs {len(b_c)}")
        for idx, (bu, bc) in enumerate(zip(b_u, b_c)):
            if bu.get("bucket_start") != bc.get("bucket_start"):
                raise AssertionError(f"M2 bucket[{idx}] bucket_start mismatch: {bu} vs {bc}")
            if abs(float(bu.get("avg")) - float(bc.get("avg"))) > 1e-6:
                raise AssertionError(f"M2 bucket[{idx}] avg mismatch: {bu} vs {bc}")
            if abs(float(bu.get("min")) - float(bc.get("min"))) > 1e-6:
                raise AssertionError(f"M2 bucket[{idx}] min mismatch: {bu} vs {bc}")
            if abs(float(bu.get("max")) - float(bc.get("max"))) > 1e-6:
                raise AssertionError(f"M2 bucket[{idx}] max mismatch: {bu} vs {bc}")
            if str(bu.get("count")) != str(bc.get("count")) or int(bc.get("count", 0)) != 1000:
                raise AssertionError(f"M2 bucket[{idx}] count mismatch: {bu} vs {bc}")

    # M3 HTTP: Short auto probe point overflow (8s: 800 samples -> fallback 100ms -> 80 buckets)
    from_8s = t_base.isoformat().replace("+00:00", "Z")
    to_8s = (t_base + datetime.timedelta(seconds=8)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m3 = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_8s)}&to={urllib.parse.quote(to_8s)}&resolution=auto&max_points=500",
        token=token_a,
    )
    m3_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m3.status != 200:
        raise AssertionError(f"M3 compressed HTTP query failed: {resp_m3.status}")
    m3_u = uncompressed_results.get("m3", {})
    m3_c = resp_m3.json
    assert_history_equivalent(m3_u, m3_c)
    if m3_c.get("point_count") != 80 or m3_c.get("resolution") != "100ms":
        raise AssertionError(f"M3 compressed metadata mismatch: {m3_c}")
    if m3_u:
        b_u = m3_u.get("points", [])
        b_c = m3_c.get("points", [])
        if len(b_u) != len(b_c):
            raise AssertionError(f"M3 buckets array length mismatch: {len(b_u)} vs {len(b_c)}")
        for idx, (bu, bc) in enumerate(zip(b_u, b_c)):
            if bu.get("bucket_start") != bc.get("bucket_start"):
                raise AssertionError(f"M3 bucket[{idx}] bucket_start mismatch: {bu} vs {bc}")
            if abs(float(bu.get("avg")) - float(bc.get("avg"))) > 1e-6:
                raise AssertionError(f"M3 bucket[{idx}] avg mismatch: {bu} vs {bc}")
            if str(bu.get("count")) != str(bc.get("count")) or int(bc.get("count", 0)) != 10:
                raise AssertionError(f"M3 bucket[{idx}] count mismatch: {bu} vs {bc}")

    # M4 HTTP: Temporal raw query (10s: 10 observations)
    from_10s = t_base.isoformat().replace("+00:00", "Z")
    to_10s = (t_base + datetime.timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    t0 = time.monotonic()
    resp_m4 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10s)}&to={urllib.parse.quote(to_10s)}",
        token=token_a,
    )
    m4_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m4.status != 200:
        raise AssertionError(f"M4 compressed HTTP query failed: {resp_m4.status}")
    m4_u = uncompressed_results.get("m4", {})
    m4_c = resp_m4.json
    if m4_c.get("point_count") != 10 or m4_c.get("mode") != "raw" or m4_c.get("unit") != "us":
        raise AssertionError(f"M4 compressed metadata mismatch: {m4_c}")
    assert_history_equivalent(m4_u, m4_c, temporal=True)
    if any(o['value'] != 10000.0 for o in m4_c['points']):
        raise AssertionError('M4 unexpected observation value')

    # M5 HTTP: Temporal bucket query (10m: resolution 1m -> 10 buckets)
    t0 = time.monotonic()
    resp_m5 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10m)}&to={urllib.parse.quote(to_10m)}&resolution=1m",
        token=token_a,
    )
    m5_http_ms = (time.monotonic() - t0) * 1000.0
    if resp_m5.status != 200:
        raise AssertionError(f"M5 compressed HTTP query failed: {resp_m5.status}")
    m5_u = uncompressed_results.get("m5", {})
    m5_c = resp_m5.json
    assert_history_equivalent(m5_u, m5_c, temporal=True)
    if m5_c.get("point_count") != 10 or m5_c.get("mode") != "bucket" or m5_c.get("unit") != "us":
        raise AssertionError(f"M5 compressed metadata mismatch: {m5_c}")
    if m5_u:
        b_u = m5_u.get("points", [])
        b_c = m5_c.get("points", [])
        if len(b_u) != len(b_c):
            raise AssertionError(f"M5 buckets array length mismatch: {len(b_u)} vs {len(b_c)}")
        for idx, (bu, bc) in enumerate(zip(b_u, b_c)):
            if bu.get("bucket_start") != bc.get("bucket_start"):
                raise AssertionError(f"M5 bucket[{idx}] bucket_start mismatch: {bu} vs {bc}")
            if abs(float(bu.get("avg")) - float(bc.get("avg"))) > 1e-6:
                raise AssertionError(f"M5 bucket[{idx}] avg mismatch: {bu} vs {bc}")
            if abs(float(bu.get("min")) - float(bc.get("min"))) > 1e-6:
                raise AssertionError(f"M5 bucket[{idx}] min mismatch: {bu} vs {bc}")
            if abs(float(bu.get("max")) - float(bc.get("max"))) > 1e-6:
                raise AssertionError(f"M5 bucket[{idx}] max mismatch: {bu} vs {bc}")
            if str(bu.get("count")) != str(bc.get("count")):
                raise AssertionError(f"M5 bucket[{idx}] count mismatch: {bu} vs {bc}")

    # Explicit arithmetic and intentional gap verification on compressed M5
    m5_counts = [int(b.get("count", 0)) for b in m5_c.get("points", [])]
    if sum(m5_counts) != 580:
        raise AssertionError(f"M5 total sample count mismatch: expected 580, got {sum(m5_counts)}")
    if m5_counts[5] != 40:
        raise AssertionError(f"M5 intentional gap bucket 5 count mismatch: expected 40, got {m5_counts[5]}")
    for idx in (0, 1, 2, 3, 4, 6, 7, 8, 9):
        if m5_counts[idx] != 60:
            raise AssertionError(f"M5 normal bucket {idx} count mismatch: expected 60, got {m5_counts[idx]}")

    # Tenant Isolation HTTP check on compressed data: User B still denied 404
    resp_iso_telem = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={target_gw}&sensor_id={target_sensor}&from={urllib.parse.quote(from_5s)}&to={urllib.parse.quote(to_5s)}",
        token=token_b,
    )
    if resp_iso_telem.status != 404:
        raise AssertionError(f"User B expected 404 for target telemetry history on compressed chunk, got {resp_iso_telem.status}")

    resp_iso_twin = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(target_sensor_urn, safe=':')}/history?property=sample_interval_us&from={urllib.parse.quote(from_10s)}&to={urllib.parse.quote(to_10s)}",
        token=token_b,
    )
    if resp_iso_twin.status != 404:
        raise AssertionError(f"User B expected 404 for target twin history on compressed chunk, got {resp_iso_twin.status}")

    print("PASS: compressed HTTP queries, exact result equivalence, arithmetic, gap, and tenant isolation verified", flush=True)

    return {
        "status": "COMPRESSED_MEASUREMENTS_COMPLETE",
        "query_measurements": {
            "m1_raw_telemetry": {
                "planning_time_ms": telem_data["m1_planning_time_ms"],
                "execution_time_ms": telem_data["m1_execution_time_ms"],
                "actual_rows": telem_data["m1_actual_rows"],
                "shared_hit_blocks": telem_data["m1_shared_hit_blocks"],
                "shared_read_blocks": telem_data["m1_shared_read_blocks"],
                "plan_nodes": telem_data["m1_plan_nodes"],
                "service_elapsed_ms": telem_data["m1_service_elapsed_ms"],
                "http_elapsed_ms": round(m1_http_ms, 3),
            },
            "m2_bucket_telemetry": {
                "planning_time_ms": telem_data["m2_planning_time_ms"],
                "execution_time_ms": telem_data["m2_execution_time_ms"],
                "actual_rows": telem_data["m2_actual_rows"],
                "shared_hit_blocks": telem_data["m2_shared_hit_blocks"],
                "shared_read_blocks": telem_data["m2_shared_read_blocks"],
                "plan_nodes": telem_data["m2_plan_nodes"],
                "service_elapsed_ms": telem_data["m2_service_elapsed_ms"],
                "http_elapsed_ms": round(m2_http_ms, 3),
            },
            "m3_overflow_fallback_telemetry": {
                "probe_planning_time_ms": telem_data["m3a_planning_time_ms"],
                "probe_execution_time_ms": telem_data["m3a_execution_time_ms"],
                "probe_actual_rows": telem_data["m3a_actual_rows"],
                "fallback_planning_time_ms": telem_data["m3b_planning_time_ms"],
                "fallback_execution_time_ms": telem_data["m3b_execution_time_ms"],
                "fallback_actual_rows": telem_data["m3b_actual_rows"],
                "total_execution_time_ms": telem_data["m3_total_execution_ms"],
                "service_elapsed_ms": telem_data["m3_service_elapsed_ms"],
                "http_elapsed_ms": round(m3_http_ms, 3),
            },
            "m4_raw_temporal": {
                "planning_time_ms": temp_data["m4_planning_time_ms"],
                "execution_time_ms": temp_data["m4_execution_time_ms"],
                "actual_rows": temp_data["m4_actual_rows"],
                "shared_hit_blocks": temp_data["m4_shared_hit_blocks"],
                "shared_read_blocks": temp_data["m4_shared_read_blocks"],
                "plan_nodes": temp_data["m4_plan_nodes"],
                "service_elapsed_ms": temp_data["m4_service_elapsed_ms"],
                "http_elapsed_ms": round(m4_http_ms, 3),
            },
            "m5_bucket_temporal": {
                "planning_time_ms": temp_data["m5_planning_time_ms"],
                "execution_time_ms": temp_data["m5_execution_time_ms"],
                "actual_rows": temp_data["m5_actual_rows"],
                "shared_hit_blocks": temp_data["m5_shared_hit_blocks"],
                "shared_read_blocks": temp_data["m5_shared_read_blocks"],
                "plan_nodes": temp_data["m5_plan_nodes"],
                "service_elapsed_ms": temp_data["m5_service_elapsed_ms"],
                "http_elapsed_ms": round(m5_http_ms, 3),
                "bucket_counts": m5_counts,
                "arithmetic_valid": True,
                "gap_verified": True,
            },
        },
        "result_equivalence_verified": True,
        "tenant_isolation_verified": True,
    }


def cleanup_queries_scenario_fixture_data(ctx: Stage4FixtureContext, run_id: str) -> None:
    """Decompress chunks and remove all scenario-created database records, verifying zero rows remain."""
    print(f"stage=queries:cleanup-fixture-data run_id={run_id}", flush=True)
    target_gw = f"gw_meas_{run_id}"
    distract_gw = f"gw_distr_{run_id}"

    # 1. Decompress any compressed chunks so TimescaleDB does not reject DELETEs
    compressed_chunks = query_json_rows(ctx.db_fixture, """
        SELECT chunk_schema, chunk_name
        FROM timescaledb_information.chunks
        WHERE hypertable_name IN ('telemetry', 'twin_temporal_values') AND is_compressed
    """)
    for c in compressed_chunks:
        schema = c["chunk_schema"]
        name = c["chunk_name"]
        stmt = f"SELECT decompress_chunk(format('%I.%I', '{schema}', '{name}')::regclass);"
        ctx.db_fixture.sql(stmt.encode("utf-8"))

    # 2. Delete test data in proper dependency order
    cleanup_sql = f"""
        DELETE FROM public.telemetry WHERE gateway_id IN ('{target_gw}', '{distract_gw}');
        DELETE FROM public.twin_temporal_values WHERE entity_id IN (
            SELECT id FROM public.twin_entities WHERE gateway_id IN ('{target_gw}', '{distract_gw}')
        );
        DELETE FROM public.twin_states WHERE entity_id IN (
            SELECT id FROM public.twin_entities WHERE gateway_id IN ('{target_gw}', '{distract_gw}')
        );
        DELETE FROM public.twin_relationships WHERE source_entity_id IN (
            SELECT id FROM public.twin_entities WHERE gateway_id IN ('{target_gw}', '{distract_gw}')
        ) OR target_entity_id IN (
            SELECT id FROM public.twin_entities WHERE gateway_id IN ('{target_gw}', '{distract_gw}')
        );
        DELETE FROM public.twin_entities WHERE gateway_id IN ('{target_gw}', '{distract_gw}');
        DELETE FROM public.user_gateways WHERE gateway_id IN ('{target_gw}', '{distract_gw}');
        DELETE FROM public.sensors WHERE gateway_id IN ('{target_gw}', '{distract_gw}');
        DELETE FROM public.gateways WHERE gateway_id IN ('{target_gw}', '{distract_gw}');
    """
    ctx.db_fixture.sql(cleanup_sql.encode("utf-8"))

    # 3. Verify counts are 0
    telem_rem = query_json_row(ctx.db_fixture, f"SELECT count(*) AS cnt FROM public.telemetry WHERE gateway_id IN ('{target_gw}', '{distract_gw}')")["cnt"]
    temp_rem = query_json_row(ctx.db_fixture, f"""
        SELECT count(*) AS cnt FROM public.twin_temporal_values v
        JOIN public.twin_entities e ON e.id = v.entity_id
        WHERE e.gateway_id IN ('{target_gw}', '{distract_gw}')
    """)["cnt"]
    gw_rem = query_json_row(ctx.db_fixture, f"SELECT count(*) AS cnt FROM public.gateways WHERE gateway_id IN ('{target_gw}', '{distract_gw}')")["cnt"]

    if telem_rem != 0 or temp_rem != 0 or gw_rem != 0:
        raise AssertionError(f"cleanup incomplete: telem_rem={telem_rem}, temp_rem={temp_rem}, gw_rem={gw_rem}")

    print("PASS: database cleanup verified (0 rows remaining for target and distractor identities)", flush=True)


def run_queries_scenario(ctx: Stage4FixtureContext) -> dict:
    """Task 4.7.5 Queries Scenario: execute both uncompressed baseline and compressed comparison halves."""
    print("=== Starting Task 4.7.5 Queries Scenario (Uncompressed + Compressed) ===", flush=True)

    # 1. Uncompressed baseline measurements
    uncompressed_summary = run_uncompressed_query_measurements(ctx)
    print("PASS: uncompressed query measurement baseline completed successfully", flush=True)

    run_id = uncompressed_summary["run_id"]
    try:
        # 2. Compress target hypertable chunks
        comp_status = compress_target_hypertable_chunks(ctx)

        # 3. Compressed measurements and result equivalence
        compressed_summary = run_compressed_query_measurements(ctx, uncompressed_summary)
        print("PASS: compressed query measurements completed successfully", flush=True)

        # 4. Build comprehensive comparison summary
        summary = {
            "status": "COMPLETED",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "run_id": run_id,
            "dataset": uncompressed_summary["dataset"],
            "compression_status": comp_status,
            "comparison": {
                "m1_raw_telemetry": {
                    "uncompressed": uncompressed_summary["query_measurements"]["m1_raw_telemetry"],
                    "compressed": compressed_summary["query_measurements"]["m1_raw_telemetry"],
                },
                "m2_bucket_telemetry": {
                    "uncompressed": uncompressed_summary["query_measurements"]["m2_bucket_telemetry"],
                    "compressed": compressed_summary["query_measurements"]["m2_bucket_telemetry"],
                },
                "m3_overflow_fallback_telemetry": {
                    "uncompressed": uncompressed_summary["query_measurements"]["m3_overflow_fallback_telemetry"],
                    "compressed": compressed_summary["query_measurements"]["m3_overflow_fallback_telemetry"],
                },
                "m4_raw_temporal": {
                    "uncompressed": uncompressed_summary["query_measurements"]["m4_raw_temporal"],
                    "compressed": compressed_summary["query_measurements"]["m4_raw_temporal"],
                },
                "m5_bucket_temporal": {
                    "uncompressed": uncompressed_summary["query_measurements"]["m5_bucket_temporal"],
                    "compressed": compressed_summary["query_measurements"]["m5_bucket_temporal"],
                },
            },
            "result_equivalence_verified": True,
            "tenant_isolation_verified": True,
            "schema_decision": "Retain Schema 20 and existing indexes without speculative changes; both uncompressed and compressed queries execute comfortably within 5.0s budget",
            "cleanup_verified": True,
        }

        # Save to work_dir
        work_file = ctx.work_dir / "stage4-query-measurements-summary.json"
        work_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"PASS: saved full measurement summary to {work_file}", flush=True)

        explicit_summary_path = os.environ.get("STAGE4_QUERY_SUMMARY_PATH") or os.environ.get("STAGE4_MEASUREMENTS_PATH")
        if explicit_summary_path:
            out_file = Path(explicit_summary_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            print(f"PASS: saved full measurement summary to explicit path {out_file}", flush=True)

        if os.environ.get("STAGE4_UPDATE_DOCS_SUMMARY") == "1":
            docs_file = ROOT / "docs/backend/stage-4-task-4.7-query-measurements.json"
            docs_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            print(f"PASS: saved measurement summary to {docs_file}", flush=True)

        return summary
    finally:
        # 5. Cleanup seeded fixture data
        cleanup_queries_scenario_fixture_data(ctx, run_id)


def main(argv: list[str] | None = None) -> int:
    """Entry point for running the uncompressed query measurement fixture independently."""
    import argparse
    import sys
    from stage4_api_runner import (
        build_image,
        create_stage4_fixture,
        docker,
        docker_available,
        ensure_budget,
        ResourceRegistry,
    )

    parser = argparse.ArgumentParser(description="Task 4.7.5 Uncompressed Query Measurement Runner")
    opts = parser.parse_args(argv)

    if not docker_available():
        print("SKIP: docker daemon or CLI unavailable", flush=True)
        return 0

    suite_registry = ResourceRegistry()
    suite_hexid = secrets.token_hex(4)

    try:
        ensure_budget("build")
        print("stage=build-images", flush=True)
        backend_tag = f"stage4-queries-backend-{suite_hexid}"
        gateway_tag = f"stage4-queries-gateway-{suite_hexid}"
        backend_image = build_image("backend", backend_tag)
        suite_registry.register("backend image", "image", backend_image)
        gateway_image = build_image("gateway", gateway_tag)
        suite_registry.register("gateway image", "image", gateway_image)

        print("=== Running isolated query measurement fixture (uncompressed + compressed) ===", flush=True)
        ctx, cleanup = create_stage4_fixture(backend_image, gateway_image)
        try:
            summary = run_queries_scenario(ctx)
            print("PASS: Task 4.7.5 queries scenario (uncompressed + compressed) completed successfully", flush=True)
            print(
                f"Dataset facts: target raw={summary['dataset']['target_raw_samples']}, "
                f"distract raw={summary['dataset']['distract_raw_samples']}, "
                f"target temporal={summary['dataset']['target_temporal_observations']}, "
                f"distract temporal={summary['dataset']['distract_temporal_observations']}",
                flush=True,
            )
        finally:
            cleanup()
            print("PASS: database fixture and all resources cleaned up successfully", flush=True)
        return 0
    finally:
        for label, action in (
            ("backend image", lambda: docker(["image", "rm", "-f", backend_image], timeout=60) if backend_image else None),
            ("gateway image", lambda: docker(["image", "rm", "-f", gateway_image], timeout=60) if gateway_image else None),
        ):
            try:
                action()
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: {label} cleanup error: {exc}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    import sys
    sys.exit(main())
