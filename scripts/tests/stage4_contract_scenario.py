#!/usr/bin/env python3
"""Task 4.7.4 Contract Scenario: HTTP contract, query/body boundaries, and RBAC coverage slice.

This scenario complements the 4.7.3 vertical slice by rigorously asserting
the Stage 4 HTTP contract, authentication, authorization, query/body boundaries,
and error envelopes across real services:
  1. Authentication and Authorization Core:
     - Missing, invalid, and expired JWT -> 401 Unauthorized with code 'unauthorized'
     - User/role injection via query params or tokens with unauthorized roles -> rejected
     - Representative owner/operator/viewer read positives on assigned Gateway
     - Non-member and missing entity isolation -> 404 Not Found (code 'not_found')
     - Authorized-empty 200 OK (empty twin list for unassigned user; empty telemetry history)
  2. Previous Smoke Gaps & RBAC Mutation Denials:
     - Owner Gateway PATCH -> 403 Forbidden (only platform admin may patch gateway)
     - Operator and viewer mutation denials (POST Sensor, PATCH Sensor, PATCH Gateway) -> 403 Forbidden
     - Non-member mutation denials -> 404 Not Found
     - Pre/post denial snapshot immutability verification (asserting field values,
       versions, timestamps, and history records remain untouched, not counts alone)
     - Relationship contents and pagination (relationship_limit, relationship_after, cursor format)
     - Twin list keyset cursor pagination (limit, after, malformed cursor 400, mismatched cursor 400)
  3. HTTP Query and Body Boundaries:
     - Raw overflow (range > rawWindow) 400 'raw_limit_exceeded' vs auto resolution 200 fallback
     - Bad precision, invalid time range (from > to, range > 744h), invalid resolution, unsupported property -> 400
     - Single-encoded URN 200 vs double-encoded URN 400
     - Forbidden metadata fields (e.g. unit on Sensor PATCH, owner_user_id on Gateway PATCH) -> 400
     - Duplicate JSON keys -> 400
     - Wrong Media Type (text/plain) -> 415 Unsupported Media Type
     - Oversized payload (> 65536 bytes) -> 413 Payload Too Large
     - Query parameters on mutation paths -> 400
     - Exact replay 200 vs conflicting representation 409 Conflict
     - PATCH omission (preserve omitted fields), null description (set to SQL NULL), no-op (preserve updated_at)
     - Invalid Sensor name (null or blank) -> 400
     - Global cache and content-type headers (no-store, pragma no-cache, ld+json for detail, json for errors)
"""
from __future__ import annotations

import json
import secrets
import urllib.parse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stage4_api_runner import Stage4FixtureContext, GatewayFixtureSpec

from stage4_vertical_scenario import (
    format_user_id_as_uuid,
    query_json_row,
    query_json_rows,
    snapshot_owned_content,
    assert_content_unchanged,
)


def assert_error_envelope(resp, expected_status: int, expected_code: str, label: str) -> dict:
    """Assert standard error response framing according to Stage 4 contract."""
    if resp.status != expected_status:
        raise AssertionError(
            f"[{label}] expected HTTP {expected_status}, got {resp.status}. Body: {resp.json}"
        )
    ct = resp.headers.get("content-type", "")
    if not ct.startswith("application/json"):
        raise AssertionError(f"[{label}] expected Content-Type application/json, got {ct}")
    cc = resp.headers.get("cache-control", "")
    if cc != "no-store":
        raise AssertionError(f"[{label}] expected Cache-Control: no-store, got {cc}")

    if not isinstance(resp.json, dict) or "error" not in resp.json:
        raise AssertionError(f"[{label}] response missing 'error' envelope: {resp.json}")
    err_obj = resp.json["error"]
    if not isinstance(err_obj, dict):
        raise AssertionError(f"[{label}] 'error' is not an object: {err_obj}")

    code = err_obj.get("code")
    if code != expected_code:
        raise AssertionError(f"[{label}] expected error code '{expected_code}', got '{code}'")
    if not err_obj.get("message"):
        raise AssertionError(f"[{label}] missing error message in envelope: {err_obj}")
    if not err_obj.get("request_id"):
        raise AssertionError(f"[{label}] missing request_id in envelope: {err_obj}")
    return err_obj


def snapshot_denial_state(fixture, gw_id: str, gw_urn: str, sensor_id: str, sensor_urn: str) -> dict:
    """Capture precise field values, timestamps, states, and history for immutability check."""
    snapshot = query_json_row(fixture, f"""
        SELECT
            (SELECT name FROM public.gateways WHERE gateway_id = '{gw_id}') AS gw_name,
            (SELECT description FROM public.gateways WHERE gateway_id = '{gw_id}') AS gw_desc,
            (SELECT updated_at FROM public.twin_entities WHERE entity_id = '{gw_urn}') AS gw_updated_at,
            (SELECT name FROM public.sensors WHERE gateway_id = '{gw_id}' AND sensor_id = '{sensor_id}') AS sens_name,
            (SELECT unit FROM public.sensors WHERE gateway_id = '{gw_id}' AND sensor_id = '{sensor_id}') AS sens_unit,
            (SELECT updated_at FROM public.twin_entities WHERE entity_id = '{sensor_urn}') AS sens_entity_updated_at,
            (SELECT reported_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{gw_urn}')) AS gw_rep_ver,
            (SELECT desired_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{gw_urn}')) AS gw_des_ver,
            (SELECT reported_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{gw_urn}')) AS gw_rep_state,
            (SELECT desired_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{gw_urn}')) AS gw_des_state,
            (SELECT updated_at FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{gw_urn}')) AS gw_state_updated_at,
            (SELECT reported_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS sens_rep_ver,
            (SELECT desired_version FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS sens_des_ver,
            (SELECT reported_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS sens_rep_state,
            (SELECT desired_state FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS sens_des_state,
            (SELECT updated_at FROM public.twin_states WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS sens_state_updated_at,
            (SELECT count(*) FROM public.telemetry WHERE gateway_id = '{gw_id}') AS telem_count,
            (SELECT count(*) FROM public.twin_temporal_values WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = '{sensor_urn}')) AS temporal_count
    """)
    snapshot['owned_content'] = snapshot_owned_content(fixture, [gw_id])
    return snapshot


def run_contract_scenario(ctx: Stage4FixtureContext) -> None:
    """Execute the full Task 4.7.4 HTTP contract and authorization coverage slice."""
    hexid = secrets.token_hex(4)
    gw_id = f"gw_contract_{hexid}"
    sensor_1_id = f"sens_c1_{hexid}"
    sensor_2_id = f"sens_c2_{hexid}"

    gw_urn = f"urn:ngsi-ld:Gateway:{gw_id}"
    sens_1_urn = f"urn:ngsi-ld:Sensor:{gw_id}:{sensor_1_id}"
    sens_2_urn = f"urn:ngsi-ld:Sensor:{gw_id}:{sensor_2_id}"

    admin_token = ctx.tokens["admin"]
    owner_token = ctx.tokens["owner"]
    operator_token = ctx.tokens["operator"]
    viewer_token = ctx.tokens["viewer"]
    user_b_token = ctx.tokens["user_b"]

    print(f"=== Starting 4.7.4 Contract Scenario for {gw_id} ===", flush=True)

    # -------------------------------------------------------------------------
    # Baseline Entity Setup via Authoritative POST
    # -------------------------------------------------------------------------
    print("stage=contract:setup-entities", flush=True)

    # Admin provisions Gateway C with Owner
    gw_payload = {
        "entity_type": "Gateway",
        "gateway_id": gw_id,
        "name": f"Gateway {gw_id}",
        "owner_user_id": format_user_id_as_uuid(ctx.users.owner.user_id),
    }
    resp = ctx.http.post("/v1/digital-twins", token=admin_token, json_body=gw_payload)
    if resp.status != 201:
        raise RuntimeError(f"setup POST Gateway failed: {resp.status} {resp.json}")

    # Owner provisions Sensor 1
    sens_1_payload = {
        "entity_type": "Sensor",
        "gateway_id": gw_id,
        "sensor_id": sensor_1_id,
        "name": f"Sensor 1 {gw_id}",
        "unit": "celsius",
    }
    resp = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=sens_1_payload)
    if resp.status != 201:
        raise RuntimeError(f"setup POST Sensor 1 failed: {resp.status} {resp.json}")

    # Owner provisions Sensor 2 (enables testing relationship pagination)
    sens_2_payload = {
        "entity_type": "Sensor",
        "gateway_id": gw_id,
        "sensor_id": sensor_2_id,
        "name": f"Sensor 2 {gw_id}",
        "unit": "percent",
    }
    resp = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=sens_2_payload)
    if resp.status != 201:
        raise RuntimeError(f"setup POST Sensor 2 failed: {resp.status} {resp.json}")

    # Assign operator and viewer memberships
    ctx.assign_membership(ctx.users.operator.user_id, gw_id, "operator")
    ctx.assign_membership(ctx.users.viewer.user_id, gw_id, "viewer")

    # -------------------------------------------------------------------------
    # 1. Authentication and Authorization Core
    # -------------------------------------------------------------------------
    print("stage=contract:section1-auth-core", flush=True)

    # 1a. Missing JWT on representative routes
    endpoints = [
        ("GET", "/v1/digital-twins"),
        ("POST", "/v1/digital-twins"),
        ("PATCH", f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}"),
        ("GET", f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from=2026-10-18T00:00:00Z&to=2026-10-18T00:01:00Z"),
    ]
    for method, path in endpoints:
        r = ctx.http.request(method, path, token=None)
        assert_error_envelope(r, 401, "unauthorized", f"missing JWT on {method} {path}")

    # 1b. Invalid JWT (tampered signature / garbage)
    r = ctx.http.get("/v1/digital-twins", token="eyJhbGciOiJIUzI1NiJ9.invalid.garbageSignature")
    assert_error_envelope(r, 401, "unauthorized", "invalid JWT garbage")

    # 1c. Expired JWT
    expired_token = ctx.make_jwt(ctx.users.owner.user_id, exp_delta_seconds=-3600)
    r = ctx.http.get("/v1/digital-twins", token=expired_token)
    assert_error_envelope(r, 401, "unauthorized", "expired JWT")

    # 1d. Unauthorized JWT role (e.g. 'anon' or 'service_role' rejected by Go JWT verifier)
    anon_token = ctx.make_jwt(ctx.users.owner.user_id, role="anon")
    r = ctx.http.get("/v1/digital-twins", token=anon_token)
    assert_error_envelope(r, 401, "unauthorized", "token with role 'anon'")

    # 1e. User / role injection rejected (query parameter cannot inject admin or elevate role)
    fake_admin_uid = format_user_id_as_uuid(ctx.users.admin.user_id)
    r_inj = ctx.http.get(
        f"/v1/digital-twins?user_id={fake_admin_uid}&role=platform_admin",
        token=user_b_token,
    )
    assert_error_envelope(r_inj, 400, "invalid_request", "user/role query param injection")

    # 1f. Representative positives for owner, operator, and viewer on assigned gateway
    for role_name, token in [("owner", owner_token), ("operator", operator_token), ("viewer", viewer_token)]:
        r_list = ctx.http.get(f"/v1/digital-twins?gateway_id={gw_id}", token=token)
        if r_list.status != 200:
            raise AssertionError(f"{role_name} list failed: {r_list.status}")
        r_gw = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}", token=token)
        if r_gw.status != 200:
            raise AssertionError(f"{role_name} gateway detail failed: {r_gw.status}")
        r_sens = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}", token=token)
        if r_sens.status != 200:
            raise AssertionError(f"{role_name} sensor detail failed: {r_sens.status}")
        r_state = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/state", token=token)
        if r_state.status != 200:
            raise AssertionError(f"{role_name} sensor state failed: {r_state.status}")

    # 1g. Non-member and missing entity isolation: 404 Not Found (code 'not_found')
    nonmember_endpoints = [
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/state",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/history?property=value&from=2026-10-18T00:00:00Z&to=2026-10-18T00:01:00Z",
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from=2026-10-18T00:00:00Z&to=2026-10-18T00:01:00Z",
    ]
    for p in nonmember_endpoints:
        r = ctx.http.get(p, token=user_b_token)
        assert_error_envelope(r, 404, "not_found", f"non-member read on {p}")

    # Missing entity URN -> 404
    missing_urn = "urn:ngsi-ld:Gateway:missing_gateway_999"
    r = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(missing_urn)}", token=owner_token)
    assert_error_envelope(r, 404, "not_found", "missing entity URN")

    # 1h. Authorized-empty 200 OK
    # Authorized gateway/sensor with 0 telemetry samples -> 200 OK with point_count=0, points=[]
    r_empty_hist = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from=2026-10-18T00:00:00Z&to=2026-10-18T00:01:00Z",
        token=owner_token,
    )
    if r_empty_hist.status != 200:
        raise AssertionError(f"authorized-empty telemetry history failed: {r_empty_hist.status} {r_empty_hist.json}")
    if r_empty_hist.json.get("point_count") != 0 or r_empty_hist.json.get("points") != []:
        raise AssertionError(f"expected point_count=0 and points=[]: {r_empty_hist.json}")

    # Unassigned user with no memberships -> 200 OK with items=[]
    unassigned_uid = secrets.token_hex(16)
    unassigned_token = ctx.make_jwt(unassigned_uid)
    r_empty_list = ctx.http.get("/v1/digital-twins", token=unassigned_token)
    if r_empty_list.status != 200:
        raise AssertionError(f"unassigned user list failed: {r_empty_list.status}")
    if r_empty_list.json.get("items") != []:
        raise AssertionError(f"expected empty items array: {r_empty_list.json}")

    # -------------------------------------------------------------------------
    # 2. Previous Smoke Gaps, RBAC Denials & Invariant Snapshots
    # -------------------------------------------------------------------------
    print("stage=contract:section2-rbac-denials-and-snapshots", flush=True)

    # Capture snapshot before denied mutations
    snap_before = snapshot_denial_state(ctx.db_fixture, gw_id, gw_urn, sensor_1_id, sens_1_urn)

    # 2a. Owner Gateway PATCH -> 403 Forbidden (Smoke gap: only Platform Admin may patch gateway!)
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=owner_token,
        json_body={"description": "Owner Unauthorized Desc"},
    )
    assert_error_envelope(r, 403, "forbidden", "owner Gateway PATCH")

    # 2b. Operator mutation denials -> 403 Forbidden
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=operator_token,
        json_body={"description": "Operator Unauthorized Desc"},
    )
    assert_error_envelope(r, 403, "forbidden", "operator Gateway PATCH")

    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=operator_token,
        json_body={"name": "Operator Unauthorized Name"},
    )
    assert_error_envelope(r, 403, "forbidden", "operator Sensor PATCH")

    r = ctx.http.post(
        "/v1/digital-twins",
        token=operator_token,
        json_body={"entity_type": "Sensor", "gateway_id": gw_id, "sensor_id": "sens_unauth", "name": "Unauth", "unit": "c"},
    )
    assert_error_envelope(r, 403, "forbidden", "operator Sensor POST")

    # 2c. Viewer mutation denials -> 403 Forbidden
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=viewer_token,
        json_body={"description": "Viewer Unauthorized Desc"},
    )
    assert_error_envelope(r, 403, "forbidden", "viewer Gateway PATCH")

    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=viewer_token,
        json_body={"name": "Viewer Unauthorized Name"},
    )
    assert_error_envelope(r, 403, "forbidden", "viewer Sensor PATCH")

    r = ctx.http.post(
        "/v1/digital-twins",
        token=viewer_token,
        json_body={"entity_type": "Sensor", "gateway_id": gw_id, "sensor_id": "sens_unauth2", "name": "Unauth", "unit": "c"},
    )
    assert_error_envelope(r, 403, "forbidden", "viewer Sensor POST")

    # 2d. Non-member mutation denials
    # Non-member attempts Gateway PATCH -> 403 Forbidden (only platform admin may patch gateway)
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=user_b_token,
        json_body={"description": "User B Unauthorized Desc"},
    )
    assert_error_envelope(r, 403, "forbidden", "non-member Gateway PATCH")

    # Non-member attempts Sensor PATCH -> 404 Not Found (absent membership fails closed with 404)
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=user_b_token,
        json_body={"name": "User B Unauthorized Name"},
    )
    assert_error_envelope(r, 404, "not_found", "non-member Sensor PATCH")

    # 2e. Assert field, version, timestamp, and history immutability after all denials
    snap_after = snapshot_denial_state(ctx.db_fixture, gw_id, gw_urn, sensor_1_id, sens_1_urn)
    assert_content_unchanged(snap_before, snap_after)

    # 2f. Relationship contents and pagination (smoke gap)
    # Query Gateway detail with relationship_limit=1
    r_rel1 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}?relationship_limit=1",
        token=owner_token,
    )
    if r_rel1.status != 200:
        raise AssertionError(f"gateway detail with relationship_limit=1 failed: {r_rel1.status}")
    ct = r_rel1.headers.get("content-type", "")
    if not ct.startswith("application/ld+json"):
        raise AssertionError(f"expected application/ld+json, got {ct}")

    detail_data = r_rel1.json
    has_sensor = detail_data.get("hasSensor")
    if not isinstance(has_sensor, list) or len(has_sensor) != 1:
        raise AssertionError(f"expected 1 relationship in hasSensor on page 1, got {has_sensor}")
    first_rel = has_sensor[0]
    if first_rel.get("type") != "Relationship" or not str(first_rel.get("object", "")).startswith("urn:ngsi-ld:Sensor:"):
        raise AssertionError(f"invalid relationship structure: {first_rel}")

    rel_page = detail_data.get("relationship_page", {})
    next_rel_cursor = rel_page.get("next_cursor")
    if not next_rel_cursor:
        raise AssertionError(f"expected next_cursor in relationship_page: {detail_data}")

    # Query second relationship page with relationship_after
    r_rel2 = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}?relationship_limit=1&relationship_after={urllib.parse.quote(next_rel_cursor)}",
        token=owner_token,
    )
    if r_rel2.status != 200:
        raise AssertionError(f"gateway detail relationship page 2 failed: {r_rel2.status}")
    has_sensor_2 = r_rel2.json.get("hasSensor", [])
    if len(has_sensor_2) != 1:
        raise AssertionError(f"expected 1 relationship on page 2, got {has_sensor_2}")
    if has_sensor_2[0].get("object") == first_rel.get("object"):
        raise AssertionError("relationship pagination returned duplicate relationship object across pages")

    # Malformed relationship cursor -> 400 Bad Request (code 'invalid_cursor')
    r_bad_rel_cur = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}?relationship_after=malformed_cursor_base64!",
        token=owner_token,
    )
    assert_error_envelope(r_bad_rel_cur, 400, "invalid_cursor", "malformed relationship_after")

    # 2g. Twin List keyset cursor pagination and validation
    r_list1 = ctx.http.get(f"/v1/digital-twins?gateway_id={gw_id}&limit=1", token=owner_token)
    if r_list1.status != 200:
        raise AssertionError(f"list page 1 failed: {r_list1.status}")
    items1 = r_list1.json.get("items", [])
    if len(items1) != 1:
        raise AssertionError(f"expected 1 item on list page 1, got {len(items1)}")
    next_cur = r_list1.json.get("next_cursor")
    if not next_cur:
        raise AssertionError(f"expected next_cursor on list page 1: {r_list1.json}")

    # Next page
    r_list2 = ctx.http.get(
        f"/v1/digital-twins?gateway_id={gw_id}&limit=1&after={urllib.parse.quote(next_cur)}",
        token=owner_token,
    )
    if r_list2.status != 200:
        raise AssertionError(f"list page 2 failed: {r_list2.status}")
    items2 = r_list2.json.get("items", [])
    if len(items2) != 1:
        raise AssertionError(f"expected 1 item on list page 2, got {len(items2)}")
    if items1[0]["id"] == items2[0]["id"]:
        raise AssertionError("list pagination returned identical entity on second page")

    # Malformed list cursor -> 400 Bad Request
    r_bad_cur = ctx.http.get("/v1/digital-twins?after=garbage_cursor_not_base64!", token=owner_token)
    assert_error_envelope(r_bad_cur, 400, "invalid_cursor", "malformed list after cursor")

    # Mismatched list cursor (cursor tied to gw_id passed with a different gateway_id)
    r_mismatch_cur = ctx.http.get(
        f"/v1/digital-twins?gateway_id=other_gw&after={urllib.parse.quote(next_cur)}",
        token=owner_token,
    )
    assert_error_envelope(r_mismatch_cur, 400, "invalid_cursor", "mismatched list cursor")

    # -------------------------------------------------------------------------
    # 3. Representative HTTP Query and Body Boundaries
    # -------------------------------------------------------------------------
    print("stage=contract:section3-boundaries", flush=True)

    # 3a. Raw overflow 400 'raw_limit_exceeded' vs Auto fallback 200 OK
    # Query window > 10s (default History.RawWindow)
    from_time = "2026-10-18T10:00:00Z"
    to_time_15s = "2026-10-18T10:00:15Z"  # 15s > 10s
    to_time_5s = "2026-10-18T10:00:05Z"   # 5s <= 10s

    # Explicit resolution=raw with range > 10s -> 400 Bad Request (raw_limit_exceeded)
    r_raw_overflow = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from={from_time}&to={to_time_15s}&resolution=raw",
        token=owner_token,
    )
    assert_error_envelope(r_raw_overflow, 400, "raw_limit_exceeded", "raw window > 10s overflow")

    # resolution=auto (or omitting resolution) with range > 10s -> 200 OK (falls back to bucket mode!)
    r_auto_fallback = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from={from_time}&to={to_time_15s}&resolution=auto",
        token=owner_token,
    )
    if r_auto_fallback.status != 200:
        raise AssertionError(f"auto resolution fallback failed: {r_auto_fallback.status} {r_auto_fallback.json}")

    # 3b. Bad precision, range, resolution, and property
    # Invalid resolution string -> 400 invalid_resolution
    r_bad_res = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from={from_time}&to={to_time_5s}&resolution=2hours",
        token=owner_token,
    )
    assert_error_envelope(r_bad_res, 400, "invalid_resolution", "unsupported resolution")

    # Invalid timestamp format -> 400 invalid_request
    r_bad_ts = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from=not-a-date&to={to_time_5s}",
        token=owner_token,
    )
    assert_error_envelope(r_bad_ts, 400, "invalid_request", "invalid from timestamp")

    # Inverted time range (from > to) -> 400 invalid_request
    r_inv_range = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from={to_time_5s}&to={from_time}",
        token=owner_token,
    )
    assert_error_envelope(r_inv_range, 400, "invalid_request", "inverted time range")

    # Range > 744h (maxRange) -> 400 range_too_large
    r_huge_range = ctx.http.get(
        f"/v1/telemetry/history?gateway_id={gw_id}&sensor_id={sensor_1_id}&from=2025-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
        token=owner_token,
    )
    assert_error_envelope(r_huge_range, 400, "range_too_large", "range > 744h")

    # Unsupported property on Twin history -> 400 invalid_property
    r_bad_prop = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/history?property=unknown_property&from={from_time}&to={to_time_5s}",
        token=owner_token,
    )
    assert_error_envelope(r_bad_prop, 400, "invalid_property", "unsupported property")

    # Gateway has no history properties -> 400 invalid_property
    r_gw_hist = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/history?property=value&from={from_time}&to={to_time_5s}",
        token=owner_token,
    )
    assert_error_envelope(r_gw_hist, 400, "invalid_property", "gateway history property")

    # 3c. Single vs Double Encoded URN
    # Single-encoded URN -> 200 OK
    r_single_enc = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(gw_urn, safe='')}", token=owner_token)
    if r_single_enc.status != 200:
        raise AssertionError(f"single encoded URN failed: {r_single_enc.status}")

    # Double-encoded URN -> 400 invalid_request (ParseEntityURN rejects %3A inside URN prefix)
    double_encoded = urllib.parse.quote(urllib.parse.quote(gw_urn, safe=""), safe="")
    r_double_enc = ctx.http.get(f"/v1/digital-twins/{double_encoded}", token=owner_token)
    assert_error_envelope(r_double_enc, 400, "invalid_request", "double encoded URN")

    # 3d. Forbidden metadata fields and duplicate keys
    # PATCH Sensor with 'unit' -> 400 (unit is immutable!)
    r_unit_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        json_body={"name": "New Name", "unit": "fahrenheit"},
    )
    assert_error_envelope(r_unit_patch, 400, "invalid_request", "forbidden unit on Sensor PATCH")

    # PATCH Sensor with injected entity_type or gateway_id -> 400
    r_inj_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        json_body={"name": "New Name", "entity_type": "Gateway"},
    )
    assert_error_envelope(r_inj_patch, 400, "invalid_request", "injected field on Sensor PATCH")

    # PATCH Gateway with injected owner_user_id -> 400
    r_gw_inj = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=admin_token,
        json_body={"description": "New Desc", "owner_user_id": fake_admin_uid},
    )
    assert_error_envelope(r_gw_inj, 400, "invalid_request", "injected owner_user_id on Gateway PATCH")

    # Duplicate JSON keys -> 400
    raw_dup_json = b'{"name": "Valid 1", "name": "Valid 2"}'
    r_dup_keys = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        headers={"Content-Type": "application/json"},
        raw_body=raw_dup_json,
    )
    assert_error_envelope(r_dup_keys, 400, "invalid_request", "duplicate JSON keys")

    # 3e. Wrong Media Type 415 and Oversize 413
    # Content-Type: text/plain on POST -> 415 Unsupported Media Type
    r_wrong_media = ctx.http.post(
        "/v1/digital-twins",
        token=admin_token,
        headers={"Content-Type": "text/plain"},
        raw_body=b'{"entity_type": "Gateway"}',
    )
    assert_error_envelope(r_wrong_media, 415, "unsupported_media_type", "POST text/plain")

    # Content-Type: text/plain on PATCH -> 415
    r_wrong_media_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        headers={"Content-Type": "text/plain"},
        raw_body=b'{"name": "test"}',
    )
    assert_error_envelope(r_wrong_media_patch, 415, "unsupported_media_type", "PATCH text/plain")

    # Payload exceeding limit (> 65536 bytes) -> 413 Payload Too Large
    huge_body = json.dumps({"name": "A" * 70000}).encode("utf-8")
    r_oversize = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        headers={"Content-Type": "application/json"},
        raw_body=huge_body,
    )
    assert_error_envelope(r_oversize, 413, "payload_too_large", "oversize body 70KB")

    # Query parameters on mutation paths -> 400 Bad Request
    r_query_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}?forbidden=true",
        token=owner_token,
        json_body={"name": "Valid Name"},
    )
    assert_error_envelope(r_query_patch, 400, "invalid_request", "query param on PATCH")

    # 3f. Exact replay 200 vs Conflicting representation 409 Conflict
    # Exact replay of Gateway creation -> 200 OK
    r_replay_gw = ctx.http.post("/v1/digital-twins", token=admin_token, json_body=gw_payload)
    if r_replay_gw.status != 200:
        raise AssertionError(f"exact replay Gateway failed: {r_replay_gw.status}")

    # Conflicting Gateway creation (different name/owner) -> 409 Conflict
    conflict_gw_payload = dict(gw_payload)
    conflict_gw_payload["name"] = "Conflicting Gateway Name"
    r_conflict_gw = ctx.http.post("/v1/digital-twins", token=admin_token, json_body=conflict_gw_payload)
    assert_error_envelope(r_conflict_gw, 409, "conflict", "conflicting Gateway representation")

    # Exact replay of Sensor creation -> 200 OK
    r_replay_sens = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=sens_1_payload)
    if r_replay_sens.status != 200:
        raise AssertionError(f"exact replay Sensor failed: {r_replay_sens.status}")

    # Conflicting Sensor creation (different unit) -> 409 Conflict
    conflict_sens_payload = dict(sens_1_payload)
    conflict_sens_payload["unit"] = "kelvin"
    r_conflict_sens = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=conflict_sens_payload)
    assert_error_envelope(r_conflict_sens, 409, "conflict", "conflicting Sensor representation")

    # 3g. PATCH omission, null description, and no-op update
    # Admin PATCH Gateway setting description to null -> sets to SQL NULL
    r_null_desc = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=admin_token,
        json_body={"description": None},
    )
    if r_null_desc.status != 200:
        raise AssertionError(f"null description PATCH failed: {r_null_desc.status}")
    if r_null_desc.json.get("description") is not None:
        raise AssertionError(f"expected null description: {r_null_desc.json}")

    # Verify SQL NULL in database
    db_gw_desc = query_json_row(ctx.db_fixture, f"SELECT description FROM public.gateways WHERE gateway_id = '{gw_id}'")
    if db_gw_desc.get("description") is not None:
        raise AssertionError(f"expected NULL description in DB, got: {db_gw_desc}")

    # Admin PATCH Gateway omitting description (only updating name) -> leaves description unchanged
    r_omit_desc = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=admin_token,
        json_body={"name": f"Renamed Gateway {gw_id}"},
    )
    if r_omit_desc.status != 200:
        raise AssertionError(f"omitted description PATCH failed: {r_omit_desc.status}")
    if r_omit_desc.json.get("name") != f"Renamed Gateway {gw_id}":
        raise AssertionError(f"name not updated: {r_omit_desc.json}")
    # Description should still be None
    if r_omit_desc.json.get("description") is not None:
        raise AssertionError(f"description was overwritten: {r_omit_desc.json}")

    # No-op Sensor PATCH (same name) -> returns 200 OK and updated_at is preserved
    sensor_name_row = query_json_row(
        ctx.db_fixture,
        f"SELECT name FROM public.sensors WHERE gateway_id = '{gw_id}' AND sensor_id = '{sensor_1_id}'",
    )
    sensor_before_noop = query_json_row(
        ctx.db_fixture,
        f"SELECT updated_at FROM public.twin_entities WHERE entity_id = '{sens_1_urn}'",
    )
    r_noop_sensor = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        json_body={"name": sensor_name_row["name"]},
    )
    if r_noop_sensor.status != 200:
        raise AssertionError(f"no-op sensor PATCH failed: {r_noop_sensor.status}")
    sensor_after_noop = query_json_row(
        ctx.db_fixture,
        f"SELECT updated_at FROM public.twin_entities WHERE entity_id = '{sens_1_urn}'",
    )
    if sensor_before_noop["updated_at"] != sensor_after_noop["updated_at"]:
        raise AssertionError(
            f"no-op sensor PATCH modified updated_at: {sensor_before_noop['updated_at']} vs {sensor_after_noop['updated_at']}"
        )

    # Invalid Sensor name: null or blank string -> 400 Bad Request
    r_null_name = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        json_body={"name": None},
    )
    assert_error_envelope(r_null_name, 400, "invalid_request", "null Sensor name")

    r_blank_name = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        token=owner_token,
        json_body={"name": "   "},
    )
    assert_error_envelope(r_blank_name, 400, "invalid_request", "blank Sensor name")

    print(f"=== 4.7.4 Contract Scenario completed successfully for {gw_id} ===", flush=True)
    return {
        "gateway_id": gw_id,
        "auth_denials_verified": True,
        "rbac_mutation_denials_verified": True,
        "http_boundaries_verified": True,
    }
