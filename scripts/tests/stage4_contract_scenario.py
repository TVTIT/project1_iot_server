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
import re
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


def parse_rel_next_uri(link_header: str | None) -> str | None:
    """Extract RFC 8288 rel="next" target URI from Link header, or return None."""
    if not link_header:
        return None
    for segment in link_header.split(','):
        segment = segment.strip()
        m = re.match(r'<([^>]+)>\s*;\s*rel="?next"?', segment, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def validate_next_uri_contract(
    next_uri: str,
    expected_path: str,
    expected_params: dict[str, str],
    cursor_param_name: str,
) -> str:
    """Validate that rel="next" URI is relative, targets the same endpoint, preserves filters/limits, and has cursor."""
    if not next_uri.startswith('/'):
        raise AssertionError(f'rel="next" URI must be relative (start with /), got: {next_uri}')
    parsed = urllib.parse.urlsplit(next_uri)
    if urllib.parse.unquote(parsed.path) != urllib.parse.unquote(expected_path):
        raise AssertionError(f'rel="next" path mismatch: got {parsed.path}, expected {expected_path}')
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    for k, v in expected_params.items():
        if qs.get(k) != [v]:
            raise AssertionError(f'rel="next" parameter {k} mismatch: got {qs.get(k)}, expected {[v]} in {next_uri}')
    cursor_vals = qs.get(cursor_param_name, [])
    if not cursor_vals or not cursor_vals[0]:
        raise AssertionError(f'rel="next" missing cursor parameter {cursor_param_name} in {next_uri}')
    return cursor_vals[0]


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
    if resp.headers.get("location") != f"/v1/digital-twins/{urllib.parse.quote(gw_urn, safe=':')}":
        raise RuntimeError(f"invalid Location header on POST Gateway: {resp.headers.get('location')}")

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
    if resp.headers.get("location") != f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn, safe=':')}":
        raise RuntimeError(f"invalid Location header on POST Sensor 1: {resp.headers.get('location')}")

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
    if resp.headers.get("location") != f"/v1/digital-twins/{urllib.parse.quote(sens_2_urn, safe=':')}":
        raise RuntimeError(f"invalid Location header on POST Sensor 2: {resp.headers.get('location')}")

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
        ("PATCH", f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs"),
        ("GET", f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:01:00Z&aggrPeriodDuration=PT1M"),
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
        r_state = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}?format=simplified", token=token)
        if r_state.status != 200:
            raise AssertionError(f"{role_name} sensor state failed: {r_state.status}")

    # 1g. Non-member and missing entity isolation: 404 Not Found (code 'not_found')
    nonmember_endpoints = [
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}?format=simplified",
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:01:00Z&aggrPeriodDuration=PT1M",
    ]
    for p in nonmember_endpoints:
        r = ctx.http.get(p, token=user_b_token)
        assert_error_envelope(r, 404, "not_found", f"non-member read on {p}")

    # Missing entity URN -> 404
    missing_urn = "urn:ngsi-ld:Gateway:missing_gateway_999"
    r = ctx.http.get(f"/v1/digital-twins/{urllib.parse.quote(missing_urn)}", token=owner_token)
    assert_error_envelope(r, 404, "not_found", "missing entity URN")

    # 1h. Authorized-empty 200 OK
    # Authorized gateway/sensor with 0 telemetry samples -> 200 OK with values=[]
    r_empty_hist = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:01:00Z&aggrPeriodDuration=PT1S",
        token=owner_token,
    )
    if r_empty_hist.status != 200:
        raise AssertionError(f"authorized-empty telemetry history failed: {r_empty_hist.status} {r_empty_hist.json}")
    meas = r_empty_hist.json.get("measurement", {})
    if meas.get("totalCount") != [] or meas.get("avg") != []:
        raise AssertionError(f"expected empty aggregate arrays: {r_empty_hist.json}")

    # Unassigned user with no memberships -> 200 OK with items=[]
    unassigned_uid = secrets.token_hex(16)
    unassigned_token = ctx.make_jwt(unassigned_uid)
    r_empty_list = ctx.http.get("/v1/digital-twins", token=unassigned_token)
    if r_empty_list.status != 200:
        raise AssertionError(f"unassigned user list failed: {r_empty_list.status}")
    empty_items = r_empty_list.json if isinstance(r_empty_list.json, list) else r_empty_list.json.get("items")
    if empty_items != []:
        raise AssertionError(f"expected empty items array: {r_empty_list.json}")

    # -------------------------------------------------------------------------
    # 2. Previous Smoke Gaps, RBAC Denials & Invariant Snapshots
    # -------------------------------------------------------------------------
    print("stage=contract:section2-rbac-denials-and-snapshots", flush=True)

    # Capture snapshot before denied mutations
    snap_before = snapshot_denial_state(ctx.db_fixture, gw_id, gw_urn, sensor_1_id, sens_1_urn)

    # Verify old root route /v1/digital-twins/{id} returns 405 Method Not Allowed
    r_old_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=admin_token,
        json_body={"description": {"type": "Property", "value": "Old"}},
    )
    if r_old_patch.status != 405:
        raise AssertionError(f"expected 405 for old root PATCH, got {r_old_patch.status}")

    # 2a. Owner Gateway PATCH -> 403 Forbidden (Smoke gap: only Platform Admin may patch gateway!)
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=owner_token,
        json_body={"description": {"type": "Property", "value": "Owner Unauthorized Desc"}},
    )
    assert_error_envelope(r, 403, "forbidden", "owner Gateway PATCH")

    # 2b. Operator mutation denials -> 403 Forbidden
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=operator_token,
        json_body={"description": {"type": "Property", "value": "Operator Unauthorized Desc"}},
    )
    assert_error_envelope(r, 403, "forbidden", "operator Gateway PATCH")

    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=operator_token,
        json_body={"name": {"type": "Property", "value": "Operator Unauthorized Name"}},
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
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=viewer_token,
        json_body={"description": {"type": "Property", "value": "Viewer Unauthorized Desc"}},
    )
    assert_error_envelope(r, 403, "forbidden", "viewer Gateway PATCH")

    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=viewer_token,
        json_body={"name": {"type": "Property", "value": "Viewer Unauthorized Name"}},
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
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=user_b_token,
        json_body={"description": {"type": "Property", "value": "User B Unauthorized Desc"}},
    )
    assert_error_envelope(r, 403, "forbidden", "non-member Gateway PATCH")

    # Non-member attempts Sensor PATCH -> 404 Not Found (absent membership fails closed with 404)
    r = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=user_b_token,
        json_body={"name": {"type": "Property", "value": "User B Unauthorized Name"}},
    )
    assert_error_envelope(r, 404, "not_found", "non-member Sensor PATCH")

    # 2e. Assert field, version, timestamp, and history immutability after all denials
    snap_after = snapshot_denial_state(ctx.db_fixture, gw_id, gw_urn, sensor_1_id, sens_1_urn)
    assert_content_unchanged(snap_before, snap_after)

    # 2f. Relationship contents and pagination (RFC 8288 rel="next" traversal)
    # Query Gateway detail with relationship_limit=1 and follow returned rel="next" URIs directly
    initial_rel_uri = f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}?relationship_limit=1"
    cur_rel_uri = initial_rel_uri
    collected_rel_objects: list[str] = []
    seen_rel_uris: set[str] = set()
    rel_page_count = 0
    max_pages = 10
    saved_rel_cursor = ""

    while cur_rel_uri:
        rel_page_count += 1
        if rel_page_count > max_pages:
            raise AssertionError(f"relationship pagination exceeded maximum bounded pages ({max_pages})")
        if cur_rel_uri in seen_rel_uris:
            raise AssertionError(f"pagination loop detected on URI: {cur_rel_uri}")
        seen_rel_uris.add(cur_rel_uri)

        r_page = ctx.http.get(cur_rel_uri, token=owner_token)
        if r_page.status != 200:
            raise AssertionError(f"relationship page {rel_page_count} failed: {r_page.status}")
        ct = r_page.headers.get("content-type", "")
        if not ct.startswith("application/ld+json"):
            raise AssertionError(f"expected application/ld+json, got {ct}")

        detail_data = r_page.json
        has_sensor = detail_data.get("hasSensor")
        if not isinstance(has_sensor, dict) or has_sensor.get("type") != "Relationship":
            raise AssertionError(f"expected hasSensor object with type Relationship on page {rel_page_count}, got {has_sensor}")
        objs = has_sensor.get("object", [])
        if len(objs) != 1 or not str(objs[0]).startswith("urn:ngsi-ld:Sensor:"):
            raise AssertionError(f"expected 1 target URN in hasSensor.object on page {rel_page_count}, got {objs}")

        target_urn = objs[0]
        if target_urn in collected_rel_objects:
            raise AssertionError(f"duplicate relationship target {target_urn} across pages")
        collected_rel_objects.append(target_urn)

        link_hdr = r_page.headers.get("link", "")
        next_uri = parse_rel_next_uri(link_hdr)
        if next_uri:
            cursor_val = validate_next_uri_contract(
                next_uri,
                expected_path=f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
                expected_params={"relationship_limit": "1"},
                cursor_param_name="relationship_after",
            )
            saved_rel_cursor = cursor_val
            cur_rel_uri = next_uri
        else:
            cur_rel_uri = None

    # Assert completeness and terminal no-next
    if len(collected_rel_objects) != 2:
        raise AssertionError(f"expected exactly 2 relationship targets, got {collected_rel_objects}")
    if set(collected_rel_objects) != {sens_1_urn, sens_2_urn}:
        raise AssertionError(f"relationship targets {collected_rel_objects} != expected {[sens_1_urn, sens_2_urn]}")
    if rel_page_count != 2:
        raise AssertionError(f"expected exactly 2 relationship pages with limit=1, took {rel_page_count}")

    # Malformed relationship cursor -> 400 Bad Request (code 'invalid_cursor')
    r_bad_rel_cur = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}?relationship_after=malformed_cursor_base64!",
        token=owner_token,
    )
    assert_error_envelope(r_bad_rel_cur, 400, "invalid_cursor", "malformed relationship_after")

    # 2g. Twin List keyset cursor pagination and validation (RFC 8288 rel="next" traversal)
    initial_list_uri = f"/v1/digital-twins?gateway_id={gw_id}&limit=1"
    cur_list_uri = initial_list_uri
    collected_items: list[dict] = []
    seen_item_ids: set[str] = set()
    seen_list_uris: set[str] = set()
    list_page_count = 0
    saved_list_cursor = ""

    while cur_list_uri:
        list_page_count += 1
        if list_page_count > max_pages:
            raise AssertionError(f"list pagination exceeded maximum bounded pages ({max_pages})")
        if cur_list_uri in seen_list_uris:
            raise AssertionError(f"pagination loop detected on URI: {cur_list_uri}")
        seen_list_uris.add(cur_list_uri)

        r_page = ctx.http.get(cur_list_uri, token=owner_token)
        if r_page.status != 200:
            raise AssertionError(f"list page {list_page_count} failed: {r_page.status}")
        if not isinstance(r_page.json, list):
            raise AssertionError(f"expected list array on page {list_page_count}, got: {type(r_page.json)}")
        items = r_page.json
        if len(items) != 1:
            raise AssertionError(f"expected exactly 1 item on list page {list_page_count}, got {len(items)}")

        item = items[0]
        item_id = item.get("id")
        if not item_id:
            raise AssertionError(f"missing item id on list page {list_page_count}: {item}")
        if item_id in seen_item_ids:
            raise AssertionError(f"duplicate entity id {item_id} across list pages")
        seen_item_ids.add(item_id)
        collected_items.append(item)

        link_hdr = r_page.headers.get("link", "")
        next_uri = parse_rel_next_uri(link_hdr)
        if next_uri:
            cursor_val = validate_next_uri_contract(
                next_uri,
                expected_path="/v1/digital-twins",
                expected_params={"gateway_id": gw_id, "limit": "1"},
                cursor_param_name="cursor",
            )
            saved_list_cursor = cursor_val
            cur_list_uri = next_uri
        else:
            cur_list_uri = None

    # Assert completeness and terminal no-next
    expected_ids = {gw_urn, sens_1_urn, sens_2_urn}
    if seen_item_ids != expected_ids:
        raise AssertionError(f"collected entity IDs {seen_item_ids} != expected {expected_ids}")
    if list_page_count != 3:
        raise AssertionError(f"expected exactly 3 pages for 3 entities with limit=1, took {list_page_count}")

    # Active filter preservation: type=Sensor&gateway_id={gw_id}&limit=1
    cur_sensor_uri = f"/v1/digital-twins?gateway_id={gw_id}&type=Sensor&limit=1"
    sensor_items: list[str] = []
    seen_sensor_uris: set[str] = set()
    sensor_page_count = 0
    while cur_sensor_uri:
        sensor_page_count += 1
        if sensor_page_count > max_pages:
            raise AssertionError("sensor list pagination loop")
        seen_sensor_uris.add(cur_sensor_uri)
        r_spage = ctx.http.get(cur_sensor_uri, token=owner_token)
        if r_spage.status != 200 or not isinstance(r_spage.json, list):
            raise AssertionError(f"sensor list page {sensor_page_count} failed: HTTP {r_spage.status}")
        if len(r_spage.json) != 1:
            raise AssertionError(f"expected 1 item on sensor list page {sensor_page_count}, got {len(r_spage.json)}")
        s_id = r_spage.json[0].get("id")
        if s_id in sensor_items:
            raise AssertionError(f"duplicate sensor {s_id}")
        sensor_items.append(s_id)
        n_uri = parse_rel_next_uri(r_spage.headers.get("link"))
        if n_uri:
            validate_next_uri_contract(
                n_uri,
                expected_path="/v1/digital-twins",
                expected_params={"gateway_id": gw_id, "type": "Sensor", "limit": "1"},
                cursor_param_name="cursor",
            )
            cur_sensor_uri = n_uri
        else:
            cur_sensor_uri = None
    if set(sensor_items) != {sens_1_urn, sens_2_urn}:
        raise AssertionError(f"type=Sensor pagination mismatch: {sensor_items}")
    if sensor_page_count != 2:
        raise AssertionError(f"expected exactly 2 pages for 2 sensors with limit=1, took {sensor_page_count}")

    # Malformed list cursor -> 400 Bad Request
    r_bad_cur = ctx.http.get("/v1/digital-twins?cursor=garbage_cursor_not_base64!", token=owner_token)
    assert_error_envelope(r_bad_cur, 400, "invalid_cursor", "malformed list after cursor")

    # Mismatched list cursor (cursor tied to gw_id passed with a different gateway_id)
    r_mismatch_cur = ctx.http.get(
        f"/v1/digital-twins?gateway_id=other_gw&cursor={urllib.parse.quote(saved_list_cursor)}",
        token=owner_token,
    )
    assert_error_envelope(r_mismatch_cur, 400, "invalid_cursor", "mismatched list cursor")

    # -------------------------------------------------------------------------
    # 2h. Real Container HTTP Accept 406 Negotiation and CORS Controls
    # -------------------------------------------------------------------------
    # A. Accept Header 406 Negotiation
    # 1. Incompatible media type -> 406 Not Acceptable
    r_incompat_accept = ctx.http.get(
        f"/v1/digital-twins?gateway_id={gw_id}",
        token=owner_token,
        headers={"Accept": "text/plain;q=1.0"},
    )
    assert_error_envelope(r_incompat_accept, 406, "not_acceptable", "Accept text/plain on list")

    # 2. Quoted comma in parameter before q=0 on JSON-LD read endpoint -> 406 Not Acceptable
    r_qzero_detail = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}",
        token=owner_token,
        headers={"Accept": 'application/ld+json;profile="https://example.org/a,b";q=0'},
    )
    assert_error_envelope(r_qzero_detail, 406, "not_acceptable", "quoted comma q=0 on detail")

    # 3. Quoted comma before q=0 on mutation route -> 406 Not Acceptable and 0 writer side-effects
    sens_qzero_id = f"sens_qzero_{hexid}"
    r_qzero_post = ctx.http.post(
        "/v1/digital-twins",
        token=owner_token,
        headers={"Accept": 'application/json;profile="https://example.org/a,b";q=0'},
        json_body={"entity_type": "Sensor", "gateway_id": gw_id, "sensor_id": sens_qzero_id, "name": "QZero", "unit": "c"},
    )
    assert_error_envelope(r_qzero_post, 406, "not_acceptable", "quoted comma q=0 on mutation POST")
    # Assert writer was never called: entity not created in database
    db_qzero_count = query_json_row(
        ctx.db_fixture,
        f"SELECT count(*) AS total_count FROM public.sensors WHERE gateway_id = '{gw_id}' AND sensor_id = '{sens_qzero_id}'",
    )["total_count"]
    if db_qzero_count != 0:
        raise AssertionError(f"Accept q=0 allowed writer execution, created {db_qzero_count} sensor rows")

    # 4. Positive Accept control -> 200 OK with application/ld+json
    r_accept_pos = ctx.http.get(
        f"/v1/digital-twins?gateway_id={gw_id}",
        token=owner_token,
        headers={"Accept": "application/ld+json"},
    )
    if r_accept_pos.status != 200 or not r_accept_pos.headers.get("content-type", "").startswith("application/ld+json"):
        raise AssertionError(f"positive Accept application/ld+json failed: {r_accept_pos.status} {r_accept_pos.headers}")

    # B. Real Container Origin / CORS Controls
    # 1. CORS Preflight with configured allowed Origin -> 204 No Content
    r_preflight_ok = ctx.http.options(
        "/v1/digital-twins",
        headers={
            "Origin": ctx.allowed_origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Authorization, Content-Type",
        },
    )
    if r_preflight_ok.status != 204:
        raise AssertionError(f"allowed Origin preflight failed: {r_preflight_ok.status}")
    if r_preflight_ok.headers.get("access-control-allow-origin") != ctx.allowed_origin:
        raise AssertionError(f"CORS allow origin mismatch: {r_preflight_ok.headers.get('access-control-allow-origin')}")
    allow_methods = r_preflight_ok.headers.get("access-control-allow-methods", "")
    if "POST" not in allow_methods or "GET" not in allow_methods:
        raise AssertionError(f"CORS allow methods missing methods: {allow_methods}")
    allow_headers = r_preflight_ok.headers.get("access-control-allow-headers", "").lower()
    if "authorization" not in allow_headers or "content-type" not in allow_headers:
        raise AssertionError(f"CORS allow headers missing headers: {allow_headers}")
    if "origin" not in r_preflight_ok.headers.get("vary", "").lower():
        raise AssertionError(f"CORS preflight missing Vary: Origin: {r_preflight_ok.headers.get('vary')}")
    if r_preflight_ok.headers.get("cache-control") != "no-store" or r_preflight_ok.headers.get("pragma") != "no-cache":
        raise AssertionError(f"CORS mutation preflight missing cache headers: {r_preflight_ok.headers}")

    # 2. Actual GET with allowed Origin exposes Link and Location headers
    r_get_cors = ctx.http.get(
        f"/v1/digital-twins?gateway_id={gw_id}&limit=1",
        token=owner_token,
        headers={"Origin": ctx.allowed_origin},
    )
    if r_get_cors.status != 200:
        raise AssertionError(f"actual GET with allowed Origin failed: {r_get_cors.status}")
    if r_get_cors.headers.get("access-control-allow-origin") != ctx.allowed_origin:
        raise AssertionError(f"GET allow origin mismatch: {r_get_cors.headers.get('access-control-allow-origin')}")
    expose_hdr = r_get_cors.headers.get("access-control-expose-headers", "").lower()
    if "link" not in expose_hdr or "location" not in expose_hdr:
        raise AssertionError(f"Access-Control-Expose-Headers missing Link/Location: {expose_hdr}")
    if not r_get_cors.headers.get("link"):
        raise AssertionError("GET with limit=1 missing Link header to expose")
    if "origin" not in r_get_cors.headers.get("vary", "").lower():
        raise AssertionError(f"GET response missing Vary: Origin: {r_get_cors.headers.get('vary')}")

    # 3. Exact replay POST with allowed Origin exposes Location
    r_post_cors = ctx.http.post(
        "/v1/digital-twins",
        token=admin_token,
        headers={"Origin": ctx.allowed_origin},
        json_body=gw_payload,
    )
    if r_post_cors.status != 200:
        raise AssertionError(f"POST with allowed Origin failed: {r_post_cors.status}")
    if r_post_cors.headers.get("access-control-allow-origin") != ctx.allowed_origin:
        raise AssertionError(f"POST allow origin mismatch: {r_post_cors.headers.get('access-control-allow-origin')}")
    expose_post = r_post_cors.headers.get("access-control-expose-headers", "").lower()
    if "location" not in expose_post:
        raise AssertionError(f"POST Access-Control-Expose-Headers missing Location: {expose_post}")

    # 4. Denied Origin Preflight positive control -> 403 Forbidden
    r_preflight_denied = ctx.http.options(
        "/v1/digital-twins",
        headers={
            "Origin": ctx.denied_origin,
            "Access-Control-Request-Method": "POST",
        },
    )
    assert_error_envelope(r_preflight_denied, 403, "forbidden", "denied origin CORS preflight")
    if "access-control-allow-origin" in r_preflight_denied.headers:
        raise AssertionError("denied preflight must not set Access-Control-Allow-Origin")

    # 5. Denied Origin Actual GET positive control -> 200 OK without CORS allow headers
    r_get_denied = ctx.http.get(
        f"/v1/digital-twins?gateway_id={gw_id}",
        token=owner_token,
        headers={"Origin": ctx.denied_origin},
    )
    if r_get_denied.status != 200:
        raise AssertionError(f"actual GET with denied Origin failed: {r_get_denied.status}")
    if "access-control-allow-origin" in r_get_denied.headers:
        raise AssertionError("actual GET with denied Origin must not set Access-Control-Allow-Origin")
    if "access-control-expose-headers" in r_get_denied.headers:
        raise AssertionError("actual GET with denied Origin must not set Access-Control-Expose-Headers")
    if "origin" not in r_get_denied.headers.get("vary", "").lower():
        raise AssertionError("actual GET with denied Origin missing Vary: Origin")

    # -------------------------------------------------------------------------
    # 3. Representative HTTP Query and Body Boundaries
    # -------------------------------------------------------------------------
    print("stage=contract:section3-boundaries", flush=True)

    # 3a. Raw overflow 400 'raw_limit_exceeded' vs Auto fallback 200 OK
    # Query window > 10s (default History.RawWindow)
    from_time = "2026-10-18T10:00:00Z"
    to_time_15s = "2026-10-18T10:00:15Z"  # 15s > 10s
    to_time_5s = "2026-10-18T10:00:05Z"   # 5s <= 10s

    # Range > 10s without period duration is raw overflow -> 400 Bad Request (raw_limit_exceeded)
    r_raw_overflow = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt={from_time}&endTimeAt={to_time_15s}",
        token=owner_token,
    )
    assert_error_envelope(r_raw_overflow, 400, "raw_limit_exceeded", "raw window > 10s overflow")

    # With aggrPeriodDuration=PT1S and range > 10s -> 200 OK bucket mode
    r_auto_fallback = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt={from_time}&endTimeAt={to_time_15s}&aggrPeriodDuration=PT1S",
        token=owner_token,
    )
    if r_auto_fallback.status != 200:
        raise AssertionError(f"bucket mode temporal query failed: {r_auto_fallback.status} {r_auto_fallback.json}")

    # 3b. Bad precision, range, resolution, and property
    # Invalid resolution string -> 400 invalid_resolution
    r_bad_res = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt={from_time}&endTimeAt={to_time_5s}&aggrPeriodDuration=2hours",
        token=owner_token,
    )
    assert_error_envelope(r_bad_res, 400, "invalid_resolution", "unsupported resolution")

    # Invalid timestamp format -> 400 invalid_request
    r_bad_ts = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt=not-a-date&endTimeAt={to_time_5s}",
        token=owner_token,
    )
    assert_error_envelope(r_bad_ts, 400, "invalid_request", "invalid from timestamp")

    # Inverted time range (from > to) -> 400 invalid_request
    r_inv_range = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt={to_time_5s}&endTimeAt={from_time}",
        token=owner_token,
    )
    assert_error_envelope(r_inv_range, 400, "invalid_request", "inverted time range")

    # Range > 744h (maxRange) -> 400 range_too_large
    r_huge_range = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=measurement&timerel=between&timeAt=2025-01-01T00:00:00Z&endTimeAt=2026-01-01T00:00:00Z",
        token=owner_token,
    )
    assert_error_envelope(r_huge_range, 400, "range_too_large", "range > 744h")

    # Unsupported property on Twin temporal -> 400 invalid_property
    r_bad_prop = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/temporal?attrs=unknown_property&timerel=between&timeAt={from_time}&endTimeAt={to_time_5s}",
        token=owner_token,
    )
    assert_error_envelope(r_bad_prop, 400, "invalid_property", "unsupported property")

    # Gateway has no history properties -> 400 invalid_property
    r_gw_hist = ctx.http.get(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/temporal?attrs=measurement&timerel=between&timeAt={from_time}&endTimeAt={to_time_5s}",
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
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        json_body={"name": {"type": "Property", "value": "New Name"}, "unit": {"type": "Property", "value": "fahrenheit"}},
    )
    assert_error_envelope(r_unit_patch, 400, "invalid_request", "forbidden unit on Sensor PATCH")

    # PATCH Sensor with injected entity_type or gateway_id -> 400
    r_inj_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        json_body={"name": {"type": "Property", "value": "New Name"}, "entity_type": "Gateway"},
    )
    assert_error_envelope(r_inj_patch, 400, "invalid_request", "injected field on Sensor PATCH")

    # PATCH Gateway with injected owner_user_id -> 400
    r_gw_inj = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=admin_token,
        json_body={"description": {"type": "Property", "value": "New Desc"}, "owner_user_id": fake_admin_uid},
    )
    assert_error_envelope(r_gw_inj, 400, "invalid_request", "injected owner_user_id on Gateway PATCH")

    # Duplicate JSON keys -> 400
    raw_dup_json = b'{"name": {"type": "Property", "value": "Valid 1"}, "name": {"type": "Property", "value": "Valid 2"}}'
    r_dup_keys = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        headers={"Content-Type": "application/json"},
        raw_body=raw_dup_json,
    )
    assert_error_envelope(r_dup_keys, 400, "invalid_request", "duplicate JSON keys")

    # Nested duplicate JSON keys inside Property -> 400
    raw_nested_dup_json = b'{"name": {"type": "Property", "type": "Property", "value": "Valid 1"}}'
    r_nested_dup = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        headers={"Content-Type": "application/json"},
        raw_body=raw_nested_dup_json,
    )
    assert_error_envelope(r_nested_dup, 400, "invalid_request", "nested duplicate JSON keys")

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
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        headers={"Content-Type": "text/plain"},
        raw_body=b'{"name": {"type": "Property", "value": "test"}}',
    )
    assert_error_envelope(r_wrong_media_patch, 415, "unsupported_media_type", "PATCH text/plain")

    # Payload exceeding limit (> 65536 bytes) -> 413 Payload Too Large
    huge_body = json.dumps({"name": {"type": "Property", "value": "A" * 70000}}).encode("utf-8")
    r_oversize = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        headers={"Content-Type": "application/json"},
        raw_body=huge_body,
    )
    assert_error_envelope(r_oversize, 413, "payload_too_large", "oversize body 70KB")

    # Query parameters on mutation paths -> 400 Bad Request
    r_query_patch = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs?forbidden=true",
        token=owner_token,
        json_body={"name": {"type": "Property", "value": "Valid Name"}},
    )
    assert_error_envelope(r_query_patch, 400, "invalid_request", "query param on PATCH")

    # 3f. Exact replay 200 vs Conflicting representation 409 Conflict
    # Exact replay of Gateway creation -> 200 OK
    r_replay_gw = ctx.http.post("/v1/digital-twins", token=admin_token, json_body=gw_payload)
    if r_replay_gw.status != 200:
        raise AssertionError(f"exact replay Gateway failed: {r_replay_gw.status}")
    if r_replay_gw.headers.get("location"):
        raise AssertionError(f"replay response must not have Location header: {r_replay_gw.headers.get('location')}")

    # Conflicting Gateway creation (different name/owner) -> 409 Conflict
    conflict_gw_payload = dict(gw_payload)
    conflict_gw_payload["name"] = "Conflicting Gateway Name"
    r_conflict_gw = ctx.http.post("/v1/digital-twins", token=admin_token, json_body=conflict_gw_payload)
    assert_error_envelope(r_conflict_gw, 409, "conflict", "conflicting Gateway representation")

    # Exact replay of Sensor creation -> 200 OK
    r_replay_sens = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=sens_1_payload)
    if r_replay_sens.status != 200:
        raise AssertionError(f"exact replay Sensor failed: {r_replay_sens.status}")
    if r_replay_sens.headers.get("location"):
        raise AssertionError(f"replay response must not have Location header: {r_replay_sens.headers.get('location')}")

    # Conflicting Sensor creation (different unit) -> 409 Conflict
    conflict_sens_payload = dict(sens_1_payload)
    conflict_sens_payload["unit"] = "kelvin"
    r_conflict_sens = ctx.http.post("/v1/digital-twins", token=owner_token, json_body=conflict_sens_payload)
    assert_error_envelope(r_conflict_sens, 409, "conflict", "conflicting Sensor representation")

    # 3g. PATCH omission, null description, and no-op update
    # Admin PATCH Gateway setting description to null -> sets to SQL NULL
    r_null_desc = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=admin_token,
        json_body={"description": {"type": "Property", "value": "urn:ngsi-ld:null"}},
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
        f"/v1/digital-twins/{urllib.parse.quote(gw_urn)}/attrs",
        token=admin_token,
        json_body={"name": {"type": "Property", "value": f"Renamed Gateway {gw_id}"}},
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
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        json_body={"name": {"type": "Property", "value": sensor_name_row["name"]}},
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
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        raw_body=b'{"name": null}',
        headers={"Content-Type": "application/json"},
    )
    assert_error_envelope(r_null_name, 400, "invalid_request", "null Sensor name")

    r_blank_name = ctx.http.patch(
        f"/v1/digital-twins/{urllib.parse.quote(sens_1_urn)}/attrs",
        token=owner_token,
        json_body={"name": {"type": "Property", "value": "   "}},
    )
    assert_error_envelope(r_blank_name, 400, "invalid_request", "blank Sensor name")

    print(f"=== 4.7.4 Contract Scenario completed successfully for {gw_id} ===", flush=True)
    return {
        "gateway_id": gw_id,
        "auth_denials_verified": True,
        "rbac_mutation_denials_verified": True,
        "http_boundaries_verified": True,
        "rel_next_traversal_verified": True,
        "accept_and_cors_controls_verified": True,
    }
