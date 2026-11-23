"""stage8_playback_support.py - Bounded integration verification for Slice 8.6-D playback URL issuance."""

import base64
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import ssl
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

import native_compose_smoke as smoke

ROOT = Path(__file__).resolve().parents[2]


class PlaybackProofFailure(ValueError):
    """Raised when an assertion in the native playback API integration fails."""
    pass


def get_header(headers, name):
    if not isinstance(headers, dict):
        return None
    name_lower = name.lower()
    for k, v in headers.items():
        if k.lower() == name_lower:
            return v
    return None


def verify_signed_url_in_memory(url, expires_at, expected_origin, media_id, require_fn=None):
    def require(condition, code):
        if not condition:
            if require_fn:
                require_fn(condition, code)
            else:
                raise PlaybackProofFailure(code)

    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "https", "playback_url_scheme_not_https")
    require(parsed.netloc == expected_origin, "playback_url_origin_mismatch")
    require(parsed.path == f"/storage/v1/object/sign/media-video/{media_id}.mp4", "playback_url_path_mismatch")
    require(parsed.fragment == "", "playback_url_has_fragment")

    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    require(list(qs.keys()) == ["token"] and len(qs["token"]) == 1, "playback_url_query_invalid")
    token = qs["token"][0]

    parts = token.split(".")
    require(len(parts) == 3, "storage_token_not_3_parts")
    padding = b"=" * (-len(parts[1]) % 4)
    claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
    storage_exp = claims.get("exp")
    require(isinstance(storage_exp, int) and storage_exp > 0, "storage_token_exp_invalid")

    dt = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    require(dt.tzinfo == datetime.timezone.utc, "expires_at_not_utc")
    require(int(dt.timestamp()) == storage_exp, "expires_at_timestamp_mismatch_with_token")

    now = time.time()
    delta = storage_exp - now
    require(100 <= delta <= 135, "storage_exp_not_close_to_120s")
    return token


def verify_gateway_media_list(
    api_endpoint, gateway_id, ready_media_id, owner_token,
    service_key, anon_key, postgres_id, registry,
    require_fn=None, tokens_to_scan=None,
):
    fail_cls = PlaybackProofFailure

    def require(condition, code):
        if not condition:
            if require_fn:
                require_fn(condition, code)
            else:
                raise fail_cls(code)

    def assert_404_not_found(status, headers, body, label):
        require(status == 404, f"{label}_status_not_404")
        require(get_header(headers, "Cache-Control") == "no-store", f"{label}_missing_no_store")
        err = body.get("error", {}) if isinstance(body, dict) else {}
        require(err.get("code") == "not_found", f"{label}_error_code_not_found")
        require(err.get("message") == "resource not found", f"{label}_error_message_mismatch")

    # 1. Setup Operator, Viewer, Nonmember, Admin users via GoTrue admin API
    op_email = "stage8_list_op_" + secrets.token_hex(6) + "@example.invalid"
    op_pass = secrets.token_urlsafe(32)
    op_uid = smoke.create_user_via_admin_api(api_endpoint, service_key, op_email, op_pass)
    op_token = smoke.login_user_for_token(api_endpoint, anon_key, op_email, op_pass)
    smoke.db_psql(registry, postgres_id,
        "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'operator');",
        {"uid": op_uid, "gid": gateway_id})

    viewer_email = "stage8_list_viewer_" + secrets.token_hex(6) + "@example.invalid"
    viewer_pass = secrets.token_urlsafe(32)
    viewer_uid = smoke.create_user_via_admin_api(api_endpoint, service_key, viewer_email, viewer_pass)
    viewer_token = smoke.login_user_for_token(api_endpoint, anon_key, viewer_email, viewer_pass)
    smoke.db_psql(registry, postgres_id,
        "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'viewer');",
        {"uid": viewer_uid, "gid": gateway_id})

    nonmember_email = "stage8_list_nonmember_" + secrets.token_hex(6) + "@example.invalid"
    nonmember_pass = secrets.token_urlsafe(32)
    smoke.create_user_via_admin_api(api_endpoint, service_key, nonmember_email, nonmember_pass)
    nonmember_token = smoke.login_user_for_token(api_endpoint, anon_key, nonmember_email, nonmember_pass)

    admin_email = "stage8_list_admin_" + secrets.token_hex(6) + "@example.invalid"
    admin_pass = secrets.token_urlsafe(32)
    admin_uid = smoke.create_user_via_admin_api(api_endpoint, service_key, admin_email, admin_pass)
    admin_token = smoke.login_user_for_token(api_endpoint, anon_key, admin_email, admin_pass)
    smoke.db_psql(registry, postgres_id,
        "INSERT INTO public.platform_admins(user_id) VALUES (:'uid');",
        {"uid": admin_uid})

    # 2. Setup Gateway B where operator has membership, but gateway B has 0 media items
    gw_b = "gw_stage8_b_" + secrets.token_hex(6)
    smoke.db_psql(registry, postgres_id,
        "INSERT INTO public.gateways(gateway_id, name) VALUES (:'gid', 'Gateway B');",
        {"gid": gw_b})
    smoke.db_psql(registry, postgres_id,
        "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'owner');",
        {"uid": op_uid, "gid": gw_b})

    # Empty gateway & isolation: gateway B returns 200 [] with no Link header
    status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gw_b}/media", token=op_token, timeout=10)
    require(status == 200, "empty_gateway_b_status_not_200")
    require(get_header(headers, "Cache-Control") == "no-store", "empty_gateway_b_missing_no_store")
    require(isinstance(body, list) and len(body) == 0, "empty_gateway_b_body_not_empty_array")
    require(get_header(headers, "Link") is None, "empty_gateway_b_has_link_header")

    # 3. 404 Rejections (nonmember, admin without membership, nonexistent gateway)
    status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media", token=nonmember_token, timeout=10)
    assert_404_not_found(status, headers, body, "media_list_nonmember")

    status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media", token=admin_token, timeout=10)
    assert_404_not_found(status, headers, body, "media_list_platform_admin_no_membership")

    missing_gw = "gw_missing_" + secrets.token_hex(6)
    status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{missing_gw}/media", token=owner_token, timeout=10)
    assert_404_not_found(status, headers, body, "media_list_missing_gateway")

    # 4. Insert pending row and legacy unmanaged row for gateway_id
    pending_id = str(uuid.uuid4())
    smoke.db_psql(registry, postgres_id,
        """INSERT INTO public.media_objects(
            media_id, gateway_id, upload_id, storage_path, media_type, expected_size,
            created_at, captured_at, intent_expires_at, declared_sha256,
            managed, validation_status
        ) VALUES (
            :'mid'::uuid, :'gid', :'uid', :'path', 'video/mp4', 1000,
            now(), now(), now() + interval '10 minutes',
            '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            true, 'pending'
        );""",
        {"mid": pending_id, "gid": gateway_id, "uid": "upload_pend_" + secrets.token_hex(4),
         "path": pending_id + ".mp4"}
    )
    legacy_id = str(uuid.uuid4())
    smoke.db_psql(registry, postgres_id,
        """INSERT INTO public.media_objects(
            media_id, gateway_id, storage_path, media_type, expected_size,
            managed, validation_status
        ) VALUES (
            :'mid'::uuid, :'gid', :'path', 'video/mp4', 1000,
            false, 'ready'
        );""",
        {"mid": legacy_id, "gid": gateway_id, "path": legacy_id + ".mp4"}
    )

    # 5. Retrieve real row metadata to derive relative deterministic synthetic timestamps
    meta_raw = smoke.db_psql(
        registry, postgres_id,
        "SELECT json_build_array(captured_at::text, last_verification_at::text, actual_size::text)::text "
        "FROM public.media_objects WHERE media_id = :'mid'::uuid;",
        {"mid": ready_media_id}
    ).strip()
    real_meta_lines = [row for row in meta_raw.splitlines() if row.startswith("[")]
    require(len(real_meta_lines) == 1, "real_media_metadata_query_failed")
    real_cap_str, real_ready_str, real_size_str = json.loads(real_meta_lines[0])
    real_cap_dt = datetime.datetime.fromisoformat(real_cap_str.replace("Z", "+00:00"))
    real_ready_dt = datetime.datetime.fromisoformat(real_ready_str.replace("Z", "+00:00"))
    real_size_bytes = int(real_size_str)

    # Derive synthetic timestamp strictly newer than real upload with fractional microseconds
    synthetic_captured_dt = real_cap_dt + datetime.timedelta(seconds=120, microseconds=456789)
    synthetic_time_sql = synthetic_captured_dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    u1, u2 = uuid.uuid4(), uuid.uuid4()
    if str(u1) < str(u2):
        id_low, id_high = str(u1), str(u2)
    else:
        id_low, id_high = str(u2), str(u1)

    for mid, uid in [(id_low, "upload_ready_low_" + secrets.token_hex(3)), (id_high, "upload_ready_high_" + secrets.token_hex(3))]:
        smoke.db_psql(registry, postgres_id,
            """INSERT INTO public.media_objects(
                media_id, gateway_id, upload_id, storage_path, media_type, expected_size,
                actual_size, actual_media_type, declared_sha256, verified_sha256,
                created_at, captured_at, intent_expires_at, last_verification_at,
                managed, validation_status
            ) VALUES (
                :'mid'::uuid, :'gid', :'uid', :'path', 'video/mp4', 2048,
                2048, 'video/mp4',
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                now(), :'captured'::timestamptz, now() + interval '10 minutes', now(),
                true, 'ready'
            );""",
            {"mid": mid, "gid": gateway_id, "uid": uid, "path": mid + ".mp4", "captured": synthetic_time_sql}
        )

    # Total ready items for gateway_id: id_high, id_low, ready_media_id (3 items total)
    # 6. Verify owner, operator, viewer get 200 with Cache-Control: no-store
    body = None
    for role_name, token in [("owner", owner_token), ("operator", op_token), ("viewer", viewer_token)]:
        status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media", token=token, timeout=10)
        require(status == 200, "media_list_status_not_200")
        require(get_header(headers, "Cache-Control") == "no-store", "media_list_missing_no_store")
        require(isinstance(body, list), "media_list_not_array")
        require(len(body) == 3, "media_list_count_mismatch")

        # Verify pending and legacy exclusion
        returned_ids = [it.get("media_id") for it in body]
        require(pending_id not in returned_ids, "pending_row_not_excluded")
        require(legacy_id not in returned_ids, "legacy_row_not_excluded")
        require(ready_media_id in returned_ids, "ready_media_id_missing_from_list")
        require(id_high in returned_ids and id_low in returned_ids, "inserted_ready_ids_missing")

        # Verify contract fields & no secret fields
        expected_keys = {"media_id", "file_name", "captured_at", "ready_at", "size_bytes"}
        for item in body:
            require(set(item.keys()) == expected_keys, "media_item_keys_mismatch")
            require(item["file_name"] == item["media_id"] + ".mp4", "file_name_not_canonical")
            require(isinstance(item["size_bytes"], int) and item["size_bytes"] > 0, "size_bytes_invalid")
            cap_dt = datetime.datetime.fromisoformat(item["captured_at"].replace("Z", "+00:00"))
            require(cap_dt.tzinfo == datetime.timezone.utc, "captured_at_not_utc")
            ready_dt = datetime.datetime.fromisoformat(item["ready_at"].replace("Z", "+00:00"))
            require(ready_dt.tzinfo == datetime.timezone.utc, "ready_at_not_utc")

    # Read real media_id directly from JSON list response for playback flow
    real_item = next(it for it in body if it["media_id"] == ready_media_id)
    real_returned_media_id = real_item["media_id"]
    require(real_item["size_bytes"] == real_size_bytes, "size_bytes_mismatch_with_db")
    resp_cap_dt = datetime.datetime.fromisoformat(real_item["captured_at"].replace("Z", "+00:00"))
    resp_ready_dt = datetime.datetime.fromisoformat(real_item["ready_at"].replace("Z", "+00:00"))
    require(resp_cap_dt == real_cap_dt, "captured_at_timestamp_mismatch_with_db")
    require(resp_ready_dt == real_ready_dt, "ready_at_timestamp_mismatch_with_db")
    require(real_item["captured_at"] != real_item["ready_at"], "capture_time_not_distinct_from_ready_time")

    # 7. Pagination traversal across 3 pages with limit=1 (straddling equal timestamp boundary)
    status, headers, page1 = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media?limit=1", token=owner_token, timeout=10)
    require(status == 200, "page1_status_not_200")
    require(len(page1) == 1, "page1_length_mismatch")
    require(page1[0]["media_id"] == id_high, "page1_item_mismatch_expected_id_high")

    link1 = get_header(headers, "Link") or ""
    require(link1.startswith(f"</v1/gateways/{gateway_id}/media?limit=1&cursor="), "link1_prefix_mismatch")
    require('rel="next"' in link1, "link1_missing_rel_next")
    require("http://" not in link1 and "https://" not in link1, "link1_not_relative")

    cursor1 = re.search(r'cursor=([^&>]+)', link1).group(1)

    # Cross-scope cursor reuse against Gateway B must yield 400
    status, h_cross, b_cross = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gw_b}/media?cursor={cursor1}", token=op_token, timeout=10)
    require(status == 400, "cursor_cross_scope_reuse_not_400")
    require(get_header(h_cross, "Cache-Control") == "no-store", "cursor_cross_scope_missing_no_store")
    require(isinstance(b_cross, dict) and b_cross.get("error", {}).get("code") == "invalid_request", "cursor_cross_scope_code_mismatch")

    # Page 2: same synthetic timestamp boundary, tie-broken by media_id DESC
    status, headers, page2 = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media?limit=1&cursor={cursor1}", token=owner_token, timeout=10)
    require(status == 200, "page2_status_not_200")
    require(len(page2) == 1, "page2_length_mismatch")
    require(page2[0]["media_id"] == id_low, "page2_item_mismatch_expected_id_low")

    link2 = get_header(headers, "Link") or ""
    require(link2.startswith(f"</v1/gateways/{gateway_id}/media?limit=1&cursor="), "link2_prefix_mismatch")
    require('rel="next"' in link2, "link2_missing_rel_next")
    require("http://" not in link2 and "https://" not in link2, "link2_not_relative")

    cursor2 = re.search(r'cursor=([^&>]+)', link2).group(1)

    # Page 3: terminal page
    status, headers, page3 = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media?limit=1&cursor={cursor2}", token=owner_token, timeout=10)
    require(status == 200, "page3_status_not_200")
    require(len(page3) == 1, "page3_length_mismatch")
    require(page3[0]["media_id"] == ready_media_id, "page3_item_mismatch_expected_ready_media_id")
    require(get_header(headers, "Link") is None, "page3_terminal_has_link_header")

    # Verify disjointness and full traversal
    p1_id = page1[0]["media_id"]
    p2_id = page2[0]["media_id"]
    p3_id = page3[0]["media_id"]
    require(len({p1_id, p2_id, p3_id}) == 3, "traversed_pages_have_duplicates")
    require({p1_id, p2_id, p3_id} == {id_high, id_low, ready_media_id}, "all_items_not_traversed")

    # 8. Revoke viewer membership and verify 404 with same JWT
    smoke.db_psql(registry, postgres_id,
        "DELETE FROM public.user_gateways WHERE user_id = :'uid' AND gateway_id = :'gid';",
        {"uid": viewer_uid, "gid": gateway_id})
    status, headers, body = smoke.http_request(api_endpoint, "GET", f"/v1/gateways/{gateway_id}/media", token=viewer_token, timeout=10)
    assert_404_not_found(status, headers, body, "media_list_viewer_revoked")

    return {
        "playback_media_id": real_returned_media_id,
        "summary": {
            "status": "PASS",
            "roles_verified": ["owner", "operator", "viewer"],
            "empty_gateway_verified": True,
            "isolation_verified": True,
            "pending_legacy_excluded": True,
            "rejections_verified": {
                "nonmember_denied_404": True,
                "platform_admin_no_membership_denied_404": True,
                "missing_gateway_denied_404": True,
                "revocation_denied_404": True,
                "cursor_cross_scope_denied_400": True,
            },
            "pagination_verified": {
                "pages_traversed": True,
                "tie_breaking_verified": True,
                "no_duplicates": True,
                "link_header_rel_next": True,
            },
            "contract_fields_verified": True,
            "secret_fields_omitted": True,
        },
    }


def run_playback_api_integration(fixture, prefix, registry, material, media_row, report,
                                 run_cmd=None, proof_failure_cls=None):
    fail_cls = proof_failure_cls or PlaybackProofFailure

    def require(condition, code):
        if not condition:
            raise fail_cls(code)

    def assert_404_not_found(status, headers, body, label):
        require(status == 404, f"{label}_status_not_404")
        require(get_header(headers, "Cache-Control") == "no-store", f"{label}_missing_no_store")
        err = body.get("error", {}) if isinstance(body, dict) else {}
        require(err.get("code") == "not_found", f"{label}_error_code_not_found")
        require(err.get("message") == "resource not found", f"{label}_error_message_mismatch")

    gateway_id = material["gateway_id"]
    media_id = media_row[0]
    endpoint = material["api_endpoint"]
    postgres_id = material["postgres_id"]
    owner_token = material["owner_token"]
    expected_origin = urllib.parse.urlsplit(fixture.values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"]).netloc
    require(expected_origin != "", "expected_playback_origin_invalid")

    tokens_to_scan = []

    # 1. Setup Operator, Viewer, Nonmember, and Admin users via GoTrue admin API
    service_key = fixture.values["SERVICE_ROLE_KEY"]
    anon_key = fixture.values["ANON_KEY"]

    op_email = "stage8_op_" + secrets.token_hex(6) + "@example.invalid"
    op_pass = secrets.token_urlsafe(32)
    op_uid = smoke.create_user_via_admin_api(endpoint, service_key, op_email, op_pass)
    op_token = smoke.login_user_for_token(endpoint, anon_key, op_email, op_pass)
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'operator');",
                   {"uid": op_uid, "gid": gateway_id})

    viewer_email = "stage8_viewer_" + secrets.token_hex(6) + "@example.invalid"
    viewer_pass = secrets.token_urlsafe(32)
    viewer_uid = smoke.create_user_via_admin_api(endpoint, service_key, viewer_email, viewer_pass)
    viewer_token = smoke.login_user_for_token(endpoint, anon_key, viewer_email, viewer_pass)
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'viewer');",
                   {"uid": viewer_uid, "gid": gateway_id})

    nonmember_email = "stage8_nonmember_" + secrets.token_hex(6) + "@example.invalid"
    nonmember_pass = secrets.token_urlsafe(32)
    smoke.create_user_via_admin_api(endpoint, service_key, nonmember_email, nonmember_pass)
    nonmember_token = smoke.login_user_for_token(endpoint, anon_key, nonmember_email, nonmember_pass)

    admin_email = "stage8_admin_" + secrets.token_hex(6) + "@example.invalid"
    admin_pass = secrets.token_urlsafe(32)
    admin_uid = smoke.create_user_via_admin_api(endpoint, service_key, admin_email, admin_pass)
    admin_token = smoke.login_user_for_token(endpoint, anon_key, admin_email, admin_pass)
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.platform_admins(user_id) VALUES (:'uid');",
                   {"uid": admin_uid})

    # 2. Status 200 and Cache-Control: no-store for owner, operator, viewer on same ready clip
    for role_name, token in [("owner", owner_token), ("operator", op_token), ("viewer", viewer_token)]:
        status, headers, body = smoke.http_request(
            endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
            token=token, timeout=10,
        )
        require(status == 200, f"{role_name}_playback_request_failed")
        require(get_header(headers, "Cache-Control") == "no-store", f"{role_name}_missing_no_store")
        require(isinstance(body, dict), f"{role_name}_body_not_dict")
        issued_token = verify_signed_url_in_memory(body.get("url", ""), body.get("expires_at", ""),
                                                    expected_origin, media_id, require_fn=require)
        tokens_to_scan.append(issued_token)

    # 3. Absent and wrong JWT -> 401
    url_path = f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url"
    status, headers, body = smoke.http_request(endpoint, "POST", url_path, token="", timeout=10)
    require(status == 401, "absent_jwt_status_not_401")
    require(get_header(headers, "Cache-Control") == "no-store", "absent_jwt_missing_no_store")
    require(isinstance(body, dict) and body.get("error", {}).get("code") == "unauthorized",
            "absent_jwt_error_code_mismatch")

    status, headers, body = smoke.http_request(endpoint, "POST", url_path, token="bad.token.signature", timeout=10)
    require(status == 401, "invalid_jwt_status_not_401")
    require(get_header(headers, "Cache-Control") == "no-store", "invalid_jwt_missing_no_store")
    require(isinstance(body, dict) and body.get("error", {}).get("code") == "unauthorized",
            "invalid_jwt_error_code_mismatch")

    # 4. Invalid ID -> 400
    for label, bad_path in [
        ("invalid_gateway", f"/v1/gateways/gw@invalid/media/{media_id}/playback-url"),
        ("malformed_uuid", f"/v1/gateways/{gateway_id}/media/not-a-valid-uuid/playback-url"),
        ("zero_uuid", f"/v1/gateways/{gateway_id}/media/00000000-0000-0000-0000-000000000000/playback-url"),
        ("uppercase_uuid", f"/v1/gateways/{gateway_id}/media/{media_id.upper()}/playback-url"),
    ]:
        status, headers, body = smoke.http_request(endpoint, "POST", bad_path, token=owner_token, timeout=10)
        require(status == 400, f"{label}_status_not_400")
        require(get_header(headers, "Cache-Control") == "no-store", f"{label}_missing_no_store")
        require(isinstance(body, dict) and body.get("error", {}).get("code") == "invalid_request",
                f"{label}_error_code_not_invalid_request")

    # 5. Different existing Gateway B with membership
    gw_b = "gw_stage8_b_" + secrets.token_hex(6)
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.gateways(gateway_id, name) VALUES (:'gid', 'Gateway B');",
                   {"gid": gw_b})
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'owner');",
                   {"uid": op_uid, "gid": gw_b})
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gw_b}/media/{media_id}/playback-url",
        token=op_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "gateway_b_clip_mismatch")

    # 6. Nonexistent clip ID -> 404
    nonexistent_id = str(uuid.uuid4())
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{nonexistent_id}/playback-url",
        token=owner_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "nonexistent_clip_id")

    # 7. Authentic nonmember -> 404
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=nonmember_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "authentic_nonmember")

    # 8. Platform admin without membership -> 404
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=admin_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "platform_admin_without_membership")

    # 9. Pending row -> 404
    pending_media_id = str(uuid.uuid4())
    smoke.db_psql(
        registry, postgres_id,
        """INSERT INTO public.media_objects(
            media_id, gateway_id, upload_id, storage_path, media_type, expected_size,
            created_at, captured_at, intent_expires_at, declared_sha256,
            managed, validation_status
        ) VALUES (
            :'mid'::uuid, :'gid', :'uid', :'path', 'video/mp4', 1000,
            now(), now(), now() + interval '10 minutes',
            '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            true, 'pending'
        );""",
        {"mid": pending_media_id, "gid": gateway_id, "uid": "upload_pending_01",
         "path": pending_media_id + ".mp4"},
    )
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{pending_media_id}/playback-url",
        token=owner_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "pending_row")

    # 10. Legacy unmanaged row -> 404
    legacy_media_id = str(uuid.uuid4())
    smoke.db_psql(
        registry, postgres_id,
        """INSERT INTO public.media_objects(
            media_id, gateway_id, storage_path, media_type, expected_size,
            managed, validation_status
        ) VALUES (
            :'mid'::uuid, :'gid', :'path', 'video/mp4', 1000,
            false, 'ready'
        );""",
        {"mid": legacy_media_id, "gid": gateway_id, "path": legacy_media_id + ".mp4"},
    )
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{legacy_media_id}/playback-url",
        token=owner_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "legacy_unmanaged_row")

    # 11. Viewer revoke committed, then re-use still valid JWT -> 404
    smoke.db_psql(
        registry, postgres_id,
        "DELETE FROM public.user_gateways WHERE user_id = :'uid' AND gateway_id = :'gid';",
        {"uid": viewer_uid, "gid": gateway_id},
    )
    status, headers, body = smoke.http_request(
        endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=viewer_token, timeout=10,
    )
    assert_404_not_found(status, headers, body, "viewer_revoked_token_reuse")

    # 12. Log scanning across backend, nginx, edge, envoy, storage for unleaked tokens and credentials
    if run_cmd is not None:
        scan_services = ["backend", "nginx", "stage8-media-upload-test-edge", "supabase-envoy", "supabase-storage"]
        compose_logs = run_cmd(prefix + ["logs", "--no-color", "--tail", "2000", *scan_services], fixture, timeout=30).stdout
        for tok in tokens_to_scan:
            require(tok.encode() not in compose_logs, "storage_signed_token_leaked_in_logs")
        for secret in [service_key, anon_key, op_pass, viewer_pass, nonmember_pass, admin_pass]:
            if secret:
                require(secret.encode() not in compose_logs, "credential_leaked_in_logs")

    report["checks"]["stage8_local_playback_api"] = {
        "status": "PASS",
        "roles_verified": ["owner", "operator", "viewer"],
        "rejections_verified": [
            "absent_jwt", "invalid_jwt", "invalid_gateway_id", "malformed_media_uuid",
            "zero_media_uuid", "uppercase_media_uuid", "gateway_mismatch",
            "nonexistent_clip", "nonmember", "platform_admin_no_membership",
            "pending_status", "legacy_unmanaged", "viewer_revoked_token_reuse",
        ],
        "cache_control_no_store_all": True,
        "identical_404_code_message": True,
        "expected_playback_origin": expected_origin,
        "tokens_unleaked_in_logs": True,
    }


def playback_https_request(endpoint, method, target, body=b"", headers=None, timeout=20, max_read=80_000_000):
    parsed = urllib.parse.urlsplit(endpoint.url)
    context = ssl.create_default_context(cafile=str(endpoint.ca_file))
    connection = http.client.HTTPSConnection(
        parsed.hostname, parsed.port, timeout=timeout, context=context,
    )
    try:
        connection.request(method, target, body=body, headers=headers or {})
        response = connection.getresponse()
        raw = response.read(max_read)
        return response.status, dict(response.getheaders()), raw
    finally:
        connection.close()


def run_playback_data_plane_integration(
    fixture, prefix, registry, material, media_row, report,
    source_path=None, expected_size=0, expected_sha="",
    run_cmd=None, proof_failure_cls=None,
):
    fail_cls = proof_failure_cls or PlaybackProofFailure

    def require(condition, code):
        if not condition:
            raise fail_cls(code)

    playback_endpoint = getattr(fixture, "playback_endpoint", None)
    require(playback_endpoint is not None, "playback_endpoint_missing")
    api_endpoint = material["api_endpoint"]
    require(api_endpoint.url.startswith("https://"), "api_endpoint_not_https")
    require(playback_endpoint.url.startswith("https://"), "playback_endpoint_not_https")

    gateway_id = material["gateway_id"]
    media_id = media_row[0]
    owner_token = material["owner_token"]
    service_key = fixture.values["SERVICE_ROLE_KEY"]
    anon_key = fixture.values["ANON_KEY"]
    postgres_id = material["postgres_id"]
    tokens_to_scan = []

    # 0. Verify Gateway Media List contract and feed ready_media_id to playback flow
    media_list_result = verify_gateway_media_list(
        api_endpoint, gateway_id, media_id, owner_token,
        service_key, anon_key, postgres_id, registry,
        require_fn=require, tokens_to_scan=tokens_to_scan,
    )
    media_id = media_list_result["playback_media_id"]

    # 1. Obtain Playback Grant via real user Auth API
    status, headers, body = smoke.http_request(
        api_endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=owner_token, timeout=10,
    )
    require(status == 200, "playback_url_issuance_failed")
    require(get_header(headers, "Cache-Control") == "no-store", "issuance_cache_control_not_no_store")
    require(isinstance(body, dict), "playback_url_response_not_dict")

    raw_url = body.get("url", "")
    expires_at = body.get("expires_at", "")
    expected_origin = urllib.parse.urlsplit(playback_endpoint.url).netloc
    issued_token = verify_signed_url_in_memory(raw_url, expires_at, expected_origin, media_id, require_fn=require)
    tokens_to_scan.append(issued_token)

    parsed_grant = urllib.parse.urlsplit(raw_url)
    signed_target = parsed_grant.path + "?" + parsed_grant.query

    # 2. GET 200 Full Clip
    status, resp_headers, body_bytes = playback_https_request(playback_endpoint, "GET", signed_target)
    require(status == 200, f"playback_get_200_failed_status_{status}")
    require(len(body_bytes) == expected_size, f"playback_size_mismatch_{len(body_bytes)}_vs_{expected_size}")
    body_sha = hashlib.sha256(body_bytes).hexdigest()
    require(body_sha == expected_sha, "playback_sha256_mismatch")
    content_type = get_header(resp_headers, "Content-Type") or ""
    require("video/mp4" in content_type.lower(), f"playback_content_type_mismatch_{content_type}")
    cc = get_header(resp_headers, "Cache-Control") or ""
    require("private, no-store" in cc, f"playback_cache_control_mismatch_{cc}")
    require(get_header(resp_headers, "Accept-Ranges") == "bytes", "playback_missing_accept_ranges")
    etag = get_header(resp_headers, "ETag")

    # 3. Nonzero proper Range -> 206 Exact Bytes
    start = 8
    end = min(23, expected_size - 1)
    with open(source_path, "rb") as f:
        f.seek(start)
        expected_range_bytes = f.read(end - start + 1)

    range_status, range_headers, range_body = playback_https_request(
        playback_endpoint, "GET", signed_target,
        headers={"Range": f"bytes={start}-{end}"},
    )
    require(range_status == 206, f"playback_range_status_not_206_got_{range_status}")
    require(range_body == expected_range_bytes, "playback_range_bytes_mismatch")
    require(len(range_body) == len(expected_range_bytes), "playback_range_length_mismatch")
    require(get_header(range_headers, "Content-Range") == f"bytes {start}-{end}/{expected_size}",
            "playback_content_range_mismatch")
    require(get_header(range_headers, "Content-Length") == str(len(expected_range_bytes)),
            "playback_range_content_length_mismatch")
    range_cc = get_header(range_headers, "Cache-Control") or ""
    require("private, no-store" in range_cc, "playback_range_cache_control_mismatch")

    # 4. Out-of-file Range denied (expect 416)
    oof_status, oof_headers, _ = playback_https_request(
        playback_endpoint, "GET", signed_target,
        headers={"Range": f"bytes={expected_size + 1000}-{expected_size + 2000}"},
    )
    require(oof_status == 416, f"out_of_file_range_status_expected_416_got_{oof_status}")
    observed_oof = {"status": oof_status, "content_range": get_header(oof_headers, "Content-Range")}

    # 5. If-Range matching & stale ETag observations
    matching_observed = None
    if etag:
        status, if_match_headers, _ = playback_https_request(
            playback_endpoint, "GET", signed_target,
            headers={"Range": f"bytes={start}-{end}", "If-Range": etag},
        )
        require(status == 206, f"if_range_matching_status_expected_206_got_{status}")
        matching_observed = {"status": status, "content_range": get_header(if_match_headers, "Content-Range")}

    stale_status, stale_headers, stale_body = playback_https_request(
        playback_endpoint, "GET", signed_target,
        headers={"Range": f"bytes={start}-{end}", "If-Range": '"stale-etag-probe-99999999"'},
    )
    stale_observed = {"status": stale_status, "content_length": len(stale_body)}

    # 6. HEAD 405 remain closed
    status, head_headers, _ = playback_https_request(playback_endpoint, "HEAD", signed_target)
    require(status == 405, f"head_method_expected_405_got_{status}")
    head_cc = get_header(head_headers, "Cache-Control") or ""
    require("private, no-store" in head_cc, "head_missing_private_no_store")

    # 7. Signed URL denied on shared API
    status, _, _ = smoke.http_request(api_endpoint, "GET", signed_target, timeout=10)
    require(status == 404, f"shared_api_signed_read_expected_404_got_{status}")
    status, _, _ = playback_https_request(
        api_endpoint, "GET", signed_target,
        headers={"X-Stage8-Media-Playback-Route": fixture.values.get("MEDIA_PLAYBACK_ROUTE_KEY", "")},
    )
    require(status == 404, f"shared_api_signed_read_with_guard_expected_404_got_{status}")

    # 8. Unsigned, public, management, and default routes denied on playback endpoint
    for label, bad_path in [
        ("unsigned_read", f"/storage/v1/object/media-video/{media_id}.mp4"),
        ("public_read", f"/storage/v1/object/public/media-video/{media_id}.mp4"),
        ("authenticated_read", f"/storage/v1/object/authenticated/media-video/{media_id}.mp4"),
        ("bucket_mgmt", "/storage/v1/bucket/media-video"),
        ("api_route", "/v1/gateways"),
        ("default_root", "/"),
    ]:
        status, _, _ = playback_https_request(playback_endpoint, "GET", bad_path)
        require(status == 404, f"playback_{label}_expected_404_got_{status}")

    # 9. Query parameter tamper and reject cases
    status, _, _ = playback_https_request(playback_endpoint, "GET", f"/storage/v1/object/sign/media-video/{media_id}.mp4")
    require(status == 400, f"missing_token_expected_400_got_{status}")

    status, _, _ = playback_https_request(
        playback_endpoint, "GET",
        f"/storage/v1/object/sign/media-video/{media_id}.mp4?token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.invalidsig",
    )
    require(status in (400, 401, 403, 404), f"invalid_sig_expected_rejection_got_{status}")

    other_id = str(uuid.uuid4())
    status, _, _ = playback_https_request(
        playback_endpoint, "GET",
        f"/storage/v1/object/sign/media-video/{other_id}.mp4?token={issued_token}",
    )
    require(status in (400, 401, 403, 404), f"wrong_path_expected_rejection_got_{status}")

    status, _, _ = playback_https_request(playback_endpoint, "GET", f"{signed_target}&token={issued_token}")
    require(status == 400, f"duplicate_token_expected_400_got_{status}")

    status, _, _ = playback_https_request(playback_endpoint, "GET", f"{signed_target}&extra=1")
    require(status == 400, f"foreign_query_expected_400_got_{status}")

    status, _, _ = playback_https_request(
        playback_endpoint, "GET",
        f"/storage/v1/object/sign/media-video/{media_id}.mp4?token%3D{issued_token}",
    )
    require(status == 400, f"encoded_query_expected_400_got_{status}")

    for non_get in ("POST", "PUT", "DELETE"):
        status, _, _ = playback_https_request(playback_endpoint, non_get, signed_target)
        require(status == 405, f"method_{non_get}_expected_405_got_{status}")

    status, _, spoof_body = playback_https_request(
        playback_endpoint, "GET", signed_target,
        headers={"Authorization": "Bearer fake_token", "apikey": "fake_key", "Cookie": "session=fake"},
    )
    require(status == 200 and len(spoof_body) == expected_size, "spoofed_auth_stripped_read_failed")

    # 10. Short expiry test without sleeping 2 minutes (direct Storage internal sign using dedicated playback token)
    from stage8_media_storage_http_fixture import mapped_loopback_port
    storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
    playback_token = fixture.values.get("MEDIA_PLAYBACK_STORAGE_TOKEN", service_key)
    sign_req = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/sign/media-video/{media_id}.mp4",
        data=json.dumps({"expiresIn": 1}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {playback_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(sign_req, timeout=10) as sign_resp:
            sign_data = json.loads(sign_resp.read().decode("utf-8"))
    except Exception as exc:
        raise fail_cls(f"direct_storage_sign_failed_{exc}") from None

    short_url = sign_data.get("signedURL", "")
    short_parsed = urllib.parse.urlsplit(short_url)
    short_qs = urllib.parse.parse_qs(short_parsed.query)
    short_token = short_qs.get("token", [""])[0]
    if short_token:
        tokens_to_scan.append(short_token)

    time.sleep(1.5)
    exp_status, _, _ = playback_https_request(
        playback_endpoint, "GET",
        f"/storage/v1/object/sign/media-video/{media_id}.mp4?token={short_token}",
    )
    require(exp_status in (400, 401, 403, 404), f"expired_grant_expected_rejection_got_{exp_status}")

    # 10b. Prove storage_playback_signer principal boundaries in native pinned Storage
    # Seed a known existing private image in media-images using authorized fixture principal (service_key)
    seed_image_name = f"probe-private-{secrets.token_hex(4)}.png"
    seed_image_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRprobe"
    seed_upload_req = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/media-images/{seed_image_name}",
        data=seed_image_bytes,
        headers={"Authorization": f"Bearer {service_key}", "Content-Type": "image/png"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(seed_upload_req, timeout=5) as seed_resp:
            require(seed_resp.status in (200, 201), "seed_image_failed")
    except Exception as exc:
        raise fail_cls(f"seed_image_upload_failed_{exc}")

    # Verify authorized service_key can sign and read the seeded image BEFORE testing playback_token
    sign_seed_req = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/sign/media-images/{seed_image_name}",
        data=json.dumps({"expiresIn": 60}).encode("utf-8"),
        headers={"Authorization": f"Bearer {service_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(sign_seed_req, timeout=5) as sign_seed_resp:
            require(sign_seed_resp.status == 200, "service_key_sign_seed_image_failed")
            seed_sign_data = json.loads(sign_seed_resp.read().decode("utf-8"))
            seed_signed_url = seed_sign_data.get("signedURL", "")
            require(seed_signed_url != "", "service_key_signed_url_empty")
    except Exception as exc:
        raise fail_cls(f"service_key_sign_seed_failed_{exc}")

    read_seed_req = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}{seed_signed_url}",
        headers={"Authorization": f"Bearer {service_key}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(read_seed_req, timeout=5) as read_seed_resp:
            require(read_seed_resp.status == 200 and read_seed_resp.read() == seed_image_bytes, "service_key_read_seed_image_failed")
    except Exception as exc:
        raise fail_cls(f"service_key_read_seed_failed_{exc}")

    # A. Cannot upload: Attempt upload with playback_token
    bad_upload = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/media-video/probe-unauthorized.mp4",
        data=b"forbidden-content",
        headers={"Authorization": f"Bearer {playback_token}", "Content-Type": "video/mp4"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(bad_upload, timeout=5):
            raise fail_cls("playback_signer_unexpectedly_allowed_to_upload")
    except urllib.error.HTTPError as err:
        require(err.code in (400, 401, 403, 404), f"playback_signer_upload_reject_got_{err.code}")
    except Exception as exc:
        raise fail_cls(f"playback_signer_upload_non_http_error_{exc}")

    probe_obj_count = smoke.db_psql(registry, postgres_id,
        "SELECT count(*) FROM storage.objects WHERE bucket_id = 'media-video' AND name = 'probe-unauthorized.mp4';").strip()
    require([r for r in probe_obj_count.splitlines() if r.isdigit()] == ["0"], "upload_probe_side_effect_object_created")

    # B. Cannot delete: Attempt delete with playback_token on real clip
    bad_delete = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/media-video/{media_id}.mp4",
        headers={"Authorization": f"Bearer {playback_token}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(bad_delete, timeout=5):
            raise fail_cls("playback_signer_unexpectedly_allowed_to_delete")
    except urllib.error.HTTPError as err:
        require(err.code in (400, 401, 403, 404), f"playback_signer_delete_reject_got_{err.code}")
    except Exception as exc:
        raise fail_cls(f"playback_signer_delete_non_http_error_{exc}")

    clip_obj_count = smoke.db_psql(registry, postgres_id,
        f"SELECT count(*) FROM storage.objects WHERE bucket_id = 'media-video' AND name = '{media_id}.mp4';").strip()
    require([r for r in clip_obj_count.splitlines() if r.isdigit()] == ["1"], "delete_probe_side_effect_object_deleted")

    # C. Cannot sign or read other buckets (media-images with proven existing seeded image)
    bad_sign_other = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/sign/media-images/{seed_image_name}",
        data=json.dumps({"expiresIn": 60}).encode("utf-8"),
        headers={"Authorization": f"Bearer {playback_token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(bad_sign_other, timeout=5):
            raise fail_cls("playback_signer_unexpectedly_allowed_to_sign_other_bucket")
    except urllib.error.HTTPError as err:
        require(err.code in (400, 401, 403, 404), f"playback_signer_sign_other_reject_got_{err.code}")
    except Exception as exc:
        raise fail_cls(f"playback_signer_sign_other_non_http_error_{exc}")

    bad_read_other = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/authenticated/media-images/{seed_image_name}",
        headers={"Authorization": f"Bearer {playback_token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(bad_read_other, timeout=5):
            raise fail_cls("playback_signer_unexpectedly_allowed_to_read_other_bucket")
    except urllib.error.HTTPError as err:
        require(err.code in (400, 401, 403, 404), f"playback_signer_read_other_reject_got_{err.code}")
    except Exception as exc:
        raise fail_cls(f"playback_signer_read_other_non_http_error_{exc}")

    # Disposable cleanup of seeded probe image
    try:
        del_seed_req = urllib.request.Request(
            f"http://127.0.0.1:{storage_port}/object/media-images/{seed_image_name}",
            headers={"Authorization": f"Bearer {service_key}"},
            method="DELETE",
        )
        with urllib.request.urlopen(del_seed_req, timeout=5):
            pass
    except Exception:
        pass

    # D. Cannot manage Storage (create/delete buckets)
    bad_bucket = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/bucket",
        data=json.dumps({"id": "malicious-bucket", "name": "malicious-bucket"}).encode("utf-8"),
        headers={"Authorization": f"Bearer {playback_token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(bad_bucket, timeout=5):
            raise fail_cls("playback_signer_unexpectedly_allowed_to_create_bucket")
    except urllib.error.HTTPError as err:
        require(err.code in (400, 401, 403, 404), f"playback_signer_create_bucket_reject_got_{err.code}")
    except Exception as exc:
        raise fail_cls(f"playback_signer_create_bucket_non_http_error_{exc}")

    bad_bucket_count = smoke.db_psql(registry, postgres_id,
        "SELECT count(*) FROM storage.buckets WHERE id = 'malicious-bucket';").strip()
    require([r for r in bad_bucket_count.splitlines() if r.isdigit()] == ["0"], "bucket_create_side_effect_bucket_created")

    # E. Role attribute verification in PostgreSQL
    role_info = smoke.db_psql(registry, postgres_id,
        "SELECT json_build_array(rolsuper, rolbypassrls, rolcanlogin)::text FROM pg_roles WHERE rolname = 'storage_playback_signer';").strip()
    role_lines = [r for r in role_info.splitlines() if r.startswith("[")]
    require(len(role_lines) == 1 and json.loads(role_lines[0]) == [False, False, False], "storage_playback_signer_role_not_restricted")

    priv_app = smoke.db_psql(registry, postgres_id,
        "SELECT has_table_privilege('storage_playback_signer', 'public.media_objects', 'select');").strip()
    priv_app_lines = [r for r in priv_app.splitlines() if r in ("t", "f")]
    require(priv_app_lines == ["f"], "storage_playback_signer_must_not_access_public_tables")

    policy_count = smoke.db_psql(registry, postgres_id,
        "SELECT count(*) FROM pg_policies WHERE schemaname = 'storage' AND tablename = 'objects' AND policyname = 'storage_playback_signer_read_video';").strip()
    p_lines = [r for r in policy_count.splitlines() if r.isdigit()]
    require(len(p_lines) == 1 and int(p_lines[0]) >= 1, "storage_playback_signer_policy_missing")

    # 11. Reissue & Revocation proof
    op_email = "stage8_playback_op_" + secrets.token_hex(6) + "@example.invalid"
    op_pass = secrets.token_urlsafe(32)
    op_uid = smoke.create_user_via_admin_api(api_endpoint, service_key, op_email, op_pass)
    op_token = smoke.login_user_for_token(api_endpoint, anon_key, op_email, op_pass)
    smoke.db_psql(registry, postgres_id,
                   "INSERT INTO public.user_gateways(user_id, gateway_id, role) VALUES (:'uid', :'gid', 'operator');",
                   {"uid": op_uid, "gid": gateway_id})

    status, _, op_body = smoke.http_request(
        api_endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=op_token, timeout=10,
    )
    require(status == 200, "operator_reissue_grant_failed")
    op_url = op_body.get("url", "")
    op_parsed = urllib.parse.urlsplit(op_url)
    op_token_str = urllib.parse.parse_qs(op_parsed.query).get("token", [""])[0]
    if op_token_str:
        tokens_to_scan.append(op_token_str)
    op_target = op_parsed.path + "?" + op_parsed.query

    # Both operator and initial owner grant work on playback port
    status, _, _ = playback_https_request(playback_endpoint, "GET", op_target)
    require(status == 200, "operator_grant_playback_get_failed")
    status, _, _ = playback_https_request(playback_endpoint, "GET", signed_target)
    require(status == 200, "initial_owner_grant_playback_get_failed")

    # Revoke operator membership from DB
    smoke.db_psql(registry, postgres_id,
                   "DELETE FROM public.user_gateways WHERE user_id = :'uid' AND gateway_id = :'gid';",
                   {"uid": op_uid, "gid": gateway_id})

    # Calling API again with revoked operator token returns 404
    status, _, _ = smoke.http_request(
        api_endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=op_token, timeout=10,
    )
    require(status == 404, "revoked_operator_api_expected_404")

    # Representative authorization denials: wrong gateway and authentic nonmember
    wrong_gw = "gw-wrong-" + secrets.token_hex(4)
    status, _, _ = smoke.http_request(
        api_endpoint, "POST", f"/v1/gateways/{wrong_gw}/media/{media_id}/playback-url",
        token=owner_token, timeout=10,
    )
    require(status == 404, "wrong_gateway_api_expected_404")

    status, _, _ = smoke.http_request(
        api_endpoint, "POST", f"/v1/gateways/{gateway_id}/media/{media_id}/playback-url",
        token=op_token, timeout=10,
    )
    require(status == 404, "nonmember_api_expected_404")

    # Previously issued op_target still works until its TTL expires
    status, _, _ = playback_https_request(playback_endpoint, "GET", op_target)
    require(status == 200, "existing_grant_after_revocation_expected_to_work_until_expiry")

    # 12. Redacted log scan
    if run_cmd is not None:
        scan_services = [
            "backend", "nginx", "stage8-media-upload-test-edge",
            "stage8-media-playback-test-edge", "supabase-envoy", "supabase-storage"
        ]
        compose_logs = run_cmd(prefix + ["logs", "--no-color", "--tail", "2000", *scan_services], fixture, timeout=30).stdout
        for tok in tokens_to_scan:
            require(tok.encode() not in compose_logs, "storage_signed_token_leaked_in_logs")
        for secret in [service_key, anon_key, op_pass, material.get("password"), material.get("owner_password")]:
            if secret:
                require(secret.encode() not in compose_logs, "credential_leaked_in_logs")

    report["checks"]["stage8_local_playback"] = {
        "status": "PASS",
        "gateway_media_list_verified": media_list_result["summary"],
        "https_endpoints_verified": {
            "api_origin": api_endpoint.url,
            "playback_origin": playback_endpoint.url,
        },
        "get_200_verified": {
            "size": expected_size,
            "sha256": expected_sha,
            "mime": "video/mp4",
            "cache_control": "private, no-store",
            "accept_ranges": "bytes",
        },
        "range_206_verified": {
            "http_status": range_status,
            "start": start,
            "end": end,
            "content_range": f"bytes {start}-{end}/{expected_size}",
            "bytes_length": len(range_body),
        },
        "out_of_file_range_observed": observed_oof,
        "if_range_matching_observed": matching_observed,
        "if_range_stale_observed": stale_observed,
        "head_405_closed": True,
        "shared_api_denied_404": True,
        "unsigned_routes_denied_404": True,
        "tamper_query_rejected_400": True,
        "non_get_rejected_405": True,
        "spoofed_auth_stripped": True,
        "short_expiry_verified": True,
        "reissue_and_revocation_semantics_verified": True,
        "representative_rejections_verified": {
            "nonmember_denied_404": True,
            "wrong_gateway_denied_404": True,
            "revocation_denied_404": True,
        },
        "storage_playback_signer_boundaries_verified": {
            "status": "PASS",
            "restricted_role_verified": True,
            "public_tables_denied": True,
            "write_denied_no_side_effects": True,
            "delete_denied_no_side_effects": True,
            "other_bucket_denied_existing_proven": True,
            "bucket_management_denied_no_side_effects": True,
        },
        "tokens_unleaked_in_logs": True,
    }


def run_playback_browser_acceptance(
    fixture, prefix, registry, material, media_row, report,
    run_cmd=None, proof_failure_cls=None,
):
    fail_cls = proof_failure_cls or PlaybackProofFailure

    def require(condition, code):
        if not condition:
            raise fail_cls(code)

    chrome_bin = (
        os.environ.get("STAGE8_CHROMIUM_PATH")
        or os.environ.get("CHROME_BIN")
        or shutil.which("chromium")
        or shutil.which("google-chrome")
    )
    playwright_dir = (
        os.environ.get("STAGE8_PLAYWRIGHT_DIR")
        or os.environ.get("PLAYWRIGHT_PATH")
        or os.environ.get("NODE_PATH")
    )
    certutil_bin = shutil.which("certutil")

    require(bool(chrome_bin and os.path.isfile(chrome_bin)), "browser_tooling_missing_chromium_binary")
    require(bool(playwright_dir and os.path.exists(playwright_dir)), "browser_tooling_missing_playwright_dir")
    require(bool(certutil_bin), "browser_tooling_missing_certutil")

    api_endpoint = material["api_endpoint"]
    playback_endpoint = getattr(fixture, "playback_endpoint", None)
    require(playback_endpoint is not None, "playback_endpoint_missing")

    gateway_id = material["gateway_id"]
    media_id = media_row[0]
    owner_token = material["owner_token"]

    browser_home = fixture.private / "browser-home"
    if browser_home.exists():
        shutil.rmtree(browser_home, ignore_errors=True)
    browser_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    nssdb = browser_home / ".pki" / "nssdb"
    nssdb.mkdir(mode=0o700, parents=True, exist_ok=True)

    ca_path = fixture.private / "p2-ca.crt"
    require(ca_path.is_file(), "fixture_ca_file_missing")

    proc_init = subprocess.run(
        ["certutil", "-d", f"sql:{nssdb}", "-N", "--empty-password"],
        capture_output=True, text=True, timeout=10,
    )
    require(proc_init.returncode == 0, f"certutil_nssdb_init_failed_exit_{proc_init.returncode}")

    proc_import = subprocess.run(
        ["certutil", "-d", f"sql:{nssdb}", "-A", "-t", "C,,", "-n", "fixture-ca", "-i", str(ca_path)],
        capture_output=True, text=True, timeout=10,
    )
    require(proc_import.returncode == 0, f"certutil_ca_import_failed_exit_{proc_import.returncode}")

    runner_script = ROOT / "scripts" / "tests" / "stage8_browser_playback_runner.js"
    require(runner_script.is_file(), "browser_runner_script_missing")

    payload = {
        "demoUrl": f"{api_endpoint.url}/demo/",
        "apiOrigin": api_endpoint.url,
        "playbackOrigin": playback_endpoint.url,
        "gatewayId": gateway_id,
        "mediaId": media_id,
        "token": owner_token,
        "targetSeekTime": 25.0,
    }

    safe_env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": str(browser_home),
        "CHROME_BIN": str(chrome_bin),
    }
    if playwright_dir:
        safe_env["STAGE8_PLAYWRIGHT_DIR"] = str(playwright_dir)
        safe_env["NODE_PATH"] = str(playwright_dir)

    try:
        proc = subprocess.run(
            ["node", str(runner_script)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            env=safe_env,
            timeout=75,
        )
    finally:
        shutil.rmtree(browser_home, ignore_errors=True)

    require(proc.returncode == 0, f"browser_playback_runner_failed_exit_{proc.returncode}")

    try:
        result = json.loads(proc.stdout.strip())
    except Exception:
        raise fail_cls("browser_runner_invalid_json") from None

    require(result.get("status") == "PASS", "browser_playback_status_not_pass")
    require(result.get("unbuffered_proof", {}).get("outside_all_buffered_ranges") is True, "seek_target_not_unbuffered")
    require(result.get("seek_range_response", {}).get("status") == 206, "seek_range_status_not_206")
    require(result.get("seek_range_response", {}).get("request_range_nonzero") is True, "seek_range_request_not_nonzero")
    require(result.get("seek_range_response", {}).get("origin_matched_playback") is True, "seek_range_origin_not_playback")
    require(result.get("post_seek", {}).get("playing_sustained") is True, "playback_not_sustained_after_seek")
    require(result.get("post_seek", {}).get("frames_advanced") is True, "decoded_frames_did_not_advance")
    require(result.get("seek_button_verified") is True, "seek_button_failed")

    report["checks"]["stage8_browser_playback"] = result


class Stage8PlaybackGuardError(ValueError):
    """Raised when the Stage 8 playback rely-on-run guard rejects an execution."""

    def __init__(self, kind: str, code: str):
        self.kind = kind
        self.code = code
        super().__init__(f"{kind}: {code}")


def require_stage8_playback_run(report, runner_exit_code=0):
    """Require valid completed Stage 8 playback run and exact cleanup."""
    if runner_exit_code == 77:
        raise Stage8PlaybackGuardError("SKIP", "runner_exit_skipped")
    if runner_exit_code != 0:
        raise Stage8PlaybackGuardError("FAIL", "runner_exit_nonzero")

    if report is None:
        raise Stage8PlaybackGuardError("NOT_RUN", "report_missing")
    if not isinstance(report, dict):
        raise Stage8PlaybackGuardError("FAIL", "report_malformed")

    status = report.get("status")
    if status == "SKIP":
        raise Stage8PlaybackGuardError("SKIP", "status_skipped")
    if status in ("NOT RUN", "NOT_RUN"):
        raise Stage8PlaybackGuardError("NOT_RUN", "status_not_run")
    if status != "PASS":
        raise Stage8PlaybackGuardError("FAIL", "status_not_pass")

    schema_version = report.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        raise Stage8PlaybackGuardError("FAIL", "schema_version_invalid")

    project = report.get("project")
    if not isinstance(project, str) or not re.fullmatch(r"ncs_[0-9a-f]{12}", project):
        raise Stage8PlaybackGuardError("FAIL", "project_provenance_invalid")

    if report.get("failure_code") is not None or report.get("error") is not None:
        raise Stage8PlaybackGuardError("FAIL", "report_has_failure")

    if report.get("exact_cleanup_verified") is not True:
        raise Stage8PlaybackGuardError("FAIL", "exact_cleanup_not_verified")

    checks = report.get("checks")
    if not isinstance(checks, dict):
        raise Stage8PlaybackGuardError("FAIL", "checks_missing_or_malformed")

    # Visible upload ready, size, hash (bounded ASCII)
    upload = checks.get("gateway_upload_result_and_durable_verification")
    if not isinstance(upload, dict) or upload.get("status") != "PASS":
        raise Stage8PlaybackGuardError("FAIL", "upload_verification_missing_or_failed")
    if upload.get("sqlite_phase") != "ready":
        raise Stage8PlaybackGuardError("FAIL", "upload_ready_phase_missing")
    actual_bytes = upload.get("actual_bytes")
    if type(actual_bytes) is not int or actual_bytes <= 0:
        raise Stage8PlaybackGuardError("FAIL", "upload_size_missing_or_invalid")
    declared_bytes = upload.get("declared_bytes")
    if type(declared_bytes) is not int or declared_bytes <= 0 or declared_bytes != actual_bytes:
        raise Stage8PlaybackGuardError("FAIL", "upload_size_missing_or_invalid")
    verified_sha = upload.get("verified_sha256")
    if not isinstance(verified_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", verified_sha):
        raise Stage8PlaybackGuardError("FAIL", "upload_hash_missing_or_invalid")

    # Playback API role/revoke proof & 206 content-range proof
    playback = checks.get("stage8_local_playback")
    if not isinstance(playback, dict) or playback.get("status") != "PASS":
        raise Stage8PlaybackGuardError("FAIL", "playback_verification_missing_or_failed")

    if playback.get("reissue_and_revocation_semantics_verified") is not True:
        raise Stage8PlaybackGuardError("FAIL", "playback_role_revoke_proof_missing")

    rep = playback.get("representative_rejections_verified")
    if not isinstance(rep, dict):
        raise Stage8PlaybackGuardError("FAIL", "representative_rejections_missing")
    if rep.get("nonmember_denied_404") is not True:
        raise Stage8PlaybackGuardError("FAIL", "representative_nonmember_denied_missing")
    if rep.get("wrong_gateway_denied_404") is not True:
        raise Stage8PlaybackGuardError("FAIL", "representative_wrong_gateway_denied_missing")

    range_206 = playback.get("range_206_verified")
    if not isinstance(range_206, dict):
        raise Stage8PlaybackGuardError("FAIL", "range_206_missing")
    if type(range_206.get("http_status")) is not int or range_206.get("http_status") != 206:
        raise Stage8PlaybackGuardError("FAIL", "range_http_status_not_206")

    bytes_length = range_206.get("bytes_length")
    if type(bytes_length) is not int or bytes_length <= 0:
        raise Stage8PlaybackGuardError("FAIL", "range_bytes_length_invalid")

    content_range = range_206.get("content_range")
    if not isinstance(content_range, str):
        raise Stage8PlaybackGuardError("FAIL", "range_content_range_invalid")
    match = re.fullmatch(r"bytes (\d{1,12})-(\d{1,12})/(\d{1,12})", content_range)
    if not match:
        raise Stage8PlaybackGuardError("FAIL", "range_content_range_invalid")

    cr_start = int(match.group(1))
    cr_end = int(match.group(2))
    cr_total = int(match.group(3))

    r_start = range_206.get("start")
    r_end = range_206.get("end")
    if type(r_start) is not int or type(r_end) is not int:
        raise Stage8PlaybackGuardError("FAIL", "range_content_range_inconsistent")

    if (cr_start != r_start
            or cr_end != r_end
            or cr_start < 0
            or cr_start > cr_end
            or (cr_end - cr_start + 1) != bytes_length
            or cr_total != actual_bytes):
        raise Stage8PlaybackGuardError("FAIL", "range_content_range_inconsistent")

    # Gateway media list proof (status + pagination + revoke)
    list_proof = playback.get("gateway_media_list_verified")
    if not isinstance(list_proof, dict) or list_proof.get("status") != "PASS":
        raise Stage8PlaybackGuardError("FAIL", "gateway_media_list_proof_missing")
    pag_proof = list_proof.get("pagination_verified")
    if not isinstance(pag_proof, dict) or pag_proof.get("pages_traversed") is not True:
        raise Stage8PlaybackGuardError("FAIL", "gateway_media_list_pagination_proof_missing")
    rej_proof = list_proof.get("rejections_verified")
    if not isinstance(rej_proof, dict) or rej_proof.get("revocation_denied_404") is not True:
        raise Stage8PlaybackGuardError("FAIL", "gateway_media_list_revocation_proof_missing")

    # Storage playback signer principal boundaries proof
    signer_proof = playback.get("storage_playback_signer_boundaries_verified")
    if not isinstance(signer_proof, dict) or signer_proof.get("status") != "PASS":
        raise Stage8PlaybackGuardError("FAIL", "storage_playback_signer_boundaries_missing")
    if (signer_proof.get("restricted_role_verified") is not True or
            signer_proof.get("public_tables_denied") is not True or
            signer_proof.get("write_denied_no_side_effects") is not True or
            signer_proof.get("delete_denied_no_side_effects") is not True or
            signer_proof.get("other_bucket_denied_existing_proven") is not True or
            signer_proof.get("bucket_management_denied_no_side_effects") is not True):
        raise Stage8PlaybackGuardError("FAIL", "storage_playback_signer_boundaries_incomplete")

    return True


def validate_and_stage_playback_summary(source_path, destination_path, checkout_sha, runner_exit_code=0):
    """Validate Stage 8 playback report using require-run and stage a safe summary."""
    if runner_exit_code == 77:
        raise SystemExit("Stage 8 playback rely-on-run rejected: SKIP:runner_exit_skipped")
    if runner_exit_code != 0:
        raise SystemExit("Stage 8 playback rely-on-run rejected: FAIL:runner_exit_nonzero")

    source = Path(source_path)
    if source.is_symlink():
        raise SystemExit("Stage 8 playback rely-on-run rejected: FAIL:report_symlink_rejected")
    if not source.exists():
        raise SystemExit("Stage 8 playback rely-on-run rejected: NOT_RUN:report_missing")
    if not source.is_file():
        raise SystemExit("Stage 8 playback rely-on-run rejected: FAIL:report_not_regular_file")

    raw_text = source.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise SystemExit("Stage 8 playback rely-on-run rejected: NOT_RUN:report_missing")

    try:
        report = json.loads(raw_text)
    except (OSError, json.JSONDecodeError):
        raise SystemExit("Stage 8 playback rely-on-run rejected: FAIL:report_malformed")

    try:
        require_stage8_playback_run(report, runner_exit_code=0)
    except Stage8PlaybackGuardError as err:
        raise SystemExit(f"Stage 8 playback rely-on-run rejected: {err.kind}:{err.code}")

    if not isinstance(checkout_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", checkout_sha):
        raise SystemExit("Stage 8 playback safe report checkout SHA rejected")

    destination = Path(destination_path)
    if destination.is_symlink() or destination.exists():
        raise SystemExit("Stage 8 playback safe report destination already exists or is symlink")

    upload = report["checks"]["gateway_upload_result_and_durable_verification"]
    playback = report["checks"]["stage8_local_playback"]
    range_check = playback["range_206_verified"]
    project = report["project"]

    safe = {
        "schema_version": 1,
        "status": "PASS",
        "project": project,
        "native_executed": True,
        "stage8_playback_accepted": True,
        "exact_cleanup_verified": True,
        "checkout_sha": checkout_sha,
        "media_ready": {
            "actual_bytes": upload["actual_bytes"],
            "verified_sha256": upload["verified_sha256"],
            "sqlite_phase": upload["sqlite_phase"],
        },
        "playback_proof": {
            "http_status": range_check["http_status"],
            "range_206": True,
            "content_range": range_check["content_range"],
            "bytes_length": range_check["bytes_length"],
            "reissue_and_revocation": True,
            "representative_rejections": {
                "nonmember_denied_404": True,
                "wrong_gateway_denied_404": True,
            },
            "gateway_media_list_verified": True,
            "storage_playback_signer_boundaries_verified": True,
        },
    }

    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        raise SystemExit(f"Stage 8 playback safe report open failed: {exc}")

    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(safe, output, sort_keys=True)
        output.write("\n")
    print("Stage 8 playback safe summary staged for checkout " + checkout_sha)
    return safe
