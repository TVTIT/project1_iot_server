#!/usr/bin/env python3
"""Opt-in bootstrap for the private media buckets through Storage's internal API."""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


VIDEO_BUCKET_ID = "media-video"
IMAGE_BUCKET_ID = "media-images"
VIDEO_LIMIT = 80_000_000
IMAGE_LIMIT = 52_428_800
EXPECTED_BUCKET = {
    "id": VIDEO_BUCKET_ID,
    "name": VIDEO_BUCKET_ID,
    "public": False,
    "file_size_limit": VIDEO_LIMIT,
    "allowed_mime_types": ["video/mp4"],
}


class BootstrapError(Exception):
    pass


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


class StorageAPI:
    def __init__(self, base_url, service_key):
        self.base_url = base_url.rstrip("/")
        self.service_key = service_key
        parsed = urllib.parse.urlsplit(self.base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise BootstrapError("Storage internal URL must be an HTTP(S) endpoint")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise BootstrapError("Storage internal URL must not contain credentials or query data")
        # Never inherit proxy settings from the environment for a request carrying
        # the service bearer, and never forward that bearer across a redirect.
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), NoRedirectHandler()
        )

    def request(self, method, path, payload=None):
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            method=method,
            headers={
                "Authorization": "Bearer " + self.service_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with self.opener.open(request, timeout=10) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise BootstrapError("Storage API response exceeded size limit")
                return response.status, json.loads(raw or b"{}")
        except urllib.error.HTTPError as error:
            # Never include the response body or request URL: either may contain secrets.
            raw = error.read(65537)
            if len(raw) <= 65536:
                try:
                    body = json.loads(raw or b"{}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    body = {}
                if isinstance(body, dict) and body.get("code") == "NoSuchBucket":
                    return error.code, {"code": "NoSuchBucket"}
            return error.code, {}
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
            raise BootstrapError("Storage API request failed") from None


def _matches(actual, expected):
    if not isinstance(actual, dict):
        return False
    for key in expected:
        if type(actual.get(key)) is not type(expected[key]) or actual.get(key) != expected[key]:
            return False
    return True


def ensure_bucket(api, expected, create_payload=None, update_payload=None):
    bucket_id = expected["id"]
    create_payload = expected if create_payload is None else create_payload
    update_payload = expected if update_payload is None else update_payload
    status, existing = api.request("GET", "/bucket/" + bucket_id)
    if status == 404 or (status == 400 and isinstance(existing, dict)
                         and existing.get("code") == "NoSuchBucket"):
        status, _ = api.request("POST", "/bucket", create_payload)
        if status not in (200, 201):
            raise BootstrapError("Storage bucket create failed (HTTP status {})".format(status))
    elif status == 200:
        status, _ = api.request("PUT", "/bucket/" + bucket_id, update_payload)
        if status not in (200, 201):
            raise BootstrapError("Storage bucket update failed (HTTP status {})".format(status))
    else:
        raise BootstrapError("Storage bucket lookup failed (HTTP status {})".format(status))

    status, actual = api.request("GET", "/bucket/" + bucket_id)
    if status != 200:
        raise BootstrapError("Storage bucket readback failed (HTTP status {})".format(status))
    if not _matches(actual, expected):
        raise BootstrapError("Storage bucket readback policy mismatch")


def ensure_image_bucket(api):
    status, existing = api.request("GET", "/bucket/" + IMAGE_BUCKET_ID)
    if status == 404 or (status == 400 and isinstance(existing, dict)
                         and existing.get("code") == "NoSuchBucket"):
        create_payload = {
            "id": IMAGE_BUCKET_ID,
            "name": IMAGE_BUCKET_ID,
            "public": False,
            "file_size_limit": IMAGE_LIMIT,
        }
        status, _ = api.request("POST", "/bucket", create_payload)
        if status not in (200, 201):
            raise BootstrapError("Image bucket create failed (HTTP status {})".format(status))
        expected = {
            "id": IMAGE_BUCKET_ID,
            "public": False,
            "file_size_limit": IMAGE_LIMIT,
        }
    elif status == 200:
        required = ("id", "name", "public", "file_size_limit", "allowed_mime_types")
        if not isinstance(existing, dict) or any(key not in existing for key in required):
            raise BootstrapError("Image bucket metadata is incomplete; refusing update")
        if (
            existing["id"] != IMAGE_BUCKET_ID
            or not isinstance(existing["name"], str)
            or type(existing["public"]) is not bool
            or (
                existing["file_size_limit"] is not None
                and (
                    type(existing["file_size_limit"]) is not int
                    or existing["file_size_limit"] < 0
                )
            )
            or (
                existing["allowed_mime_types"] is not None
                and (
                    not isinstance(existing["allowed_mime_types"], list)
                    or any(not isinstance(item, str) for item in existing["allowed_mime_types"])
                )
            )
        ):
            raise BootstrapError("Image bucket metadata is malformed; refusing update")

        old_limit = existing["file_size_limit"]
        effective_limit = IMAGE_LIMIT if old_limit is None else min(old_limit, IMAGE_LIMIT)
        expected = {
            "id": IMAGE_BUCKET_ID,
            "public": existing["public"],
            "file_size_limit": effective_limit,
            "allowed_mime_types": existing["allowed_mime_types"],
        }
        if old_limit is not None and old_limit <= IMAGE_LIMIT:
            if not _matches(existing, expected):
                raise BootstrapError("Image bucket metadata is inconsistent")
            return

        # Include visibility and MIME settings explicitly: Storage's update
        # defaults omitted properties, which could otherwise alter policy.
        update_payload = {
            "id": IMAGE_BUCKET_ID,
            "name": existing["name"],
            "public": existing["public"],
            "file_size_limit": effective_limit,
            "allowed_mime_types": existing["allowed_mime_types"],
        }
        status, _ = api.request("PUT", "/bucket/" + IMAGE_BUCKET_ID, update_payload)
        if status not in (200, 201):
            raise BootstrapError("Image bucket update failed (HTTP status {})".format(status))
    else:
        raise BootstrapError("Image bucket lookup failed (HTTP status {})".format(status))

    status, actual = api.request("GET", "/bucket/" + IMAGE_BUCKET_ID)
    if status != 200:
        raise BootstrapError("Image bucket readback failed (HTTP status {})".format(status))
    if not _matches(actual, expected):
        raise BootstrapError("Image bucket readback policy mismatch")


def ensure_media_bucket(api):
    ensure_bucket(api, EXPECTED_BUCKET)
    # The service-wide media cap is raised in this opt-in profile. Keep the existing
    # image bucket's stricter cap explicit rather than inheriting that raised limit.
    ensure_image_bucket(api)


def main():
    service_key = os.environ.get("SERVICE_ROLE_KEY", "")
    if not service_key:
        print("ERROR: SERVICE_ROLE_KEY is required for media bucket bootstrap.", file=sys.stderr)
        return 2
    base_url = os.environ.get("STORAGE_INTERNAL_URL", "http://supabase-storage:5000")
    try:
        ensure_media_bucket(StorageAPI(base_url, service_key))
    except BootstrapError as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("Storage media bucket policy verified (media-video and media-images).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
