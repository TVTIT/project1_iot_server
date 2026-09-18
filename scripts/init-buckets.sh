#!/usr/bin/env bash
set -euo pipefail

# Endpoint through Nginx or internal Envoy
STORAGE_URL="${STORAGE_URL:-http://localhost/storage/v1}"
SERVICE_ROLE_KEY="${SERVICE_ROLE_KEY:-}"

if [ -z "$SERVICE_ROLE_KEY" ]; then
    echo "ERROR: SERVICE_ROLE_KEY environment variable is required."
    exit 1
fi

create_private_bucket() {
    local bucket_id="$1"
    echo "Creating private bucket: ${bucket_id}..."
    
    # Try to create bucket
    local status_code
    status_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${STORAGE_URL}/bucket" \
        -H "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"${bucket_id}\", \"name\": \"${bucket_id}\", \"public\": false, \"file_size_limit\": 52428800}")

    if [ "$status_code" -eq 200 ] || [ "$status_code" -eq 201 ]; then
        echo " -> Bucket '${bucket_id}' created successfully (public: false)."
    elif [ "$status_code" -eq 409 ] || [ "$status_code" -eq 400 ]; then
        echo " -> Bucket '${bucket_id}' already exists or status ${status_code}."
    else
        echo " -> Warning: unexpected response code ${status_code} for bucket '${bucket_id}'"
    fi
}

echo "=== Initializing Supabase Storage Private Buckets ==="
create_private_bucket "media-images"

echo "=== Buckets initialization finished! ==="
