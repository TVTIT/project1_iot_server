#!/bin/sh
set -e

# Generate SHA1 base64 hash for Envoy basic auth user list
PASSWORD_HASH=$(printf '%s' "${DASHBOARD_PASSWORD:-admin}" | openssl sha1 -binary | openssl base64)
DASHBOARD_BASIC_AUTH="${DASHBOARD_USERNAME:-admin}:{SHA}${PASSWORD_HASH}"

# Normalize MEDIA_PLAYBACK_ENABLED: only exact literal "true" enables playback.
case "${MEDIA_PLAYBACK_ENABLED:-}" in
    true)
        SAFE_MEDIA_PLAYBACK_ENABLED="true"
        ;;
    *)
        SAFE_MEDIA_PLAYBACK_ENABLED="false"
        ;;
esac

# Validate MEDIA_PLAYBACK_ROUTE_KEY before sed substitution to prevent corrupting Envoy config.
# If playback is disabled or noncanonical, the key remains empty so the route is inert.
if [ "$SAFE_MEDIA_PLAYBACK_ENABLED" = "true" ]; then
    case "${MEDIA_PLAYBACK_ROUTE_KEY:-}" in
        "")
            SAFE_MEDIA_PLAYBACK_ROUTE_KEY=""
            ;;
        *[!0-9a-f]*)
            SAFE_MEDIA_PLAYBACK_ROUTE_KEY=""
            ;;
        ????????????????????????????????????????????????????????????????)
            SAFE_MEDIA_PLAYBACK_ROUTE_KEY="${MEDIA_PLAYBACK_ROUTE_KEY}"
            ;;
        *)
            SAFE_MEDIA_PLAYBACK_ROUTE_KEY=""
            ;;
    esac
else
    SAFE_MEDIA_PLAYBACK_ROUTE_KEY=""
fi

echo "Generating Envoy configuration..."

sed -e "s|\${ANON_KEY}|${ANON_KEY}|g" \
    -e "s|\${ANON_KEY_ASYMMETRIC}|${ANON_KEY_ASYMMETRIC:-}|g" \
    -e "s|\${SERVICE_ROLE_KEY}|${SERVICE_ROLE_KEY}|g" \
    -e "s|\${SERVICE_ROLE_KEY_ASYMMETRIC}|${SERVICE_ROLE_KEY_ASYMMETRIC:-}|g" \
    -e "s|\${SUPABASE_PUBLISHABLE_KEY}|${SUPABASE_PUBLISHABLE_KEY:-}|g" \
    -e "s|\${SUPABASE_SECRET_KEY}|${SUPABASE_SECRET_KEY:-}|g" \
    -e "s|\${MEDIA_UPLOAD_ROUTE_KEY}|${MEDIA_UPLOAD_ROUTE_KEY:-}|g" \
    -e "s|\${MEDIA_PLAYBACK_ROUTE_KEY}|${SAFE_MEDIA_PLAYBACK_ROUTE_KEY}|g" \
    -e "s|\${SUPABASE_PUBLIC_URL}|${SUPABASE_PUBLIC_URL:-http://localhost}|g" \
    -e "s|\${DASHBOARD_BASIC_AUTH}|${DASHBOARD_BASIC_AUTH}|g" \
    /etc/envoy/lds.template.yaml > /etc/envoy/lds.yaml

echo "Envoy configuration generated successfully"
echo "Starting Envoy..."

exec envoy -c /etc/envoy/envoy.yaml "$@"
