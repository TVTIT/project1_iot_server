#!/bin/sh
set -eu

IMAGE="eclipse-mosquitto:2.0.18"
RUN_ID="stage2-mqtt-spike-$$"
NETWORK="$RUN_ID"
BROKER="$RUN_ID-broker"
OLD_CLIENT="$RUN_ID-old-client"
ACTIVE_CLIENT="$RUN_ID-active-client"
WORK_DIR="$(mktemp -d)"
AUTH_DIR="$WORK_DIR/auth"
CONFIG_DIR="$WORK_DIR/config"
LOCK_FILE="$AUTH_DIR/.lock"

cleanup() {
    docker rm -f "$OLD_CLIENT" "$ACTIVE_CLIENT" "$BROKER" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    docker run --rm --user 0:0 --entrypoint /bin/sh \
        -v "$WORK_DIR:/work" "$IMAGE" -c 'rm -rf /work/auth /work/config' \
        >/dev/null 2>&1 || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

mkdir "$AUTH_DIR" "$CONFIG_DIR"
chmod 755 "$WORK_DIR" "$AUTH_DIR" "$CONFIG_DIR"
touch "$AUTH_DIR/passwd"
touch "$LOCK_FILE"
chmod 600 "$AUTH_DIR/passwd"

cat >"$CONFIG_DIR/mosquitto.conf" <<'EOF'
listener 1883 0.0.0.0
allow_anonymous false
password_file /mosquitto/auth/passwd
log_dest stdout
log_type error
log_type warning
log_type notice
EOF

add_or_update_user() {
    username="$1"
    password="$2"
    operation_id="$(openssl rand -hex 8)"
    (
        flock 9
        printf '%s\n%s\n' "$password" "$password" |
            docker run --rm -i --user 0:0 --entrypoint /bin/sh \
                -v "$AUTH_DIR:/mosquitto/auth" \
                "$IMAGE" -ec "cp /mosquitto/auth/passwd /mosquitto/auth/passwd.next.$operation_id
mosquitto_passwd -H sha512-pbkdf2 /mosquitto/auth/passwd.next.$operation_id '$username'
chown 1883:1883 /mosquitto/auth/passwd.next.$operation_id
chmod 600 /mosquitto/auth/passwd.next.$operation_id
mv /mosquitto/auth/passwd.next.$operation_id /mosquitto/auth/passwd" \
                >/dev/null
    ) 9>"$LOCK_FILE"
}

delete_user() {
    username="$1"
    operation_id="$(openssl rand -hex 8)"
    (
        flock 9
        docker run --rm --user 0:0 --entrypoint /bin/sh \
            -v "$AUTH_DIR:/mosquitto/auth" \
            "$IMAGE" -ec "cp /mosquitto/auth/passwd /mosquitto/auth/passwd.next.$operation_id
mosquitto_passwd -D /mosquitto/auth/passwd.next.$operation_id '$username'
chown 1883:1883 /mosquitto/auth/passwd.next.$operation_id
chmod 600 /mosquitto/auth/passwd.next.$operation_id
mv /mosquitto/auth/passwd.next.$operation_id /mosquitto/auth/passwd" \
            >/dev/null
    ) 9>"$LOCK_FILE"
}

reload_broker() {
    docker run --rm --pid="container:$BROKER" --network none \
        --entrypoint /bin/sh "$IMAGE" -c 'kill -HUP 1'
    sleep 1
}

can_publish() {
    username="$1"
    password="$2"
    docker run --rm --network "$NETWORK" "$IMAGE" \
        mosquitto_pub -h "$BROKER" -u "$username" -P "$password" \
        -t spike/check -m ok -q 1 >/dev/null 2>&1
}

backend_password="$(openssl rand -base64 24 | tr -d '\n')"
old_password="$(openssl rand -base64 24 | tr -d '\n')"
new_password="$(openssl rand -base64 24 | tr -d '\n')"
add_or_update_user backend_service "$backend_password"
add_or_update_user gateway_spike "$old_password"

docker network create "$NETWORK" >/dev/null
docker run -d --name "$BROKER" --network "$NETWORK" --user 1883:1883 \
    --entrypoint /usr/sbin/mosquitto \
    -v "$CONFIG_DIR/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
    -v "$AUTH_DIR:/mosquitto/auth:ro" \
    "$IMAGE" -c /mosquitto/config/mosquitto.conf >/dev/null

ready=false
for _ in $(seq 1 30); do
    if can_publish gateway_spike "$old_password"; then
        ready=true
        break
    fi
    sleep 1
done
[ "$ready" = true ] || { docker logs "$BROKER"; exit 1; }

docker run -d --name "$OLD_CLIENT" --network "$NETWORK" "$IMAGE" \
    mosquitto_sub -h "$BROKER" -u gateway_spike -P "$old_password" \
    -t spike/keepalive -d >/dev/null
sleep 1
if ! docker inspect -f '{{.State.Running}}' "$OLD_CLIENT" | grep -qx true; then
    echo "Old-credential subscriber did not remain connected before rotation." >&2
    docker logs "$OLD_CLIENT" >&2
    exit 1
fi

add_or_update_user gateway_spike "$new_password"
reload_broker

rotation_kept_existing_session=true
for _ in $(seq 1 10); do
    if docker logs "$OLD_CLIENT" 2>&1 | grep -q 'received CONNACK (5)'; then
        rotation_kept_existing_session=false
        break
    fi
    sleep 1
done
if can_publish gateway_spike "$old_password"; then
    echo "Old password unexpectedly authenticated after rotation." >&2
    exit 1
fi
can_publish gateway_spike "$new_password"
can_publish backend_service "$backend_password"

docker rm -f "$OLD_CLIENT" >/dev/null 2>&1 || true
docker run -d --name "$ACTIVE_CLIENT" --network "$NETWORK" "$IMAGE" \
    mosquitto_sub -h "$BROKER" -u gateway_spike -P "$new_password" \
    -t spike/keepalive -d >/dev/null
sleep 1
if ! docker inspect -f '{{.State.Running}}' "$ACTIVE_CLIENT" | grep -qx true; then
    echo "New-credential subscriber did not remain connected before revocation." >&2
    docker logs "$ACTIVE_CLIENT" >&2
    exit 1
fi

delete_user gateway_spike
reload_broker

revoke_kept_existing_session=true
for _ in $(seq 1 10); do
    if docker logs "$ACTIVE_CLIENT" 2>&1 | grep -q 'received CONNACK (5)'; then
        revoke_kept_existing_session=false
        break
    fi
    sleep 1
done
if can_publish gateway_spike "$new_password"; then
    echo "Revoked password unexpectedly authenticated a new connection." >&2
    exit 1
fi
can_publish backend_service "$backend_password"

printf '%s\n' \
    "Mosquitto password reload spike passed." \
    "rotation_kept_existing_session=$rotation_kept_existing_session" \
    "revoke_kept_existing_session=$revoke_kept_existing_session" \
    "old_credentials_rejected_for_new_connections=true" \
    "unrelated_backend_credential_remained_valid=true"
