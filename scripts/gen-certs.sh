#!/usr/bin/env bash
set -euo pipefail

# Directories
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOSQUITTO_CERTS_DIR="${PROJECT_ROOT}/config/mosquitto/certs"
SECURE_CA_DIR="${PROJECT_ROOT}/certs"

mkdir -p "${MOSQUITTO_CERTS_DIR}" "${SECURE_CA_DIR}"

# 1. Load environment variables from .env if present
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
fi

# Configurable domains from environment (default to localhost if not specified)
MQTT_BROKER_DOMAIN="${MQTT_BROKER_DOMAIN:-localhost}"
MQTT_ADDITIONAL_SAN="${MQTT_ADDITIONAL_SAN:-}"

FORCE_NEW_CA=false
CLI_EXTRA_SANS=()

# Parse optional CLI flags and extra domains
for arg in "$@"; do
    case "$arg" in
        --new-ca|--force-ca)
            FORCE_NEW_CA=true
            ;;
        *)
            CLI_EXTRA_SANS+=("$arg")
            ;;
    esac
done

echo "=== TLS Certificate Generator for Mosquitto Broker ==="
echo "Primary Broker Domain : ${MQTT_BROKER_DOMAIN}"

# 2. Generate or Reuse Root CA
CA_KEY="${SECURE_CA_DIR}/ca.key"
CA_CRT="${MOSQUITTO_CERTS_DIR}/ca.crt"

if [ -f "${CA_KEY}" ] && [ -f "${CA_CRT}" ] && [ "${FORCE_NEW_CA}" = false ]; then
    echo "=== Reusing existing Root CA (${CA_CRT}) ==="
else
    echo "=== 1. Generating new Root CA (RSA 4096-bit) ==="
    openssl req -new -x509 -days 3650 -extensions v3_ca \
        -newkey rsa:4096 -nodes \
        -keyout "${CA_KEY}" \
        -out "${CA_CRT}" \
        -subj "/C=VN/ST=Hanoi/L=Hanoi/O=IoT-Gateway-Platform/OU=Security/CN=IoT-Platform-Root-CA"
    
    # Mirror copy in certs directory
    cp "${CA_CRT}" "${SECURE_CA_DIR}/ca.crt"
fi

# 3. Generate Server Key & CSR (RSA 2048-bit)
echo "=== 2. Generating Server Key & CSR (RSA 2048-bit) ==="
openssl req -new -newkey rsa:2048 -nodes \
    -keyout "${MOSQUITTO_CERTS_DIR}/server.key" \
    -out "${MOSQUITTO_CERTS_DIR}/server.csr" \
    -subj "/C=VN/ST=Hanoi/L=Hanoi/O=IoT-Gateway-Platform/OU=Broker/CN=${MQTT_BROKER_DOMAIN}"

# 4. Build Subject Alternative Names (SAN) dynamically
DNS_ENTRIES=("localhost" "mosquitto")
IP_ENTRIES=("127.0.0.1")

add_san_entry() {
    local entry="$1"
    entry=$(echo "${entry}" | xargs) # trim whitespace
    [ -z "${entry}" ] && return

    if [[ "${entry}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        # Check if already exists in IP_ENTRIES
        for existing in "${IP_ENTRIES[@]}"; do
            [ "${existing}" = "${entry}" ] && return
        done
        IP_ENTRIES+=("${entry}")
    else
        # Check if already exists in DNS_ENTRIES
        for existing in "${DNS_ENTRIES[@]}"; do
            [ "${existing}" = "${entry}" ] && return
        done
        DNS_ENTRIES+=("${entry}")
    fi
}

# Add primary domain
add_san_entry "${MQTT_BROKER_DOMAIN}"

# Add comma-separated domains from MQTT_ADDITIONAL_SAN
if [ -n "${MQTT_ADDITIONAL_SAN}" ]; then
    IFS=',' read -ra EXTRA_LIST <<< "${MQTT_ADDITIONAL_SAN}"
    for item in "${EXTRA_LIST[@]}"; do
        add_san_entry "${item}"
    done
fi

# Add CLI arguments
for item in "${CLI_EXTRA_SANS[@]:-}"; do
    add_san_entry "${item}"
done

echo "=== 3. Creating SAN Configuration ==="
cat << 'EOF' > "${MOSQUITTO_CERTS_DIR}/server.ext"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
EOF

DNS_IDX=1
for dns in "${DNS_ENTRIES[@]}"; do
    echo "DNS.${DNS_IDX} = ${dns}" >> "${MOSQUITTO_CERTS_DIR}/server.ext"
    echo " -> Added DNS: ${dns}"
    DNS_IDX=$((DNS_IDX + 1))
done

IP_IDX=1
for ip in "${IP_ENTRIES[@]}"; do
    echo "IP.${IP_IDX} = ${ip}" >> "${MOSQUITTO_CERTS_DIR}/server.ext"
    echo " -> Added IP:  ${ip}"
    IP_IDX=$((IP_IDX + 1))
done

# 5. Sign Server Certificate with Root CA (Validity: 730 days)
echo "=== 4. Signing Server Certificate with Root CA (Validity: 730 days) ==="
openssl x509 -req -in "${MOSQUITTO_CERTS_DIR}/server.csr" \
    -CA "${CA_CRT}" \
    -CAkey "${CA_KEY}" \
    -CAcreateserial \
    -out "${MOSQUITTO_CERTS_DIR}/server.crt" \
    -days 730 \
    -extfile "${MOSQUITTO_CERTS_DIR}/server.ext"

# 6. Set Secure Permissions
echo "=== 5. Setting Secure Permissions ==="
chmod 600 "${CA_KEY}"
chmod 600 "${MOSQUITTO_CERTS_DIR}/server.key"
chmod 644 "${CA_CRT}"
chmod 644 "${MOSQUITTO_CERTS_DIR}/server.crt"

# Clean temporary files
rm -f "${MOSQUITTO_CERTS_DIR}/server.csr" "${MOSQUITTO_CERTS_DIR}/server.ext"

echo "=== TLS Certificate Generation Completed Successfully! ==="
echo "Files created/updated:"
echo " - CA Certificate (for Gateways):      ${MOSQUITTO_CERTS_DIR}/ca.crt"
echo " - Server Certificate:                 ${MOSQUITTO_CERTS_DIR}/server.crt"
echo " - Server Private Key:                 ${MOSQUITTO_CERTS_DIR}/server.key"
echo " - Root CA Private Key (KEEP SECRET):  ${CA_KEY}"
