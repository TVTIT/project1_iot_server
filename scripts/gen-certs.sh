#!/usr/bin/env bash
set -euo pipefail

# Directories
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOSQUITTO_CERTS_DIR="${PROJECT_ROOT}/config/mosquitto/certs"
SECURE_CA_DIR="${PROJECT_ROOT}/certs"

mkdir -p "${MOSQUITTO_CERTS_DIR}" "${SECURE_CA_DIR}"

echo "=== 1. Generating Root CA (RSA 4096-bit) ==="
openssl req -new -x509 -days 3650 -extensions v3_ca \
    -newkey rsa:4096 -nodes \
    -keyout "${SECURE_CA_DIR}/ca.key" \
    -out "${MOSQUITTO_CERTS_DIR}/ca.crt" \
    -subj "/C=VN/ST=Hanoi/L=Hanoi/O=IoT-Gateway-Platform/OU=Security/CN=IoT-Platform-Root-CA"

# Copy ca.crt to certs directory for safe keeping/distribution
cp "${MOSQUITTO_CERTS_DIR}/ca.crt" "${SECURE_CA_DIR}/ca.crt"

echo "=== 2. Generating Server Key & CSR (RSA 2048-bit) ==="
openssl req -new -newkey rsa:2048 -nodes \
    -keyout "${MOSQUITTO_CERTS_DIR}/server.key" \
    -out "${MOSQUITTO_CERTS_DIR}/server.csr" \
    -subj "/C=VN/ST=Hanoi/L=Hanoi/O=IoT-Gateway-Platform/OU=Broker/CN=tvtsupporter-40373.portmap.host"

echo "=== 3. Creating SAN Configuration ==="
cat << 'EOF' > "${MOSQUITTO_CERTS_DIR}/server.ext"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = tvtsupporter-40373.portmap.host
DNS.2 = localhost
DNS.3 = mosquitto
IP.1 = 127.0.0.1
EOF

echo "=== 4. Signing Server Certificate with Root CA (Validity: 730 days) ==="
openssl x509 -req -in "${MOSQUITTO_CERTS_DIR}/server.csr" \
    -CA "${MOSQUITTO_CERTS_DIR}/ca.crt" \
    -CAkey "${SECURE_CA_DIR}/ca.key" \
    -CAcreateserial \
    -out "${MOSQUITTO_CERTS_DIR}/server.crt" \
    -days 730 \
    -extfile "${MOSQUITTO_CERTS_DIR}/server.ext"

echo "=== 5. Setting Secure Permissions ==="
chmod 600 "${SECURE_CA_DIR}/ca.key"
chmod 600 "${MOSQUITTO_CERTS_DIR}/server.key"
chmod 644 "${MOSQUITTO_CERTS_DIR}/ca.crt"
chmod 644 "${MOSQUITTO_CERTS_DIR}/server.crt"

# Clean temporary csr and ext files
rm -f "${MOSQUITTO_CERTS_DIR}/server.csr" "${MOSQUITTO_CERTS_DIR}/server.ext"

echo "=== TLS Certificate Generation Completed! ==="
echo "Files created:"
echo " - CA Certificate (for Gateways): ${MOSQUITTO_CERTS_DIR}/ca.crt"
echo " - Server Certificate:            ${MOSQUITTO_CERTS_DIR}/server.crt"
echo " - Server Private Key:            ${MOSQUITTO_CERTS_DIR}/server.key"
echo " - Root CA Private Key (KEEP SECRET): ${SECURE_CA_DIR}/ca.key"
