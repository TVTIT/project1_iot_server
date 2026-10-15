#!/usr/bin/env bash
set -euo pipefail
umask 077

# Operator-only tool. Never invoke on deployment resources without approval.
# Keep the lexical invocation path: cd/pwd -P would hide an ancestor symlink.
SCRIPT_SOURCE="${BASH_SOURCE[0]}"
[[ "$SCRIPT_SOURCE" = /* ]] || SCRIPT_SOURCE="$PWD/$SCRIPT_SOURCE"
readonly PROJECT_ROOT="$(dirname -- "$(dirname -- "$SCRIPT_SOURCE")")"
readonly MOSQUITTO_CERTS_DIR="$PROJECT_ROOT/config/mosquitto/certs"
readonly SECURE_CA_DIR="$PROJECT_ROOT/certs"

# Supported threat model: root, the invoking operator (SUDO_UID for a rootful
# sudo launch), and the explicitly selected broker identity for its leaf cert
# directory are trusted/coordinated. Other principals must not have write access.
# Executables/PATH and this script must be trusted. Privileged concurrent rename,
# mount or same-identity file mutation is NOT defeated by a path precheck. We
# pin directory FDs below and recheck identities before publishing, but do not
# claim safety against a malicious root/operator/broker host process.
validate_destinations() {
    python3 - "$PROJECT_ROOT" "$SCRIPT_SOURCE" <<'PY'
import os, re, stat, sys
from pathlib import Path

try:
    root, script = map(Path, sys.argv[1:])
    if not root.is_absolute() or '..' in root.parts or '..' in script.parts:
        raise ValueError('path')
    effective = os.geteuid()
    operator = int(os.environ.get('SUDO_UID', str(effective))) if effective == 0 else effective
    if operator < 0:
        raise ValueError('operator')
    trusted = {0, operator, effective}
    broker = root / 'config/mosquitto/certs'
    def walk(path, leaf_owners=None, leaf_mode=None, file=False):
        current = Path('/')
        for i, part in enumerate(path.parts[1:]):
            current /= part
            try:
                st = os.lstat(current)
            except FileNotFoundError:
                return None  # Remaining components will be created beneath a checked parent.
            last = i == len(path.parts) - 2
            owners = leaf_owners if last and leaf_owners is not None else trusted
            if file and not last and current == broker:
                owners = trusted | {uid}  # Only the broker leaf, never config/mosquitto ancestors.
            if st.st_uid not in owners or stat.S_ISLNK(st.st_mode):
                raise ValueError('owner/symlink')
            if file and last:
                if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                    raise ValueError('file')
                if st.st_mode & 0o022:
                    raise ValueError('writable file')
            elif not stat.S_ISDIR(st.st_mode):
                raise ValueError('parent type')
            elif st.st_mode & 0o022 and not (st.st_mode & stat.S_ISVTX and current in root.parents):
                raise ValueError('writable parent')
            if last and leaf_mode is not None and stat.S_IMODE(st.st_mode) != leaf_mode:
                raise ValueError('mode')
        return st

    root_stat = walk(root)
    if root_stat is None or script != root / 'scripts/gen-certs.sh' or walk(script, file=True) is None:
        raise ValueError('repository')
    # Read data, never source/eval shell. Only public certificate settings are
    # used; other deployment env values are neither exported nor printed.
    values = dict(os.environ)
    env = root / '.env'
    if walk(env, file=True, leaf_mode=0o600) is not None:
        fd = os.open(env, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as f:
            if os.fstat(f.fileno()).st_size > 1048576:
                raise ValueError('env size')
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                m = re.fullmatch(r'([A-Z][A-Z0-9_]*)=(.*)', line)
                if not m or m[1] in {'PROJECT_ROOT', 'MOSQUITTO_CERTS_DIR', 'SECURE_CA_DIR', 'SCRIPT_SOURCE'}:
                    raise ValueError('env assignment')
                if m[1] not in {'MQTT_BROKER_DOMAIN', 'MQTT_ADDITIONAL_SAN', 'MQTT_CERT_UID', 'MQTT_CERT_GID'}:
                    continue
                value = m[2]
                if value.startswith(('"', "'")):
                    if len(value) < 2 or value[-1] != value[0]:
                        raise ValueError('quote')
                    value = value[1:-1]
                values[m[1]] = value
    uid, gid = int(values.get('MQTT_CERT_UID', '1883')), int(values.get('MQTT_CERT_GID', '1883'))
    if min(uid, gid) < 0 or max(uid, gid) >= 4294967295:
        raise ValueError('identity')
    if effective != 0 and (uid, gid) != (effective, os.getegid()):
        raise ValueError('cannot assign broker identity')
    domain = values.get('MQTT_BROKER_DOMAIN', 'localhost')
    extra = values.get('MQTT_ADDITIONAL_SAN', '')
    if not re.fullmatch(r'[A-Za-z0-9.-]+', domain) or any(s and not re.fullmatch(r'[A-Za-z0-9.-]+', s) for s in extra.split(',')):
        raise ValueError('SAN')
    broker = root / 'config/mosquitto/certs'
    private = root / 'certs'
    walk(broker, leaf_owners=trusted | {uid}, leaf_mode=0o755)
    walk(private, leaf_mode=0o700)
    for name, mode in [('ca.crt', 0o644), ('server.crt', 0o644), ('server.key', 0o600)]:
        walk(broker / name, leaf_owners=trusted | {uid}, leaf_mode=mode, file=True)
    for name, mode in [('ca.key', 0o600), ('ca.crt', 0o644)]:
        walk(private / name, leaf_mode=mode, file=True)
    print(f'{root_stat.st_dev}:{root_stat.st_ino}\n{uid}\n{gid}\n{domain}\n{extra}\nOK')
except (OSError, ValueError, OverflowError):
    print('Certificate validation failed: unsafe path/ownership/mode/config; publication refused', file=sys.stderr)
    sys.exit(1)
PY
}

# ALL existing ancestors, leaves and optional env are checked before mkdir,
# chmod/chown, copying keys, openssl or any other filesystem mutation.
PRECHECK="$(validate_destinations)" || exit 1
mapfile -t SETTINGS <<< "$PRECHECK"
[ "${#SETTINGS[@]}" = 6 ] && [ "${SETTINGS[5]}" = OK ] || exit 1
MQTT_CERT_UID="${SETTINGS[1]}"
MQTT_CERT_GID="${SETTINGS[2]}"
MQTT_BROKER_DOMAIN="${SETTINGS[3]}"
MQTT_ADDITIONAL_SAN="${SETTINGS[4]}"
FORCE_NEW_CA=false
SANS=(localhost mosquitto 127.0.0.1 "$MQTT_BROKER_DOMAIN")
IFS=',' read -ra EXTRA <<< "${MQTT_ADDITIONAL_SAN:-}"
SANS+=("${EXTRA[@]}")
for arg in "$@"; do
    case "$arg" in
        --new-ca|--force-ca) FORCE_NEW_CA=true ;;
        *) SANS+=("$arg") ;;
    esac
done
for san in "${SANS[@]}"; do
    [ -z "$san" ] && continue
    [[ "$san" =~ ^[A-Za-z0-9.-]+$ ]] || { echo 'Invalid SAN input' >&2; exit 1; }
done
exec {ROOT_FD}< "$PROJECT_ROOT"
[ "$(stat -Lc '%d:%i' "/proc/self/fd/$ROOT_FD")" = "${SETTINGS[0]}" ] || exit 1
CERTS_CREATED=false
[ -d "$MOSQUITTO_CERTS_DIR" ] || CERTS_CREATED=true
mkdir -p -m 755 "/proc/self/fd/$ROOT_FD/config/mosquitto/certs"
mkdir -p -m 700 "/proc/self/fd/$ROOT_FD/certs"
exec {CERTS_FD}< "/proc/self/fd/$ROOT_FD/config/mosquitto/certs"
exec {CA_FD}< "/proc/self/fd/$ROOT_FD/certs"
readonly BROKER_DIR="/proc/self/fd/$CERTS_FD"
readonly PRIVATE_DIR="/proc/self/fd/$CA_FD"
# Stage everything before publishing. No rm of existing cert/key and no sudo rm.
# Generation/ownership failures leave every existing certificate byte untouched.
STAGE="$(mktemp -d "$PRIVATE_DIR/.generation.XXXXXX")"
PUBLISHED=()
PUBLICATION_COMPLETE=false
finish_generation() {
    local result=$? name target recovery_failed=false
    if [ "$PUBLICATION_COMPLETE" = false ]; then
        # Keep old inodes until every replacement succeeds. On a normal command
        # failure, restore only names touched by our publication. This is NOT a
        # crash-atomic multi-file update; coordinated maintenance is mandatory.
        for name in "${PUBLISHED[@]}"; do
            case "$name" in
                private-ca.crt) target="$PRIVATE_DIR/ca.crt" ;;
                private-ca.key) target="$PRIVATE_DIR/ca.key" ;;
                *) target="$BROKER_DIR/$name" ;;
            esac
            if [ -f "$STAGE/old-$name" ]; then
                if [ "$STAGE/old-$name" -ef "$target" ]; then
                    : # A failed rename may have left the original inode untouched.
                else
                    mv -f "$STAGE/old-$name" "$target" || recovery_failed=true
                fi
            else
                rm -f -- "$target" || recovery_failed=true
            fi
        done
    fi
    if [ "$recovery_failed" = false ]; then
        rm -rf -- "$STAGE"
    else
        echo 'Certificate rollback incomplete; protected staging retained for operator recovery' >&2
        result=1
    fi
    return "$result"
}
trap finish_generation EXIT
CA_KEY="$PRIVATE_DIR/ca.key"
CA_CRT="$BROKER_DIR/ca.crt"
NEW_CA=true
if [ "$FORCE_NEW_CA" = false ] && { [ -e "$CA_KEY" ] || [ -e "$CA_CRT" ]; }; then
    [ -f "$CA_KEY" ] && [ -f "$CA_CRT" ] || { echo 'Incomplete CA; refusing implicit CA rotation' >&2; exit 1; }
    [ "$(stat -c %a "$CA_KEY")" = 600 ] || { echo 'CA key permissions must be 0600' >&2; exit 1; }
    cp "$CA_KEY" "$STAGE/ca.key"
    cp "$CA_CRT" "$STAGE/ca.crt"
    NEW_CA=false
else
    openssl req -new -x509 -days 3650 -extensions v3_ca -newkey rsa:4096 -nodes \
        -keyout "$STAGE/ca.key" -out "$STAGE/ca.crt" \
        -subj '/O=IoT-Gateway-Platform/OU=Security/CN=IoT-Platform-Root-CA' 2>/dev/null
fi
openssl req -new -newkey rsa:2048 -nodes -keyout "$STAGE/server.key" \
    -out "$STAGE/server.csr" -subj "/O=IoT-Gateway-Platform/OU=Broker/CN=$MQTT_BROKER_DOMAIN" 2>/dev/null
cat > "$STAGE/server.ext" <<'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names
[alt_names]
EOF
dns=0; ip=0
for san in "${SANS[@]}"; do
    [ -z "$san" ] && continue
    if [[ "$san" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        ip=$((ip+1)); printf 'IP.%s=%s\n' "$ip" "$san" >> "$STAGE/server.ext"
    else
        dns=$((dns+1)); printf 'DNS.%s=%s\n' "$dns" "$san" >> "$STAGE/server.ext"
    fi
done
openssl x509 -req -in "$STAGE/server.csr" -CA "$STAGE/ca.crt" -CAkey "$STAGE/ca.key" \
    -set_serial "0x$(openssl rand -hex 16)" -out "$STAGE/server.crt" -days 730 \
    -extfile "$STAGE/server.ext" 2>/dev/null
openssl verify -CAfile "$STAGE/ca.crt" -verify_hostname "$MQTT_BROKER_DOMAIN" "$STAGE/server.crt" >/dev/null

# Fail closed: exact mapped owner and 0600 key. No chmod644 fallback, no prompts.
set_broker_permissions() {
    local target="$1" mode="$2"
    if [ "$(id -u)" = "$MQTT_CERT_UID" ] && [ "$(id -g)" = "$MQTT_CERT_GID" ]; then
        chmod "$mode" "$target"
    elif [ "$(id -u)" = 0 ]; then
        chmod "$mode" "$target"
        chown "$MQTT_CERT_UID:$MQTT_CERT_GID" "$target"
    else
        echo 'Cannot assign broker identity; no insecure fallback' >&2
        return 1
    fi
    [ "$(stat -Lc '%u:%g:%a' "$target")" = "$MQTT_CERT_UID:$MQTT_CERT_GID:$mode" ] || return 1
}
chmod 600 "$STAGE/ca.key"
chmod 644 "$STAGE/ca.crt" "$STAGE/server.crt"
set_broker_permissions "$STAGE/server.key" 600
set_broker_permissions "$STAGE/server.crt" 644
# A separate public CA copy can have broker ownership without changing CA-key owner.
cp "$STAGE/ca.crt" "$STAGE/broker-ca.crt"
set_broker_permissions "$STAGE/broker-ca.crt" 644
# Never chmod/chown an existing directory to "repair" validation failures.
# Assign only a freshly created broker leaf; preserve all existing ancestors.
if [ "$CERTS_CREATED" = true ]; then
    set_broker_permissions "$BROKER_DIR" 755
fi
POSTCHECK="$(validate_destinations)" || exit 1
[ "$POSTCHECK" = "$PRECHECK" ] || { echo 'Certificate configuration changed during generation' >&2; exit 1; }
for pair in "$PROJECT_ROOT:$ROOT_FD" "$MOSQUITTO_CERTS_DIR:$CERTS_FD" "$SECURE_CA_DIR:$CA_FD"; do
    path="${pair%:*}"; fd="${pair##*:}"
    [ "$(stat -Lc '%d:%i' "$path")" = "$(stat -Lc '%d:%i' "/proc/self/fd/$fd")" ] || { echo 'Certificate destination identity changed' >&2; exit 1; }
done
# Hardlink backups preserve old bytes, ownership/modes and inodes on normal
# publication failure. Refuse cross-filesystem destinations before replacement;
# no copy+unlink fallback for private keys or server certificates.
[ "$(stat -Lc %d "$BROKER_DIR")" = "$(stat -Lc %d "$PRIVATE_DIR")" ] || { echo 'Certificate directories must share a filesystem' >&2; exit 1; }
for name in server.key server.crt ca.crt private-ca.crt private-ca.key; do
    case "$name" in
        private-ca.crt) target="$PRIVATE_DIR/ca.crt" ;;
        private-ca.key) target="$PRIVATE_DIR/ca.key" ;;
        *) target="$BROKER_DIR/$name" ;;
    esac
    if [ -f "$target" ]; then ln -- "$target" "$STAGE/old-$name"; fi
done
# mv within the same filesystem is atomic per file, not an atomic bundle.
# Planned maintenance is still required; this tool does not reload/deploy.
PUBLISHED+=(server.key)
mv -f "$STAGE/server.key" "$BROKER_DIR/server.key"
PUBLISHED+=(server.crt)
mv -f "$STAGE/server.crt" "$BROKER_DIR/server.crt"
PUBLISHED+=(ca.crt)
mv -f "$STAGE/broker-ca.crt" "$BROKER_DIR/ca.crt"
PUBLISHED+=(private-ca.crt)
mv -f "$STAGE/ca.crt" "$PRIVATE_DIR/ca.crt"
if [ "$NEW_CA" = true ]; then
    PUBLISHED+=(private-ca.key)
    mv -f "$STAGE/ca.key" "$CA_KEY"
fi
PUBLICATION_COMPLETE=true
echo 'Certificate generation completed; no broker reload performed.'
