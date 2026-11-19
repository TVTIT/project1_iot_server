"""Strict opt-in LAN/private endpoint profile for P8 fixture-only server startup."""
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import ssl
import stat


CONFIG_KEYS = {'schema_version', 'bind_ip', 'advertised_host', 'https_port', 'mqtt_tls_port',
               'required_san_dns', 'required_san_ips'}
MAX_CONFIG_BYTES = 8192
PRIVATE_V4 = tuple(ipaddress.ip_network(value) for value in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))
LOCAL_SUFFIXES = ('.lan', '.local', '.internal', '.test')


def _private_ipv4(value, code):
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise ValueError(code) from None
    if (not isinstance(address, ipaddress.IPv4Address)
            or not any(address in network for network in PRIVATE_V4)
            or address.is_unspecified or address.is_multicast):
        raise ValueError(code)
    return address


def _advertised_host(value):
    if not isinstance(value, str) or not value or len(value) > 253 or value != value.strip():
        raise ValueError('remote_host_rejected')
    try:
        return _private_ipv4(value, 'remote_host_rejected').compressed, 'ip'
    except ValueError:
        pass
    if any(char in value for char in '/:@?#[]\\%') or value.endswith('.'):
        raise ValueError('remote_host_rejected')
    host = value.lower()
    if not any(host.endswith(suffix) for suffix in LOCAL_SUFFIXES):
        raise ValueError('remote_host_rejected')
    labels = host.split('.')
    if any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) for label in labels):
        raise ValueError('remote_host_rejected')
    return host, 'dns'


def validate_config(value):
    if type(value) is not dict:
        raise ValueError('remote_config_rejected')
    if set(value) != CONFIG_KEYS:
        raise ValueError('remote_config_keys_rejected')
    if type(value['schema_version']) is not int or value['schema_version'] != 1:
        raise ValueError('remote_config_rejected')
    bind_ip = _private_ipv4(value['bind_ip'], 'remote_bind_rejected').compressed
    advertised_host, host_kind = _advertised_host(value['advertised_host'])
    ports = (value['https_port'], value['mqtt_tls_port'])
    if any(type(port) is not int or not 1024 <= port <= 65535 for port in ports) or ports[0] == ports[1]:
        raise ValueError('remote_ports_rejected')
    dns = value['required_san_dns']
    ips = value['required_san_ips']
    if (type(dns) is not list or type(ips) is not list or len(dns) > 8 or len(ips) > 8
            or any(type(item) is not str for item in dns + ips)):
        raise ValueError('remote_san_requirements_rejected')
    required_dns = {'localhost', 'nginx', 'mosquitto'}
    required_ips = {'127.0.0.1', bind_ip}
    if host_kind == 'dns':
        required_dns.add(advertised_host)
    else:
        required_ips.add(advertised_host)
    try:
        normalized_dns = [item.lower() for item in dns]
        normalized_ips = [ipaddress.ip_address(item).compressed for item in ips]
    except ValueError:
        raise ValueError('remote_san_requirements_rejected') from None
    if (len(set(normalized_dns)) != len(normalized_dns) or len(set(normalized_ips)) != len(normalized_ips)
            or set(normalized_dns) != required_dns or set(normalized_ips) != required_ips):
        raise ValueError('remote_san_requirements_rejected')
    return {'schema_version': 1, 'bind_ip': bind_ip, 'advertised_host': advertised_host,
        'advertised_host_kind': host_kind, 'https_port': ports[0], 'mqtt_tls_port': ports[1],
        'required_san_dns': sorted(required_dns), 'required_san_ips': sorted(required_ips),
        'advertised_https': f'https://{_url_host(advertised_host)}:{ports[0]}',
        'advertised_mqtt_tls': f'mqtts://{_url_host(advertised_host)}:{ports[1]}'}


def _url_host(host):
    return f'[{host}]' if ':' in host else host


def load_config(path):
    path = Path(path).absolute()
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('remote_config_path_rejected')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                or st.st_mode & 0o022 or st.st_size > MAX_CONFIG_BYTES):
            raise ValueError('remote_config_file_rejected')
        raw = stream.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError('remote_config_file_rejected')
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ValueError('remote_config_rejected') from None
    return validate_config(value)


def preflight_bind(profile):
    """Confirm bind IP belongs to this host and both requested ports are free."""
    bind_ip = profile['bind_ip']
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((bind_ip, 0))
    except OSError:
        raise ValueError('remote_bind_not_local') from None
    held = []
    try:
        for port in (profile['https_port'], profile['mqtt_tls_port']):
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.bind((bind_ip, port))
            except OSError:
                probe.close()
                raise ValueError('remote_port_unavailable') from None
            held.append(probe)
    finally:
        for probe in held:
            probe.close()
    return True


def validate_effective_ports(services, *, remote_config, paper_p2, paper_p8_load):
    remote = remote_config is not None
    if 'nginx' not in services or remote and 'mosquitto' not in services:
        raise ValueError('remote_exposure_rejected' if remote else 'effective_ports_rejected')
    nginx_ip = remote_config['bind_ip'] if remote else '127.0.0.1'
    nginx_port = remote_config['https_port'] if remote else '0'
    nginx_target = 443 if paper_p2 else 80
    mosquitto_expected = None
    if remote:
        mosquitto_expected = (remote_config['bind_ip'], remote_config['mqtt_tls_port'], 8883)
    elif paper_p8_load:
        mosquitto_expected = ('127.0.0.1', '0', 8883)
    for name, service in services.items():
        ports = service.get('ports', [])
        if name == 'nginx':
            expected = (nginx_ip, str(nginx_port), nginx_target)
        elif name == 'mosquitto' and mosquitto_expected:
            expected = (mosquitto_expected[0], str(mosquitto_expected[1]), mosquitto_expected[2])
        else:
            if ports:
                raise ValueError('remote_exposure_rejected' if remote else 'effective_ports_rejected')
            continue
        if (len(ports) != 1 or ports[0].get('host_ip') != expected[0]
                or str(ports[0].get('published')) != expected[1]
                or ports[0].get('target') != expected[2] or ports[0].get('protocol') != 'tcp'):
            raise ValueError('remote_exposure_rejected' if remote else 'effective_ports_rejected')
    return True


def validate_runtime_mapping(service, port_map, profile):
    target, expected_port = (443, profile['https_port']) if service == 'nginx' else (8883, profile['mqtt_tls_port']) if service == 'mosquitto' else (None, None)
    rows = port_map.get(f'{target}/tcp') if target else None
    if (not isinstance(rows, list) or len(rows) != 1 or type(rows[0]) is not dict
            or rows[0].get('HostIp') != profile['bind_ip']
            or str(rows[0].get('HostPort')) != str(expected_port)):
        raise ValueError('remote_mapping_rejected')
    return expected_port


def probe_tls(connect_host, port, server_name, ca_file, *, timeout=3.0):
    """Verify chain and hostname with system TLS validation; never disables verification."""
    if (not isinstance(connect_host, str) or not isinstance(server_name, str)
            or type(port) is not int or not 1 <= port <= 65535
            or isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout) or not 0 < timeout <= 10):
        raise ValueError('remote_tls_probe_rejected')
    try:
        context = ssl.create_default_context(cafile=str(ca_file))
        with socket.create_connection((connect_host, port), timeout=timeout) as raw:
            with context.wrap_socket(raw, server_hostname=server_name):
                return True
    except (OSError, ssl.SSLError, ValueError):
        raise ValueError('remote_tls_probe_failed') from None
