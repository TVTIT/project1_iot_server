import copy
import ipaddress
import os
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import unittest
import shutil
import importlib.util
import sys

import paper_p8_remote as remote

SMOKE_PATH = Path(__file__).with_name('native_compose_smoke.py')
SMOKE_SPEC = importlib.util.spec_from_file_location('native_compose_smoke', SMOKE_PATH)
smoke = importlib.util.module_from_spec(SMOKE_SPEC)
sys.modules[SMOKE_SPEC.name] = smoke
SMOKE_SPEC.loader.exec_module(smoke)


def valid_config():
    return {'schema_version': 1, 'bind_ip': '192.168.50.20', 'advertised_host': 'fixture-a.lan',
        'https_port': 14443, 'mqtt_tls_port': 18884,
        'required_san_dns': ['fixture-a.lan', 'localhost', 'mosquitto', 'nginx'],
        'required_san_ips': ['127.0.0.1', '192.168.50.20']}


class RemoteNetworkProfileTests(unittest.TestCase):
    def test_strict_config_requires_private_bind_advertised_host_ports_and_sans(self):
        result = remote.validate_config(valid_config())
        self.assertEqual(result['advertised_https'], 'https://fixture-a.lan:14443')
        self.assertEqual(result['advertised_mqtt_tls'], 'mqtts://fixture-a.lan:18884')
        self.assertEqual(result['bind_ip'], '192.168.50.20')

    def test_config_rejects_public_wildcard_malformed_colliding_and_unsupported_inputs(self):
        baseline = valid_config()
        variants = [
            ({**baseline, 'bind_ip': '0.0.0.0'}, 'remote_bind_rejected'),
            ({**baseline, 'bind_ip': '203.0.113.7'}, 'remote_bind_rejected'),
            ({**baseline, 'advertised_host': 'https://user@fixture-a.lan/path'}, 'remote_host_rejected'),
            ({**baseline, 'advertised_host': 'public.example.com'}, 'remote_host_rejected'),
            ({**baseline, 'https_port': baseline['mqtt_tls_port']}, 'remote_ports_rejected'),
            ({**baseline, 'https_port': float('nan')}, 'remote_ports_rejected'),
            ({**baseline, 'unknown': 'ignored'}, 'remote_config_keys_rejected'),
            ({**baseline, 'required_san_dns': ['localhost']}, 'remote_san_requirements_rejected'),
        ]
        for config, error in variants:
            with self.subTest(config=config), self.assertRaisesRegex(ValueError, error):
                remote.validate_config(config)
        with self.assertRaisesRegex(ValueError, 'remote_config_rejected'):
            remote.validate_config([])

    def test_effective_ports_are_exact_and_local_loopback_contract_unchanged(self):
        profile = remote.validate_config(valid_config())
        services = {'nginx': {'ports': [{'target': 443, 'published': '14443',
                    'host_ip': '192.168.50.20', 'protocol': 'tcp'}]},
            'mosquitto': {'ports': [{'target': 8883, 'published': '18884',
                    'host_ip': '192.168.50.20', 'protocol': 'tcp'}]},
            'postgres': {'ports': []}, 'backend': {'ports': []}}
        self.assertTrue(remote.validate_effective_ports(services, remote_config=profile,
            paper_p2=True, paper_p8_load=False))
        bad = copy.deepcopy(services)
        bad['postgres']['ports'] = [{'target': 5432, 'published': '15432',
                                     'host_ip': '192.168.50.20', 'protocol': 'tcp'}]
        with self.assertRaisesRegex(ValueError, 'remote_exposure_rejected'):
            remote.validate_effective_ports(bad, remote_config=profile,
                paper_p2=True, paper_p8_load=False)
        local = {'nginx': {'ports': [{'target': 443, 'published': '0',
                    'host_ip': '127.0.0.1', 'protocol': 'tcp'}]},
                 'mosquitto': {'ports': []}, 'postgres': {'ports': []}}
        self.assertTrue(remote.validate_effective_ports(local, remote_config=None,
            paper_p2=True, paper_p8_load=False))

    def test_runtime_readback_requires_exact_owned_bind_and_port(self):
        profile = remote.validate_config(valid_config())
        for service, target, port in (('nginx', 443, 14443), ('mosquitto', 8883, 18884)):
            row = {'HostIp': '192.168.50.20', 'HostPort': str(port)}
            remote.validate_runtime_mapping(service, {f'{target}/tcp': [row]}, profile)
            with self.assertRaisesRegex(ValueError, 'remote_mapping_rejected'):
                remote.validate_runtime_mapping(service,
                    {f'{target}/tcp': [{**row, 'HostIp': '0.0.0.0'}]}, profile)
            with self.assertRaisesRegex(ValueError, 'remote_mapping_rejected'):
                remote.validate_runtime_mapping(service,
                    {f'{target}/tcp': [{**row, 'HostPort': str(port + 1)}]}, profile)

    def test_remote_profile_compose_model_and_https_certificate_sans_are_exact(self):
        from cryptography import x509
        root = Path(tempfile.mkdtemp(prefix='fixture-', dir='/var/tmp/opencode/p8-remote-step2'))
        root.chmod(0o700)
        self.addCleanup(shutil.rmtree, root)
        profile = remote.validate_config(valid_config())
        fixture = smoke.prepare(root/'fixture', remote_network_config=profile)
        model = smoke.dry_effective_config(fixture)
        smoke.validate_effective_model(fixture, model)
        services = model['services']
        self.assertEqual(services['nginx']['ports'][0]['host_ip'], profile['bind_ip'])
        self.assertEqual(str(services['nginx']['ports'][0]['published']), str(profile['https_port']))
        self.assertEqual(services['mosquitto']['ports'][0]['target'], 8883)
        self.assertEqual(services['mosquitto']['ports'][0]['host_ip'], profile['bind_ip'])
        cert = x509.load_pem_x509_certificate((fixture.private/'p2-tls'/'server.crt').read_bytes())
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        self.assertEqual(set(san.get_values_for_type(x509.DNSName)), set(profile['required_san_dns']))
        self.assertEqual({str(ip) for ip in san.get_values_for_type(x509.IPAddress)},
                         set(profile['required_san_ips']))

    def test_tls_probe_verifies_ca_and_advertised_hostname(self):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
        from datetime import datetime, timedelta, timezone

        root = Path(tempfile.mkdtemp(prefix='tls-', dir='/var/tmp/opencode/p8-remote-step2'))
        root.chmod(0o700)
        self.addCleanup(__import__('shutil').rmtree, root)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'Step2 Test CA')])
        now = datetime.now(timezone.utc)
        ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
            .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
            .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False,
                key_encipherment=False, data_encipherment=False, key_agreement=False,
                key_cert_sign=True, crl_sign=True, encipher_only=False, decipher_only=False), True)
            .sign(ca_key, hashes.SHA256()))
        server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'fixture-a.lan')])
        cert = (x509.CertificateBuilder().subject_name(server_name).issuer_name(ca_name)
            .public_key(server_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('fixture-a.lan')]), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
            .sign(ca_key, hashes.SHA256()))
        ca_path, key_path, cert_path = root/'ca.pem', root/'server.key', root/'server.pem'
        ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM)); ca_path.chmod(0o600)
        key_path.write_bytes(server_key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption())); key_path.chmod(0o600)
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM)); cert_path.chmod(0o600)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert_path, key_path)
        listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(4)
        port = listener.getsockname()[1]
        self.addCleanup(listener.close)
        def accept_connections():
            for _ in range(3):
                try:
                    peer, _ = listener.accept()
                    try:
                        with server_context.wrap_socket(peer, server_side=True):
                            pass
                    except ssl.SSLError:
                        peer.close()
                except OSError:
                    return
        worker = threading.Thread(target=accept_connections, daemon=True); worker.start()
        self.assertTrue(remote.probe_tls('127.0.0.1', port, 'fixture-a.lan', ca_path))
        with self.assertRaisesRegex(ValueError, 'remote_tls_probe_failed'):
            remote.probe_tls('127.0.0.1', port, 'wrong.fixture-a.lan', ca_path)
        with self.assertRaisesRegex(ValueError, 'remote_tls_probe_failed'):
            remote.probe_tls('127.0.0.1', port, 'fixture-a.lan', root/'missing-ca.pem')
        listener.close(); worker.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
