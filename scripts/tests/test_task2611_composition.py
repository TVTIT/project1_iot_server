"""Source guard only: does not resolve deployment env or contact Docker."""
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


def _extract_backend_service_block(compose_text: str) -> str:
    lines = compose_text.splitlines()
    backend_lines = []
    in_backend = False
    for line in lines:
        if line.startswith('  backend:'):
            in_backend = True
            backend_lines.append(line)
            continue
        if in_backend:
            if line and not line.startswith('    '):
                break
            backend_lines.append(line)
    return '\n'.join(backend_lines)


def _assert_root_backend_contract(test_case: unittest.TestCase, backend_block: str) -> None:
    test_case.assertIn('target: backend', backend_block)
    test_case.assertIn('MQTT_CREDENTIAL_RUNTIME_ENABLED: "false"', backend_block)
    test_case.assertIn('MQTT_CREDENTIAL_API_ENABLED: ${MQTT_CREDENTIAL_API_ENABLED:-false}', backend_block)
    test_case.assertIn('MQTT_DYNSEC_MANAGER_PASSWORD: ${MQTT_DYNSEC_MANAGER_PASSWORD:-}', backend_block)
    test_case.assertNotIn('MQTT_DYNSEC_MANAGER_PASSWORD: "', backend_block)
    depends_on = backend_block.split('depends_on:', 1)[1] if 'depends_on:' in backend_block else ''
    test_case.assertNotIn('controller', depends_on.lower())
    test_case.assertNotIn('mosquitto-credential-controller', backend_block)
    test_case.assertNotIn('/mosquitto/controller', backend_block)
    test_case.assertNotIn('mosquitto_control:', backend_block)
    test_case.assertNotIn('control.sock', backend_block)


class CompositionTemplates(unittest.TestCase):
    def test_closed_single_broker_and_private_management(self):
        config = json.loads((ROOT / 'config/mosquitto/lifecycle-startup.json.example').read_text())
        self.assertEqual(config['Lifecycle']['BrokerAddress'], '127.0.0.1:18884')
        self.assertEqual(config['Controller']['ManagementAddress'], '127.0.0.1:18884')
        self.assertEqual(config['Lifecycle']['PublicAddresses'], ['0.0.0.0:8883'])
        native = (ROOT / 'config/mosquitto/lifecycle.conf.example').read_text()
        self.assertEqual([x for x in native.splitlines() if x.startswith('listener ')],
                         ['listener 18884 127.0.0.1'])
        template = (ROOT / 'config/mosquitto/lifecycle-compose.yml.example').read_text()
        self.assertNotIn(':18884"', template)
        self.assertNotIn('pid: service:', template)
        self.assertIn('exec /app/mosquitto-credential-controller <', template)
        backend = template.split('  backend:', 1)[1]
        self.assertNotIn('MQTT_BROKER_CERT_SOURCE', backend)
        self.assertIn('source: ${MQTT_TLS_CA_SOURCE:?required}', backend)
        self.assertIn('MQTT_CREDENTIAL_API_ENABLED: "false"', backend)
        self.assertIn('local_addr = "mosquitto:8883"',
                      (ROOT / 'client_rathole.toml.example').read_text())

    def test_root_compose_credential_opt_in_contract(self):
        compose_text = (ROOT / 'docker-compose.yml').read_text()
        backend = _extract_backend_service_block(compose_text)
        _assert_root_backend_contract(self, backend)

    def test_root_compose_rejects_security_regressions(self):
        compose_text = (ROOT / 'docker-compose.yml').read_text()
        backend = _extract_backend_service_block(compose_text)
        counterexamples = {
            'default enabled true': backend.replace(
                'MQTT_CREDENTIAL_API_ENABLED: ${MQTT_CREDENTIAL_API_ENABLED:-false}',
                'MQTT_CREDENTIAL_API_ENABLED: ${MQTT_CREDENTIAL_API_ENABLED:-true}',
            ),
            'hardcoded manager secret': backend.replace(
                'MQTT_DYNSEC_MANAGER_PASSWORD: ${MQTT_DYNSEC_MANAGER_PASSWORD:-}',
                'MQTT_DYNSEC_MANAGER_PASSWORD: "fixed-plaintext-password"',
            ),
            'introduced controller dependency': backend.replace(
                '      mosquitto:\n',
                '      mosquitto-controller:\n        condition: service_started\n      mosquitto:\n',
            ),
            'introduced controller mount': backend.replace(
                'mosquitto_dynsec:/mosquitto/dynsec:ro',
                'mosquitto_control:/mosquitto/controller:ro',
            ),
            'introduced controller socket': backend.replace(
                'mosquitto_dynsec:/mosquitto/dynsec:ro',
                '/var/run/mosquitto/control.sock:/var/run/mosquitto/control.sock',
            ),
            'introduced controller executable': backend.replace(
                '    container_name: iot_backend',
                '    command: ["/app/mosquitto-credential-controller"]\n    container_name: iot_backend',
            ),
            'legacy target': backend.replace(
                'target: backend',
                'target: legacy-controller',
            ),
        }
        for name, candidate in counterexamples.items():
            with self.subTest(name=name):
                self.assertNotEqual(candidate, backend, f'counterexample {name} must change the block')
                with self.assertRaises(AssertionError):
                    _assert_root_backend_contract(self, candidate)

if __name__ == '__main__':
    unittest.main()
