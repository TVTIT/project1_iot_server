"""Source guard only: does not resolve deployment env or contact Docker."""
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


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

    def test_legacy_compose_cannot_opt_in(self):
        backend = (ROOT / 'docker-compose.yml').read_text().split('  backend:', 1)[1]
        self.assertIn('MQTT_CREDENTIAL_API_ENABLED: "false"', backend)
        self.assertNotIn('MQTT_DYNSEC_MANAGER_PASSWORD', backend)


if __name__ == '__main__':
    unittest.main()
