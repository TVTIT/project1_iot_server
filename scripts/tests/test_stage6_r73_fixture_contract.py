"""Keep the owned MQTT fixture's Stage 6 convergence lane required."""
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class Stage6R73FixtureContractTests(unittest.TestCase):
    def test_native_gateway_lane_requires_runtime_convergence_marker(self):
        runner = (ROOT / "scripts/tests/stage5_mqtt_fixture.py").read_text()
        self.assertIn('TestNativeMQTTStage6GatewayQueryAndReceiptRoundTrip', runner)
        self.assertIn("'R7.3 convergence PASS:' not in output", runner)
        self.assertIn("'--- SKIP:' in output", runner)


if __name__ == "__main__":
    unittest.main()
