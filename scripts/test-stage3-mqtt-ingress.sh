#!/bin/sh
# Bounded real integration smoke: MQTT telemetry ingress through the pinned
# broker to the actual backend and TimescaleDB, independent SQL and post-commit
# application ACK. Disposable fixture; never sources the deployment .env.
set -eu

PROJECT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
exec python3 "$PROJECT_ROOT/scripts/tests/stage3_mqtt_ingress_runner.py"
