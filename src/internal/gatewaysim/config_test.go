package gatewaysim

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// validGatewayEnv returns a minimal complete Gateway simulator environment.
func validGatewayEnv() map[string]string {
	return map[string]string{
		"GATEWAY_ID":              "gateway_001",
		"SENSOR_ID":               "sensor_001",
		"GATEWAY_MQTT_BROKER_URL": "ssl://mosquitto:8883",
		"GATEWAY_MQTT_CLIENT_ID":  "gateway_sim_001",
		"GATEWAY_MQTT_USERNAME":   "gateway_001",
		"GATEWAY_MQTT_PASSWORD":   "test-only-gateway-password",
		"GATEWAY_MQTT_CA_FILE":    "/mosquitto/config/certs/ca.crt",
		"GATEWAY_OUTBOX_PATH":     "/var/lib/gatewaysim/outbox.db",
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(mapLookup(validGatewayEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.GatewayID != "gateway_001" || cfg.SensorID != "sensor_001" {
		t.Fatalf("identity = %q/%q", cfg.GatewayID, cfg.SensorID)
	}
	if cfg.SampleRateHz != 100 || cfg.SamplesPerBatch != 100 {
		t.Fatalf("profile = %d/%d", cfg.SampleRateHz, cfg.SamplesPerBatch)
	}
	if cfg.SampleIntervalUS != 10000 {
		t.Fatalf("SampleIntervalUS = %d", cfg.SampleIntervalUS)
	}
	if cfg.BatchSpan != 990*time.Millisecond {
		t.Fatalf("BatchSpan = %s", cfg.BatchSpan)
	}
	if cfg.MaxPayloadBytes != 65536 {
		t.Fatalf("MaxPayloadBytes = %d", cfg.MaxPayloadBytes)
	}
	if cfg.AckTimeout != 5*time.Second {
		t.Fatalf("AckTimeout = %s", cfg.AckTimeout)
	}
	if cfg.RetryMinDelay != time.Second || cfg.RetryMaxDelay != 30*time.Second {
		t.Fatalf("retry = %s..%s", cfg.RetryMinDelay, cfg.RetryMaxDelay)
	}
	if cfg.MaxInflight != 8 || cfg.OutboxMaxRows != 5000 || cfg.OutboxMaxBytes != 67_108_864 {
		t.Fatalf("outbox = %d/%d/%d", cfg.MaxInflight, cfg.OutboxMaxRows, cfg.OutboxMaxBytes)
	}
	if cfg.ConnectTimeout != 10*time.Second || cfg.SubscribeTimeout != 5*time.Second ||
		cfg.PublishTimeout != 5*time.Second || cfg.SQLiteTimeout != 5*time.Second ||
		cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("operation timeouts = %s/%s/%s/%s/%s", cfg.ConnectTimeout, cfg.SubscribeTimeout, cfg.PublishTimeout, cfg.SQLiteTimeout, cfg.ShutdownTimeout)
	}
}

func TestLoadRequiresEveryKey(t *testing.T) {
	for _, key := range []string{
		"GATEWAY_ID", "SENSOR_ID", "GATEWAY_MQTT_BROKER_URL", "GATEWAY_MQTT_CLIENT_ID",
		"GATEWAY_MQTT_USERNAME", "GATEWAY_MQTT_PASSWORD", "GATEWAY_MQTT_CA_FILE", "GATEWAY_OUTBOX_PATH",
	} {
		values := validGatewayEnv()
		delete(values, key)
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("expected error when %s is missing", key)
		}
	}
}

func TestLoadRejectsInvalidIdentifiersAndIdentity(t *testing.T) {
	cases := map[string]func(map[string]string){
		"invalid gateway id":  func(v map[string]string) { v["GATEWAY_ID"] = "bad id" },
		"invalid sensor id":   func(v map[string]string) { v["SENSOR_ID"] = "bad/sensor" },
		"username mismatch":   func(v map[string]string) { v["GATEWAY_MQTT_USERNAME"] = "other_gateway" },
		"client id equals gw": func(v map[string]string) { v["GATEWAY_MQTT_CLIENT_ID"] = "gateway_001" },
		"blank password":      func(v map[string]string) { v["GATEWAY_MQTT_PASSWORD"] = "   " },
		"relative ca file":    func(v map[string]string) { v["GATEWAY_MQTT_CA_FILE"] = "certs/ca.crt" },
		"memory outbox":       func(v map[string]string) { v["GATEWAY_OUTBOX_PATH"] = ":memory:" },
		"relative outbox":     func(v map[string]string) { v["GATEWAY_OUTBOX_PATH"] = "outbox.db" },
	}
	for name, mutate := range cases {
		values := validGatewayEnv()
		mutate(values)
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestLoadRejectsInvalidBrokerURL(t *testing.T) {
	invalid := []string{
		"mqtt://mosquitto:8883",
		"ssl://mosquitto",
		"ssl://user:pass@mosquitto:8883",
		"ssl://mosquitto:8883/telemetry",
		"ssl://mosquitto:8883?x=1",
		"ssl://mosquitto:8883#frag",
		"not a url",
	}
	for _, broker := range invalid {
		values := validGatewayEnv()
		values["GATEWAY_MQTT_BROKER_URL"] = broker
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("broker %q: expected error", broker)
		}
	}
	for _, broker := range []string{"ssl://mosquitto:8883", "ssl://example.com:18883"} {
		values := validGatewayEnv()
		values["GATEWAY_MQTT_BROKER_URL"] = broker
		if _, err := Load(mapLookup(values)); err != nil {
			t.Errorf("broker %q: unexpected error %v", broker, err)
		}
	}
}

func TestLoadRejectsInvalidSampleProfile(t *testing.T) {
	cases := map[string]map[string]string{
		"rate not divisor": {"GATEWAY_SAMPLE_RATE_HZ": "300"},
		"rate above cap":   {"GATEWAY_SAMPLE_RATE_HZ": "200000"},
		"rate zero":        {"GATEWAY_SAMPLE_RATE_HZ": "0"},
		"count above cap":  {"GATEWAY_SAMPLES_PER_BATCH": "1001"},
		"count zero":       {"GATEWAY_SAMPLES_PER_BATCH": "0"},
		"span over 60s":    {"GATEWAY_SAMPLE_RATE_HZ": "1", "GATEWAY_SAMPLES_PER_BATCH": "1000"},
	}
	for name, override := range cases {
		values := validGatewayEnv()
		for k, v := range override {
			values[k] = v
		}
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestLoadAcceptsAlternateValidProfile(t *testing.T) {
	values := validGatewayEnv()
	values["GATEWAY_SAMPLE_RATE_HZ"] = "50"
	values["GATEWAY_SAMPLES_PER_BATCH"] = "200"
	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SampleIntervalUS != 20000 || cfg.SamplesPerBatch != 200 {
		t.Fatalf("profile = %d/%d", cfg.SampleIntervalUS, cfg.SamplesPerBatch)
	}
}

func TestLoadRejectsInvalidLimits(t *testing.T) {
	cases := map[string]map[string]string{
		"retry order":        {"GATEWAY_RETRY_MIN_DELAY": "30s", "GATEWAY_RETRY_MAX_DELAY": "1s"},
		"inflight over rows": {"GATEWAY_MAX_INFLIGHT": "100", "GATEWAY_OUTBOX_MAX_ROWS": "50"},
		"rows above cap":     {"GATEWAY_OUTBOX_MAX_ROWS": "2000000"},
		"bytes above cap":    {"GATEWAY_OUTBOX_MAX_BYTES": "99999999999999"},
		"ack not duration":   {"GATEWAY_ACK_TIMEOUT": "soon"},
		"ack too large":      {"GATEWAY_ACK_TIMEOUT": "1h"},
	}
	for name, override := range cases {
		values := validGatewayEnv()
		for k, v := range override {
			values[k] = v
		}
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestConfigRedactsSecrets(t *testing.T) {
	cfg, err := Load(mapLookup(validGatewayEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, formatted := range []string{fmt.Sprintf("%v", cfg), fmt.Sprintf("%#v", cfg)} {
		for _, secret := range []string{
			"test-only-gateway-password",
			"/mosquitto/config/certs/ca.crt",
			"/var/lib/gatewaysim/outbox.db",
		} {
			if strings.Contains(formatted, secret) {
				t.Fatalf("formatted config leaked %q: %s", secret, formatted)
			}
		}
	}
}
