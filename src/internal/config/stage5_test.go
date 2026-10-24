package config

import (
	"testing"
	"time"
)

func TestStage5EnvironmentBounds(t *testing.T) {
	for key, value := range map[string]string{
		"STAGE5_ENABLED": "maybe", "STAGE5_REQUEST_TIMEOUT": "0s",
		"STAGE5_COMMAND_TTL": "25h", "STAGE5_MAX_PENDING_OUTBOX": "0", "STAGE5_MAX_RECEIPTS": "1000001",
	} {
		t.Run(key, func(t *testing.T) {
			values := validConfigValues()
			values[key] = value
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("accepted %s=%s", key, value)
			}
		})
	}
}

func TestStage5DefaultsAndIndependentFlag(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.Stage5.Enabled || cfg.Stage5.Control.DBTimeout != 5*time.Second || cfg.Stage5.Control.TTL != time.Minute {
		t.Fatalf("defaults: %+v %v", cfg.Stage5, err)
	}
	values["STAGE5_ENABLED"] = "true"
	for k, v := range stage5TransportValues() {
		values[k] = v
	}
	enabled, err := Load(mapLookup(values))
	if err != nil || !enabled.Stage5.Enabled || enabled.Ingress.Enabled != cfg.Ingress.Enabled || enabled.Credential.Enabled != cfg.Credential.Enabled {
		t.Fatal("Stage 5 changed unrelated feature flags", err)
	}
}

func stage5TransportValues() map[string]string {
	return map[string]string{
		"MQTT_BROKER_URL": "ssl://localhost:8883", "STAGE5_MQTT_CLIENT_ID": "backend-control", "MQTT_USERNAME": "backend_service", "MQTT_PASSWORD": "data-independent-password-123456789",
		"MQTT_TLS_CA_FILE": "/isolated/ca.crt", "MQTT_INTERNAL_MANAGEMENT_URL": "ssl://localhost:8884", "MQTT_DYNSEC_MANAGER_USERNAME": "fixture_manager", "MQTT_DYNSEC_MANAGER_PASSWORD": "manager-independent-password-123456789",
	}
}

func TestStage5TransportConfiguration(t *testing.T) {
	values := validConfigValues()
	values["STAGE5_ENABLED"] = "true"
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("enabled stage 5 accepted missing data/ACL prerequisites")
	}
	for k, v := range stage5TransportValues() {
		values[k] = v
	}
	for key, value := range map[string]string{"STAGE5_PUBLISH_WORKERS": "17", "STAGE5_SCAN_BATCH": "1025", "STAGE5_RESULT_QUEUE": "0", "STAGE5_PAYLOAD_BYTES": "8193", "STAGE5_EXPIRE_INTERVAL": "0s", "STAGE5_SCAN_INTERVAL": "0s", "STAGE5_RETRY_BACKOFF": "0s", "STAGE5_ACL_TIMEOUT": "0s", "STAGE5_SUBSCRIBE_TIMEOUT": "0s", "STAGE5_PUBLISH_TIMEOUT": "0s", "STAGE5_RECONNECT_MAX_INTERVAL": "0s", "MQTT_DYNSEC_MANAGER_USERNAME": "backend_service", "MQTT_DYNSEC_MANAGER_PASSWORD": values["MQTT_PASSWORD"]} {
		copy := map[string]string{}
		for k, v := range values {
			copy[k] = v
		}
		copy[key] = value
		if _, err := Load(mapLookup(copy)); err == nil {
			t.Errorf("invalid %s", key)
		}
	}
	if cfg, err := Load(mapLookup(values)); err != nil || cfg.Credential.Enabled || cfg.Ingress.Enabled {
		t.Fatalf("independent stage 5 config %v", err)
	}
}
