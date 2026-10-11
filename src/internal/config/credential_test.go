package config

import (
	"fmt"
	"strings"
	"testing"
)

//nolint:misspell // Mosquitto is the selected product and local service name.
func credentialValues() map[string]string {
	v := validConfigValues()
	for k, x := range map[string]string{"MQTT_CREDENTIAL_API_ENABLED": "true", "INGRESS_GATE_ENABLED": "true", "MQTT_DYNSEC_MANAGER_USERNAME": "credential_manager", "MQTT_DYNSEC_MANAGER_PASSWORD": "test-only-manager-password", "MQTT_PASSWORD": "test-only-backend-password", "MQTT_INTERNAL_MANAGEMENT_URL": "ssl://mosquitto:18884", "MQTT_TLS_CA_FILE": "/mosquitto/config/certs/ca.crt", "MQTT_DYNSEC_JSON_PATH": "/mosquitto/dynsec/dynamic-security.json", "MQTT_CONTROLLER_DIR": "/mosquitto/controller", "HTTP_WRITE_TIMEOUT": "55s", "SHUTDOWN_TIMEOUT": "55s"} {
		v[k] = x
	}
	return v
}

func TestCredentialConfiguration(t *testing.T) {
	c, e := Load(mapLookup(validConfigValues()))
	if e != nil || c.Credential.Enabled {
		t.Fatal("disabled default", e)
	}
	c, e = Load(mapLookup(credentialValues()))
	if e != nil || !c.Credential.Enabled {
		t.Fatal("enabled configuration", e)
	}
	for _, rendered := range []string{fmt.Sprint(c.Credential), fmt.Sprintf("%#v", c.Credential)} {
		if strings.Contains(rendered, c.Credential.ManagerPassword) || strings.Contains(rendered, c.Credential.BackendPassword) {
			t.Fatal("configuration secret formatting")
		}
	}
	for k, value := range map[string]string{
		"MQTT_CREDENTIAL_API_ENABLED": "bad", "MQTT_CREDENTIAL_RUNTIME_ENABLED": "true", "MQTT_DYNSEC_MANAGER_USERNAME": "backend_service", "MQTT_DYNSEC_MANAGER_PASSWORD": "", "MQTT_PASSWORD": "", "MQTT_INTERNAL_MANAGEMENT_URL": "tcp://mosquitto:18884", "MQTT_DYNSEC_JSON_PATH": "relative", "MQTT_CONTROLLER_DIR": "relative", "MQTT_CREDENTIAL_DB_TIMEOUT": "0s", "MQTT_CREDENTIAL_FINALIZE_TIMEOUT": "61s", "MQTT_CREDENTIAL_RECONCILE_TIMEOUT": "11m", "MQTT_CREDENTIAL_RECONCILE_BATCH_SIZE": "1001", "MQTT_CREDENTIAL_RECONCILE_MAX_PAGES": "0", "MQTT_DYNSEC_QUEUE_SIZE": "4097", "HTTP_WRITE_TIMEOUT": "30s", "SHUTDOWN_TIMEOUT": "10s", "MQTT_CREDENTIAL_REQUEST_TIMEOUT": "1s",
	} {
		t.Run(k, func(t *testing.T) {
			v := credentialValues()
			v[k] = value
			if _, e := Load(mapLookup(v)); e == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}

	t.Run("omitted optional legacy gate and controller paths accepted", func(t *testing.T) {
		v := credentialValues()
		delete(v, "INGRESS_GATE_ENABLED")
		delete(v, "MQTT_CONTROLLER_DIR")
		delete(v, "MQTT_DYNSEC_JSON_PATH")
		c, err := Load(mapLookup(v))
		if err != nil {
			t.Fatalf("expected omitted legacy settings to load, got error: %v", err)
		}
		if !c.Credential.Enabled {
			t.Fatal("expected credential config to be enabled")
		}
		if c.Credential.ControlDir != "" || c.Credential.SnapshotPath != "" {
			t.Fatalf("expected empty legacy paths, got ControlDir=%q, SnapshotPath=%q", c.Credential.ControlDir, c.Credential.SnapshotPath)
		}
	})

	t.Run("explicit INGRESS_GATE_ENABLED false accepted", func(t *testing.T) {
		v := credentialValues()
		v["INGRESS_GATE_ENABLED"] = "false"
		c, err := Load(mapLookup(v))
		if err != nil {
			t.Fatalf("expected INGRESS_GATE_ENABLED=false to be accepted, got error: %v", err)
		}
		if !c.Credential.Enabled {
			t.Fatal("expected credential config to be enabled")
		}
	})

	t.Run("invalid INGRESS_GATE_ENABLED rejected", func(t *testing.T) {
		v := credentialValues()
		v["INGRESS_GATE_ENABLED"] = "not-a-bool"
		if _, err := Load(mapLookup(v)); err == nil {
			t.Fatal("expected error for non-boolean INGRESS_GATE_ENABLED")
		}
	})

	t.Run("omitted CA file rejected", func(t *testing.T) {
		v := credentialValues()
		delete(v, "MQTT_TLS_CA_FILE")
		if _, err := Load(mapLookup(v)); err == nil {
			t.Fatal("expected error when MQTT_TLS_CA_FILE is omitted")
		}
	})

	t.Run("relative CA file rejected", func(t *testing.T) {
		v := credentialValues()
		v["MQTT_TLS_CA_FILE"] = "relative/ca.crt"
		if _, err := Load(mapLookup(v)); err == nil {
			t.Fatal("expected error when MQTT_TLS_CA_FILE is relative")
		}
	})

	t.Run("controller dir too long rejected", func(t *testing.T) {
		v := credentialValues()
		v["MQTT_CONTROLLER_DIR"] = "/" + strings.Repeat("a", 80)
		if _, err := Load(mapLookup(v)); err == nil {
			t.Fatal("expected error when MQTT_CONTROLLER_DIR is too long")
		}
	})
}
