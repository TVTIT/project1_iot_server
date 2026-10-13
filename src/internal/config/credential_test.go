package config

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"iot-platform/internal/mqttcredential"
)

//nolint:misspell // Mosquitto is the selected product and local service name.
func credentialValues() map[string]string {
	v := validConfigValues()
	for k, x := range map[string]string{"MQTT_CREDENTIAL_API_ENABLED": "true", "INGRESS_GATE_ENABLED": "true", "MQTT_DYNSEC_MANAGER_USERNAME": "credential_manager", "MQTT_DYNSEC_MANAGER_PASSWORD": "test-only-manager-password", "MQTT_PASSWORD": "test-only-backend-password", "MQTT_INTERNAL_MANAGEMENT_URL": "ssl://mosquitto:18884", "MQTT_TLS_CA_FILE": "/mosquitto/config/certs/ca.crt", "MQTT_DYNSEC_JSON_PATH": "/mosquitto/dynsec/dynamic-security.json", "MQTT_CONTROLLER_DIR": "/mosquitto/controller", "HTTP_WRITE_TIMEOUT": "55s", "SHUTDOWN_TIMEOUT": "55s"} {
		v[k] = x
	}
	v["MQTT_DYNSEC_WRITER_UID"] = "1883"
	v["MQTT_DYNSEC_IMAGE_REFERENCE"] = mqttcredential.QualifiedMosquittoImageRef
	v["MQTT_DYNSEC_IMAGE_CONFIG_ID"] = mqttcredential.QualifiedMosquittoConfigID
	v["MQTT_DYNSEC_PLATFORM"] = mqttcredential.QualifiedMosquittoPlatform
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
	if c.Credential.WriterUID != 1883 || !c.Credential.Qualification.Valid() {
		t.Fatal("expected explicit writer UID and typed qualified identity")
	}
	for _, rendered := range []string{fmt.Sprint(c.Credential), fmt.Sprintf("%+v", c.Credential), fmt.Sprintf("%#v", c.Credential)} {
		for _, sentinel := range []string{c.Credential.ManagerPassword, c.Credential.BackendPassword, c.Credential.SnapshotPath, "1883", mqttcredential.QualifiedMosquittoImageRef, mqttcredential.QualifiedMosquittoConfigID, mqttcredential.QualifiedMosquittoPlatform} {
			if strings.Contains(rendered, sentinel) {
				t.Fatal("configuration formatting must remain fully redacted")
			}
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
		c, err := Load(mapLookup(v))
		if err != nil {
			t.Fatalf("expected omitted legacy settings to load, got error: %v", err)
		}
		if !c.Credential.Enabled {
			t.Fatal("expected credential config to be enabled")
		}
		if c.Credential.ControlDir != "" {
			t.Fatal("expected empty compatibility controller path")
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

func TestCredentialPersistenceRequirements(t *testing.T) {
	for key, invalid := range map[string][]string{
		"MQTT_DYNSEC_JSON_PATH":       {"", "relative", "/synthetic/../store.json"},
		"MQTT_DYNSEC_WRITER_UID":      {"", "-1", "+1", "bad", "4294967296"},
		"MQTT_DYNSEC_IMAGE_REFERENCE": {"", "synthetic-image:latest"},
		"MQTT_DYNSEC_IMAGE_CONFIG_ID": {"", "synthetic-config-id"},
		"MQTT_DYNSEC_PLATFORM":        {"", "synthetic-platform"},
	} {
		for i, value := range invalid {
			t.Run(fmt.Sprintf("%s/%d", key, i), func(t *testing.T) {
				v := credentialValues()
				if value == "" {
					delete(v, key)
				} else {
					v[key] = value
				}
				_, err := Load(mapLookup(v))
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatal("expected safe error naming invalid configuration key")
				}
				if value != "" && strings.Contains(err.Error(), value) {
					t.Fatal("error echoed configuration value")
				}
				v["MQTT_CREDENTIAL_API_ENABLED"] = "false"
				if _, err := Load(mapLookup(v)); err != nil {
					t.Fatal("disabled credential API must not require persistence config", err)
				}
			})
		}
	}
	// Deliberately absent on disk: loading config must not stat/read the store.
	for _, uid := range []string{"0", "4294967295"} {
		v := credentialValues()
		v["MQTT_DYNSEC_JSON_PATH"] = "/synthetic-w1a-no-store/dynamic-security.json"
		v["MQTT_DYNSEC_WRITER_UID"] = uid
		c, err := Load(mapLookup(v))
		if err != nil {
			t.Fatal("explicit uint32 UID and synthetic path must load without I/O", err)
		}
		want, _ := strconv.ParseUint(uid, 10, 32)
		if c.Credential.WriterUID != uint32(want) || !c.Credential.Qualification.Valid() {
			t.Fatal("explicit persistence identity was not preserved")
		}
	}
}
