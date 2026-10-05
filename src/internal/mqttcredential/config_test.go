//nolint:misspell // Mosquitto paths and environment keys are protocol/configuration fixtures.
package mqttcredential

import (
	"strings"
	"testing"
	"time"
)

func startupEnv() map[string]string {
	return map[string]string{
		"MQTT_CREDENTIAL_RUNTIME_ENABLED": "true", "MQTT_BROKER_URL": "ssl://broker:8883", "MQTT_USERNAME": BackendUsername, "MQTT_PASSWORD": " literal password ", "MQTT_TLS_CA_FILE": "/trust/ca.crt", "MQTT_AUTH_DIR": "/mosquitto/auth", "MQTT_RELOAD_SOCKET": "/mosquitto/control/reload.sock", "MOSQUITTO_PASSWD_PATH": "/usr/bin/mosquitto_passwd",
	}
}
func loadMap(m map[string]string) (StartupConfig, error) {
	return LoadConfig(func(k string) (string, bool) { v, ok := m[k]; return v, ok })
}

func TestBackendPasswordValidation(t *testing.T) {
	for _, password := range []string{"", " \t ", "\u2003", "replace_with_backend_mqtt_password", "REPLACE_WITH_password", "CHANGE_ME", "a\nb", "a\rb", "a\x00b", strings.Repeat("x", 4097)} {
		if ValidateBackendPassword(password) == nil {
			t.Fatal("unsafe backend password accepted")
		}
		m := startupEnv()
		m["MQTT_PASSWORD"] = password
		if _, err := loadMap(m); err == nil {
			t.Fatal("runtime config accepted unsafe backend password")
		}
	}
	for _, password := range []string{" literal password ", "secret with spaces", strings.Repeat("x", 4096)} {
		if err := ValidateBackendPassword(password); err != nil {
			t.Fatal("valid verbatim password rejected")
		}
	}
}
func TestStartupConfig(t *testing.T) {
	c, e := loadMap(startupEnv())
	if e != nil || c.Password != " literal password " || c.Runtime.MaxPending != 8 || c.Runtime.OperationTimeout != 10*time.Second {
		t.Fatalf("defaults: %v", e)
	}
	for k, v := range startupEnv() {
		t.Run("missing_"+k, func(t *testing.T) {
			if k == "MQTT_CREDENTIAL_RUNTIME_ENABLED" {
				return
			}
			m := startupEnv()
			delete(m, k)
			if _, e := loadMap(m); e == nil {
				t.Fatal("accepted missing setting")
			}
		})
		_ = v
	}
	for k, values := range map[string][]string{
		"MQTT_CREDENTIAL_RUNTIME_ENABLED": {"bogus"}, "MQTT_BROKER_URL": {"tcp://broker:1883", "ssl://u:p@broker:8883", "ssl://broker:8883/path"}, "MQTT_USERNAME": {"other", "backend_service\n"}, "MQTT_PASSWORD": {"", "replace_with_password", "a\nb", "a\x00b"}, "MQTT_AUTH_DIR": {"relative", "/a/../b", "/a\x00b"}, "MQTT_RELOAD_SOCKET": {strings.Repeat("/x", 60)}, "MQTT_CREDENTIAL_MAX_PENDING": {"0", "1025"}, "MQTT_CREDENTIAL_MAX_FILE_BYTES": {"-1", "67108865"}, "MQTT_RELOAD_TIMEOUT": {"0s", "61s", "bad"},
	} {
		for _, v := range values {
			t.Run(k+v, func(t *testing.T) {
				m := startupEnv()
				m[k] = v
				if _, e := loadMap(m); e == nil {
					t.Fatal("accepted invalid setting")
				}
			})
		}
	}
	if _, e := loadMap(map[string]string{"MQTT_CREDENTIAL_RUNTIME_ENABLED": "false", "MQTT_BROKER_URL": "bad"}); e != nil {
		t.Fatal(e)
	}
}
