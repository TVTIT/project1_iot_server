package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ingressValues returns a configuration with ingress enabled and a valid data
// identity so individual cases can override exactly one field.
func ingressValues() map[string]string {
	v := validConfigValues()
	v["MQTT_INGRESS_ENABLED"] = "true"
	v["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
	v["MQTT_CLIENT_ID"] = "telemetry_ingress"
	v["MQTT_USERNAME"] = "backend_service"
	v["MQTT_PASSWORD"] = "test-only-ingress-password"
	v["MQTT_TLS_CA_FILE"] = "/mosquitto/config/certs/ca.crt"
	return v
}

func TestIngressFlagIndependence(t *testing.T) {
	t.Run("disabled by default independent of credential flags", func(t *testing.T) {
		for _, credential := range []string{"", "false", "true"} {
			values := validConfigValues()
			if credential != "" {
				if credential == "true" {
					values = credentialValues()
					// Credential config reads MQTT_BROKER_URL/MQTT_USERNAME/MQTT_PASSWORD.
					values["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
					values["MQTT_USERNAME"] = "backend_service"
				} else {
					values["MQTT_CREDENTIAL_API_ENABLED"] = credential
				}
			}
			cfg, err := Load(mapLookup(values))
			if err != nil {
				t.Fatalf("credential=%q: %v", credential, err)
			}
			if cfg.Ingress.Enabled {
				t.Fatalf("credential=%q: ingress must default to disabled", credential)
			}
		}
	})

	t.Run("legacy runtime flag does not enable ingress", func(t *testing.T) {
		values := validConfigValues()
		for k, v := range map[string]string{
			"MQTT_CREDENTIAL_RUNTIME_ENABLED": "true",
			"MQTT_BROKER_URL":                 "ssl://broker:8883",
			"MQTT_USERNAME":                   "backend_service",
			"MQTT_PASSWORD":                   "literal-backend-password",
			"MQTT_TLS_CA_FILE":                "/trust/ca.crt",
			"MQTT_AUTH_DIR":                   "/mosquitto/auth",
			"MQTT_RELOAD_SOCKET":              "/mosquitto/control/reload.sock",
			"MOSQUITTO_PASSWD_PATH":           "/usr/bin/mosquitto_passwd",
		} {
			values[k] = v
		}
		cfg, err := Load(mapLookup(values))
		if err != nil {
			t.Fatalf("legacy runtime fixture: %v", err)
		}
		if cfg.Ingress.Enabled {
			t.Fatal("legacy runtime must not enable ingress")
		}
	})

	t.Run("ingress does not require credential API", func(t *testing.T) {
		cfg, err := Load(mapLookup(ingressValues()))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.Ingress.Enabled || cfg.Credential.Enabled {
			t.Fatalf("ingress=%v credential=%v", cfg.Ingress.Enabled, cfg.Credential.Enabled)
		}
	})

	t.Run("ingress and credential API enabled together", func(t *testing.T) {
		values := credentialValues()
		values["MQTT_INGRESS_ENABLED"] = "true"
		values["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
		values["MQTT_CLIENT_ID"] = "telemetry_ingress"
		values["MQTT_USERNAME"] = "backend_service"
		values["MQTT_PASSWORD"] = "test-only-backend-password"
		values["MQTT_TLS_CA_FILE"] = "/mosquitto/config/certs/ca.crt"
		cfg, err := Load(mapLookup(values))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.Ingress.Enabled || !cfg.Credential.Enabled {
			t.Fatal("expected both ingress and credential API enabled")
		}
	})
}

func TestIngressDisabledIgnoresIdentityAndCA(t *testing.T) {
	values := validConfigValues()
	values["MQTT_BROKER_URL"] = "not-a-url"
	values["MQTT_CLIENT_ID"] = "bad\nid"
	values["MQTT_USERNAME"] = " "
	values["MQTT_PASSWORD"] = "bad\nsecret"
	values["MQTT_TLS_CA_FILE"] = "relative/ca.crt"
	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("disabled ingress must not validate unused identity/CA: %v", err)
	}
	if cfg.Ingress.Enabled {
		t.Fatal("ingress must remain disabled")
	}
}

func TestIngressEnabledRequiresIdentity(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string // empty means delete
	}{
		{name: "missing broker URL", key: "MQTT_BROKER_URL"},
		{name: "missing client ID", key: "MQTT_CLIENT_ID"},
		{name: "blank client ID", key: "MQTT_CLIENT_ID", value: "   "},
		{name: "missing username", key: "MQTT_USERNAME"},
		{name: "missing password", key: "MQTT_PASSWORD"},
		{name: "missing CA file", key: "MQTT_TLS_CA_FILE"},
		{name: "non-TLS broker scheme", key: "MQTT_BROKER_URL", value: "tcp://mosquitto:1883"},
		{name: "broker URL with userinfo", key: "MQTT_BROKER_URL", value: "ssl://user:pass@mosquitto:8883"},
		{name: "broker URL without port", key: "MQTT_BROKER_URL", value: "ssl://mosquitto"},
		{name: "broker URL with path", key: "MQTT_BROKER_URL", value: "ssl://mosquitto:8883/mqtt"},
		{name: "broker URL with query", key: "MQTT_BROKER_URL", value: "ssl://mosquitto:8883?a=1"},
		{name: "broker URL with fragment", key: "MQTT_BROKER_URL", value: "ssl://mosquitto:8883#x"},
		{name: "relative CA file", key: "MQTT_TLS_CA_FILE", value: "relative/ca.crt"},
		{name: "client ID with newline", key: "MQTT_CLIENT_ID", value: "bad\nid"},
		{name: "password with NUL", key: "MQTT_PASSWORD", value: "bad\x00secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := ingressValues()
			if tt.value == "" {
				delete(values, tt.key)
			} else {
				values[tt.key] = tt.value
			}
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for %s=%q", tt.key, tt.value)
			}
		})
	}
}

func TestIngressIdentityBounds(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "client ID too long", key: "MQTT_CLIENT_ID", value: strings.Repeat("a", maxMQTTClientIDLength+1)},
		{name: "username too long", key: "MQTT_USERNAME", value: strings.Repeat("a", maxMQTTUsernameLength+1)},
		{name: "password too long", key: "MQTT_PASSWORD", value: strings.Repeat("a", maxMQTTPasswordLength+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := ingressValues()
			values[tt.key] = tt.value
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("accepted oversized %s", tt.key)
			}
		})
	}

	t.Run("boundary lengths accepted", func(t *testing.T) {
		values := ingressValues()
		values["MQTT_CLIENT_ID"] = strings.Repeat("a", maxMQTTClientIDLength)
		values["MQTT_USERNAME"] = strings.Repeat("a", maxMQTTUsernameLength)
		values["MQTT_PASSWORD"] = strings.Repeat("a", maxMQTTPasswordLength)
		if _, err := Load(mapLookup(values)); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})
}

func TestIngressNumericAndDurationBounds(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "zero queue capacity", key: "MQTT_QUEUE_CAPACITY", value: "0"},
		{name: "negative queue capacity", key: "MQTT_QUEUE_CAPACITY", value: "-1"},
		{name: "zero worker count", key: "MQTT_WORKER_COUNT", value: "0"},
		{name: "zero payload", key: "MQTT_MAX_PAYLOAD_BYTES", value: "0"},
		{name: "payload above supported maximum", key: "MQTT_MAX_PAYLOAD_BYTES", value: "1048577"},
		{name: "zero connect timeout", key: "MQTT_CONNECT_TIMEOUT", value: "0s"},
		{name: "negative connect timeout", key: "MQTT_CONNECT_TIMEOUT", value: "-1s"},
		{name: "invalid connect timeout", key: "MQTT_CONNECT_TIMEOUT", value: "soon"},
		{name: "excessive connect timeout", key: "MQTT_CONNECT_TIMEOUT", value: "2m"},
		{name: "zero subscribe timeout", key: "MQTT_SUBSCRIBE_TIMEOUT", value: "0s"},
		{name: "excessive subscribe timeout", key: "MQTT_SUBSCRIBE_TIMEOUT", value: "2m"},
		{name: "zero publish timeout", key: "MQTT_PUBLISH_TIMEOUT", value: "0s"},
		{name: "excessive publish timeout", key: "MQTT_PUBLISH_TIMEOUT", value: "2m"},
		{name: "zero reconnect interval", key: "MQTT_RECONNECT_MAX_INTERVAL", value: "0s"},
		{name: "excessive reconnect interval", key: "MQTT_RECONNECT_MAX_INTERVAL", value: "11m"},
		{name: "zero samples", key: "TELEMETRY_MAX_SAMPLES_PER_MESSAGE", value: "0"},
		{name: "samples above int32", key: "TELEMETRY_MAX_SAMPLES_PER_MESSAGE", value: "2147483648"},
		{name: "zero batch duration", key: "TELEMETRY_MAX_BATCH_DURATION", value: "0s"},
		{name: "excessive batch duration", key: "TELEMETRY_MAX_BATCH_DURATION", value: "2h"},
		{name: "negative future skew", key: "TELEMETRY_MAX_FUTURE_SKEW", value: "-1s"},
		{name: "excessive future skew", key: "TELEMETRY_MAX_FUTURE_SKEW", value: "2h"},
		{name: "zero DB timeout", key: "TELEMETRY_DB_TIMEOUT", value: "0s"},
		{name: "excessive DB timeout", key: "TELEMETRY_DB_TIMEOUT", value: "2m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				values := validConfigValues()
				if enabled {
					values = ingressValues()
				}
				values[tt.key] = tt.value
				if _, err := Load(mapLookup(values)); err == nil {
					t.Fatalf("Load() error = nil for %s=%q (ingress=%v)", tt.key, tt.value, enabled)
				}
			}
		})
	}
}

func TestIngressPayloadDefaultAndMaximum(t *testing.T) {
	t.Run("default 64 KiB", func(t *testing.T) {
		cfg, err := Load(mapLookup(validConfigValues()))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.Ingress.MaxPayloadBytes != 65536 {
			t.Fatalf("MaxPayloadBytes = %d, want 65536", cfg.Ingress.MaxPayloadBytes)
		}
	})

	t.Run("supported maximum 1 MiB accepted", func(t *testing.T) {
		values := ingressValues()
		values["MQTT_MAX_PAYLOAD_BYTES"] = "1048576"
		cfg, err := Load(mapLookup(values))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.Ingress.MaxPayloadBytes != MaxMQTTMaxPayloadBytes {
			t.Fatalf("MaxPayloadBytes = %d, want %d", cfg.Ingress.MaxPayloadBytes, MaxMQTTMaxPayloadBytes)
		}
	})
}

func TestIngressQueueMemoryBound(t *testing.T) {
	t.Run("default profile is 16 MiB", func(t *testing.T) {
		cfg, err := Load(mapLookup(validConfigValues()))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		got := int64(cfg.Ingress.QueueCapacity) * int64(cfg.Ingress.MaxPayloadBytes)
		if got != DefaultIngressQueueMemoryBytes {
			t.Fatalf("default queue budget = %d, want %d", got, DefaultIngressQueueMemoryBytes)
		}
	})

	t.Run("supported cap accepted", func(t *testing.T) {
		values := ingressValues()
		values["MQTT_QUEUE_CAPACITY"] = "256"
		values["MQTT_MAX_PAYLOAD_BYTES"] = "1048576"
		cfg, err := Load(mapLookup(values))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		got := int64(cfg.Ingress.QueueCapacity) * int64(cfg.Ingress.MaxPayloadBytes)
		if got != MaxIngressQueueMemoryBytes {
			t.Fatalf("queue budget = %d, want cap %d", got, MaxIngressQueueMemoryBytes)
		}
	})

	t.Run("over supported cap rejected", func(t *testing.T) {
		values := ingressValues()
		values["MQTT_QUEUE_CAPACITY"] = "1000"
		values["MQTT_MAX_PAYLOAD_BYTES"] = "1048576"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted queue budget above the supported cap")
		}
	})

	t.Run("huge capacity rejected", func(t *testing.T) {
		values := ingressValues()
		values["MQTT_QUEUE_CAPACITY"] = "1000000"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted oversized queue capacity budget")
		}
	})
}

func TestIngressPoolReservation(t *testing.T) {
	t.Run("default leaves room", func(t *testing.T) {
		if _, err := Load(mapLookup(ingressValues())); err != nil {
			t.Fatalf("credential disabled: %v", err)
		}
		values := credentialValues()
		values["MQTT_INGRESS_ENABLED"] = "true"
		values["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
		values["MQTT_CLIENT_ID"] = "telemetry_ingress"
		values["MQTT_USERNAME"] = "backend_service"
		values["MQTT_PASSWORD"] = "test-only-backend-password"
		values["MQTT_TLS_CA_FILE"] = "/mosquitto/config/certs/ca.crt"
		if _, err := Load(mapLookup(values)); err != nil {
			t.Fatalf("credential enabled: %v", err)
		}
	})

	t.Run("workers may not consume reserved connections", func(t *testing.T) {
		// DATABASE_MAX_CONNS=10, credential disabled -> 8 workers fit exactly.
		values := ingressValues()
		values["MQTT_WORKER_COUNT"] = "9"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted worker count exceeding reserved connections")
		}
	})

	t.Run("credential owner reduces worker budget", func(t *testing.T) {
		values := credentialValues()
		values["MQTT_INGRESS_ENABLED"] = "true"
		values["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
		values["MQTT_CLIENT_ID"] = "telemetry_ingress"
		values["MQTT_USERNAME"] = "backend_service"
		values["MQTT_PASSWORD"] = "test-only-backend-password"
		values["MQTT_TLS_CA_FILE"] = "/mosquitto/config/certs/ca.crt"
		// 8 workers fit without the credential owner but not with it (7 available).
		values["MQTT_WORKER_COUNT"] = "8"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted worker count ignoring the credential owner connection")
		}
		values["MQTT_WORKER_COUNT"] = "7"
		if _, err := Load(mapLookup(values)); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})

	t.Run("small pool rejected", func(t *testing.T) {
		values := ingressValues()
		values["DATABASE_MAX_CONNS"] = "3"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted workers exceeding a small shared pool")
		}
	})

	t.Run("reservation not enforced while disabled", func(t *testing.T) {
		values := validConfigValues()
		values["MQTT_WORKER_COUNT"] = "9"
		if _, err := Load(mapLookup(values)); err != nil {
			t.Fatalf("disabled ingress must not enforce pool reservation: %v", err)
		}
	})
}

func TestIngressShutdownBudget(t *testing.T) {
	t.Run("too small rejected when enabled", func(t *testing.T) {
		values := ingressValues()
		values["SHUTDOWN_TIMEOUT"] = "5s"
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatal("accepted shutdown budget below DB + publish timeout")
		}
	})

	t.Run("exact budget accepted when enabled", func(t *testing.T) {
		values := ingressValues()
		values["SHUTDOWN_TIMEOUT"] = "8s"
		if _, err := Load(mapLookup(values)); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})

	t.Run("default shutdown preserved", func(t *testing.T) {
		cfg, err := Load(mapLookup(validConfigValues()))
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.ShutdownTimeout != 10*time.Second {
			t.Fatalf("ShutdownTimeout = %s, want 10s", cfg.ShutdownTimeout)
		}
	})
}

func TestIngressOperationalDefaults(t *testing.T) {
	cfg, err := Load(mapLookup(validConfigValues()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Ingress.QueueCapacity != 256 || cfg.Ingress.WorkerCount != 4 {
		t.Fatalf("queue=%d workers=%d", cfg.Ingress.QueueCapacity, cfg.Ingress.WorkerCount)
	}
	if cfg.Ingress.ConnectTimeout != 5*time.Second || cfg.Ingress.SubscribeTimeout != 3*time.Second ||
		cfg.Ingress.PublishTimeout != 3*time.Second || cfg.Ingress.ReconnectMaxInterval != 30*time.Second {
		t.Fatalf("mqtt timeouts: connect=%s subscribe=%s publish=%s reconnect=%s",
			cfg.Ingress.ConnectTimeout, cfg.Ingress.SubscribeTimeout, cfg.Ingress.PublishTimeout, cfg.Ingress.ReconnectMaxInterval)
	}
	if cfg.Ingress.MaxSamplesPerMessage != 1000 || cfg.Ingress.MaxBatchDuration != time.Minute ||
		cfg.Ingress.MaxFutureSkew != 2*time.Minute || cfg.Ingress.DBTimeout != 5*time.Second {
		t.Fatalf("telemetry limits: samples=%d batch=%s skew=%s db=%s",
			cfg.Ingress.MaxSamplesPerMessage, cfg.Ingress.MaxBatchDuration, cfg.Ingress.MaxFutureSkew, cfg.Ingress.DBTimeout)
	}
}

func TestIngressRedaction(t *testing.T) {
	cfg, err := Load(mapLookup(ingressValues()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	for _, rendered := range []string{fmt.Sprint(cfg.Ingress), fmt.Sprintf("%+v", cfg.Ingress), fmt.Sprintf("%#v", cfg.Ingress)} {
		for _, secret := range []string{cfg.Ingress.Password, cfg.Ingress.Username, cfg.Ingress.BrokerURL, cfg.Ingress.CAFile, cfg.Ingress.ClientID} {
			if strings.Contains(rendered, secret) {
				t.Fatalf("ingress formatting leaked %q", secret)
			}
		}
	}
}
