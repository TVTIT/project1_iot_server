package config

import (
	"testing"
	"time"
)

func TestLoadRequiresDatabaseURL(t *testing.T) {
	_, err := Load(func(_ string) (string, bool) {
		return "", false
	})
	if err == nil {
		t.Fatal("Load() error = nil, want missing DATABASE_URL error")
	}
}

func TestLoadUsesSafeOperationalDefaults(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL": "postgres://user:password@database:5432/iot",
	}

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ServerPort != 8080 {
		t.Errorf("ServerPort = %d, want 8080", cfg.ServerPort)
	}
	if cfg.DatabaseMaxConns != 10 {
		t.Errorf("DatabaseMaxConns = %d, want 10", cfg.DatabaseMaxConns)
	}
	if cfg.HTTPReadHeaderTimeout != 5*time.Second {
		t.Errorf("HTTPReadHeaderTimeout = %s, want 5s", cfg.HTTPReadHeaderTimeout)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "port out of range", key: "SERVER_PORT", value: "70000"},
		{name: "zero database connections", key: "DATABASE_MAX_CONNS", value: "0"},
		{name: "invalid duration syntax", key: "HTTP_READ_HEADER_TIMEOUT", value: "soon"},
		{name: "negative duration", key: "HTTP_READ_HEADER_TIMEOUT", value: "-5s"},
		{name: "zero duration", key: "HTTP_READ_HEADER_TIMEOUT", value: "0s"},
		{name: "min conns greater than max conns", key: "DATABASE_MIN_CONNS", value: "20"},
		{name: "zero queue capacity", key: "MQTT_QUEUE_CAPACITY", value: "0"},
		{name: "negative worker count", key: "MQTT_WORKER_COUNT", value: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{
				"DATABASE_URL": "postgres://user:password@database:5432/iot",
				tt.key:         tt.value,
			}

			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for %s=%q", tt.key, tt.value)
			}
		})
	}
}

func TestLoadRejectsDatabaseURLWithoutPostgresScheme(t *testing.T) {
	_, err := Load(mapLookup(map[string]string{
		"DATABASE_URL": "user:password@database:5432/iot",
	}))
	if err == nil {
		t.Fatal("Load() error = nil, want invalid DATABASE_URL error")
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:password@database:5432/iot")
	t.Setenv("SERVER_PORT", "9090")

	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatalf("LoadFromEnvironment() error = %v", err)
	}
	if cfg.ServerPort != 9090 {
		t.Errorf("ServerPort = %d, want 9090", cfg.ServerPort)
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
