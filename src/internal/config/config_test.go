package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const testJWTSecret = "test-only-jwt-secret-at-least-32-characters"

func TestMetricsFlagDefaultAndValidation(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MetricsEnabled {
		t.Fatal(cfg.MetricsEnabled, err)
	}
	values["METRICS_ENABLED"] = "true"
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.MetricsEnabled {
		t.Fatal(cfg.MetricsEnabled, err)
	}
	values["METRICS_ENABLED"] = "invalid"
	if _, err = Load(mapLookup(values)); err == nil {
		t.Fatal("accepted invalid flag")
	}
}

func TestWebSocketFlagOptIn(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.WebSocketEnabled {
		t.Fatalf("default websocket flag=%v err=%v", cfg.WebSocketEnabled, err)
	}
	values["WEBSOCKET_ENABLED"] = "true"
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.WebSocketEnabled {
		t.Fatalf("enabled websocket flag=%v err=%v", cfg.WebSocketEnabled, err)
	}
	values["WEBSOCKET_ENABLED"] = "invalid"
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("invalid websocket flag accepted")
	}
}

func TestWebSocketLimitsDefaultsAndValidation(t *testing.T) {
	cfg, err := Load(mapLookup(validConfigValues()))
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.WebSocket
	if cfg.WebSocketEnabled || w.MaxSessions != 32 || w.MaxSubscriptionsPerSession != 16 || w.GlobalEventSlots != 64 || w.GlobalEventBytes != 8<<20 || w.SessionReferenceBytes != 8<<20 || w.SessionEventSlots != 64 || w.SessionEventBytes != 256<<10 || w.MaxFrameBytes != 66560 || w.ReadFrameBytes != 4096 || w.RequestRatePerSecond != 10 || w.RecheckInterval != 5*time.Second || w.RecheckSweepBudget != 2*time.Second || w.RecheckConcurrency != 2 || w.HeartbeatInterval != 20*time.Second || w.PongTimeout != 45*time.Second || w.WriteTimeout != 5*time.Second || w.FanoutMode != "indexed" {
		t.Fatalf("unexpected websocket defaults: enabled=%v config=%+v", cfg.WebSocketEnabled, w)
	}
	for key, value := range map[string]string{"WS_MAX_SUBSCRIPTIONS_PER_SESSION": "17", "WS_MAX_FRAME_BYTES": "66561", "WS_PONG_TIMEOUT": "19s", "WS_RECHECK_CONCURRENCY": "3"} {
		values := validConfigValues()
		values[key] = value
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("accepted %s=%s", key, value)
		}
	}
	for _, value := range []string{"0", "-1", "10001", "nope"} {
		values := validConfigValues()
		values["WS_MAX_SESSIONS"] = value
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("accepted WS_MAX_SESSIONS=%q", value)
		}
	}
	for _, value := range []string{"64", "10000"} {
		values := validConfigValues()
		values["WS_MAX_SESSIONS"] = value
		cfg, err := Load(mapLookup(values))
		if err != nil || cfg.WebSocket.MaxSessions != mustInt(t, value) {
			t.Errorf("WS_MAX_SESSIONS=%s: config=%+v err=%v", value, cfg.WebSocket, err)
		}
	}
	for _, value := range []string{"0", "-1", "268435457", "nope"} {
		values := validConfigValues()
		values["WS_SESSION_REFERENCE_BYTES"] = value
		if _, err := Load(mapLookup(values)); err == nil {
			t.Errorf("accepted WS_SESSION_REFERENCE_BYTES=%q", value)
		}
	}
	for _, value := range []string{"1", "268435456"} {
		values := validConfigValues()
		values["WS_SESSION_REFERENCE_BYTES"] = value
		cfg, err := Load(mapLookup(values))
		if err != nil || cfg.WebSocket.SessionReferenceBytes != mustInt(t, value) {
			t.Errorf("WS_SESSION_REFERENCE_BYTES=%s: config=%+v err=%v", value, cfg.WebSocket, err)
		}
	}
	values := validConfigValues()
	values["WS_MAX_SESSIONS"] = "4"
	values["WS_MAX_SUBSCRIPTIONS_PER_SESSION"] = "3"
	cfg, err = Load(mapLookup(values))
	if err != nil || cfg.WebSocket.MaxSessions != 4 || cfg.WebSocket.MaxSubscriptionsPerSession != 3 {
		t.Fatalf("lower test limits not accepted: %+v err=%v", cfg.WebSocket, err)
	}
}

func TestWebSocketFanoutModeValidation(t *testing.T) {
	// Default: WS_FANOUT_MODE unset -> "indexed"
	cfg, err := Load(mapLookup(validConfigValues()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebSocket.FanoutMode != "indexed" {
		t.Fatalf("default fanout mode=%q, want %q", cfg.WebSocket.FanoutMode, "indexed")
	}

	// Valid values: "indexed" and "scan"
	for _, mode := range []string{"indexed", "scan"} {
		values := validConfigValues()
		values["WS_FANOUT_MODE"] = mode
		c, err := Load(mapLookup(values))
		if err != nil {
			t.Fatalf("valid mode %q rejected: %v", mode, err)
		}
		if c.WebSocket.FanoutMode != mode {
			t.Fatalf("loaded mode=%q, want %q", c.WebSocket.FanoutMode, mode)
		}
	}

	// Empty string defaults to "indexed"
	values := validConfigValues()
	values["WS_FANOUT_MODE"] = ""
	c, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("empty mode rejected: %v", err)
	}
	if c.WebSocket.FanoutMode != "indexed" {
		t.Fatalf("empty mode resulted in %q, want %q", c.WebSocket.FanoutMode, "indexed")
	}

	// Invalid values: must reject with error
	for _, mode := range []string{"invalid", "SCAN", "INDEXED", "broadcast", "direct", "1"} {
		v := validConfigValues()
		v["WS_FANOUT_MODE"] = mode
		if _, err := Load(mapLookup(v)); err == nil {
			t.Fatalf("accepted invalid WS_FANOUT_MODE=%q", mode)
		}
	}
}

func mustInt(t *testing.T, value string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscan(value, &n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMQTTRuntimeOptIn(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MQTTCredentialRuntime.Enabled {
		t.Fatalf("old HTTP fixture: %v", err)
	}
	values["MQTT_CREDENTIAL_RUNTIME_ENABLED"] = "true"
	if _, err = Load(mapLookup(values)); err == nil {
		t.Fatal("enabled runtime accepted missing MQTT settings")
	}
	values["MQTT_CREDENTIAL_RUNTIME_ENABLED"] = "false"
	values["MQTT_PASSWORD"] = "bad\nsecret"
	if _, err = Load(mapLookup(values)); err != nil {
		t.Fatal("disabled runtime validated unused MQTT password")
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	_, err := Load(func(_ string) (string, bool) {
		return "", false
	})
	if err == nil {
		t.Fatal("Load() error = nil, want missing DATABASE_URL error")
	}
}

func TestAdminMaxBodyBytes(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int
		invalid bool
	}{
		{"", 16384, false}, {"1", 1, false}, {"1048576", 1048576, false}, {"0", 0, true}, {"1048577", 0, true}, {"-1", 0, true}, {"1.5", 0, true},
	} {
		values := validConfigValues()
		if tc.raw != "" {
			values["ADMIN_MAX_BODY_BYTES"] = tc.raw
		}
		cfg, err := Load(mapLookup(values))
		if tc.invalid {
			if err == nil {
				t.Fatalf("accepted %q", tc.raw)
			}
		} else if err != nil || cfg.AdminMaxBodyBytes != tc.want {
			t.Fatalf("raw=%q value=%d err=%v", tc.raw, cfg.AdminMaxBodyBytes, err)
		}
	}
}

func TestLoadUsesSafeOperationalDefaults(t *testing.T) {
	values := validConfigValues()

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
	if cfg.SupabaseJWTClockSkew != 30*time.Second {
		t.Errorf("SupabaseJWTClockSkew = %s, want 30s", cfg.SupabaseJWTClockSkew)
	}
	if cfg.SupabaseJWTSecret != testJWTSecret {
		t.Error("SupabaseJWTSecret did not preserve the configured secret")
	}
	if cfg.SupabaseJWTIssuer != "http://localhost/auth/v1" {
		t.Errorf("SupabaseJWTIssuer = %q, want local issuer", cfg.SupabaseJWTIssuer)
	}
	if cfg.SupabaseJWTAudience != "authenticated" {
		t.Errorf("SupabaseJWTAudience = %q, want authenticated", cfg.SupabaseJWTAudience)
	}
	if cfg.AdminInventoryDefaultLimit != 100 {
		t.Errorf("AdminInventoryDefaultLimit = %d, want 100", cfg.AdminInventoryDefaultLimit)
	}
	if cfg.AdminInventoryMaxLimit != 200 {
		t.Errorf("AdminInventoryMaxLimit = %d, want 200", cfg.AdminInventoryMaxLimit)
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
		{name: "zero admin inventory default limit", key: "ADMIN_INVENTORY_DEFAULT_LIMIT", value: "0"},
		{name: "admin inventory default limit above max allowed", key: "ADMIN_INVENTORY_DEFAULT_LIMIT", value: "1001"},
		{name: "zero admin inventory max limit", key: "ADMIN_INVENTORY_MAX_LIMIT", value: "0"},
		{name: "admin inventory max limit above max allowed", key: "ADMIN_INVENTORY_MAX_LIMIT", value: "1001"},
		{name: "admin inventory default limit greater than max limit", key: "ADMIN_INVENTORY_DEFAULT_LIMIT", value: "250"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			values[tt.key] = tt.value

			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for %s=%q", tt.key, tt.value)
			}
		})
	}
}

func TestLoadRejectsDatabaseURLWithoutPostgresScheme(t *testing.T) {
	_, err := Load(mapLookup(map[string]string{
		"DATABASE_URL":          "user:password@database:5432/iot",
		"SUPABASE_JWT_SECRET":   testJWTSecret,
		"SUPABASE_JWT_ISSUER":   "http://localhost/auth/v1",
		"SUPABASE_JWT_AUDIENCE": "authenticated",
	}))
	if err == nil {
		t.Fatal("Load() error = nil, want invalid DATABASE_URL error")
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:password@database:5432/iot")
	t.Setenv("SUPABASE_JWT_SECRET", testJWTSecret)
	t.Setenv("SUPABASE_JWT_ISSUER", "http://localhost/auth/v1")
	t.Setenv("SUPABASE_JWT_AUDIENCE", "authenticated")
	t.Setenv("SERVER_PORT", "9090")

	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatalf("LoadFromEnvironment() error = %v", err)
	}
	if cfg.ServerPort != 9090 {
		t.Errorf("ServerPort = %d, want 9090", cfg.ServerPort)
	}
}

func TestLoadRequiresAuthenticationConfiguration(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "missing JWT secret", key: "SUPABASE_JWT_SECRET"},
		{name: "missing JWT issuer", key: "SUPABASE_JWT_ISSUER"},
		{name: "missing JWT audience", key: "SUPABASE_JWT_AUDIENCE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			delete(values, tt.key)
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil with missing %s", tt.key)
			}
		})
	}
}

func TestLoadRejectsInvalidAuthenticationConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "empty secret", key: "SUPABASE_JWT_SECRET", value: "   "},
		{name: "short secret", key: "SUPABASE_JWT_SECRET", value: "too-short"},
		{name: "relative issuer", key: "SUPABASE_JWT_ISSUER", value: "/auth/v1"},
		{name: "unsupported issuer scheme", key: "SUPABASE_JWT_ISSUER", value: "ftp://auth.example.com/auth/v1"},
		{name: "issuer without host", key: "SUPABASE_JWT_ISSUER", value: "https:///auth/v1"},
		{name: "issuer with user info", key: "SUPABASE_JWT_ISSUER", value: "https://user@auth.example.com/auth/v1"},
		{name: "empty audience", key: "SUPABASE_JWT_AUDIENCE", value: "   "},
		{name: "unexpected audience", key: "SUPABASE_JWT_AUDIENCE", value: "service_role"},
		{name: "invalid clock skew", key: "SUPABASE_JWT_CLOCK_SKEW", value: "soon"},
		{name: "negative clock skew", key: "SUPABASE_JWT_CLOCK_SKEW", value: "-1s"},
		{name: "excessive clock skew", key: "SUPABASE_JWT_CLOCK_SKEW", value: "5m1s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			values[tt.key] = tt.value
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for %s=%q", tt.key, tt.value)
			}
		})
	}
}

func TestLoadAcceptsZeroJWTClockSkew(t *testing.T) {
	values := validConfigValues()
	values["SUPABASE_JWT_CLOCK_SKEW"] = "0s"
	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.SupabaseJWTClockSkew != 0 {
		t.Errorf("SupabaseJWTClockSkew = %s, want 0s", cfg.SupabaseJWTClockSkew)
	}
}

func TestLoadAuthenticationErrorDoesNotExposeSecret(t *testing.T) {
	secret := "sensitive-secret-that-must-not-appear-in-errors"
	values := validConfigValues()
	values["SUPABASE_JWT_SECRET"] = secret
	values["SUPABASE_JWT_ISSUER"] = "/invalid"
	_, err := Load(mapLookup(values))
	if err == nil {
		t.Fatal("Load() error = nil, want invalid issuer error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("Load() error exposed SUPABASE_JWT_SECRET")
	}
}

func validConfigValues() map[string]string {
	return map[string]string{
		"DATABASE_URL":          "postgres://user:password@database:5432/iot",
		"SUPABASE_JWT_SECRET":   testJWTSecret,
		"SUPABASE_JWT_ISSUER":   "http://localhost/auth/v1",
		"SUPABASE_JWT_AUDIENCE": "authenticated",
	}
}

func TestAuthorizationTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "invalid"} {
		values := validConfigValues()
		values["AUTHORIZATION_TIMEOUT"] = value
		if _, err := Load(mapLookup(values)); err == nil {
			t.Fatalf("accepted timeout %q", value)
		}
	}
	cfg, err := Load(mapLookup(validConfigValues()))
	if err != nil || cfg.AuthorizationTimeout != 2*time.Second {
		t.Fatal("invalid default authorization timeout")
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
