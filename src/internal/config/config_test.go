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

func TestMediaUploadFlagOptIn(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MediaUploadEnabled {
		t.Fatalf("default media flag=%v err=%v", cfg.MediaUploadEnabled, err)
	}
	values["MEDIA_UPLOAD_ENABLED"] = "true"
	addMediaRuntimeSettings(values)
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.MediaUploadEnabled || !cfg.Credential.MediaUploadEnabled {
		t.Fatalf("enabled media flag=%v credential=%v err=%v", cfg.MediaUploadEnabled, cfg.Credential.MediaUploadEnabled, err)
	}
	values["MEDIA_UPLOAD_ENABLED"] = "yes"
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("invalid media flag accepted")
	}
}

func TestMediaPlaybackConfigValidation(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MediaPlaybackEnabled {
		t.Fatalf("default media playback flag=%v err=%v", cfg.MediaPlaybackEnabled, err)
	}

	// Playback enabled with uploads disabled (no MQTT or upload origin needed)
	values["MEDIA_PLAYBACK_ENABLED"] = "true"
	values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.MediaPlaybackEnabled || cfg.MediaUploadEnabled {
		t.Fatalf("enabled playback flag=%v uploads=%v err=%v", cfg.MediaPlaybackEnabled, cfg.MediaUploadEnabled, err)
	}
	if cfg.MediaPlayback.PublicOrigin != "https://playback.example.test" {
		t.Fatalf("unexpected playback origin: %s", cfg.MediaPlayback.PublicOrigin)
	}
	if cfg.MediaPlayback.StorageServiceToken != "playback-storage-token" {
		t.Fatalf("unexpected storage service token: %s", cfg.MediaPlayback.StorageServiceToken)
	}
	if cfg.MediaPlayback.String() != "MediaPlaybackConfig{redacted}" || cfg.MediaPlayback.GoString() != "MediaPlaybackConfig{redacted}" {
		t.Fatalf("playback config must be redacted")
	}

	// Invalid playback flag
	badValues := validConfigValues()
	badValues["MEDIA_PLAYBACK_ENABLED"] = "not-a-bool"
	if _, err := Load(mapLookup(badValues)); err == nil {
		t.Fatal("invalid playback flag accepted")
	}

	// Missing public origin
	badOrigin := validConfigValues()
	badOrigin["MEDIA_PLAYBACK_ENABLED"] = "true"
	badOrigin["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "key"
	if _, err := Load(mapLookup(badOrigin)); err == nil {
		t.Fatal("missing playback public origin accepted")
	}

	// Non-HTTPS origin
	badOrigin["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "http://insecure.test"
	if _, err := Load(mapLookup(badOrigin)); err == nil {
		t.Fatal("HTTP playback public origin accepted")
	}

	// Origin with path or fragment
	badOrigin["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.test/path"
	if _, err := Load(mapLookup(badOrigin)); err == nil {
		t.Fatal("playback origin with path accepted")
	}
	badOrigin["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.test#frag"
	if _, err := Load(mapLookup(badOrigin)); err == nil {
		t.Fatal("playback origin with fragment accepted")
	}

	// Missing playback storage token
	badKey := validConfigValues()
	badKey["MEDIA_PLAYBACK_ENABLED"] = "true"
	badKey["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	badKey["MEDIA_PLAYBACK_STORAGE_TOKEN"] = ""
	if _, err := Load(mapLookup(badKey)); err == nil {
		t.Fatal("missing playback storage token accepted")
	}
}

func TestMediaPlaybackEnabledFlagExactCanonical(t *testing.T) {
	// Missing flag defaults to false
	values := validConfigValues()
	delete(values, "MEDIA_PLAYBACK_ENABLED")
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MediaPlaybackEnabled {
		t.Fatalf("expected missing MEDIA_PLAYBACK_ENABLED to default to false, got %v, err=%v", cfg.MediaPlaybackEnabled, err)
	}

	// Exact "false" is valid and false
	values["MEDIA_PLAYBACK_ENABLED"] = "false"
	cfg, err = Load(mapLookup(values))
	if err != nil || cfg.MediaPlaybackEnabled {
		t.Fatalf("expected MEDIA_PLAYBACK_ENABLED=false to be false, got %v, err=%v", cfg.MediaPlaybackEnabled, err)
	}

	// Exact "true" is valid and true
	values["MEDIA_PLAYBACK_ENABLED"] = "true"
	values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.MediaPlaybackEnabled {
		t.Fatalf("expected MEDIA_PLAYBACK_ENABLED=true to be true, got %v, err=%v", cfg.MediaPlaybackEnabled, err)
	}

	// Noncanonical values must be rejected: TRUE, True, 1, 0, " true", "true ", "yes", "no", FALSE, False
	invalidFlags := []string{"TRUE", "True", "1", "0", " true", "true ", "yes", "no", "FALSE", "False"}
	for _, invalid := range invalidFlags {
		t.Run("reject_"+invalid, func(t *testing.T) {
			bad := validConfigValues()
			bad["MEDIA_PLAYBACK_ENABLED"] = invalid
			bad["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
			bad["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
			if _, err := Load(mapLookup(bad)); err == nil {
				t.Fatalf("expected noncanonical MEDIA_PLAYBACK_ENABLED=%q to be rejected", invalid)
			}
		})
	}
}

func TestMediaPlaybackHTTPSOriginPorts(t *testing.T) {
	validOrigins := []string{
		"https://playback.example.test",
		"https://playback.example.test:443",
		"https://playback.example.test:8443",
		"https://playback.example.test:1",
		"https://playback.example.test:65535",
	}
	for _, origin := range validOrigins {
		t.Run("accept_"+origin, func(t *testing.T) {
			values := validConfigValues()
			values["MEDIA_PLAYBACK_ENABLED"] = "true"
			values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = origin
			values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
			cfg, err := Load(mapLookup(values))
			if err != nil || cfg.MediaPlayback.PublicOrigin != origin {
				t.Fatalf("expected valid playback origin %q to be accepted, err=%v", origin, err)
			}
		})
	}

	invalidOrigins := []string{
		"https://playback.example.test:0",
		"https://playback.example.test:65536",
		"https://playback.example.test:",
		"https://playback.example.test:-1",
		"https://playback.example.test:abc",
		"https://playback.example.test:080",
		"http://playback.example.test:8443",
		"https://playback.example.test:8443/path",
		"https://playback.example.test:8443#frag",
		"https://user:pass@playback.example.test:8443",
	}
	for _, origin := range invalidOrigins {
		t.Run("reject_"+origin, func(t *testing.T) {
			bad := validConfigValues()
			bad["MEDIA_PLAYBACK_ENABLED"] = "true"
			bad["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = origin
			bad["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
			if _, err := Load(mapLookup(bad)); err == nil {
				t.Fatalf("expected invalid playback origin %q to be rejected", origin)
			}
		})
	}
}

func TestMediaPlaybackTimeoutBudgetValidation(t *testing.T) {
	// 1. Current valid defaults with HS256: 30s HTTP_WRITE > 0s authIO + 2s auth + 5s sign = 7s
	values := validConfigValues()
	values["MEDIA_PLAYBACK_ENABLED"] = "true"
	values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	if _, err := Load(mapLookup(values)); err != nil {
		t.Fatalf("valid defaults should pass budget validation, got %v", err)
	}

	// 2. Exact-boundary budget: HTTP_WRITE_TIMEOUT == authIO + auth + sign (7s) leaves no margin
	boundaryValues := validConfigValues()
	boundaryValues["MEDIA_PLAYBACK_ENABLED"] = "true"
	boundaryValues["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	boundaryValues["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	boundaryValues["AUTHORIZATION_TIMEOUT"] = "2s"
	boundaryValues["MEDIA_PLAYBACK_SIGN_TIMEOUT"] = "5s"
	boundaryValues["HTTP_WRITE_TIMEOUT"] = "7s"
	if _, err := Load(mapLookup(boundaryValues)); err == nil {
		t.Fatal("exact-boundary budget (7s <= 2s + 5s) must be rejected")
	}

	// 3. Excessive budget: HTTP_WRITE_TIMEOUT < authIO + auth + sign (6s < 7s)
	excessiveValues := validConfigValues()
	excessiveValues["MEDIA_PLAYBACK_ENABLED"] = "true"
	excessiveValues["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	excessiveValues["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	excessiveValues["AUTHORIZATION_TIMEOUT"] = "2s"
	excessiveValues["MEDIA_PLAYBACK_SIGN_TIMEOUT"] = "5s"
	excessiveValues["HTTP_WRITE_TIMEOUT"] = "6s"
	if _, err := Load(mapLookup(excessiveValues)); err == nil {
		t.Fatal("excessive budget (6s < 2s + 5s) must be rejected")
	}

	// 4. Exact-boundary with RS256: authIO is SUPABASE_JWKS_HTTP_TIMEOUT (2s)
	rs256Boundary := validConfigValues()
	rs256Boundary["SUPABASE_JWT_MODE"] = "rs256"
	rs256Boundary["SUPABASE_JWKS_URL"] = "http://envoy:8000/auth/v1/.well-known/jwks.json"
	rs256Boundary["SUPABASE_JWKS_HTTP_TIMEOUT"] = "2s"
	rs256Boundary["MEDIA_PLAYBACK_ENABLED"] = "true"
	rs256Boundary["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	rs256Boundary["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	rs256Boundary["AUTHORIZATION_TIMEOUT"] = "2s"
	rs256Boundary["MEDIA_PLAYBACK_SIGN_TIMEOUT"] = "5s"
	// Total ops = 2s (JWKS) + 2s (auth) + 5s (sign) = 9s
	rs256Boundary["HTTP_WRITE_TIMEOUT"] = "9s"
	if _, err := Load(mapLookup(rs256Boundary)); err == nil {
		t.Fatal("exact-boundary RS256 budget (9s <= 2s + 2s + 5s) must be rejected")
	}

	// 5. Margin > 0 passes: 9s + 1ms passes
	rs256Valid := validConfigValues()
	rs256Valid["SUPABASE_JWT_MODE"] = "rs256"
	rs256Valid["SUPABASE_JWKS_URL"] = "http://envoy:8000/auth/v1/.well-known/jwks.json"
	rs256Valid["SUPABASE_JWKS_HTTP_TIMEOUT"] = "2s"
	rs256Valid["MEDIA_PLAYBACK_ENABLED"] = "true"
	rs256Valid["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	rs256Valid["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "playback-storage-token"
	rs256Valid["AUTHORIZATION_TIMEOUT"] = "2s"
	rs256Valid["MEDIA_PLAYBACK_SIGN_TIMEOUT"] = "5s"
	rs256Valid["HTTP_WRITE_TIMEOUT"] = "10s"
	if _, err := Load(mapLookup(rs256Valid)); err != nil {
		t.Fatalf("valid RS256 budget (10s > 9s) should pass, got %v", err)
	}

	// 6. Playback-off is unaffected by low HTTP_WRITE_TIMEOUT
	offValues := validConfigValues()
	offValues["MEDIA_PLAYBACK_ENABLED"] = "false"
	offValues["AUTHORIZATION_TIMEOUT"] = "2s"
	offValues["MEDIA_PLAYBACK_SIGN_TIMEOUT"] = "5s"
	offValues["HTTP_WRITE_TIMEOUT"] = "6s"
	if _, err := Load(mapLookup(offValues)); err != nil {
		t.Fatalf("playback-off must not be affected by playback budget checks, got %v", err)
	}
}

func TestMediaPlaybackIndependentCredentialNoServiceRoleFallback(t *testing.T) {
	// 1. Playback enabled with only SERVICE_ROLE_KEY must fail
	onlyServiceRole := validConfigValues()
	onlyServiceRole["MEDIA_PLAYBACK_ENABLED"] = "true"
	onlyServiceRole["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	onlyServiceRole["SERVICE_ROLE_KEY"] = "my-service-role-key"
	delete(onlyServiceRole, "MEDIA_PLAYBACK_STORAGE_TOKEN")
	if _, err := Load(mapLookup(onlyServiceRole)); err == nil {
		t.Fatal("playback enabled with only SERVICE_ROLE_KEY must be rejected (no fallback allowed)")
	}

	// 2. Playback enabled with MEDIA_PLAYBACK_STORAGE_TOKEN succeeds independently
	independentToken := validConfigValues()
	independentToken["MEDIA_PLAYBACK_ENABLED"] = "true"
	independentToken["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"
	independentToken["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "my-playback-token"
	delete(independentToken, "SERVICE_ROLE_KEY")
	cfg, err := Load(mapLookup(independentToken))
	if err != nil {
		t.Fatalf("expected MEDIA_PLAYBACK_STORAGE_TOKEN to work independently, got err: %v", err)
	}
	if cfg.MediaPlayback.StorageServiceToken != "my-playback-token" {
		t.Fatalf("expected StorageServiceToken %q, got %q", "my-playback-token", cfg.MediaPlayback.StorageServiceToken)
	}
}

func TestMediaPlaybackStorageTokenRoleValidation(t *testing.T) {
	// A JWT with role "service_role" must be rejected
	serviceRoleJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.sig"
	anonJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiJ9.sig"
	authJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYXV0aGVudGljYXRlZCJ9.sig"
	playbackSignerJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic3RvcmFnZV9wbGF5YmFja19zaWduZXIifQ.sig"

	values := validConfigValues()
	values["MEDIA_PLAYBACK_ENABLED"] = "true"
	values["MEDIA_PLAYBACK_PUBLIC_ORIGIN"] = "https://playback.example.test"

	// 1. service_role JWT must fail
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = serviceRoleJWT
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("expected Load to reject MEDIA_PLAYBACK_STORAGE_TOKEN with service_role")
	}

	// 2. anon JWT must fail
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = anonJWT
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("expected Load to reject MEDIA_PLAYBACK_STORAGE_TOKEN with anon role")
	}

	// 3. authenticated JWT must fail
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = authJWT
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("expected Load to reject MEDIA_PLAYBACK_STORAGE_TOKEN with authenticated role")
	}

	// 4. storage_playback_signer JWT must succeed
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = playbackSignerJWT
	if _, err := Load(mapLookup(values)); err != nil {
		t.Fatalf("expected Load to accept MEDIA_PLAYBACK_STORAGE_TOKEN with storage_playback_signer role, got: %v", err)
	}

	// 5. non-JWT token (opaque string) passes config validation (Storage validates signature/format)
	values["MEDIA_PLAYBACK_STORAGE_TOKEN"] = "opaque-token-string"
	if _, err := Load(mapLookup(values)); err != nil {
		t.Fatalf("expected Load to accept opaque token string, got: %v", err)
	}
}

func TestMediaUploadLimitsDefaultsAndValidation(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.MediaUpload
	if cfg.MediaUploadEnabled || m.MaxUploadBytes != 80_000_000 || m.MaxPendingPerGateway != 1 ||
		m.MaxUnsettledIntents != 8 || m.TemporaryBytes != 640_000_000 || m.StoredBytes < m.MaxUploadBytes ||
		m.IntentTTL <= 0 || m.DBTimeout <= 0 || m.StorageTimeout <= 0 ||
		m.VerifyWorkers < 1 || m.VerifyBatchSize < 1 || m.VerifyMaxAttempts < 1 ||
		m.CleanupWorkers < 1 || m.CleanupBatchSize < 1 || m.CleanupMaxAttempts < 1 {
		t.Fatalf("unexpected media defaults: enabled=%v config=%+v", cfg.MediaUploadEnabled, m)
	}

	values["MEDIA_UPLOAD_ENABLED"] = "true"
	addMediaRuntimeSettings(values)
	values["MEDIA_UPLOAD_MAX_PENDING_PER_GATEWAY"] = "2"
	cfg, err = Load(mapLookup(values))
	if err != nil || !cfg.MediaUploadEnabled || cfg.MediaUpload.MaxPendingPerGateway != 2 {
		t.Fatalf("media opt-in config=%+v enabled=%v err=%v", cfg.MediaUpload, cfg.MediaUploadEnabled, err)
	}

	for key, value := range map[string]string{
		"MEDIA_UPLOAD_MAX_PENDING_PER_GATEWAY": "9",
		"MEDIA_UPLOAD_MAX_UNSETTLED_INTENTS":   "0",
		"MEDIA_UPLOAD_TEMPORARY_BYTES":         "639999999",
		"MEDIA_UPLOAD_STORED_BYTES":            "79999999",
		"MEDIA_UPLOAD_INTENT_TTL":              "0s",
		"MEDIA_UPLOAD_DB_TIMEOUT":              "soon",
		"MEDIA_UPLOAD_VERIFY_WORKERS":          "0",
		"MEDIA_UPLOAD_VERIFY_BATCH_SIZE":       "1001",
		"MEDIA_UPLOAD_VERIFY_RETRY_MIN_DELAY":  "2m",
		"MEDIA_UPLOAD_CLEANUP_RETRY_MIN_DELAY": "6m",
	} {
		bad := validConfigValues()
		bad[key] = value
		if _, err := Load(mapLookup(bad)); err == nil {
			t.Errorf("accepted %s=%q", key, value)
		}
	}
}

func TestMediaUploadCleanupMaxAttemptsValidation(t *testing.T) {
	// Attempt limit 1 must be rejected because cleanup requires >= 2 attempts (initial delete + followup confirmation)
	values := validConfigValues()
	values["MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS"] = "1"
	if _, err := Load(mapLookup(values)); err == nil {
		t.Fatal("MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS=1 must be rejected")
	}

	// Attempt limit 2 must be accepted
	values["MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS"] = "2"
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.MediaUpload.CleanupMaxAttempts != 2 {
		t.Fatalf("MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS=2 failed: cfg=%+v err=%v", cfg.MediaUpload, err)
	}
}

func addMediaRuntimeSettings(values map[string]string) {
	values["MQTT_BROKER_URL"] = "ssl://mosquitto:8883"
	values["MEDIA_UPLOAD_MQTT_CLIENT_ID"] = "backend_media"
	values["MQTT_USERNAME"] = "backend_service"
	values["MQTT_PASSWORD"] = "data-password"
	values["MQTT_TLS_CA_FILE"] = "/certs/ca.pem"
	values["MQTT_DYNSEC_MANAGER_USERNAME"] = "backend_manager"
	values["MQTT_DYNSEC_MANAGER_PASSWORD"] = "manager-password"
	values["MQTT_INTERNAL_MANAGEMENT_URL"] = "ssl://mosquitto:8883"
	values["STORAGE_INTERNAL_URL"] = "http://supabase-storage:5000"
	values["MEDIA_UPLOAD_PUBLIC_ORIGIN"] = "https://upload.example.test"
	values["SERVICE_ROLE_KEY"] = "fixture-service-token"
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
