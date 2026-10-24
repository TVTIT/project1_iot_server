package config

import (
	"testing"
	"time"
)

func TestJWTModeLegacyAndRSAConfig(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil || cfg.SupabaseJWTMode != "hs256" {
		t.Fatal("legacy default", err)
	}
	values["SUPABASE_JWT_MODE"] = "rs256"
	delete(values, "SUPABASE_JWT_SECRET")
	values["SUPABASE_JWKS_URL"] = "http://supabase-envoy:8000/auth/v1/.well-known/jwks.json"
	cfg, err = Load(mapLookup(values))
	if err != nil || cfg.SupabaseJWTSecret != "" || cfg.JWKSCacheTTL != time.Minute || cfg.JWKSHTTPTimeout != 2*time.Second {
		t.Fatal("RSA public-only defaults", err)
	}
	for key, value := range map[string]string{
		"SUPABASE_JWT_MODE": "auto", "SUPABASE_JWKS_URL": "https://user:password@example.test/keys",
		"SUPABASE_JWKS_CACHE_TTL": "0s", "SUPABASE_JWKS_HTTP_TIMEOUT": "1m", "SUPABASE_JWKS_REFRESH_COOLDOWN": "0s",
	} {
		t.Run(key, func(t *testing.T) {
			copyValues := make(map[string]string, len(values))
			for k, v := range values {
				copyValues[k] = v
			}
			copyValues[key] = value
			if _, err := Load(mapLookup(copyValues)); err == nil {
				t.Fatal("accepted invalid RSA config")
			}
		})
	}
}
