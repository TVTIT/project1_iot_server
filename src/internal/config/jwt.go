package config

import (
	"fmt"
	"net/url"
	"time"
)

func loadJWTMode(lookup LookupFunc, cfg *Config) error {
	cfg.SupabaseJWTMode = valueOrDefault(lookup, "SUPABASE_JWT_MODE", "hs256")
	switch cfg.SupabaseJWTMode {
	case "hs256":
		secret, ok := nonEmpty(lookup, "SUPABASE_JWT_SECRET")
		if !ok {
			return fmt.Errorf("SUPABASE_JWT_SECRET is required")
		}
		if len(secret) < 32 {
			return fmt.Errorf("SUPABASE_JWT_SECRET must be at least 32 characters")
		}
		cfg.SupabaseJWTSecret = secret
		return nil
	case "rs256":
		// Deliberately do not read/copy the unused legacy secret into this mode.
		endpoint, _ := nonEmpty(lookup, "SUPABASE_JWKS_URL")
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
			(u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("SUPABASE_JWKS_URL must be an absolute HTTP(S) URL without credentials, query or fragment")
		}
		cfg.SupabaseJWKSURL = endpoint
		if cfg.JWKSCacheTTL, err = boundedDuration(lookup, "SUPABASE_JWKS_CACHE_TTL", time.Minute, time.Second, time.Hour); err != nil {
			return err
		}
		if cfg.JWKSHTTPTimeout, err = boundedDuration(lookup, "SUPABASE_JWKS_HTTP_TIMEOUT", 2*time.Second, 100*time.Millisecond, 10*time.Second); err != nil {
			return err
		}
		cfg.JWKSRefreshCooldown, err = boundedDuration(lookup, "SUPABASE_JWKS_REFRESH_COOLDOWN", 5*time.Second, 5*time.Second, time.Minute)
		return err
	default:
		return fmt.Errorf("SUPABASE_JWT_MODE must be hs256 or rs256")
	}
}
