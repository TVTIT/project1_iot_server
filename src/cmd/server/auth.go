package main

import (
	"context"
	"fmt"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
)

func composeTokenVerifier(ctx context.Context, cfg config.Config) (auth.Verifier, func(), error) {
	contract := auth.VerifierConfig{Secret: cfg.SupabaseJWTSecret, Issuer: cfg.SupabaseJWTIssuer,
		Audience: cfg.SupabaseJWTAudience, ClockSkew: cfg.SupabaseJWTClockSkew}
	switch cfg.SupabaseJWTMode {
	case "hs256":
		v, err := auth.NewHS256Verifier(contract)
		return v, func() {}, err
	case "rs256":
		v, err := auth.NewRS256Verifier(ctx, auth.RS256Config{VerifierConfig: contract,
			JWKSURL: cfg.SupabaseJWKSURL, CacheTTL: cfg.JWKSCacheTTL,
			HTTPTimeout: cfg.JWKSHTTPTimeout, RefreshCooldown: cfg.JWKSRefreshCooldown})
		if err != nil {
			return nil, func() {}, err
		}
		return v, v.Close, nil
	default:
		return nil, func() {}, fmt.Errorf("unsupported JWT mode")
	}
}
