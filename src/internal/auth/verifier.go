package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const maxAccessTokenBytes = 16 * 1024

// ErrInvalidToken is returned for every externally supplied token failure.
// Parser details are intentionally not exposed to callers.
var ErrInvalidToken = errors.New("invalid access token")

// Verifier validates an access token and returns a typed human principal.
type Verifier interface {
	Verify(context.Context, string) (Principal, error)
}

// VerifierConfig contains the pinned HS256 token contract.
type VerifierConfig struct {
	Secret    string
	Issuer    string
	Audience  string
	ClockSkew time.Duration
}

type accessTokenClaims struct {
	Role string `json:"role"`
	jwt.RegisteredClaims
}

type hs256Verifier struct {
	secret []byte
	parser *jwt.Parser
}

// NewHS256Verifier creates a verifier that accepts only GoTrue human-user
// access tokens matching the configured issuer and audience.
func NewHS256Verifier(cfg VerifierConfig) (Verifier, error) {
	if len(strings.TrimSpace(cfg.Secret)) < 32 {
		return nil, fmt.Errorf("JWT secret must be at least 32 characters")
	}
	issuer, err := url.Parse(cfg.Issuer)
	if err != nil || !issuer.IsAbs() || (issuer.Scheme != "http" && issuer.Scheme != "https") || issuer.Host == "" || issuer.User != nil {
		return nil, fmt.Errorf("JWT issuer must be an absolute HTTP(S) URL without user information")
	}
	if cfg.Audience != "authenticated" {
		return nil, fmt.Errorf("JWT audience must be authenticated")
	}
	if cfg.ClockSkew < 0 || cfg.ClockSkew > 5*time.Minute {
		return nil, fmt.Errorf("JWT clock skew must be between 0s and 5m0s")
	}

	secret := append([]byte(nil), cfg.Secret...)
	return &hs256Verifier{
		secret: secret,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
			jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(cfg.ClockSkew),
			jwt.WithStrictDecoding(),
		),
	}, nil
}

func (v *hs256Verifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	if rawToken == "" || len(rawToken) > maxAccessTokenBytes {
		return Principal{}, ErrInvalidToken
	}

	claims := &accessTokenClaims{}
	token, err := v.parser.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return v.secret, nil
	})
	if err != nil || token == nil || !token.Valid {
		return Principal{}, ErrInvalidToken
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	if claims.IssuedAt == nil || claims.Role != "authenticated" {
		return Principal{}, ErrInvalidToken
	}

	userID, err := uuid.Parse(claims.Subject)
	if err != nil || userID == uuid.Nil {
		return Principal{}, ErrInvalidToken
	}

	return Principal{UserID: userID, Role: claims.Role}, nil
}
