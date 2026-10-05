package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	testSecret   = "test-only-jwt-secret-at-least-32-characters"
	testIssuer   = "http://localhost/auth/v1"
	testAudience = "authenticated"
)

func TestHS256VerifierAcceptsValidHumanToken(t *testing.T) {
	userID := uuid.New()
	verifier := newTestVerifier(t, 30*time.Second)
	token := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(userID))

	principal, err := verifier.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if principal.UserID != userID {
		t.Errorf("UserID = %s, want %s", principal.UserID, userID)
	}
	if principal.Role != "authenticated" {
		t.Errorf("Role = %q, want authenticated", principal.Role)
	}
}

func TestHS256VerifierAcceptsAudienceArrayAndOptionalNBF(t *testing.T) {
	claims := validClaims(uuid.New())
	claims.Audience = jwt.ClaimStrings{"another-audience", testAudience}
	claims.NotBefore = jwt.NewNumericDate(time.Now().Add(-time.Minute))
	token := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)

	if _, err := newTestVerifier(t, 30*time.Second).Verify(context.Background(), token); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestHS256VerifierAppliesClockSkew(t *testing.T) {
	claims := validClaims(uuid.New())
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-10 * time.Second))
	token := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)

	if _, err := newTestVerifier(t, 30*time.Second).Verify(context.Background(), token); err != nil {
		t.Fatalf("Verify() error = %v within clock skew", err)
	}
	if _, err := newTestVerifier(t, 0).Verify(context.Background(), token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken outside clock skew", err)
	}
}

func TestNewHS256VerifierRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*VerifierConfig)
	}{
		{name: "short secret", mutate: func(cfg *VerifierConfig) { cfg.Secret = "too-short" }},
		{name: "relative issuer", mutate: func(cfg *VerifierConfig) { cfg.Issuer = "/auth/v1" }},
		{name: "unsupported issuer scheme", mutate: func(cfg *VerifierConfig) { cfg.Issuer = "ftp://auth.example.com/auth/v1" }},
		{name: "issuer with user info", mutate: func(cfg *VerifierConfig) { cfg.Issuer = "https://user@auth.example.com/auth/v1" }},
		{name: "wrong audience", mutate: func(cfg *VerifierConfig) { cfg.Audience = "service_role" }},
		{name: "negative clock skew", mutate: func(cfg *VerifierConfig) { cfg.ClockSkew = -time.Second }},
		{name: "excessive clock skew", mutate: func(cfg *VerifierConfig) { cfg.ClockSkew = 5*time.Minute + time.Second }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := VerifierConfig{Secret: testSecret, Issuer: testIssuer, Audience: testAudience, ClockSkew: 30 * time.Second}
			tt.mutate(&cfg)
			_, err := NewHS256Verifier(cfg)
			if err == nil {
				t.Fatal("NewHS256Verifier() error = nil")
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Fatal("NewHS256Verifier() error exposed JWT secret")
			}
		})
	}
}

func TestHS256VerifierRejectsInvalidTokens(t *testing.T) {
	now := time.Now()
	userID := uuid.New()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	tests := []struct {
		name  string
		token func(*testing.T) string
	}{
		{name: "wrong signature", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodHS256, []byte("different-secret-at-least-32-characters"), validClaims(userID))
		}},
		{name: "tampered payload", token: func(t *testing.T) string {
			token := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(userID))
			parts := strings.Split(token, ".")
			replacement := "A"
			if strings.HasSuffix(parts[1], replacement) {
				replacement = "B"
			}
			parts[1] = parts[1][:len(parts[1])-1] + replacement
			return strings.Join(parts, ".")
		}},
		{name: "none algorithm", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, validClaims(userID))
		}},
		{name: "HS384 algorithm", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodHS384, []byte(testSecret), validClaims(userID))
		}},
		{name: "HS512 algorithm", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodHS512, []byte(testSecret), validClaims(userID))
		}},
		{name: "RS256 algorithm", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodRS256, rsaKey, validClaims(userID))
		}},
		{name: "missing issuer", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Issuer = ""
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "wrong issuer", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Issuer = "http://localhost/auth/v1/"
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "missing audience", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Audience = nil
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "wrong audience", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Audience = jwt.ClaimStrings{"service_role"}
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "missing expiry", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.ExpiresAt = nil
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "expired", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute))
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "not active", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.NotBefore = jwt.NewNumericDate(now.Add(time.Minute))
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "missing issued at", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.IssuedAt = nil
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "issued in future", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute))
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "missing subject", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Subject = ""
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "invalid subject", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Subject = "not-a-uuid"
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "zero UUID subject", token: func(t *testing.T) string {
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(uuid.Nil))
		}},
		{name: "missing role", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Role = ""
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "anon role", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Role = "anon"
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "service role", token: func(t *testing.T) string {
			claims := validClaims(userID)
			claims.Role = "service_role"
			return signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
		}},
		{name: "wrong segment count", token: func(*testing.T) string { return "only.two" }},
		{name: "invalid base64", token: func(*testing.T) string { return "%%%.%%%.%%%" }},
		{name: "payload is not JSON", token: func(t *testing.T) string {
			return signRawPayload(t, "not-json")
		}},
		{name: "oversized token", token: func(*testing.T) string { return strings.Repeat("x", maxAccessTokenBytes+1) }},
	}

	verifier := newTestVerifier(t, 30*time.Second)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal, err := verifier.Verify(context.Background(), tt.token(t))
			if !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
			}
			if principal != (Principal{}) {
				t.Fatalf("Verify() principal = %+v, want zero value", principal)
			}
		})
	}
}

func TestHS256VerifierRejectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	token := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(uuid.New()))
	_, err := newTestVerifier(t, 30*time.Second).Verify(ctx, token)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify() error = %v, want context.Canceled", err)
	}
}

func newTestVerifier(t *testing.T, skew time.Duration) Verifier {
	t.Helper()
	verifier, err := NewHS256Verifier(VerifierConfig{
		Secret:    testSecret,
		Issuer:    testIssuer,
		Audience:  testAudience,
		ClockSkew: skew,
	})
	if err != nil {
		t.Fatalf("NewHS256Verifier() error = %v", err)
	}
	return verifier
}

func validClaims(userID uuid.UUID) accessTokenClaims {
	now := time.Now()
	return accessTokenClaims{
		Role: "authenticated",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    testIssuer,
			Subject:   userID.String(),
			Audience:  jwt.ClaimStrings{testAudience},
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now.Add(-time.Second)),
		},
	}
}

func signToken(t *testing.T, method jwt.SigningMethod, key any, claims accessTokenClaims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	return signed
}

func signRawPayload(t *testing.T, payload string) string {
	t.Helper()
	token := jwt.New(jwt.SigningMethodHS256)
	unsigned, err := token.SigningString()
	if err != nil {
		t.Fatalf("SigningString() error = %v", err)
	}
	header := strings.Split(unsigned, ".")[0]
	payloadPart := token.EncodeSegment([]byte(payload))
	message := header + "." + payloadPart
	signature, err := jwt.SigningMethodHS256.Sign(message, []byte(testSecret))
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	return message + "." + token.EncodeSegment(signature)
}
