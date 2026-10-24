package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func rsaFixture(t *testing.T, kid string) (*rsa.PrivateKey, jwkset.JWKMarshal) {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	j, err := jwkset.NewJWKFromKey(&k.PublicKey, jwkset.JWKOptions{Metadata: jwkset.JWKMetadataOptions{
		KID: kid, ALG: jwkset.AlgRS256, USE: jwkset.UseSig, KEYOPS: []jwkset.KEYOPS{jwkset.KeyOpsVerify},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return k, j.Marshal()
}

func rsaToken(t *testing.T, key *rsa.PrivateKey, kid string, claims accessTokenClaims) string {
	t.Helper()
	v := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if kid != "" {
		v.Header["kid"] = kid
	}
	raw, err := v.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func rsaConfig(url string) RS256Config {
	return RS256Config{VerifierConfig: VerifierConfig{Issuer: testIssuer, Audience: testAudience},
		JWKSURL: url, CacheTTL: time.Minute, HTTPTimeout: time.Second, RefreshCooldown: 5 * time.Second}
}

func TestRS256ContractAndBoundedRefresh(t *testing.T) {
	k1, j1 := rsaFixture(t, "k1")
	k2, j2 := rsaFixture(t, "k2")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{j1, j2}})
	}))
	defer server.Close()
	v, err := NewRS256Verifier(context.Background(), rsaConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	good := validClaims(uuid.New())
	for range 3 {
		if p, err := v.Verify(context.Background(), rsaToken(t, k1, "k1", good)); err != nil || p.UserID.String() != good.Subject {
			t.Fatal(p, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("fetched JWKS on cache hit")
	}
	for name, change := range map[string]func(*accessTokenClaims){
		"issuer":   func(c *accessTokenClaims) { c.Issuer += "/wrong" },
		"audience": func(c *accessTokenClaims) { c.Audience = jwt.ClaimStrings{"anon"} },
		"subject":  func(c *accessTokenClaims) { c.Subject = uuid.Nil.String() },
		"role":     func(c *accessTokenClaims) { c.Role = "service_role" },
		"expired":  func(c *accessTokenClaims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) },
		"iat":      func(c *accessTokenClaims) { c.IssuedAt = nil },
		"nbf":      func(c *accessTokenClaims) { c.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour)) },
	} {
		t.Run(name, func(t *testing.T) {
			c := good
			change(&c)
			if _, err := v.Verify(context.Background(), rsaToken(t, k1, "k1", c)); !errors.Is(err, ErrInvalidToken) {
				t.Fatal(err)
			}
		})
	}
	for _, raw := range []string{rsaToken(t, k2, "k1", good), rsaToken(t, k1, "", good),
		signToken(t, jwt.SigningMethodHS256, []byte(testSecret), good),
		signToken(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, good)} {
		if _, err := v.Verify(context.Background(), raw); !errors.Is(err, ErrInvalidToken) {
			t.Fatal("accepted invalid token", err)
		}
	}
	for range 10 {
		_, _ = v.Verify(context.Background(), rsaToken(t, k1, uuid.NewString(), good))
	}
	if calls.Load() != 2 {
		t.Fatalf("unknown kid refresh not bounded: %d", calls.Load())
	}
}

func TestRS256HardExpiryAndRotation(t *testing.T) {
	k1, j1 := rsaFixture(t, "k1")
	k2, j2 := rsaFixture(t, "k2")
	var phase atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=600")
		switch phase.Load() {
		case 0:
			_ = json.NewEncoder(w).Encode(jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{j1}})
		case 1:
			_ = json.NewEncoder(w).Encode(jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{j1, j2}})
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	var age atomic.Int64
	now := time.Now()
	cfg := rsaConfig(server.URL)
	cfg.now = func() time.Time { return now.Add(time.Duration(age.Load())) }
	v, err := NewRS256Verifier(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	phase.Store(1)
	c := validClaims(uuid.New())
	for _, token := range []string{rsaToken(t, k2, "k2", c), rsaToken(t, k1, "k1", c)} {
		if _, err := v.Verify(context.Background(), token); err != nil {
			t.Fatal("rotation/overlap", err)
		}
	}
	phase.Store(2)
	age.Store(int64(61 * time.Second))
	if _, err := v.Verify(context.Background(), rsaToken(t, k1, "k1", c)); !errors.Is(err, ErrInvalidToken) {
		t.Fatal("stale key survived hard TTL", err)
	}
}

func TestRS256RemovedKeyCannotSurviveSuccessfulRefresh(t *testing.T) {
	k1, j1 := rsaFixture(t, "k1")
	k2, j2 := rsaFixture(t, "k2")
	var removed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		keys := []jwkset.JWKMarshal{j1}
		if removed.Load() {
			keys = []jwkset.JWKMarshal{j2}
		}
		_ = json.NewEncoder(w).Encode(jwkset.JWKSMarshal{Keys: keys})
	}))
	defer server.Close()
	var age atomic.Int64
	now := time.Now()
	cfg := rsaConfig(server.URL)
	cfg.now = func() time.Time { return now.Add(time.Duration(age.Load())) }
	v, err := NewRS256Verifier(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	c := validClaims(uuid.New())
	if _, err := v.Verify(context.Background(), rsaToken(t, k1, "k1", c)); err != nil {
		t.Fatal(err)
	}
	removed.Store(true)
	age.Store(int64(61 * time.Second))
	if _, err := v.Verify(context.Background(), rsaToken(t, k1, "k1", c)); !errors.Is(err, ErrInvalidToken) {
		t.Fatal("removed key was accepted", err)
	}
	if _, err := v.Verify(context.Background(), rsaToken(t, k2, "k2", c)); err != nil {
		t.Fatal("replacement key was not accepted", err)
	}
}

func TestRS256RejectsUnsafeKeySetsAndFetchFailure(t *testing.T) {
	_, public := rsaFixture(t, "k1")
	for name, mutate := range map[string]func(*jwkset.JWKSMarshal){
		"duplicate":   func(s *jwkset.JWKSMarshal) { s.Keys = append(s.Keys, s.Keys[0]) },
		"symmetric":   func(s *jwkset.JWKSMarshal) { s.Keys[0].KTY = jwkset.KtyOct },
		"private":     func(s *jwkset.JWKSMarshal) { s.Keys[0].D = "AQAB" },
		"encrypt":     func(s *jwkset.JWKSMarshal) { s.Keys[0].USE = jwkset.UseEnc },
		"sign":        func(s *jwkset.JWKSMarshal) { s.Keys[0].KEYOPS = []jwkset.KEYOPS{jwkset.KeyOpsSign} },
		"missing kid": func(s *jwkset.JWKSMarshal) { s.Keys[0].KID = "" },
		"x5u":         func(s *jwkset.JWKSMarshal) { s.Keys[0].X5U = "http://untrusted.invalid/key" },
		"empty":       func(s *jwkset.JWKSMarshal) { s.Keys = nil },
	} {
		t.Run(name, func(t *testing.T) {
			s := jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{public}}
			mutate(&s)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(s) }))
			defer server.Close()
			v, err := NewRS256Verifier(context.Background(), rsaConfig(server.URL))
			if err == nil {
				v.Close()
				t.Fatal("unsafe key set accepted")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	if v, err := NewRS256Verifier(context.Background(), rsaConfig(server.URL)); err == nil {
		v.Close()
		t.Fatal("startup accepted unavailable keys")
	}
}
