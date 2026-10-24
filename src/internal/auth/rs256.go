package auth

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

const maxJWKSBytes = 64 * 1024

// RS256Config pins one operator-controlled JWKS endpoint, never a token URL.
type RS256Config struct {
	VerifierConfig
	JWKSURL         string
	CacheTTL        time.Duration
	HTTPTimeout     time.Duration
	RefreshCooldown time.Duration
	now             func() time.Time
}

// JWKSCacheSnapshot contains only fixed aggregate counters, never keys or kids.
type JWKSCacheSnapshot struct {
	Hits           uint64 `json:"hits"`
	Misses         uint64 `json:"misses"`
	RefreshSuccess uint64 `json:"refresh_success"`
	RefreshFailure uint64 `json:"refresh_failure"`
}

// RS256Verifier reuses the SDK storage/refresh and jwt/v5 parser. The small
// storage policy adds hard freshness; a refresh interval alone is not expiry.
type RS256Verifier struct {
	parser    *jwt.Parser
	keys      keyfunc.Keyfunc
	store     *freshJWKS
	timeout   time.Duration
	cancel    context.CancelFunc
	transport *http.Transport
}

type freshJWKS struct {
	jwkset.Storage
	mu        sync.RWMutex
	updated   time.Time
	ttl       time.Duration
	now       func() time.Time
	hits      atomic.Uint64
	misses    atomic.Uint64
	successes atomic.Uint64
	failures  atomic.Uint64
}

func (s *freshJWKS) KeyReplaceAll(ctx context.Context, keys []jwkset.JWK) error {
	for _, k := range keys {
		p, ok := k.Key().(*rsa.PublicKey)
		if !ok || p.N.BitLen() < 2048 || p.N.BitLen() > 8192 {
			return ErrInvalidToken
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Storage.KeyReplaceAll(ctx, keys); err != nil {
		return err
	}
	s.updated = s.now()
	s.successes.Add(1)
	return nil
}

func (s *freshJWKS) KeyRead(ctx context.Context, kid string) (jwkset.JWK, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.updated.IsZero() || s.now().Sub(s.updated) >= s.ttl {
		s.misses.Add(1)
		return jwkset.JWK{}, jwkset.ErrKeyNotFound
	}
	k, err := s.Storage.KeyRead(ctx, kid)
	if err != nil {
		s.misses.Add(1)
	} else {
		s.hits.Add(1)
	}
	return k, err
}

// The SDK owns JWK decoding. Check bounded JSON metadata before it can process
// private/foreign keys or certificate URLs. Never fetch x5u/jku from key/token.
func validatePublicJWKS(raw []byte) error {
	var set jwkset.JWKSMarshal
	if err := json.Unmarshal(raw, &set); err != nil || len(set.Keys) == 0 || len(set.Keys) > 8 {
		return ErrInvalidToken
	}
	seen := make(map[string]bool, len(set.Keys))
	for _, k := range set.Keys {
		if k.KTY != jwkset.KtyRSA || k.ALG != jwkset.AlgRS256 || k.USE != jwkset.UseSig ||
			len(k.KEYOPS) != 1 || k.KEYOPS[0] != jwkset.KeyOpsVerify ||
			k.KID == "" || len(k.KID) > 128 || seen[k.KID] ||
			k.D != "" || k.P != "" || k.Q != "" || k.DP != "" || k.DQ != "" || k.QI != "" ||
			len(k.OTH) != 0 || k.K != "" || k.X5U != "" || len(k.X5C) != 0 {
			return ErrInvalidToken
		}
		seen[k.KID] = true
	}
	return nil
}

type jwksTransport struct {
	base *http.Transport
	busy chan struct{}
}

func (t *jwksTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case t.busy <- struct{}{}:
		defer func() { <-t.busy }()
	default:
		return nil, ErrInvalidToken
	}
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, ErrInvalidToken
	}
	body := response.Body
	defer func() { _ = body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, ErrInvalidToken
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes+1))
	if err != nil || len(raw) > maxJWKSBytes || validatePublicJWKS(raw) != nil {
		return nil, ErrInvalidToken
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	return response, nil
}

// NewRS256Verifier fails startup unless a bounded, valid public key set can be
// loaded. It never falls back to HS256, even when the JWKS endpoint is down.
func NewRS256Verifier(ctx context.Context, cfg RS256Config) (*RS256Verifier, error) {
	u, err := url.Parse(cfg.JWKSURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Scheme != "http" && u.Scheme != "https") || cfg.CacheTTL < time.Second || cfg.CacheTTL > time.Hour ||
		cfg.HTTPTimeout < 100*time.Millisecond || cfg.HTTPTimeout > 10*time.Second ||
		cfg.RefreshCooldown < 5*time.Second || cfg.RefreshCooldown > time.Minute {
		return nil, fmt.Errorf("invalid JWKS configuration")
	}
	if err := validateTokenContract(cfg.VerifierConfig); err != nil {
		return nil, err
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	ctx, cancel := context.WithCancel(ctx)
	store := &freshJWKS{Storage: jwkset.NewMemoryStorage(), ttl: cfg.CacheTTL, now: cfg.now}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Operator endpoint must not leak through process proxy env.
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 8192
	transport.MaxConnsPerHost = 1
	client := &http.Client{Transport: &jwksTransport{base: transport, busy: make(chan struct{}, 1)},
		Timeout: cfg.HTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("JWKS redirects prohibited") }}
	remote, err := jwkset.NewStorageFromHTTP(cfg.JWKSURL, jwkset.HTTPClientStorageOptions{
		Client: client, Ctx: ctx, HTTPTimeout: cfg.HTTPTimeout,
		Storage: store, RequireSupportedKeys: true, RefreshInterval: cfg.CacheTTL / 2,
		RefreshErrorHandler: func(context.Context, error) { store.failures.Add(1) },
	})
	if err != nil {
		cancel()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("JWKS initial fetch failed")
	}
	storage, err := jwkset.NewHTTPClient(jwkset.HTTPClientOptions{HTTPURLs: map[string]jwkset.Storage{cfg.JWKSURL: remote},
		RefreshUnknownKID: rate.NewLimiter(rate.Every(cfg.RefreshCooldown), 1), RateLimitWaitMax: cfg.HTTPTimeout})
	if err != nil {
		cancel()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("JWKS client initialization failed")
	}
	keys, err := keyfunc.New(keyfunc.Options{Ctx: ctx, Storage: storage, UseWhitelist: []jwkset.USE{jwkset.UseSig}})
	if err != nil {
		cancel()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("JWKS keyfunc initialization failed")
	}
	return &RS256Verifier{keys: keys, store: store, timeout: cfg.HTTPTimeout, cancel: cancel, transport: transport,
		parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(),
			jwt.WithLeeway(cfg.ClockSkew), jwt.WithStrictDecoding())}, nil
}

// Verify checks the pinned RS256 user-token contract against fresh public keys.
func (v *RS256Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	if raw == "" || len(raw) > maxAccessTokenBytes {
		return Principal{}, ErrInvalidToken
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	claims := &accessTokenClaims{}
	token, err := v.parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		if t.Method != jwt.SigningMethodRS256 || !ok || kid == "" || len(kid) > 128 {
			return nil, ErrInvalidToken
		}
		return v.keys.KeyfuncCtx(ctx)(t)
	})
	if err != nil || token == nil || !token.Valid || claims.IssuedAt == nil || claims.Role != "authenticated" || ctx.Err() != nil {
		return Principal{}, ErrInvalidToken
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil || id == uuid.Nil {
		return Principal{}, ErrInvalidToken
	}
	return Principal{UserID: id, Role: claims.Role}, nil
}

// Close stops SDK refresh and pending HTTP work, then releases idle connections.
func (v *RS256Verifier) Close() {
	v.cancel()
	v.transport.CloseIdleConnections()
}

// CacheSnapshot reads fixed counters without exposing key or token material.
func (v *RS256Verifier) CacheSnapshot() JWKSCacheSnapshot {
	return JWKSCacheSnapshot{v.store.hits.Load(), v.store.misses.Load(), v.store.successes.Load(), v.store.failures.Load()}
}
