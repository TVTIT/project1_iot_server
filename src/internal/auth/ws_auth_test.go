package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"iot-platform/internal/httpapi"
)

func TestWSAuthenticationMiddlewareTransportsAndRejectsAmbiguity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	token := "opaque.secret.token"
	valid := func(headers http.Header) bool {
		router := gin.New()
		router.Use(httpapi.RequestIDMiddleware())
		router.GET("/ws", WSAuthenticationMiddleware(verifierFunc(func(_ context.Context, got string) (Principal, error) {
			if got != token {
				t.Fatalf("verified wrong credential")
			}
			return Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		})), func(c *gin.Context) { c.Status(http.StatusNoContent) })
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		req.Header = make(http.Header)
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		return res.Code == http.StatusNoContent
	}
	for _, headers := range []http.Header{
		{"Authorization": {"Bearer " + token}, "Sec-WebSocket-Protocol": {"iot.telemetry.v1"}},
		{"Sec-WebSocket-Protocol": {"iot.telemetry.v1, bearer." + token}},
		{"Sec-WebSocket-Protocol": {"iot.telemetry.v1", "bearer." + token}}, // actual repeated HTTP fields
	} {
		if !valid(headers) {
			t.Fatalf("valid credential representation rejected: %#v", headers)
		}
	}
	for name, headers := range map[string]http.Header{
		"missing app":                    {"Authorization": {"Bearer " + token}},
		"both transports":                {"Authorization": {"Bearer " + token}, "Sec-WebSocket-Protocol": {"iot.telemetry.v1, bearer." + token}},
		"duplicate authorization fields": {"Authorization": {"Bearer " + token, "Bearer " + token}, "Sec-WebSocket-Protocol": {"iot.telemetry.v1"}},
		"duplicate app":                  {"Sec-WebSocket-Protocol": {"iot.telemetry.v1, iot.telemetry.v1"}},
		"multiple credential protocols":  {"Sec-WebSocket-Protocol": {"iot.telemetry.v1, bearer.one, bearer.two"}},
		"unexpected protocol":            {"Sec-WebSocket-Protocol": {"iot.telemetry.v1, other"}},
		"malformed bearer":               {"Authorization": {"Bearer  " + token}, "Sec-WebSocket-Protocol": {"iot.telemetry.v1"}},
		"query token":                    {"Sec-WebSocket-Protocol": {"iot.telemetry.v1"}},
		"control character":              {"Sec-WebSocket-Protocol": {"iot.telemetry.v1, bearer.a\rb"}},
		"oversized token":                {"Sec-WebSocket-Protocol": {"iot.telemetry.v1, bearer." + strings.Repeat("a", 8193)}},
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			r := gin.New()
			r.Use(httpapi.RequestIDMiddleware())
			r.GET("/ws", WSAuthenticationMiddleware(verifierFunc(func(context.Context, string) (Principal, error) { called = true; return Principal{}, nil })), func(c *gin.Context) { c.Status(204) })
			req := httptest.NewRequest("GET", "/ws?access_token="+token, nil)
			req.Header = make(http.Header)
			for key, values := range headers {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
			res := httptest.NewRecorder()
			r.ServeHTTP(res, req)
			if res.Code != 401 || called {
				t.Fatalf("status=%d verifierCalled=%v", res.Code, called)
			}
			if strings.Contains(res.Body.String(), token) {
				t.Fatal("credential echoed")
			}
		})
	}
}

func TestWSAuthenticationMiddlewareCountsDenialSeparatelyFromRESTAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics := &Metrics{}
	router := gin.New()
	router.GET("/ws", WSAuthenticationMiddleware(verifierFunc(func(context.Context, string) (Principal, error) { return Principal{}, errors.New("invalid") }), metrics), func(c *gin.Context) { c.Status(http.StatusNoContent) })
	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/ws", nil))
	snapshot := metrics.Snapshot()
	if res.Code != http.StatusUnauthorized || snapshot.WebSocketDeny != 1 || snapshot.Deny != 0 {
		t.Fatalf("status=%d auth metrics=%+v", res.Code, snapshot)
	}
}

func TestWSCredentialTransportsUseHS256AndRS256Verifiers(t *testing.T) {
	userID := uuid.New()
	claims := validClaims(userID)
	key, jwk := rsaFixture(t, "ws-key")
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{jwk}})
	}))
	defer jwks.Close()
	rsVerifier, err := NewRS256Verifier(context.Background(), rsaConfig(jwks.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer rsVerifier.Close()
	hsVerifier := newTestVerifier(t, 0)
	hsToken := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims)
	rsToken := rsaToken(t, key, "ws-key", claims)
	for _, tc := range []struct {
		name, token string
		verifier    Verifier
		transport   string
	}{
		{"native HS256", hsToken, hsVerifier, "native"},
		{"browser HS256", hsToken, hsVerifier, "browser"},
		{"native RS256", rsToken, rsVerifier, "native"},
		{"browser RS256", rsToken, rsVerifier, "browser"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/ws", WSAuthenticationMiddleware(tc.verifier), func(c *gin.Context) {
				p, ok := PrincipalFrom(c)
				if !ok || p.UserID != userID || p.Role != "authenticated" || !p.ExpiresAt.Equal(claims.ExpiresAt.Time) {
					t.Errorf("verified principal=%+v ok=%v", p, ok)
				}
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequest("GET", "/ws", nil)
			if tc.transport == "native" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
				req.Header.Add("Sec-WebSocket-Protocol", WSTelemetryProtocol)
			} else {
				req.Header.Add("Sec-WebSocket-Protocol", WSTelemetryProtocol+", bearer."+tc.token)
			}
			res := httptest.NewRecorder()
			router.ServeHTTP(res, req)
			if res.Code != http.StatusNoContent {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
		})
	}
	for _, tc := range []struct {
		name     string
		raw      string
		verifier Verifier
	}{
		{"expired", signToken(t, jwt.SigningMethodHS256, []byte(testSecret), func() accessTokenClaims {
			c := validClaims(userID)
			c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
			return c
		}()), newTestVerifier(t, 0)},
		{"wrong algorithm", signToken(t, jwt.SigningMethodHS512, []byte(testSecret), claims), newTestVerifier(t, 0)},
		{"wrong verifier algorithm", signToken(t, jwt.SigningMethodHS256, []byte(testSecret), claims), rsVerifier},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/ws", WSAuthenticationMiddleware(tc.verifier), func(c *gin.Context) { c.Status(204) })
			req := httptest.NewRequest("GET", "/ws", nil)
			req.Header.Set("Sec-WebSocket-Protocol", WSTelemetryProtocol+", bearer."+tc.raw)
			res := httptest.NewRecorder()
			r.ServeHTTP(res, req)
			if res.Code != 401 || strings.Contains(res.Body.String(), tc.raw) {
				t.Fatalf("status=%d leaked=%v", res.Code, strings.Contains(res.Body.String(), tc.raw))
			}
		})
	}
}

func TestWSInternalVerificationRouteAndRESTIsolation(t *testing.T) {
	v := newTestVerifier(t, 0)
	h := NewInternalAuthHandler(v, nil, nil, true)
	for _, tc := range []struct {
		path, protocol string
		want           int
	}{
		{"/internal/auth/verify/ws", "iot.telemetry.v1, bearer.invalid", 401},
		{"/internal/auth/verify", "iot.telemetry.v1, bearer.valid", 401},
		{"/internal/auth/verify/ws?access_token=token", "iot.telemetry.v1", 401},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		r.Header.Set("Sec-WebSocket-Protocol", tc.protocol)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s status=%d want=%d", tc.path, w.Code, tc.want)
		}
		if strings.Contains(w.Body.String(), "invalid") || strings.Contains(w.Body.String(), "token") {
			t.Fatal("internal error echoed credential")
		}
	}
	valid := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(uuid.New()))
	for _, path := range []string{"/internal/auth/verify/ws", "/internal/auth/verify"} {
		r := httptest.NewRequest("GET", path, nil)
		if path == "/internal/auth/verify/ws" {
			r.Header.Set("Sec-WebSocket-Protocol", WSTelemetryProtocol+", bearer."+valid)
		} else {
			r.Header.Set("Authorization", "Bearer "+valid)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body.String())
		}
	}
}

func TestP2DisabledStubBearerPerimeterRouteAndEnabledIsolation(t *testing.T) {
	v := newTestVerifier(t, 0)
	valid := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(uuid.New()))

	// Disabled mode (P2 default: WEBSOCKET_ENABLED=false):
	// Perimeter route /internal/auth/verify/ws falls back to REST Bearer verification
	// so that /v1/ws with Bearer reaches the 501 stub without requiring WS subprotocols.
	disabledHandler := NewInternalAuthHandler(v, nil, nil, false)

	// Valid Bearer-only probe (actual P2 test shape) -> 204 No Content.
	for _, path := range []string{"/internal/auth/verify", "/internal/auth/verify/ws"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+valid)
		rec := httptest.NewRecorder()
		disabledHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("disabled mode %s with Bearer: got %d want %d", path, rec.Code, http.StatusNoContent)
		}
	}

	// Missing or invalid Bearer in disabled mode -> 401 Unauthorized.
	for _, path := range []string{"/internal/auth/verify", "/internal/auth/verify/ws"} {
		reqNoAuth := httptest.NewRequest("GET", path, nil)
		recNoAuth := httptest.NewRecorder()
		disabledHandler.ServeHTTP(recNoAuth, reqNoAuth)
		if recNoAuth.Code != http.StatusUnauthorized {
			t.Fatalf("disabled mode %s without token: got %d want %d", path, recNoAuth.Code, http.StatusUnauthorized)
		}

		reqBadAuth := httptest.NewRequest("GET", path, nil)
		reqBadAuth.Header.Set("Authorization", "Bearer invalid.token")
		recBadAuth := httptest.NewRecorder()
		disabledHandler.ServeHTTP(recBadAuth, reqBadAuth)
		if recBadAuth.Code != http.StatusUnauthorized {
			t.Fatalf("disabled mode %s with bad token: got %d want %d", path, recBadAuth.Code, http.StatusUnauthorized)
		}
	}

	// Enabled mode (WEBSOCKET_ENABLED=true):
	// /internal/auth/verify/ws strictly requires WS credentials; Bearer-only without app protocol is denied.
	enabledHandler := NewInternalAuthHandler(v, nil, nil, true)

	reqBearerOnly := httptest.NewRequest("GET", "/internal/auth/verify/ws", nil)
	reqBearerOnly.Header.Set("Authorization", "Bearer "+valid)
	recBearerOnly := httptest.NewRecorder()
	enabledHandler.ServeHTTP(recBearerOnly, reqBearerOnly)
	if recBearerOnly.Code != http.StatusUnauthorized {
		t.Fatalf("enabled mode /internal/auth/verify/ws with Bearer-only (no WS protocol): got %d want %d", recBearerOnly.Code, http.StatusUnauthorized)
	}

	// Enabled mode accepts native transport (Bearer + app subprotocol) -> 204.
	reqNative := httptest.NewRequest("GET", "/internal/auth/verify/ws", nil)
	reqNative.Header.Set("Authorization", "Bearer "+valid)
	reqNative.Header.Set("Sec-WebSocket-Protocol", WSTelemetryProtocol)
	recNative := httptest.NewRecorder()
	enabledHandler.ServeHTTP(recNative, reqNative)
	if recNative.Code != http.StatusNoContent {
		t.Fatalf("enabled mode /internal/auth/verify/ws native: got %d want %d", recNative.Code, http.StatusNoContent)
	}

	// Enabled mode accepts browser transport (app subprotocol + bearer subprotocol) -> 204.
	reqBrowser := httptest.NewRequest("GET", "/internal/auth/verify/ws", nil)
	reqBrowser.Header.Set("Sec-WebSocket-Protocol", WSTelemetryProtocol+", bearer."+valid)
	recBrowser := httptest.NewRecorder()
	enabledHandler.ServeHTTP(recBrowser, reqBrowser)
	if recBrowser.Code != http.StatusNoContent {
		t.Fatalf("enabled mode /internal/auth/verify/ws browser: got %d want %d", recBrowser.Code, http.StatusNoContent)
	}

	// REST verification route (/internal/auth/verify) in enabled mode STILL requires strict Bearer.
	reqRestWS := httptest.NewRequest("GET", "/internal/auth/verify", nil)
	reqRestWS.Header.Set("Sec-WebSocket-Protocol", WSTelemetryProtocol+", bearer."+valid)
	recRestWS := httptest.NewRecorder()
	enabledHandler.ServeHTTP(recRestWS, reqRestWS)
	if recRestWS.Code != http.StatusUnauthorized {
		t.Fatalf("REST route with WS subprotocol: got %d want %d", recRestWS.Code, http.StatusUnauthorized)
	}
}
