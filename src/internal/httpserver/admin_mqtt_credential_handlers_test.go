package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/mqttcredential"
)

type credentialSpy struct {
	calls    int
	actor    uuid.UUID
	input    mqttcredential.MutationInput
	err      error
	response mqttcredential.ProvisionResponse
}

func (s *credentialSpy) Metadata(_ context.Context, actor uuid.UUID, id string) (mqttcredential.Metadata, error) {
	s.calls++
	s.actor = actor
	s.input.GatewayID = id
	return s.response.Metadata, s.err
}
func (s *credentialSpy) Provision(_ context.Context, actor uuid.UUID, in mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error) {
	s.calls++
	s.actor = actor
	s.input = in
	return s.response, s.err
}
func (s *credentialSpy) Rotate(ctx context.Context, actor uuid.UUID, in mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error) {
	return s.Provision(ctx, actor, in)
}
func (s *credentialSpy) Revoke(ctx context.Context, actor uuid.UUID, in mqttcredential.MutationInput) (mqttcredential.MutationResult, error) {
	r, err := s.Provision(ctx, actor, in)
	return r.MutationResult, err
}

type credentialReady bool

func (r credentialReady) Ready() bool { return bool(r) }

type nilCredentialReadiness struct{}

func (*nilCredentialReadiness) Ready() bool { panic("typed nil readiness invoked") }

var credentialRoutes = []struct {
	method, suffix string
	status         int
	action         mqttcredential.Action
}{
	{"GET", "", 200, ""}, {"POST", "", 201, mqttcredential.ActionProvision},
	{"POST", "/rotate", 200, mqttcredential.ActionRotate}, {"DELETE", "", 200, mqttcredential.ActionRevoke},
}

func credentialDeps(t *testing.T, spy *credentialSpy) (RouterDependencies, uuid.UUID, string) {
	t.Helper()
	d := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	id := uuid.New()
	secret := "test-only-credential-http-signing-key-32bytes"
	v, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: secret, Issuer: "http://localhost/auth/v1", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": "http://localhost/auth/v1", "aud": "authenticated", "sub": id.String(), "role": "authenticated", "admin": true, "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Add(-time.Second).Unix()}).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	d.TokenVerifier = v
	d.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return true, nil })
	d.CredentialAPIEnabled = true
	d.CredentialRequestTimeout = time.Second
	d.CredentialManager = spy
	d.CredentialStartupReadiness = credentialReady(true)
	d.AdminMaxBodyBytes = 32
	return d, id, token
}

func TestCredentialRoutesAuthorization(t *testing.T) {
	for _, route := range credentialRoutes {
		for _, name := range []string{"missing", "invalid", "owner", "operator", "viewer", "non-member", "claim-only", "admin", "lookup-outage", "disabled", "typed-nil", "not-ready", "nil-readiness", "typed-nil-readiness"} {
			t.Run(route.method+route.suffix+"/"+name, func(t *testing.T) {
				spy := &credentialSpy{}
				d, actor, token := credentialDeps(t, spy)
				want, lookups := 403, 0
				d.PlatformAdminChecker = platformAdminCheckerFunc(func(_ context.Context, got uuid.UUID) (bool, error) {
					lookups++
					if got != actor {
						t.Fatal("spoofed actor")
					}
					if name == "lookup-outage" {
						return false, errors.New("private database secret")
					}
					return name == "admin" || name == "disabled" || name == "typed-nil" || name == "not-ready" || name == "nil-readiness" || name == "typed-nil-readiness", nil
				})
				switch name {
				case "missing":
					token = ""
					want = 401
				case "invalid":
					token = "invalid"
					want = 401
				case "admin":
					want = route.status
				case "lookup-outage", "disabled", "typed-nil", "not-ready", "nil-readiness", "typed-nil-readiness":
					want = 503
				}
				if name == "disabled" {
					d.CredentialAPIEnabled = false
				}
				if name == "typed-nil" {
					var nilSpy *credentialSpy
					d.CredentialManager = nilSpy
				}
				if name == "not-ready" {
					d.CredentialStartupReadiness = credentialReady(false)
				}
				if name == "nil-readiness" {
					d.CredentialStartupReadiness = nil
				}
				if name == "typed-nil-readiness" {
					var readiness *nilCredentialReadiness
					d.CredentialStartupReadiness = readiness
				}
				router, err := NewRouter(d)
				if err != nil {
					if name == "typed-nil" || name == "nil-readiness" || name == "typed-nil-readiness" {
						return
					}
					t.Fatal(err)
				}
				req := httptest.NewRequest(route.method, "/v1/admin/gateways/gateway_test/mqtt-credential"+route.suffix, nil)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				req.Header.Set("Idempotency-Key", uuid.NewString())
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != want {
					t.Fatalf("status=%d want=%d", rec.Code, want)
				}
				if rec.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("cacheable credential response")
				}
				if name == "admin" {
					if spy.calls != 1 || spy.actor != actor || spy.input.GatewayID != "gateway_test" || spy.input.Action != route.action {
						t.Fatal("wrong service invocation")
					}
				} else if spy.calls != 0 {
					t.Fatal("denied request reached metadata/audit/broker service boundary")
				}
				if (name == "missing" || name == "invalid") && lookups != 0 {
					t.Fatal("unauthenticated lookup")
				}
				if strings.Contains(rec.Body.String(), "private database") {
					t.Fatal("unsafe error")
				}
			})
		}
	}
}

func TestCredentialRequestValidation(t *testing.T) {
	for _, route := range credentialRoutes {
		for _, tc := range []struct {
			name, body, key, id, query string
			status                     int
		}{
			{"body", "{}", "valid", "gateway_test", "", 400},
			{"whitespace", " ", "valid", "gateway_test", "", 400},
			{"oversize", strings.Repeat("x", 33), "valid", "gateway_test", "", 413},
			{"invalid-id", "", "valid", "backend_service", "", 400},
			{"query", "", "valid", "gateway_test", "?actor=spoof", 400},
			{"bad-key", "", "bad", "gateway_test", "", 400},
			{"missing-key", "", "", "gateway_test", "", 400},
			{"nil-key", "", uuid.Nil.String(), "gateway_test", "", 400},
			{"duplicate-key", "", "duplicate", "gateway_test", "", 400},
		} {
			if route.method == "GET" && strings.Contains(tc.name, "key") {
				continue
			}
			t.Run(route.method+route.suffix+tc.name, func(t *testing.T) {
				spy := &credentialSpy{}
				d, _, token := credentialDeps(t, spy)
				router, err := NewRouter(d)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(route.method, "/v1/admin/gateways/"+tc.id+"/mqtt-credential"+route.suffix+tc.query, strings.NewReader(tc.body))
				req.Header.Set("Authorization", "Bearer "+token)
				key := tc.key
				if key == "valid" || key == "duplicate" {
					key = uuid.NewString()
				}
				if key != "" {
					req.Header.Set("Idempotency-Key", key)
				}
				if tc.key == "duplicate" {
					req.Header.Add("Idempotency-Key", uuid.NewString())
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != tc.status || spy.calls != 0 {
					t.Fatalf("status=%d calls=%d", rec.Code, spy.calls)
				}
			})
		}
	}
}

func TestCredentialErrorsAndReplay(t *testing.T) {
	for _, route := range credentialRoutes {
		for _, tc := range []struct {
			code   mqttcredential.ErrorCode
			status int
		}{
			{mqttcredential.CodeInvalidRequest, 400}, {mqttcredential.CodeForbidden, 403}, {mqttcredential.CodeNotFound, 404},
			{mqttcredential.CodeCredentialConflict, 409}, {mqttcredential.CodeOperationInProgress, 409}, {mqttcredential.CodeIdempotencyConflict, 409},
			{mqttcredential.CodeRuntimeDisabled, 503}, {mqttcredential.CodeRuntimeBusy, 503}, {mqttcredential.CodeVerificationUnavailable, 503},
			{mqttcredential.CodeRecoveryRequired, 503}, {mqttcredential.CodeFinalizationPending, 503}, {mqttcredential.CodeServiceUnavailable, 503},
			{mqttcredential.CodeInternalError, 500}, {"untrusted private detail", 500},
		} {
			t.Run(route.method+route.suffix+string(tc.code), func(t *testing.T) {
				spy := &credentialSpy{err: &mqttcredential.DomainError{Code: tc.code}}
				d, _, token := credentialDeps(t, spy)
				router, err := NewRouter(d)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(route.method, "/v1/admin/gateways/gateway_test/mqtt-credential"+route.suffix, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Idempotency-Key", uuid.NewString())
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				code := tc.code
				if tc.status == 500 {
					code = mqttcredential.CodeInternalError
				}
				if rec.Code != tc.status || !strings.Contains(rec.Body.String(), `"code":"`+string(code)+`"`) || !strings.Contains(rec.Body.String(), `"request_id":`) {
					t.Fatalf("status=%d safe error contract missing", rec.Code)
				}
				if strings.Contains(rec.Body.String(), "private detail") || strings.Contains(rec.Body.String(), "operation_id") || rec.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("unsafe error response")
				}
			})
		}
	}
	for _, action := range []mqttcredential.Action{mqttcredential.ActionProvision, mqttcredential.ActionRotate} {
		for _, replay := range []bool{false, true} {
			spy := &credentialSpy{}
			m := mqttcredential.Metadata{GatewayID: "gateway_test", Username: "gateway_test", CredentialVersion: 2, Status: mqttcredential.CredentialActive, LastOperationID: uuid.New(), OperationStatus: mqttcredential.OperationSucceeded}
			spy.response.MutationResult = mqttcredential.MutationResult{Metadata: m, OperationID: m.LastOperationID, NextAction: "rotate_with_new_idempotency_key"}
			if !replay {
				secret := mqttcredential.NewSecretResult(m, m.LastOperationID, "test-only-transient-secret")
				spy.response.Secret = &secret
			}
			d, _, token := credentialDeps(t, spy)
			router, err := NewRouter(d)
			if err != nil {
				t.Fatal(err)
			}
			path := "/v1/admin/gateways/gateway_test/mqtt-credential"
			if action == mqttcredential.ActionRotate {
				path += "/rotate"
			}
			req := httptest.NewRequest("POST", path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", uuid.NewString())
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			want := 200
			if action == mqttcredential.ActionProvision {
				want = 201
			}
			if rec.Code != want || rec.Header().Get("Pragma") != "no-cache" {
				t.Fatal("wrong response status/cache policy")
			}
			if strings.Contains(rec.Body.String(), `"password":`) == replay || !strings.Contains(rec.Body.String(), `"secret_returned":`+map[bool]string{true: "false", false: "true"}[replay]) {
				t.Fatal("incorrect secret/replay DTO")
			}
			if spy.response.Secret != nil {
				b, err := spy.response.Secret.MarshalJSON()
				if err != nil || strings.Contains(string(b), "test-only-transient-secret") {
					t.Fatal("secret reference not cleared")
				}
			}
		}
	}
}

func TestCredentialFailedReplayAndRawError(t *testing.T) {
	for _, hasCode := range []bool{false, true} {
		spy := &credentialSpy{}
		spy.response.ReplayedStatus = mqttcredential.OperationFailed
		if hasCode {
			code := mqttcredential.CodeVerificationUnavailable
			spy.response.ErrorCode = &code
		}
		d, _, token := credentialDeps(t, spy)
		router, err := NewRouter(d)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("DELETE", "/v1/admin/gateways/gateway_test/mqtt-credential", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", uuid.NewString())
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		want := 500
		if hasCode {
			want = 503
		}
		if rec.Code != want {
			t.Fatal("failed replay reported as success")
		}
	}
	gin.SetMode(gin.TestMode)
	for _, err := range []error{errors.New("password=private-secret database snapshot"), fmt.Errorf("wrapped: %w", &mqttcredential.DomainError{Code: mqttcredential.CodeRecoveryRequired})} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		writeCredentialError(c, err)
		if strings.Contains(rec.Body.String(), "private-secret") {
			t.Fatal("raw exception leaked")
		}
	}
}

func TestCredentialUnsupportedMethodsAndMissingPrincipal(t *testing.T) {
	spy := &credentialSpy{}
	d, _, token := credentialDeps(t, spy)
	router, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/admin/gateways/gateway_test/mqtt-credential", "/v1/admin/gateways/gateway_test/mqtt-credential/rotate"} {
		for _, method := range []string{"PUT", "PATCH", "HEAD"} {
			req := httptest.NewRequest(method, path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != 405 || rec.Header().Get("Cache-Control") != "no-store" || spy.calls != 0 {
				t.Fatal("unsupported method reached service")
			}
		}
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/", nil)
	credentialHandler(d, "")(c)
	if rec.Code != 401 || spy.calls != 0 {
		t.Fatal("missing middleware principal accepted")
	}
}
