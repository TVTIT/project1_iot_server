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
	calls      int
	actor      uuid.UUID
	input      mqttcredential.MutationInput
	resetInput mqttcredential.ResetInput
	err        error
	response   mqttcredential.ProvisionResponse
}

func (s *credentialSpy) Reset(_ context.Context, actor uuid.UUID, in mqttcredential.ResetInput) (mqttcredential.ProvisionResponse, error) {
	s.calls++
	s.actor = actor
	s.resetInput = in
	return s.response, s.err
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

type noResetCapabilityWrapper struct {
	CredentialManager
}

type typedNilResetter struct{}

func (*typedNilResetter) Metadata(context.Context, uuid.UUID, string) (mqttcredential.Metadata, error) {
	return mqttcredential.Metadata{}, nil
}
func (*typedNilResetter) Provision(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error) {
	return mqttcredential.ProvisionResponse{}, nil
}
func (*typedNilResetter) Rotate(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error) {
	return mqttcredential.ProvisionResponse{}, nil
}
func (*typedNilResetter) Revoke(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.MutationResult, error) {
	return mqttcredential.MutationResult{}, nil
}
func (t *typedNilResetter) Reset(context.Context, uuid.UUID, mqttcredential.ResetInput) (mqttcredential.ProvisionResponse, error) {
	if t == nil {
		panic("typed nil Reset invoked")
	}
	return mqttcredential.ProvisionResponse{}, nil
}

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

	for _, name := range []string{"missing", "invalid", "owner", "operator", "viewer", "non-member", "claim-only", "admin", "lookup-outage", "disabled", "typed-nil", "not-ready", "nil-readiness", "typed-nil-readiness"} {
		t.Run("POST/reset/"+name, func(t *testing.T) {
			spy := &credentialSpy{}
			d, actor, token := credentialDeps(t, spy)
			d.AdminMaxBodyBytes = 128
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
				want = 200
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
			req := httptest.NewRequest("POST", "/v1/admin/gateways/gateway_test/mqtt-credential/reset", strings.NewReader(`{"reason":"recovery_after_unknown"}`))
			req.Header.Set("Content-Type", "application/json")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			key := uuid.New()
			req.Header.Set("Idempotency-Key", key.String())
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != want {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cacheable credential response")
			}
			if name == "admin" {
				if spy.calls != 1 || spy.actor != actor || spy.resetInput.GatewayID != "gateway_test" || spy.resetInput.IdempotencyKey != key || spy.resetInput.Reason != mqttcredential.ResetReasonRecoveryAfterUnknown {
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
	for _, path := range []string{"/v1/admin/gateways/gateway_test/mqtt-credential", "/v1/admin/gateways/gateway_test/mqtt-credential/rotate", "/v1/admin/gateways/gateway_test/mqtt-credential/reset"} {
		for _, method := range []string{"PUT", "PATCH", "HEAD", "GET", "DELETE"} {
			if path == "/v1/admin/gateways/gateway_test/mqtt-credential" && (method == "GET" || method == "DELETE") {
				continue
			}
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

	rec = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/", nil)
	credentialResetHandler(d)(c)
	if rec.Code != 401 || spy.calls != 0 {
		t.Fatal("missing middleware principal accepted for reset")
	}
}

func TestCredentialResetRoute_SuccessAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		key         string
		id          string
		query       string
		contentType string
		respStatus  int
		setupSpy    func(*credentialSpy)
		wrapManager func(CredentialManager) CredentialManager
		wantStatus  int
		wantCode    string
		wantCalls   int
	}{
		{
			name:        "success recovery_after_unknown with secret",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			setupSpy: func(s *credentialSpy) {
				m := mqttcredential.Metadata{
					GatewayID:         "gateway_test",
					Username:          "gateway_test",
					CredentialVersion: 3,
					Status:            mqttcredential.CredentialActive,
					LastOperationID:   uuid.New(),
					OperationStatus:   mqttcredential.OperationSucceeded,
				}
				s.response.MutationResult = mqttcredential.MutationResult{
					Metadata:    m,
					OperationID: m.LastOperationID,
					NextAction:  "rotate_with_new_idempotency_key",
				}
				secret := mqttcredential.NewSecretResult(m, m.LastOperationID, "reset-secret-plaintext-value")
				s.response.Secret = &secret
			},
			wantStatus: 200,
			wantCalls:  1,
		},
		{
			name:        "success lost_secret_recovery metadata replay",
			body:        `{"reason":"lost_secret_recovery"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			setupSpy: func(s *credentialSpy) {
				m := mqttcredential.Metadata{
					GatewayID:         "gateway_test",
					Username:          "gateway_test",
					CredentialVersion: 3,
					Status:            mqttcredential.CredentialActive,
					LastOperationID:   uuid.New(),
					OperationStatus:   mqttcredential.OperationSucceeded,
				}
				s.response.MutationResult = mqttcredential.MutationResult{
					Metadata:    m,
					OperationID: m.LastOperationID,
					NextAction:  "rotate_with_new_idempotency_key",
				}
			},
			wantStatus: 200,
			wantCalls:  1,
		},
		{
			name:        "empty body rejected",
			body:        "",
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "empty object rejected",
			body:        "{}",
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "unknown field actor rejected",
			body:        `{"reason":"recovery_after_unknown","actor":"spoof"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "unknown field password rejected",
			body:        `{"reason":"recovery_after_unknown","password":"secret"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "unknown field version rejected",
			body:        `{"reason":"recovery_after_unknown","version":2}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "unknown field force rejected",
			body:        `{"reason":"recovery_after_unknown","force":true}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "unknown field quiescent rejected",
			body:        `{"reason":"recovery_after_unknown","quiescent":true}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "null reason rejected",
			body:        `{"reason":null}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "empty reason string rejected",
			body:        `{"reason":""}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "arbitrary invalid reason rejected",
			body:        `{"reason":"arbitrary_reset_reason"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "duplicate key in json rejected",
			body:        `{"reason":"recovery_after_unknown","rea\u0073on":"lost_secret_recovery"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "trailing json rejected",
			body:        `{"reason":"recovery_after_unknown"}{}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "non-json content type rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "text/plain",
			wantStatus:  415,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "oversize body rejected",
			body:        `{"reason":"recovery_after_unknown"}` + strings.Repeat(" ", 150),
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  413,
			wantCode:    "request_too_large",
			wantCalls:   0,
		},
		{
			name:        "query param rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			query:       "?actor=spoof",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "missing idempotency key rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "malformed idempotency key rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "not-a-uuid",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "nil idempotency key rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         uuid.Nil.String(),
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "duplicate idempotency key header rejected",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "duplicate",
			id:          "gateway_test",
			contentType: "application/json",
			wantStatus:  400,
			wantCode:    "invalid_request",
			wantCalls:   0,
		},
		{
			name:        "missing reset capability returns 503",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wrapManager: func(m CredentialManager) CredentialManager {
				return &noResetCapabilityWrapper{CredentialManager: m}
			},
			wantStatus: 503,
			wantCode:   string(mqttcredential.CodeServiceUnavailable),
			wantCalls:  0,
		},
		{
			name:        "typed nil reset capability returns 503",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			wrapManager: func(_ CredentialManager) CredentialManager {
				var nilResetter *typedNilResetter
				return nilResetter
			},
			wantStatus: 503,
			wantCode:   string(mqttcredential.CodeServiceUnavailable),
			wantCalls:  0,
		},
		{
			name:        "domain error mapped without secret exposure",
			body:        `{"reason":"recovery_after_unknown"}`,
			key:         "valid",
			id:          "gateway_test",
			contentType: "application/json",
			setupSpy: func(s *credentialSpy) {
				s.err = &mqttcredential.DomainError{Code: mqttcredential.CodeRecoveryRequired}
				// Also attach a secret to ensure it is cleared and NOT leaked on error!
				m := mqttcredential.Metadata{GatewayID: "gateway_test"}
				secret := mqttcredential.NewSecretResult(m, uuid.New(), "do-not-leak-this-secret")
				s.response.Secret = &secret
			},
			wantStatus: 503,
			wantCode:   string(mqttcredential.CodeRecoveryRequired),
			wantCalls:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &credentialSpy{}
			if tc.setupSpy != nil {
				tc.setupSpy(spy)
			}
			d, actor, token := credentialDeps(t, spy)
			d.AdminMaxBodyBytes = 128
			if tc.wrapManager != nil {
				d.CredentialManager = tc.wrapManager(d.CredentialManager)
			}
			router, err := NewRouter(d)
			if err != nil {
				if tc.name == "typed nil reset capability returns 503" {
					return
				}
				t.Fatal(err)
			}
			url := "/v1/admin/gateways/" + tc.id + "/mqtt-credential/reset" + tc.query
			req := httptest.NewRequest("POST", url, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
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

			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cacheable reset response")
			}
			if spy.calls != tc.wantCalls {
				t.Fatalf("calls=%d want=%d", spy.calls, tc.wantCalls)
			}
			if tc.wantCalls > 0 {
				if spy.actor != actor {
					t.Fatalf("actor mismatch: got %v want %v", spy.actor, actor)
				}
				if spy.resetInput.GatewayID != tc.id {
					t.Fatalf("gateway id mismatch: got %v want %v", spy.resetInput.GatewayID, tc.id)
				}
			}
			if tc.wantCode != "" && !strings.Contains(rec.Body.String(), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("expected error code %s in body %s", tc.wantCode, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "do-not-leak-this-secret") {
				t.Fatal("leaked secret on error!")
			}
			if spy.response.Secret != nil {
				b, err := spy.response.Secret.MarshalJSON()
				if err != nil || strings.Contains(string(b), "reset-secret-plaintext-value") || strings.Contains(string(b), "do-not-leak-this-secret") {
					t.Fatal("secret reference not cleared!")
				}
			}
		})
	}
}
