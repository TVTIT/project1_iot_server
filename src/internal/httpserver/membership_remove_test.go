package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/usermanagement"
)

type fakeGatewayUserRemover struct {
	calls         int
	actor, target uuid.UUID
	gw            string
	err           error
}

func TestRemoveGatewayUserSuccessAudit(t *testing.T) {
	actor, target := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name   string
		err    error
		token  string
		body   string
		status int
	}{
		{name: "removal", status: 204}, {name: "no-op", status: 204},
		{name: "owner", err: usermanagement.ErrOwnerTransferRequired, status: 409},
		{name: "commit error", err: errors.New("private-database-sentinel"), status: 500},
		{name: "unauthenticated", token: "missing", status: 401},
		{name: "invalid body", body: "private-body-sentinel", status: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) { return auth.Principal{UserID: actor}, nil })
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return true, nil })
			deps.GatewayUserRemover = &fakeGatewayUserRemover{err: tc.err}
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodDelete, "/v1/admin/gateways/gateway_remove/users/"+target.String(), strings.NewReader(tc.body))
			req.Header.Set("X-Request-ID", "req-removal-audit")
			if tc.token != "missing" {
				req.Header.Set("Authorization", "Bearer private-token-sentinel")
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d", w.Code, tc.status)
			}
			if w.Code == 204 && (w.Body.Len() != 0 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache") {
				t.Error("success contract changed")
			}
			count := 0
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				if line == "" {
					continue
				}
				var event map[string]any
				if json.Unmarshal([]byte(line), &event) != nil {
					t.Fatal("invalid structured log")
				}
				if event["msg"] != "gateway membership revocation completed" {
					continue
				}
				count++
				if event["request_id"] != "req-removal-audit" || event["actor_id"] != actor.String() || event["target_id"] != target.String() || event["gateway_id"] != "gateway_remove" || event["outcome"] != "success" {
					t.Error("incorrect success audit identity or outcome")
				}
				// Only envelope fields and allowlisted audit attributes are permitted.
				for key := range event {
					switch key {
					case "time", "level", "msg", "request_id", "actor_id", "target_id", "gateway_id", "outcome":
					default:
						t.Errorf("unexpected audit field %s", key)
					}
				}
			}
			want := 0
			if tc.status == 204 {
				want = 1
			}
			if count != want {
				t.Errorf("success events=%d want=%d", count, want)
			}
			for _, secret := range []string{"private-token-sentinel", "private-body-sentinel", "private-database-sentinel"} {
				if strings.Contains(logs.String(), secret) {
					t.Error("private diagnostic leaked")
				}
			}
		})
	}
}

func (f *fakeGatewayUserRemover) RemoveGatewayUser(_ context.Context, actor uuid.UUID, gw string, target uuid.UUID) error {
	f.calls++
	f.actor, f.gw, f.target = actor, gw, target
	return f.err
}

func TestRemoveGatewayUserHTTP(t *testing.T) {
	actor, target := uuid.New(), uuid.New()
	base := "/v1/admin/gateways/gateway_remove/users/" + target.String()
	for _, tc := range []struct {
		name, path, body, token string
		admin                   bool
		adminErr, err           error
		nilDep                  bool
		status                  int
		code                    string
		calls                   int
	}{
		{name: "viewer removed", status: 204, calls: 1}, {name: "operator removed", status: 204, calls: 1}, {name: "no membership", status: 204, calls: 1},
		{name: "self membership", path: "/v1/admin/gateways/gateway_remove/users/" + actor.String(), status: 204, calls: 1},
		{name: "owner", err: usermanagement.ErrOwnerTransferRequired, status: 409, code: "owner_transfer_required", calls: 1},
		{name: "self owner", path: "/v1/admin/gateways/gateway_remove/users/" + actor.String(), err: usermanagement.ErrOwnerTransferRequired, status: 409, code: "owner_transfer_required", calls: 1},
		{name: "missing gateway", err: usermanagement.ErrGatewayNotFound, status: 404, code: "not_found", calls: 1},
		{name: "transaction authorization", err: usermanagement.ErrActorNotAdmin, status: 403, code: "forbidden", calls: 1},
		{name: "timeout", err: context.DeadlineExceeded, status: 503, code: "service_unavailable", calls: 1},
		{name: "unavailable", err: usermanagement.ErrMembershipUnavailable, status: 503, code: "service_unavailable", calls: 1},
		{name: "cancel", err: context.Canceled, status: 503, code: "service_unavailable", calls: 1},
		{name: "sql failure", err: errors.New("private SQL diagnostic"), status: 500, code: "internal_error", calls: 1},
		{name: "bad UUID", path: "/v1/admin/gateways/gateway_remove/users/bad", status: 400, code: "invalid_request"},
		{name: "nil UUID", path: "/v1/admin/gateways/gateway_remove/users/" + uuid.Nil.String(), status: 400, code: "invalid_request"},
		{name: "bad gateway", path: "/v1/admin/gateways/bad!id/users/" + target.String(), status: 400, code: "invalid_request"},
		{name: "reserved gateway", path: "/v1/admin/gateways/backend_service/users/" + target.String(), status: 400, code: "invalid_request"},
		{name: "body", body: "{}", status: 400, code: "invalid_request"}, {name: "whitespace body", body: " ", status: 400, code: "invalid_request"},
		{name: "query", path: base + "?role=viewer", status: 400, code: "invalid_request"}, {name: "empty query", path: base + "?", status: 400, code: "invalid_request"},
		{name: "missing JWT", token: "missing", status: 401, code: "unauthorized"}, {name: "invalid JWT", token: "invalid", status: 401, code: "unauthorized"},
		{name: "non admin", admin: true, status: 403, code: "forbidden"},
		{name: "admin check unavailable", adminErr: errors.New("private DB error"), status: 503, code: "service_unavailable"},
		{name: "nil dependency", nilDep: true, status: 503, code: "service_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeGatewayUserRemover{err: tc.err}
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.GatewayUserRemover = f
			if tc.nilDep {
				deps.GatewayUserRemover = (*fakeGatewayUserRemover)(nil)
			}
			deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, tok string) (auth.Principal, error) {
				if tok == "invalid" {
					return auth.Principal{}, errors.New("invalid token")
				}
				return auth.Principal{UserID: actor}, nil
			})
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return !tc.admin, tc.adminErr })
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			path := tc.path
			if path == "" {
				path = base
			}
			req := httptest.NewRequest(http.MethodDelete, path, strings.NewReader(tc.body))
			if tc.token != "missing" {
				tok := tc.token
				if tok == "" {
					tok = "valid"
				}
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
			if f.calls != tc.calls {
				t.Fatalf("mutation calls %d, want %d", f.calls, tc.calls)
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Error("missing cache headers")
			}
			if tc.status == 204 {
				if w.Body.Len() != 0 {
					t.Error("204 must be bodyless")
				}
			} else {
				var env httpapi.APIErrorEnvelope
				if json.Unmarshal(w.Body.Bytes(), &env) != nil || env.Error.Code != tc.code || env.Error.RequestID == "" || env.Error.RequestID != w.Header().Get("X-Request-ID") {
					t.Errorf("wrong error envelope: %s", w.Body.String())
				}
				if strings.Contains(w.Body.String(), "private") {
					t.Error("private diagnostic exposed")
				}
			}
			if tc.calls == 1 && (f.actor != actor || f.gw != "gateway_remove" || f.target.String() != path[strings.LastIndex(path, "/")+1:]) {
				t.Error("wrong actor or target")
			}
		})
	}
}

func TestRemoveGatewayUserCORS(t *testing.T) {
	for _, origin := range []string{"https://client.example.com", "https://other.example.com"} {
		f := &fakeGatewayUserRemover{}
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.GatewayUserRemover = f
		deps.CORSAllowedOrigins = []string{"https://client.example.com"}
		router, err := NewRouter(deps)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodOptions, "/v1/admin/gateways/gateway_remove/users/"+uuid.NewString(), nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", http.MethodDelete)
		req.Header.Set("Access-Control-Request-Headers", "Authorization")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if f.calls != 0 {
			t.Error("preflight executed removal")
		}
		if origin == "https://client.example.com" {
			if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != origin || !strings.Contains(w.Header().Get("Access-Control-Allow-Methods"), "DELETE") {
				t.Errorf("invalid DELETE preflight: %d %v", w.Code, w.Header())
			}
		} else if w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("non-allowlisted origin accepted")
		}
	}
}
