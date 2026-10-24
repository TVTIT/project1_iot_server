package httpserver

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

func TestHTTPMetricsBoundedTemplatesAndScopes(t *testing.T) {
	m := NewHTTPMetrics()
	d := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	d.HTTPMetrics = m
	r, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/readyz", "/v1/digital-twins/private-entity-1", "/v1/digital-twins/private-entity-2", "/not-found-private"} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	var count uint64
	for _, s := range m.Snapshot() {
		count += s.Duration.Count
		if strings.Contains(s.Route, "private") {
			t.Fatal(s)
		}
		if s.Route == "GET /v1/digital-twins/:entity_id" && (s.Duration.Count != 2 || s.Scope != "business") {
			t.Fatal(s)
		}
	}
	if count != 4 {
		t.Fatal(count)
	}
}

func TestMetricsRouteUsesExistingAdminGuardsAndFlag(t *testing.T) {
	for _, tc := range []struct {
		name, token            string
		enabled, admin, broken bool
		want                   int
	}{
		{"missing", "", true, true, false, 401}, {"bad", "bad", true, true, false, 401},
		{"regular", "valid", true, false, false, 403}, {"admin", "valid", true, true, false, 200},
		{"disabled", "valid", false, true, false, 404}, {"dependency", "valid", true, true, true, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			d.MetricsEnabled = tc.enabled
			d.MetricsSnapshot = func() any { return map[string]int{"schema_version": 1} }
			d.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
				if token != "valid" {
					return auth.Principal{}, auth.ErrInvalidToken
				}
				return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
			})
			d.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
				if tc.broken {
					return false, errors.New("private database error")
				}
				return tc.admin, nil
			})
			r, err := NewRouter(d)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "/v1/admin/metrics", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.want || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
			if strings.Contains(w.Body.String(), "private") {
				t.Fatal(w.Body.String())
			}
		})
	}
}
