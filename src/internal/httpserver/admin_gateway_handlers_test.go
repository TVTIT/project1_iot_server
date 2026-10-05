package httpserver

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
)

type gatewayProvisionerFunc func(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error)

func (f gatewayProvisionerFunc) ProvisionGateway(c context.Context, a uuid.UUID, i gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
	return f(c, a, i)
}

func TestRegisteredGatewayProvisioning(t *testing.T) {
	actor, owner := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name                 string
		authenticated, admin bool
		err                  error
		created              bool
		status               int
	}{
		{"JWT missing", false, true, nil, false, 401},
		{"owner", true, false, nil, false, 403}, {"operator", true, false, nil, false, 403}, {"viewer", true, false, nil, false, 403},
		{"created", true, true, nil, true, 201}, {"retry", true, true, nil, false, 200},
		{"conflict", true, true, gateway.ErrProvisioningConflict, false, 409},
		{"owner missing", true, true, gateway.ErrProvisioningNotFound, false, 404},
		{"invalid", true, true, gateway.ErrInvalidProvisioningInput, false, 400},
		{"timeout", true, true, context.DeadlineExceeded, false, 503},
		{"cancel", true, true, context.Canceled, false, 503},
		{"lookup", true, true, gateway.ErrProvisioningAdminLookup, false, 503},
		{"internal", true, true, errors.New("SECRET SQL"), false, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
				if !tc.authenticated {
					return auth.Principal{}, errors.New("invalid")
				}
				return auth.Principal{UserID: actor, Role: "authenticated"}, nil
			})
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return tc.admin, nil })
			deps.GatewayProvisioner = gatewayProvisionerFunc(func(_ context.Context, a uuid.UUID, i gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
				calls++
				if a != actor || i.OwnerUserID != owner || i.GatewayID != "fixture" || i.Name != " raw " {
					t.Fatal("identity or metadata changed")
				}
				return gateway.ProvisionGatewayResult{Created: tc.created, GatewayID: i.GatewayID, Name: i.Name, OwnerUserID: i.OwnerUserID, EntityID: "urn:ngsi-ld:Gateway:fixture"}, tc.err
			})
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("PUT", "/v1/admin/gateways/fixture", strings.NewReader(`{"name":" raw ","owner_user_id":"`+owner.String()+`"}`))
			req.Header.Set("Content-Type", "application/json")
			if tc.authenticated {
				req.Header.Set("Authorization", "Bearer fixture")
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if tc.status == 401 || tc.status == 403 {
				if calls != 0 {
					t.Fatal("unauthorized mutation")
				}
			} else if calls != 1 {
				t.Fatal("not called")
			}
			if strings.Contains(rec.Body.String(), "SECRET") {
				t.Fatal("unsafe error")
			}
			if tc.status == 200 || tc.status == 201 {
				if rec.Header().Get("Cache-Control") != "no-store" || !strings.Contains(rec.Body.String(), `"created_at":null`) {
					t.Fatal("response contract")
				}
			}
		})
	}
}

func TestProvisionerMandatory(t *testing.T) {
	for _, p := range []GatewayProvisioner{nil, gatewayProvisionerFunc(nil)} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.GatewayProvisioner = p
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("missing dependency accepted")
		}
	}
}

func TestRegisteredAdminLookupFailure(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return false, errors.New("SECRET lookup") })
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("PUT", "/v1/admin/gateways/fixture", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer fixture")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != 503 || strings.Contains(rec.Body.String(), "SECRET") {
		t.Fatalf("unsafe lookup failure: %d %s", rec.Code, rec.Body.String())
	}
}
