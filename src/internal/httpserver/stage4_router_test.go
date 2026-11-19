package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/telemetry"
)

func newTestStage4Router(t *testing.T, verifier auth.Verifier, mutate func(*RouterDependencies)) http.Handler {
	t.Helper()
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	if verifier != nil {
		deps.TokenVerifier = verifier
	}
	deps.CORSAllowedOrigins = []string{"https://app.test.local"}
	if mutate != nil {
		mutate(&deps)
	}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}
	return router
}

func TestStage4Router_SevenDispatchPaths(t *testing.T) {
	testActor := uuid.New()
	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid-token" {
			return auth.Principal{UserID: testActor, Role: "authenticated"}, nil
		}
		return auth.Principal{}, errors.New("invalid token")
	})

	var (
		telemetryCalled  bool
		twinListCalled   bool
		entityCreated    bool
		twinDetailCalled bool
		patchGwCalled    bool
		twinStateCalled  bool
		twinHistCalled   bool
	)
	_ = telemetryCalled

	router := newTestStage4Router(t, verifier, func(deps *RouterDependencies) {
		deps.TelemetryHistoryReader = &mockTelemetryHistoryReader{
			fn: func(_ context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
				telemetryCalled = true
				if actor != testActor {
					t.Errorf("telemetry actor = %v, want %v", actor, testActor)
				}
				if q.GatewayID != "gw-01" || q.SensorID != "s-01" {
					t.Errorf("telemetry query mismatch: %+v", q)
				}
				return &telemetry.TelemetryHistoryResult{
					GatewayID:    q.GatewayID,
					SensorID:     q.SensorID,
					From:         q.Range.From,
					To:           q.Range.To,
					Mode:         "raw",
					Resolution:   "raw",
					PointCount:   0,
					RawPoints:    []telemetry.RawSamplePoint{},
					BucketPoints: []telemetry.BucketSamplePoint{},
				}, nil
			},
		}

		deps.TwinListReader = &mockTwinListReader{
			fn: func(_ context.Context, actor uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				twinListCalled = true
				if actor != testActor {
					t.Errorf("twin list actor = %v, want %v", actor, testActor)
				}
				return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{}}, nil
			},
		}

		deps.EntityCreator = &mockEntityCreator{
			fn: func(_ context.Context, actor uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
				entityCreated = true
				if actor != testActor {
					t.Errorf("entity creator actor = %v, want %v", actor, testActor)
				}
				now := time.Now().UTC()
				return digitaltwin.CreateEntityResult{
					Created:  true,
					Type:     digitaltwin.EntityTypeGateway,
					EntityID: "urn:ngsi-ld:Gateway:gw-01",
					Gateway: &gateway.ProvisionGatewayResult{
						Created:     true,
						GatewayID:   "gw-01",
						Name:        "GW-01",
						EntityID:    "urn:ngsi-ld:Gateway:gw-01",
						OwnerUserID: testActor,
						CreatedAt:   &now,
					},
				}, nil
			},
		}

		deps.TwinDetailReader = &mockTwinDetailReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				twinDetailCalled = true
				if actor != testActor {
					t.Errorf("twin detail actor = %v, want %v", actor, testActor)
				}
				if q.EntityID != "urn:ngsi-ld:Gateway:gw-01" {
					t.Errorf("twin detail entityID = %v, want urn:ngsi-ld:Gateway:gw-01", q.EntityID)
				}
				return &digitaltwin.TwinDetailResult{
					ID:            "urn:ngsi-ld:Gateway:gw-01",
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "GW-01",
					GatewayID:     "gw-01",
					Relationships: []digitaltwin.RelationshipItem{},
				}, nil
			},
		}

		deps.GatewayPatcher = &mockGatewayPatcher{
			fn: func(_ context.Context, actor uuid.UUID, gatewayID string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				patchGwCalled = true
				if actor != testActor {
					t.Errorf("gateway patcher actor = %v, want %v", actor, testActor)
				}
				if gatewayID != "gw-01" {
					t.Errorf("gatewayID = %v, want gw-01", gatewayID)
				}
				return digitaltwin.PatchGatewayResult{
					ID:        "urn:ngsi-ld:Gateway:gw-01",
					Type:      digitaltwin.EntityTypeGateway,
					GatewayID: gatewayID,
					Name:      "Updated GW",
					UpdatedAt: time.Now(),
				}, nil
			},
		}

		deps.TwinStateReader = &mockTwinStateReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				twinStateCalled = true
				if actor != testActor {
					t.Errorf("twin state actor = %v, want %v", actor, testActor)
				}
				if q.EntityID != "urn:ngsi-ld:Gateway:gw-01" {
					t.Errorf("twin state entityID = %v, want urn:ngsi-ld:Gateway:gw-01", q.EntityID)
				}
				return &digitaltwin.TwinStateResult{
					ID:              "urn:ngsi-ld:Gateway:gw-01",
					Type:            digitaltwin.EntityTypeGateway,
					GatewayID:       "gw-01",
					ReportedState:   json.RawMessage(`{}`),
					DesiredState:    json.RawMessage(`{}`),
					ReportedVersion: 1,
					DesiredVersion:  1,
				}, nil
			},
		}

		deps.TwinHistoryReader = &mockTwinHistoryReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				twinHistCalled = true
				if actor != testActor {
					t.Errorf("twin history actor = %v, want %v", actor, testActor)
				}
				if q.EntityID != "urn:ngsi-ld:Sensor:gw-01:s-01" {
					t.Errorf("twin history entityID = %v, want urn:ngsi-ld:Sensor:gw-01:s-01", q.EntityID)
				}
				return &digitaltwin.TwinHistoryResult{
					EntityID:   q.EntityID,
					Property:   q.Property,
					From:       q.Range.From,
					To:         q.Range.To,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 0,
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:    "gw-01",
						SensorID:     "s-01",
						From:         q.Range.From,
						To:           q.Range.To,
						Mode:         "raw",
						Resolution:   "raw",
						PointCount:   0,
						RawPoints:    []telemetry.RawSamplePoint{},
						BucketPoints: []telemetry.BucketSamplePoint{},
					},
				}, nil
			},
		}
	})

	t.Run("1. GET /v1/telemetry/history is removed and returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?gateway_id=gw-01&sensor_id=s-01&from=2026-08-21T10:15:00.000Z&to=2026-08-21T10:16:00.000Z", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "req-hist-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found for removed /telemetry/history, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("2. GET /v1/digital-twins", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?type=Gateway&limit=10", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "req-list-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/ld+json") {
			t.Errorf("Content-Type = %q, want application/ld+json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("X-Request-ID") != "req-list-01" {
			t.Errorf("X-Request-ID = %q, want req-list-01", rec.Header().Get("X-Request-ID"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if !twinListCalled {
			t.Error("twin list reader was not called")
		}

		// Rejection of replaced parameters: entity_type and after
		reqBadType := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?entity_type=Gateway", nil)
		reqBadType.Header.Set("Authorization", "Bearer valid-token")
		recBadType := httptest.NewRecorder()
		router.ServeHTTP(recBadType, reqBadType)
		if recBadType.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request for entity_type parameter, got %d", recBadType.Code)
		}

		reqBadAfter := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?after=some_cursor", nil)
		reqBadAfter.Header.Set("Authorization", "Bearer valid-token")
		recBadAfter := httptest.NewRecorder()
		router.ServeHTTP(recBadAfter, reqBadAfter)
		if recBadAfter.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request for after parameter, got %d", recBadAfter.Code)
		}
	})

	t.Run("3. POST /v1/digital-twins", func(t *testing.T) {
		body := `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-ID", "req-post-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
		}
		wantLoc := "/v1/digital-twins/" + url.PathEscape("urn:ngsi-ld:Gateway:gw-01")
		if rec.Header().Get("Location") != wantLoc {
			t.Errorf("Location = %q, want %q", rec.Header().Get("Location"), wantLoc)
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Cache-Control = %q, Pragma = %q; want no-store / no-cache", rec.Header().Get("Cache-Control"), rec.Header().Get("Pragma"))
		}
		if !entityCreated {
			t.Error("entity creator was not called")
		}
	})

	t.Run("4. GET /v1/digital-twins/:entity_id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "req-detail-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		// Detail MUST return application/ld+json; charset=utf-8 per Stage 4 contract!
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/ld+json") {
			t.Errorf("Content-Type = %q, want application/ld+json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if !twinDetailCalled {
			t.Error("twin detail reader was not called")
		}
	})

	t.Run("5. PATCH /v1/digital-twins/:entity_id/attrs and root PATCH rejection", func(t *testing.T) {
		body := `{"name":{"type":"Property","value":"Updated GW"}}`
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-ID", "req-patch-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Cache-Control = %q, Pragma = %q; want no-store / no-cache", rec.Header().Get("Cache-Control"), rec.Header().Get("Pragma"))
		}
		if !patchGwCalled {
			t.Error("gateway patcher was not called")
		}

		// Old root PATCH /v1/digital-twins/:entity_id must return 405 Method Not Allowed (cannot write!)
		reqRoot := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01", strings.NewReader(body))
		reqRoot.Header.Set("Authorization", "Bearer valid-token")
		reqRoot.Header.Set("Content-Type", "application/json")
		recRoot := httptest.NewRecorder()
		router.ServeHTTP(recRoot, reqRoot)
		if recRoot.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405 Method Not Allowed for root PATCH, got %d", recRoot.Code)
		}
	})

	t.Run("6. GET /v1/digital-twins/:entity_id?format=simplified and /state 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "req-state-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for ?format=simplified, got %d: %s", rec.Code, rec.Body.String())
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if !twinStateCalled {
			t.Error("twin state reader was not called")
		}

		// Old /state route must be removed and return 404
		reqOld := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/state", nil)
		reqOld.Header.Set("Authorization", "Bearer valid-token")
		recOld := httptest.NewRecorder()
		router.ServeHTTP(recOld, reqOld)
		if recOld.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for removed /state route, got %d: %s", recOld.Code, recOld.Body.String())
		}
	})

	t.Run("7. GET /v1/digital-twins/:entity_id/temporal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "req-twin-hist-01")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/ld+json") {
			t.Errorf("Content-Type = %q, want application/ld+json", rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if !twinHistCalled {
			t.Error("twin history reader was not called")
		}

		// Also verify removed route /history returns 404
		reqOld := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/history?property=value&from=2026-08-21T10:15:00.000Z&to=2026-08-21T10:16:00.000Z", nil)
		reqOld.Header.Set("Authorization", "Bearer valid-token")
		recOld := httptest.NewRecorder()
		router.ServeHTTP(recOld, reqOld)
		if recOld.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for removed route /history, got %d: %s", recOld.Code, recOld.Body.String())
		}
	})
}

func TestStage4Router_VerifiedActor(t *testing.T) {
	actors := []uuid.UUID{uuid.New(), uuid.New()}
	for _, expectedActor := range actors {
		verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
			return auth.Principal{UserID: expectedActor, Role: "authenticated"}, nil
		})

		var calledActor uuid.UUID
		router := newTestStage4Router(t, verifier, func(deps *RouterDependencies) {
			deps.TwinListReader = &mockTwinListReader{
				fn: func(_ context.Context, actor uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
					calledActor = actor
					return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{}}, nil
				},
			}
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins", nil)
		req.Header.Set("Authorization", "Bearer token-for-actor")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if calledActor != expectedActor {
			t.Errorf("actor passed to use case = %v, want %v", calledActor, expectedActor)
		}
	}
}

func TestStage4Router_EncodedAndDoubleEncodedURN(t *testing.T) {
	var receivedEntityID string
	router := newTestStage4Router(t, nil, func(deps *RouterDependencies) {
		deps.TwinDetailReader = &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				receivedEntityID = q.EntityID
				return &digitaltwin.TwinDetailResult{
					ID:            q.EntityID,
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "GW",
					GatewayID:     "gw-01",
					Relationships: []digitaltwin.RelationshipItem{},
				}, nil
			},
		}
	})

	t.Run("single encoded URN parses correctly", func(t *testing.T) {
		receivedEntityID = ""
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		if receivedEntityID != "urn:ngsi-ld:Gateway:gw-01" {
			t.Errorf("receivedEntityID = %q, want urn:ngsi-ld:Gateway:gw-01", receivedEntityID)
		}
	})

	t.Run("raw unencoded colons in URN parse correctly", func(t *testing.T) {
		receivedEntityID = ""
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		if receivedEntityID != "urn:ngsi-ld:Gateway:gw-01" {
			t.Errorf("receivedEntityID = %q, want urn:ngsi-ld:Gateway:gw-01", receivedEntityID)
		}
	})

	t.Run("double encoded URN is rejected with 400 Bad Request and reader is not called", func(t *testing.T) {
		receivedEntityID = ""
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%253Angsi-ld%253AGateway%253Agw-01", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d: %s", rec.Code, rec.Body.String())
		}
		if receivedEntityID != "" {
			t.Errorf("reader was called with %q on double encoded URN", receivedEntityID)
		}
		var errEnv httpapi.APIErrorEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &errEnv); err != nil {
			t.Fatalf("unmarshal error envelope: %v", err)
		}
		if errEnv.Error.Code != "invalid_request" {
			t.Errorf("error code = %q, want invalid_request", errEnv.Error.Code)
		}
	})

	t.Run("double encoded URN on state is rejected with 400 Bad Request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%253Angsi-ld%253AGateway%253Agw-01?format=simplified", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("double encoded URN on temporal is rejected with 400 Bad Request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%253Angsi-ld%253ASensor%253Agw-01%253As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:16:00Z", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("double encoded URN on patch is rejected with 400 Bad Request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn%253Angsi-ld%253AGateway%253Agw-01/attrs", strings.NewReader(`{"name":{"type":"Property","value":"GW"}}`))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestStage4Router_JWTRejectionWithoutUseCaseCalls(t *testing.T) {
	callCounts := map[string]int{}

	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid-token" {
			return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		}
		return auth.Principal{}, auth.ErrInvalidToken
	})

	router := newTestStage4Router(t, verifier, func(deps *RouterDependencies) {
		deps.TelemetryHistoryReader = &mockTelemetryHistoryReader{
			fn: func(context.Context, uuid.UUID, telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
				callCounts["telemetry"]++
				return nil, nil
			},
		}
		deps.TwinListReader = &mockTwinListReader{
			fn: func(context.Context, uuid.UUID, digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				callCounts["twin_list"]++
				return nil, nil
			},
		}
		deps.EntityCreator = &mockEntityCreator{
			fn: func(context.Context, uuid.UUID, digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
				callCounts["create_entity"]++
				return digitaltwin.CreateEntityResult{}, nil
			},
		}
		deps.TwinDetailReader = &mockTwinDetailReader{
			fn: func(context.Context, uuid.UUID, digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				callCounts["twin_detail"]++
				return nil, nil
			},
		}
		deps.GatewayPatcher = &mockGatewayPatcher{
			fn: func(context.Context, uuid.UUID, string, digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				callCounts["patch_gw"]++
				return digitaltwin.PatchGatewayResult{}, nil
			},
		}
		deps.SensorPatcher = &mockSensorPatcher{
			fn: func(context.Context, uuid.UUID, string, string, digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
				callCounts["patch_sensor"]++
				return digitaltwin.PatchSensorResult{}, nil
			},
		}
		deps.TwinStateReader = &mockTwinStateReader{
			fn: func(context.Context, uuid.UUID, digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				callCounts["twin_state"]++
				return nil, nil
			},
		}
		deps.TwinHistoryReader = &mockTwinHistoryReader{
			fn: func(context.Context, uuid.UUID, digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				callCounts["twin_history"]++
				return nil, nil
			},
		}
	})

	endpoints := []struct {
		method string
		url    string
		body   string
	}{
		{http.MethodGet, "/v1/digital-twins", ""},
		{http.MethodPost, "/v1/digital-twins", `{"entity_type":"Gateway","gateway_id":"gw-01"}`},
		{http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01", ""},
		{http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01/attrs", `{"name":{"type":"Property","value":"GW"}}`},
		{http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01?format=simplified", ""},
		{http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw-01:s-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z", ""},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.url+" missing token", func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.url, strings.NewReader(ep.body))
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("WWW-Authenticate = %q, want Bearer", rec.Header().Get("WWW-Authenticate"))
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
			}
			if ep.method == http.MethodPost || ep.method == http.MethodPatch {
				if rec.Header().Get("Pragma") != "no-cache" {
					t.Errorf("Pragma = %q, want no-cache on mutation %s %s", rec.Header().Get("Pragma"), ep.method, ep.url)
				}
			}
		})

		t.Run(ep.method+" "+ep.url+" invalid token", func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.url, strings.NewReader(ep.body))
			req.Header.Set("Authorization", "Bearer invalid-token")
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("WWW-Authenticate = %q, want Bearer", rec.Header().Get("WWW-Authenticate"))
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
			}
			if ep.method == http.MethodPost || ep.method == http.MethodPatch {
				if rec.Header().Get("Pragma") != "no-cache" {
					t.Errorf("Pragma = %q, want no-cache on mutation %s %s", rec.Header().Get("Pragma"), ep.method, ep.url)
				}
			}
		})
	}

	for k, v := range callCounts {
		if v > 0 {
			t.Errorf("use case %s was called %d times on unauthenticated requests", k, v)
		}
	}
}

func TestStage4Router_CacheHeadersAndCORS(t *testing.T) {
	router := newTestStage4Router(t, nil, nil)

	t.Run("read endpoints have no-store without pragma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "" {
			t.Errorf("read endpoint unexpectedly has Pragma = %q", pragma)
		}
	})

	t.Run("mutation POST has no-store and pragma no-cache", func(t *testing.T) {
		body := `{"entity_type":"Gateway","gateway_id":"gw-01"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", rec.Header().Get("Pragma"))
		}
	})

	t.Run("mutation PATCH has no-store and pragma no-cache", func(t *testing.T) {
		body := `{"name":"GW"}`
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", rec.Header().Get("Pragma"))
		}
	})

	t.Run("405 on mutation path has no-store and pragma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/digital-twins", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", rec.Header().Get("Pragma"))
		}
	})

	t.Run("405 on read path has no-store without pragma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw-01:s-01/temporal", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "" {
			t.Errorf("read path 405 unexpectedly got Pragma = %q", pragma)
		}
	})

	t.Run("CORS preflight for mutation POST gets 204 with no-store and pragma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins", nil)
		req.Header.Set("Origin", "https://app.test.local")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204 No Content, got %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", rec.Header().Get("Pragma"))
		}
		if rec.Header().Get("Access-Control-Allow-Origin") != "https://app.test.local" {
			t.Errorf("Allow-Origin = %q, want https://app.test.local", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("CORS preflight for read GET gets 204 with no-store and no pragma", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins", nil)
		req.Header.Set("Origin", "https://app.test.local")
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204 No Content, got %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "" {
			t.Errorf("read preflight unexpectedly got Pragma = %q", pragma)
		}
	})

	t.Run("disallowed origin preflight gets 403 with no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins", nil)
		req.Header.Set("Origin", "https://evil.attacker.local")
		req.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden, got %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
	})
}

func TestStage4Router_FutureRoutes(t *testing.T) {
	router := newTestStage4Router(t, nil, nil)

	t.Run("/v1/ws remains 501", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("expected 501 Not Implemented, got %d", rec.Code)
		}
	})

	t.Run("disabled desired-state fails closed and does not match detail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01/desired-state", strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
		}
	})

	t.Run("commands does not permit client POST", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01/commands", strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 Method Not Allowed, got %d", rec.Code)
		}
	})

	t.Run("unrelated Stage 2 route does not get Stage 4 cache header on preflight", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/gateways/gw-01/sensors", nil)
		req.Header.Set("Origin", "https://app.test.local")
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204 No Content, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc == "no-store" {
			t.Errorf("unrelated route unexpectedly got Stage 4 Cache-Control: no-store")
		}
	})
}

func TestStage4Router_RepresentativeDomainErrors(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		url         string
		body        string
		contentType string
		mockSetup   func(deps *RouterDependencies)
		wantStatus  int
		wantCode    string
	}{
		{
			name:       "400 invalid input on list query",
			method:     http.MethodGet,
			url:        "/v1/digital-twins?limit=invalid",
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:   "403 forbidden domain error on detail",
			method: http.MethodGet,
			url:    "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01",
			mockSetup: func(deps *RouterDependencies) {
				deps.TwinDetailReader = &mockTwinDetailReader{
					fn: func(context.Context, uuid.UUID, digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
						return nil, digitaltwin.ErrForbidden
					},
				}
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:   "404 not found domain error on detail",
			method: http.MethodGet,
			url:    "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01",
			mockSetup: func(deps *RouterDependencies) {
				deps.TwinDetailReader = &mockTwinDetailReader{
					fn: func(context.Context, uuid.UUID, digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
						return nil, digitaltwin.ErrNotFound
					},
				}
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:        "409 conflict domain error on post entity",
			method:      http.MethodPost,
			url:         "/v1/digital-twins",
			body:        `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
			contentType: "application/json",
			mockSetup: func(deps *RouterDependencies) {
				deps.EntityCreator = &mockEntityCreator{
					fn: func(context.Context, uuid.UUID, digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
						return digitaltwin.CreateEntityResult{}, gateway.ErrProvisioningConflict
					},
				}
			},
			wantStatus: http.StatusConflict,
			wantCode:   "conflict",
		},
		{
			name:        "413 payload too large on post entity",
			method:      http.MethodPost,
			url:         "/v1/digital-twins",
			body:        strings.Repeat("x", 20000), // > TwinMaxBodyBytes (16384)
			contentType: "application/json",
			wantStatus:  http.StatusRequestEntityTooLarge,
			wantCode:    "payload_too_large",
		},
		{
			name:        "415 unsupported media type on post entity",
			method:      http.MethodPost,
			url:         "/v1/digital-twins",
			body:        `{"entity_type":"Gateway"}`,
			contentType: "text/plain",
			wantStatus:  http.StatusUnsupportedMediaType,
			wantCode:    "unsupported_media_type",
		},
		{
			name:   "500 unexpected error on state",
			method: http.MethodGet,
			url:    "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01?format=simplified",
			mockSetup: func(deps *RouterDependencies) {
				deps.TwinStateReader = &mockTwinStateReader{
					fn: func(context.Context, uuid.UUID, digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
						return nil, digitaltwin.ErrUnexpected
					},
				}
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
		{
			name:   "503 dependency unavailable on temporal history",
			method: http.MethodGet,
			url:    "/v1/digital-twins/urn:ngsi-ld:Sensor:gw-01:s-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			mockSetup: func(deps *RouterDependencies) {
				deps.TwinHistoryReader = &mockTwinHistoryReader{
					fn: func(context.Context, uuid.UUID, digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
						return nil, digitaltwin.ErrDependencyUnavailable
					},
				}
			},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "service_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestStage4Router(t, nil, tt.mockSetup)
			req := httptest.NewRequest(tt.method, tt.url, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer valid-token")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			// All error responses must be application/json; charset=utf-8, even on detail route!
			if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
				t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
			}
			var env httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("unmarshal error envelope: %v", err)
			}
			if env.Error.Code != tt.wantCode {
				t.Errorf("code = %q, want %q", env.Error.Code, tt.wantCode)
			}
			if env.Error.RequestID == "" {
				t.Error("expected non-empty request_id in error envelope")
			}
		})
	}
}
