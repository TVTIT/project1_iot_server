package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

func setupTestHeaderRouter(t *testing.T) (http.Handler, uuid.UUID) {
	t.Helper()
	testActor := uuid.New()
	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid-token" {
			return auth.Principal{UserID: testActor, Role: "authenticated"}, nil
		}
		return auth.Principal{}, errors.New("invalid token")
	})

	router := newTestStage4Router(t, verifier, func(deps *RouterDependencies) {
		gwName := "GW-01"
		deps.TwinListReader = &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{
					{
						ID:        "urn:ngsi-ld:Gateway:gw-01",
						Type:      digitaltwin.EntityTypeGateway,
						Name:      &gwName,
						GatewayID: "gw-01",
						UpdatedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
					},
				}}, nil
			},
		}
		deps.TwinDetailReader = &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				return &digitaltwin.TwinDetailResult{
					ID:            "urn:ngsi-ld:Gateway:gw-01",
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "GW-01",
					GatewayID:     "gw-01",
					Relationships: []digitaltwin.RelationshipItem{},
				}, nil
			},
		}
		deps.TwinStateReader = &mockTwinStateReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
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
			fn: func(_ context.Context, _ uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
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
		deps.EntityCreator = &mockEntityCreator{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
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
		deps.GatewayPatcher = &mockGatewayPatcher{
			fn: func(_ context.Context, _ uuid.UUID, gatewayID string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				return digitaltwin.PatchGatewayResult{
					ID:        "urn:ngsi-ld:Gateway:gw-01",
					Type:      digitaltwin.EntityTypeGateway,
					GatewayID: gatewayID,
					Name:      "Updated GW",
					UpdatedAt: time.Now(),
				}, nil
			},
		}
		deps.SensorPatcher = &mockSensorPatcher{
			fn: func(_ context.Context, _ uuid.UUID, gatewayID, sensorID string, _ digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
				return digitaltwin.PatchSensorResult{
					ID:        "urn:ngsi-ld:Sensor:" + gatewayID + ":" + sensorID,
					Type:      digitaltwin.EntityTypeSensor,
					GatewayID: gatewayID,
					SensorID:  sensorID,
					Name:      "Updated Sensor",
					UpdatedAt: time.Now(),
				}, nil
			},
		}
	})

	return router, testActor
}

func TestStage4Headers_ContentTypeAndRepresentation(t *testing.T) {
	router, _ := setupTestHeaderRouter(t)

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		wantStatus  int
		wantCT      string
		wantContext bool
	}{
		{
			name:        "1. List returns application/ld+json",
			method:      http.MethodGet,
			path:        "/v1/digital-twins?type=Gateway&limit=10",
			wantStatus:  http.StatusOK,
			wantCT:      "application/ld+json; charset=utf-8",
			wantContext: true,
		},
		{
			name:        "2. Default Detail returns application/ld+json",
			method:      http.MethodGet,
			path:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			wantStatus:  http.StatusOK,
			wantCT:      "application/ld+json; charset=utf-8",
			wantContext: true,
		},
		{
			name:        "3. Simplified State returns application/json",
			method:      http.MethodGet,
			path:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			wantStatus:  http.StatusOK,
			wantCT:      "application/json; charset=utf-8",
			wantContext: false,
		},
		{
			name:        "4. Temporal returns application/ld+json",
			method:      http.MethodGet,
			path:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			wantStatus:  http.StatusOK,
			wantCT:      "application/ld+json; charset=utf-8",
			wantContext: true,
		},
		{
			name:        "5. POST returns application/json",
			method:      http.MethodPost,
			path:        "/v1/digital-twins",
			body:        `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			wantStatus:  http.StatusCreated,
			wantCT:      "application/json; charset=utf-8",
			wantContext: false,
		},
		{
			name:        "6. PATCH returns application/json",
			method:      http.MethodPatch,
			path:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:        `{"name":{"type":"Property","value":"Updated GW"}}`,
			wantStatus:  http.StatusOK,
			wantCT:      "application/json; charset=utf-8",
			wantContext: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader *strings.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			} else {
				bodyReader = strings.NewReader("")
			}
			req := httptest.NewRequest(tc.method, tc.path, bodyReader)
			req.Header.Set("Authorization", "Bearer valid-token")
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if gotCT := rec.Header().Get("Content-Type"); gotCT != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", gotCT, tc.wantCT)
			}
			hasContext := strings.Contains(rec.Body.String(), "@context")
			if tc.wantContext && !hasContext {
				t.Errorf("expected @context in response body, got: %s", rec.Body.String())
			}
			if !tc.wantContext && hasContext {
				t.Errorf("expected NO @context in response body, got: %s", rec.Body.String())
			}
		})
	}
}

func TestStage4Headers_ErrorResponsesHaveNoContextAndAreJSON(t *testing.T) {
	router, _ := setupTestHeaderRouter(t)

	tests := []struct {
		name       string
		method     string
		path       string
		token      string
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "401 unauthorized on missing token",
			method:     http.MethodGet,
			path:       "/v1/digital-twins",
			token:      "",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
		{
			name:       "400 invalid query parameter",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?invalid_param=1",
			token:      "valid-token",
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "400 invalid cursor parameter",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?cursor=invalid_not_base64",
			token:      "valid-token",
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_cursor",
		},
		{
			name:       "405 method not allowed on root PATCH",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			token:      "valid-token",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			wantStatus: http.StatusMethodNotAllowed,
			wantCode:   "method_not_allowed",
		},
		{
			name:       "404 not found on removed /state route",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/state",
			token:      "valid-token",
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Errorf("error Content-Type = %q, want application/json", ct)
			}
			if strings.Contains(rec.Body.String(), "@context") {
				t.Errorf("error response body must not contain @context: %s", rec.Body.String())
			}

			var errResp struct {
				Error struct {
					Code      string `json:"code"`
					Message   string `json:"message"`
					RequestID string `json:"request_id"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("unmarshal error response failed: %v", err)
			}
			if errResp.Error.Code != tc.wantCode {
				t.Errorf("error code = %q, want %q", errResp.Error.Code, tc.wantCode)
			}
		})
	}
}

func TestStage4Headers_AcceptNegotiation(t *testing.T) {
	router, _ := setupTestHeaderRouter(t)

	type testCase struct {
		name          string
		method        string
		path          string
		body          string
		accept        string
		acceptHeaders []string
		wantStatus    int
		wantCT        string
		checkVary     bool
	}

	tests := []testCase{
		// --- JSON-LD Endpoints: List, Detail default, Temporal ---
		{
			name:       "List: absent Accept serves default JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: */* serves default JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "*/*",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: application/* serves default JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/*",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: application/ld+json exact match serves JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/ld+json",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: multiple ranges with valid q serves JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "text/html, application/xhtml+xml, application/ld+json;q=0.9, */*;q=0.8",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: media range with profile parameter parses correctly",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="http://example.org";q=0.9`,
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: valid q=0.5 serves JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/ld+json;q=0.5",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: q=0 exclusion returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/ld+json;q=0, application/json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: escaped quote in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: extended q* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: continuation q*0 overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;q=0;q*0=1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: extended continuation q*0* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;q=0;q*0*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: quoted profile with extended q* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: quoted comma and semicolon with positive q serves JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/ld+json;profile="https://example.org/a,b;c";q=0.9`,
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: exact match q=0 overrides type wildcard positive q returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `application/*;q=0.8, application/ld+json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: exact match q=0 overrides full wildcard positive q returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     `*/*;q=0.9, application/ld+json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:   "List: multiple Accept headers with compatible quoted comma before q=0 returns 406",
			method: http.MethodGet,
			path:   "/v1/digital-twins?type=Gateway&limit=10",
			acceptHeaders: []string{
				"text/html",
				`application/ld+json;profile="https://example.org/a,b";q=0`,
			},
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:   "List: multiple Accept headers with positive q serves JSON-LD",
			method: http.MethodGet,
			path:   "/v1/digital-twins?type=Gateway&limit=10",
			acceptHeaders: []string{
				"text/html;q=0.5",
				`application/ld+json;profile="https://example.org/a,b";q=0.9`,
			},
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: application/* with specific q=0 exclusion returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/*;q=0.8, application/ld+json;q=0",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: */*;q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "*/*;q=0",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: incompatible application/json returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "application/json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "List: incompatible text/html returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins?type=Gateway&limit=10",
			accept:     "text/html, text/plain",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: absent Accept serves default JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     "",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: incompatible application/json returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     "application/json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: incompatible text/html returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     "text/html",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: escaped quote in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: extended q* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: continuation q*0 overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;q=0;q*0=1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: extended continuation q*0* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;q=0;q*0*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Detail default: quoted profile with extended q* overriding q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01",
			accept:     `application/ld+json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: absent Accept serves default JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     "",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: application/ld+json serves JSON-LD",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     "application/ld+json",
			wantStatus: http.StatusOK,
			wantCT:     "application/ld+json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: incompatible application/json returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     "application/json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: incompatible text/html returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     "text/html",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     `application/ld+json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Temporal: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z",
			accept:     `application/ld+json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},

		// --- JSON Endpoints: Simplified State, POST, PATCH ---
		{
			name:       "Simplified State: absent Accept serves default JSON",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     "",
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: */* serves default JSON",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     "*/*",
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: application/json serves JSON",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     "application/json",
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: incompatible application/ld+json returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     "application/ld+json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: incompatible text/html returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     "text/html",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     `application/json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: escaped quote in parameter before q=0 returns 406",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     `application/json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "Simplified State: quoted comma in parameter with positive q serves JSON",
			method:     http.MethodGet,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01?format=simplified",
			accept:     `application/json;profile="https://example.org/a,b";q=0.8`,
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: absent Accept serves default JSON",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     "",
			wantStatus: http.StatusCreated,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: application/json serves JSON",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     "application/json",
			wantStatus: http.StatusCreated,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: incompatible application/ld+json returns 406",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     "application/ld+json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: incompatible text/html returns 406",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     "text/html",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     `application/json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: escaped quote in parameter before q=0 returns 406",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     `application/json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "POST: quoted comma in parameter with positive q serves JSON",
			method:     http.MethodPost,
			path:       "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0.7`,
			wantStatus: http.StatusCreated,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:   "POST: multiple Accept headers with compatible quoted comma before q=0 returns 406",
			method: http.MethodPost,
			path:   "/v1/digital-twins",
			body:   `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + uuid.New().String() + `"}`,
			acceptHeaders: []string{
				"text/html",
				`application/json;profile="https://example.org/a,b";q=0`,
			},
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: absent Accept serves default JSON",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     "",
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: application/json serves JSON",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     "application/json",
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: incompatible application/ld+json returns 406",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     "application/ld+json",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: incompatible text/html returns 406",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     "text/html",
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: quoted comma in parameter before q=0 returns 406",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: quoted semicolon in parameter before q=0 returns 406",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: escaped quote in parameter before q=0 returns 406",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
		{
			name:       "PATCH: quoted comma in parameter with positive q serves JSON",
			method:     http.MethodPatch,
			path:       "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0.6`,
			wantStatus: http.StatusOK,
			wantCT:     "application/json; charset=utf-8",
			checkVary:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader *strings.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			} else {
				bodyReader = strings.NewReader("")
			}
			req := httptest.NewRequest(tc.method, tc.path, bodyReader)
			req.Header.Set("Authorization", "Bearer valid-token")
			req.Header.Set("Origin", "https://app.test.local")
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if len(tc.acceptHeaders) > 0 {
				for _, h := range tc.acceptHeaders {
					req.Header.Add("Accept", h)
				}
			} else if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if gotCT := rec.Header().Get("Content-Type"); gotCT != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", gotCT, tc.wantCT)
			}

			if tc.checkVary {
				vary := rec.Header().Get("Vary")
				if !containsVaryHeader(vary, "Origin") {
					t.Errorf("Vary = %q does not contain Origin", vary)
				}
				if !containsVaryHeader(vary, "Accept") {
					t.Errorf("Vary = %q does not contain Accept", vary)
				}
			}

			if tc.wantStatus == http.StatusNotAcceptable {
				var errResp struct {
					Error struct {
						Code      string `json:"code"`
						Message   string `json:"message"`
						RequestID string `json:"request_id"`
					} `json:"error"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
					t.Fatalf("unmarshal 406 error response: %v", err)
				}
				if errResp.Error.Code != "not_acceptable" {
					t.Errorf("406 error code = %q, want not_acceptable", errResp.Error.Code)
				}
				if strings.Contains(rec.Body.String(), "@context") {
					t.Errorf("406 error response must not contain @context: %s", rec.Body.String())
				}
			}
		})
	}
}

func TestStage4Headers_AcceptNegotiation_ZeroWriterCallsOnQZero(t *testing.T) {
	testActor := uuid.New()
	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid-token" {
			return auth.Principal{UserID: testActor, Role: "authenticated"}, nil
		}
		return auth.Principal{}, errors.New("invalid token")
	})

	var creatorCalls int
	var gwPatcherCalls int
	var sensorPatcherCalls int

	router := newTestStage4Router(t, verifier, func(deps *RouterDependencies) {
		deps.EntityCreator = &mockEntityCreator{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
				creatorCalls++
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
		deps.GatewayPatcher = &mockGatewayPatcher{
			fn: func(_ context.Context, _ uuid.UUID, gatewayID string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				gwPatcherCalls++
				return digitaltwin.PatchGatewayResult{
					ID:        "urn:ngsi-ld:Gateway:gw-01",
					Type:      digitaltwin.EntityTypeGateway,
					GatewayID: gatewayID,
					Name:      "Updated GW",
					UpdatedAt: time.Now(),
				}, nil
			},
		}
		deps.SensorPatcher = &mockSensorPatcher{
			fn: func(_ context.Context, _ uuid.UUID, gatewayID, sensorID string, _ digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
				sensorPatcherCalls++
				return digitaltwin.PatchSensorResult{
					ID:        "urn:ngsi-ld:Sensor:" + gatewayID + ":" + sensorID,
					Type:      digitaltwin.EntityTypeSensor,
					GatewayID: gatewayID,
					SensorID:  sensorID,
					Name:      "Updated Sensor",
					UpdatedAt: time.Now(),
				}, nil
			},
		}
	})

	cases := []struct {
		name       string
		method     string
		url        string
		body       string
		accept     string
		wantStatus int
	}{
		{
			name:       "POST: quoted comma before q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: quoted semicolon before q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: escaped quote before q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: plain q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: continuation q*0 overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;q=0;q*0=1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: extended continuation q*0* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;q=0;q*0*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "POST: quoted profile with extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw-01","name":"GW-01","owner_user_id":"` + testActor.String() + `"}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: quoted comma before q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: quoted semicolon before q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a;b";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: escaped quote before q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/\"escaped\"";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: continuation q*0 overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;q=0;q*0=1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: extended continuation q*0* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;q=0;q*0*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Gateway: quoted profile with extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated GW"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Sensor: quoted comma before q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated Sensor"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Sensor: extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated Sensor"}}`,
			accept:     `application/json;q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Sensor: continuation q*0 overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated Sensor"}}`,
			accept:     `application/json;q=0;q*0=1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Sensor: extended continuation q*0* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated Sensor"}}`,
			accept:     `application/json;q=0;q*0*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "PATCH Sensor: quoted profile with extended q* overriding q=0 returns 406 and never calls writer",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/attrs",
			body:       `{"name":{"type":"Property","value":"Updated Sensor"}}`,
			accept:     `application/json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantStatus: http.StatusNotAcceptable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			creatorBefore := creatorCalls
			gwBefore := gwPatcherCalls
			sensorBefore := sensorPatcherCalls

			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer valid-token")
			req.Header.Set("Origin", "https://app.test.local")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", tc.accept)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if creatorCalls != creatorBefore {
				t.Errorf("creator was called %d times (before: %d)", creatorCalls, creatorBefore)
			}
			if gwPatcherCalls != gwBefore {
				t.Errorf("gwPatcher was called %d times (before: %d)", gwPatcherCalls, gwBefore)
			}
			if sensorPatcherCalls != sensorBefore {
				t.Errorf("sensorPatcher was called %d times (before: %d)", sensorPatcherCalls, sensorBefore)
			}
			vary := rec.Header().Get("Vary")
			if !containsVaryHeader(vary, "Origin") {
				t.Errorf("Vary = %q does not contain Origin", vary)
			}
			if !containsVaryHeader(vary, "Accept") {
				t.Errorf("Vary = %q does not contain Accept", vary)
			}
		})
	}
}

func TestStage4Headers_CORSAndCacheBoundaries(t *testing.T) {
	router, _ := setupTestHeaderRouter(t)

	// 1. CORS Expose Headers on actual GET
	t.Run("Actual GET with allowed Origin exposes Link, Location, X-Request-ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?type=Gateway&limit=10", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Origin", "https://app.test.local")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.test.local" {
			t.Errorf("Access-Control-Allow-Origin = %q, want https://app.test.local", got)
		}
		if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "Link, Location, X-Request-ID" {
			t.Errorf("Access-Control-Expose-Headers = %q, want Link, Location, X-Request-ID", got)
		}
	})

	// 2. CORS Preflight on /attrs
	t.Run("CORS Preflight on /attrs with allowed Origin exposes Link, Location, X-Request-ID and has no-store/no-cache", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs", nil)
		req.Header.Set("Origin", "https://app.test.local")
		req.Header.Set("Access-Control-Request-Method", "PATCH")
		req.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type, Idempotency-Key, X-Request-ID")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.test.local" {
			t.Errorf("Access-Control-Allow-Origin = %q, want https://app.test.local", got)
		}
		if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "Link, Location, X-Request-ID" {
			t.Errorf("Access-Control-Expose-Headers = %q, want Link, Location, X-Request-ID", got)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
		if got := rec.Header().Get("Pragma"); got != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache on mutation preflight", got)
		}
	})

	// 3. CORS Preflight with disallowed Origin returns 403 Forbidden
	t.Run("CORS Preflight with disallowed Origin returns 403 and denies access", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs", nil)
		req.Header.Set("Origin", "https://evil.attacker.com")
		req.Header.Set("Access-Control-Request-Method", "PATCH")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 Forbidden", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Access-Control-Allow-Origin should be empty, got %q", got)
		}
	})

	// 4. Cache-Control and Pragma on /temporal across responses
	temporalPath := "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?attrs=measurement&timerel=between&timeAt=2026-08-21T10:15:00Z&endTimeAt=2026-08-21T10:15:05Z"
	t.Run("/temporal response cache headers: 200, 401, 400, 405, 204 preflight", func(t *testing.T) {
		// 200 OK
		req := httptest.NewRequest(http.MethodGet, temporalPath, nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("temporal 200 failed: %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("200 temporal Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "" {
			t.Errorf("200 temporal should not have Pragma, got %q", rec.Header().Get("Pragma"))
		}

		// 401 Unauthorized
		req401 := httptest.NewRequest(http.MethodGet, temporalPath, nil)
		rec401 := httptest.NewRecorder()
		router.ServeHTTP(rec401, req401)
		if rec401.Code != http.StatusUnauthorized {
			t.Fatalf("temporal 401 failed: %d", rec401.Code)
		}
		if rec401.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("401 temporal Cache-Control = %q, want no-store", rec401.Header().Get("Cache-Control"))
		}
		if rec401.Header().Get("Pragma") != "" {
			t.Errorf("401 temporal should not have Pragma, got %q", rec401.Header().Get("Pragma"))
		}

		// 400 Bad Request
		req400 := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal?invalid=1", nil)
		req400.Header.Set("Authorization", "Bearer valid-token")
		rec400 := httptest.NewRecorder()
		router.ServeHTTP(rec400, req400)
		if rec400.Code != http.StatusBadRequest {
			t.Fatalf("temporal 400 failed: %d", rec400.Code)
		}
		if rec400.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("400 temporal Cache-Control = %q, want no-store", rec400.Header().Get("Cache-Control"))
		}
		if rec400.Header().Get("Pragma") != "" {
			t.Errorf("400 temporal should not have Pragma, got %q", rec400.Header().Get("Pragma"))
		}

		// 405 Method Not Allowed
		req405 := httptest.NewRequest(http.MethodPost, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal", nil)
		req405.Header.Set("Authorization", "Bearer valid-token")
		rec405 := httptest.NewRecorder()
		router.ServeHTTP(rec405, req405)
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Fatalf("temporal 405 failed: %d", rec405.Code)
		}
		if rec405.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("405 temporal Cache-Control = %q, want no-store", rec405.Header().Get("Cache-Control"))
		}

		// 204 Preflight
		req204 := httptest.NewRequest(http.MethodOptions, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw-01%3As-01/temporal", nil)
		req204.Header.Set("Origin", "https://app.test.local")
		req204.Header.Set("Access-Control-Request-Method", "GET")
		rec204 := httptest.NewRecorder()
		router.ServeHTTP(rec204, req204)
		if rec204.Code != http.StatusNoContent {
			t.Fatalf("temporal 204 failed: %d", rec204.Code)
		}
		if rec204.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("204 temporal Cache-Control = %q, want no-store", rec204.Header().Get("Cache-Control"))
		}
		if rec204.Header().Get("Pragma") != "" {
			t.Errorf("204 GET temporal should not have Pragma, got %q", rec204.Header().Get("Pragma"))
		}
	})

	// 5. Cache-Control and Pragma on /attrs across responses
	attrsPath := "/v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw-01/attrs"
	patchBody := `{"name":{"type":"Property","value":"Updated GW"}}`
	t.Run("/attrs response cache headers: 200, 401, 400, 405, 204 preflight", func(t *testing.T) {
		// 200 OK
		req := httptest.NewRequest(http.MethodPatch, attrsPath, strings.NewReader(patchBody))
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("attrs 200 failed: %d", rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("200 attrs Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("Pragma") != "no-cache" {
			t.Errorf("200 attrs Pragma = %q, want no-cache", rec.Header().Get("Pragma"))
		}

		// 401 Unauthorized
		req401 := httptest.NewRequest(http.MethodPatch, attrsPath, strings.NewReader(patchBody))
		req401.Header.Set("Content-Type", "application/json")
		rec401 := httptest.NewRecorder()
		router.ServeHTTP(rec401, req401)
		if rec401.Code != http.StatusUnauthorized {
			t.Fatalf("attrs 401 failed: %d", rec401.Code)
		}
		if rec401.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("401 attrs Cache-Control = %q, want no-store", rec401.Header().Get("Cache-Control"))
		}
		if rec401.Header().Get("Pragma") != "no-cache" {
			t.Errorf("401 attrs Pragma = %q, want no-cache", rec401.Header().Get("Pragma"))
		}

		// 400 Bad Request
		req400 := httptest.NewRequest(http.MethodPatch, attrsPath, strings.NewReader(`{}`))
		req400.Header.Set("Authorization", "Bearer valid-token")
		req400.Header.Set("Content-Type", "application/json")
		rec400 := httptest.NewRecorder()
		router.ServeHTTP(rec400, req400)
		if rec400.Code != http.StatusBadRequest {
			t.Fatalf("attrs 400 failed: %d", rec400.Code)
		}
		if rec400.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("400 attrs Cache-Control = %q, want no-store", rec400.Header().Get("Cache-Control"))
		}
		if rec400.Header().Get("Pragma") != "no-cache" {
			t.Errorf("400 attrs Pragma = %q, want no-cache", rec400.Header().Get("Pragma"))
		}

		// 405 Method Not Allowed (POST to /attrs)
		req405 := httptest.NewRequest(http.MethodPost, attrsPath, strings.NewReader(patchBody))
		req405.Header.Set("Authorization", "Bearer valid-token")
		req405.Header.Set("Content-Type", "application/json")
		rec405 := httptest.NewRecorder()
		router.ServeHTTP(rec405, req405)
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Fatalf("attrs 405 failed: %d", rec405.Code)
		}
		if rec405.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("405 attrs Cache-Control = %q, want no-store", rec405.Header().Get("Cache-Control"))
		}
		if rec405.Header().Get("Pragma") != "no-cache" {
			t.Errorf("405 attrs Pragma = %q, want no-cache", rec405.Header().Get("Pragma"))
		}

		// 204 Preflight
		req204 := httptest.NewRequest(http.MethodOptions, attrsPath, nil)
		req204.Header.Set("Origin", "https://app.test.local")
		req204.Header.Set("Access-Control-Request-Method", "PATCH")
		rec204 := httptest.NewRecorder()
		router.ServeHTTP(rec204, req204)
		if rec204.Code != http.StatusNoContent {
			t.Fatalf("attrs 204 failed: %d", rec204.Code)
		}
		if rec204.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("204 attrs Cache-Control = %q, want no-store", rec204.Header().Get("Cache-Control"))
		}
		if rec204.Header().Get("Pragma") != "no-cache" {
			t.Errorf("204 attrs Pragma = %q, want no-cache", rec204.Header().Get("Pragma"))
		}
	})
}

func TestStage4Headers_ExactTwinRoutesAndOldRoutesGone(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TwinListReader = &mockTwinListReader{}
	deps.TwinDetailReader = &mockTwinDetailReader{}
	deps.TwinStateReader = &mockTwinStateReader{}
	deps.TwinHistoryReader = &mockTwinHistoryReader{}
	deps.EntityCreator = &mockEntityCreator{}
	deps.GatewayPatcher = &mockGatewayPatcher{}
	deps.SensorPatcher = &mockSensorPatcher{}

	r, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() failed: %v", err)
	}

	engine, ok := r.(*gin.Engine)
	if !ok {
		t.Fatalf("expected *gin.Engine, got %T", r)
	}

	routes := engine.Routes()

	// 1. Collect all routes matching /v1/digital-twins*
	twinRoutes := make(map[string]bool)
	for _, route := range routes {
		if strings.HasPrefix(route.Path, "/v1/digital-twins") {
			key := route.Method + " " + route.Path
			twinRoutes[key] = true
		}
	}

	expectedTwinRoutes := []string{
		"GET /v1/digital-twins",
		"POST /v1/digital-twins",
		"GET /v1/digital-twins/:entity_id",
		"PATCH /v1/digital-twins/:entity_id/attrs",
		"GET /v1/digital-twins/:entity_id/temporal",
		"PATCH /v1/digital-twins/:entity_id/desired-state",
		"GET /v1/digital-twins/:entity_id/commands",
		"GET /v1/digital-twins/:entity_id/commands/:command_id",
	}

	if len(twinRoutes) != len(expectedTwinRoutes) {
		t.Fatalf("expected exactly %d routes under /v1/digital-twins, found %d: %+v", len(expectedTwinRoutes), len(twinRoutes), twinRoutes)
	}

	for _, expected := range expectedTwinRoutes {
		if !twinRoutes[expected] {
			t.Errorf("missing expected route: %s", expected)
		}
	}

	// 2. Ensure old removed routes are NOT in router.Routes()
	forbiddenRoutes := []string{
		"GET /v1/telemetry/history",
		"GET /v1/digital-twins/:entity_id/state",
		"GET /v1/digital-twins/:entity_id/history",
		"PATCH /v1/digital-twins/:entity_id",
	}

	for _, route := range routes {
		key := route.Method + " " + route.Path
		for _, forbidden := range forbiddenRoutes {
			if key == forbidden {
				t.Errorf("found removed route still registered: %s", key)
			}
		}
	}
}
