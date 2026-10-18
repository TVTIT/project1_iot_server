package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// Mock readers for testing read handlers

type mockTelemetryHistoryReader struct {
	fn func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error)
}

func (m *mockTelemetryHistoryReader) GetTelemetryHistory(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, q)
	}
	return nil, nil
}

type mockTwinListReader struct {
	fn func(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error)
}

func (m *mockTwinListReader) ListTwins(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, q)
	}
	return nil, nil
}

type mockTwinDetailReader struct {
	fn func(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error)
}

func (m *mockTwinDetailReader) GetTwinDetail(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, q)
	}
	return nil, nil
}

type mockTwinStateReader struct {
	fn func(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error)
}

func (m *mockTwinStateReader) GetTwinState(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, q)
	}
	return nil, nil
}

type mockTwinHistoryReader struct {
	fn func(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error)
}

func (m *mockTwinHistoryReader) GetTwinHistory(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, q)
	}
	return nil, nil
}

func setupTestParsers(t *testing.T) (*HistoryQueryParser, *TwinQueryParser) {
	t.Helper()
	hCfg := config.DefaultHistoryConfig()
	hp, err := NewHistoryQueryParser(&hCfg)
	if err != nil {
		t.Fatalf("NewHistoryQueryParser failed: %v", err)
	}
	tCfg := config.DefaultTwinConfig()
	tp, err := NewTwinQueryParser(&tCfg)
	if err != nil {
		t.Fatalf("NewTwinQueryParser failed: %v", err)
	}
	return hp, tp
}

func parseErrorResponse(t *testing.T, body []byte) (code string, message string) {
	t.Helper()
	var env struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("failed to unmarshal error envelope: %v (body: %s)", err, string(body))
	}
	return env.Error.Code, env.Error.Message
}

func TestReadHandlers_AuthenticationRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hp, tp := setupTestParsers(t)

	routes := []struct {
		name    string
		method  string
		path    string
		handler gin.HandlerFunc
	}{
		{
			name:    "telemetry_history",
			method:  http.MethodGet,
			path:    "/v1/telemetry/history",
			handler: getTelemetryHistory(&mockTelemetryHistoryReader{}, hp),
		},
		{
			name:    "twin_list",
			method:  http.MethodGet,
			path:    "/v1/digital-twins",
			handler: listDigitalTwins(&mockTwinListReader{}, tp),
		},
		{
			name:    "twin_detail",
			method:  http.MethodGet,
			path:    "/v1/digital-twins/:entity_id",
			handler: getDigitalTwinDetail(&mockTwinDetailReader{}, tp),
		},
		{
			name:    "twin_state",
			method:  http.MethodGet,
			path:    "/v1/digital-twins/:entity_id/state",
			handler: getDigitalTwinState(&mockTwinStateReader{}),
		},
		{
			name:    "twin_history",
			method:  http.MethodGet,
			path:    "/v1/digital-twins/:entity_id/history",
			handler: getDigitalTwinHistory(&mockTwinHistoryReader{}, hp),
		},
	}

	for _, rt := range routes {
		t.Run(rt.name+"/missing_principal", func(t *testing.T) {
			r := gin.New()
			r.Handle(rt.method, rt.path, rt.handler)

			reqPath := rt.path
			switch rt.name {
			case "twin_detail":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1"
			case "twin_state":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1/state"
			case "twin_history":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/history"
			}

			w := httptest.NewRecorder()
			req := httptest.NewRequest(rt.method, reqPath, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
			}
			if w.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", w.Header().Get("WWW-Authenticate"))
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", w.Header().Get("Cache-Control"))
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "unauthorized" {
				t.Errorf("error code = %q, want %q", code, "unauthorized")
			}
		})

		t.Run(rt.name+"/nil_principal_user_id", func(t *testing.T) {
			r := gin.New()
			r.Use(func(c *gin.Context) {
				auth.SetPrincipal(c, auth.Principal{UserID: uuid.Nil, Role: "authenticated"})
				c.Next()
			})
			r.Handle(rt.method, rt.path, rt.handler)

			reqPath := rt.path
			switch rt.name {
			case "twin_detail":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1"
			case "twin_state":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1/state"
			case "twin_history":
				reqPath = "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/history"
			}

			w := httptest.NewRecorder()
			req := httptest.NewRequest(rt.method, reqPath, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestGetTelemetryHistory_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hp, _ := setupTestParsers(t)
	testActor := uuid.MustParse("11111111-1111-4111-8111-111111111111")

	from := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 0, 5, 0, time.UTC)
	unit := "degC"

	t.Run("success_dispatch_and_actor_forwarding", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery telemetry.TelemetryHistoryQuery
		var ctxReceived context.Context

		reader := &mockTelemetryHistoryReader{
			fn: func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
				ctxReceived = ctx
				receivedActor = actor
				receivedQuery = q
				return &telemetry.TelemetryHistoryResult{
					GatewayID:  "gw1",
					SensorID:   "s1",
					Unit:       &unit,
					From:       from,
					To:         to,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					RawPoints: []telemetry.RawSamplePoint{
						{
							MeasuredAt:  from,
							Value:       25.5,
							SequenceNum: 100,
						},
					},
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/telemetry/history", getTelemetryHistory(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?gateway_id=gw1&sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z&resolution=raw&max_points=1000", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if ctxReceived == nil {
			t.Error("expected non-nil context passed to reader")
		}
		if receivedQuery.GatewayID != "gw1" || receivedQuery.SensorID != "s1" {
			t.Errorf("unexpected query: %+v", receivedQuery)
		}

		var resp TelemetryHistoryResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.GatewayID != "gw1" || resp.SensorID != "s1" || resp.PointCount != 1 {
			t.Errorf("unexpected DTO body: %+v", resp)
		}
	})

	t.Run("query_parser_rejections", func(t *testing.T) {
		tests := []struct {
			name        string
			rawQuery    string
			wantStatus  int
			wantErrCode string
		}{
			{
				name:        "missing_gateway_id",
				rawQuery:    "sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z",
				wantStatus:  http.StatusBadRequest,
				wantErrCode: "invalid_request",
			},
			{
				name:        "range_too_large",
				rawQuery:    "gateway_id=gw1&sensor_id=s1&from=2026-01-01T00:00:00Z&to=2026-03-01T00:00:00Z",
				wantStatus:  http.StatusBadRequest,
				wantErrCode: "range_too_large",
			},
			{
				name:        "unknown_query_param",
				rawQuery:    "gateway_id=gw1&sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z&extra=param",
				wantStatus:  http.StatusBadRequest,
				wantErrCode: "invalid_request",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := &mockTelemetryHistoryReader{}
				r := gin.New()
				r.Use(func(c *gin.Context) {
					auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
					c.Next()
				})
				r.GET("/v1/telemetry/history", getTelemetryHistory(reader, hp))

				w := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?"+tt.rawQuery, nil)
				r.ServeHTTP(w, req)

				if w.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
				}
				code, _ := parseErrorResponse(t, w.Body.Bytes())
				if code != tt.wantErrCode {
					t.Errorf("error code = %q, want %q", code, tt.wantErrCode)
				}
			})
		}
	})

	t.Run("domain_errors", func(t *testing.T) {
		tests := []struct {
			name        string
			errToReturn error
			wantStatus  int
			wantErrCode string
		}{
			{
				name:        "not_found",
				errToReturn: telemetry.ErrHistoryNotFound,
				wantStatus:  http.StatusNotFound,
				wantErrCode: "not_found",
			},
			{
				name:        "timeout",
				errToReturn: context.DeadlineExceeded,
				wantStatus:  http.StatusServiceUnavailable,
				wantErrCode: "service_unavailable",
			},
			{
				name:        "dependency_unavailable",
				errToReturn: telemetry.ErrHistoryDependency,
				wantStatus:  http.StatusServiceUnavailable,
				wantErrCode: "service_unavailable",
			},
			{
				name:        "unexpected",
				errToReturn: telemetry.ErrHistoryUnexpected,
				wantStatus:  http.StatusInternalServerError,
				wantErrCode: "internal_error",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := &mockTelemetryHistoryReader{
					fn: func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
						return nil, tt.errToReturn
					},
				}
				r := gin.New()
				r.Use(func(c *gin.Context) {
					auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
					c.Next()
				})
				r.GET("/v1/telemetry/history", getTelemetryHistory(reader, hp))

				w := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?gateway_id=gw1&sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z", nil)
				r.ServeHTTP(w, req)

				if w.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
				}
				code, _ := parseErrorResponse(t, w.Body.Bytes())
				if code != tt.wantErrCode {
					t.Errorf("error code = %q, want %q", code, tt.wantErrCode)
				}
			})
		}
	})

	t.Run("nil_result_or_invalid_projection", func(t *testing.T) {
		t.Run("nil_result_returns_500", func(t *testing.T) {
			reader := &mockTelemetryHistoryReader{
				fn: func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					return nil, nil // nil without error
				},
			}
			r := gin.New()
			r.Use(func(c *gin.Context) {
				auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
				c.Next()
			})
			r.GET("/v1/telemetry/history", getTelemetryHistory(reader, hp))

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?gateway_id=gw1&sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z", nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "internal_error" {
				t.Errorf("error code = %q, want %q", code, "internal_error")
			}
		})

		t.Run("dto_mapping_failure_returns_500", func(t *testing.T) {
			reader := &mockTelemetryHistoryReader{
				fn: func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					// Invalid gateway_id triggers NewTelemetryHistoryResponseDTO error
					return &telemetry.TelemetryHistoryResult{
						GatewayID:  "invalid:gateway",
						SensorID:   "s1",
						From:       from,
						To:         to,
						Mode:       "raw",
						Resolution: "raw",
					}, nil
				},
			}
			r := gin.New()
			r.Use(func(c *gin.Context) {
				auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
				c.Next()
			})
			r.GET("/v1/telemetry/history", getTelemetryHistory(reader, hp))

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/telemetry/history?gateway_id=gw1&sensor_id=s1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z", nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "internal_error" {
				t.Errorf("error code = %q, want %q", code, "internal_error")
			}
		})
	})
}

func TestListDigitalTwins_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, tp := setupTestParsers(t)
	testActor := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	t.Run("success_dispatch_and_empty_items_preserved", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinListQuery

		reader := &mockTwinListReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinListResult{
					Items:      []digitaltwin.TwinSummary{},
					NextCursor: nil,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?limit=10", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.Limit != 10 {
			t.Errorf("received limit = %d, want 10", receivedQuery.Limit)
		}

		var resp TwinListResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Items == nil || len(resp.Items) != 0 || resp.NextCursor != nil {
			t.Errorf("unexpected items in response: %+v", resp)
		}
	})

	t.Run("query_parser_invalid_cursor", func(t *testing.T) {
		reader := &mockTwinListReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?after=invalid_cursor_without_prefix", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_cursor" {
			t.Errorf("error code = %q, want %q", code, "invalid_cursor")
		}
	})

	t.Run("domain_not_found_mapped", func(t *testing.T) {
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return nil, digitaltwin.ErrNotFound
			},
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "not_found" {
			t.Errorf("error code = %q, want %q", code, "not_found")
		}
	})
}

func TestGetDigitalTwinDetail_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, tp := setupTestParsers(t)
	testActor := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

	t.Run("success_dispatch_and_json_ld_content_type", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinDetailQuery

		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinDetailResult{
					ID:            "urn:ngsi-ld:Gateway:gw1",
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "Gateway 1",
					GatewayID:     "gw1",
					Relationships: []digitaltwin.RelationshipItem{},
					UpdatedAt:     updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		// JSON-LD is REQUIRED for detail
		if ct := w.Header().Get("Content-Type"); ct != "application/ld+json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/ld+json; charset=utf-8", ct)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.EntityID != "urn:ngsi-ld:Gateway:gw1" {
			t.Errorf("received entity_id = %q, want urn:ngsi-ld:Gateway:gw1", receivedQuery.EntityID)
		}
	})

	t.Run("invalid_entity_id_urn", func(t *testing.T) {
		reader := &mockTwinDetailReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/not-a-valid-urn", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		// Errors on detail must be application/json, not ld+json!
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("error Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_request" {
			t.Errorf("error code = %q, want %q", code, "invalid_request")
		}
	})

	t.Run("projection_error_returns_500", func(t *testing.T) {
		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				// Empty name triggers NewTwinDetailResponseDTO error
				return &digitaltwin.TwinDetailResult{
					ID:        "urn:ngsi-ld:Gateway:gw1",
					Type:      digitaltwin.EntityTypeGateway,
					Name:      "",
					GatewayID: "gw1",
					UpdatedAt: updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want %q", code, "internal_error")
		}
	})
}

func TestGetDigitalTwinState_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testActor := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

	t.Run("success_dispatch_and_jsonb_preservation", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinStateQuery

		reportedJSON := json.RawMessage(`{"sequence_num":9007199254740993,"value":25.2}`)
		desiredJSON := json.RawMessage(`{"sample_interval_us":5000000}`)

		reader := &mockTwinStateReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinStateResult{
					ID:              "urn:ngsi-ld:Sensor:gw1:s1",
					Type:            digitaltwin.EntityTypeSensor,
					GatewayID:       "gw1",
					ReportedState:   reportedJSON,
					DesiredState:    desiredJSON,
					ReportedVersion: 9007199254740993,
					DesiredVersion:  5,
					UpdatedAt:       updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/state", getDigitalTwinState(reader))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/state", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" {
			t.Errorf("received entity_id = %q, want urn:ngsi-ld:Sensor:gw1:s1", receivedQuery.EntityID)
		}

		var resp TwinStateResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal state response: %v", err)
		}
		if resp.ReportedVersion != "9007199254740993" {
			t.Errorf("reported_version = %q, want %q", resp.ReportedVersion, "9007199254740993")
		}
	})

	t.Run("query_parameters_rejected_on_state_endpoint", func(t *testing.T) {
		reader := &mockTwinStateReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/state", getDigitalTwinState(reader))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/state?some=query", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_request" {
			t.Errorf("error code = %q, want %q", code, "invalid_request")
		}
	})
}

func TestGetDigitalTwinHistory_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hp, _ := setupTestParsers(t)
	testActor := uuid.MustParse("55555555-5555-4555-8555-555555555555")

	from := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 0, 5, 0, time.UTC)
	unit := "degC"

	t.Run("success_dispatch_and_actor_forwarding", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinHistoryQuery

		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gw1:s1",
					Property:   digitaltwin.SelectorValue,
					Unit:       &unit,
					From:       from,
					To:         to,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:  "gw1",
						SensorID:   "s1",
						Unit:       &unit,
						From:       from,
						To:         to,
						Mode:       "raw",
						Resolution: "raw",
						PointCount: 1,
						RawPoints: []telemetry.RawSamplePoint{
							{
								MeasuredAt:  from,
								Value:       25.5,
								SequenceNum: 100,
							},
						},
					},
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/history", getDigitalTwinHistory(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/history?property=value&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z&resolution=raw&max_points=1000", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.Property != digitaltwin.SelectorValue {
			t.Errorf("received property = %q, want value", receivedQuery.Property)
		}

		var resp TwinHistoryResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal twin history response: %v", err)
		}
		if resp.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" || resp.Property != "value" {
			t.Errorf("unexpected DTO: %+v", resp)
		}
	})

	t.Run("query_parser_gateway_entity_rejected", func(t *testing.T) {
		reader := &mockTwinHistoryReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/history", getDigitalTwinHistory(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1/history?property=value&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_property" {
			t.Errorf("error code = %q, want %q", code, "invalid_property")
		}
	})

	t.Run("domain_inconsistent_unit_error", func(t *testing.T) {
		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				return nil, digitaltwin.ErrInconsistentUnit
			},
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/history", getDigitalTwinHistory(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/history?property=sample_interval_us&from=2026-08-21T10:00:00Z&to=2026-08-21T10:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "inconsistent_unit" {
			t.Errorf("error code = %q, want %q", code, "inconsistent_unit")
		}
	})
}

func TestReadError_MappingBranches(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name        string
		inputErr    error
		wantStatus  int
		wantErrCode string
	}{
		{
			name:        "nil_error_is_noop",
			inputErr:    nil,
			wantStatus:  http.StatusOK, // untouched
			wantErrCode: "",
		},
		{
			name:        "context_deadline_exceeded",
			inputErr:    context.DeadlineExceeded,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "context_canceled",
			inputErr:    context.Canceled,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "digitaltwin_dependency_unavailable",
			inputErr:    digitaltwin.ErrDependencyUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "telemetry_dependency_unavailable",
			inputErr:    telemetry.ErrHistoryDependency,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "gateway_provisioning_unavailable",
			inputErr:    gateway.ErrGatewayProvisioningUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "admin_or_owner_provisioning_unavailable",
			inputErr:    gateway.ErrAdminOrOwnerProvisioningUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "metadata_patch_unavailable",
			inputErr:    digitaltwin.ErrMetadataPatchUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "admin_lookup_failed",
			inputErr:    gateway.ErrAdminLookupFailed,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "inventory_unavailable",
			inputErr:    gateway.ErrInventoryUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "wrapped_dependency_and_not_found_precedence",
			inputErr:    errors.Join(digitaltwin.ErrNotFound, digitaltwin.ErrDependencyUnavailable),
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "wrapped_dependency_and_range_too_large_precedence",
			inputErr:    errors.Join(digitaltwin.ErrRangeTooLarge, telemetry.ErrHistoryDependency),
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "service_unavailable",
		},
		{
			name:        "query_error_custom_code",
			inputErr:    &QueryError{Code: "resolution_too_fine", Message: "fine", Err: telemetry.ErrHistoryResolutionTooFine},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "resolution_too_fine",
		},
		{
			name:        "digitaltwin_not_found",
			inputErr:    digitaltwin.ErrNotFound,
			wantStatus:  http.StatusNotFound,
			wantErrCode: "not_found",
		},
		{
			name:        "telemetry_not_found",
			inputErr:    telemetry.ErrHistoryNotFound,
			wantStatus:  http.StatusNotFound,
			wantErrCode: "not_found",
		},
		{
			name:        "gateway_not_found",
			inputErr:    gateway.ErrNotFound,
			wantStatus:  http.StatusNotFound,
			wantErrCode: "not_found",
		},
		{
			name:        "digitaltwin_forbidden",
			inputErr:    digitaltwin.ErrForbidden,
			wantStatus:  http.StatusForbidden,
			wantErrCode: "forbidden",
		},
		{
			name:        "digitaltwin_invalid_cursor",
			inputErr:    digitaltwin.ErrInvalidCursor,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "invalid_cursor",
		},
		{
			name:        "digitaltwin_invalid_property",
			inputErr:    digitaltwin.ErrInvalidProperty,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "invalid_property",
		},
		{
			name:        "digitaltwin_inconsistent_unit",
			inputErr:    digitaltwin.ErrInconsistentUnit,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "inconsistent_unit",
		},
		{
			name:        "digitaltwin_range_too_large",
			inputErr:    digitaltwin.ErrRangeTooLarge,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "range_too_large",
		},
		{
			name:        "digitaltwin_invalid_resolution",
			inputErr:    digitaltwin.ErrInvalidResolution,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "invalid_resolution",
		},
		{
			name:        "digitaltwin_raw_limit_exceeded",
			inputErr:    digitaltwin.ErrRawLimitExceeded,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "raw_limit_exceeded",
		},
		{
			name:        "digitaltwin_resolution_too_fine",
			inputErr:    digitaltwin.ErrResolutionTooFine,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "resolution_too_fine",
		},
		{
			name:        "digitaltwin_invalid_input",
			inputErr:    digitaltwin.ErrInvalidInput,
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "invalid_request",
		},
		{
			name:        "unexpected_error",
			inputErr:    errors.New("something totally unexpected"),
			wantStatus:  http.StatusInternalServerError,
			wantErrCode: "internal_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

			writeReadError(c, "test_operation", tt.inputErr)

			if tt.inputErr == nil {
				if w.Code != http.StatusOK {
					t.Fatalf("expected untouched status 200, got %d", w.Code)
				}
				return
			}

			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != tt.wantErrCode {
				t.Errorf("error code = %q, want %q", code, tt.wantErrCode)
			}
		})
	}

	t.Run("writeReadJSONResponse_nil_dto", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

		writeReadJSONResponse(c, "test_operation", http.StatusOK, nil)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want %q", code, "internal_error")
		}
	})

	t.Run("writeReadJSONLDResponse_nil_dto", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

		writeReadJSONLDResponse(c, "test_operation", http.StatusOK, nil)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want %q", code, "internal_error")
		}
	})

	t.Run("writeReadJSONResponse_marshal_failure", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

		// Passing an unmarshalable type (channel) triggers marshal failure
		writeReadJSONResponse(c, "test_operation", http.StatusOK, make(chan int))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want %q", code, "internal_error")
		}
	})

	t.Run("writeReadJSONLDResponse_marshal_failure", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/test", nil)

		// Passing an unmarshalable type (channel) triggers marshal failure
		writeReadJSONLDResponse(c, "test_operation", http.StatusOK, make(chan int))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want %q", code, "internal_error")
		}
	})
}
