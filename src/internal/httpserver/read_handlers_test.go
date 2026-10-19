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
			handler: getDigitalTwinDetail(&mockTwinDetailReader{}, &mockTwinStateReader{}, tp),
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

	t.Run("success_root_array_and_link_header", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinListQuery
		nextCursor := "v1.eyJrIjoidGVzdCJ9"
		reader := &mockTwinListReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinListResult{
					Items: []digitaltwin.TwinSummary{
						{
							ID:        "urn:ngsi-ld:Sensor:gw_01:s1",
							Type:      digitaltwin.EntityTypeSensor,
							Name:      ptr("Sensor 1"),
							GatewayID: "gw_01",
							UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
						},
						{
							ID:        "urn:ngsi-ld:Sensor:gw_01:s2",
							Type:      digitaltwin.EntityTypeSensor,
							Name:      ptr("Sensor 2"),
							GatewayID: "gw_01",
							UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
						},
					},
					NextCursor: &nextCursor,
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
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?type=Sensor&gateway_id=gw_01&limit=10", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/ld+json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/ld+json; charset=utf-8", ct)
		}
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.Limit != 10 {
			t.Errorf("received limit = %d, want 10", receivedQuery.Limit)
		}
		if receivedQuery.EntityType == nil || *receivedQuery.EntityType != digitaltwin.EntityTypeSensor {
			t.Errorf("received entity type = %v, want Sensor", receivedQuery.EntityType)
		}
		if receivedQuery.GatewayID == nil || *receivedQuery.GatewayID != "gw_01" {
			t.Errorf("received gateway id = %v, want gw_01", receivedQuery.GatewayID)
		}

		// Verify Link header
		wantLink := "</v1/digital-twins?type=Sensor&gateway_id=gw_01&limit=10&cursor=" + nextCursor + ">; rel=\"next\""
		if link := w.Header().Get("Link"); link != wantLink {
			t.Errorf("Link header = %q, want %q", link, wantLink)
		}
		if link := w.Header().Get("Link"); strings.Contains(link, "format=") {
			t.Errorf("normalized Link must not invent a format parameter: %q", link)
		}

		// Body must be root JSON array and not contain next_cursor
		if strings.Contains(w.Body.String(), "next_cursor") {
			t.Errorf("response body must not contain next_cursor: %s", w.Body.String())
		}
		var resp []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal root array response: %v (body: %s)", err, w.Body.String())
		}
		if len(resp) != 2 {
			t.Errorf("len(resp) = %d, want 2", len(resp))
		}
	})

	t.Run("success_empty_or_last_page_no_link", func(t *testing.T) {
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{
					Items:      nil,
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
		if link := w.Header().Get("Link"); link != "" {
			t.Errorf("Link header must be absent on empty/last page, got: %q", link)
		}
		if w.Body.String() != "[]" {
			t.Errorf("expected \"[]\", got %s", w.Body.String())
		}
	})

	t.Run("link_header_url_encoding", func(t *testing.T) {
		rawCursor := "v1.test+cursor/123=="
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{
					Items:      nil,
					NextCursor: &rawCursor,
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
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		wantLink := "</v1/digital-twins?limit=10&cursor=v1.test%2Bcursor%2F123%3D%3D>; rel=\"next\""
		if link := w.Header().Get("Link"); link != wantLink {
			t.Errorf("Link header = %q, want %q", link, wantLink)
		}
	})

	t.Run("reject_replaced_query_parameters", func(t *testing.T) {
		reader := &mockTwinListReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		for _, param := range []string{"entity_type=Sensor", "after=some_cursor"} {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?"+param, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("param %q: status = %d, want %d", param, w.Code, http.StatusBadRequest)
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "invalid_request" {
				t.Errorf("param %q: code = %q, want %q", param, code, "invalid_request")
			}
		}
	})

	t.Run("reject_duplicate_query_parameters", func(t *testing.T) {
		reader := &mockTwinListReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		for _, query := range []string{"type=Gateway&type=Sensor", "cursor=a&cursor=b"} {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?"+query, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("query %q: status = %d, want %d", query, w.Code, http.StatusBadRequest)
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "invalid_request" {
				t.Errorf("query %q: code = %q, want %q", query, code, "invalid_request")
			}
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
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?cursor=invalid_cursor_without_prefix", nil)
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

// TestListDigitalTwins_SimplifiedFormat locks the project simplified SUMMARY
// projection (?format=simplified): a flat root array, no JSON-LD wrappers, and
// JSON media negotiation. This is intentionally distinct from the detail
// simplified STATE view.
func TestListDigitalTwins_SimplifiedFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, tp := setupTestParsers(t)
	testActor := uuid.MustParse("33333333-3333-4333-8333-333333333333")

	buildRouter := func(reader *mockTwinListReader) *gin.Engine {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))
		return r
	}

	summaryItems := func() []digitaltwin.TwinSummary {
		return []digitaltwin.TwinSummary{
			{
				ID:        "urn:ngsi-ld:Gateway:gw_01",
				Type:      digitaltwin.EntityTypeGateway,
				Name:      ptr("Gateway 1"),
				GatewayID: "gw_01",
				UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
			},
			{
				ID:        "urn:ngsi-ld:Sensor:gw_01:s1",
				Type:      digitaltwin.EntityTypeSensor,
				Name:      nil,
				GatewayID: "gw_01",
				UpdatedAt: time.Date(2026, 10, 15, 16, 0, 0, 123000000, time.FixedZone("ICT", 7*60*60)),
			},
		}
	}

	decodeRawString := func(t *testing.T, raw json.RawMessage) string {
		t.Helper()
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("expected JSON string, got %s: %v", string(raw), err)
		}
		return s
	}

	t.Run("flat_summary_shape", func(t *testing.T) {
		var listCalls int
		var receivedQuery digitaltwin.TwinListQuery
		reader := &mockTwinListReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				listCalls++
				if actor != testActor {
					t.Errorf("actor = %v, want %v", actor, testActor)
				}
				receivedQuery = q
				return &digitaltwin.TwinListResult{Items: summaryItems()}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&type=Sensor&limit=10", nil)
		req.Header.Set("Accept", "application/json")
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
		if vary := w.Header().Get("Vary"); !strings.Contains(vary, "Accept") {
			t.Errorf("Vary = %q, want it to contain Accept", vary)
		}
		if listCalls != 1 {
			t.Errorf("ListTwins calls = %d, want 1", listCalls)
		}
		if receivedQuery.Limit != 10 {
			t.Errorf("received limit = %d, want 10", receivedQuery.Limit)
		}
		if receivedQuery.EntityType == nil || *receivedQuery.EntityType != digitaltwin.EntityTypeSensor {
			t.Errorf("received entity type = %v, want Sensor", receivedQuery.EntityType)
		}

		if strings.Contains(w.Body.String(), "@context") || strings.Contains(w.Body.String(), "items") {
			t.Errorf("simplified list must not contain JSON-LD context or items envelope: %s", w.Body.String())
		}

		var resp []map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal root array response: %v (body: %s)", err, w.Body.String())
		}
		if len(resp) != 2 {
			t.Fatalf("len(resp) = %d, want 2 (body: %s)", len(resp), w.Body.String())
		}

		allowedKeys := map[string]bool{
			"id": true, "type": true, "name": true, "gateway_id": true, "modifiedAt": true,
		}
		for i, item := range resp {
			for key := range item {
				if !allowedKeys[key] {
					t.Errorf("item %d has unexpected key %q (body: %s)", i, key, w.Body.String())
				}
			}
		}

		if got := decodeRawString(t, resp[0]["id"]); got != "urn:ngsi-ld:Gateway:gw_01" {
			t.Errorf("item 0 id = %q", got)
		}
		if got := decodeRawString(t, resp[0]["type"]); got != "Gateway" {
			t.Errorf("item 0 type = %q", got)
		}
		if got := decodeRawString(t, resp[0]["gateway_id"]); got != "gw_01" {
			t.Errorf("item 0 gateway_id = %q", got)
		}
		if got := decodeRawString(t, resp[0]["name"]); got != "Gateway 1" {
			t.Errorf("item 0 name = %q, want flat string", got)
		}
		if got := decodeRawString(t, resp[0]["modifiedAt"]); got != "2026-10-15T09:00:00.000000Z" {
			t.Errorf("item 0 modifiedAt = %q", got)
		}

		if _, ok := resp[1]["name"]; ok {
			t.Errorf("item 1 (nil name) must omit the name key, got: %s", string(resp[1]["name"]))
		}
		if got := decodeRawString(t, resp[1]["modifiedAt"]); got != "2026-10-15T09:00:00.123000Z" {
			t.Errorf("item 1 modifiedAt = %q, want UTC microsecond instant", got)
		}
	})

	t.Run("next_cursor_link_preserves_simplified_format_and_filters", func(t *testing.T) {
		nextCursor := "v1.eyJrIjoidGVzdCJ9+abc/def=="
		var receivedQuery digitaltwin.TwinListQuery
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				receivedQuery = q
				return &digitaltwin.TwinListResult{Items: summaryItems(), NextCursor: &nextCursor}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&type=Sensor&gateway_id=gw_01&limit=10", nil)
		req.Header.Set("Accept", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		// The relative next link must preserve format=simplified plus the active
		// filters and URL-encode the opaque cursor bytes.
		wantLink := "</v1/digital-twins?type=Sensor&gateway_id=gw_01&limit=10&format=simplified&cursor=v1.eyJrIjoidGVzdCJ9%2Babc%2Fdef%3D%3D>; rel=\"next\""
		if link := w.Header().Get("Link"); link != wantLink {
			t.Errorf("Link header = %q, want %q", link, wantLink)
		}
		if receivedQuery.EntityType == nil || *receivedQuery.EntityType != digitaltwin.EntityTypeSensor {
			t.Errorf("received entity type = %v, want Sensor", receivedQuery.EntityType)
		}
		if receivedQuery.GatewayID == nil || *receivedQuery.GatewayID != "gw_01" {
			t.Errorf("received gateway id = %v, want gw_01", receivedQuery.GatewayID)
		}
		if receivedQuery.After != nil {
			t.Errorf("received cursor = %v, want nil", receivedQuery.After)
		}
	})

	t.Run("simplified_last_page_has_no_link", func(t *testing.T) {
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: summaryItems(), NextCursor: nil}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&limit=10", nil)
		req.Header.Set("Accept", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if link := w.Header().Get("Link"); link != "" {
			t.Errorf("Link header must be absent on the last page, got: %q", link)
		}
	})

	t.Run("incoming_cursor_replaced_and_format_is_not_a_data_filter", func(t *testing.T) {
		gwID := "gw_01"
		sensorType := digitaltwin.EntityTypeSensor
		incomingCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s0", &gwID, &sensorType)
		if err != nil {
			t.Fatalf("failed to encode incoming cursor: %v", err)
		}
		nextCursor := "v1.eyJrIjoibmV4dCJ9"
		var receivedQuery digitaltwin.TwinListQuery
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				receivedQuery = q
				return &digitaltwin.TwinListResult{Items: summaryItems(), NextCursor: &nextCursor}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&type=Sensor&gateway_id=gw_01&limit=10&cursor="+incomingCursor, nil)
		req.Header.Set("Accept", "application/json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		// Cursor bytes and filter binding are unchanged: format never enters the
		// domain query or the cursor binding.
		if receivedQuery.After == nil || *receivedQuery.After != incomingCursor {
			t.Errorf("received cursor = %v, want %q passed through unchanged", receivedQuery.After, incomingCursor)
		}
		wantLink := "</v1/digital-twins?type=Sensor&gateway_id=gw_01&limit=10&format=simplified&cursor=" + nextCursor + ">; rel=\"next\""
		if link := w.Header().Get("Link"); link != wantLink {
			t.Errorf("Link header = %q, want %q", link, wantLink)
		}
		if link := w.Header().Get("Link"); strings.Contains(link, "cursor="+incomingCursor) {
			t.Errorf("Link must replace the request cursor, not append it: %q", link)
		}
	})

	t.Run("relative_link_ignores_hostile_host_and_forwarded", func(t *testing.T) {
		nextCursor := "v1.next"
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: summaryItems(), NextCursor: &nextCursor}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&limit=10", nil)
		req.Host = "evil.example"
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Forwarded-Host", "evil.example")
		req.Header.Set("Forwarded", "host=evil.example;proto=http")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		link := w.Header().Get("Link")
		wantLink := "</v1/digital-twins?limit=10&format=simplified&cursor=v1.next>; rel=\"next\""
		if link != wantLink {
			t.Errorf("Link header = %q, want %q", link, wantLink)
		}
		if strings.Contains(strings.ToLower(link), "evil.example") || strings.Contains(link, "http") {
			t.Errorf("Link must stay relative and never derive from Host/Forwarded: %q", link)
		}
	})

	t.Run("cors_exposes_link_and_keeps_no_store_and_vary", func(t *testing.T) {
		nextCursor := "v1.next"
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: summaryItems(), NextCursor: &nextCursor}, nil
			},
		}
		const origin = "https://app.example.com"
		r := gin.New()
		r.Use(corsMiddleware([]string{origin}))
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins", listDigitalTwins(reader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified&limit=10", nil)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Origin", origin)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, origin)
		}
		if got := w.Header().Get("Access-Control-Expose-Headers"); got != "Link, Location, X-Request-ID" {
			t.Errorf("Access-Control-Expose-Headers = %q, want Link, Location, X-Request-ID", got)
		}
		vary := w.Header().Get("Vary")
		if !strings.Contains(vary, "Accept") || !strings.Contains(vary, "Origin") {
			t.Errorf("Vary = %q, want it to contain Accept and Origin", vary)
		}
		if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		if link := w.Header().Get("Link"); link == "" {
			t.Error("Link header must be present when NextCursor is set")
		}
	})

	t.Run("empty_result_is_empty_array", func(t *testing.T) {
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: nil}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
		}
		if body := w.Body.String(); body != "[]" {
			t.Errorf("body = %s, want [] (not null)", body)
		}
	})

	t.Run("invalid_identity_fails_closed", func(t *testing.T) {
		cases := []struct {
			name string
			item digitaltwin.TwinSummary
		}{
			{
				name: "invalid_urn",
				item: digitaltwin.TwinSummary{
					ID:        "not-a-urn",
					Type:      digitaltwin.EntityTypeSensor,
					GatewayID: "gw_01",
					UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
				},
			},
			{
				name: "type_mismatch",
				item: digitaltwin.TwinSummary{
					ID:        "urn:ngsi-ld:Sensor:gw_01:s1",
					Type:      digitaltwin.EntityTypeGateway,
					GatewayID: "gw_01",
					UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
				},
			},
			{
				name: "gateway_mismatch",
				item: digitaltwin.TwinSummary{
					ID:        "urn:ngsi-ld:Sensor:gw_01:s1",
					Type:      digitaltwin.EntityTypeSensor,
					GatewayID: "gw_02",
					UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				reader := &mockTwinListReader{
					fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
						return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{tc.item}}, nil
					},
				}
				r := buildRouter(reader)

				w := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified", nil)
				r.ServeHTTP(w, req)

				if w.Code != http.StatusInternalServerError {
					t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusInternalServerError, w.Body.String())
				}
				if link := w.Header().Get("Link"); link != "" {
					t.Errorf("no partial success: Link must be absent when projection fails, got %q", link)
				}
				code, _ := parseErrorResponse(t, w.Body.Bytes())
				if code != "internal_error" {
					t.Errorf("error code = %q, want internal_error", code)
				}
			})
		}
	})

	t.Run("invalid_timestamp_fails_closed", func(t *testing.T) {
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{
					{
						ID:        "urn:ngsi-ld:Gateway:gw_01",
						Type:      digitaltwin.EntityTypeGateway,
						GatewayID: "gw_01",
						UpdatedAt: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				}}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?format=simplified", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusInternalServerError, w.Body.String())
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("error code = %q, want internal_error", code)
		}
	})

	t.Run("normalized_default_unchanged", func(t *testing.T) {
		var listCalls int
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				listCalls++
				return &digitaltwin.TwinListResult{Items: summaryItems()}, nil
			},
		}
		r := buildRouter(reader)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?limit=10", nil)
		req.Header.Set("Accept", "application/ld+json")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/ld+json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/ld+json; charset=utf-8", ct)
		}
		if listCalls != 1 {
			t.Errorf("ListTwins calls = %d, want 1", listCalls)
		}

		var resp []map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal normalized response: %v (body: %s)", err, w.Body.String())
		}
		if len(resp) != 2 {
			t.Fatalf("len(resp) = %d, want 2", len(resp))
		}
		if _, ok := resp[0]["@context"]; !ok {
			t.Errorf("normalized item must keep @context (body: %s)", w.Body.String())
		}
		var gwProp map[string]any
		if err := json.Unmarshal(resp[0]["gateway_id"], &gwProp); err != nil {
			t.Fatalf("normalized gateway_id must be a Property object: %v", err)
		}
		if gwProp["type"] != "Property" {
			t.Errorf("normalized gateway_id type = %v, want Property", gwProp["type"])
		}
		var nameProp map[string]any
		if err := json.Unmarshal(resp[0]["name"], &nameProp); err != nil {
			t.Fatalf("normalized name must decode: %v", err)
		}
		if nameProp["type"] != "Property" {
			t.Errorf("normalized name must remain a Property wrapper, got: %s", string(resp[0]["name"]))
		}
	})

	t.Run("accept_negotiation", func(t *testing.T) {
		cases := []struct {
			name       string
			path       string
			accept     string
			wantStatus int
			wantCT     string
		}{
			{
				name:       "simplified absent Accept serves JSON",
				path:       "/v1/digital-twins?format=simplified",
				wantStatus: http.StatusOK,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified */* serves JSON",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "*/*",
				wantStatus: http.StatusOK,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified application/* serves JSON",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "application/*",
				wantStatus: http.StatusOK,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified exact JSON serves JSON",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "application/json",
				wantStatus: http.StatusOK,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified positive q serves JSON",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "application/json;q=0.5",
				wantStatus: http.StatusOK,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified ld+json is incompatible",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "application/ld+json",
				wantStatus: http.StatusNotAcceptable,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified text/html is incompatible",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "text/html, text/plain",
				wantStatus: http.StatusNotAcceptable,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified q=0 exclusion returns 406",
				path:       "/v1/digital-twins?format=simplified",
				accept:     "application/json;q=0",
				wantStatus: http.StatusNotAcceptable,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "simplified extended q* overriding q=0 returns 406",
				path:       "/v1/digital-twins?format=simplified",
				accept:     `application/json;q=0;q*=utf-8''1`,
				wantStatus: http.StatusNotAcceptable,
				wantCT:     "application/json; charset=utf-8",
			},
			{
				name:       "normalized still rejects JSON-only",
				path:       "/v1/digital-twins?type=Gateway&limit=10",
				accept:     "application/json",
				wantStatus: http.StatusNotAcceptable,
				wantCT:     "application/json; charset=utf-8",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var listCalls int
				reader := &mockTwinListReader{
					fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
						listCalls++
						return &digitaltwin.TwinListResult{Items: summaryItems()}, nil
					},
				}
				r := buildRouter(reader)

				w := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, tc.path, nil)
				if tc.accept != "" {
					req.Header.Set("Accept", tc.accept)
				}
				r.ServeHTTP(w, req)

				if w.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
				}
				if ct := w.Header().Get("Content-Type"); ct != tc.wantCT {
					t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
				}
				if vary := w.Header().Get("Vary"); !strings.Contains(vary, "Accept") {
					t.Errorf("Vary = %q, want it to contain Accept", vary)
				}
				wantCalls := 1
				if tc.wantStatus != http.StatusOK {
					wantCalls = 0
				}
				if listCalls != wantCalls {
					t.Errorf("ListTwins calls = %d, want %d", listCalls, wantCalls)
				}
			})
		}
	})

	t.Run("rejects_unsupported_or_repeated_format", func(t *testing.T) {
		var listCalls int
		reader := &mockTwinListReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
				listCalls++
				return &digitaltwin.TwinListResult{Items: summaryItems()}, nil
			},
		}
		r := buildRouter(reader)

		for _, query := range []string{
			"format=detail",
			"format=",
			"format=simplified&format=simplified",
		} {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins?"+query, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("query %q: status = %d, want %d (body: %s)", query, w.Code, http.StatusBadRequest, w.Body.String())
			}
			code, _ := parseErrorResponse(t, w.Body.Bytes())
			if code != "invalid_request" {
				t.Errorf("query %q: code = %q, want invalid_request", query, code)
			}
		}
		if listCalls != 0 {
			t.Errorf("ListTwins calls = %d, want 0 for rejected formats", listCalls)
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
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

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
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

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
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

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

	t.Run("format_simplified_dispatches_to_state_reader", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinStateQuery

		stateReader := &mockTwinStateReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinStateResult{
					ID:              "urn:ngsi-ld:Gateway:gw1",
					Type:            digitaltwin.EntityTypeGateway,
					GatewayID:       "gw1",
					ReportedState:   json.RawMessage(`{"status":"online"}`),
					DesiredState:    json.RawMessage(`{}`),
					ReportedVersion: 1,
					DesiredVersion:  0,
					UpdatedAt:       updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(&mockTwinDetailReader{}, stateReader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1?format=simplified", nil)
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
		if receivedQuery.EntityID != "urn:ngsi-ld:Gateway:gw1" {
			t.Errorf("received entity_id = %q, want urn:ngsi-ld:Gateway:gw1", receivedQuery.EntityID)
		}
		if strings.Contains(w.Body.String(), "@context") {
			t.Errorf("state response must not contain @context: %s", w.Body.String())
		}
	})

	t.Run("format_invalid_rejected_400", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(&mockTwinDetailReader{}, &mockTwinStateReader{}, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1?format=invalid", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", code)
		}
	})

	t.Run("format_simplified_mixed_with_relationship_limit_rejected_400", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(&mockTwinDetailReader{}, &mockTwinStateReader{}, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1?format=simplified&relationship_limit=10", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "invalid_request" {
			t.Errorf("code = %q, want invalid_request", code)
		}
	})

	t.Run("gateway_detail_with_next_cursor_emits_link_header", func(t *testing.T) {
		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				return &digitaltwin.TwinDetailResult{
					ID:            "urn:ngsi-ld:Gateway:gw1",
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "Gateway 1",
					GatewayID:     "gw1",
					Relationships: []digitaltwin.RelationshipItem{{TargetURN: "urn:ngsi-ld:Sensor:gw1:s1"}},
					RelationshipPage: &digitaltwin.RelationshipPage{
						Limit:      10,
						Count:      1,
						NextCursor: ptr("v1.nextcursor123"),
					},
					UpdatedAt: updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1?relationship_limit=10", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		link := w.Header().Get("Link")
		expectedLink := `</v1/digital-twins/urn:ngsi-ld:Gateway:gw1?relationship_limit=10&relationship_after=v1.nextcursor123>; rel="next"`
		if link != expectedLink {
			t.Errorf("Link header = %q, want %q", link, expectedLink)
		}
	})

	t.Run("gateway_detail_terminal_page_no_link_header", func(t *testing.T) {
		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				return &digitaltwin.TwinDetailResult{
					ID:            "urn:ngsi-ld:Gateway:gw1",
					Type:          digitaltwin.EntityTypeGateway,
					Name:          "Gateway 1",
					GatewayID:     "gw1",
					Relationships: []digitaltwin.RelationshipItem{{TargetURN: "urn:ngsi-ld:Sensor:gw1:s1"}},
					RelationshipPage: &digitaltwin.RelationshipPage{
						Limit:      10,
						Count:      1,
						NextCursor: nil,
					},
					UpdatedAt: updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		if link := w.Header().Get("Link"); link != "" {
			t.Errorf("expected no Link header on terminal page, got: %q", link)
		}
	})

	t.Run("sensor_detail_calls_both_readers_under_same_context", func(t *testing.T) {
		detailCalled := false
		stateCalled := false

		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, actor uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				detailCalled = true
				if actor != testActor {
					t.Errorf("detail actor = %v, want %v", actor, testActor)
				}
				return &digitaltwin.TwinDetailResult{
					ID:        "urn:ngsi-ld:Sensor:gw1:s1",
					Type:      digitaltwin.EntityTypeSensor,
					Name:      "Sensor 1",
					Unit:      ptr("degC"),
					GatewayID: "gw1",
					UpdatedAt: updatedAt,
				}, nil
			},
		}

		stateReader := &mockTwinStateReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				stateCalled = true
				if actor != testActor {
					t.Errorf("state actor = %v, want %v", actor, testActor)
				}
				if q.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" {
					t.Errorf("state entity_id = %q, want urn:ngsi-ld:Sensor:gw1:s1", q.EntityID)
				}
				return &digitaltwin.TwinStateResult{
					ID:              "urn:ngsi-ld:Sensor:gw1:s1",
					Type:            digitaltwin.EntityTypeSensor,
					GatewayID:       "gw1",
					ReportedState:   json.RawMessage(`{"value":25.4,"observed_at":"2026-10-18T00:00:01.000000Z","sample_interval_us":10000.0}`),
					ReportedVersion: 1,
					UpdatedAt:       updatedAt,
				}, nil
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, stateReader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
		}
		if !detailCalled || !stateCalled {
			t.Errorf("expected both readers called: detail=%v, state=%v", detailCalled, stateCalled)
		}
		var m map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &m)
		if m["isHostedBy"] == nil {
			t.Error("missing isHostedBy in Sensor detail response")
		}
		if m["measurement"] == nil {
			t.Error("missing measurement in Sensor detail response")
		}
		if m["sample_interval_us"] == nil {
			t.Error("missing sample_interval_us in Sensor detail response")
		}
	})

	t.Run("sensor_detail_fails_if_state_reader_fails", func(t *testing.T) {
		reader := &mockTwinDetailReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				return &digitaltwin.TwinDetailResult{
					ID:        "urn:ngsi-ld:Sensor:gw1:s1",
					Type:      digitaltwin.EntityTypeSensor,
					Name:      "Sensor 1",
					Unit:      ptr("degC"),
					GatewayID: "gw1",
					UpdatedAt: updatedAt,
				}, nil
			},
		}

		stateReader := &mockTwinStateReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				return nil, digitaltwin.ErrUnexpected
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, stateReader, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 when state reader fails", w.Code)
		}
	})

	t.Run("nil_dependencies_fail_closed", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(nil, nil, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 for nil dependency", w.Code)
		}
	})

	t.Run("slow_first_reader_consumes_second_reader_budget", func(t *testing.T) {
		shortTimeoutParser, err := NewTwinQueryParser(&tp.cfg, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to create parser: %v", err)
		}

		reader := &mockTwinDetailReader{
			fn: func(ctx context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				// First reader consumes 40ms of the 50ms budget
				select {
				case <-time.After(40 * time.Millisecond):
					return &digitaltwin.TwinDetailResult{
						ID:        "urn:ngsi-ld:Sensor:gw1:s1",
						Type:      digitaltwin.EntityTypeSensor,
						Name:      "Sensor 1",
						Unit:      ptr("degC"),
						GatewayID: "gw1",
						UpdatedAt: updatedAt,
					}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		stateReader := &mockTwinStateReader{
			fn: func(ctx context.Context, _ uuid.UUID, _ digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
				// Second reader tries to take 30ms.
				// Total time would be 40ms + 30ms = 70ms > 50ms total budget.
				// If readers share the budget, ctx.Done() will fire here and return error.
				select {
				case <-time.After(30 * time.Millisecond):
					return &digitaltwin.TwinStateResult{
						ID:              "urn:ngsi-ld:Sensor:gw1:s1",
						Type:            digitaltwin.EntityTypeSensor,
						GatewayID:       "gw1",
						ReportedState:   json.RawMessage(`{"value":25.4,"observed_at":"2026-10-18T00:00:01.000000Z"}`),
						ReportedVersion: 1,
						UpdatedAt:       updatedAt,
					}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, stateReader, shortTimeoutParser))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable when budget is exhausted across readers, got %d (body: %s)", w.Code, w.Body.String())
		}
	})

	t.Run("earlier_request_deadline_is_preserved", func(t *testing.T) {
		reader := &mockTwinDetailReader{
			fn: func(ctx context.Context, _ uuid.UUID, _ digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
				select {
				case <-time.After(30 * time.Millisecond):
					return &digitaltwin.TwinDetailResult{
						ID:        "urn:ngsi-ld:Sensor:gw1:s1",
						Type:      digitaltwin.EntityTypeSensor,
						Name:      "Sensor 1",
						Unit:      ptr("degC"),
						GatewayID: "gw1",
						UpdatedAt: updatedAt,
					}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id", getDigitalTwinDetail(reader, &mockTwinStateReader{}, tp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1", nil)
		// Tight 10ms deadline on request context
		ctxWithDeadline, cancel := context.WithTimeout(req.Context(), 10*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctxWithDeadline)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable when request deadline expires, got %d (body: %s)", w.Code, w.Body.String())
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

func TestGetDigitalTwinTemporal_Handler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hCfg := config.DefaultHistoryConfig()
	hp, err := NewHistoryQueryParser(&hCfg)
	if err != nil {
		t.Fatalf("NewHistoryQueryParser failed: %v", err)
	}
	testActor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")

	from := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 0, 0, 5, 0, time.UTC)
	_ = to
	unit := "degC"

	t.Run("success_raw_measurement_and_clean_domain_query", func(t *testing.T) {
		var receivedActor uuid.UUID
		var receivedQuery digitaltwin.TwinHistoryQuery

		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				receivedActor = actor
				receivedQuery = q
				return &digitaltwin.TwinHistoryResult{
					EntityID:   q.EntityID,
					Property:   q.Property,
					Unit:       &unit,
					From:       q.Range.From,
					To:         q.Range.To,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:  "gw1",
						SensorID:   "s1",
						Unit:       &unit,
						From:       q.Range.From,
						To:         q.Range.To,
						Mode:       "raw",
						Resolution: "raw",
						PointCount: 1,
						RawPoints: []telemetry.RawSamplePoint{
							{MeasuredAt: from, Value: 25.4, SequenceNum: 100},
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
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/ld+json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want application/ld+json; charset=utf-8", ct)
		}
		if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}

		// Verify fake reader received clean domain query
		if receivedActor != testActor {
			t.Errorf("received actor = %v, want %v", receivedActor, testActor)
		}
		if receivedQuery.Property != digitaltwin.SelectorValue {
			t.Errorf("received property = %q, want value", receivedQuery.Property)
		}
		if receivedQuery.Resolution != telemetry.ResolutionRaw {
			t.Errorf("received resolution = %q, want raw (never auto!)", receivedQuery.Resolution)
		}
		if receivedQuery.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" {
			t.Errorf("received entityID = %q, want urn:ngsi-ld:Sensor:gw1:s1", receivedQuery.EntityID)
		}

		var parsed map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &parsed); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}
		if parsed["id"] != "urn:ngsi-ld:Sensor:gw1:s1" || parsed["type"] != "Sensor" {
			t.Errorf("unexpected entity identity: %v, %v", parsed["id"], parsed["type"])
		}
		meas, ok := parsed["measurement"].(map[string]any)
		if !ok {
			t.Fatalf("measurement property is missing: %v", parsed)
		}
		values, ok := meas["values"].([]any)
		if !ok || len(values) != 1 {
			t.Fatalf("values length = %d, want 1: %v", len(values), meas["values"])
		}
	})

	t.Run("success_bucket_measurement_with_projection", func(t *testing.T) {
		dur := time.Minute
		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, _ uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   q.EntityID,
					Property:   q.Property,
					From:       q.Range.From,
					To:         q.Range.To,
					Mode:       "bucket",
					Resolution: "1m",
					PointCount: 1,
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:  "gw1",
						SensorID:   "s1",
						From:       q.Range.From,
						To:         q.Range.To,
						Mode:       "bucket",
						Resolution: "1m",
						PointCount: 1,
						BucketPoints: []telemetry.BucketSamplePoint{
							{BucketStart: from, Avg: 25.5, Min: 25.1, Max: 25.9, Count: 60},
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
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:05:00Z&aggrPeriodDuration=PT1M&aggrMethods=avg,totalCount", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		rawJSON := w.Body.String()
		if !strings.Contains(rawJSON, `"avg"`) || !strings.Contains(rawJSON, `"totalCount"`) {
			t.Errorf("expected avg and totalCount in JSON: %s", rawJSON)
		}
		if strings.Contains(rawJSON, `"min"`) || strings.Contains(rawJSON, `"max"`) {
			t.Errorf("min and max must be omitted: %s", rawJSON)
		}
		_ = dur
	})

	t.Run("unauthorized_missing_token", func(t *testing.T) {
		reader := &mockTwinHistoryReader{}
		r := gin.New()
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
		}
	})

	t.Run("non_member_not_found", func(t *testing.T) {
		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				return nil, telemetry.ErrHistoryNotFound
			},
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("query_validation_error_invalid_timerel", func(t *testing.T) {
		reader := &mockTwinHistoryReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=before&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("raw_limit_exceeded_error", func(t *testing.T) {
		reader := &mockTwinHistoryReader{}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:15Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseErrorResponse(t, w.Body.Bytes())
		if code != "raw_limit_exceeded" {
			t.Errorf("error code = %q, want raw_limit_exceeded", code)
		}
	})

	t.Run("domain_timeout_503", func(t *testing.T) {
		reader := &mockTwinHistoryReader{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
				return nil, context.DeadlineExceeded
			},
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: testActor, Role: "authenticated"})
			c.Next()
		})
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
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
		r.GET("/v1/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(reader, hp))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw1:s1/temporal?attrs=sample_interval_us&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:05Z", nil)
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
