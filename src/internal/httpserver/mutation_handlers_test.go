package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	"iot-platform/internal/httpapi"
)

func TestMutationHandlers_ErrorLogsExcludeRawCause(t *testing.T) {
	// The default logger is process-wide; this test and its subtests must remain serial.
	originalLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(originalLogger) })
	const marker = "private-database-error-marker"
	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		for _, tc := range []struct {
			name   string
			cause  error
			status int
			code   string
		}{
			{"unexpected", errors.New("database failure"), http.StatusInternalServerError, "internal_error"},
			{"unavailable", digitaltwin.ErrMetadataPatchUnavailable, http.StatusServiceUnavailable, "service_unavailable"},
			{"forbidden", gateway.ErrProvisioningForbidden, http.StatusForbidden, "forbidden"},
		} {
			t.Run(method+"_"+tc.name, func(t *testing.T) {
				var logs bytes.Buffer
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				cause := fmt.Errorf("%s: %w", marker, tc.cause)
				creator := &mockEntityCreator{fn: func(context.Context, uuid.UUID, digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
					return digitaltwin.CreateEntityResult{}, cause
				}}
				patcher := &mockGatewayPatcher{fn: func(context.Context, uuid.UUID, string, digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
					return digitaltwin.PatchGatewayResult{}, cause
				}}
				gin.SetMode(gin.TestMode)
				router := gin.New()
				router.Use(httpapi.RequestIDMiddleware(), func(c *gin.Context) {
					auth.SetPrincipal(c, auth.Principal{UserID: uuid.New()})
					c.Next()
				})
				router.POST("/v1/digital-twins", postDigitalTwin(creator, testBodyLimit))
				router.PATCH("/v1/digital-twins/:entity_id", patchDigitalTwin(patcher, &mockSensorPatcher{}, testBodyLimit))
				path, body, operation := "/v1/digital-twins", `{"entity_type":"Gateway","gateway_id":"gw-01","name":"Gateway","owner_user_id":"`+uuid.NewString()+`"}`, "post_digital_twin"
				if method == http.MethodPatch {
					path, body, operation = "/v1/digital-twins/urn:ngsi-ld:Gateway:gw-01", `{"name":"Updated Gateway"}`, "patch_digital_twin"
				}
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Request-ID", "req-safe-mutation-error")
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				code, _ := parseMutationErrorResponse(t, rec.Body.Bytes())
				if rec.Code != tc.status || code != tc.code {
					t.Fatalf("expected %d/%s, got %d/%s", tc.status, tc.code, rec.Code, code)
				}
				if strings.Contains(logs.String(), marker) || strings.Contains(rec.Body.String(), marker) {
					t.Fatal("raw error cause appeared in logs or response")
				}
				var entry map[string]any
				if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
					t.Fatalf("expected one structured log entry: %v", err)
				}
				if entry["request_id"] != "req-safe-mutation-error" || entry["operation"] != operation || entry["status"] != float64(tc.status) || entry["error_code"] != tc.code {
					t.Fatal("safe diagnostic fields missing or incorrect")
				}
				for key := range entry {
					switch key {
					case "time", "level", "msg", "request_id", "operation", "status", "error_code":
					default:
						t.Fatalf("unexpected diagnostic field %q", key)
					}
				}
			})
		}
	}
}

type mockEntityCreator struct {
	fn func(ctx context.Context, actor uuid.UUID, input digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error)
}

func (m *mockEntityCreator) CreateEntity(ctx context.Context, actor uuid.UUID, input digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, input)
	}
	return digitaltwin.CreateEntityResult{}, nil
}

type mockGatewayPatcher struct {
	fn func(ctx context.Context, actor uuid.UUID, gatewayID string, input digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error)
}

func (m *mockGatewayPatcher) PatchGateway(ctx context.Context, actor uuid.UUID, gatewayID string, input digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, gatewayID, input)
	}
	return digitaltwin.PatchGatewayResult{}, nil
}

type mockSensorPatcher struct {
	fn func(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error)
}

func (m *mockSensorPatcher) PatchSensor(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
	if m.fn != nil {
		return m.fn(ctx, actor, gatewayID, sensorID, input)
	}
	return digitaltwin.PatchSensorResult{}, nil
}

const testBodyLimit = int64(16384)

func parseMutationErrorResponse(t *testing.T, body []byte) (code string, message string) {
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

func setupMutationTestRouter(creator EntityCreator, gwPatcher GatewayPatcher, sensorPatcher SensorPatcher, limit int64, principal *auth.Principal) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if principal != nil {
		r.Use(func(c *gin.Context) {
			auth.SetPrincipal(c, *principal)
			c.Next()
		})
	}
	r.POST("/v1/digital-twins", postDigitalTwin(creator, limit))
	r.PATCH("/v1/digital-twins/:entity_id", patchDigitalTwin(gwPatcher, sensorPatcher, limit))
	return r
}

func TestMutationHandlers_AuthenticationRequired(t *testing.T) {
	creator := &mockEntityCreator{}
	gwPatcher := &mockGatewayPatcher{}
	sensorPatcher := &mockSensorPatcher{}

	cases := []struct {
		name   string
		method string
		url    string
		body   string
	}{
		{
			name:   "post_digital_twins_missing_principal",
			method: http.MethodPost,
			url:    "/v1/digital-twins",
			body:   `{"entity_type":"Gateway","gateway_id":"gw_01","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
		},
		{
			name:   "patch_digital_twins_missing_principal",
			method: http.MethodPatch,
			url:    "/v1/digital-twins/urn:ngsi-ld:Gateway:gw_01",
			body:   `{"name":"Renamed GW"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := setupMutationTestRouter(creator, gwPatcher, sensorPatcher, testBodyLimit, nil)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
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
			if w.Header().Get("Pragma") != "no-cache" {
				t.Fatalf("Pragma = %q, want no-cache", w.Header().Get("Pragma"))
			}
			code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
			if code != "unauthorized" {
				t.Errorf("code = %q, want %q", code, "unauthorized")
			}
		})

		t.Run(tc.name+"_nil_principal_user_id", func(t *testing.T) {
			nilPrincipal := &auth.Principal{UserID: uuid.Nil, Role: "authenticated"}
			r := setupMutationTestRouter(creator, gwPatcher, sensorPatcher, testBodyLimit, nilPrincipal)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusUnauthorized)
			}
			code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
			if code != "unauthorized" {
				t.Errorf("code = %q, want %q", code, "unauthorized")
			}
		})
	}
}

func TestMutationHandlers_NilDependency(t *testing.T) {
	adminID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	principal := &auth.Principal{UserID: adminID, Role: "authenticated"}

	t.Run("post_digital_twins_nil_creator", func(t *testing.T) {
		r := setupMutationTestRouter(nil, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(`{"entity_type":"Gateway","gateway_id":"gw_01","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
			t.Errorf("expected no-store and no-cache headers")
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "service_unavailable" {
			t.Errorf("code = %q, want %q", code, "service_unavailable")
		}
	})

	t.Run("patch_digital_twins_nil_gw_patcher", func(t *testing.T) {
		r := setupMutationTestRouter(&mockEntityCreator{}, nil, &mockSensorPatcher{}, testBodyLimit, principal)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw_01", strings.NewReader(`{"name":"GW"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "service_unavailable" {
			t.Errorf("code = %q, want %q", code, "service_unavailable")
		}
	})

	t.Run("patch_digital_twins_nil_sensor_patcher", func(t *testing.T) {
		r := setupMutationTestRouter(&mockEntityCreator{}, &mockGatewayPatcher{}, nil, testBodyLimit, principal)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw_01:s_01", strings.NewReader(`{"name":"Sensor"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "service_unavailable" {
			t.Errorf("code = %q, want %q", code, "service_unavailable")
		}
	})
}

func TestPostDigitalTwin_GatewayCreateAndReplay(t *testing.T) {
	adminID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ownerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	principal := &auth.Principal{UserID: adminID, Role: "authenticated"}

	fixedTime := time.Date(2026, 10, 18, 12, 0, 0, 123456000, time.UTC)
	desc := "Production gateway for lab A"

	for _, tc := range []struct {
		name       string
		created    bool
		wantStatus int
	}{
		{name: "gateway_create_201", created: true, wantStatus: http.StatusCreated},
		{name: "gateway_replay_200", created: false, wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedActor uuid.UUID
			var capturedInput digitaltwin.CreateEntityInput

			creator := &mockEntityCreator{
				fn: func(_ context.Context, actor uuid.UUID, input digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
					capturedActor = actor
					capturedInput = input
					return digitaltwin.CreateEntityResult{
						Created:  tc.created,
						Type:     digitaltwin.EntityTypeGateway,
						EntityID: "urn:ngsi-ld:Gateway:gw_lab_01",
						Gateway: &gateway.ProvisionGatewayResult{
							Created:     tc.created,
							GatewayID:   "gw_lab_01",
							Name:        "Lab Gateway",
							Description: &desc,
							OwnerUserID: ownerID,
							EntityID:    "urn:ngsi-ld:Gateway:gw_lab_01",
							CreatedAt:   &fixedTime,
						},
					}, nil
				},
			}

			r := setupMutationTestRouter(creator, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

			payload := map[string]any{
				"entity_type":   "Gateway",
				"gateway_id":    "gw_lab_01",
				"name":          "Lab Gateway",
				"description":   desc,
				"owner_user_id": ownerID.String(),
			}
			bodyBytes, _ := json.Marshal(payload)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Errorf("Content-Type = %q, want application/json; charset=utf-8", w.Header().Get("Content-Type"))
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Errorf("missing expected cache headers")
			}

			// Verify actor propagated accurately
			if capturedActor != adminID {
				t.Errorf("captured actor = %s, want %s", capturedActor, adminID)
			}
			if capturedInput.Type != digitaltwin.EntityTypeGateway {
				t.Errorf("captured type = %s, want %s", capturedInput.Type, digitaltwin.EntityTypeGateway)
			}
			if capturedInput.Gateway.GatewayID != "gw_lab_01" || capturedInput.Gateway.Name != "Lab Gateway" || *capturedInput.Gateway.Description != desc || capturedInput.Gateway.OwnerUserID != ownerID {
				t.Errorf("captured input mismatch: %+v", capturedInput.Gateway)
			}

			var resp PostGatewayResponseDTO
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp.GatewayID != "gw_lab_01" || resp.Name != "Lab Gateway" || resp.EntityID != "urn:ngsi-ld:Gateway:gw_lab_01" || *resp.Description != desc || resp.OwnerUserID != ownerID {
				t.Errorf("response DTO mismatch: %+v", resp)
			}
			if resp.CreatedAt == nil || *resp.CreatedAt != "2026-10-18T12:00:00.123456Z" {
				t.Errorf("CreatedAt = %v, want 2026-10-18T12:00:00.123456Z", resp.CreatedAt)
			}
		})
	}
}

func TestPostDigitalTwin_SensorCreateAndReplay(t *testing.T) {
	actorID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	principal := &auth.Principal{UserID: actorID, Role: "authenticated"}

	fixedTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	unit := "degC"

	for _, tc := range []struct {
		name       string
		created    bool
		wantStatus int
	}{
		{name: "sensor_create_201", created: true, wantStatus: http.StatusCreated},
		{name: "sensor_replay_200", created: false, wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedActor uuid.UUID
			var capturedInput digitaltwin.CreateEntityInput

			creator := &mockEntityCreator{
				fn: func(_ context.Context, actor uuid.UUID, input digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
					capturedActor = actor
					capturedInput = input
					return digitaltwin.CreateEntityResult{
						Created:  tc.created,
						Type:     digitaltwin.EntityTypeSensor,
						EntityID: "urn:ngsi-ld:Sensor:gw_01:temp_01",
						Sensor: &gateway.ProvisionSensorResult{
							Created:   tc.created,
							GatewayID: "gw_01",
							SensorID:  "temp_01",
							Name:      "Temperature Sensor",
							Unit:      &unit,
							EntityID:  "urn:ngsi-ld:Sensor:gw_01:temp_01",
							CreatedAt: &fixedTime,
						},
					}, nil
				},
			}

			r := setupMutationTestRouter(creator, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

			payload := map[string]any{
				"entity_type": "Sensor",
				"gateway_id":  "gw_01",
				"sensor_id":   "temp_01",
				"name":        "Temperature Sensor",
				"unit":        unit,
			}
			bodyBytes, _ := json.Marshal(payload)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Errorf("missing expected cache headers")
			}

			if capturedActor != actorID {
				t.Errorf("captured actor = %s, want %s", capturedActor, actorID)
			}
			if capturedInput.Type != digitaltwin.EntityTypeSensor {
				t.Errorf("captured type = %s, want %s", capturedInput.Type, digitaltwin.EntityTypeSensor)
			}
			if capturedInput.Sensor.GatewayID != "gw_01" || capturedInput.Sensor.SensorID != "temp_01" || capturedInput.Sensor.Name != "Temperature Sensor" || *capturedInput.Sensor.Unit != unit {
				t.Errorf("captured input mismatch: %+v", capturedInput.Sensor)
			}

			var resp PostSensorResponseDTO
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp.GatewayID != "gw_01" || resp.SensorID != "temp_01" || resp.Name != "Temperature Sensor" || resp.EntityID != "urn:ngsi-ld:Sensor:gw_01:temp_01" || *resp.Unit != unit {
				t.Errorf("response DTO mismatch: %+v", resp)
			}
			if resp.CreatedAt == nil || *resp.CreatedAt != "2026-10-18T12:00:00.000000Z" {
				t.Errorf("CreatedAt = %v, want 2026-10-18T12:00:00.000000Z", resp.CreatedAt)
			}
		})
	}
}

func TestPatchDigitalTwin_Gateway(t *testing.T) {
	adminID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	principal := &auth.Principal{UserID: adminID, Role: "authenticated"}
	fixedTime := time.Date(2026, 10, 18, 14, 30, 0, 0, time.UTC)

	t.Run("patch_gateway_name_and_description", func(t *testing.T) {
		var capturedActor uuid.UUID
		var capturedGWID string
		var capturedInput digitaltwin.PatchGatewayInput

		newName := "Updated GW Name"
		newDesc := "Updated GW Description"

		gwPatcher := &mockGatewayPatcher{
			fn: func(_ context.Context, actor uuid.UUID, gatewayID string, input digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				capturedActor = actor
				capturedGWID = gatewayID
				capturedInput = input
				return digitaltwin.PatchGatewayResult{
					ID:          "urn:ngsi-ld:Gateway:gw_01",
					Type:        digitaltwin.EntityTypeGateway,
					GatewayID:   "gw_01",
					Name:        newName,
					Description: &newDesc,
					UpdatedAt:   fixedTime,
				}, nil
			},
		}

		r := setupMutationTestRouter(&mockEntityCreator{}, gwPatcher, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw_01", strings.NewReader(`{"name":"Updated GW Name","description":"Updated GW Description"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
			t.Errorf("missing expected cache headers")
		}

		if capturedActor != adminID {
			t.Errorf("captured actor = %s, want %s", capturedActor, adminID)
		}
		if capturedGWID != "gw_01" {
			t.Errorf("captured gatewayID = %s, want %s", capturedGWID, "gw_01")
		}
		if capturedInput.Name == nil || *capturedInput.Name != newName {
			t.Errorf("captured name mismatch: %v", capturedInput.Name)
		}
		if !capturedInput.DescriptionSet || capturedInput.Description == nil || *capturedInput.Description != newDesc {
			t.Errorf("captured description mismatch: %v", capturedInput.Description)
		}

		var resp PatchGatewayResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.ID != "urn:ngsi-ld:Gateway:gw_01" || resp.GatewayID != "gw_01" || resp.Name != newName || *resp.Description != newDesc || resp.UpdatedAt != "2026-10-18T14:30:00.000000Z" {
			t.Errorf("response mismatch: %+v", resp)
		}
	})

	t.Run("patch_gateway_clear_description", func(t *testing.T) {
		var capturedInput digitaltwin.PatchGatewayInput

		gwPatcher := &mockGatewayPatcher{
			fn: func(_ context.Context, _ uuid.UUID, _ string, input digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				capturedInput = input
				return digitaltwin.PatchGatewayResult{
					ID:          "urn:ngsi-ld:Gateway:gw_01",
					Type:        digitaltwin.EntityTypeGateway,
					GatewayID:   "gw_01",
					Name:        "Preserved Name",
					Description: nil,
					UpdatedAt:   fixedTime,
				}, nil
			},
		}

		r := setupMutationTestRouter(&mockEntityCreator{}, gwPatcher, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw_01", strings.NewReader(`{"description":null}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if !capturedInput.DescriptionSet || capturedInput.Description != nil {
			t.Errorf("expected DescriptionSet=true and Description=nil, got DescriptionSet=%v, Description=%v", capturedInput.DescriptionSet, capturedInput.Description)
		}

		var resp PatchGatewayResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.Description != nil {
			t.Errorf("expected description nil in response, got %v", resp.Description)
		}
	})
}

func TestPatchDigitalTwin_Sensor(t *testing.T) {
	ownerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	principal := &auth.Principal{UserID: ownerID, Role: "authenticated"}
	fixedTime := time.Date(2026, 10, 18, 15, 0, 0, 0, time.UTC)
	unit := "degC"

	t.Run("patch_sensor_name", func(t *testing.T) {
		var capturedActor uuid.UUID
		var capturedGWID string
		var capturedSensorID string
		var capturedInput digitaltwin.PatchSensorInput

		newName := "Updated Sensor Name"

		sensorPatcher := &mockSensorPatcher{
			fn: func(_ context.Context, actor uuid.UUID, gatewayID string, sensorID string, input digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
				capturedActor = actor
				capturedGWID = gatewayID
				capturedSensorID = sensorID
				capturedInput = input
				return digitaltwin.PatchSensorResult{
					ID:        "urn:ngsi-ld:Sensor:gw_01:s_01",
					Type:      digitaltwin.EntityTypeSensor,
					GatewayID: "gw_01",
					SensorID:  "s_01",
					Name:      newName,
					Unit:      &unit,
					UpdatedAt: fixedTime,
				}, nil
			},
		}

		r := setupMutationTestRouter(&mockEntityCreator{}, &mockGatewayPatcher{}, sensorPatcher, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw_01:s_01", strings.NewReader(`{"name":"Updated Sensor Name"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
			t.Errorf("missing expected cache headers")
		}

		if capturedActor != ownerID {
			t.Errorf("captured actor = %s, want %s", capturedActor, ownerID)
		}
		if capturedGWID != "gw_01" || capturedSensorID != "s_01" {
			t.Errorf("captured ids mismatch: gw=%s, sensor=%s", capturedGWID, capturedSensorID)
		}
		if capturedInput.Name == nil || *capturedInput.Name != newName {
			t.Errorf("captured name = %v, want %q", capturedInput.Name, newName)
		}

		var resp PatchSensorResponseDTO
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.ID != "urn:ngsi-ld:Sensor:gw_01:s_01" || resp.GatewayID != "gw_01" || resp.SensorID != "s_01" || resp.Name != newName || *resp.Unit != unit || resp.UpdatedAt != "2026-10-18T15:00:00.000000Z" {
			t.Errorf("response mismatch: %+v", resp)
		}
	})

	t.Run("patch_sensor_forbidden_unit_field", func(t *testing.T) {
		r := setupMutationTestRouter(&mockEntityCreator{}, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw_01:s_01", strings.NewReader(`{"unit":"degF"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "invalid_request" {
			t.Errorf("code = %q, want %q", code, "invalid_request")
		}
	})
}

func TestMutationHandlers_ValidationAndFramingNegatives(t *testing.T) {
	principal := &auth.Principal{UserID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Role: "authenticated"}
	smallLimit := int64(64)

	cases := []struct {
		name       string
		method     string
		url        string
		body       string
		mediaType  string
		limit      int64
		wantStatus int
		wantCode   string
	}{
		// POST Framing and parsing
		{
			name:       "post_malformed_json",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{not-valid-json`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_duplicate_keys",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","name":"A","name":"B","gateway_id":"gw1","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_array_body",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `[{"entity_type":"Gateway"}]`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_query_params_rejected",
			method:     http.MethodPost,
			url:        "/v1/digital-twins?dry_run=true",
			body:       `{"entity_type":"Gateway","gateway_id":"gw1","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_unknown_keys",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw1","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001","extra":"bad"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_missing_entity_type",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"gateway_id":"gw1","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "post_oversized_body",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway","gateway_id":"gw_long_identifier_that_exceeds_the_small_limit","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      smallLimit,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "payload_too_large",
		},
		{
			name:       "post_unsupported_media_type_text",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway"}`,
			mediaType:  "text/plain",
			limit:      testBodyLimit,
			wantStatus: http.StatusUnsupportedMediaType,
			wantCode:   "unsupported_media_type",
		},
		{
			name:       "post_unsupported_media_type_charset",
			method:     http.MethodPost,
			url:        "/v1/digital-twins",
			body:       `{"entity_type":"Gateway"}`,
			mediaType:  "application/json; charset=iso-8859-1",
			limit:      testBodyLimit,
			wantStatus: http.StatusUnsupportedMediaType,
			wantCode:   "unsupported_media_type",
		},

		// PATCH Framing and parsing
		{
			name:       "patch_malformed_json",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1",
			body:       `{broken`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "patch_empty_body",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1",
			body:       `{}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "patch_query_params_rejected",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1?refresh=true",
			body:       `{"name":"Renamed"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "patch_invalid_urn",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/not-a-valid-urn",
			body:       `{"name":"Renamed"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "patch_unsupported_entity_type_urn",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Device:dev_01",
			body:       `{"name":"Renamed"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      testBodyLimit,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "patch_oversized_body",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1",
			body:       `{"name":"This name is intentionally made much longer than the small limit threshold to trigger 413"}`,
			mediaType:  "application/json; charset=utf-8",
			limit:      smallLimit,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "payload_too_large",
		},
		{
			name:       "patch_unsupported_media_type",
			method:     http.MethodPatch,
			url:        "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1",
			body:       `{"name":"Renamed"}`,
			mediaType:  "application/xml",
			limit:      testBodyLimit,
			wantStatus: http.StatusUnsupportedMediaType,
			wantCode:   "unsupported_media_type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := setupMutationTestRouter(&mockEntityCreator{}, &mockGatewayPatcher{}, &mockSensorPatcher{}, tc.limit, principal)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.mediaType)
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Errorf("missing expected cache headers")
			}
			code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
		})
	}
}

func TestMutationHandlers_DomainErrors(t *testing.T) {
	principal := &auth.Principal{UserID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Role: "authenticated"}

	errorCases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "forbidden",
			err:        digitaltwin.ErrForbidden,
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:       "provisioning_forbidden",
			err:        digitaltwin.ErrProvisioningForbidden,
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:       "not_found",
			err:        digitaltwin.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "conflict",
			err:        digitaltwin.ErrProvisioningConflict,
			wantStatus: http.StatusConflict,
			wantCode:   "conflict",
		},
		{
			name:       "deadline_exceeded",
			err:        context.DeadlineExceeded,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "service_unavailable",
		},
		{
			name:       "context_canceled",
			err:        context.Canceled,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "service_unavailable",
		},
		{
			name:       "provisioning_unavailable",
			err:        gateway.ErrGatewayProvisioningUnavailable,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "service_unavailable",
		},
		{
			name:       "metadata_patch_unavailable",
			err:        digitaltwin.ErrMetadataPatchUnavailable,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "service_unavailable",
		},
		{
			name:       "inconsistent_graph",
			err:        digitaltwin.ErrInconsistentGraph,
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
		{
			name:       "unexpected_internal_error",
			err:        errors.New("raw unexpected database failure"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
	}

	for _, tc := range errorCases {
		t.Run("post_"+tc.name, func(t *testing.T) {
			creator := &mockEntityCreator{
				fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
					return digitaltwin.CreateEntityResult{}, tc.err
				},
			}
			r := setupMutationTestRouter(creator, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(`{"entity_type":"Gateway","gateway_id":"gw1","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Errorf("missing expected cache headers")
			}
			code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
		})

		t.Run("patch_"+tc.name, func(t *testing.T) {
			gwPatcher := &mockGatewayPatcher{
				fn: func(_ context.Context, _ uuid.UUID, _ string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
					return digitaltwin.PatchGatewayResult{}, tc.err
				},
			}
			r := setupMutationTestRouter(&mockEntityCreator{}, gwPatcher, &mockSensorPatcher{}, testBodyLimit, principal)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", strings.NewReader(`{"name":"Renamed"}`))
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
				t.Errorf("missing expected cache headers")
			}
			code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
		})
	}
}

func TestMutationHandlers_DTOProjectionFailure(t *testing.T) {
	principal := &auth.Principal{UserID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Role: "authenticated"}

	t.Run("post_mismatched_result_dto_failure", func(t *testing.T) {
		// Mock returns Gateway type with nil Gateway result, triggering NewCreateEntityResponseDTO failure
		creator := &mockEntityCreator{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
				return digitaltwin.CreateEntityResult{
					Created:  true,
					Type:     digitaltwin.EntityTypeGateway,
					EntityID: "urn:ngsi-ld:Gateway:gw1",
					Gateway:  nil, // Corrupted: nil gateway result
				}, nil
			},
		}

		r := setupMutationTestRouter(creator, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(`{"entity_type":"Gateway","gateway_id":"gw1","name":"GW","owner_user_id":"00000000-0000-0000-0000-000000000001"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d (must abort with 500 without writing 201)", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("code = %q, want %q", code, "internal_error")
		}
	})

	t.Run("patch_gateway_dto_projection_failure", func(t *testing.T) {
		// Mock returns invalid URN ID, triggering NewPatchEntityResponseDTO failure
		gwPatcher := &mockGatewayPatcher{
			fn: func(_ context.Context, _ uuid.UUID, _ string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
				return digitaltwin.PatchGatewayResult{
					ID:        "invalid-urn-not-matching-contract",
					Type:      digitaltwin.EntityTypeGateway,
					GatewayID: "gw1",
					Name:      "Valid Name",
					UpdatedAt: time.Now().UTC(),
				}, nil
			},
		}

		r := setupMutationTestRouter(&mockEntityCreator{}, gwPatcher, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/v1/digital-twins/urn:ngsi-ld:Gateway:gw1", strings.NewReader(`{"name":"Renamed"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("code = %q, want %q", code, "internal_error")
		}
	})

	t.Run("post_digital_twin_dto_projection_failure", func(t *testing.T) {
		// Mock returns invalid EntityType, triggering NewCreateEntityResponseDTO failure
		creator := &mockEntityCreator{
			fn: func(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
				return digitaltwin.CreateEntityResult{
					Created:  true,
					Type:     digitaltwin.EntityType("unknown"),
					EntityID: "urn:ngsi-ld:Unknown:01",
				}, nil
			},
		}

		r := setupMutationTestRouter(creator, &mockGatewayPatcher{}, &mockSensorPatcher{}, testBodyLimit, principal)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/digital-twins", strings.NewReader(`{"entity_type":"Gateway","gateway_id":"gw1","name":"Valid","owner_user_id":"00000000-0000-0000-0000-000000000001"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
		}
		code, _ := parseMutationErrorResponse(t, w.Body.Bytes())
		if code != "internal_error" {
			t.Errorf("code = %q, want %q", code, "internal_error")
		}
	})
}
