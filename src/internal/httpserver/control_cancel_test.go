package httpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpapi"
)

// cancelStubRepository records how the cancellation surface was reached.
type cancelStubRepository struct {
	controlRepositoryStub
	cancelCalls    int
	authorizeCalls int
	mutation       []bool
}

func (s *cancelStubRepository) AuthorizeControlTarget(ctx context.Context, id uuid.UUID, urn string, mutation bool) error {
	s.authorizeCalls++
	s.mutation = append(s.mutation, mutation)
	return s.authorize(ctx, id, urn, mutation)
}

func (s *cancelStubRepository) CancelControl(ctx context.Context, id uuid.UUID, urn, commandID string) (digitaltwin.CommandCancellation, error) {
	s.cancelCalls++
	if s.cancel == nil {
		return digitaltwin.CommandCancellation{}, digitaltwin.ErrNotFound
	}
	return s.cancel(ctx, id, urn, commandID)
}

func cancelRouter(t *testing.T, repo ControlRepository, enabled bool) http.Handler {
	t.Helper()
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlCancelEnabled = enabled
	deps.ControlRequestTimeout = 2 * time.Second
	deps.ControlRepository = repo
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	return router
}

func cancelRequest(t *testing.T, router http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		if value == "" {
			r.Header.Del(key)
			continue
		}
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

// The cancellation surface is opt-in: with Stage 6 disabled the route does not
// exist at all, so it can never answer 409/503 on a feature that is off.
func TestControlCancelRouteIsOptIn(t *testing.T) {
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
		t.Fatal("repository reached with the cancellation route disabled")
		return digitaltwin.CommandCancellation{}, nil
	}
	router := cancelRouter(t, stub, false)
	w := cancelRequest(t, router, http.MethodPost, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel", "", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("disabled cancellation status %d body %s", w.Code, w.Body.String())
	}
	if stub.cancelCalls != 0 {
		t.Fatal("disabled cancellation reached the repository")
	}
}

func TestControlCancelContract(t *testing.T) {
	commandID := "00000000-0000-4000-8000-000000000001"
	path := "/v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/" + commandID + "/cancel"
	outcome := digitaltwin.CommandCancellation{
		EntityID:       "urn:ngsi-ld:Sensor:gw:s1",
		CommandID:      commandID,
		Status:         digitaltwin.CommandCancelled,
		DesiredVersion: digitaltwin.Version(13),
		CancelledAt:    time.Date(2026, 11, 14, 14, 30, 0, 0, time.UTC),
		StatusURL:      "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw%3As1/commands/" + commandID,
	}
	t.Run("owner success", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(_ context.Context, _ uuid.UUID, _ string, mutation bool) error {
			if !mutation {
				t.Fatal("cancellation was authorized as a read")
			}
			return nil
		}
		stub.cancel = func(_ context.Context, _ uuid.UUID, urn, id string) (digitaltwin.CommandCancellation, error) {
			if urn != "urn:ngsi-ld:Sensor:gw:s1" || id != commandID {
				t.Fatalf("unexpected target %q %q", urn, id)
			}
			return outcome, nil
		}
		router := cancelRouter(t, stub, true)
		w := cancelRequest(t, router, http.MethodPost, path, "", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("status %d body %s", w.Code, w.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"entity_id", "command_id", "status", "desired_version", "cancelled_at", "status_url"} {
			if _, ok := body[field]; !ok {
				t.Fatalf("missing %s in %s", field, w.Body.String())
			}
		}
		if string(body["status"]) != `"cancelled"` || string(body["desired_version"]) != `"13"` {
			t.Fatalf("unexpected body %s", w.Body.String())
		}
		if len(body) != 6 {
			t.Fatalf("unexpected body fields %s", w.Body.String())
		}
	})
	t.Run("repeated request is stable", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
		stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
			return outcome, nil
		}
		router := cancelRouter(t, stub, true)
		first := cancelRequest(t, router, http.MethodPost, path, "", nil)
		second := cancelRequest(t, router, http.MethodPost, path, "", nil)
		if first.Code != http.StatusOK || second.Code != http.StatusOK {
			t.Fatalf("status %d/%d", first.Code, second.Code)
		}
		if first.Body.String() != second.Body.String() {
			t.Fatalf("repeated cancellation changed the body: %s vs %s", first.Body.String(), second.Body.String())
		}
	})
	t.Run("rejects any body", func(t *testing.T) {
		for _, body := range []string{" ", "{}", "x", "\n"} {
			stub := &cancelStubRepository{}
			stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
			stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
				t.Fatal("repository reached with a non-empty body")
				return digitaltwin.CommandCancellation{}, nil
			}
			router := cancelRouter(t, stub, true)
			w := cancelRequest(t, router, http.MethodPost, path, body, nil)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("body %q status %d", body, w.Code)
			}
			assertControlErrorCode(t, w.Body.Bytes(), "invalid_request")
		}
	})
	t.Run("rejects a body that contradicts Content-Length", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
		stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
			t.Fatal("repository reached with a hidden body")
			return digitaltwin.CommandCancellation{}, nil
		}
		router := cancelRouter(t, stub, true)
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer token")
		r.ContentLength = 0
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("hidden body status %d", w.Code)
		}
	})
	t.Run("rejects a query string", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
		stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
			t.Fatal("repository reached with a query string")
			return digitaltwin.CommandCancellation{}, nil
		}
		router := cancelRouter(t, stub, true)
		w := cancelRequest(t, router, http.MethodPost, path+"?limit=1", "", nil)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("query status %d", w.Code)
		}
	})
	t.Run("negotiates accept and content type", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
		stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
			return outcome, nil
		}
		router := cancelRouter(t, stub, true)
		w := cancelRequest(t, router, http.MethodPost, path, "", map[string]string{"Accept": "application/ld+json"})
		if w.Code != http.StatusNotAcceptable {
			t.Fatalf("accept status %d", w.Code)
		}
		w = cancelRequest(t, router, http.MethodPost, path, "", map[string]string{"Content-Type": "text/plain"})
		if w.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("content type status %d", w.Code)
		}
		w = cancelRequest(t, router, http.MethodPost, path, "", map[string]string{"Content-Type": "application/json; charset=utf-8"})
		if w.Code != http.StatusOK {
			t.Fatalf("charset content type status %d", w.Code)
		}
		w = cancelRequest(t, router, http.MethodPost, path, "", map[string]string{"Content-Type": ""})
		if w.Code != http.StatusOK {
			t.Fatalf("absent content type status %d", w.Code)
		}
	})
	t.Run("maps domain errors", func(t *testing.T) {
		cases := []struct {
			name string
			err  error
			want int
			code string
		}{
			{"not cancellable", digitaltwin.ErrCommandNotCancellable, http.StatusConflict, "command_not_cancellable"},
			{"forbidden", digitaltwin.ErrForbidden, http.StatusForbidden, "forbidden"},
			{"not found", digitaltwin.ErrNotFound, http.StatusNotFound, "not_found"},
			{"invalid input", digitaltwin.ErrInvalidInput, http.StatusBadRequest, "invalid_request"},
			{"commit uncertain", errors.Join(digitaltwin.ErrDependencyUnavailable, digitaltwin.ErrControlCommitUncertain), http.StatusServiceUnavailable, "service_unavailable"},
			{"dependency", digitaltwin.ErrDependencyUnavailable, http.StatusServiceUnavailable, "service_unavailable"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				stub := &cancelStubRepository{}
				stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
				stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
					return digitaltwin.CommandCancellation{}, tc.err
				}
				router := cancelRouter(t, stub, true)
				w := cancelRequest(t, router, http.MethodPost, path, "", nil)
				if w.Code != tc.want {
					t.Fatalf("status %d body %s", w.Code, w.Body.String())
				}
				assertControlErrorCode(t, w.Body.Bytes(), tc.code)
				if strings.Contains(w.Body.String(), "cancelled") {
					t.Fatalf("error response leaked a successful cancellation: %s", w.Body.String())
				}
			})
		}
	})
	t.Run("spans one request budget", func(t *testing.T) {
		stub := &cancelStubRepository{}
		var first time.Time
		stub.authorize = func(ctx context.Context, _ uuid.UUID, _ string, _ bool) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("missing bounded deadline")
			}
			first = deadline
			return nil
		}
		stub.cancel = func(ctx context.Context, _ uuid.UUID, _ string, _ string) (digitaltwin.CommandCancellation, error) {
			deadline, ok := ctx.Deadline()
			if !ok || !deadline.Equal(first) {
				t.Fatalf("deadline reset: %v / %v", deadline, first)
			}
			return outcome, nil
		}
		router := cancelRouter(t, stub, true)
		w := cancelRequest(t, router, http.MethodPost, path, "", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
	})
	t.Run("requires authentication", func(t *testing.T) {
		stub := &cancelStubRepository{}
		stub.authorize = func(context.Context, uuid.UUID, string, bool) error {
			t.Fatal("unauthenticated request reached authorization")
			return nil
		}
		stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
			t.Fatal("unauthenticated request reached the repository")
			return digitaltwin.CommandCancellation{}, nil
		}
		router := cancelRouter(t, stub, true)
		r := httptest.NewRequest(http.MethodPost, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d", w.Code)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("cache headers %v", w.Header())
		}
	})
}

// A slow or endless body must not outlive the shared request budget.
func TestControlCancelSlowBodyIsBounded(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlCancelEnabled = true
	deps.ControlRequestTimeout = 50 * time.Millisecond
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
		t.Fatal("repository reached with an unfinished body")
		return digitaltwin.CommandCancellation{}, nil
	}
	deps.ControlRepository = stub
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprint(conn, "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\nHost: fixture.invalid\r\nAuthorization: Bearer token\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("cancellation body read ignored the shared request budget:", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusServiceUnavailable || time.Since(started) > 2*time.Second {
		t.Fatalf("body timeout: status %d elapsed %s", response.StatusCode, time.Since(started))
	}
}

// A chunked non-empty body is rejected on the same one-byte bound. This uses a
// real HTTP/1 connection so the handler cannot rely on a buffered test body.
func TestControlCancelChunkedBodyRejected(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlCancelEnabled = true
	deps.ControlRequestTimeout = time.Second
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
		t.Fatal("repository reached with a non-empty chunked body")
		return digitaltwin.CommandCancellation{}, nil
	}
	deps.ControlRepository = stub
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprint(conn, "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\nHost: fixture.invalid\r\nAuthorization: Bearer token\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\n{\r\n0\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("chunked body status %d body %s", response.StatusCode, body)
	}
	assertControlErrorCode(t, body, "invalid_request")
}

func assertControlErrorCode(t *testing.T, data []byte, code string) {
	t.Helper()
	var envelope httpapi.APIErrorEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Error.Code != code {
		t.Fatalf("want %s: %s (%v)", code, data, err)
	}
}

// The body reader of the cancellation request must never be drained without a
// bound, and an empty body must be accepted.
func TestControlCancelEmptyBodyIsAccepted(t *testing.T) {
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error) {
		return digitaltwin.CommandCancellation{Status: digitaltwin.CommandCancelled}, nil
	}
	router := cancelRouter(t, stub, true)
	r := httptest.NewRequest(http.MethodPost, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel", nil)
	r.Header.Set("Authorization", "Bearer token")
	r.Body = io.NopCloser(strings.NewReader(""))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("empty body status %d body %s", w.Code, w.Body.String())
	}
}
