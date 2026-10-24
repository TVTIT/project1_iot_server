package httpserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func TestControlRoutesAuthenticationAndCache(t *testing.T) {
	router := mustRouter(t, readinessCheckerFunc(func(_ context.Context) error { return nil }))
	for _, route := range []struct{ method, suffix string }{
		{"PATCH", "/desired-state"}, {"GET", "/commands"}, {"GET", "/commands/00000000-0000-4000-8000-000000000001"},
	} {
		t.Run(route.suffix, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(route.method, "/v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1"+route.suffix, strings.NewReader(`{}`))
			router.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Request-ID") == "" {
				t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}

type controlRepositoryStub struct {
	authorize func(context.Context, uuid.UUID, string, bool) error
	admit     func(context.Context, uuid.UUID, string, digitaltwin.DesiredRequest) (digitaltwin.RequestReceipt, error)
	list      func(context.Context, uuid.UUID, string, int, string) (digitaltwin.ControlCommandPage, error)
	get       func(context.Context, uuid.UUID, string, string) (digitaltwin.ControlCommandView, error)
}

func (s *controlRepositoryStub) AuthorizeControlTarget(ctx context.Context, id uuid.UUID, urn string, mutation bool) error {
	return s.authorize(ctx, id, urn, mutation)
}
func (s *controlRepositoryStub) Admit(ctx context.Context, id uuid.UUID, urn string, r digitaltwin.DesiredRequest) (digitaltwin.RequestReceipt, error) {
	return s.admit(ctx, id, urn, r)
}
func (s *controlRepositoryStub) ListCommands(ctx context.Context, id uuid.UUID, urn string, limit int, cursor string) (digitaltwin.ControlCommandPage, error) {
	return s.list(ctx, id, urn, limit, cursor)
}
func (s *controlRepositoryStub) GetCommand(ctx context.Context, id uuid.UUID, urn, id2 string) (digitaltwin.ControlCommandView, error) {
	return s.get(ctx, id, urn, id2)
}

func TestControlTimeoutReplayAndFailureBoundaries(t *testing.T) {
	for _, outcome := range []string{"receipt", "authorization", "admission", "deadline", "list", "detail"} {
		t.Run(outcome, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.ControlAPIEnabled = true
			deps.ControlRequestTimeout = 100 * time.Millisecond
			var firstDeadline time.Time
			stub := &controlRepositoryStub{}
			stub.authorize = func(ctx context.Context, _ uuid.UUID, urn string, mutation bool) error {
				if mutation != (outcome != "list" && outcome != "detail") {
					t.Fatal("incorrect mutation authorization")
				}
				var ok bool
				firstDeadline, ok = ctx.Deadline()
				if !ok || time.Until(firstDeadline) > 100*time.Millisecond {
					t.Fatal("missing bounded deadline")
				}
				if urn != "urn:ngsi-ld:Sensor:gw:s1" {
					t.Fatalf("URN decoded more than once: %q", urn)
				}
				if outcome == "authorization" {
					return digitaltwin.ErrForbidden
				}
				if outcome == "deadline" {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			checkDeadline := func(ctx context.Context) {
				deadline, _ := ctx.Deadline()
				if !deadline.Equal(firstDeadline) {
					t.Fatalf("deadline reset: %v / %v", deadline, firstDeadline)
				}
			}
			stub.admit = func(ctx context.Context, _ uuid.UUID, _ string, _ digitaltwin.DesiredRequest) (digitaltwin.RequestReceipt, error) {
				checkDeadline(ctx)
				if outcome == "admission" {
					return digitaltwin.RequestReceipt{}, errors.Join(digitaltwin.ErrDependencyUnavailable, digitaltwin.ErrControlCommitUncertain)
				}
				id := "00000000-0000-4000-8000-000000000001"
				return digitaltwin.RequestReceipt{StatusCode: 202, CommandID: &id, Response: []byte("{ \"status\" : \"pending\" }\n")}, nil
			}
			stub.list = func(ctx context.Context, _ uuid.UUID, _ string, _ int, _ string) (digitaltwin.ControlCommandPage, error) {
				checkDeadline(ctx)
				return digitaltwin.ControlCommandPage{}, digitaltwin.ErrDependencyUnavailable
			}
			stub.get = func(ctx context.Context, _ uuid.UUID, _ string, _ string) (digitaltwin.ControlCommandView, error) {
				checkDeadline(ctx)
				return digitaltwin.ControlCommandView{}, digitaltwin.ErrNotFound
			}
			deps.ControlRepository = stub
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			method, suffix, want := "PATCH", "/desired-state", 202
			switch outcome {
			case "authorization":
				want = 403
			case "admission", "deadline":
				want = 503
			case "list":
				method, suffix, want = "GET", "/commands", 503
			case "detail":
				method, suffix, want = "GET", "/commands/00000000-0000-4000-8000-000000000001", 404
			}
			r := httptest.NewRequest(method, "/v1/digital-twins/urn%3Angsi-ld%3ASensor%3Agw%3As1"+suffix, strings.NewReader(`{"frequency_hz":1,"expected_desired_version":"0"}`))
			r.Header.Set("Authorization", "Bearer token")
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "key")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != want {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if outcome == "receipt" && w.Body.String() != "{ \"status\" : \"pending\" }\n" {
				t.Fatal("receipt bytes re-encoded")
			}
		})
	}
}

func TestControlFailClosedConfiguration(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlRequestTimeout = time.Second
	if _, err := NewRouter(deps); err == nil {
		t.Fatal("enabled without repository")
	}
	var typedNil *controlRepositoryStub
	deps.ControlRepository = typedNil
	if _, err := NewRouter(deps); err == nil {
		t.Fatal("enabled with typed nil repository")
	}
	deps.ControlRepository = &controlRepositoryStub{}
	for _, timeout := range []time.Duration{0, time.Nanosecond, 2 * time.Minute} {
		deps.ControlRequestTimeout = timeout
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("unbounded timeout")
		}
	}
	deps.ControlAPIEnabled = false
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("PATCH", "/v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/desired-state", nil)
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("disabled %d", w.Code)
	}
}

func TestControlQueryReusesHTTPRawBound(t *testing.T) {
	r := httptest.NewRequest("GET", "/?cursor="+strings.Repeat("%61", 400), nil)
	if _, _, err := parseControlQuery(r, false); err == nil {
		t.Fatal("query exceeded existing Digital Twin raw-query byte bound")
	}
}

func TestControlRequestBudgetBoundsSlowHTTPBody(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlRequestTimeout = 50 * time.Millisecond
	deps.ControlRepository = &controlRepositoryStub{authorize: func(context.Context, uuid.UUID, string, bool) error { return nil }}
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
	if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprint(conn, "PATCH /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/desired-state HTTP/1.1\r\nHost: fixture.invalid\r\nAuthorization: Bearer token\r\nContent-Type: application/json\r\nIdempotency-Key: key\r\nContent-Length: 100\r\n\r\n{\"frequency_hz\":")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("HTTP body read ignored shared request budget:", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 503 || time.Since(started) > time.Second {
		t.Fatalf("body timeout: status %d elapsed %s", response.StatusCode, time.Since(started))
	}
}
