package httpserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

// TestControlCancelPartialNonemptyChunkedBodyKeepAlive tests the S6-02 defect:
// HTTP/1.1 keep-alive with a non-empty chunked body that never terminates.
// The client sends one chunk with '{' but no final chunk and no Connection: close.
// Expected: 400 response within the ControlRequestTimeout budget, connection closed.
// Before fix: net/http drains the unread body after handler returns, bypassing the request budget.
func TestControlCancelPartialNonemptyChunkedBodyKeepAlive(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlCancelEnabled = true
	deps.ControlRequestTimeout = 50 * time.Millisecond

	var cancelCallCount int32
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(_ context.Context, _ uuid.UUID, urn, cmdID string) (digitaltwin.CommandCancellation, error) {
		atomic.AddInt32(&cancelCallCount, 1)
		return digitaltwin.CommandCancellation{
			EntityID:       urn,
			CommandID:      cmdID,
			Status:         digitaltwin.CommandCancelled,
			DesiredVersion: 1,
			CancelledAt:    time.Now().UTC(),
			StatusURL:      "/v1/digital-twins/" + urn + "/commands/" + cmdID,
		}, nil
	}
	deps.ControlRepository = stub
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	// Use a real HTTP server with finite but larger ReadTimeout than request budget
	server := httptest.NewUnstartedServer(router)
	server.Config.ReadTimeout = time.Second // 1s > 50ms request budget
	server.Start()
	defer server.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Set a deadline longer than request budget but shorter than server ReadTimeout
	if err = conn.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	// Send HTTP/1.1 chunked request with ONE non-empty chunk, NO final chunk, NO Connection: close
	// This is the key difference from TestControlCancelChunkedBodyRejected which sends complete body + Connection: close
	request := "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\n" +
		"Host: fixture.invalid\r\n" +
		"Authorization: Bearer token\r\n" +
		"Content-Type: application/json\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"1\r\n" + // chunk size: 1 byte
		"{\r\n" // chunk data: single byte '{', NO final "0\r\n\r\n"
	_, err = fmt.Fprint(conn, request)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	elapsed := time.Since(started)

	t.Logf("request budget=50ms; response=%v error=%v elapsed=%s", response, err, elapsed)

	// Before fix: response is nil, err is i/o timeout after ~300ms (connection drain)
	// After fix: response should be 400 Bad Request within ~50-100ms
	if response == nil {
		t.Fatalf("expected 400 response within request budget, got nil response after %s: %v", elapsed, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d after %s", response.StatusCode, elapsed)
	}

	// Response should come within reasonable bound of request budget + small tolerance
	// Allow up to 200ms (4x the 50ms budget) for test scheduling variance
	if elapsed > 200*time.Millisecond {
		t.Fatalf("response took %s, exceeded request budget tolerance (50ms budget)", elapsed)
	}

	// Verify error envelope
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertControlErrorCode(t, body, "invalid_request")

	// Verify connection was closed by server (no drain/reuse)
	if !response.Close {
		t.Fatalf("expected response.Close=true (server closed connection), got false")
	}

	// Assert cancel NOT called for rejected body
	if atomic.LoadInt32(&cancelCallCount) != 0 {
		t.Fatalf("cancelCallCount after invalid body: expected 0, got %d", atomic.LoadInt32(&cancelCallCount))
	}

	// Verify a subsequent valid request on a NEW connection succeeds (no poisoned server/context)
	conn2, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn2.Close() }()
	if err = conn2.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	validRequest := "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\n" +
		"Host: fixture.invalid\r\n" +
		"Authorization: Bearer token\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	_, err = fmt.Fprint(conn2, validRequest)
	if err != nil {
		t.Fatal(err)
	}
	response2, err := http.ReadResponse(bufio.NewReader(conn2), nil)
	if err != nil {
		t.Fatalf("valid request after rejection failed: %v", err)
	}
	defer func() { _ = response2.Body.Close() }()
	// Valid empty-body cancel should reach repository and return 200 OK
	if response2.StatusCode != http.StatusOK {
		t.Fatalf("valid request after rejection: expected 200 OK, got %d", response2.StatusCode)
	}
	_, _ = io.ReadAll(response2.Body)

	// Assert cancel called exactly once for valid request
	if atomic.LoadInt32(&cancelCallCount) != 1 {
		t.Fatalf("cancelCallCount after valid request: expected 1, got %d", atomic.LoadInt32(&cancelCallCount))
	}
}

// TestControlCancelPartialContentLengthBodyKeepAlive tests the adjacent case:
// Content-Length with partial body, keep-alive, no Connection: close.
// Same fix should apply.
func TestControlCancelPartialContentLengthBodyKeepAlive(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.ControlAPIEnabled = true
	deps.ControlCancelEnabled = true
	deps.ControlRequestTimeout = 50 * time.Millisecond

	var cancelCallCount int32
	stub := &cancelStubRepository{}
	stub.authorize = func(context.Context, uuid.UUID, string, bool) error { return nil }
	stub.cancel = func(_ context.Context, _ uuid.UUID, urn, cmdID string) (digitaltwin.CommandCancellation, error) {
		atomic.AddInt32(&cancelCallCount, 1)
		return digitaltwin.CommandCancellation{
			EntityID:       urn,
			CommandID:      cmdID,
			Status:         digitaltwin.CommandCancelled,
			DesiredVersion: 1,
			CancelledAt:    time.Now().UTC(),
			StatusURL:      "/v1/digital-twins/" + urn + "/commands/" + cmdID,
		}, nil
	}
	deps.ControlRepository = stub
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(router)
	server.Config.ReadTimeout = time.Second
	server.Start()
	defer server.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if err = conn.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	// Send Content-Length: 2 but only send 1 byte, no Connection: close
	request := "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\n" +
		"Host: fixture.invalid\r\n" +
		"Authorization: Bearer token\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: 2\r\n" +
		"\r\n" +
		"{" // only 1 byte, not 2
	_, err = fmt.Fprint(conn, request)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	elapsed := time.Since(started)

	t.Logf("request budget=50ms; response=%v error=%v elapsed=%s", response, err, elapsed)

	if response == nil {
		t.Fatalf("expected 400 response within request budget, got nil response after %s: %v", elapsed, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d after %s", response.StatusCode, elapsed)
	}

	if elapsed > 200*time.Millisecond {
		t.Fatalf("response took %s, exceeded request budget tolerance", elapsed)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertControlErrorCode(t, body, "invalid_request")

	// Verify connection was closed by server (no drain/reuse)
	if !response.Close {
		t.Fatalf("expected response.Close=true (server closed connection), got false")
	}

	// Assert cancel NOT called for rejected body
	if atomic.LoadInt32(&cancelCallCount) != 0 {
		t.Fatalf("cancelCallCount after invalid body: expected 0, got %d", atomic.LoadInt32(&cancelCallCount))
	}

	// Verify a subsequent valid request on a NEW connection succeeds (no poisoned server/context)
	conn2, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn2.Close() }()
	if err = conn2.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	validRequest := "POST /v1/digital-twins/urn:ngsi-ld:Sensor:gw:s1/commands/00000000-0000-4000-8000-000000000001/cancel HTTP/1.1\r\n" +
		"Host: fixture.invalid\r\n" +
		"Authorization: Bearer token\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	_, err = fmt.Fprint(conn2, validRequest)
	if err != nil {
		t.Fatal(err)
	}
	response2, err := http.ReadResponse(bufio.NewReader(conn2), nil)
	if err != nil {
		t.Fatalf("valid request after rejection failed: %v", err)
	}
	defer func() { _ = response2.Body.Close() }()
	// Valid empty-body cancel should reach repository and return 200 OK
	if response2.StatusCode != http.StatusOK {
		t.Fatalf("valid request after rejection: expected 200 OK, got %d", response2.StatusCode)
	}
	_, _ = io.ReadAll(response2.Body)

	// Assert cancel called exactly once for valid request
	if atomic.LoadInt32(&cancelCallCount) != 1 {
		t.Fatalf("cancelCallCount after valid request: expected 1, got %d", atomic.LoadInt32(&cancelCallCount))
	}
}

// TestControlCancelHTTP2NoConnectionHeader tests that setBodyRejectionClosePolicy
// does not emit Connection header for HTTP/2 requests.
// Uses Gin test context with HTTP/2 proto; no network exchange.
func TestControlCancelHTTP2NoConnectionHeader(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/test", nil)
	c.Request.Proto = "HTTP/2.0"
	c.Request.ProtoMajor = 2
	c.Request.ProtoMinor = 0

	// Call the helper directly
	setBodyRejectionClosePolicy(c)

	// Assert Connection header is NOT set for HTTP/2
	if conn := w.Header().Get("Connection"); conn != "" {
		t.Fatalf("expected empty Connection header for HTTP/2, got %q", conn)
	}
}

// TestControlCancelHTTP1ConnectionCloseHeader tests that setBodyRejectionClosePolicy
// emits Connection: close for HTTP/1.1 requests.
func TestControlCancelHTTP1ConnectionCloseHeader(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/test", nil)
	c.Request.Proto = "HTTP/1.1"
	c.Request.ProtoMajor = 1
	c.Request.ProtoMinor = 1

	setBodyRejectionClosePolicy(c)

	if conn := w.Header().Get("Connection"); conn != "close" {
		t.Fatalf("expected Connection: close for HTTP/1.1, got %q", conn)
	}
}
