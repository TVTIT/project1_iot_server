package httpserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"iot-platform/internal/digitaltwin"
)

func TestDesiredStateRequestBoundary(t *testing.T) {
	const urn = "urn:ngsi-ld:Sensor:gateway_001:sensor_001"
	const body = `{"frequency_hz":0.1,"expected_desired_version":"9007199254740993"}`
	for _, tc := range []struct {
		name, media, key, query, target, body string
		limit                                 int64
		status                                int
		code                                  string
	}{
		{"valid", "application/json", "key", "", urn, body, 8192, 0, ""},
		{"charset", "application/json; charset=UTF-8", "key", "", urn, body, 8192, 0, ""},
		{"media", "application/ld+json", "key", "", urn, body, 8192, 415, "unsupported_media_type"},
		{"bad charset", "application/json; charset=ascii", "key", "", urn, body, 8192, 415, "unsupported_media_type"},
		{"key missing", "application/json", "", "", urn, body, 8192, 400, "invalid_request"},
		{"query", "application/json", "key", "?x=1", urn, body, 8192, 400, "invalid_request"},
		{"empty query", "application/json", "key", "?", urn, body, 8192, 400, "invalid_request"},
		{"no second decode", "application/json", "key", "", strings.Replace(urn, ":", "%3A", 1), body, 8192, 400, "invalid_request"},
		{"gateway target", "application/json", "key", "", "urn:ngsi-ld:Gateway:gateway_001", body, 8192, 400, "unsupported_command_target"},
		{"over body limit", "application/json", "key", "", urn, body, int64(len(body) - 1), 413, "payload_too_large"},
		{"at body limit", "application/json", "key", "", urn, body, int64(len(body)), 0, ""},
		{"hard cap", "application/json", "key", "", urn, strings.Repeat(" ", 8193), 999999, 413, "payload_too_large"},
		{"unrepresentable", "application/json", "key", "", urn, strings.Replace(body, "0.1", "1e-10", 1), 8192, 400, "frequency_not_representable"},
		{"unknown", "application/json", "key", "", urn, strings.Replace(body, "0.1", "0.1,\"action\":\"reboot\"", 1), 8192, 400, "invalid_request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("PATCH", "/desired-state"+tc.query, bytes.NewBufferString(tc.body))
			r.Header.Set("Content-Type", tc.media)
			r.Header.Set("Idempotency-Key", tc.key)
			input, status, err := parseDesiredStateRequest(r, tc.target, tc.limit)
			if status != tc.status {
				t.Fatalf("status=%d err=%v want %d", status, err, tc.status)
			}
			if status == 0 {
				if err != nil || input.Input.Frequency.SampleIntervalUS() != 10000000 {
					t.Fatalf("input=%v err=%v", input, err)
				}
			} else {
				_, code := controlErrorResponse(err)
				if code != tc.code {
					t.Fatalf("code=%q want %q", code, tc.code)
				}
			}
		})
	}
	for _, err := range []error{digitaltwin.ErrDesiredVersionConflict, digitaltwin.ErrCommandInProgress, digitaltwin.ErrIdempotencyConflict, digitaltwin.ErrDesiredVersionOverflow} {
		status, _ := controlErrorResponse(err)
		if status != 409 {
			t.Fatal(status)
		}
	}
	status, code := controlErrorResponse(errors.New("raw secret"))
	if status != 500 || code != "internal_error" {
		t.Fatal(status, code)
	}
}

func TestDesiredStateRequestFailureBoundaries(t *testing.T) {
	const urn = "urn:ngsi-ld:Sensor:gateway_001:sensor_001"
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{context.Canceled, 503, "service_unavailable"}, {context.DeadlineExceeded, 503, "service_unavailable"}, {digitaltwin.ErrDependencyUnavailable, 503, "service_unavailable"},
		{digitaltwin.ErrNotFound, 404, "not_found"}, {digitaltwin.ErrForbidden, 403, "forbidden"}, {digitaltwin.ErrInvalidFrequency, 400, "invalid_request"},
	} {
		s, c := controlErrorResponse(tc.err)
		if s != tc.status || c != tc.code {
			t.Fatal(s, c)
		}
	}
	for _, body := range []string{`{"frequency_hz":1,"expected_desired_version":"0","expected_desired_version":"0"}`, `{"frequency_hz":1,"expected_desired_version":"\ud800"}`} {
		r := httptest.NewRequest(http.MethodPatch, "/desired-state", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "key")
		if _, status, _ := parseDesiredStateRequest(r, urn, 8192); status != 400 {
			t.Fatal(status)
		}
	}
	for _, mutate := range []func(*http.Request){func(r *http.Request) { r.Header.Add("Idempotency-Key", "second") }, func(r *http.Request) { r.Body = nil }, func(r *http.Request) { r.Body = io.NopCloser(controlFailingReader{}) }} {
		r := httptest.NewRequest(http.MethodPatch, "/desired-state", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "key")
		mutate(r)
		if _, status, _ := parseDesiredStateRequest(r, urn, 8192); status != 400 {
			t.Fatal(status)
		}
	}
	r := httptest.NewRequest(http.MethodPatch, "/desired-state", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "key")
	if _, status, _ := parseDesiredStateRequest(r, urn, 0); status != 400 {
		t.Fatal(status)
	}
}

type controlFailingReader struct{}

func (controlFailingReader) Read([]byte) (int, error) { return 0, errors.New("test read failure") }
