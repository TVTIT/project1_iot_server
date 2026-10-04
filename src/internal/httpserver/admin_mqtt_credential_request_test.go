package httpserver

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type credentialReadFailure struct{}

func (credentialReadFailure) Read([]byte) (int, error) { return 0, errors.New("private body detail") }

func TestCredentialNoBodyReaderAndLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   io.Reader
		status int
	}{
		{"empty", strings.NewReader(""), 0},
		{"at-limit", strings.NewReader(strings.Repeat("x", 32)), 400},
		{"above-limit", strings.NewReader(strings.Repeat("x", 33)), 413},
		{"read-failure", credentialReadFailure{}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", tc.body)
			want := uuid.New()
			req.Header.Set("Idempotency-Key", want.String())
			key, status := parseCredentialRequest(req, "gateway_test", true, 32)
			if status != tc.status || (status == 0 && key != want) {
				t.Fatal("incorrect strict no-body parsing")
			}
		})
	}
	for _, query := range []string{"/?", "/?password=client-secret"} {
		req := httptest.NewRequest("GET", query, nil)
		_, status := parseCredentialRequest(req, "gateway_test", false, 32)
		if status != 400 {
			t.Fatal("query accepted")
		}
	}
}
