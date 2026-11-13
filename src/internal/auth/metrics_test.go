package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type metricsVerifier struct{}

func (metricsVerifier) Verify(_ context.Context, token string) (Principal, error) {
	if token != "valid" {
		return Principal{}, ErrInvalidToken
	}
	return Principal{}, nil
}

func TestAuthenticationMetricsPreserveAllowDeny(t *testing.T) {
	m := &Metrics{}
	r := gin.New()
	r.GET("/test", AuthenticationMiddleware(metricsVerifier{}, m), func(c *gin.Context) { c.Status(204) })
	for _, tc := range []struct {
		token  string
		status int
	}{{"", 401}, {"bad", 401}, {"valid", 204}} {
		request := httptest.NewRequest(http.MethodGet, "/test", nil)
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, request)
		if w.Code != tc.status {
			t.Fatal(w.Code)
		}
	}
	s := m.Snapshot()
	if s.Allow != 1 || s.Deny != 2 || s.Verify.Count != 2 {
		t.Fatal(s)
	}
}
