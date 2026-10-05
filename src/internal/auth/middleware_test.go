package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/httpapi"
)

type verifierFunc func(context.Context, string) (Principal, error)

func (f verifierFunc) Verify(ctx context.Context, token string) (Principal, error) {
	return f(ctx, token)
}

func TestAuthenticationMiddlewareAcceptsStrictBearerToken(t *testing.T) {
	want := Principal{UserID: uuid.New(), Role: "authenticated"}
	var verifiedToken string
	router := authTestRouter(verifierFunc(func(_ context.Context, token string) (Principal, error) {
		verifiedToken = token
		return want, nil
	}), func(c *gin.Context) {
		got, ok := PrincipalFrom(c)
		if !ok || got != want {
			t.Fatalf("PrincipalFrom() = %+v, %v; want %+v, true", got, ok, want)
		}
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "bEaReR valid-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if verifiedToken != "valid-token" {
		t.Errorf("verified token = %q", verifiedToken)
	}
}

func TestAuthenticationMiddlewareRejectsInvalidAuthorization(t *testing.T) {
	tests := []struct {
		name   string
		header []string
		path   string
	}{
		{name: "missing"},
		{name: "basic", header: []string{"Basic abc"}},
		{name: "scheme only", header: []string{"Bearer"}},
		{name: "empty token", header: []string{"Bearer "}},
		{name: "multiple spaces", header: []string{"Bearer  token"}},
		{name: "extra field", header: []string{"Bearer token extra"}},
		{name: "tab separator", header: []string{"Bearer\ttoken"}},
		{name: "tab before token", header: []string{"Bearer \ttoken"}},
		{name: "comma credentials", header: []string{"Bearer token,Bearer other"}},
		{name: "multiple headers", header: []string{"Bearer token", "Bearer other"}},
		{name: "query token", path: "/protected?access_token=query-token"},
		{name: "leading space", header: []string{" Bearer token"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifierCalled := false
			handlerCalled := false
			router := authTestRouter(verifierFunc(func(context.Context, string) (Principal, error) {
				verifierCalled = true
				return Principal{}, nil
			}), func(*gin.Context) { handlerCalled = true })
			path := tt.path
			if path == "" {
				path = "/protected"
			}
			request := httptest.NewRequest(http.MethodGet, path, nil)
			for _, value := range tt.header {
				request.Header.Add("Authorization", value)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			assertUnauthorized(t, response)
			if verifierCalled || handlerCalled {
				t.Fatal("invalid authorization reached verifier or handler")
			}
		})
	}
}

func TestAuthenticationMiddlewareDoesNotAcceptBodyToken(t *testing.T) {
	router := authTestRouter(verifierFunc(func(context.Context, string) (Principal, error) {
		t.Fatal("body token reached verifier")
		return Principal{}, nil
	}), func(*gin.Context) { t.Fatal("body token reached handler") })
	req := httptest.NewRequest(http.MethodGet, "/protected", strings.NewReader(`{"access_token":"body-token"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertUnauthorized(t, rec)
}

func TestAuthenticationMiddlewareHidesVerifierErrorAndToken(t *testing.T) {
	token := "sensitive-invalid-token"
	handlerCalled := false
	router := authTestRouter(verifierFunc(func(context.Context, string) (Principal, error) {
		return Principal{}, errors.New("signature failure with internal detail")
	}), func(*gin.Context) { handlerCalled = true })

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	assertUnauthorized(t, response)
	if handlerCalled {
		t.Fatal("handler ran after verifier failure")
	}
	if strings.Contains(response.Body.String(), token) || strings.Contains(response.Body.String(), "signature failure") {
		t.Fatal("response exposed token or verifier error")
	}
}

func TestAuthenticationMiddlewareDoesNotRetainRawToken(t *testing.T) {
	rawToken := "valid-but-sensitive-token"
	want := Principal{UserID: uuid.New(), Role: "authenticated"}
	router := authTestRouter(verifierFunc(func(_ context.Context, token string) (Principal, error) {
		if token != rawToken {
			t.Fatalf("token = %q", token)
		}
		return want, nil
	}), func(c *gin.Context) {
		for _, value := range c.Keys {
			if token, ok := value.(string); ok && token == rawToken {
				t.Fatal("raw token was retained in Gin context")
			}
		}
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+rawToken)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
}

func authTestRouter(verifier Verifier, handler gin.HandlerFunc) http.Handler {
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware())
	router.GET("/protected", AuthenticationMiddleware(verifier), handler)
	return router
}

func assertUnauthorized(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if response.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Error("401 response is missing WWW-Authenticate: Bearer")
	}
	if !strings.Contains(response.Body.String(), `"code":"unauthorized"`) {
		t.Fatalf("body = %s, want unauthorized error contract", response.Body.String())
	}
	if response.Header().Get("X-Request-ID") == "" {
		t.Fatal("401 response is missing X-Request-ID")
	}
}
