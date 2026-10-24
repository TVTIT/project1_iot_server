package auth

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestInternalAuthHandlerUsesStrictBearer(t *testing.T) {
	v := newTestVerifier(t, 0)
	h := NewInternalAuthHandler(v, &Metrics{}, nil)
	valid := signToken(t, jwt.SigningMethodHS256, []byte(testSecret), validClaims(uuid.New()))
	for _, tc := range []struct {
		path, header string
		want         int
	}{
		{"/internal/auth/verify", "Bearer " + valid, 204},
		{"/internal/auth/verify", "", 401},
		{"/internal/auth/verify", "Bearer invalid", 401},
		{"/internal/auth/verify", "Bearer " + valid + ", Bearer " + valid, 401},
		{"/v1/admin/metrics", "Bearer " + valid, 404},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		r.Header.Set("Authorization", tc.header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s: got %d want %d", tc.path, w.Code, tc.want)
		}
	}
}

func TestUnixAuthSocketPermissionsAndLifecycle(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "auth.sock")
	s, err := ListenUnixAuth(path, NewInternalAuthHandler(newTestVerifier(t, 0), nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	st, err := os.Lstat(path)
	if err != nil || st.Mode().Perm() != 0660 {
		t.Fatal("socket mode", st, err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	r, err := client.Get("http://unix/internal/auth/verify")
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	if r.StatusCode != 401 {
		t.Fatal(r.StatusCode)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("socket was not removed", err)
	}
	if err := os.WriteFile(path, []byte("do not delete"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUnixAuth(path, http.NotFoundHandler()); err == nil {
		t.Fatal("overwrote existing file")
	}
	if body, _ := os.ReadFile(path); string(body) != "do not delete" {
		t.Fatal("file changed")
	}
}
