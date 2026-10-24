package auth

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/metrics"
)

// NewInternalAuthHandler exposes identity verification only on its own router.
// Resource authorization remains on the independent business router.
func NewInternalAuthHandler(verifier Verifier, observer *Metrics, duration *metrics.Histogram) http.Handler {
	router := gin.New()
	router.HandleMethodNotAllowed = true
	router.GET("/internal/auth/verify", AuthenticationMiddleware(verifier, observer), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		started := time.Now()
		if duration != nil {
			defer func() { duration.Observe(time.Since(started)) }()
		}
		router.ServeHTTP(w, r)
	})
}

// UnixAuthServer owns exactly one socket and a bounded HTTP server.
type UnixAuthServer struct {
	server *http.Server
	errors chan error
}

// ListenUnixAuth refuses all existing paths, including stale sockets. Operators
// must explicitly clear a known stale socket; arbitrary files are never removed.
func ListenUnixAuth(path string, handler http.Handler) (*UnixAuthServer, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 || handler == nil {
		return nil, fmt.Errorf("invalid auth socket configuration")
	}
	parent := filepath.Dir(path)
	real, err := filepath.EvalSymlinks(parent)
	if err != nil || real != parent {
		return nil, fmt.Errorf("auth socket directory must exist without symlinks")
	}
	st, err := os.Stat(parent)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0007 != 0 {
		return nil, fmt.Errorf("auth socket directory must deny other users")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return nil, fmt.Errorf("auth socket path already exists or is inaccessible")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("auth socket bind failed")
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(path, 0660); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("auth socket permissions failed")
	}
	s := &UnixAuthServer{server: &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second,
		MaxHeaderBytes: 24 * 1024}, errors: make(chan error, 1)}
	go func() { s.errors <- s.server.Serve(listener) }()
	return s, nil
}

// Errors reports an unexpected listener/server termination to composition.
func (s *UnixAuthServer) Errors() <-chan error { return s.errors }

// Close drains verification requests within the caller's shared shutdown budget.
func (s *UnixAuthServer) Close(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err != nil {
		_ = s.server.Close()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
