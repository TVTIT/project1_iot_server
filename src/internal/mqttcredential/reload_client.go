package mqttcredential

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// ReloadClientConfig bounds signal request latency and selects the Unix socket.
type ReloadClientConfig struct {
	SocketPath string
	Timeout    time.Duration
}

// ReloadClient requests signal delivery, not broker reload completion.
type ReloadClient struct{ config ReloadClientConfig }

// Check proves socket reachability only, without sending a reload request.
func (r *ReloadClient) Check(ctx context.Context) error {
	if secureDir(filepath.Dir(r.config.SocketPath)) != nil {
		return ErrReloadUnavailable
	}
	st, e := os.Lstat(r.config.SocketPath)
	if e != nil || st.Mode() != os.ModeSocket|0600 || !ownerIdentity(st) {
		return ErrReloadUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()
	c, e := (&net.Dialer{}).DialContext(ctx, "unix", r.config.SocketPath)
	if e != nil {
		return ErrReloadUnavailable
	}
	_ = c.Close()
	return nil
}

// NewReloadClient validates the private control socket configuration.
func NewReloadClient(c ReloadClientConfig) (*ReloadClient, error) {
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Second
	}
	if !filepath.IsAbs(c.SocketPath) || filepath.Clean(c.SocketPath) != c.SocketPath || len(c.SocketPath) > 100 || c.Timeout <= 0 || c.Timeout > 30*time.Second {
		return nil, ErrInvalidInput
	}
	return &ReloadClient{config: c}, nil
}

// Reload is assignable to RuntimeConfig.Reload. The exact ACK proves a signal
// only; the runtime's fresh MQTT probe is still mandatory.
func (r *ReloadClient) Reload(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	defer cancel()
	c, e := (&net.Dialer{}).DialContext(ctx, "unix", r.config.SocketPath)
	if e != nil {
		return ErrReloadUnavailable
	}
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if c.SetDeadline(deadline) != nil {
		return ErrReloadUnavailable
	}
	n, e := io.WriteString(c, "reload\n")
	if e != nil || n != 7 {
		return ErrReloadUnavailable
	}
	u, ok := c.(*net.UnixConn)
	if !ok || u.CloseWrite() != nil {
		return ErrReloadUnavailable
	}
	b, e := io.ReadAll(io.LimitReader(c, 11))
	if e != nil || string(b) != "signalled\n" || ctx.Err() != nil {
		return ErrReloadUnavailable
	}
	return nil
}
