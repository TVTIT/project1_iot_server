package mqttcredential

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"path/filepath"
	"time"
)

// ControllerClient belongs only in the trusted backend. Directory access and
// matching SO_PEERCRED UID are capabilities, not human/Gateway authorization.
// No public API route should expose this object or verification challenges.
type ControllerClient struct {
	ControlDir    string
	Timeout       time.Duration
	MaxFrameBytes int
}

func (c ControllerClient) valid() bool {
	return filepath.IsAbs(c.ControlDir) && filepath.Clean(c.ControlDir) == c.ControlDir && len(c.ControlDir) <= 75 && c.Timeout > 0 && c.Timeout <= time.Minute && c.MaxFrameBytes >= 256 && c.MaxFrameBytes <= 65536
}
func (c ControllerClient) call(ctx context.Context, op string, r VerificationReceipt) (LifecycleDescription, error) {
	if !c.valid() {
		return LifecycleDescription{}, ErrLifecycleUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	conn, e := (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(c.ControlDir, "control.sock"))
	if e != nil {
		return LifecycleDescription{}, ErrLifecycleUnavailable
	}
	// The response/read error is authoritative; transport teardown is best-effort.
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if writeControllerFrame(conn, controllerRequest{Operation: op, Receipt: r}, c.MaxFrameBytes) != nil {
		return LifecycleDescription{}, ErrLifecycleUnavailable
	}
	b, e := readControllerFrame(conn, c.MaxFrameBytes)
	if e != nil {
		return LifecycleDescription{}, e
	}
	var response controllerResponse
	if json.Unmarshal(b, &response) != nil || !response.OK {
		return LifecycleDescription{}, ErrLifecycleUnavailable
	}
	return response.Description, nil
}

// Describe queries current lifecycle status from the broker lifecycle controller.
func (c ControllerClient) Describe(ctx context.Context) (LifecycleDescription, error) {
	return c.call(ctx, "Describe", VerificationReceipt{})
}

// CloseDrain commands the controller to drain and close external broker access.
func (c ControllerClient) CloseDrain(ctx context.Context) error {
	_, e := c.call(ctx, "CloseDrain", VerificationReceipt{})
	return e
}

// RestartClosed requests restarting the broker in a closed maintenance state.
func (c ControllerClient) RestartClosed(ctx context.Context) (LifecycleDescription, error) {
	return c.call(ctx, "RestartClosed", VerificationReceipt{})
}

// OpenVerified unblocks normal broker traffic after verification receipt presentation.
func (c ControllerClient) OpenVerified(ctx context.Context, r VerificationReceipt) error {
	_, e := c.call(ctx, "OpenVerified", r)
	return e
}

// ManagementDial supplies raw transport only. TLS remains end-to-end between
// backend and broker, with the originally configured URL hostname and CA.
// No address can be requested through IPC: server selects its fixed loopback.
func (c ControllerClient) ManagementDial(ctx context.Context) (net.Conn, error) {
	if !c.valid() {
		return nil, ErrLifecycleUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	conn, e := (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(c.ControlDir, "management.sock"))
	if e != nil {
		return nil, ErrLifecycleUnavailable
	}
	return conn, nil
}

// DialManagementTLS supports fresh-login probes with the same mandatory CA/SAN
// checks as DynSec. Callers must supply their explicit public trust bundle.
func (c ControllerClient) DialManagementTLS(ctx context.Context, config *tls.Config) (net.Conn, error) {
	if config == nil || config.RootCAs == nil || config.ServerName == "" || config.InsecureSkipVerify || config.MinVersion < tls.VersionTLS12 {
		return nil, ErrLifecycleUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	raw, e := c.ManagementDial(ctx)
	if e != nil {
		return nil, e
	}
	conn := tls.Client(raw, config.Clone())
	if conn.HandshakeContext(ctx) != nil {
		_ = raw.Close()
		return nil, ErrLifecycleUnavailable
	}
	return conn, nil
}
