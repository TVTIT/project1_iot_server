//nolint:misspell // Native utility version output must use the exact Mosquitto product name.
package mqttcredential

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

// Probe never treats transport failure as authentication rejection.
type Probe struct {
	broker    string
	tls       *tls.Config
	timeout   time.Duration
	newClient func(*mqtt.ClientOptions) mqtt.Client
}

// NewProbe creates a fresh-connection TLS authentication verifier.
func NewProbe(broker, ca string, timeout time.Duration) (*Probe, error) {
	u, e := url.Parse(broker)
	if e != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || !cleanPath(ca) || timeout <= 0 || timeout > time.Minute {
		return nil, ErrInvalidInput
	}
	st, e := os.Lstat(ca)
	if e != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
		return nil, ErrInvalidInput
	}
	b, e := os.ReadFile(ca)
	roots := x509.NewCertPool()
	if e != nil || !roots.AppendCertsFromPEM(b) {
		return nil, ErrInvalidInput
	}
	return &Probe{broker: broker, tls: &tls.Config{RootCAs: roots, ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}, timeout: timeout, newClient: mqtt.NewClient}, nil
}

// Login requires a successful authentication CONNACK.
func (p *Probe) Login(ctx context.Context, username, password string) error {
	return p.check(ctx, username, password, false)
}

// Rejected requires an explicit authentication-denied CONNACK, not a network failure.
func (p *Probe) Rejected(ctx context.Context, username, password string) error {
	return p.check(ctx, username, password, true)
}

func (p *Probe) check(ctx context.Context, username, password string, negative bool) error {
	if username == "" || !validPassword(password) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if ctx.Err() != nil {
		return ErrVerificationFailed
	}
	id := make([]byte, 16)
	if _, e := rand.Read(id); e != nil {
		return ErrVerificationFailed
	}
	opts := mqtt.NewClientOptions().AddBroker(p.broker).SetClientID("cred-" + hex.EncodeToString(id)).SetUsername(username).SetPassword(password).SetProtocolVersion(4).SetAutoReconnect(false).SetConnectRetry(false).SetCleanSession(true).SetConnectTimeout(p.timeout).SetWriteTimeout(p.timeout)
	// Own the transport so cancellation interrupts TLS, CONNACK and cleanup;
	// wait for Connect's token instead of abandoning its goroutine on timeout.
	var mu sync.Mutex
	var conn net.Conn
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
	defer stop()
	opts.SetCustomOpenConnectionFn(func(u *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		d := tls.Dialer{NetDialer: &net.Dialer{}, Config: p.tls.Clone()}
		c, e := d.DialContext(ctx, "tcp", u.Host)
		if e != nil {
			return nil, e
		}
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil {
			_ = c.Close()
			return nil, ctx.Err()
		}
		conn = c
		deadline, _ := ctx.Deadline()
		_ = c.SetDeadline(deadline)
		return c, nil
	})
	client := p.newClient(opts)
	token := client.Connect()
	<-token.Done()
	err := token.Error()
	client.Disconnect(0)
	mu.Lock()
	if conn != nil {
		_ = conn.Close()
	}
	mu.Unlock()
	if ctx.Err() != nil {
		return ErrVerificationFailed
	}
	if negative {
		if errors.Is(err, packets.ConnErrors[packets.ErrRefusedBadUsernameOrPassword]) || errors.Is(err, packets.ConnErrors[packets.ErrRefusedNotAuthorised]) {
			return nil
		}
		return ErrVerificationFailed
	}
	if err != nil {
		return ErrVerificationFailed
	}
	return nil
}

// Start validates local state, performs a non-signalling socket liveness check,
// and authenticates over fresh TLS before the HTTP listener is opened.
func (c StartupConfig) Start(ctx context.Context) (*Runtime, error) {
	if !c.Enabled {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.Runtime.OperationTimeout)
	defer cancel()
	st, e := os.Lstat(c.Runtime.Tool.Path)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 || st.Mode().Perm()&0022 != 0 {
		return nil, ErrToolFailure
	}
	if checkUtility(ctx, c.Runtime.Tool) != nil {
		return nil, ErrToolFailure
	}
	reload, e := NewReloadClient(ReloadClientConfig{SocketPath: c.SocketPath, Timeout: c.Runtime.ReloadTimeout})
	if e != nil {
		return nil, e
	}
	rc := c.Runtime
	rc.Reload = reload.Reload
	r, e := NewRuntime(rc)
	if e != nil {
		return nil, e
	}
	if e = r.CheckRuntime(ctx); e != nil {
		return nil, e
	}
	if e = reload.Check(ctx); e != nil {
		return nil, e
	}
	p, e := NewProbe(c.BrokerURL, c.CAFile, c.Runtime.ProbeTimeout)
	if e != nil {
		return nil, e
	}
	if e = p.Login(ctx, c.Username, c.Password); e != nil {
		return nil, e
	}
	return r, nil
}

// Read-only capability check. No credential file or password is passed.
func checkUtility(ctx context.Context, t NativeTool) error {
	ctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, t.Path, "-h")
	cmd.Env = []string{"LC_ALL=C"}
	var output cappedOutput
	cmd.Stdout = &output
	cmd.Stderr = &boundedDiscard{remaining: 4096}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 100 * time.Millisecond
	err := cmd.Run()
	var exit *exec.ExitError
	if ctx.Err() != nil || (err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1)) || !strings.Contains(output.String(), "mosquitto_passwd is a tool") || !strings.Contains(output.String(), "sha512-pbkdf2") {
		return ErrToolFailure
	}
	return nil
}

type cappedOutput struct{ bytes.Buffer }

func (b *cappedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if left := 4096 - b.Len(); left > 0 {
		if len(p) > left {
			p = p[:left]
		}
		b.Buffer.Write(p)
	}
	return n, nil
}
