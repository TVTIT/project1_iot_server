package mqttcredential

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ErrLifecycleUnavailable indicates that broker lifecycle control cannot be performed.
var ErrLifecycleUnavailable = errors.New("broker lifecycle unavailable")

// LifecycleConfig is trusted startup configuration, NEVER IPC request data.
// ChildEnv is an explicit allowlist: the parent's environment is not inherited.
// Only PATH/LANG/LC_* and the test fixture marker are supported; no credentials.
type LifecycleConfig struct {
	Executable      string
	Args            []string
	ChildEnv        []string
	PublicAddresses []string
	BrokerAddress   string
	MaxConnections  int
	DialTimeout     time.Duration
	StopTimeout     time.Duration
}

// VerificationReceipt proves completion of verification across broker lifecycle epoch and challenge nonce.
type VerificationReceipt struct {
	Epoch string `json:"epoch"`
	Nonce string `json:"nonce"`
}

// LifecycleDescription describes current broker child process and ingress gate state.
type LifecycleDescription struct {
	Epoch     string   `json:"epoch"`
	Nonce     string   `json:"nonce"`
	Open      bool     `json:"open"`
	Alive     bool     `json:"alive"`
	Addresses []string `json:"addresses,omitempty"`
}
type brokerChild struct {
	cmd      *exec.Cmd
	exited   chan struct{}
	expected bool
}

// BrokerLifecycle owns exactly one child. An unexpected child exit is terminal:
// it closes/drains ingress and signals the wrapper to exit, never respawns.
// Receipts assert backend verification, not a DB proof. Trusted backend must
// reconcile authoritative decisions, verify RAM/snapshot and finalize first.
type BrokerLifecycle struct {
	mu           sync.Mutex
	cfg          LifecycleConfig
	child        *brokerChild
	gate         *ingressGate
	epoch, nonce string
	terminal     bool
	failed       chan struct{}
	failOnce     sync.Once
}

func loopbackEndpoint(a string) bool {
	h, p, e := net.SplitHostPort(a)
	if e != nil || p == "" || p == "0" {
		return false
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// NewBrokerLifecycle validates configuration and creates a broker child and gate lifecycle manager.
func NewBrokerLifecycle(c LifecycleConfig) (*BrokerLifecycle, error) {
	if !filepath.IsAbs(c.Executable) || filepath.Clean(c.Executable) != c.Executable || !loopbackEndpoint(c.BrokerAddress) || len(c.PublicAddresses) < 1 || len(c.PublicAddresses) > 8 || c.MaxConnections < 1 || c.MaxConnections > 4096 || c.DialTimeout <= 0 || c.DialTimeout > time.Minute || c.StopTimeout <= 0 || c.StopTimeout > time.Minute {
		return nil, ErrLifecycleUnavailable
	}
	for _, a := range c.PublicAddresses {
		if _, _, e := net.SplitHostPort(a); e != nil {
			return nil, ErrLifecycleUnavailable
		}
	}
	for _, v := range c.ChildEnv {
		k, _, ok := strings.Cut(v, "=")
		if !ok || (k != "PATH" && k != "LANG" && k != "LC_ALL" && k != "LC_CTYPE" && k != "TASK265B_CHILD") {
			return nil, ErrLifecycleUnavailable
		}
	}
	c.Args = append([]string(nil), c.Args...)
	c.ChildEnv = append([]string(nil), c.ChildEnv...)
	c.PublicAddresses = append([]string(nil), c.PublicAddresses...)
	return &BrokerLifecycle{cfg: c, failed: make(chan struct{})}, nil
}
func freshIdentity() (string, error) {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		return "", ErrLifecycleUnavailable
	}
	return hex.EncodeToString(b), nil
}

// StartClosed starts the broker child process with ingress gate closed.
func (l *BrokerLifecycle) StartClosed() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.child != nil || l.terminal {
		return ErrLifecycleUnavailable
	}
	return l.spawnLocked()
}
func (l *BrokerLifecycle) spawnLocked() error {
	epoch, e := freshIdentity()
	if e != nil {
		return e
	}
	nonce, e := freshIdentity()
	if e != nil {
		return e
	}
	cmd := exec.Command(l.cfg.Executable, l.cfg.Args...)
	cmd.Env = append([]string{"LC_ALL=C", "LANG=C"}, l.cfg.ChildEnv...)
	// Own a process group for bounded teardown of the fixed executable's children.
	// Broker runs with wrapper UID; no host PID access, sudo or privilege changes.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	// stdout/stderr intentionally discarded: native logs may contain credentials.
	if cmd.Start() != nil {
		l.epoch = ""
		l.nonce = ""
		return ErrLifecycleUnavailable
	}
	child := &brokerChild{cmd: cmd, exited: make(chan struct{})}
	l.child = child
	l.epoch = epoch
	l.nonce = nonce
	go func() {
		_ = cmd.Wait()
		close(child.exited)
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.child == child && !child.expected {
			l.failLocked()
		}
	}()
	return nil
}
func (l *BrokerLifecycle) aliveLocked() bool {
	if l.child == nil {
		return false
	}
	select {
	case <-l.child.exited:
		return false
	default:
		return true
	}
}
func (l *BrokerLifecycle) failLocked() {
	if l.gate != nil {
		l.gate.close()
		l.gate = nil
	}
	l.nonce = ""
	l.terminal = true
	l.failOnce.Do(func() { close(l.failed) })
}

// Failed returns a channel closed when the broker lifecycle enters terminal failure.
func (l *BrokerLifecycle) Failed() <-chan struct{} { return l.failed }

// Describe returns a point-in-time snapshot of broker child and ingress status.
func (l *BrokerLifecycle) Describe() LifecycleDescription {
	l.mu.Lock()
	defer l.mu.Unlock()
	d := LifecycleDescription{Epoch: l.epoch, Nonce: l.nonce, Alive: l.aliveLocked()}
	if d.Alive && l.gate != nil {
		d.Open = true
		d.Addresses = l.gate.addresses()
	}
	return d
}

// OpenVerified validates receipt epoch and nonce before opening ingress gate listeners.
func (l *BrokerLifecycle) OpenVerified(r VerificationReceipt) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminal || !l.aliveLocked() || l.gate != nil || r.Epoch == "" || r.Nonce == "" || r.Epoch != l.epoch || r.Nonce != l.nonce {
		return ErrLifecycleUnavailable
	}
	// Consume the challenge even when a bind fails. No retry/replay can auto-open.
	l.nonce = ""
	g, e := newIngressGate(l.cfg.PublicAddresses, l.cfg.BrokerAddress, l.cfg.MaxConnections, l.cfg.DialTimeout)
	if e != nil {
		return e
	}
	l.gate = g
	if !l.aliveLocked() {
		l.failLocked()
		return ErrLifecycleUnavailable
	}
	return nil
}
func (l *BrokerLifecycle) closeLocked() error {
	if l.gate != nil {
		l.gate.close()
		l.gate = nil
	}
	l.nonce = ""
	if l.terminal || !l.aliveLocked() {
		return ErrLifecycleUnavailable
	}
	nonce, e := freshIdentity()
	if e == nil {
		l.nonce = nonce
	}
	return e
}

// CloseDrain closes ingress gate listeners and generates a fresh challenge nonce.
func (l *BrokerLifecycle) CloseDrain() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeLocked()
}
func (l *BrokerLifecycle) stopLocked() error {
	if l.child == nil {
		return nil
	}
	c := l.child
	c.expected = true
	select {
	case <-c.exited:
		l.child = nil
		return nil
	default:
	}
	_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGTERM)
	timer := time.NewTimer(l.cfg.StopTimeout)
	defer timer.Stop()
	select {
	case <-c.exited:
	case <-timer.C:
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
		timer.Reset(l.cfg.StopTimeout)
		select {
		case <-c.exited:
		case <-timer.C:
			return ErrLifecycleUnavailable
		}
	}
	l.child = nil
	return nil
}

// RestartClosed closes ingress, stops the broker child process, and restarts it closed.
func (l *BrokerLifecycle) RestartClosed() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminal || !l.aliveLocked() {
		return ErrLifecycleUnavailable
	}
	if e := l.closeLocked(); e != nil {
		return e
	}
	if e := l.stopLocked(); e != nil {
		l.failLocked()
		return e
	}
	return l.spawnLocked()
}

// Shutdown closes ingress gate listeners and stops child process execution permanently.
func (l *BrokerLifecycle) Shutdown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.gate != nil {
		l.gate.close()
		l.gate = nil
	}
	l.nonce = ""
	l.terminal = true
	_ = l.stopLocked()
}
