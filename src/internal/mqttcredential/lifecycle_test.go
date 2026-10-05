package mqttcredential

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixtureLifecycle(t *testing.T) *BrokerLifecycle {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// A subprocess of this test binary is the fixed trusted fixture child.
	l, err := NewBrokerLifecycle(LifecycleConfig{Executable: executable, Args: []string{"-test.run=^TestLifecycleChild$"}, ChildEnv: []string{"TASK265B_CHILD=1"}, PublicAddresses: []string{"127.0.0.1:0", "127.0.0.1:0"}, BrokerAddress: "127.0.0.1:1", MaxConnections: 2, DialTimeout: time.Second, StopTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = l.StartClosed(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Shutdown() })
	return l
}

func TestLifecycleChild(_ *testing.T) {
	if os.Getenv("TASK265B_CHILD") != "1" {
		return
	}
	if os.Getenv("PGPASSWORD") != "" || os.Getenv("JWT_SECRET") != "" {
		os.Exit(9)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestLifecycleEpochReceiptAndDeath(t *testing.T) {
	t.Setenv("PGPASSWORD", "not-inherited")
	t.Setenv("JWT_SECRET", "not-inherited")
	l := fixtureLifecycle(t)
	first := l.Describe()
	if first.Open || first.Epoch == "" || first.Nonce == "" {
		t.Fatal("must start closed with fresh identity")
	}
	bad := VerificationReceipt{Epoch: first.Epoch, Nonce: "wrong"}
	if l.OpenVerified(bad) == nil {
		t.Fatal("wrong nonce accepted")
	}
	receipt := VerificationReceipt{Epoch: first.Epoch, Nonce: first.Nonce}
	if err := l.OpenVerified(receipt); err != nil {
		t.Fatal(err)
	}
	if l.OpenVerified(receipt) == nil {
		t.Fatal("receipt replay accepted")
	}
	if err := l.CloseDrain(); err != nil {
		t.Fatal(err)
	}
	if l.OpenVerified(receipt) == nil {
		t.Fatal("old maintenance receipt accepted")
	}
	if err := l.RestartClosed(); err != nil {
		t.Fatal(err)
	}
	fresh := l.Describe()
	if fresh.Open || fresh.Epoch == first.Epoch {
		t.Fatal("restart inherited epoch/open")
	}
	if l.OpenVerified(receipt) == nil {
		t.Fatal("stale lifetime receipt accepted")
	}
	if err := l.OpenVerified(VerificationReceipt{Epoch: fresh.Epoch, Nonce: fresh.Nonce}); err != nil {
		t.Fatal(err)
	}
	opened := l.Describe()
	if !opened.Open || len(opened.Addresses) != 2 {
		t.Fatal("missing open ingress addresses")
	}
	l.mu.Lock()
	child := l.child
	l.mu.Unlock()
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Failed():
	case <-time.After(3 * time.Second):
		t.Fatal("death not observed")
	}
	if l.Describe().Open || l.RestartClosed() == nil {
		t.Fatal("unexpected death must be terminal")
	}
	for _, addr := range opened.Addresses {
		c, e := net.DialTimeout("tcp", addr, time.Second)
		if e == nil {
			_ = c.Close() // Preserve the dead-child gate assertion below.
			t.Fatal("dead child left gate reachable")
		}
	}
}

func TestLifecycleBindFailureConsumesReceipt(t *testing.T) {
	l := fixtureLifecycle(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	l.mu.Lock()
	l.cfg.PublicAddresses = []string{"127.0.0.1:0", occupied.Addr().String()}
	l.mu.Unlock()
	d := l.Describe()
	receipt := VerificationReceipt{Epoch: d.Epoch, Nonce: d.Nonce}
	if l.OpenVerified(receipt) == nil {
		t.Fatal("conflicting bind accepted")
	}
	d = l.Describe()
	if d.Open || d.Nonce != "" || !d.Alive {
		t.Fatal("bind failure must consume challenge and stay closed")
	}
	_ = occupied.Close()
	if l.OpenVerified(receipt) == nil {
		t.Fatal("bind failure receipt replay accepted")
	}
	if err := l.CloseDrain(); err != nil {
		t.Fatal(err)
	}
	d = l.Describe()
	if d.Nonce == "" || d.Nonce == receipt.Nonce {
		t.Fatal("maintenance did not reset challenge")
	}
	if err := l.OpenVerified(VerificationReceipt{Epoch: d.Epoch, Nonce: d.Nonce}); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleFailedSpawn(t *testing.T) {
	l := fixtureLifecycle(t)
	l.mu.Lock()
	l.cfg.Executable = filepath.Join(t.TempDir(), "missing")
	l.mu.Unlock()
	if l.RestartClosed() == nil || l.Describe().Open {
		t.Fatal("failed restart opened gate")
	}
}

func TestControllerPrivateHandshake(t *testing.T) {
	l := fixtureLifecycle(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := ControllerConfig{ControlDir: dir, UID: uint32(os.Geteuid()), Timeout: time.Second, MaxFrameBytes: 1024, MaxInflight: 2, ManagementAddress: "127.0.0.1:1"}
	s, err := NewLifecycleController(l, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	client := ControllerClient{ControlDir: dir, Timeout: time.Second, MaxFrameBytes: 1024}
	var state LifecycleDescription
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		state, err = client.Describe(ctx)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = client.OpenVerified(ctx, VerificationReceipt{Epoch: state.Epoch, Nonce: state.Nonce}); err != nil {
		t.Fatal(err)
	}
	if err = client.OpenVerified(ctx, VerificationReceipt{Epoch: state.Epoch, Nonce: state.Nonce}); err == nil {
		t.Fatal("IPC replay accepted")
	}
	if err = client.CloseDrain(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not drain")
	}
}
