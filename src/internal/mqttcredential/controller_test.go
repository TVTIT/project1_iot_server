package mqttcredential

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestControllerFilesystemAndUID(t *testing.T) {
	l := fixtureLifecycle(t)
	dir := t.TempDir()
	cfg := ControllerConfig{ControlDir: dir, UID: uint32(os.Geteuid()), Timeout: 50 * time.Millisecond, MaxFrameBytes: 1024, MaxInflight: 2, ManagementAddress: "127.0.0.1:1"}
	s, e := NewLifecycleController(l, cfg)
	if e != nil {
		t.Fatal(e)
	}
	os.Chmod(dir, 0755)
	if s.Serve(context.Background()) == nil {
		t.Fatal("unsafe directory accepted")
	}
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "control.sock")
	os.WriteFile(path, []byte("keep"), 0600)
	if s.Serve(context.Background()) == nil {
		t.Fatal("non socket removed")
	}
	if b, _ := os.ReadFile(path); string(b) != "keep" {
		t.Fatal("file changed")
	}
	os.Remove(path)
	live, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	live.SetUnlinkOnClose(false)
	os.Chmod(path, 0600)
	if s.Serve(context.Background()) == nil {
		t.Fatal("live socket replaced")
	}
	live.Close()
	// Owned refused socket is safely replaced, but lock prevents a second owner.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	client := ControllerClient{ControlDir: dir, Timeout: time.Second, MaxFrameBytes: 1024}
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		if _, e = client.Describe(ctx); e == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	if s.Serve(context.Background()) == nil {
		t.Fatal("second owner accepted")
	}
	conn, e := net.Dial("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], 1025)
	conn.Write(n[:])
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, e = conn.Read(b[:]); e == nil {
		t.Fatal("oversize frame accepted")
	}
	conn.Close()
	// Fill both admission slots with incomplete headers. Excess connections
	// are immediately rejected; cancellation must close pending readers.
	var pending []net.Conn
	for range 2 {
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		pending = append(pending, c)
	}
	time.Sleep(5 * time.Millisecond)
	excess, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	excess.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = excess.Read(b[:]); err == nil {
		t.Fatal("unbounded IPC admission")
	}
	excess.Close()
	for _, c := range pending {
		defer c.Close()
	}
	// Real SO_PEERCRED observed, mismatch closes before processing payload.
	peerListener, e := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "peer.sock"), Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer peerListener.Close()
	peer, e := net.Dial("unix", peerListener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	accepted, e := peerListener.AcceptUnix()
	if e != nil {
		t.Fatal(e)
	}
	if controllerPeerUID(accepted) != uint32(os.Geteuid()) {
		t.Fatal("peer UID mismatch")
	}
	peer.Close()
	accepted.Close()
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
