//nolint:errcheck // Test socket cleanup and intentionally malformed fixture setup are best effort.
package mosquittoreload

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func testServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := NewServer(Config{SocketPath: filepath.Join(dir, "r.sock"), Timeout: 100 * time.Millisecond, MaxConnections: 2})
	if e != nil {
		t.Fatal(e)
	}
	s.uid = uint32(os.Geteuid())
	return s, s.config.SocketPath
}

func start(t *testing.T, s *Server) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	for i := 0; i < 200; i++ {
		if _, e := os.Lstat(s.config.SocketPath); e == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			t.Fatal("shutdown leaked")
		}
	}
}

func exchange(t *testing.T, path, request string) string {
	t.Helper()
	c, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	io.WriteString(c, request)
	c.CloseWrite()
	b, _ := io.ReadAll(c)
	return string(b)
}

func TestProtocol(t *testing.T) {
	s, path := testServer(t)
	var count atomic.Int32
	s.signal = func() error { count.Add(1); return nil }
	stop := start(t, s)
	defer stop()
	for _, v := range []string{"", "reload", "reload\nextra", "reload\n\n", "reload\r\n", "kill 1\n", "reload\x00", "\nreload\n"} {
		if got := exchange(t, path, v); got != "" {
			t.Fatalf("unexpected ACK %q", got)
		}
	}
	if count.Load() != 0 {
		t.Fatal("invalid request signalled")
	}
	if got := exchange(t, path, "reload\n"); got != "signalled\n" {
		t.Fatal(got)
	}
	s.signal = func() error { return ErrUnavailable } // previous request has completed
	if got := exchange(t, path, "reload\n"); got != "" {
		t.Fatal("failed signal ACK")
	}
}

func TestSlowTrailingBoundedShutdown(t *testing.T) {
	s, path := testServer(t)
	var count atomic.Int32
	s.signal = func() error { count.Add(1); return nil }
	stop := start(t, s)
	c, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	io.WriteString(c, "reload\n")
	time.Sleep(15 * time.Millisecond)
	io.WriteString(c, "x")
	c.CloseWrite()
	b, _ := io.ReadAll(c)
	if len(b) != 0 || count.Load() != 0 {
		t.Fatal("trailing request signalled")
	}
	var conns []net.Conn
	for i := 0; i < 6; i++ {
		c, e := net.Dial("unix", path)
		if e == nil {
			conns = append(conns, c)
		}
	}
	stop()
	for _, c := range conns {
		c.Close()
	}
	if _, e := os.Lstat(path); !os.IsNotExist(e) {
		t.Fatal("socket retained")
	}
}

func TestSocketSafety(t *testing.T) {
	for _, kind := range []string{"file", "symlink", "permissions", "active", "stale", "replacement", "dir", "ancestor"} {
		t.Run(kind, func(t *testing.T) {
			s, path := testServer(t)
			var other *net.UnixListener
			switch kind {
			case "file":
				os.WriteFile(path, []byte("retain"), 0600)
			case "symlink":
				os.Symlink("missing", path)
			case "dir":
				os.Chmod(filepath.Dir(path), 0755)
			case "ancestor":
				real := filepath.Dir(path)
				link := real + "-link"
				os.Symlink(real, link)
				defer os.Remove(link)
				s.config.SocketPath = filepath.Join(link, "r.sock")
			default:
				var e error
				other, e = net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if e != nil {
					t.Fatal(e)
				}
				other.SetUnlinkOnClose(false)
				defer other.Close()
				os.Chmod(path, 0600)
				if kind == "permissions" {
					os.Chmod(path, 0666)
				}
				if kind == "stale" || kind == "replacement" {
					other.Close()
				}
			}
			l, inode, lock, e := s.listen()
			if kind != "stale" && kind != "replacement" {
				if e == nil {
					l.Close()
					lock.Close()
					t.Fatal("unsafe accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			l.Close()
			lock.Close()
			if kind == "replacement" {
				os.Remove(path)
				os.WriteFile(path, []byte("keep"), 0600)
				if sameSocket(path, inode, s.uid) {
					t.Fatal("replacement owned")
				}
			}
		})
	}
}

func TestRestartAndLock(t *testing.T) {
	s, _ := testServer(t)
	s.signal = func() error { return nil }
	stop := start(t, s)
	if _, _, _, e := s.listen(); e == nil {
		t.Fatal("second server accepted")
	}
	stop()
	stop = start(t, s)
	stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := s.Serve(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestConfig(t *testing.T) {
	for _, c := range []Config{{}, {SocketPath: "relative"}, {SocketPath: "/a/../b"}, {SocketPath: "/a", Timeout: -1}, {SocketPath: "/a", Timeout: time.Minute}, {SocketPath: "/a", MaxConnections: 129}} {
		if _, e := NewServer(c); e == nil {
			t.Fatal(c)
		}
	}
	s, _ := testServer(t)
	s.uid = ^uint32(0)
	if _, _, _, e := s.listen(); e == nil {
		t.Fatal("UID accepted")
	}
}

func TestLockAndPeerSafety(t *testing.T) {
	for _, kind := range []string{"symlink", "mode", "directory"} {
		t.Run(kind, func(t *testing.T) {
			s, path := testServer(t)
			lock := filepath.Join(filepath.Dir(path), ".reload.lock")
			switch kind {
			case "symlink":
				os.Symlink("missing", lock)
			case "mode":
				os.WriteFile(lock, nil, 0644)
			case "directory":
				os.Mkdir(lock, 0600)
			}
			if _, _, _, err := s.listen(); err == nil {
				t.Fatal("unsafe lock accepted")
			}
		})
	}
	s, path := testServer(t)
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	c, err := l.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.uid = ^uint32(0)
	s.signal = func() error { t.Error("wrong peer signalled"); return nil }
	s.handle(c)
	if peerUID(c) != uint32(os.Geteuid()) {
		t.Fatal("peer credential mismatch")
	}
	c.Close()
	if peerUID(c) != ^uint32(0) {
		t.Fatal("closed peer accepted")
	}
}
