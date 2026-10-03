// Package mosquittoreload implements the private, signal-only broker control socket.
package mosquittoreload

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// ErrUnavailable hides control-plane details from socket clients.
var ErrUnavailable = errors.New("broker reload unavailable")

// Config bounds the private socket server and injects its broker signal function.
type Config struct {
	SocketPath     string
	Timeout        time.Duration
	MaxConnections int
}

// NewServer does not create files. Production identity is fixed to UID 1883 and
// broker PID 1; neither request data nor configuration can select a process.
func NewServer(c Config) (*Server, error) {
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Second
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 8
	}
	if !filepath.IsAbs(c.SocketPath) || filepath.Clean(c.SocketPath) != c.SocketPath || len(c.SocketPath) > 100 || c.Timeout <= 0 || c.Timeout > 30*time.Second || c.MaxConnections < 1 || c.MaxConnections > 128 {
		return nil, ErrUnavailable
	}
	return &Server{config: c, uid: 1883, signal: signalBroker}, nil
}

// Server serves bounded signal requests on a private Unix socket.
type Server struct {
	config Config
	uid    uint32
	signal func() error
}

func owned(info os.FileInfo, uid uint32, mode os.FileMode) bool {
	return fileUID(info) == uid && info.Mode() == mode
}

// The initializer must create the control directory. Never chmod/chown an
// existing path or follow symlinks (including ancestor components).
func (s *Server) checkDir() error {
	dir := filepath.Dir(s.config.SocketPath)
	for p := dir; ; p = filepath.Dir(p) {
		i, e := os.Lstat(p)
		if e != nil || !i.IsDir() || i.Mode()&os.ModeSymlink != 0 {
			return ErrUnavailable
		}
		if p == dir && !owned(i, s.uid, os.ModeDir|0700) {
			return ErrUnavailable
		}
		if p == "/" {
			break
		}
	}
	return nil
}

func sameSocket(path string, original os.FileInfo, uid uint32) bool {
	i, e := os.Lstat(path)
	return e == nil && owned(i, uid, os.ModeSocket|0600) && os.SameFile(i, original)
}

func (s *Server) listen() (*net.UnixListener, os.FileInfo, *os.File, error) {
	if uint32(os.Geteuid()) != s.uid || s.checkDir() != nil {
		return nil, nil, nil, ErrUnavailable
	}
	// Keep a stable lock inode across restarts. It serializes stale cleanup and
	// bind between cooperating daemons, including before the listener is ready.
	fd, e := unix.Open(filepath.Join(filepath.Dir(s.config.SocketPath), ".reload.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, nil, nil, ErrUnavailable
	}
	lock := os.NewFile(uintptr(fd), "reload lock")
	fail := func() (*net.UnixListener, os.FileInfo, *os.File, error) {
		_ = lock.Close()
		return nil, nil, nil, ErrUnavailable
	}
	i, e := lock.Stat()
	if e != nil || !owned(i, s.uid, 0600) || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return fail()
	}
	if old, e := os.Lstat(s.config.SocketPath); e == nil {
		if !owned(old, s.uid, os.ModeSocket|0600) {
			return fail()
		}
		c, err := net.DialTimeout("unix", s.config.SocketPath, s.config.Timeout)
		if err == nil {
			_ = c.Close()
			return fail()
		}
		// Only ECONNREFUSED proves a dead listener. Timeouts/permissions don't.
		if !errors.Is(err, unix.ECONNREFUSED) || !sameSocket(s.config.SocketPath, old, s.uid) || os.Remove(s.config.SocketPath) != nil {
			return fail()
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return fail()
	}
	l, e := net.ListenUnix("unix", &net.UnixAddr{Name: s.config.SocketPath, Net: "unix"})
	if e != nil {
		return fail()
	}
	l.SetUnlinkOnClose(false)
	_, e = os.Lstat(s.config.SocketPath)
	if e != nil {
		_ = l.Close()
		return fail()
	}
	if os.Chmod(s.config.SocketPath, 0600) != nil {
		_ = l.Close()
		return fail()
	}
	i, e = os.Lstat(s.config.SocketPath)
	if e != nil || !owned(i, s.uid, os.ModeSocket|0600) {
		_ = l.Close()
		return fail()
	}
	return l, i, lock, nil
}

// Serve closes the listener and every accepted connection on cancellation and
// waits for the bounded handlers. ACK means only SIGHUP was sent, not reload.
func (s *Server) Serve(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	l, inode, lock, e := s.listen()
	if e != nil {
		return ErrUnavailable
	}
	defer func() { _ = lock.Close() }()
	defer func() {
		_ = l.Close()
		if sameSocket(s.config.SocketPath, inode, s.uid) {
			_ = os.Remove(s.config.SocketPath)
		}
	}()
	var mu sync.Mutex
	connections := make(map[*net.UnixConn]struct{})
	var wg sync.WaitGroup
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = l.Close()
			mu.Lock()
			for c := range connections {
				_ = c.Close()
			}
			mu.Unlock()
		case <-done:
		}
	}()
	for {
		c, err := l.AcceptUnix()
		if err != nil {
			mu.Lock()
			for conn := range connections {
				_ = conn.Close()
			}
			mu.Unlock()
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return ErrUnavailable
		}
		mu.Lock()
		if ctx.Err() != nil || len(connections) >= s.config.MaxConnections {
			mu.Unlock()
			_ = c.Close()
			continue
		}
		connections[c] = struct{}{}
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = c.Close(); mu.Lock(); delete(connections, c); mu.Unlock() }()
			s.handle(c)
		}()
	}
}

func (s *Server) handle(c *net.UnixConn) {
	if c.SetDeadline(time.Now().Add(s.config.Timeout)) != nil || peerUID(c) != s.uid {
		return
	}
	// One frame per connection, terminated by write EOF. Waiting for EOF rejects
	// even delayed trailing bytes before any signal; clients must CloseWrite.
	b, e := io.ReadAll(io.LimitReader(c, 8))
	if e != nil || string(b) != "reload\n" {
		return
	}
	if s.signal() != nil {
		return
	}
	_, _ = io.WriteString(c, "signalled\n")
}
