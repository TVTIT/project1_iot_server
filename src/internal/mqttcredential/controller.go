//go:build linux

package mqttcredential

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ControllerConfig contains only startup capabilities. The management target
// is fixed numeric loopback, never supplied by a peer. A second private socket
// carries opaque TLS bytes; it is not exposed on TCP or mounted into Gateways.
type ControllerConfig struct {
	ControlDir        string
	UID               uint32
	Timeout           time.Duration
	MaxFrameBytes     int
	MaxInflight       int
	ManagementAddress string
}

// LifecycleController serves unix-domain socket control commands for broker lifecycle.
type LifecycleController struct {
	lifecycle *BrokerLifecycle
	cfg       ControllerConfig
}
type controllerRequest struct {
	Operation string              `json:"operation"`
	Receipt   VerificationReceipt `json:"receipt,omitempty"`
}
type controllerResponse struct {
	OK          bool                 `json:"ok"`
	Description LifecycleDescription `json:"description"`
}

// NewLifecycleController validates configuration and creates a broker lifecycle controller.
func NewLifecycleController(l *BrokerLifecycle, c ControllerConfig) (*LifecycleController, error) {
	if l == nil || !filepath.IsAbs(c.ControlDir) || filepath.Clean(c.ControlDir) != c.ControlDir || len(c.ControlDir) > 75 || c.Timeout <= 0 || c.Timeout > time.Minute || c.MaxFrameBytes < 256 || c.MaxFrameBytes > 65536 || c.MaxInflight < 1 || c.MaxInflight > 128 || !loopbackEndpoint(c.ManagementAddress) {
		return nil, ErrLifecycleUnavailable
	}
	return &LifecycleController{lifecycle: l, cfg: c}, nil
}
func controllerPeerUID(c *net.UnixConn) uint32 {
	uid := ^uint32(0)
	raw, e := c.SyscallConn()
	if e != nil {
		return uid
	}
	e = raw.Control(func(fd uintptr) {
		cred, e := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if e == nil {
			uid = cred.Uid
		}
	})
	if e != nil {
		return ^uint32(0)
	}
	return uid
}
func controllerOwned(i os.FileInfo, uid uint32, mode os.FileMode) bool {
	st, ok := i.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uid && i.Mode() == mode
}
func controllerSameSocket(path string, i os.FileInfo, uid uint32) bool {
	now, e := os.Lstat(path)
	return e == nil && controllerOwned(now, uid, os.ModeSocket|0600) && os.SameFile(i, now)
}
func (s *LifecycleController) checkDir() error {
	if uint32(os.Geteuid()) != s.cfg.UID {
		return ErrLifecycleUnavailable
	}
	for p := s.cfg.ControlDir; ; p = filepath.Dir(p) {
		i, e := os.Lstat(p)
		if e != nil || !i.IsDir() || i.Mode()&os.ModeSymlink != 0 {
			return ErrLifecycleUnavailable
		}
		if p == s.cfg.ControlDir && !controllerOwned(i, s.cfg.UID, os.ModeDir|0700) {
			return ErrLifecycleUnavailable
		}
		if p == "/" {
			break
		}
	}
	return nil
}

type controllerSocket struct {
	listener *net.UnixListener
	path     string
	inode    os.FileInfo
	uid      uint32
}

func (s controllerSocket) close() {
	_ = s.listener.Close()
	if controllerSameSocket(s.path, s.inode, s.uid) {
		_ = os.Remove(s.path)
	}
}
func (s *LifecycleController) listen(name string) (controllerSocket, error) {
	path := filepath.Join(s.cfg.ControlDir, name)
	if old, e := os.Lstat(path); e == nil {
		if !controllerOwned(old, s.cfg.UID, os.ModeSocket|0600) {
			return controllerSocket{}, ErrLifecycleUnavailable
		}
		c, e := net.DialTimeout("unix", path, s.cfg.Timeout)
		if e == nil {
			_ = c.Close()
			return controllerSocket{}, ErrLifecycleUnavailable
		}
		if !errors.Is(e, unix.ECONNREFUSED) || !controllerSameSocket(path, old, s.cfg.UID) || os.Remove(path) != nil {
			return controllerSocket{}, ErrLifecycleUnavailable
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return controllerSocket{}, ErrLifecycleUnavailable
	}
	l, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		return controllerSocket{}, ErrLifecycleUnavailable
	}
	l.SetUnlinkOnClose(false)
	if os.Chmod(path, 0600) != nil {
		_ = l.Close()
		return controllerSocket{}, ErrLifecycleUnavailable
	}
	i, e := os.Lstat(path)
	if e != nil || !controllerOwned(i, s.cfg.UID, os.ModeSocket|0600) {
		_ = l.Close()
		return controllerSocket{}, ErrLifecycleUnavailable
	}
	return controllerSocket{listener: l, path: path, inode: i, uid: s.cfg.UID}, nil
}

// Serve serializes stale-socket cleanup with an owned stable lock inode and
// bounds combined control + tunnel handlers. Cancellation drains all sockets.
// No payload, native errors, command args or secrets are ever logged.
func (s *LifecycleController) Serve(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	if s.checkDir() != nil {
		return ErrLifecycleUnavailable
	}
	fd, e := unix.Open(filepath.Join(s.cfg.ControlDir, ".lifecycle.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return ErrLifecycleUnavailable
	}
	lock := os.NewFile(uintptr(fd), "lifecycle lock")
	// Release the lock on every exit; cleanup must not replace the serving error.
	defer func() { _ = lock.Close() }()
	i, e := lock.Stat()
	if e != nil || !controllerOwned(i, s.cfg.UID, 0600) || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return ErrLifecycleUnavailable
	}
	control, e := s.listen("control.sock")
	if e != nil {
		return e
	}
	defer control.close()
	management, e := s.listen("management.sock")
	if e != nil {
		return e
	}
	defer management.close()
	serving, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var handlers sync.WaitGroup
	slots := make(chan struct{}, s.cfg.MaxInflight)
	acceptDone := make(chan struct{}, 2)
	track := func(c net.Conn) bool {
		mu.Lock()
		defer mu.Unlock()
		if serving.Err() != nil {
			return false
		}
		connections[c] = struct{}{}
		return true
	}
	forget := func(c net.Conn) { _ = c.Close(); mu.Lock(); delete(connections, c); mu.Unlock() }
	for _, socket := range []controllerSocket{control, management} {
		go func(socket controllerSocket) {
			defer func() { acceptDone <- struct{}{}; cancel() }()
			for {
				c, e := socket.listener.AcceptUnix()
				if e != nil {
					return
				}
				if controllerPeerUID(c) != s.cfg.UID {
					_ = c.Close()
					continue
				}
				select {
				case slots <- struct{}{}:
				default:
					_ = c.Close()
					continue
				}
				if !track(c) {
					_ = c.Close()
					<-slots
					return
				}
				handlers.Add(1)
				go func() {
					defer handlers.Done()
					defer func() { <-slots }()
					defer forget(c)
					if socket.path == control.path {
						_ = c.SetDeadline(time.Now().Add(s.cfg.Timeout))
						s.handle(c)
						return
					}
					upstream, e := (&net.Dialer{Timeout: s.cfg.Timeout}).DialContext(serving, "tcp", s.cfg.ManagementAddress)
					if e != nil {
						return
					}
					if !track(upstream) {
						_ = upstream.Close()
						return
					}
					defer forget(upstream)
					// A manager session is bounded as well; adapters reconnect explicitly.
					deadline := time.Now().Add(s.cfg.Timeout)
					_ = c.SetDeadline(deadline)
					_ = upstream.SetDeadline(deadline)
					proxyPair(c, upstream)
				}()
			}
		}(socket)
	}
	select {
	case <-serving.Done():
	case <-s.lifecycle.Failed():
		cancel()
	}
	cancel()
	_ = control.listener.Close()
	_ = management.listener.Close()
	mu.Lock()
	for c := range connections {
		_ = c.Close()
	}
	mu.Unlock()
	<-acceptDone
	<-acceptDone
	handlers.Wait()
	return nil
}

func readControllerFrame(r io.Reader, max int) ([]byte, error) {
	var n [4]byte
	if _, e := io.ReadFull(r, n[:]); e != nil {
		return nil, ErrLifecycleUnavailable
	}
	size := binary.BigEndian.Uint32(n[:])
	if size == 0 || size > uint32(max) {
		return nil, ErrLifecycleUnavailable
	}
	b := make([]byte, int(size))
	if _, e := io.ReadFull(r, b); e != nil {
		return nil, ErrLifecycleUnavailable
	}
	return b, nil
}
func writeControllerFrame(w io.Writer, v any, max int) error {
	b, e := json.Marshal(v)
	if e != nil || len(b) > max {
		return ErrLifecycleUnavailable
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	for _, part := range [][]byte{n[:], b} {
		for len(part) > 0 {
			n, e := w.Write(part)
			if e != nil || n == 0 {
				return ErrLifecycleUnavailable
			}
			part = part[n:]
		}
	}
	return nil
}
func (s *LifecycleController) handle(c net.Conn) {
	b, e := readControllerFrame(c, s.cfg.MaxFrameBytes)
	if e != nil || strictDynSecJSON(b) != nil {
		return
	}
	var request controllerRequest
	if json.Unmarshal(b, &request) != nil {
		return
	}
	// Reject unknown fields and arbitrary targets; only these four capabilities.
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil {
		return
	}
	for k := range fields {
		if k != "operation" && k != "receipt" {
			return
		}
	}
	var result error
	switch request.Operation {
	case "Describe":
		if request.Receipt != (VerificationReceipt{}) {
			return
		}
	case "CloseDrain":
		result = s.lifecycle.CloseDrain()
	case "RestartClosed":
		result = s.lifecycle.RestartClosed()
	case "OpenVerified":
		result = s.lifecycle.OpenVerified(request.Receipt)
	default:
		return
	}
	_ = writeControllerFrame(c, controllerResponse{OK: result == nil, Description: s.lifecycle.Describe()}, s.cfg.MaxFrameBytes)
}
