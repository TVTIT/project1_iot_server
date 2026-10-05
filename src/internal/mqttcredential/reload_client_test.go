//nolint:errcheck // Mock socket peers intentionally drop requests; cleanup is best effort.
package mqttcredential

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestReloadClient(t *testing.T) {
	for _, ack := range []string{"signalled\n", "", "signalled", "signalled\nextra", "signalled\n\n", "reload\n"} {
		t.Run(ack, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "r.sock")
			l, e := net.Listen("unix", path)
			if e != nil {
				t.Fatal(e)
			}
			defer l.Close()
			done := make(chan string, 1)
			go func() {
				c, e := l.Accept()
				if e != nil {
					done <- "accept failed"
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				b, _ := io.ReadAll(io.LimitReader(c, 8))
				io.WriteString(c, ack)
				done <- string(b)
			}()
			r, e := NewReloadClient(ReloadClientConfig{SocketPath: path})
			if e != nil {
				t.Fatal(e)
			}
			e = r.Reload(context.Background())
			if (e == nil) != (ack == "signalled\n") {
				t.Fatal(e)
			}
			if e != nil && !errors.Is(e, ErrReloadUnavailable) {
				t.Fatal(e)
			}
			if got := <-done; got != "reload\n" {
				t.Fatal(got)
			}
		})
	}
}

func TestReloadCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancel"}[cancelled], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "r.sock")
			l, _ := net.Listen("unix", path)
			defer l.Close()
			done := make(chan struct{})
			accepted := make(chan struct{})
			go func() {
				defer close(done)
				c, e := l.Accept()
				if e != nil {
					return
				}
				defer c.Close()
				close(accepted)
				io.Copy(io.Discard, c)
				b := make([]byte, 1)
				c.Read(b)
			}()
			r, _ := NewReloadClient(ReloadClientConfig{SocketPath: path, Timeout: 50 * time.Millisecond})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelled {
				go func() { <-accepted; cancel() }()
			}
			if e := r.Reload(ctx); e != ErrReloadUnavailable {
				t.Fatal(e)
			}
			l.Close()
			<-done
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _ := NewReloadClient(ReloadClientConfig{SocketPath: "/nonexistent.sock"})
	if r.Reload(ctx) != ErrReloadUnavailable {
		t.Fatal("cancelled dial")
	}
}

func TestReloadClientConfig(t *testing.T) {
	for _, c := range []ReloadClientConfig{{}, {SocketPath: "relative"}, {SocketPath: "/a/../b"}, {SocketPath: "/a", Timeout: -1}, {SocketPath: "/a", Timeout: time.Minute}} {
		if _, e := NewReloadClient(c); e != ErrInvalidInput {
			t.Fatal(c, e)
		}
	}
	r, _ := NewReloadClient(ReloadClientConfig{SocketPath: filepath.Join(t.TempDir(), "missing")})
	if r.Reload(context.Background()) != ErrReloadUnavailable {
		t.Fatal("missing socket")
	}
}
