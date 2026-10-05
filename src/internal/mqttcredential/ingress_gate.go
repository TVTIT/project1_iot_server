package mqttcredential

import (
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// ingressGate proxies opaque TCP (normally TLS). It owns all accepted sockets;
// close drains both halves, including dial-in-progress, before returning.
// No per-packet goroutines, buffers, or unbounded waiting admission queues.
type ingressGate struct {
	mu          sync.Mutex
	listeners   []net.Listener
	connections map[net.Conn]struct{}
	slots       chan struct{}
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	target      string
	timeout     time.Duration
	closed      bool
}

func newIngressGate(addresses []string, target string, limit int, timeout time.Duration) (*ingressGate, error) {
	ctx, cancel := context.WithCancel(context.Background())
	g := &ingressGate{connections: make(map[net.Conn]struct{}), slots: make(chan struct{}, limit), ctx: ctx, cancel: cancel, target: target, timeout: timeout}
	for _, a := range addresses {
		l, e := net.Listen("tcp", a)
		if e != nil {
			g.close()
			return nil, ErrLifecycleUnavailable
		}
		g.listeners = append(g.listeners, l)
	}
	for _, l := range g.listeners {
		g.wg.Add(1)
		go g.accept(l)
	}
	return g, nil
}

func (g *ingressGate) addresses() []string {
	a := make([]string, 0, len(g.listeners))
	for _, l := range g.listeners {
		a = append(a, l.Addr().String())
	}
	return a
}
func (g *ingressGate) track(c net.Conn) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.connections[c] = struct{}{}
	return true
}
func (g *ingressGate) forget(c net.Conn) {
	_ = c.Close()
	g.mu.Lock()
	delete(g.connections, c)
	g.mu.Unlock()
}
func (g *ingressGate) accept(l net.Listener) {
	defer g.wg.Done()
	for {
		c, e := l.Accept()
		if e != nil {
			return
		}
		select {
		case g.slots <- struct{}{}:
		default:
			_ = c.Close()
			continue
		}
		if !g.track(c) {
			_ = c.Close()
			<-g.slots
			return
		}
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			defer func() { <-g.slots }()
			defer g.forget(c)
			upstream, e := (&net.Dialer{Timeout: g.timeout}).DialContext(g.ctx, "tcp", g.target)
			if e != nil {
				return
			}
			if !g.track(upstream) {
				_ = upstream.Close()
				return
			}
			defer g.forget(upstream)
			proxyPair(c, upstream)
		}()
	}
}

func proxyPair(a, b net.Conn) {
	done := make(chan struct{})
	go func() { _, _ = io.Copy(a, b); _ = a.Close(); _ = b.Close(); close(done) }()
	_, _ = io.Copy(b, a)
	_ = a.Close()
	_ = b.Close()
	<-done
}

func (g *ingressGate) close() {
	g.mu.Lock()
	g.closed = true
	g.cancel()
	for _, l := range g.listeners {
		_ = l.Close()
	}
	for c := range g.connections {
		_ = c.Close()
	}
	g.mu.Unlock()
	g.wg.Wait()
}
