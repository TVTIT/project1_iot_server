package mqttcredential

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestIngressGateBoundAndDrain(t *testing.T) {
	upstream, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = upstream.Close() }() // Best-effort listener teardown.
	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			c, e := upstream.Accept()
			if e != nil {
				return
			}
			accepted <- c
		}
	}()
	g, e := newIngressGate([]string{"127.0.0.1:0", "127.0.0.1:0"}, upstream.Addr().String(), 1, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer g.close()
	addresses := g.addresses()
	a, e := net.Dial("tcp", addresses[0])
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = a.Close() }() // Gate drain may already close the peer.
	var b net.Conn
	select {
	case b = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("no upstream")
	}
	defer func() { _ = b.Close() }() // Gate drain may already close the peer.
	written := make(chan error, 1)
	go func() { _, err := b.Write([]byte("TLS opaque")); written <- err }()
	buffer := make([]byte, 10)
	if err := a.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, e = io.ReadFull(a, buffer); e != nil {
		t.Fatal(e)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	excess, e := net.Dial("tcp", addresses[1])
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = excess.Close() }() // Gate rejects this peer.
	if err := excess.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, e = excess.Read(buffer); e == nil {
		t.Fatal("overflow accepted")
	}
	g.close()
	if err := a.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, e = a.Read(buffer); e == nil {
		t.Fatal("close did not disconnect")
	}
	for _, addr := range addresses {
		c, e := net.DialTimeout("tcp", addr, time.Second)
		if e == nil {
			_ = c.Close() // Preserve the unexpected-connect failure below.
			t.Fatal("listener not closed")
		}
	}
}
