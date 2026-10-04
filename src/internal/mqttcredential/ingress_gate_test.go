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
	defer upstream.Close()
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
	defer a.Close()
	var b net.Conn
	select {
	case b = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("no upstream")
	}
	defer b.Close()
	go func() { _, _ = b.Write([]byte("TLS opaque")) }()
	buffer := make([]byte, 10)
	_ = a.SetReadDeadline(time.Now().Add(time.Second))
	if _, e = io.ReadFull(a, buffer); e != nil {
		t.Fatal(e)
	}
	excess, e := net.Dial("tcp", addresses[1])
	if e != nil {
		t.Fatal(e)
	}
	defer excess.Close()
	_ = excess.SetReadDeadline(time.Now().Add(time.Second))
	if _, e = excess.Read(buffer); e == nil {
		t.Fatal("overflow accepted")
	}
	g.close()
	_ = a.SetReadDeadline(time.Now().Add(time.Second))
	if _, e = a.Read(buffer); e == nil {
		t.Fatal("close did not disconnect")
	}
	for _, addr := range addresses {
		c, e := net.DialTimeout("tcp", addr, time.Second)
		if e == nil {
			c.Close()
			t.Fatal("listener not closed")
		}
	}
}
