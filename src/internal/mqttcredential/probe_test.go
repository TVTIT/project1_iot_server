//nolint:errcheck // Mock peers deliberately close/drop traffic; negative filesystem fixtures are intentional.
package mqttcredential

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func testTLS(t *testing.T) (string, tls.Certificate) {
	t.Helper()
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	path := filepath.Join(t.TempDir(), "ca.crt")
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	c, e := tls.X509KeyPair(b, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if e != nil {
		t.Fatal(e)
	}
	return path, c
}

func TestFreshTLSProbe(t *testing.T) {
	ca, cert := testTLS(t)
	l, e := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				pkt, e := packets.ReadPacket(c)
				if e != nil {
					return
				}
				p := pkt.(*packets.ConnectPacket)
				ack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				if string(p.Password) != "good" {
					ack.ReturnCode = packets.ErrRefusedNotAuthorised
				}
				ack.Write(c)
				packets.ReadPacket(c)
			}()
		}
	}()
	defer func() { l.Close(); wg.Wait(); close(done) }()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	p, e := NewProbe("ssl://localhost:"+port, ca, 200*time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 10; i++ {
		if e = p.Login(context.Background(), BackendUsername, "good"); e != nil {
			t.Fatal(e)
		}
	}
	if p.Rejected(context.Background(), BackendUsername, "bad") != nil {
		t.Fatal("auth rejection not recognized")
	}
	if !errors.Is(p.Rejected(context.Background(), BackendUsername, "good"), ErrVerificationFailed) {
		t.Fatal("accepted login treated as rejection")
	}
	wrong, e := NewProbe("ssl://127.0.0.1:"+port, ca, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if wrong.Rejected(context.Background(), BackendUsername, "bad") == nil {
		t.Fatal("hostname failure counted as auth rejection")
	}
	other, _ := testTLS(t)
	wrong, e = NewProbe("ssl://localhost:"+port, other, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if wrong.Login(context.Background(), BackendUsername, "good") == nil {
		t.Fatal("wrong CA accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.Login(ctx, BackendUsername, "good") == nil {
		t.Fatal("canceled probe accepted")
	}
	l.Close()
	if p.Rejected(context.Background(), BackendUsername, "bad") == nil {
		t.Fatal("offline treated as auth rejection")
	}
}

func TestSocketCheckDoesNotReload(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "reload.sock")
	l, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	os.Chmod(path, 0600)
	read := make(chan []byte, 1)
	go func() {
		c, e := l.Accept()
		if e != nil {
			read <- nil
			return
		}
		defer c.Close()
		b := make([]byte, 32)
		n, _ := c.Read(b)
		read <- b[:n]
	}()
	r, _ := NewReloadClient(ReloadClientConfig{SocketPath: path})
	if e = r.Check(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(<-read) != 0 {
		t.Fatal("startup signalled reload")
	}
	os.Chmod(path, 0666)
	if r.Check(context.Background()) == nil {
		t.Fatal("unsafe socket accepted")
	}
}

func TestProbeStalledCONNACKCleanup(t *testing.T) {
	ca, cert := testTLS(t)
	l, e := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		if _, e = packets.ReadPacket(c); e != nil {
			return
		}
		b := make([]byte, 1)
		c.Read(b)
	}()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	p, _ := NewProbe("ssl://localhost:"+port, ca, 50*time.Millisecond)
	start := time.Now()
	if p.Rejected(context.Background(), BackendUsername, "bad") == nil {
		t.Fatal("timeout is not auth rejection")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("unbounded token wait")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("transport leaked")
	}
}

func TestStartupBadUtilityAndMarkers(t *testing.T) {
	m := startupEnv()
	c, _ := loadMap(m)
	c.Runtime.Tool.Path = filepath.Join(t.TempDir(), "absent")
	if _, e := c.Start(context.Background()); !errors.Is(e, ErrToolFailure) {
		t.Fatal(e)
	}
	tool := filepath.Join(t.TempDir(), "tool")
	os.WriteFile(tool, []byte("not executable"), 0600)
	c.Runtime.Tool.Path = tool
	if _, e := c.Start(context.Background()); !errors.Is(e, ErrToolFailure) {
		t.Fatal(e)
	}
	os.Chmod(tool, 0700)
	if _, e := c.Start(context.Background()); !errors.Is(e, ErrToolFailure) {
		t.Fatal("invalid executable", e)
	}
	os.WriteFile(tool, []byte("#!/bin/sh\necho 'mosquitto_passwd is a tool sha512-pbkdf2'\nexit 1\n"), 0700)
	r := runtimeFixture(t)
	c.Runtime.AuthDir = r.config.AuthDir
	os.WriteFile(filepath.Join(c.Runtime.AuthDir, ".credential.pending"), []byte("pending"), 0600)
	if _, e := c.Start(context.Background()); !errors.Is(e, ErrRecoveryRequired) {
		t.Fatal(e)
	}
}
