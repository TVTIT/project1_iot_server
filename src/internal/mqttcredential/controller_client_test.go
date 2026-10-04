package mqttcredential

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Isolated pinned-broker runner only. The backend container has its own network
// namespace and no broker config/manager store/CA private key mount. Secrets
// arrive on stdin and never appear in diagnostics.
func TestControllerPinnedBroker(t *testing.T) {
	if os.Getenv("TASK265B_ISOLATED") != "1" {
		t.Skip("isolated fixture entrypoint required")
	}
	var input struct {
		ControlDir, CAPath, WrongCAPath, Password, NewPassword, Mode, OldEpoch string
		Public                                                                 []string
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx := context.Background()
	client := ControllerClient{ControlDir: input.ControlDir, Timeout: 3 * time.Second, MaxFrameBytes: 2048}
	if input.Mode == "unauthorized" {
		if _, e := client.Describe(ctx); e == nil {
			t.Fatal("unauthorized UID accepted")
		}
		if c, e := client.ManagementDial(ctx); e == nil {
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			c.Write([]byte("x"))
			var b [1]byte
			if _, e = c.Read(b[:]); e == nil {
				t.Fatal("unauthorized tunnel accepted")
			}
		}
		return
	}
	var d LifecycleDescription
	var e error
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		d, e = client.Describe(ctx)
		if e == nil && d.Alive {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e != nil || !d.Alive || d.Open {
		t.Fatal("startup not closed/alive")
	}
	if input.OldEpoch != "" && d.Epoch == input.OldEpoch {
		t.Fatal("wrapper restart reused lifetime")
	}
	if conn, err := net.DialTimeout("tcp", "broker:18884", 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("loopback management reachable from backend network")
	}
	ca, e := os.ReadFile(input.CAPath)
	if e != nil {
		t.Fatal("CA input")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	tlsCfg := &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	probe := func(password string) error {
		opts := mqtt.NewClientOptions().AddBroker("ssl://localhost:8883").SetClientID("private-fresh-login").SetUsername("B").SetPassword(password).SetAutoReconnect(false).SetConnectRetry(false).SetConnectTimeout(time.Second)
		opts.SetCustomOpenConnectionFn(func(*url.URL, mqtt.ClientOptions) (net.Conn, error) { return client.DialManagementTLS(ctx, tlsCfg) })
		c := mqtt.NewClient(opts)
		defer c.Disconnect(0)
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return dynSecWait(pctx, c.Connect())
	}
	for _, addr := range input.Public {
		c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if e == nil {
			c.Close()
			t.Fatal("public path reachable while CLOSED")
		}
	}
	cfg := dynsecTestConfig()
	cfg.ManagerUsername = "admin"
	cfg.ManagerPassword = input.Password
	cfg.Timeout = 2 * time.Second
	cfg.BrokerURL = "ssl://localhost:8883"
	cfg.CAPEM = ca
	cfg.ManagementDial = client.ManagementDial
	var manager *DynSecClient
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		manager, e = NewDynSecClient(ctx, cfg)
		if e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	if input.Mode == "fresh" {
		manager.Close()
		return
	}
	if e = manager.CreateClient(ctx, "B"); e != nil {
		t.Fatal(e)
	}
	if e = manager.SetClientPassword(ctx, "B", input.NewPassword); e != nil {
		t.Fatal(e)
	}
	ram, e := manager.GetClient(ctx, "B")
	if e != nil || ram.Disabled {
		t.Fatal("private correlated readback failed")
	}
	manager.Close()
	// Cold restart must give fresh epoch and consume all old challenges.
	fresh, e := client.RestartClosed(ctx)
	if e != nil || fresh.Open || fresh.Epoch == d.Epoch {
		t.Fatal("cold restart identity")
	}
	if client.OpenVerified(ctx, VerificationReceipt{Epoch: d.Epoch, Nonce: d.Nonce}) == nil {
		t.Fatal("stale epoch accepted")
	}
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		e = probe(input.NewPassword)
		if e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e != nil {
		t.Fatal("new password fresh login failed")
	}
	if probe("incorrect-password") == nil {
		t.Fatal("negative password probe accepted")
	}
	wrong, e := os.ReadFile(input.WrongCAPath)
	if e != nil {
		t.Fatal("wrong CA input")
	}
	wrongRoots := x509.NewCertPool()
	wrongRoots.AppendCertsFromPEM(wrong)
	for _, bad := range []*tls.Config{{RootCAs: wrongRoots, ServerName: "localhost", MinVersion: tls.VersionTLS12}, {RootCAs: roots, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12}} {
		if conn, e := client.DialManagementTLS(ctx, bad); e == nil {
			conn.Close()
			t.Fatal("private TLS verification bypass")
		}
	}
	for _, bad := range []DynSecConfig{func() DynSecConfig { v := cfg; v.CAPEM = wrong; return v }(), func() DynSecConfig {
		v := cfg
		v.BrokerURL = strings.Replace(v.BrokerURL, "localhost", "127.0.0.1", 1)
		return v
	}()} {
		if c, e := NewDynSecClient(ctx, bad); e == nil {
			c.Close()
			t.Fatal("DynSec custom dial TLS bypass")
		}
	}
	if client.OpenVerified(ctx, VerificationReceipt{Epoch: fresh.Epoch, Nonce: "wrong"}) == nil {
		t.Fatal("wrong handshake accepted")
	}
	if e = client.OpenVerified(ctx, VerificationReceipt{Epoch: fresh.Epoch, Nonce: fresh.Nonce}); e != nil {
		t.Fatal(e)
	}
	var sessions []net.Conn
	defer func() {
		for _, c := range sessions {
			c.Close()
		}
	}()
	for _, addr := range input.Public {
		c, e := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, tlsCfg)
		if e != nil {
			t.Fatal("public fixture path unavailable")
		}
		sessions = append(sessions, c)
	}
	if e = client.CloseDrain(ctx); e != nil {
		t.Fatal(e)
	}
	for _, c := range sessions {
		c.SetReadDeadline(time.Now().Add(time.Second))
		var b [1]byte
		if _, e = io.ReadFull(c, b[:]); e == nil {
			t.Fatal("maintenance kept session")
		}
	}
	latest, e := client.Describe(ctx)
	if e != nil || latest.Open {
		t.Fatal("maintenance state")
	}
	if e = client.OpenVerified(ctx, VerificationReceipt{Epoch: latest.Epoch, Nonce: latest.Nonce}); e != nil {
		t.Fatal(e)
	}
	// Leave OPEN for the independent fixture's SIGKILL/exit/no-respawn oracle.
	t.Log("fixture epoch=" + fresh.Epoch)
	t.Log("PASS private TLS DynSec, fresh login, epoch fence, two gated paths and drain")
}
