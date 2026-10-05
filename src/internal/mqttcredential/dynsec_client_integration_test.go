package mqttcredential

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run only through the isolated task265a entrypoint. Inputs arrive on stdin,
// not argv/.env/artifact files. This test never targets a production endpoint.
func TestDynSecPinnedBroker(t *testing.T) {
	if os.Getenv("TASK265A_ISOLATED") != "1" {
		t.Skip("isolated fixture entrypoint required")
	}
	var input struct{ URL, CAPath, WrongCAPath, SnapshotPath, ManagerPassword, OldPassword, BPassword, NewPassword, Mode string }
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input unavailable")
	}
	ca, e := os.ReadFile(input.CAPath)
	if e != nil {
		t.Fatal("fixture CA unavailable")
	}
	cfg := dynsecTestConfig()
	cfg.BrokerURL = input.URL
	cfg.CAPEM = ca
	cfg.ManagerPassword = input.ManagerPassword
	cfg.Timeout = 3 * time.Second
	cfg.MaxInflight = 8
	cfg.ValidateTarget = func(s string) bool { return s == "A" || s == "B" || s == "C" }
	cfg.ValidateRole = func(s, r string) bool { return r == "gateway_A" && (s == "A" || s == "C") }
	c, e := NewDynSecClient(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	reader, e := NewDynSecSnapshotReader(DynSecSnapshotConfig{Path: input.SnapshotPath, WriterUID: 1883, MaxBytes: 1 << 20, ValidateTarget: cfg.ValidateTarget})
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	require := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	observe := func(u string, want bool) {
		t.Helper()
		v, e := reader.Observe(ctx, u)
		require(e)
		if v.Disabled != want {
			t.Fatal("snapshot observation mismatch")
		}
	}
	if input.Mode == "fault" {
		observe("B", false)
		require(c.DisableClient(ctx, "B"))
		ram, e := c.GetClient(ctx, "B")
		require(e)
		if !ram.Disabled {
			t.Fatal("RAM disable missing")
		}
		observe("B", false) // command success is RAM-only; never durable success.
		return
	}
	// Old harness finished with A disabled and B enabled. Actual six narrow APIs
	// operate through the new Paho client, not the old Python protocol client.
	require(c.EnableClient(ctx, "A"))
	observe("A", false)
	require(c.DisableClient(ctx, "A"))
	observe("A", true)
	require(c.SetClientPassword(ctx, "A", input.NewPassword))
	if c.AddClientRole(ctx, "A", "gateway_A") == nil {
		t.Fatal("duplicate role unexpectedly succeeded")
	}
	require(c.EnableClient(ctx, "A"))
	observe("A", false)
	probe, e := NewProbe(input.URL, input.CAPath, 3*time.Second)
	require(e)
	require(probe.Login(ctx, "A", input.NewPassword))
	require(probe.Rejected(ctx, "A", input.OldPassword))
	require(probe.Login(ctx, "B", input.BPassword))
	require(c.DisableClient(ctx, "A"))
	require(probe.Rejected(ctx, "A", input.NewPassword))
	observe("A", true)
	// Native command error is typed/redacted (create existing client).
	if c.CreateClient(ctx, "A") == nil {
		t.Fatal("duplicate create unexpectedly succeeded")
	}
	require(c.CreateClient(ctx, "C"))
	require(probe.Rejected(ctx, "C", input.NewPassword))
	require(c.DisableClient(ctx, "C"))
	require(c.SetClientPassword(ctx, "C", input.NewPassword))
	require(c.AddClientRole(ctx, "C", "gateway_A"))
	require(probe.Rejected(ctx, "C", input.NewPassword))
	require(c.EnableClient(ctx, "C"))
	require(probe.Login(ctx, "C", input.NewPassword))
	require(c.DisableClient(ctx, "C"))
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			v, e := c.GetClient(ctx, "B")
			if e != nil || v.Disabled {
				t.Error("concurrent matched readback failed")
			}
		})
	}
	wg.Wait()
	for _, bad := range []DynSecConfig{func() DynSecConfig { v := cfg; v.BrokerURL = "tcp://localhost:8883"; return v }(), func() DynSecConfig { v := cfg; v.CAPEM = []byte("invalid"); return v }()} {
		if other, e := NewDynSecClient(ctx, bad); e == nil {
			other.Close()
			t.Fatal("TLS configuration accepted")
		}
	}
	wrongCA, e := os.ReadFile(input.WrongCAPath)
	require(e)
	for _, bad := range []DynSecConfig{func() DynSecConfig { v := cfg; v.CAPEM = wrongCA; return v }(), func() DynSecConfig {
		v := cfg
		v.BrokerURL = strings.Replace(cfg.BrokerURL, "localhost", "127.0.0.1", 1)
		return v
	}()} {
		if other, e := NewDynSecClient(ctx, bad); e == nil {
			other.Close()
			t.Fatal("new client TLS trust/hostname bypass")
		}
	}
}
