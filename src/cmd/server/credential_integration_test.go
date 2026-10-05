package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/mqttcredential"
)

// Narrow composition smoke, not Task12's full administrative HTTP harness.
func TestCredentialCompositionIsolated(t *testing.T) {
	if os.Getenv("TASK2611_ISOLATED") != "1" {
		t.Skip("owned native broker and database only")
	}
	var input struct{ Password, BackendPassword, AppDSN string }
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, e := pgxpool.New(ctx, input.AppDSN)
	if e != nil {
		t.Fatal("fixture pool")
	}
	defer pool.Close()
	ctrl := mqttcredential.ControllerClient{ControlDir: "/control", Timeout: 3 * time.Second, MaxFrameBytes: 4096}
	d, e := ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("initial CLOSED")
	}
	cfg := config.CredentialConfig{Enabled: true, ManagerUsername: "admin", ManagerPassword: input.Password, BackendPassword: input.BackendPassword, ManagementURL: "ssl://localhost:18884", CAFile: "/ca.crt", SnapshotPath: "/security/dynsec.json", ControlDir: "/control", DBTimeout: 2 * time.Second, FinalizeTimeout: 5 * time.Second, ReconcileTimeout: 30 * time.Second, OperationTimeout: 10 * time.Second, RequestTimeout: 40 * time.Second, RecoveryTimeout: 5 * time.Second, ClientTimeout: 3 * time.Second, BatchSize: 64, MaxPages: 16, QueueSize: 16, MaxInflight: 1, MaxPayloadBytes: 65536, MaxSnapshotBytes: 1 << 20}
	c, e := composeCredentials(ctx, pool, cfg)
	if e != nil {
		t.Fatal("enabled composition", e)
	}
	if c.manager == nil || c.ready == nil || !c.ready.Ready() {
		t.Fatal("startup readiness")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || !d.Open {
		t.Fatal("verified OPEN")
	}
	if e = c.close(); e != nil {
		t.Fatal("shutdown drain")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("shutdown CLOSED")
	}
	disabled, e := composeCredentials(ctx, pool, config.CredentialConfig{})
	if e != nil || disabled.manager != nil {
		t.Fatal("disabled")
	}
	if e = disabled.close(); e != nil {
		t.Fatal(e)
	}
	// Wrong manager secret cannot pass startup; detached close must hold.
	cfg.ManagerPassword = "test-only-wrong-manager-password"
	if _, e = composeCredentials(ctx, pool, cfg); e == nil {
		t.Fatal("uncertain startup accepted")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("failure CLOSED")
	}
	for _, name := range []string{"backend", "snapshot", "ca", "invalid-ca", "repository", "adapter", "reconciler", "database"} {
		t.Run(name, func(t *testing.T) {
			bad := cfg
			bad.ManagerPassword = input.Password
			p := pool
			switch name {
			case "backend":
				bad.BackendPassword = "test-only-wrong-backend-password"
			case "snapshot":
				bad.SnapshotPath = "/security/absent.json"
			case "ca":
				bad.CAFile = "/absent-ca.crt"
			case "invalid-ca":
				bad.CAFile = "/security/dynsec.json"
			case "repository":
				bad.BatchSize = 1001
			case "adapter":
				bad.OperationTimeout = 0
			case "reconciler":
				bad.MaxPages = 0
			case "database":
				var err error
				p, err = pgxpool.New(ctx, "postgres://fixture:unused@127.0.0.1:1/absent?sslmode=disable")
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
			}
			if _, err := composeCredentials(ctx, p, bad); err == nil {
				t.Fatal("uncertain configuration passed")
			}
			d, err := ctrl.Describe(ctx)
			if err != nil || d.Open {
				t.Fatal("uncertainty not CLOSED")
			}
		})
	}
	t.Log("PASS isolated composition enabled OPEN, disabled no dependencies, shutdown CLOSED, manager uncertainty CLOSED")
}
