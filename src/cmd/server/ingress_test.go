package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pc, err := pgxpool.ParseConfig("postgres://user:pass@127.0.0.1:1/iot?sslmode=disable")
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func writeIngressTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ingress-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.crt")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	return path
}

func ingressTestConfig(caFile string) config.Config {
	return config.Config{
		ReadinessTimeout: 2 * time.Second,
		Ingress: config.IngressConfig{
			Enabled:              true,
			BrokerURL:            "ssl://mosquitto:8883",
			ClientID:             "telemetry_ingress",
			Username:             "backend_service",
			Password:             "test-only-ingress-password",
			CAFile:               caFile,
			QueueCapacity:        256,
			WorkerCount:          4,
			MaxPayloadBytes:      65536,
			ConnectTimeout:       time.Second,
			SubscribeTimeout:     time.Second,
			PublishTimeout:       time.Second,
			ReconnectMaxInterval: time.Second,
			MaxSamplesPerMessage: 1000,
			MaxBatchDuration:     time.Minute,
			MaxFutureSkew:        2 * time.Minute,
			DBTimeout:            time.Second,
		},
	}
}

// TestComposeIngressDisabledIsInert verifies that a disabled flag composes no
// transport, reads no CA (an unusable path is not touched) and that every
// lifecycle call is a safe no-op.
func TestComposeIngressDisabledIsInert(t *testing.T) {
	cfg := config.Config{Ingress: config.IngressConfig{Enabled: false, CAFile: filepath.Join(t.TempDir(), "absent.crt")}}
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress(disabled) = %v", err)
	}
	if runtime.enabled || runtime.transport != nil || runtime.ingress != nil {
		t.Fatalf("disabled ingress built components: %+v", runtime)
	}
	runtime.stopIntake()
	if err := runtime.start(); err != nil {
		t.Fatalf("start(disabled) = %v", err)
	}
	if err := runtime.drainAndClose(context.Background()); err != nil {
		t.Fatalf("drainAndClose(disabled) = %v", err)
	}
}

func TestComposeIngressEnabledInvalidCAFailsStartup(t *testing.T) {
	cfg := ingressTestConfig(filepath.Join(t.TempDir(), "absent.crt"))
	if _, err := composeIngress(cfg, testPool(t)); err == nil {
		t.Fatal("composeIngress accepted an unreadable CA file")
	}
}

func TestComposeIngressEnabledComposesAndCleansUpFinite(t *testing.T) {
	cfg := ingressTestConfig(writeIngressTestCA(t))
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress(enabled) = %v", err)
	}
	if !runtime.enabled || runtime.transport == nil || runtime.ingress == nil {
		t.Fatalf("enabled ingress missing components: %+v", runtime)
	}

	// Never started: stopIntake and drainAndClose must stay inert and finite.
	start := time.Now()
	runtime.stopIntake()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := runtime.drainAndClose(ctx); err != nil {
		t.Fatalf("drainAndClose(unstarted) = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("unstarted cleanup took %s, want finite", elapsed)
	}
}

// --- composite readiness ---------------------------------------------------

type fakePinger struct {
	calls atomic.Int32
	err   error
}

func (f *fakePinger) Ping(context.Context) error {
	f.calls.Add(1)
	return f.err
}

type fakeTransportReady struct{ ready atomic.Bool }

func (f *fakeTransportReady) Ready() bool { return f.ready.Load() }

type fakeAdmission struct{ accepting atomic.Bool }

func (f *fakeAdmission) Accepting() bool { return f.accepting.Load() }

func TestIngressReadinessComposite(t *testing.T) {
	t.Run("ready when accepting, connected and database answers", func(t *testing.T) {
		pinger := &fakePinger{}
		transport := &fakeTransportReady{}
		transport.ready.Store(true)
		admission := &fakeAdmission{}
		admission.accepting.Store(true)
		readiness := &ingressReadiness{pool: pinger, dbTimeout: time.Second, transport: transport, ingress: admission}
		if err := readiness.Ping(context.Background()); err != nil {
			t.Fatalf("Ping = %v, want nil", err)
		}
		if pinger.calls.Load() != 1 {
			t.Fatalf("database ping calls = %d, want 1", pinger.calls.Load())
		}
	})

	t.Run("not ready while admission stopped", func(t *testing.T) {
		pinger := &fakePinger{}
		transport := &fakeTransportReady{}
		transport.ready.Store(true)
		admission := &fakeAdmission{} // not accepting
		readiness := &ingressReadiness{pool: pinger, dbTimeout: time.Second, transport: transport, ingress: admission}
		if err := readiness.Ping(context.Background()); err == nil {
			t.Fatal("Ping returned nil while admission was stopped")
		}
		if pinger.calls.Load() != 0 {
			t.Fatalf("database ping ran %d times while not accepting, want 0", pinger.calls.Load())
		}
	})

	t.Run("not ready while broker is unavailable", func(t *testing.T) {
		pinger := &fakePinger{}
		transport := &fakeTransportReady{} // never ready (broker unavailable)
		admission := &fakeAdmission{}
		admission.accepting.Store(true)
		readiness := &ingressReadiness{pool: pinger, dbTimeout: time.Second, transport: transport, ingress: admission}
		if err := readiness.Ping(context.Background()); err == nil {
			t.Fatal("Ping returned nil while the transport was not connected")
		}
		if pinger.calls.Load() != 0 {
			t.Fatalf("database ping ran %d times while broker unavailable, want 0", pinger.calls.Load())
		}
	})

	t.Run("not ready when database ping fails", func(t *testing.T) {
		pinger := &fakePinger{err: errors.New("db down")}
		transport := &fakeTransportReady{}
		transport.ready.Store(true)
		admission := &fakeAdmission{}
		admission.accepting.Store(true)
		readiness := &ingressReadiness{pool: pinger, dbTimeout: time.Second, transport: transport, ingress: admission}
		if err := readiness.Ping(context.Background()); err == nil {
			t.Fatal("Ping returned nil when the database ping failed")
		}
	})
}

func TestIngressReadinessCheckerDisabledReturnsPool(t *testing.T) {
	pool := testPool(t)
	checker := ingressReadinessChecker(pool, time.Second, ingressRuntime{})
	if checker != pool {
		t.Fatalf("disabled ingress must keep the database-only checker, got %T", checker)
	}
}

func TestIngressReadinessCheckerEnabledReturnsComposite(t *testing.T) {
	cfg := ingressTestConfig(writeIngressTestCA(t))
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress = %v", err)
	}
	checker := ingressReadinessChecker(testPool(t), time.Second, runtime)
	composite, ok := checker.(*ingressReadiness)
	if !ok {
		t.Fatalf("enabled ingress readiness = %T, want *ingressReadiness", checker)
	}
	if composite.transport == nil || composite.ingress == nil {
		t.Fatal("composite readiness missing transport/ingress")
	}
}

func TestLogSummaryDisabledIsInert(_ *testing.T) {
	runtime := ingressRuntime{}
	// Must not panic or log when disabled.
	runtime.logSummary()
}

func TestLogSummaryEnabledEmitsStructuredSummary(t *testing.T) {
	cfg := ingressTestConfig(writeIngressTestCA(t))
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress = %v", err)
	}

	var buf bytes.Buffer
	testLogger := slog.New(slog.NewTextHandler(&buf, nil))
	oldLogger := slog.Default()
	slog.SetDefault(testLogger)
	defer slog.SetDefault(oldLogger)

	runtime.logSummary()

	out := buf.String()
	for _, expectedField := range []string{
		"msg=\"telemetry ingress summary\"",
		"accepted=0",
		"queue_full=0",
		"persist_failures=0",
		"committed=0",
		"queue_capacity=256",
		"queue_high_water=0",
	} {
		if !strings.Contains(out, expectedField) {
			t.Fatalf("logSummary output missing field %q; full log:\n%s", expectedField, out)
		}
	}
	for _, forbidden := range []string{"password", "secret", "token", "payload"} {
		if strings.Contains(strings.ToLower(out), forbidden) {
			t.Fatalf("logSummary output contains forbidden sensitive key %q; full log:\n%s", forbidden, out)
		}
	}
}
