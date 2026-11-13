package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
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
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttingress"
	"iot-platform/internal/telemetry"
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

func TestMQTTIngressOptionsWireConfiguredByteBudgetAndShaper(t *testing.T) {
	cfg := ingressTestConfig("unused")
	cfg.MetricsEnabled = false
	cfg.Ingress.BufferedPayloadBytes = 2 << 20
	cfg.Ingress.ShaperEnabled = true
	cfg.Ingress.ShaperRatePerSecond = 7
	cfg.Ingress.ShaperBurst = 9
	cfg.Ingress.ShaperMaxGateways = 11
	cfg.Ingress.ShaperIdleRetention = 3 * time.Minute

	got := mqttIngressOptions(cfg)
	if got.BufferedPayloadBytes != 2<<20 {
		t.Errorf("BufferedPayloadBytes = %d, want %d", got.BufferedPayloadBytes, 2<<20)
	}
	want := (mqttingress.ShaperOptions{Enabled: true, RatePerSecond: 7, Burst: 9, MaxGateways: 11, IdleRetention: 3 * time.Minute})
	if got.Shaper != want {
		t.Errorf("Shaper options = %+v, want %+v", got.Shaper, want)
	}
}

func TestMQTTIngressOptionsPreservePostCommitSinkIndependentOfMetrics(t *testing.T) {
	ws, err := composeWebSocket(config.Config{WebSocketEnabled: true, WebSocket: config.WebSocketConfig{MaxSessions: 2, MaxSubscriptionsPerSession: 2, GlobalEventSlots: 2, GlobalEventBytes: 1 << 20, SessionEventSlots: 2, SessionEventBytes: 1 << 16, MaxFrameBytes: 66560}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ws.close(context.Background()); err != nil {
			t.Errorf("close websocket runtime: %v", err)
		}
	}()
	cfg := config.Config{}
	got := mqttIngressOptions(cfg, ws.events)
	if got.PostCommitSink != ws.events {
		t.Fatal("ingress sink does not use the composed EventRuntime")
	}
	if got.MetricsEnabled {
		t.Fatal("metrics unexpectedly enabled")
	}
}

func TestComposeWebSocketDisabledHasNoRuntimeAndCloseIsIdempotent(t *testing.T) {
	w, err := composeWebSocket(config.Config{})
	if err != nil || w.hub != nil || w.events != nil {
		t.Fatalf("disabled runtime=%+v err=%v", w, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestComposeWebSocketEnabledClosesOwnedWorkerWithinSharedBudget(t *testing.T) {
	cfg := config.Config{WebSocketEnabled: true, WebSocket: config.WebSocketConfig{MaxSessions: 2, GlobalEventSlots: 2, GlobalEventBytes: 4096, SessionEventSlots: 2, SessionEventBytes: 4096, MaxFrameBytes: 66560}}
	w, err := composeWebSocket(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if w.hub == nil || w.events == nil {
		t.Fatal("enabled composition omitted hub/runtime")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMQTTIngressOptionsWireEveryMicrobatchLimit(t *testing.T) {
	cfg := ingressTestConfig("unused")
	cfg.Ingress.MicrobatchEnabled = true
	cfg.Ingress.MicrobatchMaxMessages = 7
	cfg.Ingress.MicrobatchMaxRows = 91
	cfg.Ingress.MicrobatchMaxPayloadBytes = 12345
	cfg.Ingress.MicrobatchMaxWait = 250 * time.Millisecond

	got := mqttIngressOptions(cfg).Microbatch
	want := mqttingress.MicrobatchOptions{Enabled: true, MaxMessages: 7, MaxRows: 91, MaxPayloadBytes: 12345, MaxWait: 250 * time.Millisecond}
	if got != want {
		t.Fatalf("Microbatch options = %+v, want %+v", got, want)
	}
}

func TestMQTTIngressOptionsWireRAMDedupIndependently(t *testing.T) {
	cfg := ingressTestConfig("unused")
	cfg.Ingress.RAMDedupEnabled = true
	cfg.Ingress.RAMDedupMaxEntries = 321
	cfg.Ingress.RAMDedupMaxBytes = 987654
	got := mqttIngressOptions(cfg)
	if got.RAMDedup != (mqttingress.DedupCacheOptions{Enabled: true, MaxEntries: 321, MaxBytes: 987654}) {
		t.Fatalf("RAM dedup options = %+v", got.RAMDedup)
	}
	if got.Microbatch.Enabled || got.MetricsEnabled {
		t.Fatalf("cache options unexpectedly changed independent flags: %+v", got)
	}
}

func TestComposeIngressRAMDedupMetricsOffStillEnforcesAndExportsFixedSnapshot(t *testing.T) {
	cfg := ingressTestConfig(writeIngressTestCA(t))
	cfg.Ingress.RAMDedupEnabled = true
	cfg.Ingress.RAMDedupMaxEntries = 321
	cfg.Ingress.RAMDedupMaxBytes = 987654
	cfg.MetricsEnabled = false
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.ingress.Snapshot()
	if !snapshot.RAMDedup.Enabled || snapshot.RAMDedup.MaxEntries != 321 || snapshot.RAMDedup.MaxBytes != 987654 || snapshot.RAMDedup.TTLSeconds != 600 {
		t.Fatalf("effective cache config missing with metrics off: %+v", snapshot.RAMDedup)
	}
	if runtime.metrics != nil {
		t.Fatal("persistence instrumentation should remain off")
	}

	// Metrics are an optional observer; cache enforcement and its accounting live
	// in the ingress/cache and remain available even if that observer is absent.
	cache := runtime.ingress.DedupCacheSnapshot()
	if !cache.Enabled || cache.MaxEntries != 321 || cache.MaxBytes != 987654 {
		t.Fatalf("cache enforcement snapshot = %+v", cache)
	}

	cfg.MetricsEnabled = true
	metricsRuntime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatal(err)
	}
	deps := &httpserver.RouterDependencies{}
	populateMetricsDependencies(deps, cfg, testPool(t), metricsRuntime, stage5Runtime{})
	process := deps.MetricsSnapshot().(processMetrics)
	if process.RAMDedup == nil || process.Availability["ram_dedup"] != "available" {
		t.Fatalf("metrics snapshot missing cache state: %+v", process)
	}
	if process.RAMDedup.Enabled != true || process.RAMDedup.TTLSeconds != 600 || process.RAMDedup.MaxEntries != 321 || process.RAMDedup.MaxBytes != 987654 {
		t.Fatalf("wrong fixed cache metrics: %+v", process.RAMDedup)
	}
}

func TestProcessMetricsDistinguishesDisabledAndUnavailableRAMDedup(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		cfg        config.Config
	}{
		{name: "disabled", want: "disabled", cfg: config.Config{MetricsEnabled: true}},
		{name: "cache disabled without ingress source", want: "disabled", cfg: config.Config{MetricsEnabled: true, Ingress: config.IngressConfig{Enabled: true}}},
		{name: "ingress disabled with cache flag", want: "disabled", cfg: config.Config{MetricsEnabled: true, Ingress: config.IngressConfig{RAMDedupEnabled: true}}},
		{name: "enabled without ingress source", want: "unavailable", cfg: config.Config{MetricsEnabled: true, Ingress: config.IngressConfig{Enabled: true, RAMDedupEnabled: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := &httpserver.RouterDependencies{}
			populateMetricsDependencies(deps, tc.cfg, testPool(t), ingressRuntime{}, stage5Runtime{})
			got := deps.MetricsSnapshot().(processMetrics)
			if got.Availability["ram_dedup"] != tc.want {
				t.Fatalf("ram_dedup availability = %q, want %q", got.Availability["ram_dedup"], tc.want)
			}
		})
	}
}

func TestProcessMetricsRAMDedupAvailabilityUsesRuntimeSnapshot(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "available"}[enabled], func(t *testing.T) {
			cfg := ingressTestConfig(writeIngressTestCA(t))
			cfg.MetricsEnabled = true
			cfg.Ingress.RAMDedupEnabled = enabled
			cfg.Ingress.RAMDedupMaxEntries = 10
			cfg.Ingress.RAMDedupMaxBytes = 65536
			runtime, err := composeIngress(cfg, testPool(t))
			if err != nil {
				t.Fatal(err)
			}
			deps := &httpserver.RouterDependencies{}
			populateMetricsDependencies(deps, cfg, testPool(t), runtime, stage5Runtime{})
			got := deps.MetricsSnapshot().(processMetrics)
			want := "disabled"
			if enabled {
				want = "available"
			}
			if got.RAMDedup == nil || got.RAMDedup.Enabled != enabled || got.Availability["ram_dedup"] != want {
				t.Fatalf("RAM dedup snapshot = %+v, availability = %q, want %q", got.RAMDedup, got.Availability["ram_dedup"], want)
			}
		})
	}
}

func TestComposeIngressUsesConfiguredRepositoryModeAndSharedMetrics(t *testing.T) {
	t.Run("default off keeps baseline path and no collector", func(t *testing.T) {
		cfg := ingressTestConfig(writeIngressTestCA(t))
		runtime, err := composeIngress(cfg, testPool(t))
		if err != nil {
			t.Fatal(err)
		}
		if runtime.metrics != nil {
			t.Fatal("metrics collector exists while metrics are disabled")
		}
		if runtime.ingress.Snapshot().Microbatch.Enabled {
			t.Fatal("default-off composition did not remain on baseline Persist path")
		}
	})
	t.Run("enabled requires same batch repository and shared collector", func(t *testing.T) {
		cfg := ingressTestConfig(writeIngressTestCA(t))
		cfg.MetricsEnabled = true
		cfg.Ingress.MicrobatchEnabled = true
		cfg.Ingress.MicrobatchMaxMessages = 3
		cfg.Ingress.MicrobatchMaxRows = 17
		cfg.Ingress.MicrobatchMaxPayloadBytes = 8192
		cfg.Ingress.MicrobatchMaxWait = 125 * time.Millisecond
		runtime, err := composeIngress(cfg, testPool(t))
		if err != nil {
			t.Fatal(err)
		}
		if runtime.metrics == nil {
			t.Fatal("metrics collector missing")
		}
		batch, ok := runtime.repository.(telemetry.BatchRepository)
		if !ok || batch == nil {
			t.Fatal("enabled composition did not use a BatchRepository")
		}
		if runtime.repository == nil {
			t.Fatal("baseline repository missing")
		}
		if !runtime.ingress.Snapshot().Microbatch.Enabled || runtime.ingress.Snapshot().Microbatch.MaxMessages != 3 {
			t.Fatalf("effective batching options = %+v", runtime.ingress.Snapshot().Microbatch)
		}
		deps := &httpserver.RouterDependencies{}
		populateMetricsDependencies(deps, cfg, testPool(t), runtime, stage5Runtime{})
		process := deps.MetricsSnapshot().(processMetrics)
		if process.Persistence == nil || process.Availability["persistence"] != "available" || process.Availability["group_persistence"] != "available" || process.Availability["batch_wait"] != "available" {
			t.Fatalf("metrics snapshot missing actual persistence sources: %+v", process)
		}
		if process.Ingress == nil || process.Ingress.Durations.BatchWait.Count != 0 {
			t.Fatalf("batch-wait histogram not preserved in ingress snapshot: %+v", process.Ingress)
		}
	})
}

func TestProcessMetricsReportsPersistenceAndBatchAvailabilityHonestly(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		cfg                        config.Config
		wantPersistence, wantBatch string
	}{
		{name: "disabled", cfg: config.Config{MetricsEnabled: true}, wantPersistence: "unavailable", wantBatch: "disabled"},
		{name: "enabled without metrics source", cfg: config.Config{MetricsEnabled: true, Ingress: config.IngressConfig{Enabled: true, MicrobatchEnabled: true}}, wantPersistence: "unavailable", wantBatch: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := &httpserver.RouterDependencies{}
			pool := testPool(t)
			runtime := ingressRuntime{}
			if tc.cfg.Ingress.Enabled {
				runtime = ingressRuntime{enabled: true}
			}
			populateMetricsDependencies(deps, tc.cfg, pool, runtime, stage5Runtime{})
			got := deps.MetricsSnapshot().(processMetrics)
			if got.Availability["persistence"] != tc.wantPersistence || got.Availability["group_persistence"] != tc.wantBatch || got.Availability["batch_wait"] != tc.wantBatch {
				t.Fatalf("availability = %#v", got.Availability)
			}
		})
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
	cfg.Ingress.BufferedPayloadBytes = 2 << 20
	cfg.Ingress.ShaperEnabled = true
	cfg.Ingress.ShaperRatePerSecond = 7
	cfg.Ingress.ShaperBurst = 9
	cfg.Ingress.ShaperMaxGateways = 11
	cfg.Ingress.ShaperIdleRetention = 3 * time.Minute
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress(enabled) = %v", err)
	}
	if !runtime.enabled || runtime.transport == nil || runtime.ingress == nil {
		t.Fatalf("enabled ingress missing components: %+v", runtime)
	}
	snapshot := runtime.ingress.Snapshot()
	if snapshot.BufferedPayloadCapacityBytes != cfg.Ingress.BufferedPayloadBytes {
		t.Fatalf("buffered payload capacity = %d, want configured %d", snapshot.BufferedPayloadCapacityBytes, cfg.Ingress.BufferedPayloadBytes)
	}
	if !snapshot.ShaperEnabled || snapshot.ShaperCapacity != cfg.Ingress.ShaperMaxGateways {
		t.Fatalf("shaper snapshot = enabled %v capacity %d, want enabled capacity %d", snapshot.ShaperEnabled, snapshot.ShaperCapacity, cfg.Ingress.ShaperMaxGateways)
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
	cfg.Ingress.BufferedPayloadBytes = 2 << 20
	cfg.Ingress.ShaperEnabled = true
	cfg.Ingress.ShaperRatePerSecond = 7
	cfg.Ingress.ShaperBurst = 9
	cfg.Ingress.ShaperMaxGateways = 11
	cfg.Ingress.ShaperIdleRetention = 3 * time.Minute
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
		"buffered_payload_bytes=0",
		"buffered_payload_capacity_bytes=2097152",
		"buffered_payload_high_water_bytes=0",
		"byte_budget_full=0",
		"shaper_enabled=true",
		"shaper_entries=0",
		"shaper_capacity=11",
		"shaper_passed=0",
		"shaper_rate_limited=0",
		"shaper_state_capacity=0",
	} {
		if !strings.Contains(out, expectedField) {
			t.Fatalf("logSummary output missing field %q; full log:\n%s", expectedField, out)
		}
	}
	for _, forbidden := range []string{"password", "secret", "token", "payload="} {
		if strings.Contains(strings.ToLower(out), forbidden) {
			t.Fatalf("logSummary output contains forbidden sensitive key %q; full log:\n%s", forbidden, out)
		}
	}
}

func TestProcessMetricsIngressSnapshotExportsOnlyFixedP4Fields(t *testing.T) {
	cfg := ingressTestConfig(writeIngressTestCA(t))
	cfg.Ingress.BufferedPayloadBytes = 2 << 20
	cfg.Ingress.ShaperEnabled = true
	cfg.Ingress.ShaperRatePerSecond = 7
	cfg.Ingress.ShaperBurst = 9
	cfg.Ingress.ShaperMaxGateways = 11
	cfg.Ingress.ShaperIdleRetention = 3 * time.Minute
	runtime, err := composeIngress(cfg, testPool(t))
	if err != nil {
		t.Fatalf("composeIngress = %v", err)
	}
	snapshot := runtime.ingress.Snapshot()
	payload, err := json.Marshal(processMetrics{Ingress: &snapshot})
	if err != nil {
		t.Fatalf("marshal ingress metrics: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal ingress metrics: %v", err)
	}
	ingress, ok := decoded["ingress"].(map[string]any)
	if !ok {
		t.Fatalf("serialized ingress metrics missing: %s", payload)
	}
	want := map[string]any{
		"BufferedPayloadBytes": float64(0), "BufferedPayloadCapacityBytes": float64(2 << 20),
		"BufferedPayloadHighWaterBytes": float64(0), "ByteBudgetFull": float64(0),
		"ShaperEnabled": true, "ShaperEntries": float64(0), "ShaperCapacity": float64(11),
		"ShaperPassed": float64(0), "ShaperRateLimited": float64(0), "ShaperStateCapacity": float64(0),
	}
	for field, expected := range want {
		if ingress[field] != expected {
			t.Errorf("serialized ingress %s = %#v, want %#v", field, ingress[field], expected)
		}
	}
	for _, forbidden := range []string{"Gateway", "Sensor", `"payload"`} {
		if strings.Contains(string(payload), forbidden) {
			t.Errorf("serialized metrics contain forbidden %q: %s", forbidden, payload)
		}
	}
}
