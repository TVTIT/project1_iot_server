package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttingress"
	"iot-platform/internal/telemetry"
)

// lateAdmitter breaks the construction cycle between the transport and the
// ingress core: the transport needs an Admitter and the core needs the transport
// as its Publisher. It is a single, tiny indirection, not a second pipeline.
// Before set is called it rejects every delivery as not running.
type lateAdmitter struct {
	mu    sync.RWMutex
	inner mqttingress.Admitter
}

func (a *lateAdmitter) Admit(topic string, payload []byte, receivedAt time.Time, retained bool) mqttingress.AdmissionDecision {
	a.mu.RLock()
	inner := a.inner
	a.mu.RUnlock()
	if inner == nil {
		return mqttingress.AdmissionRejectedNotRunning
	}
	return inner.Admit(topic, payload, receivedAt, retained)
}

func (a *lateAdmitter) set(inner mqttingress.Admitter) {
	a.mu.Lock()
	a.inner = inner
	a.mu.Unlock()
}

// ingressRuntime owns the optional MQTT telemetry ingress composition. When the
// feature flag is disabled it holds no transport, starts no workers and never
// reads the CA, so an HTTP-only deployment is unaffected. It is inert and safe
// to stop even when it was never started.
type ingressRuntime struct {
	enabled   bool
	transport *mqttingress.Transport
	ingress   *mqttingress.Ingress
}

// composeIngress builds the telemetry validator, repository, ingress core and
// data transport only when MQTT_INGRESS_ENABLED is true. An invalid CA or
// broker configuration fails here, at startup, before any worker runs.
func composeIngress(cfg config.Config, pool *pgxpool.Pool) (ingressRuntime, error) {
	if !cfg.Ingress.Enabled {
		return ingressRuntime{}, nil
	}

	validator, err := telemetry.NewValidator(telemetry.Limits{
		MaxPayloadBytes:  cfg.Ingress.MaxPayloadBytes,
		MaxSamples:       cfg.Ingress.MaxSamplesPerMessage,
		MaxBatchDuration: cfg.Ingress.MaxBatchDuration,
		MaxFutureSkew:    cfg.Ingress.MaxFutureSkew,
	})
	if err != nil {
		return ingressRuntime{}, fmt.Errorf("create telemetry validator: %w", err)
	}
	repository, err := telemetry.NewPostgresRepository(pool, cfg.Ingress.DBTimeout)
	if err != nil {
		return ingressRuntime{}, fmt.Errorf("create telemetry repository: %w", err)
	}

	admitter := &lateAdmitter{}
	transport, err := mqttingress.NewTransport(mqttingress.TransportOptions{
		BrokerURL:              cfg.Ingress.BrokerURL,
		ClientID:               cfg.Ingress.ClientID,
		Username:               cfg.Ingress.Username,
		Password:               cfg.Ingress.Password,
		CAFile:                 cfg.Ingress.CAFile,
		Filter:                 mqttingress.DefaultTelemetryFilter,
		ConnectTimeout:         cfg.Ingress.ConnectTimeout,
		SubscribeTimeout:       cfg.Ingress.SubscribeTimeout,
		PublishTimeout:         cfg.Ingress.PublishTimeout,
		ReconnectMaxInterval:   cfg.Ingress.ReconnectMaxInterval,
		MaxConcurrentPublishes: cfg.Ingress.WorkerCount,
	}, admitter)
	if err != nil {
		return ingressRuntime{}, fmt.Errorf("create MQTT telemetry transport: %w", err)
	}

	core, err := mqttingress.New(mqttingress.Options{
		QueueCapacity:   cfg.Ingress.QueueCapacity,
		WorkerCount:     cfg.Ingress.WorkerCount,
		MaxPayloadBytes: cfg.Ingress.MaxPayloadBytes,
		PersistTimeout:  cfg.Ingress.DBTimeout,
		PublishTimeout:  cfg.Ingress.PublishTimeout,
	}, validator, repository, transport)
	if err != nil {
		return ingressRuntime{}, fmt.Errorf("create telemetry ingress: %w", err)
	}
	admitter.set(core)

	return ingressRuntime{enabled: true, transport: transport, ingress: core}, nil
}

// start launches the transport supervisor and then the fixed worker pool. If the
// worker pool cannot start, the already-started transport is torn down so no
// connection outlives a failed startup.
func (r ingressRuntime) start() error {
	if !r.enabled {
		return nil
	}
	if err := r.transport.Start(); err != nil {
		return err
	}
	if err := r.ingress.Start(); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.transport.Close(ctx)
		return err
	}
	return nil
}

// stopIntake marks readiness false and stops the reconnect loop while keeping
// the current broker connection open, so workers can still publish post-commit
// ACKs during the drain. It is a no-op when the ingress is disabled.
func (r ingressRuntime) stopIntake() {
	if !r.enabled {
		return
	}
	r.ingress.StopAdmission()
	r.transport.StopReconnect()
}

// drainAndClose lets admitted work finish within ctx, then closes the MQTT
// connection. It must be called after stopIntake (or after a drain that already
// stopped admission).
func (r ingressRuntime) drainAndClose(ctx context.Context) error {
	if !r.enabled {
		return nil
	}
	drainErr := r.ingress.Drain(ctx)
	closeErr := r.transport.Close(ctx)
	return errors.Join(drainErr, closeErr)
}

// logSummary logs a payload-free, driver-error-free lifecycle summary of the
// ingress counters using stable numeric fields so integration runners can verify
// worker outcomes (such as persist failures or queue saturation) without metrics
// endpoints or debug APIs.
func (r ingressRuntime) logSummary() {
	if !r.enabled || r.ingress == nil {
		return
	}
	snap := r.ingress.Snapshot()
	slog.Info("telemetry ingress summary",
		"accepted", snap.Accepted,
		"queue_full", snap.QueueFull,
		"processed", snap.Processed,
		"validation_rejected", snap.ValidationRejected,
		"persist_failures", snap.PersistFailures,
		"commit_unconfirmed", snap.CommitUnconfirmed,
		"identity_mismatch", snap.IdentityMismatch,
		"ack_build_failures", snap.ACKBuildFailures,
		"committed", snap.Committed,
		"duplicate_committed", snap.DuplicateCommitted,
		"samples_new", snap.SamplesNew,
		"ack_published", snap.ACKPublished,
		"ack_publish_failures", snap.ACKPublishFailures,
		"queue_depth", snap.QueueDepth,
		"queue_capacity", snap.QueueCapacity,
	)
}

// databasePinger, transportReadiness and admissionReadiness are the small
// boundaries the composite readiness check needs, so it can be unit tested with
// fakes without a real pool, broker or worker pool.
type databasePinger interface {
	Ping(context.Context) error
}

type transportReadiness interface {
	Ready() bool
}

type admissionReadiness interface {
	Accepting() bool
}

// ingressReadiness composes the shared database, the current transport
// connection and the ingress admission state for /readyz. A broker that is
// temporarily unavailable keeps HTTP live but not ready while the supervisor
// retries; a database that does not answer within the bounded ping fails
// readiness. It satisfies the existing httpserver.ReadinessChecker interface.
type ingressReadiness struct {
	pool      databasePinger
	dbTimeout time.Duration
	transport transportReadiness
	ingress   admissionReadiness
}

var _ httpserver.ReadinessChecker = (*ingressReadiness)(nil)

func (r *ingressReadiness) Ping(ctx context.Context) error {
	if r.ingress == nil || !r.ingress.Accepting() {
		return errors.New("telemetry ingress is not accepting deliveries")
	}
	if r.transport == nil || !r.transport.Ready() {
		return errors.New("telemetry ingress transport is not connected")
	}
	if r.pool == nil {
		return errors.New("database is unavailable")
	}
	pingCtx := ctx
	if r.dbTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, r.dbTimeout)
		defer cancel()
	}
	return r.pool.Ping(pingCtx)
}

// ingressReadinessChecker returns the composite readiness check when the ingress
// is enabled, and the existing database-only checker otherwise, so the HTTP-only
// path is unchanged.
func ingressReadinessChecker(pool *pgxpool.Pool, dbTimeout time.Duration, runtime ingressRuntime) httpserver.ReadinessChecker {
	if !runtime.enabled {
		return pool
	}
	return &ingressReadiness{
		pool:      pool,
		dbTimeout: dbTimeout,
		transport: runtime.transport,
		ingress:   runtime.ingress,
	}
}
