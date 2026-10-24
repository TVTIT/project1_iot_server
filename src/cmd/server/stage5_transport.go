package main

import (
	"context"
	"errors"
	"fmt"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttcredential"

	"github.com/jackc/pgx/v5/pgxpool"
)

type stage5Runtime struct {
	transport *mqttcontrol.Transport
	workers   *mqttcontrol.Runtime
	metrics   *mqttcontrol.Metrics
}

func composeStage5Transport(cfg config.Config, pool *pgxpool.Pool) (stage5Runtime, error) {
	if !cfg.Stage5.Enabled {
		return stage5Runtime{}, nil
	}
	if pool == nil {
		return stage5Runtime{}, fmt.Errorf("stage 5 database required")
	}
	repo, err := digitaltwin.NewPostgresControlRepository(pool, cfg.Stage5.Control)
	if err != nil {
		return stage5Runtime{}, err
	}
	ca, err := defaultCompositionSeam.readCA(cfg.Stage5.Data.CAFile)
	if err != nil {
		return stage5Runtime{}, fmt.Errorf("stage 5 CA unavailable")
	}
	management := cfg.Stage5.Management
	management.CAPEM = ca
	data := cfg.Stage5.Data
	data.VerifyConnection = func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, management.Timeout)
		defer cancel()
		client, err := mqttcredential.NewDynSecClient(ctx, management)
		if err != nil {
			return err
		}
		// Also close the tracked management socket at cancellation, so a blocked
		// synchronous Paho write cannot outlive this connection's ACL budget.
		stopClose := context.AfterFunc(ctx, client.Close)
		defer func() { stopClose(); client.Close() }()
		return client.VerifyBackendACL(ctx, data.Username, data.ClientID)
	}
	transport, err := mqttcontrol.NewTransport(data, cfg.Stage5.Dispatch)
	if err != nil {
		return stage5Runtime{}, err
	}
	var observer *mqttcontrol.Metrics
	if cfg.MetricsEnabled {
		observer = &mqttcontrol.Metrics{}
	}
	workers, err := mqttcontrol.New(cfg.Stage5.Dispatch, repo, transport, observer)
	if err != nil {
		return stage5Runtime{}, err
	}
	return stage5Runtime{transport: transport, workers: workers, metrics: observer}, nil
}
func (r stage5Runtime) start() error {
	if r.transport == nil {
		return nil
	}
	if err := r.transport.Start(); err != nil {
		return err
	}
	return r.workers.Start()
}
func (r stage5Runtime) stop() {
	if r.workers != nil {
		r.workers.Stop()
	}
	if r.transport != nil {
		r.transport.StopIntake()
	}
}
func (r stage5Runtime) close(ctx context.Context) error {
	r.stop()
	if r.transport == nil {
		return nil
	}
	return errors.Join(r.transport.Close(ctx), r.workers.Close(ctx))
}

type stage5Readiness struct {
	upstream httpserver.ReadinessChecker
	control  transportReadiness
}

func (r stage5Readiness) Ping(ctx context.Context) error {
	if r.control == nil || !r.control.Ready() {
		return errors.New("stage 5 transport not ready")
	}
	return r.upstream.Ping(ctx)
}
func stage5ReadinessChecker(upstream httpserver.ReadinessChecker, r stage5Runtime) httpserver.ReadinessChecker {
	if r.workers == nil {
		return upstream
	}
	return stage5Readiness{upstream, r.workers}
}
