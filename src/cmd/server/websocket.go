package main

import (
	"context"
	"errors"
	"fmt"

	"iot-platform/internal/config"
	"iot-platform/internal/telemetryws"
)

// websocketRuntime owns the one hub, event runtime, application handler and
// periodic membership rechecker for the process.
type websocketRuntime struct {
	hub     *telemetryws.Hub
	events  *telemetryws.EventRuntime
	worker  *telemetryws.RecheckWorker
	handler *telemetryws.Handler
	metrics *telemetryws.Metrics
}

func composeWebSocket(cfg config.Config) (websocketRuntime, error) {
	if !cfg.WebSocketEnabled {
		return websocketRuntime{}, nil
	}
	w := cfg.WebSocket
	hub := telemetryws.NewHub(telemetryws.HubConfig{MaxSessions: w.MaxSessions, MaxQueueSlots: w.SessionEventSlots, MaxQueueBytes: w.SessionEventBytes, MaxFrameBytes: w.MaxFrameBytes})
	var observer *telemetryws.Metrics
	if cfg.MetricsEnabled {
		observer = &telemetryws.Metrics{}
		hub.SetMetrics(observer)
	}
	events := telemetryws.NewEventRuntime(hub, telemetryws.EventRuntimeConfig{QueueSlots: w.GlobalEventSlots, MaxPendingBytes: w.GlobalEventBytes, MaxFrameBytes: w.MaxFrameBytes}, nil)
	events.SetMetrics(observer)
	if events == nil {
		_ = hub.Close()
		return websocketRuntime{}, errors.New("websocket event runtime unavailable")
	}
	return websocketRuntime{hub: hub, events: events, metrics: observer}, nil
}

func (w *websocketRuntime) configure(cfg config.Config, repo telemetryws.TargetAuthorizerRepository) error {
	if !cfg.WebSocketEnabled {
		return nil
	}
	if w == nil || w.hub == nil || w.events == nil || repo == nil {
		return errors.New("websocket application dependencies unavailable")
	}
	authorizer, err := telemetryws.NewTargetAuthorizer(repo, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create websocket target authorizer: %w", err)
	}
	c := cfg.WebSocket
	app, err := telemetryws.NewApplicationHandlerWithOptions(cfg.CORSAllowedOrigins, telemetryws.ApplicationHandlerDeps{
		Hub: w.hub, Authorizer: authorizer,
		Options: telemetryws.HandlerOptions{
			MaxSessions: c.MaxSessions, MaxSubscriptions: c.MaxSubscriptionsPerSession,
			MaxReadBytes: int64(c.ReadFrameBytes), MaxRequestRate: c.RequestRatePerSecond,
			PingInterval: c.HeartbeatInterval, PongTimeout: c.PongTimeout, WriteTimeout: c.WriteTimeout,
		},
	})
	if err != nil {
		return fmt.Errorf("configure websocket application handler: %w", err)
	}
	w.handler = app
	w.worker = telemetryws.NewRecheckWorker(authorizer, w.hub, telemetryws.RecheckWorkerConfig{
		Interval: c.RecheckInterval, SweepBudget: c.RecheckSweepBudget,
		Concurrency: c.RecheckConcurrency, MaxSessions: c.MaxSessions,
	})
	return nil
}

func (w *websocketRuntime) start(ctx context.Context) error {
	if w == nil || w.worker == nil {
		return nil
	}
	return w.worker.StartContext(ctx)
}

func (w websocketRuntime) close(ctx context.Context) error {
	var result error
	if w.worker != nil {
		result = errors.Join(result, w.worker.StopContext(ctx))
	}
	if w.events != nil {
		result = errors.Join(result, w.events.Close(ctx))
	}
	if w.hub != nil {
		result = errors.Join(result, w.hub.Close())
	}
	return result
}
