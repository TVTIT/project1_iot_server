package main

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/metrics"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttingress"
	"iot-platform/internal/telemetry"
)

type poolMetrics struct {
	Acquired             int32   `json:"acquired"`
	Idle                 int32   `json:"idle"`
	Total                int32   `json:"total"`
	Max                  int32   `json:"max"`
	AcquireCount         int64   `json:"acquire_count"`
	EmptyAcquireCount    int64   `json:"empty_acquire_count"`
	CanceledAcquireCount int64   `json:"canceled_acquire_count"`
	AcquireSeconds       float64 `json:"acquire_seconds"`
}

type processMetrics struct {
	SchemaVersion    int                                   `json:"schema_version"`
	ProcessInstance  string                                `json:"process_instance"`
	StartedAt        time.Time                             `json:"started_at"`
	ObservedAt       time.Time                             `json:"observed_at"`
	ElapsedSeconds   float64                               `json:"elapsed_seconds"`
	Availability     map[string]string                     `json:"availability"`
	Ingress          *mqttingress.Snapshot                 `json:"ingress"`
	HTTP             []httpserver.HTTPMetricSnapshot       `json:"http"`
	Auth             auth.MetricsSnapshot                  `json:"auth"`
	UnixAuth         *auth.MetricsSnapshot                 `json:"unix_auth,omitempty"`
	UnixAuthHandler  *metrics.HistogramSnapshot            `json:"unix_auth_handler,omitempty"`
	JWKSCache        *auth.JWKSCacheSnapshot               `json:"jwks_cache,omitempty"`
	DB               poolMetrics                           `json:"db"`
	Persistence      *telemetry.PersistenceMetricsSnapshot `json:"persistence,omitempty"`
	CommandAdmission *digitaltwin.ControlMetricsSnapshot   `json:"command_admission"`
	CommandDispatch  *mqttcontrol.MetricsSnapshot          `json:"command_dispatch"`
	CommandResult    *mqttcontrol.ResultMetricsSnapshot    `json:"command_result,omitempty"`
	CommandTimeline  *metrics.ControlTimelineSnapshot      `json:"command_timeline,omitempty"`
	DispatchE2E      *float64                              `json:"dispatch_e2e_seconds"`
	ResultE2E        *float64                              `json:"result_e2e_seconds"`
	BatchWait        *float64                              `json:"batch_wait_seconds"`
	WebSocket        *float64                              `json:"websocket_seconds"`
}

// Sources are read-only, bounded memory/pool snapshots; never SQL or environment.
func populateMetricsDependencies(deps *httpserver.RouterDependencies, cfg config.Config, pool *pgxpool.Pool, ingress ingressRuntime, control stage5Runtime) {
	if !cfg.MetricsEnabled {
		return
	}
	deps.MetricsEnabled = true
	deps.HTTPMetrics = httpserver.NewHTTPMetrics()
	deps.AuthMetrics = &auth.Metrics{}
	if cfg.AuthUnixSocketPath != "" {
		deps.UnixAuthMetrics = &auth.Metrics{}
		deps.UnixAuthDuration = &metrics.Histogram{}
	}
	started, instance := time.Now(), uuid.NewString()
	deps.MetricsSnapshot = func() any {
		now := time.Now()
		batchAvailability := "disabled"
		if cfg.Ingress.MicrobatchEnabled {
			batchAvailability = "unavailable"
		}
		s := processMetrics{SchemaVersion: 1, ProcessInstance: instance, StartedAt: started.UTC(), ObservedAt: now.UTC(), ElapsedSeconds: now.Sub(started).Seconds(),
			Availability: map[string]string{"ingress": "disabled", "http": "available", "auth": "available", "db": "available", "command": "disabled", "dispatch_e2e": "not_implemented", "result": "not_implemented", "persistence": "unavailable", "group_persistence": batchAvailability, "batch_wait": batchAvailability, "websocket": "not_implemented"},
			HTTP:         deps.HTTPMetrics.Snapshot(), Auth: deps.AuthMetrics.Snapshot()}
		if deps.UnixAuthMetrics != nil {
			a, h := deps.UnixAuthMetrics.Snapshot(), deps.UnixAuthDuration.Snapshot()
			s.UnixAuth, s.UnixAuthHandler = &a, &h
		}
		if v, ok := deps.TokenVerifier.(interface{ CacheSnapshot() auth.JWKSCacheSnapshot }); ok {
			c := v.CacheSnapshot()
			s.JWKSCache = &c
		}
		if ingress.ingress != nil {
			v := ingress.ingress.Snapshot()
			s.Ingress = &v
			s.Availability["ingress"] = "available"
			if !v.Microbatch.Enabled {
				s.Availability["group_persistence"] = "disabled"
				s.Availability["batch_wait"] = "disabled"
			} else if cfg.MetricsEnabled && ingress.metrics != nil {
				s.Availability["group_persistence"] = "available"
				s.Availability["batch_wait"] = "available"
			}
		}
		if ingress.metrics != nil {
			v := ingress.metrics.Snapshot()
			s.Persistence = &v
			s.Availability["persistence"] = "available"
		}
		if deps.ControlMetrics != nil {
			v := deps.ControlMetrics.Snapshot()
			s.CommandAdmission = &v
			s.Availability["command"] = "available"
		}
		if control.metrics != nil {
			v := control.metrics.Snapshot()
			s.CommandDispatch = &v
		}
		if control.resultMetrics != nil {
			v := control.resultMetrics.Snapshot()
			s.CommandResult = &v
			s.Availability["result"] = "available"
		}
		if control.timeline != nil {
			v := control.timeline.Snapshot()
			s.CommandTimeline = &v
			s.Availability["dispatch_e2e"] = "available"
		}
		p := pool.Stat()
		s.DB = poolMetrics{p.AcquiredConns(), p.IdleConns(), p.TotalConns(), p.MaxConns(), p.AcquireCount(), p.EmptyAcquireCount(), p.CanceledAcquireCount(), p.AcquireDuration().Seconds()}
		return s
	}
}
