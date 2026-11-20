package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/metrics"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttcredential"

	"github.com/jackc/pgx/v5/pgxpool"
)

type stage5Runtime struct {
	transport             *mqttcontrol.Transport
	workers               *mqttcontrol.Runtime
	metrics               *mqttcontrol.Metrics
	results               *mqttcontrol.ResultConsumer
	resultMetrics         *mqttcontrol.ResultMetrics
	receiptMetrics        *mqttcontrol.ReceiptMetrics
	admissionMetrics      *mqttcontrol.ResponseAdmissionMetrics
	queryRuntime          *mqttcontrol.ControlQueryRuntime
	queryMetrics          *mqttcontrol.ControlQueryMetrics
	reconciliationMetrics *mqttcontrol.ReconciliationMetrics
	timeline              *metrics.ControlTimeline
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
	var timeline *metrics.ControlTimeline
	if cfg.MetricsEnabled {
		timeline, err = metrics.NewControlTimeline(1024, 10*time.Minute)
		if err != nil {
			return stage5Runtime{}, err
		}
		repo.SetControlTimeline(timeline)
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
		return client.VerifyBackendACL(ctx, data.Username, data.ClientID, cfg.MediaUploadEnabled)
	}
	transport, err := mqttcontrol.NewTransport(data, cfg.Stage5.Dispatch)
	if err != nil {
		return stage5Runtime{}, err
	}
	var observer *mqttcontrol.Metrics
	if cfg.MetricsEnabled {
		observer = &mqttcontrol.Metrics{}
	}
	var resultObserver *mqttcontrol.ResultMetrics
	var receiptObserver *mqttcontrol.ReceiptMetrics
	var responseAdmissionObserver *mqttcontrol.ResponseAdmissionMetrics
	if cfg.MetricsEnabled {
		resultObserver = &mqttcontrol.ResultMetrics{}
		receiptObserver = &mqttcontrol.ReceiptMetrics{}
		responseAdmissionObserver = &mqttcontrol.ResponseAdmissionMetrics{}
	}
	transport.SetResultMetrics(resultObserver)
	transport.SetReceiptMetrics(receiptObserver)
	transport.SetResponseAdmissionMetrics(responseAdmissionObserver)
	results, err := mqttcontrol.NewResultConsumer(cfg.Stage5.ResultWorkers, cfg.Stage5.Control.DBTimeout, repo, transport.Results(), resultObserver)
	if err != nil {
		return stage5Runtime{}, err
	}
	if err := results.ConfigureReceipts(cfg.Stage5.Stage6Enabled, repo, receiptObserver); err != nil {
		return stage5Runtime{}, fmt.Errorf("configure stage 5 receipt consumer: %w", err)
	}
	workers, err := mqttcontrol.New(cfg.Stage5.Dispatch, repo, transport, observer)
	if err != nil {
		return stage5Runtime{}, err
	}
	workers.SetControlTimeline(timeline)
	var queryMetrics *mqttcontrol.ControlQueryMetrics
	var queryRuntime *mqttcontrol.ControlQueryRuntime
	var reconciliationMetrics *mqttcontrol.ReconciliationMetrics
	if cfg.Stage5.Stage6Enabled {
		if cfg.MetricsEnabled {
			queryMetrics = &mqttcontrol.ControlQueryMetrics{}
			reconciliationMetrics = &mqttcontrol.ReconciliationMetrics{}
		}
		if cfg.Stage5.Dispatch.WakeupEnabled {
			repo.SetAdmissionNotifier(workers.Notify)
		}
		queryRuntime, err = mqttcontrol.NewControlQueryRuntime(repo, transport,
			mqttcontrol.NewReconciliationSnapshotHandler(repo, reconciliationMetrics), queryMetrics)
		if err != nil {
			return stage5Runtime{}, fmt.Errorf("configure stage 6 query runtime: %w", err)
		}
		if err := results.ConfigureSnapshots(true, queryRuntime.HandleSnapshot); err != nil {
			return stage5Runtime{}, fmt.Errorf("configure stage 6 snapshot routing: %w", err)
		}
	}
	return stage5Runtime{transport: transport, workers: workers, metrics: observer, results: results, resultMetrics: resultObserver, receiptMetrics: receiptObserver, admissionMetrics: responseAdmissionObserver, queryRuntime: queryRuntime, queryMetrics: queryMetrics, reconciliationMetrics: reconciliationMetrics, timeline: timeline}, nil
}
func (r stage5Runtime) start() error {
	if r.transport == nil {
		return nil
	}
	if err := r.transport.Start(); err != nil {
		return err
	}
	if err := r.results.Start(); err != nil {
		r.stop()
		return err
	}
	if err := r.workers.Start(); err != nil {
		r.stop()
		return err
	}
	if r.queryRuntime != nil {
		if err := r.queryRuntime.Start(); err != nil {
			r.stop()
			return err
		}
	}
	return nil
}
func (r stage5Runtime) stop() {
	if r.transport != nil {
		r.transport.StopIntake()
	}
	if r.results != nil {
		r.results.Stop()
	}
	if r.workers != nil {
		r.workers.Stop()
	}
	if r.queryRuntime != nil {
		r.queryRuntime.Stop()
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
	var queryErr error
	if r.queryRuntime != nil {
		queryErr = r.queryRuntime.Close(ctx)
	}
	return errors.Join(r.transport.Close(ctx), r.workers.Close(ctx), r.results.Close(ctx), queryErr)
}

func (r stage5Runtime) Ready() bool {
	return stage5RuntimeReadiness(r.transport != nil && r.transport.Ready(),
		r.workers != nil && r.workers.Ready(), r.results != nil && r.results.Ready())
}

func stage5RuntimeReadiness(transportReady, workersReady, resultsReady bool) bool {
	return transportReady && workersReady && resultsReady
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
	return stage5Readiness{upstream, r}
}
