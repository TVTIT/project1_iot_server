package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/media"
	"iot-platform/internal/mediastorage"
	"iot-platform/internal/mediaworker"
	"iot-platform/internal/mqttcredential"
	"iot-platform/internal/mqttmedia"

	"github.com/jackc/pgx/v5/pgxpool"
)

type mediaRuntime struct {
	transport *mqttmedia.Transport
	handler   *mediaworker.Handler
	verifier  *mediaworker.Verifier
	cleanup   *mediaworker.CleanupWorker
}

type storageSigner struct{ client *mediastorage.Client }

func (s storageSigner) SignUpload(ctx context.Context, path string) (mediaworker.SignedGrant, error) {
	grant, err := s.client.SignUpload(ctx, path)
	if err != nil {
		return mediaworker.SignedGrant{}, err
	}
	return mediaworker.SignedGrant{URL: grant.SignedURL(), ExpiresAt: grant.ExpiresAt}, nil
}

func composeMediaRuntime(cfg config.Config, pool *pgxpool.Pool) (mediaRuntime, error) {
	if !cfg.MediaUploadEnabled {
		return mediaRuntime{}, nil
	}
	if pool == nil {
		return mediaRuntime{}, fmt.Errorf("media upload database required")
	}
	repo, err := media.NewRepository(pool, cfg.MediaUpload, cfg.MediaUpload.DBTimeout)
	if err != nil {
		return mediaRuntime{}, err
	}
	storage, err := mediastorage.New(mediastorage.Config{
		InternalBaseURL: cfg.MediaRuntime.StorageInternalURL,
		PublicOrigin:    cfg.MediaRuntime.StoragePublicOrigin,
		ServiceToken:    cfg.MediaRuntime.StorageServiceToken,
		Timeout:         cfg.MediaUpload.StorageTimeout,
	})
	if err != nil {
		return mediaRuntime{}, fmt.Errorf("create media Storage client: %w", err)
	}
	ca, err := defaultCompositionSeam.readCA(cfg.MediaRuntime.Data.CAFile)
	if err != nil {
		return mediaRuntime{}, fmt.Errorf("media MQTT CA unavailable")
	}
	management := cfg.MediaRuntime.Management
	management.CAPEM = ca
	validateTarget := func(username string) bool {
		return gateway.ValidateGatewayID(username) == nil &&
			username != management.ManagerUsername &&
			username != mqttcredential.BackendUsername
	}
	management.ValidateTarget = validateTarget
	management.ValidateRole = func(username, role string) bool {
		return validateTarget(username) && role == "gateway_"+username
	}
	data := cfg.MediaRuntime.Data
	data.VerifyConnection = func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, management.Timeout)
		defer cancel()
		client, err := mqttcredential.NewDynSecClient(ctx, management)
		if err != nil {
			return err
		}
		stopClose := context.AfterFunc(ctx, client.Close)
		defer func() { stopClose(); client.Close() }()
		return client.VerifyBackendACL(ctx, data.Username, data.ClientID, true)
	}
	verifyWorkers := cfg.MediaUpload.VerifyWorkers
	verifyBatch := cfg.MediaUpload.VerifyBatchSize
	if verifyBatch > verifyWorkers {
		verifyBatch = verifyWorkers
	}
	lease := cfg.MediaUpload.StorageTimeout + time.Duration(verifyWorkers+2)*cfg.MediaUpload.DBTimeout
	if lease <= cfg.MediaUpload.StorageTimeout || lease > 10*time.Minute {
		return mediaRuntime{}, fmt.Errorf("media verification lease exceeds its bounded maximum")
	}
	transport, err := mqttmedia.NewTransport(data)
	if err != nil {
		return mediaRuntime{}, err
	}
	handler := mediaworker.New(repo, storageSigner{storage}, transport, mediaworker.Options{
		Workers:        cfg.MediaRuntime.Workers,
		SigningTimeout: cfg.MediaRuntime.SigningTimeout, GrantTTL: cfg.MediaRuntime.GrantTTL,
		InFlightGrace: cfg.MediaRuntime.InFlightGrace, PublishTimeout: data.PublishTimeout,
	})
	verifier := mediaworker.NewVerifier(repo, storage, handler, mediaworker.VerifierOptions{
		Workers: verifyWorkers, BatchSize: verifyBatch, MaxAttempts: cfg.MediaUpload.VerifyMaxAttempts,
		ScanInterval: cfg.MediaUpload.VerifyScanInterval, StorageTimeout: cfg.MediaUpload.StorageTimeout,
		RetryMin: cfg.MediaUpload.VerifyRetryMinDelay, RetryMax: cfg.MediaUpload.VerifyRetryMaxDelay,
		Lease: lease,
	})
	cleanup := mediaworker.NewCleanupWorker(repo, storage, cleanupOptionsFromConfig(cfg.MediaUpload))
	return mediaRuntime{transport: transport, handler: handler, verifier: verifier, cleanup: cleanup}, nil
}

func cleanupOptionsFromConfig(cfg config.MediaUploadConfig) mediaworker.CleanupOptions {
	workers := cfg.CleanupWorkers
	if workers < 1 {
		workers = 1
	}
	batchSize := cfg.CleanupBatchSize
	if batchSize < 1 {
		batchSize = 1
	}
	lease := cfg.StorageTimeout + 2*cfg.DBTimeout
	if lease <= cfg.StorageTimeout {
		lease = cfg.StorageTimeout + 10*time.Second
	}
	if lease > 10*time.Minute {
		lease = 10 * time.Minute
	}
	return mediaworker.CleanupOptions{
		Workers:        workers,
		BatchSize:      batchSize,
		ScanInterval:   cfg.CleanupScanInterval,
		StorageTimeout: cfg.StorageTimeout,
		FollowupDelay:  cfg.CleanupRetryMinDelay,
		RetryMin:       cfg.CleanupRetryMinDelay,
		RetryMax:       cfg.CleanupRetryMaxDelay,
		MaxAttempts:    cfg.CleanupMaxAttempts,
		Lease:          lease,
	}
}

func (r mediaRuntime) start(ctx context.Context) error {
	if r.transport == nil {
		return nil
	}
	if err := r.handler.Start(ctx, r.transport.Messages()); err != nil {
		return err
	}
	if err := r.transport.Start(); err != nil {
		r.handler.Stop()
		return err
	}
	if err := r.verifier.Start(ctx); err != nil {
		r.transport.StopIntake()
		r.handler.Stop()
		_ = r.transport.Close(context.Background())
		joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.handler.Close(joinCtx)
		return err
	}
	if err := r.cleanup.Start(ctx); err != nil {
		r.transport.StopIntake()
		r.handler.Stop()
		r.verifier.Stop()
		_ = r.transport.Close(context.Background())
		joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.handler.Close(joinCtx)
		_ = r.verifier.Close(joinCtx)
		return err
	}
	return nil
}

func (r mediaRuntime) stop() {
	if r.transport != nil {
		r.transport.StopIntake()
	}
	if r.handler != nil {
		r.handler.Stop()
	}
	if r.verifier != nil {
		r.verifier.Stop()
	}
	// Close performs cancellation and join for cleanup; deferred until close to
	// preserve the existing non-blocking stop phase.
}

func (r mediaRuntime) close(ctx context.Context) error {
	r.stop()
	if r.transport == nil {
		return nil
	}
	return errors.Join(r.handler.Close(ctx), r.verifier.Close(ctx), r.cleanup.Close(ctx), r.transport.Close(ctx))
}
