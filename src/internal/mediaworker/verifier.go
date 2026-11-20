package mediaworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"iot-platform/internal/media"
	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mediastorage"
)

// VerificationRepository defines the repository methods required for media object verification.
type VerificationRepository interface {
	ClaimDueVerification(context.Context, int, time.Duration) ([]media.Record, error)
	FinishVerification(context.Context, media.VerificationGeneration, *media.VerifiedEvidence, string) (media.Record, error)
	VerificationRetry(context.Context, media.VerificationGeneration, string, int, time.Duration) (media.Record, error)
	RecordMissingResult(context.Context, media.VerificationGeneration) (media.Record, bool, error)
}

// ObjectOpener abstracts opening an authenticated storage object stream for verification.
type ObjectOpener interface {
	Open(context.Context, string) (*mediastorage.Object, error)
}

// VerificationPublisher abstracts publishing verification outcome results.
type VerificationPublisher interface {
	PublishResult(context.Context, mediaprotocol.UploadResult) error
}

// VerifierOptions configures the verifier worker pool, scan intervals, and lease parameters.
type VerifierOptions struct {
	Workers, BatchSize, MaxAttempts int
	ScanInterval, StorageTimeout    time.Duration
	RetryMin, RetryMax, Lease       time.Duration
}

// Verifier coordinates background leasing and verification of uploaded media objects.
type Verifier struct {
	repo      VerificationRepository
	storage   ObjectOpener
	publisher VerificationPublisher
	opts      VerifierOptions

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewVerifier constructs a new media Verifier with the specified repository, storage opener, and publisher.
func NewVerifier(repo VerificationRepository, storage ObjectOpener, publisher VerificationPublisher, opts VerifierOptions) *Verifier {
	return &Verifier{repo: repo, storage: storage, publisher: publisher, opts: opts}
}

// Start launches the background polling loop and worker pool for verifying pending media objects.
func (v *Verifier) Start(ctx context.Context) error {
	if v == nil || ctx == nil || v.repo == nil || v.storage == nil || v.publisher == nil ||
		v.opts.Workers < 1 || v.opts.Workers > 8 || v.opts.BatchSize < 1 ||
		v.opts.BatchSize > v.opts.Workers || v.opts.MaxAttempts < 1 ||
		v.opts.ScanInterval <= 0 || v.opts.StorageTimeout <= 0 ||
		v.opts.RetryMin <= 0 || v.opts.RetryMax < v.opts.RetryMin ||
		v.opts.Lease <= v.opts.StorageTimeout || v.opts.Lease > 10*time.Minute {
		return ErrInvalidDelivery
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.done != nil {
		return ErrInvalidDelivery
	}
	runCtx, cancel := context.WithCancel(ctx)
	v.cancel, v.done = cancel, make(chan struct{})
	go func() {
		defer close(v.done)
		ticker := time.NewTicker(v.opts.ScanInterval)
		defer ticker.Stop()
		for {
			v.scan(runCtx)
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (v *Verifier) scan(ctx context.Context) {
	claims, err := v.repo.ClaimDueVerification(ctx, v.opts.BatchSize, v.opts.Lease)
	if err != nil || len(claims) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, rec := range claims {
		wg.Add(1)
		go func(rec media.Record) {
			defer wg.Done()
			v.process(ctx, rec)
		}(rec)
	}
	wg.Wait()
}

func (v *Verifier) process(parent context.Context, rec media.Record) {
	claim := media.VerificationGeneration{GatewayID: rec.GatewayID, UploadID: rec.UploadID,
		MediaID: rec.MediaID, Generation: rec.VerificationGeneration}
	storageCtx, storageCancel := context.WithTimeout(parent, v.opts.StorageTimeout)
	obj, err := v.storage.Open(storageCtx, rec.StoragePath)
	if errors.Is(err, mediastorage.ErrNotFound) {
		storageCancel()
		record, committed, missingErr := v.repo.RecordMissingResult(parent, claim)
		if missingErr == nil && committed {
			_ = v.publisher.PublishResult(parent, resultFromRecord(rec.GatewayID, rec.UploadID, record))
		}
		return
	}
	if err != nil {
		storageCancel()
		v.retry(parent, claim, rec, "STORAGE_UNAVAILABLE")
		return
	}
	if obj == nil || obj.Body == nil {
		storageCancel()
		v.retry(parent, claim, rec, "STORAGE_UNAVAILABLE")
		return
	}
	evidence, verifyErr := VerifyContent(obj.Body, ContentMetadata{
		DeclaredLength: rec.ExpectedSize, DeclaredSHA256: rec.DeclaredSHA256, ServerMIME: obj.ContentType,
	})
	closeErr := obj.Body.Close()
	storageCancel()
	if verifyErr == nil && closeErr != nil {
		verifyErr = &ContentError{Code: "STORAGE_UNAVAILABLE", Retryable: true}
	}
	if verifyErr != nil {
		var contentErr *ContentError
		if errors.As(verifyErr, &contentErr) && contentErr.Retryable {
			v.retry(parent, claim, rec, "STORAGE_UNAVAILABLE")
			return
		}
		if errors.As(verifyErr, &contentErr) {
			committed, finishErr := v.repo.FinishVerification(parent, claim, nil, contentErr.Code)
			if finishErr == nil {
				_ = v.publisher.PublishResult(parent, resultFromRecord(rec.GatewayID, rec.UploadID, committed))
			}
		}
		return
	}
	verified := &media.VerifiedEvidence{ActualSize: evidence.ActualSize, SHA256: evidence.SHA256, MediaType: evidence.MediaType}
	committed, finishErr := v.repo.FinishVerification(parent, claim, verified, "")
	if finishErr == nil {
		_ = v.publisher.PublishResult(parent, resultFromRecord(rec.GatewayID, rec.UploadID, committed))
	}
}

func (v *Verifier) retry(ctx context.Context, claim media.VerificationGeneration, rec media.Record, code string) {
	attempt := rec.VerificationAttempts
	if attempt < 1 {
		attempt = 1
	}
	delay := v.opts.RetryMin
	for i := 1; i < attempt && delay < v.opts.RetryMax; i++ {
		delay *= 2
		if delay > v.opts.RetryMax {
			delay = v.opts.RetryMax
		}
	}
	committed, err := v.repo.VerificationRetry(ctx, claim, code, v.opts.MaxAttempts, delay)
	if err == nil && committed.ValidationStatus == "rejected" {
		_ = v.publisher.PublishResult(ctx, resultFromRecord(rec.GatewayID, rec.UploadID, committed))
	}
}

// Stop cancels the verifier's execution loop without waiting for completion.
func (v *Verifier) Stop() {
	if v == nil {
		return
	}
	v.mu.Lock()
	cancel := v.cancel
	v.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Close stops the verifier and waits for in-flight verification jobs to finish or context cancellation.
func (v *Verifier) Close(ctx context.Context) error {
	if v == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	v.Stop()
	v.mu.Lock()
	done := v.done
	v.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
