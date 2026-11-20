package mediaworker

import (
	"context"
	"sync"
	"time"

	"iot-platform/internal/media"
)

type CleanupRepository interface {
	ExpireDue(context.Context, int) (int, error)
	ClaimDueCleanup(context.Context, int, time.Duration) ([]media.CleanupClaim, error)
	ScheduleCleanupRetry(context.Context, media.CleanupClaim, time.Duration, string) error
	ConfirmCleanup(context.Context, media.CleanupClaim) error
}

type ObjectDeleter interface {
	Delete(context.Context, string) error
}

type CleanupOptions struct {
	Workers                                                         int
	BatchSize                                                       int
	ScanInterval, StorageTimeout, FollowupDelay, RetryMin, RetryMax time.Duration
	MaxAttempts                                                     int
	Lease                                                           time.Duration
}

type CleanupWorker struct {
	repo    CleanupRepository
	storage ObjectDeleter
	opts    CleanupOptions
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewCleanupWorker(repo CleanupRepository, storage ObjectDeleter, opts CleanupOptions) *CleanupWorker {
	return &CleanupWorker{repo: repo, storage: storage, opts: opts}
}

func (w *CleanupWorker) Options() CleanupOptions {
	if w == nil {
		return CleanupOptions{}
	}
	return w.opts
}

func (w *CleanupWorker) Start(ctx context.Context) error {
	if w == nil || ctx == nil || w.repo == nil || w.storage == nil || w.opts.BatchSize < 1 ||
		w.opts.BatchSize > 64 || w.opts.ScanInterval <= 0 || w.opts.StorageTimeout <= 0 ||
		w.opts.FollowupDelay <= 0 || w.opts.RetryMin <= 0 || w.opts.RetryMax < w.opts.RetryMin ||
		w.opts.MaxAttempts < 2 || w.opts.MaxAttempts > 20 ||
		w.opts.Workers < 0 || w.opts.Workers > 4 {
		return ErrInvalidDelivery
	}
	if w.opts.Workers == 0 {
		w.opts.Workers = 1
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done != nil {
		return ErrInvalidDelivery
	}
	run, cancel := context.WithCancel(ctx)
	w.cancel, w.done = cancel, make(chan struct{})
	go func() {
		defer close(w.done)
		t := time.NewTicker(w.opts.ScanInterval)
		defer t.Stop()
		for {
			w.scan(run)
			select {
			case <-run.Done():
				return
			case <-t.C:
			}
		}
	}()
	return nil
}

func (w *CleanupWorker) lease() time.Duration {
	if w.opts.Lease > w.opts.StorageTimeout && w.opts.Lease <= 10*time.Minute {
		return w.opts.Lease
	}
	lease := 2 * w.opts.StorageTimeout
	if lease > 10*time.Minute {
		lease = 10 * time.Minute
	}
	return lease
}

func (w *CleanupWorker) scan(ctx context.Context) {
	if w == nil || ctx == nil || w.repo == nil || w.storage == nil {
		return
	}
	_, _ = w.repo.ExpireDue(ctx, w.opts.BatchSize)
	workers := w.opts.Workers
	if workers <= 0 {
		workers = 1
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var claimedCount int
	var mu sync.Mutex

	for {
		if ctx.Err() != nil {
			break
		}
		mu.Lock()
		if claimedCount >= w.opts.BatchSize {
			mu.Unlock()
			break
		}
		mu.Unlock()

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}

		mu.Lock()
		if claimedCount >= w.opts.BatchSize {
			mu.Unlock()
			<-sem
			break
		}
		claims, err := w.repo.ClaimDueCleanup(ctx, 1, w.lease())
		if err != nil || len(claims) == 0 {
			mu.Unlock()
			<-sem
			break
		}
		claimedCount += len(claims)
		c := claims[0]
		mu.Unlock()

		wg.Add(1)
		go func(claim media.CleanupClaim) {
			defer func() {
				<-sem
				wg.Done()
			}()
			w.process(ctx, claim)
		}(c)
	}
	wg.Wait()
}

func (w *CleanupWorker) process(parent context.Context, c media.CleanupClaim) {
	storageCtx, storageCancel := context.WithTimeout(parent, w.opts.StorageTimeout)
	err := w.storage.Delete(storageCtx, c.Path)
	storageCancel()
	if err != nil {
		delay := w.retryDelay(c.Attempt)
		reason := "STORAGE_UNAVAILABLE"
		if c.Attempt >= w.opts.MaxAttempts {
			reason = "CLEANUP_EXHAUSTED"
		}
		_ = w.repo.ScheduleCleanupRetry(parent, c, delay, reason)
		return
	}
	// First confirmed absence starts a bounded late-write observation window;
	// a second delete after that window catches PUTs already in flight at expiry.
	if !c.FollowupDue {
		_ = w.repo.ScheduleCleanupRetry(parent, c, w.opts.FollowupDelay, "FOLLOWUP_WINDOW")
		return
	}
	_ = w.repo.ConfirmCleanup(parent, c)
}

func (w *CleanupWorker) retryDelay(attempt int) time.Duration {
	delay := w.opts.RetryMin
	for i := 1; i < attempt && delay < w.opts.RetryMax; i++ {
		delay *= 2
		if delay > w.opts.RetryMax {
			delay = w.opts.RetryMax
		}
	}
	return delay
}

func (w *CleanupWorker) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	done := w.done
	w.mu.Unlock()
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
