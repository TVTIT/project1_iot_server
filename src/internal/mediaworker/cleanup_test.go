package mediaworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/media"
)

type cleanupRepoFake struct {
	expired, claimed, scheduled, confirmed int
	lastReason                             string
}

func (f *cleanupRepoFake) ExpireDue(ctx context.Context, _ int) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	f.expired++
	return 1, nil
}
func (f *cleanupRepoFake) ClaimDueCleanup(ctx context.Context, _ int, _ time.Duration) ([]media.CleanupClaim, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.claimed++
	return []media.CleanupClaim{{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 1}}, nil
}
func (f *cleanupRepoFake) ScheduleCleanupRetry(ctx context.Context, _ media.CleanupClaim, _ time.Duration, reason string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.scheduled++
	f.lastReason = reason
	return nil
}
func (f *cleanupRepoFake) ConfirmCleanup(ctx context.Context, _ media.CleanupClaim) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.confirmed++
	return nil
}

type cleanupStorageFake struct {
	err     error
	deletes int
}

func (f *cleanupStorageFake) Delete(context.Context, string) error { f.deletes++; return f.err }

func TestCleanupProcessRetriesFailureAndConfirmsSuccessfulDelete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantRetry int
		wantDone  int
	}{
		{name: "storage failure", err: errors.New("unavailable"), wantRetry: 1},
		{name: "first absence starts followup", err: nil, wantRetry: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, storage := &cleanupRepoFake{}, &cleanupStorageFake{err: tc.err}
			w := CleanupWorker{repo: repo, storage: storage, opts: CleanupOptions{StorageTimeout: time.Second, FollowupDelay: time.Second, RetryMin: time.Second, RetryMax: time.Minute}}
			w.process(context.Background(), media.CleanupClaim{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 1})
			if repo.scheduled != tc.wantRetry || repo.confirmed != tc.wantDone || storage.deletes != 1 {
				t.Fatalf("retry=%d confirmed=%d deletes=%d", repo.scheduled, repo.confirmed, storage.deletes)
			}
		})
	}
	repo, storage := &cleanupRepoFake{}, &cleanupStorageFake{}
	w := CleanupWorker{repo: repo, storage: storage, opts: CleanupOptions{StorageTimeout: time.Second, FollowupDelay: time.Second, RetryMin: time.Second, RetryMax: time.Minute}}
	w.process(context.Background(), media.CleanupClaim{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 2, FollowupDue: true})
	if repo.confirmed != 1 || storage.deletes != 1 {
		t.Fatalf("followup confirmation=%d deletes=%d", repo.confirmed, storage.deletes)
	}
}

func TestCleanupRetryDelayIsBounded(t *testing.T) {
	w := CleanupWorker{opts: CleanupOptions{RetryMin: 2 * time.Second, RetryMax: 5 * time.Second}}
	for attempt, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 5 * time.Second, 20: 5 * time.Second} {
		if got := w.retryDelay(attempt); got != want {
			t.Fatalf("attempt %d delay=%s want %s", attempt, got, want)
		}
	}
}

type timeoutDeleter struct{}

func (timeoutDeleter) Delete(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestCleanupStorageTimeoutRecordsDurableRetry(t *testing.T) {
	t.Run("storage delete timeout records retry", func(t *testing.T) {
		repo := &cleanupRepoFake{}
		w := CleanupWorker{
			repo: repo, storage: timeoutDeleter{},
			opts: CleanupOptions{StorageTimeout: 10 * time.Millisecond, FollowupDelay: time.Second, RetryMin: time.Second, RetryMax: time.Minute, MaxAttempts: 5},
		}
		w.process(context.Background(), media.CleanupClaim{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 1})
		if repo.scheduled != 1 || repo.lastReason != "STORAGE_UNAVAILABLE" {
			t.Fatalf("expected durable retry scheduled with STORAGE_UNAVAILABLE; got scheduled=%d reason=%q", repo.scheduled, repo.lastReason)
		}
		if repo.confirmed != 0 {
			t.Fatalf("ConfirmCleanup unexpectedly called: %d", repo.confirmed)
		}
	})

	t.Run("storage delete timeout at max attempts records exhaustion", func(t *testing.T) {
		repo := &cleanupRepoFake{}
		w := CleanupWorker{
			repo: repo, storage: timeoutDeleter{},
			opts: CleanupOptions{StorageTimeout: 10 * time.Millisecond, FollowupDelay: time.Second, RetryMin: time.Second, RetryMax: time.Minute, MaxAttempts: 5},
		}
		w.process(context.Background(), media.CleanupClaim{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 5})
		if repo.scheduled != 1 || repo.lastReason != "CLEANUP_EXHAUSTED" {
			t.Fatalf("expected cleanup exhausted scheduled; got scheduled=%d reason=%q", repo.scheduled, repo.lastReason)
		}
		if repo.confirmed != 0 {
			t.Fatalf("ConfirmCleanup unexpectedly called: %d", repo.confirmed)
		}
	})

	t.Run("parent cancellation propagation", func(t *testing.T) {
		repo := &cleanupRepoFake{}
		w := CleanupWorker{
			repo: repo, storage: timeoutDeleter{},
			opts: CleanupOptions{StorageTimeout: 100 * time.Millisecond, FollowupDelay: time.Second, RetryMin: time.Second, RetryMax: time.Minute, MaxAttempts: 5},
		}
		parentCtx, cancel := context.WithCancel(context.Background())
		cancel()
		w.process(parentCtx, media.CleanupClaim{Path: "00000000-0000-4000-8000-000000000001.mp4", Attempt: 1})
		if repo.scheduled != 0 {
			t.Fatalf("detached persistent update occurred with canceled parent: %d", repo.scheduled)
		}
	})
}

type leaseTrackingStorageFake struct {
	mu          sync.Mutex
	deleteDelay time.Duration
	currentTime time.Time
}

func (s *leaseTrackingStorageFake) Delete(_ context.Context, _ string) error {
	s.mu.Lock()
	s.currentTime = s.currentTime.Add(s.deleteDelay)
	s.mu.Unlock()
	return nil
}

type leaseTrackingRepoFake struct {
	mu           sync.Mutex
	storage      *leaseTrackingStorageFake
	pending      []media.CleanupClaim
	claimTimes   map[string]time.Time
	claimLeases  map[string]time.Duration
	maxBatchSize int
	confirmed    int
	expiredCalls int
}

func (r *leaseTrackingRepoFake) ExpireDue(_ context.Context, _ int) (int, error) {
	return 0, nil
}

func (r *leaseTrackingRepoFake) ClaimDueCleanup(_ context.Context, limit int, lease time.Duration) ([]media.CleanupClaim, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit > r.maxBatchSize {
		r.maxBatchSize = limit
	}
	if len(r.pending) == 0 {
		return nil, nil
	}
	n := limit
	if n > len(r.pending) {
		n = len(r.pending)
	}
	claimed := make([]media.CleanupClaim, n)
	copy(claimed, r.pending[:n])
	r.pending = r.pending[n:]

	now := r.storage.currentTime
	for _, c := range claimed {
		r.claimTimes[c.Path] = now
		r.claimLeases[c.Path] = lease
	}
	return claimed, nil
}

func (r *leaseTrackingRepoFake) ScheduleCleanupRetry(_ context.Context, _ media.CleanupClaim, _ time.Duration, _ string) error {
	return nil
}

func (r *leaseTrackingRepoFake) ConfirmCleanup(_ context.Context, c media.CleanupClaim) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	claimedAt := r.claimTimes[c.Path]
	lease := r.claimLeases[c.Path]
	now := r.storage.currentTime
	if now.Sub(claimedAt) > lease {
		r.expiredCalls++
		return errors.New("lease expired: item outlived its claim")
	}
	r.confirmed++
	return nil
}

func TestCleanupBatchSerialLeaseStarvationPrevented(t *testing.T) {
	claims := []media.CleanupClaim{
		{Path: "item1.mp4", Attempt: 2, FollowupDue: true},
		{Path: "item2.mp4", Attempt: 2, FollowupDue: true},
		{Path: "item3.mp4", Attempt: 2, FollowupDue: true},
	}
	storage := &leaseTrackingStorageFake{
		deleteDelay: 80 * time.Millisecond,
		currentTime: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC),
	}
	repo := &leaseTrackingRepoFake{
		storage:     storage,
		pending:     claims,
		claimTimes:  make(map[string]time.Time),
		claimLeases: make(map[string]time.Duration),
	}
	w := &CleanupWorker{
		repo:    repo,
		storage: storage,
		opts: CleanupOptions{
			Workers:        1,
			BatchSize:      16,
			StorageTimeout: 100 * time.Millisecond,
			Lease:          200 * time.Millisecond,
			MaxAttempts:    5,
		},
	}
	w.scan(context.Background())

	if repo.maxBatchSize > 1 {
		t.Fatalf("ClaimDueCleanup claimed %d items in one call; expected at most 1 item per worker slot", repo.maxBatchSize)
	}
	if repo.expiredCalls > 0 {
		t.Fatalf("%d items outlived their claim lease", repo.expiredCalls)
	}
	if repo.confirmed != 3 {
		t.Fatalf("confirmed=%d, want 3", repo.confirmed)
	}
}
