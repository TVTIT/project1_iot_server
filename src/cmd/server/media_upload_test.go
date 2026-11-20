package main

import (
	"context"
	"testing"
	"time"

	"iot-platform/internal/config"
)

func TestMediaRuntimeOffIsInert(t *testing.T) {
	runtime, err := composeMediaRuntime(config.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.transport != nil || runtime.handler != nil {
		t.Fatal("media-off composition created runtime components")
	}
	if err := runtime.start(context.Background()); err != nil {
		t.Fatalf("inert start: %v", err)
	}
	if err := runtime.close(context.Background()); err != nil {
		t.Fatalf("inert close: %v", err)
	}
}

func TestCleanupOptionsFromConfig(t *testing.T) {
	cfg := config.MediaUploadConfig{
		StorageTimeout:       10 * time.Second,
		DBTimeout:            2 * time.Second,
		VerifyWorkers:        2,
		VerifyBatchSize:      5,
		VerifyScanInterval:   3 * time.Second,
		VerifyRetryMinDelay:  7 * time.Second,
		VerifyRetryMaxDelay:  11 * time.Minute,
		VerifyMaxAttempts:    4,
		CleanupWorkers:       3,
		CleanupBatchSize:     7,
		CleanupScanInterval:  13 * time.Second,
		CleanupRetryMinDelay: 17 * time.Second,
		CleanupRetryMaxDelay: 23 * time.Minute,
		CleanupMaxAttempts:   9,
	}
	opts := cleanupOptionsFromConfig(cfg)
	if opts.Workers != 3 {
		t.Fatalf("opts.Workers=%d, want 3", opts.Workers)
	}
	if opts.BatchSize != 7 {
		t.Fatalf("opts.BatchSize=%d, want 7", opts.BatchSize)
	}
	if opts.ScanInterval != 13*time.Second {
		t.Fatalf("opts.ScanInterval=%v, want 13s", opts.ScanInterval)
	}
	if opts.FollowupDelay != 17*time.Second {
		t.Fatalf("opts.FollowupDelay=%v, want 17s", opts.FollowupDelay)
	}
	if opts.RetryMin != 17*time.Second {
		t.Fatalf("opts.RetryMin=%v, want 17s", opts.RetryMin)
	}
	if opts.RetryMax != 23*time.Minute {
		t.Fatalf("opts.RetryMax=%v, want 23m", opts.RetryMax)
	}
	if opts.MaxAttempts != 9 {
		t.Fatalf("opts.MaxAttempts=%d, want 9", opts.MaxAttempts)
	}
	if opts.StorageTimeout != 10*time.Second {
		t.Fatalf("opts.StorageTimeout=%v, want 10s", opts.StorageTimeout)
	}
	if opts.Lease <= opts.StorageTimeout || opts.Lease > 10*time.Minute {
		t.Fatalf("opts.Lease=%v out of bounds", opts.Lease)
	}
}
