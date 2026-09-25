package database

import (
	"context"
	"testing"
	"time"
)

func TestOpenRejectsInvalidDatabaseURL(t *testing.T) {
	ctx := context.Background()
	_, err := Open(ctx, "invalid-url", 5, 1, time.Second)
	if err == nil {
		t.Fatal("Open() error = nil, want invalid database configuration error")
	}
}

func TestOpenFailsOnUnreachableHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately so ping fails fast without waiting for network timeout

	_, err := Open(ctx, "postgres://user:pass@127.0.0.1:54329/unreachable?sslmode=disable", 5, 1, 50*time.Millisecond)
	if err == nil {
		t.Fatal("Open() error = nil, want connection failure on cancelled context / unreachable host")
	}
}
