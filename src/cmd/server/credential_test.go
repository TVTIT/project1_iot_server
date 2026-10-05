package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"iot-platform/internal/config"
)

func TestCredentialCompositionModes(t *testing.T) {
	c, e := composeCredentials(context.Background(), nil, config.CredentialConfig{})
	if e != nil || c.manager != nil || c.ready != nil {
		t.Fatal("disabled mode touched dependencies", e)
	}
	if e = c.close(); e != nil {
		t.Fatal(e)
	}
	if _, e = composeCredentials(context.Background(), nil, config.CredentialConfig{Enabled: true}); e == nil {
		t.Fatal("enabled missing pool")
	}
}

func TestStartupBarrierOrderingAndCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		calls := []string{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runCredentialBarrier(ctx, func(context.Context) error {
			calls = append(calls, "reconcile")
			if fail {
				return errors.New("uncertain")
			}
			return nil
		}, func(clean context.Context) error {
			if clean.Err() != nil {
				t.Fatal("cancelled cleanup")
			}
			if _, ok := clean.Deadline(); !ok {
				t.Fatal("unbounded cleanup")
			}
			calls = append(calls, "close")
			return nil
		}, time.Second)
		if fail && (err == nil || len(calls) != 2) {
			t.Fatal("failed startup not closed")
		}
		if !fail && (err != nil || len(calls) != 1) {
			t.Fatal("successful startup", err)
		}
	}
}
