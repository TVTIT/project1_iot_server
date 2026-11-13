package mqttingress

import (
	"context"
	"sync"
	"testing"
	"time"
)

type dedupTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *dedupTestClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *dedupTestClock) Add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }
func (c *dedupTestClock) NewTimer(time.Duration) ingressTimer {
	return &controlledTimer{ch: make(chan time.Time)}
}

func proofFixture(at time.Time, suffix string) (dedupKey, dedupMetadata) {
	k := dedupKey{gatewayID: "g" + suffix, sensorID: "s", bootID: "b", firstSequence: 1}
	m := dedupMetadata{messageID: "m" + suffix, payloadHash: "hash", sourceTopic: "gateways/g/telemetry/s", lastSequence: 1, sampleCount: 1, originalTimestamp: at.Add(-time.Second), confirmedCommitAt: at, expiresAt: at.Add(DedupTTL)}
	return k, m
}

func TestDedupCacheLookupTTLAndImmutablePublication(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	c := newDedupCache(true, 10, 1<<20, clock)
	k, m := proofFixture(clock.Now(), "1")
	if got := c.Lookup(k, m); got != dedupLookupMiss {
		t.Fatalf("cold lookup = %v", got)
	}
	if got := c.Publish(k, m); got != dedupPublishStored {
		t.Fatalf("publish = %v", got)
	}
	if got := c.Lookup(k, m); got != dedupLookupHit {
		t.Fatalf("matching lookup = %v", got)
	}
	bad := m
	bad.payloadHash = "other"
	if got := c.Lookup(k, bad); got != dedupLookupMismatch {
		t.Fatalf("mismatch lookup = %v", got)
	}
	if got := c.Publish(k, bad); got != dedupPublishMismatch {
		t.Fatalf("mismatch publish = %v", got)
	}
	clock.Add(DedupTTL - time.Nanosecond)
	if got := c.Lookup(k, m); got != dedupLookupHit {
		t.Fatalf("before expiry = %v", got)
	}
	if got := c.Publish(k, m); got != dedupPublishExisting {
		t.Fatalf("retry publish = %v", got)
	}
	clock.Add(time.Nanosecond)
	if got := c.Lookup(k, m); got != dedupLookupExpired {
		t.Fatalf("at expiry = %v", got)
	}
	clock.Add(time.Nanosecond)
	if got := c.Lookup(k, m); got != dedupLookupExpired {
		t.Fatalf("after expiry = %v", got)
	}
	if s := c.Snapshot(); s.Entries != 1 || s.LogicalBytes != m.logicalBytes(k) {
		t.Fatalf("snapshot after logical expiry = %+v", s)
	}
}

func TestDedupCacheBoundsAcrossGenerationsAndRotate(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	k1, m1 := proofFixture(clock.Now(), "1")
	k2, m2 := proofFixture(clock.Now(), "2")
	c := newDedupCache(true, 1, 1<<20, clock)
	if c.Publish(k1, m1) != dedupPublishStored {
		t.Fatal("first publish failed")
	}
	c.Rotate()
	if c.Publish(k2, m2) != dedupPublishFull {
		t.Fatal("combined generation cap not enforced")
	}
	if c.Lookup(k1, m1) != dedupLookupHit {
		t.Fatal("full cache altered canonical proof")
	}
	if s := c.Snapshot(); s.Entries != 1 || s.LogicalBytes != m1.logicalBytes(k1) {
		t.Fatalf("snapshot = %+v", s)
	}
	c.Rotate()
	if s := c.Snapshot(); s.Entries != 0 || s.LogicalBytes != 0 {
		t.Fatalf("rotation did not release oldest generation: %+v", s)
	}
	byteLimited := newDedupCache(true, 10, m1.logicalBytes(k1)-1, clock)
	if byteLimited.Publish(k1, m1) != dedupPublishFull {
		t.Fatal("byte cap was not enforced")
	}
}

func TestDedupCacheConcurrentSameKeyPublicationAndBypass(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	k, m := proofFixture(clock.Now(), "1")
	c := newDedupCache(true, 1, 1<<20, clock)
	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); c.Publish(k, m) }()
	}
	wg.Wait()
	if s := c.Snapshot(); s.Entries != 1 || s.LogicalBytes != m.logicalBytes(k) {
		t.Fatalf("same-key accounting = %+v", s)
	}
	if c.Publish(dedupKey{}, m) != dedupPublishInvalid {
		t.Fatal("invalid proof did not bypass")
	}
	if newDedupCache(false, 1, 1, clock).Lookup(k, m) != dedupLookupBypass {
		t.Fatal("disabled cache did not bypass")
	}
}

func TestDedupCacheGenerationLifecycleAndPreviousDoesNotPromote(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	c := newDedupCache(true, 10, 1<<20, clock)
	k, proof := proofFixture(clock.Now(), "rotation")
	if c.Publish(k, proof) != dedupPublishStored {
		t.Fatal("publish failed")
	}
	c.Rotate()
	if got := c.Lookup(k, proof); got != dedupLookupHit {
		t.Fatalf("previous lookup = %v", got)
	}
	if got := c.Snapshot(); got.CurrentEntries != 0 || got.PreviousEntries != 1 {
		t.Fatalf("lookup promoted proof: %+v", got)
	}
	c.Rotate()
	if got := c.Lookup(k, proof); got != dedupLookupMiss {
		t.Fatalf("discarded generation lookup = %v", got)
	}
	if got := c.Snapshot(); got.Entries != 0 || got.LogicalBytes != 0 {
		t.Fatalf("discard did not release logical budget: %+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.RunRotation(ctx); err != nil {
		t.Fatalf("canceled-before-start RunRotation: %v", err)
	}
}

func TestDedupCacheExpiredProofCannotSurviveTwoGenerations(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	c := newDedupCache(true, 10, 1<<20, clock)
	k, proof := proofFixture(clock.Now(), "expiry-retained")
	if c.Publish(k, proof) != dedupPublishStored {
		t.Fatal("publish failed")
	}
	clock.Add(DedupTTL - time.Minute)
	c.Rotate()
	clock.Add(time.Minute - time.Nanosecond)
	if c.Lookup(k, proof) != dedupLookupHit {
		t.Fatal("entry expired before exact TTL")
	}
	clock.Add(time.Nanosecond)
	if c.Lookup(k, proof) != dedupLookupExpired {
		t.Fatal("entry did not expire at exact TTL")
	}
	if c.Publish(k, proof) != dedupPublishExisting {
		t.Fatal("retry replaced immutable proof")
	}
	clock.Add(DedupTTL - time.Minute - time.Nanosecond)
	if c.Lookup(k, proof) != dedupLookupExpired {
		t.Fatal("expired proof became valid while physically retained near 20 minutes")
	}
	c.Rotate()
	if c.Lookup(k, proof) != dedupLookupMiss {
		t.Fatal("dropped expired generation was still visible")
	}
}

func TestDedupCacheConcurrentPublicationLookupAndRotationAccounting(t *testing.T) {
	clock := &dedupTestClock{now: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)}
	const n = 24
	c := newDedupCache(true, n, 1<<20, clock)
	keys := make([]dedupKey, n)
	proofs := make([]dedupMetadata, n)
	for i := range keys {
		keys[i], proofs[i] = proofFixture(clock.Now(), string(rune('a'+i)))
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range keys {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				c.Publish(keys[i], proofs[i])
				c.Lookup(keys[i], proofs[i])
				if j%4 == 0 {
					c.Rotate()
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	s := c.Snapshot()
	if s.Entries < 0 || s.LogicalBytes < 0 || s.Entries != s.CurrentEntries+s.PreviousEntries || s.Entries > n {
		t.Fatalf("invalid concurrent generation accounting: %+v", s)
	}
}
