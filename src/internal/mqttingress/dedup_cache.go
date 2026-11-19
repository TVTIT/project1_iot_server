package mqttingress

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type dedupCacheClock interface{ Now() time.Time }

type dedupLookupResult uint8

const (
	dedupLookupBypass dedupLookupResult = iota
	dedupLookupMiss
	dedupLookupHit
	dedupLookupExpired
	dedupLookupMismatch
)

type dedupPublishResult uint8

const (
	dedupPublishBypass dedupPublishResult = iota
	dedupPublishStored
	dedupPublishExisting
	dedupPublishMismatch
	dedupPublishFull
	dedupPublishInvalid
)

type dedupCacheEntry struct {
	metadata     dedupMetadata
	logicalBytes int64
}

type dedupCacheGeneration struct {
	entries *sync.Map
	count   int64
	bytes   int64
}

// dedupCache stores only immutable proofs whose caller has already observed a
// confirmed DB commit. Its byte limit is a logical estimate, not an RSS limit.
// Expired values remain accounted until rotation drops their generation.
type dedupCache struct {
	mu           sync.Mutex
	current      dedupCacheGeneration
	previous     dedupCacheGeneration
	enabled      bool
	maxEntries   int64
	maxBytes     int64
	entries      int64
	logicalBytes int64
	clock        dedupCacheClock
	lookups      dedupCacheCounters
	publications dedupPublicationCounters
	rotations    atomic.Int64
}

type dedupCacheCounters struct {
	hit, miss, expired, mismatch, disabledBypass atomic.Int64
}
type dedupPublicationCounters struct {
	stored, existing, mismatch, full, invalid, disabledBypass atomic.Int64
}

// DedupCacheSnapshot is fixed-cardinality cache state and cumulative event counts.
// LogicalBytes is an estimate, not process RSS; DatabaseCallsAvoided is exactly
// the validated cache-hit count because those hits return before repository calls.
type DedupCacheSnapshot struct {
	Enabled                     bool  `json:"enabled"`
	TTLSeconds                  int64 `json:"ttl_seconds"`
	MaxEntries                  int64 `json:"max_entries"`
	MaxBytes                    int64 `json:"max_bytes"`
	Entries                     int64 `json:"entries"`
	LogicalBytes                int64 `json:"logical_bytes"`
	CurrentEntries              int64 `json:"current_generation_entries"`
	PreviousEntries             int64 `json:"previous_generation_entries"`
	LookupHit                   int64 `json:"lookup_hit_total"`
	LookupMiss                  int64 `json:"lookup_miss_total"`
	LookupExpired               int64 `json:"lookup_expired_total"`
	LookupMetadataMismatch      int64 `json:"lookup_metadata_mismatch_total"`
	LookupDisabledBypass        int64 `json:"lookup_disabled_bypass_total"`
	PublicationStored           int64 `json:"publication_stored_total"`
	PublicationExisting         int64 `json:"publication_existing_total"`
	PublicationMetadataMismatch int64 `json:"publication_metadata_mismatch_total"`
	PublicationFull             int64 `json:"publication_full_total"`
	PublicationInvalidSkip      int64 `json:"publication_invalid_skip_total"`
	PublicationDisabledBypass   int64 `json:"publication_disabled_bypass_total"`
	CapacityBypass              int64 `json:"capacity_bypass_total"`
	Rotations                   int64 `json:"rotations_total"`
	DatabaseCallsAvoided        int64 `json:"database_calls_avoided_total"`
}

func newDedupCache(enabled bool, maxEntries, maxBytes int64, clock dedupCacheClock) *dedupCache {
	if clock == nil {
		clock = realDedupCacheClock{}
	}
	return &dedupCache{enabled: enabled, maxEntries: maxEntries, maxBytes: maxBytes, clock: clock, current: dedupCacheGeneration{entries: &sync.Map{}}, previous: dedupCacheGeneration{entries: &sync.Map{}}}
}

type realDedupCacheClock struct{}

func (realDedupCacheClock) Now() time.Time { return time.Now() }

func (c *dedupCache) Lookup(key dedupKey, metadata dedupMetadata) dedupLookupResult {
	if c == nil {
		return dedupLookupBypass
	}
	if !c.enabled {
		c.lookups.disabledBypass.Add(1)
		return dedupLookupBypass
	}
	if !validDedupProof(key, metadata, c.clock.Now()) {
		return dedupLookupBypass
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, generation := range []*dedupCacheGeneration{&c.current, &c.previous} {
		if stored, ok := generation.entries.Load(key); ok {
			entry := stored.(dedupCacheEntry)
			if !entry.metadata.equal(metadata) {
				c.lookups.mismatch.Add(1)
				return dedupLookupMismatch
			}
			if !c.clock.Now().Before(entry.metadata.expiresAt) {
				c.lookups.expired.Add(1)
				return dedupLookupExpired
			}
			c.lookups.hit.Add(1)
			return dedupLookupHit
		}
	}
	c.lookups.miss.Add(1)
	return dedupLookupMiss
}

func (c *dedupCache) Publish(key dedupKey, metadata dedupMetadata) dedupPublishResult {
	if c == nil || !c.enabled {
		if c != nil {
			c.publications.disabledBypass.Add(1)
		}
		return dedupPublishBypass
	}
	now := c.clock.Now()
	if !validDedupProof(key, metadata, now) {
		c.publications.invalid.Add(1)
		return dedupPublishInvalid
	}
	bytes := metadata.logicalBytes(key)
	if bytes < 0 {
		c.publications.invalid.Add(1)
		return dedupPublishInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, generation := range []*dedupCacheGeneration{&c.current, &c.previous} {
		if stored, ok := generation.entries.Load(key); ok {
			if stored.(dedupCacheEntry).metadata.equal(metadata) {
				c.publications.existing.Add(1)
				return dedupPublishExisting
			}
			c.publications.mismatch.Add(1)
			return dedupPublishMismatch
		}
	}
	if c.maxEntries <= 0 || c.maxBytes <= 0 || c.entries >= c.maxEntries || bytes > c.maxBytes-c.logicalBytes {
		c.publications.full.Add(1)
		return dedupPublishFull
	}
	// LoadOrStore remains the map-level publication primitive; reservation is
	// under the same short lock so both generations share one capacity budget.
	actual, loaded := c.current.entries.LoadOrStore(key, dedupCacheEntry{metadata: metadata, logicalBytes: bytes})
	if loaded {
		if actual.(dedupCacheEntry).metadata.equal(metadata) {
			c.publications.existing.Add(1)
			return dedupPublishExisting
		}
		c.publications.mismatch.Add(1)
		return dedupPublishMismatch
	}
	c.entries++
	c.logicalBytes += bytes
	c.current.count++
	c.current.bytes += bytes
	c.publications.stored.Add(1)
	return dedupPublishStored
}

// Rotate drops the old previous generation and advances current. The cache lock
// serializes generation ownership with lookup/publication; accounting release is
// O(1), and dropping a map releases logical budget rather than promising that
// the runtime immediately returns its memory to the OS.
func (c *dedupCache) Rotate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries -= c.previous.count
	c.logicalBytes -= c.previous.bytes
	c.previous = c.current
	c.current = dedupCacheGeneration{entries: &sync.Map{}}
	c.rotations.Add(1)
}

// RunRotation is a caller-owned lifecycle loop: one invocation periodically
// advances generations until ctx is canceled. It starts no goroutine itself,
// so disabled caches and unstarted/canceled lifecycles have no worker to leak.
func (c *dedupCache) RunRotation(ctx context.Context) error {
	if ctx == nil {
		return errors.New("mqttingress: nil dedup rotation context")
	}
	if !c.Enabled() || ctx.Err() != nil {
		return nil
	}
	timer := time.NewTimer(DedupTTL)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			c.Rotate()
			timer.Reset(DedupTTL)
		}
	}
}

func (c *dedupCache) Enabled() bool { return c != nil && c.enabled }

func (c *dedupCache) Snapshot() DedupCacheSnapshot {
	if c == nil {
		return DedupCacheSnapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := DedupCacheSnapshot{Enabled: c.enabled, TTLSeconds: int64(DedupTTL.Seconds()), MaxEntries: c.maxEntries, MaxBytes: c.maxBytes,
		LookupHit: c.lookups.hit.Load(), LookupMiss: c.lookups.miss.Load(), LookupExpired: c.lookups.expired.Load(), LookupMetadataMismatch: c.lookups.mismatch.Load(), LookupDisabledBypass: c.lookups.disabledBypass.Load(),
		PublicationStored: c.publications.stored.Load(), PublicationExisting: c.publications.existing.Load(), PublicationMetadataMismatch: c.publications.mismatch.Load(), PublicationFull: c.publications.full.Load(), PublicationInvalidSkip: c.publications.invalid.Load(), PublicationDisabledBypass: c.publications.disabledBypass.Load(), Rotations: c.rotations.Load()}
	s.Entries = c.entries
	s.LogicalBytes = c.logicalBytes
	s.CurrentEntries = c.current.count
	s.PreviousEntries = c.previous.count
	s.CapacityBypass = s.PublicationFull
	s.DatabaseCallsAvoided = s.LookupHit
	return s
}

func validDedupProof(key dedupKey, proof dedupMetadata, now time.Time) bool {
	if key.gatewayID == "" || key.sensorID == "" || key.bootID == "" || key.firstSequence < 0 || proof.messageID == "" || proof.payloadHash == "" || proof.sourceTopic == "" || proof.lastSequence < key.firstSequence || proof.sampleCount <= 0 || proof.confirmedCommitAt.IsZero() || proof.originalTimestamp.IsZero() {
		return false
	}
	if proof.lastSequence-key.firstSequence != int64(proof.sampleCount-1) || !proof.expiresAt.Equal(proof.confirmedCommitAt.Add(DedupTTL)) {
		return false
	}
	if proof.confirmedCommitAt.After(now) || !proof.confirmedCommitAt.Before(proof.expiresAt) {
		return false
	}
	return proof.logicalBytes(key) >= 0
}
