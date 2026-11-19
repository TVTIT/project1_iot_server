package mqttingress

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	shaperShardCount        = 64
	shaperCleanupBatch      = 8
	shaperRefillDenominator = int64(time.Second)
)

// ShaperOptions bounds per-Gateway delivery-attempt shaping. When disabled,
// all limits are ignored and the shaper allocates no bucket state.
type ShaperOptions struct {
	Enabled       bool
	RatePerSecond int
	Burst         int
	MaxGateways   int
	IdleRetention time.Duration
}

// ShaperOutcome is the fixed result set for one delivery attempt.
type ShaperOutcome uint8

const (
	// ShaperPass means the Gateway had an attempt token.
	ShaperPass ShaperOutcome = iota
	// ShaperRateLimited means the Gateway bucket had no attempt token.
	ShaperRateLimited
	// ShaperStateCapacity means the bounded bucket store could not allocate an entry.
	ShaperStateCapacity
)

type gatewayBucket struct {
	id           string
	tokens       int
	remainder    int64
	lastRefill   time.Time
	lastActivity time.Time
	slot         int
}

type shaperShard struct {
	mu      sync.Mutex
	entries map[string]*gatewayBucket
	// slots has no holes, so its live length tracks entries in this shard.
	// Go may retain a slice backing array or map buckets after churn; their
	// allocator overhead is not included in MaxGateways' logical entry bound.
	slots  []*gatewayBucket
	cursor int
}

// Shaper is a bounded per-Gateway token bucket. Its clock seam is private;
// NewShaper always uses time.Now, whose values preserve Go's monotonic clock.
type Shaper struct {
	options ShaperOptions
	now     func() time.Time
	shards  [shaperShardCount]shaperShard
	entries atomic.Int64
	cleanup atomic.Uint64
}

// NewShaper validates and creates a shaper. A zero-value disabled option is
// intentionally valid for existing callers that do not configure P4 shaping.
func NewShaper(opts ShaperOptions) (*Shaper, error) {
	return newShaper(opts, time.Now)
}

func newShaper(opts ShaperOptions, now func() time.Time) (*Shaper, error) {
	if now == nil {
		return nil, errors.New("mqttingress: shaper clock is required")
	}
	if opts.Enabled {
		if opts.RatePerSecond < 1 || opts.RatePerSecond > 10000 {
			return nil, errors.New("mqttingress: shaper rate must be in [1, 10000]")
		}
		if opts.Burst < 1 || opts.Burst > 100000 {
			return nil, errors.New("mqttingress: shaper burst must be in [1, 100000]")
		}
		if opts.MaxGateways < 1 || opts.MaxGateways > 100000 {
			return nil, errors.New("mqttingress: shaper gateway capacity must be in [1, 100000]")
		}
		if opts.IdleRetention < time.Minute || opts.IdleRetention > 24*time.Hour {
			return nil, errors.New("mqttingress: shaper idle retention must be in [1m, 24h]")
		}
	}
	s := &Shaper{options: opts, now: now}
	if opts.Enabled {
		for n := range s.shards {
			s.shards[n].entries = make(map[string]*gatewayBucket)
		}
	}
	return s, nil
}

// Allow accounts one delivery attempt for a Gateway ID already validated by
// the caller. Rejected attempts are not refunded by later ingress stages.
func (s *Shaper) Allow(gatewayID string) ShaperOutcome {
	if s == nil || !s.options.Enabled {
		return ShaperPass
	}
	now := s.now()
	shardIndex := hashGateway(gatewayID)
	shard := &s.shards[shardIndex]
	shard.mu.Lock()
	if bucket := shard.entries[gatewayID]; bucket != nil {
		outcome := s.consume(bucket, now)
		shard.mu.Unlock()
		s.cleanupOne(now)
		return outcome
	}
	shard.mu.Unlock()

	// A bounded cleanup attempt may free a globally counted slot. It never
	// scans other shards or searches for an arbitrary inactive entry.
	s.cleanupOne(now)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if bucket := shard.entries[gatewayID]; bucket != nil {
		return s.consume(bucket, now)
	}
	if !s.reserveEntry() {
		return ShaperStateCapacity
	}
	bucket := &gatewayBucket{id: gatewayID, tokens: s.options.Burst - 1, lastRefill: now, lastActivity: now}
	shard.add(bucket)
	return ShaperPass
}

// EntriesAndCap returns the current globally-accounted bucket entries and the
// configured global capacity. Disabled shapers report (0, 0).
func (s *Shaper) EntriesAndCap() (entries, cap int) {
	if s == nil || !s.options.Enabled {
		return 0, 0
	}
	return int(s.entries.Load()), s.options.MaxGateways
}

func (s *Shaper) consume(bucket *gatewayBucket, now time.Time) ShaperOutcome {
	s.refill(bucket, now)
	bucket.lastActivity = now
	if now.Before(bucket.lastRefill) {
		bucket.lastActivity = bucket.lastRefill
	}
	if bucket.tokens == 0 {
		return ShaperRateLimited
	}
	bucket.tokens--
	return ShaperPass
}

func (s *Shaper) refill(bucket *gatewayBucket, now time.Time) {
	if now.Before(bucket.lastRefill) {
		// A backwards injected clock cannot mint tokens or move the refill
		// baseline backwards; production time.Now values are monotonic.
		now = bucket.lastRefill
	}
	elapsed := now.Sub(bucket.lastRefill)
	if elapsed > 0 {
		missing := s.options.Burst - bucket.tokens
		if missing > 0 {
			// Max refill duration is bounded by 100000 seconds. Check for a
			// complete refill before multiplying elapsed nanoseconds by rate.
			fullAfter := time.Duration((int64(missing)*shaperRefillDenominator + int64(s.options.RatePerSecond) - 1) / int64(s.options.RatePerSecond))
			if elapsed >= fullAfter {
				bucket.tokens = s.options.Burst
				bucket.remainder = 0
			} else {
				units := elapsed.Nanoseconds()*int64(s.options.RatePerSecond) + bucket.remainder
				added := int(units / shaperRefillDenominator)
				bucket.remainder = units % shaperRefillDenominator
				bucket.tokens += added
				if bucket.tokens >= s.options.Burst {
					bucket.tokens = s.options.Burst
					bucket.remainder = 0
				}
			}
		}
		bucket.lastRefill = now
	}
}

func (s *Shaper) reserveEntry() bool {
	for {
		current := s.entries.Load()
		if current >= int64(s.options.MaxGateways) {
			return false
		}
		if s.entries.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *Shaper) cleanupOne(now time.Time) {
	index := shaperCleanupShardIndex(s.cleanup.Add(1) - 1)
	s.cleanupShard(index, now)
}

func shaperCleanupShardIndex(counter uint64) int {
	return int(counter % uint64(shaperShardCount))
}

func (s *Shaper) cleanupShard(index int, now time.Time) {
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if len(shard.slots) == 0 {
		return
	}
	for inspected := 0; inspected < shaperCleanupBatch && len(shard.slots) > 0; inspected++ {
		if shard.cursor >= len(shard.slots) {
			shard.cursor = 0
		}
		slot := shard.cursor
		bucket := shard.slots[slot]
		s.refill(bucket, now)
		if now.Before(bucket.lastActivity) || now.Sub(bucket.lastActivity) < s.options.IdleRetention {
			shard.cursor = (slot + 1) % len(shard.slots)
			continue
		}
		if bucket.tokens != s.options.Burst {
			shard.cursor = (slot + 1) % len(shard.slots)
			continue
		}
		delete(shard.entries, bucket.id)
		last := len(shard.slots) - 1
		if slot != last {
			moved := shard.slots[last]
			shard.slots[slot] = moved
			moved.slot = slot
		}
		shard.slots[last] = nil
		shard.slots = shard.slots[:last]
		bucket.slot = -1
		if len(shard.slots) == 0 || shard.cursor >= len(shard.slots) {
			shard.cursor = 0
		} else {
			// Keep the cursor on this slot so the swapped-in entry is inspected
			// next rather than skipped until a future cycle.
			shard.cursor = slot
		}
		s.entries.Add(-1)
	}
}

func (shard *shaperShard) add(bucket *gatewayBucket) {
	bucket.slot = len(shard.slots)
	shard.slots = append(shard.slots, bucket)
	shard.entries[bucket.id] = bucket
}

func hashGateway(id string) int {
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	h := offset64
	for n := 0; n < len(id); n++ {
		h ^= uint64(id[n])
		h *= prime64
	}
	return int(h % shaperShardCount)
}
