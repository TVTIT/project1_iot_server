package mqttingress

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShaperInitialBurstFractionalRefillAndCap(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(100, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 2, Burst: 3, MaxGateways: 4, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		if got := shaper.Allow("gateway-a"); got != ShaperPass {
			t.Fatalf("initial token %d: got %v", n, got)
		}
	}
	if got := shaper.Allow("gateway-a"); got != ShaperRateLimited {
		t.Fatalf("depleted outcome = %v", got)
	}
	clock.Advance(250 * time.Millisecond)
	if got := shaper.Allow("gateway-a"); got != ShaperRateLimited {
		t.Fatalf("fractional refill minted early token: %v", got)
	}
	clock.Advance(250 * time.Millisecond)
	if got := shaper.Allow("gateway-a"); got != ShaperPass {
		t.Fatalf("one second fractional refill: %v", got)
	}
	clock.Advance(10 * time.Second)
	for n := 0; n < 3; n++ {
		if got := shaper.Allow("gateway-a"); got != ShaperPass {
			t.Fatalf("refill cap token %d: %v", n, got)
		}
	}
	if got := shaper.Allow("gateway-a"); got != ShaperRateLimited {
		t.Fatalf("refill exceeded burst: %v", got)
	}
}

func TestShaperCleanupIndexWraparound(t *testing.T) {
	for _, value := range []uint64{math.MaxInt32 - 1, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint64 - 1, math.MaxUint64} {
		if got, want := shaperCleanupShardIndex(value), int(value%shaperShardCount); got != want {
			t.Errorf("cleanup shard for counter %d = %d, want %d", value, got, want)
		}
	}
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 1, IdleRetention: time.Minute}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	shaper.cleanup.Store(math.MaxUint64)
	shaper.cleanupOne(time.Now()) // Add wraps to zero; shard selection must remain nonnegative.
	if got := shaper.cleanup.Load(); got != 0 {
		t.Fatalf("cleanup counter after uint64 overflow = %d, want 0", got)
	}
}

func TestShaperSlotStorageTracksActiveEntriesAcrossShards(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 1, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	for shardIndex := 0; shardIndex < shaperShardCount; shardIndex++ {
		id := gatewayIDsForShard(shardIndex, 1)[0]
		if got := shaper.Allow(id); got != ShaperPass {
			t.Fatalf("create Gateway in shard %d: %v", shardIndex, got)
		}
		clock.Advance(2 * time.Minute)
		shaper.cleanupShard(shardIndex, clock.Now())
		if got, _ := shaper.EntriesAndCap(); got != 0 {
			t.Fatalf("evict prior entry in shard %d: entries=%d", shardIndex, got)
		}
	}
	lastID := gatewayIDsForShard(shaperShardCount-1, 1)[0]
	if got := shaper.Allow(lastID); got != ShaperPass {
		t.Fatalf("recreate final Gateway: %v", got)
	}
	active, slotCount := 0, 0
	for index := range shaper.shards {
		shard := &shaper.shards[index]
		shard.mu.Lock()
		active += len(shard.entries)
		slotCount += len(shard.slots)
		if len(shard.slots) != len(shard.entries) {
			t.Errorf("shard %d retains %d slots for %d active entries", index, len(shard.slots), len(shard.entries))
		}
		shard.mu.Unlock()
	}
	if active != 1 || slotCount != active {
		t.Fatalf("active entries=%d, retained slots=%d; want both 1", active, slotCount)
	}
}

func TestShaperGatewayIndependenceAndDisabled(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 2, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if shaper.Allow("gateway-a") != ShaperPass || shaper.Allow("gateway-a") != ShaperRateLimited {
		t.Fatal("same Gateway did not share bucket")
	}
	if shaper.Allow("gateway-b") != ShaperPass {
		t.Fatal("Gateway buckets were not independent")
	}
	disabled, err := NewShaper(ShaperOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if disabled.Allow("gateway-a") != ShaperPass {
			t.Fatal("disabled shaper rejected attempt")
		}
	}
	entries, cap := disabled.EntriesAndCap()
	if entries != 0 || cap != 0 {
		t.Fatalf("disabled shaper allocated state: entries=%d cap=%d", entries, cap)
	}
}

func TestShaperRejectsInvalidEnabledBounds(t *testing.T) {
	base := ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 1, IdleRetention: time.Minute}
	cases := []ShaperOptions{base, base, base, base}
	cases[0].RatePerSecond = 0
	cases[1].Burst = 100001
	cases[2].MaxGateways = 100001
	cases[3].IdleRetention = 59 * time.Second
	for n, opts := range cases {
		if _, err := NewShaper(opts); err == nil {
			t.Errorf("case %d accepted invalid options: %+v", n, opts)
		}
	}
}

func TestShaperExactGlobalEntryCapAcrossShards(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 3, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	ids := distinctGatewayIDs(4)
	for n := 0; n < 3; n++ {
		if got := shaper.Allow(ids[n]); got != ShaperPass {
			t.Fatalf("entry %d: %v", n, got)
		}
	}
	if got := shaper.Allow(ids[3]); got != ShaperStateCapacity {
		t.Fatalf("fourth globally capped entry: %v", got)
	}
	if got, cap := shaper.EntriesAndCap(); got != 3 || cap != 3 {
		t.Fatalf("entries/cap = %d/%d", got, cap)
	}
}

func TestShaperConcurrentGlobalCapAcrossShards(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	const capacity = 7
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: capacity, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	ids := distinctGatewayIDs(64)
	var passed atomic.Int32
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if shaper.Allow(id) == ShaperPass {
				passed.Add(1)
			}
		}(id)
	}
	wg.Wait()
	if got := passed.Load(); got != capacity {
		t.Fatalf("concurrent distinct Gateway admissions passed %d, want %d", got, capacity)
	}
	if got, cap := shaper.EntriesAndCap(); got != capacity || cap != capacity {
		t.Fatalf("concurrent entries/cap = %d/%d", got, cap)
	}
}

func TestShaperCleanupIsBoundedAndMakesDeterministicProgress(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 2, MaxGateways: 20, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	ids := gatewayIDsForShard(0, 10)
	for _, id := range ids {
		if got := shaper.Allow(id); got != ShaperPass {
			t.Fatalf("create %q: %v", id, got)
		}
		if got := shaper.Allow(id); got != ShaperPass {
			t.Fatalf("fill %q: %v", id, got)
		}
	}
	clock.Advance(2 * time.Minute)
	shaper.cleanupShard(0, clock.Now())
	if got, _ := shaper.EntriesAndCap(); got != 2 {
		t.Fatalf("one cleanup inspected more than bounded batch: entries=%d, want 2", got)
	}
	for n := 0; n < 2; n++ {
		shaper.cleanupShard(0, clock.Now())
	}
	if got, _ := shaper.EntriesAndCap(); got != 0 {
		t.Fatalf("bounded cleanup did not progress across slots: entries=%d", got)
	}
}

func TestShaperDoesNotEvictUntilRecreationWouldBeFull(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 100000, MaxGateways: 1, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 100000; n++ {
		if got := shaper.Allow("large-bucket"); got != ShaperPass {
			t.Fatalf("deplete large bucket at %d: %v", n, got)
		}
	}
	clock.Advance(time.Hour)
	shaper.cleanupShard(hashGateway("large-bucket"), clock.Now())
	if got, _ := shaper.EntriesAndCap(); got != 1 {
		t.Fatalf("evicted before bucket could refill: entries=%d", got)
	}
	if got := shaper.Allow("new-id"); got != ShaperStateCapacity {
		t.Fatalf("capacity outcome before full refill: %v", got)
	}
	clock.Advance(100000 * time.Second)
	shaper.cleanupShard(hashGateway("large-bucket"), clock.Now())
	if got, _ := shaper.EntriesAndCap(); got != 0 {
		t.Fatalf("full idle bucket not evicted safely: entries=%d", got)
	}
	if got := shaper.Allow("new-id"); got != ShaperPass {
		t.Fatalf("recreate after safe refill: %v", got)
	}
}

func TestShaperBackwardsClockAndActiveChurn(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(100, 0))
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 1, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if shaper.Allow("active") != ShaperPass {
		t.Fatal("initial token rejected")
	}
	clock.Advance(-time.Hour)
	if shaper.Allow("active") != ShaperRateLimited {
		t.Fatal("backwards clock created a token")
	}
	if shaper.Allow("churn") != ShaperStateCapacity {
		t.Fatal("active entry evicted for churn")
	}
	clock.Advance(time.Hour + time.Second)
	if shaper.Allow("active") != ShaperPass {
		t.Fatal("token did not refill from monotonic baseline")
	}
	if got, _ := shaper.EntriesAndCap(); got != 1 {
		t.Fatalf("active entry count = %d", got)
	}
}

func TestShaperConcurrentBurstIsExact(t *testing.T) {
	clock := newFakeShaperClock(time.Unix(1, 0))
	const burst = 64
	shaper, err := newShaper(ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: burst, MaxGateways: 2, IdleRetention: time.Minute}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	var passed int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < burst*4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if shaper.Allow("same-gateway") == ShaperPass {
				mu.Lock()
				passed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if passed != burst {
		t.Fatalf("passed %d attempts, want exactly burst %d", passed, burst)
	}
}

type fakeShaperClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeShaperClock(now time.Time) *fakeShaperClock { return &fakeShaperClock{now: now} }
func (c *fakeShaperClock) Now() time.Time               { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeShaperClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func distinctGatewayIDs(n int) []string {
	ids := make([]string, 0, n)
	seen := make(map[int]bool)
	for candidate := 0; len(ids) < n; candidate++ {
		id := fmt.Sprintf("gateway-%d", candidate)
		shard := hashGateway(id)
		if !seen[shard] {
			seen[shard] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func gatewayIDsForShard(shard, n int) []string {
	ids := make([]string, 0, n)
	for candidate := 0; len(ids) < n; candidate++ {
		id := fmt.Sprintf("shard-%d-%d", shard, candidate)
		if hashGateway(id) == shard {
			ids = append(ids, id)
		}
	}
	return ids
}
