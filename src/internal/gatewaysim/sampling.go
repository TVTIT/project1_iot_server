package gatewaysim

import (
	"context"
	"errors"
	"fmt"
	"time"

	"iot-platform/internal/digitaltwin"
)

type samplingState struct {
	next, first, flushAt time.Time
	count                int32
	uncertain            bool
}

type intervalUpdate struct {
	ctx      context.Context
	interval int64
	done     chan error
}

// SetSampleInterval changes sampling at a serialized, bounded batch boundary.
func (s *Scheduler) SetSampleInterval(ctx context.Context, interval int64) error {
	if ctx == nil || interval < digitaltwin.MinSampleIntervalUS || interval > digitaltwin.MaxSampleIntervalUS || s.sampling == nil {
		return digitaltwin.ErrInvalidInput
	}
	// Includes the wait for the loop and SQLite flush. A late/ambiguous caller
	// must retain its prepared receipt, not manufacture a successful result.
	ctx, cancel := context.WithTimeout(ctx, s.out.opTimeout)
	defer cancel()
	s.startMu.Lock()
	if !s.started {
		defer s.startMu.Unlock()
		return s.changeInterval(ctx, interval)
	}
	s.startMu.Unlock()
	u := intervalUpdate{ctx: ctx, interval: interval, done: make(chan error, 1)}
	select {
	case s.intervalUpdates <- u:
	case <-s.done:
		return ErrTransportClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-u.done:
		return err
	case <-s.done:
		return ErrTransportClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ActiveSampleInterval reads back the active scheduler setting, not desired state.
func (s *Scheduler) ActiveSampleInterval() int64 { return s.activeInterval.Load() }

func (s *Scheduler) resetSampling(now time.Time) {
	p := s.sampling
	p.next = now.Add(time.Duration(s.ActiveSampleInterval()) * time.Microsecond)
	p.flushAt = time.Time{}
	if s.ActiveSampleInterval() <= 1000000 {
		p.flushAt = now.Add(time.Second)
	}
}

func (s *Scheduler) changeInterval(ctx context.Context, interval int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.collectDueSamples(ctx); err != nil {
		return err
	}
	if err := s.flushSamples(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.activeInterval.Store(interval)
	s.resetSampling(s.clock.Now())
	return nil
}

// collectDueSamples advances actual due timer events, not a fixed-size synthetic
// window ending before process startup. One pass has a hard event budget; a
// process stalled beyond that budget stops explicitly instead of inventing an
// unbounded backlog. Network readiness/ACK waits never gate sampling or apply.
func (s *Scheduler) collectDueSamples(ctx context.Context) error {
	p := s.sampling
	if s.genDone {
		return nil
	}
	now := s.clock.Now()
	if p.next.IsZero() {
		s.resetSampling(now)
	}
	for events := 0; events < 1000; events++ {
		if !p.flushAt.IsZero() && !p.flushAt.After(now) && p.next.After(p.flushAt) {
			if err := s.flushSamples(ctx); err != nil {
				return err
			}
			p.flushAt = p.flushAt.Add(time.Second)
			if s.genDone {
				return nil
			}
			continue
		}
		if p.next.After(now) {
			return nil
		}
		if p.count == 0 {
			p.first = p.next
		}
		p.count++
		p.next = p.next.Add(time.Duration(s.ActiveSampleInterval()) * time.Microsecond)
		if s.ActiveSampleInterval() > 1000000 || p.count >= int32(ServerLimits().MaxSamples) {
			if err := s.flushSamples(ctx); err != nil {
				return err
			}
			if s.genDone {
				return nil
			}
		}
	}
	return errors.New("gatewaysim: sampling timer backlog exceeded bounded pass")
}

func (s *Scheduler) flushSamples(ctx context.Context) error {
	p := s.sampling
	if p != nil && p.uncertain {
		return errors.Join(ErrSchedulerCommitUnknown, ErrOutboxCommitUnknown)
	}
	if p == nil || p.count == 0 {
		return nil
	}
	id, err := s.newUUID()
	if err != nil {
		return err
	}
	item, err := s.out.Enqueue(ctx, EnqueueRequest{BootID: s.bootID, MessageID: id, MeasuredAt: p.first, ReceivedAt: s.clock.Now(), SampleCount: p.count, SampleIntervalUS: s.ActiveSampleInterval()})
	if err != nil {
		if errors.Is(err, ErrOutboxCommitUnknown) {
			p.uncertain = true
			return errors.Join(ErrSchedulerCommitUnknown, err)
		}
		if errors.Is(err, ErrOutboxFull) {
			s.generationPaused.Store(true)
		}
		return fmt.Errorf("%w: %w", ErrSchedulerEnqueue, err)
	}
	s.uniqueEnqueued.Add(1)
	s.uniquePayloadBytes.Add(int64(len(item.Payload)))
	s.uniqueSamples.Add(int64(p.count))
	p.count = 0
	p.first = time.Time{}
	if s.uniqueEnqueued.Load() == 1 && s.latency != nil {
		s.latency.RecordGenerationStart()
	}
	if s.finiteTarget > 0 && s.uniqueEnqueued.Load() >= int64(s.finiteTarget) {
		s.genDone = true
		s.completionDeadline = s.clock.Now().Add(s.completionBudget)
		if s.latency != nil {
			s.latency.RecordGenerationEnd()
		}
	}
	return nil
}
