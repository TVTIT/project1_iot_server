package digitaltwin

import (
	"errors"
	"math"
	"time"
)

// Default retry and publish timeout parameters for Stage 6 control dispatch.
const (
	DefaultRetryBaseDelay     = time.Second
	DefaultRetryCapDelay      = 8 * time.Second
	DefaultRetryMaxAttempts   = 5
	DefaultPublishTimeout     = 3 * time.Second
	DefaultPublishSafetyLease = time.Second
)

// RetryPolicy defines deterministic bounded backoff, attempt lease and jitter.
type RetryPolicy struct {
	BaseDelay      time.Duration
	CapDelay       time.Duration
	MaxAttempts    int
	PublishTimeout time.Duration
	SafetyMargin   time.Duration
}

// DefaultRetryPolicy supplies standard conservative defaults for Stage 6.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseDelay:      DefaultRetryBaseDelay,
		CapDelay:       DefaultRetryCapDelay,
		MaxAttempts:    DefaultRetryMaxAttempts,
		PublishTimeout: DefaultPublishTimeout,
		SafetyMargin:   DefaultPublishSafetyLease,
	}
}

// Validate checks policy boundaries.
func (p RetryPolicy) Validate() error {
	if p.BaseDelay < 10*time.Millisecond || p.BaseDelay > time.Hour {
		return errors.New("invalid retry base delay")
	}
	if p.CapDelay < p.BaseDelay || p.CapDelay > 24*time.Hour {
		return errors.New("invalid retry cap delay")
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > 100 {
		return errors.New("invalid retry max attempts")
	}
	if p.PublishTimeout < 10*time.Millisecond || p.PublishTimeout > time.Hour {
		return errors.New("invalid publish timeout")
	}
	if p.SafetyMargin < 0 || p.SafetyMargin > time.Hour {
		return errors.New("invalid safety margin")
	}
	return nil
}

// ComputeBackoff returns the deterministic base backoff (before jitter) for attempt n >= 1.
// Saturates at CapDelay and avoids integer overflow.
func (p RetryPolicy) ComputeBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return p.BaseDelay
	}
	shift := attempt - 1
	if shift > 30 {
		return p.CapDelay
	}
	multiplier := int64(1) << shift
	if multiplier > math.MaxInt64/int64(p.BaseDelay) {
		return p.CapDelay
	}
	delay := time.Duration(multiplier) * p.BaseDelay
	if delay > p.CapDelay {
		return p.CapDelay
	}
	return delay
}

// JitterDelay returns backoff * (0.5 + 0.5 * factor), where factor is in [0.0, 1.0].
// Always positive, range [50%, 100%]. Uses integer quotient and remainder scaling
// to prevent multiplication overflow across all positive durations.
// Non-finite float factors (NaN, Inf) are clamped to 0.0.
func JitterDelay(backoff time.Duration, factor float64) time.Duration {
	if math.IsNaN(factor) || math.IsInf(factor, 0) || factor < 0 {
		factor = 0
	} else if factor > 1 {
		factor = 1
	}
	bps := int64(5000 + factor*5000)
	nanos := backoff.Nanoseconds()
	if nanos <= 0 {
		return 0
	}
	jittered := (nanos/10000)*bps + ((nanos%10000)*bps)/10000
	return time.Duration(jittered)
}

// NextAttemptScheduleWithReason computes next_attempt_at, whether scheduling is exhausted,
// and the explicit stop reason if attempt cap was reached (distinct from normal TTL expiry wait).
func (p RetryPolicy) NextAttemptScheduleWithReason(now time.Time, attempt int, jitterFactor float64, expiresAt time.Time) (nextAttemptAt time.Time, exhausted bool, stopReason string) {
	backoff := p.ComputeBackoff(attempt)
	delay := JitterDelay(backoff, jitterFactor)
	leaseDuration := p.PublishTimeout + p.SafetyMargin
	leaseDeadline := now.Add(leaseDuration)

	target := now.Add(delay)
	if leaseDeadline.After(target) {
		target = leaseDeadline
	}

	if attempt >= p.MaxAttempts {
		return expiresAt, true, "retry_exhausted"
	}
	if !target.Before(expiresAt) || !leaseDeadline.Before(expiresAt) {
		// Remaining TTL window does not fit another lease; no future grant fits,
		// but attempt cap was not reached, so wait for normal TTL expiry (no retry_exhausted reason).
		return expiresAt, true, ""
	}
	return target, false, ""
}

// NextAttemptSchedule computes next_attempt_at and whether scheduling is exhausted.
func (p RetryPolicy) NextAttemptSchedule(now time.Time, attempt int, jitterFactor float64, expiresAt time.Time) (nextAttemptAt time.Time, exhausted bool) {
	nextAt, exh, _ := p.NextAttemptScheduleWithReason(now, attempt, jitterFactor, expiresAt)
	return nextAt, exh
}
