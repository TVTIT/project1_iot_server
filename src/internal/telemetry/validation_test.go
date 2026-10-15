package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"
)

func rangeTestBatch() BatchDTO {
	return BatchDTO{
		FirstSequence:    12,
		SampleCount:      2,
		MeasuredAt:       "2026-08-21T10:15:00.000000Z",
		SampleIntervalUS: 10_000,
	}
}

func rangeTestLimits() Limits {
	return Limits{MaxBatchDuration: time.Second, MaxFutureSkew: time.Second}
}

func rangeTestReceivedAt() time.Time {
	return time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
}

func TestValidateRangeSequenceAndIntervals(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*BatchDTO, *Limits)
		wantCode  ReasonCode
		wantLast  int64
		wantEvery time.Duration
	}{
		{name: "last sequence at max", mutate: func(b *BatchDTO, _ *Limits) { b.FirstSequence = math.MaxInt64; b.SampleCount = 1 }, wantLast: math.MaxInt64, wantEvery: 10 * time.Millisecond},
		{name: "sequence overflow", mutate: func(b *BatchDTO, _ *Limits) { b.FirstSequence = math.MaxInt64; b.SampleCount = 2 }, wantCode: ReasonInvalidSequenceRange},
		{name: "negative sequence", mutate: func(b *BatchDTO, _ *Limits) { b.FirstSequence = -1 }, wantCode: ReasonInvalidSequenceRange},
		{name: "single sample still checks positive interval", mutate: func(b *BatchDTO, _ *Limits) { b.SampleCount = 1 }, wantLast: 12, wantEvery: 10 * time.Millisecond},
		{name: "zero interval", mutate: func(b *BatchDTO, _ *Limits) { b.SampleIntervalUS = 0 }, wantCode: ReasonInvalidSequenceRange},
		{name: "negative interval", mutate: func(b *BatchDTO, _ *Limits) { b.SampleIntervalUS = -1 }, wantCode: ReasonInvalidSequenceRange},
		{name: "interval conversion overflow", mutate: func(b *BatchDTO, _ *Limits) { b.SampleIntervalUS = math.MaxInt64/1000 + 1 }, wantCode: ReasonInvalidSequenceRange},
		{name: "count zero guard", mutate: func(b *BatchDTO, _ *Limits) { b.SampleCount = 0 }, wantCode: ReasonInvalidSequenceRange},
		{name: "duration multiplication overflow", mutate: func(b *BatchDTO, _ *Limits) { b.SampleCount = 3; b.SampleIntervalUS = math.MaxInt64 / 1000 }, wantCode: ReasonInvalidSequenceRange},
		{name: "span exactly max", mutate: func(b *BatchDTO, l *Limits) {
			b.SampleCount = 2
			b.SampleIntervalUS = 1000
			l.MaxBatchDuration = time.Millisecond
		}, wantLast: 13, wantEvery: time.Millisecond},
		{name: "span over max", mutate: func(b *BatchDTO, l *Limits) {
			b.SampleCount = 2
			b.SampleIntervalUS = 1001
			l.MaxBatchDuration = time.Millisecond
		}, wantCode: ReasonInvalidSequenceRange},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, limits := rangeTestBatch(), rangeTestLimits()
			tt.mutate(&batch, &limits)
			got, err := validateRange(batch, limits, rangeTestReceivedAt())
			if tt.wantCode != "" {
				requireRangeError(t, got, err, tt.wantCode)
				return
			}
			if err != nil {
				t.Fatalf("validateRange() error = %v", err)
			}
			if got.LastSequence != tt.wantLast || got.Interval != tt.wantEvery {
				t.Fatalf("range = %#v, want last sequence %d and interval %s", got, tt.wantLast, tt.wantEvery)
			}
		})
	}
}

func TestValidateRangeTimestampParsing(t *testing.T) {
	validOffset := rangeTestBatch()
	validOffset.MeasuredAt = "2026-08-21T12:15:00.123456+02:00"
	got, err := validateRange(validOffset, rangeTestLimits(), rangeTestReceivedAt())
	if err != nil || !got.FirstMeasuredAt.Equal(time.Date(2026, 8, 21, 10, 15, 0, 123456000, time.UTC)) {
		t.Fatalf("offset timestamp = %s, %v", got.FirstMeasuredAt, err)
	}

	for _, value := range []string{
		"2026-08-21T10:15:00",          // zone required
		"2026-02-30T10:15:00Z",         // invalid date
		"2026-08-21T10:15:00.1234560Z", // seven digits, including trailing zero
		"2026-08-21T10:15:00.0000001Z", // seven digits
		"0000-08-21T10:15:00Z",         // year zero
		"0001-01-01T00:00:00+00:01",    // UTC year underflow
		"9999-12-31T23:59:59-00:01",    // UTC year overflow
		"2026-08-21T1:15:00Z",          // Go parser accepts one-digit hour
		"2026-08-21T10:15:00,1Z",       // Go parser accepts comma fraction
		"2026-08-21T10:15:00+24:00",    // Go parser accepts 24:00 offset
		"2026-08-21T10:15:00+00:60",    // invalid offset minute
		"2026-08-21T24:00:00Z",         // non-RFC wall clock hour
	} {
		t.Run(value, func(t *testing.T) {
			batch := rangeTestBatch()
			batch.MeasuredAt = value
			gotRange, err := validateRange(batch, rangeTestLimits(), rangeTestReceivedAt())
			requireRangeError(t, gotRange, err, ReasonInvalidTimestamp)
		})
	}
	batch := rangeTestBatch()
	batch.MeasuredAt = "9999-12-31T23:59:59.995000Z"
	if gotRange, err := validateRange(batch, rangeTestLimits(), rangeTestReceivedAt()); err == nil || gotRange != (validatedRange{}) || reasonCode(err) != ReasonInvalidTimestamp {
		t.Fatalf("timestamp addition beyond year 9999 = %#v, %v; want invalid_timestamp", gotRange, err)
	}
}

func TestValidateRangeFutureSkewAndOldBatches(t *testing.T) {
	received := rangeTestReceivedAt()
	limits := rangeTestLimits()
	limits.MaxFutureSkew = time.Second

	for _, tt := range []struct {
		name     string
		first    string
		count    int32
		interval int64
		wantErr  ReasonCode
	}{
		{name: "last exactly threshold", first: "2026-08-21T10:15:00.990000Z", count: 2, interval: 10_000},
		{name: "last one microsecond beyond threshold", first: "2026-08-21T10:15:00.990001Z", count: 2, interval: 10_000, wantErr: ReasonTimestampInFuture},
		{name: "first before threshold but last beyond", first: "2026-08-21T10:15:00.995000Z", count: 2, interval: 10_000, wantErr: ReasonTimestampInFuture},
		{name: "old batch accepted", first: "2001-01-01T00:00:00Z", count: 2, interval: 10_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			batch := rangeTestBatch()
			batch.MeasuredAt, batch.SampleCount, batch.SampleIntervalUS = tt.first, tt.count, tt.interval
			got, err := validateRange(batch, limits, received)
			if tt.wantErr != "" {
				requireRangeError(t, got, err, tt.wantErr)
			} else if err != nil {
				t.Fatalf("validateRange() error = %v", err)
			}
		})
	}
}

func requireRangeError(t *testing.T, got validatedRange, err error, want ReasonCode) {
	t.Helper()
	if err == nil || got != (validatedRange{}) {
		t.Fatalf("validateRange() = %#v, %v; want zero range and %s", got, err, want)
	}
	var reason *ReasonError
	if !errors.As(err, &reason) || reason.Code != want {
		t.Fatalf("validateRange() error = %v, want reason %s", err, want)
	}
}
