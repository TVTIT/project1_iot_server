package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestLimitsValidate(t *testing.T) {
	valid := Limits{
		MaxPayloadBytes:  65536,
		MaxSamples:       1000,
		MaxBatchDuration: 60 * time.Second,
		MaxFutureSkew:    2 * time.Minute,
	}

	tests := []struct {
		name   string
		mutate func(*Limits)
		field  string
	}{
		{name: "valid profile"},
		{name: "zero bytes", mutate: func(l *Limits) { l.MaxPayloadBytes = 0 }, field: "max_payload_bytes"},
		{name: "negative bytes", mutate: func(l *Limits) { l.MaxPayloadBytes = -1 }, field: "max_payload_bytes"},
		{name: "zero samples", mutate: func(l *Limits) { l.MaxSamples = 0 }, field: "max_samples"},
		{name: "negative samples", mutate: func(l *Limits) { l.MaxSamples = -1 }, field: "max_samples"},
		{name: "zero duration", mutate: func(l *Limits) { l.MaxBatchDuration = 0 }, field: "max_batch_duration"},
		{name: "negative duration", mutate: func(l *Limits) { l.MaxBatchDuration = -time.Second }, field: "max_batch_duration"},
		{name: "negative skew", mutate: func(l *Limits) { l.MaxFutureSkew = -time.Nanosecond }, field: "max_future_skew"},
		{name: "zero skew is valid", mutate: func(l *Limits) { l.MaxFutureSkew = 0 }},
	}
	maxSamplesBeyondInt32 := int64(math.MaxInt32) + 1
	if maxSamplesBeyondInt32 <= int64(^uint(0)>>1) {
		tests = append(tests, struct {
			name   string
			mutate func(*Limits)
			field  string
		}{name: "samples exceed int32", mutate: func(l *Limits) { l.MaxSamples = int(maxSamplesBeyondInt32) }, field: "max_samples"})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := valid
			if test.mutate != nil {
				test.mutate(&limits)
			}
			err := limits.Validate()
			if test.field == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			var reasonErr *ReasonError
			if !errors.As(err, &reasonErr) {
				t.Fatalf("Validate() error = %v, want *ReasonError", err)
			}
			if reasonErr.Code != ReasonInvalidPayload || reasonErr.Field != test.field {
				t.Fatalf("ReasonError = %#v, want invalid_payload for %q", reasonErr, test.field)
			}
		})
	}
}

func TestReasonErrorDoesNotIncludeFieldValue(t *testing.T) {
	err := (&ReasonError{Code: ReasonInvalidPayload, Field: `samples[0]="sensitive payload"`}).Error()
	if err != "telemetry error: invalid_payload" {
		t.Fatalf("Error() = %q, want payload-safe error", err)
	}
}
