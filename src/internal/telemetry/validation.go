package telemetry

import (
	"math"
	"strconv"
	"time"
)

// validatedRange contains the checked sequence and time bounds for a batch.
type validatedRange struct {
	FirstMeasuredAt time.Time
	LastMeasuredAt  time.Time
	LastSequence    int64
	Interval        time.Duration
}

// maxInt64 bounds signed 64-bit integer arithmetic.
const maxInt64 = int64(math.MaxInt64)

// maxExactFloat64Integer is 2^53, the largest integer magnitude a float64 (and
// therefore PostgreSQL DOUBLE PRECISION) can represent exactly. sample_interval_us
// is persisted in the temporal value_number column, so a larger-but-valid int64
// would be silently rounded on write; bound the accepted domain to keep every
// stored interval exact.
const maxExactFloat64Integer = int64(1) << 53

// validateRange checks batch sequence and time arithmetic before any samples
// are expanded. receivedAt is supplied by the caller to keep clock use explicit.
func validateRange(batch BatchDTO, limits Limits, receivedAt time.Time) (validatedRange, error) {
	invalidSequence := func(field string) (validatedRange, error) {
		return validatedRange{}, &ReasonError{Code: ReasonInvalidSequenceRange, Field: field}
	}
	invalidTimestamp := func(field string) (validatedRange, error) {
		return validatedRange{}, &ReasonError{Code: ReasonInvalidTimestamp, Field: field}
	}

	if batch.SampleCount <= 0 {
		return invalidSequence("sample_count")
	}
	if batch.FirstSequence < 0 {
		return invalidSequence("first_sequence")
	}
	steps := int64(batch.SampleCount) - 1
	if steps > maxInt64-batch.FirstSequence {
		return invalidSequence("last_sequence")
	}
	lastSequence := batch.FirstSequence + steps

	// sample_interval_us is written to the temporal value_number column as
	// DOUBLE PRECISION, so it must stay within the exact float64 integer range.
	// This bound is tighter than the duration conversion bound and therefore
	// also protects it.
	if batch.SampleIntervalUS <= 0 ||
		batch.SampleIntervalUS > maxExactFloat64Integer ||
		batch.SampleIntervalUS > maxInt64/int64(time.Microsecond) {
		return invalidSequence("sample_interval_us")
	}
	interval := time.Duration(batch.SampleIntervalUS) * time.Microsecond
	if steps > 0 && interval > time.Duration(maxInt64/steps) {
		return invalidSequence("span")
	}
	span := time.Duration(steps) * interval
	if limits.MaxBatchDuration <= 0 || span > limits.MaxBatchDuration {
		return invalidSequence("span")
	}

	first, err := parseMeasuredAt(batch.MeasuredAt)
	if err != nil {
		return invalidTimestamp("measured_at")
	}
	last, ok := addTimestampSafe(first, span)
	if !ok {
		return invalidTimestamp("last_measured_at")
	}

	if limits.MaxFutureSkew < 0 {
		return invalidTimestamp("max_future_skew")
	}
	threshold := receivedAt.Add(limits.MaxFutureSkew).UTC()
	if threshold.Year() < 1 {
		return validatedRange{}, &ReasonError{Code: ReasonTimestampInFuture, Field: "last_measured_at"}
	}
	if threshold.Year() <= 9999 && last.After(threshold) {
		return validatedRange{}, &ReasonError{Code: ReasonTimestampInFuture, Field: "last_measured_at"}
	}
	// If the threshold is beyond year 9999, every valid batch time is within it.

	return validatedRange{
		FirstMeasuredAt: first,
		LastMeasuredAt:  last,
		LastSequence:    lastSequence,
		Interval:        interval,
	}, nil
}

// parseMeasuredAt enforces the exact RFC3339 shape and fractional precision
// before time.Parse, whose accepted syntax is intentionally more permissive.
func parseMeasuredAt(value string) (time.Time, error) {
	if len(value) < 20 || !allDigits(value[:4]) || value[4] != '-' || !allDigits(value[5:7]) || value[7] != '-' ||
		!allDigits(value[8:10]) || value[10] != 'T' || !allDigits(value[11:13]) || value[13] != ':' ||
		!allDigits(value[14:16]) || value[16] != ':' || !allDigits(value[17:19]) {
		return time.Time{}, strconv.ErrSyntax
	}
	year, _ := strconv.Atoi(value[:4])
	if year == 0 {
		return time.Time{}, strconv.ErrSyntax
	}

	zoneStart := len(value) - 1
	for i := 19; i < len(value); i++ {
		if value[i] == 'Z' || value[i] == '+' || value[i] == '-' {
			zoneStart = i
			break
		}
	}
	if zoneStart != 19 {
		if value[19] != '.' {
			return time.Time{}, strconv.ErrSyntax
		}
		fractionDigits := value[20:zoneStart]
		if len(fractionDigits) == 0 || len(fractionDigits) > 6 || !allDigits(fractionDigits) {
			return time.Time{}, strconv.ErrSyntax
		}
	}

	zone := value[zoneStart:]
	if zone != "Z" {
		if len(zone) != 6 || (zone[0] != '+' && zone[0] != '-') || zone[3] != ':' || !allDigits(zone[1:3]) || !allDigits(zone[4:6]) {
			return time.Time{}, strconv.ErrSyntax
		}
		hour, _ := strconv.Atoi(zone[1:3])
		minute, _ := strconv.Atoi(zone[4:6])
		if hour > 23 || minute > 59 {
			return time.Time{}, strconv.ErrSyntax
		}
	}
	// RFC3339's calendar time fields are two digits; reject Go's accepted
	// 24:00 and leap-second spellings rather than silently normalizing them.
	hour, _ := strconv.Atoi(value[11:13])
	minute, _ := strconv.Atoi(value[14:16])
	second, _ := strconv.Atoi(value[17:19])
	if hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, strconv.ErrSyntax
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	parsed = parsed.UTC()
	if !validCalendarYear(parsed) {
		return time.Time{}, strconv.ErrSyntax
	}
	return parsed, nil
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func addTimestampSafe(value time.Time, delta time.Duration) (time.Time, bool) {
	result := value.Add(delta).UTC()
	return result, validCalendarYear(result)
}

func validCalendarYear(value time.Time) bool {
	return value.Year() >= 1 && value.Year() <= 9999
}
