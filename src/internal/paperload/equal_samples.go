package paperload

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Documented fixed maximum age policy: 30 days (2,592,000 seconds).
const (
	maxContextAgeSecondsBound = 2592000
	EqualSamplesWorkloadType  = "equal_samples"
)

// EqualSamplesSettings holds the exact 17-key object required for E2 equal_samples workloads.
type EqualSamplesSettings struct {
	SchemaVersion        int      `json:"schema_version"`
	WorkloadType         string   `json:"workload_type"`
	Seed                 int64    `json:"seed"`
	TotalSamples         int      `json:"total_samples"`
	SampleRate           int      `json:"sample_rate"`
	DurationSeconds      int      `json:"duration_seconds"`
	LogicalProducers     []string `json:"logical_producers"`
	SamplesPerProducer   int      `json:"samples_per_producer"`
	ProducerSampleRate   int      `json:"producer_sample_rate"`
	SampleIntervalUS     int64    `json:"sample_interval_us"`
	StreamSequenceStart  int64    `json:"stream_sequence_start"`
	TimestampAnchor      string   `json:"timestamp_anchor"`
	MaxContextAgeSeconds int64    `json:"max_context_age_seconds"`
	ContextSHA256        string   `json:"context_sha256"`
	ExpectedSamples      int      `json:"expected_samples"`
	ExpectedMessages     int      `json:"expected_messages"`
	SampleStreamVersion  int      `json:"sample_stream_version"`
}

// EqualSamples is a type alias for EqualSamplesSettings.
type EqualSamples = EqualSamplesSettings

// RoleExpectedCounts describes the expected message and sample counts for one logical role.
type RoleExpectedCounts struct {
	Samples  int `json:"samples"`
	Messages int `json:"messages"`
}

// EqualSamplesExpectedCounts contains calculated per-role and total expected counts.
type EqualSamplesExpectedCounts struct {
	SamplesPerMessage int                           `json:"samples_per_message"`
	TotalSamples      int                           `json:"total_samples"`
	TotalMessages     int                           `json:"total_messages"`
	PerRole           map[string]RoleExpectedCounts `json:"per_role"`
}

var equalSamplesManifestKeySet = map[string]struct{}{
	"schema_version":          {},
	"workload_type":           {},
	"seed":                    {},
	"total_samples":           {},
	"sample_rate":             {},
	"duration_seconds":        {},
	"logical_producers":       {},
	"samples_per_producer":    {},
	"producer_sample_rate":    {},
	"sample_interval_us":      {},
	"stream_sequence_start":   {},
	"timestamp_anchor":        {},
	"max_context_age_seconds": {},
	"context_sha256":          {},
	"expected_samples":        {},
	"expected_messages":       {},
	"sample_stream_version":   {},
}

var (
	strictDigitsRegex = regexp.MustCompile(`^[0-9]+$`)
	rfc3339UTCZRegex  = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?Z$`)
)

// UnmarshalJSON implements strict JSON decoding for EqualSamplesSettings.
// It rejects unknown keys, duplicate keys, null values, and omitted required keys.
func (eq *EqualSamplesSettings) UnmarshalJSON(b []byte) error {
	trimmed := bytes.TrimSpace(b)
	if bytes.Equal(trimmed, []byte("null")) {
		return errors.New("equal_samples cannot be null")
	}
	d := json.NewDecoder(bytes.NewReader(trimmed))
	tok, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return errors.New("equal_samples must be a json object")
	}
	fields := make(map[string]json.RawMessage, len(equalSamplesManifestKeySet))
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := tok.(string)
		if !ok {
			return errors.New("equal_samples key must be a string")
		}
		if _, known := equalSamplesManifestKeySet[key]; !known {
			return fmt.Errorf("unknown equal_samples field: %q", key)
		}
		if _, duplicate := fields[key]; duplicate {
			return fmt.Errorf("duplicate equal_samples field: %q", key)
		}
		var val json.RawMessage
		if err := d.Decode(&val); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(val), []byte("null")) {
			return fmt.Errorf("equal_samples field %q cannot be null", key)
		}
		fields[key] = val
	}
	tokEnd, err := d.Token()
	if err != nil {
		return err
	}
	delimEnd, ok := tokEnd.(json.Delim)
	if !ok || delimEnd != '}' {
		return errors.New("equal_samples must close with '}'")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("extra content after equal_samples object")
	}
	if len(fields) != len(equalSamplesManifestKeySet) {
		return errors.New("missing required equal_samples field")
	}

	parseIntField := func(fieldName string) (int64, error) {
		raw := bytes.TrimSpace(fields[fieldName])
		if !strictDigitsRegex.Match(raw) {
			return 0, fmt.Errorf("field %q must be an integer, got %s", fieldName, string(raw))
		}
		return strconv.ParseInt(string(raw), 10, 64)
	}

	parseStringField := func(fieldName string) (string, error) {
		raw := bytes.TrimSpace(fields[fieldName])
		if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
			return "", fmt.Errorf("field %q must be a string", fieldName)
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}

	schemaVersion, err := parseIntField("schema_version")
	if err != nil {
		return err
	}
	workloadType, err := parseStringField("workload_type")
	if err != nil {
		return err
	}
	seed, err := parseIntField("seed")
	if err != nil {
		return err
	}
	totalSamples, err := parseIntField("total_samples")
	if err != nil {
		return err
	}
	sampleRate, err := parseIntField("sample_rate")
	if err != nil {
		return err
	}
	durationSeconds, err := parseIntField("duration_seconds")
	if err != nil {
		return err
	}

	// logical_producers
	rawProducers := bytes.TrimSpace(fields["logical_producers"])
	if len(rawProducers) < 2 || rawProducers[0] != '[' || rawProducers[len(rawProducers)-1] != ']' {
		return errors.New("field 'logical_producers' must be a list of strings")
	}
	var producers []string
	if err := json.Unmarshal(rawProducers, &producers); err != nil {
		return err
	}
	if len(producers) != 2 || producers[0] != "A" || producers[1] != "B" {
		return errors.New("field 'logical_producers' must be exact ['A', 'B']")
	}

	samplesPerProducer, err := parseIntField("samples_per_producer")
	if err != nil {
		return err
	}
	producerSampleRate, err := parseIntField("producer_sample_rate")
	if err != nil {
		return err
	}
	sampleIntervalUS, err := parseIntField("sample_interval_us")
	if err != nil {
		return err
	}
	streamSequenceStart, err := parseIntField("stream_sequence_start")
	if err != nil {
		return err
	}
	timestampAnchor, err := parseStringField("timestamp_anchor")
	if err != nil {
		return err
	}
	maxContextAgeSeconds, err := parseIntField("max_context_age_seconds")
	if err != nil {
		return err
	}
	contextSHA256, err := parseStringField("context_sha256")
	if err != nil {
		return err
	}
	expectedSamples, err := parseIntField("expected_samples")
	if err != nil {
		return err
	}
	expectedMessages, err := parseIntField("expected_messages")
	if err != nil {
		return err
	}
	sampleStreamVersion, err := parseIntField("sample_stream_version")
	if err != nil {
		return err
	}

	eq.SchemaVersion = int(schemaVersion)
	eq.WorkloadType = workloadType
	eq.Seed = seed
	eq.TotalSamples = int(totalSamples)
	eq.SampleRate = int(sampleRate)
	eq.DurationSeconds = int(durationSeconds)
	eq.LogicalProducers = producers
	eq.SamplesPerProducer = int(samplesPerProducer)
	eq.ProducerSampleRate = int(producerSampleRate)
	eq.SampleIntervalUS = sampleIntervalUS
	eq.StreamSequenceStart = streamSequenceStart
	eq.TimestampAnchor = timestampAnchor
	eq.MaxContextAgeSeconds = maxContextAgeSeconds
	eq.ContextSHA256 = contextSHA256
	eq.ExpectedSamples = int(expectedSamples)
	eq.ExpectedMessages = int(expectedMessages)
	eq.SampleStreamVersion = int(sampleStreamVersion)
	return nil
}

// ParseRFC3339Anchor parses and validates an RFC3339 timestamp anchor string.
// It requires an uppercase 'Z' indicator (UTC only), four-digit calendar year in 0001..9999,
// valid calendar date and time (hours 0..23, minutes 0..59, seconds 0..59),
// and optional microsecond fraction up to 6 digits.
func ParseRFC3339Anchor(anchor string) (time.Time, error) {
	match := rfc3339UTCZRegex.FindStringSubmatch(anchor)
	if match == nil {
		if strings.HasSuffix(anchor, "Z") {
			parts := strings.Split(strings.TrimSuffix(anchor, "Z"), "-")
			if len(parts) >= 1 {
				if y, err := strconv.Atoi(parts[0]); err == nil && (y < 1 || y > 9999) {
					return time.Time{}, errors.New("anchor_year_out_of_bounds")
				}
			}
		}
		return time.Time{}, errors.New("anchor_must_be_utc_z")
	}
	year, _ := strconv.Atoi(match[1])
	month, _ := strconv.Atoi(match[2])
	day, _ := strconv.Atoi(match[3])
	hour, _ := strconv.Atoi(match[4])
	minute, _ := strconv.Atoi(match[5])
	second, _ := strconv.Atoi(match[6])

	if year < 1 || year > 9999 {
		return time.Time{}, errors.New("anchor_year_out_of_bounds")
	}
	if hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, errors.New("anchor_must_be_utc_z")
	}
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, errors.New("anchor_must_be_utc_z")
	}

	nsec := 0
	if match[7] != "" {
		fracStr := match[7]
		for len(fracStr) < 9 {
			fracStr += "0"
		}
		nsec, _ = strconv.Atoi(fracStr)
	}

	t := time.Date(year, time.Month(month), day, hour, minute, second, nsec, time.UTC)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day {
		return time.Time{}, errors.New("anchor_must_be_utc_z")
	}
	return t, nil
}

// canonicalContextPayload holds the 13 canonical keys sorted alphabetically for SHA256 calculation.
type canonicalContextPayload struct {
	DurationSeconds      int      `json:"duration_seconds"`
	LogicalProducers     []string `json:"logical_producers"`
	MaxContextAgeSeconds int64    `json:"max_context_age_seconds"`
	ProducerSampleRate   int      `json:"producer_sample_rate"`
	SampleIntervalUS     int64    `json:"sample_interval_us"`
	SampleRate           int      `json:"sample_rate"`
	SamplesPerProducer   int      `json:"samples_per_producer"`
	SchemaVersion        int      `json:"schema_version"`
	Seed                 int64    `json:"seed"`
	StreamSequenceStart  int64    `json:"stream_sequence_start"`
	TimestampAnchor      string   `json:"timestamp_anchor"`
	TotalSamples         int      `json:"total_samples"`
	WorkloadType         string   `json:"workload_type"`
}

// CalculateEqualSamplesContextSHA256 calculates the canonical SHA256 hex digest
// of the 14 canonical context keys (excluding 'context_sha256') using compact sorted JSON.
func CalculateEqualSamplesContextSHA256(eq EqualSamplesSettings) (string, error) {
	payload := canonicalContextPayload{
		DurationSeconds:      eq.DurationSeconds,
		LogicalProducers:     eq.LogicalProducers,
		MaxContextAgeSeconds: eq.MaxContextAgeSeconds,
		ProducerSampleRate:   eq.ProducerSampleRate,
		SampleIntervalUS:     eq.SampleIntervalUS,
		SampleRate:           eq.SampleRate,
		SamplesPerProducer:   eq.SamplesPerProducer,
		SchemaVersion:        eq.SchemaVersion,
		Seed:                 eq.Seed,
		StreamSequenceStart:  eq.StreamSequenceStart,
		TimestampAnchor:      eq.TimestampAnchor,
		TotalSamples:         eq.TotalSamples,
		WorkloadType:         eq.WorkloadType,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}

func validateEqualSamplesContextStructure(eq EqualSamplesSettings) error {
	if eq.SchemaVersion != 1 {
		return errors.New("unsupported_schema_version")
	}
	if eq.WorkloadType != EqualSamplesWorkloadType {
		return errors.New("unsupported_workload_type")
	}
	if eq.SampleStreamVersion != 1 {
		return errors.New("unsupported_sample_stream_version")
	}
	if eq.StreamSequenceStart != 0 {
		return errors.New("stream_sequence_start_must_be_zero")
	}
	if len(eq.LogicalProducers) != 2 || eq.LogicalProducers[0] != "A" || eq.LogicalProducers[1] != "B" {
		return errors.New("invalid_logical_producers")
	}
	if eq.Seed < 0 {
		return errors.New("seed must be an integer in [0, 2^63 - 1]")
	}
	if eq.MaxContextAgeSeconds <= 0 || eq.MaxContextAgeSeconds > maxContextAgeSecondsBound {
		return errors.New("invalid_max_context_age_seconds")
	}
	if eq.TotalSamples <= 0 || eq.TotalSamples > 60000 || eq.TotalSamples%200 != 0 {
		return errors.New("invalid_total_samples")
	}
	if eq.SampleRate < 2 || eq.SampleRate > 200 || eq.SampleRate%2 != 0 {
		return errors.New("invalid_sample_rate")
	}
	if eq.TotalSamples%eq.SampleRate != 0 {
		return errors.New("duration_must_be_integer")
	}
	expectedDuration := eq.TotalSamples / eq.SampleRate
	if eq.DurationSeconds != expectedDuration || eq.DurationSeconds < 10 || eq.DurationSeconds > 120 {
		return errors.New("invalid_duration_seconds")
	}
	if 2_000_000%eq.SampleRate != 0 {
		return errors.New("sample_interval_us_must_be_integer")
	}
	expectedInterval := int64(2_000_000 / eq.SampleRate)
	if eq.SampleIntervalUS != expectedInterval {
		return errors.New("sample_interval_us_mismatch")
	}
	if eq.SamplesPerProducer != eq.TotalSamples/2 {
		return errors.New("samples_per_producer_mismatch")
	}
	if eq.ProducerSampleRate != eq.SampleRate/2 {
		return errors.New("producer_sample_rate_mismatch")
	}
	return nil
}

// CalculateEqualSamplesExpectedCounts calculates integer-derived expected counts for S=1 or S=100.
// It validates context structure, rational boundaries, anchor syntax, and SHA256 identity
// without enforcing wall-clock freshness, allowing historical reports and replays to function.
func CalculateEqualSamplesExpectedCounts(eq EqualSamplesSettings, samplesPerMessage int) (EqualSamplesExpectedCounts, error) {
	if samplesPerMessage != 1 && samplesPerMessage != 100 {
		return EqualSamplesExpectedCounts{}, errors.New("samples_per_message must be 1 or 100")
	}
	if err := validateEqualSamplesContextStructure(eq); err != nil {
		return EqualSamplesExpectedCounts{}, err
	}
	expectedHash, err := CalculateEqualSamplesContextSHA256(eq)
	if err != nil {
		return EqualSamplesExpectedCounts{}, err
	}
	if !strings.EqualFold(eq.ContextSHA256, expectedHash) {
		return EqualSamplesExpectedCounts{}, errors.New("equal_samples_context_hash_mismatch")
	}
	if _, err := ParseRFC3339Anchor(eq.TimestampAnchor); err != nil {
		return EqualSamplesExpectedCounts{}, err
	}

	perRole := make(map[string]RoleExpectedCounts, len(eq.LogicalProducers))
	for _, role := range eq.LogicalProducers {
		if eq.SamplesPerProducer%samplesPerMessage != 0 {
			return EqualSamplesExpectedCounts{}, errors.New("samples_per_producer not divisible by samples_per_message")
		}
		perRole[role] = RoleExpectedCounts{
			Samples:  eq.SamplesPerProducer,
			Messages: eq.SamplesPerProducer / samplesPerMessage,
		}
	}
	return EqualSamplesExpectedCounts{
		SamplesPerMessage: samplesPerMessage,
		TotalSamples:      eq.TotalSamples,
		TotalMessages:     eq.TotalSamples / samplesPerMessage,
		PerRole:           perRole,
	}, nil
}

// EqualSamplesDueOffset returns the exact duration offset from schedule start for message at index (0-based).
// For message index i, due offset is (i + 1) * S * sample_interval_us microseconds,
// which equals (i + 1) * S / producer_sample_rate in integer duration arithmetic.
func EqualSamplesDueOffset(messageIndex int, samplesPerMessage int, sampleIntervalUS int64) time.Duration {
	if messageIndex < 0 || samplesPerMessage <= 0 || sampleIntervalUS <= 0 {
		return 0
	}
	us := int64(messageIndex+1) * int64(samplesPerMessage) * sampleIntervalUS
	return time.Duration(us) * time.Microsecond
}

// MessageDueOffset returns the due duration offset for messageIndex (0-based).
// It returns an error if messageIndex is out of bounds [0, ExpectedMessages-1],
// preventing N+1 messages from being scheduled.
func (eq EqualSamplesSettings) MessageDueOffset(messageIndex int, samplesPerMessage int) (time.Duration, error) {
	if messageIndex < 0 || messageIndex >= eq.ExpectedMessages {
		return 0, errors.New("equal_samples_message_index_out_of_bounds")
	}
	return EqualSamplesDueOffset(messageIndex, samplesPerMessage, eq.SampleIntervalUS), nil
}

// ValidateEqualSamples validates an equal_samples manifest against all E2 constraints,
// including rational exact counts, traffic, phases, boundaries, anchor syntax, hash identity,
// and optional freshness.
func ValidateEqualSamples(m Manifest, now time.Time, checkFreshness bool) error {
	if m.Experiment != "E2" {
		return errors.New("equal_samples is only supported for experiment E2")
	}
	if m.Profile != "pilot" {
		return errors.New("equal_samples only supports pilot profile in Local MVP")
	}
	if m.Arm != "A" && m.Arm != "B" {
		return errors.New("equal_samples arm must be 'A' or 'B'")
	}
	if len(m.Roles) != 1 || m.Roles[0] != "telemetry" {
		return errors.New("equal_samples requires telemetry-only role")
	}
	if m.SamplesPerMessage != 1 && m.SamplesPerMessage != 100 {
		return errors.New("samples_per_message must be 1 or 100")
	}
	if m.DurabilityMode != "synthetic-load" {
		return errors.New("equal_samples requires synthetic-load durability mode")
	}
	if len(m.Targets) != 1 {
		return errors.New("equal_samples requires a single target per arm")
	}
	if m.Topology.Gateways != 1 || m.Topology.SensorsPerGateway != 1 || m.Topology.Users != 0 || m.Topology.Viewers != 0 {
		return errors.New("equal_samples requires topology gateways=1, sensors_per_gateway=1, users=0, viewers=0")
	}
	if len(m.Subscriptions) != 0 {
		return errors.New("equal_samples does not accept subscriptions")
	}
	if m.Traffic.BurstMessages != 0 || m.Traffic.RampSeconds != 0 {
		return errors.New("equal_samples requires burst_messages=0 and ramp_seconds=0")
	}
	if m.Phases.WarmupSeconds != 0 || m.Phases.CooldownSeconds != 0 {
		return errors.New("equal_samples requires warmup_seconds=0 and cooldown_seconds=0")
	}
	if m.Phases.DrainSeconds != 20 {
		return errors.New("equal_samples requires drain_seconds=20")
	}

	eq := m.EqualSamples
	if eq == nil {
		return errors.New("equal_samples settings missing")
	}

	if eq.DurationSeconds != m.Phases.MeasureSeconds {
		return errors.New("phases.measure_seconds must equal equal_samples.duration_seconds")
	}
	expectedRate := float64(eq.ProducerSampleRate) / float64(m.SamplesPerMessage)
	if math.Abs(m.Traffic.RatePerSecond-expectedRate) > 1e-9 {
		return errors.New("traffic.rate_per_second does not match producer_sample_rate / samples_per_message")
	}

	minWall := m.Phases.MeasureSeconds + m.Phases.DrainSeconds
	maxWall := minWall + 60
	if m.Limits.WallSeconds < minWall || m.Limits.WallSeconds > maxWall {
		return errors.New("limits.wall_seconds out of bounds for equal_samples")
	}

	if eq.Seed < 0 || uint64(eq.Seed) != m.Seed {
		return errors.New("manifest seed must match equal_samples seed in [0, 2^63 - 1]")
	}
	if err := validateEqualSamplesContextStructure(*eq); err != nil {
		return err
	}

	if eq.ExpectedSamples != eq.SamplesPerProducer {
		return errors.New("expected_samples must equal samples_per_producer")
	}
	if eq.ExpectedMessages != eq.SamplesPerProducer/m.SamplesPerMessage {
		return errors.New("expected_messages must equal samples_per_producer / samples_per_message")
	}

	expectedHash, err := CalculateEqualSamplesContextSHA256(*eq)
	if err != nil {
		return err
	}
	if !strings.EqualFold(eq.ContextSHA256, expectedHash) {
		return errors.New("equal_samples_context_hash_mismatch")
	}

	anchorTime, err := ParseRFC3339Anchor(eq.TimestampAnchor)
	if err != nil {
		return err
	}

	if checkFreshness {
		if now.IsZero() {
			now = time.Now().UTC()
		}
		if anchorTime.After(now) {
			return errors.New("equal_samples_stream_not_in_past")
		}
		streamSpanUS := int64(eq.SamplesPerProducer-1) * eq.SampleIntervalUS
		streamEndTime := anchorTime.Add(time.Duration(streamSpanUS) * time.Microsecond)
		if streamEndTime.After(now) {
			return errors.New("equal_samples_stream_not_in_past")
		}
		ageSeconds := now.Sub(anchorTime).Seconds()
		if ageSeconds > float64(eq.MaxContextAgeSeconds) {
			return errors.New("equal_samples_context_expired")
		}
	}

	return nil
}
