package digitaltwin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Control bounds preserve the existing exact DOUBLE PRECISION interval domain.
// 2^53 microseconds is also below MaxInt64 nanoseconds: do not lower this bound.
const (
	MinSampleIntervalUS       int64 = 10000
	MaxSampleIntervalUS       int64 = 1 << 53
	MaxControlPayloadBytes          = 8 * 1024
	MaxFrequencyTokenBytes          = 128
	MaxFrequencyExponent            = 128
	ActionUpdateConfiguration       = "update_configuration"
)

// Control errors are transport-independent. HTTP mapping is in httpserver;
// MQTT parsers never include untrusted payloads in their errors.
var (
	ErrInvalidFrequency          = errors.New("invalid_frequency")
	ErrFrequencyNotRepresentable = errors.New("frequency_not_representable")
	ErrUnsupportedCommandTarget  = errors.New("unsupported_command_target")
	ErrDesiredVersionConflict    = errors.New("desired_version_conflict")
	ErrCommandInProgress         = errors.New("command_in_progress")
	ErrIdempotencyConflict       = errors.New("idempotency_conflict")
	ErrDesiredVersionOverflow    = errors.New("desired_version_overflow")
	ErrControlPayloadTooLarge    = errors.New("control_payload_too_large")
)

// Version is a nonnegative PostgreSQL BIGINT encoded as a JSON string, never a
// JavaScript number. Increment must be checked by admission before any write.
type Version int64

// MarshalJSON preserves BIGINT precision using the protocol's string encoding.
func (v Version) MarshalJSON() ([]byte, error) {
	if v < 0 {
		return nil, ErrInvalidInput
	}
	return json.Marshal(strconv.FormatInt(int64(v), 10))
}

// UnmarshalJSON rejects numbers, signs, fractions and BIGINT overflow.
func (v *Version) UnmarshalJSON(data []byte) error {
	s, err := controlString(data)
	if err != nil || len(s) > 128 || !decimalDigits(s) {
		return ErrInvalidInput
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		s = "0"
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ErrInvalidInput
	}
	*v = Version(n)
	return nil
}

// CommandStatus distinguishes durable intent/broker delivery from confirmed
// execution. Unchanged is a request outcome, never a stored command status.
type CommandStatus string

// Stored command lifecycle values and the separate no-op request outcome.
const (
	CommandPending      CommandStatus = "pending"
	CommandPublished    CommandStatus = "published"
	CommandAcknowledged CommandStatus = "acknowledged" // legacy reserved active slot
	CommandSucceeded    CommandStatus = "succeeded"
	CommandFailed       CommandStatus = "failed"
	CommandTimeout      CommandStatus = "timeout"
	RequestUnchanged                  = "unchanged"
)

// Active includes reserved acknowledged rows when protecting the Sensor slot.
func (s CommandStatus) Active() bool {
	return s == CommandPending || s == CommandPublished || s == CommandAcknowledged
}

// Terminal reports statuses that must never regress after result ingestion.
func (s CommandStatus) Terminal() bool {
	return s == CommandSucceeded || s == CommandFailed || s == CommandTimeout
}

// Valid reports whether this is a stored command status (not a request outcome).
func (s CommandStatus) Valid() bool { return s.Active() || s.Terminal() }

// SamplingParameters is the complete configuration allowlist for this stage.
type SamplingParameters struct {
	SampleIntervalUS int64 `json:"sample_interval_us"`
}

// Command is the allowlisted MQTT command envelope, also suitable for retaining
// immutable intent in a repository. Runtime lifecycle metadata stays separate.
type Command struct {
	ProtocolVersion int                `json:"protocol_version"`
	MessageType     string             `json:"message_type"`
	CommandID       string             `json:"command_id"`
	GatewayID       string             `json:"gateway_id"`
	EntityID        string             `json:"entity_id"`
	Action          string             `json:"action"`
	Parameters      SamplingParameters `json:"parameters"`
	DesiredVersion  Version            `json:"desired_version"`
	IssuedAt        time.Time          `json:"issued_at"`
	ExpiresAt       time.Time          `json:"expires_at"`
}

// CommandResult is a validated device result with safe public failure text.
type CommandResult struct {
	ProtocolVersion int                 `json:"protocol_version"`
	MessageType     string              `json:"message_type"`
	CommandID       string              `json:"command_id"`
	GatewayID       string              `json:"gateway_id"`
	EntityID        string              `json:"entity_id"`
	DesiredVersion  Version             `json:"desired_version"`
	Status          CommandStatus       `json:"status"`
	ReportedState   *SamplingParameters `json:"reported_state,omitempty"`
	CompletedAt     time.Time           `json:"completed_at"`
	ErrorCode       string              `json:"error_code,omitempty"`
	ErrorMessage    string              `json:"error_message,omitempty"`
}

// CommandRecord is domain data, not an outbox or SQL repository implementation.
type CommandRecord struct {
	Command            Command
	Status             CommandStatus
	RequestedFrequency json.Number
	CompletedAt        *time.Time
	Result             *CommandResult
}

// RequestReceipt stores exact committed response bytes for both 202 and 200.
// Replays must not regenerate these from current state. Actor/entity/key scope
// and uniqueness are the responsibility of the Task 5.2 transaction.
type RequestReceipt struct {
	ActorID        string
	EntityID       string
	IdempotencyKey string
	Fingerprint    string
	StatusCode     int
	Response       json.RawMessage
	CommandID      *string
	CreatedAt      time.Time
}

// DesiredRequest is exact parsed intent, not a grant of mutation authority.
type DesiredRequest struct {
	Frequency              Frequency
	ExpectedDesiredVersion Version
	IdempotencyKey         string
}

// Fingerprint uses exact rational identity, not rounded interval or display Hz.
// Array framing prevents concatenation ambiguities. Actor/entity/key scope is
// deliberately outside the digest, in the unique receipt key.
func (r DesiredRequest) Fingerprint() string {
	data, _ := json.Marshal([3]string{ActionUpdateConfiguration, r.Frequency.canonical, strconv.FormatInt(int64(r.ExpectedDesiredVersion), 10)})
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// ParseDesiredRequest bounds and strictly validates a REST desired-state body.
func ParseDesiredRequest(data []byte, key string) (DesiredRequest, error) {
	if len(data) > MaxControlPayloadBytes {
		return DesiredRequest{}, ErrControlPayloadTooLarge
	}
	if len(key) < 1 || len(key) > 128 {
		return DesiredRequest{}, ErrInvalidInput
	}
	for _, c := range []byte(key) {
		if c < 0x20 || c > 0x7e {
			return DesiredRequest{}, ErrInvalidInput
		}
	}
	m, err := controlObject(data, "frequency_hz", "expected_desired_version")
	if err != nil || len(m) != 2 {
		return DesiredRequest{}, ErrInvalidInput
	}
	f, err := parseFrequency(m["frequency_hz"])
	if err != nil {
		return DesiredRequest{}, err
	}
	var version Version
	if err := version.UnmarshalJSON(m["expected_desired_version"]); err != nil {
		return DesiredRequest{}, err
	}
	return DesiredRequest{Frequency: f, ExpectedDesiredVersion: version, IdempotencyKey: key}, nil
}

// SafeCommandErrorMessage returns fixed public text, never a raw device/driver
// diagnostic. Empty means the error code is not part of the protocol allowlist.
func SafeCommandErrorMessage(code string) string {
	switch code {
	case "COMMAND_EXPIRED":
		return "command expired"
	case "STALE_DESIRED_VERSION":
		return "stale desired version"
	case "UNKNOWN_SENSOR":
		return "unknown sensor"
	case "UNSUPPORTED_CONFIGURATION":
		return "unsupported configuration"
	case "APPLY_FAILED":
		return "configuration apply failed"
	default:
		return ""
	}
}
