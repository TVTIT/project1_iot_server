// Package mediaprotocol defines and validates the Stage 8 media MQTT wire contract.
package mediaprotocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"iot-platform/internal/gateway"
)

// Protocol limits and MQTT QoS/retain constraints for the media wire protocol.
const (
	MaxEnvelopeBytes = 4096
	MaxUploadBytes   = 80000000
	WireQoS          = 1
	WireRetain       = false
)

// Grant status values transmitted in downlink upload_grant messages.
const (
	GrantNeedUpload           = "need_upload"
	GrantRetryAfterCompletion = "retry_after_completion"
	GrantPending              = "pending"
	GrantVerifying            = "verifying"
	GrantReady                = "ready"
	GrantRejected             = "rejected"
	GrantExpired              = "expired"
	GrantConflict             = "conflict"
)

// Result status values transmitted in downlink upload_result messages.
const (
	ResultPending   = "pending"
	ResultVerifying = "verifying"
	ResultReady     = "ready"
	ResultRejected  = "rejected"
	ResultExpired   = "expired"
	ResultConflict  = "conflict"
)

// UploadRequest represents an uplink intent request message from a gateway.
type UploadRequest struct {
	Version     int    `json:"version"`
	GatewayID   string `json:"gateway_id"`
	UploadID    string `json:"upload_id"`
	CapturedAt  string `json:"captured_at"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

// UploadComplete represents an uplink completion notification message from a gateway.
type UploadComplete struct {
	Version   int    `json:"version"`
	GatewayID string `json:"gateway_id"`
	UploadID  string `json:"upload_id"`
}

// UploadGrant is the backend response to either client request. UploadURL is
// present only for need_upload/retry_after_completion and is never sanitized or rewritten.
type UploadGrant struct {
	Version   int    `json:"version"`
	GatewayID string `json:"gateway_id"`
	UploadID  string `json:"upload_id"`
	Status    string `json:"status"`
	MediaID   string `json:"media_id,omitempty"`
	UploadURL string `json:"upload_url,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Code      string `json:"code,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (UploadGrant) String() string { return "mediaprotocol.UploadGrant{redacted}" }

// GoString returns a redacted string representation of UploadGrant to protect signed URLs.
func (UploadGrant) GoString() string { return "mediaprotocol.UploadGrant{redacted}" }

// UploadResult represents a downlink terminal or interim verification status message.
type UploadResult struct {
	Version   int    `json:"version"`
	GatewayID string `json:"gateway_id"`
	UploadID  string `json:"upload_id"`
	Status    string `json:"status"`
	MediaID   string `json:"media_id,omitempty"`
	Code      string `json:"code,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var hashPattern = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
var canonicalUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func validCanonicalUUID(id string) bool { return canonicalUUIDPattern.MatchString(id) }

// UploadRequestTopic returns the MQTT topic for uplink upload requests from a gateway.
func UploadRequestTopic(id string) string { return "gateways/" + id + "/media/up/upload_request" }

// UploadCompleteTopic returns the MQTT topic for uplink upload completions from a gateway.
func UploadCompleteTopic(id string) string { return "gateways/" + id + "/media/up/upload_complete" }

// UploadGrantTopic returns the MQTT topic for downlink upload grants to a gateway.
func UploadGrantTopic(id string) string { return "gateways/" + id + "/media/down/upload_grant" }

// UploadResultTopic returns the MQTT topic for downlink upload results to a gateway.
func UploadResultTopic(id string) string { return "gateways/" + id + "/media/down/upload_result" }

// Decode validates topic, direction, envelope and message-specific fields.
func Decode(topic string, payload []byte) (any, error) {
	if len(payload) > MaxEnvelopeBytes {
		return nil, errors.New("media envelope exceeds maximum size")
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "gateways" || parts[2] != "media" || parts[3] == "" {
		return nil, errors.New("invalid media topic")
	}
	if err := gateway.ValidateGatewayID(parts[1]); err != nil {
		return nil, errors.New("invalid media topic")
	}
	switch parts[3] + "/" + parts[4] {
	case "up/upload_request":
		var m UploadRequest
		if err := strictDecode(payload, &m, "version", "gateway_id", "upload_id", "captured_at", "content_type", "size_bytes", "sha256"); err != nil {
			return nil, err
		}
		if m.GatewayID != "" && m.GatewayID != parts[1] {
			return nil, errors.New("invalid media envelope correlation")
		}
		m.GatewayID = parts[1]
		if err := validateBase(m.Version, m.UploadID); err != nil {
			return nil, err
		}
		if !validTime(m.CapturedAt) || m.ContentType != "video/mp4" || m.SizeBytes < 1 || m.SizeBytes > MaxUploadBytes || !hashPattern.MatchString(m.SHA256) {
			return nil, errors.New("invalid media upload request")
		}
		capturedAt, _ := time.Parse(time.RFC3339, m.CapturedAt)
		m.CapturedAt = capturedAt.UTC().Round(time.Microsecond).Format("2006-01-02T15:04:05.000000Z")
		m.SHA256 = strings.ToLower(m.SHA256)
		return m, nil
	case "up/upload_complete":
		var m UploadComplete
		if err := strictDecode(payload, &m, "version", "gateway_id", "upload_id"); err != nil {
			return nil, err
		}
		if m.GatewayID != "" && m.GatewayID != parts[1] {
			return nil, errors.New("invalid media envelope correlation")
		}
		m.GatewayID = parts[1]
		if err := validateBase(m.Version, m.UploadID); err != nil {
			return nil, err
		}
		return m, nil
	case "down/upload_grant":
		var m UploadGrant
		if err := strictDecode(payload, &m, "version", "gateway_id", "upload_id", "status", "media_id", "upload_url", "expires_at", "code", "retryable"); err != nil {
			return nil, err
		}
		if m.GatewayID != "" && m.GatewayID != parts[1] {
			return nil, errors.New("invalid media envelope correlation")
		}
		m.GatewayID = parts[1]
		if err := validateBase(m.Version, m.UploadID); err != nil {
			return nil, err
		}
		if !validGrant(m, hasField(payload, "media_id")) {
			return nil, errors.New("invalid media upload grant")
		}
		return m, nil
	case "down/upload_result":
		var m UploadResult
		if err := strictDecode(payload, &m, "version", "gateway_id", "upload_id", "status", "media_id", "code", "retryable"); err != nil {
			return nil, err
		}
		if m.GatewayID != "" && m.GatewayID != parts[1] {
			return nil, errors.New("invalid media envelope correlation")
		}
		m.GatewayID = parts[1]
		if err := validateBase(m.Version, m.UploadID); err != nil {
			return nil, err
		}
		if !oneOf(m.Status, ResultPending, ResultVerifying, ResultReady, ResultRejected, ResultExpired, ResultConflict) || !validOptionalCode(m.Code) || (hasField(payload, "media_id") && !validID(m.MediaID)) {
			return nil, errors.New("invalid media upload result")
		}
		if m.Status == ResultReady && (!validID(m.MediaID) || m.Retryable) ||
			(m.Status == ResultRejected || m.Status == ResultExpired || m.Status == ResultConflict) && m.Retryable ||
			m.Status == ResultConflict && m.Code != "ID_METADATA_CONFLICT" ||
			(m.Status == ResultPending && m.Code == "OBJECT_MISSING" && (!validCanonicalUUID(m.MediaID) || !m.Retryable)) {
			return nil, errors.New("invalid media upload result")
		}
		return m, nil
	default:
		return nil, errors.New("unsupported media topic")
	}
}

// Encode validates a typed message against its exact topic and emits compact JSON.
func Encode(topic string, message any) ([]byte, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return nil, errors.New("invalid media message")
	}
	if _, err := Decode(topic, data); err != nil {
		return nil, err
	}
	return data, nil
}

func strictDecode(data []byte, dst any, allowed ...string) error {
	if !validObjectFields(data, allowed) {
		return errors.New("malformed media envelope")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return errors.New("malformed media envelope")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("malformed media envelope")
	}
	return nil
}

func validObjectFields(data []byte, allowed []string) bool {
	if !utf8.Valid(data) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(data))
	root, err := d.Token()
	if err != nil || root != json.Delim('{') {
		return false
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allow[field] = struct{}{}
	}
	seen := make(map[string]struct{}, len(allowed))
	for d.More() {
		keyToken, err := d.Token()
		if err != nil {
			return false
		}
		key, ok := keyToken.(string)
		if !ok {
			return false
		}
		if _, exists := seen[key]; exists {
			return false
		}
		if _, exists := allow[key]; !exists {
			return false
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return false
		}
		if key == "gateway_id" && bytes.Equal(bytes.TrimSpace(value), []byte(`""`)) {
			return false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return false
	}
	var trailing any
	return d.Decode(&trailing) == io.EOF
}

func validateBase(version int, uploadID string) error {
	if version != 1 || !validID(uploadID) {
		return fmt.Errorf("invalid media envelope correlation")
	}
	return nil
}

func hasField(data []byte, field string) bool {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	_, ok := fields[field]
	return ok
}

func validGrant(m UploadGrant, hasMediaID bool) bool {
	if !oneOf(m.Status, GrantNeedUpload, GrantRetryAfterCompletion, GrantPending, GrantVerifying, GrantReady, GrantRejected, GrantExpired, GrantConflict) || !validOptionalCode(m.Code) {
		return false
	}
	if hasMediaID && !validID(m.MediaID) {
		return false
	}
	if m.Status == GrantReady && (!validID(m.MediaID) || m.Retryable) ||
		(m.Status == GrantRejected || m.Status == GrantExpired || m.Status == GrantConflict) && m.Retryable ||
		m.Status == GrantConflict && m.Code != "ID_METADATA_CONFLICT" {
		return false
	}
	hasURL := m.Status == GrantNeedUpload
	if hasURL {
		if !validID(m.MediaID) || !validTime(m.ExpiresAt) {
			return false
		}
		u, err := url.Parse(m.UploadURL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.Path == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
			return false
		}
		query, err := url.ParseQuery(u.RawQuery)
		return err == nil && query.Get("token") != ""
	}
	if m.Status == GrantRetryAfterCompletion {
		return m.UploadURL == "" && m.ExpiresAt == "" && validCanonicalUUID(m.MediaID)
	}
	return m.UploadURL == "" && m.ExpiresAt == "" && (m.MediaID == "" || validID(m.MediaID))
}

func validID(id string) bool             { return safeID.MatchString(id) }
func validOptionalCode(code string) bool { return code == "" || codePattern.MatchString(code) }
func oneOf(s string, options ...string) bool {
	for _, option := range options {
		if s == option {
			return true
		}
	}
	return false
}
func validTime(s string) bool {
	t, err := time.Parse(time.RFC3339, s)
	return err == nil && !t.IsZero()
}
