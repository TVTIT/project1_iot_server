package digitaltwin_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func mustControlTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.UTC()
}

func TestControlCancelledStatusSemantics(t *testing.T) {
	if !digitaltwin.CommandCancelled.Terminal() {
		t.Fatal("cancelled must be terminal")
	}
	if !digitaltwin.CommandCancelled.Valid() {
		t.Fatal("cancelled must be a valid stored command status")
	}
	if digitaltwin.CommandCancelled.Active() {
		t.Fatal("cancelled must not occupy the active Sensor slot")
	}
	for _, status := range []digitaltwin.CommandStatus{digitaltwin.CommandPending, digitaltwin.CommandPublished, digitaltwin.CommandAcknowledged} {
		if !status.Active() || status.Terminal() || !status.Valid() {
			t.Fatalf("active status %q changed semantics", status)
		}
	}
	for _, status := range []digitaltwin.CommandStatus{digitaltwin.CommandSucceeded, digitaltwin.CommandFailed, digitaltwin.CommandTimeout} {
		if !status.Terminal() || status.Active() || !status.Valid() {
			t.Fatalf("terminal status %q changed semantics", status)
		}
	}
}

// The stored status allowlist grows, the wire result allowlist does not: a
// device may never report "cancelled" as a command result.
func TestControlMQTTResultAcceptsOnlySucceededAndFailed(t *testing.T) {
	commandID := uuid.NewString()
	topic := "gateways/gw-01/responses/" + commandID
	envelope := func(status string, extra map[string]any) []byte {
		body := map[string]any{
			"protocol_version": 1,
			"message_type":     "command_result",
			"command_id":       commandID,
			"gateway_id":       "gw-01",
			"entity_id":        "urn:ngsi-ld:Sensor:gw-01:s1",
			"desired_version":  "3",
			"status":           status,
			"completed_at":     "2026-11-14T10:00:00.000000Z",
		}
		for key, value := range extra {
			body[key] = value
		}
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	reported := map[string]any{"reported_state": map[string]any{"sample_interval_us": 1000000}}
	for _, status := range []string{"cancelled", "acknowledged", "published", "pending", "timeout", ""} {
		if _, err := digitaltwin.ParseMQTTResult(topic, envelope(status, reported), false); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("wire result status %q accepted: %v", status, err)
		}
	}
	succeeded, err := digitaltwin.ParseMQTTResult(topic, envelope("succeeded", reported), false)
	if err != nil || succeeded.Status != digitaltwin.CommandSucceeded {
		t.Fatalf("succeeded result: %+v %v", succeeded, err)
	}
	failed, err := digitaltwin.ParseMQTTResult(topic, envelope("failed", map[string]any{"error_code": "APPLY_FAILED"}), false)
	if err != nil || failed.Status != digitaltwin.CommandFailed {
		t.Fatalf("failed result: %+v %v", failed, err)
	}
}

// A cancelled command is a stored status only. It must never be accepted as a
// correlated result for another command.
func TestControlResultCorrelationRejectsCancelledStatus(t *testing.T) {
	command := digitaltwin.Command{
		ProtocolVersion: 1,
		MessageType:     "command",
		CommandID:       uuid.NewString(),
		GatewayID:       "gw-01",
		EntityID:        "urn:ngsi-ld:Sensor:gw-01:s1",
		Action:          digitaltwin.ActionUpdateConfiguration,
		Parameters:      digitaltwin.SamplingParameters{SampleIntervalUS: 1000000},
		DesiredVersion:  3,
		IssuedAt:        mustControlTime(t, "2026-11-14T10:00:00.000000Z"),
		ExpiresAt:       mustControlTime(t, "2026-11-14T10:01:00.000000Z")}
	result := digitaltwin.CommandResult{
		ProtocolVersion: 1,
		MessageType:     "command_result",
		CommandID:       command.CommandID,
		GatewayID:       command.GatewayID,
		EntityID:        command.EntityID,
		DesiredVersion:  command.DesiredVersion,
		Status:          digitaltwin.CommandCancelled,
		CompletedAt:     command.IssuedAt.Add(1e6),
	}
	if err := digitaltwin.ValidateResultForCommand(result, command); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatalf("cancelled result accepted: %v", err)
	}
}
