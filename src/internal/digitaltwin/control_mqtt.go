package digitaltwin

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

func controlIdentity(topic string, m map[string]json.RawMessage, kind string) (commandID, gatewayID, entityID string, err error) {
	commandID, err = controlString(m["command_id"])
	if err != nil {
		return
	}
	id, e := uuid.Parse(commandID)
	if e != nil || id.String() != commandID {
		err = ErrInvalidInput
		return
	}
	gatewayID, err = controlString(m["gateway_id"])
	if err != nil {
		return
	}
	entityID, err = controlString(m["entity_id"])
	if err != nil {
		return
	}
	p, e := ParseEntityURN(entityID)
	if e != nil || p.Type != EntityTypeSensor || p.GatewayID != gatewayID {
		err = ErrInvalidInput
		return
	}
	if topic != "gateways/"+gatewayID+"/"+kind+"/"+commandID {
		err = ErrInvalidInput
	}
	return
}

// ParseMQTTCommand validates a bounded, non-retained command and topic identity.
// It neither checks current time nor executes or persists the command.
func ParseMQTTCommand(topic string, data []byte, retained bool) (Command, error) {
	if len(data) > MaxControlPayloadBytes {
		return Command{}, ErrControlPayloadTooLarge
	}
	if retained {
		return Command{}, ErrInvalidInput
	}
	m, err := controlObject(data, "protocol_version", "message_type", "command_id", "gateway_id", "entity_id", "action", "parameters", "desired_version", "issued_at", "expires_at")
	if err != nil || len(m) != 10 || !bytes.Equal(bytes.TrimSpace(m["protocol_version"]), []byte("1")) {
		return Command{}, ErrInvalidInput
	}
	var c Command
	c.CommandID, c.GatewayID, c.EntityID, err = controlIdentity(topic, m, "commands")
	if err != nil {
		return Command{}, err
	}
	c.MessageType, err = controlString(m["message_type"])
	if err != nil || c.MessageType != "command" {
		return Command{}, ErrInvalidInput
	}
	c.Action, err = controlString(m["action"])
	if err != nil || c.Action != ActionUpdateConfiguration {
		return Command{}, ErrInvalidInput
	}
	c.Parameters, err = controlInterval(m["parameters"])
	if err != nil {
		return Command{}, err
	}
	if err = c.DesiredVersion.UnmarshalJSON(m["desired_version"]); err != nil {
		return Command{}, err
	}
	c.IssuedAt, err = controlTime(m["issued_at"])
	if err != nil {
		return Command{}, err
	}
	c.ExpiresAt, err = controlTime(m["expires_at"])
	if err != nil || !c.ExpiresAt.After(c.IssuedAt) {
		return Command{}, ErrInvalidInput
	}
	c.ProtocolVersion = 1
	return c, nil
}

// ParseMQTTResult strictly validates the result envelope, without DB mutation.
// Stored-command correlation is a separate step, ValidateResultForCommand.
func ParseMQTTResult(topic string, data []byte, retained bool) (CommandResult, error) {
	if len(data) > MaxControlPayloadBytes {
		return CommandResult{}, ErrControlPayloadTooLarge
	}
	if retained {
		return CommandResult{}, ErrInvalidInput
	}
	m, err := controlObject(data, "protocol_version", "message_type", "command_id", "gateway_id", "entity_id", "desired_version", "status", "reported_state", "completed_at", "error_code", "error_message")
	if err != nil || !bytes.Equal(bytes.TrimSpace(m["protocol_version"]), []byte("1")) {
		return CommandResult{}, ErrInvalidInput
	}
	var r CommandResult
	r.CommandID, r.GatewayID, r.EntityID, err = controlIdentity(topic, m, "responses")
	if err != nil {
		return CommandResult{}, err
	}
	r.MessageType, err = controlString(m["message_type"])
	if err != nil || r.MessageType != "command_result" {
		return CommandResult{}, ErrInvalidInput
	}
	if err = r.DesiredVersion.UnmarshalJSON(m["desired_version"]); err != nil {
		return CommandResult{}, err
	}
	r.CompletedAt, err = controlTime(m["completed_at"])
	if err != nil {
		return CommandResult{}, err
	}
	s, err := controlString(m["status"])
	if err != nil {
		return CommandResult{}, err
	}
	r.Status = CommandStatus(s)
	switch r.Status {
	case CommandSucceeded:
		if _, ok := m["error_code"]; ok {
			return CommandResult{}, ErrInvalidInput
		}
		if _, ok := m["error_message"]; ok {
			return CommandResult{}, ErrInvalidInput
		}
		p, e := controlInterval(m["reported_state"])
		if e != nil {
			return CommandResult{}, e
		}
		r.ReportedState = &p
	case CommandFailed:
		if _, ok := m["reported_state"]; ok {
			return CommandResult{}, ErrInvalidInput
		}
		r.ErrorCode, err = controlString(m["error_code"])
		if err != nil {
			return CommandResult{}, err
		}
		r.ErrorMessage = SafeCommandErrorMessage(r.ErrorCode)
		if r.ErrorMessage == "" {
			return CommandResult{}, ErrInvalidInput
		}
		// A diagnostic may arrive but is NEVER retained in the public model. Its
		// size is bounded by the whole 8 KiB envelope; no arbitrary text is echoed.
		if raw, ok := m["error_message"]; ok {
			msg, e := controlString(raw)
			if e != nil || strings.ContainsAny(msg, "\r\n") {
				return CommandResult{}, ErrInvalidInput
			}
		}
	default:
		return CommandResult{}, ErrInvalidInput
	}
	r.ProtocolVersion = 1
	return r, nil
}

// ValidateResultForCommand performs pure correlation, not ingestion or terminal
// state transitions. Arrival deadline/late evidence is a repository concern.
// An expired failure may complete after expiry; a success must attest application
// within the command window. No interval is inferred from a failed result.
func ValidateResultForCommand(r CommandResult, c Command) error {
	if r.CommandID != c.CommandID || r.GatewayID != c.GatewayID || r.EntityID != c.EntityID || r.DesiredVersion != c.DesiredVersion || r.CompletedAt.Before(c.IssuedAt) {
		return ErrInvalidInput
	}
	if r.Status == CommandSucceeded {
		if r.ReportedState == nil || r.ReportedState.SampleIntervalUS != c.Parameters.SampleIntervalUS || r.CompletedAt.After(c.ExpiresAt) {
			return ErrInvalidInput
		}
	} else if r.Status != CommandFailed {
		return ErrInvalidInput
	}
	return nil
}
