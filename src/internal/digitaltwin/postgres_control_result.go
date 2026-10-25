package digitaltwin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrControlResultConflict rejects conflicting terminal evidence without overwrite.
var ErrControlResultConflict = errors.New("control_result_conflict")

// ControlResultOutcome classifies a committed result operation, not MQTT receipt.
type ControlResultOutcome string

// Result outcomes have fixed metric cardinality.
const (
	ControlResultAccepted  ControlResultOutcome = "accepted"
	ControlResultDuplicate ControlResultOutcome = "duplicate"
	ControlResultLate      ControlResultOutcome = "late"
)

// AcceptControlResult correlates stable device bytes and commits command/config
// atomically. It never adds a human actor or performs network I/O in the tx.
func (r *PostgresControlRepository) AcceptControlResult(ctx context.Context, topic string, payload []byte, receivedAt time.Time) (ControlResultOutcome, error) {
	if ctx == nil || receivedAt.IsZero() {
		return "", ErrInvalidInput
	}
	result, err := ParseMQTTResult(topic, payload, false)
	if err != nil {
		return "", err
	}
	if r == nil || isNilDependency(r.db) {
		return "", ErrDependencyUnavailable
	}
	id, err := uuid.Parse(result.CommandID)
	if err != nil {
		return "", ErrInvalidInput
	}
	var outcome ControlResultOutcome
	found := false
	err = r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, urn string, now time.Time) error {
		found = true
		var status CommandStatus
		var version Version
		var issued, expires time.Time
		var commandTopic string
		var commandBytes, parameters, canonical, late []byte
		if err := tx.QueryRow(ctx, `SELECT c.status,c.desired_version,c.issued_at,c.expires_at,c.parameters,o.topic,o.payload_bytes,c.result_bytes,c.late_result_bytes
		 FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
		 WHERE c.command_id=$1 AND c.control_version=1 AND c.action='update_configuration' FOR UPDATE OF c,o`, id).Scan(&status, &version, &issued, &expires, &parameters, &commandTopic, &commandBytes, &canonical, &late); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		command, err := ParseMQTTCommand(commandTopic, commandBytes, false)
		if err != nil {
			return ErrInvalidInput
		}
		params, err := controlInterval(parameters)
		if err != nil || command.CommandID != result.CommandID || command.EntityID != urn || command.DesiredVersion != version || command.Parameters != params || !command.IssuedAt.Equal(issued) || !command.ExpiresAt.Equal(expires) {
			return ErrInvalidInput
		}
		if err := ValidateResultForCommand(result, command); err != nil {
			return err
		}
		// A device timestamp cannot authorize a future completion or resurrect
		// work whose authoritative DB deadline elapsed while waiting for locks.
		if result.CompletedAt.After(now) {
			return ErrInvalidInput
		}
		if status == CommandSucceeded || status == CommandFailed {
			if bytes.Equal(canonical, payload) {
				outcome = ControlResultDuplicate
				return nil
			}
			return ErrControlResultConflict
		}
		if status.Active() && !now.Before(expires) {
			if err := ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
				return err
			}
			status = CommandTimeout
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(payload))
		if status == CommandTimeout {
			if len(late) > 0 {
				if bytes.Equal(late, payload) {
					outcome = ControlResultDuplicate
					return nil
				}
				return ErrControlResultConflict
			}
			if _, err := tx.Exec(ctx, `UPDATE public.twin_commands SET late_result_bytes=$2,late_result_hash=$3,late_result_received_at=$4 WHERE command_id=$1 AND late_result_bytes IS NULL`, id, payload, hash, receivedAt.UTC()); err != nil {
				return err
			}
			outcome = ControlResultLate
			return nil
		}
		if !status.Active() {
			return ErrControlResultConflict
		}
		// All mutations are protected by the same resource/state locks. Updating
		// the command first is safe only because the configuration write and
		// outbox stop below must commit (or roll back) with this exact evidence.
		_, err = tx.Exec(ctx, `UPDATE public.twin_commands SET status=$2,completed_at=$3,result_bytes=$4,result_hash=$5,result_received_at=$6,error_code=$7,error_message=$8 WHERE command_id=$1`, id, result.Status, result.CompletedAt, payload, hash, receivedAt.UTC(), nullControlString(result.ErrorCode), nullControlString(result.ErrorMessage))
		if err != nil {
			return err
		}
		if result.Status == CommandSucceeded {
			var desiredVersion Version
			var reported []byte
			if err := tx.QueryRow(ctx, `SELECT desired_version,reported_state FROM public.twin_states WHERE entity_id=$1`, entity).Scan(&desiredVersion, &reported); err != nil {
				return err
			}
			if desiredVersion != result.DesiredVersion {
				return ErrControlResultConflict
			}
			var state map[string]json.RawMessage
			if err := json.Unmarshal(reported, &state); err != nil {
				return err
			}
			if raw, ok := state["configuration"]; ok {
				var current struct {
					DesiredVersion Version `json:"desired_version"`
				}
				if err := json.Unmarshal(raw, &current); err != nil {
					return err
				}
				if current.DesiredVersion >= result.DesiredVersion {
					return ErrControlResultConflict
				}
			}
			configuration, err := json.Marshal(struct {
				SampleIntervalUS int64     `json:"sample_interval_us"`
				DesiredVersion   Version   `json:"desired_version"`
				AppliedAt        time.Time `json:"applied_at"`
			}{result.ReportedState.SampleIntervalUS, result.DesiredVersion, result.CompletedAt})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE public.twin_states SET reported_state=jsonb_set(reported_state,'{configuration}',$2::jsonb,true),reported_version=reported_version+1,updated_at=$3 WHERE entity_id=$1`, entity, configuration, now); err != nil {
				return err
			}
		}
		outboxStatus := "published"
		if result.Status == CommandFailed {
			outboxStatus = "failed"
		}
		if _, err := tx.Exec(ctx, `UPDATE public.twin_outbox SET status=$2 WHERE command_id=$1 AND status IN('pending','published')`, id, outboxStatus); err != nil {
			return err
		}
		outcome = ControlResultAccepted
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", ErrNotFound
	}
	if outcome == ControlResultAccepted && r.timeline != nil {
		r.timeline.ResultCommit(result.CommandID, string(result.Status), time.Now())
	}
	return outcome, nil
}

func nullControlString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
