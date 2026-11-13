package gatewaysim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"iot-platform/internal/digitaltwin"
)

const (
	controlMaxReceipts = 256
	controlMaxBytes    = 4 << 20
	controlRetention   = 24 * time.Hour
)

// Durable control errors do not expose payloads or SQLite diagnostics.
var (
	ErrControlFull     = errors.New("gatewaysim: control receipt capacity reached")
	ErrControlConflict = errors.New("gatewaysim: command identity conflict")
	ErrControlBusy     = errors.New("gatewaysim: another command is prepared")
	ErrControlSensor   = errors.New("gatewaysim: command is for another sensor")
)

// StoredConfiguration is confirmed only by a completed apply/readback receipt.
type StoredConfiguration struct {
	SampleIntervalUS int64
	DesiredVersion   digitaltwin.Version
	AppliedAt        time.Time
}

// ControlReceipt preserves original command and result bytes across retries.
// Fresh distinguishes admission from an existing prepared/completed receipt;
// only the serialized control owner may resume an existing prepared receipt.
type ControlReceipt struct {
	Command         digitaltwin.Command
	Topic           string
	Payload, Result []byte
	Fresh           bool
}

// ReserveControl durably admits one command before any scheduler side effect.
func (o *Outbox) ReserveControl(ctx context.Context, topic string, payload []byte, now time.Time) (ControlReceipt, error) {
	c, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		return ControlReceipt{}, err
	}
	if c.GatewayID != o.gatewayID || c.EntityID != "urn:ngsi-ld:Sensor:"+o.gatewayID+":"+o.sensorID {
		return ControlReceipt{}, ErrControlSensor
	}
	if now.IsZero() {
		return ControlReceipt{}, digitaltwin.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, o.opTimeout)
	defer cancel()
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return ControlReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := scanControlReceipt(tx.QueryRowContext(ctx, controlReceiptSelect+" WHERE command_id=?", c.CommandID).Scan)
	if err == nil {
		if !bytes.Equal(stored.Payload, payload) || stored.Topic != topic {
			return ControlReceipt{}, ErrControlConflict
		}
		return stored, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ControlReceipt{}, err
	}
	// Retention is checked only for new admission, never ahead of a duplicate.
	// Prepared receipts cannot be evicted, even if the device was offline past TTL.
	if _, err = tx.ExecContext(ctx, "DELETE FROM command_receipts WHERE status='completed' AND retain_until<=?", now.UnixMicro()); err != nil {
		return ControlReceipt{}, err
	}
	var prepared, rows, used int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(command_topic)+length(command_bytes)+COALESCE(length(result_bytes),8192)),0),COALESCE(sum(status='prepared'),0) FROM command_receipts`).Scan(&rows, &used, &prepared); err != nil {
		return ControlReceipt{}, err
	}
	if prepared != 0 {
		return ControlReceipt{}, ErrControlBusy
	}
	if rows >= controlMaxReceipts || used+int64(len(topic)+len(payload)+digitaltwin.MaxControlPayloadBytes) > controlMaxBytes {
		return ControlReceipt{}, ErrControlFull
	}
	retainFrom := c.ExpiresAt
	if now.After(retainFrom) {
		retainFrom = now
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(payload))
	_, err = tx.ExecContext(ctx, `INSERT INTO command_receipts(command_id,command_hash,command_topic,command_bytes,desired_version,sample_interval_us,expires_at,status,retain_until) VALUES(?,?,?,?,?,?,?,'prepared',?)`, c.CommandID, hash, topic, payload, int64(c.DesiredVersion), c.Parameters.SampleIntervalUS, c.ExpiresAt.UnixMicro(), retainFrom.Add(controlRetention).UnixMicro())
	if err != nil {
		return ControlReceipt{}, err
	}
	if err = o.commit(tx); err != nil {
		return ControlReceipt{}, fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	return ControlReceipt{Command: c, Topic: topic, Payload: append([]byte(nil), payload...), Fresh: true}, nil
}

// CompleteControl commits stable result bytes and, on success, configuration.
func (o *Outbox) CompleteControl(ctx context.Context, id string, payload []byte, now time.Time) error {
	r, err := digitaltwin.ParseMQTTResult("gateways/"+o.gatewayID+"/responses/"+id, payload, false)
	if err != nil {
		return err
	}
	if now.IsZero() {
		return digitaltwin.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, o.opTimeout)
	defer cancel()
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := scanControlReceipt(tx.QueryRowContext(ctx, controlReceiptSelect+" WHERE command_id=?", id).Scan)
	if err != nil {
		return err
	}
	if len(receipt.Result) > 0 {
		if bytes.Equal(receipt.Result, payload) {
			return nil
		}
		return ErrControlConflict
	}
	if err = digitaltwin.ValidateResultForCommand(r, receipt.Command); err != nil {
		return err
	}
	if r.Status == digitaltwin.CommandSucceeded {
		var version int64
		err = tx.QueryRowContext(ctx, "SELECT desired_version FROM sensor_configuration WHERE id=1").Scan(&version)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && int64(r.DesiredVersion) <= version {
			return ErrControlConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO sensor_configuration(id,sample_interval_us,desired_version,applied_at) VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET sample_interval_us=excluded.sample_interval_us,desired_version=excluded.desired_version,applied_at=excluded.applied_at`, r.ReportedState.SampleIntervalUS, int64(r.DesiredVersion), r.CompletedAt.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
	}
	retainFrom := receipt.Command.ExpiresAt
	if now.After(retainFrom) {
		retainFrom = now
	}
	_, err = tx.ExecContext(ctx, `UPDATE command_receipts SET status='completed',result_bytes=?,retain_until=? WHERE command_id=? AND status='prepared'`, payload, retainFrom.Add(controlRetention).UnixMicro(), id)
	if err != nil {
		return err
	}
	if err = o.commit(tx); err != nil {
		return fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	return nil
}

// Configuration returns the last durably confirmed applied configuration.
func (o *Outbox) Configuration(ctx context.Context) (StoredConfiguration, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, o.opTimeout)
	defer cancel()
	var c StoredConfiguration
	var applied string
	err := o.db.QueryRowContext(ctx, "SELECT sample_interval_us,desired_version,applied_at FROM sensor_configuration WHERE id=1").Scan(&c.SampleIntervalUS, &c.DesiredVersion, &applied)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredConfiguration{}, false, nil
	}
	if err != nil {
		return StoredConfiguration{}, false, err
	}
	c.AppliedAt, err = time.Parse(time.RFC3339Nano, applied)
	if err != nil || c.DesiredVersion < 0 || c.SampleIntervalUS < digitaltwin.MinSampleIntervalUS || c.SampleIntervalUS > digitaltwin.MaxSampleIntervalUS {
		return StoredConfiguration{}, false, ErrOutboxCorrupt
	}
	return c, true, nil
}

// ControlReceipts returns at most the fixed control quota, including prepared
// work. It is used before sampling starts and by the bounded resend owner.
func (o *Outbox) ControlReceipts(ctx context.Context) ([]ControlReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, o.opTimeout)
	defer cancel()
	rows, err := o.db.QueryContext(ctx, controlReceiptSelect+" ORDER BY desired_version,command_id LIMIT ?", controlMaxReceipts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var receipts []ControlReceipt
	for rows.Next() {
		r, err := scanControlReceipt(rows.Scan)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, r)
	}
	return receipts, rows.Err()
}

const controlReceiptSelect = `SELECT command_topic,command_bytes,command_hash,status,result_bytes FROM command_receipts`

func scanControlReceipt(scan func(...any) error) (ControlReceipt, error) {
	var r ControlReceipt
	var hash, status string
	if err := scan(&r.Topic, &r.Payload, &hash, &status, &r.Result); err != nil {
		return ControlReceipt{}, err
	}
	c, err := digitaltwin.ParseMQTTCommand(r.Topic, r.Payload, false)
	if err != nil || hash != fmt.Sprintf("%x", sha256.Sum256(r.Payload)) {
		return ControlReceipt{}, ErrOutboxCorrupt
	}
	r.Command = c
	if status == "completed" {
		result, err := digitaltwin.ParseMQTTResult("gateways/"+c.GatewayID+"/responses/"+c.CommandID, r.Result, false)
		if err != nil || digitaltwin.ValidateResultForCommand(result, c) != nil {
			return ControlReceipt{}, ErrOutboxCorrupt
		}
	} else if status != "prepared" || len(r.Result) != 0 {
		return ControlReceipt{}, ErrOutboxCorrupt
	}
	return r, nil
}

// createControlTables is shared by fresh creation and the schema-1 upgrade;
// callers commit the schema version and tables in the same transaction.
func createControlTables(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`CREATE TABLE sensor_configuration (
			id INTEGER PRIMARY KEY CHECK(id=1),
			sample_interval_us INTEGER NOT NULL CHECK(sample_interval_us BETWEEN 10000 AND 9007199254740992),
			desired_version INTEGER NOT NULL CHECK(desired_version>=0),
			applied_at TEXT NOT NULL
		)`,
		`CREATE TABLE command_receipts (
			command_id TEXT PRIMARY KEY,
			command_hash TEXT NOT NULL CHECK(length(command_hash)=64),
			command_topic TEXT NOT NULL,
			command_bytes BLOB NOT NULL CHECK(length(command_bytes) BETWEEN 1 AND 8192),
			desired_version INTEGER NOT NULL CHECK(desired_version>=0),
			sample_interval_us INTEGER NOT NULL CHECK(sample_interval_us BETWEEN 10000 AND 9007199254740992),
			expires_at INTEGER NOT NULL,
			status TEXT NOT NULL CHECK(status IN ('prepared','completed')),
			result_bytes BLOB,
			retain_until INTEGER NOT NULL,
			CHECK((status='prepared' AND result_bytes IS NULL) OR
			 (status='completed' AND length(result_bytes) BETWEEN 1 AND 8192 AND result_bytes IS NOT NULL))
		)`,
		`CREATE UNIQUE INDEX one_prepared_command ON command_receipts(status) WHERE status='prepared'`,
		`CREATE INDEX command_receipts_retention ON command_receipts(status,retain_until)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (o *Outbox) upgradeControlSchema(ctx context.Context) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := createControlTables(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version=2"); err != nil {
		return err
	}
	if err := o.commit(tx); err != nil {
		return fmt.Errorf("%w: %v", ErrOutboxCommitUnknown, err)
	}
	return nil
}
