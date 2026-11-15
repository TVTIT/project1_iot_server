package gatewaysim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func TestControlSQLiteSchemaFresh(t *testing.T) {
	o := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	version, err := readUserVersion(context.Background(), o.db)
	if err != nil || version != 3 {
		t.Fatalf("schema = %d, %v; want durable control schema 3", version, err)
	}
	for _, table := range []string{"sensor_configuration", "command_receipts"} {
		var n int
		if err := o.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("fresh %s: count=%d err=%v", table, n, err)
		}
	}
	// Verify new receipt columns exist in command_receipts
	for _, col := range []string{"receipt_bytes", "receipt_hash", "receipt_received_at"} {
		var dummy any
		if err := o.db.QueryRow("SELECT " + col + " FROM command_receipts LIMIT 1").Scan(&dummy); err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("missing column %s: %v", col, err)
		}
	}
}

func controlTestCommand(t *testing.T, version digitaltwin.Version, interval int64) (string, []byte, digitaltwin.Command) {
	t.Helper()
	c := digitaltwin.Command{ProtocolVersion: 1, MessageType: "command", CommandID: uuid.NewString(), GatewayID: "gateway_001", EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001", Action: digitaltwin.ActionUpdateConfiguration,
		Parameters: digitaltwin.SamplingParameters{SampleIntervalUS: interval}, DesiredVersion: version, IssuedAt: outboxBaseTime, ExpiresAt: outboxBaseTime.Add(time.Minute)}
	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return "gateways/gateway_001/commands/" + c.CommandID, payload, c
}

func controlTestResult(t *testing.T, c digitaltwin.Command, status digitaltwin.CommandStatus) []byte {
	t.Helper()
	r := digitaltwin.CommandResult{ProtocolVersion: 1, MessageType: "command_result", CommandID: c.CommandID, GatewayID: c.GatewayID, EntityID: c.EntityID, DesiredVersion: c.DesiredVersion, Status: status, CompletedAt: c.IssuedAt.Add(time.Second)}
	if status == digitaltwin.CommandSucceeded {
		r.ReportedState = &c.Parameters
	} else {
		r.ErrorCode = "APPLY_FAILED"
	}
	payload, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestControlStorePreparedCompletedReopen(t *testing.T) {
	ctx := context.Background()
	cfg := testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"))
	o := openTestOutbox(t, cfg)
	topic, payload, command := controlTestCommand(t, 1, 16667)
	receipt, err := o.ReserveControl(ctx, topic, payload, outboxBaseTime)
	if err != nil || !receipt.Fresh || !bytes.Equal(receipt.Payload, payload) || len(receipt.Result) != 0 {
		t.Fatalf("reserve fresh=%v err=%v", receipt.Fresh, err)
	}
	if _, found, err := o.Configuration(ctx); err != nil || found {
		t.Fatalf("prepared config is confirmed: found=%v err=%v", found, err)
	}
	receipt, err = o.ReserveControl(ctx, topic, payload, outboxBaseTime)
	if err != nil || receipt.Fresh {
		t.Fatalf("duplicate fresh=%v err=%v", receipt.Fresh, err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = openTestOutbox(t, cfg)
	receipts, err := o.ControlReceipts(ctx)
	if err != nil || len(receipts) != 1 || !bytes.Equal(receipts[0].Payload, payload) {
		t.Fatalf("prepared reopen count=%d err=%v", len(receipts), err)
	}
	result := controlTestResult(t, command, digitaltwin.CommandSucceeded)
	if err := o.CompleteControl(ctx, command.CommandID, result, outboxBaseTime.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.CompleteControl(ctx, command.CommandID, result, outboxBaseTime.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = openTestOutbox(t, cfg)
	configuration, found, err := o.Configuration(ctx)
	if err != nil || !found || configuration.SampleIntervalUS != 16667 || configuration.DesiredVersion != 1 {
		t.Fatalf("configuration=%+v found=%v err=%v", configuration, found, err)
	}
	receipt, err = o.ReserveControl(ctx, topic, payload, outboxBaseTime.Add(2*time.Hour))
	if err != nil || receipt.Fresh || !bytes.Equal(receipt.Result, result) {
		t.Fatalf("expired duplicate changed result: fresh=%v err=%v", receipt.Fresh, err)
	}
	conflict := bytes.Replace(payload, []byte("16667"), []byte("20000"), 1)
	if _, err := o.ReserveControl(ctx, topic, conflict, outboxBaseTime); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("ID conflict err=%v", err)
	}
	if err := o.CompleteControl(ctx, command.CommandID, controlTestResult(t, command, digitaltwin.CommandFailed), outboxBaseTime); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("terminal conflict err=%v", err)
	}
}

func TestControlStoreCapacityRetentionAndRollback(t *testing.T) {
	ctx := context.Background()
	o := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	topic, payload, c := controlTestCommand(t, 1, 10000)
	if _, err := o.ReserveControl(ctx, topic, payload, outboxBaseTime); err != nil {
		t.Fatal(err)
	}
	topic2, payload2, _ := controlTestCommand(t, 2, 20000)
	if _, err := o.ReserveControl(ctx, topic2, payload2, outboxBaseTime); !errors.Is(err, ErrControlBusy) {
		t.Fatalf("prepared serialization: %v", err)
	}
	commit := o.commit
	o.commit = func(*sql.Tx) error { return errors.New("injected finalize failure") }
	if err := o.CompleteControl(ctx, c.CommandID, controlTestResult(t, c, digitaltwin.CommandSucceeded), outboxBaseTime.Add(time.Second)); err == nil {
		t.Fatal("persist failure reported success")
	}
	o.commit = commit
	if _, found, err := o.Configuration(ctx); found || err != nil {
		t.Fatalf("rollback config: %v/%v", found, err)
	}
	if err := o.CompleteControl(ctx, c.CommandID, controlTestResult(t, c, digitaltwin.CommandSucceeded), outboxBaseTime.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Fill the fixed quota using failed results, which never change config.
	for i := 1; i < controlMaxReceipts; i++ {
		topic, payload, c := controlTestCommand(t, digitaltwin.Version(i+1), 20000)
		if _, err := o.ReserveControl(ctx, topic, payload, outboxBaseTime); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		if err := o.CompleteControl(ctx, c.CommandID, controlTestResult(t, c, digitaltwin.CommandFailed), outboxBaseTime.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.ReserveControl(ctx, topic2, payload2, outboxBaseTime); !errors.Is(err, ErrControlFull) {
		t.Fatalf("quota: %v", err)
	}
	if _, err := o.ReserveControl(ctx, topic2, payload2, c.ExpiresAt.Add(controlRetention+time.Second)); err != nil {
		t.Fatalf("retention: %v", err)
	}
	configuration, found, err := o.Configuration(ctx)
	if err != nil || !found || configuration.DesiredVersion != 1 || configuration.SampleIntervalUS != 10000 {
		t.Fatalf("watermark lost: %+v/%v/%v", configuration, found, err)
	}
}

func TestControlSQLiteUpgradePreservesUplink(t *testing.T) {
	cfg := testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"))
	o := openTestOutbox(t, cfg)
	before := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)
	controlSchemaOne(t, o)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = openTestOutbox(t, cfg)
	version, err := readUserVersion(context.Background(), o.db)
	if err != nil || version != 3 {
		t.Fatalf("upgrade version=%d err=%v", version, err)
	}
	after, found, err := o.Lookup(context.Background(), outboxMessageID)
	if err != nil || !found || !bytes.Equal(before.Payload, after.Payload) || before.Topic != after.Topic || before.FirstSequence != after.FirstSequence || before.Status != after.Status || before.ByteSize != after.ByteSize {
		t.Fatalf("uplink changed across migration: found=%v err=%v", found, err)
	}
	if got := readStoredSequence(t, o, outboxBootID); got != 100 {
		t.Fatalf("sequence watermark=%d", got)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	admin, err := OpenOutboxAdmin(cfg.Path, cfg.OpTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	if changed, err := admin.Quarantine(context.Background(), outboxMessageID, "test migration preservation"); err != nil || !changed {
		t.Fatalf("admin quarantine: changed=%v err=%v", changed, err)
	}
}

func TestControlSQLiteUpgradeRollback(t *testing.T) {
	o := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	controlSchemaOne(t, o)
	o.commit = func(*sql.Tx) error { return errors.New("injected migration commit failure") }
	if err := o.prepare(context.Background()); err == nil {
		t.Fatal("migration ignored commit failure")
	}
	version, err := readUserVersion(context.Background(), o.db)
	if err != nil || version != 1 {
		t.Fatalf("rollback version=%d err=%v", version, err)
	}
	var tables int
	if err := o.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name IN ('sensor_configuration','command_receipts')").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("partial migration tables=%d err=%v", tables, err)
	}
}

func TestControlSQLiteAdminLegacyAndQuarantinedUpgrade(t *testing.T) {
	ctx := context.Background()
	cfg := testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"))
	o := openTestOutbox(t, cfg)
	before := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)
	controlSchemaOne(t, o)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	admin, err := OpenOutboxAdmin(cfg.Path, cfg.OpTimeout)
	if err != nil {
		t.Fatalf("legacy admin must remain usable without upgrade: %v", err)
	}
	if changed, err := admin.Quarantine(ctx, outboxMessageID, "upgrade test"); err != nil || !changed {
		t.Fatalf("quarantine: %v/%v", changed, err)
	}
	version, err := readUserVersion(ctx, admin.db)
	if err != nil || version != 1 {
		t.Fatalf("admin must not migrate: %d/%v", version, err)
	}
	if err := admin.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := cfg
	wrong.SensorID = "another_sensor"
	if _, err := OpenOutbox(wrong); !errors.Is(err, ErrOutboxIdentity) {
		t.Fatalf("foreign upgrade: %v", err)
	}
	o = openTestOutbox(t, cfg)
	got, found, err := o.Lookup(ctx, outboxMessageID)
	if err != nil || !found || got.Status != outboxStatusQuarantined || !bytes.Equal(got.Payload, before.Payload) || got.ByteSize != before.ByteSize {
		t.Fatalf("quarantined upgrade: found=%v err=%v", found, err)
	}
	counts, err := o.Counts(ctx)
	if err != nil || counts.QuarantinedRows != 1 || counts.QuarantinedBytes != before.ByteSize {
		t.Fatalf("quota preservation: %+v/%v", counts, err)
	}
}

func TestControlStoreByteQuotaAndPreparedRetention(t *testing.T) {
	ctx := context.Background()
	o := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	pad := func(b []byte) []byte {
		return append(b, bytes.Repeat([]byte(" "), digitaltwin.MaxControlPayloadBytes-len(b))...)
	}
	full := false
	for i := 0; i < controlMaxReceipts; i++ {
		topic, payload, c := controlTestCommand(t, digitaltwin.Version(i+1), 10000)
		_, err := o.ReserveControl(ctx, topic, pad(payload), outboxBaseTime)
		if errors.Is(err, ErrControlFull) {
			if i >= controlMaxReceipts {
				t.Fatal("row quota hid byte quota")
			}
			full = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := o.CompleteControl(ctx, c.CommandID, pad(controlTestResult(t, c, digitaltwin.CommandFailed)), outboxBaseTime.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if !full {
		t.Fatal("byte quota was not enforced before row quota")
	}
	var used int64
	if err := o.db.QueryRow("SELECT sum(length(command_topic)+length(command_bytes)+length(result_bytes)) FROM command_receipts").Scan(&used); err != nil || used > controlMaxBytes {
		t.Fatalf("bytes=%d err=%v", used, err)
	}
	// New admission can clean old completed rows but must retain prepared work.
	topic, payload, c := controlTestCommand(t, 1000, 20000)
	now := c.ExpiresAt.Add(controlRetention + time.Second)
	if _, err := o.ReserveControl(ctx, topic, payload, now); err != nil {
		t.Fatal(err)
	}
	topic, payload, _ = controlTestCommand(t, 1001, 30000)
	if _, err := o.ReserveControl(ctx, topic, payload, now.Add(2*controlRetention)); !errors.Is(err, ErrControlBusy) {
		t.Fatalf("prepared evicted: %v", err)
	}
	r, err := o.ControlReceipts(ctx)
	if err != nil || len(r) != 1 || r[0].Command.CommandID != c.CommandID {
		t.Fatalf("prepared retention: %d/%v", len(r), err)
	}
}

func controlSchemaOne(t *testing.T, o *Outbox) {
	t.Helper()
	for _, stmt := range []string{"DROP TABLE IF EXISTS command_receipts", "DROP TABLE IF EXISTS sensor_configuration", "PRAGMA user_version=1"} {
		if _, err := o.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}

func controlSchemaTwo(t *testing.T, o *Outbox) {
	t.Helper()
	stmts := []string{
		"DROP TABLE IF EXISTS command_receipts",
		"DROP TABLE IF EXISTS sensor_configuration",
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
		"CREATE UNIQUE INDEX one_prepared_command ON command_receipts(status) WHERE status='prepared'",
		"CREATE INDEX command_receipts_retention ON command_receipts(status,retain_until)",
		"PRAGMA user_version=2",
	}
	for _, stmt := range stmts {
		if _, err := o.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}

func controlTestWireReceipt(t *testing.T, c digitaltwin.Command, commandPayload []byte, receivedAt time.Time) []byte {
	t.Helper()
	cmdHash := fmt.Sprintf("%x", sha256.Sum256(commandPayload))
	receipt := struct {
		ProtocolVersion int                 `json:"protocol_version"`
		MessageType     string              `json:"message_type"`
		CommandID       string              `json:"command_id"`
		GatewayID       string              `json:"gateway_id"`
		EntityID        string              `json:"entity_id"`
		DesiredVersion  digitaltwin.Version `json:"desired_version"`
		ReceivedAt      time.Time           `json:"received_at"`
		CommandHash     string              `json:"command_hash"`
	}{
		ProtocolVersion: 1,
		MessageType:     "command_receipt",
		CommandID:       c.CommandID,
		GatewayID:       c.GatewayID,
		EntityID:        c.EntityID,
		DesiredVersion:  c.DesiredVersion,
		ReceivedAt:      receivedAt.UTC(),
		CommandHash:     cmdHash,
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// scanControlReceiptScan builds a scan func matching the 8 destination columns of controlReceiptSelect:
// command_topic, command_bytes, command_hash, status, result_bytes, receipt_bytes, receipt_hash, receipt_received_at
func scanControlReceiptScan(topic string, commandBytes []byte, commandHash, status string, resultBytes, receiptBytes []byte, receiptHash string, receiptReceivedAt int64, dest []any) error {
	if len(dest) != 8 {
		return fmt.Errorf("scanControlReceiptScan: expected 8 destinations, got %d", len(dest))
	}
	// dest[0] = *string (topic)
	// dest[1] = *[]byte (command_bytes)
	// dest[2] = *string (command_hash)
	// dest[3] = *string (status)
	// dest[4] = *[]byte (result_bytes)
	// dest[5] = *[]byte (receipt_bytes)
	// dest[6] = *sql.NullString (receipt_hash)
	// dest[7] = *sql.NullInt64 (receipt_received_at)
	if p, ok := dest[0].(*string); ok {
		*p = topic
	}
	if p, ok := dest[1].(*[]byte); ok {
		*p = commandBytes
	}
	if p, ok := dest[2].(*string); ok {
		*p = commandHash
	}
	if p, ok := dest[3].(*string); ok {
		*p = status
	}
	if p, ok := dest[4].(*[]byte); ok {
		*p = resultBytes
	}
	if p, ok := dest[5].(*[]byte); ok {
		*p = receiptBytes
	}
	if p, ok := dest[6].(*sql.NullString); ok {
		p.String = receiptHash
		p.Valid = true
	}
	if p, ok := dest[7].(*sql.NullInt64); ok {
		p.Int64 = receiptReceivedAt
		p.Valid = true
	}
	return nil
}

func TestControlSQLiteUpgradeSchema2To3(t *testing.T) {
	ctx := context.Background()
	cfg := testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"))
	o := openTestOutbox(t, cfg)
	beforeUplink := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)
	controlSchemaTwo(t, o)

	// Seed representative schema 2 state:
	// 1. Confirmed sensor configuration
	appliedTime := outboxBaseTime.UTC().Format(time.RFC3339Nano)
	if _, err := o.db.Exec(`INSERT INTO sensor_configuration(id, sample_interval_us, desired_version, applied_at) VALUES (1, 50000, 2, ?)`, appliedTime); err != nil {
		t.Fatal(err)
	}
	// 2. Completed command with result
	topic1, payload1, c1 := controlTestCommand(t, 2, 50000)
	result1 := controlTestResult(t, c1, digitaltwin.CommandSucceeded)
	h1 := fmt.Sprintf("%x", sha256.Sum256(payload1))
	if _, err := o.db.Exec(`INSERT INTO command_receipts(command_id, command_hash, command_topic, command_bytes, desired_version, sample_interval_us, expires_at, status, result_bytes, retain_until)
		VALUES (?, ?, ?, ?, 2, 50000, ?, 'completed', ?, ?)`,
		c1.CommandID, h1, topic1, payload1, c1.ExpiresAt.UnixMicro(), result1, c1.ExpiresAt.Add(controlRetention).UnixMicro()); err != nil {
		t.Fatal(err)
	}
	// 3. Prepared command without result
	topicPrep, payloadPrep, cPrep := controlTestCommand(t, 3, 25000)
	hPrep := fmt.Sprintf("%x", sha256.Sum256(payloadPrep))
	if _, err := o.db.Exec(`INSERT INTO command_receipts(command_id, command_hash, command_topic, command_bytes, desired_version, sample_interval_us, expires_at, status, result_bytes, retain_until)
		VALUES (?, ?, ?, ?, 3, 25000, ?, 'prepared', NULL, ?)`,
		cPrep.CommandID, hPrep, topicPrep, payloadPrep, cPrep.ExpiresAt.UnixMicro(), cPrep.ExpiresAt.Add(controlRetention).UnixMicro()); err != nil {
		t.Fatal(err)
	}

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen to trigger upgrade from schema 2 to schema 3
	o = openTestOutbox(t, cfg)
	version, err := readUserVersion(ctx, o.db)
	if err != nil || version != 3 {
		t.Fatalf("upgraded version=%d err=%v, want 3", version, err)
	}

	// Verify uplink row preserved exactly
	afterUplink, found, err := o.Lookup(ctx, outboxMessageID)
	if err != nil || !found || !bytes.Equal(beforeUplink.Payload, afterUplink.Payload) || beforeUplink.Topic != afterUplink.Topic {
		t.Fatalf("uplink changed across upgrade: found=%v err=%v", found, err)
	}

	// Verify configuration preserved exactly
	cfgRead, found, err := o.Configuration(ctx)
	if err != nil || !found || cfgRead.SampleIntervalUS != 50000 || cfgRead.DesiredVersion != 2 {
		t.Fatalf("configuration changed across upgrade: %+v, found=%v, err=%v", cfgRead, found, err)
	}

	// Verify both completed and prepared receipts preserved without new evidence (receipt_bytes is nil)
	receipts, err := o.ControlReceipts(ctx)
	if err != nil || len(receipts) != 2 {
		t.Fatalf("receipts count=%d err=%v", len(receipts), err)
	}
	// receipts[0] is c1 (completed)
	if receipts[0].Command.CommandID != c1.CommandID || !bytes.Equal(receipts[0].Payload, payload1) || !bytes.Equal(receipts[0].Result, result1) {
		t.Fatalf("completed receipt changed across upgrade")
	}
	if len(receipts[0].ReceiptBytes) != 0 || receipts[0].ReceiptHash != "" || !receipts[0].ReceivedAt.IsZero() {
		t.Fatalf("legacy completed receipt must have empty receipt evidence: %+v", receipts[0])
	}
	// receipts[1] is cPrep (prepared)
	if receipts[1].Command.CommandID != cPrep.CommandID || !bytes.Equal(receipts[1].Payload, payloadPrep) || len(receipts[1].Result) != 0 {
		t.Fatalf("prepared receipt changed across upgrade")
	}
	if len(receipts[1].ReceiptBytes) != 0 || receipts[1].ReceiptHash != "" || !receipts[1].ReceivedAt.IsZero() {
		t.Fatalf("legacy prepared receipt must have empty receipt evidence: %+v", receipts[1])
	}

	// A duplicate on legacy receipt with supplied wire bytes returns stored nil evidence without fabricating:
	wireForPrep := controlTestWireReceipt(t, cPrep, payloadPrep, outboxBaseTime)
	dupLegacy, err := o.ReserveControlWithReceipt(ctx, topicPrep, payloadPrep, wireForPrep, outboxBaseTime)
	if err != nil || dupLegacy.Fresh || len(dupLegacy.ReceiptBytes) != 0 {
		t.Fatalf("duplicate on legacy receipt must return stored without fabrication: fresh=%v, len(bytes)=%d, err=%v", dupLegacy.Fresh, len(dupLegacy.ReceiptBytes), err)
	}

	// Parity check: Upgraded DB must reject partial receipt (receipt_bytes present, receipt_hash NULL) at write time via trigger.
	// Use the EXISTING completed c1 row (which already has valid result_bytes) to isolate the cross-column receipt evidence trigger
	// from the completed-result CHECK constraint.
	partialReceiptBytes := []byte(`{"protocol_version":1,"message_type":"command_receipt","command_id":"` + c1.CommandID + `","gateway_id":"gateway_001","entity_id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","desired_version":2,"received_at":"2026-01-01T00:00:00Z","command_hash":"` + h1 + `"}`)
	_, err = o.db.Exec(`UPDATE command_receipts SET receipt_bytes = ?, receipt_hash = NULL, receipt_received_at = 123456 WHERE command_id = ?`,
		partialReceiptBytes, c1.CommandID)
	if err == nil || !strings.Contains(err.Error(), "command_receipts receipt evidence must be all-null or complete") {
		t.Fatalf("upgraded DB allowed partial receipt at write or wrong error: %v", err)
	}

	// Defense-in-depth: even if trigger is bypassed, scanControlReceipt must fail closed on partial evidence.
	// Drop trigger to allow partial state to be written, then verify scanControlReceipt enforces it.
	if _, err = o.db.Exec(`DROP TRIGGER IF EXISTS command_receipts_evidence_update`); err != nil {
		t.Fatalf("drop trigger failed: %v", err)
	}
	if _, err = o.db.Exec(`UPDATE command_receipts SET receipt_bytes = ?, receipt_hash = NULL, receipt_received_at = 123456 WHERE command_id = ?`,
		partialReceiptBytes, c1.CommandID); err != nil {
		t.Fatalf("setup UPDATE for partial receipt failed after trigger drop: %v", err)
	}
	if _, err = o.ControlReceipts(ctx); err == nil || !errors.Is(err, ErrOutboxCorrupt) {
		t.Fatalf("upgraded DB allowed partial receipt at read or wrong error: %v", err)
	}

	// Corrupt receipt scan detection: if receipt_hash doesn't match sha256(receipt_bytes), scanControlReceipt fails closed with ErrOutboxCorrupt
	_, err = o.db.Exec(`UPDATE command_receipts SET receipt_bytes = X'1234', receipt_hash = ?, receipt_received_at = 123456 WHERE command_id = ?`,
		fmt.Sprintf("%x", sha256.Sum256([]byte("wrong-hash-source"))), c1.CommandID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.ControlReceipts(ctx); !errors.Is(err, ErrOutboxCorrupt) {
		t.Fatalf("scanControlReceipt on corrupt receipt_hash err=%v, want ErrOutboxCorrupt", err)
	}
}

func TestControlStoreReserveWithReceipt(t *testing.T) {
	ctx := context.Background()
	cfg := testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"))
	o := openTestOutbox(t, cfg)
	topic, payload, c := controlTestCommand(t, 1, 20000)
	now := outboxBaseTime
	wireReceipt := controlTestWireReceipt(t, c, payload, now)

	// 1. Oversize receipt (>8192 bytes) rejected at API boundary
	apiOversizeReceipt := append(bytes.Repeat([]byte(" "), 8193), wireReceipt...)
	if _, err := o.ReserveControlWithReceipt(ctx, topic, payload, apiOversizeReceipt, now); err == nil {
		t.Fatal("oversize receipt accepted")
	}

	// 2. Reserve with valid receipt
	receipt, err := o.ReserveControlWithReceipt(ctx, topic, payload, wireReceipt, now)
	if err != nil || !receipt.Fresh || !bytes.Equal(receipt.ReceiptBytes, wireReceipt) {
		t.Fatalf("reserve with receipt fresh=%v err=%v", receipt.Fresh, err)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(wireReceipt))
	if receipt.ReceiptHash != expectedHash {
		t.Fatalf("receipt hash=%s, want %s", receipt.ReceiptHash, expectedHash)
	}
	if !receipt.ReceivedAt.Equal(now.UTC()) {
		t.Fatalf("receipt received_at=%v, want %v", receipt.ReceivedAt, now.UTC())
	}

	// 3. Reopen: verify receipt evidence preserved durably across reopen
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = openTestOutbox(t, cfg)
	receipts, err := o.ControlReceipts(ctx)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("reopened count=%d err=%v", len(receipts), err)
	}
	if !bytes.Equal(receipts[0].ReceiptBytes, wireReceipt) || receipts[0].ReceiptHash != expectedHash {
		t.Fatalf("reopened receipt evidence mismatch: %+v", receipts[0])
	}

	// 4. Duplicate reserve with same receipt bytes is accepted
	dupReceipt, err := o.ReserveControlWithReceipt(ctx, topic, payload, wireReceipt, now)
	if err != nil || dupReceipt.Fresh || !bytes.Equal(dupReceipt.ReceiptBytes, wireReceipt) {
		t.Fatalf("duplicate reserve fresh=%v err=%v", dupReceipt.Fresh, err)
	}

	// 5. Duplicate reserve with conflicting receipt bytes is rejected
	conflictingReceipt := controlTestWireReceipt(t, c, payload, now.Add(time.Second))
	if _, err := o.ReserveControlWithReceipt(ctx, topic, payload, conflictingReceipt, now); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("conflicting receipt err=%v, want ErrControlConflict", err)
	}

	// Complete command c so sensor is free for next command reservation:
	if err := o.CompleteControl(ctx, c.CommandID, controlTestResult(t, c, digitaltwin.CommandSucceeded), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// 6. Injected commit failure fails closed with ErrOutboxCommitUnknown
	commit := o.commit
	o.commit = func(*sql.Tx) error { return errors.New("injected commit failure") }
	topicCommitFail, payloadCommitFail, cCommitFail := controlTestCommand(t, 2, 30000)
	wireCommitFail := controlTestWireReceipt(t, cCommitFail, payloadCommitFail, now)
	if _, err := o.ReserveControlWithReceipt(ctx, topicCommitFail, payloadCommitFail, wireCommitFail, now); !errors.Is(err, ErrOutboxCommitUnknown) {
		t.Fatalf("injected commit failure err=%v, want ErrOutboxCommitUnknown", err)
	}
	o.commit = commit

	// 7. Oversize receipt blob (>8192) in DB causes scanControlReceipt to fail closed with ErrOutboxCorrupt
	// Build a VALID wire receipt JSON, then pad with legal whitespace to EXACTLY 8193 bytes.
	// This ensures only the length guard is violated; JSON/hash/identity/time all pass.
	baseWire := controlTestWireReceipt(t, c, payload, now)
	if len(baseWire) >= digitaltwin.MaxControlPayloadBytes {
		t.Fatalf("base wire receipt %d bytes too large for padding test", len(baseWire))
	}
	// Pad with spaces (valid JSON whitespace) to exactly 8193 bytes
	oversizeReceipt := append(baseWire, bytes.Repeat([]byte(" "), digitaltwin.MaxControlPayloadBytes+1-len(baseWire))...)
	if len(oversizeReceipt) != digitaltwin.MaxControlPayloadBytes+1 {
		t.Fatalf("oversize receipt length %d, want %d", len(oversizeReceipt), digitaltwin.MaxControlPayloadBytes+1)
	}
	oversizeHash := fmt.Sprintf("%x", sha256.Sum256(oversizeReceipt))

	// Direct scanControlReceipt stub: supplies all 8 destination columns with otherwise-valid data.
	// Only invariant violated: len(receiptBytes) == 8193 > 8192.
	scanOversize := func(dest ...any) error {
		topic := "gateways/gateway_001/commands/" + c.CommandID
		cmdHash := fmt.Sprintf("%x", sha256.Sum256(payload))
		receiptReceivedAt := now.UnixMicro()
		return scanControlReceiptScan(topic, payload, cmdHash, "completed", controlTestResult(t, c, digitaltwin.CommandSucceeded), oversizeReceipt, oversizeHash, receiptReceivedAt, dest)
	}
	var scanErr error
	_, scanErr = scanControlReceipt(scanOversize)
	if !errors.Is(scanErr, ErrOutboxCorrupt) {
		t.Fatalf("scanControlReceipt on oversize receipt err=%v, want ErrOutboxCorrupt", scanErr)
	}

	// 8. Valid boundary: same receipt padded to EXACTLY 8192 bytes must scan successfully.
	validReceipt := append(baseWire, bytes.Repeat([]byte(" "), digitaltwin.MaxControlPayloadBytes-len(baseWire))...)
	if len(validReceipt) != digitaltwin.MaxControlPayloadBytes {
		t.Fatalf("valid receipt length %d, want %d", len(validReceipt), digitaltwin.MaxControlPayloadBytes)
	}
	validHash := fmt.Sprintf("%x", sha256.Sum256(validReceipt))
	scanValid := func(dest ...any) error {
		topic := "gateways/gateway_001/commands/" + c.CommandID
		cmdHash := fmt.Sprintf("%x", sha256.Sum256(payload))
		receiptReceivedAt := now.UnixMicro()
		return scanControlReceiptScan(topic, payload, cmdHash, "completed", controlTestResult(t, c, digitaltwin.CommandSucceeded), validReceipt, validHash, receiptReceivedAt, dest)
	}
	if _, scanErr = scanControlReceipt(scanValid); scanErr != nil {
		t.Fatalf("scanControlReceipt on valid 8192-byte receipt failed: %v", scanErr)
	}
}
