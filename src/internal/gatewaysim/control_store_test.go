package gatewaysim

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func TestControlSQLiteSchemaFresh(t *testing.T) {
	o := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	version, err := readUserVersion(context.Background(), o.db)
	if err != nil || version != 2 {
		t.Fatalf("schema = %d, %v; want durable control schema 2", version, err)
	}
	for _, table := range []string{"sensor_configuration", "command_receipts"} {
		var n int
		if err := o.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("fresh %s: count=%d err=%v", table, n, err)
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

// A schema-1 fixture is constructed by removing only the new control tables;
// all uplink rows, counters, identity and quotas still use the existing helper.
func controlSchemaOne(t *testing.T, o *Outbox) {
	t.Helper()
	for _, stmt := range []string{"DROP TABLE IF EXISTS command_receipts", "DROP TABLE IF EXISTS sensor_configuration", "PRAGMA user_version=1"} {
		if _, err := o.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
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
	if err != nil || version != 2 {
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
