package paperload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"iot-platform/internal/gatewaysim"
	"iot-platform/internal/telemetry"

	"github.com/google/uuid"
)

func e4TestBatch(t *testing.T, message uuid.UUID, gw, sensor string) gatewaysim.BuiltBatch {
	t.Helper()
	batch, err := gatewaysim.BuildSyntheticBatch(gatewaysim.BatchRequest{GatewayID: gw, SensorID: sensor, BootID: uuid.MustParse("00000000-0000-4000-8000-000000000001"), MessageID: message, FirstSequence: 7, MeasuredAt: time.Now().UTC().Add(-time.Minute), SampleCount: 1, SampleIntervalUS: 500000}, gatewaysim.ServerLimits(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func e4PrivateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestE4JournalExactByteRoundTripAndPrivateModes(t *testing.T) {
	dir := e4PrivateTempDir(t)
	path := filepath.Join(dir, "journal.jsonl")
	b := e4TestBatch(t, uuid.MustParse("00000000-0000-4000-8000-000000000002"), "gw-a", "s-a")
	j, err := CreateE4Journal(path, 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(b); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("journal mode=%v err=%v", st, err)
	}
	got, err := ReadE4Journal(path, 2, 1<<20, 1, []Target{{GatewayID: "gw-a", SensorID: "s-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Batch.Topic != b.Topic || string(got[0].Batch.Payload) != string(b.Payload) || got[0].Header.PayloadSHA256 == "" {
		t.Fatalf("roundtrip mismatch: %#v", got)
	}
	if strings.Contains(string(mustJSON(t, got[0].Header)), "payload\"") {
		t.Fatal("header summary exposed payload")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestE4JournalCapsExclusiveAndSymlink(t *testing.T) {
	dir := e4PrivateTempDir(t)
	path := filepath.Join(dir, "journal")
	b := e4TestBatch(t, uuid.MustParse("00000000-0000-4000-8000-000000000003"), "gw-a", "s-a")
	j, err := CreateE4Journal(path, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(b); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(b); err == nil || err.Error() != "e4_journal_message_cap" {
		t.Fatalf("cap err=%v", err)
	}
	_ = j.Close()
	if _, err := CreateE4Journal(path, 1, 1<<20); err == nil {
		t.Fatal("overwrote existing journal")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadE4Journal(link, 1, 1<<20, 1, []Target{{GatewayID: "gw-a", SensorID: "s-a"}}); err == nil {
		t.Fatal("followed symlink")
	}
	if _, err := CreateE4Journal(link, 1, 1<<20); err == nil {
		t.Fatal("created through symlink")
	}
	if _, err := CreateE4Journal(filepath.Join(dir, "bytecap"), 1, 1); err != nil {
		t.Fatal(err)
	}
}

func TestE4JournalRejectsHashMetadataAndScopeCorruption(t *testing.T) {
	for name, edit := range map[string]func(*e4JournalRecord){
		"hash":     func(r *e4JournalRecord) { r.PayloadSHA256 = "00" },
		"metadata": func(r *e4JournalRecord) { r.SensorID = "s-other" },
	} {
		t.Run(name, func(t *testing.T) {
			dir := e4PrivateTempDir(t)
			path := filepath.Join(dir, "j")
			b := e4TestBatch(t, uuid.MustParse("00000000-0000-4000-8000-000000000004"), "gw-a", "s-a")
			j, err := CreateE4Journal(path, 2, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Append(b); err != nil {
				t.Fatal(err)
			}
			_ = j.Close()
			// Rewrite using the internal record format to simulate corruption.
			r := recordFor(b)
			edit(&r)
			raw, _ := json.Marshal(r)
			if err = os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = ReadE4Journal(path, 2, 1<<20, 1, []Target{{GatewayID: "gw-a", SensorID: "s-a"}}); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
	dir := e4PrivateTempDir(t)
	path := filepath.Join(dir, "mixed")
	b := e4TestBatch(t, uuid.MustParse("00000000-0000-4000-8000-000000000005"), "gw-a", "s-a")
	j, _ := CreateE4Journal(path, 2, 1<<20)
	_ = j.Append(b)
	_ = j.Close()
	if _, err := ReadE4Journal(path, 2, 1<<20, 1, []Target{{GatewayID: "gw-b", SensorID: "s-a"}}); err == nil {
		t.Fatal("scope mismatch accepted")
	}
}

func recordFor(b gatewaysim.BuiltBatch) e4JournalRecord {
	sum := sha256.Sum256(b.Payload)
	return e4JournalRecord{Topic: b.Topic, GatewayID: b.Batch.GatewayID, SensorID: b.Batch.SensorID, MessageID: b.Batch.MessageID.String(), BootID: b.Batch.BootID.String(), FirstSequence: "7", SampleCount: b.Batch.SampleCount, PayloadSHA256: hex.EncodeToString(sum[:]), Payload: b.Payload}
}

func TestE4JournalByteCapBeforeAppendKeepsJournal(t *testing.T) {
	dir := e4PrivateTempDir(t)
	path := filepath.Join(dir, "j")
	b := e4TestBatch(t, uuid.MustParse("00000000-0000-4000-8000-000000000006"), "gw-a", "s-a")
	j, err := CreateE4Journal(path, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append(b); err == nil || err.Error() != "e4_journal_byte_cap" {
		t.Fatalf("cap err=%v", err)
	}
	_ = j.Close()
	if _, err = os.Stat(path); err != nil {
		t.Fatal("failed journal not retained", err)
	}
}

func TestE4JournalRejectsDuplicateIDWithDifferentPayload(t *testing.T) {
	dir := e4PrivateTempDir(t)
	path := filepath.Join(dir, "j")
	id := uuid.MustParse("00000000-0000-4000-8000-000000000007")
	a := e4TestBatch(t, id, "gw-a", "s-a")
	b := a
	// Payload mismatch is represented as a valid second payload using the same ID.
	var dto telemetry.BatchDTO
	if err := json.Unmarshal(a.Payload, &dto); err != nil {
		t.Fatal(err)
	}
	dto.FirstSequence++
	b.Payload, _ = json.Marshal(dto)
	b.Batch.FirstSequence = dto.FirstSequence
	j, err := CreateE4Journal(path, 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_ = j.Append(a)
	_ = j.Append(b)
	_ = j.Close()
	if _, err := ReadE4Journal(path, 2, 1<<20, 2, []Target{{GatewayID: "gw-a", SensorID: "s-a"}}); err == nil {
		t.Fatal("duplicate ID payload conflict accepted")
	}
}
