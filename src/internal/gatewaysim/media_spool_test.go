package gatewaysim

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"iot-platform/internal/mediaprotocol"
)

func mediaSpoolConfig(t *testing.T) MediaSpoolConfig {
	t.Helper()
	dir := t.TempDir()
	spool := filepath.Join(dir, "spool")
	if err := os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	return MediaSpoolConfig{DBPath: filepath.Join(dir, "media.db"), Dir: spool, GatewayID: "gateway_001", MaxFiles: 2, MaxBytes: 160000000, MaxAttempts: 3, OpTimeout: time.Second}
}

func TestMediaSpoolIngestPersistsStableRecordAndSourceReceipt(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	source := filepath.Join(t.TempDir(), "clip.mp4")
	data := []byte("finalized media bytes")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.IngestFinalized(context.Background(), "upload_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	second, err := s.IngestFinalized(context.Background(), "upload_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if first.Path != second.Path || first.SHA256 != second.SHA256 || first.SizeBytes != second.SizeBytes ||
		!first.Deadline.Equal(second.Deadline) || second.SHA256 != fmtHash(data) {
		t.Fatalf("record changed across resume: first=%+v second=%+v", first, second)
	}
	opened, err := s.OpenFile(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(opened.Name())
	opened.Close()
	if string(got) != string(data) {
		t.Fatalf("spooled bytes = %q", got)
	}
}

func TestMediaSpoolRejectsUnsafeInputAndIncompleteFiles(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	s, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	source := filepath.Join(t.TempDir(), "clip")
	if err := os.WriteFile(source, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, filepath.Dir(source), "/dev/null"} {
		if _, err := s.IngestFinalized(context.Background(), "upload_2", path, time.Now()); err == nil {
			t.Fatalf("accepted unsafe input %q", path)
		}
	}
	for _, id := range []string{"../escape", "bad/name"} {
		if _, err := s.IngestFinalized(context.Background(), id, source, time.Now()); err == nil {
			t.Fatalf("accepted unsafe ID %q", id)
		}
	}
	oversized := filepath.Join(t.TempDir(), "oversized")
	f, err := os.Create(oversized)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(mediaSpoolMaxUploadBytes + 1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := s.IngestFinalized(context.Background(), "upload_large", oversized, time.Now()); err == nil {
		t.Fatal("accepted file above fixed size cap")
	}
	rows, _ := s.List(context.Background(), time.Now())
	if len(rows) != 0 {
		t.Fatalf("invalid inputs published %d rows", len(rows))
	}
}

func TestMediaSpoolQuotaTransitionsAndReadyCorrelation(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	cfg.MaxFiles, cfg.MaxBytes = 2, 32
	s, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	source := filepath.Join(t.TempDir(), "clip")
	_ = os.WriteFile(source, []byte("small video"), 0600)
	r, err := s.IngestFinalized(context.Background(), "upload_3", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	secondSource := filepath.Join(t.TempDir(), "second")
	_ = os.WriteFile(secondSource, []byte("other"), 0600)
	ready, err := s.IngestFinalized(context.Background(), "upload_4", secondSource, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	thirdSource := filepath.Join(t.TempDir(), "third")
	_ = os.WriteFile(thirdSource, []byte("three"), 0600)
	if _, err := s.IngestFinalized(context.Background(), "upload_6", thirdSource, time.Now()); !errors.Is(err, ErrMediaSpoolFull) {
		t.Fatalf("quota error = %v", err)
	}
	if err := s.RecordAttempt(context.Background(), r.UploadID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGranted(context.Background(), r.UploadID, "33333333-3333-4333-8333-333333333333", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	granted, _ := s.Get(context.Background(), r.UploadID)
	if err := s.MarkPutMayHaveStarted(context.Background(), r.UploadID, granted.GrantExpires); err != nil {
		t.Fatal(err)
	}
	if err := s.recordResult(context.Background(), "other_id", "33333333-3333-4333-8333-333333333333", "ready", "", time.Now()); err == nil {
		t.Fatal("uncorrelated ready accepted")
	}
	if err := s.recordResult(context.Background(), r.UploadID, "different_media", "ready", "", time.Now()); err == nil {
		t.Fatal("mismatched media_id accepted")
	}
	if err := s.recordResult(context.Background(), r.UploadID, "33333333-3333-4333-8333-333333333333", "rejected", "UPLOAD_REJECTED", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.Path); err != nil {
		t.Fatalf("terminal failure removed file: %v", err)
	}
	if err := s.RecordAttempt(context.Background(), ready.UploadID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGranted(context.Background(), ready.UploadID, "44444444-4444-4444-8444-444444444444", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyMQTTResult(context.Background(), readyPayload(ready.UploadID, "44444444-4444-4444-8444-444444444444", "gateway_001"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ready.Path); !os.IsNotExist(err) {
		t.Fatalf("ready did not unlink file: %v", err)
	}
	if err := s.ApplyMQTTResult(context.Background(), readyPayload(ready.UploadID, "44444444-4444-4444-8444-444444444444", "gateway_001"), time.Now()); err != nil {
		t.Fatalf("ready replay: %v", err)
	}
}

func TestMediaSpoolResumesAttemptAndReadyCleanup(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	cfg.MaxAttempts = 1
	source := filepath.Join(t.TempDir(), "clip")
	if err := os.WriteFile(source, make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.IngestFinalized(context.Background(), "upload_resume", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(time.Minute)
	if err := s.RecordAttempt(context.Background(), r.UploadID, due); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAttempt(context.Background(), r.UploadID, due); !errors.Is(err, ErrMediaSpoolAttempts) {
		t.Fatalf("attempt cap error=%v", err)
	}
	if err := s.MarkGranted(context.Background(), r.UploadID, "55555555-5555-4555-8555-555555555555", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	granted, _ := s.Get(context.Background(), r.UploadID)
	if err := s.MarkPutMayHaveStarted(context.Background(), r.UploadID, granted.GrantExpires); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAwaitingResult(context.Background(), r.UploadID, due); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get(context.Background(), r.UploadID)
	if err != nil || got.Phase != "awaiting_result" || got.MediaID != "55555555-5555-4555-8555-555555555555" || got.Attempts != 1 || got.Path != r.Path {
		t.Fatalf("resumed record=%+v err=%v", got, err)
	}
	if err := s.ApplyMQTTResult(context.Background(), readyPayload(got.UploadID, got.MediaID, "gateway_001"), time.Now()); err != nil {
		t.Fatal(err)
	}
	// Simulate unlink not having been confirmed after the ready commit.
	if err := os.WriteFile(got.Path, []byte("left by interrupted cleanup"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := os.Lstat(got.Path); !os.IsNotExist(err) {
		t.Fatalf("ready cleanup did not resume: %v", err)
	}
}

func TestMediaSpoolGrantRequiresFreshRequestAndReadyReplayChecksStoredName(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	source := filepath.Join(t.TempDir(), "fresh.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	record, err := spool.IngestFinalized(context.Background(), "fresh_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkGranted(context.Background(), record.UploadID, "33333333-3333-4333-8333-333333333333", time.Now().Add(time.Minute)); err == nil {
		t.Fatal("grant accepted before request admission")
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	mediaID := "33333333-3333-4333-8333-333333333333"
	if err := spool.MarkGranted(context.Background(), record.UploadID, mediaID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	granted, _ := spool.Get(context.Background(), record.UploadID)
	if err := spool.MarkPutMayHaveStarted(context.Background(), record.UploadID, granted.GrantExpires); err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkGranted(context.Background(), record.UploadID, mediaID, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("stale grant reopened a possible PUT")
	}
	payload := readyPayload(record.UploadID, mediaID, "gateway_001")
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_other"), payload, time.Now()); err == nil {
		t.Fatal("wrong own-result topic accepted")
	}
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_001"), payload, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record.Path, []byte("post-ready leftover"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = spool.db.Exec("UPDATE media_spool SET spool_name='outside.mp4' WHERE upload_id=?", record.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_001"), payload, time.Now()); err == nil {
		t.Fatal("ready replay accepted corrupt persisted spool name")
	}
	if _, err := os.Stat(record.Path); err != nil {
		t.Fatalf("unexpected unlink after corrupt name: %v", err)
	}
}

func TestMediaSpoolObjectMissingReopensRequestOnlyForMatchingPendingResult(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	source := filepath.Join(t.TempDir(), "missing.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	record, err := spool.IngestFinalized(context.Background(), "missing_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	mediaID := "44444444-4444-4444-8444-444444444444"
	if err := spool.MarkGranted(context.Background(), record.UploadID, mediaID, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	granted, _ := spool.Get(context.Background(), record.UploadID)
	if err := spool.MarkPutMayHaveStarted(context.Background(), record.UploadID, granted.GrantExpires); err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkAwaitingResult(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "awaiting_result" {
		t.Fatalf("pre-missing state=%+v err=%v", state, err)
	}
	pending := mediaprotocol.UploadResult{Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.ResultPending, MediaID: mediaID, Code: "OBJECT_MISSING", Retryable: true}
	payload, encErr := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), pending)
	if encErr != nil {
		t.Fatalf("encode initial missing result: %v", encErr)
	}
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_other"), payload, time.Now()); err == nil {
		t.Fatal("wrong Gateway result accepted")
	}
	pending.MediaID = "55555555-5555-4555-8555-555555555555"
	payload, encErr = mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), pending)
	if encErr != nil {
		t.Fatalf("encode mismatched result: %v", encErr)
	}
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_001"), payload, time.Now()); err == nil {
		t.Fatal("unrelated media result reopened request")
	}
	pending.MediaID = mediaID
	payload, encErr = mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), pending)
	if encErr != nil {
		t.Fatalf("encode matching result: %v", encErr)
	}
	if err := spool.ApplyMQTTResultTopic(context.Background(), mediaprotocol.UploadResultTopic("gateway_001"), payload, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || got.Phase != "awaiting_grant" || got.MediaID != mediaID || got.Attempts != 1 {
		t.Fatalf("missing object transition = %+v err=%v", got, err)
	}
	oldExpiry := got.GrantExpires
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkGranted(context.Background(), record.UploadID, mediaID, oldExpiry); err == nil {
		t.Fatal("stale grant from the previous request was accepted after OBJECT_MISSING")
	}
	if err := spool.MarkGranted(context.Background(), record.UploadID, mediaID, oldExpiry.Add(time.Minute)); err != nil {
		t.Fatalf("fresh grant after missing-object response rejected: %v", err)
	}
}

func fmtHash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func readyPayload(uploadID, mediaID, gatewayID string) []byte {
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic(gatewayID), mediaprotocol.UploadResult{
		Version: 1, GatewayID: gatewayID, UploadID: uploadID, Status: mediaprotocol.ResultReady, MediaID: mediaID,
	})
	if err != nil {
		panic(err)
	}
	return payload
}
