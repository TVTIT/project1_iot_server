package gatewaysim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/mediaprotocol"
)

type mediaPublisherFunc func(context.Context, any) error

func (f mediaPublisherFunc) PublishMedia(ctx context.Context, message any) error {
	return f(ctx, message)
}

type mediaRecorder struct {
	mu       sync.Mutex
	messages []any
}

func (r *mediaRecorder) PublishMedia(_ context.Context, message any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, message)
	return nil
}

func (r *mediaRecorder) snapshot() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.messages...)
}

func TestMediaWorkerDoesNotTrustGrantTargetOrDeleteBeforeReady(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", Spool: spool, Publisher: mediaPublisherFunc(func(context.Context, any) error { return nil }),
		UploadOrigin: server.URL, httpClient: server.Client(), Capacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	defer func() {
		if err := worker.Stop(context.Background()); err != nil {
			t.Errorf("stop worker: %v", err)
		}
	}()
	payload, _ := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, UploadID: "clip_1", Status: mediaprotocol.GrantNeedUpload, MediaID: "11111111-1111-4111-8111-111111111111",
		UploadURL: server.URL + "/storage/v1/object/upload/sign/media-video/11111111-1111-4111-8111-111111111111.mp4?token=x", ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339),
	})
	if !worker.TryAdmit(mediaprotocol.UploadGrantTopic("gateway_001"), payload, false) {
		t.Fatal("bounded inbox rejected valid-sized delivery")
	}
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requests:
		t.Fatal("invalid grant target reached HTTP server")
	default:
	}
}

func TestMediaWorkerReadiesOnlyCorrelatedResult(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "clip.mp4")
	if err := os.WriteFile(source, []byte("mp4 fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "clip_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), mediaprotocol.UploadResult{
		Version: 1, UploadID: "clip_1", Status: mediaprotocol.ResultReady, MediaID: "11111111-1111-4111-8111-111111111111",
	})
	if err := spool.ApplyMQTTResult(context.Background(), payload, time.Now()); err == nil {
		t.Fatal("ready without persisted matching media correlation must fail")
	}
	if _, err := os.Stat(record.Path); err != nil {
		t.Fatalf("file was removed before correlated ready: %v", err)
	}
}

func TestMediaWorkerRejectsUnusableExplicitCA(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, []byte("not a CA"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", CAFile: caFile,
		Spool: spool, Publisher: mediaPublisherFunc(func(context.Context, any) error { return nil }), Capacity: 1,
	})
	if err == nil {
		t.Fatal("accepted invalid CA bundle")
	}
}

func TestMediaWorkerAmbiguousPUTReopensAsReconcileWithoutRepeatingBytes(t *testing.T) {
	const mediaID = "11111111-1111-4111-8111-111111111111"
	data := []byte("fixture media")
	puts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts++
		if r.Method != http.MethodPut || r.ContentLength != int64(len(data)) || r.Header.Get("Content-Type") != "video/mp4" {
			t.Errorf("PUT headers method=%s length=%d type=%q", r.Method, r.ContentLength, r.Header.Get("Content-Type"))
		}
		got, _ := io.ReadAll(r.Body)
		if !bytes.Equal(got, data) {
			t.Errorf("PUT body=%q", got)
		}
		w.WriteHeader(http.StatusBadGateway) // response is ambiguous; reconcile, never replay
	}))
	defer server.Close()
	cfg := mediaSpoolConfig(t)
	cfg.IntentTTL = time.Hour
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "ambiguous_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: server.URL, Spool: spool,
		Publisher: publisher, Capacity: 2, RetryMin: time.Millisecond, RetryMax: time.Second,
		PutTimeout: time.Second, httpClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	grant := mediaprotocol.UploadGrant{Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.GrantNeedUpload,
		MediaID: mediaID, UploadURL: server.URL + "/storage/v1/object/upload/sign/media-video/" + mediaID + ".mp4?token=t",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), grant)
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload})
	got, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || got.Phase != "awaiting_result" || got.MediaID != mediaID || puts != 1 {
		t.Fatalf("after ambiguous PUT record=%+v puts=%d err=%v", got, puts, err)
	}
	beforeRestart := got
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	worker.opts.Spool = spool
	if _, err := spool.db.Exec("UPDATE media_spool SET next_attempt=0 WHERE upload_id=?", record.UploadID); err != nil {
		t.Fatal(err)
	}
	worker.processDue(context.Background())
	got, err = spool.Get(context.Background(), record.UploadID)
	if err != nil || got.Phase != "awaiting_result" || puts != 1 || got.MediaID != beforeRestart.MediaID ||
		!got.Deadline.Equal(beforeRestart.Deadline) || !got.GrantExpires.Equal(beforeRestart.GrantExpires) {
		t.Fatalf("reopen replayed bytes or changed state: record=%+v puts=%d err=%v", got, puts, err)
	}
	if _, ok := publisher.snapshot()[1].(mediaprotocol.UploadComplete); !ok {
		t.Fatalf("expected completion reconciliation after reopen, messages=%#v", publisher.snapshot())
	}
}

func TestMediaWorkerLostPUTResponseAfterStorageWriteReconcilesWithoutReplay(t *testing.T) {
	const mediaID = "11111111-1111-4111-8111-111111111111"
	data := []byte("fixture media")
	var stored []byte
	puts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
			return
		}
		stored = append([]byte(nil), body...)

		// Simulate Storage committing the object before its response is lost.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack response after object write: %v", err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close hijacked connection: %v", err)
		}
	}))
	defer server.Close()

	cfg := mediaSpoolConfig(t)
	cfg.IntentTTL = time.Hour
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "lost_put_response_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: server.URL, Spool: spool,
		Publisher: publisher, Capacity: 2, RetryMin: time.Millisecond, RetryMax: time.Second,
		PutTimeout: time.Second, httpClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	grant := mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.GrantNeedUpload, MediaID: mediaID,
		UploadURL: server.URL + "/storage/v1/object/upload/sign/media-video/" + mediaID + ".mp4?token=t",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	}
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), grant)
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload})

	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "awaiting_result" || state.MediaID != mediaID {
		t.Fatalf("ambiguous PUT state=%+v err=%v", state, err)
	}
	messages := publisher.snapshot()
	if puts != 1 || !bytes.Equal(stored, data) || len(messages) != 1 {
		t.Fatalf("stored=%q PUTs=%d published=%#v", stored, puts, messages)
	}
	if _, ok := messages[0].(mediaprotocol.UploadComplete); !ok {
		t.Fatalf("expected completion reconciliation, got %#v", messages[0])
	}

	ready, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		MediaID: mediaID, Status: mediaprotocol.ResultReady,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: ready})
	if _, err := os.Stat(record.Path); !os.IsNotExist(err) {
		t.Fatalf("spool file should be removed only after matching ready; stat err=%v", err)
	}
	if puts != 1 {
		t.Fatalf("ambiguous PUT was replayed: puts=%d", puts)
	}
}

func TestMediaWorkerSuccessfulPUTReopensBeforeCompletionWithoutRepeatingBytes(t *testing.T) {
	const mediaID = "11111111-1111-4111-8111-111111111111"
	data := []byte("fixture media")
	puts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts++
		got, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || !bytes.Equal(got, data) {
			t.Errorf("unexpected upload method=%s body=%q", r.Method, got)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	cfg := mediaSpoolConfig(t)
	cfg.IntentTTL = time.Hour
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "restart_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	published := make(chan any, 1)
	blockPublisher := mediaPublisherFunc(func(_ context.Context, message any) error {
		published <- message
		return context.DeadlineExceeded
	})
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: server.URL, Spool: spool,
		Publisher: blockPublisher, Capacity: 1, RetryMin: time.Hour,
		PutTimeout: time.Second, httpClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	grant := mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.GrantNeedUpload, MediaID: mediaID,
		UploadURL: server.URL + "/storage/v1/object/upload/sign/media-video/" + mediaID + ".mp4?token=t",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	}
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), grant)
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{
		topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload,
	})
	if _, ok := (<-published).(mediaprotocol.UploadComplete); !ok {
		t.Fatal("successful PUT did not publish completion")
	}
	beforeRestart, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || beforeRestart.Phase != "awaiting_result" || beforeRestart.MediaID != mediaID ||
		puts != 1 {
		t.Fatalf("pre-restart state=%+v puts=%d err=%v", beforeRestart, puts, err)
	}
	if _, err := os.Stat(beforeRestart.Path); err != nil {
		t.Fatalf("spool file removed before correlated ready: %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	restartedPublisher := &mediaRecorder{}
	restarted, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: server.URL, Spool: spool,
		Publisher: restartedPublisher, Capacity: 1, RetryMin: time.Millisecond,
		PutTimeout: time.Second, httpClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.db.Exec("UPDATE media_spool SET next_attempt=0 WHERE upload_id=?", record.UploadID); err != nil {
		t.Fatal(err)
	}
	restarted.processDue(context.Background())
	afterRestart, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || afterRestart.Phase != "awaiting_result" || afterRestart.MediaID != mediaID ||
		!afterRestart.Deadline.Equal(beforeRestart.Deadline) || puts != 1 {
		t.Fatalf("restart changed intent or repeated PUT: state=%+v puts=%d err=%v",
			afterRestart, puts, err)
	}
	if _, ok := restartedPublisher.snapshot()[0].(mediaprotocol.UploadComplete); !ok {
		t.Fatalf("restart did not reconcile completion: %#v", restartedPublisher.snapshot())
	}
}

func TestMediaWorkerGrantReadyAndWrongCorrelationCannotDeleteSpool(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "ready_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", Spool: spool,
		Publisher: &mediaRecorder{}, Capacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	grant, _ := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.GrantReady,
		MediaID: "11111111-1111-4111-8111-111111111111",
	})
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: grant})
	if _, err := os.Stat(record.Path); err != nil {
		t.Fatalf("grant-ready deleted file without genuine result: %v", err)
	}
	terminal, _ := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.GrantRejected, Code: "UPLOAD_REJECTED",
	})
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: terminal})
	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "failed" || state.Outcome != "rejected" || state.Reason != "UPLOAD_REJECTED" {
		t.Fatalf("terminal grant not persisted: %+v err=%v", state, err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: []byte(
		`{"version":1,"gateway_id":"gateway_other","upload_id":"ready_1","status":"ready","media_id":"11111111-1111-4111-8111-111111111111"}`)})
	wrong, _ := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_other"), mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_other", UploadID: record.UploadID, Status: mediaprotocol.ResultReady,
		MediaID: "11111111-1111-4111-8111-111111111111",
	})
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_other"), payload: wrong})
	if _, err := os.Stat(record.Path); err != nil {
		t.Fatalf("spool removed by grant-ready or wrong-topic result: %v", err)
	}
}

func TestMediaWorkerRejectsDuplicateTokenAndRedirectDoesNotForward(t *testing.T) {
	var forwarded, redirects int
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded++ }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirects++
		http.Redirect(w, r, target.URL+"/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "redirect_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{GatewayID: "gateway_001", UploadOrigin: redirect.URL, Spool: spool, Publisher: publisher, Capacity: 1, httpClient: redirect.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	const mediaID = "22222222-2222-4222-8222-222222222222"
	grant := mediaprotocol.UploadGrant{Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.GrantNeedUpload,
		MediaID: mediaID, UploadURL: redirect.URL + "/storage/v1/object/upload/sign/media-video/" + mediaID + ".mp4?token=a&token=b",
		ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)}
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), grant)
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload})
	if forwarded != 0 {
		t.Fatalf("redirect forwarded to another host %d times", forwarded)
	}
	grant.UploadURL = redirect.URL + "/storage/v1/object/upload/sign/media-video/" + mediaID + ".mp4?token=a"
	payload, err = mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), grant)
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload})
	if redirects != 1 || forwarded != 0 {
		t.Fatalf("redirect handling count=%d forwarded=%d", redirects, forwarded)
	}
}

func TestMediaWorkerAttemptLimitIsDurableAndKeepsFile(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	cfg.MaxAttempts = 1
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "limit_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", Spool: spool,
		Publisher: publisher, Capacity: 1, RetryMin: time.Millisecond, RetryMax: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.processDue(context.Background())
	if len(publisher.snapshot()) != 1 {
		t.Fatalf("initial request not published: %#v", publisher.snapshot())
	}
	if _, err := spool.db.Exec("UPDATE media_spool SET next_attempt=0 WHERE upload_id=?", record.UploadID); err != nil {
		t.Fatal(err)
	}
	worker.processDue(context.Background())
	got, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || got.Phase != "failed" || got.Reason != "RETRY_LIMIT" || len(publisher.snapshot()) != 1 {
		t.Fatalf("attempt cap state=%+v publications=%#v err=%v", got, publisher.snapshot(), err)
	}
	if _, err := os.Stat(record.Path); err != nil {
		t.Fatalf("terminal retry limit removed spool file: %v", err)
	}
}

func TestMediaWorkerRetryAfterCompletionReconcilesAcrossReopen(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "reconcile_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", Spool: spool,
		Publisher: publisher, Capacity: 1, RetryMin: time.Millisecond, RetryMax: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	canonicalUUID := "23456789-abcd-4abc-8def-0123456789ab"
	payload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID, Status: mediaprotocol.GrantRetryAfterCompletion,
		MediaID: canonicalUUID,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: payload})
	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "awaiting_result" {
		t.Fatalf("retry-after-completion state=%+v err=%v", state, err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	worker.opts.Spool = spool
	if _, err := spool.db.Exec("UPDATE media_spool SET next_attempt=0 WHERE upload_id=?", record.UploadID); err != nil {
		t.Fatal(err)
	}
	worker.processDue(context.Background())
	messages := publisher.snapshot()
	if len(messages) != 2 {
		t.Fatalf("expected only completion reconciliation across reopen, got %#v", messages)
	}
	for _, message := range messages {
		if _, ok := message.(mediaprotocol.UploadComplete); !ok {
			t.Fatalf("retry-after-completion published non-reconciliation message %#v", message)
		}
	}
	state, err = spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "awaiting_result" || state.MediaID != canonicalUUID {
		t.Fatalf("reopened reconciliation state=%+v err=%v", state, err)
	}
}

func TestMediaWorkerLostGrantAndMissingObjectRecovery(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "recovery_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", Spool: spool,
		Publisher: publisher, Capacity: 10, RetryMin: time.Millisecond, RetryMax: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	canonicalUUID := "23456789-abcd-4abc-8def-0123456789ab"

	// --- Branch (a): initial grant was lost; retry_after_completion must bind committed ID and survive reopen ---
	grantPayload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status:  mediaprotocol.GrantRetryAfterCompletion,
		MediaID: canonicalUUID,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: grantPayload})

	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || state.Phase != "awaiting_result" || state.MediaID != canonicalUUID {
		t.Fatalf("expected awaiting_result with media_id=%q; got phase=%q media_id=%q err=%v",
			canonicalUUID, state.Phase, state.MediaID, err)
	}

	// Close and reopen to prove durable binding survives restart/reopen
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err = OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	worker.opts.Spool = spool

	reopened, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || reopened.Phase != "awaiting_result" || reopened.MediaID != canonicalUUID {
		t.Fatalf("reopened state mismatch: phase=%q media_id=%q want %q",
			reopened.Phase, reopened.MediaID, canonicalUUID)
	}

	// Gateway should have published UploadComplete
	sent := publisher.snapshot()
	if len(sent) != 1 {
		t.Fatalf("expected 1 published message (UploadComplete), got %d: %+v", len(sent), sent)
	}
	complete, ok := sent[0].(mediaprotocol.UploadComplete)
	if !ok || complete.UploadID != record.UploadID {
		t.Fatalf("expected UploadComplete for %q, got %+v", record.UploadID, sent[0])
	}

	// --- Negative checks: mismatched/missing ID on OBJECT_MISSING must NOT transition ---
	// Mismatched ID on OBJECT_MISSING:
	mismatchedPayload, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.ResultPending, Code: "OBJECT_MISSING",
		MediaID: "99999999-9999-4999-8999-999999999999", Retryable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: mismatchedPayload})
	afterMismatched, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || afterMismatched.Phase != "awaiting_result" {
		t.Fatalf("mismatched OBJECT_MISSING moved phase: %+v", afterMismatched)
	}

	// Missing ID on OBJECT_MISSING: codec rejects it; test raw delivery fails closed
	missingIDPayload, _ := json.Marshal(mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.ResultPending, Code: "OBJECT_MISSING", Retryable: true,
	})
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: missingIDPayload})
	afterMissingID, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || afterMissingID.Phase != "awaiting_result" {
		t.Fatalf("missing ID OBJECT_MISSING moved phase: %+v", afterMissingID)
	}

	// Wrong ready result with mismatched ID must NOT unlink spool file
	wrongReadyPayload, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.ResultReady, MediaID: "99999999-9999-4999-8999-999999999999",
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: wrongReadyPayload})
	if _, statErr := os.Stat(filepath.Join(cfg.Dir, record.UploadID+".mp4")); statErr != nil {
		t.Fatal("spool file unlinked on wrong ready result")
	}

	// Grant with ready must NOT unlink spool file
	grantReadyPayload, err := mediaprotocol.Encode(mediaprotocol.UploadGrantTopic("gateway_001"), mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.GrantReady, MediaID: canonicalUUID,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: grantReadyPayload})
	if _, statErr := os.Stat(filepath.Join(cfg.Dir, record.UploadID+".mp4")); statErr != nil {
		t.Fatal("spool file unlinked on grant ready")
	}

	// --- Branch (b): a PUT produced no object, pending+OBJECT_MISSING with matching ID leads to new request ---
	resultPayload, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic("gateway_001"), mediaprotocol.UploadResult{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status: mediaprotocol.ResultPending, Code: "OBJECT_MISSING",
		MediaID: canonicalUUID, Retryable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadResultTopic("gateway_001"), payload: resultPayload})

	afterMissing, err := spool.Get(context.Background(), record.UploadID)
	if err != nil || afterMissing.Phase != "awaiting_grant" || afterMissing.MediaID != canonicalUUID {
		t.Fatalf("expected awaiting_grant with media_id=%q; got phase=%q media_id=%q err=%v",
			canonicalUUID, afterMissing.Phase, afterMissing.MediaID, err)
	}

	// Make next_attempt due now and run processDue
	if _, err := spool.db.Exec("UPDATE media_spool SET next_attempt=0 WHERE upload_id=?", record.UploadID); err != nil {
		t.Fatal(err)
	}
	pubBefore := len(publisher.snapshot())
	worker.processDue(context.Background())
	pubAfter := publisher.snapshot()

	if len(pubAfter) != pubBefore+1 {
		t.Fatalf("expected new UploadRequest published, got %d messages (before %d)", len(pubAfter), pubBefore)
	}
	req, ok := pubAfter[len(pubAfter)-1].(mediaprotocol.UploadRequest)
	if !ok || req.UploadID != record.UploadID {
		t.Fatalf("expected UploadRequest for %q, got %+v", record.UploadID, pubAfter[len(pubAfter)-1])
	}

	// Spool file must NOT be unlinked
	if _, statErr := os.Stat(filepath.Join(cfg.Dir, record.UploadID+".mp4")); statErr != nil {
		t.Fatal("spool file was unlinked before ready")
	}
}

func TestMediaWorkerRejectsIDLessRetryAfterCompletionWithoutTransitionOrComplete(t *testing.T) {
	cfg := mediaSpoolConfig(t)
	spool, err := OpenMediaSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spool.Close(); err != nil {
			t.Errorf("close media spool: %v", err)
		}
	}()
	source := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(source, []byte("clip"), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := spool.IngestFinalized(context.Background(), "idless_1", source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.RecordAttempt(context.Background(), record.UploadID, time.Now()); err != nil {
		t.Fatal(err)
	}
	publisher := &mediaRecorder{}
	worker, err := NewMediaWorker(MediaWorkerOptions{
		GatewayID: "gateway_001", UploadOrigin: "https://storage.example", Spool: spool,
		Publisher: publisher, Capacity: 10, RetryMin: time.Millisecond, RetryMax: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	idLessGrant := mediaprotocol.UploadGrant{
		Version: 1, GatewayID: "gateway_001", UploadID: record.UploadID,
		Status:  mediaprotocol.GrantRetryAfterCompletion,
		MediaID: "",
	}
	raw, _ := json.Marshal(idLessGrant)
	worker.handle(context.Background(), mediaDelivery{topic: mediaprotocol.UploadGrantTopic("gateway_001"), payload: raw})

	state, err := spool.Get(context.Background(), record.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != "awaiting_grant" {
		t.Fatalf("spool transitioned to %q for ID-less reconcile, want awaiting_grant", state.Phase)
	}
	sent := publisher.snapshot()
	if len(sent) != 0 {
		t.Fatalf("published %d messages for ID-less reconcile, want 0: %+v", len(sent), sent)
	}
}
