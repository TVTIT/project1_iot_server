package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gatewaysim"
)

// --- helpers ---------------------------------------------------------------

func lookupFrom(values map[string]string) gatewaysim.LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// runEnv returns a complete, valid simulator environment except the outbox path.
func runEnv(outboxPath string, overrides map[string]string) map[string]string {
	values := map[string]string{
		"GATEWAY_ID":              "gateway_001",
		"SENSOR_ID":               "sensor_001",
		"GATEWAY_MQTT_BROKER_URL": "ssl://mosquitto:8883",
		"GATEWAY_MQTT_CLIENT_ID":  "gateway_sim_001",
		"GATEWAY_MQTT_USERNAME":   "gateway_001",
		"GATEWAY_MQTT_PASSWORD":   "test-only-gateway-password",
		"GATEWAY_MQTT_CA_FILE":    "/mosquitto/config/certs/ca.crt",
		"GATEWAY_OUTBOX_PATH":     outboxPath,
	}
	for key, value := range overrides {
		values[key] = value
	}
	return values
}

var seedBootID = uuid.MustParse("11111111-1111-4111-8111-111111111111")

// seedOutbox creates a real SQLite outbox at path and enqueues count pending
// batches, returning their message ids and the seeded byte total.
func seedOutbox(t *testing.T, path string, count int) ([]uuid.UUID, gatewaysim.OutboxCounts) {
	t.Helper()
	outbox, err := gatewaysim.OpenOutbox(gatewaysim.OutboxConfig{
		Path:      path,
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		MaxRows:   100,
		MaxBytes:  1 << 20,
		OpTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("OpenOutbox: %v", err)
	}
	ids := make([]uuid.UUID, 0, count)
	base := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		id := uuid.New()
		if _, err := outbox.Enqueue(context.Background(), gatewaysim.EnqueueRequest{
			BootID:           seedBootID,
			MessageID:        id,
			MeasuredAt:       base.Add(time.Duration(i) * time.Second),
			ReceivedAt:       base.Add(time.Duration(i)*time.Second + time.Second),
			SampleCount:      100,
			SampleIntervalUS: 10000,
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, id)
	}
	counts, err := outbox.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return ids, counts
}

func adminCounts(t *testing.T, path string) gatewaysim.OutboxCounts {
	t.Helper()
	outbox, err := gatewaysim.OpenOutboxAdmin(path, 5*time.Second)
	if err != nil {
		t.Fatalf("OpenOutboxAdmin: %v", err)
	}
	defer func() { _ = outbox.Close() }()
	counts, err := outbox.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	return counts
}

// recordedPublication is one Publish invocation captured by the fake transport
// so a test can compare topic/bytes across two runtime invocations.
type recordedPublication struct {
	topic   string
	payload []byte
}

// fakeTransport is a small stand-in for the MQTT transport. It becomes ready on
// Start and, when sendAck is set, delivers a valid committed telemetry ACK for
// every published batch through the real ACK admitter. It is the boundary seam
// that lets the run lifecycle be tested without a broker. It records every
// published topic/payload so a restart test can prove a replay is byte-exact.
type fakeTransport struct {
	gatewayID string
	admit     gatewaysim.ACKAdmitter
	sendAck   bool

	mu        sync.Mutex
	ready     bool
	published chan struct{}
	once      sync.Once
	recorded  []recordedPublication

	fatalCh chan struct{}
	readyCh chan struct{}
}

func newFakeTransport(sendAck bool) *fakeTransport {
	return &fakeTransport{
		sendAck:   sendAck,
		published: make(chan struct{}),
		fatalCh:   make(chan struct{}),
		readyCh:   make(chan struct{}),
	}
}

// frames returns a copy of the publications recorded so far.
func (f *fakeTransport) frames() []recordedPublication {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedPublication, len(f.recorded))
	copy(out, f.recorded)
	return out
}

func (f *fakeTransport) Start() error { return f.setReady(true) }
func (f *fakeTransport) WaitReady(context.Context) error {
	return nil
}
func (f *fakeTransport) Stop(context.Context) error { return f.setReady(false) }

func (f *fakeTransport) setReady(ready bool) error {
	f.mu.Lock()
	f.ready = ready
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) Ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready
}

func (f *fakeTransport) ReadyCh() <-chan struct{}     { return f.readyCh }
func (f *fakeTransport) FatalSignal() <-chan struct{} { return f.fatalCh }
func (f *fakeTransport) Fatal() error                 { return nil }

func (f *fakeTransport) Publish(ctx context.Context, topic string, payload []byte) error {
	if ctx.Err() != nil {
		return gatewaysim.ErrPublishUnconfirmed
	}
	f.mu.Lock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.recorded = append(f.recorded, recordedPublication{topic: topic, payload: cp})
	f.mu.Unlock()
	f.once.Do(func() { close(f.published) })
	if f.sendAck {
		var batch struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(payload, &batch); err == nil && batch.MessageID != "" {
			ack := fmt.Sprintf(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":%q,"status":"committed","committed_at":%q}`,
				batch.MessageID, time.Now().UTC().Format(time.RFC3339Nano))
			f.admit.Admit("gateways/"+f.gatewayID+"/acks/"+batch.MessageID, []byte(ack), false)
		}
	}
	return nil
}

func (f *fakeTransport) PublishMedia(context.Context, any) error { return nil }

// installFakeTransport replaces the transport seam for one test. The fake learns
// the admitter and gateway id from the composition.
func installFakeTransport(t *testing.T, fake *fakeTransport) {
	t.Helper()
	original := buildTransport
	buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, _ gatewaysim.MediaAdmitter) (simTransport, error) {
		fake.admit = admit
		fake.gatewayID = cfg.GatewayID
		return fake, nil
	}
	t.Cleanup(func() { buildTransport = original })
}

// --- administrative commands ----------------------------------------------

func TestAdminCountAndPendingMetadataOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	ids, seeded := seedOutbox(t, path, 2)

	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"count"}, lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf)
	if code != 0 {
		t.Fatalf("count exit = %d, stderr = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), fmt.Sprintf("pending_rows=%d", seeded.PendingRows)) {
		t.Fatalf("count output = %q", out.String())
	}

	out.Reset()
	errBuf.Reset()
	code = run(context.Background(), []string{"pending", "--limit", "1"}, lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf)
	if code != 0 {
		t.Fatalf("pending exit = %d, stderr = %s", code, errBuf.String())
	}
	text := out.String()
	if !strings.Contains(text, "pending message_id="+ids[0].String()) {
		t.Fatalf("pending missing first id: %q", text)
	}
	if strings.Contains(text, "payload") || strings.Contains(text, "test-only") {
		t.Fatalf("pending leaked payload/secret: %q", text)
	}
	// The listing is limited to one row but the aggregate reflects both.
	if strings.Count(text, "message_id=") != 1 {
		t.Fatalf("pending limit not honored: %q", text)
	}
}

func TestAdminRejectsInvalidLimitAndUnknownCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	seedOutbox(t, path, 1)

	var out, errBuf bytes.Buffer
	if code := run(context.Background(), []string{"pending", "--limit", "0"}, lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf); code != 2 {
		t.Fatalf("pending --limit 0 exit = %d", code)
	}
	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"bogus"}, lookupFrom(nil), &out, &errBuf); code != 2 {
		t.Fatalf("unknown command exit = %d", code)
	}
}

func TestAdminNonexistentOutboxIsNotCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.db")

	for _, command := range []string{"count", "pending"} {
		var out, errBuf bytes.Buffer
		code := run(context.Background(), []string{command}, lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf)
		if code != 1 {
			t.Fatalf("%s exit = %d, stderr = %s", command, code, errBuf.String())
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s created the outbox file", command)
		}
	}
}

func TestAdminQuarantineKeepsQuotaAndValidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	ids, seeded := seedOutbox(t, path, 2)
	beforeTotal := seeded.PendingBytes + seeded.QuarantinedBytes

	// Empty reason is rejected before any file is touched.
	var out, errBuf bytes.Buffer
	if code := run(context.Background(), []string{"quarantine", "--message-id", ids[0].String(), "--reason", "  "},
		lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf); code != 2 {
		t.Fatalf("empty reason exit = %d", code)
	}
	// A missing item is a no-op with a nonzero exit; the file is untouched.
	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"quarantine", "--message-id", uuid.New().String(), "--reason", "operator review"},
		lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf); code != 1 {
		t.Fatalf("missing id exit = %d", code)
	}

	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"quarantine", "--message-id", ids[0].String(), "--reason", "operator review"},
		lookupFrom(map[string]string{"GATEWAY_OUTBOX_PATH": path}), &out, &errBuf); code != 0 {
		t.Fatalf("quarantine exit = %d, stderr = %s", code, errBuf.String())
	}

	after := adminCounts(t, path)
	if after.PendingRows != 1 || after.QuarantinedRows != 1 {
		t.Fatalf("counts after quarantine = %+v", after)
	}
	if after.PendingBytes+after.QuarantinedBytes != beforeTotal {
		t.Fatalf("quarantine changed the quota: before %d after %d", beforeTotal, after.PendingBytes+after.QuarantinedBytes)
	}
}

// --- run lifecycle ---------------------------------------------------------

func TestRunFiniteAckSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	spool := filepath.Join(t.TempDir(), "media-spool")
	fake := newFakeTransport(true)
	installFakeTransport(t, fake)

	var out, errBuf bytes.Buffer
	env := runEnv(path, map[string]string{"GATEWAY_MEDIA_SPOOL_DIR": spool})
	code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "2s"},
		lookupFrom(env), &out, &errBuf)
	if code != 0 {
		t.Fatalf("run exit = %d, stderr = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "completed=true") {
		t.Fatalf("summary = %q", out.String())
	}
	if got := adminCounts(t, path); got.PendingRows != 0 {
		t.Fatalf("pending after success = %+v", got)
	}
	if _, err := os.Stat(spool); !os.IsNotExist(err) {
		t.Fatalf("media-off run created spool path: stat err=%v", err)
	}
}

func TestGatewayTransportOptionsKeepMediaSubscriptionOptIn(t *testing.T) {
	disabled := transportOptions(gatewaysim.Config{}, nil)
	if disabled.MediaEnabled || disabled.MediaInbox != nil {
		t.Fatalf("media-off transport options enabled media: %+v", disabled)
	}
}

func TestPrepareMediaSpoolDirCreatesPrivateButDoesNotRepairExisting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new-spool")
	if err := prepareMediaSpoolDir(dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0700 {
		t.Fatalf("created spool mode=%v", info.Mode().Perm())
	}
	loose := filepath.Join(t.TempDir(), "loose")
	if err := os.Mkdir(loose, 0750); err != nil {
		t.Fatal(err)
	}
	if err := prepareMediaSpoolDir(loose); err == nil {
		t.Fatal("accepted existing non-private spool directory")
	}
	info, err = os.Stat(loose)
	if err != nil || info.Mode().Perm() != 0750 {
		t.Fatalf("existing directory permissions changed: mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestRunMediaCompositionUsesPrivateSeparateSpoolAndWorkerInbox(t *testing.T) {
	root := t.TempDir()
	outboxPath := filepath.Join(root, "telemetry.db")
	spoolDir := filepath.Join(root, "media-spool")
	input := filepath.Join(root, "finalized.mp4")
	data := []byte("finalized fixture bytes")
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	fake := newFakeTransport(true)
	original := buildTransport
	var gotMedia gatewaysim.MediaAdmitter
	buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, media gatewaysim.MediaAdmitter) (simTransport, error) {
		if !cfg.MediaUpload.Enabled {
			t.Fatal("media config disabled in media-on composition")
		}
		opts := transportOptions(cfg, media)
		if !opts.MediaEnabled || opts.MediaInbox != media {
			t.Fatalf("media transport subscription was not configured: %+v", opts)
		}
		gotMedia = media
		fake.admit, fake.gatewayID = admit, cfg.GatewayID
		return fake, nil
	}
	t.Cleanup(func() { buildTransport = original })

	env := runEnv(outboxPath, map[string]string{
		"MEDIA_UPLOAD_ENABLED":        "true",
		"GATEWAY_MEDIA_INPUT_FILE":    input,
		"GATEWAY_MEDIA_SPOOL_DIR":     spoolDir,
		"GATEWAY_MEDIA_UPLOAD_ID":     "fixture_upload_001",
		"GATEWAY_MEDIA_UPLOAD_ORIGIN": "https://storage.example",
	})
	var out, errBuf bytes.Buffer
	if code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "2s"}, lookupFrom(env), &out, &errBuf); code != 0 {
		t.Fatalf("run exit=%d stderr=%q", code, errBuf.String())
	}
	if gotMedia == nil {
		t.Fatal("media inbox was not passed to transport")
	}
	for _, path := range []string{outboxPath, filepath.Join(spoolDir, "media.sqlite")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected separate database %q: %v", path, err)
		}
	}
	if _, err := os.Stat(filepath.Join(spoolDir, "fixture_upload_001.mp4")); err != nil {
		t.Fatalf("spooled fixture missing before correlated ready: %v", err)
	}
	got, err := os.ReadFile(input)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("configured source changed: err=%v data=%q", err, got)
	}
}

type lifecycleMediaWorkerStub struct {
	gatewaysim.MediaAdmitter
	spool     *gatewaysim.MediaSpool
	blockStop bool
	joined    bool
	stopped   bool
}

func (s *lifecycleMediaWorkerStub) Admit(topic string, payload []byte, retained bool) {}
func (s *lifecycleMediaWorkerStub) SetPublisher(publisher gatewaysim.MediaPublisher)  {}
func (s *lifecycleMediaWorkerStub) Start(ctx context.Context)                         {}
func (s *lifecycleMediaWorkerStub) Stop(ctx context.Context) error {
	s.stopped = true
	if s.blockStop {
		<-ctx.Done()
		return ctx.Err()
	}
	s.joined = true
	return nil
}

func TestRunMediaWorkerShutdownLifecycleAndSpoolOwnership(t *testing.T) {
	setupFixture := func(t *testing.T) (string, string, string, map[string]string) {
		root := t.TempDir()
		outboxPath := filepath.Join(root, "telemetry.db")
		spoolDir := filepath.Join(root, "media-spool")
		input := filepath.Join(root, "finalized.mp4")
		if err := os.WriteFile(input, []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
		env := runEnv(outboxPath, map[string]string{
			"MEDIA_UPLOAD_ENABLED":        "true",
			"GATEWAY_MEDIA_INPUT_FILE":    input,
			"GATEWAY_MEDIA_SPOOL_DIR":     spoolDir,
			"GATEWAY_MEDIA_UPLOAD_ID":     "fixture_upload_001",
			"GATEWAY_MEDIA_UPLOAD_ORIGIN": "https://storage.example",
			"GATEWAY_SHUTDOWN_TIMEOUT":    "50ms",
		})
		return outboxPath, spoolDir, input, env
	}

	t.Run("unconfirmed worker stop retains spool lock and reports nonzero exit", func(t *testing.T) {
		_, spoolDir, _, env := setupFixture(t)
		fake := newFakeTransport(true)
		origTransport := buildTransport
		origWorker := buildMediaWorker
		buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, media gatewaysim.MediaAdmitter) (simTransport, error) {
			fake.admit, fake.gatewayID = admit, cfg.GatewayID
			return fake, nil
		}
		stub := &lifecycleMediaWorkerStub{blockStop: true}
		buildMediaWorker = func(opts gatewaysim.MediaWorkerOptions) (simMediaWorker, error) {
			stub.spool = opts.Spool
			return stub, nil
		}
		t.Cleanup(func() {
			buildTransport = origTransport
			buildMediaWorker = origWorker
			if stub.spool != nil {
				_ = stub.spool.Close()
			}
		})

		var out, errBuf bytes.Buffer
		code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "2s"}, lookupFrom(env), &out, &errBuf)
		if code == 0 {
			t.Fatalf("expected nonzero exit code when media worker stop times out, got 0; stdout=%q stderr=%q", out.String(), errBuf.String())
		}
		if !stub.stopped {
			t.Fatal("media worker stop was not invoked")
		}
		if stub.joined {
			t.Fatal("media worker should not have joined when blocked")
		}

		// Pre-fix bug: mediaSpool.Close() was called, releasing the lock.
		// Fixed behavior: spool ownership and lock are retained because worker did not join.
		_, reopenErr := gatewaysim.OpenMediaSpool(gatewaysim.MediaSpoolConfig{
			DBPath: filepath.Join(spoolDir, "media.sqlite"), Dir: spoolDir,
			GatewayID: "gateway_001", MaxFiles: 10, MaxBytes: 1000000, MaxAttempts: 5, OpTimeout: time.Second,
		})
		if !errors.Is(reopenErr, gatewaysim.ErrMediaSpoolBusy) {
			t.Fatalf("expected spool lock to remain held (ErrMediaSpoolBusy), got %v", reopenErr)
		}
	})

	t.Run("successful worker join closes spool and releases ownership", func(t *testing.T) {
		_, spoolDir, _, env := setupFixture(t)
		fake := newFakeTransport(true)
		origTransport := buildTransport
		origWorker := buildMediaWorker
		buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, media gatewaysim.MediaAdmitter) (simTransport, error) {
			fake.admit, fake.gatewayID = admit, cfg.GatewayID
			return fake, nil
		}
		stub := &lifecycleMediaWorkerStub{blockStop: false}
		buildMediaWorker = func(opts gatewaysim.MediaWorkerOptions) (simMediaWorker, error) {
			stub.spool = opts.Spool
			return stub, nil
		}
		t.Cleanup(func() {
			buildTransport = origTransport
			buildMediaWorker = origWorker
		})

		var out, errBuf bytes.Buffer
		code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "2s"}, lookupFrom(env), &out, &errBuf)
		if code != 0 {
			t.Fatalf("expected exit code 0 on clean shutdown, got %d; stderr=%q", code, errBuf.String())
		}
		if !stub.joined {
			t.Fatal("media worker did not join")
		}

		// Reopen must succeed because spool was cleanly closed after join
		reopened, reopenErr := gatewaysim.OpenMediaSpool(gatewaysim.MediaSpoolConfig{
			DBPath: filepath.Join(spoolDir, "media.sqlite"), Dir: spoolDir,
			GatewayID: "gateway_001", MaxFiles: 10, MaxBytes: 1000000, MaxAttempts: 5, OpTimeout: time.Second,
		})
		if reopenErr != nil {
			t.Fatalf("reopen failed after clean shutdown: %v", reopenErr)
		}
		_ = reopened.Close()
	})

	t.Run("no media enabled preserves normal shutdown", func(t *testing.T) {
		root := t.TempDir()
		outboxPath := filepath.Join(root, "telemetry.db")
		env := runEnv(outboxPath, nil)
		fake := newFakeTransport(true)
		origTransport := buildTransport
		buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, media gatewaysim.MediaAdmitter) (simTransport, error) {
			fake.admit, fake.gatewayID = admit, cfg.GatewayID
			return fake, nil
		}
		t.Cleanup(func() { buildTransport = origTransport })

		var out, errBuf bytes.Buffer
		code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "2s"}, lookupFrom(env), &out, &errBuf)
		if code != 0 {
			t.Fatalf("expected exit code 0, got %d; stderr=%q", code, errBuf.String())
		}
	})

	t.Run("partial startup failure before media worker starts releases spool", func(t *testing.T) {
		_, spoolDir, _, env := setupFixture(t)
		origTransport := buildTransport
		buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter, media gatewaysim.MediaAdmitter) (simTransport, error) {
			return nil, errors.New("simulated transport failure")
		}
		t.Cleanup(func() { buildTransport = origTransport })

		var out, errBuf bytes.Buffer
		code := run(context.Background(), []string{"run", "--batches", "1"}, lookupFrom(env), &out, &errBuf)
		if code != 1 {
			t.Fatalf("expected exit code 1 on startup failure, got %d", code)
		}

		// Spool was opened before transport creation, so closeAll must have cleaned it up
		reopened, reopenErr := gatewaysim.OpenMediaSpool(gatewaysim.MediaSpoolConfig{
			DBPath: filepath.Join(spoolDir, "media.sqlite"), Dir: spoolDir,
			GatewayID: "gateway_001", MaxFiles: 10, MaxBytes: 1000000, MaxAttempts: 5, OpTimeout: time.Second,
		})
		if reopenErr != nil {
			t.Fatalf("spool lock leaked on partial startup failure: %v", reopenErr)
		}
		_ = reopened.Close()
	})
}

func TestRunFinitePendingFailurePreserves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	fake := newFakeTransport(false)
	installFakeTransport(t, fake)

	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "300ms"},
		lookupFrom(runEnv(path, nil)), &out, &errBuf)
	if code == 0 {
		t.Fatalf("expected nonzero exit, stdout = %q", out.String())
	}
	if !strings.Contains(out.String(), "completed=false") || !strings.Contains(out.String(), "pending_rows=1") {
		t.Fatalf("summary = %q", out.String())
	}
	if got := adminCounts(t, path); got.PendingRows != 1 {
		t.Fatalf("pending not preserved = %+v", got)
	}
}

func TestRunSignalShutdownPreservesPendingAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	fake := newFakeTransport(false)
	installFakeTransport(t, fake)

	ctx, cancel := context.WithCancel(context.Background())
	var out, errBuf bytes.Buffer
	codeCh := make(chan int, 1)
	go func() {
		codeCh <- run(ctx, nil, lookupFrom(runEnv(path, nil)), &out, &errBuf)
	}()

	select {
	case <-fake.published:
	case <-time.After(3 * time.Second):
		t.Fatal("no batch was published before the timeout")
	}
	cancel()

	select {
	case code := <-codeCh:
		if code != 0 {
			t.Fatalf("signal stop exit = %d, stderr = %s", code, errBuf.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}

	// The lock must be released and the pending batch must survive a reopen.
	if got := adminCounts(t, path); got.PendingRows < 1 {
		t.Fatalf("pending not preserved after signal = %+v", got)
	}
}

// TestRunRestartResendsPriorBootByteExact drives two real CLI runtime
// invocations against one durable SQLite file with a fake transport (the MQTT
// boundary): the first run leaves one unacknowledged pending row, and the second
// run replays that old-boot row byte-exact before acknowledging it and
// generating exactly one new batch under a new boot id.
func TestRunRestartResendsPriorBootByteExact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")

	// Run 1: no application ACK, so the committed row must survive.
	fake1 := newFakeTransport(false)
	installFakeTransport(t, fake1)
	var out1, errBuf1 bytes.Buffer
	code1 := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "300ms"},
		lookupFrom(runEnv(path, nil)), &out1, &errBuf1)
	if code1 == 0 {
		t.Fatalf("run 1 unexpectedly completed, stdout = %q", out1.String())
	}
	frames1 := fake1.frames()
	if len(frames1) != 1 {
		t.Fatalf("run 1 published %d batches, want 1", len(frames1))
	}
	if got := adminCounts(t, path); got.PendingRows != 1 {
		t.Fatalf("run 1 pending = %+v, want 1", got)
	}

	// Run 2: reopen the same file with ACKs enabled. The old-boot row must be
	// replayed with the exact same topic/bytes; the new boot starts its own
	// sequence stream at 0.
	fake2 := newFakeTransport(true)
	installFakeTransport(t, fake2)
	var out2, errBuf2 bytes.Buffer
	code2 := run(context.Background(), []string{"run", "--batches", "1", "--completion-timeout", "5s"},
		lookupFrom(runEnv(path, nil)), &out2, &errBuf2)
	if code2 != 0 {
		t.Fatalf("run 2 exit = %d, stderr = %s", code2, errBuf2.String())
	}
	frames2 := fake2.frames()
	if len(frames2) != 2 {
		t.Fatalf("run 2 published %d batches, want 2 (replay + new)", len(frames2))
	}
	if got := adminCounts(t, path); got.PendingRows != 0 {
		t.Fatalf("run 2 pending = %+v, want 0", got)
	}
	if !strings.Contains(out2.String(), "enqueued=1") {
		t.Fatalf("run 2 summary did not report exactly one new batch: %q", out2.String())
	}

	// Exactly one of the run-2 frames must be byte-exact to run 1 (the replay).
	replays := 0
	for _, frame := range frames2 {
		if frame.topic == frames1[0].topic && bytes.Equal(frame.payload, frames1[0].payload) {
			replays++
		}
	}
	if replays != 1 {
		t.Fatalf("run 2 did not replay the prior-boot row byte-exact (matches=%d)", replays)
	}
	boot1 := payloadBootID(t, frames1[0].payload)
	boot2 := ""
	for _, frame := range frames2 {
		got := payloadBootID(t, frame.payload)
		if got != boot1 {
			boot2 = got
		}
	}
	if boot2 == "" || boot2 == boot1 {
		t.Fatalf("run 2 did not generate a new batch under a distinct boot id (boot1=%s boot2=%s)", boot1, boot2)
	}
}

// payloadBootID extracts the boot_id field from a serialized telemetry batch.
func payloadBootID(t *testing.T, payload []byte) string {
	t.Helper()
	var batch struct {
		BootID string `json:"boot_id"`
	}
	if err := json.Unmarshal(payload, &batch); err != nil || batch.BootID == "" {
		t.Fatalf("payload has no boot_id: %v", err)
	}
	return batch.BootID
}

func TestRunStartupInvalidCAReleasesResources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outbox.db")
	missingCA := filepath.Join(dir, "missing-ca.crt")

	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"run", "--batches", "1"},
		lookupFrom(runEnv(path, map[string]string{"GATEWAY_MQTT_CA_FILE": missingCA})), &out, &errBuf)
	if code != 1 {
		t.Fatalf("invalid CA exit = %d, stderr = %s", code, errBuf.String())
	}
	if strings.Contains(errBuf.String(), missingCA) {
		t.Fatalf("stderr leaked the CA path: %q", errBuf.String())
	}

	// The outbox opened before the transport failure must have been closed, so
	// the lock is free and the file can be reopened.
	reopened, err := gatewaysim.OpenOutbox(gatewaysim.OutboxConfig{
		Path:      path,
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		MaxRows:   100,
		MaxBytes:  1 << 20,
		OpTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("outbox was not released after startup failure: %v", err)
	}
	_ = reopened.Close()
}

func TestRun_MeasureFlagValidation(t *testing.T) {
	var out, errBuf bytes.Buffer

	// --measure without --batches
	if code := run(context.Background(), []string{"run", "--measure"}, lookupFrom(nil), &out, &errBuf); code != 2 {
		t.Fatalf("run --measure without --batches exit = %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "--measure requires a finite run") {
		t.Fatalf("stderr missing requirement message: %q", errBuf.String())
	}

	// --measure with --batches 0
	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"run", "--batches", "0", "--measure"}, lookupFrom(nil), &out, &errBuf); code != 2 {
		t.Fatalf("run --batches 0 --measure exit = %d, want 2", code)
	}

	// --measure-capacity without --measure
	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"run", "--batches", "5", "--measure-capacity", "10"}, lookupFrom(nil), &out, &errBuf); code != 2 {
		t.Fatalf("run --measure-capacity without --measure exit = %d, want 2", code)
	}

	// --measure-capacity exceeds maximum
	out.Reset()
	errBuf.Reset()
	if code := run(context.Background(), []string{"run", "--batches", "5", "--measure", "--measure-capacity", "200000"}, lookupFrom(nil), &out, &errBuf); code != 2 {
		t.Fatalf("run --measure-capacity 200000 exit = %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "--measure-capacity must be between 1 and") {
		t.Fatalf("stderr missing capacity bounds message: %q", errBuf.String())
	}
}

func TestRun_ACKIgnoreFlagValidation(t *testing.T) {
	for _, args := range [][]string{
		{"run", "--ignore-first-valid-acks", "1"},
		{"run", "--batches", "1", "--ignore-first-valid-acks", "11"},
		{"run", "--stop-after-ignored-ack"},
		{"run", "--batches", "1", "--stop-after-ignored-ack"},
	} {
		var out, errOut bytes.Buffer
		if code := run(context.Background(), args, lookupFrom(nil), &out, &errOut); code != 2 {
			t.Fatalf("args %v exit=%d want 2", args, code)
		}
	}
}

func TestRun_StopAfterIgnoredACKIsQualifiedAndPreservesPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	fake := newFakeTransport(true)
	installFakeTransport(t, fake)
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"run", "--batches", "2", "--ignore-first-valid-acks", "1", "--stop-after-ignored-ack"}, lookupFrom(runEnv(path, nil)), &out, &errOut)
	if code == 0 {
		t.Fatalf("intentional fault stop reported successful completion: %q", out.String())
	}
	text := out.String()
	if !strings.Contains(text, "ignored_valid_acks=1 intentional_fault_stop=true") || !strings.Contains(text, "pending_rows=1") || !strings.Contains(text, "completed=false") {
		t.Fatalf("unqualified fault stop summary: %q", text)
	}
	if got := adminCounts(t, path); got.PendingRows != 1 {
		t.Fatalf("pending after stopped run=%d", got.PendingRows)
	}
}

func TestRun_FiniteBatch_WithMeasure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	fake := newFakeTransport(true)
	installFakeTransport(t, fake)

	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"run", "--batches", "2", "--completion-timeout", "5s", "--measure"},
		lookupFrom(runEnv(path, nil)), &out, &errBuf)
	if code != 0 {
		t.Fatalf("run exit = %d, stderr = %s", code, errBuf.String())
	}

	stdoutText := out.String()
	// Both run summary and measurement summary must be printed
	if !strings.Contains(stdoutText, "run summary: enqueued=2 publish_attempts=2 app_acknowledged=2 ignored_valid_acks=0 intentional_fault_stop=false pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true") {
		t.Fatalf("unexpected or missing run summary: %q", stdoutText)
	}

	if !strings.Contains(stdoutText, "measurement summary:") {
		t.Fatalf("missing measurement summary in output: %q", stdoutText)
	}
	if !strings.Contains(stdoutText, "samples=2") ||
		!strings.Contains(stdoutText, "first_publishes=2") ||
		!strings.Contains(stdoutText, "retry_attempts=0") ||
		!strings.Contains(stdoutText, "missing_origins=0") ||
		!strings.Contains(stdoutText, "overflow_samples=0") {
		t.Fatalf("unexpected measurement summary values: %q", stdoutText)
	}
	if !strings.Contains(stdoutText, "total_duration_ms=") ||
		!strings.Contains(stdoutText, "gen_duration_ms=") ||
		!strings.Contains(stdoutText, "publish_drain_duration_ms=") {
		t.Fatalf("missing window duration metrics in output: %q", stdoutText)
	}

	// Output must not contain secrets or payloads
	if !strings.Contains(stdoutText, "serialization summary: unique_bytes=") || !strings.Contains(stdoutText, "unique_samples=200") {
		t.Fatalf("missing bounded byte/sample counts: %q", stdoutText)
	}
	if strings.Contains(stdoutText, "test-only-gateway-password") || strings.Contains(stdoutText, "payload") {
		t.Fatalf("stdout leaked credential or payload: %q", stdoutText)
	}
}

func TestRun_FiniteBatch_WithoutMeasure_BackwardCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	fake := newFakeTransport(true)
	installFakeTransport(t, fake)

	var out, errBuf bytes.Buffer
	code := run(context.Background(), []string{"run", "--batches", "2", "--completion-timeout", "5s"},
		lookupFrom(runEnv(path, nil)), &out, &errBuf)
	if code != 0 {
		t.Fatalf("run exit = %d, stderr = %s", code, errBuf.String())
	}

	stdoutText := out.String()
	if !strings.Contains(stdoutText, "run summary: enqueued=2 publish_attempts=2 app_acknowledged=2 ignored_valid_acks=0 intentional_fault_stop=false pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true") {
		t.Fatalf("unexpected or missing run summary: %q", stdoutText)
	}

	// Must NOT contain measurement summary when --measure is not set
	if strings.Contains(stdoutText, "measurement summary:") {
		t.Fatalf("disabled measurement mode must not print measurement summary: %q", stdoutText)
	}
}
