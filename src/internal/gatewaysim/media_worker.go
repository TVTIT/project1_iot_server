package gatewaysim

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"iot-platform/internal/mediaprotocol"
)

const (
	// MediaTestDeclaredOverCap names the test-only malformed request case.
	MediaTestDeclaredOverCap = "declared-over-cap"
	// MediaTestStorageCapPlusOne names the test-only oversized PUT case.
	MediaTestStorageCapPlusOne = "storage-cap-plus-one"
)

// MediaPublisher abstracts the MQTT outbound publication seam for media worker notifications.
type MediaPublisher interface {
	PublishMedia(context.Context, any) error
}

// MediaTestPublisher exposes a raw publish path only for the explicit
// declared-over-cap simulator test case.
type MediaTestPublisher interface {
	PublishMediaTestPayload(context.Context, []byte) error
}

// MediaWorkerOptions configures the simulator's background media worker and storage client.
type MediaWorkerOptions struct {
	GatewayID, UploadOrigin, CAFile string
	Spool                           *MediaSpool
	Publisher                       MediaPublisher
	Capacity                        int
	RetryMin, RetryMax              time.Duration
	PutTimeout                      time.Duration
	TestCase                        string
	TestUploadID                    string
	httpClient                      *http.Client
}

type mediaDelivery struct {
	topic   string
	payload []byte
	retain  bool
}

// MediaWorker owns one bounded inbox and one serial consumer, isolated from
// telemetry and control callbacks.
type MediaWorker struct {
	opts          MediaWorkerOptions
	inbox         chan mediaDelivery
	stop          chan struct{}
	done          chan struct{}
	once          sync.Once
	startOnce     sync.Once
	doneOnce      sync.Once
	lifecycleMu   sync.Mutex
	started       bool
	cancel        context.CancelFunc
	testProbeSent bool
}

// NewMediaWorker constructs a validated media worker with bounded capacity and TLS configuration.
func NewMediaWorker(opts MediaWorkerOptions) (*MediaWorker, error) {
	u, err := url.Parse(opts.UploadOrigin)
	if validateMediaUploadOrigin(opts.UploadOrigin) != nil || err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" ||
		opts.GatewayID == "" || opts.Spool == nil || opts.Capacity <= 0 {
		return nil, errors.New("gatewaysim: invalid media worker configuration")
	}
	if opts.TestCase != "" && opts.TestCase != MediaTestDeclaredOverCap && opts.TestCase != MediaTestStorageCapPlusOne {
		return nil, errors.New("gatewaysim: invalid media test case")
	}
	if opts.TestCase != "" && !mediaUploadIDPattern.MatchString(opts.TestUploadID) {
		return nil, errors.New("gatewaysim: media test upload ID is required")
	}
	if opts.RetryMin <= 0 {
		opts.RetryMin = 5 * time.Second
	}
	if opts.RetryMax < opts.RetryMin {
		opts.RetryMax = time.Minute
	}
	if opts.PutTimeout <= 0 {
		opts.PutTimeout = time.Minute
	}
	if opts.httpClient == nil {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if opts.CAFile != "" {
			pem, err := os.ReadFile(opts.CAFile)
			if err != nil || !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("gatewaysim: invalid media CA file")
			}
		}
		opts.httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	} else {
		// Redirects can expose a signed query token to another origin.
		copyClient := *opts.httpClient
		copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		opts.httpClient = &copyClient
	}
	return &MediaWorker{opts: opts, inbox: make(chan mediaDelivery, opts.Capacity), stop: make(chan struct{}), done: make(chan struct{})}, nil
}

// Admit queues an inbound media MQTT message if capacity allows, discarding overflow.
func (w *MediaWorker) Admit(topic string, payload []byte, retained bool) {
	_ = w.TryAdmit(topic, payload, retained)
}

// TryAdmit attempts to enqueue an inbound media MQTT message into the bounded inbox.
func (w *MediaWorker) TryAdmit(topic string, payload []byte, retained bool) bool {
	if len(payload) > mediaprotocol.MaxEnvelopeBytes {
		return false
	}
	copyPayload := append([]byte(nil), payload...)
	select {
	case <-w.stop:
		return false
	case w.inbox <- mediaDelivery{topic: topic, payload: copyPayload, retain: retained}:
		return true
	default:
		return false
	}
}

// Start begins the media worker's background event loop and ticker.
func (w *MediaWorker) Start(ctx context.Context) {
	w.startOnce.Do(func() {
		workerCtx, cancel := context.WithCancel(ctx)
		w.lifecycleMu.Lock()
		w.started = true
		w.cancel = cancel
		w.lifecycleMu.Unlock()
		go func() {
			defer w.doneOnce.Do(func() { close(w.done) })
			tick := time.NewTicker(250 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-w.stop:
					return
				case d := <-w.inbox:
					if !d.retain {
						w.handle(workerCtx, d)
					}
				case <-tick.C:
					w.processDue(workerCtx)
				}
			}
		}()
	})
}

// Stop gracefully terminates the media worker loop and waits for its goroutine to finish.
func (w *MediaWorker) Stop(ctx context.Context) error {
	w.once.Do(func() { close(w.stop) })
	w.lifecycleMu.Lock()
	started := w.started
	cancel := w.cancel
	w.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !started {
		w.doneOnce.Do(func() { close(w.done) })
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetPublisher installs the already-constructed transport before Start.
func (w *MediaWorker) SetPublisher(publisher MediaPublisher) {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if !w.started {
		w.opts.Publisher = publisher
	}
}

func (w *MediaWorker) handle(ctx context.Context, d mediaDelivery) {
	if d.retain || (d.topic != mediaprotocol.UploadGrantTopic(w.opts.GatewayID) && d.topic != mediaprotocol.UploadResultTopic(w.opts.GatewayID)) {
		return
	}
	msg, err := mediaprotocol.Decode(d.topic, d.payload)
	if err != nil {
		return
	}
	switch m := msg.(type) {
	case mediaprotocol.UploadGrant:
		if m.GatewayID != w.opts.GatewayID {
			return
		}
		r, err := w.opts.Spool.Get(ctx, m.UploadID)
		if err != nil {
			return
		}
		switch m.Status {
		case mediaprotocol.GrantNeedUpload:
			u, err := url.Parse(m.UploadURL)
			expiry, timeErr := time.Parse(time.RFC3339, m.ExpiresAt)
			base, _ := url.Parse(w.opts.UploadOrigin)
			if err != nil || timeErr != nil || !validMediaTarget(u, base, m.MediaID) || expiry.Before(time.Now()) || expiry.After(r.Deadline) || (r.MediaID != "" && r.MediaID != m.MediaID) {
				return
			}
			if w.opts.Spool.MarkGranted(ctx, m.UploadID, m.MediaID, expiry) != nil {
				return
			}
			w.put(ctx, m.UploadID, m.MediaID, m.UploadURL, expiry)
		case mediaprotocol.GrantRetryAfterCompletion:
			if !canonicalMediaID(m.MediaID) {
				return
			}
			if r.MediaID != "" && r.MediaID != m.MediaID {
				return
			}
			due := time.Now().Add(w.retryDelay(r.Attempts))
			if err := w.opts.Spool.MoveToReconcile(ctx, m.UploadID, due, m.MediaID); err == nil {
				_ = w.opts.Publisher.PublishMedia(ctx, mediaprotocol.UploadComplete{Version: 1, GatewayID: w.opts.GatewayID, UploadID: m.UploadID})
			}
		case mediaprotocol.GrantRejected, mediaprotocol.GrantExpired, mediaprotocol.GrantConflict:
			_ = w.opts.Spool.RecordTerminal(ctx, m.UploadID, m.MediaID, m.Status, m.Code, time.Now())
		}
	case mediaprotocol.UploadResult:
		if m.GatewayID != w.opts.GatewayID {
			return
		}
		r, err := w.opts.Spool.Get(ctx, m.UploadID)
		if err == nil && (m.MediaID == "" || r.MediaID == "" || r.MediaID == m.MediaID) {
			now := time.Now()
			if e := w.opts.Spool.ApplyMQTTResultTopic(ctx, d.topic, d.payload, now, now.Add(w.retryDelay(r.Attempts))); e == nil &&
				(m.Status == mediaprotocol.ResultPending || m.Status == mediaprotocol.ResultVerifying) {
				after, getErr := w.opts.Spool.Get(ctx, m.UploadID)
				if getErr == nil && after.Phase == "awaiting_result" {
					_ = w.opts.Spool.SchedulePoll(ctx, m.UploadID, now.Add(w.retryDelay(r.Attempts)))
				}
			}
		}
	}
}

func (w *MediaWorker) processDue(ctx context.Context) {
	rows, err := w.opts.Spool.List(ctx, time.Now())
	if err != nil {
		return
	}
	for _, r := range rows {
		now := time.Now()
		if !r.Deadline.IsZero() && !now.Before(r.Deadline) {
			_ = w.opts.Spool.Fail(ctx, r.UploadID, "INTENT_TTL_EXPIRED", now)
			continue
		}
		switch r.Phase {
		case "awaiting_grant":
			if w.opts.TestCase == MediaTestDeclaredOverCap && r.UploadID == w.opts.TestUploadID {
				w.publishDeclaredOverCapProbe(ctx, r)
				continue
			}
			due := now.Add(w.retryDelay(r.Attempts))
			if err := w.opts.Spool.RecordAttempt(ctx, r.UploadID, due); err != nil {
				if errors.Is(err, ErrMediaSpoolAttempts) {
					_ = w.opts.Spool.Fail(ctx, r.UploadID, "RETRY_LIMIT", now)
				}
				continue
			}
			req := mediaprotocol.UploadRequest{Version: 1, GatewayID: w.opts.GatewayID, UploadID: r.UploadID, CapturedAt: r.CapturedAt.Format(time.RFC3339Nano), ContentType: "video/mp4", SizeBytes: r.SizeBytes, SHA256: r.SHA256}
			_ = w.opts.Publisher.PublishMedia(ctx, req)
		case "granted", "put_may_have_happened":
			due := now.Add(w.retryDelay(r.Attempts))
			if err := w.opts.Spool.MoveToReconcile(ctx, r.UploadID, due); err == nil {
				_ = w.opts.Publisher.PublishMedia(ctx, mediaprotocol.UploadComplete{Version: 1, GatewayID: w.opts.GatewayID, UploadID: r.UploadID})
			}
		case "awaiting_result":
			due := now.Add(w.retryDelay(r.Attempts))
			if err := w.opts.Spool.SchedulePoll(ctx, r.UploadID, due); err == nil {
				_ = w.opts.Publisher.PublishMedia(ctx, mediaprotocol.UploadComplete{Version: 1, GatewayID: w.opts.GatewayID, UploadID: r.UploadID})
			}
		}
	}
}

func (w *MediaWorker) publishDeclaredOverCapProbe(ctx context.Context, r MediaUpload) {
	if w.testProbeSent {
		return
	}
	w.testProbeSent = true
	publisher, ok := w.opts.Publisher.(MediaTestPublisher)
	if !ok {
		slog.Warn("gatewaysim media test probe unavailable", "case", MediaTestDeclaredOverCap)
		return
	}
	request := mediaprotocol.UploadRequest{
		Version: 1, GatewayID: w.opts.GatewayID, UploadID: r.UploadID,
		CapturedAt: r.CapturedAt.Format(time.RFC3339Nano), ContentType: "video/mp4",
		SizeBytes: r.SizeBytes, SHA256: r.SHA256,
	}
	payload, err := declaredOverCapTestPayload(request)
	if err == nil {
		err = publisher.PublishMediaTestPayload(ctx, payload)
	}
	if err != nil {
		slog.Warn("gatewaysim media test probe publish failed", "case", MediaTestDeclaredOverCap)
		return
	}
	slog.Info("gatewaysim media test probe published", "case", MediaTestDeclaredOverCap,
		"declared_bytes", r.SizeBytes+1, "broker_ack_only", true)
}

func (w *MediaWorker) put(ctx context.Context, id, mediaID, rawURL string, expiry time.Time) {
	r, err := w.opts.Spool.Get(ctx, id)
	if err != nil {
		return
	}
	if err = w.opts.Spool.MarkPutMayHaveStarted(ctx, id, expiry); err != nil {
		return
	}
	f, err := w.opts.Spool.OpenFile(ctx, r)
	if err != nil {
		_ = w.opts.Spool.MarkAwaitingResult(ctx, id, time.Now().Add(w.retryDelay(r.Attempts)))
		return
	}
	defer func() {
		_ = f.Close()
	}()
	u, _ := url.Parse(rawURL)
	base, _ := url.Parse(w.opts.UploadOrigin)
	if !validMediaTarget(u, base, mediaID) {
		_ = w.opts.Spool.MarkAwaitingResult(ctx, id, time.Now().Add(w.retryDelay(r.Attempts)))
		return
	}
	putDeadline := time.Now().Add(w.opts.PutTimeout)
	if expiry.Before(putDeadline) {
		putDeadline = expiry
	}
	putCtx, cancel := context.WithDeadline(ctx, putDeadline)
	defer cancel()
	testCase := ""
	if id == w.opts.TestUploadID {
		testCase = w.opts.TestCase
	}
	body, contentLength, err := mediaPUTBodyForTest(f, r.SizeBytes, testCase)
	if err != nil {
		_ = w.opts.Spool.MarkAwaitingResult(ctx, id, time.Now().Add(w.retryDelay(r.Attempts)))
		return
	}
	req, err := http.NewRequestWithContext(putCtx, http.MethodPut, rawURL, body)
	if err != nil {
		_ = w.opts.Spool.MarkAwaitingResult(ctx, id, time.Now().Add(w.retryDelay(r.Attempts)))
		return
	}
	req.ContentLength, req.Header = contentLength, make(http.Header)
	req.Header.Set("Content-Type", "video/mp4")
	resp, err := w.opts.httpClient.Do(req)
	if err == nil {
		if testCase == MediaTestStorageCapPlusOne {
			slog.Info("gatewaysim media test PUT completed", "case", MediaTestStorageCapPlusOne,
				"declared_bytes", r.SizeBytes, "content_length_bytes", contentLength, "http_status", resp.StatusCode)
		}
		_ = resp.Body.Close()
	} else if testCase == MediaTestStorageCapPlusOne {
		slog.Warn("gatewaysim media test PUT failed", "case", MediaTestStorageCapPlusOne,
			"declared_bytes", r.SizeBytes, "content_length_bytes", contentLength, "error_class", fmt.Sprintf("%T", err))
	}
	// A non-2xx, redirect, timeout or lost response is ambiguous. Never repeat
	// bytes; reconcile by upload_complete with the durable ID.
	due := time.Now().Add(w.retryDelay(r.Attempts))
	if err := w.opts.Spool.MarkAwaitingResult(ctx, id, due); err == nil {
		_ = w.opts.Publisher.PublishMedia(ctx, mediaprotocol.UploadComplete{Version: 1, GatewayID: w.opts.GatewayID, UploadID: id})
	}
}

func mediaPUTBodyForTest(body io.Reader, declaredBytes int64, testCase string) (io.Reader, int64, error) {
	if body == nil || declaredBytes < 1 {
		return nil, 0, errors.New("invalid media PUT body")
	}
	switch testCase {
	case "":
		return body, declaredBytes, nil
	case MediaTestStorageCapPlusOne:
		if declaredBytes != mediaprotocol.MaxUploadBytes {
			return nil, 0, errors.New("storage cap probe requires exact-cap declaration")
		}
		return io.MultiReader(body, bytes.NewReader([]byte{0})), declaredBytes + 1, nil
	default:
		return nil, 0, errors.New("unsupported media test case")
	}
}

func declaredOverCapTestPayload(request mediaprotocol.UploadRequest) ([]byte, error) {
	if request.SizeBytes != mediaprotocol.MaxUploadBytes || request.UploadID == "" ||
		request.GatewayID == "" || request.ContentType != "video/mp4" {
		return nil, errors.New("declared-size probe requires a valid exact-cap request")
	}
	if _, err := mediaprotocol.Encode(mediaprotocol.UploadRequestTopic(request.GatewayID), request); err != nil {
		return nil, errors.New("declared-size probe requires a valid exact-cap request")
	}
	request.SizeBytes++
	return json.Marshal(request)
}

func (w *MediaWorker) retryDelay(attempt int) time.Duration {
	delay := w.opts.RetryMin
	for i := 0; i < attempt && delay < w.opts.RetryMax; i++ {
		delay *= 2
		if delay > w.opts.RetryMax {
			delay = w.opts.RetryMax
		}
	}
	return delay
}

func validMediaTarget(target, origin *url.URL, mediaID string) bool {
	if target == nil || origin == nil || !canonicalMediaID(mediaID) ||
		target.Scheme != "https" || !strings.EqualFold(target.Hostname(), origin.Hostname()) ||
		target.Port() != origin.Port() || target.User != nil || target.Fragment != "" ||
		target.EscapedPath() != "/storage/v1/object/upload/sign/media-video/"+mediaID+".mp4" {
		return false
	}
	query, err := url.ParseQuery(target.RawQuery)
	return err == nil && len(query) == 1 && len(query["token"]) == 1 && query.Get("token") != ""
}
