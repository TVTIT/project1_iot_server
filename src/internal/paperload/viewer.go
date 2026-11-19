package paperload

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetryws"
)

const viewerFrameLimit = 66560

type viewerACK struct {
	ProtocolVersion int64                   `json:"protocol_version"`
	MessageType     string                  `json:"message_type"`
	RequestID       string                  `json:"request_id"`
	Action          string                  `json:"action"`
	Targets         []telemetryws.TargetKey `json:"targets"`
}
type viewerEvent struct {
	ProtocolVersion  int64     `json:"protocol_version"`
	MessageType      string    `json:"message_type"`
	EntityID         string    `json:"entity_id"`
	GatewayID        string    `json:"gateway_id"`
	SensorID         string    `json:"sensor_id"`
	MessageID        string    `json:"message_id"`
	BootID           string    `json:"boot_id"`
	FirstSequence    string    `json:"first_sequence"`
	SampleCount      int32     `json:"sample_count"`
	MeasuredAt       string    `json:"measured_at"`
	SampleIntervalUS int64     `json:"sample_interval_us"`
	Samples          []float64 `json:"samples"`
}

// ViewerResult is the credential-free, bounded summary of one viewer run.
type ViewerResult struct {
	Status                   string                 `json:"status"`
	Implementation           string                 `json:"implementation_status"`
	Experiment               string                 `json:"experiment"`
	Arm                      string                 `json:"arm"`
	Profile                  string                 `json:"profile"`
	Mode                     string                 `json:"mode"`
	RunID                    string                 `json:"run_id"`
	StartedAt                string                 `json:"started_at"`
	FinishedAt               string                 `json:"finished_at"`
	EffectiveIdentityWindow  int                    `json:"effective_identity_window_per_viewer"`
	ConnectionAttempts       int                    `json:"connection_attempts"`
	Connected                int                    `json:"connected_distinct_viewers"`
	Reconnects               int                    `json:"reconnections"`
	Subscribed               int                    `json:"subscribed_distinct_viewers"`
	Receiving                int                    `json:"receiving_distinct_viewers"`
	Unique                   int64                  `json:"unique_events"`
	Duplicates               int64                  `json:"duplicates"`
	WrongNamespace           int64                  `json:"wrong_namespace"`
	Messages                 int64                  `json:"messages"`
	Bytes                    int64                  `json:"bytes"`
	CloseReasons             map[string]int         `json:"close_reasons"`
	LifetimesMS              []int64                `json:"lifetime_ms"`
	SocketCount              int                    `json:"socket_count"`
	PrincipalCount           int                    `json:"principal_count"`
	LatencyAvailable         bool                   `json:"latency_available"`
	LatencyUnavailableReason string                 `json:"latency_unavailable_reason"`
	Viewers                  []ViewerSummary        `json:"viewers"`
	PrimaryError             string                 `json:"primary_error,omitempty"`
	IdentitySamples          []ViewerIdentitySample `json:"identity_samples,omitempty"`
	IdentitySampling         string                 `json:"identity_sampling_qualification"`
	PlannedStop              bool                   `json:"planned_stop,omitempty"`
	PlannedStopReason        string                 `json:"planned_stop_reason,omitempty"`
}

// ViewerIdentitySample is header/hash provenance only; full telemetry values are never retained.
type ViewerIdentitySample struct {
	ViewerID      string `json:"viewer_id"`
	GatewayID     string `json:"gateway_id"`
	SensorID      string `json:"sensor_id"`
	MessageID     string `json:"message_id"`
	BootID        string `json:"boot_id"`
	FirstSequence string `json:"first_sequence"`
	SampleCount   int32  `json:"sample_count"`
	HeaderSHA256  string `json:"header_sha256"`
}

type viewerReady struct {
	Status          string `json:"status"`
	Connected       int    `json:"connected"`
	Subscribed      int    `json:"subscribed"`
	Principals      int    `json:"principals"`
	Mode            string `json:"mode"`
	MonotonicOrigin string `json:"monotonic_origin_qualification"`
}

// ViewerSummary holds bounded per-viewer counts, not event payloads.
type ViewerSummary struct {
	ID          string `json:"id"`
	Events      int64  `json:"events"`
	Unique      int64  `json:"unique_events"`
	Duplicates  int64  `json:"duplicates"`
	Bytes       int64  `json:"bytes"`
	CloseReason string `json:"close_reason,omitempty"`
}
type viewerIdentity struct {
	sub string
	exp int64
}
type viewerSession struct {
	id, user, token string
	filters         []telemetryws.TargetKey
}
type viewerSocket struct {
	viewer  *viewerSession
	conn    *websocket.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	started time.Time
}
type viewerShared struct {
	mu          sync.Mutex
	result      ViewerResult
	seen        map[string]map[string]struct{}
	order       map[string][]string
	summaries   map[string]*ViewerSummary
	window      int
	maxMessages int64
	maxBytes    int64
	receiving   map[string]bool
	connected   map[string]bool
	subscribed  map[string]bool
}

func readViewerTokens(path string) (map[string]string, error) {
	st, e := os.Lstat(path)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() < 1 || st.Size() > 1<<20 {
		return nil, errors.New("viewer_identity_file_rejected")
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, errors.New("viewer_identity_file_rejected")
	}
	var tokens map[string]string
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if d.Decode(&tokens) != nil || len(tokens) == 0 || len(tokens) > MaxViewers {
		return nil, errors.New("viewer_identity_file_rejected")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("viewer_identity_file_rejected")
	}
	for label, token := range tokens {
		if !safeID.MatchString(label) || token == "" || strings.ContainsAny(token, "\r\n") {
			return nil, errors.New("viewer_identity_file_rejected")
		}
	}
	return tokens, nil
}
func viewerClaims(token string) (viewerIdentity, error) {
	p := strings.Split(token, ".")
	if len(p) != 3 {
		return viewerIdentity{}, errors.New("viewer_jwt_rejected")
	}
	b, e := base64.RawURLEncoding.DecodeString(p[1])
	if e != nil {
		return viewerIdentity{}, errors.New("viewer_jwt_rejected")
	}
	var c struct {
		Sub string      `json:"sub"`
		Exp json.Number `json:"exp"`
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	if d.Decode(&c) != nil {
		return viewerIdentity{}, errors.New("viewer_jwt_rejected")
	}
	exp, e := strconv.ParseInt(string(c.Exp), 10, 64)
	if e != nil || c.Sub == "" || exp <= time.Now().Unix() {
		return viewerIdentity{}, errors.New("viewer_jwt_rejected")
	}
	return viewerIdentity{sub: c.Sub, exp: exp}, nil
}

// RunViewer starts a bounded viewer-only run. Handshake concurrency is separately
// limited by workers; a successful handshake releases that slot while its reader lives.
func RunViewer(ctx context.Context, m Manifest, manifestPath, outputDir string) (ViewerResult, error) {
	return runViewer(ctx, m, manifestPath, outputDir, nil)
}

// RunViewerWithPlannedStop permits an explicit operator stop after every viewer
// has connected, subscribed, started its reader, and published the ready marker.
// Ordinary context cancellation and the manifest wall limit remain failures.
func RunViewerWithPlannedStop(ctx context.Context, m Manifest, manifestPath, outputDir string, stop <-chan struct{}) (ViewerResult, error) {
	if stop == nil {
		return ViewerResult{}, errors.New("planned_stop_channel_required")
	}
	return runViewer(ctx, m, manifestPath, outputDir, stop)
}

func runViewer(ctx context.Context, m Manifest, manifestPath, outputDir string, plannedStop <-chan struct{}) (ViewerResult, error) {
	r := ViewerResult{Status: "FAILED", Implementation: "P8.4c viewer-only client; no control/durable/server orchestration", Experiment: m.Experiment, Arm: m.Arm, Profile: m.Profile, Mode: m.Mode, RunID: m.RunID, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), CloseReasons: map[string]int{}, LatencyUnavailableReason: "publisher_origin_not_available_in_viewer_role", IdentitySampling: "header_sha256_only; max 20 globally and 1 per viewer; not a full-payload audit"}
	if e := os.Mkdir(outputDir, 0700); e != nil {
		return r, errors.New("output_directory_must_be_fresh")
	}
	base := filepath.Dir(manifestPath)
	resolve := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(base, p)
	}
	tokens, e := readViewerTokens(resolve(m.IdentityFiles["viewer"]))
	if e != nil {
		r.PrimaryError = "viewer_identity_rejected"
		return finishViewer(outputDir, r, e)
	}
	ca, e := readBoundedRegular(resolve(m.TLS.HTTPCAFile), 1<<20)
	if e != nil {
		r.PrimaryError = "viewer_ca_rejected"
		return finishViewer(outputDir, r, errors.New("viewer_ca_rejected"))
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		r.PrimaryError = "viewer_ca_rejected"
		return finishViewer(outputDir, r, errors.New("viewer_ca_rejected"))
	}
	endpoint, _ := url.Parse(m.TLS.HTTPEndpoint)
	endpoint.Scheme = "wss"
	endpoint.Path = "/v1/ws"
	byID := map[string]*viewerSession{}
	for _, sub := range m.Subscriptions {
		v := byID[sub.ViewerID]
		if v == nil {
			v = &viewerSession{id: sub.ViewerID, user: sub.UserID}
			byID[sub.ViewerID] = v
		}
		v.filters = append(v.filters, telemetryws.TargetKey{GatewayID: sub.GatewayID, SensorID: sub.SensorID})
	}
	principals := map[string]bool{}
	for _, v := range byID {
		token, ok := tokens[v.user]
		if !ok {
			r.PrimaryError = "viewer_jwt_rejected"
			return finishViewer(outputDir, r, errors.New("viewer_jwt_rejected"))
		}
		claims, err := viewerClaims(token)
		if err != nil {
			r.PrimaryError = "viewer_jwt_rejected"
			return finishViewer(outputDir, r, err)
		}
		if claims.exp <= time.Now().Add(time.Duration(m.Limits.WallSeconds)*time.Second).Unix() {
			r.PrimaryError = "viewer_jwt_lifetime_insufficient"
			return finishViewer(outputDir, r, errors.New("viewer_jwt_lifetime_insufficient"))
		}
		principals[claims.sub] = true
		v.token = token
	}
	orderedViewers := make([]*viewerSession, 0, len(byID))
	for _, v := range byID {
		orderedViewers = append(orderedViewers, v)
	}
	sort.Slice(orderedViewers, func(i, j int) bool {
		return orderedViewers[i].id < orderedViewers[j].id
	})
	window := m.Viewer.IdentityWindow
	if window == 0 {
		window = 1024
	}
	if cap := 250000 / len(byID); cap < window {
		window = max(1, cap)
	}
	r.EffectiveIdentityWindow = window
	r.SocketCount = len(byID)
	r.PrincipalCount = len(principals)
	maxMessages := m.Viewer.MaxMessages
	if maxMessages == 0 {
		maxMessages = 100000
	}
	maxBytes := m.Viewer.MaxBytes
	if maxBytes == 0 {
		maxBytes = 64 << 20
	}
	shared := &viewerShared{result: r, seen: map[string]map[string]struct{}{}, order: map[string][]string{}, summaries: map[string]*ViewerSummary{}, window: window, maxMessages: int64(maxMessages), maxBytes: maxBytes, receiving: map[string]bool{}, connected: map[string]bool{}, subscribed: map[string]bool{}}
	for id := range byID {
		shared.summaries[id] = &ViewerSummary{ID: id}
	}
	wall, cancel := context.WithTimeout(ctx, time.Duration(m.Limits.WallSeconds)*time.Second)
	defer cancel()
	handshakes := make(chan struct{}, m.Limits.Workers)
	errs := make(chan error, len(byID))
	var socketsMu sync.Mutex
	sockets := map[string]*viewerSocket{}
	startAll := func(paced bool) error {
		ramp := time.Duration(0)
		if paced && m.Traffic.RampSeconds > 0 {
			ramp = time.Duration(m.Traffic.RampSeconds) * time.Second
		}
		var wg sync.WaitGroup
		startTime := time.Now()
		total := len(orderedViewers)
		cancelled := false

		for i, v := range orderedViewers {
			targetOffset := viewerLaunchOffset(i, total, ramp)
			if targetOffset > 0 {
				delay := targetOffset - time.Since(startTime)
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-wall.Done():
						timer.Stop()
						cancelled = true
					}
				}
			}
			if cancelled || wall.Err() != nil {
				break
			}

			wg.Add(1)
			go func(v *viewerSession) {
				defer wg.Done()
				select {
				case handshakes <- struct{}{}:
				case <-wall.Done():
					select {
					case errs <- wall.Err():
					default:
					}
					return
				}
				sock, err := connectViewer(wall, endpoint.String(), roots, v, handshakes, shared)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				socketsMu.Lock()
				sockets[v.id] = sock
				socketsMu.Unlock()
				go readViewer(sock, shared, errs, time.Duration(m.Viewer.SlowPauseMS)*time.Millisecond)
			}(v)
		}

		wg.Wait()
		socketsMu.Lock()
		n := len(sockets)
		socketsMu.Unlock()
		if n != total || wall.Err() != nil {
			if err := wall.Err(); err != nil {
				return err
			}
			return errors.New("viewer_connection_failed")
		}
		return nil
	}
	stopAll := func(reason string) {
		socketsMu.Lock()
		active := make([]*viewerSocket, 0, len(sockets))
		for _, s := range sockets {
			active = append(active, s)
		}
		sockets = map[string]*viewerSocket{}
		socketsMu.Unlock()
		for _, s := range active {
			s.cancel()
			_ = s.conn.Close(websocket.StatusNormalClosure, reason)
		}
		for _, s := range active {
			<-s.done
			shared.mu.Lock()
			shared.result.LifetimesMS = append(shared.result.LifetimesMS, time.Since(s.started).Milliseconds())
			if shared.summaries[s.viewer.id] != nil && shared.summaries[s.viewer.id].CloseReason == "" {
				shared.summaries[s.viewer.id].CloseReason = reason
			}
			shared.mu.Unlock()
		}
	}
	if err := startAll(true); err != nil {
		stopAll("setup_failed")
		shared.mu.Lock()
		shared.result.PrimaryError = "viewer_connection_or_subscribe_failed"
		shared.mu.Unlock()
		return finishViewer(outputDir, shared.result, err)
	}
	// The marker is an atomic, private rendezvous: every subscribe ACK was validated
	// and every reader goroutine was launched before this point.
	stoppedBeforeReady, err := publishViewerReady(outputDir, viewerReady{Status: "ready", Connected: len(byID), Subscribed: len(byID), Principals: len(principals), Mode: m.Viewer.Mode, MonotonicOrigin: "process_local_time_since_start; not comparable across processes/hosts"}, plannedStop, nil)
	if stoppedBeforeReady {
		stopAll("planned_stop_before_ready")
		return viewerFailure(outputDir, shared, "planned_stop_before_ready")
	}
	if err != nil {
		stopAll("ready_write_failed")
		return viewerFailure(outputDir, shared, "viewer_ready_write_failed")
	}
	if stopped, ok := viewerWaitWithPlannedStop(wall, time.Duration(m.Phases.WarmupSeconds)*time.Second, errs, plannedStop); !ok {
		stopAll("warmup_failed")
		return viewerFailure(outputDir, shared, "viewer_transport_failed")
	} else if stopped {
		return completePlannedViewer(wall, outputDir, shared, errs, stopAll)
	}
	if m.Viewer.Mode == "reconnect" {
		for i := 0; i < m.Viewer.ReconnectCount; i++ {
			stopAll("planned_reconnect")
			if e := startAll(false); e != nil {
				stopAll("reconnect_failed")
				return viewerFailure(outputDir, shared, "viewer_reconnect_failed")
			}
			shared.mu.Lock()
			shared.result.Reconnects += len(byID)
			shared.mu.Unlock()
		}
	}
	if stopped, ok := viewerWaitWithPlannedStop(wall, time.Duration(m.Phases.MeasureSeconds)*time.Second, errs, plannedStop); !ok {
		stopAll("measure_failed")
		return viewerFailure(outputDir, shared, "viewer_transport_failed")
	} else if stopped {
		return completePlannedViewer(wall, outputDir, shared, errs, stopAll)
	}
	// Keep readers alive through the publisher's finite drain window; readiness,
	// handshake/ramp and steady measurement remain separate lifecycle phases.
	if stopped, ok := viewerWaitWithPlannedStop(wall, time.Duration(m.Phases.DrainSeconds)*time.Second, errs, plannedStop); !ok {
		stopAll("drain_failed")
		return viewerFailure(outputDir, shared, "viewer_transport_failed")
	} else if stopped {
		return completePlannedViewer(wall, outputDir, shared, errs, stopAll)
	}
	select {
	case <-errs:
		stopAll("unexpected_transport_end")
		return viewerFailure(outputDir, shared, "viewer_transport_failed")
	default:
	}
	stopAll("measurement_complete")
	shared.mu.Lock()
	r = shared.result
	for _, summary := range shared.summaries {
		r.Viewers = append(r.Viewers, *summary)
	}
	shared.mu.Unlock()
	r.Status = "COMPLETE"
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeViewerResult(outputDir, r); err != nil {
		return r, errors.New("result_artifact_write_failed")
	}
	return r, nil
}

func connectViewer(ctx context.Context, endpoint string, roots *x509.CertPool, v *viewerSession, slots chan struct{}, s *viewerShared) (*viewerSocket, error) {
	defer func() { <-slots }()
	s.mu.Lock()
	s.result.ConnectionAttempts++
	s.mu.Unlock()
	hdr := http.Header{"Authorization": []string{"Bearer " + v.token}}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: hdr, Subprotocols: []string{"iot.telemetry.v1"}, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: identityHost(endpoint)}}}})
	if err != nil {
		s.mu.Lock()
		reason := "connect_failed"
		if response != nil && (response.StatusCode == 401 || response.StatusCode == 403) {
			reason = "handshake_denied"
		}
		s.result.CloseReasons[reason]++
		s.mu.Unlock()
		return nil, errors.New("viewer_connect_failed")
	}
	if conn.Subprotocol() != "iot.telemetry.v1" {
		_ = conn.Close(websocket.StatusProtocolError, "subprotocol mismatch")
		s.mu.Lock()
		s.result.CloseReasons["bad_subprotocol"]++
		s.mu.Unlock()
		return nil, errors.New("viewer_subprotocol_invalid")
	}
	conn.SetReadLimit(viewerFrameLimit)
	requestID := uuid.NewString()
	raw, _ := json.Marshal(struct {
		ProtocolVersion int64                   `json:"protocol_version"`
		RequestID       string                  `json:"request_id"`
		Action          string                  `json:"action"`
		Targets         []telemetryws.TargetKey `json:"targets"`
	}{1, requestID, "subscribe", v.filters})
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if conn.Write(ackCtx, websocket.MessageText, raw) != nil {
		_ = conn.Close(websocket.StatusInternalError, "subscribe write failed")
		return nil, errors.New("viewer_subscribe_write_failed")
	}
	typ, body, err := conn.Read(ackCtx)
	if err != nil || typ != websocket.MessageText || !validViewerACK(body, requestID, v.filters) {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid subscribe ack")
		s.mu.Lock()
		s.result.CloseReasons["invalid_ack"]++
		s.mu.Unlock()
		return nil, errors.New("viewer_subscribe_ack_invalid")
	}
	readerCtx, readerCancel := context.WithCancel(ctx)
	s.mu.Lock()
	if !s.connected[v.id] {
		s.connected[v.id] = true
		s.result.Connected++
	}
	if !s.subscribed[v.id] {
		s.subscribed[v.id] = true
		s.result.Subscribed++
	}
	s.mu.Unlock()
	return &viewerSocket{viewer: v, conn: conn, ctx: readerCtx, cancel: readerCancel, done: make(chan struct{}), started: time.Now()}, nil
}
func readViewer(sock *viewerSocket, s *viewerShared, errs chan<- error, slowPause time.Duration) {
	defer close(sock.done)
	for {
		if slowPause > 0 {
			t := time.NewTimer(slowPause)
			select {
			case <-t.C:
			case <-sock.ctx.Done():
				t.Stop()
				return
			}
		}
		typ, payload, err := sock.conn.Read(sock.ctx)
		if err != nil {
			if sock.ctx.Err() != nil {
				return
			}
			status := websocket.CloseStatus(err)
			reason := fmt.Sprintf("remote_close_%d", status)
			if errors.Is(err, websocket.ErrMessageTooBig) {
				reason = "frame_limit_exceeded"
			} else if int(status) < 0 || status == websocket.StatusAbnormalClosure {
				reason = "read_error"
			}
			s.mu.Lock()
			s.result.CloseReasons[reason]++
			s.summaries[sock.viewer.id].CloseReason = reason
			s.mu.Unlock()
			errs <- errors.New("viewer_read_failed")
			return
		}
		if typ != websocket.MessageText {
			viewerBad(s, sock, "malformed_frame")
			errs <- errors.New("viewer_frame_invalid")
			return
		}
		s.mu.Lock()
		if s.result.Messages >= s.maxMessages || s.result.Bytes+int64(len(payload)) > s.maxBytes {
			s.result.CloseReasons["limit_exceeded"]++
			s.mu.Unlock()
			errs <- errors.New("viewer_limit_exceeded")
			return
		}
		s.result.Messages++
		s.result.Bytes += int64(len(payload))
		sum := s.summaries[sock.viewer.id]
		sum.Events++
		sum.Bytes += int64(len(payload))
		s.receiving[sock.viewer.id] = true
		s.result.Receiving = len(s.receiving)
		s.mu.Unlock()
		var ev viewerEvent
		if json.Unmarshal(payload, &ev) != nil || !validViewerEvent(ev) {
			viewerBad(s, sock, "malformed_event")
			errs <- errors.New("viewer_event_invalid")
			return
		}
		if !viewerMatches(sock.viewer.filters, ev.GatewayID, ev.SensorID) {
			s.mu.Lock()
			s.result.WrongNamespace++
			s.mu.Unlock()
			viewerBad(s, sock, "wrong_namespace")
			errs <- errors.New("viewer_wrong_namespace")
			return
		}
		key := strings.Join([]string{ev.GatewayID, ev.SensorID, ev.MessageID, ev.BootID, ev.FirstSequence}, "\x00")
		s.mu.Lock()
		if len(s.result.IdentitySamples) < 20 {
			perViewer := 0
			for _, sample := range s.result.IdentitySamples {
				if sample.ViewerID == sock.viewer.id {
					perViewer++
				}
			}
			if perViewer == 0 {
				header, _ := json.Marshal(struct {
					GatewayID     string `json:"gateway_id"`
					SensorID      string `json:"sensor_id"`
					MessageID     string `json:"message_id"`
					BootID        string `json:"boot_id"`
					FirstSequence string `json:"first_sequence"`
					SampleCount   int32  `json:"sample_count"`
				}{ev.GatewayID, ev.SensorID, ev.MessageID, ev.BootID, ev.FirstSequence, ev.SampleCount})
				hash := sha256.Sum256(header)
				s.result.IdentitySamples = append(s.result.IdentitySamples, ViewerIdentitySample{sock.viewer.id, ev.GatewayID, ev.SensorID, ev.MessageID, ev.BootID, ev.FirstSequence, ev.SampleCount, hex.EncodeToString(hash[:])})
			}
		}
		seen := s.seen[sock.viewer.id]
		if seen == nil {
			seen = map[string]struct{}{}
			s.seen[sock.viewer.id] = seen
		}
		if _, ok := seen[key]; ok {
			s.result.Duplicates++
			s.summaries[sock.viewer.id].Duplicates++
		} else {
			s.result.Unique++
			s.summaries[sock.viewer.id].Unique++
			seen[key] = struct{}{}
			q := append(s.order[sock.viewer.id], key)
			if len(q) > s.window {
				delete(seen, q[0])
				q = q[1:]
			}
			s.order[sock.viewer.id] = q
		}
		s.mu.Unlock()
	}
}
func validViewerEvent(e viewerEvent) bool {
	seq, err := strconv.ParseInt(e.FirstSequence, 10, 64)
	if err != nil || seq < 0 || strconv.FormatInt(seq, 10) != e.FirstSequence || e.ProtocolVersion != 1 || e.MessageType != "telemetry" || gateway.ValidateGatewayID(e.GatewayID) != nil || gateway.ValidateSensorID(e.SensorID) != nil || uuid.Validate(e.MessageID) != nil || uuid.Validate(e.BootID) != nil || e.EntityID != (telemetryws.TargetKey{GatewayID: e.GatewayID, SensorID: e.SensorID}).EntityURN() || e.SampleCount <= 0 || int(e.SampleCount) != len(e.Samples) || e.SampleIntervalUS <= 0 {
		return false
	}
	if _, err = time.Parse(time.RFC3339Nano, e.MeasuredAt); err != nil {
		return false
	}
	return true
}
func viewerBad(s *viewerShared, sock *viewerSocket, reason string) {
	s.mu.Lock()
	s.result.CloseReasons[reason]++
	s.summaries[sock.viewer.id].CloseReason = reason
	s.mu.Unlock()
}
func viewerWait(ctx context.Context, d time.Duration, errs <-chan error) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-errs:
		return false
	}
}

// viewerWaitWithPlannedStop distinguishes a deliberate stop from every bounded
// failure. Failure signals are checked before and after selecting the operator
// channel so a simultaneous transport error or wall deadline cannot become PASS.
func viewerWaitWithPlannedStop(ctx context.Context, d time.Duration, errs <-chan error, planned <-chan struct{}) (stopped, ok bool) {
	if planned == nil {
		return false, viewerWait(ctx, d, errs)
	}
	if ctx.Err() != nil {
		return false, false
	}
	select {
	case <-errs:
		return false, false
	default:
	}
	if d <= 0 {
		return false, true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return false, ctx.Err() == nil
	case <-ctx.Done():
		return false, false
	case <-errs:
		return false, false
	case <-planned:
		if ctx.Err() != nil {
			return false, false
		}
		select {
		case <-errs:
			return false, false
		default:
		}
		return true, true
	}
}

func completePlannedViewer(wall context.Context, dir string, shared *viewerShared, errs <-chan error, stopAll func(string)) (ViewerResult, error) {
	if wall.Err() != nil {
		stopAll("wall_limit")
		return viewerFailure(dir, shared, "viewer_transport_failed")
	}
	select {
	case <-errs:
		stopAll("transport_failed")
		return viewerFailure(dir, shared, "viewer_transport_failed")
	default:
	}
	stopAll("stopped_by_operator")
	if wall.Err() != nil {
		return viewerFailure(dir, shared, "viewer_transport_failed")
	}
	select {
	case <-errs:
		return viewerFailure(dir, shared, "viewer_transport_failed")
	default:
	}
	_ = os.Remove(filepath.Join(dir, "ready.json"))
	shared.mu.Lock()
	r := shared.result
	for _, summary := range shared.summaries {
		r.Viewers = append(r.Viewers, *summary)
	}
	r.PlannedStop = true
	r.PlannedStopReason = "stopped_by_operator"
	shared.mu.Unlock()
	r.Status = "COMPLETE"
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeViewerResult(dir, r); err != nil {
		return r, errors.New("result_artifact_write_failed")
	}
	return r, nil
}
func viewerFailure(dir string, s *viewerShared, reason string) (ViewerResult, error) {
	_ = os.Remove(filepath.Join(dir, "ready.json"))
	s.mu.Lock()
	r := s.result
	r.Status = "FAILED"
	r.PrimaryError = reason
	for _, v := range s.summaries {
		r.Viewers = append(r.Viewers, *v)
	}
	s.mu.Unlock()
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	_ = writeViewerResult(dir, r)
	return r, errors.New(reason)
}
func viewerMatches(filters []telemetryws.TargetKey, g, s string) bool {
	for _, f := range filters {
		if f.GatewayID == g && (f.SensorID == "" || f.SensorID == s) {
			return true
		}
	}
	return false
}
func validViewerACK(b []byte, id string, targets []telemetryws.TargetKey) bool {
	var a viewerACK
	if json.Unmarshal(b, &a) != nil || a.ProtocolVersion != 1 || a.MessageType != "ack" || a.RequestID != id || a.Action != "subscribe" || len(a.Targets) != len(targets) {
		return false
	}
	for i, t := range targets {
		if a.Targets[i] != t {
			return false
		}
	}
	return true
}
func readBoundedRegular(path string, max int64) ([]byte, error) {
	st, e := os.Lstat(path)
	if e != nil || !st.Mode().IsRegular() || st.Size() < 1 || st.Size() > max {
		return nil, errors.New("private_file_rejected")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(io.LimitReader(f, max+1))
}
func identityHost(endpoint string) string { u, _ := url.Parse(endpoint); return u.Hostname() }
func finishViewer(dir string, r ViewerResult, e error) (ViewerResult, error) {
	if e != nil {
		_ = os.Remove(filepath.Join(dir, "ready.json"))
	}
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	_ = writeViewerResult(dir, r)
	return r, e
}
func writeViewerResult(dir string, r ViewerResult) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(dir, "workload.json"), append(b, '\n'), 0600)
}

// viewerLaunchOffset returns the scheduled launch offset for viewer index i out of total viewers,
// spread over the half-open interval [0, ramp).
// When ramp <= 0, total <= 1, or i <= 0, it returns 0.
func viewerLaunchOffset(i, total int, ramp time.Duration) time.Duration {
	if ramp <= 0 || total <= 1 || i <= 0 {
		return 0
	}
	if i >= total {
		i = total - 1
	}
	return time.Duration(i) * ramp / time.Duration(total)
}

// publishViewerReady stages and syncs the marker, checks for an early operator
// stop at the publication boundary, then atomically renames it into readiness.
// afterPublish is an unexported deterministic test seam for the boundary.
func publishViewerReady(dir string, ready viewerReady, stop <-chan struct{}, afterPublish func()) (stoppedBeforeReady bool, err error) {
	b, err := json.Marshal(ready)
	if err != nil {
		return false, err
	}
	tmp := filepath.Join(dir, "ready.json.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, err
	}
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	select {
	case <-stop:
		_ = os.Remove(tmp)
		return true, nil
	default:
	}
	if err := os.Rename(tmp, filepath.Join(dir, "ready.json")); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	if afterPublish != nil {
		afterPublish()
	}
	return false, nil
}
