package telemetryws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

const nativeSlowViewerConfigEnv = "P7_SLOW_VIEWER_CONFIG_FILE"

type slowViewerConfig struct {
	URL           string    `json:"loopback_wss_url"`
	CAFile        string    `json:"fixture_ca_path"`
	JWT           string    `json:"valid_jwt"`
	Target        TargetKey `json:"target"`
	ReadyPath     string    `json:"ready_path"`
	ResumePath    string    `json:"resume_path"`
	StopPath      string    `json:"stop_path"`
	OutputPath    string    `json:"output_path"`
	MaxLifetimeMS int       `json:"max_lifetime_ms"`
	MaxFrames     int       `json:"max_frames"`
	MaxBytes      int64     `json:"max_bytes"`
}

type slowViewerResult struct {
	Outcome       string `json:"outcome"`
	Frames        int    `json:"frames_read"`
	Bytes         int64  `json:"bytes_read"`
	CloseReceived bool   `json:"server_close_received"`
	CloseStatus   int    `json:"close_status,omitempty"`
	ErrorClass    string `json:"error_class,omitempty"`
}

// TestNativeSlowViewerProbe is orchestrated externally: it records readiness after
// a real subscribe ACK, then reads no data until the resume file appears.
func TestNativeSlowViewerProbe(t *testing.T) {
	configPath := os.Getenv(nativeSlowViewerConfigEnv)
	if configPath == "" {
		t.Skip("native slow-viewer config not supplied")
	}
	cfg, err := readSlowViewerConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	result := runNativeSlowViewer(cfg)
	if err := writePrivateJSON(cfg.OutputPath, result); err != nil {
		t.Fatalf("write result: %v", err)
	}
	if result.Outcome != "server_closed" && result.Outcome != "transport_closed" {
		t.Errorf("probe outcome=%s (not a valid post-ACK close observation)", result.Outcome)
	}
}

func readSlowViewerConfig(path string) (slowViewerConfig, error) {
	var cfg slowViewerConfig
	info, err := os.Stat(path)
	if err != nil {
		return cfg, fmt.Errorf("config unavailable: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 || !info.Mode().IsRegular() {
		return cfg, errors.New("config must be a private regular file (0600 recommended)")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, errors.New("config unreadable")
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, errors.New("config is invalid JSON")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "wss" || u.Hostname() == "" {
		return cfg, errors.New("loopback_wss_url must be a valid wss URL")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return cfg, errors.New("WSS endpoint must use a loopback IP literal")
	}
	if cfg.CAFile == "" || cfg.JWT == "" || strings.ContainsAny(cfg.JWT, "\r\n") || cfg.Target.GatewayID == "" || cfg.Target.SensorID == "" {
		return cfg, errors.New("CA, JWT and concrete target are required")
	}
	if cfg.MaxLifetimeMS < 1 || cfg.MaxLifetimeMS > 180000 || cfg.MaxFrames < 1 || cfg.MaxFrames > 1024 || cfg.MaxBytes < 1 || cfg.MaxBytes > 64*1024*1024 {
		return cfg, errors.New("lifetime must be <=180000ms; frames 1..1024 and bytes 1..64MiB")
	}
	paths := []string{cfg.ReadyPath, cfg.ResumePath, cfg.StopPath, cfg.OutputPath}
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if p == "" || !filepath.IsAbs(p) {
			return cfg, errors.New("control and artifact paths must be absolute")
		}
		if _, ok := seen[p]; ok {
			return cfg, errors.New("control and artifact paths must be distinct")
		}
		seen[p] = struct{}{}
		dir, err := os.Stat(filepath.Dir(p))
		if err != nil || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
			return cfg, errors.New("control and artifact parent directories must be private")
		}
		if existing, err := os.Stat(p); err == nil && (!existing.Mode().IsRegular() || existing.Mode().Perm()&0077 != 0) {
			return cfg, errors.New("existing control and artifact files must be private regular files")
		}
	}
	return cfg, nil
}

func runNativeSlowViewer(cfg slowViewerConfig) slowViewerResult {
	out := slowViewerResult{Outcome: "setup_failed"}
	ca, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		out.ErrorClass = "ca_unavailable"
		return out
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		out.ErrorClass = "invalid_ca"
		return out
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.MaxLifetimeMS)*time.Millisecond)
	defer cancel()
	dialer := &net.Dialer{Timeout: 5 * time.Second, Control: func(_, _ string, raw syscall.RawConn) error {
		var controlErr error
		if err := raw.Control(func(fd uintptr) {
			controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 1024)
		}); err != nil {
			return err
		}
		return controlErr
	}}
	transport := &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DialContext: dialer.DialContext}
	defer transport.CloseIdleConnections()
	conn, _, err := websocket.Dial(ctx, cfg.URL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, HTTPHeader: http.Header{"Authorization": {"Bearer " + cfg.JWT}}, Subprotocols: []string{auth.WSTelemetryProtocol}})
	if err != nil {
		out.ErrorClass = "tls_or_handshake_failure"
		return out
	}
	defer func() { _ = conn.CloseNow() }()
	if conn.Subprotocol() != auth.WSTelemetryProtocol {
		out.ErrorClass = "wrong_subprotocol"
		return out
	}
	conn.SetReadLimit(66560)
	reqID := uuid.NewString()
	request, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: reqID, Action: ActionSubscribe, Targets: []TargetKey{cfg.Target}})
	if err := conn.Write(ctx, websocket.MessageText, request); err != nil {
		out.ErrorClass = "subscribe_write_failure"
		return out
	}
	_, ack, err := conn.Read(ctx)
	if err != nil {
		out.ErrorClass = "subscribe_ack_missing"
		return out
	}
	var decoded AckMessage
	if json.Unmarshal(ack, &decoded) != nil || decoded.MessageType != MessageTypeAck || decoded.Action != ActionSubscribe || decoded.RequestID != reqID || len(decoded.Targets) != 1 || decoded.Targets[0] != cfg.Target {
		out.ErrorClass = "subscribe_ack_mismatch"
		return out
	}
	if err := writePrivateJSON(cfg.ReadyPath, map[string]any{"ready": true, "subscribed": true, "target": cfg.Target}); err != nil {
		out.ErrorClass = "ready_write_failure"
		return out
	}
	for {
		if _, err := os.Stat(cfg.StopPath); err == nil {
			out.Outcome = "stopped"
			return out
		}
		if _, err := os.Stat(cfg.ResumePath); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			out.Outcome = "deadline"
			return out
		case <-time.After(50 * time.Millisecond):
		}
	}
	for out.Frames < cfg.MaxFrames && out.Bytes < cfg.MaxBytes {
		_, payload, readErr := conn.Read(ctx)
		if readErr != nil {
			out.ErrorClass = safeErrorClass(readErr)
			if status := websocket.CloseStatus(readErr); status != -1 {
				out.CloseReceived = true
				out.CloseStatus = int(status)
				out.Outcome = "server_closed"
			} else if errors.Is(readErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				out.Outcome = "read_deadline"
			} else if errors.Is(readErr, io.EOF) || errors.Is(readErr, net.ErrClosed) {
				out.Outcome = "transport_closed"
			} else {
				out.Outcome = "read_error"
			}
			return out
		}
		out.Frames++
		out.Bytes += int64(len(payload))
	}
	out.Outcome = "read_cap_reached"
	return out
}

func TestSlowViewerProbeDistinguishesCloseNowTransportEOF(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{auth.WSTelemetryProtocol}})
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		_, msg, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var request ClientMessage
		if json.Unmarshal(msg, &request) != nil {
			return
		}
		ack, _ := json.Marshal(NewAckMessage(request.RequestID, request.Action, request.Targets))
		if c.Write(r.Context(), websocket.MessageText, ack) == nil {
			time.Sleep(25 * time.Millisecond)
		}
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	u := "wss" + strings.TrimPrefix(server.URL, "https")
	conn, _, err := websocket.Dial(context.Background(), u, &websocket.DialOptions{HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}, Subprotocols: []string{auth.WSTelemetryProtocol}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	target := TargetKey{GatewayID: "gw", SensorID: "sensor"}
	req, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: uuid.NewString(), Action: ActionSubscribe, Targets: []TargetKey{target}})
	if err := conn.Write(context.Background(), websocket.MessageText, req); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.Read(context.Background()); err != nil {
		t.Fatalf("subscribe ACK: %v", err)
	}
	_, _, err = conn.Read(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected post-ACK transport EOF, got %v", err)
	}
}

func writePrivateJSON(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func TestSlowViewerProbeTLSSubscribeAndBoundedClose(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{auth.WSTelemetryProtocol}})
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		_, msg, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var request ClientMessage
		if json.Unmarshal(msg, &request) != nil {
			return
		}
		ack, _ := json.Marshal(NewAckMessage(request.RequestID, request.Action, request.Targets))
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		_ = c.Close(websocket.StatusPolicyViolation, "bounded test close")
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	u := "wss" + strings.TrimPrefix(server.URL, "https")
	conn, _, err := websocket.Dial(context.Background(), u, &websocket.DialOptions{HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}, HTTPHeader: http.Header{"Authorization": {"Bearer test-token"}}, Subprotocols: []string{auth.WSTelemetryProtocol}})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	id := uuid.NewString()
	target := TargetKey{GatewayID: "gw", SensorID: "sensor"}
	req, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: id, Action: ActionSubscribe, Targets: []TargetKey{target}})
	if err := conn.Write(context.Background(), websocket.MessageText, req); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var ack AckMessage
	if json.Unmarshal(data, &ack) != nil || ack.RequestID != id || ack.Targets[0] != target {
		t.Fatalf("bad ACK: %s", data)
	}
	_, _, err = conn.Read(context.Background())
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("expected close frame, got %v", err)
	}
}

func TestSlowViewerProbeRejectsInvalidBounds(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	config := slowViewerConfig{URL: "wss://127.0.0.1/ws", CAFile: "ca.pem", JWT: "opaque", Target: TargetKey{GatewayID: "gw", SensorID: "s"}, ReadyPath: filepath.Join(dir, "ready"), ResumePath: filepath.Join(dir, "resume"), StopPath: filepath.Join(dir, "stop"), OutputPath: filepath.Join(dir, "result"), MaxLifetimeMS: 180001, MaxFrames: 1025, MaxBytes: 1}
	path := filepath.Join(dir, "config.json")
	if err := writePrivateJSON(path, config); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlowViewerConfig(path); err == nil {
		t.Fatal("accepted over-limit lifetime/frame count")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	config.MaxLifetimeMS, config.MaxFrames = 1000, 1
	if err := writePrivateJSON(path, config); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlowViewerConfig(path); err == nil {
		t.Fatal("accepted group/world-readable config")
	}
}

func TestSlowViewerProbeAcceptsConfiguredByteAndFrameBounds(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	config := slowViewerConfig{URL: "wss://127.0.0.1/ws", CAFile: "ca.pem", JWT: "opaque", Target: TargetKey{GatewayID: "gw", SensorID: "s"}, ReadyPath: filepath.Join(dir, "ready"), ResumePath: filepath.Join(dir, "resume"), StopPath: filepath.Join(dir, "stop"), OutputPath: filepath.Join(dir, "result"), MaxLifetimeMS: 180000, MaxFrames: 1024, MaxBytes: 64 * 1024 * 1024}
	path := filepath.Join(dir, "config.json")
	if err := writePrivateJSON(path, config); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlowViewerConfig(path); err != nil {
		t.Fatalf("accepted bounds rejected: %v", err)
	}
	config.MaxBytes++
	if err := writePrivateJSON(path, config); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlowViewerConfig(path); err == nil {
		t.Fatal("accepted byte cap above 64MiB")
	}
}
