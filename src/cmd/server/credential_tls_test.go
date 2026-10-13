package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"iot-platform/internal/mqttcredential"
)

func generateTestCA(t *testing.T) ([]byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey failed: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("x509.CreateCertificate failed: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate failed: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return pemBytes, cert, priv
}

func TestBuildVerifiedTLSConfig(t *testing.T) {
	validCA, _, _ := generateTestCA(t)

	tests := []struct {
		name        string
		caPEM       []byte
		mgmtURL     string
		wantErr     bool
		checkConfig func(t *testing.T, cfg *tls.Config, target string)
	}{
		{
			name:    "empty CA PEM",
			caPEM:   nil,
			mgmtURL: "ssl://127.0.0.1:8883",
			wantErr: true,
		},
		{
			name:    "invalid CA PEM bytes",
			caPEM:   []byte("not a valid pem"),
			mgmtURL: "ssl://127.0.0.1:8883",
			wantErr: true,
		},
		{
			name:    "empty management URL",
			caPEM:   validCA,
			mgmtURL: "",
			wantErr: true,
		},
		{
			name:    "invalid management URL scheme",
			caPEM:   validCA,
			mgmtURL: "tcp://127.0.0.1:8883",
			wantErr: true,
		},
		{
			name:    "URL missing port",
			caPEM:   validCA,
			mgmtURL: "ssl://127.0.0.1",
			wantErr: true,
		},
		{
			name:    "URL missing hostname",
			caPEM:   validCA,
			mgmtURL: "ssl://:8883",
			wantErr: true,
		},
		{
			name:    "valid URL with ssl scheme and explicit port",
			caPEM:   validCA,
			mgmtURL: "ssl://localhost:8883",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *tls.Config, target string) {
				if cfg.ServerName != "localhost" {
					t.Errorf("ServerName = %q, want localhost", cfg.ServerName)
				}
				if cfg.MinVersion != tls.VersionTLS12 {
					t.Errorf("MinVersion = %d, want TLS 1.2 (%d)", cfg.MinVersion, tls.VersionTLS12)
				}
				if cfg.InsecureSkipVerify {
					t.Errorf("InsecureSkipVerify must be false")
				}
				if cfg.RootCAs == nil {
					t.Errorf("RootCAs must not be nil")
				}
				if target != "localhost:8883" {
					t.Errorf("target = %q, want localhost:8883", target)
				}
			},
		},
		{
			name:    "valid URL with tls scheme and explicit port",
			caPEM:   validCA,
			mgmtURL: "tls://mqtt.local:8883",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *tls.Config, target string) {
				if cfg.ServerName != "mqtt.local" {
					t.Errorf("ServerName = %q, want mqtt.local", cfg.ServerName)
				}
				if target != "mqtt.local:8883" {
					t.Errorf("target = %q, want mqtt.local:8883", target)
				}
			},
		},
		{
			name:    "valid URL with mqtts scheme and explicit port",
			caPEM:   validCA,
			mgmtURL: "mqtts://broker.internal:8884",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *tls.Config, target string) {
				if cfg.ServerName != "broker.internal" {
					t.Errorf("ServerName = %q, want broker.internal", cfg.ServerName)
				}
				if target != "broker.internal:8884" {
					t.Errorf("target = %q, want broker.internal:8884", target)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, target, err := buildVerifiedTLSConfig(tt.caPEM, tt.mgmtURL)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.checkConfig != nil {
				tt.checkConfig(t, cfg, target)
			}
		})
	}
}

func TestProbePrivateCredentialWithDialer(t *testing.T) {
	t.Run("invalid arguments", func(t *testing.T) {
		ctx := context.Background()
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}

		// non-positive timeout
		err := probePrivateCredentialWithDialer(ctx, nil, cfg, "localhost:8883", 0, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed for zero timeout, got %v", err)
		}

		err = probePrivateCredentialWithDialer(ctx, nil, cfg, "localhost:8883", -time.Second, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed for negative timeout, got %v", err)
		}

		// nil tlsConfig
		err = probePrivateCredentialWithDialer(ctx, nil, nil, "localhost:8883", time.Second, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed for nil tlsConfig, got %v", err)
		}

		// empty target
		err = probePrivateCredentialWithDialer(ctx, nil, cfg, "", time.Second, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed for empty target, got %v", err)
		}

		// InsecureSkipVerify true must be rejected
		insecureCfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
		err = probePrivateCredentialWithDialer(ctx, nil, insecureCfg, "localhost:8883", time.Second, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed for InsecureSkipVerify, got %v", err)
		}
	})

	t.Run("positive probe: accepted CONNACK", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			cp, ok := pkt.(*packets.ConnectPacket)
			if !ok {
				return
			}
			if cp.Username != "testuser" || string(cp.Password) != "secret" {
				return
			}
			connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			connack.ReturnCode = 0
			_ = connack.Write(serverConn)
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
		<-serverDone
	})

	t.Run("positive probe: rejected CONNACK return code", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				connack.ReturnCode = packets.ErrRefusedBadUsernameOrPassword
				_ = connack.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed, got %v", err)
		}
	})

	t.Run("negative probe: ErrRefusedBadUsernameOrPassword returns nil", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				connack.ReturnCode = packets.ErrRefusedBadUsernameOrPassword
				_ = connack.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", true)
		if err != nil {
			t.Errorf("expected nil for negative probe with ErrRefusedBadUsernameOrPassword, got %v", err)
		}
	})

	t.Run("negative probe: ErrRefusedNotAuthorised returns nil", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				connack.ReturnCode = packets.ErrRefusedNotAuthorised
				_ = connack.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", true)
		if err != nil {
			t.Errorf("expected nil for negative probe with ErrRefusedNotAuthorised, got %v", err)
		}
	})

	t.Run("negative probe: accepted return code 0 fails negative check", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				connack.ReturnCode = 0
				_ = connack.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", true)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed when negative probe gets returnCode 0, got %v", err)
		}
	})

	t.Run("EOF / disconnect is failure", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			_ = serverConn.Close()
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed on EOF, got %v", err)
		}
	})

	t.Run("unexpected packet type is failure", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				ping := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
				_ = ping.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed on Pingresp instead of Connack, got %v", err)
		}
	})

	t.Run("cancelled context fails safely", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(ctx, dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed on context cancel, got %v", err)
		}
	})

	t.Run("dial failure fails safely", func(t *testing.T) {
		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, errors.New("dial failed")
		}

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed on dial error, got %v", err)
		}
	})

	t.Run("connection closed on all exits", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer serverConn.Close()

		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}

		go func() {
			pkt, err := packets.ReadPacket(serverConn)
			if err != nil {
				return
			}
			if _, ok := pkt.(*packets.ConnectPacket); ok {
				connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
				connack.ReturnCode = 0
				_ = connack.Write(serverConn)
			}
		}()

		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		err := probePrivateCredentialWithDialer(context.Background(), dial, cfg, "localhost:8883", 2*time.Second, "testuser", "secret", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Read from clientConn should now return error (closed)
		buf := make([]byte, 1)
		_, readErr := clientConn.Read(buf)
		if readErr == nil {
			t.Errorf("expected read on closed connection to fail, got nil")
		}
	})

	t.Run("probePrivateCredential default dialer path invokes dialer", func(t *testing.T) {
		// Test probePrivateCredential helper directly without dialer seam (hits dial failure on closed port)
		cfg := &tls.Config{
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		}
		// Bounded short timeout to unreachable port
		err := probePrivateCredential(context.Background(), cfg, "127.0.0.1:54321", 50*time.Millisecond, "user", "pass", false)
		if !errors.Is(err, mqttcredential.ErrVerificationFailed) {
			t.Errorf("expected ErrVerificationFailed, got %v", err)
		}
	})
}
