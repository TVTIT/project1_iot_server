package mqttcredential

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestParseAndValidateNativeInput(t *testing.T) {
	validJSON := `{
		"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
		"manager_username": "admin",
		"manager_password": "super-secret-password-12345",
		"broker_url": "ssl://localhost:8883",
		"ca_path": "/security/ca.crt",
		"store_path": "/security/dynsec.json",
		"writer_uid": 1883,
		"run_marker": "marker-123456",
		"project_binding": "dynsec_probe_0123456789ab"
	}`

	t.Run("valid input", func(t *testing.T) {
		in, err := parseAndValidateNativeInput(strings.NewReader(validJSON))
		if err != nil {
			t.Fatalf("expected valid input, got err: %v", err)
		}
		if in.WriterUID != 1883 {
			t.Fatalf("expected WriterUID 1883, got %d", in.WriterUID)
		}
		if in.RunMarker != "marker-123456" {
			t.Fatalf("expected RunMarker 'marker-123456', got %q", in.RunMarker)
		}
		if in.ProjectBinding != "dynsec_probe_0123456789ab" {
			t.Fatalf("expected ProjectBinding 'dynsec_probe_0123456789ab', got %q", in.ProjectBinding)
		}
	})

	t.Run("duplicate json keys rejected", func(t *testing.T) {
		dupJSON := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_username": "admin2",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(dupJSON))
		if err == nil || !errors.Is(err, errInvalidInputJSON) {
			t.Fatalf("expected errInvalidInputJSON on duplicate key, got err: %v", err)
		}
	})

	t.Run("unknown fields rejected", func(t *testing.T) {
		badJSON := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab",
			"unknown_field": "disallowed"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badJSON))
		if err == nil || !errors.Is(err, errInvalidInputJSON) {
			t.Fatalf("expected errInvalidInputJSON on unknown field, got err: %v", err)
		}
	})

	t.Run("trailing data rejected", func(t *testing.T) {
		trailing := validJSON + `{"extra":"data"}`
		_, err := parseAndValidateNativeInput(strings.NewReader(trailing))
		if err == nil || !errors.Is(err, errInvalidInputJSON) {
			t.Fatalf("expected errInvalidInputJSON on trailing content, got err: %v", err)
		}
	})

	t.Run("disallowed database host rejected", func(t *testing.T) {
		badHost := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@localhost:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badHost))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on disallowed host, got err: %v", err)
		}
	})

	t.Run("disallowed database name rejected", func(t *testing.T) {
		badDB := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/wrong_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badDB))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on wrong db name, got err: %v", err)
		}
	})

	t.Run("disallowed database user rejected", func(t *testing.T) {
		badUser := `{
			"test_pg_dsn": "postgres://other_user:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badUser))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on non-fixture_admin user, got err: %v", err)
		}
	})

	t.Run("disallowed sslmode or extra query options rejected", func(t *testing.T) {
		badSSL := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=require",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badSSL))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on sslmode != disable, got err: %v", err)
		}
	})

	t.Run("disallowed broker url rejected", func(t *testing.T) {
		badBroker := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "tcp://localhost:1883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badBroker))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on non-ssl broker URL, got err: %v", err)
		}
	})

	t.Run("disallowed ca or store path rejected", func(t *testing.T) {
		badPath := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/custom/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badPath))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on non-pinned path, got err: %v", err)
		}
	})

	t.Run("missing or wrong writer uid rejected", func(t *testing.T) {
		wrongUID := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 0,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(wrongUID))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on wrong writer UID, got err: %v", err)
		}
	})

	t.Run("short run marker rejected", func(t *testing.T) {
		shortMarker := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "short",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(shortMarker))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on short run marker, got err: %v", err)
		}
	})

	t.Run("invalid project binding rejected", func(t *testing.T) {
		badBinding := `{
			"test_pg_dsn": "postgres://fixture_admin:pass@task28a-pg:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "arbitrary_probe"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(badBinding))
		if err == nil || !errors.Is(err, errInvalidInputContract) {
			t.Fatalf("expected errInvalidInputContract on invalid project binding, got err: %v", err)
		}
	})

	t.Run("oversized input rejected", func(t *testing.T) {
		oversized := validJSON + strings.Repeat(" ", 70<<10)
		_, err := parseAndValidateNativeInput(strings.NewReader(oversized))
		if err == nil || !errors.Is(err, errInputExceedsSizeLimit) {
			t.Fatalf("expected errInputExceedsSizeLimit on oversized input, got: %v", err)
		}
	})

	t.Run("generic error message does not leak raw input or dsn", func(t *testing.T) {
		secretPayload := `{
			"test_pg_dsn": "postgres://fixture_admin:secret_password_exposed@evil-host:5432/fixture_service?sslmode=disable",
			"manager_username": "admin",
			"manager_password": "super-secret-password-12345",
			"broker_url": "ssl://localhost:8883",
			"ca_path": "/security/ca.crt",
			"store_path": "/security/dynsec.json",
			"writer_uid": 1883,
			"run_marker": "marker-123456",
			"project_binding": "dynsec_probe_0123456789ab"
		}`
		_, err := parseAndValidateNativeInput(strings.NewReader(secretPayload))
		if err == nil {
			t.Fatal("expected error")
		}
		errMsg := err.Error()
		if strings.Contains(errMsg, "secret_password_exposed") || strings.Contains(errMsg, "evil-host") {
			t.Fatalf("error message leaked raw input contents: %s", errMsg)
		}
	})
}

type fakeTimeoutError struct{}

func (fakeTimeoutError) Error() string   { return "i/o timeout" }
func (fakeTimeoutError) Timeout() bool   { return true }
func (fakeTimeoutError) Temporary() bool { return true }

type fakeConn struct {
	net.Conn
	readErr  error
	closeErr error
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	return 0, f.readErr
}

func (f *fakeConn) Close() error {
	return f.closeErr
}

func TestIsRealEOFOrReset(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"syscall.ECONNRESET", syscall.ECONNRESET, true},
		{"opError wrapping ECONNRESET", &net.OpError{Err: syscall.ECONNRESET}, true},
		{"timeout error", fakeTimeoutError{}, false},
		{"net.OpError timeout", &net.OpError{Err: fakeTimeoutError{}}, false},
		{"generic error", errors.New("read: connection error"), false},
		{"syscall.EPIPE", syscall.EPIPE, false},
		{"syscall.ECONNREFUSED", syscall.ECONNREFUSED, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isRealEOFOrReset(tc.err)
			if got != tc.expected {
				t.Fatalf("isRealEOFOrReset(%v) = %v, want %v", tc.err, got, tc.expected)
			}
		})
	}
}

func TestEOFMonitoringConn(t *testing.T) {
	t.Run("EOF triggers onEOF", func(t *testing.T) {
		var notified bool
		conn := &eofMonitoringConn{
			Conn:  &fakeConn{readErr: io.EOF},
			onEOF: func() { notified = true },
		}
		buf := make([]byte, 16)
		_, err := conn.Read(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if !notified {
			t.Fatal("expected onEOF callback on io.EOF")
		}
	})

	t.Run("ECONNRESET triggers onEOF", func(t *testing.T) {
		var notified bool
		conn := &eofMonitoringConn{
			Conn:  &fakeConn{readErr: syscall.ECONNRESET},
			onEOF: func() { notified = true },
		}
		buf := make([]byte, 16)
		_, err := conn.Read(buf)
		if !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("expected ECONNRESET, got %v", err)
		}
		if !notified {
			t.Fatal("expected onEOF callback on ECONNRESET")
		}
	})

	t.Run("Timeout does NOT trigger onEOF", func(t *testing.T) {
		var notified bool
		conn := &eofMonitoringConn{
			Conn:  &fakeConn{readErr: fakeTimeoutError{}},
			onEOF: func() { notified = true },
		}
		buf := make([]byte, 16)
		_, _ = conn.Read(buf)
		if notified {
			t.Fatal("unexpected onEOF callback on timeout error")
		}
	})

	t.Run("Local Close does NOT trigger onEOF", func(t *testing.T) {
		var notified bool
		conn := &eofMonitoringConn{
			Conn:  &fakeConn{},
			onEOF: func() { notified = true },
		}
		_ = conn.Close()
		if notified {
			t.Fatal("unexpected onEOF callback on local Close()")
		}
	})

	t.Run("Generic read error does NOT trigger onEOF", func(t *testing.T) {
		var notified bool
		conn := &eofMonitoringConn{
			Conn:  &fakeConn{readErr: errors.New("read error: unexpected header")},
			onEOF: func() { notified = true },
		}
		buf := make([]byte, 16)
		_, _ = conn.Read(buf)
		if notified {
			t.Fatal("unexpected onEOF callback on generic error")
		}
	})
}

func TestCountingIntentAssertingExecutorCounter(t *testing.T) {
	var exec countingIntentAssertingExecutor
	if count := exec.ExecuteCount(); count != 0 {
		t.Fatalf("expected initial count 0, got %d", count)
	}
}

func TestExtractSecretFromResponse(t *testing.T) {
	m := Metadata{GatewayID: "gw1"}
	opID := (&[16]byte{1, 2, 3, 4})
	secret := NewSecretResult(m, *opID, "secret-test-password-123")

	val := extractSecretFromResponse(t, &secret)
	if val != "secret-test-password-123" {
		t.Fatalf("expected secret-test-password-123, got %q", val)
	}

	secret.ClearSecret()
	var b bytes.Buffer
	r := &secret
	buf, _ := r.MarshalJSON()
	b.Write(buf)
	if strings.Contains(b.String(), "secret-test-password-123") {
		t.Fatal("cleared secret exposed in marshaled JSON")
	}
}
