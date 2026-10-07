package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadCORSAllowedOrigins(t *testing.T) {
	tests := []struct {
		name        string
		rawOrigins  string
		setVar      bool
		wantOrigins []string
		wantErr     bool
		errContains string
	}{
		{
			name:        "unset variable defaults to empty allowlist",
			setVar:      false,
			wantOrigins: nil,
			wantErr:     false,
		},
		{
			name:        "empty string defaults to empty allowlist",
			rawOrigins:  "",
			setVar:      true,
			wantOrigins: nil,
			wantErr:     false,
		},
		{
			name:        "whitespace string defaults to empty allowlist",
			rawOrigins:  "   ",
			setVar:      true,
			wantOrigins: nil,
			wantErr:     false,
		},
		{
			name:        "valid single origin",
			rawOrigins:  "http://localhost:12345",
			setVar:      true,
			wantOrigins: []string{"http://localhost:12345"},
			wantErr:     false,
		},
		{
			name:        "valid multiple origins with whitespace around commas",
			rawOrigins:  "  http://localhost:12345 , https://app.example.com  ",
			setVar:      true,
			wantOrigins: []string{"http://localhost:12345", "https://app.example.com"},
			wantErr:     false,
		},
		{
			name:        "valid IPv6 origin with port",
			rawOrigins:  "http://[::1]:12345",
			setVar:      true,
			wantOrigins: []string{"http://[::1]:12345"},
			wantErr:     false,
		},
		{
			name:        "valid IPv6 origin without port",
			rawOrigins:  "https://[2001:db8::1]",
			setVar:      true,
			wantOrigins: []string{"https://[2001:db8::1]"},
			wantErr:     false,
		},
		{
			name:        "valid IPv4 origin",
			rawOrigins:  "http://127.0.0.1:12345",
			setVar:      true,
			wantOrigins: []string{"http://127.0.0.1:12345"},
			wantErr:     false,
		},
		{
			name:        "valid ASCII punycode hostname",
			rawOrigins:  "https://xn--bcher-kva.example",
			setVar:      true,
			wantOrigins: []string{"https://xn--bcher-kva.example"},
			wantErr:     false,
		},
		{
			name:        "normalizes default HTTPS port",
			rawOrigins:  "https://app.example.com:443",
			setVar:      true,
			wantOrigins: []string{"https://app.example.com"},
			wantErr:     false,
		},
		{
			name:        "normalizes default HTTP port",
			rawOrigins:  "http://localhost:80",
			setVar:      true,
			wantOrigins: []string{"http://localhost"},
			wantErr:     false,
		},
		{
			name:        "normalizes numeric port with leading zeroes",
			rawOrigins:  "http://localhost:012345",
			setVar:      true,
			wantOrigins: []string{"http://localhost:12345"},
			wantErr:     false,
		},
		{
			name:        "normalizes expanded IPv6 address with port",
			rawOrigins:  "http://[0:0:0:0:0:0:0:1]:12345",
			setVar:      true,
			wantOrigins: []string{"http://[::1]:12345"},
			wantErr:     false,
		},
		{
			name:        "normalizes expanded IPv6 address without port",
			rawOrigins:  "https://[2001:0db8::1]",
			setVar:      true,
			wantOrigins: []string{"https://[2001:db8::1]"},
			wantErr:     false,
		},
		{
			name:        "deduplicates identical origins",
			rawOrigins:  "http://localhost:12345, http://localhost:12345",
			setVar:      true,
			wantOrigins: []string{"http://localhost:12345"},
			wantErr:     false,
		},
		{
			name:        "deduplicates after normalization",
			rawOrigins:  "https://app.example.com:443, https://app.example.com",
			setVar:      true,
			wantOrigins: []string{"https://app.example.com"},
			wantErr:     false,
		},
		// Invalid entries
		{
			name:        "rejects empty entry from leading comma",
			rawOrigins:  ",http://localhost:12345",
			setVar:      true,
			wantErr:     true,
			errContains: "empty origin",
		},
		{
			name:        "rejects empty entry from trailing comma",
			rawOrigins:  "http://localhost:12345,",
			setVar:      true,
			wantErr:     true,
			errContains: "empty origin",
		},
		{
			name:        "rejects empty entry between commas",
			rawOrigins:  "http://localhost:12345,,https://app.example.com",
			setVar:      true,
			wantErr:     true,
			errContains: "empty origin",
		},
		{
			name:        "rejects single comma",
			rawOrigins:  ",",
			setVar:      true,
			wantErr:     true,
			errContains: "empty origin",
		},
		{
			name:        "rejects null origin",
			rawOrigins:  "null",
			setVar:      true,
			wantErr:     true,
			errContains: "null origin",
		},
		{
			name:        "rejects wildcard asterisk alone",
			rawOrigins:  "*",
			setVar:      true,
			wantErr:     true,
			errContains: "wildcard",
		},
		{
			name:        "rejects wildcard in scheme/host",
			rawOrigins:  "http://*",
			setVar:      true,
			wantErr:     true,
			errContains: "wildcard",
		},
		{
			name:        "rejects wildcard subdomain",
			rawOrigins:  "https://*.example.com",
			setVar:      true,
			wantErr:     true,
			errContains: "wildcard",
		},
		{
			name:        "rejects wildcard port",
			rawOrigins:  "http://localhost:*",
			setVar:      true,
			wantErr:     true,
			errContains: "wildcard",
		},
		{
			name:        "rejects missing scheme",
			rawOrigins:  "localhost:12345",
			setVar:      true,
			wantErr:     true,
			errContains: "scheme must be http or https",
		},
		{
			name:        "rejects non-http scheme ftp",
			rawOrigins:  "ftp://localhost:12345",
			setVar:      true,
			wantErr:     true,
			errContains: "scheme must be http or https",
		},
		{
			name:        "rejects non-http scheme ws",
			rawOrigins:  "ws://localhost:12345",
			setVar:      true,
			wantErr:     true,
			errContains: "scheme must be http or https",
		},
		{
			name:        "rejects userinfo",
			rawOrigins:  "http://user:pass@localhost:12345",
			setVar:      true,
			wantErr:     true,
			errContains: "userinfo",
		},
		{
			name:        "rejects subpath",
			rawOrigins:  "http://localhost:12345/api",
			setVar:      true,
			wantErr:     true,
			errContains: "path",
		},
		{
			name:        "rejects trailing slash",
			rawOrigins:  "http://localhost:12345/",
			setVar:      true,
			wantErr:     true,
			errContains: "trailing slash",
		},
		{
			name:        "rejects query string",
			rawOrigins:  "http://localhost:12345?foo=bar",
			setVar:      true,
			wantErr:     true,
			errContains: "query",
		},
		{
			name:        "rejects empty query delimiter",
			rawOrigins:  "https://app.example.com?",
			setVar:      true,
			wantErr:     true,
			errContains: "query",
		},
		{
			name:        "rejects fragment",
			rawOrigins:  "http://localhost:12345#section",
			setVar:      true,
			wantErr:     true,
			errContains: "fragment",
		},
		{
			name:        "rejects empty fragment delimiter",
			rawOrigins:  "https://app.example.com#",
			setVar:      true,
			wantErr:     true,
			errContains: "fragment",
		},
		{
			name:        "rejects non-numeric port",
			rawOrigins:  "http://localhost:abc",
			setVar:      true,
			wantErr:     true,
			errContains: "port",
		},
		{
			name:        "rejects zero port",
			rawOrigins:  "http://localhost:0",
			setVar:      true,
			wantErr:     true,
			errContains: "port",
		},
		{
			name:        "rejects port out of range",
			rawOrigins:  "http://localhost:70000",
			setVar:      true,
			wantErr:     true,
			errContains: "port",
		},
		{
			name:        "rejects negative port",
			rawOrigins:  "http://localhost:-80",
			setVar:      true,
			wantErr:     true,
			errContains: "port",
		},
		{
			name:        "rejects explicit empty port",
			rawOrigins:  "http://localhost:",
			setVar:      true,
			wantErr:     true,
			errContains: "port",
		},
		{
			name:        "rejects missing hostname",
			rawOrigins:  "http://",
			setVar:      true,
			wantErr:     true,
			errContains: "hostname",
		},
		{
			name:        "rejects invalid hostname characters",
			rawOrigins:  "http://exa<mple.com",
			setVar:      true,
			wantErr:     true,
			errContains: "host",
		},
		{
			name:        "rejects Unicode hostname without IDNA conversion",
			rawOrigins:  "https://bücher.example",
			setVar:      true,
			wantErr:     true,
			errContains: "host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			if tt.setVar {
				values["CORS_ALLOWED_ORIGINS"] = tt.rawOrigins
			} else {
				delete(values, "CORS_ALLOWED_ORIGINS")
			}

			cfg, err := Load(mapLookup(values))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error, got nil (cfg.CORSAllowedOrigins = %v)", cfg.CORSAllowedOrigins)
				}
				if !strings.Contains(err.Error(), "CORS_ALLOWED_ORIGINS entry ") {
					t.Fatalf("error %q does not identify configuration entry", err.Error())
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("Load() unexpected error = %v", err)
			}
			if !reflect.DeepEqual(cfg.CORSAllowedOrigins, tt.wantOrigins) {
				t.Fatalf("cfg.CORSAllowedOrigins = %v, want %v", cfg.CORSAllowedOrigins, tt.wantOrigins)
			}
		})
	}
}

func TestLoadCORSAllowedOriginsDoesNotLeakOriginValuesInErrors(t *testing.T) {
	for _, originWithCredentials := range []string{
		"http://operator:super-secret@localhost:12345",
		"http://operator:super-secret@localhost:bad-port",
	} {
		t.Run(originWithCredentials, func(t *testing.T) {
			values := validConfigValues()
			values["CORS_ALLOWED_ORIGINS"] = originWithCredentials

			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatal("Load() expected error, got nil")
			}
			if !strings.Contains(err.Error(), "CORS_ALLOWED_ORIGINS entry 1") {
				t.Fatalf("error %q does not contain the safe configuration context", err.Error())
			}
			for _, secret := range []string{originWithCredentials, "operator", "super-secret", "localhost", "bad-port"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaks origin data %q: %q", secret, err.Error())
				}
			}
		})
	}
}
