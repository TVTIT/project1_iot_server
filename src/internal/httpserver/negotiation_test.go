package httpserver

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestParseMediaRange_QuotedParameters(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantType  string
		wantMain  string
		wantSub   string
		wantQ     float64
		wantSpec  int
		wantValid bool
	}{
		{
			name:      "quoted comma in profile before q=0",
			raw:       `application/ld+json;profile="https://example.org/a,b";q=0`,
			wantType:  "application/ld+json",
			wantMain:  "application",
			wantSub:   "ld+json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "quoted semicolon in profile before q=0",
			raw:       `application/ld+json;profile="https://example.org/a;b";q=0`,
			wantType:  "application/ld+json",
			wantMain:  "application",
			wantSub:   "ld+json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "escaped quotes in profile before q=0",
			raw:       `application/ld+json;profile="https://example.org/\"escaped\"";q=0`,
			wantType:  "application/ld+json",
			wantMain:  "application",
			wantSub:   "ld+json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "quoted comma in profile with positive q",
			raw:       `application/json;profile="https://example.org/a,b";q=0.85`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.85,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "standard type without parameters",
			raw:       `application/ld+json`,
			wantType:  "application/ld+json",
			wantMain:  "application",
			wantSub:   "ld+json",
			wantQ:     1.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "type wildcard with q",
			raw:       `application/*;q=0.5`,
			wantType:  "application/*",
			wantMain:  "application",
			wantSub:   "*",
			wantQ:     0.5,
			wantSpec:  1,
			wantValid: true,
		},
		{
			name:      "full wildcard without q",
			raw:       `*/*`,
			wantType:  "*/*",
			wantMain:  "*",
			wantSub:   "*",
			wantQ:     1.0,
			wantSpec:  0,
			wantValid: true,
		},
		{
			name:      "extended q* overriding q=0 is ignored in favor of literal q=0",
			raw:       `application/json;q=0;q*=utf-8''1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "continuation q*0 overriding q=0 is ignored in favor of literal q=0",
			raw:       `application/json;q=0;q*0=1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "extended continuation q*0* overriding q=0 is ignored in favor of literal q=0",
			raw:       `application/json;q=0;q*0*=utf-8''1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "quoted profile with extended q* overriding q=0",
			raw:       `application/json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "quoted profile with continuation q*0 overriding q=0",
			raw:       `application/json;profile="https://example.org/a,b";q=0;q*0=1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "quoted profile with extended continuation q*0* overriding q=0",
			raw:       `application/json;profile="https://example.org/a,b";q=0;q*0*=utf-8''1`,
			wantType:  "application/json",
			wantMain:  "application",
			wantSub:   "json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
		{
			name:      "JSON-LD with quoted profile and extended q* overriding q=0",
			raw:       `application/ld+json;profile="https://example.org/a,b";q=0;q*=utf-8''1`,
			wantType:  "application/ld+json",
			wantMain:  "application",
			wantSub:   "ld+json",
			wantQ:     0.0,
			wantSpec:  2,
			wantValid: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, ok := parseMediaRange(tc.raw)
			if ok != tc.wantValid {
				t.Fatalf("parseMediaRange(%q) ok = %v, want %v", tc.raw, ok, tc.wantValid)
			}
			if !tc.wantValid {
				return
			}
			if mr.mediaType != tc.wantType {
				t.Errorf("mediaType = %q, want %q", mr.mediaType, tc.wantType)
			}
			if mr.mainType != tc.wantMain {
				t.Errorf("mainType = %q, want %q", mr.mainType, tc.wantMain)
			}
			if mr.subType != tc.wantSub {
				t.Errorf("subType = %q, want %q", mr.subType, tc.wantSub)
			}
			if math.Abs(mr.q-tc.wantQ) > 1e-6 {
				t.Errorf("q = %v, want %v", mr.q, tc.wantQ)
			}
			if mr.specificity != tc.wantSpec {
				t.Errorf("specificity = %d, want %d", mr.specificity, tc.wantSpec)
			}
		})
	}
}

func TestParseMediaRange_MalformedQuotingAndInvalidQ(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantValid bool
		wantMaxQ  float64 // if valid, q must not exceed this (must not be positive default 1.0)
	}{
		{
			name:      "unclosed quote in parameter",
			raw:       `application/ld+json;profile="unclosed;q=0`,
			wantValid: false,
		},
		{
			name:      "unclosed quote with no other params",
			raw:       `application/json;profile="unclosed`,
			wantValid: false,
		},
		{
			name:      "q is NaN",
			raw:       `application/json;q=NaN`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is +Inf",
			raw:       `application/json;q=+Inf`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is -Inf",
			raw:       `application/json;q=-Inf`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is non-numeric string",
			raw:       `application/json;q=invalid`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is greater than 1",
			raw:       `application/json;q=1.5`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is negative",
			raw:       `application/json;q=-0.1`,
			wantValid: true,
			wantMaxQ:  0.0,
		},
		{
			name:      "q is empty",
			raw:       `application/json;q=`,
			wantValid: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, ok := parseMediaRange(tc.raw)
			if ok != tc.wantValid {
				t.Fatalf("parseMediaRange(%q) ok = %v, want %v", tc.raw, ok, tc.wantValid)
			}
			if ok {
				if math.IsNaN(mr.q) || math.IsInf(mr.q, 0) || mr.q > tc.wantMaxQ {
					t.Errorf("parseMediaRange(%q) q = %v, must be <= %v and finite", tc.raw, mr.q, tc.wantMaxQ)
				}
			}
		})
	}
}

func TestNegotiateAccept_Direct(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		accepts    []string
		servedMime string
		wantOK     bool
		wantStatus int
	}{
		{
			name:       "quoted comma in profile before q=0 fails negotiation",
			accepts:    []string{`application/ld+json;profile="https://example.org/a,b";q=0`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "quoted semicolon in profile before q=0 fails negotiation",
			accepts:    []string{`application/ld+json;profile="https://example.org/a;b";q=0`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "escaped quotes in profile before q=0 fails negotiation",
			accepts:    []string{`application/ld+json;profile="https://example.org/\"escaped\"";q=0`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "quoted comma with positive q succeeds",
			accepts:    []string{`application/ld+json;profile="https://example.org/a,b";q=0.9`},
			servedMime: mimeJSONLD,
			wantOK:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "exact match q=0 overrides type wildcard positive q",
			accepts:    []string{`application/*;q=0.8, application/ld+json;profile="https://example.org/a,b";q=0`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "multiple Accept headers where compatible has quoted comma before q=0",
			accepts:    []string{"text/html", `application/ld+json;profile="https://example.org/a,b";q=0`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "multiple Accept headers where one has positive q succeeds",
			accepts:    []string{"text/html;q=0.5", `application/ld+json;profile="https://example.org/a,b";q=0.9`},
			servedMime: mimeJSONLD,
			wantOK:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "Accept with q=0 and extended q* fails negotiation for JSON",
			accepts:    []string{`application/json;q=0;q*=utf-8''1`},
			servedMime: mimeJSON,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with q=0 and continuation q*0 fails negotiation for JSON",
			accepts:    []string{`application/json;q=0;q*0=1`},
			servedMime: mimeJSON,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with q=0 and extended continuation q*0* fails negotiation for JSON",
			accepts:    []string{`application/json;q=0;q*0*=utf-8''1`},
			servedMime: mimeJSON,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with quoted profile, q=0 and extended q* fails negotiation for JSON",
			accepts:    []string{`application/json;profile="https://example.org/a,b";q=0;q*=utf-8''1`},
			servedMime: mimeJSON,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with quoted profile, q=0 and extended q* fails negotiation for JSON-LD",
			accepts:    []string{`application/ld+json;profile="https://example.org/a,b";q=0;q*=utf-8''1`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with q=0 and continuation q*0 fails negotiation for JSON-LD",
			accepts:    []string{`application/ld+json;q=0;q*0=1`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "Accept with q=0 and extended continuation q*0* fails negotiation for JSON-LD",
			accepts:    []string{`application/ld+json;q=0;q*0*=utf-8''1`},
			servedMime: mimeJSONLD,
			wantOK:     false,
			wantStatus: http.StatusNotAcceptable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			for _, a := range tc.accepts {
				req.Header.Add("Accept", a)
			}
			c.Request = req

			ok := negotiateAccept(c, tc.servedMime)
			if ok != tc.wantOK {
				t.Fatalf("negotiateAccept() ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK && w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
		})
	}
}
