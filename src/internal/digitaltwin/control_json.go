package digitaltwin

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"time"
	"unicode/utf8"
)

// controlObject validates one explicitly allowlisted object. Nested objects
// are decoded through this same function, never into maps losing duplicates.
func controlObject(data []byte, fields ...string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(data) {
		return nil, ErrInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(data))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return nil, ErrInvalidInput
	}
	m := make(map[string]json.RawMessage, len(fields))
	for d.More() {
		t, err = d.Token()
		if err != nil {
			return nil, ErrInvalidInput
		}
		key, ok := t.(string)
		if !ok {
			return nil, ErrInvalidInput
		}
		allowed := false
		for _, f := range fields {
			if key == f {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, ErrInvalidInput
		}
		if _, seen := m[key]; seen {
			return nil, ErrInvalidInput
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, ErrInvalidInput
		}
		m[key] = raw
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') {
		return nil, ErrInvalidInput
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrInvalidInput
	}
	return m, nil
}

func controlString(raw []byte) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' {
		return "", ErrInvalidInput
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", ErrInvalidInput
	}
	// Reject invalid Unicode escapes rather than accepting encoding/json's
	// replacement rune. All wire identity/version/time fields are ASCII anyway.
	for _, r := range s {
		if r == '\uFFFD' || r == 0 {
			return "", ErrInvalidInput
		}
	}
	return s, nil
}

func decimalDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func controlInterval(raw []byte) (SamplingParameters, error) {
	m, err := controlObject(raw, "sample_interval_us")
	if err != nil || len(m) != 1 {
		return SamplingParameters{}, ErrInvalidInput
	}
	token := string(bytes.TrimSpace(m["sample_interval_us"]))
	if len(token) > 16 || !decimalDigits(token) {
		return SamplingParameters{}, ErrInvalidInput
	}
	n, err := strconv.ParseInt(token, 10, 64)
	if err != nil || n < MinSampleIntervalUS || n > MaxSampleIntervalUS {
		return SamplingParameters{}, ErrInvalidInput
	}
	return SamplingParameters{SampleIntervalUS: n}, nil
}

// Match the repository's microsecond PostgreSQL time precision and strict
// calendar parser. UTC Z only; time.Parse alone accepts unwanted syntax.
var controlTimePattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$`)

func controlTime(raw []byte) (time.Time, error) {
	s, err := controlString(raw)
	if err != nil || !controlTimePattern.MatchString(s) {
		return time.Time{}, ErrInvalidInput
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() < 1 || t.Year() > 9999 {
		return time.Time{}, ErrInvalidInput
	}
	return t.UTC(), nil
}
