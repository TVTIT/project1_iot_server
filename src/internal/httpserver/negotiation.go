package httpserver

import (
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

const (
	mimeJSONLD = "application/ld+json"
	mimeJSON   = "application/json"
)

type mediaRange struct {
	mediaType   string // e.g. "application/ld+json", "*/*", "application/*"
	mainType    string // e.g. "application", "*"
	subType     string // e.g. "ld+json", "*"
	q           float64
	specificity int // 0 for */*, 1 for type/*, 2 for type/subtype
}

// negotiateAccept validates the Accept header against the servedBaseMediaType
// (e.g. "application/ld+json" or "application/json").
//
// Rules per ETSI GS CIM 009 Section 3.5 & RFC 9110:
// - Always adds Vary: Accept to the response header.
// - If Accept is absent or whitespace-only, negotiation succeeds (serves default representation).
// - Parses comma-separated media ranges and parameters (including q-values and extra parameters like profile/charset).
// - Exact match (specificity 2) takes precedence over type wildcard (specificity 1), which takes precedence over full wildcard */* (specificity 0).
// - If the best matching range has q <= 0.0 (e.g. q=0 exclusion), negotiation fails -> 406 Not Acceptable.
// - If no media range matches the served representation, negotiation fails -> 406 Not Acceptable.
// - On 406, writes httpapi.WriteError with status 406, code "not_acceptable", and returns false.
func negotiateAccept(c *gin.Context, servedBaseMediaType string) bool {
	addVary(c.Writer.Header(), "Accept")

	acceptValues := c.Request.Header.Values("Accept")
	if len(acceptValues) == 0 {
		return true
	}

	var allRanges []mediaRange
	hasNonEmptyHeader := false

	for _, rawLine := range acceptValues {
		for _, part := range splitAcceptRanges(rawLine) {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" {
				continue
			}
			hasNonEmptyHeader = true
			mr, ok := parseMediaRange(trimmed)
			if ok {
				allRanges = append(allRanges, mr)
			}
		}
	}

	if !hasNonEmptyHeader {
		return true
	}

	// If client provided Accept header values, but all were unparseable, return 406
	if len(allRanges) == 0 {
		httpapi.WriteError(c, http.StatusNotAcceptable, "not_acceptable", "acceptable media type not available")
		return false
	}

	servedParts := strings.Split(servedBaseMediaType, "/")
	if len(servedParts) != 2 {
		return true
	}
	servedMain := strings.ToLower(servedParts[0])
	servedSub := strings.ToLower(servedParts[1])

	bestSpecificity := -1
	bestQ := 0.0

	for _, mr := range allRanges {
		matches := false
		spec := -1

		if mr.mainType == "*" && mr.subType == "*" {
			matches = true
			spec = 0
		} else if mr.mainType == servedMain && mr.subType == "*" {
			matches = true
			spec = 1
		} else if mr.mainType == servedMain && mr.subType == servedSub {
			matches = true
			spec = 2
		}

		if matches {
			if spec > bestSpecificity {
				bestSpecificity = spec
				bestQ = mr.q
			} else if spec == bestSpecificity {
				if mr.q > bestQ {
					bestQ = mr.q
				}
			}
		}
	}

	if bestSpecificity == -1 || bestQ <= 0.0 {
		httpapi.WriteError(c, http.StatusNotAcceptable, "not_acceptable", "acceptable media type not available")
		return false
	}

	return true
}

// splitAcceptRanges tokenizes an Accept header value into individual media range strings,
// taking into account quoted-string syntax and escaped characters (e.g. commas within quotes).
func splitAcceptRanges(header string) []string {
	var ranges []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for i := 0; i < len(header); i++ {
		b := header[i]
		if escaped {
			current.WriteByte(b)
			escaped = false
			continue
		}
		if b == '\\' && inQuote {
			current.WriteByte(b)
			escaped = true
			continue
		}
		if b == '"' {
			inQuote = !inQuote
			current.WriteByte(b)
			continue
		}
		if b == ',' && !inQuote {
			part := strings.TrimSpace(current.String())
			if part != "" {
				ranges = append(ranges, part)
			}
			current.Reset()
			continue
		}
		current.WriteByte(b)
	}

	part := strings.TrimSpace(current.String())
	if part != "" {
		ranges = append(ranges, part)
	}
	return ranges
}

func parseMediaRange(raw string) (mediaRange, bool) {
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return mediaRange{}, false
	}
	slashIdx := strings.IndexByte(mediaType, '/')
	if slashIdx == -1 {
		return mediaRange{}, false
	}
	mainType := strings.TrimSpace(mediaType[:slashIdx])
	subType := strings.TrimSpace(mediaType[slashIdx+1:])
	if mainType == "" || subType == "" {
		return mediaRange{}, false
	}

	rawParts := splitMediaRangeSemicolons(raw)
	hasLiteralQ := false
	hasExtendedQ := false
	parsedLiteralQ := 1.0

	if len(rawParts) > 1 {
		for _, part := range rawParts[1:] {
			name, val := extractParamName(part)
			nameLower := strings.ToLower(name)
			if nameLower == "q*" || strings.HasPrefix(nameLower, "q*") {
				hasExtendedQ = true
			} else if nameLower == "q" {
				v := strings.TrimSpace(val)
				if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
					v = strings.TrimSpace(v[1 : len(v)-1])
				}
				pq, err := strconv.ParseFloat(v, 64)
				if err != nil || math.IsNaN(pq) || math.IsInf(pq, 0) || pq < 0 || pq > 1 {
					pq = 0.0
				}
				if !hasLiteralQ || pq < parsedLiteralQ {
					parsedLiteralQ = pq
				}
				hasLiteralQ = true
			}
		}
	}

	q := 1.0
	if hasExtendedQ {
		// RFC 2231 extended or continuation parameters are invalid for HTTP Accept q-values.
		// Fail closed to prevent parameter normalization overriding literal q or granting quality.
		q = 0.0
	} else if hasLiteralQ {
		q = parsedLiteralQ
	}

	spec := 2
	if mainType == "*" && subType == "*" {
		spec = 0
	} else if subType == "*" {
		spec = 1
	}

	return mediaRange{
		mediaType:   mediaType,
		mainType:    mainType,
		subType:     subType,
		q:           q,
		specificity: spec,
	}, true
}

// splitMediaRangeSemicolons splits a single media range into its media-type and parameter segments,
// respecting quoted-string syntax and escaped characters (e.g. semicolons within quoted profile).
func splitMediaRangeSemicolons(raw string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if escaped {
			current.WriteByte(b)
			escaped = false
			continue
		}
		if b == '\\' && inQuote {
			current.WriteByte(b)
			escaped = true
			continue
		}
		if b == '"' {
			inQuote = !inQuote
			current.WriteByte(b)
			continue
		}
		if b == ';' && !inQuote {
			part := strings.TrimSpace(current.String())
			if part != "" {
				parts = append(parts, part)
			}
			current.Reset()
			continue
		}
		current.WriteByte(b)
	}

	part := strings.TrimSpace(current.String())
	if part != "" {
		parts = append(parts, part)
	}
	return parts
}

// extractParamName splits a single raw parameter string into its parameter name and value,
// taking into account that '=' characters might appear inside a quoted parameter value.
func extractParamName(param string) (string, string) {
	inQuote := false
	escaped := false

	for i := 0; i < len(param); i++ {
		b := param[i]
		if escaped {
			escaped = false
			continue
		}
		if b == '\\' && inQuote {
			escaped = true
			continue
		}
		if b == '"' {
			inQuote = !inQuote
			continue
		}
		if b == '=' && !inQuote {
			return strings.TrimSpace(param[:i]), strings.TrimSpace(param[i+1:])
		}
	}
	return strings.TrimSpace(param), ""
}
