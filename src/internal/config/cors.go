package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// parseCORSAllowedOrigins parses and validates a comma-separated list of exact web origins.
// Empty or missing string returns nil, nil (safely disabling cross-origin access).
func parseCORSAllowedOrigins(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	entries := strings.Split(trimmed, ",")
	seen := make(map[string]bool)
	var origins []string

	for index, entry := range entries {
		item := strings.TrimSpace(entry)
		if item == "" {
			return nil, corsOriginError(index, "empty origin entry")
		}
		if item == "null" {
			return nil, corsOriginError(index, "null origin is not allowed")
		}
		if strings.Contains(item, "*") {
			return nil, corsOriginError(index, "wildcard origins are not allowed")
		}
		if strings.Contains(item, "?") {
			return nil, corsOriginError(index, "query strings are not allowed")
		}
		if strings.Contains(item, "#") {
			return nil, corsOriginError(index, "fragments are not allowed")
		}

		u, err := url.Parse(item)
		if err != nil {
			return nil, corsOriginError(index, "invalid origin or port syntax")
		}

		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, corsOriginError(index, "scheme must be http or https")
		}
		if u.Host == "" || u.Hostname() == "" {
			return nil, corsOriginError(index, "hostname is required")
		}
		if u.User != nil {
			return nil, corsOriginError(index, "userinfo is not allowed")
		}
		// Exact web origins must not have a path or trailing slash.
		// Note: url.Parse("http://localhost:12345/") sets u.Path to "/".
		if u.Path != "" || strings.HasSuffix(item, "/") {
			return nil, corsOriginError(index, "path and trailing slashes are not allowed")
		}

		portStr, err := corsOriginPort(u.Host)
		if err != nil {
			return nil, corsOriginError(index, err.Error())
		}
		if portStr != "" {
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 || port > 65535 {
				return nil, corsOriginError(index, "port must be between 1 and 65535")
			}
			portStr = strconv.Itoa(port)
		}

		scheme := strings.ToLower(u.Scheme)
		host, err := canonicalCORSHost(u.Hostname())
		if err != nil {
			return nil, corsOriginError(index, err.Error())
		}
		if (scheme == "http" && portStr == "80") || (scheme == "https" && portStr == "443") {
			portStr = ""
		}

		// Match browser-serialized origins: lowercase scheme/host, canonical IP
		// literals, normalized numeric ports, and no explicit default port.
		canonicalHost := host
		if strings.Contains(host, ":") {
			canonicalHost = "[" + host + "]"
		}
		if portStr != "" {
			canonicalHost = net.JoinHostPort(host, portStr)
		}
		canonical := fmt.Sprintf("%s://%s", scheme, canonicalHost)
		if !seen[canonical] {
			seen[canonical] = true
			origins = append(origins, canonical)
		}
	}

	return origins, nil
}

func corsOriginError(index int, reason string) error {
	return fmt.Errorf("CORS_ALLOWED_ORIGINS entry %d: %s", index+1, reason)
}

func corsOriginPort(host string) (string, error) {
	if strings.HasPrefix(host, "[") {
		closingBracket := strings.IndexByte(host, ']')
		if closingBracket == -1 {
			return "", fmt.Errorf("invalid host")
		}
		suffix := host[closingBracket+1:]
		if suffix == "" {
			return "", nil
		}
		if !strings.HasPrefix(suffix, ":") {
			return "", fmt.Errorf("invalid host")
		}
		return validateCORSOriginPort(suffix[1:])
	}

	if strings.Contains(host, ":") {
		if strings.Count(host, ":") != 1 {
			return "", fmt.Errorf("invalid host")
		}
		return validateCORSOriginPort(host[strings.IndexByte(host, ':')+1:])
	}
	return "", nil
}

func validateCORSOriginPort(port string) (string, error) {
	if port == "" {
		return "", fmt.Errorf("port must not be empty")
	}
	for _, character := range port {
		if character < '0' || character > '9' {
			return "", fmt.Errorf("port must be numeric")
		}
	}
	return port, nil
}

// canonicalCORSHost accepts localhost, ASCII DNS names (including punycode), and IP literals.
// Unicode domain names are intentionally rejected because this package does not perform IDNA conversion.
func canonicalCORSHost(host string) (string, error) {
	host = strings.ToLower(host)
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.String(), nil
	}
	if strings.Contains(host, ":") {
		return "", fmt.Errorf("host must be a valid IPv6 literal or ASCII DNS name")
	}
	if len(host) > 253 || isDecimalIPv4Candidate(host) {
		return "", fmt.Errorf("host must be a valid IPv4/IPv6 literal or ASCII DNS name")
	}

	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("host must be a valid ASCII DNS name")
		}
		for _, character := range []byte(label) {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return "", fmt.Errorf("host must be a valid ASCII DNS name")
			}
		}
	}
	return host, nil
}

func isDecimalIPv4Candidate(host string) bool {
	labels := strings.Split(host, ".")
	if len(labels) != 4 {
		return false
	}
	for _, label := range labels {
		if label == "" {
			return false
		}
		for _, character := range label {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}
