package httpserver

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"iot-platform/internal/digitaltwin"
)

// parseCancelRequest accepts a body-free cancellation request.
//
// The body is probed with at most one byte and Content-Length is never
// trusted: any byte, including whitespace, makes the request invalid. The
// body is never drained without a bound.
func parseCancelRequest(r *http.Request) error {
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		return digitaltwin.ErrInvalidInput
	}
	if raw := r.Header.Get("Content-Type"); raw != "" {
		media, params, err := mime.ParseMediaType(raw)
		if err != nil || media != "application/json" {
			return errControlMediaType
		}
		for key, value := range params {
			if key != "charset" || !strings.EqualFold(value, "utf-8") {
				return errControlMediaType
			}
		}
	}
	if r.Body == nil {
		return digitaltwin.ErrInvalidInput
	}
	var probe [1]byte
	read, err := io.ReadFull(io.LimitReader(r.Body, 1), probe[:])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		// A blocked or failed body read is an invalid request. An expired
		// request context is mapped by the caller.
		return digitaltwin.ErrInvalidInput
	}
	if read > 0 {
		return digitaltwin.ErrInvalidInput
	}
	return nil
}
