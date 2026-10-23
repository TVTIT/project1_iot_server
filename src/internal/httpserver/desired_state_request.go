package httpserver

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"iot-platform/internal/digitaltwin"
)

var errControlMediaType = errors.New("unsupported control media type")

// DesiredStateRequest is parsed input only, not authorization. The future
// admission transaction must resolve the target and actor against current DB
// membership, including on receipt replay. decodedEntityID is decoded ONCE by
// the HTTP boundary; this parser never URL-unescapes it again.
type DesiredStateRequest struct {
	Target digitaltwin.ParsedIdentity
	Input  digitaltwin.DesiredRequest
}

func parseDesiredStateRequest(r *http.Request, decodedEntityID string, limit int64) (DesiredStateRequest, int, error) {
	fail := func(err error) (DesiredStateRequest, int, error) {
		status, _ := controlErrorResponse(err)
		return DesiredStateRequest{}, status, err
	}
	target, err := digitaltwin.ParseEntityURN(decodedEntityID)
	if err != nil {
		return fail(err)
	}
	if target.Type != digitaltwin.EntityTypeSensor {
		return fail(digitaltwin.ErrUnsupportedCommandTarget)
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		return fail(digitaltwin.ErrInvalidInput)
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return fail(errControlMediaType)
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return fail(errControlMediaType)
		}
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		return fail(digitaltwin.ErrInvalidInput)
	}
	if limit <= 0 {
		return fail(digitaltwin.ErrInvalidInput)
	}
	if limit > digitaltwin.MaxControlPayloadBytes {
		limit = digitaltwin.MaxControlPayloadBytes
	}
	if r.Body == nil {
		return fail(digitaltwin.ErrInvalidInput)
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(data)) > limit {
		return fail(digitaltwin.ErrControlPayloadTooLarge)
	}
	if err != nil || !validJSONUnicode(data) {
		return fail(digitaltwin.ErrInvalidInput)
	}
	input, err := digitaltwin.ParseDesiredRequest(data, keys[0])
	if err != nil {
		return fail(err)
	}
	return DesiredStateRequest{Target: target, Input: input}, 0, nil
}

// controlErrorResponse defines safe status/code mapping for the later handler;
// it does not write responses or register routes. Never expose err.Error().
func controlErrorResponse(err error) (int, string) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, digitaltwin.ErrDependencyUnavailable):
		return 503, "service_unavailable"
	case errors.Is(err, digitaltwin.ErrNotFound):
		return 404, "not_found"
	case errors.Is(err, digitaltwin.ErrForbidden):
		return 403, "forbidden"
	case errors.Is(err, errControlMediaType):
		return 415, "unsupported_media_type"
	case errors.Is(err, digitaltwin.ErrControlPayloadTooLarge):
		return 413, "payload_too_large"
	case errors.Is(err, digitaltwin.ErrFrequencyNotRepresentable):
		return 400, "frequency_not_representable"
	case errors.Is(err, digitaltwin.ErrUnsupportedCommandTarget):
		return 400, "unsupported_command_target"
	case errors.Is(err, digitaltwin.ErrDesiredVersionConflict):
		return 409, "desired_version_conflict"
	case errors.Is(err, digitaltwin.ErrCommandInProgress):
		return 409, "command_in_progress"
	case errors.Is(err, digitaltwin.ErrIdempotencyConflict):
		return 409, "idempotency_conflict"
	case errors.Is(err, digitaltwin.ErrDesiredVersionOverflow):
		return 409, "desired_version_overflow"
	case errors.Is(err, digitaltwin.ErrInvalidInput), errors.Is(err, digitaltwin.ErrInvalidFrequency):
		return 400, "invalid_request"
	default:
		return 500, "internal_error"
	}
}
