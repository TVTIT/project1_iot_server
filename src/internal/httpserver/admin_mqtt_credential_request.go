package httpserver

import (
	"io"
	"net/http"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

func parseCredentialRequest(r *http.Request, id string, mutation bool, limit int64) (uuid.UUID, int) {
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(data)) > limit {
		return uuid.Nil, http.StatusRequestEntityTooLarge
	}
	if err != nil || len(data) != 0 || gateway.ValidateGatewayID(id) != nil || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return uuid.Nil, http.StatusBadRequest
	}
	if !mutation {
		return uuid.Nil, 0
	}
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return uuid.Nil, http.StatusBadRequest
	}
	key, err := uuid.Parse(values[0])
	if err != nil || key == uuid.Nil {
		return uuid.Nil, http.StatusBadRequest
	}
	return key, 0
}
