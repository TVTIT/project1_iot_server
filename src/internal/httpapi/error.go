// Package httpapi provides shared, safe HTTP transport behavior.
package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIError is the stable error object returned by the HTTP API.
type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// APIErrorEnvelope is the stable top-level API error response.
type APIErrorEnvelope struct {
	Error APIError `json:"error"`
}

// ErrorResponse builds a safe error response using the current request ID.
func ErrorResponse(c *gin.Context, code, message string) APIErrorEnvelope {
	return APIErrorEnvelope{Error: APIError{
		Code:      code,
		Message:   message,
		RequestID: RequestIDFrom(c),
	}}
}

// WriteError aborts the request and writes the stable API error contract.
func WriteError(c *gin.Context, status int, code, message string) {
	if status == http.StatusUnauthorized {
		c.Header("WWW-Authenticate", "Bearer")
	}
	c.AbortWithStatusJSON(status, ErrorResponse(c, code, message))
}
