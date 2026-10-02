package httpapi

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const requestIDContextKey = "request_id"

// RequestIDMiddleware accepts only log-safe request IDs or generates a UUID.
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if len(c.Request.Header.Values("X-Request-ID")) != 1 || !validRequestID(requestID) {
			requestID = uuid.NewString()
		}
		c.Set(requestIDContextKey, requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// RequestIDFrom returns the validated request ID associated with the request.
func RequestIDFrom(c *gin.Context) string {
	value, ok := c.Get(requestIDContextKey)
	if !ok {
		return ""
	}
	requestID, _ := value.(string)
	return requestID
}

func validRequestID(value string) bool {
	if len(value) == 0 || len(value) > 128 || !asciiLetterOrDigit(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		character := value[i]
		if !asciiLetterOrDigit(character) && character != '.' && character != '_' && character != ':' && character != '-' {
			return false
		}
	}
	return true
}

func asciiLetterOrDigit(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}
