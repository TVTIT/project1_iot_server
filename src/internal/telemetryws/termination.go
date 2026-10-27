package telemetryws

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/coder/websocket"
)

// Transport reasons extend the existing access lifecycle vocabulary. They describe
// the first observed termination trigger, not an inferred physical root cause.
const (
	// Ping's single API budget covers both writing the ping and waiting for pong.
	// This reason alone cannot prove which subphase exhausted that budget.
	RevocationReasonPingTimeout  RevocationReason = "ping_timeout"
	RevocationReasonWriteTimeout RevocationReason = "write_timeout"
	RevocationReasonTransport    RevocationReason = "transport_error"
	RevocationReasonEOF          RevocationReason = "read_eof"
	RevocationReasonRemoteClose  RevocationReason = "remote_close"
	RevocationReasonShutdown     RevocationReason = "shutdown"
	RevocationReasonCanceled     RevocationReason = "context_canceled"
	RevocationReasonRateLimit    RevocationReason = "rate_limit"
	RevocationReasonGap          RevocationReason = "stream_gap"
	RevocationReasonUnknown      RevocationReason = "unknown"
)

var closeReasonKeys = [...]string{"ping_timeout", "write_timeout", "transport_error", "read_eof", "remote_close", "shutdown", "context_canceled", "rate_limit", "stream_gap", "unknown", "queue_overflow", "jwt_expired", "membership_revoked", "authorization_error", "client_close"}

func closeReasonKey(r RevocationReason) string {
	switch r {
	case RevocationReasonSlowConsumer:
		return "queue_overflow"
	case RevocationReasonExpired:
		return "jwt_expired"
	case RevocationReasonDenied:
		return "membership_revoked"
	case RevocationReasonDBError, RevocationReasonSweepTimeout:
		return "authorization_error"
	case RevocationReasonClientClose:
		return "client_close"
	default:
		for _, key := range closeReasonKeys {
			if string(r) == key {
				return key
			}
		}
		return "unknown"
	}
}

// CloseObservation contains only allowlisted classes and process-local durations.
// No error strings, IDs, close text, claims, headers or addresses are retained.
type CloseObservation struct {
	Reason                 string   `json:"reason"`
	Operation              string   `json:"operation"`
	ErrorClass             string   `json:"error_class"`
	ContextClass           string   `json:"context_class"`
	OperationContextClass  string   `json:"operation_context_class"`
	ElapsedSeconds         float64  `json:"elapsed_seconds"`
	DeadlineElapsedSeconds *float64 `json:"deadline_elapsed_seconds,omitempty"`
}

func safeErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case websocket.CloseStatus(err) != -1:
		return "websocket_close"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, net.ErrClosed):
		return "closed_transport"
	default:
		return "other_transport_error"
	}
}

func (s *appSession) recordTermination(parent, operation context.Context, reason RevocationReason, op string, err error) {
	s.terminationOnce.Do(func() {
		obs := CloseObservation{Reason: closeReasonKey(reason), Operation: op, ErrorClass: safeErrorClass(err), ContextClass: "none", OperationContextClass: "none", ElapsedSeconds: time.Since(s.acceptedAt).Seconds()}
		if parent != nil {
			obs.ContextClass = safeErrorClass(context.Cause(parent))
		}
		if operation != nil {
			obs.OperationContextClass = safeErrorClass(context.Cause(operation))
			if deadline, ok := operation.Deadline(); ok {
				d := deadline.Sub(s.acceptedAt).Seconds()
				obs.DeadlineElapsedSeconds = &d
			}
		}
		s.metrics.sessionTerminated(obs)
	})
}

func (s *appSession) recordIOError(parent, operation context.Context, op string, err error) {
	// coder may close the underlying connection on a write-context deadline before
	// Write returns. Preserve the already-observed active write deadline when Read
	// wakes first, rather than racing it into a generic secondary transport error.
	if op == "read" {
		s.ioMu.Lock()
		if s.writeContext != nil && errors.Is(s.writeContext.Err(), context.DeadlineExceeded) {
			operation = s.writeContext
			op = "read_observed_write_deadline"
		}
		s.ioMu.Unlock()
	}
	reason := RevocationReasonTransport
	switch {
	case s.state != nil && s.state.IsExpired(time.Now()):
		reason = RevocationReasonExpired
	case parent.Err() != nil:
		reason = RevocationReasonCanceled
	case operation != nil && errors.Is(operation.Err(), context.DeadlineExceeded):
		switch op {
		case "ping":
			reason = RevocationReasonPingTimeout
		case "write", "read_observed_write_deadline":
			reason = RevocationReasonWriteTimeout
		}
	case websocket.CloseStatus(err) != -1:
		reason = RevocationReasonRemoteClose
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		reason = RevocationReasonEOF
	}
	s.recordTermination(parent, operation, reason, op, err)
}
