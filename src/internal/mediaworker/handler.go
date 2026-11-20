// Package mediaworker handles bounded Stage 8 MQTT requests away from callbacks.
package mediaworker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/media"
	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mqttmedia"
)

var ErrInvalidDelivery = errors.New("invalid media delivery")

type Delivery = mqttmedia.Delivery

type Repository interface {
	Admit(context.Context, media.Admission) (media.Record, error)
	ReserveGrant(context.Context, media.GrantRequest) (media.GrantReservation, media.GrantStatus, error)
	ConfirmGrant(context.Context, media.GrantReservation, time.Time) (bool, error)
	Complete(context.Context, string, string) (media.Record, error)
}

type SignedGrant struct {
	URL       string
	ExpiresAt time.Time
}

func (SignedGrant) String() string   { return "mediaworker.SignedGrant{redacted}" }
func (SignedGrant) GoString() string { return "mediaworker.SignedGrant{redacted}" }

type Signer interface {
	SignUpload(context.Context, string) (SignedGrant, error)
}
type Publisher interface {
	Publish(context.Context, any) error
}

type Options struct {
	Workers                                 int
	SigningTimeout, GrantTTL, InFlightGrace time.Duration
	PublishTimeout                          time.Duration
}

type Handler struct {
	repo      Repository
	signer    Signer
	publisher Publisher
	opts      Options
	mu        sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}
}

func New(repo Repository, signer Signer, publisher Publisher, opts Options) *Handler {
	return &Handler{repo: repo, signer: signer, publisher: publisher, opts: opts}
}

func (h *Handler) Start(ctx context.Context, input <-chan Delivery) error {
	if h == nil || ctx == nil || h.repo == nil || h.signer == nil || h.publisher == nil || input == nil ||
		h.opts.Workers < 1 || h.opts.Workers > 4 ||
		h.opts.SigningTimeout <= 0 || h.opts.GrantTTL <= 0 || h.opts.InFlightGrace < 0 {
		return ErrInvalidDelivery
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done != nil {
		return ErrInvalidDelivery
	}
	workCtx, cancel := context.WithCancel(ctx)
	h.cancel, h.done = cancel, make(chan struct{})
	go func() {
		defer close(h.done)
		var wg sync.WaitGroup
		for i := 0; i < h.opts.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-workCtx.Done():
						return
					case d, ok := <-input:
						if !ok {
							return
						}
						_ = h.Handle(workCtx, d)
					}
				}
			}()
		}
		wg.Wait()
	}()
	return nil
}

func (h *Handler) Stop() {
	if h == nil {
		return
	}
	h.mu.Lock()
	cancel := h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (h *Handler) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.Stop()
	h.mu.Lock()
	done := h.done
	h.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Handle is the narrow callable seam for later publishers/workers and tests.
func (h *Handler) Handle(ctx context.Context, d Delivery) error {
	if ctx == nil || d.Retained || h == nil || h.repo == nil || h.signer == nil || h.publisher == nil {
		return ErrInvalidDelivery
	}
	parts := strings.Split(d.Topic, "/")
	if len(parts) != 5 || parts[0] != "gateways" || parts[2] != "media" || parts[3] != "up" ||
		(parts[4] != "upload_request" && parts[4] != "upload_complete") {
		return ErrInvalidDelivery
	}
	message, err := mediaprotocol.Decode(d.Topic, d.Payload)
	if err != nil {
		return ErrInvalidDelivery
	}
	switch m := message.(type) {
	case mediaprotocol.UploadRequest:
		return h.handleRequest(ctx, m)
	case mediaprotocol.UploadComplete:
		return h.handleComplete(ctx, m)
	default:
		return ErrInvalidDelivery
	}
}

func (h *Handler) handleRequest(ctx context.Context, m mediaprotocol.UploadRequest) error {
	workCtx, cancel := context.WithTimeout(ctx, h.opts.SigningTimeout)
	defer cancel()
	capturedAt, _ := time.Parse(time.RFC3339Nano, m.CapturedAt)
	rec, err := h.repo.Admit(workCtx, media.Admission{GatewayID: m.GatewayID, UploadID: m.UploadID,
		CapturedAt: capturedAt, ExpectedSize: m.SizeBytes, DeclaredSHA256: m.SHA256})
	if err != nil {
		if errors.Is(err, media.ErrForbidden) || errors.Is(err, media.ErrNotFound) {
			return err
		}
		switch {
		case errors.Is(err, media.ErrConflict):
			return h.publishResult(workCtx, mediaprotocol.UploadResult{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID, Status: mediaprotocol.ResultConflict, Code: "ID_METADATA_CONFLICT"})
		case errors.Is(err, media.ErrQuota):
			return h.publishResult(workCtx, mediaprotocol.UploadResult{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID, Status: mediaprotocol.ResultPending, Code: "QUOTA_EXCEEDED", Retryable: true})
		default:
			return h.publishResult(workCtx, mediaprotocol.UploadResult{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID, Status: mediaprotocol.ResultPending, Code: "DEPENDENCY_UNAVAILABLE", Retryable: true})
		}
	}
	if rec.ValidationStatus == "ready" {
		return h.publishResult(workCtx, readyResult(m.GatewayID, m.UploadID, rec))
	}
	if rec.ValidationStatus == "rejected" || rec.ValidationStatus == "expired" {
		return h.publishResult(workCtx, resultFromRecord(m.GatewayID, m.UploadID, rec))
	}
	reservation, status, err := h.repo.ReserveGrant(workCtx, media.GrantRequest{
		GatewayID: m.GatewayID, UploadID: m.UploadID, MediaID: rec.MediaID,
		SigningTimeout: h.opts.SigningTimeout, GrantTTL: h.opts.GrantTTL, InFlightGrace: h.opts.InFlightGrace,
	})
	if err != nil {
		if errors.Is(err, media.ErrForbidden) || errors.Is(err, media.ErrNotFound) {
			return err
		}
		return h.publishGrant(workCtx, mediaprotocol.UploadGrant{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID,
			Status: mediaprotocol.GrantPending, Code: "DEPENDENCY_UNAVAILABLE", Retryable: true})
	}
	if status != media.GrantNeedUpload {
		if status == media.GrantReady {
			return h.publishResult(workCtx, readyResult(m.GatewayID, m.UploadID, rec))
		}
		if status == media.GrantConflict {
			return h.publishResult(workCtx, mediaprotocol.UploadResult{Version: 1, GatewayID: m.GatewayID,
				UploadID: m.UploadID, Status: mediaprotocol.ResultConflict, Code: "ID_METADATA_CONFLICT"})
		}
		switch status {
		case media.GrantRetryAfterCompletion, media.GrantPending, media.GrantVerifying,
			media.GrantRejected, media.GrantExpired:
		default:
			return h.publishGrant(workCtx, mediaprotocol.UploadGrant{Version: 1, GatewayID: m.GatewayID,
				UploadID: m.UploadID, Status: mediaprotocol.GrantPending, Code: "DEPENDENCY_UNAVAILABLE", Retryable: true})
		}
		retryable := status == media.GrantPending || status == media.GrantVerifying || status == media.GrantRetryAfterCompletion
		return h.publishGrant(workCtx, mediaprotocol.UploadGrant{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID,
			Status: string(status), MediaID: mediaIDWhenAllowed(status, rec.MediaID), Retryable: retryable})
	}
	// Never derive an object path from the request or media ID; use only the
	// path committed by Admit/Lookup in the repository record.
	signed, err := h.signer.SignUpload(workCtx, rec.StoragePath)
	if err != nil {
		return nil
	}
	ok, err := h.repo.ConfirmGrant(workCtx, reservation, signed.ExpiresAt)
	if err != nil || !ok || !signed.ExpiresAt.After(time.Now()) || signed.ExpiresAt.After(reservation.ReservedUntil) {
		return nil
	}
	grant := mediaprotocol.UploadGrant{Version: 1, GatewayID: m.GatewayID, UploadID: m.UploadID, Status: mediaprotocol.GrantNeedUpload,
		MediaID: rec.MediaID.String(), UploadURL: signed.URL, ExpiresAt: signed.ExpiresAt.UTC().Format(time.RFC3339)}
	return h.publishGrant(workCtx, grant)
}

func mediaIDWhenAllowed(status media.GrantStatus, id uuid.UUID) string {
	switch status {
	case media.GrantReady, media.GrantVerifying, media.GrantRejected, media.GrantExpired, media.GrantRetryAfterCompletion:
		return id.String()
	default:
		return ""
	}
}

func (h *Handler) handleComplete(ctx context.Context, m mediaprotocol.UploadComplete) error {
	workCtx, cancel := context.WithTimeout(ctx, h.opts.SigningTimeout)
	defer cancel()
	rec, err := h.repo.Complete(workCtx, m.GatewayID, m.UploadID)
	if err != nil {
		// Completion responses are projections of committed repository state only;
		// on dependency/auth failures there is no durable result to publish.
		return err
	}
	return h.publishResult(workCtx, resultFromRecord(m.GatewayID, m.UploadID, rec))
}

func (h *Handler) publishGrant(ctx context.Context, grant mediaprotocol.UploadGrant) error {
	return h.publish(ctx, grant)
}
func (h *Handler) publishResult(ctx context.Context, result mediaprotocol.UploadResult) error {
	return h.PublishResult(ctx, result)
}

// PublishResult is the deliberately narrow output seam for later verifier
// workers. The typed protocol encoder rejects unsafe correlation/status data.
func (h *Handler) PublishResult(ctx context.Context, result mediaprotocol.UploadResult) error {
	if h == nil || h.publisher == nil || ctx == nil {
		return ErrInvalidDelivery
	}
	if _, err := mediaprotocol.Encode(mediaprotocol.UploadResultTopic(result.GatewayID), result); err != nil {
		return ErrInvalidDelivery
	}
	return h.publish(ctx, result)
}
func (h *Handler) publish(ctx context.Context, message any) error {
	if h.opts.PublishTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.opts.PublishTimeout)
		defer cancel()
	}
	return h.publisher.Publish(ctx, message)
}

func readyResult(gatewayID, uploadID string, rec media.Record) mediaprotocol.UploadResult {
	return mediaprotocol.UploadResult{Version: 1, GatewayID: gatewayID, UploadID: uploadID, Status: mediaprotocol.ResultReady, MediaID: rec.MediaID.String()}
}
func resultFromRecord(gatewayID, uploadID string, rec media.Record) mediaprotocol.UploadResult {
	r := mediaprotocol.UploadResult{Version: 1, GatewayID: gatewayID, UploadID: uploadID}
	switch rec.ValidationStatus {
	case "ready":
		return readyResult(gatewayID, uploadID, rec)
	case "pending":
		r.Status = mediaprotocol.ResultPending
		r.Retryable = true
		if rec.LastVerificationError == "OBJECT_MISSING" {
			r.Code = "OBJECT_MISSING"
			if rec.MediaID != uuid.Nil {
				r.MediaID = rec.MediaID.String()
			}
		}
	case "verifying":
		r.Status = mediaprotocol.ResultVerifying
		r.Retryable = true
	case "rejected":
		r.Status = mediaprotocol.ResultRejected
		r.Code = safeTerminalCode(rec.TerminalReason)
	case "expired":
		r.Status = mediaprotocol.ResultExpired
		r.Code = "INTENT_EXPIRED"
	default:
		r.Status = mediaprotocol.ResultPending
		r.Code = "DEPENDENCY_UNAVAILABLE"
		r.Retryable = true
	}
	return r
}
func safeTerminalCode(code *string) string {
	if code == nil {
		return "UPLOAD_REJECTED"
	}
	switch *code {
	case "invalid_media", "hash_mismatch", "size_mismatch", "content_type_mismatch", "verification_exhausted":
		return strings.ToUpper(strings.ReplaceAll(*code, "-", "_"))
	}
	return "UPLOAD_REJECTED"
}
