package gatewaysim

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"iot-platform/internal/digitaltwin"
)

// ErrControlUnconfirmed requires supervised reopen; no success may be published.
var ErrControlUnconfirmed = errors.New("gatewaysim: control application is unconfirmed")

// SamplingController exposes idempotent set and actual active readback.
type SamplingController interface {
	SetSampleInterval(context.Context, int64) error
	ActiveSampleInterval() int64
}

// Controller serializes one Sensor's durable command/apply/result lifecycle.
type Controller struct {
	mu      sync.Mutex
	out     *Outbox
	sampler SamplingController
	now     func() time.Time
}

// NewController creates an inert owner; Recover must run before sampling starts.
func NewController(out *Outbox, sampler SamplingController) (*Controller, error) {
	if out == nil || sampler == nil {
		return nil, ErrControlUnconfirmed
	}
	return &Controller{out: out, sampler: sampler, now: time.Now}, nil
}

// Handle returns only durable canonical result bytes, never a synthetic ACK.
func (c *Controller) Handle(ctx context.Context, topic string, payload []byte, retained bool) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx == nil {
		return nil, digitaltwin.ErrInvalidInput
	}
	if _, err := digitaltwin.ParseMQTTCommand(topic, payload, retained); err != nil {
		return nil, err
	}
	receipt, err := c.out.ReserveControl(ctx, topic, payload, c.now())
	if err != nil {
		return nil, err
	}
	if len(receipt.Result) > 0 {
		return receipt.Result, nil
	}
	if !receipt.Fresh {
		return nil, ErrControlBusy
	}
	return c.apply(ctx, receipt)
}

// Recover restores committed configuration and resolves prepared work before
// new samples. Only this path resumes a receipt already prepared on disk.
func (c *Controller) Recover(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cfg, found, err := c.out.Configuration(ctx)
	if err != nil {
		return err
	}
	if found {
		if err := c.sampler.SetSampleInterval(ctx, cfg.SampleIntervalUS); err != nil {
			return errors.Join(ErrControlUnconfirmed, err)
		}
		if c.sampler.ActiveSampleInterval() != cfg.SampleIntervalUS {
			return ErrControlUnconfirmed
		}
	}
	receipts, err := c.out.ControlReceipts(ctx)
	if err != nil {
		return err
	}
	for _, r := range receipts {
		if len(r.Result) == 0 {
			if _, err := c.apply(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Controller) apply(ctx context.Context, receipt ControlReceipt) ([]byte, error) {
	command := receipt.Command
	now := c.now()
	if now.Before(command.IssuedAt) {
		return nil, ErrControlUnconfirmed
	}
	failure := ""
	cfg, found, err := c.out.Configuration(ctx)
	if err != nil {
		return nil, err
	}
	if found && command.DesiredVersion <= cfg.DesiredVersion {
		failure = "STALE_DESIRED_VERSION"
	}
	// Check expiry last, immediately before touching the adapter. A duplicate
	// completed receipt was already returned before these admission checks.
	now = c.now()
	if !now.Before(command.ExpiresAt) {
		failure = "COMMAND_EXPIRED"
	}
	if failure == "" {
		applyCtx, cancel := context.WithTimeout(ctx, command.ExpiresAt.Sub(now))
		err = c.sampler.SetSampleInterval(applyCtx, command.Parameters.SampleIntervalUS)
		cancel()
		if err != nil {
			if errors.Is(err, ErrOutboxFull) {
				failure = "APPLY_FAILED"
			} else {
				return nil, errors.Join(ErrControlUnconfirmed, err)
			}
		}
		if failure == "" && c.sampler.ActiveSampleInterval() != command.Parameters.SampleIntervalUS {
			return nil, ErrControlUnconfirmed
		}
	}
	result := digitaltwin.CommandResult{ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID, GatewayID: command.GatewayID, EntityID: command.EntityID, DesiredVersion: command.DesiredVersion, Status: digitaltwin.CommandSucceeded, ReportedState: &command.Parameters, CompletedAt: c.now().UTC().Truncate(time.Microsecond)}
	if failure != "" {
		result.Status = digitaltwin.CommandFailed
		result.ReportedState = nil
		result.ErrorCode = failure
		result.ErrorMessage = digitaltwin.SafeCommandErrorMessage(failure)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if err = c.out.CompleteControl(ctx, command.CommandID, payload, c.now()); err != nil {
		return nil, errors.Join(ErrControlUnconfirmed, err)
	}
	return payload, nil
}
