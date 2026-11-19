package gatewaysim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	mu             sync.Mutex
	out            *Outbox
	sampler        SamplingController
	now            func() time.Time
	stage6Enabled  bool
	recovered      bool
	enqueueReceipt func(ControlReceipt)
}

// NewController creates an inert owner; Recover must run before sampling starts.
func NewController(out *Outbox, sampler SamplingController, stage6Enabled ...bool) (*Controller, error) {
	if out == nil || sampler == nil || len(stage6Enabled) > 1 {
		return nil, ErrControlUnconfirmed
	}
	stage6 := len(stage6Enabled) == 1 && stage6Enabled[0]
	return &Controller{out: out, sampler: sampler, now: time.Now, stage6Enabled: stage6}, nil
}

// Handle returns only durable canonical result bytes, never a synthetic ACK.
func (c *Controller) Handle(ctx context.Context, topic string, payload []byte, retained bool) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx == nil {
		return nil, digitaltwin.ErrInvalidInput
	}
	command, err := digitaltwin.ParseMQTTCommand(topic, payload, retained)
	if err != nil {
		return nil, err
	}
	now := c.now()
	var receipt ControlReceipt
	if c.stage6Enabled {
		existing, found, findErr := c.out.findControlReceipt(ctx, command.CommandID)
		if findErr != nil {
			return nil, findErr
		}
		if found {
			if existing.Topic != topic || !bytes.Equal(existing.Payload, payload) {
				return nil, ErrControlConflict
			}
			c.enqueue(existing)
			if len(existing.Result) > 0 {
				return existing.Result, nil
			}
			return nil, ErrControlBusy
		}
		var wireReceipt []byte
		wireReceipt, err = digitaltwin.MarshalMQTTReceipt(digitaltwin.CommandReceipt{
			ProtocolVersion: 1,
			MessageType:     "command_receipt",
			CommandID:       command.CommandID,
			GatewayID:       command.GatewayID,
			EntityID:        command.EntityID,
			DesiredVersion:  command.DesiredVersion,
			ReceivedAt:      now.UTC().Truncate(time.Microsecond),
			CommandHash:     fmt.Sprintf("%x", sha256.Sum256(payload)),
		})
		if err != nil {
			return nil, err
		}
		receipt, err = c.out.ReserveControlWithReceipt(ctx, topic, payload, wireReceipt, now)
	} else {
		receipt, err = c.out.ReserveControl(ctx, topic, payload, now)
	}
	if err != nil {
		return nil, err
	}
	c.enqueue(receipt)
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
			c.enqueue(r)
			if _, err := c.apply(ctx, r); err != nil {
				return err
			}
		}
	}
	c.recovered = true
	return nil
}

// Readback returns a transient, read-only view serialized with command apply.
// Waiting for the apply mutex and the SQLite read are bounded by the query context.
func (c *Controller) Readback(ctx context.Context, query digitaltwin.ConfigurationQuery) (digitaltwin.ConfigurationSnapshot, error) {
	if ctx == nil || !c.stage6Enabled || !query.ValidAt(c.now()) ||
		query.GatewayID != c.out.gatewayID ||
		query.EntityID != "urn:ngsi-ld:Sensor:"+c.out.gatewayID+":"+c.out.sensorID {
		return digitaltwin.ConfigurationSnapshot{}, digitaltwin.ErrInvalidInput
	}
	if err := c.lockContext(ctx); err != nil {
		return digitaltwin.ConfigurationSnapshot{}, err
	}
	defer c.mu.Unlock()
	if !c.recovered {
		return digitaltwin.ConfigurationSnapshot{}, digitaltwin.ErrInvalidInput
	}
	if err := ctx.Err(); err != nil || !query.ValidAt(c.now()) {
		if err != nil {
			return digitaltwin.ConfigurationSnapshot{}, err
		}
		return digitaltwin.ConfigurationSnapshot{}, digitaltwin.ErrInvalidInput
	}

	snapshot := digitaltwin.ConfigurationSnapshot{
		ProtocolVersion:    1,
		MessageType:        "configuration_snapshot",
		RequestID:          query.RequestID,
		GatewayID:          query.GatewayID,
		EntityID:           query.EntityID,
		ConfigurationState: "unavailable",
	}
	actual := c.sampler.ActiveSampleInterval()
	if actual >= digitaltwin.MinSampleIntervalUS && actual <= digitaltwin.MaxSampleIntervalUS {
		snapshot.ActualSampleIntervalUS = &actual
	}

	config, found, err := c.out.Configuration(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return digitaltwin.ConfigurationSnapshot{}, ctx.Err()
		}
		// A damaged/unavailable store is never represented as an empty watermark.
		snapshot.WatermarkVersion = 0
		readAt := c.now()
		if !query.ValidAt(readAt) {
			return digitaltwin.ConfigurationSnapshot{}, digitaltwin.ErrInvalidInput
		}
		snapshot.ReadbackAt = readAt.UTC().Truncate(time.Microsecond)
		return snapshot, nil
	}
	if found {
		snapshot.ConfigurationState = "configured"
		snapshot.WatermarkVersion = config.DesiredVersion
		durable := config.SampleIntervalUS
		snapshot.DurableSampleIntervalUS = &durable
	} else {
		snapshot.ConfigurationState = "unconfigured"
	}
	readAt := c.now()
	if !query.ValidAt(readAt) {
		return digitaltwin.ConfigurationSnapshot{}, digitaltwin.ErrInvalidInput
	}
	snapshot.ReadbackAt = readAt.UTC().Truncate(time.Microsecond)
	return snapshot, nil
}

func (c *Controller) lockContext(ctx context.Context) error {
	for {
		if c.mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Controller) enqueue(receipt ControlReceipt) {
	if c.stage6Enabled && len(receipt.ReceiptBytes) > 0 && c.enqueueReceipt != nil {
		c.enqueueReceipt(receipt)
	}
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
