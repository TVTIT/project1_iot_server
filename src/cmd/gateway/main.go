// Command gateway runs the development Gateway simulator: a synthetic telemetry
// producer that stores every batch in a durable SQLite outbox before publishing
// it over MQTT/TLS and only removes a batch after an application-level
// database-commit ACK. The same binary also exposes small administrative
// commands to inspect and manually quarantine pending items.
//
// The simulator is development and integration tooling. It emits simulated data,
// not real sensor measurements, and it is not device firmware.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gatewaysim"
)

// Administrative commands only read GATEWAY_OUTBOX_PATH, so the defaults below
// keep the operator output bounded and the SQLite operations finite.
const (
	adminOpTimeout = 5 * time.Second

	defaultPendingLimit = 20
	maxPendingLimit     = 1000
	maxReasonLength     = 256

	defaultCompletionTimeout = 30 * time.Second
	minCompletionTimeout     = 100 * time.Millisecond
	maxCompletionTimeout     = 10 * time.Minute
	// maxFiniteBatches caps --batches. A finite target above
	// GATEWAY_OUTBOX_MAX_ROWS cannot complete because generation pauses at the
	// outbox quota, so a run that large should be a normal unbounded run.
	maxFiniteBatches = 100_000

	// maxAdmissionCapacity mirrors gatewaysim's fixed admission-queue bound. The
	// simulator derives a smaller value from GATEWAY_MAX_INFLIGHT.
	maxAdmissionCapacity = 4096
)

const usageText = `usage: gateway [command] [flags]

commands:
  run                     produce synthetic telemetry until stopped (default)
  pending [--limit N]     list pending outbox items (metadata only)
  count                   print pending/quarantined row and byte totals
  quarantine --message-id UUID --reason TEXT
                          move one pending item to quarantined (run stopped)
  help                    show this message

run flags:
  --batches N             stop after N unique batches (0 = until stopped)
  --completion-timeout D  time to wait for application ACKs after the target
  --measure               enable bounded ACK latency measurement (requires --batches > 0)
  --measure-capacity N    explicit measurement capacity (defaults to --batches)
`

// simTransport is the transport surface the run lifecycle composes. It is the
// gatewaysim scheduler Publisher plus the start/readiness/stop calls the CLI
// drives. *gatewaysim.Transport satisfies it.
type simTransport interface {
	gatewaysim.Publisher
	Start() error
	WaitReady(ctx context.Context) error
	Stop(ctx context.Context) error
}

// buildTransport creates the MQTT transport. It is a package-level seam so the
// full run lifecycle can be exercised by a test through a small fake transport
// without a broker. It is never nil in production.
var buildTransport = func(cfg gatewaysim.Config, admit gatewaysim.ACKAdmitter) (simTransport, error) {
	return gatewaysim.NewTransport(gatewaysim.TransportOptionsFromConfig(cfg), admit)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

// run parses one gateway simulator command and returns a process exit code. It
// is the single testable entry point: it never calls os.Exit and never reads
// os.Args or the real environment directly.
func run(ctx context.Context, args []string, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer) int {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) == 0 {
		return runSimulator(ctx, lookup, stdout, stderr, 0, defaultCompletionTimeout, 0)
	}
	switch args[0] {
	case "run":
		return parseRun(ctx, args[1:], lookup, stdout, stderr)
	case "pending":
		return parsePending(ctx, args[1:], lookup, stdout, stderr)
	case "count":
		return parseCount(ctx, args[1:], lookup, stdout, stderr)
	case "quarantine":
		return parseQuarantine(ctx, args[1:], lookup, stdout, stderr)
	case "help", "-h", "--help":
		write(stdout, usageText)
		return 0
	default:
		writef(stderr, "unknown command %q\n%s", args[0], usageText)
		return 2
	}
}

func parseRun(ctx context.Context, args []string, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	batches := fs.Int("batches", 0, "stop after N unique batches (0 = until stopped)")
	completion := fs.Duration("completion-timeout", defaultCompletionTimeout, "time to wait for application ACKs after the batch target")
	measure := fs.Bool("measure", false, "record ACK latency and emit measurement summary for finite run (requires --batches > 0)")
	measureLatency := fs.Bool("measure-latency", false, "alias for --measure")
	measureCap := fs.Int("measure-capacity", 0, "explicit measurement capacity (defaults to --batches)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		writef(stderr, "unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *batches < 0 || *batches > maxFiniteBatches {
		writef(stderr, "--batches must be between 0 and %d\n", maxFiniteBatches)
		return 2
	}
	if *completion < minCompletionTimeout || *completion > maxCompletionTimeout {
		writef(stderr, "--completion-timeout must be between %s and %s\n", minCompletionTimeout, maxCompletionTimeout)
		return 2
	}

	capacity := 0
	if *measure || *measureLatency {
		if *batches <= 0 {
			writef(stderr, "--measure requires a finite run (--batches > 0)\n")
			return 2
		}
		if *measureCap > 0 {
			if *measureCap > gatewaysim.MaxMeasurementCapacity {
				writef(stderr, "--measure-capacity must be between 1 and %d\n", gatewaysim.MaxMeasurementCapacity)
				return 2
			}
			capacity = *measureCap
		} else {
			if *batches > gatewaysim.MaxMeasurementCapacity {
				writef(stderr, "--batches exceeds max measurement capacity %d; specify --measure-capacity explicitly\n", gatewaysim.MaxMeasurementCapacity)
				return 2
			}
			capacity = *batches
		}
	} else if *measureCap > 0 {
		writef(stderr, "--measure-capacity requires --measure\n")
		return 2
	}

	return runSimulator(ctx, lookup, stdout, stderr, *batches, *completion, capacity)
}

// runSimulator composes the outbox, ACK processor, transport and scheduler and
// runs until the context ends, a finite target completes, or a classified fatal
// stop occurs. Every resource opened here is closed on every exit path, and
// SQLite is closed only after the scheduler and ACK worker have stopped.
func runSimulator(ctx context.Context, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer, batches int, completionBudget time.Duration, measureCapacity int) int {
	cfg, err := gatewaysim.Load(lookup)
	if err != nil {
		writef(stderr, "configuration error: %s\n", err)
		return 2
	}

	var tracker *gatewaysim.LatencyTracker
	if measureCapacity > 0 {
		t, err := gatewaysim.NewLatencyTracker(measureCapacity)
		if err != nil {
			writef(stderr, "error: %s\n", err)
			return 2
		}
		tracker = t
	}

	outbox, err := gatewaysim.OpenOutbox(gatewaysim.OutboxConfig{
		Path:      cfg.OutboxPath,
		GatewayID: cfg.GatewayID,
		SensorID:  cfg.SensorID,
		MaxRows:   cfg.OutboxMaxRows,
		MaxBytes:  cfg.OutboxMaxBytes,
		OpTimeout: cfg.SQLiteTimeout,
	})
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}

	var (
		processor *gatewaysim.ACKProcessor
		transport simTransport
	)
	// closeAll tears down every resource opened so far within one bounded
	// budget. It is safe once, on any startup failure after the outbox opened.
	closeAll := func() {
		tctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if processor != nil {
			_ = processor.Stop(tctx)
		}
		if transport != nil {
			_ = transport.Stop(tctx)
		}
		_ = outbox.Close()
	}

	createdProcessor, err := gatewaysim.NewACKProcessor(outbox, admissionCapacity(cfg.MaxInflight), gatewaysim.WithACKLatencyRecorder(tracker))
	if err != nil {
		writeln(stderr, "error: could not create the ACK processor")
		closeAll()
		return 1
	}
	processor = createdProcessor

	createdTransport, err := buildTransport(cfg, processor)
	if err != nil {
		writef(stderr, "error: could not create the MQTT transport: %s\n", transportErrorMessage(err))
		closeAll()
		return 1
	}
	transport = createdTransport

	scheduler, err := gatewaysim.NewScheduler(gatewaysim.SchedulerOptions{
		Outbox:           outbox,
		Publisher:        transport,
		Ack:              processor,
		Config:           cfg,
		FiniteBatch:      batches,
		CompletionBudget: completionBudget,
		Latency:          tracker,
		Logger:           slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		writeln(stderr, "error: could not create the scheduler")
		closeAll()
		return 1
	}

	processor.Start()
	if err := transport.Start(); err != nil {
		writef(stderr, "error: could not start the MQTT transport: %s\n", transportErrorMessage(err))
		closeAll()
		return 1
	}

	// Readiness gating: the scheduler also checks Ready before any publish, but
	// the CLI refuses to report "running" until the own-ACK subscription is
	// positively granted, within one bounded startup budget.
	readyCtx, cancelReady := context.WithTimeout(ctx, cfg.ConnectTimeout+cfg.SubscribeTimeout)
	readyErr := transport.WaitReady(readyCtx)
	cancelReady()
	if readyErr != nil {
		writeln(stderr, "error: MQTT transport did not reach a subscribed ready state before the startup deadline")
		closeAll()
		return 1
	}

	writef(stdout, "gateway simulator running gateway=%s sensor=%s boot_id=%s\n", cfg.GatewayID, cfg.SensorID, scheduler.BootID())

	schedulerDone := make(chan error, 1)
	go func() { schedulerDone <- scheduler.Run(ctx) }()

	select {
	case <-ctx.Done():
	case <-scheduler.Done():
	}

	// One shutdown budget for every teardown step. The scheduler's own publish
	// drain shares this deadline instead of adding a second full timeout.
	shutCtx, cancelShut := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShut()
	if deadline, ok := shutCtx.Deadline(); ok {
		scheduler.BeginShutdown(deadline)
	}
	scheduler.Stop()

	var schedErr error
	select {
	case schedErr = <-schedulerDone:
	case <-shutCtx.Done():
		schedErr = shutCtx.Err()
	}

	if err := processor.Stop(shutCtx); err != nil {
		writeln(stderr, "warning: the ACK worker did not stop within the shutdown budget")
	}
	if err := transport.Stop(shutCtx); err != nil {
		writeln(stderr, "warning: the MQTT transport did not stop within the shutdown budget")
	}

	// Read the final durable counts and payload-free counters before closing the
	// outbox, then close it after the workers have stopped.
	stats := scheduler.Stats()
	ackStats := processor.Stats()
	counts, countsErr := outbox.Counts(context.Background())
	if closeErr := outbox.Close(); closeErr != nil {
		writeln(stderr, "warning: the outbox did not close cleanly")
	}
	if countsErr != nil {
		counts = gatewaysim.OutboxCounts{}
	}

	writef(stdout, "run summary: enqueued=%d publish_attempts=%d app_acknowledged=%d pending_rows=%d pending_bytes=%d quarantined_rows=%d quarantined_bytes=%d completed=%t\n",
		stats.UniqueEnqueued, stats.PublishAttempts, ackStats.Acknowledged,
		counts.PendingRows, counts.PendingBytes, counts.QuarantinedRows, counts.QuarantinedBytes, stats.Completed)

	if tracker != nil {
		writef(stdout, "serialization summary: unique_bytes=%d attempted_bytes=%d unique_samples=%d\n",
			stats.UniquePayloadBytes, stats.AttemptedPayloadBytes, stats.UniqueSamples)
		snap := tracker.Snapshot()
		writef(stdout, "%s\n", gatewaysim.FormatMeasurementSummary(snap))
	}

	switch {
	case stats.Completed:
		return 0
	case errors.Is(schedErr, gatewaysim.ErrSchedulerCompletion):
		writeln(stderr, "error: the finite run did not complete within the completion budget; unacknowledged items are preserved for the next run")
		return 1
	case schedErr != nil && errors.Is(schedErr, context.DeadlineExceeded):
		writeln(stderr, "error: the scheduler did not stop within the shutdown budget; recovery is supervised")
		return 1
	case schedErr != nil:
		writef(stderr, "error: the simulator stopped: %s\n", schedulerErrorMessage(schedErr))
		return 1
	case ctx.Err() != nil:
		writeln(stdout, "stopped on signal; unacknowledged items are preserved for the next run")
		return 0
	default:
		return 0
	}
}

func parsePending(ctx context.Context, args []string, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pending", flag.ContinueOnError)
	fs.SetOutput(stderr)
	limit := fs.Int("limit", defaultPendingLimit, "maximum number of pending items to list")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		writef(stderr, "unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *limit < 1 || *limit > maxPendingLimit {
		writef(stderr, "--limit must be between 1 and %d\n", maxPendingLimit)
		return 2
	}

	outbox, err := openAdminOutbox(lookup)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	defer func() { _ = outbox.Close() }()

	items, err := outbox.Pending(ctx, *limit)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	counts, err := outbox.Counts(ctx)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}

	for _, item := range items {
		lastSequence := item.FirstSequence + int64(item.SampleCount) - 1
		writef(stdout, "pending message_id=%s boot_id=%s sequence=%d..%d sample_count=%d bytes=%d status=%s\n",
			item.MessageID, item.BootID, item.FirstSequence, lastSequence, item.SampleCount, item.ByteSize, item.Status)
	}
	printCounts(stdout, counts)
	return 0
}

func parseCount(ctx context.Context, args []string, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		writef(stderr, "unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	outbox, err := openAdminOutbox(lookup)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	defer func() { _ = outbox.Close() }()

	counts, err := outbox.Counts(ctx)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	printCounts(stdout, counts)
	return 0
}

func parseQuarantine(ctx context.Context, args []string, lookup gatewaysim.LookupFunc, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("quarantine", flag.ContinueOnError)
	fs.SetOutput(stderr)
	messageID := fs.String("message-id", "", "pending message UUID to quarantine")
	reason := fs.String("reason", "", "bounded operator reason for quarantine")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		writef(stderr, "unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	id, err := uuid.Parse(strings.TrimSpace(*messageID))
	if err != nil || id == uuid.Nil {
		writeln(stderr, "--message-id must be a valid UUID")
		return 2
	}
	trimmedReason := strings.TrimSpace(*reason)
	if trimmedReason == "" || len(trimmedReason) > maxReasonLength {
		writef(stderr, "--reason must be 1..%d characters\n", maxReasonLength)
		return 2
	}

	outbox, err := openAdminOutbox(lookup)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	defer func() { _ = outbox.Close() }()

	moved, err := outbox.Quarantine(ctx, id, trimmedReason)
	if err != nil {
		writeln(stderr, outboxErrorMessage(err))
		return 1
	}
	if !moved {
		writef(stdout, "no pending item with message_id=%s (nothing changed)\n", id)
		return 1
	}
	writef(stdout, "quarantined message_id=%s\n", id)
	return 0
}

// openAdminOutbox opens an existing outbox for an administrative command using
// only GATEWAY_OUTBOX_PATH. It never creates a missing file and never reads a
// credential or MQTT setting.
func openAdminOutbox(lookup gatewaysim.LookupFunc) (*gatewaysim.Outbox, error) {
	path, ok := lookup("GATEWAY_OUTBOX_PATH")
	path = strings.TrimSpace(path)
	if !ok || path == "" {
		return nil, fmt.Errorf("GATEWAY_OUTBOX_PATH is required")
	}
	return gatewaysim.OpenOutboxAdmin(path, adminOpTimeout)
}

// write, writef and writeln are best-effort operator output. The writer error
// is discarded deliberately: a failed write to the process stdout/stderr does
// not change the command's exit code, which already reflects the outcome.
func write(w io.Writer, a ...any)                 { _, _ = fmt.Fprint(w, a...) }
func writef(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
func writeln(w io.Writer, a ...any)               { _, _ = fmt.Fprintln(w, a...) }

func printCounts(stdout io.Writer, counts gatewaysim.OutboxCounts) {
	writef(stdout, "pending_rows=%d pending_bytes=%d quarantined_rows=%d quarantined_bytes=%d\n",
		counts.PendingRows, counts.PendingBytes, counts.QuarantinedRows, counts.QuarantinedBytes)
}

// admissionCapacity derives the ACK admission-queue size from the in-flight
// bound. It is a small multiple of MaxInflight so a burst of ACKs during a
// retry round still fits, and it is clamped to the simulator's documented bound.
func admissionCapacity(maxInflight int) int {
	capacity := maxInflight * 2
	if capacity < 1 {
		return 1
	}
	if capacity > maxAdmissionCapacity {
		return maxAdmissionCapacity
	}
	return capacity
}

// outboxErrorMessage maps an outbox failure to a path-, DSN- and driver-free
// message. It never echoes the raw error, which may embed a filesystem path or
// a driver string.
func outboxErrorMessage(err error) string {
	switch {
	case errors.Is(err, gatewaysim.ErrOutboxMissing):
		return "error: the outbox file does not exist"
	case errors.Is(err, gatewaysim.ErrOutboxBusy):
		return "error: the outbox is locked by another simulator or admin process; stop it first"
	case errors.Is(err, gatewaysim.ErrOutboxIdentity):
		return "error: the outbox file is bound to a different gateway or sensor"
	case errors.Is(err, gatewaysim.ErrOutboxSchema):
		return "error: the outbox file has an unsupported schema version"
	case errors.Is(err, gatewaysim.ErrOutboxCorrupt):
		return "error: the outbox file is not a usable SQLite database"
	case errors.Is(err, gatewaysim.ErrOutboxReason):
		return "error: the quarantine reason is required and bounded"
	case errors.Is(err, gatewaysim.ErrOutboxFull):
		return "error: the outbox reached its configured capacity"
	case errors.Is(err, gatewaysim.ErrOutboxCommitUnknown):
		return "error: the outbox commit outcome is unknown; supervised restart required"
	default:
		return "error: the outbox operation failed"
	}
}

// schedulerErrorMessage maps a scheduler stop to a stable, payload-free reason.
func schedulerErrorMessage(err error) string {
	switch {
	case errors.Is(err, gatewaysim.ErrSchedulerCompletion):
		return "the finite completion budget expired with pending items"
	case errors.Is(err, gatewaysim.ErrSchedulerCommitUnknown):
		return "the outbox commit outcome is unknown; supervised restart required"
	case errors.Is(err, gatewaysim.ErrSchedulerFatal):
		return "an internal failure stopped the scheduler"
	case errors.Is(err, gatewaysim.ErrSchedulerEnqueue):
		return "a batch enqueue failed"
	case errors.Is(err, gatewaysim.ErrSchedulerPending):
		return "a pending scan failed"
	case errors.Is(err, gatewaysim.ErrSchedulerLookup):
		return "an in-flight lookup failed"
	default:
		return "the scheduler stopped"
	}
}

// transportErrorMessage maps a transport construction failure to a message. The
// construction errors are already path- and credential-free; the two sentinels
// are mapped to stable text.
func transportErrorMessage(err error) string {
	switch {
	case errors.Is(err, gatewaysim.ErrTransportStuck):
		return "transport teardown was unconfirmed; supervised restart required"
	case errors.Is(err, gatewaysim.ErrTransportClosed):
		return "transport is closed"
	default:
		return err.Error()
	}
}
