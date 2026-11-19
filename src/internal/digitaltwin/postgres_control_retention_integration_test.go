package digitaltwin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func TestNativeControlRetentionPurgesHistoryButPreservesReplaySemantics(t *testing.T) {
	for _, tc := range []struct {
		name, historical, current string
		interval                  int64
	}{
		{"half_up_and_exponent", "99.9650122457140001", "1e-1", 10003},
		{"integer_fingerprints", "10", "5", 100000},
		{"leading_zero_exponent", "1.0000001e00001", "5", 100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testNativeControlRetentionReplay(t, tc.historical, tc.current, tc.interval)
		})
	}
}

func testNativeControlRetentionReplay(t *testing.T, historical, current string, interval int64) {
	t.Helper()
	f := newControlFixture(t)
	cfg := digitaltwin.DefaultControlConfig()
	cfg.Stage6Enabled = true
	var err error
	f.repo, err = digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}

	oldRequest := controlRequest(t, historical, "0", "retention-real-admit")
	if got := oldRequest.Frequency.SampleIntervalUS(); got != interval {
		t.Fatalf("Go parser interval=%d, want %d", got, interval)
	}
	oldReceipt, err := f.repo.Admit(f.ctx, f.owner, f.urn, oldRequest)
	if err != nil || oldReceipt.StatusCode != 202 || oldReceipt.CommandID == nil {
		t.Fatalf("real historical Admit receipt=%+v err=%v", oldReceipt, err)
	}
	oldID := uuid.MustParse(*oldReceipt.CommandID)
	var storedFrequency string
	if err = f.admin.QueryRow(f.ctx,
		`SELECT requested_frequency_hz FROM public.twin_commands WHERE command_id=$1`, oldID).Scan(&storedFrequency); err != nil ||
		storedFrequency != historical {
		t.Fatalf("real Admit did not preserve exact decimal frequency token: got=%q err=%v", storedFrequency, err)
	}
	var originalTopic string
	var originalPayload []byte
	if err = f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM public.twin_outbox WHERE command_id=$1`,
		oldID).Scan(&originalTopic, &originalPayload); err != nil {
		t.Fatal("read real Admit outbox:", err)
	}
	oldCommand, err := digitaltwin.ParseMQTTCommand(originalTopic, originalPayload, false)
	if err != nil {
		t.Fatal(err)
	}
	oldCommand.IssuedAt = oldCommand.IssuedAt.Add(-744 * time.Hour)
	oldCommand.ExpiresAt = oldCommand.ExpiresAt.Add(-744 * time.Hour)
	_, err = f.admin.Exec(f.ctx, `UPDATE public.twin_commands
		SET issued_at=issued_at-interval '744 hours',
		    expires_at=expires_at-interval '744 hours'
		WHERE command_id=$1`, oldID)
	if err != nil {
		t.Fatal("backdate owned historical command:", err)
	}
	payload, err := json.Marshal(oldCommand)
	if err != nil {
		t.Fatal("marshal fixture historical outbox:", err)
	}
	if _, err = f.admin.Exec(f.ctx, `DELETE FROM public.twin_outbox WHERE command_id=$1`, oldID); err != nil {
		t.Fatal("replace owned fixture outbox for backdated history:", err)
	}
	_, err = f.admin.Exec(f.ctx, `INSERT INTO public.twin_outbox
		(command_id,topic,payload,payload_bytes,status,attempt_count,next_attempt_at,created_at,published_at)
		VALUES($1,$2,$3::jsonb,$4,'failed',0,$5::timestamptz,$5::timestamptz,NULL)`,
		oldID, originalTopic, payload, payload, oldCommand.IssuedAt)
	if err != nil {
		t.Fatal("insert coherent owned historical outbox:", err)
	}
	_, err = f.admin.Exec(f.ctx, `UPDATE public.twin_command_requests q
		SET response_bytes=convert_to(jsonb_set(convert_from(q.response_bytes,'UTF8')::jsonb,
		      '{expires_at}',to_jsonb(c.expires_at::text))::text,'UTF8'),
		    created_at=q.created_at-interval '744 hours'
		FROM public.twin_commands c
		WHERE q.command_id=c.command_id AND c.command_id=$1`, oldID)
	if err != nil {
		t.Fatal("backdate owned historical admission evidence:", err)
	}
	currentRequest := controlRequest(t, current, "1", "retention-new-current")
	currentReceipt, err := f.repo.Admit(f.ctx, f.owner, f.urn, currentRequest)
	if err != nil || currentReceipt.StatusCode != 202 || currentReceipt.CommandID == nil {
		t.Fatalf("newer real Admit receipt=%+v err=%v", currentReceipt, err)
	}
	var currentFrequency string
	if err = f.admin.QueryRow(f.ctx,
		`SELECT requested_frequency_hz FROM public.twin_commands WHERE command_id=$1`,
		*currentReceipt.CommandID).Scan(&currentFrequency); err != nil || currentFrequency != current {
		t.Fatalf("real Admit did not preserve exponent token: got=%q err=%v", currentFrequency, err)
	}
	_, err = f.admin.Exec(f.ctx, `UPDATE public.twin_commands
		SET completed_at=expires_at+interval '30 seconds'
		WHERE command_id=$1 AND status='timeout' AND error_code='COMMAND_EXPIRED'`, oldID)
	if err != nil {
		t.Fatal("backdate owned expiration terminal evidence:", err)
	}
	runNativeRetentionApply(t)
	var stillExists bool
	if err = f.admin.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM public.twin_commands WHERE command_id=$1)`, oldID).Scan(&stillExists); err != nil || stillExists {
		t.Fatalf("historical command remains after real retention apply: exists=%t err=%v", stillExists, err)
	}
	replay, err := f.repo.Admit(f.ctx, f.owner, f.urn, currentRequest)
	if err != nil || replay.StatusCode != currentReceipt.StatusCode ||
		!bytes.Equal(replay.Response, currentReceipt.Response) {
		t.Fatalf("real authenticated replay on retained receipt: status=%d want=%d body=%q want=%q err=%v",
			replay.StatusCode, currentReceipt.StatusCode, replay.Response, currentReceipt.Response, err)
	}
	beforeStale := controlCommandCount(t, f)
	if _, err = f.repo.Admit(f.ctx, f.owner, f.urn, oldRequest); !errors.Is(err, digitaltwin.ErrDesiredVersionConflict) {
		t.Fatalf("purged stale original PATCH error=%v, want %v", err, digitaltwin.ErrDesiredVersionConflict)
	}
	if afterStale := controlCommandCount(t, f); afterStale != beforeStale {
		t.Fatalf("stale original PATCH created a command: before=%d after=%d", beforeStale, afterStale)
	}
}

func controlCommandCount(t *testing.T, f *controlFixture) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(f.ctx,
		`SELECT count(*) FROM public.twin_commands WHERE entity_id=$1`, f.entity).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func runNativeRetentionApply(t *testing.T) {
	t.Helper()
	script := os.Getenv("RETENTION_PSQL_PATH")
	if script == "" || os.Getenv("PGPASSWORD") == "" {
		t.Fatal("owned Stage 6 native fixture did not provide retention psql path and password environment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "psql", "-X", "-v", "ON_ERROR_STOP=1",
		"-h", "127.0.0.1", "-U", "fixture_admin", "-d", "fixture_service", "-At",
		"-v", "target_database=fixture_service", "-v", "retention_mode=apply",
		"-v", "confirm_apply_target=fixture_service", "-f", script)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("retention psql entrypoint failed: %v: %s", err, output)
	}
}
