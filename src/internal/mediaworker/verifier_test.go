package mediaworker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/media"
	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mediastorage"
)

type verifyRepoStub struct {
	record       media.Record
	finishCalled bool
	retryCalled  bool
	retryReason  string
	missing      bool
	committed    bool
}

func (r *verifyRepoStub) ClaimDueVerification(context.Context, int, time.Duration) ([]media.Record, error) {
	return nil, nil
}
func (r *verifyRepoStub) FinishVerification(ctx context.Context, _ media.VerificationGeneration, evidence *media.VerifiedEvidence, reject string) (media.Record, error) {
	if ctx.Err() != nil {
		return media.Record{}, ctx.Err()
	}
	r.finishCalled = true
	if evidence != nil {
		r.record.ValidationStatus = "ready"
	} else {
		r.record.ValidationStatus = "rejected"
		r.record.TerminalReason = &reject
	}
	r.committed = true
	return r.record, nil
}
func (r *verifyRepoStub) VerificationRetry(ctx context.Context, _ media.VerificationGeneration, code string, _ int, _ time.Duration) (media.Record, error) {
	if ctx.Err() != nil {
		return media.Record{}, ctx.Err()
	}
	r.retryCalled = true
	r.retryReason = code
	r.record.ValidationStatus = "verifying"
	return r.record, nil
}
func (r *verifyRepoStub) RecordMissingResult(ctx context.Context, _ media.VerificationGeneration) (media.Record, bool, error) {
	if ctx.Err() != nil {
		return media.Record{}, false, ctx.Err()
	}
	r.missing = true
	r.committed = true
	r.record.ValidationStatus = "pending"
	r.record.LastVerificationError = "OBJECT_MISSING"
	return r.record, true, nil
}

type objectOpenerStub struct {
	body   []byte
	stream io.ReadCloser
	mime   string
	err    error
}

func (s objectOpenerStub) Open(context.Context, string) (*mediastorage.Object, error) {
	if s.err != nil {
		return nil, s.err
	}
	body := s.stream
	if body == nil {
		body = io.NopCloser(bytes.NewReader(s.body))
	}
	return &mediastorage.Object{Body: body, ContentType: s.mime, ContentLength: int64(len(s.body))}, nil
}

type verifyPublisherStub struct {
	repo    *verifyRepoStub
	results []mediaprotocol.UploadResult
	early   bool
}

func (p *verifyPublisherStub) PublishResult(_ context.Context, result mediaprotocol.UploadResult) error {
	if !p.repo.committed {
		p.early = true
	}
	p.results = append(p.results, result)
	return nil
}

func TestVerifierPublishesReadyOnlyAfterCommittedEvidence(t *testing.T) {
	repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
	pub := &verifyPublisherStub{repo: repo}
	v := verifierForTest(repo, objectOpenerStub{body: validH264MP4, mime: "video/mp4"}, pub)
	v.process(context.Background(), repo.record)
	if !repo.finishCalled || pub.early || len(pub.results) != 1 ||
		pub.results[0].Status != mediaprotocol.ResultReady {
		t.Fatalf("finish=%v early=%v results=%+v", repo.finishCalled, pub.early, pub.results)
	}
}

func TestVerifierRejectsInvalidContentWithoutReady(t *testing.T) {
	repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
	pub := &verifyPublisherStub{repo: repo}
	v := verifierForTest(repo, objectOpenerStub{body: validH264MP4[:32], mime: "video/mp4"}, pub)
	v.process(context.Background(), repo.record)
	if !repo.finishCalled || repo.record.ValidationStatus != "rejected" || pub.early ||
		len(pub.results) != 1 || pub.results[0].Status != mediaprotocol.ResultRejected {
		t.Fatalf("record=%+v results=%+v early=%v", repo.record, pub.results, pub.early)
	}
}

func TestVerifierMissingReplayAndTransientStorageFailure(t *testing.T) {
	t.Run("durable missing", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		v := verifierForTest(repo, objectOpenerStub{err: mediastorage.ErrNotFound}, pub)
		v.process(context.Background(), repo.record)
		if !repo.missing || pub.early || len(pub.results) != 1 ||
			pub.results[0].Status != mediaprotocol.ResultPending || pub.results[0].Code != "OBJECT_MISSING" {
			t.Fatalf("missing=%v results=%+v early=%v", repo.missing, pub.results, pub.early)
		}
	})
	t.Run("transient open", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		v := verifierForTest(repo, objectOpenerStub{err: errors.New("private transport detail")}, pub)
		v.process(context.Background(), repo.record)
		if !repo.retryCalled || len(pub.results) != 0 {
			t.Fatalf("retry=%v results=%+v", repo.retryCalled, pub.results)
		}
	})
	t.Run("stream interruption", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		v := verifierForTest(repo, objectOpenerStub{stream: &streamErrorReader{data: validH264MP4[:40]}, mime: "video/mp4"}, pub)
		v.process(context.Background(), repo.record)
		if !repo.retryCalled || repo.finishCalled || len(pub.results) != 0 {
			t.Fatalf("retry=%v finish=%v results=%+v", repo.retryCalled, repo.finishCalled, pub.results)
		}
	})
}

type streamErrorReader struct{ data []byte }

func (r *streamErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, errors.New("private storage stream error")
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}
func (streamErrorReader) Close() error { return nil }

func verificationRecord(body []byte) media.Record {
	meta := validMetadata(body)
	return media.Record{MediaID: uuid.MustParse("23456789-abcd-4abc-8def-0123456789ab"),
		GatewayID: "gw1", UploadID: "u1", StoragePath: "server-owned.mp4",
		ExpectedSize: meta.DeclaredLength, DeclaredSHA256: meta.DeclaredSHA256,
		ValidationStatus: "verifying", VerificationGeneration: 3, VerificationAttempts: 1}
}

func verifierForTest(repo *verifyRepoStub, storage ObjectOpener, pub *verifyPublisherStub) *Verifier {
	return NewVerifier(repo, storage, pub, VerifierOptions{
		Workers: 1, BatchSize: 1, MaxAttempts: 3, ScanInterval: time.Second,
		StorageTimeout: time.Second, RetryMin: time.Second, RetryMax: time.Minute,
		Lease: 2 * time.Second,
	})
}

type timeoutOpener struct{}

func (timeoutOpener) Open(ctx context.Context, _ string) (*mediastorage.Object, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type timeoutBody struct {
	ctx    context.Context
	closed bool
}

func (b *timeoutBody) Read(_ []byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *timeoutBody) Close() error {
	b.closed = true
	return nil
}

type openFunc func(context.Context, string) (*mediastorage.Object, error)

func (f openFunc) Open(ctx context.Context, path string) (*mediastorage.Object, error) {
	return f(ctx, path)
}

func TestVerifierStorageTimeoutRecordsDurableRetry(t *testing.T) {
	t.Run("storage open timeout", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		v := verifierForTest(repo, timeoutOpener{}, pub)
		v.opts.StorageTimeout = 10 * time.Millisecond
		v.process(context.Background(), repo.record)
		if !repo.retryCalled || repo.retryReason != "STORAGE_UNAVAILABLE" {
			t.Fatalf("expected durable retry recorded; retryCalled=%v reason=%q", repo.retryCalled, repo.retryReason)
		}
	})

	t.Run("stream read timeout", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		body := &timeoutBody{}
		v := verifierForTest(repo, openFunc(func(ctx context.Context, _ string) (*mediastorage.Object, error) {
			body.ctx = ctx
			return &mediastorage.Object{Body: body, ContentType: "video/mp4", ContentLength: int64(len(validH264MP4))}, nil
		}), pub)
		v.opts.StorageTimeout = 10 * time.Millisecond
		v.process(context.Background(), repo.record)
		if !body.closed {
			t.Fatal("expected body closed before context cancellation")
		}
		if !repo.retryCalled || repo.retryReason != "STORAGE_UNAVAILABLE" {
			t.Fatalf("expected durable retry recorded on read timeout; retryCalled=%v reason=%q", repo.retryCalled, repo.retryReason)
		}
	})

	t.Run("parent cancellation propagation", func(t *testing.T) {
		repo := &verifyRepoStub{record: verificationRecord(validH264MP4)}
		pub := &verifyPublisherStub{repo: repo}
		v := verifierForTest(repo, timeoutOpener{}, pub)
		v.opts.StorageTimeout = 100 * time.Millisecond
		parentCtx, cancel := context.WithCancel(context.Background())
		cancel() // already canceled parent
		v.process(parentCtx, repo.record)
		if repo.retryCalled {
			t.Fatal("detached persistent update occurred with canceled parent")
		}
	})
}
